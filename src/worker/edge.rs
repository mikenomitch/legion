//! The worker's edge-boot entry points: admitting a provisional start,
//! driving it to its gate, and ratifying it (docs/edge-boot.md).
//!
//! Two things arrive here, and they are deliberately the same machinery:
//!
//! - a **manager-originated** provisional dispatch (`PostWork` carrying a
//!   `provisional` block and `allocation_id: 0`), whose ratification is a
//!   second `PostWork` with the committed payload;
//! - an **edge-initiated** start (`StartClaim` with a boot ticket), whose
//!   ratification the node fetches itself with `RatifyClaim`.
//!
//! Both funnel into [`WorkerTaskManager::admit_claim`] →
//! [`launch_provisional_allocation`] → [`WorkerTaskManager::ratify_claim`],
//! so there is exactly one claim state machine, one lease, and one kill path.

use crate::prelude::*;
use crate::structs::edge_boot::{ClaimStage, Gate, OnRefused, RefusalReason};
use crate::structs::{ProvisionalAllocation, ProvisionalClaimReport, TaskAllocation};
use crate::worker::claim_window::Feedback;
use crate::worker::claims::ClaimRecord;
use crate::worker::task_manager::{launch_provisional_allocation, WorkerTaskManager};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Build the claim record a provisional dispatch describes.
pub fn record_from_payload(
    allocation: &TaskAllocation,
    provisional: &ProvisionalAllocation,
    resources: Value,
) -> ClaimRecord {
    let now_unix = time::OffsetDateTime::now_utc().unix_timestamp();
    // The lease is carried as an absolute unix deadline so a node and a
    // manager that disagree about elapsed time still agree about *when* the
    // work dies. Converted to an Instant here because that is what a monotonic
    // sweep can compare against without being fooled by a clock step.
    let remaining = (provisional.lease_deadline_unix - now_unix).max(0) as u64;
    ClaimRecord {
        claim_id: provisional.claim_id.clone(),
        local_key: 0,
        gate: Gate::from_wire(provisional.gate),
        stage: ClaimStage::Claimed,
        lease_deadline: Instant::now() + Duration::from_secs(remaining),
        lease_deadline_unix: provisional.lease_deadline_unix,
        job_id: allocation.job_id.clone(),
        job_config_id: allocation.job_config_id.clone(),
        task_name: allocation.task_name.clone(),
        instance_id: provisional.instance_id.clone(),
        resources,
        origin: if provisional.origin.is_empty() {
            "manager".into()
        } else {
            provisional.origin.clone()
        },
        on_refused: OnRefused::parse(&provisional.on_refused).unwrap_or(OnRefused::Kill),
        ticket_jti: provisional.ticket_jti.clone(),
        allocation_id: None,
        session: provisional
            .session
            .as_ref()
            .map(|start| crate::worker::claims::SessionClaim {
                lease_issued_at_unix_ms: start.lease_issued_at_unix_ms,
                generation: 1,
            }),
    }
}

/// Admit a provisional dispatch and start driving it to its gate.
///
/// Returns the admitted record. The launch itself runs on a blocking thread
/// (image pulls and root-disk builds take seconds), so this returns as soon as
/// the claim is *held* — which is the point at which the caller may report
/// `admitted` and the ledger is already protecting the node from a second
/// admission it has no room for.
///
/// `slot` is the launch slot the caller took from the node's
/// [`crate::worker::launch_pool::LaunchPool`] before admitting; the launch
/// thread holds it until the runtime call returns. `None` only in tests.
///
/// `window` is the room an edge start reserved in the node's
/// [`crate::worker::claim_window::ClaimWindow`], with the claim's lease. An
/// admitted claim holds it until its verdict; a refused one gives it back.
/// `None` for manager-originated claims and sessions, which the window does
/// not count.
pub fn admit_and_launch(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    payload: &TaskAllocation,
    provisional: &ProvisionalAllocation,
    slot: Option<crate::worker::launch_pool::LaunchSlot>,
    window: Option<(crate::worker::claim_window::Reservation, Duration)>,
) -> R<ClaimRecord> {
    let resources: Value = if payload.task_resources.trim().is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_str(&payload.task_resources).unwrap_or_else(|_| serde_json::json!({}))
    };
    let record = record_from_payload(payload, provisional, resources);
    let gate = record.gate;
    if gate == Gate::Off {
        bail!(
            "provisional dispatch for claim {} carries gate `off`; nothing to do",
            record.claim_id
        );
    }

    // A session is admitted into its envelope, not the node's free capacity
    // (docs/session-mode.md); everything after admission is the same claim
    // machinery.
    let session = record.is_session();
    let admitted = {
        let mut guard = task_manager.lock().expect("task manager");
        let admitted = if session {
            guard.admit_session(record)?
        } else {
            guard.admit_claim(record)?
        };
        // Bound under the task manager lock, so the claim's own exit (which
        // frees the slot under the same lock) cannot run first.
        if let Some((reservation, lease)) = window {
            reservation.bind(&admitted.claim_id, lease);
        }
        admitted
    };

    // The payload's allocation id is 0 on the wire; the node runs the work
    // under the claim's negative local key.
    let mut launch_payload = payload.clone();
    launch_payload.allocation_id = admitted.local_key;

    if !session {
        crate::manager::telemetry::worker_metrics()
            .provisional_starts_total
            .inc(&[admitted.origin.as_str(), gate.as_str()]);
    }

    let claim_id = admitted.claim_id.clone();
    let manager = task_manager.clone();
    let launch = {
        let claim_id = claim_id.clone();
        move || {
            let _slot = slot;
            if let Err(err) =
                launch_provisional_allocation(&manager, &launch_payload, &claim_id, gate)
            {
                warn!(claim_id, error = ?err, "Provisional launch failed");
            }
        }
    };
    // The launch ends by starting the claim's health monitor, a tokio task,
    // so it runs inside the caller's runtime (an RPC handler's), on one of
    // its pooled blocking threads. Not a new OS thread per start: each one
    // maps and unmaps its stack, which takes the worker's memory map lock
    // for writing, and at hundreds of starts a second that stalled every
    // page fault and spawn in the worker. The launch pool bounds how many
    // run at once.
    match crate::worker::task_manager::current_runtime() {
        Some(runtime) => {
            runtime.spawn_blocking(launch);
        }
        None => {
            std::thread::Builder::new()
                .name(format!("provisional-launch-{claim_id}"))
                .spawn(launch)
                .context("failed to spawn provisional launch thread")?;
        }
    }

    Ok(admitted)
}

/// A session started at the edge (`Worker.StartSession`).
#[derive(Debug, Clone)]
pub struct EdgeSession {
    pub session_id: String,
    pub claim_id: String,
    /// True when an idempotent retry found it already on this node.
    pub deduplicated: bool,
}

/// The session id an edge start with an idempotency key derives. Scoped to
/// edge starts of the ticket's job and task, so it never collides with a
/// manager-routed key's id.
pub fn edge_derived_session_id(job_id: &str, task_name: &str, key: &str) -> String {
    crate::structs::session::derived_session_id(task_name, job_id, &format!("edge:{key}"))
}

/// Start a session on this node from a session ticket, with no manager on
/// the path (session durability plan §6.4). The caller verified the ticket
/// and holds a launch slot.
///
/// The node mints the id with its own tag (or derives it from the
/// idempotency key), builds the payload from the spec its envelope carried,
/// and admits it through the same claim machinery a manager-routed session
/// takes. A refusal is a [`crate::worker::task_manager::SessionRefusal`]
/// and holds nothing.
pub fn start_edge_session(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    node_id: &str,
    ticket: &crate::structs::edge_boot::SessionTicketClaims,
    idempotency_key: &str,
    slot: crate::worker::launch_pool::LaunchSlot,
) -> R<EdgeSession> {
    let (job_id, task_name) = (ticket.job.as_str(), ticket.task.as_str());
    let derived = !idempotency_key.trim().is_empty();
    let session_id = if derived {
        edge_derived_session_id(job_id, task_name, idempotency_key.trim())
    } else {
        crate::structs::mint_session_id(task_name, node_id)
    };
    let spec = {
        let guard = task_manager.lock().expect("task manager");
        if derived {
            if let Some(existing) = guard.session_status(&session_id) {
                return Ok(EdgeSession {
                    claim_id: crate::structs::session::session_claim_id(&existing.session_id),
                    session_id: existing.session_id,
                    deduplicated: true,
                });
            }
        }
        guard.session_spec(job_id, task_name)
    };
    let Some(spec) = spec else {
        return Err(crate::worker::task_manager::SessionRefusal::NoEnvelope {
            job_id: job_id.to_string(),
            task_name: task_name.to_string(),
        }
        .into());
    };
    let issued_at = crate::util::now_unix_ms();
    let (payload, provisional) = session_payload(
        &spec,
        job_id,
        &session_id,
        node_id,
        "edge",
        &ticket.jti,
        issued_at,
    )?;
    // An idempotent `store` start creates the session's manifest before its
    // gate opens, so two nodes that derived the same id cannot both run it
    // (session durability plan §6.7, row 3).
    crate::worker::task_manager::session_store::claim_new_store_session(task_manager, &payload)?;
    admit_and_launch(task_manager, &payload, &provisional, Some(slot), None)?;
    crate::manager::telemetry::worker_metrics()
        .edge_session_starts_total
        .inc();
    Ok(EdgeSession {
        session_id,
        claim_id: crate::structs::session::session_claim_id(&provisional.instance_id),
        deduplicated: false,
    })
}

/// The launch payload and provisional block of a session of `spec` on this
/// node, as a manager's router or an edge start builds it: the session's own
/// identity in the workload's environment (`LEGION_INSTANCE_ID`,
/// `LEGION_INSTANCE_MODE=session`), the serve gate, and the lease it was
/// first issued at (kept across a resume on another node).
pub fn session_payload(
    spec: &crate::worker::task_manager::SessionSpec,
    job_id: &str,
    session_id: &str,
    node_id: &str,
    origin: &str,
    ticket_jti: &str,
    issued_at: i64,
) -> R<(TaskAllocation, ProvisionalAllocation)> {
    let identity = crate::structs::Instance {
        instance_id: session_id.to_string(),
        job_id: job_id.to_string(),
        group: spec.task.group.clone(),
        task_name: spec.task.name.clone(),
        mode: crate::structs::InstanceMode::Session,
        state: crate::structs::InstanceState::Pending,
        slot_index: None,
        current_alloc_id: None,
        storage_handle: None,
        created_at: String::new(),
        updated_at: String::new(),
        restore_from: None,
        identity_epoch: 0,
        named: false,
        promoted_from: None,
    };
    let allocation = crate::structs::Allocation {
        id: 0,
        status: "provisional".into(),
        resources: spec.task.resources.clone(),
        node_id: node_id.to_string(),
        job_config_id: spec.job_config_id.clone(),
        task_config_id: spec.task_config_id,
        job_id: job_id.to_string(),
        instance_id: Some(session_id.to_string()),
        network: None,
    };
    let provisional = ProvisionalAllocation {
        claim_id: crate::structs::session::session_claim_id(session_id),
        gate: Gate::Serve.to_wire(),
        lease_deadline_unix: 0,
        instance_id: session_id.to_string(),
        origin: origin.to_string(),
        on_refused: OnRefused::Stop.as_str().into(),
        ticket_jti: ticket_jti.to_string(),
        session: Some(crate::structs::SessionStart {
            lease_issued_at_unix_ms: issued_at,
        }),
    };
    let payload = crate::manager::dispatcher::build_task_allocation_with(
        &spec.task,
        &allocation,
        Some(&identity),
        &[],
        Some(provisional.clone()),
    )?;
    Ok((payload, provisional))
}

/// Wait until a session's launch settles: `Ok(true)` once it runs,
/// `Ok(false)` when it is still starting at `timeout`, and an error when
/// it ended (its runtime failed) before running.
pub async fn wait_until_running(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    session_id: &str,
    timeout: Duration,
) -> R<bool> {
    let deadline = Instant::now() + timeout;
    let claim_id = crate::structs::session::session_claim_id(session_id);
    // Unregisters the waker however the wait ends, a dropped request
    // included.
    struct Registered<'a>(&'a Arc<Mutex<WorkerTaskManager>>, &'a str);
    impl Drop for Registered<'_> {
        fn drop(&mut self) {
            if let Ok(mut guard) = self.0.lock() {
                guard.release_stage_waiter(self.1);
            }
        }
    }
    let _registered = Registered(task_manager, &claim_id);
    loop {
        // Checked and registered under one lock: a stage change after the
        // check leaves a permit on the waker, so it cannot be missed.
        let (state, waiter) = {
            let mut guard = task_manager.lock().expect("task manager");
            let state = guard.session_state(session_id);
            (state, guard.stage_waiter(&claim_id))
        };
        match state.as_deref() {
            Some("running") => return Ok(true),
            Some(_) => {}
            None => {
                bail!("session {session_id} ended before it started running; see the node's log")
            }
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(false);
        }
        // Woken by the claim's next stage change or its end. The backstop
        // covers the paths that settle a session without either.
        let _ = tokio::time::timeout(
            (deadline - now).min(RUNNING_WAIT_BACKSTOP),
            waiter.notified(),
        )
        .await;
    }
}

/// The longest [`wait_until_running`] sleeps without being woken before it
/// looks again.
const RUNNING_WAIT_BACKSTOP: Duration = Duration::from_millis(50);

/// Apply a ratification verdict pushed by the manager (or fetched by the
/// node) to a live claim.
///
/// `feedback` is what the outcome says about the control plane, for the
/// node's claim window: [`Feedback::Verdict`] when a manager answered,
/// [`Feedback::Failed`] when the ratify call failed or ran out of lease.
pub fn apply_verdict(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    claim_id: &str,
    confirmed: Option<&TaskAllocation>,
    reason: RefusalReason,
    detail: &str,
    feedback: Feedback,
) {
    let metrics = crate::manager::telemetry::worker_metrics();
    let mut guard = task_manager.lock().expect("task manager");
    // Before the ratify or kill below frees the slot, so the window learns
    // how long this claim waited.
    guard.claim_window().settle(claim_id, feedback);
    let mut start_health_monitor = None;
    match confirmed {
        Some(payload) => match guard.ratify_claim(claim_id, payload) {
            // A `stage`-gated claim starts its workload at ratification, and
            // nothing else gives it a health monitor: without one it would
            // never be reported healthy.
            Ok(ratified) => start_health_monitor = ratified.start_health_monitor,
            Err(err) => {
                // The gate could not be opened, or the held start could not be
                // started. Killing is the only safe answer: the cluster now
                // believes this allocation is running, and a half-started one
                // that never reports would be worse than a reschedule.
                warn!(claim_id, error = ?err, "Ratification could not be applied; killing the claim");
                guard.kill_claim(claim_id, &format!("ratification failed locally: {err:#}"));
                metrics.provisional_killed_total.inc(&["failed"]);
            }
        },
        None => {
            guard.kill_claim(
                claim_id,
                &format!("ratification refused ({reason}): {detail}"),
            );
            metrics.provisional_killed_total.inc(&["refused"]);
        }
    }
    metrics
        .provisional_active
        .set(guard.unratified_claims().len() as i64);
    // Off the lock: the monitor takes it to register itself.
    drop(guard);
    if let Some((allocation, task)) = start_health_monitor {
        crate::worker::task_health::spawn_health_monitor(task_manager.clone(), allocation, task);
    }
}

/// Kill every claim whose lease expired. Called from the worker's supervisor
/// tick — the backstop that makes the design fail closed regardless of which
/// delivery path went missing.
pub fn sweep_leases(task_manager: &Arc<Mutex<WorkerTaskManager>>) {
    let metrics = crate::manager::telemetry::worker_metrics();
    let mut guard = task_manager.lock().expect("task manager");
    let killed = guard.sweep_expired_claims(Instant::now());
    for _ in &killed {
        metrics.provisional_killed_total.inc(&["expired"]);
    }
    metrics
        .provisional_active
        .set(guard.unratified_claims().len() as i64);
}

/// A node's cache of task configs it may start work for, keyed by job config
/// id (docs/edge-boot.md).
///
/// An edge start names an exact job config; without the matching task config
/// the node has nothing to run. This is populated from every payload the node
/// receives — an ordinary dispatch, a heartbeat's desired set, a provisional
/// start — which is exactly "configs this node has run an allocation for".
/// Nothing is fetched on the fast path: doing so would reintroduce the round
/// trip the whole feature exists to avoid.
pub type SpecCache = Arc<Mutex<HashMap<String, Vec<crate::structs::TaskConfig>>>>;

/// Record the task configs carried by dispatched allocations, so a later
/// `StartClaim` for the same job config can run without a manager.
///
/// Bounded by replacing a config's entry wholesale rather than accumulating:
/// a node that runs a hundred versions of one job holds one entry per version
/// it actually ran, not one per dispatch.
pub fn remember_specs(cache: &SpecCache, allocations: &[TaskAllocation]) {
    if allocations.is_empty() {
        return;
    }
    let mut guard = match cache.lock() {
        Ok(guard) => guard,
        Err(_) => return,
    };
    for allocation in allocations {
        if allocation.job_config_id.trim().is_empty() {
            continue;
        }
        let Ok((_, task)) = crate::worker::task_manager::task_and_allocation(allocation) else {
            continue;
        };
        let tasks = guard.entry(allocation.job_config_id.clone()).or_default();
        match tasks.iter_mut().find(|cached| cached.name == task.name) {
            Some(cached) => *cached = task,
            None => tasks.push(task),
        }
    }
}

/// The heartbeat's view of this node's unratified work.
pub fn claim_reports(task_manager: &Arc<Mutex<WorkerTaskManager>>) -> Vec<ProvisionalClaimReport> {
    task_manager
        .lock()
        .expect("task manager")
        .unratified_claims()
        .into_iter()
        .map(|claim| ProvisionalClaimReport {
            claim_id: claim.claim_id,
            job_config_id: claim.job_config_id,
            task_name: claim.task_name,
            instance_id: claim.instance_id,
            gate: claim.gate.to_wire(),
            stage: claim.stage.as_str().to_string(),
            lease_deadline_unix: claim.lease_deadline_unix,
            resources: claim.resources.to_string(),
        })
        .collect()
}

#[cfg(test)]
mod worker_edge_test {
    use super::*;

    fn payload() -> TaskAllocation {
        TaskAllocation {
            task_network: String::new(),
            allocation_id: 0,
            job_id: "agent-sandboxes".into(),
            job_config_id: "jc-1".into(),
            node_id: "node-a".into(),
            task_config_id: 7,
            task_name: "workspace".into(),
            task_runtime: "noop".into(),
            task_config: "{}".into(),
            task_checks: String::new(),
            task_on_unhealthy: String::new(),
            network: String::new(),
            task_connect: String::new(),
            task_artifacts: String::new(),
            task_identities: String::new(),
            self_terminate_after_seconds: 0,
            task_volumes: String::new(),
            task_resources: r#"{"memory":512}"#.into(),
            task_stop: String::new(),
            provisional: None,
            priority_band: String::new(),
            restore: None,
            task_suspend: String::new(),
            task_snapshots: String::new(),
            identity_epoch: 0,
            promoted_from_session: String::new(),
            session_task_spec: String::new(),
            session_task_config_id: 0,
            session_seal_keys: Vec::new(),
            envelope_resources: String::new(),
            task_lifecycle: String::new(),
            task_restart: String::new(),
            task_speculative_start: String::new(),
        }
    }

    fn provisional(lease_deadline_unix: i64) -> ProvisionalAllocation {
        ProvisionalAllocation {
            claim_id: "clm_a".into(),
            gate: Gate::Boot.to_wire(),
            lease_deadline_unix,
            instance_id: "user-42".into(),
            origin: "edge".into(),
            on_refused: "kill".into(),
            ticket_jti: "tkt_1".into(),
            session: None,
        }
    }

    #[test]
    fn dispatched_specs_are_remembered_for_later_edge_starts() {
        // Without this the spec cache is empty forever and every StartClaim
        // fails: a ticket names a job config, and the node needs the matching
        // task config to run anything.
        let cache: SpecCache = Arc::new(Mutex::new(HashMap::new()));
        remember_specs(&cache, &[payload()]);

        let guard = cache.lock().expect("cache");
        let tasks = guard.get("jc-1").expect("job config cached");
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].name, "workspace");
        assert_eq!(tasks[0].runtime, "noop");
    }

    #[test]
    fn a_redispatch_replaces_a_cached_task_rather_than_appending() {
        // A node that runs a hundred dispatches of one task must hold one
        // entry for it, not a hundred.
        let cache: SpecCache = Arc::new(Mutex::new(HashMap::new()));
        let mut updated = payload();
        updated.task_config = serde_json::json!({ "command": ["new"] }).to_string();

        remember_specs(&cache, &[payload()]);
        remember_specs(&cache, &[updated]);

        let guard = cache.lock().expect("cache");
        let tasks = guard.get("jc-1").expect("job config cached");
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].config["command"], serde_json::json!(["new"]));
    }

    #[test]
    fn two_tasks_of_one_job_config_are_both_cached() {
        let cache: SpecCache = Arc::new(Mutex::new(HashMap::new()));
        let mut sidecar = payload();
        sidecar.task_name = "sidecar".into();
        remember_specs(&cache, &[payload(), sidecar]);

        let guard = cache.lock().expect("cache");
        assert_eq!(guard.get("jc-1").expect("cached").len(), 2);
    }

    #[test]
    fn a_payload_without_a_job_config_is_not_cached() {
        let cache: SpecCache = Arc::new(Mutex::new(HashMap::new()));
        let mut orphan = payload();
        orphan.job_config_id = String::new();
        remember_specs(&cache, &[orphan]);
        assert!(cache.lock().expect("cache").is_empty());
    }

    #[test]
    fn a_record_carries_the_wire_lease_and_the_gate() {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let record = record_from_payload(&payload(), &provisional(now + 5), serde_json::json!({}));
        assert_eq!(record.gate, Gate::Boot);
        assert_eq!(record.instance_id, "user-42");
        assert_eq!(record.origin, "edge");
        assert_eq!(record.ticket_jti, "tkt_1");
        assert!(record.lease_deadline > Instant::now());
    }

    #[test]
    fn an_already_expired_lease_does_not_underflow_into_the_future() {
        // A deadline in the past must produce a deadline in the past, not a
        // saturating-subtraction wrap that would let the claim outlive its
        // lease by a century.
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let record = record_from_payload(&payload(), &provisional(now - 60), serde_json::json!({}));
        assert!(record.lease_deadline <= Instant::now() + Duration::from_millis(50));
    }

    #[test]
    fn an_absent_origin_reads_as_manager_and_a_bad_on_refused_as_kill() {
        let mut block = provisional(0);
        block.origin = String::new();
        block.on_refused = "explode".into();
        let record = record_from_payload(&payload(), &block, serde_json::json!({}));
        assert_eq!(record.origin, "manager");
        // Fail closed: an unrecognized policy kills rather than lingering.
        assert_eq!(record.on_refused, OnRefused::Kill);
    }
}
