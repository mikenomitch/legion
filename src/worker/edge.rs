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
    }
}

/// Admit a provisional dispatch and start driving it to its gate.
///
/// Returns the admitted record. The launch itself runs on a blocking thread
/// (image pulls and root-disk builds take seconds), so this returns as soon as
/// the claim is *held* — which is the point at which the caller may report
/// `admitted` and the ledger is already protecting the node from a second
/// admission it has no room for.
pub fn admit_and_launch(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    payload: &TaskAllocation,
    provisional: &ProvisionalAllocation,
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

    let admitted = {
        let mut guard = task_manager.lock().expect("task manager");
        guard.admit_claim(record)?
    };

    // The payload's allocation id is 0 on the wire; the node runs the work
    // under the claim's negative local key.
    let mut launch_payload = payload.clone();
    launch_payload.allocation_id = admitted.local_key;

    crate::manager::telemetry::worker_metrics()
        .provisional_starts_total
        .inc(&[admitted.origin.as_str(), gate.as_str()]);

    let claim_id = admitted.claim_id.clone();
    let manager = task_manager.clone();
    std::thread::Builder::new()
        .name(format!("provisional-launch-{claim_id}"))
        .spawn(move || {
            if let Err(err) =
                launch_provisional_allocation(&manager, &launch_payload, &claim_id, gate)
            {
                warn!(claim_id, error = ?err, "Provisional launch failed");
            }
        })
        .context("failed to spawn provisional launch thread")?;

    Ok(admitted)
}

/// Apply a ratification verdict pushed by the manager (or fetched by the
/// node) to a live claim.
pub fn apply_verdict(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    claim_id: &str,
    confirmed: Option<&TaskAllocation>,
    reason: RefusalReason,
    detail: &str,
) {
    let metrics = crate::manager::telemetry::worker_metrics();
    let mut guard = task_manager.lock().expect("task manager");
    match confirmed {
        Some(payload) => match guard.ratify_claim(claim_id, payload) {
            Ok(_) => {}
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
