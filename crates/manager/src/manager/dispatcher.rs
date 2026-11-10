use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::Gate;
use crate::structs::ResolvedSpeculativeStart;
use crate::structs::{
    worker_client::WorkerClient, Allocation, ProvisionalAllocation, TaskAllocation, TaskConfig,
};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::runtime::RuntimeFlavor;
use tokio::sync::Semaphore;
use tonic::transport::Channel;

pub use crate::structs::task_payload::{
    attach_session_task_spec, build_task_allocation, build_task_allocation_with, effective_gate,
    resolved_speculative_start,
};

/// Cap on concurrently in-flight `PostWork` sends across all evals.
///
/// Detached dispatch means eval workers no longer provide natural
/// backpressure: a burst of evals could otherwise pile up an unbounded set
/// of RPC tasks. The cap only bounds concurrency — sends past it queue on
/// the semaphore, they are not dropped — and it is deliberately generous:
/// the real volume is bounded by evals in flight times allocations per
/// eval, and a send is one small unary RPC.
const MAX_IN_FLIGHT_DISPATCHES: usize = 512;

fn dispatch_permits() -> Arc<Semaphore> {
    static PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    Arc::clone(PERMITS.get_or_init(|| Arc::new(Semaphore::new(MAX_IN_FLIGHT_DISPATCHES))))
}

#[instrument(err, skip_all)]
pub fn dispatch_allocations(store: &Store, allocations: &[Allocation]) -> R<()> {
    if allocations.is_empty() {
        return Ok(());
    }

    let mut task_cache: HashMap<String, Arc<Vec<(i64, TaskConfig)>>> = HashMap::new();
    let mut node_cache: HashMap<String, Arc<crate::structs::Node>> = HashMap::new();

    // Requests are built first, synchronously: it is all local store reads,
    // a build failure is a real error that must fail the eval, and the
    // caches make those reads cheap.
    let mut pending: Vec<(String, crate::structs::PostWorkRequest, i64, String)> =
        Vec::with_capacity(allocations.len());
    let mut clients: HashMap<String, WorkerClient<Channel>> = HashMap::new();

    let work_log = store.work_log();
    for allocation in allocations {
        // A node streaming its work from this manager gets the placement on
        // its stream within a round trip of the commit (sandbox-scaling plan
        // §6.13); a push would only race it.
        if work_log.streaming(&allocation.node_id) {
            continue;
        }
        if !task_cache.contains_key(&allocation.job_config_id) {
            // The read model's shared list: task configs are immutable per
            // `job_config_id`, and this used to be a SQL query (plus row
            // parsing) per eval on the leader's read pool.
            let task_configs =
                store.shared_task_configs_for_job_config(&allocation.job_config_id)?;
            task_cache.insert(allocation.job_config_id.clone(), task_configs);
        }
        let task = task_cache
            .get(&allocation.job_config_id)
            .and_then(|tasks| {
                tasks
                    .iter()
                    .find(|(id, _)| *id == allocation.task_config_id)
                    .map(|(_, task)| task)
            })
            .with_context(|| format!("Missing task config {}", allocation.task_config_id))?;

        if !node_cache.contains_key(&allocation.node_id) {
            // The read model's `Arc` (a refcount bump) instead of a SQL row
            // read and a JSON parse of every attribute bag per placed node.
            let node = match store.cached_node(&allocation.node_id)? {
                Some(node) => node,
                None => Arc::new(store.get_node(&allocation.node_id)?),
            };
            node_cache.insert(allocation.node_id.clone(), node);
        }
        let node = node_cache
            .get(&allocation.node_id)
            .context("Missing node in cache")?;
        // Dispatch is best-effort: a node the manager can't dial (no address
        // yet, or a pre-mTLS worker) still gets this allocation on its next
        // heartbeat reply.
        let target = match crate::manager::worker_plane::WorkerTarget::for_node(node) {
            Ok(target) => target,
            Err(status) => {
                debug!(
                    node_id = %allocation.node_id,
                    reason = %status.message(),
                    "Skipping allocation push; the heartbeat reply carries it"
                );
                continue;
            }
        };
        let worker_address = target.address.clone();

        let instance = match allocation.instance_id.as_deref() {
            Some(instance_id) => store.get_instance(instance_id)?,
            None => None,
        };
        // Volume mounts are resolved here (not on the worker) so the
        // worker receives concrete volume ids and the driver attrs from
        // previous mounts, and never re-derives storage from a jobspec.
        let volumes =
            store.volume_mounts_for_allocation(&allocation.job_id, task, instance.as_ref())?;
        let mut task_allocation =
            build_task_allocation(task, allocation, instance.as_ref(), &volumes)?;
        attach_gang_env(store, task, allocation, &mut task_allocation)?;
        if let Some(tasks) = task_cache.get(&allocation.job_config_id) {
            let session_task = attach_session_task_spec(
                &mut task_allocation,
                task,
                tasks.iter().map(|(id, task)| (*id, task)),
            )?;
            attach_session_seal_keys(store, &mut task_allocation, session_task);
        }
        // The push must carry everything the heartbeat's desired set does:
        // a worker launches from whichever copy reaches it first, and a
        // resumed instance launched from a push without its restore payload
        // boots cold and never restores.
        task_allocation.priority_band = store
            .cached_band_for_job_config(&allocation.job_config_id)?
            .as_str()
            .to_string();
        task_allocation.restore =
            crate::manager::server::snapshot_restore_payload(store, instance.as_ref());
        let request = crate::structs::PostWorkRequest {
            allocation: Some(task_allocation),
        };

        if !clients.contains_key(&worker_address) {
            match crate::manager::worker_plane::WorkerPlane::global().client_for(store, &target) {
                Ok(client) => {
                    clients.insert(worker_address.clone(), client);
                }
                Err(status) => {
                    warn!(
                        node_id = %allocation.node_id,
                        error = %status.message(),
                        "Skipping allocation push; the heartbeat reply carries it"
                    );
                    continue;
                }
            }
        }
        pending.push((
            worker_address,
            request,
            allocation.id,
            allocation.node_id.clone(),
        ));
    }

    // The sends leave the eval's critical path entirely. Dispatch is
    // best-effort BY DESIGN — the heartbeat reply carries the full desired
    // set, so a lost push costs at most one heartbeat interval — and the
    // eval's outcome is already durable once the plan commits, so there is
    // nothing correct about making the eval worker wait for worker RPCs.
    // The wait was most of the gap between `legion_eval_handle_seconds`
    // (~272 ms at 100k allocations) and the plan path (~94 ms): worker
    // round trips billed to the scheduler's throughput.
    //
    // Each send still takes a permit from a global cap, so a burst of
    // evals queues sends instead of accumulating unbounded RPC tasks.
    let sends = pending.into_iter().map(|(address, request, id, node_id)| {
        let mut client = clients
            .get(&address)
            .expect("client inserted above")
            .clone();
        let permits = dispatch_permits();
        async move {
            // Closed-semaphore is impossible (it is never closed); treat
            // an error like a dropped push and let the heartbeat heal it.
            let _permit = match permits.acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => return,
            };
            let started = std::time::Instant::now();
            match client.post_work(request).await {
                Ok(_) => {
                    crate::manager::telemetry::metrics()
                        .dispatch_send_seconds
                        .observe(started.elapsed().as_secs_f64());
                    info!(
                        allocation_id = id,
                        node_id = %node_id,
                        "Dispatched allocation to worker"
                    )
                }
                Err(err) => warn!(
                    allocation_id = id,
                    node_id = %node_id,
                    error = ?err,
                    "Failed to dispatch allocation to worker"
                ),
            }
        }
    });
    let send_all = futures::future::join_all(sends);

    match tokio::runtime::Handle::try_current() {
        // Detach on a real runtime. `resolve_claimed_eval` runs on the
        // blocking pool, where the ambient handle is the manager's
        // multi-thread runtime.
        Ok(handle) if handle.runtime_flavor() != RuntimeFlavor::CurrentThread => {
            handle.spawn(send_all);
            Ok(())
        }
        // No runtime (unit tests, one-off tools): keep the old blocking
        // behavior rather than silently not sending.
        _ => run_dispatch_future(async {
            send_all.await;
            Ok(())
        }),
    }
}

/// Minimum worker protocol version that understands
/// `TaskAllocation.provisional`.
///
/// This is the one genuinely dangerous version skew in the whole feature: an
/// older worker ignores the field and runs the payload the moment it arrives —
/// which, with pre-commit dispatch, means running it *before* the commit with
/// no gate and no lease. So it is fenced here, at the dispatcher, per node:
/// a node below this version silently falls back to post-commit dispatch and
/// simply does not get the speedup.
pub const MIN_PROVISIONAL_PROTOCOL_VERSION: u64 = 2;

/// One provisional dispatch the manager made while its plan was in flight.
#[derive(Debug, Clone)]
pub struct SpeculativeClaim {
    pub claim_id: String,
    pub node_id: String,
    pub task_config_id: i64,
    pub gate: Gate,
}

/// Dispatch provisional payloads for the placements of a plan that is about
/// to be applied (edge boot plan §6.3(a)).
///
/// This is a strictly internal reordering: the same manager, the same worker
/// plane, the same `PostWork` RPC, just sent while the apply is in flight
/// rather than after it commits. It saves the raft commit, the read-model
/// refresh, and the serialization of dispatch behind them.
///
/// It deliberately does **not** carry an instance id. Which keyed instance a
/// placement binds is decided inside the apply transaction, so a payload sent
/// before it cannot know — and guessing would be exactly the kind of
/// speculative binding this design refuses. Keyed starts that need to name
/// their instance up front use the edge path, where the caller already knows
/// which sandbox it is starting.
///
/// Failures are swallowed on purpose: a provisional dispatch is an
/// optimization. If it does not land, the ordinary post-commit dispatch still
/// runs and the only thing lost is the head start.
///
/// Takes the planner's placements directly rather than a prepared node list:
/// the node speculated on is `candidates[0]` (== `node_id`), and building a
/// separate list up front would clone a node id per placement on every plan,
/// including the overwhelming majority where no task opted in and this
/// returns on the first cache lookup.
#[instrument(skip_all)]
pub fn dispatch_speculative(
    store: &Store,
    job_config_id: &str,
    placements: &[crate::manager::scheduler::PlannedAllocation],
) -> Vec<SpeculativeClaim> {
    if placements.is_empty() {
        return Vec::new();
    }
    // Read from the task-config cache the planner already warmed, and bail
    // before touching anything else when no task opted in. That early return
    // is what keeps `mode: 'off'` — every job today — free on the plan path:
    // a non-speculative plan does one cache lookup and nothing more.
    let tasks = match store.shared_task_configs_for_job_config(job_config_id) {
        Ok(tasks) => tasks,
        Err(err) => {
            debug!(%job_config_id, error = ?err, "Speculative dispatch skipped; task configs unavailable");
            return Vec::new();
        }
    };
    let speculative: HashMap<i64, ResolvedSpeculativeStart> = tasks
        .iter()
        .filter_map(|(id, task)| match resolved_speculative_start(task) {
            Ok(resolved) if resolved.mode != Gate::Off => Some((*id, resolved)),
            Ok(_) => None,
            Err(err) => {
                // Admission already validated the block, so this is a
                // corrupted row rather than user error; it must not fail the
                // plan.
                warn!(task = %task.name, error = ?err, "Invalid speculativeStart; not speculating");
                None
            }
        })
        .collect();
    if speculative.is_empty() {
        return Vec::new();
    }

    let mut claims = Vec::new();
    for placement in placements {
        let node_id = &placement.node_id;
        let task_config_id = &placement.task_config_id;
        let Some(resolved) = speculative.get(task_config_id) else {
            continue;
        };
        let Some((_, task)) = tasks.iter().find(|(id, _)| id == task_config_id) else {
            continue;
        };
        match speculative_claim_for(store, node_id, *task_config_id, task, *resolved) {
            Ok(Some(claim)) => claims.push(claim),
            Ok(None) => {}
            Err(err) => warn!(
                node_id = %node_id,
                task_config_id,
                error = ?err,
                "Speculative dispatch skipped"
            ),
        }
    }
    claims
}

fn speculative_claim_for(
    store: &Store,
    node_id: &str,
    task_config_id: i64,
    task: &TaskConfig,
    speculative: ResolvedSpeculativeStart,
) -> R<Option<SpeculativeClaim>> {
    let (job_id, job_config_id) = match store.task_config_job_ids(task_config_id)? {
        Some(ids) => ids,
        None => return Ok(None),
    };

    let node = store.get_node(node_id)?;
    if crate::manager::worker_plane::worker_protocol_version(&node)
        < MIN_PROVISIONAL_PROTOCOL_VERSION
    {
        debug!(
            node_id,
            "Node predates provisional dispatch; falling back to post-commit dispatch"
        );
        return Ok(None);
    }
    if worker_address_for_node(&node).is_none() {
        return Ok(None);
    }

    let claim_id = crate::structs::edge_boot::new_claim_id();
    let lease_deadline_unix = time::OffsetDateTime::now_utc().unix_timestamp()
        + speculative.ratify_timeout.as_secs() as i64;
    let provisional = ProvisionalAllocation {
        claim_id: claim_id.clone(),
        gate: speculative.mode.to_wire(),
        lease_deadline_unix,
        instance_id: String::new(),
        origin: "manager".into(),
        on_refused: speculative.on_refused.as_str().to_string(),
        ticket_jti: String::new(),
        session: None,
    };
    // A placeholder allocation with id 0: the payload's own `allocation_id`
    // is 0 for exactly this reason, and the node keys the work by claim id.
    let allocation = Allocation {
        id: 0,
        status: "provisional".into(),
        resources: task.resources.clone(),
        node_id: node_id.to_string(),
        job_config_id,
        task_config_id,
        job_id,
        instance_id: None,
        network: None,
    };
    let payload = build_task_allocation_with(task, &allocation, None, &[], Some(provisional))?;

    let sent = run_dispatch_future(async {
        let mut client = crate::manager::worker_plane::WorkerPlane::global()
            .client(store, &node)
            .map_err(|status| anyhow!("{}", status.message()))?;
        client
            .post_work(crate::structs::PostWorkRequest {
                allocation: Some(payload),
            })
            .await?;
        Ok::<(), anyhow::Error>(())
    });
    if let Err(err) = sent {
        warn!(node_id, error = ?err, "Speculative dispatch failed; the commit path still runs");
        return Ok(None);
    }

    store.events().provisional_started(
        &claim_id,
        node_id,
        allocation.job_id.as_str(),
        allocation.job_config_id.as_str(),
        &task.name,
        "",
        speculative.mode.as_str(),
        "manager",
        "",
    );
    Ok(Some(SpeculativeClaim {
        claim_id,
        node_id: node_id.to_string(),
        task_config_id,
        gate: speculative.mode,
    }))
}

/// Deliver the apply outcome to the nodes holding provisional dispatches:
/// the ratification (edge boot plan §6.3(a)).
///
/// The payload is the *final* one — real allocation id, assigned network,
/// resolved volume mounts — carrying the same `provisional.claim_id`. That is
/// what lets a `stage`-gated node finish the start it was holding with a
/// payload that knows its ports, instead of one that was written before they
/// existed.
///
/// A claim with no matching applied allocation is left to its lease: the plan
/// was rejected, or placed elsewhere, and the node kills the work at the
/// deadline with no further message.
#[instrument(skip_all)]
pub fn dispatch_ratifications(
    store: &Store,
    allocations: &[Allocation],
    claims: &[SpeculativeClaim],
) {
    if claims.is_empty() {
        return;
    }
    let mut remaining: Vec<&SpeculativeClaim> = claims.iter().collect();
    for allocation in allocations {
        let Some(index) = remaining.iter().position(|claim| {
            claim.node_id == allocation.node_id && claim.task_config_id == allocation.task_config_id
        }) else {
            continue;
        };
        let claim = remaining.remove(index);
        if let Err(err) = deliver_ratification(store, allocation, claim) {
            warn!(
                claim_id = %claim.claim_id,
                allocation_id = allocation.id,
                error = ?err,
                "Failed to deliver ratification; the node's lease will kill the claim"
            );
        }
    }
}

fn deliver_ratification(store: &Store, allocation: &Allocation, claim: &SpeculativeClaim) -> R<()> {
    let task = store
        .get_task_config_by_id(allocation.task_config_id)?
        .with_context(|| format!("missing task config {}", allocation.task_config_id))?;
    let node = store.get_node(&allocation.node_id)?;
    worker_address_for_node(&node).context("worker address missing for ratification")?;
    let instance = match allocation.instance_id.as_deref() {
        Some(instance_id) => store.get_instance(instance_id)?,
        None => None,
    };
    let volumes =
        store.volume_mounts_for_allocation(&allocation.job_id, &task, instance.as_ref())?;
    let provisional = ProvisionalAllocation {
        claim_id: claim.claim_id.clone(),
        gate: claim.gate.to_wire(),
        lease_deadline_unix: 0,
        instance_id: allocation.instance_id.clone().unwrap_or_default(),
        origin: "manager".into(),
        on_refused: String::new(),
        ticket_jti: String::new(),
        session: None,
    };
    // Built with the ordinary runtime env (LEGION_ALLOC_ID and friends): from
    // here on this is a normal allocation, and the node replaces the payload
    // it staged with before starting anything.
    let mut payload = build_task_allocation(&task, allocation, instance.as_ref(), &volumes)?;
    attach_gang_env(store, &task, allocation, &mut payload)?;
    payload.provisional = Some(provisional);

    run_dispatch_future(async {
        let mut client = crate::manager::worker_plane::WorkerPlane::global()
            .client(store, &node)
            .map_err(|status| anyhow!("{}", status.message()))?;
        client
            .post_work(crate::structs::PostWorkRequest {
                allocation: Some(payload),
            })
            .await?;
        Ok::<(), anyhow::Error>(())
    })
}

/// `block_on` itself does not need `Send`; the bound is there so the type
/// system refuses a `MutexGuard` (or any other `!Send` state) held across an
/// `.await` on the dispatch path, which would stall every other dispatch on
/// that lock while a worker RPC is in flight.
fn run_dispatch_future<F>(future: F) -> R<()>
where
    F: std::future::Future<Output = R<()>> + Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            if handle.runtime_flavor() == RuntimeFlavor::CurrentThread {
                bail!("dispatch_allocations cannot block inside a current-thread runtime");
            }
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        Err(_) => {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(future)
        }
    }
}

fn worker_address_for_node(node: &crate::structs::Node) -> Option<String> {
    crate::manager::worker_plane::worker_address(node)
}

/// On the envelope payload of a session task with `suspend.durability =
/// "store"`, carry the job's session sealing keys (session durability plan
/// §6.6): the node seals and opens its sessions' published checkpoints with
/// them, with no manager on the path. Nothing for any other payload, so a
/// node holds key material only for jobs that publish session checkpoints.
pub fn attach_session_seal_keys(
    store: &Store,
    payload: &mut TaskAllocation,
    session_task: Option<&TaskConfig>,
) {
    let Some(session_task) = session_task else {
        return;
    };
    if !crate::structs::session::publishes_checkpoints(session_task) {
        return;
    }
    payload.session_seal_keys = store.keyring().session_seal_keys(&payload.job_id);
}

/// Add a gang member's identity to its payload's env
/// (docs/gang-scheduling.md, "Starting together"): `LEGION_GANG_RANK`,
/// `LEGION_GANG_SIZE`, `LEGION_GANG_MIN`, and `LEGION_GANG_LEADER_ADDR` when
/// rank 0's address is known. They come from the member's `gang_member` row,
/// fixed when it was placed, so every copy of the payload (a push, the
/// heartbeat's desired set, the work stream, a ratification) carries the same
/// values, and a running member is never updated, which would restart it.
/// Like the variables above, they win over user-declared keys. Free for a
/// task that is not a gang.
pub fn attach_gang_env(
    store: &Store,
    task: &TaskConfig,
    allocation: &Allocation,
    payload: &mut TaskAllocation,
) -> R<()> {
    if task.gang.is_none() {
        return Ok(());
    }
    let Some(member) = store.gang_member(allocation.id)? else {
        return Ok(());
    };
    let mut config: Value = serde_json::from_str(&payload.task_config)?;
    let Some(env) = config
        .as_object_mut()
        .map(|config| {
            config
                .entry("env")
                .or_insert_with(|| Value::Object(Default::default()))
        })
        .and_then(Value::as_object_mut)
    else {
        warn!(
            allocation_id = allocation.id,
            "Task config env is not an object; skipping gang env injection"
        );
        return Ok(());
    };
    for (key, value) in member.env() {
        env.insert(key.to_string(), Value::String(value));
    }
    payload.task_config = serde_json::to_string(&config)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{Instance, InstanceMode, InstanceState};
    use serde_json::json;

    fn allocation(instance_id: Option<&str>) -> Allocation {
        Allocation {
            id: 42,
            status: "placed".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
            instance_id: instance_id.map(String::from),
            network: None,
        }
    }

    fn slot_instance() -> Instance {
        Instance {
            instance_id: "job-default-1".into(),
            job_id: "job".into(),
            group: "default".into(),
            task_name: "db".into(),
            mode: InstanceMode::Slot,
            state: InstanceState::Active,
            slot_index: Some(1),
            current_alloc_id: Some(42),
            storage_handle: Some("local-dir/abc".into()),
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
            restore_from: None,
            identity_epoch: 0,
            named: true,
            promoted_from: None,
        }
    }

    /// The node acts on `suspend` and `snapshots` itself (idle reports,
    /// pressure suspends, a checkpoint-ready sandbox launch), so both ride
    /// the payload, and the worker rebuilds them with the instance it
    /// serves. Undeclared, they stay off the wire.
    #[test]
    fn suspend_and_snapshots_reach_the_worker() -> R<()> {
        let instance = Instance {
            mode: InstanceMode::Keyed,
            slot_index: None,
            instance_id: "user-42".into(),
            ..slot_instance()
        };
        let task: TaskConfig = serde_json::from_value(json!({
            "name": "workspace",
            "runtime": "sandbox",
            "config": { "image": "alpine", "command": ["sleep", "1"] },
            "stateful": { "mode": "keyed" },
            "suspend": { "checkpoint": "memory", "idle": { "after": "3m" }, "onPressure": true },
            "snapshots": { "keep": 3 },
        }))?;
        let built =
            build_task_allocation(&task, &allocation(Some("user-42")), Some(&instance), &[])?;
        assert!(!built.task_suspend.is_empty());
        assert!(!built.task_snapshots.is_empty());

        let (worker_alloc, worker_task) = crate::worker::task_manager::task_and_allocation(&built)?;
        assert_eq!(worker_task.suspend, task.suspend);
        assert_eq!(worker_task.snapshots, task.snapshots);
        assert_eq!(worker_alloc.instance_id.as_deref(), Some("user-42"));
        assert!(crate::structs::snapshot::memory_checkpoint_ask(&worker_task).is_some());

        let plain = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        let built = build_task_allocation(&plain, &allocation(None), None, &[])?;
        assert!(built.task_suspend.is_empty() && built.task_snapshots.is_empty());
        Ok(())
    }

    #[test]
    fn a_node_below_the_protocol_floor_is_fenced_out_of_provisional_dispatch() {
        // The one genuinely dangerous skew: an old worker ignores the
        // `provisional` field and would run the payload with no gate and no
        // lease. It falls back to post-commit dispatch instead.
        let old = crate::structs::Node {
            id: "node-old".into(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "now".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({ "workerAddress": "http://127.0.0.1:4848" }),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        };
        let version = crate::manager::worker_plane::worker_protocol_version;
        assert!(version(&old) < MIN_PROVISIONAL_PROTOCOL_VERSION);

        // Registration records the version in the node's dynamic attributes
        // (where a real worker's lands); reading only `config_attrs` fenced
        // every real worker out of provisional dispatch.
        let current = crate::structs::Node {
            dynamic_attrs: json!({ "protocolVersion": 2 }),
            ..old.clone()
        };
        assert!(version(&current) >= MIN_PROVISIONAL_PROTOCOL_VERSION);
        let configured = crate::structs::Node {
            config_attrs: json!({ "protocolVersion": 2 }),
            ..old
        };
        assert!(version(&configured) >= MIN_PROVISIONAL_PROTOCOL_VERSION);
    }

    #[test]
    fn worker_address_from_node_config() {
        let node = crate::structs::Node {
            id: "node-1".into(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "now".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({"workerAddress": "https://10.0.1.7:4848"}),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        };

        assert_eq!(
            crate::manager::worker_plane::worker_address(&node),
            Some("https://10.0.1.7:4848".to_string())
        );
    }

    #[test]
    fn run_dispatch_future_outside_runtime() -> R<()> {
        run_dispatch_future(async { Ok(()) })?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_dispatch_future_inside_runtime() -> R<()> {
        run_dispatch_future(async { Ok(()) })?;
        Ok(())
    }
}
