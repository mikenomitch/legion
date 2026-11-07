use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::edge_boot::{Gate, ResolvedSpeculativeStart};
use crate::structs::{
    worker_client::WorkerClient, Allocation, Instance, ProvisionalAllocation, TaskAllocation,
    TaskConfig,
};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::runtime::RuntimeFlavor;
use tokio::sync::Semaphore;
use tonic::transport::Channel;

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

pub fn build_task_allocation(
    task: &TaskConfig,
    allocation: &Allocation,
    instance: Option<&Instance>,
    volumes: &[crate::structs::VolumeMount],
) -> R<TaskAllocation> {
    build_task_allocation_with(task, allocation, instance, volumes, None)
}

/// [`build_task_allocation`] with an optional provisional block.
///
/// When `provisional` is set the payload describes work the store has not
/// recorded yet (docs/edge-boot.md): `allocation_id` is 0 and the node keys
/// the work by the claim id instead. `LEGION_ALLOC_ID` is deliberately
/// **absent** from such a payload rather than being filled with a placeholder
/// — an allocation id does not exist yet, and a workload that reported a fake
/// one would be lying about its own identity. `LEGION_CLAIM_ID` is injected in
/// its place, and `LEGION_INSTANCE_ID` is already correct from the first
/// millisecond, which is the identity a sandbox platform actually addresses.
pub fn build_task_allocation_with(
    task: &TaskConfig,
    allocation: &Allocation,
    instance: Option<&Instance>,
    volumes: &[crate::structs::VolumeMount],
    provisional: Option<ProvisionalAllocation>,
) -> R<TaskAllocation> {
    let mut config = task.config.clone();
    match provisional.as_ref() {
        Some(provisional) => inject_provisional_env(&mut config, &provisional.claim_id, instance),
        None => inject_runtime_env(&mut config, allocation, instance),
    }
    let task_config = serde_json::to_string(&config)?;
    let task_checks = serde_json::to_string(&task.checks)?;
    let task_on_unhealthy = serde_json::to_string(&task.on_unhealthy)?;
    let network = allocation
        .network
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?
        .unwrap_or_default();
    // Identity *specs* only (delivery/refresh instructions) — the tokens
    // themselves are fetched by the worker via IssueWorkloadIdentityToken and
    // never transit the desired-state payload.
    let task_identities = serde_json::to_string(&task.identities)?;
    // Worker self-fence window (the worker half of the at-most-one fencing
    // guarantee, see docs/stateful-workloads.md "Node loss and fencing").
    // Stateful tasks always carry a non-zero window — the 90s default applies
    // even without an `onLost` block. Stateless tasks carry 0 (no fencing;
    // the manager replaces them eagerly on node loss).
    let self_terminate_after_seconds = match task.stateful.as_ref() {
        Some(stateful) => stateful.resolved_on_lost()?.self_terminate_after.as_secs(),
        None => 0,
    };
    // Empty string (not "[]") when no artifacts: jobs without artifacts stay
    // byte-identical on the wire.
    let task_artifacts = if task.artifacts.is_empty() {
        String::new()
    } else {
        serde_json::to_string(&task.artifacts)?
    };
    // Declared upstreams only — the loopback ports they were assigned live on
    // `network.upstreams`. Empty string (not "{}") when the task declares no
    // upstreams, so jobs that do not use connect stay byte-identical.
    let task_connect = match task.connect.as_ref().filter(|connect| !connect.is_empty()) {
        Some(connect) => serde_json::to_string(connect)?,
        None => String::new(),
    };
    // Same rule for volumes: jobs without storage are unchanged on the wire.
    let task_volumes = if volumes.is_empty() {
        String::new()
    } else {
        serde_json::to_string(volumes)?
    };
    // As declared, without the scheduler's defaults folded in, so an isolating
    // runtime can size its sandbox only where the job actually asked for a size.
    let task_resources = match task.resources.as_object() {
        Some(resources) if !resources.is_empty() => serde_json::to_string(&task.resources)?,
        _ => String::new(),
    };
    // The declared stop ladder, carried because the worker rebuilds its
    // TaskConfig from these fields and would otherwise default every stop.
    // Empty means "undeclared", which the container runtime distinguishes
    // from a declared SIGTERM: only the silent case consults the image's own
    // STOPSIGNAL. Jobs that never declare `stop` stay byte-identical on the
    // wire.
    let task_stop = match task.stop.as_ref() {
        Some(stop) => serde_json::to_string(stop).context("Failed to serialize task stop")?,
        None => String::new(),
    };
    // The declared `network` block: mode, labels, and any inline policy. The
    // assigned ports travel separately on `network` (an `AllocNetwork`), so
    // this is only sent when it carries something the datapath reads — a
    // non-default mode, labels, or an inline chain — keeping jobs that only
    // declare ports byte-identical on the wire.
    let task_network = match task.network.as_ref() {
        Some(network)
            if network.mode != "auto"
                || !network.labels.is_empty()
                || network.declares_policy() =>
        {
            serde_json::to_string(network).context("Failed to serialize task network")?
        }
        _ => String::new(),
    };
    // Suspend and snapshots: the node acts on both itself (idle reports,
    // pressure suspends, a checkpoint-ready launch). Empty when undeclared,
    // so jobs without them stay byte-identical on the wire.
    let task_suspend = match task.suspend.as_ref() {
        Some(suspend) => serde_json::to_string(suspend).context("Failed to serialize suspend")?,
        None => String::new(),
    };
    let task_snapshots = match task.snapshots.as_ref() {
        Some(snapshots) => {
            serde_json::to_string(snapshots).context("Failed to serialize snapshots")?
        }
        None => String::new(),
    };
    // Lifecycle and restart decide what the worker's supervisor does when the
    // process exits. Without them every task ran as a service with
    // `restart = always`, so a batch task that exited 0 was relaunched forever
    // and never reported success. The restart block stays empty when
    // undeclared: the worker then derives the policy from the lifecycle.
    let task_restart = match task.restart.as_ref() {
        Some(restart) => serde_json::to_string(restart).context("Failed to serialize restart")?,
        None => String::new(),
    };
    // The node redeems boot tickets against the task config it was
    // dispatched, and the task's own mode caps an edge start's gate.
    let task_speculative_start = match task.speculative_start.as_ref() {
        Some(block) => {
            serde_json::to_string(block).context("Failed to serialize speculativeStart")?
        }
        None => String::new(),
    };
    Ok(TaskAllocation {
        allocation_id: allocation.id,
        job_id: allocation.job_id.clone(),
        job_config_id: allocation.job_config_id.clone(),
        node_id: allocation.node_id.clone(),
        task_config_id: allocation.task_config_id,
        task_name: task.name.clone(),
        task_runtime: task.runtime.clone(),
        task_config,
        task_checks,
        task_on_unhealthy,
        network,
        task_identities,
        self_terminate_after_seconds,
        task_artifacts,
        task_connect,
        task_volumes,
        task_resources,
        task_stop,
        task_network,
        provisional,
        // Filled by the heartbeat's desired-set builder, the one path that
        // feeds the worker's pressure response (`build_desired_allocations`).
        priority_band: String::new(),
        // Attached by the heartbeat builder, which knows the instance's
        // restore source (see `snapshot_restore_payload`).
        restore: None,
        task_suspend,
        task_snapshots,
        identity_epoch: instance.map_or(0, |instance| instance.identity_epoch),
        // A node still running the session adopts it as this allocation
        // (docs/session-mode.md, "Promotion"); any other node starts it.
        promoted_from_session: instance
            .and_then(|instance| instance.promoted_from.clone())
            .unwrap_or_default(),
        task_lifecycle: task.lifecycle.to_string(),
        task_restart,
        task_speculative_start,
        task_group: task.group.clone(),
        task_service: match task.service.as_ref() {
            Some(service) => {
                serde_json::to_string(service).context("Failed to serialize service")?
            }
            None => String::new(),
        },
        session_task_spec: String::new(),
        session_task_config_id: 0,
        session_seal_keys: Vec::new(),
        // An envelope's size is its row's, which an elastic envelope's node
        // grows and shrinks (docs/session-mode.md, "Elastic envelopes");
        // `task_resources` stays the declared floor.
        envelope_resources: if crate::structs::session::envelope_serves(task).is_some() {
            allocation.resources.to_string()
        } else {
            String::new()
        },
    })
}

/// On an envelope's payload, carry the session task it serves, so the node
/// can start that task's sessions from a session ticket with no manager on
/// the path (session durability plan §6.4). A no-op for every other task.
pub fn attach_session_task_spec<'a>(
    payload: &mut TaskAllocation,
    task: &TaskConfig,
    job_tasks: impl IntoIterator<Item = (i64, &'a TaskConfig)>,
) -> R<Option<&'a TaskConfig>> {
    let Some(serves) = crate::structs::session::envelope_serves(task) else {
        return Ok(None);
    };
    if let Some((id, session_task)) = job_tasks
        .into_iter()
        .find(|(_, candidate)| candidate.name == serves)
    {
        payload.session_task_spec = serde_json::to_string(session_task)?;
        payload.session_task_config_id = id;
        return Ok(Some(session_task));
    }
    Ok(None)
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

/// Runtime env for a provisional start. Mirrors [`inject_runtime_env`] with
/// `LEGION_ALLOC_ID` replaced by `LEGION_CLAIM_ID`.
fn inject_provisional_env(config: &mut Value, claim_id: &str, instance: Option<&Instance>) {
    let Some(object) = config.as_object_mut() else {
        return;
    };
    let env = object
        .entry("env")
        .or_insert_with(|| Value::Object(Default::default()));
    let Some(env) = env.as_object_mut() else {
        warn!(
            claim_id,
            "Task config env is not an object; skipping runtime env injection"
        );
        return;
    };
    // Never leave a stale or forged value behind: a provisional workload has
    // no allocation id, and must not be able to claim one.
    env.remove("LEGION_ALLOC_ID");
    env.insert(
        "LEGION_CLAIM_ID".to_string(),
        Value::String(claim_id.to_string()),
    );
    // A node builds an edge start from a task config it cached off another
    // allocation's dispatch, whose env names that allocation's instance. The
    // claim's own identity replaces it, and none is left behind without one.
    for key in [
        "LEGION_INSTANCE_ID",
        "LEGION_INSTANCE_KEY",
        "LEGION_INSTANCE_MODE",
        "LEGION_INSTANCE_SLOT",
    ] {
        env.remove(key);
    }
    if let Some(instance) = instance {
        env.insert(
            "LEGION_INSTANCE_ID".to_string(),
            Value::String(instance.instance_id.clone()),
        );
        // The id without its namespace: what a caller named the instance.
        env.insert(
            "LEGION_INSTANCE_KEY".to_string(),
            Value::String(
                crate::structs::namespace::instance_key(&instance.instance_id).to_string(),
            ),
        );
        env.insert(
            "LEGION_INSTANCE_MODE".to_string(),
            Value::String(instance.mode.to_string()),
        );
        if let Some(slot) = instance.slot_index {
            env.insert(
                "LEGION_INSTANCE_SLOT".to_string(),
                Value::String(slot.to_string()),
            );
        }
    }
}

/// The gate a task may actually reach, after applying the invariants that are
/// settings for nobody (edge boot plan §6.5).
///
/// A task is **downgraded to `stage`** — never silently, always with the
/// reason — when starting it before the commit would need something the apply
/// transaction has not produced yet:
///
/// - **Assigned ports.** `network.ports` and `connect.upstreams` are assigned
///   inside the apply, so a workload booted before it cannot know them. A
///   `stage`-gated task picks them up from the ratification payload before it
///   starts, which is why `stage` is unaffected.
/// - **Storage that cannot be fenced.** A volume a second node could attach
///   concurrently is mounted only after ratification. Node-local volumes are
///   exempt: the only possible racer is on the same node, where the worker's
///   claim table already fences it.
///
/// Returned alongside the reason so `job validate` can print it before the
/// deploy rather than leaving it to be discovered as a missing speedup.
pub fn effective_gate(task: &TaskConfig, declared: Gate) -> (Gate, Option<String>) {
    if declared <= Gate::Stage {
        return (declared, None);
    }
    if task.gang.is_some() {
        return (
            Gate::Stage,
            Some(format!(
                "task `{}` is a gang, whose members' ranks are assigned when the placement \
                 commits; gates above `stage` would boot it without them",
                task.name
            )),
        );
    }
    let declares_ports = task
        .network
        .as_ref()
        .is_some_and(|network| !network.ports.is_empty());
    if declares_ports {
        return (
            Gate::Stage,
            Some(format!(
                "task `{}` declares network ports, which are assigned when the placement \
                 commits; gates above `stage` would boot it without them",
                task.name
            )),
        );
    }
    if task
        .connect
        .as_ref()
        .is_some_and(|connect| !connect.is_empty())
    {
        return (
            Gate::Stage,
            Some(format!(
                "task `{}` declares connect upstreams, whose loopback ports are assigned \
                 when the placement commits; gates above `stage` would boot it \
                 without them",
                task.name
            )),
        );
    }
    if let Some(unfenceable) = unfenceable_storage(task) {
        return (
            Gate::Stage,
            Some(format!(
                "task `{}` claims volume `{unfenceable}`, whose driver cannot fence a \
                 concurrent attach from another node; it is mounted only after \
                 ratification",
                task.name
            )),
        );
    }
    (declared, None)
}

/// The first volume claim whose driver is not node-local, if any.
///
/// Node-local drivers are the exemption in §6.5: the only possible racer for
/// a node-local volume is on the same node, and the worker's claim table
/// already allows one live claim per instance there.
fn unfenceable_storage(task: &TaskConfig) -> Option<String> {
    const NODE_LOCAL_DRIVERS: &[&str] = &["local-dir", "host", "tmpfs"];
    let is_node_local =
        |driver: Option<&str>| NODE_LOCAL_DRIVERS.contains(&driver.unwrap_or("local-dir"));
    if let Some(storage) = task
        .stateful
        .as_ref()
        .and_then(|stateful| stateful.storage.as_ref())
    {
        if !is_node_local(storage.driver.as_deref()) {
            return Some("stateful.storage".to_string());
        }
    }
    task.volumes
        .iter()
        .find(|volume| !is_node_local(volume.driver.as_deref()))
        .map(|volume| volume.name.clone())
}

/// The resolved `speculativeStart` block for a task, with the invariant
/// downgrades of [`effective_gate`] applied.
pub fn resolved_speculative_start(task: &TaskConfig) -> R<ResolvedSpeculativeStart> {
    let mut resolved = task
        .speculative_start
        .as_ref()
        .map(|block| block.resolve())
        .transpose()?
        .unwrap_or_default();
    let (gate, downgrade) = effective_gate(task, resolved.mode);
    if let Some(reason) = downgrade {
        debug!(task = %task.name, reason, "Downgrading speculative start gate");
    }
    resolved.mode = gate;
    Ok(resolved)
}

/// Inject Legion placement environment variables into the task config’s `env`
/// map (the map exec/container plugins already read):
///
/// - `LEGION_ALLOC_ID` — always; the ephemeral placement id (churns each move).
/// - `LEGION_INSTANCE_ID` / `LEGION_INSTANCE_MODE` — stateful tasks only; the
///   stable instance id that survives reschedules and its mode (slot/keyed).
/// - `LEGION_INSTANCE_SLOT` — slot-mode only; ordinal `0..count-1`.
///
/// Legion-injected values win over user-declared env keys with the same name:
/// they are authoritative runtime facts, and letting a jobspec shadow them
/// would let a workload misreport its own identity (mirrors Nomad, whose
/// runtime variables always take precedence). Documented in
/// `docs/stateful-workloads.md`.
fn inject_runtime_env(config: &mut Value, allocation: &Allocation, instance: Option<&Instance>) {
    let Some(config) = config.as_object_mut() else {
        // Non-object task configs cannot carry an env map; plugins that accept
        // them do not read env either, so there is nothing to inject into.
        return;
    };

    let env = config
        .entry("env")
        .or_insert_with(|| Value::Object(Default::default()));
    let Some(env) = env.as_object_mut() else {
        warn!(
            allocation_id = allocation.id,
            "Task config env is not an object; skipping runtime env injection"
        );
        return;
    };

    env.insert(
        "LEGION_ALLOC_ID".to_string(),
        Value::String(allocation.id.to_string()),
    );
    if let Some(instance) = instance {
        env.insert(
            "LEGION_INSTANCE_ID".to_string(),
            Value::String(instance.instance_id.clone()),
        );
        // The id without its namespace: what a caller named the instance.
        env.insert(
            "LEGION_INSTANCE_KEY".to_string(),
            Value::String(
                crate::structs::namespace::instance_key(&instance.instance_id).to_string(),
            ),
        );
        env.insert(
            "LEGION_INSTANCE_MODE".to_string(),
            Value::String(instance.mode.to_string()),
        );
        if let Some(slot) = instance.slot_index {
            env.insert(
                "LEGION_INSTANCE_SLOT".to_string(),
                Value::String(slot.to_string()),
            );
        }
    }
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
    use crate::structs::{InstanceMode, InstanceState};
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

    fn env_from(task_allocation: &TaskAllocation) -> serde_json::Map<String, Value> {
        let config: Value =
            serde_json::from_str(&task_allocation.task_config).expect("config parses");
        config["env"].as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn injects_alloc_id_for_stateless_tasks_without_instance_vars() -> R<()> {
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        let built = build_task_allocation(&task, &allocation(None), None, &[])?;
        let env = env_from(&built);
        assert_eq!(env["LEGION_ALLOC_ID"], json!("42"));
        assert!(!env.contains_key("LEGION_INSTANCE_ID"));
        assert!(!env.contains_key("LEGION_INSTANCE_MODE"));
        assert!(!env.contains_key("LEGION_INSTANCE_SLOT"));
        Ok(())
    }

    #[test]
    fn injects_instance_env_for_slot_instances() -> R<()> {
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("db", "exec")
        };
        let built = build_task_allocation(
            &task,
            &allocation(Some("job-default-1")),
            Some(&slot_instance()),
            &[],
        )?;
        let env = env_from(&built);
        assert_eq!(env["LEGION_ALLOC_ID"], json!("42"));
        assert_eq!(env["LEGION_INSTANCE_ID"], json!("job-default-1"));
        assert_eq!(env["LEGION_INSTANCE_MODE"], json!("slot"));
        assert_eq!(env["LEGION_INSTANCE_SLOT"], json!("1"));
        Ok(())
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
    fn keyed_instances_omit_slot_env_var() -> R<()> {
        let instance = Instance {
            mode: InstanceMode::Keyed,
            slot_index: None,
            instance_id: "sandbox-7f3a".into(),
            ..slot_instance()
        };
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("sandbox", "exec")
        };
        let built = build_task_allocation(
            &task,
            &allocation(Some("sandbox-7f3a")),
            Some(&instance),
            &[],
        )?;
        let env = env_from(&built);
        assert_eq!(env["LEGION_INSTANCE_ID"], json!("sandbox-7f3a"));
        assert_eq!(env["LEGION_INSTANCE_MODE"], json!("keyed"));
        assert!(!env.contains_key("LEGION_INSTANCE_SLOT"));
        Ok(())
    }

    #[test]
    fn legion_env_wins_over_user_declared_keys() -> R<()> {
        // Legion runtime vars are authoritative facts; a jobspec must not be
        // able to make a workload misreport its own identity.
        let task = TaskConfig {
            config: json!({
                "command": ["true"],
                "env": {
                    "LEGION_INSTANCE_ID": "forged",
                    "LEGION_ALLOC_ID": "forged",
                    "USER_VAR": "kept"
                }
            }),
            ..TaskConfig::new("db", "exec")
        };
        let built = build_task_allocation(
            &task,
            &allocation(Some("job-default-1")),
            Some(&slot_instance()),
            &[],
        )?;
        let env = env_from(&built);
        assert_eq!(env["LEGION_INSTANCE_ID"], json!("job-default-1"));
        assert_eq!(env["LEGION_ALLOC_ID"], json!("42"));
        assert_eq!(env["USER_VAR"], json!("kept"));
        Ok(())
    }

    /// An edge start is built from a task config the node cached off another
    /// allocation's dispatch, so its env already names that allocation's
    /// instance. The claim's own identity must replace it.
    #[test]
    fn a_provisional_start_drops_a_cached_instance_identity() -> R<()> {
        let cached = TaskConfig {
            config: json!({
                "command": ["true"],
                "env": { "LEGION_INSTANCE_ID": "someone-else", "LEGION_INSTANCE_MODE": "keyed" }
            }),
            ..TaskConfig::new("box", "exec")
        };
        let provisional = || ProvisionalAllocation {
            claim_id: "clm_a".into(),
            ..Default::default()
        };
        let built = build_task_allocation_with(
            &cached,
            &allocation(Some("r-1")),
            None,
            &[],
            Some(provisional()),
        )?;
        let env = env_from(&built);
        assert!(!env.contains_key("LEGION_INSTANCE_ID"));
        assert_eq!(env["LEGION_CLAIM_ID"], json!("clm_a"));
        let mut instance = slot_instance();
        instance.instance_id = "r-1".into();
        let built = build_task_allocation_with(
            &cached,
            &allocation(Some("r-1")),
            Some(&instance),
            &[],
            Some(provisional()),
        )?;
        assert_eq!(env_from(&built)["LEGION_INSTANCE_ID"], json!("r-1"));
        Ok(())
    }

    #[test]
    fn stateless_tasks_carry_no_self_fence_window() -> R<()> {
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        let built = build_task_allocation(&task, &allocation(None), None, &[])?;
        assert_eq!(built.self_terminate_after_seconds, 0);
        Ok(())
    }

    #[test]
    fn stateful_tasks_carry_the_default_self_fence_window_without_on_lost() -> R<()> {
        // Even without an onLost block, stateful tasks get the 90s default —
        // fencing is on by default.
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("db", "exec")
        };
        let built = build_task_allocation(
            &task,
            &allocation(Some("job-default-1")),
            Some(&slot_instance()),
            &[],
        )?;
        assert_eq!(built.self_terminate_after_seconds, 90);
        Ok(())
    }

    #[test]
    fn stateful_tasks_carry_their_declared_self_fence_window() -> R<()> {
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                require_id: false,
                initial_instances: None,
                on_lost: Some(crate::structs::StatefulOnLostConfig {
                    self_terminate_after: Some("30s".into()),
                    replace_after: Some("2m".into()),
                    unknown_fields: Default::default(),
                }),
                storage: None,
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("db", "exec")
        };
        let built = build_task_allocation(
            &task,
            &allocation(Some("job-default-1")),
            Some(&slot_instance()),
            &[],
        )?;
        assert_eq!(built.self_terminate_after_seconds, 30);
        Ok(())
    }

    #[test]
    fn tasks_without_upstreams_carry_an_empty_connect_payload() -> R<()> {
        // Jobs that do not use connect must stay byte-identical on the wire —
        // an empty string, never "{}" — and an explicitly empty block counts
        // as "no upstreams" so it cannot make the manager do endpoint work.
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        let built = build_task_allocation(&task, &allocation(None), None, &[])?;
        assert_eq!(built.task_connect, "");

        let task = TaskConfig {
            connect: Some(crate::structs::ConnectConfig::default()),
            ..task
        };
        let built = build_task_allocation(&task, &allocation(None), None, &[])?;
        assert_eq!(built.task_connect, "");
        Ok(())
    }

    #[test]
    fn declared_upstreams_are_serialized_onto_the_allocation() -> R<()> {
        let connect: crate::structs::ConnectConfig = serde_json::from_value(json!({
            "upstreams": [
                { "to": "db" },
                { "to": "cache", "port": 6379, "balance": "least_conn" },
            ]
        }))?;
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            connect: Some(connect.clone()),
            ..TaskConfig::new("api", "exec")
        };

        let built = build_task_allocation(&task, &allocation(None), None, &[])?;
        let decoded: crate::structs::ConnectConfig = serde_json::from_str(&built.task_connect)?;
        assert_eq!(decoded, connect);
        // camelCase on the wire, like the rest of the jobspec.
        assert!(built.task_connect.contains("\"port\":6379"));
        Ok(())
    }

    #[test]
    fn injection_does_not_mutate_the_stored_task_config() -> R<()> {
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        build_task_allocation(&task, &allocation(None), None, &[])?;
        assert!(task.config.get("env").is_none());
        Ok(())
    }

    // ---------------------------------------------------------------
    // Edge boot (docs/edge-boot.md)
    // ---------------------------------------------------------------

    fn speculative(mode: &str) -> Option<crate::structs::edge_boot::SpeculativeStartConfig> {
        Some(crate::structs::edge_boot::SpeculativeStartConfig {
            mode: Some(mode.into()),
            ..Default::default()
        })
    }

    #[test]
    fn a_task_without_the_block_speculates_nothing_and_changes_no_payload() -> R<()> {
        // The byte-for-byte promise: a job that did not opt in must produce
        // exactly the payload it produced before this feature existed.
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        assert_eq!(resolved_speculative_start(&task)?.mode, Gate::Off);
        let built = build_task_allocation(&task, &allocation(None), None, &[])?;
        assert!(built.provisional.is_none());
        Ok(())
    }

    #[test]
    fn a_provisional_payload_carries_a_claim_id_and_no_alloc_id() -> R<()> {
        // An allocation id does not exist before the commit, and a workload
        // that reported a fake one would be lying about its own identity.
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            speculative_start: speculative("boot"),
            ..TaskConfig::new("workspace", "exec")
        };
        let mut alloc = allocation(None);
        alloc.id = 0;
        let built = build_task_allocation_with(
            &task,
            &alloc,
            None,
            &[],
            Some(ProvisionalAllocation {
                claim_id: "clm_9f2c".into(),
                gate: Gate::Boot.to_wire(),
                lease_deadline_unix: 1_770_000_005,
                instance_id: "user-42".into(),
                origin: "edge".into(),
                on_refused: "kill".into(),
                ticket_jti: "tkt_1".into(),
                session: None,
            }),
        )?;
        assert_eq!(built.allocation_id, 0);
        let config: Value = serde_json::from_str(&built.task_config)?;
        assert_eq!(config["env"]["LEGION_CLAIM_ID"], json!("clm_9f2c"));
        assert!(config["env"].get("LEGION_ALLOC_ID").is_none());
        assert_eq!(built.provisional.expect("block").gate, Gate::Boot.to_wire());
        Ok(())
    }

    #[test]
    fn a_forged_alloc_id_cannot_survive_into_a_provisional_payload() {
        // A jobspec must not be able to make an unratified workload claim an
        // allocation id it does not have.
        let mut config = json!({
            "command": ["true"],
            "env": { "LEGION_ALLOC_ID": "8123" }
        });
        inject_provisional_env(&mut config, "clm_a", None);
        assert!(config["env"].get("LEGION_ALLOC_ID").is_none());
        assert_eq!(config["env"]["LEGION_CLAIM_ID"], json!("clm_a"));
    }

    #[test]
    fn declared_ports_cap_the_gate_at_stage() -> R<()> {
        // Ports are assigned inside the apply, so a workload booted before it
        // cannot know them. `stage` is unaffected: it picks them up from the
        // ratification payload before it starts.
        let network: crate::structs::network::NetworkConfig =
            serde_json::from_value(json!({ "ports": [{ "name": "http", "to": 8080 }] }))?;
        let task = TaskConfig {
            speculative_start: speculative("serve"),
            network: Some(network),
            ..TaskConfig::new("api", "exec")
        };
        let (gate, reason) = effective_gate(&task, Gate::Serve);
        assert_eq!(gate, Gate::Stage);
        assert!(reason.expect("reason").contains("network ports"));
        assert_eq!(resolved_speculative_start(&task)?.mode, Gate::Stage);
        Ok(())
    }

    #[test]
    fn a_gang_caps_the_gate_at_stage() {
        // A member's rank is assigned with its allocation row, so a member
        // booted before the commit could not know it.
        let task = TaskConfig {
            speculative_start: speculative("boot"),
            gang: Some(crate::structs::GangConfig::default()),
            ..TaskConfig::new("trainer", "exec")
        };
        let (gate, reason) = effective_gate(&task, Gate::Boot);
        assert_eq!(gate, Gate::Stage);
        assert!(reason.expect("reason").contains("is a gang"));
    }

    #[test]
    fn connect_upstreams_cap_the_gate_at_stage() -> R<()> {
        let connect: crate::structs::ConnectConfig =
            serde_json::from_value(json!({ "upstreams": [{ "to": "db" }] }))?;
        let task = TaskConfig {
            speculative_start: speculative("boot"),
            connect: Some(connect),
            ..TaskConfig::new("api", "exec")
        };
        let (gate, reason) = effective_gate(&task, Gate::Boot);
        assert_eq!(gate, Gate::Stage);
        assert!(reason.expect("reason").contains("connect upstreams"));
        Ok(())
    }

    #[test]
    fn unfenceable_storage_caps_the_gate_at_stage() -> R<()> {
        // Storage a second node could attach concurrently is mounted only
        // after ratification, so nothing above `stage` may run against it.
        let task = TaskConfig {
            speculative_start: speculative("boot"),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Keyed,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    driver: Some("nfs".into()),
                    mode: None,
                    params: None,
                    path: "/workspace".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                    unknown_fields: Default::default(),
                }),
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("workspace", "exec")
        };
        let (gate, reason) = effective_gate(&task, Gate::Boot);
        assert_eq!(gate, Gate::Stage);
        assert!(reason.expect("reason").contains("fence"));
        Ok(())
    }

    #[test]
    fn node_local_storage_is_exempt_from_the_downgrade() -> R<()> {
        // The only possible racer for a node-local volume is on the same
        // node, where the worker's claim table already fences it.
        let task = TaskConfig {
            speculative_start: speculative("boot"),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Keyed,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    driver: None, // local-dir, the default
                    mode: None,
                    params: None,
                    path: "/workspace".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                    unknown_fields: Default::default(),
                }),
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("workspace", "exec")
        };
        assert_eq!(effective_gate(&task, Gate::Boot).0, Gate::Boot);
        assert_eq!(resolved_speculative_start(&task)?.mode, Gate::Boot);
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
