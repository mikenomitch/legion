//! Simulated worker fleets: one process multiplexing N registered nodes
//! over shared gRPC channels (`legion start worker --simulate N`).
//!
//! Each simulated node speaks the unmodified worker-plane wire contract at
//! real-worker cadences — pre-register, register, heartbeats at the
//! worker-type's cadence (`heartbeatSeconds`, resolved and jittered by the
//! same `daemon` helpers a real worker uses), health-check report every
//! `WORKER_NODE_CHECK_DURATION_SEC`, re-register on `Unauthenticated` — so
//! manager-side load per simulated node matches load per real idle node.
//! No task ever executes: dispatched allocations are recorded and echoed in
//! heartbeats (after `--ready-after`), nothing more.
//!
//! See `docs/worker-simulation.md` and
//! `internal_docs/plans/worker-simulation-plan.md`.

use crate::prelude::*;

use crate::structs::token::JoinToken;
use crate::structs::{
    manager_client::ManagerClient, worker_server::Worker, worker_server::WorkerServer,
    AllocationHealth, CheckpointRequest, ExecClientFrame, GetTaskLogsReply, GetTaskLogsRequest,
    HealthCheckRequest, HeartbeatRequest, PostWorkReply, PostWorkRequest, RestartWorkerReply,
    RestartWorkerRequest, SnapshotReport, TaskAllocation, WorkerConfig, WorkerPreRegisterRequest,
    WorkerRegisterRequest,
};
use crate::worker::daemon::{
    connect_manager_with_retry, heartbeat_interval_for, jittered_heartbeat_interval,
    start_worker_telemetry, WorkerCredentials,
};

use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tonic::{transport::Channel, Request, Response, Status, Streaming};

/// How many simulated nodes share one gRPC channel. HTTP/2 multiplexes
/// requests over one connection, so a handful of connections carries
/// thousands of nodes — the same shape nomad-nodesim uses.
const SIM_NODES_PER_CHANNEL: usize = 256;
/// Never open more than this many channels, however large the fleet.
const SIM_MAX_CHANNELS: usize = 16;
/// Fleet-status summary log cadence.
const SIM_SUMMARY_INTERVAL: Duration = Duration::from_secs(30);
/// Heartbeat retry bounds — same values as the real worker loop.
const SIM_RETRY_MIN: Duration = Duration::from_millis(500);
const SIM_RETRY_MAX: Duration = Duration::from_secs(10);

/// Options for `legion start worker --simulate N`.
#[derive(Debug, Clone)]
pub struct SimulateOptions {
    /// Total simulated nodes across all profiles.
    pub count: usize,
    /// Path to a JSON file with a `node_profiles` array (the benchmark
    /// scenario schema); `None` = one uniform default profile.
    pub profiles_path: Option<String>,
    /// Node ids are `<prefix>-<profile>-<idx>` — deterministic, so a
    /// restarted process re-registers the same nodes.
    pub name_prefix: String,
    /// Delay between receiving a desired allocation and echoing it as
    /// running in heartbeats (the KWOK stage-delay analog).
    pub ready_after: Duration,
    /// Registration ramp, nodes per second.
    pub register_rate: f64,
    /// Host the shared stub worker server advertises for dispatch
    /// (`workerAddress` of every node in this process).
    pub advertise_host: String,
    /// Fake the checkpoint capability: advertise `checkpoint`, fingerprint
    /// as a Firecracker sandbox host, answer every `capture` and `delete`
    /// request with a successful report, and report each restore payload as
    /// restored once the allocation is ready. Lets a load generator drive
    /// suspend, resume, and fork through the manager with no runtime.
    pub checkpoints: bool,
    /// Called for every allocation a node learns about, the first time it
    /// does. A load generator embedding the fleet uses it to time delivery.
    pub observer: Option<SimObserver>,
}

impl Default for SimulateOptions {
    fn default() -> Self {
        SimulateOptions {
            count: 1,
            profiles_path: None,
            name_prefix: "sim".into(),
            ready_after: Duration::ZERO,
            register_rate: 100.0,
            advertise_host: DEF_MANAGER_HOSTNAME.to_string(),
            checkpoints: false,
            observer: None,
        }
    }
}

/// How a simulated node learned about an allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimDelivery {
    /// The manager's `PostWork` push.
    Push,
    /// The desired set in a heartbeat reply.
    Heartbeat,
}

impl SimDelivery {
    pub fn as_str(self) -> &'static str {
        match self {
            SimDelivery::Push => "push",
            SimDelivery::Heartbeat => "heartbeat",
        }
    }
}

/// One allocation reaching a simulated node for the first time.
#[derive(Debug, Clone)]
pub struct SimArrival {
    pub node_id: String,
    pub allocation_id: i64,
    /// `LEGION_INSTANCE_ID` from the payload's env, empty for allocations
    /// without an instance.
    pub instance_id: String,
    /// The checkpoint the allocation restores from, when it carries a
    /// restore payload.
    pub restore_snapshot_id: Option<String>,
    pub via: SimDelivery,
    pub at: Instant,
}

/// Callback for [`SimArrival`]s. Runs on the delivering task, so it must be
/// cheap (a channel send).
#[derive(Clone)]
pub struct SimObserver(pub Arc<dyn Fn(SimArrival) + Send + Sync>);

impl std::fmt::Debug for SimObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SimObserver")
    }
}

/// One class of simulated node. Field-compatible with the benchmark
/// scenario `node_profiles` entries (`tests/benchmark_support/mod.rs`), so
/// a scenario file can be passed to `--profiles` directly. `count` is a
/// *weight*: `--simulate N` is distributed across profiles proportionally.
#[derive(Debug, Clone, Deserialize)]
pub struct SimNodeProfile {
    pub name: String,
    #[serde(default = "default_weight")]
    pub count: usize,
    /// Fingerprinted memory, GiB.
    #[serde(default = "default_memory_gb")]
    pub memory_gb: u64,
    /// Fingerprinted CPU count, 3 GHz each.
    #[serde(default = "default_cpus")]
    pub cpus: u32,
    /// Extra `config_attrs` (region, zone, class, ...).
    #[serde(default)]
    pub attrs: BTreeMap<String, Value>,
    /// Optional per-profile allocation cap (`maxAllocations` attr).
    #[serde(default)]
    pub max_allocations: Option<u32>,
}

fn default_weight() -> usize {
    1
}
fn default_memory_gb() -> u64 {
    32
}
fn default_cpus() -> u32 {
    4
}

impl Default for SimNodeProfile {
    fn default() -> Self {
        SimNodeProfile {
            name: "default".into(),
            count: 1,
            memory_gb: default_memory_gb(),
            cpus: default_cpus(),
            attrs: BTreeMap::new(),
            max_allocations: None,
        }
    }
}

/// The subset of a profiles file (or benchmark scenario file) we read.
#[derive(Debug, Deserialize)]
struct ProfilesFile {
    node_profiles: Vec<SimNodeProfile>,
}

pub fn load_profiles(path: &str) -> R<Vec<SimNodeProfile>> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read profiles file {path}"))?;
    let parsed: ProfilesFile = serde_json::from_str(&raw)
        .with_context(|| format!("{path}: expected a JSON object with a `node_profiles` array"))?;
    if parsed.node_profiles.is_empty() {
        bail!("{path}: `node_profiles` is empty");
    }
    Ok(parsed.node_profiles)
}

/// Distribute `total` nodes across profiles proportionally to their
/// `count` weights (largest-remainder rounding, ties to the earlier
/// profile). Every profile with nonzero weight gets its exact share; the
/// result always sums to `total`.
pub fn distribute(total: usize, profiles: &[SimNodeProfile]) -> R<Vec<(SimNodeProfile, usize)>> {
    let weight_sum: usize = profiles.iter().map(|profile| profile.count).sum();
    if weight_sum == 0 {
        bail!("profile weights (`count` fields) sum to zero");
    }
    let mut shares: Vec<(usize, usize, usize)> = profiles
        .iter()
        .enumerate()
        .map(|(idx, profile)| {
            let exact = total * profile.count;
            (idx, exact / weight_sum, exact % weight_sum)
        })
        .collect();
    let assigned: usize = shares.iter().map(|(_, base, _)| base).sum();
    // Hand the remainder out by descending fractional part, index as the
    // deterministic tie-break.
    let mut by_remainder: Vec<usize> = (0..shares.len()).collect();
    by_remainder.sort_by(|&a, &b| shares[b].2.cmp(&shares[a].2).then(a.cmp(&b)));
    for idx in by_remainder.into_iter().take(total - assigned) {
        shares[idx].1 += 1;
    }
    Ok(shares
        .into_iter()
        .map(|(idx, share, _)| (profiles[idx].clone(), share))
        .collect())
}

/// Synthesized fingerprint for a profile — the same shape a real worker's
/// `fingerprint_node()` produces for the fields the scheduler reads
/// (`memory_total` bytes-as-string, `cpus` map with `frequency`), plus
/// honest `os`/`arch` and an empty `volumeDrivers` (a sim node runs no
/// volume drivers, so volume-requiring jobs never place on it).
fn fingerprint_for(profile: &SimNodeProfile, checkpoints: bool) -> Value {
    let mut cpus = Map::new();
    for idx in 1..=profile.cpus {
        cpus.insert(format!("cpu-{idx}"), json!({ "frequency": "3000" }));
    }
    let mut fingerprint = json!({
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "memory_total": (profile.memory_gb * 1024 * 1024 * 1024).to_string(),
        "cpus": cpus,
        "volumeDrivers": {},
    });
    if checkpoints {
        // What a Firecracker sandbox host fingerprints, so the manager's
        // memory-checkpoint compatibility checks pass for every sim node
        // alike and a resume can land on any of them.
        for (key, value) in [
            ("sandbox.vmm", "firecracker"),
            ("sandbox.vmm.version", "1.12.0"),
            ("cpu.vendor", "GenuineIntel"),
            ("cpu.model", "6:106:6"),
            ("cpu.features", "3f9a0c21"),
        ] {
            fingerprint[key] = json!(value);
        }
    }
    fingerprint
}

/// `config_attrs` for a sim node: worker-type attributes, then profile
/// attrs (profile wins), then the resource/limit keys the scheduler reads,
/// then the markers every sim node carries — `simulated: true` and the
/// shared `workerAddress` of this process's stub server.
fn config_attrs_for(
    profile: &SimNodeProfile,
    config: &WorkerConfig,
    worker_address: &str,
) -> Value {
    let mut attrs = Map::new();
    for (key, value) in &config.attributes {
        attrs.insert(key.clone(), Value::String(value.clone()));
    }
    for (key, value) in &profile.attrs {
        attrs.insert(key.clone(), value.clone());
    }
    let resources = serde_json::from_str(&config.resources).unwrap_or_else(|_| json!({}));
    let reserved = serde_json::from_str(&config.reserved_resources).unwrap_or_else(|_| json!({}));
    attrs.insert("resources".into(), resources);
    attrs.insert("reservedResources".into(), reserved);
    if let Some(max_allocations) = profile.max_allocations.or(config.max_allocations) {
        attrs.insert("maxAllocations".into(), json!(max_allocations));
    }
    attrs.insert("simulated".into(), json!(true));
    attrs.insert("workerAddress".into(), Value::String(worker_address.into()));
    // The cadence contract is SHARED with the real worker, not forked: a sim
    // node reports the interval it will beat at exactly as a real one does, so
    // the manager derives the same lost window for it and the benchmark rig
    // exercises the real arithmetic rather than a parallel implementation.
    attrs.insert(
        "heartbeatSeconds".into(),
        json!(crate::worker::daemon::heartbeat_interval_for(config).as_secs()),
    );
    Value::Object(attrs)
}

/// Synthesized health-check report (the sim analog of `run_health_checks`).
fn health_checks_json() -> String {
    json!({ "simulated": "pass" }).to_string()
}

/// One simulated node's live state.
struct SimNode {
    name: String,
    pool: String,
    config_attrs: String,
    fingerprint_attrs: String,
    credentials: WorkerCredentials,
    /// Desired allocations and when each was first seen (for
    /// `--ready-after`). Fed by heartbeat replies and `PostWork` dispatch.
    allocations: Mutex<HashMap<i64, SimAlloc>>,
    /// Checkpoint reports owed to the manager (`checkpoints` mode).
    reports: Mutex<SimReports>,
    /// How many allocations this node reported on its last heartbeat. Only
    /// the *delta* against it feeds the fleet-wide gauge, so keeping the
    /// process-wide total current costs O(1) per heartbeat instead of a
    /// walk over every node in the fleet.
    reported_acked: AtomicUsize,
}

/// What a simulated node keeps per allocation.
#[derive(Debug, Clone)]
struct SimAlloc {
    first_seen: Instant,
    job_id: String,
    task_name: String,
    instance_id: String,
    /// The checkpoint this allocation restores from, until the node has
    /// reported the restore.
    restore: Option<String>,
    /// The restore was reported.
    restored: bool,
}

impl SimAlloc {
    fn from_payload(allocation: &TaskAllocation, now: Instant) -> Self {
        SimAlloc {
            first_seen: now,
            job_id: allocation.job_id.clone(),
            task_name: allocation.task_name.clone(),
            instance_id: payload_instance_id(allocation),
            restore: allocation
                .restore
                .as_ref()
                .map(|restore| restore.snapshot_id.clone()),
            restored: false,
        }
    }
}

/// `LEGION_INSTANCE_ID` from a payload's env, where the manager injects it.
fn payload_instance_id(allocation: &TaskAllocation) -> String {
    serde_json::from_str::<Value>(&allocation.task_config)
        .ok()
        .and_then(|config| {
            config
                .get("env")?
                .get("LEGION_INSTANCE_ID")?
                .as_str()
                .map(str::to_string)
        })
        .unwrap_or_default()
}

/// A node's checkpoint channel state (`checkpoints` mode). The manager
/// re-sends a request until its report is applied, so a node answers each
/// `(snapshot, op)` once and re-sends the recorded report while the manager
/// keeps asking — the same dedupe contract a real worker keeps.
#[derive(Debug, Default)]
struct SimReports {
    /// Reports for the next heartbeat.
    pending: Vec<SnapshotReport>,
    /// Every report this node has made, by `(snapshot_id, op)`, for the
    /// requests the manager still sends.
    answered: HashMap<(String, String), SnapshotReport>,
}

impl SimNode {
    /// Adopt allocations this node did not hold yet. Returns the ones that
    /// were new, for the observer.
    fn accept_allocations<'a>(
        &self,
        payloads: impl IntoIterator<Item = &'a TaskAllocation>,
    ) -> Vec<&'a TaskAllocation> {
        let now = Instant::now();
        let mut allocations = self.allocations.lock().expect("sim allocations lock");
        let mut adopted = Vec::new();
        for payload in payloads {
            match allocations.entry(payload.allocation_id) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(SimAlloc::from_payload(payload, now));
                    adopted.push(payload);
                }
                std::collections::hash_map::Entry::Occupied(mut held) => {
                    // A later copy may carry a restore payload the first
                    // lacked; a restore already reported is not redone.
                    let held = held.get_mut();
                    if held.restore.is_none() && !held.restored {
                        held.restore = payload
                            .restore
                            .as_ref()
                            .map(|restore| restore.snapshot_id.clone());
                    }
                }
            }
        }
        adopted
    }

    /// Reconcile against the manager's desired set: keep ids still
    /// desired, adopt new ones, drop the rest. Returns the allocations that
    /// were new. The next `acked_ids` call reflects the change.
    fn reconcile_allocations<'a>(&self, desired: &'a [TaskAllocation]) -> Vec<&'a TaskAllocation> {
        {
            let ids: HashSet<i64> = desired
                .iter()
                .map(|allocation| allocation.allocation_id)
                .collect();
            let mut allocations = self.allocations.lock().expect("sim allocations lock");
            allocations.retain(|id, _| ids.contains(id));
        }
        self.accept_allocations(desired)
    }

    /// Allocation ids old enough to report as running.
    fn acked_ids(&self, ready_after: Duration) -> Vec<i64> {
        let allocations = self.allocations.lock().expect("sim allocations lock");
        allocations
            .iter()
            .filter(|(_, alloc)| alloc.first_seen.elapsed() >= ready_after)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Restore reports for allocations that are ready and carried a restore
    /// payload, once each.
    fn take_restore_reports(&self, ready_after: Duration) -> Vec<SnapshotReport> {
        let mut allocations = self.allocations.lock().expect("sim allocations lock");
        let mut reports = Vec::new();
        for (id, alloc) in allocations.iter_mut() {
            if alloc.first_seen.elapsed() < ready_after {
                continue;
            }
            if let Some(snapshot_id) = alloc.restore.take() {
                alloc.restored = true;
                reports.push(SnapshotReport {
                    snapshot_id,
                    op: "restore".into(),
                    ok: true,
                    duration_ms: 1,
                    allocation_id: *id,
                    ..Default::default()
                });
            }
        }
        reports
    }

    /// Answer the manager's checkpoint requests. New work is "done" at once;
    /// work answered before re-sends its recorded report. Answers for
    /// requests the manager stopped sending are forgotten.
    fn answer_checkpoint_requests(&self, requests: &[CheckpointRequest]) {
        let allocations = self.allocations.lock().expect("sim allocations lock");
        let mut reports = self.reports.lock().expect("sim reports lock");
        let asked: HashSet<(String, String)> = requests
            .iter()
            .map(|request| (request.snapshot_id.clone(), request.op.clone()))
            .collect();
        reports.answered.retain(|key, _| asked.contains(key));
        for request in requests {
            let key = (request.snapshot_id.clone(), request.op.clone());
            let report = match reports.answered.get(&key) {
                Some(recorded) => recorded.clone(),
                None => {
                    let report =
                        sim_checkpoint_report(request, allocations.get(&request.allocation_id));
                    reports.answered.insert(key, report.clone());
                    report
                }
            };
            reports.pending.push(report);
        }
    }
}

/// The report a simulated node makes for one checkpoint request.
fn sim_checkpoint_report(request: &CheckpointRequest, alloc: Option<&SimAlloc>) -> SnapshotReport {
    let mut report = SnapshotReport {
        snapshot_id: request.snapshot_id.clone(),
        op: request.op.clone(),
        allocation_id: request.allocation_id,
        duration_ms: 1,
        ..Default::default()
    };
    match request.op.as_str() {
        "capture" => match alloc {
            Some(alloc) => {
                report.ok = true;
                report.total_bytes = 1 << 20;
                report.manifest_json = json!({
                    "apiVersion": 1,
                    "id": request.snapshot_id,
                    "jobId": alloc.job_id,
                    "task": alloc.task_name,
                    "originInstance": alloc.instance_id,
                    "originAllocId": request.allocation_id,
                    "runtime": if request.kind == "memory" { "sandbox" } else { "exec" },
                    "kind": request.kind,
                    "createdAt": chrono::Utc::now().to_rfc3339(),
                })
                .to_string();
            }
            None => {
                report.error = format!(
                    "simulated node holds no allocation {}",
                    request.allocation_id
                );
            }
        },
        "delete" => report.ok = true,
        other => {
            report.error = format!("simulated nodes cannot {other} checkpoints");
        }
    }
    report
}

/// Shared fleet state: node lookup for dispatch routing plus counters for
/// the summary log line.
struct SimFleet {
    nodes: HashMap<String, Arc<SimNode>>,
    /// Resolved like a real worker's (worker-type `heartbeatSeconds` via
    /// `daemon::heartbeat_interval_for`), and jittered per sleep the same
    /// way — the cadence contract is shared, not forked.
    heartbeat_interval: Duration,
    registered: AtomicU64,
    heartbeats_ok: AtomicU64,
    heartbeats_failed: AtomicU64,
    /// Allocations reported as running across the whole fleet, maintained
    /// by per-node deltas and mirrored into
    /// `legion_worker_allocations_running`. The summary loop re-derives it
    /// from the authoritative per-node sets, so a lost update corrects
    /// itself within one summary interval rather than drifting forever.
    reported_allocations: AtomicI64,
    /// `SimulateOptions::checkpoints`.
    checkpoints: bool,
    /// `SimulateOptions::observer`.
    observer: Option<SimObserver>,
}

impl SimFleet {
    /// Tell the observer, if any, about allocations a node just adopted.
    fn observe(&self, node: &SimNode, adopted: &[&TaskAllocation], via: SimDelivery) {
        let Some(observer) = &self.observer else {
            return;
        };
        let at = Instant::now();
        for payload in adopted {
            (observer.0)(SimArrival {
                node_id: node.name.clone(),
                allocation_id: payload.allocation_id,
                instance_id: payload_instance_id(payload),
                restore_snapshot_id: payload
                    .restore
                    .as_ref()
                    .map(|restore| restore.snapshot_id.clone()),
                via,
                at,
            });
        }
    }

    fn acked_total(&self, ready_after: Duration) -> usize {
        self.nodes
            .values()
            .map(|node| node.acked_ids(ready_after).len())
            .sum()
    }

    /// Fold one node's just-reported allocation count into the fleet-wide
    /// gauge. O(1): only the change against what this node last reported
    /// moves the total.
    fn record_reported_allocations(&self, node: &SimNode, reported: usize) {
        let previous = node.reported_acked.swap(reported, Ordering::Relaxed);
        let delta = reported as i64 - previous as i64;
        if delta == 0 {
            return;
        }
        let total = self
            .reported_allocations
            .fetch_add(delta, Ordering::Relaxed)
            + delta;
        crate::manager::telemetry::worker_metrics()
            .allocations_running
            .set(total);
    }

    /// Re-derive the fleet gauges from the authoritative per-node state.
    /// Called on the summary cadence: both incremental paths above are racy
    /// read-modify-writes against a gauge, so this bounds any drift to one
    /// summary interval instead of letting it accumulate.
    ///
    /// It sums what each node last *reported*, not what each node currently
    /// holds — that is both the invariant the deltas maintain (so the repair
    /// cannot itself desync the total) and what the family means: "as
    /// reported on its last heartbeat".
    fn resync_gauges(&self) {
        let reported: i64 = self
            .nodes
            .values()
            .map(|node| node.reported_acked.load(Ordering::Relaxed) as i64)
            .sum();
        self.reported_allocations.store(reported, Ordering::Relaxed);
        let metrics = crate::manager::telemetry::worker_metrics();
        metrics.allocations_running.set(reported);
        metrics
            .simulated_nodes
            .set(self.registered.load(Ordering::Relaxed) as i64);
    }
}

/// The per-process stub `Worker` service. `PostWork` routes on the
/// payload's `node_id` and records the allocation as desired — pull via
/// the next heartbeat reply would deliver it anyway, but accepting push
/// keeps the manager's dispatch path (a real per-placement cost) in any
/// benchmark measurement. Everything else is `Unimplemented`: a sim node
/// has no logs, no exec, and restarting one node of N makes no sense.
struct SimWorkerService {
    fleet: Arc<SimFleet>,
}

#[tonic::async_trait]
impl Worker for SimWorkerService {
    async fn post_work(
        &self,
        request: Request<PostWorkRequest>,
    ) -> Result<Response<PostWorkReply>, Status> {
        let allocation = request
            .into_inner()
            .allocation
            .ok_or_else(|| Status::invalid_argument("Missing allocation"))?;
        let node = self
            .fleet
            .nodes
            .get(&allocation.node_id)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "no simulated node {} in this process",
                    allocation.node_id
                ))
            })?
            .clone();
        let adopted = node.accept_allocations([&allocation]);
        self.fleet.observe(&node, &adopted, SimDelivery::Push);
        Ok(Response::new(PostWorkReply {
            message: "Task accepted (simulated)".into(),
        }))
    }

    async fn get_task_logs(
        &self,
        _request: Request<GetTaskLogsRequest>,
    ) -> Result<Response<GetTaskLogsReply>, Status> {
        Err(Status::unimplemented(
            "simulated workers run no tasks and capture no logs",
        ))
    }

    type StartClaimStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<crate::structs::StartClaimReply, Status>>
                + Send
                + 'static,
        >,
    >;

    /// Simulated nodes never accept edge starts. The simulator exists to load
    /// the scheduler; edge boot is a node-local execution path with no
    /// scheduler cost to model, and pretending to accept a claim would report
    /// capacity the fleet does not have.
    async fn start_claim(
        &self,
        _request: Request<crate::structs::StartClaimRequest>,
    ) -> Result<Response<Self::StartClaimStream>, Status> {
        Err(Status::unimplemented(
            "simulated workers do not accept edge starts",
        ))
    }

    type ExecTaskStream = crate::worker::exec::ExecTaskStream;

    async fn exec_task(
        &self,
        _request: Request<Streaming<ExecClientFrame>>,
    ) -> Result<Response<Self::ExecTaskStream>, Status> {
        Err(Status::unimplemented(
            "simulated workers run no tasks; exec is not available",
        ))
    }

    async fn restart_worker(
        &self,
        _request: Request<RestartWorkerRequest>,
    ) -> Result<Response<RestartWorkerReply>, Status> {
        Err(Status::unimplemented(
            "restarting a simulated node would restart the whole fleet process",
        ))
    }
}

/// A launched fleet: observability for tests and drivers, plus the point
/// of control for tearing the whole fleet down (the storm scenario's
/// "kill n nodes" is "kill sim processes / abort fleets").
pub struct SimFleetRun {
    fleet: Arc<SimFleet>,
    ready_after: Duration,
    worker_address: String,
    node_tasks: Vec<tokio::task::JoinHandle<()>>,
    server_task: tokio::task::JoinHandle<R<()>>,
    summary_task: tokio::task::JoinHandle<()>,
}

impl SimFleetRun {
    /// Nodes that have completed registration.
    pub fn registered(&self) -> u64 {
        self.fleet.registered.load(Ordering::Relaxed)
    }

    /// The stub worker endpoint every node in this fleet advertises.
    pub fn worker_address(&self) -> &str {
        &self.worker_address
    }

    /// Allocations currently held and old enough to report as running.
    pub fn acked_allocations(&self) -> usize {
        self.fleet.acked_total(self.ready_after)
    }

    /// Successful heartbeats since launch.
    pub fn heartbeats_ok(&self) -> u64 {
        self.fleet.heartbeats_ok.load(Ordering::Relaxed)
    }

    /// Runs until the stub server dies (i.e. forever on the happy path —
    /// node loops retry their manager forever, like a real worker).
    pub async fn wait(self) -> R<()> {
        self.server_task
            .await
            .context("stub worker server task panicked")??;
        Ok(())
    }

    /// Aborts every node loop, the stub server, and the summary loop. The
    /// manager sees the same thing as a crashed worker host: heartbeats
    /// stop, nodes go lost on the usual schedule.
    pub fn abort(&self) {
        for task in &self.node_tasks {
            task.abort();
        }
        self.server_task.abort();
        self.summary_task.abort();
    }
}

/// Run a simulated fleet against the manager in `token`. Never returns on
/// the happy path (like `run_worker`): heartbeat loops retry forever.
#[instrument(err, skip_all, fields(count = opts.count))]
pub async fn run_simulated_workers(token: &str, opts: SimulateOptions) -> R<()> {
    let join_token = JoinToken::decode(token)?;
    let run = launch_simulated_fleet(
        join_token.addr,
        join_token.worker_type,
        Some(join_token.secret),
        opts,
    )
    .await?;
    run.wait().await
}

/// Token-less entry point (tests, embedding): manager address + worker
/// type + optional join secret. Returns once the fleet is launched —
/// registration proceeds in the background at the configured ramp.
#[instrument(err, skip_all)]
pub async fn launch_simulated_fleet(
    manager_addr: String,
    worker_type: String,
    join_secret: Option<String>,
    opts: SimulateOptions,
) -> R<SimFleetRun> {
    if opts.count == 0 {
        bail!("--simulate requires at least 1 node");
    }
    let profiles = match &opts.profiles_path {
        Some(path) => load_profiles(path)?,
        None => vec![SimNodeProfile::default()],
    };
    let shares = distribute(opts.count, &profiles)?;

    info!(
        %manager_addr,
        %worker_type,
        count = opts.count,
        profiles = profiles.len(),
        "Simulated fleet connecting to manager"
    );

    // Shared channel pool, round-robin assigned per node.
    let channel_count = opts
        .count
        .div_ceil(SIM_NODES_PER_CHANNEL)
        .clamp(1, SIM_MAX_CHANNELS);
    let mut clients: Vec<ManagerClient<Channel>> = Vec::with_capacity(channel_count);
    for _ in 0..channel_count {
        clients.push(connect_manager_with_retry(&manager_addr).await?);
    }

    // One pre-register fetches the worker-type config (pool, resource
    // defaults) for the whole fleet.
    let pre_register = clients[0]
        .clone()
        .worker_pre_register(Request::new(WorkerPreRegisterRequest {
            worker_type: worker_type.clone(),
            join_token: join_secret.clone(),
        }))
        .await
        .context("pre-registration failed (is the worker type known to the manager?)")?;
    let config = pre_register
        .into_inner()
        .config
        .context("No config found")?;

    // Stub worker server on an ephemeral port; every node in this process
    // advertises it as its workerAddress.
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0")
        .await
        .context("failed to bind the stub worker port")?;
    let worker_address = format!(
        "http://{}:{}",
        opts.advertise_host,
        listener.local_addr()?.port()
    );

    // Build the fleet.
    let mut nodes: HashMap<String, Arc<SimNode>> = HashMap::new();
    for (profile, share) in &shares {
        let fingerprint = fingerprint_for(profile, opts.checkpoints).to_string();
        let config_attrs = config_attrs_for(profile, &config, &worker_address).to_string();
        for idx in 0..*share {
            let name = format!("{}-{}-{idx}", opts.name_prefix, profile.name);
            nodes.insert(
                name.clone(),
                Arc::new(SimNode {
                    name,
                    pool: config.pool.clone(),
                    config_attrs: config_attrs.clone(),
                    fingerprint_attrs: fingerprint.clone(),
                    credentials: WorkerCredentials::with_join_secret(join_secret.clone()),
                    allocations: Mutex::new(HashMap::new()),
                    reports: Mutex::new(SimReports::default()),
                    reported_acked: AtomicUsize::new(0),
                }),
            );
        }
    }
    let fleet = Arc::new(SimFleet {
        nodes,
        heartbeat_interval: heartbeat_interval_for(&config),
        registered: AtomicU64::new(0),
        heartbeats_ok: AtomicU64::new(0),
        heartbeats_failed: AtomicU64::new(0),
        reported_allocations: AtomicI64::new(0),
        checkpoints: opts.checkpoints,
        observer: opts.observer.clone(),
    });

    // A sim host is exactly the host a scale campaign scrapes, so it exports
    // the same `legion_worker_*` registry a real worker does — the families
    // aggregated over the fleet, plus the `simulated_*` ones describing the
    // fleet itself (docs/metrics.md § Worker). Started before the
    // registration ramp so a scrape during ramp-up shows the climb.
    start_worker_telemetry();
    let metrics = crate::manager::telemetry::worker_metrics();
    metrics.simulated_nodes_target.set(opts.count as i64);
    metrics.simulated_nodes.set(0);

    let server_fleet = fleet.clone();
    let server_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(WorkerServer::new(SimWorkerService {
                fleet: server_fleet,
            }))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .context("stub worker server exited")
    });

    // One task per node: ramped registration, then the heartbeat loop.
    // The ramp itself staggers heartbeats across the interval.
    let spacing = Duration::from_secs_f64(1.0 / opts.register_rate.max(0.001));
    let mut ordered: Vec<Arc<SimNode>> = fleet.nodes.values().cloned().collect();
    ordered.sort_by(|a, b| a.name.cmp(&b.name));
    let mut node_tasks = Vec::with_capacity(ordered.len());
    for (idx, node) in ordered.into_iter().enumerate() {
        let client = clients[idx % clients.len()].clone();
        let fleet = fleet.clone();
        let ready_after = opts.ready_after;
        let start_delay = spacing.mul_f64(idx as f64);
        node_tasks.push(tokio::spawn(async move {
            tokio::time::sleep(start_delay).await;
            run_sim_node(client, fleet, node, ready_after).await;
        }));
    }

    let summary_task = tokio::spawn(run_summary_loop(fleet.clone(), opts.clone()));

    Ok(SimFleetRun {
        fleet,
        ready_after: opts.ready_after,
        worker_address,
        node_tasks,
        server_task,
        summary_task,
    })
}

/// Register (with retry), then heartbeat forever at the real worker's
/// cadence, folding the 30s health-check report into every Nth tick.
async fn run_sim_node(
    client: ManagerClient<Channel>,
    fleet: Arc<SimFleet>,
    node: Arc<SimNode>,
    ready_after: Duration,
) {
    let mut backoff = SIM_RETRY_MIN;
    loop {
        match register_sim_node(&client, &node, &fleet).await {
            Ok(()) => {
                let registered = fleet.registered.fetch_add(1, Ordering::Relaxed) + 1;
                crate::manager::telemetry::worker_metrics()
                    .simulated_nodes
                    .set(registered as i64);
                break;
            }
            Err(err) => {
                warn!(node = %node.name, error = ?err, "Sim node registration failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, SIM_RETRY_MAX);
            }
        }
    }

    // Mutable for the same reason the real worker's is: a manager running
    // adaptive cadence serves an interval on the reply, and the sim fleet
    // honors it so a scale run measures the real controller rather than a
    // fleet that ignores it.
    let mut heartbeat_interval = fleet.heartbeat_interval;
    let configured_interval = fleet.heartbeat_interval;
    let ticks_per_health_check = ((WORKER_NODE_CHECK_DURATION_SEC as f64
        / heartbeat_interval.as_secs_f64().max(0.001))
    .round() as u64)
        .max(1);
    let mut backoff = SIM_RETRY_MIN;
    let mut tick: u64 = 0;
    loop {
        // Timed and classified exactly as the real worker's loop does it
        // (`daemon::run_heartbeat_loop`), so a campaign reads one set of
        // `legion_worker_heartbeat*` families whichever fleet produced it.
        let tick_started = Instant::now();
        let tick_result = heartbeat_sim_node(&client, &node, &fleet, ready_after).await;
        let metrics = crate::manager::telemetry::worker_metrics();
        metrics
            .heartbeat_duration_seconds
            .observe(tick_started.elapsed().as_secs_f64());
        metrics.heartbeats_total.inc(&[match &tick_result {
            Ok(_) => "ok",
            Err(status) if status.code() == tonic::Code::Unauthenticated => "unauthenticated",
            Err(_) => "error",
        }]);
        match tick_result {
            Ok(served) => {
                fleet.heartbeats_ok.fetch_add(1, Ordering::Relaxed);
                backoff = SIM_RETRY_MIN;
                if tick.is_multiple_of(ticks_per_health_check) {
                    // Best-effort like the real worker's check loop: a
                    // failure logs and waits for the next round.
                    let req = node.credentials.attach(Request::new(HealthCheckRequest {
                        id: node.name.clone(),
                        health_checks: health_checks_json(),
                    }));
                    if let Err(status) = client.clone().health_check(req).await {
                        debug!(node = %node.name, code = %status.code(), "Sim health check failed");
                    }
                }
                tick += 1;
                match crate::worker::daemon::adopt_served_interval(
                    heartbeat_interval,
                    configured_interval,
                    served,
                ) {
                    Some(next) => {
                        heartbeat_interval = next;
                        tokio::time::sleep(crate::worker::daemon::rephase_sleep(next)).await;
                    }
                    None => {
                        tokio::time::sleep(jittered_heartbeat_interval(heartbeat_interval)).await;
                    }
                }
            }
            Err(status) if status.code() == tonic::Code::Unauthenticated => {
                warn!(node = %node.name, "Sim heartbeat unauthenticated; re-registering");
                if let Err(err) = register_sim_node(&client, &node, &fleet).await {
                    warn!(node = %node.name, error = ?err, "Sim re-registration failed; will retry");
                }
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, SIM_RETRY_MAX);
            }
            Err(status) => {
                fleet.heartbeats_failed.fetch_add(1, Ordering::Relaxed);
                debug!(
                    node = %node.name,
                    code = %status.code(),
                    retry_in_ms = backoff.as_millis() as u64,
                    "Sim heartbeat failed; retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, SIM_RETRY_MAX);
            }
        }
    }
}

async fn register_sim_node(
    client: &ManagerClient<Channel>,
    node: &SimNode,
    fleet: &SimFleet,
) -> R<()> {
    let outcome = register_sim_node_inner(client, node, fleet).await;
    crate::manager::telemetry::worker_metrics()
        .simulated_registrations_total
        .inc(&[if outcome.is_ok() { "ok" } else { "error" }]);
    outcome
}

async fn register_sim_node_inner(
    client: &ManagerClient<Channel>,
    node: &SimNode,
    fleet: &SimFleet,
) -> R<()> {
    let req = Request::new(WorkerRegisterRequest {
        name: node.name.clone(),
        pool: node.pool.clone(),
        config_attrs: node.config_attrs.clone(),
        fingerprint_attrs: node.fingerprint_attrs.clone(),
        health_checks: health_checks_json(),
        join_token: node.credentials.join_secret().cloned(),
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: PROTOCOL_VERSION,
        // A simulated node runs nothing it could checkpoint, unless it was
        // asked to fake the checkpoint channel.
        capabilities: if fleet.checkpoints {
            vec![crate::structs::snapshot::CHECKPOINT_CAPABILITY.to_string()]
        } else {
            Vec::new()
        },
    });
    let reply = client.clone().worker_register(req).await?.into_inner();
    node.credentials.store_node_token(reply.node_token);
    Ok(())
}

/// One heartbeat round-trip. Returns the gRPC status on failure so the
/// caller can special-case `Unauthenticated`.
async fn heartbeat_sim_node(
    client: &ManagerClient<Channel>,
    node: &SimNode,
    fleet: &SimFleet,
    ready_after: Duration,
) -> Result<Option<u32>, Status> {
    let acked = node.acked_ids(ready_after);
    fleet.record_reported_allocations(node, acked.len());
    // Every running allocation's health, every beat, as a real worker
    // reports it (`TaskManager::allocation_health_snapshot`): the manager's
    // per-beat cost of a dense node is part of what a scale run measures.
    let allocation_health = acked
        .iter()
        .map(|id| AllocationHealth {
            allocation_id: *id,
            healthy: true,
            status: "running".into(),
            artifact_states: None,
        })
        .collect();
    let snapshot_reports = if fleet.checkpoints {
        let mut reports =
            std::mem::take(&mut node.reports.lock().expect("sim reports lock").pending);
        reports.extend(node.take_restore_reports(ready_after));
        reports
    } else {
        Vec::new()
    };
    let req = node.credentials.attach(Request::new(HeartbeatRequest {
        // Simulated nodes have no content store to advertise.
        content_locations: None,
        pressure_actions: Vec::new(),
        net_denials: Vec::new(),
        net_status: Vec::new(),
        net_edges: Vec::new(),
        // Simulated nodes never take provisional starts: the simulator
        // models scheduler load, and edge boot is a node-local execution
        // path with nothing to model.
        provisional_claims: Vec::new(),
        id: node.name.clone(),
        allocation_ids: acked,
        failed_allocations: Vec::new(),
        allocation_health,
        volume_reports: Vec::new(),
        // Simulated nodes run no processes, so nothing ever terminates.
        terminated_allocations: Vec::new(),
        // Nor do they have cgroups to measure.
        usage: None,
        // Nor anything to checkpoint, unless faking the channel.
        snapshot_reports: snapshot_reports.clone(),
    }));
    let reply = match client.clone().heartbeat(req).await {
        Ok(reply) => reply.into_inner(),
        Err(status) => {
            // Re-send next beat, as a real worker restores drained reports.
            if !snapshot_reports.is_empty() {
                let mut reports = node.reports.lock().expect("sim reports lock");
                let mut restored = snapshot_reports;
                restored.append(&mut reports.pending);
                reports.pending = restored;
            }
            return Err(status);
        }
    };
    let adopted = node.reconcile_allocations(&reply.desired_allocations);
    fleet.observe(node, &adopted, SimDelivery::Heartbeat);
    if fleet.checkpoints {
        node.answer_checkpoint_requests(&reply.checkpoint_requests);
    }
    Ok(reply.next_interval_seconds)
}

/// Periodic fleet-status line — cheap enough for campaign scripts to
/// grep, informative enough to spot a stalled fleet.
async fn run_summary_loop(fleet: Arc<SimFleet>, opts: SimulateOptions) {
    let mut last_ok = 0u64;
    let mut last_at = Instant::now();
    loop {
        tokio::time::sleep(SIM_SUMMARY_INTERVAL).await;
        fleet.resync_gauges();
        let ok = fleet.heartbeats_ok.load(Ordering::Relaxed);
        let elapsed = last_at.elapsed().as_secs_f64().max(0.001);
        let rate = (ok - last_ok) as f64 / elapsed;
        // Every live node heartbeats once per interval; the expected rate
        // tells the reader at a glance whether the fleet is keeping up.
        let expected = fleet.registered.load(Ordering::Relaxed) as f64
            / fleet.heartbeat_interval.as_secs_f64().max(0.001);
        info!(
            nodes_registered = fleet.registered.load(Ordering::Relaxed),
            nodes_target = opts.count,
            heartbeats_per_sec = format!("{rate:.1}"),
            heartbeats_expected_per_sec = format!("{expected:.1}"),
            heartbeats_failed_total = fleet.heartbeats_failed.load(Ordering::Relaxed),
            acked_allocations = fleet.acked_total(opts.ready_after),
            "Simulated fleet status"
        );
        last_ok = ok;
        last_at = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(name: &str, weight: usize) -> SimNodeProfile {
        SimNodeProfile {
            name: name.into(),
            count: weight,
            ..SimNodeProfile::default()
        }
    }

    #[test]
    fn distribute_is_exact_and_proportional() -> R<()> {
        // web_fleet-shaped weights: 120/80/40 → 3:2:1.
        let profiles = vec![profile("web", 120), profile("util", 80), profile("gpu", 40)];
        let shares = distribute(1200, &profiles)?;
        let counts: Vec<usize> = shares.iter().map(|(_, share)| *share).collect();
        assert_eq!(counts, vec![600, 400, 200]);
        Ok(())
    }

    #[test]
    fn distribute_handles_remainders_deterministically() -> R<()> {
        // 10 nodes over equal thirds: 4/3/3 (earlier profile wins the tie).
        let profiles = vec![profile("a", 1), profile("b", 1), profile("c", 1)];
        let shares = distribute(10, &profiles)?;
        let counts: Vec<usize> = shares.iter().map(|(_, share)| *share).collect();
        assert_eq!(counts.iter().sum::<usize>(), 10);
        assert_eq!(counts, vec![4, 3, 3]);
        // Re-running distributes identically.
        let again: Vec<usize> = distribute(10, &profiles)?
            .iter()
            .map(|(_, share)| *share)
            .collect();
        assert_eq!(counts, again);
        Ok(())
    }

    #[test]
    fn distribute_with_fewer_nodes_than_profiles() -> R<()> {
        let profiles = vec![profile("a", 1), profile("b", 1), profile("c", 1)];
        let counts: Vec<usize> = distribute(2, &profiles)?
            .iter()
            .map(|(_, share)| *share)
            .collect();
        assert_eq!(counts.iter().sum::<usize>(), 2);
        Ok(())
    }

    #[test]
    fn distribute_rejects_zero_weights() {
        let profiles = vec![profile("a", 0)];
        assert!(distribute(5, &profiles).is_err());
    }

    #[test]
    fn fingerprint_matches_scheduler_expectations() {
        let fingerprint = fingerprint_for(
            &SimNodeProfile {
                memory_gb: 16,
                cpus: 2,
                ..SimNodeProfile::default()
            },
            false,
        );
        assert_eq!(
            fingerprint["memory_total"],
            json!((16u64 * 1024 * 1024 * 1024).to_string())
        );
        assert_eq!(fingerprint["cpus"]["cpu-1"]["frequency"], json!("3000"));
        assert_eq!(fingerprint["cpus"]["cpu-2"]["frequency"], json!("3000"));
        assert!(fingerprint["cpus"].get("cpu-3").is_none());
        assert_eq!(fingerprint["volumeDrivers"], json!({}));
    }

    #[test]
    fn config_attrs_carry_profile_attrs_marker_and_address() {
        let mut profile = SimNodeProfile::default();
        profile.attrs.insert("class".into(), json!("web"));
        profile.max_allocations = Some(40);
        let config = WorkerConfig {
            attributes: HashMap::from([("tier".into(), "sim".into())]),
            resources: json!({"gpu": 1}).to_string(),
            ..WorkerConfig::default()
        };
        let attrs = config_attrs_for(&profile, &config, "http://10.0.0.9:4848");
        assert_eq!(attrs["class"], json!("web"));
        assert_eq!(attrs["tier"], json!("sim"));
        assert_eq!(attrs["simulated"], json!(true));
        assert_eq!(attrs["maxAllocations"], json!(40));
        assert_eq!(attrs["resources"], json!({"gpu": 1}));
        assert_eq!(attrs["workerAddress"], json!("http://10.0.0.9:4848"));
    }

    fn payload(id: i64) -> TaskAllocation {
        TaskAllocation {
            allocation_id: id,
            job_id: "agents".into(),
            task_name: "workspace".into(),
            task_config: json!({ "env": { "LEGION_INSTANCE_ID": format!("user-{id}") } })
                .to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn ready_after_gates_acks() {
        let node = sim_node("sim-a-0");
        let first = [payload(1), payload(2)];
        let adopted = node.accept_allocations(&first);
        assert_eq!(adopted.len(), 2);
        // Instantly acked with no delay...
        let mut acked = node.acked_ids(Duration::ZERO);
        acked.sort();
        assert_eq!(acked, vec![1, 2]);
        // ...but gated behind a ready-after window.
        assert!(node.acked_ids(Duration::from_secs(60)).is_empty());
        // Reconcile drops undesired ids and adopts new ones, reporting only
        // the new one as adopted.
        let desired = [payload(2), payload(3)];
        let adopted = node.reconcile_allocations(&desired);
        assert_eq!(
            adopted.iter().map(|a| a.allocation_id).collect::<Vec<_>>(),
            vec![3]
        );
        let mut acked = node.acked_ids(Duration::ZERO);
        acked.sort();
        assert_eq!(acked, vec![2, 3]);
    }

    #[test]
    fn a_faked_checkpoint_channel_answers_once_and_resends_while_asked() {
        let node = sim_node("sim-a-0");
        let held = [payload(7)];
        node.accept_allocations(&held);
        let capture = CheckpointRequest {
            snapshot_id: "snap-1".into(),
            op: "capture".into(),
            allocation_id: 7,
            kind: "memory".into(),
            then: "stop".into(),
            purpose: "suspend".into(),
            ..Default::default()
        };
        node.answer_checkpoint_requests(std::slice::from_ref(&capture));
        let first = std::mem::take(&mut node.reports.lock().unwrap().pending);
        assert_eq!(first.len(), 1);
        assert!(first[0].ok);
        let manifest: Value = serde_json::from_str(&first[0].manifest_json).unwrap();
        assert_eq!(manifest["originInstance"], json!("user-7"));
        assert_eq!(manifest["kind"], json!("memory"));
        // Asked again (the report has not been applied yet): the same report.
        node.answer_checkpoint_requests(std::slice::from_ref(&capture));
        let again = std::mem::take(&mut node.reports.lock().unwrap().pending);
        assert_eq!(again[0].manifest_json, first[0].manifest_json);
        // No longer asked: forgotten.
        node.answer_checkpoint_requests(&[]);
        assert!(node.reports.lock().unwrap().answered.is_empty());
    }

    #[test]
    fn a_restore_payload_is_reported_once_ready() {
        let node = sim_node("sim-a-0");
        let mut restoring = payload(9);
        restoring.restore = Some(crate::structs::SnapshotRestore {
            snapshot_id: "snap-9".into(),
            ..Default::default()
        });
        node.accept_allocations(&[restoring]);
        assert!(node
            .take_restore_reports(Duration::from_secs(60))
            .is_empty());
        let reports = node.take_restore_reports(Duration::ZERO);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].op, "restore");
        assert_eq!(reports[0].allocation_id, 9);
        assert!(node.take_restore_reports(Duration::ZERO).is_empty());
    }

    fn sim_node(name: &str) -> Arc<SimNode> {
        Arc::new(SimNode {
            name: name.into(),
            pool: "default".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: "{}".into(),
            credentials: WorkerCredentials::default(),
            allocations: Mutex::new(HashMap::new()),
            reports: Mutex::new(SimReports::default()),
            reported_acked: AtomicUsize::new(0),
        })
    }

    fn sim_fleet(names: &[&str]) -> Arc<SimFleet> {
        Arc::new(SimFleet {
            nodes: names
                .iter()
                .map(|name| ((*name).to_string(), sim_node(name)))
                .collect(),
            heartbeat_interval: Duration::from_secs(1),
            registered: AtomicU64::new(0),
            heartbeats_ok: AtomicU64::new(0),
            heartbeats_failed: AtomicU64::new(0),
            reported_allocations: AtomicI64::new(0),
            checkpoints: false,
            observer: None,
        })
    }

    /// The fleet-wide allocation total is maintained by per-node deltas
    /// rather than a walk over the fleet, so the arithmetic has to hold
    /// across growth, shrink, and repeated identical reports.
    #[test]
    fn reported_allocations_track_per_node_deltas() {
        let fleet = sim_fleet(&["a", "b"]);
        let a = fleet.nodes["a"].clone();
        let b = fleet.nodes["b"].clone();

        fleet.record_reported_allocations(&a, 3);
        fleet.record_reported_allocations(&b, 2);
        assert_eq!(fleet.reported_allocations.load(Ordering::Relaxed), 5);

        // A node reporting the same count again must not double-count.
        fleet.record_reported_allocations(&a, 3);
        assert_eq!(fleet.reported_allocations.load(Ordering::Relaxed), 5);

        // Shrink (allocations stopped) subtracts.
        fleet.record_reported_allocations(&a, 1);
        assert_eq!(fleet.reported_allocations.load(Ordering::Relaxed), 3);

        // Drained fleet returns to zero, not to a floor.
        fleet.record_reported_allocations(&a, 0);
        fleet.record_reported_allocations(&b, 0);
        assert_eq!(fleet.reported_allocations.load(Ordering::Relaxed), 0);
    }

    /// The summary-cadence repair must re-derive exactly the quantity the
    /// deltas maintain — sum of last-reported counts — or it would desync
    /// the total it exists to fix.
    #[test]
    fn resync_repairs_a_lost_delta_without_desyncing() {
        let fleet = sim_fleet(&["a", "b"]);
        fleet.record_reported_allocations(&fleet.nodes["a"].clone(), 4);
        fleet.record_reported_allocations(&fleet.nodes["b"].clone(), 6);

        // Simulate a lost update (the racy read-modify-write on the gauge).
        fleet.reported_allocations.store(3, Ordering::Relaxed);
        fleet.resync_gauges();
        assert_eq!(fleet.reported_allocations.load(Ordering::Relaxed), 10);

        // And the repaired total still tracks subsequent deltas correctly.
        fleet.record_reported_allocations(&fleet.nodes["a"].clone(), 5);
        assert_eq!(fleet.reported_allocations.load(Ordering::Relaxed), 11);
    }

    #[test]
    fn sim_cadence_is_the_real_worker_cadence() {
        // The cadence path is shared, not forked: the sim fleet resolves
        // its heartbeat interval through the same daemon helper as a real
        // worker — worker-type `heartbeatSeconds`, clamped 1..=30, default
        // WORKER_HEARTBEAT_DURATION_SEC — so the manager's 10s lost-node
        // sweep applies to sim nodes exactly as to real ones.
        let default_interval = heartbeat_interval_for(&WorkerConfig::default());
        assert_eq!(
            default_interval,
            Duration::from_secs(WORKER_HEARTBEAT_DURATION_SEC)
        );
        assert!(default_interval.as_secs() as i64 * 2 <= DEF_LOST_NODE_AT_SEC);

        // The clamp widened from 30 to 300 once the lost window began moving
        // with the cadence (docs/heartbeat-cadence-and-scale.md); the sim
        // fleet follows it because the helper is shared, not forked.
        let configured = WorkerConfig {
            heartbeat_seconds: Some(120),
            ..WorkerConfig::default()
        };
        assert_eq!(
            heartbeat_interval_for(&configured),
            Duration::from_secs(120),
            "sim fleets follow the same clamp as real workers"
        );
        let over_clamp = WorkerConfig {
            heartbeat_seconds: Some(4000),
            ..WorkerConfig::default()
        };
        assert_eq!(
            heartbeat_interval_for(&over_clamp),
            Duration::from_secs(u64::from(MAX_WORKER_HEARTBEAT_SECONDS))
        );
    }
}
