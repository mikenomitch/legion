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
    ExecClientFrame, GetTaskLogsReply, GetTaskLogsRequest, HealthCheckRequest, HeartbeatRequest,
    PostWorkReply, PostWorkRequest, RestartWorkerReply, RestartWorkerRequest, WorkerConfig,
    WorkerPreRegisterRequest, WorkerRegisterRequest,
};
use crate::worker::daemon::{
    connect_manager_with_retry, heartbeat_interval_for, jittered_heartbeat_interval,
    WorkerCredentials,
};

use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
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
        }
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
fn fingerprint_for(profile: &SimNodeProfile) -> Value {
    let mut cpus = Map::new();
    for idx in 1..=profile.cpus {
        cpus.insert(format!("cpu-{idx}"), json!({ "frequency": "3000" }));
    }
    json!({
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "memory_total": (profile.memory_gb * 1024 * 1024 * 1024).to_string(),
        "cpus": cpus,
        "volumeDrivers": {},
    })
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
    allocations: Mutex<HashMap<i64, Instant>>,
}

impl SimNode {
    fn accept_allocations(&self, ids: impl IntoIterator<Item = i64>) {
        let now = Instant::now();
        let mut allocations = self.allocations.lock().expect("sim allocations lock");
        for id in ids {
            allocations.entry(id).or_insert(now);
        }
    }

    /// Reconcile against the manager's desired set: keep ids still
    /// desired, adopt new ones, drop the rest. Returns nothing — the next
    /// `acked_ids` call reflects the change.
    fn reconcile_allocations(&self, desired: &[i64]) {
        let now = Instant::now();
        let mut allocations = self.allocations.lock().expect("sim allocations lock");
        allocations.retain(|id, _| desired.contains(id));
        for id in desired {
            allocations.entry(*id).or_insert(now);
        }
    }

    /// Allocation ids old enough to report as running.
    fn acked_ids(&self, ready_after: Duration) -> Vec<i64> {
        let allocations = self.allocations.lock().expect("sim allocations lock");
        allocations
            .iter()
            .filter(|(_, first_seen)| first_seen.elapsed() >= ready_after)
            .map(|(id, _)| *id)
            .collect()
    }
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
}

impl SimFleet {
    fn acked_total(&self, ready_after: Duration) -> usize {
        self.nodes
            .values()
            .map(|node| node.acked_ids(ready_after).len())
            .sum()
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
        node.accept_allocations([allocation.allocation_id]);
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
        let fingerprint = fingerprint_for(profile).to_string();
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
    });

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
        match register_sim_node(&client, &node).await {
            Ok(()) => {
                fleet.registered.fetch_add(1, Ordering::Relaxed);
                break;
            }
            Err(err) => {
                warn!(node = %node.name, error = ?err, "Sim node registration failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, SIM_RETRY_MAX);
            }
        }
    }

    let heartbeat_interval = fleet.heartbeat_interval;
    let ticks_per_health_check = ((WORKER_NODE_CHECK_DURATION_SEC as f64
        / heartbeat_interval.as_secs_f64().max(0.001))
    .round() as u64)
        .max(1);
    let mut backoff = SIM_RETRY_MIN;
    let mut tick: u64 = 0;
    loop {
        match heartbeat_sim_node(&client, &node, ready_after).await {
            Ok(()) => {
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
                tokio::time::sleep(jittered_heartbeat_interval(heartbeat_interval)).await;
            }
            Err(status) if status.code() == tonic::Code::Unauthenticated => {
                warn!(node = %node.name, "Sim heartbeat unauthenticated; re-registering");
                if let Err(err) = register_sim_node(&client, &node).await {
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

async fn register_sim_node(client: &ManagerClient<Channel>, node: &SimNode) -> R<()> {
    let req = Request::new(WorkerRegisterRequest {
        name: node.name.clone(),
        pool: node.pool.clone(),
        config_attrs: node.config_attrs.clone(),
        fingerprint_attrs: node.fingerprint_attrs.clone(),
        health_checks: health_checks_json(),
        join_token: node.credentials.join_secret().cloned(),
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: PROTOCOL_VERSION,
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
    ready_after: Duration,
) -> Result<(), Status> {
    let req = node.credentials.attach(Request::new(HeartbeatRequest {
        id: node.name.clone(),
        allocation_ids: node.acked_ids(ready_after),
        failed_allocations: Vec::new(),
        allocation_health: Vec::new(),
        volume_reports: Vec::new(),
    }));
    let reply = client.clone().heartbeat(req).await?.into_inner();
    let desired: Vec<i64> = reply
        .desired_allocations
        .iter()
        .map(|allocation| allocation.allocation_id)
        .collect();
    node.reconcile_allocations(&desired);
    Ok(())
}

/// Periodic fleet-status line — cheap enough for campaign scripts to
/// grep, informative enough to spot a stalled fleet.
async fn run_summary_loop(fleet: Arc<SimFleet>, opts: SimulateOptions) {
    let mut last_ok = 0u64;
    let mut last_at = Instant::now();
    loop {
        tokio::time::sleep(SIM_SUMMARY_INTERVAL).await;
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
        let fingerprint = fingerprint_for(&SimNodeProfile {
            memory_gb: 16,
            cpus: 2,
            ..SimNodeProfile::default()
        });
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

    #[test]
    fn ready_after_gates_acks() {
        let node = SimNode {
            name: "sim-a-0".into(),
            pool: "default".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: "{}".into(),
            credentials: WorkerCredentials::default(),
            allocations: Mutex::new(HashMap::new()),
        };
        node.accept_allocations([1, 2]);
        // Instantly acked with no delay...
        let mut acked = node.acked_ids(Duration::ZERO);
        acked.sort();
        assert_eq!(acked, vec![1, 2]);
        // ...but gated behind a ready-after window.
        assert!(node.acked_ids(Duration::from_secs(60)).is_empty());
        // Reconcile drops undesired ids and adopts new ones.
        node.reconcile_allocations(&[2, 3]);
        let mut acked = node.acked_ids(Duration::ZERO);
        acked.sort();
        assert_eq!(acked, vec![2, 3]);
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

        let configured = WorkerConfig {
            heartbeat_seconds: Some(120),
            ..WorkerConfig::default()
        };
        assert_eq!(
            heartbeat_interval_for(&configured),
            Duration::from_secs(30),
            "sim fleets follow the same clamp as real workers"
        );
    }
}
