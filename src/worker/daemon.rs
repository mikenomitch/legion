use crate::prelude::*;

use crate::structs::token::JoinToken;
use crate::structs::{
    manager_client::ManagerClient, worker_server::WorkerServer, AllocNetwork, HealthCheckRequest,
    HeartbeatReply, HeartbeatRequest, TaskAllocation, UpstreamEndpoints, WorkerConfig,
    WorkerPreRegisterReply, WorkerPreRegisterRequest, WorkerRegisterRequest,
};
use crate::util::random_string;
use crate::worker::{
    artifact_credentials::ManagerCredentialSource,
    balance::Endpoint,
    forwarder::{UpstreamForwarder, UpstreamSpec},
    identity::{ManagerTokenIssuer, PreparedIdentity, WorkerIdentityManager},
    node_check::{fingerprint_node, run_health_checks},
    server::WorkerDaemonService,
    state::WorkerStateStore,
    task_health::spawn_health_monitor,
    task_manager::{launch_task_allocation, WorkerTaskManager},
    task_supervisor::start_task_supervisor,
};

use futures::join;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::Duration;
use tonic::{transport::Channel, transport::Server, Response};

/// The worker's manager-issued credentials: the join secret it was started
/// with (used to register and re-register) and the node token returned by
/// registration (attached to every worker-plane RPC: heartbeats, health
/// checks, identity-token issuance, artifact-credential fetches). Empty when
/// the cluster runs without ACLs.
#[derive(Debug, Default)]
pub struct WorkerCredentials {
    join_secret: Option<String>,
    node_token: RwLock<Option<String>>,
}

impl WorkerCredentials {
    pub fn with_join_secret(join_secret: Option<String>) -> Self {
        WorkerCredentials {
            join_secret,
            node_token: RwLock::new(None),
        }
    }

    /// Attaches the node token as a bearer credential, when one is held.
    pub fn attach<T>(&self, mut request: tonic::Request<T>) -> tonic::Request<T> {
        if let Some(token) = self.node_token.read().ok().and_then(|t| t.clone()) {
            if let Ok(value) = format!("Bearer {token}").parse() {
                request.metadata_mut().insert("authorization", value);
            }
        }
        request
    }

    pub(crate) fn store_node_token(&self, token: Option<String>) {
        if let Ok(mut current) = self.node_token.write() {
            *current = token;
        }
    }

    pub(crate) fn join_secret(&self) -> Option<&String> {
        self.join_secret.as_ref()
    }
}

/// Port the worker's own gRPC server listens on, and the port it advertises to
/// the manager as its `workerAddress`. Overridable via `LEGION_WORKER_PORT` so
/// several workers can run on one host (local multi-node testing); the manager
/// dispatches to the advertised address, so both uses must agree.
fn worker_server_port() -> u64 {
    std::env::var("LEGION_WORKER_PORT")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|port| *port > 0)
        .unwrap_or(DEF_WORKER_SERVER_PORT)
}

/// Everything needed to (re-)register this worker with the manager.
#[derive(Debug, Clone)]
struct RegistrationPayload {
    name: String,
    pool: String,
    config_attrs: String,
    fingerprint_attrs: String,
    health_checks: String,
}

#[instrument(err, skip_all)]
pub async fn run_worker(token: &str, name_override: Option<&str>) -> R<()> {
    let join_token = JoinToken::decode(token)?;
    info!(worker_type = %join_token.worker_type, "Decoded join token");
    run_worker_with_manager(
        join_token.addr,
        join_token.worker_type,
        name_override,
        Some(join_token.secret),
    )
    .await
}

#[instrument(err, skip_all)]
pub async fn run_worker_local(
    manager_addr: String,
    worker_type: String,
    name_override: Option<&str>,
    join_secret: Option<String>,
) -> R<()> {
    run_worker_with_manager(manager_addr, worker_type, name_override, join_secret).await
}

/// The worker config's `hostVolumes` map. A malformed value is logged and
/// treated as empty rather than failing worker startup: the manager already
/// validated it at load time, and a worker that refuses to start over one bad
/// entry is worse than one whose `host` plugin reports itself unavailable.
fn parse_host_volumes(raw: &str) -> HashMap<String, crate::volumes::HostVolumeConfig> {
    if raw.trim().is_empty() {
        return HashMap::new();
    }
    serde_json::from_str(raw).unwrap_or_else(|err| {
        warn!(error = ?err, "Invalid hostVolumes in worker config; ignoring");
        HashMap::new()
    })
}

#[instrument(err, skip_all)]
async fn run_worker_with_manager(
    manager_addr: String,
    worker_type: String,
    name_override: Option<&str>,
    join_secret: Option<String>,
) -> R<()> {
    let server_port = worker_server_port();
    let addr = format!("{DEF_MANAGER_HOSTNAME}:{server_port}").parse()?;

    info!(%manager_addr, %worker_type, "Worker connecting to manager");

    let client: ManagerClient<Channel> = connect_manager_with_retry(&manager_addr).await?;
    let credentials = Arc::new(WorkerCredentials::with_join_secret(join_secret));

    let pre_register_res = pre_register_self(client.clone(), worker_type, &credentials).await?;
    let config = pre_register_res
        .into_inner()
        .config
        .context("No config found")?;

    let name = resolve_worker_name(name_override, config.name_prefix.clone())?;
    let state_path = WorkerStateStore::path_for_worker(&name);
    let task_manager = Arc::new(Mutex::new(WorkerTaskManager::with_state_store(
        WorkerStateStore::new(state_path),
    )));
    let artifact_fetch_config =
        parse_artifact_fetch_config(&config.artifacts).unwrap_or_else(|err| {
            warn!(error = ?err, "Invalid worker artifacts config; using defaults");
            crate::structs::WorkerArtifactFetchConfigFull::default()
        });
    // Volume drivers must be ready before any allocation can arrive, and they
    // need the node id (it goes into every plugin request) plus the operator's
    // hostVolumes declarations from the worker config.
    let host_volumes = parse_host_volumes(&config.host_volumes);
    // Task runtimes run in-process as dylibs and take no config of their own,
    // so cross-cutting worker settings reach them through the environment —
    // the same channel `sandbox` uses for LEGION_SANDBOX_KERNEL.
    apply_resource_enforcement_env(&config.resource_enforcement);
    let restored_allocations = {
        let mut manager = task_manager.lock().expect("task manager");
        manager.load_builtin_plugins();
        manager.configure_volumes(&name, host_volumes);
        manager.configure_artifacts(
            artifact_fetch_config,
            Box::new(ManagerCredentialSource::new(
                manager_addr.clone(),
                name.clone(),
                credentials.clone(),
            )),
        );
        manager.restore_allocations().unwrap_or_else(|err| {
            warn!(error = ?err, "Failed to restore persisted allocations");
            Vec::new()
        })
    };
    // Workload-identity delivery/refresh: tokens are requested from the
    // manager under this worker's node id.
    let identity_manager = Arc::new(WorkerIdentityManager::new(Arc::new(ManagerTokenIssuer {
        client: client.clone(),
        node_id: name.clone(),
        credentials: credentials.clone(),
    })));
    for (allocation, task) in restored_allocations {
        // Restored allocations were issued tokens by a previous worker
        // process; seed their refresh loops with an already-expired token so
        // they re-issue immediately.
        if !task.identities.is_empty() {
            let prepared = task
                .identities
                .iter()
                .map(|identity| PreparedIdentity {
                    identity: identity.clone(),
                    expires_at: 0,
                })
                .collect();
            identity_manager.spawn_refreshers(allocation.id, prepared, task_manager.clone());
        }
        spawn_health_monitor(task_manager.clone(), allocation, task);
    }
    // Edge boot: this node serves `Worker.StartClaim` only when its worker
    // config opts in. Off by default, independently of the manager's switch —
    // an operator who enables the feature cluster-wide still chooses which
    // nodes are edges (docs/edge-boot.md).
    let edge_boot_config: crate::structs::WorkerEdgeBootConfigFull =
        if config.edge_boot.trim().is_empty() {
            crate::structs::WorkerEdgeBootConfigFull::default()
        } else {
            serde_json::from_str(&config.edge_boot).unwrap_or_else(|err| {
                warn!(error = ?err, "Invalid edgeBoot worker config; edge starts stay disabled");
                crate::structs::WorkerEdgeBootConfigFull::default()
            })
        };
    let edge_boot = if edge_boot_config.accept_claims {
        Some(Arc::new(build_edge_boot_runtime(
            &edge_boot_config,
            &manager_addr,
            &name,
            &config.pool,
            client.clone(),
            credentials.clone(),
        )))
    } else {
        None
    };
    if edge_boot.is_some() {
        info!(worker = %name, pool = %config.pool, "Edge boot enabled: this node accepts boot tickets");
    }

    let worker_node = WorkerDaemonService {
        task_manager: task_manager.clone(),
        identity: Some(identity_manager.clone()),
        edge_boot: edge_boot.clone(),
    };
    let worker_server_fut = Server::builder()
        .add_service(WorkerServer::new(worker_node))
        .serve(addr);

    let mut fingerprint_data = fingerprint_node(config.additional_node_checks.clone());
    // The manager loads no storage drivers of its own, so this is its only
    // source of truth for what volume drivers exist in the cluster: it backs
    // job admission, placement filtering, and `legion volume drivers`.
    fingerprint_data.insert(
        "volumeDrivers".into(),
        task_manager
            .lock()
            .expect("task manager")
            .volume_fingerprint(),
    );
    let readiness = task_manager
        .lock()
        .expect("task manager")
        .readiness_checks();
    let health_checks = run_health_checks(config.additional_node_checks.clone(), Some(readiness));

    // Resolved before `config` is picked apart below (the helper borrows
    // the whole struct).
    let heartbeat_interval = heartbeat_interval_for(&config);

    let mut config_attrs = Map::new();
    for (key, value) in config.attributes {
        config_attrs.insert(key, Value::String(value));
    }
    let resources = serde_json::from_str(&config.resources).unwrap_or_else(|_| json!({}));
    let reserved_resources =
        serde_json::from_str(&config.reserved_resources).unwrap_or_else(|_| json!({}));
    config_attrs.insert("resources".into(), resources);
    config_attrs.insert("reservedResources".into(), reserved_resources);
    if let Some(max_allocations) = config.max_allocations {
        config_attrs.insert("maxAllocations".into(), json!(max_allocations));
    }
    config_attrs.insert(
        "workerAddress".into(),
        Value::String(format!("http://{}:{}", DEF_MANAGER_HOSTNAME, server_port)),
    );

    let registration = RegistrationPayload {
        name: name.clone(),
        pool: config.pool.clone(),
        config_attrs: serde_json::to_string(&config_attrs)?,
        fingerprint_attrs: serde_json::to_string(&fingerprint_data)?,
        health_checks: serde_json::to_string(&health_checks)?,
    };
    register_self(client.clone(), &registration, &credentials).await?;
    info!(worker_name = %name, "Worker registered with manager");

    // Worker telemetry: opt-in Prometheus listener (/metrics + /healthz,
    // `legion_worker_*` families) so a fleet can prove its workers are not
    // the bottleneck. Env-var rather than config: workers joined with a bare
    // token have no config file, and packed multi-worker hosts need
    // per-process ports anyway (port 0 = ephemeral, logged on bind). See
    // docs/metrics.md.
    if let Ok(port) = std::env::var("LEGION_WORKER_TELEMETRY_PORT") {
        match port.trim().parse::<u16>() {
            Ok(port) => {
                let telemetry_addr: std::net::SocketAddr = ([0, 0, 0, 0], port).into();
                tokio::spawn(async move {
                    if let Err(err) =
                        crate::manager::telemetry::serve_worker_telemetry(telemetry_addr).await
                    {
                        error!(error = ?err, "worker telemetry listener exited");
                    }
                });
            }
            Err(_) => warn!(
                value = %port,
                "Ignoring LEGION_WORKER_TELEMETRY_PORT: not a valid port number"
            ),
        }
    }
    // OTLP metrics push (opt-in via the OTEL_* env vars; no-op otherwise).
    crate::otel::start_metrics_push();

    // Registration just succeeded, so "now" is an honest last-contact time;
    // every successful heartbeat refreshes it. The self-fence watchdog kills
    // stateful allocations whose `selfTerminateAfter` window outlives this
    // clock (see docs/stateful-workloads.md "Node loss and fencing").
    let last_successful_heartbeat = Arc::new(Mutex::new(std::time::Instant::now()));
    // Node-local connect datapath. It owns no state of its own: every
    // heartbeat reply tells it which loopback listeners to run and where each
    // one may send traffic. Idle (and free) until a task declares upstreams.
    let forwarder = Arc::new(UpstreamForwarder::new());
    let heartbeat_fut = start_heartbeats(
        name.clone(),
        client.clone(),
        task_manager.clone(),
        identity_manager.clone(),
        last_successful_heartbeat.clone(),
        credentials.clone(),
        registration,
        forwarder,
        heartbeat_interval,
        edge_boot.as_ref().map(|runtime| runtime.spec_cache.clone()),
    );
    let self_fence_fut = start_self_fence_watchdog(task_manager.clone(), last_successful_heartbeat);
    let checks_fut = start_node_checks(
        name.clone(),
        config.additional_node_checks.clone(),
        client,
        task_manager.clone(),
        credentials,
    );
    let log_maintenance_fut = start_log_maintenance(task_manager.clone());
    // Notices task processes exiting, reaps them, and applies each task's
    // `restart` policy (see docs/task-supervision.md).
    let supervisor_fut = start_task_supervisor(task_manager.clone());
    let _res = join!(
        heartbeat_fut,
        self_fence_fut,
        checks_fut,
        log_maintenance_fut,
        supervisor_fut,
        worker_server_fut
    );

    Ok(())
}

/// Assemble this node's edge-boot runtime: the JWKS-backed ticket verifier,
/// the spec cache, and the ratifier that calls the manager.
fn build_edge_boot_runtime(
    config: &crate::structs::WorkerEdgeBootConfigFull,
    manager_addr: &str,
    node_id: &str,
    pool: &str,
    client: ManagerClient<Channel>,
    credentials: Arc<WorkerCredentials>,
) -> crate::worker::server::EdgeBootRuntime {
    let refresh = humantime::parse_duration(&config.jwks_refresh)
        .unwrap_or(crate::worker::boot_ticket::DEFAULT_JWKS_REFRESH);
    let skew = humantime::parse_duration(&config.clock_skew)
        .unwrap_or(crate::worker::boot_ticket::DEFAULT_CLOCK_SKEW);
    // The issuer URL is the manager address the worker already trusts (it is
    // what its join token pointed at), and the JWKS lives at the well-known
    // path the manager serves alongside gRPC.
    let issuer = manager_addr.trim_end_matches('/').to_string();
    let jwks_url = format!("{issuer}/.well-known/jwks.json");
    let source = Box::new(crate::worker::boot_ticket::HttpJwksSource::new(
        jwks_url,
        Duration::from_secs(5),
    ));
    crate::worker::server::EdgeBootRuntime {
        verifier: Arc::new(crate::worker::boot_ticket::BootTicketVerifier::new(
            source, issuer, pool, refresh, skew,
        )),
        spec_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        node_id: node_id.to_string(),
        ratifier: Arc::new(ManagerClaimRatifier {
            client,
            credentials,
        }),
    }
}

/// Ratifies claims by calling the manager's `RatifyClaim`, authenticated with
/// this node's own credentials.
struct ManagerClaimRatifier {
    client: ManagerClient<Channel>,
    credentials: Arc<WorkerCredentials>,
}

#[tonic::async_trait]
impl crate::worker::server::ClaimRatifier for ManagerClaimRatifier {
    async fn ratify(
        &self,
        request: crate::structs::RatifyClaimRequest,
    ) -> R<crate::structs::RatifyClaimReply> {
        let mut client = self.client.clone();
        let request = self.credentials.attach(tonic::Request::new(request));
        Ok(client.ratify_claim(request).await?.into_inner())
    }
}

#[instrument(err, skip_all)]
pub(crate) async fn connect_manager_with_retry(manager_addr: &str) -> R<ManagerClient<Channel>> {
    let mut backoff = Duration::from_millis(200);
    let max_backoff = Duration::from_secs(5);

    loop {
        match create_manager_client(manager_addr.to_string(), String::new()).await {
            Ok(client) => return Ok(client),
            Err(err) => {
                warn!(
                    error = ?err,
                    %manager_addr,
                    "Failed to connect to manager; retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, max_backoff);
            }
        }
    }
}

/// Publishes the worker's `resourceEnforcement` settings into the environment
/// the task runtimes read (see `docs/task-supervision.md`). An empty or
/// unparseable payload leaves enforcement off, which is the default and the
/// behavior every worker had before this existed.
fn apply_resource_enforcement_env(raw: &str) {
    let config = if raw.trim().is_empty() {
        crate::structs::WorkerResourceEnforcementConfig::default()
    } else {
        match serde_json::from_str::<crate::structs::WorkerResourceEnforcementConfig>(raw) {
            Ok(config) => config,
            Err(err) => {
                warn!(error = ?err, "Invalid worker resourceEnforcement config; enforcement off");
                crate::structs::WorkerResourceEnforcementConfig::default()
            }
        }
    };

    if config.enabled {
        info!(
            cgroup_parent = %config.cgroup_parent,
            "Resource enforcement enabled: exec tasks run in worker-owned cgroups"
        );
        std::env::set_var(crate::constants::ENV_RESOURCE_ENFORCEMENT, "1");
        std::env::set_var(crate::constants::ENV_CGROUP_PARENT, &config.cgroup_parent);
    } else {
        std::env::remove_var(crate::constants::ENV_RESOURCE_ENFORCEMENT);
        std::env::remove_var(crate::constants::ENV_CGROUP_PARENT);
    }
}

/// Parses the worker config's `artifacts` JSON payload (empty = defaults).
fn parse_artifact_fetch_config(raw: &str) -> R<crate::structs::WorkerArtifactFetchConfigFull> {
    if raw.trim().is_empty() {
        return Ok(crate::structs::WorkerArtifactFetchConfigFull::default());
    }
    let config: crate::structs::WorkerArtifactsConfig =
        serde_json::from_str(raw).context("parse worker artifacts config")?;
    config.fetch_with_defaults()
}

fn resolve_worker_name(name_override: Option<&str>, name_prefix: Option<String>) -> R<String> {
    let workers_dir = PathBuf::from(DEF_WORKER_STATE_DIR);
    if let Some(name) = name_override {
        ensure_dir(&workers_dir.join(name))?;
        return Ok(name.to_string());
    }

    let existing = find_existing_worker_id(&workers_dir)?;
    if let Some(id) = existing {
        return Ok(id);
    }

    let name_core = random_string(8);
    let name = match name_prefix {
        Some(prefix) => format!("{prefix}-{name_core}"),
        None => name_core,
    };
    ensure_dir(&workers_dir.join(&name))?;
    Ok(name)
}

fn find_existing_worker_id(workers_dir: &Path) -> R<Option<String>> {
    if !workers_dir.exists() {
        return Ok(None);
    }
    let mut entries = Vec::new();
    for entry in fs::read_dir(workers_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            entries.push(entry.file_name());
        }
    }

    match entries.len() {
        0 => Ok(None),
        1 => Ok(entries.pop().and_then(|name| name.into_string().ok())),
        _ => Err(anyhow!(
            "Multiple worker IDs found in {}; specify --name to choose one",
            workers_dir.display()
        )),
    }
}

fn ensure_dir(path: &Path) -> R<()> {
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    fs::create_dir_all(path)?;
    Ok(())
}

#[instrument(err, skip_all)]
async fn create_manager_client(
    manager_addr: String,
    _ca_cert: String,
) -> R<ManagerClient<Channel>> {
    // let ca_cert = Certificate::from_pem(ca_cert);
    // let tls: ClientTlsConfig = ClientTlsConfig::new().ca_certificate(ca_cert);
    let channel = Channel::from_shared(manager_addr)?
        // .tls_config(tls)? TODO: Temporarily doesn' do any TLS
        .connect()
        .await?;
    let client = ManagerClient::new(channel);

    Ok(client)
}

// Failed heartbeats retry forever with exponential backoff in this range. The
// cap stays low (vs the usual 30s) so a reconnecting worker is detected
// promptly — the self-fence clock keeps running regardless.
const HEARTBEAT_RETRY_MIN: Duration = Duration::from_millis(500);
const HEARTBEAT_RETRY_MAX: Duration = Duration::from_secs(10);

/// Heartbeat cadence: worker-type config (`heartbeatSeconds`, clamped to
/// 1..=30) or the built-in default. The manager's lost window stays 10s,
/// so intervals above ~3s trade detection margin for load — the manager
/// warns when serving such a config (docs/architecture-managers-workers.md).
/// Shared with the simulated-fleet mode (`worker::simulate`), whose
/// fairness contract is "same cadence resolution as a real worker".
pub(crate) fn heartbeat_interval_for(config: &WorkerConfig) -> Duration {
    Duration::from_secs(
        config
            .heartbeat_seconds
            .map(|seconds| u64::from(seconds).clamp(1, 30))
            .unwrap_or(WORKER_HEARTBEAT_DURATION_SEC),
    )
}

/// The heartbeat sleep, jittered ±10% so a fleet that (re)started together
/// does not stay phase-locked forever — 10k workers heartbeating in the
/// same instant is a self-inflicted thundering herd (heartbeat-scale plan
/// §5). Also what lets the manager-side heartbeat group commit form
/// batches from *steady-state* arrivals rather than only from bursts.
/// Shared with the simulated-fleet mode for the same fairness reason.
pub(crate) fn jittered_heartbeat_interval(interval: Duration) -> Duration {
    use rand::Rng;
    let jitter = rand::thread_rng().gen_range(0.9..=1.1);
    interval.mul_f64(jitter)
}
// How often the self-fence watchdog re-checks `last_successful_heartbeat`
// against each running allocation's window.
const SELF_FENCE_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Heartbeat loop. A failed heartbeat must NEVER exit the loop (tasks keep
/// running while the manager is unreachable — exiting would orphan them and
/// disable self-fencing); failures are retried forever with backoff while
/// `last_successful_heartbeat` tracks the last time the manager actually
/// acknowledged us. The self-fence watchdog reads that clock.
// The loop drives every per-heartbeat subsystem (tasks, identities, upstream
// forwarders, self-fence clock); they are threaded by parameter rather than
// bundled so each one's lifetime stays visible at the call site.
#[allow(clippy::too_many_arguments)]
#[instrument(err, skip_all)]
async fn start_heartbeats(
    name: String,
    client: ManagerClient<Channel>,
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    identity_manager: Arc<WorkerIdentityManager>,
    last_successful_heartbeat: Arc<Mutex<std::time::Instant>>,
    credentials: Arc<WorkerCredentials>,
    registration: RegistrationPayload,
    forwarder: Arc<UpstreamForwarder>,
    heartbeat_interval: Duration,
    // Edge-boot spec cache to keep warm from the desired set; `None` on a
    // node that does not accept edge starts.
    spec_cache: Option<crate::worker::edge::SpecCache>,
) -> R<()> {
    let mut backoff = HEARTBEAT_RETRY_MIN;

    loop {
        let tick_started = std::time::Instant::now();
        let tick_result = heartbeat_tick(
            &name,
            &client,
            &task_manager,
            &identity_manager,
            &credentials,
            &forwarder,
            spec_cache.as_ref(),
        )
        .await;
        let worker_metrics = crate::manager::telemetry::worker_metrics();
        worker_metrics
            .heartbeat_duration_seconds
            .observe(tick_started.elapsed().as_secs_f64());
        worker_metrics.heartbeats_total.inc(&[match &tick_result {
            Ok(()) => "ok",
            Err(err)
                if err
                    .downcast_ref::<tonic::Status>()
                    .is_some_and(|status| status.code() == tonic::Code::Unauthenticated) =>
            {
                "unauthenticated"
            }
            Err(_) => "error",
        }]);
        match tick_result {
            Ok(()) => {
                *last_successful_heartbeat
                    .lock()
                    .expect("heartbeat clock lock") = std::time::Instant::now();
                backoff = HEARTBEAT_RETRY_MIN;
                tokio::time::sleep(jittered_heartbeat_interval(heartbeat_interval)).await;
            }
            // The node token can be revoked/rotated (for example ACLs were
            // just enabled, or the manager state was reset). Re-register with
            // the join secret to obtain a fresh credential, then carry on.
            Err(err)
                if err
                    .downcast_ref::<tonic::Status>()
                    .is_some_and(|status| status.code() == tonic::Code::Unauthenticated) =>
            {
                warn!(worker = %name, "Heartbeat rejected as unauthenticated; re-registering");
                if let Err(err) = register_self(client.clone(), &registration, &credentials).await {
                    warn!(worker = %name, error = ?err, "Re-registration failed; will retry");
                }
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, HEARTBEAT_RETRY_MAX);
            }
            Err(err) => {
                warn!(
                    worker = %name,
                    error = ?err,
                    retry_in_ms = backoff.as_millis() as u64,
                    "Heartbeat failed; retrying (self-fence clock running)"
                );
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, HEARTBEAT_RETRY_MAX);
            }
        }
    }
}

/// One heartbeat round-trip: report running/failed allocations, then
/// reconcile against the manager's desired set.
async fn heartbeat_tick(
    name: &str,
    client: &ManagerClient<Channel>,
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    identity_manager: &Arc<WorkerIdentityManager>,
    credentials: &Arc<WorkerCredentials>,
    forwarder: &Arc<UpstreamForwarder>,
    spec_cache: Option<&crate::worker::edge::SpecCache>,
) -> R<()> {
    let mut client_clone = client.clone();

    {
        let start = std::time::Instant::now();
        let (allocations, failures, terminations, allocation_health, volume_reports) = {
            let mut manager = task_manager.lock().expect("task manager");
            let failures = manager
                .drain_failures()
                .into_iter()
                .map(|failure| crate::structs::FailedAllocation {
                    allocation_id: failure.allocation_id,
                    reason: failure.reason,
                    artifact_states: failure.artifact_states,
                })
                .collect::<Vec<_>>();
            let terminations = manager.drain_terminations();
            let allocation_health = manager.allocation_health_snapshot();
            let volume_reports = manager.drain_volume_reports();
            (
                manager.allocations_snapshot(),
                failures,
                terminations,
                allocation_health,
                volume_reports,
            )
        };
        let running = allocations.clone();
        let failures_for_redrive = failures.clone();
        let terminations_for_redrive = terminations.clone();
        let volume_reports_for_redrive = volume_reports.clone();

        let worker_metrics = crate::manager::telemetry::worker_metrics();
        worker_metrics
            .allocations_running
            .set(allocations.len() as i64);
        if !failures.is_empty() {
            worker_metrics
                .allocation_failures_reported_total
                .add(failures.len() as u64);
        }

        // Unratified work is reported separately from committed allocations:
        // the manager cannot reconcile what it has not recorded, but an
        // operator looking at capacity must still see it.
        let provisional_claims = crate::worker::edge::claim_reports(task_manager);

        let req = credentials.attach(tonic::Request::new(HeartbeatRequest {
            id: name.to_string(),
            provisional_claims,
            allocation_ids: allocations,
            failed_allocations: failures,
            allocation_health,
            volume_reports,
            terminated_allocations: terminations
                .into_iter()
                .map(|report| crate::structs::TerminatedAllocation {
                    allocation_id: report.allocation_id,
                    success: report.success,
                    exit_code: report.exit_code,
                    signal: report.signal,
                    message: report.message,
                })
                .collect(),
        }));

        let response = match client_clone.heartbeat(req).await {
            Ok(response) => response,
            Err(err) => {
                // Drained failure reports must not be lost to a dead
                // connection: put them back so the next successful
                // heartbeat still tells the manager about them.
                let mut manager = task_manager.lock().expect("task manager");
                for failure in failures_for_redrive {
                    manager.record_failure(failure.allocation_id, failure.reason);
                }
                // Same for terminal outcomes: a batch task's completion lost
                // to a dead connection would leave the job "running" forever.
                manager.requeue_terminations(terminations_for_redrive);
                // Same for volume outcomes: a destroy confirmation lost to a
                // dead connection would leave the manager re-dispatching a
                // reclaim forever.
                manager.requeue_volume_reports(volume_reports_for_redrive);
                return Err(err.into());
            }
        };
        debug!(worker = %name, elapsed_ms = start.elapsed().as_millis() as u64, "Heartbeat acknowledged by manager");
        let reply = response.into_inner();
        // The desired set is the other place a node learns job configs it may
        // later be asked to start from a ticket.
        if let Some(cache) = spec_cache {
            crate::worker::edge::remember_specs(cache, &reply.desired_allocations);
        }
        // Catch-up verdicts, for claims whose `RatifyClaim` reply was lost
        // after the manager committed (docs/edge-boot.md). Applied before the
        // reconcile below so a confirmed claim is already an ordinary
        // allocation by the time the desired set is compared.
        for verdict in &reply.claim_verdicts {
            // The committed payload is already on this reply, in
            // `desired_allocations` — the manager lists it the moment the
            // placement commits — so the verdict carries only the decision
            // and this path costs no extra bytes on the wire.
            let committed = verdict
                .confirmed
                .then(|| {
                    reply
                        .desired_allocations
                        .iter()
                        .find(|allocation| allocation.allocation_id == verdict.allocation_id)
                })
                .flatten();
            if verdict.confirmed && committed.is_none() {
                // Confirmed but not yet listed (the read model refreshes just
                // after the commit): leave the claim alone and pick it up on
                // the next tick, still inside its lease.
                debug!(
                    claim_id = %verdict.claim_id,
                    allocation_id = verdict.allocation_id,
                    "Verdict arrived before the allocation appeared in the desired set"
                );
                continue;
            }
            crate::worker::edge::apply_verdict(
                task_manager,
                &verdict.claim_id,
                committed,
                crate::structs::edge_boot::RefusalReason::from_wire(verdict.reason),
                &verdict.detail,
            );
        }
        // The lease is the backstop for every failure the reply path cannot
        // cover: a partition, a dead manager, a lost verdict. It runs on the
        // heartbeat tick because that is the one loop guaranteed to keep
        // running while everything else is stuck.
        crate::worker::edge::sweep_leases(task_manager);
        // The manager acknowledged us, so this reply is authoritative for the
        // connect datapath (see `apply_upstreams` on fail-static semantics).
        apply_upstreams(forwarder, &reply).await;
        // Allocations whose launch was dispatched this tick. Their refresh
        // loops are spawned before the (async, unawaited) launch registers
        // them in the task manager, so they must be treated as live by the
        // retain() below or the reconcile-time snapshot could abort the loops
        // we just started and never respawn them.
        // Reclaim work the manager asked for. Runs before allocation
        // reconciliation so a destroy for a volume this node no longer uses
        // does not wait behind a slow task start.
        if !reply.volume_destroy.is_empty() {
            let requests = reply.volume_destroy.clone();
            // Take the Arc under a brief lock, then run the drivers with NO
            // task-manager lock held: a destroy against a hung storage backend
            // must stall that destroy, not the heartbeats this loop exists to
            // send (a worker that cannot heartbeat self-fences its stateful
            // allocations — one stuck volume must not become an outage).
            // spawn_blocking because drivers shell out (umount, rm -rf).
            let volumes = task_manager.lock().expect("task manager").volumes_handle();
            tokio::task::spawn_blocking(move || {
                volumes.destroy_requested(&requests);
            });
        }

        let mut launched_this_tick: Vec<i64> = Vec::new();
        if !reply.desired_allocations.is_empty() {
            // Deliver identity tokens for not-yet-running allocations BEFORE
            // reconcile starts them: env tokens must be present at exec time.
            let mut desired = reply.desired_allocations;
            let mut prepared_by_alloc: HashMap<i64, Vec<PreparedIdentity>> = HashMap::new();
            let running_set: std::collections::HashSet<i64> = running.iter().copied().collect();
            let mut failed_prepare = Vec::new();
            for allocation in desired
                .iter_mut()
                .filter(|allocation| !running_set.contains(&allocation.allocation_id))
            {
                match identity_manager.prepare_task_allocation(allocation).await {
                    Ok(prepared) => {
                        prepared_by_alloc.insert(allocation.allocation_id, prepared);
                    }
                    Err(err) => {
                        // The task must not start without its identities;
                        // report the failure so the manager can reschedule.
                        failed_prepare.push(allocation.allocation_id);
                        task_manager.lock().expect("task manager").record_failure(
                            allocation.allocation_id,
                            format!("Failed to deliver workload identities: {err}"),
                        );
                    }
                }
            }
            desired.retain(|allocation| !failed_prepare.contains(&allocation.allocation_id));

            // Why the manager is stopping things, delivered before the
            // reconcile that acts on it: `reconcile_allocations` consumes
            // these as it stops each undesired allocation, so a notice that
            // landed after the reconcile would describe a stop that already
            // happened.
            let stop_causes = reply
                .stop_notices
                .iter()
                .map(|notice| {
                    (
                        notice.allocation_id,
                        crate::structs::StopCause::from_wire(&notice.cause),
                    )
                })
                .collect();
            // Preemptor links ride the same feed; only preemption notices
            // carry one, so this map is empty on every other cause.
            let stop_preemptors = reply
                .stop_notices
                .iter()
                .filter(|notice| !notice.preemptor_job.is_empty())
                .map(|notice| {
                    (
                        notice.allocation_id,
                        crate::structs::StopNoticePreemptor {
                            job: notice.preemptor_job.clone(),
                            band: notice.preemptor_band.clone(),
                        },
                    )
                })
                .collect();

            let new_allocations = {
                let mut manager = task_manager.lock().expect("task manager");
                manager.set_stop_causes(stop_causes);
                manager.set_stop_preemptors(stop_preemptors);
                manager.reconcile_allocations(&desired)
            };
            if !new_allocations.is_empty() {
                // Refresh loops are keyed by allocation id and start slightly
                // before the task process registers (the launch below is
                // dispatched but not awaited); record the ids as live-this-tick
                // so retain() doesn't abort the loops before registration. If a
                // launch actually fails, the allocation is absent from the next
                // tick's snapshot and that tick's retain prunes its loops.
                launched_this_tick.extend(new_allocations.iter().map(|a| a.allocation_id));
                for allocation in &new_allocations {
                    if let Some(prepared) = prepared_by_alloc.remove(&allocation.allocation_id) {
                        identity_manager.spawn_refreshers(
                            allocation.allocation_id,
                            prepared,
                            task_manager.clone(),
                        );
                    }
                }
                // Launch off the heartbeat task: artifact-less tasks spawn
                // processes synchronously, artifact tasks kick off staging
                // threads; neither should stall the heartbeat loop.
                let task_manager = task_manager.clone();
                tokio::task::spawn_blocking(move || {
                    for allocation in &new_allocations {
                        // Failures are recorded inside (heartbeat failure path).
                        let _ = launch_task_allocation(&task_manager, allocation);
                    }
                });
            }
        }

        // Refresh loops must never outlive their allocation — but treat
        // allocations launched this tick as live even if their async launch
        // hasn't registered them in the snapshot yet.
        let mut live = task_manager
            .lock()
            .expect("task manager")
            .allocations_snapshot();
        live.extend(launched_this_tick);
        identity_manager.retain(&live);
    }

    Ok(())
}

/// Drive the node-local connect datapath from one *successful* heartbeat
/// reply: reconcile the loopback listeners against the desired allocation set,
/// then publish this tick's endpoint snapshot.
///
/// Only called after the manager acknowledged the heartbeat, which is what
/// makes the datapath fail-static — while the manager is unreachable the
/// forwarders keep their last known good endpoints instead of blackholing
/// traffic. Within a successful reply an empty endpoint list *is* authoritative
/// (a killed or intention-denied backend must stop receiving traffic), while an
/// upstream missing from `upstream_endpoints` entirely keeps its current list.
async fn apply_upstreams(forwarder: &UpstreamForwarder, reply: &HeartbeatReply) {
    let specs = upstream_specs(&reply.desired_allocations);
    forwarder.sync(&specs).await;
    for update in &reply.upstream_endpoints {
        forwarder.set_endpoints(
            update.allocation_id,
            &update.service,
            dialable_endpoints(update),
        );
    }
}

/// Derive the listeners a desired allocation set implies: one per upstream of
/// every allocation whose task declares a `connect` block.
///
/// `task_connect` is the declaration side (which services, which strategy);
/// the loopback ports those upstreams were *assigned* live on the allocation's
/// network, next to its host ports, because both come out of the same per-node
/// port space. Allocations that declare no upstreams cost one string check.
fn upstream_specs(desired: &[TaskAllocation]) -> Vec<UpstreamSpec> {
    let mut specs = Vec::new();
    for allocation in desired {
        if allocation.task_connect.trim().is_empty() || allocation.network.trim().is_empty() {
            continue;
        }
        let network: AllocNetwork = match serde_json::from_str(&allocation.network) {
            Ok(network) => network,
            Err(err) => {
                // Retrying cannot fix a payload the manager wrote; skip this
                // allocation's upstreams rather than the whole heartbeat.
                warn!(
                    allocation_id = allocation.allocation_id,
                    error = ?err,
                    "Skipping upstream forwarders; invalid allocation network payload"
                );
                continue;
            }
        };
        for upstream in &network.upstreams {
            specs.push(UpstreamSpec {
                allocation_id: allocation.allocation_id,
                service: upstream.service.clone(),
                local_port: upstream.local_port,
                balance: upstream.balance.clone(),
            });
        }
    }
    specs
}

/// Endpoints from one `UpstreamEndpoints` entry that the forwarder can
/// actually dial. The manager mirrors `ResolveService` and keeps entries whose
/// node advertises no address (or whose port is unassigned); those are not
/// dialable, so they are dropped here rather than turned into a bogus
/// `:0` address. Endpoints are already healthy-only and intention-filtered.
fn dialable_endpoints(update: &UpstreamEndpoints) -> Vec<Endpoint> {
    update
        .endpoints
        .iter()
        .filter(|endpoint| !endpoint.address.trim().is_empty() && endpoint.port != 0)
        .map(|endpoint| Endpoint {
            addr: format!("{}:{}", endpoint.address, endpoint.port),
            allocation_id: endpoint.allocation_id,
            node_id: endpoint.node_id.clone(),
        })
        .collect()
}

/// Self-fence watchdog (the worker half of the at-most-one guarantee; see
/// docs/stateful-workloads.md "Node loss and fencing"). Independent of the
/// heartbeat loop — it must keep running precisely while heartbeats are
/// failing — it compares the time since the last successful heartbeat
/// against each running stateful allocation's `selfTerminateAfter` window
/// and kills any allocation past it. Fenced allocations are recorded as
/// failed, so the first post-reconnect heartbeat reports them
/// failed/stopped rather than running and the manager reschedules them
/// normally.
async fn start_self_fence_watchdog(
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    last_successful_heartbeat: Arc<Mutex<std::time::Instant>>,
) -> R<()> {
    loop {
        let since_last_contact = last_successful_heartbeat
            .lock()
            .expect("heartbeat clock lock")
            .elapsed();
        let fenced = task_manager
            .lock()
            .expect("task manager")
            .self_fence_expired_allocations(since_last_contact);
        if !fenced.is_empty() {
            warn!(
                fenced_allocations = ?fenced,
                lost_contact_secs = since_last_contact.as_secs(),
                "Self-fence watchdog killed allocations after prolonged manager silence"
            );
        }
        tokio::time::sleep(SELF_FENCE_CHECK_INTERVAL).await;
    }
}

/// Periodically rotate/prune captured task logs across all active allocations
/// so a chatty task cannot fill the worker's disk. Runs for the lifetime of the
/// worker; never returns on the happy path.
#[instrument(skip_all)]
async fn start_log_maintenance(task_manager: Arc<Mutex<WorkerTaskManager>>) -> R<()> {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(
            DEF_TASK_LOG_MAINTENANCE_SEC,
        ))
        .await;
        task_manager.lock().expect("task manager").maintain_logs();
    }
}

#[instrument(err, skip_all)]
async fn start_node_checks(
    name: String,
    additional_node_checks: Vec<String>,
    client: ManagerClient<Channel>,
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    credentials: Arc<WorkerCredentials>,
) -> R<()> {
    let mut client_clone = client.clone();

    loop {
        let checks_data = {
            let readiness = task_manager
                .lock()
                .expect("task manager")
                .readiness_checks();
            run_health_checks(additional_node_checks.clone(), Some(readiness))
        };
        let serialized_health_check_data = serde_json::to_string(&checks_data)?;
        let req = credentials.attach(tonic::Request::new(HealthCheckRequest {
            id: name.clone(),
            health_checks: serialized_health_check_data.clone(),
        }));

        match client_clone.health_check(req).await {
            Ok(response) => {
                debug!(worker = %name, "Health check acknowledged: {:?}", response.get_ref().message)
            }
            // The heartbeat loop owns re-registration; here just wait for it.
            Err(status) => {
                warn!(worker = %name, code = %status.code(), "Health check failed: {}", status.message())
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(
            WORKER_NODE_CHECK_DURATION_SEC,
        ))
        .await;
    }
}

#[instrument(err, skip_all)]
async fn register_self(
    client: ManagerClient<Channel>,
    registration: &RegistrationPayload,
    credentials: &Arc<WorkerCredentials>,
) -> R<()> {
    let mut c2 = client.clone();

    let build_version = env!("CARGO_PKG_VERSION");
    let req = tonic::Request::new(WorkerRegisterRequest {
        name: registration.name.clone(),
        config_attrs: registration.config_attrs.clone(),
        fingerprint_attrs: registration.fingerprint_attrs.clone(),
        health_checks: registration.health_checks.clone(),
        pool: registration.pool.clone(),
        join_token: credentials.join_secret.clone(),
        build_version: build_version.to_string(),
        protocol_version: PROTOCOL_VERSION,
    });

    let response = c2.worker_register(req).await?;
    let reply = response.into_inner();
    if reply.node_token.is_some() {
        info!(worker = %registration.name, "Received node token from manager");
    }
    credentials.store_node_token(reply.node_token);

    info!(
        worker = %registration.name,
        manager_build_version = %reply.manager_build_version,
        manager_protocol_version = reply.manager_protocol_version,
        worker_build_version = %build_version,
        worker_protocol_version = PROTOCOL_VERSION,
        "Registered with manager"
    );
    if reply.manager_build_version != build_version {
        warn!(
            worker = %registration.name,
            manager_build_version = %reply.manager_build_version,
            worker_build_version = %build_version,
            "Manager and worker build versions differ; align versions when \
             convenient (see docs/upgrades.md)"
        );
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn pre_register_self(
    client: ManagerClient<Channel>,
    worker_type: String,
    credentials: &Arc<WorkerCredentials>,
) -> R<Response<WorkerPreRegisterReply>> {
    let mut c2 = client.clone();

    let req = tonic::Request::new(WorkerPreRegisterRequest {
        worker_type,
        join_token: credentials.join_secret.clone(),
    });

    let response = c2.worker_pre_register(req).await?;

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::identity::{ManagerTokenIssuer, WorkerIdentityManager};
    use crate::worker::state::WorkerStateStore;
    use tempfile::TempDir;

    fn dead_manager_client() -> ManagerClient<Channel> {
        // connect_lazy: building the client succeeds, every RPC fails — the
        // exact shape of a manager partition.
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        ManagerClient::new(channel)
    }

    /// Regression: heartbeat failures used to `?`-exit the loop, leaving
    /// tasks running with no supervision and no self-fencing. The loop must
    /// survive an unreachable manager and keep retrying.
    #[tokio::test]
    async fn heartbeat_loop_survives_failed_heartbeats() -> R<()> {
        let temp_dir = TempDir::new()?;
        let task_manager = Arc::new(Mutex::new(WorkerTaskManager::with_state_store(
            WorkerStateStore::new(temp_dir.path().join("worker_state.json")),
        )));
        let client = dead_manager_client();
        let credentials = Arc::new(WorkerCredentials::default());
        let identity_manager = Arc::new(WorkerIdentityManager::new(Arc::new(ManagerTokenIssuer {
            client: client.clone(),
            node_id: "test-worker".into(),
            credentials: credentials.clone(),
        })));
        let clock = Arc::new(Mutex::new(std::time::Instant::now()));

        let heartbeats = tokio::spawn(start_heartbeats(
            "test-worker".into(),
            client,
            task_manager,
            identity_manager,
            clock.clone(),
            credentials,
            RegistrationPayload {
                name: "test-worker".into(),
                pool: "default".into(),
                config_attrs: "{}".into(),
                fingerprint_attrs: "{}".into(),
                health_checks: "[]".into(),
            },
            Arc::new(UpstreamForwarder::new()),
            Duration::from_secs(WORKER_HEARTBEAT_DURATION_SEC),
            None,
        ));

        // Long enough for several failed attempts (first retry after 500ms).
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(
            !heartbeats.is_finished(),
            "heartbeat loop must keep retrying through failures, not exit"
        );
        // No heartbeat ever succeeded, so the self-fence clock kept aging.
        let since_last_contact = clock.lock().expect("clock").elapsed();
        assert!(
            since_last_contact >= Duration::from_millis(1200),
            "last_successful_heartbeat must not advance on failures \
             (elapsed {since_last_contact:?})"
        );

        heartbeats.abort();
        Ok(())
    }

    /// End-to-end watchdog behavior against the real clock plumbing (short
    /// real durations, no manager): an allocation with a tiny window is
    /// fenced once the clock ages past it; one with a large window survives.
    #[tokio::test]
    async fn watchdog_fences_only_allocations_past_their_window() -> R<()> {
        let temp_dir = TempDir::new()?;
        let task_manager = Arc::new(Mutex::new(WorkerTaskManager::with_state_store(
            WorkerStateStore::new(temp_dir.path().join("worker_state.json")),
        )));
        {
            let mut manager = task_manager.lock().expect("task manager");
            manager.load_builtin_plugins();
            let task = crate::structs::TaskConfig {
                config: json!({"command": ["sleep", "5"]}),
                ..crate::structs::TaskConfig::new("exec-task", "exec")
            };
            let mut allocation = crate::structs::Allocation {
                id: 1,
                status: "running".into(),
                resources: json!({}),
                node_id: "node-1".into(),
                job_config_id: "job-1".into(),
                task_config_id: 1,
                job_id: "job".into(),
                instance_id: None,
                network: None,
            };
            manager.run_allocation(&allocation, &task)?;
            manager.set_self_fence_window(1, Duration::from_millis(50));
            allocation.id = 2;
            manager.run_allocation(&allocation, &task)?;
            manager.set_self_fence_window(2, Duration::from_secs(3600));
        }

        // A clock that last saw the manager 200ms ago: past allocation 1's
        // 50ms window, far under allocation 2's hour.
        let clock = Arc::new(Mutex::new(
            std::time::Instant::now() - Duration::from_millis(200),
        ));
        let watchdog = tokio::spawn(start_self_fence_watchdog(task_manager.clone(), clock));

        // First watchdog tick runs immediately.
        tokio::time::sleep(Duration::from_millis(300)).await;
        watchdog.abort();

        let mut manager = task_manager.lock().expect("task manager");
        let failures = manager.drain_failures();
        assert_eq!(failures.len(), 1, "exactly one allocation fenced");
        assert_eq!(failures[0].allocation_id, 1);
        assert!(failures[0].reason.contains("self-fenced"));
        assert!(
            manager.has_allocation(2),
            "under-window allocation must keep running"
        );

        // Cleanup the surviving sleep process.
        if let Some(pid) = manager.allocation_pid(2) {
            let mut system = sysinfo::System::new();
            system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
            if let Some(process) = system.process(sysinfo::Pid::from_u32(pid)) {
                process.kill();
            }
        }
        Ok(())
    }

    /// A desired allocation declaring one upstream, with the loopback port it
    /// was assigned at placement time.
    fn connect_allocation(allocation_id: i64, service: &str, local_port: u16) -> TaskAllocation {
        TaskAllocation {
            provisional: None,
            allocation_id,
            task_connect: json!({ "upstreams": [{ "service": service }] }).to_string(),
            network: json!({
                "ports": [{ "name": "http", "hostPort": 20000 }],
                "upstreams": [{
                    "service": service,
                    "localPort": local_port,
                    "balance": "least_conn",
                }],
            })
            .to_string(),
            ..TaskAllocation::default()
        }
    }

    #[test]
    fn upstream_specs_come_from_the_allocation_network() {
        let specs = upstream_specs(&[connect_allocation(11, "db", 20003)]);
        assert_eq!(
            specs,
            vec![UpstreamSpec {
                allocation_id: 11,
                service: "db".into(),
                local_port: 20003,
                balance: "least_conn".into(),
            }]
        );
    }

    #[test]
    fn upstream_specs_skip_allocations_without_connect() {
        let plain = TaskAllocation {
            provisional: None,
            allocation_id: 1,
            network: json!({ "ports": [{ "name": "http", "hostPort": 20000 }] }).to_string(),
            ..TaskAllocation::default()
        };
        // Declared upstreams but no network yet (or a corrupt payload) cannot
        // produce a listener — there is no port to bind.
        let unplaced = TaskAllocation {
            provisional: None,
            allocation_id: 2,
            task_connect: json!({ "upstreams": [{ "service": "db" }] }).to_string(),
            ..TaskAllocation::default()
        };
        let corrupt = TaskAllocation {
            provisional: None,
            allocation_id: 3,
            task_connect: json!({ "upstreams": [{ "service": "db" }] }).to_string(),
            network: "{not json".into(),
            ..TaskAllocation::default()
        };
        assert!(upstream_specs(&[plain, unplaced, corrupt]).is_empty());
    }

    #[test]
    fn dialable_endpoints_drop_entries_the_worker_cannot_dial() {
        let update = UpstreamEndpoints {
            allocation_id: 11,
            service: "db".into(),
            endpoints: vec![
                crate::structs::ServiceEndpoint {
                    address: "10.0.0.5".into(),
                    port: 20001,
                    allocation_id: 42,
                    node_id: "node-b".into(),
                    ..crate::structs::ServiceEndpoint::default()
                },
                // Node advertises no address yet.
                crate::structs::ServiceEndpoint {
                    address: String::new(),
                    port: 20002,
                    ..crate::structs::ServiceEndpoint::default()
                },
                // No port assigned to the advertised label.
                crate::structs::ServiceEndpoint {
                    address: "10.0.0.6".into(),
                    port: 0,
                    ..crate::structs::ServiceEndpoint::default()
                },
            ],
        };

        assert_eq!(
            dialable_endpoints(&update),
            vec![Endpoint {
                addr: "10.0.0.5:20001".into(),
                allocation_id: 42,
                node_id: "node-b".into(),
            }]
        );
    }

    /// The forwarder is driven entirely by heartbeat replies: one reply must
    /// both start the listener and fill in where it may send traffic.
    #[tokio::test]
    async fn heartbeat_reply_starts_and_feeds_upstream_forwarders() -> R<()> {
        let local_port = {
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let port = socket.local_addr()?.port();
            drop(socket);
            port
        };
        let forwarder = UpstreamForwarder::new();
        let reply = HeartbeatReply {
            claim_verdicts: Vec::new(),
            message: "ack".into(),
            desired_allocations: vec![connect_allocation(11, "db", local_port)],
            volume_destroy: Vec::new(),
            stop_notices: Vec::new(),
            upstream_endpoints: vec![UpstreamEndpoints {
                allocation_id: 11,
                service: "db".into(),
                endpoints: vec![crate::structs::ServiceEndpoint {
                    address: "10.0.0.5".into(),
                    port: 20001,
                    allocation_id: 42,
                    node_id: "node-b".into(),
                    ..crate::structs::ServiceEndpoint::default()
                }],
            }],
        };

        apply_upstreams(&forwarder, &reply).await;
        assert_eq!(
            forwarder.active(),
            vec![UpstreamSpec {
                allocation_id: 11,
                service: "db".into(),
                local_port,
                balance: "least_conn".into(),
            }]
        );
        // The listener is live on the loopback port the task will dial.
        assert!(
            tokio::net::TcpListener::bind(("127.0.0.1", local_port))
                .await
                .is_err(),
            "the forwarder must hold the upstream's loopback port"
        );

        // The allocation going away takes its listener with it.
        apply_upstreams(
            &forwarder,
            &HeartbeatReply {
                claim_verdicts: Vec::new(),
                message: "ack".into(),
                ..HeartbeatReply::default()
            },
        )
        .await;
        assert!(forwarder.active().is_empty());
        Ok(())
    }
}
