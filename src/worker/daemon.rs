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
use std::sync::LazyLock;
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
    // The datapath's persisted inputs, replayed before the first heartbeat
    // (docs/network-handlers.md, "The datapath across worker restarts").
    let _ = NETD_STATE.set(crate::worker::netd::state::NetdStateStore::beside(
        &state_path,
    ));
    // Access logs sit next to each allocation's own logs.
    let logs_base_dir = WorkerStateStore::new(state_path.clone()).logs_base_dir();
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
            // Same file-delivery mapping the original launch used, so a
            // refresh rewrites the file the workload is actually reading.
            let delivery_base = task_manager
                .lock()
                .expect("task manager")
                .file_delivery_base(allocation.id, &task.name, &task.runtime)
                .map(
                    |(in_task_root, host_task_dir)| crate::worker::identity::FileDeliveryBase {
                        in_task_root,
                        host_task_dir,
                    },
                );
            let prepared = task
                .identities
                .iter()
                .map(|identity| PreparedIdentity {
                    identity: identity.clone(),
                    expires_at: 0,
                    delivery_base: delivery_base.clone(),
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
    // The cadence this node will actually beat at, so the manager can derive
    // its lost window from it (`missedBeatsBeforeLost x this`) instead of
    // applying one global constant to a fleet with several cadences. Reported
    // rather than looked up because the node record carries no worker type —
    // and it is the RESOLVED value, so what the manager records is what the
    // worker does. See docs/heartbeat-cadence-and-scale.md.
    config_attrs.insert(
        "heartbeatSeconds".into(),
        json!(heartbeat_interval.as_secs()),
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
    crate::worker::balance::set_local_node(&name);

    start_worker_telemetry();

    // Registration just succeeded, so "now" is an honest last-contact time;
    // every successful heartbeat refreshes it. The self-fence watchdog kills
    // stateful allocations whose `selfTerminateAfter` window outlives this
    // clock (see docs/stateful-workloads.md "Node loss and fencing").
    let last_successful_heartbeat = Arc::new(Mutex::new(std::time::Instant::now()));
    // Node-local connect datapath. It owns no state of its own: every
    // heartbeat reply tells it which loopback listeners to run and where each
    // one may send traffic. Idle (and free) until a task declares upstreams.
    // The connect datapath, with the policy cache and denial ledger the
    // heartbeat loop feeds and drains (docs/network-handlers.md).
    let net_context = Arc::new(
        crate::worker::forwarder::ForwarderContext::new(
            Arc::new(crate::worker::netd::NetPolicyCache::new()),
            Arc::new(crate::worker::netd::Denials::new()),
        )
        .with_logs_base_dir(logs_base_dir.clone()),
    );
    let net_dns = Arc::new(crate::worker::netd::dns::NetDns::new(
        net_context.clone(),
        crate::worker::netd::dns::NetDns::host_forwarders(),
    ));
    let net_proxies = Arc::new(crate::worker::netd::proxy::EgressProxies::new(
        net_context.clone(),
    ));
    let net_ingress = Arc::new(crate::worker::netd::ingress::IngressListeners::new(
        net_context.clone(),
    ));
    let forwarder = Arc::new(
        UpstreamForwarder::with_context(net_context)
            .with_dns(net_dns)
            .with_proxies(net_proxies)
            .with_ingress(net_ingress),
    );
    // Restored tasks are still running: give them their network back now,
    // not on the first successful heartbeat.
    restore_datapath(&forwarder, &task_manager).await;
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
    let workers_dir = worker_state_dir();
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

/// Start the worker process's telemetry exporters: the opt-in Prometheus
/// listener (`/metrics` + `/healthz`, the `legion_worker_*` families) so a
/// fleet can prove its workers are not the bottleneck, plus the OTLP
/// metrics push (opt-in via the `OTEL_*` env vars; a no-op otherwise).
///
/// The listener is an env var rather than a config field deliberately:
/// workers joined with a bare token have no config file, and packed
/// multi-worker hosts need per-process ports anyway (port 0 = ephemeral,
/// logged on bind). See docs/metrics.md.
///
/// Shared with the simulated-fleet mode (`worker::simulate`) — a sim host
/// is exactly the host a scale campaign scrapes, so the two paths must not
/// drift on how telemetry gets turned on.
pub(crate) fn start_worker_telemetry() {
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
    crate::otel::start_metrics_push();
}

/// Heartbeat cadence: worker-type config (`heartbeatSeconds`, clamped to
/// `1..=MAX_WORKER_HEARTBEAT_SECONDS`) or the built-in default.
///
/// The clamp used to stop at 30 because the lost window was a fixed 10s, so
/// anything above ~3s traded detection margin for load. The window now moves
/// with the cadence (`missedBeatsBeforeLost x this value`), so a slow worker
/// type is a supported configuration — see
/// docs/heartbeat-cadence-and-scale.md.
///
/// Shared with the simulated-fleet mode (`worker::simulate`), whose fairness
/// contract is "same cadence resolution as a real worker".
pub(crate) fn heartbeat_interval_for(config: &WorkerConfig) -> Duration {
    Duration::from_secs(crate::manager::cadence::clamp_heartbeat_seconds(
        config.heartbeat_seconds,
    ))
}

/// The one-shot re-phase a worker performs when its served interval changes.
///
/// When the manager moves the fleet to a new interval, every worker learns it
/// within one OLD interval and would otherwise re-phase onto the same
/// boundary — a thundering herd the +-10% steady-state jitter (a slow random
/// walk) would take a long time to break up. Sleeping `U[0, new)` ONCE spreads
/// the fleet uniformly across the new period in exactly one beat, at the cost
/// of one interval of unevenness.
///
/// Deliberately not Nomad's permanent `[base, 2*base)` stagger: that costs a
/// third of the target rate forever, makes the operator's `N/R` arithmetic
/// wrong by a variable factor, and doubles worst-case detection time.
pub(crate) fn rephase_sleep(new_interval: Duration) -> Duration {
    use rand::Rng;
    new_interval.mul_f64(rand::thread_rng().gen_range(0.0..1.0))
}

/// Adopt a manager-served interval, if there is one and it differs.
///
/// A worker that receives nothing (adaptive cadence off, or an older manager)
/// keeps its worker-type value — exactly today's behavior. A worker that
/// receives one is never penalized for honoring it: the manager derives that
/// node's lost window from the same number, and widens before it tells anyone.
pub(crate) fn adopt_served_interval(current: Duration, served: Option<u32>) -> Option<Duration> {
    let served =
        Duration::from_secs(u64::from(served?).clamp(1, u64::from(MAX_WORKER_HEARTBEAT_SECONDS)));
    (served != current).then_some(served)
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
    // The worker-type cadence this worker starts at. Mutable because the
    // manager may serve a different one on the reply (adaptive cadence,
    // docs/heartbeat-cadence-and-scale.md).
    mut heartbeat_interval: Duration,
    // Edge-boot spec cache to keep warm from the desired set; `None` on a
    // node that does not accept edge starts.
    spec_cache: Option<crate::worker::edge::SpecCache>,
) -> R<()> {
    let mut backoff = HEARTBEAT_RETRY_MIN;

    loop {
        let tick_started = std::time::Instant::now();
        // Stops on this tick drain for one interval of the cadence the node
        // is actually on — how long every other node needs to learn an
        // endpoint is gone (docs/graceful-stop.md).
        task_manager
            .lock()
            .expect("task manager")
            .set_drain_window(heartbeat_interval);
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
            Ok(_) => "ok",
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
            Ok(served) => {
                *last_successful_heartbeat
                    .lock()
                    .expect("heartbeat clock lock") = std::time::Instant::now();
                backoff = HEARTBEAT_RETRY_MIN;
                // Adopt a manager-served interval, once, with a full-width
                // re-phase so a fleet that all learns the new value within one
                // old interval spreads across the new period instead of
                // re-phasing onto the same boundary.
                match adopt_served_interval(heartbeat_interval, served) {
                    Some(next) => {
                        info!(
                            worker = %name,
                            from_seconds = heartbeat_interval.as_secs(),
                            to_seconds = next.as_secs(),
                            "Manager served a new heartbeat interval; re-phasing once"
                        );
                        heartbeat_interval = next;
                        tokio::time::sleep(rephase_sleep(next)).await;
                    }
                    None => {
                        tokio::time::sleep(jittered_heartbeat_interval(heartbeat_interval)).await;
                    }
                }
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
) -> R<Option<u32>> {
    let mut client_clone = client.clone();
    // The cadence the manager wants, when it dictates one. `None` (the
    // default, and every pre-adaptive manager) means "keep your worker-type
    // interval".
    let served_interval;

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

        // Denials the datapath recorded since the last beat. Put back on a
        // failed heartbeat, like every other report on this request.
        let net_denials = forwarder
            .context()
            .denials()
            .map(|denials| denials.drain())
            .unwrap_or_default();
        let net_denials_for_redrive = net_denials.clone();
        // Service-graph samples since the last beat; same restore-on-failure
        // contract as the denials.
        let net_edges = forwarder.context().edges().drain();
        let net_edges_for_redrive = net_edges.clone();
        // The datapath's status for the allocations it was last told about.
        let net_status = {
            let last = LAST_DESIRED.lock().unwrap_or_else(|p| p.into_inner());
            net_status(forwarder, &last)
        };
        // A gated allocation whose capture is not ready has no network: say
        // so in its health, so rollouts and `doctor` see it rather than a
        // task that looks fine and cannot reach anything.
        {
            let mut manager = task_manager.lock().expect("task manager");
            for status in &net_status {
                if status.mode == crate::structs::netpolicy::MODE_GATED
                    && !status.capture_ready
                    && manager.has_allocation(status.allocation_id)
                {
                    manager.set_allocation_health(
                        status.allocation_id,
                        false,
                        format!("net_capture_unavailable: {}", status.problems.join("; ")),
                    );
                }
            }
        }

        let req = credentials.attach(tonic::Request::new(HeartbeatRequest {
            net_denials,
            net_status,
            net_edges,
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
                if let Some(denials) = forwarder.context().denials() {
                    denials.restore(net_denials_for_redrive);
                }
                forwarder.context().edges().restore(net_edges_for_redrive);
                return Err(err.into());
            }
        };
        debug!(worker = %name, elapsed_ms = start.elapsed().as_millis() as u64, "Heartbeat acknowledged by manager");
        let reply = response.into_inner();
        served_interval = reply.next_interval_seconds;
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
        let alloc_root = task_manager
            .lock()
            .expect("task manager")
            .alloc_root()
            .to_path_buf();
        apply_upstreams(forwarder, &reply, &alloc_root).await;
        // Config-file templates, rendered from the same feed; written before
        // any launch below so a task starts with its files in place.
        apply_templates(task_manager, &reply).await;
        // Same authority for network policy: assignments from this reply,
        // bodies fetched by hash, last known good kept on any failure.
        apply_net_policies(forwarder, &mut client_clone, credentials, name, &reply).await;
        apply_mesh_identity(forwarder, &mut client_clone, credentials, name).await;
        *LAST_DESIRED.lock().unwrap_or_else(|p| p.into_inner()) = reply.desired_allocations.clone();
        persist_datapath(forwarder, &reply);
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
                let delivery_base = task_manager
                    .lock()
                    .expect("task manager")
                    .file_delivery_base(
                        allocation.allocation_id,
                        &allocation.task_name,
                        &allocation.task_runtime,
                    )
                    .map(|(in_task_root, host_task_dir)| {
                        crate::worker::identity::FileDeliveryBase {
                            in_task_root,
                            host_task_dir,
                        }
                    });
                match identity_manager
                    .prepare_task_allocation(allocation, delivery_base)
                    .await
                {
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

            // Gated namespaces exist before anything starts (and the
            // datapath listeners inside them are bound by apply_upstreams,
            // which ran above on this same reply).
            {
                // The guard is never held across the await: take the set
                // out, reconcile, put it back.
                let mut gated = std::mem::take(
                    &mut *GATED_ALLOCATIONS.lock().unwrap_or_else(|p| p.into_inner()),
                );
                apply_net_capture(&desired, &mut gated).await;
                *GATED_ALLOCATIONS.lock().unwrap_or_else(|p| p.into_inner()) = gated;
            }
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

    Ok(served_interval)
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
async fn apply_upstreams(
    forwarder: &UpstreamForwarder,
    reply: &HeartbeatReply,
    alloc_root: &std::path::Path,
) {
    let specs = upstream_specs(&reply.desired_allocations);
    forwarder.sync(&specs).await;
    if let Some(dns) = forwarder.dns() {
        dns.sync(&dns_specs(&reply.desired_allocations, alloc_root))
            .await;
    }
    if let Some(proxies) = forwarder.proxies() {
        proxies
            .sync(&proxy_specs(&reply.desired_allocations, alloc_root))
            .await;
    }
    if let Some(ingress) = forwarder.ingress() {
        ingress
            .sync(&ingress_specs(&reply.desired_allocations))
            .await;
    }
    let context = forwarder.context();
    let mut mesh_wanted = false;
    for update in &reply.upstream_endpoints {
        let endpoints = dialable_endpoints(update);
        mesh_wanted |= endpoints.iter().any(|endpoint| endpoint.mesh_tls);
        if update.for_handler {
            // A `proxy` handler's target: dialed on hand-off, no listener.
            context.set_handler_endpoints(update.allocation_id, &update.service, endpoints);
        } else {
            forwarder.set_endpoints(update.allocation_id, &update.service, endpoints);
        }
    }
    let live: std::collections::HashSet<i64> = reply
        .desired_allocations
        .iter()
        .map(|allocation| allocation.allocation_id)
        .collect();
    context.retain_allocations(&live);
    // The node needs a mesh certificate when it dials a mesh endpoint or
    // terminates mesh TLS on an advertised port.
    mesh_wanted |= reply.desired_allocations.iter().any(|allocation| {
        !allocation.network.trim().is_empty()
            && serde_json::from_str::<AllocNetwork>(&allocation.network)
                .map(|network| network.ingress_tls.as_deref() == Some("legion"))
                .unwrap_or(false)
    });
    context.set_mesh_wanted(mesh_wanted);
}

/// Where the datapath's inputs are persisted across restarts.
static NETD_STATE: std::sync::OnceLock<crate::worker::netd::state::NetdStateStore> =
    std::sync::OnceLock::new();
/// Hash of the last persisted state, so an unchanged reply costs no write.
static LAST_NETD_STATE_HASH: LazyLock<Mutex<u64>> = LazyLock::new(|| Mutex::new(0));

/// Save what this reply told the datapath, when it differs from the last
/// save. Off the heartbeat path (a blocking task), owner-only, atomic.
fn persist_datapath(forwarder: &UpstreamForwarder, reply: &HeartbeatReply) {
    let Some(store) = NETD_STATE.get() else {
        return;
    };
    let state = crate::worker::netd::state::NetdState {
        desired: reply.desired_allocations.clone(),
        upstream_endpoints: reply.upstream_endpoints.clone(),
        net_policies: reply.net_policies.clone(),
        policy_bodies: forwarder
            .context()
            .cache()
            .map(|cache| cache.bodies())
            .unwrap_or_default(),
    };
    let hash = {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        serde_json::to_string(&state)
            .unwrap_or_default()
            .hash(&mut hasher);
        hasher.finish()
    };
    {
        let mut last = LAST_NETD_STATE_HASH
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if *last == hash {
            return;
        }
        *last = hash;
    }
    let path = store.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let store = crate::worker::netd::state::NetdStateStore::beside(&path);
        if let Err(err) = store.save(&state) {
            warn!(path = %path.display(), error = ?err, "Failed to persist datapath state");
        }
    });
}

/// Replay the persisted datapath inputs: listeners, VIPs, resolvers, egress
/// proxies, ingress ports, identities and policy bodies come back from
/// disk, so restored allocations have their network before the manager is
/// even contacted. Namespaces persist on their own (bind mounts under
/// `/var/run/netns`); this re-binds what lives inside and beside them.
async fn restore_datapath(
    forwarder: &UpstreamForwarder,
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
) {
    let Some(store) = NETD_STATE.get() else {
        return;
    };
    restore_datapath_from(forwarder, task_manager, store).await;
}

/// [`restore_datapath`] against an explicit store (tests).
async fn restore_datapath_from(
    forwarder: &UpstreamForwarder,
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    store: &crate::worker::netd::state::NetdStateStore,
) {
    let state = match store.load() {
        Ok(state) => state,
        Err(err) => {
            warn!(error = ?err, "Ignoring unreadable datapath state");
            return;
        }
    };
    if state.is_empty() {
        return;
    }
    // Only allocations that survived the restart get their datapath back;
    // the rest is the manager's to re-dispatch or forget.
    let live: std::collections::HashSet<i64> = task_manager
        .lock()
        .expect("task manager")
        .allocations_snapshot()
        .into_iter()
        .collect();
    let desired: Vec<TaskAllocation> = state
        .desired
        .into_iter()
        .filter(|a| live.contains(&a.allocation_id))
        .collect();
    if desired.is_empty() {
        return;
    }
    let reply = HeartbeatReply {
        desired_allocations: desired.clone(),
        upstream_endpoints: state
            .upstream_endpoints
            .into_iter()
            .filter(|u| live.contains(&u.allocation_id))
            .collect(),
        net_policies: state
            .net_policies
            .into_iter()
            .filter(|p| live.contains(&p.allocation_id))
            .collect(),
        ..Default::default()
    };
    if let Some(cache) = forwarder.context().cache() {
        cache.set_identities(crate::worker::netd::policy::identities_from_desired(
            &desired,
        ));
        let _ = cache.apply_assignments(&reply.net_policies);
        for (hash, body) in state.policy_bodies {
            let _ = cache.insert_body(&hash, body);
        }
    }
    let alloc_root = task_manager
        .lock()
        .expect("task manager")
        .alloc_root()
        .to_path_buf();
    apply_upstreams(forwarder, &reply, &alloc_root).await;
    *LAST_DESIRED.lock().unwrap_or_else(|p| p.into_inner()) = desired.clone();
    info!(
        allocations = desired.len(),
        listeners = forwarder.active().len(),
        "Restored the datapath from disk ahead of the first heartbeat"
    );
}

/// Templates rendered on this node (docs/service-discovery.md, "Templates").
static TEMPLATES: LazyLock<crate::worker::templates::TemplateEngine> =
    LazyLock::new(crate::worker::templates::TemplateEngine::new);

/// Render every desired allocation's `connect.templates` from this reply's
/// endpoint feed: the first time before the task launches, then on every
/// heartbeat that changes a file, applying `changeMode` after `coalesce`
/// and `splay`.
async fn apply_templates(task_manager: &Arc<Mutex<WorkerTaskManager>>, reply: &HeartbeatReply) {
    use crate::worker::templates::{ChangeAction, TemplateContext};
    let live: std::collections::HashSet<i64> = reply
        .desired_allocations
        .iter()
        .map(|a| a.allocation_id)
        .collect();
    TEMPLATES.retain(&live);
    for allocation in &reply.desired_allocations {
        if allocation.task_connect.trim().is_empty() {
            continue;
        }
        let Ok(connect) =
            serde_json::from_str::<crate::structs::ConnectConfig>(&allocation.task_connect)
        else {
            continue;
        };
        if connect.templates.is_empty() {
            continue;
        }
        let network: Option<AllocNetwork> = if allocation.network.trim().is_empty() {
            None
        } else {
            serde_json::from_str(&allocation.network).ok()
        };
        let endpoints: std::collections::BTreeMap<String, Vec<Endpoint>> = reply
            .upstream_endpoints
            .iter()
            .filter(|u| u.allocation_id == allocation.allocation_id && !u.for_handler)
            .map(|u| (u.service.clone(), dialable_endpoints(u)))
            .collect();
        let (running, base) = {
            let mut manager = task_manager.lock().expect("task manager");
            let running = manager.has_allocation(allocation.allocation_id);
            let base = manager.file_delivery_base(
                allocation.allocation_id,
                &allocation.task_name,
                &allocation.task_runtime,
            );
            let task_dir = manager.task_dir_for(allocation.allocation_id, &allocation.task_name);
            (running, (base, task_dir))
        };
        let (delivery, task_dir) = base;
        let (in_task_root, host_task_dir) = match delivery {
            Some((root, dir)) => (Some(root), dir),
            None => (None, task_dir),
        };
        let mut env: std::collections::BTreeMap<String, String> = network
            .as_ref()
            .map(|n| n.env_pairs().into_iter().collect())
            .unwrap_or_default();
        env.insert(
            "LEGION_ALLOC_ID".into(),
            allocation.allocation_id.to_string(),
        );
        env.insert(
            "LEGION_TASK_DIR".into(),
            host_task_dir.display().to_string(),
        );
        let context = TemplateContext {
            network: network.as_ref(),
            endpoints: &endpoints,
            env: &env,
        };
        let actions = TEMPLATES.render_allocation(
            allocation.allocation_id,
            &connect.templates,
            &context,
            in_task_root,
            &host_task_dir,
            !running,
        );
        for action in actions {
            let task_manager = task_manager.clone();
            let allocation_id = allocation.allocation_id;
            tokio::spawn(async move {
                let (splay, act): (std::time::Duration, ChangeAction) = match &action {
                    ChangeAction::Signal { splay, .. } | ChangeAction::Restart { splay } => {
                        (*splay, action.clone())
                    }
                };
                if !splay.is_zero() {
                    let jitter = rand::random::<u64>() % (splay.as_millis().max(1) as u64);
                    tokio::time::sleep(std::time::Duration::from_millis(jitter)).await;
                }
                let mut manager = task_manager.lock().expect("task manager");
                let result = match act {
                    ChangeAction::Signal { signal, .. } => {
                        manager.signal_allocation(allocation_id, &signal)
                    }
                    ChangeAction::Restart { .. } => manager.restart_allocation(allocation_id),
                };
                if let Err(err) = result {
                    warn!(allocation_id, error = ?err, "Template change action failed");
                } else {
                    info!(allocation_id, "Applied template change");
                }
            });
        }
    }
}

/// Keep the node's mesh certificate current (docs/network-handlers.md,
/// "mTLS"): issued on the first heartbeat after anything on the node needs
/// it, re-issued after a third of its lifetime. Fail-static: an issuance
/// that fails keeps the certificate the node has, and a node without one
/// refuses mesh flows rather than sending them in the clear.
async fn apply_mesh_identity(
    forwarder: &UpstreamForwarder,
    client: &mut ManagerClient<Channel>,
    credentials: &Arc<WorkerCredentials>,
    node_id: &str,
) {
    let context = forwarder.context();
    if !context.mesh_needs_refresh() {
        return;
    }
    let request = credentials.attach(tonic::Request::new(
        crate::structs::IssueNodeCertificateRequest {
            node_id: node_id.to_string(),
        },
    ));
    match client.issue_node_certificate(request).await {
        Ok(response) => {
            let reply = response.into_inner();
            match crate::worker::netd::tls::MeshIdentity::from_pem(
                node_id,
                &reply.cert_pem,
                &reply.key_pem,
                &reply.ca_pem,
                reply.expires_at,
            )
            .and_then(|identity| context.set_mesh_identity(identity))
            {
                Ok(()) => info!(
                    node_id,
                    expires_at = reply.expires_at,
                    "Mesh node certificate issued"
                ),
                Err(err) => warn!(error = ?err, "Rejected mesh node certificate"),
            }
        }
        Err(status) if status.code() == tonic::Code::Unimplemented => {
            debug!("Manager predates mesh certificates; mesh TLS stays unavailable")
        }
        Err(status) => warn!(
            code = ?status.code(),
            message = status.message(),
            "Failed to issue the mesh node certificate; keeping the current one"
        ),
    }
}

/// Apply the reply's network-policy assignments to the datapath's cache and
/// fetch any body the node has not seen. Fail-static: a fetch that fails
/// leaves the allocation on whatever policy it had (or none, in which case a
/// gated allocation stays closed until a later heartbeat succeeds).
async fn apply_net_policies(
    forwarder: &UpstreamForwarder,
    client: &mut ManagerClient<Channel>,
    credentials: &Arc<WorkerCredentials>,
    node_id: &str,
    reply: &HeartbeatReply,
) {
    forwarder.context().set_node_id(node_id);
    let Some(cache) = forwarder.context().cache() else {
        return;
    };
    cache.set_identities(crate::worker::netd::policy::identities_from_desired(
        &reply.desired_allocations,
    ));
    let missing = cache.apply_assignments(&reply.net_policies);
    for hash in missing {
        let request =
            credentials.attach(tonic::Request::new(crate::structs::FetchNetPolicyRequest {
                node_id: node_id.to_string(),
                hash: hash.clone(),
            }));
        match client.fetch_net_policy(request).await {
            Ok(response) => {
                let body = response.into_inner().effective_json;
                match serde_json::from_str::<crate::structs::netpolicy::EffectivePolicy>(&body) {
                    Ok(policy) => {
                        if let Err(err) = cache.insert_body(&hash, policy) {
                            warn!(hash = %hash, error = ?err, "Rejected network policy body");
                        } else {
                            debug!(hash = %hash, "Fetched network policy body");
                        }
                    }
                    Err(err) => {
                        warn!(hash = %hash, error = ?err, "Invalid network policy body from manager");
                    }
                }
            }
            Err(err) => {
                warn!(
                    hash = %hash,
                    error = ?err,
                    "Failed to fetch network policy body; keeping last known good"
                );
            }
        }
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
        // The declaration side carries the `http` block and handlers, keyed
        // the same way the assigned loopback ports are.
        let declared: crate::structs::ConnectConfig =
            serde_json::from_str(&allocation.task_connect).unwrap_or_default();
        let netns = network
            .is_gated()
            .then(|| crate::worker::netd::netns::netns_path(allocation.allocation_id));
        for upstream in &network.upstreams {
            let declaration = declared
                .upstreams
                .iter()
                .find(|candidate| candidate.key() == upstream.service);
            specs.push(UpstreamSpec {
                allocation_id: allocation.allocation_id,
                service: upstream.service.clone(),
                local_port: upstream.local_port,
                balance: upstream.balance.clone(),
                to: upstream.to.clone(),
                http: declaration.and_then(|d| d.http.clone()),
                handlers: declaration.map(|d| d.handlers.clone()).unwrap_or_default(),
                vip: upstream.vip.clone(),
                canonical_port: upstream.canonical_port,
                netns: netns.clone(),
            });
        }
    }
    specs
}

/// The per-allocation resolvers a desired set implies: one per allocation
/// that was assigned a VIP block, answering `<key>.svc.legion` with that
/// allocation's upstream VIPs.
fn dns_specs(
    desired: &[TaskAllocation],
    alloc_root: &std::path::Path,
) -> Vec<crate::worker::netd::dns::DnsSpec> {
    let mut specs = Vec::new();
    for allocation in desired {
        if allocation.network.trim().is_empty() {
            continue;
        }
        let Ok(network) = serde_json::from_str::<AllocNetwork>(&allocation.network) else {
            continue;
        };
        let Some(resolver) = network
            .resolver_address()
            .and_then(|addr| addr.parse::<std::net::Ipv4Addr>().ok())
        else {
            continue;
        };
        let names = network
            .vip_names()
            .into_iter()
            .filter_map(|(key, (vip, port))| {
                Some((key, (vip.parse::<std::net::Ipv4Addr>().ok()?, port)))
            })
            .collect();
        let sandbox = is_gated_sandbox(allocation);
        specs.push(crate::worker::netd::dns::DnsSpec {
            allocation_id: allocation.allocation_id,
            resolver,
            names,
            netns: (network.is_gated() && !sandbox)
                .then(|| crate::worker::netd::netns::netns_path(allocation.allocation_id)),
            unix: sandbox.then(|| {
                crate::worker::netd::guest_agent::host_socket_path(
                    &sandbox_vsock_base(alloc_root, allocation.allocation_id),
                    crate::worker::netd::guest_agent::DNS_PORT,
                )
            }),
        });
    }
    specs
}

/// The desired allocations from the last successful heartbeat, so the next
/// request can report the datapath's status for them.
static LAST_DESIRED: std::sync::LazyLock<Mutex<Vec<TaskAllocation>>> =
    std::sync::LazyLock::new(|| Mutex::new(Vec::new()));

/// Allocations this worker created a gated namespace for, so departures can
/// be torn down. Process-wide: there is one worker per process, and a
/// namespace outliving a worker restart is found again by path.
static GATED_ALLOCATIONS: std::sync::LazyLock<Mutex<std::collections::HashSet<i64>>> =
    std::sync::LazyLock::new(|| Mutex::new(std::collections::HashSet::new()));

/// The egress proxies a desired set implies: one inside every gated
/// allocation's namespace.
fn proxy_specs(
    desired: &[TaskAllocation],
    alloc_root: &std::path::Path,
) -> Vec<crate::worker::netd::proxy::ProxySpec> {
    let mut specs: Vec<crate::worker::netd::proxy::ProxySpec> = gated_allocation_ids(desired)
        .into_iter()
        .map(|allocation_id| crate::worker::netd::proxy::ProxySpec {
            allocation_id,
            netns: Some(crate::worker::netd::netns::netns_path(allocation_id)),
            listen: None,
            unix: None,
        })
        .collect();
    // Gated sandboxes: the proxy is the host end of the guest's vsock port.
    for allocation in desired.iter().filter(|a| is_gated_sandbox(a)) {
        specs.push(crate::worker::netd::proxy::ProxySpec {
            allocation_id: allocation.allocation_id,
            netns: None,
            listen: None,
            unix: Some(crate::worker::netd::guest_agent::host_socket_path(
                &sandbox_vsock_base(alloc_root, allocation.allocation_id),
                crate::worker::netd::guest_agent::PROXY_PORT,
            )),
        });
    }
    specs
}

/// The ingress listeners a desired set implies: one per advertised port
/// that was given a private twin at placement (the task has an ingress
/// chain), bound in the host namespace and dialing the private port inside
/// the task's namespace when it has one.
fn ingress_specs(desired: &[TaskAllocation]) -> Vec<crate::worker::netd::ingress::IngressSpec> {
    let mut specs = Vec::new();
    for allocation in desired {
        if allocation.network.trim().is_empty() {
            continue;
        }
        let Ok(network) = serde_json::from_str::<AllocNetwork>(&allocation.network) else {
            continue;
        };
        let netns = network
            .is_gated()
            .then(|| crate::worker::netd::netns::netns_path(allocation.allocation_id));
        for port in &network.ports {
            let Some(private_port) = port.private_port else {
                continue;
            };
            specs.push(crate::worker::netd::ingress::IngressSpec {
                allocation_id: allocation.allocation_id,
                label: port.name.clone(),
                host_port: port.host_port,
                private_port,
                netns: netns.clone(),
                bind: None,
            });
        }
    }
    specs
}

/// What the datapath reports about each desired allocation on the next
/// heartbeat (`NetAllocStatus`): the node's own answer to "is this
/// enforced?", read back by `legion net doctor` and the UI.
fn net_status(
    forwarder: &UpstreamForwarder,
    desired: &[TaskAllocation],
) -> Vec<crate::structs::NetAllocStatus> {
    let bound_upstreams: HashMap<i64, Vec<String>> =
        forwarder
            .active()
            .into_iter()
            .fold(HashMap::new(), |mut acc, spec| {
                acc.entry(spec.allocation_id)
                    .or_default()
                    .push(spec.service);
                acc
            });
    let resolvers: std::collections::HashSet<i64> = forwarder
        .dns()
        .map(|dns| dns.active().into_iter().collect())
        .unwrap_or_default();
    let ingress_bound: HashMap<i64, Vec<String>> = forwarder
        .ingress()
        .map(|ingress| {
            ingress.active().into_iter().fold(
                HashMap::<i64, Vec<String>>::new(),
                |mut acc, (id, label)| {
                    acc.entry(id).or_default().push(label);
                    acc
                },
            )
        })
        .unwrap_or_default();
    let cache = forwarder.context().cache();
    let mut out = Vec::new();
    for allocation in desired {
        let network: Option<AllocNetwork> = if allocation.network.trim().is_empty() {
            None
        } else {
            serde_json::from_str(&allocation.network).ok()
        };
        let Some(network) = network else {
            continue;
        };
        let id = allocation.allocation_id;
        let gated = network.is_gated();
        let expected_upstreams = network.upstreams.len();
        let expected_ingress = network
            .ports
            .iter()
            .filter(|p| p.private_port.is_some())
            .count();
        if !gated && expected_upstreams == 0 && expected_ingress == 0 && network.vip_block.is_none()
        {
            continue;
        }
        let upstreams = bound_upstreams.get(&id).cloned().unwrap_or_default();
        let ingress = ingress_bound.get(&id).cloned().unwrap_or_default();
        let resolver_ready = resolvers.contains(&id);
        let proxy_ready = forwarder
            .proxies()
            .is_some_and(|proxies| proxies.is_running(id));
        // A gated sandbox has no namespace: its capture is the vsock host
        // sockets, bound like any listener.
        let vsock_sandbox = gated && allocation.task_runtime == "sandbox";
        let mut problems: Vec<String> = forwarder
            .ingress()
            .map(|ingress| ingress.problems(id))
            .unwrap_or_default();
        let namespace_ready =
            !gated || vsock_sandbox || crate::worker::netd::netns::netns_exists(id);
        if gated && !namespace_ready {
            problems.push("gated network namespace does not exist yet".into());
        }
        if gated && !proxy_ready {
            problems.push("egress proxy is not bound inside the namespace".into());
        }
        if upstreams.len() < expected_upstreams {
            problems.push(format!(
                "{} of {} upstream listeners bound",
                upstreams.len(),
                expected_upstreams
            ));
        }
        if network.vip_block.is_some() && !resolver_ready {
            problems.push(
                "allocation resolver is not bound (port 53 needs CAP_NET_BIND_SERVICE)".into(),
            );
        }
        let capture_ready = if gated {
            namespace_ready && proxy_ready && upstreams.len() >= expected_upstreams
        } else {
            true
        };
        let policy_hash = cache
            .filter(|cache| cache.is_ready(id))
            .and_then(|cache| cache.hash_for(id))
            .unwrap_or_default();
        let (tls_ready, tls_error) = forwarder.context().ingress_tls_status(id);
        if !tls_ready {
            problems.push(format!("ingress TLS: {tls_error}"));
        }
        problems.extend(TEMPLATES.problems(id));
        if network.ingress_tls.as_deref() == Some("legion")
            && forwarder.context().mesh_identity().is_none()
        {
            problems.push("mesh certificate not issued yet".into());
        }
        out.push(crate::structs::NetAllocStatus {
            allocation_id: id,
            mode: network
                .mode
                .clone()
                .unwrap_or_else(|| crate::structs::netpolicy::MODE_HOST.to_string()),
            capture_ready,
            policy_hash,
            upstreams_bound: upstreams,
            ingress_bound: ingress,
            resolver_ready,
            proxy_ready,
            gate_shut: gated && crate::worker::netd::netns::gate_is_shut(id),
            problems,
            handlers: forwarder
                .context()
                .handler_statuses(id)
                .into_iter()
                .map(|h| crate::structs::NetHandlerStatus {
                    source: h.source,
                    handler: h.handler,
                    target: h.target,
                    healthy: h.healthy,
                    last_error: h.last_error.unwrap_or_default(),
                    calls: h.calls,
                    errors: h.errors,
                })
                .collect(),
            tls_ready,
            tls_error,
            mesh_identity: forwarder.context().mesh_identity().is_some(),
        });
    }
    out
}

/// Allocations whose effective `network.mode` is `gated`.
fn is_gated(allocation: &TaskAllocation) -> bool {
    !allocation.network.trim().is_empty()
        && serde_json::from_str::<AllocNetwork>(&allocation.network)
            .map(|network| network.is_gated())
            .unwrap_or(false)
}

/// A gated sandbox is captured over vsock, not a namespace.
fn is_gated_sandbox(allocation: &TaskAllocation) -> bool {
    is_gated(allocation) && allocation.task_runtime == "sandbox"
}

/// Gated allocations that get a network namespace (container, exec).
fn gated_allocation_ids(desired: &[TaskAllocation]) -> Vec<i64> {
    desired
        .iter()
        .filter(|allocation| is_gated(allocation) && !is_gated_sandbox(allocation))
        .map(|allocation| allocation.allocation_id)
        .collect()
}

/// The host-side vsock socket base of a gated sandbox
/// (`<allocs>/<id>/.sandbox/vsock.sock`), from the worker's allocation root.
fn sandbox_vsock_base(alloc_root: &std::path::Path, allocation_id: i64) -> PathBuf {
    crate::task_plugins::vsock_socket_base_for(&alloc_root.join(allocation_id.to_string()))
}

/// Gated namespaces: created for every gated allocation in the desired set
/// *before* anything is started (a container joins the namespace at start;
/// an exec task enters it before exec), and removed for allocations that
/// have left it. Runs on the heartbeat tick ahead of reconcile so the
/// ordering is structural, not a race. Failures are logged and retried on
/// the next tick; a gated task whose namespace is missing refuses to start
/// with a clear error rather than starting unconfined.
async fn apply_net_capture(
    desired: &[TaskAllocation],
    previously_gated: &mut std::collections::HashSet<i64>,
) {
    if !crate::worker::netd::netns::platform_supports_netns() {
        return;
    }
    let wanted: std::collections::HashSet<i64> =
        gated_allocation_ids(desired).into_iter().collect();
    for allocation_id in wanted.iter().copied() {
        if crate::worker::netd::netns::netns_exists(allocation_id) {
            continue;
        }
        let created =
            tokio::task::spawn_blocking(move || crate::worker::netd::netns::create(allocation_id))
                .await;
        match created {
            Ok(Ok(path)) => {
                info!(allocation_id, path = %path.display(), "Created gated network namespace")
            }
            Ok(Err(err)) => {
                warn!(allocation_id, error = ?err, "Failed to create gated network namespace; the task will not start until it exists")
            }
            Err(err) => warn!(allocation_id, error = ?err, "Namespace creation task panicked"),
        }
    }
    for allocation_id in previously_gated.iter().copied() {
        if wanted.contains(&allocation_id) {
            continue;
        }
        let destroyed =
            tokio::task::spawn_blocking(move || crate::worker::netd::netns::destroy(allocation_id))
                .await;
        match destroyed {
            Ok(Ok(())) => debug!(allocation_id, "Removed gated network namespace"),
            Ok(Err(err)) => {
                warn!(allocation_id, error = ?err, "Failed to remove gated network namespace")
            }
            Err(err) => warn!(allocation_id, error = ?err, "Namespace removal task panicked"),
        }
        let _ = crate::worker::netd::netns::set_gate(allocation_id, false);
    }
    *previously_gated = wanted;
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
            tags: endpoint.tags.clone(),
            mesh_tls: false,
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
    /// A restarted worker re-binds the datapath for the allocations it
    /// re-adopted, from disk, before any heartbeat.
    #[tokio::test]
    async fn restored_datapath_rebinds_listeners_for_surviving_allocations() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_path = temp_dir.path().join("worker_state.json");
        let task_manager = Arc::new(Mutex::new(WorkerTaskManager::with_state_store(
            WorkerStateStore::new(state_path.clone()),
        )));
        task_manager.lock().unwrap().load_builtin_plugins();
        // An allocation that "survived": running in the task manager.
        let allocation = crate::structs::Allocation {
            id: 31,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "web".into(),
            instance_id: None,
            network: None,
        };
        let task = crate::structs::TaskConfig {
            config: json!({"command": ["sleep", "30"]}),
            ..crate::structs::TaskConfig::new("server", "exec")
        };
        task_manager
            .lock()
            .unwrap()
            .run_allocation(&allocation, &task)?;

        let local_port = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await?
            .local_addr()?
            .port();
        let store = crate::worker::netd::state::NetdStateStore::beside(&state_path);
        store.save(&crate::worker::netd::state::NetdState {
            desired: vec![
                TaskAllocation {
                    allocation_id: 31,
                    job_id: "web".into(),
                    task_name: "server".into(),
                    task_runtime: "exec".into(),
                    task_connect: json!({ "upstreams": [{ "service": "db" }] }).to_string(),
                    network: json!({
                        "upstreams": [{ "service": "db", "localPort": local_port, "balance": "round_robin" }]
                    })
                    .to_string(),
                    ..Default::default()
                },
                // Not running here any more: its datapath is not restored.
                TaskAllocation {
                    allocation_id: 32,
                    job_id: "web".into(),
                    task_name: "gone".into(),
                    task_runtime: "exec".into(),
                    task_connect: json!({ "upstreams": [{ "service": "db" }] }).to_string(),
                    network: json!({
                        "upstreams": [{ "service": "db", "localPort": local_port + 1, "balance": "round_robin" }]
                    })
                    .to_string(),
                    ..Default::default()
                },
            ],
            upstream_endpoints: vec![UpstreamEndpoints {
                allocation_id: 31,
                service: "db".into(),
                endpoints: vec![crate::structs::ServiceEndpoint {
                    address: "10.0.0.5".into(),
                    port: 5432,
                    allocation_id: 90,
                    node_id: "node-b".into(),
                    ..Default::default()
                }],
                for_handler: false,
            }],
            ..Default::default()
        })?;

        let forwarder = UpstreamForwarder::with_context(Arc::new(
            crate::worker::forwarder::ForwarderContext::new(
                Arc::new(crate::worker::netd::NetPolicyCache::new()),
                Arc::new(crate::worker::netd::Denials::new()),
            ),
        ));
        restore_datapath_from(&forwarder, &task_manager, &store).await;
        let active: Vec<(i64, String)> = forwarder
            .active()
            .into_iter()
            .map(|spec| (spec.allocation_id, spec.service))
            .collect();
        assert_eq!(active, vec![(31, "db".to_string())]);
        assert_eq!(
            LAST_DESIRED
                .lock()
                .unwrap()
                .iter()
                .map(|a| a.allocation_id)
                .collect::<Vec<_>>(),
            vec![31]
        );
        // The listener is really bound, with the persisted endpoint feed.
        assert!(tokio::net::TcpStream::connect(("127.0.0.1", local_port))
            .await
            .is_ok());
        task_manager.lock().unwrap().reconcile_allocations(&[]);
        Ok(())
    }

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
                to: None,
                http: None,
                handlers: Vec::new(),
                vip: None,
                canonical_port: None,
                netns: None,
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
            for_handler: false,
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
                tags: Vec::new(),
                mesh_tls: false,
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
            net_policies: Vec::new(),
            claim_verdicts: Vec::new(),
            next_interval_seconds: None,
            message: "ack".into(),
            desired_allocations: vec![connect_allocation(11, "db", local_port)],
            volume_destroy: Vec::new(),
            stop_notices: Vec::new(),
            upstream_endpoints: vec![UpstreamEndpoints {
                allocation_id: 11,
                for_handler: false,
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

        apply_upstreams(&forwarder, &reply, std::path::Path::new("/tmp")).await;
        assert_eq!(
            forwarder.active(),
            vec![UpstreamSpec {
                allocation_id: 11,
                service: "db".into(),
                local_port,
                balance: "least_conn".into(),
                to: None,
                http: None,
                handlers: Vec::new(),
                vip: None,
                canonical_port: None,
                netns: None,
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
            std::path::Path::new("/tmp"),
        )
        .await;
        assert!(forwarder.active().is_empty());
        Ok(())
    }
}
