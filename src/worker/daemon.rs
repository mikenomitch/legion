use crate::prelude::*;

use crate::structs::token::JoinToken;
use crate::structs::{
    manager_client::ManagerClient, worker_server::WorkerServer, AllocNetwork, AllocationHealth,
    HealthCheckRequest, HeartbeatReply, HeartbeatRequest, TaskAllocation, WorkerConfig,
    WorkerPreRegisterReply, WorkerPreRegisterRequest, WorkerRegisterRequest,
};
use crate::util::random_string;
use crate::worker::{
    artifact_credentials::ManagerCredentialSource,
    balance::Endpoint,
    forwarder::UpstreamForwarder,
    identity::{ManagerTokenIssuer, PreparedIdentity, WorkerIdentityManager},
    netd::{DesiredNetwork, Netd, NetdInput, NetdReport},
    node_check::{fingerprint_node, run_health_checks},
    server::WorkerDaemonService,
    state::WorkerStateStore,
    task_health::spawn_health_monitor,
    task_manager::{launch_task_allocation, WorkerTaskManager},
    task_supervisor::start_task_supervisor,
};

use futures::join;
use std::fs;
use std::path::Path;
use std::sync::LazyLock;
use std::sync::RwLock;
use std::time::Duration;
use tonic::{transport::Channel, transport::Server, Response};

/// The worker's manager-issued credentials: the join secret it was started
/// with and the node token returned by registration (attached to every
/// worker-plane RPC: heartbeats, health checks, identity-token issuance,
/// artifact-credential fetches). Empty when the cluster runs without ACLs.
///
/// The node token is persisted (mode 0600) in the worker's state directory
/// and is what a restarted worker registers with first: a join token is for
/// introducing a node, and operators are told to give it a short TTL and
/// revoke it once the fleet has joined (docs/auth-and-acls.md). Re-registering
/// on the join secret at every start made each restart after that TTL fatal
/// (`Unauthenticated: unknown or revoked join token`, spot-check P0-8). The
/// join secret is the fallback when the manager refuses the node token.
#[derive(Debug, Default)]
pub struct WorkerCredentials {
    join_secret: Option<String>,
    node_token: RwLock<Option<String>>,
    /// Where the node token is persisted, once the worker knows its state
    /// directory.
    persist: RwLock<Option<WorkerStateStore>>,
}

impl WorkerCredentials {
    pub fn with_join_secret(join_secret: Option<String>) -> Self {
        WorkerCredentials {
            join_secret,
            node_token: RwLock::new(None),
            persist: RwLock::new(None),
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

    /// True once registration returned a node token.
    pub(crate) fn has_node_token(&self) -> bool {
        self.node_token
            .read()
            .map(|token| token.is_some())
            .unwrap_or(false)
    }

    /// Adopt the node token a registration reply carried. `None` keeps the
    /// current token: a manager answers a node-token re-registration without
    /// minting a new one ("keep the one you presented"), and a cluster
    /// without ACLs mints none at all. A new token is persisted at once.
    pub(crate) fn store_node_token(&self, token: Option<String>) {
        let Some(token) = token else {
            return;
        };
        if let Some(store) = self.persist.read().ok().and_then(|s| s.clone()) {
            if let Err(err) = store.save_node_token(&token) {
                // The secret itself is never logged; the path is.
                warn!(
                    path = %store.node_token_path().display(),
                    error = ?err,
                    "Failed to persist the node token; a restart will fall back to the join token"
                );
            }
        }
        if let Ok(mut current) = self.node_token.write() {
            *current = Some(token);
        }
    }

    /// Drop the node token, in memory and on disk: the manager refused it.
    pub(crate) fn clear_node_token(&self) {
        if let Ok(mut current) = self.node_token.write() {
            *current = None;
        }
        if let Some(store) = self.persist.read().ok().and_then(|s| s.clone()) {
            if let Err(err) = store.clear_node_token() {
                debug!(error = ?err, "Failed to remove the persisted node token");
            }
        }
    }

    /// Load the node token a previous worker process persisted in `store`.
    /// Returns whether one was found.
    pub(crate) fn restore_from(&self, store: &WorkerStateStore) -> bool {
        let Some(token) = store.load_node_token() else {
            return false;
        };
        if let Ok(mut current) = self.node_token.write() {
            *current = Some(token);
        }
        true
    }

    /// Persist the node token into this worker's state directory from now
    /// on, writing the current one (if any) immediately.
    pub(crate) fn persist_to(&self, store: WorkerStateStore) {
        if let Some(token) = self.node_token.read().ok().and_then(|t| t.clone()) {
            if let Err(err) = store.save_node_token(&token) {
                warn!(error = ?err, "Failed to persist the node token");
            }
        }
        if let Ok(mut persist) = self.persist.write() {
            *persist = Some(store);
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

/// Address clients outside the cluster's private network reach this node at,
/// from `LEGION_WORKER_PUBLIC_ADDRESS` (a host name or IP, no scheme or
/// port). Registered as the `publicAddress` config attribute; the manager
/// puts it on service endpoints so links work from a browser. It never
/// changes where the manager or other nodes dial (that stays the private
/// `workerAddress`). See docs/service-discovery.md, "Public addresses".
fn worker_public_address() -> Option<String> {
    let value = std::env::var("LEGION_WORKER_PUBLIC_ADDRESS").ok()?;
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value.contains("://") || value.contains('/') {
        warn!(
            value,
            "LEGION_WORKER_PUBLIC_ADDRESS must be a bare host name or IP; ignoring it"
        );
        return None;
    }
    Some(value.to_string())
}

/// Everything needed to (re-)register this worker with the manager.
#[derive(Debug, Clone)]
struct RegistrationPayload {
    name: String,
    pool: String,
    config_attrs: String,
    fingerprint_attrs: String,
    health_checks: String,
    /// Worker capabilities advertised at registration
    /// (`WorkerRegisterRequest.capabilities`).
    capabilities: Vec<String>,
}

/// What this worker advertises at registration: it always takes
/// checkpoints and fetches published snapshot parts it does not hold, and it
/// publishes snapshot parts when a `contentStore` is configured (and usable).
fn worker_capabilities(publishing: bool, edge_sessions: bool) -> Vec<String> {
    let mut capabilities = vec![
        crate::structs::snapshot::CHECKPOINT_CAPABILITY.to_string(),
        crate::structs::snapshot::CHECKPOINT_FETCH_CAPABILITY.to_string(),
        // Receives its work over `Manager.StreamWork` (sandbox-scaling plan
        // §6.13). Informational: the manager recognizes a streaming worker
        // by the stream position its heartbeat reports.
        WORK_STREAM_CAPABILITY.to_string(),
        // Runs session envelopes and admits sessions into them
        // (docs/session-mode.md). Envelope tasks require it, so a worker
        // that predates sessions is never given one.
        crate::structs::session::SESSIONS_CAPABILITY.to_string(),
    ];
    if publishing {
        capabilities.push(crate::structs::snapshot::CONTENT_PUBLISH_CAPABILITY.to_string());
    }
    // Serves `Worker.StartSession` on a session ticket
    // (`edgeBoot.acceptClaims`). `ListSessionNodes` reports a node without
    // it as not accepting edge starts.
    if edge_sessions {
        capabilities.push(crate::structs::session::EDGE_SESSIONS_CAPABILITY.to_string());
    }
    capabilities
}

/// How a worker reaches its cluster and what it advertises.
#[derive(Debug, Clone, Default)]
pub struct WorkerJoin {
    pub manager_addr: String,
    pub worker_type: String,
    pub join_secret: Option<String>,
    /// The cluster CA pin from the join token. `None` only for the
    /// in-process worker, which shares the manager's process.
    pub ca_pin: Option<String>,
    /// `--advertise-addr`: the host (or `host:port`) managers dial.
    pub advertise_addr: Option<String>,
    /// Set for the in-process worker (`legion start manager --run-worker`).
    /// It shares its process with the manager, so on a shutdown signal it
    /// stops and returns rather than exiting the process under the
    /// manager's own drain.
    pub in_process: bool,
}

#[instrument(err, skip_all)]
pub async fn run_worker(
    token: &str,
    name_override: Option<&str>,
    advertise_addr: Option<&str>,
) -> R<()> {
    let join_token = JoinToken::decode(token)?;
    info!(worker_type = %join_token.worker_type, "Decoded join token");
    run_worker_with_manager(
        WorkerJoin {
            manager_addr: join_token.addr,
            worker_type: join_token.worker_type,
            join_secret: Some(join_token.secret),
            ca_pin: Some(join_token.ca_hash),
            advertise_addr: advertise_addr.map(str::to_string),
            in_process: false,
        },
        name_override,
    )
    .await
}

#[instrument(err, skip_all)]
pub async fn run_worker_local(join: WorkerJoin, name_override: Option<&str>) -> R<()> {
    run_worker_with_manager(join, name_override).await
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
async fn run_worker_with_manager(join: WorkerJoin, name_override: Option<&str>) -> R<()> {
    let WorkerJoin {
        manager_addr,
        worker_type,
        join_secret,
        ca_pin,
        advertise_addr,
        in_process,
    } = join;
    let server_port = worker_server_port();
    let bind = crate::worker::plane::bind_address(server_port)?;
    let advertised = crate::worker::plane::advertise_address(
        &manager_addr,
        server_port,
        advertise_addr.as_deref(),
    )?;

    info!(%manager_addr, %worker_type, "Worker connecting to manager");

    // Tasks outlive the worker, but the self-fence watchdog below starts only
    // once the manager answers. Until then, fence restored stateful tasks
    // against the contact time a previous process persisted, so a worker
    // restarted (or crash-looping) during a partition cannot keep them
    // running past their window.
    let boot_store = boot_state_store(name_override);
    let boot_fence = boot_store
        .clone()
        .map(|store| tokio::spawn(run_boot_fence(store)));

    // Every connection to a manager is TLS verified against the join
    // token's CA pin, from the first byte of the first one, before the join
    // secret is sent (worker-plane-auth plan §6.8). The pin (and later the
    // cluster bundle) covers every manager the worker learns: they share
    // the cluster CA.
    match ca_pin.as_deref().filter(|pin| !pin.is_empty()) {
        Some(pin) => {
            crate::worker::plane::trust_manager(&manager_addr, pin)?;
        }
        None => bail!(
            "no cluster CA pin for the manager at {manager_addr}; join with a token from \
             `legion join-token create`"
        ),
    }
    // Every manager this worker knows, not just the token's: a restarted
    // worker starts from the set it persisted, so it comes up even when the
    // manager its token names is down (spot-check P0-5).
    let managers = crate::worker::managers::ManagerEndpoints::new(
        &manager_addr,
        boot_store
            .as_ref()
            .map(WorkerStateStore::load_known_managers)
            .unwrap_or_default(),
    )?;
    let client: ManagerClient<Channel> = ManagerClient::new(managers.channel());
    let credentials = Arc::new(WorkerCredentials::with_join_secret(join_secret));
    // A restarted worker re-registers with the node token it persisted, so
    // it does not depend on its join token still being valid (P0-8).
    if let Some(store) = &boot_store {
        if credentials.restore_from(store) {
            info!("Found a persisted node token; re-registering with it");
        }
    }

    let pre_register_res =
        pre_register_until_reachable(&client, &managers, worker_type, &credentials).await?;
    // The manager answered, so restore (below) and then the in-process
    // watchdog take over. Stopped before the restore so the two never act on
    // the same process at once.
    if let Some(boot_fence) = boot_fence {
        boot_fence.abort();
    }
    let pre_register_reply = pre_register_res.into_inner();
    // The `iss` this node's tickets carry, from the manager rather than
    // assumed to be the join-token address (it is one name for the whole
    // cluster). Heartbeats keep it current.
    crate::worker::boot_ticket::cluster_issuer()
        .learn(&pre_register_reply.workload_identity_issuer);
    let config = pre_register_reply.config.context("No config found")?;

    let name = resolve_worker_name(name_override, config.name_prefix.clone())?;
    let state_path = WorkerStateStore::path_for_worker(&name);
    info!(
        worker_name = %name,
        state_path = %state_path.display(),
        "Worker state directory (keep it stable across restarts: it is how a \
         restarted worker re-adopts its running tasks)"
    );
    // Before anything writes into it: each entry's mode is part of the
    // isolation between allocations (see `prepare_layout`).
    WorkerStateStore::new(state_path.clone()).prepare_layout()?;
    credentials.persist_to(WorkerStateStore::new(state_path.clone()));
    managers.persist_to(WorkerStateStore::new(state_path.clone()));
    // The node's key is generated (or loaded) here and never leaves this
    // host; the manager signs a CSR for it after registration.
    let node_identity = crate::worker::plane::NodeIdentity::load(
        &crate::worker::plane::identity_dir(&state_path),
        &name,
        None,
        ca_pin.clone(),
    )?;
    // A persisted bundle was pin-checked when it was first accepted; from
    // here on it, not the token's pin, is what the manager must chain to.
    if let Some(roots) = node_identity.roots() {
        crate::worker::plane::trust_manager_bundle(&manager_addr, roots);
    }
    // The datapath's persisted inputs, replayed before the first heartbeat
    // (docs/network-handlers.md, "The datapath across worker restarts").
    let netd_state = crate::worker::netd::state::NetdStateStore::beside(&state_path);
    // Access logs sit next to each allocation's own logs.
    let logs_base_dir = WorkerStateStore::new(state_path.clone()).logs_base_dir();
    let content_dir = WorkerStateStore::new(state_path.clone()).content_dir();
    let rootfs_cache_dir = WorkerStateStore::new(state_path.clone()).rootfs_cache_dir();
    let task_manager = Arc::new(Mutex::new(WorkerTaskManager::with_state_store(
        WorkerStateStore::new(state_path),
    )));
    let (artifact_fetch_config, content_config, content_store_target) =
        parse_artifacts_payload(&config.artifacts).unwrap_or_else(|err| {
            warn!(error = ?err, "Invalid worker artifacts config; using defaults");
            (
                crate::structs::WorkerArtifactFetchConfigFull::default(),
                crate::structs::WorkerContentConfig::default()
                    .resolve()
                    .ok()
                    .flatten(),
                None,
            )
        });
    let sessions_config = parse_sessions_payload(&config.artifacts);
    let session_manifests = build_session_manifests(
        content_store_target.as_ref(),
        Arc::new(ManagerCredentialSource::new(
            managers.clone(),
            name.clone(),
            credentials.clone(),
        )),
        &name,
        &advertised,
    );
    let content_store = open_content_store(&content_dir, content_config);
    // Sandbox base root disks (docs/task-runtime-sandbox.md, "The root disk
    // cache"). Only a Linux node boots microVMs, so only one keeps them. It
    // honors the content store's free-disk floor.
    if cfg!(target_os = "linux") {
        let min_free = content_store
            .as_ref()
            .map(|store| store.config().min_free_disk)
            .or_else(|| {
                crate::structs::parse_byte_size(crate::content_store::DEF_CONTENT_MIN_FREE_DISK)
                    .ok()
            })
            .unwrap_or(0);
        tokio::task::spawn_blocking(move || {
            crate::task_plugins::rootfs_cache::init(&rootfs_cache_dir, min_free)
        })
        .await
        .ok();
    }
    let content_publishing = build_content_publishing(
        content_store.clone(),
        content_store_target.as_ref(),
        Arc::new(ManagerCredentialSource::new(
            managers.clone(),
            name.clone(),
            credentials.clone(),
        )),
    );
    // Volume drivers must be ready before any allocation can arrive, and they
    // need the node id (it goes into every plugin request) plus the operator's
    // hostVolumes declarations from the worker config.
    let host_volumes = parse_host_volumes(&config.host_volumes);
    // Task runtimes run in-process as dylibs and take no config of their own,
    // so cross-cutting worker settings reach them through the environment —
    // the same channel `sandbox` uses for LEGION_SANDBOX_KERNEL.
    let enforcement_config =
        usable_resource_enforcement(parse_resource_enforcement(&config.resource_enforcement));
    apply_resource_enforcement_env(&enforcement_config);
    // The kernel classifier (mesh interfaces plan §6.A) where the node can
    // run it: the host guard, host-mode egress chains, and DNS capture, in
    // eBPF. Elsewhere, the host guard in `nftables` (worker-plane-auth plan
    // §6.13). Both match sockets by cgroup leaf, so where either runs every
    // task gets one, enforcement or not.
    let (net_kernel, classifier) = open_kernel_classifier(&enforcement_config.cgroup_parent);
    let kernel_guard = if classifier.is_some() {
        crate::worker::netd::kernel_guard::Availability::Active
    } else {
        crate::worker::netd::kernel_guard::probe()
    };
    let host_guard_attribute = if classifier.is_some() {
        "active (ebpf)".to_string()
    } else {
        kernel_guard.attribute()
    };
    match &kernel_guard {
        crate::worker::netd::kernel_guard::Availability::Active => {
            std::env::set_var(crate::constants::ENV_HOST_GUARD_KERNEL, "1");
            std::env::set_var(
                crate::constants::ENV_CGROUP_PARENT,
                &enforcement_config.cgroup_parent,
            );
            info!(
                backend = if classifier.is_some() {
                    "ebpf"
                } else {
                    "nftables"
                },
                "Host guard active in the kernel: host-mode tasks cannot reach this node's \
                 Legion ports, cloud metadata, or other allocations' listeners"
            );
        }
        crate::worker::netd::kernel_guard::Availability::Unavailable(reason) => {
            std::env::remove_var(crate::constants::ENV_HOST_GUARD_KERNEL);
            warn!(
                %reason,
                "Host guard unavailable in the kernel: host-mode tasks can reach this node's \
                 Legion ports and cloud metadata (gated tasks are still guarded by the datapath)"
            );
        }
    }
    apply_container_env(&config.container);
    // One host guard for the datapath and artifact fetches alike.
    let host_guard = Arc::new(host_guard_for(&config.security));
    let sandbox_bridges = apply_sandbox_bridges_env(enforcement_config.sandbox.as_ref());
    crate::task_plugins::warn_if_no_static_guest_binary();
    let restored_allocations = {
        let mut manager = task_manager.lock().expect("task manager");
        // Worker core's own governor takes the config directly, before any
        // allocation is restored or started. It used to read the env above
        // from the task manager's constructor, which ran before the env was
        // set, so enforcement never turned on. The governor logs whether it
        // really enforces (and why not, when it cannot).
        manager.configure_resource_enforcement(&enforcement_config);
        manager.load_builtin_plugins();
        manager.configure_volumes(&name, host_volumes);
        // Before `configure_artifacts`, which hands the store to the stager.
        manager.configure_content_store(content_store.clone());
        manager.configure_content_publishing(content_publishing);
        manager.configure_session_manifests(session_manifests);
        manager.configure_artifacts(
            artifact_fetch_config,
            Box::new(ManagerCredentialSource::new(
                managers.clone(),
                name.clone(),
                credentials.clone(),
            )),
            content_store_target.clone(),
            Some(host_guard.clone()),
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
    // The claim window bounds the unratified edge claims this node holds
    // (`edgeBoot.maxProvisionalPerNode`) and sheds edge starts when
    // ratification falls behind (docs/edge-boot.md, "Overload"). Installed
    // before the worker server starts, so no start can see the default.
    task_manager
        .lock()
        .expect("task manager")
        .configure_claim_window(Arc::new(
            crate::worker::claim_window::ClaimWindow::from_config(&edge_boot_config),
        ));
    // Direct exec: this node admits exec tickets on `Worker.ExecTask` only
    // when its worker config says `exec = { direct = true }`.
    let exec_direct = worker_exec_config(&config.security).direct;
    // One JWKS cache serves every ticket this node verifies (boot and
    // exec), fetched over TLS pinned to the cluster CA.
    let jwks = if edge_boot_config.accept_claims || exec_direct {
        Some(Arc::new(build_jwks_verifier(
            &edge_boot_config,
            &manager_addr,
            &config.pool,
            managers.clone(),
        )?))
    } else {
        None
    };
    let edge_boot = match (&jwks, edge_boot_config.accept_claims) {
        (Some(jwks), true) => Some(Arc::new(crate::worker::server::EdgeBootRuntime {
            verifier: jwks.clone(),
            spec_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            node_id: name.clone(),
            ratifier: Arc::new(ManagerClaimRatifier {
                client: client.clone(),
                credentials: credentials.clone(),
                managers: managers.clone(),
                leader: tokio::sync::Mutex::new(None),
            }),
        })),
        _ => None,
    };
    if edge_boot.is_some() {
        info!(worker = %name, pool = %config.pool, "Edge boot enabled: this node accepts boot tickets");
    }
    let direct_exec = match (&jwks, exec_direct) {
        (Some(jwks), true) => {
            info!(worker = %name, "Direct exec enabled: this node accepts exec tickets");
            let verifier = Arc::new(crate::worker::direct_exec::ExecTicketVerifier::new(
                jwks.clone(),
                name.clone(),
                time::OffsetDateTime::now_utc().unix_timestamp(),
            ));
            // The heartbeat keeps its delegated keys current.
            crate::worker::direct_exec::register_verifier(verifier.clone());
            Some(verifier)
        }
        _ => None,
    };

    // One launch pool per node, shared by every path that starts a session
    // or claim (PostWork, StartClaim, StartSession, the work stream).
    let launch_pool = Arc::new(crate::worker::launch_pool::LaunchPool::from_config(
        &edge_boot_config,
    ));

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
    // Probed off the lock and off the async runtime: a readiness check can
    // shell out to a runtime CLI (see `readiness_of`).
    let plugins = task_manager.lock().expect("task manager").plugin_handles();
    let readiness =
        tokio::task::spawn_blocking(move || crate::task_plugins::readiness_of(&plugins)).await?;
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
    // Oversubscription rides registration into the scheduler's capacity
    // math (`ResourcePool::from_node`). Only advertised by a node that can
    // actually enforce: a ratio on a node without cgroups would pack it past
    // physical memory with nothing to give memory back.
    if enforcement_config.enabled
        && crate::worker::resources::enforcement_available()
        && (enforcement_config.memory_ratio() > 1.0 || enforcement_config.cpu_ratio() > 1.0)
    {
        config_attrs.insert(
            "oversubscribe".into(),
            json!({
                "memory": enforcement_config.memory_ratio(),
                "cpu": enforcement_config.cpu_ratio(),
            }),
        );
    } else if enforcement_config.memory_ratio() > 1.0 || enforcement_config.cpu_ratio() > 1.0 {
        warn!(
            "resourceEnforcement.oversubscribe is set but this node cannot enforce (no cgroup \
             v2, or not Linux); registering at physical capacity"
        );
    }
    // The bridges a TAP sandbox can join here: a task on `bridge = "br0"`
    // carries an implicit `sandbox.bridges contains "br0"` constraint, so it
    // never places on a node that could not address its guest.
    if !sandbox_bridges.is_empty() {
        config_attrs.insert(
            crate::task_plugins::sandbox_addr::BRIDGES_ATTR.into(),
            json!(sandbox_bridges),
        );
    }
    // What managers (and edge routers) dial: the route's source address
    // toward the manager unless overridden (docs/architecture-managers-
    // workers.md, "Ports"). Always `https://`: the worker plane is mTLS.
    config_attrs.insert("workerAddress".into(), Value::String(advertised.clone()));
    // `legion node info` shows whether host-mode tasks are guarded here.
    config_attrs.insert(
        crate::worker::netd::kernel_guard::NODE_ATTR.into(),
        Value::String(host_guard_attribute.clone()),
    );
    config_attrs.insert(
        crate::worker::netd::kernel::NODE_ATTR.into(),
        Value::String(net_kernel.attribute()),
    );
    if let Some(public_address) = worker_public_address() {
        config_attrs.insert("publicAddress".into(), Value::String(public_address));
    }
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
        capabilities: worker_capabilities(
            task_manager
                .lock()
                .expect("task manager")
                .content_publishing()
                .is_some(),
            edge_boot_config.accept_claims,
        ),
    };
    let reported_managers = register_self(client.clone(), &registration, &credentials).await?;
    managers.learn(&reported_managers);
    info!(worker_name = %name, "Worker registered with manager");
    // Eager issuance: the listener needs the certificate. A CA that does
    // not match the join token's pin stops the join here; any other failure
    // (a manager outage) keeps a persisted certificate, or retries on the
    // heartbeat loop while the listener waits.
    if let Err(err) = node_identity
        .issue(&mut client.clone(), &credentials, &manager_addr)
        .await
    {
        if err
            .downcast_ref::<crate::worker::plane::PinMismatch>()
            .is_some()
        {
            return Err(err);
        }
        warn!(error = %format!("{err:#}"), "Node certificate not issued yet; retrying on the heartbeat loop");
    }
    record_manager_contact(task_manager.lock().expect("task manager").state_store());
    // Easter egg, mirrored by the manager's log line for the same join.
    info!(worker_name = %name, "We are Legion. {name} has joined");
    crate::worker::balance::set_local_node(&name);

    start_worker_telemetry();
    spawn_pressure_responder(task_manager.clone(), &enforcement_config, &name);

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
        .with_logs_base_dir(logs_base_dir.clone())
        .with_host_guard(host_guard.clone())
        .with_kernel(
            (kernel_guard == crate::worker::netd::kernel_guard::Availability::Active).then(|| {
                kernel_guard_base(
                    &config.security,
                    u16::try_from(server_port).unwrap_or_default(),
                    &manager_addr,
                    &enforcement_config.cgroup_parent,
                )
            }),
            classifier,
        ),
    );
    // Where the kernel classifier redirects host-mode flows only netd can
    // decide. Held for the worker's lifetime.
    let _host_capture = if net_context.kernel_classifier_active() {
        match crate::worker::netd::capture::HostCapture::start(net_context.clone()).await {
            Ok(capture) => {
                net_context.set_host_capture(Some(capture.addr));
                Some(capture)
            }
            Err(err) => {
                warn!(error = %format!("{err:#}"), "Capture listener unavailable; host-mode flows only netd can decide are refused");
                None
            }
        }
    } else {
        None
    };
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
    let alloc_root = task_manager
        .lock()
        .expect("task manager")
        .alloc_root()
        .to_path_buf();
    let netd = Arc::new(Netd::new(forwarder, alloc_root).with_state_store(netd_state));
    netd.send(NetdInput::NodeId(name.clone())).await;
    // The mesh shares the node identity.
    if let Some(mesh) = node_identity.mesh_identity() {
        netd.send(NetdInput::MeshIdentity(mesh)).await;
    }
    // Restored tasks are still running: give them their network back now,
    // not on the first successful heartbeat. Namespaces persist on their own
    // (bind mounts under `/var/run/netns`); this re-binds what lives inside
    // and beside them.
    {
        let live: std::collections::HashSet<i64> = task_manager
            .lock()
            .expect("task manager")
            .allocations_snapshot()
            .into_iter()
            .collect();
        netd.restore(&live).await;
    }
    // Built after the datapath: a push binds a gated sandbox's egress proxy
    // before its launch (`Netd::bind_launch`).
    let worker_node = WorkerDaemonService {
        node_id: name.clone(),
        task_manager: task_manager.clone(),
        identity: Some(identity_manager.clone()),
        edge_boot: edge_boot.clone(),
        direct_exec: direct_exec.clone(),
        launch_pool: launch_pool.clone(),
        node_identity: Some(node_identity.clone()),
        netd: Some(netd.clone()),
    };
    let worker_router = Server::builder().add_service(WorkerServer::new(worker_node));
    let worker_server_fut =
        serve_worker_plane(bind, advertised, node_identity.clone(), worker_router);
    // Work arrives over `Manager.StreamWork`: one long-lived stream, its
    // deltas patching the mirror the heartbeat reconciles from. A new
    // allocation starts as soon as its delta lands, through the same path a
    // `PostWork` push takes; a stop, a payload change, or a resync wakes the
    // heartbeat loop so its reconcile runs now instead of at the next beat.
    {
        let credentials = credentials.clone();
        let launcher = Arc::new(WorkerDaemonService {
            node_id: name.clone(),
            task_manager: task_manager.clone(),
            identity: Some(identity_manager.clone()),
            edge_boot: edge_boot.clone(),
            direct_exec: direct_exec.clone(),
            launch_pool: launch_pool.clone(),
            node_identity: Some(node_identity.clone()),
            netd: Some(netd.clone()),
        });
        tokio::spawn(crate::worker::work_stream::run_work_stream(
            client.clone(),
            move |request| credentials.attach(request),
            name.clone(),
            WORK_MIRROR.clone(),
            move |applied| launch_streamed_work(&launcher, applied),
        ));
    }
    let heartbeat_fut = start_heartbeats(
        name.clone(),
        manager_addr.clone(),
        node_identity.clone(),
        client.clone(),
        task_manager.clone(),
        identity_manager.clone(),
        last_successful_heartbeat.clone(),
        credentials.clone(),
        registration,
        netd,
        heartbeat_interval,
        edge_boot.as_ref().map(|runtime| runtime.spec_cache.clone()),
        Some(managers.clone()),
    );
    let self_fence_fut =
        start_self_fence_watchdog(task_manager.clone(), last_successful_heartbeat.clone());
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
    // Idle sessions: expiry, eviction, periodic checkpoints, and the
    // restores a restart left (docs/session-mode.md#idle-sessions).
    let session_fut = start_session_maintenance(
        task_manager.clone(),
        launch_pool.clone(),
        last_successful_heartbeat,
        sessions_config
            .fence_list_every()
            .unwrap_or(crate::structs::DEFAULT_FENCE_LIST_EVERY),
    );
    let stop_checkpoint_timeout =
        humantime::parse_duration(&edge_boot_config.stop_checkpoint_timeout)
            .unwrap_or(Duration::from_secs(60));
    let shutdown_task_manager = task_manager.clone();
    tokio::select! {
        _ = async {
            join!(
                heartbeat_fut,
                self_fence_fut,
                checks_fut,
                log_maintenance_fut,
                supervisor_fut,
                worker_server_fut,
                session_fut
            )
        } => {}
        signal = worker_shutdown_signal() => {
            stop_worker_daemon(
                shutdown_task_manager,
                signal,
                stop_checkpoint_timeout,
                WorkerExit::for_worker(in_process),
            )
            .await;
        }
    }

    Ok(())
}

/// How often the session maintenance loop runs: the keepFor sweep, disk
/// pressure eviction, and the periodic checkpoint round.
const SESSION_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5);

/// Idle-session upkeep on this node. First resumes the sessions a restart
/// found dead with an `onWorkerStop` checkpoint, then every
/// [`SESSION_MAINTENANCE_INTERVAL`] sweeps expired and orphaned checkpoints
/// (and evicts the oldest under `content.minFreeDisk` pressure), and runs a
/// `suspend.checkpointEvery` round. For `store` sessions it also
/// (docs/session-mode.md, "No split brain"): self-fences when neither a
/// manager nor the content store answered for `stateful.fenceAfter`, and
/// unfences once the store answers again; retries publishes that did not
/// land; deletes the manifests of sessions that ended; and every
/// `fence_list_every` lists this node's fence markers.
async fn start_session_maintenance(
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    launch_pool: Arc<crate::worker::launch_pool::LaunchPool>,
    last_successful_heartbeat: Arc<Mutex<std::time::Instant>>,
    fence_list_every: Duration,
) -> R<()> {
    use crate::worker::task_manager::session_sleep;
    use crate::worker::task_manager::session_store;
    let restores = task_manager
        .lock()
        .expect("task manager")
        .take_pending_session_restores();
    for session_id in restores {
        let slot = match launch_pool.acquire().await {
            Ok(slot) => slot,
            Err(err) => {
                warn!(session_id = %session_id, error = %err, "No launch slot to restore a session; it stays suspended");
                continue;
            }
        };
        let manager = task_manager.clone();
        let id = session_id.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            session_sleep::resume_session(&manager, &id, false)
        })
        .await;
        drop(slot);
        match outcome {
            Ok(Ok(_)) => info!(session_id = %session_id, "Restored session after worker restart"),
            Ok(Err(err)) => {
                warn!(session_id = %session_id, error = %err, "Session restore after restart failed; it stays suspended")
            }
            Err(err) => {
                warn!(session_id = %session_id, error = %err, "Session restore task failed")
            }
        }
    }
    let mut per_capture = Duration::from_secs(5);
    let mut last_fence_list = std::time::Instant::now();
    loop {
        tokio::time::sleep(SESSION_MAINTENANCE_INTERVAL).await;
        let manifests = task_manager
            .lock()
            .expect("task manager")
            .session_manifests();
        if let Some(manifests) = manifests {
            let since_manager = last_successful_heartbeat.lock().map(|at| at.elapsed()).ok();
            let since_store = manifests.last_success().map(|at| at.elapsed());
            let manager = task_manager.clone();
            let list_fences = last_fence_list.elapsed() >= fence_list_every;
            if list_fences {
                last_fence_list = std::time::Instant::now();
            }
            let pool = launch_pool.clone();
            let handle = tokio::runtime::Handle::current();
            let upkeep = tokio::task::spawn_blocking(move || {
                session_store::self_fence(&manager, since_manager, since_store);
                if list_fences {
                    match session_store::process_fence_list(&manager) {
                        Ok(0) => {}
                        Ok(ended) => info!(ended, "Ended session copies a move fenced"),
                        Err(err) => {
                            debug!(error = %err, "Fence list failed; retried next interval")
                        }
                    }
                }
                let fenced = !manager
                    .lock()
                    .expect("task manager")
                    .fenced_sessions()
                    .is_empty();
                if fenced {
                    session_store::unfence(&manager, || handle.block_on(pool.acquire()).ok());
                }
                session_store::retry_unpublished(&manager);
                session_store::run_manifest_deletes(&manager);
            })
            .await;
            if let Err(err) = upkeep {
                warn!(error = %err, "Store session upkeep failed");
            }
        }
        let manager = task_manager.clone();
        let mut estimate = per_capture;
        let round = tokio::task::spawn_blocking(move || {
            manager
                .lock()
                .expect("task manager")
                .sweep_idle_sessions(std::time::Instant::now(), crate::util::now_unix_ms());
            let counts = session_sleep::run_checkpoint_round(&manager, &mut estimate);
            (counts, estimate)
        })
        .await;
        match round {
            Ok(((ran, skipped), estimate)) => {
                per_capture = estimate;
                if ran > 0 || skipped > 0 {
                    debug!(ran, skipped, "Session checkpoint round");
                }
            }
            Err(err) => warn!(error = %err, "Session maintenance round failed"),
        }
    }
}

/// Suspend the sessions the idle policy found quiet past `idle.after`, each
/// on the blocking pool. A session already capturing answers busy and is
/// tried again at the next usage report.
fn suspend_idle_sessions(task_manager: &Arc<Mutex<WorkerTaskManager>>, local_keys: Vec<i64>) {
    for local_key in local_keys {
        let Some(session_id) = task_manager
            .lock()
            .expect("task manager")
            .session_id_for_local_key(local_key)
        else {
            continue;
        };
        let manager = task_manager.clone();
        tokio::task::spawn_blocking(move || {
            match crate::worker::task_manager::session_sleep::suspend_session(
                &manager,
                &session_id,
                "idle",
            ) {
                Ok(_) => info!(session_id = %session_id, "Suspended idle session"),
                Err(err) => {
                    debug!(session_id = %session_id, error = %err, "Idle session not suspended")
                }
            }
        });
    }
}

/// Resolves with the signal's name on SIGTERM or SIGINT (Ctrl-C).
async fn worker_shutdown_signal() -> &'static str {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!(error = ?err, "Failed installing the SIGINT handler");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => tokio::select! {
                _ = term.recv() => "SIGTERM",
                _ = ctrl_c => "SIGINT",
            },
            Err(err) => {
                warn!(error = ?err, "Failed installing the SIGTERM handler");
                ctrl_c.await;
                "SIGINT"
            }
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await;
        "SIGINT"
    }
}

/// The worker's orderly stop: checkpoint the sessions whose task sets
/// `suspend.onWorkerStop = "checkpoint"` (least recently checkpointed
/// first, within `edgeBoot.stopCheckpointTimeout`), flush the session log,
/// and exit. Workloads keep running, as on any worker stop: a restarted
/// worker re-adopts them, and restores from these checkpoints only the
/// sessions whose process died meanwhile.
/// What a stopping worker does with its tasks and its process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkerExit {
    /// Stop every task before returning. Only the in-process worker on
    /// macOS does: there the cluster is one process in a developer's
    /// terminal, and its Apple containers (each a VM) would otherwise keep
    /// running after it stops. Everywhere else tasks outlive the worker and
    /// the next start re-adopts them.
    stop_tasks: bool,
    /// Exit the process once stopped. False for the in-process worker,
    /// whose manager finishes its own drain and exits.
    exit_process: bool,
}

impl WorkerExit {
    fn for_worker(in_process: bool) -> Self {
        Self {
            stop_tasks: in_process && cfg!(target_os = "macos"),
            exit_process: !in_process,
        }
    }
}

/// Budget for stopping every task at an in-process worker's shutdown. Each
/// task gets its own `stop.killAfter` inside it, then a hard teardown.
const SHUTDOWN_TASK_STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Stop every task the worker runs, all at once, each on a blocking thread
/// ([`crate::worker::task_manager::ShutdownStop::run`]).
async fn stop_tasks_for_shutdown(task_manager: &Arc<Mutex<WorkerTaskManager>>) {
    let stops = task_manager
        .lock()
        .expect("task manager")
        .take_tasks_for_shutdown();
    if stops.is_empty() {
        return;
    }
    info!(
        tasks = stops.len(),
        "Stopping this node's tasks before exiting"
    );
    let deadline = std::time::Instant::now() + SHUTDOWN_TASK_STOP_TIMEOUT;
    let handles: Vec<_> = stops
        .into_iter()
        .map(|stop| {
            let allocation_id = stop.allocation_id();
            (
                allocation_id,
                tokio::task::spawn_blocking(move || stop.run(deadline)),
            )
        })
        .collect();
    // The hard teardown after the deadline has its own CLI deadlines, so
    // allow it a little past the stop budget.
    let all = futures::future::join_all(handles.into_iter().map(|(id, handle)| async move {
        if let Err(err) = handle.await {
            warn!(allocation_id = id, error = %err, "Task stop at shutdown panicked");
        }
    }));
    if tokio::time::timeout(SHUTDOWN_TASK_STOP_TIMEOUT + Duration::from_secs(30), all)
        .await
        .is_err()
    {
        warn!("Some tasks did not stop in time; exiting anyway");
    }
}

async fn stop_worker_daemon(
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    signal: &'static str,
    timeout: Duration,
    exit: WorkerExit,
) {
    info!(signal, "Worker stopping");
    let deadline = std::time::Instant::now() + timeout;
    let manager = task_manager.clone();
    let checkpointed = tokio::task::spawn_blocking(move || {
        let counts = crate::worker::task_manager::session_sleep::checkpoint_sessions_on_stop(
            &manager, deadline,
        );
        manager
            .lock()
            .expect("task manager")
            .sync_session_log(Duration::from_secs(5));
        counts
    });
    match tokio::time::timeout(timeout + Duration::from_secs(10), checkpointed).await {
        Ok(Ok((done, 0))) if done > 0 => info!(done, "Checkpointed sessions before stopping"),
        Ok(Ok((done, left))) if left > 0 => {
            warn!(
                done,
                left, "Stop checkpoint timeout reached; some sessions were not checkpointed"
            )
        }
        Ok(_) => {}
        Err(_) => warn!("Stop checkpoints did not finish in time; exiting anyway"),
    }
    if exit.stop_tasks {
        stop_tasks_for_shutdown(&task_manager).await;
    }
    if exit.exit_process {
        std::process::exit(0);
    }
    info!("In-process worker stopped");
}

/// The worker type's `exec` block, from the security payload.
fn worker_exec_config(security: &str) -> crate::structs::WorkerExecConfig {
    if security.trim().is_empty() {
        return Default::default();
    }
    serde_json::from_str::<crate::structs::WorkerSecurityConfig>(security)
        .ok()
        .and_then(|config| config.exec)
        .unwrap_or_default()
}

/// This node's ticket verifier: the cluster JWKS, cached, with the
/// edge-boot block's refresh and clock-skew settings. Boot tickets and exec
/// tickets share it.
fn build_jwks_verifier(
    config: &crate::structs::WorkerEdgeBootConfigFull,
    manager_addr: &str,
    pool: &str,
    managers: Arc<crate::worker::managers::ManagerEndpoints>,
) -> R<crate::worker::boot_ticket::BootTicketVerifier> {
    let refresh = humantime::parse_duration(&config.jwks_refresh)
        .unwrap_or(crate::worker::boot_ticket::DEFAULT_JWKS_REFRESH);
    let skew = humantime::parse_duration(&config.clock_skew)
        .unwrap_or(crate::worker::boot_ticket::DEFAULT_CLOCK_SKEW);
    // Tickets carry the cluster's issuer, which the managers report on
    // pre-registration and every heartbeat; the verifier follows it.
    let issuer = crate::worker::boot_ticket::cluster_issuer().clone();
    if issuer.get().is_empty() {
        warn!(
            "The manager reported no workload-identity issuer (is workloadIdentity.enabled \
             false?); boot, session, and exec tickets are refused until one does"
        );
    }
    // The JWKS rides the same pinned TLS as every other manager call: keys
    // an on-path attacker could swap would let it forge tickets. Any manager
    // serves it, so the fetch fails over across the ones this node knows.
    let verifier = crate::worker::plane::manager_verifier(manager_addr)
        .context("ticket verification needs the manager's CA pin to fetch the JWKS over TLS")?;
    let source = crate::worker::boot_ticket::PinnedJwksSource::new(
        manager_addr,
        verifier,
        Duration::from_secs(5),
    )?
    .with_managers(move || {
        let mut order = vec![managers.active()];
        order.extend(managers.known());
        order
    });
    Ok(crate::worker::boot_ticket::BootTicketVerifier::new(
        Box::new(source),
        String::new(),
        pool,
        refresh,
        skew,
    )
    .following(issuer))
}

/// Ratifies claims by calling the manager's `RatifyClaim`, authenticated with
/// this node's own credentials.
///
/// Only the raft leader ratifies: a follower refuses with "not the raft
/// leader" rather than forwarding, and every refused ratification kills a
/// sandbox that already booted. So a refusal moves on to the other managers
/// this node knows, and the one that answered is kept for the next claim.
struct ManagerClaimRatifier {
    client: ManagerClient<Channel>,
    credentials: Arc<WorkerCredentials>,
    managers: Arc<crate::worker::managers::ManagerEndpoints>,
    /// The manager that last ratified, while it keeps doing so.
    leader: tokio::sync::Mutex<Option<(String, ManagerClient<Channel>)>>,
}

fn is_not_leader(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status.message().contains("not the raft leader")
}

#[tonic::async_trait]
impl crate::worker::server::ClaimRatifier for ManagerClaimRatifier {
    async fn ratify(
        &self,
        request: crate::structs::RatifyClaimRequest,
    ) -> R<crate::structs::RatifyClaimReply> {
        let cached = self.leader.lock().await.clone();
        let (first_addr, mut first) =
            cached.unwrap_or_else(|| (String::new(), self.client.clone()));
        let attempt = self
            .credentials
            .attach(tonic::Request::new(request.clone()));
        // Another manager is asked only when this one certainly did nothing:
        // it is not the leader, or the call never reached it. After a
        // timeout the leader may have ratified already, and a second
        // ratification would be refused and kill a sandbox that won.
        let status = match first.ratify_claim(attempt).await {
            Ok(reply) => return Ok(reply.into_inner()),
            Err(status) if is_not_leader(&status) || status.code() == tonic::Code::Unavailable => {
                status
            }
            Err(status) => {
                if crate::worker::managers::is_unreachable(&status) {
                    // Do not keep sending claims to a manager that went away.
                    *self.leader.lock().await = None;
                }
                return Err(status.into());
            }
        };
        *self.leader.lock().await = None;
        for addr in self.managers.known() {
            if addr == first_addr {
                continue;
            }
            let Ok(channel) = crate::worker::plane::manager_channel(&addr).await else {
                continue;
            };
            let mut client = ManagerClient::new(channel);
            let attempt = self
                .credentials
                .attach(tonic::Request::new(request.clone()));
            match client.ratify_claim(attempt).await {
                Ok(reply) => {
                    *self.leader.lock().await = Some((addr, client));
                    return Ok(reply.into_inner());
                }
                Err(status)
                    if is_not_leader(&status) || status.code() == tonic::Code::Unavailable =>
                {
                    continue
                }
                Err(status) => return Err(status.into()),
            }
        }
        Err(status.into())
    }
}

/// Probe for the kernel classifier and, when the node can run it, load and
/// attach it under the worker's cgroup parent, pinned in bpffs when bpffs is
/// mounted (so enforcement survives a worker restart). The probe's answer is
/// the `netKernel` node attribute.
#[cfg(target_os = "linux")]
fn open_kernel_classifier(
    cgroup_parent: &str,
) -> (
    crate::worker::netd::kernel::probe::Availability,
    Option<crate::worker::netd::kernel::runtime::KernelNet>,
) {
    use crate::worker::netd::kernel::probe::{probe, Availability};
    use crate::worker::netd::kernel::runtime::{KernelNet, KernelSettings};
    let root = std::path::Path::new(crate::task_plugins::CGROUP_ROOT);
    let availability = probe(root);
    if !availability.is_active() {
        if let Availability::Unavailable(reason) = &availability {
            info!(%reason, "Kernel classifier unavailable; host-mode egress stays cooperative");
        }
        return (availability, None);
    }
    let bpffs = std::fs::read_to_string("/proc/mounts")
        .unwrap_or_default()
        .lines()
        .any(|line| {
            line.split_whitespace().nth(1) == Some("/sys/fs/bpf") && line.contains(" bpf ")
        });
    if !bpffs {
        warn!("No bpffs at /sys/fs/bpf: the kernel classifier stops enforcing while the worker restarts");
    }
    match KernelNet::open(KernelSettings {
        cgroup_root: root.to_path_buf(),
        cgroup_parent: cgroup_parent.to_string(),
        pin_dir: bpffs.then(|| std::path::PathBuf::from("/sys/fs/bpf/legion")),
    }) {
        Ok(net) => {
            info!("Kernel classifier active: host-mode egress chains are enforced at connect()");
            (availability, Some(net))
        }
        Err(err) => {
            let reason = format!("{err:#}");
            warn!(%reason, "Kernel classifier failed to load; using the nftables guard");
            (Availability::Unavailable(reason), None)
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn open_kernel_classifier(
    _cgroup_parent: &str,
) -> (crate::worker::netd::kernel::probe::Availability, Option<()>) {
    (
        crate::worker::netd::kernel::probe::Availability::Unavailable("not Linux".into()),
        None,
    )
}

/// The host guard this worker type's `hostGuard` block describes. A block
/// the manager validated can't fail here; if it somehow does, the worker
/// keeps the default guard (no overrides), which refuses more, never less.
/// The kernel host guard's table without its allocations: Legion's ports on
/// this node and the operator's `hostGuard` entries (plan §6.13). The
/// manager's ports count when it shares the node (its address is local).
fn kernel_guard_base(
    security: &str,
    worker_port: u16,
    manager_addr: &str,
    cgroup_parent: &str,
) -> crate::worker::netd::kernel_guard::GuardTable {
    let mut table = crate::worker::netd::kernel_guard::GuardTable {
        cgroup_parent: cgroup_parent.to_string(),
        ..Default::default()
    };
    table.control_ports.insert(worker_port);
    if let Ok(uri) = manager_addr.parse::<http::Uri>() {
        let host = uri.host().unwrap_or_default().trim_matches(['[', ']']);
        let local = match host.parse::<std::net::IpAddr>() {
            Ok(ip) => {
                ip.is_loopback() || crate::structs::host_guard::interface_addresses().contains(&ip)
            }
            Err(_) => host == "localhost",
        };
        if let (true, Some(port)) = (local, uri.port_u16()) {
            // The API, the exec WebSocket, and telemetry.
            for offset in 0..=2 {
                if let Some(port) = port.checked_add(offset) {
                    table.control_ports.insert(port);
                }
            }
        }
    }
    let config: crate::structs::WorkerSecurityConfig =
        serde_json::from_str(security).unwrap_or_default();
    if let Some(host_guard) = config.host_guard {
        for entry in host_guard.deny {
            let entry = entry.trim().to_string();
            if entry.contains(':') {
                table.deny_v6.push(entry);
            } else {
                table.deny_v4.push(entry);
            }
        }
        for entry in host_guard.allow {
            if crate::structs::host_guard::parse_allow(&entry).is_err() {
                continue;
            }
            let entry = entry.trim();
            let split = match entry.strip_prefix('[') {
                Some(rest) => rest
                    .split_once("]:")
                    .map(|(addr, port)| (addr.to_string(), port)),
                None => entry
                    .rsplit_once(':')
                    .map(|(addr, port)| (addr.to_string(), port)),
            };
            if let Some((addr, port)) = split {
                if let Ok(port) = port.parse() {
                    table.allow.push((addr, port));
                }
            }
        }
    }
    table
}

/// The allocations the kernel host guard matches this beat: every desired
/// allocation whose cgroup leaf exists, with its own VIP block and upstream
/// loopback ports, which it may still reach.
fn kernel_guarded_allocations(
    desired: &[TaskAllocation],
) -> Vec<crate::worker::netd::kernel_guard::GuardedAllocation> {
    let parent = crate::task_plugins::exec_isolation::cgroup_parent();
    desired
        .iter()
        .filter_map(|allocation| {
            let cgroup = crate::worker::netd::kernel_guard::existing_leaf(
                &parent,
                allocation.allocation_id,
            )?;
            let network: Option<AllocNetwork> = serde_json::from_str(&allocation.network).ok();
            Some(crate::worker::netd::kernel_guard::GuardedAllocation {
                allocation_id: allocation.allocation_id,
                cgroup,
                vip_block: network
                    .as_ref()
                    .and_then(|network| network.vip_block.clone()),
                resolver: network
                    .as_ref()
                    .and_then(|network| network.resolver_address())
                    .and_then(|addr| addr.parse().ok()),
                gated: network.as_ref().is_some_and(|network| network.is_gated()),
                host_sockets: allocation.task_runtime != "sandbox",
                loopback_ports: network
                    .map(|network| {
                        network
                            .upstreams
                            .iter()
                            .filter_map(|upstream| upstream.local_port)
                            .collect()
                    })
                    .unwrap_or_default(),
            })
        })
        .collect()
}

fn host_guard_for(security: &str) -> crate::structs::host_guard::HostGuard {
    let config = if security.trim().is_empty() {
        crate::structs::WorkerSecurityConfig::default()
    } else {
        serde_json::from_str(security).unwrap_or_else(|err| {
            warn!(error = ?err, "Invalid worker security config; using the default host guard");
            crate::structs::WorkerSecurityConfig::default()
        })
    };
    let guard = match config.host_guard.as_ref() {
        Some(host_guard) => {
            crate::structs::host_guard::HostGuard::new(host_guard).unwrap_or_else(|err| {
                warn!(error = ?err, "Invalid hostGuard; using the default host guard");
                crate::structs::host_guard::HostGuard::default()
            })
        }
        None => crate::structs::host_guard::HostGuard::default(),
    };
    guard.set_node_addrs(crate::structs::host_guard::interface_addresses());
    guard
}

/// Waits for the node certificate (issued at registration, or on a later
/// heartbeat if the manager was unreachable), then serves the worker plane.
async fn serve_worker_plane(
    bind: std::net::SocketAddr,
    advertised: String,
    node_identity: Arc<crate::worker::plane::NodeIdentity>,
    router: tonic::transport::server::Router,
) -> R<()> {
    while !node_identity.has_certificate() {
        tokio::time::sleep(crate::worker::plane::ISSUE_RETRY).await;
    }
    info!("Worker plane listening on {bind} (mTLS); advertised as {advertised}");
    let result = crate::worker::plane::serve(bind, node_identity, router).await;
    if let Err(err) = &result {
        error!(error = ?err, "Worker plane listener exited");
    }
    result
}

#[instrument(err, skip_all)]
pub(crate) async fn connect_manager_with_retry(manager_addr: &str) -> R<ManagerClient<Channel>> {
    let mut backoff = Duration::from_millis(200);
    let max_backoff = Duration::from_secs(5);

    loop {
        match crate::worker::plane::manager_channel(manager_addr).await {
            Ok(channel) => return Ok(ManagerClient::new(channel)),
            Err(err) if format!("{err:?}").contains("CA pin mismatch") => {
                // Not a transient failure: whatever answers at this address
                // is not the cluster this worker's token names.
                let presented: Vec<String> = Vec::new();
                let pin = match crate::worker::plane::manager_verifier(manager_addr)
                    .map(|verifier| verifier.trust())
                {
                    Some(crate::cluster_tls::ManagerTrust::Pin(pin)) => pin,
                    _ => String::new(),
                };
                return Err(anyhow::Error::new(crate::worker::plane::PinMismatch(
                    crate::cluster_tls::pin_mismatch_message(manager_addr, &presented, &pin),
                )));
            }
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

/// Parses the worker's `resourceEnforcement` settings. An empty or
/// unparseable payload gets the defaults, which enforce.
fn parse_resource_enforcement(raw: &str) -> crate::structs::WorkerResourceEnforcementConfig {
    if raw.trim().is_empty() {
        return crate::structs::WorkerResourceEnforcementConfig::default();
    }
    match serde_json::from_str::<crate::structs::WorkerResourceEnforcementConfig>(raw) {
        Ok(config) => config,
        Err(err) => {
            warn!(error = ?err, "Invalid worker resourceEnforcement config; using the defaults");
            crate::structs::WorkerResourceEnforcementConfig::default()
        }
    }
}

/// Enforcement is on by default, so a Linux node that cannot carry it turns
/// it off here, once, with one warning, rather than failing each allocation
/// at its first cgroup write. Other platforms keep the switch: on macOS it
/// only measures sandboxes (see [`apply_resource_enforcement_env`]).
fn usable_resource_enforcement(
    mut config: crate::structs::WorkerResourceEnforcementConfig,
) -> crate::structs::WorkerResourceEnforcementConfig {
    if config.enabled && cfg!(target_os = "linux") {
        if let Err(err) = crate::worker::resources::cgroup_parent_usable(&config.cgroup_parent) {
            warn!(
                error = %format!("{err:#}"),
                "Resource enforcement is on but this node cannot create its cgroups; tasks run \
                 unconfined. Run the worker as root, delegate resourceEnforcement.cgroupParent \
                 to it, or set resourceEnforcement.enabled = false"
            );
            config.enabled = false;
        }
    }
    config
}

/// Publishes the worker's `resourceEnforcement` settings into the environment
/// the task runtimes read (see `docs/resource-enforcement.md`).
fn apply_resource_enforcement_env(config: &crate::structs::WorkerResourceEnforcementConfig) {
    if config.enabled {
        // On Linux the governor logs "enabled" once it really is
        // (`NodeResourceGovernor::from_settings`), so this function does not
        // claim it on the governor's behalf.
        if !cfg!(target_os = "linux") {
            // No cgroups here: the switch measures (Apple container sandboxes,
            // through `container stats`) and enforces nothing.
            info!(
                "Resource enforcement enabled: nothing is confined on this OS, but dev-mode \
                 sandboxes are measured for usage reports and idle policies"
            );
        }
        std::env::set_var(crate::constants::ENV_RESOURCE_ENFORCEMENT, "1");
        std::env::set_var(crate::constants::ENV_CGROUP_PARENT, &config.cgroup_parent);
        let overhead = config
            .sandbox
            .as_ref()
            .and_then(|sandbox| sandbox.vmm_overhead_mib)
            .unwrap_or(crate::constants::DEF_SANDBOX_VMM_OVERHEAD_MIB);
        std::env::set_var(
            crate::constants::ENV_SANDBOX_VMM_OVERHEAD_MIB,
            overhead.to_string(),
        );
    } else {
        std::env::remove_var(crate::constants::ENV_RESOURCE_ENFORCEMENT);
        std::env::remove_var(crate::constants::ENV_CGROUP_PARENT);
        std::env::remove_var(crate::constants::ENV_SANDBOX_VMM_OVERHEAD_MIB);
    }
}

/// Publishes the worker type's `container.allowCaps` into the environment the
/// `container` runtime reads at each start. An empty payload (no `container`
/// block) clears it, which is the default allowlist. A payload that does not
/// parse also falls back to the default, with a warning: the default is the
/// narrow side, so a bad config never grants more than it meant to.
fn apply_container_env(raw: &str) {
    let allowlist = crate::structs::WorkerContainerConfig::from_payload(raw)
        .and_then(|config| {
            crate::task_plugins::CapabilityAllowlist::from_config(config.allow_caps.as_deref())
        })
        .unwrap_or_else(|err| {
            warn!(
                error = %format!("{err:#}"),
                "Invalid worker container config; container tasks get the default capability \
                 allowlist"
            );
            crate::task_plugins::CapabilityAllowlist::default()
        });
    if allowlist == crate::task_plugins::CapabilityAllowlist::default() {
        std::env::remove_var(crate::constants::ENV_CONTAINER_ALLOW_CAPS);
    } else {
        info!(
            allow_caps = %allowlist.describe(),
            "Container capability allowlist set by the worker type (container.allowCaps)"
        );
        std::env::set_var(
            crate::constants::ENV_CONTAINER_ALLOW_CAPS,
            allowlist.to_env_value(),
        );
    }
}

/// Publishes the worker's `sandbox.bridges` pools into the environment for
/// the sandbox runtime, which leases TAP guests their addresses from them
/// (docs/task-runtime-sandbox.md, "Addressing"). Returns the bridge names,
/// which the node advertises. Unlike the VMM overhead, this does not depend
/// on resource enforcement.
fn apply_sandbox_bridges_env(sandbox: Option<&crate::structs::WorkerSandboxConfig>) -> Vec<String> {
    let env = crate::task_plugins::sandbox_addr::BRIDGES_ENV;
    let Some(bridges) = sandbox
        .map(|sandbox| &sandbox.bridges)
        .filter(|bridges| !bridges.is_empty())
    else {
        std::env::remove_var(env);
        return Vec::new();
    };
    match serde_json::to_string(bridges) {
        Ok(json) => std::env::set_var(env, json),
        Err(err) => warn!(error = %err, "Failed to publish sandbox.bridges to the sandbox runtime"),
    }
    bridges.keys().cloned().collect()
}

/// Parses the worker config's `artifacts` JSON payload (empty = defaults):
/// the fetch settings plus the node content store settings, which ride in the
/// same payload. A `None` store config means the operator disabled the store.
fn parse_artifacts_payload(
    raw: &str,
) -> R<(
    crate::structs::WorkerArtifactFetchConfigFull,
    Option<crate::content_store::ContentStoreSettings>,
    Option<crate::content_publish::ContentStoreTarget>,
)> {
    let payload: crate::structs::WorkerArtifactsPayload = if raw.trim().is_empty() {
        Default::default()
    } else {
        serde_json::from_str(raw).context("parse worker artifacts config")?
    };
    let content = payload.content.unwrap_or_default().resolve()?;
    Ok((
        payload.artifacts.fetch_with_defaults()?,
        content,
        payload.content_store,
    ))
}

/// The worker config's `sessions` block, from the artifacts payload it rides
/// in (defaults when absent or unreadable).
fn parse_sessions_payload(raw: &str) -> crate::structs::WorkerSessionsConfig {
    if raw.trim().is_empty() {
        return Default::default();
    }
    serde_json::from_str::<crate::structs::WorkerArtifactsPayload>(raw)
        .ok()
        .and_then(|payload| payload.sessions)
        .unwrap_or_default()
}

/// The session manifests in the worker's `contentStore`, when one is
/// configured: what `store` sessions claim generations in (docs/session-mode.md,
/// "No split brain").
fn build_session_manifests(
    target: Option<&crate::content_publish::ContentStoreTarget>,
    credentials: Arc<dyn crate::artifacts::CredentialSource>,
    node_id: &str,
    advertised: &str,
) -> Option<Arc<crate::session_manifest::SessionManifests>> {
    let target = target?;
    match crate::session_manifest::backend_for(target, credentials) {
        Ok(backend) => Some(Arc::new(crate::session_manifest::SessionManifests::new(
            backend, node_id, advertised,
        ))),
        Err(err) => {
            warn!(error = ?err, "Invalid contentStore config; store sessions cannot suspend here");
            None
        }
    }
}

/// The publisher for the worker's `contentStore`, when one is configured.
/// Publishing reads from the node content store, so a node without one
/// cannot publish: that is logged, not fatal, because nothing in 7a requires
/// a publish to succeed.
fn build_content_publishing(
    store: Option<Arc<crate::content_store::ContentStore>>,
    target: Option<&crate::content_publish::ContentStoreTarget>,
    credentials: Arc<dyn crate::artifacts::CredentialSource>,
) -> Option<Arc<crate::content_publish::ContentPublishing>> {
    let target = target?;
    let Some(store) = store else {
        warn!(url = %target.url, "contentStore is configured but the node content store is \
            disabled; this node will not publish content");
        return None;
    };
    match target
        .publisher(credentials)
        .and_then(|publisher| Ok((publisher, target.chunk_settings()?)))
    {
        Ok((publisher, settings)) => {
            info!(
                url = %target.url,
                chunk_size = settings.chunk_size,
                dedup_window_secs = settings.dedup_window.as_secs(),
                "Content publishing enabled"
            );
            Some(Arc::new(
                crate::content_publish::ContentPublishing::with_settings(
                    store, publisher, settings,
                ),
            ))
        }
        Err(err) => {
            warn!(error = ?err, "Invalid contentStore config; this node will not publish content");
            None
        }
    }
}

/// How often the worker runs content store GC (TTL expiry and budget trim).
/// Puts already evict on demand, so this only bounds how long expired content
/// lingers.
const CONTENT_GC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(600);

/// Opens the node content store and starts its GC loop. Share budgets
/// (`"20%"`) are sized here, once, against the filesystem holding `dir`. A
/// store that cannot open is logged and skipped: artifacts then stage
/// straight from origin, which is slower but correct.
fn open_content_store(
    dir: &Path,
    settings: Option<crate::content_store::ContentStoreSettings>,
) -> Option<Arc<crate::content_store::ContentStore>> {
    let Some(settings) = settings else {
        info!("Node content store disabled by worker config");
        return None;
    };
    // The directory must exist before its filesystem can be measured.
    let _ = std::fs::create_dir_all(dir);
    let filesystem_bytes = crate::content_store::filesystem_bytes(dir);
    let config = match settings.config_for(filesystem_bytes) {
        Ok(config) => config,
        Err(err) => {
            warn!(error = ?err, dir = %dir.display(), "Cannot size the node content store; \
                artifacts will not be cached");
            return None;
        }
    };
    info!(
        dir = %dir.display(),
        filesystem_bytes = filesystem_bytes.unwrap_or(0),
        artifacts_budget_bytes = config.budgets.artifacts,
        snapshots_budget_bytes = config.budgets.snapshots,
        min_free_disk_bytes = config.min_free_disk,
        "Sized node content store budgets"
    );
    let store = match crate::content_store::ContentStore::open(dir, config) {
        Ok(store) => Arc::new(store),
        Err(err) => {
            warn!(error = ?err, dir = %dir.display(), "Failed to open node content store; \
                artifacts will not be cached");
            return None;
        }
    };
    info!(dir = %dir.display(), "Node content store open");
    // Image pulls outside the staging pipeline (a `container` task without
    // `containerImageDesugar`) cache their layers here too.
    crate::content_store::set_node_store(Some(store.clone()));
    let gc_store = store.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(CONTENT_GC_INTERVAL);
        loop {
            interval.tick().await;
            let store = gc_store.clone();
            match tokio::task::spawn_blocking(move || store.gc()).await {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => warn!(error = ?err, "Content store GC failed"),
                Err(err) => warn!(error = ?err, "Content store GC task panicked"),
            }
        }
    });
    Some(store)
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

/// The interval to beat at after a reply, when it differs from `current`.
///
/// A served interval is a FLOOR over the worker-type `configured` cadence,
/// not an override: a 60s batch pool keeps beating every 60s while the
/// manager serves 2s to a small fleet, and only slows further once the fleet
/// grows past 60s. A reply with no interval (adaptive cadence off, or an
/// older manager) means "use your own", so turning adaptive cadence off
/// returns every worker to its worker-type value without a restart. A worker
/// is never penalized for honoring the served value: the manager derives the
/// node's lost window from the same number, and widens before it tells
/// anyone.
pub(crate) fn adopt_served_interval(
    current: Duration,
    configured: Duration,
    served: Option<u32>,
) -> Option<Duration> {
    let target = match served {
        Some(served) => configured.max(Duration::from_secs(
            u64::from(served).clamp(1, u64::from(MAX_WORKER_HEARTBEAT_SECONDS)),
        )),
        None => configured,
    };
    (target != current).then_some(target)
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
    manager_addr: String,
    node_identity: Arc<crate::worker::plane::NodeIdentity>,
    client: ManagerClient<Channel>,
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    identity_manager: Arc<WorkerIdentityManager>,
    last_successful_heartbeat: Arc<Mutex<std::time::Instant>>,
    credentials: Arc<WorkerCredentials>,
    registration: RegistrationPayload,
    netd: Arc<Netd>,
    // The worker-type cadence this worker starts at. Mutable because the
    // manager may serve a slower one on the reply (adaptive cadence,
    // docs/heartbeat-cadence-and-scale.md).
    mut heartbeat_interval: Duration,
    // Edge-boot spec cache to keep warm from the desired set; `None` on a
    // node that does not accept edge starts.
    spec_cache: Option<crate::worker::edge::SpecCache>,
    // The manager set this worker fails over across (spot-check P0-5).
    // `None` pins the worker to `client`'s one manager (tests).
    managers: Option<Arc<crate::worker::managers::ManagerEndpoints>>,
) -> R<()> {
    let mut backoff = HEARTBEAT_RETRY_MIN;
    let contact_store = task_manager
        .lock()
        .expect("task manager")
        .state_store()
        .clone();
    let configured_interval = heartbeat_interval;

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
            &manager_addr,
            &node_identity,
            &client,
            &task_manager,
            &identity_manager,
            &credentials,
            &netd,
            spec_cache.as_ref(),
            managers.as_ref(),
        )
        .await;
        // Failover bookkeeping: a manager that answered (with anything) is
        // reachable; consecutive transport failures move the worker to the
        // next known manager, and the retry below goes there.
        if let Some(managers) = &managers {
            match &tick_result {
                Ok(_) => managers.record_success(),
                Err(err) => {
                    if let Some(status) = err.downcast_ref::<tonic::Status>() {
                        managers.record_failure(status);
                    }
                }
            }
        }
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
                record_manager_contact(&contact_store);
                backoff = HEARTBEAT_RETRY_MIN;
                // Adopt a manager-served interval, once, with a full-width
                // re-phase so a fleet that all learns the new value within one
                // old interval spreads across the new period instead of
                // re-phasing onto the same boundary.
                match adopt_served_interval(heartbeat_interval, configured_interval, served) {
                    Some(next) => {
                        info!(
                            worker = %name,
                            from_seconds = heartbeat_interval.as_secs(),
                            to_seconds = next.as_secs(),
                            "Manager served a new heartbeat interval; re-phasing once"
                        );
                        heartbeat_interval = next;
                        sleep_or_work(rephase_sleep(next)).await;
                    }
                    None => {
                        sleep_or_work(jittered_heartbeat_interval(heartbeat_interval)).await;
                    }
                }
            }
            // The node token can be revoked (for example ACLs were just
            // enabled, the node was removed, or the manager state was reset).
            // A token the manager refuses is useless for re-registration
            // too, so drop it and re-register with the join secret to obtain
            // a fresh credential, then carry on.
            Err(err)
                if err
                    .downcast_ref::<tonic::Status>()
                    .is_some_and(|status| status.code() == tonic::Code::Unauthenticated) =>
            {
                warn!(worker = %name, "Heartbeat rejected as unauthenticated; re-registering");
                credentials.clear_node_token();
                match register_self(client.clone(), &registration, &credentials).await {
                    Ok(reported) => {
                        if let Some(managers) = &managers {
                            managers.learn(&reported);
                        }
                    }
                    Err(err) => {
                        warn!(worker = %name, error = ?err, "Re-registration failed; will retry")
                    }
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
// Same reason as `start_heartbeats`: each per-heartbeat subsystem is threaded
// by parameter so its lifetime stays visible at the call site.
#[allow(clippy::too_many_arguments)]
async fn heartbeat_tick(
    name: &str,
    manager_addr: &str,
    node_identity: &Arc<crate::worker::plane::NodeIdentity>,
    client: &ManagerClient<Channel>,
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    identity_manager: &Arc<WorkerIdentityManager>,
    credentials: &Arc<WorkerCredentials>,
    netd: &Arc<Netd>,
    spec_cache: Option<&crate::worker::edge::SpecCache>,
    managers: Option<&Arc<crate::worker::managers::ManagerEndpoints>>,
) -> R<Option<u32>> {
    let mut client_clone = client.clone();
    // The cadence the manager wants, when it dictates one. `None` (the
    // default, and every pre-adaptive manager) means "keep your worker-type
    // interval".
    let served_interval;

    {
        let start = std::time::Instant::now();
        let (
            allocations,
            failures,
            terminations,
            allocation_health,
            volume_reports,
            snapshot_reports,
        ) = {
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
            let snapshot_reports = manager.drain_snapshot_reports();
            (
                manager.allocations_snapshot(),
                failures,
                terminations,
                allocation_health,
                volume_reports,
                snapshot_reports,
            )
        };
        let snapshot_reports_for_redrive = snapshot_reports.clone();
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

        // What the datapath recorded since the last beat — denials and
        // service-graph samples — and its status for the allocations it was
        // last told about. Put back on a failed heartbeat, like every other
        // report on this request.
        let NetdReport {
            denials: net_denials,
            edges: net_edges,
            statuses: mut net_status,
        } = netd.report();
        let net_redrive = NetdReport {
            denials: net_denials.clone(),
            edges: net_edges.clone(),
            statuses: Vec::new(),
        };
        // Config-file templates render from the same feed; a template that
        // cannot render is the allocation's network problem too.
        for status in &mut net_status {
            status
                .problems
                .extend(TEMPLATES.problems(status.allocation_id));
        }
        // Measured usage, on the heartbeat that falls due each report
        // interval (docs/resource-enforcement.md, "Measuring usage"). Only
        // with enforcement on: without it there are no worker-owned cgroups
        // to read, and an absent report reads as "not governed". On macOS
        // the same switch measures dev-mode sandboxes through `container
        // stats` instead (docs/suspend-and-resume.md, "macOS dev loop").
        let usage = if crate::task_plugins::exec_isolation::enforcement_enabled() {
            let mut sampler = USAGE_SAMPLER.lock().unwrap_or_else(|p| p.into_inner());
            sampler.note_network_activity(net_edges.iter().map(|edge| edge.allocation_id));
            let now = std::time::Instant::now();
            sampler.due(now).then(|| {
                // Sessions ride along under their negative local keys, for
                // the idle policy only (docs/session-mode.md#idle-sessions):
                // their entries come back out before the report is sent.
                let targets = {
                    let manager = task_manager.lock().expect("task manager");
                    let mut targets = manager.usage_targets();
                    targets.extend(manager.session_idle_targets());
                    targets
                };
                // What the datapath and exec server saw, for activity and
                // the idle policy (docs/suspend-and-resume.md, "Idle policy").
                let activity = netd.activity();
                activity.retain(&targets.iter().map(|target| target.allocation_id).collect());
                let inputs = crate::worker::usage::ActivityInputs {
                    datapath: activity.snapshot(),
                    exec_sessions: crate::worker::exec::open_sessions(),
                };
                // One `container stats` call covers every Apple container
                // sandbox; a node with none never runs it.
                let apple = if targets.iter().any(|target| {
                    matches!(
                        target.source,
                        crate::worker::usage::UsageSource::AppleContainer(_)
                    )
                }) {
                    crate::worker::usage::read_apple_container_stats()
                } else {
                    Default::default()
                };
                let mut report = sampler.sample_with(
                    now,
                    chrono::Utc::now().timestamp_millis(),
                    &targets,
                    |target| {
                        crate::worker::usage::read_target_sample(
                            target,
                            &apple,
                            &crate::worker::usage::SysfsNicCounters,
                        )
                    },
                    crate::worker::usage::read_node_memory(),
                    &inputs,
                );
                let (session_usage, allocations): (Vec<_>, Vec<_>) =
                    std::mem::take(&mut report.allocations)
                        .into_iter()
                        .partition(|usage| usage.allocation_id < 0);
                report.allocations = allocations;
                suspend_idle_sessions(
                    task_manager,
                    crate::worker::task_manager::session_sleep::idle_sessions_to_suspend(
                        &session_usage,
                        chrono::Utc::now().timestamp(),
                    ),
                );
                // "Who goes first", from the same ordering the responder
                // acts with, so `legion node usage` previews it.
                let metas = targets
                    .iter()
                    .filter(|target| target.allocation_id >= 0)
                    .map(|target| (target.allocation_id, target.pressure.clone()))
                    .collect();
                let view = PRESSURE_SETTINGS
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone();
                crate::worker::pressure::annotate(
                    &mut report,
                    &metas,
                    view.as_ref().map(|(settings, _)| settings),
                    view.as_ref().is_some_and(|(_, under)| *under),
                );
                // The responder measures memory itself but not activity:
                // hand it this report's quiet times for "most idle first".
                crate::worker::pressure::publish_quiet_since(&report);
                report
            })
        } else {
            None
        };
        let pressure_actions =
            std::mem::take(&mut *PRESSURE_ACTIONS.lock().unwrap_or_else(|p| p.into_inner()));
        let pressure_actions_for_redrive = pressure_actions.clone();
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

        // What the node content store holds, as a delta against the epoch
        // the manager last acknowledged. Nothing advances until the reply
        // arrives, so a failed heartbeat needs no restore: the next diff
        // carries the same change.
        let content_report = {
            let store = task_manager.lock().expect("task manager").content_store();
            match crate::worker::content_report::advertised_set(store.as_deref()) {
                Ok(current) => {
                    let mut pending = CONTENT_ADVERTISER
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .prepare(current);
                    // Checkpoint headroom rides every report: the
                    // manager shows it and keeps new memory-checkpoint
                    // instances off a node whose budget is full.
                    pending.report.snapshot_budget =
                        crate::worker::content_report::snapshot_budget(store.as_deref());
                    Some(pending)
                }
                Err(err) => {
                    warn!(error = ?err, "Failed to list the content store for advertisement");
                    None
                }
            }
        };
        // The sessions this node runs (docs/session-mode.md), as a delta
        // against what the manager last acknowledged; same no-restore
        // contract as the content report above.
        let session_report = {
            let current = task_manager
                .lock()
                .expect("task manager")
                .session_statuses();
            SESSION_ADVERTISER
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .prepare(current)
        };
        // Elastic envelopes this node wants grown or shrunk
        // (docs/session-mode.md, "Elastic envelopes"). A resize stays in
        // flight until a reply answers it, so a failed heartbeat needs no
        // restore: the next one asks again.
        let envelope_resizes = task_manager
            .lock()
            .expect("task manager")
            .envelope_resize_requests(std::time::Instant::now());
        let exec_audit = crate::worker::direct_exec::take_audit();
        // On the stream, the heartbeat stops listing what the node runs: it
        // reports its stream position and a digest of its desired set, and
        // only the allocation health that changed since the manager last
        // acknowledged it (sandbox-scaling plan §6.13). Off the stream it
        // sends the full lists, as every worker did before.
        let work_stream = WORK_MIRROR
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .state();
        let (allocations, allocation_health, health_sent) = if work_stream.is_some() {
            let reported = HEALTH_REPORTED.lock().unwrap_or_else(|p| p.into_inner());
            let changed: Vec<AllocationHealth> = allocation_health
                .iter()
                .filter(|health| reported.get(&health.allocation_id) != Some(*health))
                .cloned()
                .collect();
            (Vec::new(), changed, allocation_health)
        } else {
            (allocations, allocation_health.clone(), allocation_health)
        };
        let req = credentials.attach(tonic::Request::new(HeartbeatRequest {
            work_stream: work_stream.clone(),
            content_locations: content_report
                .as_ref()
                .map(|pending| pending.report.clone()),
            session_report: Some(session_report.report.clone()),
            envelope_resizes,
            usage,
            pressure_actions,
            net_denials,
            net_status,
            net_edges,
            id: name.to_string(),
            provisional_claims,
            allocation_ids: allocations,
            failed_allocations: failures,
            allocation_health,
            volume_reports,
            snapshot_reports,
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
            exec_audit: exec_audit.clone(),
            exec_sessions: crate::worker::direct_exec::open_sessions(),
            exec_keys_version: crate::worker::direct_exec::keys_version(),
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
                // Same for checkpoint outcomes; the manager would re-send the
                // request anyway, but a capture should not wait a heartbeat.
                manager.requeue_snapshot_reports(snapshot_reports_for_redrive);
                // And the direct exec audit, so a dead connection loses no
                // record of who opened a shell.
                crate::worker::direct_exec::restore_audit(exec_audit);
                netd.redrive(net_redrive);
                // Pressure actions too: an eviction the manager never hears
                // about would leave it waiting on an allocation that is gone.
                // Restored ahead of anything queued since, to keep order.
                {
                    let mut pending = PRESSURE_ACTIONS.lock().unwrap_or_else(|p| p.into_inner());
                    let newer = std::mem::take(&mut *pending);
                    *pending = pressure_actions_for_redrive;
                    pending.extend(newer);
                }
                return Err(err.into());
            }
        };
        debug!(worker = %name, elapsed_ms = start.elapsed().as_millis() as u64, "Heartbeat acknowledged by manager");
        let mut reply = response.into_inner();
        served_interval = reply.next_interval_seconds;
        // Terminal reports the manager answered but could not record go out
        // again next beat: a batch task's dependents wait on them.
        task_manager
            .lock()
            .expect("task manager")
            .requeue_unrecorded_terminations(
                terminations_for_redrive,
                &reply.retry_terminated_allocations,
            );
        // Before the reconcile below reads this reply's desired set: an
        // answer is newer than any payload a lagging replica built.
        if !reply.envelope_resizes.is_empty() {
            task_manager
                .lock()
                .expect("task manager")
                .apply_envelope_resize_results(&reply.envelope_resizes, std::time::Instant::now());
        }
        crate::worker::direct_exec::apply_revocations(&reply.revoked_exec_sessions);
        crate::worker::direct_exec::apply_exec_keys(&reply.exec_keys_version, &reply.exec_keys);
        // The cluster's manager set, refreshed every beat and persisted when
        // it changes, so failover (and a restart) can reach any manager.
        if let Some(managers) = managers {
            managers.learn(&reply.manager_addresses);
        }
        // The cluster's ticket issuer, so a changed `workloadIdentity.issuer`
        // reaches this node's verifier within a beat.
        if crate::worker::boot_ticket::cluster_issuer().learn(&reply.workload_identity_issuer) {
            info!(issuer = %reply.workload_identity_issuer, "Following the cluster's workload-identity issuer");
        }
        // The manager acknowledged this health. Off the stream (or when the
        // manager did not recognize the stream) the next beat starts from a
        // full report again.
        {
            let mut reported = HEALTH_REPORTED.lock().unwrap_or_else(|p| p.into_inner());
            reported.clear();
            if reply.desired_via_stream {
                reported.extend(
                    health_sent
                        .into_iter()
                        .map(|health| (health.allocation_id, health)),
                );
            }
        }
        // A streaming worker's desired set is its mirror: the reply left it
        // out and says so. Everything below reconciles against it exactly as
        // it would against a desired set on the reply.
        if reply.desired_via_stream {
            let mut mirror = WORK_MIRROR.lock().unwrap_or_else(|p| p.into_inner());
            reply.desired_allocations = mirror.desired();
            reply.stop_notices = mirror.take_stops();
        }
        if let Some(pending) = content_report {
            CONTENT_ADVERTISER
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .acknowledge(pending, reply.content_resync);
        }
        SESSION_ADVERTISER
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .acknowledge(session_report, reply.session_resync);
        // The later copy of a session id another node also runs.
        if !reply.stop_sessions.is_empty() {
            task_manager.lock().expect("task manager").stop_sessions(
                &reply.stop_sessions,
                "duplicate of a session on another node",
            );
        }
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
                crate::worker::claim_window::Feedback::Verdict,
            );
        }
        // The lease is the backstop for every failure the reply path cannot
        // cover: a partition, a dead manager, a lost verdict. It runs on the
        // heartbeat tick because that is the one loop guaranteed to keep
        // running while everything else is stuck.
        crate::worker::edge::sweep_leases(task_manager);
        // The host guard's node-address class follows the interfaces.
        netd.send(NetdInput::RefreshNodeAddresses).await;
        // The kernel host guard follows the desired set: one rule chain per
        // allocation leaf, and the drops since the last beat as denials.
        if netd.kernel_guard_active() {
            netd.send(NetdInput::KernelGuard(kernel_guarded_allocations(
                &reply.desired_allocations,
            )))
            .await;
        }
        // The manager acknowledged us, so this reply is authoritative for the
        // datapath (fail-static: see `DesiredNetwork`). Gated namespaces come
        // first inside it, before the reconcile below starts anything, so a
        // task never runs without its namespace; then every listener, the
        // endpoint feed, and the policy assignments.
        netd.send(NetdInput::Desired(DesiredNetwork {
            desired: reply.desired_allocations.clone(),
            upstream_endpoints: reply.upstream_endpoints.clone(),
            net_policies: reply.net_policies.clone(),
        }))
        .await;
        // Config-file templates, rendered from the same feed; written before
        // any launch below so a task starts with its files in place.
        apply_templates(task_manager, &reply).await;
        // KV files marked `onChange = "update"`, rewritten under running
        // allocations whose rendered content moved.
        apply_live_kv_files(task_manager, &reply);
        refresh_node_identity(
            node_identity,
            netd,
            &mut client_clone,
            credentials,
            manager_addr,
            &reply.cluster_ca_pins,
        )
        .await;
        // Policy bodies by hash, KV-held ingress certificates, and upstream
        // credentials, for the desired set just applied (the reply's, or the
        // StreamWork mirror's when the work arrives over the stream). Last
        // known good is kept on any failure.
        fetch_datapath_material(netd, &mut client_clone, credentials, name).await;
        netd.persist();
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

        // Checkpoint work (the checkpoint channel, docs/suspend-and-resume.md).
        // New work runs off the heartbeat task; work already done re-queues
        // its report for the next beat; restore outcomes are re-sent while
        // the manager still attaches the restore payload.
        {
            let fresh = {
                let mut manager = task_manager.lock().expect("task manager");
                // The manager applied (or ignored) the reports this beat
                // carried before it built the reply, so finished work this
                // reply leaves out can be forgotten now.
                manager
                    .checkpoints_mut()
                    .mark_delivered(&snapshot_reports_for_redrive);
                manager
                    .checkpoints_mut()
                    .reconcile_restores(&reply.desired_allocations);
                manager
                    .checkpoints_mut()
                    .reconcile_local_suspensions(&reply.desired_allocations);
                manager.checkpoints_mut().accept(&reply.checkpoint_requests)
            };
            for request in fresh {
                let task_manager = task_manager.clone();
                tokio::task::spawn_blocking(move || {
                    crate::worker::checkpoints::run_request(&task_manager, &request);
                });
            }
        }

        let mut launched_this_tick: Vec<i64> = Vec::new();
        // Reconcile on EVERY acknowledged reply, an empty desired set included:
        // empty is the manager saying "this node should run nothing" (the last
        // allocation was stopped, drained, or preempted). Failures never reach
        // here, since a failed heartbeat returned above, so an empty list is
        // always authoritative and never a partial read.
        //
        // Deliver identity tokens for not-yet-running allocations BEFORE
        // reconcile starts them: env tokens must be present at exec time.
        let mut desired = reply.desired_allocations;
        // A raised identity epoch (a fork or snapshot of the instance was
        // captured) re-issues that allocation's tokens now. Before the
        // spawns below, so a new allocation's first sighting only records.
        identity_manager.observe_identity_epochs(
            desired
                .iter()
                .map(|allocation| (allocation.allocation_id, allocation.identity_epoch)),
        );
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
                .map(
                    |(in_task_root, host_task_dir)| crate::worker::identity::FileDeliveryBase {
                        in_task_root,
                        host_task_dir,
                    },
                );
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

        // Gated namespaces already exist (created above, ahead of the
        // datapath listeners bound inside them).
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

/// Rewrite the `onChange = "update"` KV files of running allocations
/// (docs/kv-store.md, "Live file updates"), then apply each allocation's
/// strongest `changeMode`: one in-place restart, or each distinct signal once,
/// after a random delay of up to the largest `splay` among the changed files.
/// Allocations that have not started yet are skipped: their start writes the
/// files from the same payload.
fn apply_live_kv_files(task_manager: &Arc<Mutex<WorkerTaskManager>>, reply: &HeartbeatReply) {
    use crate::structs::workload_identity::ChangeMode;
    for allocation in &reply.desired_allocations {
        // Cheap filter: almost no task config carries the marker.
        if !allocation.task_config.contains("\"liveUpdate\"") {
            continue;
        }
        let Ok(config) = serde_json::from_str::<serde_json::Value>(&allocation.task_config) else {
            continue;
        };
        let Some(files) = config.get("files").and_then(serde_json::Value::as_array) else {
            continue;
        };
        let applied = task_manager
            .lock()
            .expect("task manager")
            .apply_live_files(allocation.allocation_id, files);
        if applied.is_empty() {
            continue;
        }
        let splay = applied
            .iter()
            .filter_map(|marker| marker.splay.as_deref())
            .filter_map(|splay| crate::structs::netpolicy::parse_duration(splay).ok())
            .max()
            .unwrap_or_default();
        let restart = applied
            .iter()
            .any(|marker| marker.change_mode == ChangeMode::Restart);
        let mut signals: Vec<String> = applied
            .iter()
            .filter(|marker| marker.change_mode == ChangeMode::Signal)
            .filter_map(|marker| marker.change_signal.clone())
            .collect();
        signals.sort();
        signals.dedup();
        if !restart && signals.is_empty() {
            continue;
        }
        let task_manager = task_manager.clone();
        let allocation_id = allocation.allocation_id;
        tokio::spawn(async move {
            if !splay.is_zero() {
                let jitter = rand::random::<u64>() % (splay.as_millis().max(1) as u64);
                tokio::time::sleep(std::time::Duration::from_millis(jitter)).await;
            }
            let mut manager = task_manager.lock().expect("task manager");
            let local = manager.local_key(allocation_id);
            // A restart re-reads every file anyway, so it subsumes signals.
            let results = if restart {
                vec![manager.restart_allocation(local)]
            } else {
                signals
                    .iter()
                    .map(|signal| manager.signal_allocation(local, signal))
                    .collect()
            };
            for result in results {
                if let Err(err) = result {
                    warn!(allocation_id, error = ?err, "Live KV file change action failed");
                }
            }
        });
    }
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
            .map(|u| {
                (
                    u.service.clone(),
                    crate::worker::netd::specs::dialable_endpoints(u),
                )
            })
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
        let (in_task_root, host_task_dir) = delivery.unwrap_or((None, task_dir));
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

/// Keep the node certificate current: re-issued after a third of its
/// lifetime, or at once when the manager's trust bundle differs from the
/// node's (a CA rotation, or a CA added to or removed from the bundle). The
/// same identity serves the worker plane listener (hot-swapped, no restart)
/// and the mesh. Fail-static: a failed
/// issuance keeps the certificate the node has, and a node without one
/// refuses mesh flows rather than sending them in the clear.
async fn refresh_node_identity(
    node_identity: &crate::worker::plane::NodeIdentity,
    netd: &Netd,
    client: &mut ManagerClient<Channel>,
    credentials: &Arc<WorkerCredentials>,
    manager_addr: &str,
    manager_ca_pins: &[String],
) {
    if !node_identity.needs_refresh(Some(manager_ca_pins)) {
        return;
    }
    match node_identity.issue(client, credentials, manager_addr).await {
        Ok(()) => {
            if let Some(mesh) = node_identity.mesh_identity() {
                netd.send(NetdInput::MeshIdentity(mesh)).await;
            }
        }
        Err(err) => warn!(
            error = %format!("{err:#}"),
            "Failed to refresh the node certificate; keeping the current one"
        ),
    }
}

/// Fetch what the datapath needs from the manager, in one
/// `FetchDatapathMaterial` call per heartbeat: policy bodies it holds an
/// assignment for but has not seen,
/// `connect.upstreams[].credentials` ("Credential injection"), and KV-held
/// port certificates, client CAs, and upstream CAs ("TLS on ports"). Each ref
/// carries the version already held, so an unchanged value costs nothing on
/// the wire, and no call is made while nothing is wanted. Fail-static: a
/// failed call keeps what the datapath holds (a gated allocation with no
/// policy body yet stays closed until a later heartbeat succeeds).
async fn fetch_datapath_material(
    netd: &Netd,
    client: &mut ManagerClient<Channel>,
    credentials: &Arc<WorkerCredentials>,
    node_id: &str,
) {
    let refs = netd.wants().material;
    if refs.is_empty() {
        return;
    }
    let request = credentials.attach(tonic::Request::new(
        crate::structs::FetchDatapathMaterialRequest {
            node_id: node_id.to_string(),
            refs,
        },
    ));
    match client.fetch_datapath_material(request).await {
        Ok(response) => {
            netd.send(NetdInput::Material(response.into_inner().materials))
                .await
        }
        Err(status) => warn!(
            code = ?status.code(),
            message = status.message(),
            "Failed to fetch datapath material; keeping what the datapath holds"
        ),
    }
}

/// Start the memory pressure response (docs/resource-enforcement.md, "Memory
/// pressure") when this node can run it: enforcement on and able to enforce,
/// `/proc/meminfo` readable, and `memoryPressure.enabled` not false.
///
/// The responder runs on its own one-second tick rather than on the
/// heartbeat, because a node running out of memory cannot wait for the next
/// beat, let alone a manager round trip. Its actions queue in
/// [`PRESSURE_ACTIONS`] for the next heartbeat to report.
fn spawn_pressure_responder(
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    config: &crate::structs::WorkerResourceEnforcementConfig,
    node_name: &str,
) {
    if !config.enabled || !crate::worker::resources::enforcement_available() {
        return;
    }
    let Some(memory) = crate::worker::usage::read_node_memory() else {
        return;
    };
    let settings = match crate::worker::pressure::PressureSettings::resolve(
        config.memory_pressure.as_ref(),
        config.memory_ratio(),
        memory.total_bytes,
        node_name,
    ) {
        Ok(settings) => settings,
        Err(err) => {
            warn!(error = ?err, "Invalid memoryPressure config; the memory pressure response is off");
            return;
        }
    };
    *PRESSURE_SETTINGS.lock().unwrap_or_else(|p| p.into_inner()) = Some((settings.clone(), false));
    if !settings.enabled {
        info!("Memory pressure response disabled by resourceEnforcement.memoryPressure.enabled");
        return;
    }
    info!(
        min_free_bytes = settings.min_free_bytes,
        max_stall_percent = settings.max_stall_percent,
        settle_ms = settings.settle.as_millis() as u64,
        steps = %settings
            .active_steps()
            .iter()
            .map(|step| step.as_str())
            .collect::<Vec<_>>()
            .join(","),
        oversubscribe_memory = settings.oversubscribe_memory,
        "Memory pressure response armed"
    );
    let responder = Arc::new(Mutex::new(crate::worker::pressure::PressureResponder::new(
        settings,
    )));
    tokio::spawn(async move {
        let mut source = crate::worker::pressure::LivePressureSource::new(task_manager);
        let mut interval = tokio::time::interval(crate::worker::pressure::RESPONDER_TICK);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let responder = responder.clone();
            // Blocking: `memory.reclaim` reclaims synchronously, and the
            // eviction path takes the task-manager lock.
            let joined = tokio::task::spawn_blocking(move || {
                let mut responder = responder.lock().unwrap_or_else(|p| p.into_inner());
                let actions = responder.tick(
                    std::time::Instant::now(),
                    chrono::Utc::now().timestamp_millis(),
                    &mut source,
                );
                let view = (responder.settings().clone(), responder.under_pressure());
                (source, actions, view)
            })
            .await;
            let (returned, actions, view) = match joined {
                Ok(result) => result,
                Err(err) => {
                    warn!(error = ?err, "Memory pressure tick panicked; the response is off");
                    return;
                }
            };
            source = returned;
            *PRESSURE_SETTINGS.lock().unwrap_or_else(|p| p.into_inner()) = Some(view);
            if actions.is_empty() {
                continue;
            }
            for action in &actions {
                warn!(
                    allocation_id = action.allocation_id,
                    job_id = %action.job_id,
                    step = %action.step,
                    freed_bytes = action.freed_bytes,
                    "{}",
                    action.detail
                );
            }
            let metrics = crate::manager::telemetry::worker_metrics();
            for action in &actions {
                metrics.pressure_actions_total.inc(&[action.step.as_str()]);
            }
            PRESSURE_ACTIONS
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .extend(actions);
        }
    });
}

/// Pressure actions taken since the last successful heartbeat, oldest first.
static PRESSURE_ACTIONS: std::sync::LazyLock<Mutex<Vec<crate::structs::PressureAction>>> =
    std::sync::LazyLock::new(|| Mutex::new(Vec::new()));

/// The pressure response's resolved settings and whether a trigger fired on
/// its last tick, for the usage report. `None` when the response never armed.
static PRESSURE_SETTINGS: std::sync::LazyLock<
    Mutex<Option<(crate::worker::pressure::PressureSettings, bool)>>,
> = std::sync::LazyLock::new(|| Mutex::new(None));

/// Usage sampler state carried between heartbeats: the previous sample per
/// allocation (for CPU rates and activity) and when the last report went out.
static USAGE_SAMPLER: std::sync::LazyLock<Mutex<crate::worker::usage::UsageSampler>> =
    std::sync::LazyLock::new(|| Mutex::new(crate::worker::usage::UsageSampler::default()));

/// The content store advertisement's epoch state (docs/artifacts.md,
/// "Content locality"). Process-wide like the usage sampler: a restarted
/// worker process starts over with a full report, which is the protocol.
static CONTENT_ADVERTISER: std::sync::LazyLock<
    Mutex<crate::worker::content_report::ContentAdvertiser>,
> = std::sync::LazyLock::new(|| Mutex::new(Default::default()));

/// Capability a worker that streams its work advertises at registration.
pub const WORK_STREAM_CAPABILITY: &str = "work-stream";

/// The desired set as `StreamWork` delivered it (`worker/work_stream.rs`).
/// Process-wide: one worker per process, and a restarted process starts
/// over with a full resync, which is the protocol.
static WORK_MIRROR: std::sync::LazyLock<crate::worker::work_stream::SharedMirror> =
    std::sync::LazyLock::new(Default::default);

/// The epoch protocol for this node's session report (docs/session-mode.md),
/// mirroring [`CONTENT_ADVERTISER`].
static SESSION_ADVERTISER: std::sync::LazyLock<
    Mutex<crate::worker::session_report::SessionAdvertiser>,
> = std::sync::LazyLock::new(Default::default);

/// Allocation health as the manager last acknowledged it, so a streaming
/// worker's heartbeat sends only what changed (sandbox-scaling plan §6.13).
static HEALTH_REPORTED: std::sync::LazyLock<Mutex<HashMap<i64, AllocationHealth>>> =
    std::sync::LazyLock::new(Default::default);

/// Wakes the heartbeat loop before its interval: a `StreamWork` message
/// arrived, and its new allocations should start (and its stops land) now
/// rather than at the next beat; or a checkpoint report is waiting.
static HEARTBEAT_WAKE: std::sync::LazyLock<tokio::sync::Notify> =
    std::sync::LazyLock::new(tokio::sync::Notify::new);

/// Send a heartbeat now instead of at the next interval: a fresh checkpoint
/// report is queued (`worker/checkpoints.rs`), and a suspend, resume, or
/// fork should not wait out the cadence to settle. Wakes already pending
/// fold into one beat.
pub(crate) fn wake_heartbeat() {
    HEARTBEAT_WAKE.notify_one();
}

/// Act on one applied work-stream message: start the checkpoint work it
/// pushed and the allocations it added, and wake the heartbeat's reconcile
/// when it stopped or changed anything. A resync's additions go through the
/// reconcile too, which knows what a restarted worker restored and what it
/// must not start twice.
fn launch_streamed_work(
    launcher: &Arc<WorkerDaemonService>,
    applied: crate::worker::work_stream::Applied,
) {
    if applied.reconcile {
        HEARTBEAT_WAKE.notify_one();
    }
    // Checkpoint work starts the moment it is pushed, deduplicated against
    // what this node already has in flight or finished, exactly as when a
    // heartbeat delivers it (docs/suspend-and-resume.md, "How it works").
    if !applied.checkpoint_requests.is_empty() {
        let fresh = launcher
            .task_manager
            .lock()
            .map(|mut manager| {
                manager
                    .checkpoints_mut()
                    .accept_pushed(&applied.checkpoint_requests)
            })
            .unwrap_or_default();
        for request in fresh {
            let task_manager = launcher.task_manager.clone();
            tokio::task::spawn_blocking(move || {
                crate::worker::checkpoints::run_request(&task_manager, &request);
            });
        }
    }
    if applied.resync {
        return;
    }
    for allocation in applied.added {
        let wanted = launcher
            .task_manager
            .lock()
            .map(|manager| manager.wants_streamed_launch(&allocation))
            .unwrap_or(false);
        if !wanted {
            continue;
        }
        let launcher = launcher.clone();
        tokio::spawn(async move {
            let allocation_id = allocation.allocation_id;
            // `accept_work`, not the `post_work` handler: the stream is
            // already authenticated, and an in-process request carries no
            // manager certificate for the worker-plane check to accept.
            if let Err(status) = launcher.accept_work(allocation).await {
                // Recorded as the allocation's failure by the launch path;
                // the heartbeat reports it.
                debug!(allocation_id, error = %status.message(), "Streamed allocation did not start");
            }
        });
    }
}

/// Sleep for `duration`, or until a work-stream message wakes the loop.
async fn sleep_or_work(duration: Duration) {
    tokio::select! {
        _ = tokio::time::sleep(duration) => {}
        _ = HEARTBEAT_WAKE.notified() => {}
    }
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

/// Persist "the manager acknowledged us just now", so the next worker process
/// can fence from it before it reaches the manager (see [`run_boot_fence`]).
/// Best-effort: a failed write leaves an older record, which fences earlier.
fn record_manager_contact(store: &WorkerStateStore) {
    if let Err(err) = store.record_manager_contact(std::time::SystemTime::now()) {
        debug!(error = ?err, "Failed to persist last manager contact");
    }
}

/// The state store a previous worker process on this host left behind, when
/// it can be found without asking the manager: an explicit state path, an
/// explicit `--name`, or the single worker directory on disk. `None` means a
/// fresh node (or an ambiguous one, which registration rejects anyway), so
/// there is nothing to fence.
fn boot_state_store(name_override: Option<&str>) -> Option<WorkerStateStore> {
    if std::env::var_os("LEGION_WORKER_STATE_PATH").is_some() {
        return Some(WorkerStateStore::new(WorkerStateStore::default_path()));
    }
    let name = match name_override {
        Some(name) => name.to_string(),
        None => find_existing_worker_id(&worker_state_dir())
            .ok()
            .flatten()?,
    };
    Some(WorkerStateStore::new(WorkerStateStore::path_for_worker(
        &name,
    )))
}

/// The self-fence for the stretch of boot before this process has reached
/// its manager: once a second, kill restored stateful allocations whose
/// window the persisted silence exceeds. Exits at once when no contact was
/// ever recorded (nothing to measure from); otherwise runs until the caller
/// aborts it.
async fn run_boot_fence(store: WorkerStateStore) {
    loop {
        let Some(since_last_contact) =
            store.since_last_manager_contact(std::time::SystemTime::now())
        else {
            return;
        };
        let tick_store = store.clone();
        let fenced = tokio::task::spawn_blocking(move || {
            crate::worker::task_manager::fence_persisted_allocations(
                &tick_store,
                since_last_contact,
            )
        })
        .await;
        match fenced {
            Ok(Ok(fenced)) if !fenced.is_empty() => warn!(
                fenced_allocations = ?fenced,
                lost_contact_secs = since_last_contact.as_secs(),
                "Boot self-fence killed restored allocations after prolonged manager silence"
            ),
            Ok(Err(err)) => warn!(error = ?err, "Boot self-fence could not read worker state"),
            Err(err) => warn!(error = ?err, "Boot self-fence task failed"),
            Ok(Ok(_)) => {}
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
        // Probed off the lock and off the async runtime: a readiness check
        // can shell out to a runtime CLI, and a hung one used to hold the
        // task-manager lock (and with it the heartbeat) for as long as it
        // hung (see `readiness_of`).
        let checks_data = {
            let plugins = task_manager.lock().expect("task manager").plugin_handles();
            let additional = additional_node_checks.clone();
            tokio::task::spawn_blocking(move || {
                run_health_checks(
                    additional,
                    Some(crate::task_plugins::readiness_of(&plugins)),
                )
            })
            .await?
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

/// Whether a registration-time refusal means "this credential is not good
/// for this node", so the next credential should be tried: the node token
/// was revoked or belongs to another node or pool.
fn credential_refused(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
    )
}

/// Register (or re-register) this node and return the manager addresses the
/// reply carried (`WorkerRegisterReply.manager_addresses`).
///
/// Credential order: the node token first, when one is held — a restarted
/// worker persisted it, and it does not expire — presented as the bearer
/// credential with no join secret, which the manager accepts for this node
/// in its current pool and answers without rotating. If the manager refuses
/// it, the token is dropped and the join secret is used, as on first join.
/// This is what makes short-lived join tokens safe (spot-check P0-8).
#[instrument(err, skip_all)]
async fn register_self(
    client: ManagerClient<Channel>,
    registration: &RegistrationPayload,
    credentials: &Arc<WorkerCredentials>,
) -> R<Vec<String>> {
    let mut c2 = client.clone();

    let build_version = env!("CARGO_PKG_VERSION");
    let request = |join_token: Option<String>| WorkerRegisterRequest {
        name: registration.name.clone(),
        config_attrs: registration.config_attrs.clone(),
        fingerprint_attrs: registration.fingerprint_attrs.clone(),
        health_checks: registration.health_checks.clone(),
        pool: registration.pool.clone(),
        join_token,
        build_version: build_version.to_string(),
        protocol_version: PROTOCOL_VERSION,
        capabilities: registration.capabilities.clone(),
    };

    let response = if credentials.has_node_token() {
        match c2
            .worker_register(credentials.attach(tonic::Request::new(request(None))))
            .await
        {
            Ok(response) => response,
            Err(status) if credential_refused(&status) && credentials.join_secret().is_some() => {
                warn!(
                    worker = %registration.name,
                    code = %status.code(),
                    reason = %status.message(),
                    "The manager refused this node's persisted node token; registering with                      the join token instead"
                );
                credentials.clear_node_token();
                c2.worker_register(tonic::Request::new(request(
                    credentials.join_secret().cloned(),
                )))
                .await?
            }
            Err(status) => return Err(status.into()),
        }
    } else {
        c2.worker_register(tonic::Request::new(request(
            credentials.join_secret().cloned(),
        )))
        .await?
    };
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

    Ok(reply.manager_addresses)
}

/// Fetch this worker's typed config, with the same credential order as
/// [`register_self`]: the persisted node token first, then the join secret.
#[instrument(err, skip_all)]
async fn pre_register_self(
    client: ManagerClient<Channel>,
    worker_type: String,
    credentials: &Arc<WorkerCredentials>,
) -> Result<Response<WorkerPreRegisterReply>, tonic::Status> {
    let mut c2 = client.clone();

    if credentials.has_node_token() {
        let req = credentials.attach(tonic::Request::new(WorkerPreRegisterRequest {
            worker_type: worker_type.clone(),
            join_token: None,
        }));
        match c2.worker_pre_register(req).await {
            Ok(response) => return Ok(response),
            Err(status) if credential_refused(&status) && credentials.join_secret().is_some() => {
                warn!(
                    code = %status.code(),
                    reason = %status.message(),
                    "The manager refused this node's persisted node token; using the join \
                     token instead"
                );
                credentials.clear_node_token();
            }
            Err(status) => return Err(status),
        }
    }

    let req = tonic::Request::new(WorkerPreRegisterRequest {
        worker_type,
        join_token: credentials.join_secret().cloned(),
    });
    c2.worker_pre_register(req).await
}

/// Pre-register against whichever known manager answers. A manager that
/// cannot be reached moves the worker to the next known address (the
/// persisted set from a previous run, plus the token's); the loop retries
/// forever with backoff, as the single-manager connect did. A manager that
/// answers with an error ends the loop with that error.
async fn pre_register_until_reachable(
    client: &ManagerClient<Channel>,
    managers: &Arc<crate::worker::managers::ManagerEndpoints>,
    worker_type: String,
    credentials: &Arc<WorkerCredentials>,
) -> R<Response<WorkerPreRegisterReply>> {
    let mut backoff = Duration::from_millis(500);
    loop {
        match pre_register_self(client.clone(), worker_type.clone(), credentials).await {
            Ok(response) => {
                managers.record_success();
                return Ok(response);
            }
            Err(status) if crate::worker::managers::is_unreachable(&status) => {
                let manager = managers.active();
                warn!(
                    %manager,
                    error = %status.message(),
                    retry_in_ms = backoff.as_millis() as u64,
                    "Failed to reach manager; trying the next known manager"
                );
                managers.fail_over();
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, Duration::from_secs(10));
            }
            Err(status) => return Err(status.into()),
        }
    }
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

    /// Only the in-process worker stays in its process at shutdown, and only
    /// on macOS does it stop its tasks: a standalone worker exits and leaves
    /// its tasks for the next start to re-adopt.
    #[test]
    fn only_the_in_process_worker_on_macos_stops_its_tasks_at_shutdown() {
        assert_eq!(
            WorkerExit::for_worker(false),
            WorkerExit {
                stop_tasks: false,
                exit_process: true,
            }
        );
        assert_eq!(
            WorkerExit::for_worker(true),
            WorkerExit {
                stop_tasks: cfg!(target_os = "macos"),
                exit_process: false,
            }
        );
    }

    /// A heartbeat whose `onChange = "update"` file content moved rewrites
    /// the file under the running process with its mode, signals it, and
    /// leaves the stored config holding the new content; an unmarked file
    /// that changed is left for the next start.
    #[tokio::test]
    async fn live_kv_files_are_rewritten_and_signal_the_running_task_test() -> R<()> {
        let dir = TempDir::new()?;
        let task_manager = Arc::new(Mutex::new(WorkerTaskManager::with_state_store(
            WorkerStateStore::new(dir.path().join("worker_state.json")),
        )));
        task_manager.lock().unwrap().load_builtin_plugins();
        let pem = dir.path().join("tls/bundle.pem");
        let other = dir.path().join("other.conf");
        let hup = dir.path().join("got-hup");
        let file = |path: &std::path::Path, content: &str, live: bool| {
            let mut entry =
                json!({ "path": path.display().to_string(), "mode": "0400", "content": content });
            if live {
                entry["liveUpdate"] = json!({ "changeMode": "signal", "changeSignal": "SIGHUP" });
            }
            entry
        };
        let config = |pem_content: &str, other_content: &str| {
            json!({
                "command": ["sh", "-c", format!(
                    "trap 'touch {}' HUP; for i in $(seq 1 100); do sleep 0.1; done",
                    hup.display()
                )],
                "files": [file(&pem, pem_content, true), file(&other, other_content, false)],
            })
        };
        let allocation = crate::structs::Allocation {
            id: 41,
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
            config: config("old", "old"),
            ..crate::structs::TaskConfig::new("server", "exec")
        };
        task_manager
            .lock()
            .unwrap()
            .run_allocation(&allocation, &task)?;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(std::fs::read_to_string(&pem)?, "old");

        let reply = HeartbeatReply {
            desired_allocations: vec![TaskAllocation {
                allocation_id: 41,
                job_id: "web".into(),
                task_name: "server".into(),
                task_runtime: "exec".into(),
                task_config: config("new", "new").to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        apply_live_kv_files(&task_manager, &reply);

        assert_eq!(std::fs::read_to_string(&pem)?, "new");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(&pem)?.permissions().mode() & 0o777, 0o400);
        }
        assert_eq!(
            std::fs::read_to_string(&other)?,
            "old",
            "unmarked files wait for a start"
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while !hup.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "task never observed SIGHUP"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        // A restart would re-materialize the stored config: it holds the
        // new content for the live file, the old one for the other.
        let stored = task_manager
            .lock()
            .unwrap()
            .task_config(41)
            .context("stored task config")?
            .config["files"]
            .clone();
        assert_eq!(stored[0]["content"], "new");
        assert_eq!(stored[1]["content"], "old");

        // The same reply again is a no-op: nothing rewritten, no signal.
        std::fs::remove_file(&hup)?;
        apply_live_kv_files(&task_manager, &reply);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(!hup.exists());
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

        let node_identity = crate::worker::plane::NodeIdentity::load(
            &temp_dir.path().join("identity"),
            "test-worker",
            None,
            None,
        )?;
        let heartbeats = tokio::spawn(start_heartbeats(
            "test-worker".into(),
            "http://127.0.0.1:1".into(),
            node_identity,
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
                capabilities: worker_capabilities(false, false),
            },
            Arc::new(Netd::new(
                Arc::new(UpstreamForwarder::new()),
                temp_dir.path().join("allocs"),
            )),
            Duration::from_secs(WORKER_HEARTBEAT_DURATION_SEC),
            None,
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

    /// Spot-check P0-8: a restarted worker registers with the node token it
    /// persisted, so it comes back after its join token is gone; a refused
    /// node token falls back to the join token; and the secret on disk is
    /// owner-only.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_restarted_worker_rejoins_on_its_persisted_node_token() -> R<()> {
        let dir = TempDir::new()?;
        let store = crate::storage::store::Store::start_storage(
            &crate::structs::config::ManagerConfigFull {
                store_path: dir.path().join("store").to_string_lossy().into_owned(),
                ..Default::default()
            },
        )?;
        store.insert_worker_type(&WorkerConfig {
            name: "default".into(),
            pool: "default".into(),
            ..Default::default()
        })?;
        let auth = Arc::new(crate::manager::acl::Authenticator::from_config(
            &crate::structs::config::AclConfig {
                enabled: Some(true),
                ..Default::default()
            }
            .with_defaults(),
        )?);
        let (_, composite) = auth
            .mint_join_token(
                &store,
                "http://127.0.0.1:4747",
                "default",
                Some(Duration::from_secs(3600)),
                "restart test",
            )
            .map_err(|status| anyhow!("{status}"))?;
        let join_secret = JoinToken::decode(&composite)?.secret;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = format!("http://127.0.0.1:{}", listener.local_addr()?.port());
        let service = crate::structs::manager_server::ManagerServer::new(
            crate::manager::server::ManagerDaemonService::with_acl(store.clone(), auth),
        );
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let _ = tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await;
        });
        let client = ManagerClient::new(Channel::from_shared(addr)?.connect_lazy());
        let state = WorkerStateStore::new(dir.path().join("w1").join("worker_state.json"));
        let payload = RegistrationPayload {
            name: "w1".into(),
            pool: "default".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: "{}".into(),
            health_checks: "[]".into(),
            capabilities: Vec::new(),
        };

        // First join: the join token mints a node token, persisted at once.
        let first = Arc::new(WorkerCredentials::with_join_secret(Some(
            join_secret.clone(),
        )));
        first.persist_to(state.clone());
        pre_register_self(client.clone(), "default".into(), &first).await?;
        register_self(client.clone(), &payload, &first).await?;
        let token = state.load_node_token().context("node token persisted")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(state.node_token_path())?
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }

        // Restart after the join token stopped working: the persisted node
        // token alone pre-registers and registers, and is kept.
        let restarted = Arc::new(WorkerCredentials::with_join_secret(Some(
            "lgnjoin_expired".into(),
        )));
        assert!(restarted.restore_from(&state));
        restarted.persist_to(state.clone());
        pre_register_self(client.clone(), "default".into(), &restarted).await?;
        register_self(client.clone(), &payload, &restarted).await?;
        assert_eq!(state.load_node_token().as_deref(), Some(token.as_str()));

        // A refused node token falls back to the join token, which may not
        // take over the node while it is live (the node token is what proves
        // a restart)...
        state.save_node_token("lgn_not_a_real_token")?;
        let fallback = Arc::new(WorkerCredentials::with_join_secret(Some(join_secret)));
        assert!(fallback.restore_from(&state));
        fallback.persist_to(state.clone());
        pre_register_self(client.clone(), "default".into(), &fallback).await?;
        let err = register_self(client.clone(), &payload, &fallback)
            .await
            .expect_err("a join token may not take over a live node");
        assert!(
            format!("{err:#}").contains("registered and live"),
            "{err:#}"
        );
        // ...and rejoins it, minting and persisting a replacement, once the
        // node has gone lost.
        store.mark_node_lost("w1")?;
        register_self(client.clone(), &payload, &fallback).await?;
        let replaced = state.load_node_token().context("replacement persisted")?;
        assert_ne!(replaced, "lgn_not_a_real_token");

        // Neither credential valid: the refusal surfaces.
        state.save_node_token("lgn_not_a_real_token")?;
        let stranded = Arc::new(WorkerCredentials::with_join_secret(Some(
            "lgnjoin_expired".into(),
        )));
        assert!(stranded.restore_from(&state));
        let err = register_self(client.clone(), &payload, &stranded)
            .await
            .expect_err("no valid credential");
        assert!(
            err.downcast_ref::<tonic::Status>()
                .is_some_and(|status| status.code() == tonic::Code::Unauthenticated),
            "{err:#}"
        );
        Ok(())
    }

    /// Serve a real manager on an ephemeral loopback port, with one active
    /// node and no allocations, so every heartbeat reply carries an empty
    /// desired set.
    async fn serve_empty_manager(
        dir: &TempDir,
        node_id: &str,
    ) -> R<(ManagerClient<Channel>, tokio::task::JoinHandle<()>)> {
        let (client, server, _store) = serve_manager(dir, node_id, "{}").await?;
        Ok((client, server))
    }

    /// [`serve_empty_manager`], with the manager's store handed back so a
    /// test can submit and plan jobs on the node and read the outcome.
    /// `fingerprint_attrs` is the node's fingerprint (`{}` gives it no
    /// capacity to plan against).
    async fn serve_manager(
        dir: &TempDir,
        node_id: &str,
        fingerprint_attrs: &str,
    ) -> R<(
        ManagerClient<Channel>,
        tokio::task::JoinHandle<()>,
        crate::storage::store::Store,
    )> {
        let store = crate::storage::store::Store::start_storage(
            &crate::structs::config::ManagerConfigFull {
                store_path: dir.path().join("store").to_string_lossy().into_owned(),
                ..Default::default()
            },
        )?;
        store.insert_node(crate::structs::NodeMsg {
            id: node_id.into(),
            pool: "default".into(),
            status: "active".into(),
            availability: "available".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: fingerprint_attrs.into(),
            dynamic_attrs: "{}".into(),
            health_checks: "{}".into(),
            last_heartbeat: "2026-01-01 00:00:00 +00:00:00".into(),
            heartbeat_source: String::new(),
        })?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = format!("http://127.0.0.1:{}", listener.local_addr()?.port());
        let service = crate::structs::manager_server::ManagerServer::new(
            crate::manager::server::ManagerDaemonService::new(store.clone()),
        );
        let server = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let _ = tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await;
        });
        let client = ManagerClient::new(Channel::from_shared(addr)?.connect_lazy());
        Ok((client, server, store))
    }

    /// The pieces `heartbeat_tick` needs besides the task manager, for tests
    /// that drive several beats against one manager.
    struct BeatHarness {
        node_id: String,
        client: ManagerClient<Channel>,
        node_identity: Arc<crate::worker::plane::NodeIdentity>,
        identity_manager: Arc<WorkerIdentityManager>,
        credentials: Arc<WorkerCredentials>,
        netd: Arc<Netd>,
        _identity_dir: TempDir,
    }

    impl BeatHarness {
        fn new(node_id: &str, client: &ManagerClient<Channel>) -> R<Self> {
            let credentials = Arc::new(WorkerCredentials::default());
            let identity_manager =
                Arc::new(WorkerIdentityManager::new(Arc::new(ManagerTokenIssuer {
                    client: client.clone(),
                    node_id: node_id.into(),
                    credentials: credentials.clone(),
                })));
            let identity_dir = tempfile::tempdir()?;
            let node_identity =
                crate::worker::plane::NodeIdentity::load(identity_dir.path(), node_id, None, None)?;
            Ok(BeatHarness {
                node_id: node_id.into(),
                client: client.clone(),
                node_identity,
                identity_manager,
                credentials,
                netd: Arc::new(Netd::new(
                    Arc::new(UpstreamForwarder::new()),
                    identity_dir.path().join("allocs"),
                )),
                _identity_dir: identity_dir,
            })
        }

        async fn beat(&self, task_manager: &Arc<Mutex<WorkerTaskManager>>) -> R<()> {
            heartbeat_tick(
                &self.node_id,
                "http://127.0.0.1:1",
                &self.node_identity,
                &self.client,
                task_manager,
                &self.identity_manager,
                &self.credentials,
                &self.netd,
                None,
                None,
            )
            .await?;
            Ok(())
        }
    }

    /// Regression (spot check, DAG stuck after its first step): a batch task
    /// the worker runs to a clean exit is reported over the real heartbeat
    /// path, the manager records it, and the `success` dependent it held
    /// back is placed. Shaped like `tests/spot-check/jobs/05-dag.ncl`: exec
    /// runtime, batch lifecycle, both steps in one group.
    #[tokio::test]
    async fn a_batch_exit_reported_by_heartbeat_releases_its_success_dependent() -> R<()> {
        use crate::manager::scheduler::{PlanMode, Scheduler};
        use crate::structs::{
            TaskDependency, TaskDependencyCondition, TaskLifecycle, TaskStatusState,
        };
        let temp_dir = TempDir::new()?;
        let node_id = "dag-node";
        let fingerprint = json!({
            "memory_total": "8589934592",
            "cpus": { "cpu-1": { "frequency": "2400" }, "cpu-2": { "frequency": "2400" } },
        })
        .to_string();
        let (client, server, store) = serve_manager(&temp_dir, node_id, &fingerprint).await?;

        let step = |name: &str| crate::structs::TaskConfig {
            lifecycle: TaskLifecycle::Batch,
            group: "pipeline".into(),
            config: json!({"command": ["true"]}),
            ..crate::structs::TaskConfig::new(name, "exec")
        };
        let job = crate::structs::JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "dag".into(),
            tasks: vec![
                step("fetch"),
                crate::structs::TaskConfig {
                    depends_on: vec![TaskDependency {
                        task: "fetch".into(),
                        condition: TaskDependencyCondition::Success,
                    }],
                    ..step("validate")
                },
            ],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[crate::structs::Submission {
                path: "dag.ncl".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config("dag")?.id();
        let placed = Scheduler::new(&store)?.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(placed.allocations.len(), 1, "validate waits for fetch");
        let fetch = placed.allocations[0].clone();

        let task_manager = Arc::new(Mutex::new(WorkerTaskManager::with_state_store(
            WorkerStateStore::new(temp_dir.path().join("worker_state.json")),
        )));
        task_manager
            .lock()
            .expect("task manager")
            .load_builtin_plugins();
        let harness = BeatHarness::new(node_id, &client)?;

        // Beat one: the desired set launches `fetch`.
        harness.beat(&task_manager).await?;
        // The supervisor's tick, run by hand: notice the exit and apply the
        // batch lifecycle's policy to it.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let exit = loop {
            let exits = task_manager.lock().expect("task manager").poll_task_exits();
            if let Some(exit) = exits
                .into_iter()
                .find(|exit| exit.allocation_id == fetch.id)
            {
                break exit;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "fetch never exited on the worker"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        crate::worker::task_supervisor::handle_exit(&task_manager, exit).await;
        // Beat two: carries the terminal report.
        harness.beat(&task_manager).await?;

        let recorded = store
            .get_allocation_by_id(fetch.id)?
            .context("fetch allocation is gone")?;
        assert_eq!(recorded.status, "complete");
        assert_eq!(
            store
                .task_statuses_for_job_config(&job_config_id)?
                .get(&fetch.task_config_id),
            Some(&TaskStatusState::Success)
        );
        let released =
            Scheduler::new(&store)?.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert!(released.held_tasks.is_empty(), "{:?}", released.held_tasks);
        assert_eq!(released.allocations.len(), 1, "validate is placed");
        server.abort();
        Ok(())
    }

    /// A terminal report the manager answered but could not record (here:
    /// it has no row for the allocation, as a replica that has not applied
    /// the placement yet would not) goes out again on the next beat, and
    /// stops going out once the worker no longer holds the allocation.
    #[tokio::test]
    async fn an_unrecorded_terminal_report_is_sent_again() -> R<()> {
        let temp_dir = TempDir::new()?;
        let node_id = "retry-node";
        let (client, server) = serve_empty_manager(&temp_dir, node_id).await?;
        let task_manager = Arc::new(Mutex::new(WorkerTaskManager::with_state_store(
            WorkerStateStore::new(temp_dir.path().join("worker_state.json")),
        )));
        let exit = crate::worker::task_manager::TaskExit {
            allocation_id: 51,
            task_name: "fetch".into(),
            exit_code: Some(0),
            signal: None,
            oom_killed_at_bytes: None,
            stderr_tail: None,
        };
        {
            let mut manager = task_manager.lock().expect("task manager");
            manager.load_builtin_plugins();
            let allocation = crate::structs::Allocation {
                id: 51,
                status: "running".into(),
                resources: json!({}),
                node_id: node_id.into(),
                job_config_id: "job-1".into(),
                task_config_id: 1,
                job_id: "dag".into(),
                instance_id: None,
                network: None,
            };
            let task = crate::structs::TaskConfig {
                config: json!({"command": ["sleep", "30"]}),
                lifecycle: crate::structs::TaskLifecycle::Batch,
                ..crate::structs::TaskConfig::new("fetch", "exec")
            };
            manager.run_allocation(&allocation, &task)?;
            manager.record_termination(51, &exit, true);
        }
        let harness = BeatHarness::new(node_id, &client)?;

        // The manager has no row for 51: the report comes back for a retry.
        // The same reply's empty desired set also stops the allocation.
        harness.beat(&task_manager).await?;
        let pending = task_manager
            .lock()
            .expect("task manager")
            .drain_terminations();
        assert_eq!(
            pending.iter().map(|r| r.allocation_id).collect::<Vec<_>>(),
            vec![51],
            "the unrecorded report is queued again"
        );
        task_manager
            .lock()
            .expect("task manager")
            .requeue_terminations(pending);

        // Sent again, refused again, and now the worker no longer holds the
        // allocation: the report ends with it.
        harness.beat(&task_manager).await?;
        let mut manager = task_manager.lock().expect("task manager");
        assert!(manager.drain_terminations().is_empty());
        while manager.has_terminating() || manager.has_draining() {
            manager.reap_draining();
            manager.reap_terminating();
            std::thread::sleep(Duration::from_millis(20));
        }
        server.abort();
        Ok(())
    }

    /// Regression (audit W1): the daemon skipped the reconcile whenever the
    /// desired set was empty, so stopping a node's LAST allocation left it
    /// running forever. Drives the real heartbeat path against a real
    /// manager that wants nothing on this node.
    #[tokio::test]
    async fn an_empty_desired_set_stops_the_last_allocation() -> R<()> {
        let temp_dir = TempDir::new()?;
        let node_id = "w1-node";
        let (client, server) = serve_empty_manager(&temp_dir, node_id).await?;
        let task_manager = Arc::new(Mutex::new(WorkerTaskManager::with_state_store(
            WorkerStateStore::new(temp_dir.path().join("worker_state.json")),
        )));
        {
            let mut manager = task_manager.lock().expect("task manager");
            manager.load_builtin_plugins();
            let allocation = crate::structs::Allocation {
                id: 41,
                status: "running".into(),
                resources: json!({}),
                node_id: node_id.into(),
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
            manager.run_allocation(&allocation, &task)?;
        }
        let credentials = Arc::new(WorkerCredentials::default());
        let identity_manager = Arc::new(WorkerIdentityManager::new(Arc::new(ManagerTokenIssuer {
            client: client.clone(),
            node_id: node_id.into(),
            credentials: credentials.clone(),
        })));

        let identity_dir = tempfile::tempdir()?;
        let node_identity =
            crate::worker::plane::NodeIdentity::load(identity_dir.path(), node_id, None, None)?;
        heartbeat_tick(
            node_id,
            "http://127.0.0.1:1",
            &node_identity,
            &client,
            &task_manager,
            &identity_manager,
            &credentials,
            &Arc::new(Netd::new(
                Arc::new(UpstreamForwarder::new()),
                std::env::temp_dir().join("legion-test-allocs"),
            )),
            None,
            None,
        )
        .await?;

        let mut manager = task_manager.lock().expect("task manager");
        assert!(
            !manager.has_allocation(41),
            "an empty desired set must stop the node's last allocation"
        );
        // Finish the stop so no `sleep` outlives the test.
        while manager.has_terminating() || manager.has_draining() {
            manager.reap_draining();
            manager.reap_terminating();
            std::thread::sleep(Duration::from_millis(20));
        }
        server.abort();
        Ok(())
    }

    /// Regression (audit W6): a worker that cannot reach its manager at boot
    /// still fences restored stateful tasks, measured from the contact time
    /// the previous process persisted rather than from "now".
    #[tokio::test]
    async fn boot_fence_kills_restored_tasks_past_their_window() -> R<()> {
        let temp_dir = TempDir::new()?;
        let store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut previous = WorkerTaskManager::with_state_store(store.clone());
        previous.load_builtin_plugins();
        let allocation = crate::structs::Allocation {
            id: 61,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "db".into(),
            instance_id: None,
            network: None,
        };
        let task = crate::structs::TaskConfig {
            config: json!({"command": ["sleep", "30"]}),
            ..crate::structs::TaskConfig::new("db", "exec")
        };
        previous.run_allocation(&allocation, &task)?;
        previous.set_self_fence_window(61, Duration::from_secs(60));
        let pid = previous.allocation_pid(61).expect("pid");
        // The previous process last heard from its manager two minutes ago.
        store.record_manager_contact(std::time::SystemTime::now() - Duration::from_secs(120))?;

        let fence = tokio::spawn(run_boot_fence(store));
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            // Reap the zombie: this test process is still the task's parent.
            previous.poll_task_exits();
            if !crate::worker::state::pid_is_running(pid) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "boot fence never killed the expired task"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        fence.abort();
        Ok(())
    }

    /// With no persisted contact there is nothing to measure silence from,
    /// so the boot fence leaves everything alone and exits.
    #[tokio::test]
    async fn boot_fence_without_a_contact_record_does_nothing() -> R<()> {
        let temp_dir = TempDir::new()?;
        let store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        tokio::time::timeout(Duration::from_secs(1), run_boot_fence(store)).await?;
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
    #[test]
    fn served_interval_is_a_floor_over_the_worker_type_cadence_test() {
        let secs = Duration::from_secs;
        // A 2s worker type follows the manager up the ladder...
        assert_eq!(
            adopt_served_interval(secs(2), secs(2), Some(21)),
            Some(secs(21))
        );
        // ...and an unchanged serve is not a re-phase.
        assert_eq!(adopt_served_interval(secs(21), secs(2), Some(21)), None);
        // The 2s floor a small fleet is served changes nothing for it.
        assert_eq!(adopt_served_interval(secs(2), secs(2), Some(2)), None);
        // A 60s batch pool keeps its own cadence until the served interval
        // passes it, rather than being sped up to the floor.
        assert_eq!(adopt_served_interval(secs(60), secs(60), Some(2)), None);
        assert_eq!(
            adopt_served_interval(secs(60), secs(60), Some(89)),
            Some(secs(89))
        );
        // A worker type faster than the floor is slowed to it.
        assert_eq!(
            adopt_served_interval(secs(1), secs(1), Some(2)),
            Some(secs(2))
        );
        // No served interval (adaptive off, or an older manager) means "use
        // your own": an adopted interval reverts without a restart.
        assert_eq!(
            adopt_served_interval(secs(21), secs(2), None),
            Some(secs(2))
        );
        assert_eq!(adopt_served_interval(secs(2), secs(2), None), None);
        // Clamped to the legal worker range like any other cadence.
        assert_eq!(
            adopt_served_interval(secs(2), secs(2), Some(10_000)),
            Some(secs(u64::from(MAX_WORKER_HEARTBEAT_SECONDS)))
        );
    }
}
