use crate::prelude::*;

use crate::structs::token::JoinToken;
use crate::structs::{
    manager_client::ManagerClient, worker_server::WorkerServer, HealthCheckRequest,
    HeartbeatRequest, WorkerPreRegisterReply, WorkerPreRegisterRequest, WorkerRegisterRequest,
};
use crate::util::random_string;
use crate::worker::{
    node_check::{fingerprint_node, run_health_checks},
    server::WorkerAgentService,
    state::WorkerStateStore,
    task_health::spawn_health_monitor,
    task_manager::WorkerTaskManager,
};

use futures::join;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::Duration;
use tonic::{transport::Channel, transport::Server, Response};

/// The worker's manager-issued credentials: the join secret it was started
/// with (used to register and re-register) and the node token returned by
/// registration (attached to heartbeats/health checks). Empty when the
/// cluster runs without ACLs.
#[derive(Debug, Default)]
struct WorkerCredentials {
    join_secret: Option<String>,
    node_token: RwLock<Option<String>>,
}

impl WorkerCredentials {
    fn attach<T>(&self, mut request: tonic::Request<T>) -> tonic::Request<T> {
        if let Some(token) = self.node_token.read().ok().and_then(|t| t.clone()) {
            if let Ok(value) = format!("Bearer {token}").parse() {
                request.metadata_mut().insert("authorization", value);
            }
        }
        request
    }

    fn store_node_token(&self, token: Option<String>) {
        if let Ok(mut current) = self.node_token.write() {
            *current = token;
        }
    }
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

#[instrument(err, skip_all)]
async fn run_worker_with_manager(
    manager_addr: String,
    worker_type: String,
    name_override: Option<&str>,
    join_secret: Option<String>,
) -> R<()> {
    let addr = format!("{DEF_MANAGER_HOSTNAME}:{DEF_WORKER_SERVER_PORT}").parse()?;

    info!(%manager_addr, %worker_type, "Worker connecting to manager");

    let client: ManagerClient<Channel> = connect_manager_with_retry(&manager_addr).await?;
    let credentials = Arc::new(WorkerCredentials {
        join_secret,
        node_token: RwLock::new(None),
    });

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
    let restored_allocations = {
        let mut manager = task_manager.lock().expect("task manager");
        manager.load_builtin_plugins();
        manager.restore_allocations().unwrap_or_else(|err| {
            warn!(error = ?err, "Failed to restore persisted allocations");
            Vec::new()
        })
    };
    for (allocation, task) in restored_allocations {
        spawn_health_monitor(task_manager.clone(), allocation, task);
    }
    let worker_node = WorkerAgentService {
        task_manager: task_manager.clone(),
    };
    let worker_server_fut = Server::builder()
        .add_service(WorkerServer::new(worker_node))
        .serve(addr);

    let fingerprint_data = fingerprint_node(config.additional_node_checks.clone());
    let readiness = task_manager
        .lock()
        .expect("task manager")
        .readiness_checks();
    let health_checks = run_health_checks(config.additional_node_checks.clone(), Some(readiness));

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
        Value::String(format!(
            "http://{}:{}",
            DEF_MANAGER_HOSTNAME, DEF_WORKER_SERVER_PORT
        )),
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

    let heartbeat_fut = start_heartbeats(
        name.clone(),
        client.clone(),
        task_manager.clone(),
        credentials.clone(),
        registration,
    );
    let checks_fut = start_node_checks(
        name.clone(),
        config.additional_node_checks.clone(),
        client,
        task_manager.clone(),
        credentials,
    );
    let _res = join!(heartbeat_fut, checks_fut, worker_server_fut);

    Ok(())
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
async fn connect_manager_with_retry(manager_addr: &str) -> R<ManagerClient<Channel>> {
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

#[instrument(err, skip_all)]
async fn start_heartbeats(
    name: String,
    client: ManagerClient<Channel>,
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    credentials: Arc<WorkerCredentials>,
    registration: RegistrationPayload,
) -> R<()> {
    let mut client_clone = client.clone();

    loop {
        let start = std::time::Instant::now();
        let (allocations, failures, allocation_health) = {
            let mut manager = task_manager.lock().expect("task manager");
            let failures = manager
                .drain_failures()
                .into_iter()
                .map(|failure| crate::structs::FailedAllocation {
                    allocation_id: failure.allocation_id,
                    reason: failure.reason,
                })
                .collect::<Vec<_>>();
            let allocation_health = manager.allocation_health_snapshot();
            (manager.allocations_snapshot(), failures, allocation_health)
        };

        let req = credentials.attach(tonic::Request::new(HeartbeatRequest {
            id: name.clone(),
            allocation_ids: allocations,
            failed_allocations: failures,
            allocation_health,
        }));

        match client_clone.heartbeat(req).await {
            Ok(response) => {
                debug!(worker = %name, elapsed_ms = start.elapsed().as_millis() as u64, "Heartbeat acknowledged by manager");
                let reply = response.into_inner();
                if !reply.desired_allocations.is_empty() {
                    let new_allocations = task_manager
                        .lock()
                        .expect("task manager")
                        .reconcile_allocations(&reply.desired_allocations);
                    for (allocation, task) in new_allocations {
                        spawn_health_monitor(task_manager.clone(), allocation, task);
                    }
                }
            }
            // The node token can be revoked/rotated (for example ACLs were
            // just enabled, or the manager state was reset). Re-register with
            // the join secret to obtain a fresh credential, then carry on.
            Err(status) if status.code() == tonic::Code::Unauthenticated => {
                warn!(worker = %name, "Heartbeat rejected as unauthenticated; re-registering");
                if let Err(err) = register_self(client.clone(), &registration, &credentials).await {
                    warn!(worker = %name, error = ?err, "Re-registration failed; will retry");
                }
            }
            Err(status) => {
                warn!(worker = %name, code = %status.code(), "Heartbeat failed: {}", status.message());
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(
            WORKER_HEARTBEAT_DURATION_SEC,
        ))
        .await;
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

    let req = tonic::Request::new(WorkerRegisterRequest {
        name: registration.name.clone(),
        config_attrs: registration.config_attrs.clone(),
        fingerprint_attrs: registration.fingerprint_attrs.clone(),
        health_checks: registration.health_checks.clone(),
        pool: registration.pool.clone(),
        join_token: credentials.join_secret.clone(),
    });

    let response = c2.worker_register(req).await?;
    let reply = response.into_inner();
    if reply.node_token.is_some() {
        info!(worker = %registration.name, "Received node token from manager");
    }
    credentials.store_node_token(reply.node_token);

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
