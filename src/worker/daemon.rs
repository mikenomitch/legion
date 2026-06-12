use crate::prelude::*;

use crate::structs::{
    manager_client::ManagerClient, worker_server::WorkerServer, HealthCheckRequest,
    HeartbeatRequest, JoinTokenClaims, WorkerPreRegisterReply, WorkerPreRegisterRequest,
    WorkerRegisterRequest,
};
use crate::util::random_string;
use crate::worker::{
    artifact_credentials::ManagerCredentialSource,
    node_check::{fingerprint_node, run_health_checks},
    server::WorkerDaemonService,
    state::WorkerStateStore,
    task_health::spawn_health_monitor,
    task_manager::{launch_task_allocation, WorkerTaskManager},
};

use futures::join;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tonic::{transport::Channel, transport::Server, Response};

#[instrument(err, skip_all)]
pub async fn run_worker(token: &str, name_override: Option<&str>) -> R<()> {
    let mut val = Validation::new(Algorithm::HS512);
    val.validate_exp = false;

    let token_data =
        decode::<JoinTokenClaims>(token, &DecodingKey::from_secret(&TMP_SECRET_KEY[..]), &val)?;

    // Reusing the validation code from above just to
    // check for expiry. Not beautiful but it works.
    if !token_data.claims.skip_expiry_validation {
        info!("Validating expiry on join token");
        val.validate_exp = true;
        decode::<JoinTokenClaims>(token, &DecodingKey::from_secret(&TMP_SECRET_KEY[..]), &val)?;
    } else {
        info!("Skipping expiry validation check on join token");
    }

    let manager_addr = token_data.claims.address;
    let worker_type = token_data.claims.worker_type;
    run_worker_with_manager(manager_addr, worker_type, name_override).await
}

#[instrument(err, skip_all)]
pub async fn run_worker_local(
    manager_addr: String,
    worker_type: String,
    name_override: Option<&str>,
) -> R<()> {
    run_worker_with_manager(manager_addr, worker_type, name_override).await
}

#[instrument(err, skip_all)]
async fn run_worker_with_manager(
    manager_addr: String,
    worker_type: String,
    name_override: Option<&str>,
) -> R<()> {
    let addr = format!("{DEF_MANAGER_HOSTNAME}:{DEF_WORKER_SERVER_PORT}").parse()?;

    info!(%manager_addr, %worker_type, "Worker connecting to manager");

    let client: ManagerClient<Channel> = connect_manager_with_retry(&manager_addr).await?;

    // TODO: Establish TLS connection properly
    let pre_register_res = pre_register_self(client.clone(), worker_type).await?;
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
    let restored_allocations = {
        let mut manager = task_manager.lock().expect("task manager");
        manager.load_builtin_plugins();
        manager.configure_artifacts(
            artifact_fetch_config,
            Box::new(ManagerCredentialSource::new(
                manager_addr.clone(),
                name.clone(),
            )),
        );
        manager.restore_allocations().unwrap_or_else(|err| {
            warn!(error = ?err, "Failed to restore persisted allocations");
            Vec::new()
        })
    };
    for (allocation, task) in restored_allocations {
        spawn_health_monitor(task_manager.clone(), allocation, task);
    }
    let worker_node = WorkerDaemonService {
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

    register_self(
        client.clone(),
        name.clone(),
        config.pool,
        config_attrs,
        fingerprint_data,
        health_checks,
    )
    .await?;
    info!(worker_name = %name, "Worker registered with manager");

    let heartbeat_fut = start_heartbeats(name.clone(), client.clone(), task_manager.clone());
    let checks_fut = start_node_checks(
        name.clone(),
        config.additional_node_checks.clone(),
        client,
        task_manager.clone(),
    );
    let log_maintenance_fut = start_log_maintenance(task_manager.clone());
    let _res = join!(
        heartbeat_fut,
        checks_fut,
        log_maintenance_fut,
        worker_server_fut
    );

    Ok(())
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

#[instrument(err, skip_all)]
async fn start_heartbeats(
    name: String,
    client: ManagerClient<Channel>,
    task_manager: Arc<Mutex<WorkerTaskManager>>,
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

        let req = tonic::Request::new(HeartbeatRequest {
            id: name.clone(),
            allocation_ids: allocations,
            failed_allocations: failures,
            allocation_health,
        });

        let response = client_clone.heartbeat(req).await?;
        debug!(worker = %name, elapsed_ms = start.elapsed().as_millis() as u64, "Heartbeat acknowledged by manager");
        let reply = response.into_inner();
        if !reply.desired_allocations.is_empty() {
            let new_allocations = task_manager
                .lock()
                .expect("task manager")
                .reconcile_allocations(&reply.desired_allocations);
            if !new_allocations.is_empty() {
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

        tokio::time::sleep(tokio::time::Duration::from_secs(
            WORKER_HEARTBEAT_DURATION_SEC,
        ))
        .await;
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
        let req = tonic::Request::new(HealthCheckRequest {
            id: name.clone(),
            health_checks: serialized_health_check_data.clone(),
        });

        let response = client_clone.health_check(req).await?;
        info!("RESPONSE={:?}", response);

        tokio::time::sleep(tokio::time::Duration::from_secs(
            WORKER_NODE_CHECK_DURATION_SEC,
        ))
        .await;
    }
}

#[instrument(err, skip_all)]
async fn register_self(
    client: ManagerClient<Channel>,
    name: String,
    pool: String,
    config_attrs: Map<String, Value>,
    fingerprint_attrs: Map<String, Value>,
    health_checks: Map<String, Value>,
) -> R<()> {
    let mut c2 = client.clone();
    let serialized_config_data = serde_json::to_string(&config_attrs)?;
    let serialized_fp_data = serde_json::to_string(&fingerprint_attrs)?;
    let serialized_health_check_data = serde_json::to_string(&health_checks)?;

    let req = tonic::Request::new(WorkerRegisterRequest {
        name,
        config_attrs: serialized_config_data,
        fingerprint_attrs: serialized_fp_data,
        health_checks: serialized_health_check_data,
        pool,
    });

    let response = c2.worker_register(req).await?;
    info!("RESPONSE={:?}", response);

    Ok(())
}

#[instrument(err, skip_all)]
async fn pre_register_self(
    client: ManagerClient<Channel>,
    worker_type: String,
) -> R<Response<WorkerPreRegisterReply>> {
    let mut c2 = client.clone();

    let req = tonic::Request::new(WorkerPreRegisterRequest { worker_type });

    let response = c2.worker_pre_register(req).await?;

    Ok(response)
}
