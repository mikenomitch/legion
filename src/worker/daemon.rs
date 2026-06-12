use crate::prelude::*;

use crate::structs::{
    manager_client::ManagerClient, worker_server::WorkerServer, HealthCheckRequest,
    HeartbeatRequest, JoinTokenClaims, WorkerPreRegisterReply, WorkerPreRegisterRequest,
    WorkerRegisterRequest,
};
use crate::util::random_string;
use crate::worker::{
    artifact_credentials::ManagerCredentialSource,
    identity::{ManagerTokenIssuer, PreparedIdentity, WorkerIdentityManager},
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
    // Workload-identity delivery/refresh: tokens are requested from the
    // manager under this worker's node id.
    let identity_manager = Arc::new(WorkerIdentityManager::new(Arc::new(ManagerTokenIssuer {
        client: client.clone(),
        node_id: name.clone(),
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
    let worker_node = WorkerDaemonService {
        task_manager: task_manager.clone(),
        identity: Some(identity_manager.clone()),
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

    // Registration just succeeded, so "now" is an honest last-contact time;
    // every successful heartbeat refreshes it. The self-fence watchdog kills
    // stateful allocations whose `selfTerminateAfter` window outlives this
    // clock (see docs/stateful-workloads.md "Node loss and fencing").
    let last_successful_heartbeat = Arc::new(Mutex::new(std::time::Instant::now()));
    let heartbeat_fut = start_heartbeats(
        name.clone(),
        client.clone(),
        task_manager.clone(),
        identity_manager.clone(),
        last_successful_heartbeat.clone(),
    );
    let self_fence_fut = start_self_fence_watchdog(task_manager.clone(), last_successful_heartbeat);
    let checks_fut = start_node_checks(
        name.clone(),
        config.additional_node_checks.clone(),
        client,
        task_manager.clone(),
    );
    let log_maintenance_fut = start_log_maintenance(task_manager.clone());
    let _res = join!(
        heartbeat_fut,
        self_fence_fut,
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

// Failed heartbeats retry forever with exponential backoff in this range. The
// cap stays low (vs the usual 30s) so a reconnecting worker is detected
// promptly — the self-fence clock keeps running regardless.
const HEARTBEAT_RETRY_MIN: Duration = Duration::from_millis(500);
const HEARTBEAT_RETRY_MAX: Duration = Duration::from_secs(10);
// How often the self-fence watchdog re-checks `last_successful_heartbeat`
// against each running allocation's window.
const SELF_FENCE_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Heartbeat loop. A failed heartbeat must NEVER exit the loop (tasks keep
/// running while the manager is unreachable — exiting would orphan them and
/// disable self-fencing); failures are retried forever with backoff while
/// `last_successful_heartbeat` tracks the last time the manager actually
/// acknowledged us. The self-fence watchdog reads that clock.
#[instrument(err, skip_all)]
async fn start_heartbeats(
    name: String,
    client: ManagerClient<Channel>,
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    identity_manager: Arc<WorkerIdentityManager>,
    last_successful_heartbeat: Arc<Mutex<std::time::Instant>>,
) -> R<()> {
    let mut backoff = HEARTBEAT_RETRY_MIN;

    loop {
        match heartbeat_tick(&name, &client, &task_manager, &identity_manager).await {
            Ok(()) => {
                *last_successful_heartbeat
                    .lock()
                    .expect("heartbeat clock lock") = std::time::Instant::now();
                backoff = HEARTBEAT_RETRY_MIN;
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    WORKER_HEARTBEAT_DURATION_SEC,
                ))
                .await;
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
) -> R<()> {
    let mut client_clone = client.clone();

    {
        let start = std::time::Instant::now();
        let (allocations, failures, allocation_health) = {
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
            let allocation_health = manager.allocation_health_snapshot();
            (manager.allocations_snapshot(), failures, allocation_health)
        };
        let running = allocations.clone();
        let failures_for_redrive = failures.clone();

        let req = tonic::Request::new(HeartbeatRequest {
            id: name.to_string(),
            allocation_ids: allocations,
            failed_allocations: failures,
            allocation_health,
        });

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
                return Err(err.into());
            }
        };
        debug!(worker = %name, elapsed_ms = start.elapsed().as_millis() as u64, "Heartbeat acknowledged by manager");
        let reply = response.into_inner();
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

            let new_allocations = task_manager
                .lock()
                .expect("task manager")
                .reconcile_allocations(&desired);
            if !new_allocations.is_empty() {
                // Refresh loops are keyed by allocation id and may start
                // slightly before the task process does (artifact staging);
                // identity_manager.retain below prunes them if the launch
                // fails and the allocation goes away.
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

        // Refresh loops must never outlive their allocation.
        let live = task_manager
            .lock()
            .expect("task manager")
            .allocations_snapshot();
        identity_manager.retain(&live);
    }

    Ok(())
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
        let identity_manager = Arc::new(WorkerIdentityManager::new(Arc::new(ManagerTokenIssuer {
            client: client.clone(),
            node_id: "test-worker".into(),
        })));
        let clock = Arc::new(Mutex::new(std::time::Instant::now()));

        let heartbeats = tokio::spawn(start_heartbeats(
            "test-worker".into(),
            client,
            task_manager,
            identity_manager,
            clock.clone(),
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
}
