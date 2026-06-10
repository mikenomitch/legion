use crate::prelude::*;

use crate::manager::join_server::get_insecure_manager_service;
use crate::manager::oidc_http::OidcHttpLayer;
use crate::manager::server::ManagerAgentService;
use crate::manager::workload_identity::WorkloadIdentityState;
use crate::storage::persistence::PersistenceRole;
use crate::storage::store::Store;
use crate::structs::{
    manager_server::ManagerServer, Eval, JobConfig, KvValueType, ManagerConfig, Node, NodeMsg,
    Submission, TaskConfig, WorkerConfig as WorkerConfigMsg, WorkerConfigAlt,
};
use crate::worker::agent::run_worker_local;

use futures::join;
use http::Method;
use jsonnet::JsonnetVm;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic_web::GrpcWebLayer;
use tower_http::cors::{Any, CorsLayer};

// Fallback poll interval used only when delay calculation fails.
const IDLE_POLL_INTERVAL: Duration = Duration::from_secs(30);
// Safety cap for empty queues so idle workers can still periodically re-check state.
const IDLE_WAIT_CAP: Duration = Duration::from_secs(300);
// Exponential backoff is reserved for true error paths (claim failures).
const ERROR_BACKOFF_MIN: Duration = Duration::from_secs(1);
const ERROR_BACKOFF_MAX: Duration = Duration::from_secs(30);

#[instrument(err, skip_all)]
pub async fn run_manager(config: ManagerConfig, run_worker: bool) -> R<()> {
    info!("Starting manager node");

    let full_config = config.with_defaults();
    let manager_node_id = format!("{}:{}", full_config.server.host, full_config.server.port);
    let store = Store::start_storage(&full_config)?;
    if let Err(err) = store.prune_rollouts() {
        warn!(error = ?err, "Failed pruning retained rollout history");
    }
    load_initial_data(&store, &config).await?;
    seed_ui_test_data(&store)?;
    store.upsert_manager_config(&manager_node_id, &full_config)?;

    // Spawn a separate grpc server for non-TLS calls
    let join_server_addr = format!(
        "{}:{}",
        full_config.server.host, full_config.server.insecure_port
    )
    .parse()?;

    let insecure_manager_service = get_insecure_manager_service();
    let join_server_fut = Server::builder()
        .add_service(insecure_manager_service)
        .serve(join_server_addr);

    let store_copy_a = store.clone();

    let store_copy_b = store.clone();

    let store_for_rotation = store.clone();

    // Workload identity (OIDC issuer): only constructed when enabled. The
    // signing keys are restored from (or first-boot persisted to) the store,
    // so tokens held by third parties stay verifiable across restarts.
    let workload_identity_state = if full_config.workload_identity.enabled {
        let default_issuer = format!(
            "http://{}:{}",
            resolve_manager_host(&full_config.server.host),
            full_config.server.port
        );
        let state = WorkloadIdentityState::initialize(
            &store,
            &full_config.workload_identity,
            &default_issuer,
            std::time::SystemTime::now(),
        )?;
        info!(
            issuer = %state.issuer_url()?,
            jwks_path = %state.jwks_path(),
            "Workload identity enabled; serving OIDC discovery and JWKS"
        );
        Some(Arc::new(state))
    } else {
        None
    };

    let manager_node = ManagerAgentService::new(store)
        .with_workload_identity(workload_identity_state.clone())
        .with_kv_debounce(Duration::from_millis(full_config.kv_debounce_ms));

    let addr = format!("{}:{}", full_config.server.host, full_config.server.port).parse()?;

    let tls_identity = load_server_tls_identity()?;
    let server_builder = match tls_identity {
        Some(identity) => {
            info!("Starting manager with TLS enabled");
            Server::builder().tls_config(ServerTlsConfig::new().identity(identity))?
        }
        None => {
            info!("Starting manager without TLS");
            Server::builder()
        }
    };

    let manager_service = ManagerServer::new(manager_node);
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any)
        .expose_headers([
            http::header::HeaderName::from_static("grpc-status"),
            http::header::HeaderName::from_static("grpc-message"),
            http::header::HeaderName::from_static("grpc-encoding"),
        ]);

    // The OIDC layer sits outermost so generic HTTP clients (AWS/GCP/Vault)
    // can GET the discovery document and JWKS on this listener; with workload
    // identity disabled it is a pure passthrough and mounts no routes.
    let start_server_fut = server_builder
        .accept_http1(true)
        .layer(OidcHttpLayer::new(workload_identity_state.clone()))
        .layer(cors)
        .layer(GrpcWebLayer::new())
        .add_service(manager_service)
        .serve(addr);

    println!("Join server starting on {join_server_addr}");
    println!("Manager node starting on {addr}");

    let node_checker_fut =
        start_node_status_checker(store_copy_a, full_config.worker_check_seconds);
    let eval_loop_fut = start_eval_loop(store_copy_b, full_config.eval_threads);
    let manager_host = resolve_manager_host(&full_config.server.host);
    let manager_addr = format!("http://{}:{}", manager_host, full_config.server.port);
    let worker_fut = async move {
        if run_worker {
            info!(%manager_addr, "Starting in-process worker");
            run_worker_local(manager_addr, "default".to_string(), None).await?;
        }
        Ok::<(), anyhow::Error>(())
    };
    let rotation_fut = async move {
        if let Some(state) = workload_identity_state {
            start_workload_identity_rotation(store_for_rotation, state).await?;
        }
        Ok::<(), anyhow::Error>(())
    };
    let _res = join!(
        join_server_fut,
        start_server_fut,
        node_checker_fut,
        eval_loop_fut,
        worker_fut,
        rotation_fut
    );

    Ok(())
}

/// Background signing-key rotation for the workload-identity issuer: rotate
/// the active key past `rotationAge`, expire retiring keys past their overlap
/// window. Every transition is persisted, so a restart mid-overlap restores
/// the exact same key set.
///
/// HA note: in a multi-manager deployment only the primary acts (same
/// leader-only convention as other store-mutating loops); standbys share the
/// replicated `signing_key` table and keep serving an identical JWKS.
#[instrument(err, skip_all)]
async fn start_workload_identity_rotation(
    store: Store,
    state: Arc<WorkloadIdentityState>,
) -> R<()> {
    let rotation_age = state.config().parsed_rotation_age();
    // Check often enough to rotate promptly, but never more than once a minute.
    let interval = Duration::from_secs((rotation_age.as_secs() / 10).max(60));
    info!(
        rotation_age_secs = rotation_age.as_secs(),
        check_interval_secs = interval.as_secs(),
        "Beginning workload identity key rotation loop"
    );
    loop {
        if store.persistence_role() == PersistenceRole::Primary {
            match state.rotation_tick(&store, std::time::SystemTime::now()) {
                Ok(true) => info!("Workload identity signing key rotated"),
                Ok(false) => debug!("Workload identity rotation not due"),
                Err(err) => warn!(error = ?err, "Workload identity rotation tick failed"),
            }
        } else {
            debug!("Skipping workload identity rotation on non-primary manager");
        }
        tokio::time::sleep(interval).await;
    }
}

fn should_seed_ui_test_data() -> bool {
    std::env::var("LEGION_SEED_UI_DATA")
        .ok()
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

#[instrument(err, skip_all)]
fn seed_ui_test_data(store: &Store) -> R<()> {
    if !should_seed_ui_test_data() {
        return Ok(());
    }

    if !store.get_nodes()?.is_empty() || !store.get_jobs()?.is_empty() {
        info!("Skipping UI seed data because the store already has nodes or jobs");
        return Ok(());
    }

    info!("Seeding manager with UI preview data");
    let now = OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339)?;
    store.insert_node(NodeMsg {
        id: "demo-worker-a".into(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: json!({"region": "us-east-1", "instanceType": "c7g.large"}).to_string(),
        fingerprint_attrs: json!({"os": "linux", "arch": "arm64"}).to_string(),
        dynamic_attrs: json!({"drain": false}).to_string(),
        health_checks: json!([
            {
                "name": "Heartbeat",
                "status": "passing",
                "output": "worker heartbeat healthy"
            }
        ])
        .to_string(),
        last_heartbeat: now.clone(),
    })?;
    store.insert_node(NodeMsg {
        id: "demo-worker-b".into(),
        pool: "batch".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: json!({"region": "us-west-2", "instanceType": "m7i.large"}).to_string(),
        fingerprint_attrs: json!({"os": "linux", "arch": "amd64"}).to_string(),
        dynamic_attrs: json!({"drain": false}).to_string(),
        health_checks: json!([
            {
                "name": "Disk",
                "status": "passing",
                "output": "83% free"
            }
        ])
        .to_string(),
        last_heartbeat: now,
    })?;

    let job = JobConfig {
        name: "ui-preview-demo".into(),
        tasks: vec![
            TaskConfig::new("api", "noop"),
            TaskConfig::new("worker", "noop"),
        ],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    };

    let submit_outcome = store.handle_job_submit(
        &job,
        &[Submission {
            path: "seed://ui-preview".into(),
            content: "seeded ui preview job".into(),
            is_primary: true,
        }],
    )?;
    store.enqueue_eval(
        "job_submit",
        Some(&job.name),
        Some(submit_outcome.job_config_id()),
        None,
        None,
    )?;

    let _ = store.put_kv(
        "preview/config/theme",
        "dark",
        KvValueType::String,
        None,
        None,
    )?;
    let _ = store.put_kv(
        "preview/flags",
        &json!({"apiExplorer": true, "seededAt": OffsetDateTime::now_utc().unix_timestamp()})
            .to_string(),
        KvValueType::Json,
        None,
        None,
    )?;

    Ok(())
}

fn resolve_manager_host(host: &str) -> String {
    match host {
        "0.0.0.0" | "::" => "127.0.0.1".to_string(),
        _ => host.to_string(),
    }
}

fn load_server_tls_identity() -> R<Option<Identity>> {
    let tls_dir = std::env::var("LEGION_TLS_SERVER_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("configs/tls/server"));

    let cert_path = tls_dir.join("server.pem");
    let key_path = tls_dir.join("server.key");

    if !(cert_path.exists() && key_path.exists()) {
        info!(
            ?tls_dir,
            "TLS credentials not found; proceeding without TLS"
        );
        return Ok(None);
    }

    // Temporarily don't do any TLS
    Ok(None)

    // let cert = std::fs::read_to_string(cert_path)?;
    // let key = std::fs::read_to_string(key_path)?;

    // Ok(Some(Identity::from_pem(cert, key)))
}

#[instrument(err, skip_all)]
async fn start_node_status_checker(store: Store, check_seconds: u64) -> R<()> {
    info!("Beginning node status checker loop");
    loop {
        let node_ids_to_update = store
            .get_node_ids_and_heartbeats()?
            .into_iter()
            .filter(
                |(id, heartbeat)| match Node::last_heartbeat_too_slow(heartbeat) {
                    Ok(too_slow) => {
                        if too_slow {
                            warn!("Node {} heartbeat too slow - marking as lost", id);
                        }
                        too_slow
                    }
                    Err(e) => {
                        warn!(
                            "Error checking heartbeat - marking node {} lost: {:?}",
                            id, e
                        );
                        true
                    }
                },
            )
            .map(|(id, _heartbeat)| id)
            .collect::<Vec<String>>();

        store.mark_nodes_as_lost(&node_ids_to_update)?;
        // Stateful allocations on lost nodes are gated behind their
        // onLost.replaceAfter loss-confirmation window (see
        // docs/stateful-workloads.md "Node loss and fencing"). Re-run the
        // reschedule path for still-lost nodes every tick so replacement
        // proceeds automatically once a window elapses — no operator action
        // needed.
        store.recheck_lost_nodes();
        if let Err(err) = store.reconcile_incomplete_rollouts() {
            warn!(error = ?err, "Failed reconciling completed_incomplete rollouts");
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(check_seconds)).await;
    }
}

#[instrument(err, skip_all)]
async fn start_eval_loop(store: Store, max_threads: usize) -> R<()> {
    let max_threads = max_threads.max(1);
    info!(max_threads, "Beginning eval loop");

    let requeued = store.requeue_in_flight_evals()?;
    if requeued > 0 {
        warn!(requeued, "Recovered stale in-flight evals at startup");
    }

    let mut join_set = JoinSet::new();
    for worker_id in 0..max_threads {
        spawn_eval_worker(&mut join_set, &store, worker_id);
    }

    let mut next_worker_id = max_threads;
    // Supervises workers forever: any worker exit/crash is immediately replaced.
    loop {
        if !restart_eval_worker_after_exit(&mut join_set, &store, &mut next_worker_id).await {
            break;
        }
    }

    Ok(())
}

async fn restart_eval_worker_after_exit(
    join_set: &mut JoinSet<()>,
    store: &Store,
    next_worker_id: &mut usize,
) -> bool {
    match join_set.join_next().await {
        Some(Ok(())) => {
            warn!("Eval worker exited unexpectedly; restarting");
        }
        Some(Err(e)) => {
            error!("Eval worker crashed; restarting: {:?}", e);
        }
        None => {
            error!("Eval worker join set drained unexpectedly");
            return false;
        }
    }

    let worker_id = *next_worker_id;
    *next_worker_id = next_worker_id.saturating_add(1);
    info!(worker_id, "Spawning replacement eval worker");
    spawn_eval_worker(join_set, store, worker_id);
    true
}

fn spawn_eval_worker(join_set: &mut JoinSet<()>, store: &Store, worker_id: usize) {
    // Each worker shares the same notifier. enqueue/requeue paths wake sleepers.
    let store_for_eval = store.clone();
    let notifier = store.eval_notifier();

    join_set.spawn(async move { eval_worker_loop(store_for_eval, notifier, worker_id).await });
}

async fn eval_worker_loop(store: Store, notifier: Arc<Notify>, worker_id: usize) {
    info!(worker_id, "Eval worker started");

    let mut error_backoff = ERROR_BACKOFF_MIN;
    loop {
        let claim_started = std::time::Instant::now();
        match store.claim_eval() {
            Ok(Some(eval)) => {
                error_backoff = ERROR_BACKOFF_MIN;
                info!(
                    worker_id,
                    eval_id = %eval.id,
                    kind = %eval.kind,
                    claim_eval_ms = claim_started.elapsed().as_millis() as u64,
                    "Claimed eval for dispatch"
                );
                handle_claimed_eval(&store, worker_id, &eval);
            }
            Ok(None) => {
                debug!(
                    worker_id,
                    claim_eval_ms = claim_started.elapsed().as_millis() as u64,
                    "No eval claimed; worker entering wait path"
                );
                // Queue is empty (or only delayed work exists), so park until notified or due.
                wait_for_next_eval(&store, &notifier, worker_id).await;
            }
            Err(e) => {
                error!(
                    worker_id,
                    claim_eval_ms = claim_started.elapsed().as_millis() as u64,
                    "Error claiming next eval: {:?}",
                    e
                );
                tokio::time::sleep(error_backoff).await;
                error_backoff = std::cmp::min(error_backoff * 2, ERROR_BACKOFF_MAX);
            }
        }
    }
}

async fn wait_for_next_eval(store: &Store, notifier: &Notify, worker_id: usize) {
    // Compute the shortest meaningful sleep: until next delayed eval is ready,
    // or a long idle cap when the queue has no pending rows.
    let wait_for = match store.next_pending_eval_delay() {
        Ok(Some(delay)) => delay,
        Ok(None) => IDLE_WAIT_CAP,
        Err(e) => {
            warn!(
                worker_id,
                "Error checking pending evals before waiting: {:?}", e
            );
            IDLE_POLL_INTERVAL
        }
    };

    if wait_for.is_zero() {
        return;
    }

    info!(
        worker_id,
        ?wait_for,
        "No ready evals; waiting for notify or timeout"
    );
    // Race wake events against timeout so workers react quickly to new work
    // without busy polling when the queue is empty.
    tokio::select! {
        _ = notifier.notified() => {}
        _ = tokio::time::sleep(wait_for) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::ManagerConfigFull;
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-agent-store-")
            .tempdir()
            .context("Error creating temp store directory")?
            .keep();

        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("Error parsing store path")?
                .to_string(),
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    #[tokio::test]
    async fn wait_for_next_eval_wakes_immediately_after_enqueue() -> R<()> {
        let store = setup_store()?;
        let notifier = store.eval_notifier();

        let wait_task = tokio::spawn({
            let store = store.clone();
            let notifier = notifier.clone();
            async move { wait_for_next_eval(&store, &notifier, 0).await }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        store.enqueue_eval("node-left", None, None, None, Some(1))?;

        tokio::time::timeout(Duration::from_millis(300), wait_task)
            .await
            .context("worker was not notified on enqueue")??;

        Ok(())
    }

    #[tokio::test]
    async fn wait_for_next_eval_does_not_busy_loop_when_only_future_work_exists() -> R<()> {
        let store = setup_store()?;
        let notifier = store.eval_notifier();
        store.enqueue_eval_after_delay(
            "node-left",
            None,
            None,
            None,
            Some(1),
            Duration::from_millis(80),
        )?;

        let start = tokio::time::Instant::now();
        wait_for_next_eval(&store, &notifier, 0).await;
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(50),
            "wait elapsed too quickly ({elapsed:?}), likely busy-looping"
        );

        Ok(())
    }

    #[tokio::test]
    async fn restart_eval_worker_after_exit_spawns_replacement_worker() -> R<()> {
        let store = setup_store()?;
        let mut join_set = JoinSet::new();
        let mut next_worker_id = 1;

        join_set.spawn(async {});
        let restarted =
            restart_eval_worker_after_exit(&mut join_set, &store, &mut next_worker_id).await;

        assert!(restarted);
        assert_eq!(next_worker_id, 2);
        assert_eq!(join_set.len(), 1);

        join_set.abort_all();
        while join_set.join_next().await.is_some() {}

        Ok(())
    }

    #[test]
    fn should_seed_ui_test_data_parses_truthy_values() {
        std::env::set_var("LEGION_SEED_UI_DATA", "true");
        assert!(should_seed_ui_test_data());

        std::env::set_var("LEGION_SEED_UI_DATA", "1");
        assert!(should_seed_ui_test_data());

        std::env::set_var("LEGION_SEED_UI_DATA", "on");
        assert!(should_seed_ui_test_data());

        std::env::set_var("LEGION_SEED_UI_DATA", "false");
        assert!(!should_seed_ui_test_data());

        std::env::remove_var("LEGION_SEED_UI_DATA");
        assert!(!should_seed_ui_test_data());
    }

    #[test]
    fn seed_ui_test_data_inserts_preview_records_when_store_is_empty() -> R<()> {
        let store = setup_store()?;

        std::env::set_var("LEGION_SEED_UI_DATA", "true");
        seed_ui_test_data(&store)?;
        std::env::remove_var("LEGION_SEED_UI_DATA");

        assert!(!store.get_nodes()?.is_empty());
        assert!(!store.get_jobs()?.is_empty());
        assert!(store.get_kv("preview/config/theme")?.is_some());

        Ok(())
    }
}

fn handle_claimed_eval(store: &Store, worker_id: usize, eval: &Eval) {
    let started = std::time::Instant::now();
    info!(
        worker_id,
        eval_id = %eval.id,
        kind = %eval.kind,
        "Dispatching claimed eval"
    );
    match eval.handle(store) {
        Ok(message) => {
            info!(
                worker_id,
                eval_id = %eval.id,
                kind = %eval.kind,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Eval {} handled successfully: {}", eval.id, message
            );
            if let Err(e) = store.complete_eval(&eval.id, true) {
                error!(worker_id, "Error completing eval {}: {:?}", eval.id, e);
            }
        }
        Err(e) => {
            error!(worker_id, eval_id = %eval.id, elapsed_ms = started.elapsed().as_millis() as u64, "Error handling eval {}: {:?}", eval.id, e);
            if let Err(complete_err) = store.complete_eval(&eval.id, false) {
                error!(
                    worker_id,
                    "Error marking eval {} as failed after handling error: {:?}",
                    eval.id,
                    complete_err
                );
            }
        }
    }
}

#[instrument(err, skip_all)]
fn parse_worker_config(config_path: &str) -> R<WorkerConfigAlt> {
    // Pluginify jsonnet
    let mut vm = JsonnetVm::new();
    let rendered = vm
        .evaluate_file(config_path)
        .map_err(|e| {
            let err = anyhow!("Error parsing worker config at path {config_path}: {e}");
            error!("{err}");
            err
        })?
        .to_string();

    let config: WorkerConfigAlt = serde_json::from_str(&rendered)
        .with_context(|| format!("Error parsing worker config at path {config_path}"))?;

    Ok(config)
}

#[instrument(err, skip_all)]
async fn load_initial_data(store: &Store, config: &ManagerConfig) -> R<()> {
    let worker_conf_path = config.with_defaults().worker_config_path.clone();
    let sub_paths = fs::read_dir(worker_conf_path)?;

    for path in sub_paths {
        let path = path.unwrap().path();
        let path_str = path.to_str().unwrap();
        let wcp = parse_worker_config(path_str)?;

        let af = wcp.additional_node_checks.unwrap_or([].to_vec());
        let attrs = wcp.attributes.unwrap_or(HashMap::new());
        let resources = wcp
            .resources
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?
            .unwrap_or_else(|| "{}".to_string());
        let reserved_resources = wcp
            .reserved_resources
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?
            .unwrap_or_else(|| "{}".to_string());

        let wc = WorkerConfigMsg {
            name: wcp.worker_type.clone(),
            pool: wcp.pool.unwrap_or("default".into()),
            additional_node_checks: af,
            attributes: attrs,
            name_prefix: wcp.name_prefix,
            resources,
            reserved_resources,
            max_allocations: wcp.max_allocations,
        };

        store.insert_worker_type(&wc)?;
    }

    Ok(())
}
