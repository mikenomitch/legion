use crate::prelude::*;

use crate::manager::oidc_http::OidcHttpLayer;
use crate::manager::server::ManagerDaemonService;
use crate::manager::workload_identity::WorkloadIdentityState;
use crate::storage::events::drafts;
use crate::storage::persistence::PersistenceRole;
use crate::storage::store::Store;
use crate::structs::{
    manager_server::ManagerServer, Eval, JobConfig, KvValueType, ManagerConfig, Node, NodeMsg,
    Submission, TaskConfig, TlsConfigFull, WorkerConfig as WorkerConfigMsg, WorkerConfigAlt,
};
use crate::worker::daemon::run_worker_local;

use futures::join;
use http::Method;
use jsonnet::JsonnetVm;
use rand::Rng;
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
// How often a raft non-leader re-checks lane leadership before acting
// (eval workers, node status checker). Bounds the takeover delay after a
// leadership change to "a few seconds" (plan JC-15).
const LEADER_POLL_INTERVAL: Duration = Duration::from_secs(1);
// Base delay before re-running an eval whose handler hit a foreign-lane
// NotRaftLeader rejection (divergent lane leadership) — long enough for the
// engine's JC-40 lane-convergence loop (or an election) to move leadership,
// short enough that acknowledged work is not parked for long. Repeated
// rejections of the SAME eval back off exponentially from this base up to
// the cap, with jitter, so a standing split parks evals quietly instead of
// re-proposing and warning every 5s forever.
const EVAL_NOT_LEADER_RETRY_DELAY: Duration = Duration::from_secs(5);
const EVAL_NOT_LEADER_RETRY_MAX_DELAY: Duration = Duration::from_secs(60);
const EVAL_NOT_LEADER_RETRY_JITTER_MS: u64 = 1_000;
// Boot-path startup writes (config load, UI seed, manager-config upsert) are
// composites that can touch two lanes. Under `lanes` topology a node that
// leads the control lane may not (yet) lead the status/scheduler lane, so the
// second half is rejected with a transient NotRaftLeader until the JC-40
// convergence loop finishes acquiring the other lanes — a few seconds at
// most. Retry across that window instead of failing manager startup.
const STARTUP_WRITE_ATTEMPTS: usize = 20;
const STARTUP_WRITE_RETRY_DELAY: Duration = Duration::from_millis(500);

#[instrument(err, skip_all)]
pub async fn run_manager(config: ManagerConfig, run_worker: bool) -> R<()> {
    info!("Starting manager node");

    let full_config = config.with_defaults();
    // Fail fast on malformed artifact/auth/node-identity config: a bad
    // matcher or unknown nodeIdentity reference should stop the manager at
    // startup, not break job admission later.
    crate::structs::workload_identity::validate_node_identities(
        &full_config.workload_identity.node_identities,
    )?;
    let node_identity_names: Vec<String> = full_config
        .workload_identity
        .node_identities
        .iter()
        .map(|identity| identity.name.clone())
        .collect();
    crate::artifacts::auth::validate_cluster_artifacts_config(
        &full_config.artifacts,
        &node_identity_names,
    )?;
    let manager_node_id = format!("{}:{}", full_config.server.host, full_config.server.port);
    let store = Store::start_storage(&full_config)?;
    // Startup-time mutations are leader-only in raft mode (they replicate
    // like any other write; a follower would be rejected with a leader hint
    // anyway) — and leadership is PER LANE: under `lanes` topology a node
    // can lead some lanes and not others, so each startup write gates on
    // the lane it proposes to. Config/seed/manager-config writes are
    // control-lane ops; rollout pruning is a scheduler-lane op (C-2 rollout
    // consolidation). A single-node bootstrap has already elected itself on
    // every lane inside `start_storage`, so the common path runs them
    // unchanged; a follower joining an existing cluster skips them — the
    // lane's leader performed them and this node receives the effects
    // through replication. (`is_raft_leader` is always true for non-raft
    // backends.)
    let is_control_leader = store.is_raft_leader(crate::storage::Lane::Control);
    let is_scheduler_leader = store.is_raft_leader(crate::storage::Lane::Scheduler);
    if is_control_leader {
        // Each of these is retried while it fails with a transient
        // cross-lane leader rejection, and warns (rather than aborting the
        // manager) if leadership never converges — see `startup_write`.
        startup_write("load initial config data", || {
            load_initial_data(&store, &config)
        })
        .await?;
        startup_write("seed UI test data", || async { seed_ui_test_data(&store) }).await?;
        startup_write("upsert manager config", || async {
            store.upsert_manager_config(&manager_node_id, &full_config)
        })
        .await?;
    } else {
        info!("Skipping control-lane startup writes on raft non-leader manager");
    }
    if is_scheduler_leader {
        if let Err(err) = store.prune_rollouts() {
            warn!(error = ?err, "Failed pruning retained rollout history");
        }
    } else {
        info!("Skipping scheduler-lane startup writes (rollout prune) on raft non-leader manager");
    }
    // Raft mode: the roles sampled above are snapshots, not promises. A node
    // that boots as non-leader (or leaderless — a full-cluster restart leaves
    // EVERY member sampling non-leader) can win an election seconds later
    // and would otherwise never run the leader-only startup writes: config
    // jobs/worker-types would not load, rollout history would not prune, the
    // manager config row would go stale. Re-run them on every leadership
    // ACQUISITION — tracked independently per lane, because control and
    // scheduler leadership can move separately under `lanes` topology.
    if store.raft_status().is_some() {
        let watch_store = store.clone();
        let watch_config = config.clone();
        let watch_full_config = full_config.clone();
        let watch_node_id = manager_node_id.clone();
        let mut was_control_leader = is_control_leader;
        let mut was_scheduler_leader = is_scheduler_leader;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(LEADER_POLL_INTERVAL).await;
                let control_leader = watch_store.is_raft_leader(crate::storage::Lane::Control);
                if leadership_acquired(was_control_leader, control_leader) {
                    info!(
                        "Raft control-lane leadership acquired; running leader-only startup writes"
                    );
                    if let Err(err) = load_initial_data(&watch_store, &watch_config).await {
                        warn!(error = ?err, "Failed loading initial config data after leadership acquisition");
                    }
                    if let Err(err) = seed_ui_test_data(&watch_store) {
                        warn!(error = ?err, "Failed seeding UI test data after leadership acquisition");
                    }
                    if let Err(err) =
                        watch_store.upsert_manager_config(&watch_node_id, &watch_full_config)
                    {
                        warn!(error = ?err, "Failed upserting manager config after leadership acquisition");
                    }
                }
                was_control_leader = control_leader;
                let scheduler_leader = watch_store.is_raft_leader(crate::storage::Lane::Scheduler);
                if leadership_acquired(was_scheduler_leader, scheduler_leader) {
                    info!("Raft scheduler-lane leadership acquired; pruning rollout history");
                    if let Err(err) = watch_store.prune_rollouts() {
                        warn!(error = ?err, "Failed pruning retained rollout history");
                    }
                }
                was_scheduler_leader = scheduler_leader;
            }
        });
    }

    let store_copy_a = store.clone();

    let store_copy_b = store.clone();

    // Workload identity (OIDC issuer): only constructed when enabled. The
    // signing keys are restored from (or first-boot persisted to) the store,
    // so tokens held by third parties stay verifiable across restarts. Built
    // before the authenticator so the control-plane identity method can verify
    // `legion` tokens against the same in-process issuer.
    let workload_identity_state = if full_config.workload_identity.enabled {
        // When the issuer URL is unset, fall back to this manager's own
        // listener. Match the scheme to the listener: with server TLS on it
        // speaks only HTTPS, so an http:// issuer would make `iss`/`jwks_uri`
        // point at an endpoint the manager won't answer. Operators should still
        // set an externally-reachable HTTPS issuer for real federation.
        let scheme = if full_config.server.tls.enabled {
            "https"
        } else {
            "http"
        };
        let default_issuer = format!(
            "{scheme}://{}:{}",
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

    let authenticator = {
        let mut authenticator = crate::manager::acl::Authenticator::from_config(&full_config.acl)
            .context("invalid acl configuration")?;
        // Phase 7: when workload identity is on, register the built-in
        // control-plane method so a workload can authenticate back to Legion
        // with its reserved `legion` token, verified against the in-process
        // issuer. Reserves the method name `legion` (a config auth method of
        // that name would collide and fail startup).
        if let Some(state) = workload_identity_state.clone() {
            authenticator
                .register_method(Box::new(
                    crate::manager::auth_method::ControlPlaneIdentityMethod::new(
                        state,
                        full_config.workload_identity.control_plane_bindings.clone(),
                        authenticator.login_ttl,
                    ),
                ))
                .context("failed to register the control-plane identity auth method")?;
        }
        Arc::new(authenticator)
    };
    if authenticator.enabled {
        info!(
            audit_only = authenticator.audit_only,
            auth_methods = ?authenticator.method_names(),
            "ACLs enabled; run `legion acl bootstrap` once to mint the initial management token"
        );
        // Prime remote JWKS documents, then keep them fresh in the background.
        authenticator.refresh_auth_methods().await;
        let refresh_authenticator = authenticator.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(refresh_authenticator.jwks_refresh_interval).await;
                refresh_authenticator.refresh_auth_methods().await;
            }
        });
        // Keep credential tables tidy: expired tokens are already rejected at
        // authentication time, pruning just removes the rows.
        let prune_store = store.clone();
        tokio::spawn(async move {
            loop {
                match prune_store.prune_expired_credentials() {
                    Ok((acl_tokens, join_tokens)) if acl_tokens + join_tokens > 0 => {
                        info!(acl_tokens, join_tokens, "Pruned expired credentials");
                    }
                    Ok(_) => {}
                    Err(err) => warn!(error = ?err, "Failed pruning expired credentials"),
                }
                tokio::time::sleep(Duration::from_secs(60 * 60)).await;
            }
        });
    } else {
        warn!("ACLs are disabled; every API caller has full access (set acl.enabled to true to enforce)");
    }

    // The in-process worker (--run-worker) registers like any external
    // worker; with ACLs enforced the manager self-issues its join token.
    let local_worker_join_secret = if run_worker && authenticator.enabled {
        let (_, composite) = authenticator
            .mint_join_token(
                &store,
                &full_config.server.advertised_address(),
                "default",
                Some(Duration::from_secs(15 * 60)),
                "in-process worker (--run-worker)",
            )
            .map_err(|e| anyhow!("failed to mint local worker join token: {e}"))?;
        Some(crate::structs::token::JoinToken::decode(&composite)?.secret)
    } else {
        None
    };

    let store_for_rotation = store.clone();

    // Clone store + authenticator for the exec WebSocket listener before the
    // service takes ownership of them.
    let exec_ws_state = crate::manager::exec_ws::ExecWsState {
        store: store.clone(),
        auth: authenticator.clone(),
    };

    // Manager-plane raft transport shares this tonic listener. The service
    // is mounted unconditionally around the store's registry cell: in raft
    // mode the engine has already populated it (start_storage); otherwise it
    // stays empty and every raft RPC answers Unavailable.
    let raft_transport_service = store.raft_transport_server();

    // Telemetry listener state (metrics + health probes). Cloned here for the
    // same reason as the exec state: the gRPC service takes the store below.
    let telemetry_state = crate::manager::telemetry::TelemetryState {
        store: store.clone(),
        raft_required: matches!(
            full_config.persistence.r#type,
            crate::structs::config::PersistenceType::Raft
        ),
        diverged_since_ms: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
    };

    let manager_node = ManagerDaemonService::with_manager_config(store, &full_config)
        .with_workload_identity(workload_identity_state.clone())
        .with_kv_debounce(Duration::from_millis(full_config.kv_debounce_ms))
        .with_authenticator(authenticator)
        .with_advertise_addr(full_config.server.advertised_address());

    let addr = format!("{}:{}", full_config.server.host, full_config.server.port).parse()?;

    // Exec WebSocket listener on the next port (browsers cannot do bidi
    // streaming over gRPC-Web; tonic's listener does not do HTTP upgrades).
    // Skipped entirely when exec is disabled.
    if full_config.exec.enabled {
        let exec_ws_addr: std::net::SocketAddr = format!(
            "{}:{}",
            full_config.server.host,
            full_config.server.port + 1
        )
        .parse()?;
        tokio::spawn(async move {
            if let Err(err) =
                crate::manager::exec_ws::serve_exec_ws(exec_ws_addr, exec_ws_state).await
            {
                tracing::error!(error = ?err, "exec ws listener exited");
            }
        });
    } else {
        info!(
            "Exec is disabled (exec.enabled=false); ExecAllocation and the exec WebSocket are off"
        );
    }

    // Telemetry listener on `server.port + 2` by convention (the exec
    // WebSocket holds `+ 1`); `telemetry.port` overrides.
    if full_config.telemetry.enabled {
        let telemetry_port = full_config
            .telemetry
            .port
            .unwrap_or(full_config.server.port + 2);
        let telemetry_addr: std::net::SocketAddr =
            format!("{}:{}", full_config.server.host, telemetry_port).parse()?;
        tokio::spawn(async move {
            if let Err(err) =
                crate::manager::telemetry::serve_telemetry(telemetry_addr, telemetry_state).await
            {
                tracing::error!(error = ?err, "telemetry listener exited");
            }
        });
    } else {
        info!(
            "Telemetry is disabled (telemetry.enabled=false); /metrics and health probes are off"
        );
    }
    // OTLP metrics push (opt-in via the OTEL_* env vars, like trace export;
    // no-op when unconfigured). Independent of the scrape listener above.
    crate::otel::start_metrics_push();

    let tls_identity = load_server_tls_identity(&full_config.server.tls)?;
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
    // The metrics layer sits innermost (after grpc-web translation) so the
    // method label reflects the real gRPC route for browser calls too.
    let start_server_fut = server_builder
        .accept_http1(true)
        .layer(OidcHttpLayer::new(workload_identity_state.clone()))
        .layer(cors)
        .layer(GrpcWebLayer::new())
        .layer(crate::manager::telemetry::GrpcMetricsLayer)
        .add_service(manager_service)
        .add_service(raft_transport_service)
        .serve(addr);

    println!("Manager node starting on {addr}");

    let node_checker_fut =
        start_node_status_checker(store_copy_a, full_config.worker_check_seconds);
    let eval_loop_fut = start_eval_loop(store_copy_b, full_config.eval_threads);
    let manager_host = resolve_manager_host(&full_config.server.host);
    let manager_addr = format!("http://{}:{}", manager_host, full_config.server.port);
    let worker_fut = async move {
        if run_worker {
            info!(%manager_addr, "Starting in-process worker");
            run_worker_local(
                manager_addr,
                "default".to_string(),
                None,
                local_worker_join_secret,
            )
            .await?;
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
/// HA note: only the primary *rotates* (same leader-only convention as other
/// store-mutating loops). Standbys don't rotate, but they must not serve a
/// boot-frozen JWKS — keys are read once at startup — so on each tick a standby
/// reloads the key set from the replicated `signing_key` table, picking up
/// keys the primary rotated in (staleness bounded by the check interval).
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
            // Standby: don't rotate, but refresh the key set so this manager's
            // JWKS/signer track keys the primary rotated in.
            match state.reload_from_store(&store) {
                Ok(true) => info!("Refreshed workload identity signing keys from store (standby)"),
                Ok(false) => debug!("Workload identity signing keys unchanged (standby)"),
                Err(err) => warn!(error = ?err, "Failed to refresh signing keys on standby"),
            }
        }
        tokio::time::sleep(interval).await;
    }
}

/// Run one boot-path startup write, absorbing the transient half of raft
/// leadership (adversarial finding, 2026-07-25: a cross-lane composite
/// rejected on the boot path aborted the whole manager — the
/// `LEGION_SEED_UI_DATA` seed writes `node` on the control lane and
/// `node_health` on the status lane, so a node that leads control but has
/// not yet acquired status fails the second half).
///
/// A `NotRaftLeader` rejection is retried across the JC-40 convergence
/// window; if leadership still has not converged the manager WARNS and
/// carries on (the work is idempotent and the leadership-acquisition
/// watcher re-runs it on the next transition, and none of it is required
/// for the manager to serve). Every other error is returned unchanged —
/// a malformed config job must still stop startup loudly.
async fn startup_write<F, Fut>(label: &str, attempt: F) -> R<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = R<()>>,
{
    startup_write_with(
        label,
        STARTUP_WRITE_ATTEMPTS,
        STARTUP_WRITE_RETRY_DELAY,
        attempt,
    )
    .await
}

/// [`startup_write`] with an explicit retry budget, so the unit test can
/// exercise the policy without waiting out the production window.
async fn startup_write_with<F, Fut>(
    label: &str,
    attempts: usize,
    delay: Duration,
    mut attempt: F,
) -> R<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = R<()>>,
{
    for attempt_number in 1..=attempts {
        match attempt().await {
            Ok(()) => return Ok(()),
            Err(err) if error_is_not_raft_leader(&err) => {
                if attempt_number == attempts {
                    warn!(
                        error = ?err,
                        "Startup write '{label}' still hit a foreign-lane leader rejection after \
                         {attempts} attempts; continuing without it (it re-runs on the next \
                         leadership acquisition)"
                    );
                    return Ok(());
                }
                debug!(
                    attempt = attempt_number,
                    "Startup write '{label}' hit a foreign-lane leader rejection; retrying while \
                     lane leadership converges"
                );
                tokio::time::sleep(delay).await;
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
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

/// Loads the manager's TLS identity when `server.tls.enabled` is set.
///
/// TLS is opt-in (never inferred from files on disk) so a stray dev cert
/// cannot flip the listener to https and strand http clients.
fn load_server_tls_identity(tls: &TlsConfigFull) -> R<Option<Identity>> {
    if !tls.enabled {
        return Ok(None);
    }
    if tls.cert_path.is_empty() || tls.key_path.is_empty() {
        bail!("server.tls.enabled requires certPath and keyPath");
    }
    let cert = std::fs::read_to_string(&tls.cert_path)
        .with_context(|| format!("failed to read TLS certificate {}", tls.cert_path))?;
    let key = std::fs::read_to_string(&tls.key_path)
        .with_context(|| format!("failed to read TLS key {}", tls.key_path))?;
    Ok(Some(Identity::from_pem(cert, key)))
}

#[instrument(err, skip_all)]
async fn start_node_status_checker(store: Store, check_seconds: u64) -> R<()> {
    info!("Beginning node status checker loop");
    let started_at = std::time::Instant::now();
    let check_interval = Duration::from_secs(check_seconds);
    let mut was_status_leader = true;
    // Rollout reconciliation piggybacks on this loop but gates on the
    // SCHEDULER lane (C-2 rollout consolidation) — under `lanes` topology
    // the status and scheduler leaders can be different nodes, so it runs
    // on its own cadence timer instead of inside the status-leader branch.
    let mut last_rollout_reconcile: Option<std::time::Instant> = None;
    // Divergent-leadership safety net (adversarial findings, 2026-07-24):
    // periodic eval regeneration on the scheduler leader while it does NOT
    // lead every lane. A no-op in `single` topology and while one node
    // leads all lanes — which, with JC-40 convergence running in the
    // engine, is the steady state.
    let mut last_divergent_regen: Option<std::time::Instant> = None;
    loop {
        // Leader-only actions (raft mode, per lane): only the STATUS-lane
        // leader may mark nodes lost / sweep health orphans; non-leaders
        // idle on a short poll and take over within a few seconds of a
        // leadership change (JC-15). Non-raft backends always report
        // leadership here.
        let status_leader = store.is_raft_leader(crate::storage::Lane::Status);
        if status_leader {
            if !was_status_leader {
                info!("Raft status-lane leadership acquired; node status checker resuming");
                was_status_leader = true;
            }
            let node_ids_to_update =
                nodes_to_mark_lost(store.get_node_ids_and_heartbeats()?, started_at.elapsed());

            // Warn-and-continue rather than `?`: under `lanes` topology the
            // status-lane leader may not lead the control lane, and the
            // composite's control half (`sync_node_availability`) is then
            // rejected with that lane's leader hint. The checker must keep
            // ticking — the mark is retried every tick until it lands.
            if let Err(err) = store.mark_nodes_as_lost(&node_ids_to_update) {
                warn!(error = ?err, "Failed marking nodes lost; will retry next tick");
            }
            // Stateful allocations on lost nodes are gated behind their
            // onLost.replaceAfter loss-confirmation window (see
            // docs/stateful-workloads.md "Node loss and fencing"). Re-run the
            // reschedule path for still-lost nodes every tick so replacement
            // proceeds automatically once a window elapses — no operator
            // action needed.
            store.recheck_lost_nodes();
            // node_health rows orphaned by node removal (`remove_node` is a
            // control-lane op and must not delete status-lane rows — plan
            // §7-C2, C-1) are swept here on the status-lane leader. Harmless
            // while they exist, so failures only delay the sweep.
            match store.prune_node_health_orphans() {
                Ok(0) => {}
                Ok(swept) => info!(swept, "Swept node_health rows orphaned by node removal"),
                Err(err) => warn!(error = ?err, "Failed sweeping orphaned node_health rows"),
            }
            // Same shape for allocation_health rows orphaned by allocation
            // removal (the C-2 cross-lane FK removal replaced the cascade
            // with this status-lane sweep).
            match store.prune_allocation_health_orphans() {
                Ok(0) => {}
                Ok(swept) => info!(
                    swept,
                    "Swept allocation_health rows orphaned by allocation removal"
                ),
                Err(err) => warn!(error = ?err, "Failed sweeping orphaned allocation_health rows"),
            }
            // Submit seeds deferred under divergent lane leadership
            // (plan §8, D1): `handle_job_submit` tolerates a status-lane
            // rejection of its seed half; this pass completes those seeds
            // here on the status leader once the control rows replicate.
            // One cheap read in steady state (zero missing rows short-
            // circuits before any replicated op).
            match store.seed_missing_task_statuses() {
                Ok(0) => {}
                Ok(seeded) => info!(
                    seeded,
                    "Seeded task statuses a deferred submit half was missing"
                ),
                Err(err) => warn!(error = ?err, "Failed seeding missing task statuses"),
            }
        } else if was_status_leader {
            info!("Not the status-lane raft leader; node status checker idling until elected");
            was_status_leader = false;
        }

        // Scheduler-lane piggyback: rollout reconciliation runs wherever the
        // SCHEDULER lane is led, on the checker cadence — even when this
        // node does not lead the status lane (the two leaderships are
        // independent under `lanes` topology).
        let scheduler_leader = store.is_raft_leader(crate::storage::Lane::Scheduler);
        if scheduler_leader
            && last_rollout_reconcile.is_none_or(|at| at.elapsed() >= check_interval)
        {
            last_rollout_reconcile = Some(std::time::Instant::now());
            if let Err(err) = store.reconcile_incomplete_rollouts() {
                warn!(error = ?err, "Failed reconciling completed_incomplete rollouts");
            }
            // Rollouts an acknowledged update never got (plan §8, D1):
            // derived here in EVERY leadership shape, not only under
            // divergence — the same signature is left by a crash between
            // the submit and the rollout insert with no restart afterwards,
            // which no leadership-acquisition hook would ever heal.
            match crate::manager::server::reconcile_missing_rollouts(&store) {
                Ok(0) => {}
                Ok(created) => info!(created, "Created missing rollouts for acknowledged updates"),
                Err(err) => warn!(error = ?err, "Failed reconciling missing rollouts"),
            }
            // Volume-claim syncs deferred by a submit under divergent lane
            // leadership (plan §8, D1) — read-only in steady state.
            match store.sync_missing_task_volume_claims() {
                Ok(0) => {}
                Ok(synced) => info!(
                    synced,
                    "Synced volume claims a deferred submit half was missing"
                ),
                Err(err) => warn!(error = ?err, "Failed syncing missing task volume claims"),
            }
        }

        // Divergent-leadership eval safety net (JC-41 × lanes): a composite
        // acked on the control/status leader must issue its eval as a
        // SEPARATE scheduler-lane op, which that node cannot propose when a
        // different node leads the scheduler lane — the enqueue is cleanly
        // rejected and the work would otherwise never be scheduled (job
        // submits acked but never placed; lost nodes never rescheduled).
        // While this node leads the scheduler lane but NOT every lane,
        // periodically re-derive that work from replicated state (the same
        // idempotent pass the leadership-acquisition hook runs). Quiet in
        // steady state: JC-40 converges leadership, so the enqueue lands on
        // the same node and this branch never runs.
        let leads_every_lane = store.is_raft_leader(crate::storage::Lane::Control)
            && store.is_raft_leader(crate::storage::Lane::Status);
        if scheduler_leader
            && !leads_every_lane
            && last_divergent_regen.is_none_or(|at| at.elapsed() >= check_interval)
        {
            last_divergent_regen = Some(std::time::Instant::now());
            if let Err(err) = store.prune_stale_evals() {
                warn!(error = ?err, "Failed pruning stale evals under divergent lane leadership");
            }
            // Order matters (plan §8, D1): derive missing rollouts BEFORE
            // regenerating evals, so the regeneration's `job_submit`
            // fallback — which bypasses rollout gating — never fires for a
            // job whose rollout this pass was about to create.
            if let Err(err) = crate::manager::server::reconcile_missing_rollouts(&store) {
                warn!(
                    error = ?err,
                    "Failed reconciling missing rollouts under divergent lane leadership"
                );
            }
            match store.regenerate_scheduling_evals() {
                Ok(enqueued) if enqueued > 0 => info!(
                    enqueued,
                    "Regenerated scheduling evals under divergent lane leadership"
                ),
                Ok(_) => {}
                Err(err) => warn!(
                    error = ?err,
                    "Failed regenerating scheduling evals under divergent lane leadership"
                ),
            }
        }

        // Lane-leadership convergence itself is NOT here: it is an engine
        // lifecycle property now (JC-40 — `raft::driver::
        // lane_convergence_loop`, started with the engine on every node
        // running a multi-lane topology, anchored on the control lane).
        // Keeping it in this loop would have left every store that does not
        // run the manager daemon (embedded uses, the raft test batteries)
        // with permanently divergent lane leadership, and would have tied
        // convergence latency to the node-status-checker cadence.

        // Volumes whose instance no longer references them (keyed destroy,
        // scale-in without retention, job destroy) are queued for reclaim.
        // One reconciling rule covers every way a handle can be dropped, so no
        // code path can forget to free storage (see docs/volumes.md).
        // Leader-gated like the other reconciliation passes: it mutates
        // replicated state, so on a follower it would only ever be rejected
        // with a leader hint.
        if scheduler_leader {
            match store.reap_orphaned_volumes() {
                Ok(orphans) => {
                    for volume in orphans {
                        store.events().volume_reclaiming(&volume);
                    }
                }
                Err(err) => warn!(error = ?err, "Failed reaping orphaned volumes"),
            }
        }

        // Gauges are read-only over this node's replica; keep them fresh so
        // scraping a follower's /metrics is not frozen.
        refresh_cluster_gauges(&store);
        let sleep_for = if status_leader {
            check_interval
        } else {
            LEADER_POLL_INTERVAL
        };
        tokio::time::sleep(sleep_for).await;
    }
}

/// Whether this poll observed a leadership ACQUISITION — the false→true
/// transition on which the leader-only recovery writes (eval requeue,
/// initial-data load, manager-config upsert, rollout prune) must re-run.
/// A node that stays leader (true→true) must not re-run them every poll,
/// and losing leadership triggers nothing.
fn leadership_acquired(was_leader: bool, is_leader: bool) -> bool {
    is_leader && !was_leader
}

/// Refreshes the cheap cluster-shape gauges (`legion_nodes{status}`,
/// `legion_allocations`) — piggybacked on the node-status-checker tick so no
/// extra loop or per-write bookkeeping is needed.
fn refresh_cluster_gauges(store: &Store) {
    let telemetry = crate::manager::telemetry::metrics();
    match store.node_status_counts() {
        Ok(counts) => {
            let entries: Vec<(Vec<String>, i64)> = counts
                .into_iter()
                .map(|(status, count)| (vec![status], count))
                .collect();
            telemetry.nodes.set_all(&entries);
        }
        Err(err) => debug!(error = ?err, "Failed refreshing node status gauge"),
    }
    match store.allocation_count() {
        Ok(count) => telemetry.allocations.set(count),
        Err(err) => debug!(error = ?err, "Failed refreshing allocation gauge"),
    }
    // Raft engine series from the local metrics view — cheap watch borrows,
    // present on leaders and followers alike. Quorum shape (plan §7-C3:
    // voters/quorum) plus the per-lane progress series (term, commit and
    // applied indexes, apply lag, local leadership) that make failover
    // drills and bottleneck attribution observable. Non-raft backends
    // report no status and leave the families empty.
    if let Some(status) = store.raft_status() {
        let lane_series = |value: fn(&crate::storage::persistence::RaftLaneStatus) -> i64| {
            status
                .lanes
                .iter()
                .map(|lane| (vec![lane.lane.clone()], value(lane)))
                .collect::<Vec<(Vec<String>, i64)>>()
        };
        telemetry
            .raft_voters
            .set_all(&lane_series(|lane| lane.voters as i64));
        telemetry
            .raft_quorum
            .set_all(&lane_series(|lane| lane.quorum as i64));
        telemetry
            .raft_term
            .set_all(&lane_series(|lane| lane.term as i64));
        telemetry
            .raft_commit_index
            .set_all(&lane_series(|lane| lane.commit_index as i64));
        telemetry
            .raft_applied_index
            .set_all(&lane_series(|lane| lane.applied_index as i64));
        telemetry.raft_apply_lag.set_all(&lane_series(|lane| {
            lane.commit_index.saturating_sub(lane.applied_index) as i64
        }));
        telemetry.raft_is_leader.set_all(&lane_series(|lane| {
            i64::from(matches!(
                lane.role,
                crate::storage::persistence::PersistenceRole::Primary
            ))
        }));
    }
}

/// Which active nodes to mark lost, given their last heartbeats and how long
/// this manager process has been up.
///
/// Heartbeats only refresh while the manager is up, so time the manager spent
/// down must not count as missed heartbeats: right after a manager restart
/// every persisted heartbeat looks stale, and marking those nodes lost would
/// deschedule — and therefore kill — every healthy task in the cluster before
/// its worker had a chance to reconnect. No node is marked lost until the
/// manager has been up for a full lost-node window; live workers heartbeat
/// every `WORKER_HEARTBEAT_DURATION_SEC` and retry with a capped backoff, so
/// they re-establish contact well within it.
/// Public for the heartbeat scale benchmark
/// (`tests/heartbeat_scale_benchmark.rs`), which times one liveness pass;
/// not part of any stable API.
///
/// Nodes whose row carries the integer heartbeat clock compare against one
/// precomputed cutoff — no per-node timestamp parsing (heartbeat-scale
/// plan §4; at 10k nodes the old per-tick RFC3339 parse storm was 5,000
/// parses/second). Rows without it (written before the column existed)
/// fall back to parsing the text, with unparseable-means-lost preserved.
pub fn nodes_to_mark_lost(
    nodes_with_heartbeats: Vec<(String, String, Option<i64>)>,
    manager_uptime: Duration,
) -> Vec<String> {
    if manager_uptime < Duration::from_secs(DEF_LOST_NODE_AT_SEC as u64) {
        return Vec::new();
    }
    let unix_cutoff = OffsetDateTime::now_utc().unix_timestamp() - DEF_LOST_NODE_AT_SEC;
    nodes_with_heartbeats
        .into_iter()
        .filter(|(id, heartbeat, heartbeat_unix)| {
            if let Some(heartbeat_unix) = heartbeat_unix {
                let too_slow = *heartbeat_unix < unix_cutoff;
                if too_slow {
                    warn!("Node {} heartbeat too slow - marking as lost", id);
                }
                return too_slow;
            }
            match Node::last_heartbeat_too_slow(heartbeat) {
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
            }
        })
        .map(|(id, _heartbeat, _unix)| id)
        .collect()
}

#[instrument(err, skip_all)]
async fn start_eval_loop(store: Store, max_threads: usize) -> R<()> {
    let max_threads = max_threads.max(1);
    info!(max_threads, "Beginning eval loop");

    // Leader-only startup recovery (raft mode): the requeue/prune are
    // themselves replicated scheduler-lane ops (JC-41), so only the
    // SCHEDULER lane's leader may run them — a follower would just be
    // rejected, and it has no eval worker to feed anyway. A follower that
    // wins a later election recovers through the leadership-acquisition
    // hook below.
    let is_raft_follower = !store.is_raft_leader(crate::storage::Lane::Scheduler);
    if is_raft_follower {
        info!("Skipping in-flight eval requeue on raft non-leader manager");
    } else {
        // Stale rows referencing superseded configs are dropped BEFORE the
        // requeue: reviving them would execute old-config work and suppress
        // regeneration for the same jobs (see Store::prune_stale_evals).
        let pruned = store.prune_stale_evals()?;
        if pruned > 0 {
            warn!(
                pruned,
                "Dropped stale evals for superseded job configs at startup"
            );
        }
        let requeued = store.requeue_in_flight_evals()?;
        if requeued > 0 {
            warn!(requeued, "Recovered stale in-flight evals at startup");
            store.events().scheduler_recovered(requeued);
        }
        store.events().scheduler_started(max_threads);
    }
    // Raft mode: queue recovery must survive leader FAILOVER, not just
    // process startup. The eval queue is REPLICATED (plan §7-C2, JC-41), so
    // a newly elected leader already HOLDS the failed leader's pending work
    // — that is what makes an acknowledged `job stop` or a mid-flight
    // rollout survive (tests/raft_eval_failover.rs). What the new leader
    // still has to do on acquisition is (1) drop rows a later replicated
    // write superseded (`prune_stale_evals`), (2) requeue evals the old
    // leader had CLAIMED but not completed — they are `in_flight` in
    // replicated state and nothing else would ever release them — and (3)
    // run `regenerate_scheduling_evals` as belt-and-braces reconciliation
    // for work whose enqueue op never landed. Every regenerated eval is an
    // idempotent pass, so re-doing finished work is safe.
    if store.raft_status().is_some() {
        let recovery_store = store.clone();
        let mut was_leader = !is_raft_follower;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(LEADER_POLL_INTERVAL).await;
                let is_leader = recovery_store.is_raft_leader(crate::storage::Lane::Scheduler);
                if leadership_acquired(was_leader, is_leader) {
                    // Drop stale local rows (superseded configs) BEFORE the
                    // requeue: a previous leadership stint's queue must not
                    // execute old-config work or suppress regeneration
                    // (Store::prune_stale_evals).
                    match recovery_store.prune_stale_evals() {
                        Ok(pruned) if pruned > 0 => warn!(
                            pruned,
                            "Dropped stale evals for superseded job configs on leadership \
                             acquisition"
                        ),
                        Ok(_) => {}
                        Err(err) => warn!(
                            error = ?err,
                            "Failed pruning stale evals after leadership acquisition"
                        ),
                    }
                    match recovery_store.requeue_in_flight_evals() {
                        Ok(requeued) if requeued > 0 => {
                            warn!(
                                requeued,
                                "Recovered stale in-flight evals after raft leadership acquisition"
                            );
                            recovery_store.events().scheduler_recovered(requeued);
                        }
                        Ok(_) => {}
                        Err(err) => warn!(
                            error = ?err,
                            "Failed requeuing in-flight evals after leadership acquisition"
                        ),
                    }
                    // Missing rollouts are derived BEFORE eval regeneration
                    // (plan §8, D1): once a job's rollout exists, the
                    // regeneration pass excludes the job from its blanket
                    // `job_submit` fallback — which places the new config at
                    // full count with no canary/manual gating — so the order
                    // is what preserves rollout semantics for updates whose
                    // scheduler-lane tail was lost.
                    match crate::manager::server::reconcile_missing_rollouts(&recovery_store) {
                        Ok(created) if created > 0 => info!(
                            created,
                            "Created missing rollouts after raft leadership acquisition"
                        ),
                        Ok(_) => {}
                        Err(err) => warn!(
                            error = ?err,
                            "Failed reconciling missing rollouts after leadership acquisition"
                        ),
                    }
                    match recovery_store.regenerate_scheduling_evals() {
                        Ok(enqueued) if enqueued > 0 => info!(
                            enqueued,
                            "Reconciled pending scheduling evals after raft leadership \
                             acquisition (belt-and-braces, plan §7-C2 JC-41)"
                        ),
                        Ok(_) => {}
                        Err(err) => warn!(
                            error = ?err,
                            "Failed regenerating scheduling evals after leadership acquisition"
                        ),
                    }
                }
                was_leader = is_leader;
            }
        });
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
    let mut was_leader = true;
    loop {
        // Leader-only scheduling (raft mode, JC-41): the eval queue is
        // REPLICATED — followers hold every eval row but never ACT on the
        // queue, and every claim/complete is itself a scheduler-lane raft op
        // that only the leader may propose. Followers idle on a short poll
        // and take over within a few seconds of a leadership change, at
        // which point they inherit the real pending queue (the acquisition
        // hook below only reconciles). Non-raft backends always report
        // leadership here.
        if !store.is_raft_leader(crate::storage::Lane::Scheduler) {
            if was_leader {
                info!(
                    worker_id,
                    "Not the raft leader; eval worker idling until elected"
                );
                was_leader = false;
            }
            tokio::time::sleep(LEADER_POLL_INTERVAL).await;
            continue;
        }
        if !was_leader {
            info!(worker_id, "Raft leadership acquired; eval worker resuming");
            was_leader = true;
        }
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
                // Eval handling is synchronous end to end — SQLite writes,
                // raft-proposal waits, and the plan-conflict retry loop's
                // backoff sleeps (`plan_with_conflict_retries`), which can
                // hold the thread for hundreds of milliseconds per retry.
                // Run it on the blocking pool so those stalls never park a
                // tokio runtime worker; awaiting the handle keeps this
                // worker's one-eval-at-a-time ordering.
                let store_for_handler = store.clone();
                if let Err(join_error) = tokio::task::spawn_blocking(move || {
                    handle_claimed_eval(&store_for_handler, worker_id, &eval)
                })
                .await
                {
                    error!(worker_id, "Eval handler task failed: {join_error:?}");
                }
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

    /// Adversarial regression (leader-only startup writes): the recovery
    /// writes must re-run exactly on leadership ACQUISITION — a node that
    /// boots as follower (or leaderless after a full-cluster restart) and
    /// wins an election later would otherwise never requeue in-flight evals
    /// or load config data.
    #[test]
    fn leadership_acquired_fires_only_on_false_to_true() {
        assert!(leadership_acquired(false, true));
        assert!(!leadership_acquired(true, true));
        assert!(!leadership_acquired(true, false));
        assert!(!leadership_acquired(false, false));
    }

    /// The NotRaftLeader release delay doubles per rejection of the same
    /// eval, caps at the max, carries bounded jitter, and resets once the
    /// eval completes (or fails terminally) — a flat 5s forever parked
    /// split-stranded evals loudly and thundered them back together.
    #[test]
    fn not_leader_retry_delay_backs_off_caps_and_resets() {
        let eval_id = "backoff-test-eval";
        clear_not_leader_attempts(eval_id);

        let base = EVAL_NOT_LEADER_RETRY_DELAY.as_millis() as u64;
        let jitter = EVAL_NOT_LEADER_RETRY_JITTER_MS;
        for attempt in 0..8u32 {
            let expected_ms = base
                .saturating_mul(1 << attempt)
                .min(EVAL_NOT_LEADER_RETRY_MAX_DELAY.as_millis() as u64);
            let delay = not_leader_retry_delay(eval_id).as_millis() as u64;
            assert!(
                (expected_ms..=expected_ms + jitter).contains(&delay),
                "attempt {attempt}: delay {delay}ms outside [{expected_ms}, {}]",
                expected_ms + jitter
            );
        }

        clear_not_leader_attempts(eval_id);
        let after_reset = not_leader_retry_delay(eval_id).as_millis() as u64;
        assert!(
            (base..=base + jitter).contains(&after_reset),
            "reset delay {after_reset}ms should return to the base"
        );
        clear_not_leader_attempts(eval_id);
    }

    /// Adversarial regression (dropped acknowledged work): a NotRaftLeader
    /// anywhere in an eval error's chain is retried, anything else fails
    /// terminally.
    #[test]
    fn not_raft_leader_errors_are_detected_through_the_chain() {
        let not_leader = anyhow::Error::new(crate::storage::raft::driver::NotRaftLeader {
            lane: Some("status".into()),
            leader_name: Some("mgr-b".into()),
            leader_addr: Some("10.0.0.2:4646".into()),
        })
        .context(
            "raft op update_task_statuses_for_allocations must run on the status lane's leader",
        )
        .context("scheduling follow-up failed");
        assert!(error_is_not_raft_leader(&not_leader));
        assert!(!error_is_not_raft_leader(&anyhow!("plain failure")));
    }

    /// Adversarial regression (2026-07-25, boot path): a startup write whose
    /// cross-lane half is rejected while lane leadership converges must be
    /// RETRIED, not abort the manager — while a genuine error (a malformed
    /// config job) must still propagate.
    #[tokio::test]
    async fn startup_writes_retry_transient_lane_rejections_but_not_real_errors() -> R<()> {
        const TEST_ATTEMPTS: usize = 4;
        const TEST_DELAY: Duration = Duration::from_millis(1);

        fn not_leader() -> anyhow::Error {
            anyhow::Error::new(crate::storage::raft::driver::NotRaftLeader {
                lane: Some("status".into()),
                leader_name: Some("mgr-b".into()),
                leader_addr: Some("10.0.0.2:4646".into()),
            })
            .context("raft op seed_ui_test_data must run on the status lane's leader")
        }

        // Converges on the third attempt: the composite succeeds and startup
        // continues.
        let mut attempts = 0;
        startup_write_with("converging write", TEST_ATTEMPTS, TEST_DELAY, || {
            attempts += 1;
            let attempt = attempts;
            async move {
                if attempt < 3 {
                    Err(not_leader())
                } else {
                    Ok(())
                }
            }
        })
        .await?;
        assert_eq!(attempts, 3, "must retry until leadership converges");

        // Never converges: warn and continue rather than abort the manager.
        let mut attempts = 0;
        startup_write_with("never-converging write", TEST_ATTEMPTS, TEST_DELAY, || {
            attempts += 1;
            async { Err(not_leader()) }
        })
        .await
        .expect("a standing lane split must not abort manager startup");
        assert_eq!(attempts, TEST_ATTEMPTS);

        // A real failure still stops startup, and is not retried.
        let mut attempts = 0;
        let err = startup_write_with("bad config", TEST_ATTEMPTS, TEST_DELAY, || {
            attempts += 1;
            async { Err(anyhow!("job config is malformed")) }
        })
        .await
        .expect_err("non-transient startup errors must still abort");
        assert!(format!("{err:#}").contains("malformed"), "{err:#}");
        assert_eq!(attempts, 1, "a real error must not be retried");
        Ok(())
    }

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

    fn rfc3339_seconds_ago(seconds: i64) -> R<String> {
        Ok(
            (OffsetDateTime::now_utc() - time::Duration::seconds(seconds))
                .format(&time::format_description::well_known::Rfc3339)?,
        )
    }

    fn node_msg(id: &str, last_heartbeat: String) -> NodeMsg {
        NodeMsg {
            id: id.to_string(),
            pool: "default".into(),
            status: "active".into(),
            availability: "available".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: "{}".into(),
            dynamic_attrs: "{}".into(),
            health_checks: "{}".into(),
            last_heartbeat,
        }
    }

    #[test]
    fn nodes_to_mark_lost_waits_for_startup_heartbeat_grace() -> R<()> {
        let lost_window = DEF_LOST_NODE_AT_SEC as u64;
        // No unix clock on these rows: exercises the text-parse fallback.
        let nodes = vec![
            ("stale-node".to_string(), rfc3339_seconds_ago(120)?, None),
            ("fresh-node".to_string(), rfc3339_seconds_ago(0)?, None),
        ];

        // Right after a manager restart every persisted heartbeat looks
        // stale; nothing may be marked lost until workers have had a full
        // window to reconnect.
        assert!(nodes_to_mark_lost(nodes.clone(), Duration::ZERO).is_empty());
        assert!(nodes_to_mark_lost(nodes.clone(), Duration::from_secs(lost_window - 1)).is_empty());

        // Once the manager has been up a full window, silence means real loss
        // — and only for the silent node.
        assert_eq!(
            nodes_to_mark_lost(nodes, Duration::from_secs(lost_window)),
            vec!["stale-node".to_string()]
        );
        Ok(())
    }

    /// Regression: a manager outage longer than the lost-node window used to
    /// mark every node lost on the status checker's first tick after restart,
    /// descheduling (and therefore killing) all healthy running tasks before
    /// their workers had a chance to re-heartbeat.
    #[test]
    fn manager_restart_does_not_deschedule_allocations_before_heartbeat_grace() -> R<()> {
        let store = setup_store()?;

        // Cluster state as persisted before the manager went down: an active
        // node running one allocation, whose last heartbeat is now older than
        // the lost-node window only because the manager was unreachable.
        store.insert_node(node_msg(
            "node-1",
            rfc3339_seconds_ago(DEF_LOST_NODE_AT_SEC + 50)?,
        ))?;
        // A second node that reconnected promptly after the manager restart;
        // it must never be marked lost and later receives the rescheduled
        // allocation once node-1's loss is confirmed.
        store.insert_node(node_msg("node-2", rfc3339_seconds_ago(0)?))?;
        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "restart-survivor".into(),
            tasks: vec![TaskConfig::new("svc", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let task_id: i64 = store.read_connection()?.query_row(
            "SELECT id FROM task_config WHERE name = ? AND job_id = ?;",
            ["svc", &job.name],
            |row| row.get(0),
        )?;
        store.place_alloc("node-1", task_id)?;

        // First status-checker tick of the freshly restarted manager: the
        // node must stay active and keep its allocation.
        let to_mark = nodes_to_mark_lost(store.get_node_ids_and_heartbeats()?, Duration::ZERO);
        store.mark_nodes_as_lost(&to_mark)?;
        assert_eq!(store.get_node("node-1")?.status, "active");
        assert_eq!(
            store.allocs_for_node("node-1")?.len(),
            1,
            "allocation must survive a manager restart"
        );

        // The grace elapsed and the node still never heartbeated: now it is
        // genuinely lost and its allocations reschedule off it.
        let to_mark = nodes_to_mark_lost(
            store.get_node_ids_and_heartbeats()?,
            Duration::from_secs(DEF_LOST_NODE_AT_SEC as u64),
        );
        assert_eq!(to_mark, vec!["node-1".to_string()]);
        store.mark_nodes_as_lost(&to_mark)?;
        // The reschedule tail is eval-mediated since C-2: pump the queue
        // the way the daemon's eval worker would.
        while let Some(eval) = store.claim_eval()? {
            eval.handle(&store)?;
            store.complete_eval(&eval.id, true)?;
        }
        assert_eq!(store.get_node("node-1")?.status, "lost");
        assert!(
            store.allocs_for_node("node-1")?.is_empty(),
            "allocations must reschedule once the loss is real"
        );
        assert_eq!(
            store.allocs_for_node("node-2")?.len(),
            1,
            "the surviving node receives the rescheduled allocation"
        );
        Ok(())
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

/// Per-eval NotRaftLeader release counts, kept in-process only: the eval
/// table has no attempt column, and durability is not needed — the count
/// only shapes the retry delay, and resetting to the base after a restart
/// or failover is the right behavior (new leadership is exactly the
/// condition the backoff waits for). Entries are dropped when the eval
/// completes or fails terminally; entries stranded by a leadership move
/// (the eval finishes on another node) are bounded by the size guard in
/// [`not_leader_retry_delay`].
static EVAL_NOT_LEADER_ATTEMPTS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, u32>>,
> = std::sync::OnceLock::new();

fn eval_not_leader_attempts() -> &'static std::sync::Mutex<std::collections::HashMap<String, u32>> {
    EVAL_NOT_LEADER_ATTEMPTS.get_or_init(Default::default)
}

fn clear_not_leader_attempts(eval_id: &str) {
    let mut attempts = eval_not_leader_attempts()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    attempts.remove(eval_id);
}

/// Exponential backoff + jitter for foreign-lane NotRaftLeader releases:
/// 5s, 10s, 20s, 40s, then capped at 60s, plus up to 1s of jitter so a
/// batch of split-parked evals doesn't thunder back on one tick. Records
/// the attempt as a side effect.
fn not_leader_retry_delay(eval_id: &str) -> Duration {
    let attempt = {
        let mut attempts = eval_not_leader_attempts()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Stale entries (evals finished by another leader after a
        // failover) never see the completion-path removal; clearing the
        // whole map at an absurd size costs at most one backoff reset per
        // stranded eval.
        if attempts.len() > 10_000 {
            attempts.clear();
        }
        let entry = attempts.entry(eval_id.to_owned()).or_insert(0);
        let current = *entry;
        *entry = entry.saturating_add(1);
        current
    };
    let base_ms = EVAL_NOT_LEADER_RETRY_DELAY.as_millis() as u64;
    let capped_ms = base_ms
        .saturating_mul(1u64 << attempt.min(10))
        .min(EVAL_NOT_LEADER_RETRY_MAX_DELAY.as_millis() as u64);
    let jitter_ms = rand::thread_rng().gen_range(0..=EVAL_NOT_LEADER_RETRY_JITTER_MS);
    Duration::from_millis(capped_ms + jitter_ms)
}

/// Handle one claimed eval and settle its row: complete on success, release
/// for retry on foreign-lane rejections and plan-conflict exhaustion, fail
/// terminally otherwise. Public so integration tests (the plan-contention
/// harness) can drive the real worker logic, requeue backstop included.
pub fn handle_claimed_eval(store: &Store, worker_id: usize, eval: &Eval) {
    let started = std::time::Instant::now();
    info!(
        worker_id,
        eval_id = %eval.id,
        kind = %eval.kind,
        "Dispatching claimed eval"
    );
    let result = eval.handle(store);
    crate::manager::telemetry::metrics()
        .evals_processed_total
        .inc(&[&eval.kind, if result.is_ok() { "ok" } else { "error" }]);
    match result {
        Ok(message) => {
            info!(
                worker_id,
                eval_id = %eval.id,
                kind = %eval.kind,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Eval {} handled successfully: {}", eval.id, message
            );
            clear_not_leader_attempts(&eval.id);
            let completed = drafts::eval_completed(eval, &message, started.elapsed());
            if let Err(e) = store.complete_eval_with_event(&eval.id, true, completed) {
                error!(worker_id, "Error completing eval {}: {:?}", eval.id, e);
            }
        }
        // A NotRaftLeader rejection means a cross-lane half of the eval's
        // work hit a lane this node does not lead (divergent per-lane
        // leadership under `lanes` topology) — the eval itself is fine and
        // MUST run again, or acknowledged work is silently dropped (failed
        // evals are never retried). Release it back to pending with a delay
        // instead of terminally failing it.
        Err(e) if error_is_not_raft_leader(&e) => {
            let retry_delay = not_leader_retry_delay(&eval.id);
            warn!(
                worker_id,
                eval_id = %eval.id,
                kind = %eval.kind,
                retry_delay_ms = retry_delay.as_millis() as u64,
                "Eval hit a foreign-lane leader rejection; releasing for retry: {e:#}"
            );
            if let Err(release_err) = store.release_eval_for_retry(&eval.id, retry_delay) {
                error!(
                    worker_id,
                    "Error releasing eval {} for retry: {:?}", eval.id, release_err
                );
            }
        }
        // Plan-conflict exhaustion is transient by definition: the plan lost
        // optimistic-concurrency races, not validity. Terminally failing the
        // eval here is how acknowledged work used to vanish (failed evals are
        // never retried — a submit that lost `plan_conflict_max_retries`
        // races silently never placed), so release it back to pending with a
        // jittered delay instead. The requeue rides one scheduler-lane op
        // with its `eval.requeued` event, so saturation shows as visibly
        // waiting work (docs/late-binding-plan-apply.md).
        Err(e) if Eval::error_is_plan_conflict(&e) => {
            let delay = Eval::plan_conflict_requeue_delay(store);
            warn!(
                worker_id,
                eval_id = %eval.id,
                kind = %eval.kind,
                delay_ms = delay.as_millis() as u64,
                "Eval exhausted plan-conflict retries; requeueing: {e:#}"
            );
            crate::manager::telemetry::metrics()
                .eval_conflict_requeues_total
                .inc();
            let requeued = drafts::eval_requeued(eval, &e, delay);
            if let Err(release_err) =
                store.release_eval_for_retry_with_event(&eval.id, delay, requeued)
            {
                error!(
                    worker_id,
                    "Error requeueing eval {} after plan conflict: {:?}", eval.id, release_err
                );
            }
        }
        Err(e) => {
            error!(worker_id, eval_id = %eval.id, elapsed_ms = started.elapsed().as_millis() as u64, "Error handling eval {}: {:?}", eval.id, e);
            clear_not_leader_attempts(&eval.id);
            let failed = drafts::eval_failed(eval, &e, started.elapsed());
            if let Err(complete_err) = store.complete_eval_with_event(&eval.id, false, failed) {
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

/// Whether an eval-handler error is (or wraps) a raft `NotRaftLeader`
/// rejection — the divergent-lane-leadership case `handle_claimed_eval`
/// retries instead of terminally failing.
fn error_is_not_raft_leader(error: &anyhow::Error) -> bool {
    crate::storage::raft::driver::NotRaftLeader::in_chain(error)
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

        let artifacts = match wcp.artifacts.as_ref() {
            None => String::new(),
            Some(config) => {
                // Fail fast on malformed fetch settings (bad humantime /
                // byte-size strings) at manager startup, not on the worker.
                config.fetch_with_defaults().with_context(|| {
                    format!("invalid artifacts config in worker config {path_str}")
                })?;
                serde_json::to_string(config)?
            }
        };

        // Host volumes are validated here so a typo (a relative path) fails at
        // manager startup rather than at a task's first mount.
        let host_volumes = match wcp.host_volumes.as_ref() {
            None => String::new(),
            Some(volumes) => {
                for (name, volume) in volumes {
                    // Exactly one of path/device: a path serves filesystem
                    // claims, a device serves block claims, and an entry with
                    // both would be ambiguous about which a claim gets.
                    match (volume.path.trim().is_empty(), volume.device.as_deref()) {
                        (true, None) => bail!(
                            "worker config {path_str}: hostVolumes.{name} needs a `path` \
                             (filesystem) or a `device` (block)"
                        ),
                        (false, Some(_)) => bail!(
                            "worker config {path_str}: hostVolumes.{name} declares both `path` \
                             and `device`; declare two entries if you mean both"
                        ),
                        (false, None) if !volume.path.starts_with('/') => bail!(
                            "worker config {path_str}: hostVolumes.{name}.path must be absolute, \
                             got {:?}",
                            volume.path
                        ),
                        (true, Some(device)) if !device.starts_with('/') => bail!(
                            "worker config {path_str}: hostVolumes.{name}.device must be \
                             absolute, got {device:?}"
                        ),
                        _ => {}
                    }
                    if volume.create && volume.device.is_some() {
                        bail!(
                            "worker config {path_str}: hostVolumes.{name} sets `create` on a \
                             `device` entry; Legion never creates or formats devices"
                        );
                    }
                }
                serde_json::to_string(volumes)?
            }
        };

        // Enforcement settings ride as JSON, like artifacts and host volumes.
        let resource_enforcement = match wcp.resource_enforcement.as_ref() {
            None => String::new(),
            Some(enforcement) => {
                if enforcement.cgroup_parent.trim().is_empty() {
                    bail!(
                        "worker config {path_str}: resourceEnforcement.cgroupParent must not be \
                         empty"
                    );
                }
                if enforcement.cgroup_parent.starts_with('/') {
                    bail!(
                        "worker config {path_str}: resourceEnforcement.cgroupParent is relative \
                         to /sys/fs/cgroup, got {:?}",
                        enforcement.cgroup_parent
                    );
                }
                serde_json::to_string(enforcement)?
            }
        };

        if let Some(seconds) = wcp.heartbeat_seconds {
            if !(1..=30).contains(&seconds) {
                bail!(
                    "worker config {path_str}: heartbeatSeconds must be between 1 and 30, \
                     got {seconds}"
                );
            }
            if seconds > 3 {
                warn!(
                    worker_type = %wcp.worker_type,
                    heartbeat_seconds = seconds,
                    "heartbeatSeconds above 3: the 10s lost-node window leaves fewer than \
                     three missed beats before workers are marked lost"
                );
            }
        }

        let wc = WorkerConfigMsg {
            name: wcp.worker_type.clone(),
            pool: wcp.pool.unwrap_or("default".into()),
            additional_node_checks: af,
            attributes: attrs,
            name_prefix: wcp.name_prefix,
            resources,
            reserved_resources,
            max_allocations: wcp.max_allocations,
            artifacts,
            host_volumes,
            heartbeat_seconds: wcp.heartbeat_seconds,
            resource_enforcement,
        };

        store.insert_worker_type(&wc)?;
    }

    Ok(())
}
