use crate::prelude::*;

use crate::manager::concurrency::{is_planning_kind, PlannerPermit};
use crate::manager::oidc_http::OidcHttpLayer;
use crate::manager::server::ManagerDaemonService;
use crate::manager::workload_identity::WorkloadIdentityState;
use crate::storage::events::drafts;
use crate::storage::persistence::PersistenceRole;
use crate::storage::store::{ClaimScope, EvalBatchLimits, EvalCompletion, Store};
use crate::structs::{
    manager_server::ManagerServer, Eval, JobConfig, KvValueType, ManagerConfig, NodeMsg,
    Submission, TaskConfig, TlsConfigFull, WorkerConfig as WorkerConfigMsg, WorkerConfigAlt,
};
use crate::worker::daemon::run_worker_local;

use futures::join;
use http::Method;
use rand::Rng;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic_web::GrpcWebLayer;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

// Fallback poll interval used only when delay calculation fails.
const IDLE_POLL_INTERVAL: Duration = Duration::from_secs(30);
// Safety cap for empty queues so idle workers can still periodically re-check state.
const IDLE_WAIT_CAP: Duration = Duration::from_secs(300);
// How long a throttled eval worker with no non-planning work parks before
// re-checking the admission gate. Short, because the thing it is waiting for
// is a permit rather than a timer, and `Notify::notify_waiters` stores no
// permit for a release that lands between the claim attempt and the wait.
const THROTTLED_GATE_POLL: Duration = Duration::from_millis(100);
// Exponential backoff is reserved for true error paths (claim failures).
const ERROR_BACKOFF_MIN: Duration = Duration::from_secs(1);
const ERROR_BACKOFF_MAX: Duration = Duration::from_secs(30);
// How often a raft non-leader re-checks lane leadership before acting
// (eval workers, node status checker). Bounds the takeover delay after a
// leadership change to "a few seconds" (plan JC-15).
const LEADER_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// How long a keyed mutation's idempotency record is kept. Retries within
/// this window are deduplicated; after it, the same key runs again. A day
/// covers any client retry loop, matching common API practice.
pub const IDEMPOTENCY_KEY_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// How often the control-lane leader prunes idempotency records older than
/// `IDEMPOTENCY_KEY_TTL`, so records live between 24 and 25 hours.
const IDEMPOTENCY_PRUNE_INTERVAL: Duration = Duration::from_secs(60 * 60);
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
// Hard ceiling on how many evals one worker takes through claim → handle →
// complete together (`Store::claim_evals`). The batch is really sized as a
// fair share of the live queue depth, so this only binds on a deep queue;
// it is deliberately NOT a config knob, because the useful value follows
// queue depth and `evalThreads`, both of which the loop already knows.
//
// 16 is chosen against what a batch COSTS, not what it saves: a worker
// holds its whole batch `in_flight` until the terminal write lands, so a
// crash mid-batch re-runs up to this many already-handled evals (handlers
// are idempotent convergence steps, so re-running is safe, just wasted).
// Sixteen keeps that bounded while still dividing the lane's per-eval round
// cost by an order of magnitude on a saturated queue.
const MAX_EVAL_BATCH: usize = 16;

/// How many times an eval worker retries the batch terminal write before
/// abandoning the batch to in-flight recovery. Bounded because retrying
/// forever would wedge the worker on a persistent write error; more than
/// one because on a STABLE leader nothing else ever recovers the batch
/// (`requeue_in_flight_evals` runs only at startup and on leadership
/// acquisition), so giving up on the first transient error orphaned up to
/// a whole batch of handled evals.
const EVAL_SETTLE_MAX_ATTEMPTS: u32 = 3;

#[instrument(err, skip_all)]
pub async fn run_manager(config: ManagerConfig, run_worker: bool) -> R<()> {
    run_manager_with(config, run_worker, None).await
}

/// [`run_manager`] with the `--restore-from` provenance: when a backup was
/// just copied into place, the manager refuses to serve if the restored KV
/// values cannot be decrypted here (docs/kv-encryption.md, "Backups").
pub async fn run_manager_with(
    config: ManagerConfig,
    run_worker: bool,
    restored_from: Option<String>,
) -> R<()> {
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
    // Heartbeat cadence and liveness: reject combinations that cannot hold
    // together, naming the offending values. A grace shorter than the window
    // it protects, or a checkpoint fast enough to re-create the commit load
    // this design removes, is a configuration error and belongs at startup
    // rather than in an incident (docs/heartbeat-cadence-and-scale.md,
    // docs/node-liveness.md).
    full_config.heartbeat.validate()?;
    full_config.liveness.validate(
        full_config.heartbeat.parsed_min_interval()
            * full_config.heartbeat.missed_beats_before_lost.max(1),
    )?;
    // Adaptive scheduler concurrency, validated HERE rather than inside its
    // own loop: that loop's error is swallowed by the `join!` below, so a
    // mistyped `mode = "enforc"` would silently leave the controller off
    // while the operator believed they had turned it on
    // (docs/adaptive-scheduler-concurrency.md).
    let adaptive_concurrency =
        crate::manager::concurrency::AdaptiveConcurrencySettings::from_scheduler_settings(
            full_config.scheduler_settings.as_ref(),
        )?;
    let manager_node_id = format!("{}:{}", full_config.server.host, full_config.server.port);
    let store = Store::start_storage(&full_config)?;
    // A restore is an explicit operator action with the operator present:
    // fail fast, naming the missing root key, rather than serve a cluster
    // whose secrets are unreadable.
    if let Some(backup) = &restored_from {
        store.verify_keyring_after_restore(backup)?;
    }
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
    // Streaming standbys take over the same way when they win the lease.
    if store.raft_status().is_some() || store.persistence_status().backend == "streaming" {
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
                    // The keyring loop initializes/rotates/rewraps on the
                    // leader only; wake it so a new leader picks that up now
                    // instead of on its next tick.
                    watch_store.keyring().wake();
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

    let store_for_keyring = store.clone();

    let store_for_shutdown = store.clone();
    let store_copy_c = store.clone();

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
                        store.clone(),
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

    // Idempotency records only need to outlive a client's retry window;
    // without this the table grows by one row per keyed mutation forever.
    // Control-lane leader only (always true outside raft mode): the delete
    // is a replicated write.
    let idempotency_store = store.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(IDEMPOTENCY_PRUNE_INTERVAL).await;
            if !idempotency_store.is_raft_leader(crate::storage::Lane::Control) {
                continue;
            }
            let cutoff = OffsetDateTime::now_utc() - IDEMPOTENCY_KEY_TTL;
            match idempotency_store.prune_idempotency_records(cutoff) {
                Ok(0) => {}
                Ok(pruned) => info!(pruned, "Pruned expired idempotency records"),
                Err(err) => warn!(error = ?err, "Failed pruning idempotency records"),
            }
        }
    });

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

    // Read-model maintenance on every manager (each holds its own cache):
    // rebuild after an outside commit to the state database, and reload a
    // rotating slice of nodes so drift from any source heals within one
    // sweep (docs/state-store.md, "Read model coherence").
    let maintenance_store = store.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(
            crate::constants::READ_MODEL_MAINTENANCE_INTERVAL_SECS,
        ));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let store = maintenance_store.clone();
            let outcome = tokio::task::spawn_blocking(move || -> R<()> {
                if !store.check_external_writes()? {
                    store.scrub_read_model_slice(
                        crate::constants::READ_MODEL_SCRUB_NODES_PER_TICK,
                    )?;
                }
                Ok(())
            })
            .await;
            match outcome {
                Ok(Ok(())) => {}
                Ok(Err(err)) => warn!(error = ?err, "Read-model maintenance failed"),
                Err(join_error) => warn!("Read-model maintenance task failed: {join_error:?}"),
            }
        }
    });

    // Clone store + authenticator for the exec WebSocket listener before the
    // service takes ownership of them.
    let exec_ws_state = crate::manager::exec_ws::ExecWsState {
        store: store.clone(),
        auth: authenticator.clone(),
        origins: Arc::new(crate::manager::origin::OriginPolicy::new(
            &full_config.server.allowed_origins,
        )),
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

    let addr: std::net::SocketAddr =
        format!("{}:{}", full_config.server.host, full_config.server.port).parse()?;

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
    // Runtime saturation probe. Started regardless of the scrape listener —
    // the OTLP push exports the same registry — and independent of it: when
    // handlers block the runtime, every metric that answers "why is a
    // do-nothing RPC slow?" comes from here.
    crate::manager::telemetry::start_runtime_probe();

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

    // The idle sweep reads the usage table heartbeats fill, so it shares
    // the service's handle.
    let usage_for_idle_sweep = manager_node.usage_handle();
    let manager_service = ManagerServer::new(manager_node);
    // Browser origins (docs/ui-architecture.md): loopback plus
    // `server.allowedOrigins`. The guard refuses everything else before any
    // handler runs; CORS only echoes the allowed origin back.
    let origin_policy = Arc::new(crate::manager::origin::OriginPolicy::new(
        &full_config.server.allowed_origins,
    ));
    let cors_policy = origin_policy.clone();
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(move |origin, _| {
            origin
                .to_str()
                .is_ok_and(|origin| cors_policy.allows(origin))
        }))
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
    let (stop_serving_tx, stop_serving_rx) = tokio::sync::oneshot::channel::<()>();
    let start_server_fut = server_builder
        .accept_http1(true)
        .layer(OidcHttpLayer::new(workload_identity_state.clone()))
        .layer(crate::manager::origin::OriginGuardLayer::new(
            origin_policy.clone(),
        ))
        .layer(cors)
        .layer(GrpcWebLayer::new())
        .layer(crate::manager::telemetry::GrpcMetricsLayer)
        .add_service(manager_service)
        .add_service(raft_transport_service)
        .serve_with_shutdown(addr, async move {
            // A dropped sender also stops the server; both mean "shut down".
            let _ = stop_serving_rx.await;
        });

    println!("Manager node starting on {addr}");

    let node_checker_fut = start_node_status_checker(
        store_copy_a,
        LivenessCheckerConfig {
            check_interval: Duration::from_secs(full_config.worker_check_seconds),
            heartbeat: full_config.heartbeat.clone(),
            liveness: full_config.liveness.clone(),
            manager_node_id: manager_node_id.clone(),
            idle_sweep: Some(IdleSweepConfig {
                usage: usage_for_idle_sweep,
                interval: full_config.suspend.idle_sweep_interval(),
                make_room: full_config.suspend.make_room,
            }),
        },
    );
    let concurrency_fut =
        start_concurrency_controller(store_copy_c, full_config.eval_threads, adaptive_concurrency);
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
    let keyring_fut = start_keyring_maintenance(store_for_keyring);
    // The background loops run as one task the daemon can abort: without an
    // API and raft transport they must not keep acting on the cluster.
    let background = tokio::spawn(async move {
        let (checker, evals, concurrency, worker, rotation, keyring) = join!(
            node_checker_fut,
            eval_loop_fut,
            concurrency_fut,
            worker_fut,
            rotation_fut,
            keyring_fut
        );
        for (name, result) in [
            ("node status checker", checker),
            ("eval loop", evals),
            ("adaptive scheduler concurrency", concurrency),
            ("in-process worker", worker),
            ("workload identity rotation", rotation),
            ("keyring maintenance", keyring),
        ] {
            if let Err(err) = result {
                error!(loop_name = name, error = ?err, "Manager background loop exited");
            }
        }
    });

    // The gRPC listener is the manager: it carries the API and, in raft mode,
    // the raft transport. If it fails to bind (port taken, bad TLS material)
    // or stops serving, exit non-zero so a supervisor restarts the process,
    // rather than keep scheduling as a headless manager nobody can reach.
    tokio::pin!(start_server_fut);
    let signal = tokio::select! {
        serve_result = &mut start_server_fut => {
            background.abort();
            let err = manager_server_exit_error(&addr, serve_result);
            shutdown_persistence(&store_for_shutdown).await;
            return Err(err);
        }
        signal = shutdown_signal() => signal,
    };

    // Graceful shutdown (SIGTERM/SIGINT). Each step is bounded, so a stuck
    // stream or store can delay the exit but never prevent it. A second
    // signal skips the rest.
    info!(signal, "Manager shutdown requested; draining");
    let _ = stop_serving_tx.send(());
    tokio::select! {
        drained = tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, &mut start_server_fut) => {
            if drained.is_err() {
                warn!(
                    timeout_secs = SHUTDOWN_DRAIN_TIMEOUT.as_secs(),
                    "Open RPCs and streams did not finish in time; closing them"
                );
            }
        }
        second = shutdown_signal() => {
            warn!(signal = second, "Second shutdown signal; exiting now");
            background.abort();
            return Ok(());
        }
    }
    // Leader-only loops stop before the store does, so nothing proposes
    // writes into a raft core that is shutting down.
    background.abort();
    shutdown_persistence(&store_for_shutdown).await;
    info!("Manager stopped");
    Ok(())
}

/// How long a graceful shutdown waits for in-flight RPCs and open streams
/// (event streams, worker connections) once the listener stops accepting.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Budget for the persistence backend's orderly stop: the final streaming
/// replica sync plus lease release, or the raft lane shutdown. Drain plus
/// this stays under Kubernetes' default 30s termination grace period.
const SHUTDOWN_PERSISTENCE_TIMEOUT: Duration = Duration::from_secs(15);

/// Resolves with the signal's name on SIGTERM or SIGINT (Ctrl-C).
async fn shutdown_signal() -> &'static str {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            // Without a handler this arm must never fire; SIGTERM still works.
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

/// Runs the store's orderly persistence stop on the blocking pool, bounded
/// by [`SHUTDOWN_PERSISTENCE_TIMEOUT`] (plus a little slack, since the
/// backend's own bound is best-effort).
async fn shutdown_persistence(store: &Store) {
    let store = store.clone();
    let stop = tokio::task::spawn_blocking(move || {
        store.shutdown_persistence(SHUTDOWN_PERSISTENCE_TIMEOUT)
    });
    let slack = Duration::from_secs(2);
    if tokio::time::timeout(SHUTDOWN_PERSISTENCE_TIMEOUT + slack, stop)
        .await
        .is_err()
    {
        warn!("Persistence shutdown did not finish in time; exiting anyway");
    }
}

/// The error `run_manager` returns when the gRPC server future resolves.
/// Serving only ends on failure, so even `Ok` is an error here.
fn manager_server_exit_error(
    addr: &std::net::SocketAddr,
    result: Result<(), tonic::transport::Error>,
) -> anyhow::Error {
    let err = match result {
        Ok(()) => anyhow!("manager gRPC server on {addr} stopped unexpectedly"),
        Err(err) => {
            anyhow::Error::new(err).context(format!("manager gRPC server on {addr} failed"))
        }
    };
    error!(error = ?err, "Manager API is down; exiting");
    err
}

/// How often the control-lane leader sweeps for KV changes whose debounced
/// reconciliation was lost (docs/kv-store.md, "Debounce / coalescing"). Far
/// above the debounce window, so the sweep almost never races a live flush
/// (and a race is harmless: reconciliation is idempotent).
const KV_RECONCILE_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// How often the keyring loop ticks when nothing wakes it earlier.
const KEYRING_TICK_INTERVAL: Duration = Duration::from_secs(15);

/// Background KV keyring maintenance (docs/kv-encryption.md). On every
/// manager: unwrap data keys that arrive by replication, or a root key that
/// appears on disk, and keep the gauges fresh. On the control-lane leader
/// only: initialize the keyring once the raft upgrade gate opens, rotate the
/// data key at `dataKeyRotationAge`, rewrap values under the active key
/// (including plaintext rows from before the upgrade), and collect retired
/// keys. Woken by every replicated keyring change and by leadership
/// acquisition; ticks every 15s otherwise. Provider calls (a KMS, for
/// `exec`) run on the blocking pool, never on the raft apply thread.
async fn start_keyring_maintenance(store: Store) -> R<()> {
    let keyring = store.keyring().clone();
    loop {
        tokio::select! {
            _ = keyring.wait_for_change() => {}
            _ = tokio::time::sleep(KEYRING_TICK_INTERVAL) => {}
        }
        let tick_store = store.clone();
        match tokio::task::spawn_blocking(move || keyring_tick(&tick_store)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(error = ?err, "KV keyring maintenance tick failed"),
            Err(err) => warn!(error = ?err, "KV keyring maintenance tick panicked"),
        }
    }
}

/// One pass of [`start_keyring_maintenance`]; also what tests drive directly.
pub fn keyring_tick(store: &Store) -> R<()> {
    let keyring = store.keyring();
    let before = keyring.snapshot();

    // A raft member without the root key (it was in the cluster before KV
    // encryption arrived, so no join reply ever carried it) fetches it from
    // another member. Before the reload below, so a fetched key unlocks
    // this same tick.
    if let Err(err) = store.fetch_root_key_if_missing() {
        warn!(error = ?err, "Failed fetching the KV root key from another manager");
    }

    // Anything locked, a provider error, or an exec provider (whose reload
    // is deferred to this loop): try to unwrap again.
    let needs_reload = !before.locked.is_empty()
        || before.provider_error.is_some()
        || keyring.provider().kind() == crate::storage::keyring::PROVIDER_EXEC;
    if needs_reload {
        let unlocked = store.load_keyring()?;
        if !unlocked.is_empty() {
            info!(keys = ?unlocked, "Unwrapped KV data keys");
        }
    }
    let after = keyring.snapshot();
    if before.state == "locked" && after.state == "unlocked" {
        store.emit_event(crate::storage::events::EventDraft::info(
            "keyring.unlocked",
            "KV keyring unlocked: every data key is available on this manager",
        ));
    } else if before.state != "locked" && after.state == "locked" {
        let reasons: Vec<String> = after
            .locked
            .iter()
            .map(|locked| format!("{}: {}", locked.key_id, locked.reason))
            .collect();
        store.emit_event(
            crate::storage::events::EventDraft::warn(
                "keyring.locked",
                "KV keyring locked: a data key cannot be unwrapped on this manager",
            )
            .meta(json!({ "keys": reasons })),
        );
    }

    if keyring.enabled() && store.is_raft_leader(crate::storage::Lane::Control) {
        match store.ensure_keyring_initialized()? {
            crate::storage::store::KeyringInit::Initialized {
                key_id,
                root_key_id,
                minted_root_key,
            } => {
                if minted_root_key {
                    info!(
                        root_key_id = %root_key_id,
                        path = %keyring.provider().location(),
                        "Minted the KV root key — keep this file with your backups and on every manager"
                    );
                }
                info!(key_id = %key_id, "Initialized the KV keyring");
            }
            crate::storage::store::KeyringInit::PendingUpgrade(reason) => {
                if before.pending_upgrade.as_deref() != Some(reason.as_str()) {
                    info!("KV encryption waits for the manager upgrade to finish: {reason}");
                }
            }
            crate::storage::store::KeyringInit::NoRootKey(reason)
                if before.provider_error.is_none() =>
            {
                warn!("KV encryption cannot start: {reason}");
            }
            _ => {}
        }
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        if let Some(reason) = keyring.rotation_due(now) {
            match store.rotate_data_key(&reason) {
                Ok(_) => {}
                Err(err) => warn!(error = ?err, "Automatic KV data key rotation failed"),
            }
        }
        if keyring.snapshot().can_encrypt()
            && !keyring.rewrap_running()
            && store.kv_rows_pending_rewrap(false)? > 0
        {
            match store.run_rewrap(false) {
                Ok(run) => info!(
                    rewrapped = run.rewrapped,
                    collected = ?run.collected_keys,
                    "Background KV rewrap finished"
                ),
                Err(err) => warn!(error = ?err, "Background KV rewrap failed"),
            }
        }
    }

    store.refresh_keyring_gauges();
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
        heartbeat_source: String::new(),
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
        heartbeat_source: String::new(),
    })?;

    let job = JobConfig {
        name: "ui-preview-demo".into(),
        tasks: vec![
            TaskConfig::new("api", "noop"),
            TaskConfig::new("worker", "noop"),
        ],
        priority: Default::default(),
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

/// Everything the status checker needs from the manager config, resolved once
/// at startup so the loop itself reads no config.
#[derive(Debug, Clone)]
pub struct LivenessCheckerConfig {
    pub check_interval: Duration,
    pub heartbeat: crate::structs::HeartbeatConfigFull,
    pub liveness: crate::structs::LivenessConfigFull,
    /// This manager's own id, as the digest identifies its sender.
    pub manager_node_id: String,
    /// The idle sweep (docs/suspend-and-resume.md, "Idle policy"). `None`
    /// turns it off (embedded uses with no heartbeat service).
    pub idle_sweep: Option<IdleSweepConfig>,
}

/// What the idle sweep needs: the usage table heartbeats fill, and how
/// often to look (manager `suspend.idleSweep`).
#[derive(Debug, Clone)]
pub struct IdleSweepConfig {
    pub usage: Arc<Mutex<crate::manager::usage::UsageTable>>,
    pub interval: Duration,
    /// Also run the make-room sweep (manager `suspend.makeRoom`, docs/
    /// suspend-and-resume.md "Capacity for resumes"), which reads the same
    /// usage reports to find the quietest instances.
    pub make_room: bool,
}

/// How often the make-room sweep looks for resumes waiting on a full node.
/// One indexed read when nothing is resuming.
const MAKE_ROOM_EVERY: Duration = Duration::from_secs(5);

/// The interval this manager currently serves, and the ladder state behind it.
///
/// With adaptive cadence off (`heartbeat.adaptive = false`) the served
/// interval is just the floor — the manager tells workers nothing and they
/// keep their worker-type `heartbeatSeconds`. The value is still tracked
/// because the freshness ratio and the sizing arithmetic need an interval to
/// be "on time" relative to.
struct ServedCadence {
    ladder: crate::manager::cadence::LadderState,
    at_max: bool,
}

#[instrument(err, skip_all)]
async fn start_node_status_checker(store: Store, config: LivenessCheckerConfig) -> R<()> {
    use crate::manager::cadence::{self, LadderState};

    info!("Beginning node status checker loop");
    let check_interval = config.check_interval;
    let min_interval = config.heartbeat.parsed_min_interval();
    let max_interval = config.heartbeat.parsed_max_interval();
    let missed_beats = config.heartbeat.missed_beats_before_lost.max(1);
    let adaptive = config.heartbeat.adaptive_enabled();
    let liveness_settings = config.liveness.settings();
    store.liveness().configure(liveness_settings.clone());

    let mut cadence_state = ServedCadence {
        ladder: LadderState::new(min_interval),
        at_max: false,
    };
    // Freshness warning debounce: 3 consecutive sub-threshold ticks, then at
    // most one line a minute. A single slow tick is not a page.
    let mut low_freshness_ticks: u32 = 0;
    let mut last_freshness_warn: Option<std::time::Instant> = None;
    let mut last_checkpoint: Option<std::time::Instant> = None;
    let mut last_digest: Option<std::time::Instant> = None;
    // Starts false so PROCESS START takes the same path as an election: seed
    // the liveness table, start the grace clock, log the acquisition. Before
    // this, the grace was keyed on process uptime and a manager that had been
    // up for hours could acquire status-lane leadership mid-life and start
    // marking lost on its very next tick (liveness plan §5.4).
    let mut was_status_leader = false;
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
    let mut last_kv_sweep: Option<std::time::Instant> = None;
    // Automatic suspend: its own cadence (default 15s), independent of the
    // status checker's, and its memory of what it already tried.
    let mut last_idle_sweep: Option<std::time::Instant> = None;
    let mut idle_sweeper = crate::manager::server::IdleSweeper::default();
    let mut last_make_room: Option<std::time::Instant> = None;
    let mut make_room_state = crate::manager::server::MakeRoomState::default();
    loop {
        // Leader-only actions (raft mode, per lane): only the STATUS-lane
        // leader may mark nodes lost / sweep health orphans; non-leaders
        // idle on a short poll and take over within a few seconds of a
        // leadership change (JC-15). Non-raft backends always report
        // leadership here.
        let status_leader = store.is_raft_leader(crate::storage::Lane::Status);
        if status_leader {
            if !was_status_leader {
                // Responsibility for liveness starts HERE, not at process
                // start. A new leader inherits no liveness state, so it seeds
                // every active node at `now` and marks nothing lost for
                // `failoverGrace` while the fleet checks back in. Seeding from
                // the persisted timestamps instead would mark the whole fleet
                // lost on the first tick, because those timestamps are stale
                // by construction on a manager that has just taken over.
                let active = match store.active_node_liveness_rows() {
                    Ok(rows) => rows.into_iter().map(|row| row.id).collect::<Vec<_>>(),
                    Err(err) => {
                        warn!(error = ?err, "Failed reading active nodes to seed liveness");
                        Vec::new()
                    }
                };
                store.liveness().become_responsible(&active);
                info!(
                    seeded_nodes = active.len(),
                    failover_grace_secs = liveness_settings.failover_grace.as_secs(),
                    "Status-lane liveness responsibility acquired; marking nothing lost until \
                     the fleet has had its failover grace to check in"
                );
                was_status_leader = true;
            }
            let now = std::time::Instant::now();
            let snapshot = match liveness_snapshot(&store, now) {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    warn!(error = ?err, "Failed reading node liveness; skipping this tick");
                    tokio::time::sleep(check_interval).await;
                    continue;
                }
            };
            let peer_gap = store.liveness().peer_gap_at(now);
            let decision = cadence::decide_lost_nodes(
                &snapshot,
                &cadence::LossContext {
                    responsible_for: store.liveness().responsible_for_at(now),
                    failover_grace: liveness_settings.failover_grace,
                    peer_gap_nodes: peer_gap.suppressed.clone(),
                    ..cadence::LossContext::default()
                },
            );
            report_liveness_metrics(
                &store,
                &snapshot,
                &decision,
                &peer_gap,
                cadence_state.ladder.served,
                missed_beats,
                now,
            );
            for (reason, count) in &decision.suppressed {
                match reason {
                    cadence::SuppressionReason::MassDisruption => error!(
                        would_be_lost = count,
                        active_nodes = snapshot.len(),
                        "More than half this fleet would be marked lost in one tick; marking \
                         NONE. A control plane does not lose half its fleet in one tick — \
                         believe the control plane broke, not the fleet \
                         (docs/heartbeat-cadence-and-scale.md)"
                    ),
                    cadence::SuppressionReason::PeerDigestGap => info!(
                        suppressed = count,
                        peers = ?peer_gap.stale_peers,
                        "Holding node losses for a peer manager that has stopped reporting \
                         liveness; a manager-to-manager problem is not fleet-wide node loss"
                    ),
                    cadence::SuppressionReason::FailoverGrace => debug!(
                        suppressed = count,
                        "Inside the liveness failover grace; marking nothing lost"
                    ),
                }
            }
            let node_ids_to_update = decision.lost;

            // Warn-and-continue rather than `?`: under `lanes` topology the
            // status-lane leader may not lead the control lane, and the
            // composite's control half (`sync_node_availability`) is then
            // rejected with that lane's leader hint. The checker must keep
            // ticking — the mark is retried every tick until it lands.
            //
            // On the blocking pool: the mark is up to three quorum rounds
            // (status, control, the batched reschedule enqueue), and running
            // it inline here parked a runtime worker and stalled the tick.
            let mark_result = {
                let store_for_mark = store.clone();
                let ids_for_mark = node_ids_to_update.clone();
                tokio::task::spawn_blocking(move || {
                    store_for_mark.mark_nodes_as_lost(&ids_for_mark)
                })
                .await
                .unwrap_or_else(|join_error| {
                    Err(anyhow::anyhow!("mark-lost task failed: {join_error:?}"))
                })
            };
            if let Err(err) = mark_result {
                warn!(error = ?err, "Failed marking nodes lost; will retry next tick");
            } else if !node_ids_to_update.is_empty() {
                crate::manager::telemetry::metrics()
                    .nodes_marked_lost_total
                    .add(node_ids_to_update.len() as u64);
                for id in &node_ids_to_update {
                    store.liveness().forget(id);
                }
            }

            // Cadence: recompute what to serve, warn when the fleet is not
            // sustaining it, and keep the per-node windows honest.
            let report = cadence::cadence_report(
                &snapshot,
                cadence_state.ladder.served,
                missed_beats,
                config.heartbeat.headroom,
                HEARTBEAT_FRESHNESS_MIN_FLEET,
                adaptive,
                cadence_state.at_max,
            );
            if adaptive {
                let target = cadence::target_interval(
                    snapshot.len(),
                    config.heartbeat.target_rate_per_second,
                    config.heartbeat.headroom,
                    min_interval,
                    max_interval,
                );
                cadence_state.at_max = target >= max_interval;
                let next = cadence::advance_ladder(
                    &cadence_state.ladder,
                    target,
                    check_interval,
                    min_interval,
                    max_interval,
                );
                if next.served != cadence_state.ladder.served {
                    info!(
                        active_nodes = snapshot.len(),
                        from_seconds = cadence_state.ladder.served.as_secs(),
                        to_seconds = next.served.as_secs(),
                        "Serving a new heartbeat interval for this fleet size"
                    );
                    if next.served < cadence_state.ladder.served {
                        narrow_windows_on_confirmation(
                            &store,
                            &snapshot,
                            next.served,
                            missed_beats,
                        );
                    }
                }
                cadence_state.ladder = next;
                // Widening runs every tick, not only on a step up: a node
                // that registered after the last step up (its window comes
                // from its worker-type cadence), or whose worker type beats
                // faster than the floor, would otherwise sit on a window
                // narrower than the interval it is about to be served.
                // Widen-only, so a slower pool's wider window is untouched,
                // and a no-op once every window is wide enough.
                widen_windows_to_served(
                    &store,
                    &snapshot,
                    cadence_state.ladder.served,
                    missed_beats,
                );
                // Only a manager running adaptive cadence puts an interval on
                // the reply. With it off (`heartbeat.adaptive = false`),
                // workers keep their worker-type value and the wire field
                // stays absent, exactly as before adaptive cadence existed.
                store
                    .liveness()
                    .set_served_interval(Some(cadence_state.ladder.served));
            }
            warn_on_stale_cadence(
                &report,
                &mut low_freshness_ticks,
                &mut last_freshness_warn,
                snapshot.len(),
            );

            // Display checkpoint: the ONE heartbeat-derived write that still
            // reaches raft, and it is two commits a minute rather than one per
            // beat per node. Never read back as liveness.
            if let Some(interval) = liveness_settings.checkpoint_interval {
                if last_checkpoint.is_none_or(|at| at.elapsed() >= interval) {
                    last_checkpoint = Some(std::time::Instant::now());
                    write_liveness_checkpoint(&store, now);
                }
            }
            // Stateful allocations on lost nodes are gated behind their
            // onLost.replaceAfter loss-confirmation window (see
            // docs/stateful-workloads.md "Node loss and fencing"). Re-run the
            // reschedule path for still-lost nodes every tick so replacement
            // proceeds automatically once a window elapses — no operator
            // action needed. Blocking pool: it reads the queue and may
            // propose a scheduler-lane round.
            {
                let store_for_recheck = store.clone();
                if let Err(join_error) =
                    tokio::task::spawn_blocking(move || store_for_recheck.recheck_lost_nodes())
                        .await
                {
                    warn!("Lost-node recheck task failed: {join_error:?}");
                }
            }
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
        } else {
            if was_status_leader {
                info!("Not the status-lane raft leader; node status checker idling until elected");
                // Stop being responsible: a re-acquisition must start a FRESH
                // grace clock, not resume the one from the last term.
                store.liveness().relinquish();
                was_status_leader = false;
            }
            // Liveness digest (liveness plan §6.3): a manager that does not
            // lead the status lane still receives heartbeats — workers dial
            // one fixed manager forever — so it batches the ids it heard from
            // and sends ONE RPC per interval to the leader. The cost is
            // `managers x 1 RPC/second`, not `nodes x 1 write per interval`.
            if last_digest.is_none_or(|at| at.elapsed() >= liveness_settings.digest_interval) {
                last_digest = Some(std::time::Instant::now());
                send_liveness_digest(&store, &config.manager_node_id).await;
            }
            crate::manager::telemetry::metrics()
                .liveness_responsible_seconds
                .set(0);
            crate::manager::telemetry::metrics()
                .liveness_tracked_nodes
                .set(0);
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

        // KV-driven reconciliation safety net: debounced KV events live in
        // memory on the node that took the write, so a flush error, a crash,
        // or a leadership change inside the window would otherwise lose
        // them. The control-lane leader re-derives missed changes from
        // replicated state (`sweep_missed_kv_changes`).
        if store.is_raft_leader(crate::storage::Lane::Control)
            && last_kv_sweep.is_none_or(|at| at.elapsed() >= KV_RECONCILE_SWEEP_INTERVAL)
        {
            last_kv_sweep = Some(std::time::Instant::now());
            match crate::manager::server::sweep_missed_kv_changes(&store) {
                Ok(0) => {}
                Ok(jobs) => warn!(jobs, "Reconciled KV changes a debounced flush had missed"),
                Err(err) => warn!(error = ?err, "Failed sweeping for missed KV changes"),
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
            // `snapshots.ttl`: expired snapshots move to `deleting`, and their
            // nodes reclaim the bytes on the next heartbeat (see
            // docs/suspend-and-resume.md). A read-only check when nothing is
            // due.
            crate::manager::server::expire_snapshots(&store);
            // Resumes whose checkpoint was lost with its node, or that waited
            // out `suspend.resumeTimeout`, settle per `onRestoreFailure`. One
            // indexed read when nothing is resuming.
            crate::manager::server::settle_stalled_resumes(&store);
        }

        // Idle sweep: suspend instances quiet past their task's
        // `suspend.idle.after`. Not leader-gated: the usage it reads lives
        // on whichever manager the nodes heartbeat to, and `begin_suspend`
        // forwards to the scheduler-lane leader. An in-memory scan when
        // nothing is due.
        if let Some(idle) = config.idle_sweep.as_ref() {
            if last_idle_sweep.is_none_or(|at| at.elapsed() >= idle.interval) {
                last_idle_sweep = Some(std::time::Instant::now());
                crate::manager::server::idle_sweep(&store, &idle.usage, &mut idle_sweeper);
            }
            // Make room for resumes pinned to a full node. Not leader-gated
            // for the same reason as the idle sweep: the usage lives here,
            // and the suspends and evals it starts forward to the leader.
            if idle.make_room && last_make_room.is_none_or(|at| at.elapsed() >= MAKE_ROOM_EVERY) {
                last_make_room = Some(std::time::Instant::now());
                crate::manager::server::make_room_sweep(&store, &idle.usage, &mut make_room_state);
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
    // KV keyring gauges ride the same tick, so a locked keyring shows within
    // one scrape even between the keyring loop's own ticks.
    store.refresh_keyring_gauges();
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
    // The read model's view of the fleet next to the DB's (`legion_nodes`
    // above): a persistent gap between the two is a truncated planning
    // universe, which previously took sampling plan_submission rows at rig
    // scale to even suspect (aws-phase5-findings.md).
    match store.read_model_node_counts() {
        Ok((all, available)) => telemetry.read_model_nodes.set_all(&[
            (vec!["all".to_string()], all),
            (vec!["available".to_string()], available),
        ]),
        Err(err) => debug!(error = ?err, "Failed refreshing read-model node gauge"),
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

/// Every active node's age and window, as the lost decision sees them.
///
/// The AGE comes from the in-memory liveness table, not from the database: a
/// steady-state heartbeat no longer writes a timestamp anywhere, so the
/// persisted clock is display state (liveness plan §6.5). The WINDOW comes
/// from the replicated row, because it is a configured value rather than an
/// observation and every manager must agree on it.
///
/// A node the table has never seen — registered since this manager became
/// responsible — is seeded at `now` on first sighting and is therefore never
/// lost on the tick that discovers it.
///
/// Public for the heartbeat scale benchmark
/// (`tests/heartbeat_scale_benchmark.rs`), which times one liveness pass; not
/// part of any stable API.
pub fn liveness_snapshot(
    store: &Store,
    now: std::time::Instant,
) -> R<Vec<crate::manager::cadence::NodeLiveness>> {
    let rows = store.active_node_liveness_rows()?;
    let unix_now = OffsetDateTime::now_utc().unix_timestamp();
    let table = store.liveness();
    let active: std::collections::HashSet<String> = rows.iter().map(|row| row.id.clone()).collect();
    // Keep the table the LIVE set: nodes removed or already lost drop out, so
    // `legion_liveness_tracked_nodes` stays comparable with
    // `legion_nodes{status="active"}` instead of accumulating history.
    table.retain(&active);
    let snapshot = rows
        .into_iter()
        .map(|row| {
            let beat = table.seed_if_missing_at(&row.id, now, unix_now);
            let window = row.window();
            crate::manager::cadence::NodeLiveness {
                id: row.id,
                age: now.saturating_duration_since(beat.at),
                window,
            }
        })
        .collect();
    Ok(snapshot)
}

/// Report the nodes this manager has heard from to the status-lane leader.
///
/// Costs ONE RPC per manager per `digestInterval`, whatever the fleet size —
/// against the ~`N/interval` forwarded raft writes per second the pre-digest
/// path cost. A failure is not an error to a worker: the leader still holds
/// this peer's last digest and will not mark its nodes lost inside
/// `peerDigestGrace`, which is exactly the fail-safe the push (rather than a
/// pull) buys.
async fn send_liveness_digest(store: &Store, manager_node_id: &str) {
    use crate::storage::liveness::DigestOutcome;

    let seen = store.liveness().drain_pending_digest();
    if seen.is_empty() {
        return;
    }
    let count = seen.len();
    let outcome = store.report_node_liveness(manager_node_id, seen).await;
    let telemetry = crate::manager::telemetry::metrics();
    telemetry
        .liveness_digest_rpcs_total
        .inc(&[outcome.metric_label()]);
    telemetry.liveness_digest_nodes.observe(count as f64);
    match &outcome {
        DigestOutcome::Accepted { served_interval } => {
            store.liveness().set_digest_fallback(false);
            // Serve the leader's cadence to the workers talking to THIS
            // manager. Without the relay, adaptive cadence only reached the
            // leader's own workers, and a fleet spread over three managers
            // kept two thirds of its beats at the floor.
            store.liveness().set_served_interval(*served_interval);
            debug!(
                nodes = count,
                "Reported node liveness to the status-lane leader"
            );
        }
        // A peer that predates digests leads the lane. Fall back to forwarding
        // each heartbeat as a replicated write — today's behavior, and correct;
        // it resolves when leadership lands on an upgraded manager.
        DigestOutcome::Unimplemented => {
            if !store.liveness().digest_fallback() {
                warn!(
                    "The status-lane leader does not accept liveness digests (a manager that \
                     predates them). Falling back to a replicated write per heartbeat until the \
                     manager upgrade finishes (docs/upgrades.md)"
                );
            }
            store.liveness().set_digest_fallback(true);
        }
        DigestOutcome::NotLeader { leader_hint } => {
            debug!(%leader_hint, "Liveness digest reached a manager that no longer leads the status lane");
        }
        DigestOutcome::NoLeader | DigestOutcome::NoEngine => {}
        DigestOutcome::Error(message) => {
            debug!(%message, nodes = count, "Liveness digest failed; the leader's peer grace covers this window");
        }
    }
}

/// Publish the liveness and cadence gauges for one tick.
fn report_liveness_metrics(
    store: &Store,
    snapshot: &[crate::manager::cadence::NodeLiveness],
    decision: &crate::manager::cadence::LossDecision,
    peer_gap: &crate::storage::liveness::PeerGap,
    served_interval: Duration,
    missed_beats: u32,
    now: std::time::Instant,
) {
    let telemetry = crate::manager::telemetry::metrics();
    telemetry
        .liveness_tracked_nodes
        .set(store.liveness().tracked() as i64);
    telemetry
        .liveness_responsible_seconds
        .set(store.liveness().responsible_for_at(now).as_secs() as i64);
    telemetry
        .heartbeat_served_interval_seconds
        .set(served_interval.as_secs() as i64);
    telemetry
        .heartbeat_lost_window_seconds
        .set((served_interval * missed_beats).as_secs() as i64);
    let ratio = crate::manager::cadence::freshness_ratio(
        snapshot,
        served_interval,
        HEARTBEAT_FRESHNESS_MIN_FLEET,
    );
    telemetry
        .heartbeat_freshness_ratio
        .set(ratio.unwrap_or(0.0));
    telemetry.heartbeat_sustainable_nodes.set(
        ratio
            .map(|ratio| (snapshot.len() as f64 * ratio).round() as i64)
            .unwrap_or(0),
    );
    for (reason, count) in &decision.suppressed {
        telemetry
            .liveness_loss_suppressed_total
            .inc_by(&[reason.as_str()], *count as u64);
    }
    let mut peer_series: Vec<(Vec<String>, i64)> = peer_gap
        .stale_peers
        .iter()
        .map(|peer| (vec![peer.clone()], 1))
        .collect();
    peer_series.extend(
        peer_gap
            .fresh_peers
            .iter()
            .map(|peer| (vec![peer.clone()], 0)),
    );
    telemetry.liveness_peer_stale.set_all(&peer_series);
}

/// Fire the runtime cadence warning, debounced.
///
/// Fires only after `HEARTBEAT_FRESHNESS_WARN_TICKS` consecutive sub-threshold
/// ticks and at most once a minute, because the point is to reach an operator
/// BEFORE the collapse, not to fill the log during one.
fn warn_on_stale_cadence(
    report: &crate::manager::cadence::CadenceReport,
    low_ticks: &mut u32,
    last_warn: &mut Option<std::time::Instant>,
    active_nodes: usize,
) {
    let Some(ratio) = report.freshness_ratio else {
        *low_ticks = 0;
        return;
    };
    if ratio >= HEARTBEAT_FRESHNESS_WARN_RATIO {
        *low_ticks = 0;
        return;
    }
    *low_ticks += 1;
    if *low_ticks < HEARTBEAT_FRESHNESS_WARN_TICKS {
        return;
    }
    let due = last_warn.is_none_or(|at| {
        at.elapsed() >= Duration::from_secs(HEARTBEAT_FRESHNESS_WARN_INTERVAL_SEC)
    });
    if !due {
        return;
    }
    *last_warn = Some(std::time::Instant::now());
    warn!(
        nodes = active_nodes,
        served_interval_secs = report.served_interval.as_secs(),
        freshness = ratio,
        lost_window_secs = report.lost_window.as_secs(),
        at_max_interval = report.at_max_interval,
        "{}",
        crate::manager::cadence::freshness_warning(report)
    );
}

/// Write the display checkpoint: every tracked node's ACTUAL last observed
/// beat time, in one batched status-lane commit.
fn write_liveness_checkpoint(store: &Store, now: std::time::Instant) {
    let beats: Vec<(String, i64)> = store
        .liveness()
        .snapshot()
        .into_iter()
        .map(|(id, beat)| (id, beat.unix))
        .collect();
    if beats.is_empty() {
        return;
    }
    let count = beats.len();
    let started = std::time::Instant::now();
    match store.checkpoint_node_heartbeats(&beats) {
        Ok(()) => {
            crate::manager::telemetry::metrics()
                .liveness_checkpoint_seconds
                .observe(started.elapsed().as_secs_f64());
            debug!(
                nodes = count,
                age_secs = now.elapsed().as_secs(),
                "Replicated the liveness display checkpoint"
            );
        }
        // Never fatal and never retried tightly: a checkpoint is display
        // state, so a missed one costs a stale column for one interval.
        Err(err) => debug!(error = ?err, "Failed writing the liveness checkpoint"),
    }
}

/// Narrow lost windows when the served interval steps down.
///
/// The asymmetry is the safety argument (heartbeat-cadence plan §6.4):
///
/// - **Widening happens immediately** ([`widen_windows_to_served`]), the
///   moment the manager DECIDES to slow down and before any worker has been
///   told. A worker that never gets the message is protected.
/// - **Narrowing waits for confirmation** — a node keeps its wide window until
///   it is observed actually beating at the faster cadence. A worker that
///   missed the speed-up is never punished for obeying the last instruction it
///   received. This is Consul's session-TTL contract, per node.
fn narrow_windows_on_confirmation(
    store: &Store,
    snapshot: &[crate::manager::cadence::NodeLiveness],
    next: Duration,
    missed_beats: u32,
) {
    let table = store.liveness();
    let updates: Vec<(String, i64)> = snapshot
        .iter()
        .filter_map(|node| {
            // Narrowing: only nodes we have SEEN beat at the new, faster
            // cadence. `gap` is the interval between this node's last two
            // observed beats; 1.5x leaves room for jitter and one slow tick.
            let gap = table.last_beat(&node.id).and_then(|beat| beat.gap)?;
            if gap > next.mul_f64(1.5) {
                return None;
            }
            // A worker type configured slower than `next` keeps its own
            // cadence (the served interval is a floor), so the window
            // follows the cadence it was actually seen beating at when that
            // is the slower of the two.
            let interval = next.max(Duration::from_secs(gap.as_secs_f64().ceil() as u64));
            let window = crate::manager::cadence::lost_window_for(interval, missed_beats);
            (window < node.window).then(|| (node.id.clone(), window.as_secs() as i64))
        })
        .collect();
    write_lost_windows(store, updates, false);
}

/// Raise every node whose lost window is narrower than `missedBeats x
/// served` to that window. Never narrows: a pool whose worker type beats
/// slower than the served interval already has the wider window it needs,
/// and a worker on an older build that ignores the served interval is
/// covered before it has been told anything.
fn widen_windows_to_served(
    store: &Store,
    snapshot: &[crate::manager::cadence::NodeLiveness],
    served: Duration,
    missed_beats: u32,
) {
    let window = crate::manager::cadence::lost_window_for(served, missed_beats);
    let updates: Vec<(String, i64)> = snapshot
        .iter()
        .filter(|node| node.window < window)
        .map(|node| (node.id.clone(), window.as_secs() as i64))
        .collect();
    write_lost_windows(store, updates, true);
}

fn write_lost_windows(store: &Store, updates: Vec<(String, i64)>, widening: bool) {
    if updates.is_empty() {
        return;
    }
    let count = updates.len();
    match store.set_node_lost_windows(&updates) {
        Ok(()) => info!(
            nodes = count,
            widening, "Moved per-node lost windows with the served cadence"
        ),
        Err(err) => warn!(error = ?err, "Failed updating per-node lost windows; will retry"),
    }
}

/// The adaptive scheduler-concurrency control loop
/// (docs/adaptive-scheduler-concurrency.md).
///
/// Copies the heartbeat-cadence shape exactly: the decision is a pure function
/// ([`crate::manager::concurrency::advance`]) and this loop only collects the
/// snapshot and applies the verdict.
///
/// **Scheduler-lane-leader-only**, for the same reason the eval workers are: a
/// follower does no planning, so its window is empty and its state would be
/// noise. State resets on leadership acquisition — a new leader starts at the
/// startup limit and re-derives within ~2s, which is cheaper than replicating
/// controller state at one op per tick.
#[instrument(err, skip_all)]
async fn start_concurrency_controller(
    store: Store,
    eval_threads: usize,
    settings: crate::manager::concurrency::AdaptiveConcurrencySettings,
) -> R<()> {
    use crate::manager::concurrency::{
        self, AdjustmentDirection, ConcurrencyMode, ConcurrencyReport, ConcurrencyState, TickInput,
    };

    // Published before the first tick so eval workers take the right path from
    // their very first claim.
    concurrency::gate().set_mode(settings.mode);
    if settings.mode == ConcurrencyMode::Off {
        info!(
            eval_threads,
            "Adaptive scheduler concurrency is off; evalThreads is the fixed planner count"
        );
        return Ok(());
    }

    let (floor, ceiling) = settings.bounds(eval_threads);
    let gate = concurrency::gate();
    let mut state = ConcurrencyState::new(ceiling);
    gate.set_limit(state.limit);
    info!(
        mode = settings.mode.as_str(),
        floor,
        ceiling,
        eval_threads,
        starting_limit = state.limit,
        "Adaptive scheduler concurrency started"
    );

    let mut ticker = tokio::time::interval(concurrency::TICK_PERIOD);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_tick = std::time::Instant::now();
    let mut was_leader = false;
    loop {
        ticker.tick().await;
        let now = std::time::Instant::now();
        let elapsed = now.saturating_duration_since(last_tick);
        last_tick = now;

        if !store.is_raft_leader(crate::storage::Lane::Scheduler) {
            if was_leader {
                info!("Scheduler-lane leadership lost; concurrency controller idling");
                concurrency::clear_report();
                was_leader = false;
            }
            // Discard whatever landed in the window: it describes a stint that
            // is over, and carrying it into the next one would decide on it.
            concurrency::conflict_window().drain();
            continue;
        }
        if !was_leader {
            info!("Scheduler-lane leadership acquired; concurrency controller resuming");
            state = ConcurrencyState::new(ceiling);
            gate.set_limit(state.limit);
            concurrency::conflict_window().drain();
            was_leader = true;
            continue;
        }

        let counts = concurrency::conflict_window().drain();
        let input = TickInput {
            applied: counts.applied,
            fallback: counts.fallback,
            rejected: counts.rejected,
            elapsed,
            floor,
            ceiling,
            tuning: settings.tuning(),
        };
        let next = concurrency::advance(&state, &input);

        // `enforce` publishes the limit into the gate; `observe` computes the
        // identical value and publishes it as a shadow, which is what makes
        // observe mode useful on its own.
        if settings.mode == ConcurrencyMode::Enforce && next.limit != state.limit {
            gate.set_limit(next.limit);
        }

        let just_adjusted = next
            .last_adjustment
            .filter(|adjustment| adjustment.ticks_ago == 0);
        if let Some(adjustment) = just_adjusted {
            // At `info` only on change; the per-tick line below is `debug`.
            info!(
                direction = adjustment.direction.as_str(),
                from = adjustment.from,
                to = adjustment.to,
                ceiling,
                mode = settings.mode.as_str(),
                conflict_rate = adjustment.conflict_rate,
                goodput = next.ewma_goodput.unwrap_or(0.0),
                "Adjusted admitted planners"
            );
            crate::manager::telemetry::metrics()
                .scheduler_concurrency_adjustments_total
                .inc(&[adjustment.direction.as_str()]);
            store.events().scheduler_concurrency_adjusted(
                adjustment.direction.as_str(),
                adjustment.from,
                adjustment.to,
                ceiling,
                adjustment.conflict_rate,
                settings.mode.as_str(),
            );
            if adjustment.direction == AdjustmentDirection::Revert {
                warn!(
                    restored_to = adjustment.to,
                    goodput_before = adjustment.goodput_before,
                    goodput_after = next.ewma_goodput.unwrap_or(0.0),
                    "Reducing scheduler concurrency did not improve goodput; conflicts are \
                     capacity-driven, not concurrency-driven — add nodes"
                );
                store.events().scheduler_concurrency_saturated(
                    adjustment.to,
                    adjustment.goodput_before,
                    next.ewma_goodput.unwrap_or(0.0),
                );
            }
        } else {
            debug!(
                limit = next.limit,
                state = next.state.as_str(),
                conflict_rate = next.ewma_conflict.unwrap_or(0.0),
                applied = counts.applied,
                fallback = counts.fallback,
                rejected = counts.rejected,
                "Scheduler concurrency tick"
            );
        }

        publish_concurrency_metrics(&next);
        concurrency::publish_report(ConcurrencyReport {
            mode: settings.mode,
            limit: next.limit,
            floor,
            ceiling,
            eval_threads,
            in_flight: gate.in_flight(),
            state: next.state,
            seconds_in_state: next.seconds_in_state(),
            conflict_rate: next.ewma_conflict,
            fallback_rate: next.ewma_fallback,
            goodput: next.ewma_goodput,
            window: counts,
            last_adjustment: next.last_adjustment,
            admission_skips: concurrency::admission_skips(),
        });
        state = next;
    }
}

fn publish_concurrency_metrics(state: &crate::manager::concurrency::ConcurrencyState) {
    use crate::manager::concurrency::{self, ControllerState};
    let metrics = crate::manager::telemetry::metrics();
    metrics.scheduler_concurrency_limit.set(state.limit as i64);
    metrics
        .scheduler_concurrency_in_flight
        .set(concurrency::gate().in_flight() as i64);
    for candidate in [
        ControllerState::Steady,
        ControllerState::Throttled,
        ControllerState::Probing,
        ControllerState::Saturated,
    ] {
        metrics
            .scheduler_concurrency_state
            .set(&[candidate.as_str()], i64::from(candidate == state.state));
    }
    metrics
        .scheduler_concurrency_conflict_rate
        .set(state.ewma_conflict.unwrap_or(0.0));
    metrics
        .scheduler_concurrency_fallback_rate
        .set(state.ewma_fallback.unwrap_or(0.0));
    metrics
        .scheduler_concurrency_goodput
        .set(state.ewma_goodput.unwrap_or(0.0));
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

    // Every worker shares the queue, so each takes only its share of it —
    // see `EvalBatchLimits`.
    let batch_limits = EvalBatchLimits {
        max: MAX_EVAL_BATCH,
        workers: max_threads,
    };

    let mut join_set = JoinSet::new();
    // Before the workers: once this is installed, plan applies hand their
    // task_status projection here instead of paying a status-lane quorum
    // round inline. Installing it after would leave the first few applies
    // on the synchronous path for no reason.
    if let Some(rx) = store.install_status_projection_queue() {
        spawn_status_projection_drain(&store, rx);
    }
    for worker_id in 0..max_threads {
        spawn_eval_worker(&mut join_set, &store, worker_id, batch_limits);
    }

    let mut next_worker_id = max_threads;
    // Supervises workers forever: any worker exit/crash is immediately replaced.
    loop {
        if !restart_eval_worker_after_exit(&mut join_set, &store, &mut next_worker_id, batch_limits)
            .await
        {
            break;
        }
    }

    Ok(())
}

async fn restart_eval_worker_after_exit(
    join_set: &mut JoinSet<()>,
    store: &Store,
    next_worker_id: &mut usize,
    batch_limits: EvalBatchLimits,
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
    spawn_eval_worker(join_set, store, worker_id, batch_limits);
    true
}

/// Drain deferred task_status projections off the plan-apply path.
///
/// Each projection is a status-lane quorum round. Running it inside the
/// scheduler-lane apply meant every applied plan paid two round trips on
/// two raft groups before the eval could finish. Here it is one round per
/// DRAIN instead of one per plan: whatever has queued up while the last
/// round was in flight is merged and projected together, which is the
/// same amortization the plan-apply coalescer already gets.
///
/// Losing a batch is survivable by design — the rows are derived from
/// allocations that are already committed, and the status leader's
/// reconciler plus the workers' own reports re-derive them. That is why
/// this can be off the critical path at all.
///
/// Deliberately NOT in the eval workers' `JoinSet`: the supervisor there
/// replaces any exited member with an eval worker, so a drain that died
/// would silently become a 17th planner while every future projection
/// send failed into a closed channel (the send path then falls back to
/// the inline status-lane round, so correctness held — but the
/// off-critical-path optimization was permanently and invisibly off, and
/// the extra worker skewed the batch-fairness arithmetic). The drain body
/// is panic-free by construction — the projection itself runs under
/// `spawn_blocking` and both error arms are logged — so a plain spawn
/// needs no supervisor of its own; if it ever does exit, the closed
/// channel degrades every send to the inline path, unchanged behavior.
fn spawn_status_projection_drain(
    store: &Store,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<Vec<crate::structs::Allocation>>,
) {
    let store = store.clone();
    tokio::spawn(async move {
        info!("Task-status projection drain started");
        while let Some(first) = rx.recv().await {
            let mut batch = first;
            // Take everything else already queued. The projection dedupes
            // by task config internally, so merging is free.
            while let Ok(more) = rx.try_recv() {
                batch.extend(more);
            }
            let store_for_write = store.clone();
            let size = batch.len();
            // spawn_blocking: this is a raft propose plus a SQLite write,
            // and it must not park a runtime worker.
            match tokio::task::spawn_blocking(move || store_for_write.project_task_statuses(&batch))
                .await
            {
                Ok(Ok(())) => debug!(allocations = size, "Projected task statuses"),
                Ok(Err(e)) => warn!(allocations = size, "Task-status projection failed: {e:#}"),
                Err(e) => warn!("Task-status projection task failed: {e:?}"),
            }
        }
        info!("Task-status projection drain stopped");
    });
}

fn spawn_eval_worker(
    join_set: &mut JoinSet<()>,
    store: &Store,
    worker_id: usize,
    batch_limits: EvalBatchLimits,
) {
    // Each worker shares the same notifier. enqueue/requeue paths wake sleepers.
    let store_for_eval = store.clone();
    let notifier = store.eval_notifier();

    join_set.spawn(async move {
        eval_worker_loop(store_for_eval, notifier, worker_id, batch_limits).await
    });
}

/// One eval worker: claim a batch, take every member of it through the
/// handler, settle the batch and claim the next one — forever.
///
/// The loop works in BATCHES rather than single evals because every fixed
/// cost in the cycle is paid per round, not per eval, and the 2026-08-05
/// AWS campaign says that fixed cost is what is left: once the wake herd
/// was fixed, the only real scheduler-lane quorum traffic was
/// `complete_and_claim_next` at 44/s (against ~44 evals/s) plus ~20/s of
/// plan applies. The amortization curve agrees from the other side — same
/// 5,000-node cluster, same 16 eval threads, `web_fleet` (6 allocations per
/// eval) sustained 219 alloc/s while `dense_fleet` (44 per eval) sustained
/// 736. 7x the work per eval for 3.4x the throughput: throughput tracking
/// work-per-eval rather than work is the signature of a per-eval cost
/// dominating, so the lever is fewer cycles per eval.
///
/// What batching does NOT change: the evals in a batch are still handled
/// strictly one at a time, in claim order, each applying its own plan in its
/// own op. Only the queue's own bookkeeping — the claim round and the
/// terminal-write round — is shared.
async fn eval_worker_loop(
    store: Store,
    notifier: Arc<Notify>,
    worker_id: usize,
    batch_limits: EvalBatchLimits,
) {
    info!(worker_id, "Eval worker started");

    let mut error_backoff = ERROR_BACKOFF_MIN;
    let mut was_leader = true;
    // The evals handed back by the previous iteration's fused
    // complete-and-claim. Carrying them here is what removes the
    // scheduler-lane rounds: the round that finished the last batch also
    // claimed this one. Empty means "ask the queue".
    let mut claimed_ahead: Vec<Eval> = Vec::new();
    loop {
        // Leader-only scheduling (raft mode, JC-41): the eval queue is
        // REPLICATED — followers hold every eval row but never ACT on the
        // queue, and every claim/complete is itself a scheduler-lane raft op
        // that only the leader may propose. Followers idle on a short poll
        // and take over within a few seconds of a leadership change, at
        // which point they inherit the real pending queue (the acquisition
        // hook below only reconciles). Non-raft backends always report
        // leadership here.
        // Evals already claimed by the previous iteration's fused write
        // are handled before anything else, including the leadership gate:
        // we held leadership when they were claimed, and dropping them here
        // would leave the rows `in_flight` for `requeue_in_flight_evals` to
        // rescue later. If leadership really has moved, the terminal write
        // fails with `NotRaftLeader` and lands in the error path below,
        // which is the same recovery the unfused loop had.
        if claimed_ahead.is_empty() && !store.is_raft_leader(crate::storage::Lane::Scheduler) {
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
        // Admission control for PLANNING (docs/adaptive-scheduler-concurrency.md).
        //
        // A permit is held PER WORKER, PER BATCH. A worker takes its batch
        // through the handlers one eval at a time, so one permit per worker
        // is exactly one concurrent planner: the limit counts workers inside
        // planning work, not evals. The permit spans the whole batch,
        // including every `plan_with_conflict_retries` backoff sleep — a
        // retrying planner still counts against the budget, because its next
        // attempt is precisely the arrival the controller is spacing out.
        //
        // The gate is asked at every batch boundary, so a lowered limit lands
        // on the next batch with no thread churn: the worker pool is never
        // resized, only how many of its members may plan at once.
        let claim_started = std::time::Instant::now();
        let carried_ahead = !claimed_ahead.is_empty();
        let (batch, mut permit, throttled) = if carried_ahead {
            // Already ours, claimed by the previous round's fused
            // settle-and-claim (and already metered by that round). If any of
            // it plans, WAIT for a permit rather than run over the limit: the
            // rows are `in_flight` to us either way, and parking here holds
            // them exactly as the queue would have.
            let batch = std::mem::take(&mut claimed_ahead);
            let permit = if batch.iter().any(|eval| is_planning_kind(&eval.kind)) {
                wait_for_permit(worker_id).await
            } else {
                None
            };
            (batch, permit, false)
        } else {
            let admission = crate::manager::concurrency::gate().admit();
            let throttled = admission.is_throttled();
            // On the blocking pool, like the handler below: a claim is a
            // scheduler-lane quorum round, and running it inline here
            // parked a tokio runtime worker for the whole round trip.
            // There are only `available_parallelism()` runtime workers no
            // matter how high `evalThreads` goes, so at high thread counts
            // the claims themselves were what serialized the loop.
            let store_for_claim = store.clone();
            let claimed = tokio::task::spawn_blocking(move || {
                claim_eval_batch(&store_for_claim, throttled, batch_limits)
            })
            .await
            .unwrap_or_else(|join_error| {
                Err(anyhow::anyhow!("eval claim task failed: {join_error:?}"))
            });
            crate::manager::telemetry::metrics()
                .eval_queue_round_seconds
                .observe(&["claim"], claim_started.elapsed().as_secs_f64());
            let batch = match claimed {
                Ok(batch) => batch,
                Err(e) => {
                    error!(
                        worker_id,
                        claim_eval_ms = claim_started.elapsed().as_millis() as u64,
                        "Error claiming next eval batch: {:?}",
                        e
                    );
                    tokio::time::sleep(error_backoff).await;
                    error_backoff = std::cmp::min(error_backoff * 2, ERROR_BACKOFF_MAX);
                    continue;
                }
            };
            // An empty claim yields an empty changeset, which skips the raft
            // entry — but it still pays the capture (a savepoint on the write
            // connection) and the wake that produced it, so the empty share is
            // the cost of waking a worker for nothing — the thundering-herd
            // signal this counter exists for.
            // Batching does not change what a tick means here: it is still one
            // round, so `claimed` counts rounds that found work rather than
            // evals. That is deliberate — dividing `evals_processed_total` by
            // this counter is exactly the evals-per-round ratio batching is
            // meant to raise, and there is no other way to see it.
            crate::manager::telemetry::metrics()
                .eval_claim_total
                .inc(&[if batch.is_empty() { "empty" } else { "claimed" }]);
            (batch, admission.into_permit(), throttled)
        };

        if batch.is_empty() {
            // Nothing to hold the budget for.
            drop(permit);
            debug!(
                worker_id,
                throttled,
                claim_eval_ms = claim_started.elapsed().as_millis() as u64,
                "No eval claimed; worker entering wait path"
            );
            // Queue is empty (or only delayed work exists), so park until
            // notified or due — plus, when throttled, until a permit frees up,
            // so a worker holding no work reacts to the gate opening as fast as
            // it reacts to new work arriving.
            wait_for_next_eval(&store, &notifier, worker_id, throttled).await;
            continue;
        }
        // A batch of nothing but stops needs no slot: hand it back before the
        // handlers run so a planner can have it.
        if !batch.iter().any(|eval| is_planning_kind(&eval.kind)) {
            permit = None;
        }

        error_backoff = ERROR_BACKOFF_MIN;
        info!(
            worker_id,
            batch_size = batch.len(),
            claim_eval_ms = claim_started.elapsed().as_millis() as u64,
            "Claimed eval batch for dispatch"
        );

        // Outcomes we ACTUALLY OBSERVED, one entry per eval. An eval whose
        // handler panicked, or that leadership loss stopped us reaching, is
        // deliberately absent: `complete_evals_and_claim_next` settles only
        // what is listed, so an unobserved eval stays `in_flight` for
        // `requeue_in_flight_evals` to recover instead of having an outcome
        // invented for it. That is the per-eval isolation the single-eval
        // loop had, preserved across the batch — one member failing neither
        // completes its siblings nor completes itself.
        let mut completions: Vec<(String, EvalCompletion)> = Vec::with_capacity(batch.len());
        for (index, eval) in batch.iter().enumerate() {
            // Stop the batch the moment this node stops leading the lane.
            // Everything from here on would fail its own writes anyway, and
            // the terminal write below cannot land either — abandoning the
            // remainder to in-flight recovery is both cheaper and the only
            // outcome that cannot half-apply a batch across a leadership
            // change. Skipped for the first eval, which keeps the carried
            // batch's "we held leadership when we claimed it" contract.
            if index > 0 && !store.is_raft_leader(crate::storage::Lane::Scheduler) {
                warn!(
                    worker_id,
                    abandoned = batch.len() - index,
                    "Lost scheduler-lane leadership mid-batch; leaving the rest in flight"
                );
                break;
            }
            // Eval handling is synchronous end to end — SQLite writes,
            // raft-proposal waits, and the plan-conflict retry loop's
            // backoff sleeps (`plan_with_conflict_retries`), which can
            // hold the thread for hundreds of milliseconds per retry.
            // Run it on the blocking pool so those stalls never park a
            // tokio runtime worker; awaiting the handle keeps this
            // worker's one-eval-at-a-time ordering, batch or not.
            let store_for_handler = store.clone();
            let eval_for_handler = eval.clone();
            // Timed end to end because the plan-phase histograms only
            // cover planning: an eval also dispatches its allocations and
            // runs post-apply bookkeeping, and the difference between
            // this and `plan_phase_seconds{phase="total"}` is the only
            // way to see that half. Still PER EVAL under batching — the
            // handlers run one at a time, so a per-batch timer would blur
            // kinds together and make the histogram unreadable.
            let handle_started = std::time::Instant::now();
            let handled = tokio::task::spawn_blocking(move || {
                // The blocking pool is shared with every heartbeat's
                // `spawn_blocking`, so the wait for a pool thread is its
                // own phase: without it, a saturated pool reads as a slow
                // handler.
                crate::manager::telemetry::metrics()
                    .eval_phase_seconds
                    .observe(
                        &[&eval_for_handler.kind, "queue_wait"],
                        handle_started.elapsed().as_secs_f64(),
                    );
                resolve_claimed_eval(&store_for_handler, worker_id, &eval_for_handler)
            })
            .await;
            crate::manager::telemetry::metrics()
                .eval_handle_seconds
                .observe(&[&eval.kind], handle_started.elapsed().as_secs_f64());
            match handled {
                Ok(completion) => completions.push((eval.id.clone(), completion)),
                // The handler panicked or was cancelled. Its outcome is
                // unknown, so record nothing for it and CARRY ON with the
                // rest of the batch: its siblings' outcomes are known and
                // must not be thrown away because this one is not.
                Err(join_error) => {
                    error!(
                        worker_id,
                        eval_id = %eval.id,
                        "Eval handler task failed: {join_error:?}"
                    );
                }
            }
        }

        // One scheduler-lane round settles the whole batch and claims the
        // next one, instead of two rounds per eval. On the blocking pool
        // for the same reason as the claim above. A worker the gate refused
        // claims narrowly again here — the same kinds it was allowed this
        // round — and re-asks the gate at the top of the next.
        //
        // The terminal write is RETRIED (bounded) before the completions
        // are dropped: a transient write error on a leader that never
        // loses leadership used to abandon every handled member of the
        // batch as `in_flight` forever, because `requeue_in_flight_evals`
        // only runs at startup and on leadership ACQUISITION — a stable
        // leader has no third chance. A retry burns nothing but this
        // worker's time; giving up still leaves the batch recoverable by
        // the existing paths, and a leadership loss fails fast on its
        // first attempt (the retries hit the same typed rejection).
        let scope = claim_scope(throttled);
        let settled = completions.len();
        let mut settle_attempts = 0;
        loop {
            let store_for_settle = store.clone();
            let completions_for_attempt = completions.clone();
            let settle_started = std::time::Instant::now();
            let outcome = tokio::task::spawn_blocking(move || {
                store_for_settle.complete_evals_and_claim_next(
                    completions_for_attempt,
                    batch_limits,
                    scope,
                )
            })
            .await
            .unwrap_or_else(|join_error| {
                Err(anyhow::anyhow!("eval settle task failed: {join_error:?}"))
            });
            crate::manager::telemetry::metrics()
                .eval_queue_round_seconds
                .observe(&["settle"], settle_started.elapsed().as_secs_f64());
            match outcome {
                Ok(next) => {
                    note_narrow_claim(scope, &next);
                    claimed_ahead = next;
                    error_backoff = ERROR_BACKOFF_MIN;
                    break;
                }
                Err(e) => {
                    settle_attempts += 1;
                    // Nothing was written — the op is one changeset — so
                    // every member of the batch is still `in_flight` and
                    // recoverable.
                    error!(
                        worker_id,
                        batch_size = batch.len(),
                        settled,
                        settle_attempts,
                        "Error writing eval batch outcomes and claiming next: {:?}",
                        e
                    );
                    tokio::time::sleep(error_backoff).await;
                    error_backoff = std::cmp::min(error_backoff * 2, ERROR_BACKOFF_MAX);
                    if settle_attempts >= EVAL_SETTLE_MAX_ATTEMPTS
                        || crate::storage::raft::driver::NotRaftLeader::in_chain(&e)
                    {
                        break;
                    }
                }
            }
        }
        // The permit ends with the batch; a carried-ahead batch asks again.
        drop(permit);
    }
}

/// The scope a worker may claim in, given what the gate told it.
fn claim_scope(throttled: bool) -> ClaimScope {
    if throttled {
        ClaimScope::NonPlanning
    } else {
        ClaimScope::Any
    }
}

/// Count the evals a throttled worker took instead of the queue head
/// (`legion_scheduler_admission_skips_total`, plan §8.2). The one deviation
/// from strict queue order the admission gate introduces — a cross-class
/// inversion, never one within a class — and it must never be silent.
fn note_narrow_claim(scope: ClaimScope, batch: &[Eval]) {
    if scope == ClaimScope::NonPlanning && !batch.is_empty() {
        crate::manager::concurrency::record_admission_skips(batch.len() as u64);
    }
}

/// The claim a worker makes given what the gate told it: the queue head when
/// admitted, only kinds that cannot conflict when throttled.
///
/// Public because admission is only meaningful end to end:
/// `tests/adaptive_scheduler_concurrency.rs` drives this function rather than
/// testing a copy of it.
pub fn claim_eval_batch(store: &Store, throttled: bool, limits: EvalBatchLimits) -> R<Vec<Eval>> {
    let scope = claim_scope(throttled);
    let batch = store.claim_evals(limits, scope)?;
    note_narrow_claim(scope, &batch);
    Ok(batch)
}

/// Wait until the gate admits this worker, for a batch it already holds.
///
/// `None` when the controller is not enforcing — there is nothing to hold.
/// The poll cap exists because `Notify::notify_waiters` stores no permit: a
/// release that lands between the refusal and the wait would otherwise be
/// missed until the next one.
async fn wait_for_permit(worker_id: usize) -> Option<PlannerPermit> {
    use crate::manager::concurrency::{self, Admission};
    let mut logged = false;
    loop {
        match concurrency::gate().admit() {
            Admission::Granted(permit) => return Some(permit),
            Admission::Unrestricted => return None,
            Admission::Throttled => {
                if !logged {
                    debug!(
                        worker_id,
                        "Holding a claimed batch that plans; waiting for a planning permit"
                    );
                    logged = true;
                }
                tokio::select! {
                    _ = concurrency::gate().released() => {}
                    _ = tokio::time::sleep(THROTTLED_GATE_POLL) => {}
                }
            }
        }
    }
}

async fn wait_for_next_eval(store: &Store, notifier: &Notify, worker_id: usize, throttled: bool) {
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

    // A throttled worker that found no non-planning work is usually waiting on
    // the GATE, not on the clock — the queue head is claimable right now, it
    // just is not admissible. Returning here would spin, so the wait is capped
    // rather than skipped, and the gate's release is added to the wakeup set.
    // The cap matters because `notify_waiters` stores no permit: a release
    // between the claim attempt and the wait would otherwise be missed.
    let wait_for = if throttled {
        if wait_for.is_zero() {
            THROTTLED_GATE_POLL
        } else {
            wait_for.min(THROTTLED_GATE_POLL)
        }
    } else if wait_for.is_zero() {
        return;
    } else {
        wait_for
    };

    if throttled {
        debug!(
            worker_id,
            ?wait_for,
            "Planning is throttled and no non-planning eval is ready; waiting on the gate"
        );
        // Race wake events against timeout so workers react quickly to new
        // work — or to the gate opening — without busy polling.
        tokio::select! {
            _ = notifier.notified() => {}
            _ = crate::manager::concurrency::gate().released() => {}
            _ = tokio::time::sleep(wait_for) => {}
        }
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
/// Handle a claimed eval and write its outcome immediately, as its own
/// scheduler-lane op.
///
/// The eval worker loop uses [`resolve_claimed_eval`] instead, so it can
/// fuse this write with the next claim. This wrapper stays for callers
/// that handle a single eval outside the loop (tests, and any one-shot
/// path), where there is no following claim to fuse with.
pub fn handle_claimed_eval(store: &Store, worker_id: usize, eval: &Eval) {
    let completion = resolve_claimed_eval(store, worker_id, eval);
    if let Err(e) = store.apply_eval_completion_standalone(&eval.id, completion) {
        error!(worker_id, "Error writing eval {} outcome: {:?}", eval.id, e);
    }
}

/// Run a claimed eval and decide how it should be written back, WITHOUT
/// writing it.
///
/// Splitting the decision from the write is what lets the caller fuse the
/// terminal write with the next claim into one scheduler-lane round
/// (`Store::complete_and_claim_next`). The AWS phase-3 campaign measured
/// the scheduler lane, not the planner, as the throughput ceiling, and
/// claim+complete were two of the ~4 rounds every eval spent there.
pub fn resolve_claimed_eval(store: &Store, worker_id: usize, eval: &Eval) -> EvalCompletion {
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
            EvalCompletion::Finished {
                succeeded: true,
                event: drafts::eval_completed(eval, &message, started.elapsed()),
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
            EvalCompletion::Released {
                delay: retry_delay,
                event: None,
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
            EvalCompletion::Released {
                delay,
                event: Some(drafts::eval_requeued(eval, &e, delay)),
            }
        }
        Err(e) => {
            error!(worker_id, eval_id = %eval.id, elapsed_ms = started.elapsed().as_millis() as u64, "Error handling eval {}: {:?}", eval.id, e);
            clear_not_leader_attempts(&eval.id);
            EvalCompletion::Finished {
                succeeded: false,
                event: drafts::eval_failed(eval, &e, started.elapsed()),
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
    let rendered = crate::config_lang::eval_file(config_path).map_err(|e| {
        let err = e.context(format!("Error parsing worker config at path {config_path}"));
        error!("{err}");
        err
    })?;

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
        // Only the two configuration extensions. The directory is a normal
        // one an operator edits, so it collects `README.md` files and editor
        // swap files, and neither should be fed to a config parser.
        if !crate::config_lang::is_config_file(&path) {
            debug!(path = %path.display(), "skipping non-config file in worker config directory");
            continue;
        }
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

        // Fail fast on malformed fetch and content settings (bad humantime /
        // byte-size strings) at manager startup, not on the worker.
        if let Some(config) = wcp.artifacts.as_ref() {
            config
                .fetch_with_defaults()
                .with_context(|| format!("invalid artifacts config in worker config {path_str}"))?;
        }
        if let Some(content) = wcp.content.as_ref() {
            content
                .resolve()
                .with_context(|| format!("invalid content config in worker config {path_str}"))?;
        }
        if let Some(target) = wcp.content_store.as_ref() {
            target.validate().with_context(|| {
                format!("invalid contentStore config in worker config {path_str}")
            })?;
        }
        // The top-level `content` and `contentStore` blocks ride in the
        // artifacts payload (see `WorkerArtifactsPayload`).
        let artifacts =
            if wcp.artifacts.is_none() && wcp.content.is_none() && wcp.content_store.is_none() {
                String::new()
            } else {
                serde_json::to_string(&crate::structs::WorkerArtifactsPayload {
                    artifacts: wcp.artifacts.clone().unwrap_or_default(),
                    content: wcp.content.clone(),
                    content_store: wcp.content_store.clone(),
                })?
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
        // An absent block still sends the defaults, so the worker gets the
        // type's `sandbox` settings and enforcement stays on.
        let resource_enforcement = {
            let mut enforcement = wcp.resource_enforcement.clone().unwrap_or_default();
            if enforcement.cgroup_parent.trim().is_empty() {
                bail!(
                    "worker config {path_str}: resourceEnforcement.cgroupParent must not be empty"
                );
            }
            if enforcement.cgroup_parent.starts_with('/') {
                bail!(
                    "worker config {path_str}: resourceEnforcement.cgroupParent is relative to \
                     /sys/fs/cgroup, got {:?}",
                    enforcement.cgroup_parent
                );
            }
            enforcement
                .validate()
                .with_context(|| format!("invalid worker config {path_str}"))?;
            if let Some(sandbox) = wcp.sandbox.as_ref() {
                if sandbox.vmm_overhead_mib == Some(0) {
                    bail!(
                        "worker config {path_str}: sandbox.vmmOverheadMiB must be at least 1; a \
                         VMM process always needs some memory of its own"
                    );
                }
                enforcement.sandbox = Some(sandbox.clone());
            }
            serde_json::to_string(&enforcement)?
        };

        // Edge-boot settings ride as JSON too. Absent means "no edge starts",
        // which is the default and the thing an operator has to opt out of.
        let edge_boot = match wcp.edge_boot.as_ref() {
            None => String::new(),
            Some(block) => serde_json::to_string(&block.with_defaults())?,
        };

        if let Some(seconds) = wcp.heartbeat_seconds {
            // The clamp widened from 1..=30 the moment the lost window began
            // moving with the cadence (heartbeat-cadence plan §7). Under the
            // old fixed 10s window every value above 3 traded detection margin
            // for load; now `missedBeatsBeforeLost x heartbeatSeconds` IS the
            // window, so a slow worker type is a supported configuration
            // rather than a trap.
            if !(1..=MAX_WORKER_HEARTBEAT_SECONDS).contains(&seconds) {
                bail!(
                    "worker config {path_str}: heartbeatSeconds must be between 1 and \
                     {MAX_WORKER_HEARTBEAT_SECONDS}, got {seconds}"
                );
            }
            let missed = config
                .with_defaults()
                .heartbeat
                .missed_beats_before_lost
                .max(1);
            let window = u64::from(seconds) * u64::from(missed);
            // The §1 hazard, caught at config load rather than discovered as
            // spontaneous stateful task death: a worker kills its own stateful
            // tasks after `selfTerminateAfter` without manager contact, so a
            // lost window at or past that fence means a HEALTHY worker
            // self-fences between beats.
            let self_terminate = crate::structs::job::DEFAULT_SELF_TERMINATE_AFTER.as_secs();
            if window >= self_terminate {
                bail!(
                    "worker config {path_str}: heartbeatSeconds {seconds} x \
                     missedBeatsBeforeLost {missed} is a {window}s lost window, at or past the \
                     {self_terminate}s stateful self-fence (stateful.onLost.selfTerminateAfter). \
                     A healthy worker would kill its own stateful tasks between beats. Lower \
                     heartbeatSeconds, lower missedBeatsBeforeLost, or raise \
                     selfTerminateAfter on the jobs that run here \
                     (docs/heartbeat-cadence-and-scale.md)"
                );
            }
            info!(
                worker_type = %wcp.worker_type,
                heartbeat_seconds = seconds,
                lost_window_seconds = window,
                missed_beats = missed,
                "Worker type heartbeat cadence: real node failures on this worker type take \
                 up to the lost window to detect"
            );
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
            edge_boot,
        };

        store.insert_worker_type(&wc)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::ManagerConfigFull;
    use tempfile::Builder;

    /// Isolated manager config on `port`: temp store, exec and telemetry
    /// listeners off so only the gRPC port is in play.
    fn isolated_manager_config(port: u16, store_dir: &std::path::Path) -> R<ManagerConfig> {
        Ok(serde_json::from_value(serde_json::json!({
            "server": { "host": "127.0.0.1", "port": port },
            "storePath": store_dir.to_str().context("temp path is not UTF-8")?,
            "exec": { "enabled": false },
            "telemetry": { "enabled": false },
        }))?)
    }

    /// The shipped worker config documents the content defaults without
    /// overriding them: its budgets stay unset, so the clamped default
    /// shares apply (an explicit "20%" would drop the clamp).
    #[test]
    fn the_default_worker_config_keeps_the_default_content_budgets_test() -> R<()> {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/configs/workers/default.ncl");
        let config = parse_worker_config(path)?;
        let settings = config
            .content
            .context("content block")?
            .resolve()?
            .context("enabled")?;
        assert_eq!(
            settings.budgets,
            crate::content_store::ContentBudgetSpecs::default()
        );
        Ok(())
    }

    /// M2 regression: a manager whose gRPC listener cannot bind must exit
    /// with an error instead of running its eval loop and node checker
    /// headless, with no API and no raft transport.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manager_exits_with_error_when_grpc_bind_fails_test() -> R<()> {
        let taken = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = taken.local_addr()?.port();
        let store_dir = Builder::new().prefix("legion-bind-fail-").tempdir()?;
        let config = isolated_manager_config(port, store_dir.path())?;

        let result = tokio::time::timeout(Duration::from_secs(60), run_manager(config, false))
            .await
            .context("run_manager kept running after its gRPC bind failed")?;
        let err = result.expect_err("a failed bind must fail run_manager");
        assert!(
            format!("{err:#}").contains("manager gRPC server on"),
            "unexpected error: {err:#}"
        );
        drop(taken);
        Ok(())
    }

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
            heartbeat_source: String::new(),
        }
    }

    /// A manager that has just become responsible for liveness must not
    /// deschedule a fleet whose persisted heartbeats are stale by
    /// construction.
    ///
    /// This is the test the old process-uptime grace only half passed: it
    /// covered a process RESTART, but not a manager up for hours that acquires
    /// status-lane leadership mid-life (liveness plan §5.4). Both take the
    /// same path now, because responsibility — not uptime — starts the clock.
    #[test]
    fn acquiring_liveness_responsibility_marks_nothing_lost() -> R<()> {
        let store = setup_store()?;
        store.insert_node(node_msg("stale-node", rfc3339_seconds_ago(3600)?))?;
        store.insert_node(node_msg("fresh-node", rfc3339_seconds_ago(0)?))?;

        // Acquisition seeds every active node at `now`, so a persisted
        // heartbeat an hour old produces no candidates at all...
        store
            .liveness()
            .become_responsible(&["stale-node".into(), "fresh-node".into()]);
        let snapshot = liveness_snapshot(&store, std::time::Instant::now())?;
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot
            .iter()
            .all(|node| node.age < Duration::from_secs(1)));
        let settled = crate::manager::cadence::LossContext {
            responsible_for: Duration::from_secs(3600),
            ..Default::default()
        };
        assert!(
            crate::manager::cadence::decide_lost_nodes(&snapshot, &settled)
                .lost
                .is_empty(),
            "a stale persisted timestamp must never be a loss decision"
        );

        // ...and even a genuinely silent node is held while the failover
        // grace runs.
        let long_ago = std::time::Instant::now() - Duration::from_secs(600);
        store.liveness().observe_at(
            "stale-node",
            long_ago,
            OffsetDateTime::now_utc().unix_timestamp() - 600,
        );
        let snapshot = liveness_snapshot(&store, std::time::Instant::now())?;
        let in_grace = crate::manager::cadence::LossContext {
            responsible_for: Duration::from_secs(1),
            ..Default::default()
        };
        let decision = crate::manager::cadence::decide_lost_nodes(&snapshot, &in_grace);
        assert!(decision.lost.is_empty());
        assert_eq!(
            decision.suppressed_count(crate::manager::cadence::SuppressionReason::FailoverGrace),
            1
        );

        // Past the grace, silence means real loss — and only for the silent
        // node.
        assert_eq!(
            crate::manager::cadence::decide_lost_nodes(&snapshot, &settled).lost,
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
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "restart-survivor".into(),
            tasks: vec![TaskConfig::new("svc", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.ncl".into(),
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

        let settled = crate::manager::cadence::LossContext {
            responsible_for: Duration::from_secs(3600),
            ..Default::default()
        };

        // First status-checker tick of the freshly restarted manager: the
        // node must stay active and keep its allocation.
        store
            .liveness()
            .become_responsible(&["node-1".into(), "node-2".into()]);
        let snapshot = liveness_snapshot(&store, std::time::Instant::now())?;
        let to_mark = crate::manager::cadence::decide_lost_nodes(&snapshot, &settled).lost;
        store.mark_nodes_as_lost(&to_mark)?;
        assert_eq!(store.get_node("node-1")?.status, "active");
        assert_eq!(
            store.allocs_for_node("node-1")?.len(),
            1,
            "allocation must survive a manager restart"
        );

        // The node still never heartbeated after the manager came back: now
        // it is genuinely lost and its allocations reschedule off it.
        let long_ago = std::time::Instant::now() - Duration::from_secs(600);
        store.liveness().observe_at(
            "node-1",
            long_ago,
            OffsetDateTime::now_utc().unix_timestamp() - 600,
        );
        let snapshot = liveness_snapshot(&store, std::time::Instant::now())?;
        let to_mark = crate::manager::cadence::decide_lost_nodes(&snapshot, &settled).lost;
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

    /// Two worker types with different cadences coexist, each with its own
    /// window. Impossible under the old global cutoff, and the reason the
    /// per-node column exists (heartbeat-cadence plan §6.4(4)).
    #[test]
    fn per_worker_type_cadences_get_their_own_windows() -> R<()> {
        let store = setup_store()?;
        let mut fast = node_msg("fast-node", rfc3339_seconds_ago(0)?);
        fast.config_attrs = json!({ "heartbeatSeconds": 2 }).to_string();
        let mut slow = node_msg("slow-node", rfc3339_seconds_ago(0)?);
        slow.config_attrs = json!({ "heartbeatSeconds": 60 }).to_string();
        store.insert_node(fast)?;
        store.insert_node(slow)?;

        store
            .liveness()
            .become_responsible(&["fast-node".into(), "slow-node".into()]);
        // Both silent for 55 seconds: the 2s node is long gone, the 60s node
        // has not even missed a beat.
        let unix = OffsetDateTime::now_utc().unix_timestamp() - 55;
        let silent_since = std::time::Instant::now() - Duration::from_secs(55);
        store.liveness().observe_at("fast-node", silent_since, unix);
        store.liveness().observe_at("slow-node", silent_since, unix);

        let snapshot = liveness_snapshot(&store, std::time::Instant::now())?;
        let windows: HashMap<String, Duration> = snapshot
            .iter()
            .map(|node| (node.id.clone(), node.window))
            .collect();
        assert_eq!(windows["fast-node"], Duration::from_secs(10));
        assert_eq!(windows["slow-node"], Duration::from_secs(300));
        let settled = crate::manager::cadence::LossContext {
            responsible_for: Duration::from_secs(3600),
            ..Default::default()
        };
        assert_eq!(
            crate::manager::cadence::decide_lost_nodes(&snapshot, &settled).lost,
            vec!["fast-node".to_string()]
        );
        Ok(())
    }

    fn windows_by_node(store: &Store) -> R<HashMap<String, Duration>> {
        Ok(liveness_snapshot(store, std::time::Instant::now())?
            .into_iter()
            .map(|node| (node.id, node.window))
            .collect())
    }

    fn insert_node_beating_every(store: &Store, id: &str, seconds: u64) -> R<()> {
        let mut node = node_msg(id, rfc3339_seconds_ago(0)?);
        node.config_attrs = json!({ "heartbeatSeconds": seconds }).to_string();
        store.insert_node(node)?;
        Ok(())
    }

    /// Widening only ever raises a window: a worker type faster than the
    /// served interval (or one that registered before the manager told it
    /// anything) is covered, and a slower pool's wider window is untouched.
    #[test]
    fn widening_raises_narrow_windows_and_never_lowers_wide_ones_test() -> R<()> {
        let store = setup_store()?;
        insert_node_beating_every(&store, "one-sec", 1)?;
        insert_node_beating_every(&store, "two-sec", 2)?;
        insert_node_beating_every(&store, "batch", 60)?;
        let now = std::time::Instant::now();

        // The 2s floor a small fleet is served under the default config.
        let snapshot = liveness_snapshot(&store, now)?;
        widen_windows_to_served(&store, &snapshot, Duration::from_secs(2), 5);
        let windows = windows_by_node(&store)?;
        assert_eq!(windows["one-sec"], Duration::from_secs(10));
        assert_eq!(windows["two-sec"], Duration::from_secs(10));
        assert_eq!(windows["batch"], Duration::from_secs(300));

        // A 20k-node fleet's 21s rung at the default rate.
        let snapshot = liveness_snapshot(&store, now)?;
        widen_windows_to_served(&store, &snapshot, Duration::from_secs(21), 5);
        let windows = windows_by_node(&store)?;
        assert_eq!(windows["two-sec"], Duration::from_secs(105));
        assert_eq!(windows["batch"], Duration::from_secs(300));
        Ok(())
    }

    /// Narrowing waits for a beat at the faster cadence, and never narrows
    /// below the cadence the node was actually seen beating at.
    #[test]
    fn narrowing_follows_the_observed_cadence_test() -> R<()> {
        let store = setup_store()?;
        for (id, seconds) in [("fast", 2), ("three", 3), ("slow", 2)] {
            insert_node_beating_every(&store, id, seconds)?;
        }
        let now = std::time::Instant::now();
        let snapshot = liveness_snapshot(&store, now)?;
        widen_windows_to_served(&store, &snapshot, Duration::from_secs(8), 5);

        let unix = OffsetDateTime::now_utc().unix_timestamp();
        let table = store.liveness();
        for (id, gap) in [("fast", 2), ("three", 3), ("slow", 8)] {
            table.observe_at(id, now - Duration::from_secs(gap), unix - gap as i64);
            table.observe_at(id, now, unix);
        }
        let snapshot = liveness_snapshot(&store, now)?;
        narrow_windows_on_confirmation(&store, &snapshot, Duration::from_secs(2), 5);
        let windows = windows_by_node(&store)?;
        assert_eq!(windows["fast"], Duration::from_secs(10));
        // Seen at 3s (a 3s worker type keeps its own cadence over a 2s
        // floor): 5 x 3s, not 5 x 2s.
        assert_eq!(windows["three"], Duration::from_secs(15));
        // Not yet seen at the faster cadence: keeps the wide window.
        assert_eq!(windows["slow"], Duration::from_secs(40));
        Ok(())
    }

    async fn served_after_one_tick(
        heartbeat: crate::structs::HeartbeatConfigFull,
    ) -> R<Option<Duration>> {
        let store = setup_store()?;
        for index in 0..30 {
            insert_node_beating_every(&store, &format!("node-{index}"), 2)?;
        }
        let checker = tokio::spawn(start_node_status_checker(
            store.clone(),
            LivenessCheckerConfig {
                check_interval: Duration::from_millis(20),
                heartbeat,
                liveness: crate::structs::LivenessConfigFull::default(),
                manager_node_id: "test-manager".into(),
                idle_sweep: None,
            },
        ));
        tokio::time::sleep(Duration::from_millis(300)).await;
        checker.abort();
        let served = store.liveness().served_interval();
        let windows = windows_by_node(&store)?;
        assert!(windows
            .values()
            .all(|window| *window == Duration::from_secs(10)));
        Ok(served)
    }

    /// The default config runs adaptive cadence, and a small fleet is served
    /// the 2s floor with the historical 10s window.
    #[tokio::test]
    async fn default_config_serves_the_floor_to_a_small_fleet_test() -> R<()> {
        let served = served_after_one_tick(crate::structs::HeartbeatConfigFull::default()).await?;
        assert_eq!(served, Some(Duration::from_secs(2)));
        Ok(())
    }

    /// `heartbeat.adaptive = false` is the old fixed behavior: nothing on the
    /// reply, workers keep their worker-type cadence, windows unchanged.
    #[tokio::test]
    async fn adaptive_false_serves_nothing_test() -> R<()> {
        let served = served_after_one_tick(crate::structs::HeartbeatConfigFull {
            adaptive: false,
            ..Default::default()
        })
        .await?;
        assert_eq!(served, None);
        Ok(())
    }

    #[tokio::test]
    async fn wait_for_next_eval_wakes_immediately_after_enqueue() -> R<()> {
        let store = setup_store()?;
        let notifier = store.eval_notifier();

        let wait_task = tokio::spawn({
            let store = store.clone();
            let notifier = notifier.clone();
            async move { wait_for_next_eval(&store, &notifier, 0, false).await }
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
        wait_for_next_eval(&store, &notifier, 0, false).await;
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
        let restarted = restart_eval_worker_after_exit(
            &mut join_set,
            &store,
            &mut next_worker_id,
            EvalBatchLimits {
                max: MAX_EVAL_BATCH,
                workers: 1,
            },
        )
        .await;

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
