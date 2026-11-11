//! The worker's registration and heartbeat loop against a real manager on a
//! loopback port: rejoining on a persisted node token, terminal reports, and
//! the desired set (docs/architecture-managers-workers.md).

use legion::prelude::*;
use legion::structs::token::JoinToken;
use legion::structs::{manager_client::ManagerClient, WorkerConfig};
use legion::worker::daemon::*;
use legion::worker::{
    forwarder::UpstreamForwarder,
    identity::{ManagerTokenIssuer, WorkerIdentityManager},
    netd::Netd,
    state::WorkerStateStore,
    task_manager::WorkerTaskManager,
};
use std::time::Duration;
use tempfile::TempDir;
use tonic::transport::Channel;

/// Spot-check P0-8: a restarted worker registers with the node token it
/// persisted, so it comes back after its join token is gone; a refused
/// node token falls back to the join token; and the secret on disk is
/// owner-only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_worker_rejoins_on_its_persisted_node_token() -> R<()> {
    let dir = TempDir::new()?;
    let store = legion::storage::store::Store::start_storage(
        &legion::structs::config::ManagerConfigFull {
            store_path: dir.path().join("store").to_string_lossy().into_owned(),
            ..Default::default()
        },
    )?;
    store.insert_worker_type(&WorkerConfig {
        name: "default".into(),
        pool: "default".into(),
        ..Default::default()
    })?;
    let auth = Arc::new(legion::manager::acl::Authenticator::from_config(
        &legion::structs::config::AclConfig {
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
    let service = legion::structs::manager_server::ManagerServer::new(
        legion::manager::server::ManagerDaemonService::with_acl(store.clone(), auth),
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
        join_attributes: HashMap::new(),
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
    let fallback = Arc::new(WorkerCredentials::with_join_secret(Some(
        join_secret.clone(),
    )));
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

    // A lost node is rejoined only with a token for the pool it was in: a
    // token pinned to `default` may not claim a node lost from `gpu`.
    store.insert_node(legion::structs::NodeMsg {
        id: "w2".into(),
        pool: "gpu".into(),
        status: "lost".into(),
        availability: "available".into(),
        config_attrs: "{}".into(),
        fingerprint_attrs: "{}".into(),
        dynamic_attrs: "{}".into(),
        health_checks: "[]".into(),
        last_heartbeat: "2026-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
    })?;
    let other_pool = RegistrationPayload {
        name: "w2".into(),
        pool: "default".into(),
        config_attrs: "{}".into(),
        fingerprint_attrs: "{}".into(),
        health_checks: "[]".into(),
        capabilities: Vec::new(),
        join_attributes: HashMap::new(),
    };
    let joiner = Arc::new(WorkerCredentials::with_join_secret(Some(join_secret)));
    let err = register_self(client.clone(), &other_pool, &joiner)
        .await
        .expect_err("a join token may not rejoin a node lost from another pool");
    let status = err
        .downcast_ref::<tonic::Status>()
        .context("the refusal is the manager's status")?;
    assert_eq!(status.code(), tonic::Code::PermissionDenied, "{err:#}");
    assert!(
        status.message().contains("is lost from pool \"gpu\""),
        "{err:#}"
    );

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
    legion::storage::store::Store,
)> {
    let store = legion::storage::store::Store::start_storage(
        &legion::structs::config::ManagerConfigFull {
            store_path: dir.path().join("store").to_string_lossy().into_owned(),
            ..Default::default()
        },
    )?;
    store.insert_node(legion::structs::NodeMsg {
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
    let service = legion::structs::manager_server::ManagerServer::new(
        legion::manager::server::ManagerDaemonService::new(store.clone()),
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
    node_identity: Arc<legion::worker::plane::NodeIdentity>,
    identity_manager: Arc<WorkerIdentityManager>,
    credentials: Arc<WorkerCredentials>,
    netd: Arc<Netd>,
    _identity_dir: TempDir,
}

impl BeatHarness {
    fn new(node_id: &str, client: &ManagerClient<Channel>) -> R<Self> {
        let credentials = Arc::new(WorkerCredentials::default());
        let identity_manager = Arc::new(WorkerIdentityManager::new(Arc::new(ManagerTokenIssuer {
            client: client.clone(),
            node_id: node_id.into(),
            credentials: credentials.clone(),
        })));
        let identity_dir = tempfile::tempdir()?;
        let node_identity =
            legion::worker::plane::NodeIdentity::load(identity_dir.path(), node_id, None, None)?;
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
    use legion::manager::scheduler::{PlanMode, Scheduler};
    use legion::structs::{
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

    let step = |name: &str| legion::structs::TaskConfig {
        lifecycle: TaskLifecycle::Batch,
        group: "pipeline".into(),
        config: json!({"command": ["true"]}),
        ..legion::structs::TaskConfig::new(name, "exec")
    };
    let job = legion::structs::JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "dag".into(),
        tasks: vec![
            step("fetch"),
            legion::structs::TaskConfig {
                depends_on: vec![TaskDependency {
                    task: "fetch".into(),
                    condition: TaskDependencyCondition::Success,
                }],
                ..step("validate")
            },
        ],
        version: None,
        pending_extensions: Default::default(),
        extensions: Default::default(),
    };
    store.handle_job_submit(
        &job,
        &[legion::structs::Submission {
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
    legion::worker::task_supervisor::handle_exit(&task_manager, exit).await;
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
    let released = Scheduler::new(&store)?.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
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
    let exit = legion::worker::task_manager::TaskExit {
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
        let allocation = legion::structs::Allocation {
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
        let task = legion::structs::TaskConfig {
            config: json!({"command": ["sleep", "30"]}),
            lifecycle: legion::structs::TaskLifecycle::Batch,
            ..legion::structs::TaskConfig::new("fetch", "exec")
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
        let allocation = legion::structs::Allocation {
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
        let task = legion::structs::TaskConfig {
            config: json!({"command": ["sleep", "30"]}),
            ..legion::structs::TaskConfig::new("server", "exec")
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
        legion::worker::plane::NodeIdentity::load(identity_dir.path(), node_id, None, None)?;
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
