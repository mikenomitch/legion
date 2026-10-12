//! Blocked evals: work that could not place waits under what blocked it,
//! and only capacity that could help wakes it (sandbox-scaling plan §6.15,
//! Phase 3; docs/scheduler-and-eval-loop.md, "Blocked evals").
//!
//! The manager runs as the real RPC service over a local store, the eval
//! worker is the daemon loop's write sequence (`claim_evals`,
//! `resolve_claimed_eval`, `complete_evals_and_claim_next`), and the wake
//! pass is the leader's (`Store::wake_blocked_evals`), each driven from the
//! test thread so every step is observable. The last test runs the same
//! story across a scheduler-lane failover on a 3-manager raft cluster.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use legion::manager::blocked::PlacementBlocked;
use legion::manager::daemon::resolve_claimed_eval;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::events::drafts;
use legion::storage::store::{BlockedEval, ClaimScope, EvalBatchLimits, EvalCompletion, Store};
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::{JobConfig, StatefulConfig, TaskConfig, TaskCount};
use legion::structs::manager_server::Manager;
use legion::structs::{GetJobInfoRequest, InstanceMode, Node, NodeMsg, Submission};
use tonic::Request;

const JOB: &str = "agents";
const TASK: &str = "workspace";

const LIMITS: EvalBatchLimits = EvalBatchLimits {
    max: 16,
    workers: 1,
};

/// A node with room for exactly one 3,000 MiB task.
fn node(id: &str, pool: &str) -> Node {
    Node {
        id: id.to_string(),
        pool: pool.into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({ "node_id": id, "maxAllocations": 10, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": "4294967296",
            "cpus": { "cpu-1": { "frequency": 2400.0 } }
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

fn keyed_job(pool: Option<&str>) -> JobConfig {
    JobConfig {
        priority: Default::default(),
        name: JOB.to_string(),
        tasks: vec![TaskConfig {
            resources: json!({ "memory": 3000, "cpu": 500 }),
            permit: pool
                .map(|pool| json!({ "pool": pool }))
                .unwrap_or(json!({})),
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Keyed,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
                envelope: None,
                promote: None,
                fence_after: None,
            }),
            ..TaskConfig::new(TASK, "noop")
        }],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    }
}

fn submit(store: &Store, job: &JobConfig) -> R<()> {
    store.handle_job_submit(
        job,
        &[Submission {
            path: format!("{}.json", job.name),
            content: "{}".to_string(),
            is_primary: true,
        }],
    )?;
    Ok(())
}

fn run_eval_worker(store: &Store) -> R<()> {
    let mut batch = store.claim_evals(LIMITS, ClaimScope::Any)?;
    while !batch.is_empty() {
        let completions = batch
            .iter()
            .map(|eval| (eval.id.clone(), resolve_claimed_eval(store, 0, eval)))
            .collect();
        batch = store.complete_evals_and_claim_next(completions, LIMITS, ClaimScope::Any)?;
    }
    Ok(())
}

/// `CreateInstance` as the RPC does it: the row, then a reschedule.
fn create(store: &Store, id: &str) -> R<()> {
    store.create_keyed_instance(JOB, "default", TASK, Some(id))?;
    let config_id = store.get_job_config(JOB)?.id();
    store.enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
    Ok(())
}

fn placed(store: &Store, id: &str) -> R<bool> {
    Ok(store
        .get_instance(id)?
        .context("instance")?
        .current_alloc_id
        .is_some())
}

/// What worker registration does after inserting the node.
fn join(store: &Store, id: &str, pool: &str) -> R<()> {
    store.insert_node(NodeMsg::from(node(id, pool)))?;
    store.request_capacity_reconcile()?;
    Ok(())
}

fn job_evals(store: &Store) -> R<Vec<(String, String)>> {
    Ok(store
        .get_evals()?
        .into_iter()
        .filter(|eval| eval.job_id == JOB)
        .map(|eval| (eval.id, eval.status))
        .collect())
}

struct Cluster {
    _dir: tempfile::TempDir,
    svc: ManagerDaemonService,
}

impl Cluster {
    fn new() -> R<Self> {
        let dir = tempfile::tempdir()?;
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.path().to_str().context("store path")?.to_string(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        })?;
        Ok(Cluster {
            _dir: dir,
            svc: ManagerDaemonService::new(store),
        })
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    /// Run the eval worker, then a wake pass, until neither has work: what
    /// the daemon's loops do between them. Returns how many evals woke.
    fn settle(&self) -> R<usize> {
        let mut woken = 0;
        loop {
            run_eval_worker(self.store())?;
            let now = self.store().wake_blocked_evals()?;
            if now == 0 {
                return Ok(woken);
            }
            woken += now;
        }
    }

    fn blocked(&self) -> R<Vec<BlockedEval>> {
        self.store().blocked_evals()
    }

    fn job_info(&self) -> R<legion::structs::Job> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let reply = runtime
            .block_on(self.svc.get_job_info(Request::new(GetJobInfoRequest {
                name: JOB.to_string(),
            })))?
            .into_inner();
        reply.job.context("job")
    }
}

/// A keyed instance that finds no room blocks under its pool's class, and
/// `job info` says so. A node joining another pool neither wakes it nor
/// re-plans the job; a node joining its pool wakes it, and it places.
#[test]
fn a_job_blocked_on_a_pool_wakes_only_for_that_pool() -> R<()> {
    let cluster = Cluster::new()?;
    let store = cluster.store();
    store.insert_node(NodeMsg::from(node("a-1", "agents")))?;
    submit(store, &keyed_job(Some("agents")))?;
    cluster.settle()?;
    create(store, "user-1")?;
    cluster.settle()?;
    assert!(placed(store, "user-1")?, "the first instance fits");

    create(store, "user-2")?;
    assert_eq!(cluster.settle()?, 0);
    assert!(!placed(store, "user-2")?);
    let blocked = cluster.blocked()?;
    assert_eq!(blocked.len(), 1, "the eval blocked instead of failing");
    assert_eq!(blocked[0].key, "class:pool=agents");
    assert_eq!(blocked[0].job_id, JOB);
    let job = cluster.job_info()?;
    assert_eq!(
        job.status, "running",
        "user-1 runs, so the derived status stays running"
    );
    let detail = job.blocked.context("the blocked block")?;
    assert_eq!(detail.key, "class:pool=agents");
    assert_eq!(detail.task, TASK);
    assert_eq!(
        detail.shortfall.get("memory").map(String::as_str),
        Some("3000MiB")
    );
    assert_eq!(
        detail.shortfall.get("cpu").map(String::as_str),
        Some("500MHz")
    );
    assert!(
        detail.message.contains("Failed to place task"),
        "{}",
        detail.message
    );
    assert!(detail.since > 0);

    // Another pool's capacity: the job is neither woken nor re-planned by
    // the cluster-wide capacity pass.
    join(store, "o-1", "other")?;
    assert_eq!(cluster.settle()?, 0, "a node in another pool wakes nothing");
    assert_eq!(
        job_evals(store)?,
        vec![(blocked[0].id.clone(), "blocked".to_string())],
        "no other eval ran for the job"
    );

    join(store, "a-2", "agents")?;
    assert_eq!(cluster.settle()?, 1, "a node in its pool wakes it");
    assert!(placed(store, "user-2")?, "and it places there");
    assert!(cluster.blocked()?.is_empty());
    assert!(cluster.job_info()?.blocked.is_none());
    Ok(())
}

/// Capacity freed on a node of the blocked eval's class wakes it.
#[test]
fn freed_capacity_wakes_a_job_blocked_on_its_class() -> R<()> {
    let cluster = Cluster::new()?;
    let store = cluster.store();
    store.insert_node(NodeMsg::from(node("n-1", "default")))?;
    submit(store, &keyed_job(None))?;
    create(store, "user-1")?;
    cluster.settle()?;
    assert!(placed(store, "user-1")?);
    create(store, "user-2")?;
    cluster.settle()?;
    assert_eq!(cluster.blocked()?.len(), 1);
    assert_eq!(cluster.blocked()?[0].key, "class:pool=default");

    store.destroy_keyed_instance("user-1")?;
    assert_eq!(cluster.settle()?, 1, "the freed node wakes it");
    assert!(placed(store, "user-2")?);
    Ok(())
}

/// With no schedulable node at all there is no class to wait on: the eval
/// blocks `escaped`, and any node wakes it.
#[test]
fn an_escaped_job_wakes_on_any_node() -> R<()> {
    let cluster = Cluster::new()?;
    let store = cluster.store();
    let job = JobConfig {
        priority: Default::default(),
        name: JOB.to_string(),
        tasks: vec![TaskConfig {
            count: TaskCount::new(1),
            resources: json!({ "memory": 64 }),
            ..TaskConfig::new(TASK, "noop")
        }],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    };
    submit(store, &job)?;
    // `RunJob` queues the submit's eval after the job row.
    let config_id = store.get_job_config(JOB)?.id();
    store.enqueue_eval("job_submit", Some(JOB), Some(&config_id), None, None)?;
    cluster.settle()?;
    let blocked = cluster.blocked()?;
    assert_eq!(blocked.len(), 1);
    assert_eq!(blocked[0].key, "escaped");
    assert_eq!(
        cluster.job_info()?.status,
        "blocked",
        "nothing is live and the placement waits for room"
    );

    join(store, "n-1", "default")?;
    assert_eq!(cluster.settle()?, 1);
    assert_eq!(
        store.allocs_for_node("n-1")?.len(),
        1,
        "placed on the new node"
    );
    Ok(())
}

/// Capacity that appears while the job's eval is running cannot wake a row
/// that is not blocked yet. The claim remembers where the capacity log was,
/// and the eval checks what changed since before it blocks: it goes
/// straight back to the queue instead, and places.
#[test]
fn an_unblock_during_the_running_eval_is_not_lost() -> R<()> {
    let cluster = Cluster::new()?;
    let store = cluster.store();
    store.insert_node(NodeMsg::from(node("n-1", "default")))?;
    submit(store, &keyed_job(None))?;
    create(store, "user-1")?;
    cluster.settle()?;
    create(store, "user-2")?;

    let batch = store.claim_evals(LIMITS, ClaimScope::Any)?;
    assert_eq!(batch.len(), 1);
    let eval = &batch[0];
    // The plan runs against the old picture and finds no room ...
    let blocked = match eval.handle(store) {
        Err(error) => PlacementBlocked::in_chain(&error)
            .cloned()
            .context("a capacity failure")?,
        Ok(outcome) => panic!("expected no room, got {outcome}"),
    };
    // ... and a node joins before the eval settles.
    store.insert_node(NodeMsg::from(node("n-2", "default")))?;
    let completion = store.completion_for_blocked(
        eval,
        &blocked,
        drafts::eval_blocked(eval, &blocked.key, &blocked.detail, &blocked.message),
    );
    assert!(
        matches!(completion, EvalCompletion::Released { .. }),
        "capacity appeared since the claim, so the eval is requeued, not blocked: {completion:?}"
    );
    store.apply_eval_completion_standalone(&eval.id, completion)?;
    assert!(cluster.blocked()?.is_empty(), "it never blocked");
    cluster.settle()?;
    assert!(placed(store, "user-2")?);
    Ok(())
}

/// A job has one blocked eval, the newest.
#[test]
fn a_job_keeps_one_blocked_eval() -> R<()> {
    let cluster = Cluster::new()?;
    let store = cluster.store();
    store.insert_node(NodeMsg::from(node("n-1", "default")))?;
    submit(store, &keyed_job(None))?;
    create(store, "user-1")?;
    cluster.settle()?;
    create(store, "user-2")?;
    cluster.settle()?;
    let first = cluster.blocked()?;
    create(store, "user-3")?;
    cluster.settle()?;
    let second = cluster.blocked()?;
    assert_eq!(second.len(), 1);
    assert_ne!(
        second[0].id, first[0].id,
        "the newer eval replaced the older"
    );
    // Room for both waiting instances.
    join(store, "n-2", "default")?;
    join(store, "n-3", "default")?;
    assert_eq!(cluster.settle()?, 1);
    assert!(placed(store, "user-2")? && placed(store, "user-3")?);
    Ok(())
}

// ── failover on a 3-manager raft cluster ─────────────────────────────────

fn wait_for(timeout: Duration, what: &str, mut probe: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if probe() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out after {timeout:?} waiting for: {what}");
}

struct RaftManager {
    store: Option<Store>,
    transport: Option<tokio::runtime::Runtime>,
}

impl RaftManager {
    fn store(&self) -> &Store {
        self.store.as_ref().expect("running")
    }

    fn is_leader(&self) -> bool {
        self.store
            .as_ref()
            .and_then(|store| store.raft_status())
            .map(|status| status.role.as_str() == "primary")
            .unwrap_or(false)
    }

    fn kill(&mut self) {
        if let Some(rt) = self.transport.take() {
            rt.shutdown_timeout(Duration::from_secs(1));
        }
        self.store = None;
    }
}

fn raft_manager(
    root: &std::path::Path,
    name: &str,
    bootstrap: bool,
    join_token: Option<String>,
) -> R<RaftManager> {
    let port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    let dir = root.join(name);
    let mut config = ManagerConfigFull {
        store_path: dir.join("store").to_str().context("utf-8")?.to_string(),
        scheduling_policy: "bin_pack".to_string(),
        scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    // The relaxed in-process timings tests/raft_eval_failover.rs documents.
    config.persistence.raft.heartbeat_interval_ms = 400;
    config.persistence.raft.election_timeout_min_ms = 1200;
    config.persistence.raft.election_timeout_max_ms = 2000;
    config.persistence.raft.bootstrap = bootstrap;
    config.persistence.raft.node_id = name.to_string();
    config.persistence.raft.advertise_addr = Some(format!("127.0.0.1:{port}"));
    config.persistence.raft.join_token = join_token;
    config.persistence.raft.data_dir = dir.join("raft").to_str().context("utf-8")?.to_string();
    let store = Store::start_storage(&config)?;
    let transport = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let service = store.raft_transport_server();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
    transport.spawn(async move {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket.set_reuseaddr(true).expect("reuseaddr");
        socket.bind(addr).expect("bind transport port");
        let listener = socket.listen(64).expect("listen");
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let _ = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await;
    });
    Ok(RaftManager {
        store: Some(store),
        transport: Some(transport),
    })
}

/// The blocked rows replicate, and so does every replica's view of the
/// capacity events: when the scheduler-lane leader dies after a node
/// joined but before its wake pass ran, the new leader replays the recent
/// events on taking over and wakes the eval itself. Nothing is lost.
#[test]
fn a_new_leader_rebuilds_from_rows_and_replays_a_missed_wake() -> R<()> {
    let dir = tempfile::tempdir()?;
    let a = raft_manager(dir.path(), "mgr-a", true, None)?;
    let (token, _) = a.store().issue_raft_join_token("test")?;
    let b = raft_manager(dir.path(), "mgr-b", false, Some(token.clone()))?;
    let c = raft_manager(dir.path(), "mgr-c", false, Some(token))?;
    let mut managers = [a, b, c];
    wait_for(Duration::from_secs(150), "both joiners are voters", || {
        managers[0]
            .store()
            .raft_status()
            .map(|status| {
                status.voters.contains(&"mgr-b".to_string())
                    && status.voters.contains(&"mgr-c".to_string())
            })
            .unwrap_or(false)
    });
    wait_for(Duration::from_secs(60), "a leader", || {
        managers.iter().any(RaftManager::is_leader)
    });
    let leader = managers
        .iter()
        .position(RaftManager::is_leader)
        .context("leader")?;
    {
        let store = managers[leader].store();
        store.insert_node(NodeMsg::from(node("n-1", "default")))?;
        submit(store, &keyed_job(None))?;
        create(store, "user-1")?;
        run_eval_worker(store)?;
        store.wake_blocked_evals()?;
        create(store, "user-2")?;
        run_eval_worker(store)?;
        assert_eq!(store.blocked_evals()?.len(), 1, "blocked on the leader");
        // Capacity appears, and the leader dies before its wake pass.
        store.insert_node(NodeMsg::from(node("n-2", "default")))?;
    }
    let alive: Vec<usize> = (0..3).filter(|index| *index != leader).collect();
    wait_for(Duration::from_secs(30), "the join replicated", || {
        alive
            .iter()
            .all(|index| managers[*index].store().get_node("n-2").is_ok())
    });
    managers[leader].kill();
    wait_for(Duration::from_secs(90), "a new leader", || {
        alive.iter().any(|index| managers[*index].is_leader())
    });
    let new_leader = *alive
        .iter()
        .find(|index| managers[**index].is_leader())
        .context("new leader")?;
    let store = managers[new_leader].store();
    assert_eq!(
        store.blocked_evals()?.len(),
        1,
        "the blocked eval is replicated state"
    );
    // The acquisition hook (`requeue_in_flight_evals`) arms the replay.
    store.requeue_in_flight_evals()?;
    wait_for(Duration::from_secs(30), "the replayed wake", || {
        store.wake_blocked_evals().unwrap_or(0) == 1
    });
    run_eval_worker(store)?;
    assert!(placed(store, "user-2")?, "placed on the node that joined");
    for manager in &mut managers {
        manager.kill();
    }
    Ok(())
}
