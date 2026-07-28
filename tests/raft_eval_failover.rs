//! Eval-queue durability across leader failover (plan §7-C2, JC-41 of
//! `docs/plans/durability-and-ops-execution-plan.md` — which REVERSES
//! JC-27).
//!
//! JC-27 took the eval queue out of raft on the premise that evals are
//! derived, regenerable work, with `regenerate_scheduling_evals` as the
//! safety net. The adversarial pass disproved the premise: an acknowledged
//! `job stop`'s teardown tail and a mid-flight rollout's tick chain carry
//! operator intent that replicated state does not record, so losing them
//! with the failed leader's local queue is permanent, user-visible data
//! loss — in `single` topology as much as `lanes`.
//!
//! JC-41 therefore makes `eval` a scheduler-lane replicated table again.
//! This test is the proof, on an in-process 3-node cluster (the
//! `tests/raft_membership.rs` / `tests/raft_durability.rs` harness):
//!
//! - (a) an acknowledged `job stop` survives the leader's death — its
//!   teardown completes on the successor;
//! - (b) a mid-flight rollout continues after failover — its armed tick is
//!   still there to be claimed;
//! - (d) followers HOLD eval rows (they never act on them), and the replica
//!   content hash — which now includes `eval` — matches across replicas.
//!
//! Class (c) (a cordon-only node is never evicted by reconciliation) and
//! the belt-and-braces reconciler itself live in `tests/eval_regen_failover.rs`
//! and `tests/node_drain.rs`; they need no cluster.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::{JobConfig, TaskConfig, TaskCount};
use legion::structs::{Node, NodeMsg, Submission};
use serde_json::json;

// ── harness (tests/raft_durability.rs pattern) ───────────────────────────

/// Poll `probe` every 100ms until it returns true or `timeout` elapses.
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

/// One cluster member: manager config, store (drop = kill the engine), and
/// a node-owned runtime serving its raft transport (so kill severs
/// established peer connections, not just the listener).
struct TestNode {
    name: &'static str,
    port: u16,
    store: Option<Store>,
    transport_rt: Option<tokio::runtime::Runtime>,
}

impl TestNode {
    fn store(&self) -> &Store {
        self.store.as_ref().expect("node is running")
    }

    fn try_store(&self) -> Option<&Store> {
        self.store.as_ref()
    }

    fn start_transport(&mut self) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("transport runtime");
        let service = self.store().raft_transport_server();
        let addr: SocketAddr = format!("127.0.0.1:{}", self.port).parse().unwrap();
        let name = self.name;
        rt.spawn(async move {
            let socket = tokio::net::TcpSocket::new_v4().expect("socket");
            socket.set_reuseaddr(true).expect("reuseaddr");
            socket.bind(addr).expect("bind transport port");
            let listener = socket.listen(64).expect("listen");
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            if let Err(err) = tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
            {
                eprintln!("[{name}] raft transport server exited: {err}");
            }
        });
        self.transport_rt = Some(rt);
        std::thread::sleep(Duration::from_millis(50));
    }

    fn stop_transport(&mut self) {
        if let Some(rt) = self.transport_rt.take() {
            rt.shutdown_timeout(Duration::from_secs(1));
        }
    }

    /// Kill the node: stop serving its transport and drop its store (which
    /// shuts down the raft engine, log store, and pipeline threads).
    fn kill(&mut self) {
        self.stop_transport();
        self.store = None;
    }
}

fn reserve_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn node_config(
    root: &std::path::Path,
    name: &'static str,
    port: u16,
    bootstrap: bool,
    join_token: Option<String>,
) -> R<ManagerConfigFull> {
    let dir = root.join(name);
    let mut config = ManagerConfigFull {
        store_path: dir
            .join("store")
            .to_str()
            .context("store path is not utf-8")?
            .to_string(),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    // Relaxed election timing for an in-process MULTI-NODE cluster. Every
    // node here shares one process and, in CI, as few as two cores in a
    // debug build, so a node can be descheduled for far longer than the
    // production defaults (250 / 600-1200 ms) tolerate: followers time out
    // against a leader that is merely starved, campaign, and the cluster
    // flaps between terms instead of converging. These values absorb
    // multi-hundred-millisecond scheduling stalls while keeping failover
    // fast enough that the tests stay quick.
    //
    // The window is WIDENED, not just inflated: `electionTimeoutMinMs` is
    // what buys starvation tolerance (it doubles the default's patience
    // before a follower campaigns), while `electionTimeoutMaxMs` is also
    // the follower leader-lease and is therefore kept close to the
    // minimum. Pushing the max to 3s made deliberate transfers
    // (`move_lane_leadership`) time out: a candidate cannot win while the
    // incumbent's lease is live, and the incumbent refreshes it on every
    // heartbeat.
    config.persistence.raft.heartbeat_interval_ms = 400;
    config.persistence.raft.election_timeout_min_ms = 1200;
    config.persistence.raft.election_timeout_max_ms = 2000;
    config.persistence.raft.bootstrap = bootstrap;
    config.persistence.raft.node_id = name.to_string();
    config.persistence.raft.advertise_addr = Some(format!("127.0.0.1:{port}"));
    config.persistence.raft.join_token = join_token;
    config.persistence.raft.data_dir = dir
        .join("raft")
        .to_str()
        .context("raft data dir is not utf-8")?
        .to_string();
    Ok(config)
}

fn start_node(
    root: &std::path::Path,
    name: &'static str,
    bootstrap: bool,
    join_token: Option<String>,
) -> R<TestNode> {
    let port = reserve_port();
    let config = node_config(root, name, port, bootstrap, join_token)?;
    let mut node = TestNode {
        name,
        port,
        store: None,
        transport_rt: None,
    };
    node.store = Some(Store::start_storage(&config)?);
    node.start_transport();
    Ok(node)
}

fn start_cluster(root: &std::path::Path) -> R<Vec<TestNode>> {
    let a = start_node(root, "mgr-a", true, None)?;
    let (token, _expires) = a.store().issue_raft_join_token("test")?;
    let b = start_node(root, "mgr-b", false, Some(token.clone()))?;
    let c = start_node(root, "mgr-c", false, Some(token))?;
    wait_for(
        Duration::from_secs(150),
        "both joiners promoted to voter",
        || {
            a.store()
                .raft_status()
                .map(|s| {
                    s.voters.contains(&"mgr-b".to_string())
                        && s.voters.contains(&"mgr-c".to_string())
                })
                .unwrap_or(false)
        },
    );
    Ok(vec![a, b, c])
}

fn is_leader(node: &TestNode) -> bool {
    node.try_store()
        .and_then(|s| s.raft_status())
        .map(|s| s.role.as_str() == "primary")
        .unwrap_or(false)
}

fn find_leader<'a>(nodes: &[&'a TestNode]) -> Option<&'a TestNode> {
    nodes.iter().copied().find(|n| is_leader(n))
}

fn converged(nodes: &[&TestNode]) -> bool {
    let hashes: Vec<Option<String>> = nodes
        .iter()
        .map(|n| n.try_store().and_then(|s| s.state_content_hash().ok()))
        .collect();
    match hashes.first() {
        Some(Some(first)) => hashes.iter().all(|h| h.as_ref() == Some(first)),
        _ => false,
    }
}

// ── workload fixtures (tests/raft_durability.rs pattern) ─────────────────

fn test_node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        status: "ready".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        config_attrs: json!({ "maxAllocations": 10, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": "4294967296",
            "cpus": { "cpu-1": { "frequency": 2400.0 } }
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

fn test_job(name: &str, count: u32, version: u32) -> JobConfig {
    JobConfig {
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![TaskConfig {
            count: TaskCount::new(count),
            config: json!({ "message": format!("v{version}") }),
            resources: json!({ "memory": 64 }),
            ..TaskConfig::new("svc", "noop")
        }],
        version: None,
    }
}

fn submission(name: &str) -> Submission {
    Submission {
        path: format!("{name}.jsonnet"),
        content: format!("job {name}"),
        is_primary: true,
    }
}

fn evals_of_kind(store: &Store, kind: &str) -> Vec<String> {
    store
        .get_evals()
        .unwrap_or_default()
        .into_iter()
        .filter(|eval| eval.kind == kind)
        .map(|eval| eval.job_id)
        .collect()
}

/// Drain the eval queue with the REAL handlers, the way the daemon's eval
/// workers do: claim → handle → complete (success or failure). Returns the
/// `(kind, ok)` pairs processed.
fn process_eval_queue(store: &Store) -> R<Vec<(String, bool)>> {
    let mut out = Vec::new();
    while let Some(eval) = store.claim_eval()? {
        let ok = match eval.handle(store) {
            Ok(outcome) => {
                eprintln!("eval {} ({}) done: {outcome}", eval.id, eval.kind);
                true
            }
            Err(err) => {
                eprintln!("eval {} ({}) failed: {err:#}", eval.id, eval.kind);
                false
            }
        };
        store.complete_eval(&eval.id, ok)?;
        out.push((eval.kind.clone(), ok));
        assert!(out.len() < 60, "eval queue is not draining");
    }
    Ok(out)
}

/// The JC-41 proof, end to end.
///
/// 1. The leader replicates cluster state (worker nodes, a running job that
///    is then STOPPED, and a second job with a mid-flight rollout) and
///    enqueues the acknowledged tails — `job_stop` and `rollout_tick` —
///    without processing them.
/// 2. Those eval rows REPLICATE: every follower holds them (class d).
/// 3. The leader dies. A survivor takes over and finds the acknowledged
///    work already in its queue — before any regeneration hook runs. Under
///    JC-27 this queue was empty and the stop tail was unrecoverable.
/// 4. Processing that inherited queue tears the stopped job down (class a)
///    and advances the rollout (class b).
/// 5. The survivors converge on a content hash that INCLUDES `eval`
///    (class d).
#[test]
fn acknowledged_work_survives_leader_failover() -> R<()> {
    let dir = tempfile::tempdir()?;
    let mut nodes = start_cluster(dir.path())?;
    let leader_idx = {
        let all: Vec<&TestNode> = nodes.iter().collect();
        wait_for(Duration::from_secs(150), "cluster has a leader", || {
            find_leader(&all).is_some()
        });
        nodes
            .iter()
            .position(is_leader)
            .expect("cluster must have a leader")
    };

    // 1. Replicated state + acknowledged, unprocessed work.
    let rollout_id = {
        let leader = nodes[leader_idx].store();
        for id in ["worker-1", "worker-2"] {
            leader.insert_node(NodeMsg::from(test_node(id)))?;
        }

        // Two placed jobs. Everything up to here is ordinary steady state:
        // the queue is drained before the acknowledged tails are enqueued,
        // so nothing below can be confused with leftover setup work.
        let stopper =
            leader.handle_job_submit(&test_job("stopper", 2, 1), &[submission("stopper")])?;
        let stopper_config = stopper.job_config_id().to_string();
        leader.enqueue_eval(
            "job_submit",
            Some("stopper"),
            Some(&stopper_config),
            None,
            None,
        )?;
        let roller =
            leader.handle_job_submit(&test_job("roller", 2, 1), &[submission("roller")])?;
        let v1 = roller.job_config_id().to_string();
        leader.enqueue_eval("job_submit", Some("roller"), Some(&v1), None, None)?;
        process_eval_queue(leader)?;
        assert_eq!(
            leader.allocs_for_job("stopper")?.len(),
            2,
            "the job must be placed before it is stopped"
        );
        assert_eq!(leader.allocs_for_job("roller")?.len(), 2);

        // (b) A second config for `roller`, mid-rollout, with the tick chain
        // armed. The tick re-arms itself only through the queue, so losing
        // the armed eval stalls the rollout forever.
        let mut v2_job = test_job("roller", 2, 2);
        v2_job.tasks[0].resources = json!({ "memory": 96 });
        let roller_v2 = leader.handle_job_submit(&v2_job, &[submission("roller-v2")])?;
        let v2 = roller_v2.job_config_id().to_string();
        assert_ne!(v1, v2);
        let rollout = leader.create_rollout(
            "roller",
            Some(&v1),
            &v2,
            "running",
            Some("default"),
            None,
            None,
            None,
            "{}",
        )?;

        // (a) The operator stops `stopper`. `StopJob` acknowledges with a
        // control-lane status flip plus a SEPARATE scheduler-lane `job_stop`
        // eval carrying the teardown (the JC-33 wrapper-sequential shape) —
        // exactly what the RPC does. Neither tail is processed: the leader
        // is about to die holding both.
        leader.stop_job("stopper")?;
        leader.enqueue_eval(
            "job_stop",
            Some("stopper"),
            Some(&stopper_config),
            None,
            None,
        )?;
        leader.enqueue_eval("rollout_tick", Some("roller"), Some(&v2), None, None)?;

        assert_eq!(
            evals_of_kind(leader, "job_stop"),
            vec!["stopper".to_string()],
            "the acknowledged stop tail must be queued"
        );
        assert_eq!(
            evals_of_kind(leader, "rollout_tick"),
            vec!["roller".to_string()],
            "the mid-flight rollout's tick must be armed"
        );
        rollout.id
    };

    // 2. Class (d), part 1: the eval rows REPLICATE. Under JC-27 followers
    // carried none, which is precisely why a failover lost them.
    let follower_names: Vec<&'static str> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader_idx)
        .map(|(_, n)| n.name)
        .collect();
    for name in &follower_names {
        let follower = nodes
            .iter()
            .find(|n| n.name == *name)
            .expect("follower still running");
        let store = follower.store().clone();
        wait_for(
            Duration::from_secs(120),
            &format!("acknowledged evals replicate to follower {name}"),
            move || {
                evals_of_kind(&store, "job_stop") == vec!["stopper".to_string()]
                    && evals_of_kind(&store, "rollout_tick") == vec!["roller".to_string()]
            },
        );
    }

    // 3. Kill the leader. A survivor takes over.
    nodes[leader_idx].kill();
    let survivors: Vec<&TestNode> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader_idx)
        .map(|(_, n)| n)
        .collect();
    let mut new_leader: Option<&TestNode> = None;
    let deadline = Instant::now() + Duration::from_secs(150);
    while Instant::now() < deadline && new_leader.is_none() {
        new_leader = find_leader(&survivors);
        std::thread::sleep(Duration::from_millis(100));
    }
    let new_leader = new_leader.expect("a survivor must take over leadership");
    let store = new_leader.store();

    // THE JC-41 PROPERTY: the successor INHERITED the acknowledged work.
    // Asserted before any recovery hook runs, so nothing here can be
    // credited to regeneration.
    assert_eq!(
        evals_of_kind(store, "job_stop"),
        vec!["stopper".to_string()],
        "the acknowledged stop tail must survive the failover in replicated state"
    );
    assert_eq!(
        evals_of_kind(store, "rollout_tick"),
        vec!["roller".to_string()],
        "the mid-flight rollout's armed tick must survive the failover"
    );

    // The daemon's leadership-acquisition hook still runs; with the queue
    // inherited it is pure reconciliation and must not duplicate work.
    let requeued = store.requeue_in_flight_evals()?;
    assert_eq!(requeued, 0, "nothing was left in flight");
    store.regenerate_scheduling_evals()?;
    assert_eq!(
        evals_of_kind(store, "job_stop").len(),
        1,
        "reconciliation must not duplicate the inherited stop tail"
    );
    assert_eq!(
        evals_of_kind(store, "rollout_tick").len(),
        1,
        "reconciliation must not duplicate the inherited rollout tick"
    );

    // 4. Process the inherited queue with the real handlers.
    let processed = process_eval_queue(store)?;
    assert!(
        processed.iter().any(|(kind, ok)| kind == "job_stop" && *ok),
        "the inherited stop tail must run on the successor: {processed:?}"
    );
    // (a) The acknowledged stop actually completed.
    assert!(
        store.allocs_for_job("stopper")?.is_empty(),
        "an acknowledged `job stop` must complete after failover — its allocations are gone"
    );
    // (b) The rollout is no longer frozen: its tick ran and moved it.
    assert!(
        processed.iter().any(|(kind, _)| kind == "rollout_tick"),
        "the mid-flight rollout's tick must be claimable after failover: {processed:?}"
    );
    let rollout_after = store
        .get_rollout(rollout_id)?
        .context("the rollout must still exist after failover")?;
    assert!(
        !store.list_rollout_events(rollout_id, Some(100))?.is_empty()
            || rollout_after.status != "running",
        "the rollout must have progressed after failover, not sat frozen (status {})",
        rollout_after.status
    );

    // 5. Class (d), part 2: the survivors converge — on a content hash that
    // now covers `eval`, so agreeing means agreeing about pending work too.
    let other = survivors
        .iter()
        .find(|n| n.name != new_leader.name)
        .expect("two survivors");
    {
        let other_store = other.store().clone();
        wait_for(
            Duration::from_secs(120),
            "the teardown replicates to the other survivor",
            move || {
                other_store
                    .allocs_for_job("stopper")
                    .map(|allocs| allocs.is_empty())
                    .unwrap_or(false)
            },
        );
    }
    wait_for(
        Duration::from_secs(150),
        "survivors converge on replicated content (eval included)",
        || converged(&survivors),
    );
    let hashes: Vec<String> = survivors
        .iter()
        .map(|n| n.store().state_content_hash().expect("hash"))
        .collect();
    assert_eq!(
        hashes[0], hashes[1],
        "replicas must agree on the content hash, which includes the eval queue (JC-41)"
    );
    let eval_tables = Store::raft_lane_table_group("scheduler")?;
    assert!(
        eval_tables.contains(&"eval"),
        "the eval queue must be part of the scheduler lane's table group: {eval_tables:?}"
    );
    Ok(())
}
