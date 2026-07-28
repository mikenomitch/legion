//! `topology = "lanes"` battery (phase C-3 of
//! `docs/plans/durability-and-ops-execution-plan.md` §7): three independent
//! raft groups (control / scheduler / status) over one state DB, made sound
//! by single-writer table ownership (§7-C2). Covers:
//!
//! (a) bring-up: a 3-node lanes cluster elects all three groups, one join
//!     token enrolls a joiner on every lane, and a write on each lane's
//!     path (KV + job submit = control, placement = scheduler, heartbeat /
//!     health = status) commits and applies on all nodes — replicas
//!     converge to identical content hashes, per table group and overall;
//! (b) independent elections: moving the status lane's leadership to
//!     another node and then killing that node re-elects ONLY the status
//!     lane — the control and scheduler lanes keep their leader and term,
//!     and control writes keep committing throughout;
//! (c) per-lane snapshot catch-up: driving one lane past its compaction
//!     horizon while a follower is down forces that lane's TABLE-SCOPED
//!     install-snapshot on restart; the other lanes' table groups on that
//!     follower are untouched (content-hash per table group) and the
//!     snapshot artifact is a lane dump, not a whole-DB image;
//! (d) cross-lane consistency: control-lane config updates racing
//!     scheduler-lane placement resolve as read-after-commit or a clean
//!     epoch-CAS rejection + retry — never divergence (state_content_hash
//!     equality at quiesce);
//! (e) CO-LOCATED leadership (JC-40): the control lane is the anchor, and
//!     the engine's convergence loop pulls the other lanes onto whichever
//!     node leads it — after bring-up, after a deliberate split, and after
//!     the all-lane leader is killed. Cross-lane composites survive a
//!     leadership change with a client-style retry without half-applying,
//!     and a node joining mid-convergence becomes a voter on every lane.
//!
//! # Networking exception
//!
//! Like `tests/raft_membership.rs` (whose harness style this reuses), real
//! tonic listeners serve ONLY the `RaftTransport` service on ephemeral
//! loopback ports; everything else is in-process.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use legion::prelude::{json, Context, R};
use legion::storage::store::{PlanApplyOutcome, Store};
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::{JobConfig, TaskConfig};
use legion::structs::{KvValueType, Node, NodeMsg, Submission};

const LANES: [&str; 3] = ["control", "scheduler", "status"];

/// Relaxed election timing for this in-process multi-node harness; applied
/// to every node by `node_config`, which documents why.
const TEST_HEARTBEAT_MS: u64 = 400;
const TEST_ELECTION_MIN_MS: u64 = 1200;
const TEST_ELECTION_MAX_MS: u64 = 2000;

/// How often a deliberate leadership move re-campaigns.
///
/// This MUST stay above the follower leader lease, which openraft derives
/// from `electionTimeoutMaxMs`: a campaign issued while the incumbent's
/// lease is still live cannot win, and — openraft 0.9 has no pre-vote —
/// still bumps the term, so campaigning faster than the lease just churns
/// leadership instead of moving it. Same 2.5x ratio the engine's own
/// convergence backoff uses.
const RETRIGGER_INTERVAL: Duration = Duration::from_millis(TEST_ELECTION_MAX_MS * 5 / 2);

/// Serializes the tests in this binary. Each test runs a 3-node cluster
/// with THREE raft groups per node (nine raft cores, at the relaxed
/// timings set in `node_config`); running four of those clusters in
/// parallel on a loaded CI machine starves heartbeats and triggers
/// spurious elections that move leadership mid-assertion. Sequential
/// execution keeps each test's timing assumptions (the bootstrap node
/// stays leader until the test says otherwise) valid; each test is fast
/// in isolation, so the serialized wall time stays small.
static CLUSTER_GATE: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn cluster_gate() -> std::sync::MutexGuard<'static, ()> {
    CLUSTER_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

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

/// One cluster member: its manager config, its store (drop = kill the
/// engine), and the tonic task serving its raft transport.
struct TestNode {
    name: &'static str,
    config: ManagerConfigFull,
    port: u16,
    store: Option<Store>,
    server: Option<(
        tokio::task::JoinHandle<()>,
        tokio::sync::oneshot::Sender<()>,
    )>,
}

impl TestNode {
    fn store(&self) -> &Store {
        self.store.as_ref().expect("node is running")
    }

    fn raft_data_dir(&self) -> PathBuf {
        PathBuf::from(&self.config.persistence.raft.data_dir)
    }

    fn log_db_path(&self, lane: &str) -> PathBuf {
        self.raft_data_dir().join(lane).join("log.sqlite")
    }

    fn snapshots_dir(&self, lane: &str) -> PathBuf {
        self.raft_data_dir().join(lane).join("snapshots")
    }

    /// This node's view of one lane's `(role, leader, term, applied)`.
    fn lane_status(&self, lane: &str) -> Option<(String, Option<String>, u64, u64)> {
        let status = self.store().raft_status()?;
        status
            .lanes
            .iter()
            .find(|entry| entry.lane == lane)
            .map(|entry| {
                (
                    entry.role.as_str().to_string(),
                    entry.leader_id.clone(),
                    entry.term,
                    entry.applied_index,
                )
            })
    }

    fn leads_lane(&self, lane: &str) -> bool {
        self.lane_status(lane)
            .is_some_and(|(role, _, _, _)| role == "primary")
    }

    /// Mounts the node's `RaftTransport` service on `127.0.0.1:port`.
    /// SO_REUSEADDR so a killed node can rebind its advertised port on
    /// restart (membership records the address; it must stay stable).
    fn start_transport(&mut self, rt: &tokio::runtime::Runtime) {
        let service = self.store().raft_transport_server();
        let addr: SocketAddr = format!("127.0.0.1:{}", self.port).parse().unwrap();
        let name = self.name;
        // Graceful shutdown (GOAWAY) closes established peer connections the
        // way a process exit would — see tests/raft_snapshot_recovery.rs.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = rt.spawn(async move {
            let socket = tokio::net::TcpSocket::new_v4().expect("socket");
            socket.set_reuseaddr(true).expect("reuseaddr");
            socket.bind(addr).expect("bind transport port");
            let listener = socket.listen(64).expect("listen");
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            if let Err(err) = tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
            {
                eprintln!("[{name}] raft transport server exited: {err}");
            }
        });
        self.server = Some((handle, shutdown_tx));
        // Let the listener reach accept() before anyone dials it.
        std::thread::sleep(Duration::from_millis(50));
    }

    /// Kill the node: stop serving its transport (closing established peer
    /// connections, as a process death would) and drop its store (which
    /// shuts down the raft engine, log stores, and pipeline threads).
    fn kill(&mut self) {
        if let Some((handle, shutdown)) = self.server.take() {
            let _ = shutdown.send(());
            std::thread::sleep(Duration::from_millis(100));
            handle.abort();
        }
        self.store = None;
    }

    fn restart(&mut self, rt: &tokio::runtime::Runtime) -> R<()> {
        self.store = Some(Store::start_storage(&self.config)?);
        self.start_transport(rt);
        Ok(())
    }
}

/// Reserve an ephemeral loopback port. The listener is dropped and the port
/// rebound by the transport server; SO_REUSEADDR keeps the rebind safe.
fn reserve_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn node_config(
    root: &Path,
    name: &'static str,
    port: u16,
    bootstrap: bool,
    join_token: Option<String>,
    snapshot_entries: u64,
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
    config.persistence.raft.heartbeat_interval_ms = TEST_HEARTBEAT_MS;
    config.persistence.raft.election_timeout_min_ms = TEST_ELECTION_MIN_MS;
    config.persistence.raft.election_timeout_max_ms = TEST_ELECTION_MAX_MS;
    config.persistence.raft.topology = "lanes".to_string();
    config.persistence.raft.bootstrap = bootstrap;
    config.persistence.raft.node_id = name.to_string();
    config.persistence.raft.advertise_addr = Some(format!("127.0.0.1:{port}"));
    config.persistence.raft.join_token = join_token;
    config.persistence.raft.snapshot_entries = snapshot_entries;
    config.persistence.raft.data_dir = dir
        .join("raft")
        .to_str()
        .context("raft data dir is not utf-8")?
        .to_string();
    Ok(config)
}

fn start_node(
    rt: &tokio::runtime::Runtime,
    root: &Path,
    name: &'static str,
    bootstrap: bool,
    join_token: Option<String>,
    snapshot_entries: u64,
) -> R<TestNode> {
    let port = reserve_port();
    let config = node_config(root, name, port, bootstrap, join_token, snapshot_entries)?;
    let mut node = TestNode {
        name,
        config: config.clone(),
        port,
        store: None,
        server: None,
    };
    node.store = Some(Store::start_storage(&config)?);
    node.start_transport(rt);
    Ok(node)
}

/// Bring up a full 3-node lanes cluster: bootstrap `mgr-a`, join `mgr-b` /
/// `mgr-c` with ONE token each (the token must enroll all three lanes), and
/// wait until every node sees a leader on every lane and the joiners are
/// voters.
fn start_cluster(
    rt: &tokio::runtime::Runtime,
    root: &Path,
    snapshot_entries: u64,
) -> R<(TestNode, TestNode, TestNode)> {
    let a = start_node(rt, root, "mgr-a", true, None, snapshot_entries)?;
    // The bootstrap node must lead EVERY lane, not just one.
    for lane in LANES {
        assert!(
            a.leads_lane(lane),
            "bootstrap node must lead the {lane} lane"
        );
    }
    let (token, _expires) = a.store().issue_raft_join_token("test")?;
    let b = start_node(
        rt,
        root,
        "mgr-b",
        false,
        Some(token.clone()),
        snapshot_entries,
    )?;
    let c = start_node(rt, root, "mgr-c", false, Some(token), snapshot_entries)?;

    // One token enrolls all lanes: the join RPC adds the joiner as learner
    // and promotes it to voter on every lane before replying.
    wait_for(
        Duration::from_secs(120),
        "both joiners promoted to voter and every lane led on every node",
        || {
            let voters_ok = a
                .store()
                .raft_status()
                .map(|s| {
                    s.voters.contains(&"mgr-b".to_string())
                        && s.voters.contains(&"mgr-c".to_string())
                })
                .unwrap_or(false);
            voters_ok
                && [&a, &b, &c].iter().all(|node| {
                    LANES.iter().all(|lane| {
                        node.lane_status(lane)
                            .is_some_and(|(_, leader, _, _)| leader.is_some())
                    })
                })
        },
    );
    Ok((a, b, c))
}

/// The single manager leading EVERY lane in `node`'s view (JC-40's steady
/// state), or `None` while the lanes disagree or any lane is leaderless.
fn co_located_leader(node: &TestNode) -> Option<String> {
    let status = node.store().raft_status()?;
    if status.lanes.len() != LANES.len() {
        return None;
    }
    let mut leaders = status.lanes.iter().map(|lane| lane.leader_id.clone());
    let first = leaders.next().flatten()?;
    for leader in leaders {
        if leader.as_deref() != Some(first.as_str()) {
            return None;
        }
    }
    Some(first)
}

/// Waits until every listed node agrees that ONE manager — one of the
/// LISTED (i.e. still live) managers — leads all three lanes, and returns
/// its name. Requiring the agreed leader to be live matters right after a
/// kill: survivors keep reporting the dead node as every lane's leader
/// until their election timers fire, which is agreement but not
/// convergence.
fn wait_for_co_location(nodes: &[&TestNode], timeout: Duration, what: &str) -> String {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let leaders: Vec<Option<String>> =
            nodes.iter().map(|node| co_located_leader(node)).collect();
        if let Some((Some(first), rest)) = leaders.split_first() {
            if rest.iter().all(|seen| seen.as_ref() == Some(first))
                && nodes.iter().any(|node| node.name == first)
            {
                return first.clone();
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    // A bare "timed out" here is unactionable — the interesting question is
    // always WHICH lane failed to converge and what each survivor believed
    // about it, so dump the per-lane view of every node.
    panic!(
        "timed out after {timeout:?} waiting for: {what}\nlast observed lane state:\n{}",
        lane_state_dump(nodes)
    );
}

/// `(role, leader, term, applied)` for every lane on every listed node, for
/// convergence-failure diagnostics.
fn lane_state_dump(nodes: &[&TestNode]) -> String {
    let mut out = String::new();
    for node in nodes {
        for lane in LANES {
            match node.lane_status(lane) {
                Some((role, leader, term, applied)) => out.push_str(&format!(
                    "  {:>6} {:<10} role={:<9} leader={:<8} term={term} applied={applied}\n",
                    node.name,
                    lane,
                    role,
                    leader.unwrap_or_else(|| "-".into()),
                )),
                None => out.push_str(&format!("  {:>6} {:<10} <no status>\n", node.name, lane)),
            }
        }
    }
    out
}

/// Turn the JC-40 convergence loop on/off on every listed node. Only the
/// leadership-INDEPENDENCE assertions need it off — production always
/// converges.
fn set_convergence(nodes: &[&TestNode], enabled: bool) -> R<()> {
    for node in nodes {
        node.store().raft_set_lane_convergence(enabled)?;
    }
    Ok(())
}

/// Campaign `node` for `lane` until it actually leads it (an `elect` makes
/// the node campaign; the incumbent can win the same race), re-triggering
/// no faster than one election timeout.
fn move_lane_leadership(node: &TestNode, lane: &str, timeout: Duration) {
    let mut last_trigger: Option<Instant> = None;
    wait_for(
        timeout,
        &format!("{} leads the {lane} lane", node.name),
        || {
            let due = last_trigger.is_none_or(|at| at.elapsed() >= RETRIGGER_INTERVAL);
            if !node.leads_lane(lane) && due {
                last_trigger = Some(Instant::now());
                let _ = node.store().raft_trigger_lane_election(lane);
            }
            node.leads_lane(lane)
        },
    );
}

/// Client-style retry of a composite store operation across a leadership
/// change: try every live node in turn and treat a leader-hint rejection as
/// retryable (that is exactly what the hint tells a client to do). Any OTHER
/// error fails the test immediately — a composite must never fail halfway
/// with something a client cannot act on.
fn retry_composite<T>(
    nodes: &[&TestNode],
    what: &str,
    timeout: Duration,
    mut op: impl FnMut(&Store) -> R<T>,
) -> T {
    let deadline = Instant::now() + timeout;
    let mut last = String::new();
    while Instant::now() < deadline {
        for node in nodes {
            match op(node.store()) {
                Ok(value) => return value,
                Err(err) => {
                    let rendered = format!("{err:#}");
                    assert!(
                        rendered.contains("not the raft leader"),
                        "{what} failed with a non-retryable error on {}: {rendered}",
                        node.name
                    );
                    last = rendered;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("timed out retrying {what}; last rejection: {last}");
}

fn kv_value(store: &Store, key: &str) -> Option<String> {
    store.get_kv(key).ok().flatten().map(|entry| entry.value)
}

fn test_node_msg(id: &str) -> NodeMsg {
    NodeMsg::from(Node {
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
    })
}

fn test_job(name: &str, count: u32) -> JobConfig {
    let mut task = TaskConfig::new("svc", "noop");
    task.count = legion::structs::job::TaskCount::new(count);
    JobConfig {
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![task],
        version: None,
    }
}

fn submission(name: &str, content: &str) -> Submission {
    Submission {
        path: format!("{name}.jsonnet"),
        content: content.to_string(),
        is_primary: true,
    }
}

/// Fingerprint of one lane's table group on `store` (plan §7-C2 groups).
fn lane_group_hash(store: &Store, lane: &str) -> R<String> {
    let tables = Store::raft_lane_table_group(lane)?;
    store.state_content_hash_for_tables(&tables)
}

/// True when all three nodes report identical overall content hashes.
fn hashes_converged(nodes: &[&TestNode]) -> bool {
    let hashes: Vec<Option<String>> = nodes
        .iter()
        .map(|n| n.store().state_content_hash().ok())
        .collect();
    match hashes.split_first() {
        Some((Some(first), rest)) => rest.iter().all(|h| h.as_ref() == Some(first)),
        _ => false,
    }
}

/// `(log_rows, last_purged_index)` of a raft log database, read via a
/// separate read-only connection (safe alongside the running engine).
fn log_state(log_db: &Path) -> Option<(i64, Option<u64>)> {
    let conn =
        rusqlite::Connection::open_with_flags(log_db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .ok()?;
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM log;", [], |row| row.get(0))
        .ok()?;
    let purged: Option<Vec<u8>> = conn
        .query_row("SELECT value FROM meta WHERE key = 'purged';", [], |row| {
            row.get(0)
        })
        .ok();
    let purged_index = purged.and_then(|bytes| {
        serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()?
            .get("index")?
            .as_u64()
    });
    Some((rows, purged_index))
}

/// (a) Bring-up + a write on each lane's path + convergence.
#[test]
fn lanes_bring_up_and_per_lane_writes_converge() -> R<()> {
    let _serial = cluster_gate();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let (a, b, c) = start_cluster(&rt, dir.path(), 10_000)?;

    // Per-lane status is real: three lanes, each led (by mgr-a right after
    // bootstrap — no elections have moved anything yet).
    let status = a.store().raft_status().context("mgr-a raft status")?;
    assert_eq!(status.lanes.len(), 3, "lanes topology reports three lanes");
    for lane in LANES {
        let (role, leader, _, _) = a.lane_status(lane).context("lane status present")?;
        assert_eq!(role, "primary", "mgr-a leads the {lane} lane");
        assert_eq!(leader.as_deref(), Some("mgr-a"));
    }

    // Per-lane quorum math (phase C-4): once every lane's group has all
    // three voters, each reports quorum 2 / survives 1 with no parity
    // warning. (Promotion iterates the lanes in arbitrary order, so the
    // non-control lanes may trail the control-lane voter set briefly.)
    wait_for(Duration::from_secs(60), "all lanes report 3 voters", || {
        a.store()
            .raft_status()
            .map(|s| !s.lanes.is_empty() && s.lanes.iter().all(|l| l.voters == 3))
            .unwrap_or(false)
    });
    let status = a.store().raft_status().context("mgr-a raft status")?;
    for lane in &status.lanes {
        assert_eq!(lane.quorum, 2, "lane {} quorum", lane.lane);
        assert_eq!(lane.survives, 1, "lane {} survives", lane.lane);
        assert!(
            lane.warning.is_none(),
            "lane {} warning: {:?}",
            lane.lane,
            lane.warning
        );
    }

    // Control-lane paths: KV + node registration + job submit (the submit
    // composite also drives the status lane's task_status seed).
    a.store()
        .put_kv("lanes/kv", "control", KvValueType::String, None, None)?;
    a.store().insert_node(test_node_msg("lane-node-a"))?;
    a.store().insert_node(test_node_msg("lane-node-b"))?;
    let outcome = a
        .store()
        .handle_job_submit(&test_job("lanes-job", 1), &[submission("lanes-job", "v1")])?;
    let job_config_id = outcome.job_config_id().to_string();
    let tasks = a.store().get_task_configs_for_job_config(&job_config_id)?;
    let (task_id, _) = *tasks.first().context("job must have a task config")?;

    // Scheduler-lane paths: a direct placement and an epoch-CAS plan apply.
    a.store().place_alloc("lane-node-a", task_id)?;
    let epoch = a.store().allocation_epoch_for_job_config(&job_config_id)?;
    let versions = BTreeMap::from([
        (format!("job_config:{job_config_id}"), epoch),
        ("node:lane-node-b".to_string(), 0),
    ]);
    let plan = a.store().insert_plan_submission(
        &job_config_id,
        "fresh",
        &[("lane-node-b".to_string(), task_id)],
        &versions,
    )?;
    let applied = a.store().apply_plan_submission(&plan)?;
    assert!(
        matches!(applied, PlanApplyOutcome::Applied(_)),
        "fresh plan must apply cleanly"
    );

    // Status-lane paths: heartbeat + health checks.
    a.store().update_node_heartbeat("lane-node-a".to_string())?;
    a.store()
        .update_node_health_checks("lane-node-a", "{\"worker\": \"pass\"}")?;

    // Every lane actually carried entries.
    for lane in LANES {
        let (_, _, _, applied_index) = a.lane_status(lane).context("lane status present")?;
        assert!(
            applied_index > 0,
            "the {lane} lane must have applied entries"
        );
    }

    // All nodes converge: overall hash AND each lane's table group.
    let all = [&a, &b, &c];
    wait_for(
        Duration::from_secs(120),
        "all replicas converge to identical content hashes",
        || hashes_converged(&all),
    );
    for lane in LANES {
        let hashes: Vec<String> = all
            .iter()
            .map(|n| lane_group_hash(n.store(), lane))
            .collect::<R<_>>()?;
        assert!(
            hashes.windows(2).all(|pair| pair[0] == pair[1]),
            "{lane} lane table group must converge on every node: {hashes:?}"
        );
    }
    // Spot checks: each lane's write is visible on a follower.
    assert_eq!(kv_value(b.store(), "lanes/kv").as_deref(), Some("control"));
    assert_eq!(b.store().allocs_for_node("lane-node-a")?.len(), 1);
    assert_eq!(c.store().allocs_for_node("lane-node-b")?.len(), 1);
    let node_on_follower = c.store().get_node("lane-node-a")?;
    assert!(
        node_on_follower.last_heartbeat != "2023-01-01T00:00:00Z",
        "heartbeat (status lane) must be visible on followers"
    );
    Ok(())
}

/// (b) Independent elections: move ONLY the status lane's leadership to
/// mgr-b, kill mgr-b, and the status lane re-elects while the control and
/// scheduler lanes keep their leader and term throughout.
///
/// The JC-40 convergence loop is SUSPENDED for the duration
/// (`raft_set_lane_convergence`): co-located leadership is the production
/// steady state, and this test is about the property underneath it — the
/// three groups elect, fail over, and serve independently. Divergent lane
/// leadership stays *safe* under JC-40 (it is merely converged away), and
/// this is the test that pins what "safe" means: the split state behaves
/// exactly as designed, including the per-lane leader hint a foreign-lane
/// write gets. Convergence itself is covered by
/// `lane_leadership_co_locates_on_the_control_anchor` and
/// `killing_the_all_lane_leader_reconverges_onto_one_survivor`.
#[test]
fn killing_a_single_lane_leader_disturbs_only_that_lane() -> R<()> {
    let _serial = cluster_gate();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let (a, mut b, c) = start_cluster(&rt, dir.path(), 10_000)?;
    set_convergence(&[&a, &b, &c], false)?;

    // Seed a node while mgr-a still leads every lane (registration is a
    // control+status composite proposed from one node).
    a.store().insert_node(test_node_msg("elect-node"))?;

    // Move ONLY the status lane's leadership to mgr-b. `elect` makes this
    // node CAMPAIGN; it does not guarantee it wins — the incumbent can win
    // the same race — so retry until leadership actually moves.
    move_lane_leadership(&b, "status", Duration::from_secs(60));
    wait_for(
        Duration::from_secs(60),
        "mgr-b leads the status lane while mgr-a keeps control and scheduler",
        || b.leads_lane("status") && a.leads_lane("control") && a.leads_lane("scheduler"),
    );

    // The aggregate status line now summarizes per-lane leaders instead of
    // pretending one leader holds everything.
    let split = a
        .store()
        .raft_status()
        .and_then(|s| s.leader_id)
        .context("aggregate leader line present")?;
    assert!(
        split.contains("status=mgr-b") && split.contains("control=mgr-a"),
        "aggregate leader must summarize per-lane leaders when they differ: {split}"
    );

    // Status-lane writes now belong on mgr-b; proposing one on mgr-a is
    // rejected with a hint naming the LANE and ITS leader.
    let err = a
        .store()
        .update_node_heartbeat("elect-node".to_string())
        .expect_err("status write on a non-status-leader must be rejected");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("status lane"),
        "rejection must name the lane: {rendered}"
    );
    assert!(
        rendered.contains("mgr-b"),
        "rejection must name that lane's leader: {rendered}"
    );
    // And the write commits on the status lane's actual leader.
    b.store().update_node_heartbeat("elect-node".to_string())?;

    // Record the undisturbed lanes' terms, then kill the node that leads
    // EXACTLY the status lane.
    let (_, control_leader, control_term, _) =
        a.lane_status("control").context("control status")?;
    let (_, scheduler_leader, scheduler_term, _) =
        a.lane_status("scheduler").context("scheduler status")?;
    assert_eq!(control_leader.as_deref(), Some("mgr-a"));
    assert_eq!(scheduler_leader.as_deref(), Some("mgr-a"));
    b.kill();

    // Control writes keep committing while the status lane re-elects (its
    // 2-of-3 quorum survives; only its leader died).
    a.store()
        .put_kv("elect/during", "yes", KvValueType::String, None, None)?;

    wait_for(
        Duration::from_secs(120),
        "a survivor takes over the status lane",
        || [&a, &c].iter().any(|node| node.leads_lane("status")),
    );

    // The OTHER lanes were undisturbed: same leader, same term.
    let (_, control_leader_after, control_term_after, _) =
        a.lane_status("control").context("control status")?;
    let (_, scheduler_leader_after, scheduler_term_after, _) =
        a.lane_status("scheduler").context("scheduler status")?;
    assert_eq!(control_leader_after.as_deref(), Some("mgr-a"));
    assert_eq!(scheduler_leader_after.as_deref(), Some("mgr-a"));
    assert_eq!(
        control_term_after, control_term,
        "control lane must not have re-elected"
    );
    assert_eq!(
        scheduler_term_after, scheduler_term,
        "scheduler lane must not have re-elected"
    );

    // The re-elected status lane serves writes again (on its new leader).
    let status_leader = [&a, &c]
        .into_iter()
        .find(|node| node.leads_lane("status"))
        .expect("status lane has a leader");
    status_leader
        .store()
        .update_node_heartbeat("elect-node".to_string())?;

    // Survivors converge.
    wait_for(
        Duration::from_secs(90),
        "survivors converge after the single-lane failover",
        || hashes_converged(&[&a, &c]),
    );
    Ok(())
}

const SNAPSHOT_ENTRIES: u64 = 16;
const CONTROL_WRITES: usize = 60;

/// (c) Per-lane snapshot catch-up: the control lane compacts past a dead
/// follower's position; on restart the follower converges via the control
/// lane's TABLE-SCOPED install-snapshot, and the other lanes' table groups
/// on that follower are not clobbered.
#[test]
fn laggard_converges_via_lane_scoped_install_snapshot() -> R<()> {
    let _serial = cluster_gate();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let (a, b, mut c) = start_cluster(&rt, dir.path(), SNAPSHOT_ENTRIES)?;

    // Seed scheduler- and status-lane state while mgr-a leads everything.
    a.store().insert_node(test_node_msg("snap-node"))?;
    let outcome = a
        .store()
        .handle_job_submit(&test_job("snap-job", 1), &[submission("snap-job", "v1")])?;
    let job_config_id = outcome.job_config_id().to_string();
    let tasks = a.store().get_task_configs_for_job_config(&job_config_id)?;
    let (task_id, _) = *tasks.first().context("job must have a task config")?;
    a.store().place_alloc("snap-node", task_id)?;
    a.store().update_node_heartbeat("snap-node".to_string())?;
    a.store()
        .update_node_health_checks("snap-node", "{\"worker\": \"pass\"}")?;

    let all = [&a, &b, &c];
    wait_for(
        Duration::from_secs(120),
        "seed state converges before the follower dies",
        || hashes_converged(&all),
    );
    // The follower's other-lane fingerprints before it goes down: nothing
    // on those lanes changes while it is down, so a correct (table-scoped)
    // control install must leave them byte-identical.
    let scheduler_before = lane_group_hash(c.store(), "scheduler")?;
    let status_before = lane_group_hash(c.store(), "status")?;
    let c_control_applied = c.lane_status("control").context("control status")?.3;
    c.kill();

    // Drive the CONTROL lane far past its compaction horizon.
    for i in 0..CONTROL_WRITES {
        a.store().put_kv(
            &format!("bulk/{i}"),
            &format!("value-{i}"),
            KvValueType::String,
            None,
            None,
        )?;
    }
    let control_log = a.log_db_path("control");
    wait_for(
        Duration::from_secs(120),
        "control lane compacted past the dead follower's position",
        || {
            log_state(&control_log)
                .is_some_and(|(_, purged)| purged.is_some_and(|p| p > c_control_applied))
        },
    );

    // Restart: convergence requires the control lane's install-snapshot
    // (the entries it needs are purged).
    c.restart(&rt)?;
    {
        let all = [&a, &b, &c];
        wait_for(
            Duration::from_secs(180),
            "restarted follower converges via the control lane's snapshot",
            || {
                kv_value(c.store(), &format!("bulk/{}", CONTROL_WRITES - 1)).is_some()
                    && hashes_converged(&all)
            },
        );
    }

    // The follower holds a control-lane snapshot artifact, and the artifact
    // is a LANE DUMP scoped to control-snapshot tables — not a whole-DB
    // image that would have clobbered the other lanes.
    let control_snapshots = c.snapshots_dir("control");
    let newest_image = std::fs::read_dir(&control_snapshots)
        .context("control snapshots dir exists on the restarted follower")?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "db"))
        .max_by_key(|path| {
            std::fs::metadata(path)
                .and_then(|m| m.modified())
                .unwrap_or(std::time::UNIX_EPOCH)
        })
        .context("restarted follower must hold an installed control snapshot image")?;
    {
        let conn = rusqlite::Connection::open_with_flags(
            &newest_image,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let lane: String = conn.query_row(
            "SELECT value FROM legion_lane_snapshot WHERE key = 'lane';",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(lane, "control", "snapshot artifact is a control-lane dump");
        let foreign: i64 = conn.query_row(
            "SELECT COUNT(*) FROM legion_lane_snapshot_tables
             WHERE name IN ('allocation', 'instance', 'node_health', 'task_status');",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            foreign, 0,
            "control dump must not carry sibling lanes' tables"
        );
    }

    // Not clobbered: the other lanes' table groups on the follower are
    // byte-identical to their pre-kill state (nothing wrote those lanes
    // while it was down) and identical across all replicas.
    assert_eq!(
        lane_group_hash(c.store(), "scheduler")?,
        scheduler_before,
        "scheduler table group must survive the control-lane install untouched"
    );
    assert_eq!(
        lane_group_hash(c.store(), "status")?,
        status_before,
        "status table group must survive the control-lane install untouched"
    );
    for lane in LANES {
        let all = [&a, &b, &c];
        let hashes: Vec<String> = all
            .iter()
            .map(|n| lane_group_hash(n.store(), lane))
            .collect::<R<_>>()?;
        assert!(
            hashes.windows(2).all(|pair| pair[0] == pair[1]),
            "{lane} lane table group must match on every node after catch-up: {hashes:?}"
        );
    }

    // Live replication continues on top of the installed tables.
    a.store()
        .put_kv("after/install", "live", KvValueType::String, None, None)?;
    {
        let store = c.store().clone();
        wait_for(
            Duration::from_secs(90),
            "post-install replication reaches the recovered follower",
            || kv_value(&store, "after/install").as_deref() == Some("live"),
        );
    }
    Ok(())
}

/// (d) Cross-lane consistency: control-lane config updates racing
/// scheduler-lane placement resolve as read-after-commit or a clean
/// epoch-CAS rejection + retry — and replicas never diverge.
#[test]
fn cross_lane_config_update_racing_placement_never_diverges() -> R<()> {
    let _serial = cluster_gate();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let (a, b, c) = start_cluster(&rt, dir.path(), 10_000)?;

    a.store().insert_node(test_node_msg("race-node-a"))?;
    a.store().insert_node(test_node_msg("race-node-b"))?;
    let outcome = a
        .store()
        .handle_job_submit(&test_job("race-job", 1), &[submission("race-job", "v1")])?;
    let job_config_id = outcome.job_config_id().to_string();
    let tasks = a.store().get_task_configs_for_job_config(&job_config_id)?;
    let (task_id, _) = *tasks.first().context("job must have a task config")?;

    // Deterministic epoch-CAS half: read the epoch, invalidate it with a
    // placement, then apply a plan carrying the STALE epoch — a clean
    // Rejected with the latest versions, never an error or a torn apply.
    let stale_epoch = a.store().allocation_epoch_for_job_config(&job_config_id)?;
    a.store().place_alloc("race-node-a", task_id)?;
    let stale_versions = BTreeMap::from([
        (format!("job_config:{job_config_id}"), stale_epoch),
        ("node:race-node-b".to_string(), 0),
    ]);
    let stale_plan = a.store().insert_plan_submission(
        &job_config_id,
        "fresh",
        &[("race-node-b".to_string(), task_id)],
        &stale_versions,
    )?;
    let rejected = a.store().apply_plan_submission(&stale_plan)?;
    let latest_versions = match rejected {
        PlanApplyOutcome::Rejected {
            latest_versions, ..
        } => latest_versions,
        PlanApplyOutcome::Applied(_) => {
            panic!("stale-epoch plan must be rejected by the CAS")
        }
    };
    // Retry with the versions the rejection reported: clean apply.
    let retry_plan = a.store().insert_plan_submission(
        &job_config_id,
        "fresh",
        &[("race-node-b".to_string(), task_id)],
        &latest_versions,
    )?;
    assert!(
        matches!(
            a.store().apply_plan_submission(&retry_plan)?,
            PlanApplyOutcome::Applied(_)
        ),
        "retry with the reported latest versions must apply"
    );

    // Concurrent half: control-lane resubmits race scheduler-lane plan
    // applies on the same store. Every plan outcome must be Applied or a
    // clean Rejected (retried with fresh versions); nothing may error.
    let control_store = a.store().clone();
    let control_thread = std::thread::spawn(move || -> R<()> {
        for i in 0..5 {
            control_store.handle_job_submit(
                &test_job("race-job", 1),
                &[submission("race-job", &format!("v{}", i + 2))],
            )?;
        }
        Ok(())
    });
    let sched_store = a.store().clone();
    let sched_job_config = job_config_id.clone();
    let sched_thread = std::thread::spawn(move || -> R<()> {
        for _ in 0..5 {
            let mut versions = BTreeMap::from([
                (
                    format!("job_config:{sched_job_config}"),
                    sched_store.allocation_epoch_for_job_config(&sched_job_config)?,
                ),
                ("node:race-node-a".to_string(), 0),
            ]);
            let mut attempts = 0;
            loop {
                attempts += 1;
                let plan = sched_store.insert_plan_submission(
                    &sched_job_config,
                    "fresh",
                    &[("race-node-a".to_string(), task_id)],
                    &versions,
                )?;
                match sched_store.apply_plan_submission(&plan)? {
                    PlanApplyOutcome::Applied(_) => break,
                    PlanApplyOutcome::Rejected {
                        latest_versions, ..
                    } => {
                        assert!(
                            attempts < 20,
                            "epoch CAS must converge under a config-update race"
                        );
                        versions = latest_versions;
                    }
                }
            }
        }
        Ok(())
    });
    control_thread
        .join()
        .expect("control thread panicked")
        .context("control-lane resubmits must stay clean under the race")?;
    sched_thread
        .join()
        .expect("scheduler thread panicked")
        .context("scheduler-lane plan applies must stay clean under the race")?;

    // Quiesce: every replica converges to the same replicated state — the
    // race may pick either order, but it must pick ONE order everywhere.
    let all = [&a, &b, &c];
    wait_for(
        Duration::from_secs(120),
        "replicas converge after the cross-lane race",
        || hashes_converged(&all),
    );
    for lane in LANES {
        let hashes: Vec<String> = all
            .iter()
            .map(|n| lane_group_hash(n.store(), lane))
            .collect::<R<_>>()?;
        assert!(
            hashes.windows(2).all(|pair| pair[0] == pair[1]),
            "{lane} lane table group must converge after the race: {hashes:?}"
        );
    }
    Ok(())
}

// ── (e) co-located lane leadership (JC-40) ───────────────────────────────

/// (e1) The control lane is the ANCHOR: after bring-up one node leads all
/// three lanes, and after a deliberate split the engine's convergence loop
/// pulls the stray lanes back onto the control leader with no operator
/// action — restoring the state cross-lane composites need.
#[test]
fn lane_leadership_co_locates_on_the_control_anchor() -> R<()> {
    let _serial = cluster_gate();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let (a, b, c) = start_cluster(&rt, dir.path(), 10_000)?;
    let all = [&a, &b, &c];

    // Bring-up: every lane already agrees on the bootstrap node.
    let leader = wait_for_co_location(
        &all,
        Duration::from_secs(90),
        "bring-up leaves one manager leading all three lanes",
    );
    assert_eq!(leader, "mgr-a", "the bootstrap node leads every lane");

    // Split the cluster's leadership deliberately, with convergence
    // suspended so the divergence is real and observable rather than raced
    // away.
    set_convergence(&all, false)?;
    move_lane_leadership(&b, "status", Duration::from_secs(60));
    move_lane_leadership(&c, "scheduler", Duration::from_secs(60));
    // The split must be visible from mgr-a (whose convergence loop is the
    // one that will repair it) before convergence is re-enabled — a's own
    // metrics learn the new lane leaders a moment after they win.
    wait_for(
        Duration::from_secs(60),
        "the lanes are genuinely split in the control leader's view",
        || co_located_leader(&a).is_none(),
    );
    // The aggregate leader line refuses to read as a single healthy leader
    // while the lanes disagree.
    let split = a
        .store()
        .raft_status()
        .and_then(|s| s.leader_id)
        .context("aggregate leader line present")?;
    assert!(
        split.contains('=') && split.contains(','),
        "the aggregate leader must summarize per-lane leaders while split: {split}"
    );

    // Re-enable: the control leader campaigns for the lanes it lost until
    // one node leads all three again.
    set_convergence(&all, true)?;
    let converged = wait_for_co_location(
        &all,
        Duration::from_secs(120),
        "the anchor-lane leader reacquires the stray lanes",
    );

    // Whoever ended up leading every lane also leads the control anchor,
    // and the aggregate leader line is a plain name again.
    let converged_node = all
        .iter()
        .find(|node| node.name == converged)
        .copied()
        .context("the converged leader must be one of the cluster's nodes")?;
    assert!(
        converged_node.leads_lane("control"),
        "co-located leadership must settle on the control-lane leader"
    );
    assert_eq!(
        a.store().raft_status().and_then(|s| s.leader_id).as_deref(),
        Some(converged.as_str()),
        "the aggregate leader line names the co-located leader once converged"
    );

    // The point of converging: a cross-lane composite (control identity +
    // status liveness) completes in one go on that node.
    converged_node
        .store()
        .insert_node(test_node_msg("converged-node"))?;
    let seeded = converged_node.store().get_node("converged-node")?;
    assert_eq!(
        seeded.status, "ready",
        "the status half of registration ran"
    );

    wait_for(
        Duration::from_secs(120),
        "replicas converge after the leadership convergence",
        || hashes_converged(&all),
    );
    Ok(())
}

/// (e2) Kill the node leading all three lanes: the survivors re-converge on
/// a single node leading every lane (each lane elects independently, then
/// the new control leader pulls the rest onto itself), and cross-lane
/// composites work again on that node.
#[test]
fn killing_the_all_lane_leader_reconverges_onto_one_survivor() -> R<()> {
    let _serial = cluster_gate();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let (mut a, b, c) = start_cluster(&rt, dir.path(), 10_000)?;
    let leader = wait_for_co_location(
        &[&a, &b, &c],
        Duration::from_secs(90),
        "bring-up leaves one manager leading all three lanes",
    );
    assert_eq!(leader, "mgr-a");

    a.kill();

    let survivors = [&b, &c];
    let converged = wait_for_co_location(
        &survivors,
        Duration::from_secs(210),
        "the survivors re-converge onto one manager leading all three lanes",
    );
    assert!(
        converged == "mgr-b" || converged == "mgr-c",
        "a survivor must lead every lane, not the dead node: {converged}"
    );
    let converged_node = survivors
        .iter()
        .find(|node| node.name == converged)
        .copied()
        .context("the converged leader must be a surviving node")?;
    for lane in LANES {
        assert!(
            converged_node.leads_lane(lane),
            "{converged} must lead the {lane} lane"
        );
    }

    // A composite that spans all three lanes (submit writes control tables,
    // seeds status task_status rows, and enqueues a scheduler-lane eval)
    // completes on the new all-lane leader.
    converged_node
        .store()
        .insert_node(test_node_msg("failover-node"))?;
    let outcome = converged_node.store().handle_job_submit(
        &test_job("failover-job", 1),
        &[submission("failover-job", "v1")],
    )?;
    let job_config_id = outcome.job_config_id().to_string();
    converged_node.store().enqueue_eval(
        "job_submit",
        Some("failover-job"),
        Some(&job_config_id),
        None,
        None,
    )?;
    assert!(
        !converged_node
            .store()
            .task_statuses_for_job_config(&job_config_id)?
            .is_empty(),
        "the status half of the submit composite must have applied"
    );

    wait_for(
        Duration::from_secs(120),
        "survivors converge after the all-lane failover",
        || hashes_converged(&survivors),
    );
    Ok(())
}

/// (e3) A composite op survives a leadership change with a client-style
/// retry and never half-applies: registration (control identity + status
/// liveness) and job submit (control tables + status seed + a scheduler
/// eval) are retried against the surviving nodes, every rejection is the
/// actionable leader-hint error, and the end state has BOTH halves of each
/// composite.
#[test]
fn composites_survive_a_leadership_change_without_half_applying() -> R<()> {
    let _serial = cluster_gate();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let (mut a, b, c) = start_cluster(&rt, dir.path(), 10_000)?;
    wait_for_co_location(
        &[&a, &b, &c],
        Duration::from_secs(90),
        "bring-up leaves one manager leading all three lanes",
    );

    // Kill the all-lane leader mid-flight and drive the composites through
    // the failover the way a client does: retry on the leader hint.
    a.kill();
    let survivors = [&b, &c];

    retry_composite(
        &survivors,
        "worker registration",
        Duration::from_secs(210),
        |store| store.insert_node(test_node_msg("retry-node")).map(|_| ()),
    );
    let job_config_id = retry_composite(
        &survivors,
        "job submit",
        Duration::from_secs(210),
        |store| {
            let outcome = store
                .handle_job_submit(&test_job("retry-job", 1), &[submission("retry-job", "v1")])?;
            let job_config_id = outcome.job_config_id().to_string();
            store.enqueue_eval(
                "job_submit",
                Some("retry-job"),
                Some(&job_config_id),
                None,
                None,
            )?;
            Ok(job_config_id)
        },
    );

    // Both composites landed on whichever survivor served them; mgr-b is a
    // replica, so wait for the effects to reach it before asserting on its
    // view (the halves are asserted individually right below).
    wait_for(
        Duration::from_secs(90),
        "the retried composites' effects reach mgr-b",
        || {
            b.store()
                .get_node("retry-node")
                .map(|node| node.status == "ready")
                .unwrap_or(false)
                && b.store()
                    .task_statuses_for_job_config(&job_config_id)
                    .map(|statuses| !statuses.is_empty())
                    .unwrap_or(false)
                && b.store()
                    .get_evals()
                    .map(|evals| evals.iter().any(|eval| eval.job_id == "retry-job"))
                    .unwrap_or(false)
        },
    );

    // Neither composite half-applied. Registration: the control half wrote
    // identity, the status half seeded liveness (a missing node_health row
    // would hydrate 'active' with an empty heartbeat).
    let registered = b.store().get_node("retry-node")?;
    assert_eq!(registered.pool, "default", "control half applied");
    assert_eq!(registered.status, "ready", "status half applied");
    assert_eq!(
        registered.last_heartbeat, "2023-01-01T00:00:00Z",
        "status half carried the registration-supplied liveness"
    );
    // Submit: control tables, the status-lane task_status seed, and the
    // scheduler-lane eval all landed.
    assert!(
        !b.store()
            .get_task_configs_for_job_config(&job_config_id)?
            .is_empty(),
        "control half applied"
    );
    assert!(
        !b.store()
            .task_statuses_for_job_config(&job_config_id)?
            .is_empty(),
        "status half applied"
    );
    assert!(
        b.store()
            .get_evals()?
            .iter()
            .any(|eval| eval.job_id == "retry-job"),
        "the scheduler-lane eval half applied"
    );

    // A retry of the same composites is a no-op, not a double-apply.
    let leader = survivors
        .iter()
        .find(|node| node.leads_lane("control"))
        .copied()
        .context("a survivor leads the control lane")?;
    leader.store().insert_node(test_node_msg("retry-node"))?;
    leader
        .store()
        .handle_job_submit(&test_job("retry-job", 1), &[submission("retry-job", "v1")])?;
    assert_eq!(
        leader
            .store()
            .get_task_configs_for_job_config(&job_config_id)?
            .len(),
        1,
        "re-running the submit composite must not duplicate task configs"
    );

    wait_for(
        Duration::from_secs(120),
        "survivors converge after the retried composites",
        || hashes_converged(&survivors),
    );
    Ok(())
}

/// (e4) A node joining WHILE the lanes are converging becomes a voter in
/// ALL lanes: the join runs on the control (anchor) leader, and its
/// per-lane learner-add/promotion waits out the convergence window instead
/// of half-applying membership across lanes.
#[test]
fn a_node_joining_during_convergence_becomes_a_voter_in_every_lane() -> R<()> {
    let _serial = cluster_gate();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let (a, b, c) = start_cluster(&rt, dir.path(), 10_000)?;
    let all = [&a, &b, &c];
    wait_for_co_location(
        &all,
        Duration::from_secs(90),
        "bring-up leaves one manager leading all three lanes",
    );

    // Split leadership so the join arrives while mgr-a leads the control
    // anchor but NOT the status lane.
    set_convergence(&all, false)?;
    move_lane_leadership(&b, "status", Duration::from_secs(60));
    // The join must arrive at a control leader that is still missing a
    // lane. (The token's embedded address is the CURRENT control leader's,
    // whichever node that is, so the joiner always dials the anchor.)
    wait_for(
        Duration::from_secs(60),
        "the control and status lanes are led by different managers",
        || {
            let control = all.iter().find(|node| node.leads_lane("control"));
            let status = all.iter().find(|node| node.leads_lane("status"));
            match (control, status) {
                (Some(control), Some(status)) => control.name != status.name,
                _ => false,
            }
        },
    );

    let (token, _expires) = a.store().issue_raft_join_token("test")?;
    // Convergence back on: the joiner's membership changes and mgr-a's
    // campaigns for the status lane now race, which is exactly the window
    // this test exists for.
    set_convergence(&all, true)?;
    let d = start_node(&rt, dir.path(), "mgr-d", false, Some(token), 10_000)?;

    // The joiner must end up a VOTER on every lane, not just the anchor.
    wait_for(
        Duration::from_secs(210),
        "mgr-d is a voter on all three lanes",
        || {
            a.store()
                .raft_status()
                .map(|status| {
                    status.voters.contains(&"mgr-d".to_string())
                        && status.lanes.len() == LANES.len()
                        && status.lanes.iter().all(|lane| lane.voters == 4)
                })
                .unwrap_or(false)
        },
    );
    // ...and from its own view too, with every lane led.
    wait_for(
        Duration::from_secs(120),
        "mgr-d sees a leader and four voters on every lane",
        || {
            d.store()
                .raft_status()
                .map(|status| {
                    status.lanes.len() == LANES.len()
                        && status
                            .lanes
                            .iter()
                            .all(|lane| lane.voters == 4 && lane.leader_id.is_some())
                })
                .unwrap_or(false)
        },
    );

    // Leadership converges again with the enlarged membership, and the new
    // member replicates every lane's state.
    let converged = wait_for_co_location(
        &[&a, &b, &c, &d],
        Duration::from_secs(210),
        "the four-node cluster converges onto one all-lane leader",
    );
    let leader = [&a, &b, &c, &d]
        .into_iter()
        .find(|node| node.name == converged)
        .context("converged leader is a cluster node")?;
    leader.store().insert_node(test_node_msg("join-node"))?;
    leader
        .store()
        .put_kv("join/kv", "control", KvValueType::String, None, None)?;
    wait_for(
        Duration::from_secs(210),
        "the joiner converges to the cluster's state",
        || hashes_converged(&[&a, &b, &c, &d]),
    );
    assert_eq!(kv_value(d.store(), "join/kv").as_deref(), Some("control"));
    Ok(())
}
