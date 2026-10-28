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
//!     node leads it. Convergence is BEST EFFORT — openraft 0.9 has no
//!     leader-transfer API, so the only lever is a campaign, which raft's
//!     election safety may refuse — so these tests assert the contract
//!     underneath it: every lane always elects a leader, convergence happens
//!     where the anchor is able to win, its campaigns stay bounded, and a
//!     standing split stays CORRECT (composites reject cleanly with a
//!     lane-named leader hint, never half-apply, and the scheduler-lane
//!     safety net re-derives the work). A node joining mid-convergence
//!     becomes a settled voter on every lane.
//!
//! # Networking exception
//!
//! Like `tests/integration/raft_membership.rs` (whose harness style this reuses), real
//! tonic listeners serve ONLY the `RaftTransport` service on ephemeral
//! loopback ports; everything else is in-process.

use crate::support::cluster_gate;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use legion::prelude::{json, Context, R};
use legion::storage::store::{PlanApplyOutcome, PlanSubmissionPlacement, Store};
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

// Cluster tests share one gate with the other raft batteries in this binary:
// see `crate::support::cluster_gate`.

/// Opt-in tracing for the whole battery: `LEGION_TEST_TRACE=1` with a
/// `RUST_LOG` filter installs a stderr subscriber, so openraft's own
/// election/vote/membership logs are visible alongside Legion's.
///
/// Worth keeping wired: the two hardest bugs this battery has caught (a
/// convergence loop campaigning into its own lane's election, and a lane
/// left in a joint membership by a join that raced a kill) were both
/// invisible from Legion's logs alone and obvious from openraft's. Run e.g.
/// `LEGION_TEST_TRACE=1 RUST_LOG=openraft=info,legion=debug cargo test
/// --test integration raft_lanes::<name> -- --nocapture`.
fn init_trace() {
    if std::env::var("LEGION_TEST_TRACE").is_err() {
        return;
    }
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .try_init();
    });
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
        let transport_tls = self
            .store()
            .raft_transport_tls()
            .expect("raft engine started");
        let addr: SocketAddr = format!("127.0.0.1:{}", self.port).parse().unwrap();
        let name = self.name;
        // Graceful shutdown (GOAWAY) closes established peer connections the
        // way a process exit would — see tests/integration/raft_snapshot_recovery.rs.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = rt.spawn(async move {
            let socket = tokio::net::TcpSocket::new_v4().expect("socket");
            socket.set_reuseaddr(true).expect("reuseaddr");
            socket.bind(addr).expect("bind transport port");
            let listener = socket.listen(64).expect("listen");
            let incoming = transport_tls
                .incoming(listener)
                .expect("raft transport listener");
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
    //
    // The gate is `raft_status().voters`, which means "SETTLED voter on
    // every lane" — a voter in a uniform, non-joint membership on all three
    // groups. That distinction is the difference between a cluster that
    // tolerates a failure and one that only looks like it does: openraft
    // applies a membership change as joint-then-uniform, `voter_ids()`
    // reports the incoming member from the joint step onwards, and a lane
    // left joint still needs a quorum of the OLD config. A 3-node bring-up
    // passes through `{a,b}` on its way to `{a,b,c}`, so a lane stuck joint
    // there needs BOTH a and b — killing a then costs that lane its write
    // path forever. Gating on the weaker signal is what let this harness
    // declare bring-up complete mid-join and kill the leader into exactly
    // that state.
    wait_for(
        Duration::from_secs(120),
        "every node settled as a voter on every lane, and every lane led on every node",
        || {
            [&a, &b, &c].iter().all(|node| {
                let voters_ok = node
                    .store()
                    .raft_status()
                    .map(|status| {
                        ["mgr-a", "mgr-b", "mgr-c"]
                            .iter()
                            .all(|name| status.voters.iter().any(|voter| voter == name))
                    })
                    .unwrap_or(false);
                voters_ok
                    && LANES.iter().all(|lane| {
                        node.lane_status(lane)
                            .is_some_and(|(_, leader, _, _)| leader.is_some())
                    })
            })
        },
    );
    // Every lane must genuinely survive one failure before any test kills a
    // node — that is the property the gate above exists to establish.
    for node in [&a, &b, &c] {
        let status = node.store().raft_status().context("raft status")?;
        for lane in status.lanes {
            assert_eq!(
                lane.voters, 3,
                "{}: lane {} has {} voters, not 3 ({:?})",
                node.name, lane.lane, lane.voters, lane.warning
            );
            assert_eq!(
                lane.survives, 1,
                "{}: lane {} survives {} failures, not 1 ({:?})",
                node.name, lane.lane, lane.survives, lane.warning
            );
        }
    }
    Ok((a, b, c))
}

/// Every lane, on every listed node, reports SOME leader — and that leader
/// is one of the listed (i.e. still live) managers.
///
/// This is the contract that outranks co-location: an unreachable lane is a
/// dead write path, whereas lanes led by different managers still serve (see
/// `docs/multi-raft.md`).
fn every_lane_has_a_live_leader(nodes: &[&TestNode]) -> bool {
    nodes.iter().all(|node| {
        LANES.iter().all(|lane| {
            node.lane_status(lane)
                .and_then(|(_, leader, _, _)| leader)
                .is_some_and(|leader| nodes.iter().any(|candidate| candidate.name == leader))
        })
    })
}

/// Convergence campaigns issued by `node` for the non-anchor lanes.
///
/// Campaigns bump raft terms, so this is the direct measure of the
/// disruption convergence is allowed to cause. Before the JC-40 governor a
/// lane convergence could not take burned 132 terms in 210s; the governor
/// caps campaigns at three per (escalating) cooldown per lane.
fn convergence_campaigns(node: &TestNode) -> u64 {
    ["scheduler", "status"]
        .iter()
        .map(|lane| node.store().raft_lane_campaigns(lane).unwrap_or(0))
        .sum()
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
        heartbeat_source: String::new(),
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
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![task],
        version: None,
        pending_extensions: Default::default(),
        extensions: Default::default(),
    }
}

/// Like [`test_job`], but the task declares one volume claim, so the
/// submit composite has a real scheduler-lane half (the volume-claim sync)
/// to defer under a standing split.
fn test_job_with_volume(name: &str, count: u32) -> JobConfig {
    let mut config = test_job(name, count);
    config.tasks[0].volumes = vec![legion::structs::job::VolumeSpec {
        name: "data".to_string(),
        driver: None,
        mode: None,
        params: None,
        path: Some("/data".to_string()),
        read_only: None,
        pending_extensions: Default::default(),
        unknown_fields: Default::default(),
    }];
    config
}

fn submission(name: &str, content: &str) -> Submission {
    Submission {
        path: format!("{name}.ncl"),
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
    init_trace();
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
    init_trace();
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

    // The split is real and readable PER LANE — that is the property, not
    // the shape of any summary string. Every lane still has a leader, and
    // the lanes name different ones.
    //
    // Read it from mgr-a, the node whose view the assertions below are
    // about, and wait for that view to catch up first: mgr-b winning the
    // status lane and mgr-a learning about it are separate events, one
    // heartbeat apart.
    wait_for(
        Duration::from_secs(60),
        "the control leader's own view shows the status lane led by mgr-b",
        || {
            a.lane_status("status")
                .and_then(|(_, leader, _, _)| leader)
                .as_deref()
                == Some("mgr-b")
        },
    );
    let status = a.store().raft_status().context("raft status")?;
    let leaders: BTreeMap<String, Option<String>> = status
        .lanes
        .iter()
        .map(|lane| (lane.lane.clone(), lane.leader_id.clone()))
        .collect();
    assert_eq!(
        leaders.get("status").cloned().flatten().as_deref(),
        Some("mgr-b"),
        "the status lane's leader moved: {leaders:?}"
    );
    assert_eq!(
        leaders.get("control").cloned().flatten().as_deref(),
        Some("mgr-a"),
        "the control lane kept its leader: {leaders:?}"
    );
    wait_for(
        Duration::from_secs(60),
        "a split still leaves every lane with a live leader on every node",
        || every_lane_has_a_live_leader(&[&a, &b, &c]),
    );
    // And the aggregate headline refuses to name a single leader while the
    // lanes disagree — whatever wording it uses, it must not read as one
    // healthy leader holding everything.
    let split = status
        .leader_id
        .clone()
        .context("aggregate leader line present")?;
    assert!(
        !["mgr-a", "mgr-b", "mgr-c"].contains(&split.as_str()),
        "the aggregate leader claimed a single leader while the lanes were split: {split}"
    );

    // Status-lane writes now belong on mgr-b; proposing one on mgr-a is
    // FORWARDED to that lane's leader (plan §8, D2) and succeeds — worker
    // liveness must not depend on which manager a worker happens to dial.
    a.store().update_node_heartbeat("elect-node".to_string())?;
    // And the write commits on the status lane's actual leader directly.
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
    init_trace();
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
/// A format-2 (candidate list) plan submission applies through the raft
/// scheduler lane: the stale rank-0 candidate is skipped, the fallback
/// binds, and every replica converges on the same bound allocation —
/// commit-time choice happens inside the replicated apply, so followers
/// hold the walk's result, not the planner's first pick
/// (docs/late-binding-plan-apply.md).
#[test]
fn candidate_plan_submission_falls_back_and_converges() -> R<()> {
    let _serial = cluster_gate();
    init_trace();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let (a, b, c) = start_cluster(&rt, dir.path(), 10_000)?;

    a.store().insert_node(test_node_msg("cand-node-a"))?;
    a.store().insert_node(test_node_msg("cand-node-b"))?;
    let outcome = a
        .store()
        .handle_job_submit(&test_job("cand-job", 1), &[submission("cand-job", "v1")])?;
    let job_config_id = outcome.job_config_id().to_string();
    let tasks = a.store().get_task_configs_for_job_config(&job_config_id)?;
    let (task_id, _) = *tasks.first().context("job must have a task config")?;

    let rival = a.store().handle_job_submit(
        &test_job("cand-rival", 1),
        &[submission("cand-rival", "v1")],
    )?;
    let rival_config = rival.job_config_id().to_string();
    let rival_tasks = a.store().get_task_configs_for_job_config(&rival_config)?;
    let (rival_task, _) = *rival_tasks
        .first()
        .context("rival job must have a task config")?;

    // Sample the vector, then let a rival land on the rank-0 candidate
    // before the apply — the walk must slide to rank 1 instead of
    // rejecting.
    let versions = BTreeMap::from([
        (
            format!("job_config:{job_config_id}"),
            a.store().allocation_epoch_for_job_config(&job_config_id)?,
        ),
        ("node:cand-node-a".to_string(), 0),
        ("node:cand-node-b".to_string(), 0),
    ]);
    let plan = a.store().insert_plan_submission_with_candidates(
        &job_config_id,
        "fresh",
        &[PlanSubmissionPlacement {
            task_id,
            candidates: vec!["cand-node-a".to_string(), "cand-node-b".to_string()],
            instance_id: None,
        }],
        &versions,
    )?;
    a.store().place_alloc("cand-node-a", rival_task)?;

    match a.store().apply_plan_submission(&plan)? {
        PlanApplyOutcome::Applied(applied) => {
            assert_eq!(applied.allocations.len(), 1);
            assert_eq!(
                applied.allocations[0].node_id, "cand-node-b",
                "the walk must bind the fallback candidate"
            );
            assert_eq!(applied.candidate_ranks, vec![1]);
        }
        PlanApplyOutcome::Rejected { reason, .. } => {
            panic!("candidate walk must absorb the race: {reason}")
        }
    }

    let all = [&a, &b, &c];
    wait_for(
        Duration::from_secs(120),
        "replicas converge after the fallback bind",
        || hashes_converged(&all),
    );
    for follower in [&b, &c] {
        let bound = follower
            .store()
            .allocs_for_node("cand-node-b")?
            .iter()
            .filter(|alloc| alloc.job_config_id == job_config_id)
            .count();
        assert_eq!(bound, 1, "followers must hold the walk's bound node");
    }
    Ok(())
}

/// epoch-CAS rejection + retry — and replicas never diverge.
#[test]
fn cross_lane_config_update_racing_placement_never_diverges() -> R<()> {
    let _serial = cluster_gate();
    init_trace();
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
    init_trace();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let (a, b, c) = start_cluster(&rt, dir.path(), 10_000)?;
    let all = [&a, &b, &c];

    // Bring-up: every lane already agrees on the bootstrap node (it is the
    // only voter when each group initializes, so this one is not a race).
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

    // Whatever else is true while the lanes are split, EVERY lane still has
    // a leader — that is the contract co-location is subordinate to.
    // A split is not an outage: every lane still has a leader, and every
    // node learns who it is within a bounded time.
    wait_for(
        Duration::from_secs(60),
        "a split still leaves every lane with a live leader on every node",
        || every_lane_has_a_live_leader(&all),
    );

    // Re-enable convergence. Convergence is BEST EFFORT — a campaign is a
    // race the anchor leader can lose — so this asserts it in the one shape
    // where the anchor demonstrably CAN win: mgr-a leads the control anchor
    // and its logs on the strayed lanes are caught up (it replicated every
    // entry mgr-b and mgr-c appended after taking them), so its campaign is
    // at least as up-to-date as any voter's and raft's election safety
    // cannot refuse it.
    set_convergence(&all, true)?;
    let converged = wait_for_co_location(
        &all,
        Duration::from_secs(120),
        "the anchor-lane leader reacquires the stray lanes it is able to win",
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

    // Convergence must reach that state CHEAPLY. Every campaign bumps a raft
    // term, and the failure this governor exists to prevent is a campaign
    // loop that burns terms without ever settling; three lanes reacquired
    // from a deliberate split is a handful of campaigns, not dozens.
    let campaigns: u64 = all.iter().map(|node| convergence_campaigns(node)).sum();
    assert!(
        campaigns <= 12,
        "converging a two-lane split took {campaigns} campaigns; convergence must be \
         bounded, not a campaign loop"
    );
    Ok(())
}

/// (e2) Kill the node leading all three lanes.
///
/// The REQUIRED outcome is that every lane elects a surviving leader — each
/// lane's 2-of-3 quorum survives one loss, and a lane with no leader is a
/// dead write path. Re-converging those leaders onto one survivor is best
/// effort on top of that (a campaign is a race the new anchor leader can
/// lose), so the test accepts either outcome and pins what must hold in
/// both: cross-lane composites complete without half-applying, and
/// convergence does not burn raft terms trying.
#[test]
fn killing_the_all_lane_leader_reconverges_onto_one_survivor() -> R<()> {
    let _serial = cluster_gate();
    init_trace();
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

    // FIRST and non-negotiable: every lane elects a leader from the
    // survivors. Each lane's 2-of-3 quorum survives the loss, so this is a
    // hard requirement, not a best-effort one — a lane with no leader is a
    // dead write path.
    wait_for(
        Duration::from_secs(120),
        "every lane elects a surviving leader after the all-lane leader dies",
        || every_lane_has_a_live_leader(&survivors),
    );

    // THEN, best effort: the new control-anchor leader pulls the other lanes
    // onto itself. It may not be able to — a campaign is a race it can lose
    // — so a standing split is an ACCEPTED outcome here, and what must hold
    // in that case is that cross-lane composites still complete cleanly.
    let agreed = || {
        co_located_leader(&b).filter(|leader| {
            Some(leader.as_str()) == co_located_leader(&c).as_deref()
                && survivors.iter().any(|node| node.name == leader.as_str())
        })
    };
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut converged = agreed();
    while converged.is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
        converged = agreed();
    }
    if let Some(leader) = &converged {
        let converged_node = survivors
            .iter()
            .find(|node| node.name == leader.as_str())
            .copied()
            .context("the converged leader must be a surviving node")?;
        assert!(
            converged_node.leads_lane("control"),
            "co-located leadership must settle on the control anchor, not some other lane"
        );
        for lane in LANES {
            assert!(
                converged_node.leads_lane(lane),
                "{leader} must lead the {lane} lane"
            );
        }
    }

    // Composites that span all three lanes complete either way: on the
    // co-located leader in one go, or — under a standing split — through the
    // client-style retry the leader hint tells a client to do. Neither may
    // half-apply.
    retry_composite(
        &survivors,
        "register a node after the all-lane failover",
        Duration::from_secs(120),
        |store| store.insert_node(test_node_msg("failover-node")),
    );
    let outcome = retry_composite(
        &survivors,
        "submit a job after the all-lane failover",
        Duration::from_secs(120),
        |store| {
            store.handle_job_submit(
                &test_job("failover-job", 1),
                &[submission("failover-job", "v1")],
            )
        },
    );
    let job_config_id = outcome.job_config_id().to_string();
    retry_composite(
        &survivors,
        "enqueue the submit's scheduler-lane eval",
        Duration::from_secs(120),
        |store| {
            store.enqueue_eval(
                "job_submit",
                Some("failover-job"),
                Some(&job_config_id),
                None,
                None,
            )
        },
    );
    // Both halves are present on every survivor: the control half (the job
    // config) and the status half (its seeded task_status rows).
    for node in survivors {
        assert!(
            node.store().get_node("failover-node").is_ok(),
            "{}: the control half of registration is missing",
            node.name
        );
        assert!(
            !node
                .store()
                .task_statuses_for_job_config(&job_config_id)?
                .is_empty(),
            "{}: the status half of the submit composite must have applied",
            node.name
        );
    }

    // And convergence stayed cheap whether or not it succeeded. This is the
    // regression the governor exists for: the pre-fix loop campaigned into
    // the status lane's own election until the terms ran to 132 in 210s with
    // the applied index frozen, and the lane never elected at all.
    let campaigns: u64 = survivors
        .iter()
        .map(|node| convergence_campaigns(node))
        .sum();
    assert!(
        campaigns <= 12,
        "convergence issued {campaigns} campaigns after the failover; campaigns bump raft \
         terms and must stay bounded\n{}",
        lane_state_dump(&survivors)
    );

    wait_for(
        Duration::from_secs(120),
        "survivors converge after the all-lane failover",
        || hashes_converged(&survivors),
    );
    Ok(())
}

/// (e5) Lanes that settle on DIFFERENT leaders permanently stay correct.
///
/// Convergence is best effort (JC-40), so a standing split is a state
/// production can reach and stay in. This test holds one deliberately —
/// convergence suspended, so it cannot be raced away — and pins the three
/// promises that make it merely *suboptimal* rather than lossy:
///
/// 1. The half of a composite that belongs to a foreign lane is rejected
///    BEFORE it is captured, with the typed leader hint naming the lane and
///    that lane's leader. It never half-applies silently.
/// 2. The halves that DID belong to this node's lanes are all present, and
///    the foreign half left no partial trace.
/// 3. The scheduler work the rejected half would have enqueued is
///    recoverable from replicated state on the scheduler lane's leader —
///    the divergent-leadership safety net `manager::daemon` runs on its
///    node-status-checker cadence (`store.regenerate_scheduling_evals`).
#[test]
fn lanes_that_settle_on_different_leaders_stay_correct() -> R<()> {
    let _serial = cluster_gate();
    init_trace();
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

    // Submit a job while one node still leads everything, so the cluster
    // carries acknowledged control-plane state whose scheduling work has not
    // been enqueued yet (the enqueue is the RPC tail's own scheduler-lane
    // op).
    let outcome = a
        .store()
        .handle_job_submit(&test_job("split-job", 1), &[submission("split-job", "v1")])?;
    let job_config_id = outcome.job_config_id().to_string();

    // Split the SCHEDULER lane away from the control anchor and keep it
    // split: this is the shape a permanently unconverged cluster is in.
    set_convergence(&all, false)?;
    move_lane_leadership(&b, "scheduler", Duration::from_secs(60));
    wait_for(
        Duration::from_secs(60),
        "mgr-b leads the scheduler lane while mgr-a keeps control and status",
        || b.leads_lane("scheduler") && a.leads_lane("control") && a.leads_lane("status"),
    );
    // A split is not an outage: every lane still elects, and every node
    // learns who leads it. (Bounded, not instantaneous — a node learns a new
    // lane leader on that lane's next heartbeat.)
    wait_for(
        Duration::from_secs(60),
        "every lane still has a live leader on every node while the lanes are split",
        || every_lane_has_a_live_leader(&all),
    );

    // (1) A scheduler-lane write issued on the control leader is FORWARDED
    // to the scheduler lane's leader (plan §8, D2) instead of failing: the
    // enqueue lands on mgr-b's lane and replicates from there.
    let eval = a
        .store()
        .enqueue_eval(
            "job_submit",
            Some("split-job"),
            Some(&job_config_id),
            None,
            None,
        )
        .expect("a scheduler-lane enqueue on the control leader must forward to mgr-b");
    assert_eq!(eval.job_id, "split-job");
    wait_for(
        Duration::from_secs(60),
        "the forwarded eval is committed on the scheduler lane's leader",
        || {
            b.store()
                .get_evals()
                .map(|evals| evals.iter().any(|eval| eval.job_id == "split-job"))
                .unwrap_or(false)
        },
    );

    // (2) A composite confined to the lanes this node DOES lead still
    // completes in one go: registration is control identity + status
    // liveness, both on mgr-a.
    a.store().insert_node(test_node_msg("split-node"))?;
    let registered = a.store().get_node("split-node")?;
    assert_eq!(registered.status, "ready", "the status half applied");
    assert_eq!(
        registered.last_heartbeat, "2023-01-01T00:00:00Z",
        "the status half carried the registration-supplied liveness"
    );

    // (3) A submit that SPANS the split is ACCEPTED on the control leader
    // (plan §8, D1): the control half — the operator's intent — commits,
    // the deferred scheduler half (the volume-claim sync) is completed by
    // the scheduler leader's reconciler, and nothing half-applies silently.
    // On the scheduler leader the submit still fails typed and actionably:
    // its control half is the operation, and that lane is led elsewhere.
    let accepted = a
        .store()
        .handle_job_submit(
            &test_job_with_volume("split-job", 2),
            &[submission("split-job", "v2")],
        )
        .expect("a submit on the control leader must be accepted under a standing split");
    let v2_config_id = accepted.job_config_id().to_string();
    assert_ne!(v2_config_id, job_config_id, "v2 must be a new config");
    let from_scheduler = b
        .store()
        .handle_job_submit(
            &test_job_with_volume("split-job", 2),
            &[submission("split-job", "v2")],
        )
        .expect_err("the submit's control half has no home on the scheduler leader");
    let from_scheduler = format!("{from_scheduler:#}");
    assert!(
        from_scheduler.contains("control lane") && from_scheduler.contains("mgr-a"),
        "the mirror rejection must name the control lane and its leader: {from_scheduler}"
    );

    // (4) The deferred halves are completed by the lane leaders'
    // reconcilers — the passes `manager::daemon` runs every checker tick —
    // once the control rows replicate to them.
    wait_for(
        Duration::from_secs(90),
        "the submit's control-lane rows reach the scheduler-lane leader",
        || {
            b.store()
                .get_task_configs_for_job_config(&v2_config_id)
                .map(|configs| !configs.is_empty())
                .unwrap_or(false)
        },
    );
    let synced = b.store().sync_missing_task_volume_claims()?;
    assert!(
        synced > 0,
        "the scheduler leader's reconciler must sync the deferred volume claims"
    );
    let claims = b.store().list_volumes(&Default::default())?;
    assert!(
        claims
            .iter()
            .any(|volume| volume.volume_id.contains("split-job")),
        "the reconciled claim rows must exist on the scheduler leader: {claims:?}"
    );
    // The scheduling work for the job is already queued — the forwarded
    // enqueue from (1) landed on this leader — so the safety net correctly
    // regenerates nothing new; what must hold is that the queue has work
    // for the job after the pass either way.
    let _ = b.store().regenerate_scheduling_evals()?;
    assert!(
        b.store()
            .get_evals()?
            .iter()
            .any(|eval| eval.job_id == "split-job"),
        "scheduling work for the accepted submit must be queued on the scheduler leader"
    );

    // (5) Once leadership converges, a client-style retry is a clean
    // `Unchanged` no-op — the composite was already completed by the
    // reconcilers, nothing was lost and nothing double-applied.
    set_convergence(&all, true)?;
    wait_for_co_location(
        &all,
        Duration::from_secs(120),
        "the anchor leader reacquires the scheduler lane once convergence resumes",
    );
    let healed = retry_composite(
        &all,
        "the submit that spanned the split",
        Duration::from_secs(120),
        |store| {
            store.handle_job_submit(
                &test_job_with_volume("split-job", 2),
                &[submission("split-job", "v2")],
            )
        },
    );
    let healed_id = healed.job_config_id().to_string();
    assert_eq!(
        healed_id, v2_config_id,
        "the retry must land on the already-accepted config, not mint a new one"
    );
    assert!(
        !a.store()
            .task_statuses_for_job_config(&healed_id)?
            .is_empty(),
        "every half of the healed submit applied"
    );

    // The split never cost the cluster consistency.
    wait_for(
        Duration::from_secs(120),
        "replicas converge under standing divergent leadership",
        || hashes_converged(&all),
    );
    Ok(())
}

/// (e6) Worker liveness survives a standing STATUS-lane split (plan §8,
/// D2): a heartbeat arriving at the manager the workers dial — the control
/// leader — is FORWARDED to the status lane's leader instead of being
/// rejected. Pre-fix, that rejection cascade marked every healthy worker
/// lost within seconds and self-fenced their stateful allocations; this
/// pins the fix end to end over the real transport, including the reverse
/// direction (the status leader's mark-lost forwarding its control-lane
/// availability half back to the anchor).
#[test]
fn worker_heartbeats_forward_across_a_status_lane_split() -> R<()> {
    let _serial = cluster_gate();
    init_trace();
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

    // Register while co-located, then split the STATUS lane away from the
    // anchor and hold it there.
    a.store().insert_node(test_node_msg("hb-node"))?;
    set_convergence(&all, false)?;
    move_lane_leadership(&b, "status", Duration::from_secs(60));
    wait_for(
        Duration::from_secs(60),
        "mgr-b leads the status lane while mgr-a keeps control",
        // Winning the lane and mgr-a learning about it are separate events,
        // one heartbeat apart; what follows routes on mgr-a's view.
        || {
            b.leads_lane("status")
                && a.leads_lane("control")
                && a.lane_status("status")
                    .and_then(|(_, leader, _, _)| leader)
                    .as_deref()
                    == Some("mgr-b")
        },
    );

    // The worker's heartbeat lands on mgr-a (workers dial ONE manager) and
    // must succeed by forwarding to mgr-b.
    a.store().update_node_heartbeat("hb-node".to_string())?;
    wait_for(
        Duration::from_secs(60),
        "the forwarded heartbeat is committed on the status lane and replicates",
        || {
            [&a, &b].iter().all(|node| {
                node.store()
                    .get_node("hb-node")
                    .map(|node| {
                        node.status == "active" && node.last_heartbeat != "2023-01-01T00:00:00Z"
                    })
                    .unwrap_or(false)
            })
        },
    );
    // Health-check reports forward the same way.
    a.store().update_node_health_checks("hb-node", "[]")?;

    // Reverse direction: the status leader marks a node lost — its status
    // half is local, and its control half (`sync_node_availability`) must
    // forward to the anchor so the lost node cannot keep receiving
    // placements.
    b.store().mark_node_lost("hb-node")?;
    wait_for(
        Duration::from_secs(60),
        "the lost mark's forwarded control half lands on the anchor and replicates",
        || {
            [&a, &b].iter().all(|node| {
                node.store()
                    .get_node("hb-node")
                    .map(|node| node.status == "lost" && node.availability == "unavailable")
                    .unwrap_or(false)
            })
        },
    );

    // The split never cost the cluster consistency.
    set_convergence(&all, true)?;
    wait_for(
        Duration::from_secs(120),
        "replicas converge after the forwarded writes",
        || hashes_converged(&all),
    );
    Ok(())
}

/// Steady-state worker liveness reaches the status-lane leader by DIGEST,
/// under a standing lane split, without a single replicated write
/// (liveness-off-the-raft-path plan §6.3, phase 4).
///
/// This is the test the whole design exists for: the worker dials mgr-a,
/// mgr-a leads nothing, and mgr-b — which owns node status — nonetheless
/// knows the node is alive, at the cost of ONE manager-plane RPC rather than
/// one raft commit per beat.
#[test]
fn worker_liveness_reaches_the_status_leader_by_digest() -> R<()> {
    let _serial = cluster_gate();
    init_trace();
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

    // An ACTIVE node — the steady-state case. (A node in any other status
    // makes its next heartbeat a transition, which is still replicated by
    // design and is what `worker_heartbeats_forward_across_a_status_lane_split`
    // covers.)
    let mut node = test_node_msg("digest-node");
    node.status = "active".into();
    a.store().insert_node(node)?;

    set_convergence(&all, false)?;
    move_lane_leadership(&b, "status", Duration::from_secs(60));
    wait_for(
        Duration::from_secs(60),
        "mgr-b leads the status lane while mgr-a keeps control",
        // Winning the lane and mgr-a learning about it are separate events,
        // one heartbeat apart; what follows routes on mgr-a's view.
        || {
            b.leads_lane("status")
                && a.leads_lane("control")
                && a.lane_status("status")
                    .and_then(|(_, leader, _, _)| leader)
                    .as_deref()
                    == Some("mgr-b")
        },
    );
    // mgr-b owns liveness now, so it seeds its table the way the status
    // checker does on acquisition.
    b.store()
        .liveness()
        .become_responsible(&["digest-node".into()]);

    // The beat lands on mgr-a, which leads nothing. It costs NO commit: the
    // persisted clock is untouched and the id is queued for the digest.
    a.store().update_node_heartbeat("digest-node".to_string())?;
    assert_eq!(
        a.store().get_node("digest-node")?.last_heartbeat,
        "2023-01-01T00:00:00Z",
        "a steady-state heartbeat must not reach the raft path"
    );

    // One RPC carries every id mgr-a heard from to the status-lane leader.
    // The reply carries the leader's served heartbeat interval, so mgr-a can
    // serve the same adaptive cadence to the workers attached to it.
    b.store()
        .liveness()
        .set_served_interval(Some(Duration::from_secs(21)));
    let seen = a.store().liveness().drain_pending_digest();
    assert_eq!(seen, vec!["digest-node".to_string()]);
    let outcome = rt.block_on(a.store().report_node_liveness("mgr-a", seen));
    assert_eq!(
        outcome,
        legion::storage::liveness::DigestOutcome::Accepted {
            served_interval: Some(Duration::from_secs(21)),
        },
        "the status-lane leader must accept a peer's liveness digest and relay its cadence"
    );
    assert!(
        b.store().liveness().last_beat("digest-node").is_some(),
        "the leader must know a node it never heard from directly"
    );

    // And the leader's periodic checkpoint is what makes the timestamp
    // readable on every manager — display state, two commits a minute.
    let beats: Vec<(String, i64)> = b
        .store()
        .liveness()
        .snapshot()
        .into_iter()
        .map(|(id, beat)| (id, beat.unix))
        .collect();
    b.store().checkpoint_node_heartbeats(&beats)?;
    wait_for(
        Duration::from_secs(60),
        "the liveness checkpoint replicates to every manager",
        || {
            [&a, &b, &c].iter().all(|node| {
                node.store()
                    .get_node("digest-node")
                    .map(|node| node.last_heartbeat != "2023-01-01T00:00:00Z")
                    .unwrap_or(false)
            })
        },
    );

    set_convergence(&all, true)?;
    wait_for(
        Duration::from_secs(120),
        "replicas converge after the checkpoint",
        || hashes_converged(&all),
    );
    Ok(())
}

/// A status-lane election marks ZERO nodes lost, on a manager that has been
/// up since before the fleet's persisted heartbeats went stale
/// (liveness-off-the-raft-path plan §12, phase 2 acceptance).
///
/// Today this manager would mark the whole fleet lost on its first tick: the
/// old grace was keyed on PROCESS uptime, and a manager that acquires
/// status-lane leadership mid-life has plenty of that.
#[test]
fn status_lane_election_marks_no_nodes_lost() -> R<()> {
    use legion::manager::cadence::{decide_lost_nodes, LossContext};
    use legion::manager::daemon::liveness_snapshot;

    let _serial = cluster_gate();
    init_trace();
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

    // A fleet whose persisted heartbeats are stale by construction — which
    // is what EVERY fleet's persisted heartbeats look like to a manager that
    // has just taken over.
    for index in 0..25 {
        let mut node = test_node_msg(&format!("fleet-{index}"));
        node.status = "active".into();
        a.store().insert_node(node)?;
    }

    set_convergence(&all, false)?;
    move_lane_leadership(&b, "status", Duration::from_secs(60));
    wait_for(
        Duration::from_secs(60),
        "mgr-b leads the status lane",
        || b.leads_lane("status"),
    );

    // Acquisition seeds every active node at `now` rather than at its
    // persisted timestamp, and the grace clock starts HERE — not at process
    // start, which for mgr-b was long ago.
    let active: Vec<String> = b
        .store()
        .active_node_liveness_rows()?
        .into_iter()
        .map(|row| row.id)
        .collect();
    assert_eq!(active.len(), 25);
    b.store().liveness().become_responsible(&active);

    let snapshot = liveness_snapshot(b.store(), Instant::now())?;
    assert_eq!(snapshot.len(), 25);
    // Inside the grace, and — because of the seed — outside it too.
    for responsible_for in [Duration::from_secs(1), Duration::from_secs(3600)] {
        let decision = decide_lost_nodes(
            &snapshot,
            &LossContext {
                responsible_for,
                ..Default::default()
            },
        );
        assert!(
            decision.lost.is_empty(),
            "an election must not mark a healthy fleet lost (responsible_for={responsible_for:?})"
        );
    }

    // The negative control: without it, "never mark anything lost" would
    // satisfy every assertion above. A genuinely silent node still goes.
    let long_ago = Instant::now() - Duration::from_secs(3600);
    b.store()
        .liveness()
        .observe_at("fleet-0", long_ago, 1_600_000_000);
    let snapshot = liveness_snapshot(b.store(), Instant::now())?;
    let decision = decide_lost_nodes(
        &snapshot,
        &LossContext {
            responsible_for: Duration::from_secs(3600),
            ..Default::default()
        },
    );
    assert_eq!(decision.lost, vec!["fleet-0".to_string()]);

    set_convergence(&all, true)?;
    Ok(())
}

/// (e3) A composite op survives a leadership change with a client-style
/// retry and never half-applies: registration (control identity + status
/// liveness) and job submit (control tables + status seed + a scheduler
/// eval) are retried against the surviving nodes, and the end state has
/// BOTH halves of each composite. Registration rejections are the
/// actionable leader-hint error; the submit may instead be ACCEPTED with a
/// deferred status half (plan §8, D1), which the status leader's
/// reconciler completes — the test drives that reconciler like the daemon
/// does.
#[test]
fn composites_survive_a_leadership_change_without_half_applying() -> R<()> {
    let _serial = cluster_gate();
    init_trace();
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
    // view (the halves are asserted individually right below). The submit's
    // status seed may have been DEFERRED (plan §8, D1 — the accepting node
    // did not lead the status lane at that moment), so the wait also drives
    // the status leader's reconciler the way the daemon's checker tick
    // does; only the status leader's call does anything, the others reject
    // and are ignored.
    wait_for(
        Duration::from_secs(90),
        "the retried composites' effects reach mgr-b",
        || {
            for node in &survivors {
                let _ = node.store().seed_missing_task_statuses();
            }
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
    init_trace();
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
