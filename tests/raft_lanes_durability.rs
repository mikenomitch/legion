//! Durability proofs for `topology = "lanes"` (plan §7 of
//! `docs/plans/durability-and-ops-execution-plan.md`): the heavy invariants
//! `tests/raft_durability.rs` proves for the `single` topology, re-proven
//! with THREE independent raft groups (control / scheduler / status) over
//! one state DB on a 3-node in-process cluster:
//!
//! 1. determinism under interleaving — a seeded randomized mixed op stream
//!    from 4 threads, each op proposed at its lane's current leader, leaves
//!    every replica byte-identical (overall AND per lane table group);
//! 2. leader loss mid-burst — killing a node that leads at least one lane
//!    mid-burst loses no acked write on any lane, every lane recovers
//!    leadership among the survivors, and writes resume on every lane;
//! 3. any single node loss — for each node in turn: one acked write per
//!    lane, kill the node, both survivors hold all three;
//! 4. the capture-time single-writer ownership guard (JC-26) stays quiet
//!    through all of it — asserted per test via the process-global
//!    violation counter (`Store::raft_ownership_guard_violations`), plus a
//!    positive control proving the counter is live, not a dead hook.
//!
//! Harness: the `tests/raft_durability.rs` pattern (per-node transport
//! runtimes so kill/partition severs ESTABLISHED peer connections) with the
//! `tests/raft_lanes.rs` lanes config and per-lane status helpers. The
//! documented networking exception applies: raft peers talk through three
//! real tonic listeners serving ONLY the `RaftTransport` service on
//! ephemeral loopback ports; everything else is in-process.

use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::{JobConfig, TaskConfig};
use legion::structs::{KvValueType, Node, NodeMsg, Submission, TaskStatusState};

const LANES: [&str; 3] = ["control", "scheduler", "status"];

// ── harness ──────────────────────────────────────────────────────────────

/// Each test drives a full 3-node lanes cluster — NINE raft cores with
/// tight (150–300ms) election timeouts; running the tests in parallel
/// starves heartbeats under load and flakes leadership. One at a time is
/// deterministic and still fast.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial_guard() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Per-op patience for leader-routed writes. Generous on purpose: on a
/// CPU-starved CI runner (2 cores, debug build, nine in-process raft cores)
/// elections flap and proposals bounce with errors a real client would
/// simply retry (see `tests/raft_durability.rs`).
const OP_PATIENCE: Duration = Duration::from_secs(120);

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
/// engine), and a node-owned runtime serving its raft transport.
///
/// The transport runtime is per-node (the `tests/raft_durability.rs`
/// pattern) because a mid-burst kill must sever ESTABLISHED peer
/// connections, not just the listener: tonic serves each accepted
/// connection on a spawned task that outlives an abort of the serve
/// future, so only shutting the whole runtime down reliably makes the node
/// dark to its peers.
struct TestNode {
    name: &'static str,
    config: ManagerConfigFull,
    port: u16,
    store: Option<Store>,
    transport_rt: Option<tokio::runtime::Runtime>,
}

impl TestNode {
    fn store(&self) -> &Store {
        self.store.as_ref().expect("node is running")
    }

    /// Whether this node currently leads `lane`, by its own metrics.
    fn leads_lane(&self, lane: &str) -> bool {
        self.store
            .as_ref()
            .is_some_and(|store| store_leads_lane(store, lane))
    }

    /// Mounts the node's `RaftTransport` service on `127.0.0.1:port`, on a
    /// fresh node-owned runtime. SO_REUSEADDR so a killed/partitioned node
    /// can rebind its advertised port (membership records the address; it
    /// must stay stable).
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
        // Let the listener reach accept() before anyone dials it.
        std::thread::sleep(Duration::from_millis(50));
    }

    /// Tear down the transport runtime: listener AND every established
    /// inbound connection die, so peers see the node go dark.
    fn stop_transport(&mut self) {
        if let Some(rt) = self.transport_rt.take() {
            rt.shutdown_timeout(Duration::from_secs(1));
        }
    }

    /// Kill the node cleanly (no in-flight callers): stop serving its
    /// transport and drop its store, shutting down all three lanes' raft
    /// cores, log stores, and pipeline threads.
    fn kill(&mut self) {
        self.stop_transport();
        self.store = None;
    }

    /// Kill the node while callers may be blocked inside it: full network
    /// isolation (outbound-block partition hook + inbound transport
    /// teardown) and drop the harness's store handle. Blocked caller
    /// threads pin the engine object alive, but from the peers' point of
    /// view the node is gone — indistinguishable from a crashed process.
    fn kill_under_load(&mut self) -> R<()> {
        self.store().raft_set_outbound_blocked(true)?;
        self.stop_transport();
        self.store = None;
        Ok(())
    }

    fn restart(&mut self) -> R<()> {
        self.store = Some(Store::start_storage(&self.config)?);
        self.start_transport();
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
    config.persistence.raft.topology = "lanes".to_string();
    config.persistence.raft.bootstrap = bootstrap;
    config.persistence.raft.node_id = name.to_string();
    config.persistence.raft.advertise_addr = Some(format!("127.0.0.1:{port}"));
    config.persistence.raft.join_token = join_token;
    // High horizon: these tests exercise replication/failover, not
    // compaction — the lane-scoped snapshot path has its own battery
    // (tests/raft_lanes.rs).
    config.persistence.raft.snapshot_entries = 10_000;
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
        config: config.clone(),
        port,
        store: None,
        transport_rt: None,
    };
    node.store = Some(Store::start_storage(&config)?);
    node.start_transport();
    Ok(node)
}

/// Bootstrap `mgr-a` (which must come up leading EVERY lane), enroll
/// `mgr-b`/`mgr-c` via one join token each (the token enrolls all three
/// lanes), and wait until both are voters and every lane has a leader from
/// every node's point of view.
fn start_cluster(root: &std::path::Path) -> R<Vec<TestNode>> {
    let a = start_node(root, "mgr-a", true, None)?;
    for lane in LANES {
        assert!(
            a.leads_lane(lane),
            "bootstrap node must lead the {lane} lane"
        );
    }
    let (token, _expires) = a.store().issue_raft_join_token("test")?;
    let b = start_node(root, "mgr-b", false, Some(token.clone()))?;
    let c = start_node(root, "mgr-c", false, Some(token))?;
    wait_for(
        Duration::from_secs(300),
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
                        node.store().raft_status().is_some_and(|s| {
                            s.lanes
                                .iter()
                                .any(|l| l.lane == *lane && l.leader_id.is_some())
                        })
                    })
                })
        },
    );
    Ok(vec![a, b, c])
}

// ── lane-leader routing ──────────────────────────────────────────────────

fn store_leads_lane(store: &Store, lane: &str) -> bool {
    store
        .raft_status()
        .map(|s| {
            s.lanes
                .iter()
                .any(|l| l.lane == lane && l.role.as_str() == "primary")
        })
        .unwrap_or(false)
}

/// The store currently leading `lane`, if any of `stores` does.
fn lane_leader(stores: &[Store], lane: &str) -> Option<Store> {
    stores
        .iter()
        .find(|store| store_leads_lane(store, lane))
        .cloned()
}

/// Store handles of every currently-running node, for the routing helpers.
fn cluster_stores(nodes: &[TestNode]) -> Vec<Store> {
    nodes
        .iter()
        .filter_map(|n| n.store.as_ref().cloned())
        .collect()
}

/// The store currently leading EVERY lane in `lanes` (cross-lane composite
/// ops propose each half from the calling node, so the caller must lead
/// all involved lanes — plan JC-40).
fn lanes_leader(stores: &[Store], lanes: &[&str]) -> Option<Store> {
    stores
        .iter()
        .find(|store| lanes.iter().all(|lane| store_leads_lane(store, lane)))
        .cloned()
}

/// Transient raft-layer errors a real client would retry: leadership
/// bounces, pipeline shutdown races, unreachable peers. Application-level
/// errors (not-found, version conflicts, capacity) are workload semantics
/// and must surface immediately.
fn is_transient(err: &anyhow::Error) -> bool {
    let msg = format!("{err:#}").to_lowercase();
    msg.contains("raft leader")
        || msg.contains("retry shortly")
        || msg.contains("replication pipeline")
        || msg.contains("unreachable")
        || (msg.contains("raft") && msg.contains("timeout"))
}

/// Run `op` against whichever store currently leads ALL of `lanes`,
/// retrying transient raft errors and leaderless windows until `patience`
/// elapses or `stop` (when given) is set. Non-transient errors surface
/// immediately — they are workload semantics.
///
/// Multi-lane requests are the composite ops (registration, job submit,
/// destroy) that propose one half per lane FROM THE CALLING NODE (JC-40:
/// reject-and-retry, no auto-forwarding). When a CI-starvation election
/// has split the lanes across nodes, no caller can satisfy such an op, so
/// after a grace period the helper consolidates: it asks the first lane's
/// leader to elect itself on the lanes it does not hold (the same
/// `raft_trigger_lane_election` test hook the lanes battery uses to MOVE
/// leadership) and keeps retrying.
fn run_via_lanes_leader<T>(
    stores: &[Store],
    lanes: &[&str],
    stop: Option<&AtomicBool>,
    patience: Duration,
    mut op: impl FnMut(&Store) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let end = Instant::now() + patience;
    let mut last_err: Option<anyhow::Error> = None;
    let mut next_consolidate = Instant::now() + Duration::from_secs(3);
    loop {
        if stop.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
            anyhow::bail!("burst stop requested");
        }
        match lanes_leader(stores, lanes) {
            Some(store) => match op(&store) {
                Ok(value) => return Ok(value),
                Err(err) if is_transient(&err) => last_err = Some(err),
                Err(err) => return Err(err),
            },
            None => {
                if lanes.len() > 1 && Instant::now() >= next_consolidate {
                    if let Some(candidate) = lane_leader(stores, lanes[0]) {
                        for lane in &lanes[1..] {
                            if !store_leads_lane(&candidate, lane) {
                                let _ = candidate.raft_trigger_lane_election(lane);
                            }
                        }
                    }
                    next_consolidate = Instant::now() + Duration::from_secs(3);
                }
            }
        }
        if Instant::now() >= end {
            return Err(match last_err {
                Some(err) => err.context(format!(
                    "no successful write via a {lanes:?} leader within {patience:?}"
                )),
                None => anyhow::anyhow!("no node led all of {lanes:?} within {patience:?}"),
            });
        }
        std::thread::sleep(Duration::from_millis(150));
    }
}

/// [`run_via_lanes_leader`] for the burst/interleaving threads: any failure
/// (including a stop) collapses to `None` — "not acked".
fn try_via_lanes_leader<T>(
    stores: &[Store],
    lanes: &[&str],
    stop: &AtomicBool,
    patience: Duration,
    op: impl FnMut(&Store) -> anyhow::Result<T>,
) -> Option<T> {
    run_via_lanes_leader(stores, lanes, Some(stop), patience, op).ok()
}

/// [`run_via_lanes_leader`] for must-succeed writes in the test bodies.
fn via_lanes_leader<T>(
    stores: &[Store],
    lanes: &[&str],
    patience: Duration,
    op: impl FnMut(&Store) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    run_via_lanes_leader(stores, lanes, None, patience, op)
}

// ── convergence fingerprints ─────────────────────────────────────────────

fn kv_value(store: &Store, key: &str) -> Option<String> {
    store.get_kv(key).ok().flatten().map(|entry| entry.value)
}

/// Fingerprint of one lane's table group on `store` (plan §7-C2 groups).
fn lane_group_hash(store: &Store, lane: &str) -> R<String> {
    let tables = Store::raft_lane_table_group(lane)?;
    store.state_content_hash_for_tables(&tables)
}

/// All listed nodes report the same overall replicated content hash.
fn converged(nodes: &[&TestNode]) -> bool {
    let hashes: Vec<Option<String>> = nodes
        .iter()
        .map(|n| n.store.as_ref().and_then(|s| s.state_content_hash().ok()))
        .collect();
    match hashes.first() {
        Some(Some(first)) => hashes.iter().all(|h| h.as_ref() == Some(first)),
        _ => false,
    }
}

/// Assert every lane's table group is byte-identical across `nodes`.
fn assert_lane_groups_converged(nodes: &[&TestNode], when: &str) -> R<()> {
    for lane in LANES {
        let hashes: Vec<String> = nodes
            .iter()
            .map(|n| lane_group_hash(n.store(), lane))
            .collect::<R<_>>()?;
        assert!(
            hashes.windows(2).all(|pair| pair[0] == pair[1]),
            "{lane} lane table group must be identical on every node {when}: {hashes:?}"
        );
    }
    Ok(())
}

// ── workload fixtures ────────────────────────────────────────────────────

fn test_node_msg(id: &str) -> NodeMsg {
    NodeMsg::from(Node {
        id: id.to_string(),
        status: "ready".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        config_attrs: json!({ "maxAllocations": 100, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": "4294967296",
            "cpus": { "cpu-1": { "frequency": 2400.0 } }
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    })
}

fn test_job(name: &str) -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![TaskConfig::new("svc", "noop")],
        version: None,
    }
}

fn submission(name: &str, content: &str) -> Submission {
    Submission {
        path: format!("{name}.ncl"),
        content: content.to_string(),
        is_primary: true,
    }
}

/// Register worker nodes and submit a base job whose task backs the
/// scheduler-lane placements. Registration and submit are control+status
/// composites proposed from the calling node, so each op is routed to a
/// node leading BOTH lanes (with retries — a CI-starvation election right
/// after bring-up must not fail the fixture). Returns
/// `(job_config_id, task_id)`.
fn seed_fixtures(stores: &[Store], prefix: &str, worker_count: usize) -> R<(String, i64)> {
    for i in 0..worker_count {
        let id = format!("{prefix}-n-{i}");
        via_lanes_leader(stores, &["control", "status"], OP_PATIENCE, |s| {
            s.insert_node(test_node_msg(&id))
        })?;
    }
    let job = format!("{prefix}-base");
    via_lanes_leader(stores, &["control", "status"], OP_PATIENCE, |s| {
        let outcome = s.handle_job_submit(&test_job(&job), &[submission(&job, "v1")])?;
        let job_config_id = outcome.job_config_id().to_string();
        // Read back on the same store that acked the submit (a follower
        // might not have applied it yet).
        let tasks = s.get_task_configs_for_job_config(&job_config_id)?;
        let (task_id, _) = *tasks.first().context("base job must have a task config")?;
        Ok((job_config_id, task_id))
    })
}

/// Deterministic xorshift64* stream — the seeded RNG behind the randomized
/// op mix (no external RNG crate whose sequence could drift on upgrade).
struct SeededRng(u64);

impl SeededRng {
    fn new(seed: u64) -> Self {
        // Never zero (the xorshift fixpoint).
        Self(seed | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

// ── 1. determinism under interleaving ────────────────────────────────────

/// Four writer threads run seeded (per-thread deterministic) streams of
/// ~160 total mixed ops, each proposed at its lane's CURRENT leader:
/// control (kv put/patch/delete, job submit/stop), scheduler (direct
/// placements, epoch-CAS plan submissions, allocation removal), status
/// (heartbeats, health checks, task status). Whatever interleaving the
/// three independent raft logs pick, changeset replication under
/// single-writer ownership must leave every replica byte-identical:
/// after quiescing, the overall content hash AND every lane's table-group
/// hash are identical on all nodes.
#[test]
fn lanes_determinism_under_interleaving() -> R<()> {
    let _serial = serial_guard();
    let guard_violations_before = Store::raft_ownership_guard_violations();
    let dir = tempfile::tempdir()?;
    let nodes = start_cluster(dir.path())?;
    let (job_config_id, task_id) = seed_fixtures(&cluster_stores(&nodes), "det", 4)?;

    let stores: Vec<Store> = nodes.iter().map(|n| n.store().clone()).collect();
    let alloc_pool: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
    let never_stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();
    for t in 0..4u64 {
        let stores = stores.clone();
        let alloc_pool = alloc_pool.clone();
        let stop = never_stop.clone();
        let barrier = barrier.clone();
        let job_config_id = job_config_id.clone();
        handles.push(std::thread::spawn(move || -> usize {
            let mut rng = SeededRng::new(0x9E37_79B9_7F4A_7C15 ^ (t + 1));
            barrier.wait();
            let mut acked = 0usize;
            for i in 0..40u64 {
                let r = rng.next();
                // Individual ops may legitimately fail (not-found on a key
                // another thread deleted, stop of a job not yet submitted,
                // a CAS rejection): a failed op proposes nothing on the
                // lane, so it cannot diverge replicas — exactly the
                // property under test.
                let ok = match r % 10 {
                    0 => try_via_lanes_leader(&stores, &["control"], &stop, OP_PATIENCE, |s| {
                        s.put_kv(
                            &format!("det/kv-{}", r % 12),
                            &format!("v{t}-{i}"),
                            KvValueType::String,
                            None,
                            None,
                        )
                    })
                    .is_some(),
                    1 => try_via_lanes_leader(&stores, &["control"], &stop, OP_PATIENCE, |s| {
                        s.put_kv(
                            &format!("det/json-{}", r % 6),
                            "{}",
                            KvValueType::Json,
                            None,
                            None,
                        )
                    })
                    .is_some(),
                    2 => try_via_lanes_leader(&stores, &["control"], &stop, OP_PATIENCE, |s| {
                        s.patch_kv(
                            &format!("det/json-{}", r % 6),
                            &format!(
                                "[{{\"op\": \"add\", \"path\": \"/writer-{t}\", \"value\": {i}}}]"
                            ),
                            None,
                            None,
                        )
                    })
                    .is_some(),
                    3 => try_via_lanes_leader(&stores, &["control"], &stop, OP_PATIENCE, |s| {
                        s.delete_kv(&format!("det/kv-{}", r % 12), None, None)
                    })
                    .is_some(),
                    4 => {
                        // Job submit is a control+status composite (the
                        // status half seeds task_status), proposed from the
                        // calling node — route it to a node leading BOTH.
                        let name = format!("det-job-{t}-{}", r % 2);
                        try_via_lanes_leader(
                            &stores,
                            &["control", "status"],
                            &stop,
                            OP_PATIENCE,
                            |s| s.handle_job_submit(&test_job(&name), &[submission(&name, "v1")]),
                        )
                        .is_some()
                    }
                    5 => try_via_lanes_leader(&stores, &["control"], &stop, OP_PATIENCE, |s| {
                        s.stop_job(&format!("det-job-{t}-{}", r % 2))
                    })
                    .is_some(),
                    6 => {
                        if r.is_multiple_of(2) {
                            match try_via_lanes_leader(
                                &stores,
                                &["scheduler"],
                                &stop,
                                OP_PATIENCE,
                                |s| s.place_alloc(&format!("det-n-{}", r % 4), task_id),
                            ) {
                                Some(alloc) => {
                                    alloc_pool.lock().unwrap().push(alloc.id);
                                    true
                                }
                                None => false,
                            }
                        } else {
                            let popped = alloc_pool.lock().unwrap().pop();
                            match popped {
                                Some(id) => try_via_lanes_leader(
                                    &stores,
                                    &["scheduler"],
                                    &stop,
                                    OP_PATIENCE,
                                    |s| s.remove_allocation_by_id(id),
                                )
                                .is_some(),
                                None => true,
                            }
                        }
                    }
                    7 => {
                        // Epoch-CAS plan submission: Applied and Rejected
                        // are both clean outcomes (tests/raft_lanes.rs
                        // proves the rejected-retry path deterministically;
                        // here the race just must never error or diverge).
                        try_via_lanes_leader(&stores, &["scheduler"], &stop, OP_PATIENCE, |s| {
                            let epoch = s.allocation_epoch_for_job_config(&job_config_id)?;
                            let versions = BTreeMap::from([
                                (format!("job_config:{job_config_id}"), epoch),
                                (format!("node:det-n-{}", r % 4), 0),
                            ]);
                            let plan = s.insert_plan_submission(
                                &job_config_id,
                                "fresh",
                                &[(format!("det-n-{}", r % 4), task_id)],
                                &versions,
                            )?;
                            s.apply_plan_submission(&plan)
                        })
                        .is_some()
                    }
                    8 => try_via_lanes_leader(&stores, &["status"], &stop, OP_PATIENCE, |s| {
                        s.update_node_heartbeat(format!("det-n-{}", r % 4))
                    })
                    .is_some(),
                    _ => {
                        if r.is_multiple_of(2) {
                            try_via_lanes_leader(&stores, &["status"], &stop, OP_PATIENCE, |s| {
                                s.update_node_health_checks(
                                    &format!("det-n-{}", r % 4),
                                    &format!("{{\"writer\": {t}, \"seq\": {i}}}"),
                                )
                            })
                            .is_some()
                        } else {
                            try_via_lanes_leader(&stores, &["status"], &stop, OP_PATIENCE, |s| {
                                s.set_task_status(
                                    task_id,
                                    if r.is_multiple_of(4) {
                                        TaskStatusState::Running
                                    } else {
                                        TaskStatusState::Pending
                                    },
                                )
                            })
                            .is_some()
                        }
                    }
                };
                if ok {
                    acked += 1;
                }
            }
            acked
        }));
    }
    let total_acked: usize = handles
        .into_iter()
        .map(|h| h.join().expect("writer thread panicked"))
        .sum();
    assert!(
        total_acked >= 100,
        "the mixed lanes workload must mostly succeed (acked {total_acked}/160)"
    );

    // Quiesce, then the determinism claim itself: identical content hashes
    // on every node — overall and per lane table group.
    let all: Vec<&TestNode> = nodes.iter().collect();
    wait_for(
        Duration::from_secs(240),
        "all replicas converge to identical content hashes",
        || converged(&all),
    );
    assert_lane_groups_converged(&all, "after the interleaved workload")?;
    for node in &all {
        assert_eq!(
            node.store().get_job("det-base")?.id,
            "det-base",
            "replicated state must be non-trivial on {}",
            node.name
        );
    }
    assert_eq!(
        Store::raft_ownership_guard_violations(),
        guard_violations_before,
        "no op in the interleaved workload may trip the ownership guard (JC-26)"
    );
    Ok(())
}

// ── 2. leader loss mid-burst ─────────────────────────────────────────────

/// Four threads burst writes onto all three lanes (kv = control, direct
/// placements = scheduler, health-check upserts = status), each write
/// routed to its lane's current leader. Mid-burst, the node leading the
/// most lanes (>= 1 by construction) is killed under load. Every write
/// that was ACKED to a caller — on every lane — survives on both
/// survivors, every lane re-elects a leader among the survivors, and
/// writes resume on every lane.
#[test]
fn lanes_leader_loss_mid_burst() -> R<()> {
    let _serial = serial_guard();
    let guard_violations_before = Store::raft_ownership_guard_violations();
    let dir = tempfile::tempdir()?;
    let mut nodes = start_cluster(dir.path())?;
    let (_job_config_id, task_id) = seed_fixtures(&cluster_stores(&nodes), "burst", 4)?;

    let stores: Vec<Store> = nodes.iter().map(|n| n.store().clone()).collect();
    let acked_kv: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let acked_allocs: Arc<Mutex<HashSet<i64>>> = Arc::new(Mutex::new(HashSet::new()));
    let acked_health_seq: Arc<Vec<AtomicU64>> =
        Arc::new((0..4).map(|_| AtomicU64::new(0)).collect());
    let stop = Arc::new(AtomicBool::new(false));
    for t in 0..4u64 {
        let stores = stores.clone();
        let acked_kv = acked_kv.clone();
        let acked_allocs = acked_allocs.clone();
        let acked_health_seq = acked_health_seq.clone();
        let stop = stop.clone();
        // Not joined: a thread blocked inside the killed node's engine
        // never returns (see kill_under_load).
        std::thread::spawn(move || {
            let worker = format!("burst-n-{t}");
            for i in 1..u64::MAX {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                // One write per lane per iteration, so every lane keeps a
                // stream of in-flight proposals across the kill.
                let key = format!("burst/{t}/{i}");
                if try_via_lanes_leader(&stores, &["control"], &stop, OP_PATIENCE, |s| {
                    s.put_kv(&key, "payload", KvValueType::String, None, None)
                })
                .is_some()
                {
                    acked_kv.lock().unwrap().insert(key);
                }
                if let Some(alloc) =
                    try_via_lanes_leader(&stores, &["scheduler"], &stop, OP_PATIENCE, |s| {
                        s.place_alloc(&worker, task_id)
                    })
                {
                    acked_allocs.lock().unwrap().insert(alloc.id);
                }
                if try_via_lanes_leader(&stores, &["status"], &stop, OP_PATIENCE, |s| {
                    s.update_node_health_checks(&worker, &format!("{{\"seq\": {i}}}"))
                })
                .is_some()
                {
                    // Health upserts to this thread's own node are ordered
                    // (one writer), so the highest acked seq is the write
                    // whose survival the assertion below checks.
                    acked_health_seq[t as usize].fetch_max(i, Ordering::SeqCst);
                }
            }
        });
    }

    // Let a healthy chunk of the burst commit on every lane, then kill.
    {
        let acked_kv = acked_kv.clone();
        let acked_allocs = acked_allocs.clone();
        let acked_health_seq = acked_health_seq.clone();
        wait_for(
            Duration::from_secs(240),
            "burst underway on every lane",
            move || {
                acked_kv.lock().unwrap().len() >= 30
                    && acked_allocs.lock().unwrap().len() >= 15
                    && acked_health_seq
                        .iter()
                        .all(|seq| seq.load(Ordering::SeqCst) >= 3)
            },
        );
    }
    // The victim: whichever node currently leads the most lanes (>= 1 by
    // pigeonhole — three lanes, three nodes). Right after bring-up that is
    // the bootstrap node leading all three; under CI-starvation flapping it
    // may lead fewer, which only makes the scenario weaker, never vacuous.
    let victim_idx = (0..nodes.len())
        .max_by_key(|&i| {
            LANES
                .iter()
                .filter(|lane| nodes[i].leads_lane(lane))
                .count()
        })
        .expect("cluster has nodes");
    let victim_lanes: Vec<&str> = LANES
        .iter()
        .copied()
        .filter(|lane| nodes[victim_idx].leads_lane(lane))
        .collect();
    assert!(
        !victim_lanes.is_empty(),
        "some node must lead at least one lane"
    );
    let victim_name = nodes[victim_idx].name;
    nodes[victim_idx].kill_under_load()?;
    stop.store(true, Ordering::SeqCst);

    // Every lane recovers leadership among the survivors — including the
    // lanes the victim led.
    let survivors: Vec<&TestNode> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != victim_idx)
        .map(|(_, n)| n)
        .collect();
    wait_for(
        Duration::from_secs(240),
        &format!("every lane re-elects among the survivors of killing {victim_name} (led {victim_lanes:?})"),
        || {
            LANES
                .iter()
                .all(|lane| survivors.iter().any(|n| n.leads_lane(lane)))
        },
    );

    // Give acks racing the kill a moment to land in the sets, then
    // snapshot. Any ack recorded after the snapshot was still
    // quorum-committed, so it is covered by the convergence assertion.
    std::thread::sleep(Duration::from_secs(1));
    let acked_kv: HashSet<String> = acked_kv.lock().unwrap().clone();
    let acked_allocs: HashSet<i64> = acked_allocs.lock().unwrap().clone();
    let acked_health: Vec<u64> = acked_health_seq
        .iter()
        .map(|seq| seq.load(Ordering::SeqCst))
        .collect();
    assert!(!acked_kv.is_empty(), "burst must have acked control writes");
    assert!(
        !acked_allocs.is_empty(),
        "burst must have acked scheduler writes"
    );
    wait_for(Duration::from_secs(240), "survivors converge", || {
        converged(&survivors)
    });

    // Every ACKED write survived, on every lane, on both survivors.
    for survivor in &survivors {
        let store = survivor.store();
        for key in &acked_kv {
            assert_eq!(
                kv_value(store, key).as_deref(),
                Some("payload"),
                "acked control-lane write {key} must be present on {}",
                survivor.name
            );
        }
        let held: HashSet<i64> = (0..4)
            .map(|t| store.allocs_for_node(&format!("burst-n-{t}")))
            .collect::<R<Vec<_>>>()?
            .into_iter()
            .flatten()
            .map(|alloc| alloc.id)
            .collect();
        let lost: Vec<&i64> = acked_allocs.difference(&held).collect();
        assert!(
            lost.is_empty(),
            "acked scheduler-lane placements {lost:?} must be present on {}",
            survivor.name
        );
        for (t, &last_acked) in acked_health.iter().enumerate() {
            let node = store.get_node(&format!("burst-n-{t}"))?;
            let seq = node
                .health_checks
                .get("seq")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            assert!(
                seq >= last_acked,
                "acked status-lane health write (seq {last_acked}) for burst-n-{t} must \
                 survive on {} (found seq {seq})",
                survivor.name
            );
        }
    }

    // Writes resume on EVERY lane, against the survivors' leaders.
    let survivor_stores: Vec<Store> = survivors.iter().map(|n| n.store().clone()).collect();
    via_lanes_leader(&survivor_stores, &["control"], OP_PATIENCE, |s| {
        s.put_kv(
            "burst/after/control",
            "resumed",
            KvValueType::String,
            None,
            None,
        )
    })?;
    let resumed_alloc = via_lanes_leader(&survivor_stores, &["scheduler"], OP_PATIENCE, |s| {
        s.place_alloc("burst-n-0", task_id)
    })?;
    via_lanes_leader(&survivor_stores, &["status"], OP_PATIENCE, |s| {
        s.update_node_heartbeat("burst-n-0".to_string())
    })?;
    wait_for(
        Duration::from_secs(240),
        "post-failover writes replicate to both survivors",
        || {
            survivors.iter().all(|n| {
                kv_value(n.store(), "burst/after/control").as_deref() == Some("resumed")
                    && n.store()
                        .allocs_for_node("burst-n-0")
                        .map(|allocs| allocs.iter().any(|a| a.id == resumed_alloc.id))
                        .unwrap_or(false)
            }) && converged(&survivors)
        },
    );
    assert_lane_groups_converged(&survivors, "after the mid-burst leader loss")?;
    assert_eq!(
        Store::raft_ownership_guard_violations(),
        guard_violations_before,
        "no op in the burst may trip the ownership guard (JC-26)"
    );
    Ok(())
}

// ── 3. acked write survives any single node loss ─────────────────────────

/// For each node in turn: commit one write per lane via that lane's
/// current leader, kill the node (engine shutdown), and assert all three
/// acked writes are present on both survivors — whichever mix of lanes the
/// victim was leading (the bootstrap-node round exercises losing every
/// lane's leader at once). The node is then restarted and the cluster
/// re-converges before the next round.
#[test]
fn lanes_acked_write_survives_any_single_node_loss() -> R<()> {
    let _serial = serial_guard();
    let guard_violations_before = Store::raft_ownership_guard_violations();
    let dir = tempfile::tempdir()?;
    let mut nodes = start_cluster(dir.path())?;
    let (_job_config_id, task_id) = seed_fixtures(&cluster_stores(&nodes), "surv", 1)?;

    for round in 0..nodes.len() {
        let victim_name = nodes[round].name;
        let key = format!("surv/{round}");
        let (alloc_id, health_round) = {
            let stores = cluster_stores(&nodes);
            // One acked write per lane, each via that lane's leader.
            via_lanes_leader(&stores, &["control"], OP_PATIENCE, |s| {
                s.put_kv(&key, "durable", KvValueType::String, None, None)
            })?;
            let alloc = via_lanes_leader(&stores, &["scheduler"], OP_PATIENCE, |s| {
                s.place_alloc("surv-n-0", task_id)
            })?;
            let health_round = (round + 1) as u64;
            via_lanes_leader(&stores, &["status"], OP_PATIENCE, |s| {
                s.update_node_health_checks("surv-n-0", &format!("{{\"round\": {health_round}}}"))
            })?;
            (alloc.id, health_round)
        };

        nodes[round].kill();
        let survivors: Vec<&TestNode> = nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != round)
            .map(|(_, n)| n)
            .collect();
        wait_for(
            Duration::from_secs(240),
            &format!("all three acked writes on both survivors after killing {victim_name}"),
            || {
                survivors.iter().all(|n| {
                    let store = n.store();
                    kv_value(store, &key).as_deref() == Some("durable")
                        && store
                            .allocs_for_node("surv-n-0")
                            .map(|allocs| allocs.iter().any(|a| a.id == alloc_id))
                            .unwrap_or(false)
                        && store
                            .get_node("surv-n-0")
                            .ok()
                            .and_then(|node| node.health_checks.get("round")?.as_u64())
                            .is_some_and(|r| r >= health_round)
                })
            },
        );

        // Bring the victim back and re-converge (every lane led again, and
        // byte-identical state) so the next round starts from a full,
        // identical 3-node cluster.
        nodes[round].restart()?;
        let all: Vec<&TestNode> = nodes.iter().collect();
        wait_for(
            Duration::from_secs(240),
            "restarted node catches up, every lane led, and all replicas converge",
            || {
                kv_value(nodes[round].store(), &key).as_deref() == Some("durable")
                    && LANES
                        .iter()
                        .all(|lane| all.iter().any(|n| n.leads_lane(lane)))
                    && converged(&all)
            },
        );
    }
    let all: Vec<&TestNode> = nodes.iter().collect();
    assert_lane_groups_converged(&all, "after the kill/restart rounds")?;
    assert_eq!(
        Store::raft_ownership_guard_violations(),
        guard_violations_before,
        "no op in the kill/restart rounds may trip the ownership guard (JC-26)"
    );
    Ok(())
}

// ── 4. the capture guard stays quiet (and is provably live) ──────────────

/// A representative workload spanning every lane — including the split
/// composites (registration, job submit, destroy) whose pre-C-2 shapes
/// were the guard's actual violators — leaves the process-global
/// ownership-violation counter untouched. Then a positive control (the
/// JC-36 escape hatch: raw operator SQL writing a scheduler-owned table on
/// the control lane) proves the counter is a live hook, not a dead zero:
/// the op is rejected before proposal, the counter increments, and the
/// foreign row exists on no replica.
///
/// Tests 1–3 assert a zero delta over their own (heavier) workloads, so
/// together the battery proves zero violations across all of it.
#[test]
fn lanes_capture_guard_stays_quiet() -> R<()> {
    let _serial = serial_guard();
    let guard_violations_before = Store::raft_ownership_guard_violations();
    let dir = tempfile::tempdir()?;
    let nodes = start_cluster(dir.path())?;
    let stores = cluster_stores(&nodes);
    let (job_config_id, task_id) = seed_fixtures(&stores, "quiet", 2)?;

    // Control lane.
    via_lanes_leader(&stores, &["control"], OP_PATIENCE, |s| {
        s.put_kv("quiet/kv", "{}", KvValueType::Json, None, None)
    })?;
    via_lanes_leader(&stores, &["control"], OP_PATIENCE, |s| {
        s.patch_kv(
            "quiet/kv",
            "[{\"op\": \"add\", \"path\": \"/patched\", \"value\": true}]",
            None,
            None,
        )
    })?;
    via_lanes_leader(&stores, &["control"], OP_PATIENCE, |s| {
        s.delete_kv("quiet/kv", None, None)
    })?;
    // Scheduler lane: direct placement, slot instance, epoch-CAS plan.
    let placed = via_lanes_leader(&stores, &["scheduler"], OP_PATIENCE, |s| {
        s.place_alloc("quiet-n-0", task_id)
    })?;
    via_lanes_leader(&stores, &["scheduler"], OP_PATIENCE, |s| {
        let instance = s.get_or_create_slot_instance("quiet-base", "svc", "svc", 0)?;
        s.set_instance_state(
            &instance.instance_id,
            legion::structs::InstanceState::Active,
        )
    })?;
    via_lanes_leader(&stores, &["scheduler"], OP_PATIENCE, |s| {
        let epoch = s.allocation_epoch_for_job_config(&job_config_id)?;
        let versions = BTreeMap::from([
            (format!("job_config:{job_config_id}"), epoch),
            ("node:quiet-n-1".to_string(), 0),
        ]);
        let plan = s.insert_plan_submission(
            &job_config_id,
            "fresh",
            &[("quiet-n-1".to_string(), task_id)],
            &versions,
        )?;
        s.apply_plan_submission(&plan)
    })?;
    // Status lane.
    via_lanes_leader(&stores, &["status"], OP_PATIENCE, |s| {
        s.update_node_heartbeat("quiet-n-0".to_string())
    })?;
    via_lanes_leader(&stores, &["status"], OP_PATIENCE, |s| {
        s.update_node_health_checks("quiet-n-0", "{\"worker\": \"pass\"}")
    })?;
    via_lanes_leader(&stores, &["status"], OP_PATIENCE, |s| {
        s.set_task_status(task_id, TaskStatusState::Running)
    })?;
    // Cross-lane composites: allocation removal, stop, and the three-lane
    // destroy (status cleanup → scheduler cleanup → control delete —
    // proposed per lane from the calling node, so it needs a node leading
    // all three).
    via_lanes_leader(&stores, &["scheduler"], OP_PATIENCE, |s| {
        s.remove_allocation_by_id(placed.id)
    })?;
    via_lanes_leader(&stores, &["control"], OP_PATIENCE, |s| {
        s.stop_job("quiet-base")
    })?;
    via_lanes_leader(
        &stores,
        &["control", "scheduler", "status"],
        OP_PATIENCE,
        |s| s.destroy_job("quiet-base"),
    )?;

    let all: Vec<&TestNode> = nodes.iter().collect();
    wait_for(
        Duration::from_secs(240),
        "the representative workload converges everywhere",
        || converged(&all),
    );
    assert_lane_groups_converged(&all, "after the representative workload")?;
    assert_eq!(
        Store::raft_ownership_guard_violations(),
        guard_violations_before,
        "the representative every-lane workload must trip zero ownership-guard \
         violations (JC-26)"
    );

    // Positive control: a control-lane op writing a scheduler-owned table
    // MUST trip the guard — proving the counter observed the whole quiet
    // workload above, rather than being disconnected. (The guard rejection
    // is non-transient, so the router propagates it after at most one
    // successful attempt on the control leader.)
    let before_control = Store::raft_ownership_guard_violations();
    let err = via_lanes_leader(&stores, &["control"], OP_PATIENCE, |s| {
        s.query_state_store(
            "INSERT INTO plan_submission (job_config_id, mode, placements, expected_epoch, \
             status, created_at) VALUES ('jc-guard', 'fresh', '[]', 0, 'pending', \
             '2026-01-01T00:00:00Z');",
            true,
        )
    })
    .expect_err("raw SQL writing a scheduler table on the control lane must be rejected");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("ownership violation") && rendered.contains("plan_submission"),
        "rejection must be the ownership-guard error: {rendered}"
    );
    assert_eq!(
        Store::raft_ownership_guard_violations(),
        before_control + 1,
        "the guard violation counter must record the rejected op"
    );
    // Rejected BEFORE proposal: the foreign row exists on no replica.
    for node in &all {
        let result = node.store().query_state_store(
            "SELECT COUNT(*) AS n FROM plan_submission WHERE job_config_id = 'jc-guard';",
            false,
        )?;
        assert_eq!(
            result.rows.first().map(|row| row.json.as_str()),
            Some("{\"n\":0}"),
            "the guard-rejected write must not exist on {}",
            node.name
        );
    }
    Ok(())
}
