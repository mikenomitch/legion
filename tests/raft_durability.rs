//! Durability proofs for the real raft engine (plan §2/§4 of
//! `docs/plans/durability-and-ops-execution-plan.md`): the headline claims —
//! no replica divergence under concurrent mixed workloads, no acked-write
//! loss across leader failure or any single-node loss, and no writes
//! accepted by a partitioned minority — each get an in-process 3-node test.
//!
//! Harness: the `tests/raft_membership.rs` pattern — everything in-process,
//! with the documented networking exception that raft peers talk through
//! three real tonic listeners serving ONLY the `RaftTransport` service on
//! ephemeral loopback ports. Node "kill" is either dropping the store
//! (engine shutdown) or, when in-flight callers pin the engine alive, full
//! network isolation via the `Store::raft_set_outbound_blocked` partition
//! test hook plus dropping the inbound listener — indistinguishable from a
//! crashed process from the peers' point of view.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::{JobConfig, TaskConfig};
use legion::structs::{InstanceState, KvValueType, Node, NodeMsg, Submission};
use serde_json::json;

// ── harness (tests/raft_membership.rs pattern) ───────────────────────────

/// Each test drives a full 3-node cluster with tight (150–300ms) election
/// timeouts; running them in parallel starves elections under load and
/// flakes leadership. One at a time is fast enough and deterministic.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial_guard() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

const RETRY_PATIENCE: Duration = Duration::from_secs(45);

/// Run a raft-proposing operation with client-style retries. Under CPU
/// starvation (2-core CI runners, debug builds, three in-process nodes)
/// the tight election timeouts flap leadership, and proposals bounce with
/// leadership/availability errors a real client would simply retry. Only
/// transient raft errors retry; application-level errors (not-found,
/// version conflicts) return immediately — they proposed nothing and are
/// part of the workload semantics under test.
fn with_leadership_retry<T>(
    patience: Duration,
    mut op: impl FnMut() -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let end = Instant::now() + patience;
    loop {
        match op() {
            Ok(v) => return Ok(v),
            Err(err) => {
                let msg = format!("{err:#}").to_lowercase();
                let transient = msg.contains("raft leader")
                    || msg.contains("retry shortly")
                    || msg.contains("replication pipeline")
                    || msg.contains("unreachable")
                    || (msg.contains("raft") && msg.contains("timeout"));
                if !transient || Instant::now() >= end {
                    return Err(err);
                }
                std::thread::sleep(Duration::from_millis(150));
            }
        }
    }
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
/// engine), and a node-owned runtime serving its raft transport.
///
/// The transport runtime is per-node (unlike `tests/raft_membership.rs`)
/// because kill/partition must sever ESTABLISHED peer connections, not just
/// the listener: tonic serves each accepted connection on a spawned task
/// that outlives an abort of the serve future, so only shutting the whole
/// runtime down reliably makes the node dark to its peers.
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

    fn try_store(&self) -> Option<&Store> {
        self.store.as_ref()
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

    /// Kill the node: stop serving its transport and drop its store (which
    /// shuts down the raft engine, log store, and pipeline threads).
    fn kill(&mut self) {
        self.stop_transport();
        self.store = None;
    }

    /// Fully isolate the node from its peers without stopping it: every
    /// outbound raft RPC fails as unreachable (partition test hook) and the
    /// inbound transport — listener and live connections — shuts down. To
    /// the rest of the cluster this is indistinguishable from a dead node.
    fn partition(&mut self) -> R<()> {
        self.store().raft_set_outbound_blocked(true)?;
        self.stop_transport();
        Ok(())
    }

    /// Undo [`Self::partition`]: unblock outbound RPCs and serve the
    /// transport again on the same advertised port.
    fn heal(&mut self) -> R<()> {
        self.store().raft_set_outbound_blocked(false)?;
        self.start_transport();
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
        config: config.clone(),
        port,
        store: None,
        transport_rt: None,
    };
    node.store = Some(Store::start_storage(&config)?);
    node.start_transport();
    Ok(node)
}

/// Bootstrap `mgr-a`, enroll `mgr-b`/`mgr-c` via join token, wait for both
/// to be voters. Returns `[a, b, c]` with `mgr-a` leading.
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

fn kv_value(store: &Store, key: &str) -> Option<String> {
    store.get_kv(key).ok().flatten().map(|entry| entry.value)
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

/// All listed nodes report the same replicated-state content hash (the
/// existing `Store::state_content_hash` helper: every replicated table,
/// rows ordered by every column).
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

/// Write `key=value` through whichever node currently leads, retrying across
/// leadership shifts. Returns the name of the leader that acked it.
fn put_kv_via_leader(nodes: &[&TestNode], key: &str, value: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut last_err = String::new();
    while Instant::now() < deadline {
        if let Some(leader) = find_leader(nodes) {
            match leader
                .store()
                .put_kv(key, value, KvValueType::String, None, None)
            {
                Ok(_) => return leader.name.to_string(),
                Err(err) => last_err = format!("{err:#}"),
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("could not write {key} via a leader within 30s (last error: {last_err})");
}

// ── workload fixtures (tests/raft_apply_roundtrip.rs pattern) ────────────

fn test_node(id: &str) -> Node {
    Node {
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
    }
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

fn submission(name: &str) -> Submission {
    Submission {
        path: format!("{name}.ncl"),
        content: format!("job {name}"),
        is_primary: true,
    }
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

/// Four writer threads run seeded (per-thread deterministic) streams of ~200
/// total mixed store ops — job submit/stop, kv put/patch/delete, node
/// heartbeats, allocation place/remove, instance ops — concurrently against
/// the leader. Whatever interleaving the scheduler produces, changeset
/// replication must leave every replica byte-identical: after a barrier wait
/// for all applies, the content hash over ALL replicated tables is identical
/// on every node.
#[test]
fn determinism_under_interleaving() -> R<()> {
    let _serial = serial_guard();
    let dir = tempfile::tempdir()?;
    let nodes = start_cluster(dir.path())?;
    let leader = &nodes[0];
    assert!(is_leader(leader), "bootstrap node must lead");

    // Fixtures the op mix targets: worker nodes for heartbeats/placement and
    // a base job whose task backs allocations and slot instances.
    for i in 0..4 {
        leader
            .store()
            .insert_node(NodeMsg::from(test_node(&format!("n-{i}"))))?;
    }
    let outcome = leader
        .store()
        .handle_job_submit(&test_job("det-base"), &[submission("det-base")])?;
    let tasks = leader
        .store()
        .get_task_configs_for_job_config(outcome.job_config_id())?;
    let (task_id, _) = tasks.first().context("base job must have a task config")?;
    let task_id = *task_id;

    let alloc_pool: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();
    for t in 0..4u64 {
        let store = leader.store().clone();
        let alloc_pool = alloc_pool.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || -> usize {
            let mut rng = SeededRng::new(0x9E37_79B9_7F4A_7C15 ^ (t + 1));
            barrier.wait();
            let mut acked = 0usize;
            for i in 0..50u64 {
                let r = rng.next();
                // Individual ops may legitimately fail (not-found on a key
                // another thread deleted, stop of a job not yet submitted):
                // a failed op proposes nothing, so it cannot diverge
                // replicas — exactly the property under test.
                let ok = match r % 9 {
                    0 => with_leadership_retry(RETRY_PATIENCE, || {
                        store.put_kv(
                            &format!("det/kv-{}", r % 12),
                            &format!("v{t}-{i}"),
                            KvValueType::String,
                            None,
                            None,
                        )
                    })
                    .is_ok(),
                    1 => with_leadership_retry(RETRY_PATIENCE, || {
                        store.put_kv(
                            &format!("det/json-{}", r % 6),
                            "{}",
                            KvValueType::Json,
                            None,
                            None,
                        )
                    })
                    .is_ok(),
                    2 => with_leadership_retry(RETRY_PATIENCE, || {
                        store.patch_kv(
                            &format!("det/json-{}", r % 6),
                            &format!("{{\"writer\": {t}, \"seq\": {i}}}"),
                            None,
                            None,
                        )
                    })
                    .is_ok(),
                    3 => with_leadership_retry(RETRY_PATIENCE, || {
                        store.delete_kv(&format!("det/kv-{}", r % 12), None, None)
                    })
                    .is_ok(),
                    4 => with_leadership_retry(RETRY_PATIENCE, || {
                        store.update_node_heartbeat(format!("n-{}", r % 4))
                    })
                    .is_ok(),
                    5 => {
                        let name = format!("det-job-{t}-{}", r % 3);
                        with_leadership_retry(RETRY_PATIENCE, || {
                            store.handle_job_submit(&test_job(&name), &[submission(&name)])
                        })
                        .is_ok()
                    }
                    6 => with_leadership_retry(RETRY_PATIENCE, || {
                        store.stop_job(&format!("det-job-{t}-{}", r % 3))
                    })
                    .is_ok(),
                    7 => {
                        if r.is_multiple_of(2) {
                            match with_leadership_retry(RETRY_PATIENCE, || {
                                store.place_alloc(&format!("n-{}", r % 4), task_id)
                            }) {
                                Ok(alloc) => {
                                    alloc_pool.lock().unwrap().push(alloc.id);
                                    true
                                }
                                Err(_) => false,
                            }
                        } else {
                            let popped = alloc_pool.lock().unwrap().pop();
                            match popped {
                                Some(id) => with_leadership_retry(RETRY_PATIENCE, || {
                                    store.remove_allocation_by_id(id)
                                })
                                .is_ok(),
                                None => true,
                            }
                        }
                    }
                    _ => match with_leadership_retry(RETRY_PATIENCE, || {
                        store.get_or_create_slot_instance("det-base", "svc", "svc", (r % 3) as u32)
                    }) {
                        Ok(instance) => with_leadership_retry(RETRY_PATIENCE, || {
                            store.set_instance_state(
                                &instance.instance_id,
                                if r.is_multiple_of(2) {
                                    InstanceState::Active
                                } else {
                                    InstanceState::Rescheduling
                                },
                            )
                        })
                        .is_ok(),
                        Err(_) => false,
                    },
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
        "the mixed workload must mostly succeed (acked {total_acked}/200)"
    );

    // Barrier-wait for applies, then the determinism claim itself: all
    // replicated tables content-hash identically on every node.
    let all: Vec<&TestNode> = nodes.iter().collect();
    wait_for(
        Duration::from_secs(150),
        "all replicas converge to identical content hashes",
        || converged(&all),
    );
    for node in &all {
        assert_eq!(
            node.store().get_job("det-base")?.id,
            "det-base",
            "replicated state must be non-trivial on {}",
            node.name
        );
    }
    Ok(())
}

// ── 2. leader loss mid-burst ─────────────────────────────────────────────

/// Kill the leader while a 4-thread write burst is in flight against it:
/// a survivor takes over, every write that was ACKED to a caller is present
/// on the new leader, writes that were in flight but never acked are either
/// applied on every survivor or on none (probed by read-back), and the
/// cluster accepts writes again.
///
/// "Kill" here is full network isolation (outbound-block hook + inbound
/// listener drop) plus dropping the harness's store handle: burst callers
/// blocked inside the dead leader pin its engine object in memory, but from
/// the peers' point of view the node is gone — the same observable state as
/// a crashed process. The blocked caller threads are deliberately leaked.
#[test]
fn leader_loss_mid_burst() -> R<()> {
    let _serial = serial_guard();
    let dir = tempfile::tempdir()?;
    let mut nodes = start_cluster(dir.path())?;
    let leader_name = {
        let all: Vec<&TestNode> = nodes.iter().collect();
        find_leader(&all).expect("cluster must have a leader").name
    };
    let leader_idx = nodes
        .iter()
        .position(|n| n.name == leader_name)
        .expect("leader is a member");

    let acked: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let attempted: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let stop = Arc::new(AtomicBool::new(false));
    for t in 0..4u32 {
        let store = nodes[leader_idx].store().clone();
        let acked = acked.clone();
        let attempted = attempted.clone();
        let stop = stop.clone();
        // Not joined: a thread blocked inside the killed leader never
        // returns (see the test doc comment).
        std::thread::spawn(move || {
            for i in 0..u32::MAX {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let key = format!("burst/{t}/{i}");
                attempted.lock().unwrap().insert(key.clone());
                if store
                    .put_kv(&key, "payload", KvValueType::String, None, None)
                    .is_ok()
                {
                    acked.lock().unwrap().insert(key);
                }
            }
        });
    }

    // Let a healthy chunk of the burst commit, then kill mid-flight.
    {
        let acked = acked.clone();
        wait_for(Duration::from_secs(180), "burst underway", move || {
            acked.lock().unwrap().len() >= 40
        });
    }
    nodes[leader_idx].partition()?;
    stop.store(true, Ordering::SeqCst);
    nodes[leader_idx].store = None;

    // A new leader emerges among the survivors...
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
    assert_ne!(new_leader.name, leader_name);

    // ...and the survivors converge with each other. Give acks that were
    // racing the kill a moment to land in the set first: any ack recorded
    // after this snapshot was committed pre-partition anyway, so it is a
    // subset of what the convergence assertions below already cover.
    std::thread::sleep(Duration::from_secs(1));
    let acked: HashSet<String> = acked.lock().unwrap().clone();
    let attempted: HashSet<String> = attempted.lock().unwrap().clone();
    assert!(!acked.is_empty(), "burst must have acked writes");
    assert!(
        attempted.len() > acked.len(),
        "the kill must have interrupted unfinished writes (attempted {}, acked {})",
        attempted.len(),
        acked.len()
    );
    wait_for(Duration::from_secs(150), "survivors converge", || {
        converged(&survivors)
    });

    // Every ACKED write survived onto the new leader (and its peer).
    for key in &acked {
        for survivor in &survivors {
            assert_eq!(
                kv_value(survivor.store(), key).as_deref(),
                Some("payload"),
                "acked write {key} must be present on survivor {}",
                survivor.name
            );
        }
    }
    // Un-acked in-flight writes: all-or-nothing. Probe each attempted key by
    // read-back — present on both survivors or on neither.
    for key in attempted.difference(&acked) {
        assert_eq!(
            kv_value(survivors[0].store(), key),
            kv_value(survivors[1].store(), key),
            "in-flight write {key} must be applied everywhere or nowhere"
        );
    }

    // The cluster accepts writes again.
    new_leader
        .store()
        .put_kv("burst/after-failover", "2", KvValueType::String, None, None)?;
    let other = survivors
        .iter()
        .find(|n| n.name != new_leader.name)
        .expect("two survivors");
    let other_store = other.store().clone();
    wait_for(
        Duration::from_secs(120),
        "post-failover write visible on the other survivor",
        move || kv_value(&other_store, "burst/after-failover").as_deref() == Some("2"),
    );
    Ok(())
}

// ── 3. minority partition rejects writes ─────────────────────────────────

/// Isolate one follower (outbound-block hook + inbound listener drop):
/// writes against it fail with the typed not-a-leader rejection carrying the
/// leader hint, the majority keeps committing, the isolated node sees none
/// of it, and after healing the partition it converges byte-identically.
#[test]
fn minority_partition_rejects_writes() -> R<()> {
    let _serial = serial_guard();
    let dir = tempfile::tempdir()?;
    let mut nodes = start_cluster(dir.path())?;

    // Isolate a follower, keeping the leader in the majority.
    let isolated_idx = nodes
        .iter()
        .position(|n| !is_leader(n))
        .expect("cluster of three must have a follower");

    // Baseline write visible everywhere pre-partition.
    {
        let all: Vec<&TestNode> = nodes.iter().collect();
        put_kv_via_leader(&all, "part/before", "0");
        wait_for(
            Duration::from_secs(120),
            "baseline write visible everywhere",
            || {
                all.iter()
                    .all(|n| kv_value(n.store(), "part/before").as_deref() == Some("0"))
            },
        );
    }

    nodes[isolated_idx].partition()?;

    // Writes against the minority node are rejected with the leader-hint
    // error (immediately after the partition it still names the last known
    // leader; once its election timeout fires the hint degrades to "no
    // leader currently known" — both are the typed NotRaftLeader rejection
    // with retry guidance).
    let err = nodes[isolated_idx]
        .store()
        .put_kv("part/rejected", "x", KvValueType::String, None, None)
        .expect_err("isolated minority node must reject writes");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("not the raft leader"),
        "rejection must be the typed not-a-leader error with hint: {rendered}"
    );
    assert!(
        rendered.contains("retry"),
        "rejection must tell the caller where/when to retry: {rendered}"
    );

    // The majority keeps accepting and committing writes...
    let majority: Vec<&TestNode> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != isolated_idx)
        .map(|(_, n)| n)
        .collect();
    put_kv_via_leader(&majority, "part/during", "1");
    wait_for(
        Duration::from_secs(120),
        "majority write visible on both majority nodes",
        || {
            majority
                .iter()
                .all(|n| kv_value(n.store(), "part/during").as_deref() == Some("1"))
        },
    );
    // ...which the isolated node cannot have seen (its inbound is dead).
    assert_eq!(
        kv_value(nodes[isolated_idx].store(), "part/during"),
        None,
        "isolated node must not receive majority commits"
    );
    // And it holds no write it acked itself while isolated.
    assert_eq!(kv_value(nodes[isolated_idx].store(), "part/rejected"), None);

    // Heal: the node rejoins and converges byte-identically.
    nodes[isolated_idx].heal()?;
    {
        let store = nodes[isolated_idx].store().clone();
        wait_for(
            Duration::from_secs(150),
            "healed node catches up with the majority write",
            move || kv_value(&store, "part/during").as_deref() == Some("1"),
        );
    }
    let all: Vec<&TestNode> = nodes.iter().collect();
    wait_for(
        Duration::from_secs(150),
        "all replicas converge after the partition heals",
        || converged(&all),
    );

    // The healed cluster accepts writes and replicates them everywhere.
    put_kv_via_leader(&all, "part/after", "2");
    wait_for(
        Duration::from_secs(120),
        "post-heal write visible everywhere",
        || {
            all.iter()
                .all(|n| kv_value(n.store(), "part/after").as_deref() == Some("2"))
        },
    );
    Ok(())
}

// ── 4. acked write survives any single node loss ─────────────────────────

/// For each node in turn: commit a write via the current leader, kill that
/// node (store drop — engine shutdown), and assert the acked write is
/// present on both survivors (the leader-kill round exercises the case
/// where a committed entry must survive an election). The node is then
/// restarted and the cluster re-converges before the next round.
#[test]
fn acked_write_survives_any_single_node_loss() -> R<()> {
    let _serial = serial_guard();
    let dir = tempfile::tempdir()?;
    let mut nodes = start_cluster(dir.path())?;

    for round in 0..nodes.len() {
        let key = format!("survive/{round}");
        let (leader_name, victim_name) = {
            let all: Vec<&TestNode> = nodes.iter().collect();
            wait_for(Duration::from_secs(150), "cluster has a leader", || {
                find_leader(&all).is_some()
            });
            // Acked write via the leader, whoever holds it this round.
            (put_kv_via_leader(&all, &key, "durable"), all[round].name)
        };

        nodes[round].kill();
        let survivors: Vec<&TestNode> = nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != round)
            .map(|(_, n)| n)
            .collect();
        wait_for(
            Duration::from_secs(150),
            &format!(
                "acked write {key} on both survivors after killing {victim_name} \
                 (leader was {leader_name})"
            ),
            || {
                survivors
                    .iter()
                    .all(|n| kv_value(n.store(), &key).as_deref() == Some("durable"))
            },
        );

        // Bring the victim back and re-converge so the next round starts
        // from a full, identical 3-node cluster.
        nodes[round].restart()?;
        let all: Vec<&TestNode> = nodes.iter().collect();
        wait_for(
            Duration::from_secs(150),
            "restarted node catches up and all replicas converge",
            || {
                kv_value(nodes[round].store(), &key).as_deref() == Some("durable")
                    && converged(&all)
            },
        );
    }
    Ok(())
}

// ── 5. a new leader catches up before it captures (R1) ───────────────────

/// A follower can win an election while entries from the previous term sit
/// in its log unapplied (it had not yet heard they committed). If it
/// captured a write against that stale state, the captured changeset would
/// conflict with the earlier entry on apply and halt the lane on every
/// replica. The new leader must first apply everything before its own term.
///
/// Holds both followers' applies (the apply-pause test hook) so whichever
/// wins is behind, writes on the old leader, isolates it, and writes the
/// same key on the new leader while its apply is still held.
#[test]
fn new_leader_applies_earlier_terms_before_capturing() -> R<()> {
    let _serial = serial_guard();
    let dir = tempfile::tempdir()?;
    let mut nodes = start_cluster(dir.path())?;
    let leader_idx = nodes
        .iter()
        .position(is_leader)
        .expect("cluster must have a leader");
    {
        let all: Vec<&TestNode> = nodes.iter().collect();
        wait_for(Duration::from_secs(150), "initial convergence", || {
            converged(&all)
        });
    }
    let followers: Vec<usize> = (0..nodes.len()).filter(|i| *i != leader_idx).collect();
    for i in &followers {
        nodes[*i].store().raft_set_apply_paused(true)?;
    }

    // The old leader commits `r1/key` (both followers append it; neither
    // applies it).
    nodes[leader_idx]
        .store()
        .put_kv("r1/key", "old", KvValueType::String, None, None)?;
    let committed = nodes[leader_idx]
        .store()
        .raft_status()
        .map(|s| s.applied_index)
        .unwrap_or(0);
    for i in &followers {
        let store = nodes[*i].store().clone();
        wait_for(
            Duration::from_secs(60),
            "follower log holds the old leader's entry",
            move || {
                store
                    .raft_status()
                    .is_some_and(|s| s.commit_index >= committed)
            },
        );
        assert_eq!(kv_value(nodes[*i].store(), "r1/key"), None);
    }

    // Kill the old leader; a held-back follower takes over.
    nodes[leader_idx].partition()?;
    let survivors: Vec<&TestNode> = followers.iter().map(|i| &nodes[*i]).collect();
    let mut new_leader: Option<&TestNode> = None;
    let deadline = Instant::now() + Duration::from_secs(150);
    while Instant::now() < deadline && new_leader.is_none() {
        new_leader = find_leader(&survivors);
        std::thread::sleep(Duration::from_millis(50));
    }
    let new_leader = new_leader.expect("a survivor must take over leadership");
    assert_eq!(kv_value(new_leader.store(), "r1/key"), None);

    // Write the same key on the new leader while its apply is still held.
    // Before the fix this captured an INSERT and the lane halted on apply.
    let writer_store = new_leader.store().clone();
    let writer = std::thread::spawn(move || {
        with_leadership_retry(RETRY_PATIENCE, || {
            writer_store.put_kv("r1/key", "new", KvValueType::String, None, None)
        })
    });
    std::thread::sleep(Duration::from_millis(500));
    for survivor in &survivors {
        survivor.store().raft_set_apply_paused(false)?;
    }
    writer
        .join()
        .expect("writer thread panicked")
        .context("the new leader's write must commit once it has caught up")?;

    // Both survivors agree, and the old leader converges after healing.
    for survivor in &survivors {
        let store = survivor.store().clone();
        wait_for(
            Duration::from_secs(60),
            "survivor applies the write",
            move || kv_value(&store, "r1/key").as_deref() == Some("new"),
        );
    }
    nodes[leader_idx].heal()?;
    let all: Vec<&TestNode> = nodes.iter().collect();
    wait_for(Duration::from_secs(150), "all replicas converge", || {
        kv_value(nodes[leader_idx].store(), "r1/key").as_deref() == Some("new") && converged(&all)
    });
    Ok(())
}
