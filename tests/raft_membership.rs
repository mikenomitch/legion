//! In-process 3-node raft cluster lifecycle (phase A-2 of
//! `docs/plans/durability-and-ops-execution-plan.md` §2): bootstrap leads,
//! joiners enroll via join token and become voters, committed writes are
//! readable on followers after apply, non-leader writes are rejected with a
//! leader hint, a killed leader is replaced and writes continue, and a
//! restarted member rejoins and converges to byte-identical state
//! (content-hashed across replicas).
//!
//! # Networking exception
//!
//! The repo's tests are deliberately in-process and network-free. This test
//! is the documented exception: raft peers only talk through the
//! `RaftTransport` gRPC contract, so the harness runs three real tonic
//! servers — serving ONLY that transport service — on ephemeral loopback
//! ports. Everything else (stores, drivers, state machines) stays
//! in-process; no manager daemon, no worker, no public API listener.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::KvValueType;

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
    server: Option<tokio::task::JoinHandle<()>>,
}

impl TestNode {
    fn store(&self) -> &Store {
        self.store.as_ref().expect("node is running")
    }

    /// Mounts the node's `RaftTransport` service on `127.0.0.1:port`.
    /// SO_REUSEADDR so a killed node can rebind its advertised port on
    /// restart (membership records the address; it must stay stable).
    fn start_transport(&mut self, rt: &tokio::runtime::Runtime) {
        let service = self.store().raft_transport_server();
        let addr: SocketAddr = format!("127.0.0.1:{}", self.port).parse().unwrap();
        let name = self.name;
        self.server = Some(rt.spawn(async move {
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
        }));
        // Let the listener reach accept() before anyone dials it.
        std::thread::sleep(Duration::from_millis(50));
    }

    /// Kill the node: stop serving its transport and drop its store (which
    /// shuts down the raft engine, log store, and pipeline threads).
    fn kill(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
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
    rt: &tokio::runtime::Runtime,
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
        server: None,
    };
    node.store = Some(Store::start_storage(&config)?);
    node.start_transport(rt);
    Ok(node)
}

fn kv_value(store: &Store, key: &str) -> Option<String> {
    store.get_kv(key).ok().flatten().map(|entry| entry.value)
}

/// Full lifecycle in one test (the cluster bring-up is the expensive part;
/// each stage depends on the previous one anyway).
#[test]
fn three_node_cluster_membership_failover_and_convergence() -> R<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let root = dir.path();

    // ── Bootstrap node leads ─────────────────────────────────────────────
    let mut a = start_node(&rt, root, "mgr-a", true, None)?;
    let status = a.store().raft_status().context("mgr-a raft status")?;
    assert_eq!(status.role.as_str(), "primary", "bootstrap node must lead");
    assert_eq!(status.leader_id.as_deref(), Some("mgr-a"));
    assert_eq!(status.voters, vec!["mgr-a".to_string()]);

    // A committed write before anyone joins: joiners must receive it via
    // log replay (snapshots land in A-3).
    a.store()
        .put_kv("cluster/seed", "pre-join", KvValueType::String, None, None)?;

    // ── Two joiners enroll via join token and become voters ──────────────
    // The joins are deliberately SEQUENTIAL: the first promotion must take
    // the cluster through exactly 2 voters so the even-parity advisory
    // (plan §7-C3, JC-29) is deterministic — concurrent joins can interleave
    // so that a promotion computes its resulting count against the other
    // join's in-flight joint membership and never observes 2.
    //
    // Generous deadlines: enrollment needs log catch-up plus two membership
    // changes per lane per joiner, and this test shares the machine with the
    // (heavy) durability suite when the full test suite runs in parallel.
    let (token, _expires) = a.store().issue_raft_join_token("test")?;
    let b = start_node(&rt, root, "mgr-b", false, Some(token.clone()))?;
    wait_for(
        Duration::from_secs(120),
        "first joiner promoted to voter",
        || {
            a.store()
                .raft_status()
                .map(|s| s.voters.contains(&"mgr-b".to_string()))
                .unwrap_or(false)
        },
    );

    // ── Even-voter-parity warning lands in the audit history ─────────────
    // mgr-b's promotion took the cluster through 2 voters (even): JC-29
    // never withholds promotion, but the plain-language advisory must be
    // recorded as a membership event on the leader. The join handler pushes
    // its history events AFTER the membership change the voter-wait above
    // observed, so poll rather than assert a snapshot.
    wait_for(
        Duration::from_secs(30),
        "join promotion through 2 voters records the parity advisory",
        || {
            a.store().raft_history(500).iter().any(|event| {
                event.event_type == "membership"
                    && event.message.contains("2 voters need both alive to commit")
            })
        },
    );

    let c = start_node(&rt, root, "mgr-c", false, Some(token))?;
    wait_for(
        Duration::from_secs(120),
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

    // ── A leader write becomes readable on followers after apply ─────────
    a.store()
        .put_kv("cluster/x", "1", KvValueType::String, None, None)?;
    for follower in [&b, &c] {
        let store = follower.store().clone();
        wait_for(
            Duration::from_secs(90),
            "leader writes visible on follower",
            || {
                kv_value(&store, "cluster/x").as_deref() == Some("1")
                    && kv_value(&store, "cluster/seed").as_deref() == Some("pre-join")
            },
        );
    }

    // ── Non-leader writes are rejected with a leader hint ────────────────
    let err = b
        .store()
        .put_kv("cluster/nope", "x", KvValueType::String, None, None)
        .expect_err("follower must reject writes");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("not the raft leader"),
        "follower rejection must say it is not the leader: {rendered}"
    );
    assert!(
        rendered.contains("mgr-a"),
        "follower rejection must name the leader: {rendered}"
    );

    // ── Leader kill → new leader elected → writes continue ───────────────
    a.kill();
    let survivors = [&b, &c];
    let mut new_leader: Option<&TestNode> = None;
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline && new_leader.is_none() {
        for node in survivors {
            let is_leader = node
                .store()
                .raft_status()
                .map(|s| s.role.as_str() == "primary")
                .unwrap_or(false);
            if is_leader {
                new_leader = Some(node);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let new_leader = new_leader.expect("a survivor must take over leadership");
    assert_ne!(new_leader.name, "mgr-a");

    new_leader.store().put_kv(
        "cluster/after-failover",
        "2",
        KvValueType::String,
        None,
        None,
    )?;
    let other = if new_leader.name == "mgr-b" { &c } else { &b };
    {
        let store = other.store().clone();
        wait_for(
            Duration::from_secs(90),
            "post-failover write visible on the other survivor",
            || kv_value(&store, "cluster/after-failover").as_deref() == Some("2"),
        );
    }

    // ── Restarted node rejoins and converges ─────────────────────────────
    a.restart(&rt)?;
    {
        let store = a.store().clone();
        wait_for(
            Duration::from_secs(120),
            "restarted node catches up with post-failover writes",
            || kv_value(&store, "cluster/after-failover").as_deref() == Some("2"),
        );
    }

    // One more write on the current leader (whoever holds it now — the
    // restarted node may have retaken leadership), then all three replicas
    // must converge to byte-identical replicated state.
    let all = [&a, &b, &c];
    let current_leader = all
        .iter()
        .find(|n| {
            n.store()
                .raft_status()
                .map(|s| s.role.as_str() == "primary")
                .unwrap_or(false)
        })
        .expect("cluster must have a leader after rejoin");
    current_leader
        .store()
        .put_kv("cluster/final", "3", KvValueType::String, None, None)?;

    wait_for(
        Duration::from_secs(120),
        "all replicas converge to identical content hashes",
        || {
            let hashes: Vec<Option<String>> = all
                .iter()
                .map(|n| n.store().state_content_hash().ok())
                .collect();
            match (&hashes[0], &hashes[1], &hashes[2]) {
                (Some(h0), Some(h1), Some(h2)) => {
                    h0 == h1
                        && h1 == h2
                        && all
                            .iter()
                            .all(|n| kv_value(n.store(), "cluster/final").as_deref() == Some("3"))
                }
                _ => false,
            }
        },
    );

    // ── Membership ergonomics (plan §7-C3, JC-29) ────────────────────────
    // Shrink the healthy 3-voter cluster on its current leader:
    //   3 -> 2 voters: proceeds, but the even-parity advisory is threaded
    //     through the command outcome AND recorded in the audit history;
    //   2 -> 1 voter: odd result, no warning;
    //   1 -> 0 voters: refused outright with the advisory (the only hard
    //     block — a zero-voter cluster can never commit again).
    let leader = *all
        .iter()
        .find(|n| {
            n.store()
                .raft_status()
                .map(|s| s.role.as_str() == "primary")
                .unwrap_or(false)
        })
        .expect("cluster must still have a leader");
    let mut others = all.iter().filter(|n| n.name != leader.name);
    let first_removed = others.next().expect("two non-leaders");
    let second_removed = others.next().expect("two non-leaders");

    let outcome = leader.store().issue_raft_command(
        "remove-member",
        Some(first_removed.name),
        "parity-test",
    )?;
    let warning = outcome
        .warning
        .expect("removing down to 2 voters must warn about even parity");
    assert!(
        warning.contains("2 voters need both alive to commit"),
        "even-parity warning must carry the advisory: {warning}"
    );
    assert!(
        leader.store().raft_history(500).iter().any(|event| {
            event.event_type == "membership"
                && event.actor == "parity-test"
                && event.message.contains("2 voters need both alive to commit")
        }),
        "the even-parity advisory must land in raft history"
    );
    // The guard computes on the leader's metrics view, which updates
    // asynchronously after the membership change commits — wait for the
    // shrunken voter set before the next removal so its resulting-count
    // computation starts from 2, not a stale 3.
    wait_for(Duration::from_secs(30), "first removal visible", || {
        leader
            .store()
            .raft_status()
            .map(|s| s.voters.len() == 2)
            .unwrap_or(false)
    });

    let outcome = leader.store().issue_raft_command(
        "remove-member",
        Some(second_removed.name),
        "parity-test",
    )?;
    assert!(
        outcome.warning.is_none(),
        "removing down to 1 voter (odd) must not warn: {:?}",
        outcome.warning
    );
    wait_for(Duration::from_secs(30), "second removal visible", || {
        leader
            .store()
            .raft_status()
            .map(|s| s.voters.len() == 1)
            .unwrap_or(false)
    });

    let err = leader
        .store()
        .issue_raft_command("remove-member", Some(leader.name), "parity-test")
        .expect_err("removing the last voter must be refused");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("last raft voter") && rendered.contains("zero voters"),
        "refusal must carry the zero-voters advisory: {rendered}"
    );

    Ok(())
}

/// KV encryption at rest across a cluster (docs/kv-encryption.md): the
/// bootstrap node mints the root key and the first data key at start; a
/// joiner receives the root key as part of joining and reads values that
/// were encrypted before it existed; the leader's upgrade gate tracks what
/// every voter advertises on the transport; and a rotation on the leader
/// reaches the follower.
#[test]
fn joiners_receive_the_root_key_and_the_upgrade_gate_tracks_voters() -> R<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let key_file = |name: &str| root.join(name).join("store").join("keys").join("root.key");

    let a = start_node(&rt, root, "mgr-a", true, None)?;
    assert_eq!(
        a.store().keyring().snapshot().state,
        "unlocked",
        "a single-voter bootstrap initializes its keyring at start"
    );
    assert!(
        key_file("mgr-a").exists(),
        "the root key was minted under storePath"
    );
    a.store()
        .put_kv("secrets/db", "hunter2", KvValueType::String, None, None)?;
    let (stored, key_id) = a.store().kv_stored_form("secrets/db")?.expect("row exists");
    assert_ne!(stored, "hunter2", "the row holds ciphertext");
    assert!(key_id.is_some());

    // ── The joiner gets the root key with the join, then reads the value ──
    let (token, _expires) = a.store().issue_raft_join_token("test")?;
    let b = start_node(&rt, root, "mgr-b", false, Some(token))?;
    wait_for(Duration::from_secs(120), "joiner promoted to voter", || {
        a.store()
            .raft_status()
            .map(|s| s.voters.contains(&"mgr-b".to_string()))
            .unwrap_or(false)
    });
    wait_for(
        Duration::from_secs(30),
        "the joiner holds the cluster's root key",
        || key_file("mgr-b").exists(),
    );
    assert_eq!(
        std::fs::read_to_string(key_file("mgr-a"))?,
        std::fs::read_to_string(key_file("mgr-b"))?,
        "the joiner's key file is byte-identical to the leader's"
    );
    {
        let store = b.store().clone();
        wait_for(
            Duration::from_secs(90),
            "the joiner decrypts a value written before it joined",
            || kv_value(&store, "secrets/db").as_deref() == Some("hunter2"),
        );
    }
    assert_eq!(b.store().keyring().snapshot().state, "unlocked");
    let (stored_on_b, _) = b
        .store()
        .kv_stored_form("secrets/db")?
        .expect("replicated row");
    assert_eq!(stored_on_b, stored, "replicas hold the same ciphertext");

    // ── The upgrade gate follows what voters advertise on the transport ──
    assert_eq!(
        a.store().keyring_upgrade_gate(),
        None,
        "every voter advertises the keyring schema"
    );
    let mut saw_closed = false;
    for _ in 0..20 {
        a.store().raft_forget_peer_schema_version("mgr-b")?;
        if let Some(reason) = a.store().keyring_upgrade_gate() {
            assert!(reason.contains("mgr-b"), "{reason}");
            saw_closed = true;
            break;
        }
        // A heartbeat reply re-advertised it between the two calls; retry.
    }
    assert!(saw_closed, "forgetting a voter's version closes the gate");
    wait_for(
        Duration::from_secs(30),
        "the next transport reply re-advertises mgr-b's schema version",
        || a.store().keyring_upgrade_gate().is_none(),
    );

    // ── A rotation on the leader reaches the follower ────────────────────
    let rotated = a.store().rotate_data_key("test")?;
    a.store().put_kv(
        "secrets/after",
        "post-rotation",
        KvValueType::String,
        None,
        None,
    )?;
    {
        let store = b.store().clone();
        wait_for(
            Duration::from_secs(90),
            "the follower reads a value under the rotated key",
            || kv_value(&store, "secrets/after").as_deref() == Some("post-rotation"),
        );
    }
    let rows = b.store().list_keyring_rows()?;
    assert_eq!(rows[0].key_id, rotated.key_id);
    assert_eq!(rows[0].state, "active");
    Ok(())
}
