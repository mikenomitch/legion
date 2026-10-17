//! Snapshots, log compaction, crash recovery, and backup/restore (phase A-3
//! of `docs/plans/durability-and-ops-execution-plan.md` §2, §A5):
//!
//! - single node: writing past `raft.snapshotEntries` produces a whole-DB
//!   snapshot image, purges the raft log behind it (the log DB shrinks), and
//!   a restart from the same directories recovers state + applied index;
//! - three nodes: a follower stopped long enough for the leader to compact
//!   past its log position catches back up via **install-snapshot** (the
//!   needed entries are purged, so replay is impossible) and converges to
//!   byte-identical replicated state;
//! - backup RPC (`Manager/GetStateBackup`) + `--restore-from`-style restore
//!   (`Store::restore_from_backup`) round-trips state into a fresh manager.
//!
//! # Networking exception
//!
//! Like `tests/raft_membership.rs` (whose harness style this reuses), the
//! multi-node test runs real tonic listeners serving ONLY the
//! `RaftTransport` service on ephemeral loopback ports; everything else is
//! in-process.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use legion::manager::server::ManagerDaemonService;
use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::manager_server::Manager;
use legion::structs::{GetStateBackupRequest, KvValueType};

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

    /// The lane's raft log database (single topology: `control`).
    fn log_db_path(&self) -> PathBuf {
        self.raft_data_dir().join("control/log.sqlite")
    }

    /// The lane's snapshots directory (single topology: `control`).
    fn snapshots_dir(&self) -> PathBuf {
        self.raft_data_dir().join("control/snapshots")
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
        // Shutdown signal: killing a real manager kills its process, which
        // closes established peer connections. In-process, aborting only the
        // accept loop would leave established server-side HTTP/2 connections
        // alive, so peers' pooled channels would keep talking to the dead
        // node's stopped raft cores instead of redialing after a restart.
        // Graceful shutdown (GOAWAY) closes them like a process exit would.
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
    /// shuts down the raft engine, log store, and pipeline threads).
    fn kill(&mut self) {
        if let Some((handle, shutdown)) = self.server.take() {
            let _ = shutdown.send(());
            // Graceful shutdown waits for in-flight RPCs; don't let a
            // wedged stream keep the dead node's service alive.
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
    config.persistence.raft.heartbeat_interval_ms = 400;
    config.persistence.raft.election_timeout_min_ms = 1200;
    config.persistence.raft.election_timeout_max_ms = 2000;
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

fn kv_value(store: &Store, key: &str) -> Option<String> {
    store.get_kv(key).ok().flatten().map(|entry| entry.value)
}

/// Number of complete snapshot images (`*.db` with a `*.meta.json` sidecar)
/// in a lane's snapshots directory.
fn snapshot_count(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                return false;
            };
            name.strip_suffix(".meta.json")
                .is_some_and(|id| dir.join(format!("{id}.db")).exists())
        })
        .count()
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

const SNAPSHOT_ENTRIES: u64 = 16;
const WRITES: usize = 60;

/// Byte size of the largest complete snapshot image in a lane's snapshots
/// directory (0 if there is none).
fn largest_snapshot_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?.to_string();
            let id = name.strip_suffix(".db")?;
            dir.join(format!("{id}.meta.json"))
                .exists()
                .then(|| entry.metadata().ok())
                .flatten()
                .map(|m| m.len())
        })
        .max()
        .unwrap_or(0)
}

/// (a) Single node: write past `snapshotEntries` → a snapshot image exists
/// and the log is purged behind it (the log DB shrinks); a restart from the
/// same directories recovers state + applied index and keeps serving writes.
#[test]
fn single_node_snapshot_compaction_and_restart_recovery() -> R<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let mut a = start_node(&rt, dir.path(), "mgr-a", true, None, SNAPSHOT_ENTRIES)?;

    for i in 0..WRITES {
        a.store().put_kv(
            &format!("bulk/{i}"),
            &format!("value-{i}"),
            KvValueType::String,
            None,
            None,
        )?;
    }

    // Snapshot trigger (snapshot_policy = LogsSinceLast(snapshotEntries))
    // and log purge (max_in_snapshot_log_to_keep = snapshotEntries / 2) are
    // asynchronous behind the applies; wait for both.
    let snapshots_dir = a.snapshots_dir();
    let log_db = a.log_db_path();
    wait_for(
        Duration::from_secs(120),
        "snapshot built and raft log purged behind it",
        || {
            snapshot_count(&snapshots_dir) >= 1
                && log_state(&log_db)
                    .is_some_and(|(rows, purged)| purged.is_some() && rows < WRITES as i64)
        },
    );
    let (rows_before_restart, purged) = log_state(&log_db).expect("log db is readable");
    assert!(
        purged.is_some(),
        "compaction must record a purge marker in the log db"
    );
    assert!(
        rows_before_restart < WRITES as i64,
        "log db must shrink below the number of writes after compaction \
         (rows={rows_before_restart}, writes={WRITES})"
    );

    // Restart from the same directories: state machine resumes from the
    // applied marker inside the state DB; the (compacted) log replays the
    // tail above it.
    a.kill();
    a.restart(&rt)?;
    for i in 0..WRITES {
        assert_eq!(
            kv_value(a.store(), &format!("bulk/{i}")).as_deref(),
            Some(format!("value-{i}").as_str()),
            "state intact after restart (key bulk/{i})"
        );
    }
    let status = a.store().raft_status().context("raft status")?;
    assert!(
        status.applied_index >= WRITES as u64,
        "applied index recovered: {} < {WRITES}",
        status.applied_index
    );

    // The restarted node keeps accepting writes on top of the recovered log.
    a.store()
        .put_kv("after/restart", "ok", KvValueType::String, None, None)?;
    assert_eq!(
        kv_value(a.store(), "after/restart").as_deref(),
        Some("ok"),
        "writes work after crash recovery"
    );
    Ok(())
}

/// (b) Three nodes: a follower stopped past the leader's compaction horizon
/// can no longer catch up by log replay (those entries are purged) and must
/// converge via install-snapshot — verified by content-hash equality plus
/// the installed snapshot artifact on the restarted follower.
#[test]
fn laggard_follower_converges_via_install_snapshot() -> R<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let root = dir.path();

    let a = start_node(&rt, root, "mgr-a", true, None, SNAPSHOT_ENTRIES)?;
    let (token, _expires) = a.store().issue_raft_join_token("test")?;
    let b = start_node(
        &rt,
        root,
        "mgr-b",
        false,
        Some(token.clone()),
        SNAPSHOT_ENTRIES,
    )?;
    let mut c = start_node(&rt, root, "mgr-c", false, Some(token), SNAPSHOT_ENTRIES)?;

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

    // Leader-only backup: a follower refuses with the leader hint (§A5).
    let follower = if a
        .store()
        .raft_status()
        .map(|s| s.role.as_str() == "primary")
        .unwrap_or(false)
    {
        &b
    } else {
        &a
    };
    let err = follower
        .store()
        .state_backup(None)
        .expect_err("state backup on a follower must be rejected");
    assert!(
        format!("{err:#}").contains("not the raft leader"),
        "follower backup rejection must carry the leader hint: {err:#}"
    );

    // Seed writes reach c, then c goes down.
    a.store()
        .put_kv("seed/before", "1", KvValueType::String, None, None)?;
    {
        let store = c.store().clone();
        wait_for(Duration::from_secs(90), "seed write visible on c", || {
            kv_value(&store, "seed/before").as_deref() == Some("1")
        });
    }
    let c_applied_at_kill = c
        .store()
        .raft_status()
        .context("c raft status")?
        .applied_index;
    c.kill();

    // Write far past the compaction horizon while c is down. The leader
    // snapshots every SNAPSHOT_ENTRIES entries and purges the log behind
    // the snapshot, so the entries c needs next disappear from the log —
    // replay becomes impossible and only install-snapshot can catch c up.
    for i in 0..WRITES {
        a.store().put_kv(
            &format!("bulk/{i}"),
            &format!("value-{i}"),
            KvValueType::String,
            None,
            None,
        )?;
    }
    let leader_log_db = a.log_db_path();
    wait_for(
        Duration::from_secs(120),
        "leader compacted its log past the dead follower's position",
        || {
            log_state(&leader_log_db)
                .is_some_and(|(_, purged)| purged.is_some_and(|p| p > c_applied_at_kill))
        },
    );

    // Restart c: it must converge to the leader's exact replicated state.
    c.restart(&rt)?;
    {
        let a_store = a.store().clone();
        let b_store = b.store().clone();
        let c_store = c.store().clone();
        // Generous timeout: the restarted follower spends up to 15s
        // leaderless (its inbound transport comes up only after engine
        // bring-up — see driver.rs), and its interim elections can bump
        // terms and force re-elections before the snapshot transfer lands.
        wait_for(
            Duration::from_secs(180),
            "restarted follower converges via install-snapshot",
            || {
                kv_value(&c_store, &format!("bulk/{}", WRITES - 1)).is_some()
                    && match (
                        a_store.state_content_hash().ok(),
                        b_store.state_content_hash().ok(),
                        c_store.state_content_hash().ok(),
                    ) {
                        (Some(ha), Some(hb), Some(hc)) => ha == hb && hb == hc,
                        _ => false,
                    }
            },
        );
    }
    // The catch-up really went through the snapshot path: the follower
    // persisted the installed image (state_machine.rs keeps it to serve
    // laggards / restarts).
    assert!(
        snapshot_count(&c.snapshots_dir()) >= 1,
        "restarted follower must hold the installed snapshot artifact"
    );
    // And c keeps applying live replication on top of the installed DB.
    let leader = [&a, &b]
        .into_iter()
        .find(|n| {
            n.store()
                .raft_status()
                .map(|s| s.role.as_str() == "primary")
                .unwrap_or(false)
        })
        .expect("cluster has a leader");
    leader
        .store()
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

/// (b2) A snapshot LARGER than the old transport ceiling installs.
///
/// openraft's chunks and append-entries batches are serde_json-encoded, so a
/// binary payload used to render as a JSON number array (~4 bytes of JSON per
/// raw byte) and tonic's default 4 MiB message limit applied on both sides.
/// Together those capped installable snapshots at roughly 1 MiB raw: a
/// bigger one produced an oversized chunk that the receiver rejected and
/// openraft hot-retried forever, so a follower could never catch up and the
/// cluster silently lost its ability to add or recover members (adversarial
/// finding, 2026-07-24). The fix is base64 payloads plus a documented 64 MiB
/// ceiling; this test pins the outcome rather than the mechanism — seed
/// enough state that the leader's snapshot image comfortably exceeds the old
/// 4 MiB limit, then make a blank joiner catch up through it.
#[test]
fn snapshot_larger_than_the_old_transport_ceiling_installs() -> R<()> {
    /// The limit that used to apply (tonic's default max message size).
    const OLD_CEILING: u64 = 4 * 1024 * 1024;
    /// 64 KiB per value × 96 writes ≈ 6 MiB of payload, comfortably past
    /// OLD_CEILING once SQLite page overhead is added, while staying small
    /// enough to stay a fast test.
    const VALUE_BYTES: usize = 64 * 1024;
    const BIG_WRITES: usize = 96;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let root = dir.path();

    // Snapshot rarely: one image per ~32 entries keeps the repeated whole-DB
    // VACUUM INTO of a growing multi-megabyte DB off the critical path.
    let a = start_node(&rt, root, "mgr-a", true, None, 32)?;

    // Distinct, incompressible-ish values so the image cannot collapse.
    for i in 0..BIG_WRITES {
        let value: String = format!("{i:08}-")
            .chars()
            .cycle()
            .take(VALUE_BYTES)
            .collect();
        a.store()
            .put_kv(&format!("big/{i}"), &value, KvValueType::String, None, None)?;
    }

    // Wait for a snapshot that actually exceeds the old ceiling, and for the
    // log to be purged behind it (so a joiner CANNOT catch up by replay).
    let a_snapshots = a.snapshots_dir();
    let a_log = a.log_db_path();
    wait_for(
        Duration::from_secs(180),
        "leader built a snapshot larger than the old 4 MiB ceiling and purged its log",
        || {
            largest_snapshot_bytes(&a_snapshots) > OLD_CEILING
                && log_state(&a_log).is_some_and(|(_, purged)| purged.is_some())
        },
    );
    let leader_image = largest_snapshot_bytes(&a_snapshots);
    assert!(
        leader_image > OLD_CEILING,
        "test must exercise a snapshot past the old ceiling: {leader_image} bytes"
    );

    // A blank joiner has no choice but install-snapshot: the entries it
    // would need were purged behind that oversized image.
    let (token, _expires) = a.store().issue_raft_join_token("test")?;
    let b = start_node(&rt, root, "mgr-b", false, Some(token), 32)?;
    {
        let a_store = a.store().clone();
        let b_store = b.store().clone();
        wait_for(
            Duration::from_secs(240),
            "joiner converges via an oversized install-snapshot",
            || {
                kv_value(&b_store, &format!("big/{}", BIG_WRITES - 1)).is_some()
                    && match (
                        a_store.state_content_hash().ok(),
                        b_store.state_content_hash().ok(),
                    ) {
                        (Some(ha), Some(hb)) => ha == hb,
                        _ => false,
                    }
            },
        );
    }
    assert!(
        largest_snapshot_bytes(&b.snapshots_dir()) > OLD_CEILING,
        "the joiner must hold the installed oversized image, proving the transfer went \
         through the snapshot path"
    );

    // Live replication still works on top of the installed image.
    a.store()
        .put_kv("after/big-install", "live", KvValueType::String, None, None)?;
    {
        let store = b.store().clone();
        wait_for(
            Duration::from_secs(90),
            "post-install replication reaches the joiner",
            || kv_value(&store, "after/big-install").as_deref() == Some("live"),
        );
    }
    Ok(())
}

/// (c) Backup RPC + restore-on-start round trip: `Manager/GetStateBackup`
/// vacuums a consistent image; `Store::restore_from_backup` (the
/// `--restore-from` implementation) seeds a fresh manager that serves the
/// backed-up state; overwrite requires force.
#[test]
fn backup_rpc_and_restore_on_start_round_trip() -> R<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let root = dir.path();

    let a = start_node(&rt, root, "mgr-a", true, None, 1_000)?;
    for (key, value) in [("app/one", "1"), ("app/two", "2"), ("app/three", "3")] {
        a.store()
            .put_kv(key, value, KvValueType::String, None, None)?;
    }
    let hash_at_backup = a.store().state_content_hash()?;

    // Backup through the real RPC (ACL coverage lives in
    // tests/acl_endpoints.rs; this exercises the handler + store path).
    let svc = ManagerDaemonService::new(a.store().clone());
    let reply = rt
        .block_on(
            svc.get_state_backup(tonic::Request::new(GetStateBackupRequest {
                out_path: None,
            })),
        )
        .expect("backup RPC succeeds on the leader")
        .into_inner();
    let backup_path = PathBuf::from(&reply.path);
    assert!(backup_path.exists(), "backup file exists at reported path");
    assert_eq!(
        reply.bytes,
        std::fs::metadata(&backup_path)?.len(),
        "reported size matches the file"
    );
    assert!(
        reply.applied_index >= 3,
        "backup reports the raft applied index it captures: {}",
        reply.applied_index
    );

    // Restore into a fresh manager directory (what `legion start manager
    // --restore-from` runs before starting), then boot a new single-node
    // cluster from it.
    let restored_port = reserve_port();
    let restored_config = node_config(root, "mgr-restored", restored_port, true, None, 1_000)?;
    Store::restore_from_backup(&restored_config.store_path, &backup_path, false)?;

    // A second restore over the now-present state DB must refuse without
    // force and succeed with it.
    let err = Store::restore_from_backup(&restored_config.store_path, &backup_path, false)
        .expect_err("restore over existing state must require force");
    assert!(
        format!("{err:#}").contains("--force-restore"),
        "refusal names the escape hatch: {err:#}"
    );
    Store::restore_from_backup(&restored_config.store_path, &backup_path, true)?;

    // The backup carries ciphertext plus the WRAPPED data keys, never the
    // root key (docs/kv-encryption.md) — the reply says so, and a restore on
    // a fresh host has to bring the key along exactly as the reply names it.
    let encryption = reply
        .encryption
        .as_ref()
        .expect("backup reply describes its encryption");
    assert!(encryption.enabled, "encryption is on by default");
    assert_eq!(encryption.root_key_provider, "file");
    assert!(
        !encryption.root_key_ids.is_empty(),
        "reply names the root key(s) a restore needs"
    );
    let key_file = PathBuf::from(&encryption.root_key_location);
    assert!(key_file.exists(), "reply names the leader's real key file");
    let restored_key_file = PathBuf::from(&restored_config.store_path)
        .join("keys")
        .join("root.key");
    std::fs::create_dir_all(restored_key_file.parent().unwrap())?;
    std::fs::copy(&key_file, &restored_key_file)?;

    let mut restored = TestNode {
        name: "mgr-restored",
        config: restored_config.clone(),
        port: restored_port,
        store: None,
        server: None,
    };
    restored.store = Some(Store::start_storage(&restored_config)?);
    restored.start_transport(&rt);

    for (key, value) in [("app/one", "1"), ("app/two", "2"), ("app/three", "3")] {
        assert_eq!(
            kv_value(restored.store(), key).as_deref(),
            Some(value),
            "restored manager serves the backed-up state (key {key})"
        );
    }
    assert_eq!(
        restored.store().state_content_hash()?,
        hash_at_backup,
        "restored replicated state is byte-identical to the backup source"
    );
    // The restored node is a live, writable fresh cluster on top of the
    // adopted state.
    restored
        .store()
        .put_kv("app/new", "post-restore", KvValueType::String, None, None)?;
    assert_eq!(
        kv_value(restored.store(), "app/new").as_deref(),
        Some("post-restore")
    );
    Ok(())
}
