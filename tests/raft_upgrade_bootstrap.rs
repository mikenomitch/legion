//! Adversarial regressions for raft bootstrap over an existing state DB
//! (plan §A4 / JC-6) and cluster-identity binding:
//!
//! 1. **Upgrade path**: data written under `local_disk` BEFORE raft is ever
//!    enabled must reach a joining member. Bootstrap now builds a seed
//!    snapshot of the existing DB and purges the log up to it, so joiners
//!    are caught up by snapshot install (which carries the whole DB), not by
//!    log replay from an empty log that describes none of the seed rows.
//!
//! 2. **Re-bootstrap refusal**: a wiped raft data_dir (cluster secret
//!    included) plus `bootstrap=true` must NOT silently mint a brand-new
//!    cluster over the old cluster's state DB — that split-brains against
//!    surviving members. The state DB is stamped with its cluster id at
//!    first engine start; a mismatch refuses startup unless the operator
//!    passes `forceNewCluster`.
//!
//! Same in-process networking exception as `tests/raft_membership.rs`: raft
//! peers only talk through the `RaftTransport` gRPC contract, so the upgrade
//! test runs real tonic servers (transport service only) on ephemeral
//! loopback ports.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::KvValueType;

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

fn reserve_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn raft_node_config(
    root: &std::path::Path,
    name: &str,
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

/// Mounts a store's `RaftTransport` service on `127.0.0.1:port`.
fn serve_transport(
    rt: &tokio::runtime::Runtime,
    store: &Store,
    port: u16,
) -> tokio::task::JoinHandle<()> {
    let service = store.raft_transport_server();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let handle = rt.spawn(async move {
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
            eprintln!("raft transport server exited: {err}");
        }
    });
    std::thread::sleep(Duration::from_millis(50));
    handle
}

fn kv_value(store: &Store, key: &str) -> Option<String> {
    store.get_kv(key).ok().flatten().map(|entry| entry.value)
}

/// JC-6 regression: seed data written under `local_disk` (before raft is
/// ever enabled) must reach a joiner — via the bootstrap seed snapshot —
/// and the replicas must converge to identical content.
#[test]
fn upgrade_from_local_disk_replicates_pre_raft_state_to_joiners() -> R<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let root = dir.path();

    let a_port = reserve_port();
    let a_config = raft_node_config(root, "mgr-a", a_port, true, None)?;

    // Phase 1: the pre-raft deployment — plain local_disk over the SAME
    // store path node A will later bootstrap raft from.
    {
        let mut local_config = a_config.clone();
        local_config.persistence.r#type = PersistenceType::LocalDisk;
        let local = Store::start_storage(&local_config)?;
        local.put_kv("seed/upgrade", "pre-raft", KvValueType::String, None, None)?;
        // Store dropped: the upgrade restarts the manager in raft mode.
    }

    // Phase 2: same manager restarts with persistence.type = raft +
    // bootstrap. The pre-raft rows must survive locally...
    let a = Store::start_storage(&a_config)?;
    let _a_server = serve_transport(&rt, &a, a_port);
    assert_eq!(
        kv_value(&a, "seed/upgrade").as_deref(),
        Some("pre-raft"),
        "bootstrap over an existing DB must keep its data"
    );

    // Phase 3: a fresh member joins. It has NONE of the pre-raft rows and
    // no log entry describes them — only the bootstrap seed snapshot can
    // deliver them.
    let (token, _expires) = a.issue_raft_join_token("test")?;
    let b_port = reserve_port();
    let b_config = raft_node_config(root, "mgr-b", b_port, false, Some(token))?;
    let b = Store::start_storage(&b_config)?;
    let _b_server = serve_transport(&rt, &b, b_port);

    wait_for(Duration::from_secs(120), "joiner promoted to voter", || {
        a.raft_status()
            .map(|s| s.voters.contains(&"mgr-b".to_string()))
            .unwrap_or(false)
    });

    // The joiner must hold the PRE-RAFT seed row (this is exactly what log
    // replay alone can never deliver)...
    {
        let b = b.clone();
        wait_for(
            Duration::from_secs(120),
            "pre-raft seed data visible on the joiner",
            move || kv_value(&b, "seed/upgrade").as_deref() == Some("pre-raft"),
        );
    }

    // ...and a post-join write still replicates normally on top of it.
    a.put_kv("seed/post-join", "live", KvValueType::String, None, None)?;
    {
        let a = a.clone();
        let b = b.clone();
        wait_for(
            Duration::from_secs(120),
            "replicas converge to identical content hashes",
            move || {
                kv_value(&b, "seed/post-join").as_deref() == Some("live")
                    && match (a.state_content_hash(), b.state_content_hash()) {
                        (Ok(ha), Ok(hb)) => ha == hb,
                        _ => false,
                    }
            },
        );
    }
    Ok(())
}

/// Cluster-identity regression: losing the raft data_dir (secret included)
/// while keeping the state DB, with `bootstrap=true` still set, must refuse
/// to mint a new cluster over the old cluster's data; `forceNewCluster` is
/// the explicit override.
#[test]
fn rebootstrap_over_existing_state_db_requires_force() -> R<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let port = reserve_port();
    let config = raft_node_config(root, "mgr-solo", port, true, None)?;

    // First life: bootstrap a cluster and write something.
    {
        let store = Store::start_storage(&config)?;
        store.put_kv("identity/data", "v1", KvValueType::String, None, None)?;
    }

    // Normal restart (data_dir intact): same cluster, starts fine.
    {
        let store = Store::start_storage(&config)?;
        assert_eq!(kv_value(&store, "identity/data").as_deref(), Some("v1"));
    }

    // Operator error: the raft data_dir (cluster secret, logs) is wiped but
    // the state DB survives and bootstrap=true is still set. A fresh secret
    // would derive a NEW cluster id — refuse.
    std::fs::remove_dir_all(&config.persistence.raft.data_dir)
        .context("failed wiping raft data dir")?;
    let err = Store::start_storage(&config)
        .expect_err("bootstrap over a foreign-stamped state DB must be refused");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("belongs to raft cluster"),
        "refusal must explain the identity mismatch: {rendered}"
    );

    // The explicit override re-stamps the DB and keeps its data.
    let mut forced = config.clone();
    forced.persistence.raft.force_new_cluster = true;
    let store = Store::start_storage(&forced)
        .context("forceNewCluster must allow a deliberate re-bootstrap")?;
    assert_eq!(
        kv_value(&store, "identity/data").as_deref(),
        Some("v1"),
        "re-stamping must not lose the existing data"
    );
    Ok(())
}
