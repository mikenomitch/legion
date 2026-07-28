//! Topology is stamped and immutable per data dir (JC-43 of
//! `docs/plans/durability-and-ops-execution-plan.md` §7).
//!
//! Switching `single` ↔ `lanes` over an existing raft data dir is
//! unrecoverable, not merely awkward: the stamped topology's groups resume
//! from logs whose entries the other topology's groups never wrote, while
//! the newly-configured groups come up blank and no existing member ever
//! joins them (a restarted member suppresses its own join loop once it has
//! state). Both walls are covered here:
//!
//! (a) a manager whose configured topology disagrees with the stamp in its
//!     raft data dir REFUSES to start, naming stamped vs. configured and the
//!     one supported migration (backup → empty data dir → restore);
//! (b) a joiner whose configured topology disagrees with the cluster's is
//!     rejected at enrollment with the same message, before it is added as a
//!     learner anywhere.
//!
//! # Networking exception
//!
//! Like `tests/raft_membership.rs`, the join test runs a real tonic listener
//! serving ONLY the `RaftTransport` service on an ephemeral loopback port.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::JoinRaftClusterRequest;

/// Request-metadata key carrying the cluster secret on every `RaftTransport`
/// RPC (mirrors `RAFT_CLUSTER_SECRET_METADATA_KEY`, which is crate-internal).
const SECRET_METADATA_KEY: &str = "x-legion-raft-secret";

fn reserve_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve port")
        .local_addr()
        .expect("local addr")
        .port()
}

/// A manager config rooted at `root/name` with an explicit topology.
/// `store_suffix` lets a test point a FRESH state DB at an EXISTING raft
/// data dir (the case only the data-dir stamp can catch).
fn node_config(
    root: &Path,
    name: &str,
    topology: &str,
    store_suffix: &str,
    port: u16,
    bootstrap: bool,
) -> R<ManagerConfigFull> {
    let dir = root.join(name);
    let mut config = ManagerConfigFull {
        store_path: dir
            .join(format!("store{store_suffix}"))
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
    config.persistence.raft.topology = topology.to_string();
    config.persistence.raft.bootstrap = bootstrap;
    config.persistence.raft.node_id = name.to_string();
    config.persistence.raft.advertise_addr = Some(format!("127.0.0.1:{port}"));
    config.persistence.raft.data_dir = dir
        .join("raft")
        .to_str()
        .context("raft data dir is not utf-8")?
        .to_string();
    Ok(config)
}

/// Every substring an operator needs in a topology-mismatch refusal: both
/// sides of the disagreement, the judgment call it implements, and the exact
/// supported migration.
fn assert_actionable_mismatch(rendered: &str, stamped: &str, configured: &str) {
    for needle in [
        "topology mismatch",
        stamped,
        configured,
        "JC-43",
        "legion state backup",
        "--restore-from",
    ] {
        assert!(
            rendered.contains(needle),
            "topology-mismatch message must mention `{needle}`: {rendered}"
        );
    }
}

/// (a) A configured topology that disagrees with the data dir's stamp
/// refuses to start — both when the state DB is the original one and when
/// only the raft data dir survives (a fresh state DB pointed at an existing
/// data dir, which the state-DB stamp cannot see).
#[test]
fn topology_flip_over_a_stamped_data_dir_refuses_to_start() -> R<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let port = reserve_port();

    // First start under `single` bootstraps and stamps the data dir.
    let single = node_config(root, "mgr-a", "single", "", port, true)?;
    let store = Store::start_storage(&single)?;
    drop(store);

    let stamp_path = PathBuf::from(&single.persistence.raft.data_dir).join("topology.json");
    let stamp: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(&stamp_path)
            .with_context(|| format!("bootstrap must write {} (JC-43)", stamp_path.display()))?,
    )?;
    assert_eq!(stamp["topology"], "single");
    assert_eq!(
        stamp["lanes"],
        serde_json::json!(["control"]),
        "the stamp records the lane SET the topology activated, not just its name"
    );

    // Restart the same node as `lanes`: refused.
    let flipped = node_config(root, "mgr-a", "lanes", "", port, true)?;
    let err = Store::start_storage(&flipped)
        .err()
        .context("flipping topology over an existing raft data dir must refuse to start")?;
    let rendered = format!("{err:#}");
    assert_actionable_mismatch(&rendered, "single", "lanes");
    assert!(
        rendered.contains(&single.persistence.raft.data_dir),
        "the refusal must name the data dir the operator has to empty: {rendered}"
    );

    // And with a FRESH state DB over the same data dir — the state-DB stamp
    // is blank here, so only the data-dir stamp can catch this.
    let fresh_db = node_config(root, "mgr-a", "lanes", "-2", port, true)?;
    let err = Store::start_storage(&fresh_db)
        .err()
        .context("a fresh state DB over a stamped data dir must still refuse")?;
    assert_actionable_mismatch(&format!("{err:#}"), "single", "lanes");

    // Restarting under the stamped topology still works (the wall is a
    // mismatch wall, not a one-start wall).
    let ok = Store::start_storage(&single)?;
    drop(ok);
    Ok(())
}

/// (b) A joiner configured with a different topology is rejected at
/// enrollment, with the same actionable message and before any membership
/// change happens.
#[test]
fn joiner_with_a_mismatched_topology_is_rejected() -> R<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let port = reserve_port();

    let config = node_config(root, "mgr-a", "single", "", port, true)?;
    let store = Store::start_storage(&config)?;

    // Serve only the RaftTransport service, as the other raft tests do.
    let service = store.raft_transport_server();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = rt.spawn(async move {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket.set_reuseaddr(true).expect("reuseaddr");
        socket.bind(addr).expect("bind transport port");
        let listener = socket.listen(64).expect("listen");
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let _ = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    std::thread::sleep(Duration::from_millis(100));

    let (token, _expires) = store.issue_raft_join_token("test")?;
    // The transport authenticates every RPC with the cluster secret; a real
    // joiner gets it from its join token, this test reads the file the
    // bootstrap minted it into.
    let secret = std::fs::read_to_string(
        PathBuf::from(&config.persistence.raft.data_dir).join("cluster_secret"),
    )?
    .trim()
    .to_string();

    let voters_before = store.raft_status().context("raft status")?.voters;

    let status = rt.block_on(async {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
            .expect("endpoint")
            .connect_lazy();
        let mut client = legion::structs::raft_transport_client::RaftTransportClient::new(channel);
        let mut request = tonic::Request::new(JoinRaftClusterRequest {
            join_token: token,
            node_id: "mgr-b".to_string(),
            advertise_addr: "127.0.0.1:1".to_string(),
            // The whole point: this manager runs `lanes`, the cluster runs
            // `single`.
            topology: "lanes".to_string(),
        });
        request
            .metadata_mut()
            .insert(SECRET_METADATA_KEY, secret.parse().expect("ascii secret"));
        // A rejection must be immediate; if the handler ever fell through to
        // add_learner against an unreachable joiner it would block instead.
        request.set_timeout(Duration::from_secs(20));
        client
            .join(request)
            .await
            .expect_err("a mismatched-topology joiner must be rejected")
    });

    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "the rejection must be permanent (the joiner's retry loop stops on \
         InvalidArgument): {status:?}"
    );
    assert_actionable_mismatch(status.message(), "single", "lanes");

    // Rejected BEFORE any membership change: the joiner was never enrolled.
    let voters_after = store.raft_status().context("raft status")?.voters;
    assert_eq!(
        voters_before, voters_after,
        "a rejected joiner must not appear in membership"
    );
    assert!(
        !voters_after.contains(&"mgr-b".to_string()),
        "mgr-b must not be a voter: {voters_after:?}"
    );

    let _ = shutdown_tx.send(());
    server.abort();
    drop(store);
    Ok(())
}
