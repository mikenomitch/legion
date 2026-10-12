//! Losing or restarting one manager must not take the cluster's workers or
//! writes with it — in the recommended `single` topology (spot-check P0-5,
//! P0-6, P2-2, P2-5, P2-10).
//!
//! An in-process 3-manager `single`-topology cluster (the
//! `tests/raft_eval_failover.rs` harness), where every manager also serves
//! the Manager API behind the leader-forward layer, exactly as
//! `legion start manager` mounts it:
//!
//! - a follower forwards a status-lane write (a lost node's recovering
//!   heartbeat) to the leader — it used to be refused with
//!   `ForwardLaneWrite lane mismatch: envelope says control, op
//!   update_node_heartbeat belongs to status`, which kept workers that dial a
//!   follower lost;
//! - any manager accepts worker registration and client writes, and tells
//!   the worker every manager it can fail over to;
//! - `raft command transfer-leader` moves leadership to the named manager;
//! - a restarted follower rejoins without deposing the leader (no term
//!   change).

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use legion::manager::leader_forward::LeaderForwardLayer;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::manager_client::ManagerClient;
use legion::structs::manager_server::ManagerServer;
use legion::structs::{
    IssueRaftCommandRequest, NodeMsg, SetNodeAvailabilityRequest, WorkerRegisterRequest,
};

/// One test at a time: each brings up a 3-node cluster, and two at once on
/// a CI box starve each other's raft timers.
static CLUSTER_GATE: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn cluster_gate() -> std::sync::MutexGuard<'static, ()> {
    CLUSTER_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

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

struct TestNode {
    name: &'static str,
    port: u16,
    config: ManagerConfigFull,
    store: Option<Store>,
    rt: Option<tokio::runtime::Runtime>,
}

impl TestNode {
    fn store(&self) -> &Store {
        self.store.as_ref().expect("node is running")
    }

    fn addr(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Serve the Manager API (behind the leader-forward layer) and the raft
    /// transport on one listener, as the daemon does.
    fn serve(&mut self) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("node runtime");
        let store = self.store().clone();
        let addr: SocketAddr = format!("127.0.0.1:{}", self.port).parse().unwrap();
        let name = self.name;
        rt.spawn(async move {
            let socket = tokio::net::TcpSocket::new_v4().expect("socket");
            socket.set_reuseaddr(true).expect("reuseaddr");
            socket.bind(addr).expect("bind manager port");
            let listener = socket.listen(64).expect("listen");
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let manager = ManagerServer::new(ManagerDaemonService::new(store.clone()));
            if let Err(err) = tonic::transport::Server::builder()
                .layer(LeaderForwardLayer::new(store.clone()))
                .add_service(manager)
                .add_service(store.raft_transport_server())
                .serve_with_incoming(incoming)
                .await
            {
                eprintln!("[{name}] manager server exited: {err}");
            }
        });
        self.rt = Some(rt);
        std::thread::sleep(Duration::from_millis(50));
    }

    fn kill(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_timeout(Duration::from_secs(1));
        }
        self.store = None;
    }

    /// Restart the manager, bringing its listener up `listener_delay` after
    /// the raft engine — the window a real manager spends between starting
    /// its engine and binding its API (keyring, auth-method JWKS fetches,
    /// read-model load), during which no leader can reach it.
    fn restart(&mut self, listener_delay: Duration) -> R<()> {
        self.store = Some(Store::start_storage(&self.config)?);
        std::thread::sleep(listener_delay);
        self.serve();
        Ok(())
    }

    fn leads(&self) -> bool {
        self.store
            .as_ref()
            .and_then(|store| store.raft_status())
            .is_some_and(|status| status.role.as_str() == "primary")
    }

    fn term(&self) -> u64 {
        self.store()
            .raft_status()
            .map(|status| status.term)
            .unwrap_or(0)
    }

    fn leader(&self) -> Option<String> {
        self.store()
            .raft_status()
            .and_then(|status| status.leader_id)
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
    // Relaxed timings for an in-process multi-node cluster in a debug build
    // (see tests/raft_eval_failover.rs). `single` is the default topology.
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
        config: config.clone(),
        store: Some(Store::start_storage(&config)?),
        rt: None,
    };
    node.serve();
    Ok(node)
}

fn start_cluster(root: &std::path::Path) -> R<Vec<TestNode>> {
    let a = start_node(root, "mgr-a", true, None)?;
    let (token, _expires) = a.store().issue_raft_join_token("test")?;
    let b = start_node(root, "mgr-b", false, Some(token.clone()))?;
    let c = start_node(root, "mgr-c", false, Some(token))?;
    wait_for(
        Duration::from_secs(150),
        "both joiners promoted to voter on every manager",
        || {
            [&a, &b, &c].iter().all(|node| {
                node.store()
                    .raft_status()
                    .map(|s| s.voters.len() == 3 && s.leader_id.is_some())
                    .unwrap_or(false)
            })
        },
    );
    Ok(vec![a, b, c])
}

fn leader_index(nodes: &[TestNode]) -> usize {
    let mut found = None;
    wait_for(Duration::from_secs(60), "a leader", || {
        found = nodes.iter().position(TestNode::leads);
        found.is_some()
    });
    found.expect("leader")
}

fn worker_node(id: &str) -> NodeMsg {
    NodeMsg {
        id: id.into(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: "{}".into(),
        fingerprint_attrs: "{}".into(),
        dynamic_attrs: "{}".into(),
        health_checks: "{}".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
    }
}

/// P0-5 / P0-6: a lost node whose worker dials a FOLLOWER recovers — both
/// halves, status and schedulability — through the follower's forwarding.
#[test]
fn a_follower_forwards_a_lost_nodes_recovery_in_single_topology() -> R<()> {
    let _serial = cluster_gate();
    let dir = tempfile::tempdir()?;
    let nodes = start_cluster(dir.path())?;
    let leader = leader_index(&nodes);
    let follower = (leader + 1) % nodes.len();

    nodes[leader].store().insert_node(worker_node("wkr-1"))?;
    nodes[leader]
        .store()
        .mark_nodes_as_lost(&["wkr-1".to_string()])?;
    wait_for(
        Duration::from_secs(30),
        "the loss replicates to the follower",
        || {
            nodes[follower]
                .store()
                .get_node("wkr-1")
                .is_ok_and(|node| node.status == "lost" && node.availability == "unavailable")
        },
    );

    // The worker's heartbeat lands on the follower. Before the fix every
    // forward of this status-lane write was refused as a lane mismatch.
    nodes[follower]
        .store()
        .update_node_heartbeat("wkr-1".to_string())
        .context("a follower must forward a recovering heartbeat in single topology")?;
    wait_for(
        Duration::from_secs(30),
        "every manager sees the node active and schedulable",
        || {
            nodes.iter().all(|node| {
                node.store()
                    .get_node("wkr-1")
                    .is_ok_and(|node| node.status == "active" && node.availability == "available")
            })
        },
    );
    Ok(())
}

/// P2-5 and P0-5: every manager accepts worker registration and client
/// writes (a follower forwards them to the leader), and the reply tells the
/// worker every manager it can fail over to.
#[test]
fn any_manager_accepts_registration_and_client_writes() -> R<()> {
    let _serial = cluster_gate();
    let dir = tempfile::tempdir()?;
    let nodes = start_cluster(dir.path())?;
    let leader = leader_index(&nodes);
    let follower = (leader + 1) % nodes.len();
    let rt = tokio::runtime::Runtime::new()?;
    let follower_addr = nodes[follower].addr();

    let reply = rt.block_on(async {
        let mut client = ManagerClient::connect(follower_addr.clone()).await?;
        let registered = client
            .worker_register(WorkerRegisterRequest {
                name: "wkr-via-follower".into(),
                pool: "default".into(),
                config_attrs: "{}".into(),
                fingerprint_attrs: "{}".into(),
                health_checks: "[]".into(),
                ..Default::default()
            })
            .await?
            .into_inner();
        // A client write (cordon) through the same follower.
        client
            .set_node_availability(SetNodeAvailabilityRequest {
                node_id: "wkr-via-follower".into(),
                availability: "drained".into(),
                evict: false,
                idempotency_key: None,
            })
            .await?;
        Ok::<_, anyhow::Error>(registered)
    })?;

    let mut expected: Vec<String> = nodes.iter().map(TestNode::addr).collect();
    expected.sort();
    let mut reported = reply.manager_addresses.clone();
    reported.sort();
    assert_eq!(reported, expected, "the worker learns every manager");
    wait_for(
        Duration::from_secs(30),
        "the forwarded registration and cordon are committed everywhere",
        || {
            nodes.iter().all(|node| {
                node.store()
                    .get_node("wkr-via-follower")
                    .is_ok_and(|node| node.availability == "drained")
            })
        },
    );
    Ok(())
}

/// P2-2: `transfer-leader` moves leadership to the named manager, from any
/// manager, and refuses a target that is not a voter with a typed error.
#[test]
fn transfer_leader_moves_leadership_to_the_target() -> R<()> {
    let _serial = cluster_gate();
    let dir = tempfile::tempdir()?;
    let nodes = start_cluster(dir.path())?;
    let leader = leader_index(&nodes);
    // Ask a follower to hand leadership to the OTHER follower.
    let asked = (leader + 1) % nodes.len();
    let target = (leader + 2) % nodes.len();
    let target_name = nodes[target].name;
    let rt = tokio::runtime::Runtime::new()?;
    let reply = rt.block_on(async {
        let mut client = ManagerClient::connect(nodes[asked].addr()).await?;
        let reply = client
            .issue_raft_command(IssueRaftCommandRequest {
                command: "transfer-leader".into(),
                target_node_id: Some(target_name.into()),
                actor: Some("test".into()),
            })
            .await?
            .into_inner();
        Ok::<_, anyhow::Error>(reply)
    })?;
    assert!(reply.warning.is_none(), "{:?}", reply.warning);
    wait_for(
        Duration::from_secs(60),
        "the target leads, as every manager sees it",
        || {
            nodes[target].leads()
                && nodes
                    .iter()
                    .all(|node| node.leader().as_deref() == Some(target_name))
        },
    );

    let status = rt
        .block_on(async {
            let mut client = ManagerClient::connect(nodes[asked].addr()).await?;
            Ok::<_, anyhow::Error>(
                client
                    .issue_raft_command(IssueRaftCommandRequest {
                        command: "transfer-leader".into(),
                        target_node_id: Some("mgr-nope".into()),
                        actor: None,
                    })
                    .await,
            )
        })?
        .expect_err("an unknown target is refused");
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
    assert!(
        status.message().contains("mgr-nope"),
        "{}",
        status.message()
    );
    Ok(())
}

/// P2-10: a restarted follower rejoins as a follower. A member whose
/// listener came up later than one leader lease plus its election timeout
/// after its engine started (so the leader could not reach it in time)
/// campaigned, kept campaigning while unreachable, and its higher term then
/// deposed the healthy leader the moment the leader's heartbeat arrived:
/// such a restart cost an election and several terms.
#[test]
fn a_restarted_follower_does_not_depose_the_leader() -> R<()> {
    let _serial = cluster_gate();
    let dir = tempfile::tempdir()?;
    let mut nodes = start_cluster(dir.path())?;
    let leader = leader_index(&nodes);
    let follower = (leader + 1) % nodes.len();
    let leader_name = nodes[leader].name;
    // Let the term settle before measuring it.
    std::thread::sleep(Duration::from_secs(3));
    let term = nodes[leader].term();

    nodes[follower].kill();
    std::thread::sleep(Duration::from_secs(3));
    // Longer than lease + election timeout (2000 + up to 2000 ms here).
    nodes[follower].restart(Duration::from_secs(6))?;
    let dump = |nodes: &[TestNode]| {
        nodes
            .iter()
            .map(|node| {
                node.store
                    .as_ref()
                    .and_then(|store| store.raft_status())
                    .map(|s| {
                        format!(
                            "{}: role={} leader={:?} term={}",
                            node.name,
                            s.role.as_str(),
                            s.leader_id,
                            s.term
                        )
                    })
                    .unwrap_or_else(|| format!("{}: down", node.name))
            })
            .collect::<Vec<_>>()
            .join("; ")
    };
    eprintln!("after restart: {}", dump(&nodes));
    for _ in 0..12 {
        std::thread::sleep(Duration::from_secs(1));
        eprintln!("t+: {}", dump(&nodes));
    }
    assert!(nodes[leader].leads(), "{leader_name} must still lead: {}", dump(&nodes));
    assert_eq!(
        nodes[leader].term(),
        term,
        "a follower restart must not start an election"
    );
    assert_eq!(nodes[follower].term(), term);
    Ok(())
}
