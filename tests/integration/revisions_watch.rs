//! Revisions, blocking reads, watches, and consistency across a raft
//! cluster (revisions-and-watches plan; docs/watches-and-blocking-queries.md).
//!
//! An in-process 3-manager `single`-topology cluster, every manager serving
//! the Manager API behind the leader-forward layer as the daemon mounts it
//! (the `tests/integration/raft_follower_writes.rs` harness). Covered:
//!
//! - a write through a follower is visible to the next read on that follower
//!   (read-after-write), and a linearizable read on a follower sees a write
//!   the leader took;
//! - revisions grow with every write and never go backwards across a leader
//!   failover, and mean the same thing on every manager (a blocking read on
//!   a survivor answers at once for a revision it already covers);
//! - a blocking read on a follower returns when the key changes through the
//!   leader;
//! - a watch resumes on another manager after its manager dies, with every
//!   change after its revision and nothing before it;
//! - a manager that restarted answers a resume from before its start with
//!   `resync_required`.

use crate::support::cluster_gate;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use legion::manager::leader_forward::LeaderForwardLayer;
use legion::manager::server::{
    ManagerDaemonService, CONSISTENCY_HEADER, MIN_REVISION_HEADER, WAIT_HEADER,
};
use legion::prelude::{Context, R};
use legion::storage::revisions::Revision;
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::manager_client::ManagerClient;
use legion::structs::manager_server::ManagerServer;
use legion::structs::{
    GetKvReply, GetKvRequest, KvValueType, PutKvRequest, WatchEvent, WatchKind, WatchRequest,
};
use tonic::transport::Channel;

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

    fn serve(&mut self) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("node runtime");
        let store = self.store().clone();
        let transport_tls = store.raft_transport_tls().expect("raft engine started");
        let addr: SocketAddr = format!("127.0.0.1:{}", self.port).parse().unwrap();
        let name = self.name;
        rt.spawn(async move {
            let socket = tokio::net::TcpSocket::new_v4().expect("socket");
            socket.set_reuseaddr(true).expect("reuseaddr");
            socket.bind(addr).expect("bind manager port");
            let listener = socket.listen(64).expect("listen");
            let incoming = transport_tls
                .incoming(listener)
                .expect("raft transport listener");
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

    fn restart(&mut self) -> R<()> {
        self.store = Some(Store::start_storage(&self.config)?);
        self.serve();
        Ok(())
    }

    fn leads(&self) -> bool {
        self.store
            .as_ref()
            .and_then(|store| store.raft_status())
            .is_some_and(|status| status.role.as_str() == "primary")
    }
}

fn reserve_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn start_node(
    root: &std::path::Path,
    name: &'static str,
    bootstrap: bool,
    join_token: Option<String>,
) -> R<TestNode> {
    let port = reserve_port();
    let dir = root.join(name);
    let mut config = ManagerConfigFull {
        store_path: dir.join("store").to_str().context("utf-8")?.to_string(),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    // The relaxed in-process timings raft_eval_failover.rs documents.
    config.persistence.raft.heartbeat_interval_ms = 400;
    config.persistence.raft.election_timeout_min_ms = 1200;
    config.persistence.raft.election_timeout_max_ms = 2000;
    config.persistence.raft.bootstrap = bootstrap;
    config.persistence.raft.node_id = name.to_string();
    config.persistence.raft.advertise_addr = Some(format!("127.0.0.1:{port}"));
    config.persistence.raft.join_token = join_token;
    config.persistence.raft.data_dir = dir.join("raft").to_str().context("utf-8")?.to_string();
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
    wait_for(Duration::from_secs(90), "a leader", || {
        found = nodes
            .iter()
            .position(|node| node.store.is_some() && node.leads());
        found.is_some()
    });
    found.expect("leader")
}

fn client(rt: &tokio::runtime::Runtime, node: &TestNode) -> R<ManagerClient<Channel>> {
    Ok(rt.block_on(ManagerClient::connect(node.addr()))?)
}

fn put(
    rt: &tokio::runtime::Runtime,
    client: &mut ManagerClient<Channel>,
    key: &str,
    value: &str,
) -> R<Revision> {
    let reply = rt
        .block_on(client.put_kv(PutKvRequest {
            key: key.into(),
            value: value.into(),
            value_type: KvValueType::String as i32,
            expected_version: None,
            lock_id: None,
        }))?
        .into_inner();
    Revision::parse(&reply.revision).map_err(|err| anyhow::anyhow!(err))
}

fn get(
    rt: &tokio::runtime::Runtime,
    client: &mut ManagerClient<Channel>,
    key: &str,
    headers: &[(&'static str, String)],
) -> R<GetKvReply> {
    let mut request = tonic::Request::new(GetKvRequest {
        key: key.into(),
        metadata_only: false,
    });
    for (name, value) in headers {
        request.metadata_mut().insert(*name, value.parse()?);
    }
    Ok(rt.block_on(client.get_kv(request))?.into_inner())
}

fn value_of(reply: &GetKvReply) -> Option<&str> {
    reply.entry.as_ref().map(|entry| entry.value.as_str())
}

#[test]
fn revisions_hold_across_followers_and_a_failover() -> R<()> {
    let _serial = cluster_gate();
    let dir = tempfile::tempdir()?;
    let mut nodes = start_cluster(dir.path())?;
    let leader = leader_index(&nodes);
    let follower = (leader + 1) % nodes.len();
    let rt = tokio::runtime::Runtime::new()?;
    let mut via_follower = client(&rt, &nodes[follower])?;
    let mut via_leader = client(&rt, &nodes[leader])?;

    // Read-after-write through a follower: the forwarded write's reply waits
    // for the follower's own replica, so the very next read there sees it.
    for round in 0..20 {
        let value = format!("v{round}");
        let written = put(&rt, &mut via_follower, "raw/key", &value)?;
        let read = get(&rt, &mut via_follower, "raw/key", &[])?;
        assert_eq!(value_of(&read), Some(value.as_str()), "round {round}");
        assert!(
            Revision::parse(&read.revision).unwrap().covers(&written),
            "a read after a write reports at least the write's revision"
        );
    }

    // A linearizable read on a follower sees a write the leader took.
    let on_leader = put(&rt, &mut via_leader, "lin/key", "fresh")?;
    let linear = get(
        &rt,
        &mut via_follower,
        "lin/key",
        &[(CONSISTENCY_HEADER, "linearizable".into())],
    )?;
    assert_eq!(value_of(&linear), Some("fresh"));
    assert!(Revision::parse(&linear.revision)
        .unwrap()
        .covers(&on_leader));

    // A blocking read on the follower wakes when the key changes through
    // the leader.
    let at = put(&rt, &mut via_leader, "block/key", "1")?;
    let blocked = {
        let mut follower_client = via_follower.clone();
        let at = at.render();
        std::thread::spawn(move || -> R<(GetKvReply, Duration)> {
            let rt = tokio::runtime::Runtime::new()?;
            let started = Instant::now();
            let reply = get(
                &rt,
                &mut follower_client,
                "block/key",
                &[(MIN_REVISION_HEADER, at), (WAIT_HEADER, "60".into())],
            )?;
            Ok((reply, started.elapsed()))
        })
    };
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        !blocked.is_finished(),
        "the read blocks while nothing changes"
    );
    let changed = put(&rt, &mut via_leader, "block/key", "2")?;
    let (reply, waited) = blocked.join().expect("blocked read")?;
    assert_eq!(value_of(&reply), Some("2"));
    assert!(waited < Duration::from_secs(30), "{waited:?}");
    assert!(Revision::parse(&reply.revision).unwrap().covers(&changed));

    // Kill the leader. The new leader's revisions continue above the old
    // one's, and a survivor that already covers a revision answers a blocking
    // read on it at once.
    let before_failover = put(&rt, &mut via_leader, "fail/key", "before")?;
    nodes[leader].kill();
    let new_leader = leader_index(&nodes);
    let survivor = (0..nodes.len())
        .find(|index| *index != leader && *index != new_leader)
        .context("a survivor")?;
    let mut via_new_leader = client(&rt, &nodes[new_leader])?;
    let after_failover = put(&rt, &mut via_new_leader, "fail/key", "after")?;
    assert!(
        after_failover.covers(&before_failover) && after_failover != before_failover,
        "{before_failover} -> {after_failover}: revisions never go backwards"
    );
    let mut via_survivor = client(&rt, &nodes[survivor])?;
    wait_for(Duration::from_secs(30), "the survivor applies it", || {
        nodes[survivor]
            .store()
            .current_revision()
            .is_ok_and(|current| current.covers(&after_failover))
    });
    let started = Instant::now();
    let caught_up = get(
        &rt,
        &mut via_survivor,
        "fail/key",
        &[
            (MIN_REVISION_HEADER, before_failover.render()),
            (WAIT_HEADER, "30".into()),
        ],
    )?;
    assert_eq!(value_of(&caught_up), Some("after"));
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "a change after the revision is already there: no wait"
    );
    for node in &mut nodes {
        node.kill();
    }
    Ok(())
}

fn open_watch(
    rt: &tokio::runtime::Runtime,
    client: &mut ManagerClient<Channel>,
    from: &str,
) -> R<tonic::Streaming<WatchEvent>> {
    Ok(rt
        .block_on(client.watch(WatchRequest {
            kind: WatchKind::Kv as i32,
            prefix: "w/".into(),
            from_revision: from.into(),
            ..Default::default()
        }))?
        .into_inner())
}

fn next(rt: &tokio::runtime::Runtime, stream: &mut tonic::Streaming<WatchEvent>) -> R<WatchEvent> {
    rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(30), stream.message())
            .await
            .context("a watch message in time")?
            .map_err(anyhow::Error::from)?
            .context("the stream is open")
    })
}

/// Read change events until `count` changes arrived.
fn changes(
    rt: &tokio::runtime::Runtime,
    stream: &mut tonic::Streaming<WatchEvent>,
    count: usize,
) -> R<(Vec<(String, String)>, String)> {
    let mut seen = Vec::new();
    let mut revision = String::new();
    while seen.len() < count {
        let event = next(rt, stream)?;
        assert!(!event.resync_required, "{event:?}");
        revision = event.revision.clone();
        for change in event.changes {
            let value = change.kv.map(|entry| entry.value).unwrap_or_default();
            seen.push((change.key, value));
        }
    }
    Ok((seen, revision))
}

#[test]
fn a_watch_resumes_on_another_manager_and_a_restarted_one_asks_for_a_relist() -> R<()> {
    let _serial = cluster_gate();
    let dir = tempfile::tempdir()?;
    let mut nodes = start_cluster(dir.path())?;
    let leader = leader_index(&nodes);
    let watched = (leader + 1) % nodes.len();
    let other = (leader + 2) % nodes.len();
    let rt = tokio::runtime::Runtime::new()?;
    let mut writer = client(&rt, &nodes[leader])?;

    // Watch on a follower; see one change.
    let mut on_watched = client(&rt, &nodes[watched])?;
    let mut stream = open_watch(&rt, &mut on_watched, "")?;
    let opening = next(&rt, &mut stream)?;
    assert!(opening.progress);
    put(&rt, &mut writer, "w/one", "1")?;
    let (seen, resume_at) = changes(&rt, &mut stream, 1)?;
    assert_eq!(seen, vec![("w/one".to_string(), "1".to_string())]);
    drop(stream);

    // One more change the watched manager applies before it dies, so its
    // restart (below) starts its journal after `resume_at`.
    let pre = put(&rt, &mut writer, "w/pre", "p")?;
    wait_for(
        Duration::from_secs(30),
        "the watched manager applies it",
        || {
            nodes[watched]
                .store()
                .current_revision()
                .is_ok_and(|current| current.covers(&pre))
        },
    );

    // Its manager dies; two more changes land while nobody watches.
    nodes[watched].kill();
    put(&rt, &mut writer, "w/two", "2")?;
    put(&rt, &mut writer, "w/one", "1b")?;

    // Resume on another manager: everything after, nothing before.
    let mut on_other = client(&rt, &nodes[other])?;
    let mut resumed = open_watch(&rt, &mut on_other, &resume_at)?;
    let first = next(&rt, &mut resumed)?;
    assert!(first.progress && !first.resync_required, "{first:?}");
    let (mut seen, _) = changes(&rt, &mut resumed, 3)?;
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ("w/one".to_string(), "1b".to_string()),
            ("w/pre".to_string(), "p".to_string()),
            ("w/two".to_string(), "2".to_string()),
        ]
    );
    drop(resumed);

    // The dead manager comes back with an empty journal: a resume from
    // before its restart is answered with resync_required.
    nodes[watched].restart()?;
    let restarted_at = nodes[watched].store().current_revision()?;
    assert!(restarted_at.covers(&Revision::parse(&resume_at).unwrap()));
    let mut on_restarted = client(&rt, &nodes[watched])?;
    let mut stale = open_watch(&rt, &mut on_restarted, &resume_at)?;
    let answer = next(&rt, &mut stale)?;
    assert!(answer.resync_required, "{answer:?}");
    assert!(!answer.oldest_revision.is_empty());
    for node in &mut nodes {
        node.kill();
    }
    Ok(())
}
