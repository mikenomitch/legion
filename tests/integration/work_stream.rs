//! `Manager.StreamWork`: sequenced delivery of work to workers
//! (sandbox-scaling plan §6.13, Phase 2; docs/architecture-managers-workers.md,
//! "Delivery").
//!
//! The manager runs as the real gRPC service on an ephemeral loopback port,
//! and the worker side is a raw stream client (plus the worker's own
//! `WorkMirror`), so every frame is visible to the test. The eval worker is
//! the daemon loop's write sequence (`claim_evals`, `resolve_claimed_eval`,
//! `complete_evals_and_claim_next`), driven from the test thread.
//!
//! Covered:
//! - a placement reaches a streaming worker within a round trip of its
//!   commit, with no heartbeat at all;
//! - a dropped stream replays exactly what was not acknowledged;
//! - another epoch, a sequence ahead of the manager's, or a pruned gap
//!   resyncs with the full desired set;
//! - a steady heartbeat from a streaming worker is under 1 KiB both ways and
//!   carries no desired set, a digest disagreement queues one resync, and a
//!   worker that does not stream keeps the full desired set;
//! - a leader kill: streams attached to a surviving manager carry on
//!   without a resync, a worker whose manager died resyncs once on another,
//!   and no allocation is lost or duplicated;
//! - creating or mounting a job-level volume claim re-sends the job's
//!   allocations, inside a raft apply, without failing it;
//! - checkpoint work (a suspend's capture, the reclaim after a resume) is
//!   pushed within a round trip with no heartbeat, never replayed, re-pushed
//!   to a new stream while outstanding, and still listed by the heartbeat
//!   until its report lands.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use legion::manager::daemon::resolve_claimed_eval;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::store::{ClaimScope, EvalBatchLimits, Store, VolumeFilter};
use legion::storage::work_log::{PendingWork, WorkLog};
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::{JobConfig, StatefulConfig, TaskConfig, VolumeSpec};
use legion::structs::manager_client::ManagerClient;
use legion::structs::manager_server::{Manager, ManagerServer};
use legion::structs::suspend::SuspendConfig;
use legion::structs::{
    stream_work_request, AllocationHealth, CreateInstanceRequest, GetInstanceRequest,
    HeartbeatReply, HeartbeatRequest, InstanceMode, Node, NodeMsg, SnapshotReport,
    StartInstanceRequest, StreamWorkAck, StreamWorkOpen, StreamWorkReply, StreamWorkRequest,
    Submission, SuspendInstanceRequest, WhoAmIRequest,
};
use legion::worker::work_stream::WorkMirror;
use prost::Message;
use tonic::transport::Channel;
use tonic::Request;

const JOB: &str = "agents";
const TASK: &str = "workspace";

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

fn wait_for(timeout: Duration, what: &str, mut probe: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if probe() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out after {timeout:?} waiting for: {what}");
}

/// Serve the Manager service for `store` on an ephemeral loopback port, on
/// its own runtime (dropping the runtime stops the server).
fn serve(store: &Store) -> R<(tokio::runtime::Runtime, SocketAddr)> {
    let rt = runtime();
    let listener = rt.block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))?;
    let addr = listener.local_addr()?;
    let service = ManagerServer::new(ManagerDaemonService::new(store.clone()));
    rt.spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let _ = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await;
    });
    Ok((rt, addr))
}

fn node(id: &str, max_allocations: usize) -> Node {
    let memory_mib = (max_allocations as u64 + 4) * 64;
    let cores = (max_allocations as u64 * 10).div_ceil(2000) + 1;
    let cpus: serde_json::Map<String, serde_json::Value> = (1..=cores)
        .map(|core| (format!("cpu-{core}"), json!({ "frequency": 2000.0 })))
        .collect();
    Node {
        id: id.to_string(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({ "node_id": id, "maxAllocations": max_allocations, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": (memory_mib * 1024 * 1024).to_string(),
            "cpus": cpus,
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

fn submit_keyed(store: &Store) -> R<()> {
    let task = TaskConfig {
        resources: json!({ "memory": 64, "cpu": 10 }),
        stateful: Some(StatefulConfig {
            mode: InstanceMode::Keyed,
            require_id: false,
            initial_instances: None,
            on_lost: None,
            storage: None,
            envelope: None,
            promote: None,
            placement: None,
            fence_after: None,
            unknown_fields: Default::default(),
        }),
        ..TaskConfig::new(TASK, "noop")
    };
    store.handle_job_submit(
        &JobConfig {
            priority: Default::default(),
            name: JOB.to_string(),
            tasks: vec![task],
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
            pending_extensions: Default::default(),
            extensions: Default::default(),
        },
        &[Submission {
            path: format!("{JOB}.json"),
            content: "{}".to_string(),
            is_primary: true,
        }],
    )?;
    Ok(())
}

fn run_eval_worker(store: &Store) -> R<()> {
    let limits = EvalBatchLimits {
        max: 16,
        workers: 1,
    };
    let mut batch = store.claim_evals(limits, ClaimScope::Any)?;
    while !batch.is_empty() {
        let completions = batch
            .iter()
            .map(|eval| (eval.id.clone(), resolve_claimed_eval(store, 0, eval)))
            .collect();
        batch = store.complete_evals_and_claim_next(completions, limits, ClaimScope::Any)?;
    }
    Ok(())
}

/// `CreateInstance` as the RPC does it, then the eval worker: the instance
/// is placed (or not) when this returns.
fn create_and_place(store: &Store, count: usize) -> R<()> {
    for _ in 0..count {
        store.create_keyed_instance(JOB, "default", TASK, None)?;
    }
    let config_id = store.get_job_config(JOB)?.id();
    store.enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
    run_eval_worker(store)
}

fn allocation_ids(store: &Store, node_id: &str) -> R<BTreeSet<i64>> {
    Ok(store
        .allocs_for_node(node_id)?
        .iter()
        .map(|allocation| allocation.id)
        .collect())
}

/// A raw `StreamWork` client: every received message is forwarded with its
/// arrival time; acks are sent automatically or by hand.
struct Probe {
    received: std::sync::mpsc::Receiver<(Instant, StreamWorkReply)>,
    requests: tokio::sync::mpsc::Sender<StreamWorkRequest>,
    task: tokio::task::JoinHandle<()>,
}

impl Probe {
    fn open(
        rt: &tokio::runtime::Runtime,
        addr: SocketAddr,
        node_id: &str,
        last: (u64, u64),
        auto_ack: bool,
    ) -> R<Probe> {
        let client = rt.block_on(ManagerClient::connect(format!("http://{addr}")))?;
        Self::open_with(rt, client, node_id, last, auto_ack)
    }

    fn open_with(
        rt: &tokio::runtime::Runtime,
        mut client: ManagerClient<Channel>,
        node_id: &str,
        last: (u64, u64),
        auto_ack: bool,
    ) -> R<Probe> {
        let (requests, rx) = tokio::sync::mpsc::channel::<StreamWorkRequest>(64);
        rt.block_on(requests.send(StreamWorkRequest {
            frame: Some(stream_work_request::Frame::Open(StreamWorkOpen {
                node_id: node_id.to_string(),
                last_epoch: last.0,
                last_seq: last.1,
            })),
        }))?;
        let mut inbound = rt
            .block_on(client.stream_work(tokio_stream::wrappers::ReceiverStream::new(rx)))?
            .into_inner();
        let (forward, received) = std::sync::mpsc::channel();
        let acks = requests.clone();
        let task = rt.spawn(async move {
            while let Ok(Some(message)) = inbound.message().await {
                let arrived = Instant::now();
                if auto_ack {
                    let _ = acks
                        .send(StreamWorkRequest {
                            frame: Some(stream_work_request::Frame::Ack(StreamWorkAck {
                                epoch: message.epoch,
                                seq: message.seq,
                            })),
                        })
                        .await;
                }
                if forward.send((arrived, message)).is_err() {
                    break;
                }
            }
        });
        Ok(Probe {
            received,
            requests,
            task,
        })
    }

    fn next(&self, timeout: Duration) -> Option<(Instant, StreamWorkReply)> {
        self.received.recv_timeout(timeout).ok()
    }

    fn expect(&self, what: &str) -> StreamWorkReply {
        self.next(Duration::from_secs(10))
            .unwrap_or_else(|| panic!("no message: {what}"))
            .1
    }

    fn ack(&self, rt: &tokio::runtime::Runtime, epoch: u64, seq: u64) {
        rt.block_on(self.requests.send(StreamWorkRequest {
            frame: Some(stream_work_request::Frame::Ack(StreamWorkAck {
                epoch,
                seq,
            })),
        }))
        .expect("ack");
    }

    fn close(self) {
        self.task.abort();
        drop(self.requests);
    }
}

fn upsert_ids(message: &StreamWorkReply) -> BTreeSet<i64> {
    message
        .upserts
        .iter()
        .map(|allocation| allocation.allocation_id)
        .collect()
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[index]
}

struct Local {
    _dir: tempfile::TempDir,
    store: Store,
    rt: tokio::runtime::Runtime,
    addr: SocketAddr,
}

fn local(nodes: &[(&str, usize)]) -> R<Local> {
    let dir = tempfile::tempdir()?;
    let store = Store::start_storage(&ManagerConfigFull {
        store_path: dir.path().to_str().context("store path")?.to_string(),
        scheduling_policy: "bin_pack".to_string(),
        scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
        ..Default::default()
    })?;
    for (id, max) in nodes {
        store.insert_node(NodeMsg::from(node(id, *max)))?;
    }
    submit_keyed(&store)?;
    run_eval_worker(&store)?;
    let (rt, addr) = serve(&store)?;
    Ok(Local {
        _dir: dir,
        store,
        rt,
        addr,
    })
}

/// Phase 2's headline: with no heartbeat running at all, each placement
/// reaches its worker's stream within one round trip of the commit. The
/// bound asserted here is loose (CI runs debug builds on shared cores); the
/// plan records the release-build numbers against its "one round trip plus
/// 5 ms at p50" target. Either way it is three orders of magnitude under
/// the heartbeat interval the push-and-poll design leaned on.
#[test]
fn a_placement_reaches_a_streaming_worker_within_a_round_trip() -> R<()> {
    let cluster = local(&[("w-1", 200)])?;
    let client = cluster
        .rt
        .block_on(ManagerClient::connect(format!("http://{}", cluster.addr)))?;
    let probe = Probe::open_with(&cluster.rt, client.clone(), "w-1", (0, 0), true)?;
    let first = probe.expect("the opening resync");
    assert!(first.resync, "a fresh worker starts with a full resync");
    assert!(first.upserts.is_empty());

    // The round trip on the same channel, for comparison.
    let mut rtts = Vec::new();
    for _ in 0..50 {
        let mut client = client.clone();
        let started = Instant::now();
        cluster.rt.block_on(client.who_am_i(WhoAmIRequest {}))?;
        rtts.push(started.elapsed());
    }
    rtts.sort();

    let placements = 40;
    let mut latencies = Vec::new();
    for _ in 0..placements {
        create_and_place(&cluster.store, 1)?;
        let committed = Instant::now();
        let (arrived, message) = probe
            .next(Duration::from_secs(5))
            .context("a placement on the stream")?;
        assert!(!message.resync);
        assert_eq!(message.upserts.len(), 1, "one placement, one upsert");
        latencies.push(arrived.saturating_duration_since(committed));
    }
    latencies.sort();
    let rtt_p50 = percentile(&rtts, 0.5);
    let p50 = percentile(&latencies, 0.5);
    let p99 = percentile(&latencies, 0.99);
    println!(
        "work_stream_measure {}",
        json!({
            "measure": "placement_to_stream",
            "placements": placements,
            "rtt_p50_us": rtt_p50.as_micros() as u64,
            "delivery_p50_us": p50.as_micros() as u64,
            "delivery_p99_us": p99.as_micros() as u64,
        })
    );
    assert!(
        p50 <= rtt_p50 + Duration::from_millis(50),
        "p50 delivery {p50:?} is not within a round trip ({rtt_p50:?}) of the commit"
    );
    probe.close();
    Ok(())
}

/// A stream that drops resumes where its worker left off: the reconnect
/// names the last message it applied, and gets exactly the messages after
/// it, in order, with no resync.
#[test]
fn a_dropped_stream_replays_what_it_did_not_ack() -> R<()> {
    let cluster = local(&[("w-1", 50)])?;
    let probe = Probe::open(&cluster.rt, cluster.addr, "w-1", (0, 0), false)?;
    let resync = probe.expect("the opening resync");
    probe.ack(&cluster.rt, resync.epoch, resync.seq);
    create_and_place(&cluster.store, 1)?;
    let unacked = probe.expect("the first placement");
    assert_eq!(unacked.seq, resync.seq + 1);
    probe.close();
    let work_log = cluster.store.work_log();
    wait_for(Duration::from_secs(5), "the stream detaches", || {
        !work_log.streaming("w-1")
    });
    // Placed while the worker was away: queued in the node's log.
    create_and_place(&cluster.store, 1)?;

    let probe = Probe::open(
        &cluster.rt,
        cluster.addr,
        "w-1",
        (resync.epoch, resync.seq),
        false,
    )?;
    let replayed = probe.expect("the unacked placement, replayed");
    assert!(!replayed.resync, "a resumable position is not resynced");
    assert_eq!(replayed.seq, unacked.seq);
    assert_eq!(upsert_ids(&replayed), upsert_ids(&unacked));
    let queued = probe.expect("the placement made while detached");
    assert_eq!(queued.seq, unacked.seq + 1);
    assert!(!queued.resync);
    probe.ack(&cluster.rt, queued.epoch, queued.seq);
    assert!(
        probe.next(Duration::from_millis(300)).is_none(),
        "nothing else is pending"
    );
    probe.close();
    Ok(())
}

/// A position the manager cannot continue from — another epoch (a failover
/// or a restart), a sequence ahead of its own, or one older than what it
/// still holds — gets the node's whole desired set once, then deltas.
#[test]
fn another_epoch_a_regression_or_a_pruned_gap_resyncs() -> R<()> {
    let cluster = local(&[("w-1", 20)])?;
    let probe = Probe::open(&cluster.rt, cluster.addr, "w-1", (0, 0), true)?;
    let opening = probe.expect("the opening resync");
    create_and_place(&cluster.store, 2)?;
    let mut last = opening.clone();
    while upsert_ids(&last).len() < 2 && last.seq < opening.seq + 2 {
        last = probe.expect("the placements");
    }
    // Let the acks land before the stream is replaced.
    std::thread::sleep(Duration::from_millis(100));
    probe.close();
    let expected = allocation_ids(&cluster.store, "w-1")?;
    assert_eq!(expected.len(), 2);

    for (label, position) in [
        ("another epoch", (opening.epoch ^ 0xff, last.seq)),
        ("ahead of the manager", (opening.epoch, last.seq + 100)),
    ] {
        let probe = Probe::open(&cluster.rt, cluster.addr, "w-1", position, true)?;
        let message = probe.expect(label);
        assert!(message.resync, "{label}: resynced");
        assert_eq!(upsert_ids(&message), expected, "{label}: the full set");
        probe.close();
    }

    // A pruned gap: messages the worker acknowledged are dropped from the
    // replay buffer, so a worker that comes back from before them (a
    // restored older state) cannot be replayed to.
    let probe = Probe::open(&cluster.rt, cluster.addr, "w-1", (0, 0), true)?;
    let fresh = probe.expect("a fresh resync");
    for _ in 0..5 {
        create_and_place(&cluster.store, 1)?;
    }
    let mut seen = fresh.seq;
    while seen < fresh.seq + 5 {
        seen = probe.expect("a placement").seq;
    }
    std::thread::sleep(Duration::from_millis(100));
    probe.close();
    let probe = Probe::open(
        &cluster.rt,
        cluster.addr,
        "w-1",
        (fresh.epoch, fresh.seq),
        true,
    )?;
    let message = probe.expect("the gap");
    assert!(
        message.resync,
        "a position older than the replay window resyncs"
    );
    assert_eq!(upsert_ids(&message).len(), 7);
    probe.close();
    Ok(())
}

/// The heartbeat of a streaming worker reports its position and a digest
/// instead of its allocations, and the reply leaves the desired set out:
/// both stay under 1 KiB however much the node runs. A digest that
/// disagrees at the same position queues one resync on the stream. A
/// worker that does not stream gets today's full desired set.
#[test]
fn a_streaming_heartbeat_is_small_and_a_drifted_digest_resyncs() -> R<()> {
    // `LEGION_MEASURE_STREAM_ALLOCS=10000 cargo test --release --test
    // work_stream -- a_streaming_heartbeat --nocapture` is the plan's
    // 10,000-allocation measurement; CI runs a few hundred.
    let allocations: usize = std::env::var("LEGION_MEASURE_STREAM_ALLOCS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(300);
    let cluster = local(&[("w-1", allocations + 8)])?;
    let svc = ManagerDaemonService::new(cluster.store.clone());
    let probe = Probe::open(&cluster.rt, cluster.addr, "w-1", (0, 0), true)?;
    let mut mirror = WorkMirror::default();
    mirror.apply(&probe.expect("the opening resync"));
    create_and_place(&cluster.store, allocations)?;
    wait_for(Duration::from_secs(10), "every placement mirrored", || {
        while let Some((_, message)) = probe.next(Duration::from_millis(1)) {
            mirror.apply(&message);
        }
        mirror.desired().len() == allocations
    });
    let ids = allocation_ids(&cluster.store, "w-1")?;
    assert_eq!(ids.len(), allocations);
    std::thread::sleep(Duration::from_millis(100));

    // The first beat reports every allocation's health (it all changed).
    let health: Vec<AllocationHealth> = ids
        .iter()
        .map(|id| AllocationHealth {
            allocation_id: *id,
            healthy: true,
            status: "running".into(),
            artifact_states: None,
        })
        .collect();
    let first = HeartbeatRequest {
        id: "w-1".into(),
        work_stream: mirror.state(),
        allocation_health: health.clone(),
        ..Default::default()
    };
    let reply = cluster
        .rt
        .block_on(svc.heartbeat(Request::new(first)))?
        .into_inner();
    assert!(reply.desired_via_stream);
    assert!(!reply.work_resync);

    // A steady beat: nothing changed, so nothing but the position.
    let steady = HeartbeatRequest {
        id: "w-1".into(),
        work_stream: mirror.state(),
        ..Default::default()
    };
    let beats = 5;
    let started = Instant::now();
    let mut reply = Default::default();
    for _ in 0..beats {
        reply = cluster
            .rt
            .block_on(svc.heartbeat(Request::new(steady.clone())))?
            .into_inner();
    }
    let stream_ms = started.elapsed().as_secs_f64() * 1000.0 / beats as f64;
    assert!(reply.desired_via_stream);
    assert!(reply.desired_allocations.is_empty());
    assert!(reply.stop_notices.is_empty());
    println!(
        "work_stream_measure {}",
        json!({
            "measure": "steady_streaming_heartbeat",
            "allocations_per_node": allocations,
            "request_bytes": steady.encoded_len(),
            "reply_bytes": reply.encoded_len(),
            "manager_ms_per_beat": stream_ms,
        })
    );
    assert!(
        steady.encoded_len() < 1024,
        "request {}",
        steady.encoded_len()
    );
    assert!(reply.encoded_len() < 1024, "reply {}", reply.encoded_len());

    // Drift: the worker claims a set the manager did not send.
    let mut drifted = steady.clone();
    if let Some(state) = drifted.work_stream.as_mut() {
        state.digest = "1:dead".into();
    }
    let reply = cluster
        .rt
        .block_on(svc.heartbeat(Request::new(drifted)))?
        .into_inner();
    assert!(reply.work_resync, "a disagreeing digest resyncs");
    let resync = probe.expect("the resync the heartbeat queued");
    assert!(resync.resync);
    assert_eq!(upsert_ids(&resync), ids);
    mirror.apply(&resync);

    // A worker that does not stream: the full desired set, as before.
    let legacy = HeartbeatRequest {
        id: "w-1".into(),
        allocation_ids: ids.iter().copied().collect(),
        allocation_health: health,
        ..Default::default()
    };
    let started = Instant::now();
    let reply = cluster
        .rt
        .block_on(svc.heartbeat(Request::new(legacy.clone())))?
        .into_inner();
    let legacy_ms = started.elapsed().as_secs_f64() * 1000.0;
    assert!(!reply.desired_via_stream);
    assert_eq!(reply.desired_allocations.len(), allocations);
    println!(
        "work_stream_measure {}",
        json!({
            "measure": "steady_full_set_heartbeat",
            "allocations_per_node": allocations,
            "request_bytes": legacy.encoded_len(),
            "reply_bytes": reply.encoded_len(),
            "manager_ms_per_beat": legacy_ms,
        })
    );
    probe.close();
    Ok(())
}

// ── leader kill on a 3-manager raft cluster ──────────────────────────────

/// One manager: its store, its raft transport, and its Manager service, each
/// on a runtime the kill shuts down.
struct RaftManager {
    name: &'static str,
    store: Option<Store>,
    transport: Option<tokio::runtime::Runtime>,
    service: Option<(tokio::runtime::Runtime, SocketAddr)>,
}

impl RaftManager {
    fn store(&self) -> &Store {
        self.store.as_ref().expect("running")
    }

    fn addr(&self) -> SocketAddr {
        self.service.as_ref().expect("serving").1
    }

    fn is_leader(&self) -> bool {
        self.store
            .as_ref()
            .and_then(|store| store.raft_status())
            .map(|status| status.role.as_str() == "primary")
            .unwrap_or(false)
    }

    fn kill(&mut self) {
        if let Some((rt, _)) = self.service.take() {
            rt.shutdown_timeout(Duration::from_secs(1));
        }
        if let Some(rt) = self.transport.take() {
            rt.shutdown_timeout(Duration::from_secs(1));
        }
        self.store = None;
    }
}

fn reserve_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn raft_manager(
    root: &std::path::Path,
    name: &'static str,
    bootstrap: bool,
    join_token: Option<String>,
) -> R<RaftManager> {
    let port = reserve_port();
    let dir = root.join(name);
    let mut config = ManagerConfigFull {
        store_path: dir.join("store").to_str().context("utf-8")?.to_string(),
        scheduling_policy: "bin_pack".to_string(),
        scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    // The relaxed in-process timings tests/integration/raft_eval_failover.rs documents.
    config.persistence.raft.heartbeat_interval_ms = 400;
    config.persistence.raft.election_timeout_min_ms = 1200;
    config.persistence.raft.election_timeout_max_ms = 2000;
    config.persistence.raft.bootstrap = bootstrap;
    config.persistence.raft.node_id = name.to_string();
    config.persistence.raft.advertise_addr = Some(format!("127.0.0.1:{port}"));
    config.persistence.raft.join_token = join_token;
    config.persistence.raft.data_dir = dir.join("raft").to_str().context("utf-8")?.to_string();
    let store = Store::start_storage(&config)?;

    let transport = runtime();
    let raft_service = store.raft_transport_server();
    let transport_tls = store.raft_transport_tls().expect("raft engine started");
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
    transport.spawn(async move {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket.set_reuseaddr(true).expect("reuseaddr");
        socket.bind(addr).expect("bind transport port");
        let listener = socket.listen(64).expect("listen");
        let incoming = transport_tls
            .incoming(listener)
            .expect("raft transport listener");
        let _ = tonic::transport::Server::builder()
            .add_service(raft_service)
            .serve_with_incoming(incoming)
            .await;
    });
    let service = serve(&store)?;
    Ok(RaftManager {
        name,
        store: Some(store),
        transport: Some(transport),
        service: Some(service),
    })
}

/// Apply everything the probes received to their mirrors, counting resyncs.
fn drain(
    probes: &BTreeMap<&'static str, Probe>,
    mirrors: &mut BTreeMap<&'static str, WorkMirror>,
    resyncs: &mut BTreeMap<&'static str, usize>,
) {
    for (node_id, probe) in probes {
        while let Some((_, message)) = probe.next(Duration::from_millis(1)) {
            if message.resync {
                *resyncs.get_mut(node_id).expect("node") += 1;
            }
            mirrors.get_mut(node_id).expect("node").apply(&message);
        }
    }
}

/// Every allocation each worker's mirror holds, by node.
fn mirrored(mirrors: &BTreeMap<&str, WorkMirror>) -> BTreeMap<String, BTreeSet<i64>> {
    mirrors
        .iter()
        .map(|(node, mirror)| {
            (
                node.to_string(),
                mirror
                    .desired()
                    .iter()
                    .map(|allocation| allocation.allocation_id)
                    .collect(),
            )
        })
        .collect()
}

/// The delivery log is soft state on whichever manager a worker streams
/// from, fed by the changeset projection every replica runs. So killing the
/// leader disturbs only the streams attached to it: a worker streaming from
/// a surviving manager keeps its position (no resync, no gap), and a worker
/// whose manager died reconnects elsewhere and resyncs exactly once. No
/// allocation is lost or delivered twice.
#[test]
fn a_leader_kill_resyncs_only_the_workers_that_lost_their_manager() -> R<()> {
    let dir = tempfile::tempdir()?;
    let a = raft_manager(dir.path(), "mgr-a", true, None)?;
    let (token, _) = a.store().issue_raft_join_token("test")?;
    let b = raft_manager(dir.path(), "mgr-b", false, Some(token.clone()))?;
    let c = raft_manager(dir.path(), "mgr-c", false, Some(token))?;
    let mut managers = vec![a, b, c];
    wait_for(Duration::from_secs(150), "both joiners are voters", || {
        managers[0]
            .store()
            .raft_status()
            .map(|status| {
                status.voters.contains(&"mgr-b".to_string())
                    && status.voters.contains(&"mgr-c".to_string())
            })
            .unwrap_or(false)
    });
    wait_for(Duration::from_secs(60), "a leader", || {
        managers.iter().any(RaftManager::is_leader)
    });
    let leader = managers
        .iter()
        .position(RaftManager::is_leader)
        .context("leader")?;
    let survivor = (leader + 1) % 3;
    eprintln!(
        "leader {}, streaming survivor {}",
        managers[leader].name, managers[survivor].name
    );

    let rt = runtime();
    {
        let store = managers[leader].store();
        store.insert_node(NodeMsg::from(node("w-1", 3)))?;
        store.insert_node(NodeMsg::from(node("w-2", 3)))?;
        submit_keyed(store)?;
        run_eval_worker(store)?;
    }
    wait_for(Duration::from_secs(30), "the survivor has the job", || {
        managers[survivor].store().get_job_config(JOB).is_ok()
    });

    // w-1 streams from a manager that survives; w-2 from the leader.
    let mut probes: BTreeMap<&'static str, Probe> = BTreeMap::new();
    let mut mirrors: BTreeMap<&'static str, WorkMirror> = BTreeMap::new();
    let mut resyncs: BTreeMap<&'static str, usize> = BTreeMap::new();
    for (node_id, manager) in [("w-1", survivor), ("w-2", leader)] {
        let probe = Probe::open(&rt, managers[manager].addr(), node_id, (0, 0), true)?;
        let mut mirror = WorkMirror::default();
        let opening = probe.expect("the opening resync");
        assert!(opening.resync);
        mirror.apply(&opening);
        probes.insert(node_id, probe);
        mirrors.insert(node_id, mirror);
        resyncs.insert(node_id, 0);
    }
    let placed = |store: &Store| -> BTreeMap<String, BTreeSet<i64>> {
        ["w-1", "w-2"]
            .iter()
            .map(|node_id| {
                (
                    node_id.to_string(),
                    allocation_ids(store, node_id).unwrap_or_default(),
                )
            })
            .collect()
    };

    create_and_place(managers[leader].store(), 4)?;
    let before = placed(managers[leader].store());
    assert_eq!(before.values().map(BTreeSet::len).sum::<usize>(), 4);
    wait_for(Duration::from_secs(30), "both streams deliver", || {
        drain(&probes, &mut mirrors, &mut resyncs);
        mirrored(&mirrors) == before
    });
    let w2_position = mirrors["w-2"].position();

    // Kill the leader: w-2's stream dies with it.
    let killed = managers[leader].name;
    managers[leader].kill();
    let alive: Vec<usize> = (0..3).filter(|index| *index != leader).collect();
    wait_for(Duration::from_secs(90), "a new leader", || {
        alive.iter().any(|index| managers[*index].is_leader())
    });
    let new_leader = *alive
        .iter()
        .find(|index| managers[**index].is_leader())
        .context("new leader")?;
    eprintln!("killed {killed}; new leader {}", managers[new_leader].name);
    probes.remove("w-2").expect("w-2").close();
    mirrors.get_mut("w-2").expect("w-2").mark_down();

    // w-2 reconnects to the survivor with its old position: another epoch.
    let probe = Probe::open(&rt, managers[survivor].addr(), "w-2", w2_position, true)?;
    probes.insert("w-2", probe);

    // Work placed after the failover reaches both workers.
    create_and_place(managers[new_leader].store(), 2)?;
    let after = placed(managers[new_leader].store());
    assert_eq!(after.values().map(BTreeSet::len).sum::<usize>(), 6);
    wait_for(Duration::from_secs(30), "both streams converge", || {
        drain(&probes, &mut mirrors, &mut resyncs);
        mirrored(&mirrors) == after
    });
    std::thread::sleep(Duration::from_millis(300));
    drain(&probes, &mut mirrors, &mut resyncs);
    assert_eq!(mirrored(&mirrors), after, "nothing lost, nothing extra");
    assert_eq!(
        resyncs["w-1"], 0,
        "a stream on a surviving manager never resyncs"
    );
    assert_eq!(
        resyncs["w-2"], 1,
        "a worker whose manager died resyncs exactly once"
    );
    for (_, probe) in probes {
        probe.close();
    }
    for manager in &mut managers {
        manager.kill();
    }
    Ok(())
}

/// A job-level volume claim rides its job's allocation payloads, so creating
/// or changing one re-sends those allocations to a streaming worker. On a
/// raft manager that lookup runs inside the state machine's apply, where an
/// error stops the raft group. The spot-check regression was a lookup by a
/// column `allocation` does not have, which failed every job submit with a
/// claim (`no such column: job_id`) once any worker streamed.
#[test]
fn a_job_volume_change_reaches_a_streaming_worker_under_raft() -> R<()> {
    const NODE: &str = "vol-node";
    const VOLUME_JOB: &str = "web";
    let dir = tempfile::tempdir()?;
    let mut config = ManagerConfigFull {
        store_path: dir
            .path()
            .join("store")
            .to_str()
            .context("utf-8")?
            .to_string(),
        scheduling_policy: "bin_pack".to_string(),
        scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    config.persistence.raft.bootstrap = true;
    config.persistence.raft.node_id = "mgr-volume".to_string();
    config.persistence.raft.data_dir = dir
        .path()
        .join("raft")
        .to_str()
        .context("utf-8")?
        .to_string();
    let store = Store::start_storage(&config)?;
    store.insert_node(NodeMsg::from(node(NODE, 4)))?;

    let job = |volumes: Vec<VolumeSpec>| JobConfig {
        priority: Default::default(),
        name: VOLUME_JOB.to_string(),
        tasks: vec![TaskConfig {
            count: legion::structs::job::TaskCount::new(1),
            resources: json!({ "memory": 64, "cpu": 10 }),
            volumes,
            ..TaskConfig::new("svc", "noop")
        }],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
        pending_extensions: Default::default(),
        extensions: Default::default(),
    };
    let submission = [Submission {
        path: format!("{VOLUME_JOB}.json"),
        content: "{}".to_string(),
        is_primary: true,
    }];
    let outcome = store.handle_job_submit(&job(Vec::new()), &submission)?;
    let tasks = store.get_task_configs_for_job_config(outcome.job_config_id())?;
    let (task_id, _) = *tasks.first().context("the job's task config")?;
    store.place_alloc(NODE, task_id)?;
    let placed = allocation_ids(&store, NODE)?;
    assert_eq!(placed.len(), 1, "the job placed on the node");

    // A worker streams from this manager, so the projection feeds its log.
    let attached = store.work_log().attach(NODE, 0, 0);
    assert!(matches!(
        WorkLog::take_pending(&attached.log, attached.session),
        PendingWork::Resync
    ));

    // Creating the claim: a volume INSERT that names the job.
    store.handle_job_submit(
        &job(vec![VolumeSpec {
            name: "data".into(),
            driver: Some("local-dir".into()),
            mode: None,
            params: None,
            path: Some("/mnt/data".into()),
            read_only: None,
            pending_extensions: Default::default(),
            unknown_fields: Default::default(),
        }]),
        &submission,
    )?;
    match WorkLog::take_pending(&attached.log, attached.session) {
        PendingWork::Changed(ids) => assert!(
            placed.is_subset(&ids),
            "the job's allocations are re-sent: {ids:?} lacks {placed:?}"
        ),
        other => panic!("expected the job's allocations marked changed, got {other:?}"),
    }

    // A mount report is a volume UPDATE whose record omits the unchanged
    // `job_id`. It reaches the same allocations through a re-read of the row.
    let volume_id = store
        .list_volumes(&VolumeFilter {
            job_id: Some(VOLUME_JOB.to_string()),
            ..Default::default()
        })?
        .first()
        .context("the claim row")?
        .volume_id
        .clone();
    store.record_volume_mounted(&volume_id, NODE, "/mnt/data", true, None, &BTreeMap::new())?;
    match WorkLog::take_pending(&attached.log, attached.session) {
        PendingWork::Changed(ids) => assert!(
            placed.is_subset(&ids),
            "the mount report re-sends the job's allocations: {ids:?} lacks {placed:?}"
        ),
        other => panic!("expected the job's allocations marked changed, got {other:?}"),
    }
    Ok(())
}

/// A node that takes memory checkpoints of sandboxes (the shape
/// `raft_lifecycle_ops.rs` uses).
fn checkpoint_node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({
            "node_id": id,
            "maxAllocations": 16,
            "resources": { "memory": 65536, "cpu": 40000 },
        }),
        fingerprint_attrs: json!({
            "arch": "x86_64",
            "memory_total": "68719476736",
            "disk_total_space": "214748364800",
            "disk_available_space": "107374182400",
            "cpus": { "cpu-1": { "frequency": 2000.0 } },
            "cpu.vendor": "GenuineIntel",
            "cpu.model": "6:106:6",
            "cpu.features": "3f9a0c21",
            "sandbox.vmm": "firecracker",
            "sandbox.vmm.version": "1.12.0",
        }),
        dynamic_attrs: json!({ "buildVersion": "0.9.0", "capabilities": ["checkpoint"] }),
        health_checks: json!({}),
    }
}

fn checkpoint_report(snapshot_id: &str, op: &str, allocation_id: i64) -> SnapshotReport {
    SnapshotReport {
        snapshot_id: snapshot_id.into(),
        op: op.into(),
        ok: true,
        manifest_json: if op == "capture" {
            json!({
                "apiVersion": 1,
                "id": snapshot_id,
                "jobId": JOB,
                "task": TASK,
                "originAllocId": allocation_id,
                "runtime": "sandbox",
                "kind": "memory",
                "createdAt": "2026-10-10T00:00:00Z",
            })
            .to_string()
        } else {
            String::new()
        },
        allocation_id,
        ..Default::default()
    }
}

/// The next message that pushes checkpoint work, acking everything read.
fn next_push(
    probe: &Probe,
    rt: &tokio::runtime::Runtime,
    what: &str,
) -> (Instant, StreamWorkReply) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some((arrived, message)) = probe.next(Duration::from_millis(100)) {
            probe.ack(rt, message.epoch, message.seq);
            if !message.checkpoint_requests.is_empty() {
                return (arrived, message);
            }
        }
    }
    panic!("no checkpoint push: {what}");
}

/// Checkpoint work rides the stream (checkpoints-on-stream plan). On a raft
/// manager, where the projection that notices the snapshot rows runs inside
/// the state machine's apply:
///
/// - a suspend's capture request reaches the streaming worker within a
///   round trip of `SuspendInstance`, with no heartbeat anywhere;
/// - a dropped stream replays the message without its push, and the new
///   stream is pushed the work still outstanding;
/// - the heartbeat reply still lists it, as the record, until the report is
///   applied;
/// - the reclaim that follows a resume is pushed too.
#[test]
fn checkpoint_work_reaches_a_streaming_worker_without_a_heartbeat() -> R<()> {
    const NODE: &str = "ckpt-node";
    let dir = tempfile::tempdir()?;
    let mut config = ManagerConfigFull {
        store_path: dir
            .path()
            .join("store")
            .to_str()
            .context("utf-8")?
            .to_string(),
        scheduling_policy: "bin_pack".to_string(),
        scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    config.persistence.raft.bootstrap = true;
    config.persistence.raft.node_id = "mgr-ckpt".to_string();
    config.persistence.raft.data_dir = dir
        .path()
        .join("raft")
        .to_str()
        .context("utf-8")?
        .to_string();
    let store = Store::start_storage(&config)?;
    store.insert_node(NodeMsg::from(checkpoint_node(NODE)))?;
    store.handle_job_submit(
        &JobConfig {
            priority: Default::default(),
            name: JOB.to_string(),
            tasks: vec![TaskConfig {
                config: json!({ "image": "alpine", "command": ["sleep", "60"] }),
                resources: json!({ "memory": 512, "cpu": 500 }),
                stateful: Some(StatefulConfig {
                    mode: InstanceMode::Keyed,
                    require_id: false,
                    initial_instances: None,
                    on_lost: None,
                    storage: None,
                    envelope: None,
                    promote: None,
                    placement: None,
                    fence_after: None,
                    unknown_fields: Default::default(),
                }),
                suspend: Some(SuspendConfig {
                    checkpoint: Some("memory".into()),
                    ..Default::default()
                }),
                ..TaskConfig::new(TASK, "sandbox")
            }],
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
            pending_extensions: Default::default(),
            extensions: Default::default(),
        },
        &[Submission {
            path: format!("{JOB}.json"),
            content: "{}".to_string(),
            is_primary: true,
        }],
    )?;
    run_eval_worker(&store)?;
    let (rt, addr) = serve(&store)?;
    let client = rt.block_on(ManagerClient::connect(format!("http://{addr}")))?;
    let beat = |request: HeartbeatRequest| -> R<HeartbeatReply> {
        Ok(rt
            .block_on(client.clone().heartbeat(HeartbeatRequest {
                id: NODE.into(),
                ..request
            }))?
            .into_inner())
    };

    let probe = Probe::open_with(&rt, client.clone(), NODE, (0, 0), false)?;
    let opening = probe.expect("the opening resync");
    assert!(opening.checkpoint_requests.is_empty(), "no work yet");
    probe.ack(&rt, opening.epoch, opening.seq);

    // One running sandbox.
    let instance_id = rt
        .block_on(client.clone().create_instance(CreateInstanceRequest {
            job_id: JOB.into(),
            ..Default::default()
        }))?
        .into_inner()
        .instance_id;
    run_eval_worker(&store)?;
    let placed = probe.expect("the placement");
    probe.ack(&rt, placed.epoch, placed.seq);
    let allocation_id = placed.upserts.first().context("the upsert")?.allocation_id;
    beat(HeartbeatRequest {
        allocation_ids: vec![allocation_id],
        allocation_health: vec![AllocationHealth {
            allocation_id,
            healthy: true,
            status: "running".into(),
            artifact_states: None,
        }],
        ..Default::default()
    })?;

    // Suspend. Nothing heartbeats from here until the report.
    let sent = Instant::now();
    let suspend = rt
        .block_on(client.clone().suspend_instance(SuspendInstanceRequest {
            instance_id: instance_id.clone(),
            checkpoint: None,
            idempotency_key: None,
        }))?
        .into_inner();
    // The health beat above changed the allocation's row, so the stream may
    // re-send the allocation the node already holds, either in the message
    // that carries the capture or (under load) in one just before it. Such a
    // message never adds or stops anything; the worker acks it, as a real
    // one would, and the capture follows.
    let mut acked = (placed.epoch, placed.seq);
    let (arrived, pushed) = loop {
        let (arrived, message) = probe
            .next(Duration::from_secs(10))
            .context("the capture push")?;
        assert!(
            message.stops.is_empty()
                && message
                    .upserts
                    .iter()
                    .all(|upsert| upsert.allocation_id == allocation_id),
            "{message:#?}"
        );
        assert_eq!(
            message.digest, placed.digest,
            "a push does not move the digest"
        );
        if !message.checkpoint_requests.is_empty() {
            break (arrived, message);
        }
        probe.ack(&rt, message.epoch, message.seq);
        acked = (message.epoch, message.seq);
    };
    let [capture] = pushed.checkpoint_requests.as_slice() else {
        panic!("one capture pushed: {:?}", pushed.checkpoint_requests);
    };
    assert_eq!(capture.op, "capture");
    assert_eq!(capture.snapshot_id, suspend.snapshot_id);
    assert_eq!(capture.allocation_id, allocation_id);
    assert_eq!(capture.then, "stop");
    let latency = arrived.saturating_duration_since(sent);
    println!(
        "work_stream_measure {}",
        json!({
            "measure": "suspend_to_capture_push",
            "latency_us": latency.as_micros() as u64,
        })
    );
    assert!(
        latency < Duration::from_secs(2),
        "the capture request took {latency:?}, a heartbeat interval or more"
    );

    // The stream drops before the worker acks the push. The reconnect
    // replays the message without its push, then pushes the capture
    // afresh because it is still outstanding.
    probe.close();
    let work_log = store.work_log();
    wait_for(Duration::from_secs(5), "the stream detaches", || {
        !work_log.streaming(NODE)
    });
    let probe = Probe::open_with(&rt, client.clone(), NODE, acked, false)?;
    let replayed = probe.expect("the replayed message");
    assert_eq!(replayed.seq, pushed.seq);
    assert!(!replayed.resync);
    assert!(
        replayed.checkpoint_requests.is_empty(),
        "a push is never replayed"
    );
    probe.ack(&rt, replayed.epoch, replayed.seq);
    let (_, again) = next_push(&probe, &rt, "the outstanding capture, on the new stream");
    assert_eq!(again.checkpoint_requests.len(), 1);
    assert_eq!(
        again.checkpoint_requests[0].snapshot_id,
        suspend.snapshot_id
    );

    // The heartbeat is still the record: it lists the capture until the
    // report lands, and not after.
    let unreported = beat(HeartbeatRequest::default())?;
    assert_eq!(unreported.checkpoint_requests.len(), 1);
    let reported = beat(HeartbeatRequest {
        snapshot_reports: vec![checkpoint_report(
            &suspend.snapshot_id,
            "capture",
            allocation_id,
        )],
        ..Default::default()
    })?;
    assert!(reported.checkpoint_requests.is_empty());
    let info = rt
        .block_on(client.clone().get_instance(GetInstanceRequest {
            instance_id: instance_id.clone(),
        }))?
        .into_inner()
        .instance
        .context("instance")?;
    assert_eq!(info.state, "suspended");

    // Resume, then report the restore: the suspend checkpoint is reclaimed,
    // and the delete is pushed as soon as its row commits.
    rt.block_on(client.clone().start_instance(StartInstanceRequest {
        instance_id: instance_id.clone(),
        idempotency_key: None,
        cold: false,
        force_move: false,
    }))?;
    run_eval_worker(&store)?;
    let resumed = loop {
        let message = probe.expect("the resumed allocation");
        probe.ack(&rt, message.epoch, message.seq);
        if let Some(allocation) = message
            .upserts
            .iter()
            .find(|allocation| allocation.restore.is_some())
        {
            break allocation.allocation_id;
        }
    };
    beat(HeartbeatRequest {
        snapshot_reports: vec![checkpoint_report(&suspend.snapshot_id, "restore", resumed)],
        ..Default::default()
    })?;
    let (_, reclaim) = next_push(&probe, &rt, "the reclaim of the suspend checkpoint");
    assert!(
        reclaim
            .checkpoint_requests
            .iter()
            .any(|request| request.op == "delete" && request.snapshot_id == suspend.snapshot_id),
        "{:?}",
        reclaim.checkpoint_requests
    );
    probe.close();
    Ok(())
}
