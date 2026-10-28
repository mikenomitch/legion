//! Scaling inputs end to end (docs/autoscaling.md, "Scaling inputs"):
//! heartbeats carrying usage reports and service-graph samples, the rollup
//! tick that folds them, and `GetScalingInputs` reading the merged totals.
//!
//! - One manager: everything merges locally, no RPC.
//! - Three managers (raft, `single` topology): each hears a different node,
//!   followers ship rollups to the status-lane leader on the liveness digest,
//!   every manager answers with the same totals (followers forward), and a
//!   node that moves to another manager is counted once.
//!
//! The merge rules themselves (stale managers, resyncs, size bounds) are
//! unit-tested in `src/manager/scaling_inputs.rs`.

use crate::support::cluster_gate;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use legion::manager::leader_forward::LeaderForwardLayer;
use legion::manager::scaling_inputs::{RollupTicker, TickOutcome};
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::manager_client::ManagerClient;
use legion::structs::manager_server::{Manager, ManagerServer};
use legion::structs::{
    AllocationHealth, AllocationUsage, Eval, GetScalingInputsRequest, HeartbeatRequest, JobConfig,
    NetEdgeSample, NodeMsg, ScalingInputs, ScalingTargetSelector, Submission, TaskConfig,
    UsageReport,
};
use tonic::Request;

const MIB: u64 = 1024 * 1024;

/// A node with two 2400 MHz cores, room for `max_allocations`.
fn node(id: &str, max_allocations: u32) -> NodeMsg {
    NodeMsg {
        id: id.to_string(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: json!({ "maxAllocations": max_allocations }).to_string(),
        fingerprint_attrs: json!({
            "memory_total": (8192 * MIB).to_string(),
            "cpus": { "cpu-1": { "frequency": "2400" }, "cpu-2": { "frequency": "2400" } }
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: "{}".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
    }
}

fn job(name: &str, tasks: Vec<serde_json::Value>) -> R<JobConfig> {
    Ok(JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: tasks
            .into_iter()
            .map(serde_json::from_value::<TaskConfig>)
            .collect::<Result<_, _>>()?,
        version: None,
    })
}

/// Submit and run the job's `job_submit` eval inline.
fn submit_and_place(store: &Store, job: &JobConfig) -> R<()> {
    store.handle_job_submit(
        job,
        &[Submission {
            path: format!("{}.json", job.name),
            content: job.name.clone(),
            is_primary: true,
        }],
    )?;
    let eval = Eval {
        id: format!("eval-{}", job.name),
        status: "pending".into(),
        kind: "job_submit".into(),
        job_id: job.name.clone(),
        job_config_id: store.get_job_config(&job.name)?.id(),
        node_name: String::new(),
        priority: 50,
    };
    // A partial placement is part of what the tests read, not a failure.
    let _ = eval.handle(store);
    Ok(())
}

fn usage(allocation_id: i64, millicores: u64, memory_mib: u64) -> AllocationUsage {
    AllocationUsage {
        allocation_id,
        cpu_millicores: millicores,
        memory_current_bytes: memory_mib * MIB,
        ..Default::default()
    }
}

fn report(sampled_at_unix_ms: i64, allocations: Vec<AllocationUsage>) -> UsageReport {
    UsageReport {
        sampled_at_unix_ms,
        allocations,
        ..Default::default()
    }
}

fn select(job: &str) -> GetScalingInputsRequest {
    GetScalingInputsRequest {
        targets: vec![ScalingTargetSelector {
            job: job.into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn single_store(dir: &std::path::Path) -> R<Store> {
    Store::start_storage(&ManagerConfigFull {
        store_path: dir.to_str().context("store path")?.to_string(),
        ..Default::default()
    })
}

/// One manager: usage, health, and service traffic heartbeat in, one tick
/// folds them, and GetScalingInputs reads request vs usage, unplaced
/// replicas, request rates, and history.
#[tokio::test(flavor = "multi_thread")]
async fn one_manager_serves_request_vs_usage() -> R<()> {
    let dir = tempfile::tempdir_in(crate::support::scratch_root())?;
    let store = single_store(dir.path())?;
    store.insert_node(node("n1", 2))?;
    store.insert_node(node("n2", 2))?;
    // `api` wants 5 replicas and fits 2 per node: 4 place, 1 does not.
    // `front` calls it.
    submit_and_place(
        &store,
        &job(
            "shop",
            vec![json!({
                "name": "api",
                "runtime": "noop",
                "count": 5,
                "config": {},
                "resources": { "cpu": 1200, "memory": 256 },
                "network": { "mode": "host", "ports": [{ "name": "http" }] },
                "service": { "name": "shop-api", "port": "http" },
            })],
        )?,
    )?;
    let svc = ManagerDaemonService::new(store.clone());
    let allocations = store.allocs_for_job("shop")?;
    assert_eq!(allocations.len(), 4, "two nodes, two each");

    // Each node reports its two replicas at 300 and 900 millicores (at 2400
    // MHz per core: 720 and 2160 MHz, 25% and 75% of 1200 MHz), all healthy.
    let mut sampled_at = 1_700_000_000_000;
    for node_id in ["n1", "n2"] {
        let mine: Vec<i64> = allocations
            .iter()
            .filter(|allocation| allocation.node_id == node_id)
            .map(|allocation| allocation.id)
            .collect();
        sampled_at += 1;
        svc.heartbeat(Request::new(HeartbeatRequest {
            id: node_id.into(),
            usage: Some(report(
                sampled_at,
                vec![usage(mine[0], 300, 128), usage(mine[1], 900, 192)],
            )),
            allocation_health: mine
                .iter()
                .map(|id| AllocationHealth {
                    allocation_id: *id,
                    healthy: true,
                    status: "running".into(),
                    artifact_states: None,
                })
                .collect(),
            ..Default::default()
        }))
        .await?;
    }

    // Before any tick there is nothing merged.
    let before = svc
        .get_scaling_inputs(Request::new(select("shop")))
        .await?
        .into_inner();
    let state = before.inputs[0].freshness.as_ref().context("freshness")?;
    assert_ne!(state.state, "current");

    let mut ticker = RollupTicker::new("local");
    let outcome = ticker.tick(&store, &svc.usage_handle()).await;
    assert_eq!(outcome, TickOutcome::Merged { rollups: 2 });

    let reply = svc
        .get_scaling_inputs(Request::new(GetScalingInputsRequest {
            history_seconds: 600,
            ..select("shop")
        }))
        .await?
        .into_inner();
    assert_eq!(reply.served_by, "local");
    assert_eq!(reply.inputs.len(), 1);
    let api = &reply.inputs[0];
    assert!(api.error.is_empty(), "{}", api.error);
    assert_eq!(
        (api.task_group.as_str(), api.task_name.as_str()),
        ("default", "api")
    );
    assert_eq!(api.desired_replicas, 5);
    assert_eq!(api.placed_replicas, 4);
    assert_eq!(api.ready_replicas, 4);
    assert_eq!(api.unplaced_replicas, 1);
    assert!(api.count_scalable);
    assert!(!api.rollout_active);
    let reservation = api.reservation.as_ref().context("reservation")?;
    assert_eq!(
        (reservation.cpu_mhz, reservation.memory_mib),
        (1200.0, 256.0)
    );

    let cpu = api.cpu.as_ref().context("cpu")?;
    assert_eq!(cpu.measured_replicas, 4);
    assert_eq!(cpu.used_millicores, 2400);
    assert!((cpu.used_mhz - 5760.0).abs() < 1e-6, "{}", cpu.used_mhz);
    assert!((cpu.reserved_mhz - 4800.0).abs() < 1e-6);
    assert!((cpu.utilization - 1.2).abs() < 1e-9, "{}", cpu.utilization);
    assert!((cpu.max_utilization - 1.8).abs() < 1e-9);
    assert_eq!(cpu.histogram.iter().sum::<u32>(), 4);
    assert_eq!(cpu.histogram.len(), cpu.histogram_bounds.len() + 1);
    let memory = api.memory.as_ref().context("memory")?;
    assert!((memory.used_mib - 640.0).abs() < 1e-6);
    assert!((memory.reserved_mib - 1024.0).abs() < 1e-6);
    let freshness = api.freshness.as_ref().context("freshness")?;
    assert_eq!(freshness.state, "current");
    assert_eq!(freshness.reporting_nodes, 2);
    assert_eq!(freshness.sampled_at_unix_ms, sampled_at);
    let requests = api.requests.as_ref().context("the task backs a service")?;
    assert_eq!(requests.service, "shop-api");
    assert_eq!(requests.requests_per_second, 0.0);
    assert_eq!(api.history.len(), 1, "one tick, one point");
    assert_eq!(api.history[0].measured_replicas, 4);

    // A caller on n1 reaches the service: one allocation of the api task
    // itself (an undeclared key resolves in the caller's namespace).
    let caller = allocations
        .iter()
        .find(|allocation| allocation.node_id == "n1")
        .context("an allocation on n1")?;
    svc.heartbeat(Request::new(HeartbeatRequest {
        id: "n1".into(),
        net_edges: vec![NetEdgeSample {
            allocation_id: caller.id,
            service: "shop-api".into(),
            protocol: "http".into(),
            requests: 400,
            errors: 100,
            latency_ms_sum: 8_000,
            latency_ms_max: 95,
            ..Default::default()
        }],
        ..Default::default()
    }))
    .await?;
    ticker.tick(&store, &svc.usage_handle()).await;
    let reply = svc
        .get_scaling_inputs(Request::new(select("shop")))
        .await?
        .into_inner();
    let requests = reply.inputs[0].requests.as_ref().context("requests")?;
    assert!(requests.requests_per_second > 0.0);
    assert!((requests.error_ratio - 0.2).abs() < 1e-9);
    assert!((requests.latency_ms_mean - 20.0).abs() < 1e-9);
    assert_eq!(requests.latency_ms_max, 95);
    assert_eq!(requests.calling_nodes, 1);

    // Unknown jobs and tasks are per-entry errors, not a failed batch.
    let reply = svc
        .get_scaling_inputs(Request::new(GetScalingInputsRequest {
            targets: vec![
                ScalingTargetSelector {
                    job: "nope".into(),
                    ..Default::default()
                },
                ScalingTargetSelector {
                    job: "shop".into(),
                    task_name: Some("missing".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert_eq!(reply.inputs.len(), 2);
    assert_eq!(reply.inputs[0].error, "job not found");
    assert!(reply.inputs[1].error.starts_with("no task matches"));

    // A namespace listing finds the job without naming it.
    let listing = svc
        .get_scaling_inputs(Request::new(GetScalingInputsRequest::default()))
        .await?
        .into_inner();
    assert!(listing.inputs.iter().any(|inputs| inputs.job_id == "shop"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Three managers
// ---------------------------------------------------------------------------

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

struct TestManager {
    port: u16,
    store: Store,
    service: Arc<ManagerDaemonService>,
    _rt: tokio::runtime::Runtime,
}

impl TestManager {
    fn addr(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn leads(&self) -> bool {
        self.store
            .raft_status()
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

/// Start one manager and serve the Manager API (behind the leader-forward
/// layer) and the raft transport on one listener, as the daemon does. The
/// service is kept so the test can heartbeat into its usage table.
fn start_manager(
    root: &std::path::Path,
    name: &str,
    bootstrap: bool,
    join_token: Option<String>,
) -> R<TestManager> {
    let port = reserve_port();
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
    let store = Store::start_storage(&config)?;
    let service = Arc::new(ManagerDaemonService::new(store.clone()));
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let transport_tls = store.raft_transport_tls().context("raft engine started")?;
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
    let (served_store, served) = (store.clone(), service.clone());
    rt.spawn(async move {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket.set_reuseaddr(true).expect("reuseaddr");
        socket.bind(addr).expect("bind manager port");
        let listener = socket.listen(64).expect("listen");
        let incoming = transport_tls
            .incoming(listener)
            .expect("raft transport listener");
        if let Err(err) = tonic::transport::Server::builder()
            .layer(LeaderForwardLayer::new(served_store.clone()))
            .add_service(ManagerServer::from_arc(served))
            .add_service(served_store.raft_transport_server())
            .serve_with_incoming(incoming)
            .await
        {
            eprintln!("manager server exited: {err}");
        }
    });
    std::thread::sleep(Duration::from_millis(50));
    Ok(TestManager {
        port,
        store,
        service,
        _rt: rt,
    })
}

async fn inputs_via(addr: String) -> R<(String, ScalingInputs)> {
    let mut client = ManagerClient::connect(addr).await?;
    let reply = client.get_scaling_inputs(select("web")).await?.into_inner();
    let inputs = reply.inputs.into_iter().next().context("one task")?;
    Ok((reply.served_by, inputs))
}

/// P1 acceptance (autoscaler-v2 plan §12): a job whose allocations heartbeat
/// to three different managers reads the same per-task CPU on every manager,
/// and a node that switches managers mid-test is not double-counted.
#[test]
fn three_managers_agree_and_a_moved_node_counts_once() -> R<()> {
    let _serial = cluster_gate();
    let dir = tempfile::tempdir_in(crate::support::scratch_root())?;
    let a = start_manager(dir.path(), "mgr-a", true, None)?;
    let (token, _expires) = a.store.issue_raft_join_token("test")?;
    let b = start_manager(dir.path(), "mgr-b", false, Some(token.clone()))?;
    let c = start_manager(dir.path(), "mgr-c", false, Some(token))?;
    let managers = [a, b, c];
    wait_for(
        Duration::from_secs(150),
        "a three-voter cluster with a leader",
        || {
            managers.iter().all(|manager| {
                manager
                    .store
                    .raft_status()
                    .is_some_and(|status| status.voters.len() == 3 && status.leader_id.is_some())
            })
        },
    );
    let mut leader = None;
    wait_for(Duration::from_secs(60), "a leader", || {
        leader = managers.iter().position(TestManager::leads);
        leader.is_some()
    });
    let leader = leader.context("leader")?;
    // Manager i hears node i; the leader first.
    let order = [leader, (leader + 1) % 3, (leader + 2) % 3];

    let leader_store = &managers[leader].store;
    for n in 0..3 {
        leader_store.insert_node(node(&format!("wkr-{n}"), 1))?;
    }
    submit_and_place(
        leader_store,
        &job(
            "web",
            vec![json!({
                "name": "api",
                "runtime": "noop",
                "count": 3,
                "config": {},
                "resources": { "cpu": 1000, "memory": 256 },
            })],
        )?,
    )?;
    wait_for(
        Duration::from_secs(30),
        "three placements on every manager",
        || {
            managers.iter().all(|manager| {
                manager
                    .store
                    .allocs_for_job("web")
                    .is_ok_and(|allocs| allocs.len() == 3)
            })
        },
    );
    let allocations = leader_store.allocs_for_job("web")?;
    let on = |node_id: &str| -> R<i64> {
        Ok(allocations
            .iter()
            .find(|allocation| allocation.node_id == node_id)
            .with_context(|| format!("an allocation on {node_id}"))?
            .id)
    };

    let rt = tokio::runtime::Runtime::new()?;
    let mut tickers: Vec<RollupTicker> = managers
        .iter()
        .enumerate()
        .map(|(i, _)| RollupTicker::new(&format!("mgr-{i}")))
        .collect();
    let beat = |manager: &TestManager, node_id: &str, sampled_at: i64, millicores: u64| -> R<()> {
        let allocation = on(node_id)?;
        rt.block_on(manager.service.heartbeat(Request::new(HeartbeatRequest {
            id: node_id.into(),
            usage: Some(report(sampled_at, vec![usage(allocation, millicores, 100)])),
            ..Default::default()
        })))?;
        Ok(())
    };
    // wkr-0 -> leader, wkr-1 -> follower 1, wkr-2 -> follower 2.
    for (n, &index) in order.iter().enumerate() {
        beat(
            &managers[index],
            &format!("wkr-{n}"),
            1_000,
            100 * (n as u64 + 1),
        )?;
    }
    // Followers ship first, then the leader merges its own.
    for &index in order.iter().rev() {
        let outcome = rt.block_on(tickers[index].tick(
            &managers[index].store,
            &managers[index].service.usage_handle(),
        ));
        if index == leader {
            assert_eq!(outcome, TickOutcome::Merged { rollups: 1 });
        } else {
            assert_eq!(
                outcome,
                TickOutcome::Sent {
                    rollups: 1,
                    full: true
                },
                "manager {index}"
            );
        }
    }

    let leader_id = managers[leader]
        .store
        .raft_status()
        .map(|status| status.node_id)
        .context("leader node id")?;
    for manager in &managers {
        let (served_by, inputs) = rt.block_on(inputs_via(manager.addr()))?;
        assert_eq!(
            served_by, leader_id,
            "every manager answers with the leader's view"
        );
        let cpu = inputs.cpu.context("cpu")?;
        assert_eq!(cpu.measured_replicas, 3);
        assert_eq!(cpu.used_millicores, 100 + 200 + 300);
        assert_eq!(inputs.freshness.context("freshness")?.reporting_nodes, 3);
    }

    // wkr-1 moves from follower 1 to follower 2 with a newer sample.
    let (from, to) = (order[1], order[2]);
    beat(&managers[to], "wkr-1", 11_000, 700)?;
    let moved =
        rt.block_on(tickers[to].tick(&managers[to].store, &managers[to].service.usage_handle()));
    assert!(
        matches!(moved, TickOutcome::Sent { full: false, .. }),
        "{moved:?}"
    );
    // The old manager still holds wkr-1's last report and ticks too: it has
    // nothing new to say, and its older rollup could not win anyway.
    let stale = rt.block_on(tickers[from].tick(
        &managers[from].store,
        &managers[from].service.usage_handle(),
    ));
    assert!(matches!(stale, TickOutcome::Sent { .. }), "{stale:?}");
    rt.block_on(tickers[leader].tick(
        &managers[leader].store,
        &managers[leader].service.usage_handle(),
    ));

    for manager in &managers {
        let (_, inputs) = rt.block_on(inputs_via(manager.addr()))?;
        let cpu = inputs.cpu.context("cpu")?;
        assert_eq!(cpu.measured_replicas, 3, "wkr-1 counted once");
        assert_eq!(cpu.used_millicores, 100 + 700 + 300);
    }
    drop(managers);
    Ok(())
}
