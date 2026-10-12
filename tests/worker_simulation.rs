//! End-to-end simulated-fleet tests (`legion start worker --simulate N`;
//! see `internal_docs/plans/worker-simulation-plan.md`): a real
//! `ManagerDaemonService` served over tonic, a profile-weighted sim fleet
//! registering against it over the real wire, scheduling onto sim nodes,
//! and ack delivery through heartbeat replies and push dispatch.

use std::time::{Duration, Instant};

use legion::manager::scheduler::{PlanMode, Scheduler};
use legion::manager::server::ManagerDaemonService;
use legion::manager::telemetry::{render_worker, worker_metrics};
use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, manager_server::ManagerServer, worker_client::WorkerClient,
    GetTaskLogsRequest, JobConfig, PostWorkRequest, Submission, TaskAllocation, TaskConfig,
    TaskCount, WorkerConfig,
};
use legion::worker::simulate::{launch_simulated_fleet, SimFleetRun, SimulateOptions};
use tempfile::TempDir;

const DEADLINE: Duration = Duration::from_secs(30);

fn setup_store(dir: &TempDir) -> R<Store> {
    let config = ManagerConfigFull {
        store_path: dir
            .path()
            .join("store")
            .to_str()
            .context("store path is not utf-8")?
            .to_string(),
        // Benchmark-style defaults: noop tasks with no declared resources
        // must fit on any node.
        task_resource_defaults: Some(json!({ "cpu": 0, "memory": 0, "allocations": 1000 })),
        ..Default::default()
    };
    let store = Store::start_storage(&config)?;
    store.insert_worker_type(&WorkerConfig {
        name: "default".into(),
        pool: "default".into(),
        ..WorkerConfig::default()
    })?;
    Ok(store)
}

/// Serve a real manager service on an ephemeral loopback port.
async fn serve_manager(store: &Store) -> R<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://127.0.0.1:{}", listener.local_addr()?.port());
    let service = ManagerServer::new(ManagerDaemonService::new(store.clone()));
    let handle = tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        if let Err(err) = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
        {
            eprintln!("test manager server exited: {err}");
        }
    });
    Ok((addr, handle))
}

async fn wait_until(what: &str, mut probe: impl FnMut() -> bool) {
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        if probe() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

fn write_profiles(dir: &TempDir) -> R<String> {
    // Weighted 3:1 — with N=8 the largest-remainder split is 6 web / 2 util.
    let path = dir.path().join("profiles.json");
    std::fs::write(
        &path,
        json!({
            "node_profiles": [
                { "name": "web", "count": 3, "memory_gb": 8, "cpus": 2,
                  "attrs": { "class": "web" } },
                { "name": "util", "count": 1, "memory_gb": 4, "cpus": 1,
                  "attrs": { "class": "util" } }
            ]
        })
        .to_string(),
    )?;
    Ok(path
        .to_str()
        .context("profiles path is not utf-8")?
        .to_string())
}

async fn launch_fleet(dir: &TempDir, manager_addr: String, count: usize) -> R<SimFleetRun> {
    let opts = SimulateOptions {
        count,
        profiles_path: Some(write_profiles(dir)?),
        name_prefix: "sim-it".into(),
        ready_after: Duration::ZERO,
        register_rate: 1000.0,
        advertise_host: "127.0.0.1".into(),
        ..Default::default()
    };
    launch_simulated_fleet(manager_addr, "default".into(), None, opts).await
}

#[tokio::test(flavor = "multi_thread")]
async fn simulated_fleet_registers_schedules_and_acks() -> R<()> {
    let dir = TempDir::new()?;
    let store = setup_store(&dir)?;
    let (manager_addr, _manager) = serve_manager(&store).await?;

    let fleet = launch_fleet(&dir, manager_addr, 8).await?;
    wait_until("all 8 sim nodes registered", || fleet.registered() == 8).await;

    // Profile-weighted fleet shape and attrs, as the manager sees them.
    let nodes = store.get_nodes()?;
    assert_eq!(nodes.len(), 8);
    let web: Vec<_> = nodes
        .iter()
        .filter(|node| node.id.starts_with("sim-it-web-"))
        .collect();
    let util: Vec<_> = nodes
        .iter()
        .filter(|node| node.id.starts_with("sim-it-util-"))
        .collect();
    assert_eq!((web.len(), util.len()), (6, 2), "3:1 weights over N=8");
    for node in &nodes {
        assert_eq!(node.config_attrs["simulated"], json!(true));
        assert!(
            node.config_attrs["workerAddress"]
                .as_str()
                .is_some_and(|addr| addr == fleet.worker_address()),
            "every node advertises the shared stub server"
        );
    }
    assert_eq!(web[0].config_attrs["class"], json!("web"));
    assert_eq!(
        web[0].fingerprint_attrs["memory_total"],
        json!((8u64 * 1024 * 1024 * 1024).to_string())
    );
    assert_eq!(
        util[0].fingerprint_attrs["cpus"]["cpu-1"]["frequency"],
        json!("3000")
    );
    assert!(util[0].fingerprint_attrs["cpus"].get("cpu-2").is_none());

    // Schedule a constrained noop job; allocations must land on web nodes
    // only, and the fleet must ack them via heartbeat-reply delivery.
    let mut task = TaskConfig::new("svc", "noop");
    task.count = TaskCount::new(3);
    task.constraints = json!({ "class": "web" });
    let job = JobConfig {
        priority: Default::default(),
        name: "sim-it-job".into(),
        tasks: vec![task],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    };
    let submission = Submission {
        path: "sim-it.ncl".into(),
        content: "{}".into(),
        is_primary: true,
    };
    store.handle_job_submit(&job, std::slice::from_ref(&submission))?;
    let job_config_id = store.get_job_config(&job.name)?.id();
    Scheduler::new(&store)?.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

    let allocations = store.allocs_for_job_config(&job_config_id)?;
    assert_eq!(allocations.len(), 3);
    for allocation in &allocations {
        assert!(
            allocation.node_id.starts_with("sim-it-web-"),
            "constraint must keep {} off util nodes",
            allocation.node_id
        );
    }

    wait_until("fleet acks the 3 allocations", || {
        fleet.acked_allocations() == 3
    })
    .await;

    // Heartbeats also refresh manager-side liveness — in the leader's MEMORY,
    // not in replicated state: a steady-state beat costs no commit
    // (docs/node-liveness.md). The persisted timestamp column is refreshed by
    // the display checkpoint, which the status-checker loop drives and this
    // test does not run.
    let nodes = store.get_nodes()?;
    for node in &nodes {
        assert!(
            store.liveness().last_beat(&node.id).is_some(),
            "every sim node has heartbeated: {} has no observed beat",
            node.id
        );
    }

    fleet.abort();
    Ok(())
}

/// A simulated host is exactly the host a scale campaign scrapes, so the
/// fleet must feed the process-wide `legion_worker_*` registry rather than
/// keeping its counts private (docs/metrics.md § Worker).
///
/// Asserted as *deltas* against a baseline: the registry is a process
/// global and the other tests in this binary run fleets of their own
/// concurrently, so absolute values are not this test's to own.
#[tokio::test(flavor = "multi_thread")]
async fn simulated_fleet_feeds_the_worker_metric_registry() -> R<()> {
    let dir = TempDir::new()?;
    let store = setup_store(&dir)?;
    let (manager_addr, _manager) = serve_manager(&store).await?;

    let metrics = worker_metrics();
    let registrations_before = metrics.simulated_registrations_total.get(&["ok"]);
    let heartbeats_before = metrics.heartbeats_total.get(&["ok"]);

    let fleet = launch_fleet(&dir, manager_addr, 4).await?;
    wait_until("fleet registered", || fleet.registered() == 4).await;

    assert!(
        metrics.simulated_registrations_total.get(&["ok"]) - registrations_before >= 4,
        "every sim node's registration is counted"
    );
    assert!(
        metrics.simulated_nodes.get() >= 4,
        "the fleet-size gauge tracks registered nodes"
    );
    assert!(
        metrics.simulated_nodes_target.get() >= 4,
        "the target gauge records the requested fleet size"
    );

    wait_until("heartbeats are counted in the shared registry", || {
        metrics.heartbeats_total.get(&["ok"]) - heartbeats_before >= 4
    })
    .await;

    // The families a campaign scrapes are actually rendered, with the
    // heartbeat histogram observed rather than merely declared.
    let rendered = render_worker();
    for family in [
        "legion_worker_heartbeats_total",
        "legion_worker_heartbeat_duration_seconds",
        "legion_worker_allocations_running",
        "legion_worker_simulated_nodes",
        "legion_worker_simulated_nodes_target",
        "legion_worker_simulated_registrations_total",
    ] {
        assert!(
            rendered.contains(family),
            "{family} missing from the worker /metrics body"
        );
    }
    assert!(
        !rendered.contains("legion_worker_heartbeat_duration_seconds_count 0"),
        "heartbeat durations must be observed, not just declared"
    );

    fleet.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stub_worker_accepts_dispatch_and_rejects_everything_else() -> R<()> {
    let dir = TempDir::new()?;
    let store = setup_store(&dir)?;
    let (manager_addr, _manager) = serve_manager(&store).await?;

    let fleet = launch_fleet(&dir, manager_addr, 4).await?;
    wait_until("fleet registered", || fleet.registered() == 4).await;

    // Push dispatch (the manager's PostWork path) routes on node_id.
    let mut worker = WorkerClient::connect(fleet.worker_address().to_string()).await?;
    worker
        .post_work(PostWorkRequest {
            allocation: Some(TaskAllocation {
                provisional: None,
                allocation_id: 999,
                node_id: "sim-it-web-0".into(),
                ..TaskAllocation::default()
            }),
        })
        .await
        .context("push dispatch to a sim node must be accepted")?;
    wait_until("pushed allocation acked", || fleet.acked_allocations() >= 1).await;

    // Unknown node → NotFound, so a misrouted dispatch is loud.
    let status = worker
        .post_work(PostWorkRequest {
            allocation: Some(TaskAllocation {
                provisional: None,
                allocation_id: 1000,
                node_id: "not-a-sim-node".into(),
                ..TaskAllocation::default()
            }),
        })
        .await
        .expect_err("dispatch to an unknown node must fail");
    assert_eq!(status.code(), tonic::Code::NotFound);

    // No logs, no exec, no restart: clean Unimplemented, not a hang.
    let status = worker
        .get_task_logs(GetTaskLogsRequest {
            allocation_id: 999,
            ..GetTaskLogsRequest::default()
        })
        .await
        .expect_err("sim workers have no logs");
    assert_eq!(status.code(), tonic::Code::Unimplemented);

    fleet.abort();
    Ok(())
}
