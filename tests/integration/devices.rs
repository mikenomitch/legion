//! The device model end to end with the fake device plugin
//! (`plugins/devices/fake`, docs/devices.md):
//!
//! - the worker side fingerprints the plugin and its report rides the
//!   node's health checks;
//! - a job asking for two devices places, and its allocation records the
//!   two ids the store assigned;
//! - the dispatched payload carries the assignment, and an `exec` task
//!   started from it sees `LEGION_DEVICE_*_IDS` and the plugin's env;
//! - an instance the plugin reports unhealthy (through the `HealthCheck`
//!   RPC) is never assigned again;
//! - a namespace quota on devices refuses the job that would exceed it.
//!
//! What a real GPU and a real OCI runtime do with the edits is not covered
//! here: there is no GPU and no runc in CI (docs/devices.md, "What has been
//! verified").

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use legion::devices::plugin::{DeviceManager, DevicePluginConfig, WorkerDevicesConfig};
use legion::manager::dispatcher::build_task_allocation;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::manager_server::Manager;
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, job::TaskConfig, job::TaskCount, Band, Eval,
    HealthCheckRequest, Node, NodeMsg, Submission,
};
use legion::worker::state::WorkerStateStore;
use legion::worker::task_manager::{launch_task_allocation, WorkerTaskManager};
use serde_json::Value;
use tonic::Request;

const MIB: u64 = 1024 * 1024;

struct Cluster {
    svc: ManagerDaemonService,
    eval_seq: u64,
    dir: tempfile::TempDir,
}

impl Cluster {
    fn new() -> R<Self> {
        let dir = tempfile::Builder::new()
            .prefix("legion-devices-")
            .tempdir_in(crate::support::scratch_root())?;
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir
                .path()
                .join("store")
                .to_str()
                .context("store path")?
                .to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        })?;
        Ok(Self {
            svc: ManagerDaemonService::new(store),
            eval_seq: 0,
            dir,
        })
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    fn add_node(&self, id: &str, health_checks: Value) -> R<()> {
        self.store().insert_node(NodeMsg::from(Node {
            id: id.to_string(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2026-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({ "maxAllocations": 50 }),
            fingerprint_attrs: json!({
                "memory_total": (8192 * MIB).to_string(),
                "cpus": { "0": { "frequency": "3000" }, "1": { "frequency": "3000" } },
            }),
            dynamic_attrs: json!({}),
            health_checks,
        }))?;
        Ok(())
    }

    /// Report health checks the way a worker's node-check loop does.
    async fn report(&self, id: &str, health_checks: Value) -> R<()> {
        self.svc
            .health_check(Request::new(HealthCheckRequest {
                id: id.to_string(),
                health_checks: health_checks.to_string(),
            }))
            .await
            .map_err(|status| anyhow::anyhow!("health_check failed: {status}"))?;
        Ok(())
    }

    fn submit(&mut self, job: JobConfig) -> R<()> {
        let name = job.name.clone();
        self.store().handle_job_submit(
            &job,
            &[Submission {
                path: format!("{name}.json"),
                content: name.clone(),
                is_primary: true,
            }],
        )?;
        let job_config_id = self.store().get_job_config(&name)?.id();
        self.eval_seq += 1;
        Eval {
            id: format!("devices-test-{}", self.eval_seq),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: name,
            job_config_id,
            node_name: String::new(),
            priority: 50,
        }
        .handle(self.store())?;
        Ok(())
    }

    /// The ids assigned to each of `job`'s allocations.
    fn assigned(&self, job: &str) -> R<Vec<Vec<String>>> {
        Ok(self
            .store()
            .allocs_for_job(job)?
            .iter()
            .map(|allocation| {
                legion::devices::assignments(&allocation.resources)
                    .into_iter()
                    .flat_map(|(_, assignment)| assignment.ids)
                    .collect()
            })
            .collect())
    }
}

fn fake_plugin_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("plugins/devices/fake/legion-device-fake")
}

/// A device manager running the shipped fake plugin over `inventory`.
fn fake_devices(dir: &Path, inventory: &Value) -> R<(DeviceManager, PathBuf)> {
    let path = dir.join("fake-devices.json");
    std::fs::write(&path, inventory.to_string())?;
    let mut config = WorkerDevicesConfig::default();
    config.plugins.insert(
        "fake".into(),
        DevicePluginConfig {
            command: vec![
                fake_plugin_path().display().to_string(),
                path.display().to_string(),
            ],
            config: Value::Null,
            timeout: Some("10s".into()),
        },
    );
    config.validate()?;
    Ok((DeviceManager::new(config), path))
}

/// Three fake GPUs: two sharing a peer group, one alone.
fn inventory(unhealthy: &[&str]) -> Value {
    let instance = |id: &str, peer: &str| {
        json!({
            "id": id,
            "healthy": !unhealthy.contains(&id),
            "attributes": { "peerGroup": peer },
        })
    };
    json!([{
        "vendor": "fake", "type": "gpu", "model": "f1",
        "attributes": { "memoryMiB": 16384 },
        "instances": [instance("fake-0", "a"), instance("fake-1", "a"), instance("fake-2", "b")],
    }])
}

fn job(name: &str, count: u32, devices: Value, command: &str) -> JobConfig {
    JobConfig {
        priority: Band::Normal,
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![TaskConfig {
            count: TaskCount::new(count),
            config: json!({ "command": ["sh", "-c", command] }),
            resources: json!({ "memory": 64, "cpu": 100, "devices": devices }),
            ..TaskConfig::new(format!("{name}-task"), "exec")
        }],
        version: None,
        pending_extensions: Default::default(),
        extensions: Default::default(),
    }
}

fn wait_until(timeout: Duration, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    false
}

/// Acceptance (device-model plan §13): a job asking for 2 fake devices
/// places, its allocation records 2 ids, and its `exec` task sees them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_job_asking_for_two_devices_gets_two_and_its_task_sees_them() -> R<()> {
    let mut cluster = Cluster::new()?;
    let (devices, _) = fake_devices(cluster.dir.path(), &inventory(&[]))?;
    let report = devices.fingerprint();
    assert_eq!(report.plugins["fake"].status, "ok", "{report:?}");
    cluster.add_node(
        "gpu-1",
        json!({ "disk": "pass", "devices": report.to_value() }),
    )?;

    cluster.submit(job(
        "train",
        1,
        json!([{ "name": "fake/gpu", "count": 2 }]),
        "echo \"$LEGION_DEVICE_FAKE_GPU_IDS|$FAKE_VISIBLE_DEVICES\" > devices.out",
    ))?;
    // The two that share a peer group.
    assert_eq!(cluster.assigned("train")?, vec![vec!["fake-0", "fake-1"]]);

    // One left: a second two-device job does not fit, a one-device job gets
    // the remaining instance.
    assert!(cluster
        .submit(job(
            "more",
            1,
            json!([{ "name": "gpu", "count": 2 }]),
            "true"
        ))
        .is_err());
    cluster.submit(job(
        "small",
        1,
        json!([{ "name": "gpu", "count": 1 }]),
        "true",
    ))?;
    assert_eq!(cluster.assigned("small")?, vec![vec!["fake-2"]]);

    // The worker half: the dispatched payload carries the assignment, and
    // the task's env carries the ids and the plugin's own env.
    let allocation = cluster
        .store()
        .allocs_for_job("train")?
        .into_iter()
        .next()
        .context("train allocation")?;
    let tasks = cluster
        .store()
        .get_task_configs_for_job_config(&allocation.job_config_id)?;
    let (_, task) = tasks.first().context("train task")?;
    let payload = build_task_allocation(task, &allocation, None, &[])?;
    assert!(
        payload.task_resources.contains("fake-0"),
        "{}",
        payload.task_resources
    );

    let worker_dir = tempfile::Builder::new()
        .prefix("legion-devices-worker-")
        .tempdir_in(crate::support::scratch_root())?;
    let mut manager = WorkerTaskManager::with_state_store(WorkerStateStore::new(
        worker_dir.path().join("worker_state.json"),
    ));
    manager.load_builtin_plugins();
    manager.configure_devices(Arc::new(devices));
    let manager = Arc::new(Mutex::new(manager));
    launch_task_allocation(&manager, &payload)?;
    let out = worker_dir
        .path()
        .join("allocs")
        .join(allocation.id.to_string())
        .join(&task.name)
        .join("devices.out");
    assert!(
        wait_until(Duration::from_secs(15), || std::fs::read_to_string(&out)
            .is_ok_and(|text| text.trim() == "fake-0,fake-1|fake-0,fake-1")),
        "task env: {:?}; health: {:?}",
        std::fs::read_to_string(&out),
        manager.lock().unwrap().allocation_health_snapshot()
    );
    crate::support::kill_task_processes_under(worker_dir.path());
    Ok(())
}

/// An instance the plugin reports unhealthy is never assigned: the report
/// arrives through the worker's `HealthCheck` RPC and the next placement
/// sees it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unhealthy_device_stops_being_schedulable() -> R<()> {
    let mut cluster = Cluster::new()?;
    let (devices, inventory_path) = fake_devices(cluster.dir.path(), &inventory(&[]))?;
    cluster.add_node(
        "gpu-1",
        json!({ "devices": devices.fingerprint().to_value() }),
    )?;

    // fake-0 goes bad; the worker's next pass reports it.
    std::fs::write(&inventory_path, inventory(&["fake-0"]).to_string())?;
    let report = devices.fingerprint();
    cluster
        .report("gpu-1", json!({ "devices": report.to_value() }))
        .await?;
    let node = cluster.store().get_node("gpu-1")?;
    let groups = legion::devices::node_devices(&node);
    assert!(!groups[0].instances[0].healthy, "{groups:?}");

    assert!(
        cluster
            .submit(job(
                "three",
                1,
                json!([{ "name": "gpu", "count": 3 }]),
                "true"
            ))
            .is_err(),
        "only two healthy instances remain"
    );
    cluster.submit(job(
        "two",
        1,
        json!([{ "name": "gpu", "count": 2 }]),
        "true",
    ))?;
    let mut ids = cluster.assigned("two")?.remove(0);
    ids.sort();
    assert_eq!(ids, vec!["fake-1", "fake-2"]);
    Ok(())
}

/// `quota.<band>.devices` caps a namespace's devices, and a request for the
/// generic `gpu` counts against a `fake/gpu` cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_quota_refuses_the_job_that_would_exceed_it() -> R<()> {
    let mut cluster = Cluster::new()?;
    let (devices, _) = fake_devices(cluster.dir.path(), &inventory(&[]))?;
    cluster.add_node(
        "gpu-1",
        json!({ "devices": devices.fingerprint().to_value() }),
    )?;
    let policy = legion::structs::namespace_policy::NamespacePolicy::from_json(
        &json!({
            "name": "gpu-cap",
            "namespaces": ["*"],
            "quota": { "normal": { "devices": { "fake/gpu": 2 } } },
        })
        .to_string(),
    )
    .map_err(|err| anyhow::anyhow!(err))?;
    cluster.store().apply_namespace_policy(policy)?;

    cluster.submit(job(
        "first",
        1,
        json!([{ "name": "fake/gpu", "count": 2 }]),
        "true",
    ))?;
    let err = cluster
        .submit(job("second", 1, json!([{ "name": "gpu" }]), "true"))
        .expect_err("over the device quota");
    let message = format!("{err:#}");
    assert!(
        message.contains("caps band 'normal' at 2 fake/gpu devices"),
        "{message}"
    );
    Ok(())
}
