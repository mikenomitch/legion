//! Burst and oversubscribe on the manager, end to end in process
//! (docs/resource-enforcement.md, "Oversubscription" and "Memory pressure").
//!
//! - A node registered with `oversubscribe.memory = 1.5` admits 1.5x its
//!   memory in reservations, and no more.
//! - A heartbeat reporting a pressure eviction removes that allocation with
//!   `cause = node_pressure`, emits `node.pressure`, and queues its
//!   replacement, while a `reclaim` report stops nothing.
//!
//! The worker half (ordering, the step runner, its triggers) is unit-tested
//! with a fake node in `src/worker/pressure.rs`. What a real kernel does is
//! in `internal_docs/suspend-resume-linux-verification.md`, "Phase 2".

use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::events::SchedulerEventFilter;
use legion::storage::store::Store;
use legion::structs::manager_server::Manager;
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, job::TaskConfig, job::TaskCount, Band, Eval,
    HeartbeatRequest, Node, NodeMsg, PressureAction, StopCause, Submission,
};
use tonic::Request;

const MIB: u64 = 1024 * 1024;

struct Cluster {
    svc: ManagerDaemonService,
    eval_seq: u64,
    _dir: tempfile::TempDir,
}

impl Cluster {
    fn new() -> R<Self> {
        let dir = tempfile::Builder::new()
            .prefix("legion-node-pressure-")
            .tempdir()?;
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.path().to_str().context("store path")?.to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        })?;
        Ok(Self {
            svc: ManagerDaemonService::new(store),
            eval_seq: 0,
            _dir: dir,
        })
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    /// An 8 GiB node, oversubscribed when `ratio` is set.
    fn add_node(&self, id: &str, ratio: Option<f64>) -> R<()> {
        let mut config_attrs = json!({ "maxAllocations": 50 });
        if let Some(ratio) = ratio {
            config_attrs["oversubscribe"] = json!({ "memory": ratio, "cpu": 1.0 });
        }
        self.store().insert_node(NodeMsg::from(Node {
            id: id.to_string(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2026-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs,
            fingerprint_attrs: json!({
                "memory_total": (8192 * MIB).to_string(),
                "cpus": {
                    "0": { "frequency": "3000" },
                    "1": { "frequency": "3000" },
                    "2": { "frequency": "3000" },
                    "3": { "frequency": "3000" },
                },
            }),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }))?;
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
            id: format!("pressure-test-{}", self.eval_seq),
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

    fn allocations(&self, job: &str) -> R<Vec<legion::structs::Allocation>> {
        self.store().allocs_for_job(job)
    }
}

fn job(name: &str, band: Band, count: u32, resources: serde_json::Value) -> JobConfig {
    JobConfig {
        priority: band,
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![TaskConfig {
            count: TaskCount::new(count),
            config: json!({ "message": "hi" }),
            resources,
            ..TaskConfig::new(format!("{name}-task"), "service")
        }],
        version: None,
    }
}

/// Acceptance (plan §13, Phase 2): a node with `oversubscribe.memory = 1.5`
/// admits 1.5x its memory in reservations. 8 GiB x 1.5 = 12 GiB, so three
/// 4 GiB tasks fit and nothing more does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_oversubscribed_node_admits_its_ratio_in_reservations() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node("w-packed", Some(1.5))?;
    cluster.submit(job(
        "fill",
        Band::Normal,
        3,
        json!({ "memory": 4096, "memoryMax": 6144, "cpu": 100 }),
    ))?;
    assert_eq!(cluster.allocations("fill")?.len(), 3);
    // Full: the next MiB does not fit.
    assert!(cluster
        .submit(job("one-more", Band::Normal, 1, json!({ "memory": 64 })))
        .is_err());

    // The same node at ratio 1.0 fits only two.
    let mut plain = Cluster::new()?;
    plain.add_node("w-plain", None)?;
    plain.submit(job(
        "fill",
        Band::Normal,
        2,
        json!({ "memory": 4096, "cpu": 100 }),
    ))?;
    assert!(plain
        .submit(job("one-more", Band::Normal, 1, json!({ "memory": 4096 })))
        .is_err());
    // `memoryMax` is a ceiling, not a dimension: a task whose ceiling is far
    // larger than any node still places on its reservation.
    let mut fresh = Cluster::new()?;
    fresh.add_node("w-fresh", None)?;
    fresh.submit(job(
        "bursty",
        Band::Normal,
        1,
        json!({ "memory": 64, "memoryMax": 1_000_000, "cpu": 100 }),
    ))?;
    assert_eq!(fresh.allocations("bursty")?.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_max_below_memory_is_rejected_at_submit() -> R<()> {
    let cluster = Cluster::new()?;
    let err = cluster
        .store()
        .handle_job_submit(
            &job(
                "inverted",
                Band::Normal,
                1,
                json!({ "memory": 512, "memoryMax": 256 }),
            ),
            &[],
        )
        .expect_err("memoryMax below memory");
    assert!(
        format!("{err:#}").contains("memoryMax (256 MiB) is below resources.memory (512 MiB)"),
        "{err:#}"
    );
    Ok(())
}

fn evict(allocation_id: i64, job_id: &str, step: &str) -> PressureAction {
    PressureAction {
        allocation_id,
        job_id: job_id.into(),
        step: step.into(),
        freed_bytes: 1900 * MIB,
        used_bytes: 1900 * MIB,
        reserved_bytes: 400 * MIB,
        available_bytes: 1100 * MIB,
        min_free_bytes: 2048 * MIB,
        detail: format!(
            "evicted by {step} on w-03: using 1.9 GiB of a 400 MiB reservation while free \
             memory was 1.1 GiB (threshold 2 GiB)"
        ),
        hint: "raise resources.memory toward real usage".into(),
        ..Default::default()
    }
}

/// Acceptance (plan §13, Phase 2), manager half: the lower-band bursting
/// allocation the worker evicted is removed with `cause = node_pressure`
/// and a replacement is queued, and the higher band is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reported_eviction_removes_the_allocation_with_node_pressure() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node("w-03", None)?;
    cluster.submit(job(
        "batch",
        Band::Low,
        1,
        json!({ "memory": 400, "memoryMax": 2048, "cpu": 100 }),
    ))?;
    cluster.submit(job(
        "api",
        Band::High,
        1,
        json!({ "memory": 400, "memoryMax": 2048, "cpu": 100 }),
    ))?;
    let batch = cluster.allocations("batch")?[0].clone();
    let api = cluster.allocations("api")?[0].clone();
    // Drop the submit evals so the queue holds only what the heartbeat adds.
    while cluster.store().get_next_eval()?.is_some() {}

    // A reclaim report stops nothing.
    cluster
        .svc
        .heartbeat(Request::new(HeartbeatRequest {
            id: "w-03".into(),
            pressure_actions: vec![PressureAction {
                step: "reclaim".into(),
                ..evict(batch.id, "batch", "reclaim")
            }],
            ..Default::default()
        }))
        .await?;
    assert_eq!(cluster.allocations("batch")?.len(), 1);

    // The eviction, then a re-send of the same report (a heartbeat retry):
    // the second is harmless.
    for _ in 0..2 {
        let reply = cluster
            .svc
            .heartbeat(Request::new(HeartbeatRequest {
                id: "w-03".into(),
                pressure_actions: vec![evict(batch.id, "batch", "evict-bursting")],
                ..Default::default()
            }))
            .await?
            .into_inner();
        assert!(
            reply
                .desired_allocations
                .iter()
                .all(|desired| desired.allocation_id != batch.id),
            "the evicted allocation is not handed back to the worker"
        );
        let api_payload = reply
            .desired_allocations
            .iter()
            .find(|desired| desired.allocation_id == api.id)
            .context("api is still desired")?;
        assert_eq!(api_payload.priority_band, "high");
    }

    assert!(cluster
        .allocations("batch")?
        .iter()
        .all(|allocation| allocation.id != batch.id));
    assert_eq!(
        cluster.allocations("api")?.len(),
        1,
        "the high band survives"
    );

    let notices = cluster.store().stop_notices_for_node("w-03")?;
    let notice = notices
        .iter()
        .find(|notice| notice.allocation_id == batch.id)
        .context("a stop notice for the evicted allocation")?;
    assert_eq!(notice.cause, StopCause::NodePressure);

    let eval = cluster
        .store()
        .get_next_eval()?
        .context("a replacement eval was queued")?;
    assert_eq!(eval.kind, "job_reschedule");
    assert_eq!(eval.job_id, "batch");

    let events = cluster
        .store()
        .list_scheduler_events(&SchedulerEventFilter {
            node_id: Some("w-03".into()),
            ..Default::default()
        })?;
    let pressure: Vec<_> = events
        .iter()
        .filter(|event| event.kind == "node.pressure")
        .collect();
    assert_eq!(pressure.len(), 2, "one reclaim, one eviction: {pressure:?}");
    let eviction = pressure
        .iter()
        .find(|event| event.level == "warn")
        .context("the eviction is a warning")?;
    assert!(
        eviction
            .message
            .contains("evicted by evict-bursting on w-03"),
        "{}",
        eviction.message
    );
    Ok(())
}

/// A node cannot evict another node's allocation by reporting it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_report_for_another_nodes_allocation_is_ignored() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node("w-a", None)?;
    cluster.submit(job("svc", Band::Low, 1, json!({ "memory": 256 })))?;
    let allocation = cluster.allocations("svc")?[0].clone();
    cluster.add_node("w-b", None)?;
    cluster
        .svc
        .heartbeat(Request::new(HeartbeatRequest {
            id: "w-b".into(),
            pressure_actions: vec![evict(allocation.id, "svc", "evict-bursting")],
            ..Default::default()
        }))
        .await?;
    assert_eq!(cluster.allocations("svc")?.len(), 1);
    Ok(())
}

/// A sandbox that declares no memory reserves and boots the runtime's own
/// 512 MiB, even when the cluster default is smaller, and the guest size is
/// pinned into the stored config so it never changes under the job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sandbox_without_memory_keeps_its_512_mib_guest() -> R<()> {
    let dir = tempfile::Builder::new()
        .prefix("legion-sandbox-default-")
        .tempdir()?;
    let store = Store::start_storage(&ManagerConfigFull {
        store_path: dir.path().to_str().context("store path")?.to_string(),
        task_resource_defaults: Some(json!({ "memory": 128, "cpu": 100 })),
        ..Default::default()
    })?;
    let mut sandbox = job("box", Band::Normal, 1, json!({}));
    sandbox.tasks[0].runtime = "sandbox".into();
    sandbox.tasks[0].config = json!({ "image": "alpine", "command": ["true"] });
    let mut service = job("svc", Band::Normal, 1, json!({}));
    service.tasks[0].config = json!({ "message": "hi" });
    for spec in [&sandbox, &service] {
        store.handle_job_submit(
            spec,
            &[Submission {
                path: format!("{}.json", spec.name),
                content: spec.name.clone(),
                is_primary: true,
            }],
        )?;
    }

    let stored = store.get_job_config("box")?;
    let task = &stored.tasks[0];
    assert_eq!(task.resources["memory"], json!(512));
    assert_eq!(task.config["memory_mib"], json!(512));

    // Other runtimes still take the cluster default.
    let stored = store.get_job_config("svc")?;
    assert_eq!(stored.tasks[0].resources["memory"], json!(128));
    Ok(())
}
