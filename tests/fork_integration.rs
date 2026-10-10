//! Fork and create-from-snapshot end to end on the manager (Phase 6;
//! docs/suspend-and-resume.md, "Fork"): ForkInstance, the waiting forks the
//! capture report releases, their placement as new instances restoring from
//! the origin's checkpoint, idempotency, the loud refusals, named snapshots,
//! and `from_snapshot` creates.
//!
//! The heartbeat stands in for the worker, as in `suspend_integration.rs`.
//! The worker half (clone the volume per fork, divergent identities, the
//! per-fork TAP) is tested in `src/worker/task_manager.rs`
//! (`an_exec_disk_fork_gives_every_fork_its_own_copy_of_the_volume`),
//! `src/worker/identity.rs`, and `src/task_plugins/sandbox_checkpoint.rs`.

use std::collections::HashSet;

use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::events::SchedulerEventFilter;
use legion::storage::store::Store;
use legion::structs::manager_server::Manager;
use legion::structs::suspend::SuspendConfig;
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, job::StatefulConfig, job::StatefulStorageConfig,
    job::TaskConfig, CheckpointRequest, CreateInstancesRequest, CreateSnapshotRequest,
    DestroyInstanceRequest, ForkInstanceReply, ForkInstanceRequest, GetInstanceRequest,
    HeartbeatReply, HeartbeatRequest, InstanceInfo, InstanceMode, Node, NodeMsg,
    SetSnapshotAliasRequest, SnapshotReport, Submission, TaskAllocation,
};
use serde_json::Value;
use tonic::{Code, Request};

const NODE: &str = "fork-node";
const JOB: &str = "agents";
const TASK: &str = "workspace";
const ORIGIN: &str = "run-17";

/// Every op a snapshot-and-clone-capable `local-dir` advertises.
const ALL_OPS: &[&str] = &["snapshot", "restore", "clone", "snapshot-delete"];

struct Cluster {
    svc: ManagerDaemonService,
}

impl Cluster {
    fn new() -> R<Self> {
        let dir = tempfile::Builder::new()
            .prefix("legion-fork-it-")
            .tempdir()?
            .keep();
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.to_str().context("store path")?.to_string(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        })?;
        Ok(Cluster {
            svc: ManagerDaemonService::new(store),
        })
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    fn add_node(&self, ops: &[&str], vmm_version: &str) -> R<()> {
        self.add_node_as(NODE, ops, vmm_version, &["checkpoint"])
    }

    fn add_node_as(
        &self,
        id: &str,
        ops: &[&str],
        vmm_version: &str,
        capabilities: &[&str],
    ) -> R<()> {
        self.store().insert_node(NodeMsg::from(Node {
            id: id.to_string(),
            pool: "default".into(),
            status: "ready".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({
                "node_id": id,
                "maxAllocations": 20,
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
                "sandbox.vmm.version": vmm_version,
                "volumeDrivers": {
                    "local-dir": {
                        "apiVersion": 1, "name": "local-dir", "nodeLocal": true,
                        "ephemeral": false, "accessModes": ["single-writer"], "available": true,
                        "ops": ops
                    }
                }
            }),
            dynamic_attrs: json!({ "buildVersion": "0.9.0", "capabilities": capabilities }),
            health_checks: json!({}),
        }))?;
        Ok(())
    }

    fn submit(&self, config: JobConfig) -> R<()> {
        self.store().handle_job_submit(
            &config,
            &[Submission {
                path: format!("{}.json", config.name),
                content: "{}".to_string(),
                is_primary: true,
            }],
        )?;
        Ok(())
    }

    fn reschedule(&self) -> R<()> {
        let config_id = self.store().get_job_config(JOB)?.id();
        self.store()
            .enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
        while let Some(eval) = self.store().get_next_eval()? {
            let outcome = eval.handle(self.store());
            self.store().set_eval_status(
                &eval.id,
                if outcome.is_ok() {
                    "complete"
                } else {
                    "failed"
                },
            )?;
        }
        Ok(())
    }

    async fn heartbeat(&self, reports: Vec<SnapshotReport>) -> R<HeartbeatReply> {
        self.heartbeat_as(NODE, reports).await
    }

    async fn heartbeat_as(&self, node: &str, reports: Vec<SnapshotReport>) -> R<HeartbeatReply> {
        Ok(self
            .svc
            .heartbeat(Request::new(HeartbeatRequest {
                id: node.into(),
                snapshot_reports: reports,
                ..Default::default()
            }))
            .await?
            .into_inner())
    }

    /// Create the origin and place it. Returns its allocation id.
    fn active_origin(&self) -> R<i64> {
        self.store()
            .create_keyed_instance(JOB, "default", TASK, Some(ORIGIN))?;
        self.reschedule()?;
        self.alloc_of(ORIGIN)?
            .context("the origin should be placed")
    }

    fn alloc_of(&self, instance_id: &str) -> R<Option<i64>> {
        Ok(self
            .store()
            .get_instance(instance_id)?
            .context("instance")?
            .current_alloc_id)
    }

    async fn fork(
        &self,
        count: u32,
        key: Option<&str>,
    ) -> Result<ForkInstanceReply, tonic::Status> {
        self.fork_with(ForkInstanceRequest {
            instance_id: ORIGIN.into(),
            count,
            idempotency_key: key.map(str::to_string),
            ..Default::default()
        })
        .await
    }

    async fn fork_with(
        &self,
        request: ForkInstanceRequest,
    ) -> Result<ForkInstanceReply, tonic::Status> {
        self.svc
            .fork_instance(Request::new(request))
            .await
            .map(|reply| reply.into_inner())
    }

    async fn info(&self, instance_id: &str) -> R<InstanceInfo> {
        self.svc
            .get_instance(Request::new(GetInstanceRequest {
                instance_id: instance_id.into(),
            }))
            .await?
            .into_inner()
            .instance
            .context("instance")
    }

    fn events(&self, kind: &str) -> R<Vec<Value>> {
        Ok(self
            .store()
            .list_scheduler_events(&SchedulerEventFilter {
                kind: Some(kind.to_string()),
                limit: Some(1000),
                ..Default::default()
            })?
            .into_iter()
            .map(|event| serde_json::from_str(&event.metadata_json).unwrap_or(Value::Null))
            .collect())
    }

    /// Allocations the node is told to run, by allocation id.
    async fn desired(&self) -> R<Vec<TaskAllocation>> {
        Ok(self.heartbeat(Vec::new()).await?.desired_allocations)
    }
}

fn task(runtime: &str, storage: bool) -> TaskConfig {
    TaskConfig {
        config: if runtime == "sandbox" {
            json!({ "image": "alpine", "command": ["sleep", "60"] })
        } else {
            json!({ "command": ["sleep", "60"] })
        },
        stateful: Some(StatefulConfig {
            mode: InstanceMode::Keyed,
            id_source: None,
            initial_instances: None,
            on_lost: None,
            storage: storage.then(|| StatefulStorageConfig {
                path: "/workspace".into(),
                driver: Some("local-dir".into()),
                mode: None,
                params: None,
                retain_on_scale_in: None,
                retain_on_stop: None,
            }),
        }),
        ..TaskConfig::new(TASK, runtime)
    }
}

fn job(task: TaskConfig) -> JobConfig {
    JobConfig {
        priority: Default::default(),
        name: JOB.to_string(),
        tasks: vec![task],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    }
}

fn manifest(snapshot_id: &str, allocation_id: i64) -> String {
    json!({
        "apiVersion": 1,
        "id": snapshot_id,
        "jobId": JOB,
        "task": TASK,
        "originAllocId": allocation_id,
        "runtime": "exec",
        "kind": "disk",
        "createdAt": "2026-10-10T00:00:00Z",
        "volumes": [{ "id": snapshot_id, "volumeId": "vol-origin", "driver": "local-dir",
                      "mode": "filesystem" }],
    })
    .to_string()
}

fn capture_ok(snapshot_id: &str, allocation_id: i64, pause_ms: u64) -> SnapshotReport {
    SnapshotReport {
        snapshot_id: snapshot_id.into(),
        op: "capture".into(),
        ok: true,
        manifest_json: manifest(snapshot_id, allocation_id),
        duration_ms: 610,
        total_bytes: 4096,
        allocation_id,
        pause_ms,
        ..Default::default()
    }
}

fn restore_report(snapshot_id: &str, allocation_id: i64, ok: bool) -> SnapshotReport {
    SnapshotReport {
        snapshot_id: snapshot_id.into(),
        op: "restore".into(),
        ok,
        error: if ok {
            String::new()
        } else {
            "failed to clone volume `data` (local-dir) from snapshot: disk full".into()
        },
        duration_ms: 90,
        allocation_id,
        ..Default::default()
    }
}

fn requests_for(reply: &HeartbeatReply, snapshot_id: &str) -> Vec<CheckpointRequest> {
    reply
        .checkpoint_requests
        .iter()
        .filter(|request| request.snapshot_id == snapshot_id)
        .cloned()
        .collect()
}

/// The acceptance check (plan §13, Phase 6), manager half: three ids in one
/// envelope, the origin keeps running, forks wait for the checkpoint, then
/// run as distinct new instances with their own allocations and volumes,
/// each restoring from the checkpoint as a fork.
#[tokio::test(flavor = "multi_thread")]
async fn three_forks_wait_for_the_checkpoint_then_run_as_new_instances() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(ALL_OPS, "1.12.0")?;
    cluster.submit(job(task("exec", true)))?;
    let origin_alloc = cluster.active_origin()?;
    let origin_storage = cluster.info(ORIGIN).await?.storage_handle;

    let reply = cluster.fork(3, Some("fork-7f2")).await?;
    assert_eq!(reply.origin, ORIGIN);
    assert_eq!(reply.instances, vec!["run-17-f1", "run-17-f2", "run-17-f3"]);
    assert_eq!(reply.kind, "disk", "exec forks through its volume");
    assert_eq!(reply.node_id, NODE);
    assert_eq!(reply.pause_ms, 0, "nothing has been captured yet");
    assert!(!reply.deduplicated);

    // The forks exist, but wait: nothing is placed against a checkpoint
    // that has not landed.
    for id in &reply.instances {
        let info = cluster.info(id).await?;
        assert_eq!(info.state, "pending");
        assert_eq!(info.pending_op.as_deref(), Some("forking"));
        assert_eq!(
            info.restore_snapshot_id.as_deref(),
            Some(reply.snapshot.as_str())
        );
        // Lineage outlives the fork checkpoint (InstanceInfo.forked_from).
        assert_eq!(info.forked_from.as_deref(), Some(ORIGIN));
    }
    assert!(cluster.info(ORIGIN).await?.forked_from.is_none());
    cluster.reschedule()?;
    assert_eq!(
        cluster.store().allocs_for_node(NODE)?.len(),
        1,
        "only the origin runs until the checkpoint lands"
    );

    // The node is asked for a live capture of the origin.
    let asked = requests_for(&cluster.heartbeat(Vec::new()).await?, &reply.snapshot);
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].op, "capture");
    assert_eq!(asked[0].then, "resume", "the origin keeps running");
    assert_eq!(asked[0].purpose, "fork");
    assert_eq!(asked[0].allocation_id, origin_alloc);

    cluster
        .heartbeat(vec![capture_ok(&reply.snapshot, origin_alloc, 0)])
        .await?;
    let forked = cluster.events("instance.forked")?;
    assert_eq!(forked.len(), 1);
    assert_eq!(forked[0]["origin"], ORIGIN);
    assert_eq!(forked[0]["instances"], json!(reply.instances));

    // The capture's placement eval runs: every fork gets an allocation on
    // the checkpoint's node.
    cluster.reschedule()?;
    let origin = cluster.info(ORIGIN).await?;
    assert_eq!(origin.state, "active");
    assert_eq!(
        origin.current_alloc_id,
        Some(origin_alloc),
        "the origin keeps its allocation"
    );
    let mut allocs = HashSet::from([origin_alloc]);
    let mut storage = HashSet::from([origin_storage.clone()]);
    for id in &reply.instances {
        let info = cluster.info(id).await?;
        assert_eq!(info.state, "active", "{id}");
        assert!(info.pending_op.is_none());
        let alloc = info.current_alloc_id.context("fork placed")?;
        assert!(allocs.insert(alloc), "fork {id} has its own allocation");
        assert!(info.storage_handle.is_some(), "fork {id} has a volume");
        assert!(
            storage.insert(info.storage_handle.clone()),
            "fork {id} has its own volume"
        );
    }

    let desired = cluster.desired().await?;
    for id in &reply.instances {
        let alloc = cluster.alloc_of(id)?.context("alloc")?;
        let payload = desired
            .iter()
            .find(|allocation| allocation.allocation_id == alloc)
            .context("the fork is dispatched")?;
        assert_eq!(payload.node_id, NODE, "forks run on the origin's node");
        let restore = payload.restore.clone().context("restore payload")?;
        assert_eq!(restore.snapshot_id, reply.snapshot);
        assert!(
            restore.fork,
            "a fork clones its volume and gets its own NIC"
        );
        assert_eq!(&restore.instance_id, id);
    }
    let origin_payload = desired
        .iter()
        .find(|allocation| allocation.allocation_id == origin_alloc)
        .context("origin dispatched")?;
    assert!(origin_payload.restore.is_none());

    // Each fork reports its restore; the spent checkpoint is reclaimed once
    // the last one has.
    let ids = reply.instances.clone();
    for (index, id) in ids.iter().enumerate() {
        let alloc = cluster.alloc_of(id)?.context("alloc")?;
        cluster
            .heartbeat(vec![restore_report(&reply.snapshot, alloc, true)])
            .await?;
        let info = cluster.info(id).await?;
        assert!(info.restore_snapshot_id.is_none());
        assert_eq!(
            info.restored_from_snapshot_id.as_deref(),
            Some(reply.snapshot.as_str())
        );
        let snapshot = cluster
            .store()
            .get_snapshot(&reply.snapshot)?
            .context("snapshot")?;
        let last = index == ids.len() - 1;
        assert_eq!(
            snapshot.state.as_str(),
            if last { "deleting" } else { "local" },
            "the checkpoint lives until every fork has restored"
        );
    }
    Ok(())
}

/// The same key twice yields one set of forks (plan §9, Phase 6).
#[tokio::test(flavor = "multi_thread")]
async fn a_retried_fork_with_one_key_creates_one_set_of_forks() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(ALL_OPS, "1.12.0")?;
    cluster.submit(job(task("exec", true)))?;
    let origin_alloc = cluster.active_origin()?;

    let first = cluster.fork(3, Some("fork-key")).await?;
    cluster
        .heartbeat(vec![capture_ok(&first.snapshot, origin_alloc, 0)])
        .await?;
    let again = cluster.fork(3, Some("fork-key")).await?;
    assert!(again.deduplicated);
    assert_eq!(again.instances, first.instances);
    assert_eq!(again.snapshot, first.snapshot);

    let instances = cluster.store().list_instances(JOB)?;
    assert_eq!(instances.len(), 4, "the origin and three forks, no more");

    // Without a key, a second fork is a second set, with the next free ids.
    let second = cluster.fork(2, None).await?;
    assert_eq!(second.instances, vec!["run-17-f4", "run-17-f5"]);
    Ok(())
}

/// A fork id is never recycled: a destroyed fork's id is skipped.
#[tokio::test(flavor = "multi_thread")]
async fn fork_ids_skip_every_id_that_ever_existed() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(ALL_OPS, "1.12.0")?;
    cluster.submit(job(task("exec", true)))?;
    let origin_alloc = cluster.active_origin()?;
    let first = cluster.fork(1, None).await?;
    cluster
        .heartbeat(vec![capture_ok(&first.snapshot, origin_alloc, 0)])
        .await?;
    cluster
        .svc
        .destroy_instance(Request::new(DestroyInstanceRequest {
            instance_id: "run-17-f1".into(),
            idempotency_key: None,
        }))
        .await?;
    // The spent checkpoint has no forks left to restore.
    let snapshot = cluster.store().get_snapshot(&first.snapshot)?;
    cluster.store().spend_unused_fork_snapshots()?;
    assert!(snapshot.is_some());
    assert_eq!(
        cluster
            .store()
            .get_snapshot(&first.snapshot)?
            .map(|snapshot| snapshot.state.as_str().to_string())
            .as_deref(),
        Some("deleting"),
        "an unreferenced fork checkpoint is reclaimed"
    );
    let second = cluster.fork(1, None).await?;
    assert_eq!(second.instances, vec!["run-17-f2"]);
    Ok(())
}

/// A capture failure leaves the origin running and terminates the forks
/// with the reason, never a fork booted from nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_fork_capture_keeps_the_origin_and_terminates_the_forks() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(ALL_OPS, "1.12.0")?;
    cluster.submit(job(task("exec", true)))?;
    let origin_alloc = cluster.active_origin()?;
    let reply = cluster.fork(2, None).await?;
    cluster
        .heartbeat(vec![SnapshotReport {
            snapshot_id: reply.snapshot.clone(),
            op: "capture".into(),
            ok: false,
            error: "failed to snapshot volume `data` (local-dir): no space left".into(),
            allocation_id: origin_alloc,
            ..Default::default()
        }])
        .await?;
    let origin = cluster.info(ORIGIN).await?;
    assert_eq!(origin.state, "active");
    assert_eq!(origin.current_alloc_id, Some(origin_alloc));
    for id in &reply.instances {
        let info = cluster.info(id).await?;
        assert_eq!(info.state, "terminated");
        assert!(
            info.detail
                .as_deref()
                .unwrap_or_default()
                .contains("no space left"),
            "{info:?}"
        );
    }
    let failed = cluster.events("instance.fork_failed")?;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["instances"], json!(reply.instances));
    cluster.reschedule()?;
    assert_eq!(cluster.store().allocs_for_node(NODE)?.len(), 1);
    Ok(())
}

/// Unsupported combinations fail loudly, with the fix in the message.
#[tokio::test(flavor = "multi_thread")]
async fn unsupported_forks_are_refused_with_a_hint() -> R<()> {
    // No snapshot-capable volume: an exec task has nothing to fork.
    let cluster = Cluster::new()?;
    cluster.add_node(ALL_OPS, "1.12.0")?;
    cluster.submit(job(task("exec", false)))?;
    cluster.active_origin()?;
    let status = cluster.fork(1, None).await.expect_err("no volume");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().contains("no stateful.storage volume"),
        "{status:?}"
    );
    let status = cluster
        .fork_with(ForkInstanceRequest {
            instance_id: ORIGIN.into(),
            checkpoint: Some("memory".into()),
            ..Default::default()
        })
        .await
        .expect_err("exec cannot take memory");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains("--checkpoint disk"), "{status:?}");

    // A driver that cannot clone.
    let cluster = Cluster::new()?;
    cluster.add_node(&["snapshot", "restore", "snapshot-delete"], "1.12.0")?;
    cluster.submit(job(task("exec", true)))?;
    cluster.active_origin()?;
    let status = cluster.fork(1, None).await.expect_err("no clone op");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains("`clone`"), "{status:?}");

    // Over the cap, and --spread of an instance whose volume would have to
    // be cloned on the origin's node.
    let cluster = Cluster::new()?;
    cluster.add_node(ALL_OPS, "1.12.0")?;
    cluster.submit(job(task("exec", true)))?;
    cluster.active_origin()?;
    let status = cluster.fork(65, None).await.expect_err("over the cap");
    assert_eq!(status.code(), Code::InvalidArgument);
    let status = cluster
        .fork_with(ForkInstanceRequest {
            instance_id: ORIGIN.into(),
            spread: true,
            ..Default::default()
        })
        .await
        .expect_err("a spread fork cannot carry a node-local volume");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().contains("Hint: drop --spread"),
        "{status:?}"
    );
    assert_eq!(
        cluster.store().list_instances(JOB)?.len(),
        1,
        "a refused fork creates nothing"
    );

    // A memory fork of a TAP-wired sandbox on a Firecracker that cannot move
    // a restored NIC.
    let cluster = Cluster::new()?;
    cluster.add_node(ALL_OPS, "1.7.0")?;
    let mut sandbox = task("sandbox", true);
    sandbox.config = json!({
        "image": "alpine",
        "command": ["sleep", "60"],
        "hypervisor": "firecracker",
        "network": { "mode": "tap", "tap": "tap0" },
    });
    sandbox.snapshots = Some(Default::default());
    cluster.submit(job(sandbox))?;
    cluster.active_origin()?;
    let status = cluster
        .fork_with(ForkInstanceRequest {
            instance_id: ORIGIN.into(),
            checkpoint: Some("memory".into()),
            ..Default::default()
        })
        .await
        .expect_err("old Firecracker with a TAP");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains("Firecracker 1.7.0"), "{status:?}");
    assert!(status.message().contains("1.12"), "{status:?}");
    Ok(())
}

/// Named snapshots and `from_snapshot` creates: a golden snapshot seeds new
/// instances by name, the name moves to a newer snapshot, and checkpoints
/// owned by an instance cannot be named.
#[tokio::test(flavor = "multi_thread")]
async fn a_named_snapshot_seeds_new_instances_and_the_name_moves() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(ALL_OPS, "1.12.0")?;
    cluster.submit(job(task("exec", true)))?;
    let origin_alloc = cluster.active_origin()?;

    let snapshot = |name: Option<&str>| CreateSnapshotRequest {
        instance_id: ORIGIN.into(),
        kind: "disk".into(),
        name: name.map(str::to_string),
        idempotency_key: None,
        publish: false,
    };
    let golden = cluster
        .svc
        .create_snapshot(Request::new(snapshot(Some("agent-golden"))))
        .await?
        .into_inner()
        .snapshot
        .context("snapshot")?;
    cluster
        .heartbeat(vec![capture_ok(&golden.snapshot_id, origin_alloc, 0)])
        .await?;

    let created = cluster
        .svc
        .create_instances(Request::new(CreateInstancesRequest {
            job_id: JOB.into(),
            count: 2,
            from_snapshot: Some("agent-golden".into()),
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert_eq!(created.restoring_from, golden.snapshot_id);
    assert_eq!(created.instance_ids.len(), 2);
    cluster.reschedule()?;
    let desired = cluster.desired().await?;
    for id in &created.instance_ids {
        let alloc = cluster.alloc_of(id)?.context("placed")?;
        let restore = desired
            .iter()
            .find(|allocation| allocation.allocation_id == alloc)
            .and_then(|allocation| allocation.restore.clone())
            .context("restore payload")?;
        assert!(restore.fork);
        assert_eq!(restore.snapshot_id, golden.snapshot_id);
    }

    // A newer snapshot; the name moves to it, the old one is untouched.
    let newer = cluster
        .svc
        .create_snapshot(Request::new(snapshot(None)))
        .await?
        .into_inner()
        .snapshot
        .context("snapshot")?;
    cluster
        .heartbeat(vec![capture_ok(&newer.snapshot_id, origin_alloc, 0)])
        .await?;
    let moved = cluster
        .svc
        .set_snapshot_alias(Request::new(SetSnapshotAliasRequest {
            job_id: JOB.into(),
            name: "agent-golden".into(),
            snapshot: newer.snapshot_id.clone(),
            idempotency_key: None,
        }))
        .await?
        .into_inner();
    assert_eq!(moved.previous_snapshot_id, golden.snapshot_id);
    assert!(moved.message.starts_with("Moved agent-golden"));
    let old = cluster
        .store()
        .get_snapshot(&golden.snapshot_id)?
        .context("old snapshot")?;
    assert_eq!(old.state.as_str(), "local", "repointing leaves it alone");
    assert!(old.name.is_none());
    let resolved = cluster
        .store()
        .resolve_snapshot("agent-golden", Some(JOB))?
        .context("name resolves")?;
    assert_eq!(resolved.snapshot_id, newer.snapshot_id);
    assert_eq!(
        cluster
            .store()
            .snapshot_aliases(std::slice::from_ref(&newer.snapshot_id))?
            .get(&newer.snapshot_id)
            .cloned(),
        Some(vec!["agent-golden".to_string()])
    );

    // A fork checkpoint is not a seed.
    let fork = cluster.fork(1, None).await?;
    cluster
        .heartbeat(vec![capture_ok(&fork.snapshot, origin_alloc, 0)])
        .await?;
    let status = cluster
        .svc
        .set_snapshot_alias(Request::new(SetSnapshotAliasRequest {
            job_id: JOB.into(),
            name: "nope".into(),
            snapshot: fork.snapshot.clone(),
            idempotency_key: None,
        }))
        .await
        .expect_err("a fork checkpoint cannot be named");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains("fork checkpoint"), "{status:?}");
    let status = cluster
        .svc
        .create_instances(Request::new(CreateInstancesRequest {
            job_id: JOB.into(),
            count: 65,
            from_snapshot: Some("agent-golden".into()),
            ..Default::default()
        }))
        .await
        .expect_err("over the cap");
    assert_eq!(status.code(), Code::InvalidArgument);
    Ok(())
}

/// A restore into a new instance that fails follows `onRestoreFailure`:
/// `fail` stops the instance with the reason instead of booting it on a
/// volume that may be empty.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_fork_restore_follows_on_restore_failure() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(ALL_OPS, "1.12.0")?;
    let mut fail = task("exec", true);
    fail.suspend = Some(SuspendConfig {
        on_restore_failure: Some("fail".into()),
        ..Default::default()
    });
    cluster.submit(job(fail))?;
    let origin_alloc = cluster.active_origin()?;
    let reply = cluster.fork(1, None).await?;
    cluster
        .heartbeat(vec![capture_ok(&reply.snapshot, origin_alloc, 0)])
        .await?;
    cluster.reschedule()?;
    let fork_id = &reply.instances[0];
    let alloc = cluster.alloc_of(fork_id)?.context("placed")?;
    cluster
        .heartbeat(vec![restore_report(&reply.snapshot, alloc, false)])
        .await?;
    let info = cluster.info(fork_id).await?;
    assert_eq!(info.state, "stopped");
    assert!(info.current_alloc_id.is_none());
    assert!(
        info.detail
            .as_deref()
            .unwrap_or_default()
            .contains("onRestoreFailure = \"fail\""),
        "{info:?}"
    );
    let failed = cluster.events("instance.restore_failed")?;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["outcome"], "stopped");
    // Nothing restores from the checkpoint any more: it is reclaimed.
    let snapshot = cluster
        .store()
        .get_snapshot(&reply.snapshot)?
        .context("snapshot")?;
    assert_eq!(snapshot.state.as_str(), "deleting");
    Ok(())
}

/// Every capability a worker with a `contentStore` advertises.
const PUBLISHING: &[&str] = &["checkpoint", "checkpoint-fetch", "content-publish"];
/// A worker without a `contentStore`.
const FETCHING: &[&str] = &["checkpoint", "checkpoint-fetch"];

/// A sandbox launched for memory checkpoints, with no instance volume: all
/// of an instance is in its memory checkpoint, so it can be spread.
fn memory_sandbox() -> TaskConfig {
    let mut sandbox = task("sandbox", false);
    sandbox.config = json!({
        "image": "alpine",
        "command": ["sleep", "60"],
        "hypervisor": "firecracker",
    });
    sandbox.snapshots = Some(Default::default());
    sandbox
}

/// A memory manifest the fork nodes' fingerprints all pass, with `url` once
/// published.
fn memory_manifest(snapshot_id: &str, allocation_id: i64, url: Option<&str>) -> String {
    json!({
        "apiVersion": 1,
        "id": snapshot_id,
        "jobId": JOB,
        "task": TASK,
        "originAllocId": allocation_id,
        "runtime": "sandbox",
        "kind": "memory",
        "createdAt": "2026-10-10T00:00:00Z",
        "memory": [{
            "role": "memory", "file": "memory", "size": 64 * 1024 * 1024,
            "digest": format!("sha256:{}", "cd".repeat(32)), "url": url,
        }],
        "compat": {
            "vmm": { "type": "firecracker", "version": "1.12.0" },
            "cpu": { "vendor": "GenuineIntel", "model": "6:106:6", "features": "3f9a0c21" },
        },
    })
    .to_string()
}

fn node_of(cluster: &Cluster, instance_id: &str) -> R<Option<String>> {
    let Some(allocation_id) = cluster.alloc_of(instance_id)? else {
        return Ok(None);
    };
    Ok(cluster
        .store()
        .get_allocation_by_id(allocation_id)?
        .map(|allocation| allocation.node_id))
}

/// `--spread`: the fork checkpoint is published, the forks wait for the
/// publish rather than the capture, and each lands on a node no sibling
/// runs on. A fork with no such node left waits without failing the rest.
/// Once every fork restored, the published checkpoint is spent like a local
/// one (the node's copy is reclaimed).
#[tokio::test(flavor = "multi_thread")]
async fn a_spread_fork_publishes_then_places_each_fork_on_its_own_node() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node_as(NODE, ALL_OPS, "1.12.0", PUBLISHING)?;
    cluster.submit(job(memory_sandbox()))?;
    let origin_alloc = cluster.active_origin()?;
    cluster.add_node_as("node-b", ALL_OPS, "1.12.0", FETCHING)?;
    cluster.add_node_as("node-c", ALL_OPS, "1.12.0", FETCHING)?;

    let reply = cluster
        .fork_with(ForkInstanceRequest {
            instance_id: ORIGIN.into(),
            count: 4,
            spread: true,
            ..Default::default()
        })
        .await?;
    assert_eq!(
        reply.kind, "memory",
        "a spread fork takes a memory checkpoint"
    );
    assert_eq!(reply.durability, "store");
    assert_eq!(reply.instances.len(), 4);

    // Captured: still waiting, now for the publish.
    cluster
        .heartbeat(vec![SnapshotReport {
            snapshot_id: reply.snapshot.clone(),
            op: "capture".into(),
            ok: true,
            manifest_json: memory_manifest(&reply.snapshot, origin_alloc, None),
            duration_ms: 610,
            total_bytes: 64 * 1024 * 1024,
            allocation_id: origin_alloc,
            pause_ms: 420,
            ..Default::default()
        }])
        .await?;
    assert!(cluster.events("instance.forked")?.is_empty());
    for id in &reply.instances {
        assert_eq!(
            cluster.info(id).await?.pending_op.as_deref(),
            Some("forking"),
            "{id} waits for the publish"
        );
    }
    cluster.reschedule()?;
    assert!(cluster.alloc_of(&reply.instances[0])?.is_none());
    let asked = requests_for(&cluster.heartbeat(Vec::new()).await?, &reply.snapshot);
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].op, "publish");

    // Published: the forks are released and spread.
    let url = format!("file:///shared/sha256/{}", "cd".repeat(32));
    cluster
        .heartbeat(vec![SnapshotReport {
            snapshot_id: reply.snapshot.clone(),
            op: "publish".into(),
            ok: true,
            manifest_json: memory_manifest(&reply.snapshot, origin_alloc, Some(&url)),
            total_bytes: 64 * 1024 * 1024,
            duration_ms: 900,
            ..Default::default()
        }])
        .await?;
    let forked = cluster.events("instance.forked")?;
    assert_eq!(forked.len(), 1);
    assert_eq!(forked[0]["instances"], json!(reply.instances));
    cluster.reschedule()?;
    let mut nodes = Vec::new();
    let mut waiting = Vec::new();
    for id in &reply.instances {
        match node_of(&cluster, id)? {
            Some(node) => nodes.push((id.clone(), node)),
            None => waiting.push(id.clone()),
        }
    }
    let distinct: HashSet<&str> = nodes.iter().map(|(_, node)| node.as_str()).collect();
    assert_eq!(nodes.len(), 3, "one fork per node: {nodes:?}");
    assert_eq!(distinct.len(), 3, "no two forks share a node: {nodes:?}");
    assert_eq!(waiting.len(), 1, "the fourth waits for a node: {waiting:?}");
    assert_eq!(cluster.info(&waiting[0]).await?.state, "pending");

    // Each placed fork restores; the waiting one still holds the
    // checkpoint, so it is not spent yet.
    for (id, node) in &nodes {
        let allocation = cluster.alloc_of(id)?.context("placed")?;
        cluster
            .heartbeat_as(
                node,
                vec![restore_report(&reply.snapshot, allocation, true)],
            )
            .await?;
    }
    assert_eq!(
        cluster
            .store()
            .get_snapshot(&reply.snapshot)?
            .context("snapshot")?
            .state
            .as_str(),
        "published"
    );
    // The last fork goes away: the published checkpoint is spent.
    cluster
        .svc
        .destroy_instance(Request::new(DestroyInstanceRequest {
            instance_id: waiting[0].clone(),
            idempotency_key: None,
        }))
        .await?;
    cluster.store().spend_unused_fork_snapshots()?;
    assert_eq!(
        cluster
            .store()
            .get_snapshot(&reply.snapshot)?
            .map(|snapshot| snapshot.state.as_str().to_string())
            .as_deref(),
        Some("deleting"),
        "a spent published fork checkpoint is reclaimed on its node"
    );
    Ok(())
}

/// A failed publish of a spread fork's checkpoint is retried by the manager,
/// and the forks keep waiting meanwhile. (The last failure terminates them;
/// the retry schedule itself is covered in `snapshot_ops`'s unit tests.)
#[tokio::test(flavor = "multi_thread")]
async fn a_spread_fork_keeps_waiting_while_its_publish_is_retried() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node_as(NODE, ALL_OPS, "1.12.0", PUBLISHING)?;
    cluster.submit(job(memory_sandbox()))?;
    let origin_alloc = cluster.active_origin()?;
    let reply = cluster
        .fork_with(ForkInstanceRequest {
            instance_id: ORIGIN.into(),
            count: 2,
            spread: true,
            ..Default::default()
        })
        .await?;
    cluster
        .heartbeat(vec![SnapshotReport {
            snapshot_id: reply.snapshot.clone(),
            op: "capture".into(),
            ok: true,
            manifest_json: memory_manifest(&reply.snapshot, origin_alloc, None),
            allocation_id: origin_alloc,
            ..Default::default()
        }])
        .await?;
    let failed = SnapshotReport {
        snapshot_id: reply.snapshot.clone(),
        op: "publish".into(),
        ok: false,
        error: "s3: access denied".into(),
        ..Default::default()
    };
    cluster.heartbeat(vec![failed.clone()]).await?;
    for id in &reply.instances {
        let info = cluster.info(id).await?;
        assert_eq!(info.state, "pending", "a retry is coming");
        assert_eq!(info.pending_op.as_deref(), Some("forking"));
    }
    assert!(cluster.events("instance.fork_failed")?.is_empty());
    Ok(())
}

/// Unsupported spread forks, and a disk fork that would reuse the origin's
/// TAP device, are refused with the fix in the message.
#[tokio::test(flavor = "multi_thread")]
async fn unsupported_spread_and_tap_forks_are_refused_with_a_hint() -> R<()> {
    // No contentStore on the origin's worker.
    let cluster = Cluster::new()?;
    cluster.add_node_as(NODE, ALL_OPS, "1.12.0", FETCHING)?;
    cluster.submit(job(memory_sandbox()))?;
    cluster.active_origin()?;
    let status = cluster
        .fork_with(ForkInstanceRequest {
            instance_id: ORIGIN.into(),
            spread: true,
            ..Default::default()
        })
        .await
        .expect_err("nowhere to publish");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains("contentStore"), "{status:?}");
    assert!(status.message().contains("Hint:"), "{status:?}");

    // A disk checkpoint cannot be published.
    let status = cluster
        .fork_with(ForkInstanceRequest {
            instance_id: ORIGIN.into(),
            spread: true,
            checkpoint: Some("disk".into()),
            ..Default::default()
        })
        .await
        .expect_err("disk cannot spread");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains("--checkpoint disk"), "{status:?}");

    // A disk fork of a TAP-wired sandbox would boot every fork on the TAP
    // device the origin holds.
    let cluster = Cluster::new()?;
    cluster.add_node(ALL_OPS, "1.12.0")?;
    let mut sandbox = task("sandbox", true);
    sandbox.config = json!({
        "image": "alpine",
        "command": ["sleep", "60"],
        "hypervisor": "firecracker",
        "network": { "mode": "tap", "tap": "tap0" },
    });
    cluster.submit(job(sandbox))?;
    cluster.active_origin()?;
    let status = cluster
        .fork_with(ForkInstanceRequest {
            instance_id: ORIGIN.into(),
            checkpoint: Some("disk".into()),
            ..Default::default()
        })
        .await
        .expect_err("a disk fork would share tap0");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().contains("tap0") && status.message().contains("--checkpoint memory"),
        "{status:?}"
    );
    assert_eq!(cluster.store().list_instances(JOB)?.len(), 1);
    Ok(())
}

/// A fork stopped before its checkpoint landed keeps waiting on it, and a
/// later start restores it, never boots it on an empty volume.
#[tokio::test(flavor = "multi_thread")]
async fn a_fork_stopped_before_its_checkpoint_restores_when_started() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(ALL_OPS, "1.12.0")?;
    cluster.submit(job(task("exec", true)))?;
    let origin_alloc = cluster.active_origin()?;
    let reply = cluster.fork(1, None).await?;
    let fork = reply.instances[0].clone();
    cluster
        .svc
        .stop_instance(Request::new(legion::structs::StopInstanceRequest {
            instance_id: fork.clone(),
            idempotency_key: None,
        }))
        .await?;
    cluster
        .heartbeat(vec![capture_ok(&reply.snapshot, origin_alloc, 0)])
        .await?;
    cluster.store().spend_unused_fork_snapshots()?;
    assert_eq!(
        cluster
            .store()
            .get_snapshot(&reply.snapshot)?
            .context("snapshot")?
            .state
            .as_str(),
        "local",
        "the stopped fork still holds its checkpoint"
    );
    let info = cluster.info(&fork).await?;
    assert_eq!(info.state, "stopped");
    assert_eq!(
        info.restore_snapshot_id.as_deref(),
        Some(reply.snapshot.as_str())
    );

    cluster
        .svc
        .start_instance(Request::new(legion::structs::StartInstanceRequest {
            instance_id: fork.clone(),
            idempotency_key: None,
            cold: false,
        }))
        .await?;
    cluster.reschedule()?;
    let allocation = cluster.alloc_of(&fork)?.context("the fork is placed")?;
    let restore = cluster
        .desired()
        .await?
        .into_iter()
        .find(|desired| desired.allocation_id == allocation)
        .and_then(|desired| desired.restore)
        .context("the fork restores from its checkpoint")?;
    assert_eq!(restore.snapshot_id, reply.snapshot);
    assert!(restore.fork);
    Ok(())
}

/// Under `stateful.idSource = "caller"` forks still get derived
/// `<origin>-f<n>` ids, which stay in the caller's namespace. An origin id
/// too long to suffix is refused rather than given a minted id.
#[tokio::test(flavor = "multi_thread")]
async fn forks_of_a_caller_named_task_get_derived_ids() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(ALL_OPS, "1.12.0")?;
    let mut caller_named = task("exec", true);
    if let Some(stateful) = caller_named.stateful.as_mut() {
        stateful.id_source = Some("caller".into());
    }
    cluster.submit(job(caller_named))?;
    cluster.active_origin()?;
    let reply = cluster.fork(2, None).await?;
    assert_eq!(reply.instances, vec!["run-17-f1", "run-17-f2"]);

    let long = "u".repeat(126);
    cluster
        .store()
        .create_keyed_instance(JOB, "default", TASK, Some(&long))?;
    cluster.reschedule()?;
    let status = cluster
        .fork_with(ForkInstanceRequest {
            instance_id: long.clone(),
            ..Default::default()
        })
        .await
        .expect_err("no room for -f1");
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    assert!(status.message().contains("idSource"), "{status:?}");
    assert!(
        cluster
            .store()
            .list_instances(JOB)?
            .iter()
            .all(|instance| instance.instance_id.len() <= 128
                && !instance.instance_id.starts_with("inst-")),
        "nothing minted"
    );
    Ok(())
}
