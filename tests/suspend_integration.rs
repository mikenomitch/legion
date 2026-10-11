//! Suspend and resume end to end on the manager (docs/suspend-and-resume.md):
//! SuspendInstance, the suspend transaction the capture report applies,
//! resume through StartInstance, `onRestoreFailure`, `reserve`, node loss,
//! and the state-machine interleavings with stop and destroy.
//!
//! The heartbeat stands in for the worker, as in `snapshot_integration.rs`.
//! The worker half (pause, capture, stop; cold boot on the retained volume)
//! runs against a real `exec` task in `src/worker/task_manager.rs`
//! (`an_exec_disk_suspend_cold_boots_on_its_retained_volume`), and the
//! guest's identity refresh in `src/task_plugins/guest_resume.rs`.
//!
//! Automatic suspend (Phase 5) is here too: the idle sweep over usage
//! reports the heartbeat carries, and the local suspensions a node's
//! `suspend` pressure step reports.

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use legion::manager::server::{
    idle_sweep, make_room_sweep, settle_stalled_resumes, IdleSweeper, MakeRoomState,
    ManagerDaemonService,
};
use legion::prelude::{json, Context, R};
use legion::storage::events::SchedulerEventFilter;
use legion::storage::store::Store;
use legion::structs::manager_server::Manager;
use legion::structs::suspend::{IdleConfig, SuspendConfig, SuspendReserveConfig};
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, job::StatefulConfig, job::StatefulStorageConfig,
    job::TaskConfig, AllocationUsage, CheckpointRequest, DestroyInstanceRequest,
    GetInstanceRequest, HeartbeatReply, HeartbeatRequest, InstanceInfo, InstanceMode, Node,
    NodeMsg, PressureAction, SnapshotReport, StartInstanceRequest, StopInstanceRequest, Submission,
    SuspendInstanceRequest, UsageReport,
};
use serde_json::Value;
use tonic::{Code, Request};

const NODE: &str = "suspend-node";
const JOB: &str = "workspaces";
const TASK: &str = "workspace";
const ID: &str = "user-42";

struct Cluster {
    svc: ManagerDaemonService,
}

impl Cluster {
    fn new() -> R<Self> {
        let dir = tempfile::Builder::new()
            .prefix("legion-suspend-it-")
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

    fn add_node(&self, capabilities: Value) -> R<()> {
        let config_attrs = json!({
            "node_id": NODE,
            "maxAllocations": 20,
            "resources": { "memory": 65536, "cpu": 40000 },
        });
        self.store().insert_node(NodeMsg::from(Node {
            id: NODE.to_string(),
            pool: "default".into(),
            status: "ready".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs,
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
                "sandbox.vmm.version": "1.7.0",
                "volumeDrivers": {
                    "local-dir": {
                        "apiVersion": 1, "name": "local-dir", "nodeLocal": true,
                        "ephemeral": false, "accessModes": ["single-writer"], "available": true,
                        "ops": ["snapshot", "restore", "clone", "snapshot-delete"]
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
        self.reschedule_job(JOB)
    }

    fn reschedule_job(&self, job: &str) -> R<()> {
        let config_id = self.store().get_job_config(job)?.id();
        self.store()
            .enqueue_eval("job_reschedule", Some(job), Some(&config_id), None, None)?;
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
        Ok(self
            .svc
            .heartbeat(Request::new(HeartbeatRequest {
                id: NODE.into(),
                snapshot_reports: reports,
                ..Default::default()
            }))
            .await?
            .into_inner())
    }

    fn active_instance(&self) -> R<i64> {
        self.active_instance_named(ID)
    }

    fn active_instance_named(&self, id: &str) -> R<i64> {
        self.store()
            .create_keyed_instance(JOB, "default", TASK, Some(id))?;
        self.reschedule()?;
        self.store()
            .get_instance(id)?
            .context("instance")?
            .current_alloc_id
            .context("the instance should be placed")
    }

    /// A heartbeat carrying a usage report, as a worker with enforcement on
    /// sends every 10s.
    async fn report_usage(&self, allocations: Vec<AllocationUsage>) -> R<HeartbeatReply> {
        Ok(self
            .svc
            .heartbeat(Request::new(HeartbeatRequest {
                id: NODE.into(),
                usage: Some(UsageReport {
                    sampled_at_unix_ms: chrono::Utc::now().timestamp_millis(),
                    allocations,
                    node_memory: None,
                    pressure: None,
                }),
                ..Default::default()
            }))
            .await?
            .into_inner())
    }

    async fn node_usage(&self) -> R<legion::structs::GetNodeUsageReply> {
        Ok(self
            .svc
            .get_node_usage(Request::new(legion::structs::GetNodeUsageRequest {
                node_id: NODE.into(),
            }))
            .await?
            .into_inner())
    }

    async fn info_of(&self, id: &str) -> R<InstanceInfo> {
        self.svc
            .get_instance(Request::new(GetInstanceRequest {
                instance_id: id.into(),
            }))
            .await?
            .into_inner()
            .instance
            .context("instance")
    }

    async fn suspend(
        &self,
        checkpoint: Option<&str>,
        key: Option<&str>,
    ) -> Result<legion::structs::SuspendInstanceReply, tonic::Status> {
        self.svc
            .suspend_instance(Request::new(SuspendInstanceRequest {
                instance_id: ID.into(),
                checkpoint: checkpoint.map(str::to_string),
                idempotency_key: key.map(str::to_string),
            }))
            .await
            .map(|reply| reply.into_inner())
    }

    async fn start(
        &self,
        cold: bool,
    ) -> Result<legion::structs::StartInstanceReply, tonic::Status> {
        self.svc
            .start_instance(Request::new(StartInstanceRequest {
                instance_id: ID.into(),
                idempotency_key: None,
                cold,
            }))
            .await
            .map(|reply| reply.into_inner())
    }

    async fn info(&self) -> R<InstanceInfo> {
        self.svc
            .get_instance(Request::new(GetInstanceRequest {
                instance_id: ID.into(),
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

    /// Suspend and have the node report a successful capture. Returns the
    /// checkpoint id.
    async fn suspended(&self, allocation_id: i64) -> R<String> {
        let reply = self.suspend(None, None).await?;
        self.heartbeat(vec![capture_ok(
            &reply.snapshot_id,
            allocation_id,
            &reply.kind,
        )])
        .await?;
        Ok(reply.snapshot_id)
    }
}

fn job(runtime: &str, suspend: Option<SuspendConfig>) -> JobConfig {
    let task = TaskConfig {
        config: if runtime == "sandbox" {
            json!({ "image": "alpine", "command": ["sleep", "60"] })
        } else {
            json!({ "command": ["sleep", "60"] })
        },
        stateful: Some(StatefulConfig {
            mode: InstanceMode::Keyed,
            require_id: false,
            initial_instances: None,
            on_lost: None,
            storage: Some(StatefulStorageConfig {
                path: "/workspace".into(),
                driver: Some("local-dir".into()),
                mode: None,
                params: None,
                retain_on_scale_in: None,
                retain_on_stop: None,
            }),
        }),
        suspend,
        ..TaskConfig::new(TASK, runtime)
    };
    JobConfig {
        priority: Default::default(),
        name: JOB.to_string(),
        tasks: vec![task],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    }
}

fn manifest(snapshot_id: &str, allocation_id: i64, kind: &str) -> String {
    json!({
        "apiVersion": 1,
        "id": snapshot_id,
        "jobId": JOB,
        "task": TASK,
        "originInstance": ID,
        "originAllocId": allocation_id,
        "runtime": if kind == "memory" { "sandbox" } else { "exec" },
        "kind": kind,
        "createdAt": "2026-10-10T00:00:00Z",
    })
    .to_string()
}

fn capture_ok(snapshot_id: &str, allocation_id: i64, kind: &str) -> SnapshotReport {
    SnapshotReport {
        snapshot_id: snapshot_id.into(),
        op: "capture".into(),
        ok: true,
        manifest_json: manifest(snapshot_id, allocation_id, kind),
        duration_ms: 740,
        total_bytes: if kind == "memory" { 1_918_893_056 } else { 0 },
        allocation_id,
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
            "restoring sandbox from checkpoint: snapshot/load failed".into()
        },
        duration_ms: 212,
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

/// The whole life: suspend frees the allocation, resume restores on the same
/// node as a NEW allocation, and the checkpoint is reclaimed afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn suspend_frees_the_allocation_and_start_resumes_as_a_new_allocation() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", Some(SuspendConfig::default())))?;
    let first_alloc = cluster.active_instance()?;

    let reply = cluster.suspend(None, Some("suspend-key")).await?;
    assert_eq!(reply.kind, "disk", "exec suspends to disk by default");
    assert_eq!(reply.node_id, NODE);
    assert!(!reply.deduplicated);
    let info = cluster.info().await?;
    assert_eq!(info.state, "active", "running until the node reports");
    assert_eq!(info.pending_op.as_deref(), Some("suspending"));

    // The node is asked to capture and then stop.
    let asked = requests_for(&cluster.heartbeat(Vec::new()).await?, &reply.snapshot_id);
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].op, "capture");
    assert_eq!(asked[0].then, "stop");
    assert_eq!(asked[0].purpose, "suspend");
    assert_eq!(asked[0].allocation_id, first_alloc);

    let reply_after = cluster
        .heartbeat(vec![capture_ok(&reply.snapshot_id, first_alloc, "disk")])
        .await?;
    // The allocation is gone (its reservation with it), and the node is not
    // asked again.
    assert!(reply_after.desired_allocations.is_empty());
    assert!(cluster.store().allocs_for_node(NODE)?.is_empty());
    assert!(requests_for(&reply_after, &reply.snapshot_id).is_empty());
    let info = cluster.info().await?;
    assert_eq!(info.state, "suspended");
    assert_eq!(info.current_alloc_id, None);
    assert_eq!(
        info.checkpoint_id.as_deref(),
        Some(reply.snapshot_id.as_str())
    );
    assert_eq!(info.checkpoint_kind.as_deref(), Some("disk"));
    assert_eq!(info.checkpoint_node_id.as_deref(), Some(NODE));
    assert_eq!(info.last_suspend_ms, Some(740));
    assert_eq!(info.suspend_reason.as_deref(), Some("manual"));
    assert!(info.pending_op.is_none());
    assert_eq!(cluster.events("instance.suspended")?.len(), 1);
    assert!(cluster
        .events("allocation.removed")?
        .iter()
        .any(|meta| meta["cause"] == "suspended"));
    let notices = cluster.store().stop_notices_for_node(NODE)?;
    assert!(
        notices
            .iter()
            .any(|notice| notice.allocation_id == first_alloc
                && notice.cause.as_str() == "suspended"),
        "allocation.removed carries cause = suspended"
    );

    // Suspended instances do not count toward desired.
    cluster.reschedule()?;
    assert!(cluster.store().allocs_for_node(NODE)?.is_empty());

    // Resume: pinned to the checkpoint's node, as a new allocation.
    let started = cluster.start(false).await?;
    assert_eq!(started.resuming_from, reply.snapshot_id);
    assert_eq!(started.node_id, NODE);
    cluster.reschedule()?;
    let info = cluster.info().await?;
    let second_alloc = info.current_alloc_id.context("resumed allocation")?;
    assert_ne!(
        second_alloc, first_alloc,
        "a resume runs as a NEW allocation, so its workload-identity token names a new id"
    );
    assert_eq!(info.pending_op.as_deref(), Some("resuming"));
    let reply_hb = cluster.heartbeat(Vec::new()).await?;
    let restore = reply_hb
        .desired_allocations
        .iter()
        .find(|allocation| allocation.allocation_id == second_alloc)
        .and_then(|allocation| allocation.restore.clone())
        .context("the resumed allocation carries the restore payload")?;
    assert_eq!(restore.snapshot_id, reply.snapshot_id);

    cluster
        .heartbeat(vec![restore_report(&reply.snapshot_id, second_alloc, true)])
        .await?;
    let info = cluster.info().await?;
    assert_eq!(info.state, "active");
    assert!(info.checkpoint_id.is_none());
    assert!(info.pending_op.is_none());
    assert_eq!(info.last_restore_ms, Some(212));
    assert_eq!(
        info.restored_from_snapshot_id.as_deref(),
        Some(reply.snapshot_id.as_str())
    );
    let resumed = cluster.events("instance.resumed")?;
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0]["restoreMs"], 212);
    // The spent checkpoint is reclaimed on its node.
    let snapshot = cluster
        .store()
        .get_snapshot(&reply.snapshot_id)?
        .context("snapshot row until the node reclaims it")?;
    assert_eq!(snapshot.state.as_str(), "deleting");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_double_suspend_with_one_key_is_one_suspend() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    cluster.active_instance()?;

    let first = cluster.suspend(None, Some("k1")).await?;
    let again = cluster.suspend(None, Some("k1")).await?;
    assert!(again.deduplicated);
    assert_eq!(again.snapshot_id, first.snapshot_id);
    // Without the key, a second suspend while the first is in flight names
    // the same checkpoint and changes nothing.
    let other = cluster.suspend(None, None).await?;
    assert!(!other.deduplicated);
    assert_eq!(other.snapshot_id, first.snapshot_id);
    assert!(
        other.message.contains("already being suspended"),
        "{}",
        other.message
    );
    let snapshots = cluster.store().list_snapshots(&Default::default())?;
    assert_eq!(snapshots.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn suspending_a_stopped_instance_is_rejected() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    cluster.active_instance()?;
    cluster
        .svc
        .stop_instance(Request::new(StopInstanceRequest {
            instance_id: ID.into(),
            idempotency_key: None,
        }))
        .await?;
    let err = cluster.suspend(None, None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message()
            .contains("no running allocation (it is stopped)"),
        "{}",
        err.message()
    );
    Ok(())
}

/// A suspend never stops a workload it failed to capture.
#[tokio::test(flavor = "multi_thread")]
async fn a_checkpoint_failure_leaves_the_allocation_running() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    let allocation_id = cluster.active_instance()?;
    let reply = cluster.suspend(None, None).await?;
    let hb = cluster
        .heartbeat(vec![SnapshotReport {
            snapshot_id: reply.snapshot_id.clone(),
            op: "capture".into(),
            ok: false,
            error: "failed to store checkpoint part memory in the content store".into(),
            allocation_id,
            ..Default::default()
        }])
        .await?;
    assert!(hb
        .desired_allocations
        .iter()
        .any(|allocation| allocation.allocation_id == allocation_id));
    let info = cluster.info().await?;
    assert_eq!(info.state, "active");
    assert_eq!(info.current_alloc_id, Some(allocation_id));
    assert!(info.pending_op.is_none());
    assert!(
        info.detail
            .as_deref()
            .is_some_and(|detail| detail.contains("suspend failed")),
        "{:?}",
        info.detail
    );
    assert!(cluster.store().get_snapshot(&reply.snapshot_id)?.is_none());
    assert_eq!(cluster.events("instance.suspend_failed")?.len(), 1);
    // And it can be suspended again.
    assert!(cluster.suspend(None, None).await.is_ok());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_restore_boots_cold_by_default() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    let first = cluster.active_instance()?;
    let checkpoint = cluster.suspended(first).await?;
    cluster.start(false).await?;
    cluster.reschedule()?;
    let second = cluster.info().await?.current_alloc_id.context("placed")?;
    cluster
        .heartbeat(vec![restore_report(&checkpoint, second, false)])
        .await?;
    let info = cluster.info().await?;
    assert!(info.checkpoint_id.is_none(), "the checkpoint is discarded");
    assert!(info.restore_snapshot_id.is_none(), "the next boot is cold");
    assert!(info
        .detail
        .as_deref()
        .is_some_and(|detail| detail.contains("booted cold")));
    let failed = cluster.events("instance.restore_failed")?;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["outcome"], "cold");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_restore_stays_suspended_with_on_restore_failure_fail() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job(
        "exec",
        Some(SuspendConfig {
            on_restore_failure: Some("fail".into()),
            ..Default::default()
        }),
    ))?;
    let first = cluster.active_instance()?;
    let checkpoint = cluster.suspended(first).await?;
    cluster.start(false).await?;
    cluster.reschedule()?;
    let second = cluster.info().await?.current_alloc_id.context("placed")?;
    cluster
        .heartbeat(vec![restore_report(&checkpoint, second, false)])
        .await?;
    let info = cluster.info().await?;
    assert_eq!(info.state, "suspended");
    assert_eq!(info.current_alloc_id, None);
    assert_eq!(info.checkpoint_id.as_deref(), Some(checkpoint.as_str()));
    assert!(info
        .detail
        .as_deref()
        .is_some_and(|detail| detail.contains("onRestoreFailure = \"fail\"")));
    assert!(cluster.store().allocs_for_node(NODE)?.is_empty());
    assert_eq!(
        cluster.events("instance.restore_failed")?[0]["outcome"],
        "suspended"
    );
    // The escape hatch: --cold discards the checkpoint and boots.
    let cold = cluster.start(true).await?;
    assert!(cold.resuming_from.is_empty());
    assert!(cold.message.contains("cold"), "{}", cold.message);
    cluster.reschedule()?;
    let info = cluster.info().await?;
    assert!(info.current_alloc_id.is_some());
    assert!(info.restore_snapshot_id.is_none());
    Ok(())
}

/// Node loss: a local checkpoint is lost with its node, and `start` follows
/// `onRestoreFailure`.
#[tokio::test(flavor = "multi_thread")]
async fn node_loss_marks_the_checkpoint_lost_and_start_follows_on_restore_failure() -> R<()> {
    for policy in ["cold", "fail"] {
        let cluster = Cluster::new()?;
        cluster.add_node(json!(["checkpoint"]))?;
        cluster.submit(job(
            "exec",
            Some(SuspendConfig {
                on_restore_failure: Some(policy.into()),
                ..Default::default()
            }),
        ))?;
        let first = cluster.active_instance()?;
        let checkpoint = cluster.suspended(first).await?;
        cluster.store().remove_node(NODE)?;
        assert_eq!(
            cluster
                .store()
                .get_snapshot(&checkpoint)?
                .context("snapshot")?
                .state
                .as_str(),
            "lost"
        );
        let started = cluster.start(false).await;
        if policy == "cold" {
            let started = started?;
            assert!(started.resuming_from.is_empty());
            assert!(started.message.contains("lost"), "{}", started.message);
            assert_eq!(cluster.info().await?.state, "pending");
            assert_eq!(cluster.events("instance.restore_failed")?.len(), 1);
        } else {
            let err = started.unwrap_err();
            assert_eq!(err.code(), Code::FailedPrecondition);
            assert!(err.message().contains("--cold"), "{}", err.message());
            let info = cluster.info().await?;
            assert_eq!(info.state, "suspended");
            assert!(info.detail.as_deref().is_some_and(|d| d.contains("lost")));
        }
    }
    Ok(())
}

/// A resume in flight whose checkpoint node goes away settles through the
/// sweep instead of waiting forever.
#[tokio::test(flavor = "multi_thread")]
async fn a_resume_whose_checkpoint_is_lost_boots_cold_through_the_sweep() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    let first = cluster.active_instance()?;
    let checkpoint = cluster.suspended(first).await?;
    cluster.start(false).await?;
    // The node is removed before the resume was placed.
    cluster.store().remove_node(NODE)?;
    settle_stalled_resumes(cluster.store());
    let info = cluster.info().await?;
    assert!(info.restore_snapshot_id.is_none());
    assert!(info.checkpoint_id.is_none());
    assert!(info.pending_op.is_none());
    let failed = cluster.events("instance.restore_failed")?;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["snapshotId"], checkpoint.as_str());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_without_the_checkpoint_capability_is_named() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!([]))?;
    cluster.submit(job("exec", None))?;
    cluster.active_instance()?;
    let err = cluster.suspend(None, None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("worker version 0.9.0"),
        "{}",
        err.message()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_memory_checkpoint_of_a_disk_only_runtime_is_refused_with_the_hint() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    cluster.active_instance()?;
    let err = cluster.suspend(Some("memory"), None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message()
            .contains("cannot take a memory checkpoint: runtime \"exec\" supports \"disk\" only"),
        "{}",
        err.message()
    );
    assert!(
        err.message().contains("Hint: use --checkpoint disk"),
        "{}",
        err.message()
    );
    let err = cluster.suspend(Some("ram"), None).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    Ok(())
}

/// `reserve = true`: the allocation row stays as the reservation, is never
/// dispatched, is neither culled nor re-placed, and resume releases it.
#[tokio::test(flavor = "multi_thread")]
async fn reserve_holds_the_allocation_row_until_resume() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job(
        "exec",
        Some(SuspendConfig {
            reserve: Some(SuspendReserveConfig::Flag(true)),
            ..Default::default()
        }),
    ))?;
    let first = cluster.active_instance()?;
    let checkpoint = cluster.suspended(first).await?;
    let held = cluster.store().allocs_for_node(NODE)?;
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].id, first);
    assert_eq!(held[0].status, "suspended");
    let info = cluster.info().await?;
    assert_eq!(info.state, "suspended");
    assert_eq!(info.current_alloc_id, Some(first));
    // Never dispatched, never culled, never re-placed.
    assert!(cluster
        .heartbeat(Vec::new())
        .await?
        .desired_allocations
        .is_empty());
    cluster.reschedule()?;
    assert_eq!(cluster.store().allocs_for_node(NODE)?.len(), 1);

    let started = cluster.start(false).await?;
    assert_eq!(started.resuming_from, checkpoint);
    cluster.reschedule()?;
    let allocs = cluster.store().allocs_for_node(NODE)?;
    assert_eq!(allocs.len(), 1, "the hold became the restore");
    assert_ne!(allocs[0].id, first);
    assert_eq!(allocs[0].status, "placed");
    Ok(())
}

/// The state machine against stop and destroy, at every point a suspend or
/// resume can be in flight.
#[tokio::test(flavor = "multi_thread")]
async fn suspend_and_resume_interleave_with_stop_and_destroy() -> R<()> {
    // Stop while the capture is in flight: the late report is discarded.
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    let allocation = cluster.active_instance()?;
    let reply = cluster.suspend(None, None).await?;
    cluster
        .svc
        .stop_instance(Request::new(StopInstanceRequest {
            instance_id: ID.into(),
            idempotency_key: None,
        }))
        .await?;
    cluster
        .heartbeat(vec![capture_ok(&reply.snapshot_id, allocation, "disk")])
        .await?;
    let info = cluster.info().await?;
    assert_eq!(info.state, "stopped");
    assert!(info.checkpoint_id.is_none());
    assert_eq!(
        cluster
            .store()
            .get_snapshot(&reply.snapshot_id)?
            .context("snapshot")?
            .state
            .as_str(),
        "deleting",
        "nothing will resume from it, so the node reclaims it"
    );
    assert!(cluster.events("instance.suspended")?.is_empty());

    // Stop a suspended instance: it is stopped and its checkpoint reclaimed;
    // start then boots it cold.
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    let allocation = cluster.active_instance()?;
    let checkpoint = cluster.suspended(allocation).await?;
    cluster
        .svc
        .stop_instance(Request::new(StopInstanceRequest {
            instance_id: ID.into(),
            idempotency_key: None,
        }))
        .await?;
    let info = cluster.info().await?;
    assert_eq!(info.state, "stopped");
    assert!(info.checkpoint_id.is_none());
    assert_eq!(
        cluster
            .store()
            .get_snapshot(&checkpoint)?
            .context("snapshot")?
            .state
            .as_str(),
        "deleting"
    );
    let started = cluster.start(false).await?;
    assert!(started.resuming_from.is_empty());

    // Destroy a suspended instance: tombstoned, checkpoint reclaimed.
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    let allocation = cluster.active_instance()?;
    let checkpoint = cluster.suspended(allocation).await?;
    cluster
        .svc
        .destroy_instance(Request::new(DestroyInstanceRequest {
            instance_id: ID.into(),
            idempotency_key: None,
        }))
        .await?;
    assert_eq!(cluster.info().await?.state, "terminated");
    assert_eq!(
        cluster
            .store()
            .get_snapshot(&checkpoint)?
            .context("snapshot")?
            .state
            .as_str(),
        "deleting"
    );

    // Stop while a resume is in flight: the restore's report no longer
    // matches anything, and the checkpoint is reclaimed.
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    let first = cluster.active_instance()?;
    let checkpoint = cluster.suspended(first).await?;
    cluster.start(false).await?;
    cluster.reschedule()?;
    let second = cluster.info().await?.current_alloc_id.context("placed")?;
    cluster
        .svc
        .stop_instance(Request::new(StopInstanceRequest {
            instance_id: ID.into(),
            idempotency_key: None,
        }))
        .await?;
    cluster
        .heartbeat(vec![restore_report(&checkpoint, second, true)])
        .await?;
    let info = cluster.info().await?;
    assert_eq!(info.state, "stopped");
    assert!(info.checkpoint_id.is_none());
    assert!(cluster.events("instance.resumed")?.is_empty());

    // A job stop leaves a suspended instance suspended (it holds nothing).
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    let allocation = cluster.active_instance()?;
    let checkpoint = cluster.suspended(allocation).await?;
    cluster.store().stop_instances_for_job(JOB)?;
    let info = cluster.info().await?;
    assert_eq!(info.state, "suspended");
    assert_eq!(info.checkpoint_id.as_deref(), Some(checkpoint.as_str()));
    Ok(())
}

/// Allocation ids are never reused, even when the newest allocation is the
/// one deleted (a stop then start here; a suspend then resume above).
#[tokio::test(flavor = "multi_thread")]
async fn allocation_ids_are_never_reused() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    let first = cluster.active_instance()?;
    cluster
        .svc
        .stop_instance(Request::new(StopInstanceRequest {
            instance_id: ID.into(),
            idempotency_key: None,
        }))
        .await?;
    assert!(cluster.store().allocs_for_node(NODE)?.is_empty());
    cluster.start(false).await?;
    cluster.reschedule()?;
    let second = cluster.info().await?.current_alloc_id.context("placed")?;
    assert!(second > first, "{second} must be a new id after {first}");
    Ok(())
}

fn idle_usage(allocation_id: i64, idle_since_unix: i64, after: u64) -> AllocationUsage {
    AllocationUsage {
        allocation_id,
        memory_current_bytes: 64 << 20,
        last_activity_unix: idle_since_unix,
        idle_since_unix,
        idle_after_seconds: after,
        ..Default::default()
    }
}

fn idle_job() -> JobConfig {
    job(
        "exec",
        Some(SuspendConfig {
            idle: Some(IdleConfig {
                after: Some("60s".into()),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )
}

/// The Phase 5 idle acceptance, with fake usage: an instance quiet for its
/// task's `idle.after` is suspended by the sweep with reason `idle`, an
/// instance that stays busy never is, and nothing happens before the window
/// runs out.
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_instance_suspends_after_its_window_and_a_busy_one_never_does() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(idle_job())?;
    let idle_alloc = cluster.active_instance_named(ID)?;
    let busy_alloc = cluster.active_instance_named("user-17")?;
    let usage = cluster.svc.usage_handle();
    let mut sweeper = IdleSweeper::default();
    let now = chrono::Utc::now().timestamp();
    let quiet_since = now - 30;

    // 30s quiet of a 60s window: nothing yet. The busy one reports activity
    // on every interval.
    cluster
        .report_usage(vec![
            idle_usage(idle_alloc, quiet_since, 60),
            idle_usage(busy_alloc, now, 60),
        ])
        .await?;
    let first = idle_sweep(cluster.store(), &usage, &mut sweeper);
    assert!(first.started.is_empty(), "{first:?}");
    let info = cluster.info().await?;
    assert!(info.idle_since.is_some(), "instance get shows idle since");
    assert_eq!(info.idle_after_seconds, Some(60));

    // Now quiet for 61s; the busy one is still busy.
    cluster
        .report_usage(vec![
            idle_usage(idle_alloc, now - 61, 60),
            idle_usage(busy_alloc, now, 60),
        ])
        .await?;
    let swept = idle_sweep(cluster.store(), &usage, &mut sweeper);
    assert_eq!(swept.started.len(), 1, "{swept:?}");
    assert_eq!(swept.started[0].0, ID);
    let snapshot_id = swept.started[0].1.clone();
    let info = cluster.info().await?;
    assert_eq!(info.pending_op.as_deref(), Some("suspending"));
    assert_eq!(info.suspend_reason.as_deref(), Some("idle"));
    // A second pass right away does not try again.
    assert!(idle_sweep(cluster.store(), &usage, &mut sweeper)
        .started
        .is_empty());

    // The node captures and stops it: suspended, reason idle.
    let asked = requests_for(&cluster.heartbeat(Vec::new()).await?, &snapshot_id);
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].then, "stop");
    cluster
        .heartbeat(vec![capture_ok(&snapshot_id, idle_alloc, "disk")])
        .await?;
    let info = cluster.info().await?;
    assert_eq!(info.state, "suspended");
    assert_eq!(info.suspend_reason.as_deref(), Some("idle"));
    assert!(
        info.idle_since.is_none(),
        "a suspended instance is not idle"
    );
    let suspended = cluster.events("instance.suspended")?;
    assert_eq!(suspended.len(), 1);
    assert_eq!(suspended[0]["reason"], "idle");

    // The busy one never moved.
    let busy = cluster.info_of("user-17").await?;
    assert_eq!(busy.state, "active");
    assert!(busy.pending_op.is_none());
    assert_eq!(busy.suspend_reason, None);
    Ok(())
}

/// The sweep leaves alone an instance that already has an operation in
/// flight, and one on a node without the checkpoint capability, and says
/// why.
#[tokio::test(flavor = "multi_thread")]
async fn the_idle_sweep_skips_what_it_cannot_suspend() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!([]))?;
    cluster.submit(idle_job())?;
    let alloc = cluster.active_instance()?;
    let usage = cluster.svc.usage_handle();
    let now = chrono::Utc::now().timestamp();
    cluster
        .report_usage(vec![idle_usage(alloc, now - 600, 60)])
        .await?;
    let swept = idle_sweep(cluster.store(), &usage, &mut IdleSweeper::default());
    assert!(swept.started.is_empty());
    assert_eq!(swept.skipped.len(), 1);
    assert!(
        swept.skipped[0].1.contains("cannot suspend instances"),
        "{:?}",
        swept.skipped
    );
    assert_eq!(cluster.info().await?.state, "active");
    Ok(())
}

fn local_capture(snapshot_id: &str, allocation_id: i64) -> SnapshotReport {
    SnapshotReport {
        local_suspension: true,
        ..capture_ok(snapshot_id, allocation_id, "disk")
    }
}

fn suspend_action(snapshot_id: &str, allocation_id: i64) -> PressureAction {
    PressureAction {
        allocation_id,
        job_id: JOB.into(),
        step: "suspend".into(),
        freed_bytes: 600 << 20,
        used_bytes: 600 << 20,
        reserved_bytes: 512 << 20,
        available_bytes: 100 << 20,
        min_free_bytes: 819 << 20,
        detail: "suspended by suspend on suspend-node: using 600 MiB of a 512 MiB reservation"
            .into(),
        snapshot_id: snapshot_id.into(),
        ..Default::default()
    }
}

/// The local-suspension apply path: a node's `suspend` pressure step
/// reports a capture the manager never asked for. The manager inserts the
/// row and applies the same suspend, with reason `pressure`; the
/// `node.pressure` event names the step; a re-sent report changes nothing;
/// and the instance resumes from the node's checkpoint.
#[tokio::test(flavor = "multi_thread")]
async fn a_local_suspension_applies_the_suspend_with_reason_pressure() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job(
        "exec",
        Some(SuspendConfig {
            on_pressure: Some(true),
            ..Default::default()
        }),
    ))?;
    let alloc = cluster.active_instance()?;
    let report = local_capture("snap-local", alloc);
    let reply = cluster
        .svc
        .heartbeat(Request::new(HeartbeatRequest {
            id: NODE.into(),
            snapshot_reports: vec![report.clone()],
            pressure_actions: vec![suspend_action("snap-local", alloc)],
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert!(
        reply.desired_allocations.is_empty(),
        "the suspended allocation leaves the desired set, which is the node's ack"
    );
    let info = cluster.info().await?;
    assert_eq!(info.state, "suspended");
    assert_eq!(info.suspend_reason.as_deref(), Some("pressure"));
    assert_eq!(info.checkpoint_id.as_deref(), Some("snap-local"));
    assert_eq!(info.checkpoint_node_id.as_deref(), Some(NODE));
    let snapshot = cluster
        .store()
        .get_snapshot("snap-local")?
        .context("the manager recorded the node's checkpoint")?;
    assert_eq!(snapshot.state.as_str(), "local");
    assert_eq!(snapshot.purpose.as_str(), "suspend");
    let pressure = cluster.events("node.pressure")?;
    assert_eq!(pressure.len(), 1);
    assert_eq!(pressure[0]["step"], "suspend");
    assert_eq!(pressure[0]["cause"], "suspended");
    assert_eq!(pressure[0]["snapshot_id"], "snap-local");
    let suspended = cluster.events("instance.suspended")?;
    assert_eq!(suspended.len(), 1);
    assert_eq!(suspended[0]["reason"], "pressure");

    // The node re-sends until it sees the allocation gone: harmless.
    cluster.heartbeat(vec![report]).await?;
    assert_eq!(cluster.events("instance.suspended")?.len(), 1);
    assert_eq!(cluster.info().await?.state, "suspended");

    // Suspended instances are not replaced.
    cluster.reschedule()?;
    assert!(cluster.store().allocs_for_node(NODE)?.is_empty());

    // And it resumes from the node's checkpoint.
    let started = cluster.start(false).await?;
    assert_eq!(started.resuming_from, "snap-local");
    Ok(())
}

/// A worker that suspended under pressure and lost contact reports the
/// local suspension when it reconnects. If the instance moved on meanwhile
/// (here: stopped), the checkpoint is recorded as `deleting` and the node is
/// asked to reclaim it; the instance is left as it is.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_local_suspension_for_an_instance_that_moved_on_is_reclaimed() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    let alloc = cluster.active_instance()?;
    cluster
        .svc
        .stop_instance(Request::new(StopInstanceRequest {
            instance_id: ID.into(),
            idempotency_key: None,
        }))
        .await?;
    let reply = cluster
        .heartbeat(vec![local_capture("snap-late", alloc)])
        .await?;
    let info = cluster.info().await?;
    assert_eq!(info.state, "stopped");
    assert!(info.checkpoint_id.is_none());
    let snapshot = cluster
        .store()
        .get_snapshot("snap-late")?
        .context("recorded so the node can be told to reclaim it")?;
    assert_eq!(snapshot.state.as_str(), "deleting");
    let next = cluster.heartbeat(Vec::new()).await?;
    let asked = requests_for(&next, "snap-late");
    assert!(
        asked.iter().any(|request| request.op == "delete")
            || requests_for(&reply, "snap-late")
                .iter()
                .any(|request| request.op == "delete"),
        "the node is asked to reclaim the checkpoint"
    );
    assert!(cluster.events("instance.suspended")?.is_empty());
    Ok(())
}

/// A SuspendInstance capture still outstanding when the node suspends the
/// same instance under pressure can never finish: the node's checkpoint
/// wins, and the requested one is dropped.
#[tokio::test(flavor = "multi_thread")]
async fn a_local_suspension_overtakes_a_pending_requested_suspend() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    let alloc = cluster.active_instance()?;
    let requested = cluster.suspend(None, None).await?;
    assert_eq!(
        cluster.info().await?.pending_op.as_deref(),
        Some("suspending")
    );
    cluster
        .heartbeat(vec![local_capture("snap-pressure", alloc)])
        .await?;
    let info = cluster.info().await?;
    assert_eq!(info.state, "suspended");
    assert_eq!(info.checkpoint_id.as_deref(), Some("snap-pressure"));
    assert_eq!(info.suspend_reason.as_deref(), Some("pressure"));
    assert!(cluster
        .store()
        .get_snapshot(&requested.snapshot_id)?
        .is_none());
    Ok(())
}

/// Suspend/resume UI plan §7.3: a pressure eviction leaves the
/// worker's detail and hint on the instance as `last_stop`, names the
/// instance in the `node.pressure` event, and a running instance reports
/// which checkpoints it can take with the RPCs' own refusals.
#[tokio::test(flavor = "multi_thread")]
async fn an_instance_records_its_last_stop_and_its_checkpoint_support() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job("exec", None))?;
    let alloc = cluster.active_instance()?;

    // An exec task with a local-dir volume: disk everything, memory nothing,
    // and no publish (node-local volume, no contentStore).
    let info = cluster.info().await?;
    assert!(info.last_stop.is_none(), "nothing has stopped yet");
    let support = info
        .checkpoint_support
        .context("an active instance reports checkpoint support")?;
    assert!(support.snapshot_disk && support.fork_disk, "{support:?}");
    assert!(!support.snapshot_memory && !support.suspend_memory && !support.fork_memory);
    assert!(
        support
            .snapshot_memory_blocker
            .contains("disk snapshots only"),
        "{}",
        support.snapshot_memory_blocker
    );
    assert_eq!(support.suspend_default, "disk");
    assert!(!support.publish && !support.spread);
    assert!(!support.spread_blocker.is_empty());

    cluster
        .svc
        .heartbeat(Request::new(HeartbeatRequest {
            id: NODE.into(),
            pressure_actions: vec![PressureAction {
                allocation_id: alloc,
                job_id: JOB.into(),
                step: "evict-bursting".into(),
                used_bytes: 900 << 20,
                reserved_bytes: 512 << 20,
                detail: "evicted by evict-bursting on suspend-node: using 900 MiB of a 512 MiB \
                         reservation"
                    .into(),
                hint: "raise resources.memory toward real usage".into(),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await?;

    let info = cluster.info().await?;
    let stop = info.last_stop.context("the eviction is recorded")?;
    assert_eq!(stop.cause, "node_pressure");
    assert!(stop.detail.contains("900 MiB"), "{}", stop.detail);
    assert_eq!(stop.hint, "raise resources.memory toward real usage");
    assert_eq!(stop.allocation_id, alloc);
    assert_eq!(stop.node_id, NODE);
    assert!(!stop.at.is_empty());
    assert!(
        info.checkpoint_support.is_none(),
        "nothing to checkpoint without a running allocation"
    );
    let pressure = cluster.events("node.pressure")?;
    assert_eq!(pressure.len(), 1);
    assert_eq!(pressure[0]["instance_id"], ID);

    // ListInstances carries the same record.
    let listed = cluster
        .svc
        .list_instances(Request::new(legion::structs::ListInstancesRequest {
            job_id: JOB.into(),
            group: None,
        }))
        .await?
        .into_inner()
        .instances;
    let row = listed
        .iter()
        .find(|instance| instance.instance_id == ID)
        .context("listed")?;
    assert_eq!(
        row.last_stop.as_ref().map(|stop| stop.cause.as_str()),
        Some("node_pressure")
    );

    // A later stop replaces it with the generic sentence for its cause.
    cluster.reschedule()?;
    cluster
        .svc
        .stop_instance(Request::new(StopInstanceRequest {
            instance_id: ID.into(),
            idempotency_key: None,
        }))
        .await?;
    let stop = cluster.info().await?.last_stop.context("stop recorded")?;
    assert_eq!(stop.cause, "stop");
    assert!(stop.hint.is_empty());
    Ok(())
}

// ---------------------------------------------------------------------------
// Capacity for resumes: partial holds, the node floor, make-room.
// ---------------------------------------------------------------------------

/// `job` with explicit resources on its task.
fn job_with(runtime: &str, suspend: Option<SuspendConfig>, resources: Value) -> JobConfig {
    let mut config = job(runtime, suspend);
    config.tasks[0].resources = resources;
    config
}

fn held_row(cluster: &Cluster) -> R<Option<legion::structs::Allocation>> {
    Ok(cluster
        .store()
        .allocs_for_node(NODE)?
        .into_iter()
        .find(|allocation| allocation.status == "suspended"))
}

/// `reserve = 0.25` keeps a quarter of memory and cpu charged to the node,
/// shown on `instance get` and `node usage`, and the resume releases it.
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_reserve_holds_its_share_and_the_resume_releases_it() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job_with(
        "exec",
        Some(SuspendConfig {
            reserve: Some(SuspendReserveConfig::Ratio(0.25)),
            ..Default::default()
        }),
        json!({ "memory": 2048, "cpu": 1000, "memoryMax": 4096 }),
    ))?;
    let first = cluster.active_instance()?;
    cluster.suspended(first).await?;

    let held = held_row(&cluster)?.context("a held row")?;
    assert_eq!(held.id, first);
    assert_eq!(held.resources["memory"], json!(512.0));
    assert_eq!(held.resources["cpu"], json!(250.0));
    assert_eq!(held.resources["allocations"], json!(0));
    let info = cluster.info().await?;
    assert_eq!(info.state, "suspended");
    assert_eq!(info.held_memory_mib, Some(512.0));
    assert_eq!(info.held_cpu_mhz, Some(250.0));

    let usage = cluster.node_usage().await?;
    let suspended = usage.suspended.context("suspended summary")?;
    assert_eq!((suspended.instances, suspended.holding), (1, 1));
    assert_eq!(suspended.held_memory_mib, 512.0);
    assert_eq!(usage.memory.context("memory")?.reserved, 512.0);
    assert!(usage.allocations.iter().any(|row| row.suspended));

    // Not dispatched, and the scheduler neither culls nor re-places it.
    assert!(cluster
        .heartbeat(Vec::new())
        .await?
        .desired_allocations
        .is_empty());
    cluster.reschedule()?;
    assert_eq!(cluster.store().allocs_for_node(NODE)?.len(), 1);

    cluster.start(false).await?;
    cluster.reschedule()?;
    let allocs = cluster.store().allocs_for_node(NODE)?;
    assert_eq!(allocs.len(), 1, "the share became the restore");
    assert_eq!(allocs[0].status, "placed");
    assert_eq!(allocs[0].resources["memory"], json!(2048));
    let info = cluster.info().await?;
    assert_eq!(info.held_memory_mib, None);
    Ok(())
}

/// Three 800 MHz instances on a 2000 MHz node: `ID` suspends, `user-3`
/// takes its room, and `ID`'s resume no longer fits. The make-room sweep
/// suspends the quietest suspendable instance, and the resume is placed
/// once that suspend lands.
async fn full_node_with_a_waiting_resume(suspend: SuspendConfig) -> R<(Cluster, i64, i64)> {
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job_with(
        "exec",
        Some(suspend),
        json!({ "memory": 256, "cpu": 800 }),
    ))?;
    let first = cluster.active_instance()?;
    let second = cluster.active_instance_named("user-2")?;
    cluster.suspended(first).await?;
    let third = cluster.active_instance_named("user-3")?;
    cluster.start(false).await?;
    cluster.reschedule()?;
    let info = cluster.info().await?;
    assert_eq!(info.pending_op.as_deref(), Some("resuming"));
    assert_eq!(info.current_alloc_id, None, "the node is full");
    Ok((cluster, second, third))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resume_on_a_full_node_makes_room_by_suspending_the_quietest() -> R<()> {
    let (cluster, second, third) = full_node_with_a_waiting_resume(SuspendConfig {
        on_pressure: Some(true),
        ..Default::default()
    })
    .await?;
    let now = chrono::Utc::now().timestamp();
    cluster
        .report_usage(vec![
            idle_usage(second, now - 600, 0),
            idle_usage(third, now - 30, 0),
        ])
        .await?;
    let usage = cluster.svc.usage_handle();
    let mut state = MakeRoomState::with_cooldown(std::time::Duration::ZERO);

    let swept = make_room_sweep(cluster.store(), &usage, &mut state);
    assert_eq!(swept.started.len(), 1, "{swept:?}");
    assert_eq!(swept.started[0].0, ID);
    assert_eq!(
        swept.started[0].1, "user-2",
        "quiet since 600s ago goes first"
    );
    let victim = cluster.info_of("user-2").await?;
    assert_eq!(victim.pending_op.as_deref(), Some("suspending"));
    assert_eq!(victim.suspend_reason.as_deref(), Some("make_room"));
    let make_room = cluster.events("instance.make_room")?;
    assert_eq!(make_room.len(), 1);
    assert_eq!(make_room[0]["instanceId"], ID);

    // Nothing more while that suspend is in flight.
    let again = make_room_sweep(cluster.store(), &usage, &mut state);
    assert!(again.started.is_empty(), "{again:?}");
    assert!(
        again
            .waiting
            .iter()
            .any(|(_, why)| why.contains("already suspending")),
        "{again:?}"
    );

    // The capture lands: the resume fits and is queued for placement.
    cluster
        .heartbeat(vec![capture_ok(&swept.started[0].2, second, "disk")])
        .await?;
    assert_eq!(cluster.info_of("user-2").await?.state, "suspended");
    let suspended = cluster.events("instance.suspended")?;
    assert!(suspended.iter().any(|meta| meta["reason"] == "make_room"));
    let nudged = make_room_sweep(cluster.store(), &usage, &mut state);
    assert_eq!(nudged.nudged, vec![ID.to_string()], "{nudged:?}");
    cluster.reschedule()?;
    let info = cluster.info().await?;
    assert!(info.current_alloc_id.is_some(), "the resume was placed");
    assert_eq!(cluster.info_of("user-3").await?.state, "active");
    Ok(())
}

/// The anti-thrash and eligibility rules: instances that became active
/// within the cooldown are never chosen, work without a `suspend` block is
/// never touched, more important bands are never touched, and a round that
/// cannot make enough room suspends nothing.
#[tokio::test(flavor = "multi_thread")]
async fn make_room_respects_the_cooldown_bands_and_never_does_a_useless_round() -> R<()> {
    // Recently active instances are off limits under the real cooldown.
    let (cluster, second, third) = full_node_with_a_waiting_resume(SuspendConfig {
        on_pressure: Some(true),
        ..Default::default()
    })
    .await?;
    let usage = cluster.svc.usage_handle();
    let now = chrono::Utc::now().timestamp();
    cluster
        .report_usage(vec![
            idle_usage(second, now - 600, 0),
            idle_usage(third, now - 600, 0),
        ])
        .await?;
    let swept = make_room_sweep(cluster.store(), &usage, &mut MakeRoomState::default());
    assert!(swept.started.is_empty(), "{swept:?}");
    assert!(
        swept
            .waiting
            .iter()
            .any(|(_, why)| why.contains("would not make room")),
        "{swept:?}"
    );

    // Not opted in (no onPressure, no idle policy): never suspended.
    let (cluster, second, third) =
        full_node_with_a_waiting_resume(SuspendConfig::default()).await?;
    let usage = cluster.svc.usage_handle();
    cluster
        .report_usage(vec![
            idle_usage(second, now - 600, 0),
            idle_usage(third, now - 600, 0),
        ])
        .await?;
    let mut state = MakeRoomState::with_cooldown(std::time::Duration::ZERO);
    let swept = make_room_sweep(cluster.store(), &usage, &mut state);
    assert!(swept.started.is_empty(), "{swept:?}");
    assert_eq!(cluster.info_of("user-2").await?.state, "active");

    // A more important band is never suspended for a normal-band resume:
    // the only instances on the node are `high`, so nothing is eligible.
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    let on_pressure = SuspendConfig {
        on_pressure: Some(true),
        ..Default::default()
    };
    cluster.submit(job_with(
        "exec",
        Some(on_pressure.clone()),
        json!({ "memory": 256, "cpu": 800 }),
    ))?;
    let first = cluster.active_instance()?;
    cluster.suspended(first).await?;
    let mut vip = job_with(
        "exec",
        Some(on_pressure),
        json!({ "memory": 256, "cpu": 800 }),
    );
    vip.name = "vip".into();
    vip.priority = legion::structs::Band::High;
    cluster.submit(vip)?;
    for id in ["v1", "v2"] {
        cluster
            .store()
            .create_keyed_instance("vip", "default", TASK, Some(id))?;
    }
    cluster.reschedule_job("vip")?;
    cluster.start(false).await?;
    cluster.reschedule()?;
    assert_eq!(cluster.info().await?.current_alloc_id, None, "full of vip");
    let usage = cluster.svc.usage_handle();
    let swept = make_room_sweep(cluster.store(), &usage, &mut state);
    assert!(swept.started.is_empty(), "{swept:?}");
    assert!(
        swept
            .waiting
            .iter()
            .any(|(_, why)| why.contains("all 0 eligible")),
        "{swept:?}"
    );
    assert_eq!(cluster.info_of("v1").await?.state, "active");
    Ok(())
}

/// A node whose `snapshots` budget cannot hold one more checkpoint takes no
/// new instance of a task that suspends to memory, with a diagnostic that
/// names the budget, and takes it again once the budget has room. A resume
/// is never held back: its checkpoint already exists.
#[tokio::test(flavor = "multi_thread")]
async fn a_full_snapshot_budget_keeps_new_memory_suspend_instances_off_the_node() -> R<()> {
    use legion::structs::{ContentLocationReport, SnapshotBudget};
    const GIB: u64 = 1024 * 1024 * 1024;
    let cluster = Cluster::new()?;
    cluster.add_node(json!(["checkpoint"]))?;
    cluster.submit(job_with(
        "sandbox",
        Some(SuspendConfig {
            checkpoint: Some("memory".into()),
            ..Default::default()
        }),
        json!({ "memory": 1024 }),
    ))?;
    let report_budget = |pinned: u64| {
        cluster.svc.heartbeat(Request::new(HeartbeatRequest {
            id: NODE.into(),
            content_locations: Some(ContentLocationReport {
                epoch: 1,
                base_epoch: 0,
                full: true,
                snapshot_budget: Some(SnapshotBudget {
                    budget_bytes: 10 * GIB,
                    used_bytes: pinned,
                    pinned_bytes: pinned,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }))
    };
    // 512 MiB of room, and the guest is 1 GiB.
    report_budget(10 * GIB - GIB / 2).await?;
    cluster
        .store()
        .create_keyed_instance(JOB, "default", TASK, Some(ID))?;
    let config_id = cluster.store().get_job_config(JOB)?.id();
    cluster
        .store()
        .enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
    let eval = cluster.store().get_next_eval()?.context("eval")?;
    let err = eval.handle(cluster.store()).expect_err("no node has room");
    cluster.store().set_eval_status(&eval.id, "failed")?;
    let message = format!("{err:#}");
    assert!(
        message.contains("snapshots content budget is full")
            && message.contains("less than 1024 MiB free")
            && message.contains("content.budgets.snapshots"),
        "{message}"
    );
    assert_eq!(cluster.info().await?.current_alloc_id, None);

    let usage = cluster.node_usage().await?;
    let budget = usage
        .suspended
        .and_then(|suspended| suspended.snapshot_budget)
        .context("the budget is on node usage")?;
    assert_eq!(budget.budget_bytes, 10 * GIB);

    // Room again: it places.
    report_budget(GIB).await?;
    cluster.reschedule()?;
    assert!(cluster.info().await?.current_alloc_id.is_some());
    Ok(())
}
