//! Snapshots end to end on the manager (docs/suspend-and-resume.md): the
//! RPCs, the heartbeat checkpoint channel, expiry, and restore placement.
//!
//! The worker half (capture through the content store and volume drivers,
//! restore at start) is covered against a real `exec` task and `local-dir`
//! volume in `src/worker/task_manager.rs`
//! (`an_exec_disk_snapshot_round_trips_through_capture_and_restore`). Here
//! the heartbeat stands in for the worker, so every manager-side transition
//! is driven exactly as a worker would drive it.

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::events::SchedulerEventFilter;
use legion::storage::store::Store;
use legion::structs::manager_server::Manager;
use legion::structs::snapshot::SnapshotsConfig;
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, job::StatefulConfig, job::StatefulStorageConfig,
    job::TaskConfig, CreateSnapshotRequest, DeleteSnapshotRequest, GetInstanceRequest,
    GetSnapshotRequest, HeartbeatReply, HeartbeatRequest, InstanceMode, ListSnapshotsRequest, Node,
    NodeMsg, RestoreInstanceRequest, SnapshotReport, Submission,
};
use serde_json::Value;
use tonic::{Code, Request};

const NODE: &str = "snap-node";
const JOB: &str = "workspaces";
const TASK: &str = "workspace";
const CPU_MODEL: &str = "6:106:6";

struct Cluster {
    svc: ManagerDaemonService,
    eval_seq: u64,
}

fn node_fingerprint(cpu_model: &str) -> Value {
    json!({
        "arch": "x86_64",
        "memory_total": "68719476736",
        "disk_total_space": "214748364800",
        "disk_available_space": "107374182400",
        "cpus": { "cpu-1": { "frequency": 2000.0 }, "cpu-2": { "frequency": 2000.0 } },
        "cpu.vendor": "GenuineIntel",
        "cpu.model": cpu_model,
        "cpu.features": "3f9a0c21",
        "sandbox.vmm": "firecracker",
        "sandbox.vmm.version": "1.7.0",
        "sandbox.vmm.firecracker.version": "1.7.0",
        "volumeDrivers": {
            "local-dir": {
                "apiVersion": 1, "name": "local-dir", "nodeLocal": true,
                "ephemeral": false, "accessModes": ["single-writer"], "available": true,
                "ops": ["snapshot", "restore", "clone", "snapshot-delete"]
            }
        }
    })
}

impl Cluster {
    fn new() -> R<Self> {
        let dir = tempfile::Builder::new()
            .prefix("legion-snapshot-it-")
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
            eval_seq: 0,
        })
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    fn add_node(&self, id: &str, cpu_model: &str, capabilities: Value) -> R<()> {
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
            fingerprint_attrs: node_fingerprint(cpu_model),
            dynamic_attrs: json!({ "buildVersion": "0.9.0", "capabilities": capabilities }),
            health_checks: json!({}),
        }))?;
        Ok(())
    }

    fn submit(&self, config: JobConfig) -> R<String> {
        self.store().handle_job_submit(
            &config,
            &[Submission {
                path: format!("{}.json", config.name),
                content: "{}".to_string(),
                is_primary: true,
            }],
        )?;
        Ok(self.store().get_job_config(&config.name)?.id())
    }

    fn reschedule(&mut self) -> R<()> {
        let config_id = self.store().get_job_config(JOB)?.id();
        self.eval_seq += 1;
        self.store()
            .enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
        // RPCs enqueue evals of their own; drain everything queued.
        let mut last = Ok(());
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
            if let Err(err) = outcome {
                last = Err(err);
            }
        }
        last
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

    /// A placed, active keyed instance of [`workspace_job`].
    fn active_instance(&mut self, id: &str) -> R<i64> {
        self.store()
            .create_keyed_instance(JOB, "default", TASK, Some(id))?;
        self.reschedule()?;
        let instance = self.store().get_instance(id)?.context("instance")?;
        instance
            .current_alloc_id
            .context("the instance should be placed")
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
}

fn workspace_job(
    runtime: &str,
    snapshots: Option<SnapshotsConfig>,
    network: Option<Value>,
) -> JobConfig {
    let mut task = TaskConfig {
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
        snapshots,
        ..TaskConfig::new(TASK, runtime)
    };
    task.network = network.map(|value| serde_json::from_value(value).expect("network config"));
    JobConfig {
        priority: Default::default(),
        name: JOB.to_string(),
        tasks: vec![task],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    }
}

/// The manifest a worker reports for a capture of `allocation_id`.
fn manifest(snapshot_id: &str, allocation_id: i64, kind: &str, cpu_model: Option<&str>) -> String {
    let mut manifest = json!({
        "apiVersion": 1,
        "id": snapshot_id,
        "jobId": JOB,
        "task": TASK,
        "originInstance": "user-42",
        "originAllocId": allocation_id,
        "runtime": "exec",
        "kind": kind,
        "createdAt": "2026-10-10T00:00:00Z",
        "volumes": [{
            "id": snapshot_id, "volumeId": "vol-1", "driver": "local-dir",
            "mode": "filesystem", "size": 4096
        }],
    });
    if let Some(model) = cpu_model {
        manifest["compat"] = json!({
            "vmm": { "type": "firecracker", "version": "1.7.0" },
            "cpu": { "vendor": "GenuineIntel", "model": model, "features": "3f9a0c21" }
        });
    }
    manifest.to_string()
}

fn capture_report(snapshot_id: &str, allocation_id: i64, manifest_json: String) -> SnapshotReport {
    SnapshotReport {
        snapshot_id: snapshot_id.into(),
        op: "capture".into(),
        ok: true,
        manifest_json,
        duration_ms: 12,
        total_bytes: 4096,
        allocation_id,
        ..Default::default()
    }
}

async fn create(
    cluster: &Cluster,
    kind: &str,
    key: Option<&str>,
) -> Result<legion::structs::CreateSnapshotReply, tonic::Status> {
    cluster
        .svc
        .create_snapshot(Request::new(CreateSnapshotRequest {
            instance_id: "user-42".into(),
            kind: kind.into(),
            name: None,
            idempotency_key: key.map(str::to_string),
            publish: false,
        }))
        .await
        .map(|reply| reply.into_inner())
}

/// Capture one disk snapshot all the way to `local`, returning its id.
async fn captured(cluster: &Cluster, allocation_id: i64, cpu_model: Option<&str>) -> R<String> {
    let snapshot = create(cluster, "disk", None)
        .await?
        .snapshot
        .context("snapshot")?;
    let id = snapshot.snapshot_id;
    cluster
        .heartbeat(vec![capture_report(
            &id,
            allocation_id,
            manifest(
                &id,
                allocation_id,
                if cpu_model.is_some() {
                    "memory"
                } else {
                    "disk"
                },
                cpu_model,
            ),
        )])
        .await?;
    Ok(id)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_disk_snapshot_rides_the_heartbeat_channel_from_capture_to_delete() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE, CPU_MODEL, json!(["checkpoint"]))?;
    cluster.submit(workspace_job(
        "exec",
        Some(SnapshotsConfig::default()),
        None,
    ))?;
    let allocation_id = cluster.active_instance("user-42")?;

    // Create, then retry with the same key: one snapshot.
    let first = create(&cluster, "disk", Some("snap-key-1")).await?;
    assert!(!first.deduplicated);
    let snapshot = first.snapshot.context("snapshot")?;
    assert_eq!(snapshot.state, "capturing");
    assert_eq!(snapshot.node_id, NODE);
    assert_eq!(snapshot.allocation_id, allocation_id);
    let again = create(&cluster, "disk", Some("snap-key-1")).await?;
    assert!(again.deduplicated);
    assert_eq!(
        again.snapshot.context("snapshot")?.snapshot_id,
        snapshot.snapshot_id
    );
    // A second capture of the same instance waits for the first.
    let busy = create(&cluster, "disk", None).await.unwrap_err();
    assert_eq!(busy.code(), Code::AlreadyExists);
    assert!(
        busy.message().contains("already being captured"),
        "{}",
        busy.message()
    );

    // The node is asked, and asked again until it reports.
    for _ in 0..2 {
        let reply = cluster.heartbeat(Vec::new()).await?;
        assert_eq!(reply.checkpoint_requests.len(), 1);
        let request = &reply.checkpoint_requests[0];
        assert_eq!(request.snapshot_id, snapshot.snapshot_id);
        assert_eq!(request.op, "capture");
        assert_eq!(request.allocation_id, allocation_id);
        assert_eq!(request.kind, "disk");
        assert_eq!(request.then, "resume");
    }

    let report = capture_report(
        &snapshot.snapshot_id,
        allocation_id,
        manifest(&snapshot.snapshot_id, allocation_id, "disk", None),
    );
    let reply = cluster.heartbeat(vec![report.clone()]).await?;
    assert!(
        reply.checkpoint_requests.is_empty(),
        "applied in the same beat"
    );
    // A duplicate report (the worker re-sends what it thinks is unapplied)
    // changes nothing and emits nothing.
    cluster.heartbeat(vec![report]).await?;
    assert_eq!(cluster.events("snapshot.created")?.len(), 1);

    let got = cluster
        .svc
        .get_snapshot(Request::new(GetSnapshotRequest {
            snapshot: snapshot.snapshot_id.clone(),
            job_id: None,
        }))
        .await?
        .into_inner()
        .snapshot
        .context("snapshot")?;
    assert_eq!(got.state, "local");
    assert_eq!(got.total_bytes, 4096);
    assert!(got.manifest_json.contains("vol-1"));
    let listed = cluster
        .svc
        .list_snapshots(Request::new(ListSnapshotsRequest::default()))
        .await?
        .into_inner()
        .snapshots;
    assert_eq!(listed.len(), 1);
    assert!(listed[0].manifest_json.is_empty(), "lists stay small");

    // Delete: `deleting` until the node confirms, then gone.
    let deleted = cluster
        .svc
        .delete_snapshot(Request::new(DeleteSnapshotRequest {
            snapshot_id: snapshot.snapshot_id.clone(),
            idempotency_key: Some("del-1".into()),
        }))
        .await?
        .into_inner();
    assert_eq!(
        deleted.message,
        format!("Deleting snapshot {}", snapshot.snapshot_id)
    );
    let reply = cluster.heartbeat(Vec::new()).await?;
    assert_eq!(reply.checkpoint_requests.len(), 1);
    assert_eq!(reply.checkpoint_requests[0].op, "delete");
    assert!(reply.checkpoint_requests[0].manifest_json.contains("vol-1"));
    cluster
        .heartbeat(vec![SnapshotReport {
            snapshot_id: snapshot.snapshot_id.clone(),
            op: "delete".into(),
            ok: true,
            ..Default::default()
        }])
        .await?;
    let gone = cluster
        .svc
        .get_snapshot(Request::new(GetSnapshotRequest {
            snapshot: snapshot.snapshot_id.clone(),
            job_id: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(gone.code(), Code::NotFound);
    assert_eq!(cluster.events("snapshot.deleted")?.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_capture_keeps_its_reason_and_emits_snapshot_failed() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE, CPU_MODEL, json!(["checkpoint"]))?;
    cluster.submit(workspace_job(
        "exec",
        Some(SnapshotsConfig::default()),
        None,
    ))?;
    let allocation_id = cluster.active_instance("user-42")?;
    let snapshot = create(&cluster, "disk", None)
        .await?
        .snapshot
        .context("snapshot")?;
    cluster
        .heartbeat(vec![SnapshotReport {
            snapshot_id: snapshot.snapshot_id.clone(),
            op: "capture".into(),
            ok: false,
            error: "failed to snapshot volume `data` (local-dir): disk full".into(),
            allocation_id,
            ..Default::default()
        }])
        .await?;
    let got = cluster
        .store()
        .get_snapshot(&snapshot.snapshot_id)?
        .context("snapshot")?;
    assert_eq!(got.state.as_str(), "failed");
    assert!(got.error.as_deref().unwrap_or("").contains("disk full"));
    assert_eq!(cluster.events("snapshot.failed")?.len(), 1);
    // A failed row has nothing on a node: delete removes it at once.
    let deleted = cluster
        .svc
        .delete_snapshot(Request::new(DeleteSnapshotRequest {
            snapshot_id: snapshot.snapshot_id.clone(),
            idempotency_key: None,
        }))
        .await?
        .into_inner();
    assert_eq!(
        deleted.message,
        format!("Deleted snapshot {}", snapshot.snapshot_id)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn keep_and_ttl_expire_old_snapshots_through_the_node() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE, CPU_MODEL, json!(["checkpoint"]))?;
    cluster.submit(workspace_job(
        "exec",
        Some(SnapshotsConfig {
            ttl: Some("1h".into()),
            keep: Some(2),
            memory: None,
        }),
        None,
    ))?;
    let allocation_id = cluster.active_instance("user-42")?;

    let oldest = captured(&cluster, allocation_id, None).await?;
    let middle = captured(&cluster, allocation_id, None).await?;
    let newest = captured(&cluster, allocation_id, None).await?;

    // keep = 2: the third capture moved the oldest to `deleting`.
    let state = |id: &str| -> R<String> {
        Ok(cluster
            .store()
            .get_snapshot(id)?
            .map(|s| s.state.as_str().to_string())
            .unwrap_or_else(|| "gone".into()))
    };
    assert_eq!(state(&oldest)?, "deleting");
    assert_eq!(state(&middle)?, "local");
    assert_eq!(state(&newest)?, "local");
    let reply = cluster.heartbeat(Vec::new()).await?;
    let deletes: Vec<&str> = reply
        .checkpoint_requests
        .iter()
        .filter(|request| request.op == "delete")
        .map(|request| request.snapshot_id.as_str())
        .collect();
    assert_eq!(deletes, vec![oldest.as_str()]);

    // ttl = 1h: every snapshot carries an expiry, and the sweep moves the
    // expired ones to `deleting`.
    let expires = cluster
        .store()
        .get_snapshot(&middle)?
        .context("snapshot")?
        .expires_at
        .context("ttl sets expires_at")?;
    assert!(expires > chrono::Utc::now().timestamp() + 3000);
    let (expired, removed) = cluster.store().expire_snapshots(expires + 1)?;
    assert!(removed.is_empty());
    assert!(
        expired.contains(&middle) && expired.contains(&newest),
        "{expired:?}"
    );
    assert_eq!(state(&newest)?, "deleting");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn restore_is_pinned_to_the_snapshot_node_and_refused_on_a_compat_mismatch() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE, CPU_MODEL, json!(["checkpoint"]))?;
    cluster.submit(workspace_job(
        "exec",
        Some(SnapshotsConfig::default()),
        None,
    ))?;
    let allocation_id = cluster.active_instance("user-42")?;

    // A snapshot recorded on a CPU this node is not.
    let foreign = captured(&cluster, allocation_id, Some("6:143:8")).await?;
    let reply = cluster
        .svc
        .restore_instance(Request::new(RestoreInstanceRequest {
            instance_id: "user-42".into(),
            snapshot: foreign.clone(),
            idempotency_key: Some("restore-1".into()),
        }))
        .await?
        .into_inner();
    assert_eq!(reply.snapshot_id, foreign);
    let err = cluster.reschedule().unwrap_err();
    let text = format!("{err:#}");
    assert!(
        text.contains("cpu.model"),
        "the reason names the attribute: {text}"
    );
    assert!(
        text.contains("6:143:8") && text.contains(CPU_MODEL),
        "{text}"
    );
    assert!(text.contains(&foreign), "{text}");
    let instance = cluster
        .store()
        .get_instance("user-42")?
        .context("instance")?;
    assert_eq!(
        instance.current_alloc_id, None,
        "never placed onto the wrong CPU"
    );
    assert_eq!(instance.restore_from.as_deref(), Some(foreign.as_str()));
    // The replaced allocation was told why it is stopping.
    let reply = cluster.heartbeat(Vec::new()).await?;
    assert!(reply
        .stop_notices
        .iter()
        .any(|notice| notice.allocation_id == allocation_id && notice.cause == "restore"));
    // Retrying with the key replays the first answer.
    let replay = cluster
        .svc
        .restore_instance(Request::new(RestoreInstanceRequest {
            instance_id: "user-42".into(),
            snapshot: foreign.clone(),
            idempotency_key: Some("restore-1".into()),
        }))
        .await?
        .into_inner();
    assert!(replay.deduplicated);
    assert_eq!(replay.snapshot_id, foreign);

    // Stop clears the restore source; start it again and capture a snapshot
    // on this CPU.
    cluster
        .svc
        .stop_instance(Request::new(legion::structs::StopInstanceRequest {
            instance_id: "user-42".into(),
            idempotency_key: None,
        }))
        .await?;
    assert_eq!(
        cluster
            .store()
            .get_instance("user-42")?
            .context("instance")?
            .restore_from,
        None
    );
    cluster
        .svc
        .start_instance(Request::new(legion::structs::StartInstanceRequest {
            cold: false,
            instance_id: "user-42".into(),
            idempotency_key: None,
        }))
        .await?;
    cluster.reschedule()?;
    let allocation_id = cluster
        .store()
        .get_instance("user-42")?
        .context("instance")?
        .current_alloc_id
        .context("placed")?;
    let native = captured(&cluster, allocation_id, Some(CPU_MODEL)).await?;
    cluster
        .svc
        .restore_instance(Request::new(RestoreInstanceRequest {
            instance_id: "user-42".into(),
            snapshot: native.clone(),
            idempotency_key: None,
        }))
        .await?;
    cluster.reschedule()?;
    let instance = cluster
        .store()
        .get_instance("user-42")?
        .context("instance")?;
    // The id may be reused (the old row is gone); the worker handles that.
    let restored_alloc = instance.current_alloc_id.context("restored placement")?;

    // The node gets the restore payload with the allocation, and the
    // instance shows what it is restoring from until the node reports.
    let reply = cluster.heartbeat(Vec::new()).await?;
    let dispatched = reply
        .desired_allocations
        .iter()
        .find(|allocation| allocation.allocation_id == restored_alloc)
        .context("restored allocation dispatched to the snapshot's node")?;
    let payload = dispatched.restore.as_ref().context("restore payload")?;
    assert_eq!(payload.snapshot_id, native);
    assert!(payload.manifest_json.contains("vol-1"));
    let info = cluster
        .svc
        .get_instance(Request::new(GetInstanceRequest {
            instance_id: "user-42".into(),
        }))
        .await?
        .into_inner()
        .instance
        .context("instance")?;
    assert_eq!(info.restore_snapshot_id.as_deref(), Some(native.as_str()));

    let reply = cluster
        .heartbeat(vec![SnapshotReport {
            snapshot_id: native.clone(),
            op: "restore".into(),
            ok: true,
            duration_ms: 40,
            allocation_id: restored_alloc,
            ..Default::default()
        }])
        .await?;
    let dispatched = reply
        .desired_allocations
        .iter()
        .find(|allocation| allocation.allocation_id == restored_alloc)
        .context("still dispatched")?;
    assert!(dispatched.restore.is_none(), "cleared once reported");
    assert_eq!(
        cluster
            .store()
            .get_instance("user-42")?
            .context("instance")?
            .restore_from,
        None
    );
    let restored = cluster.events("snapshot.restored")?;
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0]["snapshotId"], json!(native));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_restore_emits_the_workers_reason() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE, CPU_MODEL, json!(["checkpoint"]))?;
    cluster.submit(workspace_job(
        "exec",
        Some(SnapshotsConfig::default()),
        None,
    ))?;
    let allocation_id = cluster.active_instance("user-42")?;
    let snapshot = captured(&cluster, allocation_id, None).await?;
    cluster
        .svc
        .restore_instance(Request::new(RestoreInstanceRequest {
            instance_id: "user-42".into(),
            snapshot: snapshot.clone(),
            idempotency_key: None,
        }))
        .await?;
    cluster.reschedule()?;
    let restored_alloc = cluster
        .store()
        .get_instance("user-42")?
        .context("instance")?
        .current_alloc_id
        .context("placed")?;
    let reason = "restore needs TAP device tap7: it exists and another process holds it";
    cluster
        .heartbeat(vec![SnapshotReport {
            snapshot_id: snapshot.clone(),
            op: "restore".into(),
            ok: false,
            error: reason.into(),
            allocation_id: restored_alloc,
            ..Default::default()
        }])
        .await?;
    let failed = cluster.events("snapshot.restore_failed")?;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["error"], json!(reason));
    // Cleared, so the replacement after the allocation fails boots cold
    // instead of retrying the same restore forever.
    assert_eq!(
        cluster
            .store()
            .get_instance("user-42")?
            .context("instance")?
            .restore_from,
        None
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_preconditions_fail_with_actionable_reasons() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE, CPU_MODEL, json!([]))?;
    cluster.submit(workspace_job(
        "exec",
        Some(SnapshotsConfig::default()),
        None,
    ))?;
    cluster.active_instance("user-42")?;

    // An old worker: refused by name and version.
    let err = create(&cluster, "disk", None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("worker version 0.9.0"),
        "{}",
        err.message()
    );
    assert!(
        err.message().contains("upgrade the worker"),
        "{}",
        err.message()
    );
    assert!(cluster
        .heartbeat(Vec::new())
        .await?
        .checkpoint_requests
        .is_empty());

    // A capable worker, but `exec` has no memory to capture.
    let mut node = cluster.store().get_node(NODE)?;
    node.dynamic_attrs = json!({ "buildVersion": "0.9.0", "capabilities": ["checkpoint"] });
    cluster.store().insert_node(NodeMsg::from(node))?;
    let err = create(&cluster, "memory", None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("supports disk snapshots only"),
        "{}",
        err.message()
    );
    assert!(
        err.message().contains("Hint: use --disk"),
        "{}",
        err.message()
    );

    let err = create(&cluster, "everything", None).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    Ok(())
}

/// Gated plan Phase 1: a memory snapshot of a gated sandbox needs a VMM
/// that can move the restored guest's vsock (Firecracker 1.16.1 or any
/// cloud-hypervisor). On an older Firecracker it is refused at call time with
/// the version and the fix; on a new one it is taken, with a note that the
/// live capture resets the guest's open connections.
#[tokio::test(flavor = "multi_thread")]
async fn a_memory_snapshot_of_a_gated_sandbox_needs_firecracker_1_16_1() -> R<()> {
    let mut cluster = Cluster::new()?;
    cluster.add_node(NODE, CPU_MODEL, json!(["checkpoint"]))?;
    cluster.submit(workspace_job(
        "sandbox",
        Some(SnapshotsConfig::default()),
        Some(json!({ "mode": "gated" })),
    ))?;
    cluster.active_instance("user-42")?;
    let err = create(&cluster, "memory", None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "instance user-42 is gated, and node snap-node runs Firecracker 1.7.0, which cannot \
         move a restored guest's vsock to a new socket (that needs Firecracker 1.16.1 or \
         later)\nHint: upgrade Firecracker on the node, or use --disk"
    );

    let mut node = cluster.store().get_node(NODE)?;
    node.fingerprint_attrs["sandbox.vmm.version"] = json!("1.16.1");
    node.fingerprint_attrs["sandbox.vmm.firecracker.version"] = json!("1.16.1");
    cluster.store().insert_node(NodeMsg::from(node))?;
    let reply = create(&cluster, "memory", None).await?;
    assert_eq!(
        reply.notes,
        vec![
            "instance user-42 is gated on Firecracker, so this capture resets its open network \
             connections (the guest keeps running)"
                .to_string()
        ]
    );
    Ok(())
}

/// Gated plan Phase 1: `job validate` accepts a gated sandbox that asks for
/// memory checkpoints.
#[test]
fn job_validation_accepts_memory_checkpoints_on_a_gated_sandbox() {
    let job = workspace_job(
        "sandbox",
        Some(SnapshotsConfig::default()),
        Some(json!({ "mode": "gated" })),
    );
    legion::task_plugins::validate_job_config(&job).expect("gated memory snapshots validate");

    // Disk-only snapshots on the same task are fine.
    let job = workspace_job(
        "sandbox",
        Some(SnapshotsConfig {
            memory: Some(false),
            ..Default::default()
        }),
        Some(json!({ "mode": "gated" })),
    );
    legion::task_plugins::validate_job_config(&job).expect("disk-only snapshots validate");

    // And the block itself is checked.
    let job = workspace_job(
        "exec",
        Some(SnapshotsConfig {
            keep: Some(0),
            ..Default::default()
        }),
        None,
    );
    let err = legion::task_plugins::validate_job_config(&job).unwrap_err();
    assert!(format!("{err:#}").contains("snapshots.keep"), "{err:#}");
}
