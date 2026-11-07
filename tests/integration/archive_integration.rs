//! Archived idle instances on the manager (sandbox-scaling plan, Phase 5;
//! docs/suspend-and-resume.md, "Archived instances"): a suspended keyed
//! instance whose task sets `suspend.archive` is archived in the entry that
//! publishes its checkpoint, a resume reads the archive manifest back, a lost
//! manifest follows `onRestoreFailure`, `manifestOnly` keeps no row for an
//! unnamed instance, and `include_idle` pages through the bucket.
//!
//! The heartbeat stands in for the worker, as in
//! `published_resume_integration.rs`. Playing the worker, the test writes
//! the archive manifest exactly as `run_publish` does (with
//! `ArchiveManifest::render`) into a `file://` content store, which the
//! manager then reads, lists, and deletes for real.

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use legion::content_publish::document_url;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::events::SchedulerEventFilter;
use legion::storage::store::Store;
use legion::structs::archive::ArchiveManifest;
use legion::structs::manager_server::Manager;
use legion::structs::suspend::SuspendConfig;
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, job::StatefulConfig, job::TaskConfig,
    CreateInstanceRequest, DestroyInstanceRequest, GetInstanceRequest, HeartbeatReply,
    HeartbeatRequest, InstanceInfo, InstanceMode, ListInstancesRequest, Node, NodeMsg,
    SnapshotReport, StartInstanceRequest, Submission, SuspendInstanceRequest, WorkerConfig,
};
use serde_json::Value;
use tonic::{Code, Request};
use url::Url;

const NODE: &str = "node-a";
const JOB: &str = "workspaces";
const TASK: &str = "workspace";
const PART_BYTES: u64 = 64 * 1024 * 1024;

struct Cluster {
    svc: ManagerDaemonService,
    /// The `file://` contentStore root the worker type publishes to.
    root: Url,
}

impl Cluster {
    fn new(archive: &str, on_restore_failure: Option<&str>) -> R<Self> {
        let dir = tempfile::Builder::new()
            .prefix("legion-archive-it-")
            .tempdir_in(crate::support::scratch_root())?
            .keep();
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.to_str().context("path")?.to_string(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        })?;
        let content = dir.join("content");
        std::fs::create_dir_all(&content)?;
        let root = Url::from_directory_path(&content).map_err(|_| anyhow::anyhow!("root"))?;
        store.insert_worker_type(&WorkerConfig {
            name: "default".into(),
            pool: "default".into(),
            artifacts: json!({ "contentStore": { "url": root.as_str() } }).to_string(),
            ..Default::default()
        })?;
        let cluster = Cluster {
            svc: ManagerDaemonService::new(store),
            root,
        };
        cluster.add_node()?;
        cluster.submit(job(archive, on_restore_failure))?;
        Ok(cluster)
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    fn add_node(&self) -> R<()> {
        self.store().insert_node(NodeMsg::from(Node {
            id: NODE.to_string(),
            pool: "default".into(),
            status: "ready".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({
                "node_id": NODE,
                "maxAllocations": 50,
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
                "sandbox.vmm.version": "1.7.0",
            }),
            dynamic_attrs: json!({
                "buildVersion": "0.9.0",
                "capabilities": ["checkpoint", "checkpoint-fetch", "content-publish"],
            }),
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

    async fn info(&self, id: &str) -> Result<InstanceInfo, tonic::Status> {
        self.svc
            .get_instance(Request::new(GetInstanceRequest {
                instance_id: id.into(),
            }))
            .await?
            .into_inner()
            .instance
            .ok_or_else(|| tonic::Status::internal("no instance"))
    }

    async fn start(&self, id: &str) -> Result<legion::structs::StartInstanceReply, tonic::Status> {
        self.svc
            .start_instance(Request::new(StartInstanceRequest {
                instance_id: id.into(),
                idempotency_key: None,
                cold: false,
                force_move: false,
            }))
            .await
            .map(|reply| reply.into_inner())
    }

    async fn create(&self, id: Option<&str>) -> R<String> {
        Ok(self
            .svc
            .create_instance(Request::new(CreateInstanceRequest {
                job_id: JOB.into(),
                instance_id: id.map(str::to_string),
                ..Default::default()
            }))
            .await?
            .into_inner()
            .instance_id)
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

    /// Create and place `id` (or mint one), suspend it, report the capture,
    /// and play the worker's publish: write the archive manifest the
    /// manager asks for and report its URL. Returns `(instance id,
    /// checkpoint id, manifest url)`.
    async fn archived(&self, id: Option<&str>) -> R<(String, String, String)> {
        let id = self.create(id).await?;
        self.reschedule()?;
        let allocation_id = self
            .store()
            .get_instance(&id)?
            .context("instance")?
            .current_alloc_id
            .context("placed")?;
        let suspended = self
            .svc
            .suspend_instance(Request::new(SuspendInstanceRequest {
                instance_id: id.clone(),
                checkpoint: None,
                idempotency_key: None,
            }))
            .await?
            .into_inner();
        let checkpoint = suspended.snapshot_id;
        self.heartbeat(vec![SnapshotReport {
            snapshot_id: checkpoint.clone(),
            op: "capture".into(),
            ok: true,
            manifest_json: manifest(&checkpoint, None),
            duration_ms: 740,
            total_bytes: PART_BYTES,
            allocation_id,
            ..Default::default()
        }])
        .await?;
        assert_eq!(self.info(&id).await?.state, "suspended");

        let asked = self
            .heartbeat(Vec::new())
            .await?
            .checkpoint_requests
            .into_iter()
            .find(|request| request.snapshot_id == checkpoint)
            .context("a publish request")?;
        assert_eq!(asked.op, "publish");
        let instance = self.store().get_instance(&id)?.context("instance")?;
        assert_eq!(
            asked.manifest_object,
            // The namespace is always a segment, `default` included.
            format!(
                "manifests/default/{JOB}/{}/{TASK}/{id}.json",
                instance.group
            ),
            "the manager names where the archive manifest goes"
        );
        // The worker's half (`run_publish`): the parts, sealed under the
        // key the manager sent, then the manifest.
        let key_id = asked
            .part_keys
            .first()
            .context("a part key")?
            .key_id
            .clone();
        let published = manifest(&checkpoint, Some((url(), key_id)));
        let document = ArchiveManifest::render(&asked.manifest_envelope, &published)?;
        let manifest_url = document_url(&self.root, &asked.manifest_object);
        let path = Url::parse(&manifest_url)?
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("path"))?;
        std::fs::create_dir_all(path.parent().context("parent")?)?;
        std::fs::write(&path, document)?;
        self.heartbeat(vec![SnapshotReport {
            snapshot_id: checkpoint.clone(),
            op: "publish".into(),
            ok: true,
            manifest_json: published,
            duration_ms: 1_500,
            total_bytes: PART_BYTES,
            manifest_url: manifest_url.clone(),
            ..Default::default()
        }])
        .await?;
        Ok((id, checkpoint, manifest_url))
    }
}

fn job(archive: &str, on_restore_failure: Option<&str>) -> JobConfig {
    let task = TaskConfig {
        config: json!({ "image": "alpine", "command": ["sleep", "60"] }),
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
            durability: Some("store".into()),
            archive: Some(archive.into()),
            on_restore_failure: on_restore_failure.map(str::to_string),
            wake: Some(legion::structs::suspend::WakeConfig {
                port: "http".into(),
                timeout: None,
                unknown_fields: Default::default(),
            }),
            ..Default::default()
        }),
        ..TaskConfig::new(TASK, "sandbox")
    };
    JobConfig {
        priority: Default::default(),
        name: JOB.to_string(),
        tasks: vec![task],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
        pending_extensions: Default::default(),
        extensions: Default::default(),
    }
}

fn url() -> String {
    format!("file:///shared/content/sealed/{}", "cd".repeat(32))
}

/// A one-part memory manifest; once published, the part carries its `url`
/// and the checkpoint key it was sealed under.
fn manifest(snapshot_id: &str, published: Option<(String, String)>) -> String {
    let (url, key_id) = published.unzip();
    json!({
        "apiVersion": 1,
        "id": snapshot_id,
        "jobId": JOB,
        "task": TASK,
        "originAllocId": 1,
        "runtime": "sandbox",
        "kind": "memory",
        "createdAt": "2026-10-10T00:00:00Z",
        "memory": [{
            "role": "memory", "file": "memory", "digest": format!("sha256:{}", "ab".repeat(32)),
            "size": PART_BYTES, "url": url, "keyId": key_id,
        }],
        "compat": {
            "vmm": { "type": "firecracker", "version": "1.7.0" },
            "cpu": { "vendor": "GenuineIntel", "model": "6:106:6", "features": "3f9a0c21" },
        },
    })
    .to_string()
}

fn manifest_exists(url: &str) -> bool {
    Url::parse(url)
        .ok()
        .and_then(|url| url.to_file_path().ok())
        .is_some_and(|path| path.exists())
}

/// Plan §10 Phase 5: "archive and unarchive round trip, the slim row
/// refuses a duplicate create".
#[tokio::test(flavor = "multi_thread")]
async fn a_slim_archive_drops_the_snapshot_row_and_a_start_reads_it_back() -> R<()> {
    let cluster = Cluster::new("slim", None)?;
    let (id, checkpoint, manifest_url) = cluster.archived(Some("user-42")).await?;

    // Archived in the publish's own entry: no snapshot row, no allocation.
    assert!(cluster.store().get_snapshot(&checkpoint)?.is_none());
    assert!(cluster.store().allocs_for_node(NODE)?.is_empty());
    let info = cluster.info(&id).await?;
    assert_eq!(info.state, "suspended");
    assert!(info.archived);
    assert_eq!(info.manifest_url.as_deref(), Some(manifest_url.as_str()));
    assert_eq!(info.checkpoint_id.as_deref(), Some(checkpoint.as_str()));
    assert!(
        info.checkpoint_state.is_none(),
        "the row it came from is gone"
    );
    assert!(manifest_exists(&manifest_url));
    let archived = cluster.events("instance.archived")?;
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0]["manifestOnly"], false);
    assert!(
        cluster
            .heartbeat(Vec::new())
            .await?
            .checkpoint_requests
            .is_empty(),
        "nothing is re-sent for an archived checkpoint"
    );

    // The slim row still owns the name.
    let duplicate = cluster
        .svc
        .create_instance(Request::new(CreateInstanceRequest {
            job_id: JOB.into(),
            instance_id: Some(id.clone()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(duplicate.code(), Code::AlreadyExists, "{duplicate:?}");

    // Start reads the manifest back and resumes from the published parts.
    let started = cluster.start(&id).await?;
    assert_eq!(started.resuming_from, checkpoint);
    assert!(
        started.node_id.is_empty(),
        "a published checkpoint is not pinned"
    );
    let snapshot = cluster
        .store()
        .get_snapshot(&checkpoint)?
        .context("the snapshot row is back")?;
    assert_eq!(snapshot.state.as_str(), "published");
    assert!(snapshot.manifest_json.contains(&url()));
    let info = cluster.info(&id).await?;
    assert!(!info.archived);
    assert!(info.manifest_url.is_none());
    assert_eq!(info.pending_op.as_deref(), Some("resuming"));
    assert!(!manifest_exists(&manifest_url), "the manifest is spent");

    cluster.reschedule()?;
    let allocation_id = cluster
        .info(&id)
        .await?
        .current_alloc_id
        .context("placed")?;
    let restore = cluster
        .heartbeat(Vec::new())
        .await?
        .desired_allocations
        .into_iter()
        .find(|allocation| allocation.allocation_id == allocation_id)
        .and_then(|allocation| allocation.restore)
        .context("restore payload")?;
    assert_eq!(restore.snapshot_id, checkpoint);
    assert!(restore.manifest_json.contains(&url()));
    Ok(())
}

/// Plan §10 Phase 5: "a lost bucket object fails resume with
/// `onRestoreFailure`".
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_archive_manifest_follows_on_restore_failure() -> R<()> {
    // `fail`: the instance stays suspended and archived, so a start can be
    // retried once the object is back.
    let cluster = Cluster::new("slim", Some("fail"))?;
    let (id, _, manifest_url) = cluster.archived(Some("user-1")).await?;
    let path = Url::parse(&manifest_url)?
        .to_file_path()
        .map_err(|_| anyhow::anyhow!("path"))?;
    let saved = std::fs::read(&path)?;
    std::fs::remove_file(&path)?;
    let err = cluster.start(&id).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert!(err.message().contains("could not be read"), "{err:?}");
    let info = cluster.info(&id).await?;
    assert_eq!(info.state, "suspended");
    assert!(info.archived, "still archived, so a retry can read it");
    std::fs::write(&path, saved)?;
    assert!(!cluster.start(&id).await?.resuming_from.is_empty());

    // `cold` (the default): boots fresh, and says why.
    let cluster = Cluster::new("slim", None)?;
    let (id, checkpoint, manifest_url) = cluster.archived(Some("user-2")).await?;
    std::fs::remove_file(
        Url::parse(&manifest_url)?
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("path"))?,
    )?;
    let started = cluster.start(&id).await?;
    assert!(started.resuming_from.is_empty());
    let info = cluster.info(&id).await?;
    assert_eq!(info.state, "pending");
    assert!(!info.archived);
    let cold = info.cold_boot.context("cold boot recorded")?;
    assert_eq!(cold.snapshot_id, checkpoint);
    assert!(cold.reason.contains("does not exist"), "{}", cold.reason);
    Ok(())
}

/// `manifestOnly`: an unnamed instance keeps no row once archived. Get,
/// list with `include_idle`, start, and destroy all work from the bucket.
#[tokio::test(flavor = "multi_thread")]
async fn a_manifest_only_instance_lives_in_the_bucket_until_started() -> R<()> {
    let cluster = Cluster::new("manifestOnly", None)?;
    let (first, checkpoint, first_url) = cluster.archived(None).await?;
    assert!(legion::structs::is_minted_instance_id(&first));
    assert!(
        cluster.store().get_instance(&first)?.is_none(),
        "no row at all"
    );
    assert!(cluster.store().get_snapshot(&checkpoint)?.is_none());
    assert_eq!(
        cluster.events("instance.archived")?[0]["manifestOnly"],
        true
    );

    // Get answers from the manifest.
    let info = cluster.info(&first).await?;
    assert_eq!(info.state, "suspended");
    assert!(info.archived);
    assert!(!info.named);
    assert_eq!(info.job_id, JOB);
    assert_eq!(info.checkpoint_id.as_deref(), Some(checkpoint.as_str()));
    // The task's wake block still applies, so a gateway can wake it.
    assert_eq!(info.wake.map(|wake| wake.port).as_deref(), Some("http"));

    // A named instance of the same task is archived slim: its id is a name.
    let (named, _, _) = cluster.archived(Some("user-7")).await?;
    assert!(cluster.store().get_instance(&named)?.is_some());

    let (second, _, second_url) = cluster.archived(None).await?;

    // A plain list reads the store only; include_idle pages the bucket.
    let list = |token: &str, size: u32| {
        cluster
            .svc
            .list_instances(Request::new(ListInstancesRequest {
                job_id: JOB.into(),
                group: None,
                include_idle: !token.is_empty() || size > 0,
                idle_page_token: token.into(),
                idle_page_size: size,
                ..Default::default()
            }))
    };
    let plain = list("", 0).await?.into_inner();
    assert_eq!(
        plain
            .instances
            .iter()
            .map(|info| info.instance_id.as_str())
            .collect::<Vec<_>>(),
        vec!["user-7"]
    );
    // One bucket entry per page. The slim instance's manifest is listed
    // too and dropped, since its row already answered, so a page can come
    // back short while the token says there is more.
    let mut idle: Vec<String> = Vec::new();
    let mut token = String::new();
    let mut pages = 0;
    loop {
        let page = list(&token, 1).await?.into_inner();
        pages += 1;
        assert_eq!(page.instances[0].instance_id, "user-7", "rows come first");
        idle.extend(
            page.instances[1..]
                .iter()
                .inspect(|info| assert!(info.archived && info.state == "suspended"))
                .map(|info| info.instance_id.clone()),
        );
        token = page.next_idle_page_token;
        if token.is_empty() {
            break;
        }
        assert!(pages < 10, "the listing ends");
    }
    assert!(pages >= 3, "three manifests, one per page");
    idle.sort();
    let mut want = vec![first.clone(), second.clone()];
    want.sort();
    assert_eq!(idle, want);

    // Start puts the row back and resumes it; the manifest is spent.
    let started = cluster.start(&first).await?;
    assert_eq!(started.resuming_from, checkpoint);
    let row = cluster
        .store()
        .get_instance(&first)?
        .context("row is back")?;
    assert!(!row.named);
    assert_eq!(
        cluster.info(&first).await?.pending_op.as_deref(),
        Some("resuming")
    );
    assert!(!manifest_exists(&first_url));

    // Destroy deletes the manifest, and the id is gone for good.
    cluster
        .svc
        .destroy_instance(Request::new(DestroyInstanceRequest {
            instance_id: second.clone(),
            idempotency_key: None,
        }))
        .await?;
    assert!(!manifest_exists(&second_url));
    let gone = cluster.info(&second).await.unwrap_err();
    assert_eq!(gone.code(), Code::NotFound);
    let gone = cluster.start(&second).await.unwrap_err();
    assert_eq!(gone.code(), Code::NotFound);
    Ok(())
}

/// Plan §13 Phase 5, at test scale: archived instances leave one row each
/// and no snapshot or allocation rows behind them.
#[tokio::test(flavor = "multi_thread")]
async fn archived_instances_leave_only_their_rows() -> R<()> {
    let cluster = Cluster::new("slim", None)?;
    for n in 0..20 {
        cluster.archived(Some(&format!("user-{n}"))).await?;
    }
    assert_eq!(cluster.store().list_instances(JOB)?.len(), 20);
    assert!(cluster.store().allocs_for_node(NODE)?.is_empty());
    let snapshots = cluster.store().list_snapshots(&Default::default())?;
    assert!(snapshots.is_empty(), "{snapshots:?}");
    Ok(())
}
