//! Durable checkpoints and cross-node resume on the manager (suspend plan
//! Phase 7b; docs/suspend-and-resume.md, "Durability"): `durability =
//! "store"` publishes a suspend checkpoint, a published checkpoint survives
//! its node and resumes on any compatible node, placement checks `compat` per
//! candidate and prefers the node holding the parts, and WarmSnapshot queues
//! fetches on matching nodes.
//!
//! The heartbeat stands in for the workers, as in `suspend_integration.rs`.
//! The worker half (publish to a `file://` content store, fetch on another
//! node through the artifact stager, restore) runs for real in
//! `src/worker/task_manager.rs`
//! (`a_published_snapshot_restores_on_a_node_that_never_held_it`).

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::events::SchedulerEventFilter;
use legion::storage::store::Store;
use legion::structs::manager_server::Manager;
use legion::structs::suspend::SuspendConfig;
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, job::StatefulConfig, job::TaskConfig,
    CheckpointPartKey, CheckpointRequest, ContentLocation, ContentLocationReport,
    GetInstanceRequest, HeartbeatReply, HeartbeatRequest, InstanceInfo, InstanceMode, Node,
    NodeMsg, SnapshotReport, StartInstanceRequest, Submission, SuspendInstanceRequest,
    WarmSnapshotRequest,
};
use serde_json::Value;
use tonic::{Code, Request};

const NODE_A: &str = "node-a";
const NODE_B: &str = "node-b";
const NODE_C: &str = "node-c";
const JOB: &str = "workspaces";
const TASK: &str = "workspace";
const ID: &str = "user-42";
const CPU_MODEL: &str = "6:106:6";
/// 64 MiB, large enough for the location index (which skips entries under
/// 1 MiB) and so for the `locality` score.
const PART_BYTES: u64 = 64 * 1024 * 1024;

struct Cluster {
    svc: ManagerDaemonService,
}

impl Cluster {
    fn new() -> R<Self> {
        let dir = tempfile::Builder::new()
            .prefix("legion-published-it-")
            .tempdir_in(crate::support::scratch_root())?
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

    fn add_node(&self, id: &str, cpu_model: &str, region: &str, publishes: bool) -> R<()> {
        let mut capabilities = vec!["checkpoint", "checkpoint-fetch"];
        if publishes {
            capabilities.push("content-publish");
        }
        self.store().insert_node(NodeMsg::from(Node {
            id: id.to_string(),
            pool: "default".into(),
            status: "ready".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({
                "node_id": id,
                "region": region,
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
                "cpu.model": cpu_model,
                "cpu.features": "3f9a0c21",
                "sandbox.vmm": "firecracker",
                "sandbox.vmm.version": "1.7.0",
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

    /// Run every queued evaluation. Returns the errors of the ones that
    /// failed (a placement that found no node).
    fn reschedule(&self) -> R<Vec<String>> {
        let config_id = self.store().get_job_config(JOB)?.id();
        self.store()
            .enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
        let mut failed = Vec::new();
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
                failed.push(format!("{err:#}"));
            }
        }
        Ok(failed)
    }

    async fn heartbeat(&self, node: &str, reports: Vec<SnapshotReport>) -> R<HeartbeatReply> {
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

    async fn start(&self) -> Result<legion::structs::StartInstanceReply, tonic::Status> {
        self.svc
            .start_instance(Request::new(StartInstanceRequest {
                instance_id: ID.into(),
                idempotency_key: None,
                cold: false,
                force_move: false,
            }))
            .await
            .map(|reply| reply.into_inner())
    }

    fn node_of_instance(&self) -> R<Option<String>> {
        let Some(allocation_id) = self
            .store()
            .get_instance(ID)?
            .context("instance")?
            .current_alloc_id
        else {
            return Ok(None);
        };
        Ok(self
            .store()
            .get_allocation_by_id(allocation_id)?
            .map(|allocation| allocation.node_id))
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

    /// Place the instance on node A (the only node at first), suspend it
    /// with a memory checkpoint, and have A report the capture. Returns the
    /// checkpoint id.
    async fn suspended_on_a(&self, durability: &str) -> R<String> {
        self.add_node(NODE_A, CPU_MODEL, "us-west", true)?;
        self.submit(job(durability))?;
        self.store()
            .create_keyed_instance(JOB, "default", TASK, Some(ID))?;
        self.reschedule()?;
        assert_eq!(self.node_of_instance()?.as_deref(), Some(NODE_A));
        let allocation_id = self
            .store()
            .get_instance(ID)?
            .context("instance")?
            .current_alloc_id
            .context("placed")?;
        let reply = self
            .svc
            .suspend_instance(Request::new(SuspendInstanceRequest {
                instance_id: ID.into(),
                checkpoint: None,
                idempotency_key: None,
            }))
            .await?
            .into_inner();
        assert_eq!(reply.kind, "memory");
        assert_eq!(reply.durability, durability);
        self.heartbeat(
            NODE_A,
            vec![SnapshotReport {
                snapshot_id: reply.snapshot_id.clone(),
                op: "capture".into(),
                ok: true,
                manifest_json: manifest(&reply.snapshot_id, allocation_id, None),
                duration_ms: 740,
                total_bytes: PART_BYTES,
                allocation_id,
                ..Default::default()
            }],
        )
        .await?;
        assert_eq!(self.info().await?.state, "suspended");
        Ok(reply.snapshot_id)
    }

    /// Node A publishes `snapshot_id`: the publish request it is sent,
    /// carrying one key per part, and its successful report. Returns the
    /// part key the manager sent.
    async fn published_by_a(&self, snapshot_id: &str) -> R<CheckpointPartKey> {
        let asked = requests_for(&self.heartbeat(NODE_A, Vec::new()).await?, snapshot_id);
        assert_eq!(asked.len(), 1, "one publish request: {asked:?}");
        assert_eq!(asked[0].op, "publish");
        assert!(!asked[0].manifest_json.is_empty());
        assert_eq!(asked[0].part_key_error, "");
        assert_eq!(asked[0].part_keys.len(), 1, "one key per memory part");
        let key = asked[0].part_keys[0].clone();
        assert_eq!(key.digest, digest());
        assert!(key.key_id.starts_with("ck_"), "{}", key.key_id);
        assert_eq!(key.key.len(), 32);
        // And the job's chunk key, which every chunk's key derives from.
        let chunks = asked[0].chunk_key.clone().expect("a chunk key");
        assert_eq!(chunks.key.len(), 32);
        assert!(
            chunks
                .key_id
                .starts_with(&format!("{}/session/", key.key_id))
                && chunks.key_id.ends_with("/chunks"),
            "{}",
            chunks.key_id
        );
        // The request can be logged without leaking either key.
        let shown = format!("{:?}", asked[0]);
        assert!(shown.contains("redacted"), "{shown}");
        assert!(!shown.contains(&format!("{:?}", key.key)), "{shown}");
        assert!(!shown.contains(&format!("{:?}", chunks.key)), "{shown}");
        self.heartbeat(NODE_A, vec![publish_ok(snapshot_id, &key.key_id)])
            .await?;
        Ok(key)
    }

    fn advertise(&self, node: &str, digests: &[&str]) {
        self.store().content_locations().ingest(
            node,
            &ContentLocationReport {
                epoch: 1,
                base_epoch: 0,
                full: true,
                added: digests
                    .iter()
                    .map(|digest| ContentLocation {
                        digest: digest.to_string(),
                        bytes: PART_BYTES,
                    })
                    .collect(),
                removed: Vec::new(),
                snapshot_budget: None,
            },
            std::time::Instant::now(),
        );
    }
}

fn job(durability: &str) -> JobConfig {
    let task = TaskConfig {
        config: json!({ "image": "alpine", "command": ["sleep", "60"] }),
        // Keyed with no stateful.storage: everything the instance has is in
        // its checkpoint, so it can move.
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
        }),
        suspend: Some(SuspendConfig {
            durability: Some(durability.into()),
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
    }
}

fn digest() -> String {
    format!("sha256:{}", "ab".repeat(32))
}

fn url() -> String {
    format!("file:///shared/content/sealed/{}", "cd".repeat(32))
}

/// A memory manifest with one part, captured on a `CPU_MODEL` node, with
/// `url` once published.
fn manifest(snapshot_id: &str, allocation_id: i64, url: Option<String>) -> String {
    manifest_on(snapshot_id, allocation_id, url, CPU_MODEL)
}

/// [`manifest`] as a worker reports it once published: each part's `url`,
/// and the checkpoint key (`key_id`) it was sealed under.
fn published_manifest(snapshot_id: &str, allocation_id: i64, key_id: &str) -> String {
    let mut manifest: Value =
        serde_json::from_str(&manifest(snapshot_id, allocation_id, Some(url()))).unwrap();
    manifest["memory"][0]["keyId"] = json!(key_id);
    manifest.to_string()
}

/// [`manifest`], captured on a node whose CPU model is `cpu_model`.
fn manifest_on(
    snapshot_id: &str,
    allocation_id: i64,
    url: Option<String>,
    cpu_model: &str,
) -> String {
    json!({
        "apiVersion": 1,
        "id": snapshot_id,
        "jobId": JOB,
        "task": TASK,
        "originInstance": ID,
        "originAllocId": allocation_id,
        "runtime": "sandbox",
        "kind": "memory",
        "createdAt": "2026-10-10T00:00:00Z",
        "memory": [{
            "role": "memory", "file": "memory", "digest": digest(), "size": PART_BYTES,
            "url": url,
        }],
        "compat": {
            "vmm": { "type": "firecracker", "version": "1.7.0" },
            "cpu": { "vendor": "GenuineIntel", "model": cpu_model, "features": "3f9a0c21" },
        },
    })
    .to_string()
}

fn publish_ok(snapshot_id: &str, key_id: &str) -> SnapshotReport {
    SnapshotReport {
        snapshot_id: snapshot_id.into(),
        op: "publish".into(),
        ok: true,
        manifest_json: published_manifest(snapshot_id, 1, key_id),
        duration_ms: 1_500,
        total_bytes: PART_BYTES,
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

/// Plan §13 Phase 7: "a `durability = "store"` instance resumes on another
/// node after its original node is destroyed."
#[tokio::test(flavor = "multi_thread")]
async fn a_store_checkpoint_resumes_on_another_node_after_its_node_is_removed() -> R<()> {
    let cluster = Cluster::new()?;
    let checkpoint = cluster.suspended_on_a("store").await?;
    let info = cluster.info().await?;
    assert_eq!(info.checkpoint_durability.as_deref(), Some("store"));
    assert_eq!(
        info.checkpoint_state.as_deref(),
        Some("local"),
        "suspended as soon as the local write finished, published later"
    );

    let publish_key = cluster.published_by_a(&checkpoint).await?;
    let snapshot = cluster
        .store()
        .get_snapshot(&checkpoint)?
        .context("snapshot")?;
    assert_eq!(snapshot.state.as_str(), "published");
    assert!(snapshot.published_at.is_some());
    let manifest = snapshot.manifest()?.context("manifest")?;
    assert_eq!(manifest.memory[0].url.as_deref(), Some(url().as_str()));
    assert_eq!(
        manifest.memory[0].key_id.as_deref(),
        Some(publish_key.key_id.as_str())
    );
    assert!(
        !snapshot.manifest_json.contains("\"key\""),
        "a manifest never carries key bytes"
    );
    let published = cluster.events("snapshot.published")?;
    assert_eq!(published.len(), 1);
    assert_eq!(published[0]["uploadedBytes"], PART_BYTES);
    assert!(
        requests_for(&cluster.heartbeat(NODE_A, Vec::new()).await?, &checkpoint).is_empty(),
        "a published snapshot is not re-sent"
    );
    assert_eq!(
        cluster.info().await?.checkpoint_state.as_deref(),
        Some("published")
    );

    // Node A is destroyed. A published checkpoint is not lost with it.
    cluster.add_node(NODE_B, CPU_MODEL, "us-west", false)?;
    cluster.store().remove_node(NODE_A)?;
    assert_eq!(
        cluster
            .store()
            .get_snapshot(&checkpoint)?
            .context("snapshot")?
            .state
            .as_str(),
        "published"
    );

    let started = cluster.start().await?;
    assert_eq!(started.resuming_from, checkpoint);
    assert!(
        started.node_id.is_empty(),
        "a published checkpoint is not pinned to a node"
    );
    assert!(
        started.message.contains("any compatible node"),
        "{}",
        started.message
    );
    assert!(cluster.reschedule()?.is_empty());
    assert_eq!(cluster.node_of_instance()?.as_deref(), Some(NODE_B));

    // B gets the published manifest, URLs and all, to fetch from.
    let allocation_id = cluster.info().await?.current_alloc_id.context("placed")?;
    let reply = cluster.heartbeat(NODE_B, Vec::new()).await?;
    let restore = reply
        .desired_allocations
        .iter()
        .find(|allocation| allocation.allocation_id == allocation_id)
        .and_then(|allocation| allocation.restore.clone())
        .context("restore payload")?;
    assert!(restore.manifest_json.contains(&url()));
    // ...and the key to open each part: the same key the publish sealed
    // with, derived again from the manifest's keyId.
    assert_eq!(restore.part_key_error, "");
    assert_eq!(restore.part_keys, vec![publish_key.clone()]);

    cluster
        .heartbeat(
            NODE_B,
            vec![SnapshotReport {
                snapshot_id: checkpoint.clone(),
                op: "restore".into(),
                ok: true,
                duration_ms: 950,
                allocation_id,
                ..Default::default()
            }],
        )
        .await?;
    let info = cluster.info().await?;
    assert_eq!(info.state, "active");
    assert!(info.checkpoint_id.is_none());
    assert_eq!(
        info.restored_from_snapshot_id.as_deref(),
        Some(checkpoint.as_str())
    );
    // Spent, and its node is gone: the row goes at once.
    assert!(cluster.store().get_snapshot(&checkpoint)?.is_none());
    Ok(())
}

/// While the upload is still pending the checkpoint is local: the resume is
/// pinned to its node, and losing the node loses it.
#[tokio::test(flavor = "multi_thread")]
async fn an_unpublished_store_checkpoint_is_pinned_and_lost_with_its_node() -> R<()> {
    let cluster = Cluster::new()?;
    let checkpoint = cluster.suspended_on_a("store").await?;
    cluster.add_node(NODE_B, CPU_MODEL, "us-west", false)?;
    cluster.store().remove_node(NODE_A)?;
    assert_eq!(
        cluster
            .store()
            .get_snapshot(&checkpoint)?
            .context("snapshot")?
            .state
            .as_str(),
        "lost"
    );
    // onRestoreFailure = "cold" (default): boots fresh, and says why.
    let started = cluster.start().await?;
    assert!(started.resuming_from.is_empty());
    assert!(started.message.contains("lost"), "{}", started.message);
    Ok(())
}

/// A publish that fails is named, emits an event, is not re-sent until its
/// backoff passes, and leaves a working local checkpoint pinned to its node.
/// The retry itself is covered in `snapshot_ops`'s unit tests.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_publish_is_named_and_the_checkpoint_stays_local() -> R<()> {
    let cluster = Cluster::new()?;
    let checkpoint = cluster.suspended_on_a("store").await?;
    cluster
        .heartbeat(
            NODE_A,
            vec![SnapshotReport {
                snapshot_id: checkpoint.clone(),
                op: "publish".into(),
                ok: false,
                error: "this worker has no contentStore configured".into(),
                ..Default::default()
            }],
        )
        .await?;
    let snapshot = cluster
        .store()
        .get_snapshot(&checkpoint)?
        .context("snapshot")?;
    assert_eq!(snapshot.state.as_str(), "local");
    assert!(
        snapshot.error.as_deref().is_some_and(|error| error
            .starts_with("publish attempt 1 of 3 failed, retrying in 60s")
            && error.contains("contentStore")),
        "{:?}",
        snapshot.error
    );
    assert_eq!(cluster.events("snapshot.publish_failed")?.len(), 1);
    assert!(
        requests_for(&cluster.heartbeat(NODE_A, Vec::new()).await?, &checkpoint).is_empty(),
        "not re-sent until the backoff passes"
    );

    cluster.add_node(NODE_B, CPU_MODEL, "us-west", false)?;
    let started = cluster.start().await?;
    assert_eq!(
        started.node_id, NODE_A,
        "still pinned to the node holding it"
    );
    cluster.reschedule()?;
    assert_eq!(cluster.node_of_instance()?.as_deref(), Some(NODE_A));
    Ok(())
}

/// `durability = "store"` on a node whose worker cannot publish is refused
/// at call time, with the instance still running.
#[tokio::test(flavor = "multi_thread")]
async fn a_store_suspend_on_a_worker_without_a_content_store_is_refused() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node(NODE_A, CPU_MODEL, "us-west", false)?;
    cluster.submit(job("store"))?;
    cluster
        .store()
        .create_keyed_instance(JOB, "default", TASK, Some(ID))?;
    cluster.reschedule()?;
    let err = cluster
        .svc
        .suspend_instance(Request::new(SuspendInstanceRequest {
            instance_id: ID.into(),
            checkpoint: None,
            idempotency_key: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("contentStore"), "{}", err.message());
    assert!(err.message().contains("Hint:"), "{}", err.message());
    assert_eq!(cluster.info().await?.state, "active");
    Ok(())
}

/// Locality: with weights otherwise equal, a published resume lands on the
/// node the location index says holds its parts.
#[tokio::test(flavor = "multi_thread")]
async fn a_published_resume_prefers_the_node_holding_its_parts() -> R<()> {
    for (round, holder) in [NODE_B, NODE_C, NODE_B, NODE_C].into_iter().enumerate() {
        let cluster = Cluster::new()?;
        let checkpoint = cluster.suspended_on_a("store").await?;
        cluster.published_by_a(&checkpoint).await?;
        cluster.add_node(NODE_B, CPU_MODEL, "us-west", false)?;
        cluster.add_node(NODE_C, CPU_MODEL, "us-west", false)?;
        cluster.store().remove_node(NODE_A)?;
        let other = if holder == NODE_B { NODE_C } else { NODE_B };
        cluster.advertise(holder, &[&digest()]);
        cluster.advertise(other, &[]);
        cluster.start().await?;
        assert!(cluster.reschedule()?.is_empty());
        assert_eq!(
            cluster.node_of_instance()?.as_deref(),
            Some(holder),
            "round {round}"
        );
    }
    Ok(())
}

/// Publishing releases the checkpoint from the node that captured it: the
/// node's usage stops counting it as suspended there (the worker unpins the
/// parts, so its content store may evict them), and a resume follows the
/// parts wherever the location index says they are, the capturing node
/// included only while it still advertises them.
#[tokio::test(flavor = "multi_thread")]
async fn a_published_checkpoint_leaves_its_node_and_resumes_where_its_parts_are() -> R<()> {
    for (round, evicted_from_a) in [true, false].into_iter().enumerate() {
        let cluster = Cluster::new()?;
        let checkpoint = cluster.suspended_on_a("store").await?;
        assert_eq!(
            cluster.store().suspended_on_node(NODE_A)?,
            (1, PART_BYTES),
            "local until published"
        );
        cluster.published_by_a(&checkpoint).await?;
        assert_eq!(
            cluster.store().suspended_on_node(NODE_A)?,
            (0, 0),
            "a published checkpoint is no longer the node's"
        );

        cluster.add_node(NODE_B, CPU_MODEL, "us-west", false)?;
        // A's copy was evicted after the unpin, and B fetched one (a warm, or
        // an earlier resume). Or A still holds its copy and B has nothing.
        if evicted_from_a {
            cluster.advertise(NODE_A, &[]);
            cluster.advertise(NODE_B, &[&digest()]);
        } else {
            cluster.advertise(NODE_A, &[&digest()]);
            cluster.advertise(NODE_B, &[]);
        }
        let started = cluster.start().await?;
        assert!(started.node_id.is_empty(), "not pinned to node A");
        assert!(cluster.reschedule()?.is_empty());
        let expected = if evicted_from_a { NODE_B } else { NODE_A };
        assert_eq!(
            cluster.node_of_instance()?.as_deref(),
            Some(expected),
            "round {round}"
        );
        // Wherever it lands, the restore carries the URLs to fetch from, so
        // a node whose copy is gone (A included) can still restore.
        let allocation_id = cluster.info().await?.current_alloc_id.context("placed")?;
        let reply = cluster.heartbeat(expected, Vec::new()).await?;
        let restore = reply
            .desired_allocations
            .iter()
            .find(|allocation| allocation.allocation_id == allocation_id)
            .and_then(|allocation| allocation.restore.clone())
            .context("restore payload")?;
        assert!(restore.manifest_json.contains(&url()));
    }
    Ok(())
}

/// A published memory checkpoint has no pin, so `compat` is checked on every
/// candidate: an incompatible node is never chosen, and with only
/// incompatible nodes the placement fails naming the attribute.
#[tokio::test(flavor = "multi_thread")]
async fn compat_filters_every_candidate_of_a_published_restore() -> R<()> {
    let cluster = Cluster::new()?;
    let checkpoint = cluster.suspended_on_a("store").await?;
    cluster.published_by_a(&checkpoint).await?;
    cluster.add_node(NODE_B, "6:143:8", "us-west", false)?;
    cluster.store().remove_node(NODE_A)?;
    // B would even win on locality; compat still rules it out.
    cluster.advertise(NODE_B, &[&digest()]);
    cluster.start().await?;
    let errors = cluster.reschedule()?;
    assert!(
        errors
            .iter()
            .any(|error| error.contains("cpu.model") && error.contains("6:143:8")),
        "no compatible node: the placement fails naming the attribute: {errors:?}"
    );
    assert_eq!(cluster.node_of_instance()?, None);

    cluster.add_node(NODE_C, CPU_MODEL, "us-west", false)?;
    assert!(cluster.reschedule()?.is_empty());
    assert_eq!(cluster.node_of_instance()?.as_deref(), Some(NODE_C));
    Ok(())
}

/// WarmSnapshot: matching, compatible nodes get a `warm` request until they
/// report; a node already holding the parts, an incompatible node, and a
/// node outside the constraint are skipped.
#[tokio::test(flavor = "multi_thread")]
async fn warm_queues_matching_nodes_until_they_report() -> R<()> {
    let cluster = Cluster::new()?;
    let checkpoint = cluster.suspended_on_a("store").await?;

    // A local snapshot has nothing for other nodes to fetch.
    let err = cluster
        .svc
        .warm_snapshot(Request::new(WarmSnapshotRequest {
            snapshot: checkpoint.clone(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("not published"), "{}", err.message());

    cluster.published_by_a(&checkpoint).await?;
    cluster.add_node(NODE_B, CPU_MODEL, "us-west", false)?;
    cluster.add_node(NODE_C, "6:143:8", "us-west", false)?;
    cluster.add_node("node-d", CPU_MODEL, "eu-central", false)?;
    // A captured it and still holds the parts.
    cluster.advertise(NODE_A, &[&digest()]);

    let warm = || async {
        cluster
            .svc
            .warm_snapshot(Request::new(WarmSnapshotRequest {
                snapshot: checkpoint.clone(),
                constraints: vec!["region=us-west".into()],
                ..Default::default()
            }))
            .await
            .map(|reply| reply.into_inner())
    };
    let reply = warm().await?;
    assert_eq!(reply.nodes, vec![NODE_B.to_string()]);
    assert_eq!(reply.already_warm, vec![NODE_A.to_string()]);
    assert_eq!(reply.incompatible.len(), 1);
    assert!(
        reply.incompatible[0].starts_with(NODE_C),
        "{:?}",
        reply.incompatible
    );

    let asked = requests_for(&cluster.heartbeat(NODE_B, Vec::new()).await?, &checkpoint);
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].op, "warm");
    assert!(asked[0].manifest_json.contains(&url()));
    assert_eq!(asked[0].part_keys.len(), 1, "a warm carries the part keys");
    assert_eq!(asked[0].part_key_error, "");
    // Re-sent until the node reports, and warming again queues nothing new.
    assert_eq!(
        requests_for(&cluster.heartbeat(NODE_B, Vec::new()).await?, &checkpoint).len(),
        1
    );
    assert_eq!(warm().await?.nodes, vec![NODE_B.to_string()]);
    assert!(requests_for(&cluster.heartbeat("node-d", Vec::new()).await?, &checkpoint).is_empty());

    cluster
        .heartbeat(
            NODE_B,
            vec![SnapshotReport {
                snapshot_id: checkpoint.clone(),
                op: "warm".into(),
                ok: true,
                total_bytes: PART_BYTES,
                duration_ms: 300,
                ..Default::default()
            }],
        )
        .await?;
    let warmed = cluster.events("snapshot.warmed")?;
    assert_eq!(warmed.len(), 1);
    assert_eq!(warmed[0]["fetchedBytes"], PART_BYTES);
    assert!(requests_for(&cluster.heartbeat(NODE_B, Vec::new()).await?, &checkpoint).is_empty());

    let bad = cluster
        .svc
        .warm_snapshot(Request::new(WarmSnapshotRequest {
            snapshot: checkpoint.clone(),
            constraints: vec!["region".into()],
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(bad.code(), Code::InvalidArgument);
    Ok(())
}

/// A node that dies stops heartbeating and is marked lost long before anyone
/// removes it. A published checkpoint does not wait for it: the resume runs
/// on another node while the original is still in the node table.
#[tokio::test(flavor = "multi_thread")]
async fn a_published_checkpoint_resumes_elsewhere_while_its_node_is_lost() -> R<()> {
    let cluster = Cluster::new()?;
    let checkpoint = cluster.suspended_on_a("store").await?;
    cluster.published_by_a(&checkpoint).await?;
    cluster.add_node(NODE_B, CPU_MODEL, "us-west", false)?;
    cluster.store().mark_node_lost(NODE_A)?;
    cluster.start().await?;
    assert!(cluster.reschedule()?.is_empty());
    assert_eq!(cluster.node_of_instance()?.as_deref(), Some(NODE_B));
    Ok(())
}

/// The contrast: a `local` checkpoint on a lost node keeps its resume pinned
/// there, and placement says why.
#[tokio::test(flavor = "multi_thread")]
async fn a_local_checkpoint_waits_for_its_lost_node() -> R<()> {
    let cluster = Cluster::new()?;
    let checkpoint = cluster.suspended_on_a("local").await?;
    cluster.add_node(NODE_B, CPU_MODEL, "us-west", false)?;
    cluster.store().mark_node_lost(NODE_A)?;
    let started = cluster.start().await?;
    assert_eq!(started.resuming_from, checkpoint);
    assert_eq!(started.node_id, NODE_A);
    let errors = cluster.reschedule()?;
    assert!(
        errors.iter().any(|error| error.contains(NODE_A)),
        "{errors:?}"
    );
    assert_eq!(cluster.node_of_instance()?, None);
    Ok(())
}

/// Suspend `instance` (running on `node`) with a memory checkpoint captured
/// on a `cpu_model` node, and have the node report the capture. Returns the
/// checkpoint id.
async fn suspend_on(cluster: &Cluster, instance: &str, node: &str, cpu_model: &str) -> R<String> {
    let allocation_id = cluster
        .store()
        .get_instance(instance)?
        .context("instance")?
        .current_alloc_id
        .context("placed")?;
    let reply = cluster
        .svc
        .suspend_instance(Request::new(SuspendInstanceRequest {
            instance_id: instance.into(),
            checkpoint: None,
            idempotency_key: None,
        }))
        .await?
        .into_inner();
    cluster
        .heartbeat(
            node,
            vec![SnapshotReport {
                snapshot_id: reply.snapshot_id.clone(),
                op: "capture".into(),
                ok: true,
                manifest_json: manifest_on(&reply.snapshot_id, allocation_id, None, cpu_model),
                duration_ms: 740,
                total_bytes: PART_BYTES,
                allocation_id,
                ..Default::default()
            }],
        )
        .await?;
    Ok(reply.snapshot_id)
}

fn node_of(cluster: &Cluster, instance: &str) -> R<Option<String>> {
    let Some(allocation_id) = cluster
        .store()
        .get_instance(instance)?
        .context("instance")?
        .current_alloc_id
    else {
        return Ok(None);
    };
    Ok(cluster
        .store()
        .get_allocation_by_id(allocation_id)?
        .map(|allocation| allocation.node_id))
}

/// Two restores with different `compat` placed in one plan each bind the
/// instance the planner placed: the apply never swaps them. Here the older
/// instance's published checkpoint was captured on node A before A's CPU
/// changed, so it must float to B, while the newer one (captured on A after
/// the change, still local) is pinned to A. The binder alone would hand A to
/// the older instance (both snapshots were captured there) and leave the
/// pinned one nowhere to go.
#[tokio::test(flavor = "multi_thread")]
async fn restores_with_different_compat_in_one_plan_bind_their_own_nodes() -> R<()> {
    const OLDER: &str = "user-42";
    const NEWER: &str = "user-17";
    const NEW_MODEL: &str = "6:143:8";
    let cluster = Cluster::new()?;
    cluster.add_node(NODE_A, CPU_MODEL, "us-west", true)?;
    cluster.submit(job("store"))?;
    for id in [OLDER, NEWER] {
        cluster
            .store()
            .create_keyed_instance(JOB, "default", TASK, Some(id))?;
    }
    assert!(cluster.reschedule()?.is_empty());

    let older = suspend_on(&cluster, OLDER, NODE_A, CPU_MODEL).await?;
    cluster.published_by_a(&older).await?;
    // Node A's CPU changes (a new host behind the same node id).
    cluster.add_node(NODE_A, NEW_MODEL, "us-west", true)?;
    let newer = suspend_on(&cluster, NEWER, NODE_A, NEW_MODEL).await?;
    assert_eq!(
        cluster
            .store()
            .get_snapshot(&newer)?
            .context("snapshot")?
            .state
            .as_str(),
        "local",
        "not published yet, so pinned to A"
    );
    cluster.add_node(NODE_B, CPU_MODEL, "us-west", false)?;

    for id in [OLDER, NEWER] {
        cluster
            .svc
            .start_instance(Request::new(StartInstanceRequest {
                instance_id: id.into(),
                idempotency_key: None,
                cold: false,
                force_move: false,
            }))
            .await?;
    }
    let errors = cluster.reschedule()?;
    assert!(errors.is_empty(), "{errors:?}");
    assert_eq!(node_of(&cluster, OLDER)?.as_deref(), Some(NODE_B));
    assert_eq!(node_of(&cluster, NEWER)?.as_deref(), Some(NODE_A));
    Ok(())
}

/// A published restore is only placed on a worker that advertises
/// `checkpoint-fetch`. An older worker in the fleet is skipped even when it
/// holds the parts, and with only older workers the placement fails saying
/// why.
#[tokio::test(flavor = "multi_thread")]
async fn a_published_restore_skips_workers_that_cannot_fetch() -> R<()> {
    let cluster = Cluster::new()?;
    let checkpoint = cluster.suspended_on_a("store").await?;
    cluster.published_by_a(&checkpoint).await?;
    cluster.store().remove_node(NODE_A)?;
    // B is an older worker: it takes checkpoints but cannot fetch.
    cluster.store().insert_node(NodeMsg::from(Node {
        id: NODE_B.to_string(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({
            "node_id": NODE_B,
            "region": "us-west",
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
            "cpu.model": CPU_MODEL,
            "cpu.features": "3f9a0c21",
            "sandbox.vmm": "firecracker",
            "sandbox.vmm.version": "1.7.0",
        }),
        dynamic_attrs: json!({ "buildVersion": "0.8.0", "capabilities": ["checkpoint"] }),
        health_checks: json!({}),
    }))?;
    cluster.advertise(NODE_B, &[&digest()]);
    cluster.start().await?;
    let errors = cluster.reschedule()?;
    assert!(
        errors
            .iter()
            .any(|error| error.contains(NODE_B) && error.contains("checkpoint-fetch")),
        "{errors:?}"
    );
    assert_eq!(cluster.node_of_instance()?, None);

    cluster.add_node(NODE_C, CPU_MODEL, "us-west", false)?;
    assert!(cluster.reschedule()?.is_empty());
    assert_eq!(cluster.node_of_instance()?.as_deref(), Some(NODE_C));
    Ok(())
}

/// Suspend/resume UI plan §6.7: GetSnapshot names the nodes that
/// hold a published checkpoint's parts, and GetNodeContent shows the same
/// fact from the node's side, with the entry labeled by its snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_holders_and_node_content_read_the_location_index() -> R<()> {
    let cluster = Cluster::new()?;
    let checkpoint = cluster.suspended_on_a("store").await?;
    cluster.published_by_a(&checkpoint).await?;
    cluster.add_node(NODE_B, CPU_MODEL, "us-west", false)?;
    cluster.advertise(NODE_B, &[&digest()]);

    let snapshot = cluster
        .svc
        .get_snapshot(Request::new(legion::structs::GetSnapshotRequest {
            snapshot: checkpoint.clone(),
            job_id: None,
        }))
        .await?
        .into_inner()
        .snapshot
        .context("snapshot")?;
    let holder = snapshot
        .holders
        .iter()
        .find(|holder| holder.node_id == NODE_B)
        .context("node-b holds the part")?;
    assert_eq!((holder.parts_here, holder.parts_total), (1, 1));
    assert!(!holder.captured_here);

    let content = cluster
        .svc
        .get_node_content(Request::new(legion::structs::GetNodeContentRequest {
            node_id: NODE_B.into(),
        }))
        .await?
        .into_inner();
    assert!(content.index_reported);
    assert_eq!(content.entries.len(), 1);
    assert_eq!(content.entries[0].snapshot_id, checkpoint);
    let row = content
        .snapshots
        .iter()
        .find(|row| {
            row.snapshot.as_ref().map(|s| s.snapshot_id.as_str()) == Some(checkpoint.as_str())
        })
        .context("the checkpoint is listed for node-b")?;
    assert!(!row.captured_here);
    assert_eq!(row.parts_here, 1);
    Ok(())
}

/// A part published before checkpoint encryption (a `url` and no `keyId`)
/// gets no key: the worker's fetch names it. A part sealed under a key the
/// cluster no longer has gets no key either, and the restore payload says
/// why, so the worker can fail the fetch with that reason.
#[tokio::test(flavor = "multi_thread")]
async fn a_restore_payload_explains_parts_it_has_no_key_for() -> R<()> {
    let cluster = Cluster::new()?;
    let checkpoint = cluster.suspended_on_a("store").await?;
    cluster.published_by_a(&checkpoint).await?;
    // What the restore payload builder attaches for a given manifest.
    let restore_payload = |manifest_json: String| -> R<legion::structs::SnapshotRestore> {
        let (part_keys, part_key_error) = legion::manager::server::checkpoint_part_keys(
            cluster.store(),
            &manifest_json,
            legion::manager::server::PartKeyUse::Fetch,
        );
        Ok(legion::structs::SnapshotRestore {
            part_keys,
            part_key_error,
            ..Default::default()
        })
    };

    let legacy = restore_payload(manifest(&checkpoint, 1, Some(url())))?;
    assert!(legacy.part_keys.is_empty());
    assert_eq!(
        legacy.part_key_error, "",
        "the worker names a legacy part itself"
    );

    let foreign = restore_payload(published_manifest(&checkpoint, 1, "ck_0000000000000000"))?;
    assert!(foreign.part_keys.is_empty());
    assert!(
        foreign
            .part_key_error
            .contains("checkpoint key ck_0000000000000000 is not in this cluster's keyring"),
        "{}",
        foreign.part_key_error
    );

    // A local snapshot needs no keys at all.
    let local = restore_payload(manifest(&checkpoint, 1, None))?;
    assert!(local.part_keys.is_empty());
    assert_eq!(local.part_key_error, "");
    Ok(())
}
