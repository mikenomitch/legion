//! The worker half of the checkpoint channel (suspend plan §6.5;
//! docs/suspend-and-resume.md).
//!
//! The manager re-sends a [`CheckpointRequest`] on every heartbeat until it
//! has applied this worker's [`SnapshotReport`] for it. So the worker keeps:
//!
//! - **in flight**: work started and not finished. A re-sent request for it
//!   is ignored.
//! - **done**: the last report per `(snapshot, op)`. A re-sent request for
//!   finished work re-queues that report instead of redoing the work, which
//!   covers a report lost to a failed heartbeat or applied on a manager that
//!   could not forward it. Entries are dropped once the manager stops asking.
//! - **restores**: the restore payload of each allocation started from a
//!   snapshot, and its outcome once known, re-sent while the manager keeps
//!   attaching the payload.
//! - **local suspensions**: captures the memory pressure response's
//!   `suspend` step took on its own ([`run_local_suspension`]). The manager
//!   never asked, so it never re-asks: the worker re-sends each report until
//!   the manager drops the allocation from the desired set, which is how it
//!   says the suspend is applied (or that the instance moved on and the
//!   checkpoint is being reclaimed).
//!
//! The work itself (capture, delete, publish, warm) runs on a blocking
//! thread and holds the task-manager lock only for the runtime's own
//! `checkpoint` call and the resume after it, never across volume copies,
//! content-store hashing, or uploads.
//!
//! Phase 7b adds two ops (docs/suspend-and-resume.md, "Durability"):
//!
//! - **publish**: seal a captured snapshot's memory parts with the keys the
//!   manager sent ([`crate::checkpoint_seal`]), upload them to the worker's
//!   `contentStore` ([`crate::content_publish::ContentPublishing`]), and
//!   report the manifest with each part's URL and key id.
//! - **warm**: fetch a published snapshot's parts into the node content
//!   store through the artifact stager, opened with the keys the manager
//!   sent, ahead of any restore.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Instant;

use crate::content_store::{ContentClass, ContentDigest, ContentStore, PinOwner, PutOptions};
use crate::prelude::*;
use crate::structs::checkpoint::{SnapshotKind, SnapshotManifest, MANIFEST_API_VERSION};
use crate::structs::{CheckpointRequest, SnapshotReport, SnapshotRestore, TaskAllocation};
use crate::task_plugins::{AfterCheckpoint, CheckpointDir};
use crate::worker::task_manager::WorkerTaskManager;

/// Checkpoint bookkeeping, held inside [`WorkerTaskManager`].
#[derive(Default)]
pub struct CheckpointState {
    in_flight: HashSet<(String, String)>,
    done: HashMap<(String, String), SnapshotReport>,
    outbox: Vec<SnapshotReport>,
    /// Allocation id -> the snapshot it starts from.
    pub(crate) restores: HashMap<i64, SnapshotRestore>,
    /// Allocation id -> the restore's outcome, once known.
    restore_outcomes: HashMap<i64, SnapshotReport>,
    /// Allocation id -> the report of a local suspension the manager has not
    /// acknowledged yet.
    local_suspensions: HashMap<i64, SnapshotReport>,
}

fn key(snapshot_id: &str, op: &str) -> (String, String) {
    (snapshot_id.to_string(), op.to_string())
}

impl CheckpointState {
    /// Sort this heartbeat's requests into new work (returned, and marked in
    /// flight) and re-sends of finished work (their reports re-queued), and
    /// forget finished work the manager no longer asks about.
    pub fn accept(&mut self, requests: &[CheckpointRequest]) -> Vec<CheckpointRequest> {
        let asked: HashSet<(String, String)> = requests
            .iter()
            .map(|request| key(&request.snapshot_id, &request.op))
            .collect();
        self.done.retain(|key, _| asked.contains(key));
        let mut fresh = Vec::new();
        for request in requests {
            let key = key(&request.snapshot_id, &request.op);
            if self.in_flight.contains(&key) {
                continue;
            }
            // A delete must wait for a capture or a publish of the same
            // snapshot still running here: the capture would pin bytes after
            // the delete released them, and the publish would read bytes
            // the delete removed. The manager re-sends it next heartbeat.
            if request.op == "delete"
                && ["capture", "publish"].iter().any(|op| {
                    self.in_flight
                        .contains(&(request.snapshot_id.clone(), (*op).to_string()))
                })
            {
                continue;
            }
            if let Some(report) = self.done.get(&key) {
                self.outbox.push(report.clone());
                continue;
            }
            self.in_flight.insert(key);
            fresh.push(request.clone());
        }
        fresh
    }

    /// Record finished work and queue its report.
    pub fn finish(&mut self, report: SnapshotReport) {
        let key = key(&report.snapshot_id, &report.op);
        self.in_flight.remove(&key);
        self.done.insert(key, report.clone());
        self.outbox.push(report);
    }

    pub fn drain(&mut self) -> Vec<SnapshotReport> {
        std::mem::take(&mut self.outbox)
    }

    /// Put drained reports back after a failed heartbeat.
    pub fn requeue(&mut self, mut reports: Vec<SnapshotReport>) {
        reports.append(&mut self.outbox);
        self.outbox = reports;
    }

    /// Record a restore's outcome for `allocation_id`, once.
    pub fn record_restore(
        &mut self,
        allocation_id: i64,
        ok: bool,
        error: String,
        started: Instant,
    ) {
        let Some(restore) = self.restores.get(&allocation_id) else {
            return;
        };
        if self.restore_outcomes.contains_key(&allocation_id) {
            return;
        }
        let report = SnapshotReport {
            snapshot_id: restore.snapshot_id.clone(),
            op: "restore".into(),
            ok,
            error,
            manifest_json: String::new(),
            duration_ms: started.elapsed().as_millis() as u64,
            total_bytes: 0,
            allocation_id,
            local_suspension: false,
            pause_ms: 0,
        };
        self.restore_outcomes.insert(allocation_id, report.clone());
        self.outbox.push(report);
    }

    /// Re-send restore outcomes the manager has not applied yet (it still
    /// attaches the payload), and forget the ones it has.
    pub fn reconcile_restores(&mut self, desired: &[TaskAllocation]) {
        let pending: HashSet<i64> = desired
            .iter()
            .filter(|allocation| allocation.restore.is_some())
            .map(|allocation| allocation.allocation_id)
            .collect();
        self.restore_outcomes
            .retain(|allocation_id, _| pending.contains(allocation_id));
        self.restores
            .retain(|allocation_id, _| pending.contains(allocation_id));
        for report in self.restore_outcomes.values() {
            self.outbox.push(report.clone());
        }
    }

    /// Record a local suspension's report and queue it.
    pub fn record_local_suspension(&mut self, report: SnapshotReport) {
        self.local_suspensions
            .insert(report.allocation_id, report.clone());
        self.outbox.push(report);
    }

    /// Re-send local suspensions the manager still lists in the desired set
    /// (not applied yet, or the report was lost), and forget the rest.
    pub fn reconcile_local_suspensions(&mut self, desired: &[TaskAllocation]) {
        let wanted: HashSet<i64> = desired
            .iter()
            .map(|allocation| allocation.allocation_id)
            .collect();
        self.local_suspensions
            .retain(|allocation_id, _| wanted.contains(allocation_id));
        for report in self.local_suspensions.values() {
            let queued = self
                .outbox
                .iter()
                .any(|queued| queued.snapshot_id == report.snapshot_id && queued.op == report.op);
            if !queued {
                self.outbox.push(report.clone());
            }
        }
    }

    pub fn has_pending_restore(&self, allocation_id: i64) -> bool {
        self.restores.contains_key(&allocation_id)
            && !self.restore_outcomes.contains_key(&allocation_id)
    }
}

/// What [`WorkerTaskManager::checkpoint_begin`] hands the capture thread.
pub struct CaptureContext {
    pub allocation: crate::structs::Allocation,
    pub task: crate::structs::TaskConfig,
    pub runtime: String,
    pub memory: crate::structs::checkpoint::MemoryParts,
    pub dir: Option<CheckpointDir>,
    pub paused: bool,
    pub volumes: Arc<crate::worker::volumes::WorkerVolumeManager>,
    pub content: Option<Arc<ContentStore>>,
}

/// Capture one snapshot. Never panics, always returns the report.
pub fn run_capture(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    request: &CheckpointRequest,
) -> SnapshotReport {
    let started = Instant::now();
    let outcome = capture(manager, request);
    let mut report = SnapshotReport {
        snapshot_id: request.snapshot_id.clone(),
        op: "capture".into(),
        ok: false,
        error: String::new(),
        manifest_json: String::new(),
        duration_ms: started.elapsed().as_millis() as u64,
        total_bytes: 0,
        allocation_id: request.allocation_id,
        local_suspension: false,
        pause_ms: 0,
    };
    match outcome {
        Ok((manifest, pause_ms)) => {
            report.ok = true;
            report.pause_ms = pause_ms;
            report.total_bytes = manifest.total_bytes();
            report.manifest_json = serde_json::to_string(&manifest).unwrap_or_default();
            info!(
                snapshot_id = %request.snapshot_id,
                allocation_id = request.allocation_id,
                kind = manifest.kind.as_str(),
                total_bytes = report.total_bytes,
                duration_ms = report.duration_ms,
                "Snapshot captured"
            );
        }
        Err(err) => {
            report.error = format!("{err:#}");
            warn!(
                snapshot_id = %request.snapshot_id,
                allocation_id = request.allocation_id,
                error = %report.error,
                "Snapshot capture failed"
            );
        }
    }
    report
}

/// Capture one snapshot. Returns the manifest and how long the workload was
/// paused (a live memory capture only; 0 otherwise).
fn capture(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    request: &CheckpointRequest,
) -> R<(SnapshotManifest, u64)> {
    let kind: SnapshotKind = request.kind.parse().map_err(|err: String| anyhow!(err))?;
    let then = AfterCheckpoint::from_wire(&request.then)?;
    if then == AfterCheckpoint::Stop {
        // A suspend's guest never resumes, so it has no pause to report.
        return capture_for_suspend(manager, request, kind).map(|manifest| (manifest, 0));
    }
    let owner = PinOwner::snapshot(&request.snapshot_id);
    let pause_started = Instant::now();

    // 1. The runtime's half, under the lock: pause and capture memory (a
    //    memory runtime), or nothing (a disk checkpoint). A live memory
    //    checkpoint stays paused across the volume half.
    let context = manager.lock().expect("task manager").checkpoint_begin(
        request.allocation_id,
        &request.snapshot_id,
        kind,
        then,
    )?;
    let resume = |context: &CaptureContext| {
        if context.paused {
            if let Err(err) = manager
                .lock()
                .expect("task manager")
                .checkpoint_resume(context.allocation.id)
            {
                warn!(
                    allocation_id = context.allocation.id,
                    error = ?err,
                    "Failed to resume the task after a checkpoint"
                );
            }
        }
    };

    // 2. The volume half, without the lock.
    let volumes = context
        .volumes
        .snapshot_instance_volumes(context.allocation.id, &request.snapshot_id);
    resume(&context);
    let pause_ms = if context.paused {
        pause_started.elapsed().as_millis() as u64
    } else {
        0
    };
    let cleanup_dir = |context: &CaptureContext| {
        if let Some(dir) = context.dir.as_ref() {
            let _ = std::fs::remove_dir_all(dir.path());
        }
    };
    let volumes = match volumes {
        Ok(volumes) => volumes,
        Err(err) => {
            cleanup_dir(&context);
            return Err(err);
        }
    };

    // 3. Memory parts into the node content store, pinned to the snapshot,
    //    so they outlive the allocation and its scratch directory.
    let ingested = ingest_memory_parts(&context, &owner, &request.snapshot_id);
    cleanup_dir(&context);
    if let Err(err) = ingested {
        release_snapshot(
            &context.volumes,
            context.content.as_deref(),
            &owner,
            &volumes,
        );
        return Err(err);
    }

    // 4. The manifest.
    Ok((manifest_for(request, &context, kind, volumes), pause_ms))
}

/// A suspend's capture (`then = stop`).
///
/// Ordered so a suspend never stops a workload it failed to capture:
///
/// 1. Under the lock, the runtime pauses the guest and writes its memory
///    (a memory checkpoint), exactly as a live snapshot does. A disk
///    checkpoint captures nothing here.
/// 2. Without the lock, the memory parts go into the content store while
///    the guest stays paused. On any failure the guest is resumed and the
///    capture reports the error: the task keeps running.
/// 3. Only then, under the lock, the paused VMM is killed (it must never run
///    past the captured instant, or its disk would move on from the memory
///    image) and the allocation is torn down with cause `suspended`. A disk
///    checkpoint stops the task gracefully here instead.
///
/// The instance volume is neither copied nor snapshotted: it stays in place
/// on this node, which is what resume restores onto.
pub(crate) fn capture_for_suspend(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    request: &CheckpointRequest,
    kind: SnapshotKind,
) -> R<SnapshotManifest> {
    let owner = PinOwner::snapshot(&request.snapshot_id);
    let context = manager.lock().expect("task manager").checkpoint_begin(
        request.allocation_id,
        &request.snapshot_id,
        kind,
        AfterCheckpoint::Resume,
    )?;
    let resume = |context: &CaptureContext| {
        if context.paused {
            if let Err(err) = manager
                .lock()
                .expect("task manager")
                .checkpoint_resume(context.allocation.id)
            {
                warn!(
                    allocation_id = context.allocation.id,
                    error = ?err,
                    "Failed to resume the task after a failed suspend"
                );
            }
        }
    };
    let ingested = ingest_memory_parts(&context, &owner, &request.snapshot_id);
    if let Some(dir) = context.dir.as_ref() {
        let _ = std::fs::remove_dir_all(dir.path());
    }
    if let Err(err) = ingested {
        release_snapshot(&context.volumes, context.content.as_deref(), &owner, &[]);
        resume(&context);
        return Err(err.context("suspend capture failed; the task keeps running"));
    }
    if let Err(err) = manager
        .lock()
        .expect("task manager")
        .checkpoint_finish_suspend(context.allocation.id, context.paused)
    {
        // The capture is whole; a teardown hiccup must not lose it. The
        // manager deletes the allocation when this report lands, and the
        // next reconcile stops anything left.
        warn!(
            allocation_id = context.allocation.id,
            error = ?err,
            "Suspended allocation did not stop cleanly; the next reconcile finishes it"
        );
    }
    Ok(manifest_for(request, &context, kind, Vec::new()))
}

/// The `suspend` pressure step's capture (docs/resource-enforcement.md,
/// "Memory pressure"): checkpoint and stop `allocation_id` with no request
/// from the manager, under a snapshot id minted here, and queue the report
/// as a local suspension. Returns the snapshot id.
///
/// The same capture a SuspendInstance runs ([`capture_for_suspend`]), so a
/// failure leaves the task running. The allocation is held out of relaunch
/// from the start (the manager still lists it until the report lands) and
/// released again if the capture fails.
pub fn run_local_suspension(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    allocation_id: i64,
) -> R<String> {
    let kind = {
        let mut guard = manager.lock().expect("task manager");
        let kind = guard.local_suspend_kind(allocation_id)?;
        guard.hold_out_of_relaunch(allocation_id);
        kind
    };
    let request = CheckpointRequest {
        snapshot_id: crate::structs::snapshot::new_snapshot_id(),
        op: "capture".into(),
        allocation_id,
        kind: kind.as_str().to_string(),
        then: AfterCheckpoint::Stop.as_str().to_string(),
        purpose: "suspend".into(),
        manifest_json: String::new(),
        ..Default::default()
    };
    let started = Instant::now();
    match capture_for_suspend(manager, &request, kind) {
        Ok(manifest) => {
            let report = SnapshotReport {
                snapshot_id: request.snapshot_id.clone(),
                op: "capture".into(),
                ok: true,
                error: String::new(),
                manifest_json: serde_json::to_string(&manifest).unwrap_or_default(),
                duration_ms: started.elapsed().as_millis() as u64,
                total_bytes: manifest.total_bytes(),
                allocation_id,
                local_suspension: true,
                // A suspend kills the guest after the capture, so there is
                // no running workload whose pause matters.
                pause_ms: 0,
            };
            info!(
                snapshot_id = %request.snapshot_id,
                allocation_id,
                kind = kind.as_str(),
                total_bytes = report.total_bytes,
                duration_ms = report.duration_ms,
                "Suspended for memory pressure"
            );
            manager
                .lock()
                .expect("task manager")
                .checkpoints_mut()
                .record_local_suspension(report);
            Ok(request.snapshot_id)
        }
        Err(err) => {
            manager
                .lock()
                .expect("task manager")
                .release_relaunch_hold(allocation_id);
            Err(err)
        }
    }
}

/// Ingest a capture's memory parts into the node content store, pinned to
/// the snapshot. A no-op for a disk checkpoint.
fn ingest_memory_parts(context: &CaptureContext, owner: &PinOwner, snapshot_id: &str) -> R<()> {
    if context.memory.is_empty() {
        return Ok(());
    }
    let store = context.content.as_ref().context(
        "memory snapshots need the node content store, which is disabled on this worker \
         (content.enabled = false)",
    )?;
    let dir = context.dir.as_ref().context("checkpoint dir missing")?;
    for part in &context.memory.parts {
        store
            .import_file(
                ContentClass::Snapshots,
                &dir.part_path(&part.file),
                PutOptions::default()
                    .expect(ContentDigest::parse(&part.digest)?)
                    .pin(owner.clone())
                    .label(format!("{snapshot_id} {}", part.role)),
            )
            .with_context(|| {
                format!(
                    "failed to store checkpoint part {} in the content store",
                    part.role
                )
            })?;
    }
    Ok(())
}

/// The manifest for a finished capture: what restore and placement need,
/// nothing else.
fn manifest_for(
    request: &CheckpointRequest,
    context: &CaptureContext,
    kind: SnapshotKind,
    volumes: Vec<crate::structs::checkpoint::VolumeSnapshotRef>,
) -> SnapshotManifest {
    let compat = context
        .memory
        .compat
        .clone()
        .with_cpu_from_attrs(&crate::worker::node_check::local_cpu_attrs());
    SnapshotManifest {
        api_version: MANIFEST_API_VERSION,
        id: request.snapshot_id.clone(),
        job_id: context.allocation.job_id.clone(),
        task: context.task.name.clone(),
        origin_instance: context.allocation.instance_id.clone(),
        origin_alloc_id: context.allocation.id,
        runtime: context.runtime.clone(),
        kind,
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        volumes,
        memory: context.memory.parts.clone(),
        // A disk snapshot restores on any node with the volume; only a memory
        // snapshot is tied to the CPU and VMM it ran on.
        compat: if kind == SnapshotKind::Memory {
            compat
        } else {
            Default::default()
        },
        runtime_state: context.memory.runtime.clone(),
    }
}

/// Drop everything a snapshot holds on this node: its content-store pins
/// (and the blobs nothing else pins) and its volume snapshots.
fn release_snapshot(
    volumes: &crate::worker::volumes::WorkerVolumeManager,
    content: Option<&ContentStore>,
    owner: &PinOwner,
    volume_refs: &[crate::structs::checkpoint::VolumeSnapshotRef],
) -> Vec<String> {
    let mut errors = Vec::new();
    if let Some(store) = content {
        match store.pinned_by(owner) {
            Ok(digests) => {
                if let Err(err) = store.release_owner(owner) {
                    errors.push(format!("releasing content-store pins: {err:#}"));
                }
                // Memory parts are big and owned by one snapshot: free them
                // now rather than waiting for LRU or the idle TTL.
                for digest in digests {
                    if store
                        .pins(&digest)
                        .map(|pins| pins.is_empty())
                        .unwrap_or(false)
                    {
                        let _ = store.remove(&digest);
                    }
                }
            }
            Err(err) => errors.push(format!("listing content-store pins: {err:#}")),
        }
    }
    for snapshot in volume_refs {
        if let Err(err) = volumes.delete_volume_snapshot(snapshot) {
            errors.push(format!(
                "deleting volume snapshot {} ({}): {err:#}",
                snapshot.id, snapshot.driver
            ));
        }
    }
    errors
}

/// Reclaim one snapshot. Never panics, always returns the report.
pub fn run_delete(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    request: &CheckpointRequest,
) -> SnapshotReport {
    let started = Instant::now();
    let (volumes, content) = {
        let guard = manager.lock().expect("task manager");
        (guard.volumes_handle(), guard.content_store())
    };
    let mut errors = Vec::new();
    let manifest = if request.manifest_json.trim().is_empty() {
        None
    } else {
        match SnapshotManifest::from_json(&request.manifest_json) {
            Ok(manifest) => Some(manifest),
            Err(err) => {
                errors.push(format!("reading the manifest: {err:#}"));
                None
            }
        }
    };
    errors.extend(release_snapshot(
        &volumes,
        content.as_deref(),
        &PinOwner::snapshot(&request.snapshot_id),
        manifest.as_ref().map_or(&[], |manifest| &manifest.volumes),
    ));
    // A published snapshot's parts were unpinned when it was published, so
    // the pins above no longer name them. Free whichever are still here and
    // unpinned now, as a local snapshot's are, rather than leave them to LRU.
    if let (Some(store), Some(manifest)) = (content.as_deref(), manifest.as_ref()) {
        for part in &manifest.memory {
            if let Ok(digest) = ContentDigest::parse(&part.digest) {
                if store.pins(&digest).is_ok_and(|pins| pins.is_empty()) {
                    let _ = store.remove(&digest);
                }
            }
        }
    }
    if errors.is_empty() {
        info!(snapshot_id = %request.snapshot_id, "Snapshot reclaimed");
    } else {
        warn!(snapshot_id = %request.snapshot_id, errors = ?errors, "Snapshot reclaim failed");
    }
    SnapshotReport {
        snapshot_id: request.snapshot_id.clone(),
        op: "delete".into(),
        ok: errors.is_empty(),
        error: errors.join("; "),
        manifest_json: String::new(),
        duration_ms: started.elapsed().as_millis() as u64,
        total_bytes: 0,
        allocation_id: 0,
        local_suspension: false,
        pause_ms: 0,
    }
}

/// How many times a publish tries each part before reporting failure. The
/// manager does not re-send a failed publish, so transient store errors are
/// retried here.
const PUBLISH_ATTEMPTS: u32 = 3;

/// Publish one captured snapshot (`durability = "store"`). Never panics,
/// always returns the report: on success the manifest with each memory
/// part's `url`, and `total_bytes` = bytes actually uploaded.
pub fn run_publish(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    request: &CheckpointRequest,
) -> SnapshotReport {
    let started = Instant::now();
    let publishing = manager.lock().expect("task manager").content_publishing();
    let outcome = publish(publishing.as_deref(), request);
    if outcome.is_ok() {
        release_published_pins(publishing.as_deref(), &request.snapshot_id);
    }
    let mut report = SnapshotReport {
        snapshot_id: request.snapshot_id.clone(),
        op: "publish".into(),
        ok: false,
        error: String::new(),
        manifest_json: String::new(),
        duration_ms: started.elapsed().as_millis() as u64,
        total_bytes: 0,
        allocation_id: 0,
        local_suspension: false,
        pause_ms: 0,
    };
    match outcome {
        Ok((manifest, uploaded)) => {
            report.ok = true;
            report.total_bytes = uploaded;
            report.manifest_json = serde_json::to_string(&manifest).unwrap_or_default();
            info!(
                snapshot_id = %request.snapshot_id,
                parts = manifest.memory.len(),
                uploaded_bytes = uploaded,
                duration_ms = report.duration_ms,
                "Snapshot published"
            );
        }
        Err(err) => {
            report.error = format!("{err:#}");
            warn!(
                snapshot_id = %request.snapshot_id,
                error = %report.error,
                "Snapshot publish failed; it stays local to this node"
            );
        }
    }
    report
}

/// Unpin a published snapshot's parts on this node, keeping the bytes. They
/// live in the `contentStore` now, so the local copy is a cache entry like
/// any other: LRU, the budget, and `minFreeDisk` may evict it, and a later
/// restore here fetches what is gone (docs/suspend-and-resume.md,
/// "Durability"). Until the evictor gets to them the parts stay advertised,
/// so locality still steers a resume to this node. A failure only leaves the
/// pins in place, which costs room, not correctness.
fn release_published_pins(
    publishing: Option<&crate::content_publish::ContentPublishing>,
    snapshot_id: &str,
) {
    let Some(publishing) = publishing else {
        return;
    };
    match publishing
        .store()
        .release_owner(&PinOwner::snapshot(snapshot_id))
    {
        Ok(released) => debug!(snapshot_id, released, "Unpinned published snapshot parts"),
        Err(err) => warn!(
            snapshot_id,
            error = ?err,
            "Failed to unpin published snapshot parts; they stay pinned until the snapshot is \
             deleted"
        ),
    }
}

/// The publish itself: every memory part, by digest, from the node content
/// store to the configured store. Returns the manifest with URLs and the
/// bytes uploaded.
fn publish(
    publishing: Option<&crate::content_publish::ContentPublishing>,
    request: &CheckpointRequest,
) -> R<(SnapshotManifest, u64)> {
    let publishing = publishing.with_context(|| {
        format!(
            "this worker has no contentStore configured, so it cannot publish snapshot {} \
             (durability = \"store\")\nHint: add contentStore {{ url = \"s3://...\" }} to \
             this worker's config (docs/artifacts.md, \"Publishing content\")",
            request.snapshot_id
        )
    })?;
    let mut manifest = SnapshotManifest::from_json(&request.manifest_json).with_context(|| {
        format!(
            "snapshot {} has an unreadable manifest",
            request.snapshot_id
        )
    })?;
    if let Some(volume) = manifest.volumes.first() {
        bail!(
            "snapshot {} captured volume {} with driver `{}`, which stays on this node and \
             cannot be published",
            manifest.id,
            volume.volume_id,
            volume.driver
        );
    }
    let keys =
        crate::checkpoint_seal::PartKeys::from_proto(&request.part_keys, &request.part_key_error);
    let mut uploaded = 0u64;
    for part in &mut manifest.memory {
        let digest = ContentDigest::parse(&part.digest)?;
        let key = keys.for_part(&digest).with_context(|| {
            format!(
                "publishing part {} of snapshot {} needs its key",
                part.role, manifest.id
            )
        })?;
        let mut attempt = 0;
        let published = loop {
            attempt += 1;
            match publishing.publish(key) {
                Ok(published) => break published,
                Err(err) if attempt < PUBLISH_ATTEMPTS => {
                    warn!(
                        snapshot_id = %manifest.id,
                        part = %part.role,
                        attempt,
                        error = %format!("{err:#}"),
                        "Publishing a snapshot part failed; retrying"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(500 * u64::from(attempt)));
                }
                Err(err) => {
                    return Err(err.context(format!(
                        "publishing part {} ({}) of snapshot {} failed after {attempt} attempts",
                        part.role, part.digest, manifest.id
                    )))
                }
            }
        };
        if published.uploaded {
            uploaded += published.bytes;
        }
        part.url = Some(published.url);
        part.key_id = Some(published.key_id);
    }
    Ok((manifest, uploaded))
}

/// Pre-fetch a published snapshot's parts into the node content store
/// (WarmSnapshot). Never panics, always returns the report: `total_bytes`
/// is what was actually downloaded. Warmed parts are not pinned, so they
/// are ordinary cache entries.
pub fn run_warm(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    request: &CheckpointRequest,
) -> SnapshotReport {
    let started = Instant::now();
    let stager = manager.lock().expect("task manager").stager_handle();
    let outcome = SnapshotManifest::from_json(&request.manifest_json)
        .with_context(|| {
            format!(
                "snapshot {} has an unreadable manifest",
                request.snapshot_id
            )
        })
        .and_then(|manifest| {
            let keys = crate::checkpoint_seal::PartKeys::from_proto(
                &request.part_keys,
                &request.part_key_error,
            );
            fetch_published_parts(&stager, &manifest, &keys, None)
        });
    let mut report = SnapshotReport {
        snapshot_id: request.snapshot_id.clone(),
        op: "warm".into(),
        ok: false,
        error: String::new(),
        manifest_json: String::new(),
        duration_ms: started.elapsed().as_millis() as u64,
        total_bytes: 0,
        allocation_id: 0,
        local_suspension: false,
        pause_ms: 0,
    };
    match outcome {
        Ok(fetched) => {
            report.ok = true;
            report.total_bytes = fetched;
            info!(
                snapshot_id = %request.snapshot_id,
                fetched_bytes = fetched,
                duration_ms = report.duration_ms,
                "Snapshot warmed"
            );
        }
        Err(err) => {
            report.error = format!("{err:#}");
            warn!(snapshot_id = %request.snapshot_id, error = %report.error, "Snapshot warm failed");
        }
    }
    report
}

/// Bring every memory part of `manifest` into the node content store:
/// parts already there are kept (and pinned to `pin`, when given), parts
/// missing are fetched from their published `url` through the artifact
/// stager, opened with their key from `keys`, and verified by digest.
/// Returns the bytes downloaded.
///
/// A published snapshot's parts are unpinned on every node, the one that
/// captured it included, so they can be evicted at any time. A restore
/// passes `pin` to hold them from here until its allocation has started.
///
/// A missing part without a URL is the error a restore of an unpublished
/// snapshot on the wrong node would hit, so it says exactly that.
pub fn fetch_published_parts(
    stager: &crate::artifacts::ArtifactStager,
    manifest: &SnapshotManifest,
    keys: &crate::checkpoint_seal::PartKeys,
    pin: Option<&PinOwner>,
) -> R<u64> {
    let mut fetched = 0u64;
    for part in &manifest.memory {
        let digest = ContentDigest::parse(&part.digest)?;
        let Some(url) = part.url.as_deref() else {
            bail!(
                "snapshot {} part {} ({}) is not on this node and was never published, so it \
                 cannot be fetched (the snapshot is local to the node that took it)",
                manifest.id,
                part.role,
                part.digest
            );
        };
        let mut opts = PutOptions::default()
            .expect(digest.clone())
            .label(format!("{} {}", manifest.id, part.role));
        if let Some(owner) = pin {
            opts = opts.pin(owner.clone());
        }
        // A part this node already holds needs no key: pin it and move on.
        // The stager does the same under its single-flight claim, but only
        // after the key lookup below, which a locked keyring would fail.
        if let Some(store) = stager.content_store() {
            let present = match pin {
                Some(owner) => store.pin_if_present(&digest, owner)?,
                None => store.contains(&digest)?,
            };
            if present && store.get(&digest)?.is_some() {
                continue;
            }
        }
        if part.key_id.is_none() {
            bail!(
                "snapshot {} part {} ({}) was published before checkpoint encryption, and \
                 Legion no longer reads unsealed parts; take a new snapshot (or let \
                 onRestoreFailure boot the instance cold)",
                manifest.id,
                part.role,
                part.digest
            );
        }
        let key = keys.for_part(&digest).with_context(|| {
            format!(
                "fetching part {} of snapshot {} needs its key",
                part.role, manifest.id
            )
        })?;
        let done = stager
            .fetch_sealed_into_store(url, key, ContentClass::Snapshots, opts)
            .with_context(|| {
                format!(
                    "fetching part {} of snapshot {} from its content store",
                    part.role, manifest.id
                )
            })?;
        fetched += done.fetched_bytes;
    }
    Ok(fetched)
}

/// Run one request on the calling (blocking) thread and record its report.
pub fn run_request(manager: &Arc<Mutex<WorkerTaskManager>>, request: &CheckpointRequest) {
    let report = match request.op.as_str() {
        "capture" => run_capture(manager, request),
        "delete" => run_delete(manager, request),
        "publish" => run_publish(manager, request),
        "warm" => run_warm(manager, request),
        other => SnapshotReport {
            snapshot_id: request.snapshot_id.clone(),
            op: other.to_string(),
            ok: false,
            error: format!(
                "this worker does not understand checkpoint op {other:?}; upgrade the worker"
            ),
            manifest_json: String::new(),
            duration_ms: 0,
            total_bytes: 0,
            allocation_id: request.allocation_id,
            local_suspension: false,
            pause_ms: 0,
        },
    };
    manager
        .lock()
        .expect("task manager")
        .checkpoints_mut()
        .finish(report);
}

/// The identity a restored guest refreshes to (suspend plan §6.9, §6.12):
/// the NEW allocation's id and hostname, fresh entropy, the host's clock,
/// the task's `onResume`, and the workload-identity tokens the worker just
/// issued for the new allocation. Those tokens were injected into the task
/// config's `env` before the restore (`WorkerIdentityManager::
/// prepare_task_allocation`), and their `legion.io/alloc` claim names the new
/// allocation, which is what replaces the token the guest captured.
pub fn resume_identity(
    allocation: &crate::structs::Allocation,
    task: &crate::structs::TaskConfig,
    restore: &SnapshotRestore,
) -> crate::structs::checkpoint::ResumeIdentity {
    let task_env = task.config.get("env").and_then(Value::as_object);
    let mut env = std::collections::BTreeMap::new();
    let mut token = None;
    for identity in &task.identities {
        let Some(key) = identity.env.as_deref().filter(|key| !key.trim().is_empty()) else {
            continue;
        };
        if let Some(value) = task_env
            .and_then(|env| env.get(key))
            .and_then(Value::as_str)
        {
            token.get_or_insert_with(|| value.to_string());
            env.insert(key.to_string(), value.to_string());
        }
    }
    env.insert("LEGION_ALLOC_ID".into(), allocation.id.to_string());
    // The manager names the instance on the restore payload (the worker's
    // allocation carries no instance binding). A fork's differs from the
    // one the guest was captured as, so it must be refreshed too.
    let instance_id = Some(restore.instance_id.clone())
        .filter(|id| !id.is_empty())
        .or_else(|| allocation.instance_id.clone());
    if let Some(instance_id) = instance_id.as_deref() {
        env.insert("LEGION_INSTANCE_ID".into(), instance_id.to_string());
    }
    crate::structs::checkpoint::ResumeIdentity {
        alloc_id: allocation.id,
        instance_id,
        job_id: allocation.job_id.clone(),
        task_name: task.name.clone(),
        hostname: format!("legion-sbx-{}", allocation.id),
        token,
        // Phase 6: a guest restored into a new instance must not share its
        // origin's MAC on the wire.
        mac: restore
            .fork
            .then(|| crate::structs::fork::fork_mac(allocation.id)),
        env,
        entropy: crate::task_plugins::checkpoint::fresh_entropy(),
        host_time_unix_ms: chrono::Utc::now().timestamp_millis(),
        on_resume: restore.on_resume.clone(),
        fork: restore.fork,
        // File-delivered tokens live under the task directory, which the
        // sandbox runtime reads when it writes the disk.
        files: Vec::new(),
        // A TAP guest's address is leased by the sandbox runtime on the
        // restoring node, from its bridge's pool.
        guest_addr: None,
    }
}

/// Materialize a memory manifest's parts from the content store into
/// `dir`, under the file names the runtime recorded. Each part is verified
/// (rehashed) first, then hardlinked, or copied when a hardlink is not
/// possible.
pub fn materialize_parts(
    store: Option<&ContentStore>,
    manifest: &SnapshotManifest,
    dir: PathBuf,
) -> R<CheckpointDir> {
    let dir = CheckpointDir::create(dir)?;
    if manifest.memory.is_empty() {
        return Ok(dir);
    }
    let store = store.context(
        "restoring a memory snapshot needs the node content store, which is disabled on this \
         worker (content.enabled = false)",
    )?;
    for part in &manifest.memory {
        let digest = ContentDigest::parse(&part.digest)?;
        let entry = store.verify(&digest).with_context(|| {
            format!(
                "snapshot {} part {} ({}) is missing or corrupt in this node's content store",
                manifest.id, part.role, part.digest
            )
        })?;
        let dest = dir.part_path(&part.file);
        let _ = std::fs::remove_file(&dest);
        if std::fs::hard_link(&entry.path, &dest).is_err() {
            crate::volumes::copy::clone_file(&entry.path, &dest)?;
        }
    }
    dir.verify(&manifest.memory, false)?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: &str, op: &str) -> CheckpointRequest {
        CheckpointRequest {
            snapshot_id: id.into(),
            op: op.into(),
            allocation_id: 7,
            kind: "disk".into(),
            then: "resume".into(),
            purpose: "manual".into(),
            manifest_json: String::new(),
            ..Default::default()
        }
    }

    fn report(id: &str, op: &str) -> SnapshotReport {
        SnapshotReport {
            snapshot_id: id.into(),
            op: op.into(),
            ok: true,
            ..SnapshotReport::default()
        }
    }

    #[test]
    fn a_request_runs_once_and_a_resend_replays_its_report() {
        let mut state = CheckpointState::default();
        let fresh = state.accept(&[request("snap-a", "capture")]);
        assert_eq!(fresh.len(), 1);
        // Still running: a re-send starts nothing.
        assert!(state.accept(&[request("snap-a", "capture")]).is_empty());
        state.finish(report("snap-a", "capture"));
        assert_eq!(state.drain().len(), 1);
        // The report was lost or not applied: the manager asks again, and
        // the recorded report goes out again without a second capture.
        assert!(state.accept(&[request("snap-a", "capture")]).is_empty());
        assert_eq!(state.drain(), vec![report("snap-a", "capture")]);
        // The manager stopped asking: forgotten.
        assert!(state.accept(&[]).is_empty());
        assert_eq!(state.accept(&[request("snap-a", "capture")]).len(), 1);
    }

    #[test]
    fn a_delete_waits_for_a_running_capture_of_the_same_snapshot() {
        let mut state = CheckpointState::default();
        state.accept(&[request("snap-a", "capture")]);
        assert!(state.accept(&[request("snap-a", "delete")]).is_empty());
        state.finish(report("snap-a", "capture"));
        assert_eq!(state.accept(&[request("snap-a", "delete")]).len(), 1);
    }

    #[test]
    fn requeued_reports_go_out_before_new_ones() {
        let mut state = CheckpointState::default();
        state.finish(report("snap-b", "capture"));
        state.requeue(vec![report("snap-a", "capture")]);
        let ids: Vec<String> = state.drain().into_iter().map(|r| r.snapshot_id).collect();
        assert_eq!(ids, vec!["snap-a", "snap-b"]);
    }

    /// A local suspension is re-sent until the manager stops listing its
    /// allocation, never twice in one outbox.
    #[test]
    fn local_suspensions_resend_until_the_manager_drops_the_allocation() {
        let mut state = CheckpointState::default();
        state.record_local_suspension(SnapshotReport {
            allocation_id: 9,
            local_suspension: true,
            ..report("snap-p", "capture")
        });
        let listed = [TaskAllocation {
            allocation_id: 9,
            ..TaskAllocation::default()
        }];
        state.reconcile_local_suspensions(&listed);
        assert_eq!(state.drain().len(), 1, "queued once");
        state.reconcile_local_suspensions(&listed);
        let resent = state.drain();
        assert_eq!(resent.len(), 1);
        assert!(resent[0].local_suspension);
        state.reconcile_local_suspensions(&[]);
        assert!(state.drain().is_empty());
        state.reconcile_local_suspensions(&listed);
        assert!(state.drain().is_empty(), "forgotten once applied");
    }

    #[test]
    fn restore_outcomes_are_reported_once_and_resent_until_applied() {
        let mut state = CheckpointState::default();
        let payload = SnapshotRestore {
            snapshot_id: "snap-a".into(),
            kind: "disk".into(),
            manifest_json: "{}".into(),
            ..SnapshotRestore::default()
        };
        state.restores.insert(9, payload.clone());
        assert!(state.has_pending_restore(9));
        state.record_restore(
            9,
            false,
            "restore needs TAP device tap7: busy".into(),
            Instant::now(),
        );
        // A second outcome for the same allocation is ignored.
        state.record_restore(9, true, String::new(), Instant::now());
        let sent = state.drain();
        assert_eq!(sent.len(), 1);
        assert!(!sent[0].ok);
        assert!(sent[0].error.contains("tap7"));

        let desired = TaskAllocation {
            allocation_id: 9,
            restore: Some(payload),
            ..TaskAllocation::default()
        };
        state.reconcile_restores(std::slice::from_ref(&desired));
        assert_eq!(
            state.drain().len(),
            1,
            "re-sent while the manager still attaches it"
        );
        state.reconcile_restores(&[TaskAllocation {
            allocation_id: 9,
            ..TaskAllocation::default()
        }]);
        assert!(state.drain().is_empty());
        assert!(!state.has_pending_restore(9));
    }
}
