//! Scheduler event journal types.
//!
//! The manager records a `SchedulerEventRecord` whenever the scheduler does
//! something observable (eval queue activity, allocation changes, node and job
//! lifecycle transitions, rollout activity). Events are persisted to the
//! `scheduler_event` table for history/cursor queries and simultaneously
//! published on an in-process broadcast channel that backs the `StreamEvents`
//! gRPC stream. See `Store::emit_event`.

use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

use crate::artifacts::status::{ArtifactState, ArtifactStatus};
use crate::storage::store::{JobSubmitOutcome, Store};
use crate::structs::{Allocation, Eval, Node, StopCause};

/// Severity levels used by scheduler events.
pub const EVENT_LEVEL_INFO: &str = "info";
pub const EVENT_LEVEL_WARN: &str = "warn";
pub const EVENT_LEVEL_ERROR: &str = "error";

/// One persisted scheduler event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchedulerEventRecord {
    pub id: i64,
    pub kind: String,
    pub level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eval_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocation_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollout_id: Option<i64>,
    pub message: String,
    pub metadata_json: String,
    pub created_at: String,
}

/// A scheduler event waiting to be recorded.
///
/// Built fluently at emission sites and handed to `Store::emit_event`:
///
/// ```ignore
/// store.emit_event(
///     EventDraft::info("eval.completed", "Eval 7 (job_submit) completed")
///         .job("demo")
///         .eval("7"),
/// );
/// ```
#[derive(Debug, Clone)]
pub struct EventDraft {
    pub kind: String,
    pub level: String,
    pub message: String,
    pub job_id: Option<String>,
    pub node_id: Option<String>,
    pub eval_id: Option<String>,
    pub allocation_id: Option<i64>,
    pub rollout_id: Option<i64>,
    pub metadata: Value,
}

impl EventDraft {
    pub fn new(level: &str, kind: &str, message: impl Into<String>) -> Self {
        EventDraft {
            kind: kind.to_string(),
            level: level.to_string(),
            message: message.into(),
            job_id: None,
            node_id: None,
            eval_id: None,
            allocation_id: None,
            rollout_id: None,
            metadata: json!({}),
        }
    }

    pub fn info(kind: &str, message: impl Into<String>) -> Self {
        Self::new(EVENT_LEVEL_INFO, kind, message)
    }

    pub fn warn(kind: &str, message: impl Into<String>) -> Self {
        Self::new(EVENT_LEVEL_WARN, kind, message)
    }

    pub fn error(kind: &str, message: impl Into<String>) -> Self {
        Self::new(EVENT_LEVEL_ERROR, kind, message)
    }

    /// Scope the event to a job. Blank IDs are ignored.
    pub fn job(mut self, job_id: &str) -> Self {
        if !job_id.trim().is_empty() {
            self.job_id = Some(job_id.to_string());
        }
        self
    }

    /// Scope the event to a node. Blank IDs are ignored.
    pub fn node(mut self, node_id: &str) -> Self {
        if !node_id.trim().is_empty() {
            self.node_id = Some(node_id.to_string());
        }
        self
    }

    /// Scope the event to an eval. Blank IDs are ignored.
    pub fn eval(mut self, eval_id: &str) -> Self {
        if !eval_id.trim().is_empty() {
            self.eval_id = Some(eval_id.to_string());
        }
        self
    }

    pub fn allocation(mut self, allocation_id: i64) -> Self {
        self.allocation_id = Some(allocation_id);
        self
    }

    pub fn rollout(mut self, rollout_id: i64) -> Self {
        self.rollout_id = Some(rollout_id);
        self
    }

    /// Attach kind-specific structured details. Must be a JSON object.
    pub fn meta(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

impl Store {
    /// Named emission facade for scheduler events.
    ///
    /// Call sites stay one line and read as domain facts —
    /// `store.events().job_stopped(name)`. Each recorder method persists and
    /// publishes immediately, taking the store's write lock for the insert;
    /// use it from code that is *not* already holding a write transaction.
    /// Hot paths that do hold one stage the same [`drafts`] inside their
    /// transaction instead (see `Store::stage_event_locked`), which costs no
    /// extra lock acquisition or WAL commit.
    ///
    /// Like `emit_event`, every recorder method is best-effort and never
    /// fails the calling operation.
    pub fn events(&self) -> EventRecorder<'_> {
        EventRecorder { store: self }
    }
}

/// Pure constructors for every scheduler event kind Legion emits.
///
/// This module is the authoritative catalog of the stream's kinds — the
/// table in `docs/event-stream.md` mirrors it. Each constructor owns its
/// event's kind, level, message wording, scopes, and metadata shape; call
/// sites only supply domain data.
///
/// Constructors return `Option<EventDraft>` when there can be nothing worth
/// emitting (empty slices, unchanged outcomes), so callers need no guards.
pub mod drafts {
    use super::*;

    /// `scheduler.started` — the eval loop began running.
    pub fn scheduler_started(eval_threads: usize) -> EventDraft {
        EventDraft::info(
            "scheduler.started",
            format!("Scheduler started with {eval_threads} eval worker(s)"),
        )
        .meta(json!({ "evalThreads": eval_threads }))
    }

    /// `scheduler.recovered` — stale in-flight evals were requeued at startup.
    pub fn scheduler_recovered(requeued_evals: u64) -> EventDraft {
        EventDraft::warn(
            "scheduler.recovered",
            format!("Requeued {requeued_evals} stale in-flight eval(s) at startup"),
        )
        .meta(json!({ "requeued": requeued_evals }))
    }

    /// `eval.enqueued` — an eval was added to the queue.
    pub fn eval_enqueued(eval: &Eval, delay: Duration) -> EventDraft {
        EventDraft::info(
            "eval.enqueued",
            format!("Eval {} ({}) enqueued", eval.id, eval.kind),
        )
        .job(&eval.job_id)
        .node(&eval.node_name)
        .eval(&eval.id)
        .meta(json!({
            "evalKind": eval.kind,
            "priority": eval.priority,
            "delayMs": delay.as_millis() as u64,
        }))
    }

    /// `eval.completed` — an eval handler finished successfully.
    pub fn eval_completed(eval: &Eval, outcome: &str, elapsed: Duration) -> EventDraft {
        EventDraft::info(
            "eval.completed",
            format!("Eval {} ({}) completed: {outcome}", eval.id, eval.kind),
        )
        .job(&eval.job_id)
        .node(&eval.node_name)
        .eval(&eval.id)
        .meta(json!({
            "evalKind": eval.kind,
            "elapsedMs": elapsed.as_millis() as u64,
        }))
    }

    /// `eval.failed` — an eval handler returned an error.
    pub fn eval_failed(eval: &Eval, error: &anyhow::Error, elapsed: Duration) -> EventDraft {
        EventDraft::error(
            "eval.failed",
            format!("Eval {} ({}) failed: {error:#}", eval.id, eval.kind),
        )
        .job(&eval.job_id)
        .node(&eval.node_name)
        .eval(&eval.id)
        .meta(json!({
            "evalKind": eval.kind,
            "elapsedMs": elapsed.as_millis() as u64,
            "error": format!("{error:#}"),
        }))
    }

    /// `allocation.placed` — a plan was applied. One event per apply with up
    /// to 20 placements detailed in metadata. `None` when nothing was placed.
    pub fn allocations_placed(job_config_id: &str, placed: &[Allocation]) -> Option<EventDraft> {
        let first = placed.first()?;
        let placements: Vec<Value> = placed
            .iter()
            .take(20)
            .map(|alloc| {
                json!({
                    "allocationId": alloc.id,
                    "nodeId": alloc.node_id,
                    "taskConfigId": alloc.task_config_id,
                })
            })
            .collect();
        Some(
            EventDraft::info(
                "allocation.placed",
                format!(
                    "Placed {} allocation(s) for job {}",
                    placed.len(),
                    first.job_id
                ),
            )
            .job(&first.job_id)
            .meta(json!({
                "jobConfigId": job_config_id,
                "count": placed.len(),
                "placements": placements,
            })),
        )
    }

    /// `allocation.plan_rejected` — a placement plan lost an optimistic
    /// concurrency check and will be retried by the eval handler.
    pub fn plan_rejected(job_config_id: &str, reason: &str) -> EventDraft {
        EventDraft::warn(
            "allocation.plan_rejected",
            format!("Plan for job config {job_config_id} rejected: {reason}"),
        )
        .meta(json!({ "jobConfigId": job_config_id }))
    }

    /// `allocation.removed` (warn) — a node's allocations were cleared,
    /// typically because the node was lost. `None` when nothing was removed.
    pub fn allocations_removed_from_node(
        node_id: &str,
        removed: &[Allocation],
        cause: StopCause,
    ) -> Option<EventDraft> {
        if removed.is_empty() {
            return None;
        }
        let affected_jobs: BTreeMap<&str, usize> =
            removed.iter().fold(BTreeMap::new(), |mut jobs, alloc| {
                *jobs.entry(alloc.job_id.as_str()).or_default() += 1;
                jobs
            });
        let mut meta = json!({ "count": removed.len(), "jobs": affected_jobs });
        if cause.is_known() {
            meta["cause"] = json!(cause.as_str());
        }
        Some(
            EventDraft::warn(
                "allocation.removed",
                format!(
                    "Removed {} allocation(s) from node {node_id}",
                    removed.len()
                ),
            )
            .node(node_id)
            .meta(meta),
        )
    }

    /// `allocation.removed` (info) — a job's allocations were removed (stop
    /// or update path). `None` when nothing was removed.
    ///
    /// `cause` names the reason so the journal distinguishes a `job stop`
    /// from an update or a drain; it is omitted from the metadata when
    /// unknown rather than recorded as the string `"unknown"`.
    pub fn allocations_removed_for_job(
        job_id: &str,
        removed_count: usize,
        cause: StopCause,
    ) -> Option<EventDraft> {
        if removed_count == 0 {
            return None;
        }
        let mut meta = json!({ "count": removed_count });
        if cause.is_known() {
            meta["cause"] = json!(cause.as_str());
        }
        Some(
            EventDraft::info(
                "allocation.removed",
                format!("Removed {removed_count} allocation(s) for job {job_id}"),
            )
            .job(job_id)
            .meta(meta),
        )
    }

    /// `allocation.removed` (info) — a superseded job config's allocations
    /// were cleaned up. `None` when nothing was removed.
    pub fn allocations_removed_for_job_config(
        job_config_id: &str,
        removed: &[Allocation],
    ) -> Option<EventDraft> {
        let first = removed.first()?;
        Some(
            EventDraft::info(
                "allocation.removed",
                format!(
                    "Removed {} allocation(s) for job config {job_config_id}",
                    removed.len()
                ),
            )
            .job(&first.job_id)
            .meta(json!({
                "count": removed.len(),
                "jobConfigId": job_config_id,
                "cause": StopCause::Update.as_str(),
            })),
        )
    }

    /// `allocation.failed` — a worker reported an allocation failure.
    pub fn allocation_failed(
        allocation_id: i64,
        node_id: &str,
        job_id: &str,
        job_config_id: &str,
        task_config_id: i64,
        reason: &str,
    ) -> EventDraft {
        EventDraft::warn(
            "allocation.failed",
            format!("Allocation {allocation_id} failed on node {node_id}: {reason}"),
        )
        .job(job_id)
        .node(node_id)
        .allocation(allocation_id)
        .meta(json!({
            "reason": reason,
            "taskConfigId": task_config_id,
            "jobConfigId": job_config_id,
        }))
    }

    /// `allocation.terminated` — a task process exited and the allocation
    /// reached a terminal state without being rescheduled (see
    /// docs/task-supervision.md). `success` distinguishes "the batch task
    /// finished" from "it terminated without succeeding".
    pub fn allocation_terminated(
        allocation_id: i64,
        node_id: &str,
        job_id: &str,
        job_config_id: &str,
        task_config_id: i64,
        success: bool,
        message: &str,
    ) -> EventDraft {
        let text = format!("Allocation {allocation_id} terminated on node {node_id}: {message}");
        let draft = if success {
            EventDraft::info("allocation.terminated", text)
        } else {
            EventDraft::warn("allocation.terminated", text)
        };
        draft
            .job(job_id)
            .node(node_id)
            .allocation(allocation_id)
            .meta(json!({
                "success": success,
                "message": message,
                "taskConfigId": task_config_id,
                "jobConfigId": job_config_id,
            }))
    }

    /// `node.registered` — a worker registered (or re-registered).
    pub fn node_registered(node: &Node) -> EventDraft {
        EventDraft::info(
            "node.registered",
            format!("Node {} registered in pool {}", node.id, node.pool),
        )
        .node(&node.id)
        .meta(json!({ "pool": node.pool, "status": node.status }))
    }

    /// `node.ready` — a known node rejoined and was marked ready.
    pub fn node_ready(node_id: &str) -> EventDraft {
        EventDraft::info("node.ready", format!("Node {node_id} is ready")).node(node_id)
    }

    /// `node.lost` — a node was explicitly marked lost.
    pub fn node_lost(node_id: &str) -> EventDraft {
        EventDraft::warn("node.lost", format!("Node {node_id} marked lost")).node(node_id)
    }

    /// `node.lost` — the health checker marked a node lost after missed
    /// heartbeats.
    pub fn node_lost_after_missed_heartbeats(node_id: &str) -> EventDraft {
        EventDraft::warn(
            "node.lost",
            format!("Node {node_id} marked lost after missed heartbeats"),
        )
        .node(node_id)
        .meta(json!({ "reason": "heartbeat" }))
    }

    /// `node.cordoned` — a node's availability was set to `drained`, so it
    /// no longer receives new placements (existing allocations untouched
    /// until a drain evicts them).
    pub fn node_cordoned(node_id: &str) -> EventDraft {
        EventDraft::info(
            "node.cordoned",
            format!("Node {node_id} cordoned (no new placements)"),
        )
        .node(node_id)
    }

    /// `node.uncordoned` — a node's availability was restored to
    /// `available`, so it re-entered the scheduling pool.
    pub fn node_uncordoned(node_id: &str) -> EventDraft {
        EventDraft::info(
            "node.uncordoned",
            format!("Node {node_id} uncordoned (schedulable again)"),
        )
        .node(node_id)
    }

    /// `node.drained` — a drain pass evicted a drained node's allocations
    /// for replacement elsewhere. Stateful allocations protected by their
    /// `onLost` policy stay behind and are counted in `held`.
    pub fn node_drained(node_id: &str, evicted: usize, held: usize) -> EventDraft {
        EventDraft::info(
            "node.drained",
            format!("Node {node_id} drained: {evicted} allocation(s) evicted, {held} held"),
        )
        .node(node_id)
        .meta(json!({ "evicted": evicted, "held": held }))
    }

    /// `job.submitted` / `job.updated` — a job config was written. `None` for
    /// unchanged outcomes (nothing happened worth streaming).
    pub fn job_submit_outcome(
        job_id: &str,
        outcome: &JobSubmitOutcome,
        version: u32,
        task_count: usize,
    ) -> Option<EventDraft> {
        let (kind, action) = match outcome {
            JobSubmitOutcome::Created { .. } => ("job.submitted", "submitted"),
            JobSubmitOutcome::Updated { .. } => ("job.updated", "updated"),
            JobSubmitOutcome::Unchanged { .. } => return None,
        };
        Some(
            EventDraft::info(kind, format!("Job {job_id} {action} (version {version})"))
                .job(job_id)
                .meta(json!({
                    "jobConfigId": outcome.job_config_id(),
                    "version": version,
                    "tasks": task_count,
                })),
        )
    }

    /// `job.stopped` — a job was stopped.
    pub fn job_stopped(job_id: &str) -> EventDraft {
        EventDraft::info("job.stopped", format!("Job {job_id} stopped")).job(job_id)
    }

    /// `job.failed` — a job was marked failed (e.g. move limit exceeded).
    pub fn job_failed(job_id: &str) -> EventDraft {
        EventDraft::error("job.failed", format!("Job {job_id} marked failed")).job(job_id)
    }

    /// `job.destroyed` — a job and its records were removed.
    pub fn job_destroyed(job_id: &str) -> EventDraft {
        EventDraft::info("job.destroyed", format!("Job {job_id} destroyed")).job(job_id)
    }

    /// `rollout.event` — mirror of one rollout event into the unified stream,
    /// so consumers see rollout activity without polling per-rollout history.
    pub fn rollout_event(
        rollout_id: i64,
        job_id: &str,
        level: &str,
        message: &str,
        group_name: Option<&str>,
        metadata_json: &str,
    ) -> EventDraft {
        let details: Value = serde_json::from_str(metadata_json).unwrap_or_else(|_| json!({}));
        let level = match level {
            EVENT_LEVEL_WARN | EVENT_LEVEL_ERROR => level,
            _ => EVENT_LEVEL_INFO,
        };
        EventDraft::new(level, "rollout.event", message)
            .job(job_id)
            .rollout(rollout_id)
            .meta(json!({
                "groupName": group_name,
                "details": details,
            }))
    }

    /// `artifact.staging` — an allocation began staging its artifacts
    /// (derived from the first heartbeat snapshot reporting artifact rows).
    pub fn artifact_staging(
        allocation_id: i64,
        node_id: &str,
        job_id: &str,
        rows: &[ArtifactStatus],
    ) -> EventDraft {
        EventDraft::info(
            "artifact.staging",
            format!(
                "Allocation {allocation_id} staging {} artifact(s)",
                rows.len()
            ),
        )
        .job(job_id)
        .node(node_id)
        .allocation(allocation_id)
        .meta(json!({
            "count": rows.len(),
            "artifacts": artifact_rows_meta(rows),
        }))
    }

    /// `artifact.staged` — every artifact for an allocation staged
    /// successfully (digests/bytes/durations in metadata).
    pub fn artifact_staged(
        allocation_id: i64,
        node_id: &str,
        job_id: &str,
        rows: &[ArtifactStatus],
    ) -> EventDraft {
        let bytes: u64 = rows.iter().filter_map(|row| row.bytes_fetched).sum();
        EventDraft::info(
            "artifact.staged",
            format!(
                "Allocation {allocation_id} staged {} artifact(s)",
                rows.len()
            ),
        )
        .job(job_id)
        .node(node_id)
        .allocation(allocation_id)
        .meta(json!({
            "count": rows.len(),
            "bytes": bytes,
            "artifacts": artifact_rows_meta(rows),
        }))
    }

    /// `artifact.failed` — one artifact's fetch failed (the allocation-level
    /// `allocation.failed` event follows separately through the failure
    /// path). Sources are redacted by the worker; errors name auth sources,
    /// never credential values.
    pub fn artifact_failed(
        allocation_id: i64,
        node_id: &str,
        job_id: &str,
        row: &ArtifactStatus,
    ) -> EventDraft {
        let label = row.name.as_deref().unwrap_or(&row.source);
        EventDraft::warn(
            "artifact.failed",
            format!(
                "Allocation {allocation_id} artifact {label} failed: {}",
                row.error.as_deref().unwrap_or("unknown error")
            ),
        )
        .job(job_id)
        .node(node_id)
        .allocation(allocation_id)
        .meta(json!({
            "name": row.name,
            "source": row.source,
            "error": row.error,
            "attempts": row.attempts,
        }))
    }

    /// `volume.mounted` — a worker materialized a volume at a host path. For
    /// a node-local volume this is also the moment its instance becomes pinned
    /// to that node.
    pub fn volume_mounted(
        volume: &crate::structs::Volume,
        node_id: &str,
        path: &str,
    ) -> EventDraft {
        let mut draft = EventDraft::info(
            "volume.mounted",
            format!(
                "Volume {} ({}) mounted on {node_id} at {path}",
                volume.volume_id, volume.driver
            ),
        )
        .node(node_id)
        .meta(json!({
            "volumeId": volume.volume_id,
            "driver": volume.driver,
            "name": volume.name,
            "instanceId": volume.instance_id,
            "nodeLocal": volume.node_local,
            "mountPath": path,
        }));
        if let Some(job_id) = volume.job_id.as_deref() {
            draft = draft.job(job_id);
        }
        draft
    }

    /// `volume.failed` — a driver operation failed. Surfaced as a warning, not
    /// an error: the allocation's own failure event carries the fatal signal,
    /// and reclaim failures are retried.
    pub fn volume_failed(volume_id: &str, driver: &str, node_id: &str, error: &str) -> EventDraft {
        EventDraft::warn(
            "volume.failed",
            format!("Volume {volume_id} ({driver}) failed on {node_id}: {error}"),
        )
        .node(node_id)
        .meta(json!({
            "volumeId": volume_id,
            "driver": driver,
            "error": error,
        }))
    }

    /// `volume.reclaiming` — the volume's instance no longer references it, so
    /// its storage is queued for the driver's `destroy`.
    pub fn volume_reclaiming(volume: &crate::structs::Volume) -> EventDraft {
        let mut draft = EventDraft::info(
            "volume.reclaiming",
            format!(
                "Volume {} ({}) queued for reclamation",
                volume.volume_id, volume.driver
            ),
        )
        .meta(json!({
            "volumeId": volume.volume_id,
            "driver": volume.driver,
            "instanceId": volume.instance_id,
            "nodeId": volume.node_id,
        }));
        if let Some(job_id) = volume.job_id.as_deref() {
            draft = draft.job(job_id);
        }
        if let Some(node_id) = volume.node_id.as_deref() {
            draft = draft.node(node_id);
        }
        draft
    }

    /// `volume.destroyed` — a driver confirmed the storage is gone.
    pub fn volume_destroyed(volume_id: &str, driver: &str, node_id: &str) -> EventDraft {
        EventDraft::info(
            "volume.destroyed",
            format!("Volume {volume_id} ({driver}) destroyed on {node_id}"),
        )
        .node(node_id)
        .meta(json!({ "volumeId": volume_id, "driver": driver }))
    }

    /// `artifact.pinned` — mutable references were pinned to digests at
    /// submit (artifact system plan §7). One event per submit, all pins in
    /// metadata. `None` when nothing was pinned.
    pub fn artifact_pinned(job_id: &str, pins: &[ArtifactPin]) -> Option<EventDraft> {
        if pins.is_empty() {
            return None;
        }
        let entries: Vec<Value> = pins
            .iter()
            .take(20)
            .map(|pin| {
                json!({
                    "task": pin.task,
                    "from": pin.from,
                    "to": pin.to,
                })
            })
            .collect();
        Some(
            EventDraft::info(
                "artifact.pinned",
                format!("Job {job_id} pinned {} artifact reference(s)", pins.len()),
            )
            .job(job_id)
            .meta(json!({
                "count": pins.len(),
                "pins": entries,
            })),
        )
    }

    /// Per-artifact metadata rows shared by the staging/staged drafts
    /// (capped like `allocation.placed` placements).
    fn artifact_rows_meta(rows: &[ArtifactStatus]) -> Vec<Value> {
        rows.iter()
            .take(20)
            .map(|row| {
                json!({
                    "name": row.name,
                    "source": row.source,
                    "state": row.state,
                    "digest": row.digest,
                    "bytes": row.bytes_fetched,
                    "durationMs": row.duration_ms,
                })
            })
            .collect()
    }
}

/// One submit-time pin resolution (mutable reference -> digest), surfaced in
/// `job plan` output and the `artifact.pinned` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPin {
    pub task: String,
    /// The original mutable reference (redacted form).
    pub from: String,
    /// The digest/SHA it resolved to.
    pub to: String,
}

impl ArtifactPin {
    /// Human-readable rendering used in `job plan` messages.
    pub fn describe(&self) -> String {
        format!("task {}: pinned {} -> {}", self.task, self.from, self.to)
    }
}

/// Artifact staging transitions derived from two consecutive heartbeat
/// `artifact_states` snapshots for one allocation. The heartbeat repeats the
/// same snapshot every beat, so events fire only on *changes*; the pure
/// derivation lives here so the store's hot path stays one call.
#[derive(Debug, Default, PartialEq)]
pub struct ArtifactTransitions {
    /// The allocation entered staging (no previous rows, or a fresh staging
    /// run after a previously completed one — e.g. an in-place relaunch).
    pub staging_started: bool,
    /// Every artifact reached `staged` (fires once per staging run).
    pub all_staged: bool,
}

/// Aggregate state of one snapshot's rows.
fn artifact_summary(rows: &[ArtifactStatus]) -> &'static str {
    if rows.is_empty() {
        return "none";
    }
    if rows.iter().any(|row| row.state == ArtifactState::Failed) {
        return "failed";
    }
    if rows.iter().all(|row| row.state == ArtifactState::Staged) {
        return "staged";
    }
    "staging"
}

/// Computes transitions between the previously persisted `artifact_states`
/// JSON (if any) and the incoming one. Returns the parsed new rows alongside
/// so emission sites don't parse twice.
pub fn artifact_transitions(
    previous: Option<&str>,
    new: &str,
) -> (ArtifactTransitions, Vec<ArtifactStatus>) {
    let new_rows: Vec<ArtifactStatus> = serde_json::from_str(new).unwrap_or_default();
    let previous_rows: Vec<ArtifactStatus> = previous
        .map(|raw| serde_json::from_str(raw).unwrap_or_default())
        .unwrap_or_default();

    let new_summary = artifact_summary(&new_rows);
    let previous_summary = artifact_summary(&previous_rows);

    let transitions = ArtifactTransitions {
        staging_started: new_summary == "staging" && previous_summary != "staging",
        all_staged: new_summary == "staged" && previous_summary != "staged",
    };
    (transitions, new_rows)
}

/// Immediate-emit sugar over [`drafts`] for call sites that are not already
/// inside a write transaction.
#[derive(Clone, Copy)]
pub struct EventRecorder<'a> {
    store: &'a Store,
}

impl EventRecorder<'_> {
    fn emit(self, draft: EventDraft) {
        self.store.emit_event(draft);
    }

    fn emit_opt(self, draft: Option<EventDraft>) {
        if let Some(draft) = draft {
            self.emit(draft);
        }
    }

    pub fn scheduler_started(self, eval_threads: usize) {
        self.emit(drafts::scheduler_started(eval_threads));
    }

    pub fn scheduler_recovered(self, requeued_evals: u64) {
        self.emit(drafts::scheduler_recovered(requeued_evals));
    }

    pub fn allocations_removed_from_node(
        self,
        node_id: &str,
        removed: &[Allocation],
        cause: StopCause,
    ) {
        self.emit_opt(drafts::allocations_removed_from_node(
            node_id, removed, cause,
        ));
    }

    pub fn allocations_removed_for_job(self, job_id: &str, removed_count: usize, cause: StopCause) {
        self.emit_opt(drafts::allocations_removed_for_job(
            job_id,
            removed_count,
            cause,
        ));
    }

    pub fn allocations_removed_for_job_config(self, job_config_id: &str, removed: &[Allocation]) {
        self.emit_opt(drafts::allocations_removed_for_job_config(
            job_config_id,
            removed,
        ));
    }

    /// Emit [`drafts::allocation_terminated`].
    #[allow(clippy::too_many_arguments)]
    pub fn allocation_terminated(
        self,
        allocation_id: i64,
        node_id: &str,
        job_id: &str,
        job_config_id: &str,
        task_config_id: i64,
        success: bool,
        message: &str,
    ) {
        self.emit(drafts::allocation_terminated(
            allocation_id,
            node_id,
            job_id,
            job_config_id,
            task_config_id,
            success,
            message,
        ));
    }

    pub fn allocation_failed(
        self,
        allocation_id: i64,
        node_id: &str,
        job_id: &str,
        job_config_id: &str,
        task_config_id: i64,
        reason: &str,
    ) {
        self.emit(drafts::allocation_failed(
            allocation_id,
            node_id,
            job_id,
            job_config_id,
            task_config_id,
            reason,
        ));
    }

    pub fn node_registered(self, node: &Node) {
        self.emit(drafts::node_registered(node));
    }

    pub fn node_ready(self, node_id: &str) {
        self.emit(drafts::node_ready(node_id));
    }

    pub fn node_lost(self, node_id: &str) {
        self.emit(drafts::node_lost(node_id));
    }

    pub fn node_lost_after_missed_heartbeats(self, node_id: &str) {
        self.emit(drafts::node_lost_after_missed_heartbeats(node_id));
    }

    pub fn node_cordoned(self, node_id: &str) {
        self.emit(drafts::node_cordoned(node_id));
    }

    pub fn node_uncordoned(self, node_id: &str) {
        self.emit(drafts::node_uncordoned(node_id));
    }

    pub fn node_drained(self, node_id: &str, evicted: usize, held: usize) {
        self.emit(drafts::node_drained(node_id, evicted, held));
    }

    pub fn job_stopped(self, job_id: &str) {
        self.emit(drafts::job_stopped(job_id));
    }

    pub fn job_failed(self, job_id: &str) {
        self.emit(drafts::job_failed(job_id));
    }

    pub fn job_destroyed(self, job_id: &str) {
        self.emit(drafts::job_destroyed(job_id));
    }

    pub fn rollout_event(
        self,
        rollout_id: i64,
        job_id: &str,
        level: &str,
        message: &str,
        group_name: Option<&str>,
        metadata_json: &str,
    ) {
        self.emit(drafts::rollout_event(
            rollout_id,
            job_id,
            level,
            message,
            group_name,
            metadata_json,
        ));
    }

    /// Emits one `artifact.failed` per failed row in a failure report's
    /// `artifact_states` JSON. The worker attaches the final board snapshot
    /// to artifact-staging failures, so this is the reliable (race-free)
    /// source of per-artifact failure detail.
    pub fn artifact_failed_rows(
        self,
        allocation_id: i64,
        node_id: &str,
        job_id: &str,
        artifact_states: &str,
    ) {
        let rows: Vec<ArtifactStatus> = serde_json::from_str(artifact_states).unwrap_or_default();
        for row in rows.iter().filter(|row| row.state == ArtifactState::Failed) {
            self.emit(drafts::artifact_failed(allocation_id, node_id, job_id, row));
        }
    }

    pub fn artifact_pinned(self, job_id: &str, pins: &[ArtifactPin]) {
        self.emit_opt(drafts::artifact_pinned(job_id, pins));
    }

    pub fn volume_mounted(self, volume: &crate::structs::Volume, node_id: &str, path: &str) {
        self.emit(drafts::volume_mounted(volume, node_id, path));
    }

    pub fn volume_failed(self, volume_id: &str, driver: &str, node_id: &str, error: &str) {
        self.emit(drafts::volume_failed(volume_id, driver, node_id, error));
    }

    pub fn volume_reclaiming(self, volume: &crate::structs::Volume) {
        self.emit(drafts::volume_reclaiming(volume));
    }

    pub fn volume_destroyed(self, volume_id: &str, driver: &str, node_id: &str) {
        self.emit(drafts::volume_destroyed(volume_id, driver, node_id));
    }
}

/// Filter applied when listing or streaming scheduler events.
#[derive(Debug, Clone, Default)]
pub struct SchedulerEventFilter {
    /// Exclusive lower-bound event ID.
    pub after_id: Option<i64>,
    /// Max events returned by a list call (default 100, clamped to 1..=1000).
    pub limit: Option<u32>,
    /// Kind filter: exact match or category prefix (`eval` matches `eval.*`).
    pub kind: Option<String>,
    pub job_id: Option<String>,
    pub node_id: Option<String>,
    pub level: Option<String>,
}

impl SchedulerEventFilter {
    pub fn effective_limit(&self) -> u32 {
        self.limit.unwrap_or(100).clamp(1, 1_000)
    }

    /// Whether a live event passes this filter. Mirrors the SQL applied by
    /// `Store::list_scheduler_events` so streamed and listed results agree.
    pub fn matches(&self, event: &SchedulerEventRecord) -> bool {
        if let Some(after_id) = self.after_id {
            if event.id <= after_id {
                return false;
            }
        }
        if let Some(kind) = self.kind.as_deref() {
            let prefix_match = event
                .kind
                .strip_prefix(kind)
                .is_some_and(|rest| rest.starts_with('.'));
            if event.kind != kind && !prefix_match {
                return false;
            }
        }
        if let Some(job_id) = self.job_id.as_deref() {
            if event.job_id.as_deref() != Some(job_id) {
                return false;
            }
        }
        if let Some(node_id) = self.node_id.as_deref() {
            if event.node_id.as_deref() != Some(node_id) {
                return false;
            }
        }
        if let Some(level) = self.level.as_deref() {
            if event.level != level {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: i64, kind: &str, level: &str) -> SchedulerEventRecord {
        SchedulerEventRecord {
            id,
            kind: kind.into(),
            level: level.into(),
            job_id: Some("demo".into()),
            node_id: None,
            eval_id: None,
            allocation_id: None,
            rollout_id: None,
            message: "test".into(),
            metadata_json: "{}".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
        }
    }

    #[test]
    fn draft_builder_skips_blank_scopes() {
        let draft = EventDraft::info("eval.completed", "done")
            .job("  ")
            .node("node-1")
            .eval("9");
        assert_eq!(draft.job_id, None);
        assert_eq!(draft.node_id.as_deref(), Some("node-1"));
        assert_eq!(draft.eval_id.as_deref(), Some("9"));
        assert_eq!(draft.level, EVENT_LEVEL_INFO);
    }

    #[test]
    fn filter_kind_matches_exact_and_category_prefix() {
        let filter = SchedulerEventFilter {
            kind: Some("eval".into()),
            ..Default::default()
        };
        assert!(filter.matches(&record(1, "eval.completed", "info")));
        assert!(filter.matches(&record(2, "eval", "info")));
        assert!(!filter.matches(&record(3, "evaluation.completed", "info")));
        assert!(!filter.matches(&record(4, "job.submitted", "info")));
    }

    #[test]
    fn filter_applies_after_id_level_and_scopes() {
        let filter = SchedulerEventFilter {
            after_id: Some(5),
            level: Some("warn".into()),
            job_id: Some("demo".into()),
            ..Default::default()
        };
        assert!(filter.matches(&record(6, "node.lost", "warn")));
        assert!(!filter.matches(&record(5, "node.lost", "warn")));
        assert!(!filter.matches(&record(6, "node.lost", "info")));

        let other_job = SchedulerEventRecord {
            job_id: Some("other".into()),
            ..record(7, "node.lost", "warn")
        };
        assert!(!filter.matches(&other_job));
    }

    #[test]
    fn effective_limit_defaults_and_clamps() {
        assert_eq!(SchedulerEventFilter::default().effective_limit(), 100);
        let big = SchedulerEventFilter {
            limit: Some(9_999),
            ..Default::default()
        };
        assert_eq!(big.effective_limit(), 1_000);
        let zero = SchedulerEventFilter {
            limit: Some(0),
            ..Default::default()
        };
        assert_eq!(zero.effective_limit(), 1);
    }

    // --- artifact staging transitions (artifact <-> event stream wiring) ---

    fn rows_json(states: &[&str]) -> String {
        let rows: Vec<Value> = states
            .iter()
            .enumerate()
            .map(|(index, state)| {
                json!({
                    "name": format!("a{index}"),
                    "source": "https://example.com/a.tar.gz",
                    "state": state,
                })
            })
            .collect();
        serde_json::to_string(&rows).expect("rows json")
    }

    #[test]
    fn transitions_fire_once_per_staging_run() {
        // First snapshot: staging begins.
        let staging = rows_json(&["pending", "fetching"]);
        let (transitions, rows) = artifact_transitions(None, &staging);
        assert!(transitions.staging_started);
        assert!(!transitions.all_staged);
        assert_eq!(rows.len(), 2);

        // Heartbeats repeat the same snapshot: nothing new.
        let (transitions, _) = artifact_transitions(Some(&staging), &staging);
        assert_eq!(transitions, ArtifactTransitions::default());

        // Progress within staging is still staging: no event.
        let progressing = rows_json(&["staged", "fetching"]);
        let (transitions, _) = artifact_transitions(Some(&staging), &progressing);
        assert_eq!(transitions, ArtifactTransitions::default());

        // Everything staged: the staged event fires once…
        let staged = rows_json(&["staged", "staged"]);
        let (transitions, _) = artifact_transitions(Some(&progressing), &staged);
        assert!(!transitions.staging_started);
        assert!(transitions.all_staged);

        // …and repeated staged snapshots stay quiet.
        let (transitions, _) = artifact_transitions(Some(&staged), &staged);
        assert_eq!(transitions, ArtifactTransitions::default());
    }

    #[test]
    fn relaunch_after_staged_restarts_the_cycle() {
        // An in-place relaunch resets the board to pending: that is a new
        // staging run and should announce itself again.
        let staged = rows_json(&["staged"]);
        let pending = rows_json(&["pending"]);
        let (transitions, _) = artifact_transitions(Some(&staged), &pending);
        assert!(transitions.staging_started);
        assert!(!transitions.all_staged);
    }

    #[test]
    fn failed_rows_do_not_count_as_staged() {
        let staging = rows_json(&["fetching", "fetching"]);
        let failed = rows_json(&["staged", "failed"]);
        let (transitions, _) = artifact_transitions(Some(&staging), &failed);
        assert!(!transitions.all_staged);
        assert!(!transitions.staging_started);
    }

    #[test]
    fn unparseable_or_empty_snapshots_stay_quiet() {
        let (transitions, rows) = artifact_transitions(None, "not json");
        assert_eq!(transitions, ArtifactTransitions::default());
        assert!(rows.is_empty());

        let (transitions, _) = artifact_transitions(None, "[]");
        assert_eq!(transitions, ArtifactTransitions::default());
    }

    #[test]
    fn artifact_drafts_carry_scopes_and_metadata() {
        let rows: Vec<crate::artifacts::status::ArtifactStatus> =
            serde_json::from_str(&rows_json(&["fetching"])).expect("rows");
        let draft = drafts::artifact_staging(7, "node-1", "demo", &rows);
        assert_eq!(draft.kind, "artifact.staging");
        assert_eq!(draft.allocation_id, Some(7));
        assert_eq!(draft.job_id.as_deref(), Some("demo"));
        assert_eq!(draft.metadata["count"], 1);

        let mut failed_row = rows[0].clone();
        failed_row.state = crate::artifacts::status::ArtifactState::Failed;
        failed_row.error = Some("403 from endpoint (credentials from kv secrets/pull)".into());
        failed_row.attempts = Some(3);
        let draft = drafts::artifact_failed(7, "node-1", "demo", &failed_row);
        assert_eq!(draft.kind, "artifact.failed");
        assert_eq!(draft.level, EVENT_LEVEL_WARN);
        assert!(draft.message.contains("a0"), "{}", draft.message);
        assert_eq!(draft.metadata["attempts"], 3);
    }

    #[test]
    fn artifact_pinned_draft_skips_empty_and_lists_pins() {
        assert!(drafts::artifact_pinned("demo", &[]).is_none());

        let pins = vec![ArtifactPin {
            task: "api".into(),
            from: "oci://ghcr.io/acme/api:v42".into(),
            to: format!("sha256:{}", "a".repeat(64)),
        }];
        let draft = drafts::artifact_pinned("demo", &pins).expect("pin draft");
        assert_eq!(draft.kind, "artifact.pinned");
        assert_eq!(draft.job_id.as_deref(), Some("demo"));
        assert_eq!(draft.metadata["pins"][0]["task"], "api");
        assert!(pins[0].describe().contains("pinned oci://"));
    }
}
