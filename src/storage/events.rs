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

use crate::storage::store::{JobSubmitOutcome, Store};
use crate::structs::{Allocation, Eval, Node};

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
    /// `store.events().job_stopped(name)` — while the kind, level, message,
    /// scopes, and metadata for each event are defined once, in
    /// [`EventRecorder`]. Like `emit_event`, every recorder method is
    /// best-effort and never fails the calling operation.
    pub fn events(&self) -> EventRecorder<'_> {
        EventRecorder { store: self }
    }
}

/// One named constructor per scheduler event kind.
///
/// This is the catalog of everything Legion emits; `docs/event-stream.md`
/// mirrors it. Methods that take a slice of affected rows skip emission when
/// the slice is empty, so callers don't need their own guards.
#[derive(Clone, Copy)]
pub struct EventRecorder<'a> {
    store: &'a Store,
}

impl EventRecorder<'_> {
    fn emit(self, draft: EventDraft) {
        self.store.emit_event(draft);
    }

    /// `scheduler.started` — the eval loop began running.
    pub fn scheduler_started(self, eval_threads: usize) {
        self.emit(
            EventDraft::info(
                "scheduler.started",
                format!("Scheduler started with {eval_threads} eval worker(s)"),
            )
            .meta(json!({ "evalThreads": eval_threads })),
        );
    }

    /// `scheduler.recovered` — stale in-flight evals were requeued at startup.
    pub fn scheduler_recovered(self, requeued_evals: u64) {
        self.emit(
            EventDraft::warn(
                "scheduler.recovered",
                format!("Requeued {requeued_evals} stale in-flight eval(s) at startup"),
            )
            .meta(json!({ "requeued": requeued_evals })),
        );
    }

    /// `eval.enqueued` — an eval was added to the queue.
    pub fn eval_enqueued(self, eval: &Eval, delay: Duration) {
        self.emit(
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
            })),
        );
    }

    /// `eval.completed` — an eval handler finished successfully.
    pub fn eval_completed(self, eval: &Eval, outcome: &str, elapsed: Duration) {
        self.emit(
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
            })),
        );
    }

    /// `eval.failed` — an eval handler returned an error.
    pub fn eval_failed(self, eval: &Eval, error: &anyhow::Error, elapsed: Duration) {
        self.emit(
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
            })),
        );
    }

    /// `allocation.placed` — a plan was applied. One event per apply with up
    /// to 20 placements detailed in metadata. No-op when nothing was placed.
    pub fn allocations_placed(self, job_config_id: &str, placed: &[Allocation]) {
        if placed.is_empty() {
            return;
        }
        let job_id = placed
            .first()
            .map(|alloc| alloc.job_id.clone())
            .unwrap_or_default();
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
        self.emit(
            EventDraft::info(
                "allocation.placed",
                format!("Placed {} allocation(s) for job {job_id}", placed.len()),
            )
            .job(&job_id)
            .meta(json!({
                "jobConfigId": job_config_id,
                "count": placed.len(),
                "placements": placements,
            })),
        );
    }

    /// `allocation.plan_rejected` — a placement plan lost an optimistic
    /// concurrency check and will be retried by the eval handler.
    pub fn plan_rejected(self, job_config_id: &str, reason: &str) {
        self.emit(
            EventDraft::warn(
                "allocation.plan_rejected",
                format!("Plan for job config {job_config_id} rejected: {reason}"),
            )
            .meta(json!({ "jobConfigId": job_config_id })),
        );
    }

    /// `allocation.removed` (warn) — a node's allocations were cleared,
    /// typically because the node was lost. No-op when nothing was removed.
    pub fn allocations_removed_from_node(self, node_id: &str, removed: &[Allocation]) {
        if removed.is_empty() {
            return;
        }
        let affected_jobs: BTreeMap<&str, usize> =
            removed.iter().fold(BTreeMap::new(), |mut jobs, alloc| {
                *jobs.entry(alloc.job_id.as_str()).or_default() += 1;
                jobs
            });
        self.emit(
            EventDraft::warn(
                "allocation.removed",
                format!(
                    "Removed {} allocation(s) from node {node_id}",
                    removed.len()
                ),
            )
            .node(node_id)
            .meta(json!({ "count": removed.len(), "jobs": affected_jobs })),
        );
    }

    /// `allocation.removed` (info) — a job's allocations were removed (stop or
    /// update path). No-op when nothing was removed.
    pub fn allocations_removed_for_job(self, job_id: &str, removed_count: usize) {
        if removed_count == 0 {
            return;
        }
        self.emit(
            EventDraft::info(
                "allocation.removed",
                format!("Removed {removed_count} allocation(s) for job {job_id}"),
            )
            .job(job_id)
            .meta(json!({ "count": removed_count })),
        );
    }

    /// `allocation.removed` (info) — a superseded job config's allocations
    /// were cleaned up. No-op when nothing was removed.
    pub fn allocations_removed_for_job_config(self, job_config_id: &str, removed: &[Allocation]) {
        if removed.is_empty() {
            return;
        }
        let job_id = removed
            .first()
            .map(|alloc| alloc.job_id.clone())
            .unwrap_or_default();
        self.emit(
            EventDraft::info(
                "allocation.removed",
                format!(
                    "Removed {} allocation(s) for job config {job_config_id}",
                    removed.len()
                ),
            )
            .job(&job_id)
            .meta(json!({
                "count": removed.len(),
                "jobConfigId": job_config_id,
            })),
        );
    }

    /// `allocation.failed` — a worker reported an allocation failure.
    pub fn allocation_failed(
        self,
        allocation_id: i64,
        node_id: &str,
        job_id: &str,
        job_config_id: &str,
        task_config_id: i64,
        reason: &str,
    ) {
        self.emit(
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
            })),
        );
    }

    /// `node.registered` — a worker registered (or re-registered).
    pub fn node_registered(self, node: &Node) {
        self.emit(
            EventDraft::info(
                "node.registered",
                format!("Node {} registered in pool {}", node.id, node.pool),
            )
            .node(&node.id)
            .meta(json!({ "pool": node.pool, "status": node.status })),
        );
    }

    /// `node.ready` — a known node rejoined and was marked ready.
    pub fn node_ready(self, node_id: &str) {
        self.emit(EventDraft::info("node.ready", format!("Node {node_id} is ready")).node(node_id));
    }

    /// `node.lost` — a node was explicitly marked lost.
    pub fn node_lost(self, node_id: &str) {
        self.emit(
            EventDraft::warn("node.lost", format!("Node {node_id} marked lost")).node(node_id),
        );
    }

    /// `node.lost` — the health checker marked a node lost after missed
    /// heartbeats.
    pub fn node_lost_after_missed_heartbeats(self, node_id: &str) {
        self.emit(
            EventDraft::warn(
                "node.lost",
                format!("Node {node_id} marked lost after missed heartbeats"),
            )
            .node(node_id)
            .meta(json!({ "reason": "heartbeat" })),
        );
    }

    /// `job.submitted` / `job.updated` — a job config was written. Unchanged
    /// outcomes are a no-op (nothing happened worth streaming).
    pub fn job_submit_outcome(
        self,
        job_id: &str,
        outcome: &JobSubmitOutcome,
        version: u32,
        task_count: usize,
    ) {
        let (kind, action) = match outcome {
            JobSubmitOutcome::Created { .. } => ("job.submitted", "submitted"),
            JobSubmitOutcome::Updated { .. } => ("job.updated", "updated"),
            JobSubmitOutcome::Unchanged { .. } => return,
        };
        self.emit(
            EventDraft::info(kind, format!("Job {job_id} {action} (version {version})"))
                .job(job_id)
                .meta(json!({
                    "jobConfigId": outcome.job_config_id(),
                    "version": version,
                    "tasks": task_count,
                })),
        );
    }

    /// `job.stopped` — a job was stopped.
    pub fn job_stopped(self, job_id: &str) {
        self.emit(EventDraft::info("job.stopped", format!("Job {job_id} stopped")).job(job_id));
    }

    /// `job.failed` — a job was marked failed (e.g. move limit exceeded).
    pub fn job_failed(self, job_id: &str) {
        self.emit(
            EventDraft::error("job.failed", format!("Job {job_id} marked failed")).job(job_id),
        );
    }

    /// `job.destroyed` — a job and its records were removed.
    pub fn job_destroyed(self, job_id: &str) {
        self.emit(EventDraft::info("job.destroyed", format!("Job {job_id} destroyed")).job(job_id));
    }

    /// `rollout.event` — mirror of one rollout event into the unified stream,
    /// so consumers see rollout activity without polling per-rollout history.
    pub fn rollout_event(
        self,
        rollout_id: i64,
        job_id: &str,
        level: &str,
        message: &str,
        group_name: Option<&str>,
        metadata_json: &str,
    ) {
        let details: Value = serde_json::from_str(metadata_json).unwrap_or_else(|_| json!({}));
        let level = match level {
            EVENT_LEVEL_WARN | EVENT_LEVEL_ERROR => level,
            _ => EVENT_LEVEL_INFO,
        };
        self.emit(
            EventDraft::new(level, "rollout.event", message)
                .job(job_id)
                .rollout(rollout_id)
                .meta(json!({
                    "groupName": group_name,
                    "details": details,
                })),
        );
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
}
