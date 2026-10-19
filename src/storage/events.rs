//! Scheduler event journal types.
//!
//! The manager records a `SchedulerEventRecord` whenever the scheduler does
//! something observable (eval queue activity, allocation changes, node and job
//! lifecycle transitions, rollout activity). Events are persisted to the
//! `scheduler_event` table for history/cursor queries and simultaneously
//! published on an in-process broadcast channel that backs the `StreamEvents`
//! gRPC stream. See `Store::emit_event`.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

use crate::artifacts::status::{ArtifactState, ArtifactStatus};
use crate::manager::scheduler::PlannedPreemption;
use crate::storage::store::{JobSubmitOutcome, Store};
use crate::structs::{Allocation, Band, Eval, Node, StopCause};

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
///
/// Serializable because a manager that leads no raft lane forwards its
/// standalone events to a lane leader (`ForwardableLaneWrite::EmitEvent`).
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// `scheduler.concurrency_adjusted` — the adaptive concurrency controller
    /// moved the number of planners it admits
    /// (docs/adaptive-scheduler-concurrency.md).
    ///
    /// Emitted on the CHANGE, never per tick: a control loop that narrates
    /// every 500ms tick is noise an operator learns to ignore.
    pub fn scheduler_concurrency_adjusted(
        direction: &str,
        from: usize,
        to: usize,
        ceiling: usize,
        conflict_rate: f64,
        mode: &str,
    ) -> EventDraft {
        let shadow = if mode == "enforce" { "" } else { " (shadow)" };
        EventDraft::info(
            "scheduler.concurrency_adjusted",
            format!(
                "Admitted planners {from} -> {to} of {ceiling}{shadow}: conflict rate \
                 {percent:.1}%",
                percent = conflict_rate * 100.0,
            ),
        )
        .meta(json!({
            "direction": direction,
            "from": from,
            "to": to,
            "ceiling": ceiling,
            "conflictRate": conflict_rate,
            "mode": mode,
        }))
    }

    /// `scheduler.concurrency_saturated` — backing off did not improve
    /// goodput, so the conflicts are capacity-driven rather than
    /// concurrency-driven. The distinction matters because "add nodes" and
    /// "wait" are very different responses, and no other signal separates
    /// them.
    pub fn scheduler_concurrency_saturated(
        restored_to: usize,
        goodput_before: f64,
        goodput_after: f64,
    ) -> EventDraft {
        EventDraft::warn(
            "scheduler.concurrency_saturated",
            format!(
                "Reducing scheduler concurrency did not improve goodput \
                 ({goodput_before:.0} -> {goodput_after:.0} placements/sec); conflicts are \
                 capacity-driven, not concurrency-driven. Restored the limit to \
                 {restored_to} — add nodes rather than waiting"
            ),
        )
        .meta(json!({
            "restoredTo": restored_to,
            "goodputBefore": goodput_before,
            "goodputAfter": goodput_after,
        }))
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

    /// `eval.requeued` — an eval exhausted its in-handler plan-conflict
    /// retry budget and was released back to the pending queue with a delay
    /// instead of being terminally failed. Conflict is transient by
    /// definition, so the work stays visibly pending and runs again once the
    /// delay elapses (docs/late-binding-plan-apply.md).
    pub fn eval_requeued(eval: &Eval, error: &anyhow::Error, delay: Duration) -> EventDraft {
        EventDraft::warn(
            "eval.requeued",
            format!(
                "Eval {} ({}) requeued after plan conflicts; retrying in {}ms",
                eval.id,
                eval.kind,
                delay.as_millis()
            ),
        )
        .job(&eval.job_id)
        .node(&eval.node_name)
        .eval(&eval.id)
        .meta(json!({
            "evalKind": eval.kind,
            "delayMs": delay.as_millis() as u64,
            "error": format!("{error:#}"),
        }))
    }

    /// `eval.blocked` — an eval found no room for a placement and now waits
    /// under `key` for capacity that could help, instead of failing
    /// (docs/scheduler-and-eval-loop.md, "Blocked evals").
    pub fn eval_blocked(eval: &Eval, key: &str, detail: &Value, message: &str) -> EventDraft {
        EventDraft::warn(
            "eval.blocked",
            format!(
                "Eval {} ({}) blocked on {key}: {message}",
                eval.id, eval.kind
            ),
        )
        .job(&eval.job_id)
        .node(&eval.node_name)
        .eval(&eval.id)
        .meta(json!({
            "evalKind": eval.kind,
            "key": key,
            "detail": detail,
        }))
    }

    /// `eval.unblocked` — capacity that could help a blocked eval appeared
    /// (`cause`: `register`, `node`, `ready`, `freed`, or `missed` when it
    /// appeared while the eval was still running), or a manager starting as
    /// the scheduler's writer gave it one more look (`startup`), and the
    /// eval is back in the pending queue.
    pub fn eval_unblocked(
        eval_id: &str,
        job_id: &str,
        key: &str,
        cause: &str,
        node_id: &str,
    ) -> EventDraft {
        EventDraft::info(
            "eval.unblocked",
            format!("Eval {eval_id} unblocked by {cause} on node {node_id} (was blocked on {key})"),
        )
        .job(job_id)
        .node(node_id)
        .eval(eval_id)
        .meta(json!({ "key": key, "cause": cause }))
    }

    /// `allocation.placed` — a plan was applied. One event per apply with up
    /// to 20 placements detailed in metadata. `None` when nothing was placed.
    ///
    /// `candidate_ranks` is parallel to `placed`: the candidate rank each
    /// placement bound at (0 = the planner's first choice; >0 = the apply
    /// walk absorbed a race by falling back —
    /// docs/late-binding-plan-apply.md). The metadata carries the rank per
    /// placement and a top-level `fallbacks` count.
    pub fn allocations_placed(
        job_config_id: &str,
        placed: &[Allocation],
        candidate_ranks: &[usize],
    ) -> Option<EventDraft> {
        let first = placed.first()?;
        let placements: Vec<Value> = placed
            .iter()
            .enumerate()
            .take(20)
            .map(|(index, alloc)| {
                json!({
                    "allocationId": alloc.id,
                    "nodeId": alloc.node_id,
                    "taskConfigId": alloc.task_config_id,
                    "candidateRank": candidate_ranks.get(index).copied().unwrap_or(0),
                })
            })
            .collect();
        let fallbacks = candidate_ranks.iter().filter(|rank| **rank > 0).count();
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
                "fallbacks": fallbacks,
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

    /// `allocation.stopping` (info) — allocations have entered their grace
    /// window. `None` when nothing was removed or the cause is unknown.
    ///
    /// Distinct from `allocation.removed`, which says the rows are gone. This
    /// says the *processes* are being asked to exit and names how long they
    /// have — the interval that did not exist before graceful stop, and the
    /// one an operator watching a drain actually wants to see.
    ///
    /// `grace_by_task` maps task config id to `stop.killAfter` seconds. The
    /// window is the manager's expectation: the worker's real clock starts
    /// when it delivers the signal, up to one heartbeat later.
    pub fn allocations_stopping(
        removed: &[Allocation],
        cause: StopCause,
        grace_by_task: &std::collections::HashMap<i64, u64>,
    ) -> Option<EventDraft> {
        if removed.is_empty() || !cause.is_known() {
            return None;
        }
        let first = removed.first()?;
        let max_grace = removed
            .iter()
            .filter_map(|alloc| grace_by_task.get(&alloc.task_config_id).copied())
            .max()
            .unwrap_or(0);
        let allocations: Vec<Value> = removed
            .iter()
            .take(20)
            .map(|alloc| {
                json!({
                    "allocationId": alloc.id,
                    "nodeId": alloc.node_id,
                    "taskConfigId": alloc.task_config_id,
                    "graceSeconds": grace_by_task.get(&alloc.task_config_id).copied(),
                })
            })
            .collect();
        Some(
            EventDraft::info(
                "allocation.stopping",
                format!(
                    "Stopping {} allocation(s) for job {} ({cause}), up to {max_grace}s grace",
                    removed.len(),
                    first.job_id
                ),
            )
            .job(&first.job_id)
            .meta(json!({
                "count": removed.len(),
                "cause": cause.as_str(),
                "maxGraceSeconds": max_grace,
                "allocations": allocations,
            })),
        )
    }

    /// `allocation.preempted` (warn) — allocations were displaced to make
    /// room for higher-priority work, naming the preemptor.
    ///
    /// The victim -> preemptor link is the point: "Preempted" on its own is
    /// the complaint Kueue had to retrofit an answer to (its users could not
    /// tell *which* policy displaced them). One event per victim job, so a
    /// `legion events list --kind allocation.preempted` reads as a list of
    /// who lost what to whom.
    pub fn allocations_preempted(
        victims: &[Allocation],
        victim_band: Band,
        preemptor_job: &str,
        preemptor_band: Band,
    ) -> Option<EventDraft> {
        let first = victims.first()?;
        let allocations: Vec<Value> = victims
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
            EventDraft::warn(
                "allocation.preempted",
                format!(
                    "Preempted {} allocation(s) of job {} ({}) to place job {} ({})",
                    victims.len(),
                    first.job_id,
                    victim_band,
                    preemptor_job,
                    preemptor_band
                ),
            )
            .job(&first.job_id)
            .meta(json!({
                "count": victims.len(),
                "band": victim_band.as_str(),
                "preemptorJob": preemptor_job,
                "preemptorBand": preemptor_band.as_str(),
                "allocations": allocations,
            })),
        )
    }

    /// `allocation.preempt_planned` (info) — observe mode. The victim
    /// selection ran and named these allocations, and nothing was evicted.
    ///
    /// This is the evidence an operator asked for before enabling
    /// `enforce`: it says exactly what enforce mode *would* have done, in the
    /// same vocabulary, without doing it.
    pub fn preemption_planned(
        job_config_id: &str,
        preemptor_job: &str,
        preemptor_band: Band,
        victims: &[PlannedPreemption],
    ) -> Option<EventDraft> {
        if victims.is_empty() {
            return None;
        }
        let detail: Vec<Value> = victims
            .iter()
            .take(20)
            .map(|victim| {
                json!({
                    "allocationId": victim.allocation_id,
                    "nodeId": victim.node_id,
                    "jobId": victim.job_id,
                    "taskName": victim.task_name,
                    "band": victim.band.as_str(),
                })
            })
            .collect();
        Some(
            EventDraft::info(
                "allocation.preempt_planned",
                format!(
                    "Would preempt {} allocation(s) to place job {preemptor_job} \
                     ({preemptor_band}); preemption mode is observe, nothing was evicted",
                    victims.len()
                ),
            )
            .job(preemptor_job)
            .meta(json!({
                "jobConfigId": job_config_id,
                "count": victims.len(),
                "preemptorBand": preemptor_band.as_str(),
                "victims": detail,
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

    /// `allocation.reschedule_deferred` — a failed allocation's replacement
    /// waits out the task's reschedule backoff (`on_unhealthy.move_delay`,
    /// doubled per consecutive failure, capped at 30 minutes).
    /// Nothing re-places the task before `nextAttemptAt` unless an operator
    /// lifts the hold (`job run --force-reschedule`, or `instance start` for
    /// one stateful instance). See docs/jobs-tasks-and-scheduling.md.
    pub fn allocation_reschedule_deferred(
        hold: &crate::storage::store::RescheduleHold,
    ) -> EventDraft {
        let subject = match hold.instance_id.as_deref() {
            Some(instance_id) => format!("task {} (instance {instance_id})", hold.task_name),
            None => format!("task {}", hold.task_name),
        };
        EventDraft::warn(
            "allocation.reschedule_deferred",
            format!(
                "Replacement for allocation {} of {subject} deferred: {}",
                hold.allocation_id,
                hold.summary()
            ),
        )
        .job(&hold.job_id)
        .node(&hold.node_id)
        .allocation(hold.allocation_id)
        .meta(json!({
            "jobConfigId": hold.job_config_id,
            "taskConfigId": hold.task_config_id,
            "task": hold.task_name,
            "group": hold.group,
            "instanceId": hold.instance_id,
            "attempt": hold.attempt,
            "delaySecs": hold.delay_secs,
            "nextAttemptAt": hold.next_attempt_at(),
            "reason": hold.reason,
        }))
    }

    /// `allocation.reschedule_exhausted` — a failed allocation found its
    /// task's move budget spent (`move_count` moves inside `move_period`):
    /// the task is not moved again, and the job is marked failed (a
    /// `job.failed` event follows).
    pub fn allocation_reschedule_exhausted(
        allocation: &Allocation,
        task_name: &str,
        moves: u32,
        move_count: u32,
        move_period_secs: u64,
        reason: &str,
    ) -> EventDraft {
        EventDraft::error(
            "allocation.reschedule_exhausted",
            format!(
                "Allocation {} of task {task_name} failed after {moves} moves in {}s \
                 (move_count {move_count}); marking job {} failed: {reason}",
                allocation.id, move_period_secs, allocation.job_id
            ),
        )
        .job(&allocation.job_id)
        .node(&allocation.node_id)
        .allocation(allocation.id)
        .meta(json!({
            "jobConfigId": allocation.job_config_id,
            "taskConfigId": allocation.task_config_id,
            "task": task_name,
            "instanceId": allocation.instance_id,
            "moves": moves,
            "moveCount": move_count,
            "movePeriodSecs": move_period_secs,
            "reason": reason,
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

    /// `alloc.provisional_started` — a node started work before the
    /// placement was durable (docs/edge-boot.md).
    ///
    /// The ticket appears only as its `jti`: no ticket material ever reaches
    /// an event, a log line, or a store row.
    #[allow(clippy::too_many_arguments)]
    pub fn provisional_started(
        claim_id: &str,
        node_id: &str,
        job_id: &str,
        job_config_id: &str,
        task_name: &str,
        instance_id: &str,
        gate: &str,
        origin: &str,
        ticket_jti: &str,
    ) -> EventDraft {
        EventDraft::info(
            "alloc.provisional_started",
            format!(
                "Provisional start {claim_id} on node {node_id} at gate {gate} \
                 ({origin}-originated)"
            ),
        )
        .job(job_id)
        .node(node_id)
        .meta(json!({
            "claimId": claim_id,
            "jobConfigId": job_config_id,
            "taskName": task_name,
            "instanceId": instance_id,
            "gate": gate,
            "origin": origin,
            "ticketJti": ticket_jti,
        }))
    }

    /// `alloc.ratified` — a provisional allocation became durable.
    #[allow(clippy::too_many_arguments)]
    pub fn provisional_ratified(
        claim_id: &str,
        allocation_id: i64,
        node_id: &str,
        job_id: &str,
        instance_id: &str,
        gate: &str,
        latency_ms: u64,
    ) -> EventDraft {
        EventDraft::info(
            "alloc.ratified",
            format!(
                "Provisional start {claim_id} ratified as allocation {allocation_id} \
                 in {latency_ms}ms"
            ),
        )
        .job(job_id)
        .node(node_id)
        .allocation(allocation_id)
        .meta(json!({
            "claimId": claim_id,
            "instanceId": instance_id,
            "gate": gate,
            "latencyMs": latency_ms,
        }))
    }

    /// `alloc.refused` — a ratification was refused and the node is killing
    /// the work. The reason code is what an on-call responder reads to tell
    /// "my router double-sent" from "my cluster is full".
    pub fn provisional_refused(
        claim_id: &str,
        node_id: &str,
        job_id: &str,
        instance_id: &str,
        reason: &str,
        detail: &str,
    ) -> EventDraft {
        EventDraft::warn(
            "alloc.refused",
            format!("Provisional start {claim_id} refused ({reason}): {detail}"),
        )
        .job(job_id)
        .node(node_id)
        .meta(json!({
            "claimId": claim_id,
            "instanceId": instance_id,
            "reason": reason,
            "detail": detail,
        }))
    }

    /// `boot_ticket.issued` — a boot ticket was minted. The audit record for
    /// "who authorized this start"; `jti` and the accessor id only.
    #[allow(clippy::too_many_arguments)]
    pub fn boot_ticket_issued(
        jti: &str,
        job: &str,
        task: &str,
        job_config_id: &str,
        instance_id: &str,
        instance_prefix: &str,
        pool: &str,
        gate: &str,
        accessor_id: &str,
        expires_at_unix: i64,
    ) -> EventDraft {
        EventDraft::info(
            "boot_ticket.issued",
            format!("Boot ticket {jti} issued for {job}/{task} in pool {pool} at gate {gate}"),
        )
        .job(job)
        .meta(json!({
            "jti": jti,
            "task": task,
            "jobConfigId": job_config_id,
            "instanceId": instance_id,
            "instancePrefix": instance_prefix,
            "pool": pool,
            "gate": gate,
            "accessorId": accessor_id,
            "expiresAtUnix": expires_at_unix,
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

    /// `job.kv_render_refused` — a watched KV change re-rendered the job's
    /// templates into lookups of keys the job never named, so the job keeps
    /// its last render (docs/kv-store.md, the `templateKv` paragraphs).
    pub fn job_kv_render_refused(job_id: &str, keys: &[String]) -> EventDraft {
        EventDraft::warn(
            "job.kv_render_refused",
            format!(
                "Job {job_id} kept its last render: a KV change made its templates read {}, \
                 which the job did not name when it was submitted; resubmit the job to \
                 read them",
                keys.join(", ")
            ),
        )
        .job(job_id)
        .meta(json!({ "keys": keys }))
    }

    /// `job.gang_timeout` — a gang did not reach its quorum within its
    /// `placementTimeout`, so its job was failed and any survivors stopped
    /// (docs/gang-scheduling.md, "Placement timeout").
    pub fn job_gang_timeout(
        job_id: &str,
        task: &str,
        timeout_secs: u64,
        waited_secs: u64,
        stopped: usize,
        message: &str,
    ) -> EventDraft {
        EventDraft::error("job.gang_timeout", message.to_string())
            .job(job_id)
            .meta(json!({
                "task": task,
                "placementTimeoutSeconds": timeout_secs,
                "waitedSeconds": waited_secs,
                "stopped": stopped,
            }))
    }

    /// `job.gang_member_lost` — a gang member was lost (it failed, its node
    /// was lost or drained, or memory pressure evicted it) and what that did
    /// to the gang: replaced alone, the whole gang restarted, or the job
    /// failed (docs/gang-scheduling.md, "When a member is lost").
    pub fn job_gang_member_lost(
        lost: &Allocation,
        cause: StopCause,
        loss: &crate::storage::store::GangLoss,
    ) -> EventDraft {
        use crate::structs::MemberLossPolicy;
        let state = if loss.running >= loss.min {
            format!(
                "{} run, at or above its quorum of {}",
                loss.running, loss.min
            )
        } else {
            format!("{} run, below its quorum of {}", loss.running, loss.min)
        };
        let action = match loss.action {
            MemberLossPolicy::Replace if loss.running >= loss.min => {
                "replacing the lost member".to_string()
            }
            MemberLossPolicy::Replace => {
                "keeping the survivors and placing the missing members together".to_string()
            }
            MemberLossPolicy::Restart => {
                format!("restarting the whole gang ({} stopped)", loss.stopped)
            }
            MemberLossPolicy::Fail => {
                format!("failing the job ({} allocation(s) stopped)", loss.stopped)
            }
        };
        let cause = if cause.is_known() {
            cause.as_str()
        } else {
            "failed"
        };
        let message = format!(
            "Gang {} of job {} lost allocation {} on node {} ({cause}): {state}; {action}",
            loss.task_name, loss.job_id, lost.id, lost.node_id,
        );
        let draft = if loss.action == MemberLossPolicy::Replace {
            EventDraft::info("job.gang_member_lost", message)
        } else {
            EventDraft::warn("job.gang_member_lost", message)
        };
        draft.job(&loss.job_id).node(&lost.node_id).meta(json!({
            "task": loss.task_name,
            "allocationId": lost.id,
            "cause": cause,
            "action": loss.action.as_str(),
            "running": loss.running,
            "min": loss.min,
            "stopped": loss.stopped,
        }))
    }

    /// `job.destroyed` — a job and its records were removed.
    pub fn job_destroyed(job_id: &str) -> EventDraft {
        EventDraft::info("job.destroyed", format!("Job {job_id} destroyed")).job(job_id)
    }

    /// `scale.applied` — a count override was applied
    /// (`legion job scale`, docs/autoscaling.md). `previous` is the
    /// effective count before the change when the caller knows it.
    pub fn scale_applied(
        job_id: &str,
        task_group: &str,
        task_name: &str,
        previous: Option<u32>,
        count: u32,
        source: &str,
        reason: &str,
    ) -> EventDraft {
        let from = previous
            .map(|previous| format!("{previous} -> "))
            .unwrap_or_default();
        EventDraft::info(
            "scale.applied",
            format!("Job {job_id} task {task_group}/{task_name} scaled {from}{count}"),
        )
        .job(job_id)
        .meta(json!({
            "group": task_group,
            "task": task_name,
            "previous": previous,
            "count": count,
            "source": source,
            "reason": reason,
        }))
    }

    /// `scale.deferred` — a scale-in was recorded during an active rollout
    /// and waits for it to finish (docs/autoscaling.md).
    pub fn scale_deferred(
        job_id: &str,
        task_group: &str,
        task_name: &str,
        pending: u32,
        rollout_id: i64,
        source: &str,
        reason: &str,
    ) -> EventDraft {
        EventDraft::info(
            "scale.deferred",
            format!(
                "Job {job_id} task {task_group}/{task_name} scale-in to {pending} deferred until rollout {rollout_id} completes"
            ),
        )
        .job(job_id)
        .rollout(rollout_id)
        .meta(json!({
            "group": task_group,
            "task": task_name,
            "pending": pending,
            "source": source,
            "reason": reason,
        }))
    }

    /// `scale.reset` — an override was dropped: explicit
    /// `legion job scale --reset`, or a resubmit that changed the spec
    /// count reclaimed ownership (docs/autoscaling.md).
    pub fn scale_reset(job_id: &str, task_group: &str, task_name: &str, why: &str) -> EventDraft {
        EventDraft::info(
            "scale.reset",
            format!("Job {job_id} task {task_group}/{task_name} count override dropped: {why}"),
        )
        .job(job_id)
        .meta(json!({
            "group": task_group,
            "task": task_name,
            "why": why,
        }))
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
    /// A network policy denied a connection or request (docs/network-handlers.md).
    /// One event per folded denial row a node reported: `count` says how
    /// many identical denials it stands for.
    pub fn net_denied(node_id: &str, denial: &crate::structs::NetDenial) -> EventDraft {
        let what = if denial.method.is_empty() {
            denial.destination.clone()
        } else {
            format!(
                "{} {} on {}",
                denial.method, denial.path, denial.destination
            )
        };
        let message = if denial.reason.is_empty() {
            format!(
                "Denied {} {} for {}/{} by {} (x{})",
                denial.direction,
                what,
                denial.job_id,
                denial.task_name,
                denial.source,
                denial.count.max(1)
            )
        } else {
            format!(
                "Denied {} {} for {}/{} by {}: {} (x{})",
                denial.direction,
                what,
                denial.job_id,
                denial.task_name,
                denial.source,
                denial.reason,
                denial.count.max(1)
            )
        };
        let mut draft = EventDraft::warn("net.denied", message).node(node_id);
        if !denial.job_id.is_empty() {
            draft = draft.job(&denial.job_id);
        }
        if denial.allocation_id != 0 {
            draft = draft.allocation(denial.allocation_id);
        }
        draft.meta(json!({
            "direction": denial.direction,
            "protocol": denial.protocol,
            "destination": denial.destination,
            "source": denial.source,
            "reason": denial.reason,
            "identity": denial.identity,
            "task": denial.task_name,
            "count": denial.count,
            "labels": denial.labels,
            "method": denial.method,
            "path": denial.path,
        }))
    }

    /// `node.pressure` — a node's memory pressure response acted on one
    /// allocation (docs/resource-enforcement.md, "Memory pressure"). Warn for
    /// the eviction steps, info for `reclaim`, which stops nothing. The
    /// message is the worker's detail sentence, which names the step and the
    /// numbers.
    pub fn node_pressure(
        node_id: &str,
        action: &crate::structs::PressureAction,
        instance_id: Option<&str>,
    ) -> EventDraft {
        let message = if action.detail.is_empty() {
            format!(
                "{} acted on allocation {} on node {node_id}",
                action.step, action.allocation_id
            )
        } else {
            format!("Allocation {}: {}", action.allocation_id, action.detail)
        };
        let draft = if action.step == "reclaim" {
            EventDraft::info("node.pressure", message)
        } else {
            EventDraft::warn("node.pressure", message)
        };
        let mut draft = draft.node(node_id).allocation(action.allocation_id);
        if !action.job_id.is_empty() {
            draft = draft.job(&action.job_id);
        }
        let mut meta = json!({
            "step": action.step,
            "freed_bytes": action.freed_bytes,
            "used_bytes": action.used_bytes,
            "reserved_bytes": action.reserved_bytes,
            "available_bytes": action.available_bytes,
            "min_free_bytes": action.min_free_bytes,
            "stall_percent": action.stall_percent,
            "max_stall_percent": action.max_stall_percent,
            "detail": action.detail,
        });
        match action.step.as_str() {
            "reclaim" => {}
            // A suspended instance is not evicted: it keeps its state and
            // resumes, so the allocation's cause is `suspended`.
            "suspend" => {
                meta["cause"] = json!(StopCause::Suspended.as_str());
                meta["snapshot_id"] = json!(action.snapshot_id);
            }
            _ => meta["cause"] = json!(StopCause::NodePressure.as_str()),
        }
        if !action.hint.is_empty() {
            meta["hint"] = json!(action.hint);
        }
        // The stateful instance acted on, so a reader can tie the event to an
        // instance row after its allocation row is gone.
        if let Some(instance_id) = instance_id {
            meta["instance_id"] = json!(instance_id);
        }
        draft.meta(meta)
    }

    /// A network policy object was created or replaced.
    pub fn net_policy_applied(
        name: &str,
        enforcement: &str,
        priority: i64,
        affected: usize,
    ) -> EventDraft {
        EventDraft::info(
            "net.policy_applied",
            format!(
                "Network policy {name} applied ({enforcement}, priority {priority}); {affected} running allocation(s) affected"
            ),
        )
        .meta(json!({
            "policy": name,
            "enforcement": enforcement,
            "priority": priority,
            "affectedAllocations": affected,
        }))
    }

    /// A namespace was created or updated (docs/namespaces.md).
    pub fn namespace_applied(name: &str, created: bool) -> EventDraft {
        let (kind, verb) = if created {
            ("namespace.created", "created")
        } else {
            ("namespace.updated", "updated")
        };
        EventDraft::info(kind, format!("Namespace {name} {verb}"))
            .meta(json!({ "namespace": name }))
    }

    /// A namespace was deleted.
    pub fn namespace_deleted(name: &str) -> EventDraft {
        EventDraft::info("namespace.deleted", format!("Namespace {name} deleted"))
            .meta(json!({ "namespace": name }))
    }

    /// A network policy object was deleted.
    pub fn net_policy_deleted(name: &str) -> EventDraft {
        EventDraft::info(
            "net.policy_deleted",
            format!("Network policy {name} deleted"),
        )
        .meta(json!({ "policy": name }))
    }

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

    /// Shared scopes and metadata for the `snapshot.*` kinds.
    fn snapshot_draft(
        draft: EventDraft,
        snapshot: &crate::structs::snapshot::SnapshotRecord,
    ) -> EventDraft {
        let mut draft = draft.job(&snapshot.job_id).meta(json!({
            "snapshotId": snapshot.snapshot_id,
            "instanceId": snapshot.instance_id,
            "task": snapshot.task_name,
            "kind": snapshot.kind.as_str(),
            "purpose": snapshot.purpose.as_str(),
            "name": snapshot.name,
            "totalBytes": snapshot.total_bytes,
            "durationMs": snapshot.duration_ms,
            "error": snapshot.error,
            "state": snapshot.state.as_str(),
            "durability": snapshot.durability.as_str(),
        }));
        if let Some(node_id) = snapshot.node_id.as_deref() {
            draft = draft.node(node_id);
        }
        if let Some(allocation_id) = snapshot.allocation_id {
            draft = draft.allocation(allocation_id);
        }
        draft
    }

    /// `snapshot.created` — a node finished capturing a snapshot, which is
    /// now `local` (docs/suspend-and-resume.md).
    pub fn snapshot_created(snapshot: &crate::structs::snapshot::SnapshotRecord) -> EventDraft {
        snapshot_draft(
            EventDraft::info(
                "snapshot.created",
                format!(
                    "Snapshot {} ({}) of instance {} captured on {}: {} bytes in {} ms",
                    snapshot.snapshot_id,
                    snapshot.kind.as_str(),
                    snapshot.instance_id.as_deref().unwrap_or("-"),
                    snapshot.node_id.as_deref().unwrap_or("-"),
                    snapshot.total_bytes,
                    snapshot.duration_ms
                ),
            ),
            snapshot,
        )
    }

    /// `snapshot.failed` — a node could not capture a snapshot. The
    /// allocation it was taken from keeps running.
    pub fn snapshot_failed(snapshot: &crate::structs::snapshot::SnapshotRecord) -> EventDraft {
        snapshot_draft(
            EventDraft::warn(
                "snapshot.failed",
                format!(
                    "Snapshot {} of instance {} failed: {}",
                    snapshot.snapshot_id,
                    snapshot.instance_id.as_deref().unwrap_or("-"),
                    snapshot.error.as_deref().unwrap_or("unknown error")
                ),
            ),
            snapshot,
        )
    }

    /// `snapshot.published` — a `durability = "store"` snapshot's parts are
    /// all in the content store: it restores on any compatible node and
    /// survives its node (docs/suspend-and-resume.md).
    pub fn snapshot_published(
        snapshot: &crate::structs::snapshot::SnapshotRecord,
        uploaded_bytes: u64,
        publish_ms: u64,
    ) -> EventDraft {
        let draft = snapshot_draft(
            EventDraft::info(
                "snapshot.published",
                format!(
                    "Snapshot {} of instance {} published from {}: {} bytes uploaded in {} ms",
                    snapshot.snapshot_id,
                    snapshot.instance_id.as_deref().unwrap_or("-"),
                    snapshot.node_id.as_deref().unwrap_or("-"),
                    uploaded_bytes,
                    publish_ms
                ),
            ),
            snapshot,
        );
        let mut meta = draft.metadata.clone();
        if let Some(object) = meta.as_object_mut() {
            object.insert("uploadedBytes".into(), json!(uploaded_bytes));
            object.insert("publishMs".into(), json!(publish_ms));
        }
        draft.meta(meta)
    }

    /// `snapshot.publish_failed` — a `durability = "store"` snapshot could
    /// not be published (no `contentStore` on its node, or the store refused
    /// the upload). It stays `local` and keeps working from its node.
    pub fn snapshot_publish_failed(
        snapshot: &crate::structs::snapshot::SnapshotRecord,
    ) -> EventDraft {
        snapshot_draft(
            EventDraft::warn(
                "snapshot.publish_failed",
                format!(
                    "Snapshot {} of instance {} could not be published from {}, and stays \
                     local to that node: {}",
                    snapshot.snapshot_id,
                    snapshot.instance_id.as_deref().unwrap_or("-"),
                    snapshot.node_id.as_deref().unwrap_or("-"),
                    snapshot.error.as_deref().unwrap_or("unknown error")
                ),
            ),
            snapshot,
        )
    }

    /// `snapshot.warmed` / `snapshot.warm_failed` — a node finished, or
    /// failed, pre-fetching a published snapshot's parts (WarmSnapshot).
    pub fn snapshot_warm(
        snapshot_id: &str,
        job_id: Option<&str>,
        node_id: &str,
        ok: bool,
        fetched_bytes: u64,
        duration_ms: u64,
        error: &str,
    ) -> EventDraft {
        let draft = if ok {
            EventDraft::info(
                "snapshot.warmed",
                format!(
                    "Node {node_id} holds snapshot {snapshot_id}'s parts: {fetched_bytes} bytes \
                     fetched in {duration_ms} ms"
                ),
            )
        } else {
            EventDraft::warn(
                "snapshot.warm_failed",
                format!("Node {node_id} could not warm snapshot {snapshot_id}: {error}"),
            )
        };
        let mut draft = draft.node(node_id).meta(json!({
            "snapshotId": snapshot_id,
            "fetchedBytes": fetched_bytes,
            "durationMs": duration_ms,
            "error": if ok { None } else { Some(error) },
        }));
        if let Some(job_id) = job_id {
            draft = draft.job(job_id);
        }
        draft
    }

    /// `snapshot.deleted` — a snapshot's row is gone: its node reclaimed the
    /// bytes, or there was nothing left to reclaim. Covers DeleteSnapshot,
    /// `snapshots.ttl` and `snapshots.keep` expiry, and job destroy alike.
    pub fn snapshot_deleted(snapshot: &crate::structs::snapshot::SnapshotRecord) -> EventDraft {
        snapshot_draft(
            EventDraft::info(
                "snapshot.deleted",
                format!(
                    "Snapshot {} of instance {} deleted",
                    snapshot.snapshot_id,
                    snapshot.instance_id.as_deref().unwrap_or("-")
                ),
            ),
            snapshot,
        )
    }

    /// `snapshot.restored` / `snapshot.restore_failed` — an allocation
    /// started from a snapshot (RestoreInstance), or failed to. A failed
    /// restore fails the allocation, and the error names the cause (for
    /// example `restore needs TAP device tap7: ...`).
    pub fn snapshot_restore(
        snapshot_id: &str,
        job_id: &str,
        instance_id: Option<&str>,
        node_id: &str,
        allocation_id: i64,
        ok: bool,
        error: &str,
    ) -> EventDraft {
        let draft = if ok {
            EventDraft::info(
                "snapshot.restored",
                format!(
                    "Instance {} restored from snapshot {snapshot_id} as allocation {allocation_id}",
                    instance_id.unwrap_or("-")
                ),
            )
        } else {
            EventDraft::warn(
                "snapshot.restore_failed",
                format!(
                    "Instance {} failed to restore from snapshot {snapshot_id}: {error}",
                    instance_id.unwrap_or("-")
                ),
            )
        };
        draft
            .job(job_id)
            .node(node_id)
            .allocation(allocation_id)
            .meta(json!({
                "snapshotId": snapshot_id,
                "instanceId": instance_id,
                "error": if ok { Value::Null } else { json!(error) },
            }))
    }

    /// `instance.suspended` — a suspend's capture landed and the instance is
    /// `suspended`, holding no allocation (or, with `suspend.reserve`, only
    /// its reservation). docs/suspend-and-resume.md.
    pub fn instance_suspended(
        snapshot: &crate::structs::snapshot::SnapshotRecord,
        instance_id: &str,
        reserved: bool,
        reason: &str,
    ) -> EventDraft {
        let why = match reason {
            crate::structs::suspend::SUSPEND_REASON_IDLE => " (idle)",
            crate::structs::suspend::SUSPEND_REASON_PRESSURE => " (node memory pressure)",
            crate::structs::suspend::SUSPEND_REASON_MAKE_ROOM => " (making room for a resume)",
            _ => "",
        };
        let mut draft = snapshot_draft(
            EventDraft::info(
                "instance.suspended",
                format!(
                    "Instance {instance_id} suspended{why} to {} checkpoint {} on {}: {} bytes in \
                     {} ms{}",
                    snapshot.kind.as_str(),
                    snapshot.snapshot_id,
                    snapshot.node_id.as_deref().unwrap_or("-"),
                    snapshot.total_bytes,
                    snapshot.duration_ms,
                    if reserved { ", reservation held" } else { "" }
                ),
            ),
            snapshot,
        );
        if let Some(meta) = draft.metadata.as_object_mut() {
            meta.insert("reserved".into(), json!(reserved));
            meta.insert("reason".into(), json!(reason));
        }
        draft
    }

    /// `instance.make_room` — a resume waiting on its checkpoint's node did
    /// not fit, so the manager started suspends of idle suspendable
    /// instances there (manager `suspend.makeRoom`). Each suspend then lands
    /// as `instance.suspended` with reason `make_room`.
    pub fn instance_make_room(
        job_id: &str,
        instance_id: &str,
        node_id: &str,
        suspending: &[String],
    ) -> EventDraft {
        EventDraft::info(
            "instance.make_room",
            format!(
                "Resume of instance {instance_id} does not fit on {node_id}; suspending {} to \
                 make room: {}",
                suspending.len(),
                suspending.join(", ")
            ),
        )
        .job(job_id)
        .node(node_id)
        .meta(json!({
            "instanceId": instance_id,
            "suspending": suspending,
        }))
    }

    /// `instance.suspend_failed` — the node could not capture the instance.
    /// The allocation keeps running: a suspend never stops a workload it
    /// failed to capture.
    pub fn instance_suspend_failed(
        snapshot: &crate::structs::snapshot::SnapshotRecord,
    ) -> EventDraft {
        snapshot_draft(
            EventDraft::warn(
                "instance.suspend_failed",
                format!(
                    "Instance {} could not be suspended and keeps running: {}",
                    snapshot.instance_id.as_deref().unwrap_or("-"),
                    snapshot.error.as_deref().unwrap_or("unknown error")
                ),
            ),
            snapshot,
        )
    }

    /// `instance.archived` — a suspended instance's checkpoint was
    /// published and its snapshot row moved to an archive manifest in the
    /// `contentStore` (`suspend.archive`).
    pub fn instance_archived(archived: &crate::storage::store::ArchivedInstance) -> EventDraft {
        EventDraft::info(
            "instance.archived",
            format!(
                "Instance {} archived its checkpoint {} to {}{}",
                archived.instance_id,
                archived.snapshot_id,
                archived.manifest_url,
                if archived.row_deleted {
                    " and keeps no row (manifestOnly)"
                } else {
                    ""
                }
            ),
        )
        .job(&archived.job_id)
        .meta(json!({
            "instanceId": archived.instance_id,
            "snapshotId": archived.snapshot_id,
            "manifestUrl": archived.manifest_url,
            "manifestOnly": archived.row_deleted,
        }))
    }

    /// `instance.promoted` — a running session became a keyed instance
    /// (PromoteInstance, docs/session-mode.md). The node adopts the session
    /// as `allocation_id` in place.
    pub fn instance_promoted(
        job_id: &str,
        instance_id: &str,
        session_id: &str,
        node_id: &str,
        allocation_id: i64,
    ) -> EventDraft {
        EventDraft::info(
            "instance.promoted",
            format!(
                "Session {session_id} promoted to instance {instance_id} as allocation \
                 {allocation_id} on {node_id}"
            ),
        )
        .job(job_id)
        .node(node_id)
        .allocation(allocation_id)
        .meta(json!({
            "instanceId": instance_id,
            "promotedFrom": session_id,
        }))
    }

    /// `instance.resumed` — a suspended instance restored from its
    /// checkpoint as a new allocation.
    pub fn instance_resumed(
        snapshot_id: &str,
        job_id: &str,
        instance_id: &str,
        node_id: &str,
        allocation_id: i64,
        restore_ms: u64,
    ) -> EventDraft {
        EventDraft::info(
            "instance.resumed",
            format!(
                "Instance {instance_id} resumed from {snapshot_id} as allocation {allocation_id} \
                 on {node_id} ({restore_ms} ms)"
            ),
        )
        .job(job_id)
        .node(node_id)
        .allocation(allocation_id)
        .meta(json!({
            "instanceId": instance_id,
            "snapshotId": snapshot_id,
            "restoreMs": restore_ms,
        }))
    }

    /// `instance.restore_failed` — a resume could not restore its
    /// checkpoint. `outcome` is `cold` (booted fresh on the retained
    /// storage) or `suspended` (`onRestoreFailure = "fail"`).
    pub fn instance_restore_failed(
        snapshot_id: &str,
        job_id: &str,
        instance_id: &str,
        node_id: Option<&str>,
        allocation_id: Option<i64>,
        reason: &str,
        cold: bool,
    ) -> EventDraft {
        let mut draft = EventDraft::warn(
            "instance.restore_failed",
            format!(
                "Instance {instance_id} could not restore checkpoint {snapshot_id} and {}: {reason}",
                if cold {
                    "booted cold on its retained storage"
                } else {
                    "stays suspended"
                }
            ),
        )
        .job(job_id)
        .meta(json!({
            "instanceId": instance_id,
            "snapshotId": snapshot_id,
            "error": reason,
            "outcome": if cold { "cold" } else { "suspended" },
        }));
        if let Some(node_id) = node_id {
            draft = draft.node(node_id);
        }
        if let Some(allocation_id) = allocation_id {
            draft = draft.allocation(allocation_id);
        }
        draft
    }

    /// `instance.forked` (Phase 6) — a fork's checkpoint landed and its
    /// forks are being placed. The origin kept running.
    pub fn instance_forked(
        snapshot: &crate::structs::snapshot::SnapshotRecord,
        origin: &str,
        instances: &[String],
    ) -> EventDraft {
        let mut draft = snapshot_draft(
            EventDraft::info(
                "instance.forked",
                format!(
                    "Instance {origin} forked into {} ({} checkpoint {} on {}, paused {} ms)",
                    instances.join(", "),
                    snapshot.kind.as_str(),
                    snapshot.snapshot_id,
                    snapshot.node_id.as_deref().unwrap_or("-"),
                    snapshot.pause_ms
                ),
            ),
            snapshot,
        );
        if let Some(meta) = draft.metadata.as_object_mut() {
            meta.insert("origin".into(), json!(origin));
            meta.insert("instances".into(), json!(instances));
            meta.insert("pauseMs".into(), json!(snapshot.pause_ms));
        }
        draft
    }

    /// `instance.fork_failed` (Phase 6) — the node could not capture the
    /// origin. The origin keeps running; the forks that waited on the
    /// checkpoint are terminated.
    pub fn instance_fork_failed(
        snapshot: &crate::structs::snapshot::SnapshotRecord,
        instances: &[String],
    ) -> EventDraft {
        let mut draft = snapshot_draft(
            EventDraft::warn(
                "instance.fork_failed",
                format!(
                    "Instance {} could not be forked and keeps running; forks {} were \
                     terminated: {}",
                    snapshot.instance_id.as_deref().unwrap_or("-"),
                    instances.join(", "),
                    snapshot.error.as_deref().unwrap_or("unknown error")
                ),
            ),
            snapshot,
        );
        if let Some(meta) = draft.metadata.as_object_mut() {
            meta.insert("instances".into(), json!(instances));
        }
        draft
    }

    /// `instance.restore_failed` for a NEW instance (Phase 6: a fork or a
    /// `from_snapshot` create) that could not restore its snapshot. It boots
    /// cold on its own storage, or is stopped under `onRestoreFailure =
    /// "fail"`.
    pub fn new_instance_restore_failed(
        snapshot_id: &str,
        job_id: &str,
        instance_id: &str,
        node_id: &str,
        allocation_id: i64,
        reason: &str,
        cold: bool,
    ) -> EventDraft {
        EventDraft::warn(
            "instance.restore_failed",
            format!(
                "Instance {instance_id} could not restore snapshot {snapshot_id} and {}: {reason}",
                if cold {
                    "booted cold on its own storage"
                } else {
                    "was stopped"
                }
            ),
        )
        .job(job_id)
        .node(node_id)
        .allocation(allocation_id)
        .meta(json!({
            "instanceId": instance_id,
            "snapshotId": snapshot_id,
            "error": reason,
            "outcome": if cold { "cold" } else { "stopped" },
        }))
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

    /// `alloc.provisional_started` — see [`drafts::provisional_started`].
    #[allow(clippy::too_many_arguments)]
    pub fn provisional_started(
        self,
        claim_id: &str,
        node_id: &str,
        job_id: &str,
        job_config_id: &str,
        task_name: &str,
        instance_id: &str,
        gate: &str,
        origin: &str,
        ticket_jti: &str,
    ) {
        self.emit(drafts::provisional_started(
            claim_id,
            node_id,
            job_id,
            job_config_id,
            task_name,
            instance_id,
            gate,
            origin,
            ticket_jti,
        ));
    }

    /// `alloc.ratified` — see [`drafts::provisional_ratified`].
    #[allow(clippy::too_many_arguments)]
    pub fn provisional_ratified(
        self,
        claim_id: &str,
        allocation_id: i64,
        node_id: &str,
        job_id: &str,
        instance_id: &str,
        gate: &str,
        latency_ms: u64,
    ) {
        self.emit(drafts::provisional_ratified(
            claim_id,
            allocation_id,
            node_id,
            job_id,
            instance_id,
            gate,
            latency_ms,
        ));
    }

    /// `alloc.refused` — see [`drafts::provisional_refused`].
    pub fn provisional_refused(
        self,
        claim_id: &str,
        node_id: &str,
        job_id: &str,
        instance_id: &str,
        reason: &str,
        detail: &str,
    ) {
        self.emit(drafts::provisional_refused(
            claim_id,
            node_id,
            job_id,
            instance_id,
            reason,
            detail,
        ));
    }

    /// `boot_ticket.issued` — see [`drafts::boot_ticket_issued`].
    #[allow(clippy::too_many_arguments)]
    pub fn boot_ticket_issued(
        self,
        jti: &str,
        job: &str,
        task: &str,
        job_config_id: &str,
        instance_id: &str,
        instance_prefix: &str,
        pool: &str,
        gate: &str,
        accessor_id: &str,
        expires_at_unix: i64,
    ) {
        self.emit(drafts::boot_ticket_issued(
            jti,
            job,
            task,
            job_config_id,
            instance_id,
            instance_prefix,
            pool,
            gate,
            accessor_id,
            expires_at_unix,
        ));
    }

    pub fn scheduler_started(self, eval_threads: usize) {
        self.emit(drafts::scheduler_started(eval_threads));
    }

    pub fn scheduler_recovered(self, requeued_evals: u64) {
        self.emit(drafts::scheduler_recovered(requeued_evals));
    }

    pub fn scheduler_concurrency_adjusted(
        self,
        direction: &str,
        from: usize,
        to: usize,
        ceiling: usize,
        conflict_rate: f64,
        mode: &str,
    ) {
        self.emit(drafts::scheduler_concurrency_adjusted(
            direction,
            from,
            to,
            ceiling,
            conflict_rate,
            mode,
        ));
    }

    pub fn scheduler_concurrency_saturated(
        self,
        restored_to: usize,
        goodput_before: f64,
        goodput_after: f64,
    ) {
        self.emit(drafts::scheduler_concurrency_saturated(
            restored_to,
            goodput_before,
            goodput_after,
        ));
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

    pub fn allocation_reschedule_deferred(self, hold: &crate::storage::store::RescheduleHold) {
        self.emit(drafts::allocation_reschedule_deferred(hold));
    }

    pub fn allocation_reschedule_exhausted(
        self,
        allocation: &Allocation,
        task_name: &str,
        moves: u32,
        move_count: u32,
        move_period_secs: u64,
        reason: &str,
    ) {
        self.emit(drafts::allocation_reschedule_exhausted(
            allocation,
            task_name,
            moves,
            move_count,
            move_period_secs,
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

    pub fn job_kv_render_refused(self, job_id: &str, keys: &[String]) {
        self.emit(drafts::job_kv_render_refused(job_id, keys));
    }

    pub fn job_gang_member_lost(
        self,
        lost: &Allocation,
        cause: StopCause,
        loss: &crate::storage::store::GangLoss,
    ) {
        self.emit(drafts::job_gang_member_lost(lost, cause, loss));
    }

    pub fn job_gang_timeout(
        self,
        job_id: &str,
        task: &str,
        timeout_secs: u64,
        waited_secs: u64,
        stopped: usize,
        message: &str,
    ) {
        self.emit(drafts::job_gang_timeout(
            job_id,
            task,
            timeout_secs,
            waited_secs,
            stopped,
            message,
        ));
    }

    pub fn job_destroyed(self, job_id: &str) {
        self.emit(drafts::job_destroyed(job_id));
    }

    #[allow(clippy::too_many_arguments)]
    pub fn scale_applied(
        self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
        previous: Option<u32>,
        count: u32,
        source: &str,
        reason: &str,
    ) {
        self.emit(drafts::scale_applied(
            job_id, task_group, task_name, previous, count, source, reason,
        ));
    }

    #[allow(clippy::too_many_arguments)]
    pub fn scale_deferred(
        self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
        pending: u32,
        rollout_id: i64,
        source: &str,
        reason: &str,
    ) {
        self.emit(drafts::scale_deferred(
            job_id, task_group, task_name, pending, rollout_id, source, reason,
        ));
    }

    pub fn scale_reset(self, job_id: &str, task_group: &str, task_name: &str, why: &str) {
        self.emit(drafts::scale_reset(job_id, task_group, task_name, why));
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

    pub fn net_denied(self, node_id: &str, denial: &crate::structs::NetDenial) {
        self.emit(drafts::net_denied(node_id, denial));
    }

    pub fn node_pressure(
        self,
        node_id: &str,
        action: &crate::structs::PressureAction,
        instance_id: Option<&str>,
    ) {
        self.emit(drafts::node_pressure(node_id, action, instance_id));
    }

    pub fn net_policy_applied(self, name: &str, enforcement: &str, priority: i64, affected: usize) {
        self.emit(drafts::net_policy_applied(
            name,
            enforcement,
            priority,
            affected,
        ));
    }

    pub fn net_policy_deleted(self, name: &str) {
        self.emit(drafts::net_policy_deleted(name));
    }

    pub fn namespace_applied(self, name: &str, created: bool) {
        self.emit(drafts::namespace_applied(name, created));
    }

    pub fn namespace_deleted(self, name: &str) {
        self.emit(drafts::namespace_deleted(name));
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

    pub fn snapshot_created(self, snapshot: &crate::structs::snapshot::SnapshotRecord) {
        self.emit(drafts::snapshot_created(snapshot));
    }

    pub fn snapshot_failed(self, snapshot: &crate::structs::snapshot::SnapshotRecord) {
        self.emit(drafts::snapshot_failed(snapshot));
    }

    pub fn snapshot_deleted(self, snapshot: &crate::structs::snapshot::SnapshotRecord) {
        self.emit(drafts::snapshot_deleted(snapshot));
    }

    pub fn snapshot_published(
        self,
        snapshot: &crate::structs::snapshot::SnapshotRecord,
        uploaded_bytes: u64,
        publish_ms: u64,
    ) {
        self.emit(drafts::snapshot_published(
            snapshot,
            uploaded_bytes,
            publish_ms,
        ));
    }

    pub fn snapshot_publish_failed(self, snapshot: &crate::structs::snapshot::SnapshotRecord) {
        self.emit(drafts::snapshot_publish_failed(snapshot));
    }

    #[allow(clippy::too_many_arguments)]
    pub fn snapshot_warm(
        self,
        snapshot_id: &str,
        job_id: Option<&str>,
        node_id: &str,
        ok: bool,
        fetched_bytes: u64,
        duration_ms: u64,
        error: &str,
    ) {
        self.emit(drafts::snapshot_warm(
            snapshot_id,
            job_id,
            node_id,
            ok,
            fetched_bytes,
            duration_ms,
            error,
        ));
    }

    pub fn instance_suspended(
        self,
        snapshot: &crate::structs::snapshot::SnapshotRecord,
        instance_id: &str,
        reserved: bool,
        reason: &str,
    ) {
        self.emit(drafts::instance_suspended(
            snapshot,
            instance_id,
            reserved,
            reason,
        ));
    }

    pub fn instance_make_room(
        self,
        job_id: &str,
        instance_id: &str,
        node_id: &str,
        suspending: &[String],
    ) {
        self.emit(drafts::instance_make_room(
            job_id,
            instance_id,
            node_id,
            suspending,
        ));
    }

    pub fn instance_suspend_failed(self, snapshot: &crate::structs::snapshot::SnapshotRecord) {
        self.emit(drafts::instance_suspend_failed(snapshot));
    }

    pub fn instance_archived(self, archived: &crate::storage::store::ArchivedInstance) {
        self.emit(drafts::instance_archived(archived));
    }

    pub fn instance_promoted(
        self,
        job_id: &str,
        instance_id: &str,
        session_id: &str,
        node_id: &str,
        allocation_id: i64,
    ) {
        self.emit(drafts::instance_promoted(
            job_id,
            instance_id,
            session_id,
            node_id,
            allocation_id,
        ));
    }

    pub fn instance_resumed(
        self,
        snapshot_id: &str,
        job_id: &str,
        instance_id: &str,
        node_id: &str,
        allocation_id: i64,
        restore_ms: u64,
    ) {
        self.emit(drafts::instance_resumed(
            snapshot_id,
            job_id,
            instance_id,
            node_id,
            allocation_id,
            restore_ms,
        ));
    }

    #[allow(clippy::too_many_arguments)]
    pub fn instance_restore_failed(
        self,
        snapshot_id: &str,
        job_id: &str,
        instance_id: &str,
        node_id: Option<&str>,
        allocation_id: Option<i64>,
        reason: &str,
        cold: bool,
    ) {
        self.emit(drafts::instance_restore_failed(
            snapshot_id,
            job_id,
            instance_id,
            node_id,
            allocation_id,
            reason,
            cold,
        ));
    }

    pub fn instance_forked(
        self,
        snapshot: &crate::structs::snapshot::SnapshotRecord,
        origin: &str,
        instances: &[String],
    ) {
        self.emit(drafts::instance_forked(snapshot, origin, instances));
    }

    pub fn instance_fork_failed(
        self,
        snapshot: &crate::structs::snapshot::SnapshotRecord,
        instances: &[String],
    ) {
        self.emit(drafts::instance_fork_failed(snapshot, instances));
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_instance_restore_failed(
        self,
        snapshot_id: &str,
        job_id: &str,
        instance_id: &str,
        node_id: &str,
        allocation_id: i64,
        reason: &str,
        cold: bool,
    ) {
        self.emit(drafts::new_instance_restore_failed(
            snapshot_id,
            job_id,
            instance_id,
            node_id,
            allocation_id,
            reason,
            cold,
        ));
    }

    #[allow(clippy::too_many_arguments)]
    pub fn snapshot_restore(
        self,
        snapshot_id: &str,
        job_id: &str,
        instance_id: Option<&str>,
        node_id: &str,
        allocation_id: i64,
        ok: bool,
        error: &str,
    ) {
        self.emit(drafts::snapshot_restore(
            snapshot_id,
            job_id,
            instance_id,
            node_id,
            allocation_id,
            ok,
            error,
        ));
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
    /// Stateful instance filter: events whose metadata names the instance,
    /// or whose allocation fulfilled it (see [`event_names_instance`]).
    pub instance_id: Option<String>,
    /// Exact allocation ID filter.
    pub allocation_id: Option<i64>,
}

/// Metadata keys that name the instance an event is about. `instance_id` is
/// the `node.pressure` spelling (that event's metadata is snake_case
/// throughout); everything else uses `instanceId`.
const INSTANCE_ID_META_KEYS: [&str; 2] = ["instanceId", "instance_id"];
/// Metadata arrays whose members are instances the event is also about: the
/// forks of `instance.forked` / `instance.fork_failed`, and the instances an
/// `instance.make_room` suspended.
const INSTANCE_LIST_META_KEYS: [&str; 2] = ["instances", "suspending"];

/// Whether an event's metadata names `instance_id`, as its subject or as one
/// of the instances it lists. The SQL in `Store::list_scheduler_events`
/// mirrors this.
pub fn event_names_instance(metadata_json: &str, instance_id: &str) -> bool {
    let Ok(Value::Object(meta)) = serde_json::from_str::<Value>(metadata_json) else {
        return false;
    };
    INSTANCE_ID_META_KEYS
        .iter()
        .any(|key| meta.get(*key).and_then(Value::as_str) == Some(instance_id))
        || INSTANCE_LIST_META_KEYS.iter().any(|key| {
            meta.get(*key)
                .and_then(Value::as_array)
                .is_some_and(|items| items.iter().any(|item| item.as_str() == Some(instance_id)))
        })
}

/// Metadata arrays of `{ "allocationId": n, ... }` entries naming the
/// allocations a batched event covers: `placements` on `allocation.placed`,
/// `allocations` on `allocation.stopping` and `allocation.preempted`,
/// `victims` on `allocation.preempt_planned`. Those events leave the
/// `allocation_id` column unset (they cover several). `Store::
/// list_scheduler_events` matches the same keys.
pub const ALLOCATION_LIST_META_KEYS: [&str; 3] = ["placements", "allocations", "victims"];

/// Every allocation an event is about: its `allocation_id` column plus the
/// ids a batched event lists in its metadata. The SQL in
/// `Store::list_scheduler_events` mirrors this.
pub fn event_allocation_ids(event: &SchedulerEventRecord) -> Vec<i64> {
    let mut ids: Vec<i64> = event.allocation_id.into_iter().collect();
    if let Ok(Value::Object(meta)) = serde_json::from_str::<Value>(&event.metadata_json) {
        for key in ALLOCATION_LIST_META_KEYS {
            let entries = meta.get(key).and_then(Value::as_array);
            ids.extend(
                entries
                    .into_iter()
                    .flatten()
                    .filter_map(|entry| entry.get("allocationId").and_then(Value::as_i64)),
            );
        }
    }
    ids
}

impl SchedulerEventFilter {
    pub fn effective_limit(&self) -> u32 {
        self.limit.unwrap_or(100).clamp(1, 1_000)
    }

    /// Whether a live event passes this filter. Mirrors the SQL applied by
    /// `Store::list_scheduler_events` so streamed and listed results agree.
    ///
    /// The instance filter's allocation half needs the store, so this form
    /// matches an instance through event metadata only; the event stream
    /// uses [`SchedulerEventFilter::matches_with`].
    pub fn matches(&self, event: &SchedulerEventRecord) -> bool {
        self.matches_with(event, |_| None)
    }

    /// [`SchedulerEventFilter::matches`], resolving the instance an
    /// allocation fulfilled through `allocation_instance` when the event's
    /// metadata does not name the filtered instance itself.
    pub fn matches_with(
        &self,
        event: &SchedulerEventRecord,
        allocation_instance: impl Fn(i64) -> Option<String>,
    ) -> bool {
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
        if let Some(allocation_id) = self.allocation_id {
            if !event_allocation_ids(event).contains(&allocation_id) {
                return false;
            }
        }
        if let Some(instance_id) = self.instance_id.as_deref() {
            let named = event_names_instance(&event.metadata_json, instance_id);
            let fulfilled = || {
                event_allocation_ids(event)
                    .into_iter()
                    .any(|id| allocation_instance(id).is_some_and(|owner| owner == instance_id))
            };
            if !named && !fulfilled() {
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
