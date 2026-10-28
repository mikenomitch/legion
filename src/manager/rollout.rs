// Several rollout planning helpers thread a deliberately wide set of state
// through by value; splitting them into structs would obscure more than it helps.
#![allow(clippy::too_many_arguments)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::constants::allocation_status_is_terminal;
use crate::manager::dispatcher::dispatch_allocations;
use crate::manager::rollout_strategy;
use crate::manager::scheduler::{PlacementBlocked, PlanMode, Scheduler};
use crate::prelude::{bail, debug, instrument, warn, Context, R};
use crate::storage::store::{AllocationHealthSnapshot, Store};
use crate::structs::{
    Allocation, Eval, JobConfig, Rollout, RolloutCapacityTimeoutAction, RolloutConfig,
    RolloutDeadlineAction, RolloutProgression, RolloutReplacement, StopCause, TaskConfig,
    TaskLifecycle,
};

const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(2);
/// Upper bound on a capacity-blocked group's re-tick interval (see
/// `placement_wait_delay`).
const MAX_PLACEMENT_WAIT_DELAY: Duration = Duration::from_secs(30);
/// How often a group whose replacements wait on a `dependsOn` re-checks. The
/// re-check is a backstop: the dependency reaching a terminal state queues a
/// tick of its own (`Store::queue_dependency_progress`).
const DEPENDENCY_WAIT_DELAY: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct RolloutEnvelope {
    #[serde(default)]
    rollout: Option<RolloutConfig>,
    #[serde(default)]
    rollout_by_group: BTreeMap<String, RolloutConfig>,
    #[serde(default)]
    runtime: RolloutRuntime,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct RolloutRuntime {
    #[serde(default)]
    groups: BTreeMap<String, GroupRuntime>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GroupRuntime {
    #[serde(default)]
    step_index: usize,
    #[serde(default)]
    pending_manual_advances: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    active_step_started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reached_target_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    blocked_since: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_step_completed_at: Option<String>,
}

#[derive(Debug, Clone)]
struct GroupPlan {
    name: String,
    config: RolloutConfig,
    progression: RolloutProgression,
    replacement: RolloutReplacement,
    step_fractions: Vec<f64>,
    tasks: Vec<GroupTaskPlan>,
    /// Present when the effective rollout config asks for ordered slot
    /// replacement (`slots.order: 'sequential'`) and the group has a
    /// slot-mode stateful task. One slot is replaced per step, highest
    /// ordinal first, stopping above `update_from` (the staged floor).
    sequential_slots: Option<SlotSequentialPlan>,
}

/// Ordered-slot parameters resolved for one group (see `GroupPlan`).
#[derive(Debug, Clone)]
struct SlotSequentialPlan {
    /// Name of the group's slot-mode stateful task (at most one per group).
    task_name: String,
    /// The slot pool size (`count.num`) in the destination config.
    count: u32,
    /// Slots `0..update_from` are never touched (`slots.updateFrom`,
    /// default 0 = update everything).
    update_from: u32,
}

impl SlotSequentialPlan {
    /// The slot being replaced at a given step: highest ordinal first,
    /// `count-1` at step 0 down to `update_from` at the last step.
    fn slot_for_step(&self, step_index: usize) -> u32 {
        self.count
            .saturating_sub(1)
            .saturating_sub(step_index.min(u32::MAX as usize) as u32)
            .max(self.update_from)
    }
}

#[derive(Debug, Clone)]
struct GroupTaskPlan {
    name: String,
    lifecycle: TaskLifecycle,
    old_task_id: Option<i64>,
    new_task_id: Option<i64>,
    desired_new: u32,
    current_new: u32,
    current_old_allocs: Vec<Allocation>,
}

#[derive(Debug)]
enum GroupAction {
    Idle,
    Mutated {
        delay: Option<Duration>,
        message: String,
    },
    Wait {
        delay: Duration,
        message: String,
    },
    Terminal {
        status: String,
        incomplete: bool,
        message: String,
    },
}

#[derive(Debug, Clone)]
enum HealthState {
    Healthy,
    Waiting,
    Deadline(RolloutDeadlineAction),
}

#[derive(Debug)]
struct TickOutcome {
    status: String,
    incomplete: bool,
    message: String,
    next_delay: Option<Duration>,
    event_group: Option<String>,
}

#[derive(Debug, Default)]
struct RetireOutcome {
    removed: usize,
    blocked: bool,
    /// Slot ordinals retired this tick (sequential slot groups only), used
    /// to make rollout events name the slot being replaced.
    removed_slots: Vec<u32>,
}

#[derive(Debug, Default)]
struct TickInputs {
    new_allocations: Vec<Allocation>,
    allocations_by_task: HashMap<i64, Vec<i64>>,
    new_counts_by_task: HashMap<i64, u32>,
    new_task_ids: Vec<i64>,
    allocation_health: HashMap<i64, AllocationHealthSnapshot>,
}

#[derive(Debug, Default)]
struct StepTargets {
    per_task_target_new: HashMap<String, u32>,
    per_task_target_old: HashMap<String, u32>,
    needs_new_placement: bool,
    needs_old_retire: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StepOperation {
    PlaceNew,
    WaitForHealthy,
    RetireOld,
}

#[instrument(
    skip_all,
    fields(
        eval_id = %eval.id,
        job_id = %eval.job_id,
        job_config_id = %eval.job_config_id
    )
)]
pub fn handle_rollout_tick(eval: &Eval, store: &Store) -> R<String> {
    if eval.job_id.trim().is_empty() || eval.job_config_id.trim().is_empty() {
        bail!("Eval rollout_tick missing identifiers");
    }

    let rollout = store.latest_rollout_for_job_config(&eval.job_config_id)?;
    let Some(rollout) = rollout else {
        bail!("No rollout found for job config {}", eval.job_config_id);
    };

    if is_terminal_rollout_status(&rollout.status) {
        return Ok(format!(
            "Rollout {} already in terminal state {}",
            rollout.id, rollout.status
        ));
    }

    // A rollout drives the job toward its target, and treats every
    // allocation on any other config as old. Once a newer config lands
    // without superseding this rollout (a KV-triggered restart or in-place
    // update re-renders the job without going through RunJob), ticking on
    // would retire the newer allocations and then place nothing, because the
    // scheduler skips a superseded config. So a rollout whose target is no
    // longer the job's latest config ends here, and the latest config is
    // rescheduled to re-place anything already retired.
    let latest_config_id = store
        .get_job_config_optional(&rollout.job_id)?
        .map(|config| config.id());
    if latest_config_id.as_deref() != Some(rollout.to_job_config_id.as_str()) {
        return supersede_rollout(store, &rollout, latest_config_id.as_deref());
    }

    let to_config = store
        .get_job_config_by_id(&rollout.to_job_config_id)
        .with_context(|| {
            format!(
                "Missing destination job config {} for rollout {}",
                rollout.to_job_config_id, rollout.id
            )
        })?;
    let from_config = match rollout.from_job_config_id.as_ref() {
        Some(from_id) => Some(store.get_job_config_by_id(from_id).with_context(|| {
            format!(
                "Missing source job config {} for rollout {}",
                from_id, rollout.id
            )
        })?),
        None => None,
    };

    let mut envelope = parse_rollout_envelope(&rollout.config_json)?;
    let now = OffsetDateTime::now_utc();

    let plans = build_group_plans(&to_config, from_config.as_ref(), store, &rollout)?;
    ensure_runtime_groups(&mut envelope.runtime, &plans);
    let tick_inputs = build_tick_inputs(store, &rollout, &plans)?;

    let scheduler = Scheduler::new(store)?;
    let outcome = execute_tick(
        store,
        &scheduler,
        &rollout,
        &to_config,
        from_config.as_ref(),
        &plans,
        &mut envelope.runtime,
        &tick_inputs,
        now,
    )?;

    if rollout.status != outcome.status
        || rollout.incomplete != outcome.incomplete
        || rollout.message != outcome.message
    {
        let level = if outcome.status == "failed" {
            "error"
        } else if matches!(
            outcome.status.as_str(),
            "waiting_manual_start" | "waiting_manual_advance" | "paused"
        ) {
            "warn"
        } else {
            "info"
        };
        let metadata = json!({
            "status": outcome.status.as_str(),
            "incomplete": outcome.incomplete,
            "nextDelayMs": outcome.next_delay.map(|delay| delay.as_millis()),
        })
        .to_string();
        let _ = store.append_rollout_event(
            rollout.id,
            &rollout.job_id,
            outcome.event_group.as_deref(),
            level,
            &outcome.message,
            Some(&metadata),
        );
    }

    let config_json = serde_json::to_string(&envelope)?;
    store.update_rollout_config_json(rollout.id, &config_json)?;
    store.update_rollout_status(
        rollout.id,
        &outcome.status,
        outcome.incomplete,
        Some(&outcome.message),
    )?;

    // A rollout reaching a terminal status releases any deferred scale-ins
    // (docs/autoscaling.md): pending counts promote to applied and a
    // `job_scale` eval reconciles the allocation set. A NotRaftLeader here
    // propagates like the rest of this handler — the released eval retries
    // the whole tick.
    if is_terminal_rollout_status(&outcome.status) {
        store.apply_pending_scales_after_rollout(&rollout.job_id, &rollout.to_job_config_id)?;
    }

    if let Some(delay) = outcome.next_delay {
        if !is_terminal_rollout_status(&outcome.status)
            && !matches!(
                outcome.status.as_str(),
                "waiting_manual_start" | "waiting_manual_advance" | "paused"
            )
        {
            store.enqueue_eval_after_delay(
                "rollout_tick",
                Some(&rollout.job_id),
                Some(&rollout.to_job_config_id),
                None,
                Some(store.default_eval_priority),
                delay,
            )?;
        }
    }

    Ok(format!(
        "Rollout {} {}: {}",
        rollout.id, outcome.status, outcome.message
    ))
}

/// Ends a rollout whose target is no longer the job's latest config, the
/// way RunJob ends one it supersedes, and releases what it was holding.
fn supersede_rollout(store: &Store, rollout: &Rollout, latest: Option<&str>) -> R<String> {
    let message = match latest {
        Some(latest) => format!(
            "Rollout superseded: job config {latest} replaced its target {}",
            rollout.to_job_config_id
        ),
        None => "Rollout superseded: the job has no config".to_string(),
    };
    warn!(
        rollout_id = rollout.id,
        job_id = %rollout.job_id,
        "{message}"
    );
    store.update_rollout_status(rollout.id, "cancelled", false, Some(&message))?;
    let _ = store.append_rollout_event(
        rollout.id,
        &rollout.job_id,
        None,
        "warn",
        &message,
        Some(&json!({ "status": "cancelled", "reason": "superseded" }).to_string()),
    );
    // Deferred scale-ins waited on this rollout; they apply now.
    store.apply_pending_scales_after_rollout(&rollout.job_id, &rollout.to_job_config_id)?;
    if let Some(latest) = latest {
        store.enqueue_eval(
            "job_reschedule",
            Some(&rollout.job_id),
            Some(latest),
            None,
            Some(store.default_eval_priority),
        )?;
    }
    Ok(format!("Rollout {} cancelled: {message}", rollout.id))
}

#[instrument(skip_all, fields(rollout_id, priority, groups = ?groups))]
pub fn advance_rollout(
    store: &Store,
    rollout_id: i64,
    groups: &[String],
    priority: i64,
) -> R<Rollout> {
    let rollout = store
        .get_rollout(rollout_id)?
        .with_context(|| format!("Rollout {} not found", rollout_id))?;

    if is_terminal_rollout_status(&rollout.status) {
        bail!(
            "Rollout {} is already in terminal state {}",
            rollout.id,
            rollout.status
        );
    }

    let to_config = store.get_job_config_by_id(&rollout.to_job_config_id)?;
    let from_config = match rollout.from_job_config_id.as_ref() {
        Some(from_id) => Some(store.get_job_config_by_id(from_id)?),
        None => None,
    };

    let mut envelope = parse_rollout_envelope(&rollout.config_json)?;
    let plans = build_group_plans(&to_config, from_config.as_ref(), store, &rollout)?;
    ensure_runtime_groups(&mut envelope.runtime, &plans);

    let group_set = if groups.is_empty() {
        plans
            .iter()
            .map(|plan| plan.name.clone())
            .collect::<HashSet<_>>()
    } else {
        groups.iter().cloned().collect::<HashSet<_>>()
    };

    for group in &group_set {
        if !plans.iter().any(|plan| plan.name == *group) {
            bail!("Group {} is not part of rollout {}", group, rollout.id);
        }
    }

    for group in group_set {
        let entry = envelope.runtime.groups.entry(group).or_default();
        entry.pending_manual_advances = entry.pending_manual_advances.saturating_add(1);
    }

    let config_json = serde_json::to_string(&envelope)?;
    store.update_rollout_config_json(rollout.id, &config_json)?;

    store.update_rollout_status(
        rollout.id,
        "pending",
        rollout.incomplete,
        Some("Manual rollout advance queued"),
    )?;
    let metadata = json!({
        "groups": groups,
        "priority": priority,
    })
    .to_string();
    let _ = store.append_rollout_event(
        rollout.id,
        &rollout.job_id,
        None,
        "info",
        "Manual rollout advance queued",
        Some(&metadata),
    );
    store.enqueue_eval(
        "rollout_tick",
        Some(&rollout.job_id),
        Some(&rollout.to_job_config_id),
        None,
        Some(priority),
    )?;

    store
        .get_rollout(rollout.id)?
        .with_context(|| format!("Rollout {} not found after update", rollout.id))
}

#[instrument(skip_all, fields(rollout_id, revert, priority))]
pub fn stop_rollout(store: &Store, rollout_id: i64, revert: bool, priority: i64) -> R<Rollout> {
    let rollout = store
        .get_rollout(rollout_id)?
        .with_context(|| format!("Rollout {} not found", rollout_id))?;

    if is_terminal_rollout_status(&rollout.status) {
        return Ok(rollout);
    }

    if revert {
        let scheduler = Scheduler::new(store)?;
        let to_config = store.get_job_config_by_id(&rollout.to_job_config_id)?;
        rollback_rollout(store, &scheduler, &rollout, &to_config)?;
    }

    let message = if revert {
        "Rollout stopped and reverted"
    } else {
        "Rollout stopped"
    };
    store.update_rollout_status(rollout.id, "cancelled", false, Some(message))?;
    // A cancelled rollout releases deferred scale-ins too.
    store.apply_pending_scales_after_rollout(&rollout.job_id, &rollout.to_job_config_id)?;
    let metadata = json!({
        "status": "cancelled",
        "revert": revert,
        "priority": priority,
    })
    .to_string();
    let _ = store.append_rollout_event(
        rollout.id,
        &rollout.job_id,
        None,
        "warn",
        message,
        Some(&metadata),
    );

    store
        .get_rollout(rollout.id)?
        .with_context(|| format!("Rollout {} not found after stop", rollout.id))
}

#[instrument(skip_all, fields(rollout_id = rollout.id, plans = plans.len()))]
fn execute_tick(
    store: &Store,
    scheduler: &Scheduler<'_>,
    rollout: &Rollout,
    to_config: &JobConfig,
    from_config: Option<&JobConfig>,
    plans: &[GroupPlan],
    runtime: &mut RolloutRuntime,
    tick_inputs: &TickInputs,
    now: OffsetDateTime,
) -> R<TickOutcome> {
    if plans.is_empty() {
        return Ok(TickOutcome {
            status: "completed".to_string(),
            incomplete: false,
            message: "No groups to roll out".to_string(),
            next_delay: None,
            event_group: None,
        });
    }

    let mut pending_wait: Option<Duration> = None;
    let mut pending_message = "Rollout waiting".to_string();
    let mut any_incomplete = false;
    let mut any_waiting_manual = false;

    for plan in plans {
        let state = runtime.groups.entry(plan.name.clone()).or_default();
        let action = execute_group_tick(
            store,
            scheduler,
            rollout,
            to_config,
            from_config,
            plan,
            state,
            tick_inputs,
            now,
        )?;

        match action {
            GroupAction::Mutated { delay, message } => {
                let delay = delay
                    .or(plan.config.allocation_stagger)
                    .unwrap_or_else(|| Duration::from_millis(0));
                debug!(
                    rollout_id = rollout.id,
                    group = %plan.name,
                    ?delay,
                    message = %message,
                    "Rollout group mutated"
                );
                return Ok(TickOutcome {
                    status: "running".to_string(),
                    incomplete: false,
                    message,
                    next_delay: Some(delay),
                    event_group: Some(plan.name.clone()),
                });
            }
            GroupAction::Wait { delay, message } => {
                debug!(
                    rollout_id = rollout.id,
                    group = %plan.name,
                    ?delay,
                    message = %message,
                    "Rollout group waiting"
                );
                pending_wait = Some(match pending_wait {
                    Some(existing) => existing.min(delay),
                    None => delay,
                });
                pending_message = message;
                let manual_waiting = plan.progression == RolloutProgression::Manual
                    && state.pending_manual_advances == 0
                    && state.active_step_started_at.is_none()
                    && state.step_index < plan.step_fractions.len();
                any_waiting_manual |= manual_waiting;
                any_incomplete = true;
            }
            GroupAction::Idle => {
                if state.step_index < plan.step_fractions.len() {
                    any_incomplete = true;
                }
            }
            GroupAction::Terminal {
                status,
                incomplete,
                message,
            } => {
                warn!(
                    rollout_id = rollout.id,
                    group = %plan.name,
                    status = %status,
                    incomplete,
                    message = %message,
                    "Rollout reached terminal group action"
                );
                return Ok(TickOutcome {
                    status,
                    incomplete,
                    message,
                    next_delay: None,
                    event_group: Some(plan.name.clone()),
                });
            }
        }
    }

    if !any_incomplete {
        return Ok(TickOutcome {
            status: "completed".to_string(),
            incomplete: false,
            message: "Rollout completed".to_string(),
            next_delay: None,
            event_group: None,
        });
    }

    if any_waiting_manual {
        let start_pending = plans.iter().any(|plan| {
            let state = runtime.groups.get(&plan.name).cloned().unwrap_or_default();
            plan.progression == RolloutProgression::Manual
                && state.active_step_started_at.is_none()
                && state.pending_manual_advances == 0
                && state.step_index == 0
                && state.step_index < plan.step_fractions.len()
        });
        return Ok(TickOutcome {
            status: if start_pending {
                "waiting_manual_start".to_string()
            } else {
                "waiting_manual_advance".to_string()
            },
            incomplete: false,
            message: "Waiting for manual rollout advance".to_string(),
            next_delay: None,
            event_group: None,
        });
    }

    Ok(TickOutcome {
        status: "running".to_string(),
        incomplete: false,
        message: pending_message,
        next_delay: Some(pending_wait.unwrap_or(DEFAULT_RETRY_DELAY)),
        event_group: None,
    })
}

#[instrument(
    skip_all,
    fields(rollout_id = rollout.id, group = %plan.name, step_index = state.step_index)
)]
fn execute_group_tick(
    store: &Store,
    scheduler: &Scheduler<'_>,
    rollout: &Rollout,
    to_config: &JobConfig,
    _from_config: Option<&JobConfig>,
    plan: &GroupPlan,
    state: &mut GroupRuntime,
    tick_inputs: &TickInputs,
    now: OffsetDateTime,
) -> R<GroupAction> {
    if state.step_index >= plan.step_fractions.len() {
        return Ok(GroupAction::Idle);
    }

    if state.active_step_started_at.is_none() {
        if plan.progression == RolloutProgression::Manual && state.pending_manual_advances == 0 {
            return Ok(GroupAction::Wait {
                delay: DEFAULT_RETRY_DELAY,
                message: format!("Group {} waiting for manual advance", plan.name),
            });
        }

        if let Some(step_stagger) = plan.config.step_stagger {
            if let Some(last_completed) =
                parse_optional_ts(state.last_step_completed_at.as_deref())?
            {
                let elapsed = now - last_completed;
                let required = time::Duration::try_from(step_stagger)?;
                if elapsed < required {
                    let wait = (required - elapsed)
                        .try_into()
                        .context("step stagger delay overflow")?;
                    return Ok(GroupAction::Wait {
                        delay: wait,
                        message: format!("Group {} waiting for step stagger", plan.name),
                    });
                }
            }
        }

        if plan.progression == RolloutProgression::Manual && state.pending_manual_advances > 0 {
            state.pending_manual_advances -= 1;
        }
        state.active_step_started_at = Some(format_ts(now)?);
        state.reached_target_at = None;
        state.blocked_since = None;
        debug!(
            rollout_id = rollout.id,
            group = %plan.name,
            step_index = state.step_index,
            progression = ?plan.progression,
            replacement = ?plan.replacement,
            "Started rollout step"
        );
    }

    let step_fraction = plan
        .step_fractions
        .get(state.step_index)
        .copied()
        .unwrap_or(1.0);
    let step_targets = build_step_targets(plan, step_fraction, state.step_index);
    debug!(
        rollout_id = rollout.id,
        group = %plan.name,
        step_index = state.step_index,
        step_fraction,
        needs_new_placement = step_targets.needs_new_placement,
        needs_old_retire = step_targets.needs_old_retire,
        "Evaluating rollout group step"
    );

    for operation in step_operation_order(&plan.replacement) {
        if let Some(action) = execute_step_operation(
            operation,
            store,
            scheduler,
            rollout,
            to_config,
            plan,
            state,
            tick_inputs,
            now,
            &step_targets,
        )? {
            return Ok(action);
        }
    }

    let completed_step = state.step_index;
    state.step_index += 1;
    state.active_step_started_at = None;
    state.reached_target_at = None;
    state.blocked_since = None;
    state.last_step_completed_at = Some(format_ts(now)?);

    let message = match plan.sequential_slots.as_ref() {
        // Make sequential progress legible in the event log: each completed
        // step is exactly one slot becoming healthy on the new config.
        Some(slots) => format!(
            "Group {} advanced to step {} (slot {} updated and healthy)",
            plan.name,
            state.step_index,
            slots.slot_for_step(completed_step)
        ),
        None => format!("Group {} advanced to step {}", plan.name, state.step_index),
    };
    Ok(GroupAction::Mutated {
        delay: Some(Duration::from_millis(0)),
        message,
    })
}

fn build_step_targets(plan: &GroupPlan, step_fraction: f64, step_index: usize) -> StepTargets {
    let mut targets = StepTargets::default();
    for task in &plan.tasks {
        let sequential = plan
            .sequential_slots
            .as_ref()
            .filter(|slots| slots.task_name == task.name);
        let target_new = if let Some(slots) = sequential {
            // Exact integer math for the slot task: step i has replaced
            // slots count-1 ..= count-1-i, i.e. i+1 slots, capped at the
            // staged floor (slots below `update_from` are never updated).
            let updatable = slots.count.saturating_sub(slots.update_from);
            (step_index.min(u32::MAX as usize) as u32)
                .saturating_add(1)
                .min(updatable)
                .min(task.desired_new)
        } else {
            ((task.desired_new as f64) * step_fraction)
                .ceil()
                .min(task.desired_new as f64) as u32
        };
        let target_old = task.desired_new.saturating_sub(target_new);
        targets
            .per_task_target_new
            .insert(task.name.clone(), target_new);
        targets
            .per_task_target_old
            .insert(task.name.clone(), target_old);

        if task.new_task_id.is_some() && task.current_new < target_new {
            targets.needs_new_placement = true;
        }
        if (task.current_old_allocs.len() as u32) > target_old {
            targets.needs_old_retire = true;
        }
    }
    targets
}

fn step_operation_order(replacement: &RolloutReplacement) -> [StepOperation; 3] {
    match replacement {
        RolloutReplacement::CreateFirst => [
            StepOperation::PlaceNew,
            StepOperation::WaitForHealthy,
            StepOperation::RetireOld,
        ],
        RolloutReplacement::DestroyFirst => [
            StepOperation::RetireOld,
            StepOperation::PlaceNew,
            StepOperation::WaitForHealthy,
        ],
    }
}

fn execute_step_operation(
    operation: StepOperation,
    store: &Store,
    scheduler: &Scheduler<'_>,
    rollout: &Rollout,
    to_config: &JobConfig,
    plan: &GroupPlan,
    state: &mut GroupRuntime,
    tick_inputs: &TickInputs,
    now: OffsetDateTime,
    step_targets: &StepTargets,
) -> R<Option<GroupAction>> {
    match operation {
        StepOperation::PlaceNew => {
            if !step_targets.needs_new_placement {
                return Ok(None);
            }
            Ok(Some(try_place_for_group(
                store,
                scheduler,
                rollout,
                to_config,
                plan,
                state,
                tick_inputs,
                now,
                &step_targets.per_task_target_new,
            )?))
        }
        StepOperation::WaitForHealthy => {
            match evaluate_group_health(
                plan,
                state,
                now,
                &step_targets.per_task_target_new,
                &tick_inputs.allocations_by_task,
                &tick_inputs.allocation_health,
            )? {
                HealthState::Healthy => Ok(None),
                HealthState::Waiting => {
                    let wait = health_wait_delay(plan, state, now)?;
                    let message = match plan.sequential_slots.as_ref() {
                        // Sequential groups halt here: the named slot's
                        // replacement must pass the health gate before the
                        // next (lower) slot is touched.
                        Some(slots) => format!(
                            "Group {} waiting for slot {} replacement to become healthy \
                             before updating the next slot",
                            plan.name,
                            slots.slot_for_step(state.step_index)
                        ),
                        None => format!("Group {} waiting for healthy window", plan.name),
                    };
                    Ok(Some(GroupAction::Wait {
                        delay: wait,
                        message,
                    }))
                }
                HealthState::Deadline(action) => Ok(Some(on_health_deadline(
                    store, scheduler, rollout, to_config, plan, action,
                )?)),
            }
        }
        StepOperation::RetireOld => {
            if !step_targets.needs_old_retire {
                return Ok(None);
            }
            let retire = retire_old_allocations_for_group(
                store,
                plan,
                state,
                now,
                &step_targets.per_task_target_old,
            )?;
            if retire.removed > 0 {
                let message = if retire.removed_slots.is_empty() {
                    format!(
                        "Group {} retired {} old allocations",
                        plan.name, retire.removed
                    )
                } else {
                    // Sequential slot groups: name the slot(s) so the event
                    // log shows the ordinal order of the update.
                    format!(
                        "Group {} retired old allocation for slot {} (sequential update, \
                         highest slot first)",
                        plan.name,
                        retire
                            .removed_slots
                            .iter()
                            .map(|slot| slot.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                return Ok(Some(GroupAction::Mutated {
                    delay: plan.config.allocation_stagger,
                    message,
                }));
            }
            if retire.blocked {
                return Ok(Some(GroupAction::Wait {
                    delay: DEFAULT_RETRY_DELAY,
                    message: format!("Group {} waiting for batch retire window", plan.name),
                }));
            }
            Ok(None)
        }
    }
}

#[instrument(
    skip_all,
    fields(rollout_id = rollout.id, group = %plan.name, in_place = plan.config.in_place.unwrap_or(false))
)]
fn try_place_for_group(
    store: &Store,
    scheduler: &Scheduler<'_>,
    rollout: &Rollout,
    to_config: &JobConfig,
    plan: &GroupPlan,
    state: &mut GroupRuntime,
    tick_inputs: &TickInputs,
    now: OffsetDateTime,
    per_task_target_new: &HashMap<String, u32>,
) -> R<GroupAction> {
    let mut target_counts: HashMap<i64, u32> = HashMap::new();
    for task_id in &tick_inputs.new_task_ids {
        target_counts.insert(
            *task_id,
            tick_inputs
                .new_counts_by_task
                .get(task_id)
                .copied()
                .unwrap_or_default(),
        );
    }

    for task in &plan.tasks {
        if let Some(task_id) = task.new_task_id {
            let target_new = per_task_target_new
                .get(&task.name)
                .copied()
                .unwrap_or_default();
            target_counts.insert(task_id, target_new);
        }
    }

    let mut placement_note: Option<String> = None;
    if plan.config.in_place.unwrap_or(false) {
        if let Some(excluded_nodes) = build_in_place_exclusions(
            store,
            plan,
            per_task_target_new,
            &tick_inputs.new_allocations,
        )? {
            match scheduler.plan_and_apply_with_targets_and_exclusions(
                &rollout.to_job_config_id,
                PlanMode::Reschedule,
                &target_counts,
                Some(&excluded_nodes),
            ) {
                Ok(outcome) => {
                    state.blocked_since = None;
                    if !outcome.allocations.is_empty() {
                        dispatch_allocations(store, &outcome.allocations)?;
                        return Ok(GroupAction::Mutated {
                            delay: plan.config.allocation_stagger,
                            message: format!(
                                "Group {} placed {} replacement allocations in-place",
                                plan.name,
                                outcome.allocations.len()
                            ),
                        });
                    }
                    placement_note = Some(
                        "in-place preferred placement unavailable; falling back to cross-node"
                            .to_string(),
                    );
                }
                Err(err) => {
                    placement_note = Some(format!(
                        "in-place placement blocked ({err}); falling back to cross-node"
                    ));
                }
            }
        }
    }

    match scheduler.plan_and_apply_with_targets(
        &rollout.to_job_config_id,
        PlanMode::Reschedule,
        &target_counts,
    ) {
        Ok(outcome) => {
            state.blocked_since = None;
            if !outcome.allocations.is_empty() {
                dispatch_allocations(store, &outcome.allocations)?;
                return Ok(GroupAction::Mutated {
                    delay: plan.config.allocation_stagger,
                    message: format!(
                        "Group {} placed {} replacement allocations{}",
                        plan.name,
                        outcome.allocations.len(),
                        placement_note
                            .as_ref()
                            .map(|note| format!(" ({note})"))
                            .unwrap_or_default()
                    ),
                });
            }

            // Replacements held by `dependsOn` (a batch step of the new
            // version that has not exited yet) are not short of capacity:
            // wait for the dependency without arming capacityBlockTimeout.
            let held_here: Vec<&str> = outcome
                .held_tasks
                .iter()
                .map(String::as_str)
                .filter(|held| plan.tasks.iter().any(|task| task.name == *held))
                .collect();
            if !held_here.is_empty() {
                return Ok(GroupAction::Wait {
                    delay: DEPENDENCY_WAIT_DELAY,
                    message: format!(
                        "Group {} waiting for dependencies of {} to finish",
                        plan.name,
                        held_here.join(", ")
                    ),
                });
            }

            on_placement_wait(
                store,
                scheduler,
                rollout,
                to_config,
                plan,
                state,
                now,
                format!(
                    "Group {} waiting for placement capacity{}",
                    plan.name,
                    placement_note
                        .as_ref()
                        .map(|note| format!(" ({note})"))
                        .unwrap_or_default()
                ),
            )
        }
        Err(err) => {
            // A replacement that cannot coexist with the allocation it
            // replaces, because both need the same static port and the old
            // one still holds it, can never be placed first. Stop the old
            // allocation(s) and place on the next tick (a destructive update)
            // instead of waiting for capacity that cannot appear.
            if let Some(blocked) = err.downcast_ref::<PlacementBlocked>() {
                let retired = retire_static_port_holders(store, plan, blocked)?;
                if retired > 0 {
                    state.blocked_since = None;
                    return Ok(GroupAction::Mutated {
                        delay: plan.config.allocation_stagger,
                        message: format!(
                            "Group {} cannot surge: static port {} is held by the version \
                             being replaced; stopped {} old allocation(s) before placing \
                             the new one",
                            plan.name,
                            blocked.static_port().unwrap_or_default(),
                            retired
                        ),
                    });
                }
            }
            on_placement_wait(
                store,
                scheduler,
                rollout,
                to_config,
                plan,
                state,
                now,
                format!(
                    "Group {} placement blocked: {}{}",
                    plan.name,
                    err,
                    placement_note
                        .as_ref()
                        .map(|note| format!(" ({note})"))
                        .unwrap_or_default()
                ),
            )
        }
    }
}

/// Stop the old allocations whose static port blocks this group's
/// replacements: for each task the plan could not place because of a static
/// port, up to as many old allocations of the same task as replacements are
/// blocked, choosing only old allocations that hold that port (stopping any
/// other would not help). Batch tasks are left alone; old batch work is only
/// ever retired by `batchKillAfter`. Returns how many were stopped.
fn retire_static_port_holders(
    store: &Store,
    plan: &GroupPlan,
    blocked: &PlacementBlocked,
) -> R<usize> {
    let mut retire: Vec<Allocation> = Vec::new();
    for unplaced in &blocked.unplaced {
        let Some(port) = unplaced.static_port else {
            continue;
        };
        let Some(task) = plan
            .tasks
            .iter()
            .find(|task| task.name == unplaced.task_name)
        else {
            continue;
        };
        if matches!(task.lifecycle, TaskLifecycle::Batch) {
            continue;
        }
        let mut holders = task
            .current_old_allocs
            .iter()
            .filter(|allocation| {
                allocation.network.as_ref().is_some_and(|network| {
                    network.ports.iter().any(|held| held.host_port == port)
                        || network
                            .upstreams
                            .iter()
                            .any(|upstream| upstream.local_port == Some(port))
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        holders.sort_by_key(|allocation| allocation.id);
        retire.extend(holders.into_iter().take(unplaced.count as usize));
    }
    if retire.is_empty() {
        return Ok(0);
    }
    store.remove_allocations(&retire, StopCause::Update)?;
    Ok(retire.len())
}

fn on_placement_wait(
    store: &Store,
    scheduler: &Scheduler<'_>,
    rollout: &Rollout,
    to_config: &JobConfig,
    plan: &GroupPlan,
    state: &mut GroupRuntime,
    now: OffsetDateTime,
    message: String,
) -> R<GroupAction> {
    let timeout_action = plan
        .config
        .on_capacity_timeout
        .clone()
        .unwrap_or(RolloutCapacityTimeoutAction::Fail);
    let blocked_since = parse_optional_ts(state.blocked_since.as_deref())?.unwrap_or(now);
    if state.blocked_since.is_none() {
        state.blocked_since = Some(format_ts(now)?);
        debug!(
            rollout_id = rollout.id,
            group = %plan.name,
            message = %message,
            "Rollout group became capacity blocked"
        );
    }

    let elapsed = now - blocked_since;
    let mut remaining: Option<time::Duration> = None;
    if let Some(timeout) = plan.config.capacity_block_timeout {
        let timeout = time::Duration::try_from(timeout)?;
        if elapsed >= timeout {
            warn!(
                rollout_id = rollout.id,
                group = %plan.name,
                timeout_secs = timeout.whole_seconds(),
                action = ?timeout_action,
                "Capacity block timeout reached"
            );
            return on_capacity_timeout(
                store,
                scheduler,
                rollout,
                to_config,
                timeout_action,
                plan.config.rollback_on_failure.unwrap_or(false),
            );
        }
        remaining = Some(timeout - elapsed);
    }

    Ok(GroupAction::Wait {
        delay: placement_wait_delay(elapsed, remaining),
        message,
    })
}

/// How long a capacity-blocked group waits before its next tick: 2s at
/// first, then half of how long it has been blocked, capped at 30s, and
/// never past the moment `capacityBlockTimeout` would fire.
///
/// A blocked rollout used to re-tick every 2s however long it waited, which
/// put two eval events (and often a rejected plan) on the event stream every
/// two seconds — hundreds of identical events for one stuck rollout. Capacity
/// that does show up is still found within a tick of at most 30s.
fn placement_wait_delay(
    blocked_for: time::Duration,
    until_timeout: Option<time::Duration>,
) -> Duration {
    let backoff = Duration::try_from(blocked_for / 2)
        .unwrap_or_default()
        .clamp(DEFAULT_RETRY_DELAY, MAX_PLACEMENT_WAIT_DELAY);
    match until_timeout.and_then(|left| Duration::try_from(left).ok()) {
        Some(left) => backoff.min(left.max(Duration::from_millis(100))),
        None => backoff,
    }
}

fn build_in_place_exclusions(
    store: &Store,
    plan: &GroupPlan,
    per_task_target_new: &HashMap<String, u32>,
    new_allocations: &[Allocation],
) -> R<Option<HashSet<String>>> {
    let mut task_old_nodes: HashMap<i64, HashSet<String>> = HashMap::new();
    let mut required_per_task: HashMap<i64, u32> = HashMap::new();

    for task in &plan.tasks {
        let Some(task_id) = task.new_task_id else {
            continue;
        };
        if task.current_old_allocs.is_empty() {
            continue;
        }

        let desired = per_task_target_new
            .get(&task.name)
            .copied()
            .unwrap_or_default();
        let required_in_place = desired.min(task.current_old_allocs.len() as u32);
        if required_in_place == 0 {
            continue;
        }

        let nodes = task_old_nodes.entry(task_id).or_default();
        for old in &task.current_old_allocs {
            nodes.insert(old.node_id.clone());
        }
        required_per_task.insert(task_id, required_in_place);
    }

    if required_per_task.is_empty() {
        return Ok(None);
    }

    let mut satisfied = true;
    for (task_id, required_in_place) in &required_per_task {
        let Some(nodes) = task_old_nodes.get(task_id) else {
            continue;
        };
        let placed_in_place = new_allocations
            .iter()
            .filter(|alloc| alloc.task_config_id == *task_id && nodes.contains(&alloc.node_id))
            .count() as u32;
        if placed_in_place < *required_in_place {
            satisfied = false;
            break;
        }
    }

    if satisfied {
        return Ok(None);
    }

    let mut allowed_nodes = HashSet::new();
    for nodes in task_old_nodes.values() {
        allowed_nodes.extend(nodes.iter().cloned());
    }
    if allowed_nodes.is_empty() {
        return Ok(None);
    }

    let available_nodes = store.available_node_ids()?;
    if available_nodes.is_empty() {
        return Ok(None);
    }

    let allowed_available = allowed_nodes
        .intersection(&available_nodes)
        .cloned()
        .collect::<HashSet<_>>();
    if allowed_available.is_empty() {
        return Ok(None);
    }

    let excluded = available_nodes
        .difference(&allowed_available)
        .cloned()
        .collect::<HashSet<_>>();
    Ok(Some(excluded))
}

fn on_capacity_timeout(
    store: &Store,
    scheduler: &Scheduler<'_>,
    rollout: &Rollout,
    to_config: &JobConfig,
    action: RolloutCapacityTimeoutAction,
    rollback_on_failure: bool,
) -> R<GroupAction> {
    warn!(
        rollout_id = rollout.id,
        action = ?action,
        rollback_on_failure,
        "Applying capacity timeout action"
    );
    match action {
        RolloutCapacityTimeoutAction::CompleteIncomplete => Ok(GroupAction::Terminal {
            status: "completed_incomplete".to_string(),
            incomplete: true,
            message: "Rollout completed incompletely after capacity timeout".to_string(),
        }),
        RolloutCapacityTimeoutAction::Rollback => {
            rollback_rollout(store, scheduler, rollout, to_config)?;
            Ok(GroupAction::Terminal {
                status: "failed".to_string(),
                incomplete: false,
                message: "Rollout failed and rolled back after capacity timeout".to_string(),
            })
        }
        RolloutCapacityTimeoutAction::Fail => {
            if rollback_on_failure {
                rollback_rollout(store, scheduler, rollout, to_config)?;
                Ok(GroupAction::Terminal {
                    status: "failed".to_string(),
                    incomplete: false,
                    message: "Rollout failed and rolled back after capacity timeout".to_string(),
                })
            } else {
                Ok(GroupAction::Terminal {
                    status: "failed".to_string(),
                    incomplete: false,
                    message: "Rollout failed after capacity timeout".to_string(),
                })
            }
        }
    }
}

fn on_health_deadline(
    store: &Store,
    scheduler: &Scheduler<'_>,
    rollout: &Rollout,
    to_config: &JobConfig,
    plan: &GroupPlan,
    action: RolloutDeadlineAction,
) -> R<GroupAction> {
    warn!(
        rollout_id = rollout.id,
        group = %plan.name,
        action = ?action,
        rollback_on_failure = plan.config.rollback_on_failure.unwrap_or(false),
        "Applying health deadline action"
    );
    match action {
        RolloutDeadlineAction::Pause => Ok(GroupAction::Terminal {
            status: "paused".to_string(),
            incomplete: false,
            message: format!("Group {} paused after health deadline", plan.name),
        }),
        RolloutDeadlineAction::Fail => {
            if plan.config.rollback_on_failure.unwrap_or(false) {
                rollback_rollout(store, scheduler, rollout, to_config)?;
                Ok(GroupAction::Terminal {
                    status: "failed".to_string(),
                    incomplete: false,
                    message: "Rollout failed and rolled back after health deadline".to_string(),
                })
            } else {
                Ok(GroupAction::Terminal {
                    status: "failed".to_string(),
                    incomplete: false,
                    message: "Rollout failed after health deadline".to_string(),
                })
            }
        }
        RolloutDeadlineAction::Rollback => {
            rollback_rollout(store, scheduler, rollout, to_config)?;
            Ok(GroupAction::Terminal {
                status: "failed".to_string(),
                incomplete: false,
                message: "Rollout failed and rolled back after health deadline".to_string(),
            })
        }
    }
}

fn evaluate_group_health(
    plan: &GroupPlan,
    state: &mut GroupRuntime,
    now: OffsetDateTime,
    per_task_target_new: &HashMap<String, u32>,
    allocations_by_task: &HashMap<i64, Vec<i64>>,
    allocation_health: &HashMap<i64, AllocationHealthSnapshot>,
) -> R<HealthState> {
    if plan.config.ignore_health_checks.unwrap_or(false) {
        return Ok(HealthState::Healthy);
    }

    let started_at = parse_optional_ts(state.active_step_started_at.as_deref())?.unwrap_or(now);
    let deadline_action = plan
        .config
        .on_health_deadline
        .clone()
        .unwrap_or(RolloutDeadlineAction::Rollback);

    if let Some(deadline) = plan.config.healthy_deadline {
        let deadline = time::Duration::try_from(deadline)?;
        if now - started_at >= deadline {
            return Ok(HealthState::Deadline(deadline_action));
        }
    }

    let mut all_targets_healthy = true;
    for task in &plan.tasks {
        let Some(task_id) = task.new_task_id else {
            continue;
        };

        let target_new = per_task_target_new
            .get(&task.name)
            .copied()
            .unwrap_or_default();
        if target_new == 0 {
            continue;
        }

        let Some(allocations) = allocations_by_task.get(&task_id) else {
            all_targets_healthy = false;
            break;
        };
        if allocations.len() < target_new as usize {
            all_targets_healthy = false;
            break;
        }

        let healthy_count = allocations
            .iter()
            .filter(|allocation_id| {
                allocation_health
                    .get(allocation_id)
                    .map(|entry| entry.healthy)
                    .unwrap_or(false)
            })
            .count();
        if healthy_count < target_new as usize {
            all_targets_healthy = false;
            break;
        }
    }

    if !all_targets_healthy {
        state.reached_target_at = None;
        return Ok(HealthState::Waiting);
    }

    let reached_at = parse_optional_ts(state.reached_target_at.as_deref())?;
    let reached_at = match reached_at {
        Some(ts) => ts,
        None => {
            state.reached_target_at = Some(format_ts(now)?);
            now
        }
    };

    let min_healthy = plan
        .config
        .min_healthy_time
        .unwrap_or(Duration::from_secs(0));
    let min_healthy = time::Duration::try_from(min_healthy)?;
    if now - reached_at >= min_healthy {
        Ok(HealthState::Healthy)
    } else {
        Ok(HealthState::Waiting)
    }
}

fn health_wait_delay(plan: &GroupPlan, state: &GroupRuntime, now: OffsetDateTime) -> R<Duration> {
    if plan.config.ignore_health_checks.unwrap_or(false) {
        return Ok(Duration::from_millis(0));
    }

    let Some(reached_at) = parse_optional_ts(state.reached_target_at.as_deref())? else {
        return Ok(DEFAULT_RETRY_DELAY);
    };
    let min_healthy = plan
        .config
        .min_healthy_time
        .unwrap_or(Duration::from_secs(0));
    let min_healthy = time::Duration::try_from(min_healthy)?;
    let elapsed = now - reached_at;
    if elapsed >= min_healthy {
        return Ok(Duration::from_millis(0));
    }

    (min_healthy - elapsed)
        .try_into()
        .context("healthy wait delay overflow")
}

fn retire_old_allocations_for_group(
    store: &Store,
    plan: &GroupPlan,
    state: &GroupRuntime,
    now: OffsetDateTime,
    per_task_target_old: &HashMap<String, u32>,
) -> R<RetireOutcome> {
    let mut outcome = RetireOutcome::default();

    for task in &plan.tasks {
        let target_old = per_task_target_old
            .get(&task.name)
            .copied()
            .unwrap_or_default();
        let current_old = task.current_old_allocs.len() as u32;
        if current_old <= target_old {
            continue;
        }

        // The batch retire window: a running old batch allocation is left to
        // finish until `batchKillAfter` has elapsed in this step (forever when
        // unset). It only guards work in flight, so it is checked per
        // allocation below rather than for the whole task.
        let batch_window_closed = match (&task.lifecycle, plan.config.batch_kill_after) {
            (TaskLifecycle::Batch, None) => true,
            (TaskLifecycle::Batch, Some(kill_after)) => {
                let started_at =
                    parse_optional_ts(state.active_step_started_at.as_deref())?.unwrap_or(now);
                now - started_at < time::Duration::try_from(kill_after)?
            }
            _ => false,
        };

        let to_remove = (current_old - target_old) as usize;
        let mut old_allocs = task.current_old_allocs.clone();
        // Finished allocations go first. They run nothing, so retiring one
        // never cuts work short or costs capacity, and keeping it over a live
        // one would only shrink what still serves. Before this, a resubmitted
        // DAG job whose old batch steps had all exited waited on the batch
        // window forever: every old allocation was finished, none could be
        // retired, and the new version was never placed.
        old_allocs.sort_by_key(|alloc| (!allocation_status_is_terminal(&alloc.status), alloc.id));

        let sequential = plan
            .sequential_slots
            .as_ref()
            .filter(|slots| slots.task_name == task.name);
        let slot_indices = match (sequential, old_allocs.first()) {
            // Sequential slot groups retire in DESCENDING slot order (the
            // K8s StatefulSet update order: highest ordinal first, so
            // followers update before slot 0 by convention). The slot
            // identity lives on the allocation's instance row.
            (Some(slots), Some(first)) => {
                let indices = store.slot_indices_for_job_group(&first.job_id, &plan.name)?;
                // Never retire a slot below the staged floor: those keep
                // running their current allocations untouched.
                old_allocs.retain(|alloc| {
                    alloc
                        .instance_id
                        .as_deref()
                        .and_then(|id| indices.get(id))
                        .is_none_or(|slot| *slot >= slots.update_from)
                });
                // Allocations without a resolvable slot (anomalies) sort
                // first and are cleaned up before any real ordinal.
                old_allocs.sort_by_key(|alloc| {
                    std::cmp::Reverse(
                        alloc
                            .instance_id
                            .as_deref()
                            .and_then(|id| indices.get(id))
                            .copied()
                            .unwrap_or(u32::MAX),
                    )
                });
                Some(indices)
            }
            _ => None,
        };

        // One replicated op for the whole batch, not one per allocation.
        // This used to be a loop over `remove_allocation_by_id_because`,
        // which is a raft quorum round each: a step retiring 25% of a
        // 10,000-allocation job cost 2,500 sequential round trips in a
        // single tick. `remove_allocations` writes the same rows, releases
        // the same instance bindings, bumps the same epochs and stages the
        // same stop notices, in one transaction.
        let mut batch: Vec<_> = old_allocs.into_iter().take(to_remove).collect();
        if batch_window_closed {
            let chosen = batch.len();
            batch.retain(|alloc| allocation_status_is_terminal(&alloc.status));
            outcome.blocked |= batch.len() < chosen;
        }
        if batch.is_empty() {
            continue;
        }
        store.remove_allocations(&batch, StopCause::Update)?;
        outcome.removed += batch.len();
        if let Some(indices) = slot_indices.as_ref() {
            for allocation in &batch {
                if let Some(slot) = allocation
                    .instance_id
                    .as_deref()
                    .and_then(|instance_id| indices.get(instance_id))
                {
                    outcome.removed_slots.push(*slot);
                }
            }
        }
    }

    Ok(outcome)
}

fn rollback_rollout(
    store: &Store,
    scheduler: &Scheduler<'_>,
    rollout: &Rollout,
    to_config: &JobConfig,
) -> R<()> {
    let _ = store.remove_allocs_for_job_config(&to_config.id())?;

    if let Some(from_config_id) = rollout.from_job_config_id.as_ref() {
        // The new version is already gone. If the old one cannot be placed
        // back (no capacity, a lost conflict race), the failed tick marks
        // the rollout terminal and nothing would retry it, so queue a
        // reschedule of the old version first. A NotRaftLeader rejection is
        // the exception: the tick is released and re-runs this rollback.
        let outcome = scheduler
            .plan_and_apply(from_config_id, PlanMode::Reschedule)
            .inspect_err(|error| {
                if !crate::storage::raft::driver::NotRaftLeader::in_chain(error) {
                    let _ = store.queue_reschedule_retry(&rollout.job_id, from_config_id, error);
                }
            })?;
        if !outcome.allocations.is_empty() {
            dispatch_allocations(store, &outcome.allocations)?;
        }
    }

    Ok(())
}

fn build_group_plans(
    to_config: &JobConfig,
    from_config: Option<&JobConfig>,
    store: &Store,
    rollout: &Rollout,
) -> R<Vec<GroupPlan>> {
    let to_tasks = store.get_task_configs_for_job_config(&rollout.to_job_config_id)?;
    let to_alloc_counts = count_allocations_by_task(store, &rollout.to_job_config_id)?;

    let from_tasks = if let Some(from_id) = rollout.from_job_config_id.as_ref() {
        store.get_task_configs_for_job_config(from_id)?
    } else {
        Vec::new()
    };
    // Old allocations to roll: EVERY live allocation of the job that is not
    // on the destination config — not just the immediate from-config's. A
    // staged rollout (`rollout.slots.updateFrom`) deliberately completes
    // with low slots still running an older config; when a later resubmit
    // lowers the floor (the canary-promote workflow), those older-config
    // allocations must be visible to the new rollout so they are replaced
    // with the same ordering and health gating as everything else.
    let old_allocs = if rollout.from_job_config_id.is_some() {
        store
            .allocs_for_job(&rollout.job_id)?
            .into_iter()
            .filter(|allocation| allocation.job_config_id != rollout.to_job_config_id)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    // Resolve each old allocation's (group, task name) through its own
    // config version's task list (allocations from configs older than the
    // from-config carry task ids the from-config does not know).
    let mut tasks_by_config: HashMap<String, HashMap<i64, TaskConfig>> = HashMap::new();
    if let Some(from_id) = rollout.from_job_config_id.as_ref() {
        tasks_by_config.insert(from_id.clone(), from_tasks.iter().cloned().collect());
    }
    let mut old_allocs_by_group_task: HashMap<(String, String), Vec<Allocation>> = HashMap::new();
    for allocation in old_allocs {
        let config_tasks = match tasks_by_config.entry(allocation.job_config_id.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => entry.insert(
                store
                    .get_task_configs_for_job_config(&allocation.job_config_id)?
                    .into_iter()
                    .collect(),
            ),
        };
        let Some(task) = config_tasks.get(&allocation.task_config_id) else {
            continue;
        };
        old_allocs_by_group_task
            .entry((task.group.clone(), task.name.clone()))
            .or_default()
            .push(allocation);
    }

    let mut by_group_new: HashMap<String, HashMap<String, (i64, TaskConfig)>> = HashMap::new();
    for (task_id, task) in to_tasks {
        by_group_new
            .entry(task.group.clone())
            .or_default()
            .insert(task.name.clone(), (task_id, task));
    }

    let mut by_group_old: HashMap<String, HashMap<String, (i64, TaskConfig)>> = HashMap::new();
    for (task_id, task) in from_tasks {
        by_group_old
            .entry(task.group.clone())
            .or_default()
            .insert(task.name.clone(), (task_id, task));
    }

    let mut groups = by_group_new
        .keys()
        .chain(by_group_old.keys())
        .cloned()
        .chain(
            old_allocs_by_group_task
                .keys()
                .map(|(group, _)| group.clone()),
        )
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    groups.sort();

    let mut plans = Vec::new();
    for group in groups {
        let new_tasks = by_group_new.get(&group).cloned().unwrap_or_default();
        let old_tasks = by_group_old.get(&group).cloned().unwrap_or_default();

        let task_names = new_tasks
            .keys()
            .chain(old_tasks.keys())
            .cloned()
            .chain(old_allocs_by_group_task.keys().filter_map(|(g, name)| {
                if *g == group {
                    Some(name.clone())
                } else {
                    None
                }
            }))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let configured_rollout = rollout_config_for_group(to_config, &group);

        let mut tasks = Vec::new();
        let mut base_count = 0;
        let mut slot_task: Option<(String, u32)> = None;
        let mut gang_group = false;
        for name in task_names {
            let new_entry = new_tasks.get(&name).cloned();
            let old_entry = old_tasks.get(&name).cloned();
            gang_group |= new_entry
                .as_ref()
                .is_some_and(|(_, task)| task.gang.is_some());

            // Keyed-mode stateful tasks are not driven by count.num: their
            // desired count is the number of live instances created via the
            // instance management API, so a config rollout replaces every
            // live keyed instance's allocation instead of converging to
            // count.num.
            let desired_new = match new_entry.as_ref() {
                Some((_, task))
                    if task.stateful.as_ref().is_some_and(|stateful| {
                        stateful.mode == crate::structs::InstanceMode::Keyed
                    }) =>
                {
                    store.count_live_keyed_instances(&rollout.job_id, &group, &name)?
                }
                // EFFECTIVE count, not the raw spec: a `legion job scale`
                // scale-OUT mid-rollout grows the step targets on the next
                // tick (docs/autoscaling.md; scale-in is recorded pending
                // and never shrinks a live rollout's targets).
                Some((_, task)) => {
                    store.effective_task_count(&rollout.job_id, &group, &name, task.count.num)?
                }
                None => 0,
            };
            base_count = base_count.max(desired_new);

            let lifecycle = new_entry
                .as_ref()
                .map(|(_, task)| task.lifecycle.clone())
                .or_else(|| old_entry.as_ref().map(|(_, task)| task.lifecycle.clone()))
                .unwrap_or(TaskLifecycle::Service);

            if let Some((_, task)) = new_entry.as_ref() {
                if task
                    .stateful
                    .as_ref()
                    .is_some_and(|stateful| stateful.mode == crate::structs::InstanceMode::Slot)
                {
                    // At most one slot-mode stateful task per group
                    // (enforced at job admission). Effective count, same
                    // reasoning as `desired_new` above.
                    slot_task = Some((
                        task.name.clone(),
                        store.effective_task_count(
                            &rollout.job_id,
                            &group,
                            &name,
                            task.count.num,
                        )?,
                    ));
                }
            }

            let new_task_id = new_entry.as_ref().map(|(id, _)| *id);
            let old_task_id = old_entry.as_ref().map(|(id, _)| *id);

            let current_new = new_task_id
                .and_then(|id| to_alloc_counts.get(&id).copied())
                .unwrap_or_default();
            let current_old_allocs = old_allocs_by_group_task
                .get(&(group.clone(), name.clone()))
                .cloned()
                .unwrap_or_default();

            tasks.push(GroupTaskPlan {
                name,
                lifecycle,
                old_task_id,
                new_task_id,
                desired_new,
                current_new,
                current_old_allocs,
            });
        }

        let effective_rollout = rollout_strategy::resolve_rollout_strategy(
            configured_rollout,
            &group,
            base_count.max(1),
        )?;
        let progression = effective_rollout
            .progression
            .clone()
            .unwrap_or(RolloutProgression::Auto);
        let replacement = effective_rollout
            .replacement
            .clone()
            .unwrap_or(RolloutReplacement::DestroyFirst);
        // Ordered slot replacement only applies when the effective config
        // asks for it AND the group actually has a slot pool (validation
        // guarantees this for specs; strategy plugins could disagree, in
        // which case the slots block is ignored for slot-less groups).
        let sequential_slots = match (effective_rollout.slots.as_ref(), slot_task) {
            (Some(slots), Some((task_name, count))) if slots.is_sequential() => {
                Some(SlotSequentialPlan {
                    task_name,
                    count,
                    update_from: slots.update_from.unwrap_or(0),
                })
            }
            _ => None,
        };
        // Sequential groups define their own one-slot-per-step progression
        // (taking precedence over any plugin-injected step mode). A gang
        // group replaces in one step whatever the config says: its replicas
        // place as one unit, so a step that updates some of them is a broken
        // gang, not a canary. Validation rejects a multi-step config for a
        // gang group; this covers the steps a strategy plugin injects after
        // validation, and a scale-out that grows a `countEachStep` rollout
        // past one step (docs/gang-scheduling.md).
        let step_fractions = match sequential_slots.as_ref() {
            Some(slots) => sequential_step_fractions(slots.count, slots.update_from),
            None if gang_group => vec![1.0],
            None => step_fractions_for_rollout(&effective_rollout, base_count.max(1)),
        };
        plans.push(GroupPlan {
            name: group,
            config: effective_rollout,
            progression,
            replacement,
            step_fractions,
            tasks,
            sequential_slots,
        });
    }

    if plans.is_empty() {
        let config = rollout_strategy::resolve_rollout_strategy(
            rollout_config_for_group(to_config, "default"),
            "default",
            1,
        )?;
        let progression = config
            .progression
            .clone()
            .unwrap_or(RolloutProgression::Auto);
        let replacement = config
            .replacement
            .clone()
            .unwrap_or(RolloutReplacement::DestroyFirst);

        plans.push(GroupPlan {
            name: "default".to_string(),
            config: config.clone(),
            progression,
            replacement,
            step_fractions: step_fractions_for_rollout(&config, 1),
            tasks: Vec::new(),
            sequential_slots: None,
        });
    }

    if let Some(from_config) = from_config {
        for plan in &plans {
            for task in &plan.tasks {
                if task.old_task_id.is_some() && task.new_task_id.is_none() {
                    continue;
                }
                if task.new_task_id.is_none() {
                    bail!(
                        "Rollout group {} has task {} without destination task",
                        plan.name,
                        task.name
                    );
                }
            }
        }

        let from_names = from_config
            .tasks
            .iter()
            .map(|task| task.name.clone())
            .collect::<HashSet<_>>();
        let to_names = to_config
            .tasks
            .iter()
            .map(|task| task.name.clone())
            .collect::<HashSet<_>>();

        if from_names != to_names {
            // Task additions/removals are supported; this warning-level branch is
            // kept explicit so callers can reason about changed rollout shape.
        }
    }

    Ok(plans)
}

fn ensure_runtime_groups(runtime: &mut RolloutRuntime, plans: &[GroupPlan]) {
    for plan in plans {
        runtime.groups.entry(plan.name.clone()).or_default();
    }
}

fn rollout_config_for_group(config: &JobConfig, group: &str) -> RolloutConfig {
    config
        .rollout_config_for_group(group)
        .or_else(|| config.rollout.clone())
        .unwrap_or_default()
}

fn parse_rollout_envelope(raw: &str) -> R<RolloutEnvelope> {
    if raw.trim().is_empty() {
        return Ok(RolloutEnvelope::default());
    }

    let envelope = serde_json::from_str::<RolloutEnvelope>(raw)
        .with_context(|| "invalid rollout config_json payload")?;
    Ok(envelope)
}

fn count_allocations_by_task(store: &Store, job_config_id: &str) -> R<HashMap<i64, u32>> {
    let mut counts = HashMap::new();
    for allocation in store.allocs_for_job_config(job_config_id)? {
        *counts.entry(allocation.task_config_id).or_default() += 1;
    }
    Ok(counts)
}

fn build_tick_inputs(store: &Store, rollout: &Rollout, plans: &[GroupPlan]) -> R<TickInputs> {
    let new_allocations = store.allocs_for_job_config(&rollout.to_job_config_id)?;
    let mut allocations_by_task: HashMap<i64, Vec<i64>> = HashMap::new();
    let mut new_counts_by_task: HashMap<i64, u32> = HashMap::new();
    let should_load_health = plans.iter().any(|plan| {
        !plan.config.ignore_health_checks.unwrap_or(false)
            && plan.tasks.iter().any(|task| task.new_task_id.is_some())
    });
    let mut allocation_ids = if should_load_health {
        Vec::with_capacity(new_allocations.len())
    } else {
        Vec::new()
    };

    for allocation in &new_allocations {
        allocations_by_task
            .entry(allocation.task_config_id)
            .or_default()
            .push(allocation.id);
        *new_counts_by_task
            .entry(allocation.task_config_id)
            .or_default() += 1;
        if should_load_health {
            allocation_ids.push(allocation.id);
        }
    }

    let mut new_task_ids = plans
        .iter()
        .flat_map(|plan| plan.tasks.iter().filter_map(|task| task.new_task_id))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    new_task_ids.sort_unstable();

    let allocation_health = if should_load_health {
        store.allocation_health_for_allocations(&allocation_ids)?
    } else {
        HashMap::new()
    };

    Ok(TickInputs {
        new_allocations,
        allocations_by_task,
        new_counts_by_task,
        new_task_ids,
        allocation_health,
    })
}

/// Step fractions for an ordered slot rollout (`rollout.slots.order:
/// 'sequential'`): one step per slot being updated, highest slot first,
/// stopping at the staged floor (`update_from`). With `count = 4,
/// update_from = 2` this yields `[0.25, 0.5]` — two steps (slots 3 then 2),
/// deliberately NOT ending at 1.0 because slots 0..2 keep their current
/// allocations. An out-of-range floor (`update_from >= count`, rejected at
/// validation but reachable through strategy plugins) yields no steps: the
/// group completes without touching anything.
pub(crate) fn sequential_step_fractions(count: u32, update_from: u32) -> Vec<f64> {
    if count == 0 {
        return vec![1.0];
    }
    let updatable = count.saturating_sub(update_from);
    (1..=updatable)
        .map(|updated| (updated as f64) / (count as f64))
        .collect()
}

pub(crate) fn step_fractions_for_rollout(config: &RolloutConfig, base_count: u32) -> Vec<f64> {
    if base_count == 0 {
        return vec![1.0];
    }

    let mut steps = Vec::new();
    if let Some(percent_each) = config.percent_each_step {
        let mut current = percent_each;
        while current < 100 {
            steps.push((current as f64) / 100.0);
            current = current.saturating_add(percent_each);
        }
        steps.push(1.0);
    } else if let Some(count_each) = config.count_each_step {
        let mut current = count_each;
        while current < base_count {
            steps.push((current as f64) / (base_count as f64));
            current = current.saturating_add(count_each);
        }
        steps.push(1.0);
    } else if let Some(percent_steps) = config.percent_steps.as_ref() {
        for step in percent_steps {
            steps.push((*step as f64) / 100.0);
        }
    } else if let Some(count_steps) = config.count_steps.as_ref() {
        let mut cumulative = 0_u32;
        for step in count_steps {
            cumulative = cumulative.saturating_add(*step).min(base_count);
            steps.push((cumulative as f64) / (base_count as f64));
        }
        if cumulative < base_count {
            steps.push(1.0);
        }
    } else {
        steps.push(1.0);
    }

    let mut deduped = Vec::new();
    for step in steps {
        let clamped = step.clamp(0.0, 1.0);
        if deduped
            .last()
            .copied()
            .is_none_or(|previous: f64| (previous - clamped).abs() > f64::EPSILON)
        {
            deduped.push(clamped);
        }
    }

    if deduped.last().copied() != Some(1.0) {
        deduped.push(1.0);
    }

    deduped
}

fn parse_optional_ts(raw: Option<&str>) -> R<Option<OffsetDateTime>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    if raw.trim().is_empty() {
        return Ok(None);
    }

    let parsed = OffsetDateTime::parse(raw, &Rfc3339)
        .with_context(|| format!("invalid RFC3339 timestamp {raw}"))?;
    Ok(Some(parsed))
}

fn format_ts(ts: OffsetDateTime) -> R<String> {
    ts.format(&Rfc3339)
        .with_context(|| "failed to format RFC3339 timestamp")
}

fn is_terminal_rollout_status(status: &str) -> bool {
    matches!(
        status,
        "completed" | "failed" | "cancelled" | "completed_incomplete"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::store::Store;
    use crate::structs::{config::ManagerConfigFull, JobConfig, NodeMsg, Submission, TaskConfig};
    use serde_json::json;
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-rollout-tests-")
            .tempdir_in(crate::test_scratch::root())
            .context("error creating temp store directory")?
            .keep();
        let config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("error parsing temp store path")?
                .to_string(),
            ..Default::default()
        };
        let store = Store::start_storage(&config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    fn insert_node(store: &Store, id: &str) -> R<()> {
        let node = NodeMsg {
            id: id.to_string(),
            pool: "default".into(),
            status: "active".into(),
            availability: "available".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: json!({
                "memory_total": "8589934592",
                "cpus": {
                    "cpu-1": { "frequency": "2400" }
                }
            })
            .to_string(),
            dynamic_attrs: "{}".into(),
            health_checks: json!({ "worker": "pass" }).to_string(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
        };
        store.insert_node(node)?;
        Ok(())
    }

    fn submission() -> Submission {
        Submission {
            path: "job.ncl".into(),
            content: "{}".into(),
            is_primary: true,
        }
    }

    #[test]
    fn step_fractions_percent_each() {
        let config = RolloutConfig {
            percent_each_step: Some(30),
            ..Default::default()
        };
        let steps = step_fractions_for_rollout(&config, 10);
        assert_eq!(steps, vec![0.3, 0.6, 0.9, 1.0]);
    }

    #[test]
    fn step_fractions_count_each() {
        let config = RolloutConfig {
            count_each_step: Some(2),
            ..Default::default()
        };
        let steps = step_fractions_for_rollout(&config, 5);
        assert_eq!(steps, vec![0.4, 0.8, 1.0]);
    }

    #[test]
    fn step_fractions_count_steps_adds_implicit_rest() {
        let config = RolloutConfig {
            count_steps: Some(vec![1, 2]),
            ..Default::default()
        };
        let steps = step_fractions_for_rollout(&config, 10);
        assert_eq!(steps, vec![0.1, 0.3, 1.0]);
    }

    #[test]
    fn step_fractions_percent_steps_respected() {
        let config = RolloutConfig {
            percent_steps: Some(vec![10, 40, 100]),
            ..Default::default()
        };
        let steps = step_fractions_for_rollout(&config, 7);
        assert_eq!(steps, vec![0.1, 0.4, 1.0]);
    }

    #[test]
    fn step_fractions_count_steps_exact_total_has_no_extra() {
        let config = RolloutConfig {
            count_steps: Some(vec![2, 3]),
            ..Default::default()
        };
        let steps = step_fractions_for_rollout(&config, 5);
        assert_eq!(steps, vec![0.4, 1.0]);
    }

    #[test]
    fn step_fractions_defaults_to_single_full_step() {
        let config = RolloutConfig::default();
        let steps = step_fractions_for_rollout(&config, 3);
        assert_eq!(steps, vec![1.0]);
    }

    #[test]
    fn sequential_step_fractions_one_step_per_slot() {
        // count 4, no floor: four steps (slots 3, 2, 1, 0).
        assert_eq!(sequential_step_fractions(4, 0), vec![0.25, 0.5, 0.75, 1.0]);
        // count 4, updateFrom 2: two steps (slots 3, 2); deliberately does
        // NOT end at 1.0 — slots 0..2 stay on their current allocations.
        assert_eq!(sequential_step_fractions(4, 2), vec![0.25, 0.5]);
        // Defensive: an out-of-range floor yields no steps (instant
        // completion, nothing touched) rather than a panic.
        assert!(sequential_step_fractions(3, 3).is_empty());
        assert!(sequential_step_fractions(3, 7).is_empty());
    }

    #[test]
    fn build_step_targets_sequential_replaces_one_slot_per_step() {
        let task = GroupTaskPlan {
            name: "db".to_string(),
            lifecycle: TaskLifecycle::Service,
            old_task_id: Some(1),
            new_task_id: Some(2),
            desired_new: 4,
            current_new: 0,
            current_old_allocs: Vec::new(),
        };
        let plan = GroupPlan {
            name: "data".to_string(),
            config: RolloutConfig::default(),
            progression: RolloutProgression::Auto,
            replacement: RolloutReplacement::DestroyFirst,
            step_fractions: sequential_step_fractions(4, 2),
            tasks: vec![task],
            sequential_slots: Some(SlotSequentialPlan {
                task_name: "db".to_string(),
                count: 4,
                update_from: 2,
            }),
        };

        // Step 0 replaces slot 3 only; step 1 reaches the staged floor and
        // keeps slots 0..2 as old allocations (target_old = update_from).
        let step0 = build_step_targets(&plan, 0.25, 0);
        assert_eq!(step0.per_task_target_new.get("db"), Some(&1));
        assert_eq!(step0.per_task_target_old.get("db"), Some(&3));
        let step1 = build_step_targets(&plan, 0.5, 1);
        assert_eq!(step1.per_task_target_new.get("db"), Some(&2));
        assert_eq!(step1.per_task_target_old.get("db"), Some(&2));
    }

    #[test]
    fn slot_for_step_counts_down_from_highest() {
        let slots = SlotSequentialPlan {
            task_name: "db".to_string(),
            count: 4,
            update_from: 2,
        };
        assert_eq!(slots.slot_for_step(0), 3);
        assert_eq!(slots.slot_for_step(1), 2);
        // Clamped at the staged floor even for out-of-range steps.
        assert_eq!(slots.slot_for_step(5), 2);
    }

    /// A blocked rollout backs off instead of re-ticking every 2s forever
    /// (the spot check saw 500+ tick events for one stuck rollout), and never
    /// sleeps past its capacity timeout.
    #[test]
    fn blocked_rollouts_back_off_up_to_thirty_seconds() {
        let secs = time::Duration::seconds;
        assert_eq!(placement_wait_delay(secs(0), None), Duration::from_secs(2));
        assert_eq!(placement_wait_delay(secs(3), None), Duration::from_secs(2));
        assert_eq!(
            placement_wait_delay(secs(20), None),
            Duration::from_secs(10)
        );
        assert_eq!(
            placement_wait_delay(secs(600), None),
            Duration::from_secs(30)
        );
        assert_eq!(
            placement_wait_delay(secs(600), Some(secs(5))),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn step_operation_order_matches_replacement_mode() {
        assert_eq!(
            step_operation_order(&RolloutReplacement::CreateFirst),
            [
                StepOperation::PlaceNew,
                StepOperation::WaitForHealthy,
                StepOperation::RetireOld
            ]
        );
        assert_eq!(
            step_operation_order(&RolloutReplacement::DestroyFirst),
            [
                StepOperation::RetireOld,
                StepOperation::PlaceNew,
                StepOperation::WaitForHealthy
            ]
        );
    }

    #[test]
    fn build_step_targets_marks_required_actions() {
        let plan = GroupPlan {
            name: "web".to_string(),
            config: RolloutConfig::default(),
            progression: RolloutProgression::Auto,
            replacement: RolloutReplacement::CreateFirst,
            step_fractions: vec![0.5],
            tasks: vec![GroupTaskPlan {
                name: "api".to_string(),
                lifecycle: TaskLifecycle::Service,
                old_task_id: Some(1),
                new_task_id: Some(2),
                desired_new: 4,
                current_new: 1,
                current_old_allocs: vec![
                    Allocation {
                        id: 1,
                        status: "running".to_string(),
                        resources: json!({}),
                        node_id: "node-a".to_string(),
                        job_config_id: "job-1".to_string(),
                        task_config_id: 1,
                        job_id: "job".to_string(),
                        instance_id: None,
                        network: None,
                    },
                    Allocation {
                        id: 2,
                        status: "running".to_string(),
                        resources: json!({}),
                        node_id: "node-b".to_string(),
                        job_config_id: "job-1".to_string(),
                        task_config_id: 1,
                        job_id: "job".to_string(),
                        instance_id: None,
                        network: None,
                    },
                    Allocation {
                        id: 3,
                        status: "running".to_string(),
                        resources: json!({}),
                        node_id: "node-c".to_string(),
                        job_config_id: "job-1".to_string(),
                        task_config_id: 1,
                        job_id: "job".to_string(),
                        instance_id: None,
                        network: None,
                    },
                ],
            }],
            sequential_slots: None,
        };

        let targets = build_step_targets(&plan, 0.5, 0);
        assert_eq!(targets.per_task_target_new.get("api"), Some(&2));
        assert_eq!(targets.per_task_target_old.get("api"), Some(&2));
        assert!(targets.needs_new_placement);
        assert!(targets.needs_old_retire);
    }

    #[test]
    fn in_place_exclusions_prefer_old_nodes() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        insert_node(&store, "node-b")?;

        let v1 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "in-place-demo".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "version": 1 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v1, &[submission()])?;
        let v1_id = store.get_job_config(&v1.name)?.id();
        let v1_task_id = store.get_task_configs_for_job_config(&v1_id)?[0].0;
        let _old_alloc = store.place_alloc("node-a", v1_task_id)?;

        let v2 = JobConfig {
            priority: Default::default(),
            rollout: Some(RolloutConfig {
                in_place: Some(true),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "in-place-demo".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "version": 2 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v2, &[submission()])?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        let rollout = store.create_rollout(
            "in-place-demo",
            Some(&v1_id),
            &v2_id,
            "running",
            Some("default"),
            Some("auto"),
            Some("createFirst"),
            Some(true),
            "{}",
        )?;
        let v2_config = store.get_job_config_by_id(&v2_id)?;
        let v1_config = store.get_job_config_by_id(&v1_id)?;
        let plans = build_group_plans(&v2_config, Some(&v1_config), &store, &rollout)?;
        let web = plans
            .iter()
            .find(|plan| plan.name == "web")
            .context("missing web plan")?;
        let target = HashMap::from([(String::from("api"), 1_u32)]);

        let new_allocations = store.allocs_for_job_config(&rollout.to_job_config_id)?;
        let excluded = build_in_place_exclusions(&store, web, &target, &new_allocations)?
            .context("expected in-place exclusions")?;
        assert!(excluded.contains("node-b"));
        assert!(!excluded.contains("node-a"));
        Ok(())
    }

    #[test]
    fn in_place_exclusions_clear_when_replacement_already_on_old_node() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        insert_node(&store, "node-b")?;

        let v1 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "in-place-satisfied".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "version": 1 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v1, &[submission()])?;
        let v1_id = store.get_job_config(&v1.name)?.id();
        let v1_task_id = store.get_task_configs_for_job_config(&v1_id)?[0].0;
        let _old_alloc = store.place_alloc("node-a", v1_task_id)?;

        let v2 = JobConfig {
            priority: Default::default(),
            rollout: Some(RolloutConfig {
                in_place: Some(true),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "in-place-satisfied".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "version": 2 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v2, &[submission()])?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        let v2_task_id = store.get_task_configs_for_job_config(&v2_id)?[0].0;
        let _new_alloc = store.place_alloc("node-a", v2_task_id)?;

        let rollout = store.create_rollout(
            "in-place-satisfied",
            Some(&v1_id),
            &v2_id,
            "running",
            Some("default"),
            Some("auto"),
            Some("createFirst"),
            Some(true),
            "{}",
        )?;
        let v2_config = store.get_job_config_by_id(&v2_id)?;
        let v1_config = store.get_job_config_by_id(&v1_id)?;
        let plans = build_group_plans(&v2_config, Some(&v1_config), &store, &rollout)?;
        let web = plans
            .iter()
            .find(|plan| plan.name == "web")
            .context("missing web plan")?;
        let target = HashMap::from([(String::from("api"), 1_u32)]);

        let new_allocations = store.allocs_for_job_config(&rollout.to_job_config_id)?;
        let excluded = build_in_place_exclusions(&store, web, &target, &new_allocations)?;
        assert!(excluded.is_none());
        Ok(())
    }

    #[test]
    fn evaluate_group_health_requires_healthy_snapshots() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "health-gate".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = store.get_task_configs_for_job_config(&job_config_id)?[0].0;
        let allocation = store.place_alloc("node-a", task_id)?;
        let mut state = GroupRuntime {
            active_step_started_at: Some(format_ts(OffsetDateTime::now_utc())?),
            ..GroupRuntime::default()
        };
        let plan = GroupPlan {
            name: "web".to_string(),
            config: RolloutConfig::default(),
            progression: RolloutProgression::Auto,
            replacement: RolloutReplacement::CreateFirst,
            step_fractions: vec![1.0],
            tasks: vec![GroupTaskPlan {
                name: "api".to_string(),
                lifecycle: TaskLifecycle::Service,
                old_task_id: None,
                new_task_id: Some(task_id),
                desired_new: 1,
                current_new: 1,
                current_old_allocs: Vec::new(),
            }],
            sequential_slots: None,
        };
        let targets = HashMap::from([(String::from("api"), 1_u32)]);
        let allocations_by_task = HashMap::from([(task_id, vec![allocation.id])]);
        let now = OffsetDateTime::now_utc();
        let mut allocation_health = store.allocation_health_for_allocations(&[allocation.id])?;

        assert!(matches!(
            evaluate_group_health(
                &plan,
                &mut state,
                now,
                &targets,
                &allocations_by_task,
                &allocation_health
            )?,
            HealthState::Waiting
        ));

        store.upsert_allocation_health_snapshots(
            "node-a",
            &[crate::structs::AllocationHealth {
                allocation_id: allocation.id,
                healthy: true,
                status: "healthy".into(),
                artifact_states: None,
            }],
        )?;
        allocation_health = store.allocation_health_for_allocations(&[allocation.id])?;
        let status = evaluate_group_health(
            &plan,
            &mut state,
            now,
            &targets,
            &allocations_by_task,
            &allocation_health,
        )?;
        assert!(matches!(status, HealthState::Healthy));

        Ok(())
    }

    #[test]
    fn manual_advance_records_rollout_event() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;

        let v1 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "advance-events".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v1, &[submission()])?;
        let v1_id = store.get_job_config(&v1.name)?.id();

        let v2 = JobConfig {
            priority: Default::default(),
            rollout: Some(RolloutConfig {
                progression: Some(RolloutProgression::Manual),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "advance-events".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "version": 2 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v2, &[submission()])?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        let rollout = store.create_rollout(
            &v2.name,
            Some(&v1_id),
            &v2_id,
            "waiting_manual_start",
            Some("default"),
            Some("manual"),
            Some("createFirst"),
            Some(false),
            "{}",
        )?;

        let _ = advance_rollout(&store, rollout.id, &[], 50)?;
        let events = store.list_rollout_events(rollout.id, None)?;
        assert!(events
            .iter()
            .any(|event| event.message == "Manual rollout advance queued"));

        Ok(())
    }

    /// The spot check's `05-dag.ncl` shape, cut down: two batch steps and a
    /// service that serves what they built, all in one group.
    fn dag_job(name: &str, run: &str) -> JobConfig {
        let after = |task: &str| {
            vec![crate::structs::TaskDependency {
                task: task.into(),
                condition: crate::structs::TaskDependencyCondition::Success,
            }]
        };
        JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks: vec![
                TaskConfig {
                    group: "pipeline".into(),
                    lifecycle: TaskLifecycle::Batch,
                    config: json!({ "run": run }),
                    ..TaskConfig::new("fetch", "noop")
                },
                TaskConfig {
                    group: "pipeline".into(),
                    lifecycle: TaskLifecycle::Batch,
                    depends_on: after("fetch"),
                    config: json!({ "run": run }),
                    ..TaskConfig::new("report", "noop")
                },
                TaskConfig {
                    group: "pipeline".into(),
                    lifecycle: TaskLifecycle::Service,
                    depends_on: after("report"),
                    config: json!({ "run": run }),
                    ..TaskConfig::new("serve", "noop")
                },
            ],
            version: None,
        }
    }

    fn tick(store: &Store, job_id: &str, job_config_id: &str) -> R<String> {
        handle_rollout_tick(
            &Eval {
                id: "e-tick".into(),
                status: "pending".into(),
                kind: "rollout_tick".into(),
                job_id: job_id.into(),
                job_config_id: job_config_id.into(),
                node_name: String::new(),
                priority: 50,
            },
            store,
        )
    }

    /// The one allocation of `task` on `job_config_id`, if it is placed.
    fn alloc_of(store: &Store, job_config_id: &str, task: &str) -> R<Option<Allocation>> {
        let task_id = store
            .get_task_configs_for_job_config(job_config_id)?
            .into_iter()
            .find(|(_, config)| config.name == task)
            .map(|(id, _)| id)
            .with_context(|| format!("no task {task}"))?;
        Ok(store
            .allocs_for_job_config(job_config_id)?
            .into_iter()
            .find(|allocation| allocation.task_config_id == task_id))
    }

    /// What a worker reports when a task exits 0: the terminal status, and
    /// the health snapshot the supervisor records for the exit.
    fn report_exit(store: &Store, allocation: &Allocation) -> R<()> {
        store.complete_allocation(allocation.id, true)?;
        store.upsert_allocation_health_snapshots(
            &allocation.node_id,
            &[crate::structs::AllocationHealth {
                allocation_id: allocation.id,
                healthy: true,
                status: "exited with status 0".into(),
                artifact_states: None,
            }],
        )?;
        Ok(())
    }

    fn create_default_rollout(store: &Store, job: &str, from: &str, to: &str) -> R<Rollout> {
        store.create_rollout(job, Some(from), to, "pending", None, None, None, None, "{}")
    }

    /// Regression (spot check, rollout 6 stuck for 10+ minutes on "waiting
    /// for batch retire window"): resubmitting a DAG job whose old batch
    /// steps had all finished never progressed. The finished allocations
    /// still counted as old allocations to retire, and with `batchKillAfter`
    /// unset the batch retire window refused to retire any batch
    /// allocation. A finished allocation runs nothing, so it is retired at
    /// once, and the new version's DAG starts from its roots.
    #[test]
    fn rollout_over_finished_batch_steps_retires_them_and_runs_the_new_dag() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        let scheduler = Scheduler::new(&store)?;

        // v1 runs its batch work to the end. Its `serve` is not placed yet:
        // the resubmit superseded v1 first.
        let v1 = dag_job("dag-rerun", "1");
        store.handle_job_submit(&v1, &[submission()])?;
        let v1_id = store.get_job_config(&v1.name)?.id();
        let fetch = scheduler.plan_and_apply(&v1_id, PlanMode::Fresh)?;
        assert_eq!(fetch.allocations.len(), 1, "only fetch is ready");
        report_exit(&store, &fetch.allocations[0])?;
        let report = scheduler.plan_and_apply(&v1_id, PlanMode::Reschedule)?;
        assert_eq!(report.allocations.len(), 1, "report follows fetch");
        report_exit(&store, &report.allocations[0])?;

        // v2: a new `run`, rolled out with the defaults (destroyFirst, one
        // step, no batchKillAfter).
        let v2 = dag_job("dag-rerun", "2");
        store.handle_job_submit(&v2, &[submission()])?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        let rollout = create_default_rollout(&store, &v2.name, &v1_id, &v2_id)?;

        let first = tick(&store, &v2.name, &v2_id)?;
        assert!(
            first.contains("retired 2 old allocations"),
            "finished batch steps retire at once: {first}"
        );
        assert!(store.allocs_for_job_config(&v1_id)?.is_empty());

        // The new version starts its DAG from the top: fetch, not report.
        let second = tick(&store, &v2.name, &v2_id)?;
        assert!(second.contains("placed 1 replacement"), "{second}");
        let new_fetch = alloc_of(&store, &v2_id, "fetch")?.context("v2 fetch placed")?;
        assert!(alloc_of(&store, &v2_id, "report")?.is_none());

        // Each exit releases the next step through the rollout.
        report_exit(&store, &new_fetch)?;
        tick(&store, &v2.name, &v2_id)?;
        let new_report = alloc_of(&store, &v2_id, "report")?.context("v2 report placed")?;
        report_exit(&store, &new_report)?;
        // A config under rollout routes its dependents' release through the
        // rollout (a `rollout_tick`, not a `job_reschedule`), so the tick
        // must place them: a stuck rollout used to freeze them too.
        let rollout_ticks = |store: &Store| -> R<usize> {
            Ok(store
                .get_evals()?
                .iter()
                .filter(|eval| eval.kind == "rollout_tick")
                .count())
        };
        let ticks_before = rollout_ticks(&store)?;
        assert!(store.queue_dependency_progress(&new_report)?);
        assert_eq!(rollout_ticks(&store)?, ticks_before + 1);
        tick(&store, &v2.name, &v2_id)?;

        // The service that depends on the finished batch step is placed.
        let serve = alloc_of(&store, &v2_id, "serve")?.context("v2 serve placed")?;
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[crate::structs::AllocationHealth {
                allocation_id: serve.id,
                healthy: true,
                status: "healthy".into(),
                artifact_states: None,
            }],
        )?;
        let advanced = tick(&store, &v2.name, &v2_id)?;
        assert!(advanced.contains("advanced to step 1"), "{advanced}");
        let last = tick(&store, &v2.name, &v2_id)?;
        assert!(last.contains("completed"), "{last}");
        let rollout = store.get_rollout(rollout.id)?.context("rollout exists")?;
        assert_eq!(rollout.status, "completed");
        Ok(())
    }

    /// The batch retire window still protects batch work that is running:
    /// a finished old step is retired, a running one is left to finish.
    #[test]
    fn rollout_waits_for_running_batch_steps_but_not_finished_ones() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        let scheduler = Scheduler::new(&store)?;

        let v1 = dag_job("dag-midway", "1");
        store.handle_job_submit(&v1, &[submission()])?;
        let v1_id = store.get_job_config(&v1.name)?.id();
        let fetch = scheduler.plan_and_apply(&v1_id, PlanMode::Fresh)?;
        report_exit(&store, &fetch.allocations[0])?;
        // `report` is placed and still running when v2 lands.
        let report = scheduler.plan_and_apply(&v1_id, PlanMode::Reschedule)?;
        let running_report = report.allocations[0].clone();

        let v2 = dag_job("dag-midway", "2");
        store.handle_job_submit(&v2, &[submission()])?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        create_default_rollout(&store, &v2.name, &v1_id, &v2_id)?;

        let first = tick(&store, &v2.name, &v2_id)?;
        assert!(first.contains("retired 1 old allocations"), "{first}");
        let left = store.allocs_for_job_config(&v1_id)?;
        assert_eq!(
            left.iter().map(|alloc| alloc.id).collect::<Vec<_>>(),
            vec![running_report.id],
            "only the running batch step is left"
        );
        let second = tick(&store, &v2.name, &v2_id)?;
        assert!(
            second.contains("waiting for batch retire window"),
            "{second}"
        );

        // Once it finishes, the rollout retires it and moves on.
        report_exit(&store, &running_report)?;
        let third = tick(&store, &v2.name, &v2_id)?;
        assert!(third.contains("retired 1 old allocations"), "{third}");
        assert!(store.allocs_for_job_config(&v1_id)?.is_empty());
        Ok(())
    }

    fn pinned_db_job(command: &str) -> JobConfig {
        use crate::structs::{InstanceMode, StatefulConfig, StatefulStorageConfig};
        JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "fullstack".into(),
            tasks: vec![TaskConfig {
                group: "db".into(),
                resources: json!({ "memory": 512 }),
                config: json!({ "command": command }),
                stateful: Some(StatefulConfig {
                    mode: InstanceMode::Slot,
                    require_id: false,
                    initial_instances: None,
                    on_lost: None,
                    storage: Some(StatefulStorageConfig {
                        driver: Some("local-dir".into()),
                        mode: None,
                        params: Some(json!({ "size": "2G" })),
                        path: "/var/lib/postgresql/data".into(),
                        retain_on_scale_in: None,
                        retain_on_stop: None,
                    }),
                    envelope: None,
                    promote: None,
                    placement: None,
                    fence_after: None,
                }),
                ..TaskConfig::new("postgres", "noop")
            }],
            version: None,
        }
    }

    fn fail_allocation(store: &Store, allocation: &Allocation) -> R<()> {
        store
            .remove_failed_allocation(
                allocation.id,
                StopCause::Unknown,
                None,
                crate::storage::store::FailedMove {
                    policy: crate::storage::store::MovePolicy {
                        move_delay_ms: 30_000,
                        move_count: None,
                        move_period_ms: 3_600_000,
                    },
                    reason: "exit status 1".into(),
                },
            )?
            .context("removed")?;
        Ok(())
    }

    /// `insert_node` reporting `free_gib` of free disk to the disk-fit check.
    fn insert_node_with_disk(store: &Store, id: &str, free_gib: u64) -> R<()> {
        insert_node(store, id)?;
        let mut node = NodeMsg::from(store.get_node(id)?);
        let mut fingerprint: serde_json::Value = serde_json::from_str(&node.fingerprint_attrs)?;
        fingerprint["disk_available_space"] = json!((free_gib << 30).to_string());
        node.fingerprint_attrs = fingerprint.to_string();
        store.insert_node(node)?;
        Ok(())
    }

    /// Spot check 2026-10-12, rollout 8 of `fullstack`: the slot instance
    /// `fullstack-db-0` (local-dir storage, `size = "2G"`) failed on the node
    /// holding its volume, whose free disk had since dropped under 2 GiB.
    /// Every tick said "healthy but has no room for it right now" on a node
    /// 12% reserved: the disk check charged the volume's size again although
    /// it already exists there. The replacement waits out its backoff, then
    /// goes back to its node, whether the instance failed before the rollout
    /// or during it, and whether or not the other node has room.
    #[test]
    fn rollout_returns_a_failed_pinned_slot_instance_to_its_node() -> R<()> {
        for fail_before_rollout in [true, false] {
            for node_b_full in [false, true] {
                let case =
                    format!("fail before rollout {fail_before_rollout}, node-b full {node_b_full}");
                let store = setup_store()?;
                insert_node_with_disk(&store, "node-a", 50)?;
                let scheduler = Scheduler::new(&store)?;
                let v1 = pinned_db_job("v1");
                store.handle_job_submit(&v1, &[submission()])?;
                let v1_id = store.get_job_config("fullstack")?.id();
                let placed = scheduler.plan_and_apply(&v1_id, PlanMode::Fresh)?;
                let v1_alloc = placed.allocations[0].clone();
                let handle = store
                    .get_instance("fullstack-db-0")?
                    .and_then(|instance| instance.storage_handle)
                    .context("storage")?;
                store.record_volume_mounted(
                    handle.split_once('/').context("handle")?.1,
                    "node-a",
                    "/v",
                    true,
                    None,
                    &Default::default(),
                )?;
                insert_node_with_disk(&store, "node-a", 1)?;
                insert_node_with_disk(&store, "node-b", 50)?;
                if node_b_full {
                    let filler = JobConfig {
                        priority: Default::default(),
                        rollout: None,
                        rollout_by_group: Default::default(),
                        name: "filler".into(),
                        tasks: vec![TaskConfig {
                            resources: json!({ "memory": 7800 }),
                            ..TaskConfig::new("fill", "noop")
                        }],
                        version: None,
                    };
                    store.handle_job_submit(&filler, &[submission()])?;
                    let filler_id = store.get_job_config("filler")?.id();
                    let filled = scheduler.plan_and_apply(&filler_id, PlanMode::Fresh)?;
                    assert_eq!(filled.allocations[0].node_id, "node-b");
                }
                if fail_before_rollout {
                    fail_allocation(&store, &v1_alloc)?;
                }
                let v2 = pinned_db_job("v2");
                store.handle_job_submit(&v2, &[submission()])?;
                let v2_id = store.get_job_config("fullstack")?.id();
                create_default_rollout(&store, "fullstack", &v1_id, &v2_id)?;
                let mut log = Vec::new();
                if !fail_before_rollout {
                    // The rollout replaces v1 on node-a, then v2 fails there.
                    for _ in 0..3 {
                        log.push(tick(&store, "fullstack", &v2_id)?);
                        if !store.allocs_for_job_config(&v2_id)?.is_empty() {
                            break;
                        }
                    }
                    let v2_alloc = store
                        .allocs_for_job_config(&v2_id)?
                        .pop()
                        .with_context(|| format!("{case}: {log:?}"))?;
                    assert_eq!(v2_alloc.node_id, "node-a", "{case}");
                    fail_allocation(&store, &v2_alloc)?;
                    // Inside the backoff the tick waits instead of placing.
                    let waiting = tick(&store, "fullstack", &v2_id)?;
                    assert!(
                        waiting.contains("waiting for placement capacity"),
                        "{case}: {waiting}"
                    );
                    assert!(store.allocs_for_job_config(&v2_id)?.is_empty(), "{case}");
                }
                store
                    .write_connection()?
                    .execute("UPDATE reschedule_hold SET not_before_unix = 0;", [])?;
                log.push(tick(&store, "fullstack", &v2_id)?);
                let placed = store.allocs_for_job_config(&v2_id)?;
                assert_eq!(placed.len(), 1, "{case}: {log:#?}");
                assert_eq!(placed[0].node_id, "node-a", "{case}");
                assert_eq!(placed[0].instance_id.as_deref(), Some("fullstack-db-0"));
            }
        }
        Ok(())
    }
}
