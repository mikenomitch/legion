use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::manager::dispatcher::dispatch_allocations;
use crate::manager::rollout_strategy;
use crate::manager::scheduler::{PlanMode, Scheduler};
use crate::prelude::{bail, debug, instrument, warn, Context, R};
use crate::storage::store::{AllocationHealthSnapshot, Store};
use crate::structs::{
    Allocation, Eval, JobConfig, Rollout, RolloutCapacityTimeoutAction, RolloutConfig,
    RolloutDeadlineAction, RolloutProgression, RolloutReplacement, TaskConfig, TaskLifecycle,
};

const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(2);

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
    let step_targets = build_step_targets(plan, step_fraction);
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

    state.step_index += 1;
    state.active_step_started_at = None;
    state.reached_target_at = None;
    state.blocked_since = None;
    state.last_step_completed_at = Some(format_ts(now)?);

    Ok(GroupAction::Mutated {
        delay: Some(Duration::from_millis(0)),
        message: format!("Group {} advanced to step {}", plan.name, state.step_index),
    })
}

fn build_step_targets(plan: &GroupPlan, step_fraction: f64) -> StepTargets {
    let mut targets = StepTargets::default();
    for task in &plan.tasks {
        let target_new = ((task.desired_new as f64) * step_fraction)
            .ceil()
            .min(task.desired_new as f64) as u32;
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
                    Ok(Some(GroupAction::Wait {
                        delay: wait,
                        message: format!("Group {} waiting for healthy window", plan.name),
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
                return Ok(Some(GroupAction::Mutated {
                    delay: plan.config.allocation_stagger,
                    message: format!(
                        "Group {} retired {} old allocations",
                        plan.name, retire.removed
                    ),
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
                        store.update_task_statuses_for_allocations(&outcome.allocations)?;
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
                store.update_task_statuses_for_allocations(&outcome.allocations)?;
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
        Err(err) => on_placement_wait(
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
        ),
    }
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

    if let Some(timeout) = plan.config.capacity_block_timeout {
        let elapsed = now - blocked_since;
        if elapsed >= time::Duration::try_from(timeout)? {
            warn!(
                rollout_id = rollout.id,
                group = %plan.name,
                timeout_secs = timeout.as_secs(),
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
    }

    Ok(GroupAction::Wait {
        delay: DEFAULT_RETRY_DELAY,
        message,
    })
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

    let available_nodes = store
        .cached_nodes()?
        .into_iter()
        .filter(|node| node.availability == "available")
        .map(|node| node.id)
        .collect::<HashSet<_>>();
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

        if matches!(task.lifecycle, TaskLifecycle::Batch) {
            let Some(kill_after) = plan.config.batch_kill_after else {
                outcome.blocked = true;
                continue;
            };

            let started_at =
                parse_optional_ts(state.active_step_started_at.as_deref())?.unwrap_or(now);
            if now - started_at < time::Duration::try_from(kill_after)? {
                outcome.blocked = true;
                continue;
            }
        }

        let to_remove = (current_old - target_old) as usize;
        let mut old_allocs = task.current_old_allocs.clone();
        old_allocs.sort_by_key(|alloc| alloc.id);

        for allocation in old_allocs.into_iter().take(to_remove) {
            let _ = store.remove_allocation_by_id(allocation.id)?;
            outcome.removed += 1;
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
        let outcome = scheduler.plan_and_apply(from_config_id, PlanMode::Reschedule)?;
        if !outcome.allocations.is_empty() {
            store.update_task_statuses_for_allocations(&outcome.allocations)?;
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
    let old_allocs = if let Some(from_id) = rollout.from_job_config_id.as_ref() {
        store.allocs_for_job_config(from_id)?
    } else {
        Vec::new()
    };

    let old_allocs_by_task = old_allocs.into_iter().fold(
        HashMap::<i64, Vec<Allocation>>::new(),
        |mut acc, allocation| {
            acc.entry(allocation.task_config_id)
                .or_default()
                .push(allocation);
            acc
        },
    );

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
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let configured_rollout = rollout_config_for_group(to_config, &group);

        let mut tasks = Vec::new();
        let mut base_count = 0;
        for name in task_names {
            let new_entry = new_tasks.get(&name).cloned();
            let old_entry = old_tasks.get(&name).cloned();

            let desired_new = new_entry
                .as_ref()
                .map(|(_, task)| task.count.num)
                .unwrap_or(0);
            base_count = base_count.max(desired_new);

            let lifecycle = new_entry
                .as_ref()
                .map(|(_, task)| task.lifecycle.clone())
                .or_else(|| old_entry.as_ref().map(|(_, task)| task.lifecycle.clone()))
                .unwrap_or(TaskLifecycle::Service);

            let new_task_id = new_entry.as_ref().map(|(id, _)| *id);
            let old_task_id = old_entry.as_ref().map(|(id, _)| *id);

            let current_new = new_task_id
                .and_then(|id| to_alloc_counts.get(&id).copied())
                .unwrap_or_default();
            let current_old_allocs = old_task_id
                .and_then(|id| old_allocs_by_task.get(&id).cloned())
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
        let step_fractions = step_fractions_for_rollout(&effective_rollout, base_count.max(1));
        plans.push(GroupPlan {
            name: group,
            config: effective_rollout,
            progression,
            replacement,
            step_fractions,
            tasks,
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
        if deduped.last().copied().map_or(true, |previous: f64| {
            (previous - clamped).abs() > f64::EPSILON
        }) {
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
            .tempdir()
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
                "memory_total": "8388608",
                "cpus": {
                    "cpu-1": { "frequency": "2400" }
                }
            })
            .to_string(),
            dynamic_attrs: "{}".into(),
            health_checks: json!({ "worker": "pass" }).to_string(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
        };
        store.insert_node(node)?;
        Ok(())
    }

    fn submission() -> Submission {
        Submission {
            path: "job.jsonnet".into(),
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
                    },
                    Allocation {
                        id: 2,
                        status: "running".to_string(),
                        resources: json!({}),
                        node_id: "node-b".to_string(),
                        job_config_id: "job-1".to_string(),
                        task_config_id: 1,
                        job_id: "job".to_string(),
                    },
                    Allocation {
                        id: 3,
                        status: "running".to_string(),
                        resources: json!({}),
                        node_id: "node-c".to_string(),
                        job_config_id: "job-1".to_string(),
                        task_config_id: 1,
                        job_id: "job".to_string(),
                    },
                ],
            }],
        };

        let targets = build_step_targets(&plan, 0.5);
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
}
