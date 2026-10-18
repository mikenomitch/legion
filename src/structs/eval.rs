use rand::Rng;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use std::{cmp, thread};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tracing::{info, warn};

use crate::manager::dispatcher::dispatch_allocations;
use crate::manager::scheduler::{PlanMode, PlanOutcome, Scheduler};
#[cfg(test)]
use crate::prelude::Context;
use crate::prelude::{bail, json, R};
use crate::storage::errors::StorageError;
use crate::storage::store::Store;

use super::node::Node;
use super::Eval;

#[cfg(test)]
use super::config::ManagerConfigFull;
#[cfg(test)]
use super::job::{JobConfig, TaskConfig, TaskCount};
#[cfg(test)]
use super::ledger::LedgerEntry;
#[cfg(test)]
use super::Submission;

/// Record one handler phase into `legion_eval_phase_seconds{kind, phase}`
/// (docs/metrics.md). The phases of a placing eval partition
/// `legion_eval_handle_seconds`, so the time an eval spends outside its
/// plan is attributable instead of a derived remainder.
fn observe_phase(kind: &str, phase: &str, elapsed: Duration) {
    crate::manager::telemetry::metrics()
        .eval_phase_seconds
        .observe(&[kind, phase], elapsed.as_secs_f64());
}

impl Eval {
    pub fn handle(&self, store: &Store) -> R<String> {
        match self.kind.as_str() {
            "node_join" => self.handle_node_join(store),
            "node_drop" => self.handle_node_drop(store),
            "node_drain" => self.handle_node_drain(store),
            "node_lost_reschedule" => self.handle_node_lost_reschedule(store),
            "job_submit" => self.handle_job_submit(store),
            "job_update" => self.handle_job_update(store),
            "rollout_tick" => self.handle_rollout_tick(store),
            "job_stop" => self.handle_job_stop(store),
            "job_reschedule" => self.handle_job_reschedule(store),
            "job_scale" => self.handle_job_scale(store),
            other => bail!("Unsupported eval kind: {other}"),
        }
    }

    fn handle_node_join(&self, store: &Store) -> R<String> {
        let node_id = self.node_name.trim();

        if node_id.is_empty() {
            // The capacity pass a worker registration or an uncordon queues
            // (`Store::request_capacity_reconcile`). The node is already
            // registered and live; only the jobs waiting on capacity need
            // another look.
            store.reschedule_allocations_for_new_node()?;
            return Ok("Re-evaluated jobs waiting on capacity".to_string());
        }

        let node_id = node_id.to_owned();
        let last_heartbeat = Self::current_timestamp();
        // One keyed lookup, not a read of every node row: this eval used to
        // load the whole fleet to answer "does this node exist".
        let exists = match store.get_node(&node_id) {
            Ok(_) => true,
            Err(error)
                if matches!(
                    StorageError::from_anyhow(&error),
                    Some(StorageError::NotFound { .. })
                ) =>
            {
                false
            }
            Err(error) => return Err(error),
        };

        if exists {
            store.mark_node_ready(&node_id, &last_heartbeat)?;
        } else {
            let node = Node {
                id: node_id.clone(),
                status: "ready".into(),
                pool: "default".into(),
                availability: "available".into(),
                last_heartbeat,
                heartbeat_source: String::new(),
                config_attrs: json!({}),
                fingerprint_attrs: Self::default_fingerprint_attrs(),
                dynamic_attrs: json!({}),
                health_checks: json!({}),
            };

            store.insert_node(node.into())?;
        }

        store.reschedule_allocations_for_new_node()?;

        Ok(format!("Node {node_id} joined"))
    }

    fn handle_node_drop(&self, store: &Store) -> R<String> {
        let node_id = self.node_name.trim();

        if node_id.is_empty() {
            bail!("Eval node_drop missing node name");
        }

        store.mark_node_lost(node_id)?;
        store.reschedule_allocations_for_lost_node(node_id)?;

        Ok(format!("Node {node_id} dropped"))
    }

    /// Drain eviction pass for a node whose availability was set to
    /// `"drained"` with `evict = true` (SetNodeAvailability): evict the
    /// node's stateless allocations for replacement elsewhere while stateful
    /// allocations stay protected by their `onLost` policy. See
    /// `Store::reschedule_allocations_for_drained_node`.
    fn handle_node_drain(&self, store: &Store) -> R<String> {
        let node_id = self.node_name.trim();

        if node_id.is_empty() {
            bail!("Eval node_drain missing node name");
        }

        store.reschedule_allocations_for_drained_node(node_id)?;

        Ok(format!("Node {node_id} drained"))
    }

    /// Scheduler-lane reschedule pass for a node marked lost (plan §7-C2,
    /// C-2): the status-lane node monitor marks liveness and enqueues this
    /// eval instead of calling the scheduler-lane reschedule inline. The
    /// pass itself owns all stateful-protection gates
    /// (`Store::reschedule_allocations_for_lost_node`) and is idempotent;
    /// the monitor re-enqueues every tick while the node stays lost, so
    /// gated allocations are freed automatically once their
    /// `onLost.replaceAfter` window elapses.
    fn handle_node_lost_reschedule(&self, store: &Store) -> R<String> {
        let node_id = self.node_name.trim();

        if node_id.is_empty() {
            bail!("Eval node_lost_reschedule missing node name");
        }

        store.reschedule_allocations_for_lost_node(node_id)?;

        Ok(format!("Rescheduled allocations for lost node {node_id}"))
    }

    fn handle_job_submit(&self, store: &Store) -> R<String> {
        let prepare_started = Instant::now();
        // A submit that waited behind a newer submit or a stop has nothing
        // left to do; migrating or placing for it would resurrect a config
        // the job moved past (the plan itself would place nothing either).
        if let Some(reason) = store.job_config_unwanted_reason(&self.job_config_id)? {
            return Ok(format!("Job {} submit skipped: {reason}", self.job_id));
        }
        let previous_config_id = store.previous_job_config_id(&self.job_config_id)?;
        if let Some(prev_id) = previous_config_id.as_ref() {
            if store.job_configs_schedule_equivalent(prev_id, &self.job_config_id)? {
                store.migrate_allocations_to_job_config(prev_id, &self.job_config_id)?;
                self.schedule_batch_follow_up(store)?;
                observe_phase(&self.kind, "prepare", prepare_started.elapsed());
                return Ok(format!("Job {} submitted with 0 placements", self.job_id));
            }
        }
        observe_phase(&self.kind, "prepare", prepare_started.elapsed());

        // A first submit's plan is its last write unless a batch follow-up
        // is queued after it; a resubmit removes the previous config's
        // allocations after its plan, so it settles in its own write.
        if previous_config_id.is_none() && self.batch_follow_up_delay(store)?.is_none() {
            crate::storage::store::arm_eval_settle();
        }
        let outcome = self.schedule_job_allocations(store, PlanMode::Fresh)?;
        if !outcome.deltas.is_empty() {
            info!(?outcome.deltas, "Job submit allocation deltas");
        }

        let finish_started = Instant::now();
        if let Some(prev_id) = previous_config_id {
            store.remove_allocs_for_job_config(&prev_id)?;
        }

        self.schedule_batch_follow_up(store)?;
        observe_phase(&self.kind, "finish", finish_started.elapsed());

        Ok(Self::outcome_message(
            format!(
                "Job {} submitted with {} placements",
                self.job_id,
                outcome.allocations.len()
            ),
            &outcome,
        ))
    }

    fn handle_job_update(&self, store: &Store) -> R<String> {
        if self.job_id.trim().is_empty() {
            bail!("Eval job_update missing job id");
        }

        let prepare_started = Instant::now();
        // Checked first: the removal below takes EVERY allocation of the
        // job, so an update for a config the job moved past (a stale batch
        // follow-up) or for a stopped job would remove the live version and
        // then place nothing. Skipping leaves the restart marker for the
        // update that is still wanted.
        if let Some(reason) = store.job_config_unwanted_reason(&self.job_config_id)? {
            return Ok(format!("Job {} update skipped: {reason}", self.job_id));
        }

        // Consume the replicated restart marker BEFORE executing the
        // restart (see `Store::request_job_restart`): once cleared, a later
        // failover will not regenerate this restart; if clearing is
        // rejected (foreign-lane leader under `lanes`), the eval fails
        // before any churn and is released for retry. Clearing an unset
        // marker is a no-op (this eval also runs for batch re-schedules).
        store.clear_job_restart_request(&self.job_id)?;
        // A restart starts the move budget over too: replacements waiting
        // out a reschedule backoff are placed now (this is what `job run
        // --force-reschedule` promises), and earlier failures no longer
        // lengthen the next backoff or count toward `move_count`.
        let cleared = store.clear_reschedule_holds_for_job(&self.job_id)?;
        if cleared > 0 {
            info!(
                job_id = %self.job_id,
                cleared,
                "Cleared reschedule backoff for a job restart"
            );
        }

        store.remove_allocs_for_job(&self.job_id)?;
        // A restart (a batch schedule's next run, a forced reschedule, a KV
        // restart) runs the job again from the top: its tasks' statuses from
        // the previous run must not satisfy this run's `dependsOn`, or every
        // step of a DAG would start at once again.
        store.reset_task_statuses(&self.job_config_id)?;
        observe_phase(&self.kind, "prepare", prepare_started.elapsed());
        // Everything above wrote before the plan; the plan is the last write
        // unless a batch follow-up is queued after it.
        if self.batch_follow_up_delay(store)?.is_none() {
            crate::storage::store::arm_eval_settle();
        }
        let outcome = match self.schedule_job_allocations(store, PlanMode::Fresh) {
            Ok(outcome) => outcome,
            // The job's allocations are already gone. A transient failure
            // re-runs this eval (`handle_claimed_eval` releases it); any
            // other leaves the job at zero, so queue a reschedule before
            // failing.
            Err(error) => {
                if !Self::is_conflict_error(&error)
                    && !crate::storage::raft::driver::NotRaftLeader::in_chain(&error)
                {
                    let _ = store.queue_reschedule_retry(&self.job_id, &self.job_config_id, &error);
                }
                return Err(error);
            }
        };
        if !outcome.deltas.is_empty() {
            info!(?outcome.deltas, "Job update allocation deltas");
        }

        let finish_started = Instant::now();
        self.schedule_batch_follow_up(store)?;
        observe_phase(&self.kind, "finish", finish_started.elapsed());

        Ok(Self::outcome_message(
            format!(
                "Job {} updated with {} placements",
                self.job_id,
                outcome.allocations.len()
            ),
            &outcome,
        ))
    }

    fn handle_rollout_tick(&self, store: &Store) -> R<String> {
        let rollout_id = store
            .latest_rollout_for_job_config(&self.job_config_id)?
            .map(|rollout| rollout.id);
        match crate::manager::rollout::handle_rollout_tick(self, store) {
            Ok(message) => Ok(message),
            // A typed NotRaftLeader is a TRANSIENT leadership shape, not a
            // rollout failure: stamping `failed` here would be permanent
            // (the released eval's retry finds a terminal rollout and
            // no-ops), turning one lane blip under divergent lane
            // leadership into a dead rollout whose job then converges via
            // the gating-bypassing regeneration path (plan §8). Propagate
            // it unstamped — `handle_claimed_eval` releases the eval for
            // retry, and the tick resumes once leadership converges.
            Err(error) if crate::storage::raft::driver::NotRaftLeader::in_chain(&error) => {
                Err(error)
            }
            Err(error) => {
                if let Some(rollout_id) = rollout_id {
                    let _ = store.update_rollout_status(
                        rollout_id,
                        "failed",
                        false,
                        Some(&error.to_string()),
                    );
                    if let Ok(Some(rollout)) = store.get_rollout(rollout_id) {
                        let _ = store.append_rollout_event(
                            rollout_id,
                            &rollout.job_id,
                            None,
                            "error",
                            &format!("Rollout tick failed: {}", error),
                            Some(
                                &json!({
                                    "status": "failed",
                                    "error": error.to_string(),
                                })
                                .to_string(),
                            ),
                        );
                    }
                }
                Err(error)
            }
        }
    }

    fn handle_job_stop(&self, store: &Store) -> R<String> {
        if self.job_id.trim().is_empty() {
            bail!("Eval job_stop missing job id");
        }

        store.remove_allocs_for_job(&self.job_id)?;
        // Stateful instance ROWS of a stopped job are always retained: slot
        // rows are terminated so a later re-run re-materializes the same
        // ordinal identities; keyed rows are merely stopped, so resubmitting
        // the job + `legion instance start` revives them with their old
        // handles (only DestroyInstance / job destroy is forever). Slot
        // storage handles are retained too unless the task opts out with
        // `stateful.storage.retainOnStop: false` (the K8s
        // `whenDeleted: Delete` analog), in which case the handles are
        // dropped and a re-run mints fresh ones. No-op for stateless jobs.
        store.stop_instances_for_job(&self.job_id)?;
        // The stopped job's capacity is free: retry jobs waiting for room.
        store.request_capacity_reconcile_if_waiting();

        Ok(format!("Job {} stopped", self.job_id))
    }

    fn handle_job_reschedule(&self, store: &Store) -> R<String> {
        if self.job_id.trim().is_empty() || self.job_config_id.trim().is_empty() {
            bail!("Eval job_reschedule missing identifiers");
        }

        // The plan is this eval's last write unless a scheduled batch task
        // needs its follow-up enqueued after it, so an applied plan may
        // settle the eval in its own entry (sandbox-scaling plan, Phase 1).
        if self.batch_follow_up_delay(store)?.is_none() {
            crate::storage::store::arm_eval_settle();
        }
        let outcome = self.schedule_job_allocations(store, PlanMode::Reschedule)?;
        if !outcome.deltas.is_empty() {
            info!(?outcome.deltas, "Job reschedule allocation deltas");
        }

        let finish_started = Instant::now();
        self.schedule_batch_follow_up(store)?;
        observe_phase(&self.kind, "finish", finish_started.elapsed());

        Ok(Self::outcome_message(
            format!(
                "Job {} rescheduled with {} placements",
                self.job_id,
                outcome.allocations.len()
            ),
            &outcome,
        ))
    }

    /// An eval's outcome line, naming what the plan left undone (replicas
    /// no node had room for, tasks waiting on a dependency) so the
    /// `eval.completed` event says why a job is short.
    fn outcome_message(message: String, outcome: &PlanOutcome) -> String {
        let shortfall = outcome.shortfall_summary();
        if shortfall.is_empty() {
            message
        } else {
            format!("{message}; {shortfall}")
        }
    }

    /// Reconcile a job's allocation set to its EFFECTIVE desired counts
    /// after a `legion job scale` (docs/autoscaling.md). Scale-in first:
    /// surplus allocations above each task's effective count are retired
    /// (highest slots first for slot-mode stateful tasks — their instances
    /// terminate through `slot_still_desired_tx`'s override-aware check —
    /// newest-first for stateless). Then scale-out: a Reschedule-mode plan
    /// places only the shortfall, leaving existing allocations alone.
    /// Idempotent — a re-run converges to the same counts.
    fn handle_job_scale(&self, store: &Store) -> R<String> {
        if self.job_id.trim().is_empty() || self.job_config_id.trim().is_empty() {
            bail!("Eval job_scale missing identifiers");
        }

        let prepare_started = Instant::now();
        let retired = store.retire_surplus_allocations(&self.job_config_id)?;
        observe_phase(&self.kind, "prepare", prepare_started.elapsed());
        // A scale-in queues a capacity pass after its plan; a scale-out's
        // plan is its last write.
        if retired.is_empty() {
            crate::storage::store::arm_eval_settle();
        }
        let outcome = self.schedule_job_allocations(store, PlanMode::Reschedule)?;
        if !outcome.deltas.is_empty() {
            info!(?outcome.deltas, "Job scale allocation deltas");
        }
        if !retired.is_empty() {
            // Scale-in freed capacity: retry jobs waiting for room.
            store.request_capacity_reconcile_if_waiting();
        }

        Ok(format!(
            "Job {} scaled: {} placed, {} retired",
            self.job_id,
            outcome.allocations.len(),
            retired.len()
        ))
    }

    fn schedule_job_allocations(&self, store: &Store, mode: PlanMode) -> R<PlanOutcome> {
        if self.job_config_id.trim().is_empty() {
            bail!("Eval missing job config id");
        }

        let init_started = Instant::now();
        let scheduler = Scheduler::new(store)?;
        observe_phase(&self.kind, "scheduler_init", init_started.elapsed());
        let avoided = self.excluded_nodes_for_mode(&mode);
        let Some(avoided) = avoided else {
            let outcome = self.plan_with_conflict_retries(store, &scheduler, mode, None)?;
            let dispatch_started = Instant::now();
            dispatch_allocations(store, &outcome.allocations)?;
            observe_phase(&self.kind, "dispatch", dispatch_started.elapsed());
            self.schedule_preemption_retry(store, &outcome)?;
            return Ok(outcome);
        };

        // A reschedule after a failure AVOIDS the node the allocation failed
        // on rather than excluding it: the first pass plans without it, and
        // only what that pass could not place is planned again with the node
        // allowed. On a one- or two-node cluster the failed node is often
        // the only one that fits, and a hard exclusion left the job with
        // nothing ("No nodes available to place allocations") for good.
        let first = match self.plan_with_conflict_retries(
            store,
            &scheduler,
            mode.clone(),
            Some(&avoided),
        ) {
            Ok(outcome) => Some(outcome),
            Err(error)
                if !Self::is_conflict_error(&error)
                    && !crate::storage::raft::driver::NotRaftLeader::in_chain(&error) =>
            {
                info!(
                    job_id = %self.job_id,
                    job_config_id = %self.job_config_id,
                    avoided_node = %self.node_name,
                    error = %format!("{error:#}"),
                    "Reschedule found no other node; retrying with the failed node allowed"
                );
                None
            }
            Err(error) => return Err(error),
        };
        let mut dispatch_time = Duration::ZERO;
        let mut dispatch = |allocations: &[crate::structs::Allocation]| -> R<()> {
            let started = Instant::now();
            dispatch_allocations(store, allocations)?;
            dispatch_time += started.elapsed();
            Ok(())
        };
        let outcome = match first {
            Some(first) if first.unplaced.is_empty() => {
                dispatch(&first.allocations)?;
                first
            }
            Some(mut first) => {
                dispatch(&first.allocations)?;
                match self.plan_with_conflict_retries(store, &scheduler, mode, None) {
                    Ok(second) => {
                        dispatch(&second.allocations)?;
                        first.allocations.extend(second.allocations);
                        first.deltas.extend(second.deltas);
                        first.evictions.extend(second.evictions);
                        first.unplaced = second.unplaced;
                        first
                    }
                    // The first pass placed something and dispatched it; the
                    // rest stays reported as unplaced on the first outcome.
                    Err(error)
                        if !crate::storage::raft::driver::NotRaftLeader::in_chain(&error) =>
                    {
                        info!(
                            job_id = %self.job_id,
                            error = %format!("{error:#}"),
                            "Failed node could not take the remainder either"
                        );
                        first
                    }
                    Err(error) => return Err(error),
                }
            }
            None => {
                let outcome = self.plan_with_conflict_retries(store, &scheduler, mode, None)?;
                dispatch(&outcome.allocations)?;
                outcome
            }
        };
        observe_phase(&self.kind, "dispatch", dispatch_time);
        self.schedule_preemption_retry(store, &outcome)?;
        Ok(outcome)
    }

    /// A plan that placed part of the job and left a replica waiting for
    /// preemption victims to age past `victimExemptAge` queues one delayed
    /// `job_reschedule` for when they have. Nothing else would retry it: a
    /// victim getting older is not a capacity event. (A plan that placed
    /// nothing gets the same retry from the eval worker, which requeues the
    /// eval itself.)
    fn schedule_preemption_retry(&self, store: &Store, outcome: &PlanOutcome) -> R<()> {
        let Some(delay) = outcome
            .unplaced
            .iter()
            .filter_map(|task| task.blocked.retry_after)
            .min()
        else {
            return Ok(());
        };
        if self.job_id.trim().is_empty()
            || store.delayed_eval_pending("job_reschedule", &self.job_config_id)?
        {
            return Ok(());
        }
        store.enqueue_eval_after_delay(
            "job_reschedule",
            Some(&self.job_id),
            Some(&self.job_config_id),
            None,
            None,
            delay,
        )?;
        Ok(())
    }

    /// The node a reschedule should avoid: the one the failed allocation ran
    /// on. Avoided, not excluded — see `schedule_job_allocations`.
    fn excluded_nodes_for_mode(&self, mode: &PlanMode) -> Option<HashSet<String>> {
        if matches!(mode, PlanMode::Reschedule) && !self.node_name.trim().is_empty() {
            return Some(HashSet::from([self.node_name.clone()]));
        }

        None
    }

    fn plan_with_conflict_retries(
        &self,
        store: &Store,
        scheduler: &Scheduler,
        mode: PlanMode,
        excluded: Option<&HashSet<String>>,
    ) -> R<PlanOutcome> {
        let max_plan_retries = Self::max_plan_conflict_retries(store);
        let backoff = Self::plan_conflict_backoff_settings(store);
        let mut conflict_count = 0usize;
        let mut total_backoff_delay = Duration::from_millis(0);
        // Every attempt counts toward `plan`, rejected ones included; the
        // sleeps between attempts are `conflict_backoff`. Before this split
        // a rejected attempt and its backoff were visible nowhere but the
        // handler's total.
        let started = Instant::now();
        let observe = |slept: Duration| {
            observe_phase(&self.kind, "plan", started.elapsed().saturating_sub(slept));
            if !slept.is_zero() {
                observe_phase(&self.kind, "conflict_backoff", slept);
            }
        };

        for attempt in 1..=max_plan_retries {
            match scheduler.plan_and_apply_with_exclusions(
                &self.job_config_id,
                mode.clone(),
                excluded,
            ) {
                Ok(outcome) => {
                    observe(total_backoff_delay);
                    return Ok(outcome);
                }
                Err(error) => {
                    if !Self::is_conflict_error(&error) {
                        observe(total_backoff_delay);
                        return Err(error);
                    }
                    conflict_count += 1;

                    if attempt < max_plan_retries {
                        let delay = Self::adaptive_retry_delay(backoff, attempt, conflict_count);
                        total_backoff_delay += delay;
                        warn!(
                            attempt,
                            conflict_count,
                            max_attempts = max_plan_retries,
                            retry_delay_ms = delay.as_millis(),
                            error = %error,
                            "Plan submission rejected due to stale state; retrying"
                        );
                        thread::sleep(delay);
                        continue;
                    }

                    observe(total_backoff_delay);
                    warn!(
                        attempts = max_plan_retries,
                        conflict_count,
                        total_backoff_ms = total_backoff_delay.as_millis(),
                        "Plan conflict retry budget exhausted"
                    );
                    return Err(error.context(format!(
                        "plan submission failed after {} attempts",
                        max_plan_retries
                    )));
                }
            }
        }

        unreachable!("retry loop returns or errors")
    }

    fn is_conflict_error(error: &anyhow::Error) -> bool {
        StorageError::from_anyhow(error)
            .is_some_and(|storage| matches!(storage, StorageError::Conflict { .. }))
    }

    /// Whether an eval-handler error is (or wraps) an optimistic-concurrency
    /// plan conflict (`StorageError::Conflict`). This is the transient class
    /// `handle_claimed_eval` requeues with a delay instead of terminally
    /// failing: the plan lost races, not validity, and failed evals are
    /// never retried (docs/late-binding-plan-apply.md).
    pub fn error_is_plan_conflict(error: &anyhow::Error) -> bool {
        Self::is_conflict_error(error)
    }

    /// The requeue-backstop delay for a conflict-exhausted eval:
    /// `plan_conflict_requeue_delay_ms` (default 1000, clamped to
    /// [100, 60_000]) plus uniform jitter of up to the base, so a burst of
    /// requeued racers does not re-fire in lockstep and re-collide.
    pub fn plan_conflict_requeue_delay(store: &Store) -> Duration {
        const DEFAULT_MS: u64 = 1_000;
        const MIN_MS: u64 = 100;
        const MAX_MS: u64 = 60_000;

        let base = store
            .scheduler_settings
            .as_ref()
            .and_then(|settings| settings.get("plan_conflict_requeue_delay_ms"))
            .and_then(|value| value.as_u64())
            .unwrap_or(DEFAULT_MS)
            .clamp(MIN_MS, MAX_MS);
        let jitter = rand::thread_rng().gen_range(0..=base);
        Duration::from_millis(base + jitter)
    }

    fn max_plan_conflict_retries(store: &Store) -> usize {
        const DEFAULT_RETRIES: usize = 3;
        const MAX_RETRIES: usize = 20;

        store
            .scheduler_settings
            .as_ref()
            .and_then(|settings| settings.get("plan_conflict_max_retries"))
            .and_then(|value| value.as_u64())
            .map(|value| value.clamp(1, MAX_RETRIES as u64) as usize)
            .unwrap_or(DEFAULT_RETRIES)
    }

    fn plan_conflict_backoff_settings(store: &Store) -> RetryBackoffSettings {
        const DEFAULT_BASE_MS: u64 = 25;
        const DEFAULT_MAX_MS: u64 = 500;
        const DEFAULT_JITTER_MS: u64 = 35;
        const MAX_BASE_MS: u64 = 5_000;
        const MAX_DELAY_MS: u64 = 30_000;
        const MAX_JITTER_MS: u64 = 10_000;

        let get_setting = |key: &str| -> Option<u64> {
            store
                .scheduler_settings
                .as_ref()
                .and_then(|settings| settings.get(key))
                .and_then(|value| value.as_u64())
        };

        let base_delay_ms = get_setting("plan_conflict_backoff_base_ms")
            .unwrap_or(DEFAULT_BASE_MS)
            .clamp(1, MAX_BASE_MS);
        let max_delay_ms = get_setting("plan_conflict_backoff_max_ms")
            .unwrap_or(DEFAULT_MAX_MS)
            .clamp(base_delay_ms, MAX_DELAY_MS);
        let jitter_ms = get_setting("plan_conflict_backoff_jitter_ms")
            .unwrap_or(DEFAULT_JITTER_MS)
            .clamp(0, MAX_JITTER_MS);

        RetryBackoffSettings {
            base_delay_ms,
            max_delay_ms,
            jitter_ms,
        }
    }

    fn adaptive_retry_delay(
        settings: RetryBackoffSettings,
        attempt: usize,
        conflict_count: usize,
    ) -> Duration {
        let exponent = cmp::min(
            (attempt.saturating_sub(1) + conflict_count.saturating_sub(1)) as u32,
            16,
        );
        let multiplier = 1_u64 << exponent;
        let raw = settings.base_delay_ms.saturating_mul(multiplier);
        let capped = raw.min(settings.max_delay_ms);
        let jitter = if settings.jitter_ms == 0 {
            0
        } else {
            rand::thread_rng().gen_range(0..=settings.jitter_ms)
        };
        Duration::from_millis(capped.saturating_add(jitter).min(settings.max_delay_ms))
    }

    fn schedule_batch_follow_up(&self, store: &Store) -> R<()> {
        if let Some(delay) = self.batch_follow_up_delay(store)? {
            // One timer per config. Every placing eval passes through here,
            // and a DAG's steps each queue a `job_reschedule` as they finish
            // (`Store::queue_dependency_progress`); without this check each
            // of them would add another delayed `job_update`, and the job
            // would re-run once per step at the next schedule tick.
            if store.delayed_eval_pending("job_update", &self.job_config_id)? {
                return Ok(());
            }
            store.enqueue_eval_after_delay(
                "job_update",
                Some(&self.job_id),
                Some(&self.job_config_id),
                None,
                None,
                delay,
            )?;
        }

        Ok(())
    }

    /// When the job's scheduled batch tasks next need a `job_update`, or
    /// `None` when it has none (and the follow-up writes nothing).
    fn batch_follow_up_delay(&self, store: &Store) -> R<Option<std::time::Duration>> {
        if self.job_id.trim().is_empty() || self.job_config_id.trim().is_empty() {
            return Ok(None);
        }

        // The read model's copy (immutable per config id), not a SQL query
        // per placing eval.
        let tasks = store.shared_task_configs_for_job_config(&self.job_config_id)?;
        let mut min_delay: Option<std::time::Duration> = None;
        for (_, task) in tasks.iter() {
            if task.lifecycle != super::job::TaskLifecycle::Batch {
                continue;
            }
            let Some(schedule) = task.schedule.as_ref() else {
                continue;
            };
            let delay = schedule.next_delay().map_err(|err| {
                anyhow::anyhow!("invalid batch schedule for task {}: {err}", task.name)
            })?;
            min_delay = Some(match min_delay {
                Some(current) => current.min(delay),
                None => delay,
            });
        }

        Ok(min_delay)
    }

    fn current_timestamp() -> String {
        OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into())
    }

    fn default_fingerprint_attrs() -> serde_json::Value {
        json!({
            "memory_total": "8589934592",
            "cpus": {
                "cpu-1": { "frequency": "2400" },
                "cpu-2": { "frequency": "2400" },
            }
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct RetryBackoffSettings {
    base_delay_ms: u64,
    max_delay_ms: u64,
    jitter_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::store::Store;
    use crate::structs::NodeMsg;
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-eval-")
            .tempdir_in(crate::test_scratch::root())
            .context("error creating temp store directory")?
            .keep();
        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("error parsing store path")?
                .to_string(),
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    fn find_node<'a>(ledger: &'a [LedgerEntry], node_id: &str) -> Option<&'a LedgerEntry> {
        ledger.iter().find(|entry| entry.node.id == node_id)
    }
    #[test]
    fn max_plan_conflict_retries_defaults_to_three() -> R<()> {
        let store = setup_store()?;
        assert_eq!(Eval::max_plan_conflict_retries(&store), 3);
        Ok(())
    }

    #[test]
    fn max_plan_conflict_retries_respects_scheduler_settings() -> R<()> {
        let store_dir = Builder::new()
            .prefix("legion-eval-")
            .tempdir_in(crate::test_scratch::root())
            .context("error creating temp store directory")?
            .keep();
        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("error parsing store path")?
                .to_string(),
            scheduler_settings: Some(json!({ "plan_conflict_max_retries": 9 })),
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;
        assert_eq!(Eval::max_plan_conflict_retries(&store), 9);

        Ok(())
    }

    #[test]
    fn plan_conflict_backoff_settings_use_defaults() -> R<()> {
        let store = setup_store()?;
        let settings = Eval::plan_conflict_backoff_settings(&store);
        assert_eq!(settings.base_delay_ms, 25);
        assert_eq!(settings.max_delay_ms, 500);
        assert_eq!(settings.jitter_ms, 35);
        Ok(())
    }

    #[test]
    fn adaptive_retry_delay_is_bounded() {
        let settings = RetryBackoffSettings {
            base_delay_ms: 10,
            max_delay_ms: 80,
            jitter_ms: 0,
        };
        let delay = Eval::adaptive_retry_delay(settings, 6, 6);
        assert_eq!(delay, Duration::from_millis(80));
    }

    #[test]
    fn error_is_plan_conflict_sees_through_context_wrapping() {
        // The exhaustion path wraps the conflict in context — exactly what
        // `plan_with_conflict_retries` returns after its last attempt.
        let wrapped = anyhow::Error::new(StorageError::Conflict {
            message: "plan submission rejected".into(),
        })
        .context("plan submission failed after 3 attempts");
        assert!(Eval::error_is_plan_conflict(&wrapped));

        let unrelated = anyhow::anyhow!("no available nodes matching constraints");
        assert!(!Eval::error_is_plan_conflict(&unrelated));

        let not_found = anyhow::Error::new(StorageError::NotFound {
            entity: "job",
            id: "x".into(),
        });
        assert!(!Eval::error_is_plan_conflict(&not_found));
    }

    #[test]
    fn plan_conflict_requeue_delay_is_jittered_within_bounds() -> R<()> {
        let store = setup_store()?;
        for _ in 0..10 {
            let delay = Eval::plan_conflict_requeue_delay(&store);
            assert!(delay >= Duration::from_millis(1_000), "delay {delay:?}");
            assert!(delay <= Duration::from_millis(2_000), "delay {delay:?}");
        }
        Ok(())
    }

    #[test]
    fn plan_conflict_requeue_delay_respects_scheduler_settings() -> R<()> {
        let store_dir = Builder::new()
            .prefix("legion-eval-")
            .tempdir_in(crate::test_scratch::root())
            .context("error creating temp store directory")?
            .keep();
        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("error parsing store path")?
                .to_string(),
            scheduler_settings: Some(json!({ "plan_conflict_requeue_delay_ms": 100 })),
            ..Default::default()
        };
        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;

        for _ in 0..10 {
            let delay = Eval::plan_conflict_requeue_delay(&store);
            assert!(delay >= Duration::from_millis(100), "delay {delay:?}");
            assert!(delay <= Duration::from_millis(200), "delay {delay:?}");
        }
        Ok(())
    }

    fn make_node(id: &str, zone: &str) -> Node {
        Node {
            id: id.to_string(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({ "node_id": id, "zone": zone }),
            fingerprint_attrs: json!({
                "memory_total": "8589934592",
                "cpus": {
                    "cpu-1": { "frequency": "2400" },
                    "cpu-2": { "frequency": "2400" },
                }
            }),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    /// `legion job scale` end to end at the eval layer
    /// (docs/autoscaling.md): a count override changes the effective
    /// desired count with NO new config version; the `job_scale` eval
    /// converges the allocation set both ways (scale-out places only the
    /// shortfall, scale-in retires surplus); and resubmits arbitrate
    /// ownership via `base_count` — an unchanged spec count keeps the
    /// override, a changed one returns the count to the spec.
    #[test]
    fn job_scale_eval_converges_and_resubmits_arbitrate() -> R<()> {
        let store = setup_store()?;
        for node in ["node-1", "node-2"] {
            Eval {
                id: node.into(),
                status: "pending".into(),
                kind: "node_join".into(),
                job_id: String::new(),
                job_config_id: String::new(),
                node_name: node.into(),
                priority: 50,
            }
            .handle(&store)?;
        }

        let submission = Submission {
            path: "web.ncl".into(),
            content: "v1".into(),
            is_primary: true,
        };
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "web".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                resources: json!({ "memory": 128 }),
                config: json!({ "message": "v1" }),
                ..TaskConfig::new("api", "service")
            }],
            version: None,
        };
        store.handle_job_submit(&job, std::slice::from_ref(&submission))?;
        let job_config_id = store.get_job_config("web")?.id();
        let submit_eval = Eval {
            id: "e-submit".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: "web".into(),
            job_config_id: job_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        };
        submit_eval.handle(&store)?;
        assert_eq!(store.allocs_for_job("web")?.len(), 2);

        // Scale OUT to 5: only the shortfall is placed, on the same config.
        store.set_task_scale_override("web", "default", "api", 5, 2, "operator", "test", "t")?;
        let existing_ids: std::collections::HashSet<i64> = store
            .allocs_for_job("web")?
            .iter()
            .map(|alloc| alloc.id)
            .collect();
        let scale_eval = Eval {
            id: "e-scale".into(),
            status: "pending".into(),
            kind: "job_scale".into(),
            job_id: "web".into(),
            job_config_id: job_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        };
        scale_eval.handle(&store)?;
        let allocs = store.allocs_for_job("web")?;
        assert_eq!(allocs.len(), 5, "scale-out converges to the override");
        assert!(
            existing_ids
                .iter()
                .all(|id| allocs.iter().any(|alloc| alloc.id == *id)),
            "scale-out must not churn existing allocations"
        );
        assert_eq!(
            store.get_job_config("web")?.id(),
            job_config_id,
            "scaling must not mint a new config version"
        );

        // Scale IN to 1: surplus retired newest-first.
        store.set_task_scale_override("web", "default", "api", 1, 2, "operator", "test", "t")?;
        scale_eval.handle(&store)?;
        let remaining = store.allocs_for_job("web")?;
        assert_eq!(remaining.len(), 1, "scale-in converges to the override");
        assert_eq!(
            remaining[0].id,
            *existing_ids.iter().min().context("existing ids")?,
            "scale-in retires newest allocations first"
        );

        // Resubmit with the SAME spec count (other fields changed): the
        // override survives the deploy.
        let v2 = JobConfig {
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                resources: json!({ "memory": 128 }),
                config: json!({ "message": "v2" }),
                ..TaskConfig::new("api", "service")
            }],
            ..job.clone()
        };
        store.handle_job_submit(&v2, std::slice::from_ref(&submission))?;
        let scale = store
            .task_scale_for("web", "default", "api")?
            .context("override must survive a resubmit with unchanged count")?;
        assert_eq!(scale.count, Some(1));

        // Resubmit that CHANGES the spec count: new intent, override
        // cleared, the spec owns the count again.
        let v3 = JobConfig {
            tasks: vec![TaskConfig {
                count: TaskCount::new(4),
                resources: json!({ "memory": 128 }),
                config: json!({ "message": "v3" }),
                ..TaskConfig::new("api", "service")
            }],
            ..job.clone()
        };
        store.handle_job_submit(&v3, &[submission])?;
        assert!(
            store.task_scale_for("web", "default", "api")?.is_none(),
            "a resubmit changing the spec count reclaims ownership"
        );
        assert_eq!(
            store.effective_task_count("web", "default", "api", 4)?,
            4,
            "effective count returns to the spec"
        );

        Ok(())
    }

    /// The deferred scale-in lifecycle (docs/autoscaling.md): a pending
    /// count leaves the effective count untouched until promotion, and
    /// clearing drops the row entirely.
    #[test]
    fn pending_scale_in_waits_until_promoted() -> R<()> {
        let store = setup_store()?;
        store.set_task_scale_pending("web", "default", "api", 2, 6, "operator", "shrink", "t")?;

        let scale = store
            .task_scale_for("web", "default", "api")?
            .context("pending row")?;
        assert_eq!(scale.count, None);
        assert_eq!(scale.pending_count, Some(2));
        assert_eq!(
            store.effective_task_count("web", "default", "api", 6)?,
            6,
            "a pending scale-in must not change the effective count"
        );

        let promoted = store.promote_pending_task_scales("web")?;
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0].count, Some(2));
        assert_eq!(promoted[0].pending_count, None);
        assert_eq!(store.effective_task_count("web", "default", "api", 6)?, 2);

        assert!(store.clear_task_scale("web", "default", "api")?);
        assert_eq!(store.effective_task_count("web", "default", "api", 6)?, 6);
        assert!(!store.clear_task_scale("web", "default", "api")?);
        Ok(())
    }

    #[test]
    fn eval_chain_updates_ledger_state() -> R<()> {
        let store = setup_store()?;

        assert!(store.get_ledger()?.is_empty());

        let node_one_eval = Eval {
            id: "1".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "node-1".into(),
            priority: 50,
        };
        node_one_eval.handle(&store)?;

        let mut ledger = store.get_ledger()?;
        assert_eq!(ledger.len(), 1);
        let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
        assert!(node_one_entry.allocations.is_empty());

        let node_two_eval = Eval {
            id: "2".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "node-2".into(),
            priority: 50,
        };
        node_two_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        assert_eq!(ledger.len(), 2);
        let node_two_entry = find_node(&ledger, "node-2").context("missing node-2")?;
        assert!(node_two_entry.allocations.is_empty());

        let job_one_name = "job-1".to_string();
        let job_one_task = TaskConfig {
            config: json!({ "message": "hi" }),
            resources: json!({ "memory": 128 }),
            ..TaskConfig::new("task-one", "service")
        };
        let job_one_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_one_name.clone(),
            tasks: vec![job_one_task.clone()],
            version: None,
        };
        let job_one_subs = vec![Submission {
            path: "job-one".into(),
            content: "job-one".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_one_config, &job_one_subs)?;
        let job_one_config_id = store.get_job_config(&job_one_name)?.id();

        let job_one_eval = Eval {
            id: "3".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_one_name.clone(),
            job_config_id: job_one_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        };
        job_one_eval.handle(&store)?;

        let job_one_config_id_v2 = store.get_job_config(&job_one_name)?.id();

        let job_update_eval = Eval {
            id: "5".into(),
            status: "pending".into(),
            kind: "job_update".into(),
            job_id: job_one_name.clone(),
            job_config_id: job_one_config_id_v2.clone(),
            node_name: String::new(),
            priority: 50,
        };
        job_update_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let job_one_allocations: Vec<_> = ledger
            .iter()
            .flat_map(|entry| entry.allocations.iter())
            .filter(|alloc| alloc.job_config_id == job_one_config_id_v2)
            .collect();
        assert_eq!(job_one_allocations.len(), 1);

        let job_two_name = "job-2".to_string();
        let job_two_task = TaskConfig {
            config: json!({ "message": "hola" }),
            resources: json!({ "memory": 128 }),
            ..TaskConfig::new("task-two", "service")
        };
        let job_two_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_two_name.clone(),
            tasks: vec![job_two_task.clone()],
            version: None,
        };
        let job_two_subs = vec![Submission {
            path: "job-two".into(),
            content: "job-two".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_two_config, &job_two_subs)?;
        let job_two_config_id = store.get_job_config(&job_two_name)?.id();

        let job_two_submit_eval = Eval {
            id: "6".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_two_name.clone(),
            job_config_id: job_two_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        };
        job_two_submit_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let total_allocations: usize = ledger.iter().map(|entry| entry.allocations.len()).sum();
        assert_eq!(total_allocations, 2);

        let stop_job_one_eval = Eval {
            id: "7".into(),
            status: "pending".into(),
            kind: "job_stop".into(),
            job_id: job_one_name.clone(),
            job_config_id: job_one_config_id_v2.clone(),
            node_name: String::new(),
            priority: 50,
        };
        stop_job_one_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let job_two_allocations: Vec<_> = ledger
            .iter()
            .flat_map(|entry| entry.allocations.iter())
            .filter(|alloc| alloc.job_config_id == job_two_config_id)
            .collect();
        assert_eq!(job_two_allocations.len(), 1);

        let stop_job_two_eval = Eval {
            id: "8".into(),
            status: "pending".into(),
            kind: "job_stop".into(),
            job_id: job_two_name,
            job_config_id: job_two_config_id,
            node_name: String::new(),
            priority: 50,
        };
        stop_job_two_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let total_allocations: usize = ledger.iter().map(|entry| entry.allocations.len()).sum();
        assert_eq!(total_allocations, 0);

        Ok(())
    }

    #[test]
    fn job_stop_terminates_stateful_instances_but_retains_rows() -> R<()> {
        use crate::structs::{InstanceMode, InstanceState, StatefulConfig, StatefulStorageConfig};

        let store = setup_store()?;
        store.insert_node(NodeMsg::from(make_node("node-a", "east")))?;

        let job_name = "stateful-stop".to_string();
        let task = TaskConfig {
            count: TaskCount::new(2),
            resources: json!({ "memory": 64 }),
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: Some(StatefulStorageConfig {
                    driver: None,
                    mode: None,
                    params: None,
                    path: "/var/lib/data".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                }),
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
            }),
            ..TaskConfig::new("db", "service")
        };
        let job_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![task],
            version: None,
        };
        store.handle_job_submit(
            &job_config,
            &[Submission {
                path: "job.ncl".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config(&job_name)?.id();

        Eval {
            id: "1".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_name.clone(),
            job_config_id: job_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        }
        .handle(&store)?;

        let instances = store.list_instances(&job_name)?;
        assert_eq!(instances.len(), 2);
        let handles: Vec<Option<String>> = instances
            .iter()
            .map(|instance| instance.storage_handle.clone())
            .collect();
        assert!(handles.iter().all(Option::is_some));

        Eval {
            id: "2".into(),
            status: "pending".into(),
            kind: "job_stop".into(),
            job_id: job_name.clone(),
            job_config_id,
            node_name: String::new(),
            priority: 50,
        }
        .handle(&store)?;

        let stopped = store.list_instances(&job_name)?;
        assert_eq!(stopped.len(), 2, "instance rows retained after stop");
        for (instance, handle) in stopped.iter().zip(&handles) {
            assert_eq!(instance.state, InstanceState::Terminated);
            assert_eq!(instance.current_alloc_id, None);
            assert_eq!(&instance.storage_handle, handle, "storage handles retained");
        }
        Ok(())
    }

    #[test]
    fn job_stop_stops_keyed_instances_and_resubmit_does_not_resurrect_them() -> R<()> {
        use crate::structs::{InstanceMode, InstanceState, StatefulConfig, StatefulStorageConfig};

        let store = setup_store()?;
        store.insert_node(NodeMsg::from(make_node("node-a", "east")))?;

        let job_name = "keyed-stop-eval".to_string();
        let task = TaskConfig {
            resources: json!({ "memory": 64 }),
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Keyed,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: Some(StatefulStorageConfig {
                    driver: None,
                    mode: None,
                    params: None,
                    path: "/workspace".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                }),
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
            }),
            ..TaskConfig::new("sandbox", "service")
        };
        let job_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![task],
            version: None,
        };
        let subs = vec![Submission {
            path: "job.ncl".into(),
            content: "job".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_config, &subs)?;
        let job_config_id = store.get_job_config(&job_name)?.id();

        store.create_keyed_instance(&job_name, "default", "sandbox", Some("user-a"))?;
        Eval {
            id: "1".into(),
            status: "pending".into(),
            kind: "job_reschedule".into(),
            job_id: job_name.clone(),
            job_config_id: job_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        }
        .handle(&store)?;
        let handle = store
            .get_instance("user-a")?
            .context("placed instance")?
            .storage_handle;
        assert!(handle.is_some());

        // Job stop: keyed instance STOPPED (not terminated), handle retained,
        // allocation gone.
        Eval {
            id: "2".into(),
            status: "pending".into(),
            kind: "job_stop".into(),
            job_id: job_name.clone(),
            job_config_id: job_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        }
        .handle(&store)?;
        assert!(store.allocs_for_job(&job_name)?.is_empty());
        let stopped = store.get_instance("user-a")?.context("row retained")?;
        assert_eq!(stopped.state, InstanceState::Stopped);
        assert_eq!(stopped.storage_handle, handle, "handle retained on stop");

        // Resubmitting the job does NOT resurrect a stopped instance: the
        // reconcile plans nothing for it until an explicit start.
        store.handle_job_submit(&job_config, &subs)?;
        Eval {
            id: "3".into(),
            status: "pending".into(),
            kind: "job_reschedule".into(),
            job_id: job_name.clone(),
            job_config_id: job_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        }
        .handle(&store)?;
        assert!(store.allocs_for_job(&job_name)?.is_empty());
        assert_eq!(
            store.get_instance("user-a")?.context("still there")?.state,
            InstanceState::Stopped
        );

        // Explicit start revives it with the SAME id and storage handle.
        store.start_keyed_instance("user-a")?;
        Eval {
            id: "4".into(),
            status: "pending".into(),
            kind: "job_reschedule".into(),
            job_id: job_name.clone(),
            job_config_id,
            node_name: String::new(),
            priority: 50,
        }
        .handle(&store)?;
        let allocations = store.allocs_for_job(&job_name)?;
        assert_eq!(allocations.len(), 1);
        assert_eq!(allocations[0].instance_id.as_deref(), Some("user-a"));
        let revived = store.get_instance("user-a")?.context("revived")?;
        assert_eq!(revived.state, InstanceState::Active);
        assert_eq!(revived.storage_handle, handle, "old handle re-bound");
        Ok(())
    }

    /// Observation count of one `legion_eval_phase_seconds` series, read
    /// from the exposition (the registry is process-global, so tests compare
    /// before and after rather than absolute values).
    fn phase_count(kind: &str, phase: &str) -> u64 {
        let prefix =
            format!("legion_eval_phase_seconds_count{{kind=\"{kind}\",phase=\"{phase}\"}} ");
        crate::manager::telemetry::render()
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0)
    }

    #[test]
    fn job_reschedule_excludes_failed_node() -> R<()> {
        let store = setup_store()?;
        store.insert_node(NodeMsg::from(make_node("node-a", "east")))?;
        store.insert_node(NodeMsg::from(make_node("node-b", "east")))?;

        let job_name = "reschedule-job".to_string();
        let task = TaskConfig {
            count: TaskCount::new(2),
            resources: json!({ "memory": 128 }),
            ..TaskConfig::new("api", "service")
        };
        let job_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![task],
            version: None,
        };
        let job_subs = vec![Submission {
            path: "job.ncl".into(),
            content: "job".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_config, &job_subs)?;
        let job_config_id = store.get_job_config(&job_name)?.id();

        let submit_eval = Eval {
            id: "1".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_name.clone(),
            job_config_id: job_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        };
        let phases = ["prepare", "scheduler_init", "plan", "dispatch", "finish"];
        let before: Vec<u64> = phases
            .iter()
            .map(|phase| phase_count("job_submit", phase))
            .collect();
        submit_eval.handle(&store)?;
        // Every handler phase of a placing eval is observed, so the eval's
        // wall time is attributable (legion_eval_phase_seconds).
        for (phase, before) in phases.iter().zip(before) {
            assert!(
                phase_count("job_submit", phase) > before,
                "phase {phase} was not observed"
            );
        }

        store.mark_node_lost("node-a")?;
        store.remove_allocs_for_node("node-a")?;

        let reschedule_eval = Eval {
            id: "2".into(),
            status: "pending".into(),
            kind: "job_reschedule".into(),
            job_id: job_name.clone(),
            job_config_id,
            node_name: "node-a".into(),
            priority: 50,
        };
        reschedule_eval.handle(&store)?;

        let allocations = store.allocs_for_job(&job_name)?;
        assert_eq!(allocations.len(), 2);
        assert!(allocations.iter().all(|alloc| alloc.node_id == "node-b"));

        Ok(())
    }

    /// A failed-allocation reschedule as the manager runs it: the removal
    /// spends the move budget and leaves a hold, and the delayed eval names
    /// the failed node.
    struct FailedReschedule<'a> {
        store: &'a Store,
        job: String,
        job_config_id: String,
        seq: u32,
    }

    impl<'a> FailedReschedule<'a> {
        fn submit(store: &'a Store, job_config: &JobConfig) -> R<Self> {
            store.handle_job_submit(
                job_config,
                &[Submission {
                    path: "job.ncl".into(),
                    content: "job".into(),
                    is_primary: true,
                }],
            )?;
            let job_config_id = store.get_job_config(&job_config.name)?.id();
            let mut this = Self {
                store,
                job: job_config.name.clone(),
                job_config_id,
                seq: 0,
            };
            this.eval("job_submit", "")?;
            Ok(this)
        }

        fn eval(&mut self, kind: &str, node_name: &str) -> R<String> {
            self.seq += 1;
            Eval {
                id: format!("{kind}-{}", self.seq),
                status: "pending".into(),
                kind: kind.into(),
                job_id: self.job.clone(),
                job_config_id: self.job_config_id.clone(),
                node_name: node_name.into(),
                priority: 50,
            }
            .handle(self.store)
        }

        /// Fail one allocation the way a worker report does, with a 30s
        /// backoff, and return the node it failed on.
        fn fail(&self, allocation: &crate::structs::Allocation) -> R<String> {
            let removal = self
                .store
                .remove_failed_allocation(
                    allocation.id,
                    crate::structs::stop::StopCause::Unknown,
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
                .context("allocation removed")?;
            assert!(matches!(
                removal.verdict,
                crate::storage::store::MoveVerdict::Deferred(_)
            ));
            Ok(allocation.node_id.clone())
        }

        /// The backoff runs out (the delayed eval's moment).
        fn expire_holds(&self) -> R<()> {
            self.store.write_connection()?.execute(
                "UPDATE reschedule_hold SET not_before_unix = 0 WHERE job_id = ?1;",
                [&self.job],
            )?;
            Ok(())
        }

        fn allocations(&self) -> R<Vec<crate::structs::Allocation>> {
            self.store.allocs_for_job(&self.job)
        }
    }

    fn pinned_slot_job(name: &str) -> JobConfig {
        use crate::structs::{InstanceMode, StatefulConfig, StatefulStorageConfig};
        JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks: vec![TaskConfig {
                group: "db".into(),
                resources: json!({ "memory": 512 }),
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
                ..TaskConfig::new("postgres", "service")
            }],
            version: None,
        }
    }

    /// Fill `node_id` so nothing else fits on it.
    fn fill_node(store: &Store, node_id: &str) -> R<()> {
        let filler = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: format!("filler-{node_id}"),
            tasks: vec![TaskConfig {
                resources: json!({ "memory": 8000 }),
                constraints: json!({ "node_id": node_id }),
                ..TaskConfig::new("fill", "service")
            }],
            version: None,
        };
        let placed = FailedReschedule::submit(store, &filler)?.allocations()?;
        assert_eq!(placed.len(), 1);
        assert_eq!(placed[0].node_id, node_id);
        Ok(())
    }

    /// Place the spot check's `postgres` (slot mode, local-dir storage) on
    /// node-a, have the worker report its volume mounted there, and add
    /// node-b. Returns the harness and the instance's allocation.
    /// `make_node` reporting `free_gib` of free disk, which the disk-fit
    /// check reads.
    fn make_node_with_disk(id: &str, free_gib: u64) -> Node {
        let mut node = make_node(id, "east");
        node.fingerprint_attrs["disk_available_space"] = json!((free_gib << 30).to_string());
        node
    }

    /// The spot check's `postgres` (slot mode, local-dir storage, `size =
    /// "2G"`) placed on node-a, its volume reported mounted there, and
    /// node-b added. Afterwards node-a's free disk drops to 1 GiB, under the
    /// declared size, as the rig's worker's did: the volume already exists
    /// there, so that must not keep the instance off its node. Returns the
    /// harness and the instance's allocation.
    fn pinned_postgres(store: &Store) -> R<(FailedReschedule<'_>, crate::structs::Allocation)> {
        store.insert_node(NodeMsg::from(make_node_with_disk("node-a", 50)))?;
        let db = FailedReschedule::submit(store, &pinned_slot_job("fullstack"))?;
        let allocation = db.allocations()?.pop().context("postgres placed")?;
        assert_eq!(allocation.node_id, "node-a");
        let handle = store
            .get_instance("fullstack-db-0")?
            .and_then(|instance| instance.storage_handle)
            .context("slot instance has storage")?;
        let volume_id = handle.split_once('/').context("driver/volume handle")?.1;
        store.record_volume_mounted(
            volume_id,
            "node-a",
            "/var/lib/legion/volumes/v",
            true,
            None,
            &Default::default(),
        )?;
        store.insert_node(NodeMsg::from(make_node_with_disk("node-a", 1)))?;
        store.insert_node(NodeMsg::from(make_node_with_disk("node-b", 50)))?;
        Ok((db, allocation))
    }

    /// Spot check 2026-10-12 (P0): a slot instance whose local-dir volume
    /// lives on node-a fails there. Its replacement waits out the backoff,
    /// then goes back to node-a, the only node holding its data, whether or
    /// not another node has room.
    #[test]
    fn a_failed_pinned_slot_instance_returns_to_its_node_after_the_backoff() -> R<()> {
        for other_node_full in [false, true] {
            let store = setup_store()?;
            let (mut db, allocation) = pinned_postgres(&store)?;
            if other_node_full {
                fill_node(&store, "node-b")?;
            }
            let failed_on = db.fail(&allocation)?;

            // Inside the backoff nothing is placed, by any eval.
            db.eval("job_reschedule", &failed_on)?;
            db.eval("job_reschedule", "")?;
            assert!(db.allocations()?.is_empty(), "the replacement waits");
            let waiting = store.get_instance("fullstack-db-0")?.context("instance")?;
            assert_eq!(waiting.state, crate::structs::InstanceState::Rescheduling);

            // The delayed reschedule names node-a, and node-a is still the
            // only place the replacement may go.
            db.expire_holds()?;
            let outcome = db.eval("job_reschedule", &failed_on)?;
            let placed = db.allocations()?;
            assert_eq!(
                placed.len(),
                1,
                "{outcome} (other node full: {other_node_full})"
            );
            assert_eq!(placed[0].node_id, "node-a");
            assert_eq!(placed[0].instance_id.as_deref(), Some("fullstack-db-0"));
        }
        Ok(())
    }

    /// `job run --force-reschedule` (a `job_update` eval) places a pinned
    /// slot instance inside its backoff, on its node.
    #[test]
    fn force_reschedule_places_a_pinned_slot_instance_inside_its_backoff() -> R<()> {
        let store = setup_store()?;
        let (mut db, allocation) = pinned_postgres(&store)?;
        db.fail(&allocation)?;
        db.eval("job_update", "")?;
        let placed = db.allocations()?;
        assert_eq!(placed.len(), 1);
        assert_eq!(placed[0].node_id, "node-a");
        assert!(store
            .reschedule_hold_for_instance("fullstack-db-0")?
            .is_none());
        Ok(())
    }

    /// When the pinned node is not a candidate at all, the error names it
    /// and says why, instead of blaming capacity on the other node (the
    /// rig's first message: "no available nodes … Preemption did not run").
    #[test]
    fn a_pinned_instance_whose_node_is_not_schedulable_says_so() -> R<()> {
        let store = setup_store()?;
        let (mut db, allocation) = pinned_postgres(&store)?;
        fill_node(&store, "node-b")?;
        let failed_on = db.fail(&allocation)?;
        store.set_node_availability("node-a", "drained")?;
        db.expire_holds()?;
        let error = db
            .eval("job_reschedule", &failed_on)
            .expect_err("node-a is drained and node-b is full");
        let message = format!("{error:#}");
        assert!(
            message.contains("holds node-local storage on node node-a, which is cordoned"),
            "{message}"
        );
        assert!(!message.contains("Preemption did not run"), "{message}");
        Ok(())
    }

    /// `legion instance start` on a slot instance waiting out its backoff
    /// lifts the hold, and the reschedule it queues places the instance on
    /// its node. Any other start of a slot instance is still refused.
    #[test]
    fn instance_start_retries_a_pinned_slot_instance_inside_its_backoff() -> R<()> {
        use crate::storage::store::StartInstanceOutcome;
        let store = setup_store()?;
        let (mut db, allocation) = pinned_postgres(&store)?;
        let refused = store
            .start_keyed_instance("fullstack-db-0")
            .expect_err("an active slot instance is not started");
        assert!(format!("{refused:#}").contains("slot-mode"), "{refused:#}");

        db.fail(&allocation)?;
        db.eval("job_reschedule", "")?;
        assert!(db.allocations()?.is_empty(), "held");
        assert!(matches!(
            store.start_keyed_instance("fullstack-db-0")?,
            StartInstanceOutcome::BackoffLifted
        ));
        db.eval("job_reschedule", "")?;
        let placed = db.allocations()?;
        assert_eq!(placed.len(), 1);
        assert_eq!(placed[0].node_id, "node-a");
        Ok(())
    }

    /// A stateless task is not pinned: its replacement prefers another
    /// node, and falls back to the failed one only when nothing else fits.
    #[test]
    fn a_failed_stateless_task_prefers_another_node() -> R<()> {
        let job = |name: &str| JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks: vec![TaskConfig {
                resources: json!({ "memory": 512 }),
                ..TaskConfig::new("api", "service")
            }],
            version: None,
        };
        for other_node_full in [false, true] {
            let store = setup_store()?;
            store.insert_node(NodeMsg::from(make_node("node-a", "east")))?;
            let mut api = FailedReschedule::submit(&store, &job("api"))?;
            let allocation = api.allocations()?.pop().context("api placed")?;
            assert_eq!(allocation.node_id, "node-a");
            store.insert_node(NodeMsg::from(make_node("node-b", "east")))?;
            if other_node_full {
                fill_node(&store, "node-b")?;
            }
            let failed_on = api.fail(&allocation)?;
            api.expire_holds()?;
            api.eval("job_reschedule", &failed_on)?;
            let placed = api.allocations()?;
            assert_eq!(placed.len(), 1);
            let expected = if other_node_full { "node-a" } else { "node-b" };
            assert_eq!(
                placed[0].node_id, expected,
                "other node full: {other_node_full}"
            );
        }
        Ok(())
    }

    #[test]
    fn job_submit_update_reconciles_allocations() -> R<()> {
        let store = setup_store()?;
        store.insert_node(NodeMsg::from(make_node("node-a", "east")))?;
        store.insert_node(NodeMsg::from(make_node("node-b", "west")))?;

        let job_name = "update-job".to_string();
        let base_task = TaskConfig {
            config: json!({ "message": "v1" }),
            resources: json!({ "memory": 128 }),
            constraints: json!({ "node_id": "node-a" }),
            ..TaskConfig::new("web", "service")
        };

        let job_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![base_task.clone()],
            version: None,
        };
        let job_subs = vec![Submission {
            path: "job.ncl".into(),
            content: "job".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_config, &job_subs)?;
        let job_config_id = store.get_job_config(&job_name)?.id();

        let initial_eval = Eval {
            id: "1".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_name.clone(),
            job_config_id: job_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        };
        initial_eval.handle(&store)?;

        let initial_alloc = store
            .allocs_for_job(&job_name)?
            .into_iter()
            .next()
            .context("missing initial allocation")?;
        assert_eq!(initial_alloc.node_id, "node-a");

        let unchanged_task = TaskConfig {
            config: json!({ "message": "v2" }),
            ..base_task.clone()
        };
        let unchanged_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![unchanged_task.clone()],
            version: None,
        };
        store.handle_job_submit(&unchanged_config, &job_subs)?;
        let unchanged_config_id = store.get_job_config(&job_name)?.id();
        let unchanged_eval = Eval {
            id: "2".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_name.clone(),
            job_config_id: unchanged_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        };
        unchanged_eval.handle(&store)?;

        let unchanged_allocations = store.allocs_for_job(&job_name)?;
        assert_eq!(unchanged_allocations.len(), 1);
        let unchanged_alloc = &unchanged_allocations[0];
        assert_eq!(unchanged_alloc.id, initial_alloc.id);
        assert_eq!(unchanged_alloc.node_id, "node-a");
        assert_eq!(unchanged_alloc.job_config_id, unchanged_config_id);

        let moved_task = TaskConfig {
            constraints: json!({ "node_id": "node-b" }),
            ..unchanged_task.clone()
        };
        let moved_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![moved_task.clone()],
            version: None,
        };
        store.handle_job_submit(&moved_config, &job_subs)?;
        let moved_config_id = store.get_job_config(&job_name)?.id();
        let moved_eval = Eval {
            id: "3".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_name.clone(),
            job_config_id: moved_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        };
        moved_eval.handle(&store)?;

        let moved_allocations = store.allocs_for_job(&job_name)?;
        assert_eq!(moved_allocations.len(), 1);
        let moved_alloc = &moved_allocations[0];
        assert_eq!(moved_alloc.node_id, "node-b");
        assert_ne!(moved_alloc.id, initial_alloc.id);
        assert_eq!(moved_alloc.job_config_id, moved_config_id);

        let removed_task = TaskConfig {
            count: TaskCount::new(0),
            ..moved_task
        };
        let removed_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![removed_task],
            version: None,
        };
        store.handle_job_submit(&removed_config, &job_subs)?;
        let removed_config_id = store.get_job_config(&job_name)?.id();
        let removed_eval = Eval {
            id: "4".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_name,
            job_config_id: removed_config_id,
            node_name: String::new(),
            priority: 50,
        };
        removed_eval.handle(&store)?;

        assert!(store.allocs_for_job("update-job")?.is_empty());

        Ok(())
    }

    fn single_task_job(name: &str, version: u32) -> JobConfig {
        JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks: vec![TaskConfig {
                config: json!({ "version": version }),
                ..TaskConfig::new("web", "noop")
            }],
            version: None,
        }
    }

    fn job_eval(kind: &str, job_id: &str, job_config_id: &str) -> Eval {
        Eval {
            id: format!("{kind}-{job_config_id}"),
            status: "pending".into(),
            kind: kind.into(),
            job_id: job_id.into(),
            job_config_id: job_config_id.into(),
            node_name: String::new(),
            priority: 50,
        }
    }

    fn pending_reschedules(store: &Store, job_config_id: &str) -> R<usize> {
        Ok(store
            .get_evals()?
            .iter()
            .filter(|eval| {
                eval.status == "pending"
                    && eval.kind == "job_reschedule"
                    && eval.job_config_id == job_config_id
            })
            .count())
    }

    /// SC6: a restart (`job_update`) removes every allocation before it
    /// places. When the placement then fails, a retry must be queued, or the
    /// job sits at zero with nothing re-deriving it.
    #[test]
    fn job_update_that_cannot_place_queues_a_retry() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node("node-1", "a").into())?;
        let subs = [Submission {
            path: "job.ncl".into(),
            content: "{}".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&single_task_job("restart", 1), &subs)?;
        let config_id = store.get_job_config("restart")?.id();
        job_eval("job_submit", "restart", &config_id).handle(&store)?;
        assert_eq!(store.allocs_for_job("restart")?.len(), 1);

        // The only node is cordoned, so the restart cannot place.
        store.set_node_availability("node-1", "drained")?;
        assert!(job_eval("job_update", "restart", &config_id)
            .handle(&store)
            .is_err());
        assert!(store.allocs_for_job("restart")?.is_empty());
        assert_eq!(pending_reschedules(&store, &config_id)?, 1);
        Ok(())
    }

    /// SC6 x SC5: an update for a config the job has moved past must not
    /// remove the live version and then place nothing.
    #[test]
    fn stale_job_update_leaves_the_live_version_alone() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node("node-1", "a").into())?;
        let subs = [Submission {
            path: "job.ncl".into(),
            content: "{}".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&single_task_job("stale", 1), &subs)?;
        let v1 = store.get_job_config("stale")?.id();
        job_eval("job_submit", "stale", &v1).handle(&store)?;
        store.handle_job_submit(&single_task_job("stale", 2), &subs)?;
        let v2 = store.get_job_config("stale")?.id();
        job_eval("job_submit", "stale", &v2).handle(&store)?;
        let live: Vec<i64> = store
            .allocs_for_job("stale")?
            .iter()
            .map(|a| a.id)
            .collect();
        assert_eq!(live.len(), 1);

        job_eval("job_update", "stale", &v1).handle(&store)?;
        let after: Vec<i64> = store
            .allocs_for_job("stale")?
            .iter()
            .map(|a| a.id)
            .collect();
        assert_eq!(
            after, live,
            "the v2 allocation must survive a stale v1 update"
        );
        Ok(())
    }

    /// SC6: a rollback removes the new version, then places the old one.
    /// When that placement fails, the old version must get a retry.
    #[test]
    fn rollback_that_cannot_place_queues_a_retry_of_the_old_version() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node("node-1", "a").into())?;
        let subs = [Submission {
            path: "job.ncl".into(),
            content: "{}".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&single_task_job("revert", 1), &subs)?;
        let v1 = store.get_job_config("revert")?.id();
        job_eval("job_submit", "revert", &v1).handle(&store)?;
        store.handle_job_submit(&single_task_job("revert", 2), &subs)?;
        let v2 = store.get_job_config("revert")?.id();
        let rollout = store.create_rollout(
            "revert",
            Some(&v1),
            &v2,
            "running",
            None,
            None,
            None,
            None,
            "{}",
        )?;
        // The rollout replaced v1 with v2, and then the node was cordoned.
        let v1_alloc = store.allocs_for_job_config(&v1)?[0].id;
        store.remove_allocation_by_id(v1_alloc)?;
        let v2_task = store.get_task_configs_for_job_config(&v2)?[0].0;
        store.place_alloc("node-1", v2_task)?;
        store.set_node_availability("node-1", "drained")?;

        assert!(crate::manager::rollout::stop_rollout(&store, rollout.id, true, 50).is_err());
        assert!(store.allocs_for_job("revert")?.is_empty());
        assert_eq!(pending_reschedules(&store, &v1)?, 1);
        Ok(())
    }

    /// A rollout whose target is no longer the job's latest config (a
    /// KV-triggered restart re-rendered the job past it) must not retire the
    /// newer allocations as "old": it cancels itself, and the latest config
    /// is rescheduled.
    #[test]
    fn stale_rollout_cancels_instead_of_retiring_newer_allocations() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node("node-1", "a").into())?;
        let subs = [Submission {
            path: "job.ncl".into(),
            content: "{}".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&single_task_job("stale", 1), &subs)?;
        let v1 = store.get_job_config("stale")?.id();
        store.handle_job_submit(&single_task_job("stale", 2), &subs)?;
        let v2 = store.get_job_config("stale")?.id();
        let rollout = store.create_rollout(
            "stale",
            Some(&v1),
            &v2,
            "running",
            None,
            None,
            None,
            None,
            "{}",
        )?;
        // A newer version lands without going through RunJob, and its
        // allocation is placed.
        store.handle_job_submit(&single_task_job("stale", 3), &subs)?;
        let v3 = store.get_job_config("stale")?.id();
        let v3_task = store.get_task_configs_for_job_config(&v3)?[0].0;
        let v3_alloc = store.place_alloc("node-1", v3_task)?;

        job_eval("rollout_tick", "stale", &v2).handle(&store)?;

        let rollout = store.get_rollout(rollout.id)?.context("rollout")?;
        assert_eq!(rollout.status, "cancelled");
        assert!(
            rollout.message.contains("superseded"),
            "{:?}",
            rollout.message
        );
        let remaining: Vec<i64> = store
            .allocs_for_job("stale")?
            .iter()
            .map(|allocation| allocation.id)
            .collect();
        assert_eq!(remaining, vec![v3_alloc.id]);
        assert_eq!(pending_reschedules(&store, &v3)?, 1);
        Ok(())
    }
}
