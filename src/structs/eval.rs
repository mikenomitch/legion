use rand::Rng;
use std::collections::HashSet;
use std::time::Duration;
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
            other => bail!("Unsupported eval kind: {other}"),
        }
    }

    fn handle_node_join(&self, store: &Store) -> R<String> {
        let node_id = self.node_name.trim();

        if node_id.is_empty() {
            bail!("Eval node_join missing node name");
        }

        let node_id = node_id.to_owned();
        let last_heartbeat = Self::current_timestamp();
        let nodes = store.get_nodes()?;

        if nodes.iter().any(|node| node.id == node_id) {
            store.mark_node_ready(&node_id, &last_heartbeat)?;
        } else {
            let node = Node {
                id: node_id.clone(),
                status: "ready".into(),
                pool: "default".into(),
                availability: "available".into(),
                last_heartbeat,
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
        let previous_config_id = store.previous_job_config_id(&self.job_config_id)?;
        if let Some(prev_id) = previous_config_id.as_ref() {
            if store.job_configs_schedule_equivalent(prev_id, &self.job_config_id)? {
                store.migrate_allocations_to_job_config(prev_id, &self.job_config_id)?;
                self.schedule_batch_follow_up(store)?;
                return Ok(format!("Job {} submitted with 0 placements", self.job_id));
            }
        }

        let outcome = self.schedule_job_allocations(store, PlanMode::Fresh)?;
        if !outcome.deltas.is_empty() {
            info!(?outcome.deltas, "Job submit allocation deltas");
        }

        if let Some(prev_id) = previous_config_id {
            store.remove_allocs_for_job_config(&prev_id)?;
        }

        self.schedule_batch_follow_up(store)?;

        Ok(format!(
            "Job {} submitted with {} placements",
            self.job_id,
            outcome.allocations.len()
        ))
    }

    fn handle_job_update(&self, store: &Store) -> R<String> {
        if self.job_id.trim().is_empty() {
            bail!("Eval job_update missing job id");
        }

        // Consume the replicated restart marker BEFORE executing the
        // restart (see `Store::request_job_restart`): once cleared, a later
        // failover will not regenerate this restart; if clearing is
        // rejected (foreign-lane leader under `lanes`), the eval fails
        // before any churn and is released for retry. Clearing an unset
        // marker is a no-op (this eval also runs for batch re-schedules).
        store.clear_job_restart_request(&self.job_id)?;

        store.remove_allocs_for_job(&self.job_id)?;
        let outcome = self.schedule_job_allocations(store, PlanMode::Fresh)?;
        if !outcome.deltas.is_empty() {
            info!(?outcome.deltas, "Job update allocation deltas");
        }

        self.schedule_batch_follow_up(store)?;

        Ok(format!(
            "Job {} updated with {} placements",
            self.job_id,
            outcome.allocations.len()
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

        Ok(format!("Job {} stopped", self.job_id))
    }

    fn handle_job_reschedule(&self, store: &Store) -> R<String> {
        if self.job_id.trim().is_empty() || self.job_config_id.trim().is_empty() {
            bail!("Eval job_reschedule missing identifiers");
        }

        let outcome = self.schedule_job_allocations(store, PlanMode::Reschedule)?;
        if !outcome.deltas.is_empty() {
            info!(?outcome.deltas, "Job reschedule allocation deltas");
        }

        self.schedule_batch_follow_up(store)?;

        Ok(format!(
            "Job {} rescheduled with {} placements",
            self.job_id,
            outcome.allocations.len()
        ))
    }

    fn schedule_job_allocations(&self, store: &Store, mode: PlanMode) -> R<PlanOutcome> {
        if self.job_config_id.trim().is_empty() {
            bail!("Eval missing job config id");
        }

        let scheduler = Scheduler::new(store)?;
        let excluded = self.excluded_nodes_for_mode(&mode);
        let outcome =
            self.plan_with_conflict_retries(store, &scheduler, mode, excluded.as_ref())?;

        dispatch_allocations(store, &outcome.allocations)?;
        Ok(outcome)
    }

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

        for attempt in 1..=max_plan_retries {
            match scheduler.plan_and_apply_with_exclusions(
                &self.job_config_id,
                mode.clone(),
                excluded,
            ) {
                Ok(outcome) => return Ok(outcome),
                Err(error) => {
                    if !Self::is_conflict_error(&error) {
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
        if self.job_id.trim().is_empty() || self.job_config_id.trim().is_empty() {
            return Ok(());
        }

        let tasks = store.get_task_configs_for_job_config(&self.job_config_id)?;
        let mut min_delay: Option<std::time::Duration> = None;
        for (_, task) in tasks {
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

        if let Some(delay) = min_delay {
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
            .tempdir()
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
            .tempdir()
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

    fn make_node(id: &str, zone: &str) -> Node {
        Node {
            id: id.to_string(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
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
                id_source: None,
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
            }),
            ..TaskConfig::new("db", "service")
        };
        let job_config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![task],
            version: None,
        };
        store.handle_job_submit(
            &job_config,
            &[Submission {
                path: "job.jsonnet".into(),
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
                id_source: None,
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
            }),
            ..TaskConfig::new("sandbox", "service")
        };
        let job_config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![task],
            version: None,
        };
        let subs = vec![Submission {
            path: "job.jsonnet".into(),
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
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![task],
            version: None,
        };
        let job_subs = vec![Submission {
            path: "job.jsonnet".into(),
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
        submit_eval.handle(&store)?;

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
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![base_task.clone()],
            version: None,
        };
        let job_subs = vec![Submission {
            path: "job.jsonnet".into(),
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
}
