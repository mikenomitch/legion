//! Store reads behind `GetCapacityDemand` (docs/autoscaling.md, "Capacity
//! demand"; `crate::manager::capacity_demand`).
//!
//! The pending work node autoscaling sizes against: every task of an
//! active job that is short of its effective count, with the facts the
//! simulation needs about it. One cheap SQL pass finds the candidate jobs
//! (a superset: any task of the latest config with fewer allocations of
//! that config than its count, or a keyed instance without one), and only
//! those are read in detail, so a cluster whose jobs are all placed pays
//! for one query.

use super::*;

/// One task of an active job that is short of its count.
#[derive(Debug, Clone)]
pub struct PendingTaskRow {
    /// The job (canonical id, `namespace/name` form for a non-default
    /// namespace).
    pub job_id: String,
    /// The config the pending replicas would run (the job's latest).
    pub job_config_id: String,
    /// The task as the latest config defines it.
    pub task: TaskConfig,
    /// The count the task should have: its effective count
    /// ([`Store::effective_task_count`]), or for a keyed task its live
    /// instances.
    pub desired: u32,
    /// Its allocations, across every config version (an old version's
    /// allocation occupies capacity too; calls entry 35).
    pub placed: u32,
    /// `desired - placed`.
    pub pending: u32,
    /// The nodes holding the task's allocations, for `distinct`.
    pub placed_nodes: Vec<String>,
}

/// The pending work, and the short jobs left out of it.
#[derive(Debug, Clone, Default)]
pub struct PendingWork {
    /// Every short task.
    pub tasks: Vec<PendingTaskRow>,
    /// Short jobs with an eval still pending or in flight: the scheduler
    /// has not yet tried the capacity that exists, so they are not demand.
    pub awaiting_scheduler: Vec<String>,
}

impl Store {
    /// The pending work node autoscaling sizes against.
    ///
    /// - Jobs: active (not stopped or failed). A job with an eval pending
    ///   or in flight is listed in [`PendingWork::awaiting_scheduler`]
    ///   instead; a blocked eval is exactly the work this is for.
    /// - Tasks: the latest config's. Left out: `count.per` tasks (they
    ///   follow nodes rather than ask for them), cron tasks, and batch tasks
    ///   that already finished.
    /// - Counts: the effective count through [`Self::effective_task_count`]
    ///   against allocations across versions; keyed tasks count their
    ///   active instances that have no allocation, as the capacity pass
    ///   does.
    #[instrument(err, skip_all)]
    pub fn pending_capacity_work(&self) -> R<PendingWork> {
        let candidates: Vec<(String, String, bool)> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(&format!(
                "SELECT j.id, latest.id,
                    EXISTS (
                      SELECT 1 FROM eval e
                      WHERE e.job_id = j.id AND e.status IN ('pending', 'in_flight')
                    )
                 FROM job j
                 INNER JOIN job_config latest ON latest.id = {LATEST_CONFIG_FOR_JOB}
                 WHERE j.status NOT IN ('stopped', 'failed')
                   AND EXISTS (
                     SELECT 1 FROM task_config tc
                     LEFT JOIN task_scale ts
                       ON ts.job_id = tc.job_id AND ts.task_group = tc.group_name
                      AND ts.task_name = tc.name AND ts.base_count = tc.count
                     WHERE tc.job_config_id = latest.id
                       AND (tc.count_per IS NULL OR tc.count_per = '')
                       AND (
                         (SELECT COUNT(*) FROM allocation a WHERE a.task_config_id = tc.id)
                           < COALESCE(ts.count, tc.count)
                         OR EXISTS (
                           SELECT 1 FROM instance i
                           WHERE i.job_id = tc.job_id AND i.group_name = tc.group_name
                             AND i.task_name = tc.name AND i.mode = 'keyed'
                             AND i.current_alloc_id IS NULL
                             AND i.state NOT IN ('terminated', 'stopped', 'suspended')
                             AND COALESCE(i.pending_op, '') != 'fork'
                         )
                       )
                   )
                 ORDER BY j.id;"
            ))?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            Self::collect_rows(rows)?
        };

        let mut work = PendingWork::default();
        for (job_id, latest, awaiting) in candidates {
            let rows = self.pending_tasks_of(&job_id, &latest)?;
            if rows.is_empty() {
                continue;
            }
            if awaiting {
                work.awaiting_scheduler.push(job_id);
                continue;
            }
            work.tasks.extend(rows);
        }
        Ok(work)
    }

    /// The short tasks of one job, against its config `latest`.
    fn pending_tasks_of(&self, job_id: &str, latest: &str) -> R<Vec<PendingTaskRow>> {
        let tasks = self.get_task_configs_for_job_config(latest)?;
        let statuses = self.task_statuses_for_job_config(latest)?;
        // Every allocation of the job, any version: (group, task) -> nodes.
        let mut placed: HashMap<(String, String), Vec<String>> = HashMap::new();
        let unplaced_keyed: HashMap<(String, String), u32> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare_cached(
                "SELECT tc.group_name, tc.name, a.node_id
                 FROM job_config jc
                 INNER JOIN allocation a ON a.job_config_id = jc.id
                 INNER JOIN task_config tc ON tc.id = a.task_config_id
                 WHERE jc.job_id = ?1;",
            )?;
            let rows = stmt.query_map([job_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?;
            for (group, task, node) in Self::collect_rows(rows)? {
                placed.entry((group, task)).or_default().push(node);
            }
            let mut stmt = conn.prepare_cached(
                "SELECT group_name, task_name, COUNT(*) FROM instance
                 WHERE job_id = ?1 AND mode = 'keyed' AND current_alloc_id IS NULL
                   AND state NOT IN ('terminated', 'stopped', 'suspended')
                   AND COALESCE(pending_op, '') != 'fork'
                 GROUP BY group_name, task_name;",
            )?;
            let rows = stmt.query_map([job_id], |row| {
                Ok((
                    (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
                    row.get::<_, u32>(2)?,
                ))
            })?;
            Self::collect_rows(rows)?.into_iter().collect()
        };

        let mut out = Vec::new();
        for (task_id, task) in tasks {
            if task.count.per.is_some() || task.schedule.is_some() {
                continue;
            }
            let key = (task.group.clone(), task.name.clone());
            let nodes = placed.get(&key).cloned().unwrap_or_default();
            let placed_count = u32::try_from(nodes.len()).unwrap_or(u32::MAX);
            let keyed = task
                .stateful
                .as_ref()
                .is_some_and(|stateful| stateful.mode == InstanceMode::Keyed);
            let (desired, pending) = if keyed {
                let waiting = unplaced_keyed.get(&key).copied().unwrap_or(0);
                (placed_count.saturating_add(waiting), waiting)
            } else {
                if task.lifecycle == TaskLifecycle::Batch
                    && statuses.get(&task_id).is_some_and(|status| {
                        matches!(
                            status,
                            TaskStatusState::Success
                                | TaskStatusState::Complete
                                | TaskStatusState::Failed
                        )
                    })
                {
                    continue;
                }
                let desired =
                    self.effective_task_count(job_id, &task.group, &task.name, task.count.num)?;
                (desired, desired.saturating_sub(placed_count))
            };
            if pending == 0 {
                continue;
            }
            out.push(PendingTaskRow {
                job_id: job_id.to_string(),
                job_config_id: latest.to_string(),
                task,
                desired,
                placed: placed_count,
                pending,
                placed_nodes: nodes,
            });
        }
        Ok(out)
    }
}
