//! The job status a reader sees, derived from the job's workload (job status
//! plan, docs/jobs-tasks-and-scheduling.md#job-status).
//!
//! The `job.status` column is a lifecycle flag (`active`, `stopped`,
//! `failed`) written on submit, stop, resubmit, and KV failure/recovery. It
//! never moves on allocation changes, so on its own it cannot say whether a
//! service is up or a batch job has finished. `GetJobInfo` and `ListJobs`
//! instead return [`derive_job_status`] over [`JobStatusFacts`] read here:
//! live allocations across every version of the job, the outcome of each task
//! in the latest version, and keyed instance states. The column itself stays
//! the lifecycle flag, and every internal gate (`stopped` jobs reject
//! instance RPCs, the reconcilers skip `stopped`/`failed` jobs) keeps reading
//! it.

use super::*;

/// The workload facts [`derive_job_status`] needs for one job.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobStatusFacts {
    /// Allocations of any version of the job that are neither terminal nor a
    /// suspended instance's held reservation: the work a worker is running or
    /// starting.
    pub live_allocations: u64,
    /// One entry per task of the job's latest version.
    pub tasks: Vec<TaskStatusFact>,
    /// Keyed instances parked by SuspendInstance.
    pub suspended_instances: u64,
}

/// One task of a job's latest version, as far as the job status cares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStatusFact {
    /// Whether the task currently asks for any allocation: `count.num > 0`,
    /// or for a keyed task, at least one pending, active, or rescheduling
    /// instance. A task scaled to zero, or a keyed task with no live
    /// instances, wants nothing and cannot hold the job in `pending`.
    pub wants_work: bool,
    /// The task has a cron `schedule`, so finishing a run is not the end.
    pub scheduled: bool,
    /// The task's `task_status` row. `None` when the row has not been seeded
    /// yet, which reads the same as `pending`.
    pub outcome: Option<TaskStatusState>,
}

/// The status `GetJobInfo` and `ListJobs` report for a job.
///
/// `lifecycle` is the stored `job.status` column; `facts` is `None` for a job
/// with no config rows yet. First match wins:
///
/// | Status      | When |
/// | ----------- | ---- |
/// | `stopped`   | the job was stopped (lifecycle) |
/// | `failed`    | a required KV dependency vanished (lifecycle) |
/// | `running`   | any allocation of any version is live |
/// | `failed`    | nothing live, and a task's last run failed |
/// | `pending`   | nothing live, and a task that wants work has not finished |
/// | `scheduled` | nothing live; a cron task finished its run and waits for the next |
/// | `complete`  | nothing live; every task that wants work finished successfully |
/// | `suspended` | nothing live or wanted; a keyed instance is suspended |
/// | `idle`      | nothing live or wanted (scaled to zero, or no keyed instances) |
pub fn derive_job_status(lifecycle: &str, facts: Option<&JobStatusFacts>) -> &'static str {
    match lifecycle {
        "stopped" => return "stopped",
        "failed" => return "failed",
        _ => {}
    }
    let Some(facts) = facts else {
        return "pending";
    };
    if facts.live_allocations > 0 {
        return "running";
    }

    let wanted = facts.tasks.iter().filter(|task| task.wants_work);
    let (mut failed, mut unfinished, mut cron_waiting, mut finished) = (false, false, false, false);
    for task in wanted {
        match task.outcome {
            Some(TaskStatusState::Failed) => failed = true,
            Some(state) if state.is_complete() => {
                if task.scheduled {
                    cron_waiting = true;
                } else {
                    finished = true;
                }
            }
            _ => unfinished = true,
        }
    }

    if failed {
        "failed"
    } else if unfinished {
        "pending"
    } else if cron_waiting {
        "scheduled"
    } else if finished {
        "complete"
    } else if facts.suspended_instances > 0 {
        "suspended"
    } else {
        "idle"
    }
}

impl Store {
    /// [`Store::get_job`] with `status` replaced by [`derive_job_status`].
    pub fn get_job_with_status(&self, name: &str) -> R<Job> {
        let mut job = self.get_job(name)?;
        let facts = self.job_status_facts(Some(&job.id))?;
        job.status = derive_job_status(&job.status, facts.get(&job.id)).to_string();
        Ok(job)
    }

    /// [`Store::get_jobs`] with every `status` replaced by
    /// [`derive_job_status`]. Three aggregate reads cover every job, so the
    /// cost is one pass over allocations, latest-version tasks, and
    /// instances, not a query per job.
    pub fn get_jobs_with_status(&self) -> R<Vec<Job>> {
        let mut jobs = self.get_jobs()?;
        let facts = self.job_status_facts(None)?;
        for job in &mut jobs {
            job.status = derive_job_status(&job.status, facts.get(&job.id)).to_string();
        }
        Ok(jobs)
    }

    /// Read [`JobStatusFacts`] for one job (`Some(id)`) or every job (`None`),
    /// keyed by job id. A job with no config rows has no entry.
    pub fn job_status_facts(&self, job_id: Option<&str>) -> R<HashMap<String, JobStatusFacts>> {
        let conn = self.read_connection()?;
        let mut facts: HashMap<String, JobStatusFacts> = HashMap::new();
        // The single-job read gets a plain equality so SQLite walks the
        // job_config/allocation/instance indexes for that job instead of
        // scanning every row (an `?1 IS NULL OR ...` filter defeats them).
        // `GetJobInfo` is polled by the UI's job page, so this matters.
        let (job_clause, config_clause, instance_clause) = match job_id {
            Some(_) => ("WHERE j.id = ?1", "AND jc.job_id = ?1", "AND job_id = ?1"),
            None => ("", "", ""),
        };
        let job_params: Vec<&dyn rusqlite::ToSql> = match &job_id {
            Some(id) => vec![id],
            None => Vec::new(),
        };

        let mut tasks = conn.prepare_cached(&format!(
            "SELECT tc.job_id, tc.count, tc.schedule IS NOT NULL, ts.status,
                    CASE WHEN json_extract(tc.stateful, '$.mode') = 'keyed' THEN
                        (SELECT COUNT(*) FROM instance i
                         WHERE i.job_id = tc.job_id AND i.task_name = tc.name
                           AND i.state IN ('pending', 'active', 'rescheduling'))
                    END
             FROM job j
             INNER JOIN task_config tc ON tc.job_config_id = {LATEST_CONFIG_FOR_JOB}
             LEFT JOIN task_status ts ON ts.task_config_id = tc.id
             {job_clause};"
        ))?;
        let rows = tasks.query_map(job_params.as_slice(), |row| {
            let job_id: String = row.get(0)?;
            let count: i64 = row.get(1)?;
            let scheduled: bool = row.get(2)?;
            let outcome: Option<String> = row.get(3)?;
            let live_keyed_instances: Option<i64> = row.get(4)?;
            let wants_work = match live_keyed_instances {
                Some(live) => live > 0,
                None => count > 0,
            };
            Ok((
                job_id,
                TaskStatusFact {
                    wants_work,
                    scheduled,
                    // An unreadable status reads as unfinished rather than
                    // failing the whole list.
                    outcome: outcome.and_then(|raw| raw.parse().ok()),
                },
            ))
        })?;
        for row in rows {
            let (job_id, task) = row?;
            facts.entry(job_id).or_default().tasks.push(task);
        }

        // The terminal and held-reservation statuses are inlined from the
        // constants so the per-job statement keeps `?1` as its only
        // parameter.
        let mut live = conn.prepare_cached(&format!(
            "SELECT jc.job_id, COUNT(*)
             FROM allocation a
             INNER JOIN job_config jc ON jc.id = a.job_config_id
             WHERE a.status NOT IN ('{complete}', '{failed}', '{suspended}') {config_clause}
             GROUP BY jc.job_id;",
            complete = crate::constants::ALLOCATION_STATUS_COMPLETE,
            failed = crate::constants::ALLOCATION_STATUS_FAILED,
            suspended = crate::constants::ALLOCATION_STATUS_SUSPENDED,
        ))?;
        let rows = live.query_map(job_params.as_slice(), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (job_id, count) = row?;
            facts.entry(job_id).or_default().live_allocations = count as u64;
        }

        let mut suspended = conn.prepare_cached(&format!(
            "SELECT job_id, COUNT(*) FROM instance
             WHERE state = 'suspended' {instance_clause}
             GROUP BY job_id;"
        ))?;
        let rows = suspended.query_map(job_params.as_slice(), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (job_id, count) = row?;
            facts.entry(job_id).or_default().suspended_instances = count as u64;
        }

        Ok(facts)
    }
}

#[cfg(test)]
mod job_status_tests {
    use super::*;

    fn task(wants_work: bool, outcome: Option<TaskStatusState>) -> TaskStatusFact {
        TaskStatusFact {
            wants_work,
            scheduled: false,
            outcome,
        }
    }

    fn cron(outcome: Option<TaskStatusState>) -> TaskStatusFact {
        TaskStatusFact {
            scheduled: true,
            ..task(true, outcome)
        }
    }

    fn facts(live: u64, tasks: Vec<TaskStatusFact>) -> JobStatusFacts {
        JobStatusFacts {
            live_allocations: live,
            tasks,
            suspended_instances: 0,
        }
    }

    #[test]
    fn lifecycle_stopped_and_failed_win_over_live_work_test() {
        let busy = facts(3, vec![task(true, Some(TaskStatusState::Running))]);
        assert_eq!(derive_job_status("stopped", Some(&busy)), "stopped");
        assert_eq!(derive_job_status("failed", Some(&busy)), "failed");
    }

    #[test]
    fn any_live_allocation_is_running_test() {
        let busy = facts(1, vec![task(true, Some(TaskStatusState::Failed))]);
        assert_eq!(derive_job_status("active", Some(&busy)), "running");
    }

    #[test]
    fn finished_batch_job_is_complete_test() {
        let done = facts(
            0,
            vec![
                task(true, Some(TaskStatusState::Success)),
                task(true, Some(TaskStatusState::Complete)),
            ],
        );
        assert_eq!(derive_job_status("active", Some(&done)), "complete");
    }

    #[test]
    fn unplaced_or_unseeded_work_is_pending_test() {
        let waiting = facts(
            0,
            vec![
                task(true, Some(TaskStatusState::Success)),
                task(true, Some(TaskStatusState::Pending)),
            ],
        );
        assert_eq!(derive_job_status("active", Some(&waiting)), "pending");
        // A service whose allocation was lost still reads `running` on its
        // task row, but with nothing live it is waiting for placement.
        let lost = facts(0, vec![task(true, Some(TaskStatusState::Running))]);
        assert_eq!(derive_job_status("active", Some(&lost)), "pending");
        assert_eq!(
            derive_job_status("active", Some(&facts(0, vec![task(true, None)]))),
            "pending"
        );
        assert_eq!(derive_job_status("active", None), "pending");
    }

    #[test]
    fn a_failed_task_beats_its_blocked_dependents_test() {
        // `b` depends on `a` succeeding and will never be placed; the failure
        // is the thing to look at, not the wait.
        let broken = facts(
            0,
            vec![
                task(true, Some(TaskStatusState::Failed)),
                task(true, Some(TaskStatusState::Pending)),
            ],
        );
        assert_eq!(derive_job_status("active", Some(&broken)), "failed");
    }

    #[test]
    fn cron_between_runs_is_scheduled_test() {
        let between = facts(
            0,
            vec![
                cron(Some(TaskStatusState::Success)),
                task(true, Some(TaskStatusState::Success)),
            ],
        );
        assert_eq!(derive_job_status("active", Some(&between)), "scheduled");
        let failed_run = facts(0, vec![cron(Some(TaskStatusState::Failed))]);
        assert_eq!(derive_job_status("active", Some(&failed_run)), "failed");
    }

    #[test]
    fn nothing_wanted_is_idle_or_suspended_test() {
        let scaled_to_zero = facts(0, vec![task(false, Some(TaskStatusState::Pending))]);
        assert_eq!(derive_job_status("active", Some(&scaled_to_zero)), "idle");
        let parked = JobStatusFacts {
            suspended_instances: 2,
            ..scaled_to_zero
        };
        assert_eq!(derive_job_status("active", Some(&parked)), "suspended");
    }
}
