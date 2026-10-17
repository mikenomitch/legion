//! Gang waits: when a gang started waiting for its quorum, and when it gives
//! up (docs/gang-scheduling.md, "Placement timeout").
//!
//! A gang short of its quorum makes its plan place nothing, and its eval
//! blocks like any other ([`super::blocked_evals`]). A blocked eval's row is
//! a poor clock, though: it is woken back to `pending` by every capacity
//! event, re-planned, and blocked again, and a woken eval can be collapsed
//! into another of its job's evals. So the start of a gang's wait lives in a
//! row of its own, one per job config, in the `gang_wait` table:
//!
//! - **Started** by the entry that first blocks the gang's eval
//!   ([`Store::write_blocked_eval`]), and kept as the gang blocks again: the
//!   insert never overwrites, so `since` is the start of the whole wait.
//!   When the gang sets `placementTimeout`, `deadline` is `since` plus it.
//! - **Ended** by any eval of the config that succeeds (a plan that leaves
//!   a gang short fails its eval, so success means the quorum placed or
//!   nothing was missing; [`Store::delete_job_blocked_evals_tx`]), a job
//!   stop or restart (`job_stop`, `job_update`), a destroy, or the timeout
//!   itself.
//! - **Enforced** by the scheduler-lane leader: its blocked-eval waker
//!   wakes the blocked eval of a gang past its deadline
//!   ([`Store::wake_timed_out_gangs`]), and the eval worker, finding the
//!   gang still short, fails the job instead of blocking it again
//!   ([`Store::time_out_gang`]).
//!
//! Scheduler lane, like `eval`. Rows are few (one per waiting gang job).

use super::*;

/// One waiting gang, as its row records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GangWait {
    pub job_config_id: String,
    pub job_id: String,
    pub task_name: String,
    /// When the gang first blocked short of its quorum, unix seconds.
    pub since_unix: i64,
    /// `since_unix + placementTimeout`, or `None` when the gang waits
    /// indefinitely.
    pub deadline_unix: Option<i64>,
}

impl Store {
    /// Start (or keep) the wait of the gang `task_name` of `job_config_id`,
    /// inside the write that blocks its eval, and return the row as it now
    /// stands. An existing row is kept as is: `since` is the start of the
    /// whole wait, however often the gang blocks again.
    pub(crate) fn note_gang_wait_tx(
        conn: &Connection,
        job_config_id: &str,
        job_id: &str,
        task_name: &str,
        placement_timeout_secs: Option<u64>,
    ) -> R<GangWait> {
        let now = Self::now_unix();
        let deadline = placement_timeout_secs
            .map(|secs| now.saturating_add(i64::try_from(secs).unwrap_or(i64::MAX)));
        conn.prepare_cached(
            "INSERT INTO gang_wait (job_config_id, job_id, task_name, since_unix, deadline_unix)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(job_config_id) DO NOTHING;",
        )?
        .execute(params![job_config_id, job_id, task_name, now, deadline])?;
        Self::gang_wait_on(conn, job_config_id)?
            .with_context(|| format!("gang wait for {job_config_id} vanished mid-write"))
    }

    fn gang_wait_on(conn: &Connection, job_config_id: &str) -> R<Option<GangWait>> {
        Ok(conn
            .prepare_cached(
                "SELECT job_config_id, job_id, task_name, since_unix, deadline_unix
                 FROM gang_wait WHERE job_config_id = ?1;",
            )?
            .query_row([job_config_id], Self::gang_wait_from_row)
            .optional()?)
    }

    fn gang_wait_from_row(row: &Row<'_>) -> rusqlite::Result<GangWait> {
        Ok(GangWait {
            job_config_id: row.get(0)?,
            job_id: row.get(1)?,
            task_name: row.get(2)?,
            since_unix: row.get(3)?,
            deadline_unix: row.get(4)?,
        })
    }

    /// The wait of a gang of `job_config_id`, if one is waiting.
    pub fn gang_wait(&self, job_config_id: &str) -> R<Option<GangWait>> {
        let conn = self.read_connection()?;
        Self::gang_wait_on(&conn, job_config_id)
    }

    /// End every wait of `job_id`'s gangs: the job stopped, restarted, or
    /// failed, so whatever it waits for next starts a new clock.
    pub fn clear_gang_waits_for_job(&self, job_id: &str) -> R<()> {
        let waiting: bool = self.read_connection()?.query_row(
            "SELECT EXISTS (SELECT 1 FROM gang_wait WHERE job_id = ?1);",
            [job_id],
            |row| row.get(0),
        )?;
        if !waiting {
            return Ok(());
        }
        let job_id = job_id.to_owned();
        self.replicated(Lane::Scheduler, "clear_gang_waits_for_job", move |s| {
            let conn = s.write_connection()?;
            Self::delete_gang_waits_for_job_tx(&conn, &job_id)
        })
    }

    pub(crate) fn delete_gang_waits_for_job_tx(conn: &Connection, job_id: &str) -> R<()> {
        conn.prepare_cached("DELETE FROM gang_wait WHERE job_id = ?1;")?
            .execute([job_id])?;
        Ok(())
    }

    /// The waits whose `placementTimeout` has run out.
    pub fn expired_gang_waits(&self) -> R<Vec<GangWait>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT job_config_id, job_id, task_name, since_unix, deadline_unix
             FROM gang_wait
             WHERE deadline_unix IS NOT NULL AND deadline_unix <= ?1
             ORDER BY deadline_unix;",
        )?;
        let rows = stmt.query_map([Self::now_unix()], Self::gang_wait_from_row)?;
        Self::collect_rows(rows)
    }

    /// The leader's timer (run by the blocked-eval waker): return the
    /// blocked eval of every gang past its deadline to `pending` (cause
    /// `timeout`), so the eval worker re-plans it once more and fails the job
    /// if the gang is still short ([`Self::time_out_gang`]). A wait whose
    /// config is no longer wanted (the job moved on, stopped, or failed) is
    /// dropped instead. Returns how many evals woke.
    #[instrument(err, skip_all)]
    pub fn wake_timed_out_gangs(&self) -> R<usize> {
        let expired = self.expired_gang_waits()?;
        if expired.is_empty() {
            return Ok(0);
        }
        let blocked = self.blocked_evals_by_job()?;
        let mut wakes: Vec<(String, String, String, &'static str, String)> = Vec::new();
        let mut stale: Vec<String> = Vec::new();
        for wait in &expired {
            // Nothing left to time out: the config is no longer wanted, or no
            // eval of the job is waiting to place it at all.
            let queued: bool = self.read_connection()?.query_row(
                "SELECT EXISTS (
                     SELECT 1 FROM eval
                     WHERE job_id = ?1 AND status IN ('pending', 'blocked')
                 );",
                [&wait.job_id],
                |row| row.get(0),
            )?;
            if !queued
                || self
                    .job_config_unwanted_reason(&wait.job_config_id)?
                    .is_some()
            {
                stale.push(wait.job_config_id.clone());
                continue;
            }
            // An eval that is not blocked (pending, or running right now)
            // checks the deadline itself when its plan comes up short.
            if let Some(eval) = blocked
                .get(&wait.job_id)
                .filter(|eval| eval.job_config_id == wait.job_config_id)
            {
                wakes.push((
                    eval.id.clone(),
                    eval.job_id.clone(),
                    eval.key.clone(),
                    "timeout",
                    String::new(),
                ));
            }
        }
        let woken = wakes.len();
        if !wakes.is_empty() || !stale.is_empty() {
            self.replicated(Lane::Scheduler, "wake_timed_out_gangs", move |s| {
                if !stale.is_empty() {
                    let conn = s.write_connection()?;
                    for job_config_id in &stale {
                        conn.prepare_cached("DELETE FROM gang_wait WHERE job_config_id = ?1;")?
                            .execute([job_config_id])?;
                    }
                }
                s.unblock_evals_apply(wakes)
            })?;
        }
        if woken > 0 {
            crate::manager::telemetry::metrics()
                .evals_unblocked_total
                .inc_by(&["timeout"], woken as u64);
        }
        Ok(woken)
    }

    /// The gang of `eval`'s config is still short and its `placementTimeout`
    /// has run out: fail the job (docs/gang-scheduling.md, "Placement
    /// timeout"). The job is marked `failed` first, so no plan racing this
    /// one can place it again (a failed gang job places nothing until `job
    /// run` restarts it), then every allocation of the job stops with cause
    /// `gang` (the survivors of a top-up), and the wait ends. Returns the
    /// message the eval fails with, or `None` when the gang's wait has not
    /// run out.
    #[instrument(err, skip_all, fields(eval_id = %eval.id, job_id = %eval.job_id))]
    pub fn time_out_gang(&self, eval: &Eval, report: &Value) -> R<Option<String>> {
        let Some(wait) = self.gang_wait(&eval.job_config_id)? else {
            return Ok(None);
        };
        let Some(deadline) = wait.deadline_unix else {
            return Ok(None);
        };
        if deadline > Self::now_unix() {
            return Ok(None);
        }
        let waited = Self::now_unix().saturating_sub(wait.since_unix).max(0) as u64;
        let timeout = report
            .get("placementTimeoutSeconds")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| deadline.saturating_sub(wait.since_unix).max(0) as u64);
        let message = format!(
            "Gang {} did not reach its quorum within placementTimeout {} (waited {}); the job \
             is failed and places nothing until `legion job run` restarts it. Last attempt: {}",
            wait.task_name,
            humantime::format_duration(Duration::from_secs(timeout)),
            humantime::format_duration(Duration::from_secs(waited)),
            report
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded"),
        );
        self.mark_job_failed(&eval.job_id)?;
        let allocations = self.allocs_for_job(&eval.job_id)?;
        if !allocations.is_empty() {
            self.remove_allocations(&allocations, StopCause::Gang)?;
        }
        self.clear_gang_waits_for_job(&eval.job_id)?;
        self.events().job_gang_timeout(
            &eval.job_id,
            &wait.task_name,
            timeout,
            waited,
            allocations.len(),
            &message,
        );
        crate::manager::telemetry::metrics()
            .gang_outcomes_total
            .inc(&["timeout"]);
        warn!(
            job_id = %eval.job_id,
            task = %wait.task_name,
            timeout_secs = timeout,
            waited_secs = waited,
            stopped = allocations.len(),
            "Gang placement timed out; job failed"
        );
        Ok(Some(message))
    }
}
