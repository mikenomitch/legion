//! Reschedule backoff (docs/jobs-tasks-and-scheduling.md, "Manager-side move
//! logic").
//!
//! When a worker reports a failed allocation (or the memory pressure
//! response evicts one for outgrowing its reservation), the task's
//! `on_unhealthy` move budget decides what happens next: the replacement is
//! deferred by a backoff that doubles per consecutive failure (a failure
//! continues the chain while it comes within `move_period` of the previous
//! replacement's release), or the job fails once `move_count` moves happened
//! inside `move_period`.
//!
//! That decision is made here, inside the scheduler-lane op that removes the
//! allocation, and recorded as a **hold** row in `reschedule_hold`:
//!
//! - **The planner honors it.** A `Reschedule` plan leaves one replacement
//!   of the task unplaced per active hold, and the keyed and slot binders
//!   prefer instances that are not held. So the backoff applies to every
//!   eval that could re-place the task (the capacity pass, a node join, the
//!   divergent-leadership regeneration), not only to the delayed
//!   `job_reschedule` the failure queues. Before holds, any of those placed
//!   the replacement at once, and a task that could never start was
//!   re-placed about once a minute whatever its delay said.
//! - **It is replicated.** Any manager can show it (`instance get`, `job
//!   info`), and the rows are the task's move history (the `move_count`
//!   budget and the backoff chain), so the backoff is the same whichever
//!   manager handles the next failure and survives a restart or a leader
//!   change.
//! - **It can be lifted.** A `job_update` eval (`job run --force-reschedule`,
//!   restarting a stopped or failed job) deletes the job's holds and with
//!   them its move history; StartInstance lifts one instance's hold (keyed
//!   or slot) and keeps the history.

use super::*;

/// Ceiling on the reschedule backoff, unless `move_delay` itself is longer.
pub const MOVE_DELAY_MAX: Duration = Duration::from_secs(30 * 60);

/// How much of a failure reason a hold, its event, and the operator-facing
/// detail keep. The full reason stays on the `allocation.failed` event.
const RESCHEDULE_REASON_MAX_CHARS: usize = 400;

/// The wait before rescheduling a failed allocation, given how many
/// failures came before it in its chain: `base`, doubled per earlier
/// failure, capped at [`MOVE_DELAY_MAX`] (or `base` when it is larger).
pub fn move_backoff(base: Duration, earlier_failures: usize) -> Duration {
    let factor = 1u32 << earlier_failures.min(31);
    base.saturating_mul(factor).min(MOVE_DELAY_MAX.max(base))
}

/// A failure reason on one line, at most `max_chars` characters: line
/// breaks (a stderr tail) become ` / `, and a cut reason ends in `...`.
pub fn summarize_reason(reason: &str, max_chars: usize) -> String {
    let one_line = reason
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" / ");
    if one_line.chars().count() <= max_chars {
        return one_line;
    }
    let mut cut: String = one_line.chars().take(max_chars.saturating_sub(3)).collect();
    cut.push_str("...");
    cut
}

/// `30s`, `4m`, `1h30m`: a backoff as an operator reads it.
pub(crate) fn short_duration(secs: u64) -> String {
    let (hours, minutes, seconds) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    let mut out = String::new();
    if hours > 0 {
        out.push_str(&format!("{hours}h"));
    }
    if minutes > 0 {
        out.push_str(&format!("{minutes}m"));
    }
    if seconds > 0 || out.is_empty() {
        out.push_str(&format!("{seconds}s"));
    }
    out
}

fn rfc3339(unix: i64) -> String {
    OffsetDateTime::from_unix_timestamp(unix)
        .ok()
        .and_then(|time| time.format(&Rfc3339).ok())
        .unwrap_or_else(|| unix.to_string())
}

/// A task's move budget (`on_unhealthy.move_*`), as the removal op needs
/// it. Carried in full by a forwarded op, so the executor never derives it
/// from its own replica.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MovePolicy {
    pub move_delay_ms: u64,
    pub move_count: Option<u32>,
    pub move_period_ms: u64,
}

impl From<&crate::structs::TaskOnUnhealthyConfig> for MovePolicy {
    fn from(config: &crate::structs::TaskOnUnhealthyConfig) -> Self {
        Self {
            move_delay_ms: config.move_delay.as_millis() as u64,
            move_count: config.move_count,
            move_period_ms: config.move_period.as_millis() as u64,
        }
    }
}

/// What the removal op needs to spend the move budget: the policy and why
/// the allocation failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailedMove {
    pub policy: MovePolicy,
    pub reason: String,
}

/// A failed allocation's deferred replacement (one `reschedule_hold` row).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RescheduleHold {
    /// The allocation that failed.
    pub allocation_id: i64,
    pub job_id: String,
    pub job_config_id: String,
    pub task_config_id: i64,
    pub task_name: String,
    pub group: String,
    /// The stateful instance waiting for the replacement; `None` for a
    /// stateless task.
    pub instance_id: Option<String>,
    /// The node the allocation failed on.
    pub node_id: String,
    /// How many times in a row this instance (or, for a stateless task,
    /// the task) failed without a replacement surviving `move_period`,
    /// counting this failure. The backoff doubles per earlier one.
    pub attempt: u32,
    pub delay_secs: u64,
    /// When the replacement may be placed. Zero once lifted.
    pub not_before_unix: i64,
    /// The failure reason, on one line and cut to a few hundred characters.
    pub reason: String,
    pub created_at_unix: i64,
}

impl RescheduleHold {
    /// When the replacement may be placed, RFC 3339.
    pub fn next_attempt_at(&self) -> String {
        rfc3339(self.not_before_unix)
    }

    /// The operator's one-liner: `attempt 4 failed (artifact staging failed:
    /// ...); next attempt at 2026-10-12T02:31:05Z (backoff 4m)`.
    pub fn summary(&self) -> String {
        let reason = summarize_reason(&self.reason, 200);
        format!(
            "attempt {} failed ({}); next attempt at {} (backoff {})",
            self.attempt,
            if reason.is_empty() {
                "no reason reported"
            } else {
                reason.as_str()
            },
            self.next_attempt_at(),
            short_duration(self.delay_secs)
        )
    }

    /// How an operator retries before the backoff runs out.
    pub fn retry_hint(&self) -> String {
        match self.instance_id.as_deref() {
            Some(instance_id) => format!("`legion instance start {instance_id}` retries now"),
            None => "`legion job run <spec> --force-reschedule` retries now".to_string(),
        }
    }
}

/// What the move budget decided for a failed allocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MoveVerdict {
    /// The replacement waits out the hold, then a delayed `job_reschedule`
    /// places it.
    Deferred(RescheduleHold),
    /// `move_count` moves already happened inside `move_period`: the job
    /// fails instead of moving the task again.
    Exhausted {
        task_name: String,
        moves: u32,
        move_count: u32,
        move_period_secs: u64,
        reason: String,
    },
}

/// A removed failed allocation and the move budget's verdict on it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FailedRemoval {
    pub allocation: Allocation,
    pub verdict: MoveVerdict,
}

const HOLD_COLUMNS: &str = "allocation_id, job_id, job_config_id, task_config_id, task_name, \
     group_name, instance_id, node_id, attempt, delay_secs, not_before_unix, reason, \
     created_at_unix";

fn hold_from_row(row: &Row<'_>) -> Result<RescheduleHold, rusqlite::Error> {
    Ok(RescheduleHold {
        allocation_id: row.get(0)?,
        job_id: row.get(1)?,
        job_config_id: row.get(2)?,
        task_config_id: row.get(3)?,
        task_name: row.get(4)?,
        group: row.get(5)?,
        instance_id: row.get(6)?,
        node_id: row.get(7)?,
        attempt: row.get(8)?,
        delay_secs: row.get::<_, i64>(9)?.max(0) as u64,
        not_before_unix: row.get(10)?,
        reason: row.get(11)?,
        created_at_unix: row.get(12)?,
    })
}

impl Store {
    /// Remove a failed allocation and spend its task's move budget, in one
    /// scheduler-lane op: the removal, the verdict, and (when the
    /// replacement is deferred) the hold the planner honors commit together,
    /// so no eval can re-place the task between the removal and the hold.
    ///
    /// Returns `None` when the allocation is already gone (a re-sent
    /// report), which spends nothing. The caller emits the events and
    /// queues the delayed `job_reschedule` (or fails the job) from the
    /// verdict. Forwards to the scheduler-lane leader from a follower, like
    /// [`Self::remove_allocation_by_id_explained`]: worker reports land on
    /// whichever manager the worker dials.
    pub fn remove_failed_allocation(
        &self,
        allocation_id: i64,
        cause: StopCause,
        explanation: Option<StopExplanation>,
        failure: FailedMove,
    ) -> R<Option<FailedRemoval>> {
        match self.remove_failed_allocation_local(
            allocation_id,
            cause,
            explanation.clone(),
            failure.clone(),
        ) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::RemoveFailedAllocation {
                        allocation_id,
                        cause,
                        explanation,
                        failure,
                    },
                    err,
                ),
            other => other,
        }
    }

    pub(crate) fn remove_failed_allocation_local(
        &self,
        allocation_id: i64,
        cause: StopCause,
        explanation: Option<StopExplanation>,
        failure: FailedMove,
    ) -> R<Option<FailedRemoval>> {
        self.replicated(Lane::Scheduler, "remove_failed_allocation", move |s| {
            s.remove_failed_allocation_apply(allocation_id, cause, explanation.as_ref(), &failure)
        })
    }

    fn remove_failed_allocation_apply(
        &self,
        allocation_id: i64,
        cause: StopCause,
        explanation: Option<&StopExplanation>,
        failure: &FailedMove,
    ) -> R<Option<FailedRemoval>> {
        let Some(allocation) = self.get_allocation_by_id(allocation_id)? else {
            return Ok(None);
        };
        let now = Self::now_unix();
        let policy = &failure.policy;
        let period_secs = (policy.move_period_ms / 1000) as i64;

        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        // Expired rows are neither holds nor move history any more. Pruned
        // here, like the stop notices: a table no failure writes to is not
        // growing.
        tx.execute(
            "DELETE FROM reschedule_hold WHERE expires_at_unix < ?1;",
            [now],
        )?;
        let (task_name, group): (String, String) = tx
            .query_row(
                "SELECT name, group_name FROM task_config WHERE id = ?1;",
                [allocation.task_config_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .unwrap_or_default();
        // Moves of this task inside `move_period` (inclusive, as the
        // in-memory history it replaces counted them): the `move_count`
        // budget.
        let earlier: u32 = tx.query_row(
            "SELECT COUNT(*) FROM reschedule_hold \
             WHERE job_config_id = ?1 AND task_config_id = ?2 AND created_at_unix >= ?3;",
            params![
                allocation.job_config_id,
                allocation.task_config_id,
                now - period_secs
            ],
            |row| row.get(0),
        )?;
        // The backoff follows a chain of failures: the previous hold of the
        // same instance (or, for a stateless task, of the task) continues
        // it when this failure came within `move_period` of that hold's
        // release, that is, before its replacement survived a whole period.
        // Per instance, so one keyed sandbox that keeps failing does not
        // slow another's first retry. The release is computed from the
        // delay, so a lifted hold (StartInstance) still continues its chain.
        let previous: Option<(u32, i64)> = tx
            .query_row(
                "SELECT attempt, created_at_unix + delay_secs FROM reschedule_hold \
                 WHERE job_config_id = ?1 AND task_config_id = ?2 \
                 AND COALESCE(instance_id, '') = COALESCE(?3, '') \
                 ORDER BY created_at_unix DESC, allocation_id DESC LIMIT 1;",
                params![
                    allocation.job_config_id,
                    allocation.task_config_id,
                    allocation.instance_id
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let attempt = match previous {
            Some((attempt, released_at)) if now <= released_at + period_secs => attempt + 1,
            _ => 1,
        };
        let reason = summarize_reason(&failure.reason, RESCHEDULE_REASON_MAX_CHARS);

        let stopping_event = self.remove_allocation_row_tx(&tx, &allocation, cause, explanation)?;

        let verdict = match policy.move_count {
            Some(limit) if earlier >= limit => MoveVerdict::Exhausted {
                task_name,
                moves: earlier,
                move_count: limit,
                move_period_secs: period_secs.max(0) as u64,
                reason,
            },
            _ => {
                let delay = move_backoff(
                    Duration::from_millis(policy.move_delay_ms),
                    (attempt - 1) as usize,
                );
                // Round up, so an eval queued for exactly `delay` never
                // finds its own hold still active.
                let delay_secs = delay.as_secs() + u64::from(delay.subsec_nanos() > 0);
                let hold = RescheduleHold {
                    allocation_id: allocation.id,
                    job_id: allocation.job_id.clone(),
                    job_config_id: allocation.job_config_id.clone(),
                    task_config_id: allocation.task_config_id,
                    task_name,
                    group,
                    instance_id: allocation.instance_id.clone(),
                    node_id: allocation.node_id.clone(),
                    attempt,
                    delay_secs,
                    not_before_unix: now + delay_secs as i64,
                    reason,
                    created_at_unix: now,
                };
                // Kept for a move period past its release: it counts toward
                // `move_count` for a period from the failure, and continues
                // the backoff chain for a period from the release.
                let expires_at = hold.not_before_unix + period_secs;
                tx.execute(
                    &format!(
                        "INSERT OR REPLACE INTO reschedule_hold ({HOLD_COLUMNS}, expires_at_unix) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14);"
                    ),
                    params![
                        hold.allocation_id,
                        hold.job_id,
                        hold.job_config_id,
                        hold.task_config_id,
                        hold.task_name,
                        hold.group,
                        hold.instance_id,
                        hold.node_id,
                        hold.attempt,
                        hold.delay_secs as i64,
                        hold.not_before_unix,
                        hold.reason,
                        hold.created_at_unix,
                        expires_at
                    ],
                )?;
                MoveVerdict::Deferred(hold)
            }
        };
        tx.commit()?;
        self.publish_staged(stopping_event);
        drop(conn);
        self.cache_remove_allocations(std::slice::from_ref(&allocation))?;
        Ok(Some(FailedRemoval {
            allocation,
            verdict,
        }))
    }

    /// Replacements a `Reschedule` plan of `job_config_id` must leave
    /// unplaced right now, per task config id: one per active hold. A hold
    /// on an instance counts only while that instance is still unbound
    /// (an explicit start or a targeted placement may have bound it since),
    /// and an instance counts once however many holds it has.
    pub(crate) fn held_replacements_for_job_config(
        &self,
        job_config_id: &str,
    ) -> R<HashMap<i64, u32>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT h.task_config_id, \
                    COUNT(DISTINCT COALESCE(h.instance_id, 'alloc:' || h.allocation_id)) \
             FROM reschedule_hold h \
             WHERE h.job_config_id = ?1 AND h.not_before_unix > ?2 \
               AND (h.instance_id IS NULL OR EXISTS (\
                   SELECT 1 FROM instance i WHERE i.instance_id = h.instance_id \
                   AND i.current_alloc_id IS NULL AND i.state IN ('pending', 'rescheduling'))) \
             GROUP BY h.task_config_id;",
        )?;
        let rows = stmt.query_map(params![job_config_id, Self::now_unix()], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, u32>(1)?))
        })?;
        Ok(Self::collect_rows(rows)?.into_iter().collect())
    }

    /// The job's active holds (replacements still waiting out their
    /// backoff), soonest next attempt first, for `job info`.
    pub fn reschedule_holds_for_job(&self, job_id: &str) -> R<Vec<RescheduleHold>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT {HOLD_COLUMNS} FROM reschedule_hold \
             WHERE job_id = ?1 AND not_before_unix > ?2 \
             ORDER BY not_before_unix ASC, allocation_id ASC;"
        ))?;
        let rows = stmt.query_map(params![job_id, Self::now_unix()], hold_from_row)?;
        Self::collect_rows(rows)
    }

    /// The active hold on one instance, if it is waiting out a backoff: the
    /// newest, when it failed more than once.
    pub fn reschedule_hold_for_instance(&self, instance_id: &str) -> R<Option<RescheduleHold>> {
        let conn = self.read_connection()?;
        let hold = conn
            .query_row(
                &format!(
                    "SELECT {HOLD_COLUMNS} FROM reschedule_hold \
                     WHERE instance_id = ?1 AND not_before_unix > ?2 \
                     ORDER BY created_at_unix DESC, allocation_id DESC LIMIT 1;"
                ),
                params![instance_id, Self::now_unix()],
                hold_from_row,
            )
            .optional()?;
        Ok(hold)
    }

    /// Delete a job's holds, and with them its move history: the job is
    /// starting again from the top (a `job_update` eval: `job run
    /// --force-reschedule`, or restarting a stopped or failed job). Returns
    /// how many rows went.
    pub fn clear_reschedule_holds_for_job(&self, job_id: &str) -> R<usize> {
        // Most restarts (every batch schedule's next run) have nothing to
        // clear: skip the replicated write for them.
        let any: bool = self.read_connection()?.query_row(
            "SELECT EXISTS (SELECT 1 FROM reschedule_hold WHERE job_id = ?1);",
            [job_id],
            |row| row.get(0),
        )?;
        if !any {
            return Ok(0);
        }
        let job_id = job_id.to_owned();
        self.replicated(
            Lane::Scheduler,
            "clear_reschedule_holds_for_job",
            move |s| {
                let conn = s.write_connection()?;
                Ok(conn.execute("DELETE FROM reschedule_hold WHERE job_id = ?1;", [&job_id])?)
            },
        )
    }

    /// Lift an instance's active hold inside the caller's transaction, so
    /// its replacement may be placed now (StartInstance on a `rescheduling`
    /// keyed instance). The rows stay as move history, so a failure after
    /// the retry still backs off from where it was. Returns whether a hold
    /// was active.
    pub(super) fn lift_instance_reschedule_hold_tx(tx: &Connection, instance_id: &str) -> R<bool> {
        let lifted = tx.execute(
            "UPDATE reschedule_hold SET not_before_unix = 0 \
             WHERE instance_id = ?1 AND not_before_unix > ?2;",
            params![instance_id, Self::now_unix()],
        )?;
        Ok(lifted > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn move_backoff_doubles_and_caps() {
        let delays: Vec<u64> = (0..8)
            .map(|earlier| move_backoff(Duration::from_secs(30), earlier).as_secs())
            .collect();
        assert_eq!(delays, vec![30, 60, 120, 240, 480, 960, 1800, 1800]);
        // A `move_delay` above the cap is honored as is, and zero stays zero.
        assert_eq!(
            move_backoff(Duration::from_secs(3600), 3),
            Duration::from_secs(3600)
        );
        assert_eq!(move_backoff(Duration::ZERO, 5), Duration::ZERO);
        assert_eq!(move_backoff(Duration::from_secs(30), 1000), MOVE_DELAY_MAX);
    }

    #[test]
    fn reasons_and_durations_read_on_one_line() {
        assert_eq!(
            summarize_reason("exit 1\n  boom: bad\n\nlast line ", 100),
            "exit 1 / boom: bad / last line"
        );
        let long = "x".repeat(50);
        let cut = summarize_reason(&long, 10);
        assert_eq!(cut, "xxxxxxx...");
        assert_eq!(short_duration(30), "30s");
        assert_eq!(short_duration(240), "4m");
        assert_eq!(short_duration(5400), "1h30m");
        assert_eq!(short_duration(0), "0s");
    }
}
