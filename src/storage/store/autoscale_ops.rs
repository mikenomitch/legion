//! Task autoscaling's replicated state (docs/autoscaling.md;
//! autoscaler-v2 plan §6.5, §6.6, §7). Every table here is Control-lane,
//! beside `task_scale`:
//!
//! - **Scale writes** ([`Store::apply_scale_write`]): one Control entry that
//!   checks the compare-and-set version, refuses an autoscaler write to a
//!   paused or pinned target, writes the override (or the pending scale-in,
//!   the pin, its release, or a reset) for the task and every member of its
//!   group, and records the history row and the policy snapshot its digest
//!   names. All or nothing.
//! - **Versions**: a value of the `task_scale` counter in `scaling_counter`,
//!   stamped on every write of a `task_scale` row. The counter never goes
//!   back, so a version never repeats, even across a delete and recreate (the
//!   ABA a per-row counter would allow).
//! - **History** (`scale_history`): capped at [`HISTORY_CAP`] rows per task;
//!   `scaling_policy_snapshot` keeps each effective policy a row's digest
//!   names, pruned when no row names it any more.
//! - **Pauses** (`scaling_pause`) and the **cluster's scaling defaults**
//!   (`scaling_defaults`, one row).

use super::*;
use crate::storage::store::TaskScalePin;

/// The counter behind `task_scale.version`.
pub const TASK_SCALE_COUNTER: &str = "task_scale";

/// History rows kept per task.
pub const HISTORY_CAP: i64 = 200;

/// The wildcard of `scaling_pause` scopes: `job_id = '*'` is the
/// cluster-wide pause, `task_group = '*'` a whole job's.
pub const PAUSE_ANY: &str = "*";

/// What a scale write does to the target's row(s).
#[derive(Debug, Clone, PartialEq)]
pub enum ScaleWriteOp {
    /// Apply `count` as the override (clears a pending scale-in, keeps any
    /// pin).
    Set { count: u32 },
    /// Record a scale-in to `count` that waits for the active rollout.
    Pending { count: u32 },
    /// Set `count` and pin a floor and/or ceiling until `until_ms`.
    Pin {
        count: u32,
        min: Option<u32>,
        max: Option<u32>,
        until_ms: i64,
    },
    /// End the pin; the count stays.
    Release,
    /// Drop the override rows: the spec owns the count again.
    Reset,
}

/// A history row to record with a write, or on its own (a dry-run "would").
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NewHistory {
    pub summary: String,
    pub decision_id: String,
    /// The engine's decision, JSON; empty for people.
    pub decision_json: String,
    pub policy_digest: String,
    /// The effective policy (canonical JSON) behind `policy_digest`, stored
    /// once per digest.
    pub policy_json: String,
}

/// One scale write ([`Store::apply_scale_write`]).
#[derive(Debug, Clone, PartialEq)]
pub struct ScaleWrite {
    pub job_id: String,
    pub task_group: String,
    /// The task whose row the version is compared against and the history
    /// is recorded for: the one carrying `autoscale`, or the one a person
    /// named.
    pub task_name: String,
    /// Other tasks of the group that move with it (a group target).
    pub members: Vec<String>,
    pub op: ScaleWriteOp,
    /// The current spec count (the override's `base_count`).
    pub base_count: u32,
    pub source: String,
    pub reason: String,
    pub principal: String,
    /// Compare-and-set: the version the writer read (0: no row).
    pub expected_version: Option<u64>,
    /// An autoscaler write: refused while the target is paused or pinned to
    /// a count, and outside a pinned floor or ceiling.
    pub autoscaler: bool,
    /// The count before, for history.
    pub from: u32,
    pub history: NewHistory,
    pub now_ms: i64,
}

/// What a scale write did.
#[derive(Debug, Clone, PartialEq)]
pub struct ScaleWriteOutcome {
    /// The primary row's version after the write (0 after a reset).
    pub version: u64,
    /// The history row written.
    pub history_id: i64,
    /// The count the write left in place (after clamping into a pin).
    pub count: Option<u32>,
}

/// One `scale_history` row.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryRow {
    pub id: i64,
    pub job_id: String,
    pub task_group: String,
    pub task_name: String,
    pub at_ms: i64,
    pub from: u32,
    pub to: u32,
    pub source: String,
    pub principal: String,
    pub summary: String,
    pub applied: bool,
    pub kind: String,
    pub policy_digest: String,
    pub decision_id: String,
    pub decision: String,
}

/// One `scaling_pause` row.
#[derive(Debug, Clone, PartialEq)]
pub struct PauseRow {
    /// `*` for the cluster-wide pause.
    pub job_id: String,
    /// `*` for a whole job.
    pub task_group: String,
    /// `*` for a whole job.
    pub task_name: String,
    pub until_ms: Option<i64>,
    pub reason: String,
    pub principal: String,
    pub created_ms: i64,
}

impl PauseRow {
    /// `all`, `job`, or `task`.
    pub fn scope(&self) -> &'static str {
        if self.job_id == PAUSE_ANY {
            "all"
        } else if self.task_group == PAUSE_ANY {
            "job"
        } else {
            "task"
        }
    }

    /// Whether the pause holds at `now_ms`.
    pub fn active(&self, now_ms: i64) -> bool {
        self.until_ms.is_none_or(|until| until > now_ms)
    }

    /// Whether the pause covers the target.
    pub fn covers(&self, job_id: &str, task_group: &str, task_name: &str) -> bool {
        match self.scope() {
            "all" => true,
            "job" => self.job_id == job_id,
            _ => {
                self.job_id == job_id && self.task_group == task_group && self.task_name == task_name
            }
        }
    }
}

/// The pause of `pauses` that covers a target at `now_ms`, narrowest first
/// (a task's own, then its job's, then the cluster-wide one).
pub fn covering_pause<'a>(
    pauses: &'a [PauseRow],
    job_id: &str,
    task_group: &str,
    task_name: &str,
    now_ms: i64,
) -> Option<&'a PauseRow> {
    let rank = |pause: &PauseRow| match pause.scope() {
        "task" => 0,
        "job" => 1,
        _ => 2,
    };
    pauses
        .iter()
        .filter(|pause| pause.active(now_ms) && pause.covers(job_id, task_group, task_name))
        .min_by_key(|pause| rank(pause))
}

/// The cluster's scaling defaults row.
#[derive(Debug, Clone, PartialEq)]
pub struct ScalingDefaultsRow {
    /// The record as written (JSON).
    pub record: String,
    pub version: u64,
    pub updated_by: String,
    pub updated_at: String,
}

fn history_kind(op: &ScaleWriteOp) -> &'static str {
    match op {
        ScaleWriteOp::Set { .. } => "scaled",
        ScaleWriteOp::Pending { .. } => "deferred",
        ScaleWriteOp::Pin { .. } => "pinned",
        ScaleWriteOp::Release => "released",
        ScaleWriteOp::Reset => "reset",
    }
}

impl Store {
    /// Advance a named counter and return its new value (starting at 1).
    pub(crate) fn next_scaling_counter_tx(conn: &Connection, name: &str) -> R<u64> {
        let value: i64 = conn.query_row(
            "INSERT INTO scaling_counter (name, value) VALUES (?1, 1) \
             ON CONFLICT(name) DO UPDATE SET value = value + 1 RETURNING value;",
            [name],
            |row| row.get(0),
        )?;
        Ok(value.max(0) as u64)
    }

    fn task_scale_tx(
        conn: &Connection,
        job_id: &str,
        task_group: &str,
        task_name: &str,
    ) -> R<Option<TaskScale>> {
        Ok(conn
            .prepare_cached(concat!(
                "SELECT ",
                task_scale_columns!(),
                " FROM task_scale WHERE job_id = ?1 AND task_group = ?2 AND task_name = ?3;"
            ))?
            .query_row(
                params![job_id, task_group, task_name],
                Self::task_scale_from_row,
            )
            .optional()?)
    }

    fn pauses_tx(conn: &Connection) -> R<Vec<PauseRow>> {
        let mut stmt = conn.prepare_cached(
            "SELECT job_id, task_group, task_name, until_ms, reason, principal, created_ms \
             FROM scaling_pause ORDER BY job_id, task_group, task_name;",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(PauseRow {
                job_id: row.get(0)?,
                task_group: row.get(1)?,
                task_name: row.get(2)?,
                until_ms: row.get(3)?,
                reason: row.get(4)?,
                principal: row.get(5)?,
                created_ms: row.get(6)?,
            })
        })?;
        Self::collect_rows(rows)
    }

    /// Apply one scale write as a single Control entry (see the module
    /// docs). Refusals are `FailedPrecondition` with the sentence the caller
    /// sees: a version conflict, a pause, a pin.
    #[instrument(err, skip_all, fields(job_id = %write.job_id, task = %write.task_name))]
    pub fn apply_scale_write(&self, write: ScaleWrite) -> R<ScaleWriteOutcome> {
        self.replicated(Lane::Control, "apply_scale_write", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let outcome = Self::apply_scale_write_tx(&tx, &write)?;
            tx.commit()?;
            Ok(outcome)
        })
    }

    fn apply_scale_write_tx(conn: &Connection, write: &ScaleWrite) -> R<ScaleWriteOutcome> {
        let label = crate::manager::autoscale::target_label(
            &write.job_id,
            &write.task_group,
            &write.task_name,
        );
        let current = Self::task_scale_tx(conn, &write.job_id, &write.task_group, &write.task_name)?;
        let found = current.as_ref().map_or(0, |row| row.version);
        if let Some(expected) = write.expected_version {
            if expected != found {
                return Err(StorageError::failed_precondition(format!(
                    "{label}: version conflict: the write expected override version {expected}, \
                     but it is at {found}; someone else changed the count. Re-read and retry"
                ))
                .into());
            }
        }
        let active_pin = current
            .as_ref()
            .and_then(|row| row.pin.clone())
            .filter(|pin| pin.active(write.now_ms));
        let mut op = write.op.clone();
        if write.autoscaler {
            let pauses = Self::pauses_tx(conn)?;
            if let Some(pause) = covering_pause(
                &pauses,
                &write.job_id,
                &write.task_group,
                &write.task_name,
                write.now_ms,
            ) {
                let why = if pause.reason.is_empty() {
                    String::new()
                } else {
                    format!(": \"{}\"", pause.reason)
                };
                return Err(StorageError::failed_precondition(format!(
                    "{label}: autoscaling is paused ({} pause by {}{why}); the autoscaler may not \
                     write until `legion scaling resume`",
                    pause.scope(),
                    pause.principal
                ))
                .into());
            }
            if let (Some(pin), ScaleWriteOp::Set { count } | ScaleWriteOp::Pending { count }) =
                (&active_pin, &op)
            {
                if let Some(pinned) = pin.count() {
                    return Err(StorageError::failed_precondition(format!(
                        "{label}: pinned at {pinned} by {} until {}; the autoscaler may not \
                         change it (`legion job scale {} --release` ends the pin)",
                        pin.by,
                        clock_text(pin.until_ms),
                        write.job_id
                    ))
                    .into());
                }
                let floor = pin.min.unwrap_or(0);
                let ceiling = pin.max.unwrap_or(u32::MAX);
                if *count < floor || *count > ceiling {
                    return Err(StorageError::failed_precondition(format!(
                        "{label}: {count} is outside the pinned bounds {floor}..{} set by {} \
                         until {}",
                        pin.max.map_or("∞".to_string(), |max| max.to_string()),
                        pin.by,
                        clock_text(pin.until_ms)
                    ))
                    .into());
                }
            }
            if matches!(op, ScaleWriteOp::Pin { .. } | ScaleWriteOp::Release | ScaleWriteOp::Reset)
            {
                return Err(StorageError::InvalidInput {
                    message: "the autoscaler writes counts only; pins, releases, and resets are \
                              a person's"
                        .to_string(),
                }
                .into());
            }
        }
        // A pin written with a count outside its own floor or ceiling moves
        // the count into it.
        if let ScaleWriteOp::Pin {
            count, min, max, ..
        } = &mut op
        {
            if let Some(min) = *min {
                *count = (*count).max(min);
            }
            if let Some(max) = *max {
                *count = (*count).min(max);
            }
        }
        let tasks: Vec<&str> = std::iter::once(write.task_name.as_str())
            .chain(
                write
                    .members
                    .iter()
                    .map(String::as_str)
                    .filter(|member| *member != write.task_name),
            )
            .collect();
        let mut version = 0;
        let mut to = write.from;
        let mut applied = false;
        for (index, task) in tasks.iter().enumerate() {
            let written = match &op {
                ScaleWriteOp::Set { count } => {
                    to = *count;
                    applied = *count != write.from;
                    Self::upsert_task_scale_override_tx(
                        conn,
                        &write.job_id,
                        &write.task_group,
                        task,
                        *count,
                        write.base_count,
                        &write.source,
                        &write.reason,
                        &write.principal,
                    )?
                }
                ScaleWriteOp::Pending { count } => {
                    to = *count;
                    Self::upsert_task_scale_pending_tx(
                        conn,
                        &write.job_id,
                        &write.task_group,
                        task,
                        *count,
                        write.base_count,
                        &write.source,
                        &write.reason,
                        &write.principal,
                    )?
                }
                ScaleWriteOp::Pin {
                    count,
                    min,
                    max,
                    until_ms,
                } => {
                    to = *count;
                    applied = *count != write.from;
                    let version = Self::upsert_task_scale_override_tx(
                        conn,
                        &write.job_id,
                        &write.task_group,
                        task,
                        *count,
                        write.base_count,
                        &write.source,
                        &write.reason,
                        &write.principal,
                    )?;
                    if index == 0 {
                        conn.execute(
                            "UPDATE task_scale SET pin_until_ms = ?4, pin_min = ?5, pin_max = ?6, \
                             pin_by = ?7, pin_reason = ?8 \
                             WHERE job_id = ?1 AND task_group = ?2 AND task_name = ?3;",
                            params![
                                write.job_id,
                                write.task_group,
                                task,
                                until_ms,
                                min,
                                max,
                                write.principal,
                                write.reason
                            ],
                        )?;
                    }
                    version
                }
                ScaleWriteOp::Release => {
                    if index > 0 {
                        continue;
                    }
                    if current.is_none() {
                        return Err(StorageError::failed_precondition(format!(
                            "{label}: there is no pin to release"
                        ))
                        .into());
                    }
                    let version = Self::next_scaling_counter_tx(conn, TASK_SCALE_COUNTER)?;
                    conn.execute(
                        "UPDATE task_scale SET pin_until_ms = NULL, pin_min = NULL, pin_max = NULL, \
                         pin_by = '', pin_reason = '', version = ?4, updated_at = ?5 \
                         WHERE job_id = ?1 AND task_group = ?2 AND task_name = ?3;",
                        params![
                            write.job_id,
                            write.task_group,
                            task,
                            version as i64,
                            Self::now_rfc3339()?
                        ],
                    )?;
                    version
                }
                ScaleWriteOp::Reset => {
                    to = write.base_count;
                    applied = write.base_count != write.from;
                    conn.execute(
                        "DELETE FROM task_scale \
                         WHERE job_id = ?1 AND task_group = ?2 AND task_name = ?3;",
                        params![write.job_id, write.task_group, task],
                    )?;
                    0
                }
            };
            if index == 0 {
                version = written;
            }
        }
        let history_id = Self::insert_history_tx(
            conn,
            &write.job_id,
            &write.task_group,
            &write.task_name,
            write.now_ms,
            write.from,
            to,
            &write.source,
            &write.principal,
            history_kind(&op),
            applied,
            &write.history,
            &write.reason,
        )?;
        Ok(ScaleWriteOutcome {
            version,
            history_id,
            count: match op {
                ScaleWriteOp::Reset | ScaleWriteOp::Release => None,
                _ => Some(to),
            },
        })
    }

    /// Insert one history row (and the policy snapshot its digest names),
    /// then cap the target's history.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_history_tx(
        conn: &Connection,
        job_id: &str,
        task_group: &str,
        task_name: &str,
        at_ms: i64,
        from: u32,
        to: u32,
        source: &str,
        principal: &str,
        kind: &str,
        applied: bool,
        history: &NewHistory,
        reason: &str,
    ) -> R<i64> {
        let summary = if history.summary.is_empty() {
            reason.to_string()
        } else {
            history.summary.clone()
        };
        if !history.policy_digest.is_empty() && !history.policy_json.is_empty() {
            conn.execute(
                "INSERT OR IGNORE INTO scaling_policy_snapshot (digest, policy, first_used_ms) \
                 VALUES (?1, ?2, ?3);",
                params![history.policy_digest, history.policy_json, at_ms],
            )?;
        }
        let id: i64 = conn.query_row(
            "INSERT INTO scale_history (job_id, task_group, task_name, at_ms, from_count, \
             to_count, source, principal, summary, applied, kind, policy_digest, decision_id, \
             decision) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14) \
             RETURNING id;",
            params![
                job_id,
                task_group,
                task_name,
                at_ms,
                from,
                to,
                source,
                principal,
                summary,
                applied,
                kind,
                history.policy_digest,
                history.decision_id,
                history.decision_json
            ],
            |row| row.get(0),
        )?;
        let removed = conn.execute(
            "DELETE FROM scale_history WHERE job_id = ?1 AND task_group = ?2 AND task_name = ?3 \
             AND id NOT IN (SELECT id FROM scale_history WHERE job_id = ?1 AND task_group = ?2 \
             AND task_name = ?3 ORDER BY id DESC LIMIT ?4);",
            params![job_id, task_group, task_name, HISTORY_CAP],
        )?;
        if removed > 0 {
            Self::prune_policy_snapshots_tx(conn)?;
        }
        Ok(id)
    }

    fn prune_policy_snapshots_tx(conn: &Connection) -> R<usize> {
        Ok(conn.execute(
            "DELETE FROM scaling_policy_snapshot WHERE digest NOT IN \
             (SELECT DISTINCT policy_digest FROM scale_history);",
            [],
        )?)
    }

    /// Record a history row on its own: a dry-run policy's "would"
    /// (`RecordScalingAction`).
    #[allow(clippy::too_many_arguments)]
    pub fn record_scale_history(
        &self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
        at_ms: i64,
        from: u32,
        to: u32,
        source: &str,
        principal: &str,
        kind: &str,
        history: NewHistory,
    ) -> R<i64> {
        let (job_id, task_group, task_name) = (
            job_id.to_owned(),
            task_group.to_owned(),
            task_name.to_owned(),
        );
        let (source, principal, kind) = (source.to_owned(), principal.to_owned(), kind.to_owned());
        self.replicated(Lane::Control, "record_scale_history", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let id = Self::insert_history_tx(
                &tx, &job_id, &task_group, &task_name, at_ms, from, to, &source, &principal,
                &kind, false, &history, "",
            )?;
            tx.commit()?;
            Ok(id)
        })
    }

    fn history_from_row(row: &Row<'_>) -> Result<HistoryRow, rusqlite::Error> {
        Ok(HistoryRow {
            id: row.get(0)?,
            job_id: row.get(1)?,
            task_group: row.get(2)?,
            task_name: row.get(3)?,
            at_ms: row.get(4)?,
            from: row.get::<_, i64>(5)?.max(0) as u32,
            to: row.get::<_, i64>(6)?.max(0) as u32,
            source: row.get(7)?,
            principal: row.get(8)?,
            summary: row.get(9)?,
            applied: row.get(10)?,
            kind: row.get(11)?,
            policy_digest: row.get(12)?,
            decision_id: row.get(13)?,
            decision: row.get(14)?,
        })
    }

    /// A job's history, newest first, optionally narrowed to one group or
    /// task.
    pub fn scale_history(
        &self,
        job_id: &str,
        task_group: Option<&str>,
        task_name: Option<&str>,
        limit: usize,
    ) -> R<Vec<HistoryRow>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT id, job_id, task_group, task_name, at_ms, from_count, to_count, source, \
             principal, summary, applied, kind, policy_digest, decision_id, decision \
             FROM scale_history WHERE job_id = ?1 \
             AND (?2 IS NULL OR task_group = ?2) AND (?3 IS NULL OR task_name = ?3) \
             ORDER BY id DESC LIMIT ?4;",
        )?;
        let rows = stmt.query_map(
            params![job_id, task_group, task_name, limit as i64],
            Self::history_from_row,
        )?;
        Self::collect_rows(rows)
    }

    /// The effective policy a history digest names, if it is still kept.
    pub fn scaling_policy_snapshot(&self, digest: &str) -> R<Option<String>> {
        let conn = self.read_connection()?;
        Ok(conn
            .query_row(
                "SELECT policy FROM scaling_policy_snapshot WHERE digest = ?1;",
                [digest],
                |row| row.get(0),
            )
            .optional()?)
    }

    /// Every pause, expired ones included (they are inert, and a resume or a
    /// new pause replaces them).
    pub fn scaling_pauses(&self) -> R<Vec<PauseRow>> {
        let conn = self.read_connection()?;
        Self::pauses_tx(&conn)
    }

    /// Set a pause. `job_id = "*"` pauses everything; `task_group = "*"`
    /// (with `task_name = "*"`) a whole job.
    #[instrument(err, skip(self, reason, principal))]
    pub fn set_scaling_pause(
        &self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
        until_ms: Option<i64>,
        reason: &str,
        principal: &str,
        now_ms: i64,
    ) -> R<()> {
        let row = PauseRow {
            job_id: job_id.to_owned(),
            task_group: task_group.to_owned(),
            task_name: task_name.to_owned(),
            until_ms,
            reason: reason.to_owned(),
            principal: principal.to_owned(),
            created_ms: now_ms,
        };
        self.replicated(Lane::Control, "set_scaling_pause", move |s| {
            let conn = s.write_connection()?;
            conn.execute(
                "INSERT INTO scaling_pause \
                 (job_id, task_group, task_name, until_ms, reason, principal, created_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT(job_id, task_group, task_name) DO UPDATE SET \
                 until_ms = ?4, reason = ?5, principal = ?6, created_ms = ?7;",
                params![
                    row.job_id,
                    row.task_group,
                    row.task_name,
                    row.until_ms,
                    row.reason,
                    row.principal,
                    row.created_ms
                ],
            )?;
            Ok(())
        })
    }

    /// Remove a pause. Returns whether one existed at exactly this scope.
    #[instrument(err, skip(self))]
    pub fn clear_scaling_pause(&self, job_id: &str, task_group: &str, task_name: &str) -> R<bool> {
        let (job_id, task_group, task_name) = (
            job_id.to_owned(),
            task_group.to_owned(),
            task_name.to_owned(),
        );
        self.replicated(Lane::Control, "clear_scaling_pause", move |s| {
            let conn = s.write_connection()?;
            let removed = conn.execute(
                "DELETE FROM scaling_pause WHERE job_id = ?1 AND task_group = ?2 AND task_name = ?3;",
                params![job_id, task_group, task_name],
            )?;
            Ok(removed > 0)
        })
    }

    /// The cluster's scaling defaults, if any were ever applied.
    pub fn scaling_defaults(&self) -> R<Option<ScalingDefaultsRow>> {
        let conn = self.read_connection()?;
        Ok(conn
            .query_row(
                "SELECT record, version, updated_by, updated_at FROM scaling_defaults WHERE id = 1;",
                [],
                |row| {
                    Ok(ScalingDefaultsRow {
                        record: row.get(0)?,
                        version: row.get::<_, i64>(1)?.max(0) as u64,
                        updated_by: row.get(2)?,
                        updated_at: row.get(3)?,
                    })
                },
            )
            .optional()?)
    }

    /// Replace the cluster's scaling defaults. Returns the new version.
    #[instrument(err, skip(self, record))]
    pub fn put_scaling_defaults(&self, record: &str, updated_by: &str) -> R<u64> {
        let (record, updated_by) = (record.to_owned(), updated_by.to_owned());
        self.replicated(Lane::Control, "put_scaling_defaults", move |s| {
            let conn = s.write_connection()?;
            let version: i64 = conn.query_row(
                "INSERT INTO scaling_defaults (id, record, version, updated_by, updated_at) \
                 VALUES (1, ?1, 1, ?2, ?3) \
                 ON CONFLICT(id) DO UPDATE SET record = ?1, version = version + 1, \
                 updated_by = ?2, updated_at = ?3 RETURNING version;",
                params![record, updated_by, Self::now_rfc3339()?],
                |row| row.get(0),
            )?;
            Ok(version.max(0) as u64)
        })
    }

    /// The latest config id of every job that is not stopped or failed and
    /// has an autoscaled task, sorted: what `WatchScalingPolicies` polls to
    /// notice a change.
    pub fn autoscaled_job_configs(&self) -> R<Vec<(String, String)>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT DISTINCT tc.job_id, tc.job_config_id FROM task_config tc \
             INNER JOIN job j ON j.id = tc.job_id \
             WHERE tc.autoscale IS NOT NULL AND j.status NOT IN ('stopped', 'failed') \
             AND tc.job_config_id = {LATEST_CONFIG_FOR_JOB} \
             ORDER BY tc.job_id;"
        ))?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Self::collect_rows(rows)
    }

    /// A cheap fingerprint of the namespace policies: their count and newest
    /// update.
    pub fn namespace_policy_fingerprint(&self) -> R<(i64, String)> {
        let conn = self.read_connection()?;
        Ok(conn.query_row(
            "SELECT COUNT(*), COALESCE(MAX(updated_at), '') FROM namespace_policy;",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?)
    }
}

fn clock_text(at_ms: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(at_ms)
        .map(|at| at.format("%H:%M UTC").to_string())
        .unwrap_or_else(|| at_ms.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::config::ManagerConfigFull;
    use crate::structs::Submission;

    fn store() -> R<(tempfile::TempDir, Store)> {
        let dir = tempfile::tempdir_in(crate::test_scratch::root())?;
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.path().to_str().context("utf-8")?.to_string(),
            ..Default::default()
        })?;
        Ok((dir, store))
    }

    fn submit(store: &Store, count: u32, autoscale: Option<Value>) -> R<()> {
        let mut task = json!({
            "name": "api", "runtime": "exec", "count": count,
            "config": { "command": ["sleep", "1"] },
            "resources": { "cpu": 500, "memory": 64 },
        });
        if let Some(autoscale) = autoscale {
            task["autoscale"] = autoscale;
        }
        let config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "web".into(),
            tasks: vec![serde_json::from_value(task)?],
            version: None,
            pending_extensions: Default::default(),
        };
        store.handle_job_submit(
            &config,
            &[Submission {
                path: "web.json".into(),
                content: format!("web-{count}-{}", config.tasks[0].autoscale.is_some()),
                is_primary: true,
            }],
        )?;
        Ok(())
    }

    fn write(op: ScaleWriteOp, expected: Option<u64>, autoscaler: bool, now_ms: i64) -> ScaleWrite {
        ScaleWrite {
            job_id: "web".into(),
            task_group: "default".into(),
            task_name: "api".into(),
            members: Vec::new(),
            op,
            base_count: 2,
            source: if autoscaler { "autoscaler" } else { "operator" }.into(),
            reason: String::new(),
            principal: "me".into(),
            expected_version: expected,
            autoscaler,
            from: 2,
            history: NewHistory {
                summary: "test".into(),
                policy_digest: "d1".into(),
                policy_json: "{}".into(),
                ..Default::default()
            },
            now_ms,
        }
    }

    fn refusal(err: anyhow::Error) -> String {
        StorageError::from_anyhow(&err)
            .map(|err| err.to_string())
            .unwrap_or_else(|| format!("{err:#}"))
    }

    /// Two writers from one version: the first wins, the second gets a
    /// version conflict. A person's write moves the version too, so the
    /// autoscaler cannot overwrite a scale made a moment earlier.
    #[test]
    fn compare_and_set_conflicts_test() -> R<()> {
        let (_dir, store) = store()?;
        submit(&store, 2, Some(json!(true)))?;
        let first = store.apply_scale_write(write(ScaleWriteOp::Set { count: 3 }, Some(0), true, 1))?;
        assert!(first.version > 0);
        let err = store
            .apply_scale_write(write(ScaleWriteOp::Set { count: 4 }, Some(0), true, 1))
            .unwrap_err();
        assert!(refusal(err).contains("version conflict"));
        // A person's write in between.
        let human = store.apply_scale_write(write(ScaleWriteOp::Set { count: 5 }, None, false, 1))?;
        assert!(human.version > first.version);
        let err = store
            .apply_scale_write(write(ScaleWriteOp::Set { count: 6 }, Some(first.version), true, 1))
            .unwrap_err();
        assert!(refusal(err).contains("version conflict"));
        assert_eq!(store.effective_task_count("web", "default", "api", 2)?, 5);
        Ok(())
    }

    /// A version never repeats across a delete and recreate (no ABA).
    #[test]
    fn versions_survive_delete_and_recreate_test() -> R<()> {
        let (_dir, store) = store()?;
        submit(&store, 2, Some(json!(true)))?;
        let first = store.apply_scale_write(write(ScaleWriteOp::Set { count: 3 }, None, false, 1))?;
        store.apply_scale_write(write(ScaleWriteOp::Reset, None, false, 2))?;
        assert!(store.task_scale_for("web", "default", "api")?.is_none());
        let again = store.apply_scale_write(write(ScaleWriteOp::Set { count: 3 }, Some(0), false, 3))?;
        assert!(again.version > first.version, "{again:?} vs {first:?}");
        Ok(())
    }

    /// History is capped per task, newest first, and dry-run rows are not
    /// applied. A digest whose last row is capped away loses its snapshot.
    #[test]
    fn history_cap_order_and_snapshots_test() -> R<()> {
        let (_dir, store) = store()?;
        submit(&store, 2, Some(json!(true)))?;
        store.record_scale_history(
            "web", "default", "api", 1, 2, 3, "autoscaler", "app", "would",
            NewHistory {
                summary: "would".into(),
                policy_digest: "old".into(),
                policy_json: "{\"old\":true}".into(),
                ..Default::default()
            },
        )?;
        assert_eq!(store.scaling_policy_snapshot("old")?.as_deref(), Some("{\"old\":true}"));
        for at in 2..(HISTORY_CAP + 5) {
            store.apply_scale_write(write(
                ScaleWriteOp::Set {
                    count: (at % 5) as u32 + 2,
                },
                None,
                false,
                at,
            ))?;
        }
        let rows = store.scale_history("web", None, None, 1000)?;
        assert_eq!(rows.len() as i64, HISTORY_CAP);
        assert!(rows.windows(2).all(|pair| pair[0].id > pair[1].id));
        assert!(rows.iter().all(|row| row.kind == "scaled"));
        assert!(store.scaling_policy_snapshot("old")?.is_none(), "pruned with its rows");
        assert!(store.scaling_policy_snapshot("d1")?.is_some());
        Ok(())
    }

    /// Pauses refuse autoscaler writes (task, job, and cluster scope) but
    /// not people's; count pins refuse autoscaler writes; floor pins bound
    /// them; an expired pin binds nothing.
    #[test]
    fn pause_and_pin_refusals_test() -> R<()> {
        let (_dir, store) = store()?;
        submit(&store, 2, Some(json!(true)))?;
        for (job, group, task) in [("web", "default", "api"), ("web", "*", "*"), ("*", "*", "*")] {
            store.set_scaling_pause(job, group, task, None, "incident", "alice", 0)?;
            let err = store
                .apply_scale_write(write(ScaleWriteOp::Set { count: 3 }, None, true, 10))
                .unwrap_err();
            assert!(refusal(err).contains("autoscaling is paused"));
            store.apply_scale_write(write(ScaleWriteOp::Set { count: 3 }, None, false, 10))?;
            assert!(store.clear_scaling_pause(job, group, task)?);
        }
        // An expired pause binds nothing.
        store.set_scaling_pause("*", "*", "*", Some(5), "", "alice", 0)?;
        store.apply_scale_write(write(ScaleWriteOp::Set { count: 4 }, None, true, 10))?;

        // A count pin.
        let pinned = store.apply_scale_write(write(
            ScaleWriteOp::Pin {
                count: 12,
                min: Some(12),
                max: Some(12),
                until_ms: 100,
            },
            None,
            false,
            10,
        ))?;
        assert_eq!(pinned.count, Some(12));
        let err = store
            .apply_scale_write(write(ScaleWriteOp::Set { count: 6 }, None, true, 50))
            .unwrap_err();
        assert!(refusal(err).contains("pinned at 12"));
        // Expired: the autoscaler may write again, and the count stayed.
        assert_eq!(store.effective_task_count("web", "default", "api", 2)?, 12);
        store.apply_scale_write(write(ScaleWriteOp::Set { count: 11 }, None, true, 100))?;

        // A floor pin raises the count at once and bounds the autoscaler.
        let floor = store.apply_scale_write(write(
            ScaleWriteOp::Pin {
                count: 3,
                min: Some(10),
                max: None,
                until_ms: 1000,
            },
            None,
            false,
            200,
        ))?;
        assert_eq!(floor.count, Some(10));
        let err = store
            .apply_scale_write(write(ScaleWriteOp::Set { count: 9 }, None, true, 300))
            .unwrap_err();
        assert!(refusal(err).contains("outside the pinned bounds 10..∞"));
        store.apply_scale_write(write(ScaleWriteOp::Set { count: 14 }, None, true, 300))?;
        store.apply_scale_write(write(ScaleWriteOp::Release, None, false, 400))?;
        let row = store.task_scale_for("web", "default", "api")?.context("row")?;
        assert!(row.pin.is_none());
        assert_eq!(row.count, Some(14));
        let kinds: Vec<String> = store
            .scale_history("web", None, None, 100)?
            .into_iter()
            .map(|row| row.kind)
            .collect();
        assert!(kinds.contains(&"pinned".to_string()) && kinds.contains(&"released".to_string()));
        Ok(())
    }

    /// A group target writes every member in one entry.
    #[test]
    fn group_targets_write_every_member_test() -> R<()> {
        let (_dir, store) = store()?;
        submit(&store, 2, Some(json!(true)))?;
        let mut group = write(ScaleWriteOp::Set { count: 4 }, None, true, 1);
        group.members = vec!["api".into(), "sidecar".into()];
        store.apply_scale_write(group)?;
        assert_eq!(store.effective_task_count("web", "default", "sidecar", 2)?, 4);
        assert_eq!(store.effective_task_count("web", "default", "api", 2)?, 4);
        Ok(())
    }

    #[test]
    fn scaling_defaults_versions_test() -> R<()> {
        let (_dir, store) = store()?;
        assert!(store.scaling_defaults()?.is_none());
        assert_eq!(store.put_scaling_defaults("{\"cpu\":0.6}", "ops")?, 1);
        assert_eq!(store.put_scaling_defaults("{}", "ops")?, 2);
        let row = store.scaling_defaults()?.context("row")?;
        assert_eq!((row.record.as_str(), row.version), ("{}", 2));
        Ok(())
    }
}
