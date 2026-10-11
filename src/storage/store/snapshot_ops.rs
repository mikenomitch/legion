//! Snapshot rows and the instance restore source (suspend plan §6.5, §7;
//! docs/suspend-and-resume.md).
//!
//! The manager owns a snapshot's state machine (see
//! [`crate::structs::snapshot::SnapshotState`]); the node that holds the
//! bytes owns the work. Every write here is a scheduler-lane op, beside the
//! `instance` and `volume` rows it interacts with. Worker reports arrive on
//! heartbeats, which can land on any manager, so [`Store::apply_snapshot_report`]
//! forwards to the lane leader like the volume reports do.

use super::*;
use crate::structs::checkpoint::SnapshotKind;
use crate::structs::snapshot::{
    SnapshotDurability, SnapshotPurpose, SnapshotRecord, SnapshotState,
};

const SNAPSHOT_COLUMNS: &str = "snapshot_id, job_id, task_name, origin_instance_id, name, kind, \
     purpose, state, node_id, allocation_id, manifest_json, total_bytes, duration_ms, error, \
     created_at, expires_at, pause_ms, durability, published_at";

/// How many times the manager asks a node to publish a `store` snapshot
/// before leaving it `local` for good.
pub const PUBLISH_ATTEMPTS: u32 = 3;

/// The wait before the next publish attempt, after `failures` failed ones.
/// `None` once the attempts are spent. One minute, then five: long enough
/// for a heartbeat to drop the failed work on the worker, short enough that
/// a store blip does not leave a checkpoint node-bound for long.
pub fn publish_retry_backoff(failures: u32) -> Option<std::time::Duration> {
    match failures {
        0 | 1 => Some(std::time::Duration::from_secs(60)),
        n if n < PUBLISH_ATTEMPTS => Some(std::time::Duration::from_secs(300)),
        _ => None,
    }
}

/// Everything [`Store::create_snapshot`] needs to insert a `capturing` row.
/// Serde so a follower can forward a [`Store::begin_suspend`] to the
/// scheduler-lane leader (the idle sweep runs wherever usage arrives).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewSnapshot {
    pub snapshot_id: String,
    pub job_id: String,
    pub task_name: String,
    pub instance_id: String,
    pub name: Option<String>,
    pub kind: SnapshotKind,
    pub purpose: SnapshotPurpose,
    pub node_id: String,
    pub allocation_id: i64,
    /// Unix seconds, from the task's `snapshots.ttl`.
    pub expires_at: Option<i64>,
    /// `purpose = suspend` only: why (`manual`, `idle`, `pressure`), written
    /// to the instance when the suspend starts. `None` reads as `manual`.
    #[serde(default)]
    pub suspend_reason: Option<String>,
    /// `store` publishes the snapshot once it is captured. Defaults to
    /// `local` in a forward from an older manager.
    #[serde(default)]
    pub durability: SnapshotDurability,
}

/// Filters for [`Store::list_snapshots`]. Every field is exact-match.
#[derive(Debug, Clone, Default)]
pub struct SnapshotFilter {
    pub job_id: Option<String>,
    pub instance_id: Option<String>,
    pub state: Option<SnapshotState>,
}

/// One worker report, as the store applies it. The wire form is
/// [`crate::structs::SnapshotReport`]; this mirror is serde so a follower can
/// forward it to the scheduler-lane leader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotReportRecord {
    pub snapshot_id: String,
    pub op: String,
    pub ok: bool,
    pub error: String,
    pub manifest_json: String,
    pub duration_ms: u64,
    pub total_bytes: u64,
    pub allocation_id: i64,
    /// A capture the worker took on its own (the `suspend` pressure step).
    /// Defaults on deserialize, so a forward from an older manager applies.
    #[serde(default)]
    pub local_suspension: bool,
    /// Phase 6: how long a live capture paused the workload. Absent in
    /// records forwarded by an older manager.
    #[serde(default)]
    pub pause_ms: u64,
}

impl From<&crate::structs::SnapshotReport> for SnapshotReportRecord {
    fn from(report: &crate::structs::SnapshotReport) -> Self {
        SnapshotReportRecord {
            snapshot_id: report.snapshot_id.clone(),
            op: report.op.clone(),
            ok: report.ok,
            error: report.error.clone(),
            manifest_json: report.manifest_json.clone(),
            duration_ms: report.duration_ms,
            total_bytes: report.total_bytes,
            allocation_id: report.allocation_id,
            local_suspension: report.local_suspension,
            pause_ms: report.pause_ms,
        }
    }
}

/// What applying a report changed, for the caller's events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotReportOutcome {
    /// A capture finished: the row is now `local`. `evicted` are older
    /// snapshots of the same instance that `snapshots.keep` moved to
    /// `deleting`.
    Captured {
        snapshot: SnapshotRecord,
        evicted: Vec<String>,
    },
    /// A capture failed: the row is now `failed`.
    CaptureFailed { snapshot: SnapshotRecord },
    /// The node reclaimed the bytes and the row is gone.
    Deleted { snapshot: SnapshotRecord },
    /// The node could not reclaim the bytes. The row stays `deleting` and the
    /// request is re-sent.
    DeleteFailed { snapshot: SnapshotRecord },
    /// A publish finished: every part is in the content store and the row
    /// is `published`.
    Published {
        snapshot: SnapshotRecord,
        /// Bytes the worker actually sent (0 when the store already held
        /// every part).
        uploaded_bytes: u64,
        /// How long the upload took on the worker.
        duration_ms: u64,
        /// The forks of a spread fork, released for placement now that
        /// their checkpoint can restore on any node.
        released_forks: Vec<String>,
    },
    /// A publish failed. The row stays `local` with `error` set. With
    /// `retrying` the manager re-sends the publish after a backoff;
    /// otherwise it was the last attempt, and the snapshot keeps working
    /// from its node. `terminated_forks`: the forks of a spread fork that
    /// waited on this publish and were terminated by its last failure.
    PublishFailed {
        snapshot: SnapshotRecord,
        retrying: bool,
        terminated_forks: Vec<String>,
    },
    /// An allocation started from the snapshot (`ok`) or failed to. The
    /// instance's `restore_from` is cleared either way.
    Restored {
        snapshot_id: String,
        job_id: String,
        instance_id: Option<String>,
        allocation_id: i64,
        ok: bool,
        error: String,
    },
    /// A suspend capture landed: the instance is now `suspended` with this
    /// snapshot as its checkpoint. `reserved` when `suspend.reserve` kept the
    /// allocation row to hold the capacity.
    Suspended {
        snapshot: SnapshotRecord,
        instance_id: String,
        reserved: bool,
        /// `manual`, `idle`, or `pressure`.
        #[serde(default)]
        reason: String,
    },
    /// A local suspension (the node's `suspend` pressure step) arrived for an
    /// instance that has moved on (stopped, destroyed, or rescheduled while
    /// the node was out of contact). Its checkpoint was recorded as
    /// `deleting`, so the node reclaims it.
    LocalSuspensionDiscarded {
        snapshot_id: String,
        allocation_id: i64,
        reason: String,
    },
    /// A suspend capture failed. The instance stays `active` and its
    /// allocation keeps running.
    SuspendFailed { snapshot: SnapshotRecord },
    /// A resume's restore succeeded; the checkpoint is being reclaimed.
    Resumed {
        snapshot_id: String,
        /// `memory` or `disk`.
        kind: String,
        job_id: String,
        instance_id: String,
        node_id: String,
        allocation_id: i64,
        restore_ms: u64,
    },
    /// A resume's restore failed. `cold`: the instance boots fresh on its
    /// storage (`onRestoreFailure = "cold"`); otherwise it is `suspended`
    /// again with its checkpoint.
    ResumeFailed {
        snapshot_id: String,
        job_id: String,
        instance_id: String,
        node_id: String,
        allocation_id: i64,
        error: String,
        cold: bool,
    },
    /// Phase 6: a fork capture landed. The checkpoint is `local` and the
    /// forks waiting on it are released for placement.
    Forked {
        snapshot: SnapshotRecord,
        origin: String,
        instances: Vec<String>,
    },
    /// Phase 6: a fork capture failed. The origin keeps running; the forks
    /// that waited on it are terminated with the reason.
    ForkFailed {
        snapshot: SnapshotRecord,
        instances: Vec<String>,
    },
    /// Phase 6: a restore into a new instance (a fork or a `from_snapshot`
    /// create) failed. `cold`: the instance boots fresh on its own storage
    /// (`onRestoreFailure = "cold"`); otherwise it is `stopped`.
    NewInstanceRestoreFailed {
        snapshot_id: String,
        job_id: String,
        instance_id: String,
        node_id: String,
        allocation_id: i64,
        error: String,
        cold: bool,
    },
    /// Nothing to apply: a duplicate (the report was already applied) or a
    /// report for a row that no longer expects it.
    Ignored,
}

/// Outcome of [`Store::delete_snapshot`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteSnapshotOutcome {
    /// The row moved to `deleting`; its node reclaims it next heartbeat.
    Deleting,
    /// The row was removed at once: nothing on a node holds its bytes.
    Removed(Box<SnapshotRecord>),
    /// Already `deleting`.
    AlreadyDeleting,
}

/// Outcome of [`Store::restore_keyed_instance`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreInstanceOutcome {
    pub removed_allocations: usize,
}

/// A pending instance that must restore from a snapshot (a resume, a
/// RestoreInstance rollback, or any instance created with `restore_from`).
///
/// A `local` snapshot exists on `node_id` only, so placement pins the
/// instance there and checks the manifest's `compat` against that node. A
/// `published` one restores anywhere: placement checks `compat` per
/// candidate and lets the `locality` score prefer nodes holding the parts.
#[derive(Debug, Clone, Default)]
pub struct RestorePin {
    pub instance_id: String,
    pub snapshot_id: String,
    /// Where the snapshot was captured (and its local copy lives).
    pub node_id: Option<String>,
    pub manifest_json: String,
    /// The snapshot is `published`: no hard pin.
    pub published: bool,
    /// The snapshot is a spread fork's checkpoint (`instance fork
    /// --spread`): each of its forks goes to a node none of its siblings
    /// runs on.
    pub spread: bool,
}

impl RestorePin {
    /// The node a placement must use, when the snapshot is node-bound.
    pub fn hard_pin(&self) -> Option<&str> {
        if self.published {
            None
        } else {
            self.node_id.as_deref()
        }
    }
}

impl Store {
    fn snapshot_from_row(row: &Row<'_>) -> Result<SnapshotRecord, rusqlite::Error> {
        let parse_err = |idx: usize, err: String| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
            )
        };
        let kind: String = row.get(5)?;
        let purpose: String = row.get(6)?;
        let state: String = row.get(7)?;
        Ok(SnapshotRecord {
            snapshot_id: row.get(0)?,
            job_id: row.get(1)?,
            task_name: row.get(2)?,
            instance_id: row.get(3)?,
            name: row.get(4)?,
            kind: kind.parse().map_err(|err| parse_err(5, err))?,
            purpose: purpose.parse().map_err(|err| parse_err(6, err))?,
            state: state.parse().map_err(|err| parse_err(7, err))?,
            node_id: row.get(8)?,
            allocation_id: row.get(9)?,
            manifest_json: row.get(10)?,
            total_bytes: row.get::<_, i64>(11)?.max(0) as u64,
            duration_ms: row.get::<_, i64>(12)?.max(0) as u64,
            error: row.get(13)?,
            created_at: row.get(14)?,
            expires_at: row.get(15)?,
            pause_ms: row.get::<_, Option<i64>>(16)?.unwrap_or(0).max(0) as u64,
            durability: row
                .get::<_, String>(17)?
                .parse()
                .map_err(|err| parse_err(17, err))?,
            published_at: row.get(18)?,
        })
    }

    pub(crate) fn get_snapshot_on(
        conn: &Connection,
        snapshot_id: &str,
    ) -> R<Option<SnapshotRecord>> {
        Ok(conn
            .query_row(
                &format!("SELECT {SNAPSHOT_COLUMNS} FROM snapshot WHERE snapshot_id = ?1;"),
                [snapshot_id],
                Self::snapshot_from_row,
            )
            .optional()?)
    }

    /// One snapshot by id.
    pub fn get_snapshot(&self, snapshot_id: &str) -> R<Option<SnapshotRecord>> {
        let conn = self.read_connection()?;
        Self::get_snapshot_on(&conn, snapshot_id)
    }

    /// A snapshot by id, or by name within `job_id`.
    pub fn resolve_snapshot(
        &self,
        id_or_name: &str,
        job_id: Option<&str>,
    ) -> R<Option<SnapshotRecord>> {
        if let Some(snapshot) = self.get_snapshot(id_or_name)? {
            return Ok(Some(snapshot));
        }
        let Some(job_id) = job_id else {
            return Ok(None);
        };
        let conn = self.read_connection()?;
        let target: Option<String> = conn
            .query_row(
                "SELECT snapshot_id FROM snapshot_alias WHERE job_id = ?1 AND name = ?2;",
                params![job_id, id_or_name],
                |row| row.get(0),
            )
            .optional()?;
        match target {
            Some(snapshot_id) => Self::get_snapshot_on(&conn, &snapshot_id),
            None => Ok(None),
        }
    }

    /// Snapshots matching `filter`, newest first.
    pub fn list_snapshots(&self, filter: &SnapshotFilter) -> R<Vec<SnapshotRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshot \
             WHERE (?1 IS NULL OR job_id = ?1) \
             AND (?2 IS NULL OR origin_instance_id = ?2) \
             AND (?3 IS NULL OR state = ?3) \
             ORDER BY created_at DESC, rowid DESC;"
        ))?;
        let rows = stmt.query_map(
            params![
                filter.job_id,
                filter.instance_id,
                filter.state.map(|state| state.as_str())
            ],
            Self::snapshot_from_row,
        )?;
        Self::collect_rows(rows)
    }

    /// Rows with work outstanding on `node_id`: `capturing` (capture it),
    /// `deleting` (reclaim it), and `local` rows with `durability = 'store'`
    /// that have no recorded publish failure or whose retry is due (publish
    /// it). This is the heartbeat's
    /// per-node lookup, an indexed seek on `(node_id, state)` that returns
    /// nothing on a node with no snapshot work.
    pub fn snapshot_work_for_node(&self, node_id: &str) -> R<Vec<SnapshotRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshot \
             WHERE node_id = ?1 AND (state IN ('capturing', 'deleting') \
             OR (state = 'local' AND durability = 'store' AND (error IS NULL \
             OR (publish_retry_at IS NOT NULL AND publish_retry_at <= ?2)))) \
             ORDER BY created_at ASC, rowid ASC;"
        ))?;
        let rows = stmt.query_map(
            params![node_id, chrono::Utc::now().timestamp()],
            Self::snapshot_from_row,
        )?;
        Self::collect_rows(rows)
    }

    /// Insert a `capturing` row (and its alias, when named).
    ///
    /// Refuses a second capture of the same instance while one is still
    /// `capturing`, and a name already used in the job.
    #[instrument(err, skip_all, fields(snapshot_id = %new.snapshot_id))]
    pub fn create_snapshot(&self, new: NewSnapshot) -> R<SnapshotRecord> {
        self.replicated(Lane::Scheduler, "create_snapshot", move |s| {
            s.create_snapshot_apply(&new)
        })
    }

    fn create_snapshot_apply(&self, new: &NewSnapshot) -> R<SnapshotRecord> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let record = Self::create_snapshot_tx(&tx, new)?;
        tx.commit()?;
        Ok(record)
    }

    /// Insert a `capturing` row inside the caller's transaction (see
    /// [`Self::create_snapshot`]).
    pub(crate) fn create_snapshot_tx(tx: &Connection, new: &NewSnapshot) -> R<SnapshotRecord> {
        let busy: Option<String> = tx
            .query_row(
                "SELECT snapshot_id FROM snapshot \
                 WHERE origin_instance_id = ?1 AND state = 'capturing' LIMIT 1;",
                [&new.instance_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(busy) = busy {
            bail!(StorageError::Conflict {
                message: format!(
                    "instance {} is already being captured (snapshot {busy}); wait for it to \
                     finish",
                    new.instance_id
                ),
            });
        }
        if let Some(name) = new.name.as_deref() {
            let taken: Option<String> = tx
                .query_row(
                    "SELECT snapshot_id FROM snapshot_alias WHERE job_id = ?1 AND name = ?2;",
                    params![new.job_id, name],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(taken) = taken {
                bail!(StorageError::Conflict {
                    message: format!(
                        "snapshot name {name:?} is already used by snapshot {taken} in job {}",
                        new.job_id
                    ),
                });
            }
        }
        let now = chrono::Utc::now().timestamp();
        tx.execute(
            "INSERT INTO snapshot (snapshot_id, job_id, task_name, origin_instance_id, name, \
             kind, purpose, state, node_id, allocation_id, created_at, expires_at, durability) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'capturing', ?8, ?9, ?10, ?11, ?12);",
            params![
                new.snapshot_id,
                new.job_id,
                new.task_name,
                new.instance_id,
                new.name,
                new.kind.as_str(),
                new.purpose.as_str(),
                new.node_id,
                new.allocation_id,
                now,
                new.expires_at,
                new.durability.as_str(),
            ],
        )?;
        if let Some(name) = new.name.as_deref() {
            tx.execute(
                "INSERT INTO snapshot_alias (job_id, name, snapshot_id, updated_at) \
                 VALUES (?1, ?2, ?3, ?4);",
                params![new.job_id, name, new.snapshot_id, now],
            )?;
        }
        let record = Self::get_snapshot_on(tx, &new.snapshot_id)?
            .context("snapshot row vanished inside its own transaction")?;
        Ok(record)
    }

    /// Apply one worker report (see [`SnapshotReportOutcome`]). Forwards to
    /// the scheduler-lane leader when this manager is not it, because a
    /// heartbeat can land on any manager.
    pub fn apply_snapshot_report(
        &self,
        node_id: &str,
        report: &SnapshotReportRecord,
    ) -> R<SnapshotReportOutcome> {
        match self.apply_snapshot_report_local(node_id, report) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::ApplySnapshotReport {
                        node_id: node_id.to_owned(),
                        report: report.clone(),
                    },
                    err,
                ),
            other => other,
        }
    }

    pub(crate) fn apply_snapshot_report_local(
        &self,
        node_id: &str,
        report: &SnapshotReportRecord,
    ) -> R<SnapshotReportOutcome> {
        let node_id = node_id.to_owned();
        let report = report.clone();
        self.replicated(Lane::Scheduler, "apply_snapshot_report", move |s| {
            s.apply_snapshot_report_apply(&node_id, &report)
        })
    }

    fn apply_snapshot_report_apply(
        &self,
        node_id: &str,
        report: &SnapshotReportRecord,
    ) -> R<SnapshotReportOutcome> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let mut post = super::suspend_ops::ReportPostCommit::default();
        let outcome = match report.op.as_str() {
            "capture" => {
                let Some(snapshot) = Self::get_snapshot_on(&tx, &report.snapshot_id)? else {
                    // A suspend the node took on its own under memory
                    // pressure: no row until now.
                    if report.local_suspension && report.ok {
                        let outcome =
                            self.apply_local_suspension_tx(&tx, node_id, report, &mut post)?;
                        tx.commit()?;
                        self.publish_staged(post.stopping_event);
                        self.cache_remove_allocations(&post.allocations)?;
                        return Ok(outcome);
                    }
                    return Ok(SnapshotReportOutcome::Ignored);
                };
                if snapshot.node_id.as_deref() != Some(node_id) {
                    return Ok(SnapshotReportOutcome::Ignored);
                }
                match snapshot.state {
                    SnapshotState::Capturing if snapshot.purpose == SnapshotPurpose::Suspend => {
                        self.apply_suspend_capture_tx(&tx, snapshot, report, &mut post)?
                    }
                    // Phase 6: a fork capture also releases its waiting forks.
                    SnapshotState::Capturing if snapshot.purpose == SnapshotPurpose::Fork => {
                        self.apply_fork_capture_tx(&tx, snapshot, report)?
                    }
                    SnapshotState::Capturing if report.ok => {
                        tx.execute(
                            "UPDATE snapshot SET state = 'local', manifest_json = ?2, \
                             total_bytes = ?3, duration_ms = ?4, error = NULL, pause_ms = ?5 \
                             WHERE snapshot_id = ?1;",
                            params![
                                report.snapshot_id,
                                report.manifest_json,
                                report.total_bytes as i64,
                                report.duration_ms as i64,
                                report.pause_ms as i64
                            ],
                        )?;
                        let evicted = self.apply_snapshot_keep_tx(&tx, &snapshot)?;
                        // A manual snapshot can seed new instances
                        // (`from_snapshot`), which start with copies of this
                        // instance's tokens.
                        if let Some(instance_id) = snapshot.instance_id.as_deref() {
                            Self::advance_identity_epoch_tx(&tx, instance_id)?;
                        }
                        let snapshot = Self::get_snapshot_on(&tx, &report.snapshot_id)?
                            .context("snapshot row vanished")?;
                        SnapshotReportOutcome::Captured { snapshot, evicted }
                    }
                    SnapshotState::Capturing => {
                        tx.execute(
                            "UPDATE snapshot SET state = 'failed', error = ?2, duration_ms = ?3 \
                             WHERE snapshot_id = ?1;",
                            params![report.snapshot_id, report.error, report.duration_ms as i64],
                        )?;
                        let snapshot = Self::get_snapshot_on(&tx, &report.snapshot_id)?
                            .context("snapshot row vanished")?;
                        SnapshotReportOutcome::CaptureFailed { snapshot }
                    }
                    // Deleted while the node was still capturing: keep the
                    // manifest so the delete request names exactly what to
                    // reclaim, and stay `deleting`.
                    SnapshotState::Deleting if report.ok && snapshot.manifest_json.is_empty() => {
                        tx.execute(
                            "UPDATE snapshot SET manifest_json = ?2, total_bytes = ?3 \
                             WHERE snapshot_id = ?1;",
                            params![
                                report.snapshot_id,
                                report.manifest_json,
                                report.total_bytes as i64
                            ],
                        )?;
                        SnapshotReportOutcome::Ignored
                    }
                    _ => SnapshotReportOutcome::Ignored,
                }
            }
            "delete" => {
                let Some(snapshot) = Self::get_snapshot_on(&tx, &report.snapshot_id)? else {
                    return Ok(SnapshotReportOutcome::Ignored);
                };
                if snapshot.state != SnapshotState::Deleting
                    || snapshot.node_id.as_deref() != Some(node_id)
                {
                    return Ok(SnapshotReportOutcome::Ignored);
                }
                if report.ok {
                    tx.execute(
                        "DELETE FROM snapshot WHERE snapshot_id = ?1;",
                        [&report.snapshot_id],
                    )?;
                    tx.execute(
                        "DELETE FROM snapshot_alias WHERE snapshot_id = ?1;",
                        [&report.snapshot_id],
                    )?;
                    SnapshotReportOutcome::Deleted { snapshot }
                } else {
                    tx.execute(
                        "UPDATE snapshot SET error = ?2 WHERE snapshot_id = ?1;",
                        params![report.snapshot_id, report.error],
                    )?;
                    let snapshot = Self::get_snapshot_on(&tx, &report.snapshot_id)?
                        .context("snapshot row vanished")?;
                    SnapshotReportOutcome::DeleteFailed { snapshot }
                }
            }
            "publish" => {
                let Some(snapshot) = Self::get_snapshot_on(&tx, &report.snapshot_id)? else {
                    return Ok(SnapshotReportOutcome::Ignored);
                };
                let (attempts, retry_at): (i64, Option<i64>) = tx.query_row(
                    "SELECT publish_attempts, publish_retry_at FROM snapshot \
                     WHERE snapshot_id = ?1;",
                    [&report.snapshot_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let now = chrono::Utc::now().timestamp();
                // After a failure the row carries the error until its retry
                // is due; a report arriving before then is a replay of the
                // failure already counted.
                let retry_due = retry_at.is_some_and(|at| at <= now);
                if snapshot.state != SnapshotState::Local
                    || snapshot.durability != SnapshotDurability::Store
                    || (snapshot.error.is_some() && !retry_due)
                    || snapshot.node_id.as_deref() != Some(node_id)
                {
                    return Ok(SnapshotReportOutcome::Ignored);
                }
                if report.ok {
                    tx.execute(
                        "UPDATE snapshot SET state = 'published', manifest_json = ?2, \
                         error = NULL, published_at = ?3, publish_retry_at = NULL \
                         WHERE snapshot_id = ?1;",
                        params![report.snapshot_id, report.manifest_json, now],
                    )?;
                    let snapshot = Self::get_snapshot_on(&tx, &report.snapshot_id)?
                        .context("snapshot row vanished")?;
                    // A spread fork waits for its checkpoint to be published,
                    // since its forks may be placed on any node.
                    let released_forks = if snapshot.purpose == SnapshotPurpose::Fork {
                        self.release_forks_tx(&tx, &snapshot)?
                    } else {
                        Vec::new()
                    };
                    SnapshotReportOutcome::Published {
                        snapshot,
                        uploaded_bytes: report.total_bytes,
                        duration_ms: report.duration_ms,
                        released_forks,
                    }
                } else {
                    // Retried by re-sending `op = publish` after a backoff: a
                    // flaky store or an expired credential often recovers.
                    // After the last attempt the error stays and stops the
                    // re-sends, and the snapshot keeps working from its node.
                    let attempts = attempts + 1;
                    let retry_in = publish_retry_backoff(attempts as u32);
                    let error = match retry_in {
                        Some(delay) => format!(
                            "publish attempt {attempts} of {PUBLISH_ATTEMPTS} failed, retrying \
                             in {}s: {}",
                            delay.as_secs(),
                            report.error
                        ),
                        None => format!(
                            "publish failed after {PUBLISH_ATTEMPTS} attempts: {}",
                            report.error
                        ),
                    };
                    tx.execute(
                        "UPDATE snapshot SET error = ?2, publish_attempts = ?3, \
                         publish_retry_at = ?4 WHERE snapshot_id = ?1;",
                        params![
                            report.snapshot_id,
                            error,
                            attempts,
                            retry_in.map(|delay| now + delay.as_secs() as i64)
                        ],
                    )?;
                    // A spread fork that can never be published has nothing
                    // its forks could start from on another node.
                    let terminated_forks =
                        if retry_in.is_none() && snapshot.purpose == SnapshotPurpose::Fork {
                            self.fail_waiting_forks_tx(&tx, &snapshot, &error)?
                        } else {
                            Vec::new()
                        };
                    let snapshot = Self::get_snapshot_on(&tx, &report.snapshot_id)?
                        .context("snapshot row vanished")?;
                    SnapshotReportOutcome::PublishFailed {
                        snapshot,
                        retrying: retry_in.is_some(),
                        terminated_forks,
                    }
                }
            }
            "restore" => {
                // Only the allocation the instance is currently bound to may
                // clear its restore source: a late report from an allocation
                // that was already replaced must not cancel a newer restore.
                let instance: Option<(String, String)> = tx
                    .query_row(
                        "SELECT instance_id, job_id FROM instance \
                         WHERE restore_from = ?1 AND current_alloc_id = ?2;",
                        params![report.snapshot_id, report.allocation_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                let Some((instance_id, job_id)) = instance else {
                    return Ok(SnapshotReportOutcome::Ignored);
                };
                if let Some(outcome) = self.apply_resume_restore_tx(
                    &tx,
                    &instance_id,
                    &job_id,
                    report,
                    node_id,
                    &mut post,
                )? {
                    outcome
                } else if let Some(outcome) =
                    self.apply_new_instance_restore_tx(&tx, &instance_id, &job_id, report, node_id)?
                {
                    outcome
                } else {
                    tx.execute(
                        "UPDATE instance SET restore_from = NULL WHERE instance_id = ?1;",
                        [&instance_id],
                    )?;
                    if report.ok {
                        tx.execute(
                            "UPDATE instance SET last_restore_ms = ?2, \
                             restored_from_snapshot_id = ?3 WHERE instance_id = ?1;",
                            params![instance_id, report.duration_ms as i64, report.snapshot_id],
                        )?;
                    } else {
                        // A rollback whose restore failed: the replacement
                        // boots cold on the retained storage.
                        tx.execute(
                            "UPDATE instance SET cold_boot = ?2 WHERE instance_id = ?1;",
                            params![
                                instance_id,
                                crate::structs::suspend::InstanceColdBoot::record_now(
                                    &report.snapshot_id,
                                    &report.error
                                )
                            ],
                        )?;
                    }
                    SnapshotReportOutcome::Restored {
                        snapshot_id: report.snapshot_id.clone(),
                        job_id,
                        instance_id: Some(instance_id),
                        allocation_id: report.allocation_id,
                        ok: report.ok,
                        error: report.error.clone(),
                    }
                }
            }
            other => {
                warn!(
                    node_id,
                    snapshot_id = %report.snapshot_id,
                    op = other,
                    "Ignoring snapshot report with an unknown op"
                );
                SnapshotReportOutcome::Ignored
            }
        };
        tx.commit()?;
        self.publish_staged(post.stopping_event);
        self.cache_remove_allocations(&post.allocations)?;
        Ok(outcome)
    }

    /// `snapshots.keep`: after `captured` lands, move the oldest `local`
    /// manual snapshots of the same instance beyond the task's keep count to
    /// `deleting`. Returns the ids moved.
    fn apply_snapshot_keep_tx(&self, tx: &Connection, captured: &SnapshotRecord) -> R<Vec<String>> {
        let Some(instance_id) = captured.instance_id.as_deref() else {
            return Ok(Vec::new());
        };
        if captured.purpose != SnapshotPurpose::Manual {
            return Ok(Vec::new());
        }
        let keep = self
            .snapshot_settings_for(&captured.job_id, &captured.task_name)
            .map(|settings| settings.keep)
            .unwrap_or(crate::structs::snapshot::DEFAULT_SNAPSHOT_KEEP);
        let mut stmt = tx.prepare(
            "SELECT snapshot_id FROM snapshot \
             WHERE origin_instance_id = ?1 AND purpose = 'manual' \
             AND state IN ('local', 'published') \
             ORDER BY created_at DESC, rowid DESC;",
        )?;
        let ids: Vec<String> =
            Self::collect_rows(stmt.query_map([instance_id], |row| row.get(0))?)?;
        drop(stmt);
        let evicted: Vec<String> = ids.into_iter().skip(keep as usize).collect();
        for id in &evicted {
            tx.execute(
                "UPDATE snapshot SET state = 'deleting' WHERE snapshot_id = ?1;",
                [id],
            )?;
        }
        Ok(evicted)
    }

    /// The resolved `snapshots` block of the job's current config for
    /// `task_name`, when it declares one.
    pub fn snapshot_settings_for(
        &self,
        job_id: &str,
        task_name: &str,
    ) -> Option<crate::structs::snapshot::ResolvedSnapshots> {
        let config = self.get_job_config(job_id).ok()?;
        let task = config.tasks.iter().find(|task| task.name == task_name)?;
        task.snapshots.as_ref()?.resolve().ok()
    }

    /// Delete a snapshot (see the DeleteSnapshot RPC).
    #[instrument(err, skip_all, fields(snapshot_id))]
    pub fn delete_snapshot(&self, snapshot_id: &str) -> R<DeleteSnapshotOutcome> {
        let snapshot_id = snapshot_id.to_owned();
        self.replicated(Lane::Scheduler, "delete_snapshot", move |s| {
            s.delete_snapshot_apply(&snapshot_id)
        })
    }

    fn delete_snapshot_apply(&self, snapshot_id: &str) -> R<DeleteSnapshotOutcome> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let snapshot = Self::get_snapshot_on(&tx, snapshot_id)?
            .ok_or_else(|| StorageError::not_found("snapshot", snapshot_id))?;
        let outcome = Self::discard_snapshot_tx(&tx, snapshot)?;
        tx.commit()?;
        Ok(outcome)
    }

    /// Start deleting `snapshot` inside the caller's transaction: rows with
    /// bytes on a live node move to `deleting` for the node to reclaim, rows
    /// with nothing left to reclaim are removed at once.
    pub(crate) fn discard_snapshot_tx(
        tx: &Connection,
        snapshot: SnapshotRecord,
    ) -> R<DeleteSnapshotOutcome> {
        let snapshot_id = snapshot.snapshot_id.clone();
        let snapshot_id = snapshot_id.as_str();
        let outcome = match snapshot.state {
            SnapshotState::Deleting => DeleteSnapshotOutcome::AlreadyDeleting,
            // Nothing on a node holds a failed capture (the worker drops the
            // partial parts) or a lost one (its node is gone).
            SnapshotState::Failed | SnapshotState::Lost => {
                Self::remove_snapshot_row_tx(tx, snapshot_id)?;
                DeleteSnapshotOutcome::Removed(Box::new(snapshot))
            }
            SnapshotState::Capturing | SnapshotState::Local | SnapshotState::Published => {
                let node_exists = match snapshot.node_id.as_deref() {
                    Some(node_id) => tx
                        .query_row("SELECT 1 FROM node WHERE id = ?1;", [node_id], |_| Ok(()))
                        .optional()?
                        .is_some(),
                    None => false,
                };
                if node_exists {
                    tx.execute(
                        "UPDATE snapshot SET state = 'deleting', error = NULL \
                         WHERE snapshot_id = ?1;",
                        [snapshot_id],
                    )?;
                    DeleteSnapshotOutcome::Deleting
                } else {
                    Self::remove_snapshot_row_tx(tx, snapshot_id)?;
                    DeleteSnapshotOutcome::Removed(Box::new(snapshot))
                }
            }
        };
        Ok(outcome)
    }

    pub(crate) fn remove_snapshot_row_tx(tx: &Connection, snapshot_id: &str) -> R<()> {
        tx.execute(
            "DELETE FROM snapshot WHERE snapshot_id = ?1;",
            [snapshot_id],
        )?;
        tx.execute(
            "DELETE FROM snapshot_alias WHERE snapshot_id = ?1;",
            [snapshot_id],
        )?;
        Ok(())
    }

    /// `snapshots.ttl`: move every `local` snapshot past its `expires_at` to
    /// `deleting`, and remove `failed` or `lost` rows past theirs outright.
    /// Returns `(moved to deleting, removed)`.
    #[instrument(err, skip_all)]
    pub fn expire_snapshots(&self, now_unix: i64) -> R<(Vec<String>, Vec<SnapshotRecord>)> {
        // Cheap read first, so the sweep proposes nothing on a cluster with
        // nothing to expire.
        {
            let conn = self.read_connection()?;
            let due: i64 = conn.query_row(
                "SELECT COUNT(*) FROM snapshot WHERE expires_at IS NOT NULL \
                 AND expires_at <= ?1 AND state IN ('local', 'published', 'failed', 'lost');",
                [now_unix],
                |row| row.get(0),
            )?;
            if due == 0 {
                return Ok((Vec::new(), Vec::new()));
            }
        }
        self.replicated(Lane::Scheduler, "expire_snapshots", move |s| {
            s.expire_snapshots_apply(now_unix)
        })
    }

    fn expire_snapshots_apply(&self, now_unix: i64) -> R<(Vec<String>, Vec<SnapshotRecord>)> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let mut stmt = tx.prepare(&format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshot WHERE expires_at IS NOT NULL \
             AND expires_at <= ?1 AND state IN ('local', 'published', 'failed', 'lost');"
        ))?;
        let due = Self::collect_rows(stmt.query_map([now_unix], Self::snapshot_from_row)?)?;
        drop(stmt);
        let mut deleting = Vec::new();
        let mut removed = Vec::new();
        for snapshot in due {
            // A snapshot an instance is restoring from waits for the restore
            // to report before it expires.
            let in_use: bool = tx
                .query_row(
                    "SELECT 1 FROM instance WHERE restore_from = ?1 LIMIT 1;",
                    [&snapshot.snapshot_id],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if in_use {
                continue;
            }
            if snapshot.state.is_restorable() {
                // A published snapshot whose node is gone has nothing left to
                // reclaim on a node, so it goes at once.
                match Self::discard_snapshot_tx(&tx, snapshot.clone())? {
                    DeleteSnapshotOutcome::Removed(gone) => removed.push(*gone),
                    _ => deleting.push(snapshot.snapshot_id),
                }
            } else {
                Self::remove_snapshot_row_tx(&tx, &snapshot.snapshot_id)?;
                removed.push(snapshot);
            }
        }
        tx.commit()?;
        Ok((deleting, removed))
    }

    /// Mark every snapshot held only by a removed node `lost`: its bytes went
    /// with the node. A `published` snapshot is untouched, because its parts
    /// live in the content store and any compatible node restores it. A
    /// `local` snapshot whose publish had not finished is lost too: the node
    /// that would have uploaded it is gone. Called by [`Store::remove_node`].
    pub fn mark_node_snapshots_lost(&self, node_id: &str) -> R<usize> {
        let node_id = node_id.to_owned();
        self.replicated(Lane::Scheduler, "mark_node_snapshots_lost", move |s| {
            let conn = s.write_connection()?;
            Self::mark_node_snapshots_lost_tx(&conn, &node_id)
        })
    }

    pub(crate) fn mark_node_snapshots_lost_tx(tx: &Connection, node_id: &str) -> R<usize> {
        // A snapshot already being deleted has nothing left to reclaim.
        tx.execute(
            "DELETE FROM snapshot_alias WHERE snapshot_id IN (\
                 SELECT snapshot_id FROM snapshot WHERE node_id = ?1 AND state = 'deleting');",
            [node_id],
        )?;
        tx.execute(
            "DELETE FROM snapshot WHERE node_id = ?1 AND state = 'deleting';",
            [node_id],
        )?;
        Ok(tx.execute(
            "UPDATE snapshot SET state = 'lost', \
             error = 'its node was removed from the cluster' \
             WHERE node_id = ?1 AND state IN ('capturing', 'local');",
            [node_id],
        )?)
    }

    /// Job destroy's snapshot cleanup, inside the scheduler-lane cleanup
    /// transaction: snapshots with bytes on a node move to `deleting` so the
    /// node reclaims them, rows with nothing to reclaim are removed, and the
    /// job's names are freed.
    pub(crate) fn destroy_job_snapshots_tx(tx: &Connection, job_id: &str) -> R<()> {
        tx.execute(
            "UPDATE snapshot SET state = 'deleting' \
             WHERE job_id = ?1 AND state IN ('capturing', 'local', 'published');",
            [job_id],
        )?;
        tx.execute(
            "DELETE FROM snapshot WHERE job_id = ?1 AND state IN ('failed', 'lost');",
            [job_id],
        )?;
        tx.execute("DELETE FROM snapshot_alias WHERE job_id = ?1;", [job_id])?;
        Ok(())
    }

    /// Roll a keyed instance back to `snapshot_id` (the RestoreInstance
    /// RPC's storage half): stop its allocation with cause `restore`, move it
    /// to `pending`, and record the restore source in the same transaction.
    /// The RPC handler validates ownership and states first; this re-checks
    /// the ones a concurrent write could change.
    #[instrument(err, skip_all, fields(instance_id, snapshot_id))]
    pub fn restore_keyed_instance(
        &self,
        instance_id: &str,
        snapshot_id: &str,
    ) -> R<RestoreInstanceOutcome> {
        let instance_id = instance_id.to_owned();
        let snapshot_id = snapshot_id.to_owned();
        self.replicated(Lane::Scheduler, "restore_keyed_instance", move |s| {
            s.restore_keyed_instance_apply(&instance_id, &snapshot_id)
        })
    }

    fn restore_keyed_instance_apply(
        &self,
        instance_id: &str,
        snapshot_id: &str,
    ) -> R<RestoreInstanceOutcome> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let snapshot = Self::get_snapshot_on(&tx, snapshot_id)?
            .ok_or_else(|| StorageError::not_found("snapshot", snapshot_id))?;
        if !snapshot.state.is_restorable() {
            bail!(StorageError::ForeignKey {
                message: format!(
                    "snapshot {snapshot_id} is {}, not local or published; only a captured \
                     snapshot can be restored",
                    snapshot.state
                ),
            });
        }
        let state: Option<String> = tx
            .query_row(
                "SELECT state FROM instance WHERE instance_id = ?1 AND mode = 'keyed';",
                [instance_id],
                |row| row.get(0),
            )
            .optional()?;
        match state.as_deref() {
            None => bail!(StorageError::not_found("instance", instance_id)),
            Some("pending" | "active" | "rescheduling") => {}
            Some(other) => bail!(StorageError::ForeignKey {
                message: format!(
                    "instance {instance_id} is {other}; start it before restoring a snapshot"
                ),
            }),
        }

        let mut stmt = tx.prepare(
            "SELECT allocation.id, allocation.status, allocation.resources, allocation.node_id, \
             allocation.job_config_id, allocation.task_config_id, job_config.job_id, \
             allocation.instance_id, allocation.network \
             FROM allocation \
             INNER JOIN job_config ON job_config.id = allocation.job_config_id \
             WHERE allocation.instance_id = ?;",
        )?;
        let allocations = Self::collect_rows(
            stmt.query_map([instance_id], |row| self.allocation_from_row(row))?,
        )?;
        drop(stmt);

        let now = Self::now_rfc3339()?;
        tx.execute(
            "DELETE FROM allocation WHERE instance_id = ?;",
            [instance_id],
        )?;
        tx.execute(
            "UPDATE instance SET state = 'pending', current_alloc_id = NULL, \
             restore_from = ?1, cold_boot = NULL, updated_at = ?2 WHERE instance_id = ?3;",
            params![snapshot_id, now, instance_id],
        )?;
        self.bump_epochs_for_allocations_tx(&tx, &allocations)?;
        let stopping_event = self.record_stop_notices_tx(&tx, &allocations, StopCause::Restore)?;
        tx.commit()?;
        self.publish_staged(stopping_event);
        self.cache_remove_allocations(&allocations)?;
        info!(
            instance_id,
            snapshot_id,
            removed_allocations = allocations.len(),
            "Restoring keyed instance from snapshot"
        );
        Ok(RestoreInstanceOutcome {
            removed_allocations: allocations.len(),
        })
    }

    /// Restoring instances of a keyed task awaiting placement, with where
    /// their snapshot lives. Only called for keyed tasks, so slot and
    /// stateless placements pay nothing for it.
    pub fn restore_pins_for_task(&self, task_id: i64) -> R<Vec<RestorePin>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT instance.instance_id, snapshot.snapshot_id, snapshot.node_id, \
             snapshot.manifest_json, snapshot.state, snapshot.purpose, snapshot.durability \
             FROM instance \
             INNER JOIN task_config ON task_config.id = ?1 \
             INNER JOIN snapshot ON snapshot.snapshot_id = instance.restore_from \
             WHERE instance.job_id = task_config.job_id \
             AND instance.group_name = task_config.group_name \
             AND instance.task_name = task_config.name \
             AND instance.state IN ('pending', 'rescheduling') \
             AND instance.current_alloc_id IS NULL \
             AND COALESCE(instance.pending_op, '') != 'fork' \
             ORDER BY instance.created_at ASC, instance.instance_id ASC;",
        )?;
        let rows = stmt.query_map([task_id], |row| {
            Ok(RestorePin {
                instance_id: row.get(0)?,
                snapshot_id: row.get(1)?,
                node_id: row.get(2)?,
                manifest_json: row.get(3)?,
                published: row.get::<_, String>(4)? == SnapshotState::Published.as_str(),
                // A fork checkpoint is published only for --spread.
                spread: row.get::<_, String>(5)? == SnapshotPurpose::Fork.as_str()
                    && row.get::<_, String>(6)? == SnapshotDurability::Store.as_str(),
            })
        })?;
        Self::collect_rows(rows)
    }

    /// The nodes running an instance forked from `snapshot_id` (restoring
    /// from it, or restored from it): where a spread fork's siblings are.
    pub fn nodes_running_forks_of(&self, snapshot_id: &str) -> R<HashSet<String>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT DISTINCT allocation.node_id FROM instance \
             INNER JOIN allocation ON allocation.id = instance.current_alloc_id \
             WHERE (instance.restore_from = ?1 OR instance.restored_from_snapshot_id = ?1) \
             AND instance.state != 'terminated';",
        )?;
        let rows = stmt.query_map([snapshot_id], |row| row.get(0))?;
        Ok(Self::collect_rows(rows)?.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::config::ManagerConfigFull;

    fn store() -> R<Store> {
        let dir = tempfile::Builder::new()
            .prefix("legion-snapshot-ops-")
            .tempdir()?
            .keep();
        Store::start_storage(&ManagerConfigFull {
            store_path: dir.to_str().context("utf-8 path")?.to_string(),
            ..Default::default()
        })
    }

    fn report(op: &str, ok: bool) -> SnapshotReportRecord {
        SnapshotReportRecord {
            snapshot_id: "snap-retry".into(),
            op: op.into(),
            ok,
            error: if ok {
                String::new()
            } else {
                "bucket said 503".into()
            },
            manifest_json: "{}".into(),
            duration_ms: 1,
            total_bytes: 1,
            allocation_id: 7,
            local_suspension: false,
            pause_ms: 0,
        }
    }

    fn work(store: &Store) -> R<Vec<String>> {
        Ok(store
            .snapshot_work_for_node("n1")?
            .into_iter()
            .map(|snapshot| snapshot.snapshot_id)
            .collect())
    }

    /// Make the pending retry due now, as if its backoff had passed.
    fn backoff_passes(store: &Store) -> R<()> {
        store.write_connection()?.execute(
            "UPDATE snapshot SET publish_retry_at = publish_retry_at - 3600 \
             WHERE snapshot_id = 'snap-retry';",
            [],
        )?;
        Ok(())
    }

    /// A failed publish is re-sent after a backoff, up to
    /// [`PUBLISH_ATTEMPTS`] attempts, and a replay of a counted failure is
    /// ignored. After the last attempt the snapshot stays `local` for good.
    #[test]
    fn a_failed_publish_is_retried_with_backoff_then_given_up() -> R<()> {
        let store = store()?;
        store.create_snapshot(NewSnapshot {
            snapshot_id: "snap-retry".into(),
            job_id: "job".into(),
            task_name: "task".into(),
            instance_id: "user-42".into(),
            name: None,
            kind: SnapshotKind::Memory,
            purpose: SnapshotPurpose::Manual,
            node_id: "n1".into(),
            allocation_id: 7,
            expires_at: None,
            suspend_reason: None,
            durability: SnapshotDurability::Store,
        })?;
        store.apply_snapshot_report("n1", &report("capture", true))?;
        assert_eq!(work(&store)?, vec!["snap-retry"], "publish is due");

        for attempt in 1..PUBLISH_ATTEMPTS {
            let outcome = store.apply_snapshot_report("n1", &report("publish", false))?;
            let SnapshotReportOutcome::PublishFailed {
                snapshot, retrying, ..
            } = outcome
            else {
                panic!("attempt {attempt}: {outcome:?}");
            };
            assert!(retrying, "attempt {attempt}");
            let error = snapshot.error.unwrap_or_default();
            assert!(
                error.starts_with(&format!(
                    "publish attempt {attempt} of {PUBLISH_ATTEMPTS} failed, retrying in"
                )),
                "{error}"
            );
            assert!(work(&store)?.is_empty(), "not re-sent before the backoff");
            assert!(
                matches!(
                    store.apply_snapshot_report("n1", &report("publish", false))?,
                    SnapshotReportOutcome::Ignored
                ),
                "a replayed failure is not counted twice"
            );
            backoff_passes(&store)?;
            assert_eq!(work(&store)?, vec!["snap-retry"], "re-sent once due");
        }

        let outcome = store.apply_snapshot_report("n1", &report("publish", false))?;
        let SnapshotReportOutcome::PublishFailed {
            snapshot, retrying, ..
        } = outcome
        else {
            panic!("last attempt: {outcome:?}");
        };
        assert!(!retrying);
        assert_eq!(snapshot.state, SnapshotState::Local);
        assert!(snapshot
            .error
            .unwrap_or_default()
            .starts_with("publish failed after 3 attempts: bucket said 503"));
        assert!(work(&store)?.is_empty(), "given up");
        Ok(())
    }

    /// A retry that succeeds publishes the snapshot as usual.
    #[test]
    fn a_retried_publish_can_succeed() -> R<()> {
        let store = store()?;
        store.create_snapshot(NewSnapshot {
            snapshot_id: "snap-retry".into(),
            job_id: "job".into(),
            task_name: "task".into(),
            instance_id: "user-42".into(),
            name: None,
            kind: SnapshotKind::Memory,
            purpose: SnapshotPurpose::Manual,
            node_id: "n1".into(),
            allocation_id: 7,
            expires_at: None,
            suspend_reason: None,
            durability: SnapshotDurability::Store,
        })?;
        store.apply_snapshot_report("n1", &report("capture", true))?;
        store.apply_snapshot_report("n1", &report("publish", false))?;
        backoff_passes(&store)?;
        let outcome = store.apply_snapshot_report("n1", &report("publish", true))?;
        let SnapshotReportOutcome::Published { snapshot, .. } = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(snapshot.state, SnapshotState::Published);
        assert!(snapshot.error.is_none());
        assert!(work(&store)?.is_empty());
        Ok(())
    }
}
