//! Suspended instances: the store half of SuspendInstance, resume through
//! StartInstance, and the resume sweep (suspend plan §6.6, §7;
//! docs/suspend-and-resume.md).
//!
//! A suspend rides the snapshot checkpoint channel. SuspendInstance inserts a
//! `purpose = suspend` snapshot row and marks the instance
//! `pending_op = suspend` ([`Store::begin_suspend`]). The worker captures and
//! stops the allocation, and its report applies the suspend in one
//! transaction ([`Store::apply_suspend_capture_tx`], called from
//! `apply_snapshot_report`). A resume sets `restore_from` to the checkpoint
//! and `pending_op = resume` ([`Store::resume_suspended_instance`]), and the
//! restore's report settles it. Every write is a scheduler-lane op, beside the
//! `instance`, `allocation`, and `snapshot` rows it touches.

use super::*;
use crate::structs::snapshot::{SnapshotPurpose, SnapshotRecord, SnapshotState};
use crate::structs::suspend::{
    ArchiveMode, InstanceColdBoot, InstanceSuspendState, OnRestoreFailure, ResolvedSuspend,
    PENDING_RESUME, PENDING_SUSPEND, SUSPEND_REASON_MANUAL, SUSPEND_REASON_PARK,
    SUSPEND_REASON_PRESSURE,
};

/// The `suspend_reason` a local suspension records: `park` when the node
/// says the workload parked itself, `pressure` for anything else, which is
/// what every local suspension meant before parks existed.
pub(crate) fn local_suspension_reason(reported: &str) -> &'static str {
    if reported == SUSPEND_REASON_PARK {
        SUSPEND_REASON_PARK
    } else {
        SUSPEND_REASON_PRESSURE
    }
}

const SUSPEND_COLUMNS: &str = "checkpoint_id, suspend_reason, suspended_at, pending_op, \
     restored_from_snapshot_id, last_suspend_ms, last_restore_ms, resume_requested_at, \
     lifecycle_error, cold_boot, manifest_url";

/// Work [`Store::apply_snapshot_report`] finishes after its transaction
/// commits: publish the staged stop event and refresh the allocation cache.
#[derive(Default)]
pub(crate) struct ReportPostCommit {
    pub stopping_event: Option<SchedulerEventRecord>,
    pub allocations: Vec<Allocation>,
}

/// Outcome of [`Store::resume_suspended_instance`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeOutcome {
    /// The instance is `pending` with `restore_from` set: placement pins it
    /// to the checkpoint's node (`node_id`) and the worker restores from it.
    /// `node_id` is `None` for a published checkpoint, which restores on
    /// any compatible node.
    Resuming {
        snapshot_id: String,
        node_id: Option<String>,
    },
    /// The instance is `pending` and boots fresh on its retained storage.
    /// `reason` is set when the checkpoint could not be used (it was lost or
    /// failed) and `onRestoreFailure = "cold"` chose a cold boot; `None` for
    /// an explicit `--cold`. `discarded` names the checkpoint thrown away.
    Cold {
        reason: Option<String>,
        discarded: Option<String>,
    },
    /// The checkpoint cannot be used and `onRestoreFailure = "fail"`: the
    /// instance stays `suspended` with `reason` in its detail.
    RestoreUnavailable { reason: String },
}

/// What a resume of an archived instance read back from its archive
/// manifest (see [`Store::resume_archived_instance`]).
#[derive(Debug, Clone)]
pub enum ArchivedCheckpoint {
    /// The manifest was read: the snapshot row to put back.
    Read(Box<SnapshotRecord>),
    /// It could not be read (gone, unreadable, unreachable): why.
    Unreadable(String),
}

/// A suspended instance archived as its checkpoint was published (see
/// [`Store::archive_suspended_instance_tx`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchivedInstance {
    pub instance_id: String,
    pub job_id: String,
    pub snapshot_id: String,
    pub manifest_url: String,
    /// `archive = "manifestOnly"`: the instance row went too.
    pub row_deleted: bool,
}

/// A resume the sweep gave up on (see [`Store::settle_stalled_resumes`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbandonedResume {
    pub instance_id: String,
    pub job_id: String,
    pub snapshot_id: String,
    /// True: booted cold. False: back to `suspended` (`onRestoreFailure =
    /// "fail"` and the checkpoint is gone).
    pub cold: bool,
    pub reason: String,
}

/// A resume waiting to be placed on the one node holding its checkpoint
/// (see [`Store::waiting_pinned_resumes`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitingResume {
    pub instance_id: String,
    pub job_id: String,
    pub group: String,
    pub task_name: String,
    /// The node the checkpoint is on, which the resume is pinned to.
    pub node_id: String,
    /// Unix seconds when StartInstance asked for the resume.
    pub requested_at: i64,
}

fn suspend_state_from_row(
    row: &Row<'_>,
    at: usize,
) -> Result<InstanceSuspendState, rusqlite::Error> {
    Ok(InstanceSuspendState {
        checkpoint_id: row.get(at)?,
        suspend_reason: row.get(at + 1)?,
        suspended_at: row.get(at + 2)?,
        pending_op: row.get(at + 3)?,
        restored_from_snapshot_id: row.get(at + 4)?,
        last_suspend_ms: row.get(at + 5)?,
        last_restore_ms: row.get(at + 6)?,
        resume_requested_at: row.get(at + 7)?,
        lifecycle_error: row.get(at + 8)?,
        cold_boot: row
            .get::<_, Option<String>>(at + 9)?
            .and_then(|raw| serde_json::from_str(&raw).ok()),
        manifest_url: row.get(at + 10)?,
    })
}

impl Store {
    /// The suspend columns of one instance. Default (all unset) for an
    /// instance that does not exist.
    pub fn instance_suspend_state(&self, instance_id: &str) -> R<InstanceSuspendState> {
        let conn = self.read_connection()?;
        Self::instance_suspend_state_on(&conn, instance_id)
    }

    pub(crate) fn instance_suspend_state_on(
        conn: &Connection,
        instance_id: &str,
    ) -> R<InstanceSuspendState> {
        Ok(conn
            .query_row(
                &format!("SELECT {SUSPEND_COLUMNS} FROM instance WHERE instance_id = ?1;"),
                [instance_id],
                |row| suspend_state_from_row(row, 0),
            )
            .optional()?
            .unwrap_or_default())
    }

    /// The suspend columns of every instance of a job that has any, keyed by
    /// instance id. One query for `instance list`.
    pub fn instance_suspend_states_for_job(
        &self,
        job_id: &str,
    ) -> R<HashMap<String, InstanceSuspendState>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT instance_id, {SUSPEND_COLUMNS} FROM instance WHERE job_id = ?1 \
             AND (checkpoint_id IS NOT NULL OR pending_op IS NOT NULL \
             OR suspended_at IS NOT NULL OR last_restore_ms IS NOT NULL \
             OR lifecycle_error IS NOT NULL OR cold_boot IS NOT NULL);"
        ))?;
        let rows = stmt.query_map([job_id], |row| {
            Ok((row.get::<_, String>(0)?, suspend_state_from_row(row, 1)?))
        })?;
        Ok(Self::collect_rows(rows)?.into_iter().collect())
    }

    /// The resolved `suspend` block of the job's current config for
    /// `task_name`. Defaults when the task declares none (SuspendInstance
    /// works without the block).
    pub fn suspend_settings_for(&self, job_id: &str, task_name: &str) -> ResolvedSuspend {
        let conn = match self.read_connection() {
            Ok(conn) => conn,
            Err(_) => return ResolvedSuspend::default(),
        };
        Self::latest_suspend_settings_on(&conn, job_id, task_name)
    }

    /// The `suspend` block of the newest config of `(job, task)`, read
    /// inside whatever connection or transaction the caller holds, so every
    /// replica resolves the same value.
    fn latest_suspend_settings_on(
        conn: &Connection,
        job_id: &str,
        task_name: &str,
    ) -> ResolvedSuspend {
        let raw: Option<Option<String>> = conn
            .query_row(
                "SELECT task_config.suspend FROM task_config \
                 INNER JOIN job_config ON job_config.id = task_config.job_config_id \
                 WHERE task_config.job_id = ?1 AND task_config.name = ?2 \
                 ORDER BY job_config.version DESC LIMIT 1;",
                params![job_id, task_name],
                |row| row.get(0),
            )
            .optional()
            .ok()
            .flatten();
        Self::resolve_suspend_column(raw.flatten())
    }

    /// The `suspend` block of the task config an allocation runs.
    pub(super) fn allocation_suspend_settings_on(
        conn: &Connection,
        allocation_id: i64,
    ) -> ResolvedSuspend {
        let raw: Option<Option<String>> = conn
            .query_row(
                "SELECT task_config.suspend FROM task_config \
                 INNER JOIN allocation ON allocation.task_config_id = task_config.id \
                 WHERE allocation.id = ?1;",
                [allocation_id],
                |row| row.get(0),
            )
            .optional()
            .ok()
            .flatten();
        Self::resolve_suspend_column(raw.flatten())
    }

    fn resolve_suspend_column(raw: Option<String>) -> ResolvedSuspend {
        raw.and_then(|raw| {
            serde_json::from_str::<crate::structs::suspend::SuspendConfig>(&raw).ok()
        })
        .and_then(|config| config.resolve().ok())
        .unwrap_or_default()
    }

    fn instance_allocations_tx(&self, tx: &Connection, instance_id: &str) -> R<Vec<Allocation>> {
        let mut stmt = tx.prepare(
            "SELECT allocation.id, allocation.status, allocation.resources, allocation.node_id, \
             allocation.job_config_id, allocation.task_config_id, job_config.job_id, \
             allocation.instance_id, allocation.network \
             FROM allocation \
             INNER JOIN job_config ON job_config.id = allocation.job_config_id \
             WHERE allocation.instance_id = ?;",
        )?;
        let rows = stmt.query_map([instance_id], |row| self.allocation_from_row(row))?;
        Self::collect_rows(rows)
    }

    /// Keep a suspended instance's allocation row as its node charge, when
    /// it holds anything under its job's `suspend.reserve`
    /// (docs/suspend-and-resume.md, "Capacity for resumes"). The row turns
    /// `suspended`, which keeps it out of every worker's desired set, and
    /// below a full hold its resources shrink to the held share, so the fit
    /// check, the read model, and the plan-apply recheck all count exactly
    /// that share. Returns false when nothing is held and the caller deletes
    /// the row as before.
    fn hold_for_suspend_tx(
        &self,
        tx: &Connection,
        instance_id: &str,
        reserve: crate::structs::suspend::SuspendReserve,
        post: &mut ReportPostCommit,
    ) -> R<bool> {
        use crate::resources::SuspendedHold;
        let allocations = self.instance_allocations_tx(tx, instance_id)?;
        let Some(allocation) = allocations.first() else {
            return Ok(false);
        };
        let hold = self
            .resource_policy
            .suspended_hold(&allocation.resources, reserve)
            .unwrap_or_else(|err| {
                // A row whose resources do not parse cannot be scaled. Hold it
                // whole rather than free capacity nobody can account for.
                warn!(instance_id, error = %err, "Suspended hold fell back to the full reservation");
                SuspendedHold::Full
            });
        match hold {
            SuspendedHold::Nothing => return Ok(false),
            SuspendedHold::Full => {
                tx.execute(
                    "UPDATE allocation SET status = ?2 WHERE instance_id = ?1;",
                    params![instance_id, crate::constants::ALLOCATION_STATUS_SUSPENDED],
                )?;
            }
            SuspendedHold::Partial(resources) => {
                // Ports go with the share: a suspended instance serves
                // nothing, and the resume is placed with fresh ones.
                tx.execute(
                    "UPDATE allocation SET status = ?2, resources = ?3, network = NULL \
                     WHERE instance_id = ?1;",
                    params![
                        instance_id,
                        crate::constants::ALLOCATION_STATUS_SUSPENDED,
                        resources
                    ],
                )?;
            }
        }
        // The resources (and so the node's usage aggregate) may have changed
        // under the same row id: bump the epochs so the cached aggregate and
        // any plan built on the old numbers are rejected.
        self.bump_epochs_for_allocations_tx(tx, &allocations)?;
        post.allocations.extend(allocations);
        Ok(true)
    }

    /// Delete an instance's allocation rows as a suspend does: epochs bumped
    /// and stop notices recorded with cause `suspended`, so the worker and
    /// the event stream both see why.
    fn remove_for_suspend_tx(
        &self,
        tx: &Connection,
        instance_id: &str,
        post: &mut ReportPostCommit,
    ) -> R<()> {
        let allocations = self.instance_allocations_tx(tx, instance_id)?;
        if allocations.is_empty() {
            return Ok(());
        }
        tx.execute(
            "DELETE FROM allocation WHERE instance_id = ?;",
            [instance_id],
        )?;
        self.bump_epochs_for_allocations_tx(tx, &allocations)?;
        post.stopping_event =
            self.record_stop_notices_tx(tx, &allocations, StopCause::Suspended)?;
        post.allocations.extend(allocations);
        Ok(())
    }

    /// Discard an instance's suspend checkpoint and clear its suspend
    /// bookkeeping, inside the caller's transaction. Used when an instance
    /// leaves `suspended` other than by resuming (stop, destroy).
    pub(crate) fn clear_suspend_tx(tx: &Connection, instance_id: &str) -> R<()> {
        let state = Self::instance_suspend_state_on(tx, instance_id)?;
        if let Some(checkpoint) = state.checkpoint_id.as_deref() {
            if let Some(snapshot) = Self::get_snapshot_on(tx, checkpoint)? {
                if snapshot.purpose == SnapshotPurpose::Suspend {
                    Self::discard_snapshot_tx(tx, snapshot)?;
                }
            }
        }
        // A stop or destroy ends the run a cold boot record describes.
        tx.execute(
            "UPDATE instance SET checkpoint_id = NULL, pending_op = NULL, \
             resume_requested_at = NULL, cold_boot = NULL, manifest_url = NULL \
             WHERE instance_id = ?1;",
            [instance_id],
        )?;
        Ok(())
    }

    /// Start a suspend (the SuspendInstance RPC's storage half, and the idle
    /// sweep's): insert the `purpose = suspend` snapshot row the checkpoint
    /// channel captures, and mark the instance `pending_op = suspend` with
    /// `new.suspend_reason`, in one transaction. Refuses unless the instance
    /// is a keyed `active` instance still bound to `new.allocation_id` with
    /// no other operation outstanding.
    ///
    /// Forwarded to the scheduler-lane leader when this manager does not
    /// lead it: the idle sweep runs on whichever manager holds the node's
    /// usage report, which is whichever one the node heartbeats to.
    #[instrument(err, skip_all, fields(snapshot_id = %new.snapshot_id))]
    pub fn begin_suspend(&self, new: NewSnapshot) -> R<SnapshotRecord> {
        match self.begin_suspend_local(new.clone()) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::BeginSuspend { new },
                    err,
                ),
            other => other,
        }
    }

    /// [`Self::begin_suspend`] without forwarding (what a forwarded op runs).
    pub(crate) fn begin_suspend_local(&self, new: NewSnapshot) -> R<SnapshotRecord> {
        self.replicated(Lane::Scheduler, "begin_suspend", move |s| {
            s.begin_suspend_apply(&new)
        })
    }

    fn begin_suspend_apply(&self, new: &NewSnapshot) -> R<SnapshotRecord> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let row: Option<(String, String, Option<i64>, Option<String>)> = tx
            .query_row(
                "SELECT mode, state, current_alloc_id, pending_op FROM instance \
                 WHERE instance_id = ?1;",
                [&new.instance_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some((mode, state, current, pending)) = row else {
            bail!(StorageError::not_found("instance", &new.instance_id));
        };
        if mode != "keyed" || state != "active" || current != Some(new.allocation_id) {
            bail!(StorageError::Conflict {
                message: format!(
                    "instance {} changed while the suspend was being prepared (it is {state}); \
                     retry",
                    new.instance_id
                ),
            });
        }
        if let Some(pending) = pending {
            bail!(StorageError::Conflict {
                message: format!(
                    "instance {} is already being {}ed; wait for it to finish",
                    new.instance_id,
                    if pending == PENDING_SUSPEND {
                        "suspend"
                    } else {
                        "resum"
                    }
                ),
            });
        }
        let record = Self::create_snapshot_tx(&tx, new)?;
        tx.execute(
            "UPDATE instance SET pending_op = ?1, lifecycle_error = NULL, suspend_reason = ?3 \
             WHERE instance_id = ?2;",
            params![
                PENDING_SUSPEND,
                new.instance_id,
                new.suspend_reason
                    .as_deref()
                    .unwrap_or(SUSPEND_REASON_MANUAL)
            ],
        )?;
        tx.commit()?;
        Ok(record)
    }

    /// A worker's report on a suspend capture, inside
    /// `apply_snapshot_report`'s transaction.
    ///
    /// Success applies the suspend: the snapshot becomes `local`, the
    /// allocation is deleted (or, with `suspend.reserve`, kept with status
    /// `suspended` so its reservation holds), and the instance becomes
    /// `suspended` with `checkpoint_id`. A failure drops the snapshot row
    /// (the worker kept nothing) and leaves the instance `active`: the
    /// worker never stops a workload it failed to capture.
    pub(crate) fn apply_suspend_capture_tx(
        &self,
        tx: &Connection,
        snapshot: SnapshotRecord,
        report: &SnapshotReportRecord,
        post: &mut ReportPostCommit,
    ) -> R<SnapshotReportOutcome> {
        let Some(instance_id) = snapshot.instance_id.clone() else {
            return Ok(SnapshotReportOutcome::Ignored);
        };
        if !report.ok {
            Self::remove_snapshot_row_tx(tx, &snapshot.snapshot_id)?;
            tx.execute(
                "UPDATE instance SET pending_op = NULL, lifecycle_error = ?2 \
                 WHERE instance_id = ?1 AND pending_op = 'suspend';",
                params![instance_id, format!("suspend failed: {}", report.error)],
            )?;
            let snapshot = SnapshotRecord {
                state: SnapshotState::Failed,
                error: Some(report.error.clone()),
                duration_ms: report.duration_ms,
                ..snapshot
            };
            return Ok(SnapshotReportOutcome::SuspendFailed { snapshot });
        }

        tx.execute(
            "UPDATE snapshot SET state = 'local', manifest_json = ?2, total_bytes = ?3, \
             duration_ms = ?4, error = NULL WHERE snapshot_id = ?1;",
            params![
                snapshot.snapshot_id,
                report.manifest_json,
                report.total_bytes as i64,
                report.duration_ms as i64
            ],
        )?;
        let snapshot =
            Self::get_snapshot_on(tx, &snapshot.snapshot_id)?.context("snapshot row vanished")?;
        let instance: Option<(String, Option<i64>, Option<String>)> = tx
            .query_row(
                "SELECT state, current_alloc_id, pending_op FROM instance WHERE instance_id = ?1;",
                [&instance_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let still_suspending = matches!(
            instance.as_ref(),
            Some((state, current, pending))
                if state == "active"
                    && *current == snapshot.allocation_id
                    && pending.as_deref() == Some(PENDING_SUSPEND)
        );
        if !still_suspending {
            // Stopped, destroyed, or restored while the node was capturing.
            // Nothing will ever resume from this checkpoint: reclaim it.
            warn!(
                instance_id,
                snapshot_id = %snapshot.snapshot_id,
                "Suspend capture finished after its instance changed; discarding the checkpoint"
            );
            tx.execute(
                "UPDATE instance SET pending_op = NULL WHERE instance_id = ?1 \
                 AND pending_op = 'suspend';",
                [&instance_id],
            )?;
            Self::discard_snapshot_tx(tx, snapshot)?;
            return Ok(SnapshotReportOutcome::Ignored);
        }

        let allocation_id = snapshot.allocation_id.unwrap_or_default();
        let settings = Self::allocation_suspend_settings_on(tx, allocation_id);
        // Written when the suspend started (begin_suspend, or the local
        // suspension path below).
        let reason = Self::instance_suspend_state_on(tx, &instance_id)?
            .suspend_reason
            .unwrap_or_else(|| SUSPEND_REASON_MANUAL.to_string());
        let now = Self::now_rfc3339()?;
        let now_unix = OffsetDateTime::now_utc().unix_timestamp();
        let held = self.hold_for_suspend_tx(tx, &instance_id, settings.reserve, post)?;
        if !held {
            self.remove_for_suspend_tx(tx, &instance_id, post)?;
        }
        tx.execute(
            "UPDATE instance SET state = 'suspended', \
             current_alloc_id = CASE WHEN ?7 THEN current_alloc_id ELSE NULL END, \
             checkpoint_id = ?2, suspend_reason = ?3, suspended_at = ?4, pending_op = NULL, \
             last_suspend_ms = ?5, lifecycle_error = NULL, updated_at = ?6 \
             WHERE instance_id = ?1;",
            params![
                instance_id,
                snapshot.snapshot_id,
                reason,
                now_unix,
                report.duration_ms as i64,
                now,
                held
            ],
        )?;
        Ok(SnapshotReportOutcome::Suspended {
            snapshot,
            instance_id,
            reserved: held,
            reason,
        })
    }

    /// A local suspension (the node's `suspend` pressure step, plan §6.6,
    /// or a park whose deadline passed, docs/park.md): the node
    /// checkpointed and stopped an allocation on its own and reported a
    /// capture for a snapshot id it minted, which has no row yet. Inside
    /// `apply_snapshot_report`'s transaction.
    ///
    /// When the instance is still `active` on that allocation, insert the
    /// row as a `purpose = suspend` capture and apply the same transaction a
    /// SuspendInstance capture does, with the report's reason (`pressure`
    /// or `park`, [`local_suspension_reason`]) as `suspend_reason`. A
    /// SuspendInstance capture still outstanding for it can never finish (the
    /// node already stopped the allocation), so its row is discarded and the
    /// local one wins.
    ///
    /// When the instance has moved on (stopped, destroyed, or rescheduled
    /// while the node was out of contact), nothing will ever resume from the
    /// checkpoint: record it as `deleting` so the node reclaims the bytes.
    pub(crate) fn apply_local_suspension_tx(
        &self,
        tx: &Connection,
        node_id: &str,
        report: &SnapshotReportRecord,
        post: &mut ReportPostCommit,
    ) -> R<SnapshotReportOutcome> {
        let manifest =
            crate::structs::checkpoint::SnapshotManifest::from_json(&report.manifest_json)
                .context("local suspension report carries no readable manifest")?;
        let bound: Option<(String, Option<String>)> = tx
            .query_row(
                "SELECT instance.instance_id, instance.pending_op FROM instance \
                 INNER JOIN allocation ON allocation.id = instance.current_alloc_id \
                 WHERE instance.current_alloc_id = ?1 AND instance.state = 'active' \
                 AND instance.mode = 'keyed' AND allocation.node_id = ?2;",
                params![report.allocation_id, node_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let now = chrono::Utc::now().timestamp();
        let insert = |state: &str| -> R<()> {
            tx.execute(
                "INSERT INTO snapshot (snapshot_id, job_id, task_name, origin_instance_id, \
                 kind, purpose, state, node_id, allocation_id, manifest_json, total_bytes, \
                 duration_ms, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 'suspend', ?6, ?7, ?8, ?9, ?10, ?11, ?12);",
                params![
                    report.snapshot_id,
                    manifest.job_id,
                    manifest.task,
                    manifest.origin_instance,
                    manifest.kind.as_str(),
                    state,
                    node_id,
                    report.allocation_id,
                    report.manifest_json,
                    report.total_bytes as i64,
                    report.duration_ms as i64,
                    now
                ],
            )?;
            Ok(())
        };
        let Some((instance_id, pending)) = bound else {
            insert("local")?;
            let snapshot =
                Self::get_snapshot_on(tx, &report.snapshot_id)?.context("snapshot row vanished")?;
            Self::discard_snapshot_tx(tx, snapshot)?;
            return Ok(SnapshotReportOutcome::LocalSuspensionDiscarded {
                snapshot_id: report.snapshot_id.clone(),
                allocation_id: report.allocation_id,
                reason: "the instance is no longer running on this allocation".to_string(),
            });
        };
        match pending.as_deref() {
            Some(PENDING_SUSPEND) => {
                let outstanding: Vec<String> = {
                    let mut stmt = tx.prepare(
                        "SELECT snapshot_id FROM snapshot WHERE origin_instance_id = ?1 \
                         AND purpose = 'suspend' AND state = 'capturing';",
                    )?;
                    let ids =
                        Self::collect_rows(stmt.query_map([&instance_id], |row| row.get(0))?)?;
                    ids
                };
                for snapshot_id in outstanding {
                    Self::remove_snapshot_row_tx(tx, &snapshot_id)?;
                }
            }
            Some(_) => {
                // A resume is still settling on this allocation, and the
                // node's own stop overtook its report. The allocation is
                // gone and its state is in the new checkpoint, so the old
                // one is spent: discard it and apply the suspend.
                Self::clear_suspend_tx(tx, &instance_id)?;
                tx.execute(
                    "UPDATE instance SET restore_from = NULL WHERE instance_id = ?1;",
                    [&instance_id],
                )?;
            }
            None => {}
        }
        insert("capturing")?;
        tx.execute(
            "UPDATE instance SET pending_op = ?2, suspend_reason = ?3, lifecycle_error = NULL \
             WHERE instance_id = ?1;",
            params![
                instance_id,
                PENDING_SUSPEND,
                local_suspension_reason(&report.local_suspension_reason)
            ],
        )?;
        let snapshot =
            Self::get_snapshot_on(tx, &report.snapshot_id)?.context("snapshot row vanished")?;
        self.apply_suspend_capture_tx(tx, snapshot, report, post)
    }

    /// A worker's report on a restore that settles a resume, inside
    /// `apply_snapshot_report`'s transaction. `None` when the restore is not a
    /// resume (a RestoreInstance rollback), which the caller handles as
    /// before.
    pub(crate) fn apply_resume_restore_tx(
        &self,
        tx: &Connection,
        instance_id: &str,
        job_id: &str,
        report: &SnapshotReportRecord,
        node_id: &str,
        post: &mut ReportPostCommit,
    ) -> R<Option<SnapshotReportOutcome>> {
        let state = Self::instance_suspend_state_on(tx, instance_id)?;
        if state.pending_op.as_deref() != Some(PENDING_RESUME)
            || state.checkpoint_id.as_deref() != Some(report.snapshot_id.as_str())
        {
            return Ok(None);
        }
        let snapshot = Self::get_snapshot_on(tx, &report.snapshot_id)?;
        let resumed = SnapshotReportOutcome::Resumed {
            snapshot_id: report.snapshot_id.clone(),
            kind: snapshot
                .as_ref()
                .map(|snapshot| snapshot.kind.as_str().to_string())
                .unwrap_or_default(),
            job_id: job_id.to_string(),
            instance_id: instance_id.to_string(),
            node_id: node_id.to_string(),
            allocation_id: report.allocation_id,
            restore_ms: report.duration_ms,
        };
        if report.ok {
            tx.execute(
                "UPDATE instance SET restore_from = NULL, checkpoint_id = NULL, \
                 pending_op = NULL, resume_requested_at = NULL, lifecycle_error = NULL, \
                 cold_boot = NULL, last_restore_ms = ?2, restored_from_snapshot_id = ?3 \
                 WHERE instance_id = ?1;",
                params![instance_id, report.duration_ms as i64, report.snapshot_id],
            )?;
            // A suspend checkpoint is owned by its instance and is spent once
            // the resume succeeds.
            if let Some(snapshot) = snapshot {
                Self::discard_snapshot_tx(tx, snapshot)?;
            }
            return Ok(Some(resumed));
        }

        let policy =
            Self::allocation_suspend_settings_on(tx, report.allocation_id).on_restore_failure;
        let error = report.error.clone();
        match policy {
            OnRestoreFailure::Cold => {
                // The worker failed the allocation with the same reason; with
                // `restore_from` cleared, its replacement boots fresh.
                tx.execute(
                    "UPDATE instance SET restore_from = NULL, checkpoint_id = NULL, \
                     pending_op = NULL, resume_requested_at = NULL, lifecycle_error = ?2, \
                     cold_boot = ?3 WHERE instance_id = ?1;",
                    params![
                        instance_id,
                        format!(
                            "restore from {} failed, booted cold: {error}",
                            report.snapshot_id
                        ),
                        InstanceColdBoot::record_now(&report.snapshot_id, &error)
                    ],
                )?;
                if let Some(snapshot) = snapshot {
                    Self::discard_snapshot_tx(tx, snapshot)?;
                }
            }
            OnRestoreFailure::Fail => {
                // Back to suspended, checkpoint kept, so the platform decides:
                // retry `start`, or `start --cold`.
                self.remove_for_suspend_tx(tx, instance_id, post)?;
                let now = Self::now_rfc3339()?;
                tx.execute(
                    "UPDATE instance SET state = 'suspended', current_alloc_id = NULL, \
                     restore_from = NULL, pending_op = NULL, resume_requested_at = NULL, \
                     lifecycle_error = ?2, updated_at = ?3 WHERE instance_id = ?1;",
                    params![
                        instance_id,
                        format!(
                            "restore from {} failed, still suspended (onRestoreFailure = \
                             \"fail\"): {error}",
                            report.snapshot_id
                        ),
                        now
                    ],
                )?;
            }
        }
        Ok(Some(SnapshotReportOutcome::ResumeFailed {
            snapshot_id: report.snapshot_id.clone(),
            job_id: job_id.to_string(),
            instance_id: instance_id.to_string(),
            node_id: node_id.to_string(),
            allocation_id: report.allocation_id,
            error,
            cold: policy == OnRestoreFailure::Cold,
        }))
    }

    /// Resume a suspended keyed instance (StartInstance on a `suspended`
    /// instance). `cold` discards the checkpoint and boots fresh. A held
    /// reservation (`suspend.reserve`) is released in the same transaction,
    /// so the restore lands on the capacity it held.
    #[instrument(err, skip_all, fields(instance_id))]
    pub fn resume_suspended_instance(&self, instance_id: &str, cold: bool) -> R<ResumeOutcome> {
        self.resume_archived_instance(instance_id, cold, None)
    }

    /// [`Store::resume_suspended_instance`] for an instance that may be
    /// archived (`suspend.archive`): `archived` is what the caller read back
    /// from its archive manifest. A manifest that was read puts the snapshot
    /// row back in the same transaction that starts the resume, so the
    /// resume costs no extra raft entry. One that could not be read is an
    /// unusable checkpoint: `onRestoreFailure` decides between a cold boot
    /// and staying suspended (and archived, so a later start can retry).
    #[instrument(err, skip_all, fields(instance_id))]
    pub fn resume_archived_instance(
        &self,
        instance_id: &str,
        cold: bool,
        archived: Option<ArchivedCheckpoint>,
    ) -> R<ResumeOutcome> {
        let instance_id = instance_id.to_owned();
        let (outcome, allocations) =
            self.replicated(Lane::Scheduler, "resume_suspended_instance", move |s| {
                let mut conn = s.write_connection()?;
                let tx = Self::write_tx(&mut conn)?;
                let resumed =
                    s.resume_suspended_instance_tx(&tx, &instance_id, cold, archived.as_ref())?;
                tx.commit()?;
                Ok(resumed)
            })?;
        self.cache_remove_allocations(&allocations)?;
        Ok(outcome)
    }

    /// Resume an instance archived with `archive = "manifestOnly"`, which
    /// has no row: put the instance row back from the manifest's envelope
    /// (`suspended`, unnamed, archived at `manifest_url`) and resume it, in
    /// one transaction. Refuses a manifest whose job is gone or was
    /// re-created since it was written (NotFound). When the row already
    /// exists, a concurrent start got there first, and this resumes it the
    /// usual way.
    #[instrument(err, skip_all, fields(instance_id = %manifest.envelope.instance_id))]
    pub fn resume_manifest_only_instance(
        &self,
        manifest: crate::structs::archive::ArchiveManifest,
        manifest_url: &str,
        cold: bool,
    ) -> R<ResumeOutcome> {
        let manifest_url = manifest_url.to_owned();
        let (outcome, allocations) =
            self.replicated(Lane::Scheduler, "resume_manifest_only_instance", move |s| {
                let mut conn = s.write_connection()?;
                let tx = Self::write_tx(&mut conn)?;
                let envelope = &manifest.envelope;
                // Read on the transaction, not a pooled read connection,
                // which would deadlock on a store without a read pool.
                let job_created_at = tx
                    .query_row(&GET_JOB, [envelope.job_id.as_str()], |row| {
                        s.job_from_row(row)
                    })
                    .optional()?
                    .map(|job| job.created_at);
                if job_created_at != Some(envelope.job_created_at) {
                    bail!(StorageError::not_found("instance", &envelope.instance_id));
                }
                if !Self::instance_id_exists_tx(&tx, &envelope.instance_id)? {
                    let now = Self::now_rfc3339()?;
                    tx.execute(
                        "INSERT INTO instance (instance_id, job_id, group_name, task_name, mode, \
                         state, created_at, updated_at, named, checkpoint_id, suspend_reason, \
                         suspended_at, last_suspend_ms, forked_from, identity_epoch, \
                         manifest_url) \
                         VALUES (?1, ?2, ?3, ?4, 'keyed', 'suspended', ?5, ?6, 0, ?7, ?8, ?9, \
                         ?10, ?11, ?12, ?13);",
                        params![
                            envelope.instance_id,
                            envelope.job_id,
                            envelope.group,
                            envelope.task,
                            envelope.created_at,
                            now,
                            envelope.snapshot.snapshot_id,
                            envelope.suspend_reason,
                            envelope.suspended_at,
                            envelope.last_suspend_ms,
                            envelope.forked_from,
                            envelope.identity_epoch as i64,
                            manifest_url,
                        ],
                    )?;
                }
                let archived = ArchivedCheckpoint::Read(Box::new(manifest.snapshot_record()));
                let resumed = s.resume_suspended_instance_tx(
                    &tx,
                    &envelope.instance_id,
                    cold,
                    Some(&archived),
                )?;
                tx.commit()?;
                Ok(resumed)
            })?;
        self.cache_remove_allocations(&allocations)?;
        Ok(outcome)
    }

    fn resume_suspended_instance_tx(
        &self,
        tx: &Connection,
        instance_id: &str,
        cold: bool,
        archived: Option<&ArchivedCheckpoint>,
    ) -> R<(ResumeOutcome, Vec<Allocation>)> {
        let row: Option<(String, String, String)> = tx
            .query_row(
                "SELECT state, job_id, task_name FROM instance WHERE instance_id = ?1 \
                 AND mode = 'keyed';",
                [instance_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((state, job_id, task_name)) = row else {
            bail!(StorageError::not_found("instance", instance_id));
        };
        if state != "suspended" {
            bail!(StorageError::Conflict {
                message: format!(
                    "instance {instance_id} is {state}, not suspended; retry StartInstance"
                ),
            });
        }
        let suspend = Self::instance_suspend_state_on(tx, instance_id)?;
        let settings = Self::latest_suspend_settings_on(tx, &job_id, &task_name);
        // An archived instance's snapshot row lives in its manifest: put it
        // back from what the caller read, or say why it cannot be.
        let archive_unusable = match (suspend.manifest_url.as_deref(), archived) {
            (None, _) => None,
            (Some(_), Some(ArchivedCheckpoint::Read(snapshot)))
                if suspend.checkpoint_id.as_deref() == Some(snapshot.snapshot_id.as_str()) =>
            {
                Self::insert_snapshot_record_tx(tx, snapshot)?;
                None
            }
            (Some(url), Some(ArchivedCheckpoint::Read(snapshot))) => Some(format!(
                "archive manifest {url} holds checkpoint {}, not {}",
                snapshot.snapshot_id,
                suspend.checkpoint_id.as_deref().unwrap_or("-")
            )),
            (Some(url), Some(ArchivedCheckpoint::Unreadable(reason))) => Some(format!(
                "checkpoint {} is archived and its manifest {url} could not be read ({reason})",
                suspend.checkpoint_id.as_deref().unwrap_or("-")
            )),
            (Some(_), None) if cold => None,
            (Some(url), None) => Some(format!(
                "checkpoint {} is archived at {url}, and the manifest was not read",
                suspend.checkpoint_id.as_deref().unwrap_or("-")
            )),
        };
        let snapshot = match suspend.checkpoint_id.as_deref() {
            Some(id) => Self::get_snapshot_on(tx, id)?,
            None => None,
        };
        let unusable = archive_unusable.or_else(|| match snapshot.as_ref() {
            None => Some(format!(
                "checkpoint {} no longer exists",
                suspend.checkpoint_id.as_deref().unwrap_or("-")
            )),
            Some(snapshot) if snapshot.state.is_restorable() => None,
            Some(snapshot) => Some(format!(
                "checkpoint {} is {}{}",
                snapshot.snapshot_id,
                snapshot.state,
                snapshot
                    .error
                    .as_deref()
                    .map(|error| format!(" ({error})"))
                    .unwrap_or_default()
            )),
        });
        if !cold && settings.on_restore_failure == OnRestoreFailure::Fail {
            if let Some(reason) = unusable.as_ref() {
                let reason =
                    format!("{reason}; the instance stays suspended (onRestoreFailure = \"fail\")");
                tx.execute(
                    "UPDATE instance SET lifecycle_error = ?2 WHERE instance_id = ?1;",
                    params![instance_id, reason],
                )?;
                return Ok((ResumeOutcome::RestoreUnavailable { reason }, Vec::new()));
            }
        }

        // A held reservation goes now: the restore is placed on its node as
        // an ordinary allocation.
        let held = self.instance_allocations_tx(tx, instance_id)?;
        if !held.is_empty() {
            tx.execute(
                "DELETE FROM allocation WHERE instance_id = ?;",
                [instance_id],
            )?;
            self.bump_epochs_for_allocations_tx(tx, &held)?;
        }
        let now = Self::now_rfc3339()?;
        let outcome = if cold || unusable.is_some() {
            let discarded = suspend.checkpoint_id.clone();
            if let Some(snapshot) = snapshot {
                Self::discard_snapshot_tx(tx, snapshot)?;
            }
            let reason = (!cold).then(|| {
                format!(
                    "{}; booted cold on the retained storage",
                    unusable.clone().unwrap_or_default()
                )
            });
            // An explicit --cold records no cold boot: it was asked for.
            let cold_boot = unusable.as_ref().filter(|_| !cold).map(|unusable| {
                InstanceColdBoot::record_now(discarded.as_deref().unwrap_or_default(), unusable)
            });
            tx.execute(
                "UPDATE instance SET state = 'pending', current_alloc_id = NULL, \
                 checkpoint_id = NULL, pending_op = NULL, restore_from = NULL, \
                 resume_requested_at = NULL, lifecycle_error = ?2, updated_at = ?3, \
                 cold_boot = ?4, manifest_url = NULL WHERE instance_id = ?1;",
                params![instance_id, reason, now, cold_boot],
            )?;
            ResumeOutcome::Cold { reason, discarded }
        } else {
            let snapshot = snapshot.expect("usable checkpoint");
            tx.execute(
                "UPDATE instance SET state = 'pending', current_alloc_id = NULL, \
                 restore_from = ?2, pending_op = ?3, resume_requested_at = ?4, \
                 lifecycle_error = NULL, cold_boot = NULL, updated_at = ?5, \
                 manifest_url = NULL WHERE instance_id = ?1;",
                params![
                    instance_id,
                    snapshot.snapshot_id,
                    PENDING_RESUME,
                    OffsetDateTime::now_utc().unix_timestamp(),
                    now
                ],
            )?;
            // A published checkpoint is not pinned to a node: placement
            // picks any compatible one, preferring nodes holding its parts.
            let node_id = if snapshot.state == SnapshotState::Published {
                None
            } else {
                snapshot.node_id
            };
            ResumeOutcome::Resuming {
                snapshot_id: snapshot.snapshot_id,
                node_id,
            }
        };
        info!(instance_id, outcome = ?outcome, "Resuming suspended instance");
        Ok((outcome, held))
    }

    /// Put an archived checkpoint's snapshot row back, inside the caller's
    /// transaction. A row already there (a resume retried after its first
    /// attempt put it back) is kept.
    fn insert_snapshot_record_tx(tx: &Connection, snapshot: &SnapshotRecord) -> R<()> {
        tx.execute(
            "INSERT OR IGNORE INTO snapshot (snapshot_id, job_id, task_name, \
             origin_instance_id, name, kind, purpose, state, node_id, allocation_id, \
             manifest_json, total_bytes, duration_ms, error, created_at, expires_at, pause_ms, \
             durability, published_at) \
             VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, NULL, ?13, ?14, \
             ?15, ?16, ?17);",
            params![
                snapshot.snapshot_id,
                snapshot.job_id,
                snapshot.task_name,
                snapshot.instance_id,
                snapshot.kind.as_str(),
                snapshot.purpose.as_str(),
                snapshot.state.as_str(),
                snapshot.node_id,
                snapshot.allocation_id,
                snapshot.manifest_json,
                snapshot.total_bytes as i64,
                snapshot.duration_ms as i64,
                snapshot.created_at,
                snapshot.expires_at,
                snapshot.pause_ms as i64,
                snapshot.durability.as_str(),
                snapshot.published_at,
            ],
        )?;
        Ok(())
    }

    /// Archive a suspended instance whose checkpoint `snapshot` was just
    /// published with an archive manifest at `manifest_url`, inside the
    /// transaction that marks it published (docs/suspend-and-resume.md,
    /// "Archived instances"). The snapshot row goes; the instance row keeps
    /// its id and records the URL (`archive = "slim"`), or goes too for an
    /// unnamed instance that owns no volume (`"manifestOnly"`). `None`, and
    /// nothing changed, when the instance moved on since the publish was
    /// asked for (resumed, stopped, destroyed), still holds capacity, or its
    /// task no longer archives.
    pub(crate) fn archive_suspended_instance_tx(
        &self,
        tx: &Connection,
        snapshot: &SnapshotRecord,
        manifest_url: &str,
    ) -> R<Option<ArchivedInstance>> {
        let Some(instance_id) = snapshot.instance_id.as_deref() else {
            return Ok(None);
        };
        type Row = (
            String,
            String,
            String,
            bool,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let row: Option<Row> = tx
            .query_row(
                "SELECT job_id, task_name, state, named, storage_handle, checkpoint_id, \
                 pending_op FROM instance WHERE instance_id = ?1 AND mode = 'keyed';",
                [instance_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get::<_, i64>(3)? != 0,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((job_id, task_name, state, named, storage_handle, checkpoint_id, pending_op)) =
            row
        else {
            return Ok(None);
        };
        if state != "suspended"
            || checkpoint_id.as_deref() != Some(snapshot.snapshot_id.as_str())
            || pending_op.is_some()
        {
            return Ok(None);
        }
        let Some(mode) = Self::latest_suspend_settings_on(tx, &job_id, &task_name).archive else {
            return Ok(None);
        };
        if !self.instance_allocations_tx(tx, instance_id)?.is_empty() {
            return Ok(None);
        }
        Self::remove_snapshot_row_tx(tx, &snapshot.snapshot_id)?;
        let row_deleted = mode == ArchiveMode::ManifestOnly && !named && storage_handle.is_none();
        if row_deleted {
            tx.execute(
                "DELETE FROM instance WHERE instance_id = ?1;",
                [instance_id],
            )?;
        } else {
            tx.execute(
                "UPDATE instance SET manifest_url = ?2, updated_at = ?3 WHERE instance_id = ?1;",
                params![instance_id, manifest_url, Self::now_rfc3339()?],
            )?;
        }
        Ok(Some(ArchivedInstance {
            instance_id: instance_id.to_string(),
            job_id,
            snapshot_id: snapshot.snapshot_id.clone(),
            manifest_url: manifest_url.to_string(),
            row_deleted,
        }))
    }

    /// The resume sweep, run by the manager's maintenance loop: settle every
    /// in-flight resume that can no longer restore. A resume whose
    /// checkpoint was lost with its node, or that has waited longer than
    /// `suspend.resumeTimeout` to be placed on the checkpoint's node, boots
    /// cold under `onRestoreFailure = "cold"`. Under `"fail"`, a lost
    /// checkpoint puts the instance back to `suspended`, and a resume that is
    /// only waiting keeps waiting.
    ///
    /// A local checkpoint whose node reports a VMM or CPU its `compat` block
    /// refuses settles at once, like a lost one: it lives only on that node,
    /// so waiting out `resumeTimeout` cannot help. Cheap when nothing is
    /// resuming: one indexed read, no write.
    #[instrument(err, skip_all)]
    pub fn settle_stalled_resumes(&self, now_unix: i64) -> R<Vec<AbandonedResume>> {
        type Candidate = (
            String,
            String,
            String,
            String,
            Option<i64>,
            String,
            Option<String>,
            String,
        );
        let candidates: Vec<Candidate> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(
                "SELECT instance.instance_id, instance.job_id, instance.task_name, \
                 instance.restore_from, instance.resume_requested_at, snapshot.state, \
                 snapshot.node_id, snapshot.manifest_json \
                 FROM instance INNER JOIN snapshot ON snapshot.snapshot_id = instance.restore_from \
                 WHERE instance.pending_op = 'resume' AND instance.current_alloc_id IS NULL;",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            })?;
            Self::collect_rows(rows)?
        };
        let mut abandoned = Vec::new();
        for (
            instance_id,
            job_id,
            task_name,
            snapshot_id,
            requested_at,
            state,
            node_id,
            manifest_json,
        ) in candidates
        {
            let settings = self.suspend_settings_for(&job_id, &task_name);
            let state_parsed = state.parse::<SnapshotState>().ok();
            let lost = !state_parsed.is_some_and(|state| state.is_restorable());
            let published = state_parsed == Some(SnapshotState::Published);
            let waited = now_unix - requested_at.unwrap_or(now_unix);
            let timed_out = waited >= settings.resume_timeout.as_secs() as i64;
            let cold = settings.on_restore_failure == OnRestoreFailure::Cold;
            // A published checkpoint can still find another node, so only a
            // local one is refused for good by its node's fingerprint.
            let incompatible = (!lost && !published)
                .then(|| {
                    self.local_checkpoint_refusal(&snapshot_id, node_id.as_deref(), &manifest_json)
                })
                .flatten();
            if !(lost || incompatible.is_some() || (timed_out && cold)) {
                continue;
            }
            let reason = if lost {
                format!("checkpoint {snapshot_id} is {state} (its node is gone)")
            } else if let Some(refusal) = incompatible {
                refusal
            } else if published {
                format!(
                    "the resume could not be placed on any node compatible with checkpoint \
                     {snapshot_id} within resumeTimeout ({}s)",
                    settings.resume_timeout.as_secs()
                )
            } else {
                format!(
                    "the resume could not be placed on checkpoint {snapshot_id}'s node within \
                     resumeTimeout ({}s)",
                    settings.resume_timeout.as_secs()
                )
            };
            let applied = {
                let instance_id = instance_id.clone();
                let snapshot_id = snapshot_id.clone();
                let reason = reason.clone();
                self.replicated(Lane::Scheduler, "abandon_resume", move |s| {
                    s.abandon_resume_apply(&instance_id, &snapshot_id, cold, &reason)
                })?
            };
            if applied {
                abandoned.push(AbandonedResume {
                    instance_id,
                    job_id,
                    snapshot_id,
                    cold,
                    reason,
                });
            }
        }
        Ok(abandoned)
    }

    /// Why a local checkpoint can never resume on its node: the node
    /// reports a VMM version or CPU that differs from what the checkpoint's
    /// `compat` block needs. The scheduler refuses the same placement
    /// (`check_restore_compat`), so without this the resume would retry
    /// every eval until `resumeTimeout`. Only a reported, different value
    /// counts: a missing attribute (a fingerprint still arriving), a missing
    /// node, or an unreadable manifest leave the resume waiting as before.
    fn local_checkpoint_refusal(
        &self,
        snapshot_id: &str,
        node_id: Option<&str>,
        manifest_json: &str,
    ) -> Option<String> {
        let node_id = node_id?;
        let manifest =
            crate::structs::checkpoint::SnapshotManifest::from_json(manifest_json).ok()?;
        if manifest.compat.is_empty() {
            return None;
        }
        let node = self.get_node(node_id).ok()?;
        let attrs = node.fingerprint_attrs.as_object()?;
        let mismatch = crate::structs::checkpoint::compat_matches(&manifest, attrs).err()?;
        mismatch.found.as_ref()?;
        Some(format!(
            "checkpoint {snapshot_id}'s node {node_id} cannot resume it: {mismatch}"
        ))
    }

    fn abandon_resume_apply(
        &self,
        instance_id: &str,
        snapshot_id: &str,
        cold: bool,
        reason: &str,
    ) -> R<bool> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let now = Self::now_rfc3339()?;
        let updated = if cold {
            tx.execute(
                "UPDATE instance SET restore_from = NULL, checkpoint_id = NULL, \
                 pending_op = NULL, resume_requested_at = NULL, lifecycle_error = ?3, \
                 updated_at = ?4, cold_boot = ?5 WHERE instance_id = ?1 AND restore_from = ?2 \
                 AND pending_op = 'resume' AND current_alloc_id IS NULL;",
                params![
                    instance_id,
                    snapshot_id,
                    format!("{reason}; booted cold on the retained storage"),
                    now,
                    InstanceColdBoot::record_now(snapshot_id, reason)
                ],
            )?
        } else {
            tx.execute(
                "UPDATE instance SET state = 'suspended', restore_from = NULL, \
                 pending_op = NULL, resume_requested_at = NULL, lifecycle_error = ?3, \
                 updated_at = ?4 WHERE instance_id = ?1 AND restore_from = ?2 \
                 AND pending_op = 'resume' AND current_alloc_id IS NULL;",
                params![
                    instance_id,
                    snapshot_id,
                    format!("{reason}; the instance stays suspended (onRestoreFailure = \"fail\")"),
                    now
                ],
            )?
        };
        if updated > 0 && cold {
            if let Some(snapshot) = Self::get_snapshot_on(&tx, snapshot_id)? {
                Self::discard_snapshot_tx(&tx, snapshot)?;
            }
        }
        tx.commit()?;
        Ok(updated > 0)
    }

    /// Resumes waiting to be placed on their checkpoint's node: `pending_op
    /// = resume` with no allocation yet, restoring from a `local`
    /// checkpoint (a published one is not pinned, so it is never waiting on
    /// one node). Oldest request first. One indexed read, and empty unless
    /// something is resuming: the make-room sweep calls it every pass.
    pub fn waiting_pinned_resumes(&self) -> R<Vec<WaitingResume>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT instance.instance_id, instance.job_id, instance.group_name, \
             instance.task_name, snapshot.node_id, COALESCE(instance.resume_requested_at, 0) \
             FROM instance INNER JOIN snapshot ON snapshot.snapshot_id = instance.restore_from \
             WHERE instance.pending_op = 'resume' AND instance.current_alloc_id IS NULL \
             AND snapshot.state = 'local' AND snapshot.node_id IS NOT NULL \
             ORDER BY 6, instance.instance_id;",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(WaitingResume {
                instance_id: row.get(0)?,
                job_id: row.get(1)?,
                group: row.get(2)?,
                task_name: row.get(3)?,
                node_id: row.get(4)?,
                requested_at: row.get(5)?,
            })
        })?;
        Self::collect_rows(rows)
    }

    /// Suspended instances whose checkpoint is local to `node_id`, and
    /// those checkpoints' bytes, for `GetNodeUsage`'s suspended line
    /// (docs/suspend-and-resume.md, "Capacity for resumes"). A published
    /// checkpoint does not count: its node holds at most an unpinned cache
    /// copy, which the content store may already have evicted.
    pub fn suspended_on_node(&self, node_id: &str) -> R<(u32, u64)> {
        let conn = self.read_connection()?;
        let (count, bytes): (i64, i64) = conn.query_row(
            "SELECT COUNT(*), COALESCE(SUM(snapshot.total_bytes), 0) FROM instance \
             INNER JOIN snapshot ON snapshot.snapshot_id = instance.checkpoint_id \
             WHERE instance.state = 'suspended' AND snapshot.node_id = ?1 \
             AND snapshot.state != 'published';",
            [node_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok((count.max(0) as u32, bytes.max(0) as u64))
    }

    /// Count the keyed instances of a (job, group, task) the scheduler must
    /// keep an allocation for: the live ones, plus suspended ones holding
    /// their reservation (`suspend.reserve`), whose allocation row stays so
    /// the capacity stays taken. Forks still waiting for their checkpoint
    /// (`pending_op = 'fork'`) are not placed yet, so they do not count.
    pub fn count_keyed_allocations_desired(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
    ) -> R<u32> {
        let conn = self.read_connection()?;
        let count: i64 = conn
            .prepare_cached(
                "SELECT COUNT(*) FROM instance \
                 WHERE job_id = ?1 AND group_name = ?2 AND task_name = ?3 AND mode = 'keyed' \
                 AND (state NOT IN ('terminated', 'stopped', 'suspended') \
                 OR (state = 'suspended' AND current_alloc_id IS NOT NULL)) \
                 AND COALESCE(pending_op, '') != 'fork';",
            )?
            .query_row(params![job_id, group, task_name], |row| row.get(0))?;
        Ok(count.max(0) as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A park says so; anything else a node reports for a local suspension,
    /// an empty reason included, is the pressure step it always was.
    #[test]
    fn a_local_suspension_records_park_or_pressure_test() {
        assert_eq!(local_suspension_reason("park"), SUSPEND_REASON_PARK);
        for other in ["", "pressure", "idle", "manual", "PARK"] {
            assert_eq!(
                local_suspension_reason(other),
                SUSPEND_REASON_PRESSURE,
                "{other:?}"
            );
        }
    }
}
