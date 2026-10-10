//! Fork, create-from-snapshot, and snapshot names: the store half of
//! ForkInstance, `CreateInstance(s).from_snapshot`, and SetSnapshotAlias
//! (suspend plan §6.9, Phase 6; docs/suspend-and-resume.md, "Fork").
//!
//! A fork is a `purpose = fork` snapshot of the origin plus N new keyed
//! instances whose `restore_from` names it. Until the capture lands the
//! forks carry `pending_op = 'fork'`, which keeps them out of the keyed
//! desired count and the binder, so nothing is placed against a checkpoint
//! that does not exist yet. The capture report releases them
//! ([`Store::apply_fork_capture_tx`]), and from there they are ordinary
//! restoring instances, pinned to the checkpoint's node by the existing
//! restore pins. A spread fork's checkpoint is published first
//! (`durability = store`), its forks are released when the publish lands
//! ([`Store::release_forks_tx`]), and the scheduler places each on a node no
//! sibling runs on. Restores into a new instance settle through
//! [`Store::apply_new_instance_restore_tx`], which follows the task's
//! `suspend.onRestoreFailure`. A fork checkpoint is spent once no instance
//! restores from it any more. Every write is a scheduler-lane op, beside the
//! `instance` and `snapshot` rows it touches.

use super::*;
use crate::storage::store::{NewSnapshot, SnapshotReportOutcome, SnapshotReportRecord};
use crate::structs::fork::{fork_instance_id, PENDING_FORK};
use crate::structs::snapshot::{
    SnapshotDurability, SnapshotPurpose, SnapshotRecord, SnapshotState,
};
use crate::structs::suspend::OnRestoreFailure;

/// What [`Store::begin_fork`] created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkCreated {
    /// The `capturing` fork checkpoint.
    pub snapshot: SnapshotRecord,
    /// The new instances, in creation order.
    pub instances: Vec<String>,
}

/// Outcome of [`Store::set_snapshot_alias`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasOutcome {
    /// The snapshot the name pointed at before, if it existed.
    pub previous: Option<String>,
    /// False when the name already pointed at the snapshot.
    pub changed: bool,
}

/// The origin columns [`Store::begin_fork`] checks: mode, state, current
/// allocation, pending op, group, task.
type OriginRow = (String, String, Option<i64>, Option<String>, String, String);

impl Store {
    /// Start a fork (the ForkInstance RPC's storage half): insert the
    /// `capturing` fork checkpoint and `count` new keyed instances waiting on
    /// it, in one transaction. Refuses unless the origin is a keyed `active`
    /// instance still bound to `new.allocation_id` with no other operation in
    /// flight.
    ///
    /// Fork ids are `<origin>-f<n>` with the lowest `n` whose id has never
    /// existed in any job (ids are never recycled), or a minted id when the
    /// origin id is too long to suffix. With `derived_ids_only` (a task whose
    /// `stateful.idSource = "caller"`, so every id must stay in the caller's
    /// namespace) a fork that would need a minted id is refused instead.
    #[instrument(err, skip_all, fields(snapshot_id = %new.snapshot_id, count))]
    pub fn begin_fork(
        &self,
        new: NewSnapshot,
        count: u32,
        derived_ids_only: bool,
    ) -> R<ForkCreated> {
        self.replicated(Lane::Scheduler, "begin_fork", move |s| {
            s.begin_fork_apply(&new, count, derived_ids_only)
        })
    }

    fn begin_fork_apply(
        &self,
        new: &NewSnapshot,
        count: u32,
        derived_ids_only: bool,
    ) -> R<ForkCreated> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let row: Option<OriginRow> = tx
            .query_row(
                "SELECT mode, state, current_alloc_id, pending_op, group_name, task_name \
                 FROM instance WHERE instance_id = ?1;",
                [&new.instance_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((mode, state, current, pending, group, task_name)) = row else {
            bail!(StorageError::not_found("instance", &new.instance_id));
        };
        if mode != "keyed" || state != "active" || current != Some(new.allocation_id) {
            bail!(StorageError::ForeignKey {
                message: format!(
                    "instance {} changed while the fork was being prepared (it is {state}); retry",
                    new.instance_id
                ),
            });
        }
        if let Some(pending) = pending {
            bail!(StorageError::ForeignKey {
                message: format!(
                    "instance {} has a {pending} in flight; fork it once that finishes",
                    new.instance_id
                ),
            });
        }
        let snapshot = Self::create_snapshot_tx(&tx, new)?;

        let mut instances = Vec::with_capacity(count as usize);
        let mut n = 1u32;
        while instances.len() < count as usize {
            let id = match fork_instance_id(&new.instance_id, n) {
                Some(candidate) => {
                    n += 1;
                    let taken = tx
                        .query_row(
                            "SELECT 1 FROM instance WHERE instance_id = ?1;",
                            [&candidate],
                            |_| Ok(()),
                        )
                        .optional()?
                        .is_some();
                    if taken {
                        continue;
                    }
                    self.insert_keyed_instance_tx(
                        &tx,
                        &new.job_id,
                        &group,
                        &task_name,
                        Some(&candidate),
                    )?
                }
                None if derived_ids_only => bail!(StorageError::InvalidInput {
                    message: format!(
                        "instance {} is too long to fork: a fork id is `<origin>-f<n>`, at most \
                         128 characters, and its task sets stateful.idSource = \"caller\", so \
                         Legion will not mint an id outside the caller's namespace\nHint: use \
                         shorter instance ids, or create the new instances from a snapshot \
                         (`legion instance create --from-snapshot`) with ids you choose",
                        new.instance_id
                    ),
                }),
                None => {
                    self.insert_keyed_instance_tx(&tx, &new.job_id, &group, &task_name, None)?
                }
            };
            tx.execute(
                "UPDATE instance SET restore_from = ?2, pending_op = ?3, forked_from = ?4 \
                 WHERE instance_id = ?1;",
                params![id, snapshot.snapshot_id, PENDING_FORK, new.instance_id],
            )?;
            instances.push(id);
        }
        tx.commit()?;
        info!(
            origin = %new.instance_id,
            snapshot_id = %snapshot.snapshot_id,
            forks = ?instances,
            "Fork requested"
        );
        Ok(ForkCreated {
            snapshot,
            instances,
        })
    }

    /// The forks waiting on, or restoring from, `snapshot_id`, in creation
    /// order. Used to answer an idempotent retry and `fork --wait`.
    pub fn instances_restoring_from(&self, snapshot_id: &str) -> R<Vec<String>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT instance_id FROM instance WHERE restore_from = ?1 \
             ORDER BY created_at ASC, instance_id ASC;",
        )?;
        let rows = stmt.query_map([snapshot_id], |row| row.get(0))?;
        Self::collect_rows(rows)
    }

    /// A worker's report on a fork capture, inside `apply_snapshot_report`'s
    /// transaction. Success makes the checkpoint `local` and releases the
    /// waiting forks for placement, except for a spread fork
    /// (`durability = store`), whose forks wait for the publish so they can
    /// go to other nodes ([`Self::release_forks_tx`] runs when it lands).
    /// Failure leaves the origin running (the worker resumed it) and
    /// terminates the forks with the reason, since there is nothing for them
    /// to start from.
    pub(crate) fn apply_fork_capture_tx(
        &self,
        tx: &Connection,
        snapshot: SnapshotRecord,
        report: &SnapshotReportRecord,
    ) -> R<SnapshotReportOutcome> {
        let origin = snapshot.instance_id.clone().unwrap_or_default();

        if !report.ok {
            tx.execute(
                "UPDATE snapshot SET state = 'failed', error = ?2, duration_ms = ?3 \
                 WHERE snapshot_id = ?1;",
                params![
                    snapshot.snapshot_id,
                    report.error,
                    report.duration_ms as i64
                ],
            )?;
            let forks = self.fail_waiting_forks_tx(tx, &snapshot, &report.error)?;
            let snapshot = Self::get_snapshot_on(tx, &snapshot.snapshot_id)?
                .context("snapshot row vanished")?;
            return Ok(SnapshotReportOutcome::ForkFailed {
                snapshot,
                instances: forks,
            });
        }

        tx.execute(
            "UPDATE snapshot SET state = 'local', manifest_json = ?2, total_bytes = ?3, \
             duration_ms = ?4, pause_ms = ?5, error = NULL WHERE snapshot_id = ?1;",
            params![
                snapshot.snapshot_id,
                report.manifest_json,
                report.total_bytes as i64,
                report.duration_ms as i64,
                report.pause_ms as i64
            ],
        )?;
        let snapshot =
            Self::get_snapshot_on(tx, &snapshot.snapshot_id)?.context("snapshot row vanished")?;
        if snapshot.durability == SnapshotDurability::Store {
            // Spread: captured, now published by the node before the forks
            // are placed anywhere.
            return Ok(SnapshotReportOutcome::Captured {
                snapshot,
                evicted: Vec::new(),
            });
        }
        let forks = self.release_forks_tx(tx, &snapshot)?;
        Ok(SnapshotReportOutcome::Forked {
            snapshot,
            origin,
            instances: forks,
        })
    }

    /// Release the forks waiting on `snapshot` for placement (their
    /// checkpoint is restorable now), and spend the checkpoint when every
    /// fork was stopped or destroyed meanwhile. Returns the released forks.
    pub(crate) fn release_forks_tx(
        &self,
        tx: &Connection,
        snapshot: &SnapshotRecord,
    ) -> R<Vec<String>> {
        let forks = Self::waiting_forks_tx(tx, &snapshot.snapshot_id)?;
        tx.execute(
            "UPDATE instance SET pending_op = NULL WHERE restore_from = ?1 AND pending_op = ?2;",
            params![snapshot.snapshot_id, PENDING_FORK],
        )?;
        if forks.is_empty() {
            // Every fork was destroyed while the node captured: nothing
            // will restore from this checkpoint.
            Self::spend_fork_snapshot_if_unused_tx(tx, &snapshot.snapshot_id)?;
        }
        Ok(forks)
    }

    /// Terminate the forks waiting on `snapshot` with `reason`: their
    /// checkpoint will never exist (a failed capture), or never leave its
    /// node (a spread fork whose publish failed for good). The origin keeps
    /// running. Returns the terminated forks.
    pub(crate) fn fail_waiting_forks_tx(
        &self,
        tx: &Connection,
        snapshot: &SnapshotRecord,
        reason: &str,
    ) -> R<Vec<String>> {
        let forks = Self::waiting_forks_tx(tx, &snapshot.snapshot_id)?;
        let origin = snapshot.instance_id.clone().unwrap_or_default();
        let now = Self::now_rfc3339()?;
        tx.execute(
            "UPDATE instance SET state = 'terminated', restore_from = NULL, \
             pending_op = NULL, lifecycle_error = ?3, updated_at = ?4 \
             WHERE restore_from = ?1 AND pending_op = ?2;",
            params![
                snapshot.snapshot_id,
                PENDING_FORK,
                format!("fork of {origin} failed, so this fork never started: {reason}"),
                now
            ],
        )?;
        if snapshot.state != SnapshotState::Failed {
            // A captured checkpoint nothing will restore from is reclaimed.
            Self::spend_fork_snapshot_if_unused_tx(tx, &snapshot.snapshot_id)?;
        }
        Ok(forks)
    }

    fn waiting_forks_tx(tx: &Connection, snapshot_id: &str) -> R<Vec<String>> {
        let mut stmt = tx.prepare(
            "SELECT instance_id FROM instance WHERE restore_from = ?1 AND pending_op = ?2 \
             ORDER BY created_at ASC, instance_id ASC;",
        )?;
        let forks = Self::collect_rows(
            stmt.query_map(params![snapshot_id, PENDING_FORK], |row| row.get(0))?,
        )?;
        Ok(forks)
    }

    /// A worker's report on a restore into a NEW instance (a fork, or a
    /// `from_snapshot` create), inside `apply_snapshot_report`'s transaction.
    /// `None` when the snapshot was taken from this very instance (a
    /// RestoreInstance rollback), which the caller handles as before.
    ///
    /// A failed restore follows the task's `suspend.onRestoreFailure`:
    /// `cold` lets the replacement boot fresh on the instance's (cloned)
    /// storage, `fail` stops the instance with the reason. A fork checkpoint
    /// is spent once nothing restores from it.
    pub(crate) fn apply_new_instance_restore_tx(
        &self,
        tx: &Connection,
        instance_id: &str,
        job_id: &str,
        report: &SnapshotReportRecord,
        node_id: &str,
    ) -> R<Option<SnapshotReportOutcome>> {
        let Some(snapshot) = Self::get_snapshot_on(tx, &report.snapshot_id)? else {
            return Ok(None);
        };
        if snapshot.instance_id.as_deref() == Some(instance_id) {
            return Ok(None);
        }
        let now = Self::now_rfc3339()?;
        let outcome = if report.ok {
            tx.execute(
                "UPDATE instance SET restore_from = NULL, lifecycle_error = NULL, \
                 last_restore_ms = ?2, restored_from_snapshot_id = ?3 WHERE instance_id = ?1;",
                params![instance_id, report.duration_ms as i64, report.snapshot_id],
            )?;
            SnapshotReportOutcome::Restored {
                snapshot_id: report.snapshot_id.clone(),
                job_id: job_id.to_string(),
                instance_id: Some(instance_id.to_string()),
                allocation_id: report.allocation_id,
                ok: true,
                error: String::new(),
            }
        } else {
            let policy =
                Self::allocation_suspend_settings_on(tx, report.allocation_id).on_restore_failure;
            let cold = policy == OnRestoreFailure::Cold;
            if cold {
                // The worker failed the allocation with the same reason; with
                // `restore_from` cleared, its replacement boots fresh.
                tx.execute(
                    "UPDATE instance SET restore_from = NULL, lifecycle_error = ?2, \
                     updated_at = ?3 WHERE instance_id = ?1;",
                    params![
                        instance_id,
                        format!(
                            "restore from {} failed, booted cold on its own storage: {}",
                            report.snapshot_id, report.error
                        ),
                        now
                    ],
                )?;
            } else {
                let allocations = self.instance_allocations_on(tx, instance_id)?;
                tx.execute(
                    "DELETE FROM allocation WHERE instance_id = ?;",
                    [instance_id],
                )?;
                self.bump_epochs_for_allocations_tx(tx, &allocations)?;
                let _ = self.record_stop_notices_tx(tx, &allocations, StopCause::Stop)?;
                tx.execute(
                    "UPDATE instance SET state = 'stopped', current_alloc_id = NULL, \
                     restore_from = NULL, lifecycle_error = ?2, updated_at = ?3 \
                     WHERE instance_id = ?1;",
                    params![
                        instance_id,
                        format!(
                            "restore from {} failed, stopped (onRestoreFailure = \"fail\"): {}",
                            report.snapshot_id, report.error
                        ),
                        now
                    ],
                )?;
            }
            SnapshotReportOutcome::NewInstanceRestoreFailed {
                snapshot_id: report.snapshot_id.clone(),
                job_id: job_id.to_string(),
                instance_id: instance_id.to_string(),
                node_id: node_id.to_string(),
                allocation_id: report.allocation_id,
                error: report.error.clone(),
                cold,
            }
        };
        Self::spend_fork_snapshot_if_unused_tx(tx, &report.snapshot_id)?;
        Ok(Some(outcome))
    }

    fn instance_allocations_on(&self, tx: &Connection, instance_id: &str) -> R<Vec<Allocation>> {
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

    /// Discard a `local` or `published` fork checkpoint that no instance restores from any
    /// more. True when it was discarded. Manual and suspend snapshots are
    /// never touched.
    pub(crate) fn spend_fork_snapshot_if_unused_tx(tx: &Connection, snapshot_id: &str) -> R<bool> {
        let Some(snapshot) = Self::get_snapshot_on(tx, snapshot_id)? else {
            return Ok(false);
        };
        // A published fork checkpoint (a spread fork) is spent the same way.
        // Its node's copy is reclaimed; the parts in the content store stay,
        // as for every published snapshot.
        if snapshot.purpose != SnapshotPurpose::Fork
            || !matches!(
                snapshot.state,
                SnapshotState::Local | SnapshotState::Published
            )
        {
            return Ok(false);
        }
        let in_use = tx
            .query_row(
                "SELECT 1 FROM instance WHERE restore_from = ?1 LIMIT 1;",
                [snapshot_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if in_use {
            return Ok(false);
        }
        Self::discard_snapshot_tx(tx, snapshot)?;
        Ok(true)
    }

    /// The maintenance sweep's half of spending fork checkpoints: discard
    /// every `local` or `published` fork checkpoint nothing restores from (its forks were
    /// stopped or destroyed before they restored). Cheap when there is none:
    /// one indexed read, no write. Returns the ids discarded.
    #[instrument(err, skip_all)]
    pub fn spend_unused_fork_snapshots(&self) -> R<Vec<String>> {
        let unused: Vec<String> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(
                "SELECT snapshot_id FROM snapshot WHERE purpose = 'fork' \
                 AND state IN ('local', 'published') \
                 AND NOT EXISTS (SELECT 1 FROM instance \
                 WHERE instance.restore_from = snapshot.snapshot_id);",
            )?;
            let rows = stmt.query_map([], |row| row.get(0))?;
            Self::collect_rows(rows)?
        };
        if unused.is_empty() {
            return Ok(Vec::new());
        }
        self.replicated(Lane::Scheduler, "spend_unused_fork_snapshots", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let mut spent = Vec::new();
            for snapshot_id in unused {
                if Self::spend_fork_snapshot_if_unused_tx(&tx, &snapshot_id)? {
                    spent.push(snapshot_id);
                }
            }
            tx.commit()?;
            Ok(spent)
        })
    }

    /// Create keyed instances that restore from `snapshot_id` (the
    /// `from_snapshot` half of CreateInstance(s)), all or nothing. The
    /// handler validates the snapshot; this re-checks what a concurrent
    /// write could change.
    #[instrument(err, skip_all, fields(job_id, snapshot_id))]
    pub fn create_keyed_instances_from_snapshot(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        requested_ids: &[String],
        mint_count: u32,
        snapshot_id: &str,
    ) -> R<Vec<String>> {
        let job_id = job_id.to_owned();
        let group = group.to_owned();
        let task_name = task_name.to_owned();
        let requested_ids = requested_ids.to_vec();
        let snapshot_id = snapshot_id.to_owned();
        self.replicated(
            Lane::Scheduler,
            "create_keyed_instances_from_snapshot",
            move |s| {
                let mut conn = s.write_connection()?;
                let tx = Self::write_tx(&mut conn)?;
                let snapshot = Self::get_snapshot_on(&tx, &snapshot_id)?
                    .ok_or_else(|| StorageError::not_found("snapshot", &snapshot_id))?;
                if !matches!(
                    snapshot.state,
                    SnapshotState::Local | SnapshotState::Published
                ) {
                    bail!(StorageError::ForeignKey {
                        message: format!(
                            "snapshot {snapshot_id} is {}; only a captured (local or published) \
                             snapshot can seed new instances",
                            snapshot.state
                        ),
                    });
                }
                let mut created = Vec::with_capacity(requested_ids.len() + mint_count as usize);
                for id in &requested_ids {
                    created.push(s.insert_keyed_instance_tx(
                        &tx,
                        &job_id,
                        &group,
                        &task_name,
                        Some(id),
                    )?);
                }
                for _ in 0..mint_count {
                    created
                        .push(s.insert_keyed_instance_tx(&tx, &job_id, &group, &task_name, None)?);
                }
                for id in &created {
                    tx.execute(
                        "UPDATE instance SET restore_from = ?2 WHERE instance_id = ?1;",
                        params![id, snapshot_id],
                    )?;
                }
                tx.commit()?;
                Ok(created)
            },
        )
    }

    /// Every name pointing at each of `snapshot_ids`, keyed by snapshot id.
    pub fn snapshot_aliases(&self, snapshot_ids: &[String]) -> R<HashMap<String, Vec<String>>> {
        let mut aliases: HashMap<String, Vec<String>> = HashMap::new();
        if snapshot_ids.is_empty() {
            return Ok(aliases);
        }
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT name FROM snapshot_alias WHERE snapshot_id = ?1 ORDER BY name ASC;",
        )?;
        for snapshot_id in snapshot_ids {
            let names: Vec<String> =
                Self::collect_rows(stmt.query_map([snapshot_id], |row| row.get(0))?)?;
            if !names.is_empty() {
                aliases.insert(snapshot_id.clone(), names);
            }
        }
        Ok(aliases)
    }

    /// Point `name` in `job_id` at `snapshot_id` (SetSnapshotAlias). The
    /// snapshot must be a `local`, `manual` snapshot of the job. The
    /// snapshot's own `name` column follows the alias, so `snapshot list`
    /// shows the name on the snapshot it currently resolves to.
    #[instrument(err, skip_all, fields(job_id, name, snapshot_id))]
    pub fn set_snapshot_alias(
        &self,
        job_id: &str,
        name: &str,
        snapshot_id: &str,
    ) -> R<AliasOutcome> {
        let job_id = job_id.to_owned();
        let name = name.to_owned();
        let snapshot_id = snapshot_id.to_owned();
        self.replicated(Lane::Scheduler, "set_snapshot_alias", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let snapshot = Self::get_snapshot_on(&tx, &snapshot_id)?
                .ok_or_else(|| StorageError::not_found("snapshot", &snapshot_id))?;
            if snapshot.job_id != job_id {
                bail!(StorageError::InvalidInput {
                    message: format!(
                        "snapshot {snapshot_id} belongs to job {}, not {job_id}; a name points \
                         only at a snapshot of its own job",
                        snapshot.job_id
                    ),
                });
            }
            if let Some(reason) = alias_target_problem(&snapshot) {
                bail!(StorageError::ForeignKey { message: reason });
            }
            let previous: Option<String> = tx
                .query_row(
                    "SELECT snapshot_id FROM snapshot_alias WHERE job_id = ?1 AND name = ?2;",
                    params![job_id, name],
                    |row| row.get(0),
                )
                .optional()?;
            if previous.as_deref() == Some(snapshot_id.as_str()) {
                return Ok(AliasOutcome {
                    previous,
                    changed: false,
                });
            }
            let now = chrono::Utc::now().timestamp();
            tx.execute(
                "INSERT INTO snapshot_alias (job_id, name, snapshot_id, updated_at) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT (job_id, name) DO UPDATE SET snapshot_id = excluded.snapshot_id, \
                 updated_at = excluded.updated_at;",
                params![job_id, name, snapshot_id, now],
            )?;
            if let Some(previous) = previous.as_deref() {
                Self::release_snapshot_name_tx(&tx, previous, &name)?;
            }
            tx.execute(
                "UPDATE snapshot SET name = ?2 WHERE snapshot_id = ?1 AND name IS NULL;",
                params![snapshot_id, name],
            )?;
            tx.commit()?;
            Ok(AliasOutcome {
                previous,
                changed: true,
            })
        })
    }

    /// Remove a snapshot name. Returns the snapshot it pointed at, `None`
    /// when the name did not exist.
    #[instrument(err, skip_all, fields(job_id, name))]
    pub fn delete_snapshot_alias(&self, job_id: &str, name: &str) -> R<Option<String>> {
        let job_id = job_id.to_owned();
        let name = name.to_owned();
        self.replicated(Lane::Scheduler, "delete_snapshot_alias", move |s| {
            let mut conn = s.write_connection()?;
            let tx = Self::write_tx(&mut conn)?;
            let target: Option<String> = tx
                .query_row(
                    "SELECT snapshot_id FROM snapshot_alias WHERE job_id = ?1 AND name = ?2;",
                    params![job_id, name],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(target) = target.as_deref() {
                tx.execute(
                    "DELETE FROM snapshot_alias WHERE job_id = ?1 AND name = ?2;",
                    params![job_id, name],
                )?;
                Self::release_snapshot_name_tx(&tx, target, &name)?;
            }
            tx.commit()?;
            Ok(target)
        })
    }
}

impl Store {
    /// `name` no longer points at `snapshot_id`: clear the snapshot's `name`
    /// column if it showed that name, falling back to another name that
    /// still points at it.
    fn release_snapshot_name_tx(tx: &Connection, snapshot_id: &str, name: &str) -> R<()> {
        tx.execute(
            "UPDATE snapshot SET name = (SELECT alias.name FROM snapshot_alias AS alias \
             WHERE alias.snapshot_id = ?1 ORDER BY alias.name ASC LIMIT 1) \
             WHERE snapshot_id = ?1 AND name = ?2;",
            params![snapshot_id, name],
        )?;
        Ok(())
    }
}

/// Why `snapshot` cannot seed new instances or carry a name, as the
/// FailedPrecondition message. `None` when it can: a `local` or `published`
/// `manual` snapshot. Shared by SetSnapshotAlias and `from_snapshot`.
pub fn alias_target_problem(snapshot: &SnapshotRecord) -> Option<String> {
    match snapshot.purpose {
        SnapshotPurpose::Manual => {}
        SnapshotPurpose::Suspend => {
            return Some(format!(
                "snapshot {} is instance {}'s suspend checkpoint, which is spent when it \
                 resumes\nHint: take a manual snapshot with `legion snapshot create`",
                snapshot.snapshot_id,
                snapshot.instance_id.as_deref().unwrap_or("-")
            ))
        }
        SnapshotPurpose::Fork => {
            return Some(format!(
                "snapshot {} is a fork checkpoint, which is spent once its forks have \
                 restored\nHint: take a manual snapshot with `legion snapshot create`",
                snapshot.snapshot_id
            ))
        }
    }
    // A published snapshot names and seeds like a local one, and its seeded
    // instances may go to any compatible node.
    if !matches!(
        snapshot.state,
        SnapshotState::Local | SnapshotState::Published
    ) {
        return Some(format!(
            "snapshot {} is {}{}; only a captured (local or published) snapshot can seed new \
             instances",
            snapshot.snapshot_id,
            snapshot.state,
            snapshot
                .error
                .as_deref()
                .map(|error| format!(" ({error})"))
                .unwrap_or_default()
        ));
    }
    None
}
