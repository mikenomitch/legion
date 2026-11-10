//! Keyed children (docs/child-sessions.md): a keyed instance that another
//! unit of its job owns.
//!
//! The owner reference is the `owner` column of the child's own row
//! (`kind:id`), written by the create and never changed. Because both the
//! owner and the child are raft rows, the cascade needs no notice and no
//! node: [`Store::stop_keyed_instance`] and [`Store::destroy_keyed_instance`]
//! end an owner's keyed children in the entry that ends the owner, applying
//! the owner's verb. Owners that end any other way (an allocation that
//! completes or is lost, a slot instance scaled away, a job stopped) are
//! caught by [`Store::end_orphaned_keyed_children`], which the leader runs
//! on its maintenance tick.

use super::*;
use crate::storage::queries::INSTANCE_SELECT_ATTRS;
use crate::structs::children::{OWNER_ALLOCATION, OWNER_INSTANCE};

/// Allocation statuses that are an end (child-sessions plan §6.4).
const TERMINAL_ALLOCATION: [&str; 4] = ["complete", "failed", "stopped", "lost"];

/// What a keyed child's create came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyedChildOutcome {
    Created(Box<Instance>),
    /// The owner already has this many children; nothing was written.
    AtMax { children: u32 },
}

/// The verb an owner's end applies to its keyed children.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerEnd {
    Stopped,
    Destroyed,
}

impl OwnerEnd {
    pub fn as_str(self) -> &'static str {
        match self {
            OwnerEnd::Stopped => "stopped",
            OwnerEnd::Destroyed => "destroyed",
        }
    }
}

/// One keyed child the cascade or the sweep ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndedChild {
    pub instance_id: String,
    pub job_id: String,
    pub owner: String,
    pub end: OwnerEnd,
}

impl Store {
    /// The keyed children of `owner` (`kind:id`) that have not been
    /// destroyed, oldest first.
    pub fn keyed_children_of(&self, owner: &str) -> R<Vec<Instance>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT {} FROM instance WHERE owner = ?1 AND state != 'terminated' \
             ORDER BY created_at, instance_id;",
            INSTANCE_SELECT_ATTRS
        ))?;
        let rows = stmt.query_map([owner], |row| self.instance_from_row(row))?;
        Self::collect_rows(rows)
    }

    /// Whether `owner` (`kind:id`) has ended, as the store sees it, and how.
    /// A suspended, pending, or rescheduling instance has not ended; a
    /// missing row has (an unnamed instance's row is deleted on destroy, and
    /// a finished allocation's row is removed).
    pub fn raft_owner_end(&self, owner: &str) -> R<Option<OwnerEnd>> {
        let conn = self.read_connection()?;
        Self::raft_owner_end_on(&conn, owner)
    }

    fn raft_owner_end_on(conn: &Connection, owner: &str) -> R<Option<OwnerEnd>> {
        let Some((kind, id)) = owner.split_once(':') else {
            return Ok(Some(OwnerEnd::Stopped));
        };
        match kind {
            OWNER_INSTANCE => {
                let state: Option<String> = conn
                    .query_row(
                        "SELECT state FROM instance WHERE instance_id = ?1;",
                        [id],
                        |row| row.get(0),
                    )
                    .optional()?;
                Ok(match state.as_deref() {
                    Some("stopped") => Some(OwnerEnd::Stopped),
                    Some("terminated") | None => Some(OwnerEnd::Destroyed),
                    Some(_) => None,
                })
            }
            OWNER_ALLOCATION => {
                let Ok(allocation_id) = id.parse::<i64>() else {
                    return Ok(Some(OwnerEnd::Stopped));
                };
                let status: Option<String> = conn
                    .query_row(
                        "SELECT status FROM allocation WHERE id = ?1;",
                        [allocation_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                Ok(match status.as_deref() {
                    Some(status) if TERMINAL_ALLOCATION.contains(&status) => {
                        Some(OwnerEnd::Stopped)
                    }
                    Some(_) => None,
                    None => Some(OwnerEnd::Stopped),
                })
            }
            // A session owner is its node's to judge, and owns no keyed
            // child (a durable child of an ephemeral owner is refused).
            _ => Ok(None),
        }
    }

    /// Keyed children of a task awaiting their first placement, each with
    /// the node its owner runs on: `(instance_id, node_id)`, oldest first.
    /// The scheduler prefers that node (child-sessions plan §6.3). Empty,
    /// after one indexed read, for every task with no pending child.
    pub fn owner_node_preferences_for_task(&self, task_id: i64) -> R<Vec<(String, String)>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT instance.instance_id, instance.owner FROM instance \
             INNER JOIN task_config ON task_config.id = ?1 \
             WHERE instance.owner IS NOT NULL \
             AND instance.job_id = task_config.job_id \
             AND instance.group_name = task_config.group_name \
             AND instance.task_name = task_config.name \
             AND instance.state = 'pending' AND instance.current_alloc_id IS NULL \
             ORDER BY instance.created_at ASC, instance.instance_id ASC;",
        )?;
        let pending: Vec<(String, String)> =
            Self::collect_rows(stmt.query_map([task_id], |row| Ok((row.get(0)?, row.get(1)?)))?)?;
        drop(stmt);
        Self::owner_nodes_on(&conn, pending)
    }

    /// [`Self::owner_node_preferences_for_task`] for a task named by job,
    /// group, and name (a dry-run plan of an unsubmitted config).
    pub fn owner_node_preferences_for_task_named(
        &self,
        job_id: &str,
        group_name: &str,
        task_name: &str,
    ) -> R<Vec<(String, String)>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT instance_id, owner FROM instance \
             WHERE owner IS NOT NULL AND job_id = ?1 AND group_name = ?2 AND task_name = ?3 \
             AND state = 'pending' AND current_alloc_id IS NULL \
             ORDER BY created_at ASC, instance_id ASC;",
        )?;
        let pending: Vec<(String, String)> = Self::collect_rows(
            stmt.query_map(params![job_id, group_name, task_name], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?,
        )?;
        drop(stmt);
        Self::owner_nodes_on(&conn, pending)
    }

    /// Resolve each `(child, owner)` to `(child, owner's node)`, dropping
    /// children whose owner is placed nowhere right now.
    fn owner_nodes_on(conn: &Connection, pending: Vec<(String, String)>) -> R<Vec<(String, String)>> {
        let mut preferences = Vec::with_capacity(pending.len());
        for (instance_id, owner) in pending {
            let Some((kind, id)) = owner.split_once(':') else {
                continue;
            };
            let node: Option<String> = match kind {
                OWNER_INSTANCE => conn
                    .query_row(
                        "SELECT node_id FROM allocation WHERE instance_id = ?1 LIMIT 1;",
                        [id],
                        |row| row.get(0),
                    )
                    .optional()?,
                OWNER_ALLOCATION => match id.parse::<i64>() {
                    Ok(allocation_id) => conn
                        .query_row(
                            "SELECT node_id FROM allocation WHERE id = ?1;",
                            [allocation_id],
                            |row| row.get(0),
                        )
                        .optional()?,
                    Err(_) => None,
                },
                _ => None,
            };
            if let Some(node_id) = node {
                preferences.push((instance_id, node_id));
            }
        }
        Ok(preferences)
    }

    /// Register a keyed instance as a child of `owner` (`kind:id`): the row
    /// and its owner reference in one transaction, refused when the owner
    /// has ended or already owns `max` children. `other_children` is what
    /// the owner owns outside the store (its session children, which the
    /// caller counted), so `max` bounds both kinds together.
    #[instrument(err, skip_all, fields(owner))]
    pub fn create_keyed_child_instance(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        requested_id: Option<&str>,
        owner: &str,
        max: u32,
        other_children: u32,
    ) -> R<KeyedChildOutcome> {
        let job_id = job_id.to_owned();
        let group = group.to_owned();
        let task_name = task_name.to_owned();
        let requested_id = requested_id.map(str::to_owned);
        let owner = owner.to_owned();
        self.replicated(Lane::Scheduler, "create_keyed_child_instance", move |s| {
            s.create_keyed_child_instance_apply(
                &job_id,
                &group,
                &task_name,
                requested_id.as_deref(),
                &owner,
                max,
                other_children,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn create_keyed_child_instance_apply(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        requested_id: Option<&str>,
        owner: &str,
        max: u32,
        other_children: u32,
    ) -> R<KeyedChildOutcome> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        // Checked in the write: an owner that ended between the handler's
        // read and this entry owns nothing, so nothing could end the child.
        if Self::raft_owner_end_on(&tx, owner)?.is_some() {
            bail!(StorageError::not_found(
                "owner",
                owner.split_once(':').map_or(owner, |(_, id)| id)
            ));
        }
        let keyed: u32 = tx.query_row(
            "SELECT COUNT(*) FROM instance WHERE owner = ?1 AND state != 'terminated';",
            [owner],
            |row| row.get(0),
        )?;
        let children = keyed.saturating_add(other_children);
        if children >= max {
            return Ok(KeyedChildOutcome::AtMax { children });
        }
        let instance_id =
            self.insert_keyed_instance_tx(&tx, job_id, group, task_name, requested_id)?;
        tx.execute(
            "UPDATE instance SET owner = ?2 WHERE instance_id = ?1;",
            params![instance_id, owner],
        )?;
        let instance = tx.query_row(&GET_INSTANCE, [instance_id.as_str()], |row| {
            self.instance_from_row(row)
        })?;
        tx.commit()?;
        Ok(KeyedChildOutcome::Created(Box::new(instance)))
    }

    /// End the keyed children of `owner` with the owner's verb, inside the
    /// entry that ends the owner. A child already at or past the verb is
    /// left as it is. Called before the owner's own rows change, so a
    /// reader never sees an ended owner with a running keyed child.
    pub(super) fn cascade_keyed_children_apply(
        &self,
        owner: &str,
        end: OwnerEnd,
    ) -> R<Vec<EndedChild>> {
        let children = self.keyed_children_of(owner)?;
        let mut ended = Vec::new();
        for child in children {
            if child.mode != InstanceMode::Keyed {
                continue;
            }
            let acted = match end {
                OwnerEnd::Stopped => {
                    child.state != InstanceState::Stopped
                        && matches!(
                            self.stop_keyed_instance_apply(&child.instance_id)?,
                            StopInstanceOutcome::Stopped { .. }
                        )
                }
                OwnerEnd::Destroyed => matches!(
                    self.destroy_keyed_instance_apply(&child.instance_id)?,
                    DestroyInstanceOutcome::Destroyed { .. }
                ),
            };
            if acted {
                self.events()
                    .child_ended(&child.job_id, &child.instance_id, owner, end.as_str());
                info!(
                    instance_id = %child.instance_id,
                    job_id = %child.job_id,
                    owner,
                    end = end.as_str(),
                    "Ended a keyed child with its owner"
                );
                ended.push(EndedChild {
                    instance_id: child.instance_id,
                    job_id: child.job_id,
                    owner: owner.to_string(),
                    end,
                });
            }
        }
        Ok(ended)
    }

    /// Keyed children whose owner ended some way other than an instance
    /// stop or destroy (those cascade in their own entry): an `allocation`
    /// owner that completed, failed, or was lost, or an instance owner
    /// ended by its job's stop or a scale-down. Reads only; the leader
    /// calls [`Store::end_orphaned_keyed_children`] when this is not empty.
    pub fn orphaned_keyed_children(&self) -> R<Vec<(String, String)>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT instance_id, owner, state FROM instance \
             WHERE owner IS NOT NULL AND state != 'terminated';",
        )?;
        let rows: Vec<(String, String, String)> = Self::collect_rows(
            stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?,
        )?;
        drop(stmt);
        let mut orphaned = Vec::new();
        for (instance_id, owner, state) in rows {
            match Self::raft_owner_end_on(&conn, &owner)? {
                Some(OwnerEnd::Destroyed) => orphaned.push((instance_id, owner)),
                Some(OwnerEnd::Stopped) if state != "stopped" => orphaned.push((instance_id, owner)),
                _ => {}
            }
        }
        Ok(orphaned)
    }

    /// End every keyed child whose owner has ended (see
    /// [`Store::orphaned_keyed_children`]), in one scheduler-lane entry.
    /// Each owner's state is read again inside the entry.
    #[instrument(err, skip_all)]
    pub fn end_orphaned_keyed_children(&self) -> R<Vec<EndedChild>> {
        self.replicated(Lane::Scheduler, "end_orphaned_keyed_children", move |s| {
            let mut owners: Vec<String> = s
                .orphaned_keyed_children()?
                .into_iter()
                .map(|(_, owner)| owner)
                .collect();
            owners.sort();
            owners.dedup();
            let mut ended = Vec::new();
            for owner in owners {
                if let Some(end) = s.raft_owner_end(&owner)? {
                    ended.extend(s.cascade_keyed_children_apply(&owner, end)?);
                }
            }
            Ok(ended)
        })
    }
}
