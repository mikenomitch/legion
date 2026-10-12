//! Store half of session promotion (docs/session-mode.md, "Promotion";
//! sandbox-scaling plan §6.6).
//!
//! A running session has no row. Promoting it writes the two rows a keyed
//! instance has, in one transaction: the instance, and an allocation bound to
//! it on the session's node. The node adopts the running session as that
//! allocation (`TaskAllocation.promoted_from_session`), so nothing restarts.
//!
//! The allocation reserves nothing: the session already sits inside its
//! node's envelope, whose allocation holds the room, and stays there until
//! it stops. A replacement the scheduler places after node loss is an
//! ordinary keyed placement with the task's full resources.

use super::*;

/// What a promotion wrote.
#[derive(Debug, Clone)]
pub struct PromotedSession {
    pub instance: Instance,
    pub allocation: Allocation,
}

/// The session a promotion acts on, as the session index reported it.
#[derive(Debug, Clone)]
pub struct PromoteSession {
    pub session_id: String,
    pub job_id: String,
    pub job_config_id: String,
    pub group: String,
    pub task_name: String,
    pub node_id: String,
    /// The name to give it. `None` keeps the session id, unnamed.
    pub new_id: Option<String>,
}

impl Store {
    /// Write a promoted session's instance and allocation rows. Refuses an
    /// id that names any existing instance, tombstones included, exactly as
    /// CreateInstance does. Call inside a scheduler-lane write (the
    /// PromoteInstance handler runs it under group commit with its
    /// idempotency record).
    #[instrument(err, skip_all, fields(session_id = %promote.session_id))]
    pub fn promote_session(&self, promote: &PromoteSession) -> R<PromotedSession> {
        let promote = promote.clone();
        self.replicated(Lane::Scheduler, "promote_session", move |s| {
            s.promote_session_apply(&promote)
        })
    }

    fn promote_session_apply(&self, promote: &PromoteSession) -> R<PromotedSession> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let instance_id = promote
            .new_id
            .clone()
            .unwrap_or_else(|| promote.session_id.clone());
        let named = promote.new_id.is_some();
        if Self::instance_id_exists_tx(&tx, &instance_id)? {
            bail!(StorageError::Conflict {
                message: format!(
                    "instance id {instance_id} already exists (keyed ids are never recycled, \
                     including stopped and destroyed ones); choose a different id"
                ),
            });
        }
        if named
            && Self::keyed_id_collides_with_slot_pattern_tx(&tx, &promote.job_id, &instance_id)?
        {
            bail!(StorageError::Conflict {
                message: format!(
                    "instance id {instance_id} collides with the reserved slot-id pattern of a \
                     slot-mode stateful task in job {}",
                    promote.job_id
                ),
            });
        }
        let task_id: i64 = tx
            .query_row(
                "SELECT id FROM task_config WHERE job_config_id = ?1 AND name = ?2;",
                params![promote.job_config_id, promote.task_name],
                |row| row.get(0),
            )
            .optional()?
            .with_context(|| {
                format!(
                    "the session's job config {} no longer has task {}",
                    promote.job_config_id, promote.task_name
                )
            })?;
        let now = Self::now_rfc3339()?;
        tx.execute(
            "INSERT INTO instance (instance_id, job_id, group_name, task_name, mode, state, \
             slot_index, current_alloc_id, storage_handle, created_at, updated_at, named, \
             promoted_from) \
             VALUES (?1, ?2, ?3, ?4, 'keyed', 'pending', NULL, NULL, NULL, ?5, ?5, ?6, ?7);",
            params![
                instance_id,
                promote.job_id,
                promote.group,
                promote.task_name,
                now,
                named,
                promote.session_id
            ],
        )?;
        // Bound to exactly this instance, on the session's node, reserving
        // nothing of its own (the envelope already does).
        let allocation = self.insert_allocation_with_data(
            &tx,
            &promote.node_id,
            task_id,
            &promote.job_config_id,
            &json!({}),
            None,
            Some(&instance_id),
        )?;
        self.bump_epochs_for_allocations_tx(&tx, std::slice::from_ref(&allocation))?;
        let instance = tx.query_row(&GET_INSTANCE, [instance_id.as_str()], |row| {
            self.instance_from_row(row)
        })?;
        tx.commit()?;
        Ok(PromotedSession {
            instance,
            allocation,
        })
    }
}
