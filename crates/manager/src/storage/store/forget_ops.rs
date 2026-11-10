//! `ForgetNode`: drop a node's record and revoke its node token
//! (docs/architecture-managers-workers.md, "Forgetting a node").
//!
//! A composite across lanes, like [`Store::remove_node`]: the scheduler
//! lane removes the node's allocations and marks its snapshots lost, then
//! one control-lane write deletes the `node` row and the node's worker
//! token together, so no reader ever sees the record gone while its token
//! still authenticates. `node_health` and `allocation_health` rows left
//! behind are swept by the status lane's orphan passes, as for any removal.

use super::*;

/// What forgetting a node took with it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ForgetOutcome {
    /// Allocations removed (and replaced elsewhere by the scheduler).
    pub allocations_removed: usize,
    /// Node tokens revoked.
    pub tokens_revoked: usize,
    /// Snapshots stored on the node, now `lost`.
    pub snapshots_lost: usize,
    /// Node-local volumes still pinned to the node.
    pub volumes_orphaned: usize,
}

impl Store {
    /// Node-local volumes pinned to `node_id` that still hold data.
    pub fn node_local_volumes_on(&self, node_id: &str) -> R<usize> {
        let conn = self.read_connection()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM volume
             WHERE node_id = ?1 AND node_local = 1 AND state IN ('pending', 'created');",
            [node_id],
            |row| row.get(0),
        )?;
        Ok(usize::try_from(count).unwrap_or(0))
    }

    /// Forget a node: remove its allocations (the scheduler replaces them),
    /// mark its snapshots lost, then delete its record and revoke its node
    /// token in one control-lane write. The caller decides whether the node
    /// may be forgotten (the `ForgetNode` guard); this does it.
    #[instrument(err, skip_all, fields(node_id))]
    pub fn forget_node(&self, node_id: &str) -> R<ForgetOutcome> {
        let volumes_orphaned = self.node_local_volumes_on(node_id)?;
        // The same path a drain takes for what it evicts: remove, let a
        // gang that lost its quorum react, then re-plan the affected
        // configs so the replacements place elsewhere.
        let removed = self.allocs_for_node(node_id)?;
        let affected = self.remove_allocations(&removed, StopCause::NodeLost)?;
        let affected = self.gang_members_lost_on_node(&removed, StopCause::NodeLost, affected);
        let snapshots_lost = self.mark_node_snapshots_lost(node_id)?;
        let id = node_id.to_owned();
        let tokens_revoked = self.replicated(Lane::Control, "forget_node", move |s| {
            s.forget_node_apply(&id)
        })?;
        if let Err(err) = self.reconcile_job_configs_or_requeue(affected) {
            // The record is gone either way; a config that failed to re-plan
            // has its own delayed reschedule queued.
            warn!(node_id, error = %err, "Re-planning after forgetting a node failed");
        }
        if !removed.is_empty() {
            self.events()
                .allocations_removed_from_node(node_id, &removed, StopCause::NodeLost);
        }
        Ok(ForgetOutcome {
            allocations_removed: removed.len(),
            tokens_revoked,
            snapshots_lost,
            volumes_orphaned,
        })
    }

    fn forget_node_apply(&self, node_id: &str) -> R<usize> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        tx.execute("DELETE FROM node WHERE id = ?1;", [node_id])?;
        let tokens = tx.execute(
            "DELETE FROM acl_token WHERE token_type = 'worker' AND name = ?1;",
            [&format!("node:{node_id}")],
        )?;
        tx.commit()?;
        drop(conn);
        self.cache_remove_node(node_id)?;
        Ok(tokens)
    }
}
