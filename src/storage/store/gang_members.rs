//! Gang members: each gang allocation's rank, and the facts about its gang
//! it starts with (docs/gang-scheduling.md, "Starting together").
//!
//! A gang places together, but each member starts on its own, so the
//! workload's rendezvous is where the members meet. What the rendezvous
//! needs from Legion is who each member is: its rank, the world size, the
//! quorum, and where rank 0 runs. They are assigned in the transaction that
//! inserts the allocation row ([`Store::assign_gang_member_tx`], called from
//! the one insert every placement path shares), kept in the `gang_member`
//! table, and injected into the task's env at dispatch
//! (`crate::manager::dispatcher::attach_gang_env`).
//!
//! - **Rank** is the lowest rank no live member of the task holds, so a
//!   fresh gang (a submit, or a `restart`) ranks `0..count-1`, and a
//!   `replace` top-up hands each missing rank to a replacement.
//! - **Size, quorum, and leader address are fixed at placement.** A running
//!   member's payload must not change: a changed payload is an in-place
//!   update, which restarts it. So a later `job scale` or a replaced rank 0
//!   reaches only the members placed after it.
//!
//! Rows of removed allocations are pruned when the task places again, and
//! with the job on destroy: at most one stale row per member lost since.

use super::*;

/// One gang member's identity, as its row records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GangMember {
    pub allocation_id: i64,
    /// `0..size-1`, unique among the task's live members.
    pub rank: u32,
    /// The gang's count when the member was placed (after `job scale`).
    pub size: u32,
    /// The gang's quorum (`gang.min`, else the count) when it was placed.
    pub quorum: u32,
    /// Where rank 0 ran when this member was placed: its allocation's
    /// network address, else the host of its node's `workerAddress`. `None`
    /// when that node advertises no address.
    pub leader_address: Option<String>,
}

impl GangMember {
    /// The variables the member starts with, in the order they are set.
    pub fn env(&self) -> Vec<(&'static str, String)> {
        let mut env = vec![
            ("LEGION_GANG_RANK", self.rank.to_string()),
            ("LEGION_GANG_SIZE", self.size.to_string()),
            ("LEGION_GANG_MIN", self.quorum.to_string()),
        ];
        if let Some(address) = &self.leader_address {
            env.push(("LEGION_GANG_LEADER_ADDR", address.clone()));
        }
        env
    }
}

impl Store {
    /// Bind a rank to `allocation`, just inserted in `tx`, when its task is a
    /// gang (module docs). Free for every other task: one read-model lookup.
    pub(crate) fn assign_gang_member_tx(&self, tx: &Connection, allocation: &Allocation) -> R<()> {
        let Some(task) = self.task_for_insert_tx(tx, allocation)? else {
            return Ok(());
        };
        let Some(gang) = task.gang.as_ref() else {
            return Ok(());
        };
        // Members removed since the task last placed hold no rank.
        tx.prepare_cached(
            "DELETE FROM gang_member
             WHERE job_config_id = ?1
               AND NOT EXISTS (SELECT 1 FROM allocation WHERE allocation.id = gang_member.allocation_id);",
        )?
        .execute([&allocation.job_config_id])?;
        let held: HashSet<u32> = {
            let mut stmt = tx.prepare_cached(
                "SELECT g.rank FROM gang_member g JOIN allocation a ON a.id = g.allocation_id
                 WHERE a.task_config_id = ?1 AND g.allocation_id != ?2;",
            )?;
            let rows = stmt
                .query_map(params![allocation.task_config_id, allocation.id], |row| {
                    row.get::<_, u32>(0)
                })?;
            rows.collect::<Result<_, _>>()?
        };
        let rank = (0..).find(|rank| !held.contains(rank)).unwrap_or_default();
        let size =
            self.effective_task_count(&allocation.job_id, &task.group, &task.name, task.count.num)?;
        let quorum = gang.quorum(size);
        let leader_address = if rank == 0 {
            Self::member_address_tx(tx, &allocation.node_id, allocation.network.as_ref())?
        } else {
            let leader: Option<(String, Option<String>)> = tx
                .prepare_cached(
                    "SELECT a.node_id, a.network FROM gang_member g
                     JOIN allocation a ON a.id = g.allocation_id
                     WHERE a.task_config_id = ?1 AND g.rank = 0 AND g.allocation_id != ?2
                     LIMIT 1;",
                )?
                .query_row(params![allocation.task_config_id, allocation.id], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .optional()?;
            match leader {
                Some((node_id, network)) => {
                    let network: Option<crate::structs::AllocNetwork> = network
                        .as_deref()
                        .and_then(|raw| serde_json::from_str(raw).ok());
                    Self::member_address_tx(tx, &node_id, network.as_ref())?
                }
                None => None,
            }
        };
        tx.prepare_cached(
            "INSERT OR REPLACE INTO gang_member
                 (allocation_id, job_id, job_config_id, rank, size, quorum, leader_address)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
        )?
        .execute(params![
            allocation.id,
            allocation.job_id,
            allocation.job_config_id,
            rank,
            size,
            quorum,
            leader_address
        ])?;
        Ok(())
    }

    /// The task of an allocation being inserted: from the read model when it
    /// holds the config (the common case), else from `tx`, which sees a
    /// config written earlier in the same transaction and caches nothing.
    fn task_for_insert_tx(
        &self,
        tx: &Connection,
        allocation: &Allocation,
    ) -> R<Option<TaskConfig>> {
        {
            let guard = self.read_model_reader()?;
            if let Some(tasks) = guard.task_configs.get(&allocation.job_config_id) {
                return Ok(tasks
                    .iter()
                    .find(|(id, _)| *id == allocation.task_config_id)
                    .filter(|(_, task)| task.gang.is_some())
                    .map(|(_, task)| task.clone()));
            }
        }
        Ok(tx
            .prepare_cached(&crate::storage::queries::GET_TASK_BY_ID)?
            .query_row([allocation.task_config_id], |row| {
                self.task_config_from_row(row)
            })
            .optional()?
            .filter(|task| task.gang.is_some()))
    }

    /// A member's address, as service discovery reports it: its allocation's
    /// network address, else the host of its node's `workerAddress`.
    fn member_address_tx(
        tx: &Connection,
        node_id: &str,
        network: Option<&crate::structs::AllocNetwork>,
    ) -> R<Option<String>> {
        if let Some(address) = network.and_then(|network| network.address.clone()) {
            return Ok(Some(address));
        }
        Ok(tx
            .prepare_cached("SELECT config_attrs FROM node WHERE id = ?1;")?
            .query_row([node_id], |row| row.get::<_, Value>(0))
            .optional()?
            .and_then(|attrs| worker_address_host(&attrs)))
    }

    /// The gang identity of `allocation_id`, if it is a member of a gang.
    pub fn gang_member(&self, allocation_id: i64) -> R<Option<GangMember>> {
        let conn = self.read_connection()?;
        let member = conn
            .prepare_cached(
                "SELECT allocation_id, rank, size, quorum, leader_address
                 FROM gang_member WHERE allocation_id = ?1;",
            )?
            .query_row([allocation_id], |row| {
                Ok(GangMember {
                    allocation_id: row.get(0)?,
                    rank: row.get(1)?,
                    size: row.get(2)?,
                    quorum: row.get(3)?,
                    leader_address: row.get(4)?,
                })
            })
            .optional()?;
        Ok(member)
    }

    pub(crate) fn delete_gang_members_for_job_tx(conn: &Connection, job_id: &str) -> R<()> {
        conn.prepare_cached("DELETE FROM gang_member WHERE job_id = ?1;")?
            .execute([job_id])?;
        Ok(())
    }
}
