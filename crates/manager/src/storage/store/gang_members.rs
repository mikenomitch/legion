//! Gang members: each gang allocation's rank, and the facts about its gang
//! it starts with (docs/gang-scheduling.md, "Starting together").
//!
//! A gang places together, but each member starts on its own, so the
//! workload's rendezvous is where the members meet. What the rendezvous
//! needs from Legion is who each member is: its rank, the world size, the
//! quorum, where rank 0 runs, and which incarnation of the gang it belongs
//! to (the epoch). They are assigned in the transaction that inserts the
//! allocation row ([`Store::assign_gang_member_tx`], called from the one
//! insert every placement path shares), kept in the `gang_member` table, and
//! injected into the task's env at dispatch
//! (`crate::manager::dispatcher::attach_gang_env`).
//!
//! - **Rank** is the lowest rank no live member of the task holds, so a
//!   fresh gang (a submit, or a `restart`) ranks `0..count-1`, and a
//!   `replace` top-up hands each missing rank to a replacement.
//! - **The view stays current.** Size, quorum, and leader address describe
//!   the gang as it is now: when they change (a `job scale`, rank 0 placed
//!   again on another node, a scale-in that leaves a rank at or above the
//!   new size) every live member's row is rewritten and the gang's **epoch**
//!   goes up by one ([`Store::sync_gang_view_tx`]). A member's process
//!   cannot see new environment variables, so the epoch is what workers act
//!   on: every heartbeat reply carries the view of each member on the node
//!   ([`Store::gang_views_for_node`]), and a worker whose member started
//!   under an older epoch restarts it in place with the new values. A
//!   `restartInPlace` loss raises the epoch with no other change
//!   ([`Store::bump_gang_epoch_tx`]).
//! - **Readiness.** `placed_unix` is when the member was placed and `ready`
//!   whether it has reported healthy once; the scheduler leader's gang
//!   sweep marks members ready and enforces `gang.readyTimeout`
//!   (`store/gang_sweep.rs`).
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
    /// The gang's count (after `job scale`).
    pub size: u32,
    /// The gang's quorum (`gang.min`, else the count).
    pub quorum: u32,
    /// Where rank 0 runs: its allocation's network address, else the host
    /// of its node's `workerAddress`. `None` when that node advertises no
    /// address.
    pub leader_address: Option<String>,
    /// The gang's incarnation (module docs).
    pub epoch: u64,
}

impl GangMember {
    /// The variables the member starts with, in the order they are set.
    pub fn env(&self) -> Vec<(&'static str, String)> {
        let mut env = vec![
            ("LEGION_GANG_RANK", self.rank.to_string()),
            ("LEGION_GANG_SIZE", self.size.to_string()),
            ("LEGION_GANG_MIN", self.quorum.to_string()),
            ("LEGION_GANG_EPOCH", self.epoch.to_string()),
        ];
        if let Some(address) = &self.leader_address {
            env.push(("LEGION_GANG_LEADER_ADDR", address.clone()));
        }
        env
    }

    /// The heartbeat feed's shape (`HeartbeatReply.gang_members`).
    pub fn to_view(&self) -> crate::structs::GangMemberView {
        crate::structs::GangMemberView {
            allocation_id: self.allocation_id,
            rank: self.rank,
            size: self.size,
            min: self.quorum,
            leader_address: self.leader_address.clone().unwrap_or_default(),
            epoch: self.epoch,
        }
    }
}

/// One live member as the view sync reads it.
pub(crate) struct LiveMember {
    allocation_id: i64,
    node_id: String,
    network: Option<String>,
    pub(crate) member: GangMember,
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
        // The gang's epoch so far, stale rows included: a gang placed afresh
        // after a `restart` (every old member gone) is a new incarnation.
        let last_epoch: Option<i64> = tx
            .prepare_cached(
                "SELECT MAX(g.epoch) FROM gang_member g
                 WHERE g.job_config_id = ?1 AND g.allocation_id != ?2;",
            )?
            .query_row(params![allocation.job_config_id, allocation.id], |row| {
                row.get(0)
            })?;
        // Members removed since the task last placed hold no rank.
        tx.prepare_cached(
            "DELETE FROM gang_member
             WHERE job_config_id = ?1
               AND NOT EXISTS (SELECT 1 FROM allocation WHERE allocation.id = gang_member.allocation_id);",
        )?
        .execute([&allocation.job_config_id])?;
        let live = Self::live_gang_members_tx(tx, allocation.task_config_id, Some(allocation.id))?;
        let held: HashSet<u32> = live.iter().map(|live| live.member.rank).collect();
        let rank = (0..).find(|rank| !held.contains(rank)).unwrap_or_default();
        let epoch = match live.iter().map(|live| live.member.epoch).max() {
            Some(epoch) => epoch,
            None => last_epoch.map_or(0, |epoch| epoch.max(0) as u64 + 1),
        };
        let size =
            self.effective_task_count(&allocation.job_id, &task.group, &task.name, task.count.num)?;
        let quorum = gang.quorum(size);
        tx.prepare_cached(
            "INSERT OR REPLACE INTO gang_member
                 (allocation_id, job_id, job_config_id, rank, size, quorum, leader_address,
                  epoch, placed_unix, ready)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?8, 0);",
        )?
        .execute(params![
            allocation.id,
            allocation.job_id,
            allocation.job_config_id,
            rank,
            size,
            quorum,
            epoch as i64,
            Self::now_unix(),
        ])?;
        // The new member's leader address, and every other member's view if
        // this placement changed it (a new size, a new rank 0).
        Self::sync_gang_view_tx(
            tx,
            allocation.task_config_id,
            size,
            quorum,
            Some(allocation.id),
        )?;
        Ok(())
    }

    /// The live members of a gang task, by rank, optionally leaving one out.
    pub(crate) fn live_gang_members_tx(
        tx: &Connection,
        task_config_id: i64,
        except: Option<i64>,
    ) -> R<Vec<LiveMember>> {
        let mut stmt = tx.prepare_cached(
            "SELECT g.allocation_id, g.rank, g.size, g.quorum, g.leader_address, g.epoch,
                    a.node_id, a.network
             FROM gang_member g JOIN allocation a ON a.id = g.allocation_id
             WHERE a.task_config_id = ?1 AND g.allocation_id != ?2
             ORDER BY g.rank, g.allocation_id;",
        )?;
        let rows = stmt.query_map(params![task_config_id, except.unwrap_or(0)], |row| {
            Ok(LiveMember {
                allocation_id: row.get(0)?,
                member: GangMember {
                    allocation_id: row.get(0)?,
                    rank: row.get(1)?,
                    size: row.get(2)?,
                    quorum: row.get(3)?,
                    leader_address: row.get(4)?,
                    epoch: row.get::<_, i64>(5)?.max(0) as u64,
                },
                node_id: row.get(6)?,
                network: row.get(7)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Bring every live member's view of the gang of `task_config_id` to
    /// `size` and `quorum`, ranks dense below `size`, and rank 0's current
    /// address; when any running member's view changes, raise the gang's
    /// epoch for all of them (module docs). `fresh` is a member placed in
    /// the calling transaction, whose own view filling in changes nothing
    /// for the rest. Returns whether the epoch went up.
    pub(crate) fn sync_gang_view_tx(
        tx: &Connection,
        task_config_id: i64,
        size: u32,
        quorum: u32,
        fresh: Option<i64>,
    ) -> R<bool> {
        let live = Self::live_gang_members_tx(tx, task_config_id, None)?;
        if live.is_empty() {
            return Ok(false);
        }
        // Ranks: keep each member's when it is below the size and unique,
        // and hand the rest the lowest free ranks (a scale-in can leave a
        // rank at or above the new size).
        let mut taken: HashSet<u32> = HashSet::new();
        let ceiling = size.max(live.len() as u32);
        let mut ranks: Vec<Option<u32>> = live
            .iter()
            .map(|member| {
                let rank = member.member.rank;
                (rank < ceiling && taken.insert(rank)).then_some(rank)
            })
            .collect();
        for rank in ranks.iter_mut().filter(|rank| rank.is_none()) {
            let free = (0..)
                .find(|candidate| !taken.contains(candidate))
                .unwrap_or_default();
            taken.insert(free);
            *rank = Some(free);
        }
        let leader = live
            .iter()
            .zip(&ranks)
            .find(|(_, rank)| **rank == Some(0))
            .map(|(live, _)| live);
        let leader_address = match leader {
            Some(leader) => {
                let network: Option<crate::structs::AllocNetwork> = leader
                    .network
                    .as_deref()
                    .and_then(|raw| serde_json::from_str(raw).ok());
                Some(Self::member_address_tx(
                    tx,
                    &leader.node_id,
                    network.as_ref(),
                )?)
            }
            // Rank 0 is being replaced: the members keep the address they
            // had until a new rank 0 places.
            None => None,
        };
        let differs = |live: &LiveMember, rank: &Option<u32>| {
            let member = &live.member;
            Some(member.rank) != *rank
                || member.size != size
                || member.quorum != quorum
                || leader_address
                    .as_ref()
                    .is_some_and(|address| member.leader_address != *address)
        };
        if !live
            .iter()
            .zip(&ranks)
            .any(|(live, rank)| differs(live, rank))
        {
            return Ok(false);
        }
        // The member being placed in this transaction runs nothing yet: its
        // own view filling in is not a change the others must restart for.
        let others_changed = live
            .iter()
            .zip(&ranks)
            .any(|(live, rank)| Some(live.allocation_id) != fresh && differs(live, rank));
        let epoch = live.iter().map(|live| live.member.epoch).max().unwrap_or(0)
            + u64::from(others_changed);
        for (live, rank) in live.iter().zip(&ranks) {
            let address = match &leader_address {
                Some(address) => address.clone(),
                None => live.member.leader_address.clone(),
            };
            tx.prepare_cached(
                "UPDATE gang_member
                 SET rank = ?2, size = ?3, quorum = ?4, leader_address = ?5, epoch = ?6
                 WHERE allocation_id = ?1;",
            )?
            .execute(params![
                live.allocation_id,
                rank.unwrap_or(live.member.rank),
                size,
                quorum,
                address,
                epoch as i64
            ])?;
        }
        Ok(others_changed)
    }

    /// Raise the epoch of every live member of `task_config_id` by one, so
    /// each restarts in place (`onMemberLoss = "restartInPlace"`). Returns
    /// how many members it reached.
    pub(crate) fn bump_gang_epoch_tx(tx: &Connection, task_config_id: i64) -> R<usize> {
        let live = Self::live_gang_members_tx(tx, task_config_id, None)?;
        let Some(epoch) = live.iter().map(|live| live.member.epoch).max() else {
            return Ok(0);
        };
        for live in &live {
            tx.prepare_cached("UPDATE gang_member SET epoch = ?2 WHERE allocation_id = ?1;")?
                .execute(params![live.allocation_id, (epoch + 1) as i64])?;
        }
        Ok(live.len())
    }

    /// The current view of every gang member on `node_id`, for the
    /// heartbeat reply (`HeartbeatReply.gang_members`). One indexed query;
    /// empty for a node that runs no gang member.
    pub fn gang_views_for_node(&self, node_id: &str) -> R<Vec<crate::structs::GangMemberView>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT g.allocation_id, g.rank, g.size, g.quorum, g.leader_address, g.epoch
             FROM allocation a JOIN gang_member g ON g.allocation_id = a.id
             WHERE a.node_id = ?1
             ORDER BY g.allocation_id;",
        )?;
        let rows = stmt.query_map([node_id], |row| {
            Ok(GangMember {
                allocation_id: row.get(0)?,
                rank: row.get(1)?,
                size: row.get(2)?,
                quorum: row.get(3)?,
                leader_address: row.get(4)?,
                epoch: row.get::<_, i64>(5)?.max(0) as u64,
            })
        })?;
        Ok(rows
            .map(|member| member.map(|member| member.to_view()))
            .collect::<Result<_, _>>()?)
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
                "SELECT allocation_id, rank, size, quorum, leader_address, epoch
                 FROM gang_member WHERE allocation_id = ?1;",
            )?
            .query_row([allocation_id], |row| {
                Ok(GangMember {
                    allocation_id: row.get(0)?,
                    rank: row.get(1)?,
                    size: row.get(2)?,
                    quorum: row.get(3)?,
                    leader_address: row.get(4)?,
                    epoch: row.get::<_, i64>(5)?.max(0) as u64,
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
