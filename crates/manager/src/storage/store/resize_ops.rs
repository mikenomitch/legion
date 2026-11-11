//! Store half of resizing a top-level allocation in place (docs/resize.md;
//! resize plan §6.2).
//!
//! A resize is one scheduler-lane compare-and-set on the allocation's row,
//! the shape an elastic envelope's resize already has
//! ([`super::envelope_ops`]). The row's `resources` is rewritten to the new
//! effective amounts, so everything that reads rows sees them: the
//! scheduler's read model and free pools, plan apply's capacity recheck,
//! preemption, `legion node usage`, and the payload a worker launches from.
//! `resources_override` records what differs from the spec, who set it, and
//! why. It bumps the node's and the job config's allocation epochs like a
//! placement does.
//!
//! **A grow takes only free room**, recomputed from committed rows inside
//! this transaction with the allocation's current size given back; it never
//! preempts, and a refused grow writes nothing. **A shrink is committed
//! after the node applied it**, so the room is released only once the unit
//! no longer uses it.

use super::*;
use crate::structs::resize::{ResourceSize, ResourcesOverride};

/// One allocation resize, as a manager asks for it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AllocationResizeRequest {
    pub allocation_id: i64,
    /// The effective size the manager read: a compare-and-set.
    pub from: ResourceSize,
    /// The full effective size after the resize.
    pub to: ResourceSize,
    #[serde(default)]
    pub reason: String,
    /// Who asked: an accessor id or principal.
    #[serde(default)]
    pub by: String,
    /// Check and answer without writing.
    #[serde(default)]
    pub dry_run: bool,
}

/// What an allocation resize did.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AllocationResizeOutcome {
    /// `applied`, `refused` (no room), `stale` (the row is not at `from`),
    /// `gone` (no live allocation), or `invalid`.
    pub outcome: String,
    pub message: String,
    /// The committed effective size after this answer, when there is a row.
    pub size: Option<ResourceSize>,
    /// On a refused grow: the node's free room beyond what the allocation
    /// already holds, by resource.
    #[serde(default)]
    pub free: BTreeMap<String, f64>,
    #[serde(default)]
    pub node_id: String,
}

impl AllocationResizeOutcome {
    fn answer(outcome: &str, message: impl Into<String>, size: Option<ResourceSize>) -> Self {
        AllocationResizeOutcome {
            outcome: outcome.to_string(),
            message: message.into(),
            size,
            free: BTreeMap::new(),
            node_id: String::new(),
        }
    }
}

/// A live allocation's resize view: where it runs, its task, and its sizes.
#[derive(Debug, Clone, PartialEq)]
pub struct AllocationSize {
    pub allocation_id: i64,
    pub node_id: String,
    pub status: String,
    pub job_id: String,
    pub instance_id: Option<String>,
    pub task_name: String,
    pub runtime: String,
    /// The task's spec resources with the cluster defaults folded in.
    pub spec: Value,
    /// The row's resources: the effective reservation.
    pub effective: Value,
    /// The stored override record, when a resize left one.
    pub resources_override: Option<Value>,
}

impl Store {
    /// The resize view of one allocation. `None` when no row has the id.
    pub fn allocation_size(&self, allocation_id: i64) -> R<Option<AllocationSize>> {
        let conn = self.read_connection()?;
        #[allow(clippy::type_complexity)]
        let row: Option<(
            String,
            String,
            String,
            i64,
            Value,
            Option<String>,
            Option<String>,
            Option<String>,
        )> = conn
            .prepare_cached(
                "SELECT node_id, status, job_config_id, task_config_id, resources, instance_id, \
                 resources_override, \
                 (SELECT job_id FROM job_config WHERE job_config.id = allocation.job_config_id) \
                 FROM allocation WHERE id = ?1;",
            )?
            .query_row([allocation_id], |row| {
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
            })
            .optional()?;
        drop(conn);
        let Some((
            node_id,
            status,
            job_config_id,
            task_config_id,
            effective,
            instance_id,
            record,
            job_id,
        )) = row
        else {
            return Ok(None);
        };
        let tasks = self.shared_task_configs_for_job_config(&job_config_id)?;
        let Some((_, task)) = tasks.iter().find(|(id, _)| *id == task_config_id) else {
            return Ok(None);
        };
        let spec = self
            .resource_policy
            .normalize_task_resources(&task.resources, false)?;
        Ok(Some(AllocationSize {
            allocation_id,
            node_id,
            status,
            job_id: job_id.unwrap_or_default(),
            instance_id,
            task_name: task.name.clone(),
            runtime: task.runtime.clone(),
            spec,
            effective,
            resources_override: record.and_then(|raw| serde_json::from_str(&raw).ok()),
        }))
    }

    /// The resize overrides on a set of allocations, by id: the allocations
    /// with none are left out.
    pub fn resource_overrides(&self, allocation_ids: &[i64]) -> R<HashMap<i64, String>> {
        let mut found = HashMap::new();
        if allocation_ids.is_empty() {
            return Ok(found);
        }
        let conn = self.read_connection()?;
        if allocation_ids.len() > 8 {
            // A listing: one walk of the partial index, which holds only
            // the resized allocations, instead of a probe per allocation.
            let wanted: HashSet<i64> = allocation_ids.iter().copied().collect();
            let mut statement = conn.prepare_cached(
                "SELECT id, resources_override FROM allocation \
                 WHERE resources_override IS NOT NULL;",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (id, record) = row?;
                if wanted.contains(&id) {
                    found.insert(id, record);
                }
            }
            return Ok(found);
        }
        let mut statement = conn.prepare_cached(
            "SELECT resources_override FROM allocation \
             WHERE id = ?1 AND resources_override IS NOT NULL;",
        )?;
        for id in allocation_ids {
            if let Some(record) = statement
                .query_row([id], |row| row.get::<_, String>(0))
                .optional()?
            {
                found.insert(*id, record);
            }
        }
        Ok(found)
    }

    /// Resize one top-level allocation's row. Runs as a scheduler-lane
    /// write, group-committed with other small scheduler writes, and
    /// forwarded to the lane leader when this manager does not lead it.
    #[instrument(skip_all, fields(allocation_id = request.allocation_id))]
    pub fn resize_allocation(
        &self,
        request: &AllocationResizeRequest,
    ) -> R<AllocationResizeOutcome> {
        match self.resize_allocation_local(request) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::ResizeAllocation {
                        request: request.clone(),
                    },
                    err,
                ),
            other => other,
        }
    }

    /// [`Self::resize_allocation`] without forwarding: what a forwarded
    /// `ResizeAllocation` runs on the lane leader.
    pub(crate) fn resize_allocation_local(
        &self,
        request: &AllocationResizeRequest,
    ) -> R<AllocationResizeOutcome> {
        let request = request.clone();
        self.scheduler_group_commit("resize_allocation", move |s| {
            s.resize_allocation_apply(&request)
        })
    }

    fn resize_allocation_apply(
        &self,
        request: &AllocationResizeRequest,
    ) -> R<AllocationResizeOutcome> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let row: Option<(String, String, String, i64, Value)> = tx
            .prepare_cached(
                "SELECT node_id, status, job_config_id, task_config_id, resources \
                 FROM allocation WHERE id = ?1;",
            )?
            .query_row([request.allocation_id], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .optional()?;
        let Some((node_id, status, job_config_id, task_config_id, resources)) = row else {
            return Ok(AllocationResizeOutcome::answer(
                "gone",
                format!("allocation {} does not exist", request.allocation_id),
                None,
            ));
        };
        if !crate::constants::allocation_status_is_dispatchable(&status) {
            return Ok(AllocationResizeOutcome::answer(
                "gone",
                format!(
                    "allocation {} is {status}, not running",
                    request.allocation_id
                ),
                None,
            ));
        }
        let tasks = self.shared_task_configs_for_job_config(&job_config_id)?;
        let Some((_, task)) = tasks.iter().find(|(id, _)| *id == task_config_id) else {
            return Ok(AllocationResizeOutcome::answer(
                "gone",
                format!(
                    "allocation {}'s task config no longer exists",
                    request.allocation_id
                ),
                None,
            ));
        };
        if crate::structs::session::envelope_serves(task).is_some() {
            return Ok(AllocationResizeOutcome::answer(
                "invalid",
                format!(
                    "allocation {} is a session envelope; it resizes through stateful.envelope",
                    request.allocation_id
                ),
                None,
            ));
        }

        let current = ResourceSize::of(&resources);
        let mut outcome = AllocationResizeOutcome::answer("applied", "", Some(current));
        outcome.node_id = node_id.clone();
        if current == request.to {
            // A retried request whose first answer was lost.
            return Ok(outcome);
        }
        if current != request.from {
            outcome.outcome = "stale".into();
            outcome.message = format!(
                "allocation {} holds {}, not {}",
                request.allocation_id,
                current.to_json(),
                request.from.to_json()
            );
            return Ok(outcome);
        }
        let after = match request.to.apply_to(&resources) {
            Ok(after) => after,
            Err(err) => {
                outcome.outcome = "invalid".into();
                outcome.message = format!("{err}");
                return Ok(outcome);
            }
        };

        if current.grows_to(&request.to) {
            let node = tx
                .prepare_cached(&GET_NODE)?
                .query_row([&node_id], |row| self.node_from_row(row))
                .optional()?;
            let Some(node) = node else {
                return Ok(AllocationResizeOutcome::answer(
                    "gone",
                    format!("node {node_id} does not exist"),
                    None,
                ));
            };
            if node.availability != "available" {
                outcome.outcome = "refused".into();
                outcome.message = format!(
                    "node {node_id} is {} and takes no new room",
                    node.availability
                );
                return Ok(outcome);
            }
            // The node's free room with this allocation's current size given
            // back: what a placement of the new size would see.
            let rows = Self::collect_rows(
                tx.prepare_cached(
                    "SELECT id, resources FROM allocation \
                     WHERE node_id = ?1 AND id != ?2 AND status NOT IN ('complete', 'failed');",
                )?
                .query_map(params![node_id, request.allocation_id], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, Value>(1)?))
                })?,
            )?;
            let pool = ResourcePool::free_from_rows(
                &node,
                rows.iter().map(|(id, resources)| (*id, resources)),
                &self.resource_policy,
            );
            let fits = self
                .resource_policy
                .usage_request(&after)
                .is_ok_and(|request| pool.can_fit(&request));
            if !fits {
                let mut short = Vec::new();
                for (key, held, wanted) in [
                    ("memory", current.memory, request.to.memory),
                    ("cpu", current.cpu, request.to.cpu),
                ] {
                    let (held, wanted) = (held.unwrap_or(0.0), wanted.unwrap_or(0.0));
                    let Some(room) = pool.amount(key) else {
                        continue;
                    };
                    let free = (room - held).max(0.0);
                    outcome.free.insert(key.to_string(), free);
                    if wanted - held > free + 1e-9 {
                        short.push(match key {
                            "memory" => format!(
                                "node {node_id} has {} free; the grow needs {} more",
                                crate::structs::resize::mib(free),
                                crate::structs::resize::mib(wanted - held)
                            ),
                            _ => format!(
                                "node {node_id} has {} MHz of cpu free; the grow needs {} more",
                                crate::structs::resize::json_amount(free),
                                crate::structs::resize::json_amount(wanted - held)
                            ),
                        });
                    }
                }
                outcome.outcome = "refused".into();
                outcome.message = if short.is_empty() {
                    format!("node {node_id} has no free room for the new size")
                } else {
                    short.join("; ")
                };
                return Ok(outcome);
            }
        }

        outcome.size = Some(request.to);
        if request.dry_run {
            return Ok(outcome);
        }
        let spec = self
            .resource_policy
            .normalize_task_resources(&task.resources, false)?;
        let at = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let record = ResourcesOverride::between(&spec, &after, &request.reason, &request.by, &at)
            .map(|record| serde_json::to_string(&record))
            .transpose()?;
        tx.prepare_cached(
            "UPDATE allocation SET resources = ?1, resources_override = ?2 WHERE id = ?3;",
        )?
        .execute(params![after, record, request.allocation_id])?;
        self.bump_job_config_epoch_tx(&tx, &job_config_id)?;
        self.bump_scope_epoch_tx(&tx, "node", &node_id)?;
        tx.commit()?;
        Ok(outcome)
    }
}
