//! Size overrides that outlive one allocation (docs/resize.md; resize plan
//! §6.5 and §6.6).
//!
//! Two records sit beside an allocation's own `resources_override`:
//!
//! - **A task-level override** (`task_resize`, `legion job resize`): one row
//!   per task, beside the count override in `task_scale` and control-owned
//!   like it, so a submit arbitrates both in the capture that writes the
//!   config.
//! - **A unit-level size** (`instance.resources_override`): the size a
//!   resize gave one keyed or slot instance, kept with the instance so it
//!   survives a suspend and resume, a restart, and a move.
//!
//! Both record the task's declared size when they were written and bind
//! only while the spec still declares it: a submit that changes the task's
//! `resources` is the author's newer intent, and the spec wins. Every new
//! allocation is sized at one choke point, [`Store::placement_size_tx`],
//! called from the insert every placement path goes through: the unit's
//! size over the task's override over the spec.

use super::*;
use crate::structs::resize::{ResourceSize, ResourcesOverride};

/// A task-level size override.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskResize {
    pub job_id: String,
    pub task_group: String,
    pub task_name: String,
    /// The overridden amounts; unset amounts follow the spec.
    pub size: ResourceSize,
    /// The task's declared amounts when the override was written.
    pub base: ResourceSize,
    pub reason: String,
    /// Who set it: an accessor id or principal.
    pub by: String,
    /// RFC 3339.
    pub at: String,
}

impl TaskResize {
    /// Whether the override still applies to a task that declares
    /// `resources`: only to the spec it was laid over.
    pub fn binds_to(&self, declared: ResourceSize) -> bool {
        self.base == declared
    }

    /// The override as the record an allocation row carries.
    pub fn record(&self) -> ResourcesOverride {
        ResourcesOverride {
            size: self.size,
            spec: self.base,
            reason: self.reason.clone(),
            by: self.by.clone(),
            at: self.at.clone(),
        }
    }
}

/// A placement whose unit carries a size its node has no room for: the node
/// was chosen for the task's size, and the unit's own is larger. One more
/// "the world moved" class for plan apply, which tries the next candidate.
#[derive(Debug)]
pub struct PlacementSizeUnavailable(pub String);

impl std::fmt::Display for PlacementSizeUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PlacementSizeUnavailable {}

/// `over` laid over `under`: every amount `over` names wins.
fn layered(under: ResourceSize, over: ResourceSize) -> ResourceSize {
    ResourceSize {
        memory: over.memory.or(under.memory),
        memory_max: over.memory_max.or(under.memory_max),
        cpu: over.cpu.or(under.cpu),
    }
}

impl Store {
    /// A task's declared size as overrides compare it: the resizable
    /// amounts of its `resources` with the cluster defaults folded in, so
    /// a raw config row and a normalized task config read the same.
    pub fn declared_size(&self, declared: &Value) -> ResourceSize {
        ResourceSize::of(
            &self
                .resource_policy
                .normalize_task_resources(declared, false)
                .unwrap_or_else(|_| declared.clone()),
        )
    }

    fn task_resize_on(
        conn: &Connection,
        job_id: &str,
        task_group: &str,
        task_name: &str,
    ) -> R<Option<TaskResize>> {
        let row: Option<(String, String, String, String, String)> = conn
            .prepare_cached(
                "SELECT size, base, reason, principal, updated_at FROM task_resize \
                 WHERE job_id = ?1 AND task_group = ?2 AND task_name = ?3;",
            )?
            .query_row(params![job_id, task_group, task_name], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .optional()?;
        Ok(row.map(|(size, base, reason, by, at)| TaskResize {
            job_id: job_id.to_string(),
            task_group: task_group.to_string(),
            task_name: task_name.to_string(),
            size: serde_json::from_str(&size).unwrap_or_default(),
            base: serde_json::from_str(&base).unwrap_or_default(),
            reason,
            by,
            at,
        }))
    }

    /// The task-level size override on one task, bound or not.
    pub fn task_resize_for(
        &self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
    ) -> R<Option<TaskResize>> {
        let conn = self.read_connection()?;
        Self::task_resize_on(&conn, job_id, task_group, task_name)
    }

    /// Set (or replace) a task's size override. `base` is the task's
    /// declared size now: the override binds only while the spec still
    /// declares it.
    #[instrument(err, skip(self, reason, by))]
    pub fn set_task_resize(
        &self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
        size: ResourceSize,
        base: ResourceSize,
        reason: &str,
        by: &str,
    ) -> R<()> {
        let (job_id, task_group, task_name) = (
            job_id.to_owned(),
            task_group.to_owned(),
            task_name.to_owned(),
        );
        let (reason, by) = (reason.to_owned(), by.to_owned());
        self.replicated(Lane::Control, "set_task_resize", move |s| {
            let conn = s.write_connection()?;
            let now = Self::now_rfc3339()?;
            conn.execute(
                "INSERT INTO task_resize \
                 (job_id, task_group, task_name, size, base, reason, principal, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
                 ON CONFLICT(job_id, task_group, task_name) DO UPDATE SET \
                 size = ?4, base = ?5, reason = ?6, principal = ?7, updated_at = ?8;",
                params![
                    job_id,
                    task_group,
                    task_name,
                    serde_json::to_string(&size)?,
                    serde_json::to_string(&base)?,
                    reason,
                    by,
                    now
                ],
            )?;
            Ok(())
        })
    }

    /// Remove a task's size override. Returns whether one existed.
    #[instrument(err, skip(self))]
    pub fn clear_task_resize(&self, job_id: &str, task_group: &str, task_name: &str) -> R<bool> {
        let (job_id, task_group, task_name) = (
            job_id.to_owned(),
            task_group.to_owned(),
            task_name.to_owned(),
        );
        self.replicated(Lane::Control, "clear_task_resize", move |s| {
            let conn = s.write_connection()?;
            let removed = conn.execute(
                "DELETE FROM task_resize \
                 WHERE job_id = ?1 AND task_group = ?2 AND task_name = ?3;",
                params![job_id, task_group, task_name],
            )?;
            Ok(removed > 0)
        })
    }

    /// Submit arbitration for size overrides, in the submit's own
    /// transaction (resize plan §6.5): an override laid over a size the new
    /// spec no longer declares is dropped, and so is one for a task the new
    /// version does not have. Returns the (group, task) pairs dropped
    /// because the spec changed.
    pub(crate) fn arbitrate_task_resizes_tx(
        &self,
        tx: &Connection,
        job_id: &str,
        job_config_id: &str,
        tasks: &[TaskConfig],
    ) -> R<Vec<(String, String)>> {
        let mut dropped = Vec::new();
        for task in tasks {
            let Some(existing) = Self::task_resize_on(tx, job_id, &task.group, &task.name)? else {
                continue;
            };
            if !existing.binds_to(self.declared_size(&task.resources)) {
                tx.execute(
                    "DELETE FROM task_resize \
                     WHERE job_id = ?1 AND task_group = ?2 AND task_name = ?3;",
                    params![job_id, task.group, task.name],
                )?;
                dropped.push((task.group.clone(), task.name.clone()));
            }
        }
        tx.execute(
            "DELETE FROM task_resize WHERE job_id = ?1 AND NOT EXISTS (\
                 SELECT 1 FROM task_config \
                 WHERE task_config.job_config_id = ?2 \
                 AND task_config.group_name = task_resize.task_group \
                 AND task_config.name = task_resize.task_name);",
            params![job_id, job_config_id],
        )?;
        Ok(dropped)
    }

    /// The size a resize left on one instance, when it has one.
    pub fn instance_size(&self, instance_id: &str) -> R<Option<ResourcesOverride>> {
        let conn = self.read_connection()?;
        let raw: Option<Option<String>> = conn
            .prepare_cached("SELECT resources_override FROM instance WHERE instance_id = ?1;")?
            .query_row([instance_id], |row| row.get(0))
            .optional()?;
        Ok(raw
            .flatten()
            .and_then(|raw| serde_json::from_str(&raw).ok()))
    }

    /// Record (or, with `None`, clear) the size an instance carries across
    /// its allocations.
    #[instrument(err, skip(self, record))]
    pub fn set_instance_size(&self, instance_id: &str, record: Option<ResourcesOverride>) -> R<()> {
        let instance_id = instance_id.to_owned();
        let record = record.map(|r| serde_json::to_string(&r)).transpose()?;
        self.replicated(Lane::Scheduler, "set_instance_size", move |s| {
            let conn = s.write_connection()?;
            conn.execute(
                "UPDATE instance SET resources_override = ?1 WHERE instance_id = ?2;",
                params![record, instance_id],
            )?;
            Ok(())
        })
    }

    /// A task's resources as the scheduler should pack them: the bound
    /// task-level override laid over `declared`, and for a keyed task the
    /// largest size any instance still waiting for an allocation carries,
    /// so the node chosen has room for whichever instance binds to it.
    pub fn effective_task_resources(
        &self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
        declared: &Value,
        keyed: bool,
    ) -> R<Value> {
        let conn = self.read_connection()?;
        let base = self.declared_size(declared);
        let mut size = Self::task_resize_on(&conn, job_id, task_group, task_name)?
            .filter(|resize| resize.binds_to(base))
            .map(|resize| resize.size)
            .unwrap_or_default();
        if keyed {
            let mut statement = conn.prepare_cached(
                "SELECT resources_override FROM instance \
                 WHERE resources_override IS NOT NULL AND job_id = ?1 AND group_name = ?2 \
                 AND task_name = ?3 AND current_alloc_id IS NULL AND state != 'terminated';",
            )?;
            let rows = statement.query_map(params![job_id, task_group, task_name], |row| {
                row.get::<_, String>(0)
            })?;
            let larger = |a: Option<f64>, b: Option<f64>| match (a, b) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            };
            for raw in rows {
                let Ok(record) = serde_json::from_str::<ResourcesOverride>(&raw?) else {
                    continue;
                };
                if record.spec != base {
                    continue;
                }
                size = ResourceSize {
                    memory: larger(size.memory, record.size.memory),
                    memory_max: larger(size.memory_max, record.size.memory_max),
                    cpu: larger(size.cpu, record.size.cpu),
                };
            }
        }
        if size.is_empty() {
            return Ok(declared.clone());
        }
        // An override the spec's other amounts make invalid (a ceiling under
        // the reservation) is not applied: the spec is what places.
        Ok(size.apply_to(declared).unwrap_or_else(|_| declared.clone()))
    }

    /// A task's declared resources with its bound task-level override laid
    /// over them, on a transaction the caller holds (plan apply's capacity
    /// recheck packs what the row will hold).
    pub(crate) fn task_resources_with_override_tx(
        &self,
        tx: &Connection,
        job_id: &str,
        task_group: &str,
        task_name: &str,
        declared: Value,
    ) -> R<Value> {
        let Some(resize) = Self::task_resize_on(tx, job_id, task_group, task_name)?
            .filter(|resize| resize.binds_to(self.declared_size(&declared)))
        else {
            return Ok(declared);
        };
        Ok(resize.size.apply_to(&declared).unwrap_or(declared))
    }

    /// The resources a new allocation of `task_id` is written with, and the
    /// override record that goes on its row: the bound unit-level size of
    /// `instance_id` over the bound task-level override over `resources`.
    /// `(resources, None)` when neither applies.
    pub(crate) fn placement_size_tx(
        &self,
        tx: &Connection,
        task_id: i64,
        instance_id: Option<&str>,
        resources: &Value,
    ) -> R<(Value, Option<String>)> {
        let (job_id, group, name, declared): (String, String, String, Value) = tx
            .prepare_cached(
                "SELECT job_id, group_name, name, resources FROM task_config WHERE id = ?1;",
            )?
            .query_row([task_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?;
        let base = self.declared_size(&declared);
        let mut size = ResourceSize::default();
        let mut record: Option<ResourcesOverride> = None;
        if let Some(resize) =
            Self::task_resize_on(tx, &job_id, &group, &name)?.filter(|resize| resize.binds_to(base))
        {
            size = resize.size;
            record = Some(resize.record());
        }
        if let Some(instance_id) = instance_id {
            let raw: Option<Option<String>> = tx
                .prepare_cached("SELECT resources_override FROM instance WHERE instance_id = ?1;")?
                .query_row([instance_id], |row| row.get(0))
                .optional()?;
            if let Some(unit) = raw
                .flatten()
                .and_then(|raw| serde_json::from_str::<ResourcesOverride>(&raw).ok())
                .filter(|unit| unit.spec == base)
            {
                size = layered(size, unit.size);
                record = Some(ResourcesOverride { size, ..unit });
            }
        }
        if size.is_empty() {
            return Ok((resources.clone(), None));
        }
        match size.apply_to(resources) {
            Ok(after) => Ok((
                after,
                record.map(|r| serde_json::to_string(&r)).transpose()?,
            )),
            Err(_) => Ok((resources.clone(), None)),
        }
    }

    /// Whether `node_id` has room for `after` beyond what its live
    /// allocations hold, inside a placement's transaction.
    pub(crate) fn node_fits_tx(&self, tx: &Connection, node_id: &str, after: &Value) -> R<bool> {
        let Some(node) = tx
            .prepare_cached(&GET_NODE)?
            .query_row([node_id], |row| self.node_from_row(row))
            .optional()?
        else {
            return Ok(false);
        };
        let rows = Self::collect_rows(
            tx.prepare_cached(
                "SELECT id, resources FROM allocation \
                 WHERE node_id = ?1 AND status NOT IN ('complete', 'failed');",
            )?
            .query_map([node_id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Value>(1)?))
            })?,
        )?;
        let pool = ResourcePool::free_from_rows(
            &node,
            rows.iter().map(|(id, resources)| (*id, resources)),
            &self.resource_policy,
        );
        Ok(self
            .resource_policy
            .usage_request(after)
            .is_ok_and(|request| pool.can_fit(&request)))
    }

    /// Stop allocations a task-level resize could not change in place, so
    /// the scheduler places their replacements at the new size
    /// (`legion job resize --replace`). One scheduler-lane entry; the
    /// caller enqueues the eval that re-places them.
    #[instrument(err, skip_all, fields(count = allocation_ids.len()))]
    pub fn replace_allocations_for_resize(&self, allocation_ids: &[i64]) -> R<Vec<Allocation>> {
        let allocation_ids = allocation_ids.to_vec();
        self.replicated(
            Lane::Scheduler,
            "replace_allocations_for_resize",
            move |s| {
                let mut removed = Vec::new();
                for id in &allocation_ids {
                    if let Some(allocation) = s.get_allocation_by_id(*id)? {
                        removed.push(allocation);
                    }
                }
                if removed.is_empty() {
                    return Ok(removed);
                }
                let mut conn = s.write_connection()?;
                let tx = Self::write_tx(&mut conn)?;
                for allocation in &removed {
                    tx.execute("DELETE FROM allocation WHERE id = ?;", [allocation.id])?;
                }
                let stopping_event =
                    s.record_stop_notices_tx(&tx, &removed, StopCause::Reschedule)?;
                s.release_instance_bindings_tx(&tx, &removed)?;
                s.bump_epochs_for_allocations_tx(&tx, &removed)?;
                tx.commit()?;
                s.publish_staged(stopping_event);
                drop(conn);
                s.cache_remove_allocations(&removed)?;
                let mut by_config: HashMap<String, Vec<Allocation>> = HashMap::new();
                for allocation in &removed {
                    by_config
                        .entry(allocation.job_config_id.clone())
                        .or_default()
                        .push(allocation.clone());
                }
                for (job_config_id, allocations) in by_config {
                    s.events()
                        .allocations_removed_for_job_config(&job_config_id, &allocations);
                }
                Ok(removed)
            },
        )
    }

    /// The live allocations of one task of a job, across its job configs,
    /// in id order: what a task-level resize brings to the new size.
    pub fn live_allocations_of_task(
        &self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
    ) -> R<Vec<i64>> {
        let conn = self.read_connection()?;
        let mut statement = conn.prepare_cached(
            "SELECT allocation.id FROM allocation \
             INNER JOIN task_config ON task_config.id = allocation.task_config_id \
             WHERE task_config.job_id = ?1 AND task_config.group_name = ?2 \
             AND task_config.name = ?3 AND allocation.status NOT IN ('complete', 'failed') \
             ORDER BY allocation.id;",
        )?;
        let rows = statement.query_map(params![job_id, task_group, task_name], |row| row.get(0))?;
        Self::collect_rows(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_override_binds_only_to_the_spec_it_was_laid_over_test() {
        let resize = TaskResize {
            job_id: "web".into(),
            task_group: "default".into(),
            task_name: "api".into(),
            size: ResourceSize {
                memory: Some(1024.0),
                ..Default::default()
            },
            base: ResourceSize {
                memory: Some(256.0),
                cpu: Some(500.0),
                ..Default::default()
            },
            reason: String::new(),
            by: String::new(),
            at: String::new(),
        };
        assert!(resize.binds_to(ResourceSize::of(
            &json!({"memory": 256, "cpu": 500, "disk": 10})
        )));
        assert!(!resize.binds_to(ResourceSize::of(&json!({"memory": 512, "cpu": 500}))));
        assert_eq!(resize.record().spec, resize.base);
    }

    #[test]
    fn a_unit_size_wins_over_a_task_override_amount_by_amount_test() {
        let task = ResourceSize {
            memory: Some(1024.0),
            cpu: Some(2000.0),
            ..Default::default()
        };
        let unit = ResourceSize {
            memory: Some(4096.0),
            ..Default::default()
        };
        let both = layered(task, unit);
        assert_eq!(both.memory, Some(4096.0));
        assert_eq!(both.cpu, Some(2000.0));
    }
}
