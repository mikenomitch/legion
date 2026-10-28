//! Store reads behind scaling inputs (docs/autoscaling.md, "Scaling
//! inputs"; `crate::manager::scaling_inputs`).
//!
//! All reads, all cheap: the rollup loop looks an allocation up once in its
//! life (its task and reservation never change), and `GetScalingInputs`
//! reads one job's placed replicas per job it answers for.

use super::*;
use crate::manager::scaling_inputs::TargetKey;

/// Ids per `IN (...)` lookup, well inside SQLite's variable limit.
const SCALING_LOOKUP_CHUNK: usize = 500;

/// One allocation's facts for a rollup: its task, its stored resources, and
/// its task's `connect` block (JSON) for edge attribution.
#[derive(Debug, Clone)]
pub struct ScalingAllocRow {
    pub allocation_id: i64,
    pub job_id: String,
    pub task_group: String,
    pub task_name: String,
    pub resources: Value,
    pub connect: Option<String>,
}

/// One placed allocation of a job, for replica counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalingReplicaRow {
    pub allocation_id: i64,
    pub task_group: String,
    pub task_name: String,
    pub healthy: bool,
}

impl Store {
    /// The rollup facts for `allocation_ids`. Ids with no allocation row
    /// (already removed) are absent.
    pub fn scaling_alloc_rows(&self, allocation_ids: &[i64]) -> R<Vec<ScalingAllocRow>> {
        let mut rows = Vec::new();
        if allocation_ids.is_empty() {
            return Ok(rows);
        }
        let conn = self.read_connection()?;
        for chunk in allocation_ids.chunks(SCALING_LOOKUP_CHUNK) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(",");
            let mut stmt = conn.prepare(&format!(
                "SELECT a.id, tc.job_id, tc.group_name, tc.name, a.resources, tc.connect
                 FROM allocation a
                 INNER JOIN task_config tc ON tc.id = a.task_config_id
                 WHERE a.id IN ({placeholders});"
            ))?;
            let found = stmt.query_map(params_from_iter(chunk.iter()), |row| {
                Ok(ScalingAllocRow {
                    allocation_id: row.get(0)?,
                    job_id: row.get(1)?,
                    task_group: row.get(2)?,
                    task_name: row.get(3)?,
                    resources: row.get(4)?,
                    connect: row.get(5)?,
                })
            })?;
            rows.extend(Self::collect_rows(found)?);
        }
        Ok(rows)
    }

    /// Every service a task in a job's latest config declares, as its
    /// canonical reference (`namespace/name`, or the bare name in
    /// `default`), with the tasks that back it.
    pub fn scaling_service_targets(&self) -> R<HashMap<String, Vec<TargetKey>>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT tc.job_id, tc.group_name, tc.name, tc.service
             FROM task_config tc
             INNER JOIN job_config jc ON jc.id = tc.job_config_id
             INNER JOIN (
                 SELECT job_id, MAX(version) AS version FROM job_config GROUP BY job_id
             ) latest ON latest.job_id = jc.job_id AND latest.version = jc.version
             WHERE tc.service IS NOT NULL;",
        )?;
        let rows = Self::collect_rows(stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?)?;
        let mut services: HashMap<String, Vec<TargetKey>> = HashMap::new();
        for (job_id, group, name, raw) in rows {
            let Ok(service) = from_str::<crate::structs::ServiceConfig>(&raw) else {
                // A row we wrote ourselves that does not parse is skipped
                // rather than failing every other service's attribution.
                continue;
            };
            let reference = crate::structs::namespace::job_id(
                crate::structs::namespace::job_namespace(&job_id),
                service.service_name(&name),
            );
            services
                .entry(reference)
                .or_default()
                .push(TargetKey::new(&job_id, &group, &name));
        }
        Ok(services)
    }

    /// Every placed allocation of `job_id`, across its versions, with its
    /// task and whether its latest health report says healthy.
    pub fn scaling_replica_rows(&self, job_id: &str) -> R<Vec<ScalingReplicaRow>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached(
            "SELECT a.id, tc.group_name, tc.name, COALESCE(ah.healthy, 0)
             FROM allocation a
             INNER JOIN task_config tc ON tc.id = a.task_config_id
             LEFT JOIN allocation_health ah ON ah.allocation_id = a.id
             WHERE tc.job_id = ?1;",
        )?;
        let rows = stmt.query_map([job_id], |row| {
            Ok(ScalingReplicaRow {
                allocation_id: row.get(0)?,
                task_group: row.get(1)?,
                task_name: row.get(2)?,
                healthy: row.get::<_, i64>(3)? != 0,
            })
        })?;
        Self::collect_rows(rows)
    }

    /// The stored lifecycle flag of a job (`active`, `stopped`, `failed`),
    /// not the derived status `GetJobInfo` reports. `None` for no such job.
    pub fn job_lifecycle_flag(&self, job_id: &str) -> R<Option<String>> {
        let conn = self.read_connection()?;
        Ok(conn
            .query_row("SELECT status FROM job WHERE id = ?1;", [job_id], |row| {
                row.get::<_, String>(0)
            })
            .optional()?)
    }
}
