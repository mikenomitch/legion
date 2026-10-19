//! Storage for namespace objects (`legion namespace`, docs/namespaces.md).
//!
//! A namespace row is small: a name, a description and free-form `meta`.
//! Its *contents* are not linked to it by a foreign key — a job's namespace
//! is part of the job's id (`ns/name`, or the bare name in `default`) — so
//! "what is in this namespace" is one query per namespaced kind, all of them
//! in [`Store::namespace_contents`], the single list the emptiness check and
//! `delete --dry-run` both read. Writes are raft-replicated on the control
//! lane with the `job` table, so the delete-if-empty check and the delete
//! are one entry: a submit that races in makes the delete refuse.

use crate::prelude::*;
use crate::storage::errors::StorageError;
use crate::storage::store::Store;
use crate::storage::Lane;
use crate::structs::namespace::{validate_new_namespace_name, Namespace, DEFAULT_NAMESPACE};
use serde::Serialize;
use std::collections::BTreeMap;

use rusqlite::{params, Connection, OptionalExtension, Row};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// One stored namespace plus bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceRecord {
    pub namespace: Namespace,
    pub created_at: String,
    pub updated_at: String,
}

/// What a namespace holds, by kind. Every namespaced kind appears here; a
/// kind added later joins this struct, which is what keeps the emptiness
/// check from forgetting one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceContents {
    /// Canonical job ids.
    pub jobs: Vec<String>,
    /// Volume ids owned by a job in the namespace.
    pub volumes: Vec<String>,
    /// Snapshot ids of a job in the namespace.
    pub snapshots: Vec<String>,
}

impl NamespaceContents {
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty() && self.volumes.is_empty() && self.snapshots.is_empty()
    }

    /// `3 jobs, 1 volume`, for the refusal message.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        for (count, one, many) in [
            (self.jobs.len(), "job", "jobs"),
            (self.volumes.len(), "volume", "volumes"),
            (self.snapshots.len(), "snapshot", "snapshots"),
        ] {
            if count > 0 {
                parts.push(format!("{count} {}", if count == 1 { one } else { many }));
            }
        }
        parts.join(", ")
    }
}

/// What live allocations reserve, in one priority band: the unit a
/// namespace policy's quota is charged in and the
/// `legion_namespace_reserved_*` gauges show. A live allocation is any not
/// yet `complete` or `failed`, the same set the scheduler counts against a
/// node's capacity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Reservation {
    /// CPU in MHz (`resources.cpu`, after `taskResourceDefaults`).
    pub cpu: f64,
    /// Memory in MiB.
    pub memory: f64,
    /// Exclusive cores (`resources.cores`).
    pub cores: f64,
    pub allocations: u64,
}

impl Reservation {
    fn add(&mut self, other: &Reservation) {
        self.cpu += other.cpu;
        self.memory += other.memory;
        self.cores += other.cores;
        self.allocations += other.allocations;
    }
}

/// One namespace's footprint: its registered jobs and its live
/// reservations by band.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceUsage {
    pub jobs: u64,
    /// Keyed by band name (`high`, `normal`, ...); a band with no live
    /// allocation is absent.
    pub bands: BTreeMap<String, Reservation>,
}

impl NamespaceUsage {
    /// Every band summed.
    pub fn total(&self) -> Reservation {
        let mut total = Reservation::default();
        for reservation in self.bands.values() {
            total.add(reservation);
        }
        total
    }

    /// Fold another namespace's usage in (a policy's aggregate).
    pub fn absorb(&mut self, other: &NamespaceUsage) {
        self.jobs += other.jobs;
        for (band, reservation) in &other.bands {
            self.bands.entry(band.clone()).or_default().add(reservation);
        }
    }
}

/// SQL predicate matching a job-id column to one namespace: bare ids for
/// `default`, `ns/…` for the rest. Namespace names cannot hold LIKE
/// wildcards (`validate_namespace_name`), but the prefix test avoids LIKE
/// anyway so the predicate never depends on that.
fn job_id_in_namespace_sql(column: &str) -> String {
    format!(
        "((?1 = '{DEFAULT_NAMESPACE}' AND instr({column}, '/') = 0)
          OR (?1 <> '{DEFAULT_NAMESPACE}' AND substr({column}, 1, length(?1) + 1) = ?1 || '/'))"
    )
}

impl Store {
    fn namespace_from_row(row: &Row<'_>) -> rusqlite::Result<NamespaceRecord> {
        let meta: String = row.get(2)?;
        let meta = serde_json::from_str(&meta).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(err))
        })?;
        Ok(NamespaceRecord {
            namespace: Namespace {
                name: row.get(0)?,
                description: row.get(1)?,
                meta,
            },
            created_at: row.get(3)?,
            updated_at: row.get(4)?,
        })
    }

    /// Every namespace, by name.
    #[instrument(err, skip_all)]
    pub fn list_namespaces(&self) -> R<Vec<NamespaceRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT name, description, meta, created_at, updated_at FROM namespace
             ORDER BY name ASC;",
        )?;
        let rows = stmt.query_map([], Self::namespace_from_row)?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all, fields(namespace = name))]
    pub fn get_namespace(&self, name: &str) -> R<Option<NamespaceRecord>> {
        let conn = self.read_connection()?;
        conn.query_row(
            "SELECT name, description, meta, created_at, updated_at FROM namespace
             WHERE name = ?;",
            [name],
            Self::namespace_from_row,
        )
        .optional()
        .map_err(Into::into)
    }

    /// Whether `name` exists. `default` always does.
    pub fn namespace_exists(&self, name: &str) -> R<bool> {
        if name == DEFAULT_NAMESPACE {
            return Ok(true);
        }
        Ok(self.get_namespace(name)?.is_some())
    }

    /// Create a namespace, or update its description and meta. Returns the
    /// stored record and whether it was created.
    #[instrument(err, skip_all, fields(namespace = %namespace.name))]
    pub fn apply_namespace(&self, namespace: Namespace) -> R<(NamespaceRecord, bool)> {
        self.replicated(Lane::Control, "apply_namespace", move |s| {
            s.apply_namespace_apply(namespace.clone())
        })
    }

    fn apply_namespace_apply(&self, namespace: Namespace) -> R<(NamespaceRecord, bool)> {
        let conn = self.write_connection()?;
        let existed = conn
            .query_row(
                "SELECT 1 FROM namespace WHERE name = ?;",
                [namespace.name.as_str()],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !existed {
            validate_new_namespace_name(&namespace.name).map_err(invalid_input)?;
        }
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let meta = serde_json::to_string(&namespace.meta)?;
        conn.execute(
            "INSERT INTO namespace (name, description, meta, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?4)
             ON CONFLICT(name) DO UPDATE SET
                description = excluded.description,
                meta = excluded.meta,
                updated_at = excluded.updated_at;",
            params![namespace.name, namespace.description, meta, now],
        )?;
        let record = conn.query_row(
            "SELECT name, description, meta, created_at, updated_at FROM namespace
             WHERE name = ?;",
            [namespace.name.as_str()],
            Self::namespace_from_row,
        )?;
        Ok((record, !existed))
    }

    /// Everything in `name`, by kind (see [`NamespaceContents`]).
    #[instrument(err, skip_all, fields(namespace = name))]
    pub fn namespace_contents(&self, name: &str) -> R<NamespaceContents> {
        let conn = self.read_connection()?;
        namespace_contents_on(&conn, name)
    }

    /// Delete `name` if it holds nothing. `default` cannot be deleted. The
    /// emptiness check runs inside the same replicated write as the delete,
    /// so a submit that lands between a cascade's sweep and this call makes
    /// it refuse — never succeed falsely.
    #[instrument(err, skip_all, fields(namespace = name))]
    pub fn delete_namespace_if_empty(&self, name: &str) -> R<()> {
        let name = name.to_owned();
        self.replicated(Lane::Control, "delete_namespace", move |s| {
            s.delete_namespace_apply(&name)
        })
    }

    fn delete_namespace_apply(&self, name: &str) -> R<()> {
        if name == DEFAULT_NAMESPACE {
            return Err(anyhow::Error::new(StorageError::failed_precondition(
                "the default namespace cannot be deleted",
            )));
        }
        let conn = self.write_connection()?;
        let exists = conn
            .query_row(
                "SELECT 1 FROM namespace WHERE name = ?;",
                [name],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            return Err(anyhow::Error::new(StorageError::not_found(
                "namespace",
                name,
            )));
        }
        let contents = namespace_contents_on(&conn, name)?;
        if !contents.is_empty() {
            return Err(anyhow::Error::new(StorageError::failed_precondition(
                format!("namespace '{name}' is not empty: {}", contents.summary()),
            )));
        }
        conn.execute("DELETE FROM namespace WHERE name = ?;", [name])?;
        Ok(())
    }
}

fn namespace_contents_on(conn: &Connection, name: &str) -> R<NamespaceContents> {
    // `live` excludes rows already on their way out (a volume being
    // reclaimed, a snapshot being deleted): a cascade has done its part for
    // those, and the worker report that removes them must not hold the
    // namespace hostage.
    let ids = |table: &str, id_column: &str, job_column: &str, live: &str| -> R<Vec<String>> {
        let sql = format!(
            "SELECT {id_column} FROM {table} WHERE {job_column} IS NOT NULL AND {} {live}
             ORDER BY {id_column};",
            job_id_in_namespace_sql(job_column)
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([name], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    };
    Ok(NamespaceContents {
        jobs: ids("job", "id", "id", "")?,
        volumes: ids(
            "volume",
            "volume_id",
            "job_id",
            "AND state NOT IN ('destroying', 'destroyed')",
        )?,
        snapshots: ids(
            "snapshot",
            "snapshot_id",
            "job_id",
            "AND state <> 'deleting'",
        )?,
    })
}

/// Every namespace's usage, keyed by namespace name. A namespace with no
/// jobs is absent; callers treat absent as zero.
pub(crate) fn namespace_usage_on(
    conn: &Connection,
    policy: &crate::resources::ResourcePolicy,
) -> R<BTreeMap<String, NamespaceUsage>> {
    use crate::structs::namespace::job_namespace;
    let mut usage: BTreeMap<String, NamespaceUsage> = BTreeMap::new();
    let mut jobs = conn.prepare_cached("SELECT id FROM job;")?;
    for id in jobs.query_map([], |row| row.get::<_, String>(0))? {
        usage
            .entry(job_namespace(&id?).to_string())
            .or_default()
            .jobs += 1;
    }
    let mut allocations = conn.prepare_cached(&format!(
        "SELECT tc.job_id, jc.priority, a.resources FROM allocation a
         INNER JOIN task_config tc ON tc.id = a.task_config_id
         INNER JOIN job_config jc ON jc.id = a.job_config_id
         WHERE a.status NOT IN ('{}', '{}');",
        crate::constants::ALLOCATION_STATUS_COMPLETE,
        crate::constants::ALLOCATION_STATUS_FAILED,
    ))?;
    let rows = allocations.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, serde_json::Value>(2)?,
        ))
    })?;
    for row in rows {
        let (job_id, band, resources) = row?;
        // A row nothing can parse reserves nothing here; the scheduler's
        // own walk is what warns about it.
        let Ok(request) = policy
            .normalize_task_resources(&resources, false)
            .and_then(|normalized| crate::resources::ResourceRequest::from_value(&normalized))
        else {
            continue;
        };
        let band = crate::structs::Band::from_stored(&band)
            .as_str()
            .to_string();
        usage
            .entry(job_namespace(&job_id).to_string())
            .or_default()
            .bands
            .entry(band)
            .or_default()
            .add(&Reservation {
                cpu: request.amount("cpu").unwrap_or(0.0),
                memory: request.amount("memory").unwrap_or(0.0),
                cores: match request.values.get("cores") {
                    Some(crate::resources::ResourceValue::Amount(cores)) => *cores,
                    Some(crate::resources::ResourceValue::Items(cores)) => cores.len() as f64,
                    None => 0.0,
                },
                allocations: 1,
            });
    }
    Ok(usage)
}

impl Store {
    /// Every namespace's jobs and live reservations by band
    /// ([`NamespaceUsage`]).
    #[instrument(err, skip_all)]
    pub fn namespace_usage(&self) -> R<BTreeMap<String, NamespaceUsage>> {
        let conn = self.read_connection()?;
        namespace_usage_on(&conn, &self.resource_policy)
    }
}

fn invalid_input(message: String) -> anyhow::Error {
    anyhow::Error::new(StorageError::InvalidInput { message })
}

#[cfg(test)]
mod namespace_store_test {
    use super::*;
    use crate::structs::{JobConfig, ManagerConfigFull};
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-namespace-store-")
            .tempdir()
            .context("Error creating temp store directory")?
            .keep();
        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("Error parsing store path")?
                .to_string(),
            ..Default::default()
        };
        Store::start_storage(&store_config)
    }

    fn job(name: &str) -> JobConfig {
        JobConfig {
            name: name.to_string(),
            tasks: vec![crate::structs::TaskConfig {
                config: serde_json::json!({}),
                ..crate::structs::TaskConfig::new("main", "noop")
            }],
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
        }
    }

    #[test]
    fn usage_counts_jobs_and_charges_live_allocations_by_band() -> R<()> {
        let store = setup_store()?;
        store.insert_node(crate::structs::NodeMsg {
            id: "node-a".into(),
            pool: "default".into(),
            status: "ready".into(),
            availability: "available".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: "{}".into(),
            dynamic_attrs: "{}".into(),
            health_checks: "{}".into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
            heartbeat_source: String::new(),
        })?;
        store.apply_namespace(Namespace {
            name: "prod".into(),
            ..Default::default()
        })?;
        let mut web = job("prod/web");
        web.tasks[0].resources = serde_json::json!({ "cpu": 500, "memory": 256 });
        web.priority = crate::structs::Band::High;
        store.handle_job_submit(&web, &[])?;
        store.handle_job_submit(&job("prod/batch"), &[])?;
        store.handle_job_submit(&job("web"), &[])?;
        let task_id: i64 = store.read_connection()?.query_row(
            "SELECT id FROM task_config WHERE job_id = 'prod/web';",
            [],
            |row| row.get(0),
        )?;
        store.apply_allocation_plan(&[
            ("node-a".to_string(), task_id),
            ("node-a".to_string(), task_id),
        ])?;

        let usage = store.namespace_usage()?;
        let prod = &usage["prod"];
        assert_eq!(prod.jobs, 2);
        let high = prod.bands["high"];
        assert_eq!(
            (high.cpu, high.memory, high.allocations),
            (1000.0, 512.0, 2)
        );
        assert!(
            !prod.bands.contains_key("normal"),
            "no live normal allocations"
        );
        assert_eq!(prod.total().allocations, 2);
        assert_eq!(usage["default"].jobs, 1);
        assert!(usage["default"].bands.is_empty());
        Ok(())
    }

    #[test]
    fn default_exists_and_cannot_be_deleted() -> R<()> {
        let store = setup_store()?;
        assert!(store.namespace_exists("default")?);
        assert!(store.get_namespace("default")?.is_some());
        let err = store.delete_namespace_if_empty("default").unwrap_err();
        assert!(format!("{err:#}").contains("cannot be deleted"), "{err:#}");
        Ok(())
    }

    #[test]
    fn apply_creates_then_updates() -> R<()> {
        let store = setup_store()?;
        let (created, was_created) = store.apply_namespace(Namespace {
            name: "payments-prod".into(),
            description: "Payments".into(),
            meta: [("owner".to_string(), "payments".to_string())].into(),
        })?;
        assert!(was_created);
        assert_eq!(created.namespace.meta["owner"], "payments");
        let (updated, was_created) = store.apply_namespace(Namespace {
            name: "payments-prod".into(),
            description: "Payments, production".into(),
            meta: Default::default(),
        })?;
        assert!(!was_created);
        assert_eq!(updated.namespace.description, "Payments, production");
        assert_eq!(updated.created_at, created.created_at);
        let names: Vec<_> = store
            .list_namespaces()?
            .into_iter()
            .map(|record| record.namespace.name)
            .collect();
        assert_eq!(names, vec!["default", "payments-prod"]);
        Ok(())
    }

    #[test]
    fn creation_rejects_bad_and_reserved_names() -> R<()> {
        let store = setup_store()?;
        for name in ["Bad", "legion-system", "a/b", ""] {
            assert!(
                store
                    .apply_namespace(Namespace {
                        name: name.into(),
                        ..Default::default()
                    })
                    .is_err(),
                "{name}"
            );
        }
        Ok(())
    }

    #[test]
    fn delete_refuses_while_jobs_remain_and_counts_only_its_own() -> R<()> {
        let store = setup_store()?;
        store.apply_namespace(Namespace {
            name: "prod".into(),
            ..Default::default()
        })?;
        store.apply_namespace(Namespace {
            name: "prod2".into(),
            ..Default::default()
        })?;
        store.handle_job_submit(&job("prod/web"), &[])?;
        store.handle_job_submit(&job("prod2/web"), &[])?;
        store.handle_job_submit(&job("web"), &[])?;

        let prod = store.namespace_contents("prod")?;
        assert_eq!(prod.jobs, vec!["prod/web"]);
        let default = store.namespace_contents("default")?;
        assert_eq!(default.jobs, vec!["web"]);

        let err = store.delete_namespace_if_empty("prod").unwrap_err();
        assert!(
            format!("{err:#}").contains("namespace 'prod' is not empty: 1 job"),
            "{err:#}"
        );
        store.stop_job("prod/web")?;
        store.destroy_job("prod/web")?;
        store.delete_namespace_if_empty("prod")?;
        assert!(!store.namespace_exists("prod")?);
        // The sibling whose name shares a prefix is untouched.
        assert_eq!(store.namespace_contents("prod2")?.jobs, vec!["prod2/web"]);
        Ok(())
    }
}
