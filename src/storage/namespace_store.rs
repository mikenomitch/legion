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
use crate::structs::namespace::{
    is_reserved_namespace, validate_new_namespace_name, Namespace, DEFAULT_NAMESPACE,
    SYSTEM_NAMESPACE,
};
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
    pub(crate) fn add(&mut self, other: &Reservation) {
        self.cpu += other.cpu;
        self.memory += other.memory;
        self.cores += other.cores;
        self.allocations += other.allocations;
    }

    /// `count` of these.
    pub(crate) fn times(&self, count: u32) -> Reservation {
        let n = f64::from(count);
        Reservation {
            cpu: self.cpu * n,
            memory: self.memory * n,
            cores: self.cores * n,
            allocations: self.allocations * u64::from(count),
        }
    }

    /// Dimension by dimension, the larger of the two.
    pub(crate) fn max_with(&self, other: &Reservation) -> Reservation {
        Reservation {
            cpu: self.cpu.max(other.cpu),
            memory: self.memory.max(other.memory),
            cores: self.cores.max(other.cores),
            allocations: self.allocations.max(other.allocations),
        }
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
            crate::storage::namespace_policy_store::enforce_namespace_count_on(
                &conn,
                &namespace.name,
            )?;
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

    /// Create the reserved `legion-system` namespace, and the namespace
    /// policy of the same name that sends its jobs to the managers' system
    /// workers (`pools.default = "legion-system"`), when the namespace does
    /// not exist yet. The control-lane leader runs this at startup and on
    /// taking leadership; the namespace cannot be deleted, so after the first
    /// leader of an empty store has run it, it changes nothing. Returns
    /// whether it created them.
    ///
    /// It bypasses the reserved-prefix check that stops `legion namespace
    /// apply` from creating a `legion-*` namespace. The policy is an
    /// ordinary one: an operator may edit or delete it, and it is only
    /// written together with the namespace, so a deleted policy stays
    /// deleted.
    #[instrument(err, skip_all)]
    pub fn ensure_system_namespace(&self) -> R<bool> {
        // Read first: every later leadership change then costs no write.
        if self.get_namespace(SYSTEM_NAMESPACE)?.is_some() {
            return Ok(false);
        }
        self.replicated(Lane::Control, "ensure_system_namespace", |s| {
            s.ensure_system_namespace_apply()
        })
    }

    fn ensure_system_namespace_apply(&self) -> R<bool> {
        let conn = self.write_connection()?;
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let created = conn.execute(
            "INSERT INTO namespace (name, description, meta, created_at, updated_at)
             VALUES (?1, ?2, '{}', ?3, ?3)
             ON CONFLICT(name) DO NOTHING;",
            params![
                SYSTEM_NAMESPACE,
                "Legion's own operational jobs, run on the managers' system workers",
                now
            ],
        )? > 0;
        if !created {
            return Ok(false);
        }
        let mut policy = crate::structs::namespace_policy::NamespacePolicy {
            name: SYSTEM_NAMESPACE.to_string(),
            description: "Jobs in legion-system that name no pool run on the managers' system \
                          workers, or on a --run-worker manager's worker"
                .to_string(),
            namespaces: vec![SYSTEM_NAMESPACE.to_string()],
            // Both pools: the system workers' (`legion-system`) and the one a
            // `--run-worker` manager's worker joins (`default`). The pool is
            // not what keeps legion-system off ordinary workers; their
            // `namespaces` is (a node with the default `["*"]` refuses it).
            pools: Some(crate::structs::namespace_policy::PoolRule {
                allowed: None,
                default: Some(crate::structs::namespace_policy::PoolDefault::Many(vec![
                    SYSTEM_NAMESPACE.to_string(),
                    "default".to_string(),
                ])),
            }),
            ..Default::default()
        };
        policy
            .normalize()
            .map_err(|message| anyhow::Error::new(StorageError::InvalidInput { message }))?;
        conn.execute(
            "INSERT INTO namespace_policy (name, object, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?3)
             ON CONFLICT(name) DO NOTHING;",
            params![policy.name, serde_json::to_string(&policy)?, now],
        )?;
        Ok(true)
    }

    fn delete_namespace_apply(&self, name: &str) -> R<()> {
        if name == DEFAULT_NAMESPACE {
            return Err(anyhow::Error::new(StorageError::failed_precondition(
                "the default namespace cannot be deleted",
            )));
        }
        if is_reserved_namespace(name) {
            return Err(anyhow::Error::new(StorageError::failed_precondition(
                format!("namespace '{name}' is reserved for Legion and cannot be deleted"),
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

/// Live reservations per `(job id, band)`: allocations not yet `complete` or
/// `failed`, summed in SQL. An allocation's `resources` is its task's,
/// normalized at submit (`cpu`, `memory`, defaults applied), so the sums need
/// no per-row parsing; `cores` may be a count or a list of core ids.
pub(crate) fn live_reservations_on(
    conn: &Connection,
) -> R<BTreeMap<(String, String), Reservation>> {
    let mut stmt = conn.prepare_cached(&format!(
        "SELECT tc.job_id, jc.priority, COUNT(*),
                COALESCE(SUM(CAST(json_extract(a.resources, '$.cpu') AS REAL)), 0),
                COALESCE(SUM(CAST(json_extract(a.resources, '$.memory') AS REAL)), 0),
                COALESCE(SUM(CASE json_type(a.resources, '$.cores')
                    WHEN 'array' THEN json_array_length(a.resources, '$.cores')
                    WHEN 'integer' THEN json_extract(a.resources, '$.cores')
                    WHEN 'real' THEN json_extract(a.resources, '$.cores')
                    ELSE 0 END), 0)
         FROM allocation a
         INNER JOIN task_config tc ON tc.id = a.task_config_id
         INNER JOIN job_config jc ON jc.id = a.job_config_id
         WHERE a.status NOT IN ('{}', '{}') AND json_valid(a.resources)
         GROUP BY tc.job_id, jc.priority;",
        crate::constants::ALLOCATION_STATUS_COMPLETE,
        crate::constants::ALLOCATION_STATUS_FAILED,
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            Reservation {
                allocations: row.get::<_, i64>(2)?.max(0) as u64,
                cpu: row.get(3)?,
                memory: row.get(4)?,
                cores: row.get(5)?,
            },
        ))
    })?;
    let mut out: BTreeMap<(String, String), Reservation> = BTreeMap::new();
    for row in rows {
        let (job, band, reservation) = row?;
        let band = crate::structs::Band::from_stored(&band)
            .as_str()
            .to_string();
        out.entry((job, band)).or_default().add(&reservation);
    }
    Ok(out)
}

/// Every namespace's usage, keyed by namespace name. A namespace with no
/// jobs is absent; callers treat absent as zero.
pub(crate) fn namespace_usage_on(conn: &Connection) -> R<BTreeMap<String, NamespaceUsage>> {
    use crate::structs::namespace::job_namespace;
    let mut usage: BTreeMap<String, NamespaceUsage> = BTreeMap::new();
    let mut jobs = conn.prepare_cached("SELECT id FROM job;")?;
    for id in jobs.query_map([], |row| row.get::<_, String>(0))? {
        usage
            .entry(job_namespace(&id?).to_string())
            .or_default()
            .jobs += 1;
    }
    for ((job_id, band), reservation) in live_reservations_on(conn)? {
        usage
            .entry(job_namespace(&job_id).to_string())
            .or_default()
            .bands
            .entry(band)
            .or_default()
            .add(&reservation);
    }
    Ok(usage)
}

impl Store {
    /// Every namespace's jobs and live reservations by band
    /// ([`NamespaceUsage`]).
    #[instrument(err, skip_all)]
    pub fn namespace_usage(&self) -> R<BTreeMap<String, NamespaceUsage>> {
        let conn = self.read_connection()?;
        namespace_usage_on(&conn)
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
            .tempdir_in(crate::test_scratch::root())
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
            pending_extensions: Default::default(),
            extensions: Default::default(),
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

    /// The first control leader of an empty store creates `legion-system`
    /// and its policy; it can't be deleted, and a later run changes nothing,
    /// so an operator's edit or delete of the policy sticks.
    #[test]
    fn system_namespace_is_created_once_and_cannot_be_deleted() -> R<()> {
        let store = setup_store()?;
        assert!(!store.namespace_exists(SYSTEM_NAMESPACE)?);
        assert!(store.ensure_system_namespace()?);
        assert!(store.namespace_exists(SYSTEM_NAMESPACE)?);
        let policy = store
            .get_namespace_policy(SYSTEM_NAMESPACE)?
            .expect("the system namespace's policy")
            .policy;
        assert_eq!(policy.namespaces, vec![SYSTEM_NAMESPACE.to_string()]);
        assert_eq!(
            policy
                .pools
                .and_then(|pools| pools.default)
                .map(|default| default.label()),
            Some(format!("{SYSTEM_NAMESPACE}, default"))
        );

        let err = store
            .delete_namespace_if_empty(SYSTEM_NAMESPACE)
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("reserved for Legion"),
            "{err:#}"
        );

        assert!(store.delete_namespace_policy(SYSTEM_NAMESPACE)?);
        assert!(!store.ensure_system_namespace()?, "already there");
        assert!(
            store.get_namespace_policy(SYSTEM_NAMESPACE)?.is_none(),
            "a deleted policy stays deleted"
        );
        // Still not creatable through the ordinary path, and a sibling is not.
        assert!(store
            .apply_namespace(Namespace {
                name: "legion-other".into(),
                ..Default::default()
            })
            .is_err());
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
