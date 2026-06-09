use crate::constraints::{ConstraintSet, DistinctTracker, PermitSet};
use crate::prelude::*;
use crate::resources::ResourcePolicy;
use crate::storage::errors::StorageError;
use crate::storage::migrations::get_migrations;
use crate::storage::persistence::{
    build_persistence_driver, PersistenceDriver, PersistenceRole, PersistenceStatus,
    RaftHistoryEvent, RaftStatus,
};
use crate::storage::queries::{
    DELETE_SIGNING_KEY, GET_ALLOCATION_BY_ID, GET_ALLOCS, GET_ALLOCS_FOR_JOB,
    GET_ALLOCS_FOR_JOB_CONFIG, GET_ALLOCS_FOR_NODE, GET_EVALS, GET_INSTANCE, GET_INSTANCES_FOR_JOB,
    GET_JOB, GET_JOBS, GET_JOB_CONFIG, GET_JOB_CONFIG_BY_ID, GET_JOB_UPDATE_FOR_CONFIG,
    GET_MANAGER_CONFIGS, GET_NEXT_EVAL, GET_NODE, GET_NODES, GET_SIGNING_KEYS,
    GET_TASK_FOR_JOB_CONIFG, GET_WORKER_TYPE, INSERT_ALLOCATION, INSERT_EVAL, INSERT_JOB,
    INSERT_JOB_CONFIG, INSERT_JOB_CONFIG_SUBMISSION, INSERT_JOB_UPDATE, INSERT_NODE,
    INSERT_SIGNING_KEY, INSERT_SUBMISSION, INSERT_TASK, INSERT_WORKER_TYPE, NODE_ATTRS,
    UPDATE_SIGNING_KEY_STATE, UPSERT_INSTANCE, UPSERT_MANAGER_CONFIG,
};
use crate::structs::{
    slot_instance_id, Allocation, AllocationHealth, Eval, Instance, InstanceMode, InstanceState,
    Job, JobConfig, KvEntry, KvLock, KvValueType, LedgerEntry, ManagerConfigFull,
    ManagerNodeConfig, Node, NodeMsg, Rollout, RolloutConfig, SigningKeyRecord, StatefulConfig,
    Submission, TaskCheck, TaskConfig, TaskCount, TaskDependency, TaskLifecycle,
    TaskOnUnhealthyConfig, TaskSchedule, TaskStatusState, WorkerConfig,
};
use crate::util::random_string;

use rand::{seq::SliceRandom, thread_rng, Rng};
use rusqlite::{
    named_params, params, params_from_iter, types::ValueRef, Connection, Error as RusqliteError,
    OpenFlags, OptionalExtension, Row, Transaction,
};
use rusqlite_migration::Migrations;
use serde::Serialize;
use serde_json::{from_str, to_string, to_string_pretty, Value};
use serde_rusqlite::to_params_named;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{MutexGuard, RwLock};
use std::time::{Duration, Instant};
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::Notify;

// warning incorrectly
#[allow(dead_code)]
pub enum Msg {
    Content(String),
}

pub type MemSafeConn = Arc<Mutex<Connection>>;
const READ_MODEL_LOCK_ERROR: &str = "Error locking read model";

#[derive(Debug, Clone, Default)]
pub struct SchedulingReadModel {
    pub nodes: HashMap<String, Node>,
    pub allocations_by_node: HashMap<String, Vec<Allocation>>,
    pub task_configs: HashMap<String, Vec<(i64, TaskConfig)>>,
}

#[derive(Debug)]
struct AllocationConstraintState {
    constraints: ConstraintSet,
    permit: PermitSet,
    distinct: Option<DistinctTracker>,
}

#[derive(Debug)]
struct AllocationTaskState {
    constraints: ConstraintSet,
    permit: PermitSet,
    distinct: Option<DistinctTracker>,
    job_config_id: String,
    resources: Value,
    lifecycle: TaskLifecycle,
}

#[derive(Debug, Clone)]
pub struct PlanSubmission {
    pub id: i64,
    pub job_config_id: String,
    pub expected_versions: BTreeMap<String, i64>,
}

#[derive(Debug, Clone)]
pub enum PlanApplyOutcome {
    Applied(Vec<Allocation>),
    Rejected {
        reason: String,
        latest_versions: BTreeMap<String, i64>,
    },
}

#[derive(Debug, Clone)]
pub struct Store {
    pub write_conn: MemSafeConn,
    pub read_conns: Arc<Vec<MemSafeConn>>,
    pub read_index: Arc<AtomicUsize>,
    pub scheduling_policy: String,
    pub default_eval_priority: i64,
    pub rollout_retention_days: u64,
    pub scheduler_settings: Option<Value>,
    pub resource_policy: ResourcePolicy,
    persistence_driver: Arc<dyn PersistenceDriver>,
    read_model: Arc<RwLock<SchedulingReadModel>>,
    // Cross-task wake signal for eval workers waiting on an empty queue.
    eval_notify: Arc<Notify>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobSubmitOutcome {
    Created { job_config_id: String },
    Updated { job_config_id: String },
    Unchanged { job_config_id: String },
}

/// Result of [`Store::destroy_keyed_instance`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestroyInstanceOutcome {
    /// The instance was live and is now terminated; `removed_allocations`
    /// allocation rows were deleted (0 when it had no current placement).
    Destroyed { removed_allocations: usize },
    /// The instance was already a tombstone; nothing changed (idempotent
    /// no-op).
    AlreadyTerminated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobKvReference {
    pub job_id: String,
    pub job_config_id: String,
    pub kv_key: String,
    pub json_pointer: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationHealthSnapshot {
    pub allocation_id: i64,
    pub node_id: String,
    pub healthy: bool,
    pub status: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateStoreQueryRow {
    pub json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateStoreQueryResult {
    pub statement_type: String,
    pub columns: Vec<String>,
    pub rows: Vec<StateStoreQueryRow>,
    pub affected_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateStoreSchemaColumn {
    pub ordinal: i32,
    pub name: String,
    pub data_type: String,
    pub not_null: bool,
    pub default_value: Option<String>,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateStoreSchemaObject {
    pub kind: String,
    pub name: String,
    pub create_sql: String,
    pub columns: Vec<StateStoreSchemaColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedStateQueryRecord {
    pub name: String,
    pub sql: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RolloutEventRecord {
    pub id: i64,
    pub rollout_id: i64,
    pub job_id: String,
    pub group_name: Option<String>,
    pub level: String,
    pub message: String,
    pub metadata_json: String,
    pub created_at: String,
}

impl JobSubmitOutcome {
    pub fn job_config_id(&self) -> &str {
        match self {
            JobSubmitOutcome::Created { job_config_id }
            | JobSubmitOutcome::Updated { job_config_id }
            | JobSubmitOutcome::Unchanged { job_config_id } => job_config_id,
        }
    }

    pub fn is_unchanged(&self) -> bool {
        matches!(self, JobSubmitOutcome::Unchanged { .. })
    }
}

pub const CONN_LOCK_ERROR: &str = "Error locking database connection";
const READ_ONLY_CONN_POOL_SIZE: usize = 2;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

// NOTE: This will become a general interface for all storage backends
impl Store {
    #[instrument(err, skip_all)]
    pub fn start_storage(config: &ManagerConfigFull) -> R<Store> {
        let db_path = Store::db_path(&config.store_path)?;
        let manager_node_id = format!("{}:{}", config.server.host, config.server.port);
        let persistence_driver: Arc<dyn PersistenceDriver> =
            build_persistence_driver(&config.persistence, db_path.clone(), &manager_node_id);
        persistence_driver.start()?;

        let write_conn = Store::create_write_conn(&db_path)?;
        let _migrations = Store::create_initial_tables(&Arc::clone(&write_conn))?;
        let read_conns = Store::create_read_pool(&db_path)?;
        let resource_policy = ResourcePolicy::from_manager_config(config)?;

        let store = Store {
            write_conn,
            read_conns: Arc::new(read_conns),
            read_index: Arc::new(AtomicUsize::new(0)),
            scheduling_policy: config.scheduling_policy.clone(),
            default_eval_priority: config.default_priority,
            rollout_retention_days: config.rollout_retention_days,
            scheduler_settings: config.scheduler_settings.clone(),
            resource_policy,
            persistence_driver,
            read_model: Arc::new(RwLock::new(SchedulingReadModel::default())),
            eval_notify: Arc::new(Notify::new()),
        };

        store.refresh_read_model()?;

        Ok(store)
    }

    pub fn persistence_status(&self) -> PersistenceStatus {
        self.persistence_driver.status()
    }

    pub fn persistence_role(&self) -> PersistenceRole {
        self.persistence_status().role
    }

    pub fn raft_status(&self) -> Option<RaftStatus> {
        self.persistence_driver.raft_status()
    }

    pub fn raft_history(&self, limit: usize) -> Vec<RaftHistoryEvent> {
        self.persistence_driver.raft_history(limit)
    }

    pub fn issue_raft_command(
        &self,
        command: &str,
        target: Option<&str>,
        actor: &str,
    ) -> R<RaftHistoryEvent> {
        self.persistence_driver
            .issue_raft_command(command, target, actor)
    }

    pub fn issue_raft_join_token(&self, actor: &str) -> R<(String, String)> {
        self.persistence_driver.issue_raft_join_token(actor)
    }

    #[instrument(err, skip_all)]
    fn db_path(store_path: &str) -> R<std::path::PathBuf> {
        // let conn = Connection::open_in_memory()?;

        // Using the disk version for now to simulate logs
        // Would eventually split this into a cluster that shares
        // logs via raft before committing to an in-memory sqlite db

        std::fs::create_dir_all(store_path)?;
        let tmp = std::path::Path::new(store_path).join("store.sqlite");
        Ok(tmp)
    }

    #[instrument(err, skip_all)]
    fn create_write_conn(db_path: &Path) -> R<MemSafeConn> {
        let final_path = db_path.to_str().context("Error parsing store path")?;
        let conn = Connection::open(final_path)?;
        Self::configure_connection(&conn, false)?;
        Ok(Arc::new(Mutex::new(conn)))
    }

    #[instrument(err, skip_all)]
    fn create_readonly_conn(db_path: &Path) -> R<MemSafeConn> {
        let final_path = db_path.to_str().context("Error parsing store path")?;
        let conn = Connection::open_with_flags(final_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        Self::configure_connection(&conn, true)?;
        conn.pragma_update(None, "query_only", "ON")?;
        Ok(Arc::new(Mutex::new(conn)))
    }

    fn configure_connection(conn: &Connection, read_only: bool) -> R<()> {
        conn.busy_timeout(SQLITE_BUSY_TIMEOUT)?;

        if !read_only {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
        }

        conn.pragma_update(None, "foreign_keys", "ON")?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    fn create_read_pool(db_path: &Path) -> R<Vec<MemSafeConn>> {
        let mut read_conns = Vec::with_capacity(READ_ONLY_CONN_POOL_SIZE);
        for _ in 0..READ_ONLY_CONN_POOL_SIZE {
            read_conns.push(Self::create_readonly_conn(db_path)?);
        }
        Ok(read_conns)
    }

    #[allow(dead_code)]
    pub fn reset_all_tables(&self) -> R<()> {
        let migrations = get_migrations();
        let mut conn = self.write_connection()?;
        migrations.to_version(&mut conn, 0)?;
        migrations.to_latest(&mut conn)?;
        drop(conn);
        self.refresh_read_model()?;

        Ok(())
    }

    #[instrument(skip_all)]
    fn create_initial_tables(conn: &MemSafeConn) -> R<Migrations<'_>> {
        let migrations = get_migrations();
        let mut conn = Self::lock_conn(conn, "migration")?;
        info!("Running migrations");
        migrations.to_latest(&mut conn)?;

        Ok(migrations)
    }

    fn lock_conn<'a>(
        conn: &'a MemSafeConn,
        connection_kind: &str,
    ) -> R<MutexGuard<'a, Connection>> {
        let lock_wait_started = Instant::now();
        let guard = conn.lock().map_err(|_| anyhow!(CONN_LOCK_ERROR))?;
        debug!(
            connection_kind,
            lock_wait_ms = lock_wait_started.elapsed().as_millis(),
            "Acquired sqlite connection lock"
        );
        Ok(guard)
    }

    fn write_connection(&self) -> R<MutexGuard<'_, Connection>> {
        Self::lock_conn(&self.write_conn, "write")
    }

    fn read_connection(&self) -> R<MutexGuard<'_, Connection>> {
        let pool = self.read_conns.as_slice();
        let index = self.read_index.fetch_add(1, Ordering::Relaxed);
        let conn = pool.get(index % pool.len()).unwrap_or(&self.write_conn);
        Self::lock_conn(conn, "read")
    }

    fn read_model_reader(&self) -> R<std::sync::RwLockReadGuard<'_, SchedulingReadModel>> {
        self.read_model
            .read()
            .map_err(|_| anyhow!(READ_MODEL_LOCK_ERROR))
    }

    fn read_model_writer(&self) -> R<std::sync::RwLockWriteGuard<'_, SchedulingReadModel>> {
        self.read_model
            .write()
            .map_err(|_| anyhow!(READ_MODEL_LOCK_ERROR))
    }

    fn load_read_model(&self) -> R<SchedulingReadModel> {
        let conn = self.read_connection()?;
        let mut nodes_stmt = conn.prepare(GET_NODES.as_str())?;
        let nodes = Self::collect_rows(nodes_stmt.query_map([], |row| self.node_from_row(row))?)?;

        let mut task_stmt = conn.prepare(
            r#"
            SELECT id, name, kind, count, count_per, group_name, lifecycle, schedule, depends_on, config, resources, constraints, permit, affinities, checks, on_unhealthy, kv_injections, job_config_id, identities, stateful
            FROM task_config;
            "#,
        )?;
        let task_rows = task_stmt.query_map([], |row| {
            let id = row.get(0)?;
            let count_num: u32 = row.get::<_, i64>(3)? as u32;
            let count_per: Option<String> = row.get(4)?;
            let group: String = row.get(5)?;
            let lifecycle = self.parse_task_lifecycle_column(6, row.get(6)?)?;
            let schedule = self.parse_schedule_column(7, row.get(7)?)?;
            let depends_on = self.parse_depends_on_column(8, row.get(8)?)?;
            let task = TaskConfig {
                name: row.get(1)?,
                kind: row.get(2)?,
                count: TaskCount {
                    num: count_num,
                    per: count_per,
                },
                group,
                lifecycle,
                schedule,
                depends_on,
                config: row.get(9)?,
                resources: row.get(10)?,
                constraints: row.get(11)?,
                permit: row.get(12)?,
                affinities: row.get(13)?,
                checks: self.parse_checks_column(14, row.get(14)?)?,
                on_unhealthy: self.parse_on_unhealthy_column(15, row.get(15)?)?,
                kv_injections: self.parse_kv_injections_column(16, row.get(16)?)?,
                identities: self.parse_identities_column(18, row.get(18)?)?,
                stateful: self.parse_stateful_column(19, row.get(19)?)?,
            };
            let job_config_id: String = row.get(17)?;
            Ok((job_config_id, id, task))
        })?;

        let mut task_configs: HashMap<String, Vec<(i64, TaskConfig)>> = HashMap::new();
        for row in task_rows {
            let (job_config_id, id, task) = row?;
            task_configs
                .entry(job_config_id)
                .or_default()
                .push((id, task));
        }

        let mut allocs_stmt = conn.prepare(GET_ALLOCS.as_str())?;
        let allocs =
            Self::collect_rows(allocs_stmt.query_map([], |row| self.allocation_from_row(row))?)?;
        let mut allocations_by_node: HashMap<String, Vec<Allocation>> = HashMap::new();
        for alloc in allocs {
            allocations_by_node
                .entry(alloc.node_id.clone())
                .or_default()
                .push(alloc);
        }

        Ok(SchedulingReadModel {
            nodes: nodes
                .into_iter()
                .map(|node| (node.id.clone(), node))
                .collect(),
            allocations_by_node,
            task_configs,
        })
    }

    fn refresh_read_model(&self) -> R<()> {
        let model = self.load_read_model()?;
        let mut guard = self.read_model_writer()?;
        *guard = model;
        Ok(())
    }

    pub fn scheduling_read_model(&self) -> R<SchedulingReadModel> {
        Ok(self.read_model_reader()?.clone())
    }

    fn cache_node(&self, node: Node) -> R<()> {
        let mut guard = self.read_model_writer()?;
        guard.nodes.insert(node.id.clone(), node);
        Ok(())
    }

    fn cache_node_dynamic_attrs(&self, node_id: &str, dynamic_attrs: Value) -> R<()> {
        let mut guard = self.read_model_writer()?;
        let cached = guard
            .nodes
            .get_mut(node_id)
            .with_context(|| format!("Missing cached node {node_id}"))?;
        cached.dynamic_attrs = dynamic_attrs;
        Ok(())
    }

    fn cache_nodes_lost(&self, ids: &[String]) -> R<()> {
        let mut guard = self.read_model_writer()?;
        for id in ids {
            if let Some(node) = guard.nodes.get_mut(id) {
                node.status = "lost".to_string();
                node.availability = "unavailable".to_string();
            }
        }
        Ok(())
    }

    fn cache_remove_node(&self, id: &str) -> R<()> {
        let mut guard = self.read_model_writer()?;
        guard.nodes.remove(id);
        guard.allocations_by_node.remove(id);
        Ok(())
    }

    fn cache_add_allocations(&self, allocations: &[Allocation]) -> R<()> {
        let mut guard = self.read_model_writer()?;
        for allocation in allocations {
            guard
                .allocations_by_node
                .entry(allocation.node_id.clone())
                .or_default()
                .push(allocation.clone());
        }
        Ok(())
    }

    fn cache_clear_allocations_for_node(&self, node_id: &str) -> R<()> {
        let mut guard = self.read_model_writer()?;
        guard
            .allocations_by_node
            .insert(node_id.to_string(), Vec::new());
        Ok(())
    }

    fn cache_remove_allocations(&self, allocations: &[Allocation]) -> R<()> {
        let mut guard = self.read_model_writer()?;
        for allocation in allocations {
            if let Some(node_allocs) = guard.allocations_by_node.get_mut(&allocation.node_id) {
                node_allocs.retain(|alloc| alloc.id != allocation.id);
            }
        }
        Ok(())
    }

    fn cache_set_task_configs_for_job_config(
        &self,
        job_config_id: &str,
        tasks: Vec<(i64, TaskConfig)>,
    ) -> R<()> {
        let mut guard = self.read_model_writer()?;
        guard.task_configs.insert(job_config_id.to_string(), tasks);
        Ok(())
    }

    pub fn cached_task_configs_for_job_config(
        &self,
        job_config_id: &str,
    ) -> R<Vec<(i64, TaskConfig)>> {
        {
            let guard = self.read_model_reader()?;
            if let Some(tasks) = guard.task_configs.get(job_config_id) {
                return Ok(tasks.clone());
            }
        }

        let tasks = self.get_task_configs_for_job_config(job_config_id)?;
        self.cache_set_task_configs_for_job_config(job_config_id, tasks.clone())?;
        Ok(tasks)
    }

    pub fn cached_allocations_by_node(&self) -> R<HashMap<String, Vec<Allocation>>> {
        Ok(self.read_model_reader()?.allocations_by_node.clone())
    }

    pub fn cached_nodes(&self) -> R<Vec<Node>> {
        Ok(self.read_model_reader()?.nodes.values().cloned().collect())
    }

    pub fn allocation_epoch_for_job_config(&self, job_config_id: &str) -> R<i64> {
        self.allocation_epoch_for_scope("job_config", job_config_id)
    }

    pub fn allocation_version_vector(
        &self,
        job_config_id: &str,
        node_ids: &HashSet<String>,
    ) -> R<BTreeMap<String, i64>> {
        let mut versions = BTreeMap::new();
        versions.insert(
            Self::scope_vector_key("job_config", job_config_id),
            self.allocation_epoch_for_scope("job_config", job_config_id)?,
        );
        for node_id in node_ids {
            versions.insert(
                Self::scope_vector_key("node", node_id),
                self.allocation_epoch_for_scope("node", node_id)?,
            );
        }
        Ok(versions)
    }

    fn allocation_epoch_for_scope(&self, scope_type: &str, scope_id: &str) -> R<i64> {
        let conn = self.read_connection()?;
        let epoch: Option<i64> = conn
            .query_row(
                "SELECT epoch FROM allocation_epoch_partition WHERE scope_type = ?1 AND scope_id = ?2;",
                params![scope_type, scope_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(epoch.unwrap_or(0))
    }

    fn scope_vector_key(scope_type: &str, scope_id: &str) -> String {
        format!("{scope_type}:{scope_id}")
    }

    fn parse_scope_vector_key(scope_key: &str) -> R<(&str, &str)> {
        let (scope_type, scope_id) = scope_key
            .split_once(':')
            .with_context(|| format!("invalid scope key '{scope_key}' in plan version vector"))?;
        Ok((scope_type, scope_id))
    }

    fn ensure_scope_epoch_tx(
        &self,
        tx: &Transaction<'_>,
        scope_type: &str,
        scope_id: &str,
    ) -> R<()> {
        tx.execute(
            "INSERT OR IGNORE INTO allocation_epoch_partition(scope_type, scope_id, epoch) VALUES (?1, ?2, 0);",
            params![scope_type, scope_id],
        )?;
        Ok(())
    }

    fn scope_epoch_tx(&self, tx: &Transaction<'_>, scope_type: &str, scope_id: &str) -> R<i64> {
        self.ensure_scope_epoch_tx(tx, scope_type, scope_id)?;
        let epoch = tx.query_row(
            "SELECT epoch FROM allocation_epoch_partition WHERE scope_type = ?1 AND scope_id = ?2;",
            params![scope_type, scope_id],
            |row| row.get(0),
        )?;
        Ok(epoch)
    }

    fn scope_vector_epochs_tx(
        &self,
        tx: &Transaction<'_>,
        scopes: &BTreeMap<String, i64>,
    ) -> R<BTreeMap<String, i64>> {
        let mut latest = BTreeMap::new();
        for scope_key in scopes.keys() {
            let (scope_type, scope_id) = Self::parse_scope_vector_key(scope_key)?;
            let epoch = self.scope_epoch_tx(tx, scope_type, scope_id)?;
            latest.insert(scope_key.clone(), epoch);
        }
        Ok(latest)
    }

    fn bump_scope_epoch_tx(
        &self,
        tx: &Transaction<'_>,
        scope_type: &str,
        scope_id: &str,
    ) -> R<i64> {
        self.ensure_scope_epoch_tx(tx, scope_type, scope_id)?;
        tx.execute(
            "UPDATE allocation_epoch_partition SET epoch = epoch + 1 WHERE scope_type = ?1 AND scope_id = ?2;",
            params![scope_type, scope_id],
        )?;
        self.scope_epoch_tx(tx, scope_type, scope_id)
    }

    fn bump_job_config_epoch_tx(&self, tx: &Transaction<'_>, job_config_id: &str) -> R<i64> {
        self.bump_scope_epoch_tx(tx, "job_config", job_config_id)
    }

    fn bump_job_config_epochs_tx(
        &self,
        tx: &Transaction<'_>,
        job_config_ids: &HashSet<String>,
    ) -> R<()> {
        for job_config_id in job_config_ids {
            self.bump_job_config_epoch_tx(tx, job_config_id)?;
        }
        Ok(())
    }

    // IMPORTANT: any transaction that mutates allocation rows must call this
    // (or bump_job_config_epoch_tx) before commit so stale plans are rejected.
    fn bump_epochs_for_allocations_tx(
        &self,
        tx: &Transaction<'_>,
        allocations: &[Allocation],
    ) -> R<()> {
        let job_config_ids: HashSet<String> = allocations
            .iter()
            .map(|alloc| alloc.job_config_id.clone())
            .collect();
        self.bump_job_config_epochs_tx(tx, &job_config_ids)?;

        let node_ids: HashSet<String> = allocations
            .iter()
            .map(|alloc| alloc.node_id.clone())
            .collect();
        for node_id in node_ids {
            self.bump_scope_epoch_tx(tx, "node", &node_id)?;
        }
        Ok(())
    }

    pub fn insert_plan_submission(
        &self,
        job_config_id: &str,
        mode: &str,
        placements: &[(String, i64)],
        expected_versions: &BTreeMap<String, i64>,
    ) -> R<PlanSubmission> {
        let created_at = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let placements_json = serde_json::to_string(placements)?;
        let expected_vector_json = serde_json::to_string(expected_versions)?;
        let expected_job_epoch = *expected_versions
            .get(&Self::scope_vector_key("job_config", job_config_id))
            .unwrap_or(&0);
        let conn = self.write_connection()?;
        let id = conn.query_row(
            "INSERT INTO plan_submission(job_config_id, mode, placements, expected_epoch, expected_version_vector, status, created_at) VALUES (?1, ?2, ?3, ?4, ?5, 'queued', ?6) RETURNING id;",
            params![
                job_config_id,
                mode,
                placements_json,
                expected_job_epoch,
                expected_vector_json,
                created_at
            ],
            |row| row.get(0),
        )?;

        Ok(PlanSubmission {
            id,
            job_config_id: job_config_id.to_string(),
            expected_versions: expected_versions.clone(),
        })
    }

    pub fn apply_plan_submission(&self, submission: &PlanSubmission) -> R<PlanApplyOutcome> {
        let tx_started = Instant::now();
        debug!(
            submission_id = submission.id,
            job_config_id = %submission.job_config_id,
            expected_scope_count = submission.expected_versions.len(),
            "Applying plan submission"
        );
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;

        let latest_versions = self.scope_vector_epochs_tx(&tx, &submission.expected_versions)?;
        if latest_versions != submission.expected_versions {
            let latest_job_epoch = latest_versions
                .get(&Self::scope_vector_key(
                    "job_config",
                    &submission.job_config_id,
                ))
                .copied()
                .unwrap_or_default();
            let reason = format!(
                "plan epoch for job config {} is stale; expected {}, latest {}",
                submission.job_config_id,
                submission
                    .expected_versions
                    .get(&Self::scope_vector_key(
                        "job_config",
                        &submission.job_config_id
                    ))
                    .copied()
                    .unwrap_or_default(),
                latest_job_epoch
            );
            tx.execute(
                "UPDATE plan_submission SET status = 'rejected', rejection_reason = ?1 WHERE id = ?2;",
                params![reason, submission.id],
            )?;
            tx.commit()?;
            warn!(
                submission_id = submission.id,
                job_config_id = %submission.job_config_id,
                latest_scope_count = latest_versions.len(),
                duration_ms = tx_started.elapsed().as_millis() as u64,
                "Rejected plan submission due to stale version vector"
            );
            return Ok(PlanApplyOutcome::Rejected {
                reason,
                latest_versions,
            });
        }

        let placements_json: String = tx.query_row(
            "SELECT placements FROM plan_submission WHERE id = ?1;",
            [submission.id],
            |row| row.get(0),
        )?;
        let placements: Vec<(String, i64)> = serde_json::from_str(&placements_json)?;

        let mut nodes_by_id: HashMap<String, Node> = HashMap::new();
        if !placements.is_empty() {
            let node_ids: Vec<String> = placements
                .iter()
                .map(|(node_id, _)| node_id.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            let placeholders = std::iter::repeat("?")
                .take(node_ids.len())
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "SELECT id, pool, status, availability, config_attrs, fingerprint_attrs, dynamic_attrs, health_checks, last_heartbeat FROM node WHERE id IN ({placeholders});"
            );
            let mut stmt = tx.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(node_ids.iter()), |row| {
                self.node_from_row(row)
            })?;
            for row in rows {
                let node = row?;
                nodes_by_id.insert(node.id.clone(), node);
            }
        }

        let mut allocations = Vec::new();
        let mut task_status_updates: HashMap<i64, TaskStatusState> = HashMap::new();
        let mut task_states: HashMap<i64, AllocationTaskState> = HashMap::new();
        for (node_id, task_id) in &placements {
            let state = match task_states.entry(*task_id) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert(self.load_allocation_task_state(&tx, *task_id)?)
                }
            };

            let node = nodes_by_id.get(node_id).with_context(|| {
                format!(
                    "Node {node_id} not found while applying plan {}",
                    submission.id
                )
            })?;
            if !state
                .constraints
                .is_satisfied(node, state.distinct.as_ref())
                || !state.permit.is_permitted(node)
            {
                let reason = format!(
                    "Node {} does not satisfy constraints for task {}",
                    node_id, task_id
                );
                tx.execute(
                    "UPDATE plan_submission SET status = 'rejected', rejection_reason = ?1 WHERE id = ?2;",
                    params![reason, submission.id],
                )?;
                let latest_versions =
                    self.scope_vector_epochs_tx(&tx, &submission.expected_versions)?;
                tx.commit()?;
                warn!(
                    submission_id = submission.id,
                    job_config_id = %submission.job_config_id,
                    duration_ms = tx_started.elapsed().as_millis() as u64,
                    "Rejected plan submission after validation during apply"
                );
                return Ok(PlanApplyOutcome::Rejected {
                    reason,
                    latest_versions,
                });
            }

            if let Some(tracker) = state.distinct.as_mut() {
                tracker.insert_from_node(node);
            }

            let allocation = self.insert_allocation_with_data(
                &tx,
                node_id,
                *task_id,
                &state.job_config_id,
                &state.resources,
            )?;
            task_status_updates
                .entry(*task_id)
                .or_insert_with(|| Self::task_status_for_lifecycle(&state.lifecycle));
            allocations.push(allocation);
        }

        if !allocations.is_empty() {
            self.apply_task_status_updates_tx(&tx, &task_status_updates)?;
            self.bump_epochs_for_allocations_tx(&tx, &allocations)?;
        }

        tx.execute(
            "UPDATE plan_submission SET status = 'applied', rejection_reason = NULL WHERE id = ?1;",
            [submission.id],
        )?;
        tx.commit()?;
        info!(
            submission_id = submission.id,
            job_config_id = %submission.job_config_id,
            placement_count = placements.len(),
            applied_allocations = allocations.len(),
            duration_ms = tx_started.elapsed().as_millis(),
            "Applied plan submission transaction"
        );

        self.cache_add_allocations(&allocations)?;

        Ok(PlanApplyOutcome::Applied(allocations))
    }

    fn collect_rows<T, I>(iter: I) -> R<Vec<T>>
    where
        I: Iterator<Item = Result<T, rusqlite::Error>>,
    {
        let values: Result<Vec<_>, rusqlite::Error> = iter.collect();
        let values = values.map_err(anyhow::Error::new)?;
        Ok(values)
    }

    fn ensure_kv_key(key: &str) -> R<()> {
        if key.trim().is_empty() {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "kv key cannot be empty".to_string(),
            }));
        }

        Ok(())
    }

    fn normalize_kv_value(value: &str, value_type: KvValueType) -> R<String> {
        match value_type {
            KvValueType::Unspecified => Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "kv value type must be specified".to_string(),
            })),
            KvValueType::String => Ok(value.to_string()),
            KvValueType::Number => {
                let parsed: Value = from_str(value).map_err(|err| {
                    anyhow::Error::new(StorageError::InvalidInput {
                        message: format!("invalid number value: {err}"),
                    })
                })?;
                match parsed {
                    Value::Number(_) => Ok(parsed.to_string()),
                    _ => Err(anyhow::Error::new(StorageError::InvalidInput {
                        message: "value is not a number".to_string(),
                    })),
                }
            }
            KvValueType::Json => {
                let parsed: Value = from_str(value).map_err(|err| {
                    anyhow::Error::new(StorageError::InvalidInput {
                        message: format!("invalid json value: {err}"),
                    })
                })?;
                Ok(parsed.to_string())
            }
        }
    }

    fn now_rfc3339() -> R<String> {
        Ok(OffsetDateTime::now_utc().format(&Rfc3339)?)
    }

    fn insert_task_statuses(
        tx: &Transaction<'_>,
        task_ids: &[i64],
        job_id: &str,
        job_config_id: &str,
    ) -> R<()> {
        let now = Self::now_rfc3339()?;
        for task_id in task_ids {
            tx.execute(
                "INSERT INTO task_status (task_config_id, job_id, job_config_id, status, updated_at) VALUES (?, ?, ?, ?, ?);",
                params![task_id, job_id, job_config_id, TaskStatusState::Pending.as_str(), now],
            )?;
        }
        Ok(())
    }

    fn parse_rfc3339(value: &str) -> R<OffsetDateTime> {
        Ok(OffsetDateTime::parse(value, &Rfc3339)?)
    }

    fn random_lock_id() -> String {
        let mut rng = thread_rng();
        let bytes: [u8; 16] = rng.gen();
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn load_kv_lock_tx(&self, tx: &Transaction<'_>, key: &str) -> R<Option<KvLock>> {
        let lock = tx
            .query_row(
                "SELECT key, lock_id, owner, acquired_at, expires_at FROM kv_lock WHERE key = ?;",
                [key],
                |row| self.kv_lock_from_row(row),
            )
            .optional()?;
        Ok(lock)
    }

    fn job_config_ids_with_incomplete_rollouts(&self) -> R<Vec<String>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT to_job_config_id
             FROM rollout
             WHERE status = 'completed_incomplete';",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        Self::collect_rows(rows)
    }

    fn enforce_kv_lock(&self, tx: &Transaction<'_>, key: &str, lock_id: Option<&str>) -> R<()> {
        let Some(lock) = self.load_kv_lock_tx(tx, key)? else {
            return Ok(());
        };

        let expires_at = Self::parse_rfc3339(&lock.expires_at)?;
        if expires_at <= OffsetDateTime::now_utc() {
            tx.execute("DELETE FROM kv_lock WHERE key = ?;", [key])?;
            return Ok(());
        }

        if lock_id != Some(lock.lock_id.as_str()) {
            return Err(anyhow::Error::new(StorageError::Conflict {
                message: "kv lock is held by another client".to_string(),
            }));
        }

        Ok(())
    }

    //
    // from_row helpers
    //

    fn job_from_row(&self, row: &Row<'_>) -> Result<Job, rusqlite::Error> {
        let job = Job {
            id: row.get(0)?,
            name: row.get(1)?,
            status: row.get(2)?,
            created_at: row.get(3)?,
        };

        Ok(job)
    }

    fn node_from_row(&self, row: &Row<'_>) -> Result<Node, rusqlite::Error> {
        let node = Node {
            id: row.get(0)?,
            pool: row.get(1)?,
            status: row.get(2)?,
            availability: row.get(3)?,
            config_attrs: row.get(4)?,
            fingerprint_attrs: row.get(5)?,
            dynamic_attrs: row.get(6)?,
            health_checks: row.get(7)?,
            last_heartbeat: row.get(8)?,
        };

        Ok(node)
    }

    fn eval_from_row(&self, row: &Row<'_>) -> Result<Eval, rusqlite::Error> {
        let eval = Eval {
            id: row.get::<_, i64>(0)?.to_string(),
            status: row.get(1)?,
            kind: row.get(2)?,
            job_id: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
            job_config_id: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
            node_name: row.get::<_, Option<String>>(5)?.unwrap_or_default(),
            priority: row.get(6)?,
        };
        let _ready_at: String = row.get(7)?;

        Ok(eval)
    }

    fn kv_entry_from_row(&self, row: &Row<'_>) -> Result<KvEntry, rusqlite::Error> {
        let value_type: i32 = row.get(2)?;
        let entry = KvEntry {
            key: row.get(0)?,
            value: row.get(1)?,
            value_type,
            version: row.get(3)?,
            created_at: row.get(4)?,
            updated_at: row.get(5)?,
        };

        Ok(entry)
    }

    fn kv_lock_from_row(&self, row: &Row<'_>) -> Result<KvLock, rusqlite::Error> {
        let lock = KvLock {
            key: row.get(0)?,
            lock_id: row.get(1)?,
            owner: row.get(2)?,
            acquired_at: row.get(3)?,
            expires_at: row.get(4)?,
        };

        Ok(lock)
    }

    fn rollout_from_row(&self, row: &Row<'_>) -> Result<Rollout, rusqlite::Error> {
        let in_place: Option<i64> = row.get(10)?;
        Ok(Rollout {
            id: row.get(0)?,
            job_id: row.get(1)?,
            from_job_config_id: row.get(2)?,
            to_job_config_id: row.get(3)?,
            status: row.get(4)?,
            incomplete: row.get::<_, i64>(5)? != 0,
            message: row.get(6)?,
            strategy: row.get(7)?,
            progression: row.get(8)?,
            replacement: row.get(9)?,
            in_place: in_place.map(|value| value != 0),
            config_json: row.get(11)?,
            created_at: row.get(12)?,
            updated_at: row.get(13)?,
            completed_at: row.get(14)?,
        })
    }

    fn allocation_health_snapshot_from_row(
        &self,
        row: &Row<'_>,
    ) -> Result<AllocationHealthSnapshot, rusqlite::Error> {
        Ok(AllocationHealthSnapshot {
            allocation_id: row.get(0)?,
            node_id: row.get(1)?,
            healthy: row.get::<_, i64>(2)? != 0,
            status: row.get(3)?,
            updated_at: row.get(4)?,
        })
    }

    fn rollout_event_from_row(&self, row: &Row<'_>) -> Result<RolloutEventRecord, rusqlite::Error> {
        Ok(RolloutEventRecord {
            id: row.get(0)?,
            rollout_id: row.get(1)?,
            job_id: row.get(2)?,
            group_name: row.get(3)?,
            level: row.get(4)?,
            message: row.get(5)?,
            metadata_json: row.get(6)?,
            created_at: row.get(7)?,
        })
    }

    fn job_kv_reference_from_row(&self, row: &Row<'_>) -> Result<JobKvReference, RusqliteError> {
        Ok(JobKvReference {
            job_id: row.get(0)?,
            job_config_id: row.get(1)?,
            kv_key: row.get(2)?,
            json_pointer: row.get(3)?,
        })
    }

    fn worker_type_from_row(&self, row: &Row<'_>) -> Result<WorkerConfig, rusqlite::Error> {
        let af: String = row.get(2)?;
        let attrs: String = row.get(3)?;
        let parsed_attrs = serde_json::from_str(&attrs).unwrap();
        let name_prefix = row.get(4)?;
        let resources = row.get(5)?;
        let reserved_resources = row.get(6)?;
        let max_allocations = row.get(7)?;

        Ok(WorkerConfig {
            name: row.get(0)?,
            pool: row.get(1)?,
            additional_node_checks: af
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.to_string())
                .collect(),
            attributes: parsed_attrs,
            name_prefix,
            resources,
            reserved_resources,
            max_allocations,
        })
    }

    fn parse_checks_column(
        &self,
        idx: usize,
        raw: String,
    ) -> Result<Vec<TaskCheck>, rusqlite::Error> {
        serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_on_unhealthy_column(
        &self,
        idx: usize,
        raw: String,
    ) -> Result<TaskOnUnhealthyConfig, rusqlite::Error> {
        serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_schedule_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Option<TaskSchedule>, rusqlite::Error> {
        raw.map(|value| {
            value.parse::<TaskSchedule>().map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    idx,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
                )
            })
        })
        .transpose()
    }

    fn parse_depends_on_column(
        &self,
        idx: usize,
        raw: String,
    ) -> Result<Vec<TaskDependency>, rusqlite::Error> {
        serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_kv_injections_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Option<crate::structs::TaskKvInjections>, rusqlite::Error> {
        let Some(raw) = raw else {
            return Ok(None);
        };
        serde_json::from_str(&raw).map(Some).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_identities_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Vec<crate::structs::WorkloadIdentity>, rusqlite::Error> {
        let Some(raw) = raw else {
            return Ok(Vec::new());
        };
        serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_stateful_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Option<StatefulConfig>, rusqlite::Error> {
        let Some(raw) = raw else {
            return Ok(None);
        };
        serde_json::from_str(&raw).map(Some).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_task_lifecycle_column(
        &self,
        idx: usize,
        raw: String,
    ) -> Result<TaskLifecycle, rusqlite::Error> {
        raw.parse::<TaskLifecycle>().map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
            )
        })
    }

    fn task_config_from_row(&self, row: &Row<'_>) -> Result<TaskConfig, rusqlite::Error> {
        let count_num: u32 = row.get::<_, i64>(3)? as u32;
        let count_per: Option<String> = row.get(4)?;
        let group: String = row.get(5)?;
        let lifecycle = self.parse_task_lifecycle_column(6, row.get(6)?)?;
        let schedule = self.parse_schedule_column(7, row.get(7)?)?;
        let depends_on = self.parse_depends_on_column(8, row.get(8)?)?;
        let task_config = TaskConfig {
            name: row.get(1)?,
            kind: row.get(2)?,
            count: TaskCount {
                num: count_num,
                per: count_per,
            },
            group,
            lifecycle,
            schedule,
            depends_on,
            config: row.get(9)?,
            resources: row.get(10)?,
            constraints: row.get(11)?,
            permit: row.get(12)?,
            affinities: row.get(13)?,
            checks: self.parse_checks_column(14, row.get(14)?)?,
            on_unhealthy: self.parse_on_unhealthy_column(15, row.get(15)?)?,
            kv_injections: self.parse_kv_injections_column(16, row.get(16)?)?,
            identities: self.parse_identities_column(19, row.get(19)?)?,
            stateful: self.parse_stateful_column(20, row.get(20)?)?,
        };

        Ok(task_config)
    }

    fn allocation_from_row(&self, row: &Row<'_>) -> Result<Allocation, rusqlite::Error> {
        let allocation = Allocation {
            id: row.get(0)?,
            status: row.get(1)?,
            resources: row.get(2)?,
            node_id: row.get(3)?,
            job_config_id: row.get(4)?,
            task_config_id: row.get(5)?,
            job_id: row.get(6)?,
            instance_id: row.get(7)?,
        };

        Ok(allocation)
    }

    fn instance_from_row(&self, row: &Row<'_>) -> Result<Instance, rusqlite::Error> {
        let mode: String = row.get(4)?;
        let state: String = row.get(5)?;
        let parse_err = |idx: usize, err: String| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
            )
        };
        let instance = Instance {
            instance_id: row.get(0)?,
            job_id: row.get(1)?,
            group: row.get(2)?,
            task_name: row.get(3)?,
            mode: mode.parse().map_err(|err| parse_err(4, err))?,
            state: state.parse().map_err(|err| parse_err(5, err))?,
            slot_index: row.get::<_, Option<i64>>(6)?.map(|slot| slot as u32),
            current_alloc_id: row.get(7)?,
            storage_handle: row.get(8)?,
            created_at: row.get(9)?,
            updated_at: row.get(10)?,
        };

        Ok(instance)
    }

    fn manager_config_from_row(&self, row: &Row<'_>) -> Result<ManagerNodeConfig, rusqlite::Error> {
        Ok(ManagerNodeConfig {
            node_id: row.get(0)?,
            config_json: row.get(1)?,
            updated_at: row.get(2)?,
        })
    }

    //
    // MANAGER CONFIG
    //

    #[instrument(err, skip_all)]
    pub fn upsert_manager_config(&self, node_id: &str, config: &ManagerConfigFull) -> R<()> {
        let fmt = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
                 sign:mandatory]:[offset_minute]:[offset_second]"
        );

        let timestamp_now = OffsetDateTime::now_utc().format(fmt)?;
        let config_json = to_string_pretty(config)?;

        let conn = self.write_connection()?;

        let params = named_params! {
            ":node_id": node_id,
            ":config": config_json,
            ":updated_at": timestamp_now,
        };

        conn.execute(&UPSERT_MANAGER_CONFIG, params)?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn get_manager_configs(&self) -> R<Vec<ManagerNodeConfig>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_MANAGER_CONFIGS.as_str())?;
        let configs =
            Self::collect_rows(stmt.query_map([], |row| self.manager_config_from_row(row))?)?;

        Ok(configs)
    }

    //
    // NODES
    //

    #[instrument(err, skip_all)]
    pub fn get_node(&self, id: &str) -> R<Node> {
        let conn = self.read_connection()?;
        let node = conn
            .query_row(&GET_NODE, [id], |row| self.node_from_row(row))
            .map_err(|error| match error {
                RusqliteError::QueryReturnedNoRows => {
                    anyhow::Error::new(StorageError::not_found("node", id))
                }
                other => anyhow::Error::from(other),
            })?;
        Ok(node)
    }

    #[instrument(err, skip_all)]
    pub fn get_nodes(&self) -> R<Vec<Node>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_NODES.as_str())?;
        let nodes = Self::collect_rows(stmt.query_map([], |row| self.node_from_row(row))?)?;

        Ok(nodes)
    }

    pub fn job_config_ids_with_count_per(&self) -> R<Vec<String>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT job_config_id FROM task_config WHERE count_per IS NOT NULL AND count_per != '';",
        )?;
        let rows = stmt.query_map([], |row| row.get(0))?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all)]
    pub fn mark_node_ready(&self, id: &str, last_heartbeat: &str) -> R<()> {
        let conn = self.write_connection()?;
        let sql = r#"
        UPDATE node
        SET status = :status, availability = :availability, last_heartbeat = :last_heartbeat
        WHERE id = :id;
        "#;

        let params = named_params! {
            ":id": id,
            ":status": "ready",
            ":availability": "available",
            ":last_heartbeat": last_heartbeat,
        };

        conn.execute(sql, params)?;

        drop(conn);
        let node = self.get_node(id)?;
        self.cache_node(node)?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn mark_node_lost(&self, id: &str) -> R<()> {
        let conn = self.write_connection()?;
        let sql = r#"
        UPDATE node
        SET status = :status, availability = :availability
        WHERE id = :id;
        "#;

        let params = named_params! {
            ":id": id,
            ":status": "lost",
            ":availability": "unavailable",
        };

        conn.execute(sql, params)?;

        drop(conn);
        let node = self.get_node(id)?;
        self.cache_node(node)?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn remove_allocs_for_node(&self, node_id: &str) -> R<Vec<Allocation>> {
        let allocs = self.allocs_for_node(node_id)?;

        if allocs.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM allocation WHERE node_id = ?;", [node_id])?;
        self.release_instance_bindings_tx(&tx, &allocs)?;
        self.bump_epochs_for_allocations_tx(&tx, &allocs)?;
        tx.commit()?;

        self.cache_clear_allocations_for_node(node_id)?;

        Ok(allocs)
    }

    #[instrument(err, skip_all)]
    pub fn remove_node(&self, id: &str) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute("DELETE FROM node WHERE id = ?;", [id])?;

        self.cache_remove_node(id)?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn get_node_ids_and_heartbeats(&self) -> R<Vec<(String, String)>> {
        let conn = self.read_connection()?;
        let sql = "SELECT id, last_heartbeat FROM node WHERE status = 'active'";
        let mut stmt = conn.prepare(sql)?;
        let nodes_id_and_hb =
            Self::collect_rows(stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?)?;

        Ok(nodes_id_and_hb)
    }

    #[instrument(err, skip_all)]
    pub fn mark_nodes_as_lost(&self, ids: &[String]) -> R<()> {
        if ids.is_empty() {
            return Ok(());
        }

        {
            let conn = self.write_connection()?;
            let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let sql = format!(
                "UPDATE node SET status = 'lost', availability = 'unavailable' WHERE id IN ({})",
                placeholders
            );

            let mut stmt = conn.prepare(&sql)?;
            stmt.execute(params_from_iter(ids))?;
        }

        self.cache_nodes_lost(ids)?;

        for node_id in ids {
            self.reschedule_allocations_for_lost_node(node_id)?;
        }

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn insert_node(&self, node: NodeMsg) -> R<Node> {
        let conn = self.write_connection()?;
        // TODO:
        // Its not ideal that I am using NodeMsg here
        // I get an error about serializing Map, and the simplest
        // way around it is to just use the node message when using
        // to_params_named, as its already a string

        let node = conn.query_row(
            &INSERT_NODE,
            to_params_named(node)?.to_slice().as_slice(),
            |row| self.node_from_row(row),
        )?;

        self.cache_node(node.clone())?;

        Ok(node)
    }

    #[instrument(err, skip_all)]
    pub fn update_node_dyamic(&self, id: &str, dynamic_diff: &Map<String, Value>) -> R<Node> {
        let node = self.get_node(id)?;
        let default_map = Map::new();

        let mut res = node
            .dynamic_attrs
            .as_object()
            .unwrap_or(&default_map)
            .clone();

        res.extend(dynamic_diff.clone());
        let res_as_string = to_string(&res).context("failed to serialize dynamic attrs")?;

        let conn = self.write_connection()?;

        let update_sql = format!(
            r#"
        UPDATE node
        SET dynamic_attrs = :dynamic_attrs
        WHERE id IN (:id)
        RETURNING {NODE_ATTRS};
        "#
        );

        let params = named_params! {
            ":id": id,
            ":dynamic_attrs": res_as_string,
        };

        let mut node = conn.query_row(&update_sql, params, |row| self.node_from_row(row))?;
        let dynamic_attrs = Value::Object(res);
        node.dynamic_attrs = dynamic_attrs.clone();

        self.cache_node_dynamic_attrs(&node.id, dynamic_attrs)?;

        Ok(node)
    }

    #[instrument(err, skip_all)]
    pub fn update_node_heartbeat(&self, id: String) -> R<()> {
        let timestamp_now = OffsetDateTime::now_utc().format(&Rfc3339)?;

        let conn = self.write_connection()?;
        let sql = r#"
        UPDATE node
        SET last_heartbeat = :ts, status = 'active'
        WHERE id = :id;
"#;
        info!(
            "Updating heartbeat for node {} with TS: {}",
            id, timestamp_now
        );

        let params = named_params! {
            ":id": id,
            ":ts": timestamp_now,
        };

        conn.execute(sql, params)?;

        drop(conn);
        let node = self.get_node(&id)?;
        self.cache_node(node)?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn update_node_health_checks(&self, id: &str, health_checks: &str) -> R<()> {
        let conn = self.write_connection()?;
        let sql = r#"
        UPDATE node
        SET health_checks = :health_checks
        WHERE id = :id;
"#;

        let params = named_params! {
            ":id": id,
            ":health_checks": health_checks,
        };

        conn.execute(sql, params)?;

        drop(conn);
        let node = self.get_node(id)?;
        self.cache_node(node)?;

        Ok(())
    }

    //
    // WORKER TYPES
    //

    #[instrument(err, skip_all)]
    pub fn get_worker_type(&self, name: String) -> R<WorkerConfig> {
        let conn = self.read_connection()?;
        let config = conn
            .query_row(&GET_WORKER_TYPE, [name.clone()], |row| {
                self.worker_type_from_row(row)
            })
            .map_err(|error| match error {
                RusqliteError::QueryReturnedNoRows => {
                    anyhow::Error::new(StorageError::not_found("worker type", name))
                }
                other => anyhow::Error::from(other),
            })?;

        Ok(config)
    }

    #[instrument(err, skip_all)]
    pub fn insert_worker_type(&self, config: &WorkerConfig) -> R<WorkerConfig> {
        let conn = self.write_connection()?;
        let node_checks_to_save = config.additional_node_checks.join(",");
        let attrs_to_save = serde_json::to_string(&config.attributes)?;
        let resources_to_save = if config.resources.is_empty() {
            "{}".to_string()
        } else {
            config.resources.clone()
        };
        let reserved_resources_to_save = if config.reserved_resources.is_empty() {
            "{}".to_string()
        } else {
            config.reserved_resources.clone()
        };

        let params = named_params! {
            ":name": config.name,
            ":pool": config.pool,
            ":additional_node_checks": node_checks_to_save,
            ":attributes": attrs_to_save,
            ":name_prefix": config.name_prefix,
            ":resources": resources_to_save,
            ":reserved_resources": reserved_resources_to_save,
            ":max_allocations": config.max_allocations,
        };

        let config = conn.query_row(&INSERT_WORKER_TYPE, params, |row| {
            self.worker_type_from_row(row)
        })?;

        Ok(config)
    }

    //
    // JOBS
    //

    #[instrument(err, skip_all)]
    pub fn get_job(&self, name: &str) -> R<Job> {
        let conn = self.read_connection()?;
        let job = conn
            .query_row(&GET_JOB, [name], |row| self.job_from_row(row))
            .map_err(|error| match error {
                RusqliteError::QueryReturnedNoRows => {
                    anyhow::Error::new(StorageError::not_found("job", name))
                }
                other => anyhow::Error::from(other),
            })?;
        Ok(job)
    }

    pub fn get_jobs(&self) -> R<Vec<Job>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&GET_JOBS)?;
        let jobs = Self::collect_rows(stmt.query_map([], |row| self.job_from_row(row))?)?;

        Ok(jobs)
    }

    #[instrument(err, skip_all)]
    pub fn stop_job(&self, name: &str) -> R<()> {
        let conn = self.write_connection()?;
        let sql = r#"
        UPDATE job
        SET status = 'stopped'
        WHERE name = ?;
"#;
        conn.execute(sql, [name])?;

        // TODO: Create a job_update record
        // that kicks off the eval?
        // ... or maybe just this adds the eval straight up?

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn mark_job_failed(&self, job_id: &str) -> R<()> {
        let conn = self.write_connection()?;
        let sql = r#"
        UPDATE job
        SET status = 'failed'
        WHERE id = ?;
"#;
        conn.execute(sql, [job_id])?;
        Ok(())
    }

    /// Mark a job failed because a required KV dependency vanished
    /// (`onDelete: fail`). The job status becomes `failed` and every live
    /// allocation status is stamped `failed:<reason>` so the diagnostic is
    /// visible in `job list`, the ledger, and state-store queries. The
    /// allocation rows are retained (the workload is not torn down and no
    /// reschedule storm starts); a later KV put that triggers a restart or
    /// rollout recovers the job (see `docs/kv-store.md`).
    #[instrument(err, skip(self))]
    pub fn mark_job_failed_for_kv(&self, job_id: &str, reason: &str) -> R<()> {
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        tx.execute("UPDATE job SET status = 'failed' WHERE id = ?;", [job_id])?;
        tx.execute(
            "UPDATE allocation SET status = ?1
             WHERE job_config_id IN (SELECT id FROM job_config WHERE job_id = ?2);",
            (format!("failed:{reason}"), job_id),
        )?;
        tx.commit()?;
        self.refresh_read_model()?;
        Ok(())
    }

    /// Clear a `failed` job status (back to `done`) when a KV-triggered
    /// restart or rollout actively recovers the job. No-op when the job is
    /// not currently failed. Returns whether a row was updated.
    #[instrument(err, skip(self))]
    pub fn clear_job_failed_status(&self, job_id: &str) -> R<bool> {
        let conn = self.write_connection()?;
        let updated = conn.execute(
            "UPDATE job SET status = 'done' WHERE id = ?1 AND status = 'failed';",
            [job_id],
        )?;
        Ok(updated > 0)
    }

    #[instrument(err, skip_all)]
    pub fn destroy_job(&self, name: &str) -> R<()> {
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;

        let job = tx.query_row(&GET_JOB, [name], |row| self.job_from_row(row))?;
        if job.status != "stopped" {
            bail!("Job must be stopped before it can be destroyed");
        }

        let submission_hashes: Vec<String> = {
            let mut sub_stmt = tx.prepare(
                r#"
                SELECT DISTINCT jcs.submission_hash
                FROM job_config_submission jcs
                INNER JOIN job_config jc ON jc.id = jcs.job_config_id
                WHERE jc.job_id = ?;
            "#,
            )?;

            let rows = sub_stmt.query_map([job.id.clone()], |row| row.get(0))?;
            Self::collect_rows(rows)?
        };

        let delete_sql = r#"
            DELETE FROM job
            WHERE name = ? AND status = 'stopped';
        "#;

        tx.execute(delete_sql, [name])?;

        for submission_hash in submission_hashes {
            let reference_count: i64 = tx.query_row(
                r#"
                SELECT COUNT(*)
                FROM job_config_submission
                WHERE submission_hash = ?;
            "#,
                [submission_hash.clone()],
                |row| row.get(0),
            )?;

            if reference_count == 0 {
                tx.execute("DELETE FROM submission WHERE hash = ?;", [submission_hash])?;
            }
        }

        tx.commit()
            .map_err(|_| anyhow!("Error committing transaction"))
    }

    fn rollout_meta_from_value(
        meta: Value,
    ) -> Result<(Option<RolloutConfig>, BTreeMap<String, RolloutConfig>), RusqliteError> {
        let Some(meta) = meta.as_object() else {
            return Ok((None, BTreeMap::new()));
        };

        let rollout = match meta.get("rollout") {
            Some(value) if !value.is_null() => Some(
                serde_json::from_value::<RolloutConfig>(value.clone()).map_err(|err| {
                    RusqliteError::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?,
            ),
            _ => None,
        };

        let rollout_by_group = match meta.get("rolloutByGroup") {
            Some(value) if !value.is_null() => serde_json::from_value::<
                BTreeMap<String, RolloutConfig>,
            >(value.clone())
            .map_err(|err| {
                RusqliteError::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?,
            _ => BTreeMap::new(),
        };

        Ok((rollout, rollout_by_group))
    }

    pub fn get_job_config_optional(&self, name: &str) -> R<Option<JobConfig>> {
        let conn = self.read_connection()?;
        let maybe_job_config: Option<(JobConfig, String)> = conn
            .query_row(&GET_JOB_CONFIG, [name], |row| {
                let tasks_to_replace = vec![];
                let job_config_id = row.get(0)?;
                let meta: Value = row.get(3)?;
                let (rollout, rollout_by_group) = Self::rollout_meta_from_value(meta)?;
                let jc = JobConfig {
                    name: row.get(1)?,
                    tasks: tasks_to_replace,
                    rollout,
                    rollout_by_group,
                    version: Some(row.get(4)?),
                };

                Ok((jc, job_config_id))
            })
            .optional()?;

        let Some((job_config, jc_id)) = maybe_job_config else {
            return Ok(None);
        };

        let mut stmt = conn.prepare(&GET_TASK_FOR_JOB_CONIFG)?;
        let tasks =
            Self::collect_rows(stmt.query_map([jc_id], |row| self.task_config_from_row(row))?)?;

        let jc_2 = JobConfig {
            tasks,
            ..job_config
        };

        Ok(Some(jc_2))
    }

    pub fn get_job_config_by_id_optional(&self, id: &str) -> R<Option<JobConfig>> {
        let conn = self.read_connection()?;
        let maybe_job_config: Option<(JobConfig, String)> = conn
            .query_row(&GET_JOB_CONFIG_BY_ID, [id], |row| {
                let tasks_to_replace = vec![];
                let job_config_id = row.get(0)?;
                let meta: Value = row.get(3)?;
                let (rollout, rollout_by_group) = Self::rollout_meta_from_value(meta)?;
                let jc = JobConfig {
                    name: row.get(1)?,
                    tasks: tasks_to_replace,
                    rollout,
                    rollout_by_group,
                    version: Some(row.get(4)?),
                };

                Ok((jc, job_config_id))
            })
            .optional()?;

        let Some((job_config, jc_id)) = maybe_job_config else {
            return Ok(None);
        };

        let mut stmt = conn.prepare(&GET_TASK_FOR_JOB_CONIFG)?;
        let tasks =
            Self::collect_rows(stmt.query_map([jc_id], |row| self.task_config_from_row(row))?)?;

        let jc_2 = JobConfig {
            tasks,
            ..job_config
        };

        Ok(Some(jc_2))
    }

    #[instrument(err, skip_all)]
    pub fn get_job_config(&self, name: &str) -> R<JobConfig> {
        self.get_job_config_optional(name)?
            .ok_or_else(|| StorageError::not_found("job config", name).into())
    }

    #[instrument(err, skip_all)]
    pub fn get_job_config_by_id(&self, id: &str) -> R<JobConfig> {
        self.get_job_config_by_id_optional(id)?
            .ok_or_else(|| StorageError::not_found("job config", id).into())
    }

    /// The owning job id for a job config id, if the config exists.
    #[instrument(err, skip_all)]
    pub fn job_id_for_job_config(&self, job_config_id: &str) -> R<Option<String>> {
        let conn = self.read_connection()?;
        let job_id = conn
            .query_row(
                "SELECT job_id FROM job_config WHERE id = ?;",
                [job_config_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(job_id)
    }

    pub fn list_job_spec_history(&self, name: &str) -> R<Vec<(String, String, Option<i64>)>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT jc.id, jc.created_at, jc.version
            FROM job_config jc
            INNER JOIN job j ON j.id = jc.job_id
            WHERE j.name = ?
            ORDER BY jc.created_at DESC;
            "#,
        )?;

        let rows = stmt.query_map([name], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get::<_, Option<i64>>(2)?))
        })?;

        Self::collect_rows(rows)
    }

    #[allow(dead_code)]
    pub fn handle_job_submit(
        &self,
        config: &JobConfig,
        subs: &[Submission],
    ) -> R<JobSubmitOutcome> {
        let config = self.normalize_job_config(config)?;
        // Note: There is a race condition here with the version
        // I think this should prob be all in a single lock, but
        // I am not going to worry about this for now

        let mut next_version = 1;
        let mut last_versions_id: Option<String> = None;
        let mut was_update = false;

        if let Some(latest_job_config) = self.get_job_config_optional(&config.name)? {
            // if latest_job_config is deep equal to config, return unchanged
            if latest_job_config.is_same_as(&config) {
                return Ok(JobSubmitOutcome::Unchanged {
                    job_config_id: latest_job_config.id(),
                });
            } else {
                next_version = latest_job_config.version.unwrap_or(0) + 1;
                last_versions_id = Some(latest_job_config.id());
                was_update = true;
            }
        }

        let mut conn = self.write_connection()?;
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let tx = conn.transaction()?;

        let get_job_res = tx.query_row(&GET_JOB, [config.name.clone()], |row| {
            self.job_from_row(row)
        });

        let job = match get_job_res {
            Ok(j) => Ok(j),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                let job_params = named_params! {
                    ":id": config.name,
                    ":name": config.name,
                    ":status": "done",
                    ":created_at": now,
                };

                tx.query_row(&INSERT_JOB, job_params, |row| self.job_from_row(row))
            }
            Err(error) => Err(error),
        }?;

        let now = time::OffsetDateTime::now_utc();
        let meta = if config.rollout.is_none() && config.rollout_by_group.is_empty() {
            json!({})
        } else {
            json!({
                "rollout": config.rollout,
                "rolloutByGroup": config.rollout_by_group,
            })
        };
        let config_params = named_params! {
            ":id": format!("{}-{}", config.name, next_version.to_string()),
            ":name": config.name,
            ":type": "default",
            ":meta": meta,
            ":version": next_version,
            ":created_at": now,
            ":job_id": job.id,
        };

        let (_config, job_config_id): (JobConfig, String) =
            tx.query_row(&INSERT_JOB_CONFIG, config_params, |row| {
                let tasks = vec![];
                Ok((
                    JobConfig {
                        name: row.get(1)?,
                        rollout: config.rollout.clone(),
                        rollout_by_group: config.rollout_by_group.clone(),
                        version: Some(row.get(4)?),
                        tasks,
                    },
                    row.get(0)?,
                ))
            })?;

        let mut task_ids = Vec::new();
        for t in &config.tasks {
            let checks = serde_json::to_string(&t.checks)?;
            let on_unhealthy = serde_json::to_string(&t.on_unhealthy)?;
            let depends_on = serde_json::to_string(&t.depends_on)?;
            let kv_injections = t
                .kv_injections
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            let identities = if t.identities.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&t.identities)?)
            };
            let stateful = t.stateful.as_ref().map(serde_json::to_string).transpose()?;
            let schedule = t.schedule.as_ref().map(ToString::to_string);
            let task_params = named_params! {
                ":name": t.name,
                ":kind": t.kind,
                ":count": t.count.num,
                ":count_per": t.count.per,
                ":group_name": t.group,
                ":lifecycle": t.lifecycle.to_string(),
                ":schedule": schedule,
                ":depends_on": depends_on,
                ":config": t.config,
                ":resources": t.resources,
                ":constraints": t.constraints,
                ":permit": t.permit,
                ":affinities": t.affinities,
                ":checks": checks,
                ":on_unhealthy": on_unhealthy,
                ":kv_injections": kv_injections,
                ":job_id": job.id,
                ":job_config_id": job_config_id,
                ":identities": identities,
                ":stateful": stateful,
            };

            let task_id: i64 = tx.query_row(&INSERT_TASK, task_params, |row| row.get(0))?;
            task_ids.push(task_id);
        }

        for s in subs {
            let sub_hash = s.hash();

            let sub_params = named_params! {
                ":content": s.content,
                ":hash": sub_hash,
            };

            tx.execute(&INSERT_SUBMISSION, sub_params)?;

            let job_config_sub_params = named_params! {
                ":path": s.path,
                ":is_primary": s.is_primary,
                ":job_config_id": job_config_id,
                ":submission_hash": sub_hash,
            };

            tx.execute(&INSERT_JOB_CONFIG_SUBMISSION, job_config_sub_params)?;
        }

        let job_update_params = named_params! {
            ":id": format!("{}-{}", job.id, next_version.to_string()),
            ":status": "active",
            ":job_id": job.id,
            ":to_job_config_id": job_config_id,
            ":from_job_config_id": last_versions_id.clone(),
        };

        tx.execute(&INSERT_JOB_UPDATE, job_update_params)?;

        Self::insert_task_statuses(&tx, &task_ids, &job.id, &job_config_id)?;

        tx.commit()
            .map_err(|_| anyhow!("Error committing transaction"))?;

        let cached_tasks = self.get_task_configs_for_job_config(&job_config_id)?;
        self.cache_set_task_configs_for_job_config(&job_config_id, cached_tasks)?;

        if was_update {
            Ok(JobSubmitOutcome::Updated { job_config_id })
        } else {
            Ok(JobSubmitOutcome::Created { job_config_id })
        }
    }

    pub fn normalize_job_config(&self, config: &JobConfig) -> R<JobConfig> {
        let tasks = config
            .tasks
            .iter()
            .map(|task| {
                let resources = self
                    .resource_policy
                    .normalize_task_resources(&task.resources, true)?;
                Ok(TaskConfig {
                    resources,
                    ..task.clone()
                })
            })
            .collect::<R<Vec<_>>>()?;

        Ok(JobConfig {
            tasks,
            ..config.clone()
        })
    }

    #[instrument(err, skip_all)]
    pub fn previous_job_config_id(&self, job_config_id: &str) -> R<Option<String>> {
        let conn = self.read_connection()?;
        let previous = conn
            .query_row(&GET_JOB_UPDATE_FOR_CONFIG, [job_config_id], |row| {
                row.get::<_, Option<String>>(4)
            })
            .optional()?;
        Ok(previous.flatten())
    }

    #[instrument(err, skip_all)]
    pub fn create_rollout(
        &self,
        job_id: &str,
        from_job_config_id: Option<&str>,
        to_job_config_id: &str,
        status: &str,
        strategy: Option<&str>,
        progression: Option<&str>,
        replacement: Option<&str>,
        in_place: Option<bool>,
        config_json: &str,
    ) -> R<Rollout> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        let in_place = in_place.map(i64::from);
        let rollout = conn.query_row(
            "INSERT INTO rollout (
                job_id,
                from_job_config_id,
                to_job_config_id,
                status,
                strategy,
                progression,
                replacement,
                in_place,
                config_json,
                created_at,
                updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)
            RETURNING id, job_id, from_job_config_id, to_job_config_id, status, incomplete,
                      message, strategy, progression, replacement, in_place, config_json,
                      created_at, updated_at, completed_at;",
            params![
                job_id,
                from_job_config_id,
                to_job_config_id,
                status,
                strategy,
                progression,
                replacement,
                in_place,
                config_json,
                now
            ],
            |row| self.rollout_from_row(row),
        )?;

        Ok(rollout)
    }

    pub fn list_rollouts(&self, job_id: Option<&str>, active_only: bool) -> R<Vec<Rollout>> {
        let conn = self.read_connection()?;
        let base_sql = "SELECT id, job_id, from_job_config_id, to_job_config_id, status,
                incomplete, message, strategy, progression, replacement, in_place,
                config_json, created_at, updated_at, completed_at
            FROM rollout";

        let sql = match (job_id, active_only) {
            (Some(_), true) => {
                format!(
                    "{base_sql} WHERE job_id = ?1 AND status NOT IN ('completed','failed','cancelled','completed_incomplete') ORDER BY id DESC;"
                )
            }
            (Some(_), false) => format!("{base_sql} WHERE job_id = ?1 ORDER BY id DESC;"),
            (None, true) => format!(
                "{base_sql} WHERE status NOT IN ('completed','failed','cancelled','completed_incomplete') ORDER BY id DESC;"
            ),
            (None, false) => format!("{base_sql} ORDER BY id DESC;"),
        };

        let mut stmt = conn.prepare(&sql)?;
        if let Some(job_id) = job_id {
            let rows = stmt.query_map([job_id], |row| self.rollout_from_row(row))?;
            Self::collect_rows(rows)
        } else {
            let rows = stmt.query_map([], |row| self.rollout_from_row(row))?;
            Self::collect_rows(rows)
        }
    }

    pub fn get_rollout(&self, rollout_id: i64) -> R<Option<Rollout>> {
        let conn = self.read_connection()?;
        let rollout = conn
            .query_row(
                "SELECT id, job_id, from_job_config_id, to_job_config_id, status,
                        incomplete, message, strategy, progression, replacement, in_place,
                        config_json, created_at, updated_at, completed_at
                 FROM rollout
                 WHERE id = ?;",
                [rollout_id],
                |row| self.rollout_from_row(row),
            )
            .optional()?;

        Ok(rollout)
    }

    pub fn latest_rollout_for_job_config(&self, job_config_id: &str) -> R<Option<Rollout>> {
        let conn = self.read_connection()?;
        let rollout = conn
            .query_row(
                "SELECT id, job_id, from_job_config_id, to_job_config_id, status,
                        incomplete, message, strategy, progression, replacement, in_place,
                        config_json, created_at, updated_at, completed_at
                 FROM rollout
                 WHERE to_job_config_id = ?
                 ORDER BY id DESC
                 LIMIT 1;",
                [job_config_id],
                |row| self.rollout_from_row(row),
            )
            .optional()?;

        Ok(rollout)
    }

    #[instrument(err, skip_all)]
    pub fn update_rollout_status(
        &self,
        rollout_id: i64,
        status: &str,
        incomplete: bool,
        message: Option<&str>,
    ) -> R<()> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        let completed_at = match status {
            "completed" | "failed" | "cancelled" | "completed_incomplete" => Some(now.clone()),
            _ => None,
        };
        conn.execute(
            "UPDATE rollout
             SET status = ?1, incomplete = ?2, message = COALESCE(?3, message),
                 updated_at = ?4, completed_at = ?5
             WHERE id = ?6;",
            params![
                status,
                i64::from(incomplete),
                message,
                now,
                completed_at,
                rollout_id
            ],
        )?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn update_rollout_config_json(&self, rollout_id: i64, config_json: &str) -> R<()> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        conn.execute(
            "UPDATE rollout
             SET config_json = ?1, updated_at = ?2
             WHERE id = ?3;",
            params![config_json, now, rollout_id],
        )?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn append_rollout_event(
        &self,
        rollout_id: i64,
        job_id: &str,
        group_name: Option<&str>,
        level: &str,
        message: &str,
        metadata_json: Option<&str>,
    ) -> R<RolloutEventRecord> {
        let conn = self.write_connection()?;
        let created_at = Self::now_rfc3339()?;
        let metadata_json = metadata_json.unwrap_or("{}");
        if metadata_json.trim().is_empty() {
            return Err(anyhow::anyhow!(
                "rollout event metadata_json cannot be empty"
            ));
        }
        let _: Value = serde_json::from_str(metadata_json)
            .with_context(|| format!("invalid rollout event metadata_json: {metadata_json}"))?;

        let event = conn.query_row(
            "INSERT INTO rollout_event (
                rollout_id,
                job_id,
                group_name,
                level,
                message,
                metadata_json,
                created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            RETURNING id, rollout_id, job_id, group_name, level, message, metadata_json, created_at;",
            params![
                rollout_id,
                job_id,
                group_name,
                level,
                message,
                metadata_json,
                created_at
            ],
            |row| self.rollout_event_from_row(row),
        )?;

        Ok(event)
    }

    #[instrument(err, skip_all)]
    pub fn list_rollout_events(
        &self,
        rollout_id: i64,
        limit: Option<u32>,
    ) -> R<Vec<RolloutEventRecord>> {
        let conn = self.read_connection()?;
        let requested = limit.unwrap_or(500).clamp(1, 2_000);
        let mut stmt = conn.prepare(
            "SELECT id, rollout_id, job_id, group_name, level, message, metadata_json, created_at
             FROM (
                SELECT id, rollout_id, job_id, group_name, level, message, metadata_json, created_at
                FROM rollout_event
                WHERE rollout_id = ?1
                ORDER BY id DESC
                LIMIT ?2
             )
             ORDER BY id ASC;",
        )?;
        let rows = stmt.query_map(params![rollout_id, requested as i64], |row| {
            self.rollout_event_from_row(row)
        })?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all)]
    pub fn prune_rollouts(&self) -> R<u64> {
        let conn = self.write_connection()?;
        let cutoff =
            OffsetDateTime::now_utc() - TimeDuration::days(self.rollout_retention_days as i64);
        let cutoff = cutoff
            .format(&Rfc3339)
            .context("failed to format rollout retention cutoff")?;
        let changed = conn.execute(
            "DELETE FROM rollout WHERE completed_at IS NOT NULL AND completed_at < ?;",
            [cutoff],
        )?;
        Ok(changed as u64)
    }

    #[instrument(err, skip_all)]
    pub fn job_configs_schedule_equivalent(&self, from_id: &str, to_id: &str) -> R<bool> {
        let from_tasks = self.get_task_configs_for_job_config(from_id)?;
        let to_tasks = self.get_task_configs_for_job_config(to_id)?;

        if from_tasks.len() != to_tasks.len() {
            return Ok(false);
        }

        let to_map = to_tasks
            .into_iter()
            .map(|(_, task)| (task.name.clone(), task))
            .collect::<HashMap<_, _>>();

        for (_, task) in from_tasks {
            let Some(to_task) = to_map.get(&task.name) else {
                return Ok(false);
            };
            if !task_schedule_equivalent(&task, to_task) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    #[instrument(err, skip_all)]
    pub fn migrate_allocations_to_job_config(
        &self,
        from_job_config_id: &str,
        to_job_config_id: &str,
    ) -> R<Vec<Allocation>> {
        let from_tasks = self.get_task_configs_for_job_config(from_job_config_id)?;
        let to_tasks = self.get_task_configs_for_job_config(to_job_config_id)?;
        let to_map = to_tasks
            .into_iter()
            .map(|(id, task)| (task.name.clone(), id))
            .collect::<HashMap<_, _>>();

        let mut task_id_map = HashMap::new();
        for (old_id, task) in from_tasks {
            let new_id = to_map
                .get(&task.name)
                .copied()
                .with_context(|| format!("missing updated task config for {}", task.name))?;
            task_id_map.insert(old_id, new_id);
        }

        let allocations = self.allocs_for_job_config(from_job_config_id)?;
        if allocations.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        for allocation in &allocations {
            let new_task_id = task_id_map
                .get(&allocation.task_config_id)
                .copied()
                .context("missing task id mapping for allocation")?;
            tx.execute(
                "UPDATE allocation SET job_config_id = ?, task_config_id = ? WHERE id = ?;",
                (to_job_config_id, new_task_id, allocation.id),
            )?;
        }
        let updated_allocations = allocations
            .into_iter()
            .map(|mut allocation| {
                let new_task_id = task_id_map
                    .get(&allocation.task_config_id)
                    .copied()
                    .unwrap_or(allocation.task_config_id);
                allocation.job_config_id = to_job_config_id.to_string();
                allocation.task_config_id = new_task_id;
                allocation
            })
            .collect::<Vec<_>>();

        // Allocation migration mutates allocation ownership and must invalidate
        // plan submissions for both source and destination job configs.
        self.bump_job_config_epoch_tx(&tx, from_job_config_id)?;
        self.bump_epochs_for_allocations_tx(&tx, &updated_allocations)?;
        tx.commit()?;

        self.cache_remove_allocations(&updated_allocations)?;
        self.cache_add_allocations(&updated_allocations)?;

        Ok(updated_allocations)
    }

    #[instrument(err, skip_all)]
    pub fn get_submissions_for_config(&self, job_config_id: &str) -> R<Vec<Submission>> {
        let conn = self.read_connection()?;

        let sql = r#"
        SELECT jcs.path, jcs.is_primary, s.content
        FROM submission s
        INNER JOIN job_config_submission jcs ON jcs.submission_hash = s.hash
        WHERE jcs.job_config_id = ?;
"#;
        let mut stmt = conn.prepare(sql)?;
        let subs = Self::collect_rows(stmt.query_map([job_config_id], |row| {
            Ok(Submission {
                path: row.get(0)?,
                is_primary: row.get(1)?,
                content: row.get(2)?,
            })
        })?)?;

        Ok(subs)
    }

    #[instrument(err, skip_all)]
    pub fn get_task_instances_for_job_config(&self, job_config_id: &str) -> R<Vec<(i64, u32)>> {
        let conn = self.read_connection()?;
        let sql = r#"
        SELECT id, count
        FROM task_config
        WHERE job_config_id = ?;
        "#;
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([job_config_id], |row| {
            let id: i64 = row.get(0)?;
            let count: u32 = row.get::<_, i64>(1)? as u32;
            Ok((id, count))
        })?;

        let mut task_instances = Vec::new();
        for row in rows {
            task_instances.push(row?);
        }

        Ok(task_instances)
    }

    #[instrument(err, skip_all)]
    pub fn get_task_configs_for_job_config(
        &self,
        job_config_id: &str,
    ) -> R<Vec<(i64, TaskConfig)>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&GET_TASK_FOR_JOB_CONIFG)?;
        let rows = stmt.query_map([job_config_id], |row| {
            let id = row.get(0)?;
            let task = self.task_config_from_row(row)?;
            Ok((id, task))
        })?;

        Self::collect_rows(rows)
    }

    pub fn task_statuses_for_job_config(
        &self,
        job_config_id: &str,
    ) -> R<HashMap<i64, TaskStatusState>> {
        let conn = self.read_connection()?;
        let mut stmt = conn
            .prepare("SELECT task_config_id, status FROM task_status WHERE job_config_id = ?;")?;
        let rows = stmt.query_map([job_config_id], |row| {
            let task_id: i64 = row.get(0)?;
            let status: String = row.get(1)?;
            let status = status.parse::<TaskStatusState>().map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
                )
            })?;
            Ok((task_id, status))
        })?;

        let mut statuses = HashMap::new();
        for row in rows {
            let (task_id, status) = row?;
            statuses.insert(task_id, status);
        }
        Ok(statuses)
    }

    pub fn set_task_status(&self, task_config_id: i64, status: TaskStatusState) -> R<()> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        conn.execute(
            "UPDATE task_status SET status = ?, updated_at = ? WHERE task_config_id = ?;",
            params![status.as_str(), now, task_config_id],
        )?;
        Ok(())
    }

    fn task_status_for_lifecycle(lifecycle: &TaskLifecycle) -> TaskStatusState {
        match lifecycle {
            TaskLifecycle::Batch => TaskStatusState::Success,
            TaskLifecycle::Service | TaskLifecycle::Sidecar => TaskStatusState::Running,
        }
    }

    fn apply_task_status_updates_tx(
        &self,
        tx: &Transaction<'_>,
        updates: &HashMap<i64, TaskStatusState>,
    ) -> R<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let now = Self::now_rfc3339()?;
        let mut update_stmt = tx.prepare(
            "UPDATE task_status SET status = ?1, updated_at = ?2 WHERE task_config_id = ?3;",
        )?;
        for (task_config_id, status) in updates {
            update_stmt.execute(params![status.as_str(), now, task_config_id])?;
        }
        Ok(())
    }

    pub fn update_task_statuses_for_allocations(&self, allocations: &[Allocation]) -> R<bool> {
        if allocations.is_empty() {
            return Ok(false);
        }

        let tx_started = Instant::now();
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let mut task_cache: HashMap<String, HashMap<i64, TaskConfig>> = HashMap::new();
        let mut updates: HashMap<i64, TaskStatusState> = HashMap::new();

        for allocation in allocations {
            let tasks = task_cache
                .entry(allocation.job_config_id.clone())
                .or_insert_with(HashMap::new);
            if tasks.is_empty() {
                let configs = self.get_task_configs_for_job_config(&allocation.job_config_id)?;
                for (id, config) in configs {
                    tasks.insert(id, config);
                }
            }
            let task = tasks
                .get(&allocation.task_config_id)
                .with_context(|| format!("Missing task config {}", allocation.task_config_id))?;

            updates.insert(
                allocation.task_config_id,
                Self::task_status_for_lifecycle(&task.lifecycle),
            );
        }

        self.apply_task_status_updates_tx(&tx, &updates)?;
        tx.commit()?;
        debug!(
            allocation_count = allocations.len(),
            updated_tasks = updates.len(),
            duration_ms = tx_started.elapsed().as_millis(),
            "Updated task statuses for allocations"
        );

        Ok(!updates.is_empty())
    }

    //
    // ALLOCS
    //

    #[instrument(err, skip_all)]
    pub fn place_alloc(&self, node_id: &str, task_id: i64) -> R<Allocation> {
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let allocation = self.insert_allocation(&tx, node_id, task_id)?;
        self.bump_epochs_for_allocations_tx(&tx, std::slice::from_ref(&allocation))?;
        tx.commit()?;

        self.cache_add_allocations(std::slice::from_ref(&allocation))?;

        Ok(allocation)
    }

    // NOTE: this helper only inserts allocation rows; callers must bump
    // job-config epochs for affected configs before committing the tx.
    fn insert_allocation(
        &self,
        tx: &Transaction<'_>,
        node_id: &str,
        task_id: i64,
    ) -> R<Allocation> {
        // This helper writes a single allocation row. Callers mutating allocation state
        // must call bump_epochs_for_allocations_tx (or bump_job_config_epoch_tx)
        // in the same transaction before commit.
        let (job_config_id, resources): (String, Value) = tx.query_row(
            "SELECT job_config_id, resources FROM task_config WHERE id = ?;",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        self.insert_allocation_with_data(tx, node_id, task_id, &job_config_id, &resources)
    }

    fn insert_allocation_with_data(
        &self,
        tx: &Transaction<'_>,
        node_id: &str,
        task_id: i64,
        job_config_id: &str,
        resources: &Value,
    ) -> R<Allocation> {
        // Stateful tasks: pick (or re-materialize) the instance this
        // allocation will fulfill before writing the row, so the binding is
        // atomic with the placement.
        let instance_id = self.assign_stateful_instance_tx(tx, task_id)?;

        let params = named_params! {
            ":status": "placed",
            ":resources": resources,
            ":node_id": node_id,
            ":job_config_id": job_config_id,
            ":task_config_id": task_id,
            ":instance_id": instance_id,
        };

        let allocation = tx.query_row(&INSERT_ALLOCATION, params, |row| {
            self.allocation_from_row(row)
        })?;

        if let Some(instance_id) = allocation.instance_id.as_deref() {
            let now = Self::now_rfc3339()?;
            tx.execute(
                "UPDATE instance SET state = 'active', current_alloc_id = ?1, updated_at = ?2 \
                 WHERE instance_id = ?3;",
                params![allocation.id, now, instance_id],
            )?;
        }

        Ok(allocation)
    }

    /// For a stateful task, pick the instance the new allocation will fulfill.
    /// This is the single choke point all placement paths flow through, so the
    /// instance binding is atomic with the allocation insert. Returns `None`
    /// for stateless tasks. Must run inside the same transaction that inserts
    /// the allocation.
    ///
    /// - Slot mode: the lowest slot `0..count.num-1` not currently fulfilled
    ///   by an allocation of this task config; the instance row is created or
    ///   re-activated as needed (slot ids are reused within the pool).
    /// - Keyed mode: any live (non-terminated) keyed instance of this
    ///   (job, group, task) not currently bound to an allocation of this task
    ///   config. Instances only come into existence via CreateInstance; this
    ///   never creates one. Keyed ids are not ordinals, so "any unbound" is
    ///   correct — a rescheduling instance is simply the unbound one.
    fn assign_stateful_instance_tx(&self, tx: &Transaction<'_>, task_id: i64) -> R<Option<String>> {
        let (job_id, group, task_name, count, stateful): (
            String,
            String,
            String,
            i64,
            Option<String>,
        ) = tx.query_row(
            "SELECT job_id, group_name, name, count, stateful FROM task_config WHERE id = ?;",
            [task_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;

        let Some(stateful) = stateful else {
            return Ok(None);
        };
        let stateful: StatefulConfig = serde_json::from_str(&stateful)
            .with_context(|| format!("invalid stateful config for task {task_name}"))?;
        if stateful.mode == InstanceMode::Keyed {
            return self
                .assign_keyed_instance_tx(tx, task_id, &job_id, &group, &task_name, &stateful)
                .map(Some);
        }

        // Slots already fulfilled by live allocations of this task config.
        // Scoped per config so a create-first rollout's new allocations can
        // take over instances still referenced by the outgoing config.
        let mut taken_stmt = tx.prepare(
            "SELECT instance.slot_index FROM allocation \
             INNER JOIN instance ON instance.instance_id = allocation.instance_id \
             WHERE allocation.task_config_id = ?1 AND allocation.instance_id IS NOT NULL;",
        )?;
        let taken: HashSet<u32> = Self::collect_rows(
            taken_stmt.query_map([task_id], |row| row.get::<_, Option<i64>>(0))?,
        )?
        .into_iter()
        .flatten()
        .map(|slot| slot as u32)
        .collect();

        let desired = count.max(0) as u32;
        let slot = (0..desired)
            .find(|slot| !taken.contains(slot))
            .with_context(|| {
                format!(
                "no free slot for stateful task {task_name} (group {group}); all {desired} slots \
                 already have allocations for this job config"
            )
            })?;

        let instance_id = slot_instance_id(&job_id, &group, slot);
        let now = Self::now_rfc3339()?;
        let existing = tx
            .query_row(&GET_INSTANCE, [instance_id.as_str()], |row| {
                self.instance_from_row(row)
            })
            .optional()?;

        match existing {
            Some(existing) => {
                // Re-activation handles both reschedule (Rescheduling -> Active
                // via the caller's binding update) and scale-out of a retained
                // Terminated slot: the same id and storage handle come back.
                // Mint a handle only if none was retained and the task (still)
                // declares storage.
                let storage_handle = existing
                    .storage_handle
                    .or_else(|| stateful.storage.as_ref().map(|_| mint_storage_handle()));
                tx.execute(
                    "UPDATE instance SET task_name = ?1, storage_handle = ?2, updated_at = ?3 \
                     WHERE instance_id = ?4;",
                    params![task_name, storage_handle, now, instance_id],
                )?;
            }
            None => {
                let storage_handle = stateful.storage.as_ref().map(|_| mint_storage_handle());
                tx.execute(
                    "INSERT INTO instance (instance_id, job_id, group_name, task_name, mode, \
                     state, slot_index, current_alloc_id, storage_handle, created_at, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, 'slot', 'pending', ?5, NULL, ?6, ?7, ?7);",
                    params![
                        instance_id,
                        job_id,
                        group,
                        task_name,
                        slot,
                        storage_handle,
                        now
                    ],
                )?;
            }
        }

        Ok(Some(instance_id))
    }

    /// Keyed-mode arm of [`Self::assign_stateful_instance_tx`]: bind the new
    /// allocation to an unbound live keyed instance of (job, group, task).
    ///
    /// "Unbound" is scoped per task config (like slots) so a create-first
    /// rollout's new allocations can take over instances still referenced by
    /// the outgoing config. The oldest unbound instance is picked for
    /// determinism; since keyed ids are not ordinals, any unbound instance is
    /// a correct choice. A storage handle is minted on first placement when
    /// the task declares `stateful.storage`, and the retained handle is
    /// reattached on every re-bind — that is what makes a replacement
    /// allocation *the same sandbox*.
    fn assign_keyed_instance_tx(
        &self,
        tx: &Transaction<'_>,
        task_id: i64,
        job_id: &str,
        group: &str,
        task_name: &str,
        stateful: &StatefulConfig,
    ) -> R<String> {
        let instance: Option<Instance> = tx
            .query_row(
                "SELECT instance_id, job_id, group_name, task_name, mode, state, slot_index, \
                 current_alloc_id, storage_handle, created_at, updated_at FROM instance \
                 WHERE job_id = ?1 AND group_name = ?2 AND task_name = ?3 \
                 AND mode = 'keyed' AND state != 'terminated' \
                 AND instance_id NOT IN (\
                     SELECT instance_id FROM allocation \
                     WHERE task_config_id = ?4 AND instance_id IS NOT NULL\
                 ) \
                 ORDER BY created_at ASC, instance_id ASC LIMIT 1;",
                params![job_id, group, task_name, task_id],
                |row| self.instance_from_row(row),
            )
            .optional()?;

        let instance = instance.with_context(|| {
            format!(
                "no unbound keyed instance for task {task_name} (group {group}, job {job_id}); \
                 keyed allocations are driven by CreateInstance, not count"
            )
        })?;

        if instance.storage_handle.is_none() && stateful.storage.is_some() {
            let now = Self::now_rfc3339()?;
            tx.execute(
                "UPDATE instance SET storage_handle = ?1, updated_at = ?2 WHERE instance_id = ?3;",
                params![mint_storage_handle(), now, instance.instance_id],
            )?;
        }

        Ok(instance.instance_id)
    }

    /// After allocation rows are deleted, transition the instances they
    /// fulfilled. Instances still referenced by another live allocation (for
    /// example the already-placed replacement during a config update) keep
    /// running and just re-point `current_alloc_id`. Orphaned slot instances
    /// become `rescheduling` while their slot is still desired, or
    /// `terminated` when the latest job config no longer wants the slot
    /// (scale-in) — retaining the row, and the storage handle unless
    /// `retain_on_scale_in` is false. Orphaned keyed instances always become
    /// `rescheduling`: allocation churn never terminates a keyed instance,
    /// only DestroyInstance (or job stop) does. Must run in the same
    /// transaction as the deletion, before commit.
    fn release_instance_bindings_tx(&self, tx: &Transaction<'_>, removed: &[Allocation]) -> R<()> {
        let instance_ids: HashSet<&str> = removed
            .iter()
            .filter_map(|alloc| alloc.instance_id.as_deref())
            .collect();
        if instance_ids.is_empty() {
            return Ok(());
        }

        let now = Self::now_rfc3339()?;
        for instance_id in instance_ids {
            let surviving_alloc: Option<i64> = tx
                .query_row(
                    "SELECT MAX(id) FROM allocation WHERE instance_id = ?;",
                    [instance_id],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();

            if let Some(alloc_id) = surviving_alloc {
                tx.execute(
                    "UPDATE instance SET current_alloc_id = ?1, updated_at = ?2 \
                     WHERE instance_id = ?3 AND current_alloc_id IS NOT ?1;",
                    params![alloc_id, now, instance_id],
                )?;
                continue;
            }

            let instance = tx
                .query_row(&GET_INSTANCE, [instance_id], |row| {
                    self.instance_from_row(row)
                })
                .optional()?;
            let Some(instance) = instance else {
                continue;
            };
            if instance.state == InstanceState::Terminated {
                continue;
            }

            let (still_desired, retain_storage) = self.slot_still_desired_tx(tx, &instance)?;
            if still_desired {
                tx.execute(
                    "UPDATE instance SET state = 'rescheduling', current_alloc_id = NULL, \
                     updated_at = ?1 WHERE instance_id = ?2;",
                    params![now, instance_id],
                )?;
            } else {
                let storage_handle = if retain_storage {
                    instance.storage_handle.clone()
                } else {
                    None
                };
                info!(
                    instance_id,
                    job_id = %instance.job_id,
                    retain_storage,
                    "Terminating scaled-in instance"
                );
                tx.execute(
                    "UPDATE instance SET state = 'terminated', current_alloc_id = NULL, \
                     storage_handle = ?1, updated_at = ?2 WHERE instance_id = ?3;",
                    params![storage_handle, now, instance_id],
                )?;
            }
        }

        Ok(())
    }

    /// Whether the instance's slot is still inside the latest job config's
    /// desired range, plus the effective storage retention policy. Keyed
    /// instances are always "still desired" here — their lifecycle is owned by
    /// the management API (DestroyInstance), never by allocation churn.
    fn slot_still_desired_tx(&self, tx: &Transaction<'_>, instance: &Instance) -> R<(bool, bool)> {
        let (InstanceMode::Slot, Some(slot)) = (instance.mode, instance.slot_index) else {
            return Ok((true, true));
        };

        let latest_task: Option<(i64, Option<String>)> = tx
            .query_row(
                "SELECT task_config.count, task_config.stateful FROM task_config \
                 INNER JOIN job_config ON job_config.id = task_config.job_config_id \
                 WHERE task_config.job_id = ?1 AND task_config.group_name = ?2 \
                 AND task_config.name = ?3 \
                 ORDER BY job_config.version DESC LIMIT 1;",
                params![instance.job_id, instance.group, instance.task_name],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        let Some((count, stateful)) = latest_task else {
            // Task disappeared from the job entirely: terminate, retain storage.
            return Ok((false, true));
        };

        let retain = stateful
            .as_deref()
            .and_then(|raw| serde_json::from_str::<StatefulConfig>(raw).ok())
            .and_then(|stateful| stateful.storage)
            .map(|storage| storage.retains_on_scale_in())
            .unwrap_or(true);

        Ok(((slot as i64) < count, retain))
    }

    fn load_allocation_constraints(
        &self,
        tx: &Transaction<'_>,
        task_id: i64,
    ) -> R<AllocationConstraintState> {
        let (constraints, permit): (Value, Value) = tx.query_row(
            "SELECT constraints, permit FROM task_config WHERE id = ?;",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let constraint_set = ConstraintSet::new(&constraints)
            .with_context(|| format!("Failed to parse constraints for task {task_id}"))?;
        let permit_set = PermitSet::new(&permit)
            .with_context(|| format!("Failed to parse permit for task {task_id}"))?;
        let distinct_keys = constraint_set.distinct_keys();
        let distinct = if distinct_keys.is_empty() {
            None
        } else {
            let mut tracker = DistinctTracker::new(&distinct_keys);
            let sql = "SELECT node.id, node.pool, node.status, node.availability, \
                node.config_attrs, node.fingerprint_attrs, node.dynamic_attrs, \
                node.health_checks, node.last_heartbeat \
                FROM node INNER JOIN allocation ON allocation.node_id = node.id \
                WHERE allocation.task_config_id = ?;";
            let mut stmt = tx.prepare(sql)?;
            let rows = stmt.query_map([task_id], |row| self.node_from_row(row))?;
            for row in rows {
                tracker.insert_from_node(&row?);
            }
            Some(tracker)
        };

        Ok(AllocationConstraintState {
            constraints: constraint_set,
            permit: permit_set,
            distinct,
        })
    }

    fn load_allocation_task_state(
        &self,
        tx: &Transaction<'_>,
        task_id: i64,
    ) -> R<AllocationTaskState> {
        let (job_config_id, resources, constraints, permit, lifecycle): (
            String,
            Value,
            Value,
            Value,
            String,
        ) = tx.query_row(
            "SELECT job_config_id, resources, constraints, permit, lifecycle FROM task_config WHERE id = ?;",
            [task_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;

        let constraint_set = ConstraintSet::new(&constraints)
            .with_context(|| format!("Failed to parse constraints for task {task_id}"))?;
        let permit_set = PermitSet::new(&permit)
            .with_context(|| format!("Failed to parse permit for task {task_id}"))?;
        let distinct_keys = constraint_set.distinct_keys();
        let distinct = if distinct_keys.is_empty() {
            None
        } else {
            let mut tracker = DistinctTracker::new(&distinct_keys);
            let sql = "SELECT node.id, node.pool, node.status, node.availability, \
                node.config_attrs, node.fingerprint_attrs, node.dynamic_attrs, \
                node.health_checks, node.last_heartbeat \
                FROM node INNER JOIN allocation ON allocation.node_id = node.id \
                WHERE allocation.task_config_id = ?;";
            let mut stmt = tx.prepare(sql)?;
            let rows = stmt.query_map([task_id], |row| self.node_from_row(row))?;
            for row in rows {
                tracker.insert_from_node(&row?);
            }
            Some(tracker)
        };

        Ok(AllocationTaskState {
            constraints: constraint_set,
            permit: permit_set,
            distinct,
            job_config_id,
            resources,
            lifecycle: lifecycle.parse().map_err(|err: String| anyhow!(err))?,
        })
    }

    #[instrument(err, skip_all)]
    pub fn apply_allocation_plan(&self, placements: &[(String, i64)]) -> R<Vec<Allocation>> {
        if placements.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let mut allocations = Vec::new();

        let mut constraint_states: HashMap<i64, AllocationConstraintState> = HashMap::new();

        for placement in placements {
            let (node_id, task_id) = (&placement.0, placement.1);
            let state = match constraint_states.entry(task_id) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert(self.load_allocation_constraints(&tx, task_id)?)
                }
            };

            let node = tx.query_row(&GET_NODE, [node_id], |row| self.node_from_row(row))?;
            if !state
                .constraints
                .is_satisfied(&node, state.distinct.as_ref())
                || !state.permit.is_permitted(&node)
            {
                bail!(
                    "Node {} does not satisfy constraints for task {}",
                    node_id,
                    task_id
                );
            }

            if let Some(tracker) = state.distinct.as_mut() {
                tracker.insert_from_node(&node);
            }

            let allocation = self.insert_allocation(&tx, node_id, task_id)?;
            allocations.push(allocation);
        }

        self.bump_epochs_for_allocations_tx(&tx, &allocations)?;
        tx.commit()?;

        self.cache_add_allocations(&allocations)?;

        Ok(allocations)
    }

    #[instrument(err, skip_all)]
    pub fn count_allocations_for_task(&self, task_id: i64) -> R<u32> {
        let conn = self.read_connection()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM allocation WHERE task_config_id = ?;",
            [task_id],
            |row| row.get(0),
        )?;

        Ok(count as u32)
    }

    #[instrument(err, skip_all)]
    pub fn allocs_for_job(&self, job_id: &str) -> R<Vec<Allocation>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_ALLOCS_FOR_JOB.as_str())?;
        let alloc_iter = stmt.query_map([job_id], |row| self.allocation_from_row(row))?;

        let mut allocs = Vec::new();
        for alloc in alloc_iter {
            allocs.push(alloc?);
        }

        Ok(allocs)
    }

    #[instrument(err, skip_all)]
    pub fn allocs_for_job_config(&self, job_config_id: &str) -> R<Vec<Allocation>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_ALLOCS_FOR_JOB_CONFIG.as_str())?;
        let alloc_iter = stmt.query_map([job_config_id], |row| self.allocation_from_row(row))?;

        let mut allocs = Vec::new();
        for alloc in alloc_iter {
            allocs.push(alloc?);
        }

        Ok(allocs)
    }

    #[instrument(err, skip_all)]
    pub fn allocs_for_node(&self, node_id: &str) -> R<Vec<Allocation>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_ALLOCS_FOR_NODE.as_str())?;
        let alloc_iter = stmt.query_map([node_id], |row| self.allocation_from_row(row))?;

        let mut allocs = Vec::new();
        for alloc in alloc_iter {
            allocs.push(alloc?);
        }

        Ok(allocs)
    }

    #[instrument(err, skip_all)]
    pub fn get_allocation_by_id(&self, allocation_id: i64) -> R<Option<Allocation>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_ALLOCATION_BY_ID.as_str())?;
        let allocation = stmt
            .query_row([allocation_id], |row| self.allocation_from_row(row))
            .optional()?;
        Ok(allocation)
    }

    #[instrument(err, skip_all)]
    pub fn upsert_allocation_health_snapshots(
        &self,
        node_id: &str,
        snapshots: &[AllocationHealth],
    ) -> R<()> {
        if snapshots.is_empty() {
            return Ok(());
        }

        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let now = Self::now_rfc3339()?;
        for snapshot in snapshots {
            tx.execute(
                "INSERT INTO allocation_health (
                    allocation_id,
                    node_id,
                    healthy,
                    status,
                    updated_at
                )
                SELECT ?1, ?2, ?3, ?4, ?5
                WHERE EXISTS (SELECT 1 FROM allocation WHERE id = ?1 AND node_id = ?2)
                ON CONFLICT(allocation_id) DO UPDATE
                SET node_id = excluded.node_id,
                    healthy = excluded.healthy,
                    status = excluded.status,
                    updated_at = excluded.updated_at;",
                params![
                    snapshot.allocation_id,
                    node_id,
                    i64::from(snapshot.healthy),
                    snapshot.status.as_str(),
                    now.as_str()
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn reconcile_incomplete_rollouts(&self) -> R<u64> {
        let mut reconciled = 0_u64;
        for job_config_id in self.job_config_ids_with_incomplete_rollouts()? {
            match self.reconcile_job_config_allocations(&job_config_id) {
                Ok(()) => {
                    reconciled += 1;
                }
                Err(err) => {
                    warn!(
                        job_config_id = %job_config_id,
                        error = ?err,
                        "failed reconciling completed_incomplete rollout"
                    );
                }
            }
        }
        Ok(reconciled)
    }

    #[instrument(err, skip_all)]
    pub fn allocation_health_for_allocations(
        &self,
        allocation_ids: &[i64],
    ) -> R<HashMap<i64, AllocationHealthSnapshot>> {
        if allocation_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let placeholders = std::iter::repeat("?")
            .take(allocation_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT allocation_id, node_id, healthy, status, updated_at
             FROM allocation_health
             WHERE allocation_id IN ({placeholders});"
        );
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(allocation_ids.iter()), |row| {
            self.allocation_health_snapshot_from_row(row)
        })?;
        let snapshots = Self::collect_rows(rows)?;
        Ok(snapshots
            .into_iter()
            .map(|snapshot| (snapshot.allocation_id, snapshot))
            .collect())
    }

    #[instrument(err, skip_all)]
    pub fn allocation_health_for_job_config(
        &self,
        job_config_id: &str,
    ) -> R<HashMap<i64, AllocationHealthSnapshot>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT ah.allocation_id, ah.node_id, ah.healthy, ah.status, ah.updated_at
             FROM allocation_health ah
             INNER JOIN allocation a ON a.id = ah.allocation_id
             WHERE a.job_config_id = ?1;",
        )?;
        let rows = stmt.query_map([job_config_id], |row| {
            self.allocation_health_snapshot_from_row(row)
        })?;
        let snapshots = Self::collect_rows(rows)?;
        Ok(snapshots
            .into_iter()
            .map(|snapshot| (snapshot.allocation_id, snapshot))
            .collect())
    }

    #[instrument(err, skip_all)]
    pub fn remove_allocation_by_id(&self, allocation_id: i64) -> R<Option<Allocation>> {
        let allocation = self.get_allocation_by_id(allocation_id)?;
        let Some(allocation) = allocation else {
            return Ok(None);
        };

        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM allocation WHERE id = ?;", [allocation_id])?;
        self.release_instance_bindings_tx(&tx, std::slice::from_ref(&allocation))?;
        self.bump_job_config_epoch_tx(&tx, &allocation.job_config_id)?;
        tx.commit()?;

        self.cache_remove_allocations(std::slice::from_ref(&allocation))?;

        Ok(Some(allocation))
    }

    #[instrument(err, skip_all)]
    pub fn get_allocs(&self) -> R<Vec<Allocation>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_ALLOCS.as_str())?;
        let alloc_iter = stmt.query_map([], |row| self.allocation_from_row(row))?;

        let mut allocs = Vec::new();
        for alloc in alloc_iter {
            allocs.push(alloc?);
        }

        Ok(allocs)
    }

    #[instrument(err, skip_all)]
    pub fn get_allocs_by_node(&self) -> R<HashMap<String, Vec<Allocation>>> {
        let allocs = self.get_allocs()?;
        let mut by_node: HashMap<String, Vec<Allocation>> = HashMap::new();

        for alloc in allocs {
            by_node
                .entry(alloc.node_id.clone())
                .or_default()
                .push(alloc);
        }

        Ok(by_node)
    }

    #[instrument(err, skip_all)]
    pub fn remove_allocs_for_job(&self, job_id: &str) -> R<()> {
        let allocations = self.allocs_for_job(job_id)?;
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let sql = r#"
        DELETE FROM allocation
        WHERE job_config_id IN (
            SELECT id FROM job_config WHERE job_id = :job_id
        );
        "#;

        let params = named_params! { ":job_id": job_id };

        tx.execute(sql, params)?;
        self.release_instance_bindings_tx(&tx, &allocations)?;
        self.bump_epochs_for_allocations_tx(&tx, &allocations)?;
        tx.commit()?;

        self.cache_remove_allocations(&allocations)?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn remove_allocs_for_job_config(&self, job_config_id: &str) -> R<Vec<Allocation>> {
        let allocations = self.allocs_for_job_config(job_config_id)?;
        if allocations.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM allocation WHERE job_config_id = ?;",
            [job_config_id],
        )?;
        self.release_instance_bindings_tx(&tx, &allocations)?;
        self.bump_job_config_epoch_tx(&tx, job_config_id)?;
        tx.commit()?;

        self.cache_remove_allocations(&allocations)?;

        Ok(allocations)
    }

    #[instrument(err, skip_all)]
    pub fn reconcile_job_config_allocations(&self, job_config_id: &str) -> R<()> {
        let nodes: Vec<Node> = self
            .get_nodes()?
            .into_iter()
            .filter(|node| node.availability == "available")
            .collect();

        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        let tasks = self.get_task_configs_for_job_config(job_config_id)?;
        if tasks.is_empty() {
            return Ok(());
        }

        let allocations_by_node = self.get_allocs_by_node()?;
        let mut rng = thread_rng();

        for (task_id, task_config) in tasks {
            let constraint_set =
                ConstraintSet::new(&task_config.constraints).with_context(|| {
                    format!("Failed to parse constraints for task {}", task_config.name)
                })?;
            let permit_set = PermitSet::new(&task_config.permit)
                .with_context(|| format!("Failed to parse permit for task {}", task_config.name))?;
            let eligible = nodes
                .iter()
                .filter(|node| {
                    constraint_set.is_satisfied(node, None) && permit_set.is_permitted(node)
                })
                .collect::<Vec<_>>();

            if eligible.is_empty() {
                bail!(
                    "Failed to place task {}: no available nodes matching constraints or permit",
                    task_config.name
                );
            }

            if let Some(per_key) = task_config.count.per.as_deref() {
                let grouped = group_nodes_by_attr(&eligible, per_key);
                if grouped.is_empty() {
                    bail!(
                        "Failed to place task {}: no eligible nodes with attribute {}",
                        task_config.name,
                        per_key
                    );
                }

                for group in grouped.values() {
                    let placed = count_allocations_in_group(&allocations_by_node, task_id, group);
                    for _ in placed..task_config.count.num {
                        let node = group
                            .choose(&mut rng)
                            .context("No nodes available to place allocations")?;
                        self.place_alloc(&node.id, task_id)?;
                    }
                }
            } else {
                let current = self.count_allocations_for_task(task_id)?;
                for _ in current..task_config.count.num {
                    let node = eligible
                        .choose(&mut rng)
                        .context("No nodes available to place allocations")?;
                    self.place_alloc(&node.id, task_id)?;
                }
            }
        }

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn reschedule_allocations_for_lost_node(&self, node_id: &str) -> R<()> {
        let removed_allocs = self.remove_allocs_for_node(node_id)?;

        if removed_allocs.is_empty() {
            return Ok(());
        }

        let affected_job_configs: HashSet<String> = removed_allocs
            .into_iter()
            .map(|alloc| alloc.job_config_id)
            .collect();

        for job_config_id in affected_job_configs {
            self.reconcile_job_config_allocations(&job_config_id)?;
        }

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn reschedule_allocations_for_new_node(&self) -> R<()> {
        let mut job_config_ids = self.job_config_ids_with_count_per()?;
        for job_config_id in self.job_config_ids_with_incomplete_rollouts()? {
            if !job_config_ids.contains(&job_config_id) {
                job_config_ids.push(job_config_id);
            }
        }
        for job_config_id in job_config_ids {
            self.reconcile_job_config_allocations(&job_config_id)?;
        }
        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn get_ledger(&self) -> R<Vec<LedgerEntry>> {
        let nodes = self.get_nodes()?;
        let allocations = self.get_allocs_by_node()?;

        let mut ledger = Vec::new();

        for node in nodes {
            let node_allocations = allocations.get(&node.id).cloned().unwrap_or_default();
            ledger.push(LedgerEntry {
                node,
                allocations: node_allocations,
            });
        }

        Ok(ledger)
    }

    //
    // INSTANCES (stateful workloads; see docs/stateful-workloads.md)
    //

    #[instrument(err, skip_all)]
    pub fn get_instance(&self, instance_id: &str) -> R<Option<Instance>> {
        let conn = self.read_connection()?;
        let instance = conn
            .query_row(&GET_INSTANCE, [instance_id], |row| {
                self.instance_from_row(row)
            })
            .optional()?;
        Ok(instance)
    }

    #[instrument(err, skip_all)]
    pub fn list_instances(&self, job_id: &str) -> R<Vec<Instance>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_INSTANCES_FOR_JOB.as_str())?;
        let rows = stmt.query_map([job_id], |row| self.instance_from_row(row))?;
        Self::collect_rows(rows)
    }

    //
    // SIGNING KEYS (workload-identity OIDC issuer; see docs/workload-identity.md)
    //
    // The private key PEM is persisted in this SQLite store — the same trust
    // domain as the CA material. Persistence is what keeps externally-held
    // tokens verifiable across manager restarts.
    //

    fn signing_key_from_row(row: &Row<'_>) -> Result<SigningKeyRecord, rusqlite::Error> {
        Ok(SigningKeyRecord {
            kid: row.get(0)?,
            alg: row.get(1)?,
            private_key_pem: row.get(2)?,
            public_jwk_x: row.get(3)?,
            public_jwk_y: row.get(4)?,
            created_at: row.get::<_, i64>(5)? as u64,
            not_after: row.get::<_, Option<i64>>(6)?.map(|v| v as u64),
            state: row.get(7)?,
        })
    }

    /// Persist a freshly generated signing key.
    #[instrument(err, skip_all, fields(kid = %record.kid, state = %record.state))]
    pub fn insert_signing_key(&self, record: &SigningKeyRecord) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute(
            &INSERT_SIGNING_KEY,
            named_params! {
                ":kid": record.kid,
                ":alg": record.alg,
                ":private_key_pem": record.private_key_pem,
                ":public_jwk_x": record.public_jwk_x,
                ":public_jwk_y": record.public_jwk_y,
                ":created_at": record.created_at as i64,
                ":not_after": record.not_after.map(|v| v as i64),
                ":state": record.state,
            },
        )?;
        Ok(())
    }

    /// All persisted signing keys (including expired rows, newest first).
    #[instrument(err, skip_all)]
    pub fn list_signing_keys(&self) -> R<Vec<SigningKeyRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_SIGNING_KEYS.as_str())?;
        let rows = stmt.query_map([], Self::signing_key_from_row)?;
        Self::collect_rows(rows)
    }

    /// Persist a key lifecycle transition (`active` → `retiring` → `expired`).
    #[instrument(err, skip_all, fields(kid = %kid, state = %state))]
    pub fn update_signing_key_state(
        &self,
        kid: &str,
        state: &str,
        not_after: Option<u64>,
    ) -> R<()> {
        let conn = self.write_connection()?;
        let updated = conn.execute(
            &UPDATE_SIGNING_KEY_STATE,
            named_params! {
                ":kid": kid,
                ":state": state,
                ":not_after": not_after.map(|v| v as i64),
            },
        )?;
        if updated == 0 {
            bail!("signing key {kid} not found");
        }
        Ok(())
    }

    /// Remove a signing key row entirely. Normal rotation keeps expired rows
    /// for audit; this exists for operator cleanup.
    #[instrument(err, skip_all, fields(kid = %kid))]
    pub fn delete_signing_key(&self, kid: &str) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute(&DELETE_SIGNING_KEY, named_params! { ":kid": kid })?;
        Ok(())
    }

    /// Insert or fully replace an instance row (terminated keyed rows are
    /// never deleted — the never-recycle guarantee depends on them remaining).
    #[instrument(err, skip_all)]
    pub fn upsert_instance(&self, instance: &Instance) -> R<Instance> {
        let now = Self::now_rfc3339()?;
        let created_at = if instance.created_at.is_empty() {
            now.clone()
        } else {
            instance.created_at.clone()
        };
        let conn = self.write_connection()?;
        let params = named_params! {
            ":instance_id": instance.instance_id,
            ":job_id": instance.job_id,
            ":group_name": instance.group,
            ":task_name": instance.task_name,
            ":mode": instance.mode.as_str(),
            ":state": instance.state.as_str(),
            ":slot_index": instance.slot_index,
            ":current_alloc_id": instance.current_alloc_id,
            ":storage_handle": instance.storage_handle,
            ":created_at": created_at,
            ":updated_at": now,
        };
        let instance =
            conn.query_row(&UPSERT_INSTANCE, params, |row| self.instance_from_row(row))?;
        Ok(instance)
    }

    /// Fetch the deterministic slot instance for (job, group, slot), creating
    /// a Pending row (no storage handle) if it does not exist yet. The
    /// allocation-insert path is what activates instances and mints storage
    /// handles; this method exists for introspection and pre-materialization.
    #[instrument(err, skip_all)]
    pub fn get_or_create_slot_instance(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        slot: u32,
    ) -> R<Instance> {
        let instance_id = slot_instance_id(job_id, group, slot);
        if let Some(existing) = self.get_instance(&instance_id)? {
            return Ok(existing);
        }

        let now = Self::now_rfc3339()?;
        let conn = self.write_connection()?;
        // INSERT OR IGNORE + re-read keeps this race-safe against the
        // allocation-insert path creating the same slot concurrently.
        conn.execute(
            "INSERT OR IGNORE INTO instance (instance_id, job_id, group_name, task_name, mode, \
             state, slot_index, current_alloc_id, storage_handle, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, 'slot', 'pending', ?5, NULL, NULL, ?6, ?6);",
            params![instance_id, job_id, group, task_name, slot, now],
        )?;
        let instance = conn.query_row(&GET_INSTANCE, [instance_id.as_str()], |row| {
            self.instance_from_row(row)
        })?;
        Ok(instance)
    }

    /// Point an instance at its (new) current allocation, or clear the binding.
    #[instrument(err, skip_all)]
    pub fn set_instance_current_alloc(
        &self,
        instance_id: &str,
        current_alloc_id: Option<i64>,
    ) -> R<()> {
        let now = Self::now_rfc3339()?;
        let conn = self.write_connection()?;
        let updated = conn.execute(
            "UPDATE instance SET current_alloc_id = ?1, updated_at = ?2 WHERE instance_id = ?3;",
            params![current_alloc_id, now, instance_id],
        )?;
        if updated == 0 {
            bail!("instance {instance_id} not found");
        }
        Ok(())
    }

    /// Transition an instance's lifecycle state.
    #[instrument(err, skip_all)]
    pub fn set_instance_state(&self, instance_id: &str, state: InstanceState) -> R<()> {
        let now = Self::now_rfc3339()?;
        let conn = self.write_connection()?;
        let updated = conn.execute(
            "UPDATE instance SET state = ?1, updated_at = ?2 WHERE instance_id = ?3;",
            params![state.as_str(), now, instance_id],
        )?;
        if updated == 0 {
            bail!("instance {instance_id} not found");
        }
        Ok(())
    }

    /// Terminate every instance of a stopped job. Rows are always retained:
    /// slot rows keep their storage handles so a later re-run re-materializes
    /// the same identities; keyed rows become permanent tombstones and their
    /// storage handles are dropped (matching DestroyInstance — a terminated
    /// keyed instance's state must never be reattachable, since the id can
    /// never run again). Called from the job_stop eval after its allocations
    /// are removed.
    #[instrument(err, skip_all)]
    pub fn terminate_instances_for_job(&self, job_id: &str) -> R<u64> {
        let now = Self::now_rfc3339()?;
        let conn = self.write_connection()?;
        let updated = conn.execute(
            "UPDATE instance SET state = 'terminated', current_alloc_id = NULL, \
             storage_handle = CASE WHEN mode = 'keyed' THEN NULL ELSE storage_handle END, \
             updated_at = ?1 \
             WHERE job_id = ?2 AND state != 'terminated';",
            params![now, job_id],
        )?;
        Ok(updated as u64)
    }

    /// Register a new keyed-mode instance (the CreateInstance RPC's storage
    /// half; task resolution and id policy live in the server handler).
    ///
    /// When `requested_id` is `Some` it must not collide with ANY existing
    /// instance row — including Terminated tombstones — enforcing the
    /// never-recycle guarantee; collisions surface as
    /// [`StorageError::Conflict`]. When `None`, a random id is minted
    /// (`{task}-{16 alphanumerics}`). The row starts `pending` with no storage
    /// handle; the first placement mints one if the task declares storage.
    #[instrument(err, skip_all)]
    pub fn create_keyed_instance(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        requested_id: Option<&str>,
    ) -> R<Instance> {
        let now = Self::now_rfc3339()?;
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;

        let exists = |tx: &Transaction<'_>, id: &str| -> R<bool> {
            let found: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM instance WHERE instance_id = ?;",
                    [id],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(found.is_some())
        };

        let instance_id = match requested_id {
            Some(id) => {
                if exists(&tx, id)? {
                    bail!(StorageError::Conflict {
                        message: format!(
                            "instance id {id} already exists (keyed ids are never recycled, \
                             including destroyed ones); choose a different id"
                        ),
                    });
                }
                id.to_string()
            }
            None => loop {
                // Collisions on 16 random alphanumerics are practically
                // impossible; the loop is belt-and-braces.
                let candidate = format!("{task_name}-{}", random_string(16));
                if !exists(&tx, &candidate)? {
                    break candidate;
                }
            },
        };

        tx.execute(
            "INSERT INTO instance (instance_id, job_id, group_name, task_name, mode, state, \
             slot_index, current_alloc_id, storage_handle, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, 'keyed', 'pending', NULL, NULL, NULL, ?5, ?5);",
            params![instance_id, job_id, group, task_name, now],
        )?;
        let instance = tx.query_row(&GET_INSTANCE, [instance_id.as_str()], |row| {
            self.instance_from_row(row)
        })?;
        tx.commit()?;
        Ok(instance)
    }

    /// Destroy a keyed-mode instance (the DestroyInstance RPC's storage half):
    /// delete its allocations through the normal allocation-removal path,
    /// transition the row to `terminated`, and drop its storage handle.
    ///
    /// The row is retained forever as a tombstone so the id can never be
    /// created again. Destroying an already-terminated instance is a no-op
    /// (returns [`DestroyInstanceOutcome::AlreadyTerminated`]). Slot-mode
    /// instances are rejected: their lifecycle is owned by the scheduler via
    /// `count.num`.
    #[instrument(err, skip_all)]
    pub fn destroy_keyed_instance(&self, instance_id: &str) -> R<DestroyInstanceOutcome> {
        let instance = self
            .get_instance(instance_id)?
            .ok_or_else(|| StorageError::not_found("instance", instance_id))?;
        if instance.mode != InstanceMode::Keyed {
            bail!(StorageError::InvalidInput {
                message: format!(
                    "instance {instance_id} is slot-mode; slot instances are managed by the \
                     scheduler via count.num, not DestroyInstance"
                ),
            });
        }
        if instance.state == InstanceState::Terminated {
            return Ok(DestroyInstanceOutcome::AlreadyTerminated);
        }

        let now = Self::now_rfc3339()?;
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;

        // Stop the instance's allocation(s) through the normal removal path
        // (delete row + epoch bump + cache eviction); the worker stops the
        // task on its next heartbeat reconcile. The instance is terminated in
        // the same transaction, so the release hook semantics (which skip
        // terminated instances) can never race this.
        let mut stmt = tx.prepare(
            "SELECT allocation.id, allocation.status, allocation.resources, allocation.node_id, \
             allocation.job_config_id, allocation.task_config_id, job_config.job_id, \
             allocation.instance_id \
             FROM allocation \
             INNER JOIN job_config ON job_config.id = allocation.job_config_id \
             WHERE allocation.instance_id = ?;",
        )?;
        let allocations = Self::collect_rows(
            stmt.query_map([instance_id], |row| self.allocation_from_row(row))?,
        )?;
        drop(stmt);

        tx.execute(
            "DELETE FROM allocation WHERE instance_id = ?;",
            [instance_id],
        )?;
        tx.execute(
            "UPDATE instance SET state = 'terminated', current_alloc_id = NULL, \
             storage_handle = NULL, updated_at = ?1 WHERE instance_id = ?2;",
            params![now, instance_id],
        )?;
        self.bump_epochs_for_allocations_tx(&tx, &allocations)?;
        tx.commit()?;

        self.cache_remove_allocations(&allocations)?;

        info!(
            instance_id,
            job_id = %instance.job_id,
            removed_allocations = allocations.len(),
            "Destroyed keyed instance"
        );
        Ok(DestroyInstanceOutcome::Destroyed {
            removed_allocations: allocations.len(),
        })
    }

    /// Count the live (pending/active/rescheduling) keyed instances of a
    /// (job, group, task). This is the desired allocation count for a
    /// keyed-mode task — `count.num` is ignored for keyed tasks.
    #[instrument(err, skip_all)]
    pub fn count_live_keyed_instances(&self, job_id: &str, group: &str, task_name: &str) -> R<u32> {
        let conn = self.read_connection()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM instance \
             WHERE job_id = ?1 AND group_name = ?2 AND task_name = ?3 \
             AND mode = 'keyed' AND state != 'terminated';",
            params![job_id, group, task_name],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as u32)
    }

    //
    // EVALS
    //

    #[instrument(err, skip_all)]
    pub fn insert_eval(&self, eval: &Eval) -> R<()> {
        let conn = self.write_connection()?;

        conn.execute("PRAGMA foreign_keys = OFF;", [])?;

        let params = named_params! {
            ":id": eval.id,
            ":status": eval.status,
            ":type": eval.kind,
            ":job_id": if eval.job_id.is_empty() { None::<String> } else { Some(eval.job_id.clone()) },
            ":job_config_id": if eval.job_config_id.is_empty() { None::<String> } else { Some(eval.job_config_id.clone()) },
            ":node_id": if eval.node_name.is_empty() { None::<String> } else { Some(eval.node_name.clone()) },
            ":priority": eval.priority,
            ":ready_at": Self::now_rfc3339()?,
        };

        conn.execute(&INSERT_EVAL, params)?;
        conn.execute("PRAGMA foreign_keys = ON;", [])?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn set_eval_status(&self, id: &str, status: &str) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute("UPDATE eval SET status = ? WHERE id = ?;", [status, id])?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn get_evals(&self) -> R<Vec<Eval>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&GET_EVALS)?;
        let rows = stmt.query_map([], |row| self.eval_from_row(row))?;

        Self::collect_rows(rows)
    }

    // #[instrument(err, skip_all)]
    pub fn get_next_eval(&self) -> R<Option<Eval>> {
        let conn = self.read_connection()?;
        let now = Self::now_rfc3339()?;
        let eval = conn.query_row(&GET_NEXT_EVAL, [now], |row| self.eval_from_row(row));

        match eval {
            Ok(eval) => Ok(Some(eval)),
            Err(RusqliteError::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(err, skip_all)]
    pub fn claim_eval(&self) -> R<Option<Eval>> {
        let tx_started = Instant::now();
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;

        let eval = tx
            .query_row(
                "UPDATE eval
                SET status = 'in_flight'
                WHERE id = (
                    SELECT id
                    FROM eval
                    WHERE status = 'pending' AND ready_at <= :now
                    ORDER BY priority DESC, id ASC
                    LIMIT 1
                )
                RETURNING id, status, type, job_id, job_config_id, node_id, priority, ready_at;",
                named_params! {":now": Self::now_rfc3339()?},
                |row| self.eval_from_row(row),
            )
            .optional()?;

        let Some(mut eval) = eval else {
            tx.commit()?;
            debug!(
                duration_ms = tx_started.elapsed().as_millis() as u64,
                "No claimable eval was found"
            );
            return Ok(None);
        };

        let eval_id = eval.id.clone();
        tx.execute(
            "DELETE FROM eval
            WHERE status = 'pending'
              AND id != :id
              AND (
                (:job_id IS NOT NULL AND :job_id != '' AND job_id = :job_id)
                OR (:node_id IS NOT NULL AND :node_id != '' AND node_id = :node_id)
              );",
            named_params! {
                ":id": eval_id,
                ":job_id": if eval.job_id.trim().is_empty() { None::<String> } else { Some(eval.job_id.clone()) },
                ":node_id": if eval.node_name.trim().is_empty() { None::<String> } else { Some(eval.node_name.clone()) },
            },
        )?;

        tx.commit()?;
        info!(
            eval_id = eval.id,
            eval_kind = %eval.kind,
            eval_priority = eval.priority,
            duration_ms = tx_started.elapsed().as_millis(),
            "Claimed eval transaction"
        );
        eval.status = "in_flight".to_string();

        Ok(Some(eval))
    }

    // Expose a clone so runtime worker tasks can await wake notifications.
    pub fn eval_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.eval_notify)
    }

    // Returns how long until the earliest pending eval becomes claimable.
    // None means there is no pending eval row at all.
    pub fn next_pending_eval_delay(&self) -> R<Option<Duration>> {
        let conn = self.read_connection()?;

        let next_ready_at: Option<String> = conn
            .query_row(
                "SELECT ready_at FROM eval WHERE status = 'pending' ORDER BY ready_at ASC LIMIT 1;",
                [],
                |row| row.get(0),
            )
            .optional()?;

        let Some(next_ready_at) = next_ready_at else {
            return Ok(None);
        };

        let next_ready_at = OffsetDateTime::parse(&next_ready_at, &Rfc3339)?;
        let now = OffsetDateTime::now_utc();
        if next_ready_at <= now {
            return Ok(Some(Duration::ZERO));
        }

        Ok(Some((next_ready_at - now).try_into()?))
    }

    #[instrument(err, skip_all)]
    pub fn requeue_in_flight_evals(&self) -> R<u64> {
        let conn = self.write_connection()?;
        let updated = conn.execute(
            "UPDATE eval SET status = 'pending' WHERE status = 'in_flight';",
            [],
        )?;

        if updated > 0 {
            // Startup recovery moved work from in_flight->pending; wake all waiters.
            self.eval_notify.notify_waiters();
        }

        Ok(updated as u64)
    }

    #[instrument(err, skip_all)]
    pub fn complete_eval(&self, eval_id: &str, succeeded: bool) -> R<()> {
        let tx_started = Instant::now();
        let mut conn: MutexGuard<'_, Connection> = self.write_connection()?;
        let tx = conn.transaction()?;

        if succeeded {
            tx.execute("DELETE FROM eval WHERE id = ?;", [eval_id])?;
        } else {
            tx.execute("UPDATE eval SET status = 'failed' WHERE id = ?;", [eval_id])?;
        }
        tx.commit()?;
        debug!(
            eval_id,
            succeeded,
            duration_ms = tx_started.elapsed().as_millis(),
            "Completed eval transaction"
        );

        Ok(())
    }

    pub fn enqueue_eval(
        &self,
        kind: &str,
        job_id: Option<&str>,
        job_config_id: Option<&str>,
        node_id: Option<&str>,
        priority: Option<i64>,
    ) -> R<Eval> {
        self.enqueue_eval_after_delay(
            kind,
            job_id,
            job_config_id,
            node_id,
            priority,
            Duration::ZERO,
        )
    }

    pub fn enqueue_eval_after_delay(
        &self,
        kind: &str,
        job_id: Option<&str>,
        job_config_id: Option<&str>,
        node_id: Option<&str>,
        priority: Option<i64>,
        delay: Duration,
    ) -> R<Eval> {
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;

        let job_id_value = job_id
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string);
        let job_config_id_value = job_config_id
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string);
        let node_id_value = node_id
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string);

        let ready_at =
            (OffsetDateTime::now_utc() + time::Duration::try_from(delay)?).format(&Rfc3339)?;

        let params = named_params! {
            ":id": None::<i64>,
            ":status": "pending",
            ":type": kind,
            ":job_id": job_id_value,
            ":job_config_id": job_config_id_value,
            ":node_id": node_id_value,
            ":priority": priority.unwrap_or(self.default_eval_priority),
            ":ready_at": ready_at,
        };

        tx.execute(&INSERT_EVAL, params)?;
        let next_id = tx.last_insert_rowid();

        let eval = Eval {
            id: next_id.to_string(),
            status: "pending".into(),
            kind: kind.to_string(),
            job_id: job_id_value.unwrap_or_default(),
            job_config_id: job_config_id_value.unwrap_or_default(),
            node_name: node_id_value.unwrap_or_default(),
            priority: priority.unwrap_or(self.default_eval_priority),
        };

        tx.commit()?;

        if delay.is_zero() {
            // Zero-delay evals are immediately claimable, so wake one sleeping worker.
            self.eval_notify.notify_one();
        }

        Ok(eval)
    }

    //
    // KV STORE
    //

    #[instrument(err, skip_all)]
    pub fn put_kv(
        &self,
        key: &str,
        value: &str,
        value_type: KvValueType,
        expected_version: Option<i64>,
        lock_id: Option<&str>,
    ) -> R<KvEntry> {
        Self::ensure_kv_key(key)?;
        let normalized_value = Self::normalize_kv_value(value, value_type)?;

        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        self.enforce_kv_lock(&tx, key, lock_id)?;

        let existing = tx
            .query_row(
                "SELECT key, value, value_type, version, created_at, updated_at FROM kv_entry WHERE key = ?;",
                [key],
                |row| self.kv_entry_from_row(row),
            )
            .optional()?;

        let now = Self::now_rfc3339()?;
        let value_type_value = value_type as i32;

        let entry = if let Some(existing) = existing {
            if let Some(expected) = expected_version {
                if expected != existing.version {
                    return Err(anyhow::Error::new(StorageError::Conflict {
                        message: format!(
                            "expected version {expected} does not match current version {}",
                            existing.version
                        ),
                    }));
                }
            }

            let next_version = existing.version + 1;
            tx.execute(
                "UPDATE kv_entry SET value = :value, value_type = :value_type, version = :version, updated_at = :updated_at WHERE key = :key;",
                named_params! {
                    ":value": normalized_value,
                    ":value_type": value_type_value,
                    ":version": next_version,
                    ":updated_at": now,
                    ":key": key,
                },
            )?;

            KvEntry {
                key: existing.key,
                value: normalized_value,
                value_type: value_type_value,
                version: next_version,
                created_at: existing.created_at,
                updated_at: now,
            }
        } else {
            if let Some(expected) = expected_version {
                if expected != 0 {
                    return Err(anyhow::Error::new(StorageError::Conflict {
                        message: "kv entry does not exist".to_string(),
                    }));
                }
            }

            tx.execute(
                "INSERT INTO kv_entry (key, value, value_type, version, created_at, updated_at) VALUES (:key, :value, :value_type, :version, :created_at, :updated_at);",
                named_params! {
                    ":key": key,
                    ":value": normalized_value,
                    ":value_type": value_type_value,
                    ":version": 1_i64,
                    ":created_at": now,
                    ":updated_at": now,
                },
            )?;

            KvEntry {
                key: key.to_string(),
                value: normalized_value,
                value_type: value_type_value,
                version: 1,
                created_at: now.clone(),
                updated_at: now,
            }
        };

        tx.commit()?;

        Ok(entry)
    }

    #[instrument(err, skip_all)]
    pub fn get_kv(&self, key: &str) -> R<Option<KvEntry>> {
        Self::ensure_kv_key(key)?;
        let conn = self.read_connection()?;
        let entry = conn
            .query_row(
                "SELECT key, value, value_type, version, created_at, updated_at FROM kv_entry WHERE key = ?;",
                [key],
                |row| self.kv_entry_from_row(row),
            )
            .optional()?;

        Ok(entry)
    }

    #[instrument(err, skip_all)]
    pub fn list_kv(&self, prefix: &str) -> R<Vec<KvEntry>> {
        let conn = self.read_connection()?;
        let pattern = format!("{prefix}%");
        let mut stmt = conn.prepare(
            "SELECT key, value, value_type, version, created_at, updated_at FROM kv_entry WHERE key LIKE ? ORDER BY key ASC;",
        )?;
        let rows = stmt.query_map([pattern], |row| self.kv_entry_from_row(row))?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all)]
    pub fn delete_kv(
        &self,
        key: &str,
        expected_version: Option<i64>,
        lock_id: Option<&str>,
    ) -> R<()> {
        Self::ensure_kv_key(key)?;
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        self.enforce_kv_lock(&tx, key, lock_id)?;

        let existing = tx
            .query_row(
                "SELECT key, value, value_type, version, created_at, updated_at FROM kv_entry WHERE key = ?;",
                [key],
                |row| self.kv_entry_from_row(row),
            )
            .optional()?;

        let Some(existing) = existing else {
            return Err(anyhow::Error::new(StorageError::not_found("kv entry", key)));
        };

        if let Some(expected) = expected_version {
            if expected != existing.version {
                return Err(anyhow::Error::new(StorageError::Conflict {
                    message: format!(
                        "expected version {expected} does not match current version {}",
                        existing.version
                    ),
                }));
            }
        }

        tx.execute("DELETE FROM kv_entry WHERE key = ?;", [key])?;
        tx.commit()?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn patch_kv(
        &self,
        key: &str,
        patch_json: &str,
        expected_version: Option<i64>,
        lock_id: Option<&str>,
    ) -> R<KvEntry> {
        Self::ensure_kv_key(key)?;
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        self.enforce_kv_lock(&tx, key, lock_id)?;

        let mut entry = tx
            .query_row(
                "SELECT key, value, value_type, version, created_at, updated_at FROM kv_entry WHERE key = ?;",
                [key],
                |row| self.kv_entry_from_row(row),
            )
            .optional()?
            .ok_or_else(|| anyhow::Error::new(StorageError::not_found("kv entry", key)))?;

        if entry.value_type != KvValueType::Json as i32 {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "kv entry is not JSON".to_string(),
            }));
        }

        if let Some(expected) = expected_version {
            if expected != entry.version {
                return Err(anyhow::Error::new(StorageError::Conflict {
                    message: format!(
                        "expected version {expected} does not match current version {}",
                        entry.version
                    ),
                }));
            }
        }

        let mut current: Value = from_str(&entry.value).map_err(|err| {
            anyhow::Error::new(StorageError::InvalidInput {
                message: format!("invalid stored json value: {err}"),
            })
        })?;
        let patch: json_patch::Patch = from_str(patch_json).map_err(|err| {
            anyhow::Error::new(StorageError::InvalidInput {
                message: format!("invalid json patch: {err}"),
            })
        })?;
        json_patch::patch(&mut current, &patch.0).map_err(|err| {
            anyhow::Error::new(StorageError::InvalidInput {
                message: format!("unable to apply json patch: {err}"),
            })
        })?;

        let updated_value = current.to_string();
        let next_version = entry.version + 1;
        let now = Self::now_rfc3339()?;

        tx.execute(
            "UPDATE kv_entry SET value = :value, version = :version, updated_at = :updated_at WHERE key = :key;",
            named_params! {
                ":value": updated_value,
                ":version": next_version,
                ":updated_at": now,
                ":key": key,
            },
        )?;

        entry.value = updated_value;
        entry.version = next_version;
        entry.updated_at = now;

        tx.commit()?;

        Ok(entry)
    }

    #[instrument(err, skip_all)]
    pub fn acquire_kv_lock(
        &self,
        key: &str,
        lock_id: Option<&str>,
        owner: Option<&str>,
        ttl_seconds: i64,
        force: bool,
    ) -> R<KvLock> {
        Self::ensure_kv_key(key)?;
        if ttl_seconds <= 0 {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "ttl_seconds must be greater than zero".to_string(),
            }));
        }

        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let now = OffsetDateTime::now_utc();
        let expires_at = now + time::Duration::seconds(ttl_seconds);
        let lock_id_value = lock_id
            .map(str::to_string)
            .unwrap_or_else(Self::random_lock_id);

        if let Some(existing) = self.load_kv_lock_tx(&tx, key)? {
            let expired = Self::parse_rfc3339(&existing.expires_at)? <= now;
            let lock_matches = lock_id
                .map(|id| id == existing.lock_id.as_str())
                .unwrap_or(false);
            if !expired && !lock_matches && !force {
                return Err(anyhow::Error::new(StorageError::Conflict {
                    message: "kv lock is already held".to_string(),
                }));
            }
        }

        let acquired_at = now.format(&Rfc3339)?;
        let expires_at = expires_at.format(&Rfc3339)?;
        let owner_value = owner.map(str::to_string);

        tx.execute(
            "INSERT INTO kv_lock (key, lock_id, owner, acquired_at, expires_at) VALUES (:key, :lock_id, :owner, :acquired_at, :expires_at)\n            ON CONFLICT(key) DO UPDATE SET lock_id = :lock_id, owner = :owner, acquired_at = :acquired_at, expires_at = :expires_at;",
            named_params! {
                ":key": key,
                ":lock_id": lock_id_value,
                ":owner": owner_value,
                ":acquired_at": acquired_at,
                ":expires_at": expires_at,
            },
        )?;

        tx.commit()?;

        Ok(KvLock {
            key: key.to_string(),
            lock_id: lock_id_value,
            owner: owner_value,
            acquired_at,
            expires_at,
        })
    }

    #[instrument(err, skip_all)]
    pub fn renew_kv_lock(&self, key: &str, lock_id: &str, ttl_seconds: i64) -> R<KvLock> {
        Self::ensure_kv_key(key)?;
        if ttl_seconds <= 0 {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "ttl_seconds must be greater than zero".to_string(),
            }));
        }

        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let Some(mut lock) = self.load_kv_lock_tx(&tx, key)? else {
            return Err(anyhow::Error::new(StorageError::not_found("kv lock", key)));
        };

        let now = OffsetDateTime::now_utc();
        let expires_at = Self::parse_rfc3339(&lock.expires_at)?;
        if expires_at <= now {
            tx.execute("DELETE FROM kv_lock WHERE key = ?;", [key])?;
            return Err(anyhow::Error::new(StorageError::Conflict {
                message: "kv lock has expired".to_string(),
            }));
        }

        if lock.lock_id != lock_id {
            return Err(anyhow::Error::new(StorageError::Conflict {
                message: "kv lock id does not match".to_string(),
            }));
        }

        let new_expires_at = (now + time::Duration::seconds(ttl_seconds)).format(&Rfc3339)?;
        tx.execute(
            "UPDATE kv_lock SET expires_at = :expires_at WHERE key = :key;",
            named_params! {
                ":expires_at": new_expires_at,
                ":key": key,
            },
        )?;

        lock.expires_at = new_expires_at;
        tx.commit()?;

        Ok(lock)
    }

    #[instrument(err, skip_all)]
    pub fn release_kv_lock(&self, key: &str, lock_id: &str) -> R<()> {
        Self::ensure_kv_key(key)?;
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let Some(lock) = self.load_kv_lock_tx(&tx, key)? else {
            return Err(anyhow::Error::new(StorageError::not_found("kv lock", key)));
        };

        if lock.lock_id != lock_id {
            return Err(anyhow::Error::new(StorageError::Conflict {
                message: "kv lock id does not match".to_string(),
            }));
        }

        tx.execute("DELETE FROM kv_lock WHERE key = ?;", [key])?;
        tx.commit()?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn get_kv_lock(&self, key: &str) -> R<Option<KvLock>> {
        Self::ensure_kv_key(key)?;
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let lock = self.load_kv_lock_tx(&tx, key)?;
        let Some(lock) = lock else {
            tx.commit()?;
            return Ok(None);
        };

        let expires_at = Self::parse_rfc3339(&lock.expires_at)?;
        if expires_at <= OffsetDateTime::now_utc() {
            tx.execute("DELETE FROM kv_lock WHERE key = ?;", [key])?;
            tx.commit()?;
            return Ok(None);
        }

        tx.commit()?;
        Ok(Some(lock))
    }

    #[instrument(err, skip_all)]
    pub fn query_state_store(&self, sql: &str, allow_write: bool) -> R<StateStoreQueryResult> {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "sql must not be empty".to_string(),
            }));
        }

        let statement_type = Self::sqlite_statement_type(trimmed);
        if !allow_write && !Self::is_read_only_statement(&statement_type) {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: format!(
                    "statement type '{statement_type}' is not allowed in read-only mode; pass allow_write to enable mutating queries"
                ),
            }));
        }

        if Self::is_query_statement(&statement_type) {
            let conn = if allow_write {
                self.write_connection()?
            } else {
                self.read_connection()?
            };
            let mut stmt = conn.prepare(trimmed)?;
            let columns: Vec<String> = stmt
                .column_names()
                .iter()
                .map(|column| (*column).to_string())
                .collect();
            let mut rows = stmt.query([])?;
            let mut output_rows = Vec::new();
            while let Some(row) = rows.next()? {
                let mut map = serde_json::Map::new();
                for (index, column_name) in columns.iter().enumerate() {
                    let value = match row.get_ref(index)? {
                        ValueRef::Null => Value::Null,
                        ValueRef::Integer(value) => Value::from(value),
                        ValueRef::Real(value) => Value::from(value),
                        ValueRef::Text(value) => {
                            Value::from(String::from_utf8_lossy(value).to_string())
                        }
                        ValueRef::Blob(value) => Value::from(format!(
                            "0x{}",
                            value
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>()
                        )),
                    };
                    map.insert(column_name.clone(), value);
                }
                output_rows.push(StateStoreQueryRow {
                    json: Value::Object(map).to_string(),
                });
            }

            return Ok(StateStoreQueryResult {
                statement_type,
                columns,
                rows: output_rows,
                affected_rows: 0,
            });
        }

        let conn = if allow_write {
            self.write_connection()?
        } else {
            self.read_connection()?
        };
        let affected_rows = conn.execute(trimmed, [])? as u64;

        Ok(StateStoreQueryResult {
            statement_type,
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows,
        })
    }

    #[instrument(err, skip_all)]
    pub fn state_store_schema(&self) -> R<Vec<StateStoreSchemaObject>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT type, name, COALESCE(sql, '') FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY type, name;",
        )?;
        let mut objects = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let kind: String = row.get(0)?;
            let name: String = row.get(1)?;
            let create_sql: String = row.get(2)?;
            let pragma = format!("PRAGMA table_info('{}');", name.replace('\'', "''"));
            let mut column_stmt = conn.prepare(&pragma)?;
            let columns_iter = column_stmt.query_map([], |column_row| {
                Ok(StateStoreSchemaColumn {
                    ordinal: column_row.get(0)?,
                    name: column_row.get(1)?,
                    data_type: column_row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                    not_null: column_row.get::<_, i64>(3)? != 0,
                    default_value: column_row.get(4)?,
                    primary_key: column_row.get::<_, i64>(5)? != 0,
                })
            })?;
            objects.push(StateStoreSchemaObject {
                kind,
                name,
                create_sql,
                columns: Self::collect_rows(columns_iter)?,
            });
        }

        Ok(objects)
    }

    #[instrument(err, skip_all)]
    pub fn list_saved_state_queries(&self) -> R<Vec<SavedStateQueryRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT name, sql, created_at, updated_at FROM saved_state_query ORDER BY name ASC;",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(SavedStateQueryRecord {
                name: row.get(0)?,
                sql: row.get(1)?,
                created_at: row.get(2)?,
                updated_at: row.get(3)?,
            })
        })?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all)]
    pub fn upsert_saved_state_query(&self, name: &str, sql: &str) -> R<SavedStateQueryRecord> {
        let name = name.trim();
        let sql = sql.trim();
        if name.is_empty() {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "saved query name must not be empty".to_string(),
            }));
        }
        if sql.is_empty() {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "saved query sql must not be empty".to_string(),
            }));
        }

        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO saved_state_query (name, sql, created_at, updated_at) VALUES (:name, :sql, :now, :now) ON CONFLICT(name) DO UPDATE SET sql = excluded.sql, updated_at = excluded.updated_at;",
            named_params! {
                ":name": name,
                ":sql": sql,
                ":now": now,
            },
        )?;
        let query = tx.query_row(
            "SELECT name, sql, created_at, updated_at FROM saved_state_query WHERE name = ?;",
            [name],
            |row| {
                Ok(SavedStateQueryRecord {
                    name: row.get(0)?,
                    sql: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                })
            },
        )?;
        tx.commit()?;
        Ok(query)
    }

    /// Look up a previously recorded response for an idempotency `key`.
    ///
    /// Returns `Some(response_message)` when a request with this key was already
    /// applied, allowing the caller to replay the prior outcome instead of
    /// re-applying. An empty key disables deduplication and always returns
    /// `None`.
    pub fn idempotency_lookup(&self, key: &str) -> R<Option<String>> {
        let key = key.trim();
        if key.is_empty() {
            return Ok(None);
        }
        let conn = self.read_connection()?;
        let result = conn
            .query_row(
                "SELECT response_message FROM idempotency_record WHERE key = ?;",
                [key],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(result)
    }

    /// Record the response for an idempotency `key`.
    ///
    /// Uses `INSERT OR IGNORE` so a concurrent duplicate cannot overwrite an
    /// existing record. Returns `true` when the record was newly inserted and
    /// `false` when a record already existed (the caller raced another request
    /// carrying the same key). An empty key is a no-op and returns `true`.
    pub fn idempotency_record(&self, key: &str, operation: &str, message: &str) -> R<bool> {
        let key = key.trim();
        if key.is_empty() {
            return Ok(true);
        }
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let conn = self.write_connection()?;
        let changed = conn.execute(
            "INSERT OR IGNORE INTO idempotency_record (key, operation, response_message, created_at) VALUES (:key, :op, :msg, :now);",
            named_params! {
                ":key": key,
                ":op": operation,
                ":msg": message,
                ":now": now,
            },
        )?;
        Ok(changed > 0)
    }

    #[instrument(err, skip_all)]
    pub fn delete_saved_state_query(&self, name: &str) -> R<()> {
        let name = name.trim();
        if name.is_empty() {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "saved query name must not be empty".to_string(),
            }));
        }

        let conn = self.write_connection()?;
        let deleted = conn.execute("DELETE FROM saved_state_query WHERE name = ?;", [name])?;
        if deleted == 0 {
            return Err(anyhow::Error::new(StorageError::not_found(
                "saved_state_query",
                name,
            )));
        }
        Ok(())
    }

    fn sqlite_statement_type(sql: &str) -> String {
        sql.trim_start()
            .split_whitespace()
            .next()
            .unwrap_or("UNKNOWN")
            .trim_matches(|c: char| !c.is_ascii_alphanumeric())
            .to_uppercase()
    }

    fn is_query_statement(statement_type: &str) -> bool {
        matches!(statement_type, "SELECT" | "PRAGMA" | "WITH" | "EXPLAIN")
    }

    fn is_read_only_statement(statement_type: &str) -> bool {
        Self::is_query_statement(statement_type)
    }

    #[instrument(err, skip_all)]
    pub fn replace_job_kv_references(&self, job_id: &str, refs: &[JobKvReference]) -> R<()> {
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM job_kv_reference WHERE job_id = ?;", [job_id])?;
        for reference in refs {
            tx.execute(
                "INSERT INTO job_kv_reference (job_id, job_config_id, kv_key, json_pointer) VALUES (?1, ?2, ?3, ?4);",
                params![
                    reference.job_id,
                    reference.job_config_id,
                    reference.kv_key,
                    reference.json_pointer,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn job_kv_references_for_key(&self, key: &str) -> R<Vec<JobKvReference>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT job_id, job_config_id, kv_key, json_pointer FROM job_kv_reference WHERE kv_key = ?;",
        )?;
        let rows = stmt.query_map([key], |row| self.job_kv_reference_from_row(row))?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all)]
    pub fn job_kv_references_for_job(&self, job_id: &str) -> R<Vec<JobKvReference>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT job_id, job_config_id, kv_key, json_pointer FROM job_kv_reference WHERE job_id = ?;",
        )?;
        let rows = stmt.query_map([job_id], |row| self.job_kv_reference_from_row(row))?;
        Self::collect_rows(rows)
    }
}

/// Mint an opaque storage handle for an instance. The `local-dir/` scheme is
/// the placeholder contract for v1: attach/detach drivers are a follow-up (see
/// `docs/plans/volume-plugin-system-plan.md`); today the handle only proves
/// identity stability (same instance => same handle across reschedules).
fn mint_storage_handle() -> String {
    format!("local-dir/{}", random_string(32))
}

fn group_nodes_by_attr<'a>(nodes: &'a [&'a Node], key: &str) -> HashMap<String, Vec<&'a Node>> {
    let mut grouped: HashMap<String, Vec<&'a Node>> = HashMap::new();
    for node in nodes {
        if key == "id" {
            grouped.entry(node.id.clone()).or_default().push(*node);
            continue;
        }
        let Some(value) = node_attribute(node, key) else {
            continue;
        };
        let token = value_token(value);
        grouped.entry(token).or_default().push(*node);
    }
    grouped
}

fn count_allocations_in_group(
    allocations_by_node: &HashMap<String, Vec<Allocation>>,
    task_id: i64,
    nodes: &[&Node],
) -> u32 {
    let mut count = 0;
    for node in nodes {
        if let Some(allocs) = allocations_by_node.get(&node.id) {
            count += allocs
                .iter()
                .filter(|alloc| alloc.task_config_id == task_id)
                .count() as u32;
        }
    }
    count
}

fn node_attribute<'a>(node: &'a Node, key: &str) -> Option<&'a Value> {
    node.config_attrs
        .as_object()
        .and_then(|attrs| attrs.get(key))
        .or_else(|| {
            node.fingerprint_attrs
                .as_object()
                .and_then(|attrs| attrs.get(key))
        })
        .or_else(|| {
            node.dynamic_attrs
                .as_object()
                .and_then(|attrs| attrs.get(key))
        })
}

fn value_token(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

fn task_schedule_equivalent(left: &TaskConfig, right: &TaskConfig) -> bool {
    left.count == right.count
        && left.schedule == right.schedule
        && left.resources == right.resources
        && left.constraints == right.constraints
        && left.permit == right.permit
        && left.affinities == right.affinities
}

// =====
// =====
// TESTS
// =====
// =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::scheduler::{PlanMode, Scheduler};
    use rusqlite::params;
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("khepri-store-")
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

        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    #[test]
    fn idempotency_record_and_lookup_roundtrip() -> R<()> {
        let store = setup_store()?;

        // Unknown key: no recorded response.
        assert_eq!(store.idempotency_lookup("k1")?, None);

        // First record wins and is replayable.
        assert!(store.idempotency_record("k1", "run_job", "Job submit success")?);
        assert_eq!(
            store.idempotency_lookup("k1")?.as_deref(),
            Some("Job submit success")
        );

        // A second record for the same key is a no-op and keeps the original.
        assert!(!store.idempotency_record("k1", "run_job", "different")?);
        assert_eq!(
            store.idempotency_lookup("k1")?.as_deref(),
            Some("Job submit success")
        );

        // Empty key disables deduplication entirely.
        assert_eq!(store.idempotency_lookup("")?, None);
        assert!(store.idempotency_record("", "run_job", "ignored")?);
        Ok(())
    }

    fn make_node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            config_attrs: json!({}),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    fn make_node_msg(id: &str, status: &str) -> NodeMsg {
        NodeMsg {
            id: id.to_string(),
            pool: "default".into(),
            status: status.to_string(),
            availability: "available".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: "{}".into(),
            dynamic_attrs: "{}".into(),
            health_checks: "{}".into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
        }
    }

    fn task(name: &str) -> TaskConfig {
        TaskConfig::new(name, "noop")
    }

    fn task_id_for(store: &Store, task_name: &str, job_id: &str) -> R<i64> {
        let conn = store.read_connection()?;
        let id = conn.query_row(
            "SELECT id FROM task_config WHERE name = ? AND job_id = ?;",
            [task_name, job_id],
            |row| row.get(0),
        )?;

        Ok(id)
    }

    fn insert_eval(
        store: &Store,
        id: i64,
        status: &str,
        kind: &str,
        job_id: Option<&str>,
        job_config_id: Option<&str>,
        node_id: Option<&str>,
        priority: i64,
    ) -> R<()> {
        let conn = store.write_connection()?;
        conn.execute(
            "INSERT INTO eval (id, status, type, job_id, job_config_id, node_id, priority) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
            params![id, status, kind, job_id, job_config_id, node_id, priority],
        )?;

        Ok(())
    }

    // fn teardown_store(store: &Store) -> R<()> {
    //     let migrations = Store::get_migrations();
    //     let mut conn = store.conn.lock().map_err(|_| anyhow!(CONN_LOCK_ERROR))?;
    //     migrations.to_version(&mut conn, 0)?;

    //     Ok(())
    // }

    #[test]
    fn manager_config_round_trips_and_overwrites() -> R<()> {
        let store = setup_store()?;

        let config = ManagerConfigFull {
            server: crate::structs::ServerConfigFull {
                host: "127.0.0.1".into(),
                port: 1234,
                insecure_port: 4321,
            },
            store_path: "tests/store".into(),
            worker_config_path: "configs/workers".into(),
            worker_check_seconds: 5,
            ..ManagerConfigFull::default()
        };

        store.upsert_manager_config("manager-a", &config)?;

        let configs = store.get_manager_configs()?;
        assert_eq!(configs.len(), 1);

        let parsed: serde_json::Value = serde_json::from_str(&configs[0].config_json)?;
        assert_eq!(parsed["server"]["port"], 1234);

        let mut updated = config.clone();
        updated.worker_check_seconds = 99;
        updated.server.port = 9876;

        store.upsert_manager_config("manager-a", &updated)?;

        let configs = store.get_manager_configs()?;
        assert_eq!(configs.len(), 1);
        let parsed_updated: serde_json::Value = serde_json::from_str(&configs[0].config_json)?;
        assert_eq!(parsed_updated["server"]["port"], 9876);
        assert_eq!(parsed_updated["workerCheckSeconds"], 99);

        Ok(())
    }

    #[test]
    fn read_model_tracks_nodes_and_updates() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "active"))?;
        let model = store.scheduling_read_model()?;
        let node = model.nodes.get("node-one").context("missing cached node")?;
        assert_eq!(node.status, "active");

        let mut diff = Map::new();
        diff.insert("region".to_string(), json!("us-west"));
        store.update_node_dyamic("node-one", &diff)?;
        let model = store.scheduling_read_model()?;
        let node = model.nodes.get("node-one").context("missing cached node")?;
        assert_eq!(node.dynamic_attrs["region"], "us-west");

        store.mark_node_lost("node-one")?;
        let model = store.scheduling_read_model()?;
        let node = model.nodes.get("node-one").context("missing cached node")?;
        assert_eq!(node.status, "lost");
        assert_eq!(node.availability, "unavailable");

        Ok(())
    }

    #[test]
    fn task_identities_round_trip_through_persistence() -> R<()> {
        use crate::structs::workload_identity::{WorkloadIdentity, WorkloadIdentityFile};

        let store = setup_store()?;

        let identity = WorkloadIdentity {
            name: "aws".into(),
            audience: vec!["sts.amazonaws.com".into()],
            ttl: Some("15m".into()),
            file: Some(WorkloadIdentityFile {
                path: "/var/run/legion/identity/aws/token".into(),
                mode: Some("0400".into()),
            }),
            ..Default::default()
        };
        let task = TaskConfig {
            identities: vec![identity.clone()],
            ..task("worker")
        };
        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "identity-persist".into(),
            tasks: vec![task],
            version: None,
        };

        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;

        let loaded = store.get_job_config(&job.name)?;
        assert_eq!(loaded.tasks.len(), 1);
        assert_eq!(loaded.tasks[0].identities, vec![identity]);
        Ok(())
    }

    #[test]
    fn tasks_without_identities_round_trip_empty() -> R<()> {
        let store = setup_store()?;
        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "no-identity-persist".into(),
            tasks: vec![task("worker")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let loaded = store.get_job_config(&job.name)?;
        assert!(loaded.tasks[0].identities.is_empty());
        Ok(())
    }

    #[test]
    fn read_model_tracks_allocations_add_remove() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "ready"))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "cache-allocs".into(),
            tasks: vec![task("worker")],
            version: None,
        };

        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, "worker", &job.name)?;
        let alloc = store.place_alloc("node-one", task_id)?;

        let model = store.scheduling_read_model()?;
        let node_allocs = model
            .allocations_by_node
            .get("node-one")
            .cloned()
            .unwrap_or_default();
        assert_eq!(node_allocs.len(), 1);
        assert_eq!(node_allocs[0].id, alloc.id);
        assert_eq!(node_allocs[0].job_config_id, job_config_id);

        store.remove_allocs_for_node("node-one")?;
        let model = store.scheduling_read_model()?;
        let node_allocs = model
            .allocations_by_node
            .get("node-one")
            .cloned()
            .unwrap_or_default();
        assert!(node_allocs.is_empty());

        Ok(())
    }

    #[test]
    fn read_model_tracks_task_config_changes() -> R<()> {
        let store = setup_store()?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "cache-tasks".into(),
            tasks: vec![task("alpha")],
            version: None,
        };

        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;

        let job_config_id = store.get_job_config(&job.name)?.id();
        let model = store.scheduling_read_model()?;
        let tasks = model
            .task_configs
            .get(&job_config_id)
            .cloned()
            .unwrap_or_default();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].1.name, "alpha");

        let updated_job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: job.name.clone(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                ..task("beta")
            }],
            version: None,
        };

        store.handle_job_submit(
            &updated_job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;

        let new_job_config_id = store.get_job_config(&job.name)?.id();
        let model = store.scheduling_read_model()?;
        let tasks = model
            .task_configs
            .get(&new_job_config_id)
            .cloned()
            .unwrap_or_default();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].1.name, "beta");
        assert_eq!(tasks[0].1.count.num, 2);

        Ok(())
    }

    #[test]
    fn adding_updating_and_getting_worker_types() -> R<()> {
        let store = setup_store()?;

        let mut attrs = HashMap::new();
        attrs.insert("test".to_string(), "val".to_string());

        let worker_config = WorkerConfig {
            name: "test".to_string(),
            pool: "default".to_string(),
            additional_node_checks: vec![],
            attributes: attrs,
            name_prefix: None,
            resources: "{}".into(),
            reserved_resources: "{}".into(),
            max_allocations: None,
        };

        let returned_config = store.insert_worker_type(&worker_config)?;
        assert_eq!(returned_config.name, worker_config.name);

        let returned_config = store.get_worker_type("test".to_string())?;
        assert_eq!(returned_config.name, worker_config.name);
        assert_eq!(returned_config.pool, worker_config.pool);
        let val = returned_config
            .attributes
            .get("test")
            .context("no test attr")?;

        assert_eq!(val, "val");

        Ok(())
    }

    #[test]
    fn worker_type_with_empty_checks_round_trips_as_empty_vec() -> R<()> {
        let store = setup_store()?;

        let worker_config = WorkerConfig {
            name: "empty".to_string(),
            pool: "default".to_string(),
            additional_node_checks: vec![],
            attributes: HashMap::new(),
            name_prefix: None,
            resources: "{}".into(),
            reserved_resources: "{}".into(),
            max_allocations: None,
        };

        store.insert_worker_type(&worker_config)?;

        let returned_config = store.get_worker_type(worker_config.name.clone())?;
        assert!(returned_config.additional_node_checks.is_empty());

        Ok(())
    }

    #[test]
    fn mark_nodes_as_lost_updates_status_and_availability() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        store.mark_nodes_as_lost(&["node-one".to_string()])?;

        let node_one = store.get_node("node-one")?;
        assert_eq!(node_one.status, "lost");
        assert_eq!(node_one.availability, "unavailable");

        let node_two = store.get_node("node-two")?;
        assert_eq!(node_two.status, "active");
        assert_eq!(node_two.availability, "available");

        Ok(())
    }

    #[test]
    fn mark_nodes_as_lost_is_noop_for_empty_input() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "active"))?;

        store.mark_nodes_as_lost(&[])?;

        let node_one = store.get_node("node-one")?;
        assert_eq!(node_one.status, "active");
        assert_eq!(node_one.availability, "available");

        Ok(())
    }

    #[test]
    fn mark_nodes_as_lost_reschedules_allocations() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "lost-reschedule".into(),
            tasks: vec![task("worker")],
            version: None,
        };

        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;

        let task_id = task_id_for(&store, "worker", &job.name)?;
        store.place_alloc("node-one", task_id)?;

        store.mark_nodes_as_lost(&["node-one".to_string()])?;

        let node_one_allocs = store.allocs_for_node("node-one")?;
        let node_two_allocs = store.allocs_for_node("node-two")?;

        assert!(node_one_allocs.is_empty());
        assert_eq!(node_two_allocs.len(), 1);

        Ok(())
    }

    #[test]
    fn dynamic_attrs_can_be_updated_after_join() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "active"))?;

        let mut diff = Map::new();
        diff.insert("foo".to_string(), json!("bar"));

        let updated_node = store.update_node_dyamic("node-one", &diff)?;
        let foo = updated_node
            .dynamic_attrs
            .get("foo")
            .context("foo missing from dynamic attrs")?;

        assert_eq!(foo, "bar");

        Ok(())
    }

    #[test]
    fn get_ledger_includes_lost_nodes() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        store.mark_nodes_as_lost(&["node-two".to_string()])?;

        let ledger = store.get_ledger()?;
        assert_eq!(ledger.len(), 2);

        let lost_entry = ledger
            .iter()
            .find(|entry| entry.node.id == "node-two")
            .context("node-two missing from ledger")?;
        assert_eq!(lost_entry.node.status, "lost");

        Ok(())
    }

    #[test]
    fn handle_job_submit_adds_new_job() -> R<()> {
        let sub_one = Submission {
            path: "sub-1.jsonnet".into(),
            content: "sub-1".into(),
            is_primary: true,
        };
        let sub_two = Submission {
            path: "sub-2.libsonnet".into(),
            content: "sub-2".into(),
            is_primary: false,
        };

        let subs = vec![sub_one.clone(), sub_two.clone()];

        let task_one = TaskConfig {
            config: json!({ "message": "hello-1" }),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("task-1", "kind-1")
        };

        let task_two = TaskConfig {
            count: TaskCount::new(2),
            config: json!({ "message": "hi-2" }),
            resources: json!({ "memory": 512 }),
            ..TaskConfig::new("task-2", "kind-2")
        };

        let task_configs = vec![task_one.clone(), task_two.clone()];
        let job_name = "test".to_string();
        let job_config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: task_configs.clone(),
            version: None,
        };
        let store = setup_store()?;

        let returned_config = store.get_job(&job_name);
        assert!(returned_config.is_err());
        assert_eq!(
            returned_config.unwrap_err().to_string(),
            "job not found (id: test)"
        );

        // NOTE: What should this return (figure it out in concontent later)
        let submit_response = store.handle_job_submit(&job_config, &subs)?;
        assert!(matches!(submit_response, JobSubmitOutcome::Created { .. }));

        let job = store.get_job(&job_name)?;
        assert_eq!(job.name, job_name);

        let latest_job_config = store.get_job_config(&job_name)?;
        assert_eq!(latest_job_config.name, job_name);

        let t1_from_store = latest_job_config.tasks.get(0).context("oof")?;
        assert_eq!(task_one.name, t1_from_store.name);
        assert_eq!(task_one.kind, t1_from_store.kind);
        assert_eq!(
            task_one.resources.get("memory").context("no memory 1")?,
            t1_from_store
                .resources
                .get("memory")
                .context("no memory 2")?
        );

        assert_eq!(
            task_one.config.get("message").context("no message 1")?,
            t1_from_store
                .config
                .get("message")
                .context("no message 2")?
        );

        let t2_from_store = latest_job_config.tasks.get(1).context("oof")?;
        assert_eq!(task_two.name, t2_from_store.name);

        let subs_from_store = store.get_submissions_for_config(&latest_job_config.id())?;
        assert_eq!(subs_from_store.len(), 2);
        let sub_one_from_store = subs_from_store.get(0).context("no sub 1")?;
        let sub_two_from_store = subs_from_store.get(1).context("no sub 2")?;
        assert_eq!(sub_one_from_store.path, sub_one.path);
        assert_eq!(sub_two_from_store.path, sub_two.path);

        Ok(())
    }

    #[test]
    fn handle_job_submit_rolls_back_on_failure() -> R<()> {
        let task_one = TaskConfig {
            config: json!({ "message": "hello-1" }),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("task-1", "kind-1")
        };

        let job_name = "rollback-job".to_string();
        let job_config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![task_one],
            version: None,
        };

        let store = setup_store()?;
        {
            let conn = store.write_connection()?;
            conn.execute("DROP TABLE job_config_submission;", [])?;
        }

        let result = store.handle_job_submit(
            &job_config,
            &[Submission {
                path: "sub-1.jsonnet".into(),
                content: "sub-1".into(),
                is_primary: true,
            }],
        );

        assert!(result.is_err());
        assert!(store.get_job(&job_name).is_err());
        assert!(store.get_job_config(&job_name).is_err());

        Ok(())
    }

    #[test]
    fn handle_job_submit_updates_job_properly() -> R<()> {
        let orib_sub = Submission {
            path: "sub-1.jsonnet".into(),
            content: "sub-1".into(),
            is_primary: true,
        };
        let orig_submissions = vec![orib_sub.clone()];

        let orig_task = TaskConfig {
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("orig-task", "kind-1")
        };

        let orig_task_configs = vec![orig_task.clone()];
        let job_config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "jobname".into(),
            tasks: orig_task_configs.clone(),
            version: None,
        };

        let store = setup_store()?;

        // Original Job is submitted
        let submit_response = store.handle_job_submit(&job_config, &orig_submissions)?;
        assert!(matches!(submit_response, JobSubmitOutcome::Created { .. }));

        // Get config for initial insert
        let first_job_config = store.get_job_config("jobname".into())?;
        assert_eq!(first_job_config.name, job_config.name);
        assert_eq!(
            1,
            first_job_config
                .version
                .context("missing version on config")?,
        );

        // Submit an updates job config
        // updated: task kind config

        let new_task = TaskConfig {
            kind: "kind-new".to_string(),
            ..orig_task
        };
        let new_task_configs = vec![new_task.clone()];
        let job_config_with_modified_kind = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            tasks: new_task_configs.clone(),
            ..job_config
        };

        let submit_response =
            store.handle_job_submit(&job_config_with_modified_kind.clone(), &orig_submissions)?;
        assert!(matches!(submit_response, JobSubmitOutcome::Updated { .. }));

        // Get config for initial insert
        let second_job_config = store.get_job_config("jobname".into())?;
        assert_eq!(
            2,
            second_job_config
                .version
                .context("missing version on config")?,
        );

        // Submit an updates job config
        // updated: additional task added

        let extra_task = TaskConfig {
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("extra-task", "kind-extra")
        };
        let new_task_configs = vec![new_task.clone(), extra_task.clone()];

        let job_config_with_extra_task = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            tasks: new_task_configs.clone(),
            ..job_config_with_modified_kind
        };

        let submit_response =
            store.handle_job_submit(&job_config_with_extra_task, &orig_submissions)?;
        assert!(matches!(submit_response, JobSubmitOutcome::Updated { .. }));

        // Get config for initial insert
        let third_job_config = store.get_job_config("jobname".into())?;
        assert_eq!(
            3,
            third_job_config
                .version
                .context("missing version on config")?,
        );

        // // Submit an updates job config
        // // updated: metadata on job

        // let job_config_with_new_metadata = JobConfig {
        //     ... TODO ...
        // };

        // let submit_response = store.handle_job_submit(&job_config, &orig_submissions);
        // assert!(submit_response.is_ok());

        // // Get config for initial insert
        // let fourth_job_config = store.get_job_config("jobname".into())?;
        // assert_eq!(
        //     4,
        //     fourth_job_config
        //         .version
        //         .context("missing version on config")?,
        // );

        Ok(())
    }

    #[test]
    fn handle_job_submit_with_unchanged_job_is_idempotent() -> R<()> {
        let orig_submissions = vec![];
        let orig_task = TaskConfig {
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("dupe-task", "kind")
        };

        let orig_task_configs = vec![orig_task.clone()];
        let job_config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "jobname".into(),
            tasks: orig_task_configs.clone(),
            version: None,
        };

        let store = setup_store()?;

        // Original Job is submitted
        let submit_response = store.handle_job_submit(&job_config, &orig_submissions)?;
        assert!(matches!(submit_response, JobSubmitOutcome::Created { .. }));

        // Resubmitting with unchanged config is idempotent
        let submit_response = store.handle_job_submit(&job_config, &orig_submissions)?;
        assert!(matches!(
            submit_response,
            JobSubmitOutcome::Unchanged { .. }
        ));

        Ok(())
    }

    #[test]
    fn placing_allocations_and_querying_them() -> R<()> {
        let store = setup_store()?;

        let node_one_id = "node-one".to_string();
        let node_two_id = "node-two".to_string();
        store.insert_node(NodeMsg::from(make_node(&node_one_id)))?;
        store.insert_node(NodeMsg::from(make_node(&node_two_id)))?;

        let job_one_name = "job-one".to_string();
        let task_one = TaskConfig {
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("job-one-task-1", "driver-a")
        };
        let task_two = TaskConfig {
            config: json!({ "message": "world" }),
            resources: json!({ "memory": 512 }),
            ..TaskConfig::new("job-one-task-2", "driver-b")
        };
        let job_one_config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_one_name.clone(),
            tasks: vec![task_one.clone(), task_two.clone()],
            version: None,
        };
        let job_one_subs = vec![Submission {
            path: "job-one-sub.jsonnet".into(),
            content: "job-one".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_one_config, &job_one_subs)?;

        let job_one_config_id = store.get_job_config(&job_one_name)?.id();
        let task_one_id = task_id_for(&store, &task_one.name, &job_one_name)?;
        let task_two_id = task_id_for(&store, &task_two.name, &job_one_name)?;

        let job_two_name = "job-two".to_string();
        let task_three = TaskConfig {
            config: json!({ "message": "hola" }),
            resources: json!({ "memory": 128 }),
            ..TaskConfig::new("job-two-task-1", "driver-c")
        };
        let job_two_config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_two_name.clone(),
            tasks: vec![task_three.clone()],
            version: None,
        };
        let job_two_subs = vec![Submission {
            path: "job-two-sub.jsonnet".into(),
            content: "job-two".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_two_config, &job_two_subs)?;

        let job_two_config_id = store.get_job_config(&job_two_name)?.id();
        let task_three_id = task_id_for(&store, &task_three.name, &job_two_name)?;

        let alloc_one = store.place_alloc(&node_one_id, task_one_id)?;
        let alloc_two = store.place_alloc(&node_two_id, task_two_id)?;
        let alloc_three = store.place_alloc(&node_one_id, task_three_id)?;

        assert_eq!(alloc_one.status, "placed");
        assert_eq!(alloc_one.node_id, node_one_id);
        assert_eq!(alloc_one.task_config_id, task_one_id);
        assert_eq!(alloc_one.job_config_id, job_one_config_id);
        let expected_one = store
            .resource_policy
            .normalize_task_resources(&task_one.resources, false)?;
        let expected_two = store
            .resource_policy
            .normalize_task_resources(&task_two.resources, false)?;
        let expected_three = store
            .resource_policy
            .normalize_task_resources(&task_three.resources, false)?;

        assert_eq!(alloc_one.resources, expected_one);

        assert_eq!(alloc_two.resources, expected_two);
        assert_eq!(alloc_three.job_config_id, job_two_config_id);
        assert_eq!(alloc_three.resources, expected_three);

        let job_one_allocs = store.allocs_for_job(&job_one_name)?;
        assert_eq!(job_one_allocs.len(), 2);
        let job_one_task_ids: Vec<i64> = job_one_allocs.iter().map(|a| a.task_config_id).collect();
        assert!(job_one_task_ids.contains(&task_one_id));
        assert!(job_one_task_ids.contains(&task_two_id));

        let job_two_allocs = store.allocs_for_job(&job_two_name)?;
        assert_eq!(job_two_allocs.len(), 1);
        assert_eq!(job_two_allocs[0].task_config_id, task_three_id);

        let job_one_config_allocs = store.allocs_for_job_config(&job_one_config_id)?;
        assert_eq!(job_one_config_allocs.len(), 2);
        let job_two_config_allocs = store.allocs_for_job_config(&job_two_config_id)?;
        assert_eq!(job_two_config_allocs.len(), 1);
        assert_eq!(job_two_config_allocs[0].task_config_id, task_three_id);

        let node_one_allocs = store.allocs_for_node(&node_one_id)?;
        assert_eq!(node_one_allocs.len(), 2);
        let node_two_allocs = store.allocs_for_node(&node_two_id)?;
        assert_eq!(node_two_allocs.len(), 1);

        let all_allocs = store.get_allocs()?;
        assert_eq!(all_allocs.len(), 3);

        let allocs_by_node = store.get_allocs_by_node()?;
        let node_one_group = allocs_by_node
            .get(&node_one_id)
            .context("missing node one allocs")?;
        assert_eq!(node_one_group.len(), 2);
        let node_two_group = allocs_by_node
            .get(&node_two_id)
            .context("missing node two allocs")?;
        assert_eq!(node_two_group.len(), 1);

        Ok(())
    }

    #[test]
    fn destroying_a_job_destroys_all_associated_objects() -> R<()> {
        let store = setup_store()?;

        let submissions = vec![
            Submission {
                path: "sub-1.jsonnet".into(),
                content: "sub-1".into(),
                is_primary: true,
            },
            Submission {
                path: "sub-2.jsonnet".into(),
                content: "sub-2".into(),
                is_primary: false,
            },
        ];

        let tasks = vec![TaskConfig::new("task-1", "kind-1")];

        let job_name = "solo-job".to_string();
        let job_config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks,
            version: None,
        };

        store.handle_job_submit(&job_config, &submissions)?;
        store.stop_job(&job_name)?;

        {
            let conn = store.read_connection()?;
            let submission_count: i64 =
                conn.query_row("SELECT COUNT(*) FROM submission;", [], |row| row.get(0))?;
            assert_eq!(submission_count, 2);
        }

        store.destroy_job(&job_name)?;

        let conn = store.read_connection()?;
        let job_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM job WHERE name = ?;",
            [job_name],
            |row| row.get(0),
        )?;
        let job_config_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM job_config WHERE job_id = ?;",
            [job_config.name],
            |row| row.get(0),
        )?;
        let job_config_submission_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM job_config_submission;", [], |row| {
                row.get(0)
            })?;
        let submission_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM submission;", [], |row| row.get(0))?;

        assert_eq!(job_count, 0);
        assert_eq!(job_config_count, 0);
        assert_eq!(job_config_submission_count, 0);
        assert_eq!(submission_count, 0);

        Ok(())
    }

    #[test]
    fn destroying_a_job_wont_destroy_other_used_submissions() -> R<()> {
        let store = setup_store()?;

        let shared_submission = Submission {
            path: "shared.jsonnet".into(),
            content: "shared-content".into(),
            is_primary: true,
        };

        let tasks = vec![TaskConfig::new("task-shared", "kind")];

        let job_one_config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-one".into(),
            tasks: tasks.clone(),
            version: None,
        };

        let job_two_config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-two".into(),
            tasks,
            version: None,
        };

        let job_one_subs = vec![shared_submission.clone()];
        let job_two_subs = vec![shared_submission.clone()];

        store.handle_job_submit(&job_one_config, &job_one_subs)?;
        store.handle_job_submit(&job_two_config, &job_two_subs)?;

        store.stop_job("job-one")?;
        store.destroy_job("job-one")?;

        let job_two = store.get_job("job-two")?;
        assert_eq!(job_two.name, "job-two");

        let conn = store.read_connection()?;
        let submission_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM submission;", [], |row| row.get(0))?;
        let job_one_records: i64 = conn.query_row(
            "SELECT COUNT(*) FROM job WHERE name = ?;",
            ["job-one"],
            |row| row.get(0),
        )?;
        let job_two_submission_refs: i64 = conn.query_row(
            r#"
            SELECT COUNT(*)
            FROM job_config_submission jcs
            INNER JOIN job_config jc ON jc.id = jcs.job_config_id
            WHERE jc.job_id = ?;
        "#,
            ["job-two"],
            |row| row.get(0),
        )?;

        assert_eq!(job_one_records, 0);
        assert_eq!(submission_count, 1);
        assert_eq!(job_two_submission_refs, 1);

        Ok(())
    }

    #[test]
    fn claim_eval_marks_in_flight_and_dedupes_pending() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-1", "ready"))?;

        insert_eval(
            &store,
            1,
            "pending",
            "node_join",
            None,
            None,
            Some("node-1"),
            50,
        )?;
        insert_eval(
            &store,
            2,
            "pending",
            "node_join",
            None,
            None,
            Some("node-1"),
            50,
        )?;

        let claimed = store.claim_eval()?.expect("eval should be claimed");
        assert_eq!(claimed.id, "1");
        assert_eq!(claimed.status, "in_flight");

        let conn = store.read_connection()?;
        let pending: i64 = conn.query_row(
            "SELECT COUNT(*) FROM eval WHERE status = 'pending';",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            pending, 0,
            "duplicates for the same job/node should be removed"
        );

        Ok(())
    }

    #[test]
    fn delayed_eval_is_not_claimed_until_ready() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-1", "ready"))?;
        let delayed = store.enqueue_eval_after_delay(
            "node_join",
            None,
            None,
            Some("node-1"),
            Some(10),
            Duration::from_secs(60),
        )?;

        assert!(store.claim_eval()?.is_none());

        let delayed_id = delayed.id.clone();
        let conn = store.write_connection()?;
        conn.execute(
            "UPDATE eval SET ready_at = '1970-01-01T00:00:00Z' WHERE id = ?;",
            [delayed_id],
        )?;
        drop(conn);

        let claimed = store
            .claim_eval()?
            .context("delayed eval should now be claimable")?;
        assert_eq!(claimed.id, delayed.id);

        Ok(())
    }

    #[test]
    fn complete_eval_removes_or_marks_terminal() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-1", "ready"))?;
        store.insert_node(make_node_msg("node-2", "ready"))?;

        insert_eval(
            &store,
            1,
            "pending",
            "node_join",
            None,
            None,
            Some("node-1"),
            50,
        )?;
        insert_eval(
            &store,
            2,
            "pending",
            "node_drop",
            None,
            None,
            Some("node-2"),
            50,
        )?;

        let first = store.claim_eval()?.expect("eval should be claimed");
        store.complete_eval(&first.id, true)?;

        let second = store.claim_eval()?.expect("second eval should be claimed");
        store.complete_eval(&second.id, false)?;

        let conn = store.read_connection()?;
        let remaining: i64 = conn.query_row("SELECT COUNT(*) FROM eval;", [], |row| row.get(0))?;
        assert_eq!(remaining, 1, "successful evals should be removed");

        let failed_status: String = conn.query_row(
            "SELECT status FROM eval WHERE id = ?;",
            [second.id],
            |row| row.get(0),
        )?;
        assert_eq!(failed_status, "failed");

        Ok(())
    }

    #[test]
    fn requeue_in_flight_evals_moves_stale_work_back_to_pending() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-1", "ready"))?;

        insert_eval(
            &store,
            1,
            "in_flight",
            "node_join",
            None,
            None,
            Some("node-1"),
            50,
        )?;
        insert_eval(
            &store,
            2,
            "pending",
            "node_join",
            None,
            None,
            Some("node-1"),
            40,
        )?;

        let requeued = store.requeue_in_flight_evals()?;
        assert_eq!(requeued, 1);

        let claimed = store.claim_eval()?.expect("eval should be claimed");
        assert_eq!(
            claimed.id, "1",
            "higher-priority stale eval should run first"
        );

        Ok(())
    }

    #[test]
    fn plan_submission_conflict_is_scoped_to_job_config_epoch() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;

        let job_one = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-one".into(),
            tasks: vec![TaskConfig::new("task-one", "noop")],
            version: None,
        };
        let job_two = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-two".into(),
            tasks: vec![TaskConfig::new("task-two", "noop")],
            version: None,
        };

        let submission = [Submission {
            path: "job.jsonnet".into(),
            content: "job".into(),
            is_primary: true,
        }];

        store.handle_job_submit(&job_one, &submission)?;
        store.handle_job_submit(&job_two, &submission)?;

        let job_one_config_id = store.get_job_config("job-one")?.id();
        let job_two_config_id = store.get_job_config("job-two")?.id();
        let task_one_id = task_id_for(&store, "task-one", "job-one")?;
        let task_two_id = task_id_for(&store, "task-two", "job-two")?;

        let job_one_epoch = store.allocation_epoch_for_job_config(&job_one_config_id)?;
        let job_two_epoch = store.allocation_epoch_for_job_config(&job_two_config_id)?;
        let job_one_versions = BTreeMap::from([
            (
                Store::scope_vector_key("job_config", &job_one_config_id),
                job_one_epoch,
            ),
            (Store::scope_vector_key("node", "node-a"), 0),
        ]);
        let job_two_versions = BTreeMap::from([
            (
                Store::scope_vector_key("job_config", &job_two_config_id),
                job_two_epoch,
            ),
            (Store::scope_vector_key("node", "node-b"), 0),
        ]);

        let job_one_plan = store.insert_plan_submission(
            &job_one_config_id,
            "fresh",
            &[("node-a".to_string(), task_one_id)],
            &job_one_versions,
        )?;
        let job_two_plan = store.insert_plan_submission(
            &job_two_config_id,
            "fresh",
            &[("node-b".to_string(), task_two_id)],
            &job_two_versions,
        )?;

        assert!(matches!(
            store.apply_plan_submission(&job_one_plan)?,
            PlanApplyOutcome::Applied(_)
        ));
        assert!(matches!(
            store.apply_plan_submission(&job_two_plan)?,
            PlanApplyOutcome::Applied(_)
        ));

        Ok(())
    }

    #[test]
    fn place_alloc_bumps_job_config_epoch() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-epoch".into(),
            tasks: vec![TaskConfig::new("task", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;

        let job_config_id = store.get_job_config("job-epoch")?.id();
        let task_id = task_id_for(&store, "task", "job-epoch")?;
        let before = store.allocation_epoch_for_job_config(&job_config_id)?;
        store.place_alloc("node-a", task_id)?;
        let after = store.allocation_epoch_for_job_config(&job_config_id)?;

        assert_eq!(after, before + 1);
        Ok(())
    }

    #[test]
    fn migrate_allocations_bumps_source_and_destination_job_epochs() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let job_v1 = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-migrate".into(),
            tasks: vec![TaskConfig::new("task", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job_v1,
            &[Submission {
                path: "v1.jsonnet".into(),
                content: "v1".into(),
                is_primary: true,
            }],
        )?;

        let v1_id = store.get_job_config("job-migrate")?.id();
        let v1_task = task_id_for(&store, "task", "job-migrate")?;
        store.place_alloc("node-a", v1_task)?;

        let mut v2_task = TaskConfig::new("task", "noop");
        v2_task.config = json!({"changed": true});
        let job_v2 = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-migrate".into(),
            tasks: vec![v2_task],
            version: None,
        };
        store.handle_job_submit(
            &job_v2,
            &[Submission {
                path: "v2.jsonnet".into(),
                content: "v2".into(),
                is_primary: true,
            }],
        )?;
        let v2_id = store.get_job_config("job-migrate")?.id();
        assert_ne!(v1_id, v2_id);

        let v1_before = store.allocation_epoch_for_job_config(&v1_id)?;
        let v2_before = store.allocation_epoch_for_job_config(&v2_id)?;

        let migrated = store.migrate_allocations_to_job_config(&v1_id, &v2_id)?;
        assert_eq!(migrated.len(), 1);

        let v1_after = store.allocation_epoch_for_job_config(&v1_id)?;
        let v2_after = store.allocation_epoch_for_job_config(&v2_id)?;

        assert_eq!(v1_after, v1_before + 1);
        assert_eq!(v2_after, v2_before + 1);

        Ok(())
    }

    #[test]
    fn plan_submission_rejects_stale_epoch_for_same_job_config() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-stale".into(),
            tasks: vec![TaskConfig::new("task", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;

        let job_config_id = store.get_job_config("job-stale")?.id();
        let task_id = task_id_for(&store, "task", "job-stale")?;
        let expected_versions = store
            .allocation_version_vector(&job_config_id, &HashSet::from(["node-a".to_string()]))?;

        let first = store.insert_plan_submission(
            &job_config_id,
            "fresh",
            &[("node-a".to_string(), task_id)],
            &expected_versions,
        )?;
        let second = store.insert_plan_submission(
            &job_config_id,
            "fresh",
            &[("node-a".to_string(), task_id)],
            &expected_versions,
        )?;

        assert!(matches!(
            store.apply_plan_submission(&first)?,
            PlanApplyOutcome::Applied(_)
        ));

        match store.apply_plan_submission(&second)? {
            PlanApplyOutcome::Rejected { .. } => {}
            PlanApplyOutcome::Applied(_) => {
                panic!("expected stale plan to be rejected");
            }
        }

        Ok(())
    }

    #[test]
    fn plan_submission_rejects_stale_epoch_for_shared_node_scope() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let make_job = |name: &str, task_name: &str| JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks: vec![TaskConfig::new(task_name, "noop")],
            version: None,
        };

        let submission = [Submission {
            path: "job.jsonnet".into(),
            content: "job".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&make_job("job-scope-a", "task-a"), &submission)?;
        store.handle_job_submit(&make_job("job-scope-b", "task-b"), &submission)?;

        let job_a_config = store.get_job_config("job-scope-a")?.id();
        let job_b_config = store.get_job_config("job-scope-b")?.id();
        let task_a = task_id_for(&store, "task-a", "job-scope-a")?;
        let task_b = task_id_for(&store, "task-b", "job-scope-b")?;

        let job_a_versions = store
            .allocation_version_vector(&job_a_config, &HashSet::from(["node-a".to_string()]))?;
        let job_b_versions = store
            .allocation_version_vector(&job_b_config, &HashSet::from(["node-a".to_string()]))?;

        let first = store.insert_plan_submission(
            &job_a_config,
            "fresh",
            &[("node-a".to_string(), task_a)],
            &job_a_versions,
        )?;
        let second = store.insert_plan_submission(
            &job_b_config,
            "fresh",
            &[("node-a".to_string(), task_b)],
            &job_b_versions,
        )?;

        assert!(matches!(
            store.apply_plan_submission(&first)?,
            PlanApplyOutcome::Applied(_)
        ));
        assert!(matches!(
            store.apply_plan_submission(&second)?,
            PlanApplyOutcome::Rejected { .. }
        ));

        Ok(())
    }

    #[test]
    fn plan_submission_rejects_due_to_constraints_with_latest_epoch() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;

        let constrained_task = {
            let mut task = TaskConfig::new("task", "noop");
            task.constraints = json!({ "node_id": "node-a" });
            task
        };
        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-constraints".into(),
            tasks: vec![constrained_task],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;

        let job_config_id = store.get_job_config("job-constraints")?.id();
        let task_id = task_id_for(&store, "task", "job-constraints")?;
        let expected_versions = store
            .allocation_version_vector(&job_config_id, &HashSet::from(["node-b".to_string()]))?;
        let expected_job_epoch = *expected_versions
            .get(&Store::scope_vector_key("job_config", &job_config_id))
            .unwrap_or(&0);

        let submission = store.insert_plan_submission(
            &job_config_id,
            "fresh",
            &[("node-b".to_string(), task_id)],
            &expected_versions,
        )?;
        let outcome = store.apply_plan_submission(&submission)?;
        match outcome {
            PlanApplyOutcome::Rejected {
                reason,
                latest_versions,
            } => {
                assert!(
                    reason.contains("does not satisfy constraints"),
                    "unexpected rejection reason: {reason}"
                );
                let latest_job_epoch = latest_versions
                    .get(&Store::scope_vector_key("job_config", &job_config_id))
                    .copied()
                    .unwrap_or_default();
                assert_eq!(latest_job_epoch, expected_job_epoch);
            }
            PlanApplyOutcome::Applied(_) => {
                bail!("expected constraint-based rejection");
            }
        }

        Ok(())
    }

    #[test]
    fn task_statuses_initialize_and_update() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "status-demo".into(),
            tasks: vec![
                TaskConfig {
                    lifecycle: TaskLifecycle::Batch,
                    ..TaskConfig::new("prep", "exec")
                },
                TaskConfig {
                    lifecycle: TaskLifecycle::Service,
                    ..TaskConfig::new("api", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config(&job.name)?.id();

        let statuses = store.task_statuses_for_job_config(&job_config_id)?;
        assert!(statuses
            .values()
            .all(|status| *status == TaskStatusState::Pending));

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        store.update_task_statuses_for_allocations(&outcome.allocations)?;

        let statuses = store.task_statuses_for_job_config(&job_config_id)?;
        let prep_id = task_id_for(&store, "prep", &job.name)?;
        let api_id = task_id_for(&store, "api", &job.name)?;
        assert_eq!(statuses.get(&prep_id), Some(&TaskStatusState::Success));
        assert_eq!(statuses.get(&api_id), Some(&TaskStatusState::Running));

        Ok(())
    }

    #[test]
    fn allocation_health_snapshots_upsert_and_cleanup() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "health-demo".into(),
            tasks: vec![TaskConfig::new("web", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let task_id = task_id_for(&store, "web", &job.name)?;
        let allocation = store.place_alloc("node-a", task_id)?;

        store.upsert_allocation_health_snapshots(
            "node-a",
            &[AllocationHealth {
                allocation_id: allocation.id,
                healthy: false,
                status: "starting".into(),
            }],
        )?;
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[AllocationHealth {
                allocation_id: allocation.id,
                healthy: true,
                status: "healthy".into(),
            }],
        )?;

        let health = store.allocation_health_for_allocations(&[allocation.id])?;
        let entry = health
            .get(&allocation.id)
            .context("missing allocation health snapshot")?;
        assert!(entry.healthy);
        assert_eq!(entry.status, "healthy");

        store.upsert_allocation_health_snapshots(
            "node-b",
            &[AllocationHealth {
                allocation_id: allocation.id,
                healthy: false,
                status: "spoofed".into(),
            }],
        )?;
        let health = store.allocation_health_for_allocations(&[allocation.id])?;
        let entry = health
            .get(&allocation.id)
            .context("missing allocation health snapshot after non-owner write")?;
        assert_eq!(entry.node_id, "node-a");
        assert!(entry.healthy);
        assert_eq!(entry.status, "healthy");

        let removed = store.remove_allocation_by_id(allocation.id)?;
        assert!(removed.is_some());
        let health = store.allocation_health_for_allocations(&[allocation.id])?;
        assert!(health.is_empty());

        Ok(())
    }

    #[test]
    fn rollout_events_append_list_and_prune_with_rollout() -> R<()> {
        let store = setup_store()?;

        let v1 = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "rollout-events".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v1, &[])?;
        let v1_id = store.get_job_config(&v1.name)?.id();

        let v2 = JobConfig {
            rollout: Some(RolloutConfig {
                replacement: Some(crate::structs::RolloutReplacement::CreateFirst),
                ..RolloutConfig::default()
            }),
            rollout_by_group: Default::default(),
            name: "rollout-events".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "version": 2 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v2, &[])?;
        let v2_id = store.get_job_config(&v2.name)?.id();

        let rollout = store.create_rollout(
            &v2.name,
            Some(&v1_id),
            &v2_id,
            "running",
            Some("default"),
            Some("auto"),
            Some("createFirst"),
            Some(false),
            "{}",
        )?;

        let first = store.append_rollout_event(
            rollout.id,
            &rollout.job_id,
            None,
            "info",
            "Rollout created",
            None,
        )?;
        let second = store.append_rollout_event(
            rollout.id,
            &rollout.job_id,
            Some("web"),
            "info",
            "Group web advanced to step 1",
            Some(r#"{"step":1}"#),
        )?;

        let events = store.list_rollout_events(rollout.id, None)?;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, first.id);
        assert_eq!(events[1].id, second.id);
        assert_eq!(events[1].group_name.as_deref(), Some("web"));

        let limited = store.list_rollout_events(rollout.id, Some(1))?;
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].id, second.id);

        let conn = store.write_connection()?;
        conn.execute(
            "UPDATE rollout SET completed_at = '1970-01-01T00:00:00Z', status = 'completed' WHERE id = ?1;",
            [rollout.id],
        )?;
        drop(conn);

        let pruned = store.prune_rollouts()?;
        assert_eq!(pruned, 1);
        assert!(store.get_rollout(rollout.id)?.is_none());
        assert!(store.list_rollout_events(rollout.id, None)?.is_empty());

        Ok(())
    }

    #[test]
    fn new_node_reconciles_completed_incomplete_rollouts() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;

        let v1 = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "reconcile-incomplete".into(),
            tasks: vec![TaskConfig::new("web", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &v1,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let v1_id = store.get_job_config(&v1.name)?.id();

        let v2 = JobConfig {
            rollout: Some(RolloutConfig::default()),
            rollout_by_group: Default::default(),
            name: "reconcile-incomplete".into(),
            tasks: vec![TaskConfig {
                config: json!({"version": 2}),
                ..TaskConfig::new("web", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(
            &v2,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        store.remove_allocs_for_job_config(&v2_id)?;

        let rollout = store.create_rollout(
            "reconcile-incomplete",
            Some(&v1_id),
            &v2_id,
            "completed_incomplete",
            Some("default"),
            Some("auto"),
            Some("createFirst"),
            Some(false),
            "{}",
        )?;
        store.append_rollout_event(
            rollout.id,
            &rollout.job_id,
            None,
            "warn",
            "Rollout completed incompletely after capacity timeout",
            None,
        )?;

        assert!(store.allocs_for_job_config(&v2_id)?.is_empty());
        store.reschedule_allocations_for_new_node()?;
        assert_eq!(store.allocs_for_job_config(&v2_id)?.len(), 1);

        Ok(())
    }

    #[test]
    fn incomplete_rollout_reconcile_attempts_placements() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;

        let v1 = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "reconcile-incomplete-loop".into(),
            tasks: vec![TaskConfig::new("web", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &v1,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let v1_id = store.get_job_config(&v1.name)?.id();

        let v2 = JobConfig {
            rollout: Some(RolloutConfig::default()),
            rollout_by_group: Default::default(),
            name: "reconcile-incomplete-loop".into(),
            tasks: vec![TaskConfig {
                config: json!({"version": 2}),
                ..TaskConfig::new("web", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(
            &v2,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        store.remove_allocs_for_job_config(&v2_id)?;

        let _ = store.create_rollout(
            "reconcile-incomplete-loop",
            Some(&v1_id),
            &v2_id,
            "completed_incomplete",
            Some("default"),
            Some("auto"),
            Some("createFirst"),
            Some(false),
            "{}",
        )?;

        assert!(store.allocs_for_job_config(&v2_id)?.is_empty());
        let reconciled = store.reconcile_incomplete_rollouts()?;
        assert_eq!(reconciled, 1);
        assert_eq!(store.allocs_for_job_config(&v2_id)?.len(), 1);

        Ok(())
    }

    //
    // INSTANCES (stateful workloads)
    //

    fn stateful_slot_task(name: &str, count: u32, with_storage: bool) -> TaskConfig {
        TaskConfig {
            count: TaskCount::new(count),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: None,
                storage: with_storage.then(|| crate::structs::StatefulStorageConfig {
                    path: "/var/lib/data".into(),
                    retain_on_scale_in: None,
                }),
            }),
            ..TaskConfig::new(name, "noop")
        }
    }

    fn submit_job(store: &Store, name: &str, tasks: Vec<TaskConfig>) -> R<String> {
        let config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks,
            version: None,
        };
        store.handle_job_submit(
            &config,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        Ok(store.get_job_config(name)?.id())
    }

    #[test]
    fn stateful_task_config_round_trips_through_persistence() -> R<()> {
        let store = setup_store()?;
        let job_config_id = submit_job(
            &store,
            "stateful-rt",
            vec![stateful_slot_task("db", 2, true)],
        )?;

        let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
        let (_, task) = tasks.first().context("missing task")?;
        let stateful = task.stateful.as_ref().context("stateful must persist")?;
        assert_eq!(stateful.mode, InstanceMode::Slot);
        assert_eq!(
            stateful.storage.as_ref().map(|s| s.path.as_str()),
            Some("/var/lib/data")
        );
        Ok(())
    }

    #[test]
    fn stateless_task_config_round_trips_without_stateful() -> R<()> {
        let store = setup_store()?;
        let job_config_id = submit_job(&store, "stateless-rt", vec![task("web")])?;
        let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
        assert!(tasks[0].1.stateful.is_none());
        Ok(())
    }

    #[test]
    fn place_alloc_assigns_lowest_free_slot_and_activates_instance() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "slot-place";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 3, true)])?;
        let task_id = task_id_for(&store, "db", job_id)?;

        let first = store.place_alloc("node-a", task_id)?;
        let second = store.place_alloc("node-a", task_id)?;
        let third = store.place_alloc("node-a", task_id)?;

        assert_eq!(first.instance_id.as_deref(), Some("slot-place-default-0"));
        assert_eq!(second.instance_id.as_deref(), Some("slot-place-default-1"));
        assert_eq!(third.instance_id.as_deref(), Some("slot-place-default-2"));

        let instances = store.list_instances(job_id)?;
        assert_eq!(instances.len(), 3);
        for (slot, instance) in instances.iter().enumerate() {
            assert_eq!(instance.mode, InstanceMode::Slot);
            assert_eq!(instance.state, InstanceState::Active);
            assert_eq!(instance.slot_index, Some(slot as u32));
            assert!(
                instance
                    .storage_handle
                    .as_deref()
                    .is_some_and(|handle| handle.starts_with("local-dir/")),
                "storage handle minted on first placement"
            );
        }
        assert_eq!(instances[0].current_alloc_id, Some(first.id));
        assert_eq!(instances[2].current_alloc_id, Some(third.id));

        // A fourth placement would exceed the slot pool and must fail loudly.
        assert!(store.place_alloc("node-a", task_id).is_err());
        Ok(())
    }

    #[test]
    fn removed_allocation_reuses_slot_instance_and_storage_handle() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "slot-reuse";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 2, true)])?;
        let task_id = task_id_for(&store, "db", job_id)?;

        let slot0 = store.place_alloc("node-a", task_id)?;
        let slot1 = store.place_alloc("node-a", task_id)?;
        let slot1_instance = store
            .get_instance(slot1.instance_id.as_deref().context("instance id")?)?
            .context("instance row")?;
        let original_handle = slot1_instance.storage_handle.clone();
        assert!(original_handle.is_some());

        // Kill slot 1's allocation: instance transitions to rescheduling and
        // keeps its handle.
        store.remove_allocation_by_id(slot1.id)?;
        let rescheduling = store
            .get_instance(&slot1_instance.instance_id)?
            .context("instance retained")?;
        assert_eq!(rescheduling.state, InstanceState::Rescheduling);
        assert_eq!(rescheduling.current_alloc_id, None);
        assert_eq!(rescheduling.storage_handle, original_handle);

        // The replacement reuses slot 1's instance id and storage handle.
        let replacement = store.place_alloc("node-a", task_id)?;
        assert_eq!(
            replacement.instance_id.as_deref(),
            Some(slot1_instance.instance_id.as_str())
        );
        let reactivated = store
            .get_instance(&slot1_instance.instance_id)?
            .context("instance reactivated")?;
        assert_eq!(reactivated.state, InstanceState::Active);
        assert_eq!(reactivated.current_alloc_id, Some(replacement.id));
        assert_eq!(reactivated.storage_handle, original_handle);

        // Slot 0 was never disturbed.
        let slot0_instance = store
            .get_instance(slot0.instance_id.as_deref().context("instance id")?)?
            .context("slot 0 instance")?;
        assert_eq!(slot0_instance.state, InstanceState::Active);
        assert_eq!(slot0_instance.current_alloc_id, Some(slot0.id));
        Ok(())
    }

    #[test]
    fn scale_in_terminates_high_slots_and_scale_out_reattaches_them() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "slot-scale";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 3, true)])?;
        let v1_task = task_id_for(&store, "db", job_id)?;
        for _ in 0..3 {
            store.place_alloc("node-a", v1_task)?;
        }
        let handles_before: Vec<Option<String>> = store
            .list_instances(job_id)?
            .into_iter()
            .map(|instance| instance.storage_handle)
            .collect();

        // Scale in to 2: submit v2, place its allocations, then drop v1's
        // (mirrors the job_submit eval flow: plan fresh, then remove previous).
        let v2_config_id = submit_job(&store, job_id, vec![stateful_slot_task("db", 2, true)])?;
        let v1_config_id = format!("{job_id}-1");
        let v2_task = task_id_for_config(&store, "db", &v2_config_id)?;
        store.place_alloc("node-a", v2_task)?;
        store.place_alloc("node-a", v2_task)?;
        store.remove_allocs_for_job_config(&v1_config_id)?;

        let instances = store.list_instances(job_id)?;
        assert_eq!(instances.len(), 3, "terminated rows are retained");
        assert_eq!(instances[0].state, InstanceState::Active);
        assert_eq!(instances[1].state, InstanceState::Active);
        assert_eq!(instances[2].state, InstanceState::Terminated);
        assert_eq!(
            instances[2].storage_handle, handles_before[2],
            "scale-in retains the storage handle by default"
        );
        // Slots 0/1 kept their identity and storage across the config update.
        assert_eq!(instances[0].storage_handle, handles_before[0]);
        assert_eq!(instances[1].storage_handle, handles_before[1]);

        // Scale back out to 3: the same slot id is re-materialized with the
        // retained handle.
        let v3_config_id = submit_job(&store, job_id, vec![stateful_slot_task("db", 3, true)])?;
        let v3_task = task_id_for_config(&store, "db", &v3_config_id)?;
        for _ in 0..3 {
            store.place_alloc("node-a", v3_task)?;
        }
        store.remove_allocs_for_job_config(&v2_config_id)?;

        let instances = store.list_instances(job_id)?;
        assert_eq!(instances.len(), 3);
        assert_eq!(instances[2].instance_id, "slot-scale-default-2");
        assert_eq!(instances[2].state, InstanceState::Active);
        assert_eq!(instances[2].storage_handle, handles_before[2]);
        Ok(())
    }

    #[test]
    fn scale_in_drops_storage_handle_when_retention_disabled() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "slot-noretain";
        let no_retain_task = |count: u32| TaskConfig {
            count: TaskCount::new(count),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    path: "/var/lib/data".into(),
                    retain_on_scale_in: Some(false),
                }),
            }),
            ..TaskConfig::new("db", "noop")
        };

        submit_job(&store, job_id, vec![no_retain_task(2)])?;
        let v1_task = task_id_for(&store, "db", job_id)?;
        store.place_alloc("node-a", v1_task)?;
        store.place_alloc("node-a", v1_task)?;

        let v2_config_id = submit_job(&store, job_id, vec![no_retain_task(1)])?;
        let v2_task = task_id_for_config(&store, "db", &v2_config_id)?;
        store.place_alloc("node-a", v2_task)?;
        store.remove_allocs_for_job_config(&format!("{job_id}-1"))?;

        let instances = store.list_instances(job_id)?;
        assert_eq!(instances[1].state, InstanceState::Terminated);
        assert_eq!(instances[1].storage_handle, None);
        Ok(())
    }

    #[test]
    fn stateless_jobs_create_no_instance_rows() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "stateless-job";
        submit_job(&store, job_id, vec![task("web")])?;
        let task_id = task_id_for(&store, "web", job_id)?;

        let allocation = store.place_alloc("node-a", task_id)?;
        assert_eq!(allocation.instance_id, None);
        assert!(store.list_instances(job_id)?.is_empty());

        store.remove_allocation_by_id(allocation.id)?;
        assert!(store.list_instances(job_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn instance_store_methods_round_trip() -> R<()> {
        let store = setup_store()?;
        let job_id = "inst-crud";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 2, false)])?;

        assert!(store.get_instance("missing")?.is_none());

        let created = store.get_or_create_slot_instance(job_id, "default", "db", 0)?;
        assert_eq!(created.instance_id, "inst-crud-default-0");
        assert_eq!(created.state, InstanceState::Pending);
        assert_eq!(created.slot_index, Some(0));
        assert_eq!(created.storage_handle, None);

        // Idempotent: a second call returns the same row.
        let again = store.get_or_create_slot_instance(job_id, "default", "db", 0)?;
        assert_eq!(again.instance_id, created.instance_id);
        assert_eq!(again.created_at, created.created_at);

        store.set_instance_state(&created.instance_id, InstanceState::Active)?;
        store.set_instance_current_alloc(&created.instance_id, Some(77))?;
        let updated = store
            .get_instance(&created.instance_id)?
            .context("instance exists")?;
        assert_eq!(updated.state, InstanceState::Active);
        assert_eq!(updated.current_alloc_id, Some(77));

        store.set_instance_current_alloc(&created.instance_id, None)?;
        let cleared = store
            .get_instance(&created.instance_id)?
            .context("instance exists")?;
        assert_eq!(cleared.current_alloc_id, None);

        // Upsert overwrites mutable fields and keeps identity.
        let upserted = store.upsert_instance(&Instance {
            storage_handle: Some("local-dir/manual".into()),
            state: InstanceState::Terminated,
            ..cleared
        })?;
        assert_eq!(upserted.state, InstanceState::Terminated);
        assert_eq!(upserted.storage_handle.as_deref(), Some("local-dir/manual"));

        // Errors on unknown ids.
        assert!(store
            .set_instance_state("missing", InstanceState::Active)
            .is_err());
        assert!(store.set_instance_current_alloc("missing", None).is_err());
        Ok(())
    }

    #[test]
    fn terminate_instances_for_job_retains_rows_and_handles() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "inst-stop";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 2, true)])?;
        let task_id = task_id_for(&store, "db", job_id)?;
        store.place_alloc("node-a", task_id)?;
        store.place_alloc("node-a", task_id)?;

        let handles: Vec<Option<String>> = store
            .list_instances(job_id)?
            .into_iter()
            .map(|instance| instance.storage_handle)
            .collect();

        assert_eq!(store.terminate_instances_for_job(job_id)?, 2);
        let instances = store.list_instances(job_id)?;
        assert_eq!(instances.len(), 2);
        for (instance, handle) in instances.iter().zip(&handles) {
            assert_eq!(instance.state, InstanceState::Terminated);
            assert_eq!(instance.current_alloc_id, None);
            assert_eq!(&instance.storage_handle, handle);
        }

        // Idempotent.
        assert_eq!(store.terminate_instances_for_job(job_id)?, 0);
        Ok(())
    }

    fn task_id_for_config(store: &Store, task_name: &str, job_config_id: &str) -> R<i64> {
        let conn = store.read_connection()?;
        let id = conn.query_row(
            "SELECT id FROM task_config WHERE name = ? AND job_config_id = ?;",
            [task_name, job_config_id],
            |row| row.get(0),
        )?;
        Ok(id)
    }

    //
    // KEYED-MODE INSTANCES (sandbox style; see docs/stateful-workloads.md)
    //

    fn stateful_keyed_task(name: &str, with_storage: bool) -> TaskConfig {
        TaskConfig {
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Keyed,
                id_source: None,
                storage: with_storage.then(|| crate::structs::StatefulStorageConfig {
                    path: "/workspace".into(),
                    retain_on_scale_in: None,
                }),
            }),
            ..TaskConfig::new(name, "noop")
        }
    }

    #[test]
    fn keyed_placement_binds_each_live_instance_and_mints_handles() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-place";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;
        let task_id = task_id_for(&store, "sandbox", job_id)?;

        let first = store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        let second = store.create_keyed_instance(job_id, "default", "sandbox", Some("user-b"))?;
        assert_eq!(first.state, InstanceState::Pending);
        assert_eq!(first.slot_index, None);
        assert_eq!(first.storage_handle, None, "handle minted at placement");
        assert_eq!(second.instance_id, "user-b");

        let alloc_one = store.place_alloc("node-a", task_id)?;
        let alloc_two = store.place_alloc("node-a", task_id)?;
        let mut bound: Vec<&str> = vec![
            alloc_one.instance_id.as_deref().context("bound")?,
            alloc_two.instance_id.as_deref().context("bound")?,
        ];
        bound.sort_unstable();
        assert_eq!(bound, vec!["user-a", "user-b"]);

        for id in ["user-a", "user-b"] {
            let instance = store.get_instance(id)?.context("instance")?;
            assert_eq!(instance.state, InstanceState::Active);
            assert!(instance
                .storage_handle
                .as_deref()
                .is_some_and(|handle| handle.starts_with("local-dir/")));
            assert!(instance.current_alloc_id.is_some());
        }

        // No unbound live instance left: a third placement must fail loudly
        // instead of inventing an instance.
        let err = store.place_alloc("node-a", task_id).expect_err("no free");
        assert!(
            err.to_string().contains("CreateInstance"),
            "error should point at the management API: {err}"
        );
        Ok(())
    }

    #[test]
    fn keyed_allocation_loss_sets_rescheduling_and_replacement_rebinds_identity() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-resched";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;
        let task_id = task_id_for(&store, "sandbox", job_id)?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;

        let alloc = store.place_alloc("node-a", task_id)?;
        let handle = store
            .get_instance("user-a")?
            .context("instance")?
            .storage_handle;
        assert!(handle.is_some());

        // The release hook must NOT terminate a keyed instance just because
        // its allocation died — only DestroyInstance terminates keyed rows.
        store.remove_allocation_by_id(alloc.id)?;
        let waiting = store.get_instance("user-a")?.context("retained")?;
        assert_eq!(waiting.state, InstanceState::Rescheduling);
        assert_eq!(waiting.current_alloc_id, None);
        assert_eq!(waiting.storage_handle, handle, "handle survives the loss");

        // The replacement re-binds the same id + storage handle.
        let replacement = store.place_alloc("node-a", task_id)?;
        assert_eq!(replacement.instance_id.as_deref(), Some("user-a"));
        let reactivated = store.get_instance("user-a")?.context("reactivated")?;
        assert_eq!(reactivated.state, InstanceState::Active);
        assert_eq!(reactivated.current_alloc_id, Some(replacement.id));
        assert_eq!(reactivated.storage_handle, handle);
        Ok(())
    }

    #[test]
    fn create_keyed_instance_rejects_collisions_including_tombstones() -> R<()> {
        let store = setup_store()?;
        let job_id = "keyed-recycle";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;

        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;

        // Live collision.
        let err = store
            .create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))
            .expect_err("live collision");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::Conflict { .. })
        ));

        // Tombstone collision: a destroyed id can NEVER be created again.
        store.destroy_keyed_instance("user-a")?;
        let err = store
            .create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))
            .expect_err("tombstoned ids are never recycled");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::Conflict { .. })
        ));

        // Server-minted ids are fresh and unique.
        let minted = store.create_keyed_instance(job_id, "default", "sandbox", None)?;
        assert_ne!(minted.instance_id, "user-a");
        assert!(minted.instance_id.starts_with("sandbox-"));
        assert!(crate::structs::validate_keyed_instance_id(&minted.instance_id).is_ok());
        let minted_two = store.create_keyed_instance(job_id, "default", "sandbox", None)?;
        assert_ne!(minted.instance_id, minted_two.instance_id);
        Ok(())
    }

    #[test]
    fn destroy_keyed_instance_stops_alloc_terminates_and_drops_handle() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-destroy";
        submit_job(
            &store,
            job_id,
            vec![
                stateful_keyed_task("sandbox", true),
                stateful_slot_task("db", 1, true),
            ],
        )?;
        let keyed_task = task_id_for(&store, "sandbox", job_id)?;
        let slot_task = task_id_for(&store, "db", job_id)?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        let alloc = store.place_alloc("node-a", keyed_task)?;
        let slot_alloc = store.place_alloc("node-a", slot_task)?;

        let outcome = store.destroy_keyed_instance("user-a")?;
        assert_eq!(
            outcome,
            DestroyInstanceOutcome::Destroyed {
                removed_allocations: 1
            }
        );
        assert!(
            store.get_allocation_by_id(alloc.id)?.is_none(),
            "the allocation is deleted through the normal stop path"
        );
        let tombstone = store.get_instance("user-a")?.context("row retained")?;
        assert_eq!(tombstone.state, InstanceState::Terminated);
        assert_eq!(tombstone.current_alloc_id, None);
        assert_eq!(
            tombstone.storage_handle, None,
            "keyed destroy always drops storage (unlike slot scale-in retention)"
        );

        // Idempotent no-op on an already-terminated instance.
        assert_eq!(
            store.destroy_keyed_instance("user-a")?,
            DestroyInstanceOutcome::AlreadyTerminated
        );

        // Slot instances are owned by the scheduler, not this API.
        let slot_id = slot_alloc.instance_id.context("slot bound")?;
        let err = store
            .destroy_keyed_instance(&slot_id)
            .expect_err("slot rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::InvalidInput { .. })
        ));

        // Unknown ids surface NotFound.
        let err = store
            .destroy_keyed_instance("missing")
            .expect_err("missing rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::NotFound { .. })
        ));
        Ok(())
    }

    #[test]
    fn job_stop_termination_drops_keyed_handles_but_retains_slot_handles() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-stop";
        submit_job(
            &store,
            job_id,
            vec![
                stateful_keyed_task("sandbox", true),
                stateful_slot_task("db", 1, true),
            ],
        )?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        store.place_alloc("node-a", task_id_for(&store, "sandbox", job_id)?)?;
        store.place_alloc("node-a", task_id_for(&store, "db", job_id)?)?;

        assert_eq!(store.terminate_instances_for_job(job_id)?, 2);
        let keyed = store.get_instance("user-a")?.context("keyed row")?;
        assert_eq!(keyed.state, InstanceState::Terminated);
        assert_eq!(keyed.storage_handle, None, "keyed handles dropped on stop");
        let slot = store
            .get_instance(&slot_instance_id(job_id, "default", 0))?
            .context("slot row")?;
        assert_eq!(slot.state, InstanceState::Terminated);
        assert!(slot.storage_handle.is_some(), "slot handles retained");
        Ok(())
    }

    #[test]
    fn count_live_keyed_instances_counts_only_live_rows_of_the_task() -> R<()> {
        let store = setup_store()?;
        let job_id = "keyed-count";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", false)])?;

        assert_eq!(
            store.count_live_keyed_instances(job_id, "default", "sandbox")?,
            0
        );
        store.create_keyed_instance(job_id, "default", "sandbox", Some("a"))?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("b"))?;
        assert_eq!(
            store.count_live_keyed_instances(job_id, "default", "sandbox")?,
            2
        );

        // Terminated tombstones do not count as desired.
        store.destroy_keyed_instance("a")?;
        assert_eq!(
            store.count_live_keyed_instances(job_id, "default", "sandbox")?,
            1
        );

        // Other (job, group, task) scopes are not counted.
        assert_eq!(
            store.count_live_keyed_instances(job_id, "other", "sandbox")?,
            0
        );
        assert_eq!(
            store.count_live_keyed_instances("nope", "default", "sandbox")?,
            0
        );
        Ok(())
    }

    // #[test]
    // fn handle_job_submit_with_unchanged_job_and_force_update_works() -> R<()> {
    //     assert!(true);
    //     let _store = setup_store()?;
    //     Ok(())
    // }

    // #[test]
    // fn handle_job_stop() -> R<()> {
    //     assert!(true);
    //     let _store = setup_store()?;
    //     Ok(())
    // }

    // #[test]
    // fn handle_job_purge() -> R<()> {
    //     assert!(true);
    //     let _store = setup_store()?;
    //     Ok(())
    // }
}
