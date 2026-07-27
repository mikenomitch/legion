// A few low-level persistence helpers take many columns by parameter.
#![allow(clippy::too_many_arguments)]

use crate::constraints::{ConstraintSet, DistinctTracker, PermitSet};
use crate::prelude::*;
use crate::resources::ResourcePolicy;
use crate::storage::errors::StorageError;
use crate::storage::events::{drafts, EventDraft, SchedulerEventFilter, SchedulerEventRecord};
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
    ManagerNodeConfig, Node, NodeMsg, ReplaceAfter, ResolvedOnLostPolicy, Rollout, RolloutConfig,
    SigningKeyRecord, StatefulConfig, StatefulOnLostConfig, Submission, TaskCheck, TaskConfig,
    TaskCount, TaskDependency, TaskLifecycle, TaskOnUnhealthyConfig, TaskSchedule, TaskStatusState,
    Volume, WorkerConfig,
};
use crate::util::random_string;

use lazy_static::lazy_static;
use rand::{thread_rng, Rng};
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
use tokio::sync::{broadcast, Notify};

// warning incorrectly
#[allow(dead_code)]
pub enum Msg {
    Content(String),
}

pub type MemSafeConn = Arc<Mutex<Connection>>;
const READ_MODEL_LOCK_ERROR: &str = "Error locking read model";

/// RFC3339 with the subsecond field pinned to nine digits, so the rendered
/// string sorts lexicographically in chronological order. Used for any
/// timestamp SQL compares or orders by (see `Store::sortable_timestamp`).
const SORTABLE_TIMESTAMP_FORMAT: &[time::format_description::BorrowedFormatItem<'_>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]Z");

/// Parse a nullable JSONB text column into an optional typed value, mapping
/// parse failures onto rusqlite conversion errors.
fn parse_json_column<T: serde::de::DeserializeOwned>(
    idx: usize,
    raw: Option<String>,
) -> Result<Option<T>, rusqlite::Error> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    serde_json::from_str(&raw).map(Some).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(idx, rusqlite::types::Type::Text, Box::new(err))
    })
}

/// Extract the host portion of a node's advertised `workerAddress` config
/// attribute (for example `http://10.0.0.5:4848` -> `10.0.0.5`).
fn worker_address_host(config_attrs: &Value) -> Option<String> {
    let address = config_attrs.as_object()?.get("workerAddress")?.as_str()?;
    let without_scheme = address
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(address);
    let host = without_scheme
        .split('/')
        .next()
        .unwrap_or(without_scheme)
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(without_scheme);
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

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
    network_raw: Option<String>,
}

/// A service declared in a job's latest config. Endpoint counts come from
/// `service_endpoints`.
#[derive(Debug, Clone)]
pub struct DeclaredService {
    pub service: String,
    pub job_id: String,
    pub task_name: String,
    pub tags: Vec<String>,
    pub meta: BTreeMap<String, String>,
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
    // Live fan-out of scheduler events to StreamEvents subscribers. Persisted
    // history lives in the scheduler_event table; this only carries new events.
    event_tx: broadcast::Sender<SchedulerEventRecord>,
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

/// Result of [`Store::stop_keyed_instance`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopInstanceOutcome {
    /// The instance was live and is now `stopped` (row and storage handle
    /// retained); `removed_allocations` allocation rows were deleted (0 when
    /// it had no current placement).
    Stopped { removed_allocations: usize },
    /// The instance was already stopped; nothing changed (idempotent no-op).
    AlreadyStopped,
}

/// Result of [`Store::start_keyed_instance`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartInstanceOutcome {
    /// The instance was `stopped` and is now `pending`, awaiting placement.
    Started,
    /// The instance was already live (pending/active/rescheduling); nothing
    /// changed (idempotent no-op).
    AlreadyRunning,
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
    /// JSON-encoded per-artifact staging states from the worker heartbeat
    /// (`Vec<ArtifactStatus>`); `None` when the task declares no artifacts.
    pub artifact_states: Option<String>,
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
// Live subscribers that fall this many events behind must catch up from the
// scheduler_event table (StreamEvents handles the Lagged case).
const EVENT_BROADCAST_CAPACITY: usize = 1_024;
// Retained journal size; older scheduler events are pruned opportunistically.
const EVENT_RETAINED_ROWS: i64 = 10_000;
// Prune once every N inserts to keep the hot emit path cheap.
const EVENT_PRUNE_INTERVAL: i64 = 256;

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
            event_tx: broadcast::channel(EVENT_BROADCAST_CAPACITY).0,
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
        // The store holds secret material — ACL/join-token hashes and the OIDC
        // private signing key (a leak of which lets anyone mint workload
        // identities trusted by AWS/GCP/Vault). SQLite creates store.sqlite and
        // its WAL/-shm siblings at the process umask (commonly 0644), so lock
        // the containing directory to owner-only; the 0700 dir denies traversal
        // to those files regardless of their own modes.
        crate::util::restrict_dir_to_owner(std::path::Path::new(store_path))?;
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

    pub(crate) fn write_connection(&self) -> R<MutexGuard<'_, Connection>> {
        Self::lock_conn(&self.write_conn, "write")
    }

    pub(crate) fn read_connection(&self) -> R<MutexGuard<'_, Connection>> {
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
            SELECT id, name, kind, count, count_per, group_name, lifecycle, schedule, depends_on, config, resources, constraints, permit, affinities, checks, on_unhealthy, kv_injections, job_config_id, identities, stateful, artifacts, network, service, actions, volumes
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
                artifacts: self.parse_artifacts_column(20, row.get(20)?)?,
                network: parse_json_column(21, row.get(21)?)?,
                service: parse_json_column(22, row.get(22)?)?,
                actions: self.parse_actions_column(23, row.get(23)?)?,
                volumes: self.parse_volumes_column(24, row.get(24)?)?,
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
            let rejected_event = self.stage_event_locked(
                &tx,
                drafts::plan_rejected(&submission.job_config_id, &reason),
            );
            tx.commit()?;
            self.publish_staged(rejected_event);
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
            let placeholders = std::iter::repeat_n("?", node_ids.len())
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
                let rejected_event = self.stage_event_locked(
                    &tx,
                    drafts::plan_rejected(&submission.job_config_id, &reason),
                );
                tx.commit()?;
                self.publish_staged(rejected_event);
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

            let network =
                self.assign_allocation_network(&tx, node_id, *task_id, state.network_raw.clone())?;
            let allocation = self.insert_allocation_with_data(
                &tx,
                node_id,
                *task_id,
                &state.job_config_id,
                &state.resources,
                network.as_ref(),
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
        let placed_event = drafts::allocations_placed(&submission.job_config_id, &allocations)
            .and_then(|draft| self.stage_event_locked(&tx, draft));
        tx.commit()?;
        self.publish_staged(placed_event);
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

    pub(crate) fn collect_rows<T, I>(iter: I) -> R<Vec<T>>
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

    /// RFC3339 with a fixed-width, nine-digit subsecond field.
    ///
    /// `Rfc3339` trims trailing zeros from the subsecond field, so its output
    /// has a variable length and does NOT sort lexicographically in
    /// chronological order: `…:03.4374Z` compares GREATER than
    /// `…:03.437409Z` because `'Z' > '0'`. Any column SQL orders or ranges
    /// with `<=` must therefore be written through this format instead.
    fn sortable_timestamp(at: OffsetDateTime) -> R<String> {
        Ok(at.format(SORTABLE_TIMESTAMP_FORMAT)?)
    }

    fn sortable_now() -> R<String> {
        Self::sortable_timestamp(OffsetDateTime::now_utc())
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
            artifact_states: row.get(5)?,
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
        let artifacts = row.get(8)?;

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
            artifacts,
            host_volumes: row.get(9)?,
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

    fn parse_artifacts_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Vec<crate::structs::ArtifactSpec>, rusqlite::Error> {
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

    fn parse_volumes_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Vec<crate::structs::VolumeSpec>, rusqlite::Error> {
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

    fn parse_actions_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Vec<crate::structs::ActionSpec>, rusqlite::Error> {
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
            artifacts: self.parse_artifacts_column(21, row.get(21)?)?,
            network: parse_json_column(22, row.get(22)?)?,
            service: parse_json_column(23, row.get(23)?)?,
            actions: self.parse_actions_column(24, row.get(24)?)?,
            volumes: self.parse_volumes_column(25, row.get(25)?)?,
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
            network: parse_json_column(8, row.get(8)?)?,
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
        // A ready node is by definition not lost: clear lost_at so the
        // node-loss confirmation gate stops counting (its gated allocations
        // keep running, bound to their instances).
        let sql = r#"
        UPDATE node
        SET status = :status, availability = :availability, last_heartbeat = :last_heartbeat,
            lost_at = NULL
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

        self.events().node_ready(id);

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn mark_node_lost(&self, id: &str) -> R<()> {
        let conn = self.write_connection()?;
        // lost_at is set once (COALESCE) so repeated marks never push the
        // loss-confirmation clock back; it is cleared when the node
        // heartbeats again or re-registers.
        let sql = r#"
        UPDATE node
        SET status = :status, availability = :availability,
            lost_at = COALESCE(lost_at, :lost_at)
        WHERE id = :id;
        "#;

        let params = named_params! {
            ":id": id,
            ":status": "lost",
            ":availability": "unavailable",
            ":lost_at": Self::now_rfc3339()?,
        };

        conn.execute(sql, params)?;

        drop(conn);
        let node = self.get_node(id)?;
        self.cache_node(node)?;

        self.events().node_lost(id);

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
        drop(conn);

        self.cache_clear_allocations_for_node(node_id)?;

        self.events()
            .allocations_removed_from_node(node_id, &allocs);

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
            // See mark_node_lost: lost_at is set once and never refreshed
            // while the node stays lost.
            let sql = format!(
                "UPDATE node SET status = 'lost', availability = 'unavailable', \
                 lost_at = COALESCE(lost_at, '{}') WHERE id IN ({})",
                Self::now_rfc3339()?,
                placeholders
            );

            let mut stmt = conn.prepare(&sql)?;
            stmt.execute(params_from_iter(ids))?;
        }

        self.cache_nodes_lost(ids)?;

        for node_id in ids {
            self.events().node_lost_after_missed_heartbeats(node_id);
        }

        // Per-node, log-and-continue: a job that currently has nowhere to go
        // (cluster at capacity, unsatisfiable constraints) must not stop the
        // other lost nodes from being drained, nor abort the caller's
        // node-status sweep. `recheck_lost_nodes` retries every tick.
        for node_id in ids {
            if let Err(err) = self.reschedule_allocations_for_lost_node(node_id) {
                warn!(
                    %node_id,
                    error = ?err,
                    "Failed rescheduling allocations off lost node"
                );
            }
        }

        Ok(())
    }

    /// When the node was marked lost (RFC3339), or `None` while it is not
    /// lost. Drives the stateful node-loss confirmation gate (see
    /// [`Self::reschedule_allocations_for_lost_node`]).
    #[instrument(err, skip_all)]
    pub fn node_lost_at(&self, node_id: &str) -> R<Option<String>> {
        let conn = self.read_connection()?;
        let lost_at: Option<String> = conn
            .query_row("SELECT lost_at FROM node WHERE id = ?;", [node_id], |row| {
                row.get(0)
            })
            .optional()?
            .flatten();
        Ok(lost_at)
    }

    /// Overwrite a node's `lost_at` timestamp. Test/ops helper for the
    /// node-loss confirmation gate: lets a loss be backdated so the
    /// `replaceAfter` threshold can be exercised without real waiting.
    #[instrument(err, skip_all)]
    pub fn set_node_lost_at(&self, node_id: &str, lost_at: &str) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute(
            "UPDATE node SET lost_at = ? WHERE id = ?;",
            params![lost_at, node_id],
        )?;
        Ok(())
    }

    /// Ids of all nodes currently marked lost.
    #[instrument(err, skip_all)]
    pub fn lost_node_ids(&self) -> R<Vec<String>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare("SELECT id FROM node WHERE status = 'lost';")?;
        let ids = Self::collect_rows(stmt.query_map([], |row| row.get(0))?)?;
        Ok(ids)
    }

    /// Re-run the lost-node reschedule path for every node still marked
    /// lost, so stateful allocations whose `onLost.replaceAfter` threshold
    /// has since elapsed get replaced WITHOUT operator action. Called by the
    /// manager's node-status monitor on every tick (the same loop that marks
    /// nodes lost); errors are logged per node so one wedged job cannot
    /// stall the sweep.
    #[instrument(skip_all)]
    pub fn recheck_lost_nodes(&self) {
        let node_ids = match self.lost_node_ids() {
            Ok(node_ids) => node_ids,
            Err(err) => {
                warn!(error = ?err, "Failed listing lost nodes for loss-confirmation recheck");
                return;
            }
        };
        for node_id in node_ids {
            if let Err(err) = self.reschedule_allocations_for_lost_node(&node_id) {
                warn!(
                    %node_id,
                    error = ?err,
                    "Failed rechecking lost node for confirmed-loss replacements"
                );
            }
        }
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
        drop(conn);

        self.cache_node(node.clone())?;

        self.events().node_registered(&node);

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
        SET last_heartbeat = :ts, status = 'active', lost_at = NULL
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
            ":artifacts": config.artifacts,
            ":host_volumes": config.host_volumes,
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
        drop(conn);

        // TODO: Create a job_update record
        // that kicks off the eval?
        // ... or maybe just this adds the eval straight up?

        self.events().job_stopped(name);

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
        drop(conn);

        self.events().job_failed(job_id);

        Ok(())
    }

    /// Mark a job failed because a required KV dependency vanished
    /// (`onDelete: fail`). The job status becomes `failed` and the impacted
    /// allocations' status is stamped `failed:<reason>` so the diagnostic is
    /// visible in `job list`, the ledger, and state-store queries.
    ///
    /// Scope: when `impacted_tasks` is `Some`, only allocations whose task
    /// config is one of those (by name, across all of the job's config
    /// versions) are stamped — a missing injection key impacts just the tasks
    /// that depend on it, leaving sibling tasks' allocations untouched. When
    /// `None` (a legacy `$key` reference, which is not scoped to one task and
    /// makes the whole config unrenderable), every allocation of the job is
    /// stamped.
    ///
    /// The allocation rows are retained (the workload is not torn down and no
    /// reschedule storm starts); a later KV put that triggers a restart or
    /// rollout recovers the job (see `docs/kv-store.md`).
    #[instrument(err, skip(self))]
    pub fn mark_job_failed_for_kv(
        &self,
        job_id: &str,
        reason: &str,
        impacted_tasks: Option<&[String]>,
    ) -> R<()> {
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        tx.execute("UPDATE job SET status = 'failed' WHERE id = ?;", [job_id])?;
        let stamp = format!("failed:{reason}");
        match impacted_tasks {
            Some(tasks) if !tasks.is_empty() => {
                // Build an IN (...) list of task-config ids for the named tasks
                // across every config version of the job. rusqlite has no array
                // binding, so bind the names positionally.
                let placeholders = std::iter::repeat_n("?", tasks.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "UPDATE allocation SET status = ?1 \
                     WHERE task_config_id IN (\
                         SELECT id FROM task_config WHERE job_id = ?2 AND name IN ({placeholders})\
                     );"
                );
                let mut params: Vec<&dyn rusqlite::ToSql> = vec![&stamp, &job_id];
                for task in tasks {
                    params.push(task);
                }
                tx.execute(&sql, params.as_slice())?;
            }
            _ => {
                tx.execute(
                    "UPDATE allocation SET status = ?1 \
                     WHERE job_config_id IN (SELECT id FROM job_config WHERE job_id = ?2);",
                    (stamp.as_str(), job_id),
                )?;
            }
        }
        tx.commit()?;
        self.refresh_read_model()?;
        Ok(())
    }

    /// Look up a job's status by its job id (== name). Returns `None` when the
    /// job row is absent.
    pub fn job_status_by_id(&self, job_id: &str) -> R<Option<String>> {
        let conn = self.read_connection()?;
        let status: Option<String> = conn
            .query_row("SELECT status FROM job WHERE id = ?;", [job_id], |row| {
                row.get(0)
            })
            .optional()?;
        Ok(status)
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

    /// Clear a `stopped` job status (back to `done`) when the job is
    /// resubmitted via RunJob. A resubmit revives the job, so the
    /// instance-management RPCs (CreateInstance/StartInstance), which reject
    /// stopped jobs, must accept it again. Stopped keyed instances
    /// deliberately stay `stopped` through this — reviving each one takes an
    /// explicit StartInstance. No-op when the job is not currently stopped.
    /// Returns whether a row was updated.
    #[instrument(err, skip(self))]
    pub fn clear_job_stopped_status(&self, job_id: &str) -> R<bool> {
        let conn = self.write_connection()?;
        let updated = conn.execute(
            "UPDATE job SET status = 'done' WHERE id = ?1 AND status = 'stopped';",
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
            .map_err(|_| anyhow!("Error committing transaction"))?;
        drop(conn);

        self.events().job_destroyed(name);

        Ok(())
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
            let network = t.network.as_ref().map(serde_json::to_string).transpose()?;
            let service = t.service.as_ref().map(serde_json::to_string).transpose()?;
            let identities = if t.identities.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&t.identities)?)
            };
            let stateful = t.stateful.as_ref().map(serde_json::to_string).transpose()?;
            let artifacts = if t.artifacts.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&t.artifacts)?)
            };
            let actions = if t.actions.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&t.actions)?)
            };
            let volumes = if t.volumes.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&t.volumes)?)
            };
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
                ":artifacts": artifacts,
                ":network": network,
                ":service": service,
                ":actions": actions,
                ":volumes": volumes,
            };

            let task_id: i64 = tx.query_row(&INSERT_TASK, task_params, |row| row.get(0))?;
            // Claim rows exist from submit so `legion volume list` shows what a
            // job asked for before anything is placed, and a resubmit refreshes
            // params without disturbing a live volume's pin.
            self.sync_task_volume_claims_tx(&tx, &job.id, t)?;
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

        let outcome = if was_update {
            JobSubmitOutcome::Updated { job_config_id }
        } else {
            JobSubmitOutcome::Created { job_config_id }
        };
        let submit_event =
            drafts::job_submit_outcome(&job.id, &outcome, next_version, config.tasks.len())
                .and_then(|draft| self.stage_event_locked(&tx, draft));
        tx.commit()
            .map_err(|_| anyhow!("Error committing transaction"))?;
        self.publish_staged(submit_event);
        drop(conn);

        let cached_tasks = self.get_task_configs_for_job_config(outcome.job_config_id())?;
        self.cache_set_task_configs_for_job_config(outcome.job_config_id(), cached_tasks)?;

        Ok(outcome)
    }

    pub fn normalize_job_config(&self, config: &JobConfig) -> R<JobConfig> {
        let tasks = config
            .tasks
            .iter()
            .map(|task| {
                let resources = self
                    .resource_policy
                    .normalize_task_resources(&task.resources, true)?;
                let mut task = TaskConfig {
                    resources,
                    ..task.clone()
                };
                task.normalize_network()?;
                Ok(task)
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
        drop(conn);

        self.events().rollout_event(
            rollout_id,
            job_id,
            level,
            &event.message,
            event.group_name.as_deref(),
            &event.metadata_json,
        );

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

    /// Record a scheduler event: persist it to the journal and publish it to
    /// live `StreamEvents` subscribers.
    ///
    /// Takes the write lock for the insert, so only use this from code that
    /// is not already holding a write connection; code with an open write
    /// transaction stages the event inside it via `stage_event_locked` +
    /// `publish_staged` instead, which costs no extra lock acquisition or
    /// WAL commit.
    ///
    /// Emission is strictly best-effort — scheduling work must never fail
    /// because the event journal could not be written, so errors are logged
    /// and swallowed here.
    pub fn emit_event(&self, draft: EventDraft) {
        let conn = match self.write_connection() {
            Ok(conn) => conn,
            Err(err) => {
                warn!(kind = %draft.kind, error = ?err, "Failed locking store for scheduler event");
                return;
            }
        };
        let event = self.stage_event_locked(&conn, draft);
        self.publish_staged(event);
    }

    /// Insert `draft` through an already-held write connection or open
    /// transaction, returning the persisted record for later publication.
    ///
    /// Best-effort: failures are logged and `None` is returned. When called
    /// with a transaction, the event commits or rolls back atomically with
    /// the state change it describes.
    fn stage_event_locked(
        &self,
        conn: &Connection,
        draft: EventDraft,
    ) -> Option<SchedulerEventRecord> {
        // Operator escape hatch: silences event persistence and streaming
        // entirely. Also how the benches A/B the cost of emission itself
        // (see docs/event-stream.md).
        if std::env::var_os("LEGION_EVENTS_OFF").is_some() {
            return None;
        }
        match self.insert_event(conn, &draft) {
            Ok(event) => Some(event),
            Err(err) => {
                warn!(
                    kind = %draft.kind,
                    error = ?err,
                    "Failed recording scheduler event"
                );
                None
            }
        }
    }

    /// Publish a staged event to live subscribers once its transaction has
    /// committed. Call while still holding the write guard the event was
    /// staged under, so subscribers observe events in strict ID order.
    fn publish_staged(&self, event: Option<SchedulerEventRecord>) {
        if let Some(event) = event {
            // send only fails when there are no live subscribers; fine.
            let _ = self.event_tx.send(event);
        }
    }

    fn insert_event(&self, conn: &Connection, draft: &EventDraft) -> R<SchedulerEventRecord> {
        let created_at = Self::now_rfc3339()?;
        let metadata_json = if draft.metadata.is_object() {
            draft.metadata.to_string()
        } else {
            json!({ "value": draft.metadata }).to_string()
        };

        conn.execute(
            "INSERT INTO scheduler_event (
                kind, level, job_id, node_id, eval_id, allocation_id, rollout_id,
                message, metadata_json, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10);",
            params![
                draft.kind,
                draft.level,
                draft.job_id,
                draft.node_id,
                draft.eval_id,
                draft.allocation_id,
                draft.rollout_id,
                draft.message,
                metadata_json,
                created_at
            ],
        )?;
        // Safe under the held write-connection guard: nothing can insert
        // between the statement above and this read.
        let id = conn.last_insert_rowid();

        if id % EVENT_PRUNE_INTERVAL == 0 {
            conn.execute(
                "DELETE FROM scheduler_event WHERE id <= ?1;",
                [id - EVENT_RETAINED_ROWS],
            )?;
        }

        Ok(SchedulerEventRecord {
            id,
            kind: draft.kind.clone(),
            level: draft.level.clone(),
            job_id: draft.job_id.clone(),
            node_id: draft.node_id.clone(),
            eval_id: draft.eval_id.clone(),
            allocation_id: draft.allocation_id,
            rollout_id: draft.rollout_id,
            message: draft.message.clone(),
            metadata_json,
            created_at,
        })
    }

    /// Subscribe to live scheduler events. Receivers that lag behind the
    /// broadcast buffer should resume from the journal via
    /// `list_scheduler_events` using the last event ID they saw.
    pub fn subscribe_events(&self) -> broadcast::Receiver<SchedulerEventRecord> {
        self.event_tx.subscribe()
    }

    /// List journal events matching `filter`, ordered oldest-to-newest.
    ///
    /// Without an `after_id` cursor this returns the most recent
    /// `filter.effective_limit()` events; with a cursor it pages forward from
    /// `after_id` so pollers never skip events.
    #[instrument(err, skip_all)]
    pub fn list_scheduler_events(
        &self,
        filter: &SchedulerEventFilter,
    ) -> R<Vec<SchedulerEventRecord>> {
        let conn = self.read_connection()?;
        let limit = filter.effective_limit() as i64;

        let mut clauses: Vec<String> = Vec::new();
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        if let Some(after_id) = filter.after_id {
            clauses.push(format!("id > ?{}", params.len() + 1));
            params.push(Box::new(after_id));
        }
        if let Some(kind) = filter.kind.as_deref() {
            clauses.push(format!(
                "(kind = ?{} OR kind LIKE ?{} || '.%')",
                params.len() + 1,
                params.len() + 2
            ));
            params.push(Box::new(kind.to_string()));
            params.push(Box::new(kind.to_string()));
        }
        if let Some(job_id) = filter.job_id.as_deref() {
            clauses.push(format!("job_id = ?{}", params.len() + 1));
            params.push(Box::new(job_id.to_string()));
        }
        if let Some(node_id) = filter.node_id.as_deref() {
            clauses.push(format!("node_id = ?{}", params.len() + 1));
            params.push(Box::new(node_id.to_string()));
        }
        if let Some(level) = filter.level.as_deref() {
            clauses.push(format!("level = ?{}", params.len() + 1));
            params.push(Box::new(level.to_string()));
        }
        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        params.push(Box::new(limit));
        let limit_param = params.len();

        const EVENT_COLUMNS: &str = "id, kind, level, job_id, node_id, eval_id, allocation_id, rollout_id, message, metadata_json, created_at";
        // With a cursor, page forward (oldest first). Without one, take the
        // newest window and re-sort ascending, like list_rollout_events.
        let sql = if filter.after_id.is_some() {
            format!(
                "SELECT {EVENT_COLUMNS} FROM scheduler_event {where_clause} ORDER BY id ASC LIMIT ?{limit_param};"
            )
        } else {
            format!(
                "SELECT {EVENT_COLUMNS} FROM (
                    SELECT {EVENT_COLUMNS} FROM scheduler_event {where_clause} ORDER BY id DESC LIMIT ?{limit_param}
                ) ORDER BY id ASC;"
            )
        };

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            params_from_iter(params.iter().map(|param| param.as_ref())),
            |row| self.scheduler_event_from_row(row),
        )?;
        Self::collect_rows(rows)
    }

    /// Highest event ID currently in the journal (0 when empty). Used by
    /// clients to initialize a polling/streaming cursor.
    #[instrument(err, skip_all)]
    pub fn latest_scheduler_event_id(&self) -> R<i64> {
        let conn = self.read_connection()?;
        let latest: Option<i64> =
            conn.query_row("SELECT MAX(id) FROM scheduler_event;", [], |row| row.get(0))?;
        Ok(latest.unwrap_or(0))
    }

    fn scheduler_event_from_row(
        &self,
        row: &Row<'_>,
    ) -> Result<SchedulerEventRecord, rusqlite::Error> {
        Ok(SchedulerEventRecord {
            id: row.get(0)?,
            kind: row.get(1)?,
            level: row.get(2)?,
            job_id: row.get(3)?,
            node_id: row.get(4)?,
            eval_id: row.get(5)?,
            allocation_id: row.get(6)?,
            rollout_id: row.get(7)?,
            message: row.get(8)?,
            metadata_json: row.get(9)?,
            created_at: row.get(10)?,
        })
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
                .or_default();
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
        let (job_config_id, resources, network_raw): (String, Value, Option<String>) = tx
            .query_row(
                "SELECT job_config_id, resources, network FROM task_config WHERE id = ?;",
                [task_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;

        let network = self.assign_allocation_network(tx, node_id, task_id, network_raw)?;
        self.insert_allocation_with_data(
            tx,
            node_id,
            task_id,
            &job_config_id,
            &resources,
            network.as_ref(),
        )
    }

    /// Assign host ports for a new allocation of `task_id` on `node_id` inside
    /// the current transaction. Reads ports already assigned on the node so
    /// concurrent placements in the same tx or committed earlier cannot
    /// collide. Static port conflicts fail placement with a node-scoped error.
    fn assign_allocation_network(
        &self,
        tx: &Transaction<'_>,
        node_id: &str,
        task_id: i64,
        network_raw: Option<String>,
    ) -> R<Option<crate::structs::AllocNetwork>> {
        let Some(raw) = network_raw else {
            return Ok(None);
        };
        let network: crate::structs::NetworkConfig = from_str(&raw)
            .with_context(|| format!("invalid stored network config for task {task_id}"))?;
        if network.ports.is_empty() {
            return Ok(None);
        }

        let mut used: HashSet<u16> = HashSet::new();
        let mut stmt = tx
            .prepare("SELECT network FROM allocation WHERE node_id = ? AND network IS NOT NULL;")?;
        let rows = stmt.query_map([node_id], |row| row.get::<_, Option<String>>(0))?;
        for row in rows {
            let Some(raw) = row? else { continue };
            let existing: crate::structs::AllocNetwork = from_str(&raw)
                .with_context(|| format!("invalid stored allocation network on node {node_id}"))?;
            used.extend(existing.ports.iter().map(|port| port.host_port));
        }

        let ports = crate::structs::assign_ports(&network, &used)
            .with_context(|| format!("failed to assign ports on node {node_id}"))?;
        let address = tx
            .query_row(
                "SELECT config_attrs FROM node WHERE id = ?;",
                [node_id],
                |row| row.get::<_, Value>(0),
            )
            .ok()
            .and_then(|attrs| worker_address_host(&attrs));

        Ok(Some(crate::structs::AllocNetwork { address, ports }))
    }

    fn insert_allocation_with_data(
        &self,
        tx: &Transaction<'_>,
        node_id: &str,
        task_id: i64,
        job_config_id: &str,
        resources: &Value,
        network: Option<&crate::structs::AllocNetwork>,
    ) -> R<Allocation> {
        let network_json = network.map(to_string).transpose()?;
        // Stateful tasks: pick (or re-materialize) the instance this
        // allocation will fulfill before writing the row, so the binding is
        // atomic with the placement.
        let instance_id = self.assign_stateful_instance_tx(tx, task_id, node_id)?;

        let params = named_params! {
            ":status": "placed",
            ":resources": resources,
            ":node_id": node_id,
            ":job_config_id": job_config_id,
            ":task_config_id": task_id,
            ":instance_id": instance_id,
            ":network": network_json,
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
    fn assign_stateful_instance_tx(
        &self,
        tx: &Transaction<'_>,
        task_id: i64,
        node_id: &str,
    ) -> R<Option<String>> {
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
                .assign_keyed_instance_tx(
                    tx, task_id, &job_id, &group, &task_name, &stateful, node_id,
                )
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

        // Slots occupied by a live allocation of ANY config of this job
        // (e.g. the outgoing config mid-rollout). Preferring slots outside
        // this set makes the binder pick exactly the slot whose old
        // allocation was just retired, which is what lets a sequential
        // (`rollout.slots.order`) rollout replace highest-slot-first and what
        // keeps a staged (`updateFrom`) rollout from double-binding a slot
        // whose instance still runs the old config. The per-config `taken`
        // set above remains the hard constraint; the job-wide set is only a
        // preference so the fresh-plan-then-remove-previous submit flow
        // (which deliberately places new-config allocations while the old
        // config's are still live) keeps working.
        let mut job_taken_stmt = tx.prepare(
            "SELECT instance.slot_index FROM allocation \
             INNER JOIN instance ON instance.instance_id = allocation.instance_id \
             WHERE instance.job_id = ?1 AND instance.group_name = ?2 \
             AND instance.mode = 'slot';",
        )?;
        let job_taken: HashSet<u32> = Self::collect_rows(
            job_taken_stmt.query_map(params![job_id, group], |row| row.get::<_, Option<i64>>(0))?,
        )?
        .into_iter()
        .flatten()
        .map(|slot| slot as u32)
        .collect();

        let desired = count.max(0) as u32;
        // Node-local storage makes slot choice matter: an allocation that
        // landed on node-a1 must bind the slot whose data is ON node-a1, not
        // just the lowest free one. Preferring the pinned slot makes the
        // planner's pin and the binder's choice agree even if their orderings
        // diverge — the allocation always meets its own data.
        let pinned_slot = (0..desired)
            .filter(|slot| !taken.contains(slot))
            .find(|slot| {
                let candidate = slot_instance_id(&job_id, &group, *slot);
                matches!(
                    self.pinned_node_for_instance_tx(tx, &candidate),
                    Ok(Some(pinned)) if pinned == node_id
                )
            });
        let slot = pinned_slot
            .or_else(|| {
                (0..desired).find(|slot| !taken.contains(slot) && !job_taken.contains(slot))
            })
            .or_else(|| (0..desired).find(|slot| !taken.contains(slot)))
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
                // The slot id space is EXCLUSIVE: a row carrying this derived id
                // must be this very (job, group, task, slot) slot. If a row with
                // the same id exists but belongs to a different job/group/slot or
                // a different mode (e.g. a keyed instance some caller created with
                // `--id otherjob-default-0`), adopting it would let two jobs share
                // one instance row, storage handle, and workload-identity subject.
                // Refuse — never hijack. (Cross-job collisions are prevented at
                // create time too, see `keyed_id_collides_with_slot_pattern`, so
                // this is defence in depth.)
                if existing.mode != InstanceMode::Slot
                    || existing.job_id != job_id
                    || existing.group != group
                    || existing.slot_index != Some(slot)
                {
                    bail!(StorageError::Conflict {
                        message: format!(
                            "slot instance id {instance_id} is already owned by a different \
                             instance (job {}, group {}, mode {:?}, slot {:?}); slot ids are \
                             exclusive to their (job, group, slot) and cannot be adopted",
                            existing.job_id, existing.group, existing.mode, existing.slot_index
                        ),
                    });
                }
                // Re-activation handles both reschedule (Rescheduling -> Active
                // via the caller's binding update) and scale-out of a retained
                // Terminated slot: the same id and storage handle come back.
                // Mint a handle only if none was retained and the task (still)
                // declares storage.
                let storage_handle = match existing.storage_handle {
                    Some(handle) => Some(handle),
                    None => self.mint_instance_volume_tx(
                        tx,
                        &instance_id,
                        &job_id,
                        &task_name,
                        stateful.storage.as_ref(),
                    )?,
                };
                tx.execute(
                    "UPDATE instance SET task_name = ?1, storage_handle = ?2, updated_at = ?3 \
                     WHERE instance_id = ?4;",
                    params![task_name, storage_handle, now, instance_id],
                )?;
            }
            None => {
                let storage_handle = self.mint_instance_volume_tx(
                    tx,
                    &instance_id,
                    &job_id,
                    &task_name,
                    stateful.storage.as_ref(),
                )?;
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
    /// a correct choice. Stopped instances are never bound (they are paused,
    /// not awaiting placement), and terminated ones are tombstones. A storage
    /// handle is minted on first placement when the task declares
    /// `stateful.storage`, and the retained handle is reattached on every
    /// re-bind — that is what makes a replacement allocation *the same
    /// sandbox*.
    fn assign_keyed_instance_tx(
        &self,
        tx: &Transaction<'_>,
        task_id: i64,
        job_id: &str,
        group: &str,
        task_name: &str,
        stateful: &StatefulConfig,
        node_id: &str,
    ) -> R<String> {
        // Unbound live instances of this (job, group, task), oldest first.
        // Ordering by `node_local_pin DESC` first makes an allocation bind the
        // instance whose node-local data already lives on the node it landed
        // on; without a pin the ordering is unchanged (oldest wins), so keyed
        // binding without volumes behaves exactly as before.
        let instance: Option<Instance> = tx
            .query_row(
                "SELECT instance.instance_id, instance.job_id, instance.group_name, \
                 instance.task_name, instance.mode, instance.state, instance.slot_index, \
                 instance.current_alloc_id, instance.storage_handle, instance.created_at, \
                 instance.updated_at FROM instance \
                 LEFT JOIN volume ON volume.instance_id = instance.instance_id \
                 AND volume.node_local = 1 AND volume.state IN ('pending', 'created') \
                 AND instance.storage_handle = volume.plugin || '/' || volume.volume_id \
                 WHERE instance.job_id = ?1 AND instance.group_name = ?2 \
                 AND instance.task_name = ?3 \
                 AND instance.mode = 'keyed' AND instance.state NOT IN ('terminated', 'stopped') \
                 AND instance.instance_id NOT IN (\
                     SELECT instance_id FROM allocation \
                     WHERE task_config_id = ?4 AND instance_id IS NOT NULL\
                 ) \
                 ORDER BY (volume.node_id = ?5) DESC, instance.created_at ASC, \
                 instance.instance_id ASC LIMIT 1;",
                params![job_id, group, task_name, task_id, node_id],
                |row| self.instance_from_row(row),
            )
            .optional()?;

        let instance = instance.with_context(|| {
            format!(
                "no unbound keyed instance for task {task_name} (group {group}, job {job_id}); \
                 keyed allocations are driven by CreateInstance, not count"
            )
        })?;

        if instance.storage_handle.is_none() {
            if let Some(handle) = self.mint_instance_volume_tx(
                tx,
                &instance.instance_id,
                job_id,
                task_name,
                stateful.storage.as_ref(),
            )? {
                let now = Self::now_rfc3339()?;
                tx.execute(
                    "UPDATE instance SET storage_handle = ?1, updated_at = ?2 \
                     WHERE instance_id = ?3;",
                    params![handle, now, instance.instance_id],
                )?;
            }
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
    /// `retain_on_scale_in` is false. Orphaned keyed instances become
    /// `rescheduling`: allocation churn never terminates or stops a keyed
    /// instance, only DestroyInstance/StopInstance (or a job stop) does.
    /// `Stopped` rows are skipped exactly like `Terminated` ones — the
    /// stop/destroy paths set those states in the SAME transaction that
    /// removes the allocations, so this hook can never flip a deliberately
    /// stopped instance back to `rescheduling` (nor can a later, unrelated
    /// allocation death resurrect it). Must run in the same transaction as
    /// the deletion, before commit.
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
            if matches!(
                instance.state,
                InstanceState::Terminated | InstanceState::Stopped
            ) {
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
        let (job_config_id, resources, constraints, permit, lifecycle, network_raw): (
            String,
            Value,
            Value,
            Value,
            String,
            Option<String>,
        ) = tx.query_row(
            "SELECT job_config_id, resources, constraints, permit, lifecycle, network FROM task_config WHERE id = ?;",
            [task_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
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
            network_raw,
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
        let mut staged_events = Vec::new();
        for snapshot in snapshots {
            // Artifact staging transitions are derived by diffing against
            // the previously persisted snapshot, inside this transaction so
            // events commit atomically with the state they describe. The
            // extra read only happens for snapshots carrying artifact rows;
            // plain tasks stay on the single-statement path.
            let previous_states: Option<String> = if snapshot.artifact_states.is_some() {
                tx.query_row(
                    "SELECT artifact_states FROM allocation_health WHERE allocation_id = ?1;",
                    [snapshot.allocation_id],
                    |row| row.get(0),
                )
                .optional()?
                .flatten()
            } else {
                None
            };

            let inserted = tx.execute(
                "INSERT INTO allocation_health (
                    allocation_id,
                    node_id,
                    healthy,
                    status,
                    updated_at,
                    artifact_states
                )
                SELECT ?1, ?2, ?3, ?4, ?5, ?6
                WHERE EXISTS (SELECT 1 FROM allocation WHERE id = ?1 AND node_id = ?2)
                ON CONFLICT(allocation_id) DO UPDATE
                SET node_id = excluded.node_id,
                    healthy = excluded.healthy,
                    status = excluded.status,
                    updated_at = excluded.updated_at,
                    artifact_states = excluded.artifact_states;",
                params![
                    snapshot.allocation_id,
                    node_id,
                    i64::from(snapshot.healthy),
                    snapshot.status.as_str(),
                    now.as_str(),
                    snapshot.artifact_states.as_deref()
                ],
            )?;

            let Some(new_states) = snapshot.artifact_states.as_deref() else {
                continue;
            };
            // The guarded insert writes nothing for unknown allocations;
            // skip events for those too.
            if inserted == 0 {
                continue;
            }
            let (transitions, rows) = crate::storage::events::artifact_transitions(
                previous_states.as_deref(),
                new_states,
            );
            if !transitions.staging_started && !transitions.all_staged {
                continue;
            }
            let job_id = self
                .job_id_for_allocation_tx(&tx, snapshot.allocation_id)?
                .unwrap_or_default();
            if transitions.staging_started {
                staged_events.push(self.stage_event_locked(
                    &tx,
                    drafts::artifact_staging(snapshot.allocation_id, node_id, &job_id, &rows),
                ));
            }
            if transitions.all_staged {
                staged_events.push(self.stage_event_locked(
                    &tx,
                    drafts::artifact_staged(snapshot.allocation_id, node_id, &job_id, &rows),
                ));
            }
        }
        tx.commit()?;
        for event in staged_events {
            self.publish_staged(event);
        }
        Ok(())
    }

    /// Job ID owning an allocation, resolved inside an open transaction (the
    /// allocation table stores job_config_id; the job comes via join). Only
    /// consulted when an artifact staging transition actually fires.
    fn job_id_for_allocation_tx(&self, tx: &Connection, allocation_id: i64) -> R<Option<String>> {
        let job_id = tx
            .query_row(
                "SELECT job_config.job_id FROM allocation
                 INNER JOIN job_config ON job_config.id = allocation.job_config_id
                 WHERE allocation.id = ?1;",
                [allocation_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(job_id)
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

        let placeholders = std::iter::repeat_n("?", allocation_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT allocation_id, node_id, healthy, status, updated_at, artifact_states
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
    /// One resolvable endpoint of a declared service, derived (never
    /// registered) from task config + allocation + health + node state.
    pub fn service_endpoints(
        &self,
        service_filter: Option<&str>,
    ) -> R<Vec<crate::structs::ServiceEndpointRow>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT tc.name, tc.service, tc.job_id,
                    a.id, a.node_id, a.network,
                    ah.healthy, ah.status,
                    n.status, n.config_attrs
             FROM task_config tc
             INNER JOIN allocation a ON a.task_config_id = tc.id
             LEFT JOIN allocation_health ah ON ah.allocation_id = a.id
             LEFT JOIN node n ON n.id = a.node_id
             WHERE tc.service IS NOT NULL;",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<bool>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<Value>>(9)?,
            ))
        })?;

        let mut endpoints = Vec::new();
        for row in rows {
            let (
                task_name,
                service_raw,
                job_id,
                allocation_id,
                node_id,
                network_raw,
                alloc_healthy,
                health_status,
                node_status,
                node_config_attrs,
            ) = row?;

            let service: crate::structs::ServiceConfig = from_str(&service_raw)
                .with_context(|| format!("invalid stored service config for task {task_name}"))?;
            let service_name = service.service_name(&task_name).to_string();
            if let Some(filter) = service_filter {
                if service_name != filter {
                    continue;
                }
            }

            let network: Option<crate::structs::AllocNetwork> = network_raw
                .as_deref()
                .map(from_str)
                .transpose()
                .with_context(|| {
                    format!("invalid stored network for allocation {allocation_id}")
                })?;
            let port = service
                .port_label()
                .and_then(|label| network.as_ref().and_then(|network| network.port(label)))
                .map(|port| port.host_port);
            let address = network
                .as_ref()
                .and_then(|network| network.address.clone())
                .or_else(|| node_config_attrs.as_ref().and_then(worker_address_host));

            // Nodes report `active`/`ready` while alive; `lost` (or a missing
            // row) is the dead state. Endpoint liveness is leased on this.
            let node_alive = matches!(node_status.as_deref(), Some(status) if status != "lost");
            let healthy = alloc_healthy.unwrap_or(false) && node_alive;
            let health_status = if node_alive {
                health_status.unwrap_or_default()
            } else {
                format!(
                    "node {} is {}",
                    node_id,
                    node_status.as_deref().unwrap_or("unknown")
                )
            };

            endpoints.push(crate::structs::ServiceEndpointRow {
                service: service_name,
                job_id,
                task_name,
                allocation_id,
                node_id,
                address,
                port,
                healthy,
                health_status,
                tags: service.tags.clone(),
                meta: service.meta_strings(),
                port_label: service.port_label().map(str::to_string),
            });
        }

        Ok(endpoints)
    }

    /// Services declared by the latest job config of each job, including ones
    /// with no allocations yet. Returns (service, job_id, task_name, tags).
    pub fn declared_services(&self) -> R<Vec<DeclaredService>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT tc.name, tc.service, tc.job_id
             FROM task_config tc
             INNER JOIN job_config jc ON jc.id = tc.job_config_id
             INNER JOIN (
                 SELECT job_id, MAX(version) AS version FROM job_config GROUP BY job_id
             ) latest ON latest.job_id = jc.job_id AND latest.version = jc.version
             WHERE tc.service IS NOT NULL;",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;

        let mut declared = Vec::new();
        for row in rows {
            let (task_name, service_raw, job_id) = row?;
            let service: crate::structs::ServiceConfig = from_str(&service_raw)
                .with_context(|| format!("invalid stored service config for task {task_name}"))?;
            declared.push(DeclaredService {
                service: service.service_name(&task_name).to_string(),
                job_id,
                tags: service.tags.clone(),
                meta: service.meta_strings(),
                task_name,
            });
        }
        Ok(declared)
    }

    pub fn allocation_health_for_job_config(
        &self,
        job_config_id: &str,
    ) -> R<HashMap<i64, AllocationHealthSnapshot>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT ah.allocation_id, ah.node_id, ah.healthy, ah.status, ah.updated_at, \
                    ah.artifact_states
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
        drop(conn);

        self.cache_remove_allocations(&allocations)?;

        self.events()
            .allocations_removed_for_job(job_id, allocations.len());

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
        drop(conn);

        self.cache_remove_allocations(&allocations)?;

        self.events()
            .allocations_removed_for_job_config(job_config_id, &allocations);

        Ok(allocations)
    }

    /// Bring a job config back to its desired allocation counts.
    ///
    /// Runs the real scheduler in `Reschedule` mode rather than a second,
    /// simplified placement pass: replacements after a node loss or a node
    /// join have to honor node capacity, distinct constraints, group
    /// co-location and stateful instance counts exactly like every other
    /// placement path, and be scored by the configured policy.
    #[instrument(err, skip_all)]
    pub fn reconcile_job_config_allocations(&self, job_config_id: &str) -> R<()> {
        if self
            .get_task_configs_for_job_config(job_config_id)?
            .is_empty()
        {
            return Ok(());
        }

        let scheduler = crate::manager::scheduler::Scheduler::new(self)?;
        let outcome = scheduler.plan_and_apply(
            job_config_id,
            crate::manager::scheduler::PlanMode::Reschedule,
        )?;
        if !outcome.allocations.is_empty() {
            debug!(
                %job_config_id,
                placed = outcome.allocations.len(),
                "Reconciled job config allocations"
            );
        }

        Ok(())
    }

    /// Free a lost node's allocations for replacement — gated for stateful
    /// tasks (the at-most-one fencing guarantee, see
    /// docs/stateful-workloads.md "Node loss and fencing"):
    ///
    /// - Stateless allocations are removed and replaced immediately (today's
    ///   eager behavior).
    /// - A stateful allocation is only removed once the node has been lost
    ///   longer than its task's `onLost.replaceAfter` — i.e. provably past
    ///   the worker's `selfTerminateAfter` self-fence window — so the
    ///   replacement can never run concurrently with the original. With
    ///   `replaceAfter: "never"` it is never removed here; only node
    ///   recovery or an operator override (ReplaceInstance) frees it.
    ///
    /// Gated allocations keep their rows: the instance stays `active` and
    /// bound (`current_alloc_id` intact), the scheduler keeps counting the
    /// allocation as placed (so no plan path double-places the instance),
    /// and instance get/list surfaces a `pending-loss-confirmation` detail.
    /// The node-status monitor re-runs this method every tick (see
    /// [`Self::recheck_lost_nodes`]), so replacement proceeds automatically
    /// once the threshold elapses. Idempotent.
    #[instrument(err, skip_all)]
    pub fn reschedule_allocations_for_lost_node(&self, node_id: &str) -> R<()> {
        let allocs = self.allocs_for_node(node_id)?;
        if allocs.is_empty() {
            return Ok(());
        }

        let lost_for = self.node_lost_for(node_id)?;
        let mut removable = Vec::new();
        for alloc in allocs {
            if self.lost_allocation_replaceable(&alloc, lost_for)? {
                removable.push(alloc);
            } else {
                info!(
                    allocation_id = alloc.id,
                    %node_id,
                    instance_id = alloc.instance_id.as_deref().unwrap_or_default(),
                    lost_for_secs = lost_for.map(TimeDuration::whole_seconds),
                    "Holding stateful allocation on lost node pending loss confirmation"
                );
            }
        }
        if removable.is_empty() {
            return Ok(());
        }

        let affected_job_configs = self.remove_allocations(&removable)?;
        for job_config_id in affected_job_configs {
            self.reconcile_job_config_allocations(&job_config_id)?;
        }

        Ok(())
    }

    /// How long the node has been lost, when known. `None` means the node is
    /// not (or no longer) marked lost — callers treat that as "not
    /// confirmable yet", never as "free to replace".
    fn node_lost_for(&self, node_id: &str) -> R<Option<TimeDuration>> {
        let Some(lost_at) = self.node_lost_at(node_id)? else {
            return Ok(None);
        };
        let lost_at = OffsetDateTime::parse(&lost_at, &Rfc3339)
            .with_context(|| format!("invalid lost_at timestamp {lost_at:?} for node {node_id}"))?;
        Ok(Some(OffsetDateTime::now_utc() - lost_at))
    }

    /// Whether a lost node's allocation may be freed for replacement now.
    /// Stateless tasks: always (eager). Stateful tasks: only once the loss
    /// is confirmed per the task's `onLost` policy.
    fn lost_allocation_replaceable(
        &self,
        alloc: &Allocation,
        lost_for: Option<TimeDuration>,
    ) -> R<bool> {
        let Some(stateful) = self.task_stateful_config(alloc.task_config_id)? else {
            return Ok(true);
        };
        let policy = Self::on_lost_policy_or_safe_default(&alloc.job_id, &stateful);
        match policy.replace_after {
            ReplaceAfter::Never => Ok(false),
            ReplaceAfter::After(threshold) => {
                // Unknown loss duration = unconfirmable = hold (safe side).
                let Some(lost_for) = lost_for else {
                    return Ok(false);
                };
                let threshold = TimeDuration::try_from(threshold)
                    .context("onLost.replaceAfter overflows the comparable duration range")?;
                Ok(lost_for > threshold)
            }
        }
    }

    /// Resolve a stored stateful block's `onLost` policy. Validation rejects
    /// unresolvable policies at submit time, so a failure here means a row
    /// predating validation or hand-edited state — fall back to the safe
    /// defaults rather than wedging the whole lost-node sweep.
    fn on_lost_policy_or_safe_default(
        job_id: &str,
        stateful: &StatefulConfig,
    ) -> ResolvedOnLostPolicy {
        stateful.resolved_on_lost().unwrap_or_else(|err| {
            warn!(
                %job_id,
                error = ?err,
                "Stored stateful onLost policy failed to resolve; using defaults"
            );
            StatefulOnLostConfig::default()
                .resolve()
                .expect("default onLost policy resolves")
        })
    }

    /// The task's stored stateful block, if any (`None` = stateless task).
    fn task_stateful_config(&self, task_config_id: i64) -> R<Option<StatefulConfig>> {
        let conn = self.read_connection()?;
        let raw: Option<String> = conn
            .query_row(
                "SELECT stateful FROM task_config WHERE id = ?;",
                [task_config_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        raw.map(|raw| {
            serde_json::from_str(&raw)
                .with_context(|| format!("invalid stateful config for task {task_config_id}"))
        })
        .transpose()
    }

    /// Remove specific allocation rows (releasing instance bindings and
    /// bumping epochs, like every other allocation-removal path) and return
    /// the affected job config ids.
    pub(crate) fn remove_allocations(&self, allocs: &[Allocation]) -> R<HashSet<String>> {
        if allocs.is_empty() {
            return Ok(HashSet::new());
        }

        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        for alloc in allocs {
            tx.execute("DELETE FROM allocation WHERE id = ?;", [alloc.id])?;
        }
        self.release_instance_bindings_tx(&tx, allocs)?;
        self.bump_epochs_for_allocations_tx(&tx, allocs)?;
        tx.commit()?;

        self.cache_remove_allocations(allocs)?;

        Ok(allocs
            .iter()
            .map(|alloc| alloc.job_config_id.clone())
            .collect())
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

    /// Settle every instance of a stopped job. Rows are always retained, but
    /// the two modes land in different states:
    ///
    /// - slot rows become `terminated`, so a later `job run` re-materializes
    ///   the same ordinal identities. Their storage handles are retained by
    ///   default (the re-run reattaches the same data — exactly today's
    ///   scale-out semantics), but a task with
    ///   `stateful.storage.retainOnStop: false` (the K8s
    ///   `whenDeleted: Delete` analog) has its slot handles DROPPED here, so
    ///   the revived slots mint fresh handles;
    /// - keyed rows become `stopped` (with handles retained, always — keyed
    ///   stop semantics are fixed per instance), so resubmitting the job and
    ///   running `legion instance start <id>` revives the same sandbox with
    ///   its persisted state. A job stop is a pause, not a destroy: only
    ///   DestroyInstance / job destroy is forever. Stopped keyed instances
    ///   stay stopped when the job is resubmitted — they are not desired by
    ///   the scheduler until explicitly started.
    ///
    /// `job destroy` never reaches this path — it unconditionally deletes
    /// every instance row (and handle) via the job FK cascade, regardless of
    /// `retainOnStop`.
    ///
    /// Called from the job_stop eval after its allocations are removed.
    /// Idempotent: already-stopped/terminated rows are untouched.
    #[instrument(err, skip_all)]
    pub fn stop_instances_for_job(&self, job_id: &str) -> R<u64> {
        let now = Self::now_rfc3339()?;
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;

        // Resolve the per-task job-stop retention policy from the LATEST job
        // config version (mirroring how `slot_still_desired_tx` resolves the
        // scale-in policy) and drop the handles of the slot rows this stop is
        // about to terminate when `retainOnStop: false`. Already-settled
        // (terminated/stopped) rows are untouched, keeping the stop
        // idempotent; keyed rows are never targeted (their stop semantics are
        // fixed, and validation rejects the knob in keyed mode anyway).
        let mut stmt = tx.prepare(
            "SELECT DISTINCT group_name, task_name FROM instance \
             WHERE job_id = ?1 AND mode = 'slot' AND storage_handle IS NOT NULL \
             AND state NOT IN ('terminated', 'stopped');",
        )?;
        let slot_tasks: Vec<(String, String)> =
            Self::collect_rows(stmt.query_map([job_id], |row| Ok((row.get(0)?, row.get(1)?)))?)?;
        drop(stmt);
        for (group, task_name) in slot_tasks {
            let stateful: Option<Option<String>> = tx
                .query_row(
                    "SELECT task_config.stateful FROM task_config \
                     INNER JOIN job_config ON job_config.id = task_config.job_config_id \
                     WHERE task_config.job_id = ?1 AND task_config.group_name = ?2 \
                     AND task_config.name = ?3 \
                     ORDER BY job_config.version DESC LIMIT 1;",
                    params![job_id, group, task_name],
                    |row| row.get(0),
                )
                .optional()?;
            let retain = stateful
                .flatten()
                .as_deref()
                .and_then(|raw| serde_json::from_str::<StatefulConfig>(raw).ok())
                .and_then(|stateful| stateful.storage)
                .map(|storage| storage.retains_on_stop())
                .unwrap_or(true);
            if !retain {
                info!(
                    job_id,
                    group,
                    task = %task_name,
                    "Dropping slot storage handles on job stop (retainOnStop: false)"
                );
                tx.execute(
                    "UPDATE instance SET storage_handle = NULL, updated_at = ?1 \
                     WHERE job_id = ?2 AND group_name = ?3 AND task_name = ?4 \
                     AND mode = 'slot' AND state NOT IN ('terminated', 'stopped');",
                    params![now, job_id, group, task_name],
                )?;
            }
        }

        let updated = tx.execute(
            "UPDATE instance SET \
             state = CASE WHEN mode = 'keyed' THEN 'stopped' ELSE 'terminated' END, \
             current_alloc_id = NULL, updated_at = ?1 \
             WHERE job_id = ?2 AND state NOT IN ('terminated', 'stopped');",
            params![now, job_id],
        )?;
        tx.commit()?;
        Ok(updated as u64)
    }

    /// Whether ANY instance row (live, stopped, or tombstone) owns this id.
    fn instance_id_exists_tx(tx: &Transaction<'_>, id: &str) -> R<bool> {
        let found: Option<i64> = tx
            .query_row(
                "SELECT 1 FROM instance WHERE instance_id = ?;",
                [id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(found.is_some())
    }

    /// Whether a caller-supplied keyed id structurally aliases a slot id of a
    /// slot-mode stateful task in the SAME job. Slot ids are the deterministic
    /// `{job_id}-{group}-{slot}` pattern and the slot id space is exclusive, so
    /// a keyed id that lands on `job-group-N` for some slot task with
    /// `count.num > N` would, once that slot materializes, fight over one
    /// instance row. We forbid it at create time so the two id spaces stay
    /// disjoint within a job (cross-job aliasing is additionally caught when
    /// the slot binds, see `assign_stateful_instance_tx`). Checks every job
    /// config version's slot tasks so a not-yet-current (or future) slot can't
    /// be aliased either.
    fn keyed_id_collides_with_slot_pattern_tx(
        tx: &Transaction<'_>,
        job_id: &str,
        id: &str,
    ) -> R<bool> {
        let mut stmt = tx.prepare(
            "SELECT DISTINCT group_name, count FROM task_config \
             WHERE job_id = ?1 AND stateful IS NOT NULL \
             AND json_extract(stateful, '$.mode') = 'slot';",
        )?;
        let slot_tasks: Vec<(String, i64)> =
            Self::collect_rows(stmt.query_map([job_id], |row| Ok((row.get(0)?, row.get(1)?)))?)?;
        for (group, count) in slot_tasks {
            for slot in 0..count.max(0) as u32 {
                if slot_instance_id(job_id, &group, slot) == id {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Insert one pending keyed instance row inside `tx`, enforcing the
    /// never-recycle collision rule. `requested_id: None` mints
    /// `{task}-{16 alphanumerics}`. Shared by the single and batch create
    /// paths so both apply identical rules.
    fn insert_keyed_instance_tx(
        &self,
        tx: &Transaction<'_>,
        job_id: &str,
        group: &str,
        task_name: &str,
        requested_id: Option<&str>,
    ) -> R<String> {
        let now = Self::now_rfc3339()?;
        let instance_id = match requested_id {
            Some(id) => {
                if Self::instance_id_exists_tx(tx, id)? {
                    bail!(StorageError::Conflict {
                        message: format!(
                            "instance id {id} already exists (keyed ids are never recycled, \
                             including stopped and destroyed ones); choose a different id"
                        ),
                    });
                }
                if Self::keyed_id_collides_with_slot_pattern_tx(tx, job_id, id)? {
                    bail!(StorageError::Conflict {
                        message: format!(
                            "instance id {id} collides with the reserved slot-id pattern \
                             {{job}}-{{group}}-{{slot}} of a slot-mode stateful task in job \
                             {job_id}; slot and keyed id spaces are disjoint within a job"
                        ),
                    });
                }
                id.to_string()
            }
            None => loop {
                // Collisions on 16 random alphanumerics are practically
                // impossible; the loop is belt-and-braces.
                let candidate = format!("{task_name}-{}", random_string(16));
                if !Self::instance_id_exists_tx(tx, &candidate)? {
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
        Ok(instance_id)
    }

    /// Register a new keyed-mode instance (the CreateInstance RPC's storage
    /// half; task resolution and id policy live in the server handler).
    ///
    /// When `requested_id` is `Some` it must not collide with ANY existing
    /// instance row — including Stopped rows and Terminated tombstones —
    /// enforcing the never-recycle guarantee; collisions surface as
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
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let instance_id =
            self.insert_keyed_instance_tx(&tx, job_id, group, task_name, requested_id)?;
        let instance = tx.query_row(&GET_INSTANCE, [instance_id.as_str()], |row| {
            self.instance_from_row(row)
        })?;
        tx.commit()?;
        Ok(instance)
    }

    /// Register a batch of keyed-mode instances ALL-OR-NOTHING (the
    /// CreateInstances RPC's storage half): every id in `requested_ids` plus
    /// `mint_count` server-minted ids, in one transaction. Any collision —
    /// with an existing row (live/stopped/tombstone) or a duplicate within
    /// `requested_ids` itself — aborts the whole batch with
    /// [`StorageError::Conflict`] naming the offending id, creating nothing.
    /// Returns the created ids in request order (explicit first, then
    /// minted).
    #[instrument(err, skip_all)]
    pub fn create_keyed_instances(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        requested_ids: &[String],
        mint_count: u32,
    ) -> R<Vec<String>> {
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let mut created = Vec::with_capacity(requested_ids.len() + mint_count as usize);
        for id in requested_ids {
            created.push(self.insert_keyed_instance_tx(&tx, job_id, group, task_name, Some(id))?);
        }
        for _ in 0..mint_count {
            created.push(self.insert_keyed_instance_tx(&tx, job_id, group, task_name, None)?);
        }
        tx.commit()?;
        Ok(created)
    }

    /// Ensure each seed id from `stateful.initialInstances` exists (the
    /// RunJob seeding half; see the field docs on
    /// [`crate::structs::StatefulConfig::initial_instances`]).
    ///
    /// Ids that have never existed are created as `pending` keyed rows; ids
    /// that already exist in ANY state — live, stopped, or a terminated
    /// tombstone, even under another job — are skipped silently
    /// (`INSERT OR IGNORE`), so resubmits are idempotent and a deliberately
    /// destroyed id is never resurrected. Returns the ids actually created
    /// by this call.
    #[instrument(err, skip_all)]
    pub fn seed_keyed_instances(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        ids: &[String],
    ) -> R<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let now = Self::now_rfc3339()?;
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let mut created = Vec::new();
        for id in ids {
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO instance (instance_id, job_id, group_name, task_name, \
                 mode, state, slot_index, current_alloc_id, storage_handle, created_at, \
                 updated_at) \
                 VALUES (?1, ?2, ?3, ?4, 'keyed', 'pending', NULL, NULL, NULL, ?5, ?5);",
                params![id, job_id, group, task_name, now],
            )?;
            if inserted > 0 {
                created.push(id.clone());
            }
        }
        tx.commit()?;
        if !created.is_empty() {
            info!(
                job_id,
                group,
                task_name,
                seeded = created.len(),
                "Seeded keyed instances from initialInstances"
            );
        }
        Ok(created)
    }

    /// Destroy a keyed-mode instance (the DestroyInstance RPC's storage half):
    /// delete its allocations through the normal allocation-removal path,
    /// transition the row to `terminated`, and drop its storage handle.
    ///
    /// The row is retained forever as a tombstone so the id can never be
    /// created again. Destroying a `stopped` instance works the same way
    /// (stopped -> terminated; the retained handle is dropped) — stop is the
    /// reversible pause, destroy is forever. Destroying an
    /// already-terminated instance is a no-op (returns
    /// [`DestroyInstanceOutcome::AlreadyTerminated`]). Slot-mode instances
    /// are rejected: their lifecycle is owned by the scheduler via
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
             allocation.instance_id, allocation.network \
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

    /// Stop (pause) a keyed-mode instance (the StopInstance RPC's storage
    /// half): delete its allocations through the normal allocation-removal
    /// path and transition the row to `stopped`, RETAINING the row and its
    /// storage handle. The id stays owned (creates still collide) and
    /// [`Self::start_keyed_instance`] reverses the whole thing — unlike
    /// destroy, stop is not forever.
    ///
    /// The state flips to `stopped` in the SAME transaction that removes the
    /// allocations (mirroring [`Self::destroy_keyed_instance`]), so the
    /// release hook — which skips `stopped` rows — can never race this and
    /// flip the instance to `rescheduling`.
    ///
    /// Stopping an already-stopped instance is a no-op (returns
    /// [`StopInstanceOutcome::AlreadyStopped`]). Slot-mode instances and
    /// terminated tombstones are rejected with
    /// [`StorageError::InvalidInput`]; the RPC layer pre-checks the
    /// terminated case to surface FailedPrecondition.
    #[instrument(err, skip_all)]
    pub fn stop_keyed_instance(&self, instance_id: &str) -> R<StopInstanceOutcome> {
        let instance = self
            .get_instance(instance_id)?
            .ok_or_else(|| StorageError::not_found("instance", instance_id))?;
        if instance.mode != InstanceMode::Keyed {
            bail!(StorageError::InvalidInput {
                message: format!(
                    "instance {instance_id} is slot-mode; slot instances are managed by the \
                     scheduler via count.num, not StopInstance"
                ),
            });
        }
        match instance.state {
            InstanceState::Stopped => return Ok(StopInstanceOutcome::AlreadyStopped),
            InstanceState::Terminated => bail!(StorageError::InvalidInput {
                message: format!(
                    "instance {instance_id} is terminated; destroyed instances are tombstoned \
                     forever and cannot be stopped"
                ),
            }),
            InstanceState::Pending | InstanceState::Active | InstanceState::Rescheduling => {}
        }

        let now = Self::now_rfc3339()?;
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;

        // Stop the instance's allocation(s) through the normal removal path
        // (delete row + epoch bump + cache eviction); the worker stops the
        // task on its next heartbeat reconcile. The instance is marked
        // stopped in the same transaction, so the release hook (which skips
        // stopped instances) can never flip it to rescheduling.
        let mut stmt = tx.prepare(
            "SELECT allocation.id, allocation.status, allocation.resources, allocation.node_id, \
             allocation.job_config_id, allocation.task_config_id, job_config.job_id, \
             allocation.instance_id, allocation.network \
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
            "UPDATE instance SET state = 'stopped', current_alloc_id = NULL, updated_at = ?1 \
             WHERE instance_id = ?2;",
            params![now, instance_id],
        )?;
        self.bump_epochs_for_allocations_tx(&tx, &allocations)?;
        tx.commit()?;

        self.cache_remove_allocations(&allocations)?;

        info!(
            instance_id,
            job_id = %instance.job_id,
            removed_allocations = allocations.len(),
            "Stopped keyed instance"
        );
        Ok(StopInstanceOutcome::Stopped {
            removed_allocations: allocations.len(),
        })
    }

    /// Start (resume) a stopped keyed-mode instance (the StartInstance RPC's
    /// storage half): transition `stopped -> pending` so the next placement
    /// eval binds a fresh allocation to the same instance id and its
    /// RETAINED storage handle. Job-status gating (a stopped job schedules
    /// nothing) lives in the RPC handler.
    ///
    /// Starting an already-live (pending/active/rescheduling) instance is a
    /// no-op (returns [`StartInstanceOutcome::AlreadyRunning`]). Slot-mode
    /// instances and terminated tombstones are rejected with
    /// [`StorageError::InvalidInput`]; the RPC layer pre-checks the
    /// terminated case to surface FailedPrecondition.
    #[instrument(err, skip_all)]
    pub fn start_keyed_instance(&self, instance_id: &str) -> R<StartInstanceOutcome> {
        let instance = self
            .get_instance(instance_id)?
            .ok_or_else(|| StorageError::not_found("instance", instance_id))?;
        if instance.mode != InstanceMode::Keyed {
            bail!(StorageError::InvalidInput {
                message: format!(
                    "instance {instance_id} is slot-mode; slot instances are managed by the \
                     scheduler via count.num, not StartInstance"
                ),
            });
        }
        match instance.state {
            InstanceState::Pending | InstanceState::Active | InstanceState::Rescheduling => {
                return Ok(StartInstanceOutcome::AlreadyRunning);
            }
            InstanceState::Terminated => bail!(StorageError::InvalidInput {
                message: format!(
                    "instance {instance_id} is terminated; destroyed ids are never recycled — \
                     create a new instance instead"
                ),
            }),
            InstanceState::Stopped => {}
        }

        let now = Self::now_rfc3339()?;
        let conn = self.write_connection()?;
        // Guard on the current state so a concurrent destroy/stop cannot be
        // overwritten by this transition.
        let updated = conn.execute(
            "UPDATE instance SET state = 'pending', updated_at = ?1 \
             WHERE instance_id = ?2 AND state = 'stopped';",
            params![now, instance_id],
        )?;
        if updated == 0 {
            bail!(StorageError::Conflict {
                message: format!(
                    "instance {instance_id} changed state concurrently; retry StartInstance"
                ),
            });
        }
        info!(
            instance_id,
            job_id = %instance.job_id,
            "Started keyed instance (stopped -> pending)"
        );
        Ok(StartInstanceOutcome::Started)
    }

    /// Count the live (pending/active/rescheduling) keyed instances of a
    /// (job, group, task). This is the desired allocation count for a
    /// keyed-mode task — `count.num` is ignored for keyed tasks. `stopped`
    /// instances are NOT live: they are paused and get no allocation until
    /// StartInstance; `terminated` rows are tombstones.
    #[instrument(err, skip_all)]
    pub fn count_live_keyed_instances(&self, job_id: &str, group: &str, task_name: &str) -> R<u32> {
        let conn = self.read_connection()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM instance \
             WHERE job_id = ?1 AND group_name = ?2 AND task_name = ?3 \
             AND mode = 'keyed' AND state NOT IN ('terminated', 'stopped');",
            params![job_id, group, task_name],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as u32)
    }

    /// Map of `instance_id -> slot ordinal` for the slot pool of a
    /// (job, group). Used by sequential (`rollout.slots.order`) rollouts to
    /// retire old allocations in descending slot order — an allocation's slot
    /// identity is reachable only through its instance row
    /// (`allocation.instance_id -> instance.slot_index`).
    #[instrument(err, skip_all)]
    pub fn slot_indices_for_job_group(&self, job_id: &str, group: &str) -> R<HashMap<String, u32>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT instance_id, slot_index FROM instance \
             WHERE job_id = ?1 AND group_name = ?2 AND mode = 'slot' \
             AND slot_index IS NOT NULL;",
        )?;
        let rows = stmt.query_map(params![job_id, group], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        let mut indices = HashMap::new();
        for row in rows {
            let (instance_id, slot) = row?;
            indices.insert(instance_id, slot.max(0) as u32);
        }
        Ok(indices)
    }

    /// Count live allocations bound to (job, group)'s slot instances that do
    /// NOT belong to `exclude_job_config_id`.
    ///
    /// This is how a plain (non-rollout) reschedule learns that some slots
    /// are deliberately still occupied by an older config — the staged
    /// (`rollout.slots.updateFrom`) state — and must not be double-placed:
    /// the latest config's effective desired count is `count.num` minus this
    /// number, so a reschedule only replaces slots that are actually empty.
    #[instrument(err, skip_all)]
    pub fn count_slot_allocations_excluding_config(
        &self,
        job_id: &str,
        group: &str,
        exclude_job_config_id: &str,
    ) -> R<u32> {
        let conn = self.read_connection()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM allocation \
             INNER JOIN instance ON instance.instance_id = allocation.instance_id \
             WHERE instance.job_id = ?1 AND instance.group_name = ?2 \
             AND instance.mode = 'slot' AND allocation.job_config_id != ?3;",
            params![job_id, group, exclude_job_config_id],
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
            ":ready_at": Self::sortable_now()?,
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
        let now = Self::sortable_now()?;
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
                named_params! {":now": Self::sortable_now()?},
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
        // Collapse only work this claim genuinely subsumes: same entity, same
        // kind, and already claimable.
        //
        // Matching on kind matters because different kinds are not
        // interchangeable — dropping a job's `rollout_tick` because a
        // `job_reschedule` for it was claimed stalls the rollout forever
        // (ticks are self-perpetuating, nothing re-drives them), and dropping
        // a `job_stop` the same way loses an operator's request outright.
        //
        // Matching on `ready_at` matters because a delayed eval is a timer,
        // not a queued duplicate: a batch task's next scheduled `job_update`
        // must survive an unrelated `job_update` running now.
        tx.execute(
            "DELETE FROM eval
            WHERE status = 'pending'
              AND id != :id
              AND type = :type
              AND ready_at <= :now
              AND (
                (:job_id IS NOT NULL AND :job_id != '' AND job_id = :job_id)
                OR (:node_id IS NOT NULL AND :node_id != '' AND node_id = :node_id)
              );",
            named_params! {
                ":id": eval_id,
                ":type": eval.kind,
                ":now": Self::sortable_now()?,
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
        self.complete_eval_impl(eval_id, succeeded, None)
    }

    /// Complete an eval and record its lifecycle event (`eval.completed` /
    /// `eval.failed`, built via `drafts`) in the same transaction, so the
    /// event costs no extra lock acquisition on the eval critical path.
    #[instrument(err, skip_all)]
    pub fn complete_eval_with_event(
        &self,
        eval_id: &str,
        succeeded: bool,
        event: EventDraft,
    ) -> R<()> {
        self.complete_eval_impl(eval_id, succeeded, Some(event))
    }

    fn complete_eval_impl(
        &self,
        eval_id: &str,
        succeeded: bool,
        event: Option<EventDraft>,
    ) -> R<()> {
        let tx_started = Instant::now();
        let mut conn: MutexGuard<'_, Connection> = self.write_connection()?;
        let tx = conn.transaction()?;

        if succeeded {
            tx.execute("DELETE FROM eval WHERE id = ?;", [eval_id])?;
        } else {
            tx.execute("UPDATE eval SET status = 'failed' WHERE id = ?;", [eval_id])?;
        }
        let staged = event.and_then(|draft| self.stage_event_locked(&tx, draft));
        tx.commit()?;
        self.publish_staged(staged);
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
            Self::sortable_timestamp(OffsetDateTime::now_utc() + time::Duration::try_from(delay)?)?;

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

        let enqueue_event = self.stage_event_locked(&tx, drafts::eval_enqueued(&eval, delay));
        tx.commit()?;
        self.publish_staged(enqueue_event);
        drop(conn);

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
        sql.split_whitespace()
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

    // =====
    // VOLUMES
    // =====
    //
    // The manager owns the volume state machine; workers own the drivers. See
    // docs/volumes.md and internal_docs/plans/volume-plugin-system-plan.md.

    fn volume_from_row(&self, row: &Row<'_>) -> Result<Volume, rusqlite::Error> {
        let state: String = row.get(6)?;
        Ok(Volume {
            volume_id: row.get(0)?,
            name: row.get(1)?,
            plugin: row.get(2)?,
            job_id: row.get(3)?,
            instance_id: row.get(4)?,
            task_name: row.get(5)?,
            state: state.parse().map_err(|err: String| {
                rusqlite::Error::FromSqlConversionFailure(
                    6,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
                )
            })?,
            node_id: row.get(7)?,
            node_local: row.get::<_, i64>(8)? != 0,
            read_only: row.get::<_, i64>(9)? != 0,
            params: parse_json_map_column(10, row.get(10)?)?,
            attrs: parse_json_map_column(11, row.get(11)?)?,
            mount_path: row.get(12)?,
            last_error: row.get(13)?,
            created_at: row.get(14)?,
            updated_at: row.get(15)?,
            volume_mode: row.get::<_, String>(16)?.parse().map_err(|err: String| {
                rusqlite::Error::FromSqlConversionFailure(
                    16,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
                )
            })?,
            fs: row.get(17)?,
        })
    }

    /// Mint an instance's storage handle and the volume row behind it.
    ///
    /// Both happen in the SAME transaction as the placement, so a handle can
    /// never exist without a row — which is what lets the reaper treat "no
    /// instance references this volume" as an unambiguous reclaim signal.
    /// Returns `None` for tasks that declare no `stateful.storage`.
    fn mint_instance_volume_tx(
        &self,
        tx: &Transaction<'_>,
        instance_id: &str,
        job_id: &str,
        task_name: &str,
        storage: Option<&crate::structs::StatefulStorageConfig>,
    ) -> R<Option<String>> {
        let Some(storage) = storage else {
            return Ok(None);
        };
        let plugin = storage.plugin_name().to_string();
        // Mint the handle first so the format lives in exactly one place, then
        // split the id back out for the row.
        let handle = mint_storage_handle(&plugin);
        let volume_id = handle
            .split_once('/')
            .map(|(_, id)| id.to_string())
            .context("minted storage handle is malformed")?;
        let now = Self::now_rfc3339()?;
        let params = serde_json::to_string(&storage.param_map())?;
        // The manager learns node-locality from worker fingerprints. Default
        // to `true` when nothing advertises the plugin yet: wrongly pinning
        // costs a placement delay, while wrongly NOT pinning silently starts
        // the workload on an empty volume. Corrected on first mount, when the
        // worker reports what the driver actually said.
        let node_local = self.plugin_is_node_local_tx(tx, &plugin)?;
        tx.execute(
            "INSERT INTO volume (volume_id, name, plugin, job_id, instance_id, task_name, \
             state, node_id, node_local, read_only, params, attrs, mount_path, last_error, \
             created_at, updated_at, volume_mode, fs) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', NULL, ?7, 0, ?8, '{}', NULL, NULL, ?9, \
             ?9, ?10, ?11);",
            params![
                volume_id,
                crate::structs::INSTANCE_VOLUME_NAME,
                plugin,
                job_id,
                instance_id,
                task_name,
                node_local as i64,
                params,
                now,
                storage.volume_mode().as_str(),
                (storage.volume_mode() == crate::volumes::VolumeMode::Block).then(|| storage
                    .param_map()
                    .get("fs")
                    .and_then(Value::as_str)
                    .unwrap_or(crate::volumes::DEFAULT_BLOCK_FS)
                    .to_string()),
            ],
        )?;
        info!(volume_id, plugin, instance_id, "Minted instance volume");
        Ok(Some(handle))
    }

    /// Upsert the volume rows behind a task's `volumes[]` claims.
    ///
    /// Claims are **shared**: their id is derived from
    /// `(job, group, task, name)`, so every allocation of the task mounts the
    /// same volume. That is what "claim of storage that already exists" means —
    /// a read-only `/mnt/models` shared by ten replicas is the point. Per-
    /// allocation scratch is `$LEGION_TASK_DIR`, not a volume.
    ///
    /// Unlike instance volumes these have no `instance_id`, so instance churn
    /// never reclaims them; only a job destroy (via the reaper) or an explicit
    /// `legion volume delete` does.
    fn sync_task_volume_claims_tx(
        &self,
        tx: &Transaction<'_>,
        job_id: &str,
        task: &TaskConfig,
    ) -> R<()> {
        let now = Self::now_rfc3339()?;
        for claim in &task.volumes {
            let volume_id = claim_volume_id(job_id, &task.group, &task.name, &claim.name);
            let plugin = claim.plugin_name().to_string();
            let node_local = self.plugin_is_node_local_tx(tx, &plugin)?;
            let params = serde_json::to_string(&claim.param_map())?;
            // Re-submitting a job refreshes params/plugin without resetting a
            // live volume's node pin or state — EXCEPT when the row is a
            // tombstone. A claim id is a pure function of (job, group, task,
            // name), so re-declaring one after a job destroy lands on the same
            // row; leaving it `destroyed` would make the claim invisible in
            // `legion volume list` and immune to the reaper, even though the
            // worker happily mounts it.
            tx.execute(
                "INSERT INTO volume (volume_id, name, plugin, job_id, instance_id, task_name, \
                 state, node_id, node_local, read_only, params, attrs, mount_path, last_error, \
                 created_at, updated_at, volume_mode) \
                 VALUES (?1, ?2, ?3, ?4, NULL, ?5, 'pending', NULL, ?6, ?7, ?8, '{}', NULL, \
                 NULL, ?9, ?9, ?10) \
                 ON CONFLICT(volume_id) DO UPDATE SET \
                 plugin = ?3, job_id = ?4, task_name = ?5, node_local = ?6, read_only = ?7, \
                 params = ?8, updated_at = ?9, volume_mode = ?10, \
                 state = CASE WHEN volume.state = 'destroyed' THEN 'pending' ELSE volume.state END, \
                 last_error = CASE WHEN volume.state = 'destroyed' THEN NULL ELSE volume.last_error END;",
                params![
                    volume_id,
                    claim.name,
                    plugin,
                    job_id,
                    task.name,
                    node_local as i64,
                    claim.is_read_only() as i64,
                    params,
                    now,
                    claim.volume_mode().as_str(),
                ],
            )?;
        }
        Ok(())
    }

    /// The volume mounts a worker must materialize for one allocation.
    ///
    /// Resolves the task's declarations against the `volume` table so the
    /// worker receives concrete ids and the driver attrs from previous mounts —
    /// it never re-derives a volume from the jobspec. Instance storage comes
    /// first so `LEGION_STORAGE_DIR` is unambiguous.
    #[instrument(err, skip_all)]
    pub fn volume_mounts_for_allocation(
        &self,
        job_id: &str,
        task: &TaskConfig,
        instance: Option<&Instance>,
    ) -> R<Vec<crate::structs::VolumeMount>> {
        let mut mounts = Vec::new();

        if let (Some(storage), Some(instance)) = (
            task.stateful
                .as_ref()
                .and_then(|stateful| stateful.storage.as_ref()),
            instance,
        ) {
            if let Some(handle) = instance.storage_handle.as_deref() {
                let handle = crate::volumes::StorageHandle::parse(handle)?;
                let attrs = self
                    .get_volume(&handle.volume_id)?
                    .map(|volume| volume.attrs)
                    .unwrap_or_default();
                mounts.push(crate::structs::VolumeMount {
                    volume_id: handle.volume_id,
                    name: crate::structs::INSTANCE_VOLUME_NAME.to_string(),
                    plugin: handle.plugin,
                    mode: storage.volume_mode(),
                    params: storage.param_map(),
                    attrs,
                    path: Some(storage.path.clone()),
                    read_only: false,
                    instance_volume: true,
                });
            }
        }

        for claim in &task.volumes {
            let volume_id = claim_volume_id(job_id, &task.group, &task.name, &claim.name);
            let attrs = self
                .get_volume(&volume_id)?
                .map(|volume| volume.attrs)
                .unwrap_or_default();
            mounts.push(crate::structs::VolumeMount {
                volume_id,
                name: claim.name.clone(),
                plugin: claim.plugin_name().to_string(),
                mode: claim.volume_mode(),
                params: claim.param_map(),
                attrs,
                path: claim.path.clone(),
                read_only: claim.is_read_only(),
                instance_volume: false,
            });
        }

        Ok(mounts)
    }

    /// Whether any node advertises `plugin` as node-local. Unknown plugins are
    /// treated as node-local (see [`Self::mint_instance_volume_tx`]).
    fn plugin_is_node_local_tx(&self, tx: &Transaction<'_>, plugin: &str) -> R<bool> {
        let mut stmt = tx.prepare("SELECT fingerprint_attrs FROM node;")?;
        let rows = stmt.query_map([], |row| row.get::<_, Value>(0))?;
        for row in rows {
            let Ok(attrs) = row else { continue };
            if let Some(node_local) = attrs
                .get("volumePlugins")
                .and_then(|plugins| plugins.get(plugin))
                .and_then(|caps| caps.get("nodeLocal"))
                .and_then(Value::as_bool)
            {
                return Ok(node_local);
            }
        }
        Ok(true)
    }

    #[instrument(err, skip_all)]
    pub fn get_volume(&self, volume_id: &str) -> R<Option<Volume>> {
        let conn = self.read_connection()?;
        let volume = conn
            .query_row(VOLUME_SELECT.as_str(), [volume_id], |row| {
                self.volume_from_row(row)
            })
            .optional()?;
        Ok(volume)
    }

    /// Volumes matching the given filters, newest first. Every filter is
    /// optional; passing none lists the whole cluster.
    #[instrument(err, skip_all)]
    pub fn list_volumes(&self, filter: &VolumeFilter) -> R<Vec<Volume>> {
        let conn = self.read_connection()?;
        let mut sql = VOLUME_SELECT_ALL.clone();
        let mut clauses: Vec<String> = Vec::new();
        let mut args: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        if let Some(job_id) = filter.job_id.as_deref() {
            clauses.push(format!("job_id = ?{}", args.len() + 1));
            args.push(Box::new(job_id.to_string()));
        }
        if let Some(node_id) = filter.node_id.as_deref() {
            clauses.push(format!("node_id = ?{}", args.len() + 1));
            args.push(Box::new(node_id.to_string()));
        }
        if let Some(plugin) = filter.plugin.as_deref() {
            clauses.push(format!("plugin = ?{}", args.len() + 1));
            args.push(Box::new(plugin.to_string()));
        }
        if let Some(instance_id) = filter.instance_id.as_deref() {
            clauses.push(format!("instance_id = ?{}", args.len() + 1));
            args.push(Box::new(instance_id.to_string()));
        }
        if let Some(state) = filter.state {
            clauses.push(format!("state = ?{}", args.len() + 1));
            args.push(Box::new(state.as_str().to_string()));
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY created_at DESC, volume_id ASC;");

        let mut stmt = conn.prepare(&sql)?;
        let refs = args
            .iter()
            .map(|arg| arg.as_ref() as &dyn rusqlite::ToSql)
            .collect::<Vec<_>>();
        let rows = stmt.query_map(refs.as_slice(), |row| self.volume_from_row(row))?;
        Self::collect_rows(rows)
    }

    /// Record a successful mount reported by a worker: pin the node, refresh
    /// the driver's attrs, and clear any previous error.
    ///
    /// This is where `node_local` becomes authoritative — the worker asked the
    /// driver, so its answer beats the manager's mint-time guess.
    #[instrument(err, skip_all)]
    pub fn record_volume_mounted(
        &self,
        volume_id: &str,
        node_id: &str,
        mount_path: &str,
        node_local: bool,
        fs: Option<&str>,
        attrs: &BTreeMap<String, Value>,
    ) -> R<()> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        let attrs_json = serde_json::to_string(attrs)?;
        // A successful mount is proof the storage exists and is in use, so it
        // is allowed to correct a stale `destroyed` tombstone — a task volume
        // claim re-declared after a job destroy lands on the same deterministic
        // id, and leaving it tombstoned would make a mounted volume invisible
        // in `legion volume list` and immune to the reaper.
        //
        // `destroying` is the one state a mount report must NOT overwrite: a
        // reclaim is deliberately in flight, and resurrecting the row would
        // race the driver's destroy.
        conn.execute(
            "UPDATE volume SET state = 'created', node_id = ?1, mount_path = ?2, \
             node_local = ?3, attrs = ?4, fs = COALESCE(?5, fs), last_error = NULL, \
             updated_at = ?6 WHERE volume_id = ?7 AND state != 'destroying';",
            params![
                node_id,
                mount_path,
                node_local as i64,
                attrs_json,
                fs,
                now,
                volume_id
            ],
        )?;
        Ok(())
    }

    /// Record a driver failure against a volume so it explains itself in
    /// `legion volume list` instead of only in a worker log.
    #[instrument(err, skip_all)]
    pub fn record_volume_error(&self, volume_id: &str, error: &str) -> R<()> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        conn.execute(
            "UPDATE volume SET last_error = ?1, updated_at = ?2 WHERE volume_id = ?3;",
            params![error, now, volume_id],
        )?;
        Ok(())
    }

    /// Mark every live volume no instance references as `destroying`.
    ///
    /// One rule covers every way a handle can be dropped — keyed destroy, slot
    /// scale-in with `retainOnScaleIn: false`, `retainOnStop: false` on job
    /// stop, and a job destroy that cascades the instance rows away. Reclaim
    /// therefore cannot be forgotten by a code path that drops a handle
    /// without knowing about volumes.
    ///
    /// Returns the volumes newly marked, for event emission.
    #[instrument(err, skip_all)]
    pub fn reap_orphaned_volumes(&self) -> R<Vec<Volume>> {
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let now = Self::now_rfc3339()?;
        let orphans: Vec<Volume> = {
            let mut stmt = tx.prepare(&format!(
                // Two orphan rules, one query:
                //  - an INSTANCE volume no instance references any more (keyed
                //    destroy, scale-in without retention, retainOnStop: false,
                //    or a job destroy that cascaded the instance rows away);
                //  - a task CLAIM whose job no longer exists.
                // Reconciling on "nothing references this" means no handle-drop
                // path can forget to free storage.
                "{} WHERE state IN ('pending', 'created') AND (\
                     (instance_id IS NOT NULL AND NOT EXISTS (\
                         SELECT 1 FROM instance \
                         WHERE instance.instance_id = volume.instance_id \
                         AND instance.storage_handle = volume.plugin || '/' || volume.volume_id\
                     )) \
                     OR (instance_id IS NULL AND job_id IS NOT NULL AND NOT EXISTS (\
                         SELECT 1 FROM job WHERE job.id = volume.job_id\
                     ))\
                 );",
                *VOLUME_SELECT_ALL
            ))?;
            let rows = stmt.query_map([], |row| self.volume_from_row(row))?;
            Self::collect_rows(rows)?
        };
        for volume in &orphans {
            tx.execute(
                "UPDATE volume SET state = 'destroying', updated_at = ?1 WHERE volume_id = ?2;",
                params![now, volume.volume_id],
            )?;
        }
        tx.commit()?;
        if !orphans.is_empty() {
            info!(
                count = orphans.len(),
                "Marked orphaned volumes for reclamation"
            );
        }
        Ok(orphans)
    }

    /// Volumes this node should run `destroy` for.
    ///
    /// A node-local volume can only be reclaimed by its own node. A volume
    /// with no pin (never mounted, or a network driver) may be reclaimed by
    /// any node that has the plugin — the caller filters on that.
    #[instrument(err, skip_all)]
    pub fn volumes_to_destroy_for_node(&self, node_id: &str) -> R<Vec<Volume>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&format!(
            "{} WHERE state = 'destroying' \
             AND (node_id = ?1 OR node_id IS NULL OR node_local = 0) \
             ORDER BY updated_at ASC;",
            *VOLUME_SELECT_ALL
        ))?;
        let rows = stmt.query_map([node_id], |row| self.volume_from_row(row))?;
        Self::collect_rows(rows)
    }

    /// A worker confirmed the driver reclaimed the volume.
    #[instrument(err, skip_all)]
    pub fn mark_volume_destroyed(&self, volume_id: &str) -> R<()> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        conn.execute(
            "UPDATE volume SET state = 'destroyed', node_id = NULL, mount_path = NULL, \
             last_error = NULL, updated_at = ?1 WHERE volume_id = ?2;",
            params![now, volume_id],
        )?;
        info!(volume_id, "Volume destroyed");
        Ok(())
    }

    /// Operator-initiated reclaim (`legion volume delete`).
    ///
    /// Without `force` the volume is queued for its driver like any other
    /// reclaim. With `force` it is marked `destroyed` immediately, WITHOUT
    /// driver confirmation — the escape hatch for a node-local volume whose
    /// node is never coming back. Forcing leaves whatever bytes exist on the
    /// (dead) node; that is the operator's explicit call, and the reason it is
    /// not the default.
    #[instrument(err, skip_all)]
    pub fn delete_volume(&self, volume_id: &str, force: bool) -> R<Volume> {
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let volume: Volume = tx
            .query_row(VOLUME_SELECT.as_str(), [volume_id], |row| {
                self.volume_from_row(row)
            })
            .optional()?
            .with_context(|| format!("no volume {volume_id}"))?;
        let now = Self::now_rfc3339()?;

        // Detaching the handle first means the instance mints a fresh volume
        // on its next placement instead of pointing at storage being reclaimed.
        if let Some(instance_id) = volume.instance_id.as_deref() {
            tx.execute(
                "UPDATE instance SET storage_handle = NULL, updated_at = ?1 \
                 WHERE instance_id = ?2 AND storage_handle = ?3;",
                params![now, instance_id, volume.handle()],
            )?;
        }
        let state = if force { "destroyed" } else { "destroying" };
        tx.execute(
            "UPDATE volume SET state = ?1, updated_at = ?2, \
             node_id = CASE WHEN ?1 = 'destroyed' THEN NULL ELSE node_id END \
             WHERE volume_id = ?3;",
            params![state, now, volume_id],
        )?;
        tx.commit()?;

        warn!(volume_id, force, "Operator requested volume deletion");
        self.get_volume(volume_id)?
            .with_context(|| format!("volume {volume_id} vanished during delete"))
    }

    /// Node pins for the instances of `task_id` that are awaiting placement.
    ///
    /// A node-local volume binds its instance to one node, so a replacement
    /// allocation for that instance must land there. Returns `(instance_id,
    /// node_id)` pairs for live instances of this task that have a pinned
    /// volume and no current allocation — exactly the set the next plan must
    /// place. Ordered so slot pools are deterministic.
    #[instrument(err, skip_all)]
    pub fn pinned_placements_for_task(&self, task_id: i64) -> R<Vec<(String, String)>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT instance.instance_id, volume.node_id \
             FROM instance \
             INNER JOIN volume ON volume.instance_id = instance.instance_id \
             INNER JOIN task_config ON task_config.id = ?1 \
             WHERE instance.job_id = task_config.job_id \
             AND instance.group_name = task_config.group_name \
             AND instance.task_name = task_config.name \
             AND instance.state IN ('pending', 'rescheduling') \
             AND instance.current_alloc_id IS NULL \
             AND instance.storage_handle = volume.plugin || '/' || volume.volume_id \
             AND volume.node_local = 1 \
             AND volume.node_id IS NOT NULL \
             AND volume.state IN ('pending', 'created') \
             ORDER BY instance.slot_index ASC, instance.instance_id ASC;",
        )?;
        let rows = stmt.query_map([task_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Self::collect_rows(rows)
    }

    /// The node a specific instance's volume is pinned to, if any. Used by the
    /// allocation binder to pair an allocation with the instance whose data
    /// already lives on the node it landed on.
    fn pinned_node_for_instance_tx(
        &self,
        tx: &Transaction<'_>,
        instance_id: &str,
    ) -> R<Option<String>> {
        let node_id = tx
            .query_row(
                "SELECT volume.node_id FROM volume \
                 INNER JOIN instance ON instance.instance_id = volume.instance_id \
                 WHERE volume.instance_id = ?1 AND volume.node_local = 1 \
                 AND volume.node_id IS NOT NULL AND volume.state IN ('pending', 'created') \
                 AND instance.storage_handle = volume.plugin || '/' || volume.volume_id \
                 LIMIT 1;",
                [instance_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?;
        Ok(node_id.flatten())
    }

    /// Volume plugin capabilities advertised across the cluster, keyed by
    /// plugin name, with the node ids that provide each.
    ///
    /// This is the manager's only source of plugin metadata (it loads no
    /// drivers itself), so it backs both admission validation and
    /// `legion volume plugins`.
    #[instrument(err, skip_all)]
    pub fn cluster_volume_plugins(&self) -> R<BTreeMap<String, ClusterVolumePlugin>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare("SELECT id, status, fingerprint_attrs FROM node;")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Value>(2)?,
            ))
        })?;

        let mut out: BTreeMap<String, ClusterVolumePlugin> = BTreeMap::new();
        for row in rows {
            let (node_id, status, attrs) = row?;
            let Some(plugins) = attrs.get("volumePlugins").and_then(Value::as_object) else {
                continue;
            };
            for (name, raw) in plugins {
                let Ok(caps) =
                    serde_json::from_value::<crate::volumes::VolumeCapabilities>(raw.clone())
                else {
                    continue;
                };
                let entry = out
                    .entry(name.clone())
                    .or_insert_with(|| ClusterVolumePlugin {
                        capabilities: caps.clone(),
                        nodes: Vec::new(),
                        available_nodes: Vec::new(),
                    });
                entry.nodes.push(node_id.clone());
                // "Available" means a live node could actually mount it, which
                // is the question admission and placement care about.
                if caps.available && status != "lost" {
                    entry.available_nodes.push(node_id.clone());
                    entry.capabilities = caps;
                }
            }
        }
        for entry in out.values_mut() {
            entry.nodes.sort();
            entry.nodes.dedup();
            entry.available_nodes.sort();
            entry.available_nodes.dedup();
        }
        Ok(out)
    }
}

/// Filters for [`Store::list_volumes`].
#[derive(Debug, Clone, Default)]
pub struct VolumeFilter {
    pub job_id: Option<String>,
    pub node_id: Option<String>,
    pub plugin: Option<String>,
    pub instance_id: Option<String>,
    pub state: Option<crate::structs::VolumeState>,
}

/// A volume plugin as the cluster sees it: one capability record plus the
/// nodes providing it.
#[derive(Debug, Clone)]
pub struct ClusterVolumePlugin {
    pub capabilities: crate::volumes::VolumeCapabilities,
    /// Every node that has the plugin installed, including ones where it is
    /// unusable.
    pub nodes: Vec<String>,
    /// Nodes where the plugin is installed AND usable — the placement set.
    pub available_nodes: Vec<String>,
}

const VOLUME_COLUMNS: &str = "volume_id, name, plugin, job_id, instance_id, task_name, state, \
     node_id, node_local, read_only, params, attrs, mount_path, last_error, created_at, \
     updated_at, volume_mode, fs";

lazy_static! {
    static ref VOLUME_SELECT: String =
        format!("SELECT {VOLUME_COLUMNS} FROM volume WHERE volume_id = ?;");
    static ref VOLUME_SELECT_ALL: String = format!("SELECT {VOLUME_COLUMNS} FROM volume");
}

/// Deterministic id for a task volume claim.
///
/// Claims are shared per `(job, group, task, name)`, so the id must be a pure
/// function of those — a resubmit has to land on the same volume, not orphan
/// the old one. The readable prefix keeps `legion volume list` legible; the
/// hash suffix keeps ids unique after the prefix is sanitized and truncated.
fn claim_volume_id(job_id: &str, group: &str, task: &str, name: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(format!("{job_id}\u{1}{group}\u{1}{task}\u{1}{name}"));
    let digest = format!("{:x}", hasher.finalize());
    let label = format!("{job_id}-{task}-{name}")
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .take(48)
        .collect::<String>();
    format!("claim-{label}-{}", &digest[..12])
}

/// Parse a JSONB object column into a sorted map, tolerating NULL and
/// non-object values (both become empty).
fn parse_json_map_column(
    idx: usize,
    raw: Option<String>,
) -> Result<BTreeMap<String, Value>, rusqlite::Error> {
    let Some(raw) = raw else {
        return Ok(BTreeMap::new());
    };
    let value: Value = serde_json::from_str(&raw).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(idx, rusqlite::types::Type::Text, Box::new(err))
    })?;
    Ok(value
        .as_object()
        .map(|map| {
            map.iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        })
        .unwrap_or_default())
}

/// Mint an opaque storage handle for an instance: `{plugin}/{volume_id}`.
///
/// The format predates volume drivers (instances already minted
/// `local-dir/<rand>`), which is exactly why it was kept — every instance row
/// written before this feature still parses. See `docs/volumes.md`.
fn mint_storage_handle(plugin: &str) -> String {
    format!("{plugin}/{}", random_string(32))
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
                ..Default::default()
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
    fn tasks_with_actions_round_trip() -> R<()> {
        use crate::structs::{ActionParam, ActionParamType, ActionSpec};
        let store = setup_store()?;
        let mut t = task("worker");
        t.actions = vec![ActionSpec {
            name: "flush-cache".into(),
            description: "Flush it".into(),
            command: vec!["/bin/echo".into(), "${param.scope}".into()],
            params: vec![ActionParam {
                name: "scope".into(),
                description: String::new(),
                param_type: ActionParamType::Enum,
                default: Some("all".into()),
                required: None,
                values: vec!["all".into(), "sessions".into()],
                pattern: None,
                min: None,
                max: None,
                sensitive: false,
            }],
            tty: false,
            timeout: None,
        }];
        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "actions-persist".into(),
            tasks: vec![t.clone()],
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
        // Read back through both parsers: get_job_config and the read-model.
        let loaded = store.get_job_config(&job.name)?;
        assert_eq!(loaded.tasks[0].actions, t.actions);
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
            host_volumes: String::new(),
            name: "test".to_string(),
            pool: "default".to_string(),
            additional_node_checks: vec![],
            attributes: attrs,
            name_prefix: None,
            resources: "{}".into(),
            reserved_resources: "{}".into(),
            max_allocations: None,
            artifacts: String::new(),
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
            host_volumes: String::new(),
            name: "empty".to_string(),
            pool: "default".to_string(),
            additional_node_checks: vec![],
            attributes: HashMap::new(),
            name_prefix: None,
            resources: "{}".into(),
            reserved_resources: "{}".into(),
            max_allocations: None,
            artifacts: String::new(),
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

    /// Slot-mode stateful task with an explicit onLost policy (None values =
    /// the defaults).
    fn fenced_slot_task(
        name: &str,
        self_terminate_after: Option<&str>,
        replace_after: Option<&str>,
    ) -> TaskConfig {
        let mut task = stateful_slot_task(name, 1, true);
        if self_terminate_after.is_some() || replace_after.is_some() {
            if let Some(stateful) = task.stateful.as_mut() {
                stateful.on_lost = Some(StatefulOnLostConfig {
                    self_terminate_after: self_terminate_after.map(String::from),
                    replace_after: replace_after.map(String::from),
                });
            }
        }
        task
    }

    fn backdated_rfc3339(minutes_ago: i64) -> R<String> {
        Ok((OffsetDateTime::now_utc() - TimeDuration::minutes(minutes_ago)).format(&Rfc3339)?)
    }

    #[test]
    fn mark_node_lost_sets_lost_at_once_and_heartbeat_clears_it() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-one", "active"))?;
        assert_eq!(store.node_lost_at("node-one")?, None);

        store.mark_node_lost("node-one")?;
        let first = store.node_lost_at("node-one")?.context("lost_at set")?;

        // A repeated mark never pushes the loss-confirmation clock back.
        store.mark_node_lost("node-one")?;
        assert_eq!(store.node_lost_at("node-one")?.as_deref(), Some(&*first));

        // A heartbeat means the node is back: lost_at clears, status resets.
        store.update_node_heartbeat("node-one".into())?;
        assert_eq!(store.node_lost_at("node-one")?, None);
        assert_eq!(store.get_node("node-one")?.status, "active");

        // Re-registration (the upsert path) also clears a stale lost_at.
        store.mark_node_lost("node-one")?;
        assert!(store.node_lost_at("node-one")?.is_some());
        store.insert_node(make_node_msg("node-one", "active"))?;
        assert_eq!(store.node_lost_at("node-one")?, None);
        Ok(())
    }

    #[test]
    fn stateful_alloc_on_lost_node_is_held_before_replace_threshold() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        let job_id = "fence-hold";
        submit_job(&store, job_id, vec![fenced_slot_task("db", None, None)])?;
        let task_id = task_id_for(&store, "db", job_id)?;
        let alloc = store.place_alloc("node-one", task_id)?;
        let instance_id = alloc.instance_id.clone().context("instance bound")?;

        // Node just lost: well inside the default 180s replaceAfter window.
        store.mark_nodes_as_lost(&["node-one".to_string()])?;

        // NOT freed: the allocation row stays on the lost node, the instance
        // stays active and bound, and nothing was placed elsewhere.
        let held = store.allocs_for_node("node-one")?;
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].id, alloc.id);
        assert!(store.allocs_for_node("node-two")?.is_empty());
        let instance = store.get_instance(&instance_id)?.context("instance")?;
        assert_eq!(instance.state, InstanceState::Active);
        assert_eq!(instance.current_alloc_id, Some(alloc.id));

        // Re-running the sweep (the monitor tick) is idempotent while the
        // threshold has not elapsed.
        store.recheck_lost_nodes();
        assert_eq!(store.allocs_for_node("node-one")?.len(), 1);
        assert!(store.allocs_for_node("node-two")?.is_empty());
        Ok(())
    }

    #[test]
    fn stateful_alloc_freed_after_threshold_rebinds_same_instance() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        let job_id = "fence-free";
        submit_job(
            &store,
            job_id,
            vec![fenced_slot_task("db", Some("30s"), Some("2m"))],
        )?;
        let task_id = task_id_for(&store, "db", job_id)?;
        let alloc = store.place_alloc("node-one", task_id)?;
        let instance_id = alloc.instance_id.clone().context("instance bound")?;
        let original_handle = store
            .get_instance(&instance_id)?
            .context("instance")?
            .storage_handle;
        assert!(original_handle.is_some());

        store.mark_nodes_as_lost(&["node-one".to_string()])?;
        // Held while inside the window...
        assert_eq!(store.allocs_for_node("node-one")?.len(), 1);

        // ...then the loss is confirmed: backdate lost_at past replaceAfter
        // and run the monitor sweep.
        store.set_node_lost_at("node-one", &backdated_rfc3339(10)?)?;
        store.recheck_lost_nodes();

        assert!(store.allocs_for_node("node-one")?.is_empty());
        let replacements = store.allocs_for_node("node-two")?;
        assert_eq!(replacements.len(), 1);
        // The replacement re-binds the SAME instance id and storage handle.
        assert_eq!(replacements[0].instance_id.as_deref(), Some(&*instance_id));
        let rebound = store.get_instance(&instance_id)?.context("instance")?;
        assert_eq!(rebound.state, InstanceState::Active);
        assert_eq!(rebound.current_alloc_id, Some(replacements[0].id));
        assert_eq!(rebound.storage_handle, original_handle);
        Ok(())
    }

    #[test]
    fn stateful_alloc_with_replace_after_never_blocks_until_node_returns() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        let job_id = "fence-never";
        submit_job(
            &store,
            job_id,
            vec![fenced_slot_task("db", None, Some("never"))],
        )?;
        let task_id = task_id_for(&store, "db", job_id)?;
        let alloc = store.place_alloc("node-one", task_id)?;
        let instance_id = alloc.instance_id.clone().context("instance bound")?;

        store.mark_nodes_as_lost(&["node-one".to_string()])?;
        // Even an ancient loss never auto-replaces with "never".
        store.set_node_lost_at("node-one", &backdated_rfc3339(60 * 24)?)?;
        store.recheck_lost_nodes();
        assert_eq!(store.allocs_for_node("node-one")?.len(), 1);
        assert!(store.allocs_for_node("node-two")?.is_empty());

        // The node returning is what frees it: binding intact, no churn.
        store.update_node_heartbeat("node-one".into())?;
        store.recheck_lost_nodes();
        let kept = store.allocs_for_node("node-one")?;
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].id, alloc.id);
        let instance = store.get_instance(&instance_id)?.context("instance")?;
        assert_eq!(instance.state, InstanceState::Active);
        assert_eq!(instance.current_alloc_id, Some(alloc.id));
        Ok(())
    }

    #[test]
    fn node_recovery_before_threshold_keeps_binding_and_clears_lost_at() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        let job_id = "fence-recover";
        submit_job(&store, job_id, vec![fenced_slot_task("db", None, None)])?;
        let task_id = task_id_for(&store, "db", job_id)?;
        let alloc = store.place_alloc("node-one", task_id)?;
        let instance_id = alloc.instance_id.clone().context("instance bound")?;

        store.mark_nodes_as_lost(&["node-one".to_string()])?;
        assert!(store.node_lost_at("node-one")?.is_some());

        // Node heartbeats again BEFORE the threshold: lost_at clears and the
        // allocation/instance binding is untouched — no replacement ever
        // happened.
        store.update_node_heartbeat("node-one".into())?;
        assert_eq!(store.node_lost_at("node-one")?, None);
        store.recheck_lost_nodes();

        let kept = store.allocs_for_node("node-one")?;
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].id, alloc.id);
        assert!(store.allocs_for_node("node-two")?.is_empty());
        let instance = store.get_instance(&instance_id)?.context("instance")?;
        assert_eq!(instance.state, InstanceState::Active);
        assert_eq!(instance.current_alloc_id, Some(alloc.id));
        Ok(())
    }

    #[test]
    fn stateless_allocs_on_lost_node_replaced_immediately_while_stateful_held() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        let job_id = "fence-mixed";
        submit_job(
            &store,
            job_id,
            vec![task("web"), fenced_slot_task("db", None, None)],
        )?;
        let web_task = task_id_for(&store, "web", job_id)?;
        let db_task = task_id_for(&store, "db", job_id)?;
        store.place_alloc("node-one", web_task)?;
        let db_alloc = store.place_alloc("node-one", db_task)?;

        store.mark_nodes_as_lost(&["node-one".to_string()])?;

        // Stateless web: replaced eagerly on node-two. Stateful db: held on
        // the lost node pending loss confirmation.
        let lost_node_allocs = store.allocs_for_node("node-one")?;
        assert_eq!(lost_node_allocs.len(), 1);
        assert_eq!(lost_node_allocs[0].id, db_alloc.id);
        assert_eq!(lost_node_allocs[0].task_config_id, db_task);

        let replaced = store.allocs_for_node("node-two")?;
        assert_eq!(replaced.len(), 1);
        assert_eq!(replaced[0].task_config_id, web_task);
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
    fn claim_eval_dedupe_spares_other_kinds_and_delayed_work() -> R<()> {
        let store = setup_store()?;
        let job_config_id = submit_job(&store, "job-a", vec![task("api")])?;
        let cfg = Some(job_config_id.as_str());

        // A rollout's next tick and a batch task's next scheduled update are
        // both pending for the same job when a reschedule gets claimed.
        store.enqueue_eval("rollout_tick", Some("job-a"), cfg, None, Some(50))?;
        store.enqueue_eval_after_delay(
            "job_update",
            Some("job-a"),
            cfg,
            None,
            Some(50),
            Duration::from_secs(3600),
        )?;
        // Two identical reschedules: the redundant one is what dedupe is for.
        store.enqueue_eval("job_reschedule", Some("job-a"), cfg, None, Some(50))?;
        store.enqueue_eval("job_reschedule", Some("job-a"), cfg, None, Some(50))?;

        let claimed = store.claim_eval()?.context("eval should be claimed")?;
        assert_eq!(claimed.kind, "rollout_tick");
        store.complete_eval(&claimed.id, true)?;
        let claimed = store.claim_eval()?.context("eval should be claimed")?;
        assert_eq!(claimed.kind, "job_reschedule");

        let remaining: Vec<String> = store
            .get_evals()?
            .into_iter()
            .filter(|eval| eval.status == "pending")
            .map(|eval| eval.kind)
            .collect();
        assert_eq!(
            remaining,
            vec!["job_update".to_string()],
            "only the duplicate reschedule should have been collapsed"
        );

        Ok(())
    }

    #[test]
    fn zero_delay_evals_are_immediately_claimable() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-1", "ready"))?;

        // `ready_at` is range-compared as text in SQL, so a timestamp format
        // that does not sort lexicographically makes a freshly enqueued eval
        // intermittently invisible. Roughly one write in ten lands on a
        // trailing-zero subsecond, so a single round would miss the
        // regression.
        for _ in 0..200 {
            let enqueued = store.enqueue_eval("node_join", None, None, Some("node-1"), Some(50))?;
            let claimed = store
                .claim_eval()?
                .context("zero-delay eval should be claimable right away")?;
            assert_eq!(claimed.id, enqueued.id);
            store.complete_eval(&claimed.id, true)?;
        }

        Ok(())
    }

    #[test]
    fn sortable_timestamps_sort_chronologically() -> R<()> {
        let base = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
        let earlier = Store::sortable_timestamp(base + TimeDuration::nanoseconds(437_400_000))?;
        let later = Store::sortable_timestamp(base + TimeDuration::nanoseconds(437_409_000))?;
        assert!(
            earlier < later,
            "expected {earlier} to sort before {later} as text"
        );
        assert_eq!(earlier.len(), later.len());
        // Still a valid RFC3339 instant, so parsing paths are unaffected.
        assert_eq!(
            OffsetDateTime::parse(&later, &Rfc3339)?,
            base + TimeDuration::nanoseconds(437_409_000)
        );

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
    fn artifact_staging_transitions_emit_events_once() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "artifact-events".into(),
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

        let artifact_events = |store: &Store| -> R<Vec<SchedulerEventRecord>> {
            store.list_scheduler_events(&SchedulerEventFilter {
                kind: Some("artifact".into()),
                ..Default::default()
            })
        };

        let snapshot = |states: &str, status: &str| AllocationHealth {
            allocation_id: allocation.id,
            healthy: false,
            status: status.into(),
            artifact_states: Some(states.into()),
        };
        let staging =
            r#"[{"name":"tool","source":"https://example.com/t.tar.gz","state":"fetching"}]"#;
        let staged = r#"[{"name":"tool","source":"https://example.com/t.tar.gz","state":"staged","digest":"sha256:abc","bytesFetched":10}]"#;

        // First staging snapshot announces the run.
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[snapshot(staging, "staging artifacts")],
        )?;
        let events = artifact_events(&store)?;
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0].kind, "artifact.staging");
        assert_eq!(events[0].allocation_id, Some(allocation.id));
        assert_eq!(events[0].job_id.as_deref(), Some("artifact-events"));
        assert_eq!(events[0].node_id.as_deref(), Some("node-a"));

        // Repeated identical heartbeats emit nothing new.
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[snapshot(staging, "staging artifacts")],
        )?;
        assert_eq!(artifact_events(&store)?.len(), 1);

        // Completion emits artifact.staged exactly once.
        store.upsert_allocation_health_snapshots("node-a", &[snapshot(staged, "starting")])?;
        store.upsert_allocation_health_snapshots("node-a", &[snapshot(staged, "healthy")])?;
        let events = artifact_events(&store)?;
        assert_eq!(events.len(), 2, "{events:?}");
        assert_eq!(events[1].kind, "artifact.staged");
        assert!(events[1].metadata_json.contains("sha256:abc"));

        // Snapshots without artifact rows never touch the artifact kinds.
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[AllocationHealth {
                allocation_id: allocation.id,
                healthy: true,
                status: "healthy".into(),
                artifact_states: None,
            }],
        )?;
        assert_eq!(artifact_events(&store)?.len(), 2);
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
                artifact_states: None,
            }],
        )?;
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[AllocationHealth {
                allocation_id: allocation.id,
                healthy: true,
                status: "healthy".into(),
                artifact_states: None,
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
                artifact_states: None,
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
    fn scheduler_events_emit_and_list_with_filters() -> R<()> {
        let store = setup_store()?;
        assert_eq!(store.latest_scheduler_event_id()?, 0);

        store.emit_event(
            EventDraft::info("eval.completed", "Eval 1 (job_submit) completed")
                .job("demo")
                .eval("1"),
        );
        store.emit_event(EventDraft::warn("node.lost", "Node node-a marked lost").node("node-a"));
        store.emit_event(
            EventDraft::info("allocation.placed", "Placed 2 allocation(s) for job demo")
                .job("demo")
                .meta(json!({ "count": 2 })),
        );

        let all = store.list_scheduler_events(&SchedulerEventFilter::default())?;
        assert_eq!(all.len(), 3);
        assert!(all.windows(2).all(|pair| pair[0].id < pair[1].id));
        assert_eq!(store.latest_scheduler_event_id()?, all[2].id);

        let demo_events = store.list_scheduler_events(&SchedulerEventFilter {
            job_id: Some("demo".into()),
            ..Default::default()
        })?;
        assert_eq!(demo_events.len(), 2);

        let eval_events = store.list_scheduler_events(&SchedulerEventFilter {
            kind: Some("eval".into()),
            ..Default::default()
        })?;
        assert_eq!(eval_events.len(), 1);
        assert_eq!(eval_events[0].kind, "eval.completed");
        assert_eq!(eval_events[0].eval_id.as_deref(), Some("1"));

        let warn_events = store.list_scheduler_events(&SchedulerEventFilter {
            level: Some("warn".into()),
            ..Default::default()
        })?;
        assert_eq!(warn_events.len(), 1);
        assert_eq!(warn_events[0].node_id.as_deref(), Some("node-a"));

        let after = store.list_scheduler_events(&SchedulerEventFilter {
            after_id: Some(all[0].id),
            ..Default::default()
        })?;
        assert_eq!(after.len(), 2);
        assert_eq!(after[0].id, all[1].id);

        let placed = &all[2];
        assert_eq!(placed.metadata_json, json!({ "count": 2 }).to_string());

        Ok(())
    }

    #[test]
    fn event_recorder_emits_expected_kinds_and_skips_empty() -> R<()> {
        let store = setup_store()?;

        // Empty inputs are no-ops, so callers don't need their own guards.
        store.events().allocations_removed_from_node("node-a", &[]);
        store.events().allocations_removed_for_job("demo", 0);
        assert!(drafts::allocations_placed("cfg-1", &[]).is_none());
        assert!(drafts::job_submit_outcome(
            "demo",
            &JobSubmitOutcome::Unchanged {
                job_config_id: "cfg-1".into(),
            },
            1,
            1,
        )
        .is_none());
        assert!(store
            .list_scheduler_events(&SchedulerEventFilter::default())?
            .is_empty());

        let submitted = drafts::job_submit_outcome(
            "demo",
            &JobSubmitOutcome::Created {
                job_config_id: "cfg-1".into(),
            },
            1,
            2,
        )
        .context("created outcome should produce an event")?;
        store.emit_event(submitted);
        store
            .events()
            .allocation_failed(7, "node-a", "demo", "cfg-1", 3, "task crashed");
        store.events().node_lost_after_missed_heartbeats("node-a");

        let events = store.list_scheduler_events(&SchedulerEventFilter::default())?;
        let kinds: Vec<&str> = events.iter().map(|event| event.kind.as_str()).collect();
        assert_eq!(
            kinds,
            vec!["job.submitted", "allocation.failed", "node.lost"]
        );

        let failed = &events[1];
        assert_eq!(failed.level, "warn");
        assert_eq!(failed.job_id.as_deref(), Some("demo"));
        assert_eq!(failed.node_id.as_deref(), Some("node-a"));
        assert_eq!(failed.allocation_id, Some(7));
        assert!(failed.message.contains("task crashed"));

        let lost = &events[2];
        assert_eq!(
            lost.metadata_json,
            json!({ "reason": "heartbeat" }).to_string()
        );

        Ok(())
    }

    #[test]
    fn complete_eval_with_event_records_event_atomically() -> R<()> {
        let store = setup_store()?;
        let eval = store.enqueue_eval("job_submit", None, None, None, None)?;
        let claimed = store.claim_eval()?.context("eval should be claimable")?;
        assert_eq!(claimed.id, eval.id);

        store.complete_eval_with_event(
            &eval.id,
            true,
            drafts::eval_completed(&claimed, "done", std::time::Duration::from_millis(5)),
        )?;

        assert!(store.get_evals()?.is_empty());
        let events = store.list_scheduler_events(&SchedulerEventFilter {
            kind: Some("eval.completed".into()),
            ..Default::default()
        })?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].eval_id.as_deref(), Some(eval.id.as_str()));
        assert!(events[0].message.contains("done"));

        Ok(())
    }

    #[test]
    fn scheduler_events_list_without_cursor_returns_newest_window() -> R<()> {
        let store = setup_store()?;
        for index in 0..10 {
            store.emit_event(EventDraft::info("eval.enqueued", format!("event {index}")));
        }

        let window = store.list_scheduler_events(&SchedulerEventFilter {
            limit: Some(3),
            ..Default::default()
        })?;
        assert_eq!(window.len(), 3);
        // Newest three, still in ascending order.
        assert_eq!(window[0].message, "event 7");
        assert_eq!(window[2].message, "event 9");

        Ok(())
    }

    #[tokio::test]
    async fn scheduler_events_broadcast_to_subscribers() -> R<()> {
        let store = setup_store()?;
        let mut subscriber = store.subscribe_events();

        store.emit_event(EventDraft::info("job.submitted", "Job demo submitted").job("demo"));

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), subscriber.recv())
            .await
            .context("timed out waiting for broadcast event")??;
        assert_eq!(event.kind, "job.submitted");
        assert_eq!(event.job_id.as_deref(), Some("demo"));
        assert!(event.id > 0);

        Ok(())
    }

    #[test]
    fn store_mutations_emit_scheduler_events() -> R<()> {
        let store = setup_store()?;
        store.insert_node(NodeMsg::from(make_node("events-node")))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "events-job".into(),
            tasks: vec![TaskConfig {
                resources: json!({ "memory": 64 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[])?;
        store.enqueue_eval("job_submit", Some(&job.name), None, None, None)?;
        store.stop_job(&job.name)?;
        store.mark_node_lost("events-node")?;

        let events = store.list_scheduler_events(&SchedulerEventFilter::default())?;
        let kinds: Vec<&str> = events.iter().map(|event| event.kind.as_str()).collect();
        assert!(kinds.contains(&"node.registered"));
        assert!(kinds.contains(&"job.submitted"));
        assert!(kinds.contains(&"eval.enqueued"));
        assert!(kinds.contains(&"job.stopped"));
        assert!(kinds.contains(&"node.lost"));

        let submit = events
            .iter()
            .find(|event| event.kind == "job.submitted")
            .context("missing job.submitted event")?;
        assert_eq!(submit.job_id.as_deref(), Some("events-job"));
        assert_eq!(submit.level, "info");

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
                initial_instances: None,
                on_lost: None,
                storage: with_storage.then(|| crate::structs::StatefulStorageConfig {
                    plugin: None,
                    mode: None,
                    params: None,
                    path: "/var/lib/data".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
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
    fn slot_binding_prefers_slot_with_no_live_allocation_in_job() -> R<()> {
        // Sequential-rollout shape: v1 holds slots 0..2; v2 must bind the
        // slot whose old allocation was just retired (slot 2), not the
        // lowest slot free in v2's own scope (slot 0, still live under v1).
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "slot-seq-bind";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 3, true)])?;
        let v1_task = task_id_for(&store, "db", job_id)?;
        let mut v1_allocs = Vec::new();
        for _ in 0..3 {
            v1_allocs.push(store.place_alloc("node-a", v1_task)?);
        }
        let slot2_handle = store
            .get_instance(&format!("{job_id}-default-2"))?
            .context("slot 2 instance")?
            .storage_handle
            .clone();

        let v2_config_id = submit_job(
            &store,
            job_id,
            vec![TaskConfig {
                config: json!({ "version": 2 }),
                ..stateful_slot_task("db", 3, true)
            }],
        )?;
        let v2_task = task_id_for_config(&store, "db", &v2_config_id)?;

        // Retire slot 2's old allocation, then place a v2 allocation: it
        // must re-bind slot 2's instance (same id and storage handle).
        store.remove_allocation_by_id(v1_allocs[2].id)?;
        let replacement = store.place_alloc("node-a", v2_task)?;
        assert_eq!(
            replacement.instance_id.as_deref(),
            Some(format!("{job_id}-default-2").as_str()),
            "replacement must bind the freed slot, not slot 0"
        );
        let rebound = store
            .get_instance(&format!("{job_id}-default-2"))?
            .context("slot 2 instance after rebind")?;
        assert_eq!(rebound.storage_handle, slot2_handle);

        // Fallback parity with the fresh-plan-then-remove flow: with no
        // job-wide-free slot left, per-config scoping still applies and the
        // next v2 placement takes the lowest slot free in v2's scope.
        let takeover = store.place_alloc("node-a", v2_task)?;
        assert_eq!(
            takeover.instance_id.as_deref(),
            Some(format!("{job_id}-default-0").as_str())
        );
        Ok(())
    }

    #[test]
    fn slot_indices_and_other_config_counts_round_trip() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "slot-helpers";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 2, false)])?;
        let v1_task = task_id_for(&store, "db", job_id)?;
        store.place_alloc("node-a", v1_task)?;
        store.place_alloc("node-a", v1_task)?;

        let indices = store.slot_indices_for_job_group(job_id, "default")?;
        assert_eq!(indices.len(), 2);
        assert_eq!(indices.get(&format!("{job_id}-default-0")), Some(&0));
        assert_eq!(indices.get(&format!("{job_id}-default-1")), Some(&1));
        assert!(store
            .slot_indices_for_job_group(job_id, "other-group")?
            .is_empty());

        // Both v1 allocations count as "other config" from v2's viewpoint,
        // and as zero from v1's own viewpoint.
        let v1_config_id = format!("{job_id}-1");
        let v2_config_id = submit_job(
            &store,
            job_id,
            vec![TaskConfig {
                config: json!({ "version": 2 }),
                ..stateful_slot_task("db", 2, false)
            }],
        )?;
        assert_eq!(
            store.count_slot_allocations_excluding_config(job_id, "default", &v2_config_id)?,
            2
        );
        assert_eq!(
            store.count_slot_allocations_excluding_config(job_id, "default", &v1_config_id)?,
            0
        );
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
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    plugin: None,
                    mode: None,
                    params: None,
                    path: "/var/lib/data".into(),
                    retain_on_scale_in: Some(false),
                    retain_on_stop: None,
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
    fn stop_instances_for_job_terminates_slot_rows_and_retains_handles() -> R<()> {
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

        // Mirror the job_stop eval: allocations are removed first, then the
        // instances are settled. Default retainOnStop (absent = true) keeps
        // every slot handle, like K8s `whenDeleted: Retain`.
        store.remove_allocs_for_job(job_id)?;
        assert_eq!(store.stop_instances_for_job(job_id)?, 2);
        let instances = store.list_instances(job_id)?;
        assert_eq!(instances.len(), 2);
        for (instance, handle) in instances.iter().zip(&handles) {
            assert_eq!(instance.state, InstanceState::Terminated);
            assert_eq!(instance.current_alloc_id, None);
            assert_eq!(&instance.storage_handle, handle);
        }

        // Idempotent.
        assert_eq!(store.stop_instances_for_job(job_id)?, 0);

        // Resubmitting the job re-materializes the SAME ordinal identities
        // with the SAME retained handles.
        let v2_config_id = submit_job(&store, job_id, vec![stateful_slot_task("db", 2, true)])?;
        let v2_task = task_id_for_config(&store, "db", &v2_config_id)?;
        store.place_alloc("node-a", v2_task)?;
        store.place_alloc("node-a", v2_task)?;
        let revived = store.list_instances(job_id)?;
        assert_eq!(revived.len(), 2);
        assert_eq!(revived[0].instance_id, "inst-stop-default-0");
        assert_eq!(revived[1].instance_id, "inst-stop-default-1");
        for (instance, handle) in revived.iter().zip(&handles) {
            assert_eq!(instance.state, InstanceState::Active);
            assert_eq!(
                &instance.storage_handle, handle,
                "re-run reattaches the retained handle"
            );
        }
        Ok(())
    }

    #[test]
    fn job_stop_drops_slot_handles_when_retain_on_stop_disabled() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "stop-noretain";
        let no_stop_retain_task = || TaskConfig {
            count: TaskCount::new(2),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    plugin: None,
                    mode: None,
                    params: None,
                    path: "/var/lib/data".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: Some(false),
                }),
            }),
            ..TaskConfig::new("db", "noop")
        };

        submit_job(&store, job_id, vec![no_stop_retain_task()])?;
        let task_id = task_id_for(&store, "db", job_id)?;
        store.place_alloc("node-a", task_id)?;
        store.place_alloc("node-a", task_id)?;
        let original_handles: Vec<Option<String>> = store
            .list_instances(job_id)?
            .into_iter()
            .map(|instance| instance.storage_handle)
            .collect();
        assert!(original_handles.iter().all(Option::is_some));

        // Job stop (the K8s `whenDeleted: Delete` analog): rows are retained
        // — identity comes back — but the data binding is dropped.
        store.remove_allocs_for_job(job_id)?;
        assert_eq!(store.stop_instances_for_job(job_id)?, 2);
        let instances = store.list_instances(job_id)?;
        assert_eq!(instances.len(), 2, "rows retained despite handle drop");
        for instance in &instances {
            assert_eq!(instance.state, InstanceState::Terminated);
            assert_eq!(instance.current_alloc_id, None);
            assert_eq!(
                instance.storage_handle, None,
                "retainOnStop: false drops the handle on job stop"
            );
        }

        // Idempotent.
        assert_eq!(store.stop_instances_for_job(job_id)?, 0);

        // Resubmit: the same ordinal ids come back, but with FRESH handles —
        // the identity survives the stop, the data binding does not.
        let v2_config_id = submit_job(&store, job_id, vec![no_stop_retain_task()])?;
        let v2_task = task_id_for_config(&store, "db", &v2_config_id)?;
        store.place_alloc("node-a", v2_task)?;
        store.place_alloc("node-a", v2_task)?;
        let revived = store.list_instances(job_id)?;
        assert_eq!(revived.len(), 2);
        assert_eq!(revived[0].instance_id, "stop-noretain-default-0");
        assert_eq!(revived[1].instance_id, "stop-noretain-default-1");
        for (instance, original) in revived.iter().zip(&original_handles) {
            assert_eq!(instance.state, InstanceState::Active);
            assert!(instance.storage_handle.is_some(), "fresh handle minted");
            assert_ne!(&instance.storage_handle, original);
        }
        Ok(())
    }

    #[test]
    fn job_stop_drops_handles_only_for_tasks_with_retain_on_stop_disabled() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "stop-mixed";
        // Three groups: a slot task that keeps the default (retain), a slot
        // task that opts out, and a keyed task to prove the slot-only knob
        // never touches keyed handles.
        let keep_task = TaskConfig {
            group: "keep".into(),
            ..stateful_slot_task("keep-db", 1, true)
        };
        let drop_task = TaskConfig {
            group: "drop".into(),
            count: TaskCount::new(1),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    plugin: None,
                    mode: None,
                    params: None,
                    path: "/var/lib/data".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: Some(false),
                }),
            }),
            ..TaskConfig::new("drop-db", "noop")
        };
        let keyed_task = TaskConfig {
            group: "sandboxes".into(),
            ..stateful_keyed_task("sandbox", true)
        };
        submit_job(&store, job_id, vec![keep_task, drop_task, keyed_task])?;
        store.create_keyed_instance(job_id, "sandboxes", "sandbox", Some("user-a"))?;
        store.place_alloc("node-a", task_id_for(&store, "keep-db", job_id)?)?;
        store.place_alloc("node-a", task_id_for(&store, "drop-db", job_id)?)?;
        store.place_alloc("node-a", task_id_for(&store, "sandbox", job_id)?)?;

        let keep_id = slot_instance_id(job_id, "keep", 0);
        let drop_id = slot_instance_id(job_id, "drop", 0);
        let keep_handle = store
            .get_instance(&keep_id)?
            .context("keep")?
            .storage_handle;
        let keyed_handle = store
            .get_instance("user-a")?
            .context("keyed")?
            .storage_handle;
        assert!(keep_handle.is_some());
        assert!(keyed_handle.is_some());

        store.remove_allocs_for_job(job_id)?;
        assert_eq!(store.stop_instances_for_job(job_id)?, 3);

        // Only the opted-out task's slot handle is dropped.
        let keep = store.get_instance(&keep_id)?.context("keep")?;
        assert_eq!(keep.state, InstanceState::Terminated);
        assert_eq!(keep.storage_handle, keep_handle, "default task retains");
        let dropped = store.get_instance(&drop_id)?.context("drop")?;
        assert_eq!(dropped.state, InstanceState::Terminated);
        assert_eq!(dropped.storage_handle, None, "opted-out task drops");
        // Keyed instances are paused with handles retained, regardless of any
        // slot task's retainOnStop.
        let keyed = store.get_instance("user-a")?.context("keyed")?;
        assert_eq!(keyed.state, InstanceState::Stopped);
        assert_eq!(keyed.storage_handle, keyed_handle, "keyed always retains");
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
                initial_instances: None,
                on_lost: None,
                storage: with_storage.then(|| crate::structs::StatefulStorageConfig {
                    plugin: None,
                    mode: None,
                    params: None,
                    path: "/workspace".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
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
    fn keyed_instance_cannot_hijack_another_jobs_slot_id() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;

        // Job A owns slot ids `job-a-default-{0,1,2}`.
        let job_a = "job-a";
        submit_job(&store, job_a, vec![stateful_slot_task("db", 3, true)])?;
        let slot_task = task_id_for(&store, "db", job_a)?;

        // Job B is a keyed sandbox job. A caller tries to create a keyed
        // instance whose id is exactly job A's slot-0 id.
        let job_b = "job-b";
        submit_job(&store, job_b, vec![stateful_keyed_task("sandbox", true)])?;
        let hijack_id = slot_instance_id(job_a, "default", 0); // "job-a-default-0"
        let created = store.create_keyed_instance(job_b, "default", "sandbox", Some(&hijack_id))?;

        // Job A's slot-0 placement must NOT adopt job B's row — it is a hard
        // error, because the slot id space is exclusive.
        let err = store
            .place_alloc("node-a", slot_task)
            .expect_err("slot placement must refuse to adopt a foreign row");
        assert!(
            err.to_string().contains("exclusive"),
            "error should explain slot ids are exclusive: {err}"
        );

        // Job B's row is untouched: still keyed, still owned by job B.
        let row = store.get_instance(&hijack_id)?.context("job B row")?;
        assert_eq!(row.job_id, job_b);
        assert_eq!(row.mode, InstanceMode::Keyed);
        assert_eq!(row.created_at, created.created_at);

        Ok(())
    }

    #[test]
    fn keyed_create_rejects_id_aliasing_same_job_slot_pattern() -> R<()> {
        let store = setup_store()?;
        // One job with both a slot task (slots 0,1) and a keyed task.
        let job_id = "mixed";
        submit_job(
            &store,
            job_id,
            vec![
                stateful_slot_task("db", 2, true),
                TaskConfig {
                    group: "sandboxes".into(),
                    ..stateful_keyed_task("sandbox", true)
                },
            ],
        )?;

        // A keyed id matching the slot pattern of the slot task is rejected.
        let colliding = slot_instance_id(job_id, "default", 1); // "mixed-default-1"
        let err = store
            .create_keyed_instance(job_id, "sandboxes", "sandbox", Some(&colliding))
            .expect_err("keyed id aliasing a slot id must be rejected");
        assert!(
            err.to_string().contains("slot-id pattern"),
            "error should mention the reserved slot-id pattern: {err}"
        );

        // A slot index beyond the pool (slot 2) is NOT a slot id, so it's fine.
        let ok_id = slot_instance_id(job_id, "default", 2); // "mixed-default-2"
        store.create_keyed_instance(job_id, "sandboxes", "sandbox", Some(&ok_id))?;
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
    fn job_stop_stops_keyed_instances_and_terminates_slot_instances() -> R<()> {
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
        let keyed_handle = store
            .get_instance("user-a")?
            .context("keyed row")?
            .storage_handle;
        assert!(keyed_handle.is_some());

        // Keyed rows are merely STOPPED (handle retained — a job stop is a
        // pause, revivable via start_keyed_instance); slot rows keep today's
        // terminated-with-handle behavior so a re-run re-materializes them.
        assert_eq!(store.stop_instances_for_job(job_id)?, 2);
        let keyed = store.get_instance("user-a")?.context("keyed row")?;
        assert_eq!(keyed.state, InstanceState::Stopped);
        assert_eq!(keyed.current_alloc_id, None);
        assert_eq!(
            keyed.storage_handle, keyed_handle,
            "keyed handles retained on job stop"
        );
        let slot = store
            .get_instance(&slot_instance_id(job_id, "default", 0))?
            .context("slot row")?;
        assert_eq!(slot.state, InstanceState::Terminated);
        assert!(slot.storage_handle.is_some(), "slot handles retained");

        // Idempotent: stopped/terminated rows are not touched again.
        assert_eq!(store.stop_instances_for_job(job_id)?, 0);
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

        // Stopped instances do not count as desired either: a stopped
        // instance gets no allocation until start_keyed_instance.
        store.stop_keyed_instance("b")?;
        assert_eq!(
            store.count_live_keyed_instances(job_id, "default", "sandbox")?,
            0
        );
        store.start_keyed_instance("b")?;
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

    #[test]
    fn stop_keyed_instance_pauses_alloc_retains_row_and_handle() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-pause";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;
        let task_id = task_id_for(&store, "sandbox", job_id)?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        let alloc = store.place_alloc("node-a", task_id)?;
        let handle = store
            .get_instance("user-a")?
            .context("instance")?
            .storage_handle;
        assert!(handle.is_some());

        // Stop: allocation removed through the normal stop path, state
        // stopped — NOT rescheduling (the release hook must not fire on a
        // stop) — and the row + storage handle retained.
        let outcome = store.stop_keyed_instance("user-a")?;
        assert_eq!(
            outcome,
            StopInstanceOutcome::Stopped {
                removed_allocations: 1
            }
        );
        assert!(
            store.get_allocation_by_id(alloc.id)?.is_none(),
            "the allocation is deleted through the normal stop path"
        );
        let stopped = store.get_instance("user-a")?.context("row retained")?;
        assert_eq!(stopped.state, InstanceState::Stopped);
        assert_eq!(stopped.current_alloc_id, None);
        assert_eq!(stopped.storage_handle, handle, "handle retained on stop");

        // The id is still owned: creating it again collides (stopped is not
        // destroyed).
        let err = store
            .create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))
            .expect_err("stopped ids still collide");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::Conflict { .. })
        ));

        // A stopped instance is never bound by placement: with only the
        // stopped instance, there is nothing to place.
        let err = store
            .place_alloc("node-a", task_id)
            .expect_err("stopped instances are not placement candidates");
        assert!(err.to_string().contains("no unbound keyed instance"));

        // Idempotent no-op on an already-stopped instance.
        assert_eq!(
            store.stop_keyed_instance("user-a")?,
            StopInstanceOutcome::AlreadyStopped
        );
        Ok(())
    }

    #[test]
    fn stop_keyed_instance_rejects_slot_terminated_and_missing() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-pause-reject";
        submit_job(
            &store,
            job_id,
            vec![
                stateful_keyed_task("sandbox", true),
                stateful_slot_task("db", 1, true),
            ],
        )?;
        let slot_alloc = store.place_alloc("node-a", task_id_for(&store, "db", job_id)?)?;

        // Slot instances are owned by the scheduler, not this API.
        let slot_id = slot_alloc.instance_id.context("slot bound")?;
        let err = store
            .stop_keyed_instance(&slot_id)
            .expect_err("slot rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::InvalidInput { .. })
        ));

        // Terminated tombstones can never be stopped.
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-gone"))?;
        store.destroy_keyed_instance("user-gone")?;
        let err = store
            .stop_keyed_instance("user-gone")
            .expect_err("terminated rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::InvalidInput { .. })
        ));

        // Unknown ids surface NotFound.
        let err = store
            .stop_keyed_instance("missing")
            .expect_err("missing rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::NotFound { .. })
        ));
        Ok(())
    }

    #[test]
    fn start_keyed_instance_revives_stopped_row_with_same_identity_and_handle() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-revive";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;
        let task_id = task_id_for(&store, "sandbox", job_id)?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        store.place_alloc("node-a", task_id)?;
        let handle = store
            .get_instance("user-a")?
            .context("instance")?
            .storage_handle;

        store.stop_keyed_instance("user-a")?;
        assert_eq!(
            store.start_keyed_instance("user-a")?,
            StartInstanceOutcome::Started
        );
        let pending = store.get_instance("user-a")?.context("instance")?;
        assert_eq!(pending.state, InstanceState::Pending);
        assert_eq!(pending.storage_handle, handle, "handle survives the cycle");

        // The next placement re-binds the SAME id and storage handle: the
        // revived workload is the same logical instance.
        let replacement = store.place_alloc("node-a", task_id)?;
        assert_eq!(replacement.instance_id.as_deref(), Some("user-a"));
        let active = store.get_instance("user-a")?.context("instance")?;
        assert_eq!(active.state, InstanceState::Active);
        assert_eq!(active.storage_handle, handle);

        // Idempotent no-op on a live instance, in every live state.
        assert_eq!(
            store.start_keyed_instance("user-a")?,
            StartInstanceOutcome::AlreadyRunning
        );
        Ok(())
    }

    #[test]
    fn start_keyed_instance_rejects_slot_terminated_and_missing() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-revive-reject";
        submit_job(
            &store,
            job_id,
            vec![
                stateful_keyed_task("sandbox", true),
                stateful_slot_task("db", 1, true),
            ],
        )?;
        let slot_alloc = store.place_alloc("node-a", task_id_for(&store, "db", job_id)?)?;

        let slot_id = slot_alloc.instance_id.context("slot bound")?;
        let err = store
            .start_keyed_instance(&slot_id)
            .expect_err("slot rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::InvalidInput { .. })
        ));

        // Terminated is forever: a destroyed id can never be started again.
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-gone"))?;
        store.destroy_keyed_instance("user-gone")?;
        let err = store
            .start_keyed_instance("user-gone")
            .expect_err("terminated rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::InvalidInput { .. })
        ));

        let err = store
            .start_keyed_instance("missing")
            .expect_err("missing rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::NotFound { .. })
        ));
        Ok(())
    }

    #[test]
    fn destroy_after_stop_terminates_and_drops_retained_handle() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-stop-destroy";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;
        let task_id = task_id_for(&store, "sandbox", job_id)?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        store.place_alloc("node-a", task_id)?;

        store.stop_keyed_instance("user-a")?;
        assert!(store
            .get_instance("user-a")?
            .context("stopped row")?
            .storage_handle
            .is_some());

        // Stop is a pause; destroy is forever — even from stopped, destroy
        // drops the retained handle and tombstones the id.
        let outcome = store.destroy_keyed_instance("user-a")?;
        assert_eq!(
            outcome,
            DestroyInstanceOutcome::Destroyed {
                removed_allocations: 0
            }
        );
        let tombstone = store.get_instance("user-a")?.context("tombstone")?;
        assert_eq!(tombstone.state, InstanceState::Terminated);
        assert_eq!(tombstone.storage_handle, None, "handle dropped on destroy");
        Ok(())
    }

    #[test]
    fn release_hook_leaves_stopped_instances_stopped() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-release-stop";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;
        let task_id = task_id_for(&store, "sandbox", job_id)?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        let alloc = store.place_alloc("node-a", task_id)?;

        // Simulate an allocation death racing a stop: the instance is
        // already marked stopped when its (stale) allocation is removed. The
        // release hook must NOT flip it back to rescheduling — that would
        // resurrect a deliberately paused instance.
        store.set_instance_state("user-a", InstanceState::Stopped)?;
        store.remove_allocation_by_id(alloc.id)?;
        let instance = store.get_instance("user-a")?.context("instance")?;
        assert_eq!(instance.state, InstanceState::Stopped);
        let handle = instance.storage_handle;
        assert!(handle.is_some(), "handle untouched by the release hook");
        Ok(())
    }

    #[test]
    fn create_keyed_instances_batch_is_all_or_nothing() -> R<()> {
        let store = setup_store()?;
        let job_id = "keyed-batch";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;

        // Explicit ids + minted extras, in request order.
        let created = store.create_keyed_instances(
            job_id,
            "default",
            "sandbox",
            &["a".to_string(), "b".to_string()],
            2,
        )?;
        assert_eq!(created.len(), 4);
        assert_eq!(&created[..2], &["a".to_string(), "b".to_string()]);
        assert!(created[2..].iter().all(|id| id.starts_with("sandbox-")));
        for id in &created {
            let instance = store.get_instance(id)?.context("created row")?;
            assert_eq!(instance.state, InstanceState::Pending);
            assert_eq!(instance.mode, InstanceMode::Keyed);
        }

        // Any collision — here "b", including with a tombstone — fails the
        // whole batch and creates NOTHING.
        store.destroy_keyed_instance("b")?;
        let err = store
            .create_keyed_instances(
                job_id,
                "default",
                "sandbox",
                &[
                    "fresh-1".to_string(),
                    "b".to_string(),
                    "fresh-2".to_string(),
                ],
                1,
            )
            .expect_err("tombstone collision aborts the batch");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::Conflict { .. })
        ));
        assert!(err.to_string().contains("b"), "offending id named: {err}");
        assert!(store.get_instance("fresh-1")?.is_none(), "nothing created");
        assert!(store.get_instance("fresh-2")?.is_none(), "nothing created");
        assert_eq!(store.list_instances(job_id)?.len(), 4);

        // Duplicates WITHIN the request also abort atomically.
        let err = store
            .create_keyed_instances(
                job_id,
                "default",
                "sandbox",
                &["dupe".to_string(), "dupe".to_string()],
                0,
            )
            .expect_err("in-request duplicate aborts the batch");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::Conflict { .. })
        ));
        assert!(store.get_instance("dupe")?.is_none(), "nothing created");

        // Count-only mints N distinct ids.
        let minted = store.create_keyed_instances(job_id, "default", "sandbox", &[], 3)?;
        assert_eq!(minted.len(), 3);
        let unique: HashSet<&String> = minted.iter().collect();
        assert_eq!(unique.len(), 3, "minted ids are distinct");
        Ok(())
    }

    #[test]
    fn seed_keyed_instances_creates_new_and_skips_any_existing() -> R<()> {
        let store = setup_store()?;
        let job_id = "keyed-seed";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;

        let ids = vec!["seed-a".to_string(), "seed-b".to_string()];
        let created = store.seed_keyed_instances(job_id, "default", "sandbox", &ids)?;
        assert_eq!(created, ids);
        assert_eq!(
            store.count_live_keyed_instances(job_id, "default", "sandbox")?,
            2
        );

        // Re-seeding is a silent no-op (resubmits are idempotent).
        let created = store.seed_keyed_instances(job_id, "default", "sandbox", &ids)?;
        assert!(created.is_empty());

        // A destroyed id is skipped, NOT resurrected: the seed list never
        // overrides a deliberate destroy.
        store.destroy_keyed_instance("seed-a")?;
        let created = store.seed_keyed_instances(job_id, "default", "sandbox", &ids)?;
        assert!(created.is_empty());
        assert_eq!(
            store
                .get_instance("seed-a")?
                .context("tombstone retained")?
                .state,
            InstanceState::Terminated
        );

        // A stopped id is skipped too: it stays stopped until StartInstance.
        store.stop_keyed_instance("seed-b")?;
        let created = store.seed_keyed_instances(job_id, "default", "sandbox", &ids)?;
        assert!(created.is_empty());
        assert_eq!(
            store
                .get_instance("seed-b")?
                .context("stopped retained")?
                .state,
            InstanceState::Stopped
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

    fn service_task(name: &str) -> TaskConfig {
        let mut task = TaskConfig::new(name, "noop");
        task.network = Some(crate::structs::NetworkConfig {
            ports: vec![crate::structs::PortConfig {
                name: "http".into(),
                static_port: None,
                to: Some(8080),
            }],
            ..Default::default()
        });
        task.service = Some(
            serde_json::from_value(json!({ "port": "http", "tags": ["v1"] }))
                .expect("service config"),
        );
        task
    }

    fn submit_service_job(store: &Store, name: &str, tasks: Vec<TaskConfig>) -> R<()> {
        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks,
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
        Ok(())
    }

    #[test]
    fn port_assignment_gives_distinct_dynamic_ports() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        submit_service_job(&store, "svc-ports", vec![service_task("web")])?;
        let task_id = task_id_for(&store, "web", "svc-ports")?;

        let first = store.place_alloc("node-a", task_id)?;
        let second = store.place_alloc("node-a", task_id)?;

        let first_port = first
            .network
            .as_ref()
            .and_then(|network| network.port("http"))
            .map(|port| port.host_port)
            .context("first alloc missing http port")?;
        let second_port = second
            .network
            .as_ref()
            .and_then(|network| network.port("http"))
            .map(|port| port.host_port)
            .context("second alloc missing http port")?;

        assert_ne!(first_port, second_port);
        assert!(first_port >= crate::structs::DYNAMIC_PORT_MIN);
        assert!(second_port <= crate::structs::DYNAMIC_PORT_MAX);
        Ok(())
    }

    #[test]
    fn static_port_conflict_fails_placement_with_clear_error() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let mut task = TaskConfig::new("redis", "noop");
        task.service =
            Some(serde_json::from_value(json!({ "port": 6379 })).expect("service config"));
        submit_service_job(&store, "svc-conflict", vec![task])?;
        let task_id = task_id_for(&store, "redis", "svc-conflict")?;

        store.place_alloc("node-a", task_id)?;
        let err = store.place_alloc("node-a", task_id).unwrap_err();
        let details = format!("{err:#}");
        assert!(details.contains("static port 6379"), "got: {details}");
        assert!(details.contains("node-a"), "got: {details}");
        Ok(())
    }

    #[test]
    fn service_endpoints_projection_gates_on_health_and_node() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        submit_service_job(&store, "svc-proj", vec![service_task("web")])?;
        let task_id = task_id_for(&store, "web", "svc-proj")?;
        let alloc = store.place_alloc("node-a", task_id)?;

        // No health snapshot yet: endpoint exists but is not healthy.
        let endpoints = store.service_endpoints(Some("web"))?;
        assert_eq!(endpoints.len(), 1);
        assert!(!endpoints[0].healthy);
        assert!(endpoints[0].port.is_some());

        // Healthy snapshot: endpoint becomes healthy.
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[AllocationHealth {
                allocation_id: alloc.id,
                healthy: true,
                status: "healthy".into(),
                artifact_states: None,
            }],
        )?;
        let endpoints = store.service_endpoints(Some("web"))?;
        assert!(endpoints[0].healthy);

        // Unhealthy snapshot: gated out again.
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[AllocationHealth {
                allocation_id: alloc.id,
                healthy: false,
                status: "tcp connect refused".into(),
                artifact_states: None,
            }],
        )?;
        let endpoints = store.service_endpoints(Some("web"))?;
        assert!(!endpoints[0].healthy);
        assert_eq!(endpoints[0].health_status, "tcp connect refused");

        // Healthy again but node lost: still not healthy (liveness lease).
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[AllocationHealth {
                allocation_id: alloc.id,
                healthy: true,
                status: "healthy".into(),
                artifact_states: None,
            }],
        )?;
        store.mark_node_lost("node-a")?;
        let endpoints = store.service_endpoints(Some("web"))?;
        assert!(!endpoints[0].healthy);
        assert!(endpoints[0].health_status.contains("node node-a is lost"));

        // Removing the allocation removes the endpoint entirely.
        store.remove_allocation_by_id(alloc.id)?;
        let endpoints = store.service_endpoints(Some("web"))?;
        assert!(endpoints.is_empty());

        // But the declared service is still listed (0 endpoints).
        let declared = store.declared_services()?;
        assert!(declared.iter().any(|entry| entry.service == "web"));
        Ok(())
    }

    #[test]
    fn service_port_sugar_persists_explicit_network() -> R<()> {
        let store = setup_store()?;
        let mut task = TaskConfig::new("redis", "noop");
        task.service =
            Some(serde_json::from_value(json!({ "port": 6379 })).expect("service config"));
        submit_service_job(&store, "svc-sugar", vec![task])?;

        let job_config_id = store.get_job_config("svc-sugar")?.id();
        let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
        let (_, stored) = tasks.first().context("missing stored task")?;
        let network = stored
            .network
            .as_ref()
            .context("network not materialized")?;
        assert_eq!(network.ports.len(), 1);
        assert_eq!(network.ports[0].name, "redis");
        assert_eq!(network.ports[0].static_port, Some(6379));
        Ok(())
    }
}
