use crate::constraints::{ConstraintSet, DistinctTracker};
use crate::prelude::*;
use crate::resources::ResourcePolicy;
use crate::storage::errors::StorageError;
use crate::storage::migrations::get_migrations;
use crate::storage::queries::{
    GET_ALLOCATION_BY_ID, GET_ALLOCS, GET_ALLOCS_FOR_JOB, GET_ALLOCS_FOR_JOB_CONFIG,
    GET_ALLOCS_FOR_NODE, GET_EVALS, GET_JOB, GET_JOBS, GET_JOB_CONFIG, GET_JOB_CONFIG_BY_ID,
    GET_JOB_UPDATE_FOR_CONFIG, GET_MANAGER_CONFIGS, GET_NEXT_EVAL, GET_NODE, GET_NODES,
    GET_TASK_FOR_JOB_CONIFG, GET_WORKER_TYPE, INSERT_ALLOCATION, INSERT_EVAL, INSERT_JOB,
    INSERT_JOB_CONFIG, INSERT_JOB_CONFIG_SUBMISSION, INSERT_JOB_UPDATE, INSERT_NODE,
    INSERT_SUBMISSION, INSERT_TASK, INSERT_WORKER_TYPE, NODE_ATTRS, UPSERT_MANAGER_CONFIG,
};
use crate::structs::{
    Allocation, Eval, Job, JobConfig, LedgerEntry, ManagerConfigFull, ManagerNodeConfig, Node,
    NodeMsg, Submission, TaskConfig, WorkerConfig,
};

use rand::{seq::SliceRandom, thread_rng};
use rusqlite::{
    named_params, params_from_iter, Connection, Error as RusqliteError, OpenFlags,
    OptionalExtension, Row, Transaction,
};
use rusqlite_migration::Migrations;
use serde_json::{to_string, to_string_pretty};
use serde_rusqlite::to_params_named;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{MutexGuard, RwLock};
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::OffsetDateTime;

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
    distinct: Option<DistinctTracker>,
}

#[derive(Debug, Clone)]
pub struct Store {
    pub write_conn: MemSafeConn,
    pub read_conns: Arc<Vec<MemSafeConn>>,
    pub read_index: Arc<AtomicUsize>,
    pub scheduling_policy: String,
    pub default_eval_priority: i64,
    pub scheduler_settings: Option<Value>,
    pub resource_policy: ResourcePolicy,
    read_model: Arc<RwLock<SchedulingReadModel>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobSubmitOutcome {
    Created { job_config_id: String },
    Updated { job_config_id: String },
    Unchanged { job_config_id: String },
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

// NOTE: This will become a general interface for all storage backends
impl Store {
    #[instrument(err, skip_all)]
    pub fn start_storage(config: &ManagerConfigFull) -> R<Store> {
        let db_path = Store::db_path(&config.store_path)?;
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
            scheduler_settings: config.scheduler_settings.clone(),
            resource_policy,
            read_model: Arc::new(RwLock::new(SchedulingReadModel::default())),
        };

        store.refresh_read_model()?;

        Ok(store)
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
        Ok(Arc::new(Mutex::new(conn)))
    }

    #[instrument(err, skip_all)]
    fn create_readonly_conn(db_path: &Path) -> R<MemSafeConn> {
        let final_path = db_path.to_str().context("Error parsing store path")?;
        let conn = Connection::open_with_flags(final_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        conn.pragma_update(None, "query_only", "ON")?;
        Ok(Arc::new(Mutex::new(conn)))
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
        let mut conn = Self::lock_conn(conn)?;
        info!("Pragma update");
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        info!("Running migrations");
        migrations.to_latest(&mut conn)?;

        Ok(migrations)
    }

    fn lock_conn(conn: &MemSafeConn) -> R<MutexGuard<'_, Connection>> {
        conn.lock().map_err(|_| anyhow!(CONN_LOCK_ERROR))
    }

    fn write_connection(&self) -> R<MutexGuard<'_, Connection>> {
        Self::lock_conn(&self.write_conn)
    }

    fn read_connection(&self) -> R<MutexGuard<'_, Connection>> {
        let pool = self.read_conns.as_slice();
        let index = self.read_index.fetch_add(1, Ordering::Relaxed);
        let conn = pool.get(index % pool.len()).unwrap_or(&self.write_conn);
        Self::lock_conn(conn)
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
            SELECT id, name, kind, count, config, resources, constraints, affinities, job_config_id
            FROM task_config;
            "#,
        )?;
        let task_rows = task_stmt.query_map([], |row| {
            let id = row.get(0)?;
            let task = TaskConfig {
                name: row.get(1)?,
                kind: row.get(2)?,
                count: row.get::<_, i64>(3)? as u32,
                config: row.get(4)?,
                resources: row.get(5)?,
                constraints: row.get(6)?,
                affinities: row.get(7)?,
            };
            let job_config_id: String = row.get(8)?;
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

    fn collect_rows<T, I>(iter: I) -> R<Vec<T>>
    where
        I: Iterator<Item = Result<T, rusqlite::Error>>,
    {
        let values: Result<Vec<_>, rusqlite::Error> = iter.collect();
        let values = values.map_err(anyhow::Error::new)?;
        Ok(values)
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

        Ok(eval)
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

    fn task_config_from_row(&self, row: &Row<'_>) -> Result<TaskConfig, rusqlite::Error> {
        let task_config = TaskConfig {
            name: row.get(1)?,
            kind: row.get(2)?,
            count: row.get(3)?,
            config: row.get(4)?,
            resources: row.get(5)?,
            constraints: row.get(6)?,
            affinities: row.get(7)?,
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
        };

        Ok(allocation)
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

        let conn = self.write_connection()?;
        conn.execute("DELETE FROM allocation WHERE node_id = ?;", [node_id])?;

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
        let sql = "SELECT id, last_heartbeat FROM node";
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

        let conn = self.write_connection()?;
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let sql = format!(
            "UPDATE node SET status = 'lost', availability = 'unavailable' WHERE id IN ({})",
            placeholders
        );

        let mut stmt = conn.prepare(&sql)?;
        stmt.execute(params_from_iter(ids))?;

        self.cache_nodes_lost(ids)?;

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

    pub fn get_job_config_optional(&self, name: &str) -> R<Option<JobConfig>> {
        let conn = self.read_connection()?;
        let maybe_job_config: Option<(JobConfig, String)> = conn
            .query_row(&GET_JOB_CONFIG, [name], |row| {
                let tasks_to_replace = vec![];
                let job_config_id = row.get(0)?;
                let jc = JobConfig {
                    name: row.get(1)?,
                    tasks: tasks_to_replace,
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
                let jc = JobConfig {
                    name: row.get(1)?,
                    tasks: tasks_to_replace,
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
        let config_params = named_params! {
            ":id": format!("{}-{}", config.name, next_version.to_string()),
            ":name": config.name,
            ":type": "default",
            ":meta": "{}",
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
                        version: Some(row.get(4)?),
                        tasks,
                    },
                    row.get(0)?,
                ))
            })?;

        for t in &config.tasks {
            let task_params = named_params! {
                ":name": t.name,
                ":kind": t.kind,
                ":count": t.count,
                ":config": t.config,
                ":resources": t.resources,
                ":constraints": t.constraints,
                ":affinities": t.affinities,
                ":job_id": job.id,
                ":job_config_id": job_config_id,
            };

            tx.execute(&INSERT_TASK, task_params)?;
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
        tx.commit()?;

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
            let task = TaskConfig {
                name: row.get(1)?,
                kind: row.get(2)?,
                count: row.get::<_, i64>(3)? as u32,
                config: row.get(4)?,
                resources: row.get(5)?,
                constraints: row.get(6)?,
                affinities: row.get(7)?,
            };

            Ok((id, task))
        })?;

        Self::collect_rows(rows)
    }

    //
    // ALLOCS
    //

    #[instrument(err, skip_all)]
    pub fn place_alloc(&self, node_id: &str, task_id: i64) -> R<Allocation> {
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        let allocation = self.insert_allocation(&tx, node_id, task_id)?;
        tx.commit()?;

        self.cache_add_allocations(std::slice::from_ref(&allocation))?;

        Ok(allocation)
    }

    fn insert_allocation(
        &self,
        tx: &Transaction<'_>,
        node_id: &str,
        task_id: i64,
    ) -> R<Allocation> {
        let (job_config_id, resources): (String, Value) = tx.query_row(
            "SELECT job_config_id, resources FROM task_config WHERE id = ?;",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        let params = named_params! {
            ":status": "placed",
            ":resources": resources,
            ":node_id": node_id,
            ":job_config_id": job_config_id,
            ":task_config_id": task_id,
        };

        let allocation = tx.query_row(&INSERT_ALLOCATION, params, |row| {
            self.allocation_from_row(row)
        })?;

        Ok(allocation)
    }

    fn load_allocation_constraints(
        &self,
        tx: &Transaction<'_>,
        task_id: i64,
    ) -> R<AllocationConstraintState> {
        let constraints: Value = tx.query_row(
            "SELECT constraints FROM task_config WHERE id = ?;",
            [task_id],
            |row| row.get(0),
        )?;
        let constraint_set = ConstraintSet::new(&constraints)
            .with_context(|| format!("Failed to parse constraints for task {task_id}"))?;
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
            distinct,
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
    pub fn remove_allocation_by_id(&self, allocation_id: i64) -> R<Option<Allocation>> {
        let allocation = self.get_allocation_by_id(allocation_id)?;
        let Some(allocation) = allocation else {
            return Ok(None);
        };

        let conn = self.write_connection()?;
        conn.execute("DELETE FROM allocation WHERE id = ?;", [allocation_id])?;

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
        let conn = self.write_connection()?;
        let sql = r#"
        DELETE FROM allocation
        WHERE job_config_id IN (
            SELECT id FROM job_config WHERE job_id = :job_id
        );
        "#;

        let params = named_params! { ":job_id": job_id };

        conn.execute(sql, params)?;

        self.cache_remove_allocations(&allocations)?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn remove_allocs_for_job_config(&self, job_config_id: &str) -> R<Vec<Allocation>> {
        let allocations = self.allocs_for_job_config(job_config_id)?;
        if allocations.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self.write_connection()?;
        conn.execute(
            "DELETE FROM allocation WHERE job_config_id = ?;",
            [job_config_id],
        )?;

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

        let task_instances = self.get_task_instances_for_job_config(job_config_id)?;
        if task_instances.is_empty() {
            return Ok(());
        }

        let mut rng = thread_rng();

        for (task_id, desired_count) in task_instances {
            let current = self.count_allocations_for_task(task_id)?;

            for _ in current..desired_count {
                let node = nodes
                    .choose(&mut rng)
                    .context("No nodes available to place allocations")?;
                self.place_alloc(&node.id, task_id)?;
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
        let eval = conn.query_row(&GET_NEXT_EVAL, [], |row| self.eval_from_row(row));

        match eval {
            Ok(eval) => Ok(Some(eval)),
            Err(RusqliteError::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(err, skip_all)]
    pub fn claim_eval(&self) -> R<Option<Eval>> {
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;

        let eval_id: Option<i64> = tx
            .query_row(
                "SELECT id FROM eval WHERE status = 'pending' ORDER BY priority DESC, id ASC LIMIT 1;",
                [],
                |row| row.get(0),
            )
            .optional()?;

        let Some(eval_id) = eval_id else {
            tx.commit()?;
            return Ok(None);
        };

        let mut eval = tx.query_row(
            "UPDATE eval SET status = 'in_flight' WHERE id = :id RETURNING id, status, type, job_id, job_config_id, node_id, priority;",
            named_params! {":id": eval_id},
            |row| self.eval_from_row(row),
        )?;

        let eval_id = eval.id.clone();

        if !eval.job_id.trim().is_empty() {
            tx.execute(
                "DELETE FROM eval WHERE status = 'pending' AND job_id = :job_id AND id != :id;",
                named_params! {":job_id": eval.job_id.clone(), ":id": eval_id.clone()},
            )?;
        }

        if !eval.node_name.trim().is_empty() {
            tx.execute(
                "DELETE FROM eval WHERE status = 'pending' AND node_id = :node_id AND id != :id;",
                named_params! {":node_id": eval.node_name.clone(), ":id": eval_id.clone()},
            )?;
        }

        tx.commit()?;
        eval.status = "in_flight".to_string();

        Ok(Some(eval))
    }

    #[instrument(err, skip_all)]
    pub fn complete_eval(&self, eval_id: &str, succeeded: bool) -> R<()> {
        let conn: MutexGuard<'_, Connection> = self.write_connection()?;

        if succeeded {
            conn.execute("DELETE FROM eval WHERE id = ?;", [eval_id])?;
        } else {
            conn.execute("UPDATE eval SET status = 'failed' WHERE id = ?;", [eval_id])?;
        }

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

        let params = named_params! {
            ":id": None::<i64>,
            ":status": "pending",
            ":type": kind,
            ":job_id": job_id_value,
            ":job_config_id": job_config_id_value,
            ":node_id": node_id_value,
            ":priority": priority.unwrap_or(self.default_eval_priority),
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

        Ok(eval)
    }
}

fn task_schedule_equivalent(left: &TaskConfig, right: &TaskConfig) -> bool {
    left.count == right.count
        && left.resources == right.resources
        && left.constraints == right.constraints
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
    fn read_model_tracks_allocations_add_remove() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "ready"))?;

        let job = JobConfig {
            name: "cache-allocs".into(),
            tasks: vec![TaskConfig {
                name: "worker".into(),
                count: 1,
                kind: "noop".into(),
                config: json!({}),
                resources: json!({}),
                constraints: json!({}),
                affinities: json!({}),
            }],
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
            name: "cache-tasks".into(),
            tasks: vec![TaskConfig {
                name: "alpha".into(),
                count: 1,
                kind: "noop".into(),
                config: json!({}),
                resources: json!({}),
                constraints: json!({}),
                affinities: json!({}),
            }],
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
            name: job.name.clone(),
            tasks: vec![TaskConfig {
                name: "beta".into(),
                count: 2,
                kind: "noop".into(),
                config: json!({}),
                resources: json!({}),
                constraints: json!({}),
                affinities: json!({}),
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
        assert_eq!(tasks[0].1.count, 2);

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
            name: "task-1".to_string(),
            kind: "kind-1".to_string(),
            count: 1,
            config: json!({ "message": "hello-1" }),
            resources: json!({ "memory": 256 }),
            constraints: json!({}),
            affinities: json!({}),
        };

        let task_two = TaskConfig {
            name: "task-2".to_string(),
            kind: "kind-2".to_string(),
            count: 2,
            config: json!({ "message": "hi-2" }),
            resources: json!({ "memory": 512 }),
            constraints: json!({}),
            affinities: json!({}),
        };

        let task_configs = vec![task_one.clone(), task_two.clone()];
        let job_name = "test".to_string();
        let job_config = JobConfig {
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
            name: "task-1".to_string(),
            kind: "kind-1".to_string(),
            count: 1,
            config: json!({ "message": "hello-1" }),
            resources: json!({ "memory": 256 }),
            constraints: json!({}),
            affinities: json!({}),
        };

        let job_name = "rollback-job".to_string();
        let job_config = JobConfig {
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
            name: "orig-task".to_string(),
            kind: "kind-1".to_string(),
            count: 1,
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            constraints: json!({}),
            affinities: json!({}),
        };

        let orig_task_configs = vec![orig_task.clone()];
        let job_config = JobConfig {
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
            name: "extra-task".to_string(),
            kind: "kind-extra".to_string(),
            count: 1,
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            constraints: json!({}),
            affinities: json!({}),
        };
        let new_task_configs = vec![new_task.clone(), extra_task.clone()];

        let job_config_with_extra_task = JobConfig {
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
            name: "dupe-task".to_string(),
            kind: "kind".to_string(),
            count: 1,
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            constraints: json!({}),
            affinities: json!({}),
        };

        let orig_task_configs = vec![orig_task.clone()];
        let job_config = JobConfig {
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
            name: "job-one-task-1".to_string(),
            kind: "driver-a".to_string(),
            count: 1,
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            constraints: json!({}),
            affinities: json!({}),
        };
        let task_two = TaskConfig {
            name: "job-one-task-2".to_string(),
            kind: "driver-b".to_string(),
            count: 1,
            config: json!({ "message": "world" }),
            resources: json!({ "memory": 512 }),
            constraints: json!({}),
            affinities: json!({}),
        };
        let job_one_config = JobConfig {
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
            name: "job-two-task-1".to_string(),
            kind: "driver-c".to_string(),
            count: 1,
            config: json!({ "message": "hola" }),
            resources: json!({ "memory": 128 }),
            constraints: json!({}),
            affinities: json!({}),
        };
        let job_two_config = JobConfig {
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

        let tasks = vec![TaskConfig {
            name: "task-1".to_string(),
            kind: "kind-1".to_string(),
            count: 1,
            config: json!({}),
            resources: json!({}),
            constraints: json!({}),
            affinities: json!({}),
        }];

        let job_name = "solo-job".to_string();
        let job_config = JobConfig {
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

        let tasks = vec![TaskConfig {
            name: "task-shared".to_string(),
            kind: "kind".to_string(),
            count: 1,
            config: json!({}),
            resources: json!({}),
            constraints: json!({}),
            affinities: json!({}),
        }];

        let job_one_config = JobConfig {
            name: "job-one".into(),
            tasks: tasks.clone(),
            version: None,
        };

        let job_two_config = JobConfig {
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
