use crate::prelude::*;

use crate::structs::{
    Job, JobConfig, ManagerConfigFull, Node, NodeMsg, Submission, TaskConfig, WorkerConfig,
};

use rusqlite::{named_params, Connection, Row};
use rusqlite_migration::{Migrations, M};
use serde_json::to_string;
use serde_rusqlite::to_params_named;
use time::macros::format_description;
use time::OffsetDateTime;

// warning incorrectly
#[allow(dead_code)]
pub enum Msg {
    Content(String),
}

pub type MemSafeConn = Arc<Mutex<Connection>>;

#[derive(Debug)]
pub struct Store {
    pub conn: MemSafeConn,
}

pub const CONN_LOCK_ERROR: &str = "Error locking database connection";

// NOTE: This will become a general interface for all storage backends
impl Store {
    #[instrument(err, skip_all)]
    pub fn start_storage(config: &ManagerConfigFull) -> R<Store> {
        let conn = Store::create_memsafe_conn(&config.store_path)?;
        let _migrations = Store::create_initial_tables(&Arc::clone(&conn))?;

        let store = Store { conn };

        Ok(store)
    }

    #[instrument(err, skip_all)]
    fn create_memsafe_conn(store_path: &str) -> R<MemSafeConn> {
        // let conn = Connection::open_in_memory()?;

        // Using the disk version for now to simulate logs
        // Would eventually split this into a cluster that shares
        // logs via raft before committing to an in-memory sqlite db

        std::fs::create_dir_all(store_path)?;
        let tmp = std::path::Path::new(store_path).join("store.sqlite");
        let final_path = tmp.to_str().ok_or("Error parsing store path")?;
        let conn = Connection::open(final_path)?;
        Ok(Arc::new(Mutex::new(conn)))
    }

    pub fn get_migrations() -> Migrations<'static> {
        Migrations::new(vec![
            M::up(
                "CREATE TABLE IF NOT EXISTS node (
                id STRING PRIMARY KEY,
                pool STRING NOT NULL,
                status STRING NOT NULL,
                availability STRING NOT NULL,
                config_attrs  JSONB NOT NULL,
                fingerprint_attrs  JSONB NOT NULL,
                dynamic_attrs JSONB NOT NULL,
                health_checks JSONB NOT NULL,
                last_heartbeat TIMESTAMP NOT NULL
            )",
            )
            .down("DROP TABLE node;"),
            M::up(
                "CREATE TABLE IF NOT EXISTS worker_type (
                name STRING PRIMARY KEY,
                pool STRING NOT NULL,
                additional_node_checks Text NOT NULL,
                attributes JSONB NOT NULL,
                name_prefix STRING
            )",
            )
            .down("DROP TABLE worker_type;"),
            M::up(
                "CREATE TABLE IF NOT EXISTS job (
                id STRING PRIMARY KEY,
                name STRING NOT NULL,
                status STRING NOT NULL,
                created_at TIMESTAMP NOT NULL
            )",
            )
            .down("DROP TABLE job;"),
            M::up(
                "CREATE TABLE IF NOT EXISTS job_config (
                id STRING PRIMARY KEY,
                name STRING NOT NULL,
                type STRING NOT NULL,
                meta JSONB NOT NULL,
                version INTEGER NOT NULL,
                created_at TIMESTAMP NOT NULL,
                job_id STRING NOT NULL,
                FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE
            )",
            )
            .down("DROP TABLE job_config;"),
            M::up(
                "CREATE TABLE IF NOT EXISTS submission (
                id STRING PRIMARY KEY,
                filename STRING NOT NULL,
                text TEXT NOT NULL,
                job_config_id STRING NOT NULL,
                is_primary INTEGER NOT NULL,
                FOREIGN KEY(job_config_id) REFERENCES job_config(id) ON DELETE CASCADE
            )",
            )
            .down("DROP TABLE submission;"),
            M::up(
                "CREATE TABLE IF NOT EXISTS task_config (
                id STRING PRIMARY KEY,
                count INTEGER NOT NULL,
                name STRING NOT NULL,
                driver STRING NOT NULL,
                config JSONB NOT NULL,
                resources JSONB NOT NULL,
                constraints JSONB NOT NULL,
                job_id STRING NOT NULL,
                job_config_id STRING NOT NULL,
                FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE,
                FOREIGN KEY(job_config_id) REFERENCES job_config(id) ON DELETE CASCADE
            )",
            )
            .down("DROP TABLE task_config;"),
            M::up(
                "CREATE TABLE IF NOT EXISTS job_update (
                id STRING PRIMARY KEY,
                status STRING NOT NULL,
                job_id STRING NOT NULL,
                to_job_config_id STRING NOT NULL,
                from_job_config_id STRING,
                FOREIGN KEY(job_id) REFERENCES job(id) ON DELETE CASCADE,
                FOREIGN KEY(to_job_config_id) REFERENCES job_config(id) ON DELETE CASCADE,
                FOREIGN KEY(from_job_config_id) REFERENCES job_config(id) ON DELETE CASCADE
            )",
            )
            .down("DROP TABLE job_update;"),
        ])
    }

    pub fn clear_all_tables(&self) -> R<()> {
        let migrations = Store::get_migrations();
        let mut conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        migrations.to_version(&mut conn, 0)?;
        migrations.to_latest(&mut conn)?;

        Ok(())
    }

    #[instrument(skip_all)]
    fn create_initial_tables(conn: &MemSafeConn) -> R<Migrations<'_>> {
        let migrations = Store::get_migrations();
        let mut conn = conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        info!("Pragma update");
        conn.pragma_update(None, "journal_mode", "WAL")?;
        info!("Running migrations");
        migrations.to_latest(&mut conn)?;

        Ok(migrations)
    }

    //
    // NODES
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

    #[instrument(err, skip_all)]
    pub fn get_node(&self, id: &str) -> R<Node> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;

        let sql = r#"
        SELECT id, pool, status, availability, config_attrs, fingerprint_attrs, dynamic_attrs, health_checks, last_heartbeat
        FROM node
        WHERE id = ?;
"#;
        let node = conn.query_row(sql, [id], |row| self.node_from_row(row))?;

        Ok(node)
    }

    #[instrument(err, skip_all)]
    pub fn get_nodes(&self) -> R<Vec<Node>> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        let sql = r#"
SELECT id, pool, status, availability, config_attrs, fingerprint_attrs, dynamic_attrs, health_checks, last_heartbeat
FROM node;
"#;

        let mut stmt = conn.prepare(sql)?;
        let node_iter = stmt.query_map([], |row| self.node_from_row(row))?;
        let mut nodes = Vec::new();
        for node in node_iter {
            let n = node?;
            nodes.push(n);
        }

        Ok(nodes)
    }

    #[instrument(err, skip_all)]
    pub fn get_node_ids_and_heartbeats(&self) -> R<Vec<(String, String)>> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        let mut stmt = conn.prepare("SELECT id, last_heartbeat FROM node")?;
        let node_iter = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;

        let mut nodes_id_and_hb = Vec::new();
        for node in node_iter {
            let h = node?;
            nodes_id_and_hb.push(h);
        }

        Ok(nodes_id_and_hb)
    }

    #[instrument(err, skip_all)]
    pub fn mark_nodes_as_lost(&self, ids: &[String]) -> R<()> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        let sql = r#"
        UPDATE node
        SET status = 'lost'
        WHERE id IN (?1);
"#;
        let joined = ids.join(",");
        conn.execute(sql, [joined])?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn insert_node(&self, node: NodeMsg) -> R<Node> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        let sql = r#"
        INSERT INTO node (id, pool, status, availability, config_attrs, fingerprint_attrs, dynamic_attrs, health_checks, last_heartbeat)
        VALUES (:id, :pool, :status, :availability, :config_attrs, :fingerprint_attrs, :dynamic_attrs, :health_checks, :last_heartbeat)
        ON CONFLICT(id) DO UPDATE SET pool = :pool, status = :status, availability = :availability, config_attrs = :config_attrs, fingerprint_attrs = :fingerprint_attrs, dynamic_attrs = :dynamic_attrs, health_checks = :health_checks, last_heartbeat = :last_heartbeat
        RETURNING id, pool, status, availability, config_attrs, fingerprint_attrs, dynamic_attrs, health_checks, last_heartbeat;
"#;

        // TODO:
        // Its not ideal that I am using NodeMsg here
        // I get an error about serializing Map, and the simplest
        // way around it is to just use the node message when using
        // to_params_named, as its already a string
        let node = conn.query_row(sql, to_params_named(node)?.to_slice().as_slice(), |row| {
            self.node_from_row(row)
        })?;

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
        let res_as_string = to_string(&res).map_err(|e| e.to_string())?;

        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        let sql = r#"
        UPDATE node
        SET dynamic_attrs = :dynamic_attrs
        WHERE id IN (:id)
        RETURNING id, pool, status, availability, config_attrs, fingerprint_attrs, dynamic_attrs, health_checks, last_heartbeat;
"#;

        let params = named_params! {
            ":id": id,
            ":dynamic_attrs": res_as_string,
        };

        let node = conn.query_row(sql, params, |row| self.node_from_row(row))?;

        Ok(node)
    }

    #[instrument(err, skip_all)]
    pub fn update_node_heartbeat(&self, id: String) -> R<()> {
        let fmt = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
                 sign:mandatory]:[offset_minute]:[offset_second]"
        );

        let timestamp_now = OffsetDateTime::now_utc().format(fmt)?;

        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
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

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn update_node_health_checks(&self, id: &str, health_checks: &str) -> R<()> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
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

        Ok(())
    }

    //
    // WORKER TYPES
    //

    #[instrument(err, skip_all)]
    pub fn get_worker_type(&self, name: String) -> R<WorkerConfig> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        let sql = "SELECT name, pool, additional_node_checks, attributes, name_prefix FROM worker_type WHERE name = ?";

        let config = conn.query_row(sql, [name], |row| {
            let af: String = row.get(2)?;
            let attrs: String = row.get(3)?;
            let parsed_attrs = serde_json::from_str(&attrs).unwrap();
            let node_checks: Vec<String> = if af.is_empty() {
                vec![]
            } else {
                af.split(",").map(|s| s.to_string()).collect()
            };

            let name_prefix = row.get(4)?;

            Ok(WorkerConfig {
                name: row.get(0)?,
                pool: row.get(1)?,
                additional_node_checks: node_checks,
                attributes: parsed_attrs,
                name_prefix,
            })
        })?;

        Ok(config)
    }

    #[instrument(err, skip_all)]
    pub fn insert_worker_type(&self, config: &WorkerConfig) -> R<WorkerConfig> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;

        let node_checks_to_save = config.additional_node_checks.join(",");
        let attrs_to_save = serde_json::to_string(&config.attributes)?;

        let sql = r#"
INSERT INTO worker_type (name, pool, additional_node_checks, attributes, name_prefix)
VALUES (:name, :pool, :af, :attrs, :name_prefix)
ON CONFLICT(name) DO UPDATE SET pool = :pool, additional_node_checks = :af, attributes = :attrs, name_prefix = :name_prefix
RETURNING name, pool, additional_node_checks, attributes, name_prefix;
"#;

        let params = named_params! {
            ":name": config.name,
            ":pool": config.pool,
            ":af": node_checks_to_save,
            ":attrs": attrs_to_save,
            ":name_prefix": config.name_prefix,
        };

        let config = conn.query_row(sql, params, |row| {
            let af: String = row.get(2)?;
            let attrs: String = row.get(3)?;
            let parsed_attrs = serde_json::from_str(&attrs).unwrap();
            let name_prefix = row.get(4)?;

            Ok(WorkerConfig {
                name: row.get(0)?,
                pool: row.get(1)?,
                additional_node_checks: af.split(",").map(|s| s.to_string()).collect(),
                attributes: parsed_attrs,
                name_prefix,
            })
        })?;

        Ok(config)
    }

    //
    // JOBS
    //

    #[instrument(err, skip_all)]
    pub fn get_job(&self, name: &str) -> R<Job> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;

        let sql = r#"
SELECT id, name, status, created_at
FROM job
WHERE name = ?;
"#;

        let job = conn.query_row(sql, [name], |row| {
            Ok(Job {
                id: row.get(0)?,
                name: row.get(1)?,
                status: row.get(2)?,
                created_at: row.get(3)?,
            })
        })?;

        Ok(job)
    }

    pub fn get_jobs(&self) -> R<Vec<Job>> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        let sql = r#"
SELECT id, name, status, created_at
FROM job;
"#;

        let mut stmt = conn.prepare(sql)?;
        let job_iter = stmt.query_map([], |row| self.job_from_row(row))?;
        let mut jobs = Vec::new();
        for job in job_iter {
            let j = job?;
            jobs.push(j);
        }

        Ok(jobs)
    }

    #[instrument(err, skip_all)]
    pub fn stop_job(&self, name: &str) -> R<()> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
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
        let mut conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        let tx = conn.transaction()?;

        let get_job_sql = r#"
        SELECT id, name, status, created_at
        FROM job
        WHERE name = ?;
        "#;

        let job = tx.query_row(get_job_sql, [name], |row| {
            Ok(Job {
                id: row.get(0)?,
                name: row.get(1)?,
                status: row.get(2)?,
                created_at: row.get(3)?,
            })
        })?;

        if job.status != "stopped" {
            return Err("Job must be stopped before it can be destroyed".into());
        }

        let update_sql = r#"
            DELETE FROM job
            WHERE name = ? AND status = 'stopped';
        "#;

        tx.execute(update_sql, [name])?;

        tx.commit()
            .map_err(|_| "Error committing transaction".into())
    }

    #[instrument(err, skip_all)]
    pub fn get_job_config(&self, name: &str) -> R<JobConfig> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;

        let jc_sql = r#"
SELECT id, name, type, version, meta, created_at
FROM job_config
WHERE name = ?
ORDER BY version DESC
LIMIT 1;
"#;

        let (job_config, jc_id): (JobConfig, String) = conn.query_row(jc_sql, [name], |row| {
            let tasks_to_replace = vec![];
            let job_config_id = row.get(0)?;
            let jc = JobConfig {
                name: row.get(1)?,
                tasks: tasks_to_replace,
                version: Some(row.get(3)?),
            };

            Ok((jc, job_config_id))
        })?;

        println!("jc_id: {}", jc_id);

        let task_sql = r#"
SELECT id, name, driver, count, config, resources, constraints, job_id, job_config_id
FROM task_config
WHERE job_config_id = ?;
"#;
        let mut stmt = conn.prepare(task_sql)?;

        let task_iter = stmt.query_map([jc_id], |row| {
            Ok(TaskConfig {
                name: row.get(1)?,
                driver: row.get(2)?,
                count: row.get(3)?,
                config: row.get(4)?,
                resources: row.get(5)?,
                constraints: row.get(6)?,
            })
        })?;

        let mut tasks = Vec::new();
        for task in task_iter {
            let t = task?;
            tasks.push(t);
        }

        let jc_2 = JobConfig {
            tasks: tasks,
            ..job_config
        };

        Ok(jc_2)
    }

    #[allow(dead_code)]
    pub fn handle_job_submit(&self, config: &JobConfig, subs: &Vec<Submission>) -> R<()> {
        // Note: There is a race condition here with the version
        // I think this should prob be all in a single lock, but
        // I am not going to worry about this for now

        let mut next_version = 1;
        let mut last_versions_id = "".to_string();

        if let Ok(latest_job_config) = self.get_job_config(&config.name) {
            // if latest_job_config is deep equal to config, return an error
            if latest_job_config.is_same_as(config) {
                return Err("Job config is unchanged".into());
            } else {
                next_version = latest_job_config.version.unwrap_or(0) + 1;
                last_versions_id = latest_job_config.id();
            }
        }

        let mut conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let tx = conn.transaction()?;

        let get_job_sql = r#"
SELECT id, name, status, created_at
FROM job
WHERE name = ?;
"#;

        let get_job_res = tx.query_row(get_job_sql, [config.name.clone()], |row| {
            Ok(Job {
                id: row.get(0)?,
                name: row.get(1)?,
                status: row.get(2)?,
                created_at: row.get(3)?,
            })
        });

        let job = match get_job_res {
            Ok(j) => Ok(j),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                let job_sql = r#"
INSERT INTO job (id, name, status, created_at)
VALUES (:id, :name, :status, :created_at)
RETURNING id, name, status, created_at;
"#;

                let job_params = named_params! {
                    ":id": config.name,
                    ":name": config.name,
                    ":status": "done",
                    ":created_at": now,
                };

                tx.query_row(job_sql, job_params, |row| {
                    Ok(Job {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        status: row.get(2)?,
                        created_at: row.get(3)?,
                    })
                })
            }
            Err(error) => Err(error),
        }?;

        let jc_sql = r#"
INSERT INTO job_config (id, name, type, meta, version, created_at, job_id)
VALUES (:id, :name, :type, :meta, :version, :created_at, :job_id)
RETURNING id, name, type, meta, version, created_at;
"#;

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
            tx.query_row(jc_sql, config_params, |row| {
                let tasks = vec![];
                Ok((
                    JobConfig {
                        name: row.get(1)?,
                        version: Some(row.get(4)?),
                        tasks: tasks,
                    },
                    row.get(0)?,
                ))
            })?;

        let tasks_sql = r#"
INSERT INTO task_config (name, driver, count, config, resources, constraints, job_id, job_config_id)
VALUES (:name, :driver, :count, :config, :resources, :constraints, :job_id, :job_config_id);
"#;

        config.tasks.iter().for_each(|t| {
            let task_params = named_params! {
                ":name": t.name,
                ":driver": t.driver,
                ":count": t.count,
                ":config": t.config,
                ":resources": t.resources,
                ":constraints": t.constraints,
                ":job_id": job.id,
                ":job_config_id": job_config_id,
            };

            let _num_rows_changes = tx.execute(tasks_sql, task_params);
        });

        let subs_sql = r#"
INSERT INTO submission (filename, text, is_primary, job_config_id)
VALUES (:filename, :text, :is_primary, :job_config_id);
"#;

        subs.iter().for_each(|s| {
            let sub_params = named_params! {
                ":filename": s.filename,
                ":text": s.text,
                ":is_primary": s.is_primary,
                ":job_config_id": job_config_id,
            };

            let _num_sub_rows_changes = tx.execute(subs_sql, sub_params);
        });

        let job_update_sql = r#"
INSERT INTO job_update (id, status, job_id, to_job_config_id, from_job_config_id)
VALUES (:id, :status, :job_id, :to_job_config_id, :from_job_config_id);
"#;

        let job_update_params = named_params! {
            ":id": format!("{}-{}", job.id, next_version.to_string()),
            ":status": "active",
            ":job_id": job.id,
            ":to_job_config_id": job_config_id,
            ":from_job_config_id": last_versions_id,
        };

        let _num_update_row_changes = tx.execute(job_update_sql, job_update_params);

        tx.commit()
            .map_err(|_| "Error committing transaction".into())
    }

    #[instrument(err, skip_all)]
    pub fn get_submissions_for_config(&self, job_config_id: &str) -> R<Vec<Submission>> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;

        let sql = r#"
SELECT s.id, s.filename, s.text, s.job_config_id, s.is_primary
FROM submission s
WHERE s.job_config_id = ?;
"#;
        let mut stmt = conn.prepare(sql)?;
        let sub_iter = stmt.query_map([job_config_id], |row| {
            Ok(Submission {
                filename: row.get(1)?,
                text: row.get(2)?,
                is_primary: row.get(4)?,
            })
        })?;

        let mut subs = Vec::new();
        for sub in sub_iter {
            let s = sub?;
            subs.push(s);
        }

        Ok(subs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_store() -> R<Store> {
        let store_config = ManagerConfigFull {
            store_path: "tests/store".to_string(),
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        store.clear_all_tables()?;
        Ok(store)
    }

    // fn teardown_store(store: &Store) -> R<()> {
    //     let migrations = Store::get_migrations();
    //     let mut conn = store.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
    //     migrations.to_version(&mut conn, 0)?;

    //     Ok(())
    // }

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
        };

        let returned_config = store.insert_worker_type(&worker_config)?;
        assert_eq!(returned_config.name, worker_config.name);

        let returned_config = store.get_worker_type("test".to_string())?;
        assert_eq!(returned_config.name, worker_config.name);
        assert_eq!(returned_config.pool, worker_config.pool);
        let val = returned_config
            .attributes
            .get("test")
            .ok_or("no test attr")?;

        assert_eq!(val, "val");

        Ok(())
    }

    #[test]
    fn handle_job_submit_adds_new_job() -> R<()> {
        let sub_one = Submission {
            filename: "sub-1.jsonnet".into(),
            text: "sub-1".into(),
            is_primary: true,
        };
        let sub_two = Submission {
            filename: "sub-2.libsonnet".into(),
            text: "sub-2".into(),
            is_primary: false,
        };

        let subs = vec![sub_one.clone(), sub_two.clone()];

        let task_one = TaskConfig {
            name: "task-1".to_string(),
            driver: "driver-1".to_string(),
            count: 1,
            config: json!({ "message": "hello-1" }),
            resources: json!({ "memory": 256 }),
            constraints: json!({}),
        };

        let task_two = TaskConfig {
            name: "task-2".to_string(),
            driver: "driver-2".to_string(),
            count: 2,
            config: json!({ "message": "hi-2" }),
            resources: json!({ "memory": 512 }),
            constraints: json!({}),
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
            "Query returned no rows"
        );

        // NOTE: What should this return (figure it out in context later)
        let submit_response = store.handle_job_submit(&job_config, &subs);
        assert!(submit_response.is_ok());

        let job = store.get_job(&job_name)?;
        assert_eq!(job.name, job_name);

        let latest_job_config = store.get_job_config(&job_name)?;
        assert_eq!(latest_job_config.name, job_name);

        let t1_from_store = latest_job_config.tasks.get(0).ok_or("oof")?;
        assert_eq!(task_one.name, t1_from_store.name);
        assert_eq!(task_one.driver, t1_from_store.driver);
        assert_eq!(
            task_one.resources.get("memory").ok_or("no memory 1")?,
            t1_from_store.resources.get("memory").ok_or("no memory 2")?
        );

        assert_eq!(
            task_one.config.get("message").ok_or("no message 1")?,
            t1_from_store.config.get("message").ok_or("no message 2")?
        );

        let t2_from_store = latest_job_config.tasks.get(1).ok_or("oof")?;
        assert_eq!(task_two.name, t2_from_store.name);

        let subs_from_store = store.get_submissions_for_config(&latest_job_config.id())?;
        assert_eq!(subs_from_store.len(), 2);
        let sub_one_from_store = subs_from_store.get(0).ok_or("no sub 1")?;
        let sub_two_from_store = subs_from_store.get(1).ok_or("no sub 2")?;
        assert_eq!(sub_one_from_store.filename, sub_one.filename);
        assert_eq!(sub_two_from_store.filename, sub_two.filename);

        Ok(())
    }

    #[test]
    fn handle_job_submit_updates_job_properly() -> R<()> {
        let orib_sub = Submission {
            filename: "sub-1.jsonnet".into(),
            text: "sub-1".into(),
            is_primary: true,
        };
        let orig_submissions = vec![orib_sub.clone()];

        let orig_task = TaskConfig {
            name: "orig-task".to_string(),
            driver: "driver-1".to_string(),
            count: 1,
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            constraints: json!({}),
        };

        let orig_task_configs = vec![orig_task.clone()];
        let job_config = JobConfig {
            name: "jobname".into(),
            tasks: orig_task_configs.clone(),
            version: None,
        };

        let store = setup_store()?;

        // Original Job is submitted
        let submit_response = store.handle_job_submit(&job_config, &orig_submissions);
        assert!(submit_response.is_ok());

        // Get config for initial insert
        let first_job_config = store.get_job_config("jobname".into())?;
        assert_eq!(first_job_config.name, job_config.name);
        assert_eq!(
            1,
            first_job_config
                .version
                .ok_or("missing version on config")?,
        );

        // Submit an updates job config
        // updated: task driver config

        let new_task = TaskConfig {
            driver: "driver-new".to_string(),
            ..orig_task
        };
        let new_task_configs = vec![new_task.clone()];
        let job_config_with_modified_driver = JobConfig {
            tasks: new_task_configs.clone(),
            ..job_config
        };

        let submit_response =
            store.handle_job_submit(&job_config_with_modified_driver.clone(), &orig_submissions);
        assert!(submit_response.is_ok());

        // Get config for initial insert
        let second_job_config = store.get_job_config("jobname".into())?;
        assert_eq!(
            2,
            second_job_config
                .version
                .ok_or("missing version on config")?,
        );

        // Submit an updates job config
        // updated: additional task added

        let extra_task = TaskConfig {
            name: "extra-task".to_string(),
            driver: "driver-extra".to_string(),
            count: 1,
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            constraints: json!({}),
        };
        let new_task_configs = vec![new_task.clone(), extra_task.clone()];

        let job_config_with_extra_task = JobConfig {
            tasks: new_task_configs.clone(),
            ..job_config_with_modified_driver
        };

        let submit_response =
            store.handle_job_submit(&job_config_with_extra_task, &orig_submissions);
        assert!(submit_response.is_ok());

        // Get config for initial insert
        let third_job_config = store.get_job_config("jobname".into())?;
        assert_eq!(
            3,
            third_job_config
                .version
                .ok_or("missing version on config")?,
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
        //         .ok_or("missing version on config")?,
        // );

        Ok(())
    }

    #[test]
    fn handle_job_submit_with_unchanged_job_returns_error() -> R<()> {
        let orig_submissions = vec![];
        let orig_task = TaskConfig {
            name: "dupe-task".to_string(),
            driver: "driver".to_string(),
            count: 1,
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            constraints: json!({}),
        };

        let orig_task_configs = vec![orig_task.clone()];
        let job_config = JobConfig {
            name: "jobname".into(),
            tasks: orig_task_configs.clone(),
            version: None,
        };

        let store = setup_store()?;

        // Original Job is submitted
        let submit_response = store.handle_job_submit(&job_config, &orig_submissions);
        assert!(submit_response.is_ok());

        // Errors on resubmit
        let submit_response = store.handle_job_submit(&job_config, &orig_submissions);
        assert!(submit_response.is_err());
        assert_eq!(
            submit_response.unwrap_err().to_string(),
            "Job config is unchanged"
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
