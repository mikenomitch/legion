use crate::prelude::*;

use crate::structs::{ManagerConfigFull, Node, NodeMsg, WorkerConfig};

use rusqlite::{named_params, Connection};
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
        info!("Starting store");
        let conn = Store::create_memsafe_conn(&config.store_path)?;
        Store::create_initial_tables(&Arc::clone(&conn))?;

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

    #[instrument(err, skip_all)]
    fn create_initial_tables(conn: &MemSafeConn) -> R<()> {
        let migrations = Migrations::new(vec![
            M::up(
                "CREATE TABLE IF NOT EXISTS node (
                id    STRING PRIMARY KEY,
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
        ]);

        let mut conn = conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        migrations.to_latest(&mut conn)?;

        Ok(())
    }

    // ========================================
    // NODES
    // ========================================

    #[instrument(err, skip_all)]
    pub fn get_node(&self, id: &str) -> R<Node> {
        info!("In get node yay");
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;

        let sql = r#"
        SELECT id, pool, status, availability, config_attrs, fingerprint_attrs, dynamic_attrs, health_checks, last_heartbeat
        FROM node
        WHERE id = ?;
"#;

        let node = conn.query_row(sql, [id], |row| {
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
        })?;

        info!("leaving get node");
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
        let node_iter = stmt.query_map([], |row| {
            Ok(Node {
                id: row.get(0)?,
                pool: row.get(1)?,
                status: row.get(2)?,
                availability: row.get(3)?,
                config_attrs: row.get(4)?,
                fingerprint_attrs: row.get(5)?,
                dynamic_attrs: row.get(6)?,
                health_checks: row.get(7)?,
                last_heartbeat: row.get(8)?,
            })
        })?;

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
            Ok(Node {
                id: row.get(0)?,
                pool: row.get(1)?,
                status: row.get(2)?,
                availability: row.get(3)?,
                config_attrs: row.get(4)?,
                fingerprint_attrs: row.get(5)?,
                dynamic_attrs: row.get(6)?,
                health_checks: row.get(7)?,
                last_heartbeat: row.get(8)?,
            })
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

        let node = conn.query_row(sql, params, |row| {
            Ok(Node {
                id: row.get(0)?,
                pool: row.get(1)?,
                status: row.get(2)?,
                availability: row.get(3)?,
                config_attrs: row.get(4)?,
                fingerprint_attrs: row.get(5)?,
                dynamic_attrs: row.get(6)?,
                health_checks: row.get(7)?,
                last_heartbeat: row.get(8)?,
            })
        })?;

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
}
