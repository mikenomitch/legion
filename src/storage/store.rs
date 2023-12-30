use crate::constants::DEFAULT_STORE_PATH;
use crate::util::{merge_json_strings, R};
use log::debug;
use rusqlite::{named_params, Connection};
use rusqlite_migration::{Migrations, M};
use serde_rusqlite::to_params_named;
use std::sync::{Arc, Mutex};
use time::macros::format_description;
use time::OffsetDateTime;

use crate::structs::{Node, WorkerConfig};

// warning incorrectly
#[allow(dead_code)]
pub enum Msg {
    Content(String),
}

pub type MemSafeConn = Arc<Mutex<Connection>>;
pub struct Store {
    pub conn: MemSafeConn,
}

pub const CONN_LOCK_ERROR: &str = "Error locking database connection";

// NOTE: This will become a general interface for all storage backends
impl Store {
    pub fn start_storage() -> R<Store> {
        debug!("Starting store");
        let conn = Store::create_memsafe_conn()?;

        Store::create_initial_tables(Arc::clone(&conn))?;

        let store = Store { conn };
        Ok(store)
    }

    fn create_memsafe_conn() -> R<MemSafeConn> {
        // let conn = Connection::open_in_memory()?;

        // Using the disk version for now to simulate logs
        // Would eventually split this into a cluster that shares
        // logs via raft before committing to an in-memory sqlite db
        let conn = Connection::open(DEFAULT_STORE_PATH)?;
        Ok(Arc::new(Mutex::new(conn)))
    }

    fn create_initial_tables(conn: MemSafeConn) -> R<()> {
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
        conn.pragma_update(None, "journal_mode", &"WAL")?;
        migrations.to_latest(&mut conn)?;

        Ok(())
    }

    // ========================================
    // NODES
    // ========================================

    pub fn get_node(&self, id: String) -> R<Node> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;

        let sql = r#"
        SELECT id, pool, status, availability, config_attrs, fingerprint_attrs, dynamic_attrs, health_checks, last_heartbeat
        FROM node
        WHERE id = ?;
"#;

        let node = conn.query_row(sql, [id], |row| {
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

    pub fn mark_nodes_as_lost(&self, ids: Vec<String>) -> R<()> {
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

    pub fn insert_node(&self, node: Node) -> R<Node> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        let sql = r#"
        INSERT INTO node (id, pool, status, availability, config_attrs, fingerprint_attrs, dynamic_attrs, health_checks, last_heartbeat)
        VALUES (:id, :pool, :status, :availability, :config_attrs, :fingerprint_attrs, :dynamic_attrs, :health_checks, :last_heartbeat)
        ON CONFLICT(id) DO UPDATE SET pool = :pool, status = :status, availability = :availability, config_attrs = :config_attrs, fingerprint_attrs = :fingerprint_attrs, dynamic_attrs = :dynamic_attrs, health_checks = :health_checks, last_heartbeat = :last_heartbeat
        RETURNING id, pool, status, availability, config_attrs, fingerprint_attrs, dynamic_attrs, health_checks, last_heartbeat;
"#;

        let node = conn.query_row(
            sql,
            to_params_named(&node)?.to_slice().as_slice(),
            |row| {
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
            },
        )?;

        Ok(node)
    }

    pub fn update_node_dyamic(&self, id: String, dynamic_diff: String) -> R<Node> {
        let node = self.get_node(id.clone())?;
        let initial_dynamic = node.dynamic_attrs.clone();
        let result = merge_json_strings(initial_dynamic, dynamic_diff);

        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        let sql = r#"
        UPDATE node
        SET dynamic_attrs = :dynamic_attrs
        WHERE id IN (:id)
        RETURNING id, pool, status, availability, config_attrs, fingerprint_attrs, dynamic_attrs, health_checks, last_heartbeat;
"#;

        let params = named_params! {
            ":id": id,
            ":dynamic_attrs": result,
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
        debug!(
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

    pub fn update_node_health_checks(&self, id: String, health_checks: String) -> R<()> {
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

        println!("DOING IT!!! {}", health_checks);

        conn.execute(sql, params)?;

        Ok(())
    }

    pub fn get_worker_type(&self, name: String) -> R<WorkerConfig> {
        let conn = self.conn.lock().map_err(|_| CONN_LOCK_ERROR)?;
        let sql = "SELECT name, pool, additional_node_checks, attributes, name_prefix FROM worker_type WHERE name = ?";

        let config = conn.query_row(sql, [name], |row| {
            let af: String = row.get(2)?;

            let attrs: String = row.get(3)?;
            let parsed_attrs = serde_json::from_str(&attrs).unwrap();

            let node_checks: Vec<String>;
            if af == "" {
                node_checks = vec![];
            } else {
                node_checks = af.split(",").map(|s| s.to_string()).collect();
            }

            let name_prefix = row.get(4)?;

            Ok(WorkerConfig {
                name: row.get(0)?,
                pool: row.get(1)?,
                additional_node_checks: node_checks,
                attributes: parsed_attrs,
                name_prefix: name_prefix,
            })
        })?;

        Ok(config)
    }

    pub fn insert_worker_type(&self, config: WorkerConfig) -> R<WorkerConfig> {
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
                name_prefix: name_prefix,
            })
        })?;

        Ok(config)
    }
}
