use crate::{constants::DEFAULT_STORE_PATH, worker::fingerprint};
// use crate::structs::Node;
use rusqlite_migration::{Migrations, M};
use crate::util::{R, merge_json_strings};
use log::debug;
use rusqlite::Connection;
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use time::macros::format_description;

use crate::structs::{WorkerConfig, Node};

// warning incorrectly
#[allow(dead_code)]
pub enum Msg {
    Content(String),
}

pub type MemSafeConn = Arc<Mutex<Connection>>;
pub struct Store {
    pub conn: MemSafeConn,
}

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
            M::up("CREATE TABLE IF NOT EXISTS node (
                id    STRING PRIMARY KEY,
                status STRING NOT NULL,
                pool STRING NOT NULL,
                attrs  JSONB NOT NULL,
                dynamic JSONB NOT NULL,
                last_heartbeat TIMESTAMP NOT NULL
            )").down("DROP TABLE node;"),
            M::up("CREATE TABLE IF NOT EXISTS worker_type (
                name STRING PRIMARY KEY,
                pool STRING NOT NULL,
                additional_fingerprinters Text NOT NULL,
                attributes JSONB NOT NULL
            )").down("DROP TABLE worker_type;"),
        ]);

        let mut conn = conn.lock().unwrap();

        // Note: from rusqlite_migrations:
        conn.pragma_update(None, "journal_mode", &"WAL").unwrap();

        migrations.to_latest(&mut conn).unwrap();

        Ok(())
    }

    // ========================================
    // NODES
    // ========================================

    pub fn get_node(&self, id: String) -> R<Node> {
        let conn = self.conn.lock().unwrap();
        let sql = "SELECT id, attrs, dynamic, status, pool, last_heartbeat FROM node WHERE id = ?";
        let node = conn.query_row(sql, [id], |row| {
            Ok(Node {
                id: row.get(0)?,
                attrs: row.get(1)?,
                dynamic: row.get(2)?,
                status: row.get(3)?,
                pool: row.get(4)?,
                last_heartbeat: row.get(5)?,
            })
        })?;

        Ok(node)
    }

    pub fn get_nodes(&self) -> R<Vec<Node>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, attrs, dynamic, status, pool, last_heartbeat FROM node")?;
        let node_iter = stmt.query_map([], |row| {
            Ok(Node {
                id: row.get(0)?,
                attrs: row.get(1)?,
                dynamic: row.get(2)?,
                status: row.get(3)?,
                pool: row.get(4)?,
                last_heartbeat: row.get(5)?,
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
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, last_heartbeat FROM node")?;
        let node_iter = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;

        let mut nodes_id_and_hb = Vec::new();
        for node in node_iter {
            let h = node?;
            nodes_id_and_hb.push(h);
        }

        Ok(nodes_id_and_hb)
    }

    pub fn mark_nodes_as_lost(&self, ids: Vec<String>) -> R<()> {
        let conn = self.conn.lock().unwrap();
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
        let conn = self.conn.lock().unwrap();
        let sql = r#"
INSERT INTO node (id, attrs, dynamic, status, pool, last_heartbeat)
VALUES (?1, ?2, ?3, ?4, ?5, ?6)
ON CONFLICT(id) DO UPDATE SET attrs = ?2, dynamic = ?3, status = ?4, pool = ?5, last_heartbeat = ?6
RETURNING id, attrs, dynamic, status, pool, last_heartbeat;
"#;
        let node = conn.query_row(sql, [node.id, node.attrs, node.dynamic, node.status, node.pool, node.last_heartbeat], |row| {
            Ok(Node {
                id: row.get(0)?,
                attrs: row.get(1)?,
                dynamic: row.get(2)?,
                status: row.get(3)?,
                pool: row.get(4)?,
                last_heartbeat: row.get(5)?,
            })
        })?;

        Ok(node)
    }

    pub fn update_node_dyamic(&self, id: String, dynamic_diff: String) -> R<Node> {
        let node = self.get_node(id.clone())?;
        let initial_dynamic = node.dynamic.clone();
        let result = merge_json_strings(initial_dynamic, dynamic_diff);

        let conn = self.conn.lock().unwrap();
        let sql = r#"
        UPDATE node
        SET dynamic = ?2
        WHERE id IN (?1)
        RETURNING id, attrs, dynamic, status, pool, last_heartbeat;
"#;
        let node = conn.query_row(sql, [id, result], |row| {
            Ok(Node {
                id: row.get(0)?,
                attrs: row.get(1)?,
                dynamic: row.get(2)?,
                status: row.get(3)?,
                pool: row.get(4)?,
                last_heartbeat: row.get(5)?,
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

        let conn = self.conn.lock().unwrap();
        let sql = r#"
        UPDATE node
        SET last_heartbeat = ?1, status = 'active'
        WHERE id = ?2;
"#;
        debug!("Updating heartbeat for node {} with TS: {}", id, timestamp_now);
        conn.execute(sql, [timestamp_now, id])?;

        Ok(())
    }

    pub fn get_worker_type(&self, name: String) -> R<WorkerConfig> {
        let conn = self.conn.lock().unwrap();
        let sql = "SELECT name, pool, additional_fingerprinters, attributes FROM worker_type WHERE name = ?";

        let config = conn.query_row(sql, [name], |row| {
            let af: String= row.get(2)?;
            let attrs: String= row.get(3)?;

            println!("Af: {}", af);
            println!("Attrs: {}", attrs);

            let fingerprinters: Vec<String>;
            if af == "" {
                fingerprinters = vec![];
            } else {
                fingerprinters = af.split(",").map(|s| s.to_string()).collect();
            }

            Ok(WorkerConfig {
                name: row.get(0)?,
                pool: row.get(1)?,
                additional_fingerprinters: fingerprinters,
                attributes: serde_json::from_str(&attrs).unwrap(),
            })
        })?;

        Ok(config)
    }

    pub fn insert_worker_type(&self, config: WorkerConfig) -> R<WorkerConfig> {
        let conn = self.conn.lock().unwrap();

        let fingerprinters_to_save = config.additional_fingerprinters.join(",");
        let attrs_to_save = serde_json::to_string(&config.attributes)?;

        println!("&config.attributes: {:?}", &config.attributes);
        println!("Fingerprinters to save: {}", fingerprinters_to_save);
        println!("attrs_to_save: {}", attrs_to_save);

        let sql = r#"
INSERT INTO worker_type (name, pool, additional_fingerprinters, attributes)
VALUES (?1, ?2, ?3, ?4)
ON CONFLICT(name) DO UPDATE SET pool = ?2, additional_fingerprinters = ?3, attributes = ?4
RETURNING name, pool, additional_fingerprinters, attributes;
"#;

            let config = conn.query_row(sql, [config.name, config.pool, fingerprinters_to_save, attrs_to_save], |row| {
            let af: String= row.get(2)?;
            let attrs: String= row.get(3)?;

            Ok(WorkerConfig {
                name: row.get(0)?,
                pool: row.get(1)?,
                additional_fingerprinters: af.split(",").map(|s| s.to_string()).collect(),
                attributes: serde_json::from_str(&attrs).unwrap(),
            })
        })?;

        Ok(config)
    }
}
