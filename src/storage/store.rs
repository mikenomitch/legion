use crate::constants::DEFAULT_STORE_PATH;
// use crate::structs::Node;
use crate::util::{R, merge_json_strings};
use log::debug;
use rusqlite::Connection;
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;

use crate::structs::Node as Node;

// warning incorrectly
#[allow(dead_code)]
pub enum Msg {
    Content(String),
}

pub type MemSafeConn = Arc<Mutex<Connection>>;
pub struct Store {
    conn: MemSafeConn,
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
        let conn = conn.lock().unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS node (
                id    STRING PRIMARY KEY,
                status STRING NOT NULL,
                attrs  JSONB NOT NULL,
                dynamic JSONB NOT NULL,
                last_heartbeat TIMESTAMP NOT NULL
            )",
            (), // empty list of parameters.
        )?;

        Ok(())
    }

    // ========================================
    // NODES
    // ========================================

    pub fn get_node(&self, id: String) -> R<Node> {
        let conn = self.conn.lock().unwrap();
        let sql = "SELECT id, attrs, dynamic, status, last_heartbeat FROM node WHERE id = ?";
        let node = conn.query_row(sql, [id], |row| {
            Ok(Node {
                id: row.get(0).unwrap(),
                attrs: row.get(1).unwrap(),
                dynamic: row.get(2).unwrap(),
                status: row.get(3).unwrap(),
                last_heartbeat: row.get(4).unwrap(),
            })
        })?;

        Ok(node)
    }

    pub fn get_nodes(&self) -> R<Vec<Node>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, attrs, dynamic, status, last_heartbeat FROM node")?;
        let node_iter = stmt.query_map([], |row| {
            Ok(Node {
                id: row.get(0).unwrap(),
                attrs: row.get(1).unwrap(),
                dynamic: row.get(2).unwrap(),
                status: row.get(3).unwrap(),
                last_heartbeat: row.get(4).unwrap(),
            })
        })?;

        let mut nodes = Vec::new();
        for node in node_iter {
            let n = node.unwrap();
            nodes.push(n);
        }

        Ok(nodes)
    }

    pub fn insert_node(&self, node: Node) -> R<Node> {
        let conn = self.conn.lock().unwrap();
        let sql = r#"
INSERT INTO node (id, attrs, dynamic, status, last_heartbeat)
VALUES (?1, ?2, ?3, ?4, ?5)
ON CONFLICT(id) DO UPDATE SET attrs = ?2, dynamic = ?3, status = ?4, last_heartbeat = ?5
RETURNING id, attrs, dynamic, status, last_heartbeat;
"#;
        let node = conn.query_row(sql, [node.id, node.attrs, node.dynamic, node.status, node.last_heartbeat], |row| {
            Ok(Node {
                id: row.get(0).unwrap(),
                attrs: row.get(1).unwrap(),
                dynamic: row.get(2).unwrap(),
                status: row.get(3).unwrap(),
                last_heartbeat: row.get(4).unwrap(),
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
INSERT INTO node (id, attrs, dynamic)
VALUES (?1, ?2, ?3)
ON CONFLICT(id) DO UPDATE SET attrs = ?2, dynamic = ?3
RETURNING id, attrs, dynamic, status, last_heartbeat;
"#;
        let node = conn.query_row(sql, [id, node.attrs, result], |row| {
            Ok(Node {
                id: row.get(0).unwrap(),
                attrs: row.get(1).unwrap(),
                dynamic: row.get(2).unwrap(),
                status: row.get(3).unwrap(),
                last_heartbeat: row.get(4).unwrap(),
            })
        })?;

        Ok(node)
    }

    pub fn update_node_heartbeat(&self, id: String) -> R<()> {
        let timestamp_now = OffsetDateTime::now_utc().to_string();

        let conn = self.conn.lock().unwrap();
        let sql = r#"
        UPDATE node
        SET last_heartbeat = ?1
        WHERE id = ?2;
"#;
        println!("Updating heartbeat for node {} with TS: {}", id, timestamp_now);
        conn.execute(sql, [timestamp_now, id])?;

        Ok(())
    }
}
