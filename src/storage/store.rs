use crate::util::R;
use log::debug;
use rusqlite::Connection;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct Node {
    pub id: String,
    pub attrs: String,
}

// warning incorrectly
#[allow(dead_code)]
pub enum Msg {
    Content(String),
}

pub type MemSafeConn = Arc<Mutex<Connection>>;
pub struct Store {
    conn: MemSafeConn,
}

// add function test to memsafeconn
impl Store {
    pub fn start_storage() -> R<Store> {
        debug!("Starting store");
        let conn = Store::create_memsafe_conn()?;

        Store::create_initial_tables(Arc::clone(&conn))?;

        let store = Store { conn };
        Ok(store)
    }

    fn create_memsafe_conn() -> R<MemSafeConn> {
        debug!("Creating connection");
        let conn = Connection::open_in_memory()?;
        Ok(Arc::new(Mutex::new(conn)))
    }

    fn create_initial_tables(conn: MemSafeConn) -> R<()> {
        let conn = conn.lock().unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS node (
                id    STRING PRIMARY KEY,
                attrs  STRING NOT NULL
            )",
            (), // empty list of parameters.
        )?;

        Ok(())
    }

    // ========================================
    // NODES
    // ========================================

    pub fn upsert_node(&self, id: String, attrs: String) -> R<()> {
        let conn = self.conn.lock().unwrap();
        let sql = r#"
INSERT INTO node (id, attrs)
VALUES (?1, ?2)
ON CONFLICT(id) DO UPDATE SET attrs = ?2;
"#;
        conn.execute(sql, (id, attrs))?;

        Ok(())
    }

    pub fn get_nodes(&self) -> R<Vec<Node>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, attrs FROM node")?;
        let node_iter = stmt.query_map([], |row| {
            Ok(Node {
                id: row.get(0).unwrap(),
                attrs: row.get(1).unwrap(),
            })
        })?;

        let mut nodes = Vec::new();
        for node in node_iter {
            let n = node.unwrap();
            nodes.push(n);
        }

        Ok(nodes)
    }

    // ========================================
    // JOBS
    // ========================================
}


// NODES

// // Add a connection here?!?!
// fn upsert_node(_node: Node) -> R<()> {
//     Ok(())
// }

// fn get_nodes() -> R<()> {
//     Ok(())
// }
