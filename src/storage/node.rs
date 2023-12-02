use crate::util::R;
use crate::cmd::structs::AgentConfig;
use log::debug;
use rusqlite::Connection;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Debug)]
struct Node {
    id: String,
    attrs: String,
}

// warning incorrectly
#[allow(dead_code)]
pub enum Msg {
    Content(String),
}

pub fn start_storage(
    _receiver_from_server: Receiver<Msg>,
    _sender_to_server: Arc<Mutex<Sender<String>>>,
    _config: AgentConfig,
) -> R<()> {
    thread::spawn(move || {
        debug!("Starting store!");
        let conn = Connection::open("foo.db").unwrap();

        conn.execute(
            "CREATE TABLE node (
            id    STRING PRIMARY KEY,
            attrs  STRING NOT NULL
        )",
            (), // empty list of parameters.
        ).unwrap();

        let node = Node {
            id: "test".into(),
            attrs: "fingers".into(),
        };

        conn.execute(
            "INSERT INTO node (id, attrs) VALUES (?1, ?2)",
            (&node.id, &node.attrs),
        ).unwrap();

        let mut stmt = conn.prepare("SELECT id, attrs FROM node").unwrap();
        let node_iter = stmt.query_map([], |row| {
            Ok(Node {
                id: row.get(0).unwrap(),
                attrs: row.get(1).unwrap(),
            })
        }).unwrap();

        for node in node_iter {
            println!("Found node {:?}", node.unwrap());
        }
    });

    Ok(())
}
