use crate::cmd::structs::AgentConfig;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::sync::{Arc, Mutex};
use log::debug;

// warning incorrectly
#[allow(dead_code)]
pub enum Msg {
    Content(String),
}

pub fn start_raft(
    _receiver_from_server: Receiver<Msg>,
    _sender_to_server: Arc<Mutex<Sender<String>>>,
    _config: AgentConfig,
) {
    thread::spawn(move || {
        debug!("Starting raft!");
    });
}
