use crate::cmd::structs::AgentConfig;

// use std::any::Any;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};

use raft::eraftpb::ConfState;
use raft::prelude::*;
use raft::storage::MemStorage;
use slog::Logger;

type ProposeCallback = Box<dyn Fn() + Send>;

// warning incorrectly
#[allow(dead_code)]
pub enum Msg {
    Propose {
        id: u8,
        message: String,
        cb: ProposeCallback,
    },
    // Here we don't use Raft Message, so use dead_code to
    // avoid the compiler warning.
    #[allow(dead_code)]
    Raft(Message),
}

pub fn start_raft(
    receiver_from_server: Receiver<Msg>,
    sender_to_server: Arc<Mutex<Sender<String>>>,
    config: AgentConfig,
) {
    thread::spawn(move || {
        println!("Starting raft");

        let logger = raft::default_logger();
        let storage = create_storage();
        let mut raft_node = create_node(config, storage, logger);

        drive_raft(receiver_from_server, &mut raft_node)
    });
}

fn drive_raft(receiver_from_server: Receiver<Msg>, raft_node: &mut RawNode<MemStorage>) {
    // Loop forever to drive the Raft.
    let mut t = Instant::now();
    let mut timeout = Duration::from_millis(100);

    // Use a HashMap to hold the `propose` callbacks.
    let mut cbs = HashMap::new();

    loop {
        match receiver_from_server.recv_timeout(timeout) {
            // Client sends a proposal to this node first
            Ok(Msg::Propose { id, message, cb }) => {
                cbs.insert(id, cb);

                // NOTE: this is only proposing the request ID
                // be added to the log, change to include the full request,
                // and the id would be part of it, the rest is whatever you're adding
                // to the actual log!

                let new_line = message.as_bytes().to_vec();
                raft_node.propose(vec![], new_line).unwrap();
            }
            // Message is received from another node
            Ok(Msg::Raft(m)) => raft_node.step(m).unwrap(),
            Err(RecvTimeoutError::Timeout) => (),
            Err(RecvTimeoutError::Disconnected) => return,
        }

        let d = t.elapsed();
        t = Instant::now();
        if d >= timeout {
            timeout = Duration::from_millis(100);
            // We drive Raft every 100ms.
            raft_node.tick();
        } else {
            timeout -= d;
        }

        on_ready(raft_node, &mut cbs);
    }
}

fn on_ready(raft_group: &mut RawNode<MemStorage>, cbs: &mut HashMap<u8, ProposeCallback>) {
    if !raft_group.has_ready() {
        return;
    }
    let store = raft_group.raft.raft_log.store.clone();

    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready = raft_group.ready();

    let handle_messages = |msgs: Vec<Message>| {
        for _msg in msgs {
            // TODO: send messages to other peers.
        }
    };

    if !ready.messages().is_empty() {
        // Send out the messages come from the node.
        handle_messages(ready.take_messages());
    }

    if !ready.snapshot().is_empty() {
        // This is a snapshot, we need to apply the snapshot at first.
        store.wl().apply_snapshot(ready.snapshot().clone()).unwrap();
    }

    let mut _last_apply_index = 0;
    let mut handle_committed_entries = |committed_entries: Vec<Entry>| {
        for entry in committed_entries {
            // Mostly, you need to save the last apply index to resume applying
            // after restart. Here we just ignore this because we use a Memory storage.
            _last_apply_index = entry.index;

            if entry.data.is_empty() {
                // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                continue;
            }

            if entry.get_entry_type() == EntryType::EntryNormal {
                if let Some(cb) = cbs.remove(entry.data.first().unwrap()) {
                    cb();
                }
            }

            // TODO: handle EntryConfChange
        }
    };

    handle_committed_entries(ready.take_committed_entries());

    if !ready.entries().is_empty() {
        // Append entries to the Raft log.
        store.wl().append(ready.entries()).unwrap();
    }

    if let Some(hs) = ready.hs() {
        // Raft HardState changed, and we need to persist it.
        store.wl().set_hardstate(hs.clone());
    }

    if !ready.persisted_messages().is_empty() {
        // Send out the persisted messages come from the node.
        handle_messages(ready.take_persisted_messages());
    }

    // Advance the Raft.
    let mut light_rd = raft_group.advance(ready);
    // Update commit index.
    if let Some(commit) = light_rd.commit_index() {
        store.wl().mut_hard_state().set_commit(commit);
    }
    // Send out the messages.
    handle_messages(light_rd.take_messages());
    // Apply all committed entries.
    handle_committed_entries(light_rd.take_committed_entries());
    // Advance the apply index.
    raft_group.advance_apply();
}

// Example making a proposal of something for raft!
fn create_storage() -> MemStorage {
    // Create a storage for Raft, and here we just use a simple memory storage.
    // You need to build your own persistent storage in your production.
    // Please check the Storage trait in src/storage.rs to see how to implement one.

    MemStorage::new_with_conf_state(ConfState::from((vec![1], vec![])))
}

fn create_node(config: AgentConfig, storage: MemStorage, logger: Logger) -> RawNode<MemStorage> {
    // Create the configuration for the Raft node.
    let cfg = Config {
        // The unique ID for the Raft node.
        id: config.manager.id,
        // Election tick is for how long the follower may campaign again after
        // it doesn't receive any message from the leader.
        election_tick: config.manager.election_tick,
        // Heartbeat tick is for how long the leader needs to send
        // a heartbeat to keep alive.
        heartbeat_tick: config.manager.heartbeat_tick,
        // The max size limits the max size of each appended message. Mostly, 1 MB is enough.
        max_size_per_msg: 1024 * 1024 * 1024,
        // Max inflight msgs that the leader sends messages to follower without
        // receiving ACKs.
        max_inflight_msgs: 256,
        // The Raft applied index.
        // You need to save your applied index when you apply the committed Raft logs.
        applied: 0,
        ..Default::default()
    };

    RawNode::new(&cfg, storage, &logger).unwrap()
}
