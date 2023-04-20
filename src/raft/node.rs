use raft::prelude::*;
use raft::storage::MemStorage;
use slog::Logger;
// use std::any::Any;
use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError};

// use std::thread;
use std::time::{Duration, Instant};

use raft::eraftpb::ConfState;

use crate::cmd::structs::AgentConfig;

type ProposeCallback = Box<dyn Fn() + Send>;

// warning incorrectly
#[allow(dead_code)]
enum Msg {
    Propose {
        id: u8,
        cb: ProposeCallback,
    },
    // Here we don't use Raft Message, so use dead_code to
    // avoid the compiler warning.
    #[allow(dead_code)]
    Raft(Message),
}

pub fn start_raft(config: AgentConfig) {
    let logger = raft::default_logger();
    let storage = create_storage();
    let mut r = create_node(config, storage, logger);

    let (_sender, receiver) = mpsc::channel();
    // raft::default_logger().type_id();

    // Loop forever to drive the Raft.
    let mut t = Instant::now();
    let mut timeout = Duration::from_millis(100);

    // Use a HashMap to hold the `propose` callbacks.
    let mut cbs = HashMap::new();

    loop {
        match receiver.recv_timeout(timeout) {
            // Client sends a proposal to this node first
            Ok(Msg::Propose { id, cb }) => {
                cbs.insert(id, cb);
                r.propose(vec![], vec![id]).unwrap();
            }
            // Message is received from another node
            Ok(Msg::Raft(m)) => r.step(m).unwrap(),
            Err(RecvTimeoutError::Timeout) => (),
            Err(RecvTimeoutError::Disconnected) => return,
        }

        let d = t.elapsed();
        t = Instant::now();
        if d >= timeout {
            timeout = Duration::from_millis(100);
            // We drive Raft every 100ms.
            r.tick();
        } else {
            timeout -= d;
        }

        on_ready(&mut r, &mut cbs);
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
            // Send messages to other peers.
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

// // Example making a proposal of something for raft!
// fn send_propose(logger: Logger, sender: mpsc::Sender<Msg>) {
//     thread::spawn(move || {
//         // Wait some time and send the request to the Raft.
//         thread::sleep(Duration::from_secs(10));

//         let (s1, r1) = mpsc::channel::<u8>();

//         println!("propose a request");

//         slog::info!(logger, "propose a request");

//         // Send a command to the Raft, wait for the Raft to apply it
//         // and get the result.
//         sender
//             .send(Msg::Propose {
//                 id: 1,
//                 cb: Box::new(move || {
//                     s1.send(0).unwrap();
//                 }),
//             })
//             .unwrap();

//         let n = r1.recv().unwrap();
//         assert_eq!(n, 0);

//         println!("receive the propose callback");
//         slog::info!(logger, "receive the propose callback");
//     });
// }

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
        id: 1,
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
