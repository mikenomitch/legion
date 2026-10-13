//! Per-node work delivery log for `Manager.StreamWork` (sandbox-scaling
//! plan §6.13, Phase 2).
//!
//! **Soft state over a durable record.** The committed allocation rows are
//! the source of truth, as they always were. This log is held in memory on
//! whichever manager a worker streams from, and records only what that
//! worker needs to be told next:
//!
//! - **`dirty`**: allocation ids on the node whose rows changed since the
//!   last message. The store's changeset projection marks them on every
//!   replica ([`WorkLog::note_changes`]), so a follower serves a stream as
//!   well as the leader does, from its own replica.
//! - **`held`**: what the worker holds after the last message, with each
//!   payload, so the next message can say what left the set and the
//!   heartbeat can compare digests without a store read.
//! - **`seq`** and **`unacked`**: the node's message sequence and the sent
//!   messages the worker has not acknowledged, kept for replay when a
//!   dropped stream reconnects in the same epoch.
//!
//! A message is materialized from the store when it is sent, not when the
//! change commits, so a burst of changes to one allocation becomes one
//! upsert, and there is no `Pending → Enqueued` state to keep: an id is
//! either dirty or it is not.
//!
//! **The epoch** is a random non-zero value per manager process. A worker
//! whose last message came from another epoch (a failover, a restart, a
//! different manager) cannot be replayed to and gets a full resync, which
//! is today's full desired set sent once.

use crate::structs::{StopNotice, StreamWorkReply, TaskAllocation, WorkStreamState};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Sent messages a node may leave unacknowledged before a reconnect has to
/// resync instead of replaying. A worker acks every message it applies, so
/// the window only fills while a stream is down or the worker is stuck.
const MAX_UNACKED: usize = 256;

/// The order-independent digest of a set of allocation ids:
/// `<count>:<xor of splitmix64(id)>` in lowercase hex. Both sides update it
/// per id, so neither has to walk the set to compare.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkDigest {
    count: u64,
    xor: u64,
}

impl WorkDigest {
    fn mix(id: i64) -> u64 {
        // splitmix64: a cheap, well-distributed bijection, so distinct id sets
        // collide only by accident of the xor.
        let mut z = (id as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub fn insert(&mut self, id: i64) {
        self.count += 1;
        self.xor ^= Self::mix(id);
    }

    pub fn remove(&mut self, id: i64) {
        self.count = self.count.saturating_sub(1);
        self.xor ^= Self::mix(id);
    }

    pub fn of(ids: impl IntoIterator<Item = i64>) -> Self {
        let mut digest = Self::default();
        for id in ids {
            digest.insert(id);
        }
        digest
    }

    pub fn render(&self) -> String {
        format!("{:x}:{:x}", self.count, self.xor)
    }
}

/// What the next message for a node must contain, taken from its log for
/// the materializer ([`WorkLog::take_pending`]).
#[derive(Debug)]
pub enum PendingWork {
    /// Nothing changed since the last message.
    Nothing,
    /// The worker must be sent the node's whole desired set.
    Resync,
    /// These allocations changed. Each is either upserted (still desired on
    /// this node) or stopped (no longer desired, and the worker held it).
    Changed(BTreeSet<i64>),
}

/// One node's delivery state.
#[derive(Debug, Default)]
struct NodeState {
    /// The stream currently attached, by generation; 0 when none.
    session: u64,
    /// The last message's sequence number.
    seq: u64,
    /// Allocations changed since the last message.
    dirty: BTreeSet<i64>,
    /// The next message must be a full resync.
    resync: bool,
    /// What the worker holds after the last message.
    held: BTreeMap<i64, TaskAllocation>,
    digest: WorkDigest,
    /// Sent, not yet acknowledged, oldest first.
    unacked: VecDeque<StreamWorkReply>,
}

/// One node's log and the wake its stream waits on.
#[derive(Debug, Default)]
pub struct NodeWorkLog {
    state: Mutex<NodeState>,
    /// Woken when the node's log gains work to send.
    pub wake: tokio::sync::Notify,
}

impl NodeWorkLog {
    fn lock(&self) -> std::sync::MutexGuard<'_, NodeState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// How a heartbeat from a streaming worker is answered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeartbeatStreamCheck {
    /// The worker's stream is attached here in the same epoch: leave the
    /// desired set out of the reply.
    pub via_stream: bool,
    /// The worker's digest disagreed with ours at the same position: a full
    /// resync has been queued on its stream.
    pub resync: bool,
}

/// Where an attached stream starts.
#[derive(Debug)]
pub struct Attached {
    pub log: Arc<NodeWorkLog>,
    pub session: u64,
    /// Messages to replay before live ones; empty on a resync.
    pub replay: Vec<StreamWorkReply>,
}

/// Every node's delivery log on this manager.
#[derive(Debug)]
pub struct WorkLog {
    epoch: u64,
    next_session: std::sync::atomic::AtomicU64,
    nodes: Mutex<HashMap<String, Arc<NodeWorkLog>>>,
}

impl Default for WorkLog {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkLog {
    pub fn new() -> Self {
        let mut epoch = rand::random::<u64>();
        if epoch == 0 {
            epoch = 1;
        }
        WorkLog {
            epoch,
            next_session: std::sync::atomic::AtomicU64::new(1),
            nodes: Mutex::new(HashMap::new()),
        }
    }

    /// This manager process's delivery epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    fn lock_nodes(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<NodeWorkLog>>> {
        self.nodes.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn node(&self, node_id: &str) -> Option<Arc<NodeWorkLog>> {
        self.lock_nodes().get(node_id).cloned()
    }

    /// Whether any node has a log here (a worker has streamed from this
    /// manager). The projection skips its lookups when none has.
    pub fn has_nodes(&self) -> bool {
        !self.lock_nodes().is_empty()
    }

    /// Queue a full resync for every one of `nodes` that has a log: a
    /// changeset touched their allocations in a way the projection could
    /// not name row by row.
    pub fn resync_nodes<'a>(&self, nodes: impl IntoIterator<Item = &'a str>) {
        let logs = self.lock_nodes();
        for node in nodes {
            if let Some(log) = logs.get(node) {
                log.lock().resync = true;
                log.wake.notify_one();
            }
        }
    }

    /// Queue a full resync for every node with a log: the projection could
    /// not tell which allocations a committed changeset touched.
    pub fn resync_all(&self) {
        for log in self.lock_nodes().values() {
            log.lock().resync = true;
            log.wake.notify_one();
        }
    }

    /// Mark allocations changed by a committed changeset. Called by the
    /// store's projection on every replica; a node nobody streams for has
    /// no log here and costs nothing.
    pub fn note_changes<'a>(&self, changes: impl IntoIterator<Item = (&'a str, i64)>) {
        let nodes = self.lock_nodes();
        if nodes.is_empty() {
            return;
        }
        let mut woken: Vec<&Arc<NodeWorkLog>> = Vec::new();
        for (node_id, allocation_id) in changes {
            if let Some(log) = nodes.get(node_id) {
                log.lock().dirty.insert(allocation_id);
                if !woken.iter().any(|seen| Arc::ptr_eq(seen, log)) {
                    woken.push(log);
                }
            }
        }
        for log in woken {
            log.wake.notify_one();
        }
    }

    /// Whether a stream for `node_id` is attached to this manager. The
    /// dispatcher skips the best-effort `PostWork` push for such a node:
    /// the stream delivers the placement within a round trip.
    pub fn streaming(&self, node_id: &str) -> bool {
        self.node(node_id)
            .is_some_and(|log| log.lock().session != 0)
    }

    /// Attach a stream for `node_id` that last applied `(last_epoch,
    /// last_seq)`. Replays what it missed when that is still held, and
    /// queues a full resync otherwise. A stream attached for the node
    /// before is superseded: its sender sees a different session and stops.
    pub fn attach(&self, node_id: &str, last_epoch: u64, last_seq: u64) -> Attached {
        let log = self
            .lock_nodes()
            .entry(node_id.to_string())
            .or_default()
            .clone();
        let session = self
            .next_session
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let replay = {
            let mut state = log.lock();
            state.session = session;
            let resumable = last_epoch == self.epoch
                && last_seq <= state.seq
                && (last_seq == state.seq
                    || state
                        .unacked
                        .front()
                        .is_some_and(|oldest| oldest.seq <= last_seq + 1));
            if resumable {
                state
                    .unacked
                    .iter()
                    .filter(|message| message.seq > last_seq)
                    .cloned()
                    .collect()
            } else {
                state.resync = true;
                state.unacked.clear();
                Vec::new()
            }
        };
        log.wake.notify_one();
        Attached {
            log,
            session,
            replay,
        }
    }

    /// Detach `session` from `node_id`, if it is still the attached one.
    /// The node's log stays, so a reconnect can resume.
    pub fn detach(&self, node_id: &str, session: u64) {
        if let Some(log) = self.node(node_id) {
            let mut state = log.lock();
            if state.session == session {
                state.session = 0;
            }
        }
    }

    /// Whether `session` is still the stream attached for the node.
    pub fn is_current(log: &NodeWorkLog, session: u64) -> bool {
        log.lock().session == session
    }

    /// Queue a full resync for the node's stream (its log lost track of
    /// what changed).
    pub fn request_resync(log: &NodeWorkLog) {
        log.lock().resync = true;
        log.wake.notify_one();
    }

    /// The worker applied everything up to `seq`: prune it.
    pub fn ack(&self, log: &NodeWorkLog, epoch: u64, seq: u64) {
        if epoch != self.epoch {
            return;
        }
        let mut state = log.lock();
        while state
            .unacked
            .front()
            .is_some_and(|message| message.seq <= seq)
        {
            state.unacked.pop_front();
        }
    }

    /// Take what the next message must carry, clearing it from the log.
    pub fn take_pending(log: &NodeWorkLog) -> PendingWork {
        let mut state = log.lock();
        if state.resync {
            state.resync = false;
            state.dirty.clear();
            return PendingWork::Resync;
        }
        if state.dirty.is_empty() {
            return PendingWork::Nothing;
        }
        PendingWork::Changed(std::mem::take(&mut state.dirty))
    }

    /// Whether the worker holds `allocation_id` (so its disappearance from
    /// the desired set is a stop to send).
    pub fn holds(log: &NodeWorkLog, allocation_id: i64) -> bool {
        log.lock().held.contains_key(&allocation_id)
    }

    /// Record and number a materialized message: a full set when `resync`,
    /// otherwise upserts and stops. Returns the message to send, or `None`
    /// when it would carry nothing.
    pub fn commit_message(
        &self,
        log: &NodeWorkLog,
        resync: bool,
        upserts: Vec<TaskAllocation>,
        stops: Vec<StopNotice>,
    ) -> Option<StreamWorkReply> {
        let mut state = log.lock();
        if resync {
            state.held = upserts
                .iter()
                .map(|allocation| (allocation.allocation_id, allocation.clone()))
                .collect();
            state.digest = WorkDigest::of(state.held.keys().copied());
        } else {
            if upserts.is_empty() && stops.is_empty() {
                return None;
            }
            for allocation in &upserts {
                if state
                    .held
                    .insert(allocation.allocation_id, allocation.clone())
                    .is_none()
                {
                    state.digest.insert(allocation.allocation_id);
                }
            }
            for stop in &stops {
                if state.held.remove(&stop.allocation_id).is_some() {
                    state.digest.remove(stop.allocation_id);
                }
            }
        }
        state.seq += 1;
        let message = StreamWorkReply {
            epoch: self.epoch,
            seq: state.seq,
            resync,
            upserts,
            stops: if resync { Vec::new() } else { stops },
            digest: state.digest.render(),
        };
        state.unacked.push_back(message.clone());
        if state.unacked.len() > MAX_UNACKED {
            // Too far behind to replay: the next reconnect resyncs.
            state.unacked.pop_front();
        }
        Some(message)
    }

    /// Answer a heartbeat that carries a stream position. The desired set
    /// is left out when the worker's stream is attached here in this
    /// epoch; a digest that disagrees once nothing is in flight queues a
    /// full resync.
    pub fn check_heartbeat(
        &self,
        node_id: &str,
        reported: &WorkStreamState,
    ) -> HeartbeatStreamCheck {
        let Some(log) = self.node(node_id) else {
            return HeartbeatStreamCheck::default();
        };
        let mut state = log.lock();
        if state.session == 0 || reported.epoch != self.epoch {
            return HeartbeatStreamCheck::default();
        }
        let settled = reported.seq == state.seq && !state.resync && state.dirty.is_empty();
        let resync = settled && reported.digest != state.digest.render();
        if resync {
            state.resync = true;
        }
        drop(state);
        if resync {
            log.wake.notify_one();
        }
        HeartbeatStreamCheck {
            via_stream: true,
            resync,
        }
    }

    /// The payloads the worker holds, for the heartbeat feeds computed from
    /// the desired set (connect endpoints, network policy). Only cloned when
    /// `wanted` says one of them needs it.
    pub fn held_payloads(
        &self,
        node_id: &str,
        wanted: impl Fn(&TaskAllocation) -> bool,
    ) -> Vec<TaskAllocation> {
        let Some(log) = self.node(node_id) else {
            return Vec::new();
        };
        let state = log.lock();
        if !state.held.values().any(&wanted) {
            return Vec::new();
        }
        state.held.values().cloned().collect()
    }
}

#[cfg(test)]
mod work_log_test {
    use super::*;

    fn payload(id: i64) -> TaskAllocation {
        TaskAllocation {
            allocation_id: id,
            node_id: "n1".into(),
            ..Default::default()
        }
    }

    #[test]
    fn digest_is_order_independent_and_updates_per_id() {
        let mut incremental = WorkDigest::default();
        incremental.insert(3);
        incremental.insert(1);
        incremental.insert(2);
        incremental.remove(3);
        assert_eq!(incremental, WorkDigest::of([2, 1]));
        assert_ne!(WorkDigest::of([1, 2]), WorkDigest::of([1, 3]));
        assert_eq!(WorkDigest::of([]).render(), "0:0");
    }

    #[test]
    fn a_fresh_worker_resyncs_then_receives_deltas_in_sequence() {
        let log = WorkLog::new();
        let attached = log.attach("n1", 0, 0);
        assert!(attached.replay.is_empty());
        assert!(matches!(
            WorkLog::take_pending(&attached.log),
            PendingWork::Resync
        ));
        let first = log
            .commit_message(&attached.log, true, vec![payload(1)], Vec::new())
            .expect("resync is always sent");
        assert_eq!((first.seq, first.resync), (1, true));

        log.note_changes([("n1", 2), ("n2", 9)]);
        let PendingWork::Changed(changed) = WorkLog::take_pending(&attached.log) else {
            panic!("a change on the node is pending");
        };
        assert_eq!(changed.into_iter().collect::<Vec<_>>(), vec![2]);
        let second = log
            .commit_message(&attached.log, false, vec![payload(2)], Vec::new())
            .expect("an upsert");
        assert_eq!(second.seq, 2);
        assert_eq!(second.digest, WorkDigest::of([1, 2]).render());
        let third = log
            .commit_message(
                &attached.log,
                false,
                Vec::new(),
                vec![StopNotice {
                    allocation_id: 1,
                    cause: "stopped".into(),
                    ..Default::default()
                }],
            )
            .expect("a stop");
        assert_eq!(third.digest, WorkDigest::of([2]).render());
        assert!(log
            .commit_message(&attached.log, false, Vec::new(), Vec::new())
            .is_none());
    }

    #[test]
    fn a_reconnect_in_the_same_epoch_replays_what_was_not_acked() {
        let log = WorkLog::new();
        let attached = log.attach("n1", 0, 0);
        let _ = WorkLog::take_pending(&attached.log);
        log.commit_message(&attached.log, true, vec![payload(1)], Vec::new());
        log.commit_message(&attached.log, false, vec![payload(2)], Vec::new());
        log.commit_message(&attached.log, false, vec![payload(3)], Vec::new());
        log.ack(&attached.log, log.epoch(), 1);
        log.detach("n1", attached.session);
        assert!(!log.streaming("n1"));

        let again = log.attach("n1", log.epoch(), 2);
        assert_eq!(
            again.replay.iter().map(|m| m.seq).collect::<Vec<_>>(),
            vec![3]
        );
        assert!(matches!(
            WorkLog::take_pending(&again.log),
            PendingWork::Nothing
        ));
        assert!(log.streaming("n1"));
    }

    #[test]
    fn another_epoch_a_gap_or_a_regression_resyncs() {
        let log = WorkLog::new();
        let attached = log.attach("n1", 0, 0);
        let _ = WorkLog::take_pending(&attached.log);
        for id in 1..=3 {
            log.commit_message(&attached.log, id == 1, vec![payload(id)], Vec::new());
        }
        log.ack(&attached.log, log.epoch(), 3);
        // Another epoch.
        let other = log.attach("n1", log.epoch().wrapping_add(1), 3);
        assert!(other.replay.is_empty());
        assert!(matches!(
            WorkLog::take_pending(&other.log),
            PendingWork::Resync
        ));
        // Ahead of the manager.
        let ahead = log.attach("n1", log.epoch(), 99);
        assert!(matches!(
            WorkLog::take_pending(&ahead.log),
            PendingWork::Resync
        ));
        // Behind what was pruned: messages 1..=3 were acked and dropped.
        log.commit_message(&ahead.log, false, vec![payload(4)], Vec::new());
        log.ack(&ahead.log, log.epoch(), 4);
        let behind = log.attach("n1", log.epoch(), 1);
        assert!(matches!(
            WorkLog::take_pending(&behind.log),
            PendingWork::Resync
        ));
    }

    #[test]
    fn a_heartbeat_digest_mismatch_resyncs_only_once_nothing_is_in_flight() {
        let log = WorkLog::new();
        let attached = log.attach("n1", 0, 0);
        let _ = WorkLog::take_pending(&attached.log);
        log.commit_message(&attached.log, true, vec![payload(1)], Vec::new());
        let agree = WorkStreamState {
            epoch: log.epoch(),
            seq: 1,
            digest: WorkDigest::of([1]).render(),
        };
        assert_eq!(
            log.check_heartbeat("n1", &agree),
            HeartbeatStreamCheck {
                via_stream: true,
                resync: false
            }
        );
        // Behind: a message is in flight, so a different digest is expected.
        log.commit_message(&attached.log, false, vec![payload(2)], Vec::new());
        assert!(!log.check_heartbeat("n1", &agree).resync);
        // Caught up but wrong.
        let drifted = WorkStreamState {
            epoch: log.epoch(),
            seq: 2,
            digest: WorkDigest::of([1]).render(),
        };
        assert!(log.check_heartbeat("n1", &drifted).resync);
        assert!(matches!(
            WorkLog::take_pending(&attached.log),
            PendingWork::Resync
        ));
        // Another epoch, or no stream: the full desired set as today.
        let stale = WorkStreamState {
            epoch: log.epoch() ^ 1,
            ..drifted
        };
        assert!(!log.check_heartbeat("n1", &stale).via_stream);
        log.detach("n1", attached.session);
        assert!(!log.check_heartbeat("n1", &agree).via_stream);
    }
}
