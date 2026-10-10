//! Queued snapshot warms: which nodes should pre-fetch which published
//! snapshot's parts (suspend plan §6.8, "Pre-warm"; the WarmSnapshot RPC).
//!
//! Warming is best effort, so the queue is leader-local soft state like the
//! content location index ([`crate::storage::content_index`]): nothing here
//! is persisted or replicated. WarmSnapshot queues an entry per node, the
//! node's next heartbeat carries a `warm` checkpoint request for it, and the
//! node's report clears it. An entry the node never answers (it is down, or
//! heartbeats a different manager) expires after [`WARM_TTL`], and a manager
//! restart drops every entry. Either way the only cost is a node that is not
//! warm, which placement never depends on.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long a queued warm waits for its node to report before it is
/// dropped.
pub const WARM_TTL: Duration = Duration::from_secs(15 * 60);

/// One queued warm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedWarm {
    pub snapshot_id: String,
    /// The published manifest, whose parts carry their URLs.
    pub manifest_json: String,
}

#[derive(Debug, Clone)]
struct Entry {
    manifest_json: String,
    queued_at: Instant,
}

/// Node id -> snapshot id -> queued warm.
#[derive(Debug, Default)]
pub struct SnapshotWarmQueue {
    nodes: Mutex<HashMap<String, HashMap<String, Entry>>>,
}

impl SnapshotWarmQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a warm of `snapshot_id` on `node_id`. Returns false when one is
    /// already queued there (the call is idempotent).
    pub fn queue(
        &self,
        node_id: &str,
        snapshot_id: &str,
        manifest_json: &str,
        now: Instant,
    ) -> bool {
        let mut nodes = self.nodes.lock().expect("snapshot warm queue");
        let queued = nodes.entry(node_id.to_string()).or_default();
        if queued
            .get(snapshot_id)
            .is_some_and(|entry| now.duration_since(entry.queued_at) < WARM_TTL)
        {
            return false;
        }
        queued.insert(
            snapshot_id.to_string(),
            Entry {
                manifest_json: manifest_json.to_string(),
                queued_at: now,
            },
        );
        true
    }

    /// The warms queued for `node_id`, oldest first, dropping expired ones.
    /// Cheap on a node with nothing queued: one map probe.
    pub fn pending_for(&self, node_id: &str, now: Instant) -> Vec<QueuedWarm> {
        let mut nodes = self.nodes.lock().expect("snapshot warm queue");
        let Some(queued) = nodes.get_mut(node_id) else {
            return Vec::new();
        };
        queued.retain(|_, entry| now.duration_since(entry.queued_at) < WARM_TTL);
        let mut pending: Vec<(Instant, QueuedWarm)> = queued
            .iter()
            .map(|(snapshot_id, entry)| {
                (
                    entry.queued_at,
                    QueuedWarm {
                        snapshot_id: snapshot_id.clone(),
                        manifest_json: entry.manifest_json.clone(),
                    },
                )
            })
            .collect();
        if queued.is_empty() {
            nodes.remove(node_id);
        }
        pending.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| a.1.snapshot_id.cmp(&b.1.snapshot_id))
        });
        pending.into_iter().map(|(_, warm)| warm).collect()
    }

    /// The node reported on the warm: forget it. Returns whether it was
    /// queued (a duplicate report finds nothing).
    pub fn complete(&self, node_id: &str, snapshot_id: &str) -> bool {
        let mut nodes = self.nodes.lock().expect("snapshot warm queue");
        let Some(queued) = nodes.get_mut(node_id) else {
            return false;
        };
        let removed = queued.remove(snapshot_id).is_some();
        if queued.is_empty() {
            nodes.remove(node_id);
        }
        removed
    }

    /// Drop every warm of a snapshot (it was deleted).
    pub fn forget_snapshot(&self, snapshot_id: &str) {
        let mut nodes = self.nodes.lock().expect("snapshot warm queue");
        for queued in nodes.values_mut() {
            queued.remove(snapshot_id);
        }
        nodes.retain(|_, queued| !queued.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_warm_is_queued_once_delivered_until_reported_and_expires() {
        let queue = SnapshotWarmQueue::new();
        let now = Instant::now();
        assert!(queue.queue("node-a", "snap-1", "{}", now));
        assert!(
            !queue.queue("node-a", "snap-1", "{}", now),
            "a second warm of the same snapshot on the same node is a no-op"
        );
        assert!(queue.queue("node-b", "snap-1", "{}", now));
        assert!(queue.pending_for("node-c", now).is_empty());

        // Re-sent on every heartbeat until the node reports.
        assert_eq!(queue.pending_for("node-a", now).len(), 1);
        assert_eq!(queue.pending_for("node-a", now).len(), 1);
        assert!(queue.complete("node-a", "snap-1"));
        assert!(!queue.complete("node-a", "snap-1"));
        assert!(queue.pending_for("node-a", now).is_empty());

        // Never reported: dropped after the TTL, and can be queued again.
        let later = now + WARM_TTL + Duration::from_secs(1);
        assert!(queue.pending_for("node-b", later).is_empty());
        assert!(queue.queue("node-b", "snap-1", "{}", later));
        queue.forget_snapshot("snap-1");
        assert!(queue.pending_for("node-b", later).is_empty());
    }
}
