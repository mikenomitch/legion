//! The content location index: which node holds which content, by digest.
//!
//! Suspend plan §6.8, amendment 2. Workers advertise what their node content
//! store holds (`crate::content_store`) on the heartbeat, as deltas against
//! an epoch, and the manager folds them into `digest -> {node: bytes}` here.
//! The scheduler's `locality` score term reads it to prefer nodes that
//! already hold a placement's bytes.
//!
//! Like node liveness ([`crate::storage::liveness`]) this is leader-local
//! soft state: nothing here is persisted or replicated. A manager that starts
//! (or takes over) with an empty index asks each worker for a full report the
//! first time it hears from it, and until that report lands the node simply
//! scores 0 on locality. Losing the index costs placement quality for a few
//! heartbeats, never correctness.
//!
//! # The protocol
//!
//! Every report carries the worker's `epoch` after the report. A delta also
//! carries `base_epoch`, the epoch it applies on top of:
//!
//! - **Full** (`full = true`): replace the node's set, take its epoch.
//! - **Delta** whose `base_epoch` matches what this manager holds: apply
//!   `added` / `removed`, take the new epoch. A report with no entries and
//!   `base_epoch == epoch` is the steady-state "still at epoch N" probe.
//! - **Anything else** (an unknown node, an epoch gap after a lost reply, a
//!   manager restart or failover): ignore it and answer `resync`, and the
//!   worker sends a full report on its next heartbeat.

use crate::structs::{ContentLocation, ContentLocationReport, SnapshotBudget};
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Only content at least this large is advertised. Small blobs download in
/// well under a heartbeat, so steering placement for them buys nothing and
/// costs index memory. Workers filter before sending, and the manager filters
/// again so a misbehaving worker cannot flood the index.
pub const MIN_ADVERTISED_BYTES: u64 = 1024 * 1024;

/// A node silent this long is dropped from the index. Matches the usage
/// table's staleness window: twelve missed report intervals. A node that
/// comes back gets a resync on its first heartbeat.
pub const CONTENT_INDEX_STALE_AFTER: Duration = Duration::from_secs(120);

/// How often the heartbeat path sweeps stale nodes.
const SWEEP_EVERY: Duration = Duration::from_secs(60);

/// One node's advertised content.
#[derive(Debug, Clone)]
struct NodeContent {
    epoch: u64,
    digests: HashMap<String, u64>,
    last_seen: Instant,
}

#[derive(Debug, Default)]
struct IndexInner {
    nodes: HashMap<String, NodeContent>,
    /// The reverse map the scheduler reads: digest -> node -> bytes.
    by_digest: HashMap<String, HashMap<String, u64>>,
    /// Each node's `snapshots` budget class as last reported (Track 2,
    /// "Capacity for resumes"). Soft state like the rest: a node missing
    /// here is treated as having room.
    snapshot_budgets: HashMap<String, SnapshotBudget>,
    last_sweep: Option<Instant>,
}

impl IndexInner {
    fn insert(&mut self, node_id: &str, digest: &str, bytes: u64) {
        self.by_digest
            .entry(digest.to_string())
            .or_default()
            .insert(node_id.to_string(), bytes);
    }

    fn remove(&mut self, node_id: &str, digest: &str) {
        if let Some(holders) = self.by_digest.get_mut(digest) {
            holders.remove(node_id);
            if holders.is_empty() {
                self.by_digest.remove(digest);
            }
        }
    }

    fn forget(&mut self, node_id: &str) -> bool {
        self.snapshot_budgets.remove(node_id);
        let Some(node) = self.nodes.remove(node_id) else {
            return false;
        };
        for digest in node.digests.keys() {
            self.remove(node_id, digest);
        }
        true
    }
}

/// One item a placement needs on its node: a content digest and, when the
/// caller knows it, its size. Pinned artifacts know only the digest (the size
/// comes from whichever node advertises it). Snapshot restore manifests know
/// both.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContentRef {
    /// `sha256:<hex>`, the node content store's key.
    pub digest: String,
    pub bytes: Option<u64>,
}

impl ContentRef {
    pub fn digest(digest: impl Into<String>) -> Self {
        Self {
            digest: digest.into(),
            bytes: None,
        }
    }

    pub fn sized(digest: impl Into<String>, bytes: u64) -> Self {
        Self {
            digest: digest.into(),
            bytes: Some(bytes),
        }
    }
}

/// Per-node locality for one placement: the fraction of the content set's
/// bytes each node already holds, in `0.0..=1.0`. Nodes absent from the map
/// score 0, which is also what a node that has not reported yet scores.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LocalityScores {
    scores: HashMap<String, f64>,
    /// Bytes of the content set the index could size. Content no node holds
    /// and the caller did not size cannot tell nodes apart, so it is left
    /// out of both sides of the fraction.
    pub total_bytes: u64,
}

impl LocalityScores {
    pub fn score(&self, node_id: &str) -> f64 {
        self.scores.get(node_id).copied().unwrap_or(0.0)
    }

    /// Nodes holding at least some of the content set.
    pub fn warm_nodes(&self) -> usize {
        self.scores.len()
    }

    /// For tests and benchmarks that build scores without an index.
    pub fn from_scores(scores: HashMap<String, f64>, total_bytes: u64) -> Self {
        Self {
            scores,
            total_bytes,
        }
    }
}

/// What [`ContentLocationIndex::ingest`] decided about a report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    /// A full report replaced the node's set.
    Replaced,
    /// A delta applied (or a probe matched).
    Applied,
    /// The report could not be applied; ask the worker for a full report.
    Resync,
}

/// `digest -> {node: bytes}`, in memory, off raft. Owned by
/// [`crate::storage::store::Store`] (shared across clones) so the heartbeat
/// handler writes it and every `Scheduler` reads it.
#[derive(Debug, Default)]
pub struct ContentLocationIndex {
    inner: RwLock<IndexInner>,
}

impl ContentLocationIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one heartbeat's report in. See the module docs for the protocol.
    pub fn ingest(
        &self,
        node_id: &str,
        report: &ContentLocationReport,
        now: Instant,
    ) -> IngestOutcome {
        let mut inner = self.inner.write().unwrap_or_else(|p| p.into_inner());
        let outcome = if report.full {
            inner.forget(node_id);
            let mut digests = HashMap::with_capacity(report.added.len());
            for ContentLocation { digest, bytes } in &report.added {
                if *bytes >= MIN_ADVERTISED_BYTES {
                    digests.insert(digest.clone(), *bytes);
                }
            }
            for (digest, bytes) in &digests {
                inner.insert(node_id, digest, *bytes);
            }
            inner.nodes.insert(
                node_id.to_string(),
                NodeContent {
                    epoch: report.epoch,
                    digests,
                    last_seen: now,
                },
            );
            IngestOutcome::Replaced
        } else {
            let matches = inner
                .nodes
                .get(node_id)
                .is_some_and(|node| node.epoch == report.base_epoch);
            if !matches {
                IngestOutcome::Resync
            } else {
                let mut added = Vec::new();
                let mut removed = Vec::new();
                {
                    let node = inner.nodes.get_mut(node_id).expect("checked above");
                    node.epoch = report.epoch;
                    node.last_seen = now;
                    for digest in &report.removed {
                        if node.digests.remove(digest).is_some() {
                            removed.push(digest.clone());
                        }
                    }
                    for ContentLocation { digest, bytes } in &report.added {
                        if *bytes >= MIN_ADVERTISED_BYTES {
                            node.digests.insert(digest.clone(), *bytes);
                            added.push((digest.clone(), *bytes));
                        }
                    }
                }
                for digest in &removed {
                    inner.remove(node_id, digest);
                }
                for (digest, bytes) in &added {
                    inner.insert(node_id, digest, *bytes);
                }
                IngestOutcome::Applied
            }
        };
        // The budget is a fresh reading, not a delta: take it whatever the
        // epoch outcome, so a node needing a resync still reports headroom.
        match report.snapshot_budget {
            Some(budget) => {
                inner.snapshot_budgets.insert(node_id.to_string(), budget);
            }
            None => {
                inner.snapshot_budgets.remove(node_id);
            }
        }
        Self::sweep_locked(&mut inner, now);
        outcome
    }

    /// A node's `snapshots` budget class as it last reported it.
    pub fn snapshot_budget(&self, node_id: &str) -> Option<SnapshotBudget> {
        self.inner
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .snapshot_budgets
            .get(node_id)
            .copied()
    }

    /// Every node whose `snapshots` budget has less room than
    /// `needed_bytes` (see [`Self::snapshot_budget_short`]), read once so the
    /// scheduler filters a task's candidates with set lookups instead of a
    /// lock per node. Empty, and so free to apply, when no node is short.
    pub fn snapshot_budget_short_nodes(&self, needed_bytes: u64) -> HashSet<String> {
        self.inner
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .snapshot_budgets
            .iter()
            .filter(|(_, budget)| {
                budget.budget_bytes.saturating_sub(budget.pinned_bytes) < needed_bytes
            })
            .map(|(node_id, _)| node_id.clone())
            .collect()
    }

    /// Whether a node's `snapshots` budget has less room than `needed_bytes`
    /// for a new checkpoint: the budget minus what is pinned (pinned
    /// content cannot be evicted to make room). A node that has not reported
    /// a budget has room, because missing data is not evidence of a full
    /// disk (the same rule the scheduler's disk fit uses).
    pub fn snapshot_budget_short(&self, node_id: &str, needed_bytes: u64) -> bool {
        self.snapshot_budget(node_id).is_some_and(|budget| {
            budget.budget_bytes.saturating_sub(budget.pinned_bytes) < needed_bytes
        })
    }

    /// Drop a node's entries (deregistered, or gone silent).
    pub fn forget_node(&self, node_id: &str) -> bool {
        self.inner
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .forget(node_id)
    }

    /// Drop every entry. A manager that loses leadership calls nothing here;
    /// this exists for tests that model a restart.
    pub fn clear(&self) {
        *self.inner.write().unwrap_or_else(|p| p.into_inner()) = IndexInner::default();
    }

    fn sweep_locked(inner: &mut IndexInner, now: Instant) {
        if inner
            .last_sweep
            .is_some_and(|last| now.duration_since(last) < SWEEP_EVERY)
        {
            return;
        }
        inner.last_sweep = Some(now);
        let stale: Vec<String> = inner
            .nodes
            .iter()
            .filter(|(_, node)| now.duration_since(node.last_seen) >= CONTENT_INDEX_STALE_AFTER)
            .map(|(id, _)| id.clone())
            .collect();
        for node_id in stale {
            inner.forget(&node_id);
        }
    }

    /// The epoch this manager holds for a node, when it has its report.
    pub fn node_epoch(&self, node_id: &str) -> Option<u64> {
        self.inner
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .nodes
            .get(node_id)
            .map(|node| node.epoch)
    }

    /// Bytes of `digest` a node advertises, if any.
    pub fn bytes_on(&self, digest: &str, node_id: &str) -> Option<u64> {
        self.inner
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .by_digest
            .get(digest)?
            .get(node_id)
            .copied()
    }

    /// Everything one node advertises, with its epoch, when this manager
    /// holds its report (GetNodeContent). Unordered.
    pub fn node_entries(&self, node_id: &str) -> Option<(u64, Vec<(String, u64)>)> {
        let inner = self.inner.read().unwrap_or_else(|p| p.into_inner());
        let node = inner.nodes.get(node_id)?;
        Some((
            node.epoch,
            node.digests
                .iter()
                .map(|(digest, bytes)| (digest.clone(), *bytes))
                .collect(),
        ))
    }

    /// How many nodes advertise `digest`.
    pub fn holder_count(&self, digest: &str) -> usize {
        self.inner
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .by_digest
            .get(digest)
            .map_or(0, HashMap::len)
    }

    /// For a set of digests (one snapshot's parts), the nodes holding any of
    /// them: `node -> (parts held, bytes held)`.
    pub fn holders_of(&self, digests: &[&str]) -> HashMap<String, (u32, u64)> {
        let inner = self.inner.read().unwrap_or_else(|p| p.into_inner());
        let mut held: HashMap<String, (u32, u64)> = HashMap::new();
        for digest in digests {
            for (node_id, bytes) in inner.by_digest.get(*digest).into_iter().flatten() {
                let entry = held.entry(node_id.clone()).or_default();
                entry.0 += 1;
                entry.1 = entry.1.saturating_add(*bytes);
            }
        }
        held
    }

    /// Nodes in the index, and distinct digests across them.
    pub fn size(&self) -> (usize, usize) {
        let inner = self.inner.read().unwrap_or_else(|p| p.into_inner());
        (inner.nodes.len(), inner.by_digest.len())
    }

    /// Locality for one placement's content set, or `None` when the term
    /// should be skipped: an empty set, or one whose bytes nothing can size
    /// (no node holds any of it and the caller gave no sizes), which would
    /// score every node 0 anyway.
    ///
    /// Cost is one hash probe per content item plus one map write per
    /// (item, holder) pair. It never walks the candidate list: candidates
    /// look their score up afterwards.
    pub fn locality_scores(&self, content: &[ContentRef]) -> Option<LocalityScores> {
        if content.is_empty() {
            return None;
        }
        let inner = self.inner.read().unwrap_or_else(|p| p.into_inner());
        let mut total: u64 = 0;
        let mut present: HashMap<&str, u64> = HashMap::new();
        for item in content {
            let holders = inner.by_digest.get(&item.digest);
            // The caller's size wins. Otherwise any holder's size will do:
            // the bytes are content addressed, so every holder agrees.
            let size = item
                .bytes
                .or_else(|| holders.and_then(|h| h.values().next().copied()));
            let Some(size) = size else { continue };
            total = total.saturating_add(size);
            if let Some(holders) = holders {
                for node_id in holders.keys() {
                    *present.entry(node_id.as_str()).or_default() += size;
                }
            }
        }
        if total == 0 {
            return None;
        }
        let scores = present
            .into_iter()
            .map(|(node_id, bytes)| (node_id.to_string(), (bytes as f64 / total as f64).min(1.0)))
            .collect();
        Some(LocalityScores {
            scores,
            total_bytes: total,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn loc(digest: &str, bytes: u64) -> ContentLocation {
        ContentLocation {
            digest: digest.to_string(),
            bytes,
        }
    }

    fn full(epoch: u64, added: Vec<ContentLocation>) -> ContentLocationReport {
        ContentLocationReport {
            epoch,
            base_epoch: 0,
            full: true,
            added,
            removed: Vec::new(),
            snapshot_budget: None,
        }
    }

    fn delta(
        base_epoch: u64,
        epoch: u64,
        added: Vec<ContentLocation>,
        removed: Vec<&str>,
    ) -> ContentLocationReport {
        ContentLocationReport {
            epoch,
            base_epoch,
            full: false,
            added,
            removed: removed.into_iter().map(str::to_string).collect(),
            snapshot_budget: None,
        }
    }

    /// The read side GetNodeContent and GetSnapshot use: one node's entries
    /// with its epoch, holder counts, and per-node part totals for a set.
    #[test]
    fn node_entries_and_holders_read_back_what_was_ingested() {
        let index = ContentLocationIndex::new();
        let now = Instant::now();
        index.ingest(
            "w-1",
            &full(1, vec![loc("a", 2 * MIB), loc("b", 3 * MIB)]),
            now,
        );
        index.ingest("w-2", &full(1, vec![loc("a", 2 * MIB)]), now);

        let (epoch, mut entries) = index.node_entries("w-1").expect("w-1 reported");
        entries.sort();
        assert_eq!(epoch, 1);
        assert_eq!(entries, vec![("a".into(), 2 * MIB), ("b".into(), 3 * MIB)]);
        assert!(index.node_entries("w-9").is_none());

        assert_eq!(index.holder_count("a"), 2);
        assert_eq!(index.holder_count("b"), 1);
        assert_eq!(index.holder_count("z"), 0);

        let held = index.holders_of(&["a", "b", "z"]);
        assert_eq!(held.get("w-1"), Some(&(2, 5 * MIB)));
        assert_eq!(held.get("w-2"), Some(&(1, 2 * MIB)));
        assert_eq!(held.len(), 2);
    }

    #[test]
    fn the_snapshot_budget_is_taken_on_every_report_and_dropped_with_the_node() {
        const GIB: u64 = 1024 * 1024 * 1024;
        let index = ContentLocationIndex::new();
        let now = Instant::now();
        let budget = SnapshotBudget {
            budget_bytes: 10 * GIB,
            used_bytes: 9 * GIB,
            pinned_bytes: 8 * GIB,
        };
        let mut report = full(1, Vec::new());
        report.snapshot_budget = Some(budget);
        index.ingest("n1", &report, now);
        assert_eq!(index.snapshot_budget("n1"), Some(budget));
        assert!(
            !index.snapshot_budget_short("n1", 2 * GIB),
            "2 GiB fits in 2 GiB of room"
        );
        assert!(index.snapshot_budget_short("n1", 3 * GIB));
        assert!(index.snapshot_budget_short_nodes(2 * GIB).is_empty());
        assert_eq!(
            index.snapshot_budget_short_nodes(3 * GIB),
            HashSet::from(["n1".to_string()])
        );
        assert!(
            !index.snapshot_budget_short("unreported", u64::MAX),
            "no report is not a full budget"
        );

        // A delta this manager cannot apply still updates the budget.
        let mut gap = delta(7, 8, Vec::new(), Vec::new());
        gap.snapshot_budget = Some(SnapshotBudget {
            pinned_bytes: 10 * GIB,
            ..budget
        });
        assert_eq!(index.ingest("n1", &gap, now), IngestOutcome::Resync);
        assert!(index.snapshot_budget_short("n1", 1));

        // A report without one (the store was turned off) clears it.
        index.ingest("n1", &full(2, Vec::new()), now);
        assert_eq!(index.snapshot_budget("n1"), None);
        let mut again = full(3, Vec::new());
        again.snapshot_budget = Some(budget);
        index.ingest("n1", &again, now);
        assert!(index.forget_node("n1"));
        assert_eq!(index.snapshot_budget("n1"), None);
    }

    #[test]
    fn deltas_apply_on_a_matching_epoch_and_resync_otherwise() {
        let index = ContentLocationIndex::new();
        let now = Instant::now();

        // Unknown node: a delta cannot apply, so ask for a full report.
        assert_eq!(
            index.ingest("n1", &delta(0, 1, vec![loc("a", 4 * MIB)], vec![]), now),
            IngestOutcome::Resync
        );
        assert_eq!(index.bytes_on("a", "n1"), None);

        assert_eq!(
            index.ingest("n1", &full(1, vec![loc("a", 4 * MIB)]), now),
            IngestOutcome::Replaced
        );
        assert_eq!(index.bytes_on("a", "n1"), Some(4 * MIB));

        // A delta on top of epoch 1.
        assert_eq!(
            index.ingest("n1", &delta(1, 2, vec![loc("b", 2 * MIB)], vec!["a"]), now),
            IngestOutcome::Applied
        );
        assert_eq!(index.bytes_on("a", "n1"), None);
        assert_eq!(index.bytes_on("b", "n1"), Some(2 * MIB));
        assert_eq!(index.node_epoch("n1"), Some(2));

        // The steady-state probe: no entries, base == epoch.
        assert_eq!(
            index.ingest("n1", &delta(2, 2, vec![], vec![]), now),
            IngestOutcome::Applied
        );

        // A gap (the worker's reply to epoch 3 was lost, it resends 2->3
        // while we hold 2... or we missed one): resync, state untouched.
        assert_eq!(
            index.ingest("n1", &delta(5, 6, vec![loc("c", 2 * MIB)], vec![]), now),
            IngestOutcome::Resync
        );
        assert_eq!(index.bytes_on("c", "n1"), None);
        assert_eq!(index.bytes_on("b", "n1"), Some(2 * MIB));

        // The full report that answers the resync replaces everything.
        assert_eq!(
            index.ingest("n1", &full(7, vec![loc("c", 2 * MIB)]), now),
            IngestOutcome::Replaced
        );
        assert_eq!(index.bytes_on("b", "n1"), None);
        assert_eq!(index.bytes_on("c", "n1"), Some(2 * MIB));
        assert_eq!(index.size(), (1, 1));
    }

    #[test]
    fn small_entries_are_not_indexed() {
        let index = ContentLocationIndex::new();
        let now = Instant::now();
        index.ingest(
            "n1",
            &full(1, vec![loc("small", MIB - 1), loc("big", MIB)]),
            now,
        );
        assert_eq!(index.bytes_on("small", "n1"), None);
        assert_eq!(index.bytes_on("big", "n1"), Some(MIB));
        index.ingest("n1", &delta(1, 2, vec![loc("tiny", 10)], vec![]), now);
        assert_eq!(index.bytes_on("tiny", "n1"), None);
    }

    #[test]
    fn a_restarted_manager_resyncs_every_node() {
        let index = ContentLocationIndex::new();
        let now = Instant::now();
        index.ingest("n1", &full(3, vec![loc("a", 8 * MIB)]), now);
        // Restart or failover: the new leader starts empty.
        index.clear();
        let set = [ContentRef::digest("a")];
        assert_eq!(index.locality_scores(&set), None, "nothing known yet");
        // The worker's next probe cannot match, so it is told to resync...
        assert_eq!(
            index.ingest("n1", &delta(3, 3, vec![], vec![]), now),
            IngestOutcome::Resync
        );
        // ...and its full report restores the node's locality.
        index.ingest("n1", &full(4, vec![loc("a", 8 * MIB)]), now);
        assert_eq!(index.locality_scores(&set).unwrap().score("n1"), 1.0);
    }

    #[test]
    fn locality_is_bytes_present_over_total() {
        let index = ContentLocationIndex::new();
        let now = Instant::now();
        index.ingest(
            "n1",
            &full(1, vec![loc("a", 30 * MIB), loc("b", 10 * MIB)]),
            now,
        );
        index.ingest("n2", &full(1, vec![loc("b", 10 * MIB)]), now);

        let set = [ContentRef::digest("a"), ContentRef::digest("b")];
        let scores = index.locality_scores(&set).expect("scores");
        assert_eq!(scores.total_bytes, 40 * MIB);
        assert_eq!(scores.score("n1"), 1.0);
        assert_eq!(scores.score("n2"), 0.25);
        // A node that never reported scores 0.
        assert_eq!(scores.score("n3"), 0.0);

        // A caller-sized item nobody holds still counts toward the total.
        let set = [ContentRef::digest("a"), ContentRef::sized("z", 30 * MIB)];
        let scores = index.locality_scores(&set).expect("scores");
        assert_eq!(scores.score("n1"), 0.5);

        // Empty sets and unsizeable sets skip the term.
        assert_eq!(index.locality_scores(&[]), None);
        assert_eq!(index.locality_scores(&[ContentRef::digest("nope")]), None);
    }

    #[test]
    fn silent_nodes_are_swept() {
        let index = ContentLocationIndex::new();
        let start = Instant::now();
        index.ingest("n1", &full(1, vec![loc("a", 8 * MIB)]), start);
        index.ingest("n2", &full(1, vec![loc("a", 8 * MIB)]), start);
        // n2 keeps beating; n1 goes quiet past the window.
        let later = start + CONTENT_INDEX_STALE_AFTER + Duration::from_secs(1);
        index.ingest("n2", &delta(1, 1, vec![], vec![]), later);
        assert_eq!(index.bytes_on("a", "n1"), None);
        assert_eq!(index.bytes_on("a", "n2"), Some(8 * MIB));
        assert!(!index.forget_node("n1"));
        assert!(index.forget_node("n2"));
        assert_eq!(index.size(), (0, 0));
    }
}
