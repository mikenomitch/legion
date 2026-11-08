//! Revisions and the change journal behind blocking reads, `Manager.Watch`,
//! and `StreamWork` resume (revisions-and-watches plan;
//! docs/watches-and-blocking-queries.md).
//!
//! **A revision is a replica's applied position.** In raft mode each
//! component is a raft group's applied log index, which commits in the same
//! transaction as the rows it describes (`raft_applied`), so every manager
//! agrees on what a revision means and a new leader's indexes only grow. In
//! `single` topology there is one group and a revision is one number; under
//! `lanes` there are three (Control, Scheduler, Status) and a revision is
//! written `c.s.t`. In `local_disk` mode it is a store-wide counter
//! persisted in blocks (`store_revision`).
//!
//! **The journal is soft state, per manager.** Every replica's changeset
//! projection records which jobs, allocations, and KV keys a committed entry
//! changed, tagged with the entry's group and index
//! ([`RevisionHub::record`]). The journal holds the newest
//! [`JOURNAL_CAPACITY`] changes; the `horizon` says, per group, the index at
//! or below which changes may be missing (the journal's start when the
//! process started, the newest evicted change, or the position a reset
//! happened at). A reader asking for "changes after R" is served when R is
//! at or above the horizon in every group it cares about, and told to relist
//! otherwise.
//!
//! **Per-node allocation changes** are kept apart, keyed by node, so a
//! worker reconnecting to a manager that is not the one it streamed from can
//! be sent only what changed on its node since the revision it last applied
//! (`StreamWork` resume, ST3), without scanning the global journal once per
//! worker.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Mutex;

/// Raft groups a revision has a component for: Control, Scheduler, Status
/// (`Lane::ALL` order). `single` topology and `local_disk` mode use only the
/// first.
pub const GROUPS: usize = 3;

/// Changes the global journal keeps. Watches and blocking reads that need
/// changes older than this relist (watches) or answer at once (reads).
pub const JOURNAL_CAPACITY: usize = 65_536;

/// Allocation changes kept per node for `StreamWork` resume. A node that
/// changed more than this since a worker's last revision gets a full resync,
/// which is what every reconnect cost before resume existed.
pub const NODE_CAPACITY: usize = 256;

/// How many revisions `local_disk` mode reserves per persisted write: the
/// counter is persisted only when it crosses a block boundary, and a restart
/// resumes from the reserved bound, so it never goes backwards.
pub const LOCAL_BLOCK: u64 = 1024;

/// A replica's applied position: one raft log index per group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Revision(pub [u64; GROUPS]);

impl Revision {
    /// A single-group revision (`single` topology, `local_disk`).
    pub fn single(index: u64) -> Self {
        let mut parts = [0; GROUPS];
        parts[0] = index;
        Revision(parts)
    }

    /// Parse the wire form: `N` (one group) or `c.s.t` (three). Whitespace
    /// around the value is ignored. An empty string is not a revision.
    pub fn parse(text: &str) -> Result<Revision, String> {
        let text = text.trim();
        if text.is_empty() {
            return Err("a revision is empty".to_string());
        }
        let parts: Vec<&str> = text.split('.').collect();
        if parts.len() != 1 && parts.len() != GROUPS {
            return Err(format!(
                "revision {text:?} is not a revision: expect a number (\"4812\") or \
                 {GROUPS} dot-separated numbers (\"4812.90211.120034\"), exactly as a reply \
                 returned it"
            ));
        }
        let mut out = [0u64; GROUPS];
        for (slot, part) in out.iter_mut().zip(parts) {
            *slot = part.parse::<u64>().map_err(|_| {
                format!(
                    "revision {text:?} is not a revision: {part:?} is not a whole number; pass \
                     a revision back exactly as a reply returned it"
                )
            })?;
        }
        Ok(Revision(out))
    }

    /// The wire form: `N` when only the first group has moved, `c.s.t`
    /// otherwise. Parsing the output gives the same revision back.
    pub fn render(&self) -> String {
        if self.0[1..].iter().all(|part| *part == 0) {
            self.0[0].to_string()
        } else {
            self.0
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(".")
        }
    }

    /// Whether this position includes everything `other` does, in every
    /// group.
    pub fn covers(&self, other: &Revision) -> bool {
        self.0.iter().zip(other.0.iter()).all(|(a, b)| a >= b)
    }

    /// Raise `group` to at least `index`.
    pub fn raise(&mut self, group: usize, index: u64) {
        if let Some(slot) = self.0.get_mut(group) {
            *slot = (*slot).max(index);
        }
    }

    /// The component-wise maximum.
    pub fn max(&self, other: &Revision) -> Revision {
        let mut out = *self;
        for (group, index) in other.0.iter().enumerate() {
            out.raise(group, *index);
        }
        out
    }

    /// One group's index.
    pub fn get(&self, group: usize) -> u64 {
        self.0.get(group).copied().unwrap_or(0)
    }
}

impl std::fmt::Display for Revision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.render())
    }
}

/// What kind of object a journal entry names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeKind {
    /// A job's row, versions, allocations, or task outcomes moved: anything
    /// its `GetJobInfo` answer (including the derived status) reads.
    Job,
    /// One allocation row.
    Allocation,
    /// One KV key.
    Kv,
}

impl ChangeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeKind::Job => "job",
            ChangeKind::Allocation => "allocation",
            ChangeKind::Kv => "kv",
        }
    }
}

/// One object a committed entry changed, as the projection saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectChange {
    pub kind: ChangeKind,
    /// The job id, the allocation id (decimal), or the KV key.
    pub key: String,
    /// The owning job, when known: a job's own id, an allocation's job.
    pub job: Option<String>,
    /// The allocation's node.
    pub node: Option<String>,
    /// The row was deleted by this entry.
    pub deleted: bool,
}

/// A journal entry: an [`ObjectChange`] with where it committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// Local arrival order on this manager; watchers resume by it.
    pub seq: u64,
    /// The raft group (or 0) and index the change committed at.
    pub group: usize,
    pub index: u64,
    pub change: ObjectChange,
}

impl Change {
    /// Whether this change committed after `position`.
    pub fn after(&self, position: &Revision) -> bool {
        self.index > position.get(self.group)
    }
}

/// Why changes since a revision cannot be served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TooOld {
    /// The oldest revision this manager can serve changes after.
    pub oldest: Revision,
}

/// A batch of journal entries handed to a live reader.
#[derive(Debug, Clone, Default)]
pub struct Batch {
    pub changes: Vec<Change>,
    /// The last local sequence included; pass it back for the next batch.
    pub last_seq: u64,
    /// Every change at or below this has been recorded (and so is in this
    /// batch or an earlier one).
    pub recorded: Revision,
    /// Bumped by every reset. A reader that saw another generation missed
    /// changes and must resync.
    pub generation: u64,
}

#[derive(Debug, Default)]
struct NodeChanges {
    /// Changes at or below this index (in the allocation group) may be
    /// missing for this node.
    horizon: u64,
    entries: VecDeque<(u64, i64)>,
}

#[derive(Debug, Default)]
struct HubState {
    journal: VecDeque<Change>,
    next_seq: u64,
    horizon: Revision,
    recorded: Revision,
    generation: u64,
    /// The group allocation rows commit in (0 in `single`/`local_disk`,
    /// the Scheduler group under `lanes`), as the last record saw it.
    alloc_group: usize,
    nodes: HashMap<String, NodeChanges>,
    /// Allocation-group index at or below which no node's changes can be
    /// served ([`RevisionHub::forget_nodes`]).
    node_floor: u64,
    /// `local_disk` mode: the counter, and the bound persisted so far.
    local: u64,
    local_reserved: u64,
}

/// The journal, its horizons, and the wake for readers.
#[derive(Debug)]
pub struct RevisionHub {
    state: Mutex<HubState>,
    /// Held while the projection publishes an entry's work-log notes and
    /// journal record together, and while a reader takes a position that
    /// must agree with the work log (`StreamWork` tagging and resume).
    publish: Mutex<()>,
    /// Carries the newest local sequence; every record and reset sends.
    wake: tokio::sync::watch::Sender<u64>,
    capacity: usize,
    node_capacity: usize,
}

impl Default for RevisionHub {
    fn default() -> Self {
        Self::new()
    }
}

impl RevisionHub {
    pub fn new() -> Self {
        Self::with_capacity(JOURNAL_CAPACITY, NODE_CAPACITY)
    }

    pub fn with_capacity(capacity: usize, node_capacity: usize) -> Self {
        let (wake, _) = tokio::sync::watch::channel(0);
        RevisionHub {
            state: Mutex::new(HubState::default()),
            publish: Mutex::new(()),
            wake,
            capacity: capacity.max(1),
            node_capacity: node_capacity.max(1),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HubState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The publish lock (see the field). Never take it while holding a
    /// work-log lock: the projection takes it first.
    pub fn publishing(&self) -> std::sync::MutexGuard<'_, ()> {
        self.publish.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Readers wait on this; it changes on every record and reset.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.wake.subscribe()
    }

    /// Start (or restart) the journal at `at`: everything at or below it is
    /// outside the window, every node's changes are forgotten, and every
    /// live reader sees a new generation. Called when the store opens, after
    /// a snapshot install replaced rows under the projection, and after a
    /// projection failed part way.
    pub fn reset(&self, at: Revision) {
        let seq = {
            let mut state = self.lock();
            state.journal.clear();
            state.nodes.clear();
            state.horizon = state.horizon.max(&at);
            state.recorded = state.recorded.max(&at);
            state.generation += 1;
            state.next_seq
        };
        self.wake.send_replace(seq);
    }

    /// Record the objects one committed entry changed, at `(group, index)`,
    /// and which allocations it changed on which nodes (`node_allocations`;
    /// `unknown_nodes` are nodes whose allocation changes could not be named,
    /// so a resume across this index must resync them). `alloc_group` is the
    /// group allocation rows commit in on this replica.
    pub fn record(
        &self,
        group: usize,
        index: u64,
        alloc_group: usize,
        changes: Vec<ObjectChange>,
        node_allocations: &[(String, i64)],
        unknown_nodes: &[String],
    ) {
        let seq = {
            let mut state = self.lock();
            state.alloc_group = alloc_group;
            for change in changes {
                state.next_seq += 1;
                let seq = state.next_seq;
                state.journal.push_back(Change {
                    seq,
                    group,
                    index,
                    change,
                });
            }
            while state.journal.len() > self.capacity {
                if let Some(evicted) = state.journal.pop_front() {
                    state.horizon.raise(evicted.group, evicted.index);
                }
            }
            if group == alloc_group {
                let floor = state.horizon.get(alloc_group).max(state.node_floor);
                for (node, allocation) in node_allocations {
                    let entry = state
                        .nodes
                        .entry(node.clone())
                        .or_insert_with(|| NodeChanges {
                            horizon: floor,
                            entries: VecDeque::new(),
                        });
                    entry.entries.push_back((index, *allocation));
                    while entry.entries.len() > self.node_capacity {
                        if let Some((evicted, _)) = entry.entries.pop_front() {
                            entry.horizon = entry.horizon.max(evicted);
                        }
                    }
                }
                for node in unknown_nodes {
                    let entry = state
                        .nodes
                        .entry(node.clone())
                        .or_insert_with(|| NodeChanges {
                            horizon: floor,
                            entries: VecDeque::new(),
                        });
                    entry.horizon = entry.horizon.max(index);
                }
            }
            state.recorded.raise(group, index);
            state.next_seq
        };
        self.wake.send_replace(seq);
    }

    /// Every change that committed after `since`, when the journal still
    /// holds them all for `groups` (the groups the caller's objects commit
    /// in), with the sequence to continue from.
    pub fn changes_since(&self, since: &Revision, groups: &[usize]) -> Result<Batch, TooOld> {
        let state = self.lock();
        if groups
            .iter()
            .any(|group| since.get(*group) < state.horizon.get(*group))
        {
            return Err(TooOld {
                oldest: state.horizon,
            });
        }
        Ok(Batch {
            changes: state
                .journal
                .iter()
                .filter(|change| change.after(since))
                .cloned()
                .collect(),
            last_seq: state.next_seq,
            recorded: state.recorded,
            generation: state.generation,
        })
    }

    /// Changes recorded after local sequence `after_seq`, for a reader that
    /// is already caught up to it. `missed` is set when the journal evicted
    /// some of them before the reader came back for them.
    pub fn changes_after(&self, after_seq: u64) -> (Batch, bool) {
        let state = self.lock();
        let missed = state
            .journal
            .front()
            .is_some_and(|oldest| oldest.seq > after_seq + 1)
            && after_seq < state.next_seq;
        let changes = state
            .journal
            .iter()
            .filter(|change| change.seq > after_seq)
            .cloned()
            .collect();
        (
            Batch {
                changes,
                last_seq: state.next_seq,
                recorded: state.recorded,
                generation: state.generation,
            },
            missed,
        )
    }

    /// Where a reader starting now begins: the newest sequence, what has
    /// been recorded, and the generation.
    pub fn position(&self) -> Batch {
        let state = self.lock();
        Batch {
            changes: Vec::new(),
            last_seq: state.next_seq,
            recorded: state.recorded,
            generation: state.generation,
        }
    }

    /// The oldest revision this manager can serve changes after.
    pub fn horizon(&self) -> Revision {
        self.lock().horizon
    }

    /// Everything recorded so far.
    pub fn recorded(&self) -> Revision {
        self.lock().recorded
    }

    /// The allocations that changed on `node` after `since`, when this
    /// manager still holds them all; `None` when a resume from `since` must
    /// resync instead (the window rolled, the process restarted, or a
    /// change on the node could not be named).
    pub fn node_changes_since(&self, node: &str, since: &Revision) -> Option<BTreeSet<i64>> {
        let state = self.lock();
        let group = state.alloc_group;
        let since = since.get(group);
        let horizon = state
            .nodes
            .get(node)
            .map(|entry| entry.horizon)
            .unwrap_or(0)
            .max(state.horizon.get(group))
            .max(state.node_floor);
        if since < horizon {
            return None;
        }
        Some(
            state
                .nodes
                .get(node)
                .map(|entry| {
                    entry
                        .entries
                        .iter()
                        .filter(|(index, _)| *index > since)
                        .map(|(_, allocation)| *allocation)
                        .collect()
                })
                .unwrap_or_default(),
        )
    }

    /// Forget every node's changes: a change could not be attributed to any
    /// node, so no resume across `at` can be served.
    pub fn forget_nodes(&self, at: Revision) {
        let mut state = self.lock();
        let group = state.alloc_group;
        state.nodes.clear();
        state.node_floor = state.node_floor.max(at.get(group));
    }

    /// `local_disk` mode: start the counter from the persisted bound.
    pub fn start_local(&self, reserved: u64) {
        let mut state = self.lock();
        state.local = state.local.max(reserved);
        state.local_reserved = state.local_reserved.max(reserved);
    }

    /// `local_disk` mode: the next revision, and the bound to persist first
    /// when it crosses the reserved block (`None` otherwise).
    pub fn next_local(&self) -> (u64, Option<u64>) {
        let mut state = self.lock();
        state.local += 1;
        let persist = if state.local > state.local_reserved {
            state.local_reserved = state.local + LOCAL_BLOCK;
            Some(state.local_reserved)
        } else {
            None
        };
        (state.local, persist)
    }

    /// `local_disk` mode: the current revision.
    pub fn local(&self) -> u64 {
        self.lock().local
    }
}

#[cfg(test)]
mod revisions_test {
    use super::*;

    fn job(key: &str) -> ObjectChange {
        ObjectChange {
            kind: ChangeKind::Job,
            key: key.into(),
            job: Some(key.into()),
            node: None,
            deleted: false,
        }
    }

    #[test]
    fn revisions_round_trip_and_compare_by_component() {
        assert_eq!(Revision::parse("4812").unwrap(), Revision::single(4812));
        assert_eq!(Revision::single(4812).render(), "4812");
        let lanes = Revision::parse(" 1.2.3 ").unwrap();
        assert_eq!(lanes, Revision([1, 2, 3]));
        assert_eq!(lanes.render(), "1.2.3");
        assert_eq!(Revision([7, 0, 0]).render(), "7");
        assert!(Revision::parse("").is_err());
        assert!(Revision::parse("1.2").is_err());
        assert!(Revision::parse("x").is_err());
        assert!(Revision::parse("-1").is_err());
        assert!(Revision([3, 3, 3]).covers(&Revision([3, 2, 1])));
        assert!(!Revision([3, 3, 3]).covers(&Revision([1, 4, 1])));
        assert_eq!(
            Revision([1, 5, 2]).max(&Revision([4, 0, 3])),
            Revision([4, 5, 3])
        );
    }

    #[test]
    fn changes_since_filters_per_group_and_refuses_what_rolled_out() {
        let hub = RevisionHub::with_capacity(3, 8);
        hub.reset(Revision([10, 20, 0]));
        hub.record(0, 11, 1, vec![job("a")], &[], &[]);
        hub.record(1, 21, 1, vec![job("b")], &[], &[]);
        hub.record(0, 12, 1, vec![job("c")], &[], &[]);

        let batch = hub.changes_since(&Revision([11, 20, 0]), &[0, 1]).unwrap();
        let keys: Vec<_> = batch
            .changes
            .iter()
            .map(|c| c.change.key.as_str())
            .collect();
        assert_eq!(keys, vec!["b", "c"]);
        assert_eq!(batch.recorded, Revision([12, 21, 0]));

        // Older than the start of the window.
        match hub.changes_since(&Revision([9, 20, 0]), &[0]) {
            Err(too_old) => assert_eq!(too_old.oldest, Revision([10, 20, 0])),
            Ok(_) => panic!("a revision below the window must be refused"),
        }
        // A group the caller does not care about is not checked.
        assert!(hub.changes_since(&Revision([10, 0, 0]), &[0]).is_ok());

        // Eviction moves the horizon of the evicted change's group.
        hub.record(0, 13, 1, vec![job("d")], &[], &[]);
        assert_eq!(hub.horizon(), Revision([11, 20, 0]));
        assert!(hub.changes_since(&Revision([10, 20, 0]), &[0]).is_err());
        assert!(hub.changes_since(&Revision([11, 20, 0]), &[0]).is_ok());
    }

    #[test]
    fn live_readers_continue_by_sequence_and_see_resets() {
        let hub = RevisionHub::with_capacity(2, 8);
        let start = hub.position();
        hub.record(0, 1, 0, vec![job("a")], &[], &[]);
        let (batch, missed) = hub.changes_after(start.last_seq);
        assert!(!missed);
        assert_eq!(batch.changes.len(), 1);
        let (empty, _) = hub.changes_after(batch.last_seq);
        assert!(empty.changes.is_empty());

        // A reader that fell behind the window is told it missed some.
        hub.record(0, 2, 0, vec![job("b")], &[], &[]);
        hub.record(0, 3, 0, vec![job("c")], &[], &[]);
        hub.record(0, 4, 0, vec![job("d")], &[], &[]);
        let (_, missed) = hub.changes_after(batch.last_seq);
        assert!(missed);

        let before = hub.position().generation;
        hub.reset(Revision::single(9));
        assert_eq!(hub.position().generation, before + 1);
        assert_eq!(hub.horizon(), Revision::single(9));
    }

    #[test]
    fn node_changes_resume_only_inside_the_nodes_window() {
        let hub = RevisionHub::with_capacity(16, 2);
        hub.reset(Revision::single(100));
        hub.record(0, 101, 0, Vec::new(), &[("n1".into(), 7)], &[]);
        hub.record(0, 102, 0, Vec::new(), &[("n2".into(), 8)], &[]);
        hub.record(0, 103, 0, Vec::new(), &[("n1".into(), 9)], &[]);

        let n1: Vec<i64> = hub
            .node_changes_since("n1", &Revision::single(101))
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(n1, vec![9]);
        assert_eq!(
            hub.node_changes_since("n3", &Revision::single(100)),
            Some(BTreeSet::new()),
            "a node with no changes since the window opened has nothing to resend"
        );
        assert_eq!(
            hub.node_changes_since("n1", &Revision::single(99)),
            None,
            "older than the window"
        );

        // A third change on n1 evicts its oldest: 101 is no longer servable.
        hub.record(0, 104, 0, Vec::new(), &[("n1".into(), 10)], &[]);
        assert_eq!(hub.node_changes_since("n1", &Revision::single(100)), None);
        assert!(hub
            .node_changes_since("n1", &Revision::single(101))
            .is_some());

        // An unattributable change on n2 makes a resume across it resync.
        hub.record(0, 105, 0, Vec::new(), &[], &["n2".into()]);
        assert_eq!(hub.node_changes_since("n2", &Revision::single(104)), None);
        assert!(hub
            .node_changes_since("n2", &Revision::single(105))
            .is_some());

        hub.forget_nodes(Revision::single(106));
        assert_eq!(hub.node_changes_since("n1", &Revision::single(105)), None);
        assert!(hub
            .node_changes_since("n1", &Revision::single(106))
            .is_some());
    }

    #[test]
    fn the_local_counter_never_goes_backwards_across_a_restart() {
        let hub = RevisionHub::new();
        hub.start_local(0);
        let (first, persist) = hub.next_local();
        assert_eq!(first, 1);
        assert_eq!(persist, Some(1 + LOCAL_BLOCK));
        let (second, persist) = hub.next_local();
        assert_eq!((second, persist), (2, None));

        // A restart resumes from the persisted bound, above anything issued.
        let restarted = RevisionHub::new();
        restarted.start_local(1 + LOCAL_BLOCK);
        let (next, _) = restarted.next_local();
        assert!(next > second);
    }
}
