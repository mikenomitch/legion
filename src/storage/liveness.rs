//! Node liveness as leader-local soft state.
//!
//! `internal_docs/plans/liveness-off-the-raft-path-plan.md` §6.2. A heartbeat
//! timestamp is the one fact in Legion that is *derived* rather than reported,
//! worthless after a few seconds, and already thrown away across a
//! control-plane gap — so it does not earn a replicated commit and an fsync.
//! It lives here, in the status-lane leader's memory, and only status
//! TRANSITIONS (`active <-> lost`) are replicated.
//!
//! Nothing in this table is persisted, replicated, or read back from disk. A
//! newly elected leader inherits nothing and REBUILDS the table from incoming
//! heartbeats under [`LivenessTable::become_responsible`]'s grace, which is
//! why seeding is at `now` rather than at the persisted timestamp: the
//! persisted value is stale by construction and seeding from it would mark the
//! fleet lost on the first tick.
//!
//! Owned by [`crate::storage::store::Store`] because that is what the
//! heartbeat handler and the liveness tick both hold. Under `single` topology
//! and on a one-manager cluster the peer/digest half never engages at all.

use crate::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

/// One observed heartbeat.
#[derive(Debug, Clone, Copy)]
pub struct Beat {
    /// Monotonic observation time — what the lost decision compares.
    pub at: Instant,
    /// Wall-clock unix seconds, carried ONLY so the display checkpoint can
    /// record the time the beat was observed rather than the time the
    /// checkpoint was written (liveness plan §6.5).
    pub unix: i64,
    /// Gap since the previous beat from this node, when there was one. Drives
    /// the narrow-on-confirmation rule: a node's lost window shrinks only once
    /// it is observed actually beating at the faster cadence.
    pub gap: Option<Duration>,
}

/// What a peer manager last told us.
#[derive(Debug, Clone)]
struct PeerObservation {
    last_digest: Instant,
    claimed: HashSet<String>,
}

/// Resolved `liveness` config block (`docs/node-liveness.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivenessSettings {
    /// Mark nothing lost for this long after becoming responsible.
    pub failover_grace: Duration,
    /// How often a non-leading manager reports what it heard to the leader.
    pub digest_interval: Duration,
    /// How long the leader keeps trusting a silent peer's last digest.
    pub peer_digest_grace: Duration,
    /// How often the leader replicates a display checkpoint. `None` = off.
    pub checkpoint_interval: Option<Duration>,
    /// Accumulation window for the heartbeat group commit. Zero (the default)
    /// is today's commit-while-you-can batching.
    pub batch_window: Duration,
}

impl Default for LivenessSettings {
    fn default() -> Self {
        Self {
            failover_grace: Duration::from_secs(DEF_LIVENESS_FAILOVER_GRACE_SEC),
            digest_interval: Duration::from_secs(DEF_LIVENESS_DIGEST_INTERVAL_SEC),
            peer_digest_grace: Duration::from_secs(DEF_LIVENESS_PEER_DIGEST_GRACE_SEC),
            checkpoint_interval: Some(Duration::from_secs(DEF_LIVENESS_CHECKPOINT_INTERVAL_SEC)),
            batch_window: Duration::ZERO,
        }
    }
}

#[derive(Debug, Default)]
struct TableState {
    /// node id -> last observed beat.
    seen: HashMap<String, Beat>,
    /// When this manager became responsible for liveness: process start OR
    /// status-lane leadership acquisition, whichever is LATER. THE fix for the
    /// process-uptime hazard (liveness plan §5.4) — a manager up for six hours
    /// that wins an election gets a fresh clock, not a six-hour-old one.
    responsible_since: Option<Instant>,
    /// Peer managers that have reported digests to us.
    peers: HashMap<String, PeerObservation>,
    /// Node ids this manager has observed since the last digest drain, for
    /// the digest it sends when it does NOT lead the status lane.
    pending_digest: HashSet<String>,
}

/// The liveness table. Cheap to clone (one `Arc` inside `Store`), safe to
/// touch from the gRPC handler and the status-checker tick concurrently.
#[derive(Debug)]
pub struct LivenessTable {
    state: StdMutex<TableState>,
    settings: StdMutex<LivenessSettings>,
    /// Set while a manager that does NOT accept liveness digests leads the
    /// status lane — the mixed-version window during a manager upgrade
    /// (liveness plan §10). While it is set, heartbeats travel as the
    /// replicated write every manager version understands: correct, just
    /// slower, and it clears the moment leadership lands on an upgraded
    /// manager or the peer starts accepting digests.
    digest_fallback: std::sync::atomic::AtomicBool,
    /// The interval this manager currently serves workers, in seconds, and
    /// whether it serves one at all.
    ///
    /// Written by the status-checker tick (which owns the ladder) and read by
    /// the heartbeat handler (which puts it on the reply). Shared through the
    /// liveness table because that is the one object both already hold; a
    /// second channel between the same two places would be a second thing to
    /// keep in sync. `0` means "serve nothing" — adaptive cadence is off and
    /// workers keep their worker-type `heartbeatSeconds`.
    served_interval_seconds: std::sync::atomic::AtomicU64,
}

impl Default for LivenessTable {
    fn default() -> Self {
        Self::new()
    }
}

impl LivenessTable {
    pub fn new() -> Self {
        Self {
            state: StdMutex::new(TableState::default()),
            settings: StdMutex::new(LivenessSettings::default()),
            digest_fallback: std::sync::atomic::AtomicBool::new(false),
            served_interval_seconds: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The interval this manager tells workers to use, or `None` when it
    /// dictates nothing (adaptive cadence off — the default).
    pub fn served_interval(&self) -> Option<Duration> {
        let seconds = self
            .served_interval_seconds
            .load(std::sync::atomic::Ordering::Relaxed);
        (seconds > 0).then(|| Duration::from_secs(seconds))
    }

    pub fn set_served_interval(&self, interval: Option<Duration>) {
        self.served_interval_seconds.store(
            interval.map(|interval| interval.as_secs()).unwrap_or(0),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Whether heartbeats must currently travel as replicated writes because
    /// the status-lane leader does not accept liveness digests.
    pub fn digest_fallback(&self) -> bool {
        self.digest_fallback
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_digest_fallback(&self, fallback: bool) {
        self.digest_fallback
            .store(fallback, std::sync::atomic::Ordering::Relaxed);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TableState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn settings(&self) -> LivenessSettings {
        self.settings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn configure(&self, settings: LivenessSettings) {
        *self
            .settings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = settings;
    }

    /// Record a heartbeat this manager received. One map write: no lock beyond
    /// this table, no commit, no fsync, no raft pipeline hop.
    pub fn observe_at(&self, node_id: &str, at: Instant, unix: i64) {
        let mut state = self.lock();
        let gap = state
            .seen
            .get(node_id)
            .map(|beat| at.duration_since(beat.at));
        state
            .seen
            .insert(node_id.to_string(), Beat { at, unix, gap });
        state.pending_digest.insert(node_id.to_string());
    }

    pub fn observe(&self, node_id: &str) {
        self.observe_at(
            node_id,
            Instant::now(),
            time::OffsetDateTime::now_utc().unix_timestamp(),
        );
    }

    /// Merge one peer manager's digest. The peer's claimed set REPLACES the
    /// previous one: it is the authoritative list of nodes that manager is
    /// currently hearing from, and a node that moved managers must not stay
    /// attributed to the old one.
    pub fn observe_peer_at(&self, peer: &str, node_ids: Vec<String>, at: Instant, unix: i64) {
        let mut state = self.lock();
        for id in &node_ids {
            let gap = state.seen.get(id).map(|beat| at.duration_since(beat.at));
            state.seen.insert(id.clone(), Beat { at, unix, gap });
        }
        state.peers.insert(
            peer.to_string(),
            PeerObservation {
                last_digest: at,
                claimed: node_ids.into_iter().collect(),
            },
        );
    }

    pub fn observe_peer(&self, peer: &str, node_ids: Vec<String>) {
        self.observe_peer_at(
            peer,
            node_ids,
            Instant::now(),
            time::OffsetDateTime::now_utc().unix_timestamp(),
        );
    }

    /// Ids observed since the last drain, for the outgoing digest.
    pub fn drain_pending_digest(&self) -> Vec<String> {
        let mut state = self.lock();
        state.pending_digest.drain().collect()
    }

    /// Take responsibility for liveness: reset the grace clock and seed every
    /// currently-active node at `now`.
    ///
    /// Seeding at `now` rather than at each node's persisted timestamp is the
    /// load-bearing detail (liveness plan §6.4(2)): the persisted value is
    /// stale by construction on a leader that has just taken over, so seeding
    /// from it would mark the whole fleet lost on the first tick. Nomad seeds
    /// every node at `failover_heartbeat_ttl` for exactly this reason.
    pub fn become_responsible_at(&self, active_nodes: &[String], at: Instant, unix: i64) {
        let mut state = self.lock();
        state.responsible_since = Some(at);
        state.seen.retain(|id, _| active_nodes.contains(id));
        for id in active_nodes {
            state.seen.entry(id.clone()).or_insert(Beat {
                at,
                unix,
                gap: None,
            });
        }
    }

    pub fn become_responsible(&self, active_nodes: &[String]) {
        self.become_responsible_at(
            active_nodes,
            Instant::now(),
            time::OffsetDateTime::now_utc().unix_timestamp(),
        );
    }

    /// Stop being responsible (leadership lost). The table is kept — this
    /// manager may still be observing heartbeats for the digest — but the
    /// grace clock is cleared so a re-acquisition starts a fresh one.
    pub fn relinquish(&self) {
        self.lock().responsible_since = None;
    }

    pub fn responsible_since(&self) -> Option<Instant> {
        self.lock().responsible_since
    }

    /// How long this manager has been responsible. `Duration::ZERO` when it is
    /// not responsible at all, which is the safe reading: the caller is inside
    /// any grace and marks nothing lost.
    pub fn responsible_for_at(&self, now: Instant) -> Duration {
        self.lock()
            .responsible_since
            .map(|since| now.saturating_duration_since(since))
            .unwrap_or(Duration::ZERO)
    }

    pub fn responsible_for(&self) -> Duration {
        self.responsible_for_at(Instant::now())
    }

    pub fn last_beat(&self, node_id: &str) -> Option<Beat> {
        self.lock().seen.get(node_id).copied()
    }

    /// First sighting of a node the table has never seen — a node that
    /// registered after this manager became responsible. Seeded at `now` and
    /// therefore never lost on the tick that discovers it.
    pub fn seed_if_missing_at(&self, node_id: &str, at: Instant, unix: i64) -> Beat {
        let mut state = self.lock();
        *state.seen.entry(node_id.to_string()).or_insert(Beat {
            at,
            unix,
            gap: None,
        })
    }

    /// Drop nodes that are no longer active (removed, or marked lost). Keeps
    /// the table the live set rather than a history of everything that ever
    /// beat.
    pub fn retain(&self, keep: &HashSet<String>) {
        let mut state = self.lock();
        state.seen.retain(|id, _| keep.contains(id));
        // The pending digest is pruned with it, so a manager that later loses
        // status-lane leadership does not report nodes that no longer exist.
        state.pending_digest.retain(|id| keep.contains(id));
    }

    pub fn forget(&self, node_id: &str) {
        self.lock().seen.remove(node_id);
    }

    pub fn tracked(&self) -> usize {
        self.lock().seen.len()
    }

    /// Every tracked node's last observed beat.
    pub fn snapshot(&self) -> HashMap<String, Beat> {
        self.lock().seen.clone()
    }

    /// Nodes whose beats we only learn about through a peer manager that has
    /// gone quiet, and the peers themselves.
    ///
    /// The suppression is bounded on BOTH sides. A peer that reported within
    /// one digest interval is not a gap at all; a peer silent for longer than
    /// `peer_digest_grace` is not a gap either — it is a manager that is
    /// genuinely gone, and the workers dialing it are genuinely unreachable,
    /// so its nodes become eligible again. In between, a manager-to-manager
    /// problem must not present as fleet-wide node loss.
    pub fn peer_gap_at(&self, now: Instant) -> PeerGap {
        let settings = self.settings();
        let state = self.lock();
        let mut suppressed = HashSet::new();
        let mut stale_peers = Vec::new();
        let mut fresh_peers = Vec::new();
        for (peer, observation) in &state.peers {
            let age = now.saturating_duration_since(observation.last_digest);
            if age > settings.digest_interval && age <= settings.peer_digest_grace {
                suppressed.extend(observation.claimed.iter().cloned());
            }
            // A peer that has missed several digests is visibly quiet, and an
            // operator should be able to alert on THAT rather than on the
            // node-loss it would otherwise be mistaken for.
            if age > settings.digest_interval * 3 {
                stale_peers.push(peer.clone());
            } else {
                fresh_peers.push(peer.clone());
            }
        }
        stale_peers.sort();
        fresh_peers.sort();
        PeerGap {
            suppressed,
            stale_peers,
            fresh_peers,
        }
    }

    pub fn peer_gap(&self) -> PeerGap {
        self.peer_gap_at(Instant::now())
    }
}

/// What one liveness digest attempt did (liveness plan §6.3, §10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DigestOutcome {
    /// The status-lane leader merged it.
    Accepted,
    /// The peer we asked does not lead the status lane; it named who does.
    NotLeader {
        leader_hint: String,
    },
    /// The peer predates liveness digests. Heartbeats fall back to replicated
    /// writes until the manager roll finishes.
    Unimplemented,
    /// No leader is known right now (mid-election). Nothing to do but retry —
    /// the previous leader's `peerDigestGrace` covers this window.
    NoLeader,
    /// Not running raft at all, so there is no peer and no digest to send.
    NoEngine,
    Error(String),
}

impl DigestOutcome {
    /// The `outcome` label on `legion_liveness_digest_rpcs_total`.
    pub fn metric_label(&self) -> &'static str {
        match self {
            DigestOutcome::Accepted => "ok",
            DigestOutcome::NotLeader { .. } | DigestOutcome::NoLeader => "not_leader",
            DigestOutcome::Unimplemented => "unimplemented",
            DigestOutcome::NoEngine | DigestOutcome::Error(_) => "error",
        }
    }
}

/// What [`LivenessTable::peer_gap_at`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PeerGap {
    /// Node ids not eligible to be marked lost this tick.
    pub suppressed: HashSet<String>,
    /// Peers that have missed several digests (`legion_liveness_peer_stale`).
    pub stale_peers: Vec<String>,
    /// Peers reporting normally, so the gauge can be cleared for them.
    pub fresh_peers: Vec<String>,
}

#[cfg(test)]
mod liveness_table_test {
    use super::*;

    fn table() -> LivenessTable {
        LivenessTable::new()
    }

    #[test]
    fn observing_a_beat_records_it_and_queues_a_digest_test() {
        let table = table();
        table.observe("node-7");
        assert_eq!(table.tracked(), 1);
        assert!(table.last_beat("node-7").is_some());
        assert_eq!(table.drain_pending_digest(), vec!["node-7".to_string()]);
        assert!(table.drain_pending_digest().is_empty(), "drain is a take");
    }

    #[test]
    fn a_second_beat_records_its_gap_test() {
        let table = table();
        let start = Instant::now();
        table.observe_at("node-7", start, 100);
        table.observe_at("node-7", start + Duration::from_secs(2), 102);
        assert_eq!(
            table.last_beat("node-7").and_then(|beat| beat.gap),
            Some(Duration::from_secs(2))
        );
    }

    #[test]
    fn acquisition_seeds_every_active_node_at_now_test() {
        // A node whose persisted heartbeat is an hour old must not be lost on
        // the first tick after an election.
        let table = table();
        let now = Instant::now();
        table.become_responsible_at(&["a".into(), "b".into()], now, 1_000);
        assert_eq!(table.tracked(), 2);
        assert_eq!(table.last_beat("a").expect("seeded").at, now);
        assert_eq!(table.responsible_for_at(now), Duration::ZERO);
        assert_eq!(
            table.responsible_for_at(now + Duration::from_secs(30)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn acquisition_drops_nodes_that_are_no_longer_active_test() {
        let table = table();
        table.observe("gone");
        table.become_responsible(&["still-here".into()]);
        assert!(table.last_beat("gone").is_none());
        assert!(table.last_beat("still-here").is_some());
    }

    #[test]
    fn relinquishing_clears_the_grace_clock_test() {
        let table = table();
        table.become_responsible(&[]);
        assert!(table.responsible_since().is_some());
        table.relinquish();
        assert!(table.responsible_since().is_none());
        assert_eq!(table.responsible_for(), Duration::ZERO);
    }

    #[test]
    fn a_peers_digest_counts_as_observation_test() {
        let table = table();
        let now = Instant::now();
        table.observe_peer_at("manager-c", vec!["c-1".into(), "c-2".into()], now, 500);
        assert_eq!(table.tracked(), 2);
        assert_eq!(table.last_beat("c-1").expect("observed").unix, 500);
        // A peer's digest is not re-broadcast: only beats WE received are.
        assert!(table.drain_pending_digest().is_empty());
    }

    #[test]
    fn a_silent_peers_nodes_are_suppressed_inside_the_grace_test() {
        let table = table();
        let start = Instant::now();
        table.observe_peer_at("manager-c", vec!["c-1".into()], start, 0);
        table.observe_peer_at("manager-a", vec!["a-1".into()], start, 0);

        // Just inside the grace: c-1 suppressed, a-1 (reported by a peer that
        // is still reporting) not.
        let at = start + Duration::from_secs(29);
        table.observe_peer_at("manager-a", vec!["a-1".into()], at, 0);
        let gap = table.peer_gap_at(at);
        assert!(gap.suppressed.contains("c-1"));
        assert!(!gap.suppressed.contains("a-1"));
        assert_eq!(gap.stale_peers, vec!["manager-c".to_string()]);

        // Past the grace, a genuinely-gone manager's nodes become eligible.
        let at = start + Duration::from_secs(31);
        assert!(!table.peer_gap_at(at).suppressed.contains("c-1"));
    }

    #[test]
    fn retain_prunes_nodes_that_left_the_active_set_test() {
        let table = table();
        table.observe("a");
        table.observe("b");
        table.retain(&HashSet::from(["a".to_string()]));
        assert_eq!(table.tracked(), 1);
        assert!(table.last_beat("b").is_none());
    }
}
