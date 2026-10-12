//! The session index: which node runs which session (docs/session-mode.md).
//!
//! Sessions are never written to the replicated store. Each node reports the
//! sessions it runs on its heartbeat ([`crate::worker::session_report`]), as
//! deltas against an epoch, and the manager that receives the heartbeat
//! folds them in here. Like the content location index
//! ([`crate::storage::content_index`]) and node liveness, this is soft state
//! held by each manager for the nodes that heartbeat to it: nothing here is
//! persisted or replicated, and a manager that starts (or takes a node over)
//! asks the node for a full report the first time it hears from it.
//!
//! A manager answering for a session it does not index asks its peers
//! (`RaftTransport.QuerySessions`) and merges the answers
//! ([`crate::manager::sessions`]).
//!
//! # The protocol
//!
//! The same as the content index's:
//!
//! - **Full** (`full = true`): replace the node's sessions, take its epoch.
//! - **Delta** whose `base_epoch` matches what this manager holds: apply
//!   `upserts` / `removed`, take the new epoch. No entries and
//!   `base_epoch == epoch` is the steady-state probe.
//! - **Anything else**: ignore it and answer `resync`.
//!
//! # Duplicates
//!
//! Two nodes can briefly run one session id: an idempotent create that
//! raced to two managers before either node reported. When this manager
//! indexes both copies, it keeps the one with the earlier lease (ties broken
//! by node id) and asks the other node to stop its copy in the heartbeat
//! reply, until that node's report no longer lists it.

use crate::storage::content_index::IngestOutcome;
use crate::structs::{
    CoveredNode, IndexedSession, NodeSessionUsage, QuerySessionsReply, QuerySessionsRequest,
    SessionReport, SessionStatus,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// A node silent this long is dropped from the index, like the content
/// index. A node that comes back gets a resync on its first heartbeat.
pub const SESSION_INDEX_STALE_AFTER: Duration = Duration::from_secs(120);

/// How long a router's own record of a start stands in for the node's
/// report. A report normally lands within one heartbeat.
pub const RECENT_START_TTL: Duration = Duration::from_secs(30);

/// How long this manager remembers a session it stopped, so a retried
/// StopInstance or DestroyInstance answers success rather than NotFound.
pub const RECENT_END_TTL: Duration = Duration::from_secs(600);

/// How often ingest sweeps stale nodes and expired recent starts.
const SWEEP_EVERY: Duration = Duration::from_secs(15);

/// The per-node totals of one job's task: session count and summed
/// resources.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Usage {
    pub sessions: u32,
    pub amounts: BTreeMap<String, f64>,
}

impl Usage {
    fn add(&mut self, amounts: &BTreeMap<String, f64>) {
        self.sessions += 1;
        for (name, amount) in amounts {
            *self.amounts.entry(name.clone()).or_insert(0.0) += amount;
        }
    }

    fn subtract(&mut self, amounts: &BTreeMap<String, f64>) {
        self.sessions = self.sessions.saturating_sub(1);
        for (name, amount) in amounts {
            if let Some(used) = self.amounts.get_mut(name) {
                *used = (*used - amount).max(0.0);
            }
        }
    }

    fn to_proto(&self, node_id: &str) -> NodeSessionUsage {
        NodeSessionUsage {
            node_id: node_id.to_string(),
            sessions: self.sessions,
            resources: serde_json::to_string(&self.amounts).unwrap_or_else(|_| "{}".into()),
        }
    }
}

/// A session's reserved amounts, parsed from its `resources` JSON. A
/// suspended session holds none: its node freed its envelope share when it
/// checkpointed (it still counts in [`Usage::sessions`]).
pub fn session_amounts(status: &SessionStatus) -> BTreeMap<String, f64> {
    if status.state == "suspended" {
        return BTreeMap::new();
    }
    serde_json::from_str::<serde_json::Value>(&status.resources)
        .map(|value| crate::structs::session::resource_amounts(&value))
        .unwrap_or_default()
}

#[derive(Debug)]
struct NodeSessions {
    epoch: u64,
    sessions: HashMap<String, SessionStatus>,
    last_seen: Instant,
}

#[derive(Debug)]
struct RecentStart {
    node_id: String,
    status: SessionStatus,
    at: Instant,
}

type UsageKey = (String, String);

#[derive(Debug, Default)]
struct Inner {
    nodes: HashMap<String, NodeSessions>,
    /// session id -> the nodes reporting it (normally one).
    by_id: HashMap<String, HashSet<String>>,
    /// (job, task) -> node -> totals, kept in step with `nodes`.
    usage: HashMap<UsageKey, HashMap<String, Usage>>,
    /// Starts this manager's router made, until the node reports them.
    recent: HashMap<String, RecentStart>,
    /// node -> duplicate session ids that node should stop.
    stops: HashMap<String, HashSet<String>>,
    /// Sessions this manager stopped, until [`RECENT_END_TTL`].
    ended: HashMap<String, Instant>,
    last_sweep: Option<Instant>,
}

impl Inner {
    fn add(&mut self, node_id: &str, status: SessionStatus) {
        self.by_id
            .entry(status.session_id.clone())
            .or_default()
            .insert(node_id.to_string());
        self.usage
            .entry((status.job_id.clone(), status.task_name.clone()))
            .or_default()
            .entry(node_id.to_string())
            .or_default()
            .add(&session_amounts(&status));
        if let Some(recent) = self.recent.get(&status.session_id) {
            if recent.node_id == node_id {
                self.recent.remove(&status.session_id);
            }
        }
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.sessions.insert(status.session_id.clone(), status);
        }
    }

    fn remove(&mut self, node_id: &str, session_id: &str) {
        let Some(status) = self
            .nodes
            .get_mut(node_id)
            .and_then(|node| node.sessions.remove(session_id))
        else {
            return;
        };
        if let Some(nodes) = self.by_id.get_mut(session_id) {
            nodes.remove(node_id);
            if nodes.is_empty() {
                self.by_id.remove(session_id);
            }
        }
        let key = (status.job_id.clone(), status.task_name.clone());
        if let Some(per_node) = self.usage.get_mut(&key) {
            if let Some(usage) = per_node.get_mut(node_id) {
                usage.subtract(&session_amounts(&status));
                if usage.sessions == 0 {
                    per_node.remove(node_id);
                }
            }
            if per_node.is_empty() {
                self.usage.remove(&key);
            }
        }
    }

    fn forget(&mut self, node_id: &str) -> bool {
        self.stops.remove(node_id);
        let Some(ids) = self
            .nodes
            .get(node_id)
            .map(|node| node.sessions.keys().cloned().collect::<Vec<_>>())
        else {
            return false;
        };
        for id in ids {
            self.remove(node_id, &id);
        }
        self.nodes.remove(node_id);
        true
    }

    /// Mark the later copies of `session_id` for stopping.
    fn converge(&mut self, session_id: &str) {
        let Some(nodes) = self.by_id.get(session_id) else {
            return;
        };
        if nodes.len() < 2 {
            return;
        }
        let mut copies: Vec<(i64, String)> = nodes
            .iter()
            .filter_map(|node_id| {
                let status = self.nodes.get(node_id)?.sessions.get(session_id)?;
                Some((status.lease_issued_at_unix_ms, node_id.clone()))
            })
            .collect();
        copies.sort();
        for (_, node_id) in copies.into_iter().skip(1) {
            self.stops
                .entry(node_id)
                .or_default()
                .insert(session_id.to_string());
        }
    }

    fn sweep(&mut self, now: Instant) {
        if self
            .last_sweep
            .is_some_and(|last| now.duration_since(last) < SWEEP_EVERY)
        {
            return;
        }
        self.last_sweep = Some(now);
        let stale: Vec<String> = self
            .nodes
            .iter()
            .filter(|(_, node)| now.duration_since(node.last_seen) >= SESSION_INDEX_STALE_AFTER)
            .map(|(id, _)| id.clone())
            .collect();
        for node_id in stale {
            self.forget(&node_id);
        }
        self.recent
            .retain(|_, start| now.duration_since(start.at) < RECENT_START_TTL);
        self.ended
            .retain(|_, at| now.duration_since(*at) < RECENT_END_TTL);
    }

    fn indexed(&self, node_id: &str, status: &SessionStatus, recent: bool) -> IndexedSession {
        IndexedSession {
            node_id: node_id.to_string(),
            status: Some(status.clone()),
            recent,
        }
    }

    /// Recent starts not yet in a report.
    fn unreported(&self) -> impl Iterator<Item = &RecentStart> {
        self.recent.values().filter(|start| {
            !self
                .nodes
                .get(&start.node_id)
                .is_some_and(|node| node.sessions.contains_key(&start.status.session_id))
        })
    }
}

/// What [`SessionIndex::ingest`] decided about a report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionIngest {
    pub outcome: IngestOutcome,
    /// Duplicate session ids the reporting node should stop
    /// (`HeartbeatReply.stop_sessions`).
    pub stop: Vec<String>,
}

/// Which node runs which session, in memory, off raft. Owned by
/// [`crate::storage::store::Store`] (shared across clones) so the heartbeat
/// handler writes it and the instance RPCs and `QuerySessions` read it.
#[derive(Debug, Default)]
pub struct SessionIndex {
    inner: RwLock<Inner>,
}

impl SessionIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one heartbeat's report in. See the module docs.
    pub fn ingest(&self, node_id: &str, report: &SessionReport, now: Instant) -> SessionIngest {
        let mut inner = self.inner.write().unwrap_or_else(|p| p.into_inner());
        let outcome = if report.full {
            inner.forget(node_id);
            inner.nodes.insert(
                node_id.to_string(),
                NodeSessions {
                    epoch: report.epoch,
                    sessions: HashMap::with_capacity(report.upserts.len()),
                    last_seen: now,
                },
            );
            for status in &report.upserts {
                inner.add(node_id, status.clone());
            }
            IngestOutcome::Replaced
        } else if inner
            .nodes
            .get(node_id)
            .is_some_and(|node| node.epoch == report.base_epoch)
        {
            for session_id in &report.removed {
                inner.remove(node_id, session_id);
            }
            for status in &report.upserts {
                // An upsert replaces the previous copy wholesale, so its
                // usage is re-added from the new resources.
                inner.remove(node_id, &status.session_id);
                inner.add(node_id, status.clone());
            }
            let node = inner.nodes.get_mut(node_id).expect("checked above");
            node.epoch = report.epoch;
            node.last_seen = now;
            IngestOutcome::Applied
        } else {
            IngestOutcome::Resync
        };
        if outcome != IngestOutcome::Resync {
            for status in &report.upserts {
                inner.converge(&status.session_id);
            }
        }
        // Stops stand until the node stops listing the session.
        let listed: HashSet<String> = inner
            .nodes
            .get(node_id)
            .map(|node| node.sessions.keys().cloned().collect())
            .unwrap_or_default();
        let stop = match inner.stops.get_mut(node_id) {
            Some(stops) => {
                stops.retain(|id| listed.contains(id));
                let mut stop: Vec<String> = stops.iter().cloned().collect();
                stop.sort();
                stop
            }
            None => Vec::new(),
        };
        if stop.is_empty() {
            inner.stops.remove(node_id);
        }
        inner.sweep(now);
        SessionIngest { outcome, stop }
    }

    /// Record a start this manager's router just made, so a read before the
    /// node's next report finds it and the router counts its room.
    pub fn record_start(&self, node_id: &str, status: SessionStatus, now: Instant) {
        let mut inner = self.inner.write().unwrap_or_else(|p| p.into_inner());
        inner.recent.insert(
            status.session_id.clone(),
            RecentStart {
                node_id: node_id.to_string(),
                status,
                at: now,
            },
        );
    }

    /// Forget a session this manager just stopped, so a read right after
    /// the stop does not find it, and remember the stop for
    /// [`RECENT_END_TTL`] ([`Self::recently_ended`]). The node's next report
    /// confirms it.
    pub fn record_stop(&self, node_id: &str, session_id: &str) {
        let mut inner = self.inner.write().unwrap_or_else(|p| p.into_inner());
        inner.ended.insert(session_id.to_string(), Instant::now());
        if inner
            .recent
            .get(session_id)
            .is_some_and(|start| start.node_id == node_id)
        {
            inner.recent.remove(session_id);
        }
        inner.remove(node_id, session_id);
    }

    /// Answer a `QuerySessions` request from this manager's view.
    pub fn query(&self, request: &QuerySessionsRequest, now: Instant) -> QuerySessionsReply {
        let inner = self.inner.read().unwrap_or_else(|p| p.into_inner());
        let covered_nodes = inner
            .nodes
            .iter()
            .map(|(node_id, node)| CoveredNode {
                node_id: node_id.clone(),
                age_ms: now.saturating_duration_since(node.last_seen).as_millis() as u64,
            })
            .collect();
        let task_matches = |status: &SessionStatus| {
            request.task_name.is_empty() || status.task_name == request.task_name
        };

        if !request.session_id.is_empty() {
            let mut sessions: Vec<IndexedSession> = inner
                .by_id
                .get(&request.session_id)
                .into_iter()
                .flatten()
                .filter_map(|node_id| {
                    let status = inner
                        .nodes
                        .get(node_id)?
                        .sessions
                        .get(&request.session_id)?;
                    Some(inner.indexed(node_id, status, false))
                })
                .collect();
            sessions.extend(
                inner
                    .unreported()
                    .filter(|start| start.status.session_id == request.session_id)
                    .map(|start| inner.indexed(&start.node_id, &start.status, true)),
            );
            return QuerySessionsReply {
                sessions,
                covered_nodes,
                node_usage: Vec::new(),
            };
        }

        if request.usage_only {
            let mut usage: HashMap<String, Usage> = HashMap::new();
            for ((job, task), per_node) in &inner.usage {
                if *job != request.job_id
                    || (!request.task_name.is_empty() && *task != request.task_name)
                {
                    continue;
                }
                for (node_id, node_usage) in per_node {
                    let entry = usage.entry(node_id.clone()).or_default();
                    entry.sessions += node_usage.sessions;
                    for (name, amount) in &node_usage.amounts {
                        *entry.amounts.entry(name.clone()).or_insert(0.0) += amount;
                    }
                }
            }
            for start in inner.unreported() {
                if start.status.job_id == request.job_id && task_matches(&start.status) {
                    usage
                        .entry(start.node_id.clone())
                        .or_default()
                        .add(&session_amounts(&start.status));
                }
            }
            return QuerySessionsReply {
                sessions: Vec::new(),
                covered_nodes,
                node_usage: usage
                    .iter()
                    .map(|(node_id, usage)| usage.to_proto(node_id))
                    .collect(),
            };
        }

        let mut sessions = Vec::new();
        if !request.job_id.is_empty() {
            for (node_id, node) in &inner.nodes {
                for status in node.sessions.values() {
                    if status.job_id == request.job_id && task_matches(status) {
                        sessions.push(inner.indexed(node_id, status, false));
                    }
                }
            }
            for start in inner.unreported() {
                if start.status.job_id == request.job_id && task_matches(&start.status) {
                    sessions.push(inner.indexed(&start.node_id, &start.status, true));
                }
            }
        }
        QuerySessionsReply {
            sessions,
            covered_nodes,
            node_usage: Vec::new(),
        }
    }

    /// Whether this manager stopped a session with this id within
    /// [`RECENT_END_TTL`]. Only this manager's own stops: a retry that lands
    /// on another manager answers NotFound.
    pub fn recently_ended(&self, session_id: &str) -> bool {
        let inner = self.inner.read().unwrap_or_else(|p| p.into_inner());
        inner
            .ended
            .get(session_id)
            .is_some_and(|at| at.elapsed() < RECENT_END_TTL)
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

    /// Drop a node's entries (deregistered, or gone silent).
    pub fn forget_node(&self, node_id: &str) -> bool {
        self.inner
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .forget(node_id)
    }

    /// Drop every entry. For tests that model a manager restart.
    pub fn clear(&self) {
        *self.inner.write().unwrap_or_else(|p| p.into_inner()) = Inner::default();
    }

    /// Nodes covered, and sessions indexed across them.
    pub fn size(&self) -> (usize, usize) {
        let inner = self.inner.read().unwrap_or_else(|p| p.into_inner());
        (
            inner.nodes.len(),
            inner.nodes.values().map(|node| node.sessions.len()).sum(),
        )
    }
}

#[cfg(test)]
mod session_index_test {
    use super::*;

    fn status(id: &str, iat: i64) -> SessionStatus {
        SessionStatus {
            session_id: id.into(),
            job_id: "agents".into(),
            job_config_id: "agents-1".into(),
            task_name: "workspace".into(),
            envelope_allocation_id: 7,
            state: "running".into(),
            lease_issued_at_unix_ms: iat,
            resources: r#"{"cpu":500,"memory":512}"#.into(),
            ..Default::default()
        }
    }

    fn full(epoch: u64, upserts: Vec<SessionStatus>) -> SessionReport {
        SessionReport {
            epoch,
            base_epoch: 0,
            full: true,
            upserts,
            removed: Vec::new(),
        }
    }

    fn delta(
        base: u64,
        epoch: u64,
        upserts: Vec<SessionStatus>,
        removed: &[&str],
    ) -> SessionReport {
        SessionReport {
            epoch,
            base_epoch: base,
            full: false,
            upserts,
            removed: removed.iter().map(|id| id.to_string()).collect(),
        }
    }

    fn by_id(index: &SessionIndex, id: &str) -> Vec<IndexedSession> {
        index
            .query(
                &QuerySessionsRequest {
                    session_id: id.into(),
                    ..Default::default()
                },
                Instant::now(),
            )
            .sessions
    }

    #[test]
    fn an_unknown_node_resyncs_then_deltas_apply_test() {
        let index = SessionIndex::new();
        let now = Instant::now();
        let outcome = index.ingest("n1", &delta(0, 0, vec![], &[]), now).outcome;
        assert_eq!(outcome, IngestOutcome::Resync);
        let outcome = index
            .ingest("n1", &full(1, vec![status("a", 1)]), now)
            .outcome;
        assert_eq!(outcome, IngestOutcome::Replaced);
        let outcome = index
            .ingest("n1", &delta(1, 2, vec![status("b", 2)], &["a"]), now)
            .outcome;
        assert_eq!(outcome, IngestOutcome::Applied);
        assert!(by_id(&index, "a").is_empty());
        assert_eq!(by_id(&index, "b").len(), 1);
        // A gap after a lost reply resyncs.
        let outcome = index.ingest("n1", &delta(5, 6, vec![], &[]), now).outcome;
        assert_eq!(outcome, IngestOutcome::Resync);
    }

    #[test]
    fn usage_follows_upserts_and_removals_test() {
        let index = SessionIndex::new();
        let now = Instant::now();
        index.ingest("n1", &full(1, vec![status("a", 1), status("b", 1)]), now);
        let usage = |index: &SessionIndex| {
            index
                .query(
                    &QuerySessionsRequest {
                        job_id: "agents".into(),
                        task_name: "workspace".into(),
                        usage_only: true,
                        ..Default::default()
                    },
                    Instant::now(),
                )
                .node_usage
        };
        let first = usage(&index);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].sessions, 2);
        assert_eq!(first[0].resources, r#"{"cpu":1000.0,"memory":1024.0}"#);
        index.ingest("n1", &delta(1, 2, vec![status("a", 1)], &["b"]), now);
        assert_eq!(usage(&index)[0].sessions, 1);
        // A router start counts before the node reports it.
        index.record_start("n2", status("c", 3), now);
        assert_eq!(usage(&index).len(), 2);
    }

    #[test]
    fn the_later_duplicate_is_stopped_until_its_node_drops_it_test() {
        let index = SessionIndex::new();
        let now = Instant::now();
        index.ingest("n1", &full(1, vec![status("dup", 100)]), now);
        let second = index.ingest("n2", &full(1, vec![status("dup", 200)]), now);
        assert_eq!(second.stop, vec!["dup".to_string()]);
        // The earlier copy is never asked to stop.
        let first = index.ingest("n1", &delta(1, 1, vec![], &[]), now);
        assert!(first.stop.is_empty());
        // Re-sent until the node's report drops it.
        let again = index.ingest("n2", &delta(1, 1, vec![], &[]), now);
        assert_eq!(again.stop, vec!["dup".to_string()]);
        let gone = index.ingest("n2", &delta(1, 2, vec![], &["dup"]), now);
        assert!(gone.stop.is_empty());
        assert_eq!(by_id(&index, "dup").len(), 1);
    }

    #[test]
    fn a_recent_start_stands_in_until_the_report_test() {
        let index = SessionIndex::new();
        let now = Instant::now();
        index.record_start("n1", status("s", 1), now);
        let found = by_id(&index, "s");
        assert_eq!(found.len(), 1);
        assert!(found[0].recent);
        index.ingest("n1", &full(1, vec![status("s", 1)]), now);
        let found = by_id(&index, "s");
        assert_eq!(found.len(), 1);
        assert!(!found[0].recent);
    }

    #[test]
    fn a_stop_hides_the_session_and_is_remembered_test() {
        let index = SessionIndex::new();
        let now = Instant::now();
        index.ingest("n1", &full(1, vec![status("s", 1)]), now);
        assert!(!index.recently_ended("s"));
        index.record_stop("n1", "s");
        assert!(by_id(&index, "s").is_empty());
        assert!(index.recently_ended("s"));
        assert!(!index.recently_ended("other"));
    }

    #[test]
    fn silent_nodes_are_swept_test() {
        let index = SessionIndex::new();
        let start = Instant::now();
        index.ingest("n1", &full(1, vec![status("a", 1)]), start);
        let later = start + SESSION_INDEX_STALE_AFTER + Duration::from_secs(1);
        index.ingest("n2", &full(1, vec![]), later);
        assert_eq!(index.size(), (1, 0));
        assert!(by_id(&index, "a").is_empty());
    }
}
