//! The manager's half of session mode (docs/session-mode.md): the cluster
//! view of running sessions, the session router, and the calls that stop
//! sessions on their nodes.
//!
//! Nothing here writes the replicated store. A session lives on its node and
//! in the session index ([`crate::storage::session_index`]) of whichever
//! manager its node heartbeats to; every read here merges this manager's
//! index with its peers' (`RaftTransport.QuerySessions`).

use crate::prelude::*;
use crate::storage::session_index::Usage;
use crate::storage::store::Store;
use crate::structs::edge_boot::Gate;
use crate::structs::session::{
    envelope_serves, resource_amounts, session_claim_id, SessionPlacement, ENVELOPE_RUNTIME,
};
use crate::structs::{
    Allocation, IndexedSession, InstanceInfo, JobConfig, ProvisionalAllocation, QuerySessionsReply,
    QuerySessionsRequest, SessionStart, SessionStatus, TaskConfig,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tonic::Status;

/// How long the router reuses its view of envelope room before reading the
/// index (and its peers) again. Its own starts update the cached view, so
/// the staleness that matters is other managers' starts and ended sessions,
/// which the nodes' own admission catches.
const ROUTER_VIEW_TTL: Duration = Duration::from_secs(1);

/// Nodes the router tries before giving up on a start.
const ROUTER_ATTEMPTS: usize = 3;

/// Bound on one start or stop call to a worker.
const WORKER_CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// A recent start stands in for a node report when the start was issued
/// after the report, less this allowance for clock skew between managers.
const RECENT_SKEW_MS: i64 = 1_000;

/// The cluster's view of some sessions, merged across managers.
#[derive(Debug, Clone, Default)]
pub struct SessionView {
    /// One entry per (node, session id).
    pub sessions: Vec<IndexedSession>,
    /// Nodes some manager holds a full report for.
    pub covered: HashSet<String>,
    /// Per node, the summed usage of the matching sessions (usage queries).
    pub usage: HashMap<String, Usage>,
    /// When the view was read, RFC 3339.
    pub as_of: String,
}

fn now_unix_ms() -> i64 {
    (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}

fn rfc3339_ms(unix_ms: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(unix_ms)
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default()
}

/// Read sessions from this manager's index and every peer's, and merge.
pub async fn query(store: &Store, request: QuerySessionsRequest) -> SessionView {
    let local = store.sessions().query(&request, Instant::now());
    let mut replies = vec![local];
    replies.extend(store.query_peer_sessions(request).await);
    merge(replies, now_unix_ms())
}

/// Merge managers' answers. A node's reported sessions come from the
/// manager that heard from it last; a router's recent start counts until a
/// report issued after it (see [`RECENT_SKEW_MS`]) takes over.
pub fn merge(replies: Vec<QuerySessionsReply>, now_ms: i64) -> SessionView {
    let mut freshest: HashMap<String, (usize, u64)> = HashMap::new();
    for (index, reply) in replies.iter().enumerate() {
        for covered in &reply.covered_nodes {
            let entry = freshest
                .entry(covered.node_id.clone())
                .or_insert((index, covered.age_ms));
            if covered.age_ms < entry.1 {
                *entry = (index, covered.age_ms);
            }
        }
    }
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut sessions = Vec::new();
    for (index, reply) in replies.iter().enumerate() {
        for entry in reply.sessions.iter().filter(|entry| !entry.recent) {
            let Some(status) = entry.status.as_ref() else {
                continue;
            };
            let owner = freshest.get(&entry.node_id).map(|(owner, _)| *owner);
            if owner.is_some_and(|owner| owner != index) {
                continue;
            }
            if seen.insert((entry.node_id.clone(), status.session_id.clone())) {
                sessions.push(entry.clone());
            }
        }
    }
    for reply in &replies {
        for entry in reply.sessions.iter().filter(|entry| entry.recent) {
            let Some(status) = entry.status.as_ref() else {
                continue;
            };
            let after_report = match freshest.get(&entry.node_id) {
                None => true,
                Some((_, age)) => {
                    status.lease_issued_at_unix_ms >= now_ms - *age as i64 - RECENT_SKEW_MS
                }
            };
            if after_report && seen.insert((entry.node_id.clone(), status.session_id.clone())) {
                sessions.push(entry.clone());
            }
        }
    }
    let mut usage: HashMap<String, Usage> = HashMap::new();
    for (index, reply) in replies.iter().enumerate() {
        for node_usage in &reply.node_usage {
            let owner = freshest.get(&node_usage.node_id).map(|(owner, _)| *owner);
            if owner.is_some_and(|owner| owner != index) {
                continue;
            }
            let amounts: BTreeMap<String, f64> =
                serde_json::from_str(&node_usage.resources).unwrap_or_default();
            let entry = usage.entry(node_usage.node_id.clone()).or_default();
            entry.sessions += node_usage.sessions;
            for (name, amount) in amounts {
                *entry.amounts.entry(name).or_insert(0.0) += amount;
            }
        }
    }
    SessionView {
        sessions,
        covered: freshest.into_keys().collect(),
        usage,
        as_of: rfc3339_ms(now_ms),
    }
}

/// Of the copies of one session id, the one that stays and the ones that
/// should stop. The higher generation wins (a copy resumed from a newer
/// checkpoint holds the newer state); then the earliest lease; then the
/// lower node id, so every manager picks the same copy.
pub fn winner(mut copies: Vec<IndexedSession>) -> Option<(IndexedSession, Vec<IndexedSession>)> {
    copies.sort_by(|a, b| {
        let key = |entry: &IndexedSession| {
            let (generation, lease) = entry
                .status
                .as_ref()
                .map(|status| (status.generation, status.lease_issued_at_unix_ms))
                .unwrap_or_default();
            (std::cmp::Reverse(generation), lease, entry.node_id.clone())
        };
        key(a).cmp(&key(b))
    });
    let mut copies = copies.into_iter();
    let first = copies.next()?;
    Some((first, copies.collect()))
}

/// The running session with this id, if any: the surviving copy, with a stop
/// sent to any later copy. An id without the minted shape is never a session,
/// so it is answered without asking the peers.
///
/// An id that carries a node tag is asked of that node first
/// (`Worker.GetSession`): one call instead of a fan-out. The node's answer is
/// final when it has the session or says it never had it; a node that does
/// not answer, a tag no live node has, or a node that says the tag is not
/// its own falls back to the merged view.
pub async fn find(store: &Store, session_id: &str) -> Option<IndexedSession> {
    if !crate::structs::is_minted_instance_id(session_id) {
        return None;
    }
    if let Some(tag) = crate::structs::session_node_tag(session_id) {
        match probe_tagged(store, session_id, tag).await {
            Probe::Found(entry) => return Some(*entry),
            Probe::Absent => return None,
            Probe::Unknown => {}
        }
    }
    let view = query(
        store,
        QuerySessionsRequest {
            session_id: session_id.to_string(),
            ..Default::default()
        },
    )
    .await;
    let (kept, later) = winner(view.sessions)?;
    stop_duplicates(store, later);
    Some(kept)
}

/// What the node a session id names said about it.
enum Probe {
    Found(Box<IndexedSession>),
    /// The node answered and does not hold the session.
    Absent,
    /// No answer worth trusting: fall back to the merged view.
    Unknown,
}

/// How long the tag-to-node map is reused before the node list is read again.
const NODE_TAG_TTL: Duration = Duration::from_secs(5);

struct NodeTags {
    read_at: Instant,
    by_tag: HashMap<String, Vec<String>>,
}

fn node_tag_cache() -> &'static Mutex<HashMap<usize, NodeTags>> {
    static CACHE: OnceLock<Mutex<HashMap<usize, NodeTags>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The nodes whose tag is `tag` (almost always one). Read from the store's
/// node list at most every [`NODE_TAG_TTL`]; a miss re-reads once, so a node
/// that joined since is found.
pub fn nodes_for_tag(store: &Store, tag: &str) -> Vec<String> {
    let key = std::sync::Arc::as_ptr(store.sessions()) as usize;
    let lookup = |fresh: bool| -> Option<Vec<String>> {
        let mut cache = node_tag_cache().lock().unwrap_or_else(|e| e.into_inner());
        let stale = cache
            .get(&key)
            .is_none_or(|tags| fresh || tags.read_at.elapsed() > NODE_TAG_TTL);
        if stale {
            let mut by_tag: HashMap<String, Vec<String>> = HashMap::new();
            for node_id in store.cached_node_ids().unwrap_or_default() {
                by_tag
                    .entry(crate::structs::node_tag(&node_id))
                    .or_default()
                    .push(node_id);
            }
            cache.insert(
                key,
                NodeTags {
                    read_at: Instant::now(),
                    by_tag,
                },
            );
        }
        cache
            .get(&key)
            .and_then(|tags| tags.by_tag.get(tag).cloned())
    };
    lookup(false).or_else(|| lookup(true)).unwrap_or_default()
}

async fn probe_tagged(store: &Store, session_id: &str, tag: &str) -> Probe {
    let nodes = nodes_for_tag(store, tag);
    if nodes.is_empty() {
        return Probe::Unknown;
    }
    let mut absent = 0;
    for node_id in &nodes {
        match get_on_node(store, node_id, session_id).await {
            Ok(Some(status)) => {
                return Probe::Found(Box::new(IndexedSession {
                    node_id: node_id.clone(),
                    status: Some(status),
                    recent: false,
                }))
            }
            Ok(None) => absent += 1,
            Err(err) => {
                debug!(session_id, node_id = %node_id, error = %err, "Session probe failed");
            }
        }
    }
    if absent == nodes.len() {
        Probe::Absent
    } else {
        Probe::Unknown
    }
}

/// Ask one node for a session (`Worker.GetSession`). `Ok(None)` when the
/// node answers that it does not hold it (never had it, or evicted it);
/// a `wrong_node` answer is an error, so the caller looks elsewhere.
pub async fn get_on_node(
    store: &Store,
    node_id: &str,
    session_id: &str,
) -> Result<Option<SessionStatus>, Status> {
    let (mut client, target) = worker_client(store, node_id)?;
    let mut request = tonic::Request::new(crate::structs::GetSessionRequest {
        session_id: session_id.to_string(),
        ticket: String::new(),
    });
    request.set_timeout(WORKER_CALL_TIMEOUT);
    match client.get_session(request).await {
        Ok(reply) => Ok(reply.into_inner().session),
        Err(status)
            if status.code() == tonic::Code::NotFound
                && !status.message().starts_with("wrong_node:") =>
        {
            Ok(None)
        }
        Err(status) => Err(crate::manager::worker_plane::worker_call_error(
            &target.node_id,
            &target.address,
            status,
        )),
    }
}

/// How often each manager sweeps the derived ids newly reported to it for
/// copies on nodes that heartbeat to other managers.
pub const DUPLICATE_SWEEP_EVERY: Duration = Duration::from_secs(1);

/// Ids per peer query in one sweep.
const DUPLICATE_SWEEP_BATCH: usize = 2_000;

/// The managers' batched duplicate sweep (session durability plan §6.7,
/// mechanism 3): ask every peer, in one call per batch, about the untagged
/// (derived) session ids newly reported here, and stop the losing copy of
/// any id two nodes run. Only derived ids can be held twice: a minted id
/// carries its node's tag. Returns how many copies it asked to stop.
pub async fn sweep_duplicates(store: &Store) -> usize {
    let ids = store.sessions().take_sweep_ids();
    let mut stopped = 0;
    for batch in ids.chunks(DUPLICATE_SWEEP_BATCH) {
        let view = query(
            store,
            QuerySessionsRequest {
                session_ids: batch.to_vec(),
                ..Default::default()
            },
        )
        .await;
        let mut by_id: HashMap<String, Vec<IndexedSession>> = HashMap::new();
        for entry in view.sessions {
            let Some(id) = entry.status.as_ref().map(|s| s.session_id.clone()) else {
                continue;
            };
            by_id.entry(id).or_default().push(entry);
        }
        for copies in by_id.into_values().filter(|copies| copies.len() > 1) {
            if let Some((_, later)) = winner(copies) {
                stopped += later.len();
                stop_duplicates(store, later);
            }
        }
    }
    stopped
}

/// Bound on a suspend or resume call to a worker: a memory capture or a
/// restore of a large guest takes longer than a start or a stop.
const SLEEP_CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// Where a session is, for the verbs that must reach it (suspend, start,
/// get). Unlike [`find`], it keeps apart the answers a caller words
/// differently: a node that evicted the session, and a node that holds it
/// but cannot be reached.
#[derive(Debug)]
pub enum Located {
    Found(Box<IndexedSession>),
    /// The node evicted the session's checkpoint; the message is the node's
    /// own (`session X was evicted from NODE at TIME (reason); …`), with
    /// its `\nHint:` line.
    Evicted(String),
    /// The id's tag names a node, the node did not answer, and no manager's
    /// index has the session either. The status is the UNAVAILABLE answer.
    Unreachable(Status),
    NotFound,
}

/// What one node answered to `Worker.GetSession`.
enum NodeAnswer {
    Holds(SessionStatus),
    Absent,
    Evicted(String),
    /// The id's tag is not this node's (a stale tag map): ask elsewhere.
    WrongNode,
    Failed(Status),
}

async fn ask_node(store: &Store, node_id: &str, session_id: &str) -> NodeAnswer {
    ask_node_within(store, node_id, session_id, WORKER_CALL_TIMEOUT).await
}

async fn ask_node_within(
    store: &Store,
    node_id: &str,
    session_id: &str,
    timeout: Duration,
) -> NodeAnswer {
    let (mut client, target) = match worker_client(store, node_id) {
        Ok(found) => found,
        Err(status) => return NodeAnswer::Failed(status),
    };
    let mut request = tonic::Request::new(crate::structs::GetSessionRequest {
        session_id: session_id.to_string(),
        ticket: String::new(),
    });
    request.set_timeout(timeout);
    match client.get_session(request).await {
        Ok(reply) => match reply.into_inner().session {
            Some(status) => NodeAnswer::Holds(status),
            None => NodeAnswer::Absent,
        },
        Err(status) if status.code() == tonic::Code::NotFound => {
            let message = status.message();
            if let Some(rest) = message.strip_prefix("evicted:") {
                NodeAnswer::Evicted(rest.trim_start().to_string())
            } else if message.starts_with("wrong_node:") {
                NodeAnswer::WrongNode
            } else {
                NodeAnswer::Absent
            }
        }
        Err(status) => NodeAnswer::Failed(crate::manager::worker_plane::worker_call_error(
            &target.node_id,
            &target.address,
            status,
        )),
    }
}

/// Find a session for a verb that must reach it. The tagged node is asked
/// first; when it does not answer, the merged index stands in (a node can
/// be unreachable from this manager and still heartbeat to another), and
/// only when that has nothing either is the answer [`Located::Unreachable`].
pub async fn locate(store: &Store, session_id: &str) -> Located {
    if !crate::structs::is_minted_instance_id(session_id) {
        return Located::NotFound;
    }
    let mut unreachable: Option<(String, Status)> = None;
    if let Some(tag) = crate::structs::session_node_tag(session_id) {
        let nodes = nodes_for_tag(store, tag);
        let mut absent = 0;
        for node_id in &nodes {
            match ask_node(store, node_id, session_id).await {
                NodeAnswer::Holds(status) => {
                    return Located::Found(Box::new(IndexedSession {
                        node_id: node_id.clone(),
                        status: Some(status),
                        recent: false,
                    }))
                }
                NodeAnswer::Evicted(message) => return Located::Evicted(message),
                NodeAnswer::Absent => absent += 1,
                NodeAnswer::WrongNode => {}
                NodeAnswer::Failed(status) => {
                    debug!(session_id, node_id = %node_id, error = %status, "Session probe failed");
                    unreachable.get_or_insert((node_id.clone(), status));
                }
            }
        }
        if !nodes.is_empty() && absent == nodes.len() {
            return Located::NotFound;
        }
    }
    let view = query(
        store,
        QuerySessionsRequest {
            session_id: session_id.to_string(),
            ..Default::default()
        },
    )
    .await;
    if let Some((kept, later)) = winner(view.sessions) {
        stop_duplicates(store, later);
        return Located::Found(Box::new(kept));
    }
    match unreachable {
        Some((node_id, status)) => {
            Located::Unreachable(unreachable_answer(store, &node_id, session_id, &status))
        }
        None => Located::NotFound,
    }
}

/// The UNAVAILABLE answer for a session whose node does not answer: which
/// node, what the cluster last knew of it, and that the session is not
/// lost — a local session lives only there, so the caller waits for the
/// node rather than starting over.
pub fn unreachable_answer(
    store: &Store,
    node_id: &str,
    session_id: &str,
    cause: &Status,
) -> Status {
    let heard = store
        .get_node(node_id)
        .ok()
        .map(|node| {
            let age = time::OffsetDateTime::parse(
                &node.last_heartbeat,
                &time::format_description::well_known::Rfc3339,
            )
            .ok()
            .map(|at| {
                (time::OffsetDateTime::now_utc() - at)
                    .whole_seconds()
                    .max(0)
            });
            match age {
                Some(secs) => format!(
                    "is {} and has not been heard from for {}",
                    node.status,
                    humantime::format_duration(Duration::from_secs(secs as u64))
                ),
                None => format!("is {}", node.status),
            }
        })
        .unwrap_or_else(|| "is not in the node list".to_string());
    let cause = cause
        .message()
        .split("\nHint:")
        .next()
        .unwrap_or_default()
        .to_string();
    Status::unavailable(format!(
        "node {node_id} holds session {session_id} and {heard} ({cause})\nHint: a session \
         lives on its node; retry when the node is back (`legion node info {node_id}`)"
    ))
}

/// Suspend sessions on one node (`Worker.SuspendSessions`).
pub async fn suspend_on_node(
    store: &Store,
    node_id: &str,
    session_ids: &[String],
    reason: &str,
) -> Result<crate::structs::SuspendSessionsReply, Status> {
    let (mut client, target) = worker_client(store, node_id)?;
    let mut request = tonic::Request::new(crate::structs::SuspendSessionsRequest {
        session_ids: session_ids.to_vec(),
        reason: reason.to_string(),
    });
    request.set_timeout(SLEEP_CALL_TIMEOUT);
    client
        .suspend_sessions(request)
        .await
        .map(tonic::Response::into_inner)
        .map_err(|status| {
            let status = crate::manager::worker_plane::worker_call_error(
                &target.node_id,
                &target.address,
                status,
            );
            if crate::manager::worker_plane::is_unreachable(&status) {
                unreachable_answer(store, node_id, &session_ids.join(", "), &status)
            } else {
                status
            }
        })
}

/// Resume a suspended session on a node (`Worker.ResumeSession`). The
/// node's own answers pass through: RESOURCE_EXHAUSTED when the envelope is
/// full (the checkpoint stays), NOT_FOUND `evicted:` when it was evicted
/// meanwhile. `from_manifest` names the session's job and task: the node
/// then resumes a `store` session from its manifest even if it never held
/// it, moving it (only with `force_move` when the manifest says another
/// node runs it).
pub async fn resume_on_node(
    store: &Store,
    node_id: &str,
    session_id: &str,
    cold: bool,
    from_manifest: Option<(&str, &str)>,
    force_move: bool,
) -> Result<crate::structs::ResumeSessionReply, Status> {
    let (mut client, target) = worker_client(store, node_id)?;
    let (job_id, task_name) = from_manifest.unwrap_or_default();
    let mut request = tonic::Request::new(crate::structs::ResumeSessionRequest {
        session_id: session_id.to_string(),
        cold,
        job_id: job_id.to_string(),
        task_name: task_name.to_string(),
        force_move,
    });
    request.set_timeout(SLEEP_CALL_TIMEOUT);
    client
        .resume_session(request)
        .await
        .map(tonic::Response::into_inner)
        .map_err(|status| {
            let status = crate::manager::worker_plane::worker_call_error(
                &target.node_id,
                &target.address,
                status,
            );
            if crate::manager::worker_plane::is_unreachable(&status) {
                unreachable_answer(store, node_id, session_id, &status)
            } else if let Some(rest) = status.message().strip_prefix("evicted:") {
                Status::not_found(rest.trim_start().to_string())
            } else {
                status
            }
        })
}

/// How long the manager waits for the node a `store` session's manifest
/// names before it calls that node unreachable (session durability plan
/// §6.7, "Moving is cautious for running sessions").
pub const MOVE_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// What [`resume_store_session`] found.
#[derive(Debug)]
pub enum StoreResume {
    /// Resumed on `node_id` from the session's manifest.
    Resumed {
        node_id: String,
        reply: Box<crate::structs::ResumeSessionReply>,
    },
    /// It runs on `node_id` already (it moved there earlier, and the node
    /// its id is tagged with no longer holds it).
    Running { node_id: String },
}

/// Whether the node row says the node is gone: `down`, `lost`, or no row.
fn node_is_down(store: &Store, node_id: &str) -> bool {
    store
        .get_node(node_id)
        .map(|node| node.status == "down" || node.status == "lost")
        .unwrap_or(true)
}

/// Resume a `store` session from its manifest on a node with room (session
/// durability plan §6.6, "Resume"): the node its id is tagged with lost it,
/// evicted its local copy, is full, or does not answer. Nodes are tried in
/// the router's order. A node that finds the manifest naming another node
/// as running the session answers `running_elsewhere`; the manager then
/// probes that node itself (`GetSession`, [`MOVE_PROBE_TIMEOUT`]): running
/// there, the answer is that it runs; not there, the move goes ahead; no
/// answer, it goes ahead only when that node is marked down (or lost) or the
/// caller passed `force_move`, and otherwise answers UNAVAILABLE.
///
/// `skip` is a node that already refused the resume for room (the session's
/// own node, asked first), so no attempt is spent on it again.
pub async fn resume_store_session(
    store: &Store,
    job_id: &str,
    task: &TaskConfig,
    session_id: &str,
    cold: bool,
    force_move: bool,
    skip: Option<&str>,
) -> Result<StoreResume, Status> {
    refresh_room(store, job_id, &task.name).await?;
    let needs = resource_amounts(&task.resources);
    let placement = crate::structs::session::placement(task);
    let mut tried: Vec<String> = skip.map(str::to_string).into_iter().collect();
    let mut first = match reserve(store, job_id, &task.name, &needs, &tried, placement) {
        Ok(reservation) => Some(reservation),
        Err(NoRoom { envelopes }) => {
            return Err(Status::resource_exhausted(format!(
                "no envelope for {job_id}/{} has room to resume session {session_id} \
                 ({envelopes} nodes); its checkpoint stays in the content store, retry once \
                 sessions end",
                task.name,
            )));
        }
    };
    let mut refusals = Vec::new();
    for _ in 0..ROUTER_ATTEMPTS {
        let reservation = match first.take() {
            Some(reservation) => reservation,
            None => match reserve(store, job_id, &task.name, &needs, &tried, placement) {
                Ok(reservation) => reservation,
                Err(_) => break,
            },
        };
        let node_id = reservation.node_id.clone();
        tried.push(node_id.clone());
        let mut force = force_move;
        for _ in 0..2 {
            let answer = resume_on_node(
                store,
                &node_id,
                session_id,
                cold,
                Some((job_id, &task.name)),
                force,
            )
            .await;
            let status = match answer {
                Ok(reply) => {
                    if !reply.already_running {
                        reservation.settle(Settle::Started(needs));
                    }
                    return Ok(StoreResume::Resumed {
                        node_id,
                        reply: Box::new(reply),
                    });
                }
                Err(status) => status,
            };
            let elsewhere = status.code() == tonic::Code::FailedPrecondition
                && status.message().starts_with("running_elsewhere:");
            if !elsewhere || force {
                match status.code() {
                    // The manifest is gone, the claim was lost, or the
                    // caller cannot see it: the same everywhere.
                    tonic::Code::NotFound
                    | tonic::Code::Aborted
                    | tonic::Code::PermissionDenied => return Err(status),
                    _ => {
                        if matches!(
                            status.code(),
                            tonic::Code::ResourceExhausted | tonic::Code::FailedPrecondition
                        ) {
                            note_full(store, job_id, &task.name, &node_id);
                        }
                        refusals.push(format!("{node_id}: {}", status.message()));
                        break;
                    }
                }
            }
            let Some((holder, generation)) =
                crate::session_manifest::winner_from_message(status.message())
            else {
                return Err(status);
            };
            match ask_node_within(store, &holder, session_id, MOVE_PROBE_TIMEOUT).await {
                NodeAnswer::Holds(found) if found.state != "suspended" => {
                    return Ok(StoreResume::Running { node_id: holder });
                }
                NodeAnswer::Holds(_)
                | NodeAnswer::Absent
                | NodeAnswer::Evicted(_)
                | NodeAnswer::WrongNode => force = true,
                NodeAnswer::Failed(cause) => {
                    if !node_is_down(store, &holder) {
                        return Err(Status::unavailable(format!(
                            "running_elsewhere: session {session_id} runs on node {holder} \
                             (generation {generation}), which did not answer within {}s ({}). \
                             Its node is not marked down, so the session was not moved\nHint: \
                             retry when the node is back, or pass --force-move once you know \
                             it is down [winner node={holder} generation={generation}]",
                            MOVE_PROBE_TIMEOUT.as_secs(),
                            cause.message().split("\nHint:").next().unwrap_or_default()
                        )));
                    }
                    force = true;
                }
            }
        }
    }
    Err(Status::resource_exhausted(format!(
        "no node resumed session {session_id} ({}); its checkpoint stays in the content store, \
         retry once sessions end",
        refusals.join("; ")
    )))
}

/// Stop the later copies of a duplicated session, in the background.
fn stop_duplicates(store: &Store, later: Vec<IndexedSession>) {
    if later.is_empty() {
        return;
    }
    let store = store.clone();
    tokio::spawn(async move {
        for copy in later {
            let Some(status) = copy.status else {
                continue;
            };
            info!(
                session_id = %status.session_id,
                node_id = %copy.node_id,
                "Stopping a duplicate session copy"
            );
            if let Err(err) =
                stop_on_node(&store, &copy.node_id, &[status.session_id], "duplicate").await
            {
                debug!(node_id = %copy.node_id, error = %err, "Duplicate stop failed");
            }
        }
    });
}

/// End sessions on one node (`Worker.StopSessions`). Returns the ids the
/// node ended.
pub async fn stop_on_node(
    store: &Store,
    node_id: &str,
    session_ids: &[String],
    reason: &str,
) -> Result<Vec<String>, Status> {
    let (mut client, target) = worker_client(store, node_id)?;
    let mut request = tonic::Request::new(crate::structs::StopSessionsRequest {
        session_ids: session_ids.to_vec(),
        reason: reason.to_string(),
    });
    request.set_timeout(WORKER_CALL_TIMEOUT);
    let stopped = client
        .stop_sessions(request)
        .await
        .map_err(|status| {
            let status = crate::manager::worker_plane::worker_call_error(
                &target.node_id,
                &target.address,
                status,
            );
            Status::unavailable(format!(
                "node {node_id} could not be reached to stop the session: {}",
                status.message()
            ))
        })?
        .into_inner()
        .stopped;
    for session_id in &stopped {
        store.sessions().record_stop(node_id, session_id);
    }
    crate::manager::telemetry::metrics()
        .session_stops_total
        .inc_by(&[reason], stopped.len() as u64);
    Ok(stopped)
}

/// The wire shape of a running session (`InstanceInfo` with `mode =
/// "session"`, `durable = false`).
pub fn instance_info(entry: &IndexedSession, group: &str) -> InstanceInfo {
    let status = entry.status.clone().unwrap_or_default();
    let started = rfc3339_ms(status.lease_issued_at_unix_ms);
    InstanceInfo {
        instance_id: status.session_id,
        job_id: status.job_id,
        group: group.to_string(),
        task: status.task_name,
        mode: crate::structs::InstanceMode::Session.to_string(),
        state: match status.state.as_str() {
            "running" => "active",
            "suspended" => "suspended",
            _ => "pending",
        }
        .to_string(),
        created_at: started.clone(),
        updated_at: started.clone(),
        durable: false,
        named: false,
        node_id: Some(entry.node_id.clone()),
        lease_issued_at: Some(started),
        envelope_alloc_id: (status.envelope_allocation_id > 0)
            .then_some(status.envelope_allocation_id),
        ..Default::default()
    }
}

/// The group a session task's sessions report under: the task's own group.
pub fn group_of(config: &JobConfig, task_name: &str) -> String {
    config
        .tasks
        .iter()
        .find(|task| task.name == task_name)
        .map(|task| task.group.clone())
        .unwrap_or_default()
}

/// One node's envelope for a session task, as the store has it placed.
#[derive(Debug, Clone)]
struct Envelope {
    node_id: String,
    room: BTreeMap<String, f64>,
}

/// The router's cached view of one task's envelopes and their use.
#[derive(Debug, Clone)]
struct RoomView {
    read_at: Instant,
    envelopes: Vec<Envelope>,
    used: HashMap<String, BTreeMap<String, f64>>,
}

/// The router's state for one session task: its cached view, and the
/// starts it has sent and not yet heard back from.
#[derive(Debug, Default)]
struct RoomState {
    view: Option<RoomView>,
    /// A refresh is reading the view again. Other starts keep using the
    /// stale one meanwhile, so an expiry costs one read, not one per start
    /// in flight.
    refreshing: bool,
    /// What each node's unanswered starts hold. Counted when a start picks
    /// its node, so concurrent creates spread across nodes instead of all
    /// taking the one that had the most room. Kept apart from `view`, so a
    /// refresh does not drop it.
    in_flight: HashMap<String, BTreeMap<String, f64>>,
}

type RoomKey = (usize, String, String);

fn room_cache() -> &'static Mutex<HashMap<RoomKey, RoomState>> {
    static CACHE: OnceLock<Mutex<HashMap<RoomKey, RoomState>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_room_cache() -> std::sync::MutexGuard<'static, HashMap<RoomKey, RoomState>> {
    room_cache().lock().unwrap_or_else(|p| p.into_inner())
}

/// Keyed by the store's identity too, so two clusters in one process (the
/// test suites) never share a view.
fn room_key(store: &Store, job_id: &str, task_name: &str) -> RoomKey {
    (
        std::sync::Arc::as_ptr(store.sessions()) as usize,
        job_id.to_string(),
        task_name.to_string(),
    )
}

/// Every node that holds a live envelope allocation for a session task: the
/// nodes its sessions can start on, and the nodes a listing must hear from to
/// be complete.
pub fn envelope_nodes(store: &Store, job_id: &str, task_name: &str) -> R<Vec<String>> {
    Ok(placed_envelopes(store, job_id, task_name)?
        .into_iter()
        .map(|envelope| envelope.node_id)
        .collect())
}

fn placed_envelopes(store: &Store, job_id: &str, task_name: &str) -> R<Vec<Envelope>> {
    let mut configs: HashMap<String, std::sync::Arc<Vec<(i64, TaskConfig)>>> = HashMap::new();
    let mut envelopes: BTreeMap<String, Envelope> = BTreeMap::new();
    for allocation in store.allocs_for_job(job_id)? {
        if !crate::constants::allocation_status_is_dispatchable(&allocation.status) {
            continue;
        }
        if !configs.contains_key(&allocation.job_config_id) {
            configs.insert(
                allocation.job_config_id.clone(),
                store.shared_task_configs_for_job_config(&allocation.job_config_id)?,
            );
        }
        let serves = configs[&allocation.job_config_id]
            .iter()
            .find(|(id, _)| *id == allocation.task_config_id)
            .is_some_and(|(_, task)| {
                task.runtime == ENVELOPE_RUNTIME && envelope_serves(task) == Some(task_name)
            });
        if !serves {
            continue;
        }
        // During a job update a node can briefly hold two; either is the
        // same room.
        envelopes.insert(
            allocation.node_id.clone(),
            Envelope {
                node_id: allocation.node_id.clone(),
                room: resource_amounts(&allocation.resources),
            },
        );
    }
    Ok(envelopes.into_values().collect())
}

/// Make sure the router holds a view of a task's envelopes, reading it
/// again once it is older than [`ROUTER_VIEW_TTL`]. Single-flight: while one
/// start reads, the others pick from the stale view instead of each reading
/// the store and asking every peer at the same moment.
async fn refresh_room(store: &Store, job_id: &str, task_name: &str) -> Result<(), Status> {
    let key = room_key(store, job_id, task_name);
    {
        let mut cache = lock_room_cache();
        let state = cache.entry(key.clone()).or_default();
        match &state.view {
            Some(view) if view.read_at.elapsed() < ROUTER_VIEW_TTL => return Ok(()),
            Some(_) if state.refreshing => return Ok(()),
            _ => state.refreshing = true,
        }
    }
    // Clears `refreshing` however this ends, a cancelled start included.
    struct Refreshing(RoomKey);
    impl Drop for Refreshing {
        fn drop(&mut self) {
            if let Some(state) = lock_room_cache().get_mut(&self.0) {
                state.refreshing = false;
            }
        }
    }
    let _refreshing = Refreshing(key.clone());
    let envelopes = placed_envelopes(store, job_id, task_name)
        .map_err(|err| Status::internal(format!("failed to read envelopes: {err:#}")))?;
    let usage = query(
        store,
        QuerySessionsRequest {
            job_id: job_id.to_string(),
            task_name: task_name.to_string(),
            usage_only: true,
            ..Default::default()
        },
    )
    .await
    .usage;
    let view = RoomView {
        read_at: Instant::now(),
        envelopes,
        used: usage
            .into_iter()
            .map(|(node_id, usage)| (node_id, usage.amounts))
            .collect(),
    };
    lock_room_cache().entry(key).or_default().view = Some(view);
    Ok(())
}

/// Why [`reserve`] found no node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NoRoom {
    /// How many nodes hold an envelope for the task.
    envelopes: usize,
}

/// Pick a node with room for `needs`, skipping `tried`, by the task's
/// `stateful.placement`, and count the start against it until it is
/// answered.
fn reserve(
    store: &Store,
    job_id: &str,
    task_name: &str,
    needs: &BTreeMap<String, f64>,
    tried: &[String],
    placement: SessionPlacement,
) -> Result<Reservation, NoRoom> {
    let key = room_key(store, job_id, task_name);
    let mut cache = lock_room_cache();
    let state = cache.entry(key.clone()).or_default();
    let Some(view) = state.view.as_ref() else {
        return Err(NoRoom { envelopes: 0 });
    };
    let rng = &mut rand::thread_rng();
    let picked = match placement {
        SessionPlacement::Spread => best_node(view, &state.in_flight, needs, tried, rng),
        SessionPlacement::Fast => two_choices(view, &state.in_flight, needs, tried, rng),
    }
    // The view is up to a refresh old and does not see stops, while the
    // node is authoritative. When the view says every envelope is full, a
    // first attempt still asks the node it thinks least full, so a view
    // behind on stops costs one round trip, not a refusal.
    .or_else(|| {
        tried
            .is_empty()
            .then(|| least_full(view, &state.in_flight, needs))
            .flatten()
    });
    let node_id = picked.ok_or(NoRoom {
        envelopes: view.envelopes.len(),
    })?;
    add_amounts(
        state.in_flight.entry(node_id.clone()).or_default(),
        needs,
        1.0,
    );
    Ok(Reservation {
        key,
        node_id,
        needs: needs.clone(),
        settled: false,
    })
}

/// How much of `envelope` would be left once a session of `needs` is added:
/// the smallest fraction of any sized resource, counting the view's use and
/// the router's unanswered starts. `None` when the session does not fit.
fn room_left(
    view: &RoomView,
    in_flight: &HashMap<String, BTreeMap<String, f64>>,
    envelope: &Envelope,
    needs: &BTreeMap<String, f64>,
) -> Option<f64> {
    let used = view.used.get(&envelope.node_id);
    let pending = in_flight.get(&envelope.node_id);
    let mut left = f64::INFINITY;
    for (name, need) in needs {
        let Some(room) = envelope.room.get(name) else {
            continue;
        };
        let taken = used.and_then(|used| used.get(name)).copied().unwrap_or(0.0)
            + pending
                .and_then(|pending| pending.get(name))
                .copied()
                .unwrap_or(0.0);
        let free = room - taken - need;
        if free < -1e-9 {
            return None;
        }
        if *room > 0.0 {
            left = left.min(free / room);
        }
    }
    Some(left)
}

/// The envelope the view thinks has the most room once `needs` is added,
/// even when that is less than nothing: the one node worth asking when the
/// view finds none with room.
fn least_full(
    view: &RoomView,
    in_flight: &HashMap<String, BTreeMap<String, f64>>,
    needs: &BTreeMap<String, f64>,
) -> Option<String> {
    let left = |envelope: &Envelope| -> f64 {
        let used = view.used.get(&envelope.node_id);
        let pending = in_flight.get(&envelope.node_id);
        needs
            .iter()
            .filter_map(|(name, need)| {
                let room = *envelope.room.get(name)?;
                let taken = used.and_then(|used| used.get(name)).copied().unwrap_or(0.0)
                    + pending
                        .and_then(|pending| pending.get(name))
                        .copied()
                        .unwrap_or(0.0);
                (room > 0.0).then(|| (room - taken - need) / room)
            })
            .fold(f64::INFINITY, f64::min)
    };
    view.envelopes
        .iter()
        .max_by(|a, b| left(a).total_cmp(&left(b)))
        .map(|envelope| envelope.node_id.clone())
}

/// `stateful.placement = "spread"`: the node with the most room left; a tie
/// goes to a random one of the tied nodes, so managers that read the same
/// view do not all pick the same node.
fn best_node(
    view: &RoomView,
    in_flight: &HashMap<String, BTreeMap<String, f64>>,
    needs: &BTreeMap<String, f64>,
    tried: &[String],
    rng: &mut impl rand::Rng,
) -> Option<String> {
    let mut best: Option<(f64, &str)> = None;
    let mut ties = 0u32;
    for envelope in &view.envelopes {
        if tried.contains(&envelope.node_id) {
            continue;
        }
        let Some(left) = room_left(view, in_flight, envelope, needs) else {
            continue;
        };
        match best {
            Some((most, _)) if left < most - 1e-12 => {}
            // Reservoir sampling over the ties: each of k tied nodes ends
            // up picked with probability 1/k, in one pass.
            Some((most, _)) if left <= most + 1e-12 => {
                ties += 1;
                if rng.gen_range(0..ties) == 0 {
                    best = Some((most, &envelope.node_id));
                }
            }
            _ => {
                best = Some((left, &envelope.node_id));
                ties = 1;
            }
        }
    }
    best.map(|(_, node_id)| node_id.to_string())
}

/// How many random envelopes `fast` placement reads looking for two with
/// room before it falls back to reading them all.
const FAST_SAMPLES: usize = 8;

/// `stateful.placement = "fast"`: power of two choices. Of two envelopes
/// sampled at random among those with room, the one with more left. Reads
/// at most [`FAST_SAMPLES`] envelopes whatever the fleet's size, and falls
/// back to [`best_node`] when they turn up nothing, which happens only when
/// most of the fleet is full or already tried.
fn two_choices(
    view: &RoomView,
    in_flight: &HashMap<String, BTreeMap<String, f64>>,
    needs: &BTreeMap<String, f64>,
    tried: &[String],
    rng: &mut impl rand::Rng,
) -> Option<String> {
    if view.envelopes.is_empty() {
        return None;
    }
    let mut best: Option<(f64, &str)> = None;
    let mut fitting = 0;
    for _ in 0..FAST_SAMPLES {
        let envelope = &view.envelopes[rng.gen_range(0..view.envelopes.len())];
        if tried.contains(&envelope.node_id) {
            continue;
        }
        let Some(left) = room_left(view, in_flight, envelope, needs) else {
            continue;
        };
        if best.is_none_or(|(most, _)| left > most) {
            best = Some((left, &envelope.node_id));
        }
        fitting += 1;
        if fitting == 2 {
            break;
        }
    }
    match best {
        Some((_, node_id)) => Some(node_id.to_string()),
        None => best_node(view, in_flight, needs, tried, rng),
    }
}

fn add_amounts(totals: &mut BTreeMap<String, f64>, amounts: &BTreeMap<String, f64>, sign: f64) {
    for (name, amount) in amounts {
        *totals.entry(name.clone()).or_insert(0.0) += sign * amount;
    }
}

/// One start's hold on a node's room, from [`reserve`] until the node
/// answers. Dropped unsettled (the start failed, or its caller went away),
/// it only gives the room back.
struct Reservation {
    key: RoomKey,
    node_id: String,
    needs: BTreeMap<String, f64>,
    settled: bool,
}

/// What a node's answer to a start says about its room.
enum Settle {
    /// It took the session: count it until a refresh reads it back.
    Started(BTreeMap<String, f64>),
    /// It refused for room: count the node as full until the next refresh.
    Full,
    /// Anything else: give the room back.
    Released,
}

impl Reservation {
    fn settle(mut self, outcome: Settle) {
        self.settled = true;
        let mut cache = lock_room_cache();
        let Some(state) = cache.get_mut(&self.key) else {
            return;
        };
        release(state, &self.node_id, &self.needs);
        let Some(view) = state.view.as_mut() else {
            return;
        };
        match outcome {
            Settle::Started(amounts) => {
                add_amounts(
                    view.used.entry(self.node_id.clone()).or_default(),
                    &amounts,
                    1.0,
                );
            }
            Settle::Full => {
                if let Some(envelope) = view.envelopes.iter().find(|e| e.node_id == self.node_id) {
                    view.used
                        .insert(self.node_id.clone(), envelope.room.clone());
                }
            }
            Settle::Released => {}
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        if let Some(state) = lock_room_cache().get_mut(&self.key) {
            release(state, &self.node_id, &self.needs);
        }
    }
}

fn release(state: &mut RoomState, node_id: &str, needs: &BTreeMap<String, f64>) {
    if let Some(pending) = state.in_flight.get_mut(node_id) {
        add_amounts(pending, needs, -1.0);
        if pending.values().all(|amount| amount.abs() < 1e-9) {
            state.in_flight.remove(node_id);
        }
    }
}

/// Count `node_id` as full in the router's view of a task until the next
/// refresh: it refused a session for room outside [`start`] (a resume on
/// the session's own node).
pub fn note_full(store: &Store, job_id: &str, task_name: &str, node_id: &str) {
    let key = room_key(store, job_id, task_name);
    let mut cache = lock_room_cache();
    let Some(view) = cache.get_mut(&key).and_then(|state| state.view.as_mut()) else {
        return;
    };
    if let Some(envelope) = view.envelopes.iter().find(|e| e.node_id == node_id) {
        view.used.insert(node_id.to_string(), envelope.room.clone());
    }
}

/// Every node holding an envelope for a session task, for an edge picker
/// (`Manager.ListSessionNodes`). Read from the store's envelope rows and
/// node rows plus the session index's usage; no node is called.
pub async fn session_nodes(
    store: &Store,
    job_id: &str,
    task: &TaskConfig,
) -> Result<Vec<crate::structs::SessionNode>, Status> {
    let envelopes = placed_envelopes(store, job_id, &task.name)
        .map_err(|err| Status::internal(format!("failed to read envelopes: {err:#}")))?;
    let usage = query(
        store,
        QuerySessionsRequest {
            job_id: job_id.to_string(),
            task_name: task.name.clone(),
            usage_only: true,
            ..Default::default()
        },
    )
    .await
    .usage;
    let needs = resource_amounts(&task.resources);
    let mut nodes: Vec<crate::structs::SessionNode> = envelopes
        .into_iter()
        .map(|envelope| {
            let node = store.get_node(&envelope.node_id).ok();
            let used = usage.get(&envelope.node_id);
            let room: BTreeMap<String, f64> = envelope
                .room
                .iter()
                .map(|(name, amount)| {
                    let taken = used
                        .and_then(|used| used.amounts.get(name))
                        .copied()
                        .unwrap_or(0.0);
                    (name.clone(), (amount - taken).max(0.0))
                })
                .collect();
            let weight = fits(&room, &needs);
            let address = node
                .as_ref()
                .and_then(crate::manager::worker_plane::worker_address)
                .unwrap_or_default();
            let reason = session_node_reason(node.as_ref(), &address, weight);
            crate::structs::SessionNode {
                tag: crate::structs::node_tag(&envelope.node_id),
                node_id: envelope.node_id,
                address,
                accepting: reason.is_empty(),
                reason: reason.to_string(),
                room: serde_json::to_string(&room).unwrap_or_else(|_| "{}".into()),
                weight,
                sessions: used.map(|used| used.sessions).unwrap_or(0),
            }
        })
        .collect();
    nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    Ok(nodes)
}

/// Why a node holding an envelope does not accept edge session starts, or
/// `""` when it does (`SessionNode.reason`).
fn session_node_reason(
    node: Option<&crate::structs::Node>,
    address: &str,
    weight: u32,
) -> &'static str {
    match node {
        None => "lost",
        Some(node) if node.status == "lost" => "lost",
        // A registered, heartbeating node is `active` (`ready` is what the
        // node-ready eval writes, and the next beat turns it `active`).
        // Matching only `ready` once reported every live node as down.
        Some(node) if node.status != "active" && node.status != "ready" => "down",
        Some(node) if node.availability == "draining" || node.availability == "drained" => {
            "draining"
        }
        Some(node) if node.availability != "available" => "ineligible",
        Some(_) if address.is_empty() => "no-address",
        Some(node)
            if !crate::structs::snapshot::node_advertises(
                node,
                crate::structs::session::EDGE_SESSIONS_CAPABILITY,
            ) =>
        {
            "no-edge-sessions"
        }
        Some(_) if weight == 0 => "full",
        Some(_) => "",
    }
}

/// How many sessions needing `needs` fit in `room`. A resource the session
/// does not size does not bound it; a session that sizes nothing fits
/// without bound (reported as `u32::MAX`).
fn fits(room: &BTreeMap<String, f64>, needs: &BTreeMap<String, f64>) -> u32 {
    let mut fit = f64::INFINITY;
    for (name, need) in needs {
        if *need <= 0.0 {
            continue;
        }
        let Some(left) = room.get(name) else {
            continue;
        };
        fit = fit.min((left / need + 1e-9).floor());
    }
    if fit.is_infinite() {
        u32::MAX
    } else {
        fit.clamp(0.0, u32::MAX as f64) as u32
    }
}

/// A session the router started (or found running, for a retried
/// idempotent create).
#[derive(Debug, Clone)]
pub struct StartedSession {
    /// The session's id: the derived id when one was asked for, else the id
    /// minted for the node that took it (it carries that node's tag).
    pub session_id: String,
    pub node_id: String,
    pub state: String,
    pub deduplicated: bool,
}

/// Start an unnamed session of a session task: pick a node whose envelope
/// has room, send it the start, and try the next node if it refuses.
/// `derived_id` is the id an idempotency key derived, which makes a retry
/// find the running session instead of starting a second. Without one, each
/// attempt mints an id carrying the tag of the node it is sent to
/// ([`crate::structs::mint_session_id`]), so a later read goes straight to
/// that node.
pub async fn start(
    store: &Store,
    config: &JobConfig,
    task: &TaskConfig,
    derived_id: Option<&str>,
) -> Result<StartedSession, Status> {
    let metrics = crate::manager::telemetry::metrics();
    let job_id = config.name.as_str();
    if let Some(session_id) = derived_id {
        if let Some(existing) = find(store, session_id).await {
            metrics.session_starts_total.inc(&["deduplicated"]);
            return Ok(StartedSession {
                session_id: session_id.to_string(),
                node_id: existing.node_id.clone(),
                state: existing
                    .status
                    .map(|status| status.state)
                    .unwrap_or_default(),
                deduplicated: true,
            });
        }
    }
    refresh_room(store, job_id, &task.name).await?;
    let needs = resource_amounts(&task.resources);
    let placement = crate::structs::session::placement(task);
    let mut first = match reserve(store, job_id, &task.name, &needs, &[], placement) {
        Ok(reservation) => Some(reservation),
        Err(NoRoom { envelopes: 0 }) => {
            metrics.session_starts_total.inc(&["no_envelope"]);
            return Err(Status::failed_precondition(format!(
                "no node runs an envelope for {job_id}/{} yet; the scheduler places one on \
                 every eligible node that reports the `sessions` capability (see `legion job \
                 status {job_id}`)",
                task.name
            )));
        }
        Err(NoRoom { envelopes }) => {
            metrics.session_starts_total.inc(&["no_room"]);
            return Err(Status::resource_exhausted(format!(
                "every envelope for {job_id}/{} is full ({envelopes} nodes); sessions free room \
                 as they end, or raise stateful.envelope.resources",
                task.name,
            )));
        }
    };
    let task_config_id = store
        .shared_task_configs_for_job_config(&config.id())
        .map_err(|err| Status::internal(format!("failed to read task configs: {err:#}")))?
        .iter()
        .find(|(_, candidate)| candidate.name == task.name)
        .map(|(id, _)| *id)
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "job {job_id} has no task config for {} yet",
                task.name
            ))
        })?;
    let band = store
        .cached_band_for_job_config(&config.id())
        .map(|band| band.as_str().to_string())
        .unwrap_or_default();
    let mut refusals = Vec::new();
    let mut tried: Vec<String> = Vec::new();
    while tried.len() < ROUTER_ATTEMPTS {
        let reservation = match first.take() {
            Some(reservation) => reservation,
            None => match reserve(store, job_id, &task.name, &needs, &tried, placement) {
                Ok(reservation) => reservation,
                Err(_) => break,
            },
        };
        let node_id = reservation.node_id.clone();
        tried.push(node_id.clone());
        let issued_at = now_unix_ms();
        let minted;
        let session_id = match derived_id {
            Some(id) => id,
            None => {
                minted = crate::structs::mint_session_id(&task.name, &node_id);
                minted.as_str()
            }
        };
        match send_start(
            store,
            config,
            task,
            task_config_id,
            &band,
            &node_id,
            session_id,
            issued_at,
        )
        .await
        {
            Ok(()) => {
                let status = SessionStatus {
                    session_id: session_id.to_string(),
                    job_id: job_id.to_string(),
                    job_config_id: config.id(),
                    task_name: task.name.clone(),
                    envelope_allocation_id: 0,
                    state: "starting".into(),
                    lease_issued_at_unix_ms: issued_at,
                    resources: task.resources.to_string(),
                    ..Default::default()
                };
                store
                    .sessions()
                    .record_start(&node_id, status, Instant::now());
                reservation.settle(Settle::Started(needs.clone()));
                metrics.session_starts_total.inc(&["started"]);
                debug!(session_id, node_id = %node_id, "Session started");
                return Ok(StartedSession {
                    session_id: session_id.to_string(),
                    node_id,
                    state: "starting".into(),
                    deduplicated: false,
                });
            }
            Err(status) => {
                // An idempotent `store` start that another node created
                // first (session durability plan §6.7, row 3): the session
                // exists, on the node the refusal names.
                if status.code() == tonic::Code::AlreadyExists {
                    if let Some((winner, _)) =
                        crate::session_manifest::winner_from_message(status.message())
                    {
                        metrics.session_starts_total.inc(&["deduplicated"]);
                        return Ok(StartedSession {
                            session_id: session_id.to_string(),
                            node_id: winner,
                            state: "running".into(),
                            deduplicated: true,
                        });
                    }
                }
                reservation.settle(
                    if matches!(
                        status.code(),
                        tonic::Code::ResourceExhausted | tonic::Code::FailedPrecondition
                    ) {
                        Settle::Full
                    } else {
                        Settle::Released
                    },
                );
                debug!(
                    session_id,
                    node_id = %node_id,
                    code = ?status.code(),
                    "Node refused a session start"
                );
                refusals.push(format!("{node_id}: {}", status.message()));
            }
        }
    }
    metrics.session_starts_total.inc(&["refused"]);
    Err(Status::resource_exhausted(format!(
        "no node accepted the session ({}); retry once sessions end, or raise \
         stateful.envelope.resources",
        refusals.join("; ")
    )))
}

#[allow(clippy::too_many_arguments)]
async fn send_start(
    store: &Store,
    config: &JobConfig,
    task: &TaskConfig,
    task_config_id: i64,
    band: &str,
    node_id: &str,
    session_id: &str,
    issued_at: i64,
) -> Result<(), Status> {
    let (mut client, target) = worker_client(store, node_id)?;
    // The session's own identity, for the workload's environment
    // (`LEGION_INSTANCE_ID`, `LEGION_INSTANCE_MODE=session`). Never stored.
    let identity = crate::structs::Instance {
        instance_id: session_id.to_string(),
        job_id: config.name.clone(),
        group: task.group.clone(),
        task_name: task.name.clone(),
        mode: crate::structs::InstanceMode::Session,
        state: crate::structs::InstanceState::Pending,
        slot_index: None,
        current_alloc_id: None,
        storage_handle: None,
        created_at: String::new(),
        updated_at: String::new(),
        restore_from: None,
        identity_epoch: 0,
        named: false,
        promoted_from: None,
    };
    let allocation = Allocation {
        id: 0,
        status: "provisional".into(),
        resources: task.resources.clone(),
        node_id: node_id.to_string(),
        job_config_id: config.id(),
        task_config_id,
        job_id: config.name.clone(),
        instance_id: Some(session_id.to_string()),
        network: None,
    };
    let mut payload = crate::manager::dispatcher::build_task_allocation_with(
        task,
        &allocation,
        Some(&identity),
        &[],
        Some(ProvisionalAllocation {
            claim_id: session_claim_id(session_id),
            gate: Gate::Serve.to_wire(),
            lease_deadline_unix: 0,
            instance_id: session_id.to_string(),
            origin: "manager".into(),
            on_refused: "stop".into(),
            ticket_jti: String::new(),
            session: Some(SessionStart {
                lease_issued_at_unix_ms: issued_at,
            }),
        }),
    )
    .map_err(|err| Status::internal(format!("failed to build the session payload: {err:#}")))?;
    payload.priority_band = band.to_string();
    let mut request = tonic::Request::new(crate::structs::PostWorkRequest {
        allocation: Some(payload),
    });
    request.set_timeout(WORKER_CALL_TIMEOUT);
    client
        .post_work(request)
        .await
        .map(|_| ())
        .map_err(|status| {
            crate::manager::worker_plane::worker_call_error(
                &target.node_id,
                &target.address,
                status,
            )
        })
}

/// The worker plane's mTLS client for a node (one cached channel per
/// address), and the target its errors name.
fn worker_client(
    store: &Store,
    node_id: &str,
) -> Result<
    (
        crate::structs::worker_client::WorkerClient<tonic::transport::Channel>,
        crate::manager::worker_plane::WorkerTarget,
    ),
    Status,
> {
    // The read model's row, as dispatch reads it: a start must not pay a
    // store read and a parse of the node's attributes to learn its address.
    let node = match store.cached_node(node_id).ok().flatten() {
        Some(node) => node,
        None => std::sync::Arc::new(
            store
                .get_node(node_id)
                .map_err(|_| Status::not_found(format!("node {node_id} not found")))?,
        ),
    };
    let target = crate::manager::worker_plane::WorkerTarget::for_node(&node)?;
    let client = crate::manager::worker_plane::WorkerPlane::global().client_for(store, &target)?;
    Ok((client, target))
}

#[cfg(test)]
mod sessions_test {
    use super::*;
    use crate::structs::CoveredNode;

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

    fn entry(node: &str, id: &str, iat: i64, recent: bool) -> IndexedSession {
        IndexedSession {
            node_id: node.into(),
            status: Some(status(id, iat)),
            recent,
        }
    }

    fn covered(node: &str, age_ms: u64) -> CoveredNode {
        CoveredNode {
            node_id: node.into(),
            age_ms,
        }
    }

    #[test]
    fn a_node_moved_between_managers_is_read_from_the_fresher_test() {
        // Manager 0 heard from n1 a minute ago (it ended `old` since);
        // manager 1 heard from it a second ago.
        let stale = QuerySessionsReply {
            sessions: vec![entry("n1", "old", 1, false)],
            covered_nodes: vec![covered("n1", 60_000)],
            node_usage: Vec::new(),
        };
        let fresh = QuerySessionsReply {
            sessions: vec![entry("n1", "new", 2, false)],
            covered_nodes: vec![covered("n1", 1_000)],
            node_usage: Vec::new(),
        };
        let view = merge(vec![stale, fresh], 100_000);
        let ids: Vec<_> = view
            .sessions
            .iter()
            .map(|e| e.status.as_ref().unwrap().session_id.clone())
            .collect();
        assert_eq!(ids, vec!["new".to_string()]);
        assert!(view.covered.contains("n1"));
    }

    #[test]
    fn a_recent_start_counts_until_a_later_report_test() {
        let now = 100_000;
        // The node reported 5 s ago; a start issued 1 s ago is not in it yet.
        let router = QuerySessionsReply {
            sessions: vec![
                entry("n1", "fresh", now - 1_000, true),
                entry("n1", "ended", now - 20_000, true),
            ],
            covered_nodes: Vec::new(),
            node_usage: Vec::new(),
        };
        let owner = QuerySessionsReply {
            sessions: Vec::new(),
            covered_nodes: vec![covered("n1", 5_000)],
            node_usage: Vec::new(),
        };
        let view = merge(vec![router, owner], now);
        let ids: Vec<_> = view
            .sessions
            .iter()
            .map(|e| e.status.as_ref().unwrap().session_id.clone())
            .collect();
        assert_eq!(ids, vec!["fresh".to_string()]);
    }

    #[test]
    fn the_earliest_lease_wins_a_duplicate_test() {
        let (kept, later) = winner(vec![
            entry("n2", "dup", 200, false),
            entry("n1", "dup", 100, false),
            entry("n0", "dup", 200, false),
        ])
        .unwrap();
        assert_eq!(kept.node_id, "n1");
        let losers: Vec<_> = later.iter().map(|e| e.node_id.as_str()).collect();
        assert_eq!(losers, vec!["n0", "n2"]);
    }

    #[test]
    fn the_router_ranks_by_room_left_and_skips_full_nodes_test() {
        let envelope = |node: &str| Envelope {
            node_id: node.into(),
            room: [("cpu".to_string(), 2000.0), ("memory".to_string(), 2048.0)]
                .into_iter()
                .collect(),
        };
        let used = |cpu: f64, memory: f64| -> BTreeMap<String, f64> {
            [("cpu".to_string(), cpu), ("memory".to_string(), memory)]
                .into_iter()
                .collect()
        };
        let view = RoomView {
            read_at: Instant::now(),
            envelopes: vec![envelope("busy"), envelope("idle"), envelope("full")],
            used: [
                ("busy".to_string(), used(1000.0, 1024.0)),
                ("full".to_string(), used(2000.0, 2048.0)),
            ]
            .into_iter()
            .collect(),
        };
        let needs = used(500.0, 512.0);
        let none = HashMap::new();
        let mut rng = rand::thread_rng();
        let pick = |in_flight: &HashMap<String, BTreeMap<String, f64>>,
                    tried: &[String],
                    rng: &mut rand::rngs::ThreadRng| {
            best_node(&view, in_flight, &needs, tried, rng)
        };
        assert_eq!(pick(&none, &[], &mut rng).as_deref(), Some("idle"));
        assert_eq!(
            pick(&none, &["idle".to_string()], &mut rng).as_deref(),
            Some("busy")
        );
        assert_eq!(
            pick(&none, &["idle".to_string(), "busy".to_string()], &mut rng),
            None,
            "a full node is never picked"
        );
        // Starts in flight count: with 1,500 of idle's 2,000 MHz held by
        // unanswered starts, busy has more room left.
        let in_flight = HashMap::from([("idle".to_string(), used(1500.0, 512.0))]);
        assert_eq!(pick(&in_flight, &[], &mut rng).as_deref(), Some("busy"));
    }

    /// `fast` placement never picks a full or tried node, takes the roomier
    /// of its two samples, and falls back to a full read when its samples
    /// find nothing.
    #[test]
    fn fast_placement_picks_the_roomier_of_two_nodes_with_room_test() {
        let room =
            |cpu: f64| -> BTreeMap<String, f64> { BTreeMap::from([("cpu".to_string(), cpu)]) };
        let envelope = |node: &str| Envelope {
            node_id: node.into(),
            room: room(1000.0),
        };
        let needs = room(100.0);
        let none = HashMap::new();
        let mut rng = rand::thread_rng();

        // One node with room among many full ones: the samples rarely find
        // it, and the fallback read does.
        let mut envelopes: Vec<Envelope> =
            (0..64).map(|n| envelope(&format!("full-{n}"))).collect();
        envelopes.push(envelope("open"));
        let mut used: HashMap<String, BTreeMap<String, f64>> = (0..64)
            .map(|n| (format!("full-{n}"), room(1000.0)))
            .collect();
        let view = RoomView {
            read_at: Instant::now(),
            envelopes,
            used: used.clone(),
        };
        for _ in 0..50 {
            assert_eq!(
                two_choices(&view, &none, &needs, &[], &mut rng).as_deref(),
                Some("open")
            );
        }
        assert_eq!(
            two_choices(&view, &none, &needs, &["open".to_string()], &mut rng),
            None,
            "a full or tried node is never picked"
        );

        // Two nodes with room: it takes the roomier one whenever it sampled
        // both (samples are with replacement, so three picks in four), so
        // the emptier node takes about 300 of 400.
        used.clear();
        used.insert("a".into(), room(800.0));
        let view = RoomView {
            read_at: Instant::now(),
            envelopes: vec![envelope("a"), envelope("b")],
            used,
        };
        let picks: Vec<String> = (0..400)
            .filter_map(|_| two_choices(&view, &none, &needs, &[], &mut rng))
            .collect();
        let to_b = picks.iter().filter(|node| *node == "b").count();
        assert!(to_b > 250, "b, the emptier node, took {to_b} of 400 picks");
    }

    /// What one pick costs the router at each placement and fleet size: the
    /// work done under the router's lock per session start.
    ///
    /// `cargo test --release --lib -- pick_cost_measure --ignored --nocapture`
    #[test]
    #[ignore = "measurement; prints numbers"]
    fn pick_cost_measure() {
        let room = |cpu: f64, memory: f64| -> BTreeMap<String, f64> {
            BTreeMap::from([("cpu".to_string(), cpu), ("memory".to_string(), memory)])
        };
        let needs = room(60.0, 128.0);
        let none = HashMap::new();
        let mut rng = rand::thread_rng();
        for nodes in [100usize, 1_000, 10_000, 100_000] {
            let view = RoomView {
                read_at: Instant::now(),
                envelopes: (0..nodes)
                    .map(|n| Envelope {
                        node_id: format!("node-{n:06}"),
                        room: room(3200.0, 6144.0),
                    })
                    .collect(),
                // Half the fleet half full, so ranking has work to do.
                used: (0..nodes)
                    .step_by(2)
                    .map(|n| (format!("node-{n:06}"), room(1600.0, 3072.0)))
                    .collect(),
            };
            let picks = (2_000_000 / nodes).clamp(50, 200_000);
            let time = |pick: &mut dyn FnMut() -> Option<String>| {
                let started = Instant::now();
                for _ in 0..picks {
                    std::hint::black_box(pick());
                }
                started.elapsed().as_secs_f64() * 1e6 / picks as f64
            };
            let spread = time(&mut || best_node(&view, &none, &needs, &[], &mut rng));
            let fast = time(&mut || two_choices(&view, &none, &needs, &[], &mut rng));
            println!(
                "{nodes:>7} nodes: spread {spread:>9.2} us/pick ({:>9.0}/s), fast {fast:>6.2} us/pick ({:>9.0}/s)",
                1e6 / spread,
                1e6 / fast
            );
        }
    }

    /// Nodes with the same room left are picked at random, so managers that
    /// read the same view do not all send their next start to one node.
    #[test]
    fn the_router_breaks_ties_at_random_test() {
        let room: BTreeMap<String, f64> = BTreeMap::from([("cpu".to_string(), 2000.0)]);
        let view = RoomView {
            read_at: Instant::now(),
            envelopes: ["a", "b", "c", "d"]
                .iter()
                .map(|node| Envelope {
                    node_id: node.to_string(),
                    room: room.clone(),
                })
                .collect(),
            used: HashMap::new(),
        };
        let needs = BTreeMap::from([("cpu".to_string(), 500.0)]);
        let mut rng = rand::thread_rng();
        let mut picked: HashMap<String, usize> = HashMap::new();
        for _ in 0..4_000 {
            let node = best_node(&view, &HashMap::new(), &needs, &[], &mut rng).unwrap();
            *picked.entry(node).or_default() += 1;
        }
        assert_eq!(picked.len(), 4, "every tied node is picked: {picked:?}");
        assert!(
            picked.values().all(|count| *count > 700),
            "ties are spread evenly: {picked:?}"
        );
    }

    /// A start's room is held from the pick until the node answers, and
    /// given back if the start is dropped unanswered, so a cancelled create
    /// never leaks room.
    #[test]
    fn a_reservation_holds_room_until_settled_or_dropped_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::start_storage(&crate::structs::config::ManagerConfigFull {
            store_path: dir.path().join("store").to_str().unwrap_or_default().into(),
            ..Default::default()
        })?;
        let key = room_key(&store, "agents", "workspace");
        let room = BTreeMap::from([("cpu".to_string(), 1000.0)]);
        lock_room_cache().insert(
            key.clone(),
            RoomState {
                view: Some(RoomView {
                    read_at: Instant::now(),
                    envelopes: vec![Envelope {
                        node_id: "n1".into(),
                        room: room.clone(),
                    }],
                    used: HashMap::new(),
                }),
                ..Default::default()
            },
        );
        let needs = BTreeMap::from([("cpu".to_string(), 500.0)]);
        let first = reserve(
            &store,
            "agents",
            "workspace",
            &needs,
            &[],
            SessionPlacement::Spread,
        )
        .expect("room");
        let second = reserve(
            &store,
            "agents",
            "workspace",
            &needs,
            &[],
            SessionPlacement::Spread,
        )
        .expect("room");
        // Two starts in flight fill the envelope by the view. A first
        // attempt still asks the least full node, which is authoritative
        // (the view may be behind on stops)...
        let asked = reserve(
            &store,
            "agents",
            "workspace",
            &needs,
            &[],
            SessionPlacement::Spread,
        )
        .expect("the least full node is asked");
        assert_eq!(asked.node_id, "n1");
        drop(asked);
        // ...and a retry, which has asked it, finds no room.
        assert_eq!(
            reserve(
                &store,
                "agents",
                "workspace",
                &needs,
                &["n1".to_string()],
                SessionPlacement::Spread
            )
            .err(),
            Some(NoRoom { envelopes: 1 }),
            "two starts in flight fill the envelope"
        );
        drop(second);
        let third = reserve(
            &store,
            "agents",
            "workspace",
            &needs,
            &[],
            SessionPlacement::Spread,
        )
        .expect("room given back");
        first.settle(Settle::Started(needs.clone()));
        third.settle(Settle::Released);
        let cache = lock_room_cache();
        let state = &cache[&key];
        assert!(state.in_flight.is_empty(), "{:?}", state.in_flight);
        assert_eq!(
            state.view.as_ref().map(|view| view.used["n1"]["cpu"]),
            Some(500.0),
            "a started session counts until the next refresh"
        );
        Ok(())
    }

    #[test]
    fn the_node_list_weighs_room_in_sessions_of_the_task_size_test() {
        let room = BTreeMap::from([("cpu".to_string(), 2100.0), ("memory".to_string(), 512.0)]);
        let needs = BTreeMap::from([("cpu".to_string(), 500.0), ("memory".to_string(), 128.0)]);
        assert_eq!(fits(&room, &needs), 4);
        let tight = BTreeMap::from([("cpu".to_string(), 499.0)]);
        assert_eq!(fits(&tight, &needs), 0);
        assert_eq!(fits(&room, &BTreeMap::new()), u32::MAX);
    }

    /// Regression (spot check): `ListSessionNodes` compared the node's status
    /// to `ready`, but a registered, heartbeating node is `active`, so every
    /// live node with an envelope read `accepting: false, reason: "down"`.
    #[test]
    fn a_live_node_that_advertises_edge_sessions_accepts_test() {
        let node = |status: &str, availability: &str, caps: &[&str]| crate::structs::Node {
            id: "node-1".into(),
            status: status.into(),
            pool: "default".into(),
            availability: availability.into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs: serde_json::json!({}),
            fingerprint_attrs: serde_json::json!({}),
            dynamic_attrs: serde_json::json!({ "capabilities": caps }),
            health_checks: serde_json::json!({}),
        };
        let edge = ["sessions", "edge-sessions"];
        let addr = "10.0.0.1:4849";

        assert_eq!(
            session_node_reason(Some(&node("active", "available", &edge)), addr, 4),
            ""
        );
        assert_eq!(
            session_node_reason(Some(&node("ready", "available", &edge)), addr, 4),
            ""
        );
        assert_eq!(
            session_node_reason(Some(&node("lost", "available", &edge)), addr, 4),
            "lost"
        );
        assert_eq!(session_node_reason(None, addr, 4), "lost");
        assert_eq!(
            session_node_reason(Some(&node("initializing", "available", &edge)), addr, 4),
            "down"
        );
        assert_eq!(
            session_node_reason(Some(&node("active", "draining", &edge)), addr, 4),
            "draining"
        );
        assert_eq!(
            session_node_reason(Some(&node("active", "available", &["sessions"])), addr, 4),
            "no-edge-sessions"
        );
        assert_eq!(
            session_node_reason(Some(&node("active", "available", &edge)), "", 4),
            "no-address"
        );
        assert_eq!(
            session_node_reason(Some(&node("active", "available", &edge)), addr, 0),
            "full"
        );
    }
}
