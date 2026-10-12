//! The manager's half of session mode (docs/session-mode.md): the cluster
//! view of running sessions, the session router, and the calls that stop
//! sessions on their nodes.
//!
//! Nothing here writes the replicated store. A session lives on its node and
//! in the session index ([`crate::storage::session_index`]) of whichever
//! manager its node heartbeats to; every read here merges this manager's
//! index with its peers' (`RaftTransport.QuerySessions`).

use crate::prelude::*;
use crate::storage::session_index::{session_amounts, Usage};
use crate::storage::store::Store;
use crate::structs::edge_boot::Gate;
use crate::structs::session::{
    envelope_serves, resource_amounts, session_claim_id, ENVELOPE_RUNTIME,
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

/// When an idempotent create checks whether a retry raced its session onto
/// a second node: two heartbeats at the default cadence, by which time both
/// copies have reported.
const DUPLICATE_CHECK_AFTER: Duration = Duration::from_secs(20);

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

/// Of the copies of one session id, the one that stays (the earliest lease,
/// ties broken by node id) and the ones that should stop.
pub fn winner(mut copies: Vec<IndexedSession>) -> Option<(IndexedSession, Vec<IndexedSession>)> {
    copies.sort_by(|a, b| {
        let key = |entry: &IndexedSession| {
            (
                entry
                    .status
                    .as_ref()
                    .map(|status| status.lease_issued_at_unix_ms)
                    .unwrap_or_default(),
                entry.node_id.clone(),
            )
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
pub async fn find(store: &Store, session_id: &str) -> Option<IndexedSession> {
    if !crate::structs::is_minted_instance_id(session_id) {
        return None;
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
    // The worker plane's mTLS client for the node (one cached channel per
    // node and address).
    let target = crate::manager::worker_plane::target_for_node(store, node_id)?;
    let mut client = target.client.clone();
    let mut request = tonic::Request::new(crate::structs::StopSessionsRequest {
        session_ids: session_ids.to_vec(),
        reason: reason.to_string(),
    });
    request.set_timeout(WORKER_CALL_TIMEOUT);
    let stopped = client
        .stop_sessions(request)
        .await
        .map_err(|status| {
            let status = target.explain(status);
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

type RoomKey = (usize, String, String);

fn room_cache() -> &'static Mutex<HashMap<RoomKey, RoomView>> {
    static CACHE: OnceLock<Mutex<HashMap<RoomKey, RoomView>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
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

async fn room_view(store: &Store, job_id: &str, task_name: &str) -> Result<RoomView, Status> {
    let key = room_key(store, job_id, task_name);
    if let Some(view) = room_cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&key)
        .filter(|view| view.read_at.elapsed() < ROUTER_VIEW_TTL)
    {
        return Ok(view.clone());
    }
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
    room_cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(key, view.clone());
    Ok(view)
}

/// Adjust the cached view after a start or a refusal, so the next create
/// within the TTL does not pick the same full node.
fn note_in_view(store: &Store, job_id: &str, task_name: &str, node_id: &str, used: NoteUse) {
    let key = room_key(store, job_id, task_name);
    let mut cache = room_cache().lock().unwrap_or_else(|p| p.into_inner());
    let Some(view) = cache.get_mut(&key) else {
        return;
    };
    match used {
        NoteUse::Started(amounts) => {
            let entry = view.used.entry(node_id.to_string()).or_default();
            for (name, amount) in amounts {
                *entry.entry(name).or_insert(0.0) += amount;
            }
        }
        NoteUse::Full => {
            if let Some(envelope) = view.envelopes.iter().find(|e| e.node_id == node_id) {
                view.used.insert(node_id.to_string(), envelope.room.clone());
            }
        }
    }
}

enum NoteUse {
    Started(BTreeMap<String, f64>),
    Full,
}

/// Nodes with room for `needs`, most room first: by the smallest fraction of
/// any sized resource left after the session is added.
fn rank(view: &RoomView, needs: &BTreeMap<String, f64>) -> Vec<String> {
    let mut ranked: Vec<(f64, String)> = view
        .envelopes
        .iter()
        .filter_map(|envelope| {
            let used = view.used.get(&envelope.node_id);
            let mut left = f64::INFINITY;
            for (name, need) in needs {
                let Some(room) = envelope.room.get(name) else {
                    continue;
                };
                let taken = used.and_then(|used| used.get(name)).copied().unwrap_or(0.0);
                let free = room - taken - need;
                if free < -1e-9 {
                    return None;
                }
                if *room > 0.0 {
                    left = left.min(free / room);
                }
            }
            Some((left, envelope.node_id.clone()))
        })
        .collect();
    ranked.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    ranked.into_iter().map(|(_, node_id)| node_id).collect()
}

/// A session the router started (or found running, for a retried
/// idempotent create).
#[derive(Debug, Clone)]
pub struct StartedSession {
    pub node_id: String,
    pub state: String,
    pub deduplicated: bool,
}

/// Start an unnamed session of a session task: pick a node whose envelope
/// has room, send it the start, and try the next node if it refuses.
/// `derived` says the id came from an idempotency key, which makes a retry
/// find the running session instead of starting a second.
pub async fn start(
    store: &Store,
    config: &JobConfig,
    task: &TaskConfig,
    session_id: &str,
    derived: bool,
) -> Result<StartedSession, Status> {
    let metrics = crate::manager::telemetry::metrics();
    let job_id = config.name.as_str();
    if derived {
        if let Some(existing) = find(store, session_id).await {
            metrics.session_starts_total.inc(&["deduplicated"]);
            return Ok(StartedSession {
                node_id: existing.node_id.clone(),
                state: existing
                    .status
                    .map(|status| status.state)
                    .unwrap_or_default(),
                deduplicated: true,
            });
        }
    }
    let view = room_view(store, job_id, &task.name).await?;
    if view.envelopes.is_empty() {
        metrics.session_starts_total.inc(&["no_envelope"]);
        return Err(Status::failed_precondition(format!(
            "no node runs an envelope for {job_id}/{} yet; the scheduler places one on every \
             eligible node that reports the `sessions` capability (see `legion job status \
             {job_id}`)",
            task.name
        )));
    }
    let needs = resource_amounts(&task.resources);
    let candidates = rank(&view, &needs);
    if candidates.is_empty() {
        metrics.session_starts_total.inc(&["no_room"]);
        return Err(Status::resource_exhausted(format!(
            "every envelope for {job_id}/{} is full ({} nodes); sessions free room as they \
             end, or raise stateful.envelope.resources",
            task.name,
            view.envelopes.len()
        )));
    }
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
    for node_id in candidates.into_iter().take(ROUTER_ATTEMPTS) {
        let issued_at = now_unix_ms();
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
                };
                store
                    .sessions()
                    .record_start(&node_id, status.clone(), Instant::now());
                note_in_view(
                    store,
                    job_id,
                    &task.name,
                    &node_id,
                    NoteUse::Started(session_amounts(&status)),
                );
                metrics.session_starts_total.inc(&["started"]);
                if derived {
                    schedule_duplicate_check(store, session_id);
                }
                debug!(session_id, node_id = %node_id, "Session started");
                return Ok(StartedSession {
                    node_id,
                    state: "starting".into(),
                    deduplicated: false,
                });
            }
            Err(status) => {
                if matches!(
                    status.code(),
                    tonic::Code::ResourceExhausted | tonic::Code::FailedPrecondition
                ) {
                    note_in_view(store, job_id, &task.name, &node_id, NoteUse::Full);
                }
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
    let target = crate::manager::worker_plane::target_for_node(store, node_id)?;
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
    let mut client = target.client.clone();
    let mut request = tonic::Request::new(crate::structs::PostWorkRequest {
        allocation: Some(payload),
    });
    request.set_timeout(WORKER_CALL_TIMEOUT);
    client
        .post_work(request)
        .await
        .map(|_| ())
        .map_err(|status| target.explain(status))
}

/// After an idempotent start, look for a copy a concurrent retry started on
/// another node (through another manager), and stop the later one.
fn schedule_duplicate_check(store: &Store, session_id: &str) {
    let store = store.clone();
    let session_id = session_id.to_string();
    tokio::spawn(async move {
        tokio::time::sleep(DUPLICATE_CHECK_AFTER).await;
        let _ = find(&store, &session_id).await;
    });
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
        assert_eq!(
            rank(&view, &needs),
            vec!["idle".to_string(), "busy".to_string()]
        );
    }
}
