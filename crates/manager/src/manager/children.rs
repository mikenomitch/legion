//! Child sessions, the managers' side (docs/child-sessions.md).
//!
//! A child carries a typed reference to its owner, and whoever holds the
//! child's record ends it when the owner's authority says the owner ended.
//! For a session child the record is on its node, so the managers' part is:
//!
//! - **authorizing a start from outside** ([`resolve_owner`],
//!   [`declaration`]): the owner must be a running unit of the same job
//!   whose task names the child task in `children.tasks`;
//! - **telling nodes about owners only the managers can judge**
//!   ([`owner_notices`]): an `instance` or `allocation` owner that reached
//!   a terminal state in the store, a `session` owner that was promoted,
//!   and a `session` owner on another node that ended or cannot be found.
//!
//! Nothing here writes to raft. The notices are computed from the session
//! index (which sessions carry an owner) and the store (what the owner is
//! now), on every heartbeat of a node that holds children and pushed on
//! its work stream, so a lost notice is repeated until the node's report
//! no longer lists the child.

use crate::manager::sessions;
use crate::storage::store::Store;
use crate::structs::children::{
    self, ChildrenConfig, EVENT_DESTROYED, EVENT_PROMOTED, EVENT_STOPPED, EVENT_SUSPENDED,
    OWNER_ALLOCATION, OWNER_INSTANCE, OWNER_SESSION,
};
use crate::structs::{
    ChildInfo, InstanceState, JobConfig, OwnerNotice, OwnerRef, SessionStatus, TaskConfig,
};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tonic::Status;

/// Allocation statuses that are an end (child-sessions plan §6.4).
const TERMINAL_ALLOCATION: [&str; 4] = ["complete", "failed", "stopped", "lost"];

/// An owner a create named, found.
#[derive(Debug, Clone)]
pub struct ResolvedOwner {
    pub owner: OwnerRef,
    /// The owner's task.
    pub task_name: String,
    /// The node the owner runs on, when it runs.
    pub node_id: Option<String>,
}

/// Find the owner a create names among the running units of `config`'s job.
/// An empty `kind` tries a session, then an instance, then an allocation
/// id. NOT_FOUND for anything else, with one message, so existence is not
/// revealed.
pub async fn resolve_owner(
    store: &Store,
    config: &JobConfig,
    requested: &OwnerRef,
) -> Result<ResolvedOwner, Status> {
    children::check_owner_request(requested).map_err(Status::invalid_argument)?;
    let job_id = config.name.as_str();
    let id = requested.id.trim();
    let wants = |kind: &str| requested.kind.is_empty() || requested.kind == kind;
    let not_found = || Status::not_found(children::owner_not_found(id));
    if wants(OWNER_SESSION) {
        if let Some(entry) = sessions::find(store, id).await {
            let status = entry.status.clone().unwrap_or_default();
            if status.job_id != job_id {
                return Err(not_found());
            }
            if status.owner.is_some() {
                return Err(Status::failed_precondition(children::owner_is_child(
                    &children::owner_ref(OWNER_SESSION, id),
                )));
            }
            return Ok(ResolvedOwner {
                owner: children::owner_ref(OWNER_SESSION, id),
                task_name: status.task_name,
                node_id: Some(entry.node_id),
            });
        }
    }
    let read = |err: anyhow::Error| Status::internal(format!("failed to read the owner: {err:#}"));
    if wants(OWNER_INSTANCE) {
        if let Some(instance) = store.get_instance(id).map_err(read)? {
            if instance.job_id != job_id || instance_ended(instance.state).is_some() {
                return Err(not_found());
            }
            let node_id = instance
                .current_alloc_id
                .and_then(|alloc| store.get_allocation_by_id(alloc).ok().flatten())
                .map(|alloc| alloc.node_id);
            return Ok(ResolvedOwner {
                owner: children::owner_ref(OWNER_INSTANCE, &instance.instance_id),
                task_name: instance.task_name,
                node_id,
            });
        }
    }
    if wants(OWNER_ALLOCATION) {
        if let Ok(allocation_id) = id.parse::<i64>() {
            if let Some(allocation) = store.get_allocation_by_id(allocation_id).map_err(read)? {
                if allocation.job_id != job_id
                    || TERMINAL_ALLOCATION.contains(&allocation.status.as_str())
                {
                    return Err(not_found());
                }
                let task_name = store
                    .shared_task_configs_for_job_config(&allocation.job_config_id)
                    .map_err(read)?
                    .iter()
                    .find(|(task_id, _)| *task_id == allocation.task_config_id)
                    .map(|(_, task)| task.name.clone())
                    .ok_or_else(not_found)?;
                // An allocation with an instance identity is owned through
                // it, so the reference outlives a reschedule.
                let owner = match allocation.instance_id.as_deref() {
                    Some(instance_id) if !instance_id.is_empty() => {
                        children::owner_ref(OWNER_INSTANCE, instance_id)
                    }
                    _ => children::owner_ref(OWNER_ALLOCATION, &allocation_id.to_string()),
                };
                return Ok(ResolvedOwner {
                    owner,
                    task_name,
                    node_id: Some(allocation.node_id),
                });
            }
        }
    }
    Err(not_found())
}

/// The `children` block that lets `owner_task` own a child of `child`, or
/// the FAILED_PRECONDITION that says why not (child-sessions plan §6.3).
pub fn declaration<'a>(
    config: &'a JobConfig,
    owner_task: &str,
    child: &TaskConfig,
) -> Result<&'a ChildrenConfig, Status> {
    let job_id = config.name.as_str();
    let owner = config
        .tasks
        .iter()
        .find(|task| task.name == owner_task)
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "job {job_id} no longer has task {owner_task}, the owner's task"
            ))
        })?;
    let block = owner
        .children
        .as_ref()
        .filter(|block| block.declares(&child.name))
        .ok_or_else(|| {
            Status::failed_precondition(children::not_declared(job_id, owner_task, &child.name))
        })?;
    if crate::structs::session::is_session_task(owner)
        && !crate::structs::session::is_session_task(child)
    {
        return Err(Status::failed_precondition(
            children::keyed_child_of_session(owner_task, &child.name),
        ));
    }
    Ok(block)
}

/// An owner's children across the cluster, as every manager's session
/// index lists them.
pub async fn children_of(store: &Store, job_id: &str, owner: &OwnerRef) -> Vec<ChildInfo> {
    let mut found: HashMap<String, ChildInfo> = store
        .sessions()
        .children_of(&owner.kind, &owner.id)
        .into_iter()
        .map(|(node_id, status)| (status.session_id.clone(), child_info(&node_id, &status)))
        .collect();
    // Other managers index other nodes: ask them, when any child could
    // exist at all (the job declares children).
    let view = sessions::query(
        store,
        crate::structs::QuerySessionsRequest {
            job_id: job_id.to_string(),
            ..Default::default()
        },
    )
    .await;
    for entry in view.sessions {
        let Some(status) = entry.status.as_ref() else {
            continue;
        };
        let owned = status
            .owner
            .as_ref()
            .is_some_and(|candidate| candidate.kind == owner.kind && candidate.id == owner.id);
        if owned {
            found
                .entry(status.session_id.clone())
                .or_insert_with(|| child_info(&entry.node_id, status));
        }
    }
    let mut children: Vec<ChildInfo> = found.into_values().collect();
    children.sort_by(|a, b| a.session_id.cmp(&b.session_id));
    children
}

fn child_info(node_id: &str, status: &SessionStatus) -> ChildInfo {
    ChildInfo {
        session_id: status.session_id.clone(),
        task_name: status.task_name.clone(),
        node_id: node_id.to_string(),
        state: status.state.clone(),
        reason: String::new(),
    }
}

/// Whether a job's current spec lets any task own children. Reads that
/// list children are skipped for every job that does not.
pub fn job_declares_children(config: &JobConfig) -> bool {
    config.tasks.iter().any(|task| task.children.is_some())
}

/// The terminal event an instance state is, if it is one. Suspended,
/// rescheduling, pending, and active instances have not ended.
fn instance_ended(state: InstanceState) -> Option<&'static str> {
    match state {
        InstanceState::Stopped => Some(EVENT_STOPPED),
        InstanceState::Terminated => Some(EVENT_DESTROYED),
        _ => None,
    }
}

/// What the store says about an `instance` or `allocation` owner.
#[derive(Debug, Clone, PartialEq)]
enum OwnerState {
    Alive,
    Suspended,
    Ended { event: &'static str, detail: String },
}

fn raft_owner_state(store: &Store, owner: &OwnerRef) -> OwnerState {
    match owner.kind.as_str() {
        OWNER_INSTANCE => match store.get_instance(&owner.id) {
            Ok(Some(instance)) => match instance_ended(instance.state) {
                Some(event) => OwnerState::Ended {
                    event,
                    detail: format!("instance {} {event}", owner.id),
                },
                None if instance.state == InstanceState::Suspended => OwnerState::Suspended,
                None => OwnerState::Alive,
            },
            // An unnamed instance's row is deleted on destroy.
            Ok(None) => OwnerState::Ended {
                event: EVENT_DESTROYED,
                detail: format!("instance {} destroyed", owner.id),
            },
            // A failed read is not an end.
            Err(_) => OwnerState::Alive,
        },
        OWNER_ALLOCATION => {
            let Ok(allocation_id) = owner.id.parse::<i64>() else {
                return OwnerState::Ended {
                    event: EVENT_STOPPED,
                    detail: format!("allocation {} is not an allocation id", owner.id),
                };
            };
            match store.get_allocation_by_id(allocation_id) {
                Ok(Some(allocation))
                    if TERMINAL_ALLOCATION.contains(&allocation.status.as_str()) =>
                {
                    OwnerState::Ended {
                        event: EVENT_STOPPED,
                        detail: format!("allocation {allocation_id} {}", allocation.status),
                    }
                }
                Ok(Some(_)) => OwnerState::Alive,
                Ok(None) => OwnerState::Ended {
                    event: EVENT_STOPPED,
                    detail: format!("allocation {allocation_id} is gone"),
                },
                Err(_) => OwnerState::Alive,
            }
        }
        _ => OwnerState::Alive,
    }
}

/// What this manager remembers between heartbeats about owners it has to
/// ask about: `session` owners that are not on their child's node.
#[derive(Debug, Default)]
struct Watch {
    /// By owner session id.
    remote: HashMap<String, RemoteOwner>,
    /// Owners last seen suspended, so `suspended` is sent on the change and
    /// not on every beat (a child resumed on demand must stay resumed).
    suspended: HashMap<String, Instant>,
}

#[derive(Debug, Clone)]
struct RemoteOwner {
    /// When the owner was last asked about.
    asked: Instant,
    /// A probe is in flight.
    asking: bool,
    /// When it was last found running, or first asked about.
    seen: Instant,
    /// Decided: the owner ended, and why.
    ended: Option<String>,
}

fn watches() -> &'static Mutex<HashMap<usize, Watch>> {
    static WATCHES: OnceLock<Mutex<HashMap<usize, Watch>>> = OnceLock::new();
    WATCHES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn watch_key(store: &Store) -> usize {
    std::sync::Arc::as_ptr(store.sessions()) as usize
}

/// How long a watch entry nobody asked about is kept.
const WATCH_TTL: Duration = Duration::from_secs(3600);

/// The shortest `orphanAfter` any task of the job declares for `child_task`:
/// how long a child outlives a session owner nobody can reach.
fn orphan_after(store: &Store, job_id: &str, child_task: &str) -> Duration {
    store
        .get_job_config(job_id)
        .ok()
        .and_then(|config| {
            config
                .tasks
                .iter()
                .filter_map(|task| task.children.as_ref())
                .filter(|block| block.declares(child_task))
                .map(ChildrenConfig::orphan_after)
                .min()
        })
        .unwrap_or(children::DEFAULT_ORPHAN_AFTER)
}

/// Ask where a `session` owner is, off the heartbeat path, and record the
/// answer for the next beat.
fn probe_remote_owner(store: &Store, owner_id: String, job_id: String, child_task: String) {
    let store = store.clone();
    let key = watch_key(&store);
    let fallback_id = owner_id.clone();
    let probe = async move {
        let located = sessions::locate(&store, &owner_id).await;
        let after = orphan_after(&store, &job_id, &child_task);
        let mut guard = watches().lock().unwrap_or_else(|p| p.into_inner());
        let Some(entry) = guard
            .get_mut(&key)
            .and_then(|watch| watch.remote.get_mut(&owner_id))
        else {
            return;
        };
        entry.asking = false;
        match located {
            sessions::Located::Found(_) => entry.seen = Instant::now(),
            // Its node answered, and every manager's index agrees: it ended.
            sessions::Located::NotFound | sessions::Located::Evicted(_) => {
                entry.ended = Some(format!("session {owner_id} ended"));
            }
            // Its node does not answer: an end only after `orphanAfter`.
            sessions::Located::Unreachable(_) => {
                if entry.seen.elapsed() >= after {
                    entry.ended = Some(format!(
                        "session {owner_id} has not been reachable for {}",
                        humantime::format_duration(Duration::from_secs(after.as_secs()))
                    ));
                }
            }
        }
    };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn(probe);
        }
        Err(_) => {
            // No runtime (a synchronous test): the next beat asks again.
            let mut guard = watches().lock().unwrap_or_else(|p| p.into_inner());
            if let Some(entry) = guard
                .get_mut(&key)
                .and_then(|watch| watch.remote.get_mut(&fallback_id))
            {
                entry.asking = false;
            }
        }
    }
}

/// The notices a node is owed about the owners of the child sessions its
/// last report lists (`HeartbeatReply.owner_notices`, and pushed on
/// `StreamWork`). Free for a node that reports no child.
pub fn owner_notices(store: &Store, node_id: &str) -> Vec<OwnerNotice> {
    let owned = store.sessions().owned_on(node_id);
    if owned.is_empty() {
        return Vec::new();
    }
    // One decision per owner, whatever number of children it has here.
    let mut owners: HashMap<(String, String), &SessionStatus> = HashMap::new();
    for status in &owned {
        if let Some(owner) = status.owner.as_ref() {
            owners
                .entry((owner.kind.clone(), owner.id.clone()))
                .or_insert(status);
        }
    }
    let key = watch_key(store);
    let mut notices = Vec::new();
    for ((kind, id), child) in owners {
        let owner = children::owner_ref(&kind, &id);
        if children::raft_owned(&owner) {
            match raft_owner_state(store, &owner) {
                OwnerState::Alive => {
                    let mut guard = watches().lock().unwrap_or_else(|p| p.into_inner());
                    if let Some(watch) = guard.get_mut(&key) {
                        watch.suspended.remove(&children::owner_label(&owner));
                    }
                }
                OwnerState::Suspended => {
                    let mut guard = watches().lock().unwrap_or_else(|p| p.into_inner());
                    let watch = guard.entry(key).or_default();
                    let first = watch
                        .suspended
                        .insert(children::owner_label(&owner), Instant::now())
                        .is_none();
                    if first {
                        notices.push(OwnerNotice {
                            owner: Some(owner),
                            event: EVENT_SUSPENDED.into(),
                            promoted_to: None,
                            detail: format!("instance {id} suspended"),
                        });
                    }
                }
                OwnerState::Ended { event, detail } => notices.push(OwnerNotice {
                    owner: Some(owner),
                    event: event.into(),
                    promoted_to: None,
                    detail,
                }),
            }
            continue;
        }
        // A session owner on the child's own node is that node's to judge.
        if store.sessions().holds(node_id, &id) {
            continue;
        }
        // Promoted: its children follow it to the instance it became.
        if let Ok(Some(instance)) = store.instance_promoted_from(&id) {
            notices.push(OwnerNotice {
                owner: Some(owner),
                event: EVENT_PROMOTED.into(),
                promoted_to: Some(children::owner_ref(OWNER_INSTANCE, &instance.instance_id)),
                detail: format!("session {id} was promoted to {}", instance.instance_id),
            });
            continue;
        }
        // Ended through this manager moments ago.
        if store.sessions().recently_ended(&id) {
            notices.push(OwnerNotice {
                owner: Some(owner),
                event: EVENT_STOPPED.into(),
                promoted_to: None,
                detail: format!("session {id} ended"),
            });
            continue;
        }
        // Somewhere else, or gone: ask, at most once a minute.
        let now = Instant::now();
        let ask = {
            let mut guard = watches().lock().unwrap_or_else(|p| p.into_inner());
            let watch = guard.entry(key).or_default();
            watch
                .remote
                .retain(|_, entry| now.duration_since(entry.asked) < WATCH_TTL);
            match watch.remote.get_mut(&id) {
                Some(entry) => {
                    if let Some(detail) = entry.ended.clone() {
                        notices.push(OwnerNotice {
                            owner: Some(owner),
                            event: EVENT_STOPPED.into(),
                            promoted_to: None,
                            detail,
                        });
                        false
                    } else if !entry.asking
                        && now.duration_since(entry.asked) >= children::ORPHAN_CHECK_EVERY
                    {
                        entry.asked = now;
                        entry.asking = true;
                        true
                    } else {
                        false
                    }
                }
                None => {
                    watch.remote.insert(
                        id.clone(),
                        RemoteOwner {
                            asked: now,
                            asking: true,
                            seen: now,
                            ended: None,
                        },
                    );
                    true
                }
            }
        };
        if ask {
            probe_remote_owner(store, id, child.job_id.clone(), child.task_name.clone());
        }
    }
    notices.sort_by(|a, b| {
        let key = |notice: &OwnerNotice| notice.owner.as_ref().map(children::owner_label);
        key(a).cmp(&key(b))
    });
    notices
}

/// Whether a node's stream has anything to push: only nodes that report a
/// child are ever looked at.
pub fn node_has_children(store: &Store, node_id: &str) -> bool {
    store.sessions().any_owned() && !store.sessions().owned_on(node_id).is_empty()
}

#[cfg(test)]
mod children_test {
    use super::*;

    #[test]
    fn only_stopped_and_terminated_instances_have_ended_test() {
        assert_eq!(instance_ended(InstanceState::Stopped), Some(EVENT_STOPPED));
        assert_eq!(
            instance_ended(InstanceState::Terminated),
            Some(EVENT_DESTROYED)
        );
        for alive in [
            InstanceState::Pending,
            InstanceState::Active,
            InstanceState::Rescheduling,
            InstanceState::Suspended,
        ] {
            assert_eq!(instance_ended(alive), None, "{alive:?} is not an end");
        }
    }
}
