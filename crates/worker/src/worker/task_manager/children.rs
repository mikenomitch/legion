//! Child sessions on their node (docs/child-sessions.md): the owner
//! reference each child carries, admission against the owner task's
//! `children` block, and the cascade that ends children with their owner.
//!
//! The reference travels in the session's launch payload
//! (`ProvisionalAllocation.session.owner`), which the session log already
//! persists, so a restarted worker knows its children. [`ChildLink`] is the
//! in-memory index over it.
//!
//! Who ends a child:
//!
//! - **A `session` owner on this node:** this node, when the owner is
//!   stopped, destroyed, or its workload exits. A suspended owner is not
//!   ended, and an owner that moved to another node becomes a remote owner.
//! - **Every other owner** (an `instance`, an `allocation`, a session on
//!   another node): the managers, with an owner notice
//!   ([`WorkerTaskManager::apply_owner_notice`]).

use super::WorkerTaskManager;
use crate::prelude::*;
use crate::structs::children::{
    self, EVENT_DESTROYED, EVENT_PROMOTED, EVENT_STOPPED, EVENT_SUSPENDED, OWNER_ALLOCATION,
    OWNER_INSTANCE, OWNER_SESSION,
};
use crate::structs::{
    ChildInfo, OwnerNotice, OwnerRef, ProvisionalAllocation, TaskAllocation, TaskConfig,
};
use crate::worker::session_log::SessionLogState;
use std::time::Instant;

/// What a node knows about one child's owner.
#[derive(Debug, Clone, PartialEq)]
pub struct ChildLink {
    pub owner: OwnerRef,
    /// A `session` owner that does not run on this node (the child was
    /// placed elsewhere, or the owner moved): the managers say when it
    /// ended.
    pub remote: bool,
    /// The owner task's `children.suspendWithOwner`.
    pub suspend_with_owner: bool,
    /// The child's task, for listings after it is gone.
    pub task_name: String,
}

/// A child that ended, remembered so its owner can learn why.
#[derive(Debug, Clone)]
pub struct GoneChild {
    pub owner: OwnerRef,
    pub task_name: String,
    pub reason: String,
    pub at: Instant,
}

/// Why a node refused to start a child.
#[derive(Debug, Clone, PartialEq)]
pub enum ChildRefusal {
    /// Nothing with the owner's id runs on this node.
    OwnerNotFound(String),
    /// The owner's task does not declare the child task.
    NotDeclared(String),
    /// The owner is itself a child.
    OwnerIsChild(String),
    /// The owner already holds `children.max` children.
    AtMax(String),
}

impl std::fmt::Display for ChildRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChildRefusal::OwnerNotFound(message) => f.write_str(message),
            ChildRefusal::NotDeclared(message)
            | ChildRefusal::OwnerIsChild(message)
            | ChildRefusal::AtMax(message) => {
                write!(f, "{}{message}", children::REFUSED_FOR_OWNER)
            }
        }
    }
}

impl std::error::Error for ChildRefusal {}

impl ChildRefusal {
    /// The gRPC status the refusal answers with (child-sessions plan §6.3).
    pub fn to_status(&self) -> tonic::Status {
        let message = self.to_string();
        match self {
            ChildRefusal::OwnerNotFound(_) => tonic::Status::not_found(message),
            ChildRefusal::NotDeclared(_) | ChildRefusal::OwnerIsChild(_) => {
                tonic::Status::failed_precondition(message)
            }
            ChildRefusal::AtMax(_) => tonic::Status::resource_exhausted(message),
        }
    }
}

/// The owner a session start names, if any.
pub fn start_owner(provisional: &ProvisionalAllocation) -> Option<&OwnerRef> {
    provisional
        .session
        .as_ref()
        .and_then(|start| start.owner.as_ref())
        .filter(|owner| !owner.id.is_empty())
}

/// The link a launch payload carries, as it was stored at admission.
fn link_of_payload(payload: &TaskAllocation) -> Option<ChildLink> {
    let start = payload.provisional.as_ref()?.session.as_ref()?;
    let owner = start.owner.clone().filter(|owner| !owner.id.is_empty())?;
    Some(ChildLink {
        owner,
        remote: start.owner_remote,
        suspend_with_owner: start.suspend_with_owner,
        task_name: payload.task_name.clone(),
    })
}

/// Write a link back into a launch payload, so the session log keeps it.
fn store_link(payload: &mut TaskAllocation, link: Option<&ChildLink>) {
    let Some(start) = payload
        .provisional
        .as_mut()
        .and_then(|provisional| provisional.session.as_mut())
    else {
        return;
    };
    match link {
        Some(link) => {
            start.owner = Some(link.owner.clone());
            start.owner_remote = link.remote;
            start.suspend_with_owner = link.suspend_with_owner;
        }
        None => {
            start.owner = None;
            start.owner_remote = false;
            start.suspend_with_owner = false;
        }
    }
}

fn same_owner(a: &OwnerRef, b: &OwnerRef) -> bool {
    a.kind == b.kind && a.id == b.id
}

/// A running unit of this node that can own children, and its task.
#[derive(Debug, Clone)]
pub struct LocalOwner {
    pub owner: OwnerRef,
    pub job_id: String,
    pub task: TaskConfig,
}

impl WorkerTaskManager {
    /// The owner link of a session this node holds, running or suspended.
    pub fn child_link(&self, session_id: &str) -> Option<&ChildLink> {
        self.session_owners.get(session_id)
    }

    /// The task config of a session task this node serves: the session's
    /// own (it carries `children`), else the spec its envelope carries.
    fn session_task_config(
        &self,
        local_key: Option<i64>,
        job_id: &str,
        task_name: &str,
    ) -> Option<TaskConfig> {
        local_key
            .and_then(|local| self.task_configs.get(&local).cloned())
            .or_else(|| self.session_spec(job_id, task_name).map(|spec| spec.task))
    }

    /// Whether a session with this id runs or sleeps on this node.
    fn session_is_here(&self, session_id: &str) -> bool {
        self.claims
            .by_instance(session_id)
            .is_some_and(|claim| claim.is_session())
            || self
                .idle_sessions
                .get(session_id)
                .is_some_and(|row| row.state == SessionLogState::Suspended)
    }

    /// The unit the allocation behind an API socket is, as an owner: its
    /// session when it is a session, its instance when it is a keyed or
    /// slot instance, its allocation otherwise.
    pub fn self_owner(&self, allocation_id: i64) -> Option<LocalOwner> {
        let local = self.local_key(allocation_id);
        let allocation = self.allocations.get(&local)?;
        let task = self.task_configs.get(&local)?.clone();
        let session = self
            .claims
            .by_local_key(local)
            .filter(|claim| claim.is_session() && !claim.is_ratified());
        let owner = match (session, allocation.instance_id.as_deref()) {
            (Some(claim), _) => children::owner_ref(OWNER_SESSION, &claim.instance_id),
            (None, Some(instance_id)) if !instance_id.is_empty() => {
                children::owner_ref(OWNER_INSTANCE, instance_id)
            }
            _ => children::owner_ref(
                OWNER_ALLOCATION,
                &self.external_id(local).unwrap_or(allocation_id).to_string(),
            ),
        };
        Some(LocalOwner {
            owner,
            job_id: allocation.job_id.clone(),
            task,
        })
    }

    /// Find the owner a start names among the units of `job_id` this node
    /// runs. An empty `kind` tries a session, then an instance, then an
    /// allocation id.
    pub fn resolve_local_owner(&self, job_id: &str, requested: &OwnerRef) -> Option<LocalOwner> {
        let wants = |kind: &str| requested.kind.is_empty() || requested.kind == kind;
        let id = requested.id.as_str();
        if wants(OWNER_SESSION) {
            let running = self
                .claims
                .by_instance(id)
                .filter(|claim| claim.is_session() && !claim.is_ratified())
                .filter(|claim| claim.job_id == job_id)
                .map(|claim| (Some(claim.local_key), claim.task_name.clone()));
            let found = running.or_else(|| {
                self.idle_sessions
                    .get(id)
                    .filter(|row| row.state == SessionLogState::Suspended && row.job_id == job_id)
                    .map(|row| (None, row.task_name.clone()))
            });
            if let Some((local, task_name)) = found {
                let task = self.session_task_config(local, job_id, &task_name)?;
                return Some(LocalOwner {
                    owner: children::owner_ref(OWNER_SESSION, id),
                    job_id: job_id.to_string(),
                    task,
                });
            }
        }
        let owned_by_session = |local: &i64| {
            self.claims
                .by_local_key(*local)
                .is_some_and(|claim| claim.is_session() && !claim.is_ratified())
        };
        if wants(OWNER_INSTANCE) {
            let found = self.allocations.iter().find(|(local, allocation)| {
                allocation.job_id == job_id
                    && allocation.instance_id.as_deref() == Some(id)
                    && !owned_by_session(local)
                    && self.instances.contains_key(local)
            });
            if let Some((local, _)) = found {
                return Some(LocalOwner {
                    owner: children::owner_ref(OWNER_INSTANCE, id),
                    job_id: job_id.to_string(),
                    task: self.task_configs.get(local)?.clone(),
                });
            }
        }
        if wants(OWNER_ALLOCATION) {
            let allocation_id: i64 = id.parse().ok()?;
            let local = self.local_key(allocation_id);
            let allocation = self.allocations.get(&local)?;
            if allocation.job_id != job_id
                || owned_by_session(&local)
                || !self.instances.contains_key(&local)
            {
                return None;
            }
            // An allocation with an instance identity is owned through it,
            // so the reference outlives a reschedule.
            let owner = match allocation.instance_id.as_deref() {
                Some(instance_id) if !instance_id.is_empty() => {
                    children::owner_ref(OWNER_INSTANCE, instance_id)
                }
                _ => children::owner_ref(OWNER_ALLOCATION, id),
            };
            return Some(LocalOwner {
                owner,
                job_id: job_id.to_string(),
                task: self.task_configs.get(&local)?.clone(),
            });
        }
        None
    }

    /// The live children of an owner on this node, by id.
    pub fn children_of(&self, owner: &OwnerRef) -> Vec<String> {
        let mut ids: Vec<String> = self
            .session_owners
            .iter()
            .filter(|(_, link)| same_owner(&link.owner, owner))
            .map(|(id, _)| id.clone())
            .collect();
        ids.sort();
        ids
    }

    /// An owner's children on this node for a listing: live ones with their
    /// state, and the ones that ended recently as `gone` with the reason.
    pub fn child_infos(&mut self, owner: &OwnerRef, node_id: &str) -> Vec<ChildInfo> {
        self.reconcile_children();
        let mut infos: Vec<ChildInfo> = self
            .session_owners
            .iter()
            .filter(|(_, link)| same_owner(&link.owner, owner))
            .map(|(id, link)| ChildInfo {
                session_id: id.clone(),
                task_name: link.task_name.clone(),
                node_id: node_id.to_string(),
                state: self.session_state(id).unwrap_or_else(|| "starting".into()),
                reason: String::new(),
            })
            .collect();
        infos.extend(
            self.gone_children
                .iter()
                .filter(|(_, gone)| same_owner(&gone.owner, owner))
                .map(|(id, gone)| ChildInfo {
                    session_id: id.clone(),
                    task_name: gone.task_name.clone(),
                    node_id: node_id.to_string(),
                    state: "gone".into(),
                    reason: gone.reason.clone(),
                }),
        );
        infos.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        infos
    }

    /// Why a child this node held is gone, if it still remembers.
    pub fn gone_child(&self, session_id: &str) -> Option<&GoneChild> {
        self.gone_children
            .get(session_id)
            .filter(|gone| gone.at.elapsed() < children::GONE_CHILD_TTL)
    }

    /// Check a session start that names an owner, before it is admitted.
    /// `Ok(None)` for a start without one.
    ///
    /// A start a manager routed (`origin = "manager"`) was authorized there
    /// against the stored jobspec, so an owner this node does not run is
    /// accepted as the manager named it. Every other start (a ticket, the
    /// API socket) must name an owner running here whose task declares the
    /// child task.
    pub fn check_child(
        &self,
        payload: &TaskAllocation,
        provisional: &ProvisionalAllocation,
    ) -> Result<Option<ChildLink>, ChildRefusal> {
        let Some(requested) = start_owner(provisional) else {
            return Ok(None);
        };
        let start = provisional.session.as_ref().expect("checked above");
        if self.session_owners.contains_key(&requested.id) {
            return Err(ChildRefusal::OwnerIsChild(children::owner_is_child(
                requested,
            )));
        }
        let trusted = provisional.origin == "manager";
        let child_task = payload.task_name.as_str();
        let link = match self.resolve_local_owner(&payload.job_id, requested) {
            Some(local) => {
                let Some(block) = local
                    .task
                    .children
                    .as_ref()
                    .filter(|block| block.declares(child_task))
                else {
                    return Err(ChildRefusal::NotDeclared(children::not_declared(
                        &payload.job_id,
                        &local.task.name,
                        child_task,
                    )));
                };
                let held =
                    self.children_of(&local.owner).len() as u32 + start.owner_children_elsewhere;
                if held >= block.max {
                    return Err(ChildRefusal::AtMax(children::at_max(
                        &local.owner,
                        block.max,
                    )));
                }
                ChildLink {
                    owner: local.owner,
                    remote: false,
                    suspend_with_owner: block.suspend_with_owner,
                    task_name: child_task.to_string(),
                }
            }
            None if trusted && !requested.kind.is_empty() => ChildLink {
                owner: requested.clone(),
                remote: requested.kind == OWNER_SESSION,
                suspend_with_owner: start.suspend_with_owner,
                task_name: child_task.to_string(),
            },
            None => {
                return Err(ChildRefusal::OwnerNotFound(children::owner_not_found(
                    &requested.id,
                )))
            }
        };
        Ok(Some(link))
    }

    /// Record an admitted child's link. The launch payload carries the same
    /// link into the session log.
    pub fn link_child(&mut self, session_id: &str, link: ChildLink) {
        self.gone_children.remove(session_id);
        self.session_owners.insert(session_id.to_string(), link);
    }

    /// Write a child's resolved link into the payload it launches from,
    /// so the session log persists the owner as this node resolved it.
    pub fn stamp_child_link(&self, session_id: &str, payload: &mut TaskAllocation) {
        if let Some(link) = self.session_owners.get(session_id) {
            store_link(payload, Some(link));
        }
    }

    /// Rebuild the links from what the session log restored: running
    /// sessions' payloads and suspended rows.
    pub(crate) fn restore_child_links(&mut self) {
        let mut links: Vec<(String, ChildLink)> = self
            .session_payloads
            .iter()
            .filter_map(|(id, payload)| Some((id.clone(), link_of_payload(payload)?)))
            .collect();
        links.extend(self.idle_sessions.iter().filter_map(|(id, row)| {
            if row.state != SessionLogState::Suspended {
                return None;
            }
            Some((id.clone(), link_of_payload(row.payload.as_ref()?)?))
        }));
        for (id, link) in links {
            self.session_owners.entry(id).or_insert(link);
        }
    }

    /// Persist a child's changed (or removed) link: into its launch payload
    /// and its session log row.
    fn persist_link(&mut self, session_id: &str) {
        let link = self.session_owners.get(session_id).cloned();
        if let Some(payload) = self.session_payloads.get_mut(session_id) {
            store_link(payload, link.as_ref());
        }
        let idle_row = self.idle_sessions.get_mut(session_id).map(|row| {
            if let Some(payload) = row.payload.as_mut() {
                store_link(payload, link.as_ref());
            }
            row.clone()
        });
        if let Some(row) = idle_row {
            if let Some(log) = self.session_log() {
                log.put(row);
            }
            return;
        }
        let local = self
            .claims
            .by_instance(session_id)
            .filter(|claim| claim.is_session() && !claim.is_ratified())
            .map(|claim| claim.local_key);
        if let Some(local) = local {
            self.log_running_session(local);
        }
    }

    /// A `session` owner left this node without ending (it moved, or a
    /// newer copy superseded it): its children stay, and the managers say
    /// when it ends from now on.
    pub(crate) fn mark_children_remote(&mut self, owner_session_id: &str) {
        let owner = children::owner_ref(OWNER_SESSION, owner_session_id);
        for child in self.children_of(&owner) {
            if let Some(link) = self.session_owners.get_mut(&child) {
                link.remote = true;
            }
            self.persist_link(&child);
        }
    }

    /// Point an owner's children at its new reference (a promoted session
    /// became an instance).
    pub(crate) fn rekey_children(&mut self, from: &OwnerRef, to: &OwnerRef) -> usize {
        let kids = self.children_of(from);
        for child in &kids {
            if let Some(link) = self.session_owners.get_mut(child) {
                link.owner = to.clone();
                link.remote = false;
            }
            self.persist_link(child);
        }
        if !kids.is_empty() {
            info!(
                from = %children::owner_label(from),
                to = %children::owner_label(to),
                children = kids.len(),
                "Re-keyed children to their promoted owner"
            );
        }
        kids.len()
    }

    /// A session this node runs was promoted to the instance `instance_id`:
    /// its children follow it, and if it was a child itself it is detached
    /// (child-sessions plan §6.5).
    pub(crate) fn session_promoted(&mut self, session_id: &str, instance_id: &str) {
        self.rekey_children(
            &children::owner_ref(OWNER_SESSION, session_id),
            &children::owner_ref(OWNER_INSTANCE, instance_id),
        );
        if self.session_owners.remove(session_id).is_some() {
            info!(session_id, "Promoted child detached from its owner");
        }
    }

    /// End every child of an owner on this node. Returns the ids ended.
    pub fn end_children(&mut self, owner: &OwnerRef, reason: &str) -> Vec<String> {
        let kids = self.children_of(owner);
        if kids.is_empty() {
            return kids;
        }
        for child in &kids {
            self.child_end_reasons
                .insert(child.clone(), reason.to_string());
        }
        info!(
            owner = %children::owner_label(owner),
            children = kids.len(),
            reason,
            "Ending children with their owner"
        );
        let ended = self.stop_sessions(&kids, reason);
        self.reconcile_children();
        ended
    }

    /// End the children of sessions about to be stopped, before the owners
    /// themselves, so no child outlives the report of its owner's stop. A
    /// stop that keeps the owner's manifest (it moved, or lost to a newer
    /// copy) is not an end: its children stay, as a remote owner's.
    pub(crate) fn cascade_before_stop(&mut self, session_ids: &[String], keeps_manifest: bool) {
        if self.session_owners.is_empty() {
            return;
        }
        for session_id in session_ids {
            if keeps_manifest {
                self.mark_children_remote(session_id);
                continue;
            }
            let owner = children::owner_ref(OWNER_SESSION, session_id);
            let kids = self.children_of(&owner);
            if kids.is_empty() {
                continue;
            }
            for child in &kids {
                self.child_end_reasons
                    .insert(child.clone(), "owner ended".to_string());
            }
            info!(
                session_id,
                children = kids.len(),
                "Ending children with their owner"
            );
            self.stop_sessions(&kids, "owner ended");
        }
    }

    /// Settle the ownership index against what the node holds:
    ///
    /// - a child that is gone is forgotten and remembered as `gone`;
    /// - a child that was promoted is detached;
    /// - a child whose `session` owner ran here and is gone is ended.
    pub fn reconcile_children(&mut self) {
        let now = Instant::now();
        self.gone_children
            .retain(|_, gone| now.duration_since(gone.at) < children::GONE_CHILD_TTL);
        if self.session_owners.is_empty() {
            return;
        }
        let links: Vec<(String, ChildLink)> = self
            .session_owners
            .iter()
            .map(|(id, link)| (id.clone(), link.clone()))
            .collect();
        let mut orphans = Vec::new();
        for (child, link) in links {
            let claim = self.claims.by_instance(&child);
            let idle = self.idle_sessions.get(&child).map(|row| row.state);
            let promoted = claim.is_some_and(|claim| claim.is_session() && claim.is_ratified());
            let running = claim.is_some_and(|claim| claim.is_session() && !claim.is_ratified());
            if promoted {
                self.session_owners.remove(&child);
                continue;
            }
            if !running && idle != Some(SessionLogState::Suspended) {
                let reason = self.child_end_reasons.remove(&child).unwrap_or_else(|| {
                    if idle == Some(SessionLogState::Evicted) {
                        "evicted".to_string()
                    } else {
                        "exited".to_string()
                    }
                });
                self.session_owners.remove(&child);
                self.gone_children.insert(
                    child,
                    GoneChild {
                        owner: link.owner,
                        task_name: link.task_name,
                        reason,
                        at: now,
                    },
                );
                continue;
            }
            if link.owner.kind != OWNER_SESSION || link.remote {
                continue;
            }
            // A promoted owner the adoption did not re-key (same id).
            let owner_promoted = self
                .claims
                .by_instance(&link.owner.id)
                .is_some_and(|claim| claim.is_session() && claim.is_ratified());
            if owner_promoted {
                let to = children::owner_ref(OWNER_INSTANCE, &link.owner.id);
                self.rekey_children(&link.owner, &to);
                continue;
            }
            if !self.session_is_here(&link.owner.id) {
                orphans.push(child);
            }
        }
        if orphans.is_empty() {
            return;
        }
        for child in &orphans {
            self.child_end_reasons
                .insert(child.clone(), "owner ended".to_string());
        }
        info!(
            children = orphans.len(),
            "Ending children whose owner is gone"
        );
        self.stop_sessions(&orphans, "owner ended");
        // The stops above removed the claims; record them as gone now.
        let ended: Vec<String> = orphans
            .into_iter()
            .filter(|child| !self.session_is_here(child))
            .collect();
        for child in ended {
            if let Some(link) = self.session_owners.remove(&child) {
                self.child_end_reasons.remove(&child);
                self.gone_children.insert(
                    child,
                    GoneChild {
                        owner: link.owner,
                        task_name: link.task_name,
                        reason: "owner ended".into(),
                        at: now,
                    },
                );
            }
        }
    }

    /// The children to suspend because their owner did
    /// (`children.suspendWithOwner`): the running ones.
    pub fn children_to_suspend_with(&self, owner: &OwnerRef) -> Vec<String> {
        self.children_of(owner)
            .into_iter()
            .filter(|child| {
                self.session_owners
                    .get(child)
                    .is_some_and(|link| link.suspend_with_owner)
                    && self.running_session(child).is_some()
            })
            .collect()
    }

    /// Act on a manager's notice about an owner of children this node holds.
    /// Returns the children to suspend, which the caller does off the lock.
    pub fn apply_owner_notice(&mut self, notice: &OwnerNotice) -> Vec<String> {
        let Some(owner) = notice.owner.as_ref() else {
            return Vec::new();
        };
        match notice.event.as_str() {
            EVENT_STOPPED | EVENT_DESTROYED => {
                let reason = if notice.detail.is_empty() {
                    "owner ended"
                } else {
                    notice.detail.as_str()
                };
                self.end_children(owner, reason);
                Vec::new()
            }
            EVENT_PROMOTED => {
                if let Some(to) = notice.promoted_to.as_ref() {
                    self.rekey_children(owner, to);
                }
                Vec::new()
            }
            EVENT_SUSPENDED => self.children_to_suspend_with(owner),
            other => {
                debug!(event = other, "Ignoring an unknown owner notice");
                Vec::new()
            }
        }
    }
}

/// Suspend the children of an owner that just suspended, each on its own
/// thread (a suspend captures a checkpoint). A child whose task has no
/// `suspend` block keeps running.
pub fn suspend_children(manager: &Arc<Mutex<WorkerTaskManager>>, children: Vec<String>) {
    for child in children {
        let manager = manager.clone();
        let spawned = std::thread::Builder::new()
            .name("suspend-with-owner".into())
            .spawn(move || {
                match super::session_sleep::suspend_session(&manager, &child, "owner suspended") {
                    Ok(_) => info!(session_id = %child, "Suspended child with its owner"),
                    Err(err) => {
                        debug!(session_id = %child, error = %err, "Child not suspended with its owner")
                    }
                }
            });
        if let Err(err) = spawned {
            warn!(error = %err, "Failed to spawn a child suspend");
        }
    }
}

/// After a session suspended: suspend its children that follow it.
pub fn owner_suspended(manager: &Arc<Mutex<WorkerTaskManager>>, session_id: &str) {
    let children = {
        let guard = manager.lock().expect("task manager");
        if guard.session_owners.is_empty() {
            return;
        }
        guard.children_to_suspend_with(&children::owner_ref(OWNER_SESSION, session_id))
    };
    suspend_children(manager, children);
}

/// Apply a batch of owner notices (a heartbeat reply's, or a pushed one).
pub fn apply_owner_notices(manager: &Arc<Mutex<WorkerTaskManager>>, notices: &[OwnerNotice]) {
    if notices.is_empty() {
        return;
    }
    let mut to_suspend = Vec::new();
    {
        let mut guard = manager.lock().expect("task manager");
        for notice in notices {
            to_suspend.extend(guard.apply_owner_notice(notice));
        }
    }
    suspend_children(manager, to_suspend);
}
