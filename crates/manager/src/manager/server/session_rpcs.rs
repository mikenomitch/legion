//! The instance RPCs on sessions (docs/session-mode.md), and PromoteInstance.
//!
//! A session has no row, so every instance handler reaches this module from
//! its NotFound path: it asks the session index (this manager's and its
//! peers', [`crate::manager::sessions`]) whether a session has the id, after
//! the handler's coarse `AclScope::any()` check and before the session's own
//! job scope is enforced, so a caller without job access cannot probe which
//! session ids exist.

use super::*;
use crate::manager::sessions;
use crate::structs::{
    IndexedSession, PromoteInstanceReply, PromoteInstanceRequest, QuerySessionsRequest,
    SuspendInstanceReply,
};
use serde::{Deserialize, Serialize};

/// The session half of a ListInstances reply.
#[derive(Debug, Default)]
pub(super) struct SessionListing {
    pub infos: Vec<InstanceInfo>,
    pub complete: bool,
    pub as_of: String,
    pub next_page_token: String,
    pub count: u64,
    pub by_node: std::collections::HashMap<String, u64>,
}

/// What a promotion records under its idempotency key.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RecordedPromotion {
    instance_id: String,
    promoted_from: String,
    node_id: String,
    allocation_id: i64,
}

impl RecordedPromotion {
    fn into_reply(self, deduplicated: bool) -> PromoteInstanceReply {
        PromoteInstanceReply {
            instance_id: self.instance_id,
            promoted_from: self.promoted_from,
            node_id: self.node_id,
            allocation_id: self.allocation_id,
            durable: true,
            deduplicated,
        }
    }
}

impl ManagerDaemonService {
    /// ListSessionNodes: the nodes an edge picker may start sessions on.
    pub(super) async fn list_session_nodes_rpc(
        &self,
        request: Request<crate::structs::ListSessionNodesRequest>,
    ) -> Result<Response<crate::structs::ListSessionNodesReply>, Status> {
        let ident = self.authenticate_acl(&request)?;
        let request = request.into_inner();
        if request.job_id.trim().is_empty() {
            return Err(Status::invalid_argument("job_id is required"));
        }
        acl::enforce(
            &ident,
            AclResource::Job,
            AclAction::Read,
            &AclScope::job(&request.job_id),
        )?;
        let config = self
            .store
            .get_job_config(&request.job_id)
            .map_err(|_| Status::not_found(format!("job {} not found", request.job_id)))?;
        let session_tasks: Vec<&TaskConfig> = config
            .tasks
            .iter()
            .filter(|task| crate::structs::session::is_session_task(task))
            .collect();
        let task = match request.task_name.as_str() {
            "" if session_tasks.len() == 1 => session_tasks[0],
            "" => {
                return Err(Status::failed_precondition(format!(
                    "job {} has {} session tasks; name one with --task (task_name in the API)",
                    request.job_id,
                    session_tasks.len()
                )))
            }
            name => session_tasks
                .iter()
                .find(|task| task.name == name)
                .copied()
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "job {} has no session task {name}",
                        request.job_id
                    ))
                })?,
        };
        let nodes = sessions::session_nodes(&self.store, &request.job_id, task).await?;
        Ok(Response::new(crate::structs::ListSessionNodesReply {
            job_id: request.job_id,
            task_name: task.name.clone(),
            nodes,
            as_of: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        }))
    }

    /// RunSession: one run in a fresh, single-use session
    /// (docs/session-runs.md).
    pub(super) async fn run_session_rpc(
        &self,
        request: Request<crate::structs::RunSessionRequest>,
    ) -> Result<Response<crate::structs::RunSessionResponse>, Status> {
        let ident = self.authenticate_acl(&request)?;
        let mut request = request.into_inner();
        if request.job_id.trim().is_empty() {
            return Err(Status::invalid_argument("job_id is required"));
        }
        let scope = AclScope::job(&request.job_id);
        acl::enforce(&ident, AclResource::Job, AclAction::Write, &scope)?;
        if !request.command.is_empty() {
            // An arbitrary command is an exec, and needs what exec needs.
            acl::enforce(&ident, AclResource::Exec, AclAction::Write, &scope).map_err(
                |status| {
                    if status.code() == tonic::Code::PermissionDenied {
                        Status::permission_denied(format!(
                            "--command needs exec:write on job {} (running the task's own \
                         run.command needs only job:write)",
                            request.job_id
                        ))
                    } else {
                        status
                    }
                },
            )?;
        }
        let config = self
            .store
            .get_job_config(&request.job_id)
            .map_err(|_| Status::not_found(format!("job {} not found", request.job_id)))?;
        let session_tasks: Vec<&TaskConfig> = config
            .tasks
            .iter()
            .filter(|task| crate::structs::session::is_session_task(task))
            .collect();
        let task = match request.task_name.as_str() {
            "" => {
                // The tasks that take runs (a `run` block, or the isolate
                // runtime), with an isolate task's own VM task left out: it
                // exists to serve the isolate task.
                let runnable =
                    crate::manager::isolates::default_run_task(&session_tasks, &config.tasks);
                match (runnable.as_slice(), session_tasks.as_slice()) {
                    ([only], _) => *only,
                    ([], [only]) => *only,
                    ([], _) => {
                        return Err(Status::failed_precondition(format!(
                            "job {} has no session task with a run block; add one \
                             (run = {{ command = [...], wall = \"30s\" }})",
                            request.job_id
                        )))
                    }
                    (several, _) => {
                        return Err(Status::failed_precondition(format!(
                            "job {} has {} session tasks that take runs; name one with --task \
                             (task_name in the API)",
                            request.job_id,
                            several.len()
                        )))
                    }
                }
            }
            name => session_tasks
                .iter()
                .find(|task| task.name == name)
                .copied()
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "job {} has no session task {name}",
                        request.job_id
                    ))
                })?,
        };
        // The isolate runtime's own fields, before anything is routed:
        // refused on other runtimes, checked against the isolate task.
        crate::manager::isolates::check_run_fields(
            &request.job_id,
            task,
            &request.tier,
            &request.tenant,
            &request.session_id,
            &request.command,
        )
        .map_err(|(code, message)| Status::new(code, message))?;
        crate::manager::isolates::check_run_args(&request.job_id, task, &request.args)
            .map_err(Status::invalid_argument)?;
        let isolate = crate::structs::isolate::is_isolate_task(task);
        let wall = if isolate {
            // No `run` block: the budgets are `config.limits`, and a run that
            // may go to the VM tier gets the VM task's `run.wall` too.
            let limits = crate::structs::run::run_limits(&request.job_id, task)
                .map_err(|err| Status::failed_precondition(format!("task {}: {err}", task.name)))?;
            crate::manager::isolates::run_wall(
                &request.tier,
                limits.wall,
                crate::manager::isolates::vm_task(task, &config.tasks),
            )
        } else {
            let run = task
                .run
                .as_ref()
                .ok_or_else(|| {
                    Status::failed_precondition(crate::structs::run::no_run_block(
                        &request.job_id,
                        &task.name,
                    ))
                })?
                .resolve()
                .map_err(|err| Status::failed_precondition(format!("task {}: {err}", task.name)))?;
            if !request.command.is_empty() && !run.allow_command {
                return Err(Status::failed_precondition(
                    crate::structs::run::command_not_allowed(&task.name),
                ));
            }
            run.wall
        };
        if !request.command.is_empty() {
            info!(
                target: "exec_audit",
                principal = %ident.principal,
                accessor_id = ident.accessor_id.as_deref().unwrap_or("-"),
                job_id = %request.job_id,
                task = %task.name,
                command = ?request.command,
                dry_run = request.dry_run,
                "session run with a caller's command"
            );
        }
        // The key a node sees is scoped to the caller and the request, as
        // every recorded idempotency key is: another caller's same key, or
        // the same key on a different request, is a different run.
        request.ticket.clear();
        let scoped = scoped_idempotency_key(
            &ident,
            "run_session",
            request.idempotency_key.as_deref(),
            &(
                &request.job_id,
                &task.name,
                &request.command,
                &request.input,
                &request.get,
                request
                    .files
                    .iter()
                    .map(|file| {
                        use sha2::{Digest, Sha256};
                        (&file.path, format!("{:x}", Sha256::digest(&file.content)))
                    })
                    .collect::<Vec<_>>(),
                (
                    &request.tier,
                    &request.tenant,
                    &request.session_id,
                    &request.args,
                ),
            ),
        );
        request.idempotency_key = (!scoped.is_empty()).then_some(scoped);
        request.task_name = task.name.clone();
        request.job_id = config.name.clone();
        sessions::run(&self.store, &config, task, wall, request)
            .await
            .map(Response::new)
    }

    /// CreateInstance without an id on a session task: start an unnamed
    /// session through the router. Writes nothing to the replicated store;
    /// an idempotency key derives the id instead of recording it.
    pub(super) async fn create_session(
        &self,
        identity: &AclIdentity,
        request: &CreateInstanceRequest,
        config: &JobConfig,
        task: &TaskConfig,
    ) -> Result<Response<CreateInstanceReply>, Status> {
        if request
            .from_snapshot
            .as_deref()
            .is_some_and(|from| !from.trim().is_empty())
        {
            return Err(Status::invalid_argument(format!(
                "task {} is a session task, and an unnamed session cannot start from a \
                 snapshot; create a named instance (instance_id) to restore one",
                task.name
            )));
        }
        let scoped = scoped_idempotency_key(
            identity,
            "create_instance",
            request.idempotency_key.as_deref(),
            &request,
        );
        let derived_id = (!scoped.is_empty()).then(|| {
            crate::structs::session::derived_session_id(&task.name, &config.name, &scoped)
        });
        // A child of a running unit of this job (docs/child-sessions.md):
        // `job:write` was enforced by the caller, the owner's jobspec
        // decides the rest.
        let child = match request.owner.as_ref().filter(|owner| !owner.id.is_empty()) {
            Some(requested) => {
                let resolved =
                    crate::manager::children::resolve_owner(&self.store, config, requested).await?;
                let block =
                    crate::manager::children::declaration(config, &resolved.task_name, task)?;
                // `max` bounds session and keyed children together: the
                // node counts the sessions, so it is told what is left.
                let keyed = if crate::structs::children::raft_owned(&resolved.owner) {
                    self.store
                        .keyed_children_of(&crate::structs::children::owner_label(&resolved.owner))
                        .map_err(|e| map_store_error("Error reading the owner's children", e))?
                        .len() as u32
                } else {
                    0
                };
                if keyed >= block.max {
                    return Err(Status::resource_exhausted(
                        crate::structs::children::at_max(&resolved.owner, block.max),
                    ));
                }
                Some(sessions::ChildStart {
                    owner: resolved.owner,
                    prefer_node: resolved.node_id,
                    max: block.max - keyed,
                    suspend_with_owner: block.suspend_with_owner,
                })
            }
            None => None,
        };
        let started = sessions::start(
            &self.store,
            config,
            task,
            derived_id.as_deref(),
            child.as_ref(),
        )
        .await?;
        Ok(Response::new(CreateInstanceReply {
            instance_id: started.session_id,
            deduplicated: started.deduplicated,
            restoring_from: String::new(),
            mode: InstanceMode::Session.to_string(),
            durable: false,
            node_id: started.node_id,
            state: started.state,
            owner: started.owner,
        }))
    }

    /// Create a keyed instance as a child of `resolved` (docs/child-sessions.md).
    /// The caller has authorized the create (`job:write`, or a worker
    /// vouching for one of its allocations); the jobspec decides the rest.
    /// One scheduler-lane entry: the row with its owner reference, the
    /// placement eval, and the idempotency record.
    pub(super) async fn create_keyed_child(
        &self,
        config: &JobConfig,
        task: &TaskConfig,
        resolved: crate::manager::children::ResolvedOwner,
        requested_id: Option<String>,
        idempotency_key: String,
    ) -> Result<Response<CreateInstanceReply>, Status> {
        use crate::storage::store::KeyedChildOutcome;
        use crate::structs::children;
        let block = crate::manager::children::declaration(config, &resolved.task_name, task)?;
        // `declaration` refuses a session task owning a keyed child; an
        // owner that is a session by kind (a promoted task's leftover) is
        // refused the same way.
        if !children::raft_owned(&resolved.owner) {
            return Err(Status::failed_precondition(
                children::keyed_child_of_session(&resolved.task_name, &task.name),
            ));
        }
        self.store
            .check_namespace_quota_growth(&config.name, config.priority, &task.resources, 1)
            .map_err(|e| map_store_error("Namespace policy refused the instance", e))?;
        // `max` bounds session and keyed children together. The session
        // count is the index's (soft state); the keyed count is read in the
        // write.
        let session_children = crate::manager::children::session_children_of(
            &self.store,
            &config.name,
            &resolved.owner,
        )
        .await
        .len() as u32;
        let max = block.max;
        let owner = resolved.owner.clone();
        let label = children::owner_label(&owner);
        let job_id = config.name.clone();
        let group = task.group.clone();
        let task_name = task.name.clone();
        let config_id = config.id();
        let priority = self.store.default_eval_priority;
        let key = idempotency_key.clone();
        let outcome = self
            .store
            .scheduler_group_commit("create_child_instance", move |s| {
                claimed_body(s, |s| {
                    let outcome = s.create_keyed_child_instance(
                        &job_id,
                        &group,
                        &task_name,
                        requested_id.as_deref(),
                        &label,
                        max,
                        session_children,
                    )?;
                    if let KeyedChildOutcome::Created(instance) = &outcome {
                        s.enqueue_eval(
                            "job_reschedule",
                            Some(&job_id),
                            Some(&config_id),
                            None,
                            Some(priority),
                        )?;
                        claim_idempotency_key(s, &key, "create_instance", &instance.instance_id)?;
                    }
                    Ok(outcome)
                })
            });
        let outcome =
            match self.claimed_write(&idempotency_key, "Error creating child instance", outcome)? {
                Ok(outcome) => outcome,
                Err(recorded) => {
                    return Ok(Response::new(CreateInstanceReply {
                        instance_id: recorded,
                        deduplicated: true,
                        restoring_from: String::new(),
                        mode: InstanceMode::Keyed.to_string(),
                        durable: true,
                        node_id: String::new(),
                        state: String::new(),
                        owner: Some(owner),
                    }))
                }
            };
        match outcome {
            KeyedChildOutcome::AtMax { .. } => {
                Err(Status::resource_exhausted(children::at_max(&owner, max)))
            }
            KeyedChildOutcome::Created(instance) => {
                info!(
                    instance_id = %instance.instance_id,
                    job_id = %instance.job_id,
                    owner = %children::owner_label(&owner),
                    "Created keyed child instance"
                );
                Ok(Response::new(CreateInstanceReply {
                    instance_id: instance.instance_id,
                    deduplicated: false,
                    restoring_from: String::new(),
                    mode: InstanceMode::Keyed.to_string(),
                    durable: true,
                    node_id: String::new(),
                    state: String::new(),
                    owner: Some(owner),
                }))
            }
        }
    }

    /// The running session with this id, once `action` on its job is
    /// enforced. `None` when no session has the id.
    pub(super) async fn find_session(
        &self,
        identity: &AclIdentity,
        session_id: &str,
        action: AclAction,
    ) -> Result<Option<IndexedSession>, Status> {
        let Some(found) = sessions::find(&self.store, session_id).await else {
            return Ok(None);
        };
        let job_id = found
            .status
            .as_ref()
            .map(|status| status.job_id.clone())
            .unwrap_or_default();
        acl::enforce(identity, AclResource::Job, action, &AclScope::job(&job_id))?;
        Ok(Some(found))
    }

    /// A session for a verb that must reach it (suspend, start, get), once
    /// `action` on its job is enforced. Unlike [`Self::find_session`], an
    /// evicted session answers NOT_FOUND with its node's account of the
    /// eviction, and a session whose node does not answer answers
    /// UNAVAILABLE naming the node. `None` when no session has the id.
    ///
    /// Those two answers come before the job scope is known (the session's
    /// job is on the node that evicted it or cannot be reached); the
    /// handler's coarse check has already run, and neither answer carries
    /// more than the id the caller sent and the node it names.
    pub(super) async fn locate_session(
        &self,
        identity: &AclIdentity,
        session_id: &str,
        action: AclAction,
    ) -> Result<Option<IndexedSession>, Status> {
        match sessions::locate(&self.store, session_id).await {
            sessions::Located::Found(found) => {
                let job_id = found
                    .status
                    .as_ref()
                    .map(|status| status.job_id.clone())
                    .unwrap_or_default();
                acl::enforce(identity, AclResource::Job, action, &AclScope::job(&job_id))?;
                Ok(Some(*found))
            }
            sessions::Located::Evicted(message) => Err(Status::not_found(message)),
            sessions::Located::Unreachable(status) => Err(status),
            sessions::Located::NotFound => Ok(None),
        }
    }

    /// Whether a session's task carries a `suspend` block: only those
    /// sessions sleep on their node. Without one, suspend keeps answering
    /// that a session must be promoted first.
    fn session_can_sleep(&self, entry: &IndexedSession) -> bool {
        let Some(status) = entry.status.as_ref() else {
            return false;
        };
        self.store
            .get_job_config(&status.job_id)
            .ok()
            .and_then(|config| {
                config
                    .tasks
                    .iter()
                    .find(|task| task.name == status.task_name)
                    .map(|task| task.suspend.is_some())
            })
            .unwrap_or(false)
    }

    /// SuspendInstance on a session whose task has a suspend block: the
    /// node checkpoints it into its own content store and frees its
    /// envelope share (`Worker.SuspendSessions`). `None` when the id is no
    /// such session, so the caller answers as for any missing row.
    pub(super) async fn suspend_session_instance(
        &self,
        identity: &AclIdentity,
        session_id: &str,
    ) -> Result<Option<SuspendInstanceReply>, Status> {
        let Some(found) = self
            .locate_session(identity, session_id, AclAction::Write)
            .await?
        else {
            return Ok(None);
        };
        if !self.session_can_sleep(&found) {
            return Ok(None);
        }
        let reply = sessions::suspend_on_node(
            &self.store,
            &found.node_id,
            &[session_id.to_string()],
            "requested",
        )
        .await?;
        if let Some(failed) = reply.failed.into_iter().next() {
            let message = failed
                .message
                .strip_prefix("evicted:")
                .map(|rest| rest.trim_start().to_string())
                .unwrap_or(failed.message);
            return Err(Status::new(code_from_name(&failed.code), message));
        }
        let Some(done) = reply.suspended.into_iter().next() else {
            return Err(Status::internal(format!(
                "node {} answered without suspending session {session_id}",
                found.node_id
            )));
        };
        let message = if done.already_suspended {
            format!(
                "Instance {session_id} already suspended ({})",
                done.snapshot_id
            )
        } else {
            format!(
                "Instance {session_id} suspended on {} ({} checkpoint {}, {} bytes{})",
                found.node_id,
                done.kind,
                done.snapshot_id,
                done.bytes,
                if done.published_url.is_empty() {
                    String::new()
                } else {
                    format!(", published to {}", done.published_url)
                }
            )
        };
        Ok(Some(SuspendInstanceReply {
            message,
            snapshot_id: done.snapshot_id,
            kind: done.kind,
            node_id: found.node_id,
            deduplicated: false,
            durability: if done.durability.is_empty() {
                "local".to_string()
            } else {
                done.durability
            },
        }))
    }

    /// The job and task of a `store` session id: the session task whose
    /// sessions publish checkpoints (`suspend.durability = "store"`) and
    /// whose id shape the id has, once `Write` on the job is enforced. The
    /// manager keeps no record of sessions, so this scans the jobs.
    fn store_session_task(
        &self,
        identity: &AclIdentity,
        session_id: &str,
    ) -> Result<Option<(String, TaskConfig)>, Status> {
        let jobs = self
            .store
            .get_jobs()
            .map_err(|e| map_store_error("Error loading jobs", e))?;
        for job in jobs {
            let Ok(config) = self.store.get_job_config(&job.id) else {
                continue;
            };
            let found = config.tasks.iter().find(|task| {
                crate::structs::session::is_session_task(task)
                    && crate::structs::session::publishes_checkpoints(task)
                    && crate::structs::is_minted_for_task(session_id, &task.name)
            });
            if let Some(task) = found {
                acl::enforce(
                    identity,
                    AclResource::Job,
                    AclAction::Write,
                    &AclScope::job(&job.id),
                )?;
                return Ok(Some((job.id.clone(), task.clone())));
            }
        }
        Ok(None)
    }

    /// Resume a `store` session from its manifest on any node with room
    /// (docs/session-mode.md, "Moving a session"), other than `skip` (a node
    /// that already refused it for room). `None` when the id is no `store`
    /// session's.
    async fn resume_store_instance(
        &self,
        identity: &AclIdentity,
        session_id: &str,
        cold: bool,
        force_move: bool,
        skip: Option<&str>,
    ) -> Result<Option<StartInstanceReply>, Status> {
        let Some((job_id, task)) = self.store_session_task(identity, session_id)? else {
            return Ok(None);
        };
        match sessions::resume_store_session(
            &self.store,
            &job_id,
            &task,
            session_id,
            cold,
            force_move,
            skip,
            None,
        )
        .await?
        {
            sessions::StoreResume::Running { node_id } => Ok(Some(StartInstanceReply {
                message: format!("Instance {session_id} already running on {node_id}"),
                node_id,
                ..Default::default()
            })),
            sessions::StoreResume::Resumed { node_id, reply } => {
                let message = if reply.already_running {
                    format!("Instance {session_id} already running on {node_id}")
                } else if reply.moved_from.is_empty() {
                    format!(
                        "Instance {session_id} resumed on {node_id} from the content store \
                         (generation {})",
                        reply.generation
                    )
                } else {
                    format!(
                        "Instance {session_id} moved from {} and resumed on {node_id} \
                         (generation {})",
                        reply.moved_from, reply.generation
                    )
                };
                Ok(Some(StartInstanceReply {
                    message,
                    resuming_from: reply.resumed_from,
                    node_id,
                    ..Default::default()
                }))
            }
        }
    }

    /// StartInstance on a session: running answers that it already runs;
    /// suspended resumes it on its node (`Worker.ResumeSession`) under the
    /// same id, from its checkpoint or, `cold`, booted fresh. A `store`
    /// session whose node lost it, evicted its local copy, is full, or does
    /// not answer resumes from its manifest on another node instead
    /// (`force_move` moves it even when its old node did not answer the
    /// probe and is not marked down). `None` when no session has the id.
    pub(super) async fn start_session_instance(
        &self,
        identity: &AclIdentity,
        session_id: &str,
        cold: bool,
        force_move: bool,
    ) -> Result<Option<StartInstanceReply>, Status> {
        let located = self
            .locate_session(identity, session_id, AclAction::Write)
            .await;
        let found = match located {
            Ok(Some(found)) => found,
            Ok(None) => {
                return self
                    .resume_store_instance(identity, session_id, cold, force_move, None)
                    .await
            }
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::NotFound | tonic::Code::Unavailable
                ) =>
            {
                // Evicted, or its node does not answer: a `store` session
                // lives on in the content store.
                return match self
                    .resume_store_instance(identity, session_id, cold, force_move, None)
                    .await?
                {
                    Some(reply) => Ok(Some(reply)),
                    None => Err(status),
                };
            }
            Err(status) => return Err(status),
        };
        let state = found
            .status
            .as_ref()
            .map(|status| status.state.clone())
            .unwrap_or_default();
        if state != "suspended" {
            return Ok(Some(StartInstanceReply {
                message: format!("Instance {session_id} already running"),
                node_id: found.node_id,
                ..Default::default()
            }));
        }
        let resumed = sessions::resume_on_node(
            &self.store,
            &found.node_id,
            session_id,
            cold,
            None,
            false,
            None,
        )
        .await;
        if let Err(status) = resumed.as_ref() {
            // Full, or its local copy went meanwhile: a `store` session
            // resumes from its manifest elsewhere. A node full for this
            // session is full for the router too, and is not tried again.
            let full = status.code() == tonic::Code::ResourceExhausted;
            if full {
                if let Some(status) = found.status.as_ref() {
                    sessions::note_full(
                        &self.store,
                        &status.job_id,
                        &status.task_name,
                        &found.node_id,
                    );
                }
            }
            if full || status.code() == tonic::Code::NotFound {
                let skip = full.then_some(found.node_id.as_str());
                if let Some(reply) = self
                    .resume_store_instance(identity, session_id, cold, force_move, skip)
                    .await?
                {
                    return Ok(Some(reply));
                }
            }
        }
        let reply = resumed.map_err(|status| {
            if status.code() == tonic::Code::ResourceExhausted {
                Status::resource_exhausted(format!(
                    "{}\nHint: the session stays suspended with its checkpoint; retry \
                         when another session on {} ends or suspends",
                    status.message().split("\nHint:").next().unwrap_or_default(),
                    found.node_id
                ))
            } else {
                status
            }
        })?;
        let message = if reply.already_running {
            format!("Instance {session_id} already running")
        } else {
            format!("Instance {session_id} resumed on {}", found.node_id)
        };
        Ok(Some(StartInstanceReply {
            message,
            resuming_from: reply.resumed_from,
            node_id: found.node_id,
            ..Default::default()
        }))
    }

    /// A running session's `InstanceInfo`, its group read from the job.
    /// List what a unit owns (docs/child-sessions.md), for `GetInstance`.
    /// Skipped for a job whose spec declares no `children` block, which is
    /// almost every job.
    pub(super) async fn attach_children(&self, info: &mut InstanceInfo) {
        let declares = self
            .store
            .get_job_config(&info.job_id)
            .is_ok_and(|config| crate::manager::children::job_declares_children(&config));
        if !declares {
            return;
        }
        let kind = if info.mode == InstanceMode::Session.to_string() {
            crate::structs::children::OWNER_SESSION
        } else {
            crate::structs::children::OWNER_INSTANCE
        };
        let owner = crate::structs::children::owner_ref(kind, &info.instance_id);
        info.children =
            crate::manager::children::children_of(&self.store, &info.job_id, &owner).await;
    }

    pub(super) fn session_info(&self, entry: &IndexedSession) -> InstanceInfo {
        let (job_id, task_name) = entry
            .status
            .as_ref()
            .map(|status| (status.job_id.as_str(), status.task_name.as_str()))
            .unwrap_or_default();
        let group = self
            .store
            .get_job_config(job_id)
            .map(|config| sessions::group_of(&config, task_name))
            .unwrap_or_default();
        sessions::instance_info(entry, &group)
    }

    /// StopInstance or DestroyInstance on a session: end it on its node.
    /// `None` when no session has the id and this manager did not just end
    /// one with it.
    pub(super) async fn end_session(
        &self,
        identity: &AclIdentity,
        session_id: &str,
        reason: &str,
    ) -> Result<Option<String>, Status> {
        let Some(found) = self
            .find_session(identity, session_id, AclAction::Write)
            .await?
        else {
            return Ok(self
                .store
                .sessions()
                .recently_ended(session_id)
                .then(|| format!("Session {session_id} already ended")));
        };
        sessions::stop_on_node(
            &self.store,
            &found.node_id,
            &[session_id.to_string()],
            reason,
        )
        .await?;
        // Stopping a session that ended in the meantime is the same outcome.
        Ok(Some(match reason {
            "destroy" => format!("Destroyed session {session_id}"),
            _ => format!("Stopped session {session_id}"),
        }))
    }

    /// The error an instance RPC that does not apply to running sessions
    /// answers when it finds no row: FailedPrecondition naming promotion
    /// when the id is a session the caller may read, NotFound otherwise.
    pub(super) async fn missing_instance(
        &self,
        identity: &AclIdentity,
        instance_id: &str,
        verb: &str,
    ) -> Status {
        match self
            .find_session(identity, instance_id, AclAction::Read)
            .await
        {
            Ok(Some(_)) => Status::failed_precondition(format!(
                "instance {instance_id} is a running session (docs/session-mode.md), and a \
                 session cannot be {verb}. Promote it to a keyed instance first: `legion \
                 instance promote {instance_id}`"
            )),
            Ok(None) => Status::not_found(format!("instance {instance_id} not found")),
            Err(status) => status,
        }
    }

    /// The running sessions of a job for ListInstances: a page of them in id
    /// order, whether every envelope node has reported, when the index was
    /// read, the next page's token, and the totals by node.
    pub(super) async fn list_job_sessions(
        &self,
        job_id: &str,
        group: Option<&str>,
        limit: u32,
        after: &str,
    ) -> SessionListing {
        let mut listing = SessionListing {
            complete: true,
            ..Default::default()
        };
        let Ok(config) = self.store.get_job_config(job_id) else {
            return listing;
        };
        let session_tasks: Vec<&TaskConfig> = config
            .tasks
            .iter()
            .filter(|task| crate::structs::session::is_session_task(task))
            .filter(|task| group.is_none_or(|group| task.group == group))
            .collect();
        if session_tasks.is_empty() {
            return listing;
        }
        // One task named when the group narrows to one, so peers filter.
        let task_name = match (group, session_tasks.as_slice()) {
            (Some(_), [only]) => only.name.clone(),
            _ => String::new(),
        };

        // Totals, from the per-node usage every index keeps: cheap at any
        // count.
        let usage = sessions::query(
            &self.store,
            QuerySessionsRequest {
                job_id: job_id.to_string(),
                task_name: task_name.clone(),
                usage_only: true,
                ..Default::default()
            },
        )
        .await;
        for (node_id, node_usage) in &usage.usage {
            listing.count += u64::from(node_usage.sessions);
            listing
                .by_node
                .insert(node_id.clone(), u64::from(node_usage.sessions));
        }
        listing.as_of = usage.as_of.clone();
        let mut covered = usage.covered;

        if limit > 0 {
            let view = sessions::query(
                &self.store,
                QuerySessionsRequest {
                    job_id: job_id.to_string(),
                    task_name: task_name.clone(),
                    after_session_id: after.to_string(),
                    limit: limit.saturating_add(1),
                    ..Default::default()
                },
            )
            .await;
            covered.extend(view.covered);
            listing.as_of = view.as_of;
            let mut by_id: std::collections::BTreeMap<String, Vec<IndexedSession>> =
                std::collections::BTreeMap::new();
            for entry in view.sessions {
                let Some(status) = entry.status.as_ref() else {
                    continue;
                };
                by_id
                    .entry(status.session_id.clone())
                    .or_default()
                    .push(entry);
            }
            if by_id.len() > limit as usize {
                let keep: Vec<String> = by_id.keys().take(limit as usize).cloned().collect();
                listing.next_page_token = keep.last().cloned().unwrap_or_default();
                by_id.retain(|id, _| keep.binary_search(id).is_ok());
            }
            listing.infos = by_id
                .into_values()
                .filter_map(sessions::winner)
                .filter(|(kept, _)| {
                    kept.status.as_ref().is_some_and(|status| {
                        session_tasks
                            .iter()
                            .any(|task| task.name == status.task_name)
                    })
                })
                .map(|(kept, _)| {
                    let task_name = kept
                        .status
                        .as_ref()
                        .map(|status| status.task_name.clone())
                        .unwrap_or_default();
                    sessions::instance_info(&kept, &sessions::group_of(&config, &task_name))
                })
                .collect();
        }
        for task in &session_tasks {
            match sessions::envelope_nodes(&self.store, job_id, &task.name) {
                Ok(nodes) => {
                    listing.complete &= nodes.iter().all(|node| covered.contains(node));
                }
                Err(_) => listing.complete = false,
            }
        }
        listing
    }

    pub(super) async fn promote_instance_rpc(
        &self,
        request: Request<PromoteInstanceRequest>,
    ) -> Result<Response<PromoteInstanceReply>, Status> {
        // Coarse first: the session's job is unknown until the lookup.
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "promote_instance",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let recorded: RecordedPromotion = serde_json::from_str(&recorded).unwrap_or_default();
            return Ok(Response::new(recorded.into_reply(true)));
        }
        let new_id = inner.new_id.trim();
        if !new_id.is_empty() {
            validate_keyed_instance_id(new_id).map_err(Status::invalid_argument)?;
        }

        // Already promoted, under the session's id or the requested one: the
        // outcome a retry without a key should see. Rows win over the index,
        // which lists the session until its node reports the adoption.
        let promoted_id = if new_id.is_empty() {
            inner.instance_id.as_str()
        } else {
            new_id
        };
        // A new name is qualified by the session's namespace, which is not
        // known yet; the session's own (minted, cluster-unique) id finds a
        // named promotion wherever it landed.
        // A taken name is refused by the promotion itself, in the right
        // namespace.
        let existing = if new_id.is_empty() {
            self.store.get_instance(promoted_id)
        } else {
            self.store
                .instance_promoted_from(&inner.instance_id)
                .map(|found| {
                    found.filter(|instance| {
                        crate::structs::namespace::instance_key(&instance.instance_id) == new_id
                    })
                })
        }
        .map_err(|e| map_store_error("Error loading instance", e))?;
        if let Some(instance) = existing {
            acl::enforce(
                &identity,
                AclResource::Job,
                AclAction::Write,
                &AclScope::job(&instance.job_id),
            )?;
            if instance.promoted_from.as_deref() != Some(inner.instance_id.as_str()) {
                return Err(Status::already_exists(format!(
                    "instance id {promoted_id} already exists (keyed ids are never recycled, \
                     including stopped and destroyed ones); choose a different id"
                )));
            }
            let node_id = instance
                .current_alloc_id
                .and_then(|id| self.store.get_allocation_by_id(id).ok().flatten())
                .map(|allocation| allocation.node_id)
                .unwrap_or_default();
            return Ok(Response::new(
                RecordedPromotion {
                    instance_id: instance.instance_id,
                    promoted_from: inner.instance_id.clone(),
                    node_id,
                    allocation_id: instance.current_alloc_id.unwrap_or_default(),
                }
                .into_reply(true),
            ));
        }
        let Some(found) = self
            .find_session(&identity, &inner.instance_id, AclAction::Write)
            .await?
        else {
            return Err(Status::not_found(format!(
                "no running session has id {}",
                inner.instance_id
            )));
        };
        let status = found.status.clone().unwrap_or_default();
        // A node that reports no job config (a session isolate's
        // synthesized status) is read against the job's current one.
        let tasks: Vec<TaskConfig> = if status.job_config_id.is_empty() {
            self.store
                .get_job_config(&status.job_id)
                .map(|config| config.tasks)
                .unwrap_or_default()
        } else {
            self.store
                .shared_task_configs_for_job_config(&status.job_config_id)
                .map_err(|e| map_store_error("Error loading job config", e))?
                .iter()
                .map(|(_, task)| task.clone())
                .collect()
        };
        let task = tasks
            .into_iter()
            .find(|task| task.name == status.task_name)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "the session's job config {} no longer has task {}",
                    status.job_config_id, status.task_name
                ))
            })?;
        // An isolate session is never a keyed instance (isolate-runtime
        // calls entry 10): refused before any write.
        crate::structs::session::promotable(&task).map_err(Status::failed_precondition)?;
        if !new_id.is_empty() && !crate::structs::session::accepts_names(&task) {
            return Err(Status::invalid_argument(format!(
                "task {} sets stateful.promote.onName = false; promote without new_id to keep \
                 the session's id",
                task.name
            )));
        }

        let promote = crate::storage::store::PromoteSession {
            session_id: status.session_id.clone(),
            job_id: status.job_id.clone(),
            job_config_id: status.job_config_id.clone(),
            group: task.group.clone(),
            task_name: task.name.clone(),
            node_id: found.node_id.clone(),
            new_id: (!new_id.is_empty()).then(|| new_id.to_string()),
            // A child of an instance or an allocation stays its child: the
            // reference moves into the row, in this entry. A child of a
            // session is detached (a durable child of an ephemeral owner is
            // not allowed).
            owner: status
                .owner
                .as_ref()
                .filter(|owner| crate::structs::children::raft_owned(owner))
                .map(crate::structs::children::owner_label),
        };
        let key = idempotency_key.clone();
        // One scheduler-lane entry: the instance row, its allocation, the
        // event, and the idempotency record.
        let promoted = self
            .store
            .scheduler_group_commit("promote_instance", move |s| {
                claimed_body(s, |s| {
                    let promoted = s.promote_session(&promote)?;
                    let recorded = RecordedPromotion {
                        instance_id: promoted.instance.instance_id.clone(),
                        promoted_from: promote.session_id.clone(),
                        node_id: promote.node_id.clone(),
                        allocation_id: promoted.allocation.id,
                    };
                    // Claimed before the event: a concurrent promote with this
                    // key that committed first rolls this one back whole.
                    claim_idempotency_key(
                        s,
                        &key,
                        "promote_instance",
                        &serde_json::to_string(&recorded)?,
                    )?;
                    s.events().instance_promoted(
                        &promote.job_id,
                        &promoted.instance.instance_id,
                        &promote.session_id,
                        &promote.node_id,
                        promoted.allocation.id,
                    );
                    Ok((promoted, recorded))
                })
            });
        let (promoted, recorded) =
            match self.claimed_write(&idempotency_key, "Error promoting session", promoted)? {
                Ok(promoted) => promoted,
                Err(winner) => {
                    let winner: RecordedPromotion =
                        serde_json::from_str(&winner).unwrap_or_default();
                    return Ok(Response::new(winner.into_reply(true)));
                }
            };
        // The session is an allocation from here on; its node stops listing
        // it once it adopts it.
        self.store
            .sessions()
            .record_stop(&found.node_id, &status.session_id);
        crate::manager::telemetry::metrics()
            .session_promotions_total
            .inc();
        // Push the allocation so the node adopts it now rather than at its
        // next heartbeat. A failed push changes nothing: the desired set
        // carries the allocation, and the node adopts it from there.
        let store = self.store.clone();
        let allocation = promoted.allocation.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(err) =
                crate::manager::dispatcher::dispatch_allocations(&store, &[allocation])
            {
                debug!(error = ?err, "Promotion push failed; the desired set delivers it");
            }
        });
        info!(
            instance_id = %recorded.instance_id,
            session_id = %recorded.promoted_from,
            node_id = %recorded.node_id,
            allocation_id = recorded.allocation_id,
            "Promoted session"
        );
        Ok(Response::new(recorded.into_reply(false)))
    }
}

/// A gRPC code from its name as a node reports it in a
/// `SessionSleepFailure` (`NotFound`, `FailedPrecondition`, …).
fn code_from_name(name: &str) -> tonic::Code {
    match name {
        "NotFound" | "not_found" => tonic::Code::NotFound,
        "FailedPrecondition" | "failed_precondition" => tonic::Code::FailedPrecondition,
        "Aborted" | "aborted" => tonic::Code::Aborted,
        "ResourceExhausted" | "resource_exhausted" => tonic::Code::ResourceExhausted,
        "Unavailable" | "unavailable" => tonic::Code::Unavailable,
        "InvalidArgument" | "invalid_argument" => tonic::Code::InvalidArgument,
        _ => tonic::Code::Internal,
    }
}
