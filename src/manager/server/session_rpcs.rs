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
};
use serde::{Deserialize, Serialize};

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
        );
        let (session_id, derived) = if scoped.is_empty() {
            (crate::structs::mint_instance_id(&task.name), false)
        } else {
            (
                crate::structs::session::derived_session_id(&task.name, &config.name, &scoped),
                true,
            )
        };
        let started = sessions::start(&self.store, config, task, &session_id, derived).await?;
        Ok(Response::new(CreateInstanceReply {
            instance_id: session_id,
            deduplicated: started.deduplicated,
            restoring_from: String::new(),
            mode: InstanceMode::Session.to_string(),
            durable: false,
            node_id: started.node_id,
            state: started.state,
        }))
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

    /// A running session's `InstanceInfo`, its group read from the job.
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

    /// The running sessions of a job for ListInstances, after its rows: the
    /// infos, whether every node holding an envelope for the job has
    /// reported, and when the view was read.
    pub(super) async fn list_job_sessions(
        &self,
        job_id: &str,
        group: Option<&str>,
    ) -> (Vec<InstanceInfo>, bool, String) {
        let config = match self.store.get_job_config(job_id) {
            Ok(config) => config,
            Err(_) => return (Vec::new(), true, String::new()),
        };
        let session_tasks: Vec<&TaskConfig> = config
            .tasks
            .iter()
            .filter(|task| crate::structs::session::is_session_task(task))
            .filter(|task| group.is_none_or(|group| task.group == group))
            .collect();
        if session_tasks.is_empty() {
            return (Vec::new(), true, String::new());
        }
        let view = sessions::query(
            &self.store,
            QuerySessionsRequest {
                job_id: job_id.to_string(),
                ..Default::default()
            },
        )
        .await;
        let mut by_id: std::collections::BTreeMap<String, Vec<IndexedSession>> =
            std::collections::BTreeMap::new();
        for entry in view.sessions {
            let Some(status) = entry.status.as_ref() else {
                continue;
            };
            if !session_tasks
                .iter()
                .any(|task| task.name == status.task_name)
            {
                continue;
            }
            by_id
                .entry(status.session_id.clone())
                .or_default()
                .push(entry);
        }
        let infos = by_id
            .into_values()
            .filter_map(sessions::winner)
            .map(|(kept, _)| {
                let task_name = kept
                    .status
                    .as_ref()
                    .map(|status| status.task_name.clone())
                    .unwrap_or_default();
                sessions::instance_info(&kept, &sessions::group_of(&config, &task_name))
            })
            .collect();
        let mut complete = true;
        for task in &session_tasks {
            match sessions::envelope_nodes(&self.store, job_id, &task.name) {
                Ok(nodes) => {
                    complete &= nodes.iter().all(|node| view.covered.contains(node));
                }
                Err(_) => complete = false,
            }
        }
        (infos, complete, view.as_of)
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
        if let Some(instance) = self
            .store
            .get_instance(promoted_id)
            .map_err(|e| map_store_error("Error loading instance", e))?
        {
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
        let task = self
            .store
            .shared_task_configs_for_job_config(&status.job_config_id)
            .map_err(|e| map_store_error("Error loading job config", e))?
            .iter()
            .find(|(_, task)| task.name == status.task_name)
            .map(|(_, task)| task.clone())
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "the session's job config {} no longer has task {}",
                    status.job_config_id, status.task_name
                ))
            })?;
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
        };
        let key = idempotency_key.clone();
        // One scheduler-lane entry: the instance row, its allocation, the
        // event, and the idempotency record.
        let promoted = self
            .store
            .scheduler_group_commit("promote_instance", move |s| {
                let promoted = s.promote_session(&promote)?;
                s.events().instance_promoted(
                    &promote.job_id,
                    &promoted.instance.instance_id,
                    &promote.session_id,
                    &promote.node_id,
                    promoted.allocation.id,
                );
                let recorded = RecordedPromotion {
                    instance_id: promoted.instance.instance_id.clone(),
                    promoted_from: promote.session_id.clone(),
                    node_id: promote.node_id.clone(),
                    allocation_id: promoted.allocation.id,
                };
                s.idempotency_record(&key, "promote_instance", &serde_json::to_string(&recorded)?)?;
                Ok((promoted, recorded))
            })
            .map_err(|e| map_store_error("Error promoting session", e))?;
        let (promoted, recorded) = promoted;
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
