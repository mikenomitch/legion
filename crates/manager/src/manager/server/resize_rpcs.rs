//! Resize RPCs (docs/resize.md): `ResizeAllocation` and `ResizeInstance`.
//!
//! A top-level allocation's resize is one compare-and-set on its row
//! (`Store::resize_allocation`) plus one call to its node, in an order that
//! never strands the unit: **a grow reserves, then applies; a shrink
//! applies, then releases.** A session's resize is its node's alone and
//! writes nothing here. A runtime's `needs_restart` or `not_supported` is
//! an answer the caller gets back; nothing is ever restarted to resize it.

use std::collections::BTreeMap;

use super::snapshot_rpcs::{node_build_version, node_has_capability};
use super::*;
use crate::manager::sessions;
use crate::storage::store::{AllocationResizeRequest, AllocationSize};
use crate::structs::resize::{json_amount, mib, ResizeOutcome, ResourceSize, RESIZE_CAPABILITY};
use crate::structs::{
    ResizeAllocationRequest, ResizeInstanceRequest, ResizeReply, ResizeResources,
    ResizeSessionReply, ResizeSessionRequest,
};

/// How long a manager waits for a node to answer a resize.
const RESIZE_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

fn requested(to: Option<&ResizeResources>) -> Result<ResourceSize, Status> {
    let size = to
        .map(|to| ResourceSize {
            memory: to.memory,
            memory_max: to.memory_max,
            cpu: to.cpu,
        })
        .unwrap_or_default();
    size.validate_request()
        .map_err(|err| Status::invalid_argument(format!("{err}")))?;
    Ok(size)
}

fn wire(size: &ResourceSize) -> ResizeResources {
    ResizeResources {
        memory: size.memory,
        memory_max: size.memory_max,
        cpu: size.cpu,
    }
}

/// What a caller should do about an outcome that changed nothing.
fn hint_for(
    outcome: ResizeOutcome,
    unit: &str,
    free: &BTreeMap<String, f64>,
    held: &ResourceSize,
) -> String {
    match outcome {
        ResizeOutcome::NeedsRestart => format!(
            "restart the {unit} to start it at the new size (a redeploy with the new \
             resources does the same for every allocation of the task)"
        ),
        ResizeOutcome::Refused if !free.is_empty() => {
            let mut most = Vec::new();
            if let Some(free) = free.get("memory") {
                most.push(mib(held.memory.unwrap_or(0.0) + free));
            }
            if let Some(free) = free.get("cpu") {
                most.push(format!(
                    "{} MHz",
                    json_amount(held.cpu.unwrap_or(0.0) + free)
                ));
            }
            format!(
                "resize to at most {} here, or move the {unit}",
                most.join(" and ")
            )
        }
        ResizeOutcome::Deferred => "retry in a few seconds".to_string(),
        _ => String::new(),
    }
}

/// The reply for a unit, before an outcome is known.
fn base_reply(from: &ResourceSize, to: &ResourceSize, spec: &Value, node_id: &str) -> ResizeReply {
    ResizeReply {
        outcome: ResizeOutcome::Applied.as_str().to_string(),
        from: from.to_json().to_string(),
        to: to.to_json().to_string(),
        spec: ResourceSize::of(spec).to_json().to_string(),
        node_id: node_id.to_string(),
        ..Default::default()
    }
}

/// Call a node's `Worker.ResizeSession`.
async fn resize_on_node(
    store: &Store,
    node_id: &str,
    request: ResizeSessionRequest,
) -> Result<ResizeSessionReply, Status> {
    let node = store
        .cached_node(node_id)
        .ok()
        .flatten()
        .map(Ok)
        .unwrap_or_else(|| store.get_node(node_id).map(Arc::new))
        .map_err(|_| Status::not_found(format!("node {node_id} not found")))?;
    let target = crate::manager::worker_plane::WorkerTarget::for_node(&node)?;
    let mut client =
        crate::manager::worker_plane::WorkerPlane::global().client_for(store, &target)?;
    let mut request = tonic::Request::new(request);
    request.set_timeout(RESIZE_CALL_TIMEOUT);
    match client.resize_session(request).await {
        Ok(reply) => Ok(reply.into_inner()),
        Err(status)
            if matches!(
                status.code(),
                tonic::Code::NotFound
                    | tonic::Code::InvalidArgument
                    | tonic::Code::FailedPrecondition
            ) =>
        {
            Err(status)
        }
        Err(status) => {
            let status = crate::manager::worker_plane::worker_call_error(
                &target.node_id,
                &target.address,
                status,
            );
            Err(Status::unavailable(format!(
                "node {node_id} could not be reached to resize the unit: {}",
                status.message()
            )))
        }
    }
}

impl ManagerDaemonService {
    /// Fill an instance's effective resources and its resize override from
    /// its current allocation's row (GetInstance).
    pub(super) fn attach_resources(&self, info: &mut InstanceInfo, allocation_id: Option<i64>) {
        let Some(view) = allocation_id
            .and_then(|allocation_id| self.store.allocation_size(allocation_id).ok().flatten())
        else {
            return;
        };
        info.resources = ResourceSize::of(&view.effective).to_json().to_string();
        info.resources_override = view
            .resources_override
            .map(|record| record.to_string())
            .unwrap_or_default();
    }

    /// A node that cannot answer a resize, as a `not_supported` reply.
    fn node_cannot_resize(&self, node_id: &str) -> Option<String> {
        let node = self.store.get_node(node_id).ok()?;
        (!node_has_capability(&node, RESIZE_CAPABILITY)).then(|| {
            format!(
                "node {node_id} runs worker {}, which cannot resize a running unit; upgrade \
                 the worker",
                node_build_version(&node)
            )
        })
    }

    pub(super) async fn resize_allocation_rpc(
        &self,
        request: Request<ResizeAllocationRequest>,
    ) -> Result<Response<ResizeReply>, Status> {
        // Coarse first: the allocation's job is unknown until the lookup.
        let identity = self.check_acl(
            &request,
            AclResource::Scale,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let size = requested(inner.to.as_ref())?;
        let view = self
            .store
            .allocation_size(inner.allocation_id)
            .map_err(|e| map_store_error("Error loading allocation", e))?
            .ok_or_else(|| {
                Status::not_found(format!("allocation {} not found", inner.allocation_id))
            })?;
        acl::enforce(
            &identity,
            AclResource::Scale,
            AclAction::Write,
            &self.job_read_scope(&view.job_id),
        )?;
        let key = scoped_idempotency_key(
            &identity,
            "resize_allocation",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        let reply = self
            .resize_allocation_unit(
                &identity,
                view,
                size,
                &inner.reason,
                inner.force,
                inner.dry_run,
                &key,
                "allocation",
            )
            .await?;
        Ok(Response::new(reply))
    }

    pub(super) async fn resize_instance_rpc(
        &self,
        request: Request<ResizeInstanceRequest>,
    ) -> Result<Response<ResizeReply>, Status> {
        // Coarse first: the instance's job is unknown until the lookup.
        let identity = self.check_acl(
            &request,
            AclResource::Scale,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let size = requested(inner.to.as_ref())?;
        let instance = self
            .store
            .get_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?;
        let Some(instance) = instance else {
            return self.resize_session(&identity, &inner, size).await;
        };
        acl::enforce(
            &identity,
            AclResource::Scale,
            AclAction::Write,
            &self.job_read_scope(&instance.job_id),
        )?;
        let view = match instance.current_alloc_id {
            Some(allocation_id) => self
                .store
                .allocation_size(allocation_id)
                .map_err(|e| map_store_error("Error loading allocation", e))?,
            None => None,
        };
        let Some(view) = view else {
            return Err(Status::failed_precondition(format!(
                "instance {} is {}; it has no running allocation to resize. Hint: start it, \
                 then resize it",
                inner.instance_id,
                instance.state.as_str()
            )));
        };
        let key = scoped_idempotency_key(
            &identity,
            "resize_instance",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        let reply = self
            .resize_allocation_unit(
                &identity,
                view,
                size,
                &inner.reason,
                inner.force,
                inner.dry_run,
                &key,
                "instance",
            )
            .await?;
        Ok(Response::new(reply))
    }

    /// A running session's resize: its node's alone.
    async fn resize_session(
        &self,
        identity: &AclIdentity,
        inner: &ResizeInstanceRequest,
        size: ResourceSize,
    ) -> Result<Response<ResizeReply>, Status> {
        let not_found = || Status::not_found(format!("instance {} not found", inner.instance_id));
        let found = sessions::find(&self.store, &inner.instance_id)
            .await
            .ok_or_else(not_found)?;
        let job_id = found
            .status
            .as_ref()
            .map(|status| status.job_id.clone())
            .unwrap_or_default();
        acl::enforce(
            identity,
            AclResource::Scale,
            AclAction::Write,
            &self.job_read_scope(&job_id),
        )?;
        let metrics = crate::manager::telemetry::metrics();
        let mut reply = ResizeReply {
            node_id: found.node_id.clone(),
            instance_id: inner.instance_id.clone(),
            job_id: job_id.clone(),
            dry_run: inner.dry_run,
            ..Default::default()
        };
        if let Some(message) = self.node_cannot_resize(&found.node_id) {
            reply.outcome = ResizeOutcome::NotSupported.as_str().to_string();
            reply.message = message;
            return Ok(Response::new(reply));
        }
        let done = resize_on_node(
            &self.store,
            &found.node_id,
            ResizeSessionRequest {
                session_id: inner.instance_id.clone(),
                allocation_id: 0,
                to: Some(wire(&size)),
                force: inner.force,
                dry_run: inner.dry_run,
                reason: inner.reason.clone(),
            },
        )
        .await?;
        let outcome = ResizeOutcome::parse(&done.outcome).unwrap_or(ResizeOutcome::Refused);
        let free: BTreeMap<String, f64> = serde_json::from_str::<Value>(&done.free)
            .ok()
            .map(|value| crate::structs::session::resource_amounts(&value))
            .unwrap_or_default();
        let held = serde_json::from_str::<Value>(&done.from)
            .map(|value| ResourceSize::of(&value))
            .unwrap_or_default();
        reply.outcome = done.outcome;
        reply.from = done.from;
        reply.to = done.to;
        reply.message = done.message;
        reply.free = done.free;
        reply.hint = match outcome {
            // A session's room is its envelope's, not the node's.
            ResizeOutcome::Refused if !free.is_empty() => format!(
                "resize within the envelope's free room, end other sessions of the task, or \
                 raise stateful.envelope ({} held now)",
                held.to_json()
            ),
            other => hint_for(other, "session", &BTreeMap::new(), &held),
        };
        reply.retryable = matches!(outcome, ResizeOutcome::Deferred)
            || (outcome == ResizeOutcome::Refused && !free.is_empty());
        if let Ok(config) = self.store.get_job_config(&job_id) {
            if let Some(task) = config.tasks.iter().find(|task| task.name == done.task_name) {
                reply.spec = ResourceSize::of(&task.resources).to_json().to_string();
            }
        }
        if !inner.dry_run {
            metrics.resizes_total.inc(&["session", outcome.as_str()]);
        }
        Ok(Response::new(reply))
    }

    /// Resize one top-level allocation: the compare-and-set and the node's
    /// apply, in the order that never strands the unit.
    #[allow(clippy::too_many_arguments)]
    async fn resize_allocation_unit(
        &self,
        identity: &AclIdentity,
        view: AllocationSize,
        size: ResourceSize,
        reason: &str,
        force: bool,
        dry_run: bool,
        idempotency_key: &str,
        unit: &'static str,
    ) -> Result<ResizeReply, Status> {
        if let Some(stored) = self
            .store
            .idempotency_lookup(idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            if let Ok(mut reply) = serde_json::from_str::<ResizeReply>(&stored) {
                reply.deduplicated = true;
                return Ok(reply);
            }
        }
        let allocation_id = view.allocation_id;
        if !crate::constants::allocation_status_is_dispatchable(&view.status) {
            return Err(Status::failed_precondition(format!(
                "allocation {allocation_id} is {}, not running",
                view.status
            )));
        }
        if view.runtime == crate::structs::session::ENVELOPE_RUNTIME {
            return Err(Status::failed_precondition(format!(
                "allocation {allocation_id} is a session envelope; it resizes through \
                 stateful.envelope (docs/session-mode.md, \"Elastic envelopes\")"
            )));
        }
        let from = ResourceSize::of(&view.effective);
        let after = size
            .apply_to(&view.effective)
            .map_err(|err| Status::invalid_argument(format!("{err}")))?;
        let to = ResourceSize::of(&after);
        let mut reply = base_reply(&from, &to, &view.spec, &view.node_id);
        reply.allocation_id = allocation_id;
        reply.instance_id = view.instance_id.clone().unwrap_or_default();
        reply.job_id = view.job_id.clone();
        reply.dry_run = dry_run;
        let metrics = crate::manager::telemetry::metrics();
        let answered = |mut reply: ResizeReply, outcome: ResizeOutcome, message: String| {
            reply.outcome = outcome.as_str().to_string();
            reply.message = message;
            if !dry_run {
                metrics.resizes_total.inc(&[unit, outcome.as_str()]);
            }
            reply
        };
        if from == to {
            return Ok(answered(reply, ResizeOutcome::Applied, String::new()));
        }
        if let Some(message) = self.node_cannot_resize(&view.node_id) {
            return Ok(answered(reply, ResizeOutcome::NotSupported, message));
        }

        let node_request = |dry_run: bool, size: &ResourceSize, force: bool| ResizeSessionRequest {
            session_id: String::new(),
            allocation_id,
            to: Some(wire(size)),
            force,
            dry_run,
            reason: reason.to_string(),
        };
        // Ask the runtime before anything is committed, so `needs_restart`
        // and `not_supported` cost nothing and leave nothing to undo.
        let preflight =
            resize_on_node(&self.store, &view.node_id, node_request(true, &size, force)).await?;
        let preflight_outcome =
            ResizeOutcome::parse(&preflight.outcome).unwrap_or(ResizeOutcome::Refused);
        if preflight_outcome != ResizeOutcome::Applied {
            reply.hint = hint_for(preflight_outcome, unit, &BTreeMap::new(), &from);
            return Ok(answered(reply, preflight_outcome, preflight.message));
        }

        let by = identity
            .accessor_id
            .as_deref()
            .map(|accessor| format!("accessor:{accessor}"))
            .unwrap_or_else(|| identity.principal.clone());
        let commit =
            |from: ResourceSize, to: ResourceSize, dry_run: bool| AllocationResizeRequest {
                allocation_id,
                from,
                to,
                reason: reason.to_string(),
                by: by.clone(),
                dry_run,
            };
        let store_write = |request: AllocationResizeRequest| {
            self.store
                .resize_allocation(&request)
                .map_err(|e| map_store_error("Error resizing allocation", e))
        };
        // The row's answer as a reply, or a status for the answers that are
        // errors. `None` means committed (or, for a dry run, would commit).
        let refused_by_row = |mut reply: ResizeReply,
                              row: crate::storage::store::AllocationResizeOutcome|
         -> Result<Option<ResizeReply>, Status> {
            match row.outcome.as_str() {
                "applied" => Ok(None),
                "refused" => {
                    reply.free = crate::structs::session::amounts_json(&row.free).to_string();
                    reply.hint = hint_for(ResizeOutcome::Refused, unit, &row.free, &from);
                    reply.retryable = true;
                    Ok(Some(answered(reply, ResizeOutcome::Refused, row.message)))
                }
                "stale" => Err(Status::failed_precondition(format!(
                    "stale: {}. Another resize landed first; read the {unit} and try again",
                    row.message
                ))),
                "gone" => Err(Status::not_found(row.message)),
                _ => Err(Status::failed_precondition(row.message)),
            }
        };

        if dry_run {
            let row = store_write(commit(from, to, true))?;
            if let Some(refusal) = refused_by_row(reply.clone(), row)? {
                return Ok(refusal);
            }
            return Ok(answered(reply, ResizeOutcome::Applied, String::new()));
        }

        if from.grows_to(&to) {
            // Reserve, then apply.
            let row = store_write(commit(from, to, false))?;
            if let Some(refusal) = refused_by_row(reply.clone(), row)? {
                return Ok(refusal);
            }
            let applied = resize_on_node(
                &self.store,
                &view.node_id,
                node_request(false, &size, force),
            )
            .await;
            let undo = |why: &str| {
                // The reservation goes back; the unit never grew.
                if let Err(err) = self.store.resize_allocation(&commit(to, from, false)) {
                    warn!(
                        allocation_id,
                        error = ?err,
                        "Could not release a reservation a failed grow took ({why}); the node \
                         converges the allocation to the committed size on its next heartbeat"
                    );
                }
            };
            match applied {
                Ok(done) => {
                    let outcome =
                        ResizeOutcome::parse(&done.outcome).unwrap_or(ResizeOutcome::Refused);
                    if outcome != ResizeOutcome::Applied {
                        undo("the runtime refused");
                        reply.hint = hint_for(outcome, unit, &BTreeMap::new(), &from);
                        return Ok(answered(reply, outcome, done.message));
                    }
                }
                Err(status) => {
                    undo("the node did not answer");
                    metrics.resizes_total.inc(&[unit, "error"]);
                    return Err(status);
                }
            }
        } else {
            // Apply, then release.
            let done = resize_on_node(
                &self.store,
                &view.node_id,
                node_request(false, &size, force),
            )
            .await
            .inspect_err(|_| metrics.resizes_total.inc(&[unit, "error"]))?;
            let outcome = ResizeOutcome::parse(&done.outcome).unwrap_or(ResizeOutcome::Refused);
            if outcome != ResizeOutcome::Applied {
                reply.hint = hint_for(outcome, unit, &BTreeMap::new(), &from);
                return Ok(answered(reply, outcome, done.message));
            }
            let row = store_write(commit(from, to, false));
            let failed = match row {
                Ok(row) => refused_by_row(reply.clone(), row).map(|refusal| refusal.is_some()),
                Err(status) => Err(status),
            };
            if !matches!(failed, Ok(false)) {
                // The row still holds the old size: put the unit back at it
                // rather than leave it smaller than what is reserved for it.
                let back = ResourceSize {
                    memory: size.memory.and(from.memory),
                    memory_max: size.memory_max.and(from.memory_max),
                    cpu: size.cpu.and(from.cpu),
                };
                if !back.is_empty() {
                    let _ = resize_on_node(
                        &self.store,
                        &view.node_id,
                        node_request(false, &back, false),
                    )
                    .await;
                }
                metrics.resizes_total.inc(&[unit, "stale"]);
                return Err(failed.err().unwrap_or_else(|| {
                    Status::failed_precondition(format!(
                        "stale: the {unit} changed while it was being resized; read it and try \
                         again"
                    ))
                }));
            }
        }

        let reply = answered(reply, ResizeOutcome::Applied, String::new());
        let (kind, subject) = match (unit, view.instance_id.as_deref()) {
            ("instance", Some(instance_id)) => {
                ("instance.resized", format!("Instance {instance_id}"))
            }
            _ => ("allocation.resized", format!("Allocation {allocation_id}")),
        };
        self.store.emit_event(
            crate::storage::events::EventDraft::info(
                kind,
                format!(
                    "{subject} resized in place on {}: {} -> {}{}",
                    view.node_id,
                    from.to_json(),
                    to.to_json(),
                    if reason.is_empty() {
                        String::new()
                    } else {
                        format!(" ({reason})")
                    }
                ),
            )
            .job(&view.job_id)
            .node(&view.node_id)
            .allocation(allocation_id)
            .meta(json!({
                "allocationId": allocation_id,
                "instanceId": view.instance_id,
                "task": view.task_name,
                "runtime": view.runtime,
                "from": from.to_json(),
                "to": to.to_json(),
                "spec": ResourceSize::of(&view.spec).to_json(),
                "reason": reason,
                "by": by,
            })),
        );
        if !idempotency_key.is_empty() {
            if let Ok(stored) = serde_json::to_string(&reply) {
                let _ = self
                    .store
                    .idempotency_record(idempotency_key, "resize", &stored);
            }
        }
        Ok(reply)
    }
}
