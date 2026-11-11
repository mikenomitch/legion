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
use crate::structs::resize::ResourcesOverride;
use crate::structs::resize::{json_amount, mib, ResizeOutcome, ResourceSize, RESIZE_CAPABILITY};
use crate::structs::{
    ResizeAllocationRequest, ResizeInstanceRequest, ResizeReply, ResizeResources,
    ResizeSessionReply, ResizeSessionRequest, ResizeTaskReply, ResizeTaskRequest,
};

/// How long a resize by moving waits for a keyed instance's suspend to
/// finish (its checkpoint is captured, and published when its task says so).
const MOVE_SUSPEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);

/// How long a resize by moving waits for the resumed instance to be placed
/// at its new size before it is put back at the old one, in milliseconds.
/// A static so a test can shorten the wait; nothing else sets it.
pub static MOVE_RESUME_TIMEOUT_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(60_000);

fn move_resume_timeout() -> std::time::Duration {
    std::time::Duration::from_millis(
        MOVE_RESUME_TIMEOUT_MS.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// The per-allocation outcomes `ResizeTask` adds to a unit's own: stopped
/// for replacement, and could not be asked.
const OUTCOME_REPLACED: &str = "replaced";
const OUTCOME_ERROR: &str = "error";

/// The memory a sandbox's guest boots with: its ceiling, or its
/// reservation when it declares none (docs/task-runtime-sandbox.md, "Guest
/// size"). A memory checkpoint restores only into a guest of the size it
/// was taken at.
fn guest_size(resources: &ResourceSize) -> Option<f64> {
    resources.memory_max.or(resources.memory)
}

/// A request carrying the caller's credentials on to another handler of
/// this service (a move suspends and resumes through the handlers that
/// already do both, under the caller's own grants).
fn forwarded<T>(metadata: &tonic::metadata::MetadataMap, message: T) -> Request<T> {
    let mut request = Request::new(message);
    *request.metadata_mut() = metadata.clone();
    request
}

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
        ResizeOutcome::NeedsRestart if unit == "allocation" => format!(
            "restart the {unit} to start it at the new size, or `legion job resize --replace` \
             to replace every allocation of the task at it"
        ),
        ResizeOutcome::NeedsRestart => format!(
            "restart the {unit} to start it at the new size, or pass --move to suspend it and \
             resume it at the new size on a node with room (a unit that can move: a keyed \
             instance with a suspend block, or a session with suspend.durability = \"store\")"
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
            if unit == "allocation" {
                format!(
                    "resize to at most {} here, or `legion job resize --replace` to place the \
                     task's allocations at the new size elsewhere",
                    most.join(" and ")
                )
            } else {
                format!(
                    "resize to at most {} here, or pass --move to suspend the {unit} and resume \
                     it at the new size on a node with room",
                    most.join(" and ")
                )
            }
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
            // No allocation (suspended, stopped): the size it carries to
            // its next one, when a resize left one.
            if let Ok(Some(record)) = self.store.instance_size(&info.instance_id) {
                info.resources_override = serde_json::to_string(&record).unwrap_or_default();
            }
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
        let metadata = request.metadata().clone();
        let inner = request.into_inner();
        let size = requested(inner.to.as_ref())?;
        let instance = self
            .store
            .get_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?;
        let Some(instance) = instance else {
            if inner.move_unit {
                return self.move_session(&identity, &inner, size).await;
            }
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
        if inner.move_unit {
            return self
                .move_instance(&identity, &metadata, &inner, &instance, view, size, &key)
                .await;
        }
        let spec = view.spec.clone();
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
        // The size stays with the instance, not only with this allocation:
        // its next one (a resume, a restart, a reschedule) starts at it.
        if !inner.dry_run && !reply.deduplicated && reply.outcome == "applied" {
            self.record_instance_size(
                &identity,
                &instance.instance_id,
                &spec,
                &reply.to,
                &inner.reason,
            );
        }
        Ok(Response::new(reply))
    }

    /// Who is resizing, as overrides record it.
    fn resized_by(identity: &AclIdentity) -> String {
        identity
            .accessor_id
            .as_deref()
            .map(|accessor| format!("accessor:{accessor}"))
            .unwrap_or_else(|| identity.principal.clone())
    }

    /// Keep an instance's size with the instance. `to` is the effective
    /// size as a reply carries it; a size equal to the spec clears the
    /// record.
    fn record_instance_size(
        &self,
        identity: &AclIdentity,
        instance_id: &str,
        spec: &Value,
        to: &str,
        reason: &str,
    ) {
        let Ok(to) = serde_json::from_str::<Value>(to) else {
            return;
        };
        let at = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default();
        let record =
            ResourcesOverride::between(spec, &to, reason, &Self::resized_by(identity), &at);
        if let Err(err) = self.store.set_instance_size(instance_id, record) {
            warn!(
                instance_id,
                error = ?err,
                "Could not record an instance's resized size; its next allocation starts at the \
                 task's size"
            );
        }
    }

    /// Resize a keyed instance by moving it (docs/resize.md, "When it does
    /// not fit: move"): record the new size with the instance, suspend it,
    /// and start it again, so the scheduler places it at that size on a
    /// node with room. An instance no node takes within the resume timeout
    /// is put back at its old size.
    #[allow(clippy::too_many_arguments)]
    async fn move_instance(
        &self,
        identity: &AclIdentity,
        metadata: &tonic::metadata::MetadataMap,
        inner: &ResizeInstanceRequest,
        instance: &Instance,
        view: AllocationSize,
        size: ResourceSize,
        idempotency_key: &str,
    ) -> Result<Response<ResizeReply>, Status> {
        if let Some(stored) = self
            .store
            .idempotency_lookup(idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            if let Ok(mut reply) = serde_json::from_str::<ResizeReply>(&stored) {
                reply.deduplicated = true;
                return Ok(Response::new(reply));
            }
        }
        let instance_id = instance.instance_id.as_str();
        let config = self
            .store
            .get_job_config(&instance.job_id)
            .map_err(|e| map_store_error("Error loading job config", e))?;
        let task = config
            .tasks
            .iter()
            .find(|task| task.group == instance.group && task.name == instance.task_name);
        let movable =
            instance.mode == InstanceMode::Keyed && task.is_some_and(|task| task.suspend.is_some());
        if !movable {
            return Err(Status::failed_precondition(format!(
                "instance {instance_id} cannot move: only a keyed instance whose task has a \
                 suspend block can be suspended and resumed elsewhere. Hint: resize it in place \
                 (drop --move), or restart it at the new size"
            )));
        }
        let from = ResourceSize::of(&view.effective);
        let after = size
            .apply_to(&view.effective)
            .map_err(|err| Status::invalid_argument(format!("{err}")))?;
        let to = ResourceSize::of(&after);
        let mut reply = base_reply(&from, &to, &view.spec, &view.node_id);
        reply.allocation_id = view.allocation_id;
        reply.instance_id = instance_id.to_string();
        reply.job_id = view.job_id.clone();
        reply.dry_run = inner.dry_run;
        // A guest's size changes only at boot, and a memory checkpoint
        // restores only into a guest of its own size: such a move goes
        // through a disk checkpoint, and says so.
        let reboots = view.runtime == "sandbox" && guest_size(&from) != guest_size(&to);
        let reboot_note = if reboots {
            " The guest's size changes only at boot, so this move takes a disk checkpoint: \
             the guest boots fresh at the new size with its disk kept and its memory gone."
        } else {
            ""
        };
        if inner.dry_run {
            reply.message = format!(
                "would suspend instance {instance_id} and resume it at {} on a node with room; \
                 whether a node has that room is decided when it resumes.{reboot_note}",
                to.to_json()
            );
            return Ok(Response::new(reply));
        }
        let metrics = crate::manager::telemetry::metrics();
        let previous = self
            .store
            .instance_size(instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?;
        let at = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default();
        let by = Self::resized_by(identity);
        let record = ResourcesOverride::between(&view.spec, &after, &inner.reason, &by, &at);
        self.store
            .set_instance_size(instance_id, record)
            .map_err(|e| map_store_error("Error recording the instance's size", e))?;
        let put_back = |why: &str| {
            if let Err(err) = self.store.set_instance_size(instance_id, previous.clone()) {
                warn!(instance_id, error = ?err, "Could not restore an instance's size after a failed move ({why})");
            }
        };

        // Suspend through the handler that already does it, under the
        // caller's own grants (`job:write`).
        if let Err(status) = self
            .suspend_instance_rpc(forwarded(
                metadata,
                crate::structs::SuspendInstanceRequest {
                    instance_id: instance_id.to_string(),
                    checkpoint: reboots.then(|| "disk".to_string()),
                    idempotency_key: None,
                },
            ))
            .await
        {
            put_back("the suspend was refused");
            metrics.resizes_total.inc(&["instance", "error"]);
            return Err(Status::new(
                status.code(),
                format!(
                    "instance {instance_id} could not be suspended to move it: {}",
                    status.message()
                ),
            ));
        }
        let state_of = || {
            self.store
                .get_instance(instance_id)
                .ok()
                .flatten()
                .map(|instance| (instance.state, instance.current_alloc_id))
        };
        let deadline = tokio::time::Instant::now() + MOVE_SUSPEND_TIMEOUT;
        loop {
            match state_of() {
                Some((InstanceState::Suspended, _)) => break,
                Some(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                _ => {
                    metrics.resizes_total.inc(&["instance", "error"]);
                    return Err(Status::deadline_exceeded(format!(
                        "instance {instance_id} did not finish suspending within {}s. Its new \
                         size {} is recorded with it: `legion instance start {instance_id}` \
                         resumes it at that size once it is suspended",
                        MOVE_SUSPEND_TIMEOUT.as_secs(),
                        to.to_json()
                    )));
                }
            }
        }
        if let Err(status) = self
            .start_instance(forwarded(
                metadata,
                StartInstanceRequest {
                    instance_id: instance_id.to_string(),
                    idempotency_key: None,
                    cold: false,
                    force_move: false,
                },
            ))
            .await
        {
            metrics.resizes_total.inc(&["instance", "error"]);
            return Err(Status::new(
                status.code(),
                format!(
                    "instance {instance_id} is suspended with its new size {} recorded, and \
                     could not be started: {}. Hint: `legion instance start {instance_id}`",
                    to.to_json(),
                    status.message()
                ),
            ));
        }
        let deadline = tokio::time::Instant::now() + move_resume_timeout();
        let placed = loop {
            match state_of() {
                Some((InstanceState::Active, Some(allocation_id)))
                    if allocation_id != view.allocation_id =>
                {
                    break self
                        .store
                        .get_allocation_by_id(allocation_id)
                        .ok()
                        .flatten();
                }
                _ if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                _ => break None,
            }
        };
        let Some(allocation) = placed else {
            // No node took it at the new size: back to the old one, and
            // one more eval so it is placed at it.
            put_back("no node had room");
            let _ = self.store.enqueue_eval(
                "job_reschedule",
                Some(&instance.job_id),
                Some(&config.id()),
                None,
                Some(self.store.default_eval_priority),
            );
            reply.outcome = ResizeOutcome::Refused.as_str().to_string();
            reply.message = format!(
                "no node placed instance {instance_id} at {} within {}s; it was put back at {} \
                 and is resuming",
                to.to_json(),
                move_resume_timeout().as_secs(),
                from.to_json()
            );
            reply.hint = "free room on a node (or add one) and try again".to_string();
            reply.retryable = true;
            metrics.resizes_total.inc(&["instance", "refused"]);
            return Ok(Response::new(reply));
        };
        reply.moved_to = allocation.node_id.clone();
        reply.node_id = allocation.node_id.clone();
        reply.allocation_id = allocation.id;
        reply.message = reboot_note.trim().to_string();
        metrics.resizes_total.inc(&["instance", "applied"]);
        self.store.emit_event(
            crate::storage::events::EventDraft::info(
                "instance.resized",
                format!(
                    "Instance {instance_id} resized by moving from {} to {}: {} -> {}",
                    view.node_id,
                    allocation.node_id,
                    from.to_json(),
                    to.to_json()
                ),
            )
            .job(&view.job_id)
            .node(&allocation.node_id)
            .allocation(allocation.id)
            .meta(json!({
                "allocationId": allocation.id,
                "instanceId": instance_id,
                "task": view.task_name,
                "from": from.to_json(),
                "to": to.to_json(),
                "movedFrom": view.node_id,
                "movedTo": allocation.node_id,
                "reason": inner.reason,
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
        Ok(Response::new(reply))
    }

    /// Resize a `store` session by moving it: suspend it on its node and
    /// resume it from its manifest, at the new size, on another node whose
    /// envelope has the room. A session no node takes is resumed where it
    /// was, at the size it had.
    async fn move_session(
        &self,
        identity: &AclIdentity,
        inner: &ResizeInstanceRequest,
        size: ResourceSize,
    ) -> Result<Response<ResizeReply>, Status> {
        let session_id = inner.instance_id.as_str();
        let not_found = || Status::not_found(format!("instance {session_id} not found"));
        let found = sessions::find(&self.store, session_id)
            .await
            .ok_or_else(not_found)?;
        let job_id = found
            .status
            .as_ref()
            .map(|status| status.job_id.clone())
            .unwrap_or_default();
        let read_scope = self.job_read_scope(&job_id);
        // A caller who cannot read the session's job is answered as for a
        // missing id, never PermissionDenied (an existence oracle).
        let hide = |denied: Status| {
            if super::session_rpcs::hides_session(identity, &read_scope, &denied) {
                not_found()
            } else {
                denied
            }
        };
        acl::enforce(identity, AclResource::Scale, AclAction::Write, &read_scope)
            .map_err(hide)?;
        // A move suspends and resumes the session, which is `job:write`.
        acl::enforce(
            identity,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job(&job_id),
        )
        .map_err(hide)?;
        let task_name = found
            .status
            .as_ref()
            .map(|status| status.task_name.clone())
            .unwrap_or_default();
        let config = self
            .store
            .get_job_config(&job_id)
            .map_err(|e| map_store_error("Error loading job config", e))?;
        let task = config
            .tasks
            .iter()
            .find(|task| task.name == task_name)
            .filter(|task| crate::structs::session::publishes_checkpoints(task))
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "session {session_id} cannot move: its task does not set \
                     suspend.durability = \"store\", so its checkpoint lives only on node {}. \
                     Hint: resize it in place (drop --move)",
                    found.node_id
                ))
            })?;
        // A session suspends to a memory checkpoint, which restores only
        // into a guest of the size it was taken at.
        let held = found
            .status
            .as_ref()
            .and_then(|status| serde_json::from_str::<Value>(&status.resources).ok())
            .map(|resources| ResourceSize::of(&resources))
            .unwrap_or_else(|| ResourceSize::of(&task.resources));
        if task.runtime == "sandbox" {
            let after = size
                .apply_to(&held.to_json())
                .map(|value| ResourceSize::of(&value))
                .map_err(|err| Status::invalid_argument(format!("{err}")))?;
            if guest_size(&held) != guest_size(&after) {
                return Err(Status::failed_precondition(format!(
                    "session {session_id} cannot move to a different guest size: a session \
                     resumes from a memory checkpoint, which restores only into a guest of the \
                     size it was taken at ({} MiB). Hint: --move can change its reservation \
                     (--memory, when the task declares memoryMax) and --cpu; for a larger guest, \
                     start a new session",
                    json_amount(guest_size(&held).unwrap_or(0.0))
                )));
            }
        }
        // The node's own dry run names the session's size now and after.
        let asked = resize_on_node(
            &self.store,
            &found.node_id,
            ResizeSessionRequest {
                session_id: session_id.to_string(),
                allocation_id: 0,
                to: Some(wire(&size)),
                force: inner.force,
                dry_run: true,
                reason: inner.reason.clone(),
                idempotency_key: String::new(),
            },
        )
        .await?;
        let mut reply = ResizeReply {
            outcome: ResizeOutcome::Applied.as_str().to_string(),
            from: asked.from.clone(),
            to: asked.to.clone(),
            spec: ResourceSize::of(&task.resources).to_json().to_string(),
            node_id: found.node_id.clone(),
            instance_id: session_id.to_string(),
            job_id: job_id.clone(),
            dry_run: inner.dry_run,
            ..Default::default()
        };
        if inner.dry_run {
            reply.message = format!(
                "would suspend session {session_id} and resume it at {} on another node whose \
                 envelope has room; whether one does is decided when it resumes",
                asked.to
            );
            return Ok(Response::new(reply));
        }
        let metrics = crate::manager::telemetry::metrics();
        let suspended = sessions::suspend_on_node(
            &self.store,
            &found.node_id,
            &[session_id.to_string()],
            "resize",
        )
        .await?;
        if let Some(failure) = suspended.failed.first() {
            metrics.resizes_total.inc(&["session", "error"]);
            return Err(Status::failed_precondition(format!(
                "session {session_id} could not be suspended to move it: {}",
                failure.message
            )));
        }
        let mut sized = task.clone();
        sized.resources = size
            .apply_to(&task.resources)
            .map_err(|err| Status::invalid_argument(format!("{err}")))?;
        let moved = sessions::resume_store_session(
            &self.store,
            &job_id,
            &sized,
            session_id,
            false,
            false,
            Some(&found.node_id),
            Some(wire(&size)),
        )
        .await;
        match moved {
            Ok(
                sessions::StoreResume::Resumed { node_id, .. }
                | sessions::StoreResume::Running { node_id },
            ) => {
                reply.moved_to = node_id.clone();
                reply.node_id = node_id;
                metrics.resizes_total.inc(&["session", "applied"]);
                Ok(Response::new(reply))
            }
            Err(refusal) => {
                // Nobody took it: back where it was, at the size it had.
                let back = sessions::resume_on_node(
                    &self.store,
                    &found.node_id,
                    session_id,
                    false,
                    None,
                    false,
                    None,
                )
                .await;
                if let Err(status) = back {
                    metrics.resizes_total.inc(&["session", "error"]);
                    return Err(Status::unavailable(format!(
                        "session {session_id} was suspended to move it, no node took it at the \
                         new size ({}), and node {} could not resume it at its old size ({}). It \
                         stays suspended with its checkpoint. Hint: `legion instance start \
                         {session_id}`",
                        refusal.message(),
                        found.node_id,
                        status.message()
                    )));
                }
                reply.outcome = ResizeOutcome::Refused.as_str().to_string();
                reply.to = asked.to;
                reply.message = format!(
                    "no other node took session {session_id} at the new size ({}); it was \
                     resumed on {} at the size it had",
                    refusal
                        .message()
                        .split("\nHint:")
                        .next()
                        .unwrap_or_default(),
                    found.node_id
                );
                reply.hint =
                    "end other sessions of the task, raise stateful.envelope, or add a node, \
                     then try again"
                        .to_string();
                reply.retryable = true;
                metrics.resizes_total.inc(&["session", "refused"]);
                Ok(Response::new(reply))
            }
        }
    }

    pub(super) async fn resize_task_rpc(
        &self,
        request: Request<ResizeTaskRequest>,
    ) -> Result<Response<ResizeTaskReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Scale,
            AclAction::Write,
            self.job_read_scope(&request.get_ref().job_id),
        )?;
        let inner = request.into_inner();
        let asked = inner.to.as_ref().map(|to| ResourceSize {
            memory: to.memory,
            memory_max: to.memory_max,
            cpu: to.cpu,
        });
        let size = match (inner.reset, asked.filter(|size| !size.is_empty())) {
            (true, Some(_)) => {
                return Err(Status::invalid_argument(
                    "reset cannot be combined with memory, memoryMax, or cpu",
                ))
            }
            (true, None) => None,
            (false, _) => Some(requested(inner.to.as_ref())?),
        };
        let key = scoped_idempotency_key(
            &identity,
            "resize_task",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(stored) = self
            .store
            .idempotency_lookup(&key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            if let Ok(mut reply) = serde_json::from_str::<ResizeTaskReply>(&stored) {
                reply.deduplicated = true;
                return Ok(Response::new(reply));
            }
        }
        let job_id = inner.job_id.as_str();
        let config = self
            .store
            .get_job_config(job_id)
            .map_err(|e| map_store_error("Error loading job config", e))?;
        let matching: Vec<&TaskConfig> = config
            .tasks
            .iter()
            .filter(|task| inner.task_group.is_empty() || task.group == inner.task_group)
            .filter(|task| inner.task_name.is_empty() || task.name == inner.task_name)
            .collect();
        let task = match matching.as_slice() {
            [task] => *task,
            [] => {
                return Err(Status::not_found(format!(
                    "job {job_id} has no task {}{}",
                    inner.task_name,
                    if inner.task_group.is_empty() {
                        String::new()
                    } else {
                        format!(" in group {}", inner.task_group)
                    }
                )))
            }
            many => {
                return Err(Status::invalid_argument(format!(
                    "job {job_id} has {} tasks that match ({}); name one with --task (and \
                     --group)",
                    many.len(),
                    many.iter()
                        .map(|task| format!("{}/{}", task.group, task.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )))
            }
        };
        if crate::structs::session::is_session_task(task)
            || crate::structs::session::envelope_serves(task).is_some()
        {
            return Err(Status::failed_precondition(format!(
                "{job_id}/{} is a session task: its room is its envelope (stateful.envelope), \
                 and each session is resized on its own with `legion instance resize`",
                task.name
            )));
        }
        let base = self.store.declared_size(&task.resources);
        let base_json = base.to_json();
        let previous = self
            .store
            .task_resize_for(job_id, &task.group, &task.name)
            .map_err(|e| map_store_error("Error reading the task's size override", e))?
            .filter(|resize| resize.binds_to(base));
        let from = previous
            .as_ref()
            .and_then(|resize| resize.size.apply_to(&base_json).ok())
            .map(|value| ResourceSize::of(&value))
            .unwrap_or(base);
        let to_json = match size {
            Some(size) => size
                .apply_to(&base_json)
                .map_err(|err| Status::invalid_argument(format!("{err}")))?,
            None => base_json.clone(),
        };
        let to = ResourceSize::of(&to_json);
        let by = Self::resized_by(&identity);
        let record = ResourcesOverride::between(&base_json, &to_json, &inner.reason, &by, "");
        if !inner.dry_run {
            match record.as_ref() {
                Some(record) => self
                    .store
                    .set_task_resize(
                        job_id,
                        &task.group,
                        &task.name,
                        record.size,
                        base,
                        &inner.reason,
                        &by,
                    )
                    .map_err(|e| map_store_error("Error writing the task's size override", e))?,
                None => {
                    self.store
                        .clear_task_resize(job_id, &task.group, &task.name)
                        .map_err(|e| {
                            map_store_error("Error clearing the task's size override", e)
                        })?;
                }
            }
        }

        // Bring what runs to the new size, one allocation at a time.
        let allocation_ids = self
            .store
            .live_allocations_of_task(job_id, &task.group, &task.name)
            .map_err(|e| map_store_error("Error listing the task's allocations", e))?;
        let mut reply = ResizeTaskReply {
            job_id: job_id.to_string(),
            task_group: task.group.clone(),
            task_name: task.name.clone(),
            spec: base_json.to_string(),
            from: from.to_json().to_string(),
            to: to.to_json().to_string(),
            override_active: record.is_some(),
            dry_run: inner.dry_run,
            ..Default::default()
        };
        let mut to_replace = Vec::new();
        for allocation_id in allocation_ids {
            let Some(view) = self
                .store
                .allocation_size(allocation_id)
                .map_err(|e| map_store_error("Error loading allocation", e))?
            else {
                continue;
            };
            let held = ResourceSize::of(&view.effective);
            // A request names amounts to set; an allocation that holds a
            // ceiling the new size no longer declares gets its reservation
            // as its ceiling, which is what no ceiling means.
            let mut wanted = to;
            if wanted.memory_max.is_none() && held.memory_max.is_some() {
                wanted.memory_max = wanted.memory.or(held.memory);
            }
            let instance_id = view.instance_id.clone();
            let answer = self
                .resize_allocation_unit(
                    &identity,
                    view,
                    wanted,
                    &inner.reason,
                    inner.force,
                    inner.dry_run,
                    "",
                    "allocation",
                )
                .await;
            let mut answer = match answer {
                Ok(answer) => answer,
                Err(status) => ResizeReply {
                    outcome: OUTCOME_ERROR.to_string(),
                    message: status.message().to_string(),
                    allocation_id,
                    job_id: job_id.to_string(),
                    from: held.to_json().to_string(),
                    to: to.to_json().to_string(),
                    dry_run: inner.dry_run,
                    ..Default::default()
                },
            };
            if answer.outcome == "applied" {
                reply.applied += 1;
                // The task's size now governs this unit: a size it carried
                // of its own is dropped.
                if let (false, Some(instance_id)) = (inner.dry_run, instance_id.as_deref()) {
                    if matches!(self.store.instance_size(instance_id), Ok(Some(_))) {
                        let _ = self.store.set_instance_size(instance_id, None);
                    }
                }
            } else if inner.replace {
                if !inner.dry_run {
                    to_replace.push(allocation_id);
                }
                answer.message = format!(
                    "{} to place its replacement at the new size ({}: {})",
                    if inner.dry_run {
                        "would be stopped"
                    } else {
                        "stopped"
                    },
                    answer.outcome,
                    answer.message
                );
                answer.outcome = OUTCOME_REPLACED.to_string();
                answer.hint = String::new();
                reply.replaced += 1;
            } else {
                reply.pending += 1;
            }
            reply.allocations.push(answer);
        }
        if !to_replace.is_empty() {
            self.store
                .replace_allocations_for_resize(&to_replace)
                .map_err(|e| map_store_error("Error replacing allocations", e))?;
            self.store
                .enqueue_eval(
                    "job_scale",
                    Some(job_id),
                    Some(&config.id()),
                    None,
                    Some(self.store.default_eval_priority),
                )
                .map_err(|e| map_store_error("Error enqueueing the replacement eval", e))?;
        }
        if reply.pending > 0 {
            reply.hint = format!(
                "{} allocation(s) kept their old size and take the new one when they are next \
                 replaced; pass --replace to stop them now so the scheduler places their \
                 replacements at it",
                reply.pending
            );
        }
        if !inner.dry_run {
            crate::manager::telemetry::metrics()
                .resizes_total
                .inc(&["task", if inner.reset { "reset" } else { "applied" }]);
            self.store.emit_event(
                crate::storage::events::EventDraft::info(
                    "task.resized",
                    format!(
                        "Job {job_id} task {}/{} {}: {} -> {} ({} applied in place, {} kept \
                         their size, {} replaced){}",
                        task.group,
                        task.name,
                        if inner.reset {
                            "size override removed"
                        } else {
                            "resized"
                        },
                        from.to_json(),
                        to.to_json(),
                        reply.applied,
                        reply.pending,
                        reply.replaced,
                        if inner.reason.is_empty() {
                            String::new()
                        } else {
                            format!(" ({})", inner.reason)
                        }
                    ),
                )
                .job(job_id)
                .meta(json!({
                    "group": task.group,
                    "task": task.name,
                    "from": from.to_json(),
                    "to": to.to_json(),
                    "spec": base_json,
                    "reset": inner.reset,
                    "applied": reply.applied,
                    "pending": reply.pending,
                    "replaced": reply.replaced,
                    "reason": inner.reason,
                    "by": by,
                })),
            );
            if !key.is_empty() {
                if let Ok(stored) = serde_json::to_string(&reply) {
                    let _ = self.store.idempotency_record(&key, "resize_task", &stored);
                }
            }
        }
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
        let read_scope = self.job_read_scope(&job_id);
        // A caller who cannot read the session's job is answered as for a
        // missing id, never PermissionDenied (an existence oracle).
        acl::enforce(identity, AclResource::Scale, AclAction::Write, &read_scope).map_err(
            |denied| {
                if super::session_rpcs::hides_session(identity, &read_scope, &denied) {
                    not_found()
                } else {
                    denied
                }
            },
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
                // Per node, as for session runs: the node keeps the answer.
                idempotency_key: inner
                    .idempotency_key
                    .as_deref()
                    .map(str::trim)
                    .filter(|key| !key.is_empty())
                    .map(|key| format!("{}:{key}", Self::resized_by(identity)))
                    .unwrap_or_default(),
            },
        )
        .await?;
        reply.deduplicated = done.deduplicated;
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
            idempotency_key: String::new(),
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

        let by = Self::resized_by(identity);
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
