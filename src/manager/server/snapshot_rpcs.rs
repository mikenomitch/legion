//! Snapshot RPCs and the manager half of the heartbeat checkpoint channel
//! (suspend plan §6.5, §7; docs/suspend-and-resume.md).
//!
//! The channel, in one paragraph: a `snapshot` row in `capturing` or
//! `deleting` is outstanding work for its node. Every heartbeat from that
//! node gets a [`CheckpointRequest`] per such row (see
//! [`build_checkpoint_requests`]); the worker does the work once, reports a
//! [`SnapshotReport`], and re-sends the same report whenever it sees the same
//! request again, until the row leaves that state. A restored allocation's
//! outcome rides the same reports (`op = restore`). Suspend reuses all of
//! it: a `purpose = suspend` row with `then = stop` (see `suspend_rpcs`).
//!
//! Phase 7b adds two ops. A `local` row with `durability = store` is publish
//! work for its node until the node reports (`op = publish`), which moves it
//! to `published`. A queued warm (WarmSnapshot, held in memory in
//! [`crate::storage::snapshot_warm`]) is `op = warm` work for its node until
//! that node reports.

use super::*;
use crate::storage::store::{
    DeleteSnapshotOutcome, NewSnapshot, SnapshotFilter, SnapshotReportOutcome, SnapshotReportRecord,
};
use crate::structs::checkpoint::SnapshotKind;
use crate::structs::snapshot::{
    SnapshotDurability, SnapshotPurpose, SnapshotRecord, SnapshotState,
};
use crate::structs::{
    CheckpointRequest, CreateSnapshotReply, CreateSnapshotRequest, DeleteSnapshotReply,
    DeleteSnapshotRequest, GetSnapshotReply, GetSnapshotRequest, ListSnapshotsReply,
    ListSnapshotsRequest, RestoreInstanceReply, RestoreInstanceRequest, SnapshotInfo,
    SnapshotReport, WarmSnapshotReply, WarmSnapshotRequest,
};

pub use crate::structs::snapshot::{CHECKPOINT_CAPABILITY, CONTENT_PUBLISH_CAPABILITY};

fn unix_to_rfc3339(unix: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(unix, 0)
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default()
}

/// Wire shape of a snapshot row. `with_manifest` is false for list replies.
pub(crate) fn snapshot_info(snapshot: SnapshotRecord, with_manifest: bool) -> SnapshotInfo {
    SnapshotInfo {
        snapshot_id: snapshot.snapshot_id,
        job_id: snapshot.job_id,
        task: snapshot.task_name,
        instance_id: snapshot.instance_id.unwrap_or_default(),
        name: snapshot.name.unwrap_or_default(),
        kind: snapshot.kind.as_str().to_string(),
        purpose: snapshot.purpose.as_str().to_string(),
        state: snapshot.state.as_str().to_string(),
        node_id: snapshot.node_id.unwrap_or_default(),
        allocation_id: snapshot.allocation_id.unwrap_or_default(),
        total_bytes: snapshot.total_bytes,
        duration_ms: snapshot.duration_ms,
        error: snapshot.error.unwrap_or_default(),
        created_at: unix_to_rfc3339(snapshot.created_at),
        expires_at: snapshot.expires_at.map(unix_to_rfc3339).unwrap_or_default(),
        pause_ms: snapshot.pause_ms,
        aliases: Vec::new(),
        holders: Vec::new(),
        manifest_json: if with_manifest {
            snapshot.manifest_json
        } else {
            String::new()
        },
        durability: snapshot.durability.as_str().to_string(),
        published_at: snapshot
            .published_at
            .map(unix_to_rfc3339)
            .unwrap_or_default(),
    }
}

/// Why a snapshot of `instance` on `node` cannot be published
/// (`durability = "store"`), as the FailedPrecondition message. `None` when
/// it can. Shared by CreateSnapshot `publish` and SuspendInstance.
///
/// A published snapshot must be made only of content-store parts, so the
/// instance's data must not sit on a node-local volume (a suspend leaves the
/// volume in place, a snapshot copies it inside its driver: either way it
/// cannot follow the snapshot to another node). And the node's worker must
/// have a `contentStore` to publish to.
pub(crate) fn publish_precondition(
    task: &TaskConfig,
    instance: &Instance,
    node: &Node,
    field: &str,
) -> Option<String> {
    if let Some(driver) = instance
        .storage_handle
        .as_deref()
        .and_then(|handle| handle.split_once('/'))
        .map(|(driver, _)| driver)
    {
        let node_local = node
            .fingerprint_attrs
            .get("volumeDrivers")
            .and_then(|drivers| drivers.get(driver))
            .and_then(|caps| caps.get("nodeLocal"))
            .and_then(Value::as_bool)
            .unwrap_or_else(|| {
                crate::structs::snapshot::NODE_LOCAL_BUILTIN_DRIVERS.contains(&driver)
            });
        if node_local {
            return Some(crate::structs::snapshot::store_durability_volume_error(
                &task.name, driver, field,
            ));
        }
    }
    if !node_has_capability(node, CONTENT_PUBLISH_CAPABILITY) {
        return Some(format!(
            "instance {} runs on node {}, whose worker has no contentStore to publish to (it \
             does not advertise the `{CONTENT_PUBLISH_CAPABILITY}` capability)\nHint: add a \
             contentStore {{ url }} block to that worker's config (docs/artifacts.md, \
             \"Publishing content\"), or keep {field} = \"local\"",
            instance.instance_id, node.id
        ));
    }
    None
}

/// Whether a node's worker advertised `capability` at registration.
pub(crate) fn node_has_capability(node: &Node, capability: &str) -> bool {
    crate::structs::snapshot::node_advertises(node, capability)
}

/// The worker version a node registered with, for error messages.
pub(crate) fn node_build_version(node: &Node) -> String {
    node.dynamic_attrs
        .get("buildVersion")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string()
}

/// Whether the node's copy of a volume driver advertises an optional op.
fn node_driver_supports(node: &Node, driver: &str, op: &str) -> bool {
    node.fingerprint_attrs
        .get("volumeDrivers")
        .and_then(|drivers| drivers.get(driver))
        .and_then(|caps| caps.get("ops"))
        .and_then(Value::as_array)
        .is_some_and(|ops| ops.iter().any(|advertised| advertised.as_str() == Some(op)))
}

/// Why a running allocation of `task` cannot take a `kind` snapshot on
/// `node`, as the FailedPrecondition message. `None` when it can.
pub(crate) fn snapshot_precondition(
    kind: SnapshotKind,
    task: &TaskConfig,
    instance: &Instance,
    allocation: &crate::structs::Allocation,
    node: &Node,
) -> Option<String> {
    // The instance volume's driver must be able to copy it, for either kind:
    // a memory snapshot without its disk would restore a guest whose page
    // cache describes a disk that moved on.
    let instance_driver = instance
        .storage_handle
        .as_deref()
        .and_then(|handle| handle.split_once('/'))
        .map(|(driver, _)| driver.to_string());
    if let Some(driver) = instance_driver.as_deref() {
        if !node_driver_supports(node, driver, crate::volumes::VolumeOp::Snapshot.as_str()) {
            return Some(format!(
                "instance {} stores its data on volume driver `{driver}`, which does not \
                 support snapshots on node {} (it does not advertise the `snapshot` op)",
                instance.instance_id, node.id
            ));
        }
    }
    match kind {
        SnapshotKind::Disk => {
            if instance_driver.is_none() {
                return Some(format!(
                    "instance {} has no stateful.storage volume, so a disk snapshot has nothing \
                     to capture; declare stateful.storage on task {:?}, or take a memory \
                     snapshot of a sandbox",
                    instance.instance_id, task.name
                ));
            }
            None
        }
        SnapshotKind::Memory => {
            if task.runtime != "sandbox" {
                return Some(format!(
                    "instance {} cannot take a memory snapshot: runtime {:?} supports disk \
                     snapshots only\nHint: use --disk, or run this task with runtime = \"sandbox\"",
                    instance.instance_id, task.runtime
                ));
            }
            let node_vmm = node
                .fingerprint_attrs
                .get(crate::structs::checkpoint::ATTR_SANDBOX_VMM)
                .and_then(Value::as_str);
            let support = crate::task_plugins::sandbox_checkpoint_support(
                &crate::task_plugins::CheckpointQuery::new(&task.config).with_node_vmm(node_vmm),
            );
            if support != crate::task_plugins::CheckpointSupport::Memory {
                return Some(format!(
                    "instance {} cannot take a memory snapshot: its sandbox runs on node {} \
                     without Firecracker or cloud-hypervisor (it resolved {:?})\nHint: use --disk",
                    instance.instance_id,
                    node.id,
                    node_vmm.unwrap_or("no VMM")
                ));
            }
            if crate::task_plugins::allocation_is_gated(allocation) {
                return Some(crate::structs::snapshot::gated_memory_checkpoint_error(
                    &task.name, None,
                ));
            }
            if crate::structs::snapshot::memory_checkpoint_ask(task).is_none() {
                return Some(format!(
                    "instance {} cannot take a memory snapshot: task {:?} was not launched for \
                     memory checkpoints\nHint: add a snapshots block to the task (without \
                     memory = false) and restart the instance, or use --disk",
                    instance.instance_id, task.name
                ));
            }
            None
        }
    }
}

impl ManagerDaemonService {
    /// Enforce `action` on a snapshot's job. NotFound and PermissionDenied
    /// stay indistinguishable for a caller with no snapshot access, because
    /// the coarse check ran before the lookup.
    fn enforce_snapshot_scope(
        &self,
        identity: &AclIdentity,
        action: AclAction,
        job_id: &str,
    ) -> Result<(), Status> {
        acl::enforce(
            identity,
            AclResource::Snapshot,
            action,
            &AclScope::job(job_id),
        )
    }

    #[instrument(skip_all, fields(instance_id = %request.get_ref().instance_id))]
    pub(super) async fn create_snapshot_rpc(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Snapshot,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "create_snapshot",
            inner.idempotency_key.as_deref(),
        );
        if let Some(snapshot_id) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let snapshot = self
                .store
                .get_snapshot(&snapshot_id)
                .map_err(|e| map_store_error("Error loading snapshot", e))?
                .ok_or_else(|| {
                    Status::not_found(format!(
                        "snapshot {snapshot_id} created by this idempotency key no longer exists"
                    ))
                })?;
            self.enforce_snapshot_scope(&identity, AclAction::Write, &snapshot.job_id)?;
            return Ok(Response::new(CreateSnapshotReply {
                snapshot: Some(snapshot_info(snapshot, false)),
                deduplicated: true,
            }));
        }

        let kind: SnapshotKind = inner.kind.parse().map_err(Status::invalid_argument)?;
        let name = inner
            .name
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty());
        if let Some(name) = name.as_deref() {
            crate::structs::snapshot::validate_snapshot_name(name)
                .map_err(Status::invalid_argument)?;
        }

        let instance = self
            .store
            .get_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?
            .ok_or_else(|| {
                Status::not_found(format!("instance {} not found", inner.instance_id))
            })?;
        self.enforce_snapshot_scope(&identity, AclAction::Write, &instance.job_id)?;

        let allocation = match (instance.state, instance.current_alloc_id) {
            (InstanceState::Active, Some(allocation_id)) => self
                .store
                .get_allocation_by_id(allocation_id)
                .map_err(|e| map_store_error("Error loading allocation", e))?,
            _ => None,
        }
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "instance {} has no running allocation (it is {}); start it before taking a \
                 snapshot",
                instance.instance_id, instance.state
            ))
        })?;
        let node = self
            .store
            .get_node(&allocation.node_id)
            .map_err(|e| map_store_error("Error loading node", e))?;
        if !node_has_capability(&node, CHECKPOINT_CAPABILITY) {
            return Err(Status::failed_precondition(format!(
                "node {} runs worker version {}, which cannot take snapshots (it does not \
                 advertise the `{CHECKPOINT_CAPABILITY}` capability); upgrade the worker",
                node.id,
                node_build_version(&node)
            )));
        }
        let task = self
            .store
            .cached_task_configs_for_job_config(&allocation.job_config_id)
            .map_err(|e| map_store_error("Error loading task config", e))?
            .into_iter()
            .find(|(id, _)| *id == allocation.task_config_id)
            .map(|(_, task)| task)
            .ok_or_else(|| {
                Status::internal(format!(
                    "task config {} of allocation {} is missing",
                    allocation.task_config_id, allocation.id
                ))
            })?;
        if let Some(reason) = snapshot_precondition(kind, &task, &instance, &allocation, &node) {
            return Err(Status::failed_precondition(reason));
        }
        let durability = if inner.publish {
            if kind != SnapshotKind::Memory {
                return Err(Status::failed_precondition(format!(
                    "a disk snapshot of instance {} cannot be published: it is a copy of the \
                     instance volume inside its driver on node {}, not content-store parts\n\
                     Hint: drop --publish, or publish a memory snapshot of a sandbox without \
                     stateful.storage",
                    instance.instance_id, node.id
                )));
            }
            if let Some(reason) = publish_precondition(&task, &instance, &node, "--publish") {
                return Err(Status::failed_precondition(reason));
            }
            SnapshotDurability::Store
        } else {
            SnapshotDurability::Local
        };

        let expires_at = task
            .snapshots
            .as_ref()
            .and_then(|snapshots| snapshots.resolve().ok())
            .and_then(|resolved| resolved.ttl)
            .map(|ttl| chrono::Utc::now().timestamp() + ttl.as_secs() as i64);
        let snapshot = self
            .store
            .create_snapshot(NewSnapshot {
                snapshot_id: crate::structs::snapshot::new_snapshot_id(),
                job_id: instance.job_id.clone(),
                task_name: task.name.clone(),
                instance_id: instance.instance_id.clone(),
                name,
                kind,
                purpose: SnapshotPurpose::Manual,
                node_id: node.id.clone(),
                allocation_id: allocation.id,
                expires_at,
                suspend_reason: None,
                durability,
            })
            .map_err(|e| map_store_error("Error creating snapshot", e))?;
        info!(
            snapshot_id = %snapshot.snapshot_id,
            instance_id = %instance.instance_id,
            node_id = %node.id,
            kind = kind.as_str(),
            durability = durability.as_str(),
            "Snapshot requested"
        );
        self.store
            .idempotency_record(&idempotency_key, "create_snapshot", &snapshot.snapshot_id)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(CreateSnapshotReply {
            snapshot: Some(snapshot_info(snapshot, false)),
            deduplicated: false,
        }))
    }

    #[instrument(skip_all)]
    pub(super) async fn list_snapshots_rpc(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Snapshot,
            AclAction::Read,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let state = inner
            .state
            .filter(|state| !state.trim().is_empty())
            .map(|state| state.parse::<SnapshotState>())
            .transpose()
            .map_err(Status::invalid_argument)?;
        let filter = SnapshotFilter {
            job_id: inner.job_id.filter(|value| !value.trim().is_empty()),
            instance_id: inner.instance_id.filter(|value| !value.trim().is_empty()),
            state,
        };
        let mut snapshots: Vec<SnapshotInfo> = self
            .store
            .list_snapshots(&filter)
            .map_err(|e| map_store_error("Error listing snapshots", e))?
            .into_iter()
            .filter(|snapshot| {
                identity.allows(
                    AclResource::Snapshot,
                    AclAction::Read,
                    &AclScope::job(&snapshot.job_id),
                )
            })
            .map(|snapshot| snapshot_info(snapshot, false))
            .collect();
        // Only named snapshots carry aliases; one lookup per named row.
        let named: Vec<String> = snapshots
            .iter()
            .filter(|snapshot| !snapshot.name.is_empty())
            .map(|snapshot| snapshot.snapshot_id.clone())
            .collect();
        let mut aliases = self
            .store
            .snapshot_aliases(&named)
            .map_err(|e| map_store_error("Error loading snapshot names", e))?;
        for snapshot in &mut snapshots {
            snapshot.aliases = aliases.remove(&snapshot.snapshot_id).unwrap_or_default();
        }
        Ok(Response::new(ListSnapshotsReply { snapshots }))
    }

    #[instrument(skip_all, fields(snapshot = %request.get_ref().snapshot))]
    pub(super) async fn get_snapshot_rpc(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<GetSnapshotReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Snapshot,
            AclAction::Read,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let snapshot = self
            .store
            .resolve_snapshot(&inner.snapshot, inner.job_id.as_deref())
            .map_err(|e| map_store_error("Error loading snapshot", e))?
            .ok_or_else(|| Status::not_found(format!("snapshot {} not found", inner.snapshot)))?;
        self.enforce_snapshot_scope(&identity, AclAction::Read, &snapshot.job_id)?;
        let mut aliases = self
            .store
            .snapshot_aliases(std::slice::from_ref(&snapshot.snapshot_id))
            .map_err(|e| map_store_error("Error loading snapshot names", e))?;
        let holders = super::node_content_rpcs::snapshot_holders(&self.store, &snapshot);
        let mut info = snapshot_info(snapshot, true);
        info.aliases = aliases.remove(&info.snapshot_id).unwrap_or_default();
        info.holders = holders;
        Ok(Response::new(GetSnapshotReply {
            snapshot: Some(info),
        }))
    }

    #[instrument(skip_all, fields(snapshot_id = %request.get_ref().snapshot_id))]
    pub(super) async fn delete_snapshot_rpc(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Snapshot,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "delete_snapshot",
            inner.idempotency_key.as_deref(),
        );
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(DeleteSnapshotReply {
                message,
                deduplicated: true,
            }));
        }
        let snapshot = self
            .store
            .get_snapshot(&inner.snapshot_id)
            .map_err(|e| map_store_error("Error loading snapshot", e))?
            .ok_or_else(|| {
                Status::not_found(format!("snapshot {} not found", inner.snapshot_id))
            })?;
        self.enforce_snapshot_scope(&identity, AclAction::Write, &snapshot.job_id)?;
        if snapshot.state == SnapshotState::Capturing {
            return Err(Status::failed_precondition(format!(
                "snapshot {} is still capturing; delete it once it is local or failed",
                snapshot.snapshot_id
            )));
        }
        let restoring = self
            .store
            .list_instances(&snapshot.job_id)
            .map_err(|e| map_store_error("Error loading instances", e))?
            .into_iter()
            .find(|instance| instance.restore_from.as_deref() == Some(&snapshot.snapshot_id));
        if let Some(instance) = restoring {
            return Err(Status::failed_precondition(format!(
                "instance {} is restoring from snapshot {} right now; delete it once the \
                 restore reports back",
                instance.instance_id, snapshot.snapshot_id
            )));
        }
        let outcome = self
            .store
            .delete_snapshot(&snapshot.snapshot_id)
            .map_err(|e| map_store_error("Error deleting snapshot", e))?;
        self.store
            .snapshot_warms()
            .forget_snapshot(&snapshot.snapshot_id);
        let message = match outcome {
            DeleteSnapshotOutcome::Deleting => {
                format!("Deleting snapshot {}", snapshot.snapshot_id)
            }
            DeleteSnapshotOutcome::Removed(removed) => {
                self.store.events().snapshot_deleted(&removed);
                format!("Deleted snapshot {}", snapshot.snapshot_id)
            }
            DeleteSnapshotOutcome::AlreadyDeleting => {
                format!("Snapshot {} already deleting", snapshot.snapshot_id)
            }
        };
        self.store
            .idempotency_record(&idempotency_key, "delete_snapshot", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(DeleteSnapshotReply {
            message,
            deduplicated: false,
        }))
    }

    #[instrument(skip_all, fields(instance_id = %request.get_ref().instance_id))]
    pub(super) async fn restore_instance_rpc(
        &self,
        request: Request<RestoreInstanceRequest>,
    ) -> Result<Response<RestoreInstanceReply>, Status> {
        // Coarse checks first, so a caller with neither grant cannot probe
        // which instances or snapshots exist.
        let identity = self.check_acl(
            &request,
            AclResource::Snapshot,
            AclAction::Read,
            AclScope::any(),
        )?;
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Write,
            &AclScope::any(),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "restore_instance",
            inner.idempotency_key.as_deref(),
        );
        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            // Recorded as "<snapshot_id> <message>".
            let (snapshot_id, message) = recorded
                .split_once(' ')
                .map(|(id, message)| (id.to_string(), message.to_string()))
                .unwrap_or_else(|| (String::new(), recorded.clone()));
            return Ok(Response::new(RestoreInstanceReply {
                message,
                snapshot_id,
                deduplicated: true,
            }));
        }

        let instance = self
            .store
            .get_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?
            .ok_or_else(|| {
                Status::not_found(format!("instance {} not found", inner.instance_id))
            })?;
        self.enforce_snapshot_scope(&identity, AclAction::Read, &instance.job_id)?;
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job(&instance.job_id),
        )?;
        if instance.mode != InstanceMode::Keyed {
            return Err(Status::invalid_argument(format!(
                "instance {} is slot-mode; slot lifecycles are owned by the scheduler, so only \
                 keyed instances can be restored in place",
                instance.instance_id
            )));
        }
        if matches!(
            instance.state,
            InstanceState::Stopped | InstanceState::Terminated
        ) {
            return Err(Status::failed_precondition(format!(
                "instance {} is {}; start it before restoring a snapshot",
                instance.instance_id, instance.state
            )));
        }
        let job = self
            .store
            .get_job(&instance.job_id)
            .map_err(|e| map_store_error("Error loading job", e))?;
        if job.status == "stopped" {
            return Err(Status::failed_precondition(format!(
                "job {} is stopped; run the job first, then restore its instances",
                instance.job_id
            )));
        }
        let snapshot = self
            .store
            .resolve_snapshot(&inner.snapshot, Some(&instance.job_id))
            .map_err(|e| map_store_error("Error loading snapshot", e))?
            .ok_or_else(|| Status::not_found(format!("snapshot {} not found", inner.snapshot)))?;
        if snapshot.instance_id.as_deref() != Some(instance.instance_id.as_str()) {
            return Err(Status::invalid_argument(format!(
                "snapshot {} was taken from instance {}, not {}; a snapshot rolls back only the \
                 instance it came from\nHint: start a new instance from it with `legion instance \
                 create {} --from-snapshot {}`",
                snapshot.snapshot_id,
                snapshot.instance_id.as_deref().unwrap_or("-"),
                instance.instance_id,
                snapshot.job_id,
                snapshot.snapshot_id
            )));
        }
        if !snapshot.state.is_restorable() {
            return Err(Status::failed_precondition(format!(
                "snapshot {} is {}{}; only a captured (local or published) snapshot can be \
                 restored",
                snapshot.snapshot_id,
                snapshot.state,
                snapshot
                    .error
                    .as_deref()
                    .map(|error| format!(" ({error})"))
                    .unwrap_or_default()
            )));
        }

        self.store
            .restore_keyed_instance(&instance.instance_id, &snapshot.snapshot_id)
            .map_err(|e| map_store_error("Error restoring instance", e))?;
        let config = self
            .store
            .get_job_config(&instance.job_id)
            .map_err(|e| map_store_error("Error loading job config", e))?;
        self.store
            .enqueue_eval(
                "job_reschedule",
                Some(&instance.job_id),
                Some(&config.id()),
                None,
                Some(self.store.default_eval_priority),
            )
            .map_err(|e| map_store_error("Error scheduling instance restore", e))?;
        let message = format!(
            "Restoring instance {} from snapshot {}",
            instance.instance_id, snapshot.snapshot_id
        );
        self.store
            .idempotency_record(
                &idempotency_key,
                "restore_instance",
                &format!("{} {message}", snapshot.snapshot_id),
            )
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(RestoreInstanceReply {
            message,
            snapshot_id: snapshot.snapshot_id,
            deduplicated: false,
        }))
    }

    #[instrument(skip_all, fields(snapshot = %request.get_ref().snapshot))]
    pub(super) async fn warm_snapshot_rpc(
        &self,
        request: Request<WarmSnapshotRequest>,
    ) -> Result<Response<WarmSnapshotReply>, Status> {
        // Coarse first, so a caller with no snapshot access cannot probe
        // which snapshots exist.
        let identity = self.check_acl(
            &request,
            AclResource::Snapshot,
            AclAction::Read,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let constraints = parse_warm_constraints(&inner.constraints)?;
        let idempotency_key =
            scoped_idempotency_key(&identity, "warm_snapshot", inner.idempotency_key.as_deref());
        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let recorded: RecordedWarm = serde_json::from_str(&recorded).unwrap_or_default();
            return Ok(Response::new(recorded.into_reply(true)));
        }
        let snapshot = self
            .store
            .resolve_snapshot(&inner.snapshot, inner.job_id.as_deref())
            .map_err(|e| map_store_error("Error loading snapshot", e))?
            .ok_or_else(|| Status::not_found(format!("snapshot {} not found", inner.snapshot)))?;
        self.enforce_snapshot_scope(&identity, AclAction::Read, &snapshot.job_id)?;
        if snapshot.state != SnapshotState::Published {
            let hint = if snapshot.durability == SnapshotDurability::Store
                && snapshot.state == SnapshotState::Local
                && snapshot.error.is_none()
            {
                "its upload is still running; retry once it is published".to_string()
            } else {
                "a local snapshot exists on its node only. Publish snapshots with \
                 suspend.durability = \"store\" or `legion snapshot create --publish`"
                    .to_string()
            };
            return Err(Status::failed_precondition(format!(
                "snapshot {} is {}, not published, so there is nothing for other nodes to \
                 fetch\nHint: {hint}",
                snapshot.snapshot_id, snapshot.state
            )));
        }
        let manifest = snapshot
            .manifest()
            .map_err(|err| Status::internal(format!("snapshot manifest unreadable: {err:#}")))?
            .ok_or_else(|| Status::internal("published snapshot has no manifest"))?;

        // The nodes the scheduler would consider (`availability =
        // available`), so a drained or lost node is never warmed.
        let nodes = self
            .store
            .available_node_arcs()
            .map_err(|e| map_store_error("Error loading nodes", e))?;
        let mut plan = WarmPlan::default();
        let now = std::time::Instant::now();
        for node in nodes
            .iter()
            .filter(|node| constraints.is_satisfied(node, None))
        {
            // A worker that cannot fetch published parts cannot warm them.
            if !node_has_capability(node, crate::structs::snapshot::CHECKPOINT_FETCH_CAPABILITY) {
                continue;
            }
            let empty = serde_json::Map::new();
            let attrs = node.fingerprint_attrs.as_object().unwrap_or(&empty);
            if let Err(mismatch) = crate::structs::checkpoint::compat_matches(&manifest, attrs) {
                plan.incompatible.push(format!("{}: {mismatch}", node.id));
                continue;
            }
            // The location index only carries entries of at least
            // MIN_ADVERTISED_BYTES, so small parts (a VMM config) are never
            // evidence either way; the node re-checks them cheaply anyway.
            let holds_all = manifest
                .memory
                .iter()
                .filter(|part| part.size >= crate::storage::content_index::MIN_ADVERTISED_BYTES)
                .all(|part| {
                    self.store
                        .content_locations()
                        .bytes_on(&part.digest, &node.id)
                        .is_some()
                })
                && manifest
                    .memory
                    .iter()
                    .any(|part| part.size >= crate::storage::content_index::MIN_ADVERTISED_BYTES);
            if holds_all {
                plan.already_warm.push(node.id.clone());
                continue;
            }
            self.store.snapshot_warms().queue(
                &node.id,
                &snapshot.snapshot_id,
                &snapshot.manifest_json,
                now,
            );
            plan.nodes.push(node.id.clone());
        }
        if plan.nodes.is_empty() && plan.already_warm.is_empty() {
            return Err(Status::failed_precondition(format!(
                "no available node matches the constraints and can restore snapshot {}{}",
                snapshot.snapshot_id,
                if plan.incompatible.is_empty() {
                    String::new()
                } else {
                    format!(" (incompatible: {})", plan.incompatible.join("; "))
                }
            )));
        }
        info!(
            snapshot_id = %snapshot.snapshot_id,
            nodes = plan.nodes.len(),
            already_warm = plan.already_warm.len(),
            incompatible = plan.incompatible.len(),
            "Snapshot warm queued"
        );
        let recorded = RecordedWarm {
            message: format!(
                "Warming snapshot {} on {} node(s){}",
                snapshot.snapshot_id,
                plan.nodes.len(),
                if plan.already_warm.is_empty() {
                    String::new()
                } else {
                    format!(" ({} already warm)", plan.already_warm.len())
                }
            ),
            snapshot_id: snapshot.snapshot_id,
            nodes: plan.nodes,
            already_warm: plan.already_warm,
            incompatible: plan.incompatible,
        };
        self.store
            .idempotency_record(
                &idempotency_key,
                "warm_snapshot",
                &serde_json::to_string(&recorded).unwrap_or_default(),
            )
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(recorded.into_reply(false)))
    }
}

/// Which nodes a WarmSnapshot call picked, and why it skipped the others.
#[derive(Debug, Default)]
struct WarmPlan {
    nodes: Vec<String>,
    already_warm: Vec<String>,
    incompatible: Vec<String>,
}

/// A WarmSnapshot reply, recorded under its idempotency key.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct RecordedWarm {
    message: String,
    snapshot_id: String,
    nodes: Vec<String>,
    already_warm: Vec<String>,
    incompatible: Vec<String>,
}

impl RecordedWarm {
    fn into_reply(self, deduplicated: bool) -> WarmSnapshotReply {
        WarmSnapshotReply {
            snapshot_id: self.snapshot_id,
            nodes: self.nodes,
            already_warm: self.already_warm,
            incompatible: self.incompatible,
            message: self.message,
            deduplicated,
        }
    }
}

/// `key=value` constraints, as a [`crate::constraints::ConstraintSet`] (the
/// same matching a jobspec `constraint = { key = value }` block gets).
fn parse_warm_constraints(raw: &[String]) -> Result<crate::constraints::ConstraintSet, Status> {
    let mut map = serde_json::Map::new();
    for entry in raw {
        let Some((key, value)) = entry
            .split_once('=')
            .map(|(key, value)| (key.trim(), value.trim()))
            .filter(|(key, value)| !key.is_empty() && !value.is_empty())
        else {
            return Err(Status::invalid_argument(format!(
                "constraint {entry:?} is not key=value (for example region=us-west)"
            )));
        };
        map.insert(key.to_string(), Value::String(value.to_string()));
    }
    crate::constraints::ConstraintSet::new(&Value::Object(map))
        .map_err(|err| Status::invalid_argument(format!("invalid constraint: {err:#}")))
}

/// Apply a heartbeat's snapshot reports and emit their events. Never fails
/// the heartbeat: a report that cannot be applied now is re-sent by the
/// worker as long as the manager keeps re-sending the request.
///
/// Every report a heartbeat carries, and the events each one emits, commit
/// as one scheduler-lane entry, group-committed with other nodes' reports
/// and instance writes (sandbox-scaling plan, Phase 1). Each report used to
/// be an entry of its own plus one per event: a suspend's capture report
/// was three. A report that fails rolls back alone inside the entry. A
/// manager that does not lead the scheduler lane falls back to applying
/// them one by one, each forwarded to the leader as before.
pub(super) fn apply_snapshot_reports(store: &Store, node_id: &str, reports: &[SnapshotReport]) {
    // Warms advance an in-memory queue, not a row, so they stay out of the
    // replicated entry.
    let (warms, rows): (Vec<&SnapshotReport>, Vec<&SnapshotReport>) =
        reports.iter().partition(|report| report.op == "warm");
    for report in warms {
        apply_one_snapshot_report(store, node_id, report);
    }
    if rows.is_empty() {
        return;
    }
    let node = node_id.to_string();
    let batch: Vec<SnapshotReport> = rows.iter().map(|report| (*report).clone()).collect();
    let fused = store.scheduler_group_commit("apply_snapshot_reports", move |s| {
        for report in &batch {
            apply_one_snapshot_report(s, &node, report);
        }
        Ok(())
    });
    match fused {
        Ok(()) => {}
        Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => {
            for report in rows {
                apply_one_snapshot_report(store, node_id, report);
            }
        }
        Err(err) => {
            warn!(
                node_id,
                reports = rows.len(),
                error = ?err,
                "Failed applying snapshot reports; the worker will re-send them"
            );
        }
    }
}

/// One report: advance its row and emit its event. Inside the fused entry
/// both land in the same changeset; outside it (the fallback) each is its
/// own op, as before.
fn apply_one_snapshot_report(store: &Store, node_id: &str, report: &SnapshotReport) {
    {
        // A warm has no row to advance: clear it from the queue and say how
        // it went. Duplicates (a re-sent report) find nothing queued and
        // emit nothing.
        if report.op == "warm" {
            if store
                .snapshot_warms()
                .complete(node_id, &report.snapshot_id)
            {
                let job_id = store
                    .get_snapshot(&report.snapshot_id)
                    .ok()
                    .flatten()
                    .map(|snapshot| snapshot.job_id);
                if report.ok {
                    info!(
                        node_id,
                        snapshot_id = %report.snapshot_id,
                        fetched_bytes = report.total_bytes,
                        duration_ms = report.duration_ms,
                        "Snapshot warmed"
                    );
                } else {
                    warn!(
                        node_id,
                        snapshot_id = %report.snapshot_id,
                        error = %report.error,
                        "Snapshot warm failed"
                    );
                }
                store.events().snapshot_warm(
                    &report.snapshot_id,
                    job_id.as_deref(),
                    node_id,
                    report.ok,
                    report.total_bytes,
                    report.duration_ms,
                    &report.error,
                );
            }
            return;
        }
        let record = SnapshotReportRecord::from(report);
        let outcome = match store.apply_snapshot_report(node_id, &record) {
            Ok(outcome) => outcome,
            Err(err) => {
                warn!(
                    node_id,
                    snapshot_id = %report.snapshot_id,
                    op = %report.op,
                    error = ?err,
                    "Failed applying snapshot report; the worker will re-send it"
                );
                return;
            }
        };
        match outcome {
            SnapshotReportOutcome::Captured { snapshot, evicted } => {
                info!(
                    node_id,
                    snapshot_id = %snapshot.snapshot_id,
                    total_bytes = snapshot.total_bytes,
                    duration_ms = snapshot.duration_ms,
                    evicted = evicted.len(),
                    "Snapshot captured"
                );
                crate::manager::telemetry::metrics()
                    .checkpoint_bytes
                    .observe(&[snapshot.kind.as_str()], snapshot.total_bytes as f64);
                store.events().snapshot_created(&snapshot);
            }
            SnapshotReportOutcome::Suspended {
                snapshot,
                instance_id,
                reserved,
                reason,
            } => {
                info!(
                    node_id,
                    instance_id,
                    snapshot_id = %snapshot.snapshot_id,
                    kind = snapshot.kind.as_str(),
                    total_bytes = snapshot.total_bytes,
                    duration_ms = snapshot.duration_ms,
                    reserved,
                    reason = %reason,
                    "Instance suspended"
                );
                let metrics = crate::manager::telemetry::metrics();
                metrics.instance_suspend_seconds.observe(
                    &[snapshot.kind.as_str()],
                    snapshot.duration_ms as f64 / 1000.0,
                );
                metrics
                    .checkpoint_bytes
                    .observe(&[snapshot.kind.as_str()], snapshot.total_bytes as f64);
                metrics.instance_suspends_total.inc(&[reason.as_str()]);
                store
                    .events()
                    .instance_suspended(&snapshot, &instance_id, reserved, &reason);
                if !reserved {
                    store.events().allocations_removed_for_job(
                        &snapshot.job_id,
                        1,
                        crate::structs::StopCause::Suspended,
                    );
                }
            }
            SnapshotReportOutcome::LocalSuspensionDiscarded {
                snapshot_id,
                allocation_id,
                reason,
            } => {
                warn!(
                    node_id,
                    snapshot_id = %snapshot_id,
                    allocation_id,
                    reason = %reason,
                    "Local suspension arrived after its instance moved on; reclaiming the checkpoint"
                );
            }
            SnapshotReportOutcome::SuspendFailed { snapshot } => {
                warn!(
                    node_id,
                    instance_id = snapshot.instance_id.as_deref().unwrap_or(""),
                    snapshot_id = %snapshot.snapshot_id,
                    error = snapshot.error.as_deref().unwrap_or(""),
                    "Suspend failed; the instance keeps running"
                );
                store.events().instance_suspend_failed(&snapshot);
            }
            SnapshotReportOutcome::Resumed {
                snapshot_id,
                kind,
                job_id,
                instance_id,
                node_id,
                allocation_id,
                restore_ms,
            } => {
                info!(
                    node_id,
                    instance_id, snapshot_id, allocation_id, restore_ms, "Instance resumed"
                );
                crate::manager::telemetry::metrics()
                    .instance_resume_seconds
                    .observe(&[kind.as_str()], restore_ms as f64 / 1000.0);
                store.events().instance_resumed(
                    &snapshot_id,
                    &job_id,
                    &instance_id,
                    &node_id,
                    allocation_id,
                    restore_ms,
                );
            }
            SnapshotReportOutcome::ResumeFailed {
                snapshot_id,
                job_id,
                instance_id,
                node_id,
                allocation_id,
                error,
                cold,
            } => {
                warn!(
                    node_id,
                    instance_id,
                    snapshot_id,
                    error = %error,
                    cold,
                    "Resume could not restore its checkpoint"
                );
                store.events().instance_restore_failed(
                    &snapshot_id,
                    &job_id,
                    &instance_id,
                    Some(&node_id),
                    Some(allocation_id),
                    &error,
                    cold,
                );
            }
            SnapshotReportOutcome::CaptureFailed { snapshot } => {
                warn!(
                    node_id,
                    snapshot_id = %snapshot.snapshot_id,
                    error = snapshot.error.as_deref().unwrap_or(""),
                    "Snapshot capture failed"
                );
                store.events().snapshot_failed(&snapshot);
            }
            SnapshotReportOutcome::Published {
                snapshot,
                uploaded_bytes,
                duration_ms,
                released_forks,
            } => {
                info!(
                    node_id,
                    snapshot_id = %snapshot.snapshot_id,
                    uploaded_bytes,
                    duration_ms,
                    "Snapshot published"
                );
                let metrics = crate::manager::telemetry::metrics();
                metrics
                    .checkpoint_publish_bytes
                    .observe(&[snapshot.kind.as_str()], uploaded_bytes as f64);
                metrics
                    .checkpoint_publish_seconds
                    .observe(&[snapshot.kind.as_str()], duration_ms as f64 / 1000.0);
                metrics.checkpoint_publish_total.inc(&["published"]);
                store
                    .events()
                    .snapshot_published(&snapshot, uploaded_bytes, duration_ms);
                // A spread fork's forks waited for the publish.
                if !released_forks.is_empty() {
                    let origin = snapshot.instance_id.clone().unwrap_or_default();
                    info!(
                        node_id,
                        origin = %origin,
                        snapshot_id = %snapshot.snapshot_id,
                        forks = released_forks.len(),
                        "Fork checkpoint published; placing spread forks"
                    );
                    store
                        .events()
                        .instance_forked(&snapshot, &origin, &released_forks);
                    enqueue_job_placement(store, &snapshot.job_id);
                }
            }
            SnapshotReportOutcome::PublishFailed {
                snapshot,
                retrying,
                terminated_forks,
            } => {
                warn!(
                    node_id,
                    snapshot_id = %snapshot.snapshot_id,
                    error = snapshot.error.as_deref().unwrap_or(""),
                    retrying,
                    "Snapshot publish failed; it stays local to its node"
                );
                crate::manager::telemetry::metrics()
                    .checkpoint_publish_total
                    .inc(&[if retrying { "retrying" } else { "failed" }]);
                store.events().snapshot_publish_failed(&snapshot);
                if !terminated_forks.is_empty() {
                    store
                        .events()
                        .instance_fork_failed(&snapshot, &terminated_forks);
                }
            }
            SnapshotReportOutcome::Deleted { snapshot } => {
                store.events().snapshot_deleted(&snapshot);
            }
            SnapshotReportOutcome::DeleteFailed { snapshot } => {
                warn!(
                    node_id,
                    snapshot_id = %snapshot.snapshot_id,
                    error = snapshot.error.as_deref().unwrap_or(""),
                    "Snapshot reclaim failed on the node; retrying"
                );
            }
            SnapshotReportOutcome::Restored {
                snapshot_id,
                job_id,
                instance_id,
                allocation_id,
                ok,
                error,
            } => {
                store.events().snapshot_restore(
                    &snapshot_id,
                    &job_id,
                    instance_id.as_deref(),
                    node_id,
                    allocation_id,
                    ok,
                    &error,
                );
            }
            SnapshotReportOutcome::Forked {
                snapshot,
                origin,
                instances,
            } => {
                info!(
                    node_id,
                    origin = %origin,
                    snapshot_id = %snapshot.snapshot_id,
                    pause_ms = snapshot.pause_ms,
                    forks = instances.len(),
                    "Fork checkpoint landed; placing forks"
                );
                crate::manager::telemetry::metrics()
                    .checkpoint_bytes
                    .observe(&[snapshot.kind.as_str()], snapshot.total_bytes as f64);
                store
                    .events()
                    .instance_forked(&snapshot, &origin, &instances);
                // The forks were held out of placement until now.
                if !instances.is_empty() {
                    enqueue_job_placement(store, &snapshot.job_id);
                }
            }
            SnapshotReportOutcome::ForkFailed {
                snapshot,
                instances,
            } => {
                warn!(
                    node_id,
                    origin = snapshot.instance_id.as_deref().unwrap_or(""),
                    snapshot_id = %snapshot.snapshot_id,
                    error = snapshot.error.as_deref().unwrap_or(""),
                    "Fork capture failed; the origin keeps running"
                );
                store.events().instance_fork_failed(&snapshot, &instances);
            }
            SnapshotReportOutcome::NewInstanceRestoreFailed {
                snapshot_id,
                job_id,
                instance_id,
                node_id,
                allocation_id,
                error,
                cold,
            } => {
                warn!(
                    node_id,
                    instance_id,
                    snapshot_id,
                    error = %error,
                    cold,
                    "A new instance could not restore its snapshot"
                );
                store.events().new_instance_restore_failed(
                    &snapshot_id,
                    &job_id,
                    &instance_id,
                    &node_id,
                    allocation_id,
                    &error,
                    cold,
                );
            }
            SnapshotReportOutcome::Ignored => {}
        }
    }
}

/// Enqueue a placement evaluation for a job's current config, as
/// CreateInstance does. Never fails the caller: a missed eval is picked up
/// by the next one for the job.
fn enqueue_job_placement(store: &Store, job_id: &str) {
    let config_id = match store.get_job_config(job_id) {
        Ok(config) => config.id(),
        Err(err) => {
            warn!(job_id, error = ?err, "Failed loading job config to place forks");
            return;
        }
    };
    if let Err(err) = store.enqueue_eval(
        "job_reschedule",
        Some(job_id),
        Some(&config_id),
        None,
        Some(store.default_eval_priority),
    ) {
        warn!(job_id, error = ?err, "Failed enqueueing fork placement");
    }
}

/// Checkpoint work for one node: a request per `capturing` or `deleting`
/// snapshot it holds, re-sent every heartbeat until the node reports. Only
/// sent to workers that advertised the `checkpoint` capability. Never fails
/// the heartbeat.
pub(super) fn build_checkpoint_requests(store: &Store, node_id: &str) -> Vec<CheckpointRequest> {
    let work = match store.snapshot_work_for_node(node_id) {
        Ok(work) => work,
        Err(err) => {
            warn!(node_id, error = ?err, "Failed listing snapshot work");
            return Vec::new();
        }
    };
    let warms = store
        .snapshot_warms()
        .pending_for(node_id, std::time::Instant::now());
    if work.is_empty() && warms.is_empty() {
        return Vec::new();
    }
    let capable = store
        .get_node(node_id)
        .map(|node| node_has_capability(&node, CHECKPOINT_CAPABILITY))
        .unwrap_or(false);
    if !capable {
        return Vec::new();
    }
    let warm_requests = warms.into_iter().map(|warm| {
        let (part_keys, part_key_error) =
            checkpoint_part_keys(store, &warm.manifest_json, PartKeyUse::Fetch);
        CheckpointRequest {
            snapshot_id: warm.snapshot_id,
            op: "warm".into(),
            allocation_id: 0,
            kind: String::new(),
            then: String::new(),
            purpose: String::new(),
            manifest_json: warm.manifest_json,
            part_keys,
            part_key_error,
        }
    });
    work.into_iter()
        .map(|snapshot| match snapshot.state {
            // A `store` snapshot that is captured but not yet published.
            SnapshotState::Local => {
                let (part_keys, part_key_error) =
                    checkpoint_part_keys(store, &snapshot.manifest_json, PartKeyUse::Publish);
                CheckpointRequest {
                    snapshot_id: snapshot.snapshot_id,
                    op: "publish".into(),
                    allocation_id: 0,
                    kind: snapshot.kind.as_str().into(),
                    then: String::new(),
                    purpose: snapshot.purpose.as_str().into(),
                    manifest_json: snapshot.manifest_json,
                    part_keys,
                    part_key_error,
                }
            }
            SnapshotState::Capturing => CheckpointRequest {
                snapshot_id: snapshot.snapshot_id,
                op: "capture".into(),
                allocation_id: snapshot.allocation_id.unwrap_or_default(),
                kind: snapshot.kind.as_str().into(),
                // A suspend leaves the task stopped; every other capture
                // keeps it running.
                then: if snapshot.purpose == SnapshotPurpose::Suspend {
                    crate::task_plugins::AfterCheckpoint::Stop
                } else {
                    crate::task_plugins::AfterCheckpoint::Resume
                }
                .as_str()
                .into(),
                purpose: snapshot.purpose.as_str().into(),
                manifest_json: String::new(),
                ..Default::default()
            },
            _ => CheckpointRequest {
                snapshot_id: snapshot.snapshot_id,
                op: "delete".into(),
                allocation_id: 0,
                kind: snapshot.kind.as_str().into(),
                then: String::new(),
                purpose: snapshot.purpose.as_str().into(),
                manifest_json: snapshot.manifest_json,
                ..Default::default()
            },
        })
        .chain(warm_requests)
        .collect()
}

/// What a set of part keys is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartKeyUse {
    /// Sealing a new publish: every part, under the current checkpoint key.
    Publish,
    /// Opening published parts (a restore or a warm): each part with a
    /// `url`, under the checkpoint key its `keyId` names.
    Fetch,
}

/// The part keys a worker needs for one piece of checkpoint work on the
/// snapshot whose manifest is `manifest_json`, and the reason for any key
/// it cannot have (docs/suspend-and-resume.md, "Security"). Keys are
/// derived only for the parts of a manifest this manager holds, and only
/// travel with work the manager chose to send, so a worker can never ask for
/// one. Never fails the heartbeat: a missing key becomes `part_key_error`,
/// which the worker reports when it actually needs that key.
pub fn checkpoint_part_keys(
    store: &Store,
    manifest_json: &str,
    usage: PartKeyUse,
) -> (Vec<crate::structs::CheckpointPartKey>, String) {
    let Ok(manifest) = crate::structs::checkpoint::SnapshotManifest::from_json(manifest_json)
    else {
        // The worker reports the unreadable manifest itself.
        return (Vec::new(), String::new());
    };
    let mut keys = Vec::new();
    let mut errors: Vec<String> = Vec::new();
    for part in &manifest.memory {
        let key_id = match usage {
            PartKeyUse::Publish => None,
            PartKeyUse::Fetch => {
                if part.url.is_none() {
                    continue;
                }
                match part.key_id.as_deref() {
                    Some(id) => Some(id),
                    // Published before checkpoint encryption: the worker's
                    // fetch names it.
                    None => continue,
                }
            }
        };
        let derived = crate::content_store::ContentDigest::parse(&part.digest)
            .and_then(|digest| store.keyring().checkpoint_part_key(key_id, &digest));
        match derived {
            Ok(key) => keys.push(key.to_proto()),
            Err(err) => {
                let err = format!("{err:#}");
                if !errors.contains(&err) {
                    errors.push(err);
                }
            }
        }
    }
    if !errors.is_empty() {
        warn!(
            snapshot_id = %manifest.id,
            error = %errors.join("; "),
            "Cannot derive checkpoint part keys; the work will fail on the worker"
        );
    }
    (keys, errors.join("; "))
}

/// The restore payload for an allocation whose instance is restoring.
/// `None` for every ordinary allocation, at no store cost: the instance row
/// is already loaded.
pub(crate) fn snapshot_restore_payload(
    store: &Store,
    instance: Option<&Instance>,
) -> Option<crate::structs::SnapshotRestore> {
    let snapshot_id = instance?.restore_from.as_deref()?;
    let snapshot = match store.get_snapshot(snapshot_id) {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => return None,
        Err(err) => {
            warn!(snapshot_id, error = ?err, "Failed loading restore snapshot");
            return None;
        }
    };
    let instance = instance?;
    let on_resume = store
        .suspend_settings_for(&instance.job_id, &instance.task_name)
        .on_resume;
    // Phase 6: a snapshot taken from another instance seeds a new one (a
    // fork, or a `from_snapshot` create): the worker clones its volumes and
    // gives the guest a NIC identity of its own.
    let fork = snapshot.instance_id.as_deref() != Some(instance.instance_id.as_str());
    let (part_keys, part_key_error) =
        checkpoint_part_keys(store, &snapshot.manifest_json, PartKeyUse::Fetch);
    Some(crate::structs::SnapshotRestore {
        snapshot_id: snapshot.snapshot_id,
        kind: snapshot.kind.as_str().to_string(),
        manifest_json: snapshot.manifest_json,
        on_resume,
        fork,
        instance_id: instance.instance_id.clone(),
        part_keys,
        part_key_error,
    })
}

/// The `snapshots.ttl` sweep, run by the manager daemon's maintenance loop.
pub fn expire_snapshots(store: &Store) {
    match store.expire_snapshots(chrono::Utc::now().timestamp()) {
        Ok((deleting, removed)) => {
            if !deleting.is_empty() {
                info!(
                    count = deleting.len(),
                    "Expired snapshots; reclaiming on their nodes"
                );
            }
            for snapshot in removed {
                store.events().snapshot_deleted(&snapshot);
            }
        }
        Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => {}
        Err(err) => warn!(error = ?err, "Failed expiring snapshots"),
    }
    // Phase 6: fork checkpoints whose forks were stopped or destroyed before
    // they restored.
    match store.spend_unused_fork_snapshots() {
        Ok(spent) if !spent.is_empty() => {
            info!(count = spent.len(), "Reclaiming unused fork checkpoints");
        }
        Ok(_) => {}
        Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => {}
        Err(err) => warn!(error = ?err, "Failed reclaiming unused fork checkpoints"),
    }
}
