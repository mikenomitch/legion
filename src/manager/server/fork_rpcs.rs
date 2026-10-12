//! ForkInstance, `CreateInstance(s).from_snapshot`, and snapshot names
//! (suspend plan §6.9, Phase 6; docs/suspend-and-resume.md, "Fork").
//!
//! A fork rides the snapshot checkpoint channel like a suspend does: this
//! module inserts a `purpose = fork` snapshot row and the waiting fork
//! instances (`Store::begin_fork`), `snapshot_rpcs` asks the origin's node
//! for a live capture (`then = resume`), and the node's report releases the
//! forks for placement (`Store::apply_fork_capture_tx`; for `--spread`, the
//! publish that follows it, `Store::release_forks_tx`). From there a fork
//! is an ordinary instance restoring from a snapshot taken from another
//! instance, which is also exactly what `from_snapshot` creates.

use serde::{Deserialize, Serialize};

use super::snapshot_rpcs::{node_build_version, node_has_capability, CONTENT_PUBLISH_CAPABILITY};
use super::suspend_rpcs::suspend_memory_precondition;
use super::*;
use crate::storage::store::NewSnapshot;
use crate::structs::checkpoint::SnapshotKind;
use crate::structs::fork::MAX_FORK_COUNT;
use crate::structs::snapshot::{SnapshotDurability, SnapshotPurpose, SnapshotRecord};
use crate::structs::{
    DeleteSnapshotAliasReply, DeleteSnapshotAliasRequest, ForkInstanceReply, ForkInstanceRequest,
    SetSnapshotAliasReply, SetSnapshotAliasRequest,
};

/// What a fork reply carries, recorded under its idempotency key so a retry
/// returns the same forks instead of creating more.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RecordedFork {
    origin: String,
    snapshot: String,
    kind: String,
    node_id: String,
    instances: Vec<String>,
    /// Absent in forks recorded before spread forks shipped (all local).
    #[serde(default)]
    durability: String,
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

/// Whether a sandbox task wires its guest NIC to a host TAP device.
fn sandbox_uses_tap(task: &TaskConfig) -> bool {
    task.runtime == "sandbox"
        && task
            .config
            .get("network")
            .and_then(|network| network.get("mode"))
            .and_then(Value::as_str)
            == Some("tap")
}

/// Why `instance` cannot be forked with a `kind` checkpoint on `node`, as
/// the FailedPrecondition message. `None` when it can.
///
/// A memory fork needs what a memory suspend needs (a sandbox launched for
/// memory checkpoints on a real VMM, without gated networking). Either kind
/// needs the instance volume's driver to `snapshot` and `clone` it, so each
/// fork gets its own copy, and a disk fork needs a volume to copy at all. A
/// memory fork of a TAP-wired Firecracker sandbox also needs a Firecracker
/// that can move a restored NIC to another TAP device.
pub(crate) fn fork_precondition(
    kind: SnapshotKind,
    task: &TaskConfig,
    instance: &Instance,
    allocation: &crate::structs::Allocation,
    node: &Node,
) -> Option<String> {
    let id = &instance.instance_id;
    if kind == SnapshotKind::Memory {
        if let Some(reason) = suspend_memory_precondition(task, instance, allocation, node) {
            return Some(reason);
        }
    }
    let driver = instance
        .storage_handle
        .as_deref()
        .and_then(|handle| handle.split_once('/'))
        .map(|(driver, _)| driver.to_string());
    match driver.as_deref() {
        None if kind == SnapshotKind::Disk => {
            return Some(format!(
                "instance {id} has no stateful.storage volume, so a disk fork has nothing to \
                 copy; runtime {:?} can fork only through a volume whose driver supports \
                 snapshot and clone\nHint: declare stateful.storage on task {:?}, or run it as \
                 a sandbox launched for memory checkpoints",
                task.runtime, task.name
            ))
        }
        None => {}
        Some(driver) => {
            for op in [
                crate::volumes::VolumeOp::Snapshot,
                crate::volumes::VolumeOp::Clone,
            ] {
                if !node_driver_supports(node, driver, op.as_str()) {
                    return Some(format!(
                        "instance {id} stores its data on volume driver `{driver}`, which does \
                         not support `{}` on node {}, so its forks could not get their own copy \
                         of the volume\nHint: use a volume driver that supports snapshot and \
                         clone (local-dir does)",
                        op.as_str(),
                        node.id
                    ));
                }
            }
        }
    }
    // A disk fork boots each fork fresh. On `tap = "auto"` that is the
    // fork's own `lgf` device, but a fixed TAP is the one the origin holds.
    // Submit refuses a fixed TAP on a keyed task, so only a spec stored
    // before that rule reaches this.
    if let Some(tap) = crate::structs::fork::sandbox_fixed_tap(task) {
        if kind == SnapshotKind::Disk {
            return Some(format!(
                "instance {id} wires its guest to the fixed TAP device {tap:?} \
                 (config.network.tap), and a disk fork boots each fork fresh on that device, \
                 which the origin already holds on node {}\nHint: use --checkpoint memory, \
                 which moves each fork to its own TAP device, or resubmit the job with tap = \
                 \"auto\"",
                node.id
            ));
        }
    }
    if kind == SnapshotKind::Memory && sandbox_uses_tap(task) {
        let vmm = node
            .fingerprint_attrs
            .get(crate::structs::checkpoint::ATTR_SANDBOX_VMM)
            .and_then(Value::as_str)
            .unwrap_or_default();
        let version = node
            .fingerprint_attrs
            .get(crate::structs::checkpoint::ATTR_SANDBOX_VMM_VERSION)
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        if vmm == "firecracker" && !crate::structs::fork::firecracker_can_override_tap(version) {
            return Some(format!(
                "instance {id} wires its guest to a TAP device, and node {} runs Firecracker \
                 {version}, which cannot move a restored NIC to a new TAP device (that needs \
                 Firecracker 1.12 or later), so every fork would claim the origin's TAP\nHint: \
                 upgrade Firecracker on the node, or use --checkpoint disk",
                node.id
            ));
        }
    }
    None
}

/// Why `instance` cannot be forked with `--spread` on `node`, as the
/// FailedPrecondition message. `None` when it can.
///
/// A spread fork's forks restore on other nodes from a published
/// checkpoint, so the checkpoint must be all content-store parts (a memory
/// checkpoint of an instance with no volume: a fork clones the volume on
/// the origin's node, which no other node can reach), and the origin's
/// worker must have a `contentStore` to publish to.
pub(crate) fn spread_precondition(
    kind: SnapshotKind,
    task: &TaskConfig,
    instance: &Instance,
    node: &Node,
) -> Option<String> {
    let id = &instance.instance_id;
    if kind != SnapshotKind::Memory {
        return Some(format!(
            "--spread places forks on other nodes, which restore from a published checkpoint, \
             and a disk checkpoint is a copy of instance {id}'s volume on node {} that cannot be \
             published\nHint: drop --checkpoint disk (a spread fork takes a memory \
             checkpoint), or drop --spread",
            node.id
        ));
    }
    if let Some(driver) = instance
        .storage_handle
        .as_deref()
        .and_then(|handle| handle.split_once('/'))
        .map(|(driver, _)| driver)
    {
        return Some(format!(
            "--spread places forks on other nodes, but instance {id} keeps its data on a \
             `{driver}` volume (stateful.storage of task {:?}), and each fork's copy is cloned \
             on the origin's node {}, so a fork on another node could not get it\nHint: drop \
             --spread (forks run on the origin's node), or drop stateful.storage so the whole \
             instance is in its memory checkpoint",
            task.name, node.id
        ));
    }
    if !node_has_capability(node, CONTENT_PUBLISH_CAPABILITY) {
        return Some(format!(
            "--spread publishes the fork checkpoint so forks can restore on other nodes, but \
             instance {id} runs on node {}, whose worker has no contentStore to publish to (it \
             does not advertise the `{CONTENT_PUBLISH_CAPABILITY}` capability)\nHint: add a \
             contentStore {{ url }} block to that worker's config (docs/artifacts.md, \
             \"Publishing content\"), or drop --spread",
            node.id
        ));
    }
    None
}

impl ManagerDaemonService {
    #[instrument(skip_all, fields(instance_id = %request.get_ref().instance_id))]
    pub(super) async fn fork_instance_rpc(
        &self,
        request: Request<ForkInstanceRequest>,
    ) -> Result<Response<ForkInstanceReply>, Status> {
        // Coarse first, both grants, so a caller with neither cannot probe
        // which instances exist.
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::any(),
        )?;
        acl::enforce(
            &identity,
            AclResource::Snapshot,
            AclAction::Write,
            &AclScope::any(),
        )?;
        let inner = request.into_inner();
        let count = inner.count.max(1);
        if count > MAX_FORK_COUNT {
            return Err(Status::invalid_argument(format!(
                "count {count} is above the limit of {MAX_FORK_COUNT} forks per call\nHint: \
                 fork in smaller batches"
            )));
        }
        let requested = inner
            .checkpoint
            .as_deref()
            .map(str::trim)
            .filter(|raw| !raw.is_empty())
            .map(|raw| match raw {
                "memory" => Ok(SnapshotKind::Memory),
                "disk" => Ok(SnapshotKind::Disk),
                other => Err(Status::invalid_argument(format!(
                    "checkpoint {other:?} is not valid (expected memory or disk)"
                ))),
            })
            .transpose()?;
        let idempotency_key =
            scoped_idempotency_key(&identity, "fork_instance", inner.idempotency_key.as_deref());
        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let recorded: RecordedFork = serde_json::from_str(&recorded).unwrap_or_default();
            let pause_ms = self
                .store
                .get_snapshot(&recorded.snapshot)
                .ok()
                .flatten()
                .map(|snapshot| snapshot.pause_ms)
                .unwrap_or_default();
            return Ok(Response::new(ForkInstanceReply {
                origin: recorded.origin,
                snapshot: recorded.snapshot,
                pause_ms,
                instances: recorded.instances,
                kind: recorded.kind,
                node_id: recorded.node_id,
                deduplicated: true,
                durability: if recorded.durability.is_empty() {
                    SnapshotDurability::Local.as_str().to_string()
                } else {
                    recorded.durability
                },
            }));
        }

        let instance = self
            .store
            .get_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?
            .ok_or_else(|| {
                Status::not_found(format!("instance {} not found", inner.instance_id))
            })?;
        for resource in [AclResource::Job, AclResource::Snapshot] {
            acl::enforce(
                &identity,
                resource,
                AclAction::Write,
                &AclScope::job(&instance.job_id),
            )?;
        }
        if instance.mode != InstanceMode::Keyed {
            return Err(Status::invalid_argument(format!(
                "instance {} is slot-mode; fork applies to keyed instances \
                 (stateful.mode = \"keyed\")",
                instance.instance_id
            )));
        }
        let job = self
            .store
            .get_job(&instance.job_id)
            .map_err(|e| map_store_error("Error loading job", e))?;
        if job.status == "stopped" {
            return Err(Status::failed_precondition(format!(
                "job {} is stopped; run the job first, then fork its instances",
                instance.job_id
            )));
        }
        let suspend_state = self
            .store
            .instance_suspend_state(&instance.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?;
        if let Some(pending) = suspend_state.pending_op.as_deref() {
            return Err(Status::failed_precondition(format!(
                "instance {} has a {pending} in flight; fork it once that finishes",
                instance.instance_id
            )));
        }
        let allocation = match (instance.state, instance.current_alloc_id) {
            (InstanceState::Active, Some(allocation_id)) => self
                .store
                .get_allocation_by_id(allocation_id)
                .map_err(|e| map_store_error("Error loading allocation", e))?,
            _ => None,
        }
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "instance {} has no running allocation (it is {}); only an active instance can \
                 be forked{}",
                instance.instance_id,
                instance.state,
                if instance.state == InstanceState::Suspended {
                    "\nHint: start it first, or create instances from a snapshot with \
                     `legion instance create --from-snapshot`"
                } else {
                    ""
                }
            ))
        })?;
        let node = self
            .store
            .get_node(&allocation.node_id)
            .map_err(|e| map_store_error("Error loading node", e))?;
        if !node_has_capability(&node, CHECKPOINT_CAPABILITY) {
            return Err(Status::failed_precondition(format!(
                "node {} runs worker version {}, which cannot fork instances (it does not \
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
        // Memory when the origin can take one, else disk. A spread fork is
        // always memory: only content-store parts can be published.
        let kind = requested.unwrap_or_else(|| {
            if inner.spread {
                return SnapshotKind::Memory;
            }
            if fork_precondition(SnapshotKind::Memory, &task, &instance, &allocation, &node)
                .is_none()
            {
                SnapshotKind::Memory
            } else {
                SnapshotKind::Disk
            }
        });
        // Spread's own refusals first: their hints fit a spread request.
        if inner.spread {
            if let Some(reason) = spread_precondition(kind, &task, &instance, &node) {
                return Err(Status::failed_precondition(reason));
            }
        }
        if let Some(reason) = fork_precondition(kind, &task, &instance, &allocation, &node) {
            return Err(Status::failed_precondition(reason));
        }
        // A spread fork's checkpoint is published so its forks can restore
        // on other nodes; every other fork checkpoint stays on the origin's
        // node, where all its forks run and where it is spent.
        let durability = if inner.spread {
            SnapshotDurability::Store
        } else {
            SnapshotDurability::Local
        };

        let new = NewSnapshot {
            snapshot_id: crate::structs::snapshot::new_snapshot_id(),
            job_id: instance.job_id.clone(),
            task_name: task.name.clone(),
            instance_id: instance.instance_id.clone(),
            name: None,
            kind,
            purpose: SnapshotPurpose::Fork,
            node_id: node.id.clone(),
            allocation_id: allocation.id,
            // Spent once its forks restore: never subject to ttl.
            expires_at: None,
            suspend_reason: None,
            durability,
        };
        let require_id = task
            .stateful
            .as_ref()
            .is_some_and(|stateful| stateful.require_id);
        // One scheduler-lane entry for the fork and its idempotency record,
        // group-committed with other instance writes (sandbox-scaling plan,
        // Phase 1).
        let origin = instance.instance_id.clone();
        let node_id = node.id.clone();
        let key = idempotency_key.clone();
        let recorded = self
            .store
            .scheduler_group_commit("fork_instance", move |s| {
                let created = s.begin_fork(new, count, require_id)?;
                let recorded = RecordedFork {
                    origin,
                    snapshot: created.snapshot.snapshot_id.clone(),
                    kind: kind.as_str().to_string(),
                    node_id,
                    instances: created.instances,
                    durability: durability.as_str().to_string(),
                };
                s.idempotency_record(
                    &key,
                    "fork_instance",
                    &serde_json::to_string(&recorded).unwrap_or_default(),
                )?;
                Ok(recorded)
            })
            .map_err(|e| map_store_error("Error forking instance", e))?;
        Ok(Response::new(ForkInstanceReply {
            origin: recorded.origin,
            snapshot: recorded.snapshot,
            pause_ms: 0,
            instances: recorded.instances,
            kind: recorded.kind,
            node_id: recorded.node_id,
            deduplicated: false,
            durability: recorded.durability,
        }))
    }

    /// Resolve and check the snapshot a `from_snapshot` create restores
    /// from: `snapshot:read` on the job, a snapshot of this task, usable as
    /// a seed, at most [`MAX_FORK_COUNT`] instances, and no memory snapshot
    /// into a task that now declares gated networking.
    pub(super) fn resolve_seed_snapshot(
        &self,
        identity: &AclIdentity,
        job_id: &str,
        task: &TaskConfig,
        from: &str,
        count: usize,
    ) -> Result<SnapshotRecord, Status> {
        acl::enforce(
            identity,
            AclResource::Snapshot,
            AclAction::Read,
            &AclScope::job(job_id),
        )?;
        if count > MAX_FORK_COUNT as usize {
            return Err(Status::invalid_argument(format!(
                "{count} instances is above the limit of {MAX_FORK_COUNT} per create from a \
                 snapshot\nHint: create them in smaller batches"
            )));
        }
        let snapshot = self
            .store
            .resolve_snapshot(from, Some(job_id))
            .map_err(|e| map_store_error("Error loading snapshot", e))?
            .filter(|snapshot| snapshot.job_id == job_id)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "no snapshot {from:?} in job {job_id}\nHint: `legion snapshot list --job \
                     {job_id}` shows its snapshots and names"
                ))
            })?;
        if snapshot.task_name != task.name {
            return Err(Status::invalid_argument(format!(
                "snapshot {} was taken from task {:?}, not {:?}; a snapshot seeds instances of \
                 the task it came from",
                snapshot.snapshot_id, snapshot.task_name, task.name
            )));
        }
        if let Some(reason) = crate::storage::store::alias_target_problem(&snapshot) {
            return Err(Status::failed_precondition(reason));
        }
        if snapshot.kind == SnapshotKind::Memory
            && crate::structs::snapshot::task_declares_gated_network(task)
        {
            return Err(Status::failed_precondition(format!(
                "snapshot {} is a memory snapshot, and task {:?} now declares gated networking, \
                 which sandbox memory checkpoints don't support yet\nHint: use a disk snapshot, \
                 or drop network gating for this task",
                snapshot.snapshot_id, task.name
            )));
        }
        Ok(snapshot)
    }

    #[instrument(skip_all, fields(job_id = %request.get_ref().job_id, name = %request.get_ref().name))]
    pub(super) async fn set_snapshot_alias_rpc(
        &self,
        request: Request<SetSnapshotAliasRequest>,
    ) -> Result<Response<SetSnapshotAliasReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Snapshot,
            AclAction::Write,
            AclScope::job(&request.get_ref().job_id),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "set_snapshot_alias",
            inner.idempotency_key.as_deref(),
        );
        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let mut reply: SetSnapshotAliasReply = serde_json::from_str::<RecordedAlias>(&recorded)
                .unwrap_or_default()
                .into();
            reply.deduplicated = true;
            return Ok(Response::new(reply));
        }
        let name = inner.name.trim().to_string();
        crate::structs::snapshot::validate_snapshot_name(&name)
            .map_err(Status::invalid_argument)?;
        let snapshot = self
            .store
            .resolve_snapshot(inner.snapshot.trim(), Some(&inner.job_id))
            .map_err(|e| map_store_error("Error loading snapshot", e))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "no snapshot {:?} in job {}",
                    inner.snapshot, inner.job_id
                ))
            })?;
        let outcome = self
            .store
            .set_snapshot_alias(&inner.job_id, &name, &snapshot.snapshot_id)
            .map_err(|e| map_store_error("Error naming snapshot", e))?;
        let message = match (outcome.changed, outcome.previous.as_deref()) {
            (false, _) => format!("{name} already points at {}", snapshot.snapshot_id),
            (true, Some(previous)) => {
                format!("Moved {name} from {previous} to {}", snapshot.snapshot_id)
            }
            (true, None) => format!("Pointed {name} at {}", snapshot.snapshot_id),
        };
        info!(
            job_id = %inner.job_id,
            name,
            snapshot_id = %snapshot.snapshot_id,
            previous = outcome.previous.as_deref().unwrap_or(""),
            "Snapshot name set"
        );
        let recorded = RecordedAlias {
            message,
            snapshot_id: snapshot.snapshot_id,
            previous_snapshot_id: outcome.previous.unwrap_or_default(),
        };
        self.store
            .idempotency_record(
                &idempotency_key,
                "set_snapshot_alias",
                &serde_json::to_string(&recorded).unwrap_or_default(),
            )
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(recorded.into()))
    }

    #[instrument(skip_all, fields(job_id = %request.get_ref().job_id, name = %request.get_ref().name))]
    pub(super) async fn delete_snapshot_alias_rpc(
        &self,
        request: Request<DeleteSnapshotAliasRequest>,
    ) -> Result<Response<DeleteSnapshotAliasReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Snapshot,
            AclAction::Write,
            AclScope::job(&request.get_ref().job_id),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "delete_snapshot_alias",
            inner.idempotency_key.as_deref(),
        );
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(DeleteSnapshotAliasReply {
                message,
                deduplicated: true,
            }));
        }
        let name = inner.name.trim();
        let removed = self
            .store
            .delete_snapshot_alias(&inner.job_id, name)
            .map_err(|e| map_store_error("Error removing snapshot name", e))?;
        let message = match removed {
            Some(snapshot_id) => format!("Removed {name} (it pointed at {snapshot_id})"),
            None => format!("No snapshot name {name} in job {}", inner.job_id),
        };
        self.store
            .idempotency_record(&idempotency_key, "delete_snapshot_alias", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(DeleteSnapshotAliasReply {
            message,
            deduplicated: false,
        }))
    }
}

/// What a SetSnapshotAlias reply carries, recorded under its idempotency key.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RecordedAlias {
    message: String,
    snapshot_id: String,
    previous_snapshot_id: String,
}

impl From<RecordedAlias> for SetSnapshotAliasReply {
    fn from(recorded: RecordedAlias) -> Self {
        SetSnapshotAliasReply {
            message: recorded.message,
            snapshot_id: recorded.snapshot_id,
            previous_snapshot_id: recorded.previous_snapshot_id,
            deduplicated: false,
        }
    }
}
