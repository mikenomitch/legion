//! SuspendInstance, and resume through StartInstance (suspend plan §6.6;
//! docs/suspend-and-resume.md).
//!
//! A suspend is a snapshot with `purpose = suspend`: this module inserts the
//! row and marks the instance (`Store::begin_suspend`), the checkpoint
//! channel in `snapshot_rpcs` sends the node `then = stop`, and the node's
//! report applies the suspend (`Store::apply_suspend_capture_tx`). A resume
//! sets the instance's restore source to its checkpoint
//! (`Store::resume_suspended_instance`), and the existing restore path does
//! the rest.

use serde::{Deserialize, Serialize};

use super::snapshot_rpcs::{node_build_version, node_has_capability};
use super::*;
use crate::storage::store::{NewSnapshot, ResumeOutcome, SnapshotFilter};
use crate::structs::checkpoint::SnapshotKind;
use crate::structs::snapshot::{SnapshotDurability, SnapshotPurpose, SnapshotState};
use crate::structs::suspend::PENDING_SUSPEND;
use crate::structs::{SuspendInstanceReply, SuspendInstanceRequest};

/// What a suspend reply carries, recorded under its idempotency key so a
/// retry returns the same answer.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RecordedSuspend {
    message: String,
    snapshot_id: String,
    kind: String,
    node_id: String,
    #[serde(default)]
    durability: String,
}

impl RecordedSuspend {
    fn into_reply(self, deduplicated: bool) -> SuspendInstanceReply {
        SuspendInstanceReply {
            message: self.message,
            snapshot_id: self.snapshot_id,
            kind: self.kind,
            node_id: self.node_id,
            deduplicated,
            durability: if self.durability.is_empty() {
                "local".to_string()
            } else {
                self.durability
            },
        }
    }
}

/// What a start reply carries for a resume, recorded the same way. Older
/// records are a bare message, which still parses (see `from_record`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct RecordedStart {
    pub message: String,
    #[serde(default)]
    pub resuming_from: String,
    #[serde(default)]
    pub node_id: String,
}

impl RecordedStart {
    pub(super) fn from_record(recorded: String) -> Self {
        serde_json::from_str(&recorded).unwrap_or(RecordedStart {
            message: recorded,
            ..Default::default()
        })
    }

    pub(super) fn into_reply(self, deduplicated: bool) -> StartInstanceReply {
        StartInstanceReply {
            message: self.message,
            deduplicated,
            resuming_from: self.resuming_from,
            node_id: self.node_id,
        }
    }
}

/// Why `task`'s running allocation cannot take a memory checkpoint for a
/// suspend on `node`, as the FailedPrecondition message. `None` when it can.
/// Unlike a memory snapshot, a suspend leaves the volume in place, so the
/// volume driver needs no `snapshot` op.
pub(super) fn suspend_memory_precondition(
    task: &TaskConfig,
    instance: &Instance,
    allocation: &crate::structs::Allocation,
    node: &Node,
) -> Option<String> {
    let subject = format!("instance {}", instance.instance_id);
    if task.runtime != "sandbox" {
        return Some(crate::structs::suspend::disk_only_runtime_error(
            &subject,
            &task.runtime,
            false,
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
            "{subject} cannot take a memory checkpoint: its sandbox runs on node {} without \
             Firecracker or cloud-hypervisor (it resolved {:?})\nHint: use --checkpoint disk",
            node.id,
            node_vmm.unwrap_or("no VMM")
        ));
    }
    if let Some(refusal) =
        gated_capture_refusal(task, allocation, node, &subject, "use --checkpoint disk")
    {
        return Some(refusal);
    }
    if crate::structs::snapshot::memory_checkpoint_ask(task).is_none() {
        return Some(format!(
            "{subject} cannot take a memory checkpoint: task {:?} was not launched for memory \
             checkpoints\nHint: add a suspend block to the task (or a snapshots block) and \
             restart the instance, or use --checkpoint disk",
            task.name
        ));
    }
    None
}

/// The VMM a sandbox task runs on `node`, and that VMM's version there: the
/// task's own `hypervisor` when it names a VMM, else the node's default
/// (`sandbox.vmm`). The version is the node's per-VMM attribute
/// (`sandbox.vmm.<name>.version`), falling back to `sandbox.vmm.version`
/// for the default.
pub(super) fn sandbox_vmm_on_node(task: &TaskConfig, node: &Node) -> (String, Option<String>) {
    use crate::structs::checkpoint::{
        vmm_version_attr, ATTR_SANDBOX_VMM, ATTR_SANDBOX_VMM_VERSION,
    };
    let attr = |key: &str| {
        node.fingerprint_attrs
            .get(key)
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    let default = attr(ATTR_SANDBOX_VMM).unwrap_or_default();
    let vmm = match task
        .config
        .get("hypervisor")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("firecracker" | "fc") => "firecracker".to_string(),
        Some("cloud-hypervisor" | "cloudhypervisor" | "ch") => "cloud-hypervisor".to_string(),
        _ => default.clone(),
    };
    let version = attr(&vmm_version_attr(&vmm)).or_else(|| {
        (vmm == default)
            .then(|| attr(ATTR_SANDBOX_VMM_VERSION))
            .flatten()
    });
    (vmm, version)
}

/// Why a memory capture of a gated sandbox's `allocation` would produce a
/// checkpoint its node's VMM cannot restore: a Firecracker older than
/// 1.16.1 cannot move the guest's vsock to the restoring allocation's
/// socket (gated plan §6.2). Compat is an exact VMM version match, so this
/// check at capture covers every later restore. `None` when it can.
pub(super) fn gated_capture_refusal(
    task: &TaskConfig,
    allocation: &crate::structs::Allocation,
    node: &Node,
    subject: &str,
    fix: &str,
) -> Option<String> {
    if !crate::task_plugins::allocation_is_gated(allocation) {
        return None;
    }
    let (vmm, version) = sandbox_vmm_on_node(task, node);
    crate::structs::fork::gated_memory_checkpoint_refusal(
        subject,
        &node.id,
        &vmm,
        version.as_deref(),
        fix,
    )
}

/// The note a live memory capture of `allocation` deserves, for the
/// `notes` of CreateSnapshot and ForkInstance: Firecracker resets every open
/// vsock connection at snapshot time, so a gated guest that keeps running
/// loses its open connections (gated plan §6.3, calls entry 13).
pub(super) fn gated_capture_note(
    task: &TaskConfig,
    allocation: &crate::structs::Allocation,
    node: &Node,
    instance_id: &str,
) -> Option<String> {
    if !crate::task_plugins::allocation_is_gated(allocation) {
        return None;
    }
    let (vmm, _) = sandbox_vmm_on_node(task, node);
    (vmm == "firecracker").then(|| {
        format!(
            "instance {instance_id} is gated on Firecracker, so this capture resets its open \
             network connections (the guest keeps running)"
        )
    })
}

/// The checkpoint kind a suspend takes when the caller does not say: the
/// task's `suspend.checkpoint`, else memory when the sandbox was launched for
/// memory checkpoints and its node can take one, else disk.
pub(super) fn default_suspend_kind(
    task: &TaskConfig,
    instance: &Instance,
    allocation: &crate::structs::Allocation,
    node: &Node,
) -> SnapshotKind {
    let declared = task
        .suspend
        .as_ref()
        .and_then(|suspend| suspend.resolve().ok())
        .and_then(|resolved| resolved.checkpoint);
    if let Some(kind) = declared {
        return kind;
    }
    if suspend_memory_precondition(task, instance, allocation, node).is_none() {
        SnapshotKind::Memory
    } else {
        SnapshotKind::Disk
    }
}

impl ManagerDaemonService {
    #[instrument(skip_all, fields(instance_id = %request.get_ref().instance_id))]
    pub(super) async fn suspend_instance_rpc(
        &self,
        request: Request<SuspendInstanceRequest>,
    ) -> Result<Response<SuspendInstanceReply>, Status> {
        // Coarse first, so a caller with no grant cannot probe which
        // instances exist. An explicit memory checkpoint also needs
        // `snapshot:write` before anything is looked up.
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
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
        if requested == Some(SnapshotKind::Memory) {
            acl::enforce(
                &identity,
                AclResource::Snapshot,
                AclAction::Write,
                &AclScope::any(),
            )?;
        }
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "suspend_instance",
            inner.idempotency_key.as_deref(),
        );
        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let recorded: RecordedSuspend = serde_json::from_str(&recorded).unwrap_or_default();
            return Ok(Response::new(recorded.into_reply(true)));
        }

        let instance = self
            .store
            .get_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?
            .ok_or_else(|| {
                Status::not_found(format!("instance {} not found", inner.instance_id))
            })?;
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job(&instance.job_id),
        )?;
        if instance.mode != InstanceMode::Keyed {
            return Err(Status::invalid_argument(format!(
                "instance {} is slot-mode; suspend applies to keyed instances \
                 (stateful.mode = \"keyed\")",
                instance.instance_id
            )));
        }
        let suspend_state = self
            .store
            .instance_suspend_state(&instance.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?;

        // Already there, or on the way: name the checkpoint and change nothing.
        let settled = if instance.state == InstanceState::Suspended {
            let snapshot_id = suspend_state.checkpoint_id.clone().unwrap_or_default();
            Some(self.recorded_for_existing(
                &snapshot_id,
                format!(
                    "Instance {} already suspended ({snapshot_id})",
                    instance.instance_id
                ),
            ))
        } else if suspend_state.pending_op.as_deref() == Some(PENDING_SUSPEND) {
            let capturing = self
                .store
                .list_snapshots(&SnapshotFilter {
                    job_id: Some(instance.job_id.clone()),
                    instance_id: Some(instance.instance_id.clone()),
                    state: Some(SnapshotState::Capturing),
                })
                .map_err(|e| map_store_error("Error loading snapshots", e))?
                .into_iter()
                .find(|snapshot| snapshot.purpose == SnapshotPurpose::Suspend)
                .map(|snapshot| snapshot.snapshot_id)
                .unwrap_or_default();
            Some(self.recorded_for_existing(
                &capturing,
                format!(
                    "Instance {} is already being suspended ({capturing})",
                    instance.instance_id
                ),
            ))
        } else {
            None
        };
        if let Some(recorded) = settled {
            return Ok(Response::new(recorded.into_reply(false)));
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
                 be suspended",
                instance.instance_id, instance.state
            ))
        })?;
        let node = self
            .store
            .get_node(&allocation.node_id)
            .map_err(|e| map_store_error("Error loading node", e))?;
        if !node_has_capability(&node, CHECKPOINT_CAPABILITY) {
            return Err(Status::failed_precondition(format!(
                "node {} runs worker version {}, which cannot suspend instances (it does not \
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
        let kind =
            requested.unwrap_or_else(|| default_suspend_kind(&task, &instance, &allocation, &node));
        // `durability = "store"` (Phase 7b): refuse here, with the instance
        // still running, anything that could not be published afterwards.
        let durability = task
            .suspend
            .as_ref()
            .and_then(|suspend| suspend.resolve().ok())
            .map(|resolved| resolved.durability)
            .unwrap_or_default();
        if durability == SnapshotDurability::Store {
            if let Some(reason) = super::snapshot_rpcs::publish_precondition(
                &task,
                &instance,
                &node,
                "suspend.durability",
            ) {
                return Err(Status::failed_precondition(reason));
            }
        }
        if kind == SnapshotKind::Memory {
            acl::enforce(
                &identity,
                AclResource::Snapshot,
                AclAction::Write,
                &AclScope::job(&instance.job_id),
            )?;
            if let Some(reason) = suspend_memory_precondition(&task, &instance, &allocation, &node)
            {
                return Err(Status::failed_precondition(reason));
            }
        }

        let new = NewSnapshot {
            snapshot_id: crate::structs::snapshot::new_snapshot_id(),
            job_id: instance.job_id.clone(),
            task_name: task.name.clone(),
            instance_id: instance.instance_id.clone(),
            name: None,
            kind,
            purpose: SnapshotPurpose::Suspend,
            node_id: node.id.clone(),
            allocation_id: allocation.id,
            // Owned by the instance: never subject to snapshots.ttl.
            expires_at: None,
            suspend_reason: Some(crate::structs::suspend::SUSPEND_REASON_MANUAL.to_string()),
            durability,
        };
        let recorded = RecordedSuspend {
            message: format!(
                "Suspending instance {} ({} checkpoint {} on {})",
                instance.instance_id,
                kind.as_str(),
                new.snapshot_id,
                node.id
            ),
            snapshot_id: new.snapshot_id.clone(),
            kind: kind.as_str().to_string(),
            node_id: node.id.clone(),
            durability: durability.as_str().to_string(),
        };
        let record = serde_json::to_string(&recorded).unwrap_or_default();
        // One scheduler-lane entry for the suspend and its idempotency
        // record, group-committed with other instance writes
        // (sandbox-scaling plan, Phase 1). A manager that does not lead the
        // scheduler lane falls back to the forwarding pair it always used.
        let fused = {
            let new = new.clone();
            let key = idempotency_key.clone();
            let record = record.clone();
            self.store
                .scheduler_group_commit("suspend_instance", move |s| {
                    let snapshot = s.begin_suspend_local(new)?;
                    s.idempotency_record(&key, "suspend_instance", &record)?;
                    Ok(snapshot)
                })
        };
        let snapshot = match fused {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => {
                let snapshot = self
                    .store
                    .begin_suspend(new)
                    .map_err(|e| map_store_error("Error suspending instance", e))?;
                self.store
                    .idempotency_record(&idempotency_key, "suspend_instance", &record)
                    .map_err(|e| map_store_error("Error recording idempotency", e))?;
                snapshot
            }
            other => other.map_err(|e| map_store_error("Error suspending instance", e))?,
        };
        info!(
            instance_id = %instance.instance_id,
            snapshot_id = %snapshot.snapshot_id,
            node_id = %node.id,
            kind = kind.as_str(),
            durability = durability.as_str(),
            "Suspend requested"
        );
        Ok(Response::new(recorded.into_reply(false)))
    }

    fn recorded_for_existing(&self, snapshot_id: &str, message: String) -> RecordedSuspend {
        let snapshot = self.store.get_snapshot(snapshot_id).ok().flatten();
        RecordedSuspend {
            message,
            snapshot_id: snapshot_id.to_string(),
            kind: snapshot
                .as_ref()
                .map(|snapshot| snapshot.kind.as_str().to_string())
                .unwrap_or_default(),
            durability: snapshot
                .as_ref()
                .map(|snapshot| snapshot.durability.as_str().to_string())
                .unwrap_or_default(),
            node_id: snapshot
                .and_then(|snapshot| snapshot.node_id)
                .unwrap_or_default(),
        }
    }

    /// StartInstance on a `suspended` instance: restore from its checkpoint,
    /// or boot cold. Returns the reply it recorded.
    ///
    /// One scheduler-lane entry, group-committed with other instance writes
    /// (sandbox-scaling plan, Phase 1): the resume itself, an
    /// `instance.restore_failed` event when the checkpoint is discarded, the
    /// placement eval, and the idempotency record. A resume that cannot
    /// restore (`RestoreUnavailable`) commits its `lifecycle_error` and
    /// nothing else, and is refused without a record so a retry can succeed
    /// once the cause is fixed.
    ///
    /// `archived` is what the handler read back from an archived instance's
    /// archive manifest (docs/suspend-and-resume.md, "Archived instances").
    pub(super) fn resume_instance(
        &self,
        instance: &Instance,
        cold: bool,
        archived: Option<crate::storage::store::ArchivedCheckpoint>,
        idempotency_key: &str,
    ) -> Result<RecordedStart, Status> {
        let instance_id = instance.instance_id.clone();
        self.resume_with(
            &instance.job_id,
            &instance.instance_id,
            idempotency_key,
            move |s| s.resume_archived_instance(&instance_id, cold, archived),
        )
    }

    /// StartInstance on an instance archived with `archive =
    /// "manifestOnly"`, which has no row: the row comes back from
    /// `manifest`, and the resume runs as for any archived instance, in the
    /// same one entry.
    pub(super) fn resume_manifest_only(
        &self,
        manifest: crate::structs::archive::ArchiveManifest,
        manifest_url: &str,
        cold: bool,
        idempotency_key: &str,
    ) -> Result<RecordedStart, Status> {
        let job_id = manifest.envelope.job_id.clone();
        let instance_id = manifest.envelope.instance_id.clone();
        let url = manifest_url.to_string();
        self.resume_with(&job_id, &instance_id, idempotency_key, move |s| {
            s.resume_manifest_only_instance(manifest, &url, cold)
        })
    }

    fn resume_with(
        &self,
        job_id: &str,
        instance_id: &str,
        idempotency_key: &str,
        resume: impl FnOnce(&Store) -> R<ResumeOutcome> + Send + 'static,
    ) -> Result<RecordedStart, Status> {
        let config_id = self
            .store
            .get_job_config(job_id)
            .map_err(|e| map_store_error("Error loading job config", e))?
            .id();
        let shown_id = instance_id.to_string();
        let instance_id = instance_id.to_string();
        let job_id = job_id.to_string();
        let priority = self.store.default_eval_priority;
        let key = idempotency_key.to_string();
        let resumed = self
            .store
            .scheduler_group_commit("resume_instance", move |s| {
                let recorded = match resume(s)? {
                    ResumeOutcome::RestoreUnavailable { reason } => return Ok(Err(reason)),
                    ResumeOutcome::Resuming {
                        snapshot_id,
                        node_id,
                    } => RecordedStart {
                        message: match node_id.as_deref() {
                            Some(node_id) => format!(
                                "Resuming instance {instance_id} from {snapshot_id} on {node_id}"
                            ),
                            None => format!(
                                "Resuming instance {instance_id} from published {snapshot_id} on \
                                 any compatible node"
                            ),
                        },
                        resuming_from: snapshot_id,
                        node_id: node_id.unwrap_or_default(),
                    },
                    ResumeOutcome::Cold { reason, discarded } => {
                        if let (Some(reason), Some(snapshot_id)) =
                            (reason.as_deref(), discarded.as_deref())
                        {
                            s.events().instance_restore_failed(
                                snapshot_id,
                                &job_id,
                                &instance_id,
                                None,
                                None,
                                reason,
                                true,
                            );
                        }
                        RecordedStart {
                            message: match reason {
                                Some(reason) => {
                                    format!("Started instance {instance_id} cold: {reason}")
                                }
                                None => format!(
                                    "Started instance {instance_id} cold (discarded checkpoint {})",
                                    discarded.as_deref().unwrap_or("-")
                                ),
                            },
                            ..Default::default()
                        }
                    }
                };
                s.enqueue_eval(
                    "job_reschedule",
                    Some(&job_id),
                    Some(&config_id),
                    None,
                    Some(priority),
                )?;
                s.idempotency_record(
                    &key,
                    "start_instance",
                    &serde_json::to_string(&recorded).unwrap_or_default(),
                )?;
                Ok(Ok(recorded))
            })
            .map_err(|e| map_store_error("Error resuming instance", e))?;
        resumed.map_err(|reason| {
            Status::failed_precondition(format!(
                "instance {shown_id} cannot resume: {reason}\nHint: `legion instance start \
                 {shown_id} --cold` boots it fresh on its retained storage"
            ))
        })
    }
}

/// The resume sweep, run by the manager daemon's maintenance loop beside
/// [`super::expire_snapshots`]: settle resumes whose checkpoint was lost or
/// that waited out `suspend.resumeTimeout` (see
/// `Store::settle_stalled_resumes`), emit `instance.restore_failed`, and
/// queue placement for the ones that now boot cold.
pub fn settle_stalled_resumes(store: &Store) {
    let abandoned = match store.settle_stalled_resumes(chrono::Utc::now().timestamp()) {
        Ok(abandoned) => abandoned,
        Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => return,
        Err(err) => {
            warn!(error = ?err, "Failed settling stalled resumes");
            return;
        }
    };
    for resume in abandoned {
        warn!(
            instance_id = %resume.instance_id,
            snapshot_id = %resume.snapshot_id,
            cold = resume.cold,
            reason = %resume.reason,
            "Resume abandoned its checkpoint"
        );
        store.events().instance_restore_failed(
            &resume.snapshot_id,
            &resume.job_id,
            &resume.instance_id,
            None,
            None,
            &resume.reason,
            resume.cold,
        );
        if resume.cold {
            if let Ok(config) = store.get_job_config(&resume.job_id) {
                if let Err(err) = store.enqueue_eval(
                    "job_reschedule",
                    Some(&resume.job_id),
                    Some(&config.id()),
                    None,
                    Some(store.default_eval_priority),
                ) {
                    warn!(error = ?err, job_id = %resume.job_id, "Failed queueing a cold resume");
                }
            }
        }
    }
}

/// How long the idle sweep leaves an allocation alone after trying to
/// suspend it. A started suspend clears it from the next usage report within
/// a heartbeat or two, and one that was skipped (an operation in flight, a
/// node that cannot checkpoint) should not cost store reads every sweep.
const IDLE_RETRY_AFTER: std::time::Duration = std::time::Duration::from_secs(60);

/// The idle sweep's memory between passes: which allocations it already
/// tried, and when. In memory only, like the usage it reads.
#[derive(Debug, Default)]
pub struct IdleSweeper {
    attempted: HashMap<i64, std::time::Instant>,
}

/// What one idle sweep did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct IdleSweepOutcome {
    /// `(instance id, snapshot id)` for every suspend started.
    pub started: Vec<(String, String)>,
    /// Allocations past their window that were not suspended, with why.
    pub skipped: Vec<(i64, String)>,
}

/// The idle sweep (suspend plan §6.7, docs/suspend-and-resume.md "Idle
/// policy"), run by the manager daemon's maintenance loop every
/// `suspend.idleSweep` (default 15s).
///
/// The worker decides when an allocation went quiet and reports
/// `idle_since_unix` with its usage. This pass finds, in memory, the ones
/// quiet for at least their task's `idle.after`
/// ([`crate::manager::usage::UsageTable::idle_due`]), and starts a suspend
/// for each through [`Store::begin_suspend`] with `suspend_reason = idle`,
/// exactly as SuspendInstance would with the task's default checkpoint kind.
/// The store is read only for those allocations, never on a quiet cluster.
pub fn idle_sweep(
    store: &Store,
    usage: &Mutex<crate::manager::usage::UsageTable>,
    sweeper: &mut IdleSweeper,
) -> IdleSweepOutcome {
    let now = std::time::Instant::now();
    let due = usage
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .idle_due(chrono::Utc::now().timestamp());
    sweeper
        .attempted
        .retain(|_, at| now.duration_since(*at) < IDLE_RETRY_AFTER);
    let metrics = crate::manager::telemetry::metrics();
    let mut outcome = IdleSweepOutcome::default();
    for candidate in due {
        if sweeper.attempted.contains_key(&candidate.allocation_id) {
            continue;
        }
        sweeper.attempted.insert(candidate.allocation_id, now);
        match begin_idle_suspend(store, &candidate) {
            Ok((instance_id, snapshot_id)) => {
                info!(
                    instance_id = %instance_id,
                    snapshot_id = %snapshot_id,
                    allocation_id = candidate.allocation_id,
                    node_id = %candidate.node_id,
                    idle_seconds = chrono::Utc::now().timestamp() - candidate.idle_since_unix,
                    "Idle suspend requested"
                );
                metrics.idle_sweep_suspends_total.inc(&["started"]);
                outcome.started.push((instance_id, snapshot_id));
            }
            Err(reason) => {
                debug!(
                    allocation_id = candidate.allocation_id,
                    node_id = %candidate.node_id,
                    reason = %reason,
                    "Idle suspend skipped"
                );
                metrics.idle_sweep_suspends_total.inc(&["skipped"]);
                outcome.skipped.push((candidate.allocation_id, reason));
            }
        }
    }
    outcome
}

/// Start one idle suspend. `Err` names why it was skipped.
fn begin_idle_suspend(
    store: &Store,
    candidate: &crate::manager::usage::IdleDue,
) -> Result<(String, String), String> {
    begin_automatic_suspend(
        store,
        candidate.allocation_id,
        &candidate.node_id,
        crate::structs::suspend::SUSPEND_REASON_IDLE,
    )
}

/// Start a suspend the manager decided on itself (the idle sweep, or
/// make-room), with the task's default checkpoint kind and durability, after
/// the same checks SuspendInstance makes. `Err` names why it was skipped.
pub(super) fn begin_automatic_suspend(
    store: &Store,
    allocation_id: i64,
    node_id: &str,
    reason: &str,
) -> Result<(String, String), String> {
    let allocation = store
        .get_allocation_by_id(allocation_id)
        .map_err(|err| format!("loading allocation: {err:#}"))?
        .ok_or("the allocation is gone")?;
    if allocation.node_id != node_id {
        return Err(format!(
            "the allocation runs on {}, not on the reporting node",
            allocation.node_id
        ));
    }
    let instance_id = allocation
        .instance_id
        .clone()
        .ok_or("the allocation is not a keyed instance")?;
    let instance = store
        .get_instance(&instance_id)
        .map_err(|err| format!("loading instance: {err:#}"))?
        .ok_or("the instance is gone")?;
    if instance.mode != InstanceMode::Keyed
        || instance.state != InstanceState::Active
        || instance.current_alloc_id != Some(allocation.id)
    {
        return Err(format!("the instance is {}", instance.state));
    }
    let suspend_state = store
        .instance_suspend_state(&instance_id)
        .map_err(|err| format!("loading instance: {err:#}"))?;
    if let Some(pending) = suspend_state.pending_op {
        return Err(format!("a {pending} is already in flight"));
    }
    let node = store
        .get_node(&allocation.node_id)
        .map_err(|err| format!("loading node: {err:#}"))?;
    if !node_has_capability(&node, CHECKPOINT_CAPABILITY) {
        return Err(format!(
            "node {} runs worker version {}, which cannot suspend instances",
            node.id,
            node_build_version(&node)
        ));
    }
    let task = store
        .cached_task_configs_for_job_config(&allocation.job_config_id)
        .map_err(|err| format!("loading task config: {err:#}"))?
        .into_iter()
        .find(|(id, _)| *id == allocation.task_config_id)
        .map(|(_, task)| task)
        .ok_or("the task config is missing")?;
    let kind = default_suspend_kind(&task, &instance, &allocation, &node);
    if kind == SnapshotKind::Memory {
        if let Some(reason) = suspend_memory_precondition(&task, &instance, &allocation, &node) {
            return Err(reason);
        }
    }
    // An idle suspend publishes like a manual one, and is skipped (with the
    // reason in the sweep's log) when the checkpoint could not be published.
    let durability = task
        .suspend
        .as_ref()
        .and_then(|suspend| suspend.resolve().ok())
        .map(|resolved| resolved.durability)
        .unwrap_or_default();
    if durability == SnapshotDurability::Store {
        if let Some(reason) = super::snapshot_rpcs::publish_precondition(
            &task,
            &instance,
            &node,
            "suspend.durability",
        ) {
            return Err(reason);
        }
    }
    let snapshot = store
        .begin_suspend(NewSnapshot {
            snapshot_id: crate::structs::snapshot::new_snapshot_id(),
            job_id: instance.job_id.clone(),
            task_name: task.name.clone(),
            instance_id: instance.instance_id.clone(),
            name: None,
            kind,
            purpose: SnapshotPurpose::Suspend,
            node_id: node.id.clone(),
            allocation_id: allocation.id,
            expires_at: None,
            suspend_reason: Some(reason.to_string()),
            durability,
        })
        .map_err(|err| format!("{err:#}"))?;
    Ok((instance_id, snapshot.snapshot_id))
}
