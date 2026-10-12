// gRPC handler signatures are fixed by the generated `Manager` trait, so the
// large `tonic::Status` error variant cannot be boxed away here.
#![allow(clippy::result_large_err)]

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use crate::prelude::*;

use crate::manager::acl::{self, Authenticator};
use crate::manager::archive;
use crate::manager::dispatcher::build_task_allocation;
use crate::manager::job_templates;
use crate::manager::rollout::{
    advance_rollout as advance_rollout_state, sequential_step_fractions,
    step_fractions_for_rollout, stop_rollout as stop_rollout_state,
};
use crate::manager::rollout_strategy;
use crate::manager::scheduler::{PlanConfigOutcome, PlanMode, Scheduler};
use crate::manager::workload_identity::{claims_for_allocation, WorkloadIdentityState};
use crate::storage::errors::StorageError;
use crate::storage::events::{ArtifactPin, SchedulerEventFilter, SchedulerEventRecord};
use crate::storage::store::{
    ArchivedCheckpoint, DestroyInstanceOutcome, JobKvReference, JobSubmitOutcome,
    JobTemplateVersionRecord, StartInstanceOutcome, StopInstanceOutcome, Store,
};
use crate::structs::acl::{AclAction, AclIdentity, AclResource, AclScope};
use crate::structs::manager_server::Manager;
// KV encryption keyring surface (docs/kv-encryption.md).
use crate::structs::{
    validate_keyed_instance_id, worker_client::WorkerClient, AcquireKvLockReply,
    AcquireKvLockRequest, AdvanceRolloutReply, AdvanceRolloutRequest, AllocationHealth,
    AllocationLog, BootTicket, CreateExecKeyReply, CreateExecKeyRequest, CreateInstanceReply,
    CreateInstanceRequest, CreateInstancesReply, CreateInstancesRequest, DeleteExecKeyReply,
    DeleteExecKeyRequest, DeleteJobTemplateReply, DeleteJobTemplateRequest, DeleteKvReply,
    DeleteKvRequest, DeleteSavedStateQueryReply, DeleteSavedStateQueryRequest, DeleteVolumeReply,
    DeleteVolumeRequest, DestroyInstanceReply, DestroyInstanceRequest, DestroyJobReply,
    DestroyJobRequest, ExecClientFrame, FailedAllocation, GetAllocationLogsReply,
    GetAllocationLogsRequest, GetArtifactCredentialReply, GetArtifactCredentialRequest,
    GetHeartbeatCadenceReply, GetHeartbeatCadenceRequest, GetIngressCertificatesReply,
    GetIngressCertificatesRequest, GetInstanceReply, GetInstanceRequest, GetJobInfoReply,
    GetJobInfoRequest, GetJobScaleStatusReply, GetJobScaleStatusRequest, GetJobSpecByConfigRequest,
    GetJobSpecReply, GetJobSpecRequest, GetJobTemplateReply, GetJobTemplateRequest, GetKvLockReply,
    GetKvLockRequest, GetKvReply, GetKvRequest, GetLedgerReply, GetLedgerRequest,
    GetManagerConfigsReply, GetManagerConfigsRequest, GetMeshCaReply, GetMeshCaRequest,
    GetNodeContentReply, GetNodeContentRequest, GetNodeReply, GetNodeRequest, GetNodeUsageReply,
    GetNodeUsageRequest, GetNodesReply, GetNodesRequest, GetObservabilityReply,
    GetObservabilityRequest, GetRaftJoinTokenReply, GetRaftJoinTokenRequest, GetRaftStatusReply,
    GetRaftStatusRequest, GetRolloutReply, GetRolloutRequest, GetRolloutStatusReply,
    GetRolloutStatusRequest, GetSchedulerConcurrencyReply, GetSchedulerConcurrencyRequest,
    GetStateBackupReply, GetStateBackupRequest, GetStateStoreSchemaReply,
    GetStateStoreSchemaRequest, GetTaskLogsRequest, GetUpstreamCredentialsReply,
    GetUpstreamCredentialsRequest, GetVolumeReply, GetVolumeRequest, HealthCheckReply,
    HealthCheckRequest, HeartbeatReply, HeartbeatRequest, IngressCertificate, Instance,
    InstanceInfo, InstanceMode, InstanceState, IssueBootTicketReply, IssueBootTicketRequest,
    IssueNodeCertificateReply, IssueNodeCertificateRequest, IssueNodeIdentityTokenReply,
    IssueNodeIdentityTokenRequest, IssueRaftCommandReply, IssueRaftCommandRequest,
    IssueWorkloadIdentityTokenReply, IssueWorkloadIdentityTokenRequest, JobConfig,
    JobSpecHistoryItem, JobTemplate, JobTemplateProvenance, JobTemplateSummary,
    JobTemplateVariable, JobTemplateVersionInfo, KvEntry, KvInjectionAction, KvJobReference,
    KvValueType, LedgerNode, ListBootTicketsReply, ListBootTicketsRequest, ListEventsReply,
    ListEventsRequest, ListExecKeysReply, ListExecKeysRequest, ListInstancesReply,
    ListInstancesRequest, ListJobActionsReply, ListJobActionsRequest, ListJobSpecHistoryReply,
    ListJobSpecHistoryRequest, ListJobTemplatesReply, ListJobTemplatesRequest, ListJobsReply,
    ListJobsRequest, ListKvReply, ListKvRequest, ListNodeUsageReply, ListNodeUsageRequest,
    ListRaftHistoryReply, ListRaftHistoryRequest, ListRolloutEventsReply, ListRolloutEventsRequest,
    ListRolloutsReply, ListRolloutsRequest, ListSavedStateQueriesReply,
    ListSavedStateQueriesRequest, ListServiceEdgesReply, ListServiceEdgesRequest,
    ListServicesReply, ListServicesRequest, ListVolumeDriversReply, ListVolumeDriversRequest,
    ListVolumesReply, ListVolumesRequest, MintExecTicketReply, MintExecTicketRequest, Node,
    NodeMsg, PatchKvReply, PatchKvRequest, PlanJobReply, PlanJobRequest, PlannedAllocation,
    PutKvReply, PutKvRequest, QueryStateStoreReply, QueryStateStoreRequest, RaftHistoryEvent,
    RaftStatus, RatifyClaimReply, RatifyClaimRequest, ReleaseKvLockReply, ReleaseKvLockRequest,
    RenderJobTemplateReply, RenderJobTemplateRequest, RenewKvLockReply, RenewKvLockRequest,
    ReplaceInstanceReply, ReplaceInstanceRequest, ResolveServiceReply, ResolveServiceRequest,
    RestartWorkerReply, RestartWorkerRequest, RolloutAllocationStatus, RolloutEvent,
    RolloutGroupStatus, RolloutProgression, RolloutReplacement, RotateMeshCaReply,
    RotateMeshCaRequest, RunJobReply, RunJobRequest, RunJobTemplateReply, RunJobTemplateRequest,
    SavedStateQuery, ScaleTaskReply, ScaleTaskRequest, SchedulerEvent, ServiceEdge,
    ServiceEndpoint, ServiceSummary, SetNodeAvailabilityReply, SetNodeAvailabilityRequest,
    StartInstanceReply, StartInstanceRequest, StopInstanceReply, StopInstanceRequest, StopJobReply,
    StopJobRequest, StopNotice, StopRolloutReply, StopRolloutRequest, StreamEventsRequest,
    Submission, TaskConfig, TaskOnUnhealthyConfig, TaskScaleStatus, UpdateNodeDynamicReply,
    UpdateNodeDynamicRequest, UpsertJobTemplateReply, UpsertJobTemplateRequest,
    UpsertSavedStateQueryReply, UpsertSavedStateQueryRequest, UpstreamCredentialValue,
    WorkerConfig, WorkerPreRegisterReply, WorkerPreRegisterRequest, WorkerRegisterReply,
    WorkerRegisterRequest,
};
use crate::structs::{
    AllocNetPolicy, DeleteNetPolicyReply, DeleteNetPolicyRequest, FetchNetPolicyReply,
    FetchNetPolicyRequest, GetNetPolicyObjectReply, GetNetPolicyObjectRequest,
    ListNetPoliciesReply, ListNetPoliciesRequest, NetPolicy, ShowNetPolicyReply,
    ShowNetPolicyRequest, TestNetPolicyReply, TestNetPolicyRequest, UpsertNetPolicyReply,
    UpsertNetPolicyRequest,
};
use crate::structs::{
    AuthMethodInfo, CreateAclTokenReply, CreateAclTokenRequest, CreateJoinTokenReply,
    CreateJoinTokenRequest, DeleteAclPolicyReply, DeleteAclPolicyRequest, DeleteAclTokenReply,
    DeleteAclTokenRequest, DeleteAclUserReply, DeleteAclUserRequest, DeleteJoinTokenReply,
    DeleteJoinTokenRequest, GetAclPolicyReply, GetAclPolicyRequest, GetAuthMethodsReply,
    GetAuthMethodsRequest, ListAclPoliciesReply, ListAclPoliciesRequest, ListAclTokensReply,
    ListAclTokensRequest, ListAclUsersReply, ListAclUsersRequest, ListJoinTokensReply,
    ListJoinTokensRequest, LoginReply, LoginRequest, StopCause, UpsertAclPolicyReply,
    UpsertAclPolicyRequest, UpsertAclUserReply, UpsertAclUserRequest, WhoAmIReply, WhoAmIRequest,
};
use crate::structs::{
    BackupEncryptionInfo, GetKeyringStatusReply, GetKeyringStatusRequest, KeyringKeyInfo,
    KeyringRewrapProgress, ResetKeyringReply, ResetKeyringRequest, RewrapKvReply, RewrapKvRequest,
    RewrapRootKeyReply, RewrapRootKeyRequest, RotateDataKeyReply, RotateDataKeyRequest,
};
use crate::task_plugins::validate_job_config;
use crate::tracing_util::{o11y_enabled, read_recent_observability};

use time::{Duration as TimeDuration, OffsetDateTime};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

// Phase 6: ForkInstance, `from_snapshot` creates, and snapshot names.
mod checkpoint_support;
mod fork_rpcs;
mod make_room;
mod node_content_rpcs;
mod session_rpcs;
mod snapshot_rpcs;
mod suspend_rpcs;
mod work_stream;
pub use make_room::{make_room_sweep, MakeRoomOutcome, MakeRoomState, MAKE_ROOM_COOLDOWN};
pub(crate) use snapshot_rpcs::snapshot_restore_payload;
pub use snapshot_rpcs::{
    checkpoint_part_keys, expire_snapshots, PartKeyUse, CHECKPOINT_CAPABILITY,
};
pub use suspend_rpcs::{idle_sweep, settle_stalled_resumes, IdleSweepOutcome, IdleSweeper};

/// The delegated exec keys as last read, their version, and when.
type CachedExecKeys = (std::time::Instant, String, Vec<crate::structs::ExecKey>);

fn map_store_error(action: &str, err: anyhow::Error) -> Status {
    let status = store_error_status(action, &err);
    // Log by what the error means for the server. A refused or missing-thing
    // answer (NotFound, FailedPrecondition, a non-leader redirect, ...) is
    // the caller's to act on and is returned to it in full, so it is not a
    // server error: an ERROR line for every `legion job info typo` or every
    // heartbeat that races its node's registration buried the real ones.
    match status.code() {
        tonic::Code::Internal | tonic::Code::DataLoss | tonic::Code::Unknown => {
            error!("{action}: {:?}", err)
        }
        tonic::Code::Unavailable => warn!("{action}: {err:#}"),
        _ => debug!("{action}: {err:#}"),
    }
    status
}

/// The gRPC status for a store error (see [`map_store_error`]).
fn store_error_status(action: &str, err: &anyhow::Error) -> Status {
    // A write proposed on a raft non-leader: FAILED_PRECONDITION with the
    // leader name+address in the message so callers can redial (plan §A4,
    // JC-4). Checked before the storage-error classes because the raft
    // pipeline wraps the typed error in context.
    if let Some(not_leader) = err
        .chain()
        .find_map(|cause| cause.downcast_ref::<crate::storage::raft::driver::NotRaftLeader>())
    {
        let mut status = Status::failed_precondition(format!("{action}: {not_leader}"));
        // Machine-readable leader address: the leader-forward layer replays
        // the request there, so a caller of any manager gets the leader's
        // answer instead of this rejection (spot-check P2-5).
        if let Some(value) = not_leader
            .leader_addr
            .as_deref()
            .and_then(|addr| addr.parse().ok())
        {
            status.metadata_mut().insert(
                crate::manager::leader_forward::RAFT_LEADER_METADATA_KEY,
                value,
            );
        }
        return status;
    }

    let Some(storage_error) = StorageError::from_anyhow(err) else {
        // An untyped error is a bug or an unexpected failure, not a caller
        // mistake, so INTERNAL — but with its cause: the caller gets the same
        // sentence the server just logged rather than a bare "Internal
        // error". Errors that ARE a caller's state problem are typed
        // (`StorageError::FailedPrecondition`, `NotFound`, ...) and mapped
        // below.
        return Status::internal(format!("{action}: {err:#}"));
    };

    let message = format!("{action}: {storage_error}");
    match storage_error {
        StorageError::NotFound { .. } => Status::not_found(message),
        StorageError::Conflict { .. } => Status::already_exists(message),
        StorageError::InvalidInput { .. } => Status::invalid_argument(message),
        StorageError::FailedPrecondition { .. } => Status::failed_precondition(message),
        StorageError::ForeignKey { .. } => Status::failed_precondition(message),
        StorageError::DatabaseBusy | StorageError::DatabaseLocked => Status::unavailable(message),
        StorageError::DatabaseReadOnly => Status::failed_precondition(message),
        StorageError::DatabaseCorrupt
        | StorageError::DatabaseIo { .. }
        | StorageError::Sqlite { .. } => Status::internal(message),
        // Root key not available on this manager: the operator-facing
        // message names the key, the provider, and the path; it clears the
        // moment the root key is placed, so it is a retryable precondition.
        StorageError::KeyringLocked { .. } => Status::failed_precondition(message),
        StorageError::KvCiphertextInvalid { .. } => Status::internal(message),
    }
}

/// The exact confirmation `ResetKeyring` requires.
pub const KEYRING_RESET_CONFIRMATION: &str = "DELETE-UNDECRYPTABLE-VALUES";

/// The one sentence `legion keyring status` prints as the next step
/// (docs/kv-encryption.md). Empty when there is nothing to do.
fn keyring_hint(
    snapshot: &crate::storage::keyring::KeyringSnapshot,
    plaintext_rows: i64,
    pending_rewrap: i64,
    raft_follower: bool,
) -> String {
    match snapshot.state.as_str() {
        "disabled" => {
            if plaintext_rows > 0 || snapshot.rows.is_empty() {
                "KV encryption is off (encryption.enabled = false); values are stored as \
                 plaintext. Remove the setting (the default is on) to encrypt them."
                    .to_string()
            } else {
                "KV encryption is off (encryption.enabled = false) but encrypted values remain; \
                 run `legion keyring rewrap --plaintext` to convert them, or turn encryption \
                 back on."
                    .to_string()
            }
        }
        "pending_upgrade" => snapshot.pending_upgrade.clone().unwrap_or_default(),
        "uninitialized" => {
            if raft_follower {
                "The control-lane leader initializes the keyring; this manager follows.".to_string()
            } else {
                "The keyring has no data key yet; the manager creates one on its next start or \
                 keyring tick (values are stored as plaintext until then)."
                    .to_string()
            }
        }
        "locked" => {
            if let Some(err) = &snapshot.provider_error {
                return format!(
                    "Root key provider `{}` at {} failed: {err}. Fix the provider, then check \
                     `legion keyring status` again.",
                    snapshot.provider, snapshot.location
                );
            }
            let mut root_key_ids: Vec<String> = snapshot
                .locked
                .iter()
                .map(|locked| locked.root_key_id.clone())
                .collect();
            root_key_ids.sort();
            root_key_ids.dedup();
            format!(
                "Root key {} is not available on this manager (provider `{}` at {}). Copy it \
                 from a manager that has it — or from where you keep it — into place (or set \
                 {}), then check `legion keyring status` again; reads and writes under the \
                 affected data key(s) fail until then. If the key is gone for good: `legion \
                 keyring reset --i-understand-encrypted-values-will-be-lost`.",
                root_key_ids.join(", "),
                snapshot.provider,
                snapshot.location,
                crate::storage::keyring::DEF_ROOT_KEY_ENV,
            )
        }
        _ => {
            if let Some(err) = &snapshot.rewrap_error {
                return format!(
                    "The last rewrap failed: {err}. Run `legion keyring rewrap` to retry."
                );
            }
            if snapshot.rewrap_running {
                return format!("A rewrap is converting {pending_rewrap} value(s); nothing to do.");
            }
            if plaintext_rows > 0 {
                return format!(
                    "{plaintext_rows} value(s) are still stored as plaintext (written before \
                     encryption was on). Run `legion keyring rewrap` to encrypt them now, or let \
                     the manager's keyring tick do it."
                );
            }
            if pending_rewrap > 0 {
                return format!(
                    "{pending_rewrap} value(s) are still under a retired data key. Run `legion \
                     keyring rewrap` to finish now, or wait for the background rewrap."
                );
            }
            String::new()
        }
    }
}

/// The keyring as the answering manager sees it, with the one-line next
/// step (`GetKeyringStatus`, `legion keyring status`).
fn keyring_status_reply(svc: &ManagerDaemonService) -> Result<GetKeyringStatusReply, Status> {
    let keyring = svc.store.keyring();
    let snapshot = keyring.snapshot();
    let counts = svc
        .store
        .kv_encryption_counts()
        .map_err(|e| map_store_error("Error counting kv rows", e))?;
    let pending = svc
        .store
        .kv_rows_pending_rewrap(false)
        .map_err(|e| map_store_error("Error counting kv rows", e))?;
    let plaintext_rows = counts.get(&None).copied().unwrap_or(0);
    let encrypted_rows: i64 = counts
        .iter()
        .filter(|(key, _)| key.is_some())
        .map(|(_, count)| *count)
        .sum();
    let keys: Vec<KeyringKeyInfo> = keyring
        .describe_keys(&counts)
        .into_iter()
        .map(|described| KeyringKeyInfo {
            key_id: described.row.key_id,
            state: described.row.state,
            algorithm: described.row.algorithm,
            created_at: described.row.created_at,
            retired_at: described.row.retired_at,
            root_key_id: described.row.root_key_id,
            root_provider: described.row.root_provider,
            value_count: described.value_count,
            unwrapped: described.unwrapped,
            error: described.error.unwrap_or_default(),
        })
        .collect();
    let active_key_age_seconds = keyring
        .active_key()
        .map(|(_, created)| (OffsetDateTime::now_utc().unix_timestamp() - created).max(0))
        .unwrap_or(0);
    let raft_follower = svc.store.raft_status().is_some()
        && !svc.store.is_raft_leader(crate::storage::Lane::Control);
    let hint = keyring_hint(&snapshot, plaintext_rows, pending, raft_follower);
    Ok(GetKeyringStatusReply {
        enabled: snapshot.enabled,
        state: snapshot.state,
        root_key_provider: snapshot.provider,
        root_key_location: snapshot.location,
        root_key_id: snapshot.root_key_id.unwrap_or_default(),
        keys,
        plaintext_rows,
        encrypted_rows,
        rewrap: Some(KeyringRewrapProgress {
            pending,
            running: snapshot.rewrap_running,
            last_error: snapshot.rewrap_error.unwrap_or_default(),
        }),
        data_key_rotation_age: humantime::format_duration(snapshot.rotation_age).to_string(),
        active_key_age_seconds,
        hint,
        manager: svc.advertise_addr.clone(),
    })
}

/// Whether a mapped store error was the typed raft not-the-leader rejection
/// (`map_store_error` renders it as `FAILED_PRECONDITION` carrying the
/// `NotRaftLeader` display text, which this crate owns). Used by RPC tails
/// that treat exactly that rejection as "deferred to the lane's reconciler"
/// (plan §8, D1) while every other failure stays an error.
fn status_is_not_raft_leader(status: &Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status.message().contains("not the raft leader")
}

/// Outcome of the worker<->manager protocol-version handshake performed at
/// registration. See [`protocol_compat`] and `docs/upgrades.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionCompat {
    /// Worker speaks a protocol version this manager supports.
    Accept,
    /// Worker reported `0`: a pre-versioning build (proto3 decodes a missing
    /// field as `0`). Admitted so rolling upgrades from releases that predate
    /// the handshake keep working, but worth a warning in the logs.
    AcceptWithWarn,
    /// Worker speaks a protocol version newer than this manager supports —
    /// the worker was upgraded before the manager (wrong order). Reject with
    /// FAILED_PRECONDITION; the operator must upgrade managers first.
    Reject,
}

/// Pure protocol-compatibility decision for worker registration.
///
/// Policy (managers are upgraded first, then workers — see docs/upgrades.md):
/// - `worker_pv == 0`: pre-versioning worker, accept with a warning.
/// - `worker_pv <= manager_pv`: accept (managers stay compatible with all
///   protocol versions up to their own).
/// - `worker_pv > manager_pv`: reject — the manager cannot guarantee it
///   satisfies a contract newer than it knows about.
pub fn protocol_compat(worker_pv: u32, manager_pv: u32) -> VersionCompat {
    if worker_pv == 0 {
        VersionCompat::AcceptWithWarn
    } else if worker_pv > manager_pv {
        VersionCompat::Reject
    } else {
        VersionCompat::Accept
    }
}

/// Outcome of `submit_rendered_job`, shared by RunJob and RunJobTemplate.
struct SubmittedJob {
    message: &'static str,
    job_name: String,
    job_config_id: String,
    /// Advisory notes appended to the reply message: a priority band that
    /// cannot do what its author expects because preemption is disabled (the
    /// Nomad discoverability failure inverted, caught at submit rather than
    /// discovered from a troubleshooting guide), and `resources` a runtime
    /// accepts but cannot honor as written (`memoryMax` on an ungoverned
    /// runtime, a sandbox `memory_mib` that disagrees with `resources`).
    warnings: Vec<String>,
}

impl SubmittedJob {
    /// The reply message plus any warnings, one per line.
    fn message_with_warnings(&self) -> String {
        if self.warnings.is_empty() {
            return self.message.to_string();
        }
        let mut message = self.message.to_string();
        for warning in &self.warnings {
            message.push('\n');
            message.push_str("warning: ");
            message.push_str(warning);
        }
        message
    }
}

/// Store record -> proto message for one template version.
fn job_template_to_proto(record: JobTemplateVersionRecord) -> JobTemplate {
    JobTemplate {
        name: record.name,
        version: record.version,
        latest_version: record.latest_version,
        description: record.description,
        source_nickel: record.source_nickel,
        variables: record
            .variables
            .into_iter()
            .map(|variable| JobTemplateVariable {
                required: variable.is_required(),
                key: variable.key,
                r#type: variable.var_type,
                label: variable.label.unwrap_or_default(),
                description: variable.description,
                default_json: variable.default.as_ref().map(|value| value.to_string()),
                options: variable.options,
            })
            .collect(),
        created_at: record.created_at,
        created_by: record.created_by,
    }
}

/// Recorded idempotent outcome for RunJobTemplate. RunJob records a bare
/// message string; template runs also need the job name and config id to
/// replay a faithful reply, so they record this JSON envelope instead.
#[derive(serde::Serialize, serde::Deserialize)]
struct TemplateRunRecord {
    message: String,
    job_name: String,
    job_config_id: String,
}

/// Load the latest config of a job that must be runnable for keyed-instance
/// mutations (CreateInstance/CreateInstances). A stopped job schedules
/// nothing, so growing it would only strand pending rows — reject loudly.
// `Status` is a large error type, but these helpers mirror the gRPC handlers
// that call them (which return `Result<_, Status>` throughout this file).
fn keyed_job_config(store: &Store, job_id: &str) -> Result<JobConfig, Status> {
    let job = store
        .get_job(job_id)
        .map_err(|e| map_store_error("Error loading job", e))?;
    if job.status == "stopped" {
        return Err(Status::failed_precondition(format!(
            "job {job_id} is stopped; run it before creating instances"
        )));
    }
    store
        .get_job_config(job_id)
        .map_err(|e| map_store_error("Error loading job config", e))
}

/// Resolve the single keyed-mode stateful task an instance-create request
/// targets. `group`/`task` filters may be omitted when the job has exactly
/// one keyed task; ambiguity is rejected with InvalidArgument, never guessed
/// (see the CreateInstance/CreateInstances proto comments). Shared by the
/// single and batch create handlers so both apply identical rules.
fn resolve_keyed_task<'a>(
    config: &'a JobConfig,
    job_id: &str,
    group: Option<&str>,
    task: Option<&str>,
) -> Result<&'a TaskConfig, Status> {
    let group_filter = group.filter(|group| !group.trim().is_empty());
    let task_filter = task.filter(|task| !task.trim().is_empty());
    let mut candidates: Vec<&TaskConfig> = config
        .tasks
        .iter()
        .filter(|task| {
            task.stateful
                .as_ref()
                .is_some_and(|stateful| stateful.mode == InstanceMode::Keyed)
        })
        .filter(|task| group_filter.is_none_or(|group| task.group == group))
        .filter(|task| task_filter.is_none_or(|name| task.name == name))
        .collect();
    match candidates.len() {
        0 => Err(Status::invalid_argument(format!(
            "job {} has no keyed-mode stateful task matching group={} task={}; keyed \
             instances can only be created for tasks declaring stateful.mode = \"keyed\"",
            job_id,
            group_filter.unwrap_or("<any>"),
            task_filter.unwrap_or("<any>"),
        ))),
        1 => Ok(candidates.remove(0)),
        _ => {
            let matches: Vec<String> = candidates
                .iter()
                .map(|task| format!("{}/{}", task.group, task.name))
                .collect();
            Err(Status::invalid_argument(format!(
                "multiple keyed tasks match in job {} ({}); disambiguate with group and task",
                job_id,
                matches.join(", "),
            )))
        }
    }
}

/// Resolve the single SCALABLE task a `legion job scale` request targets
/// (docs/autoscaling.md): not keyed-mode stateful (no count to scale) and
/// not `count: { per: ... }` (group-converging; rejected in v1).
/// `group`/`task` filters may be omitted when the job has exactly one
/// scalable task; ambiguity is rejected with the candidate list, never
/// guessed — the same rules as `resolve_keyed_task`, inverted.
fn resolve_scalable_task<'a>(
    config: &'a JobConfig,
    job_id: &str,
    group: Option<&str>,
    task: Option<&str>,
) -> Result<&'a TaskConfig, Status> {
    let group_filter = group.filter(|group| !group.trim().is_empty());
    let task_filter = task.filter(|task| !task.trim().is_empty());
    let mut candidates: Vec<&TaskConfig> = config
        .tasks
        .iter()
        .filter(|task| {
            let keyed = task
                .stateful
                .as_ref()
                .is_some_and(|stateful| stateful.mode == InstanceMode::Keyed);
            !keyed && task.count.per.is_none()
        })
        .filter(|task| group_filter.is_none_or(|group| task.group == group))
        .filter(|task| task_filter.is_none_or(|name| task.name == name))
        .collect();
    match candidates.len() {
        0 => Err(Status::invalid_argument(format!(
            "job {} has no scalable task matching group={} task={}; keyed stateful tasks \
             have no count (use the instance API) and count.per tasks cannot be scaled",
            job_id,
            group_filter.unwrap_or("<any>"),
            task_filter.unwrap_or("<any>"),
        ))),
        1 => Ok(candidates.remove(0)),
        _ => {
            let matches: Vec<String> = candidates
                .iter()
                .map(|task| format!("{}/{}", task.group, task.name))
                .collect();
            Err(Status::invalid_argument(format!(
                "multiple scalable tasks match in job {} ({}); disambiguate with task_group and task_name",
                job_id,
                matches.join(", "),
            )))
        }
    }
}

const DEFAULT_PAGE_SIZE: usize = 20;
const MAX_PAGE_SIZE: usize = 100;

fn normalize_query(value: Option<String>) -> Option<String> {
    value.and_then(|query| {
        let trimmed = query.trim().to_lowercase();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

fn parse_page(page: Option<u32>, page_size: Option<u32>) -> (usize, usize) {
    let page = page.unwrap_or(1).max(1) as usize;
    let size = page_size.unwrap_or(DEFAULT_PAGE_SIZE as u32).max(1) as usize;
    (page, size.min(MAX_PAGE_SIZE))
}

fn paginate<T>(values: Vec<T>, page: usize, page_size: usize) -> (Vec<T>, u32) {
    let total_count = values.len() as u32;
    let start = page.saturating_sub(1).saturating_mul(page_size);
    if start >= values.len() {
        return (Vec::new(), total_count);
    }
    let end = (start + page_size).min(values.len());
    (
        values.into_iter().skip(start).take(end - start).collect(),
        total_count,
    )
}

fn map_raft_status(status: crate::storage::persistence::RaftStatus) -> RaftStatus {
    RaftStatus {
        enabled: status.enabled,
        backend: status.backend,
        node_id: status.node_id,
        cluster_id: status.cluster_id,
        role: status.role.as_str().to_string(),
        leader_id: status.leader_id,
        term: status.term,
        commit_index: status.commit_index,
        applied_index: status.applied_index,
        lease_held: status.lease_held,
        voters: status.voters,
        learners: status.learners,
        peers: status.peers,
        election_epoch_ms: status.election_epoch_ms,
        join_token_ttl_seconds: status.join_token_ttl_seconds,
        raft_library: status.raft_library,
        lanes: status
            .lanes
            .into_iter()
            .map(|lane| crate::structs::RaftLaneStatus {
                lane: lane.lane,
                role: lane.role.as_str().to_string(),
                leader_id: lane.leader_id,
                term: lane.term,
                commit_index: lane.commit_index,
                applied_index: lane.applied_index,
                voters: lane.voters,
                quorum: lane.quorum,
                survives: lane.survives,
                warning: lane.warning,
            })
            .collect(),
    }
}

fn map_raft_event(event: crate::storage::persistence::RaftHistoryEvent) -> RaftHistoryEvent {
    RaftHistoryEvent {
        event_id: event.event_id,
        event_type: event.event_type,
        message: event.message,
        actor: event.actor,
        target: event.target,
        term: event.term,
        leader_id: event.leader_id,
        created_at: event.created_at,
    }
}

/// Map a stored [`Instance`] to its wire shape (`InstanceInfo` in
/// `proto/legion.proto`), attaching the node-loss diagnostic when the
/// instance is gated (see [`instance_loss_detail`]).
fn map_instance_info(
    store: &Store,
    usage: &crate::manager::usage::UsageTable,
    instance: Instance,
    suspend: crate::structs::suspend::InstanceSuspendState,
    history: crate::storage::store::InstanceHistory,
) -> InstanceInfo {
    // Loss confirmation first: it explains an instance that is bound but
    // waiting. A volume pin explains one that is UNBOUND and waiting — the two
    // never apply at once, and both answer "why has this not come back". A
    // suspend or restore failure explains the rest.
    let detail = instance_loss_detail(store, &instance)
        .or_else(|| instance_volume_pin_detail(store, &instance))
        .or_else(|| suspend.lifecycle_error.clone());
    // The checkpoint's shape, for `instance list`'s CHECKPOINT column. Only
    // suspended or resuming instances have one, so others pay nothing.
    let checkpoint = suspend
        .checkpoint_id
        .as_deref()
        .and_then(|id| store.get_snapshot(id).ok().flatten());
    let pending_op = suspend.pending_op.as_deref().map(|op| match op {
        crate::structs::suspend::PENDING_SUSPEND => "suspending".to_string(),
        crate::structs::suspend::PENDING_RESUME => "resuming".to_string(),
        crate::structs::fork::PENDING_FORK => "forking".to_string(),
        other => other.to_string(),
    });
    let usage = instance
        .current_alloc_id
        .and_then(|allocation_id| usage.allocation(allocation_id).cloned());
    // The idle policy's view, from the node's latest report: only a running
    // instance with a policy has one.
    let idle = usage
        .as_ref()
        .filter(|usage| usage.idle_after_seconds > 0 && usage.idle_since_unix > 0)
        .filter(|_| instance.state == InstanceState::Active);
    let idle_since = idle.and_then(|usage| {
        chrono::DateTime::<chrono::Utc>::from_timestamp(usage.idle_since_unix, 0)
            .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
    });
    let idle_after_seconds = idle.map(|usage| usage.idle_after_seconds);
    let checkpoint_support = checkpoint_support::load_checkpoint_support(store, &instance);
    let last_stop = history.last_stop.map(|stop| crate::structs::InstanceStop {
        cause: stop.cause,
        detail: stop.detail,
        hint: stop.hint,
        at: chrono::DateTime::<chrono::Utc>::from_timestamp(stop.at, 0)
            .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            .unwrap_or_default(),
        allocation_id: stop.allocation_id,
        node_id: stop.node_id,
        preemptor_job: stop.preemptor_job,
    });
    let (held_memory_mib, held_cpu_mhz) = suspended_hold_of(store, &instance);
    let cold_boot = suspend
        .cold_boot
        .clone()
        .map(|cold| crate::structs::InstanceColdBoot {
            snapshot_id: cold.snapshot_id,
            reason: cold.reason,
            at: chrono::DateTime::<chrono::Utc>::from_timestamp(cold.at, 0)
                .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
                .unwrap_or_default(),
        });
    InstanceInfo {
        cold_boot,
        last_stop,
        checkpoint_support,
        forked_from: history.forked_from,
        held_memory_mib,
        held_cpu_mhz,
        usage,
        instance_id: instance.instance_id,
        job_id: instance.job_id,
        group: instance.group,
        task: instance.task_name,
        mode: instance.mode.to_string(),
        state: instance.state.to_string(),
        slot_index: instance.slot_index,
        current_alloc_id: instance.current_alloc_id,
        storage_handle: instance.storage_handle,
        created_at: instance.created_at,
        updated_at: instance.updated_at,
        detail,
        restore_snapshot_id: instance.restore_from,
        checkpoint_kind: checkpoint
            .as_ref()
            .map(|snapshot| snapshot.kind.as_str().to_string()),
        checkpoint_durability: checkpoint
            .as_ref()
            .map(|snapshot| snapshot.durability.as_str().to_string()),
        checkpoint_state: checkpoint
            .as_ref()
            .map(|snapshot| snapshot.state.as_str().to_string()),
        checkpoint_node_id: checkpoint
            .as_ref()
            .and_then(|snapshot| snapshot.node_id.clone()),
        checkpoint_bytes: checkpoint.as_ref().map(|snapshot| snapshot.total_bytes),
        checkpoint_id: suspend.checkpoint_id,
        suspended_at: suspend.suspended_at.and_then(|unix| {
            chrono::DateTime::<chrono::Utc>::from_timestamp(unix, 0)
                .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        }),
        suspend_reason: suspend.suspend_reason,
        restored_from_snapshot_id: suspend.restored_from_snapshot_id,
        last_restore_ms: suspend.last_restore_ms.map(|ms| ms.max(0) as u64),
        last_suspend_ms: suspend.last_suspend_ms.map(|ms| ms.max(0) as u64),
        pending_op,
        idle_since,
        idle_after_seconds,
        // From the node's network status report, which the handlers
        // attach ([`attach_guest_address`]).
        guest_address: None,
        guest_bridge: None,
        // Set by GetInstance only ([`attach_wake_and_endpoints`]).
        endpoints: Vec::new(),
        wake: None,
        named: instance.named,
        archived: suspend.manifest_url.is_some(),
        manifest_url: suspend.manifest_url,
        durable: true,
        promoted_from: instance.promoted_from,
        node_id: None,
        lease_issued_at: None,
        envelope_alloc_id: None,
    }
}

/// The InstanceInfo of an instance archived with `archive =
/// "manifestOnly"`, which has no row: what its archive manifest's envelope
/// says (docs/suspend-and-resume.md, "Archived instances").
fn manifest_only_instance_info(
    manifest: &crate::structs::archive::ArchiveManifest,
    manifest_url: &str,
) -> InstanceInfo {
    let envelope = &manifest.envelope;
    let at = |unix: i64| {
        chrono::DateTime::<chrono::Utc>::from_timestamp(unix, 0)
            .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
    };
    InstanceInfo {
        instance_id: envelope.instance_id.clone(),
        job_id: envelope.job_id.clone(),
        group: envelope.group.clone(),
        task: envelope.task.clone(),
        mode: InstanceMode::Keyed.to_string(),
        state: InstanceState::Suspended.to_string(),
        created_at: envelope.created_at.clone(),
        updated_at: envelope
            .suspended_at
            .and_then(at)
            .unwrap_or_else(|| envelope.created_at.clone()),
        checkpoint_id: Some(envelope.snapshot.snapshot_id.clone()),
        checkpoint_durability: Some(envelope.snapshot.durability.as_str().to_string()),
        suspended_at: envelope.suspended_at.and_then(at),
        suspend_reason: envelope.suspend_reason.clone(),
        last_suspend_ms: envelope.last_suspend_ms.map(|ms| ms.max(0) as u64),
        forked_from: envelope.forked_from.clone(),
        named: envelope.named,
        archived: true,
        manifest_url: Some(manifest_url.to_string()),
        ..Default::default()
    }
}

/// What a suspended instance keeps charged to its node, as `(memory MiB,
/// cpu MHz)`, from its held allocation row (docs/suspend-and-resume.md,
/// "Capacity for resumes"). `(None, None)` when it holds nothing or is not
/// suspended. Lookups that fail degrade to nothing shown.
fn suspended_hold_of(store: &Store, instance: &Instance) -> (Option<f64>, Option<f64>) {
    if instance.state != InstanceState::Suspended {
        return (None, None);
    }
    let Some(allocation) = instance
        .current_alloc_id
        .and_then(|id| store.get_allocation_by_id(id).ok().flatten())
        .filter(|allocation| allocation.status == crate::constants::ALLOCATION_STATUS_SUSPENDED)
    else {
        return (None, None);
    };
    match store.resource_policy.usage_request(&allocation.resources) {
        Ok(request) => (
            Some(request.amount("memory").unwrap_or(0.0)),
            Some(request.amount("cpu").unwrap_or(0.0)),
        ),
        Err(_) => (None, None),
    }
}

/// `instance get`'s note for a running instance whose task has an idle
/// policy that cannot act: its node sends no usage reports (a worker without
/// `resourceEnforcement.enabled`, or one that predates usage), so nothing
/// measures its activity and the idle sweep never sees it. `None` otherwise.
/// Lookups that fail degrade to no note.
fn idle_policy_unmeasured_detail(
    store: &Store,
    usage: &crate::manager::usage::UsageTable,
    instance: &Instance,
) -> Option<String> {
    if instance.state != InstanceState::Active {
        return None;
    }
    let allocation_id = instance.current_alloc_id?;
    if usage.allocation(allocation_id).is_some() {
        return None;
    }
    let settings = store.suspend_settings_for(&instance.job_id, &instance.task_name);
    let idle = settings.idle?;
    let allocation = store.get_allocation_by_id(allocation_id).ok()??;
    let node = if usage.reports_for(&allocation.node_id) {
        format!(
            "node {} reports usage but not for this allocation (its runtime is not governed \
             by the worker cgroup)",
            allocation.node_id
        )
    } else {
        format!(
            "node {} sends no usage reports (resource enforcement is off or unavailable on its \
             worker; check the worker log)",
            allocation.node_id
        )
    };
    Some(format!(
        "idle-unmeasured: the task's suspend.idle policy (after {}s) cannot act because {node}, \
         so the instance will not suspend when idle",
        idle.after.as_secs()
    ))
}

/// The `egress-unenforced` detail for an allocation of `task`: a sandbox
/// whose `auto` network would be gated, running on a dev backend that has
/// no vsock to the datapath (gated plan §6.6). `None` otherwise, and while
/// the gated default is closed. Lookup failures degrade to no detail.
fn egress_unenforced_detail(
    store: &Store,
    allocation: &crate::structs::Allocation,
    task: &crate::structs::TaskConfig,
) -> Option<String> {
    if !store.sandbox_gated_default() || task.runtime != "sandbox" {
        return None;
    }
    let node = store.get_node(&allocation.node_id).ok()?;
    let node_vmm = node
        .fingerprint_attrs
        .get(crate::structs::checkpoint::ATTR_SANDBOX_VMM)
        .and_then(Value::as_str);
    crate::structs::network::egress_unenforced_detail(task, node_vmm, true)
}

/// [`egress_unenforced_detail`] for an instance's current allocation.
fn instance_egress_unenforced_detail(store: &Store, instance: &Instance) -> Option<String> {
    if !store.sandbox_gated_default() || instance.state != InstanceState::Active {
        return None;
    }
    let allocation = store
        .get_allocation_by_id(instance.current_alloc_id?)
        .ok()??;
    let task = store
        .get_task_config_by_id(allocation.task_config_id)
        .ok()??;
    egress_unenforced_detail(store, &allocation, &task)
}

/// `pending-loss-confirmation` diagnostic for instance get/list: when an
/// instance's current allocation sits on a LOST node, explain why no
/// replacement has been planned yet (the node-loss confirmation gate, see
/// docs/stateful-workloads.md "Node loss and fencing"). `None` in every
/// other situation. Lookup failures degrade to no detail — diagnostics must
/// never fail the read path.
fn instance_loss_detail(store: &Store, instance: &Instance) -> Option<String> {
    let alloc_id = instance.current_alloc_id?;
    let allocation = store.get_allocation_by_id(alloc_id).ok()??;
    let node = store.get_node(&allocation.node_id).ok()?;
    if node.status != "lost" {
        return None;
    }
    let lost_at = store
        .node_lost_at(&node.id)
        .ok()
        .flatten()
        .unwrap_or_else(|| "unknown".into());
    Some(format!(
        "pending-loss-confirmation: allocation {alloc_id} is on lost node {} (lost at \
         {lost_at}); the manager replaces it only once the loss-confirmation window \
         (stateful.onLost.replaceAfter) elapses, keeps it if the node returns first, or \
         replaces it immediately on operator override (`legion instance replace {}`)",
        node.id, instance.instance_id
    ))
}

/// `volume-pinned` diagnostic for instance get/list: when an instance is
/// awaiting placement and holds node-local storage, name the node it is waiting
/// for. Without this, a replica stuck pending looks like a capacity problem
/// when it is really a storage-locality one (see docs/volumes.md).
fn instance_volume_pin_detail(store: &Store, instance: &Instance) -> Option<String> {
    // Only instances the scheduler is actually trying to place. A stopped or
    // terminated instance is not waiting for anything, so telling its reader
    // "this can only be placed on node-a1" would be noise at best.
    if instance.current_alloc_id.is_some()
        || !matches!(
            instance.state,
            InstanceState::Pending | InstanceState::Rescheduling
        )
    {
        return None;
    }
    let volume = store
        .list_volumes(&crate::storage::store::VolumeFilter {
            instance_id: Some(instance.instance_id.clone()),
            ..Default::default()
        })
        .ok()?
        .into_iter()
        .find(|volume| volume.pins_placement())?;
    let node_id = volume.node_id.clone()?;
    let node_status = store
        .get_node(&node_id)
        .map(|node| node.status)
        .unwrap_or_else(|_| "unknown".into());
    Some(format!(
        "volume-pinned: volume {} ({}) is node-local and lives on node {node_id} (status:          {node_status}), so this instance can only be placed there. It waits for that node          rather than starting on empty storage; `legion volume delete --force {}` abandons          the data and frees the instance to place elsewhere",
        volume.volume_id, volume.driver, volume.volume_id
    ))
}

fn map_scheduler_event(event: SchedulerEventRecord) -> SchedulerEvent {
    SchedulerEvent {
        id: event.id,
        kind: event.kind,
        level: event.level,
        job_id: event.job_id,
        node_id: event.node_id,
        eval_id: event.eval_id,
        allocation_id: event.allocation_id,
        rollout_id: event.rollout_id,
        message: event.message,
        metadata_json: event.metadata_json,
        created_at: event.created_at,
    }
}

fn scheduler_event_filter(
    after_id: Option<i64>,
    limit: Option<u32>,
    kind: Option<String>,
    job_id: Option<String>,
    node_id: Option<String>,
    level: Option<String>,
) -> SchedulerEventFilter {
    SchedulerEventFilter {
        // Explicit 0 means "from the very beginning"; only reject negatives.
        after_id: after_id.filter(|id| *id >= 0),
        limit,
        kind: normalize_event_filter_value(kind),
        job_id: normalize_event_filter_value(job_id),
        node_id: normalize_event_filter_value(node_id),
        level: normalize_event_filter_value(level),
        instance_id: None,
        allocation_id: None,
    }
}

/// `GetJobSpecReply.rendered_jobspec`: the stored config as the JSON shape
/// `RunJobRequest.parsed_jobspec` takes.
fn rendered_jobspec_json(config: &JobConfig) -> Result<String, Status> {
    serde_json::to_string(config).map_err(|error| {
        error!(?error, job = %config.name, "Error serializing job config");
        Status::internal("Error serializing job config")
    })
}

fn normalize_event_filter_value(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim().to_string();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

// Outbound buffer for one StreamEvents subscriber. Slow consumers exert
// backpressure here; if they fall further behind than the store's broadcast
// buffer, the stream task backfills from the journal.
const EVENT_STREAM_BUFFER: usize = 64;
// Batch size used when replaying journal history into a stream.
const EVENT_STREAM_REPLAY_BATCH: u32 = 500;

#[derive(Debug, Clone)]
struct KvReference {
    key: String,
    json_pointer: String,
}

fn parse_kv_reference(value: &str) -> Option<&str> {
    let value = value.strip_prefix('$')?;
    // `${…}` is interpolation syntax (`${artifact.tool}` and friends), not a
    // KV reference — KV references are bare `$path/key` strings.
    if value.starts_with('{') {
        return None;
    }
    if value.is_empty() || !value.contains('/') {
        return None;
    }
    Some(value)
}

fn escape_json_pointer_segment(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn decode_json_pointer_segment(value: &str) -> String {
    value.replace("~1", "/").replace("~0", "~")
}

fn append_json_pointer(base: &str, segment: &str) -> String {
    let escaped = escape_json_pointer_segment(segment);
    if base.is_empty() {
        format!("/{escaped}")
    } else {
        format!("{base}/{escaped}")
    }
}

fn split_json_pointer(pointer: &str) -> Vec<String> {
    if pointer.is_empty() {
        return Vec::new();
    }
    pointer
        .trim_start_matches('/')
        .split('/')
        .map(decode_json_pointer_segment)
        .collect()
}

fn join_json_pointer(segments: &[String]) -> String {
    if segments.is_empty() {
        return String::new();
    }
    let encoded: Vec<String> = segments
        .iter()
        .map(|segment| escape_json_pointer_segment(segment))
        .collect();
    format!("/{}", encoded.join("/"))
}

fn normalize_kv_reference_pointer(value: &Value, pointer: &str) -> R<String> {
    let mut segments = split_json_pointer(pointer);
    if segments.is_empty() {
        return Ok(pointer.to_string());
    }

    match segments[0].as_str() {
        "task" => {
            segments[0] = "tasks".to_string();
            segments.insert(1, "0".to_string());
            Ok(join_json_pointer(&segments))
        }
        "tasks" => {
            let Some(tasks_value) = value.get("tasks") else {
                return Ok(pointer.to_string());
            };
            if let Some(map) = tasks_value.as_object() {
                let task_name = segments.get(1).context("kv reference missing task name")?;
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                let index = keys
                    .iter()
                    .position(|key| key.as_str() == task_name)
                    .context("kv reference task name not found in tasks map")?;
                segments[1] = index.to_string();
                Ok(join_json_pointer(&segments))
            } else {
                Ok(pointer.to_string())
            }
        }
        _ => Ok(pointer.to_string()),
    }
}

fn kv_entry_value(entry: &KvEntry) -> R<Value> {
    let value_type = KvValueType::try_from(entry.value_type).unwrap_or(KvValueType::Unspecified);
    match value_type {
        KvValueType::String => Ok(Value::String(entry.value.clone())),
        KvValueType::Number | KvValueType::Json => {
            let parsed: Value = serde_json::from_str(&entry.value)?;
            Ok(parsed)
        }
        KvValueType::Unspecified => bail!("kv value type must be specified for {}", entry.key),
    }
}

fn resolve_kv_references_in_value(
    store: &Store,
    value: &mut Value,
    refs: &mut Vec<KvReference>,
    pointer: &str,
) -> R<()> {
    match value {
        Value::String(text) => {
            let key = parse_kv_reference(text).map(str::to_string);
            if let Some(key) = key {
                let entry = store
                    .get_kv(&key)?
                    .ok_or_else(|| anyhow!("kv entry {key} not found"))?;
                *value = kv_entry_value(&entry)?;
                refs.push(KvReference {
                    key,
                    json_pointer: pointer.to_string(),
                });
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter_mut().enumerate() {
                let child = append_json_pointer(pointer, &index.to_string());
                resolve_kv_references_in_value(store, item, refs, &child)?;
            }
        }
        Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                let child = append_json_pointer(pointer, key);
                resolve_kv_references_in_value(store, item, refs, &child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// How materialization treats a referenced KV key that is missing from the
/// store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MissingKvBehavior {
    /// Fail the whole render. Used on the submit path: a jobspec must not be
    /// admitted while one of its required KV references is unresolvable.
    Error,
    /// Keep the previously materialized value in the task config. Used
    /// during KV-change reconciliation, where a deleted key resolves through
    /// the edge's `onDelete` policy instead of failing the entire re-render.
    KeepExisting,
}

fn resolve_jobspec_kv_references(store: &Store, jobspec: &str) -> R<(JobConfig, Vec<KvReference>)> {
    let mut value: Value = serde_json::from_str(jobspec)?;
    let mut refs = Vec::new();
    resolve_kv_references_in_value(store, &mut value, &mut refs, "")?;
    let mut refs = refs
        .into_iter()
        .map(|reference| {
            let json_pointer = normalize_kv_reference_pointer(&value, &reference.json_pointer)?;
            Ok(KvReference {
                key: reference.key,
                json_pointer,
            })
        })
        .collect::<R<Vec<_>>>()?;
    let mut config: JobConfig = serde_json::from_value(value)?;
    refs.extend(apply_job_kv_injections(
        store,
        &mut config,
        MissingKvBehavior::Error,
    )?);
    Ok((config, refs))
}

/// Re-materialize a stored job config from the current KV state. Returns the
/// updated config plus the refreshed injection dependency edges (legacy `$`
/// reference edges are positional and do not change here).
fn apply_kv_references_to_config(
    store: &Store,
    config: &JobConfig,
    refs: &[JobKvReference],
    missing: MissingKvBehavior,
) -> R<(JobConfig, Vec<KvReference>)> {
    let mut value = serde_json::to_value(config)?;
    for reference in refs {
        if !reference.json_pointer.starts_with('/') {
            continue;
        }
        if reference.json_pointer.contains("/kvInjections/") {
            continue;
        }
        let Some(entry) = store.get_kv(&reference.kv_key)? else {
            match missing {
                MissingKvBehavior::Error => {
                    bail!("kv entry {} not found", reference.kv_key)
                }
                // Deleted key: leave the last rendered value in place; the
                // edge's delete policy decides what happens to the job.
                MissingKvBehavior::KeepExisting => continue,
            }
        };
        let replacement = kv_entry_value(&entry)?;
        let pointer = reference.json_pointer.as_str();
        let target = if pointer.is_empty() {
            Some(&mut value)
        } else {
            value.pointer_mut(pointer)
        };
        let Some(target) = target else {
            bail!("jobspec pointer {} missing during kv update", pointer);
        };
        *target = replacement;
    }
    let mut updated: JobConfig = serde_json::from_value(value)?;
    let injection_refs = apply_job_kv_injections(store, &mut updated, missing)?;
    Ok((updated, injection_refs))
}

fn extract_template_kv_keys(template: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut rest = template;
    while let Some(pos) = rest.find("kv \"") {
        let tail = &rest[pos + 4..];
        if let Some(end) = tail.find('"') {
            let key = tail[..end].trim();
            if !key.is_empty() {
                keys.push(key.to_string());
            }
            rest = &tail[end + 1..];
        } else {
            break;
        }
    }
    keys
}

/// Outcome of rendering a template against the current KV state.
enum RenderedTemplate {
    Value(String),
    /// The first KV key the template needs that is missing from the store.
    MissingKey(String),
}

fn render_template_with_kv(store: &Store, template: &str) -> R<RenderedTemplate> {
    let mut rendered = template.to_string();
    for key in extract_template_kv_keys(template) {
        let Some(entry) = store.get_kv(&key)? else {
            return Ok(RenderedTemplate::MissingKey(key));
        };
        let replacement = entry.value;
        // Replace the braced form first; replacing the bare token first
        // would leave the surrounding `{{ }}` behind.
        let token = format!("kv \"{key}\"");
        let braced = format!("{{{{ {token} }}}}");
        rendered = rendered.replace(&braced, &replacement);
        rendered = rendered.replace(&token, &replacement);
    }
    Ok(RenderedTemplate::Value(rendered))
}

fn render_template_source(
    store: &Store,
    source: &crate::structs::KvTemplateSource,
) -> R<RenderedTemplate> {
    if let Some(key) = source.template_kv.as_deref() {
        let Some(entry) = store.get_kv(key)? else {
            return Ok(RenderedTemplate::MissingKey(key.to_string()));
        };
        return render_template_with_kv(store, &entry.value);
    }
    let template = source
        .template
        .as_deref()
        .context("template or template_kv must be set")?;
    render_template_with_kv(store, template)
}

/// KV keys an injection depends on: the `templateKv` pointer key itself plus
/// every `kv "..."` lookup inside the (inline or KV-stored) template text.
/// Tolerates missing keys so dependency edges survive deletes — when the
/// `templateKv` key is gone its transitive lookups are unknowable, but the
/// direct edge is still reported.
fn template_source_deps(
    store: &Store,
    source: &crate::structs::KvTemplateSource,
) -> R<Vec<String>> {
    if let Some(key) = source.template_kv.as_deref() {
        let mut deps = vec![key.to_string()];
        if let Some(entry) = store.get_kv(key)? {
            deps.extend(extract_template_kv_keys(&entry.value));
        }
        return Ok(deps);
    }
    let template = source
        .template
        .as_deref()
        .context("template or template_kv must be set")?;
    Ok(extract_template_kv_keys(template))
}

fn apply_task_kv_injections(
    store: &Store,
    task: &mut TaskConfig,
    missing: MissingKvBehavior,
) -> R<Vec<KvReference>> {
    let Some(injections) = task.kv_injections.as_ref() else {
        return Ok(Vec::new());
    };
    let mut refs = Vec::new();

    for env in &injections.env {
        let deps = template_source_deps(store, &env.value)?;
        match render_template_source(store, &env.value)? {
            RenderedTemplate::Value(rendered) => {
                let map = task
                    .config
                    .as_object_mut()
                    .context("task config must be an object")?;
                let env_value = map.entry("env").or_insert_with(|| json!({}));
                let env_map = env_value
                    .as_object_mut()
                    .context("task config env must be an object")?;
                env_map.insert(env.name.clone(), Value::String(rendered));
            }
            RenderedTemplate::MissingKey(key) => match missing {
                MissingKvBehavior::Error => bail!("kv entry {key} not found"),
                // Keep the previously rendered value already present in the
                // stored task config.
                MissingKvBehavior::KeepExisting => {}
            },
        }
        for key in deps {
            refs.push(KvReference {
                key,
                json_pointer: format!("/tasks/{}/kvInjections/env/{}", task.name, env.name),
            });
        }
    }

    let defaults = injections.kv.clone().unwrap_or_default();
    for file in &injections.files {
        let deps = template_source_deps(store, &file.file)?;
        match render_template_source(store, &file.file)? {
            RenderedTemplate::Value(rendered) => {
                let map = task
                    .config
                    .as_object_mut()
                    .context("task config must be an object")?;
                let files_value = map.entry("files").or_insert_with(|| json!([]));
                let files = files_value
                    .as_array_mut()
                    .context("task config files must be an array")?;
                let mut entry = json!({
                    "path": file.path,
                    "mode": file.mode,
                    "content": rendered
                });
                // `onChange = "update"` entries carry what the worker needs to
                // rewrite them under a running task. The marker is what opts
                // the file in: the worker never rewrites an unmarked file.
                if file.kv.resolve_with_defaults(&defaults).on_change == KvInjectionAction::Update {
                    let live = crate::structs::LiveFileUpdate {
                        change_mode: file.change_mode.unwrap_or_default(),
                        change_signal: file.change_signal.clone(),
                        splay: file.splay.clone(),
                    };
                    entry["liveUpdate"] = serde_json::to_value(live)?;
                }
                // Upsert by path so re-materialization replaces the previous
                // render instead of appending a duplicate file entry.
                if let Some(existing) = files.iter_mut().find(|value| {
                    value.get("path").and_then(Value::as_str) == Some(file.path.as_str())
                }) {
                    *existing = entry;
                } else {
                    files.push(entry);
                }
            }
            RenderedTemplate::MissingKey(key) => match missing {
                MissingKvBehavior::Error => bail!("kv entry {key} not found"),
                MissingKvBehavior::KeepExisting => {}
            },
        }
        for key in deps {
            refs.push(KvReference {
                key,
                json_pointer: format!("/tasks/{}/kvInjections/files/{}", task.name, file.path),
            });
        }
    }
    Ok(refs)
}

fn apply_job_kv_injections(
    store: &Store,
    config: &mut JobConfig,
    missing: MissingKvBehavior,
) -> R<Vec<KvReference>> {
    let mut refs = Vec::new();
    for task in &mut config.tasks {
        refs.extend(apply_task_kv_injections(store, task, missing)?);
    }
    Ok(refs)
}

fn strongest_action(a: KvInjectionAction, b: KvInjectionAction) -> KvInjectionAction {
    let rank = |value: &KvInjectionAction| match value {
        KvInjectionAction::None => 0,
        KvInjectionAction::Update => 1,
        KvInjectionAction::Restart => 2,
        KvInjectionAction::Rollout => 3,
        KvInjectionAction::Fail => 4,
    };
    if rank(&a) >= rank(&b) {
        a
    } else {
        b
    }
}

#[derive(Debug, Clone)]
struct RolloutLaunchConfig {
    config_json: String,
    strategy: Option<String>,
    progression: Option<String>,
    replacement: Option<String>,
    in_place: Option<bool>,
    is_manual: bool,
}

fn rollout_progression_name(value: &RolloutProgression) -> &'static str {
    match value {
        RolloutProgression::Auto => "auto",
        RolloutProgression::Manual => "manual",
    }
}

fn rollout_replacement_name(value: &RolloutReplacement) -> &'static str {
    match value {
        RolloutReplacement::DestroyFirst => "destroyFirst",
        RolloutReplacement::CreateFirst => "createFirst",
    }
}

fn build_rollout_launch_config(config: &JobConfig) -> Result<RolloutLaunchConfig, Status> {
    let config_json = serde_json::to_string(&json!({
        "rollout": &config.rollout,
        "rolloutByGroup": &config.rollout_by_group,
    }))
    .map_err(|e| Status::internal(format!("Error serializing rollout config: {e}")))?;

    let strategy = config
        .rollout
        .as_ref()
        .and_then(|rollout| rollout.strategy.clone())
        .or_else(|| Some("default".to_string()));
    let progression = config
        .rollout
        .as_ref()
        .and_then(|rollout| rollout.progression.as_ref())
        .map(|value| rollout_progression_name(value).to_string());
    let replacement = config
        .rollout
        .as_ref()
        .and_then(|rollout| rollout.replacement.as_ref())
        .map(|value| rollout_replacement_name(value).to_string());
    let in_place = config.rollout.as_ref().and_then(|rollout| rollout.in_place);
    let is_manual = progression.as_deref() == Some("manual");

    Ok(RolloutLaunchConfig {
        config_json,
        strategy,
        progression,
        replacement,
        in_place,
        is_manual,
    })
}

/// Reason stamped onto a job and its allocations when a watched KV key is
/// deleted and the resolved policy is `fail`.
const MISSING_KV_DEPENDENCY_REASON: &str = "missing_kv_dependency";

/// Pending coalesced KV events while a debounce window is open. Keyed by KV
/// key; the value is the latest event type (`true` = delete), so a delete
/// arriving after a put within the window still resolves through `onDelete`.
#[derive(Debug, Default)]
struct KvDebounceState {
    events: HashMap<String, bool>,
    flush_scheduled: bool,
}

#[derive(Debug)]
pub struct ManagerDaemonService {
    pub store: Store,
    pub auth: Arc<Authenticator>,
    /// Externally reachable manager address embedded in join tokens.
    pub advertise_addr: String,
    allocation_failures: Arc<Mutex<AllocationFailureTracker>>,
    /// OIDC issuer state; `None` when `workloadIdentity.enabled` is false
    /// (the default), in which case IssueWorkloadIdentityToken returns
    /// FailedPrecondition.
    workload_identity: Option<Arc<WorkloadIdentityState>>,
    /// Debounce window for KV-driven reconciliation (`kvDebounceMs` in the
    /// manager config). Zero reconciles synchronously inside the KV RPC.
    kv_debounce: Duration,
    kv_pending: Arc<Mutex<KvDebounceState>>,
    /// Cluster artifact settings (allowed schemes, auth matchers, container
    /// image desugar flag) applied at admission.
    artifacts_config: crate::structs::ArtifactsManagerConfigFull,
    /// Names of `workloadIdentity.nodeIdentities` for `auth.nodeIdentity`
    /// reference validation.
    node_identity_names: Vec<String>,
    /// Fetcher registry used to resolve `resolve: "pin"` references at
    /// submit (blocking; callers wrap in block_in_place).
    fetcher_registry: Arc<Mutex<crate::artifacts::FetcherRegistry>>,
    /// Whether `ExecAllocation` is enabled (config `exec.enabled`, default
    /// true). When false the endpoint rejects with FailedPrecondition.
    exec_enabled: bool,
    /// Direct-exec ticket lifetimes (`exec.tickets`).
    exec_tickets: crate::structs::ExecTicketsConfigFull,
    /// The delegated exec keys and their version, re-read at most once a
    /// second for heartbeats (only workers that admit exec tickets ask).
    exec_keys: Arc<Mutex<Option<CachedExecKeys>>>,
    /// Edge boot settings (`edgeBoot`, docs/edge-boot.md). Disabled by
    /// default: with `enabled: false` no ticket can be minted and no claim
    /// ratified, so a cluster that upgrades and changes nothing grows no new
    /// authorization surface.
    edge_boot: crate::structs::EdgeBootConfigFull,
    /// Last-reported provisional claims per node (docs/edge-boot.md).
    ///
    /// In memory on purpose: unratified work lives for seconds, and writing
    /// it to the store on every heartbeat would put a per-node write on the
    /// hottest path in the system to describe state that is stale before the
    /// row lands. A failover shows nothing until the next heartbeat, which is
    /// at most one interval of incompleteness and never a wrong answer.
    provisional_claims: Arc<Mutex<HashMap<String, Vec<crate::structs::ProvisionalClaimReport>>>>,
    /// The signing CA's pin, for `HeartbeatReply.cluster_ca_pin`, re-read at
    /// most every few seconds: it changes only on a CA rotation, and the
    /// heartbeat path must not pay a store read per beat for it.
    ca_pin_cache: Arc<Mutex<Option<(String, std::time::Instant)>>>,
    /// Heartbeat cadence settings (`heartbeat`, docs/heartbeat-cadence-and-scale.md).
    /// Read by the cadence report; the served interval itself comes from the
    /// status checker's ladder through the liveness table.
    heartbeat: crate::structs::HeartbeatConfigFull,
    /// Merged network policies by content hash (docs/network-handlers.md),
    /// filled by the heartbeat builder and served by `FetchNetPolicy`. In
    /// memory on purpose: the merge is deterministic, so any manager can
    /// recompute a body from the same inputs after a failover; this is only
    /// a shortcut for the common case. Bounded (`NET_POLICY_CACHE_MAX`).
    net_policy_cache: Arc<Mutex<HashMap<String, crate::structs::netpolicy::EffectivePolicy>>>,
    /// (when read, policy generation) — the store's generation counter,
    /// re-read at most every `NET_POLICY_GENERATION_TTL`, so a cluster with
    /// no policies pays a mutex read per heartbeat rather than a query.
    net_policy_generation: Arc<Mutex<Option<(std::time::Instant, u64)>>>,
    /// The service graph: per (caller job, caller task, service, tag), the
    /// last 15 minutes of per-minute buckets folded from every node's
    /// `net_edges` heartbeat samples. In memory: a failover loses at most the
    /// window, and the next heartbeats refill it.
    net_edges: Arc<Mutex<HashMap<EdgeKey, EdgeWindow>>>,
    /// The datapath status each node last reported for its allocations
    /// (docs/network-handlers.md). In memory like `provisional_claims`: it
    /// is at most one heartbeat stale and a failover shows nothing until
    /// the next beat, never a wrong answer.
    net_status: Arc<Mutex<HashMap<String, Vec<crate::structs::NetAllocStatus>>>>,
    /// How often `StreamEvents` and exec sessions re-check their caller's
    /// credential (`acl::STREAM_REAUTH_INTERVAL`; tests shorten it).
    stream_reauth_interval: Duration,
    /// The latest usage report per node (docs/resource-enforcement.md,
    /// "Measuring usage"). In memory and never in the store: it is replaced
    /// every report interval, so a restart or failover shows nothing until
    /// the next report rather than anything wrong. Shared with the idle
    /// sweep (see [`Self::usage_handle`]).
    usage: Arc<Mutex<crate::manager::usage::UsageTable>>,
    /// The manager's configured `evalThreads`. With adaptive scheduler
    /// concurrency enabled this is the CEILING on admitted planners rather
    /// than a target, and `GetSchedulerConcurrency` reports it as such even
    /// when this manager does not lead the scheduler lane.
    eval_threads: usize,
}

/// Distinct merged policy bodies kept in memory.
const NET_POLICY_CACHE_MAX: usize = 4096;
/// How stale the cached policy generation may be before the store is asked.
const NET_POLICY_GENERATION_TTL: Duration = Duration::from_secs(1);

impl ManagerDaemonService {
    /// Service with ACLs disabled (tests, embedded use).
    pub fn new(store: Store) -> Self {
        Self::with_acl(store, Arc::new(Authenticator::disabled()))
    }

    pub fn with_acl(store: Store, auth: Arc<Authenticator>) -> Self {
        Self {
            store,
            auth,
            advertise_addr: DEF_MANAGER_ADDRESS.to_string(),
            allocation_failures: Arc::new(Mutex::new(AllocationFailureTracker::default())),
            workload_identity: None,
            // Constructor default is "no debounce" so direct service users
            // (tests, embedding) see synchronous behavior; `run_manager`
            // always passes the configured window (default 500ms).
            kv_debounce: Duration::ZERO,
            kv_pending: Arc::new(Mutex::new(KvDebounceState::default())),
            artifacts_config: crate::structs::ArtifactsManagerConfigFull::default(),
            node_identity_names: Vec::new(),
            fetcher_registry: Arc::new(Mutex::new(
                crate::artifacts::FetcherRegistry::with_builtins(),
            )),
            exec_enabled: true,
            exec_tickets: crate::structs::ExecTicketsConfigFull::default(),
            exec_keys: Arc::new(Mutex::new(None)),
            edge_boot: crate::structs::EdgeBootConfigFull::default(),
            provisional_claims: Arc::new(Mutex::new(HashMap::new())),
            ca_pin_cache: Arc::new(Mutex::new(None)),
            heartbeat: crate::structs::HeartbeatConfigFull::default(),
            net_policy_cache: Arc::new(Mutex::new(HashMap::new())),
            net_policy_generation: Arc::new(Mutex::new(None)),
            net_status: Arc::new(Mutex::new(HashMap::new())),
            net_edges: Arc::new(Mutex::new(HashMap::new())),
            stream_reauth_interval: acl::STREAM_REAUTH_INTERVAL,
            usage: Arc::new(Mutex::new(crate::manager::usage::UsageTable::default())),
            eval_threads: 1,
        }
    }

    /// The signing cluster CA's pin, cached for a few seconds (see
    /// `ca_pin_cache`). Empty when no CA exists yet.
    /// The delegated exec keys for a heartbeat that asked with `version`:
    /// `(keys, current version)`, with no keys when the worker already holds
    /// the current set. Cached for a second, so a fleet of direct-exec
    /// workers costs one small read per second, not one per heartbeat.
    fn exec_keys_for(&self, version: &str) -> (Vec<crate::structs::ExecKey>, String) {
        if version.is_empty() {
            return (Vec::new(), String::new());
        }
        let mut cache = self.exec_keys.lock().unwrap_or_else(|p| p.into_inner());
        let fresh = cache
            .as_ref()
            .is_some_and(|(at, _, _)| at.elapsed() < Duration::from_secs(1));
        if !fresh {
            match self.store.list_exec_keys() {
                Ok(keys) => {
                    let current = crate::manager::exec::exec_keys_version(&keys);
                    *cache = Some((std::time::Instant::now(), current, keys));
                }
                Err(err) => {
                    debug!(error = ?err, "Error loading exec keys");
                    return (Vec::new(), version.to_string());
                }
            }
        }
        let Some((_, current, keys)) = cache.as_ref() else {
            return (Vec::new(), version.to_string());
        };
        if current == version {
            (Vec::new(), current.clone())
        } else {
            (keys.clone(), current.clone())
        }
    }

    fn cluster_ca_pin(&self) -> String {
        const TTL: Duration = Duration::from_secs(5);
        let mut cache = self.ca_pin_cache.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((pin, read_at)) = cache.as_ref() {
            if read_at.elapsed() < TTL {
                return pin.clone();
            }
        }
        let pin = self
            .store
            .mesh_ca()
            .ok()
            .flatten()
            .and_then(|ca| ca.pin().ok())
            .unwrap_or_default();
        *cache = Some((pin.clone(), std::time::Instant::now()));
        pin
    }

    /// Overrides how often long-lived streams re-check their credential.
    /// Production uses `acl::STREAM_REAUTH_INTERVAL`; tests shorten it.
    pub fn with_stream_reauth_interval(mut self, interval: Duration) -> Self {
        self.stream_reauth_interval = interval;
        self
    }

    /// The caller's credential, kept so a long-lived stream can re-check it.
    fn stream_credential<T>(&self, request: &Request<T>) -> acl::StreamCredential {
        acl::StreamCredential::new(self.auth.clone(), request.metadata())
            .with_interval(self.stream_reauth_interval)
    }

    /// The store's network-policy generation, cached for
    /// `NET_POLICY_GENERATION_TTL`. Zero means no policy has ever been
    /// written on this cluster.
    fn net_policy_generation_cached(&self) -> u64 {
        let now = std::time::Instant::now();
        {
            let cached = self
                .net_policy_generation
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if let Some((read_at, generation)) = *cached {
                if now.duration_since(read_at) < NET_POLICY_GENERATION_TTL {
                    return generation;
                }
            }
        }
        let generation = self.store.net_policy_generation().unwrap_or(0);
        *self
            .net_policy_generation
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some((now, generation));
        generation
    }

    /// Forget the cached generation (after a write on this manager).
    fn bump_net_policy_generation(&self) {
        *self
            .net_policy_generation
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = None;
    }

    fn remember_net_policy(&self, policy: &crate::structs::netpolicy::EffectivePolicy) {
        let mut cache = self
            .net_policy_cache
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if cache.len() >= NET_POLICY_CACHE_MAX && !cache.contains_key(&policy.hash) {
            cache.clear();
        }
        cache
            .entry(policy.hash.clone())
            .or_insert_with(|| policy.clone());
    }

    /// Attach the manager's heartbeat-cadence settings (`run_manager` always
    /// does; direct service users get the defaults, which serve no interval).
    pub fn with_heartbeat_config(mut self, heartbeat: crate::structs::HeartbeatConfigFull) -> Self {
        self.heartbeat = heartbeat;
        self
    }

    /// Attach the manager's `evalThreads`, the ceiling the adaptive
    /// concurrency controller works under.
    pub fn with_eval_threads(mut self, eval_threads: usize) -> Self {
        self.eval_threads = eval_threads.max(1);
        self
    }

    /// Drop-in ACL gate for RPC handlers: authenticates the caller and
    /// requires `action` on `resource` within `scope`.
    ///
    /// Every user-facing endpoint must call this (or `authenticate_acl` +
    /// `acl::enforce` when the scope is only known after parsing the request
    /// body) before doing any work. See AGENTS.md and
    /// `tests/acl_endpoints.rs`, which fails when an endpoint is missing
    /// coverage.
    fn check_acl<T>(
        &self,
        request: &Request<T>,
        resource: AclResource,
        action: AclAction,
        scope: AclScope,
    ) -> Result<AclIdentity, Status> {
        let identity = self.authenticate_acl(request)?;
        acl::enforce(&identity, resource, action, &scope)?;
        Ok(identity)
    }

    /// Resolves the caller's identity without enforcing anything. Pair with
    /// `acl::enforce` once the request body has been parsed.
    fn authenticate_acl<T>(&self, request: &Request<T>) -> Result<AclIdentity, Status> {
        self.auth.authenticate(request.metadata(), &self.store)
    }

    /// Every manager a worker may dial (`HeartbeatReply.manager_addresses`,
    /// `WorkerRegisterReply.manager_addresses`): the replicated raft
    /// membership's advertised listeners, with this manager's own API scheme
    /// (`https` when `server.tls` is on). Empty outside raft mode, which
    /// leaves a worker's known set unchanged.
    fn worker_manager_addresses(&self) -> Vec<String> {
        let scheme = if self.advertise_addr.starts_with("https://") {
            "https"
        } else {
            "http"
        };
        self.store
            .raft_member_addresses()
            .into_iter()
            .map(|host_port| format!("{scheme}://{host_port}"))
            .collect()
    }

    /// Job scope for read/lifecycle endpoints, carrying the job's stored
    /// template provenance so `templates`-scoped rules cover the jobs their
    /// templates created. Provenance lookup failures degrade to a scope
    /// without the attribute (fail closed for template-scoped rules).
    fn job_read_scope(&self, name: &str) -> AclScope {
        let provenance = self
            .store
            .job_template_provenance(name)
            .unwrap_or_default()
            .map(|provenance| provenance.name);
        AclScope::job(name).with_template(provenance)
    }

    /// Everything RunJob does once it holds a rendered jobspec, shared with
    /// RunJobTemplate so both submission paths behave identically: KV
    /// reference resolution, precise ACL enforcement, the template collision
    /// guard, task validation, artifact admission, persistence (with
    /// provenance), status clears, KV-ref bookkeeping, keyed-instance
    /// seeding, and rollout/eval enqueueing.
    fn submit_rendered_job(
        &self,
        identity: &AclIdentity,
        js: &str,
        mut subs: Vec<Submission>,
        // Explicit eval-queue override (`--priority`/`--asap`). `None` lets
        // the job's priority band set the queue position, which is the
        // default and the point of having one field mean both things.
        priority: Option<i64>,
        force_reschedule: bool,
        template: Option<&JobTemplateProvenance>,
    ) -> Result<SubmittedJob, Status> {
        if !subs.iter().any(|sub| sub.path == "job.rendered.json") {
            subs.push(Submission {
                path: "job.rendered.json".into(),
                content: js.to_string(),
                is_primary: false,
            });
        }

        let (mut config, kv_refs) =
            resolve_jobspec_kv_references(&self.store, js).map_err(|e| {
                error!("Error parsing jobspec: {:?}", e);
                Status::invalid_argument(format!("Error parsing jobspec: {e}"))
            })?;
        // On the spec-changing write path the template attribute is only ever
        // the template the submission is going through — never the job's
        // stored provenance (that would let a template-scoped identity push
        // arbitrary specs to a job its template once created).
        // The band is normalized (absent = `normal`) BEFORE enforcement, on
        // both the plain and the through-a-template submit path. Two
        // consequences, both required: a `bands`-scoped policy matches
        // un-banded submissions instead of silently granting nothing, and a
        // template that renders `priority: 'critical'` is gated by the same
        // rule as a hand-written spec — a RunJob-only check would be
        // bypassable by rendering.
        // A job with a host-level task (worker-plane-auth plan §6.16) needs a
        // `job:write` rule with `host = true`: "can deploy" must not quietly
        // mean "is root on every node".
        let host_level = config
            .tasks
            .iter()
            .find_map(crate::task_plugins::task_host_level_reason);
        let scope = match template {
            Some(template) => AclScope::job_via_template(&config.name, &template.name),
            None => AclScope::job(&config.name),
        }
        .with_band(config.priority);
        let host_scope = scope.clone().with_host(host_level.is_some());
        acl::enforce(identity, AclResource::Job, AclAction::Write, &host_scope).map_err(|status| {
            let denied = status.code() == tonic::Code::PermissionDenied;
            if let (true, Some(reason)) = (denied, host_level.as_deref()) {
                // Name the host grant only when it is what is missing.
                if acl::enforce(identity, AclResource::Job, AclAction::Write, &scope).is_ok() {
                    return Status::permission_denied(format!(
                        "job {:?} has a host-level task ({reason}). Submitting it needs a \
                         job:write rule with host = true (docs/auth-and-acls.md, \
                         \"Host-level jobs\")",
                        config.name
                    ));
                }
            }
            if denied && config.priority != crate::structs::Band::default() {
                Status::permission_denied(format!(
                    "{}: this token's policy does not allow submitting jobs in the `{}`                      priority band (see the `bands` selector in docs/auth-and-acls.md)",
                    status.message(),
                    config.priority
                ))
            } else {
                status
            }
        })?;

        if let Some(template) = template {
            self.enforce_template_collision_guard(identity, &config.name, template)?;
        }

        validate_job_config(&config).map_err(|e| {
            error!("Error validating task configs: {:?}", e);
            Status::invalid_argument(format!("Error validating task configs: {e}"))
        })?;
        let ingress_chains: Vec<_> = config
            .tasks
            .iter()
            .map(|task| task.network.as_ref().and_then(|n| n.ingress.as_ref()))
            .collect();
        enforce_kv_cert_read(identity, &ingress_chains)?;
        enforce_kv_credential_read(
            identity,
            config.tasks.iter().filter_map(|task| task.connect.as_ref()),
        )?;
        self.validate_job_volumes(&config).map_err(|e| {
            error!("Error validating volumes: {:?}", e);
            Status::invalid_argument(format!("Error validating volumes: {e:#}"))
        })?;
        self.validate_job_capabilities(&config).map_err(|e| {
            error!("Error validating capabilities: {:?}", e);
            Status::invalid_argument(format!("Error validating capabilities: {e:#}"))
        })?;
        // Artifact admission: desugar/auth/pin. Pin resolution can reach
        // registries, so it runs as blocking work.
        let artifact_pins =
            run_blocking_admission(|| self.normalize_job_artifacts(&mut config, true)).map_err(
                |e| {
                    error!("Error normalizing artifacts: {:?}", e);
                    Status::invalid_argument(format!("Error normalizing artifacts: {e:#}"))
                },
            )?;
        let config = config;
        let submit_outcome = match template {
            Some(template) => self
                .store
                .handle_job_submit_from_template(&config, &subs, template),
            None => self.store.handle_job_submit(&config, &subs),
        }
        .map_err(|e| map_store_error("Error running job", e))?;

        // Resubmitting revives a previously stopped job: clear the stopped
        // status so the instance-management RPCs accept it again. Stopped
        // keyed instances stay stopped — each needs an explicit
        // `instance start` (see docs/stateful-workloads.md).
        let was_stopped = self
            .store
            .clear_job_stopped_status(&config.name)
            .map_err(|e| map_store_error("Error clearing stopped job status", e))?;

        // A manual `job run` is also the recovery path for a job stuck
        // `failed` by a missing KV dependency under onChange:none (which never
        // auto-recovers). Clearing the failed status here re-runs the job's
        // allocations through the normal submit/eval flow with the latest KV
        // state (see docs/kv-store.md).
        let was_failed = self
            .store
            .clear_job_failed_status(&config.name)
            .map_err(|e| map_store_error("Error clearing failed job status", e))?;

        // Pin provenance joins the event stream only when the submit changed
        // something — unchanged re-submits resolve the same digests and would
        // spam the stream otherwise.
        if !matches!(submit_outcome, JobSubmitOutcome::Unchanged { .. }) {
            self.store
                .events()
                .artifact_pinned(&config.name, &artifact_pins);
        }

        let job_config_id = submit_outcome.job_config_id().to_string();
        let stored_refs: Vec<JobKvReference> = kv_refs
            .into_iter()
            .map(|reference| JobKvReference {
                job_id: config.name.clone(),
                job_config_id: job_config_id.clone(),
                kv_key: reference.key,
                json_pointer: reference.json_pointer,
            })
            .collect();
        self.store
            .replace_job_kv_references(&config.name, &stored_refs)
            .map_err(|e| map_store_error("Error storing kv references", e))?;

        // Seed keyed instances declared via `stateful.initialInstances` now —
        // after the config is persisted, BEFORE any placement eval is
        // enqueued — so the seeded rows exist when that eval plans. Ids that
        // already exist in ANY state (live, stopped, or a terminated
        // tombstone) are skipped silently: the list is a SEED, not a
        // reconciled desired set, so resubmits are idempotent, destroyed ids
        // stay destroyed, and stopped instances stay stopped until an
        // explicit `instance start`.
        let mut seeded = 0usize;
        for task in &config.tasks {
            let keyed_seed = task.stateful.as_ref().and_then(|stateful| {
                (stateful.mode == InstanceMode::Keyed)
                    .then_some(stateful.initial_instances.as_ref())
                    .flatten()
            });
            let Some(ids) = keyed_seed else { continue };
            seeded += self
                .store
                .seed_keyed_instances(&config.name, &task.group, &task.name, ids)
                .map_err(|e| map_store_error("Error seeding initial instances", e))?
                .len();
        }

        let (eval_kind, message) = match submit_outcome {
            JobSubmitOutcome::Unchanged { .. } => {
                if force_reschedule {
                    (
                        Some("job_update"),
                        "Job config unchanged; forced reschedule queued",
                    )
                } else if was_stopped {
                    // Re-running a stopped job's unchanged spec starts it
                    // again. It used to answer "no update needed" and place
                    // nothing, which left no obvious way to restart a job.
                    (
                        Some("job_update"),
                        "Job config unchanged; job was stopped, starting it again",
                    )
                } else if was_failed {
                    (
                        Some("job_update"),
                        "Job config unchanged; job had failed, starting it again",
                    )
                } else if self
                    .store
                    .job_is_idle(&config.name)
                    .map_err(|e| map_store_error("Error loading job allocations", e))?
                {
                    // Nothing of the job is running or queued to run (it
                    // finished, or nothing could be placed): a re-run means
                    // run it again.
                    (
                        Some("job_update"),
                        "Job config unchanged; job was not running, starting it again",
                    )
                } else {
                    (
                        None,
                        "Job config unchanged; no update needed (the job is running; use \
                         --force-reschedule to restart it)",
                    )
                }
            }
            JobSubmitOutcome::Created { .. } => (Some("job_submit"), "Job submit success"),
            JobSubmitOutcome::Updated { .. } => {
                let previous_job_config_id = self
                    .store
                    .previous_job_config_id(&job_config_id)
                    .map_err(|e| map_store_error("Error loading previous job config", e))?;
                match create_rollout_for_update(
                    &self.store,
                    &config.name,
                    previous_job_config_id.as_deref(),
                    &job_config_id,
                    &config,
                    "Error creating rollout",
                ) {
                    Ok(true) => (None, "Rollout created; waiting for manual start"),
                    Ok(false) => (Some("rollout_tick"), "Rollout started"),
                    // Standing-split tolerance (plan §8, D1): the update is
                    // acknowledged (control half committed); the rollout is
                    // a scheduler-lane projection the scheduler leader's
                    // `reconcile_missing_rollouts` pass derives from
                    // replicated state — with full canary/manual gating —
                    // once the config replicates to it. Failing the RPC
                    // here would strand the update: the client's retry
                    // returns `Unchanged`, which never creates a rollout.
                    Err(status) if status_is_not_raft_leader(&status) => {
                        warn!(
                            job = %config.name,
                            job_config_id,
                            "rollout creation deferred: this node does not lead the \
                             scheduler lane; its reconciler will create the rollout"
                        );
                        (
                            None,
                            "Job update accepted; rollout deferred until raft lane \
                             leadership converges",
                        )
                    }
                    Err(status) => return Err(status),
                }
            }
        };

        // The enqueue forwards to the scheduler-lane leader when this node
        // does not lead that lane (plan §8, D2); a residual typed rejection
        // (leader unknown/unreachable) is tolerated because the eval is
        // exactly what `regenerate_scheduling_evals` re-derives — the job is
        // accepted either way, so failing the RPC after the control-lane
        // commit would only mislead the client into resubmitting.
        if let Some(kind) = eval_kind {
            if let Err(e) = self
                .store
                .enqueue_eval(
                    kind,
                    Some(&config.name),
                    Some(&job_config_id),
                    None,
                    priority,
                )
                .map_err(|e| map_store_error("Error running job", e))
            {
                if !status_is_not_raft_leader(&e) {
                    return Err(e);
                }
                warn!(
                    job = %config.name,
                    job_config_id,
                    "submit eval enqueue deferred: the scheduler leader's regeneration \
                     pass will re-derive it"
                );
            }
        } else if seeded > 0 {
            // An unchanged resubmit normally needs no eval, but if it seeded
            // rows (possible when a previous submit failed between persisting
            // the config and seeding), the new instances still need placing.
            if let Err(e) = self
                .store
                .enqueue_eval(
                    "job_reschedule",
                    Some(&config.name),
                    Some(&job_config_id),
                    None,
                    priority,
                )
                .map_err(|e| map_store_error("Error scheduling seeded instance placement", e))
            {
                if !status_is_not_raft_leader(&e) {
                    return Err(e);
                }
                warn!(
                    job = %config.name,
                    job_config_id,
                    "seeded-instance eval enqueue deferred: the scheduler leader's \
                     regeneration pass will re-derive it"
                );
            }
        }

        Ok(SubmittedJob {
            message,
            job_name: config.name.clone(),
            job_config_id,
            warnings: self
                .band_submit_warnings(&config)
                .into_iter()
                .chain(crate::task_plugins::resource_submit_warnings(&config))
                .chain(crate::structs::suspend::sandbox_submit_warnings(&config))
                .chain(self.gated_egress_submit_warnings(&config))
                .chain(crate::task_plugins::container_security_warnings(&config))
                .collect(),
        })
    }

    /// The node OS a task's `auto` is explained against when no placement is
    /// in hand (`net policy show` and `test`): Linux when any node in the
    /// cluster runs Linux, which is where an unset container mode is gated
    /// (worker-plane-auth plan §6.12).
    fn cluster_node_os(&self) -> Option<&'static str> {
        let nodes = self.store.get_nodes().ok()?;
        nodes
            .iter()
            .any(|node| {
                node.fingerprint_attrs
                    .get(crate::structs::network::ATTR_NODE_OS)
                    .and_then(Value::as_str)
                    == Some("linux")
            })
            .then_some("linux")
    }

    /// `gated-default` warnings for a submit or a plan (worker-plane-auth
    /// plan §6.12): a task that would run gated with nothing allowing its
    /// egress. A stored policy object with an egress chain whose selector
    /// matches the task counts as coverage. Its `nodePools` are ignored,
    /// because the pool is not known until placement, so a pool-scoped
    /// object silences the warning rather than raising a false one.
    fn gated_egress_submit_warnings(&self, config: &JobConfig) -> Vec<String> {
        let policies = self.store.list_net_policies().unwrap_or_default();
        crate::structs::network::gated_egress_warnings(config, |task| {
            let facts = crate::structs::netpolicy::NetSelectorFacts {
                node_pool: None,
                namespace: String::new(),
                runtime: task.runtime.clone(),
                job: config.name.clone(),
                labels: task
                    .network
                    .as_ref()
                    .map(|network| network.labels.clone())
                    .unwrap_or_default(),
            };
            policies.iter().any(|record| {
                if record.object.egress.is_none() {
                    return false;
                }
                let mut selector = record.object.selector.clone();
                selector.node_pools.clear();
                selector.matches(&facts)
            })
        })
    }

    /// Warn at submit when a job's priority band cannot do what its author
    /// almost certainly expects.
    ///
    /// This is Nomad's biggest preemption ergonomics failure inverted. There,
    /// `priority` sits in the jobspec doing nothing until an operator flips a
    /// cluster API most users never find, and "preemption not working" grew
    /// its own troubleshooting genre. Here the field always orders the eval
    /// queue — but if it *cannot* displace running work, the submit says so
    /// once, at the moment the expectation is formed.
    fn band_submit_warnings(&self, config: &JobConfig) -> Vec<String> {
        // Only a band that could displace something has an expectation to
        // disappoint. `normal` and below are the default posture.
        if !config.priority.outranks(crate::structs::Band::default()) {
            return Vec::new();
        }
        let settings = match Scheduler::new(&self.store) {
            Ok(scheduler) => scheduler.preemption_settings().clone(),
            // A scheduler that will not build is a much louder problem than a
            // missing advisory line; the submit itself already succeeded.
            Err(_) => return Vec::new(),
        };
        match settings.mode {
            crate::manager::scheduler::PreemptionMode::Off => vec![format!(
                "job {} is priority `{}`, but preemption is off for this cluster \
                 (schedulerSettings.preemption.mode). The band will order the eval queue and \
                 cannot displace running work. See docs/preemption-and-eviction.md",
                config.name, config.priority
            )],
            crate::manager::scheduler::PreemptionMode::Observe => vec![format!(
                "job {} is priority `{}`, and preemption is in observe mode: victims will be \
                 identified and journalled (`allocation.preempt_planned`) but nothing is \
                 evicted, so the band cannot displace running work yet. See \
                 docs/preemption-and-eviction.md",
                config.name, config.priority
            )],
            crate::manager::scheduler::PreemptionMode::Enforce
                if !settings.disabled_pools.is_empty() =>
            {
                vec![format!(
                    "job {} is priority `{}`; preemption is enforced but disabled in pool(s) {}. \
                     Tasks constrained to those pools cannot displace lower-band work",
                    config.name,
                    config.priority,
                    settings.disabled_pools.join(", ")
                )]
            }
            crate::manager::scheduler::PreemptionMode::Enforce => Vec::new(),
        }
    }

    /// A template run may only create new jobs or update jobs whose latest
    /// config came from the same template — unless the caller also holds
    /// plain `job:write` on the job name. Without this, any template whose
    /// rendered job name is user-influenced would let a template-only
    /// identity overwrite unrelated jobs.
    fn enforce_template_collision_guard(
        &self,
        identity: &AclIdentity,
        job_name: &str,
        template: &JobTemplateProvenance,
    ) -> Result<(), Status> {
        let exists = self
            .store
            .get_job_config_optional(job_name)
            .map_err(|e| map_store_error("Error checking existing job", e))?
            .is_some();
        if !exists {
            return Ok(());
        }
        let provenance = self
            .store
            .job_template_provenance(job_name)
            .map_err(|e| map_store_error("Error loading job provenance", e))?;
        if provenance
            .as_ref()
            .is_some_and(|provenance| provenance.name == template.name)
        {
            return Ok(());
        }
        // Plain job:write (no template attribute) may still update it.
        if identity.allows(AclResource::Job, AclAction::Write, &AclScope::job(job_name)) {
            return Ok(());
        }
        Err(Status::failed_precondition(format!(
            "job '{job_name}' already exists and was not created from template \
             '{}'; updating it through this template requires job:write on the job",
            template.name
        )))
    }

    /// Join gate for worker registration: when ACLs are enforced, callers
    /// prove membership with a join token secret unless they hold a
    /// management credential or the node token of the node they register as.
    /// Audit-only mode logs failures but lets registration proceed (the
    /// migration ramp for existing fleets).
    ///
    /// The returned grant says what the caller may register as: a join token
    /// pins its worker type (and so its pool), and a node token pins its node.
    fn verify_join_for_registration(
        &self,
        identity: &AclIdentity,
        join_token: Option<&str>,
    ) -> Result<RegistrationGrant, Status> {
        if !self.auth.enabled || identity.is_management {
            return Ok(RegistrationGrant::Unrestricted);
        }
        // A node token proves only its own node. Without a join secret it
        // cannot introduce any other node (that was a takeover path: register
        // as a peer, rotate away the peer's token, mint its credentials).
        if let Some(node_id) = acl::worker_node_id(identity) {
            if join_token.is_none_or(|secret| secret.trim().is_empty()) {
                return Ok(RegistrationGrant::Node(node_id.to_string()));
            }
        }
        match self.auth.verify_join_secret(&self.store, join_token) {
            Ok(record) => Ok(RegistrationGrant::JoinToken(record)),
            Err(status) if self.auth.audit_only => {
                warn!(
                    target: "acl_audit",
                    resource = "worker-plane",
                    action = "register",
                    enforced = false,
                    "join token verification failed (audit-only): {}",
                    status.message()
                );
                Ok(RegistrationGrant::Unrestricted)
            }
            Err(status) => Err(status),
        }
    }

    /// Enforces a registration grant against the requested worker type
    /// (pre-registration) or node name and pool (registration). Audit-only
    /// mode logs the mismatch and allows it, like every other worker-plane
    /// check.
    fn enforce_registration_grant(
        &self,
        grant: &RegistrationGrant,
        worker_type: Option<&str>,
        node: Option<(&str, &str)>,
    ) -> Result<(), Status> {
        let refusal = match grant {
            RegistrationGrant::Unrestricted => None,
            RegistrationGrant::JoinToken(record) => {
                if let Some(requested) = worker_type.filter(|t| *t != record.worker_type) {
                    Some(format!(
                        "this join token is pinned to worker type {:?}, not {requested:?}",
                        record.worker_type
                    ))
                } else if let Some((_, pool)) = node {
                    let pinned = self
                        .store
                        .get_worker_type(record.worker_type.clone())
                        .map_err(|e| map_store_error("Error loading worker type", e))?;
                    (pool != pinned.pool).then(|| {
                        format!(
                            "this join token is pinned to worker type {:?}, whose pool is \
                             {:?}, not {pool:?}",
                            record.worker_type, pinned.pool
                        )
                    })
                } else {
                    None
                }
            }
            RegistrationGrant::Node(own) => match node {
                Some((name, _)) if name != own => Some(format!(
                    "the node token for {own:?} may not register node {name:?}; \
                     present a join token instead"
                )),
                Some((name, pool)) => match self.store.get_node(name) {
                    Ok(existing) if existing.pool != pool => Some(format!(
                        "node {name:?} is registered in pool {:?}; a node token may not \
                         move it to {pool:?}",
                        existing.pool
                    )),
                    _ => None,
                },
                None => None,
            },
        };
        let Some(message) = refusal else {
            return Ok(());
        };
        if self.auth.audit_only {
            warn!(
                target: "acl_audit",
                resource = "worker-plane",
                action = "register",
                enforced = false,
                "registration outside its grant (audit-only): {message}"
            );
            return Ok(());
        }
        Err(Status::permission_denied(message))
    }

    /// Scope check against a rollout's job, used by rollout endpoints that
    /// only carry a numeric rollout ID.
    fn enforce_rollout_job(
        &self,
        identity: &AclIdentity,
        action: AclAction,
        rollout_id: i64,
    ) -> Result<(), Status> {
        let rollout = self
            .store
            .get_rollout(rollout_id)
            .map_err(|e| map_store_error("Error loading rollout", e))?;
        match rollout {
            Some(rollout) => acl::enforce(
                identity,
                AclResource::Job,
                action,
                &AclScope::job(&rollout.job_id),
            ),
            // Unknown rollout: require coarse job access; handlers then
            // produce their normal not-found/empty replies.
            None => acl::enforce(identity, AclResource::Job, action, &AclScope::any()),
        }
    }

    /// Service wired with cluster config (artifact admission settings + node
    /// identity names). `run_manager` validates the config before this.
    pub fn with_manager_config(
        store: Store,
        config: &crate::structs::config::ManagerConfigFull,
    ) -> Self {
        let mut service = Self::new(store);
        service.artifacts_config = config.artifacts.clone();
        service.node_identity_names = config
            .workload_identity
            .node_identities
            .iter()
            .map(|identity| identity.name.clone())
            .collect();
        service.exec_enabled = config.exec.enabled;
        service.exec_tickets = config.exec.tickets.clone();
        service.heartbeat = config.heartbeat.clone();
        service.eval_threads = config.eval_threads.max(1);
        service
    }

    /// Attach the workload-identity issuer (only constructed when the
    /// feature is enabled in the manager config).
    pub fn with_workload_identity(mut self, state: Option<Arc<WorkloadIdentityState>>) -> Self {
        self.workload_identity = state;
        self
    }

    /// Attach the authenticator built from `acl` config (defaults to ACLs
    /// disabled via `new`).
    pub fn with_authenticator(mut self, auth: Arc<Authenticator>) -> Self {
        self.auth = auth;
        self
    }

    /// Sets the address join tokens point workers at.
    pub fn with_advertise_addr(mut self, advertise_addr: impl Into<String>) -> Self {
        self.advertise_addr = advertise_addr.into();
        self
    }

    /// The usage table heartbeats fill, for the idle sweep the maintenance
    /// loop runs (docs/suspend-and-resume.md, "Idle policy").
    pub fn usage_handle(&self) -> Arc<Mutex<crate::manager::usage::UsageTable>> {
        self.usage.clone()
    }

    /// Attach the cluster's edge-boot settings (docs/edge-boot.md).
    pub fn with_edge_boot(mut self, edge_boot: crate::structs::EdgeBootConfigFull) -> Self {
        self.edge_boot = edge_boot;
        self
    }

    /// Render a committed allocation as the dispatch payload a node needs to
    /// finish a ratified claim. `None` when anything it depends on is already
    /// gone, which the caller reports rather than papering over.
    fn committed_payload(
        &self,
        allocation_id: i64,
        claim_id: &str,
        gate: i32,
    ) -> Option<crate::structs::TaskAllocation> {
        let allocation = self
            .store
            .get_allocation_by_id(allocation_id)
            .ok()
            .flatten()?;
        let task = self
            .store
            .get_task_config_by_id(allocation.task_config_id)
            .ok()
            .flatten()?;
        let instance = match allocation.instance_id.as_deref() {
            Some(instance_id) => self.store.get_instance(instance_id).ok().flatten(),
            None => None,
        };
        let volumes = self
            .store
            .volume_mounts_for_allocation(&allocation.job_id, &task, instance.as_ref())
            .ok()?;
        let mut payload = crate::manager::dispatcher::build_task_allocation(
            &task,
            &allocation,
            instance.as_ref(),
            &volumes,
        )
        .ok()?;
        payload.provisional = Some(crate::structs::ProvisionalAllocation {
            claim_id: claim_id.to_string(),
            gate,
            lease_deadline_unix: 0,
            instance_id: allocation.instance_id.clone().unwrap_or_default(),
            origin: "edge".into(),
            on_refused: String::new(),
            ticket_jti: String::new(),
            session: None,
        });
        Some(payload)
    }

    /// The archive manifest of a `manifestOnly` instance with no row, for
    /// the instance handlers' NotFound path. A contentStore that could not
    /// be read is `Unavailable`, not NotFound: the instance may well exist.
    async fn find_manifest_only(
        &self,
        instance_id: &str,
    ) -> Result<Option<(crate::structs::archive::ArchiveManifest, String)>, Status> {
        archive::find_manifest_only(&self.store, instance_id)
            .await
            .map_err(|err| Status::unavailable(format!("{err:#}")))
    }

    /// The live, unratified claim for an instance, and the node holding it.
    ///
    /// Reads the same in-memory per-node reports `legion node claims` serves,
    /// so it inherits the same freshness contract: current within one
    /// heartbeat, empty (never wrong) after a manager failover.
    fn provisional_claim_for(
        &self,
        instance_id: &str,
    ) -> (Option<crate::structs::ProvisionalClaimReport>, String) {
        let Ok(guard) = self.provisional_claims.lock() else {
            return (None, String::new());
        };
        for (node_id, claims) in guard.iter() {
            if let Some(claim) = claims.iter().find(|claim| claim.instance_id == instance_id) {
                return (Some(claim.clone()), node_id.clone());
            }
        }
        (None, String::new())
    }

    /// Verdicts for claims a node is still holding that this manager has
    /// already decided (`HeartbeatReply.claim_verdicts`).
    ///
    /// The `RatifyClaim` reply is the primary channel; this covers the case
    /// where that reply was lost after the manager committed. Without it the
    /// node would kill work the cluster believes is running, and the instance
    /// would stay bound to a dead allocation until the next reschedule.
    ///
    /// The lookup is the instance binding itself, so it needs no new state:
    /// an instance bound to an allocation on *this* node means confirmed, and
    /// one bound elsewhere means this node lost the race.
    fn claim_verdicts_for(
        &self,
        node_id: &str,
        claims: &[crate::structs::ProvisionalClaimReport],
    ) -> Vec<crate::structs::ClaimVerdict> {
        let mut verdicts = Vec::new();
        for claim in claims {
            if claim.instance_id.trim().is_empty() {
                continue;
            }
            let Ok(Some(instance)) = self.store.get_instance(&claim.instance_id) else {
                continue;
            };
            let Some(allocation_id) = instance.current_alloc_id else {
                continue;
            };
            let Ok(Some(allocation)) = self.store.get_allocation_by_id(allocation_id) else {
                continue;
            };
            if allocation.node_id == node_id {
                verdicts.push(crate::structs::ClaimVerdict {
                    claim_id: claim.claim_id.clone(),
                    confirmed: true,
                    allocation_id,
                    reason: 0,
                    detail: String::new(),
                });
            } else {
                verdicts.push(crate::structs::ClaimVerdict {
                    claim_id: claim.claim_id.clone(),
                    confirmed: false,
                    allocation_id: 0,
                    reason: crate::structs::edge_boot::RefusalReason::InstanceClaimed.to_wire(),
                    detail: format!("bound to {}", allocation.node_id),
                });
            }
        }
        verdicts
    }

    /// Admission normalization for artifacts (artifact system plan §§1,6,7,8):
    /// desugars kind-implicit artifacts (behind `containerImageDesugar`),
    /// enforces cluster-scoped rules (scheme allowlist, nodeIdentity refs),
    /// bakes the effective auth from cluster matchers, and pins mutable
    /// references at submit. Returns the pins performed (rendered into plan
    /// messages and emitted as `artifact.pinned` events on submit).
    ///
    /// Workers receive exactly this normalized list and never re-derive
    /// implicit artifacts or re-resolve pinned tags.
    fn normalize_job_artifacts(
        &self,
        config: &mut JobConfig,
        resolve_pins: bool,
    ) -> R<Vec<ArtifactPin>> {
        use crate::structs::ArtifactResolve;

        let mut notes = Vec::new();
        let mut registry = crate::task_plugins::TaskPluginRegistry::new();
        for task in &mut config.tasks {
            if self.artifacts_config.container_image_desugar {
                registry.load_plugin(&task.runtime, false)?;
                let plugin = registry
                    .plugin(&task.runtime)
                    .context("task plugin loaded but missing")?;
                if plugin.supports_artifacts() {
                    for implicit in plugin.implicit_artifacts(&task.config)? {
                        let already_declared = implicit.name.as_deref().is_some_and(|name| {
                            task.artifacts
                                .iter()
                                .any(|artifact| artifact.name.as_deref() == Some(name))
                        });
                        if !already_declared {
                            task.artifacts.push(implicit);
                        }
                    }
                }
            }
            if task.artifacts.is_empty() {
                continue;
            }

            // Re-validate after desugaring so implicit specs obey the same
            // rules as user-declared ones.
            task.validate_artifacts()?;
            crate::artifacts::auth::validate_artifacts_against_cluster(
                &task.name,
                &task.artifacts,
                &self.artifacts_config,
                &self.node_identity_names,
            )?;
            crate::artifacts::auth::apply_cluster_auth(&mut task.artifacts, &self.artifacts_config);

            if !resolve_pins {
                continue;
            }
            for spec in &mut task.artifacts {
                if spec.effective_resolve() != ArtifactResolve::Pin
                    || !spec.is_mutable_reference()?
                {
                    continue;
                }
                let resolved = self.resolve_pin(spec).with_context(|| {
                    format!(
                        "resolve artifact {} at submit failed; fix the reference or set \
                         resolve: \"always\" to re-resolve at staging instead",
                        spec.redacted_source()
                    )
                })?;
                notes.push(ArtifactPin {
                    task: task.name.clone(),
                    from: spec.redacted_source(),
                    to: resolved.digest,
                });
                spec.pinned_from = Some(spec.source.clone());
                spec.source = resolved.pinned_source;
            }
        }
        Ok(notes)
    }

    /// Admission check for `container` tasks' capabilities against the
    /// worker types' `container.allowCaps`.
    ///
    /// The real gate is on the worker, which refuses to start a container
    /// whose effective capability set leaves its node's allowlist. This is
    /// the cheap half of it: when *no* worker type allows a task's set, the
    /// task could never start anywhere, so the job is rejected at submit
    /// with the same advice instead of placing and failing. A set some
    /// worker type allows passes, even if placement then lands the task on a
    /// node of another type; that start fails on the node.
    fn validate_job_capabilities(&self, config: &JobConfig) -> R<()> {
        let mut requested = Vec::new();
        for task in &config.tasks {
            if let Some(caps) = crate::task_plugins::container_task_capabilities(task)? {
                requested.push((task.name.as_str(), caps));
            }
        }
        if requested.is_empty() {
            return Ok(());
        }
        let allowlists: Vec<(String, crate::task_plugins::CapabilityAllowlist)> = self
            .store
            .list_worker_types()?
            .into_iter()
            .map(|worker_type| {
                let allowlist =
                    crate::structs::WorkerContainerConfig::from_payload(&worker_type.container)
                        .and_then(|container| {
                            crate::task_plugins::CapabilityAllowlist::from_config(
                                container.allow_caps.as_deref(),
                            )
                        })
                        .unwrap_or_default();
                (worker_type.name, allowlist)
            })
            .collect();
        if allowlists.is_empty() {
            // No worker types configured: nothing to judge against.
            return Ok(());
        }
        for (task, caps) in requested {
            if allowlists
                .iter()
                .any(|(_, allowlist)| allowlist.refused(&caps).is_empty())
            {
                continue;
            }
            // Name what the most permissive type still refuses, so the
            // message points at the smallest change.
            let (_, closest) = allowlists
                .iter()
                .min_by_key(|(_, allowlist)| allowlist.refused(&caps).len())
                .expect("at least one worker type");
            let refused: Vec<String> = closest
                .refused(&caps)
                .iter()
                .map(|cap| cap.trim_start_matches("CAP_").to_string())
                .collect();
            let types: Vec<&str> = allowlists.iter().map(|(name, _)| name.as_str()).collect();
            bail!(
                "task {task} would run with capabilities no worker type allows container tasks \
                 to have: {refused}. An operator allows them with container.allowCaps in a \
                 worker type's config (worker types: {types}); otherwise take them out of the \
                 task (remove them from security.capabilities.add, or list them in \
                 security.capabilities.drop)",
                refused = refused.join(", "),
                types = types.join(", "),
            );
        }
        Ok(())
    }

    /// Admission check for volume declarations, against what the cluster
    /// actually provides.
    ///
    /// The manager loads no storage drivers, so "does plugin X exist" is only
    /// answerable from worker fingerprints — which is exactly why this lives at
    /// admission rather than in `validate_job_config` (which is offline and
    /// cluster-blind). Catching it here means a job with unavailable storage is
    /// rejected at submit with an actionable message, instead of placing and
    /// then failing at mount.
    fn validate_job_volumes(&self, config: &JobConfig) -> R<()> {
        let needs_check = config
            .tasks
            .iter()
            .any(|task| !task.required_volume_drivers().is_empty());
        if !needs_check {
            return Ok(());
        }
        let cluster = self.store.cluster_volume_drivers()?;
        if cluster.is_empty() {
            // Nothing in the cluster advertises plugin metadata: either no
            // worker has joined yet, or they predate fingerprinting. Either
            // way there is nothing to validate against, and rejecting the job
            // would be worse than letting placement decide.
            debug!("No volume driver metadata advertised yet; skipping volume admission checks");
            return Ok(());
        }

        let mut registry = crate::task_plugins::TaskPluginRegistry::new();
        for task in &config.tasks {
            let mut declarations: Vec<VolumeDeclaration> = Vec::new();
            if let Some(storage) = task
                .stateful
                .as_ref()
                .and_then(|stateful| stateful.storage.as_ref())
            {
                declarations.push(VolumeDeclaration {
                    label: "stateful.storage".to_string(),
                    driver: storage.driver_name().to_string(),
                    mode: storage.volume_mode(),
                    params: storage.param_map(),
                    is_instance_volume: true,
                });
            }
            for volume in &task.volumes {
                declarations.push(VolumeDeclaration {
                    label: format!("volume {:?}", volume.name),
                    driver: volume.driver_name().to_string(),
                    mode: volume.volume_mode(),
                    params: volume.param_map(),
                    is_instance_volume: false,
                });
            }
            if declarations.is_empty() {
                continue;
            }

            // What the task RUNTIME can consume. A microVM runtime takes a block
            // device and cannot use a directory; exec and container are the
            // reverse. Loaded lazily so jobs without volumes never pay for it.
            let runtime_modes = match registry.load_plugin(&task.runtime, false) {
                Ok(()) => registry
                    .plugin(&task.runtime)
                    .map(|plugin| plugin.supported_volume_modes().to_vec())
                    .unwrap_or_else(|| vec![crate::volumes::VolumeMode::Filesystem]),
                // An unknown runtime fails elsewhere with a better message; do not
                // turn "no such task runtime" into a confusing volume error.
                Err(_) => vec![crate::volumes::VolumeMode::Filesystem],
            };

            for VolumeDeclaration {
                label,
                driver,
                mode,
                params,
                is_instance_volume,
            } in declarations
            {
                if !runtime_modes.contains(&mode) {
                    bail!(
                        "task {} {label}: task runtime `{}` cannot consume a `{mode}` volume (it \
                         supports: {}). Filesystem volumes are directories a process reads; \
                         block volumes are devices a guest attaches",
                        task.name,
                        task.runtime,
                        runtime_modes
                            .iter()
                            .map(|mode| mode.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
                let Some(entry) = cluster.get(&driver) else {
                    bail!(
                        "task {} {label}: no node in the cluster provides volume driver `{driver}`.                          Install it under plugins/volumes/{driver}/ on a worker, or run                          `legion volume drivers` to see what is available",
                        task.name
                    );
                };
                if entry.available_nodes.is_empty() {
                    bail!(
                        "task {} {label}: volume driver `{driver}` is installed on {} but unusable                          everywhere: {}",
                        task.name,
                        entry.nodes.join(", "),
                        entry.capabilities.detail
                    );
                }
                // A published checkpoint (Phase 7b) resumes on another node,
                // which a node-local instance volume cannot follow. Built-in
                // drivers are refused offline; this catches plugin drivers
                // by what they advertise.
                if is_instance_volume
                    && entry.capabilities.node_local
                    && task
                        .suspend
                        .as_ref()
                        .and_then(|suspend| suspend.resolve().ok())
                        .is_some_and(|resolved| {
                            resolved.durability
                                == crate::structs::snapshot::SnapshotDurability::Store
                        })
                {
                    bail!(
                        "{}",
                        crate::structs::snapshot::store_durability_volume_error(
                            &task.name,
                            &driver,
                            "suspend.durability"
                        )
                    );
                }
                if !entry.capabilities.serves_mode(mode) {
                    bail!(
                        "task {} {label}: volume driver `{driver}` cannot serve a `{mode}` volume \
                         (it serves: {}). Pick a driver that does, or change the volume's `mode`",
                        task.name,
                        entry
                            .capabilities
                            .volume_modes
                            .iter()
                            .map(|mode| mode.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
                let missing = entry.capabilities.missing_params(&params);
                if !missing.is_empty() {
                    bail!(
                        "task {} {label}: volume driver `{driver}` requires params: {}",
                        task.name,
                        missing.join(", ")
                    );
                }
                // An instance volume promises data survives reschedules; an
                // ephemeral driver cannot keep that promise, and a job that
                // silently loses its database on every restart is the worst
                // possible failure mode.
                if is_instance_volume && entry.capabilities.ephemeral {
                    bail!(
                        "task {} {label}: volume driver `{driver}` is ephemeral (its contents do                          not survive an unmount), so it cannot back stateful.storage — which                          promises an instance's data survives reschedules. Declare it under                          `volumes` instead if you want scratch space",
                        task.name
                    );
                }
            }
        }
        Ok(())
    }

    /// Resolves one mutable reference to its digest using the same auth chain
    /// a worker would: KV credentials are read directly from the store the
    /// manager owns. Identity-authed resolution needs token issuance (WI plan
    /// phases 2/4); until then it fails with the documented suggestion.
    fn resolve_pin(
        &self,
        spec: &crate::structs::ArtifactSpec,
    ) -> R<crate::artifacts::fetcher::ResolvedIdentity> {
        use crate::artifacts::fetcher::{FetchLimits, FetchRequest};

        let parsed = crate::structs::parse_artifact_source(&spec.source)?;
        let credential = match spec.auth.as_ref() {
            None => None,
            Some(auth) => {
                if let Some(kv_path) = auth.kv.as_deref() {
                    let entry = self
                        .store
                        .get_kv(kv_path)?
                        .with_context(|| format!("credentials from kv {kv_path}: not found"))?;
                    Some(crate::artifacts::Credential::from_json(&entry.value)?)
                } else {
                    bail!(
                        "identity-authenticated sources cannot be resolved manager-side yet \
                         (workload-identity issuance is not wired); set resolve: \"always\""
                    );
                }
            }
        };

        let fetcher = self
            .fetcher_registry
            .lock()
            .expect("fetcher registry")
            .fetcher(&parsed.scheme)?;
        let limits = FetchLimits::default();
        let redacted = spec.redacted_source();
        let request = FetchRequest {
            scheme: &parsed.scheme,
            url: &parsed.url,
            redacted_source: &redacted,
            checksum: spec.checksum.as_deref(),
            media_type: spec.media_type.as_deref(),
            credential: credential.as_ref(),
            limits: &limits,
        };
        fetcher
            .resolve(&request)?
            .with_context(|| format!("{redacted} has no resolvable identity"))
    }

    /// Set the debounce window for KV-driven job reconciliation
    /// (`kvDebounceMs`). Zero disables debouncing.
    pub fn with_kv_debounce(mut self, window: Duration) -> Self {
        self.kv_debounce = window;
        self
    }

    /// Defer a KV event into the debounce window; returns false when
    /// debouncing is disabled and the caller must reconcile synchronously
    /// (errors then surface on the KV RPC).
    ///
    /// With a non-zero window the event is coalesced into the pending batch
    /// (per key, last event wins — so a delete after a put still resolves
    /// through `onDelete`) and a single flush task applies the whole batch
    /// after the window elapses. The first event opens the window; later
    /// events join the open window without extending it, so reconciliation
    /// latency is bounded by one window. Events are only ever coalesced,
    /// never dropped: everything recorded before the flush drains the map is
    /// part of the flushed batch, and anything arriving after the drain
    /// opens the next window. Flush errors are logged (there is no caller to
    /// surface them to). Events lost to a flush error, a crash, or a
    /// leadership change inside the window are recovered from replicated
    /// state by the control-lane leader's periodic
    /// [`sweep_missed_kv_changes`].
    fn queue_kv_change(&self, key: &str, is_delete: bool) -> bool {
        if self.kv_debounce.is_zero() {
            return false;
        }

        let mut pending = self.kv_pending.lock().expect("kv debounce state");
        pending.events.insert(key.to_string(), is_delete);
        if pending.flush_scheduled {
            return true;
        }
        pending.flush_scheduled = true;
        drop(pending);

        let store = self.store.clone();
        let state = Arc::clone(&self.kv_pending);
        let window = self.kv_debounce;
        tokio::spawn(async move {
            tokio::time::sleep(window).await;
            let changes: Vec<(String, bool)> = {
                let mut pending = state.lock().expect("kv debounce state");
                pending.flush_scheduled = false;
                pending.events.drain().collect()
            };
            if let Err(status) = reconcile_kv_changes(&store, &changes) {
                error!(
                    error = %status.message(),
                    keys = changes.len(),
                    "Debounced KV reconciliation failed; the leader's KV sweep will retry"
                );
            }
        });
        true
    }
}

fn cancel_active_rollouts_for_job(store: &Store, job_id: &str) -> Result<(), Status> {
    let active_rollouts = store
        .list_rollouts(Some(job_id), true)
        .map_err(|e| map_store_error("Error listing active rollouts", e))?;

    for rollout in active_rollouts {
        store
            .update_rollout_status(
                rollout.id,
                "cancelled",
                false,
                Some("Rollout superseded by newer job update"),
            )
            .map_err(|e| map_store_error("Error cancelling superseded rollout", e))?;
        store
            .append_rollout_event(
                rollout.id,
                &rollout.job_id,
                None,
                "warn",
                "Rollout cancelled because a newer job config was submitted",
                Some(&json!({ "status": "cancelled", "reason": "superseded" }).to_string()),
            )
            .map_err(|e| map_store_error("Error writing rollout cancel event", e))?;
    }

    Ok(())
}

fn remove_stale_allocations_for_new_rollout(
    store: &Store,
    job_id: &str,
    new_job_config_id: &str,
    previous_job_config_id: Option<&str>,
) -> Result<(), Status> {
    let keep = std::iter::once(new_job_config_id)
        .chain(previous_job_config_id)
        .collect::<HashSet<_>>();
    let allocations = store
        .allocs_for_job(job_id)
        .map_err(|e| map_store_error("Error loading job allocations", e))?;

    for allocation in allocations {
        if keep.contains(allocation.job_config_id.as_str()) {
            continue;
        }
        // Stateful allocations (instance-bound) are never bulk-removed here:
        // they may be the deliberate remainder of a staged rollout
        // (`rollout.slots.updateFrom`), where low slots keep running an
        // older config until a later resubmit lowers the floor. The new
        // rollout sees them as old allocations and replaces them with the
        // configured ordering and health gating instead of an abrupt stop.
        if allocation.instance_id.is_some() {
            continue;
        }
        store
            .remove_allocation_by_id_because(allocation.id, StopCause::Update)
            .map_err(|e| map_store_error("Error removing stale allocation", e))?;
    }

    Ok(())
}

/// Create the rollouts that acknowledged updates never got (plan §8, D1).
///
/// An `Updated` submit whose scheduler-lane rollout creation was rejected
/// (divergent lane leadership) or lost to a crash leaves a job whose latest
/// config is acknowledged but will never deploy: the client's retry returns
/// `Unchanged` — which creates no rollout — and eval regeneration's
/// `job_submit` fallback would place the new config at FULL count,
/// bypassing canary/staged gating and `waiting_manual_start`. This pass
/// runs on the scheduler-lane leader (node-status-checker cadence, and
/// immediately before every `regenerate_scheduling_evals` call so the
/// fallback can never outrun it), finds the acknowledged-update signature
/// via `Store::jobs_with_unrolled_latest_config`, and creates each missing
/// rollout exactly as `run_job` would have — manual strategies wait for
/// their operator, automatic ones are armed by the regeneration pass's
/// rollout branch on the same tick. Returns how many rollouts were created.
pub(crate) fn reconcile_missing_rollouts(store: &Store) -> anyhow::Result<u64> {
    let candidates = store.jobs_with_unrolled_latest_config()?;
    let mut created = 0u64;
    for (job_id, latest_config_id, previous_config_id) in candidates {
        let Some(config) = store.get_job_config_optional(&job_id)? else {
            continue;
        };
        // The candidate query keys on each job's latest config; re-check
        // against the loaded config so a submit racing this pass cannot
        // create a rollout for a superseded config.
        if config.id() != latest_config_id {
            continue;
        }
        match create_rollout_for_update(
            store,
            &job_id,
            previous_config_id.as_deref(),
            &latest_config_id,
            &config,
            "Error creating reconciled rollout",
        ) {
            Ok(is_manual) => {
                created += 1;
                info!(
                    job_id,
                    job_config_id = %latest_config_id,
                    manual = is_manual,
                    "Created the rollout an acknowledged update was missing"
                );
            }
            Err(status) => warn!(
                job_id,
                job_config_id = %latest_config_id,
                error = %status,
                "Failed creating missing rollout; will retry next pass"
            ),
        }
    }
    Ok(created)
}

fn create_rollout_for_update(
    store: &Store,
    job_id: &str,
    previous_job_config_id: Option<&str>,
    new_job_config_id: &str,
    config: &JobConfig,
    create_error: &str,
) -> Result<bool, Status> {
    cancel_active_rollouts_for_job(store, job_id)?;
    remove_stale_allocations_for_new_rollout(
        store,
        job_id,
        new_job_config_id,
        previous_job_config_id,
    )?;

    let launch = build_rollout_launch_config(config)?;
    let status = if launch.is_manual {
        "waiting_manual_start"
    } else {
        "pending"
    };
    let rollout = store
        .create_rollout(
            job_id,
            previous_job_config_id,
            new_job_config_id,
            status,
            launch.strategy.as_deref(),
            launch.progression.as_deref(),
            launch.replacement.as_deref(),
            launch.in_place,
            &launch.config_json,
        )
        .map_err(|e| map_store_error(create_error, e))?;
    store
        .append_rollout_event(
            rollout.id,
            &rollout.job_id,
            None,
            "info",
            if launch.is_manual {
                "Rollout created; waiting for manual start"
            } else {
                "Rollout started"
            },
            Some(
                &json!({
                    "status": rollout.status,
                    "progression": rollout.progression,
                })
                .to_string(),
            ),
        )
        .map_err(|e| map_store_error("Error writing rollout event", e))?;

    Ok(launch.is_manual)
}

/// Which allocations a `fail` action should stamp `failed:missing_kv_dependency`.
///
/// Injection edges name the task they belong to, so a missing key only impacts
/// that task's allocations. Legacy `$key` jobspec references are positional and
/// not scoped to one task config (a missing one can no longer render the whole
/// config), so they fail the whole job. `WholeJob` therefore subsumes `Tasks`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum KvFailScope {
    /// No edge resolved to `fail`.
    None,
    /// Only injection edges on these task names failed.
    Tasks(std::collections::BTreeSet<String>),
    /// A legacy `$` reference failed: the whole job is impacted.
    WholeJob,
}

impl KvFailScope {
    fn add_task(&mut self, task: &str) {
        match self {
            KvFailScope::None => {
                let mut set = std::collections::BTreeSet::new();
                set.insert(task.to_string());
                *self = KvFailScope::Tasks(set);
            }
            KvFailScope::Tasks(set) => {
                set.insert(task.to_string());
            }
            KvFailScope::WholeJob => {}
        }
    }

    fn add_whole_job(&mut self) {
        *self = KvFailScope::WholeJob;
    }
}

/// Resolve the coalesced action for one job given a batch of KV changes
/// (`key -> latest event was a delete`). Returns the strongest action across
/// all matching dependency edges plus whether any matched edge saw a delete
/// (delete-triggered restart/rollout must fire even when the re-rendered
/// config is byte-identical, because the rendered value cannot change when
/// the key is gone).
///
/// Edge semantics:
/// - Legacy `$key` jobspec references are implicitly watched: puts roll out
///   (historical behavior) and deletes fail the job (the reference is
///   required and can no longer be rendered).
/// - Injection edges resolve their entry policy against the task defaults
///   (`KvInjectionPolicy::resolve_with_defaults`) and are skipped unless
///   `watch` resolves to true.
fn resolve_job_kv_action(
    store: &Store,
    config: &JobConfig,
    job_refs: &[JobKvReference],
    changed: &HashMap<String, bool>,
) -> R<(KvInjectionAction, bool, KvFailScope)> {
    let mut action = KvInjectionAction::None;
    let mut delete_matched = false;
    let mut fail_scope = KvFailScope::None;

    for reference in job_refs {
        if reference.json_pointer.contains("/kvInjections/") {
            continue;
        }
        let Some(&is_delete) = changed.get(&reference.kv_key) else {
            continue;
        };
        let selected = if is_delete {
            delete_matched = true;
            fail_scope.add_whole_job();
            KvInjectionAction::Fail
        } else {
            KvInjectionAction::Rollout
        };
        action = strongest_action(action, selected);
    }

    for task in &config.tasks {
        let Some(injections) = task.kv_injections.as_ref() else {
            continue;
        };
        let defaults = injections.kv.clone().unwrap_or_default();
        let edges = injections
            .env
            .iter()
            .map(|env| (&env.kv, &env.value))
            .chain(injections.files.iter().map(|file| (&file.kv, &file.file)));
        for (policy, source) in edges {
            let policy = policy.resolve_with_defaults(&defaults);
            if !policy.watch {
                continue;
            }
            let deps = template_source_deps(store, source)
                .context("resolving kv injection dependencies")?;
            for dep in deps {
                let Some(&is_delete) = changed.get(&dep) else {
                    continue;
                };
                let selected = if is_delete {
                    delete_matched = true;
                    policy.on_delete
                } else {
                    policy.on_change
                };
                if selected == KvInjectionAction::Fail {
                    fail_scope.add_task(&task.name);
                }
                action = strongest_action(action, selected);
            }
        }
    }

    Ok((action, delete_matched, fail_scope))
}

/// Apply a batch of KV changes to every job that references one of the
/// changed keys. `changes` is a list of `(key, is_delete)` events; within
/// the batch the last event per key wins (the batch is reconciled against
/// the *current* KV state, so this only matters for choosing between
/// `onChange` and `onDelete`).
///
/// Per affected job:
/// 1. Resolve the strongest action across all matching dependency edges
///    (`none < restart < rollout < fail`).
/// 2. `fail`: mark the job + allocations failed with reason
///    `missing_kv_dependency` and stop — no re-render, no rollout, no eval.
///    Dependency edges are retained so a later put can recover the job.
/// 3. Otherwise re-materialize the config from current KV state (deleted
///    keys keep their last rendered value), store it, refresh the dependency
///    edges, and schedule the resolved action. A previously failed job is
///    flipped back to `done` when a restart/rollout actively recovers it.
fn reconcile_kv_changes(store: &Store, changes: &[(String, bool)]) -> Result<(), Status> {
    if changes.is_empty() {
        return Ok(());
    }

    // Last event per key wins within the batch.
    let mut changed: HashMap<String, bool> = HashMap::new();
    for (key, is_delete) in changes {
        changed.insert(key.clone(), *is_delete);
    }

    let mut jobs_to_update: HashMap<String, String> = HashMap::new();
    for key in changed.keys() {
        let refs = store
            .job_kv_references_for_key(key)
            .map_err(|e| map_store_error("Error loading kv references", e))?;
        for reference in refs {
            jobs_to_update
                .entry(reference.job_id)
                .or_insert(reference.job_config_id);
        }
    }

    for (job_id, job_config_id) in jobs_to_update {
        let job_refs = store
            .job_kv_references_for_job(&job_id)
            .map_err(|e| map_store_error("Error loading job kv references", e))?;
        if job_refs.is_empty() {
            continue;
        }

        // A stopped job schedules nothing: a watched-key delete must NOT flip
        // it to `failed`, and a put must NOT re-place its (gone) allocations.
        // We still refresh the stored rendered config below for non-stopped
        // jobs; for a stopped one we skip entirely — there are no allocations
        // to act on and the config is re-rendered on the next `job run`.
        let job_status = store
            .job_status_by_id(&job_id)
            .map_err(|e| map_store_error("Error loading job status", e))?;
        if job_status.as_deref() == Some("stopped") {
            debug!(
                job_id = %job_id,
                "Skipping KV reconcile for stopped job"
            );
            continue;
        }

        let current_config = store
            .get_job_config_by_id(&job_config_id)
            .map_err(|e| map_store_error("Error loading job config", e))?;

        let (action, delete_matched, fail_scope) =
            resolve_job_kv_action(store, &current_config, &job_refs, &changed)
                .map_err(|e| map_store_error("Error resolving kv change action", e))?;

        if action == KvInjectionAction::Fail {
            let impacted_tasks = match &fail_scope {
                // Defensive: an action of Fail always sets a scope. Treat an
                // unexpected None as whole-job rather than stamping nothing.
                KvFailScope::None | KvFailScope::WholeJob => None,
                KvFailScope::Tasks(tasks) => Some(tasks.iter().cloned().collect::<Vec<_>>()),
            };
            store
                .mark_job_failed_for_kv(
                    &job_id,
                    MISSING_KV_DEPENDENCY_REASON,
                    impacted_tasks.as_deref(),
                )
                .map_err(|e| map_store_error("Error failing job for missing kv dependency", e))?;
            warn!(
                job_id = %job_id,
                reason = MISSING_KV_DEPENDENCY_REASON,
                "Watched KV key deleted with onDelete=fail; job marked failed"
            );
            // Keep the dependency edges: a later put to the missing key is
            // the recovery path (it resolves through onChange).
            continue;
        }

        let (updated_config, injection_refs) = apply_kv_references_to_config(
            store,
            &current_config,
            &job_refs,
            MissingKvBehavior::KeepExisting,
        )
        .map_err(|e| map_store_error("Error applying kv references", e))?;
        let submissions = store
            .get_submissions_for_config(&job_config_id)
            .map_err(|e| map_store_error("Error loading job submissions", e))?;
        let submit_outcome = store
            .handle_job_submit(&updated_config, &submissions)
            .map_err(|e| map_store_error("Error updating job for kv change", e))?;
        let new_job_config_id = submit_outcome.job_config_id().to_string();

        // Refresh dependency edges: legacy `$` reference pointers are
        // positional and survive as-is; injection edges are recomputed so
        // template content changes re-track their transitive keys.
        let mut updated_refs: Vec<JobKvReference> = job_refs
            .iter()
            .filter(|reference| !reference.json_pointer.contains("/kvInjections/"))
            .map(|reference| JobKvReference {
                job_id: job_id.clone(),
                job_config_id: new_job_config_id.clone(),
                kv_key: reference.kv_key.clone(),
                json_pointer: reference.json_pointer.clone(),
            })
            .collect();
        updated_refs.extend(injection_refs.into_iter().map(|reference| JobKvReference {
            job_id: job_id.clone(),
            job_config_id: new_job_config_id.clone(),
            kv_key: reference.key,
            json_pointer: reference.json_pointer,
        }));
        store
            .replace_job_kv_references(&job_id, &updated_refs)
            .map_err(|e| map_store_error("Error updating kv references", e))?;

        // An unchanged config needs no action for puts (idempotent rewrite),
        // but a delete-triggered restart/rollout still fires: the rendered
        // value cannot change when the key is gone.
        if submit_outcome.is_unchanged() && !delete_matched {
            continue;
        }

        // `update` cannot clear a KV failure: the stamps on the failed
        // allocations only go away when they are replaced. Recover the way
        // `restart` does.
        let action =
            if action == KvInjectionAction::Update && job_status.as_deref() == Some("failed") {
                KvInjectionAction::Restart
            } else {
                action
            };

        match action {
            KvInjectionAction::None => {}
            KvInjectionAction::Update => {
                // Repoint the running allocations at the re-rendered config
                // without stopping them; their workers rewrite the marked
                // files on the next heartbeat. Done here, synchronously,
                // rather than through a `job_submit` eval: until the
                // allocations move, the job matches the signature
                // `reconcile_missing_rollouts` repairs (a newest config
                // with no allocations and no rollout), and that pass would
                // turn this update into a rollout.
                let previous = store
                    .previous_job_config_id(&new_job_config_id)
                    .map_err(|e| map_store_error("Error loading previous job config", e))?;
                let in_place = match previous.as_deref() {
                    Some(previous) => store
                        .job_configs_schedule_equivalent(previous, &new_job_config_id)
                        .map_err(|e| map_store_error("Error comparing job configs", e))?,
                    None => false,
                };
                match previous {
                    Some(previous) if in_place => {
                        let moved = store
                            .migrate_allocations_to_job_config(&previous, &new_job_config_id)
                            .map_err(|e| {
                                map_store_error("Error updating allocations in place", e)
                            })?;
                        info!(
                            job_id = %job_id,
                            job_config_id = %new_job_config_id,
                            allocations = moved.len(),
                            "KV update applied in place"
                        );
                    }
                    // Defensive: a files-only re-render is always
                    // schedule-equivalent. If it ever is not, fall back to a
                    // rollout rather than strand the allocations.
                    _ => {
                        warn!(
                            job_id = %job_id,
                            "KV update changed scheduling inputs; rolling out instead"
                        );
                        let is_manual = create_rollout_for_update(
                            store,
                            &job_id,
                            previous.as_deref(),
                            &new_job_config_id,
                            &updated_config,
                            "Error creating rollout for kv update",
                        )?;
                        if !is_manual {
                            store
                                .enqueue_eval(
                                    "rollout_tick",
                                    Some(&job_id),
                                    Some(&new_job_config_id),
                                    None,
                                    Some(store.default_eval_priority),
                                )
                                .map_err(|e| {
                                    map_store_error("Error scheduling kv update rollout", e)
                                })?;
                        }
                    }
                }
            }
            KvInjectionAction::Restart => {
                store
                    .clear_job_failed_status(&job_id)
                    .map_err(|e| map_store_error("Error clearing failed job status", e))?;
                // Persist the restart intent in REPLICATED state before the
                // ack: the job_update eval below is leader-local and dies
                // with the leader, and an unchanged-config restart leaves no
                // other replicated trace for the failover regeneration pass
                // to find (see Store::request_job_restart).
                store
                    .request_job_restart(&job_id)
                    .map_err(|e| map_store_error("Error recording restart intent", e))?;
                store
                    .enqueue_eval(
                        "job_update",
                        Some(&job_id),
                        Some(&new_job_config_id),
                        None,
                        Some(store.default_eval_priority),
                    )
                    .map_err(|e| map_store_error("Error scheduling kv update restart", e))?;
            }
            KvInjectionAction::Rollout => {
                store
                    .clear_job_failed_status(&job_id)
                    .map_err(|e| map_store_error("Error clearing failed job status", e))?;
                let previous_job_config_id = store
                    .previous_job_config_id(&new_job_config_id)
                    .map_err(|e| map_store_error("Error loading previous job config", e))?;
                let is_manual = create_rollout_for_update(
                    store,
                    &job_id,
                    previous_job_config_id.as_deref(),
                    &new_job_config_id,
                    &updated_config,
                    "Error creating rollout for kv update",
                )?;

                if !is_manual {
                    store
                        .enqueue_eval(
                            "rollout_tick",
                            Some(&job_id),
                            Some(&new_job_config_id),
                            None,
                            Some(store.default_eval_priority),
                        )
                        .map_err(|e| map_store_error("Error scheduling kv update rollout", e))?;
                }
            }
            // Fail is fully handled above (before re-materialization).
            KvInjectionAction::Fail => unreachable!("fail action handled before materialization"),
        }
    }

    Ok(())
}

/// Safety net for KV-driven reconciliation: finds jobs whose stored config
/// no longer matches what their KV references render to now, or whose
/// `onDelete = "fail"` key is gone while the job is not failed, and
/// reconciles them as if the KV events had just arrived. Returns how many
/// jobs it reconciled.
///
/// Debounced KV events are held in memory on the manager that took the
/// write, so a flush error, a crash, or a leadership change inside the
/// window drops them. Everything this sweep compares is replicated, so the
/// control-lane leader recovers those changes on its next tick. A job that is
/// already up to date compares equal and costs one re-render.
///
/// What it cannot see: a missed delete whose policy is `restart` or
/// `rollout`. A deleted key keeps its last rendered value, so the stored
/// config looks current, and nothing replicated records that the delete
/// was never acted on.
pub(crate) fn sweep_missed_kv_changes(store: &Store) -> anyhow::Result<usize> {
    let mut reconciled = 0;
    for job_id in store.job_ids_with_kv_references()? {
        let refs = store.job_kv_references_for_job(&job_id)?;
        let Some(job_config_id) = refs.first().map(|r| r.job_config_id.clone()) else {
            continue;
        };
        // Same skip as reconcile_kv_changes: a stopped job acts on nothing.
        let status = store.job_status_by_id(&job_id)?;
        if status.as_deref() == Some("stopped") {
            continue;
        }
        let current = store.get_job_config_by_id(&job_config_id)?;

        let mut keys: Vec<String> = refs.iter().map(|r| r.kv_key.clone()).collect();
        keys.sort();
        keys.dedup();
        let mut present = Vec::new();
        let mut missing = Vec::new();
        for key in keys {
            if store.get_kv(&key)?.is_some() {
                present.push(key);
            } else {
                missing.push(key);
            }
        }

        let mut missed: Vec<(String, bool)> = Vec::new();
        // Missed put: re-render exactly as a reconcile would and compare the
        // way submit does, so "different" here means the reconcile would
        // store a new config rather than return Unchanged.
        let (rendered, _) =
            apply_kv_references_to_config(store, &current, &refs, MissingKvBehavior::KeepExisting)?;
        if !current.is_same_as(&store.normalize_job_config(&rendered)?) {
            missed.extend(present.iter().map(|key| (key.clone(), false)));
        }
        // Missed fail-on-delete: the key is gone but the job never failed.
        if !missing.is_empty() && status.as_deref() != Some("failed") {
            let deleted: HashMap<String, bool> =
                missing.iter().map(|key| (key.clone(), true)).collect();
            let (action, _, _) = resolve_job_kv_action(store, &current, &refs, &deleted)?;
            if action == KvInjectionAction::Fail {
                missed.extend(missing.iter().map(|key| (key.clone(), true)));
            }
        }
        if missed.is_empty() {
            continue;
        }
        warn!(
            job_id = %job_id,
            keys = missed.len(),
            "Job missed a KV-driven reconciliation; reconciling now"
        );
        reconcile_kv_changes(store, &missed)
            .map_err(|status| anyhow!("reconciling job {job_id}: {}", status.message()))?;
        reconciled += 1;
    }
    Ok(reconciled)
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct TaskKey {
    job_config_id: String,
    task_config_id: i64,
}

#[derive(Debug, Clone)]
struct AllocationFailureContext {
    job_id: String,
    job_config_id: String,
    task_config_id: i64,
    node_id: String,
    on_unhealthy: TaskOnUnhealthyConfig,
}

#[derive(Debug, Default)]
struct AllocationFailureTracker {
    allocations: HashMap<i64, AllocationFailureContext>,
    move_history: HashMap<TaskKey, Vec<OffsetDateTime>>,
}

#[tonic::async_trait]
impl Manager for ManagerDaemonService {
    #[instrument(skip_all)]
    async fn run_job(
        &self,
        request: Request<RunJobRequest>,
    ) -> Result<Response<RunJobReply>, Status> {
        // ACL: coarse gate here; exact job-name scope is enforced below once
        // the jobspec has been parsed.
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let js = inner.parsed_jobspec;
        let subs = inner.submissions;
        let priority = inner.priority;
        let idempotency_key =
            scoped_idempotency_key(&identity, "run_job", inner.idempotency_key.as_deref());

        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(RunJobReply {
                message,
                deduplicated: true,
            }));
        }

        let submitted =
            self.submit_rendered_job(&identity, &js, subs, priority, inner.force_reschedule, None)?;

        let message = submitted.message_with_warnings();
        // The lookup above only catches a duplicate that arrived after the
        // first one recorded. Two submits carrying the same key CONCURRENTLY
        // both miss it, and the record is `INSERT OR IGNORE` — so the loser
        // learns it lost here, and must say so. Reporting `deduplicated:
        // false` to both would tell a retrying agent it created two
        // submissions when the key exists precisely to promise it did not.
        // (Submitting the same spec twice is itself harmless — the second
        // resolves to `Unchanged` — so the loser is corrected, not failed.)
        let first = self
            .store
            .idempotency_record(&idempotency_key, "run_job", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        let (message, deduplicated) = if first {
            (message, false)
        } else {
            // The winner's recorded response is the authoritative one, and
            // is what a later retry of this key would be handed.
            let recorded = self
                .store
                .idempotency_lookup(&idempotency_key)
                .map_err(|e| map_store_error("Error checking idempotency", e))?;
            (recorded.unwrap_or(message), true)
        };

        Ok(Response::new(RunJobReply {
            message,
            deduplicated,
        }))
    }

    #[instrument(skip_all)]
    async fn upsert_job_template(
        &self,
        request: Request<UpsertJobTemplateRequest>,
    ) -> Result<Response<UpsertJobTemplateReply>, Status> {
        // ACL: coarse template:write gate before touching the body; the
        // precise name scope is enforced once the name is read.
        let identity = self.check_acl(
            &request,
            AclResource::Template,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let name = inner.name.trim().to_string();
        job_templates::validate_template_name(&name)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        acl::enforce(
            &identity,
            AclResource::Template,
            AclAction::Write,
            &AclScope::template(&name),
        )?;

        let manifest = job_templates::parse_manifest_bounded(inner.source_nickel.clone())
            .await
            .map_err(template_eval_status)?;

        // When every variable has a default the template is fully renderable
        // right now — trial-render it so authoring mistakes surface at
        // upsert instead of at the first submit.
        if manifest
            .variables
            .iter()
            .all(|variable| variable.default.is_some())
        {
            let values = job_templates::resolve_variable_values(&manifest.variables, "{}")
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let rendered = job_templates::render_bounded(&inner.source_nickel, &values)
                .await
                .map_err(|e| match template_eval_status(e) {
                    status if status.code() == tonic::Code::InvalidArgument => {
                        Status::invalid_argument(format!(
                            "trial render with defaults failed: {}",
                            status.message()
                        ))
                    }
                    status => status,
                })?;
            let config: JobConfig = serde_json::from_str(&rendered).map_err(|e| {
                Status::invalid_argument(format!(
                    "trial render with defaults did not produce a valid jobspec: {e}"
                ))
            })?;
            validate_job_config(&config).map_err(|e| {
                Status::invalid_argument(format!(
                    "trial render with defaults produced an invalid jobspec: {e}"
                ))
            })?;
        }

        let record = self
            .store
            .upsert_job_template_version(
                &name,
                manifest.description.as_deref(),
                &inner.source_nickel,
                &manifest.variables,
                &identity.principal,
            )
            .map_err(|e| map_store_error("Error storing template", e))?;
        info!(template = %name, version = record.version, "job template version stored");

        Ok(Response::new(UpsertJobTemplateReply {
            template: Some(job_template_to_proto(record)),
        }))
    }

    #[instrument(skip_all)]
    async fn list_job_templates(
        &self,
        request: Request<ListJobTemplatesRequest>,
    ) -> Result<Response<ListJobTemplatesReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Template,
            AclAction::Read,
            AclScope::any(),
        )?;
        let query = normalize_query(request.into_inner().query);

        let mut templates = self
            .store
            .list_job_templates()
            .map_err(|e| map_store_error("Error listing templates", e))?;
        templates.retain(|template| {
            identity.allows(
                AclResource::Template,
                AclAction::Read,
                &AclScope::template(&template.name),
            )
        });
        if let Some(query) = query {
            templates.retain(|template| {
                template.name.to_lowercase().contains(&query)
                    || template
                        .description
                        .as_deref()
                        .unwrap_or_default()
                        .to_lowercase()
                        .contains(&query)
            });
        }

        Ok(Response::new(ListJobTemplatesReply {
            templates: templates
                .into_iter()
                .map(|template| JobTemplateSummary {
                    name: template.name,
                    latest_version: template.latest_version,
                    description: template.description,
                    variable_count: template.variable_count,
                    updated_at: template.updated_at,
                })
                .collect(),
        }))
    }

    #[instrument(skip_all)]
    async fn get_job_template(
        &self,
        request: Request<GetJobTemplateRequest>,
    ) -> Result<Response<GetJobTemplateReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Template,
            AclAction::Read,
            AclScope::template(&request.get_ref().name),
        )?;
        let inner = request.into_inner();

        let record = self
            .store
            .get_job_template_version(&inner.name, inner.version)
            .map_err(|e| map_store_error("Error loading template", e))?;
        let versions = self
            .store
            .list_job_template_versions(&inner.name)
            .map_err(|e| map_store_error("Error loading template versions", e))?
            .into_iter()
            .map(|info| JobTemplateVersionInfo {
                version: info.version,
                created_at: info.created_at,
                created_by: info.created_by,
            })
            .collect();

        Ok(Response::new(GetJobTemplateReply {
            template: Some(job_template_to_proto(record)),
            versions,
        }))
    }

    #[instrument(skip_all)]
    async fn delete_job_template(
        &self,
        request: Request<DeleteJobTemplateRequest>,
    ) -> Result<Response<DeleteJobTemplateReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Template,
            AclAction::Write,
            AclScope::template(&request.get_ref().name),
        )?;
        let name = request.into_inner().name;

        self.store
            .delete_job_template(&name)
            .map_err(|e| map_store_error("Error deleting template", e))?;
        info!(template = %name, "job template deleted");

        Ok(Response::new(DeleteJobTemplateReply {
            message: format!("Deleted template {name}"),
        }))
    }

    #[instrument(skip_all)]
    async fn render_job_template(
        &self,
        request: Request<RenderJobTemplateRequest>,
    ) -> Result<Response<RenderJobTemplateReply>, Status> {
        // ACL: coarse job:write gate (template-scoped rules pass the
        // wildcard check); the precise rendered-name + template scope is
        // enforced after rendering, before anything is returned.
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();

        let template = self
            .store
            .get_job_template_version(&inner.name, inner.version)
            .map_err(|e| map_store_error("Error loading template", e))?;
        let values =
            job_templates::resolve_variable_values(&template.variables, &inner.variables_json)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let rendered = job_templates::render_bounded(&template.source_nickel, &values)
            .await
            .map_err(template_eval_status)?;

        let (config, _kv_refs) = resolve_jobspec_kv_references(&self.store, &rendered)
            .map_err(|e| Status::invalid_argument(format!("rendered jobspec is invalid: {e}")))?;
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Write,
            // Same band normalization as the submit path: a render whose
            // output the caller could not submit should fail here, not at the
            // submit that follows it.
            &AclScope::job_via_template(&config.name, &template.name).with_band(config.priority),
        )?;
        validate_job_config(&config).map_err(|e| {
            Status::invalid_argument(format!("rendered jobspec failed validation: {e}"))
        })?;

        Ok(Response::new(RenderJobTemplateReply {
            rendered_jobspec: rendered,
            job_name: config.name,
        }))
    }

    #[instrument(skip_all)]
    async fn run_job_template(
        &self,
        request: Request<RunJobTemplateRequest>,
    ) -> Result<Response<RunJobTemplateReply>, Status> {
        // ACL: coarse job:write gate (template-scoped rules pass the
        // wildcard check); the precise rendered-name + template scope is
        // enforced inside submit_rendered_job once the spec exists.
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let priority = inner.priority;
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "run_job_template",
            inner.idempotency_key.as_deref(),
        );

        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let record: TemplateRunRecord =
                serde_json::from_str(&recorded).unwrap_or(TemplateRunRecord {
                    message: recorded,
                    job_name: String::new(),
                    job_config_id: String::new(),
                });
            return Ok(Response::new(RunJobTemplateReply {
                message: record.message,
                deduplicated: true,
                job_name: record.job_name,
                job_config_id: record.job_config_id,
            }));
        }

        let template = self
            .store
            .get_job_template_version(&inner.name, inner.version)
            .map_err(|e| map_store_error("Error loading template", e))?;
        let values =
            job_templates::resolve_variable_values(&template.variables, &inner.variables_json)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let rendered = job_templates::render_bounded(&template.source_nickel, &values)
            .await
            .map_err(template_eval_status)?;

        // The template source is the primary submission so spec history
        // shows what was actually run; the resolved variable values ride
        // along for the audit trail (submit_rendered_job adds the rendered
        // JSON itself).
        let subs = vec![
            Submission {
                path: format!("template://{}/{}", template.name, template.version),
                content: template.source_nickel.clone(),
                is_primary: true,
            },
            Submission {
                path: "template.variables.json".into(),
                content: serde_json::to_string_pretty(&values).unwrap_or_else(|_| "{}".into()),
                is_primary: false,
            },
        ];
        let provenance = JobTemplateProvenance {
            name: template.name.clone(),
            version: template.version,
        };

        let submitted = self.submit_rendered_job(
            &identity,
            &rendered,
            subs,
            priority,
            inner.force_reschedule,
            Some(&provenance),
        )?;
        info!(
            template = %template.name,
            version = template.version,
            job = %submitted.job_name,
            "job submitted from template"
        );

        let record = TemplateRunRecord {
            message: submitted.message_with_warnings(),
            job_name: submitted.job_name.clone(),
            job_config_id: submitted.job_config_id.clone(),
        };
        self.store
            .idempotency_record(
                &idempotency_key,
                "run_job_template",
                &serde_json::to_string(&record).unwrap_or_else(|_| submitted.message.to_string()),
            )
            .map_err(|e| map_store_error("Error recording idempotency", e))?;

        Ok(Response::new(RunJobTemplateReply {
            message: submitted.message.into(),
            deduplicated: false,
            job_name: submitted.job_name,
            job_config_id: submitted.job_config_id,
        }))
    }

    #[instrument(skip_all)]
    async fn get_job_info(
        &self,
        request: Request<GetJobInfoRequest>,
    ) -> Result<Response<GetJobInfoReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Read,
            self.job_read_scope(&request.get_ref().name),
        )?;
        let name = request.into_inner().name;

        let mut job = self
            .store
            .get_job_with_status(&name)
            .map_err(|e| map_store_error("Error getting job", e))?;
        self.store
            .overlay_blocked_jobs(std::slice::from_mut(&mut job))
            .map_err(|e| map_store_error("Error loading blocked evals", e))?;
        let template = self
            .store
            .job_template_provenance(&name)
            .map_err(|e| map_store_error("Error loading job provenance", e))?;

        let reply = GetJobInfoReply {
            job: Some(job),
            template,
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn list_rollouts(
        &self,
        request: Request<ListRolloutsRequest>,
    ) -> Result<Response<ListRolloutsReply>, Status> {
        let identity =
            self.check_acl(&request, AclResource::Job, AclAction::Read, AclScope::any())?;
        let request = request.into_inner();
        let job_id = request
            .job_id
            .as_deref()
            .filter(|job| !job.trim().is_empty());
        let query = normalize_query(request.query);
        let sort_by = request.sort_by.unwrap_or_else(|| "updated_at".to_string());
        let sort_desc = request
            .sort_order
            .as_deref()
            .unwrap_or("desc")
            .eq_ignore_ascii_case("desc");
        let (page, page_size) = parse_page(request.page, request.page_size);

        let mut rollouts = self
            .store
            .list_rollouts(job_id, request.active_only)
            .map_err(|e| map_store_error("Error listing rollouts", e))?;
        rollouts.retain(|rollout| {
            identity.allows(
                AclResource::Job,
                AclAction::Read,
                &AclScope::job(&rollout.job_id),
            )
        });

        if let Some(query) = query {
            rollouts.retain(|rollout| {
                let id = rollout.id.to_string();
                let status = rollout.status.to_lowercase();
                let strategy = rollout.strategy.clone().unwrap_or_default().to_lowercase();
                id.contains(&query)
                    || rollout.job_id.to_lowercase().contains(&query)
                    || status.contains(&query)
                    || strategy.contains(&query)
            });
        }

        rollouts.sort_by(|a, b| {
            let ordering = match sort_by.as_str() {
                "created_at" => a.created_at.cmp(&b.created_at),
                "status" => a.status.cmp(&b.status),
                "job_id" => a.job_id.cmp(&b.job_id),
                _ => a.updated_at.cmp(&b.updated_at),
            };
            if sort_desc {
                ordering.reverse()
            } else {
                ordering
            }
        });

        let (rollouts, total_count) = paginate(rollouts, page, page_size);

        Ok(Response::new(ListRolloutsReply {
            rollouts,
            total_count,
            page: page as u32,
            page_size: page_size as u32,
        }))
    }

    #[instrument(skip_all)]
    async fn get_rollout(
        &self,
        request: Request<GetRolloutRequest>,
    ) -> Result<Response<GetRolloutReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let request = request.into_inner();
        self.enforce_rollout_job(&identity, AclAction::Read, request.id)?;
        let rollout = self
            .store
            .get_rollout(request.id)
            .map_err(|e| map_store_error("Error loading rollout", e))?;
        Ok(Response::new(GetRolloutReply { rollout }))
    }

    #[instrument(skip_all)]
    async fn get_rollout_status(
        &self,
        request: Request<GetRolloutStatusRequest>,
    ) -> Result<Response<GetRolloutStatusReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let request = request.into_inner();
        self.enforce_rollout_job(&identity, AclAction::Read, request.id)?;
        let rollout = self
            .store
            .get_rollout(request.id)
            .map_err(|e| map_store_error("Error loading rollout", e))?;
        let Some(rollout) = rollout else {
            return Ok(Response::new(GetRolloutStatusReply {
                rollout: None,
                groups: Vec::new(),
            }));
        };
        let groups = build_rollout_group_status(&self.store, &rollout)
            .map_err(|e| map_store_error("Error building rollout status", e))?;

        Ok(Response::new(GetRolloutStatusReply {
            rollout: Some(rollout),
            groups,
        }))
    }

    #[instrument(skip_all)]
    async fn list_rollout_events(
        &self,
        request: Request<ListRolloutEventsRequest>,
    ) -> Result<Response<ListRolloutEventsReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let request = request.into_inner();
        if request.rollout_id <= 0 {
            return Err(Status::invalid_argument(
                "rollout_id must be greater than 0",
            ));
        }
        self.enforce_rollout_job(&identity, AclAction::Read, request.rollout_id)?;
        let events = self
            .store
            .list_rollout_events(request.rollout_id, request.limit)
            .map_err(|e| map_store_error("Error listing rollout events", e))?
            .into_iter()
            .map(|event| RolloutEvent {
                id: event.id,
                rollout_id: event.rollout_id,
                job_id: event.job_id,
                group_name: event.group_name,
                level: event.level,
                message: event.message,
                metadata_json: event.metadata_json,
                created_at: event.created_at,
            })
            .collect::<Vec<_>>();

        Ok(Response::new(ListRolloutEventsReply { events }))
    }

    #[instrument(skip_all)]
    async fn list_events(
        &self,
        request: Request<ListEventsRequest>,
    ) -> Result<Response<ListEventsReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Read,
            AclScope::none(),
        )?;
        let request = request.into_inner();
        let filter = SchedulerEventFilter {
            instance_id: normalize_event_filter_value(request.instance_id),
            allocation_id: request.allocation_id,
            ..scheduler_event_filter(
                request.after_id,
                request.limit,
                request.kind,
                request.job_id,
                request.node_id,
                request.level,
            )
        };

        let events = self
            .store
            .list_scheduler_events(&filter)
            .map_err(|e| map_store_error("Error listing scheduler events", e))?;
        let latest_id = self
            .store
            .latest_scheduler_event_id()
            .map_err(|e| map_store_error("Error reading latest scheduler event id", e))?;

        Ok(Response::new(ListEventsReply {
            events: events.into_iter().map(map_scheduler_event).collect(),
            latest_id,
        }))
    }

    type ExecAllocationStream = crate::manager::exec::ExecAllocationStream;

    #[instrument(skip_all)]
    async fn exec_allocation(
        &self,
        request: Request<Streaming<ExecClientFrame>>,
    ) -> Result<Response<Self::ExecAllocationStream>, Status> {
        if !self.exec_enabled {
            return Err(Status::failed_precondition(
                "exec is disabled on this manager (exec.enabled=false)",
            ));
        }
        // Authenticate from request metadata (native gRPC and the WS bridge,
        // which lifts the in-frame token into metadata before dialing). ACL
        // enforcement + action rendering happen on the parsed ExecStart.
        let identity = self.authenticate_acl(&request)?;
        // Coarse gate before touching the stream, so a credential-less caller
        // is denied immediately (and cannot probe by holding the connection).
        // Either exec:write or action:write anywhere clears it; the precise
        // job/host/action scope is enforced once the ExecStart is parsed.
        if !identity.would_allow(AclResource::Exec, AclAction::Write, &AclScope::any())
            && !identity.would_allow(AclResource::Action, AclAction::Write, &AclScope::any())
        {
            acl::enforce(
                &identity,
                AclResource::Exec,
                AclAction::Write,
                &AclScope::any(),
            )?;
        }
        let credential = self.stream_credential(&request);
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await
            .map_err(|e| Status::internal(format!("exec stream error: {e}")))?
            .ok_or_else(|| Status::invalid_argument("exec stream closed before start frame"))?;
        let start = match first.frame {
            Some(crate::structs::exec_client_frame::Frame::Start(start)) => start,
            _ => {
                return Err(Status::invalid_argument(
                    "first exec frame must be ExecStart",
                ))
            }
        };

        // Adapt the tonic client stream into the channel the shared core reads.
        let (in_tx, in_rx) = tokio::sync::mpsc::channel::<ExecClientFrame>(128);
        tokio::spawn(async move {
            while let Ok(Some(frame)) = inbound.message().await {
                if in_tx.send(frame).await.is_err() {
                    break;
                }
            }
        });

        let out_rx = crate::manager::exec::run_exec_session(
            &self.store,
            &identity,
            start,
            in_rx,
            credential,
        )
        .await?;
        Ok(Response::new(ReceiverStream::new(out_rx)))
    }

    /// Mint a direct-exec ticket (docs/exec-and-actions.md, "Direct exec").
    /// ACL: the coarse `exec`/`action` gate here, before anything is
    /// looked up; the precise scope inside `mint_exec_ticket`, which runs
    /// `ExecAllocation`'s own resolution.
    #[instrument(skip_all, fields(allocation_id, instance_id))]
    async fn mint_exec_ticket(
        &self,
        request: Request<MintExecTicketRequest>,
    ) -> Result<Response<MintExecTicketReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        if !identity.would_allow(AclResource::Exec, AclAction::Write, &AclScope::any())
            && !identity.would_allow(AclResource::Action, AclAction::Write, &AclScope::any())
        {
            acl::enforce(
                &identity,
                AclResource::Exec,
                AclAction::Write,
                &AclScope::any(),
            )?;
        }
        let req = request.into_inner();
        tracing::Span::current().record("allocation_id", req.allocation_id);
        tracing::Span::current().record("instance_id", req.instance_id.as_str());
        if !self.exec_enabled {
            return Err(Status::failed_precondition(
                "exec is disabled on this manager (exec.enabled=false)",
            ));
        }
        let Some(state) = self.workload_identity.as_ref() else {
            return Err(Status::failed_precondition(
                "exec tickets are signed by the workload-identity issuer, which is not enabled \
                 on this manager\nHint: set workloadIdentity.enabled: true in the manager \
                 config, or exec through the manager (drop --direct)",
            ));
        };
        let store = self.store.clone();
        let state = state.clone();
        let limits = self.exec_tickets.clone();
        let reply = tokio::task::spawn_blocking(move || {
            crate::manager::exec::mint_exec_ticket(
                &store,
                &identity,
                &state,
                &limits,
                &req,
                std::time::SystemTime::now(),
            )
        })
        .await
        .map_err(|e| Status::internal(format!("mint task failed: {e}")))??;
        Ok(Response::new(reply))
    }

    /// Register a delegated exec minting key (docs/exec-and-actions.md,
    /// "Delegated minting keys").
    #[instrument(skip_all)]
    async fn create_exec_key(
        &self,
        request: Request<CreateExecKeyRequest>,
    ) -> Result<Response<CreateExecKeyReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let idempotency_key =
            scoped_idempotency_key(&identity, "create_exec_key", req.idempotency_key.as_deref());
        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            if let Some(key) = self
                .store
                .get_exec_key(&recorded)
                .map_err(|e| map_store_error("Error loading exec key", e))?
            {
                return Ok(Response::new(CreateExecKeyReply {
                    key: Some(key),
                    deduplicated: true,
                }));
            }
        }
        let key = crate::manager::exec::new_exec_key(
            &identity,
            &self.exec_tickets,
            &req,
            time::OffsetDateTime::now_utc(),
        )?;
        self.store
            .insert_exec_key(&key)
            .map_err(|e| map_store_error("Error storing exec key", e))?;
        if !idempotency_key.is_empty() {
            self.store
                .idempotency_record(&idempotency_key, "create_exec_key", &key.id)
                .map_err(|e| map_store_error("Error recording idempotency", e))?;
        }
        *self.exec_keys.lock().unwrap_or_else(|p| p.into_inner()) = None;
        info!(
            target: "exec_audit",
            principal = %identity.principal,
            accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
            key_id = %key.id,
            jobs = %key.jobs,
            "exec key created"
        );
        Ok(Response::new(CreateExecKeyReply {
            key: Some(key),
            deduplicated: false,
        }))
    }

    #[instrument(skip_all)]
    async fn list_exec_keys(
        &self,
        request: Request<ListExecKeysRequest>,
    ) -> Result<Response<ListExecKeysReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Read,
            AclScope::none(),
        )?;
        let keys = self
            .store
            .list_exec_keys()
            .map_err(|e| map_store_error("Error listing exec keys", e))?;
        Ok(Response::new(ListExecKeysReply { keys }))
    }

    #[instrument(skip_all, fields(key_id = %request.get_ref().id))]
    async fn delete_exec_key(
        &self,
        request: Request<DeleteExecKeyRequest>,
    ) -> Result<Response<DeleteExecKeyReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let idempotency_key =
            scoped_idempotency_key(&identity, "delete_exec_key", req.idempotency_key.as_deref());
        if self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
            .is_some()
        {
            return Ok(Response::new(DeleteExecKeyReply { deduplicated: true }));
        }
        self.store
            .delete_exec_key(&req.id)
            .map_err(|e| map_store_error("Error deleting exec key", e))?;
        if !idempotency_key.is_empty() {
            self.store
                .idempotency_record(&idempotency_key, "delete_exec_key", &req.id)
                .map_err(|e| map_store_error("Error recording idempotency", e))?;
        }
        *self.exec_keys.lock().unwrap_or_else(|p| p.into_inner()) = None;
        info!(
            target: "exec_audit",
            principal = %identity.principal,
            accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
            key_id = %req.id,
            "exec key deleted"
        );
        Ok(Response::new(DeleteExecKeyReply {
            deduplicated: false,
        }))
    }
    #[instrument(skip_all)]
    async fn list_job_actions(
        &self,
        request: Request<ListJobActionsRequest>,
    ) -> Result<Response<ListJobActionsReply>, Status> {
        let req = request.get_ref();
        self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Read,
            AclScope::job(&req.job),
        )?;
        let actions = crate::manager::exec::list_job_actions(&self.store, &req.job)?;
        Ok(Response::new(ListJobActionsReply { actions }))
    }

    type StreamEventsStream = ReceiverStream<Result<SchedulerEvent, Status>>;

    type StreamWorkStream = ReceiverStream<Result<crate::structs::StreamWorkReply, Status>>;

    /// Sequenced delivery of work to one worker (see `work_stream`).
    #[instrument(skip_all)]
    async fn stream_work(
        &self,
        request: Request<tonic::Streaming<crate::structs::StreamWorkRequest>>,
    ) -> Result<Response<Self::StreamWorkStream>, Status> {
        self.stream_work_rpc(request).await
    }

    #[instrument(skip_all)]
    async fn stream_events(
        &self,
        request: Request<StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Read,
            AclScope::none(),
        )?;
        let credential = self.stream_credential(&request);
        let request = request.into_inner();
        let filter = SchedulerEventFilter {
            instance_id: normalize_event_filter_value(request.instance_id),
            allocation_id: request.allocation_id,
            ..scheduler_event_filter(
                request.after_id,
                None,
                request.kind,
                request.job_id,
                request.node_id,
                request.level,
            )
        };

        let store = self.store.clone();
        // Subscribe before replaying history so no event can fall between the
        // journal snapshot and the live feed; overlap is deduped via event IDs.
        let mut live = store.subscribe_events();
        let (tx, rx) = tokio::sync::mpsc::channel(EVENT_STREAM_BUFFER);

        tokio::spawn(async move {
            let mut last_sent = match filter.after_id {
                Some(after_id) => {
                    match replay_journal_events(&store, &filter, after_id, &tx).await {
                        Ok(cursor) => cursor,
                        Err(StreamSendStopped) => return,
                    }
                }
                // Pure tail: live subscription only sees events emitted after
                // subscribe, so no replay and no cursor floor is needed.
                None => 0,
            };

            // Re-check the credential while the stream runs, so a revoked or
            // expired token (or a revoked policy) closes the feed.
            let mut reauth = tokio::time::interval(credential.interval);
            reauth.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            reauth.tick().await; // the first tick fires immediately
            loop {
                let received = tokio::select! {
                    received = live.recv() => received,
                    _ = reauth.tick() => {
                        let still_allowed = credential.reauthenticate(&store).and_then(|identity| {
                            acl::enforce(
                                &identity,
                                AclResource::Operator,
                                AclAction::Read,
                                &AclScope::none(),
                            )
                        });
                        if let Err(status) = still_allowed {
                            let _ = tx.send(Err(status)).await;
                            return;
                        }
                        continue;
                    }
                };
                match received {
                    Ok(event) => {
                        // The instance filter's allocation half reads the
                        // allocation row, so an allocation placed after the
                        // stream opened still matches.
                        let allocation_instance = |id: i64| {
                            store
                                .get_allocation_by_id(id)
                                .ok()
                                .flatten()
                                .and_then(|allocation| allocation.instance_id)
                        };
                        if event.id <= last_sent
                            || !filter.matches_with(&event, allocation_instance)
                        {
                            continue;
                        }
                        last_sent = event.id;
                        if tx.send(Ok(map_scheduler_event(event))).await.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        debug!(skipped, "StreamEvents subscriber lagged; backfilling");
                        match replay_journal_events(&store, &filter, last_sent, &tx).await {
                            Ok(cursor) => last_sent = cursor.max(last_sent),
                            Err(StreamSendStopped) => return,
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    #[instrument(skip_all)]
    async fn advance_rollout(
        &self,
        request: Request<AdvanceRolloutRequest>,
    ) -> Result<Response<AdvanceRolloutReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let request = request.into_inner();
        self.enforce_rollout_job(&identity, AclAction::Write, request.id)?;
        let priority = request.priority.unwrap_or(self.store.default_eval_priority);
        let rollout = advance_rollout_state(&self.store, request.id, &request.groups, priority)
            .map_err(|e| Status::invalid_argument(format!("Error advancing rollout: {e}")))?;

        Ok(Response::new(AdvanceRolloutReply {
            message: "Rollout advance queued".to_string(),
            rollout: Some(rollout),
        }))
    }

    #[instrument(skip_all)]
    async fn stop_rollout(
        &self,
        request: Request<StopRolloutRequest>,
    ) -> Result<Response<StopRolloutReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let request = request.into_inner();
        self.enforce_rollout_job(&identity, AclAction::Write, request.id)?;
        let priority = request.priority.unwrap_or(self.store.default_eval_priority);
        let rollout = stop_rollout_state(&self.store, request.id, request.revert, priority)
            .map_err(|e| Status::invalid_argument(format!("Error stopping rollout: {e}")))?;

        Ok(Response::new(StopRolloutReply {
            message: if request.revert {
                "Rollout stopped and reverted".to_string()
            } else {
                "Rollout stopped".to_string()
            },
            rollout: Some(rollout),
        }))
    }

    #[instrument(skip_all)]
    async fn plan_job(
        &self,
        request: Request<PlanJobRequest>,
    ) -> Result<Response<PlanJobReply>, Status> {
        // ACL: planning never mutates, so it requires job:read; the exact
        // job-name scope is enforced after the jobspec parses.
        let identity =
            self.check_acl(&request, AclResource::Job, AclAction::Read, AclScope::any())?;
        let jobspec = request.into_inner().parsed_jobspec;
        if jobspec.trim().is_empty() {
            return Err(Status::invalid_argument("Plan request missing jobspec"));
        }

        let (config, _kv_refs) =
            resolve_jobspec_kv_references(&self.store, &jobspec).map_err(|e| {
                error!("Error parsing jobspec for plan: {:?}", e);
                Status::invalid_argument(format!("Error parsing jobspec: {e}"))
            })?;
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Read,
            &AclScope::job(&config.name),
        )?;
        let mut config = self
            .store
            .normalize_job_config(&config)
            .map_err(|e| map_store_error("Error normalizing jobspec", e))?;
        validate_job_config(&config).map_err(|e| {
            error!("Error validating task configs: {:?}", e);
            Status::invalid_argument(format!("Error validating task configs: {e}"))
        })?;
        self.validate_job_volumes(&config).map_err(|e| {
            error!("Error validating volumes: {:?}", e);
            Status::invalid_argument(format!("Error validating volumes: {e:#}"))
        })?;
        self.validate_job_capabilities(&config).map_err(|e| {
            error!("Error validating capabilities: {:?}", e);
            Status::invalid_argument(format!("Error validating capabilities: {e:#}"))
        })?;
        // Artifact admission for plan mirrors run: pins resolve here too so
        // the plan shows the digests a submit would ship.
        let pin_notes = run_blocking_admission(|| self.normalize_job_artifacts(&mut config, true))
            .map_err(|e| {
                error!("Error normalizing artifacts: {:?}", e);
                Status::invalid_argument(format!("Error normalizing artifacts: {e:#}"))
            })?;
        let config = config;

        let scheduler = Scheduler::new(&self.store).map_err(|e| {
            error!("Error building scheduler: {:?}", e);
            Status::internal("Error preparing scheduler")
        })?;

        let existing_config = self
            .store
            .get_job_config_optional(&config.name)
            .map_err(|e| map_store_error("Error getting job config", e))?;

        // Digest provenance for the plan: where a pinned reference differs
        // from the stored config's pin, surface `old -> new` so a moved tag
        // is visible before submitting (artifact system plan §7).
        let artifact_diffs = existing_config
            .as_ref()
            .map(|existing| artifact_pin_diffs(existing, &config))
            .unwrap_or_default();

        let (plan, task_lookup, index_report, preemptions, message, matches_existing, same_content) =
            match existing_config {
                Some(existing) if existing.is_same_as(&config) => {
                    let (plan, index_report, preemptions) = scheduler
                        .plan_only_with_report(&existing.id(), PlanMode::Fresh)
                        .map_err(|e| {
                            error!("Error planning job {}: {:?}", existing.name, e);
                            Status::invalid_argument(format!("Error planning job: {e}"))
                        })?;
                    let task_configs =
                        self.store
                            .get_task_configs_for_job_config(&existing.id())
                            .map_err(|e| map_store_error("Error getting task configs", e))?;
                    let task_lookup = task_configs
                        .into_iter()
                        .map(|(id, task)| (id, task.name))
                        .collect::<HashMap<_, _>>();

                    (
                        plan,
                        task_lookup,
                        index_report,
                        preemptions,
                        "Job config unchanged; showing force-reschedule placements".to_string(),
                        true,
                        true,
                    )
                }
                Some(_existing) => {
                    let PlanConfigOutcome {
                        allocations,
                        task_lookup,
                        index_report,
                        preemptions,
                    } = scheduler
                        .plan_for_config(&config, PlanMode::Fresh, Some(&config.name))
                        .map_err(|e| {
                            error!("Error planning updated job {}: {:?}", config.name, e);
                            Status::invalid_argument(format!("Error planning job: {e}"))
                        })?;
                    (
                        allocations,
                        task_lookup,
                        index_report,
                        preemptions,
                        "Job config changed; showing updated placements".to_string(),
                        true,
                        false,
                    )
                }
                None => {
                    let PlanConfigOutcome {
                        allocations,
                        task_lookup,
                        index_report,
                        preemptions,
                    } = scheduler
                        .plan_for_config(&config, PlanMode::Fresh, None)
                        .map_err(|e| {
                            error!("Error planning new job {}: {:?}", config.name, e);
                            Status::invalid_argument(format!("Error planning job: {e}"))
                        })?;
                    (
                        allocations,
                        task_lookup,
                        index_report,
                        preemptions,
                        "Job config new; showing placements".to_string(),
                        false,
                        false,
                    )
                }
            };

        let allocations = plan
            .into_iter()
            .map(|placement| PlannedAllocation {
                node_id: placement.node_id,
                task_config_id: placement.task_config_id,
                task_name: task_lookup
                    .get(&placement.task_config_id)
                    .cloned()
                    .unwrap_or_default(),
                candidate_nodes: placement.candidates,
            })
            .collect();

        let mut message = message;
        for note in pin_notes
            .iter()
            .map(ArtifactPin::describe)
            .chain(artifact_diffs.iter().cloned())
        {
            message.push('\n');
            message.push_str(&note);
        }

        let index_usage = index_report
            .into_iter()
            .map(|usage| crate::structs::TaskIndexUsage {
                task_name: usage.task_name,
                index: usage.index.unwrap_or_default(),
                mode: usage.mode.unwrap_or_default().to_string(),
                candidates: usage.candidates as u32,
                truncated: usage.truncated,
                refills: usage.refills,
                fell_back: usage.fell_back,
                shadow_agreement: usage.shadow_agreement,
                reason: usage.reason.unwrap_or_default(),
                score_sampled: usage.score_sampled.unwrap_or_default() as u32,
            })
            .collect();

        let preemptions = preemptions
            .into_iter()
            .map(|victim| crate::structs::PlannedPreemption {
                node_id: victim.node_id,
                job_id: victim.job_id,
                task_name: victim.task_name,
                band: victim.band.as_str().to_string(),
                allocation_id: victim.allocation_id,
                kill_after_seconds: victim.kill_after_secs,
            })
            .collect();

        let reply = PlanJobReply {
            allocations,
            message,
            matches_existing,
            same_content,
            index_usage,
            preemptions,
            preemption_mode: scheduler.preemption_settings().mode.as_str().to_string(),
            warnings: self
                .gated_egress_submit_warnings(&config)
                .into_iter()
                .chain(crate::task_plugins::container_security_warnings(&config))
                .collect(),
        };

        Ok(Response::new(reply))
    }

    async fn get_job_spec(
        &self,
        request: Request<GetJobSpecRequest>,
    ) -> Result<Response<GetJobSpecReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Read,
            self.job_read_scope(&request.get_ref().name),
        )?;
        let name = request.into_inner().name;

        let config = match self
            .store
            .get_job_config_optional(&name)
            .map_err(|e| map_store_error("Error getting job config", e))?
        {
            Some(config) => config,
            None => {
                return Err(Status::not_found("Job config not found"));
            }
        };

        let submissions = self
            .store
            .get_submissions_for_config(&config.id())
            .map_err(|e| map_store_error("Error getting submissions", e))?;
        let rendered_jobspec = rendered_jobspec_json(&config)?;

        Ok(Response::new(GetJobSpecReply {
            submissions,
            rendered_jobspec,
        }))
    }

    async fn get_job_spec_by_config(
        &self,
        request: Request<GetJobSpecByConfigRequest>,
    ) -> Result<Response<GetJobSpecReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let config_id = request.into_inner().config_id;
        // Resolve the config to its job so name-scoped policies apply.
        let job_name = self
            .store
            .job_name_for_config(&config_id)
            .map_err(|e| map_store_error("Error resolving job config", e))?;
        let scope = match &job_name {
            Some(name) => self.job_read_scope(name),
            None => AclScope::any(),
        };
        acl::enforce(&identity, AclResource::Job, AclAction::Read, &scope)?;

        let submissions = self
            .store
            .get_submissions_for_config(&config_id)
            .map_err(|e| map_store_error("Error getting submissions", e))?;
        let config = self
            .store
            .get_job_config_by_id_optional(&config_id)
            .map_err(|e| map_store_error("Error getting job config", e))?
            .ok_or_else(|| Status::not_found("Job config not found"))?;
        let rendered_jobspec = rendered_jobspec_json(&config)?;

        Ok(Response::new(GetJobSpecReply {
            submissions,
            rendered_jobspec,
        }))
    }

    #[instrument(skip_all)]
    async fn list_job_spec_history(
        &self,
        request: Request<ListJobSpecHistoryRequest>,
    ) -> Result<Response<ListJobSpecHistoryReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Read,
            self.job_read_scope(&request.get_ref().name),
        )?;
        let name = request.into_inner().name;
        let items = self
            .store
            .list_job_spec_history(&name)
            .map_err(|e| map_store_error("Error listing job spec history", e))?
            .into_iter()
            .map(|row| JobSpecHistoryItem {
                config_id: row.config_id,
                created_at: row.created_at,
                version: row.version,
                template: row.template,
            })
            .collect();

        Ok(Response::new(ListJobSpecHistoryReply { items }))
    }

    #[instrument(skip_all)]
    async fn list_jobs(
        &self,
        request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsReply>, Status> {
        let identity =
            self.check_acl(&request, AclResource::Job, AclAction::Read, AclScope::any())?;
        let request = request.into_inner();
        let query = normalize_query(request.query);
        let sort_by = request.sort_by.unwrap_or_else(|| "updated_at".to_string());
        let sort_desc = request
            .sort_order
            .as_deref()
            .unwrap_or("desc")
            .eq_ignore_ascii_case("desc");
        let (page, page_size) = parse_page(request.page, request.page_size);

        // `status` is derived from each job's workload (running, pending,
        // complete, ...), so the query, filter, counts, and sort below all
        // see what a reader sees.
        let mut jobs = self
            .store
            .get_jobs_with_status()
            .map_err(|e| map_store_error("Error getting jobs", e))?;
        // Before the query, the counts, and the status filter: `blocked` is
        // a status like any other to all three.
        self.store
            .overlay_blocked_jobs(&mut jobs)
            .map_err(|e| map_store_error("Error loading blocked evals", e))?;
        // Provenance feeds the `templates` rule selector so template-scoped
        // identities see the jobs their templates created. One batch query
        // instead of one per row; management tokens skip it entirely.
        let provenance = if identity.is_management {
            HashMap::new()
        } else {
            self.store
                .job_template_provenance_map()
                .map_err(|e| map_store_error("Error loading job provenance", e))?
        };
        jobs.retain(|job| {
            let scope = AclScope::job(&job.name).with_template(
                provenance
                    .get(&job.name)
                    .map(|provenance| provenance.name.clone()),
            );
            identity.allows(AclResource::Job, AclAction::Read, &scope)
        });

        if let Some(query) = query {
            jobs.retain(|job| {
                job.id.to_lowercase().contains(&query)
                    || job.name.to_lowercase().contains(&query)
                    || job.status.to_lowercase().contains(&query)
            });
        }

        // Counted before the status filter on purpose: a filter control that
        // shows "failed (3)" must keep saying 3 while "running" is selected.
        let mut status_counts: HashMap<String, u32> = HashMap::new();
        for job in &jobs {
            *status_counts.entry(job.status.clone()).or_insert(0) += 1;
        }

        if let Some(status) = request
            .status
            .as_deref()
            .map(str::trim)
            .filter(|status| !status.is_empty())
        {
            jobs.retain(|job| job.status.eq_ignore_ascii_case(status));
        }

        jobs.sort_by(|a, b| {
            let ordering = match sort_by.as_str() {
                "name" => a.name.cmp(&b.name),
                "status" => a.status.cmp(&b.status),
                "created_at" => a.created_at.cmp(&b.created_at),
                _ => a.updated_at.cmp(&b.updated_at),
            };
            let ordering = if sort_desc {
                ordering.reverse()
            } else {
                ordering
            };
            // Break ties on the name, always ascending and never reversed:
            // the mtime is whole seconds, so a batch submitted together ties
            // on it, and an arbitrary order there makes paging drop and
            // duplicate rows between pages.
            ordering.then_with(|| a.name.cmp(&b.name))
        });

        let (jobs, total_count) = paginate(jobs, page, page_size);

        Ok(Response::new(ListJobsReply {
            jobs,
            total_count,
            page: page as u32,
            page_size: page_size as u32,
            status_counts,
        }))
    }

    #[instrument(skip_all)]
    async fn stop_job(
        &self,
        request: Request<StopJobRequest>,
    ) -> Result<Response<StopJobReply>, Status> {
        // Lifecycle write: provenance-aware scope, so template-scoped
        // identities can stop the jobs their templates created (they cannot
        // change specs this way — RunJob never carries provenance).
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            self.job_read_scope(&request.get_ref().name),
        )?;
        let inner = request.into_inner();
        let name = inner.name;
        let priority = inner.priority.unwrap_or(self.store.default_eval_priority);
        let idempotency_key =
            scoped_idempotency_key(&identity, "stop_job", inner.idempotency_key.as_deref());

        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(StopJobReply {
                message,
                deduplicated: true,
            }));
        }

        let job_config_id = self
            .store
            .get_job_config(&name)
            .map_err(|e| map_store_error("Error stopping job", e))?
            .id();

        let _ = self
            .store
            .stop_job(&name)
            .map_err(|e| map_store_error("Error stopping job", e))?;

        self.store
            .enqueue_eval(
                "job_stop",
                Some(&name),
                Some(&job_config_id),
                None,
                Some(priority),
            )
            .map_err(|e| map_store_error("Error stopping job", e))?;

        let message = format!("Stopped {}", name);
        self.store
            .idempotency_record(&idempotency_key, "stop_job", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        let reply = StopJobReply {
            message,
            deduplicated: false,
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn scale_task(
        &self,
        request: Request<ScaleTaskRequest>,
    ) -> Result<Response<ScaleTaskReply>, Status> {
        // The whole point of the `scale` resource: resize rights without
        // redeploy rights (docs/autoscaling.md, docs/auth-and-acls.md).
        let ident = self.check_acl(
            &request,
            AclResource::Scale,
            AclAction::Write,
            self.job_read_scope(&request.get_ref().name),
        )?;
        let inner = request.into_inner();
        let name = inner.name;
        let idempotency_key =
            scoped_idempotency_key(&ident, "scale_task", inner.idempotency_key.as_deref());

        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(ScaleTaskReply {
                message,
                deduplicated: true,
                deferred: false,
            }));
        }

        if inner.count.is_none() && !inner.reset {
            return Err(Status::invalid_argument(
                "provide count (the desired replicas) or reset (return the count to the spec)",
            ));
        }
        if inner.count.is_some() && inner.reset {
            return Err(Status::invalid_argument(
                "count and reset are mutually exclusive",
            ));
        }

        let config = keyed_job_config(&self.store, &name)?;
        let task = resolve_scalable_task(
            &config,
            &name,
            inner.task_group.as_deref(),
            inner.task_name.as_deref(),
        )?;
        let (task_group, task_name, spec_count) =
            (task.group.clone(), task.name.clone(), task.count.num);
        let job_config_id = config.id();
        let reason = inner.reason.unwrap_or_default();
        let previous = self
            .store
            .effective_task_count(&name, &task_group, &task_name, spec_count)
            .map_err(|e| map_store_error("Error reading scale state", e))?;

        let (message, deferred) = if inner.reset {
            let existed = self
                .store
                .clear_task_scale(&name, &task_group, &task_name)
                .map_err(|e| map_store_error("Error resetting scale", e))?;
            if existed {
                self.store
                    .events()
                    .scale_reset(&name, &task_group, &task_name, "explicit reset");
                self.store
                    .enqueue_eval(
                        "job_scale",
                        Some(&name),
                        Some(&job_config_id),
                        None,
                        Some(self.store.default_eval_priority),
                    )
                    .map_err(|e| map_store_error("Error enqueueing scale eval", e))?;
            }
            (
                format!(
                    "{}/{}/{}: {} -> {} (override dropped; spec owns the count)",
                    name, task_group, task_name, previous, spec_count
                ),
                false,
            )
        } else {
            let count = inner.count.unwrap_or_default();
            // The ECS rule (docs/autoscaling.md): scale-out applies during
            // a rollout, scale-in defers until it finishes.
            let active_rollout = self
                .store
                .list_rollouts(Some(&name), true)
                .map_err(|e| map_store_error("Error checking rollouts", e))?
                .into_iter()
                .next();
            match active_rollout {
                Some(rollout) if count < previous => {
                    self.store
                        .set_task_scale_pending(
                            &name,
                            &task_group,
                            &task_name,
                            count,
                            spec_count,
                            "operator",
                            &reason,
                            &ident.principal,
                        )
                        .map_err(|e| map_store_error("Error recording pending scale", e))?;
                    self.store.events().scale_deferred(
                        &name,
                        &task_group,
                        &task_name,
                        count,
                        rollout.id,
                        "operator",
                        &reason,
                    );
                    (
                        format!(
                            "deferred: rollout {} is in progress; scale-in to {} applies when it completes",
                            rollout.id, count
                        ),
                        true,
                    )
                }
                _ => {
                    self.store
                        .set_task_scale_override(
                            &name,
                            &task_group,
                            &task_name,
                            count,
                            spec_count,
                            "operator",
                            &reason,
                            &ident.principal,
                        )
                        .map_err(|e| map_store_error("Error applying scale", e))?;
                    self.store.events().scale_applied(
                        &name,
                        &task_group,
                        &task_name,
                        Some(previous),
                        count,
                        "operator",
                        &reason,
                    );
                    self.store
                        .enqueue_eval(
                            "job_scale",
                            Some(&name),
                            Some(&job_config_id),
                            None,
                            Some(self.store.default_eval_priority),
                        )
                        .map_err(|e| map_store_error("Error enqueueing scale eval", e))?;
                    (
                        format!(
                            "{}/{}/{}: {} -> {} (spec count {}; override active)",
                            name, task_group, task_name, previous, count, spec_count
                        ),
                        false,
                    )
                }
            }
        };

        self.store
            .idempotency_record(&idempotency_key, "scale_task", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(ScaleTaskReply {
            message,
            deduplicated: false,
            deferred,
        }))
    }

    #[instrument(skip_all)]
    async fn get_job_scale_status(
        &self,
        request: Request<GetJobScaleStatusRequest>,
    ) -> Result<Response<GetJobScaleStatusReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Scale,
            AclAction::Read,
            self.job_read_scope(&request.get_ref().name),
        )?;
        let name = request.into_inner().name;
        let config = self
            .store
            .get_job_config(&name)
            .map_err(|e| map_store_error("Error loading job config", e))?;
        let scales = self
            .store
            .task_scales_for_job(&name)
            .map_err(|e| map_store_error("Error loading scale state", e))?;

        let tasks = config
            .tasks
            .iter()
            .map(|task| {
                let scale = scales
                    .iter()
                    .find(|scale| scale.task_group == task.group && scale.task_name == task.name);
                let override_count = scale.and_then(|scale| scale.count);
                TaskScaleStatus {
                    task_group: task.group.clone(),
                    task_name: task.name.clone(),
                    spec_count: task.count.num,
                    override_count,
                    pending_count: scale.and_then(|scale| scale.pending_count),
                    effective_count: override_count.unwrap_or(task.count.num),
                    source: scale.map(|scale| scale.source.clone()).unwrap_or_default(),
                    reason: scale.map(|scale| scale.reason.clone()).unwrap_or_default(),
                    updated_at: scale
                        .map(|scale| scale.updated_at.clone())
                        .unwrap_or_default(),
                }
            })
            .collect();

        Ok(Response::new(GetJobScaleStatusReply { tasks }))
    }

    #[instrument(skip_all)]
    async fn destroy_job(
        &self,
        request: Request<DestroyJobRequest>,
    ) -> Result<Response<DestroyJobReply>, Status> {
        // Lifecycle write: provenance-aware scope (see stop_job).
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            self.job_read_scope(&request.get_ref().name),
        )?;
        let inner = request.into_inner();
        let name = inner.name;
        let idempotency_key =
            scoped_idempotency_key(&identity, "destroy_job", inner.idempotency_key.as_deref());

        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(DestroyJobReply {
                message,
                deduplicated: true,
            }));
        }

        let _ = self
            .store
            .destroy_job(&name)
            .map_err(|e| map_store_error("Error deleting job", e))?;

        // TODO: Conditionalize
        let message = format!("Destroyed {}", name);
        self.store
            .idempotency_record(&idempotency_key, "destroy_job", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        let reply = DestroyJobReply {
            message,
            deduplicated: false,
        };

        Ok(Response::new(reply))
    }

    /// Create a keyed-mode stateful instance and enqueue placement. See the
    /// proto comments on `CreateInstance` for the full contract (task
    /// resolution, id-source policy, never-recycle collision rules).
    #[instrument(skip_all)]
    async fn create_instance(
        &self,
        request: Request<CreateInstanceRequest>,
    ) -> Result<Response<CreateInstanceReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::job(&request.get_ref().job_id),
        )?;
        let inner = request.into_inner();
        // Phase 6: restoring reads the snapshot. Checked before anything is
        // looked up, so a caller without the grant learns nothing.
        if inner
            .from_snapshot
            .as_deref()
            .is_some_and(|from| !from.trim().is_empty())
        {
            acl::enforce(
                &identity,
                AclResource::Snapshot,
                AclAction::Read,
                &AclScope::job(&inner.job_id),
            )?;
        }
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "create_instance",
            inner.idempotency_key.as_deref(),
        );

        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            // Recorded as `<id>`, or `<id> <snapshot>` for a create from a
            // snapshot (ids never contain spaces).
            let (instance_id, restoring_from) = recorded
                .split_once(' ')
                .map(|(id, snapshot)| (id.to_string(), snapshot.to_string()))
                .unwrap_or((recorded, String::new()));
            return Ok(Response::new(CreateInstanceReply {
                instance_id,
                deduplicated: true,
                restoring_from,
                mode: InstanceMode::Keyed.to_string(),
                durable: true,
                node_id: String::new(),
                state: String::new(),
            }));
        }

        let config = keyed_job_config(&self.store, &inner.job_id)?;
        let task = resolve_keyed_task(
            &config,
            &inner.job_id,
            inner.group.as_deref(),
            inner.task.as_deref(),
        )?;

        // Id policy: a valid explicit key is always honored, and requireId
        // refuses a create without one. See proto comments.
        let requested_id = inner.instance_id.as_deref().filter(|id| !id.is_empty());
        let require_id = task
            .stateful
            .as_ref()
            .is_some_and(|stateful| stateful.require_id);
        match requested_id {
            Some(id) => validate_keyed_instance_id(id).map_err(Status::invalid_argument)?,
            None if require_id => {
                return Err(Status::invalid_argument(format!(
                    "task {} sets stateful.requireId = true; CreateInstance must supply \
                     instance_id",
                    task.name
                )));
            }
            None => {}
        }

        // Session mode (docs/session-mode.md): a create without an id starts
        // an unnamed session through the session router and writes nothing
        // to the replicated store. A name makes it an ordinary keyed
        // instance, durable from birth, unless the task refuses names.
        if crate::structs::session::is_session_task(task) {
            if requested_id.is_none() {
                return self.create_session(&identity, &inner, &config, task).await;
            }
            if !crate::structs::session::accepts_names(task) {
                return Err(Status::invalid_argument(format!(
                    "task {} sets stateful.promote.onName = false: its instances are unnamed \
                     sessions; omit instance_id",
                    task.name
                )));
            }
        }

        // Phase 6: restore from a snapshot instead of booting empty.
        let seed = inner
            .from_snapshot
            .as_deref()
            .map(str::trim)
            .filter(|from| !from.is_empty())
            .map(|from| self.resolve_seed_snapshot(&identity, &inner.job_id, task, from, 1))
            .transpose()?;
        let restoring_from = seed
            .as_ref()
            .map(|seed| seed.snapshot_id.clone())
            .unwrap_or_default();
        // One scheduler-lane entry for the whole create: the row, the
        // placement eval (a reschedule plan adds exactly the missing keyed
        // allocations without disturbing live ones), and the idempotency
        // record, group-committed with whatever other instance writes are
        // in flight (sandbox-scaling plan, Phase 1). It used to be three
        // entries, one of them on the control lane.
        let job_id = inner.job_id.clone();
        let group = task.group.clone();
        let task_name = task.name.clone();
        let requested = requested_id.map(str::to_string);
        let config_id = config.id();
        let priority = self.store.default_eval_priority;
        let seed_snapshot = seed.map(|seed| seed.snapshot_id);
        let key = idempotency_key.clone();
        let instance_id = self
            .store
            .scheduler_group_commit("create_instance", move |s| {
                let instance_id = match seed_snapshot.as_deref() {
                    Some(snapshot_id) => {
                        let requested: Vec<String> = requested.into_iter().collect();
                        let mint = u32::from(requested.is_empty());
                        s.create_keyed_instances_from_snapshot(
                            &job_id,
                            &group,
                            &task_name,
                            &requested,
                            mint,
                            snapshot_id,
                        )?
                        .into_iter()
                        .next()
                        .context("create from snapshot created nothing")?
                    }
                    None => {
                        s.create_keyed_instance(&job_id, &group, &task_name, requested.as_deref())?
                            .instance_id
                    }
                };
                s.enqueue_eval(
                    "job_reschedule",
                    Some(&job_id),
                    Some(&config_id),
                    None,
                    Some(priority),
                )?;
                let recorded = match seed_snapshot.as_deref() {
                    Some(snapshot_id) => format!("{instance_id} {snapshot_id}"),
                    None => instance_id.clone(),
                };
                s.idempotency_record(&key, "create_instance", &recorded)?;
                Ok(instance_id)
            })
            .map_err(|e| map_store_error("Error creating instance", e))?;

        Ok(Response::new(CreateInstanceReply {
            instance_id,
            deduplicated: false,
            restoring_from,
            mode: InstanceMode::Keyed.to_string(),
            durable: true,
            node_id: String::new(),
            state: String::new(),
        }))
    }

    /// Create a batch of keyed-mode instances atomically (explicit ids plus
    /// `count` server-minted ones) and enqueue ONE placement eval for the
    /// whole batch. All-or-nothing: any invalid or colliding id fails the
    /// call and creates nothing. See the proto comments on `CreateInstances`
    /// for the full contract.
    #[instrument(skip_all)]
    async fn create_instances(
        &self,
        request: Request<CreateInstancesRequest>,
    ) -> Result<Response<CreateInstancesReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::job(&request.get_ref().job_id),
        )?;
        let inner = request.into_inner();
        // Phase 6: restoring reads the snapshot (see create_instance).
        if inner
            .from_snapshot
            .as_deref()
            .is_some_and(|from| !from.trim().is_empty())
        {
            acl::enforce(
                &identity,
                AclResource::Snapshot,
                AclAction::Read,
                &AclScope::job(&inner.job_id),
            )?;
        }
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "create_instances",
            inner.idempotency_key.as_deref(),
        );

        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            // The recorded replay is the comma-joined created ids (the id
            // charset cannot contain commas, so the join is unambiguous),
            // then ` <snapshot>` for a create from a snapshot.
            let (ids, restoring_from) = recorded
                .split_once(' ')
                .map(|(ids, snapshot)| (ids.to_string(), snapshot.to_string()))
                .unwrap_or((recorded, String::new()));
            return Ok(Response::new(CreateInstancesReply {
                instance_ids: ids
                    .split(',')
                    .filter(|id| !id.is_empty())
                    .map(str::to_string)
                    .collect(),
                deduplicated: true,
                restoring_from,
            }));
        }

        if inner.instance_ids.is_empty() && inner.count == 0 {
            return Err(Status::invalid_argument(
                "CreateInstances has nothing to create: supply instance_ids, a non-zero count, \
                 or both",
            ));
        }
        for id in &inner.instance_ids {
            validate_keyed_instance_id(id).map_err(|err| {
                Status::invalid_argument(format!("instance id {id:?} is invalid: {err}"))
            })?;
        }

        let config = keyed_job_config(&self.store, &inner.job_id)?;
        let task = resolve_keyed_task(
            &config,
            &inner.job_id,
            inner.group.as_deref(),
            inner.task.as_deref(),
        )?;

        // requireId means every id is caller-supplied, and server-minting
        // (`count`) contradicts that.
        let require_id = task
            .stateful
            .as_ref()
            .is_some_and(|stateful| stateful.require_id);
        if require_id && inner.count > 0 {
            return Err(Status::invalid_argument(format!(
                "task {} sets stateful.requireId = true, so every id must be caller-supplied; \
                 count > 0 asks the server to mint ids. List explicit instance_ids instead",
                task.name
            )));
        }
        // A session task's unnamed instances are sessions, which a batch of
        // rows cannot express; named ones are ordinary keyed instances.
        if crate::structs::session::is_session_task(task) {
            if inner.count > 0 {
                return Err(Status::invalid_argument(format!(
                    "task {} is a session task: unnamed instances are sessions, started one per \
                     CreateInstance call without an id. Use count only for keyed tasks, or list \
                     instance_ids to create named instances",
                    task.name
                )));
            }
            if !crate::structs::session::accepts_names(task) {
                return Err(Status::invalid_argument(format!(
                    "task {} sets stateful.promote.onName = false: its instances are unnamed \
                     sessions; create them with CreateInstance and no id",
                    task.name
                )));
            }
        }

        // Phase 6: restore every instance from a snapshot.
        let seed = inner
            .from_snapshot
            .as_deref()
            .map(str::trim)
            .filter(|from| !from.is_empty())
            .map(|from| {
                self.resolve_seed_snapshot(
                    &identity,
                    &inner.job_id,
                    task,
                    from,
                    inner.instance_ids.len() + inner.count as usize,
                )
            })
            .transpose()?;
        let restoring_from = seed
            .as_ref()
            .map(|seed| seed.snapshot_id.clone())
            .unwrap_or_default();
        // One scheduler-lane entry for the rows, the placement eval (one
        // covers the whole batch: a reschedule plan adds exactly the missing
        // keyed allocations without disturbing live ones), and the
        // idempotency record, as in `create_instance`.
        let job_id = inner.job_id.clone();
        let group = task.group.clone();
        let task_name = task.name.clone();
        let requested = inner.instance_ids.clone();
        let count = inner.count;
        let config_id = config.id();
        let priority = self.store.default_eval_priority;
        let seed_snapshot = seed.map(|seed| seed.snapshot_id);
        let key = idempotency_key.clone();
        let created = self
            .store
            .scheduler_group_commit("create_instances", move |s| {
                let created = match seed_snapshot.as_deref() {
                    Some(snapshot_id) => s.create_keyed_instances_from_snapshot(
                        &job_id,
                        &group,
                        &task_name,
                        &requested,
                        count,
                        snapshot_id,
                    )?,
                    None => {
                        s.create_keyed_instances(&job_id, &group, &task_name, &requested, count)?
                    }
                };
                s.enqueue_eval(
                    "job_reschedule",
                    Some(&job_id),
                    Some(&config_id),
                    None,
                    Some(priority),
                )?;
                let recorded = match seed_snapshot.as_deref() {
                    Some(snapshot_id) => format!("{} {snapshot_id}", created.join(",")),
                    None => created.join(","),
                };
                s.idempotency_record(&key, "create_instances", &recorded)?;
                Ok(created)
            })
            .map_err(|e| map_store_error("Error creating instances", e))?;

        Ok(Response::new(CreateInstancesReply {
            instance_ids: created,
            deduplicated: false,
            restoring_from,
        }))
    }

    /// Stop (pause) a keyed-mode instance: stop its allocation, RETAIN the
    /// row and storage handle, keep the id owned. Reversible via
    /// StartInstance — see the proto comments on `StopInstance`.
    #[instrument(skip_all)]
    async fn stop_instance(
        &self,
        request: Request<StopInstanceRequest>,
    ) -> Result<Response<StopInstanceReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let idempotency_key =
            scoped_idempotency_key(&identity, "stop_instance", inner.idempotency_key.as_deref());

        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(StopInstanceReply {
                message,
                deduplicated: true,
            }));
        }

        let Some(instance) = self
            .store
            .get_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?
        else {
            // A running session has no row: stopping it ends it on its node,
            // with no write anywhere (docs/session-mode.md).
            return match self
                .end_session(&identity, &inner.instance_id, "stop")
                .await?
            {
                Some(message) => Ok(Response::new(StopInstanceReply {
                    message,
                    deduplicated: false,
                })),
                None => Err(Status::not_found(format!(
                    "instance {} not found",
                    inner.instance_id
                ))),
            };
        };
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job(&instance.job_id),
        )?;
        if instance.mode != InstanceMode::Keyed {
            return Err(Status::invalid_argument(format!(
                "instance {} is slot-mode; slot lifecycles are owned by the scheduler via \
                 count.num — stop the job or scale in instead of StopInstance",
                inner.instance_id
            )));
        }
        if instance.state == InstanceState::Terminated {
            return Err(Status::failed_precondition(format!(
                "instance {} is terminated; destroyed instances are tombstoned forever \
                 (never recycled) and can never be stopped or started again",
                inner.instance_id
            )));
        }
        let manifest_url = self
            .store
            .instance_suspend_state(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?
            .manifest_url;

        // One scheduler-lane entry: the stop and its idempotency record.
        // No eval follows. The stop already removed the allocation rows
        // (workers stop on their next heartbeat), so there is nothing to
        // place, and work waiting for the room it frees is woken by the
        // blocked-eval index, not by a reschedule pass over this job
        // (sandbox-scaling plan, Phases 1 and 3).
        let instance_id = inner.instance_id.clone();
        let key = idempotency_key.clone();
        let message = self
            .store
            .scheduler_group_commit("stop_instance", move |s| {
                let message = match s.stop_keyed_instance(&instance_id)? {
                    StopInstanceOutcome::Stopped { .. } => {
                        format!("Stopped instance {instance_id}")
                    }
                    StopInstanceOutcome::AlreadyStopped => {
                        format!("Instance {instance_id} already stopped")
                    }
                };
                s.idempotency_record(&key, "stop_instance", &message)?;
                Ok(message)
            })
            .map_err(|e| map_store_error("Error stopping instance", e))?;
        // A stop discards an archived checkpoint like any other.
        if let Some(url) = manifest_url.as_deref() {
            archive::delete_manifest_logged(&self.store, url, &inner.instance_id).await;
        }

        Ok(Response::new(StopInstanceReply {
            message,
            deduplicated: false,
        }))
    }

    /// Start (resume) a stopped keyed-mode instance: stopped -> pending plus
    /// a placement eval, re-binding the same id and retained storage handle.
    /// See the proto comments on `StartInstance`.
    #[instrument(skip_all)]
    async fn start_instance(
        &self,
        request: Request<StartInstanceRequest>,
    ) -> Result<Response<StartInstanceReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "start_instance",
            inner.idempotency_key.as_deref(),
        );

        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(
                suspend_rpcs::RecordedStart::from_record(recorded).into_reply(true),
            ));
        }

        let instance = self
            .store
            .get_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?;
        let Some(instance) = instance else {
            // An instance archived with `archive = "manifestOnly"` has no
            // row: its archive manifest is the whole record.
            let Some((manifest, url)) = self.find_manifest_only(&inner.instance_id).await? else {
                // A running session needs no start (docs/session-mode.md).
                return match self
                    .find_session(&identity, &inner.instance_id, AclAction::Write)
                    .await?
                {
                    Some(_) => Ok(Response::new(StartInstanceReply {
                        message: format!("Instance {} already running", inner.instance_id),
                        deduplicated: false,
                        resuming_from: String::new(),
                        node_id: String::new(),
                    })),
                    None => Err(Status::not_found(format!(
                        "instance {} not found",
                        inner.instance_id
                    ))),
                };
            };
            acl::enforce(
                &identity,
                AclResource::Job,
                AclAction::Write,
                &AclScope::job(&manifest.envelope.job_id),
            )?;
            let job = self
                .store
                .get_job(&manifest.envelope.job_id)
                .map_err(|e| map_store_error("Error loading job", e))?;
            if job.status == "stopped" {
                return Err(Status::failed_precondition(format!(
                    "job {} is stopped; run the job first, then start its instances",
                    job.name
                )));
            }
            let recorded =
                self.resume_manifest_only(manifest, &url, inner.cold, &idempotency_key)?;
            // The row is back, so the manifest is no longer the record: a
            // later destroy must not leave it to resume the id again.
            archive::delete_manifest_logged(&self.store, &url, &inner.instance_id).await;
            return Ok(Response::new(recorded.into_reply(false)));
        };
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job(&instance.job_id),
        )?;
        if instance.mode != InstanceMode::Keyed {
            return Err(Status::invalid_argument(format!(
                "instance {} is slot-mode; slot lifecycles are owned by the scheduler via \
                 count.num, not StartInstance",
                inner.instance_id
            )));
        }
        if instance.state == InstanceState::Terminated {
            return Err(Status::failed_precondition(format!(
                "instance {} is terminated; destroyed ids are never recycled — create a NEW \
                 instance id instead",
                inner.instance_id
            )));
        }
        // A stopped job schedules nothing: starting one of its instances
        // would only strand it in pending, so reject loudly and point at the
        // fix. (Run the job again, then start its instances — job stop marks
        // keyed instances stopped, and a resubmit deliberately does NOT
        // resurrect them.)
        let job = self
            .store
            .get_job(&instance.job_id)
            .map_err(|e| map_store_error("Error loading job", e))?;
        if job.status == "stopped" {
            return Err(Status::failed_precondition(format!(
                "job {} is stopped; run the job first, then start its instances",
                instance.job_id
            )));
        }

        // A suspended instance resumes from its checkpoint (or boots cold).
        if instance.state == InstanceState::Suspended {
            // An archived instance's checkpoint is read back from its
            // archive manifest; the resume puts the snapshot row back in
            // the same entry.
            let manifest_url = self
                .store
                .instance_suspend_state(&inner.instance_id)
                .map_err(|e| map_store_error("Error loading instance", e))?
                .manifest_url;
            let archived = match manifest_url.as_deref() {
                Some(url) if !inner.cold => {
                    Some(match archive::read_manifest(&self.store, url).await {
                        Ok(manifest) => {
                            ArchivedCheckpoint::Read(Box::new(manifest.snapshot_record()))
                        }
                        Err(err) => ArchivedCheckpoint::Unreadable(err.to_string()),
                    })
                }
                _ => None,
            };
            let recorded =
                self.resume_instance(&instance, inner.cold, archived, &idempotency_key)?;
            if let Some(url) = manifest_url.as_deref() {
                archive::delete_manifest_logged(&self.store, url, &inner.instance_id).await;
            }
            return Ok(Response::new(recorded.into_reply(false)));
        }

        // One scheduler-lane entry: the start, its placement eval (placed
        // promptly, exactly like CreateInstance), and the idempotency record.
        let config_id = self
            .store
            .get_job_config(&instance.job_id)
            .map_err(|e| map_store_error("Error loading job config", e))?
            .id();
        let instance_id = inner.instance_id.clone();
        let job_id = instance.job_id.clone();
        let priority = self.store.default_eval_priority;
        let key = idempotency_key.clone();
        let message = self
            .store
            .scheduler_group_commit("start_instance", move |s| {
                let message = match s.start_keyed_instance(&instance_id)? {
                    StartInstanceOutcome::Started => {
                        s.enqueue_eval(
                            "job_reschedule",
                            Some(&job_id),
                            Some(&config_id),
                            None,
                            Some(priority),
                        )?;
                        format!("Started instance {instance_id}")
                    }
                    StartInstanceOutcome::AlreadyRunning => {
                        format!("Instance {instance_id} already running")
                    }
                };
                s.idempotency_record(&key, "start_instance", &message)?;
                Ok(message)
            })
            .map_err(|e| map_store_error("Error starting instance", e))?;

        Ok(Response::new(StartInstanceReply {
            message,
            deduplicated: false,
            resuming_from: String::new(),
            node_id: String::new(),
        }))
    }

    /// Destroy a keyed-mode instance: stop its allocation, drop its storage
    /// handle, retain the row as a never-recycle tombstone. Idempotent-ish:
    /// destroying an already-terminated instance succeeds as a no-op.
    #[instrument(skip_all)]
    async fn destroy_instance(
        &self,
        request: Request<DestroyInstanceRequest>,
    ) -> Result<Response<DestroyInstanceReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "destroy_instance",
            inner.idempotency_key.as_deref(),
        );

        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(DestroyInstanceReply {
                message,
                deduplicated: true,
            }));
        }

        let instance = self
            .store
            .get_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?;
        let Some(instance) = instance else {
            // A running session has no row either: destroying it is the same
            // as stopping it, since nothing of it remains (docs/session-mode.md).
            if let Some(message) = self
                .end_session(&identity, &inner.instance_id, "destroy")
                .await?
            {
                return Ok(Response::new(DestroyInstanceReply {
                    message,
                    deduplicated: false,
                }));
            }
            // An instance archived with `archive = "manifestOnly"` has no
            // row, so destroying it is deleting its archive manifest.
            let (manifest, url) = self
                .find_manifest_only(&inner.instance_id)
                .await?
                .ok_or_else(|| {
                    Status::not_found(format!("instance {} not found", inner.instance_id))
                })?;
            acl::enforce(
                &identity,
                AclResource::Job,
                AclAction::Write,
                &AclScope::job(&manifest.envelope.job_id),
            )?;
            archive::delete_manifest(&self.store, &url)
                .await
                .map_err(|err| {
                    Status::unavailable(format!(
                        "could not delete the archive manifest of instance {}: {err:#}",
                        inner.instance_id
                    ))
                })?;
            let message = format!("Destroyed instance {}", inner.instance_id);
            let key = idempotency_key.clone();
            let recorded = message.clone();
            self.store
                .scheduler_group_commit("destroy_instance", move |s| {
                    s.idempotency_record(&key, "destroy_instance", &recorded)
                })
                .map_err(|e| map_store_error("Error destroying instance", e))?;
            return Ok(Response::new(DestroyInstanceReply {
                message,
                deduplicated: false,
            }));
        };
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job(&instance.job_id),
        )?;
        let manifest_url = self
            .store
            .instance_suspend_state(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?
            .manifest_url;

        // One scheduler-lane entry: the destroy and its idempotency record.
        // No eval follows, for the reason `stop_instance` gives: the destroy
        // already removed the allocation rows, and waiting work wakes from
        // the blocked-eval index (sandbox-scaling plan, Phases 1 and 3).
        let instance_id = inner.instance_id.clone();
        let key = idempotency_key.clone();
        let message = self
            .store
            .scheduler_group_commit("destroy_instance", move |s| {
                let message = match s.destroy_keyed_instance(&instance_id)? {
                    DestroyInstanceOutcome::Destroyed { .. } => {
                        format!("Destroyed instance {instance_id}")
                    }
                    DestroyInstanceOutcome::AlreadyTerminated => {
                        format!("Instance {instance_id} already terminated")
                    }
                };
                s.idempotency_record(&key, "destroy_instance", &message)?;
                Ok(message)
            })
            .map_err(|e| map_store_error("Error destroying instance", e))?;
        // An archived instance's checkpoint went with it.
        if let Some(url) = manifest_url.as_deref() {
            archive::delete_manifest_logged(&self.store, url, &inner.instance_id).await;
        }

        Ok(Response::new(DestroyInstanceReply {
            message,
            deduplicated: false,
        }))
    }

    /// Operator override for the node-loss confirmation gate: immediately
    /// free a stateful instance's allocation on a LOST node for replacement
    /// (K8s analog: force-deleting a pod stuck on a lost node). See the
    /// proto comments on `ReplaceInstance` for the contract and the
    /// split-brain warning.
    #[instrument(skip_all)]
    async fn replace_instance(
        &self,
        request: Request<ReplaceInstanceRequest>,
    ) -> Result<Response<ReplaceInstanceReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "replace_instance",
            inner.idempotency_key.as_deref(),
        );

        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(ReplaceInstanceReply {
                message,
                deduplicated: true,
            }));
        }

        let Some(instance) = self
            .store
            .get_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?
        else {
            return Err(self
                .missing_instance(&identity, &inner.instance_id, "replaced")
                .await);
        };
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job(&instance.job_id),
        )?;
        let Some(alloc_id) = instance.current_alloc_id else {
            return Err(Status::failed_precondition(format!(
                "instance {} has no current allocation; nothing is gated — a missing \
                 allocation reschedules through the normal eval paths",
                inner.instance_id
            )));
        };
        let allocation = self
            .store
            .get_allocation_by_id(alloc_id)
            .map_err(|e| map_store_error("Error loading allocation", e))?
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "instance {} points at allocation {alloc_id}, which no longer exists; \
                     it is already being replaced",
                    inner.instance_id
                ))
            })?;
        let node = self
            .store
            .get_node(&allocation.node_id)
            .map_err(|e| map_store_error("Error loading node", e))?;
        if node.status != "lost" {
            return Err(Status::failed_precondition(format!(
                "instance {} allocation {alloc_id} is on node {} (status {:?}), not a lost \
                 node; the loss-confirmation gate only ever holds allocations on lost nodes \
                 — there is nothing to force here",
                inner.instance_id, node.id, node.status
            )));
        }

        // Free the gated allocation (instance -> rescheduling, id and
        // storage handle retained) and re-place excluding the lost node.
        self.store
            .remove_allocation_by_id_because(allocation.id, StopCause::Reschedule)
            .map_err(|e| map_store_error("Error removing gated allocation", e))?;
        let config = self
            .store
            .get_job_config(&instance.job_id)
            .map_err(|e| map_store_error("Error loading job config", e))?;
        self.store
            .enqueue_eval(
                "job_reschedule",
                Some(&instance.job_id),
                Some(&config.id()),
                Some(&allocation.node_id),
                Some(self.store.default_eval_priority),
            )
            .map_err(|e| map_store_error("Error scheduling replacement", e))?;

        warn!(
            instance_id = %inner.instance_id,
            allocation_id = alloc_id,
            node_id = %allocation.node_id,
            "Operator override: force-replacing instance allocation on lost node \
             (skipping the loss-confirmation window)"
        );
        let message = format!(
            "Replacing instance {}: freed allocation {alloc_id} on lost node {}",
            inner.instance_id, allocation.node_id
        );
        self.store
            .idempotency_record(&idempotency_key, "replace_instance", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;

        Ok(Response::new(ReplaceInstanceReply {
            message,
            deduplicated: false,
        }))
    }

    /// List a job's stateful instances (slot and keyed, including terminated
    /// tombstones), optionally filtered by group.
    #[instrument(skip_all)]
    async fn list_instances(
        &self,
        request: Request<ListInstancesRequest>,
    ) -> Result<Response<ListInstancesReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Read,
            AclScope::job(&request.get_ref().job_id),
        )?;
        let inner = request.into_inner();
        let group = inner.group.filter(|group| !group.trim().is_empty());

        let mut suspend = self
            .store
            .instance_suspend_states_for_job(&inner.job_id)
            .map_err(|e| map_store_error("Error listing instances", e))?;
        let mut history = self
            .store
            .instance_lineage_for_job(&inner.job_id)
            .map_err(|e| map_store_error("Error listing instances", e))?;
        let addresses = guest_addresses(&self.net_status.lock().unwrap_or_else(|p| p.into_inner()));
        let rows = self
            .store
            .list_instances(&inner.job_id)
            .map_err(|e| map_store_error("Error listing instances", e))?;
        let mut instances: Vec<InstanceInfo> = {
            let usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
            rows.into_iter()
                .filter(|instance| group.as_deref().is_none_or(|group| instance.group == group))
                .map(|instance| {
                    let state = suspend.remove(&instance.instance_id).unwrap_or_default();
                    let history = history.remove(&instance.instance_id).unwrap_or_default();
                    let mut info = map_instance_info(&self.store, &usage, instance, state, history);
                    attach_guest_address(&mut info, &addresses);
                    info
                })
                .collect()
        };

        // Archived instances with no row (`archive = "manifestOnly"`) live
        // only in the bucket: list a page of them after the rows.
        let mut next_idle_page_token = String::new();
        if inner.include_idle {
            let page_size = match inner.idle_page_size {
                0 => archive::DEFAULT_IDLE_PAGE,
                size => size as usize,
            };
            let (listed, next) = archive::list_job_archives(
                &self.store,
                &inner.job_id,
                &inner.idle_page_token,
                page_size,
            )
            .await
            .map_err(|err| {
                Status::unavailable(format!(
                    "could not list the archived instances of job {} in its contentStore: \
                     {err:#}",
                    inner.job_id
                ))
            })?;
            let rows: std::collections::HashSet<String> = instances
                .iter()
                .map(|info| info.instance_id.clone())
                .collect();
            for archived in listed {
                if rows.contains(&archived.instance_id)
                    || group
                        .as_deref()
                        .is_some_and(|group| archived.group != group)
                {
                    continue;
                }
                instances.push(InstanceInfo {
                    instance_id: archived.instance_id,
                    job_id: archived.job_id,
                    group: archived.group,
                    task: archived.task,
                    mode: InstanceMode::Keyed.to_string(),
                    state: InstanceState::Suspended.to_string(),
                    created_at: archived.written_at.clone(),
                    updated_at: archived.written_at,
                    archived: true,
                    manifest_url: Some(archived.url),
                    ..Default::default()
                });
            }
            next_idle_page_token = next;
        }

        // Running sessions have no row: they come from the session index
        // of this manager and its peers, after the rows.
        let (sessions, complete, as_of) = self
            .list_job_sessions(&inner.job_id, group.as_deref())
            .await;
        instances.extend(sessions);

        Ok(Response::new(ListInstancesReply {
            instances,
            next_idle_page_token,
            complete,
            as_of,
        }))
    }

    /// Fetch one stateful instance by id.
    #[instrument(skip_all)]
    async fn get_instance(
        &self,
        request: Request<GetInstanceRequest>,
    ) -> Result<Response<GetInstanceReply>, Status> {
        let identity =
            self.check_acl(&request, AclResource::Job, AclAction::Read, AclScope::any())?;
        let inner = request.into_inner();
        let instance = self
            .store
            .get_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?;
        let Some(instance) = instance else {
            // A running session has no row: answer from the session index.
            if let Some(session) = self
                .find_session(&identity, &inner.instance_id, AclAction::Read)
                .await?
            {
                return Ok(Response::new(GetInstanceReply {
                    instance: Some(self.session_info(&session)),
                    provisional: None,
                    provisional_node_id: String::new(),
                }));
            }
            // An instance archived with `archive = "manifestOnly"` has no
            // row: answer from its archive manifest.
            let (manifest, url) = self
                .find_manifest_only(&inner.instance_id)
                .await?
                .ok_or_else(|| {
                    Status::not_found(format!("instance {} not found", inner.instance_id))
                })?;
            acl::enforce(
                &identity,
                AclResource::Job,
                AclAction::Read,
                &AclScope::job(&manifest.envelope.job_id),
            )?;
            // Its task's `suspend.wake` block still applies: the gateway
            // wakes an archived instance like any suspended one.
            let mut info = manifest_only_instance_info(&manifest, &url);
            attach_wake_and_endpoints(&self.store, &mut info);
            return Ok(Response::new(GetInstanceReply {
                instance: Some(info),
                provisional: None,
                provisional_node_id: String::new(),
            }));
        };
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Read,
            &AclScope::job(&instance.job_id),
        )?;

        // A live, unratified claim for this instance (docs/edge-boot.md). An
        // instance with no allocation and a sandbox demonstrably running on a
        // node is otherwise inexplicable; this is the state that explains it.
        let (provisional, provisional_node_id) = self.provisional_claim_for(&inner.instance_id);
        let suspend = self
            .store
            .instance_suspend_state(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?;
        let history = self
            .store
            .instance_history(&inner.instance_id)
            .map_err(|e| map_store_error("Error loading instance", e))?;

        let usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        let idle_unmeasured = idle_policy_unmeasured_detail(&self.store, &usage, &instance);
        let egress_unenforced = instance_egress_unenforced_detail(&self.store, &instance);
        let mut info = map_instance_info(&self.store, &usage, instance, suspend, history);
        drop(usage);
        attach_guest_address(
            &mut info,
            &guest_addresses(&self.net_status.lock().unwrap_or_else(|p| p.into_inner())),
        );
        attach_wake_and_endpoints(&self.store, &mut info);
        if info.detail.is_none() {
            info.detail = idle_unmeasured.or(egress_unenforced);
        }
        Ok(Response::new(GetInstanceReply {
            instance: Some(info),
            provisional,
            provisional_node_id,
        }))
    }

    // The handler lives in `session_rpcs`, and enforces `job:write` on the
    // session's job itself (coarse `AclScope::any()` first).
    async fn promote_instance(
        &self,
        request: Request<crate::structs::PromoteInstanceRequest>,
    ) -> Result<Response<crate::structs::PromoteInstanceReply>, Status> {
        self.promote_instance_rpc(request).await
    }

    async fn list_session_nodes(
        &self,
        request: Request<crate::structs::ListSessionNodesRequest>,
    ) -> Result<Response<crate::structs::ListSessionNodesReply>, Status> {
        self.list_session_nodes_rpc(request).await
    }

    // === SNAPSHOTS (docs/suspend-and-resume.md) ===
    // The handlers live in `snapshot_rpcs`; each enforces `snapshot` ACLs
    // itself (coarse `AclScope::any()` first, the job scope after lookup).

    async fn create_snapshot(
        &self,
        request: Request<crate::structs::CreateSnapshotRequest>,
    ) -> Result<Response<crate::structs::CreateSnapshotReply>, Status> {
        self.create_snapshot_rpc(request).await
    }

    async fn list_snapshots(
        &self,
        request: Request<crate::structs::ListSnapshotsRequest>,
    ) -> Result<Response<crate::structs::ListSnapshotsReply>, Status> {
        self.list_snapshots_rpc(request).await
    }

    async fn get_snapshot(
        &self,
        request: Request<crate::structs::GetSnapshotRequest>,
    ) -> Result<Response<crate::structs::GetSnapshotReply>, Status> {
        self.get_snapshot_rpc(request).await
    }

    async fn delete_snapshot(
        &self,
        request: Request<crate::structs::DeleteSnapshotRequest>,
    ) -> Result<Response<crate::structs::DeleteSnapshotReply>, Status> {
        self.delete_snapshot_rpc(request).await
    }

    async fn restore_instance(
        &self,
        request: Request<crate::structs::RestoreInstanceRequest>,
    ) -> Result<Response<crate::structs::RestoreInstanceReply>, Status> {
        self.restore_instance_rpc(request).await
    }

    // Phase 6. The handler lives in `fork_rpcs`, and enforces `job:write`
    // and `snapshot:write` on the origin's job itself.
    async fn fork_instance(
        &self,
        request: Request<crate::structs::ForkInstanceRequest>,
    ) -> Result<Response<crate::structs::ForkInstanceReply>, Status> {
        self.fork_instance_rpc(request).await
    }

    // Phase 6. `snapshot:write` on the job, enforced in `fork_rpcs`.
    async fn set_snapshot_alias(
        &self,
        request: Request<crate::structs::SetSnapshotAliasRequest>,
    ) -> Result<Response<crate::structs::SetSnapshotAliasReply>, Status> {
        self.set_snapshot_alias_rpc(request).await
    }

    // Phase 6. `snapshot:write` on the job, enforced in `fork_rpcs`.
    async fn delete_snapshot_alias(
        &self,
        request: Request<crate::structs::DeleteSnapshotAliasRequest>,
    ) -> Result<Response<crate::structs::DeleteSnapshotAliasReply>, Status> {
        self.delete_snapshot_alias_rpc(request).await
    }

    // The handler lives in `snapshot_rpcs`, and enforces `snapshot:read`
    // itself (coarse, then on the snapshot's job).
    async fn warm_snapshot(
        &self,
        request: Request<crate::structs::WarmSnapshotRequest>,
    ) -> Result<Response<crate::structs::WarmSnapshotReply>, Status> {
        self.warm_snapshot_rpc(request).await
    }

    // The handler lives in `suspend_rpcs`, and enforces `job:write` (plus
    // `snapshot:write` for a memory checkpoint) itself.
    async fn suspend_instance(
        &self,
        request: Request<crate::structs::SuspendInstanceRequest>,
    ) -> Result<Response<crate::structs::SuspendInstanceReply>, Status> {
        self.suspend_instance_rpc(request).await
    }

    /// List volumes, optionally filtered. See docs/volumes.md.
    #[instrument(skip_all)]
    async fn list_volumes(
        &self,
        request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesReply>, Status> {
        // Volumes are job-scoped storage, so they read under the `job`
        // resource. Listing gates coarsely and filters per-item below, like
        // the other list endpoints.
        let identity =
            self.check_acl(&request, AclResource::Job, AclAction::Read, AclScope::any())?;
        let inner = request.into_inner();
        let filter = crate::storage::store::VolumeFilter {
            job_id: non_empty(inner.job_id),
            node_id: non_empty(inner.node_id),
            driver: non_empty(inner.driver),
            instance_id: non_empty(inner.instance_id),
            state: inner
                .state
                .as_deref()
                .filter(|state| !state.trim().is_empty())
                .map(|state| {
                    state.parse::<crate::structs::VolumeState>().map_err(|err| {
                        Status::invalid_argument(format!("invalid volume state filter: {err}"))
                    })
                })
                .transpose()?,
        };

        let volumes = self
            .store
            .list_volumes(&filter)
            .map_err(|e| map_store_error("Error listing volumes", e))?
            .into_iter()
            .filter(|volume| match volume.job_id.as_deref() {
                Some(job_id) => {
                    identity.allows(AclResource::Job, AclAction::Read, &AclScope::job(job_id))
                }
                // Volumes with no owning job (operator-created, future) are
                // only visible to callers who can read any job.
                None => identity.allows(AclResource::Job, AclAction::Read, &AclScope::any()),
            })
            .map(volume_info)
            .collect();

        Ok(Response::new(ListVolumesReply { volumes }))
    }

    /// Fetch one volume by id.
    #[instrument(skip_all)]
    async fn get_volume(
        &self,
        request: Request<GetVolumeRequest>,
    ) -> Result<Response<GetVolumeReply>, Status> {
        // Coarse gate first, precise job scope after the lookup, so a
        // credential-less caller cannot probe volume existence via
        // NotFound-vs-PermissionDenied.
        let identity =
            self.check_acl(&request, AclResource::Job, AclAction::Read, AclScope::any())?;
        let inner = request.into_inner();
        let volume = self
            .store
            .get_volume(&inner.volume_id)
            .map_err(|e| map_store_error("Error loading volume", e))?
            .ok_or_else(|| Status::not_found(format!("volume {} not found", inner.volume_id)))?;
        if let Some(job_id) = volume.job_id.as_deref() {
            acl::enforce(
                &identity,
                AclResource::Job,
                AclAction::Read,
                &AclScope::job(job_id),
            )?;
        }

        Ok(Response::new(GetVolumeReply {
            volume: Some(volume_info(volume)),
        }))
    }

    /// Delete a volume: detach it from its instance and queue its storage for
    /// the owning worker's `destroy` (or, with `force`, tombstone it outright).
    #[instrument(skip_all)]
    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let idempotency_key =
            scoped_idempotency_key(&identity, "DeleteVolume", inner.idempotency_key.as_deref());

        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let volume = self
                .store
                .get_volume(&inner.volume_id)
                .map_err(|e| map_store_error("Error loading volume", e))?;
            // A replay must not hand out a volume the caller could not
            // delete. (The record is usually gone by now.)
            if let Some(volume) = volume.as_ref() {
                acl::enforce(
                    &identity,
                    AclResource::Job,
                    AclAction::Write,
                    &volume_write_scope(volume),
                )?;
            }
            return Ok(Response::new(DeleteVolumeReply {
                volume: volume.map(volume_info),
                message,
                deduplicated: true,
            }));
        }

        let existing = self
            .store
            .get_volume(&inner.volume_id)
            .map_err(|e| map_store_error("Error loading volume", e))?
            .ok_or_else(|| Status::not_found(format!("volume {} not found", inner.volume_id)))?;
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Write,
            &volume_write_scope(&existing),
        )?;

        // In-use protection: deleting a volume a live workload is writing to
        // would detach the handle and then reclaim storage under it. Stop the
        // workload first (job stop / instance stop / scale in); --force remains
        // the override for storage that is already unreachable.
        if !inner.force {
            if let Some(who) = self
                .store
                .volume_in_use(&existing)
                .map_err(|e| map_store_error("Error checking volume usage", e))?
            {
                return Err(Status::failed_precondition(format!(
                    "volume {} is in use: {who}. Stop the workload first, or pass --force to \
                     abandon the volume record anyway (the worker will still refuse to reclaim \
                     storage under a live mount)",
                    inner.volume_id
                )));
            }
        }

        let volume = self
            .store
            .delete_volume(&inner.volume_id, inner.force)
            .map_err(|e| map_store_error("Error deleting volume", e))?;
        let message = if inner.force {
            format!(
                "Volume {} marked destroyed without driver confirmation; any data on its node \
                 is left behind",
                volume.volume_id
            )
        } else {
            format!(
                "Volume {} queued for reclamation by its {} driver",
                volume.volume_id, volume.driver
            )
        };
        if !idempotency_key.is_empty() {
            if let Err(err) =
                self.store
                    .idempotency_record(&idempotency_key, "DeleteVolume", &message)
            {
                warn!(error = ?err, "Failed recording idempotency key for DeleteVolume");
            }
        }

        Ok(Response::new(DeleteVolumeReply {
            volume: Some(volume_info(volume)),
            message,
            deduplicated: false,
        }))
    }

    /// The volume drivers installed across the cluster, aggregated from worker
    /// fingerprints (the manager loads no drivers of its own).
    #[instrument(skip_all)]
    async fn list_volume_drivers(
        &self,
        request: Request<ListVolumeDriversRequest>,
    ) -> Result<Response<ListVolumeDriversReply>, Status> {
        // Cluster capability data, like node fingerprints: gated on node read.
        self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Read,
            AclScope::any(),
        )?;
        let drivers = self
            .store
            .cluster_volume_drivers()
            .map_err(|e| map_store_error("Error listing volume drivers", e))?
            .into_iter()
            .map(|(name, entry)| crate::structs::VolumeDriverInfo {
                name,
                node_local: entry.capabilities.node_local,
                ephemeral: entry.capabilities.ephemeral,
                access_modes: entry.capabilities.access_modes,
                volume_modes: entry
                    .capabilities
                    .volume_modes
                    .iter()
                    .map(|mode| mode.to_string())
                    .collect(),
                ops: entry.capabilities.ops,
                required_params: entry.capabilities.required_params,
                optional_params: entry.capabilities.optional_params,
                detail: entry.capabilities.detail,
                nodes: entry.nodes,
                available_nodes: entry.available_nodes,
            })
            .collect();

        Ok(Response::new(ListVolumeDriversReply { drivers }))
    }

    #[instrument(skip_all, fields(alloc_id, identity_name, node_id))]
    async fn issue_workload_identity_token(
        &self,
        request: Request<IssueWorkloadIdentityTokenRequest>,
    ) -> Result<Response<IssueWorkloadIdentityTokenReply>, Status> {
        // ACL: worker-plane endpoint (workers fetch tokens for allocations
        // placed on them; node ownership is enforced against the authenticated
        // principal below).
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
        // Capture the caller's authenticated identity before `identity` is
        // shadowed by the spec-identity lookup further down.
        let caller_is_management = identity.is_management;
        let caller_principal = identity.principal.clone();
        let req = request.into_inner();
        tracing::Span::current().record("alloc_id", req.alloc_id);
        tracing::Span::current().record("identity_name", req.identity_name.as_str());
        tracing::Span::current().record("node_id", req.node_id.as_str());

        let Some(state) = self.workload_identity.as_ref() else {
            return Err(Status::failed_precondition(
                "workload identity is not enabled on this manager \
                 (set workloadIdentity.enabled: true in the manager config)",
            ));
        };

        let allocation = self
            .store
            .get_allocation_by_id(req.alloc_id)
            .map_err(|e| map_store_error("Error loading allocation", e))?
            .ok_or_else(|| Status::not_found(format!("allocation {} not found", req.alloc_id)))?;

        // AuthZ: a worker may only request tokens for allocations placed on it.
        // Bind the request to the *authenticated* principal: a node worker
        // token has principal `node:<node_id>` (minted at WorkerRegister and
        // attached to every worker-plane RPC), so require it to match the
        // node_id in the request. This closes cross-node identity theft — a
        // compromised worker cannot set `node_id` to a peer's and mint that
        // peer's federation credentials. Management/ACL-disabled callers
        // (is_management) are exempt. This mints AWS/GCP/Vault-scoped tokens,
        // so unlike Heartbeat the node id must be proven, not merely claimed.
        if !caller_is_management && caller_principal != format!("node:{}", req.node_id) {
            return Err(Status::permission_denied(format!(
                "worker {caller_principal:?} may not request tokens for node {:?}",
                req.node_id
            )));
        }
        if allocation.node_id != req.node_id {
            return Err(Status::permission_denied(format!(
                "allocation {} is not placed on node {:?}",
                req.alloc_id, req.node_id
            )));
        }

        let task = self
            .store
            .get_task_configs_for_job_config(&allocation.job_config_id)
            .map_err(|e| map_store_error("Error loading task configs", e))?
            .into_iter()
            .find(|(id, _)| *id == allocation.task_config_id)
            .map(|(_, task)| task)
            .ok_or_else(|| {
                Status::internal(format!(
                    "task config {} missing for allocation {}",
                    allocation.task_config_id, req.alloc_id
                ))
            })?;

        // The named identity in the *stored spec* owns audience / TTL /
        // sub template / extra claims — never the request.
        let identity = task
            .identities
            .iter()
            .find(|identity| identity.name == req.identity_name)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "task {} declares no identity named {:?}",
                    task.name, req.identity_name
                ))
            })?;

        // All claims are derived server-side from the scheduling record.
        let claims = claims_for_allocation(&self.store, &allocation, &task, DEF_CLUSTER_REGION)
            .map_err(|e| {
                error!(error = ?e, "Error deriving workload identity claims");
                Status::internal("failed to derive workload identity claims")
            })?;

        let issued = state
            .issue(&claims, identity, std::time::SystemTime::now())
            .map_err(|e| {
                error!(error = ?e, "Error signing workload identity token");
                Status::internal("failed to sign workload identity token")
            })?;

        // Lifecycle log only — never the token bytes.
        info!(
            expires_at = issued.expires_at,
            kid = %issued.kid,
            "Issued workload identity token"
        );

        Ok(Response::new(IssueWorkloadIdentityTokenReply {
            token: issued.token,
            expires_at: issued.expires_at as i64,
            name: req.identity_name,
        }))
    }

    async fn get_mesh_ca(
        &self,
        request: Request<GetMeshCaRequest>,
    ) -> Result<Response<GetMeshCaReply>, Status> {
        self.check_acl(&request, AclResource::Net, AclAction::Read, AclScope::any())?;
        let ca = self
            .store
            .mesh_ca()
            .map_err(|e| map_store_error("Error reading the mesh CA", e))?;
        Ok(Response::new(match ca {
            Some(ca) => {
                let phase = ca.phase(time::OffsetDateTime::now_utc());
                let (phase_name, overlap_until) = match phase {
                    crate::storage::netpolicy_store::CaRotationPhase::Stable => {
                        ("stable", String::new())
                    }
                    crate::storage::netpolicy_store::CaRotationPhase::Overlap { until } => (
                        "overlap",
                        until
                            .format(&time::format_description::well_known::Rfc3339)
                            .unwrap_or_default(),
                    ),
                };
                GetMeshCaReply {
                    bundle_pem: ca.bundle_pem(),
                    pin: ca.pin().unwrap_or_default(),
                    phase: phase_name.to_string(),
                    previous_cert_pem: if overlap_until.is_empty() {
                        String::new()
                    } else {
                        ca.previous_cert_pem.clone().unwrap_or_default()
                    },
                    overlap_until,
                    cert_pem: ca.cert_pem,
                    created_at: ca.created_at,
                }
            }
            None => GetMeshCaReply::default(),
        }))
    }

    async fn rotate_mesh_ca(
        &self,
        request: Request<RotateMeshCaRequest>,
    ) -> Result<Response<RotateMeshCaReply>, Status> {
        // The mesh CA signs every node's certificate, so rotating it is a
        // cluster-wide write. `AclScope::none()` carries no pool, so only an
        // unscoped `net:write` rule matches; `any()` (list gating) would let a
        // pool-scoped net admin rotate trust for every pool.
        let identity = self.check_acl(
            &request,
            AclResource::Net,
            AclAction::Write,
            AclScope::none(),
        )?;
        let ca = self
            .store
            .rotate_mesh_ca()
            .map_err(|e| map_store_error("Error rotating the mesh CA", e))?;
        let overlap_until = match ca.phase(time::OffsetDateTime::now_utc()) {
            crate::storage::netpolicy_store::CaRotationPhase::Overlap { until } => until
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            crate::storage::netpolicy_store::CaRotationPhase::Stable => String::new(),
        };
        info!(
            accessor = identity.accessor_id.as_deref().unwrap_or("-"),
            overlap_until = %overlap_until,
            "Rotated the cluster CA; the new CA signs now, the previous one stays trusted \
             until the overlap ends, and nodes re-issue on their next heartbeat"
        );
        Ok(Response::new(RotateMeshCaReply {
            pin: ca.pin().unwrap_or_default(),
            overlap_until,
            cert_pem: ca.cert_pem,
            created_at: ca.created_at,
        }))
    }

    #[instrument(skip_all, fields(node_id))]
    async fn issue_node_certificate(
        &self,
        request: Request<IssueNodeCertificateRequest>,
    ) -> Result<Response<IssueNodeCertificateReply>, Status> {
        // ACL: worker-plane endpoint. Node ownership is bound to the
        // authenticated principal (same trust model as node identity tokens).
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
        let caller_is_management = identity.is_management;
        let caller_principal = identity.principal.clone();
        let audit_only = identity.audit_only;
        let req = request.into_inner();
        tracing::Span::current().record("node_id", req.node_id.as_str());
        if !caller_is_management && caller_principal != format!("node:{}", req.node_id) {
            if audit_only {
                warn!(
                    target: "acl_audit",
                    principal = %caller_principal,
                    node_id = %req.node_id,
                    "audit-only: a certificate for another node would have been refused"
                );
            } else {
                return Err(Status::permission_denied(format!(
                    "worker {caller_principal:?} may not request a certificate for node {:?}",
                    req.node_id
                )));
            }
        }
        let node = self
            .store
            .get_node(&req.node_id)
            .map_err(|_| Status::not_found(format!("node {:?} is not registered", req.node_id)))?;
        if req.csr_pem.trim().is_empty() {
            return Err(Status::invalid_argument(
                "csr_pem is required: workers generate their own key and send a certificate \
                 signing request; the manager never generates or returns a node's private key",
            ));
        }
        // The certificate's names come from the authenticated caller, never
        // from the CSR. A fleet certificate (the simulator) needs the node to
        // have registered as part of that fleet.
        let subject = if req.fleet.is_empty() {
            crate::manager::mesh_ca::CsrSubject::Node(req.node_id.clone())
        } else {
            let registered = crate::manager::worker_plane::worker_fleet(&node);
            if registered.as_deref() != Some(req.fleet.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "node {:?} did not register as part of fleet {:?} (its workerFleet is {:?})",
                    req.node_id,
                    req.fleet,
                    registered.unwrap_or_default()
                )));
            }
            crate::manager::mesh_ca::CsrSubject::Fleet(req.fleet.clone())
        };
        let ca = self
            .store
            .ensure_mesh_ca()
            .map_err(|e| map_store_error("Error creating the cluster CA", e))?;
        let (cert_pem, expires_at) =
            crate::manager::mesh_ca::sign_csr(&ca.cert_pem, &ca.key_pem, &req.csr_pem, &subject)
                .map_err(|e| {
                    Status::invalid_argument(format!("invalid certificate signing request: {e:#}"))
                })?;
        info!(node_id = %req.node_id, fleet = %req.fleet, expires_at, "Issued node certificate");
        Ok(Response::new(IssueNodeCertificateReply {
            cert_pem: format!("{cert_pem}{}", ca.chain_suffix()),
            ca_pem: ca.bundle_pem(),
            expires_at,
        }))
    }

    #[instrument(skip_all, fields(node_id, identity_name))]
    async fn issue_node_identity_token(
        &self,
        request: Request<IssueNodeIdentityTokenRequest>,
    ) -> Result<Response<IssueNodeIdentityTokenReply>, Status> {
        // ACL: worker-plane endpoint. Node ownership is bound to the
        // authenticated principal below (same trust model as task issuance).
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
        let caller_is_management = identity.is_management;
        let caller_principal = identity.principal.clone();
        let req = request.into_inner();
        tracing::Span::current().record("node_id", req.node_id.as_str());
        tracing::Span::current().record("identity_name", req.identity_name.as_str());

        let Some(state) = self.workload_identity.as_ref() else {
            return Err(Status::failed_precondition(
                "workload identity is not enabled on this manager \
                 (set workloadIdentity.enabled: true in the manager config)",
            ));
        };

        // Bind the request to the authenticated per-node principal — a worker
        // may only mint its own node's credential.
        if !caller_is_management && caller_principal != format!("node:{}", req.node_id) {
            return Err(Status::permission_denied(format!(
                "worker {caller_principal:?} may not request tokens for node {:?}",
                req.node_id
            )));
        }

        // Node identity config owns audience / TTL / sub template.
        let node_identity = state.node_identity(&req.identity_name).ok_or_else(|| {
            Status::not_found(format!(
                "no node identity named {:?} is configured \
                 (declare it under workloadIdentity.nodeIdentities)",
                req.identity_name
            ))
        })?;

        // Region/pool derived server-side from the node's registration record.
        let node = self
            .store
            .get_node(&req.node_id)
            .map_err(|_| Status::not_found(format!("node {:?} is not registered", req.node_id)))?;

        let claims = crate::structs::workload_identity::NodeIdentityClaims {
            region: DEF_CLUSTER_REGION.to_string(),
            pool: node.pool.clone(),
            node_id: req.node_id.clone(),
        };

        let issued = state
            .issue_node(&claims, &node_identity, std::time::SystemTime::now())
            .map_err(|e| {
                error!(error = ?e, "Error signing node identity token");
                Status::internal("failed to sign node identity token")
            })?;

        info!(
            expires_at = issued.expires_at,
            kid = %issued.kid,
            "Issued node identity token"
        );

        Ok(Response::new(IssueNodeIdentityTokenReply {
            token: issued.token,
            expires_at: issued.expires_at as i64,
            name: req.identity_name,
        }))
    }

    #[instrument(skip_all)]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        // ACL: worker-plane endpoint. Requires the node token minted at
        // registration (or a management token) when ACLs are enforced.
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
        crate::manager::telemetry::metrics().heartbeats_total.inc();
        let req = request.into_inner();
        let id = req.id;
        acl::enforce_node_binding(&identity, &id)?;
        let provisional_claims = req.provisional_claims;
        // Checkpoint outcomes first: a failed restore also fails its
        // allocation in `failed_allocations` below, and the instance's
        // restore source must be cleared before that reschedules it, or the
        // replacement would try the same restore again.
        snapshot_rpcs::apply_snapshot_reports(&self.store, &id, &req.snapshot_reports);

        // Offloaded, not called inline. In steady state this is now cheap —
        // one read-model lookup and one map write, no commit and no fsync
        // (docs/node-liveness.md) — but the call is still BLOCKING on the
        // paths that do commit: a `lost -> active` transition, and the
        // mixed-version fallback where a status-lane leader that predates
        // liveness digests makes every beat a replicated write again.
        //
        // Running those on a tokio worker thread parks it for the whole
        // commit, so at most `worker_threads` heartbeats can be in flight —
        // and, worse, every other heartbeat stays stuck in the gRPC layer
        // where the coalescer cannot see it. The coalescer batches whatever
        // is queued when a commit finishes, so starving its queue keeps
        // batches at size 1 and defeats the group commit entirely
        // (HEARTBEAT_BATCH_MAX is 128). Measured on gp3 EBS before the
        // offload: 86 heartbeats/sec, 17% of the 2s cadence, host idle.
        {
            let store = self.store.clone();
            let heartbeat_id = id.clone();
            tokio::task::spawn_blocking(move || store.update_node_heartbeat(heartbeat_id))
                .await
                .map_err(|e| Status::internal(format!("heartbeat task failed: {e}")))?
                .map_err(|e| map_store_error("Error updating node heartbeat", e))?;
        }

        if let Err(err) = handle_failed_allocations(
            &self.store,
            &self.allocation_failures,
            &id,
            &req.failed_allocations,
        ) {
            error!(node_id = %id, error = ?err, "Error handling failed allocations");
        }
        if let Err(err) =
            handle_terminated_allocations(&self.store, &id, &req.terminated_allocations)
        {
            error!(node_id = %id, error = ?err, "Error handling terminated allocations");
        }
        if let Err(err) =
            update_allocation_health_snapshots(&self.store, &id, &req.allocation_health)
        {
            error!(
                node_id = %id,
                error = ?err,
                "Error updating allocation health snapshots"
            );
        }

        // Volume outcomes flow manager-ward here: workers own the drivers, the
        // manager owns the state machine (see docs/volumes.md).
        apply_volume_reports(&self.store, &id, &req.volume_reports);

        // Before the desired set is built, so an allocation the worker just
        // evicted for memory pressure is already gone from this reply and is
        // not handed straight back to it.
        if let Err(err) = handle_pressure_actions(&self.store, &id, &req.pressure_actions) {
            error!(node_id = %id, error = ?err, "Error handling pressure actions");
        }

        // A worker that receives its work over `StreamWork` gets no desired
        // set here: it holds it already, and the heartbeat carries only a
        // digest of it (sandbox-scaling plan §6.13). Its connect and policy
        // feeds are computed from the payloads the stream sent, and only
        // when some allocation needs one, so a steady beat reads nothing.
        let stream = req
            .work_stream
            .as_ref()
            .map(|reported| self.store.work_log().check_heartbeat(&id, reported))
            .unwrap_or_default();
        let (desired_allocations, feed_allocations) = if stream.via_stream {
            let held = self.store.work_log().held_payloads(&id, |allocation| {
                !allocation.task_connect.is_empty()
                    || !allocation.task_network.is_empty()
                    || !allocation.network.is_empty()
            });
            (Vec::new(), held)
        } else {
            let desired = build_desired_allocations(&self.store, &id, &req.allocation_ids)
                .map_err(|e| map_store_error("Error building desired allocations", e))?;
            (desired.clone(), desired)
        };
        // Endpoint feed for the node-local upstream forwarders. Costs nothing
        // (no store access at all) when no desired allocation uses connect, and
        // is infallible by construction: a connect-layer problem must never
        // stop this node from reconciling its allocations.
        let mut upstream_endpoints = build_upstream_endpoints(&self.store, &feed_allocations);
        // Effective network policy per desired allocation, as content hashes.
        // Same contract as the endpoint feed: no store access when no policy
        // exists and no task declares one; never fails the heartbeat.
        let net_policies = self.build_net_policies(&id, &feed_allocations);
        // `proxy` handler targets ride the same feed, flagged `for_handler`.
        upstream_endpoints.extend(self.build_handler_endpoints(&feed_allocations, &net_policies));
        // Denials the node's datapath recorded, turned into events so "which
        // rule blocked it" is answerable without node access.
        for denial in &req.net_denials {
            self.store.events().net_denied(&id, denial);
        }
        // The node's own view of its datapath, replaced wholesale per node.
        {
            let mut status = self.net_status.lock().unwrap_or_else(|p| p.into_inner());
            if req.net_status.is_empty() {
                status.remove(&id);
            } else {
                status.insert(id.clone(), req.net_status.clone());
            }
        }
        // Measured usage, when this beat carries a report (every 10s from
        // workers with enforcement on). In memory only, never the store.
        {
            let now = std::time::Instant::now();
            let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(report) = req.usage.clone() {
                usage.ingest(&id, report, now);
            }
            usage.sweep(now);
        }
        // Content locality (docs/artifacts.md, "Content locality"): fold the
        // node's content report into the in-memory location index, and ask
        // for a full report when this manager cannot apply a delta (it just
        // started or took over, or a reply was lost). Off raft, like usage.
        let content_resync = req.content_locations.as_ref().is_some_and(|report| {
            let outcome = self.store.content_locations().ingest(
                &id,
                report,
                std::time::Instant::now(),
            );
            if outcome == crate::storage::content_index::IngestOutcome::Resync {
                debug!(node_id = %id, base_epoch = report.base_epoch, "Content report needs resync");
                true
            } else {
                false
            }
        });
        // The node's sessions, into the in-memory session index, by the same
        // epoch protocol (docs/session-mode.md). The reply carries a resync
        // request and any duplicate session copies this node should stop.
        let (session_resync, stop_sessions) = match req.session_report.as_ref() {
            Some(report) => {
                let ingest = self
                    .store
                    .sessions()
                    .ingest(&id, report, std::time::Instant::now());
                let resync = ingest.outcome == crate::storage::content_index::IngestOutcome::Resync;
                if resync {
                    debug!(node_id = %id, base_epoch = report.base_epoch, "Session report needs resync");
                }
                (resync, ingest.stop)
            }
            None => (false, Vec::new()),
        };
        // Service-graph samples: fold into per-(caller, service) windows.
        if !req.net_edges.is_empty() {
            let callers: HashMap<i64, (String, String)> = feed_allocations
                .iter()
                .map(|a| (a.allocation_id, (a.job_id.clone(), a.task_name.clone())))
                .collect();
            self.fold_net_edges(&req.net_edges, &callers);
        }
        // Reclaim work for this node. Re-sent every heartbeat until the worker
        // confirms, so a worker that dies mid-destroy simply retries.
        let volume_destroy = build_volume_destroy_requests(&self.store, &id);
        // Checkpoint work for this node, re-sent until it reports (the
        // checkpoint channel, see `snapshot_rpcs`). One indexed lookup.
        let checkpoint_requests = snapshot_rpcs::build_checkpoint_requests(&self.store, &id);
        // Why this node's recently-removed allocations went away. Advisory:
        // a lookup failure costs the worker a cause, never a reconcile, so
        // it is logged and dropped rather than failing the heartbeat. A
        // streaming worker got each one with its stop.
        let stop_notices = if stream.via_stream {
            Vec::new()
        } else {
            match self.store.stop_notices_for_node(&id) {
                Ok(notices) => notices
                    .into_iter()
                    .map(|notice| StopNotice {
                        allocation_id: notice.allocation_id,
                        cause: notice.cause.as_str().to_string(),
                        preemptor_job: notice.preemptor_job,
                        preemptor_band: notice.preemptor_band,
                    })
                    .collect(),
                Err(err) => {
                    debug!(node_id = %id, error = ?err, "Error loading stop notices");
                    Vec::new()
                }
            }
        };

        // Catch-up verdicts for claims this node is still holding that the
        // manager has already decided (docs/edge-boot.md). Empty for every
        // node running only committed work.
        let claim_verdicts = self.claim_verdicts_for(&id, &provisional_claims);
        // Keep the operator-visible view current, and drop the entry entirely
        // when a node reports no claims so the map stays the live set rather
        // than a history of every node that ever held one.
        if let Ok(mut guard) = self.provisional_claims.lock() {
            if provisional_claims.is_empty() {
                guard.remove(&id);
            } else {
                guard.insert(id.clone(), provisional_claims.clone());
            }
        }

        // The cadence this manager wants, when it dictates one at all. On by
        // default (adaptive cadence); absent with `heartbeat.adaptive = false`,
        // and a worker that receives nothing keeps its worker-type
        // `heartbeatSeconds`. A non-leader serves the leader's value, relayed
        // on its liveness digest reply.
        let next_interval_seconds = self
            .store
            .liveness()
            .served_interval()
            .map(|interval| interval.as_secs() as u32);

        // Direct exec (docs/exec-and-actions.md, "Direct exec"): finished
        // sessions join the `exec_audit` log the proxied path writes, and
        // open ones are re-checked against their minting token, as the
        // proxied path re-checks every 30 seconds.
        for record in &req.exec_audit {
            // The same `subject` shape the proxied path logs. An action's
            // rendered argv can hold sensitive parameter values, so workers
            // never report it: the action's name stands in.
            let session = if record.action.is_empty() {
                format!("exec:{}", record.command.join(" "))
            } else {
                format!("action:{}", record.action)
            };
            info!(
                target: "exec_audit",
                node_id = %id,
                jti = %record.jti,
                minted_by = %record.subject,
                allocation_id = record.allocation_id,
                job_id = %record.job,
                task = %record.task,
                subject = %session,
                tty = record.tty,
                exit_code = record.exit_code,
                started_at = record.started_at,
                ended_at = record.ended_at,
                ended_by = %record.ended_by,
                "direct exec session ended"
            );
        }
        let revoked_exec_sessions = if req.exec_sessions.is_empty() {
            Vec::new()
        } else {
            crate::manager::exec::revoked_exec_sessions(&self.store, &self.auth, &req.exec_sessions)
        };

        let (exec_keys, exec_keys_version) = self.exec_keys_for(&req.exec_keys_version);

        let reply = HeartbeatReply {
            net_policies,
            message: "ack".into(),
            desired_allocations,
            upstream_endpoints,
            volume_destroy,
            stop_notices,
            claim_verdicts,
            next_interval_seconds,
            checkpoint_requests,
            content_resync,
            cluster_ca_pin: self.cluster_ca_pin(),
            revoked_exec_sessions,
            exec_keys,
            exec_keys_version,
            manager_addresses: self.worker_manager_addresses(),
            desired_via_stream: stream.via_stream,
            work_resync: stream.resync,
            session_resync,
            stop_sessions,
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all, fields(node_id = %request.get_ref().node_id))]
    async fn get_node_usage(
        &self,
        request: Request<GetNodeUsageRequest>,
    ) -> Result<Response<GetNodeUsageReply>, Status> {
        // Coarse check before the lookup, so a caller without node access
        // cannot tell which node ids exist; the pool scope is enforced after.
        let identity = self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Read,
            AclScope::any(),
        )?;
        let node_id = request.into_inner().node_id;
        let node = self
            .store
            .get_node(&node_id)
            .map_err(|e| map_store_error("Error getting node", e))?;
        acl::enforce(
            &identity,
            AclResource::Node,
            AclAction::Read,
            &AclScope::pool(&node.pool),
        )?;
        Ok(Response::new(self.node_usage_reply(&node)?))
    }

    async fn list_node_usage(
        &self,
        request: Request<ListNodeUsageRequest>,
    ) -> Result<Response<ListNodeUsageReply>, Status> {
        self.list_node_usage_rpc(request).await
    }

    async fn get_node_content(
        &self,
        request: Request<GetNodeContentRequest>,
    ) -> Result<Response<GetNodeContentReply>, Status> {
        self.get_node_content_rpc(request).await
    }

    #[instrument(skip_all)]
    async fn get_heartbeat_cadence(
        &self,
        request: Request<GetHeartbeatCadenceRequest>,
    ) -> Result<Response<GetHeartbeatCadenceReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Read,
            AclScope::any(),
        )?;
        // A manager that does not lead the status lane does not own liveness,
        // so it has no snapshot to report. It answers zeros with
        // `is_liveness_owner: false` rather than a guess: an approximate
        // freshness ratio is worse than an honest "ask the owner".
        if !self.store.is_raft_leader(crate::storage::Lane::Status) {
            return Ok(Response::new(GetHeartbeatCadenceReply {
                missed_beats_before_lost: self.heartbeat.missed_beats_before_lost,
                adaptive_enabled: self.heartbeat.adaptive_enabled(),
                is_liveness_owner: false,
                ..Default::default()
            }));
        }
        let now = std::time::Instant::now();
        let snapshot = crate::manager::daemon::liveness_snapshot(&self.store, now)
            .map_err(|e| map_store_error("Error reading node liveness", e))?;
        let served = self
            .store
            .liveness()
            .served_interval()
            .unwrap_or_else(|| self.heartbeat.parsed_min_interval());
        let report = crate::manager::cadence::cadence_report(
            &snapshot,
            served,
            self.heartbeat.missed_beats_before_lost.max(1),
            self.heartbeat.headroom,
            HEARTBEAT_FRESHNESS_MIN_FLEET,
            self.heartbeat.adaptive_enabled(),
            served >= self.heartbeat.parsed_max_interval(),
        );
        Ok(Response::new(GetHeartbeatCadenceReply {
            active_nodes: report.active_nodes as u32,
            served_interval_seconds: report.served_interval.as_secs() as u32,
            lost_window_seconds: report.lost_window.as_secs() as u32,
            freshness_ratio: report.freshness_ratio.unwrap_or(0.0),
            freshness_measurable: report.freshness_ratio.is_some(),
            sustainable_nodes_estimate: report.sustainable_nodes.unwrap_or(0) as u32,
            suggested_heartbeat_seconds: report.suggested_heartbeat_seconds.unwrap_or(0) as u32,
            adaptive_enabled: report.adaptive_enabled,
            at_max_interval: report.at_max_interval,
            missed_beats_before_lost: report.missed_beats_before_lost,
            is_liveness_owner: true,
        }))
    }

    #[instrument(skip_all)]
    async fn health_check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckReply>, Status> {
        // ACL: worker-plane endpoint (see `heartbeat`).
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
        let req = request.into_inner();
        let id = req.id;
        acl::enforce_node_binding(&identity, &id)?;

        self.store
            .update_node_health_checks(&id, &req.health_checks)
            .map_err(|e| map_store_error("Error updating node health checks", e))?;

        let reply = HealthCheckReply {
            message: "ack".into(),
        };

        Ok(Response::new(reply))
    }

    // ...

    #[instrument(skip_all)]
    async fn worker_pre_register(
        &self,
        request: Request<WorkerPreRegisterRequest>,
    ) -> Result<Response<WorkerPreRegisterReply>, Status> {
        // ACL: worker-plane endpoint. When ACLs are enforced, callers prove
        // membership with a join token secret (or an already-issued worker /
        // management bearer credential).
        let identity = self.authenticate_acl(&request)?;
        let grant =
            self.verify_join_for_registration(&identity, request.get_ref().join_token.as_deref())?;
        let request = request.into_inner();
        let worker_type = request.worker_type;
        self.enforce_registration_grant(&grant, Some(&worker_type), None)?;

        let worker_config = self
            .store
            .get_worker_type(worker_type.clone())
            .map_err(|e| map_store_error("Error getting worker type", e))?;

        let config = WorkerConfig {
            name: worker_type,
            pool: worker_config.pool,
            additional_node_checks: worker_config.additional_node_checks,
            attributes: worker_config.attributes,
            name_prefix: worker_config.name_prefix,
            resources: worker_config.resources,
            reserved_resources: worker_config.reserved_resources,
            max_allocations: worker_config.max_allocations,
            artifacts: worker_config.artifacts,
            host_volumes: worker_config.host_volumes,
            heartbeat_seconds: worker_config.heartbeat_seconds,
            resource_enforcement: worker_config.resource_enforcement,
            edge_boot: worker_config.edge_boot,
            security: worker_config.security,
            container: worker_config.container,
        };

        let reply = WorkerPreRegisterReply {
            config: Some(config),
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn worker_register(
        &self,
        request: Request<WorkerRegisterRequest>,
    ) -> Result<Response<WorkerRegisterReply>, Status> {
        // ACL: worker-plane endpoint (see `worker_pre_register`). Successful
        // registration mints (or rotates) the node token used for heartbeats.
        let identity = self.authenticate_acl(&request)?;
        let grant =
            self.verify_join_for_registration(&identity, request.get_ref().join_token.as_deref())?;
        let request = request.into_inner();
        let node_id = request.name.clone();
        self.enforce_registration_grant(&grant, None, Some((&node_id, &request.pool)))?;

        // Protocol-version handshake (see docs/upgrades.md): accept workers
        // at or below our protocol version, warn on pre-versioning workers
        // (proto3 decodes a missing field as 0), reject newer workers so the
        // managers-first upgrade order is enforced.
        match protocol_compat(request.protocol_version, PROTOCOL_VERSION) {
            VersionCompat::Accept => {}
            VersionCompat::AcceptWithWarn => {
                warn!(
                    node_id = %node_id,
                    manager_protocol_version = PROTOCOL_VERSION,
                    "Worker registered without a protocol version (pre-versioning build); \
                     accepting for rolling-upgrade compatibility — upgrade this worker"
                );
            }
            VersionCompat::Reject => {
                return Err(Status::failed_precondition(format!(
                    "worker {node_id} speaks protocol version {} but this manager only \
                     supports up to {}: the worker was upgraded before the manager. \
                     Upgrade all managers first, then re-join the worker \
                     (see docs/upgrades.md)",
                    request.protocol_version, PROTOCOL_VERSION
                )));
            }
        }

        // Seed the node's dynamic attrs with its versions so `legion node
        // list` and the UI expose fleet skew. An empty build version means a
        // worker built before version reporting existed.
        let dynamic_attrs = json!({
            "buildVersion": if request.build_version.is_empty() {
                "unknown"
            } else {
                request.build_version.as_str()
            },
            "protocolVersion": request.protocol_version,
            // Optional features the worker implements (`checkpoint`, ...).
            // Feature-specific work is only sent to workers that list it.
            "capabilities": request.capabilities,
        })
        .to_string();

        // Re-registration (a worker restart, or a token rotation) keeps the
        // operator's availability: a drained node must not silently become
        // schedulable again. `unavailable` is NOT operator intent — node
        // loss sets it — and a registering worker is by definition alive,
        // so it returns to `available` here, the same way a recovering
        // heartbeat does. Keeping it registered the node `active` but
        // unschedulable, which the scheduler then reported as "No nodes
        // available" (spot-check P0-6). Only a new node starts
        // `available`. The value is resolved here, not in the store's apply
        // step, so every replica applies the same explicit value.
        let availability = match self.store.get_node(&node_id) {
            Ok(existing) if existing.availability == "unavailable" => "available".to_string(),
            Ok(existing) => existing.availability,
            Err(err)
                if matches!(
                    StorageError::from_anyhow(&err),
                    Some(StorageError::NotFound { .. })
                ) =>
            {
                "available".to_string()
            }
            Err(err) => return Err(map_store_error("Error loading node", err)),
        };

        let node_msg = NodeMsg {
            id: request.name,
            fingerprint_attrs: request.fingerprint_attrs,
            config_attrs: request.config_attrs,
            dynamic_attrs,
            status: "active".to_string(),
            availability,
            pool: request.pool,
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            health_checks: request.health_checks,
        };

        self.store
            .insert_node(node_msg)
            .map_err(|e| map_store_error("Error registering worker", e))?;
        // New capacity: let jobs that are short of their count (or have a
        // `count.per` task) try again. Not fatal to the registration; the
        // next leader's regeneration pass reschedules every active job.
        if let Err(err) = self.store.request_capacity_reconcile() {
            warn!(node_id = %node_id, error = %err, "Failed to queue the capacity pass after registration");
        }

        // A worker that re-registers with its own node token (a restart:
        // the token is persisted in its state directory) keeps it — the
        // reply carries none, meaning "keep the one you presented". Rotating
        // it here would make every restart depend on the worker persisting
        // the new secret before anything else goes wrong, and would gain
        // nothing: the caller already proved it holds this node's credential.
        // A join-token (or management) registration mints and rotates.
        let node_token = match &grant {
            _ if !self.auth.enabled => None,
            RegistrationGrant::Node(_) => {
                info!(node_id = %node_id, "Worker re-registered with its node token; keeping it");
                None
            }
            _ => {
                let secret = self.auth.mint_node_token(&self.store, &node_id)?;
                info!(node_id = %node_id, "Minted node token for registered worker");
                Some(secret)
            }
        };

        info!(
            node_id = %node_id,
            worker_build_version = %request.build_version,
            worker_protocol_version = request.protocol_version,
            "Worker registered"
        );
        // Tell the operator now, not at 3am, when this manager can't dial
        // the worker back (worker-plane-auth plan §6.7).
        crate::manager::worker_plane::spawn_probe_after_registration(
            self.store.clone(),
            node_id.clone(),
        );
        // Easter egg: every join is announced the same way on both sides.
        info!(node_id = %node_id, "We are Legion. {node_id} has joined");

        let reply = WorkerRegisterReply {
            message: "Worker registered".into(),
            node_token,
            manager_build_version: env!("CARGO_PKG_VERSION").to_string(),
            manager_protocol_version: PROTOCOL_VERSION,
            manager_addresses: self.worker_manager_addresses(),
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn get_nodes(
        &self,
        request: Request<GetNodesRequest>,
    ) -> Result<Response<GetNodesReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Read,
            AclScope::any(),
        )?;
        let request = request.into_inner();
        let query = normalize_query(request.query);
        let sort_by = request.sort_by.unwrap_or_else(|| "id".to_string());
        let sort_desc = request
            .sort_order
            .as_deref()
            .unwrap_or("asc")
            .eq_ignore_ascii_case("desc");
        let (page, page_size) = parse_page(request.page, request.page_size);

        let nodes = self
            .store
            .get_nodes()
            .map_err(|e| map_store_error("Error getting nodes", e))?;

        let mut node_messages: Vec<NodeMsg> = nodes.into_iter().map(Into::into).collect();
        node_messages.retain(|node| {
            identity.allows(
                AclResource::Node,
                AclAction::Read,
                &AclScope::pool(&node.pool),
            )
        });

        if let Some(query) = query {
            node_messages.retain(|node| {
                node.id.to_lowercase().contains(&query)
                    || node.status.to_lowercase().contains(&query)
                    || node.pool.to_lowercase().contains(&query)
                    || node.availability.to_lowercase().contains(&query)
            });
        }

        node_messages.sort_by(|a, b| {
            let ordering = match sort_by.as_str() {
                "status" => a.status.cmp(&b.status),
                "last_heartbeat" => a.last_heartbeat.cmp(&b.last_heartbeat),
                "pool" => a.pool.cmp(&b.pool),
                _ => a.id.cmp(&b.id),
            };
            if sort_desc {
                ordering.reverse()
            } else {
                ordering
            }
        });

        let (nodes, total_count) = paginate(node_messages, page, page_size);

        Ok(Response::new(GetNodesReply {
            nodes,
            total_count,
            page: page as u32,
            page_size: page_size as u32,
        }))
    }

    #[instrument(skip_all)]
    async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> Result<Response<GetNodeReply>, Status> {
        // Coarse gate before the node lookup ("check before you fetch"), so
        // a caller with no node access cannot probe which node ids exist;
        // the precise pool scope is enforced after the node is loaded.
        let identity = self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Read,
            AclScope::any(),
        )?;
        let id = request.into_inner().id;
        let node = self
            .store
            .get_node(&id)
            .map_err(|e| map_store_error("Error getting node", e))?;
        acl::enforce(
            &identity,
            AclResource::Node,
            AclAction::Read,
            &AclScope::pool(&node.pool),
        )?;

        let provisional_claims = self
            .provisional_claims
            .lock()
            .map(|guard| guard.get(&id).cloned().unwrap_or_default())
            .unwrap_or_default();
        let reply = GetNodeReply {
            node: Some(node.into()),
            provisional_claims,
        };
        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn restart_worker(
        &self,
        request: Request<RestartWorkerRequest>,
    ) -> Result<Response<RestartWorkerReply>, Status> {
        // Coarse gate before the node lookup (see `get_node`).
        let identity = self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Write,
            AclScope::any(),
        )?;
        let node_id = request.into_inner().node_id;
        let node = self.store.get_node(&node_id).map_err(|e| {
            error!(error = ?e, node_id = %node_id, "Failed to get node for restart");
            Status::not_found("Node not found")
        })?;
        acl::enforce(
            &identity,
            AclResource::Node,
            AclAction::Write,
            &AclScope::pool(&node.pool),
        )?;

        let mut client =
            crate::manager::worker_plane::WorkerPlane::global().client(&self.store, &node)?;
        let address = crate::manager::worker_plane::worker_address(&node).unwrap_or_default();
        let response = client
            .restart_worker(tonic::Request::new(RestartWorkerRequest {
                node_id: node_id.clone(),
            }))
            .await
            .map_err(|status| {
                crate::manager::worker_plane::worker_call_error(&node_id, &address, status)
            })?;

        let reply = RestartWorkerReply {
            message: response.into_inner().message,
        };

        Ok(Response::new(reply))
    }

    /// Cordon / drain / uncordon a node. See the proto comments on
    /// `SetNodeAvailability` for the full contract.
    #[instrument(skip_all)]
    async fn set_node_availability(
        &self,
        request: Request<SetNodeAvailabilityRequest>,
    ) -> Result<Response<SetNodeAvailabilityReply>, Status> {
        // Coarse gate before the node lookup ("check before you fetch");
        // the precise pool scope is enforced after the node is loaded.
        let identity = self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let node_id = inner.node_id;
        let availability = inner.availability;
        let evict = inner.evict;
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "set_node_availability",
            inner.idempotency_key.as_deref(),
        );

        if availability != "available" && availability != "drained" {
            return Err(Status::invalid_argument(format!(
                "invalid availability {availability:?}: expected \"available\" or \"drained\""
            )));
        }

        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(SetNodeAvailabilityReply {
                node: None,
                message,
                deduplicated: true,
            }));
        }

        let node = self
            .store
            .get_node(&node_id)
            .map_err(|e| map_store_error("Error setting node availability", e))?;
        acl::enforce(
            &identity,
            AclResource::Node,
            AclAction::Write,
            &AclScope::pool(&node.pool),
        )?;

        // The evict intent rides the replicated write so failover
        // regeneration can tell an evicting drain from a plain cordon
        // (Store::set_node_availability_with_evict).
        let node = self
            .store
            .set_node_availability_with_evict(&node_id, &availability, evict)
            .map_err(|e| map_store_error("Error setting node availability", e))?;

        let draining = evict && availability == "drained";
        if draining {
            self.store
                .enqueue_eval("node_drain", None, None, Some(&node_id), None)
                .map_err(|e| map_store_error("Error enqueueing drain eval", e))?;
        }
        if availability == "available" {
            // An uncordoned node is new capacity, like a registration. The
            // uncordon itself already committed, so a failed enqueue is
            // logged rather than reported as a failed request.
            if let Err(err) = self.store.request_capacity_reconcile() {
                warn!(%node_id, error = %err, "Failed to queue the capacity pass after uncordon");
            }
        }

        let message = if availability == "available" {
            format!("Uncordoned node {node_id}")
        } else if draining {
            format!("Draining node {node_id}")
        } else {
            format!("Cordoned node {node_id}")
        };
        info!(%node_id, %availability, evict, "Node availability changed");

        self.store
            .idempotency_record(&idempotency_key, "set_node_availability", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;

        Ok(Response::new(SetNodeAvailabilityReply {
            node: Some(node.into()),
            message,
            deduplicated: false,
        }))
    }

    #[instrument(skip_all)]
    async fn get_allocation_logs(
        &self,
        request: Request<GetAllocationLogsRequest>,
    ) -> Result<Response<GetAllocationLogsReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let req = request.into_inner();
        // Coarse gate before any lookup ("check before you fetch"), so a
        // caller with no job read anywhere cannot probe which allocation ids
        // exist; the precise job scope is enforced once the job is known.
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Read,
            &AclScope::any(),
        )?;

        // Resolve which allocations to read: a single allocation id when given,
        // otherwise every allocation belonging to the named job. Removed
        // allocations come from the manager's allocation history
        // (docs/logging.md, "Logs of removed allocations").
        let mut removed: Vec<crate::storage::store::RemovedAllocation> = Vec::new();
        let allocations = if let Some(allocation_id) = req.allocation_id {
            let live = self
                .store
                .get_allocation_by_id(allocation_id)
                .map_err(|e| map_store_error("Error loading allocation", e))?;
            match live {
                Some(alloc) => {
                    acl::enforce(
                        &identity,
                        AclResource::Job,
                        AclAction::Read,
                        &AclScope::job(&alloc.job_id),
                    )?;
                    vec![alloc]
                }
                None => {
                    let gone = self
                        .store
                        .removed_allocations(None, Some(allocation_id), 1)
                        .map_err(|e| map_store_error("Error loading allocation history", e))?
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            Status::not_found(format!(
                                "allocation {allocation_id} not found (removed allocations are \
                                 remembered for 24 hours)"
                            ))
                        })?;
                    acl::enforce(
                        &identity,
                        AclResource::Job,
                        AclAction::Read,
                        &AclScope::job(&gone.job_id),
                    )?;
                    removed.push(gone);
                    Vec::new()
                }
            }
        } else {
            acl::enforce(
                &identity,
                AclResource::Job,
                AclAction::Read,
                &AclScope::job(&req.job),
            )?;
            if req.job.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "either job or allocation_id is required",
                ));
            }
            // A destroyed job is gone, but its removed allocations' logs are
            // not: fall through to the history before reporting NotFound.
            let (live, job_missing) = match self.store.get_job(&req.job) {
                Ok(job) => (
                    self.store
                        .allocs_for_job(&job.id)
                        .map_err(|e| map_store_error("Error loading job allocations", e))?,
                    false,
                ),
                Err(err)
                    if matches!(
                        StorageError::from_anyhow(&err),
                        Some(StorageError::NotFound { .. })
                    ) =>
                {
                    (Vec::new(), true)
                }
                Err(err) => return Err(map_store_error("Error loading job", err)),
            };
            if live.is_empty() || req.include_removed {
                removed = self
                    .store
                    .removed_allocations(Some(&req.job), None, REMOVED_ALLOCATION_LOGS_LIMIT)
                    .map_err(|e| map_store_error("Error loading allocation history", e))?;
            }
            if job_missing && removed.is_empty() {
                return Err(Status::not_found(format!(
                    "Error loading job: job {} not found",
                    req.job
                )));
            }
            live
        };

        // Caches keyed by node / job config so we reuse worker connections and
        // task-config lookups across the (often co-located) allocations.
        let mut clients: HashMap<String, WorkerClient<Channel>> = HashMap::new();
        let mut task_names: HashMap<String, HashMap<i64, String>> = HashMap::new();
        let mut logs = Vec::new();

        for allocation in allocations {
            let task_name = task_names
                .entry(allocation.job_config_id.clone())
                .or_insert_with(|| {
                    self.store
                        .get_task_configs_for_job_config(&allocation.job_config_id)
                        .map(|configs| {
                            configs
                                .into_iter()
                                .map(|(id, task)| (id, task.name))
                                .collect()
                        })
                        .unwrap_or_else(|err| {
                            // Task names are best-effort enrichment, so a lookup
                            // failure must not fail the whole logs request — but
                            // log it so a store outage isn't silently masked.
                            warn!(
                                job_config_id = %allocation.job_config_id,
                                error = ?err,
                                "Failed to load task configs for log task-name lookup; task names will be empty"
                            );
                            HashMap::new()
                        })
                })
                .get(&allocation.task_config_id)
                .cloned()
                .unwrap_or_default();

            let mut entry = AllocationLog {
                allocation_id: allocation.id,
                node_id: allocation.node_id.clone(),
                task_name,
                streams: Vec::new(),
                error: String::new(),
                removed: false,
                removal_cause: String::new(),
                removed_at_unix: 0,
                status: allocation.status.clone(),
            };

            match fetch_allocation_logs(&self.store, &allocation, &req, &mut clients).await {
                Ok(streams) => entry.streams = streams,
                Err(message) => entry.error = message,
            }

            logs.push(entry);
        }

        // Removed allocations: the worker still holds their files (until its
        // log retention expires), so the read is the same proxied call.
        for gone in removed {
            let allocation = crate::structs::Allocation {
                id: gone.allocation_id,
                status: gone.status.clone(),
                resources: Value::Null,
                node_id: gone.node_id.clone(),
                job_config_id: gone.job_config_id.clone(),
                task_config_id: gone.task_config_id,
                job_id: gone.job_id.clone(),
                instance_id: None,
                network: None,
            };
            let mut entry = AllocationLog {
                allocation_id: gone.allocation_id,
                node_id: gone.node_id,
                task_name: gone.task_name,
                streams: Vec::new(),
                error: String::new(),
                removed: true,
                removal_cause: gone.cause,
                removed_at_unix: gone.removed_at_unix,
                status: gone.status,
            };
            match fetch_allocation_logs(&self.store, &allocation, &req, &mut clients).await {
                Ok(streams) => entry.streams = streams,
                Err(message) => entry.error = message,
            }
            logs.push(entry);
        }

        Ok(Response::new(GetAllocationLogsReply { allocations: logs }))
    }

    #[instrument(skip_all)]
    async fn update_node_dynamic(
        &self,
        request: Request<UpdateNodeDynamicRequest>,
    ) -> Result<Response<UpdateNodeDynamicReply>, Status> {
        // Coarse gate before the node lookup (see `get_node`).
        let identity = self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Write,
            AclScope::any(),
        )?;
        let r = request.into_inner();
        let id = r.id;
        // Enforce against the node's pool before mutating it.
        let existing = self
            .store
            .get_node(&id)
            .map_err(|e| map_store_error("Error getting node", e))?;
        acl::enforce(
            &identity,
            AclResource::Node,
            AclAction::Write,
            &AclScope::pool(&existing.pool),
        )?;
        let dd_as_map: Map<String, Value> = serde_json::from_str(&r.dynamic_diff).map_err(|e| {
            error!("Error parsing dynamic diff: {:?}", e);
            Status::internal("Error parsing dynamic diff")
        })?;

        let node = self
            .store
            .update_node_dyamic(&id, &dd_as_map)
            .map_err(|e| map_store_error("Error updating node", e))?;
        let allocs = self
            .store
            .allocs_for_node(&id)
            .map_err(|e| map_store_error("Error getting allocations for node", e))?;
        let mut job_configs_to_eval = HashMap::new();
        for alloc in allocs {
            job_configs_to_eval
                .entry(alloc.job_config_id)
                .or_insert(alloc.job_id);
        }
        for (job_config_id, job_id) in job_configs_to_eval {
            self.store
                .enqueue_eval(
                    "job_update",
                    Some(&job_id),
                    Some(&job_config_id),
                    Some(&id),
                    Some(self.store.default_eval_priority),
                )
                .map_err(|e| map_store_error("Error enqueueing eval for node", e))?;
        }
        let reply = UpdateNodeDynamicReply {
            node: Some(node.into()),
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn get_manager_configs(
        &self,
        request: Request<GetManagerConfigsRequest>,
    ) -> Result<Response<GetManagerConfigsReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Read,
            AclScope::none(),
        )?;
        let request = request.into_inner();
        let query = normalize_query(request.query);
        let sort_by = request.sort_by.unwrap_or_else(|| "updated_at".to_string());
        let sort_desc = request
            .sort_order
            .as_deref()
            .unwrap_or("desc")
            .eq_ignore_ascii_case("desc");
        let (page, page_size) = parse_page(request.page, request.page_size);

        let mut configs = self
            .store
            .get_manager_configs()
            .map_err(|e| map_store_error("Error getting manager configs", e))?;

        if let Some(query) = query {
            configs.retain(|config| config.node_id.to_lowercase().contains(&query));
        }

        configs.sort_by(|a, b| {
            let ordering = match sort_by.as_str() {
                "node_id" => a.node_id.cmp(&b.node_id),
                _ => a.updated_at.cmp(&b.updated_at),
            };
            if sort_desc {
                ordering.reverse()
            } else {
                ordering
            }
        });

        let (configs, total_count) = paginate(configs, page, page_size);

        Ok(Response::new(GetManagerConfigsReply {
            configs,
            total_count,
            page: page as u32,
            page_size: page_size as u32,
        }))
    }

    #[instrument(skip_all)]
    async fn get_observability(
        &self,
        request: Request<GetObservabilityRequest>,
    ) -> Result<Response<GetObservabilityReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Read,
            AclScope::none(),
        )?;
        let max_lines = request.into_inner().max_lines.unwrap_or(300).min(1_000) as usize;
        let enabled = o11y_enabled();
        let (mut logs, mut traces) = if enabled {
            read_recent_observability()
                .map_err(|_| Status::internal("Error reading observability data"))?
        } else {
            (Vec::new(), Vec::new())
        };

        if logs.len() > max_lines {
            logs = logs.split_off(logs.len() - max_lines);
        }
        if traces.len() > max_lines {
            traces = traces.split_off(traces.len() - max_lines);
        }
        let persistence = self.store.persistence_status();

        Ok(Response::new(GetObservabilityReply {
            o11y_enabled: enabled,
            logs,
            traces,
            persistence_backend: persistence.backend,
            persistence_role: persistence.role.as_str().to_string(),
            persistence_lease_held: persistence.lease_held,
        }))
    }

    /// The adaptive scheduler-concurrency report
    /// (docs/adaptive-scheduler-concurrency.md).
    ///
    /// Modelled line for line on `get_heartbeat_cadence`, including the "this
    /// manager does not lead the lane, ask the one that does" branch: a
    /// manager that does no planning has no window to report, and an
    /// approximate answer is worse than an honest "ask the leader".
    #[instrument(skip_all)]
    async fn get_scheduler_concurrency(
        &self,
        request: Request<GetSchedulerConcurrencyRequest>,
    ) -> Result<Response<GetSchedulerConcurrencyReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Read,
            AclScope::any(),
        )?;
        let is_leader = self.store.is_raft_leader(crate::storage::Lane::Scheduler);
        let report = crate::manager::concurrency::latest_report();
        let Some(report) = report.filter(|_| is_leader) else {
            // Either this manager does not lead the scheduler lane, or the
            // controller is off (or has not ticked yet). Both answer with
            // zeros; `mode` and `is_scheduler_lane_leader` say which.
            let mode =
                crate::manager::concurrency::AdaptiveConcurrencySettings::from_scheduler_settings(
                    self.store.scheduler_settings.as_ref(),
                )
                .map(|settings| settings.mode)
                .unwrap_or_default();
            return Ok(Response::new(GetSchedulerConcurrencyReply {
                mode: mode.as_str().to_string(),
                eval_threads: self.eval_threads as u32,
                is_scheduler_lane_leader: is_leader,
                ..Default::default()
            }));
        };

        let goodput = report.goodput.unwrap_or(0.0);
        let last = report.last_adjustment;
        Ok(Response::new(GetSchedulerConcurrencyReply {
            mode: report.mode.as_str().to_string(),
            limit: report.limit as u32,
            floor: report.floor as u32,
            ceiling: report.ceiling as u32,
            eval_threads: report.eval_threads as u32,
            in_flight: report.in_flight as u32,
            state: report.state.as_str().to_string(),
            seconds_in_state: report.seconds_in_state as u32,
            conflict_rate: report.conflict_rate.unwrap_or(0.0),
            fallback_rate: report.fallback_rate.unwrap_or(0.0),
            goodput,
            window_applied: report.window.applied as u32,
            window_fallback: report.window.fallback as u32,
            window_rejected: report.window.rejected as u32,
            last_adjustment_direction: last
                .map(|adjustment| adjustment.direction.as_str().to_string())
                .unwrap_or_default(),
            last_adjustment_from: last.map(|adjustment| adjustment.from).unwrap_or(0) as u32,
            last_adjustment_to: last.map(|adjustment| adjustment.to).unwrap_or(0) as u32,
            last_adjustment_seconds_ago: last
                .map(|adjustment| {
                    (u64::from(adjustment.ticks_ago)
                        * crate::manager::concurrency::TICK_PERIOD.as_millis() as u64)
                        / 1_000
                })
                .unwrap_or(0) as u32,
            last_adjustment_goodput_delta: last
                .filter(|adjustment| adjustment.goodput_before > 0.0)
                .map(|adjustment| (goodput - adjustment.goodput_before) / adjustment.goodput_before)
                .unwrap_or(0.0),
            admission_skips: report.admission_skips,
            is_scheduler_lane_leader: true,
        }))
    }

    #[instrument(skip_all)]
    async fn get_raft_status(
        &self,
        request: Request<GetRaftStatusRequest>,
    ) -> Result<Response<GetRaftStatusReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Read,
            AclScope::none(),
        )?;
        let status = self
            .store
            .raft_status()
            .map(map_raft_status)
            .unwrap_or(RaftStatus {
                enabled: false,
                backend: self.store.persistence_status().backend,
                node_id: String::new(),
                cluster_id: String::new(),
                role: "standby".to_string(),
                leader_id: None,
                term: 0,
                commit_index: 0,
                applied_index: 0,
                lease_held: false,
                voters: Vec::new(),
                learners: Vec::new(),
                peers: Vec::new(),
                election_epoch_ms: 0,
                join_token_ttl_seconds: 0,
                raft_library: String::new(),
                lanes: Vec::new(),
            });
        Ok(Response::new(GetRaftStatusReply {
            status: Some(status),
        }))
    }

    #[instrument(skip_all)]
    async fn list_raft_history(
        &self,
        request: Request<ListRaftHistoryRequest>,
    ) -> Result<Response<ListRaftHistoryReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Read,
            AclScope::none(),
        )?;
        let limit = request.into_inner().limit.unwrap_or(50).min(500) as usize;
        let events = self
            .store
            .raft_history(limit)
            .into_iter()
            .map(map_raft_event)
            .collect();
        Ok(Response::new(ListRaftHistoryReply { events }))
    }

    #[instrument(skip_all)]
    async fn issue_raft_command(
        &self,
        request: Request<IssueRaftCommandRequest>,
    ) -> Result<Response<IssueRaftCommandReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let actor = req.actor.unwrap_or_else(|| "api".to_string());
        // Off the async workers: `transfer-leader` / `step-down` wait (up to
        // their deadline) for leadership to move, and membership changes
        // wait for commits.
        let outcome = {
            let store = self.store.clone();
            let command = req.command.clone();
            let target = req.target_node_id.clone();
            tokio::task::spawn_blocking(move || {
                store.issue_raft_command(&command, target.as_deref(), &actor)
            })
            .await
            .map_err(|e| Status::internal(format!("raft command task failed: {e}")))?
            .map_err(|e| map_store_error("Error issuing raft command", e))?
        };
        let status = self
            .store
            .raft_status()
            .map(map_raft_status)
            .ok_or_else(|| Status::failed_precondition("raft backend is not enabled"))?;
        Ok(Response::new(IssueRaftCommandReply {
            command: req.command,
            event: Some(map_raft_event(outcome.event)),
            status: Some(status),
            warning: outcome.warning,
        }))
    }

    #[instrument(skip_all)]
    async fn get_raft_join_token(
        &self,
        request: Request<GetRaftJoinTokenRequest>,
    ) -> Result<Response<GetRaftJoinTokenReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Write,
            AclScope::none(),
        )?;
        let actor = request
            .into_inner()
            .actor
            .unwrap_or_else(|| "api".to_string());
        let (token, expires_at) = self
            .store
            .issue_raft_join_token(&actor)
            .map_err(|e| map_store_error("Error issuing raft join token", e))?;
        let status = self
            .store
            .raft_status()
            .map(map_raft_status)
            .ok_or_else(|| Status::failed_precondition("raft backend is not enabled"))?;
        Ok(Response::new(GetRaftJoinTokenReply {
            token,
            expires_at,
            status: Some(status),
        }))
    }

    #[instrument(skip_all)]
    async fn get_state_backup(
        &self,
        request: Request<GetStateBackupRequest>,
    ) -> Result<Response<GetStateBackupReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let out = req.out_path.as_ref().map(std::path::Path::new);
        // All-lane-leader-only in raft mode (the image spans every lane's
        // tables; JC-40 co-locates leadership so this is one node):
        // `state_backup` returns the typed NotRaftLeader error, which
        // map_store_error turns into FAILED_PRECONDITION with the leader
        // hint (plan §A4/§A5).
        let info = self
            .store
            .state_backup(out)
            .map_err(|e| map_store_error("Error creating state backup", e))?;
        // Tell the operator, right where the backup is reported, what the
        // file does NOT contain: the root key (docs/kv-encryption.md).
        let snapshot = self.store.keyring().snapshot();
        let counts = self
            .store
            .kv_encryption_counts()
            .map_err(|e| map_store_error("Error counting kv rows", e))?;
        let mut root_key_ids: Vec<String> = snapshot
            .rows
            .iter()
            .map(|row| row.root_key_id.clone())
            .collect();
        root_key_ids.sort();
        root_key_ids.dedup();
        Ok(Response::new(GetStateBackupReply {
            path: info.path.display().to_string(),
            bytes: info.bytes,
            applied_index: info.applied_index,
            encryption: Some(BackupEncryptionInfo {
                enabled: snapshot.enabled,
                root_key_provider: snapshot.provider,
                root_key_location: snapshot.location,
                root_key_ids,
                plaintext_rows: counts.get(&None).copied().unwrap_or(0),
            }),
        }))
    }

    #[instrument(skip_all)]
    async fn get_keyring_status(
        &self,
        request: Request<GetKeyringStatusRequest>,
    ) -> Result<Response<GetKeyringStatusReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Read,
            AclScope::none(),
        )?;
        Ok(Response::new(keyring_status_reply(self)?))
    }

    #[instrument(skip_all)]
    async fn rotate_data_key(
        &self,
        request: Request<RotateDataKeyRequest>,
    ) -> Result<Response<RotateDataKeyReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let idempotency_key =
            scoped_idempotency_key(&identity, "rotate_data_key", req.idempotency_key.as_deref());
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let recorded: Value = serde_json::from_str(&message).unwrap_or_default();
            return Ok(Response::new(RotateDataKeyReply {
                key_id: recorded["keyId"].as_str().unwrap_or_default().to_string(),
                retired_key_id: recorded["retiredKeyId"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                deduplicated: true,
            }));
        }
        let retired_key_id = self
            .store
            .keyring()
            .active_key()
            .map(|(id, _)| id)
            .unwrap_or_default();
        let row = self
            .store
            .rotate_data_key("operator request")
            .map_err(|e| map_store_error("Error rotating the KV data key", e))?;
        // The keyring loop starts the background rewrap right away.
        self.store.keyring().wake();
        let message = json!({ "keyId": row.key_id, "retiredKeyId": retired_key_id }).to_string();
        self.store
            .idempotency_record(&idempotency_key, "rotate_data_key", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(RotateDataKeyReply {
            key_id: row.key_id,
            retired_key_id,
            deduplicated: false,
        }))
    }

    #[instrument(skip_all)]
    async fn rewrap_kv(
        &self,
        request: Request<RewrapKvRequest>,
    ) -> Result<Response<RewrapKvReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let idempotency_key =
            scoped_idempotency_key(&identity, "rewrap_kv", req.idempotency_key.as_deref());
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let recorded: Value = serde_json::from_str(&message).unwrap_or_default();
            return Ok(Response::new(RewrapKvReply {
                rewrapped: recorded["rewrapped"].as_i64().unwrap_or(0),
                remaining: recorded["remaining"].as_i64().unwrap_or(0),
                collected_keys: Vec::new(),
                deduplicated: true,
            }));
        }
        let run = self
            .store
            .run_rewrap(req.plaintext)
            .map_err(|e| map_store_error("Error rewrapping kv values", e))?;
        let message = json!({ "rewrapped": run.rewrapped, "remaining": run.remaining }).to_string();
        self.store
            .idempotency_record(&idempotency_key, "rewrap_kv", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(RewrapKvReply {
            rewrapped: run.rewrapped,
            remaining: run.remaining,
            collected_keys: run.collected_keys,
            deduplicated: false,
        }))
    }

    #[instrument(skip_all)]
    async fn rewrap_root_key(
        &self,
        request: Request<RewrapRootKeyRequest>,
    ) -> Result<Response<RewrapRootKeyReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let idempotency_key =
            scoped_idempotency_key(&identity, "rewrap_root_key", req.idempotency_key.as_deref());
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let recorded: Value = serde_json::from_str(&message).unwrap_or_default();
            return Ok(Response::new(RewrapRootKeyReply {
                keys: recorded["keys"].as_i64().unwrap_or(0),
                root_key_id: recorded["rootKeyId"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                skipped: Vec::new(),
                deduplicated: true,
            }));
        }
        let outcome = self
            .store
            .rewrap_root_key()
            .map_err(|e| map_store_error("Error rewrapping the KV keyring", e))?;
        let message = json!({ "keys": outcome.rows, "rootKeyId": outcome.root_key_id }).to_string();
        self.store
            .idempotency_record(&idempotency_key, "rewrap_root_key", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(RewrapRootKeyReply {
            keys: outcome.rows as i64,
            root_key_id: outcome.root_key_id,
            skipped: outcome.skipped,
            deduplicated: false,
        }))
    }

    #[instrument(skip_all)]
    async fn reset_keyring(
        &self,
        request: Request<ResetKeyringRequest>,
    ) -> Result<Response<ResetKeyringReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Operator,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        if req.confirm != KEYRING_RESET_CONFIRMATION {
            return Err(Status::invalid_argument(format!(
                "keyring reset deletes every KV value this manager cannot decrypt; confirm with \
                 `confirm = \"{KEYRING_RESET_CONFIRMATION}\"` (the CLI flag is \
                 --i-understand-encrypted-values-will-be-lost)"
            )));
        }
        let outcome = self
            .store
            .reset_keyring()
            .map_err(|e| map_store_error("Error resetting the KV keyring", e))?;
        // Every deleted value is a delete for the jobs that depended on it:
        // they fail through `missing_kv_dependency` and recover on a put.
        for key in &outcome.deleted_values {
            if !self.queue_kv_change(key, true) {
                reconcile_kv_changes(&self.store, &[(key.clone(), true)])?;
            }
        }
        let active_key_id = self
            .store
            .keyring()
            .active_key()
            .map(|(id, _)| id)
            .unwrap_or_default();
        Ok(Response::new(ResetKeyringReply {
            deleted_values: outcome.deleted_values,
            dropped_keys: outcome.dropped_keys,
            active_key_id,
        }))
    }

    #[instrument(skip_all)]
    async fn get_ledger(
        &self,
        request: Request<GetLedgerRequest>,
    ) -> Result<Response<GetLedgerReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Read,
            AclScope::any(),
        )?;
        let ledger = self
            .store
            .get_ledger()
            .map_err(|e| map_store_error("Error getting ledger", e))?;

        // Enrich allocations with per-artifact staging states from the
        // latest worker health snapshots (artifact system plan §9).
        let allocation_ids: Vec<i64> = ledger
            .iter()
            .flat_map(|entry| entry.allocations.iter().map(|allocation| allocation.id))
            .collect();
        let allocation_health = self
            .store
            .allocation_health_for_allocations(&allocation_ids)
            .map_err(|e| map_store_error("Error getting allocation health", e))?;
        // Task names, so a client can group a job's allocations by task
        // without a second lookup.
        let task_config_ids: Vec<i64> = ledger
            .iter()
            .flat_map(|entry| {
                entry
                    .allocations
                    .iter()
                    .map(|allocation| allocation.task_config_id)
            })
            .collect();
        let task_names = self
            .store
            .task_names_for_configs(&task_config_ids)
            .map_err(|e| map_store_error("Error getting task names", e))?;

        let mut nodes: Vec<LedgerNode> = ledger.into_iter().map(Into::into).collect();
        for node in &mut nodes {
            for allocation in &mut node.allocations {
                allocation.artifact_states = allocation_health
                    .get(&allocation.id)
                    .and_then(|snapshot| snapshot.artifact_states.clone());
                allocation.task_name = task_names
                    .get(&allocation.task_config_id)
                    .cloned()
                    .unwrap_or_default();
            }
        }
        nodes.retain(|entry| match &entry.node {
            Some(node) => identity.allows(
                AclResource::Node,
                AclAction::Read,
                &AclScope::pool(&node.pool),
            ),
            None => true,
        });

        let reply: GetLedgerReply = GetLedgerReply { nodes };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn upsert_net_policy(
        &self,
        request: Request<UpsertNetPolicyRequest>,
    ) -> Result<Response<UpsertNetPolicyReply>, Status> {
        // Coarse gate before the body is parsed, precise per-pool scope after.
        let identity = self.check_acl(
            &request,
            AclResource::Net,
            AclAction::Write,
            AclScope::any(),
        )?;
        let req = request.into_inner();
        let mut object: crate::structs::netpolicy::NetPolicyObject =
            serde_json::from_str(&req.object_json)
                .map_err(|e| Status::invalid_argument(format!("invalid net policy object: {e}")))?;
        object
            .normalize_and_validate()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        enforce_net_policy_scope(&identity, AclAction::Write, &object)?;
        enforce_kv_cert_read(&identity, &[object.ingress.as_ref()])?;

        let affected = self.affected_allocations_for_policy(&object);
        if req.dry_run {
            return Ok(Response::new(UpsertNetPolicyReply {
                policy: Some(net_policy_to_proto(&object, "", "")),
                affected_allocations: affected,
            }));
        }
        let record = self
            .store
            .upsert_net_policy(object)
            .map_err(|e| map_store_error("Error saving net policy", e))?;
        self.bump_net_policy_generation();
        self.store.events().net_policy_applied(
            &record.object.name,
            &record.object.enforcement,
            record.object.priority,
            affected.len(),
        );
        Ok(Response::new(UpsertNetPolicyReply {
            policy: Some(net_policy_to_proto(
                &record.object,
                &record.created_at,
                &record.updated_at,
            )),
            affected_allocations: affected,
        }))
    }

    #[instrument(skip_all)]
    async fn get_net_policy_object(
        &self,
        request: Request<GetNetPolicyObjectRequest>,
    ) -> Result<Response<GetNetPolicyObjectReply>, Status> {
        let identity =
            self.check_acl(&request, AclResource::Net, AclAction::Read, AclScope::any())?;
        let name = request.into_inner().name;
        let record = self
            .store
            .get_net_policy(&name)
            .map_err(|e| map_store_error("Error loading net policy", e))?
            .ok_or_else(|| Status::not_found(format!("net policy '{name}' not found")))?;
        enforce_net_policy_scope(&identity, AclAction::Read, &record.object)?;
        Ok(Response::new(GetNetPolicyObjectReply {
            policy: Some(net_policy_to_proto(
                &record.object,
                &record.created_at,
                &record.updated_at,
            )),
        }))
    }

    #[instrument(skip_all)]
    async fn list_net_policies(
        &self,
        request: Request<ListNetPoliciesRequest>,
    ) -> Result<Response<ListNetPoliciesReply>, Status> {
        let identity =
            self.check_acl(&request, AclResource::Net, AclAction::Read, AclScope::any())?;
        let records = self
            .store
            .list_net_policies()
            .map_err(|e| map_store_error("Error listing net policies", e))?;
        let policies = records
            .iter()
            .filter(|record| {
                enforce_net_policy_scope(&identity, AclAction::Read, &record.object).is_ok()
            })
            .map(|record| {
                net_policy_to_proto(&record.object, &record.created_at, &record.updated_at)
            })
            .collect();
        let generation = self.store.net_policy_generation().unwrap_or(0);
        Ok(Response::new(ListNetPoliciesReply {
            policies,
            generation,
        }))
    }

    #[instrument(skip_all)]
    async fn delete_net_policy(
        &self,
        request: Request<DeleteNetPolicyRequest>,
    ) -> Result<Response<DeleteNetPolicyReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Net,
            AclAction::Write,
            AclScope::any(),
        )?;
        let name = request.into_inner().name;
        let record = self
            .store
            .get_net_policy(&name)
            .map_err(|e| map_store_error("Error loading net policy", e))?
            .ok_or_else(|| Status::not_found(format!("net policy '{name}' not found")))?;
        enforce_net_policy_scope(&identity, AclAction::Write, &record.object)?;
        self.store
            .delete_net_policy(&name)
            .map_err(|e| map_store_error("Error deleting net policy", e))?;
        self.bump_net_policy_generation();
        self.store.events().net_policy_deleted(&name);
        Ok(Response::new(DeleteNetPolicyReply {}))
    }

    #[instrument(skip_all)]
    async fn show_net_policy(
        &self,
        request: Request<ShowNetPolicyRequest>,
    ) -> Result<Response<ShowNetPolicyReply>, Status> {
        let identity =
            self.check_acl(&request, AclResource::Net, AclAction::Read, AclScope::any())?;
        let req = request.into_inner();
        acl::enforce(
            &identity,
            AclResource::Net,
            AclAction::Read,
            &AclScope::job(&req.job_id),
        )?;
        let (task, facts) =
            self.net_policy_task_facts(&req.job_id, &req.task_name, &req.node_pool)?;
        let (effective, matched) = self.effective_net_policy(&facts, task.network.as_ref());
        let declared_mode = task
            .network
            .as_ref()
            .map(|n| n.mode.clone())
            .unwrap_or_else(|| "auto".to_string());
        let effective_mode = effective.mode.clone().unwrap_or_else(|| {
            crate::structs::network::resolve_network_mode_with(
                &declared_mode,
                &task.runtime,
                &crate::structs::network::AutoModeFacts {
                    node_os: self.cluster_node_os(),
                    ..crate::structs::network::AutoModeFacts::for_task(&task)
                },
                self.store.sandbox_gated_default(),
            )
        });
        let node_status_json = self.net_status_for_task(&req.job_id, &task.name);
        let notes = self.egress_unenforced_notes(&req.job_id, &task);
        Ok(Response::new(ShowNetPolicyReply {
            effective_json: serde_json::to_string(&effective).unwrap_or_default(),
            matched_policies: matched,
            declared_mode,
            effective_mode,
            node_status_json,
            notes,
        }))
    }

    #[instrument(skip_all)]
    async fn test_net_policy(
        &self,
        request: Request<TestNetPolicyRequest>,
    ) -> Result<Response<TestNetPolicyReply>, Status> {
        use crate::structs::netpolicy::{
            evaluate_connect, evaluate_request, ConnectFacts, RequestFacts, Verdict,
        };
        let identity =
            self.check_acl(&request, AclResource::Net, AclAction::Read, AclScope::any())?;
        let req = request.into_inner();
        acl::enforce(
            &identity,
            AclResource::Net,
            AclAction::Read,
            &AclScope::job(&req.job_id),
        )?;
        let (task, facts) =
            self.net_policy_task_facts(&req.job_id, &req.task_name, &req.node_pool)?;
        let (effective, _) = self.effective_net_policy(&facts, task.network.as_ref());

        let (direction, chain, target) = if !req.to.trim().is_empty() {
            (
                "egress",
                effective.egress.as_ref(),
                req.to.trim().to_string(),
            )
        } else if !req.from.trim().is_empty() {
            (
                "ingress",
                effective.ingress.as_ref(),
                req.from.trim().to_string(),
            )
        } else {
            return Err(Status::invalid_argument(
                "set `to` (egress) or `from` (ingress)",
            ));
        };
        let parsed = parse_test_target(&target);
        let (host, ip, port, mut path) = parsed;
        if !req.path.trim().is_empty() {
            path = Some(req.path.trim().to_string());
        }
        // The host guard sits below every policy, so it answers first, by
        // the address the name resolves to (resolved here, on the manager).
        // Its built-in classes are the same on every node; a node's own
        // addresses and its `hostGuard` overrides are judged on the worker.
        if direction == "egress" {
            let guard_host = host
                .clone()
                .or_else(|| ip.map(|ip| ip.to_string()))
                .unwrap_or_default();
            if !guard_host.is_empty() {
                let guard = crate::structs::host_guard::HostGuard::default();
                let refused = crate::worker::netd::guard::resolve_and_guard(
                    &crate::worker::netd::guard::SystemResolver,
                    Some(&guard),
                    &guard_host,
                    port,
                )
                .await;
                if let Err(crate::worker::netd::guard::GuardRefusal::Denied(reason)) = refused {
                    let hint = crate::structs::host_guard::GuardDenial::hint();
                    return Ok(Response::new(TestNetPolicyReply {
                        verdict: "deny".into(),
                        source: crate::structs::host_guard::HOST_GUARD_SOURCE.into(),
                        explanation: format!(
                            "{}: {reason}",
                            crate::structs::host_guard::HOST_GUARD_SOURCE
                        ),
                        reason,
                        direction: direction.into(),
                        protocol: chain.map(|c| c.protocol.clone()).unwrap_or_default(),
                        hint: hint.to_string(),
                    }));
                }
            }
        }
        let Some(chain) = chain else {
            // A gated task's egress that nothing covers is denied
            // (`gated-default`), exactly as its datapath would.
            let declared_mode = task
                .network
                .as_ref()
                .map(|network| network.mode.as_str())
                .unwrap_or(crate::structs::netpolicy::MODE_AUTO);
            let mode = effective.mode.clone().unwrap_or_else(|| {
                crate::structs::network::resolve_network_mode_with(
                    declared_mode,
                    &task.runtime,
                    &crate::structs::network::AutoModeFacts {
                        node_os: self.cluster_node_os(),
                        ..crate::structs::network::AutoModeFacts::for_task(&task)
                    },
                    self.store.sandbox_gated_default(),
                )
            });
            if direction == "egress" && mode == crate::structs::netpolicy::MODE_GATED {
                let destination = host
                    .clone()
                    .or_else(|| ip.map(|ip| ip.to_string()))
                    .unwrap_or_else(|| target.clone());
                let reason = format!(
                    "task {} is gated and nothing allows {destination}",
                    task.name
                );
                return Ok(Response::new(TestNetPolicyReply {
                    verdict: "deny".into(),
                    source: crate::structs::netpolicy::SOURCE_GATED_DEFAULT.into(),
                    explanation: format!(
                        "{target}: denied by {} — {reason}",
                        crate::structs::netpolicy::SOURCE_GATED_DEFAULT
                    ),
                    reason,
                    direction: direction.into(),
                    protocol: String::new(),
                    hint: crate::structs::netpolicy::gated_default_hint(&destination),
                }));
            }
            return Ok(Response::new(TestNetPolicyReply {
                verdict: "allow".into(),
                source: String::new(),
                reason: String::new(),
                direction: direction.into(),
                protocol: String::new(),
                explanation: format!(
                    "no {direction} policy applies to this task; everything is allowed"
                ),
                hint: String::new(),
            }));
        };
        let identity_str = req.identity.trim();
        let identity_opt = (!identity_str.is_empty()).then_some(identity_str);
        let verdict = if !req.method.trim().is_empty() || path.is_some() {
            if !chain.is_http() {
                // A tcp chain never sees requests; report the connect verdict.
                evaluate_connect(
                    chain,
                    &ConnectFacts {
                        host: host.as_deref(),
                        ip,
                        port,
                        identity: identity_opt,
                    },
                )
            } else {
                let method = if req.method.trim().is_empty() {
                    "GET"
                } else {
                    req.method.trim()
                };
                evaluate_request(
                    chain,
                    &RequestFacts {
                        host: host.as_deref(),
                        ip,
                        port,
                        identity: identity_opt,
                        method,
                        path: path.as_deref().unwrap_or("/"),
                        headers: &[],
                    },
                )
            }
        } else {
            evaluate_connect(
                chain,
                &ConnectFacts {
                    host: host.as_deref(),
                    ip,
                    port,
                    identity: identity_opt,
                },
            )
        };
        let (verdict_str, source, reason) = match &verdict {
            Verdict::Allow => ("allow".to_string(), String::new(), String::new()),
            Verdict::Deny { source, reason } => (
                "deny".to_string(),
                source.clone(),
                reason.clone().unwrap_or_default(),
            ),
            Verdict::Defer => ("defer".to_string(), String::new(), String::new()),
        };
        let explanation = match &verdict {
            Verdict::Allow if chain.denies_by_default() => format!("{target}: allowed by a rule"),
            Verdict::Allow => format!("{target}: allowed (no rule matched; policy is allow)"),
            Verdict::Deny { source, reason } => match reason {
                Some(reason) => format!("{target}: denied by {source} — {reason}"),
                None => format!("{target}: denied by {source}"),
            },
            Verdict::Defer => format!(
                "{target}: a request-time rule (path/method/header) would decide; pass --method/--path to test it"
            ),
        };
        Ok(Response::new(TestNetPolicyReply {
            verdict: verdict_str,
            source,
            reason,
            direction: direction.into(),
            protocol: chain.protocol.clone(),
            explanation,
            hint: String::new(),
        }))
    }

    #[instrument(skip_all)]
    async fn fetch_net_policy(
        &self,
        request: Request<FetchNetPolicyRequest>,
    ) -> Result<Response<FetchNetPolicyReply>, Status> {
        // ACL: worker-plane endpoint (nodes fetch the merged policy bodies the
        // heartbeat reply named for allocations placed on them).
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
        let req = request.into_inner();
        acl::enforce_node_binding(&identity, &req.node_id)?;
        if req.hash.trim().is_empty() {
            return Err(Status::invalid_argument("hash is required"));
        }
        let cached = {
            let cache = self
                .net_policy_cache
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            cache.get(&req.hash).cloned()
        };
        let policy = match cached {
            Some(policy) => policy,
            None => {
                // A failover or restart emptied the cache: recompute from the
                // node's desired set, which is what produced the hash.
                let desired = build_desired_allocations(&self.store, &req.node_id, &[])
                    .map_err(|e| map_store_error("Error building desired allocations", e))?;
                let _ = self.build_net_policies(&req.node_id, &desired);
                let cache = self
                    .net_policy_cache
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                cache.get(&req.hash).cloned().ok_or_else(|| {
                    Status::not_found(format!("no policy body for hash {}", req.hash))
                })?
            }
        };
        Ok(Response::new(FetchNetPolicyReply {
            effective_json: serde_json::to_string(&policy).unwrap_or_default(),
        }))
    }

    #[instrument(skip_all, fields(node_id = %request.get_ref().node_id, allocations = request.get_ref().known.len()))]
    async fn get_ingress_certificates(
        &self,
        request: Request<GetIngressCertificatesRequest>,
    ) -> Result<Response<GetIngressCertificatesReply>, Status> {
        // ACL: worker-plane endpoint (nodes fetch the KV certificates behind
        // `certFrom = { kv }` ingress chains of allocations placed on them).
        // The node never names a KV path: each one is derived below from the
        // allocation's effective chain, which only a submitter holding
        // `kv:read` on the path could have declared.
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
        let req = request.into_inner();
        acl::enforce_node_binding(&identity, &req.node_id)?;

        let desired = build_desired_allocations(&self.store, &req.node_id, &[])
            .map_err(|e| map_store_error("Error building desired allocations", e))?;
        // The same merge the heartbeat ran; it leaves each body in the cache.
        let hashes: HashMap<i64, String> = self
            .build_net_policies(&req.node_id, &desired)
            .into_iter()
            .map(|assignment| (assignment.allocation_id, assignment.policy_hash))
            .collect();

        let certificates = req
            .known
            .iter()
            .map(|known| {
                let mut out = IngressCertificate {
                    allocation_id: known.allocation_id,
                    ..Default::default()
                };
                let policy = hashes.get(&known.allocation_id).and_then(|hash| {
                    self.net_policy_cache
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .get(hash)
                        .cloned()
                });
                let Some(path) = policy
                    .as_ref()
                    .and_then(|policy| policy.ingress.as_ref())
                    .and_then(|chain| chain.tls.as_ref())
                    .and_then(|tls| tls.cert_kv_path())
                    .map(str::to_string)
                else {
                    out.error =
                        "allocation is not placed on this node or has no certFrom = { kv } chain"
                            .into();
                    return out;
                };
                out.kv_path = path.clone();
                match self.store.get_kv(&path) {
                    Ok(Some(entry)) => {
                        out.version = entry.version;
                        if entry.version == known.version {
                            out.unchanged = true;
                        } else if let Err(err) =
                            crate::worker::netd::tls::validate_bundle(&entry.value)
                        {
                            out.error = format!("kv {path} {err:#}");
                        } else {
                            out.bundle_pem = entry.value;
                        }
                    }
                    Ok(None) => out.error = format!("kv {path} not found"),
                    Err(err) => {
                        warn!(kv_path = %path, error = ?err, "Failed reading ingress certificate");
                        out.error = format!("kv {path} could not be read");
                    }
                }
                out
            })
            .collect();
        Ok(Response::new(GetIngressCertificatesReply { certificates }))
    }

    #[instrument(skip_all, fields(node_id = %request.get_ref().node_id, credentials = request.get_ref().known.len()))]
    async fn get_upstream_credentials(
        &self,
        request: Request<GetUpstreamCredentialsRequest>,
    ) -> Result<Response<GetUpstreamCredentialsReply>, Status> {
        // ACL: worker-plane endpoint (nodes fetch the KV secrets behind
        // `connect.upstreams[].credentials` of allocations placed on them).
        // The node never names a KV path: each one is derived below from the
        // allocation's own connect declaration, which only a submitter
        // holding `kv:read` on the path could have written.
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
        let req = request.into_inner();
        acl::enforce_node_binding(&identity, &req.node_id)?;

        let desired = build_desired_allocations(&self.store, &req.node_id, &[])
            .map_err(|e| map_store_error("Error building desired allocations", e))?;
        let declared: HashMap<i64, crate::structs::ConnectConfig> = desired
            .iter()
            .filter(|allocation| !allocation.task_connect.trim().is_empty())
            .filter_map(|allocation| {
                serde_json::from_str(&allocation.task_connect)
                    .ok()
                    .map(|connect| (allocation.allocation_id, connect))
            })
            .collect();

        let credentials = req
            .known
            .iter()
            .map(|known| {
                let header = known.header.trim().to_ascii_lowercase();
                let mut out = UpstreamCredentialValue {
                    allocation_id: known.allocation_id,
                    upstream: known.upstream.clone(),
                    header: known.header.clone(),
                    ..Default::default()
                };
                let credential = declared
                    .get(&known.allocation_id)
                    .and_then(|connect| {
                        connect
                            .upstreams
                            .iter()
                            .find(|upstream| upstream.key() == known.upstream)
                    })
                    .and_then(|upstream| {
                        upstream
                            .credentials
                            .iter()
                            .find(|credential| credential.header.eq_ignore_ascii_case(&header))
                    });
                let Some(credential) = credential else {
                    out.error = format!(
                        "allocation is not placed on this node or upstream {} declares no \
                         credential for header {header}",
                        known.upstream
                    );
                    out.absent = true;
                    return out;
                };
                out.source = credential.source();
                if let Some(name) = credential.identity_name() {
                    self.upstream_identity_token(known, name, &mut out);
                    return out;
                }
                let Some(path) = credential.kv_path().map(str::to_string) else {
                    out.error = format!(
                        "upstream {} credential {header} names no provider",
                        known.upstream
                    );
                    out.absent = true;
                    return out;
                };
                match self.store.get_kv(&path) {
                    Ok(Some(entry)) => {
                        out.version = entry.version;
                        if entry.version == known.version {
                            out.unchanged = true;
                        } else {
                            // One trailing line ending (a value put from a
                            // file) is not part of the secret.
                            let value = entry.value.trim_end_matches(['\r', '\n']);
                            if value.contains(['\r', '\n', '\0']) || value.is_empty() {
                                // A header value cannot carry these; sending
                                // it would split the request.
                                out.error = format!(
                                    "kv {path} is empty or holds a line break or NUL, which \
                                     an HTTP header cannot carry"
                                );
                            } else {
                                out.value = value.to_string();
                            }
                        }
                    }
                    Ok(None) => {
                        out.error = format!("kv {path} not found");
                        out.absent = true;
                    }
                    Err(err) => {
                        warn!(kv_path = %path, error = ?err, "Failed reading upstream credential");
                        out.error = format!("kv {path} could not be read");
                    }
                }
                out
            })
            .collect();
        Ok(Response::new(GetUpstreamCredentialsReply { credentials }))
    }

    #[instrument(skip_all)]
    async fn list_service_edges(
        &self,
        request: Request<ListServiceEdgesRequest>,
    ) -> Result<Response<ListServiceEdgesReply>, Status> {
        // Like the catalog, a projection over jobs and allocations: job:read.
        self.check_acl(&request, AclResource::Job, AclAction::Read, AclScope::any())?;
        let req = request.into_inner();
        let window_secs = if req.window_secs == 0 {
            300
        } else {
            req.window_secs.clamp(60, (EDGE_WINDOW_MINUTES * 60) as u32)
        };
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let since_minute = now / 60 - i64::from(window_secs / 60);

        // Observed edges. `latency_ms_avg` accumulates the sum until the
        // division below.
        let mut edges: std::collections::BTreeMap<EdgeKey, ServiceEdge> =
            std::collections::BTreeMap::new();
        {
            let windows = self.net_edges.lock().unwrap_or_else(|p| p.into_inner());
            for ((job, task, service, tag), window) in windows.iter() {
                let mut edge = ServiceEdge {
                    caller_job: job.clone(),
                    caller_task: task.clone(),
                    service: service.clone(),
                    tag: tag.clone(),
                    protocol: window.protocol.clone(),
                    allowed: true,
                    last_seen_unix: window.last_seen,
                    ..Default::default()
                };
                for bucket in window.buckets.iter().filter(|b| b.minute >= since_minute) {
                    edge.requests += bucket.requests;
                    edge.errors += bucket.errors;
                    edge.denied += bucket.denied;
                    edge.latency_ms_avg += bucket.latency_ms_sum as f64;
                    edge.latency_ms_max = edge.latency_ms_max.max(bucket.latency_ms_max);
                    edge.bytes_out += bucket.bytes_out;
                    edge.bytes_in += bucket.bytes_in;
                }
                edges.insert(
                    (job.clone(), task.clone(), service.clone(), tag.clone()),
                    edge,
                );
            }
        }

        // Declared edges, and the allow verdict for every edge.
        let allow_by_service: HashMap<String, Vec<String>> = self
            .store
            .declared_services()
            .map_err(|e| map_store_error("Error listing declared services", e))?
            .into_iter()
            .map(|s| (s.service, s.allow))
            .collect();
        let consumers = self
            .store
            .declared_consumers()
            .map_err(|e| map_store_error("Error listing declared consumers", e))?;
        for consumer in consumers {
            for upstream in &consumer.upstreams {
                let service = upstream.key().to_string();
                // Tagged observed edges for the same (caller, service) are
                // declared too.
                let mut any = false;
                for ((job, task, svc, _), edge) in edges.iter_mut() {
                    if *job == consumer.job_id && *task == consumer.task_name && *svc == service {
                        edge.declared = true;
                        any = true;
                    }
                }
                if !any {
                    edges.insert(
                        (
                            consumer.job_id.clone(),
                            consumer.task_name.clone(),
                            service.clone(),
                            String::new(),
                        ),
                        ServiceEdge {
                            caller_job: consumer.job_id.clone(),
                            caller_task: consumer.task_name.clone(),
                            service,
                            declared: true,
                            allowed: true,
                            ..Default::default()
                        },
                    );
                }
            }
        }
        let secs = f64::from(window_secs);
        for ((job, _, service, _), edge) in edges.iter_mut() {
            if let Some(allow) = allow_by_service.get(service) {
                edge.allowed = crate::structs::allow_permits(allow, job);
            }
            edge.rps = edge.requests as f64 / secs;
            edge.error_rate = if edge.requests + edge.errors > 0 {
                edge.errors as f64 / (edge.requests + edge.errors) as f64
            } else {
                0.0
            };
            if edge.requests > 0 {
                edge.latency_ms_avg /= edge.requests as f64;
            }
        }
        let filter = req.service.trim();
        let mut out: Vec<ServiceEdge> = edges
            .into_values()
            .filter(|edge| filter.is_empty() || edge.service == filter || edge.caller_job == filter)
            .collect();
        out.sort_by(|a, b| {
            a.service
                .cmp(&b.service)
                .then_with(|| a.caller_job.cmp(&b.caller_job))
                .then_with(|| a.caller_task.cmp(&b.caller_task))
                .then_with(|| a.tag.cmp(&b.tag))
        });
        Ok(Response::new(ListServiceEdgesReply {
            edges: out,
            window_secs,
        }))
    }

    async fn list_services(
        &self,
        request: Request<ListServicesRequest>,
    ) -> Result<Response<ListServicesReply>, Status> {
        // Services are a projection over jobs/allocations, so job:read governs.
        self.check_acl(&request, AclResource::Job, AclAction::Read, AclScope::any())?;
        let endpoints = self
            .store
            .service_endpoints(None)
            .map_err(|e| map_store_error("Error listing service endpoints", e))?;
        let declared = self
            .store
            .declared_services()
            .map_err(|e| map_store_error("Error listing declared services", e))?;

        let mut summaries: std::collections::BTreeMap<String, ServiceSummary> =
            std::collections::BTreeMap::new();
        for entry in declared {
            summaries
                .entry(entry.service.clone())
                .or_insert(ServiceSummary {
                    service: entry.service,
                    job_id: entry.job_id,
                    task_name: entry.task_name,
                    healthy_count: 0,
                    total_count: 0,
                    tags: entry.tags,
                    // Intentions come from the same task config the rest of the
                    // summary does, so `doctor` can explain a denied consumer
                    // with plain job:read.
                    allow: entry.allow,
                    meta: entry.meta.into_iter().collect(),
                });
        }
        for endpoint in endpoints {
            let summary = summaries
                .entry(endpoint.service.clone())
                .or_insert(ServiceSummary {
                    service: endpoint.service.clone(),
                    job_id: endpoint.job_id.clone(),
                    task_name: endpoint.task_name.clone(),
                    healthy_count: 0,
                    total_count: 0,
                    tags: endpoint.tags.clone(),
                    allow: endpoint.allow.clone(),
                    meta: endpoint.meta.clone().into_iter().collect(),
                });
            summary.total_count += 1;
            if endpoint.healthy {
                summary.healthy_count += 1;
            }
        }

        Ok(Response::new(ListServicesReply {
            services: summaries.into_values().collect(),
        }))
    }

    #[instrument(skip_all)]
    async fn resolve_service(
        &self,
        request: Request<ResolveServiceRequest>,
    ) -> Result<Response<ResolveServiceReply>, Status> {
        // Services are a projection over jobs/allocations, so job:read governs.
        self.check_acl(&request, AclResource::Job, AclAction::Read, AclScope::any())?;
        let req = request.into_inner();
        let endpoints = self
            .store
            .service_endpoints(Some(&req.service))
            .map_err(|e| map_store_error("Error resolving service", e))?;

        let endpoints = endpoints
            .into_iter()
            .filter(|endpoint| req.include_unhealthy || endpoint.healthy)
            .map(|endpoint| ServiceEndpoint {
                service: endpoint.service,
                job_id: endpoint.job_id,
                task_name: endpoint.task_name,
                allocation_id: endpoint.allocation_id,
                node_id: endpoint.node_id,
                address: endpoint.address.unwrap_or_default(),
                public_address: endpoint.public_address.unwrap_or_default(),
                port: u32::from(endpoint.port.unwrap_or(0)),
                healthy: endpoint.healthy,
                health_status: endpoint.health_status,
                tags: endpoint.tags,
                meta: endpoint.meta.into_iter().collect(),
                port_label: endpoint.port_label.unwrap_or_default(),
                mesh_tls: endpoint.mesh_tls,
            })
            .collect();

        Ok(Response::new(ResolveServiceReply { endpoints }))
    }

    #[instrument(skip_all)]
    async fn put_kv(&self, request: Request<PutKvRequest>) -> Result<Response<PutKvReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Kv,
            AclAction::Write,
            AclScope::path(&request.get_ref().key),
        )?;
        let req = request.into_inner();
        let value_type = KvValueType::try_from(req.value_type).unwrap_or(KvValueType::Unspecified);
        let entry = self
            .store
            .put_kv(
                &req.key,
                &req.value,
                value_type,
                req.expected_version,
                req.lock_id.as_deref(),
            )
            .map_err(|e| map_store_error("Error putting kv entry", e))?;

        if !self.queue_kv_change(&req.key, false) {
            reconcile_kv_changes(&self.store, &[(req.key.clone(), false)])?;
        }

        Ok(Response::new(PutKvReply { entry: Some(entry) }))
    }

    #[instrument(skip_all)]
    async fn get_kv(&self, request: Request<GetKvRequest>) -> Result<Response<GetKvReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Kv,
            AclAction::Read,
            AclScope::path(&request.get_ref().key),
        )?;
        let req = request.into_inner();
        let entry = if req.metadata_only {
            self.store.get_kv_metadata(&req.key)
        } else {
            self.store.get_kv(&req.key)
        }
        .map_err(|e| map_store_error("Error getting kv entry", e))?;
        // Who reads this key, limited to jobs the caller may see.
        let references = self
            .store
            .job_kv_references_for_key(&req.key)
            .map_err(|e| map_store_error("Error getting kv references", e))?
            .into_iter()
            .filter(|reference| {
                identity.allows(
                    AclResource::Job,
                    AclAction::Read,
                    &self.job_read_scope(&reference.job_id),
                )
            })
            .map(|reference| KvJobReference {
                job_id: reference.job_id,
                job_config_id: reference.job_config_id,
                json_pointer: reference.json_pointer,
            })
            .collect();

        Ok(Response::new(GetKvReply { entry, references }))
    }

    /// Resolves an artifact credential from KV for a worker about to fetch.
    /// The value is returned for immediate use and never persisted on
    /// allocations or logged (the span carries node/path/source only).
    ///
    /// SECURITY BOUNDARY (KV ACLs do not exist yet): any task spec may
    /// reference any KV path via auth.kv, so possession of job-submit access
    /// implies read access to every KV credential. Revisit when the ACL
    /// system lands — this credential read must become an early enforcement
    /// point. Tracked in internal_docs/TODO.md (Auth, TLS & security).
    #[instrument(skip_all, fields(node_id = %request.get_ref().node_id, kv_path = %request.get_ref().kv_path, source = %request.get_ref().source))]
    async fn get_artifact_credential(
        &self,
        request: Request<GetArtifactCredentialRequest>,
    ) -> Result<Response<GetArtifactCredentialReply>, Status> {
        // ACL: worker-plane endpoint (workers fetch staged-artifact
        // credentials referenced by their allocations).
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
        let req = request.into_inner();
        acl::enforce_node_binding(&identity, &req.node_id)?;
        let entry = self
            .store
            .get_kv(&req.kv_path)
            .map_err(|e| map_store_error("Error reading artifact credential", e))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "artifact credential kv path {} not found",
                    req.kv_path
                ))
            })?;

        // Validate the credential shape server-side so a malformed KV value
        // fails with an actionable error naming the path, not a worker-side
        // parse error mid-staging. The parsed form is dropped immediately.
        crate::artifacts::Credential::from_json(&entry.value).map_err(|err| {
            Status::failed_precondition(format!(
                "kv {} does not hold a valid artifact credential: {err:#}",
                req.kv_path
            ))
        })?;

        Ok(Response::new(GetArtifactCredentialReply {
            credential_json: entry.value,
        }))
    }

    #[instrument(skip_all)]
    async fn list_kv(
        &self,
        request: Request<ListKvRequest>,
    ) -> Result<Response<ListKvReply>, Status> {
        let identity =
            self.check_acl(&request, AclResource::Kv, AclAction::Read, AclScope::any())?;
        let req = request.into_inner();
        // Values only on request: browsing key names should not decrypt and
        // ship every secret in the prefix.
        let entries = if req.include_values {
            self.store.list_kv(&req.prefix)
        } else {
            self.store.list_kv_keys(&req.prefix)
        };
        let mut entries = entries.map_err(|e| map_store_error("Error listing kv entries", e))?;
        entries.retain(|entry| {
            identity.allows(
                AclResource::Kv,
                AclAction::Read,
                &AclScope::path(&entry.key),
            )
        });

        Ok(Response::new(ListKvReply { entries }))
    }

    #[instrument(skip_all)]
    async fn delete_kv(
        &self,
        request: Request<DeleteKvRequest>,
    ) -> Result<Response<DeleteKvReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Kv,
            AclAction::Write,
            AclScope::path(&request.get_ref().key),
        )?;
        let req = request.into_inner();
        self.store
            .delete_kv(&req.key, req.expected_version, req.lock_id.as_deref())
            .map_err(|e| map_store_error("Error deleting kv entry", e))?;
        if !self.queue_kv_change(&req.key, true) {
            reconcile_kv_changes(&self.store, &[(req.key.clone(), true)])?;
        }

        Ok(Response::new(DeleteKvReply {
            message: "kv entry deleted".to_string(),
        }))
    }

    #[instrument(skip_all)]
    async fn patch_kv(
        &self,
        request: Request<PatchKvRequest>,
    ) -> Result<Response<PatchKvReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Kv,
            AclAction::Write,
            AclScope::path(&request.get_ref().key),
        )?;
        let req = request.into_inner();
        let entry = self
            .store
            .patch_kv(
                &req.key,
                &req.patch_json,
                req.expected_version,
                req.lock_id.as_deref(),
            )
            .map_err(|e| map_store_error("Error patching kv entry", e))?;

        if !self.queue_kv_change(&req.key, false) {
            reconcile_kv_changes(&self.store, &[(req.key.clone(), false)])?;
        }

        Ok(Response::new(PatchKvReply { entry: Some(entry) }))
    }

    #[instrument(skip_all)]
    async fn acquire_kv_lock(
        &self,
        request: Request<AcquireKvLockRequest>,
    ) -> Result<Response<AcquireKvLockReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Kv,
            AclAction::Write,
            AclScope::path(&request.get_ref().key),
        )?;
        let req = request.into_inner();
        let lock = self
            .store
            .acquire_kv_lock(
                &req.key,
                req.lock_id.as_deref(),
                req.owner.as_deref(),
                req.ttl_seconds,
                req.force,
            )
            .map_err(|e| map_store_error("Error acquiring kv lock", e))?;

        Ok(Response::new(AcquireKvLockReply { lock: Some(lock) }))
    }

    #[instrument(skip_all)]
    async fn renew_kv_lock(
        &self,
        request: Request<RenewKvLockRequest>,
    ) -> Result<Response<RenewKvLockReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Kv,
            AclAction::Write,
            AclScope::path(&request.get_ref().key),
        )?;
        let req = request.into_inner();
        let lock = self
            .store
            .renew_kv_lock(&req.key, &req.lock_id, req.ttl_seconds)
            .map_err(|e| map_store_error("Error renewing kv lock", e))?;

        Ok(Response::new(RenewKvLockReply { lock: Some(lock) }))
    }

    #[instrument(skip_all)]
    async fn release_kv_lock(
        &self,
        request: Request<ReleaseKvLockRequest>,
    ) -> Result<Response<ReleaseKvLockReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Kv,
            AclAction::Write,
            AclScope::path(&request.get_ref().key),
        )?;
        let req = request.into_inner();
        self.store
            .release_kv_lock(&req.key, &req.lock_id)
            .map_err(|e| map_store_error("Error releasing kv lock", e))?;

        Ok(Response::new(ReleaseKvLockReply {
            message: "kv lock released".to_string(),
        }))
    }

    #[instrument(skip_all)]
    async fn get_kv_lock(
        &self,
        request: Request<GetKvLockRequest>,
    ) -> Result<Response<GetKvLockReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Kv,
            AclAction::Read,
            AclScope::path(&request.get_ref().key),
        )?;
        let req = request.into_inner();
        let lock = self
            .store
            .get_kv_lock(&req.key)
            .map_err(|e| map_store_error("Error getting kv lock", e))?;

        Ok(Response::new(GetKvLockReply { lock }))
    }

    #[instrument(skip_all)]
    async fn query_state_store(
        &self,
        request: Request<QueryStateStoreRequest>,
    ) -> Result<Response<QueryStateStoreReply>, Status> {
        // ACL: raw state access is powerful (it can read every table,
        // including ACL metadata); grant `state` capabilities sparingly.
        let action = if request.get_ref().allow_write {
            AclAction::Write
        } else {
            AclAction::Read
        };
        self.check_acl(&request, AclResource::State, action, AclScope::none())?;
        let req = request.into_inner();
        let result = self
            .store
            .query_state_store(&req.sql, req.allow_write)
            .map_err(|e| map_store_error("Error querying state store", e))?;

        Ok(Response::new(QueryStateStoreReply {
            statement_type: result.statement_type,
            columns: result.columns,
            rows: result
                .rows
                .into_iter()
                .map(|row| crate::structs::QueryStateStoreRow { json: row.json })
                .collect(),
            affected_rows: result.affected_rows,
        }))
    }

    #[instrument(skip_all)]
    async fn get_state_store_schema(
        &self,
        request: Request<GetStateStoreSchemaRequest>,
    ) -> Result<Response<GetStateStoreSchemaReply>, Status> {
        self.check_acl(
            &request,
            AclResource::State,
            AclAction::Read,
            AclScope::none(),
        )?;
        let objects = self
            .store
            .state_store_schema()
            .map_err(|e| map_store_error("Error loading state store schema", e))?
            .into_iter()
            .map(|object| crate::structs::StateStoreSchemaObject {
                kind: object.kind,
                name: object.name,
                create_sql: object.create_sql,
                columns: object
                    .columns
                    .into_iter()
                    .map(|column| crate::structs::StateStoreSchemaColumn {
                        ordinal: column.ordinal,
                        name: column.name,
                        data_type: column.data_type,
                        not_null: column.not_null,
                        default_value: column.default_value,
                        primary_key: column.primary_key,
                    })
                    .collect(),
            })
            .collect();

        Ok(Response::new(GetStateStoreSchemaReply { objects }))
    }

    #[instrument(skip_all)]
    async fn list_saved_state_queries(
        &self,
        request: Request<ListSavedStateQueriesRequest>,
    ) -> Result<Response<ListSavedStateQueriesReply>, Status> {
        self.check_acl(
            &request,
            AclResource::State,
            AclAction::Read,
            AclScope::none(),
        )?;
        let queries = self
            .store
            .list_saved_state_queries()
            .map_err(|e| map_store_error("Error listing saved state queries", e))?
            .into_iter()
            .map(|query| SavedStateQuery {
                name: query.name,
                sql: query.sql,
                created_at: query.created_at,
                updated_at: query.updated_at,
            })
            .collect();

        Ok(Response::new(ListSavedStateQueriesReply { queries }))
    }

    #[instrument(skip_all)]
    async fn upsert_saved_state_query(
        &self,
        request: Request<UpsertSavedStateQueryRequest>,
    ) -> Result<Response<UpsertSavedStateQueryReply>, Status> {
        self.check_acl(
            &request,
            AclResource::State,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let query = self
            .store
            .upsert_saved_state_query(&req.name, &req.sql)
            .map_err(|e| map_store_error("Error saving state query", e))?;

        Ok(Response::new(UpsertSavedStateQueryReply {
            query: Some(SavedStateQuery {
                name: query.name,
                sql: query.sql,
                created_at: query.created_at,
                updated_at: query.updated_at,
            }),
        }))
    }

    #[instrument(skip_all)]
    async fn delete_saved_state_query(
        &self,
        request: Request<DeleteSavedStateQueryRequest>,
    ) -> Result<Response<DeleteSavedStateQueryReply>, Status> {
        self.check_acl(
            &request,
            AclResource::State,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        self.store
            .delete_saved_state_query(&req.name)
            .map_err(|e| map_store_error("Error deleting saved state query", e))?;

        Ok(Response::new(DeleteSavedStateQueryReply {
            message: "saved query deleted".to_string(),
        }))
    }

    #[instrument(skip_all)]
    async fn who_am_i(
        &self,
        request: Request<WhoAmIRequest>,
    ) -> Result<Response<WhoAmIReply>, Status> {
        // ACL: open endpoint. Identity introspection must work for anonymous
        // callers so the UI/CLI can discover whether ACLs are enabled.
        let identity = self.authenticate_acl(&request)?;
        Ok(Response::new(WhoAmIReply {
            acl_enabled: self.auth.enabled,
            audit_only: self.auth.audit_only,
            source: identity.source.as_str().to_string(),
            principal: identity.principal.clone(),
            token_type: if identity.is_management && self.auth.enabled {
                "management".to_string()
            } else if self.auth.enabled
                && identity.source != crate::structs::acl::AclSource::Anonymous
            {
                "client".to_string()
            } else {
                "none".to_string()
            },
            policies: identity.policies.clone(),
            expires_at: identity.expires_at.clone(),
            accessor_id: identity.accessor_id.clone(),
            rules_json: identity.effective_rules_json(),
        }))
    }

    #[instrument(skip_all)]
    async fn login(&self, request: Request<LoginRequest>) -> Result<Response<LoginReply>, Status> {
        // ACL: open endpoint; this is how callers obtain credentials.
        let req = request.into_inner();
        let (token, secret) = self
            .auth
            .login(
                &self.store,
                &req.method,
                req.username.as_deref(),
                req.password.as_deref(),
                req.jwt.as_deref(),
                req.ttl.as_deref(),
            )
            .await?;
        info!(principal = %token.name, "ACL login succeeded");
        Ok(Response::new(LoginReply {
            token: Some(token),
            secret,
        }))
    }

    #[instrument(skip_all)]
    async fn upsert_acl_policy(
        &self,
        request: Request<UpsertAclPolicyRequest>,
    ) -> Result<Response<UpsertAclPolicyReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let policy = self
            .store
            .upsert_acl_policy(&req.name, &req.description, &req.rules_json)
            .map_err(|e| map_store_error("Error saving ACL policy", e))?;
        Ok(Response::new(UpsertAclPolicyReply {
            policy: Some(policy),
        }))
    }

    #[instrument(skip_all)]
    async fn get_acl_policy(
        &self,
        request: Request<GetAclPolicyRequest>,
    ) -> Result<Response<GetAclPolicyReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Read,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let policy = self
            .store
            .get_acl_policy(&req.name)
            .map_err(|e| map_store_error("Error getting ACL policy", e))?
            .ok_or_else(|| Status::not_found(format!("ACL policy `{}` not found", req.name)))?;
        Ok(Response::new(GetAclPolicyReply {
            policy: Some(policy),
        }))
    }

    #[instrument(skip_all)]
    async fn list_acl_policies(
        &self,
        request: Request<ListAclPoliciesRequest>,
    ) -> Result<Response<ListAclPoliciesReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Read,
            AclScope::none(),
        )?;
        let policies = self
            .store
            .list_acl_policies()
            .map_err(|e| map_store_error("Error listing ACL policies", e))?;
        Ok(Response::new(ListAclPoliciesReply { policies }))
    }

    #[instrument(skip_all)]
    async fn delete_acl_policy(
        &self,
        request: Request<DeleteAclPolicyRequest>,
    ) -> Result<Response<DeleteAclPolicyReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        self.store
            .delete_acl_policy(&req.name)
            .map_err(|e| map_store_error("Error deleting ACL policy", e))?;
        Ok(Response::new(DeleteAclPolicyReply {
            message: format!("ACL policy `{}` deleted", req.name),
        }))
    }

    #[instrument(skip_all)]
    async fn create_acl_token(
        &self,
        request: Request<CreateAclTokenRequest>,
    ) -> Result<Response<CreateAclTokenReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();

        let token_type = if req.token_type.is_empty() {
            "client".to_string()
        } else {
            req.token_type
        };
        match token_type.as_str() {
            // Escalation guard: only management tokens may mint management
            // tokens, regardless of acl:write policy grants.
            "management" => {
                if !identity.is_management {
                    return Err(Status::permission_denied(
                        "creating a management token requires a management token",
                    ));
                }
                if !req.policies.is_empty() {
                    return Err(Status::invalid_argument(
                        "management tokens must not list policies",
                    ));
                }
            }
            "client" => {
                if req.policies.is_empty() {
                    return Err(Status::invalid_argument(
                        "client tokens must list at least one policy",
                    ));
                }
                let missing = self
                    .store
                    .missing_acl_policies(&req.policies)
                    .map_err(|e| map_store_error("Error validating policies", e))?;
                if !missing.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "unknown ACL policies: {}",
                        missing.join(", ")
                    )));
                }
            }
            other => {
                return Err(Status::invalid_argument(format!(
                    "unknown token type `{other}` (expected `management` or `client`)"
                )));
            }
        }

        let ttl: Option<Duration> = req
            .ttl
            .as_deref()
            .map(|raw| {
                raw.parse::<humantime::Duration>()
                    .map(Into::into)
                    .map_err(|e| Status::invalid_argument(format!("invalid ttl `{raw}`: {e}")))
            })
            .transpose()?;
        // Server-enforced lifetime bounds: when acl.tokenMaxTtl is set,
        // client tokens must expire within it (management tokens are exempt).
        if token_type == "client" {
            if let Some(max) = self.auth.token_max_ttl {
                match ttl {
                    None => {
                        return Err(Status::invalid_argument(format!(
                            "client tokens must set a ttl of at most {} (acl.tokenMaxTtl)",
                            humantime::format_duration(max)
                        )))
                    }
                    Some(requested) if requested > max => {
                        return Err(Status::invalid_argument(format!(
                            "requested ttl exceeds acl.tokenMaxTtl ({})",
                            humantime::format_duration(max)
                        )))
                    }
                    Some(_) => {}
                }
            }
        }
        let (token, secret) =
            self.auth
                .mint_token(&self.store, &req.name, &token_type, &req.policies, ttl)?;
        info!(accessor_id = %token.accessor_id, name = %token.name, "ACL token created");
        Ok(Response::new(CreateAclTokenReply {
            token: Some(token),
            secret,
        }))
    }

    #[instrument(skip_all)]
    async fn list_acl_tokens(
        &self,
        request: Request<ListAclTokensRequest>,
    ) -> Result<Response<ListAclTokensReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Read,
            AclScope::none(),
        )?;
        let tokens = self
            .store
            .list_acl_tokens()
            .map_err(|e| map_store_error("Error listing ACL tokens", e))?
            .iter()
            .map(|record| record.to_info())
            .collect();
        Ok(Response::new(ListAclTokensReply { tokens }))
    }

    #[instrument(skip_all)]
    async fn delete_acl_token(
        &self,
        request: Request<DeleteAclTokenRequest>,
    ) -> Result<Response<DeleteAclTokenReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        self.store
            .delete_acl_token(&req.accessor_id)
            .map_err(|e| map_store_error("Error deleting ACL token", e))?;
        info!(accessor_id = %req.accessor_id, "ACL token deleted");
        Ok(Response::new(DeleteAclTokenReply {
            message: format!("ACL token `{}` deleted", req.accessor_id),
        }))
    }

    #[instrument(skip_all)]
    async fn upsert_acl_user(
        &self,
        request: Request<UpsertAclUserRequest>,
    ) -> Result<Response<UpsertAclUserReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let missing = self
            .store
            .missing_acl_policies(&req.policies)
            .map_err(|e| map_store_error("Error validating policies", e))?;
        if !missing.is_empty() {
            return Err(Status::invalid_argument(format!(
                "unknown ACL policies: {}",
                missing.join(", ")
            )));
        }
        let password_hash = if req.password.is_empty() {
            None
        } else {
            Some(
                acl::hash_password(&req.password)
                    .map_err(|e| Status::internal(format!("failed to hash password: {e:#}")))?,
            )
        };
        let user = self
            .store
            .upsert_acl_user(&req.username, password_hash.as_deref(), &req.policies)
            .map_err(|e| map_store_error("Error saving ACL user", e))?;
        Ok(Response::new(UpsertAclUserReply {
            user: Some(user.to_user()),
        }))
    }

    #[instrument(skip_all)]
    async fn list_acl_users(
        &self,
        request: Request<ListAclUsersRequest>,
    ) -> Result<Response<ListAclUsersReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Read,
            AclScope::none(),
        )?;
        let users = self
            .store
            .list_acl_users()
            .map_err(|e| map_store_error("Error listing ACL users", e))?
            .iter()
            .map(|record| record.to_user())
            .collect();
        Ok(Response::new(ListAclUsersReply { users }))
    }

    #[instrument(skip_all)]
    async fn delete_acl_user(
        &self,
        request: Request<DeleteAclUserRequest>,
    ) -> Result<Response<DeleteAclUserReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        self.store
            .delete_acl_user(&req.username)
            .map_err(|e| map_store_error("Error deleting ACL user", e))?;
        Ok(Response::new(DeleteAclUserReply {
            message: format!("ACL user `{}` deleted", req.username),
        }))
    }

    #[instrument(skip_all)]
    async fn get_auth_methods(
        &self,
        request: Request<GetAuthMethodsRequest>,
    ) -> Result<Response<GetAuthMethodsReply>, Status> {
        // ACL: open endpoint. Login UIs need the method list before any
        // credential exists; only public metadata is returned.
        let _ = request;
        let methods = self
            .auth
            .method_infos()
            .into_iter()
            .map(|info| AuthMethodInfo {
                name: info.name,
                kind: info.kind,
                oidc_issuer: info.oidc_issuer,
                oidc_client_id: info.oidc_client_id,
                oidc_scopes: info.oidc_scopes,
            })
            .collect();
        Ok(Response::new(GetAuthMethodsReply { methods }))
    }

    #[instrument(skip_all)]
    async fn create_join_token(
        &self,
        request: Request<CreateJoinTokenRequest>,
    ) -> Result<Response<CreateJoinTokenReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        let worker_type = if req.worker_type.trim().is_empty() {
            "default".to_string()
        } else {
            req.worker_type
        };
        let ttl = match req.ttl.as_deref() {
            None => Some(Duration::from_secs(24 * 60 * 60)),
            Some("none") | Some("0") => None,
            Some(raw) => Some(
                raw.parse::<humantime::Duration>()
                    .map(Into::into)
                    .map_err(|e| Status::invalid_argument(format!("invalid ttl `{raw}`: {e}")))?,
            ),
        };
        let (info, token) = self.auth.mint_join_token(
            &self.store,
            &self.advertise_addr,
            &worker_type,
            ttl,
            &req.description,
        )?;
        info!(id = %info.id, worker_type = %info.worker_type, "Join token created");
        Ok(Response::new(CreateJoinTokenReply {
            info: Some(info),
            token,
        }))
    }

    #[instrument(skip_all)]
    async fn list_join_tokens(
        &self,
        request: Request<ListJoinTokensRequest>,
    ) -> Result<Response<ListJoinTokensReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Read,
            AclScope::none(),
        )?;
        let tokens = self
            .store
            .list_join_tokens()
            .map_err(|e| map_store_error("Error listing join tokens", e))?
            .iter()
            .map(|record| record.to_info())
            .collect();
        Ok(Response::new(ListJoinTokensReply { tokens }))
    }

    #[instrument(skip_all)]
    async fn delete_join_token(
        &self,
        request: Request<DeleteJoinTokenRequest>,
    ) -> Result<Response<DeleteJoinTokenReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Write,
            AclScope::none(),
        )?;
        let req = request.into_inner();
        self.store
            .delete_join_token(&req.id)
            .map_err(|e| map_store_error("Error deleting join token", e))?;
        info!(id = %req.id, "Join token deleted");
        Ok(Response::new(DeleteJoinTokenReply {
            message: format!("join token `{}` deleted", req.id),
        }))
    }

    // -----------------------------------------------------------------
    // Edge boot (docs/edge-boot.md)
    // -----------------------------------------------------------------

    #[instrument(skip_all, fields(job, task, jti))]
    async fn issue_boot_ticket(
        &self,
        request: Request<IssueBootTicketRequest>,
    ) -> Result<Response<IssueBootTicketReply>, Status> {
        // Scope is the job named in the body, so authenticate first and
        // enforce precisely once it is parsed.
        let identity = self.authenticate_acl(&request)?;
        let req = request.into_inner();
        acl::enforce(
            &identity,
            AclResource::Boot,
            AclAction::Write,
            &AclScope::job(&req.job),
        )?;
        tracing::Span::current().record("job", req.job.as_str());
        tracing::Span::current().record("task", req.task.as_str());

        if !self.edge_boot.enabled {
            return Err(Status::failed_precondition(
                "edge boot is not enabled on this manager \
                 (set edgeBoot.enabled: true in the manager config)",
            ));
        }
        let Some(state) = self.workload_identity.as_ref() else {
            return Err(Status::failed_precondition(
                "boot tickets are signed by the workload-identity issuer, which is not enabled \
                 on this manager (set workloadIdentity.enabled: true)",
            ));
        };

        let idempotency_key = scoped_idempotency_key(
            &identity,
            "issue_boot_ticket",
            req.idempotency_key.as_deref(),
        );
        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let reply: MintedTicketRecord = serde_json::from_str(&recorded)
                .map_err(|e| Status::internal(format!("corrupt idempotency record: {e}")))?;
            return Ok(Response::new(IssueBootTicketReply {
                ticket: reply.ticket,
                jti: reply.jti,
                job_config_id: reply.job_config_id,
                expires_at_unix: reply.expires_at_unix,
                deduplicated: true,
            }));
        }

        // A ticket names an EXACT job config, never "latest": pinning at mint
        // is what keeps a redeploy from silently widening what a live ticket
        // can start.
        let job_config_id = if req.job_config_id.trim().is_empty() {
            self.store
                .get_job_config_optional(&req.job)
                .map_err(|e| map_store_error("Error resolving job config", e))?
                .ok_or_else(|| {
                    Status::not_found(format!("job `{}` has no current config", req.job))
                })?
                .id()
        } else {
            req.job_config_id.clone()
        };

        let task = self
            .store
            .get_task_configs_for_job_config(&job_config_id)
            .map_err(|e| map_store_error("Error loading task configs", e))?
            .into_iter()
            .map(|(_, task)| task)
            .find(|task| task.name == req.task)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "job config {job_config_id} has no task `{}`",
                    req.task
                ))
            })?;

        let ttl = match req.ttl.trim() {
            "" => self.edge_boot.default_ticket_ttl.clone(),
            raw => raw.to_string(),
        };
        let ttl = humantime::parse_duration(&ttl)
            .map_err(|e| Status::invalid_argument(format!("invalid ttl `{ttl}`: {e}")))?;

        let minted = match req.scope.trim() {
            "" | "boot" => {
                self.mint_boot_ticket(state, &identity, &req, &job_config_id, &task, ttl)?
            }
            "session" => {
                if !req.instance_id.trim().is_empty()
                    || !req.instance_prefix.trim().is_empty()
                    || !req.job_config_id.trim().is_empty()
                    || req.gate != 0
                    || req.uses > 1
                {
                    return Err(Status::invalid_argument(
                        "a session ticket names no instance, instance prefix, job config, gate, \
                         or use count: the node mints the session id and starts the spec of the \
                         envelope it holds, and the ticket is good for any number of sessions \
                         until it expires",
                    ));
                }
                let session_request = crate::manager::edge_boot::SessionTicketRequest {
                    job: req.job.clone(),
                    task: req.task.clone(),
                    pool: req.pool.clone(),
                    ttl,
                    accessor_id: identity.accessor_id.clone().unwrap_or_default(),
                };
                crate::manager::edge_boot::validate_session_ticket_request(
                    &session_request,
                    &task,
                    &self.edge_boot,
                )
                .map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
                let jti = crate::structs::edge_boot::new_ticket_id();
                crate::manager::edge_boot::mint_session_ticket(
                    state,
                    &session_request,
                    &job_config_id,
                    &jti,
                    std::time::SystemTime::now(),
                )
                .map_err(|e| Status::internal(format!("failed to mint session ticket: {e:#}")))?
            }
            other => {
                return Err(Status::invalid_argument(format!(
                    "unknown ticket scope `{other}` (boot|session)"
                )))
            }
        };
        let jti = minted.record.jti.clone();
        tracing::Span::current().record("jti", jti.as_str());

        self.store
            .insert_boot_ticket(&minted.record)
            .map_err(|e| map_store_error("Error recording boot ticket", e))?;
        self.store.events().boot_ticket_issued(
            &minted.record.jti,
            &minted.record.job,
            &minted.record.task,
            &minted.record.job_config_id,
            &minted.record.instance_id,
            &minted.record.instance_prefix,
            &minted.record.pool,
            &minted.record.gate,
            &minted.record.accessor_id,
            minted.record.expires_at_unix,
        );

        let reply = IssueBootTicketReply {
            ticket: minted.token,
            jti: minted.record.jti.clone(),
            job_config_id,
            expires_at_unix: minted.record.expires_at_unix,
            deduplicated: false,
        };
        // The idempotency record carries the token so a retry returns the
        // capability that was already minted instead of a second one.
        let record = serde_json::to_string(&MintedTicketRecord {
            ticket: reply.ticket.clone(),
            jti: reply.jti.clone(),
            job_config_id: reply.job_config_id.clone(),
            expires_at_unix: reply.expires_at_unix,
        })
        .map_err(|e| Status::internal(format!("failed to record idempotency: {e}")))?;
        self.store
            .idempotency_record(&idempotency_key, "issue_boot_ticket", &record)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;

        info!(jti = %reply.jti, job = %req.job, task = %req.task, "Boot ticket issued");
        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn list_boot_tickets(
        &self,
        request: Request<ListBootTicketsRequest>,
    ) -> Result<Response<ListBootTicketsReply>, Status> {
        // Coarse gate before the lookup, precise filtering after — a
        // credential-less caller must not be able to probe which jobs have
        // live tickets.
        let identity = self.check_acl(
            &request,
            AclResource::Boot,
            AclAction::Read,
            AclScope::any(),
        )?;
        let req = request.into_inner();
        let now_unix = time::OffsetDateTime::now_utc().unix_timestamp();
        let tickets = self
            .store
            .list_boot_tickets(&req.job, now_unix)
            .map_err(|e| map_store_error("Error listing boot tickets", e))?
            .into_iter()
            .filter(|record| {
                identity.allows(
                    AclResource::Boot,
                    AclAction::Read,
                    &AclScope::job(&record.job),
                )
            })
            .map(|record| BootTicket {
                gate: crate::structs::edge_boot::Gate::parse(&record.gate)
                    .unwrap_or_default()
                    .to_wire(),
                uses_remaining: record.uses_remaining(),
                jti: record.jti,
                job: record.job,
                task: record.task,
                job_config_id: record.job_config_id,
                instance_id: record.instance_id,
                instance_prefix: record.instance_prefix,
                pool: record.pool,
                expires_at_unix: record.expires_at_unix,
                accessor_id: record.accessor_id,
                created_at: record.created_at,
                scope: if record.gate == crate::manager::edge_boot::SESSION_TICKET_GATE {
                    "session".to_string()
                } else {
                    "boot".to_string()
                },
            })
            .collect();
        Ok(Response::new(ListBootTicketsReply { tickets }))
    }

    #[instrument(skip_all, fields(node_id, claim_id, verdict))]
    async fn ratify_claim(
        &self,
        request: Request<RatifyClaimRequest>,
    ) -> Result<Response<RatifyClaimReply>, Status> {
        // ACL: worker-plane endpoint — the node that started the work asks
        // the manager to commit it, authenticated with its node token. The
        // ticket (when present) is re-verified below; nodes are honest but
        // are not trusted to be the only holder of a capability.
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
        let req = request.into_inner();
        tracing::Span::current().record("node_id", req.node_id.as_str());
        tracing::Span::current().record("claim_id", req.claim_id.as_str());
        acl::enforce_node_binding(&identity, &req.node_id)?;

        let refuse = |reason: crate::structs::edge_boot::RefusalReason, detail: String| {
            crate::manager::telemetry::worker_metrics()
                .provisional_refused_total
                .inc(&[reason.as_str()]);
            Ok(Response::new(RatifyClaimReply {
                confirmed: false,
                allocation_id: 0,
                reason: reason.to_wire(),
                detail,
                allocation: None,
            }))
        };

        // An edge-originated claim carries the ticket that authorized it,
        // spent inside the ratification's own entry.
        let mut spend = None;
        if !req.ticket.trim().is_empty() {
            if !self.edge_boot.enabled {
                return refuse(
                    crate::structs::edge_boot::RefusalReason::Disabled,
                    "edge boot is not enabled on this manager".into(),
                );
            }
            let Some(state) = self.workload_identity.as_ref() else {
                return refuse(
                    crate::structs::edge_boot::RefusalReason::Disabled,
                    "the workload-identity issuer that signs boot tickets is not enabled".into(),
                );
            };
            let node = self
                .store
                .get_node(&req.node_id)
                .map_err(|e| map_store_error("Error loading node", e))?;
            match crate::manager::edge_boot::verify_ticket(
                state,
                &req.ticket,
                &req.instance_id,
                &node.pool,
            ) {
                Ok(claims) => {
                    spend = Some(crate::manager::edge_boot::TicketSpend {
                        jti: claims.jti,
                        now_unix: time::OffsetDateTime::now_utc().unix_timestamp(),
                    });
                }
                Err(err) => {
                    return refuse(
                        crate::structs::edge_boot::RefusalReason::TicketInvalid,
                        format!("{err:#}"),
                    );
                }
            }
        }

        let started = std::time::Instant::now();
        let claim = crate::manager::edge_boot::ClaimRatification {
            node_id: &req.node_id,
            claim_id: &req.claim_id,
            job_config_id: &req.job_config_id,
            task_name: &req.task_name,
            instance_id: &req.instance_id,
            gate: crate::structs::edge_boot::Gate::from_wire(req.gate),
        };
        let verdict = crate::manager::edge_boot::ratify(&self.store, &claim, spend, started)
            .map_err(|e| Status::internal(format!("ratification failed: {e:#}")))?;

        let job_id = self
            .store
            .job_id_for_job_config(&req.job_config_id)
            .unwrap_or_default()
            .unwrap_or_default();
        match verdict {
            crate::manager::edge_boot::Verdict::Confirmed { allocation_id } => {
                tracing::Span::current().record("verdict", "confirmed");
                crate::manager::telemetry::worker_metrics()
                    .provisional_ratify_seconds
                    .observe(started.elapsed().as_secs_f64());
                // `alloc.ratified` was recorded inside the ratification's
                // own entry.
                // The committed payload rides back with the verdict: the
                // node booted from one that could not name assigned ports,
                // the bound instance, or resolved mounts, and must replace it
                // before it starts the workload or opens the gate.
                let payload = self.committed_payload(allocation_id, &req.claim_id, req.gate);
                if payload.is_none() {
                    warn!(
                        allocation_id,
                        claim_id = %req.claim_id,
                        "Ratified allocation could not be rendered; the node will kill the claim"
                    );
                }
                Ok(Response::new(RatifyClaimReply {
                    confirmed: true,
                    allocation_id,
                    reason: 0,
                    detail: String::new(),
                    allocation: payload,
                }))
            }
            crate::manager::edge_boot::Verdict::Refused { reason, detail } => {
                tracing::Span::current().record("verdict", "refused");
                self.store.events().provisional_refused(
                    &req.claim_id,
                    &req.node_id,
                    &job_id,
                    &req.instance_id,
                    reason.as_str(),
                    &detail,
                );
                refuse(reason, detail)
            }
        }
    }
}

/// The idempotency payload for `IssueBootTicket`.
///
/// It carries the signed ticket so a retried mint returns the capability that
/// already exists rather than minting a second one — two live tickets where
/// the caller believes there is one is precisely the surface this feature
/// must not grow.
#[derive(serde::Serialize, serde::Deserialize)]
struct MintedTicketRecord {
    ticket: String,
    jti: String,
    job_config_id: String,
    expires_at_unix: i64,
}

/// The stored form of a client-chosen idempotency key, scoped to one
/// operation and one caller. Keys used to be a single global namespace: a
/// `job run` key replayed on `job destroy` silently skipped the destroy and
/// reported `deduplicated`, and a second caller presenting someone else's key
/// read back their recorded reply. The caller is the token's accessor id
/// (stable across a token's lifetime and safe to store), falling back to the
/// principal for JWT and anonymous callers. A missing or blank key stays
/// empty, which disables deduplication. Length prefixes keep the encoding
/// unambiguous whatever the parts contain.
fn scoped_idempotency_key(identity: &AclIdentity, operation: &str, key: Option<&str>) -> String {
    let key = key.map(str::trim).unwrap_or_default();
    if key.is_empty() {
        return String::new();
    }
    let caller = identity
        .accessor_id
        .as_deref()
        .unwrap_or(identity.principal.as_str());
    format!("{operation}/{}:{caller}/{key}", caller.len())
}

/// The scope a volume write is checked against. A job's volume belongs to
/// that job. A volume no job owns belongs to no job's scope, so deleting it
/// takes cluster-wide `job:write`: `AclScope::none()` carries no job name, so
/// only a rule without `names`/`namespaces` selectors matches it.
fn volume_write_scope(volume: &crate::structs::volume::Volume) -> AclScope {
    match volume.job_id.as_deref() {
        Some(job_id) => AclScope::job(job_id),
        None => AclScope::none(),
    }
}

/// What a worker-registration caller may register as, decided by
/// `verify_join_for_registration` from the credential it presented.
#[derive(Debug)]
enum RegistrationGrant {
    /// ACLs off, a management credential, or an audit-only pass.
    Unrestricted,
    /// A verified join token: pins the worker type, and through it the pool.
    JoinToken(crate::storage::acl_store::JoinTokenRecord),
    /// A node token: may only re-register its own node, in its current pool.
    Node(String),
}

/// Marker error: the StreamEvents client went away, stop the stream task.
struct StreamSendStopped;

/// Send journal events with `id > after_id` (matching `filter`) into a stream
/// channel in batches. Returns the new cursor (highest event ID sent, or
/// `after_id` when nothing matched).
async fn replay_journal_events(
    store: &Store,
    filter: &SchedulerEventFilter,
    after_id: i64,
    tx: &tokio::sync::mpsc::Sender<Result<SchedulerEvent, Status>>,
) -> Result<i64, StreamSendStopped> {
    let mut cursor = after_id;
    loop {
        let batch_filter = SchedulerEventFilter {
            after_id: Some(cursor),
            limit: Some(EVENT_STREAM_REPLAY_BATCH),
            ..filter.clone()
        };
        let batch = match store.list_scheduler_events(&batch_filter) {
            Ok(batch) => batch,
            Err(err) => {
                error!(error = ?err, "Error replaying scheduler events into stream");
                let _ = tx
                    .send(Err(Status::internal("Error replaying scheduler events")))
                    .await;
                return Err(StreamSendStopped);
            }
        };
        if batch.is_empty() {
            return Ok(cursor);
        }
        let batch_len = batch.len();
        for event in batch {
            cursor = cursor.max(event.id);
            if tx.send(Ok(map_scheduler_event(event))).await.is_err() {
                return Err(StreamSendStopped);
            }
        }
        if (batch_len as u32) < EVENT_STREAM_REPLAY_BATCH {
            return Ok(cursor);
        }
    }
}

/// Proxy a single allocation's log read to the worker hosting it.
///
/// Returns the captured streams, or a human-readable error string suitable for
/// the per-allocation `error` field (so one unreachable node does not fail the
/// whole request). Worker clients are reused across allocations on the same node.
async fn fetch_allocation_logs(
    store: &Store,
    allocation: &crate::structs::Allocation,
    req: &GetAllocationLogsRequest,
    clients: &mut HashMap<String, WorkerClient<Channel>>,
) -> Result<Vec<crate::structs::TaskLogStream>, String> {
    let node = store
        .get_node(&allocation.node_id)
        .map_err(|e| format!("failed to load node {}: {e}", allocation.node_id))?;
    let target = crate::manager::worker_plane::WorkerTarget::for_node(&node)
        .map_err(|status| status.message().to_string())?;
    // Keyed by what the channel verifies, not the address alone: two nodes
    // that advertise one address must not share a channel.
    let key = format!("{}@{}", target.server_name, target.address);
    if !clients.contains_key(&key) {
        let client = crate::manager::worker_plane::WorkerPlane::global()
            .client_for(store, &target)
            .map_err(|status| status.message().to_string())?;
        clients.insert(key.clone(), client);
    }
    let client = clients.get_mut(&key).expect("worker client just inserted");

    let reply = client
        .get_task_logs(GetTaskLogsRequest {
            allocation_id: allocation.id,
            stream: req.stream.clone(),
            max_bytes: req.max_bytes,
            tail: req.tail,
            offset: req.offset,
        })
        .await
        .map_err(|status| {
            let status = crate::manager::worker_plane::worker_call_error(
                &allocation.node_id,
                &target.address,
                status,
            );
            if crate::manager::worker_plane::is_unreachable(&status) {
                status.message().to_string()
            } else {
                format!("worker on {} returned {}", allocation.node_id, status)
            }
        })?;

    Ok(reply.into_inner().streams)
}

/// Record terminal, non-failure allocation outcomes reported by a worker: the
/// task's process exited and the task's `restart` policy says that is the end
/// of it (see docs/task-supervision.md).
///
/// Failures do NOT come through here — they arrive as `failed_allocations` and
/// go through the reschedule/move machinery. This path is the one that makes a
/// batch task's success real: the allocation row becomes terminal and the
/// task's status follows the exit code.
/// Apply what a node's memory pressure response did (docs/resource-
/// enforcement.md, "Memory pressure"): a `node.pressure` event per action,
/// and for the eviction steps, the allocation's removal with
/// `cause = node_pressure` plus an eval so the scheduler replaces it.
///
/// The worker has already stopped the task; this records the fact. Reports
/// for allocations this node does not hold (already removed, or another
/// node's) are ignored, which also makes a re-sent report harmless.
fn handle_pressure_actions(
    store: &Store,
    node_id: &str,
    actions: &[crate::structs::PressureAction],
) -> R<()> {
    let metrics = crate::manager::telemetry::metrics();
    for action in actions {
        metrics
            .node_pressure_actions_total
            .inc(&[node_id, action.step.as_str()]);
        if action.step == "reclaim" {
            let instance_id = pressure_instance_id(store, action);
            store
                .events()
                .node_pressure(node_id, action, instance_id.as_deref());
            continue;
        }
        if action.step == "suspend" {
            // The state change rides the local suspension's snapshot report
            // (applied earlier in this heartbeat): the instance is
            // `suspended`, not evicted, and nothing is rescheduled.
            let instance_id = pressure_instance_id(store, action);
            store
                .events()
                .node_pressure(node_id, action, instance_id.as_deref());
            continue;
        }
        let Some(existing) = store.get_allocation_by_id(action.allocation_id)? else {
            debug!(
                allocation_id = action.allocation_id,
                node_id, "Pressure-evicted allocation already removed"
            );
            continue;
        };
        if existing.node_id != node_id {
            warn!(
                allocation_id = action.allocation_id,
                reported_node_id = %node_id,
                owner_node_id = %existing.node_id,
                "Ignoring pressure eviction report from non-owner node"
            );
            continue;
        }
        store
            .events()
            .node_pressure(node_id, action, existing.instance_id.as_deref());
        // The worker's own sentence and hint become the instance's
        // `last_stop`, so the row explains itself without the event.
        let explanation = crate::storage::store::StopExplanation {
            detail: action.detail.clone(),
            hint: action.hint.clone(),
        };
        let Some(removed) = store.remove_allocation_by_id_explained(
            action.allocation_id,
            StopCause::NodePressure,
            Some(explanation),
        )?
        else {
            continue;
        };
        let priority = store
            .cached_band_for_job_config(&removed.job_config_id)
            .map(|band| band.eval_priority())
            .unwrap_or(store.default_eval_priority);
        store.enqueue_eval(
            "job_reschedule",
            Some(&removed.job_id),
            Some(&removed.job_config_id),
            Some(node_id),
            Some(priority),
        )?;
        info!(
            allocation_id = removed.id,
            job_id = %removed.job_id,
            node_id,
            step = %action.step,
            "Allocation removed for node memory pressure; replacement scheduled"
        );
    }
    Ok(())
}

/// The instance a pressure action touched, for the `node.pressure` event:
/// the allocation's instance while its row exists, else (a pressure suspend,
/// whose allocation the snapshot report already removed) the instance its
/// checkpoint was taken from. Lookup failures read as none: the field is
/// advisory.
fn pressure_instance_id(store: &Store, action: &crate::structs::PressureAction) -> Option<String> {
    if let Ok(Some(allocation)) = store.get_allocation_by_id(action.allocation_id) {
        if allocation.instance_id.is_some() {
            return allocation.instance_id;
        }
    }
    if action.snapshot_id.is_empty() {
        return None;
    }
    store
        .get_snapshot(&action.snapshot_id)
        .ok()
        .flatten()
        .and_then(|snapshot| snapshot.instance_id)
}

fn handle_terminated_allocations(
    store: &Store,
    node_id: &str,
    terminated: &[crate::structs::TerminatedAllocation],
) -> R<()> {
    let mut freed_capacity = false;
    for report in terminated {
        let allocation_id = report.allocation_id;
        let Some(existing) = store.get_allocation_by_id(allocation_id)? else {
            warn!(
                allocation_id,
                node_id, "Terminated allocation not found; ignoring report"
            );
            continue;
        };
        if existing.node_id != node_id {
            warn!(
                allocation_id,
                reported_node_id = %node_id,
                owner_node_id = %existing.node_id,
                "Ignoring terminal allocation report from non-owner node"
            );
            continue;
        }

        let Some(allocation) = store.complete_allocation(allocation_id, report.success)? else {
            continue;
        };
        freed_capacity = true;
        // Tasks that depend on this one were held until it exited; queue
        // the eval that places them. Best effort: a failure here must not
        // fail the heartbeat, and the next eval of the job re-checks.
        if let Err(error) = store.queue_dependency_progress(&allocation) {
            warn!(
                allocation_id,
                job_id = %allocation.job_id,
                error = %format!("{error:#}"),
                "Failed to queue dependency progress after a terminal report"
            );
        }

        store.events().allocation_terminated(
            allocation_id,
            node_id,
            &allocation.job_id,
            &allocation.job_config_id,
            allocation.task_config_id,
            report.success,
            &report.message,
        );
        info!(
            allocation_id,
            node_id,
            job_id = %allocation.job_id,
            success = report.success,
            status = %report.message,
            "Allocation reached a terminal state"
        );
    }
    // A finished allocation no longer holds node capacity: retry jobs that
    // are short of room (no-op unless some plan left replicas unplaced).
    if freed_capacity {
        store.request_capacity_reconcile_if_waiting();
    }
    Ok(())
}

fn handle_failed_allocations(
    store: &Store,
    failure_tracker: &Arc<Mutex<AllocationFailureTracker>>,
    node_id: &str,
    failed: &[FailedAllocation],
) -> R<()> {
    let mut freed_capacity = false;
    for failure in failed {
        let allocation_id = failure.allocation_id;
        let allocation_context = {
            let mut tracker = failure_tracker.lock().expect("allocation failure tracker");
            tracker.context_for_allocation(store, allocation_id)?
        };

        let Some(context) = allocation_context else {
            warn!(
                allocation_id,
                node_id, "Failed allocation not found; skipping reschedule"
            );
            continue;
        };
        if context.node_id != node_id {
            warn!(
                allocation_id,
                reported_node_id = %node_id,
                owner_node_id = %context.node_id,
                "Ignoring failed allocation report from non-owner node"
            );
            continue;
        }

        store.remove_allocation_by_id(allocation_id)?;
        freed_capacity = true;

        store.events().allocation_failed(
            allocation_id,
            node_id,
            &context.job_id,
            &context.job_config_id,
            context.task_config_id,
            &failure.reason,
        );
        // Artifact staging failures additionally carry the final per-artifact
        // states; surface each failed artifact as its own event with the
        // redacted source, error, and attempt count.
        if let Some(artifact_states) = failure.artifact_states.as_deref() {
            store.events().artifact_failed_rows(
                allocation_id,
                node_id,
                &context.job_id,
                artifact_states,
            );
        }

        let move_decision = {
            let mut tracker = failure_tracker.lock().expect("allocation failure tracker");
            tracker.record_move(&context)?
        };

        match move_decision {
            MoveDecision::Reschedule { delay } => {
                let job_id = context.job_id.clone();
                let job_config_id = context.job_config_id.clone();
                let node_id = node_id.to_string();
                let store = store.clone();
                schedule_reschedule(store, job_id, job_config_id, node_id, delay);
            }
            MoveDecision::JobFailed => {
                store.mark_job_failed(&context.job_id)?;
                // The task failed for good: say so in its status, so a
                // `complete` dependent can run and a `success` one never does.
                if let Err(error) = store.set_task_status(
                    context.task_config_id,
                    crate::structs::TaskStatusState::Failed,
                ) {
                    warn!(
                        allocation_id,
                        job_id = %context.job_id,
                        error = %format!("{error:#}"),
                        "Failed to record the failed task status"
                    );
                }
                warn!(
                    allocation_id,
                    job_id = %context.job_id,
                    reason = %failure.reason,
                    "Move limit exceeded; marking job failed"
                );
            }
        }

        failure_tracker
            .lock()
            .expect("allocation failure tracker")
            .allocations
            .remove(&allocation_id);

        warn!(
            allocation_id,
            node_id,
            reason = %failure.reason,
            "Allocation reported failed; reschedule requested"
        );
    }

    // The removed rows freed node capacity (see
    // `Store::request_capacity_reconcile_if_waiting`). Reports that were
    // ignored (unknown, or from a node that does not own the allocation)
    // freed nothing.
    if freed_capacity {
        store.request_capacity_reconcile_if_waiting();
    }
    Ok(())
}

/// Maps a template evaluation error to a gRPC status: a full evaluation
/// budget is the manager's problem and retryable (`RESOURCE_EXHAUSTED`),
/// anything else is the template's or the values' fault.
fn template_eval_status(err: anyhow::Error) -> Status {
    if err.is::<job_templates::EvalSlotsBusy>() {
        Status::resource_exhausted(err.to_string())
    } else {
        Status::invalid_argument(err.to_string())
    }
}

/// Runs blocking admission work (artifact pin resolution can reach
/// registries) without stalling the async runtime where possible.
/// `block_in_place` needs the multi-thread runtime; current-thread callers
/// (tests) run inline.
fn run_blocking_admission<T>(work: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(work)
        }
        _ => work(),
    }
}

/// Lines describing artifacts whose pinned source changed between the stored
/// job config and a newly normalized one (`job plan` digest diffs, plan §7).
/// Artifacts are matched by task + name (or original mutable reference).
fn artifact_pin_diffs(existing: &JobConfig, planned: &JobConfig) -> Vec<String> {
    let mut diffs = Vec::new();
    for planned_task in &planned.tasks {
        let Some(existing_task) = existing
            .tasks
            .iter()
            .find(|task| task.name == planned_task.name)
        else {
            continue;
        };
        for planned_artifact in &planned_task.artifacts {
            let matched = existing_task.artifacts.iter().find(|artifact| {
                match (planned_artifact.name.as_deref(), artifact.name.as_deref()) {
                    (Some(a), Some(b)) => a == b,
                    _ => artifact.pinned_from == planned_artifact.pinned_from,
                }
            });
            let Some(existing_artifact) = matched else {
                continue;
            };
            if existing_artifact.pinned_from.is_some()
                && planned_artifact.pinned_from.is_some()
                && existing_artifact.source != planned_artifact.source
            {
                diffs.push(format!(
                    "task {}: artifact {} moved: {} -> {}",
                    planned_task.name,
                    planned_artifact
                        .pinned_from
                        .as_deref()
                        .map(crate::structs::redact_source)
                        .unwrap_or_default(),
                    crate::structs::redact_source(&existing_artifact.source),
                    crate::structs::redact_source(&planned_artifact.source),
                ));
            }
        }
    }
    diffs
}

fn update_allocation_health_snapshots(
    store: &Store,
    node_id: &str,
    snapshots: &[AllocationHealth],
) -> R<()> {
    if snapshots.is_empty() {
        return Ok(());
    }

    store.upsert_allocation_health_snapshots(node_id, snapshots)
}

enum MoveDecision {
    Reschedule { delay: Duration },
    JobFailed,
}

fn schedule_reschedule(
    store: Store,
    job_id: String,
    job_config_id: String,
    node_id: String,
    delay: Duration,
) {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn(async move {
                if delay.as_secs() > 0 {
                    tokio::time::sleep(delay).await;
                }
                if let Err(err) = store.enqueue_eval(
                    "job_reschedule",
                    Some(&job_id),
                    Some(&job_config_id),
                    Some(&node_id),
                    None,
                ) {
                    error!(
                        job_id = %job_id,
                        job_config_id = %job_config_id,
                        error = ?err,
                        "Failed to enqueue reschedule after unhealthy allocation"
                    );
                }
            });
        }
        Err(_) => {
            let delay_for_thread = delay;
            let job_id = job_id.clone();
            let job_config_id = job_config_id.clone();
            let node_id = node_id.clone();
            std::thread::spawn(move || {
                if delay_for_thread.as_secs() > 0 {
                    std::thread::sleep(delay_for_thread);
                }
                if let Err(err) = store.enqueue_eval(
                    "job_reschedule",
                    Some(&job_id),
                    Some(&job_config_id),
                    Some(&node_id),
                    None,
                ) {
                    error!(
                        job_id = %job_id,
                        job_config_id = %job_config_id,
                        error = ?err,
                        "Failed to enqueue reschedule after unhealthy allocation"
                    );
                }
            });
        }
    }
}

impl AllocationFailureTracker {
    fn context_for_allocation(
        &mut self,
        store: &Store,
        allocation_id: i64,
    ) -> R<Option<AllocationFailureContext>> {
        if let Some(context) = self.allocations.get(&allocation_id) {
            return Ok(Some(context.clone()));
        }

        let Some(allocation) = store.get_allocation_by_id(allocation_id)? else {
            return Ok(None);
        };

        let task_configs = store.cached_task_configs_for_job_config(&allocation.job_config_id)?;
        let task = task_configs
            .into_iter()
            .find(|(task_id, _)| *task_id == allocation.task_config_id)
            .map(|(_, task)| task)
            .context("Missing task config for allocation")?;

        let context = AllocationFailureContext {
            job_id: allocation.job_id,
            job_config_id: allocation.job_config_id,
            task_config_id: allocation.task_config_id,
            node_id: allocation.node_id,
            on_unhealthy: task.on_unhealthy,
        };
        self.allocations.insert(allocation_id, context.clone());
        Ok(Some(context))
    }

    fn record_move(&mut self, context: &AllocationFailureContext) -> R<MoveDecision> {
        let now = OffsetDateTime::now_utc();
        let key = TaskKey {
            job_config_id: context.job_config_id.clone(),
            task_config_id: context.task_config_id,
        };
        let history = self.move_history.entry(key).or_default();
        let window = time_duration(context.on_unhealthy.move_period);
        history.retain(|time| now - *time <= window);

        if let Some(limit) = context.on_unhealthy.move_count {
            if history.len() as u32 >= limit {
                return Ok(MoveDecision::JobFailed);
            }
        }

        history.push(now);
        Ok(MoveDecision::Reschedule {
            delay: context.on_unhealthy.move_delay,
        })
    }
}

fn time_duration(duration: Duration) -> TimeDuration {
    let secs = duration.as_secs() as i64;
    let nanos = duration.subsec_nanos() as i64;
    TimeDuration::seconds(secs) + TimeDuration::nanoseconds(nanos)
}

fn build_rollout_group_status(
    store: &Store,
    rollout: &crate::structs::Rollout,
) -> R<Vec<RolloutGroupStatus>> {
    let to_config = store.get_job_config_by_id(&rollout.to_job_config_id)?;
    let to_tasks = store.get_task_configs_for_job_config(&rollout.to_job_config_id)?;
    let from_tasks = if let Some(from_job_config_id) = rollout.from_job_config_id.as_deref() {
        store.get_task_configs_for_job_config(from_job_config_id)?
    } else {
        Vec::new()
    };
    let to_allocations = store.allocs_for_job_config(&rollout.to_job_config_id)?;
    let from_allocations = if let Some(from_job_config_id) = rollout.from_job_config_id.as_deref() {
        store.allocs_for_job_config(from_job_config_id)?
    } else {
        Vec::new()
    };
    let mut allocation_ids = to_allocations
        .iter()
        .map(|allocation| allocation.id)
        .collect::<Vec<_>>();
    allocation_ids.extend(from_allocations.iter().map(|allocation| allocation.id));
    let allocation_health = store.allocation_health_for_allocations(&allocation_ids)?;

    let runtime_groups = rollout
        .config_json
        .parse::<Value>()
        .ok()
        .and_then(|raw| raw.get("runtime").cloned())
        .and_then(|runtime| runtime.get("groups").cloned())
        .and_then(|groups| groups.as_object().cloned())
        .unwrap_or_default();

    let node_health = store
        .get_nodes()?
        .into_iter()
        .map(|node| (node.id.clone(), derive_node_health(&node.health_checks)))
        .collect::<HashMap<_, _>>();

    let mut to_by_id = HashMap::new();
    let mut from_by_id = HashMap::new();
    let mut groups = BTreeSet::new();
    let mut desired_count_by_group = HashMap::new();
    for task in &to_config.tasks {
        desired_count_by_group
            .entry(task.group.clone())
            .and_modify(|count: &mut u32| *count = (*count).max(task.count.num))
            .or_insert(task.count.num);
    }
    for (task_id, task) in to_tasks {
        groups.insert(task.group.clone());
        to_by_id.insert(task_id, task);
    }
    for (task_id, task) in from_tasks {
        groups.insert(task.group.clone());
        from_by_id.insert(task_id, task);
    }
    groups.extend(runtime_groups.keys().cloned());

    let mut allocations_by_group: HashMap<String, Vec<RolloutAllocationStatus>> = HashMap::new();
    for allocation in &to_allocations {
        let Some(task) = to_by_id.get(&allocation.task_config_id) else {
            continue;
        };
        allocations_by_group
            .entry(task.group.clone())
            .or_default()
            .push(RolloutAllocationStatus {
                allocation_id: allocation.id,
                node_id: allocation.node_id.clone(),
                task_config_id: allocation.task_config_id,
                task_name: task.name.clone(),
                task_group: task.group.clone(),
                lifecycle: task.lifecycle.to_string(),
                status: allocation.status.clone(),
                role: "replacement".to_string(),
                node_healthy: node_health
                    .get(&allocation.node_id)
                    .and_then(|value| *value),
                allocation_healthy: allocation_health.get(&allocation.id).map(|h| h.healthy),
                allocation_health: allocation_health
                    .get(&allocation.id)
                    .map(|h| h.status.clone()),
                artifact_states: allocation_health
                    .get(&allocation.id)
                    .and_then(|h| h.artifact_states.clone()),
            });
    }
    for allocation in &from_allocations {
        let Some(task) = from_by_id.get(&allocation.task_config_id) else {
            continue;
        };
        allocations_by_group
            .entry(task.group.clone())
            .or_default()
            .push(RolloutAllocationStatus {
                allocation_id: allocation.id,
                node_id: allocation.node_id.clone(),
                task_config_id: allocation.task_config_id,
                task_name: task.name.clone(),
                task_group: task.group.clone(),
                lifecycle: task.lifecycle.to_string(),
                status: allocation.status.clone(),
                role: "replacee".to_string(),
                node_healthy: node_health
                    .get(&allocation.node_id)
                    .and_then(|value| *value),
                allocation_healthy: allocation_health.get(&allocation.id).map(|h| h.healthy),
                allocation_health: allocation_health
                    .get(&allocation.id)
                    .map(|h| h.status.clone()),
                artifact_states: allocation_health
                    .get(&allocation.id)
                    .and_then(|h| h.artifact_states.clone()),
            });
    }

    let mut group_statuses = Vec::new();
    for group in groups {
        let runtime = runtime_groups.get(&group);
        let step_index = runtime
            .and_then(|value| value.get("stepIndex"))
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32;
        let pending_manual_advances = runtime
            .and_then(|value| value.get("pendingManualAdvances"))
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32;
        let base_count = desired_count_by_group.get(&group).copied().unwrap_or(1);
        let configured_rollout = to_config
            .rollout_config_for_group(&group)
            .or_else(|| to_config.rollout.clone())
            .unwrap_or_default();
        let effective_rollout =
            rollout_strategy::resolve_rollout_strategy(configured_rollout, &group, base_count)?;
        // Ordered slot groups (`rollout.slots.order: 'sequential'`) have one
        // step per slot being updated; mirror the engine's step math so
        // step_index/total_steps stay legible for sequential progress.
        let slot_count = to_config
            .tasks
            .iter()
            .find(|task| {
                task.group == group
                    && task
                        .stateful
                        .as_ref()
                        .is_some_and(|stateful| stateful.mode == InstanceMode::Slot)
            })
            .map(|task| task.count.num);
        let total_steps = match (effective_rollout.slots.as_ref(), slot_count) {
            (Some(slots), Some(count)) if slots.is_sequential() => {
                sequential_step_fractions(count, slots.update_from.unwrap_or(0)).len() as u32
            }
            _ => step_fractions_for_rollout(&effective_rollout, base_count).len() as u32,
        };

        let mut allocations = allocations_by_group.remove(&group).unwrap_or_default();
        allocations.sort_by_key(|entry| entry.allocation_id);

        group_statuses.push(RolloutGroupStatus {
            group,
            step_index,
            total_steps,
            pending_manual_advances,
            allocations,
        });
    }

    Ok(group_statuses)
}

fn derive_node_health(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(flag) => Some(*flag),
        Value::String(text) => Some(text.eq_ignore_ascii_case("pass")),
        Value::Array(values) => {
            let mut seen = false;
            let mut all_ok = true;
            for item in values {
                if let Some(ok) = derive_node_health(item) {
                    seen = true;
                    all_ok &= ok;
                }
            }
            if seen {
                Some(all_ok)
            } else {
                None
            }
        }
        Value::Object(map) => {
            let mut seen = false;
            let mut all_ok = true;
            for entry in map.values() {
                if let Some(ok) = derive_node_health(entry) {
                    seen = true;
                    all_ok &= ok;
                }
            }
            if seen {
                Some(all_ok)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn build_desired_allocations(
    store: &Store,
    node_id: &str,
    _reported_allocations: &[i64],
) -> R<Vec<crate::structs::TaskAllocation>> {
    let desired = store.allocs_for_node(node_id)?;
    if desired.is_empty() {
        return Ok(Vec::new());
    }

    let mut task_cache = DesiredTaskCache::default();
    let mut allocations = Vec::new();

    for allocation in desired {
        // A finished allocation keeps its row (the slot is still spent) but
        // must never be dispatched again — re-sending it would relaunch a
        // completed batch task on the next worker restart.
        // A suspended instance's held reservation (`suspend.reserve`) is a
        // row with nothing to run.
        if !crate::constants::allocation_status_is_dispatchable(&allocation.status) {
            continue;
        }
        allocations.push(desired_payload(store, &allocation, &mut task_cache)?);
    }

    Ok(allocations)
}

/// Task configs by job config, for building many payloads.
#[derive(Default)]
pub(crate) struct DesiredTaskCache(HashMap<String, HashMap<i64, TaskConfig>>);

/// The payload a worker runs one allocation from: the same
/// `TaskAllocation` whichever path delivers it — the heartbeat's desired
/// set, a `StreamWork` message, or the `PostWork` push — so a worker that
/// launches from whichever copy reaches it first launches the same thing.
pub(crate) fn desired_payload(
    store: &Store,
    allocation: &crate::structs::Allocation,
    task_cache: &mut DesiredTaskCache,
) -> R<crate::structs::TaskAllocation> {
    let tasks = task_cache
        .0
        .entry(allocation.job_config_id.clone())
        .or_default();
    if tasks.is_empty() {
        // The cached accessor, deliberately: this runs once per distinct job
        // config per node per heartbeat, so on a 5,000-node fleet at a 2s
        // cadence it was ~10k read-connection acquisitions per second for
        // rows the read model already holds. Task configs are immutable per
        // `job_config_id` — a change mints a new config id — which is why the
        // scheduler already reads them this way and why the cache needs no
        // invalidation.
        let task_configs = store.cached_task_configs_for_job_config(&allocation.job_config_id)?;
        for (id, config) in task_configs {
            tasks.insert(id, config);
        }
    }
    let task = tasks
        .get(&allocation.task_config_id)
        .with_context(|| format!("Missing task config {}", allocation.task_config_id))?;
    let instance = match allocation.instance_id.as_deref() {
        Some(instance_id) => store.get_instance(instance_id)?,
        None => None,
    };
    let volumes =
        store.volume_mounts_for_allocation(&allocation.job_id, task, instance.as_ref())?;
    let mut payload = build_task_allocation(task, allocation, instance.as_ref(), &volumes)?;
    crate::manager::dispatcher::attach_session_task_spec(
        &mut payload,
        task,
        tasks.iter().map(|(id, task)| (*id, task)),
    )?;
    // The band orders the worker's memory pressure response. Read from the
    // read model, so building a payload pays no store read for it.
    payload.priority_band = store
        .cached_band_for_job_config(&allocation.job_config_id)?
        .as_str()
        .to_string();
    payload.restore = snapshot_rpcs::snapshot_restore_payload(store, instance.as_ref());
    Ok(payload)
}

/// Resolve the current endpoint set for every upstream declared by this node's
/// desired allocations — the service-discovery feed the worker's loopback
/// forwarders are driven from.
///
/// Fast path: allocations carry their `connect` block on the wire, so when no
/// desired allocation declares upstreams this returns an empty vector without
/// touching the store. Nodes running only plain jobs therefore pay zero extra
/// queries per heartbeat (heartbeats run every
/// `WORKER_HEARTBEAT_DURATION_SEC`, so this is the hot path).
///
/// Otherwise each *distinct* `(upstream service, consuming job)` pair is
/// resolved once and memoized: the store lookup is an indexed seek over just
/// that service's rows (see [`Store::service_endpoints`]), and the resulting
/// wire list is shared by every allocation of the same job — a node running
/// `count: 40` of one client task would otherwise rebuild the identical list 40
/// times, every 2s. Endpoints are filtered to healthy ones and gated by the
/// target service's `service.allow` intentions against the *consuming*
/// allocation's job id; a denied consumer gets an empty list rather than an
/// error.
///
/// The feed is **additive and never fatal** — which is why it returns a plain
/// `Vec` and not an `R<_>`. A store failure (or an unparseable stored payload)
/// omits that upstream's entry instead of failing the heartbeat: the worker only
/// calls `set_endpoints` for entries actually present in the reply, so an
/// omitted upstream keeps its last known good endpoints — the same fail-static
/// property, without taking the node's whole allocation reconciliation (and,
/// after each stateful task's fence window, its running allocations) down with
/// it.
impl ManagerDaemonService {
    /// The token behind a `from = { identity = "<name>" }` upstream
    /// credential (docs/network-handlers.md, "Credential injection"): the
    /// task's identity of that name, minted for the allocation exactly as
    /// `IssueWorkloadIdentityToken` would. The token the node already holds
    /// (`known.version` is its expiry) stays until less than half the
    /// identity's TTL remains, so a steady node mints about twice per TTL.
    /// The caller has already checked the allocation is on the calling node.
    fn upstream_identity_token(
        &self,
        known: &crate::structs::UpstreamCredentialKnown,
        identity_name: &str,
        out: &mut UpstreamCredentialValue,
    ) {
        let Some(state) = self.workload_identity.as_ref() else {
            out.error = "workload identity is not enabled on this manager (set \
                         workloadIdentity.enabled: true in the manager config)"
                .to_string();
            return;
        };
        let allocation = match self.store.get_allocation_by_id(known.allocation_id) {
            Ok(Some(allocation)) => allocation,
            Ok(None) => {
                out.error = format!("allocation {} not found", known.allocation_id);
                out.absent = true;
                return;
            }
            Err(err) => {
                warn!(allocation_id = known.allocation_id, error = ?err, "Failed loading allocation for an upstream identity");
                out.error = "allocation could not be loaded".to_string();
                return;
            }
        };
        let task = match self
            .store
            .get_task_configs_for_job_config(&allocation.job_config_id)
        {
            Ok(tasks) => tasks
                .into_iter()
                .find(|(id, _)| *id == allocation.task_config_id)
                .map(|(_, task)| task),
            Err(err) => {
                warn!(allocation_id = known.allocation_id, error = ?err, "Failed loading task config for an upstream identity");
                None
            }
        };
        let Some(task) = task else {
            out.error = "task config could not be loaded".to_string();
            return;
        };
        let Some(identity) = task
            .identities
            .iter()
            .find(|identity| identity.name == identity_name)
        else {
            out.error = format!(
                "task {} declares no identity named {identity_name:?}",
                task.name
            );
            out.absent = true;
            return;
        };

        let now = std::time::SystemTime::now();
        let now_secs = now
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs() as i64)
            .unwrap_or_default();
        let half_life = (state.token_lifetime(identity).as_secs() / 2) as i64;
        if known.version > 0 && known.version - now_secs > half_life {
            out.version = known.version;
            out.unchanged = true;
            return;
        }

        let claims =
            match claims_for_allocation(&self.store, &allocation, &task, DEF_CLUSTER_REGION) {
                Ok(claims) => claims,
                Err(err) => {
                    error!(error = ?err, "Error deriving workload identity claims for an upstream");
                    out.error = "failed to derive workload identity claims".to_string();
                    return;
                }
            };
        match state.issue(&claims, identity, now) {
            Ok(issued) => {
                // Lifecycle log only, never the token.
                info!(
                    allocation_id = known.allocation_id,
                    upstream = %known.upstream,
                    identity = %identity_name,
                    expires_at = issued.expires_at,
                    kid = %issued.kid,
                    "Issued workload identity token for upstream injection"
                );
                out.version = issued.expires_at as i64;
                out.value = issued.token;
            }
            Err(err) => {
                error!(error = ?err, "Error signing workload identity token for an upstream");
                out.error = "failed to sign workload identity token".to_string();
            }
        }
    }

    /// Effective network policy for every desired allocation, as content
    /// hashes. Zero store access when the cluster has no policy (cached
    /// generation) and no task on this node declares one.
    fn build_net_policies(
        &self,
        node_id: &str,
        desired: &[crate::structs::TaskAllocation],
    ) -> Vec<AllocNetPolicy> {
        use crate::structs::netpolicy::NetSelectorFacts;
        // Gated by placement, which can resolve `auto` with no `network`
        // block on the task at all (a default sandbox, gated plan Phase 2):
        // the allocation's own network records it.
        let placed_gated = |allocation: &crate::structs::TaskAllocation| {
            allocation
                .network
                .contains(crate::structs::netpolicy::MODE_GATED)
                && serde_json::from_str::<crate::structs::AllocNetwork>(&allocation.network)
                    .is_ok_and(|placed| placed.is_gated())
        };
        let any_inline = desired
            .iter()
            .any(|allocation| !allocation.task_network.trim().is_empty());
        if self.net_policy_generation_cached() == 0
            && !any_inline
            && !desired.iter().any(placed_gated)
        {
            return Vec::new();
        }
        // Stored policies once per heartbeat; the node's pool once, and only
        // when a policy could select on it.
        let stored: Vec<crate::structs::netpolicy::NetPolicyObject> = match self
            .store
            .list_net_policies()
        {
            Ok(records) => records.into_iter().map(|record| record.object).collect(),
            Err(err) => {
                warn!(node_id = %node_id, error = ?err, "Skipping net policies; listing failed");
                return Vec::new();
            }
        };
        let needs_pool = stored.iter().any(|p| !p.selector.node_pools.is_empty());
        let node_pool = if needs_pool {
            self.store.get_node(node_id).ok().map(|node| node.pool)
        } else {
            None
        };
        let mut out = Vec::new();
        for allocation in desired {
            let network: Option<crate::structs::NetworkConfig> =
                if allocation.task_network.trim().is_empty() {
                    None
                } else {
                    match serde_json::from_str(&allocation.task_network) {
                        Ok(network) => Some(network),
                        Err(err) => {
                            warn!(
                                allocation_id = allocation.allocation_id,
                                error = ?err,
                                "Skipping net policy; invalid task network payload"
                            );
                            continue;
                        }
                    }
                };
            let facts = NetSelectorFacts {
                node_pool: node_pool.clone(),
                namespace: String::new(),
                runtime: allocation.task_runtime.clone(),
                job: allocation.job_id.clone(),
                labels: network
                    .as_ref()
                    .map(|n| n.labels.clone())
                    .unwrap_or_default(),
            };
            let (effective, _) = merge_net_policies(&stored, &facts, network.as_ref());
            // A gated allocation is told about an *empty* policy too, so the
            // node can tell "known: nothing covers it" (`gated-default`) from
            // "pending" (`policy-pending`). Gated by its declaration, by a
            // `require` policy, or by placement resolving `auto` (a default
            // sandbox, gated plan Phase 2), which the allocation's own
            // network records.
            let gated = placed_gated(allocation)
                || network
                    .as_ref()
                    .is_some_and(|n| n.mode == crate::structs::netpolicy::MODE_GATED)
                || effective.mode.as_deref() == Some(crate::structs::netpolicy::MODE_GATED);
            if effective.is_empty() && !gated {
                continue;
            }
            self.remember_net_policy(&effective);
            out.push(AllocNetPolicy {
                allocation_id: allocation.allocation_id,
                policy_hash: effective.hash.clone(),
            });
        }
        out
    }

    /// The effective policy for one task, given selector facts. Returns the
    /// merge and the matched stored policy names in application order.
    fn effective_net_policy(
        &self,
        facts: &crate::structs::netpolicy::NetSelectorFacts,
        network: Option<&crate::structs::NetworkConfig>,
    ) -> (crate::structs::netpolicy::EffectivePolicy, Vec<String>) {
        let stored: Vec<crate::structs::netpolicy::NetPolicyObject> = self
            .store
            .list_net_policies()
            .map(|records| records.into_iter().map(|record| record.object).collect())
            .unwrap_or_default();
        merge_net_policies(&stored, facts, network)
    }

    /// Resolve (job, task) to the task config and its selector facts.
    fn net_policy_task_facts(
        &self,
        job_id: &str,
        task_name: &str,
        node_pool: &str,
    ) -> Result<
        (
            crate::structs::TaskConfig,
            crate::structs::netpolicy::NetSelectorFacts,
        ),
        Status,
    > {
        if job_id.trim().is_empty() {
            return Err(Status::invalid_argument("job_id is required"));
        }
        let config = self
            .store
            .get_job_config_optional(job_id)
            .map_err(|e| map_store_error("Error loading job", e))?
            .ok_or_else(|| Status::not_found(format!("job '{job_id}' not found")))?;
        let task = if task_name.trim().is_empty() {
            if config.tasks.len() == 1 {
                config.tasks[0].clone()
            } else {
                return Err(Status::invalid_argument(format!(
                    "job '{job_id}' has {} tasks; name one with task_name ({})",
                    config.tasks.len(),
                    config
                        .tasks
                        .iter()
                        .map(|t| t.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
        } else {
            config
                .tasks
                .iter()
                .find(|t| t.name == task_name)
                .cloned()
                .ok_or_else(|| {
                    Status::not_found(format!("task '{task_name}' not found in job '{job_id}'"))
                })?
        };
        let facts = crate::structs::netpolicy::NetSelectorFacts {
            node_pool: (!node_pool.trim().is_empty()).then(|| node_pool.trim().to_string()),
            namespace: String::new(),
            runtime: task.runtime.clone(),
            job: job_id.to_string(),
            labels: task
                .network
                .as_ref()
                .map(|n| n.labels.clone())
                .unwrap_or_default(),
        };
        Ok((task, facts))
    }

    /// The datapath status nodes last reported for this task's running
    /// allocations, as JSON, for `net doctor` and the UI.
    /// Endpoint lists for every `proxy` handler target (`service` or `node`)
    /// on the desired allocations' effective chains and upstreams, so the
    /// datapath can hand a flow to an external proxy without a listener of
    /// its own. Empty on nodes with no handler anywhere — the common case
    /// costs one pass over the policy hashes.
    fn build_handler_endpoints(
        &self,
        desired: &[crate::structs::TaskAllocation],
        policies: &[crate::structs::AllocNetPolicy],
    ) -> Vec<crate::structs::UpstreamEndpoints> {
        use crate::structs::netpolicy::HANDLER_PROXY;
        let mut wanted: Vec<(i64, String, String)> = Vec::new();
        let by_alloc: HashMap<i64, &str> = policies
            .iter()
            .map(|p| (p.allocation_id, p.policy_hash.as_str()))
            .collect();
        for allocation in desired {
            let mut services: Vec<String> = Vec::new();
            if let Some(hash) = by_alloc.get(&allocation.allocation_id) {
                let cached = {
                    let cache = self
                        .net_policy_cache
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    cache.get(*hash).cloned()
                };
                if let Some(policy) = cached {
                    for chain in [policy.egress.as_ref(), policy.ingress.as_ref()]
                        .into_iter()
                        .flatten()
                    {
                        for hop in &chain.handlers {
                            if hop.handler.handler == HANDLER_PROXY {
                                if let Some(service) =
                                    hop.handler.service.as_ref().or(hop.handler.node.as_ref())
                                {
                                    services.push(service.clone());
                                }
                            }
                        }
                    }
                }
            }
            if !allocation.task_connect.trim().is_empty() {
                if let Ok(connect) =
                    serde_json::from_str::<crate::structs::ConnectConfig>(&allocation.task_connect)
                {
                    for upstream in &connect.upstreams {
                        for hop in &upstream.handlers {
                            if hop.handler == HANDLER_PROXY {
                                if let Some(service) = hop.service.as_ref().or(hop.node.as_ref()) {
                                    services.push(service.clone());
                                }
                            }
                        }
                    }
                }
            }
            services.sort();
            services.dedup();
            for service in services {
                wanted.push((allocation.allocation_id, service, allocation.job_id.clone()));
            }
        }
        if wanted.is_empty() {
            return Vec::new();
        }
        let mut resolved: HashMap<String, Vec<crate::structs::ServiceEndpointRow>> = HashMap::new();
        let mut out = Vec::new();
        for (allocation_id, service, job_id) in wanted {
            if !resolved.contains_key(&service) {
                let rows = self
                    .store
                    .service_endpoints(Some(&service))
                    .unwrap_or_default();
                resolved.insert(service.clone(), rows);
            }
            let endpoints = resolved[&service]
                .iter()
                .filter(|row| row.healthy && crate::structs::allow_permits(&row.allow, &job_id))
                .map(|row| ServiceEndpoint {
                    service: row.service.clone(),
                    job_id: row.job_id.clone(),
                    task_name: row.task_name.clone(),
                    allocation_id: row.allocation_id,
                    node_id: row.node_id.clone(),
                    address: row.address.clone().unwrap_or_default(),
                    public_address: row.public_address.clone().unwrap_or_default(),
                    port: u32::from(row.port.unwrap_or(0)),
                    healthy: row.healthy,
                    health_status: row.health_status.clone(),
                    tags: row.tags.clone(),
                    meta: row.meta.clone().into_iter().collect(),
                    port_label: row.port_label.clone().unwrap_or_default(),
                    mesh_tls: row.mesh_tls,
                })
                .collect();
            out.push(crate::structs::UpstreamEndpoints {
                allocation_id,
                service,
                endpoints,
                for_handler: true,
            });
        }
        out
    }

    /// Fold one node's edge samples into the graph windows.
    fn fold_net_edges(
        &self,
        samples: &[crate::structs::NetEdgeSample],
        callers: &HashMap<i64, (String, String)>,
    ) {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let minute = now / 60;
        let mut edges = self.net_edges.lock().unwrap_or_else(|p| p.into_inner());
        for sample in samples {
            let Some((job, task)) = callers.get(&sample.allocation_id) else {
                continue;
            };
            let key = (
                job.clone(),
                task.clone(),
                sample.service.clone(),
                sample.tag.clone(),
            );
            let window = edges.entry(key).or_default();
            if !sample.protocol.is_empty() {
                window.protocol = sample.protocol.clone();
            }
            window.last_seen = now;
            if window.buckets.back().is_none_or(|b| b.minute != minute) {
                window.buckets.push_back(EdgeBucket {
                    minute,
                    ..Default::default()
                });
                while window.buckets.len() as i64 > EDGE_WINDOW_MINUTES {
                    window.buckets.pop_front();
                }
            }
            let bucket = window.buckets.back_mut().expect("bucket just pushed");
            bucket.requests += sample.requests;
            bucket.errors += sample.errors;
            bucket.denied += sample.denied;
            bucket.latency_ms_sum += sample.latency_ms_sum;
            bucket.latency_ms_max = bucket.latency_ms_max.max(sample.latency_ms_max);
            bucket.bytes_out += sample.bytes_out;
            bucket.bytes_in += sample.bytes_in;
        }
        // Idle edges age out with their last bucket.
        edges.retain(|_, window| now - window.last_seen <= EDGE_WINDOW_MINUTES * 60);
    }

    /// The `egress-unenforced` note for each running allocation of `task`
    /// that lands on a dev backend while the gated default is open (gated
    /// plan §6.6). Lookup failures degrade to no note.
    fn egress_unenforced_notes(
        &self,
        job_id: &str,
        task: &crate::structs::TaskConfig,
    ) -> Vec<String> {
        if !self.store.sandbox_gated_default() || task.runtime != "sandbox" {
            return Vec::new();
        }
        let Ok(allocations) = self.store.allocs_for_job(job_id) else {
            return Vec::new();
        };
        let mut notes = Vec::new();
        for allocation in allocations {
            let is_task = self
                .store
                .get_task_config_by_id(allocation.task_config_id)
                .ok()
                .flatten()
                .is_some_and(|candidate| candidate.name == task.name);
            if !is_task || allocation.status != crate::constants::ALLOCATION_STATUS_PLACED {
                continue;
            }
            if let Some(note) = egress_unenforced_detail(&self.store, &allocation, task) {
                let note = format!(
                    "allocation {} on node {}: {note}",
                    allocation.id, allocation.node_id
                );
                if !notes.contains(&note) {
                    notes.push(note);
                }
            }
        }
        notes
    }

    fn net_status_for_task(&self, job_id: &str, task_name: &str) -> Vec<String> {
        let Ok(allocations) = self.store.allocs_for_job(job_id) else {
            return Vec::new();
        };
        let mut task_allocs: std::collections::HashSet<i64> = std::collections::HashSet::new();
        let mut configs: HashMap<i64, bool> = HashMap::new();
        for allocation in &allocations {
            let is_task = *configs.entry(allocation.task_config_id).or_insert_with(|| {
                self.store
                    .get_task_config_by_id(allocation.task_config_id)
                    .ok()
                    .flatten()
                    .is_some_and(|task| task.name == task_name)
            });
            if is_task {
                task_allocs.insert(allocation.id);
            }
        }
        let status = self.net_status.lock().unwrap_or_else(|p| p.into_inner());
        let mut out = Vec::new();
        for (node_id, entries) in status.iter() {
            for entry in entries {
                if task_allocs.contains(&entry.allocation_id) {
                    out.push(
                        serde_json::to_string(&net_status_json(node_id, entry)).unwrap_or_default(),
                    );
                }
            }
        }
        out
    }

    /// Running allocations whose effective policy would change if `object`
    /// were applied, as `job/task@allocation`. Best effort; used by dry runs
    /// and the applied event.
    fn affected_allocations_for_policy(
        &self,
        object: &crate::structs::netpolicy::NetPolicyObject,
    ) -> Vec<String> {
        use crate::structs::netpolicy::NetSelectorFacts;
        let Ok(allocations) = self.store.get_allocs() else {
            return Vec::new();
        };
        let mut nodes: HashMap<String, Option<String>> = HashMap::new();
        let mut configs: HashMap<i64, crate::structs::TaskConfig> = HashMap::new();
        let mut out = Vec::new();
        for allocation in allocations {
            let pool = nodes
                .entry(allocation.node_id.clone())
                .or_insert_with(|| {
                    self.store
                        .get_node(&allocation.node_id)
                        .ok()
                        .map(|n| n.pool)
                })
                .clone();
            let task = match configs.get(&allocation.task_config_id) {
                Some(task) => task.clone(),
                None => match self.store.get_task_config_by_id(allocation.task_config_id) {
                    Ok(Some(task)) => {
                        configs.insert(allocation.task_config_id, task.clone());
                        task
                    }
                    _ => continue,
                },
            };
            let facts = NetSelectorFacts {
                node_pool: pool,
                namespace: String::new(),
                runtime: task.runtime.clone(),
                job: allocation.job_id.clone(),
                labels: task
                    .network
                    .as_ref()
                    .map(|n| n.labels.clone())
                    .unwrap_or_default(),
            };
            if object.selector.matches(&facts) {
                out.push(format!(
                    "{}/{}@{}",
                    allocation.job_id, task.name, allocation.id
                ));
            }
        }
        out
    }
}

/// Merge the stored policies that match `facts` with a task's inline block.
fn merge_net_policies(
    stored: &[crate::structs::netpolicy::NetPolicyObject],
    facts: &crate::structs::netpolicy::NetSelectorFacts,
    network: Option<&crate::structs::NetworkConfig>,
) -> (crate::structs::netpolicy::EffectivePolicy, Vec<String>) {
    use crate::structs::netpolicy::{merge_policies, InlinePolicy};
    let matching: Vec<crate::structs::netpolicy::NetPolicyObject> = stored
        .iter()
        .filter(|policy| policy.selector.matches(facts))
        .cloned()
        .collect();
    let inline = InlinePolicy {
        mode: network.map(|n| n.mode.as_str()).filter(|m| *m != "auto"),
        egress: network.and_then(|n| n.egress.as_ref()),
        ingress: network.and_then(|n| n.ingress.as_ref()),
    };
    let effective = merge_policies(&matching, inline);
    let matched = effective
        .sources
        .iter()
        .filter_map(|source| source.strip_prefix("policy/").map(|s| s.to_string()))
        .collect();
    (effective, matched)
}

/// Per-pool scope enforcement for a policy object: a caller must hold the
/// action on every pool the selector names (or on any pool when it names
/// none, i.e. a cluster-wide policy).
/// `certFrom = { kv }` hands a KV value to the datapath, so naming one needs
/// the same right as reading it: `kv:read` on the path. Without this, job
/// submit (or policy write) access would be a way to read any private key in
/// KV. Applied to RunJob, template runs, and UpsertNetPolicy.
fn enforce_kv_cert_read(
    identity: &crate::structs::AclIdentity,
    chains: &[Option<&crate::structs::netpolicy::ChainConfig>],
) -> Result<(), Status> {
    for chain in chains.iter().flatten() {
        if let Some(path) = chain.tls.as_ref().and_then(|tls| tls.cert_kv_path()) {
            acl::enforce(
                identity,
                AclResource::Kv,
                AclAction::Read,
                &AclScope::path(path),
            )
            .map_err(|status| {
                Status::new(
                    status.code(),
                    format!(
                        "{} (ingress tls.certFrom names kv {path}; the submitter needs kv \
                             read on it)",
                        status.message()
                    ),
                )
            })?;
        }
    }
    Ok(())
}

/// `connect.upstreams[].credentials` hands a KV value to the datapath, which
/// sends it to the upstream's origin, so naming one needs the same right as
/// reading it: `kv:read` on the path. Without this, job submit access would
/// be a way to exfiltrate any secret in KV to a host of the submitter's
/// choosing. Applied to RunJob and template runs.
fn enforce_kv_credential_read<'a>(
    identity: &crate::structs::AclIdentity,
    connects: impl Iterator<Item = &'a crate::structs::ConnectConfig>,
) -> Result<(), Status> {
    for connect in connects {
        for upstream in &connect.upstreams {
            for path in upstream.credentials.iter().filter_map(|c| c.kv_path()) {
                acl::enforce(
                    identity,
                    AclResource::Kv,
                    AclAction::Read,
                    &AclScope::path(path),
                )
                .map_err(|status| {
                    Status::new(
                        status.code(),
                        format!(
                            "{} (connect.upstreams[{}].credentials names kv {path}; the \
                             submitter needs kv read on it)",
                            status.message(),
                            upstream.key()
                        ),
                    )
                })?;
            }
        }
    }
    Ok(())
}

fn enforce_net_policy_scope(
    identity: &crate::structs::AclIdentity,
    action: AclAction,
    object: &crate::structs::netpolicy::NetPolicyObject,
) -> Result<(), Status> {
    if object.selector.node_pools.is_empty() {
        return acl::enforce(identity, AclResource::Net, action, &AclScope::none());
    }
    for pool in &object.selector.node_pools {
        acl::enforce(identity, AclResource::Net, action, &AclScope::pool(pool))?;
    }
    Ok(())
}

/// A `NetAllocStatus` as the JSON the CLI and UI read.
/// (caller job, caller task, service, tag).
type EdgeKey = (String, String, String, String);

/// Minutes of per-minute buckets the manager keeps per edge.
const EDGE_WINDOW_MINUTES: i64 = 15;

#[derive(Debug, Default, Clone)]
struct EdgeBucket {
    minute: i64,
    requests: u64,
    errors: u64,
    denied: u64,
    latency_ms_sum: u64,
    latency_ms_max: u64,
    bytes_out: u64,
    bytes_in: u64,
}

#[derive(Debug, Default, Clone)]
struct EdgeWindow {
    protocol: String,
    last_seen: i64,
    buckets: std::collections::VecDeque<EdgeBucket>,
}

fn net_status_json(node_id: &str, status: &crate::structs::NetAllocStatus) -> Value {
    json!({
        "nodeId": node_id,
        "allocationId": status.allocation_id,
        "mode": status.mode,
        "captureReady": status.capture_ready,
        "policyHash": status.policy_hash,
        "upstreamsBound": status.upstreams_bound,
        "ingressBound": status.ingress_bound,
        "resolverReady": status.resolver_ready,
        "proxyReady": status.proxy_ready,
        "gateShut": status.gate_shut,
        "problems": status.problems,
        "handlers": status.handlers.iter().map(|h| json!({
            "source": h.source,
            "handler": h.handler,
            "target": h.target,
            "healthy": h.healthy,
            "lastError": h.last_error,
            "calls": h.calls,
            "errors": h.errors,
        })).collect::<Vec<_>>(),
        "tlsReady": status.tls_ready,
        "tlsError": status.tls_error,
        "meshIdentity": status.mesh_identity,
        "guestAddress": status.guest_address,
        "guestBridge": status.guest_bridge,
    })
}

/// Every TAP sandbox guest address the nodes reported, by allocation id, as
/// `(address, bridge)` (`NetAllocStatus.guest_address`).
fn guest_addresses(
    status: &HashMap<String, Vec<crate::structs::NetAllocStatus>>,
) -> HashMap<i64, (String, String)> {
    status
        .values()
        .flatten()
        .filter(|entry| !entry.guest_address.is_empty())
        .map(|entry| {
            (
                entry.allocation_id,
                (entry.guest_address.clone(), entry.guest_bridge.clone()),
            )
        })
        .collect()
}

/// Fill `info`'s guest address from the node reports, for its running
/// allocation.
/// What `legion start gateway` needs from GetInstance (docs/gateway.md):
/// the task's `suspend.wake` block and where the current allocation serves
/// each declared port. Lookups that fail leave the fields empty, which the
/// gateway reads as "not routable".
fn attach_wake_and_endpoints(store: &Store, info: &mut InstanceInfo) {
    info.wake = store
        .suspend_settings_for(&info.job_id, &info.task)
        .wake
        .map(|wake| crate::structs::InstanceWake {
            port: wake.port,
            timeout_ms: wake.timeout.as_millis() as u64,
        });
    let Some(network) = info
        .current_alloc_id
        .and_then(|id| store.get_allocation_by_id(id).ok().flatten())
        .filter(|allocation| allocation.status != crate::constants::ALLOCATION_STATUS_SUSPENDED)
        .and_then(|allocation| allocation.network)
    else {
        return;
    };
    let address = network.address.clone().unwrap_or_default();
    info.endpoints = network
        .ports
        .iter()
        .map(|port| crate::structs::InstanceEndpoint {
            port: port.name.clone(),
            address: address.clone(),
            number: u32::from(port.host_port),
        })
        .collect();
}

fn attach_guest_address(info: &mut InstanceInfo, addresses: &HashMap<i64, (String, String)>) {
    if let Some((address, bridge)) = info
        .current_alloc_id
        .and_then(|allocation_id| addresses.get(&allocation_id))
    {
        info.guest_address = Some(address.clone());
        info.guest_bridge = Some(bridge.clone());
    }
}

fn net_policy_to_proto(
    object: &crate::structs::netpolicy::NetPolicyObject,
    created_at: &str,
    updated_at: &str,
) -> NetPolicy {
    NetPolicy {
        name: object.name.clone(),
        priority: object.priority,
        enforcement: object.enforcement.clone(),
        object_json: serde_json::to_string(object).unwrap_or_default(),
        description: object.description.clone(),
        created_at: created_at.to_string(),
        updated_at: updated_at.to_string(),
    }
}

/// `host[:port]`, an IP, or a URL → (host, ip, port, path) for `TestNetPolicy`.
fn parse_test_target(
    target: &str,
) -> (
    Option<String>,
    Option<std::net::IpAddr>,
    u16,
    Option<String>,
) {
    if let Ok(parsed) = crate::structs::parse_upstream_target(target) {
        let ip = parsed.host.parse().ok();
        return (Some(parsed.host), ip, parsed.port, None);
    }
    if let Some((scheme, rest)) = target.split_once("://") {
        // A URL with a path: split it off and parse the authority.
        let (authority, path) = match rest.find('/') {
            Some(slash) => (&rest[..slash], Some(rest[slash..].to_string())),
            None => (rest, None),
        };
        if let Ok(parsed) =
            crate::structs::parse_upstream_target(&format!("{scheme}://{authority}"))
        {
            let ip = parsed.host.parse().ok();
            return (Some(parsed.host), ip, parsed.port, path);
        }
    }
    let (host, port) = match target.rsplit_once(':') {
        Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) && !port.is_empty() => {
            (host.to_string(), port.parse().unwrap_or(0))
        }
        _ => (target.to_string(), 0),
    };
    let ip = host.parse().ok();
    (Some(host.to_ascii_lowercase()), ip, port, None)
}

fn build_upstream_endpoints(
    store: &Store,
    desired: &[crate::structs::TaskAllocation],
) -> Vec<crate::structs::UpstreamEndpoints> {
    if desired
        .iter()
        .all(|allocation| allocation.task_connect.trim().is_empty())
    {
        return Vec::new();
    }

    // Keyed by (service, consuming job id) because intentions are per consumer:
    // the same service yields a different list to a permitted and a denied job.
    let mut resolved: HashMap<(String, String), Vec<ServiceEndpoint>> = HashMap::new();
    let mut upstreams = Vec::new();

    for allocation in desired {
        if allocation.task_connect.trim().is_empty() {
            continue;
        }
        // A payload we wrote ourselves failing to parse is a manager bug, and
        // retrying cannot fix it — skip this allocation's upstreams rather than
        // fail the heartbeat and stall reconciliation for the whole node.
        let connect: crate::structs::ConnectConfig =
            match serde_json::from_str(&allocation.task_connect) {
                Ok(connect) => connect,
                Err(err) => {
                    warn!(
                        allocation_id = allocation.allocation_id,
                        error = ?err,
                        "Skipping upstream endpoints; invalid connect payload"
                    );
                    continue;
                }
            };
        for upstream in &connect.upstreams {
            // External upstreams dial a fixed destination; the worker reads it
            // off `AllocUpstream.to` and needs no endpoint feed.
            if upstream.is_external() {
                continue;
            }
            let key = (upstream.service.clone(), allocation.job_id.clone());
            if !resolved.contains_key(&key) {
                let rows = match store.service_endpoints(Some(&upstream.service)) {
                    Ok(rows) => rows,
                    Err(err) => {
                        // Same reasoning as the corrupt-payload arm above:
                        // omitting this upstream leaves the forwarder on its
                        // last known good endpoints, while failing here would
                        // stall this node's reconciliation entirely.
                        warn!(
                            allocation_id = allocation.allocation_id,
                            service = %upstream.service,
                            error = ?err,
                            "Skipping upstream endpoints; service lookup failed"
                        );
                        continue;
                    }
                };
                let endpoints = rows
                    .into_iter()
                    .filter(|row| {
                        row.healthy && crate::structs::allow_permits(&row.allow, &allocation.job_id)
                    })
                    .map(|row| ServiceEndpoint {
                        service: row.service,
                        job_id: row.job_id,
                        task_name: row.task_name,
                        allocation_id: row.allocation_id,
                        node_id: row.node_id,
                        address: row.address.unwrap_or_default(),
                        public_address: row.public_address.unwrap_or_default(),
                        port: u32::from(row.port.unwrap_or(0)),
                        healthy: row.healthy,
                        health_status: row.health_status,
                        tags: row.tags,
                        meta: row.meta.into_iter().collect(),
                        port_label: row.port_label.unwrap_or_default(),
                        mesh_tls: row.mesh_tls,
                    })
                    .collect::<Vec<_>>();
                resolved.insert(key.clone(), endpoints);
            }
            let endpoints = resolved.get(&key).cloned().unwrap_or_default();
            debug!(
                allocation_id = allocation.allocation_id,
                service = %upstream.service,
                endpoints = endpoints.len(),
                "Resolved upstream endpoints"
            );
            upstreams.push(crate::structs::UpstreamEndpoints {
                allocation_id: allocation.allocation_id,
                service: upstream.service.clone(),
                endpoints,
                for_handler: false,
            });
        }
    }

    upstreams
}

/// Apply the volume operation outcomes a worker reported.
///
/// A successful mount pins the volume's node (and corrects the manager's
/// mint-time node-local guess with what the driver actually said); a confirmed
/// destroy is the only thing that advances `destroying -> destroyed`; a failure
/// is recorded on the row so `legion volume list` explains a stuck volume.
/// Failures here are logged, never fatal to the heartbeat — a wedged volume
/// must not stop a node from reporting its allocations.
///
/// All three writes are SCHEDULER-lane replicated ops (`volume` is
/// scheduler-owned, see `storage::raft::ownership`) called from a worker-plane
/// RPC. On a follower they therefore return the typed `NotRaftLeader` error
/// with that lane's leader hint; the warn-and-continue below is deliberately
/// what handles it. Nothing is lost: the worker re-reports on its next
/// heartbeat, and the reclaim reaper re-dispatches `destroying` volumes until
/// a destroy is confirmed, so the state machine converges once the report
/// reaches the lane's leader.
fn apply_volume_reports(store: &Store, node_id: &str, reports: &[crate::structs::VolumeReport]) {
    for report in reports {
        if !report.ok {
            let driver = store
                .get_volume(&report.volume_id)
                .ok()
                .flatten()
                .map(|volume| volume.driver)
                .unwrap_or_else(|| "unknown".to_string());
            warn!(
                node_id,
                volume_id = %report.volume_id,
                op = %report.op,
                error = %report.error,
                "Volume operation failed on worker"
            );
            if let Err(err) = store.record_volume_error(&report.volume_id, &report.error) {
                warn!(error = ?err, "Failed recording volume error");
            }
            store
                .events()
                .volume_failed(&report.volume_id, &driver, node_id, &report.error);
            continue;
        }

        match report.op.as_str() {
            "mount" => {
                let attrs = serde_json::from_str(&report.attrs).unwrap_or_default();
                if let Err(err) = store.record_volume_mounted(
                    &report.volume_id,
                    node_id,
                    &report.mount_path,
                    report.node_local,
                    Some(report.fs.as_str()).filter(|fs| !fs.is_empty()),
                    &attrs,
                ) {
                    warn!(error = ?err, "Failed recording volume mount");
                    continue;
                }
                if let Ok(Some(volume)) = store.get_volume(&report.volume_id) {
                    store
                        .events()
                        .volume_mounted(&volume, node_id, &report.mount_path);
                }
            }
            "destroy" => {
                let driver = store
                    .get_volume(&report.volume_id)
                    .ok()
                    .flatten()
                    .map(|volume| volume.driver)
                    .unwrap_or_else(|| "unknown".to_string());
                if let Err(err) = store.mark_volume_destroyed(&report.volume_id) {
                    warn!(error = ?err, "Failed marking volume destroyed");
                    continue;
                }
                store
                    .events()
                    .volume_destroyed(&report.volume_id, &driver, node_id);
            }
            // `unmount` needs no state change: the volume stays `created` and
            // keeps its pin, which is exactly what makes a reschedule reattach.
            _ => {}
        }
    }
}

/// Volumes this node should reclaim, filtered to plugins it actually has.
///
/// A node-local volume only ever goes to its own node. An unpinned or network
/// volume can go to any node with the plugin, so we check this node advertises
/// it before asking — otherwise a worker would fail the same destroy forever.
fn build_volume_destroy_requests(
    store: &Store,
    node_id: &str,
) -> Vec<crate::structs::VolumeDestroyRequest> {
    let volumes = match store.volumes_to_destroy_for_node(node_id) {
        Ok(volumes) => volumes,
        Err(err) => {
            warn!(node_id, error = ?err, "Failed listing volumes to destroy");
            return Vec::new();
        }
    };
    if volumes.is_empty() {
        return Vec::new();
    }
    let node_drivers = store
        .get_node(node_id)
        .ok()
        .and_then(|node| {
            node.fingerprint_attrs
                .get("volumeDrivers")
                .and_then(Value::as_object)
                .map(|drivers| {
                    drivers
                        .keys()
                        .cloned()
                        .collect::<std::collections::HashSet<_>>()
                })
        })
        .unwrap_or_default();

    volumes
        .into_iter()
        .filter(|volume| node_drivers.contains(&volume.driver))
        .map(|volume| crate::structs::VolumeDestroyRequest {
            volume_id: volume.volume_id,
            driver: volume.driver,
            name: volume.name,
            params: serde_json::to_string(&volume.params).unwrap_or_else(|_| "{}".into()),
            attrs: serde_json::to_string(&volume.attrs).unwrap_or_else(|_| "{}".into()),
        })
        .collect()
}

/// An optional proto string filter, treating empty/whitespace as absent —
/// One volume declaration flattened for the admission check, from either
/// `stateful.storage` or a `volumes[]` claim.
struct VolumeDeclaration {
    label: String,
    driver: String,
    mode: crate::volumes::VolumeMode,
    params: std::collections::BTreeMap<String, Value>,
    is_instance_volume: bool,
}

/// gRPC clients differ on whether an unset filter arrives as None or "".
fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

/// Wire form of a stored volume.
fn volume_info(volume: crate::structs::Volume) -> crate::structs::VolumeInfo {
    crate::structs::VolumeInfo {
        volume_id: volume.volume_id,
        name: volume.name,
        driver: volume.driver,
        job_id: volume.job_id,
        instance_id: volume.instance_id,
        task_name: volume.task_name,
        state: volume.state.to_string(),
        node_id: volume.node_id,
        node_local: volume.node_local,
        volume_mode: volume.volume_mode.to_string(),
        fs: volume.fs,
        read_only: volume.read_only,
        params: serde_json::to_string(&volume.params).unwrap_or_else(|_| "{}".into()),
        attrs: serde_json::to_string(&volume.attrs).unwrap_or_else(|_| "{}".into()),
        mount_path: volume.mount_path,
        last_error: volume.last_error,
        created_at: volume.created_at,
        updated_at: volume.updated_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::config::ManagerConfigFull;
    use crate::structs::worker_server::Worker;
    use crate::structs::{
        AllocationHealth, Eval, GetAllocationLogsRequest, JobConfig, ListRolloutEventsRequest,
        NodeMsg, PostWorkReply, PostWorkRequest, PutKvRequest, RestartWorkerReply,
        RestartWorkerRequest, RunJobRequest, Submission, TaskConfig, UpdateNodeDynamicRequest,
    };
    use futures::StreamExt;
    use serde_json::{json, Map, Value};
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-manager-server-")
            .tempdir()
            .context("error creating temp store directory")?
            .keep();
        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("error parsing store path")?
                .to_string(),
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    /// A raft non-leader rejection maps to FAILED_PRECONDITION with the
    /// leader's name and address in the message (plan §A4, JC-4) — even
    /// when the typed error is buried under pipeline context, as the raft
    /// driver produces it.
    #[test]
    fn map_store_error_keeps_the_cause_of_a_refused_request() {
        // P2-2: a precondition the caller can fix is FAILED_PRECONDITION
        // with the store's own sentence, not a bare "Internal error".
        let err = anyhow::Error::new(StorageError::failed_precondition(
            "Job web must be stopped before it can be destroyed (it is running)",
        ))
        .context("raft op destroy_job");
        let status = map_store_error("Error deleting job", err);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(
            status
                .message()
                .contains("must be stopped before it can be destroyed"),
            "{}",
            status.message()
        );
        assert!(!status.message().contains("foreign key"));

        // An untyped failure is still INTERNAL, but carries its cause.
        let err = anyhow!("disk quota exceeded").context("failed writing plan");
        let status = map_store_error("Error running job", err);
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(
            status.message(),
            "Error running job: failed writing plan: disk quota exceeded"
        );
    }

    /// The submit half of the capability allowlist: a container task whose
    /// effective set no worker type allows is rejected before it places; a
    /// set some type allows, and any drop-only adjustment, passes.
    #[test]
    fn capabilities_no_worker_type_allows_are_rejected_at_submit() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        let job = |security: Value| -> R<crate::structs::JobConfig> {
            Ok(serde_json::from_value(json!({
                "name": "caps",
                "tasks": [{
                    "name": "tool",
                    "runtime": "container",
                    "config": { "image": "alpine", "command": ["true"], "security": security },
                }],
            }))?)
        };
        let admin = job(json!({ "capabilities": { "add": ["SYS_ADMIN"] } }))?;
        let dropper = job(json!({ "capabilities": { "drop": ["ALL"] } }))?;

        // No worker types yet: nothing to judge against.
        manager.validate_job_capabilities(&admin)?;

        let worker_type = |name: &str, container: &str| crate::structs::WorkerConfig {
            name: name.into(),
            pool: "default".into(),
            resources: "{}".into(),
            reserved_resources: "{}".into(),
            container: container.into(),
            ..Default::default()
        };
        store.insert_worker_type(&worker_type("default", ""))?;
        let err = manager
            .validate_job_capabilities(&admin)
            .expect_err("no worker type allows SYS_ADMIN")
            .to_string();
        assert!(err.contains("task tool"), "{err}");
        assert!(err.contains("SYS_ADMIN"), "{err}");
        assert!(err.contains("container.allowCaps"), "{err}");
        assert!(err.contains("worker types: default"), "{err}");
        manager.validate_job_capabilities(&dropper)?;
        manager.validate_job_capabilities(&job(json!({}))?)?;

        // One worker type allows it: admitted (that type's nodes can run it).
        store.insert_worker_type(&worker_type("privileged", r#"{"allowCaps":["ALL"]}"#))?;
        manager.validate_job_capabilities(&admin)?;

        // A task pinned to macOS ignores capabilities, so it is not judged.
        let mut mac = admin.clone();
        store.insert_worker_type(&worker_type("privileged", ""))?;
        assert!(manager.validate_job_capabilities(&mac).is_err());
        mac.tasks[0].constraints = json!({ "os": "macos" });
        manager.validate_job_capabilities(&mac)?;
        Ok(())
    }

    #[tokio::test]
    async fn destroying_a_running_job_is_a_failed_precondition_with_the_reason() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        let config: crate::structs::JobConfig = serde_json::from_value(json!({
            "name": "still-running",
            "tasks": [{ "name": "web", "runtime": "noop" }],
        }))?;
        store.handle_job_submit(
            &config,
            &[crate::structs::Submission {
                path: "job.json".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let status = manager
            .destroy_job(Request::new(crate::structs::DestroyJobRequest {
                name: "still-running".into(),
                ..Default::default()
            }))
            .await
            .expect_err("a running job cannot be destroyed");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(
            status.message().contains("legion job stop still-running"),
            "{}",
            status.message()
        );
        Ok(())
    }

    #[test]
    fn map_store_error_surfaces_raft_leader_hint() {
        let err = anyhow::Error::new(crate::storage::raft::driver::NotRaftLeader {
            lane: None,
            leader_name: Some("mgr-b".to_string()),
            leader_addr: Some("10.0.0.2:4747".to_string()),
        })
        .context("raft op stop_job must run on the leader");
        let status = map_store_error("Error stopping job", err);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(status.message().contains("not the raft leader"));
        assert!(status.message().contains("mgr-b"));
        assert!(status.message().contains("10.0.0.2:4747"));

        // Under `lanes` topology the hint names WHICH lane's leader to
        // contact — different lanes can have different leaders.
        let err = anyhow::Error::new(crate::storage::raft::driver::NotRaftLeader {
            lane: Some("status".to_string()),
            leader_name: Some("mgr-c".to_string()),
            leader_addr: Some("10.0.0.3:4747".to_string()),
        })
        .context("raft op mark_nodes_as_lost must run on the status lane's leader");
        let status = map_store_error("Error marking nodes lost", err);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(status.message().contains("for the status lane"));
        assert!(status.message().contains("mgr-c"));
        assert!(status.message().contains("10.0.0.3:4747"));
    }

    /// Table-test for the pure protocol-compat decision used by
    /// `worker_register` (see docs/upgrades.md for the policy).
    #[test]
    fn protocol_compat_decision_table() {
        use super::VersionCompat::*;
        let cases = [
            // (worker_pv, manager_pv, expected)
            (0, 1, AcceptWithWarn), // pre-versioning worker: rolling upgrade
            (0, 0, AcceptWithWarn), // both pre-versioning (unreachable today)
            (1, 1, Accept),         // exact match
            (1, 2, Accept),         // older worker, newer manager: fine
            (1, u32::MAX, Accept),  // arbitrarily newer manager
            (2, 1, Reject),         // worker upgraded before manager
            (u32::MAX, 1, Reject),  // absurdly new worker
            (1, 0, Reject),         // versioned worker, pre-versioning manager
        ];
        for (worker_pv, manager_pv, expected) in cases {
            assert_eq!(
                protocol_compat(worker_pv, manager_pv),
                expected,
                "protocol_compat({worker_pv}, {manager_pv})"
            );
        }
    }

    #[test]
    fn kv_references_ignore_interpolation_tokens() {
        // Bare `$path/key` strings are KV references…
        assert_eq!(parse_kv_reference("$flags/global"), Some("flags/global"));
        // …but `${…}` interpolation tokens are not, even with slashes.
        assert_eq!(parse_kv_reference("${artifact.tool}/bin/run"), None);
        assert_eq!(parse_kv_reference("${artifact.model}"), None);
        assert_eq!(parse_kv_reference("no-dollar"), None);
        assert_eq!(parse_kv_reference("$nokey"), None);
    }

    fn insert_node(store: &Store, id: &str) -> R<()> {
        let node = NodeMsg {
            id: id.to_string(),
            pool: "default".into(),
            status: "active".into(),
            availability: "available".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: json!({
                "memory_total": "8589934592",
                "cpus": {
                    "cpu-1": { "frequency": "2400" },
                    "cpu-2": { "frequency": "2400" },
                }
            })
            .to_string(),
            dynamic_attrs: "{}".into(),
            health_checks: "{}".into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
            heartbeat_source: String::new(),
        };
        store.insert_node(node)?;
        Ok(())
    }

    fn submission() -> Submission {
        Submission {
            path: "job.ncl".into(),
            content: "{}".into(),
            is_primary: true,
        }
    }

    // --- IssueWorkloadIdentityToken ---

    fn wi_state(store: &Store) -> R<Arc<WorkloadIdentityState>> {
        let config = crate::structs::WorkloadIdentityConfigFull {
            enabled: true,
            issuer: Some("https://manager.test:4747".into()),
            ..Default::default()
        };
        Ok(Arc::new(WorkloadIdentityState::initialize(
            store,
            &config,
            "http://127.0.0.1:4747",
            std::time::SystemTime::now(),
        )?))
    }

    fn aws_identity_task(task_name: &str) -> TaskConfig {
        use crate::structs::workload_identity::{WorkloadIdentity, WorkloadIdentityFile};
        TaskConfig {
            identities: vec![WorkloadIdentity {
                name: "aws".into(),
                audience: vec!["sts.amazonaws.com".into()],
                file: Some(WorkloadIdentityFile {
                    path: "/var/run/legion/identity/aws/token".into(),
                    mode: Some("0400".into()),
                }),
                ..Default::default()
            }],
            ..TaskConfig::new(task_name, "noop")
        }
    }

    /// Submit a single-task job and place its allocation on `node-1`.
    fn submit_and_place_identity_task(
        store: &Store,
        job_name: &str,
        task: TaskConfig,
    ) -> R<crate::structs::Allocation> {
        insert_node(store, "node-1")?;
        let job = JobConfig {
            priority: Default::default(),
            name: job_name.into(),
            tasks: vec![task],
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(job_name)?.id();
        let (task_id, _) = store
            .get_task_configs_for_job_config(&job_config_id)?
            .into_iter()
            .next()
            .context("missing task config")?;
        store.place_alloc("node-1", task_id)
    }

    fn issue_request(
        alloc_id: i64,
        identity_name: &str,
        node_id: &str,
    ) -> Request<IssueWorkloadIdentityTokenRequest> {
        Request::new(IssueWorkloadIdentityTokenRequest {
            alloc_id,
            identity_name: identity_name.into(),
            node_id: node_id.into(),
        })
    }

    fn submit_plain_job(store: &Store, job_name: &str) -> R<()> {
        let job = JobConfig {
            priority: Default::default(),
            name: job_name.into(),
            tasks: vec![TaskConfig::new(format!("{job_name}-task"), "service")],
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        Ok(())
    }

    /// Gated plan §6.7: the node's report of a TAP guest's leased address
    /// reaches the instance serving that allocation, and no other.
    #[test]
    fn a_reported_guest_address_reaches_its_instance() {
        let report = |allocation_id, address: &str| crate::structs::NetAllocStatus {
            allocation_id,
            mode: "host".into(),
            guest_address: address.into(),
            guest_bridge: if address.is_empty() { "" } else { "br0" }.into(),
            ..Default::default()
        };
        let status = HashMap::from([(
            "w-03".to_string(),
            vec![report(7, "10.88.128.7"), report(8, "")],
        )]);
        let addresses = guest_addresses(&status);
        let mut running = InstanceInfo {
            current_alloc_id: Some(7),
            ..Default::default()
        };
        attach_guest_address(&mut running, &addresses);
        assert_eq!(running.guest_address.as_deref(), Some("10.88.128.7"));
        assert_eq!(running.guest_bridge.as_deref(), Some("br0"));
        for current_alloc_id in [Some(8), Some(9), None] {
            let mut other = InstanceInfo {
                current_alloc_id,
                ..Default::default()
            };
            attach_guest_address(&mut other, &addresses);
            assert_eq!(other.guest_address, None);
        }
    }

    /// Gated plan Phase 2: an allocation placed gated by `auto` (a sandbox
    /// that declares no `network` block) is told about its empty policy,
    /// so its node denies with `gated-default` instead of waiting forever
    /// on `policy-pending`, even on a cluster with no policy objects.
    #[test]
    fn a_placement_gated_allocation_gets_its_empty_policy() -> R<()> {
        let manager = ManagerDaemonService::new(setup_store()?);
        let placed = |allocation_id: i64, mode: &str| crate::structs::TaskAllocation {
            allocation_id,
            task_runtime: "sandbox".into(),
            network: json!({ "mode": mode }).to_string(),
            ..Default::default()
        };
        let assignments =
            manager.build_net_policies("node-1", &[placed(7, "gated"), placed(8, "host")]);
        assert_eq!(assignments.len(), 1, "{assignments:?}");
        assert_eq!(assignments[0].allocation_id, 7);
        assert!(!assignments[0].policy_hash.is_empty());
        Ok(())
    }

    /// Gated plan §6.5: `net policy test` answers like the datapath. A gated
    /// task that no egress chain covers is denied with `gated-default` and
    /// the fix; a host task is allowed; and with the Phase 2 gate open, a
    /// sandbox that declares nothing is gated by default too.
    #[tokio::test]
    async fn net_policy_test_reports_the_gated_default() -> R<()> {
        let store = setup_store()?;
        let job = |name: &str, runtime: &str, network: Option<Value>| -> R<JobConfig> {
            let mut task = serde_json::json!({
                "name": "workspace",
                "runtime": runtime,
                "config": { "image": "alpine", "command": ["sleep", "1"] },
            });
            if let Some(network) = network {
                task["network"] = network;
            }
            Ok(JobConfig {
                priority: Default::default(),
                name: name.into(),
                tasks: vec![serde_json::from_value(task)?],
                rollout: None,
                rollout_by_group: Default::default(),
                version: None,
            })
        };
        store.handle_job_submit(
            &job("gated", "container", Some(json!({ "mode": "gated" })))?,
            &[submission()],
        )?;
        store.handle_job_submit(&job("plain", "container", None)?, &[submission()])?;
        store.handle_job_submit(&job("agents", "sandbox", None)?, &[submission()])?;
        let manager = ManagerDaemonService::new(store.clone());
        let test = |job_id: &'static str| {
            let manager = &manager;
            async move {
                manager
                    .test_net_policy(Request::new(TestNetPolicyRequest {
                        job_id: job_id.into(),
                        to: "https://example.com".into(),
                        ..Default::default()
                    }))
                    .await
                    .map(|reply| reply.into_inner())
            }
        };

        let reply = test("gated").await?;
        assert_eq!(reply.verdict, "deny");
        assert_eq!(reply.source, "gated-default");
        assert_eq!(
            reply.reason,
            "task workspace is gated and nothing allows example.com"
        );
        assert!(
            reply
                .hint
                .contains("add \"example.com\" to network.egress.allow"),
            "{}",
            reply.hint
        );
        assert_eq!(test("plain").await?.verdict, "allow");

        // The default sandbox: host until Phase 0 opens the gate.
        assert_eq!(test("agents").await?.verdict, "allow");
        store.set_sandbox_gated_default(true);
        assert_eq!(test("agents").await?.source, "gated-default");
        Ok(())
    }

    /// `ListJobs.status` narrows the page to one status (case-insensitively)
    /// while `status_counts` keeps counting the whole query-matched set, so a
    /// filter control can show "stopped (1)" while "running" is selected.
    #[tokio::test]
    async fn list_jobs_filters_by_status_and_reports_counts() -> R<()> {
        let store = setup_store()?;
        submit_plain_job(&store, "list-a")?;
        submit_plain_job(&store, "list-b")?;
        submit_plain_job(&store, "other-c")?;
        store.stop_job("list-b")?;
        let manager = ManagerDaemonService::new(store);

        let all = manager
            .list_jobs(Request::new(ListJobsRequest::default()))
            .await?
            .into_inner();
        assert_eq!(all.total_count, 3);
        assert_eq!(all.status_counts.get("stopped"), Some(&1));
        // The status is derived from the workload: nothing is placed, so the
        // two live jobs read `pending`, not the stored lifecycle `active`.
        assert_eq!(all.status_counts.get("pending"), Some(&2));
        assert_eq!(all.status_counts.values().sum::<u32>(), 3);

        let stopped = manager
            .list_jobs(Request::new(ListJobsRequest {
                status: Some("STOPPED".into()),
                ..Default::default()
            }))
            .await?
            .into_inner();
        assert_eq!(stopped.total_count, 1);
        assert_eq!(stopped.jobs.len(), 1);
        assert_eq!(stopped.jobs[0].name, "list-b");
        // The counts are unchanged by the status filter itself …
        assert_eq!(stopped.status_counts, all.status_counts);

        // … but they do follow the free-text query.
        let queried = manager
            .list_jobs(Request::new(ListJobsRequest {
                query: Some("list-".into()),
                status: Some("stopped".into()),
                ..Default::default()
            }))
            .await?
            .into_inner();
        assert_eq!(queried.total_count, 1);
        assert_eq!(queried.status_counts.values().sum::<u32>(), 2);

        // An exact match: a prefix never matches a status.
        let prefix = manager
            .list_jobs(Request::new(ListJobsRequest {
                status: Some("stop".into()),
                ..Default::default()
            }))
            .await?
            .into_inner();
        assert_eq!(prefix.total_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn issue_token_derives_claims_from_the_allocation_record() -> R<()> {
        let store = setup_store()?;
        let allocation =
            submit_and_place_identity_task(&store, "wi-job", aws_identity_task("api"))?;
        let state = wi_state(&store)?;
        let manager = ManagerDaemonService::new(store).with_workload_identity(Some(state.clone()));

        let reply = manager
            .issue_workload_identity_token(issue_request(allocation.id, "aws", "node-1"))
            .await?
            .into_inner();
        assert_eq!(reply.name, "aws");
        assert!(reply.expires_at > 0);

        // The token verifies against the issuer and carries claims derived
        // from the scheduling record — not from anything the caller sent.
        let claims = state.verify(&reply.token, "sts.amazonaws.com")?;
        assert_eq!(claims["sub"], json!("auto:wi-job:default:api"));
        assert_eq!(claims["aud"], json!(["sts.amazonaws.com"]));
        assert_eq!(claims["legion.io/job"], json!("wi-job"));
        assert_eq!(claims["legion.io/task"], json!("api"));
        assert_eq!(claims["legion.io/node"], json!("node-1"));
        assert_eq!(claims["legion.io/alloc"], json!(allocation.id.to_string()));
        assert_eq!(claims["exp"], json!(reply.expires_at));
        assert!(claims.get("legion.io/instance").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn issue_token_for_stateful_allocation_carries_instance_claims() -> R<()> {
        use crate::structs::StatefulConfig;
        let store = setup_store()?;
        let task = TaskConfig {
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
                envelope: None,
                promote: None,
            }),
            ..aws_identity_task("db")
        };
        let allocation = submit_and_place_identity_task(&store, "wi-stateful", task)?;
        assert!(allocation.instance_id.is_some());
        let state = wi_state(&store)?;
        let manager = ManagerDaemonService::new(store).with_workload_identity(Some(state.clone()));

        let reply = manager
            .issue_workload_identity_token(issue_request(allocation.id, "aws", "node-1"))
            .await?
            .into_inner();
        let claims = state.verify(&reply.token, "sts.amazonaws.com")?;
        assert_eq!(claims["legion.io/instance"], json!("wi-stateful-default-0"));
        assert_eq!(claims["legion.io/slot"], json!("0"));
        Ok(())
    }

    #[tokio::test]
    async fn issue_token_rejects_foreign_node_requests() -> R<()> {
        let store = setup_store()?;
        let allocation =
            submit_and_place_identity_task(&store, "wi-job", aws_identity_task("api"))?;
        let state = wi_state(&store)?;
        let manager = ManagerDaemonService::new(store).with_workload_identity(Some(state));

        let err = manager
            .issue_workload_identity_token(issue_request(allocation.id, "aws", "node-evil"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        Ok(())
    }

    #[tokio::test]
    async fn issue_token_binds_node_id_to_authenticated_worker() -> R<()> {
        use crate::manager::acl::Authenticator;
        use crate::structs::AclConfigFull;

        let store = setup_store()?;
        insert_node(&store, "node-2")?;
        // Allocation is placed on node-1 by submit_and_place_identity_task.
        let allocation =
            submit_and_place_identity_task(&store, "wi-job", aws_identity_task("api"))?;
        let state = wi_state(&store)?;

        // ACLs on: the caller's worker token principal is `node:<id>`.
        let auth = Arc::new(Authenticator::from_config(&AclConfigFull {
            enabled: true,
            ..Default::default()
        })?);
        let node1_token = auth.mint_node_token(&store, "node-1")?;
        let node2_token = auth.mint_node_token(&store, "node-2")?;
        let manager = ManagerDaemonService::new(store)
            .with_workload_identity(Some(state))
            .with_authenticator(auth);

        let authed = |token: &str, node_id: &str| {
            let mut request = issue_request(allocation.id, "aws", node_id);
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {token}").parse().expect("ascii token"),
            );
            request
        };

        // node-1's own token issues for its own allocation.
        let reply = manager
            .issue_workload_identity_token(authed(&node1_token, "node-1"))
            .await?
            .into_inner();
        assert_eq!(reply.name, "aws");

        // node-2's token forging node-1 in the request is denied: the request
        // node_id must match the authenticated principal, not just the
        // allocation's node.
        let err = manager
            .issue_workload_identity_token(authed(&node2_token, "node-1"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);

        // And node-2 asking for node-2 (matching principal) is still rejected
        // because the allocation is not on node-2 — defense in depth.
        let err = manager
            .issue_workload_identity_token(authed(&node2_token, "node-2"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        Ok(())
    }

    /// A workload-identity state that also declares a `pull` node identity.
    fn wi_state_with_node_identity(store: &Store) -> R<Arc<WorkloadIdentityState>> {
        use crate::structs::workload_identity::NodeIdentityConfig;
        let config = crate::structs::WorkloadIdentityConfigFull {
            enabled: true,
            issuer: Some("https://manager.test:4747".into()),
            node_identities: vec![NodeIdentityConfig {
                name: "pull".into(),
                audience: vec!["sts.amazonaws.com".into()],
                ttl: Some("15m".into()),
                sub_template: None,
            }],
            ..Default::default()
        };
        Ok(Arc::new(WorkloadIdentityState::initialize(
            store,
            &config,
            "http://127.0.0.1:4747",
            std::time::SystemTime::now(),
        )?))
    }

    #[tokio::test]
    async fn issue_node_token_derives_claims_from_node_record() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-1")?;
        let state = wi_state_with_node_identity(&store)?;
        let manager = ManagerDaemonService::new(store).with_workload_identity(Some(state.clone()));

        let reply = manager
            .issue_node_identity_token(Request::new(IssueNodeIdentityTokenRequest {
                node_id: "node-1".into(),
                identity_name: "pull".into(),
            }))
            .await?
            .into_inner();
        assert_eq!(reply.name, "pull");

        // Verifies against the issuer; claims are node-scoped and derived from
        // the node record (pool) — never from the request.
        let claims = state.verify(&reply.token, "sts.amazonaws.com")?;
        assert_eq!(claims["sub"], json!("auto:node:default"));
        assert_eq!(claims["legion.io/scope"], json!("node"));
        assert_eq!(claims["legion.io/pool"], json!("default"));
        assert_eq!(claims["legion.io/node"], json!("node-1"));
        assert!(claims.get("legion.io/alloc").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn issue_node_token_rejects_unknown_identity_and_node() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-1")?;
        let state = wi_state_with_node_identity(&store)?;
        let manager = ManagerDaemonService::new(store).with_workload_identity(Some(state));

        let err = manager
            .issue_node_identity_token(Request::new(IssueNodeIdentityTokenRequest {
                node_id: "node-1".into(),
                identity_name: "bogus".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);

        let err = manager
            .issue_node_identity_token(Request::new(IssueNodeIdentityTokenRequest {
                node_id: "ghost".into(),
                identity_name: "pull".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        Ok(())
    }

    #[tokio::test]
    async fn issue_node_token_binds_node_id_to_authenticated_worker() -> R<()> {
        use crate::manager::acl::Authenticator;
        use crate::structs::AclConfigFull;

        let store = setup_store()?;
        insert_node(&store, "node-1")?;
        insert_node(&store, "node-2")?;
        let state = wi_state_with_node_identity(&store)?;
        let auth = Arc::new(Authenticator::from_config(&AclConfigFull {
            enabled: true,
            ..Default::default()
        })?);
        let node2_token = auth.mint_node_token(&store, "node-2")?;
        let manager = ManagerDaemonService::new(store)
            .with_workload_identity(Some(state))
            .with_authenticator(auth);

        // node-2's credential may not mint node-1's token.
        let mut request = Request::new(IssueNodeIdentityTokenRequest {
            node_id: "node-1".into(),
            identity_name: "pull".into(),
        });
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {node2_token}")
                .parse()
                .expect("ascii token"),
        );
        let err = manager
            .issue_node_identity_token(request)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        Ok(())
    }

    #[tokio::test]
    async fn issue_token_rejects_unknown_identity_and_unknown_alloc() -> R<()> {
        let store = setup_store()?;
        let allocation =
            submit_and_place_identity_task(&store, "wi-job", aws_identity_task("api"))?;
        let state = wi_state(&store)?;
        let manager = ManagerDaemonService::new(store).with_workload_identity(Some(state));

        let err = manager
            .issue_workload_identity_token(issue_request(allocation.id, "bogus", "node-1"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);

        let err = manager
            .issue_workload_identity_token(issue_request(999_999, "aws", "node-1"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        Ok(())
    }

    #[tokio::test]
    async fn issue_token_requires_workload_identity_enabled() -> R<()> {
        let store = setup_store()?;
        let allocation =
            submit_and_place_identity_task(&store, "wi-job", aws_identity_task("api"))?;
        // No workload-identity state attached == feature disabled.
        let manager = ManagerDaemonService::new(store);

        let err = manager
            .issue_workload_identity_token(issue_request(allocation.id, "aws", "node-1"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        Ok(())
    }

    // --- ReplaceInstance / node-loss diagnostics ---

    /// Submit a single stateful slot task (count 1) and place it on `node-1`.
    fn submit_and_place_stateful_task(
        store: &Store,
        job_name: &str,
    ) -> R<crate::structs::Allocation> {
        insert_node(store, "node-1")?;
        insert_node(store, "node-2")?;
        let task = TaskConfig {
            stateful: Some(crate::structs::StatefulConfig {
                mode: crate::structs::InstanceMode::Slot,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    driver: None,
                    mode: None,
                    params: None,
                    path: "/var/lib/db".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                }),
                envelope: None,
                promote: None,
            }),
            ..TaskConfig::new("db", "noop")
        };
        let job = JobConfig {
            priority: Default::default(),
            name: job_name.into(),
            tasks: vec![task],
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(job_name)?.id();
        let (task_id, _) = store
            .get_task_configs_for_job_config(&job_config_id)?
            .into_iter()
            .next()
            .context("missing task config")?;
        store.place_alloc("node-1", task_id)
    }

    #[tokio::test]
    async fn replace_instance_frees_gated_allocation_and_enqueues_reschedule() -> R<()> {
        let store = setup_store()?;
        let allocation = submit_and_place_stateful_task(&store, "fence-job")?;
        let instance_id = allocation.instance_id.clone().context("instance bound")?;
        store.mark_nodes_as_lost(&["node-1".to_string()])?;
        // Pump the C-2 eval-mediated reschedule tail so the gating below is
        // exercised by the real lost-node pass, not by an unprocessed queue.
        while let Some(eval) = store.claim_eval()? {
            eval.handle(&store)?;
            store.complete_eval(&eval.id, true)?;
        }
        // Gated: still on the lost node.
        assert_eq!(store.allocs_for_node("node-1")?.len(), 1);

        let manager = ManagerDaemonService::new(store.clone());
        let reply = manager
            .replace_instance(tonic::Request::new(ReplaceInstanceRequest {
                instance_id: instance_id.clone(),
                idempotency_key: None,
            }))
            .await?
            .into_inner();
        assert!(
            reply.message.contains("freed allocation"),
            "{}",
            reply.message
        );

        // The gated allocation is gone; the instance awaits its replacement
        // with id and storage handle intact; a reschedule eval excluding the
        // lost node is queued.
        assert!(store.allocs_for_node("node-1")?.is_empty());
        let instance = store.get_instance(&instance_id)?.context("instance")?;
        assert_eq!(instance.state, crate::structs::InstanceState::Rescheduling);
        assert!(instance.storage_handle.is_some());
        let evals = store.get_evals()?;
        let reschedule = evals
            .iter()
            .find(|eval| eval.kind == "job_reschedule")
            .context("reschedule eval queued")?;
        assert_eq!(reschedule.node_name, "node-1");
        Ok(())
    }

    /// A worker restart re-registers the node; that must not undo an
    /// operator's drain (S6 in the 2026-10-09 architecture audit).
    #[tokio::test]
    async fn re_registration_keeps_node_availability() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        let register = || {
            tonic::Request::new(WorkerRegisterRequest {
                name: "drained-node".into(),
                pool: "default".into(),
                config_attrs: "{}".into(),
                fingerprint_attrs: "{}".into(),
                health_checks: "[]".into(),
                ..Default::default()
            })
        };
        manager.worker_register(register()).await?;
        assert_eq!(store.get_node("drained-node")?.availability, "available");

        store.set_node_availability("drained-node", "drained")?;
        manager.worker_register(register()).await?;
        assert_eq!(store.get_node("drained-node")?.availability, "drained");
        Ok(())
    }

    /// A worker that re-registers after its node was marked LOST must come
    /// back schedulable (spot-check P0-6). `unavailable` is what loss sets,
    /// not operator intent, so keeping it registered an `active` node that
    /// every placement then skipped ("No nodes available").
    #[tokio::test]
    async fn re_registration_after_loss_makes_the_node_schedulable() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        let register = || {
            tonic::Request::new(WorkerRegisterRequest {
                name: "lost-node".into(),
                pool: "default".into(),
                config_attrs: "{}".into(),
                fingerprint_attrs: "{}".into(),
                health_checks: "[]".into(),
                ..Default::default()
            })
        };
        manager.worker_register(register()).await?;
        store.mark_nodes_as_lost(&["lost-node".to_string()])?;
        assert_eq!(store.get_node("lost-node")?.availability, "unavailable");

        manager.worker_register(register()).await?;
        let node = store.get_node("lost-node")?;
        assert_eq!(node.status, "active");
        assert_eq!(node.availability, "available");
        assert!(store.available_node_ids()?.contains("lost-node"));
        Ok(())
    }

    #[tokio::test]
    async fn replace_instance_rejects_allocations_on_healthy_nodes() -> R<()> {
        let store = setup_store()?;
        let allocation = submit_and_place_stateful_task(&store, "fence-healthy")?;
        let instance_id = allocation.instance_id.clone().context("instance bound")?;

        let manager = ManagerDaemonService::new(store.clone());
        let err = manager
            .replace_instance(tonic::Request::new(ReplaceInstanceRequest {
                instance_id,
                idempotency_key: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(
            err.message().contains("not a lost node"),
            "{}",
            err.message()
        );
        // Nothing happened.
        assert_eq!(store.allocs_for_node("node-1")?.len(), 1);

        let missing = manager
            .replace_instance(tonic::Request::new(ReplaceInstanceRequest {
                instance_id: "no-such-instance".into(),
                idempotency_key: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::NotFound);
        Ok(())
    }

    #[tokio::test]
    async fn instance_get_surfaces_pending_loss_confirmation_detail() -> R<()> {
        let store = setup_store()?;
        let allocation = submit_and_place_stateful_task(&store, "fence-detail")?;
        let instance_id = allocation.instance_id.clone().context("instance bound")?;
        let manager = ManagerDaemonService::new(store.clone());

        // Healthy node: no detail.
        let reply = manager
            .get_instance(tonic::Request::new(GetInstanceRequest {
                instance_id: instance_id.clone(),
            }))
            .await?
            .into_inner();
        assert_eq!(reply.instance.and_then(|info| info.detail), None);

        // Lost node, gated allocation: the diagnostic explains the wait.
        store.mark_nodes_as_lost(&["node-1".to_string()])?;
        // Pump the C-2 eval-mediated reschedule tail so the gating below is
        // exercised by the real lost-node pass, not by an unprocessed queue.
        while let Some(eval) = store.claim_eval()? {
            eval.handle(&store)?;
            store.complete_eval(&eval.id, true)?;
        }
        let reply = manager
            .get_instance(tonic::Request::new(GetInstanceRequest {
                instance_id: instance_id.clone(),
            }))
            .await?
            .into_inner();
        let detail = reply
            .instance
            .and_then(|info| info.detail)
            .context("detail set while gated")?;
        assert!(
            detail.starts_with("pending-loss-confirmation:")
                && detail.contains("node-1")
                && detail.contains("replaceAfter"),
            "{detail}"
        );

        // list_instances carries the same diagnostic.
        let listed = manager
            .list_instances(tonic::Request::new(ListInstancesRequest {
                job_id: "fence-detail".into(),
                group: None,
                ..Default::default()
            }))
            .await?
            .into_inner();
        assert!(listed.instances[0]
            .detail
            .as_deref()
            .is_some_and(|detail| detail.starts_with("pending-loss-confirmation:")));
        Ok(())
    }

    #[test]
    fn rollout_launch_config_defaults_to_default_strategy() -> R<()> {
        let config = JobConfig {
            priority: Default::default(),
            name: "rollout-defaults".into(),
            tasks: vec![TaskConfig::new("api", "noop")],
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
        };
        let launch = build_rollout_launch_config(&config)?;
        assert_eq!(launch.strategy.as_deref(), Some("default"));
        assert!(launch.progression.is_none());
        assert!(launch.replacement.is_none());
        assert_eq!(launch.in_place, None);
        assert!(!launch.is_manual);
        assert!(launch.config_json.contains("\"rolloutByGroup\":{}"));
        Ok(())
    }

    #[test]
    fn rollout_launch_config_serializes_manual_create_first() -> R<()> {
        let config = JobConfig {
            priority: Default::default(),
            name: "rollout-manual".into(),
            tasks: vec![TaskConfig::new("api", "noop")],
            rollout: Some(crate::structs::RolloutConfig {
                progression: Some(RolloutProgression::Manual),
                replacement: Some(RolloutReplacement::CreateFirst),
                in_place: Some(true),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            version: None,
        };
        let launch = build_rollout_launch_config(&config)?;
        assert_eq!(launch.progression.as_deref(), Some("manual"));
        assert_eq!(launch.replacement.as_deref(), Some("createFirst"));
        assert_eq!(launch.in_place, Some(true));
        assert!(launch.is_manual);
        Ok(())
    }

    #[derive(Debug, Clone)]
    struct TestWorker {
        called: Arc<Mutex<Option<String>>>,
    }

    #[tonic::async_trait]
    impl Worker for TestWorker {
        type StartClaimStream = std::pin::Pin<
            Box<
                dyn tokio_stream::Stream<Item = Result<crate::structs::StartClaimReply, Status>>
                    + Send
                    + 'static,
            >,
        >;

        async fn start_claim(
            &self,
            _request: Request<crate::structs::StartClaimRequest>,
        ) -> Result<Response<Self::StartClaimStream>, Status> {
            Err(Status::unimplemented("test worker does not accept claims"))
        }

        async fn post_work(
            &self,
            _request: Request<PostWorkRequest>,
        ) -> Result<Response<PostWorkReply>, Status> {
            Err(Status::unimplemented("not used in test"))
        }

        async fn stop_sessions(
            &self,
            _request: Request<crate::structs::StopSessionsRequest>,
        ) -> Result<Response<crate::structs::StopSessionsReply>, Status> {
            Err(Status::unimplemented("not used in test"))
        }

        async fn get_session(
            &self,
            _request: Request<crate::structs::GetSessionRequest>,
        ) -> Result<Response<crate::structs::GetSessionReply>, Status> {
            Err(Status::unimplemented("not used in test"))
        }

        async fn start_session(
            &self,
            _request: Request<crate::structs::StartSessionRequest>,
        ) -> Result<Response<crate::structs::StartSessionReply>, Status> {
            Err(Status::unimplemented("not used in test"))
        }

        async fn stop_session(
            &self,
            _request: Request<crate::structs::StopSessionRequest>,
        ) -> Result<Response<crate::structs::StopSessionReply>, Status> {
            Err(Status::unimplemented("not used in test"))
        }

        async fn restart_worker(
            &self,
            request: Request<RestartWorkerRequest>,
        ) -> Result<Response<RestartWorkerReply>, Status> {
            let node_id = request.into_inner().node_id;
            let mut guard = self.called.lock().expect("mutex");
            *guard = Some(node_id);
            Ok(Response::new(RestartWorkerReply {
                message: "ok".into(),
            }))
        }

        async fn probe(
            &self,
            _request: Request<crate::structs::ProbeRequest>,
        ) -> Result<Response<crate::structs::ProbeReply>, Status> {
            Ok(Response::new(crate::structs::ProbeReply {
                node_id: "node-1".into(),
                protocol_version: u64::from(crate::constants::PROTOCOL_VERSION),
            }))
        }

        async fn get_task_logs(
            &self,
            request: Request<GetTaskLogsRequest>,
        ) -> Result<Response<crate::structs::GetTaskLogsReply>, Status> {
            let allocation_id = request.into_inner().allocation_id;
            Ok(Response::new(crate::structs::GetTaskLogsReply {
                streams: vec![crate::structs::TaskLogStream {
                    stream: "stdout".into(),
                    content: format!("logs for {allocation_id}"),
                    size_bytes: 0,
                    truncated: false,
                    start_offset: 0,
                    next_offset: 0,
                }],
            }))
        }

        type ExecTaskStream =
            tokio_stream::wrappers::ReceiverStream<Result<crate::structs::ExecServerFrame, Status>>;

        async fn exec_task(
            &self,
            _request: Request<Streaming<ExecClientFrame>>,
        ) -> Result<Response<Self::ExecTaskStream>, Status> {
            Err(Status::unimplemented("not used in test"))
        }
    }

    /// The restart is proxied over the mTLS worker plane: the manager dials
    /// with its own certificate and verifies the node's name. A listener
    /// serving another node's certificate at the registered address is
    /// refused with an actionable message instead of receiving the call.
    #[tokio::test]
    async fn manager_forwards_worker_restart() -> R<()> {
        let store = setup_store()?;
        // The worker plane is mTLS: serve the test worker with node-1's
        // certificate from this cluster's CA, and require manager
        // certificates from the same CA.
        let ca = store.ensure_mesh_ca()?;
        let (key_pem, csr) = crate::manager::mesh_ca::generate_csr("node-1")?;
        let (cert_pem, _) = crate::manager::mesh_ca::sign_csr(
            &ca.cert_pem,
            &ca.key_pem,
            &csr,
            &crate::manager::mesh_ca::CsrSubject::Node("node-1".into()),
        )?;
        let identity = crate::cluster_tls::SwappableIdentity::new();
        identity.set(crate::cluster_tls::certified_key(&cert_pem, &key_pem)?);
        let roots = crate::cluster_tls::SwappableRoots::from_pem(&ca.bundle_pem())?;
        let tls = crate::cluster_tls::server_config(
            identity,
            Some(crate::cluster_tls::ClusterClientVerifier::new(roots)),
            false,
        )?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let worker_address = format!("https://{}", listener.local_addr()?);

        let called = Arc::new(Mutex::new(None));
        let worker = TestWorker {
            called: called.clone(),
        };

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(crate::structs::worker_server::WorkerServer::new(worker))
                .serve_with_incoming_shutdown(
                    crate::cluster_tls::tls_incoming(listener, tls),
                    async {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
                .ok();
        });

        let node = Node {
            id: "node-1".into(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({ "workerAddress": worker_address }),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({ "protocolVersion": crate::constants::PROTOCOL_VERSION }),
            health_checks: json!({}),
        };
        store.insert_node(NodeMsg::from(node))?;
        let manager = ManagerDaemonService::new(store.clone());

        let response = manager
            .restart_worker(Request::new(RestartWorkerRequest {
                node_id: "node-1".into(),
            }))
            .await?;
        assert_eq!(response.into_inner().message, "ok");
        assert_eq!(
            called.lock().expect("mutex").as_deref(),
            Some("node-1"),
            "the worker saw the restart"
        );

        // A pre-mTLS worker (protocol 2, plaintext address) is never dialed.
        let old = Node {
            id: "node-old".into(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({ "workerAddress": "http://127.0.0.1:1" }),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({ "protocolVersion": 2 }),
            health_checks: json!({}),
        };
        store.insert_node(NodeMsg::from(old))?;
        let status = manager
            .restart_worker(Request::new(RestartWorkerRequest {
                node_id: "node-old".into(),
            }))
            .await
            .expect_err("a protocol-2 worker is refused");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(
            status.message().contains("protocol 2"),
            "{}",
            status.message()
        );

        let _ = shutdown_tx.send(());
        Ok(())
    }

    #[tokio::test]
    async fn get_allocation_logs_requires_a_target() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store);

        let status = manager
            .get_allocation_logs(Request::new(GetAllocationLogsRequest {
                job: String::new(),
                allocation_id: None,
                stream: "all".into(),
                max_bytes: 0,
                tail: false,
                offset: 0,
                include_removed: false,
            }))
            .await
            .expect_err("missing target should error");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        Ok(())
    }

    /// P1-9: once an allocation is removed, `legion job logs` still finds
    /// it (job, task, node from the allocation history) and asks its node.
    /// Here the node has no worker address, so the entry carries an error
    /// rather than streams — but it is there, marked removed, with its cause,
    /// instead of "No allocations found". A destroyed job's logs too.
    #[tokio::test]
    async fn get_allocation_logs_reaches_removed_allocations() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        let alloc =
            submit_and_place_identity_task(&store, "gone", TaskConfig::new("fetch", "noop"))?;
        store.remove_allocs_for_job("gone")?;
        assert!(store.allocs_for_job("gone")?.is_empty());

        let request = |job: &str, allocation_id: Option<i64>| {
            Request::new(GetAllocationLogsRequest {
                job: job.into(),
                allocation_id,
                stream: "all".into(),
                ..Default::default()
            })
        };
        let reply = manager
            .get_allocation_logs(request("gone", None))
            .await?
            .into_inner();
        assert_eq!(reply.allocations.len(), 1, "{reply:?}");
        let entry = &reply.allocations[0];
        assert_eq!(entry.allocation_id, alloc.id);
        assert!(entry.removed);
        assert_eq!(entry.removal_cause, "stop");
        assert_eq!(entry.task_name, "fetch");
        assert_eq!(entry.node_id, "node-1");
        assert!(entry.removed_at_unix > 0);
        assert!(!entry.error.is_empty(), "node-1 has no worker address");

        // By id.
        let reply = manager
            .get_allocation_logs(request("", Some(alloc.id)))
            .await?
            .into_inner();
        assert_eq!(reply.allocations.len(), 1);
        assert!(reply.allocations[0].removed);

        // An id that never existed is still NotFound.
        let status = manager
            .get_allocation_logs(request("", Some(alloc.id + 1000)))
            .await
            .expect_err("unknown allocation");
        assert_eq!(status.code(), tonic::Code::NotFound);

        // The job is destroyed; its removed allocations stay readable.
        store.stop_job("gone")?;
        store.destroy_job("gone")?;
        let reply = manager
            .get_allocation_logs(request("gone", None))
            .await?
            .into_inner();
        assert_eq!(reply.allocations.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn get_allocation_logs_unknown_job_is_not_found() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store);

        let status = manager
            .get_allocation_logs(Request::new(GetAllocationLogsRequest {
                job: "missing-job".into(),
                allocation_id: None,
                stream: "all".into(),
                max_bytes: 0,
                tail: false,
                offset: 0,
                include_removed: false,
            }))
            .await
            .expect_err("unknown job should error");
        assert_eq!(status.code(), tonic::Code::NotFound);
        Ok(())
    }

    #[tokio::test]
    async fn kv_reference_updates_trigger_job_restart() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store);

        manager
            .put_kv(Request::new(PutKvRequest {
                key: "secrets/key".into(),
                value: "first".into(),
                value_type: KvValueType::String as i32,
                expected_version: None,
                lock_id: None,
            }))
            .await?;

        let jobspec = json!({
            "name": "kv-job",
            "task": {
                "runtime": "exec",
                "config": {
                    "command": ["echo", "hi"],
                    "env": {
                        "KEY": "$secrets/key"
                    }
                }
            }
        })
        .to_string();

        manager
            .run_job(Request::new(RunJobRequest {
                parsed_jobspec: jobspec,
                submissions: vec![submission()],
                priority: None,
                force_reschedule: false,
                idempotency_key: None,
            }))
            .await?;

        let config = manager.store.get_job_config("kv-job")?;
        let env_value = config.tasks[0]
            .config
            .get("env")
            .and_then(|env| env.get("KEY"))
            .and_then(|value| value.as_str())
            .context("missing env var")?;
        assert_eq!(env_value, "first");

        manager
            .put_kv(Request::new(PutKvRequest {
                key: "secrets/key".into(),
                value: "second".into(),
                value_type: KvValueType::String as i32,
                expected_version: None,
                lock_id: None,
            }))
            .await?;

        let updated = manager.store.get_job_config("kv-job")?;
        let updated_env_value = updated.tasks[0]
            .config
            .get("env")
            .and_then(|env| env.get("KEY"))
            .and_then(|value| value.as_str())
            .context("missing updated env var")?;
        assert_eq!(updated_env_value, "second");

        let evals = manager.store.get_evals()?;
        assert!(
            evals
                .iter()
                .any(|eval| eval.kind == "rollout_tick" && eval.job_id == "kv-job"),
            "expected rollout_tick eval after kv change"
        );

        Ok(())
    }

    #[tokio::test]
    async fn kv_task_env_restart_policy_enqueues_job_update() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store);

        manager
            .put_kv(Request::new(PutKvRequest {
                key: "flags/global".into(),
                value: "alpha".into(),
                value_type: KvValueType::String as i32,
                expected_version: None,
                lock_id: None,
            }))
            .await?;

        let jobspec = json!({
            "name": "kv-restart-policy-job",
            "task": {
                "runtime": "exec",
                "kv": { "watch": true, "onChange": "restart", "onDelete": "fail" },
                "env": [
                    {
                        "name": "FLAGS",
                        "value": { "template": "{{ kv \"flags/global\" }}" }
                    }
                ],
                "config": { "command": ["echo", "hi"] }
            }
        })
        .to_string();

        manager
            .run_job(Request::new(RunJobRequest {
                parsed_jobspec: jobspec,
                submissions: vec![submission()],
                priority: None,
                force_reschedule: false,
                idempotency_key: None,
            }))
            .await?;

        manager
            .put_kv(Request::new(PutKvRequest {
                key: "flags/global".into(),
                value: "beta".into(),
                value_type: KvValueType::String as i32,
                expected_version: None,
                lock_id: None,
            }))
            .await?;

        let evals = manager.store.get_evals()?;
        assert!(evals
            .iter()
            .any(|eval| { eval.kind == "job_update" && eval.job_id == "kv-restart-policy-job" }));

        Ok(())
    }

    #[tokio::test]
    async fn kv_task_env_rollout_policy_enqueues_rollout_tick() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store);

        manager
            .put_kv(Request::new(PutKvRequest {
                key: "tenant/id".into(),
                value: "t1".into(),
                value_type: KvValueType::String as i32,
                expected_version: None,
                lock_id: None,
            }))
            .await?;

        let jobspec = json!({
            "name": "kv-rollout-policy-job",
            "task": {
                "runtime": "exec",
                "kv": { "watch": true, "onChange": "rollout", "onDelete": "fail" },
                "files": [
                    {
                        "path": "/tmp/tenant.conf",
                        "file": { "template": "tenant={{ kv \"tenant/id\" }}\n" }
                    }
                ],
                "config": { "command": ["echo", "hi"] }
            }
        })
        .to_string();

        manager
            .run_job(Request::new(RunJobRequest {
                parsed_jobspec: jobspec,
                submissions: vec![submission()],
                priority: None,
                force_reschedule: false,
                idempotency_key: None,
            }))
            .await?;

        manager
            .put_kv(Request::new(PutKvRequest {
                key: "tenant/id".into(),
                value: "t2".into(),
                value_type: KvValueType::String as i32,
                expected_version: None,
                lock_id: None,
            }))
            .await?;

        let evals = manager.store.get_evals()?;
        assert!(evals
            .iter()
            .any(|eval| { eval.kind == "rollout_tick" && eval.job_id == "kv-rollout-policy-job" }));

        Ok(())
    }

    // --- KV-driven task updates (reactive injections) ---

    async fn put_kv_entry(manager: &ManagerDaemonService, key: &str, value: &str) -> R<()> {
        manager
            .put_kv(Request::new(PutKvRequest {
                key: key.into(),
                value: value.into(),
                value_type: KvValueType::String as i32,
                expected_version: None,
                lock_id: None,
            }))
            .await?;
        Ok(())
    }

    async fn delete_kv_entry(manager: &ManagerDaemonService, key: &str) -> R<()> {
        manager
            .delete_kv(Request::new(crate::structs::DeleteKvRequest {
                key: key.into(),
                expected_version: None,
                lock_id: None,
            }))
            .await?;
        Ok(())
    }

    async fn run_jobspec(manager: &ManagerDaemonService, jobspec: Value) -> Result<(), Status> {
        manager
            .run_job(Request::new(RunJobRequest {
                parsed_jobspec: jobspec.to_string(),
                submissions: vec![submission()],
                priority: None,
                force_reschedule: false,
                idempotency_key: None,
            }))
            .await?;
        Ok(())
    }

    fn evals_of_kind(store: &Store, job_id: &str, kind: &str) -> R<usize> {
        Ok(store
            .get_evals()?
            .iter()
            .filter(|eval| eval.kind == kind && eval.job_id == job_id)
            .count())
    }

    fn task_env_value(store: &Store, job: &str, task: usize, name: &str) -> R<String> {
        let config = store.get_job_config(job)?;
        config.tasks[task]
            .config
            .get("env")
            .and_then(|env| env.get(name))
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .with_context(|| format!("missing env var {name} on task {task}"))
    }

    /// One exec task with a single watched env injection on `key`.
    fn watched_env_jobspec(job: &str, key: &str, on_change: &str, on_delete: &str) -> Value {
        json!({
            "name": job,
            "task": {
                "runtime": "exec",
                "kv": { "watch": true, "onChange": on_change, "onDelete": on_delete },
                "env": [
                    {
                        "name": "WATCHED",
                        "value": { "template": format!("{{{{ kv \"{key}\" }}}}") }
                    }
                ],
                "config": { "command": ["echo", "hi"] }
            }
        })
    }

    #[tokio::test]
    async fn kv_delete_with_on_delete_none_takes_no_action() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store);

        put_kv_entry(&manager, "del/none", "v1").await?;
        run_jobspec(
            &manager,
            watched_env_jobspec("kv-del-none", "del/none", "restart", "none"),
        )
        .await?;

        delete_kv_entry(&manager, "del/none").await?;

        assert_eq!(
            evals_of_kind(&manager.store, "kv-del-none", "job_update")?,
            0
        );
        assert_eq!(
            evals_of_kind(&manager.store, "kv-del-none", "rollout_tick")?,
            0
        );
        assert_eq!(manager.store.get_job("kv-del-none")?.status, "active");
        // The last rendered value is kept in the desired config.
        assert_eq!(
            task_env_value(&manager.store, "kv-del-none", 0, "WATCHED")?,
            "v1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn kv_delete_with_on_delete_restart_enqueues_job_update() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store);

        put_kv_entry(&manager, "del/restart", "v1").await?;
        run_jobspec(
            &manager,
            watched_env_jobspec("kv-del-restart", "del/restart", "none", "restart"),
        )
        .await?;

        delete_kv_entry(&manager, "del/restart").await?;

        assert_eq!(
            evals_of_kind(&manager.store, "kv-del-restart", "job_update")?,
            1
        );
        assert_eq!(manager.store.get_job("kv-del-restart")?.status, "active");
        Ok(())
    }

    #[tokio::test]
    async fn kv_delete_with_on_delete_rollout_creates_rollout() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store);

        put_kv_entry(&manager, "del/rollout", "v1").await?;
        run_jobspec(
            &manager,
            watched_env_jobspec("kv-del-rollout", "del/rollout", "none", "rollout"),
        )
        .await?;
        assert!(manager
            .store
            .list_rollouts(Some("kv-del-rollout"), false)?
            .is_empty());

        delete_kv_entry(&manager, "del/rollout").await?;

        assert_eq!(
            manager
                .store
                .list_rollouts(Some("kv-del-rollout"), false)?
                .len(),
            1
        );
        assert_eq!(
            evals_of_kind(&manager.store, "kv-del-rollout", "rollout_tick")?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn kv_delete_with_on_delete_fail_marks_job_failed_with_reason() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        put_kv_entry(&manager, "del/fail", "v1").await?;
        run_jobspec(
            &manager,
            watched_env_jobspec("kv-del-fail", "del/fail", "restart", "fail"),
        )
        .await?;

        // Place an allocation so the failure reason is visible on it.
        insert_node(&store, "node-1")?;
        let job_config_id = store.get_job_config("kv-del-fail")?.id();
        let (task_id, _) = store
            .get_task_configs_for_job_config(&job_config_id)?
            .into_iter()
            .next()
            .context("missing task config")?;
        let allocation = store.place_alloc("node-1", task_id)?;

        delete_kv_entry(&manager, "del/fail").await?;

        // Job and allocation surface the failure with the explicit reason.
        assert_eq!(store.get_job("kv-del-fail")?.status, "failed");
        let allocs = store.allocs_for_job("kv-del-fail")?;
        let failed = allocs
            .iter()
            .find(|alloc| alloc.id == allocation.id)
            .context("allocation missing after fail")?;
        assert_eq!(failed.status, "failed:missing_kv_dependency");

        // No restart/rollout storm: nothing scheduled, no rollout created.
        assert_eq!(evals_of_kind(&store, "kv-del-fail", "job_update")?, 0);
        assert_eq!(evals_of_kind(&store, "kv-del-fail", "rollout_tick")?, 0);
        assert!(store.list_rollouts(Some("kv-del-fail"), false)?.is_empty());

        // The dependency edges survive so the job can recover later.
        assert!(!store.job_kv_references_for_job("kv-del-fail")?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn kv_put_after_fail_recovers_job_via_on_change() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        put_kv_entry(&manager, "recover/key", "v1").await?;
        run_jobspec(
            &manager,
            watched_env_jobspec("kv-recover", "recover/key", "restart", "fail"),
        )
        .await?;

        delete_kv_entry(&manager, "recover/key").await?;
        assert_eq!(store.get_job("kv-recover")?.status, "failed");

        // Recovery path: putting the key back resolves through onChange.
        put_kv_entry(&manager, "recover/key", "v2").await?;

        assert_eq!(store.get_job("kv-recover")?.status, "active");
        assert_eq!(evals_of_kind(&store, "kv-recover", "job_update")?, 1);
        assert_eq!(task_env_value(&store, "kv-recover", 0, "WATCHED")?, "v2");
        Ok(())
    }

    #[tokio::test]
    async fn kv_reconcile_skips_stopped_job() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        put_kv_entry(&manager, "stopped/key", "v1").await?;
        run_jobspec(
            &manager,
            watched_env_jobspec("kv-stopped", "stopped/key", "restart", "fail"),
        )
        .await?;

        // Stop the job: KV events must now be inert.
        store.stop_job("kv-stopped")?;
        assert_eq!(store.get_job("kv-stopped")?.status, "stopped");

        // A watched-key delete (onDelete: fail) must NOT flip a stopped job to
        // failed, and a put (onChange: restart) must NOT re-place allocations.
        delete_kv_entry(&manager, "stopped/key").await?;
        assert_eq!(store.get_job("kv-stopped")?.status, "stopped");

        put_kv_entry(&manager, "stopped/key", "v2").await?;
        assert_eq!(store.get_job("kv-stopped")?.status, "stopped");

        assert_eq!(evals_of_kind(&store, "kv-stopped", "job_update")?, 0);
        assert_eq!(evals_of_kind(&store, "kv-stopped", "rollout_tick")?, 0);
        assert!(store.list_rollouts(Some("kv-stopped"), false)?.is_empty());
        Ok(())
    }

    /// Two exec tasks, each with its own watched env key and onDelete: fail.
    fn two_task_watched_jobspec(job: &str, key_a: &str, key_b: &str) -> Value {
        json!({
            "name": job,
            "tasks": [
                {
                    "name": "task-a",
                    "runtime": "exec",
                    "kv": { "watch": true, "onChange": "restart", "onDelete": "fail" },
                    "env": [
                        {
                            "name": "WATCHED",
                            "value": { "template": format!("{{{{ kv \"{key_a}\" }}}}") }
                        }
                    ],
                    "config": { "command": ["echo", "a"] }
                },
                {
                    "name": "task-b",
                    "runtime": "exec",
                    "kv": { "watch": true, "onChange": "restart", "onDelete": "fail" },
                    "env": [
                        {
                            "name": "WATCHED",
                            "value": { "template": format!("{{{{ kv \"{key_b}\" }}}}") }
                        }
                    ],
                    "config": { "command": ["echo", "b"] }
                }
            ]
        })
    }

    #[tokio::test]
    async fn kv_fail_stamps_only_impacted_tasks_allocations() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        put_kv_entry(&manager, "scope/a", "a1").await?;
        put_kv_entry(&manager, "scope/b", "b1").await?;
        run_jobspec(
            &manager,
            two_task_watched_jobspec("kv-scope", "scope/a", "scope/b"),
        )
        .await?;

        // Place one allocation per task so we can observe per-task stamping.
        insert_node(&store, "node-1")?;
        let job_config_id = store.get_job_config("kv-scope")?.id();
        let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
        let task_a = tasks
            .iter()
            .find(|(_, t)| t.name == "task-a")
            .map(|(id, _)| *id)
            .context("task-a config")?;
        let task_b = tasks
            .iter()
            .find(|(_, t)| t.name == "task-b")
            .map(|(id, _)| *id)
            .context("task-b config")?;
        let alloc_a = store.place_alloc("node-1", task_a)?;
        let alloc_b = store.place_alloc("node-1", task_b)?;

        // Delete only task-a's key: only task-a's allocation is stamped.
        delete_kv_entry(&manager, "scope/a").await?;

        assert_eq!(store.get_job("kv-scope")?.status, "failed");
        let allocs = store.allocs_for_job("kv-scope")?;
        let a = allocs
            .iter()
            .find(|alloc| alloc.id == alloc_a.id)
            .context("alloc a")?;
        let b = allocs
            .iter()
            .find(|alloc| alloc.id == alloc_b.id)
            .context("alloc b")?;
        assert_eq!(a.status, "failed:missing_kv_dependency");
        assert_ne!(
            b.status, "failed:missing_kv_dependency",
            "task-b's allocation must be untouched: it does not depend on scope/a"
        );
        Ok(())
    }

    #[tokio::test]
    async fn kv_put_after_fail_with_restart_clears_stamps_and_replaces_allocations() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        put_kv_entry(&manager, "clr/key", "v1").await?;
        run_jobspec(
            &manager,
            watched_env_jobspec("kv-clr", "clr/key", "restart", "fail"),
        )
        .await?;

        insert_node(&store, "node-1")?;
        let job_config_id = store.get_job_config("kv-clr")?.id();
        let (task_id, _) = store
            .get_task_configs_for_job_config(&job_config_id)?
            .into_iter()
            .next()
            .context("task config")?;
        let failed_alloc = store.place_alloc("node-1", task_id)?;

        delete_kv_entry(&manager, "clr/key").await?;
        assert_eq!(store.get_job("kv-clr")?.status, "failed");
        let stamped = store
            .allocs_for_job("kv-clr")?
            .into_iter()
            .find(|alloc| alloc.id == failed_alloc.id)
            .context("failed alloc")?;
        assert_eq!(stamped.status, "failed:missing_kv_dependency");

        // Put the key back: onChange=restart recovers — job status back to
        // active and a job_update enqueued to replace the failed allocations.
        put_kv_entry(&manager, "clr/key", "v2").await?;
        assert_eq!(store.get_job("kv-clr")?.status, "active");
        assert_eq!(evals_of_kind(&store, "kv-clr", "job_update")?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn job_run_recovers_kv_failed_job_under_on_change_none() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        put_kv_entry(&manager, "none/key", "v1").await?;
        // onChange: none — a later put does NOT auto-recover.
        run_jobspec(
            &manager,
            watched_env_jobspec("kv-none", "none/key", "none", "fail"),
        )
        .await?;

        delete_kv_entry(&manager, "none/key").await?;
        assert_eq!(store.get_job("kv-none")?.status, "failed");

        // Putting the key back with onChange: none leaves the job failed.
        put_kv_entry(&manager, "none/key", "v2").await?;
        assert_eq!(store.get_job("kv-none")?.status, "failed");

        // A manual `job run` is the documented recovery path: it clears the
        // failed status.
        run_jobspec(
            &manager,
            watched_env_jobspec("kv-none", "none/key", "none", "fail"),
        )
        .await?;
        assert_eq!(store.get_job("kv-none")?.status, "active");
        Ok(())
    }

    #[tokio::test]
    async fn kv_changes_ignored_when_watch_false() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store);

        put_kv_entry(&manager, "unwatched/key", "v1").await?;
        let jobspec = json!({
            "name": "kv-unwatched",
            "task": {
                "runtime": "exec",
                // Defaults would watch, but the entry opts out explicitly.
                "kv": { "watch": true, "onChange": "restart", "onDelete": "fail" },
                "env": [
                    {
                        "name": "WATCHED",
                        "value": { "template": "{{ kv \"unwatched/key\" }}" },
                        "kv": { "watch": false }
                    }
                ],
                "config": { "command": ["echo", "hi"] }
            }
        });
        run_jobspec(&manager, jobspec).await?;

        put_kv_entry(&manager, "unwatched/key", "v2").await?;
        delete_kv_entry(&manager, "unwatched/key").await?;

        assert_eq!(
            evals_of_kind(&manager.store, "kv-unwatched", "job_update")?,
            0
        );
        assert_eq!(
            evals_of_kind(&manager.store, "kv-unwatched", "rollout_tick")?,
            0
        );
        assert_eq!(manager.store.get_job("kv-unwatched")?.status, "active");
        // The put still refreshed the desired config (state-only update).
        assert_eq!(
            task_env_value(&manager.store, "kv-unwatched", 0, "WATCHED")?,
            "v2"
        );
        Ok(())
    }

    #[tokio::test]
    async fn kv_entry_policy_overrides_defaults_field_by_field() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store);

        put_kv_entry(&manager, "prec/a", "a1").await?;
        put_kv_entry(&manager, "prec/b", "b1").await?;
        let jobspec = json!({
            "name": "kv-precedence",
            "task": {
                "runtime": "exec",
                "kv": { "watch": true, "onChange": "restart" },
                "env": [
                    {
                        // Silent entry: inherits watch + onChange=restart.
                        "name": "A",
                        "value": { "template": "{{ kv \"prec/a\" }}" }
                    },
                    {
                        // Overrides onChange only; watch inherited from defaults.
                        "name": "B",
                        "value": { "template": "{{ kv \"prec/b\" }}" },
                        "kv": { "onChange": "rollout" }
                    }
                ],
                "config": { "command": ["echo", "hi"] }
            }
        });
        run_jobspec(&manager, jobspec).await?;

        put_kv_entry(&manager, "prec/a", "a2").await?;
        assert_eq!(
            evals_of_kind(&manager.store, "kv-precedence", "job_update")?,
            1
        );
        assert_eq!(
            evals_of_kind(&manager.store, "kv-precedence", "rollout_tick")?,
            0
        );

        put_kv_entry(&manager, "prec/b", "b2").await?;
        assert_eq!(
            evals_of_kind(&manager.store, "kv-precedence", "rollout_tick")?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn kv_template_kv_source_tracks_template_and_transitive_keys() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        put_kv_entry(&manager, "ui/banner", "hello").await?;
        put_kv_entry(&manager, "templates/banner", "msg={{ kv \"ui/banner\" }}").await?;
        let jobspec = json!({
            "name": "kv-template-kv",
            "task": {
                "runtime": "exec",
                "kv": { "watch": true, "onChange": "restart", "onDelete": "fail" },
                "env": [
                    {
                        "name": "BANNER",
                        "value": { "templateKv": "templates/banner" }
                    }
                ],
                "config": { "command": ["echo", "hi"] }
            }
        });
        run_jobspec(&manager, jobspec).await?;
        assert_eq!(
            task_env_value(&store, "kv-template-kv", 0, "BANNER")?,
            "msg=hello"
        );

        // Both the template key and its transitive lookup are tracked edges.
        let keys: Vec<String> = store
            .job_kv_references_for_job("kv-template-kv")?
            .into_iter()
            .map(|reference| reference.kv_key)
            .collect();
        assert!(keys.iter().any(|key| key == "templates/banner"));
        assert!(keys.iter().any(|key| key == "ui/banner"));

        // Changing a transitive key triggers the edge's policy.
        put_kv_entry(&manager, "ui/banner", "world").await?;
        assert_eq!(evals_of_kind(&store, "kv-template-kv", "job_update")?, 1);
        assert_eq!(
            task_env_value(&store, "kv-template-kv", 0, "BANNER")?,
            "msg=world"
        );

        // Changing the template key itself also triggers the policy.
        put_kv_entry(&manager, "templates/banner", "v2={{ kv \"ui/banner\" }}").await?;
        assert_eq!(evals_of_kind(&store, "kv-template-kv", "job_update")?, 2);
        assert_eq!(
            task_env_value(&store, "kv-template-kv", 0, "BANNER")?,
            "v2=world"
        );
        Ok(())
    }

    #[tokio::test]
    async fn kv_put_only_affects_tasks_watching_the_key() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        put_kv_entry(&manager, "iso/a", "v1").await?;
        let jobspec = json!({
            "name": "kv-isolation",
            "tasks": [
                {
                    "name": "watcher",
                    "runtime": "exec",
                    "kv": { "watch": true, "onChange": "restart", "onDelete": "none" },
                    "env": [
                        { "name": "A", "value": { "template": "{{ kv \"iso/a\" }}" } }
                    ],
                    "config": { "command": ["echo", "hi"] }
                },
                {
                    "name": "bystander",
                    "runtime": "exec",
                    "config": { "command": ["echo", "bye"], "env": { "STATIC": "fixed" } }
                }
            ]
        });
        run_jobspec(&manager, jobspec).await?;

        let before = store.get_job_config("kv-isolation")?;
        let bystander_before = before
            .tasks
            .iter()
            .find(|task| task.name == "bystander")
            .context("missing bystander task")?
            .config
            .clone();

        put_kv_entry(&manager, "iso/a", "v2").await?;

        let after = store.get_job_config("kv-isolation")?;
        let watcher = after
            .tasks
            .iter()
            .find(|task| task.name == "watcher")
            .context("missing watcher task")?;
        let bystander = after
            .tasks
            .iter()
            .find(|task| task.name == "bystander")
            .context("missing bystander task")?;

        assert_eq!(
            watcher.config.get("env").and_then(|env| env.get("A")),
            Some(&Value::String("v2".into()))
        );
        assert_eq!(bystander.config, bystander_before);
        assert_eq!(evals_of_kind(&store, "kv-isolation", "job_update")?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn kv_file_injection_rerender_replaces_existing_entry() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        put_kv_entry(&manager, "file/key", "v1").await?;
        let jobspec = json!({
            "name": "kv-file-rerender",
            "task": {
                "runtime": "exec",
                "kv": { "watch": true, "onChange": "restart", "onDelete": "none" },
                "files": [
                    {
                        "path": "/tmp/kv-rerender.conf",
                        "file": { "template": "value={{ kv \"file/key\" }}" }
                    }
                ],
                "config": { "command": ["echo", "hi"] }
            }
        });
        run_jobspec(&manager, jobspec).await?;

        put_kv_entry(&manager, "file/key", "v2").await?;

        let config = store.get_job_config("kv-file-rerender")?;
        let files = config.tasks[0]
            .config
            .get("files")
            .and_then(Value::as_array)
            .context("missing files array")?;
        assert_eq!(files.len(), 1, "re-render must replace, not append");
        assert_eq!(
            files[0].get("content").and_then(Value::as_str),
            Some("value=v2")
        );
        Ok(())
    }

    fn live_file_jobspec(job: &str, key: &str) -> Value {
        json!({
            "name": job,
            "task": {
                "runtime": "exec",
                "kv": { "watch": true, "onDelete": "fail" },
                "files": [
                    {
                        "path": "/tmp/legion-live.pem",
                        "mode": "0400",
                        "file": { "template": format!("{{{{ kv \"{key}\" }}}}") },
                        "kv": { "onChange": "update" },
                        "changeMode": "signal",
                        "changeSignal": "SIGHUP",
                        "splay": "5s"
                    }
                ],
                "config": { "command": ["echo", "hi"] }
            }
        })
    }

    #[tokio::test]
    async fn kv_file_update_moves_allocations_in_place_without_a_rollout_test() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        put_kv_entry(&manager, "certs/live", "pem-1").await?;
        run_jobspec(&manager, live_file_jobspec("kv-live", "certs/live")).await?;
        insert_node(&store, "node-1")?;
        let first_config = store.get_job_config("kv-live")?.id();
        let (task_id, _) = store
            .get_task_configs_for_job_config(&first_config)?
            .into_iter()
            .next()
            .context("task config")?;
        let alloc = store.place_alloc("node-1", task_id)?;

        put_kv_entry(&manager, "certs/live", "pem-2").await?;

        // The same allocation now points at the re-rendered config.
        let latest = store.get_job_config("kv-live")?;
        assert_ne!(latest.id(), first_config);
        let moved = store
            .allocs_for_job("kv-live")?
            .into_iter()
            .find(|candidate| candidate.id == alloc.id)
            .context("allocation must survive an update")?;
        assert_eq!(moved.job_config_id, latest.id());

        // Nothing replaces it: no restart, no rollout, and the failover pass
        // sees nothing to repair.
        assert_eq!(evals_of_kind(&store, "kv-live", "job_update")?, 0);
        assert_eq!(evals_of_kind(&store, "kv-live", "rollout_tick")?, 0);
        assert!(store.list_rollouts(Some("kv-live"), false)?.is_empty());
        assert_eq!(reconcile_missing_rollouts(&store)?, 0);

        // The rendered entry carries what the worker needs.
        let files = latest.tasks[0]
            .config
            .get("files")
            .and_then(Value::as_array)
            .context("files")?;
        assert_eq!(files[0]["content"], "pem-2");
        assert_eq!(
            files[0]["liveUpdate"],
            json!({ "changeMode": "signal", "changeSignal": "SIGHUP", "splay": "5s" })
        );
        Ok(())
    }

    /// The node names allocations, never paths: the manager derives the KV
    /// path from the allocation's chain, sends the bundle once per version,
    /// and reports per-allocation problems without failing the call.
    #[tokio::test]
    async fn get_ingress_certificates_derives_the_path_and_skips_unchanged_test() -> R<()> {
        use crate::manager::mesh_ca::{generate_ca, issue_node_cert};
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        let (ca_pem, ca_key) = generate_ca()?;
        let (cert, key, _) = issue_node_cert(&ca_pem, &ca_key, "edge")?;
        let bundle = format!("{cert}{key}");
        put_kv_entry(&manager, "certs/edge", &bundle).await?;
        run_jobspec(
            &manager,
            json!({
                "name": "edge",
                "task": {
                    "runtime": "exec",
                    "config": { "command": ["sleep", "30"] },
                    "network": {
                        "ports": [{ "name": "https", "static": 8443 }],
                        "ingress": { "tls": { "mode": "server", "certFrom": { "kv": "certs/edge" } } },
                    },
                },
            }),
        )
        .await?;
        insert_node(&store, "node-1")?;
        let job_config_id = store.get_job_config("edge")?.id();
        let (task_id, _) = store
            .get_task_configs_for_job_config(&job_config_id)?
            .into_iter()
            .next()
            .context("task config")?;
        let alloc = store.place_alloc("node-1", task_id)?;

        let fetch = |known: Vec<(i64, i64)>| {
            let manager = &manager;
            async move {
                manager
                    .get_ingress_certificates(Request::new(GetIngressCertificatesRequest {
                        node_id: "node-1".into(),
                        known: known
                            .into_iter()
                            .map(|(allocation_id, version)| {
                                crate::structs::IngressCertificateKnown {
                                    allocation_id,
                                    version,
                                }
                            })
                            .collect(),
                    }))
                    .await
                    .map(|reply| reply.into_inner().certificates)
            }
        };

        let first = fetch(vec![(alloc.id, 0), (alloc.id + 999, 0)]).await?;
        assert_eq!(first[0].kv_path, "certs/edge");
        assert_eq!(first[0].bundle_pem, bundle);
        assert!(first[0].error.is_empty(), "{}", first[0].error);
        assert!(
            first[1].error.contains("not placed on this node"),
            "{}",
            first[1].error
        );

        let again = fetch(vec![(alloc.id, first[0].version)]).await?;
        assert!(again[0].unchanged);
        assert!(
            again[0].bundle_pem.is_empty(),
            "no material for an unchanged version"
        );

        // A bad put is reported against the allocation, not sent.
        put_kv_entry(&manager, "certs/edge", &cert).await?;
        let bad = fetch(vec![(alloc.id, first[0].version)]).await?;
        assert!(bad[0].bundle_pem.is_empty());
        assert!(bad[0].error.contains("no private key"), "{}", bad[0].error);
        Ok(())
    }

    /// The node names (allocation, upstream, header), never paths: the
    /// manager derives the KV path from the allocation's own connect
    /// declaration, sends the value once per version, and marks a deleted
    /// key or an undeclared header `absent` so the node drops what it holds.
    #[tokio::test]
    async fn get_upstream_credentials_derives_the_path_and_skips_unchanged_test() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        put_kv_entry(&manager, "secrets/openai", "sk-live-1\n").await?;
        run_jobspec(
            &manager,
            json!({
                "name": "agent",
                "task": {
                    "runtime": "exec",
                    "config": { "command": ["sleep", "30"] },
                    "connect": {
                        "upstreams": [{
                            "name": "openai",
                            "to": "https://api.openai.com",
                            "credentials": [{
                                "header": "Authorization",
                                "prefix": "Bearer ",
                                "from": { "kv": "secrets/openai" },
                            }],
                        }],
                    },
                },
            }),
        )
        .await?;
        insert_node(&store, "node-1")?;
        let job_config_id = store.get_job_config("agent")?.id();
        let (task_id, _) = store
            .get_task_configs_for_job_config(&job_config_id)?
            .into_iter()
            .next()
            .context("task config")?;
        let alloc = store.place_alloc("node-1", task_id)?;

        let fetch = |known: Vec<(i64, &'static str, &'static str, i64)>| {
            let manager = &manager;
            async move {
                manager
                    .get_upstream_credentials(Request::new(GetUpstreamCredentialsRequest {
                        node_id: "node-1".into(),
                        known: known
                            .into_iter()
                            .map(|(allocation_id, upstream, header, version)| {
                                crate::structs::UpstreamCredentialKnown {
                                    allocation_id,
                                    upstream: upstream.into(),
                                    header: header.into(),
                                    version,
                                }
                            })
                            .collect(),
                    }))
                    .await
                    .map(|reply| reply.into_inner().credentials)
            }
        };

        let first = fetch(vec![
            (alloc.id, "openai", "authorization", 0),
            (alloc.id, "openai", "x-api-key", 0),
            (alloc.id + 999, "openai", "authorization", 0),
        ])
        .await?;
        assert_eq!(first[0].source, "kv secrets/openai");
        assert_eq!(
            first[0].value, "sk-live-1",
            "one trailing newline is trimmed"
        );
        assert!(first[0].error.is_empty(), "{}", first[0].error);
        assert!(
            first[1].absent && first[1].value.is_empty(),
            "{:?}",
            first[1]
        );
        assert!(first[2].absent, "an allocation on another node is absent");
        assert!(
            first[2].error.contains("not placed on this node"),
            "{}",
            first[2].error
        );

        let again = fetch(vec![(
            alloc.id,
            "openai",
            "authorization",
            first[0].version,
        )])
        .await?;
        assert!(again[0].unchanged);
        assert!(
            again[0].value.is_empty(),
            "no material for an unchanged version"
        );

        // A value no header can carry is reported, not sent, and is not
        // authoritative (the node keeps its last good value).
        put_kv_entry(&manager, "secrets/openai", "a\r\nInjected: yes").await?;
        let bad = fetch(vec![(
            alloc.id,
            "openai",
            "authorization",
            first[0].version,
        )])
        .await?;
        assert!(bad[0].value.is_empty());
        assert!(!bad[0].absent);
        assert!(bad[0].error.contains("line break"), "{}", bad[0].error);

        // Deleting the key revokes it.
        delete_kv_entry(&manager, "secrets/openai").await?;
        let gone = fetch(vec![(
            alloc.id,
            "openai",
            "authorization",
            first[0].version,
        )])
        .await?;
        assert!(gone[0].absent);
        assert_eq!(gone[0].error, "kv secrets/openai not found");
        Ok(())
    }

    /// `from = { identity }`: the manager mints the task's identity for the
    /// allocation, the node keeps it while more than half its TTL is left,
    /// and an identity the task does not declare is absent.
    #[tokio::test]
    async fn get_upstream_credentials_mints_an_identity_token_test() -> R<()> {
        use crate::structs::workload_identity::WorkloadIdentity;
        let store = setup_store()?;
        let wi = wi_state(&store)?;
        let manager =
            ManagerDaemonService::new(store.clone()).with_workload_identity(Some(wi.clone()));
        let task = TaskConfig {
            identities: vec![WorkloadIdentity {
                name: "api".into(),
                audience: vec!["https://api.example.com".into()],
                ttl: Some("10m".into()),
                ..Default::default()
            }],
            connect: Some(serde_json::from_value(json!({
                "upstreams": [{
                    "name": "api",
                    "to": "https://api.example.com",
                    "credentials": [{
                        "header": "authorization",
                        "prefix": "Bearer ",
                        "from": { "identity": "api" },
                    }],
                }],
            }))?),
            ..TaskConfig::new("agent-task", "noop")
        };
        task.validate_identities()?;
        let alloc = submit_and_place_identity_task(&store, "agent", task)?;

        let fetch = |version: i64| {
            let manager = &manager;
            async move {
                manager
                    .get_upstream_credentials(Request::new(GetUpstreamCredentialsRequest {
                        node_id: "node-1".into(),
                        known: vec![crate::structs::UpstreamCredentialKnown {
                            allocation_id: alloc.id,
                            upstream: "api".into(),
                            header: "authorization".into(),
                            version,
                        }],
                    }))
                    .await
                    .map(|reply| reply.into_inner().credentials.remove(0))
            }
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        let first = fetch(0).await?;
        assert!(first.error.is_empty(), "{}", first.error);
        assert_eq!(first.source, "identity api");
        assert!(
            (first.version - now - 600).abs() <= 5,
            "the version is the token's expiry: {} vs now {now}",
            first.version
        );
        let claims = wi
            .verify(&first.value, "https://api.example.com")
            .context("the injected value is a token for the upstream's audience")?;
        assert_eq!(claims["legion.io/job"], "agent");

        // Fresh enough: nothing is minted or sent.
        let again = fetch(first.version).await?;
        assert!(again.unchanged && again.value.is_empty(), "{again:?}");
        assert_eq!(again.version, first.version);

        // Less than half the TTL left: a new token.
        let stale = fetch(now + 200).await?;
        assert!(!stale.unchanged);
        assert!(!stale.value.is_empty());

        // Workload identity off on the manager: an error the node reports,
        // not an authoritative absence.
        let disabled = ManagerDaemonService::new(store.clone());
        let off = disabled
            .get_upstream_credentials(Request::new(GetUpstreamCredentialsRequest {
                node_id: "node-1".into(),
                known: vec![crate::structs::UpstreamCredentialKnown {
                    allocation_id: alloc.id,
                    upstream: "api".into(),
                    header: "authorization".into(),
                    version: 0,
                }],
            }))
            .await?
            .into_inner()
            .credentials
            .remove(0);
        assert!(off.value.is_empty() && !off.absent);
        assert!(off.error.contains("not enabled"), "{}", off.error);
        Ok(())
    }

    #[tokio::test]
    async fn kv_file_update_on_a_failed_job_recovers_through_restart_test() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        put_kv_entry(&manager, "certs/gone", "pem-1").await?;
        run_jobspec(&manager, live_file_jobspec("kv-live-fail", "certs/gone")).await?;
        delete_kv_entry(&manager, "certs/gone").await?;
        assert_eq!(store.get_job("kv-live-fail")?.status, "failed");

        // Only a replacement clears the failure stamps, so the put restarts.
        put_kv_entry(&manager, "certs/gone", "pem-2").await?;
        assert_eq!(store.get_job("kv-live-fail")?.status, "active");
        assert_eq!(evals_of_kind(&store, "kv-live-fail", "job_update")?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn kv_update_is_rejected_where_it_cannot_apply_test() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        put_kv_entry(&manager, "flags/x", "1").await?;

        let cases = [
            (
                // A task-level default reaches env entries too.
                json!({ "kv": { "watch": true, "onChange": "update" },
                        "env": [{ "name": "X", "value": { "template": "{{ kv \"flags/x\" }}" } }] }),
                "cannot change a running process's environment",
            ),
            (
                json!({ "files": [{ "path": "/tmp/x", "file": { "template": "{{ kv \"flags/x\" }}" },
                                    "kv": { "watch": true, "onDelete": "update" } }] }),
                "onDelete cannot be \"update\"",
            ),
            (
                json!({ "files": [{ "path": "/tmp/x", "file": { "template": "{{ kv \"flags/x\" }}" },
                                    "kv": { "watch": true, "onChange": "restart" },
                                    "changeMode": "signal", "changeSignal": "SIGHUP" }] }),
                "changeMode only applies with kv.onChange = \"update\"",
            ),
            (
                json!({ "files": [{ "path": "/tmp/x", "file": { "template": "{{ kv \"flags/x\" }}" },
                                    "kv": { "watch": true, "onChange": "update" },
                                    "changeMode": "signal" }] }),
                "changeMode = \"signal\" needs changeSignal",
            ),
        ];
        for (index, (task, expected)) in cases.into_iter().enumerate() {
            let mut task = task;
            task["runtime"] = json!("exec");
            task["config"] = json!({ "command": ["echo", "hi"] });
            let status = run_jobspec(
                &manager,
                json!({ "name": format!("kv-reject-{index}"), "task": task }),
            )
            .await
            .expect_err("submit must be rejected");
            assert!(
                status.message().contains(expected),
                "case {index}: {:?} should mention {expected:?}",
                status.message()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn kv_debounce_coalesces_rapid_puts_into_one_action() -> R<()> {
        let store = setup_store()?;
        // Seed without opening a debounce window.
        store.put_kv("burst/key", "v0", KvValueType::String, None, None)?;
        let manager =
            ManagerDaemonService::new(store.clone()).with_kv_debounce(Duration::from_millis(100));

        run_jobspec(
            &manager,
            watched_env_jobspec("kv-burst", "burst/key", "restart", "fail"),
        )
        .await?;

        for value in ["v1", "v2", "v3", "v4", "v5"] {
            put_kv_entry(&manager, "burst/key", value).await?;
        }

        // Inside the window: nothing applied yet.
        assert_eq!(evals_of_kind(&store, "kv-burst", "job_update")?, 0);
        assert_eq!(task_env_value(&store, "kv-burst", 0, "WATCHED")?, "v0");

        tokio::time::sleep(Duration::from_millis(400)).await;

        // Exactly one coalesced action, rendered from the latest KV state.
        assert_eq!(evals_of_kind(&store, "kv-burst", "job_update")?, 1);
        assert_eq!(task_env_value(&store, "kv-burst", 0, "WATCHED")?, "v5");
        Ok(())
    }

    #[tokio::test]
    async fn kv_debounce_put_then_delete_resolves_through_on_delete() -> R<()> {
        let store = setup_store()?;
        store.put_kv("burst/del", "v0", KvValueType::String, None, None)?;
        let manager =
            ManagerDaemonService::new(store.clone()).with_kv_debounce(Duration::from_millis(100));

        run_jobspec(
            &manager,
            watched_env_jobspec("kv-burst-del", "burst/del", "restart", "fail"),
        )
        .await?;

        // Put then delete inside the same window: the delete wins and the
        // edge resolves through onDelete (fail), not onChange.
        put_kv_entry(&manager, "burst/del", "v1").await?;
        delete_kv_entry(&manager, "burst/del").await?;

        tokio::time::sleep(Duration::from_millis(400)).await;

        assert_eq!(store.get_job("kv-burst-del")?.status, "failed");
        assert_eq!(evals_of_kind(&store, "kv-burst-del", "job_update")?, 0);
        Ok(())
    }

    /// M4: a KV put whose debounced reconciliation never ran (here: the
    /// write bypasses the RPC, as if the flush were lost to a crash) is
    /// recovered by the leader sweep, and a job that is up to date is left
    /// alone.
    #[tokio::test]
    async fn kv_sweep_recovers_a_missed_put_and_is_quiet_otherwise_test() -> R<()> {
        let store = setup_store()?;
        store.put_kv("sweep/key", "v0", KvValueType::String, None, None)?;
        let manager = ManagerDaemonService::new(store.clone());
        run_jobspec(
            &manager,
            watched_env_jobspec("kv-sweep", "sweep/key", "restart", "fail"),
        )
        .await?;

        // Up to date: nothing to do, no writes, no evals.
        assert_eq!(sweep_missed_kv_changes(&store)?, 0);
        // A reconciled put (the normal path) leaves nothing for the sweep.
        put_kv_entry(&manager, "sweep/key", "v1").await?;
        assert_eq!(evals_of_kind(&store, "kv-sweep", "job_update")?, 1);
        assert_eq!(sweep_missed_kv_changes(&store)?, 0);

        // The lost event: the value changes with no reconciliation.
        store.put_kv("sweep/key", "v2", KvValueType::String, None, None)?;
        assert_eq!(task_env_value(&store, "kv-sweep", 0, "WATCHED")?, "v1");

        assert_eq!(sweep_missed_kv_changes(&store)?, 1);
        assert_eq!(task_env_value(&store, "kv-sweep", 0, "WATCHED")?, "v2");
        assert_eq!(evals_of_kind(&store, "kv-sweep", "job_update")?, 2);
        // Converged: the next sweep is a no-op.
        assert_eq!(sweep_missed_kv_changes(&store)?, 0);
        Ok(())
    }

    /// M4: a missed delete of an `onDelete = "fail"` key is recovered too.
    #[tokio::test]
    async fn kv_sweep_recovers_a_missed_fail_on_delete_test() -> R<()> {
        let store = setup_store()?;
        store.put_kv("sweep/del", "v0", KvValueType::String, None, None)?;
        let manager = ManagerDaemonService::new(store.clone());
        run_jobspec(
            &manager,
            watched_env_jobspec("kv-sweep-del", "sweep/del", "restart", "fail"),
        )
        .await?;

        store.delete_kv("sweep/del", None, None)?;
        assert_ne!(store.get_job("kv-sweep-del")?.status, "failed");

        assert_eq!(sweep_missed_kv_changes(&store)?, 1);
        assert_eq!(store.get_job("kv-sweep-del")?.status, "failed");
        // Already failed: the sweep does not re-fail it every tick.
        assert_eq!(sweep_missed_kv_changes(&store)?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn run_job_fails_when_required_kv_key_is_missing() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store);

        let err = run_jobspec(
            &manager,
            watched_env_jobspec("kv-missing-key", "nope/missing", "restart", "fail"),
        )
        .await
        .expect_err("submit should fail for missing kv key");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("kv entry nope/missing not found"),
            "unexpected error message: {}",
            err.message()
        );
        Ok(())
    }

    /// KV file injection into containers works now that the task directory
    /// is mounted — but only at a destination the container can actually see.
    /// A host-absolute path outside the mount is rejected at submit with the
    /// fix in the message, rather than staging to a directory the workload
    /// could never read.
    #[tokio::test]
    async fn run_job_scopes_container_file_injection_to_the_task_dir() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store);

        let out_of_range = json!({
            "name": "kv-container-files",
            "task": {
                "runtime": "container",
                "files": [
                    {
                        "path": "/etc/app/conf",
                        "file": { "template": "static-content" }
                    }
                ],
                "config": { "image": "alpine", "command": ["echo", "hi"] }
            }
        });
        let err = run_jobspec(&manager, out_of_range)
            .await
            .expect_err("a path outside the mount is unreachable from inside");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        let message = err.message();
        assert!(message.contains("/legion/task"), "names the fix: {message}");
        assert!(
            message.contains("outside the container task directory"),
            "unexpected error message: {message}"
        );

        // A relative path resolves against the mount and is accepted.
        let in_range = json!({
            "name": "kv-container-files-ok",
            "task": {
                "runtime": "container",
                "files": [
                    {
                        "path": "config/app.toml",
                        "file": { "template": "static-content" }
                    }
                ],
                "config": { "image": "alpine", "command": ["echo", "hi"] }
            }
        });
        run_jobspec(&manager, in_range)
            .await
            .expect("a path under the mount is accepted");
        Ok(())
    }

    #[tokio::test]
    async fn run_job_cancels_superseded_rollouts_and_cleans_stale_allocations() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        insert_node(&store, "node-a")?;

        let v1 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "replace-rollout".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "command": ["echo", "v1"] }),
                ..TaskConfig::new("api", "exec")
            }],
            version: None,
        };
        store.handle_job_submit(&v1, &[submission()])?;
        let v1_id = store.get_job_config(&v1.name)?.id();
        let v1_task_id = store.get_task_configs_for_job_config(&v1_id)?[0].0;
        let _v1_alloc = store.place_alloc("node-a", v1_task_id)?;

        let v2 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "replace-rollout".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "command": ["echo", "v2"] }),
                ..TaskConfig::new("api", "exec")
            }],
            version: None,
        };
        store.handle_job_submit(&v2, &[submission()])?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        let v2_task_id = store.get_task_configs_for_job_config(&v2_id)?[0].0;
        let _v2_alloc = store.place_alloc("node-a", v2_task_id)?;

        let superseded = store.create_rollout(
            "replace-rollout",
            Some(&v1_id),
            &v2_id,
            "running",
            Some("default"),
            Some("auto"),
            Some("createFirst"),
            Some(false),
            "{}",
        )?;

        let jobspec = json!({
            "name": "replace-rollout",
            "task": {
                "runtime": "exec",
                "group": "web",
                "config": {
                    "command": ["echo", "v3"]
                }
            }
        })
        .to_string();

        manager
            .run_job(Request::new(RunJobRequest {
                parsed_jobspec: jobspec,
                submissions: vec![submission()],
                priority: None,
                force_reschedule: false,
                idempotency_key: None,
            }))
            .await?;

        let cancelled = store
            .get_rollout(superseded.id)?
            .context("missing superseded rollout")?;
        assert_eq!(cancelled.status, "cancelled");

        let stale_allocs = store
            .allocs_for_job("replace-rollout")?
            .into_iter()
            .filter(|alloc| alloc.job_config_id == v1_id)
            .collect::<Vec<_>>();
        assert!(
            stale_allocs.is_empty(),
            "expected stale v1 allocations to be removed"
        );

        Ok(())
    }

    #[tokio::test]
    async fn stop_rollout_reverts_target_allocations() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        insert_node(&store, "node-a")?;

        let v1 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "stop-rollout".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "version": 1 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v1, &[submission()])?;
        let v1_id = store.get_job_config(&v1.name)?.id();
        let v1_task_id = store.get_task_configs_for_job_config(&v1_id)?[0].0;
        let _v1_alloc = store.place_alloc("node-a", v1_task_id)?;

        let v2 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "stop-rollout".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "version": 2 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v2, &[submission()])?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        let v2_task_id = store.get_task_configs_for_job_config(&v2_id)?[0].0;
        let _v2_alloc = store.place_alloc("node-a", v2_task_id)?;

        let rollout = store.create_rollout(
            "stop-rollout",
            Some(&v1_id),
            &v2_id,
            "running",
            Some("default"),
            Some("auto"),
            Some("createFirst"),
            Some(false),
            "{}",
        )?;

        let response = manager
            .stop_rollout(Request::new(StopRolloutRequest {
                id: rollout.id,
                revert: true,
                priority: None,
            }))
            .await?
            .into_inner();

        assert_eq!(response.message, "Rollout stopped and reverted");
        let stopped = response.rollout.context("missing stopped rollout")?;
        assert_eq!(stopped.status, "cancelled");
        assert!(store.allocs_for_job_config(&v2_id)?.is_empty());
        assert!(!store.allocs_for_job_config(&v1_id)?.is_empty());

        Ok(())
    }

    #[test]
    fn update_node_metadata_evicts_allocations_with_invalid_constraints() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        let node_a = Node {
            id: "node-a".into(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({ "node_id": "node-a" }),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({ "tier": "edge" }),
            health_checks: json!({}),
        };
        let node_b = Node {
            id: "node-b".into(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({ "node_id": "node-b" }),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({ "tier": "edge" }),
            health_checks: json!({}),
        };
        store.insert_node(NodeMsg::from(node_a))?;
        store.insert_node(NodeMsg::from(node_b))?;

        let job_name = "metadata-evict".to_string();
        let task = TaskConfig {
            constraints: json!({ "tier": "edge" }),
            ..TaskConfig::new("task", "noop")
        };
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![task],
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(&job_name)?.id();

        let initial_eval = Eval {
            id: "1".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_name.clone(),
            job_config_id: job_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        };
        initial_eval.handle(&store)?;

        let initial_alloc = store
            .allocs_for_job(&job_name)?
            .into_iter()
            .next()
            .context("missing initial allocation")?;
        assert!(["node-a", "node-b"].contains(&initial_alloc.node_id.as_str()));
        let target_node_id = initial_alloc.node_id.clone();
        let expected_node_id = if target_node_id == "node-a" {
            "node-b"
        } else {
            "node-a"
        };

        let mut diff: Map<String, Value> = Map::new();
        diff.insert("tier".into(), json!("core"));
        let runtime = tokio::runtime::Runtime::new()?;
        let diff_json = serde_json::to_string(&diff)?;
        let update_result = runtime.block_on(async {
            manager
                .update_node_dynamic(Request::new(UpdateNodeDynamicRequest {
                    id: target_node_id.clone(),
                    dynamic_diff: diff_json,
                }))
                .await
        });
        update_result.map_err(|err| anyhow!(err))?;

        let evals = store.get_evals()?;
        let update_eval = evals
            .iter()
            .find(|eval| eval.kind == "job_update" && eval.job_id == job_name)
            .context("missing job_update eval")?;
        update_eval.handle(&store)?;

        let updated_allocs = store.allocs_for_job(&job_name)?;
        assert_eq!(updated_allocs.len(), 1);
        assert_eq!(updated_allocs[0].node_id, expected_node_id);

        Ok(())
    }

    #[test]
    fn failed_allocations_enqueue_reschedule() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job".into(),
            tasks: vec![TaskConfig {
                on_unhealthy: TaskOnUnhealthyConfig {
                    move_delay: Duration::from_secs(0),
                    ..TaskOnUnhealthyConfig::default()
                },
                ..TaskConfig::new("task", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = store.get_task_configs_for_job_config(&job_config_id)?[0].0;
        let allocation = store.place_alloc("node-a", task_id)?;

        let tracker = Arc::new(Mutex::new(AllocationFailureTracker::default()));
        handle_failed_allocations(
            &store,
            &tracker,
            "node-a",
            &[FailedAllocation {
                allocation_id: allocation.id,
                reason: "boom".into(),
                artifact_states: None,
            }],
        )?;
        // The reschedule is enqueued from a spawned thread, so poll rather
        // than sleep a fixed 10ms, which flaked under a loaded machine.
        // The freed capacity can also queue a capacity pass (`node_join`), in
        // either order, so look for the reschedule rather than take the
        // first eval.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let eval = loop {
            let reschedule = store
                .get_evals()?
                .into_iter()
                .find(|eval| eval.kind == "job_reschedule");
            if reschedule.is_some() || std::time::Instant::now() >= deadline {
                break reschedule;
            }
            std::thread::sleep(Duration::from_millis(10));
        };

        assert!(store.get_allocation_by_id(allocation.id)?.is_none());
        let eval = eval.context("missing job_reschedule eval")?;
        assert_eq!(eval.job_config_id, job_config_id);
        assert_eq!(eval.node_name, "node-a");

        Ok(())
    }

    #[test]
    fn artifact_staging_failures_emit_per_artifact_events() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "artifact-fail-job".into(),
            tasks: vec![TaskConfig::new("task", "noop")],
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = store.get_task_configs_for_job_config(&job_config_id)?[0].0;
        let allocation = store.place_alloc("node-a", task_id)?;

        let artifact_states = serde_json::json!([
            {
                "name": "tool",
                "source": "https://example.com/tool.tar.gz",
                "state": "failed",
                "attempts": 3,
                "error": "404 after retries",
            },
            // Non-failed rows in the final snapshot produce no events.
            {
                "name": "ok",
                "source": "https://example.com/ok.tar.gz",
                "state": "staged",
            },
        ])
        .to_string();

        let tracker = Arc::new(Mutex::new(AllocationFailureTracker::default()));
        handle_failed_allocations(
            &store,
            &tracker,
            "node-a",
            &[FailedAllocation {
                allocation_id: allocation.id,
                reason: "artifact staging failed: 404".into(),
                artifact_states: Some(artifact_states),
            }],
        )?;

        let events = store.list_scheduler_events(&SchedulerEventFilter {
            kind: Some("artifact.failed".into()),
            ..Default::default()
        })?;
        assert_eq!(events.len(), 1, "{events:?}");
        let event = &events[0];
        assert_eq!(event.allocation_id, Some(allocation.id));
        assert_eq!(event.job_id.as_deref(), Some("artifact-fail-job"));
        assert_eq!(event.node_id.as_deref(), Some("node-a"));
        assert!(event.message.contains("tool"), "{}", event.message);
        assert!(event.metadata_json.contains("404 after retries"));

        // The allocation-level failure event is still emitted alongside.
        let allocation_events = store.list_scheduler_events(&SchedulerEventFilter {
            kind: Some("allocation.failed".into()),
            ..Default::default()
        })?;
        assert_eq!(allocation_events.len(), 1);
        Ok(())
    }

    #[test]
    fn failed_allocations_from_non_owner_node_are_ignored() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        insert_node(&store, "node-b")?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-non-owner".into(),
            tasks: vec![TaskConfig::new("task", "noop")],
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = store.get_task_configs_for_job_config(&job_config_id)?[0].0;
        let allocation = store.place_alloc("node-a", task_id)?;

        let tracker = Arc::new(Mutex::new(AllocationFailureTracker::default()));
        handle_failed_allocations(
            &store,
            &tracker,
            "node-b",
            &[FailedAllocation {
                allocation_id: allocation.id,
                reason: "spoofed".into(),
                artifact_states: None,
            }],
        )?;
        std::thread::sleep(Duration::from_millis(10));

        assert!(store.get_allocation_by_id(allocation.id)?.is_some());
        let evals = store.get_evals()?;
        assert!(evals.is_empty());

        Ok(())
    }

    #[test]
    fn allocation_health_snapshots_upsert() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        insert_node(&store, "node-b")?;
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "health-upsert".into(),
            tasks: vec![TaskConfig::new("task", "noop")],
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = store.get_task_configs_for_job_config(&job_config_id)?[0].0;
        let allocation = store.place_alloc("node-a", task_id)?;

        update_allocation_health_snapshots(
            &store,
            "node-a",
            &[AllocationHealth {
                allocation_id: allocation.id,
                healthy: false,
                status: "starting".into(),
                artifact_states: None,
            }],
        )?;
        update_allocation_health_snapshots(
            &store,
            "node-a",
            &[AllocationHealth {
                allocation_id: allocation.id,
                healthy: true,
                status: "healthy".into(),
                artifact_states: None,
            }],
        )?;

        let health = store.allocation_health_for_allocations(&[allocation.id])?;
        let entry = health
            .get(&allocation.id)
            .context("missing allocation health")?;
        assert!(entry.healthy);
        assert_eq!(entry.status, "healthy");

        update_allocation_health_snapshots(
            &store,
            "node-b",
            &[AllocationHealth {
                allocation_id: allocation.id,
                healthy: false,
                status: "wrong node".into(),
                artifact_states: None,
            }],
        )?;
        let health = store.allocation_health_for_allocations(&[allocation.id])?;
        let entry = health
            .get(&allocation.id)
            .context("missing allocation health after non-owner write attempt")?;
        assert!(entry.healthy);
        assert_eq!(entry.status, "healthy");
        assert_eq!(entry.node_id, "node-a");

        Ok(())
    }

    #[test]
    fn rollout_group_status_includes_allocation_health() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;

        let v1 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "rollout-health".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "version": 1 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v1, &[submission()])?;
        let v1_id = store.get_job_config(&v1.name)?.id();
        let v1_task_id = store.get_task_configs_for_job_config(&v1_id)?[0].0;
        let old_alloc = store.place_alloc("node-a", v1_task_id)?;

        let v2 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "rollout-health".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "version": 2 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v2, &[submission()])?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        let v2_task_id = store.get_task_configs_for_job_config(&v2_id)?[0].0;
        let new_alloc = store.place_alloc("node-a", v2_task_id)?;

        let rollout = store.create_rollout(
            "rollout-health",
            Some(&v1_id),
            &v2_id,
            "running",
            Some("default"),
            Some("manual"),
            Some("createFirst"),
            Some(false),
            r#"{"runtime":{"groups":{"web":{"stepIndex":1,"pendingManualAdvances":2}}}}"#,
        )?;

        update_allocation_health_snapshots(
            &store,
            "node-a",
            &[AllocationHealth {
                allocation_id: new_alloc.id,
                healthy: true,
                status: "http passed".into(),
                artifact_states: None,
            }],
        )?;
        let groups = build_rollout_group_status(&store, &rollout)?;
        let web = groups
            .iter()
            .find(|group| group.group == "web")
            .context("missing web group")?;
        assert_eq!(web.step_index, 1);
        assert_eq!(web.pending_manual_advances, 2);
        assert!(web
            .allocations
            .iter()
            .any(|allocation| allocation.allocation_id == old_alloc.id));
        let replacement = web
            .allocations
            .iter()
            .find(|allocation| allocation.allocation_id == new_alloc.id)
            .context("missing replacement allocation")?;
        assert_eq!(replacement.role, "replacement");
        assert_eq!(replacement.allocation_healthy, Some(true));
        assert_eq!(
            replacement.allocation_health.as_deref(),
            Some("http passed")
        );

        Ok(())
    }

    #[test]
    fn rollout_group_status_partitions_allocations_by_group() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;

        let v1 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "rollout-groups".into(),
            tasks: vec![
                TaskConfig {
                    group: "web".into(),
                    config: json!({ "version": 1 }),
                    ..TaskConfig::new("web-task", "noop")
                },
                TaskConfig {
                    group: "api".into(),
                    config: json!({ "version": 1 }),
                    ..TaskConfig::new("api-task", "noop")
                },
            ],
            version: None,
        };
        store.handle_job_submit(&v1, &[submission()])?;
        let v1_id = store.get_job_config(&v1.name)?.id();
        let v1_tasks = store.get_task_configs_for_job_config(&v1_id)?;
        let v1_web = v1_tasks
            .iter()
            .find(|(_, task)| task.group == "web")
            .map(|(id, _)| *id)
            .context("missing v1 web task")?;
        let v1_api = v1_tasks
            .iter()
            .find(|(_, task)| task.group == "api")
            .map(|(id, _)| *id)
            .context("missing v1 api task")?;
        let old_web = store.place_alloc("node-a", v1_web)?;
        let old_api = store.place_alloc("node-a", v1_api)?;

        let v2 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "rollout-groups".into(),
            tasks: vec![
                TaskConfig {
                    group: "web".into(),
                    config: json!({ "version": 2 }),
                    ..TaskConfig::new("web-task", "noop")
                },
                TaskConfig {
                    group: "api".into(),
                    config: json!({ "version": 2 }),
                    ..TaskConfig::new("api-task", "noop")
                },
            ],
            version: None,
        };
        store.handle_job_submit(&v2, &[submission()])?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        let v2_tasks = store.get_task_configs_for_job_config(&v2_id)?;
        let v2_web = v2_tasks
            .iter()
            .find(|(_, task)| task.group == "web")
            .map(|(id, _)| *id)
            .context("missing v2 web task")?;
        let v2_api = v2_tasks
            .iter()
            .find(|(_, task)| task.group == "api")
            .map(|(id, _)| *id)
            .context("missing v2 api task")?;
        let new_web = store.place_alloc("node-a", v2_web)?;
        let new_api = store.place_alloc("node-a", v2_api)?;

        update_allocation_health_snapshots(
            &store,
            "node-a",
            &[
                AllocationHealth {
                    allocation_id: new_web.id,
                    healthy: true,
                    status: "healthy".into(),
                    artifact_states: None,
                },
                AllocationHealth {
                    allocation_id: new_api.id,
                    healthy: false,
                    status: "starting".into(),
                    artifact_states: None,
                },
            ],
        )?;

        let rollout = store.create_rollout(
            "rollout-groups",
            Some(&v1_id),
            &v2_id,
            "running",
            Some("default"),
            Some("manual"),
            Some("createFirst"),
            Some(false),
            r#"{"runtime":{"groups":{"web":{"stepIndex":1},"api":{"stepIndex":0},"ghost":{"stepIndex":0}}}}"#,
        )?;

        let groups = build_rollout_group_status(&store, &rollout)?;
        let web = groups
            .iter()
            .find(|group| group.group == "web")
            .context("missing web group")?;
        let api = groups
            .iter()
            .find(|group| group.group == "api")
            .context("missing api group")?;
        let ghost = groups
            .iter()
            .find(|group| group.group == "ghost")
            .context("missing runtime-only group")?;

        assert_eq!(web.allocations.len(), 2);
        assert!(web
            .allocations
            .iter()
            .all(|allocation| allocation.task_group == "web"));
        assert!(web
            .allocations
            .iter()
            .any(|allocation| allocation.allocation_id == old_web.id));
        assert!(web
            .allocations
            .iter()
            .any(|allocation| allocation.allocation_id == new_web.id));

        assert_eq!(api.allocations.len(), 2);
        assert!(api
            .allocations
            .iter()
            .all(|allocation| allocation.task_group == "api"));
        assert!(api
            .allocations
            .iter()
            .any(|allocation| allocation.allocation_id == old_api.id));
        assert!(api
            .allocations
            .iter()
            .any(|allocation| allocation.allocation_id == new_api.id));
        assert_eq!(ghost.allocations.len(), 0);

        Ok(())
    }

    #[test]
    fn derive_node_health_handles_nested_objects() {
        let checks = json!({
            "network": "pass",
            "plugins": {
                "exec": true,
                "container": "pass"
            }
        });
        assert_eq!(derive_node_health(&checks), Some(true));

        let checks = json!({
            "network": "pass",
            "plugins": {
                "exec": true,
                "container": "fail"
            }
        });
        assert_eq!(derive_node_health(&checks), Some(false));
    }

    #[tokio::test]
    async fn list_rollout_events_returns_persisted_events() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        insert_node(&store, "node-a")?;

        let v1 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "rollout-events-api".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v1, &[submission()])?;
        let v1_id = store.get_job_config(&v1.name)?.id();

        let v2 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "rollout-events-api".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "version": 2 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v2, &[submission()])?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        let rollout = store.create_rollout(
            "rollout-events-api",
            Some(&v1_id),
            &v2_id,
            "running",
            Some("default"),
            Some("auto"),
            Some("createFirst"),
            Some(false),
            "{}",
        )?;
        store.append_rollout_event(
            rollout.id,
            &rollout.job_id,
            Some("web"),
            "info",
            "step advanced",
            Some(r#"{"step":1}"#),
        )?;

        let response = manager
            .list_rollout_events(Request::new(ListRolloutEventsRequest {
                rollout_id: rollout.id,
                limit: Some(10),
            }))
            .await?
            .into_inner();

        assert_eq!(response.events.len(), 1);
        assert_eq!(response.events[0].rollout_id, rollout.id);
        assert_eq!(response.events[0].group_name.as_deref(), Some("web"));
        assert_eq!(response.events[0].message, "step advanced");
        Ok(())
    }

    #[tokio::test]
    async fn list_events_returns_filtered_events_and_cursor() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        store.emit_event(
            crate::storage::events::EventDraft::info("eval.completed", "Eval 1 completed")
                .job("demo"),
        );
        store.emit_event(
            crate::storage::events::EventDraft::warn("node.lost", "Node node-a marked lost")
                .node("node-a"),
        );

        let response = manager
            .list_events(Request::new(ListEventsRequest::default()))
            .await?
            .into_inner();
        assert_eq!(response.events.len(), 2);
        assert_eq!(response.latest_id, response.events[1].id);

        let filtered = manager
            .list_events(Request::new(ListEventsRequest {
                kind: Some("node".into()),
                ..Default::default()
            }))
            .await?
            .into_inner();
        assert_eq!(filtered.events.len(), 1);
        assert_eq!(filtered.events[0].kind, "node.lost");
        assert_eq!(filtered.events[0].node_id.as_deref(), Some("node-a"));
        // latest_id is unfiltered: the cursor always tracks the journal head.
        assert_eq!(filtered.latest_id, response.latest_id);

        let after = manager
            .list_events(Request::new(ListEventsRequest {
                after_id: Some(response.events[0].id),
                ..Default::default()
            }))
            .await?
            .into_inner();
        assert_eq!(after.events.len(), 1);
        assert_eq!(after.events[0].id, response.events[1].id);

        Ok(())
    }

    #[tokio::test]
    async fn stream_events_replays_backlog_then_delivers_live_events() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        store.emit_event(crate::storage::events::EventDraft::info(
            "job.submitted",
            "Job demo submitted",
        ));

        let mut stream = manager
            .stream_events(Request::new(StreamEventsRequest {
                after_id: Some(0),
                ..Default::default()
            }))
            .await?
            .into_inner();

        let replayed = tokio::time::timeout(Duration::from_secs(2), stream.next()).await?;
        let replayed = replayed.context("stream closed before replaying backlog")??;
        assert_eq!(replayed.kind, "job.submitted");

        store.emit_event(crate::storage::events::EventDraft::warn(
            "node.lost",
            "Node node-a marked lost",
        ));

        let live = tokio::time::timeout(Duration::from_secs(2), stream.next()).await?;
        let live = live.context("stream closed before delivering live event")??;
        assert_eq!(live.kind, "node.lost");
        assert!(live.id > replayed.id);

        Ok(())
    }

    #[tokio::test]
    async fn stream_events_applies_filters_to_live_events() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());

        let mut stream = manager
            .stream_events(Request::new(StreamEventsRequest {
                level: Some("error".into()),
                ..Default::default()
            }))
            .await?
            .into_inner();

        store.emit_event(crate::storage::events::EventDraft::info(
            "eval.completed",
            "Eval 1 completed",
        ));
        store.emit_event(crate::storage::events::EventDraft::error(
            "eval.failed",
            "Eval 2 failed",
        ));

        let event = tokio::time::timeout(Duration::from_secs(2), stream.next()).await?;
        let event = event.context("stream closed before delivering error event")??;
        assert_eq!(event.kind, "eval.failed");
        assert_eq!(event.level, "error");

        Ok(())
    }

    #[test]
    fn desired_allocations_include_reported() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job".into(),
            tasks: vec![TaskConfig {
                on_unhealthy: TaskOnUnhealthyConfig {
                    move_delay: Duration::from_secs(0),
                    ..TaskOnUnhealthyConfig::default()
                },
                ..TaskConfig::new("task", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = store.get_task_configs_for_job_config(&job_config_id)?[0].0;
        let allocation = store.place_alloc("node-a", task_id)?;

        let desired = build_desired_allocations(&store, "node-a", &[])?;
        assert_eq!(desired.len(), 1);

        let desired = build_desired_allocations(&store, "node-a", &[allocation.id])?;
        assert_eq!(desired.len(), 1);

        Ok(())
    }

    /// A finished allocation keeps its row (its slot is still spent) but must
    /// never be handed back to a worker — re-dispatching one would relaunch a
    /// completed batch task on every heartbeat.
    #[test]
    fn terminal_allocations_are_never_dispatched_again() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "batch-job".into(),
            tasks: vec![TaskConfig {
                lifecycle: crate::structs::TaskLifecycle::Batch,
                ..TaskConfig::new("prep", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = store.get_task_configs_for_job_config(&job_config_id)?[0].0;
        let allocation = store.place_alloc("node-a", task_id)?;
        assert_eq!(build_desired_allocations(&store, "node-a", &[])?.len(), 1);

        handle_terminated_allocations(
            &store,
            "node-a",
            &[crate::structs::TerminatedAllocation {
                allocation_id: allocation.id,
                success: true,
                exit_code: Some(0),
                signal: None,
                message: "exited with status 0".into(),
            }],
        )?;

        assert!(build_desired_allocations(&store, "node-a", &[])?.is_empty());
        assert!(store.get_allocation_by_id(allocation.id)?.is_some());

        // A report from a node that does not own the allocation is ignored.
        handle_terminated_allocations(
            &store,
            "node-b",
            &[crate::structs::TerminatedAllocation {
                allocation_id: allocation.id,
                success: false,
                exit_code: Some(1),
                signal: None,
                message: "exited with status 1".into(),
            }],
        )?;
        assert_eq!(
            store
                .get_allocation_by_id(allocation.id)?
                .expect("allocation exists")
                .status,
            "complete"
        );

        Ok(())
    }

    /// A terminal report for a task other tasks depend on queues the eval
    /// that places them (they were held until it exited); a report for a
    /// task nothing depends on queues nothing.
    #[test]
    fn a_terminal_report_queues_the_dependents_eval() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "dag-report".into(),
            tasks: vec![
                TaskConfig {
                    lifecycle: crate::structs::TaskLifecycle::Batch,
                    group: "prep".into(),
                    ..TaskConfig::new("prep", "noop")
                },
                TaskConfig {
                    group: "api".into(),
                    depends_on: vec![crate::structs::TaskDependency {
                        task: "prep".into(),
                        condition: crate::structs::TaskDependencyCondition::Success,
                    }],
                    ..TaskConfig::new("api", "noop")
                },
            ],
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let placed = crate::manager::scheduler::Scheduler::new(&store)?
            .plan_and_apply(&job_config_id, crate::manager::scheduler::PlanMode::Fresh)?;
        assert_eq!(placed.allocations.len(), 1, "api waits for prep");
        assert_eq!(evals_of_kind(&store, "dag-report", "job_reschedule")?, 0);

        handle_terminated_allocations(
            &store,
            "node-a",
            &[crate::structs::TerminatedAllocation {
                allocation_id: placed.allocations[0].id,
                success: true,
                exit_code: Some(0),
                signal: None,
                message: "exited with status 0".into(),
            }],
        )?;
        assert_eq!(evals_of_kind(&store, "dag-report", "job_reschedule")?, 1);
        Ok(())
    }

    /// Re-running the unchanged spec of a stopped job starts it again (it
    /// used to reply "no update needed" and place nothing). A job that is
    /// running keeps the old answer.
    #[tokio::test]
    async fn rerunning_an_unchanged_stopped_job_starts_it_again() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        insert_node(&store, "node-a")?;
        let jobspec = json!({
            "name": "revive",
            "task": { "runtime": "exec", "config": { "command": ["true"] } }
        })
        .to_string();
        let run = || {
            manager.run_job(Request::new(RunJobRequest {
                parsed_jobspec: jobspec.clone(),
                submissions: vec![submission()],
                priority: None,
                force_reschedule: false,
                idempotency_key: None,
            }))
        };

        run().await?;
        let job_config_id = store.get_job_config("revive")?.id();
        let task_id = store.get_task_configs_for_job_config(&job_config_id)?[0].0;
        store.place_alloc("node-a", task_id)?;
        let running = run().await?.into_inner();
        assert!(
            running.message.contains("no update needed"),
            "{}",
            running.message
        );
        assert_eq!(evals_of_kind(&store, "revive", "job_update")?, 0);

        store.stop_job("revive")?;
        store.remove_allocs_for_job("revive")?;
        let revived = run().await?.into_inner();
        assert!(
            revived.message.contains("starting it again"),
            "{}",
            revived.message
        );
        assert_eq!(store.get_job("revive")?.status, "active");
        assert_eq!(evals_of_kind(&store, "revive", "job_update")?, 1);
        Ok(())
    }

    /// GetJobInfo reports what the job is doing, not the stored `active`.
    #[tokio::test]
    async fn job_info_reports_the_derived_status() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        insert_node(&store, "node-a")?;
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "status-job".into(),
            tasks: vec![TaskConfig::new("api", "noop")],
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        async fn status_of(manager: &ManagerDaemonService) -> R<String> {
            let reply = manager
                .get_job_info(Request::new(GetJobInfoRequest {
                    name: "status-job".into(),
                }))
                .await?;
            Ok(reply.into_inner().job.expect("job").status)
        }
        assert_eq!(
            status_of(&manager).await?,
            "pending",
            "accepted, nothing placed yet"
        );

        let job_config_id = store.get_job_config("status-job")?.id();
        let task_id = store.get_task_configs_for_job_config(&job_config_id)?[0].0;
        store.place_alloc("node-a", task_id)?;
        assert_eq!(status_of(&manager).await?, "running");

        let listed = manager
            .list_jobs(Request::new(ListJobsRequest::default()))
            .await?
            .into_inner();
        assert_eq!(listed.jobs[0].status, "running");
        assert_eq!(listed.status_counts.get("running"), Some(&1));

        store.stop_job("status-job")?;
        assert_eq!(status_of(&manager).await?, "stopped");
        Ok(())
    }

    //
    // CONNECT: UPSTREAM ENDPOINT FEED (see docs/service-discovery.md)
    //

    /// Submit a one-task job publishing `<name>` on a static port, place it on
    /// `node-a`, and (optionally) mark it healthy. Distinct ports keep several
    /// services co-resident on one node.
    fn seed_upstream_service(
        store: &Store,
        name: &str,
        port: u16,
        allow: &[&str],
        healthy: bool,
    ) -> R<()> {
        let mut task = TaskConfig::new(name, "noop");
        task.service = Some(serde_json::from_value(json!({
            "port": port,
            "allow": allow,
        }))?);
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: format!("svc-{name}"),
            tasks: vec![task],
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = store.get_task_configs_for_job_config(&job_config_id)?[0].0;
        let allocation = store.place_alloc("node-a", task_id)?;
        if healthy {
            store.upsert_allocation_health_snapshots(
                "node-a",
                &[AllocationHealth {
                    allocation_id: allocation.id,
                    healthy: true,
                    status: "healthy".into(),
                    artifact_states: None,
                }],
            )?;
        }
        Ok(())
    }

    /// A desired allocation for `job_id` declaring one upstream. Only the
    /// fields the endpoint feed reads are populated.
    fn consumer_allocation(
        allocation_id: i64,
        job_id: &str,
        service: &str,
    ) -> crate::structs::TaskAllocation {
        crate::structs::TaskAllocation {
            provisional: None,
            allocation_id,
            job_id: job_id.into(),
            task_connect: json!({ "upstreams": [{ "service": service }] }).to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn upstream_endpoints_fast_path_never_touches_the_store() -> R<()> {
        // Heartbeats run every 2s on every node, so jobs that declare no
        // upstreams must cost exactly zero store reads. `read_index` counts
        // every read connection checkout, so it is the honest witness.
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        seed_upstream_service(&store, "web", 18080, &[], true)?;

        let plain = crate::structs::TaskAllocation {
            provisional: None,
            allocation_id: 1,
            job_id: "plain".into(),
            ..Default::default()
        };
        let reads_before = store.read_index.load(std::sync::atomic::Ordering::Relaxed);
        let endpoints = build_upstream_endpoints(&store, std::slice::from_ref(&plain));
        assert!(endpoints.is_empty());
        assert_eq!(
            store.read_index.load(std::sync::atomic::Ordering::Relaxed),
            reads_before,
            "fast path must not read the store"
        );

        // An empty desired set is the same story.
        let endpoints = build_upstream_endpoints(&store, &[]);
        assert!(endpoints.is_empty());
        assert_eq!(
            store.read_index.load(std::sync::atomic::Ordering::Relaxed),
            reads_before
        );

        // …but one allocation with upstreams does put the store to work.
        let consumer = consumer_allocation(2, "consumer", "web");
        let endpoints = build_upstream_endpoints(&store, &[plain, consumer]);
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].allocation_id, 2);
        assert_eq!(endpoints[0].service, "web");
        assert_eq!(endpoints[0].endpoints.len(), 1);
        assert!(
            store.read_index.load(std::sync::atomic::Ordering::Relaxed) > reads_before,
            "resolving an upstream must read the store"
        );
        Ok(())
    }

    #[test]
    fn upstream_endpoints_are_healthy_only_and_memoized_per_service_and_consumer() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        seed_upstream_service(&store, "web", 18081, &[], true)?;
        seed_upstream_service(&store, "starting", 18082, &[], false)?;

        // A node running `count: N` of one client task asks for the same
        // (service, job) pair N times per heartbeat; it must be resolved once.
        let desired = vec![
            consumer_allocation(1, "consumer", "web"),
            consumer_allocation(2, "consumer", "web"),
            consumer_allocation(3, "consumer", "starting"),
        ];
        let reads_before = store.read_index.load(std::sync::atomic::Ordering::Relaxed);
        let endpoints = build_upstream_endpoints(&store, &desired);
        let reads = store.read_index.load(std::sync::atomic::Ordering::Relaxed) - reads_before;

        assert_eq!(endpoints.len(), 3);
        assert_eq!(endpoints[0].endpoints.len(), 1);
        assert_eq!(endpoints[1].endpoints.len(), 1);
        // No health snapshot yet => not healthy => not dialable.
        assert!(endpoints[2].endpoints.is_empty());
        assert_eq!(reads, 2, "each distinct upstream service is resolved once");

        let endpoint = &endpoints[0].endpoints[0];
        assert_eq!(endpoint.service, "web");
        assert_eq!(endpoint.port, 18081);
        assert!(endpoint.healthy);

        // Two *different* consuming jobs cannot share a memo entry: intentions
        // are evaluated per consumer, so each gets its own lookup.
        let desired = vec![
            consumer_allocation(1, "consumer-a", "web"),
            consumer_allocation(2, "consumer-a", "web"),
            consumer_allocation(3, "consumer-b", "web"),
        ];
        let reads_before = store.read_index.load(std::sync::atomic::Ordering::Relaxed);
        let endpoints = build_upstream_endpoints(&store, &desired);
        let reads = store.read_index.load(std::sync::atomic::Ordering::Relaxed) - reads_before;
        assert_eq!(endpoints.len(), 3);
        assert_eq!(reads, 2, "one lookup per (service, consuming job) pair");
        Ok(())
    }

    #[test]
    fn upstream_endpoints_apply_service_intentions() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        // No `allow` => open to everyone (the pre-connect behavior).
        seed_upstream_service(&store, "open", 18090, &[], true)?;
        // Explicit wildcard => also open to everyone.
        seed_upstream_service(&store, "star", 18091, &["*"], true)?;
        // Named consumers only.
        seed_upstream_service(&store, "gated", 18092, &["gateway"], true)?;

        let visible = |job_id: &str, service: &str| -> R<usize> {
            let desired = vec![consumer_allocation(1, job_id, service)];
            let endpoints = build_upstream_endpoints(&store, &desired);
            assert_eq!(endpoints.len(), 1, "one entry per declared upstream");
            Ok(endpoints[0].endpoints.len())
        };

        assert_eq!(visible("anyone", "open")?, 1);
        assert_eq!(visible("anyone", "star")?, 1);
        assert_eq!(visible("gateway", "gated")?, 1);
        // Denied consumers get an empty list, never an error.
        assert_eq!(visible("intruder", "gated")?, 0);
        Ok(())
    }

    #[test]
    fn a_corrupt_connect_payload_does_not_fail_the_heartbeat() -> R<()> {
        // The endpoint feed is additive; one unparseable payload must not stop
        // the node from reconciling its allocations (or resolving the others).
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        seed_upstream_service(&store, "web", 18095, &[], true)?;

        let corrupt = crate::structs::TaskAllocation {
            provisional: None,
            allocation_id: 1,
            job_id: "broken".into(),
            task_connect: "{not json".into(),
            ..Default::default()
        };
        let healthy = consumer_allocation(2, "consumer", "web");
        let endpoints = build_upstream_endpoints(&store, &[corrupt, healthy]);

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].allocation_id, 2);
        assert_eq!(endpoints[0].endpoints.len(), 1);
        Ok(())
    }

    #[test]
    fn a_failing_service_lookup_does_not_fail_the_heartbeat() -> R<()> {
        // A store-level failure is the other half of the additive contract: if
        // it propagated, `heartbeat` would return Internal and the node would
        // stop starting, stopping and health-reporting allocations entirely —
        // over a connect-only problem. Omitting the entry instead leaves the
        // forwarder on its last known good endpoints (the worker only applies
        // upstreams present in the reply).
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        seed_upstream_service(&store, "broken", 18096, &[], true)?;
        seed_upstream_service(&store, "fine", 18097, &[], true)?;

        // Make `service_endpoints(Some("broken"))` fail the way a bad row does:
        // an allocation network that will not deserialize.
        {
            let conn = store.write_connection()?;
            conn.execute(
                "UPDATE allocation SET network = 'not json' WHERE task_config_id IN
                 (SELECT id FROM task_config WHERE name = 'broken');",
                [],
            )?;
        }
        assert!(
            store.service_endpoints(Some("broken")).is_err(),
            "test setup must actually break the lookup"
        );

        let desired = vec![
            consumer_allocation(1, "consumer", "broken"),
            consumer_allocation(2, "consumer", "fine"),
        ];
        let endpoints = build_upstream_endpoints(&store, &desired);

        assert_eq!(endpoints.len(), 1, "the failing upstream is omitted");
        assert_eq!(endpoints[0].allocation_id, 2);
        assert_eq!(endpoints[0].service, "fine");
        assert_eq!(endpoints[0].endpoints.len(), 1);
        Ok(())
    }

    //
    // KEYED-MODE INSTANCE RPCs (see docs/stateful-workloads.md)
    //

    fn keyed_task(name: &str, group: &str, require_id: bool) -> TaskConfig {
        TaskConfig {
            group: group.into(),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Keyed,
                require_id,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    driver: None,
                    mode: None,
                    params: None,
                    path: "/workspace".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                }),
                envelope: None,
                promote: None,
            }),
            ..TaskConfig::new(name, "noop")
        }
    }

    fn submit_keyed_job(store: &Store, name: &str, tasks: Vec<TaskConfig>) -> R<String> {
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks,
            version: None,
        };
        store.handle_job_submit(&job, &[submission()])?;
        let job_config_id = store.get_job_config(name)?.id();
        Eval {
            id: format!("{name}-submit"),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: name.into(),
            job_config_id: job_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        }
        .handle(store)?;
        Ok(job_config_id)
    }

    /// Handle (and complete) all pending evals of `kind` for `job`, returning
    /// how many were processed — stands in for the background eval workers.
    fn handle_pending_evals(store: &Store, kind: &str, job: &str) -> R<usize> {
        let mut handled = 0;
        for eval in store.get_evals()? {
            if eval.kind == kind && eval.job_id == job && eval.status == "pending" {
                eval.handle(store)?;
                store.complete_eval(&eval.id, true)?;
                handled += 1;
            }
        }
        Ok(handled)
    }

    #[test]
    fn keyed_instance_rpc_lifecycle_create_place_kill_replace_destroy() -> R<()> {
        use crate::structs::InstanceState;

        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        let manager = ManagerDaemonService::new(store.clone());
        let job_id = "sandbox-rpc";
        submit_keyed_job(
            &store,
            job_id,
            vec![keyed_task("sandbox", "default", false)],
        )?;
        // Keyed task with zero instances: the submit eval placed nothing.
        assert!(store.allocs_for_job(job_id)?.is_empty());

        let runtime = tokio::runtime::Runtime::new()?;

        // Create -> the eval places one allocation bound to the new id.
        let reply = runtime
            .block_on(manager.create_instance(Request::new(CreateInstanceRequest {
                job_id: job_id.into(),
                group: None,
                task: None,
                instance_id: Some("user-a".into()),
                idempotency_key: None,
                from_snapshot: None,
            })))
            .map_err(|err| anyhow!(err))?
            .into_inner();
        assert_eq!(reply.instance_id, "user-a");
        assert!(!reply.deduplicated);
        assert_eq!(handle_pending_evals(&store, "job_reschedule", job_id)?, 1);

        let allocations = store.allocs_for_job(job_id)?;
        assert_eq!(allocations.len(), 1);
        assert_eq!(allocations[0].instance_id.as_deref(), Some("user-a"));
        let placed = store.get_instance("user-a")?.context("instance")?;
        assert_eq!(placed.state, InstanceState::Active);
        let original_handle = placed.storage_handle.clone();
        assert!(original_handle.is_some());

        // Kill the allocation: the replacement reuses id + storage handle.
        store.remove_allocation_by_id(allocations[0].id)?;
        store.enqueue_eval(
            "job_reschedule",
            Some(job_id),
            Some(&store.get_job_config(job_id)?.id()),
            None,
            None,
        )?;
        handle_pending_evals(&store, "job_reschedule", job_id)?;
        let replacement = store.allocs_for_job(job_id)?;
        assert_eq!(replacement.len(), 1);
        assert_eq!(replacement[0].instance_id.as_deref(), Some("user-a"));
        let rebound = store.get_instance("user-a")?.context("instance")?;
        assert_eq!(rebound.state, InstanceState::Active);
        assert_eq!(rebound.storage_handle, original_handle);

        // Destroy: allocation stopped, instance tombstoned, handle dropped.
        let reply = runtime
            .block_on(
                manager.destroy_instance(Request::new(DestroyInstanceRequest {
                    instance_id: "user-a".into(),
                    idempotency_key: None,
                })),
            )
            .map_err(|err| anyhow!(err))?
            .into_inner();
        assert_eq!(reply.message, "Destroyed instance user-a");
        handle_pending_evals(&store, "job_reschedule", job_id)?;
        assert!(store.allocs_for_job(job_id)?.is_empty());
        let tombstone = store.get_instance("user-a")?.context("tombstone")?;
        assert_eq!(tombstone.state, InstanceState::Terminated);
        assert_eq!(tombstone.storage_handle, None);

        // Destroying again is a no-op, not an error.
        let reply = runtime
            .block_on(
                manager.destroy_instance(Request::new(DestroyInstanceRequest {
                    instance_id: "user-a".into(),
                    idempotency_key: None,
                })),
            )
            .map_err(|err| anyhow!(err))?
            .into_inner();
        assert_eq!(reply.message, "Instance user-a already terminated");

        // Never-recycle: re-creating the destroyed id is rejected ...
        let status = runtime
            .block_on(manager.create_instance(Request::new(CreateInstanceRequest {
                job_id: job_id.into(),
                group: None,
                task: None,
                instance_id: Some("user-a".into()),
                idempotency_key: None,
                from_snapshot: None,
            })))
            .expect_err("tombstoned id must be rejected");
        assert_eq!(status.code(), tonic::Code::AlreadyExists);

        // ... while a fresh (server-minted) instance gets a different id and
        // a FRESH storage handle after placement.
        let reply = runtime
            .block_on(manager.create_instance(Request::new(CreateInstanceRequest {
                job_id: job_id.into(),
                group: None,
                task: None,
                instance_id: None,
                idempotency_key: None,
                from_snapshot: None,
            })))
            .map_err(|err| anyhow!(err))?
            .into_inner();
        assert_ne!(reply.instance_id, "user-a");
        handle_pending_evals(&store, "job_reschedule", job_id)?;
        let minted = store
            .get_instance(&reply.instance_id)?
            .context("minted instance")?;
        assert_eq!(minted.state, InstanceState::Active);
        assert!(minted.storage_handle.is_some());
        assert_ne!(
            minted.storage_handle, original_handle,
            "a new sandbox must never inherit a dead one's storage"
        );
        Ok(())
    }

    #[test]
    fn create_instance_validates_job_task_resolution_and_id_policy() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        let manager = ManagerDaemonService::new(store.clone());
        let runtime = tokio::runtime::Runtime::new()?;
        let create = |job: &str, group: Option<&str>, task: Option<&str>, id: Option<&str>| {
            runtime.block_on(manager.create_instance(Request::new(CreateInstanceRequest {
                job_id: job.into(),
                group: group.map(String::from),
                task: task.map(String::from),
                instance_id: id.map(String::from),
                idempotency_key: None,
                from_snapshot: None,
            })))
        };

        // Unknown job.
        let status = create("ghost", None, None, None).expect_err("unknown job");
        assert_eq!(status.code(), tonic::Code::NotFound);

        // Job without a keyed task.
        submit_keyed_job(&store, "plain", vec![TaskConfig::new("web", "noop")])?;
        let status = create("plain", None, None, None).expect_err("no keyed task");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        // Two keyed tasks: ambiguous unless disambiguated by group/task.
        submit_keyed_job(
            &store,
            "multi",
            vec![
                keyed_task("sandbox", "sandboxes", false),
                keyed_task("runner", "runners", true),
            ],
        )?;
        let status = create("multi", None, None, None).expect_err("ambiguous keyed task");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("sandboxes/sandbox"));
        assert!(status.message().contains("runners/runner"));

        // Group filter resolves; the server-minted id lands on that task.
        let reply = create("multi", Some("sandboxes"), None, None)
            .map_err(|err| anyhow!(err))?
            .into_inner();
        let instance = store
            .get_instance(&reply.instance_id)?
            .context("created instance")?;
        assert_eq!(instance.group, "sandboxes");
        assert_eq!(instance.task_name, "sandbox");

        // requireId: the id is required ...
        let status = create("multi", None, Some("runner"), None).expect_err("caller id required");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("requireId"));
        // ... and an explicit one is accepted.
        let reply = create("multi", None, Some("runner"), Some("tenant-1"))
            .map_err(|err| anyhow!(err))?
            .into_inner();
        assert_eq!(reply.instance_id, "tenant-1");

        // Explicit ids are accepted without requireId too ...
        let reply = create("multi", Some("sandboxes"), None, Some("explicit.id_OK-1"))
            .map_err(|err| anyhow!(err))?
            .into_inner();
        assert_eq!(reply.instance_id, "explicit.id_OK-1");
        // ... but the charset is always enforced.
        for bad in ["has space", "slash/y", "", "a:b"] {
            let result = create("multi", Some("sandboxes"), None, Some(bad));
            match bad {
                // Empty string is treated as absent => server mints instead.
                "" => assert!(result.is_ok()),
                _ => {
                    let status = result.expect_err("invalid charset");
                    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{bad:?}");
                }
            }
        }

        // Collision with a live id of another task is also rejected: ids are
        // unique cluster-wide.
        let status = create("multi", Some("sandboxes"), None, Some("tenant-1"))
            .expect_err("cluster-wide id collision");
        assert_eq!(status.code(), tonic::Code::AlreadyExists);

        // Stopped jobs cannot grow new instances.
        store.stop_job("multi")?;
        let status = create("multi", Some("sandboxes"), None, None).expect_err("stopped job");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        Ok(())
    }

    #[test]
    fn instance_rpcs_replay_idempotency_keys() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        let manager = ManagerDaemonService::new(store.clone());
        let job_id = "sandbox-idem";
        submit_keyed_job(
            &store,
            job_id,
            vec![keyed_task("sandbox", "default", false)],
        )?;
        let runtime = tokio::runtime::Runtime::new()?;

        // Create twice with the same key: one instance, replayed reply.
        let first = runtime
            .block_on(manager.create_instance(Request::new(CreateInstanceRequest {
                job_id: job_id.into(),
                group: None,
                task: None,
                instance_id: None,
                idempotency_key: Some("create-1".into()),
                from_snapshot: None,
            })))
            .map_err(|err| anyhow!(err))?
            .into_inner();
        let second = runtime
            .block_on(manager.create_instance(Request::new(CreateInstanceRequest {
                job_id: job_id.into(),
                group: None,
                task: None,
                instance_id: None,
                idempotency_key: Some("create-1".into()),
                from_snapshot: None,
            })))
            .map_err(|err| anyhow!(err))?
            .into_inner();
        assert!(!first.deduplicated);
        assert!(second.deduplicated);
        assert_eq!(first.instance_id, second.instance_id);
        assert_eq!(store.list_instances(job_id)?.len(), 1);

        // Destroy twice with the same key: replayed reply, still one tombstone.
        let first = runtime
            .block_on(
                manager.destroy_instance(Request::new(DestroyInstanceRequest {
                    instance_id: second.instance_id.clone(),
                    idempotency_key: Some("destroy-1".into()),
                })),
            )
            .map_err(|err| anyhow!(err))?
            .into_inner();
        let replay = runtime
            .block_on(
                manager.destroy_instance(Request::new(DestroyInstanceRequest {
                    instance_id: second.instance_id.clone(),
                    idempotency_key: Some("destroy-1".into()),
                })),
            )
            .map_err(|err| anyhow!(err))?
            .into_inner();
        assert!(!first.deduplicated);
        assert!(replay.deduplicated);
        assert_eq!(first.message, replay.message);
        Ok(())
    }

    #[test]
    fn list_and_get_instance_rpcs_filter_and_round_trip() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;
        let manager = ManagerDaemonService::new(store.clone());
        let job_id = "sandbox-list";
        submit_keyed_job(
            &store,
            job_id,
            vec![
                keyed_task("sandbox", "sandboxes", false),
                keyed_task("runner", "runners", false),
            ],
        )?;
        store.create_keyed_instance(job_id, "sandboxes", "sandbox", Some("s-1"))?;
        store.create_keyed_instance(job_id, "sandboxes", "sandbox", Some("s-2"))?;
        store.create_keyed_instance(job_id, "runners", "runner", Some("r-1"))?;

        let runtime = tokio::runtime::Runtime::new()?;

        let all = runtime
            .block_on(manager.list_instances(Request::new(ListInstancesRequest {
                job_id: job_id.into(),
                group: None,
                ..Default::default()
            })))
            .map_err(|err| anyhow!(err))?
            .into_inner();
        assert_eq!(all.instances.len(), 3);

        let filtered = runtime
            .block_on(manager.list_instances(Request::new(ListInstancesRequest {
                job_id: job_id.into(),
                group: Some("sandboxes".into()),
                ..Default::default()
            })))
            .map_err(|err| anyhow!(err))?
            .into_inner();
        let ids: Vec<&str> = filtered
            .instances
            .iter()
            .map(|instance| instance.instance_id.as_str())
            .collect();
        assert_eq!(ids, vec!["s-1", "s-2"]);
        assert!(filtered
            .instances
            .iter()
            .all(|instance| instance.mode == "keyed" && instance.state == "pending"));

        let fetched = runtime
            .block_on(manager.get_instance(Request::new(GetInstanceRequest {
                instance_id: "r-1".into(),
            })))
            .map_err(|err| anyhow!(err))?
            .into_inner();
        let instance = fetched.instance.context("instance present")?;
        assert_eq!(instance.job_id, job_id);
        assert_eq!(instance.group, "runners");
        assert_eq!(instance.task, "runner");
        assert_eq!(instance.slot_index, None);

        let status = runtime
            .block_on(manager.get_instance(Request::new(GetInstanceRequest {
                instance_id: "missing".into(),
            })))
            .expect_err("unknown instance");
        assert_eq!(status.code(), tonic::Code::NotFound);
        Ok(())
    }

    /// Direct exec (docs/exec-and-actions.md, "Direct exec"): a ticket is
    /// minted for exactly the callers `ExecAllocation` would admit for the
    /// same target, and the ticket names what was authorized.
    #[tokio::test]
    async fn mint_exec_ticket_admits_exactly_what_exec_allocation_admits_test() -> R<()> {
        use crate::structs::acl::{AclIdentity, AclRule, AclSource};
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        run_jobspec(
            &manager,
            json!({
                "name": "shell",
                "task": {
                    "name": "main",
                    "runtime": "exec",
                    "config": { "command": ["sleep", "600"] },
                    "actions": [{ "name": "flush", "command": ["echo", "flushed"] }]
                }
            }),
        )
        .await?;
        store.insert_node(NodeMsg {
            id: "node-1".into(),
            pool: "default".into(),
            status: "active".into(),
            availability: "available".into(),
            config_attrs: json!({ "workerAddress": "https://10.0.3.17:4848" }).to_string(),
            fingerprint_attrs: "{}".into(),
            dynamic_attrs: json!({
                "protocolVersion": crate::manager::worker_plane::MIN_WORKER_PLANE_PROTOCOL
            })
            .to_string(),
            health_checks: "{}".into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
            heartbeat_source: String::new(),
        })?;
        let config = store.get_job_config("shell")?.id();
        let (task_id, _) = store
            .get_task_configs_for_job_config(&config)?
            .into_iter()
            .next()
            .context("task config")?;
        let alloc = store.place_alloc("node-1", task_id)?;

        let issuer = crate::manager::workload_identity::WorkloadIdentityState::initialize(
            &store,
            &crate::structs::WorkloadIdentityConfigFull {
                enabled: true,
                ..Default::default()
            },
            "http://127.0.0.1:4747",
            std::time::SystemTime::now(),
        )?;
        let limits = crate::structs::ExecTicketsConfigFull::default();

        let identity = |rules: Value| AclIdentity {
            principal: "tester".into(),
            accessor_id: Some("acc-1".into()),
            is_management: false,
            is_worker: false,
            policies: vec!["p".into()],
            rules: serde_json::from_value::<Vec<AclRule>>(rules).expect("rules"),
            source: AclSource::Token,
            expires_at: None,
            audit_only: false,
        };
        let exec_host = identity(json!([
            { "resources": ["exec"], "actions": ["write"], "names": ["shell"], "host": true }
        ]));
        let exec_no_host = identity(json!([
            { "resources": ["exec"], "actions": ["write"], "names": ["shell"] }
        ]));
        let action_only = identity(json!([
            { "resources": ["action"], "actions": ["write"], "names": ["shell"], "actionNames": ["flush"] }
        ]));
        let other_job = identity(json!([
            { "resources": ["exec"], "actions": ["write"], "names": ["other"], "host": true }
        ]));
        let reader = identity(json!([{ "resources": ["job"], "actions": ["read"] }]));

        let command = MintExecTicketRequest {
            allocation_id: alloc.id,
            command: vec!["/bin/sh".into()],
            tty: true,
            ..Default::default()
        };
        let action = MintExecTicketRequest {
            allocation_id: alloc.id,
            action: "flush".into(),
            ..Default::default()
        };
        for (name, who) in [
            ("exec with host", &exec_host),
            ("exec without host", &exec_no_host),
            ("action only", &action_only),
            ("another job", &other_job),
            ("reader", &reader),
        ] {
            for request in [&command, &action] {
                let start = crate::structs::ExecStart {
                    allocation_id: request.allocation_id,
                    command: request.command.clone(),
                    action: request.action.clone(),
                    tty: request.tty,
                    ..Default::default()
                };
                let proxied = crate::manager::exec::resolve_for_test(&store, who, &start);
                let minted = crate::manager::exec::mint_exec_ticket(
                    &store,
                    who,
                    &issuer,
                    &limits,
                    request,
                    std::time::SystemTime::now(),
                );
                assert_eq!(
                    proxied.is_ok(),
                    minted.is_ok(),
                    "{name} / {:?}: proxied {:?}, minted {:?}",
                    request.action,
                    proxied.err(),
                    minted.err()
                );
                if let Err(status) = minted {
                    assert_eq!(status.code(), tonic::Code::PermissionDenied, "{name}");
                }
            }
        }

        // The ticket names what was authorized, on the allocation's node.
        let reply = crate::manager::exec::mint_exec_ticket(
            &store,
            &action_only,
            &issuer,
            &limits,
            &action,
            std::time::SystemTime::now(),
        )?;
        assert_eq!(reply.node, "node-1");
        assert_eq!(reply.worker_address, "https://10.0.3.17:4848");
        let claims = crate::structs::exec_ticket::ExecTicketClaims::from_claims(&issuer.verify(
            &reply.ticket,
            crate::structs::exec_ticket::EXEC_TICKET_AUDIENCE,
        )?)?;
        assert_eq!(claims.allocation_id, alloc.id);
        assert_eq!(claims.action.as_deref(), Some("flush"));
        assert_eq!(
            claims.argv,
            Some(vec!["echo".to_string(), "flushed".to_string()])
        );
        assert_eq!(claims.sub, "accessor:acc-1");
        assert_eq!(claims.max_session, 3600);

        // A redeem window above the manager's ceiling is refused by name.
        let status = crate::manager::exec::mint_exec_ticket(
            &store,
            &exec_host,
            &issuer,
            &limits,
            &MintExecTicketRequest {
                ttl: "1h".into(),
                ..command.clone()
            },
            std::time::SystemTime::now(),
        )
        .expect_err("above the ceiling");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status.message().contains("exec.tickets"),
            "{}",
            status.message()
        );

        // A session never outlives the credential that minted it.
        let expiring = AclIdentity {
            expires_at: Some(
                (time::OffsetDateTime::now_utc() + time::Duration::minutes(5))
                    .format(&time::format_description::well_known::Rfc3339)?,
            ),
            ..exec_host.clone()
        };
        let reply = crate::manager::exec::mint_exec_ticket(
            &store,
            &expiring,
            &issuer,
            &limits,
            &command,
            std::time::SystemTime::now(),
        )?;
        assert!(
            reply.max_session_seconds <= 300,
            "{}",
            reply.max_session_seconds
        );
        Ok(())
    }
}

impl ManagerDaemonService {
    /// Mint a boot ticket (`IssueBootTicketRequest.scope = "boot"`).
    fn mint_boot_ticket(
        &self,
        state: &crate::manager::workload_identity::WorkloadIdentityState,
        identity: &AclIdentity,
        req: &IssueBootTicketRequest,
        job_config_id: &str,
        task: &TaskConfig,
        ttl: std::time::Duration,
    ) -> Result<crate::manager::edge_boot::MintedTicket, Status> {
        let ticket_request = crate::manager::edge_boot::TicketRequest {
            job: req.job.clone(),
            task: req.task.clone(),
            job_config_id: job_config_id.to_string(),
            instance_id: req.instance_id.clone(),
            instance_prefix: req.instance_prefix.clone(),
            pool: req.pool.clone(),
            gate: crate::structs::edge_boot::Gate::from_wire(req.gate),
            ttl,
            uses: if req.uses == 0 { 1 } else { req.uses },
            accessor_id: identity.accessor_id.clone().unwrap_or_default(),
        };
        crate::manager::edge_boot::validate_ticket_request(&ticket_request, task, &self.edge_boot)
            .map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        let jti = crate::structs::edge_boot::new_ticket_id();
        crate::manager::edge_boot::mint(state, &ticket_request, &jti, std::time::SystemTime::now())
            .map_err(|e| Status::internal(format!("failed to mint boot ticket: {e:#}")))
    }
}
