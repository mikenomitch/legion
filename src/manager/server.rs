// gRPC handler signatures are fixed by the generated `Manager` trait, so the
// large `tonic::Status` error variant cannot be boxed away here.
#![allow(clippy::result_large_err)]

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use crate::prelude::*;

use crate::manager::acl::{self, Authenticator};
use crate::manager::dispatcher::build_task_allocation;
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
    DestroyInstanceOutcome, JobKvReference, JobSubmitOutcome, StartInstanceOutcome,
    StopInstanceOutcome, Store,
};
use crate::structs::acl::{AclAction, AclIdentity, AclResource, AclScope};
use crate::structs::manager_server::Manager;
use crate::structs::{
    validate_keyed_instance_id, worker_client::WorkerClient, AcquireKvLockReply,
    AcquireKvLockRequest, AdvanceRolloutReply, AdvanceRolloutRequest, AllocationHealth,
    AllocationLog, CreateInstanceReply, CreateInstanceRequest, CreateInstancesReply,
    CreateInstancesRequest, DeleteKvReply, DeleteKvRequest, DeleteSavedStateQueryReply,
    DeleteSavedStateQueryRequest, DestroyInstanceReply, DestroyInstanceRequest, DestroyJobReply,
    DestroyJobRequest, FailedAllocation, GetAllocationLogsReply, GetAllocationLogsRequest,
    GetArtifactCredentialReply, GetArtifactCredentialRequest, GetInstanceReply, GetInstanceRequest,
    GetJobInfoReply, GetJobInfoRequest, GetJobSpecByConfigRequest, GetJobSpecReply,
    GetJobSpecRequest, GetKvLockReply, GetKvLockRequest, GetKvReply, GetKvRequest, GetLedgerReply,
    GetLedgerRequest, GetManagerConfigsReply, GetManagerConfigsRequest, GetNodeReply,
    GetNodeRequest, GetNodesReply, GetNodesRequest, GetObservabilityReply, GetObservabilityRequest,
    GetRaftJoinTokenReply, GetRaftJoinTokenRequest, GetRaftStatusReply, GetRaftStatusRequest,
    GetRolloutReply, GetRolloutRequest, GetRolloutStatusReply, GetRolloutStatusRequest,
    GetStateStoreSchemaReply, GetStateStoreSchemaRequest, GetTaskLogsRequest, HealthCheckReply,
    HealthCheckRequest, HeartbeatReply, HeartbeatRequest, Instance, InstanceInfo, InstanceMode,
    InstanceState, IssueRaftCommandReply, IssueRaftCommandRequest, IssueWorkloadIdentityTokenReply,
    IssueWorkloadIdentityTokenRequest, JobConfig, JobSpecHistoryItem, KvEntry, KvInjectionAction,
    KvValueType, LedgerNode, ListEventsReply, ListEventsRequest, ListInstancesReply,
    ListInstancesRequest, ListJobSpecHistoryReply, ListJobSpecHistoryRequest, ListJobsReply,
    ListJobsRequest, ListKvReply, ListKvRequest, ListRaftHistoryReply, ListRaftHistoryRequest,
    ListRolloutEventsReply, ListRolloutEventsRequest, ListRolloutsReply, ListRolloutsRequest,
    ListSavedStateQueriesReply, ListSavedStateQueriesRequest, ListServicesReply,
    ListServicesRequest, Node, NodeMsg, PatchKvReply, PatchKvRequest, PlanJobReply, PlanJobRequest,
    PlannedAllocation, PutKvReply, PutKvRequest, QueryStateStoreReply, QueryStateStoreRequest,
    RaftHistoryEvent, RaftStatus, ReleaseKvLockReply, ReleaseKvLockRequest, RenewKvLockReply,
    RenewKvLockRequest, ReplaceInstanceReply, ReplaceInstanceRequest, ResolveServiceReply,
    ResolveServiceRequest, RestartWorkerReply, RestartWorkerRequest, RolloutAllocationStatus,
    RolloutEvent, RolloutGroupStatus, RolloutProgression, RolloutReplacement, RunJobReply,
    RunJobRequest, SavedStateQuery, SchedulerEvent, ServiceEndpoint, ServiceSummary,
    StartInstanceReply, StartInstanceRequest, StopInstanceReply, StopInstanceRequest, StopJobReply,
    StopJobRequest, StopRolloutReply, StopRolloutRequest, StreamEventsRequest, Submission,
    TaskConfig, TaskOnUnhealthyConfig, UpdateNodeDynamicReply, UpdateNodeDynamicRequest,
    UpsertSavedStateQueryReply, UpsertSavedStateQueryRequest, WorkerConfig, WorkerPreRegisterReply,
    WorkerPreRegisterRequest, WorkerRegisterReply, WorkerRegisterRequest,
};
use crate::structs::{
    AuthMethodInfo, BootstrapAclReply, BootstrapAclRequest, CreateAclTokenReply,
    CreateAclTokenRequest, CreateJoinTokenReply, CreateJoinTokenRequest, DeleteAclPolicyReply,
    DeleteAclPolicyRequest, DeleteAclTokenReply, DeleteAclTokenRequest, DeleteAclUserReply,
    DeleteAclUserRequest, DeleteJoinTokenReply, DeleteJoinTokenRequest, GetAclPolicyReply,
    GetAclPolicyRequest, GetAuthMethodsReply, GetAuthMethodsRequest, ListAclPoliciesReply,
    ListAclPoliciesRequest, ListAclTokensReply, ListAclTokensRequest, ListAclUsersReply,
    ListAclUsersRequest, ListJoinTokensReply, ListJoinTokensRequest, LoginReply, LoginRequest,
    UpsertAclPolicyReply, UpsertAclPolicyRequest, UpsertAclUserReply, UpsertAclUserRequest,
    WhoAmIReply, WhoAmIRequest,
};
use crate::task_plugins::validate_job_config;
use crate::tracing_util::{o11y_enabled, read_recent_observability};

use time::{Duration as TimeDuration, OffsetDateTime};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

fn map_store_error(action: &str, err: anyhow::Error) -> Status {
    error!("{action}: {:?}", err);

    let Some(storage_error) = StorageError::from_anyhow(&err) else {
        return Status::internal(action);
    };

    let message = format!("{action}: {storage_error}");
    match storage_error {
        StorageError::NotFound { .. } => Status::not_found(message),
        StorageError::Conflict { .. } => Status::already_exists(message),
        StorageError::InvalidInput { .. } => Status::invalid_argument(message),
        StorageError::ForeignKey { .. } => Status::failed_precondition(message),
        StorageError::DatabaseBusy | StorageError::DatabaseLocked => Status::unavailable(message),
        StorageError::DatabaseReadOnly => Status::failed_precondition(message),
        StorageError::DatabaseCorrupt
        | StorageError::DatabaseIo { .. }
        | StorageError::Sqlite { .. } => Status::internal(message),
    }
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
fn map_instance_info(store: &Store, instance: Instance) -> InstanceInfo {
    let detail = instance_loss_detail(store, &instance);
    InstanceInfo {
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
    }
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
    let normalize = |value: Option<String>| {
        value.and_then(|value| {
            let trimmed = value.trim().to_string();
            (!trimmed.is_empty()).then_some(trimmed)
        })
    };
    SchedulerEventFilter {
        // Explicit 0 means "from the very beginning"; only reject negatives.
        after_id: after_id.filter(|id| *id >= 0),
        limit,
        kind: normalize(kind),
        job_id: normalize(job_id),
        node_id: normalize(node_id),
        level: normalize(level),
    }
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
                let entry = json!({
                    "path": file.path,
                    "mode": file.mode,
                    "content": rendered
                });
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
        KvInjectionAction::Restart => 1,
        KvInjectionAction::Rollout => 2,
        KvInjectionAction::Fail => 3,
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
}

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
        }
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

    /// Join gate for worker registration: when ACLs are enforced, callers
    /// prove membership with a join token secret unless they already hold a
    /// worker or management credential. Audit-only mode logs failures but
    /// lets registration proceed (the migration ramp for existing fleets).
    fn verify_join_for_registration(
        &self,
        identity: &AclIdentity,
        join_token: Option<&str>,
    ) -> Result<(), Status> {
        if !self.auth.enabled || identity.is_management || identity.is_worker {
            return Ok(());
        }
        match self.auth.verify_join_secret(&self.store, join_token) {
            Ok(_) => Ok(()),
            Err(status) if self.auth.audit_only => {
                warn!(
                    target: "acl_audit",
                    resource = "worker-plane",
                    action = "register",
                    enforced = false,
                    "join token verification failed (audit-only): {}",
                    status.message()
                );
                Ok(())
            }
            Err(status) => Err(status),
        }
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

    fn cancel_active_rollouts_for_job(&self, job_id: &str) -> Result<(), Status> {
        let active_rollouts = self
            .store
            .list_rollouts(Some(job_id), true)
            .map_err(|e| map_store_error("Error listing active rollouts", e))?;

        for rollout in active_rollouts {
            self.store
                .update_rollout_status(
                    rollout.id,
                    "cancelled",
                    false,
                    Some("Rollout superseded by newer job update"),
                )
                .map_err(|e| map_store_error("Error cancelling superseded rollout", e))?;
            self.store
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
                registry.load_plugin(&task.kind, false)?;
                let plugin = registry
                    .plugin(&task.kind)
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
    /// surface them to); the next KV write retriggers reconciliation from
    /// current state.
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
                    "Debounced KV reconciliation failed"
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
            .remove_allocation_by_id(allocation.id)
            .map_err(|e| map_store_error("Error removing stale allocation", e))?;
    }

    Ok(())
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

        match action {
            KvInjectionAction::None => {}
            KvInjectionAction::Restart => {
                store
                    .clear_job_failed_status(&job_id)
                    .map_err(|e| map_store_error("Error clearing failed job status", e))?;
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
        let mut subs = inner.submissions;
        let priority = inner.priority.unwrap_or(self.store.default_eval_priority);
        let idempotency_key = inner.idempotency_key.unwrap_or_default();

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

        if !subs.iter().any(|sub| sub.path == "job.rendered.json") {
            subs.push(Submission {
                path: "job.rendered.json".into(),
                content: js.clone(),
                is_primary: false,
            });
        }

        let (mut config, kv_refs) =
            resolve_jobspec_kv_references(&self.store, &js).map_err(|e| {
                error!("Error parsing jobspec: {:?}", e);
                Status::invalid_argument(format!("Error parsing jobspec: {e}"))
            })?;
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job(&config.name),
        )?;

        let force_reschedule = inner.force_reschedule;
        validate_job_config(&config).map_err(|e| {
            error!("Error validating task configs: {:?}", e);
            Status::invalid_argument(format!("Error validating task configs: {e}"))
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
        let submit_outcome = self
            .store
            .handle_job_submit(&config, &subs)
            .map_err(|e| map_store_error("Error running job", e))?;

        // Resubmitting revives a previously stopped job: clear the stopped
        // status so the instance-management RPCs accept it again. Stopped
        // keyed instances stay stopped — each needs an explicit
        // `instance start` (see docs/stateful-workloads.md).
        self.store
            .clear_job_stopped_status(&config.name)
            .map_err(|e| map_store_error("Error clearing stopped job status", e))?;

        // A manual `job run` is also the recovery path for a job stuck
        // `failed` by a missing KV dependency under onChange:none (which never
        // auto-recovers). Clearing the failed status here re-runs the job's
        // allocations through the normal submit/eval flow with the latest KV
        // state (see docs/kv-store.md).
        self.store
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
                } else {
                    (None, "Job config unchanged; no update needed")
                }
            }
            JobSubmitOutcome::Created { .. } => (Some("job_submit"), "Job submit success"),
            JobSubmitOutcome::Updated { .. } => {
                let previous_job_config_id = self
                    .store
                    .previous_job_config_id(&job_config_id)
                    .map_err(|e| map_store_error("Error loading previous job config", e))?;
                let is_manual = create_rollout_for_update(
                    &self.store,
                    &config.name,
                    previous_job_config_id.as_deref(),
                    &job_config_id,
                    &config,
                    "Error creating rollout",
                )?;

                if is_manual {
                    (None, "Rollout created; waiting for manual start")
                } else {
                    (Some("rollout_tick"), "Rollout started")
                }
            }
        };

        if let Some(kind) = eval_kind {
            self.store
                .enqueue_eval(
                    kind,
                    Some(&config.name),
                    Some(&job_config_id),
                    None,
                    Some(priority),
                )
                .map_err(|e| map_store_error("Error running job", e))?;
        } else if seeded > 0 {
            // An unchanged resubmit normally needs no eval, but if it seeded
            // rows (possible when a previous submit failed between persisting
            // the config and seeding), the new instances still need placing.
            self.store
                .enqueue_eval(
                    "job_reschedule",
                    Some(&config.name),
                    Some(&job_config_id),
                    None,
                    Some(priority),
                )
                .map_err(|e| map_store_error("Error scheduling seeded instance placement", e))?;
        }

        self.store
            .idempotency_record(&idempotency_key, "run_job", message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;

        let reply = RunJobReply {
            message: message.into(),
            deduplicated: false,
        };

        Ok(Response::new(reply))
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
            AclScope::job(&request.get_ref().name),
        )?;
        let name = request.into_inner().name;

        let job = self
            .store
            .get_job(&name)
            .map_err(|e| map_store_error("Error getting job", e))?;

        let reply = GetJobInfoReply { job: Some(job) };

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
        let filter = scheduler_event_filter(
            request.after_id,
            request.limit,
            request.kind,
            request.job_id,
            request.node_id,
            request.level,
        );

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

    type StreamEventsStream = ReceiverStream<Result<SchedulerEvent, Status>>;

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
        let request = request.into_inner();
        let filter = scheduler_event_filter(
            request.after_id,
            None,
            request.kind,
            request.job_id,
            request.node_id,
            request.level,
        );

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

            loop {
                match live.recv().await {
                    Ok(event) => {
                        if event.id <= last_sent || !filter.matches(&event) {
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

        let (plan, task_lookup, message, matches_existing, same_content) = match existing_config {
            Some(existing) if existing.is_same_as(&config) => {
                let plan = scheduler
                    .plan_only(&existing.id(), PlanMode::Fresh)
                    .map_err(|e| {
                        error!("Error planning job {}: {:?}", existing.name, e);
                        Status::invalid_argument(format!("Error planning job: {e}"))
                    })?;
                let task_configs = self
                    .store
                    .get_task_configs_for_job_config(&existing.id())
                    .map_err(|e| map_store_error("Error getting task configs", e))?;
                let task_lookup = task_configs
                    .into_iter()
                    .map(|(id, task)| (id, task.name))
                    .collect::<HashMap<_, _>>();

                (
                    plan,
                    task_lookup,
                    "Job config unchanged; showing force-reschedule placements".to_string(),
                    true,
                    true,
                )
            }
            Some(_existing) => {
                let PlanConfigOutcome {
                    allocations,
                    task_lookup,
                } = scheduler
                    .plan_for_config(&config, PlanMode::Fresh, Some(&config.name))
                    .map_err(|e| {
                        error!("Error planning updated job {}: {:?}", config.name, e);
                        Status::invalid_argument(format!("Error planning job: {e}"))
                    })?;
                (
                    allocations,
                    task_lookup,
                    "Job config changed; showing updated placements".to_string(),
                    true,
                    false,
                )
            }
            None => {
                let PlanConfigOutcome {
                    allocations,
                    task_lookup,
                } = scheduler
                    .plan_for_config(&config, PlanMode::Fresh, None)
                    .map_err(|e| {
                        error!("Error planning new job {}: {:?}", config.name, e);
                        Status::invalid_argument(format!("Error planning job: {e}"))
                    })?;
                (
                    allocations,
                    task_lookup,
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

        let reply = PlanJobReply {
            allocations,
            message,
            matches_existing,
            same_content,
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
            AclScope::job(&request.get_ref().name),
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

        let reply = GetJobSpecReply { submissions };

        Ok(Response::new(reply))
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
            Some(name) => AclScope::job(name),
            None => AclScope::any(),
        };
        acl::enforce(&identity, AclResource::Job, AclAction::Read, &scope)?;

        let submissions = self
            .store
            .get_submissions_for_config(&config_id)
            .map_err(|e| map_store_error("Error getting submissions", e))?;

        Ok(Response::new(GetJobSpecReply { submissions }))
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
            AclScope::job(&request.get_ref().name),
        )?;
        let name = request.into_inner().name;
        let items = self
            .store
            .list_job_spec_history(&name)
            .map_err(|e| map_store_error("Error listing job spec history", e))?
            .into_iter()
            .map(|(config_id, created_at, version)| JobSpecHistoryItem {
                config_id,
                created_at,
                version,
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
        let sort_by = request.sort_by.unwrap_or_else(|| "created_at".to_string());
        let sort_desc = request
            .sort_order
            .as_deref()
            .unwrap_or("desc")
            .eq_ignore_ascii_case("desc");
        let (page, page_size) = parse_page(request.page, request.page_size);

        let mut jobs = self
            .store
            .get_jobs()
            .map_err(|e| map_store_error("Error getting jobs", e))?;
        jobs.retain(|job| {
            identity.allows(AclResource::Job, AclAction::Read, &AclScope::job(&job.name))
        });

        if let Some(query) = query {
            jobs.retain(|job| {
                job.id.to_lowercase().contains(&query)
                    || job.name.to_lowercase().contains(&query)
                    || job.status.to_lowercase().contains(&query)
            });
        }

        jobs.sort_by(|a, b| {
            let ordering = match sort_by.as_str() {
                "name" => a.name.cmp(&b.name),
                "status" => a.status.cmp(&b.status),
                _ => a.created_at.cmp(&b.created_at),
            };
            if sort_desc {
                ordering.reverse()
            } else {
                ordering
            }
        });

        let (jobs, total_count) = paginate(jobs, page, page_size);

        Ok(Response::new(ListJobsReply {
            jobs,
            total_count,
            page: page as u32,
            page_size: page_size as u32,
        }))
    }

    #[instrument(skip_all)]
    async fn stop_job(
        &self,
        request: Request<StopJobRequest>,
    ) -> Result<Response<StopJobReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::job(&request.get_ref().name),
        )?;
        let inner = request.into_inner();
        let name = inner.name;
        let priority = inner.priority.unwrap_or(self.store.default_eval_priority);
        let idempotency_key = inner.idempotency_key.unwrap_or_default();

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
    async fn destroy_job(
        &self,
        request: Request<DestroyJobRequest>,
    ) -> Result<Response<DestroyJobReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::job(&request.get_ref().name),
        )?;
        let inner = request.into_inner();
        let name = inner.name;
        let idempotency_key = inner.idempotency_key.unwrap_or_default();

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
        self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::job(&request.get_ref().job_id),
        )?;
        let inner = request.into_inner();
        let idempotency_key = inner.idempotency_key.unwrap_or_default();

        if let Some(instance_id) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(CreateInstanceReply {
                instance_id,
                deduplicated: true,
            }));
        }

        let config = keyed_job_config(&self.store, &inner.job_id)?;
        let task = resolve_keyed_task(
            &config,
            &inner.job_id,
            inner.group.as_deref(),
            inner.task.as_deref(),
        )?;

        // Id policy: idSource declares the default source; a valid explicit
        // key is always honored. See proto comments.
        let requested_id = inner.instance_id.as_deref().filter(|id| !id.is_empty());
        let id_source = task
            .stateful
            .as_ref()
            .and_then(|stateful| stateful.id_source.as_deref());
        match requested_id {
            Some(id) => validate_keyed_instance_id(id).map_err(Status::invalid_argument)?,
            None => {
                if id_source == Some("caller") {
                    return Err(Status::invalid_argument(format!(
                        "task {} declares stateful.idSource = \"caller\"; CreateInstance must \
                         supply instance_id",
                        task.name
                    )));
                }
            }
        }

        let instance = self
            .store
            .create_keyed_instance(&inner.job_id, &task.group, &task.name, requested_id)
            .map_err(|e| map_store_error("Error creating instance", e))?;

        // Place promptly: a reschedule plan adds exactly the missing keyed
        // allocations without disturbing live ones.
        self.store
            .enqueue_eval(
                "job_reschedule",
                Some(&inner.job_id),
                Some(&config.id()),
                None,
                Some(self.store.default_eval_priority),
            )
            .map_err(|e| map_store_error("Error scheduling instance placement", e))?;

        self.store
            .idempotency_record(&idempotency_key, "create_instance", &instance.instance_id)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;

        Ok(Response::new(CreateInstanceReply {
            instance_id: instance.instance_id,
            deduplicated: false,
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
        self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Write,
            AclScope::job(&request.get_ref().job_id),
        )?;
        let inner = request.into_inner();
        let idempotency_key = inner.idempotency_key.unwrap_or_default();

        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            // The recorded replay is the comma-joined created ids (the id
            // charset cannot contain commas, so the join is unambiguous).
            return Ok(Response::new(CreateInstancesReply {
                instance_ids: recorded
                    .split(',')
                    .filter(|id| !id.is_empty())
                    .map(str::to_string)
                    .collect(),
                deduplicated: true,
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

        // idSource "caller" requires every id to be caller-supplied;
        // server-minting (`count`) contradicts that policy.
        let id_source = task
            .stateful
            .as_ref()
            .and_then(|stateful| stateful.id_source.as_deref());
        if id_source == Some("caller") && inner.count > 0 {
            return Err(Status::invalid_argument(format!(
                "task {} declares stateful.idSource = \"caller\", so every id must be \
                 caller-supplied; count > 0 asks the server to mint ids — list explicit \
                 instance_ids instead",
                task.name
            )));
        }

        let created = self
            .store
            .create_keyed_instances(
                &inner.job_id,
                &task.group,
                &task.name,
                &inner.instance_ids,
                inner.count,
            )
            .map_err(|e| map_store_error("Error creating instances", e))?;

        // One placement eval covers the whole batch: a reschedule plan adds
        // exactly the missing keyed allocations without disturbing live ones.
        self.store
            .enqueue_eval(
                "job_reschedule",
                Some(&inner.job_id),
                Some(&config.id()),
                None,
                Some(self.store.default_eval_priority),
            )
            .map_err(|e| map_store_error("Error scheduling instance placement", e))?;

        self.store
            .idempotency_record(&idempotency_key, "create_instances", &created.join(","))
            .map_err(|e| map_store_error("Error recording idempotency", e))?;

        Ok(Response::new(CreateInstancesReply {
            instance_ids: created,
            deduplicated: false,
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
        let idempotency_key = inner.idempotency_key.unwrap_or_default();

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

        let outcome = self
            .store
            .stop_keyed_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error stopping instance", e))?;

        let message = match outcome {
            StopInstanceOutcome::Stopped { .. } => {
                // Reconcile promptly. The stop itself already removed the
                // allocation rows (workers stop on their next heartbeat); the
                // eval just confirms nothing else needs (re)placement.
                if let Ok(config) = self.store.get_job_config(&instance.job_id) {
                    self.store
                        .enqueue_eval(
                            "job_reschedule",
                            Some(&instance.job_id),
                            Some(&config.id()),
                            None,
                            Some(self.store.default_eval_priority),
                        )
                        .map_err(|e| map_store_error("Error scheduling reconcile", e))?;
                }
                format!("Stopped instance {}", inner.instance_id)
            }
            StopInstanceOutcome::AlreadyStopped => {
                format!("Instance {} already stopped", inner.instance_id)
            }
        };

        self.store
            .idempotency_record(&idempotency_key, "stop_instance", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;

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
        let idempotency_key = inner.idempotency_key.unwrap_or_default();

        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(StartInstanceReply {
                message,
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

        let outcome = self
            .store
            .start_keyed_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error starting instance", e))?;

        let message = match outcome {
            StartInstanceOutcome::Started => {
                // Place promptly, exactly like CreateInstance does.
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
                    .map_err(|e| map_store_error("Error scheduling instance placement", e))?;
                format!("Started instance {}", inner.instance_id)
            }
            StartInstanceOutcome::AlreadyRunning => {
                format!("Instance {} already running", inner.instance_id)
            }
        };

        self.store
            .idempotency_record(&idempotency_key, "start_instance", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;

        Ok(Response::new(StartInstanceReply {
            message,
            deduplicated: false,
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
        let idempotency_key = inner.idempotency_key.unwrap_or_default();

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

        let outcome = self
            .store
            .destroy_keyed_instance(&inner.instance_id)
            .map_err(|e| map_store_error("Error destroying instance", e))?;

        let message = match outcome {
            DestroyInstanceOutcome::Destroyed { .. } => {
                // Reconcile promptly. The destroy itself already removed the
                // allocation rows (workers stop on their next heartbeat); the
                // eval just confirms nothing else needs (re)placement.
                if let Ok(config) = self.store.get_job_config(&instance.job_id) {
                    self.store
                        .enqueue_eval(
                            "job_reschedule",
                            Some(&instance.job_id),
                            Some(&config.id()),
                            None,
                            Some(self.store.default_eval_priority),
                        )
                        .map_err(|e| map_store_error("Error scheduling reconcile", e))?;
                }
                format!("Destroyed instance {}", inner.instance_id)
            }
            DestroyInstanceOutcome::AlreadyTerminated => {
                format!("Instance {} already terminated", inner.instance_id)
            }
        };

        self.store
            .idempotency_record(&idempotency_key, "destroy_instance", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;

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
        let idempotency_key = inner.idempotency_key.unwrap_or_default();

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
            .remove_allocation_by_id(allocation.id)
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

        let instances = self
            .store
            .list_instances(&inner.job_id)
            .map_err(|e| map_store_error("Error listing instances", e))?
            .into_iter()
            .filter(|instance| group.as_deref().is_none_or(|group| instance.group == group))
            .map(|instance| map_instance_info(&self.store, instance))
            .collect();

        Ok(Response::new(ListInstancesReply { instances }))
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
            .map_err(|e| map_store_error("Error loading instance", e))?
            .ok_or_else(|| {
                Status::not_found(format!("instance {} not found", inner.instance_id))
            })?;
        acl::enforce(
            &identity,
            AclResource::Job,
            AclAction::Read,
            &AclScope::job(&instance.job_id),
        )?;

        Ok(Response::new(GetInstanceReply {
            instance: Some(map_instance_info(&self.store, instance)),
        }))
    }

    #[instrument(skip_all, fields(alloc_id, identity_name, node_id))]
    async fn issue_workload_identity_token(
        &self,
        request: Request<IssueWorkloadIdentityTokenRequest>,
    ) -> Result<Response<IssueWorkloadIdentityTokenReply>, Status> {
        // ACL: worker-plane endpoint (workers fetch tokens for allocations
        // placed on them; the manager already checks node ownership below).
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
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

        // AuthZ: a worker may only request tokens for allocations placed on
        // it. This is the same trust model as Heartbeat — workers are
        // admitted via join token and identified by their claimed node id;
        // there is no per-request worker credential today.
        // TODO(worker-authn): bind worker RPCs to a per-node credential
        // (mTLS or a join-token-derived bearer) so node_id is proven rather
        // than claimed; see the join-token trust model in src/structs/token.rs.
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

    #[instrument(skip_all)]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        // ACL: worker-plane endpoint. Requires the node token minted at
        // registration (or a management token) when ACLs are enforced.
        let identity = self.authenticate_acl(&request)?;
        acl::enforce_worker_plane(&identity)?;
        let req = request.into_inner();
        let id = req.id;

        self.store
            .update_node_heartbeat(id.clone())
            .map_err(|e| map_store_error("Error updating node heartbeat", e))?;

        if let Err(err) = handle_failed_allocations(
            &self.store,
            &self.allocation_failures,
            &id,
            &req.failed_allocations,
        ) {
            error!(node_id = %id, error = ?err, "Error handling failed allocations");
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

        let desired_allocations = build_desired_allocations(&self.store, &id, &req.allocation_ids)
            .map_err(|e| map_store_error("Error building desired allocations", e))?;

        let reply = HeartbeatReply {
            message: "ack".into(),
            desired_allocations,
        };

        Ok(Response::new(reply))
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
        self.verify_join_for_registration(&identity, request.get_ref().join_token.as_deref())?;
        let request = request.into_inner();
        let worker_type = request.worker_type;

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
        self.verify_join_for_registration(&identity, request.get_ref().join_token.as_deref())?;
        let request = request.into_inner();
        let dynamic_attrs = json!({}).to_string();
        let node_id = request.name.clone();

        let node_msg = NodeMsg {
            id: request.name,
            fingerprint_attrs: request.fingerprint_attrs,
            config_attrs: request.config_attrs,
            dynamic_attrs,
            status: "active".to_string(),
            availability: "available".to_string(),
            pool: request.pool,
            last_heartbeat: String::new(),
            health_checks: request.health_checks,
        };

        self.store
            .insert_node(node_msg)
            .map_err(|e| map_store_error("Error registering worker", e))?;

        let node_token = if self.auth.enabled {
            let secret = self.auth.mint_node_token(&self.store, &node_id)?;
            info!(node_id = %node_id, "Minted node token for registered worker");
            Some(secret)
        } else {
            None
        };

        let reply = WorkerRegisterReply {
            message: "Worker registered".into(),
            node_token,
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
        let identity = self.authenticate_acl(&request)?;
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

        let reply = GetNodeReply {
            node: Some(node.into()),
        };
        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn restart_worker(
        &self,
        request: Request<RestartWorkerRequest>,
    ) -> Result<Response<RestartWorkerReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
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

        let worker_address = worker_address_for_node(&node)
            .ok_or_else(|| Status::failed_precondition("Worker address missing"))?;

        let mut client = WorkerClient::connect(worker_address)
            .await
            .map_err(|e| Status::unavailable(format!("Failed to connect to worker: {e}")))?;
        let response = client
            .restart_worker(tonic::Request::new(RestartWorkerRequest {
                node_id: node_id.clone(),
            }))
            .await
            .map_err(|e| Status::unavailable(format!("Worker restart failed: {e}")))?;

        let reply = RestartWorkerReply {
            message: response.into_inner().message,
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn get_allocation_logs(
        &self,
        request: Request<GetAllocationLogsRequest>,
    ) -> Result<Response<GetAllocationLogsReply>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let req = request.into_inner();

        // Resolve which allocations to read: a single allocation id when given,
        // otherwise every allocation belonging to the named job.
        let allocations = if let Some(allocation_id) = req.allocation_id {
            let alloc = self
                .store
                .get_allocation_by_id(allocation_id)
                .map_err(|e| map_store_error("Error loading allocation", e))?
                .ok_or_else(|| {
                    Status::not_found(format!("allocation {allocation_id} not found"))
                })?;
            acl::enforce(
                &identity,
                AclResource::Job,
                AclAction::Read,
                &AclScope::job(&alloc.job_id),
            )?;
            vec![alloc]
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
            let job = self
                .store
                .get_job(&req.job)
                .map_err(|e| map_store_error("Error loading job", e))?;
            self.store
                .allocs_for_job(&job.id)
                .map_err(|e| map_store_error("Error loading job allocations", e))?
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
        let identity = self.authenticate_acl(&request)?;
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
        let event = self
            .store
            .issue_raft_command(&req.command, req.target_node_id.as_deref(), &actor)
            .map(map_raft_event)
            .map_err(|e| map_store_error("Error issuing raft command", e))?;
        let status = self
            .store
            .raft_status()
            .map(map_raft_status)
            .ok_or_else(|| Status::failed_precondition("raft backend is not enabled"))?;
        Ok(Response::new(IssueRaftCommandReply {
            command: req.command,
            event: Some(event),
            status: Some(status),
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

        let mut nodes: Vec<LedgerNode> = ledger.into_iter().map(Into::into).collect();
        for node in &mut nodes {
            for allocation in &mut node.allocations {
                allocation.artifact_states = allocation_health
                    .get(&allocation.id)
                    .and_then(|snapshot| snapshot.artifact_states.clone());
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
        for (service, job_id, task_name, tags) in declared {
            summaries.entry(service.clone()).or_insert(ServiceSummary {
                service,
                job_id,
                task_name,
                healthy_count: 0,
                total_count: 0,
                tags,
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
                port: u32::from(endpoint.port.unwrap_or(0)),
                healthy: endpoint.healthy,
                health_status: endpoint.health_status,
                tags: endpoint.tags,
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
        self.check_acl(
            &request,
            AclResource::Kv,
            AclAction::Read,
            AclScope::path(&request.get_ref().key),
        )?;
        let req = request.into_inner();
        let entry = self
            .store
            .get_kv(&req.key)
            .map_err(|e| map_store_error("Error getting kv entry", e))?;

        Ok(Response::new(GetKvReply { entry }))
    }

    /// Resolves an artifact credential from KV for a worker about to fetch.
    /// The value is returned for immediate use and never persisted on
    /// allocations or logged (the span carries node/path/source only).
    ///
    /// SECURITY BOUNDARY (KV ACLs do not exist yet): any task spec may
    /// reference any KV path via auth.kv, so possession of job-submit access
    /// implies read access to every KV credential. Revisit when the ACL
    /// system lands — this credential read must become an early enforcement
    /// point. Tracked in docs/TASK-LIST.md (ACL System).
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
        let mut entries = self
            .store
            .list_kv(&req.prefix)
            .map_err(|e| map_store_error("Error listing kv entries", e))?;
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
    async fn bootstrap_acl(
        &self,
        request: Request<BootstrapAclRequest>,
    ) -> Result<Response<BootstrapAclReply>, Status> {
        // ACL: open endpoint guarded by its one-shot semantics — it only
        // succeeds while no management token exists.
        let req = request.into_inner();
        let (token, secret) = self.auth.bootstrap(&self.store, req.secret.as_deref())?;
        info!(accessor_id = %token.accessor_id, "ACL bootstrap: initial management token created");
        Ok(Response::new(BootstrapAclReply {
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
}

fn worker_address_for_node(node: &Node) -> Option<String> {
    let attrs = node.config_attrs.as_object()?;
    attrs
        .get("workerAddress")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
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
    let worker_address = worker_address_for_node(&node)
        .ok_or_else(|| format!("worker address missing for node {}", allocation.node_id))?;

    if !clients.contains_key(&worker_address) {
        let endpoint = Endpoint::from_shared(worker_address.clone())
            .map_err(|e| format!("invalid worker address {worker_address}: {e}"))?;
        clients.insert(
            worker_address.clone(),
            WorkerClient::new(endpoint.connect_lazy()),
        );
    }
    let client = clients
        .get_mut(&worker_address)
        .expect("worker client just inserted");

    let reply = client
        .get_task_logs(GetTaskLogsRequest {
            allocation_id: allocation.id,
            stream: req.stream.clone(),
            max_bytes: req.max_bytes,
            tail: req.tail,
            offset: req.offset,
        })
        .await
        .map_err(|status| format!("worker on {} returned {}", allocation.node_id, status))?;

    Ok(reply.into_inner().streams)
}

fn handle_failed_allocations(
    store: &Store,
    failure_tracker: &Arc<Mutex<AllocationFailureTracker>>,
    node_id: &str,
    failed: &[FailedAllocation],
) -> R<()> {
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

    Ok(())
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

    let mut task_cache: HashMap<String, HashMap<i64, TaskConfig>> = HashMap::new();
    let mut allocations = Vec::new();

    for allocation in desired {
        let tasks = task_cache
            .entry(allocation.job_config_id.clone())
            .or_default();
        if tasks.is_empty() {
            let task_configs = store.get_task_configs_for_job_config(&allocation.job_config_id)?;
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
        allocations.push(build_task_allocation(task, &allocation, instance.as_ref())?);
    }

    Ok(allocations)
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
    use tokio::sync::oneshot;
    use tonic::transport::Server;

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
                "memory_total": "8388608",
                "cpus": {
                    "cpu-1": { "frequency": "2400" },
                    "cpu-2": { "frequency": "2400" },
                }
            })
            .to_string(),
            dynamic_attrs: "{}".into(),
            health_checks: "{}".into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
        };
        store.insert_node(node)?;
        Ok(())
    }

    fn submission() -> Submission {
        Submission {
            path: "job.jsonnet".into(),
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
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: None,
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
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    path: "/var/lib/db".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                }),
            }),
            ..TaskConfig::new("db", "noop")
        };
        let job = JobConfig {
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
        async fn post_work(
            &self,
            _request: Request<PostWorkRequest>,
        ) -> Result<Response<PostWorkReply>, Status> {
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
    }

    #[tokio::test]
    async fn manager_forwards_worker_restart() -> R<()> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        drop(listener);
        let worker_address = format!("http://{}", addr);

        let called = Arc::new(Mutex::new(None));
        let worker = TestWorker {
            called: called.clone(),
        };

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            Server::builder()
                .add_service(crate::structs::worker_server::WorkerServer::new(worker))
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
        });
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let store = setup_store()?;
        let node = Node {
            id: "node-1".into(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            config_attrs: json!({ "workerAddress": worker_address }),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        };
        store.insert_node(NodeMsg::from(node))?;
        let manager = ManagerDaemonService::new(store);

        let response = manager
            .restart_worker(Request::new(RestartWorkerRequest {
                node_id: "node-1".into(),
            }))
            .await?;

        assert_eq!(response.into_inner().message, "ok");
        let guard = called.lock().expect("mutex");
        assert_eq!(guard.as_deref(), Some("node-1"));

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
            }))
            .await
            .expect_err("missing target should error");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
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
                "kind": "exec",
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
                "kind": "exec",
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
                "kind": "exec",
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
                "kind": "exec",
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
        assert_eq!(manager.store.get_job("kv-del-none")?.status, "done");
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
        assert_eq!(manager.store.get_job("kv-del-restart")?.status, "done");
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

        assert_eq!(store.get_job("kv-recover")?.status, "done");
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
                    "kind": "exec",
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
                    "kind": "exec",
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
        // done and a job_update enqueued to replace the failed allocations.
        put_kv_entry(&manager, "clr/key", "v2").await?;
        assert_eq!(store.get_job("kv-clr")?.status, "done");
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
        assert_eq!(store.get_job("kv-none")?.status, "done");
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
                "kind": "exec",
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
        assert_eq!(manager.store.get_job("kv-unwatched")?.status, "done");
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
                "kind": "exec",
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
                "kind": "exec",
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
                    "kind": "exec",
                    "kv": { "watch": true, "onChange": "restart", "onDelete": "none" },
                    "env": [
                        { "name": "A", "value": { "template": "{{ kv \"iso/a\" }}" } }
                    ],
                    "config": { "command": ["echo", "hi"] }
                },
                {
                    "name": "bystander",
                    "kind": "exec",
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
                "kind": "exec",
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

    #[tokio::test]
    async fn run_job_rejects_file_injection_for_container_tasks() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store);

        let jobspec = json!({
            "name": "kv-container-files",
            "task": {
                "kind": "container",
                "files": [
                    {
                        "path": "/etc/app/conf",
                        "file": { "template": "static-content" }
                    }
                ],
                "config": { "image": "alpine", "command": ["echo", "hi"] }
            }
        });
        let err = run_jobspec(&manager, jobspec)
            .await
            .expect_err("submit should fail for unsupported file injection");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("does not support kv file injection"),
            "unexpected error message: {}",
            err.message()
        );
        Ok(())
    }

    #[tokio::test]
    async fn run_job_cancels_superseded_rollouts_and_cleans_stale_allocations() -> R<()> {
        let store = setup_store()?;
        let manager = ManagerDaemonService::new(store.clone());
        insert_node(&store, "node-a")?;

        let v1 = JobConfig {
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
                "kind": "exec",
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
        std::thread::sleep(Duration::from_millis(10));

        assert!(store.get_allocation_by_id(allocation.id)?.is_none());
        let evals = store.get_evals()?;
        let eval = evals.first().context("missing eval")?;
        assert_eq!(eval.kind, "job_reschedule");
        assert_eq!(eval.job_config_id, job_config_id);
        assert_eq!(eval.node_name, "node-a");

        Ok(())
    }

    #[test]
    fn artifact_staging_failures_emit_per_artifact_events() -> R<()> {
        let store = setup_store()?;
        insert_node(&store, "node-a")?;

        let job = JobConfig {
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

    //
    // KEYED-MODE INSTANCE RPCs (see docs/stateful-workloads.md)
    //

    fn keyed_task(name: &str, group: &str, id_source: Option<&str>) -> TaskConfig {
        TaskConfig {
            group: group.into(),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Keyed,
                id_source: id_source.map(String::from),
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    path: "/workspace".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                }),
            }),
            ..TaskConfig::new(name, "noop")
        }
    }

    fn submit_keyed_job(store: &Store, name: &str, tasks: Vec<TaskConfig>) -> R<String> {
        let job = JobConfig {
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
        submit_keyed_job(&store, job_id, vec![keyed_task("sandbox", "default", None)])?;
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
                keyed_task("sandbox", "sandboxes", None),
                keyed_task("runner", "runners", Some("caller")),
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

        // idSource = "caller": the id is required ...
        let status = create("multi", None, Some("runner"), None).expect_err("caller id required");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("idSource"));
        // ... and an explicit one is accepted.
        let reply = create("multi", None, Some("runner"), Some("tenant-1"))
            .map_err(|err| anyhow!(err))?
            .into_inner();
        assert_eq!(reply.instance_id, "tenant-1");

        // Explicit ids are accepted even when idSource is unset/uuid ...
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
        submit_keyed_job(&store, job_id, vec![keyed_task("sandbox", "default", None)])?;
        let runtime = tokio::runtime::Runtime::new()?;

        // Create twice with the same key: one instance, replayed reply.
        let first = runtime
            .block_on(manager.create_instance(Request::new(CreateInstanceRequest {
                job_id: job_id.into(),
                group: None,
                task: None,
                instance_id: None,
                idempotency_key: Some("create-1".into()),
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
                keyed_task("sandbox", "sandboxes", None),
                keyed_task("runner", "runners", None),
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
            })))
            .map_err(|err| anyhow!(err))?
            .into_inner();
        assert_eq!(all.instances.len(), 3);

        let filtered = runtime
            .block_on(manager.list_instances(Request::new(ListInstancesRequest {
                job_id: job_id.into(),
                group: Some("sandboxes".into()),
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
}
