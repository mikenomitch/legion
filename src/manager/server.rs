// gRPC handler signatures are fixed by the generated `Manager` trait, so the
// large `tonic::Status` error variant cannot be boxed away here.
#![allow(clippy::result_large_err)]

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use crate::prelude::*;

use crate::manager::dispatcher::build_task_allocation;
use crate::manager::rollout::{
    advance_rollout as advance_rollout_state, step_fractions_for_rollout,
    stop_rollout as stop_rollout_state,
};
use crate::manager::rollout_strategy;
use crate::manager::scheduler::{PlanConfigOutcome, PlanMode, Scheduler};
use crate::storage::errors::StorageError;
use crate::storage::events::{SchedulerEventFilter, SchedulerEventRecord};
use crate::storage::store::{JobKvReference, JobSubmitOutcome, Store};
use crate::structs::manager_server::Manager;
use crate::structs::{
    worker_client::WorkerClient, AcquireKvLockReply, AcquireKvLockRequest, AdvanceRolloutReply,
    AdvanceRolloutRequest, AllocationHealth, AllocationLog, DeleteKvReply, DeleteKvRequest,
    DeleteSavedStateQueryReply, DeleteSavedStateQueryRequest, DestroyJobReply, DestroyJobRequest,
    FailedAllocation, GetAllocationLogsReply, GetAllocationLogsRequest, GetArtifactCredentialReply,
    GetArtifactCredentialRequest, GetJobInfoReply, GetJobInfoRequest, GetJobSpecByConfigRequest,
    GetJobSpecReply, GetJobSpecRequest, GetKvLockReply, GetKvLockRequest, GetKvReply, GetKvRequest,
    GetLedgerReply, GetLedgerRequest, GetManagerConfigsReply, GetManagerConfigsRequest,
    GetNodeReply, GetNodeRequest, GetNodesReply, GetNodesRequest, GetObservabilityReply,
    GetObservabilityRequest, GetRaftJoinTokenReply, GetRaftJoinTokenRequest, GetRaftStatusReply,
    GetRaftStatusRequest, GetRolloutReply, GetRolloutRequest, GetRolloutStatusReply,
    GetRolloutStatusRequest, GetStateStoreSchemaReply, GetStateStoreSchemaRequest,
    GetTaskLogsRequest, HealthCheckReply, HealthCheckRequest, HeartbeatReply, HeartbeatRequest,
    IssueRaftCommandReply, IssueRaftCommandRequest, JobConfig, JobSpecHistoryItem, KvEntry,
    KvInjectionAction, KvValueType, LedgerNode, ListEventsReply, ListEventsRequest,
    ListJobSpecHistoryReply, ListJobSpecHistoryRequest, ListJobsReply, ListJobsRequest,
    ListKvReply, ListKvRequest, ListRaftHistoryReply, ListRaftHistoryRequest,
    ListRolloutEventsReply, ListRolloutEventsRequest, ListRolloutsReply, ListRolloutsRequest,
    ListSavedStateQueriesReply, ListSavedStateQueriesRequest, Node, NodeMsg, PatchKvReply,
    PatchKvRequest, PlanJobReply, PlanJobRequest, PlannedAllocation, PutKvReply, PutKvRequest,
    QueryStateStoreReply, QueryStateStoreRequest, RaftHistoryEvent, RaftStatus, ReleaseKvLockReply,
    ReleaseKvLockRequest, RenewKvLockReply, RenewKvLockRequest, RestartWorkerReply,
    RestartWorkerRequest, RolloutAllocationStatus, RolloutEvent, RolloutGroupStatus,
    RolloutProgression, RolloutReplacement, RunJobReply, RunJobRequest, SavedStateQuery,
    SchedulerEvent, StopJobReply, StopJobRequest, StopRolloutReply, StopRolloutRequest,
    StreamEventsRequest, Submission, TaskConfig, TaskOnUnhealthyConfig, UpdateNodeDynamicReply,
    UpdateNodeDynamicRequest, UpsertSavedStateQueryReply, UpsertSavedStateQueryRequest,
    WorkerConfig, WorkerPreRegisterReply, WorkerPreRegisterRequest, WorkerRegisterReply,
    WorkerRegisterRequest,
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
    refs.extend(apply_job_kv_injections(store, &mut config)?);
    Ok((config, refs))
}

fn apply_kv_references_to_config(
    store: &Store,
    config: &JobConfig,
    refs: &[JobKvReference],
) -> R<JobConfig> {
    let mut value = serde_json::to_value(config)?;
    for reference in refs {
        if !reference.json_pointer.starts_with('/') {
            continue;
        }
        if reference.json_pointer.contains("/kvInjections/") {
            continue;
        }
        let entry = store
            .get_kv(&reference.kv_key)?
            .ok_or_else(|| anyhow!("kv entry {} not found", reference.kv_key))?;
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
    let _ = apply_job_kv_injections(store, &mut updated)?;
    Ok(updated)
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

fn render_template_with_kv(store: &Store, template: &str) -> R<String> {
    let mut rendered = template.to_string();
    for key in extract_template_kv_keys(template) {
        let entry = store
            .get_kv(&key)?
            .ok_or_else(|| anyhow!("kv entry {} not found", key))?;
        let replacement = entry.value;
        let token = format!("kv \"{key}\"");
        rendered = rendered.replace(&token, &replacement);
        let token = format!("{{{{ {token} }}}}");
        rendered = rendered.replace(&token, &replacement);
    }
    Ok(rendered)
}

fn resolve_template_source(
    store: &Store,
    source: &crate::structs::KvTemplateSource,
) -> R<(String, Vec<String>)> {
    if let Some(key) = source.template_kv.as_deref() {
        let entry = store
            .get_kv(key)?
            .ok_or_else(|| anyhow!("kv entry {} not found", key))?;
        let template = entry.value;
        let mut deps = vec![key.to_string()];
        deps.extend(extract_template_kv_keys(&template));
        let rendered = render_template_with_kv(store, &template)?;
        return Ok((rendered, deps));
    }
    let template = source
        .template
        .as_deref()
        .context("template or template_kv must be set")?;
    let deps = extract_template_kv_keys(template);
    let rendered = render_template_with_kv(store, template)?;
    Ok((rendered, deps))
}

fn apply_task_kv_injections(store: &Store, task: &mut TaskConfig) -> R<Vec<KvReference>> {
    let Some(injections) = task.kv_injections.as_ref() else {
        return Ok(Vec::new());
    };
    let mut refs = Vec::new();

    for env in &injections.env {
        let (rendered, deps) = resolve_template_source(store, &env.value)?;
        let map = task
            .config
            .as_object_mut()
            .context("task config must be an object")?;
        let env_value = map.entry("env").or_insert_with(|| json!({}));
        let env_map = env_value
            .as_object_mut()
            .context("task config env must be an object")?;
        env_map.insert(env.name.clone(), Value::String(rendered));
        for key in deps {
            refs.push(KvReference {
                key,
                json_pointer: format!("/tasks/{}/kvInjections/env/{}", task.name, env.name),
            });
        }
    }

    for file in &injections.files {
        let (rendered, deps) = resolve_template_source(store, &file.file)?;
        let map = task
            .config
            .as_object_mut()
            .context("task config must be an object")?;
        let files_value = map.entry("files").or_insert_with(|| json!([]));
        let files = files_value
            .as_array_mut()
            .context("task config files must be an array")?;
        files.push(json!({
            "path": file.path,
            "mode": file.mode,
            "content": rendered
        }));
        for key in deps {
            refs.push(KvReference {
                key,
                json_pointer: format!("/tasks/{}/kvInjections/files/{}", task.name, file.path),
            });
        }
    }
    Ok(refs)
}

fn apply_job_kv_injections(store: &Store, config: &mut JobConfig) -> R<Vec<KvReference>> {
    let mut refs = Vec::new();
    for task in &mut config.tasks {
        refs.extend(apply_task_kv_injections(store, task)?);
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

#[derive(Debug)]
pub struct ManagerDaemonService {
    pub store: Store,
    allocation_failures: Arc<Mutex<AllocationFailureTracker>>,
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
    pub fn new(store: Store) -> Self {
        Self {
            store,
            allocation_failures: Arc::new(Mutex::new(AllocationFailureTracker::default())),
            artifacts_config: crate::structs::ArtifactsManagerConfigFull::default(),
            node_identity_names: Vec::new(),
            fetcher_registry: Arc::new(Mutex::new(
                crate::artifacts::FetcherRegistry::with_builtins(),
            )),
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
        service
    }

    /// Admission normalization for artifacts (artifact system plan §§1,6,7,8):
    /// desugars kind-implicit artifacts (behind `containerImageDesugar`),
    /// enforces cluster-scoped rules (scheme allowlist, nodeIdentity refs),
    /// bakes the effective auth from cluster matchers, and pins mutable
    /// references at submit. Returns human-readable pin notes
    /// (`task api: pinned oci://… -> sha256:…`) for plan/run messages.
    ///
    /// Workers receive exactly this normalized list and never re-derive
    /// implicit artifacts or re-resolve pinned tags.
    fn normalize_job_artifacts(
        &self,
        config: &mut JobConfig,
        resolve_pins: bool,
    ) -> R<Vec<String>> {
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
                notes.push(format!(
                    "task {}: pinned {} -> {}",
                    task.name,
                    spec.redacted_source(),
                    resolved.digest
                ));
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

    fn remove_stale_allocations_for_new_rollout(
        &self,
        job_id: &str,
        new_job_config_id: &str,
        previous_job_config_id: Option<&str>,
    ) -> Result<(), Status> {
        let keep = std::iter::once(new_job_config_id)
            .chain(previous_job_config_id)
            .collect::<HashSet<_>>();
        let allocations = self
            .store
            .allocs_for_job(job_id)
            .map_err(|e| map_store_error("Error loading job allocations", e))?;

        for allocation in allocations {
            if keep.contains(allocation.job_config_id.as_str()) {
                continue;
            }
            self.store
                .remove_allocation_by_id(allocation.id)
                .map_err(|e| map_store_error("Error removing stale allocation", e))?;
        }

        Ok(())
    }

    fn create_rollout_for_update(
        &self,
        job_id: &str,
        previous_job_config_id: Option<&str>,
        new_job_config_id: &str,
        config: &JobConfig,
        create_error: &str,
    ) -> Result<bool, Status> {
        self.cancel_active_rollouts_for_job(job_id)?;
        self.remove_stale_allocations_for_new_rollout(
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
        let rollout = self
            .store
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
        self.store
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

    fn handle_kv_change(&self, key: &str, is_delete: bool) -> Result<(), Status> {
        let refs = self
            .store
            .job_kv_references_for_key(key)
            .map_err(|e| map_store_error("Error loading kv references", e))?;
        if refs.is_empty() {
            return Ok(());
        }

        let mut jobs_to_update: HashMap<String, String> = HashMap::new();
        for reference in refs {
            jobs_to_update
                .entry(reference.job_id)
                .or_insert(reference.job_config_id);
        }

        for (job_id, job_config_id) in jobs_to_update {
            let job_refs = self
                .store
                .job_kv_references_for_job(&job_id)
                .map_err(|e| map_store_error("Error loading job kv references", e))?;
            if job_refs.is_empty() {
                continue;
            }
            let current_config = self
                .store
                .get_job_config_by_id(&job_config_id)
                .map_err(|e| map_store_error("Error loading job config", e))?;
            let updated_config =
                apply_kv_references_to_config(&self.store, &current_config, &job_refs)
                    .map_err(|e| map_store_error("Error applying kv references", e))?;
            let has_legacy_refs = job_refs
                .iter()
                .any(|reference| !reference.json_pointer.contains("/kvInjections/"));
            let mut action = if has_legacy_refs {
                KvInjectionAction::Rollout
            } else {
                KvInjectionAction::None
            };
            for task in &updated_config.tasks {
                let Some(injections) = task.kv_injections.as_ref() else {
                    continue;
                };
                let defaults = injections.kv.clone().unwrap_or_default();
                for env in &injections.env {
                    let policy =
                        if env.kv.watch || env.kv.on_change.is_some() || env.kv.on_delete.is_some()
                        {
                            env.kv.clone()
                        } else {
                            defaults.clone()
                        };
                    if !policy.watch {
                        continue;
                    }
                    let (_, deps) = resolve_template_source(&self.store, &env.value)
                        .map_err(|e| map_store_error("Error resolving kv env injection", e))?;
                    if deps.iter().any(|dep| dep == key) {
                        let selected = if is_delete {
                            policy.on_delete.unwrap_or(KvInjectionAction::Fail)
                        } else {
                            policy.on_change.unwrap_or(KvInjectionAction::None)
                        };
                        action = strongest_action(action, selected);
                    }
                }
                for file in &injections.files {
                    let policy = if file.kv.watch
                        || file.kv.on_change.is_some()
                        || file.kv.on_delete.is_some()
                    {
                        file.kv.clone()
                    } else {
                        defaults.clone()
                    };
                    if !policy.watch {
                        continue;
                    }
                    let (_, deps) = resolve_template_source(&self.store, &file.file)
                        .map_err(|e| map_store_error("Error resolving kv file injection", e))?;
                    if deps.iter().any(|dep| dep == key) {
                        let selected = if is_delete {
                            policy.on_delete.unwrap_or(KvInjectionAction::Fail)
                        } else {
                            policy.on_change.unwrap_or(KvInjectionAction::None)
                        };
                        action = strongest_action(action, selected);
                    }
                }
            }
            let submissions = self
                .store
                .get_submissions_for_config(&job_config_id)
                .map_err(|e| map_store_error("Error loading job submissions", e))?;
            let submit_outcome = self
                .store
                .handle_job_submit(&updated_config, &submissions)
                .map_err(|e| map_store_error("Error updating job for kv change", e))?;
            let new_job_config_id = submit_outcome.job_config_id().to_string();
            let updated_refs: Vec<JobKvReference> = job_refs
                .iter()
                .map(|reference| JobKvReference {
                    job_id: job_id.clone(),
                    job_config_id: new_job_config_id.clone(),
                    kv_key: reference.kv_key.clone(),
                    json_pointer: reference.json_pointer.clone(),
                })
                .collect();
            self.store
                .replace_job_kv_references(&job_id, &updated_refs)
                .map_err(|e| map_store_error("Error updating kv references", e))?;

            if submit_outcome.is_unchanged() {
                continue;
            }

            match action {
                KvInjectionAction::None => {}
                KvInjectionAction::Restart => {
                    self.store
                        .enqueue_eval(
                            "job_update",
                            Some(&job_id),
                            Some(&new_job_config_id),
                            None,
                            Some(self.store.default_eval_priority),
                        )
                        .map_err(|e| map_store_error("Error scheduling kv update restart", e))?;
                }
                KvInjectionAction::Rollout | KvInjectionAction::Fail => {
                    let previous_job_config_id = self
                        .store
                        .previous_job_config_id(&new_job_config_id)
                        .map_err(|e| map_store_error("Error loading previous job config", e))?;
                    let is_manual = self.create_rollout_for_update(
                        &job_id,
                        previous_job_config_id.as_deref(),
                        &new_job_config_id,
                        &updated_config,
                        "Error creating rollout for kv update",
                    )?;

                    if !is_manual {
                        self.store
                            .enqueue_eval(
                                "rollout_tick",
                                Some(&job_id),
                                Some(&new_job_config_id),
                                None,
                                Some(self.store.default_eval_priority),
                            )
                            .map_err(|e| {
                                map_store_error("Error scheduling kv update rollout", e)
                            })?;
                    }
                }
            }
        }

        Ok(())
    }
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

        let force_reschedule = inner.force_reschedule;
        validate_job_config(&config).map_err(|e| {
            error!("Error validating task configs: {:?}", e);
            Status::invalid_argument(format!("Error validating task configs: {e}"))
        })?;
        // Artifact admission: desugar/auth/pin. Pin resolution can reach
        // registries, so it runs as blocking work.
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
                let is_manual = self.create_rollout_for_update(
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
        let request = request.into_inner();
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
        let request = request.into_inner();
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
        let request = request.into_inner();
        if request.rollout_id <= 0 {
            return Err(Status::invalid_argument(
                "rollout_id must be greater than 0",
            ));
        }
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
        let request = request.into_inner();
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
        let request = request.into_inner();
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
        let jobspec = request.into_inner().parsed_jobspec;
        if jobspec.trim().is_empty() {
            return Err(Status::invalid_argument("Plan request missing jobspec"));
        }

        let (config, _kv_refs) =
            resolve_jobspec_kv_references(&self.store, &jobspec).map_err(|e| {
                error!("Error parsing jobspec for plan: {:?}", e);
                Status::invalid_argument(format!("Error parsing jobspec: {e}"))
            })?;
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
        for note in pin_notes.iter().chain(artifact_diffs.iter()) {
            message.push('\n');
            message.push_str(note);
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
        let config_id = request.into_inner().config_id;

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

    #[instrument(skip_all)]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
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
        let request = request.into_inner();
        let dynamic_attrs = json!({}).to_string();

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

        let reply = WorkerRegisterReply {
            message: "Worker registered".into(),
        };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn get_nodes(
        &self,
        request: Request<GetNodesRequest>,
    ) -> Result<Response<GetNodesReply>, Status> {
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
        let id = request.into_inner().id;
        let node = self
            .store
            .get_node(&id)
            .map_err(|e| map_store_error("Error getting node", e))?;

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
        let node_id = request.into_inner().node_id;
        let node = self.store.get_node(&node_id).map_err(|e| {
            error!(error = ?e, node_id = %node_id, "Failed to get node for restart");
            Status::not_found("Node not found")
        })?;

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
            vec![alloc]
        } else {
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
        let r = request.into_inner();
        let id = r.id;
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
        _request: Request<GetRaftStatusRequest>,
    ) -> Result<Response<GetRaftStatusReply>, Status> {
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
        _request: Request<GetLedgerRequest>,
    ) -> Result<Response<GetLedgerReply>, Status> {
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

        let reply: GetLedgerReply = GetLedgerReply { nodes };

        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn put_kv(&self, request: Request<PutKvRequest>) -> Result<Response<PutKvReply>, Status> {
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

        self.handle_kv_change(&req.key, false)?;

        Ok(Response::new(PutKvReply { entry: Some(entry) }))
    }

    #[instrument(skip_all)]
    async fn get_kv(&self, request: Request<GetKvRequest>) -> Result<Response<GetKvReply>, Status> {
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
        let req = request.into_inner();
        let entries = self
            .store
            .list_kv(&req.prefix)
            .map_err(|e| map_store_error("Error listing kv entries", e))?;

        Ok(Response::new(ListKvReply { entries }))
    }

    #[instrument(skip_all)]
    async fn delete_kv(
        &self,
        request: Request<DeleteKvRequest>,
    ) -> Result<Response<DeleteKvReply>, Status> {
        let req = request.into_inner();
        self.store
            .delete_kv(&req.key, req.expected_version, req.lock_id.as_deref())
            .map_err(|e| map_store_error("Error deleting kv entry", e))?;
        self.handle_kv_change(&req.key, true)?;

        Ok(Response::new(DeleteKvReply {
            message: "kv entry deleted".to_string(),
        }))
    }

    #[instrument(skip_all)]
    async fn patch_kv(
        &self,
        request: Request<PatchKvRequest>,
    ) -> Result<Response<PatchKvReply>, Status> {
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

        self.handle_kv_change(&req.key, false)?;

        Ok(Response::new(PatchKvReply { entry: Some(entry) }))
    }

    #[instrument(skip_all)]
    async fn acquire_kv_lock(
        &self,
        request: Request<AcquireKvLockRequest>,
    ) -> Result<Response<AcquireKvLockReply>, Status> {
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
        _request: Request<GetStateStoreSchemaRequest>,
    ) -> Result<Response<GetStateStoreSchemaReply>, Status> {
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
        _request: Request<ListSavedStateQueriesRequest>,
    ) -> Result<Response<ListSavedStateQueriesReply>, Status> {
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
        let req = request.into_inner();
        self.store
            .delete_saved_state_query(&req.name)
            .map_err(|e| map_store_error("Error deleting saved state query", e))?;

        Ok(Response::new(DeleteSavedStateQueryReply {
            message: "saved query deleted".to_string(),
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
        let total_steps = step_fractions_for_rollout(&effective_rollout, base_count).len() as u32;

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
        allocations.push(build_task_allocation(task, &allocation)?);
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
}
