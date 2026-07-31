// A few low-level persistence helpers take many columns by parameter.
#![allow(clippy::too_many_arguments)]

use crate::constants::{
    ALLOCATION_STATUS_COMPLETE, ALLOCATION_STATUS_FAILED, STOP_NOTICE_RETENTION_SECS,
};
use crate::constraints::{ConstraintSet, DistinctTracker, PermitSet};
use crate::prelude::*;
use crate::resources::{ResourcePolicy, ResourcePool, ResourceRequest};
use crate::scheduling_index::{IndexSet, IndexSettings};
use crate::storage::errors::StorageError;
use crate::storage::events::{drafts, EventDraft, SchedulerEventFilter, SchedulerEventRecord};
use crate::storage::migrations::get_migrations;
use crate::storage::persistence::{
    build_persistence_driver, PersistenceDriver, PersistenceRole, PersistenceStatus,
    RaftHistoryEvent, RaftStatus,
};
use crate::storage::raft::{capture, ReplicatedOp, ReplicationHandle};
use crate::storage::Lane;

use crate::storage::queries::{
    DELETE_SIGNING_KEY, GET_ALLOCATION_BY_ID, GET_ALLOCS, GET_ALLOCS_FOR_JOB,
    GET_ALLOCS_FOR_JOB_CONFIG, GET_ALLOCS_FOR_NODE, GET_EVALS, GET_INSTANCE, GET_INSTANCES_FOR_JOB,
    GET_JOB, GET_JOBS, GET_JOB_CONFIG, GET_JOB_CONFIG_BY_ID, GET_JOB_UPDATE_FOR_CONFIG,
    GET_MANAGER_CONFIGS, GET_NEXT_EVAL, GET_NODE, GET_NODES, GET_SIGNING_KEYS,
    GET_TASK_FOR_JOB_CONIFG, GET_WORKER_TYPE, INSERT_ALLOCATION, INSERT_EVAL, INSERT_JOB,
    INSERT_JOB_CONFIG, INSERT_JOB_CONFIG_SUBMISSION, INSERT_JOB_UPDATE, INSERT_NODE,
    INSERT_SIGNING_KEY, INSERT_SUBMISSION, INSERT_TASK, INSERT_WORKER_TYPE, NODE_JOINED_ATTRS,
    NODE_JOINED_FROM, SEED_NODE_HEALTH, UPDATE_SIGNING_KEY_STATE, UPSERT_INSTANCE,
    UPSERT_MANAGER_CONFIG,
};
use crate::structs::{
    slot_instance_id, Allocation, AllocationHealth, Band, Eval, Instance, InstanceMode,
    InstanceState, Job, JobConfig, JobTemplateProvenance, KvEntry, KvLock, KvValueType,
    LedgerEntry, ManagerConfigFull, ManagerNodeConfig, Node, NodeMsg, ReplaceAfter,
    ResolvedOnLostPolicy, Rollout, RolloutConfig, SigningKeyRecord, StatefulConfig,
    StatefulOnLostConfig, StopCause, Submission, TaskCheck, TaskConfig, TaskCount, TaskDependency,
    TaskLifecycle, TaskOnUnhealthyConfig, TaskSchedule, TaskStatusState, Volume, WorkerConfig,
};
use crate::util::random_string;

use lazy_static::lazy_static;
use rand::{thread_rng, Rng};
use rusqlite::{
    named_params, params, params_from_iter, types::ValueRef, Connection, Error as RusqliteError,
    OpenFlags, OptionalExtension, Row,
};
use rusqlite_migration::Migrations;
use serde::Serialize;
use serde_json::{from_str, to_string, to_string_pretty, Value};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{MutexGuard, OnceLock, RwLock};
use std::time::{Duration, Instant};
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::{broadcast, Notify};

// warning incorrectly
#[allow(dead_code)]
pub enum Msg {
    Content(String),
}

pub type MemSafeConn = Arc<Mutex<Connection>>;
const READ_MODEL_LOCK_ERROR: &str = "Error locking read model";

/// RFC3339 with the subsecond field pinned to nine digits, so the rendered
/// string sorts lexicographically in chronological order. Used for any
/// timestamp SQL compares or orders by (see `Store::sortable_timestamp`).
const SORTABLE_TIMESTAMP_FORMAT: &[time::format_description::BorrowedFormatItem<'_>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]Z");

/// Parse a nullable JSONB text column into an optional typed value, mapping
/// parse failures onto rusqlite conversion errors.
fn parse_json_column<T: serde::de::DeserializeOwned>(
    idx: usize,
    raw: Option<String>,
) -> Result<Option<T>, rusqlite::Error> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    serde_json::from_str(&raw).map(Some).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(idx, rusqlite::types::Type::Text, Box::new(err))
    })
}

/// Extract the host portion of a node's advertised `workerAddress` config
/// attribute (for example `http://10.0.0.5:4848` -> `10.0.0.5`).
fn worker_address_host(config_attrs: &Value) -> Option<String> {
    let address = config_attrs.as_object()?.get("workerAddress")?.as_str()?;
    let without_scheme = address
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(address);
    let host = without_scheme
        .split('/')
        .next()
        .unwrap_or(without_scheme)
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(without_scheme);
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

/// Allocations per node, each node's vector shared by refcount: a plan's
/// snapshot bumps refcounts instead of deep-cloning every allocation, and
/// the few writers copy-on-write via `Arc::make_mut`.
pub type SharedNodeAllocations = HashMap<String, Arc<Vec<Allocation>>>;

#[derive(Debug, Clone, Default)]
pub struct SchedulingReadModel {
    /// Nodes are `Arc`'d so a plan's snapshot is refcount bumps, not deep
    /// clones of every node's JSON attribute bags. In-place updates go
    /// through `Arc::make_mut` (copy-on-write while a plan still holds the
    /// old snapshot).
    pub nodes: HashMap<String, Arc<Node>>,
    pub allocations_by_node: SharedNodeAllocations,
    pub task_configs: HashMap<String, Vec<(i64, TaskConfig)>>,
    /// Declared scheduling indexes over `nodes` (docs/scheduling-indexes.md).
    /// Maintained by the same write path that mutates `nodes` and
    /// `allocations_by_node`, so it cannot drift from them; derived state
    /// only, rebuilt on every full read-model load.
    pub indexes: IndexSet,
    /// Free resource capacity per **available** node — fingerprinted
    /// capacity minus every current allocation's usage — recomputed for the
    /// affected node by the same reprojection pass that maintains
    /// `indexes`. Values are `Arc`'d so the per-plan snapshot clone is a
    /// refcount bump per node instead of a deep `ResourcePool` copy; the
    /// plan-time `ResourceLedger` copies-on-write only the pools it
    /// actually debits.
    pub(crate) node_pools: HashMap<String, Arc<ResourcePool>>,
    /// Mirror of `allocation_epoch_partition`, keyed by
    /// `Store::scope_vector_key`, kept COHERENT with `allocations_by_node`
    /// (adversarial finding, 2026-07-28 — the plan-fence gap): an epoch
    /// becomes visible in SQLite at commit, but the allocation cache is
    /// patched later (after the raft changeset projection, in the worst
    /// case), so a scheduler that sampled epochs from the DB could see a
    /// NEW epoch with the OLD cache — phantom capacity that passes the
    /// plan-apply CAS and oversubscribes the node. The scheduler now
    /// samples from this mirror instead, and every cache updater maintains
    /// the invariant `mirror epoch ≤ epoch of the state the cached
    /// allocations were read from` (epochs are READ before allocations and
    /// PUBLISHED after them), so a torn view can only make the CAS
    /// conservative (a spurious reject + retry), never permissive.
    pub scope_epochs: HashMap<String, i64>,
    /// Publish counter for allocation-visible state: bumped (under the same
    /// write lock) every time `allocations_by_node`/`scope_epochs` are
    /// published — the per-scope refresh funnel, node removal, and full
    /// model rebuilds (which carry the previous value forward, +1).
    ///
    /// This is what makes the LAZY optimistic-concurrency sample sound
    /// (plan-path scale levers, D4): the scheduler reads `(capacity,
    /// generation)` under one guard, plans, then reads `(epochs,
    /// generation)` under one guard. An unchanged generation proves no
    /// allocation publish happened in between, so the late epoch sample is
    /// exactly what a pre-capacity sample would have returned — the "sample
    /// no later than the capacity read" fence invariant holds. A moved
    /// generation proves nothing either way, so the scheduler falls back to
    /// the eager whole-fleet sample.
    pub generation: u64,
    /// The availability-filtered, id-sorted node list every plan starts
    /// from, cached so a plan is a `Vec<Arc<Node>>` clone (refcount bumps)
    /// instead of an O(fleet log fleet) filter + sort. Invalidated (set to
    /// `None`) by every `nodes` mutation and rebuilt lazily on the next
    /// plan, so churny fleets pay at most what they paid before this cache
    /// existed.
    pub(crate) available_nodes_cache: Option<Arc<Vec<Arc<Node>>>>,
}

#[derive(Debug)]
struct AllocationConstraintState {
    constraints: ConstraintSet,
    permit: PermitSet,
    distinct: Option<DistinctTracker>,
}

#[derive(Debug)]
struct AllocationTaskState {
    constraints: ConstraintSet,
    permit: PermitSet,
    distinct: Option<DistinctTracker>,
    job_config_id: String,
    resources: Value,
    network_raw: Option<String>,
    connect_raw: Option<String>,
}

/// A service declared in a job's latest config, whether or not it has any
/// allocations yet. Endpoint counts come from `service_endpoints`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredService {
    pub service: String,
    pub job_id: String,
    pub task_name: String,
    pub tags: Vec<String>,
    /// Connect intentions (`service.allow`). Empty means every job may connect.
    /// Surfaced through `ListServices` so `legion service doctor` can explain a
    /// denied consumer with plain `job:read`, instead of reading task configs
    /// out of the state store.
    pub allow: Vec<String>,
    pub meta: BTreeMap<String, String>,
}

/// What [`Store::plan_capacity_snapshot`] hands the scheduler: current
/// allocations by node plus the cached free pools, cloned under one guard,
/// and the read-model generation those were published at (the lazy
/// version-sample fence — see [`SchedulingReadModel::generation`]).
pub(crate) type PlanCapacityParts = (
    SharedNodeAllocations,
    HashMap<String, Arc<ResourcePool>>,
    u64,
);

#[derive(Debug, Clone)]
pub struct PlanSubmission {
    pub id: i64,
    pub job_config_id: String,
    pub expected_versions: BTreeMap<String, i64>,
}

/// One allocation's preemption-relevant metadata, straight off the join in
/// [`Store::preemption_victim_metadata`]. Normalized into a
/// `VictimCandidate` by the planner, which owns the resource parsing.
#[derive(Debug, Clone)]
pub struct PreemptionVictimRow {
    pub allocation_id: i64,
    pub node_id: String,
    pub job_id: String,
    pub job_config_id: String,
    pub task_config_id: i64,
    pub task_name: String,
    /// The allocation's own resource claim — what evicting it would return
    /// to the node.
    pub resources: Value,
    /// Placement time in unix seconds. `None` for rows written before
    /// allocation timestamps existed.
    pub created_at_unix: Option<i64>,
    /// The task's declared count, which `disruption.minAvailable` percentages
    /// resolve against.
    pub task_count: u32,
    pub disruption_json: Option<String>,
    pub stop_json: Option<String>,
    /// The owning job config's priority band.
    pub band: Band,
    /// True when this allocation's instance holds node-local storage.
    pub node_pinned: bool,
}

/// What one plan's eviction half did, so the apply path can finish the
/// non-transactional bookkeeping after the commit.
#[derive(Default)]
struct AppliedEvictions {
    /// The victim rows that were actually deleted. A victim that had already
    /// disappeared by apply time (a `job stop` landed first) is simply
    /// absent — a plan's eviction list is a decision, not a lock.
    victims: Vec<Allocation>,
    /// `allocation.preempted` events, staged in the transaction and
    /// published after it commits.
    events: Vec<SchedulerEventRecord>,
    /// `(pool, band, count)` rows for `legion_preemptions_total`.
    metric_rows: Vec<(String, String, u64)>,
}

/// One live stop notice, as the heartbeat feed serves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredStopNotice {
    pub allocation_id: i64,
    pub cause: StopCause,
    /// Job that displaced this allocation; empty for every cause but
    /// [`StopCause::Preempted`].
    pub preemptor_job: String,
    /// That job's band; empty when `preemptor_job` is.
    pub preemptor_band: String,
}

/// One task's count override (`legion job scale`, docs/autoscaling.md),
/// keyed by the version-stable task tuple so it survives resubmits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskScale {
    pub job_id: String,
    pub task_group: String,
    pub task_name: String,
    /// The applied override; `None` while only a pending scale-in exists.
    pub count: Option<u32>,
    /// A scale-in recorded during an active rollout, promoted into `count`
    /// when the rollout reaches a terminal status.
    pub pending_count: Option<u32>,
    /// The spec count the override was laid over — the resubmit-ownership
    /// arbiter (see `handle_job_submit_apply`).
    pub base_count: u32,
    /// `operator` today; `autoscaler` when the policy evaluator ships.
    pub source: String,
    pub reason: String,
    pub principal: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub enum PlanApplyOutcome {
    Applied(Vec<Allocation>),
    Rejected {
        reason: String,
        latest_versions: BTreeMap<String, i64>,
    },
}

/// One scheduler plan queued for the plan-apply group commit (D5): the
/// arguments of [`Store::submit_and_apply_plan`], owned so a batch leader
/// can carry them across threads.
#[derive(Debug, Clone)]
struct PlanApplyRequest {
    job_config_id: String,
    mode: String,
    placements: Vec<(String, i64)>,
    expected_versions: BTreeMap<String, i64>,
    /// Allocations this plan evicts to pay for `placements`. Carried on the
    /// same request, applied in the same transaction, fenced by the same
    /// `expected_versions` compare-and-set.
    evictions: Vec<PlanEviction>,
}

/// One allocation a plan will evict, as it rides the `plan_submission` row.
///
/// Deliberately a snapshot rather than a reference: the plan is a durable
/// record of a decision, and by the time it applies the allocation may be
/// gone (a job stop, a node loss). Apply re-reads the live rows and skips
/// what has already disappeared, so a vanished victim is a no-op, never an
/// error.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanEviction {
    pub allocation_id: i64,
    pub node_id: String,
    pub job_id: String,
    pub job_config_id: String,
    pub task_config_id: i64,
    /// The victim's band at plan time, for the event and the notice.
    pub band: String,
}

type PlanApplyReply = (PlanSubmission, PlanApplyOutcome);

#[derive(Debug, Clone)]
pub struct Store {
    pub write_conn: MemSafeConn,
    pub read_conns: Arc<Vec<MemSafeConn>>,
    // Filesystem path of the state database (`<storePath>/store.sqlite`).
    // Needed by snapshot install (atomic DB swap + connection reopen) and by
    // `state_backup`'s default destination (plan §A5).
    db_path: Arc<std::path::PathBuf>,
    pub read_index: Arc<AtomicUsize>,
    pub scheduling_policy: String,
    pub default_eval_priority: i64,
    pub rollout_retention_days: u64,
    pub scheduler_settings: Option<Value>,
    /// Parsed `schedulerSettings.indexes` / `index_min_nodes` (fails loudly
    /// at startup on malformed declarations).
    pub index_settings: Arc<IndexSettings>,
    pub resource_policy: ResourcePolicy,
    persistence_driver: Arc<dyn PersistenceDriver>,
    read_model: Arc<RwLock<SchedulingReadModel>>,
    // Cross-task wake signal for eval workers waiting on an empty queue.
    eval_notify: Arc<Notify>,
    // Live fan-out of scheduler events to StreamEvents subscribers. Persisted
    // history lives in the scheduler_event table; this only carries new events.
    event_tx: broadcast::Sender<SchedulerEventRecord>,
    // True once the raft engine has started (set via `set_raft_mode`). While
    // set, `write_connection` refuses callers that are neither capturing nor
    // applying a changeset — any unclassified write path fails loudly instead
    // of silently diverging replicas (JC-14).
    raft_mode: Arc<AtomicBool>,
    // Replication pipeline handle, installed by the raft engine at start
    // (phase A-1). Until it is set, `replicated()` in raft mode errors.
    replication: Arc<OnceLock<ReplicationHandle>>,
    // Whether the write connection runs at `synchronous = FULL` (raft mode —
    // see `configure_connection`); remembered so snapshot install reopens
    // the connection with the same durability level.
    durable_sync: bool,
    // Two-sweep observation grace for the status-lane orphan sweeps
    // (adversarial finding F2, 2026-07-28): an orphan is only DELETED after
    // being observed by two consecutive sweeps, so a freshly failed-over
    // status leader whose control/scheduler replicas still lag cannot
    // replicate deletions of rows that are only orphans in its stale local
    // view. Process-local by design — a leadership change starts a fresh
    // (empty) observation window, which is exactly the protection.
    node_health_orphan_candidates: Arc<std::sync::Mutex<HashSet<String>>>,
    allocation_health_orphan_candidates: Arc<std::sync::Mutex<HashSet<i64>>>,
    // Plan-apply group commit (plan-path scale levers, D5): concurrent
    // submit_and_apply_plan callers coalesce into one replicated op — one
    // capture → one raft propose/quorum round for the whole batch — with
    // per-plan savepoint isolation inside. Shared across clones so every
    // eval worker feeds the same queue.
    plan_coalescer: Arc<crate::storage::coalesce::Coalescer<PlanApplyRequest, PlanApplyReply>>,
    // Heartbeat group commit (heartbeat-scale plan §2): concurrent
    // update_node_heartbeat callers coalesce into one status-lane op
    // stamping every batched node id.
    heartbeat_coalescer: Arc<crate::storage::coalesce::Coalescer<String, ()>>,
}

/// Result of [`Store::state_backup`]: where the backup landed, its size,
/// and the raft applied index it captures (0 outside raft mode).
#[derive(Debug, Clone)]
pub struct StateBackupInfo {
    pub path: std::path::PathBuf,
    pub bytes: u64,
    pub applied_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobSubmitOutcome {
    Created { job_config_id: String },
    Updated { job_config_id: String },
    Unchanged { job_config_id: String },
}

/// Result of [`Store::destroy_keyed_instance`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestroyInstanceOutcome {
    /// The instance was live and is now terminated; `removed_allocations`
    /// allocation rows were deleted (0 when it had no current placement).
    Destroyed { removed_allocations: usize },
    /// The instance was already a tombstone; nothing changed (idempotent
    /// no-op).
    AlreadyTerminated,
}

/// Result of [`Store::stop_keyed_instance`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopInstanceOutcome {
    /// The instance was live and is now `stopped` (row and storage handle
    /// retained); `removed_allocations` allocation rows were deleted (0 when
    /// it had no current placement).
    Stopped { removed_allocations: usize },
    /// The instance was already stopped; nothing changed (idempotent no-op).
    AlreadyStopped,
}

/// Result of [`Store::start_keyed_instance`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartInstanceOutcome {
    /// The instance was `stopped` and is now `pending`, awaiting placement.
    Started,
    /// The instance was already live (pending/active/rescheduling); nothing
    /// changed (idempotent no-op).
    AlreadyRunning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobKvReference {
    pub job_id: String,
    pub job_config_id: String,
    pub kv_key: String,
    pub json_pointer: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationHealthSnapshot {
    pub allocation_id: i64,
    pub node_id: String,
    pub healthy: bool,
    pub status: String,
    pub updated_at: String,
    /// JSON-encoded per-artifact staging states from the worker heartbeat
    /// (`Vec<ArtifactStatus>`); `None` when the task declares no artifacts.
    pub artifact_states: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateStoreQueryRow {
    pub json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateStoreQueryResult {
    pub statement_type: String,
    pub columns: Vec<String>,
    pub rows: Vec<StateStoreQueryRow>,
    pub affected_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateStoreSchemaColumn {
    pub ordinal: i32,
    pub name: String,
    pub data_type: String,
    pub not_null: bool,
    pub default_value: Option<String>,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateStoreSchemaObject {
    pub kind: String,
    pub name: String,
    pub create_sql: String,
    pub columns: Vec<StateStoreSchemaColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedStateQueryRecord {
    pub name: String,
    pub sql: String,
    pub created_at: String,
    pub updated_at: String,
}

/// One declared variable of a job template, parsed from the template's
/// manifest at upsert time and stored as JSON in
/// `job_template_version.variables`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobTemplateVariableRecord {
    pub key: String,
    #[serde(rename = "type")]
    pub var_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Defaults to true when `default` is absent, false when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
}

impl JobTemplateVariableRecord {
    /// Effective requiredness: explicit flag, else "no default means required".
    pub fn is_required(&self) -> bool {
        self.required.unwrap_or(self.default.is_none())
    }
}

/// One stored job template version, plus the template's latest version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobTemplateVersionRecord {
    pub name: String,
    pub version: u32,
    pub latest_version: u32,
    pub description: Option<String>,
    pub source_jsonnet: String,
    pub variables: Vec<JobTemplateVariableRecord>,
    pub created_at: String,
    pub created_by: String,
}

/// Latest-version summary row for template listings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobTemplateSummaryRecord {
    pub name: String,
    pub latest_version: u32,
    pub description: Option<String>,
    pub variable_count: u32,
    pub updated_at: String,
}

/// Version-history metadata (no source).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobTemplateVersionInfoRecord {
    pub version: u32,
    pub created_at: String,
    pub created_by: String,
}

/// One row of a job's spec history, including template provenance when the
/// config was submitted through RunJobTemplate.
#[derive(Debug, Clone)]
pub struct JobSpecHistoryRow {
    pub config_id: String,
    pub created_at: String,
    pub version: Option<i64>,
    pub template: Option<JobTemplateProvenance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RolloutEventRecord {
    pub id: i64,
    pub rollout_id: i64,
    pub job_id: String,
    pub group_name: Option<String>,
    pub level: String,
    pub message: String,
    pub metadata_json: String,
    pub created_at: String,
}

impl JobSubmitOutcome {
    pub fn job_config_id(&self) -> &str {
        match self {
            JobSubmitOutcome::Created { job_config_id }
            | JobSubmitOutcome::Updated { job_config_id }
            | JobSubmitOutcome::Unchanged { job_config_id } => job_config_id,
        }
    }

    pub fn is_unchanged(&self) -> bool {
        matches!(self, JobSubmitOutcome::Unchanged { .. })
    }
}

pub const CONN_LOCK_ERROR: &str = "Error locking database connection";

/// SQL fragment: the id of the newest `job_config` for the job aliased `j`
/// in the enclosing query. Shared by the standing-split reconcilers (plan
/// §8, D1), which all scope themselves to each job's latest config.
const LATEST_CONFIG_FOR_JOB: &str = "(SELECT jc.id FROM job_config jc \
     WHERE jc.job_id = j.id ORDER BY jc.created_at DESC LIMIT 1)";
const READ_ONLY_CONN_POOL_SIZE: usize = 2;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
// Live subscribers that fall this many events behind must catch up from the
// scheduler_event table (StreamEvents handles the Lagged case).
const EVENT_BROADCAST_CAPACITY: usize = 1_024;
// Retained journal size; older scheduler events are pruned opportunistically.
const EVENT_RETAINED_ROWS: i64 = 10_000;
// Prune once every N inserts to keep the hot emit path cheap.
const EVENT_PRUNE_INTERVAL: i64 = 256;
// Heartbeats coalescing into one status-lane commit are capped per batch
// (heartbeat-scale plan §2): 128 single-row upserts is a small changeset,
// and the cap bounds both the IN-list and the raft entry size.
const HEARTBEAT_BATCH_MAX: usize = 128;
// Middle bits of a scheduler_event id carrying the per-process sequence:
// the high bits carry unix milliseconds and the low `Lane::ID_STRIDE_BITS`
// carry the minting lane's residue (see `next_scheduler_event_id`).
// 18 + 3 bits below the millisecond field keeps ids inside i64 until the
// 22nd century (i64::MAX >> 21 ms ≈ year 2109).
const EVENT_ID_SEQ_BITS: u32 = 18;

// NOTE: This will become a general interface for all storage backends
impl Store {
    #[instrument(err, skip_all)]
    pub fn start_storage(config: &ManagerConfigFull) -> R<Store> {
        let db_path = Store::db_path(&config.store_path)?;
        let manager_node_id = format!("{}:{}", config.server.host, config.server.port);
        let persistence_driver: Arc<dyn PersistenceDriver> =
            build_persistence_driver(&config.persistence, db_path.clone(), &manager_node_id);
        let raft_enabled = matches!(
            config.persistence.r#type,
            crate::structs::config::PersistenceType::Raft
        );
        // Voter-count config validation (plan §7-C3): `expectedVoters = 0`
        // fails the load; even expectations / even peer lists log the parity
        // advisory once per process and proceed.
        if raft_enabled {
            crate::storage::raft::quorum::validate_raft_config(&config.persistence.raft)?;
        }
        // The raft engine needs the migrated store (its state machines apply
        // into it), so its start is deferred to `start_engine` below;
        // local_disk/streaming start exactly where they always did.
        if !raft_enabled {
            persistence_driver.start()?;
        }

        let write_conn = Store::create_write_conn(&db_path, raft_enabled)?;
        let _migrations = Store::create_initial_tables(&Arc::clone(&write_conn))?;
        let read_conns = Store::create_read_pool(&db_path)?;
        let resource_policy = ResourcePolicy::from_manager_config(config)?;
        let index_settings = Arc::new(IndexSettings::from_scheduler_settings(
            config.scheduler_settings.as_ref(),
            &config.scheduling_policy,
        )?);

        let store = Store {
            write_conn,
            read_conns: Arc::new(read_conns),
            db_path: Arc::new(db_path.clone()),
            read_index: Arc::new(AtomicUsize::new(0)),
            scheduling_policy: config.scheduling_policy.clone(),
            default_eval_priority: config.default_priority,
            rollout_retention_days: config.rollout_retention_days,
            scheduler_settings: config.scheduler_settings.clone(),
            index_settings,
            resource_policy,
            persistence_driver,
            read_model: Arc::new(RwLock::new(SchedulingReadModel::default())),
            eval_notify: Arc::new(Notify::new()),
            event_tx: broadcast::channel(EVENT_BROADCAST_CAPACITY).0,
            raft_mode: Arc::new(AtomicBool::new(false)),
            replication: Arc::new(OnceLock::new()),
            durable_sync: raft_enabled,
            node_health_orphan_candidates: Arc::new(std::sync::Mutex::new(HashSet::new())),
            allocation_health_orphan_candidates: Arc::new(std::sync::Mutex::new(HashSet::new())),
            plan_coalescer: Arc::new(crate::storage::coalesce::Coalescer::new(
                Self::plan_apply_batch_max(config.scheduler_settings.as_ref()),
            )),
            heartbeat_coalescer: Arc::new(crate::storage::coalesce::Coalescer::new(
                HEARTBEAT_BATCH_MAX,
            )),
        };

        store.refresh_read_model()?;

        // Raft mode: start the engine AFTER migrations (the state machines
        // apply into the migrated DB) and BEFORE this store is handed to
        // anything serving traffic. `start_engine` installs the replication
        // pipeline and arms the raft-mode write guard; startup-time writes
        // that follow (requeue, manager-config upsert, seeds) run through
        // replication and are gated to the leader by the daemon.
        if raft_enabled {
            store.persistence_driver.clone().start_engine(&store)?;
        }

        Ok(store)
    }

    /// Clone handed to the raft engine (state machines, pipeline threads):
    /// shares the database connections, read model, event broadcast, eval
    /// notify, and the raft-mode flag with this store, but holds a
    /// placeholder persistence driver and a fresh (never-populated)
    /// replication cell. This breaks the reference cycle engine-owned store
    /// clones would otherwise create (engine → store → driver → engine), so
    /// dropping the last real store shuts the engine down.
    pub(crate) fn raft_engine_clone(&self) -> Store {
        let mut clone = self.clone();
        clone.persistence_driver =
            Arc::new(crate::storage::persistence::LocalDiskPersistenceDriver::new());
        clone.replication = Arc::new(OnceLock::new());
        clone
    }

    /// Clone installed in the transport registry as the `ForwardLaneWrite`
    /// executor (plan §8, D2). Like [`Self::raft_engine_clone`] it holds a
    /// placeholder persistence driver — the registry is (indirectly) owned
    /// by the driver, so a real-driver clone would be the reference cycle
    /// `driver → registry → store → driver` and the engine (with its data
    /// dir lock) would never drop — but unlike it, the REPLICATION cell is
    /// shared: the executor must propose forwarded ops through the real
    /// pipeline. The placeholder driver also makes re-forwarding
    /// structurally impossible (rule 2 of `raft::forward`): a leadership
    /// rejection inside the executor cannot resolve a lane leader to
    /// forward to, so it propagates typed to the origin.
    pub(crate) fn forward_executor_clone(&self) -> Store {
        let mut clone = self.clone();
        clone.persistence_driver =
            Arc::new(crate::storage::persistence::LocalDiskPersistenceDriver::new());
        clone
    }

    /// The registry cell the `RaftTransport` tonic service dispatches
    /// through. For non-raft backends this is a fresh, never-populated cell:
    /// the mounted service then answers `Unavailable`, which peers treat as
    /// "engine not started here".
    pub(crate) fn raft_transport_registry_cell(
        &self,
    ) -> Arc<OnceLock<crate::storage::raft::RaftTransportRegistry>> {
        self.persistence_driver
            .as_any()
            .downcast_ref::<crate::storage::raft::driver::MultiRaftDriver>()
            .map(|driver| driver.transport_registry_cell())
            .unwrap_or_default()
    }

    pub fn persistence_status(&self) -> PersistenceStatus {
        self.persistence_driver.status()
    }

    pub fn persistence_role(&self) -> PersistenceRole {
        self.persistence_status().role
    }

    pub fn raft_status(&self) -> Option<RaftStatus> {
        self.persistence_driver.raft_status()
    }

    /// Whether this manager may ACT for `lane` right now: in raft mode, true
    /// only while this node leads the raft group carrying the lane (a cheap
    /// metrics-watch probe, no I/O); in every other persistence mode, always
    /// true. The daemon's scheduling loops (eval workers, node status
    /// checker) poll this so followers idle and take over within a poll
    /// interval of a leadership change (plan JC-15).
    pub(crate) fn is_raft_leader(&self, lane: Lane) -> bool {
        match self
            .persistence_driver
            .as_any()
            .downcast_ref::<crate::storage::raft::driver::MultiRaftDriver>()
        {
            Some(driver) => driver.is_leader(lane),
            None => true,
        }
    }

    /// The tonic service serving this store's manager-plane raft transport
    /// (`RaftTransport` in `proto/legion.proto`). The daemon mounts it on
    /// the manager listener; in-process multi-node tests mount it on their
    /// own ephemeral listeners. For non-raft backends the service's registry
    /// cell is never populated, so every RPC answers `Unavailable`.
    pub fn raft_transport_server(
        &self,
    ) -> crate::structs::raft_transport_server::RaftTransportServer<
        crate::storage::raft::server::RaftTransportService,
    > {
        crate::structs::raft_transport_server::RaftTransportServer::new(
            crate::storage::raft::server::RaftTransportService::new(
                self.raft_transport_registry_cell(),
            ),
        )
        // Raise tonic's 4 MiB default: openraft snapshot chunks and
        // append-entries batches are serde_json-encoded (~4x byte blowup)
        // and must never be rejected for size, or the peer hot-retries the
        // oversized message forever (see raft::network::RAFT_MAX_MESSAGE_BYTES).
        .max_decoding_message_size(crate::storage::raft::network::RAFT_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(crate::storage::raft::network::RAFT_MAX_MESSAGE_BYTES)
    }

    /// Partition test hook (in-process durability tests only — not an
    /// operator surface): while blocked, every outbound raft transport RPC
    /// from this node fails as unreachable before dialing. Paired with
    /// dropping the node's inbound transport listener this fully isolates
    /// the node from its peers — the in-process equivalent of yanking its
    /// network cable (`tests/raft_durability.rs`). Errors on non-raft
    /// backends and before the raft engine has started.
    pub fn raft_set_outbound_blocked(&self, blocked: bool) -> R<()> {
        self.persistence_driver
            .as_any()
            .downcast_ref::<crate::storage::raft::driver::MultiRaftDriver>()
            .context("raft_set_outbound_blocked requires the raft persistence backend")?
            .set_outbound_blocked(blocked)
    }

    /// Deterministic fingerprint of this replica's replicated state: every
    /// user table except the ones that legitimately differ between healthy
    /// replicas (plan §7-C2):
    ///
    /// - `raft_applied` (applied marker legitimately lags on followers) and
    ///   `raft_cluster` (node-local identity stamp) — raft bookkeeping,
    ///   never replicated;
    /// - `scheduler_event` / `idempotency_record` — commutative tables
    ///   pruned locally per node (JC-28), so their retained windows differ.
    ///
    /// `eval` is deliberately INCLUDED again (JC-41 reverses JC-27): the
    /// queue is replicated scheduler-lane state, so replicas that disagree
    /// about pending work have diverged — exactly what this fingerprint
    /// exists to catch.
    ///
    /// Rows are ordered by every column. Two replicas that converged
    /// report identical hashes. Used by the raft convergence/determinism
    /// tests and usable for operator replica comparison.
    pub fn state_content_hash(&self) -> R<String> {
        let tables: Vec<String> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(
                "SELECT name FROM sqlite_master
                 WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                   AND name NOT IN ('raft_applied', 'raft_cluster',
                                    'scheduler_event', 'idempotency_record')
                 ORDER BY name;",
            )?;
            let rows = stmt.query_map([], |row| row.get(0))?;
            Self::collect_rows(rows)?
        };
        let refs: Vec<&str> = tables.iter().map(String::as_str).collect();
        self.state_content_hash_for_tables(&refs)
    }

    /// [`Store::state_content_hash`] restricted to an explicit table list —
    /// lets tests fingerprint one raft lane's table group at a time (plan
    /// §7-C3: a lane's snapshot install must converge its own tables and
    /// leave sibling lanes' groups untouched). Tables missing from the
    /// schema are an error, not silently skipped.
    pub fn state_content_hash_for_tables(&self, tables: &[&str]) -> R<String> {
        let mut tables: Vec<&str> = tables.to_vec();
        tables.sort_unstable();
        let conn = self.read_connection()?;
        let mut content = String::new();
        for table in &tables {
            // Order by every column ordinal so the fingerprint is
            // independent of physical row order (WITHOUT ROWID tables and
            // vacuumed tables included).
            let cols = {
                let stmt = conn.prepare(&format!("SELECT * FROM \"{table}\" LIMIT 0;"))?;
                stmt.column_count()
            };
            let order: Vec<String> = (1..=cols).map(|i| i.to_string()).collect();
            let mut stmt = conn.prepare(&format!(
                "SELECT * FROM \"{table}\" ORDER BY {};",
                order.join(", ")
            ))?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                content.push_str(table);
                content.push(':');
                for i in 0..cols {
                    let value: rusqlite::types::Value = row.get(i)?;
                    content.push_str(&format!("{value:?}|"));
                }
                content.push('\n');
            }
        }
        Ok(format!(
            "{:016x}",
            crate::storage::raft::types::fnv1a64(&content)
        ))
    }

    /// The tables a raft lane single-writes, by lane wire name (`control` /
    /// `scheduler` / `status`) — the table groups of plan §7-C2. Public so
    /// integration tests can fingerprint per lane via
    /// [`Store::state_content_hash_for_tables`]. The commutative tables
    /// (`scheduler_event`, `idempotency_record`) are excluded for the same
    /// reason [`Store::state_content_hash`] excludes them: their locally
    /// pruned retention windows legitimately differ between healthy
    /// replicas (JC-28).
    pub fn raft_lane_table_group(lane: &str) -> R<Vec<&'static str>> {
        let lane = crate::storage::raft::Lane::from_wire_name(lane)
            .with_context(|| format!("unknown raft lane `{lane}`"))?;
        Ok(
            crate::storage::raft::ownership::snapshot_tables_for_lane(lane)
                .into_iter()
                .filter(|table| {
                    crate::storage::raft::ownership::owner_lane(table)
                        != Some(crate::storage::raft::ownership::Owner::Commutative)
                })
                .collect(),
        )
    }

    /// Test hook (lanes battery): trigger an election for ONE lane's raft
    /// group on this node, by lane wire name. The operator surface
    /// (`raft command trigger-election`) elects every lane at once; proving
    /// elections are independent requires moving a single lane's
    /// leadership. Errors on non-raft backends, before engine start, and
    /// for lanes the topology does not run.
    pub fn raft_trigger_lane_election(&self, lane: &str) -> R<()> {
        let lane = crate::storage::raft::Lane::from_wire_name(lane)
            .with_context(|| format!("unknown raft lane `{lane}`"))?;
        self.persistence_driver
            .as_any()
            .downcast_ref::<crate::storage::raft::driver::MultiRaftDriver>()
            .context("raft_trigger_lane_election requires the raft persistence backend")?
            .trigger_lane_election(lane)
    }

    /// Test hook (lanes battery): pause/resume this node's JC-40
    /// anchor-lane convergence loop. Production never calls it — the
    /// leadership-INDEPENDENCE test needs to hold the three lanes under
    /// DIFFERENT leaders long enough to assert on that state, and the
    /// control leader's convergence campaigns would pull them back
    /// (which is, in production, the whole point). Errors on non-raft
    /// backends; a no-op under `single` topology, where no convergence loop
    /// runs.
    pub fn raft_set_lane_convergence(&self, enabled: bool) -> R<()> {
        self.persistence_driver
            .as_any()
            .downcast_ref::<crate::storage::raft::driver::MultiRaftDriver>()
            .context("raft_set_lane_convergence requires the raft persistence backend")?
            .set_lane_convergence(enabled);
        Ok(())
    }

    /// Test hook (lanes battery): how many JC-40 convergence campaigns this
    /// node has issued for `lane` since its engine started.
    ///
    /// Convergence is best-effort and its campaigns bump raft terms, so the
    /// battery asserts the BOUND on them directly (a handful per cooldown
    /// window, per lane) rather than inferring it from term numbers, which
    /// also move for reasons convergence does not control. Errors on
    /// non-raft backends and for an unknown lane name; always `0` under
    /// `single` topology and for the anchor lane, which is never campaigned
    /// for.
    pub fn raft_lane_campaigns(&self, lane: &str) -> R<u64> {
        let lane = crate::storage::Lane::from_wire_name(lane)
            .with_context(|| format!("unknown raft lane `{lane}`"))?;
        Ok(self
            .persistence_driver
            .as_any()
            .downcast_ref::<crate::storage::raft::driver::MultiRaftDriver>()
            .context("raft_lane_campaigns requires the raft persistence backend")?
            .lane_campaigns(lane))
    }

    /// Test hook (lanes durability battery): process-global count of
    /// capture-time single-writer ownership-guard violations (JC-26) since
    /// process start — every changeset the guard found touching a table
    /// outside its op's lane, whether the op was then rejected (enforcing,
    /// the only live path since C-2) or warned away (the dormant grace
    /// path). The durability tests assert their whole workload leaves this
    /// counter untouched: zero must mean "nothing ever tripped the guard",
    /// not "trips were retried into silence". Process-global on purpose —
    /// in-process multi-node clusters share it, so one assertion covers
    /// every node.
    pub fn raft_ownership_guard_violations() -> u64 {
        crate::storage::raft::ownership::violation_count()
    }

    pub fn raft_history(&self, limit: usize) -> Vec<RaftHistoryEvent> {
        self.persistence_driver.raft_history(limit)
    }

    pub fn issue_raft_command(
        &self,
        command: &str,
        target: Option<&str>,
        actor: &str,
    ) -> R<crate::storage::persistence::RaftCommandOutcome> {
        self.persistence_driver
            .issue_raft_command(command, target, actor)
    }

    pub fn issue_raft_join_token(&self, actor: &str) -> R<(String, String)> {
        self.persistence_driver.issue_raft_join_token(actor)
    }

    /// Node counts grouped by status (`active`, `lost`, ...) for the
    /// `legion_nodes{status}` gauge. One indexed aggregate on a read
    /// connection — cheap enough for the node-status-checker tick.
    pub fn node_status_counts(&self) -> R<Vec<(String, i64)>> {
        let conn = self.read_connection()?;
        // Liveness lives in node_health (plan §7-C2); a node without a
        // health row hydrates the registration default 'active'.
        let mut stmt = conn.prepare(
            "SELECT COALESCE(node_health.status, 'active') AS status, COUNT(*)
             FROM node LEFT JOIN node_health ON node_health.node_id = node.id
             GROUP BY 1;",
        )?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Self::collect_rows(rows)
    }

    /// Total live allocations, for the `legion_allocations` gauge.
    pub fn allocation_count(&self) -> R<i64> {
        let conn = self.read_connection()?;
        Ok(conn.query_row("SELECT COUNT(*) FROM allocation;", [], |row| row.get(0))?)
    }

    #[instrument(err, skip_all)]
    fn db_path(store_path: &str) -> R<std::path::PathBuf> {
        // let conn = Connection::open_in_memory()?;

        // Using the disk version for now to simulate logs
        // Would eventually split this into a cluster that shares
        // logs via raft before committing to an in-memory sqlite db

        std::fs::create_dir_all(store_path)?;
        // The store holds secret material — ACL/join-token hashes and the OIDC
        // private signing key (a leak of which lets anyone mint workload
        // identities trusted by AWS/GCP/Vault). SQLite creates store.sqlite and
        // its WAL/-shm siblings at the process umask (commonly 0644), so lock
        // the containing directory to owner-only; the 0700 dir denies traversal
        // to those files regardless of their own modes.
        crate::util::restrict_dir_to_owner(std::path::Path::new(store_path))?;
        let tmp = std::path::Path::new(store_path).join("store.sqlite");
        Ok(tmp)
    }

    #[instrument(err, skip_all)]
    fn create_write_conn(db_path: &Path, durable_sync: bool) -> R<MemSafeConn> {
        let final_path = db_path.to_str().context("Error parsing store path")?;
        let conn = Connection::open(final_path)?;
        Self::configure_connection(&conn, false, durable_sync)?;
        Ok(Arc::new(Mutex::new(conn)))
    }

    #[instrument(err, skip_all)]
    fn create_readonly_conn(db_path: &Path) -> R<MemSafeConn> {
        let final_path = db_path.to_str().context("Error parsing store path")?;
        let conn = Connection::open_with_flags(final_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        Self::configure_connection(&conn, true, false)?;
        conn.pragma_update(None, "query_only", "ON")?;
        Ok(Arc::new(Mutex::new(conn)))
    }

    /// `durable_sync` (raft mode) runs the write connection at
    /// `synchronous = FULL` instead of `NORMAL`. Raft purges its log behind
    /// snapshots, so the "state is recoverable from the log" argument that
    /// justifies NORMAL for local_disk does not hold past the purge horizon:
    /// after a power loss, an applied marker that regressed below
    /// `last_purged_log_id` (WAL not yet checkpointed) would make openraft's
    /// startup reapply fail permanently — a bricked node. FULL makes every
    /// applied entry fsync before it is acknowledged, so the marker can
    /// never regress past durable state.
    fn configure_connection(conn: &Connection, read_only: bool, durable_sync: bool) -> R<()> {
        conn.busy_timeout(SQLITE_BUSY_TIMEOUT)?;

        if !read_only {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            let synchronous = if durable_sync { "FULL" } else { "NORMAL" };
            conn.pragma_update(None, "synchronous", synchronous)?;
        }

        conn.pragma_update(None, "foreign_keys", "ON")?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    fn create_read_pool(db_path: &Path) -> R<Vec<MemSafeConn>> {
        let mut read_conns = Vec::with_capacity(READ_ONLY_CONN_POOL_SIZE);
        for _ in 0..READ_ONLY_CONN_POOL_SIZE {
            read_conns.push(Self::create_readonly_conn(db_path)?);
        }
        Ok(read_conns)
    }

    #[allow(dead_code)]
    pub fn reset_all_tables(&self) -> R<()> {
        let migrations = get_migrations();
        let mut conn = self.write_connection()?;
        migrations.to_version(&mut conn, 0)?;
        migrations.to_latest(&mut conn)?;
        drop(conn);
        self.refresh_read_model()?;

        Ok(())
    }

    #[instrument(skip_all)]
    fn create_initial_tables(conn: &MemSafeConn) -> R<Migrations<'_>> {
        let migrations = get_migrations();
        let mut conn = Self::lock_conn(conn, "migration")?;
        info!("Running migrations");
        migrations.to_latest(&mut conn)?;

        Ok(migrations)
    }

    fn lock_conn<'a>(
        conn: &'a MemSafeConn,
        connection_kind: &str,
    ) -> R<MutexGuard<'a, Connection>> {
        let lock_wait_started = Instant::now();
        let guard = conn.lock().map_err(|_| anyhow!(CONN_LOCK_ERROR))?;
        let waited = lock_wait_started.elapsed();
        if connection_kind == "write" {
            crate::manager::telemetry::metrics()
                .store_write_lock_wait_seconds
                .observe(waited.as_secs_f64());
        }
        debug!(
            connection_kind,
            lock_wait_ms = waited.as_millis(),
            "Acquired sqlite connection lock"
        );
        Ok(guard)
    }

    pub(crate) fn write_connection(&self) -> R<MutexGuard<'_, Connection>> {
        // Raft-mode bypass guard (JC-14): every mutation must flow through
        // `replicated()` (which captures on the pipeline thread), the raft
        // apply path, or the scoped `local_write` seam for node-local
        // maintenance (JC-28 journal pruning; the eval queue left this seam
        // with JC-41). A write-connection grab from any other context is
        // an unclassified write path — fail it loudly instead of letting it
        // mutate this replica alone.
        if self.raft_mode.load(Ordering::Acquire)
            && !capture::in_capture()
            && !capture::in_apply()
            && !capture::in_local_write()
        {
            return Err(anyhow!(
                "write_connection called outside capture/apply/local-write in raft mode: route \
                 this mutation through Store::replicated() so it reaches quorum before applying \
                 (or Store::local_write for node-local maintenance — plan §7-C2)"
            ));
        }
        Self::lock_conn(&self.write_conn, "write")
    }

    /// Whether the raft engine has taken ownership of the write path.
    pub(crate) fn raft_mode(&self) -> bool {
        self.raft_mode.load(Ordering::Acquire)
    }

    /// Flip the store into (or out of) raft mode. Called by the raft engine
    /// once startup-time writes (migrations, manager-config upsert) have
    /// completed; from then on the `write_connection` bypass guard is armed.
    /// The eval-queue recovery the daemon runs at startup happens AFTER this
    /// flip and goes through the scheduler lane like any other eval write
    /// (JC-41).
    pub(crate) fn set_raft_mode(&self, enabled: bool) {
        self.raft_mode.store(enabled, Ordering::Release);
    }

    /// Install the replication pipeline handle. One-shot: the engine sets it
    /// exactly once at start; a second call is a startup-sequencing bug.
    pub(crate) fn set_replication_handle(&self, handle: ReplicationHandle) -> R<()> {
        self.replication
            .set(handle)
            .map_err(|_| anyhow!("raft replication pipeline handle already installed"))
    }

    /// The replication seam every public mutating store method routes
    /// through (plan §2, JC-1/JC-12/JC-13):
    ///
    /// - `local_disk`/`streaming` mode: run `f(self)` inline — semantics are
    ///   byte-identical to the pre-raft store.
    /// - Inside an active capture on this thread: run `f(self)` inline, so
    ///   nested public-method calls compose into the outer capture and a
    ///   composite operation produces ONE atomic raft entry (JC-13).
    /// - Raft mode: hand `(lane, op, f)` to the replication pipeline, which
    ///   captures `f`'s row effects on its thread, proposes them via raft,
    ///   and unblocks this caller with `f`'s return value after local apply.
    pub(crate) fn replicated<T: Send + 'static>(
        &self,
        lane: Lane,
        op: &'static str,
        f: impl FnOnce(&Store) -> R<T> + Send + 'static,
    ) -> R<T> {
        if capture::in_capture() || !self.raft_mode() {
            return f(self);
        }

        let Some(handle) = self.replication.get() else {
            return Err(anyhow!(
                "raft replication pipeline not started (op {op}, lane {lane:?})"
            ));
        };
        let erased: ReplicatedOp = Box::new(move |store: &Store| {
            f(store).map(|value| Box::new(value) as Box<dyn std::any::Any + Send>)
        });
        let boxed = (handle.submit)(lane, op, erased)?;
        boxed
            .downcast::<T>()
            .map(|value| *value)
            .map_err(|_| anyhow!("replicated op {op} returned a value of an unexpected type"))
    }

    /// A scoped LOCAL write (plan §7-C2): committed to this node's
    /// database only, never replicated. The ONLY legal write path for
    /// local-only tables (a class JC-41 emptied — the eval queue is
    /// replicated again) and for local pruning of
    /// commutative tables (JC-28, the one live caller); in debug builds (raft mode) the write's
    /// changes are asserted to stay inside that scope, so an exempt path
    /// can never silently grow. See `capture::local_write` for the locking
    /// and assertion details.
    pub(crate) fn local_write<T>(&self, op: &'static str, f: impl FnOnce(&Store) -> R<T>) -> R<T> {
        capture::local_write(self, op, f)
    }

    /// Forward `op` to its lane's leader after a local typed rejection
    /// (plan §8, D2 — the lane-write forwarding seam). Called by the
    /// forwarding wrappers around worker-plane and daemon writes whose loss
    /// cascades (heartbeats → false node-lost + self-fencing; failure
    /// reports → silent loss; availability sync → lost nodes staying
    /// schedulable) and whose callers cannot chase per-lane leaders.
    ///
    /// The error contract is preserved end to end: every failure path out
    /// of the forward carries a typed `NotRaftLeader` in its chain EXCEPT a
    /// real execution failure on the leader, which propagates as itself —
    /// callers' tolerance logic sees exactly the surface it saw before
    /// forwarding existed, plus successes it could not have had.
    fn forward_after_rejection<T: serde::de::DeserializeOwned>(
        &self,
        op: crate::storage::raft::forward::ForwardableLaneWrite,
        original: anyhow::Error,
    ) -> R<T> {
        let Some(driver) = self
            .persistence_driver
            .as_any()
            .downcast_ref::<crate::storage::raft::driver::MultiRaftDriver>()
        else {
            // Non-raft backends never produce the typed rejection, so this
            // is defensive: surface what actually happened.
            return Err(original);
        };
        match driver.forward_lane_write(&op) {
            Ok(value) => {
                debug!(
                    op = op.op_name(),
                    lane = op.lane().wire_name(),
                    "lane write forwarded to the lane leader"
                );
                serde_json::from_value(value).with_context(|| {
                    format!("undecodable forwarded return value for {}", op.op_name())
                })
            }
            Err(forward_err) => {
                warn!(
                    op = op.op_name(),
                    lane = op.lane().wire_name(),
                    error = %forward_err,
                    "lane-write forwarding did not complete"
                );
                Err(forward_err)
            }
        }
    }

    /// Open a write transaction as a SAVEPOINT (JC-5). `BEGIN` cannot run
    /// inside the capture savepoint, and a top-level SQLite savepoint
    /// behaves exactly like a deferred transaction, so this is the one
    /// mechanical replacement for every `conn.transaction()` call site.
    /// `Savepoint` derefs to `Connection` and rolls back on drop unless
    /// `commit()` is called — the same contract `Transaction` had.
    pub(crate) fn write_tx(conn: &mut Connection) -> R<rusqlite::Savepoint<'_>> {
        Ok(rusqlite::Savepoint::new(conn)?)
    }

    // ── snapshots / backup / restore (plan §A5, phase A-3) ───────────────

    /// Filesystem path of this store's state database.
    pub(crate) fn state_db_path(&self) -> &std::path::Path {
        &self.db_path
    }

    /// Highest applied raft log index recorded in `raft_applied` on `conn`
    /// (max across lanes; `0` when nothing was ever applied — including
    /// non-raft stores, whose table is empty).
    fn max_applied_index_on(conn: &Connection) -> R<u64> {
        let mut stmt = conn.prepare("SELECT last_applied FROM raft_applied;")?;
        let rows = stmt.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
        let mut max_index = 0u64;
        for row in rows {
            let bytes = row?;
            let log_id: openraft::LogId<u64> = serde_json::from_slice(&bytes)
                .context("failed decoding raft_applied last_applied")?;
            max_index = max_index.max(log_id.index);
        }
        Ok(max_index)
    }

    /// Write a consistent point-in-time backup of the state database to
    /// `out` (or a timestamped default under `<storePath>/backups/`) via
    /// `VACUUM INTO`, and return where it landed, its size, and the raft
    /// applied index it captures (plan §A5).
    ///
    /// **What this is, precisely.** The image is a transactionally
    /// consistent copy of THIS node's applied state at one wall-clock
    /// moment — the whole database file, every lane's tables. It is not a
    /// cluster-wide snapshot at one raft index: under `lanes` the three
    /// groups apply independently, so the image sits at a different applied
    /// index per lane, and `applied_index` in the reply is the MAX across
    /// lanes (a progress marker, not a cut). That is exactly what restore
    /// consumes — `--restore-from` seeds a FRESH cluster from this file
    /// (JC-27 clears the cluster stamp), it never rejoins a running one —
    /// so a per-node point-in-time image is the right and only meaningful
    /// artifact. Documented for operators in `docs/multi-raft.md`.
    ///
    /// Works in every persistence mode. In raft mode it requires leadership
    /// of EVERY lane: a node missing any lane's leadership returns the typed
    /// [`NotRaftLeader`] error for that lane (with its leader hint) so the
    /// RPC layer maps it to `FAILED_PRECONDITION`. A follower's DB is a
    /// valid replica, but "the backup" should be the authoritative line of
    /// history for all of it — and since JC-40 co-locates lane leadership on
    /// one node, requiring all lanes costs nothing in steady state while
    /// refusing the one case where a single-lane check would have lied: a
    /// node that leads `control` but lags on another lane, whose image would
    /// be the freshest for one lane and stale for the others.
    /// (Under `single`, every lane maps to the one group, so this is the
    /// same check it always was.)
    ///
    /// The vacuum runs under [`capture::exclusive_state_access`]: the
    /// capture mutex plus the held write-connection guard guarantee the
    /// image is a transactionally consistent snapshot with no capture
    /// savepoint or half-applied changeset in it.
    ///
    /// [`NotRaftLeader`]: crate::storage::raft::driver::NotRaftLeader
    pub fn state_backup(&self, out: Option<&std::path::Path>) -> R<StateBackupInfo> {
        if self.raft_mode() {
            if let Some(driver) = self
                .persistence_driver
                .as_any()
                .downcast_ref::<crate::storage::raft::driver::MultiRaftDriver>()
            {
                // All-lane leadership (JC-40 co-location): the image spans
                // every lane's tables, so it must come from the node that
                // leads every lane.
                for lane in Lane::ALL {
                    if !driver.is_leader(lane) {
                        return Err(anyhow::Error::new(driver.not_leader_error(lane)).context(
                            "state backup must run on the node that leads every raft lane (the \
                             image covers all lanes' tables)",
                        ));
                    }
                }
            }
        }

        let dest = match out {
            Some(path) => path.to_path_buf(),
            None => {
                let dir = self
                    .db_path
                    .parent()
                    .context("state db path has no parent directory")?
                    .join("backups");
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                dir.join(format!("legion-state-backup-{nanos}.sqlite"))
            }
        };
        if dest.exists() {
            bail!(
                "backup destination {} already exists (VACUUM INTO refuses to overwrite); \
                 pass a fresh path",
                dest.display()
            );
        }
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed creating backup directory {}", parent.display())
            })?;
        }
        let dest_str = dest
            .to_str()
            .context("backup destination path is not utf-8")?
            .to_string();

        let applied_index = capture::exclusive_state_access(self, |conn| {
            // Applied index and image are read under the same exclusive
            // connection guard, so they describe the same state.
            let applied_index = Self::max_applied_index_on(conn)?;
            conn.execute("VACUUM INTO ?1;", [&dest_str])
                .with_context(|| format!("failed running VACUUM INTO {}", dest.display()))?;
            Ok(applied_index)
        })?;
        let bytes = std::fs::metadata(&dest)
            .with_context(|| format!("backup file {} missing after vacuum", dest.display()))?
            .len();
        info!(
            path = %dest.display(),
            bytes,
            applied_index,
            "Wrote state store backup"
        );
        Ok(StateBackupInfo {
            path: dest,
            bytes,
            applied_index,
        })
    }

    /// Copy `backup` into place as `<store_path>/store.sqlite` — the
    /// `--restore-from` path on `legion start manager` (plan §A5). Runs
    /// BEFORE `start_storage`; refuses to overwrite an existing state DB
    /// unless `force` is set.
    ///
    /// The copied database's `raft_applied` rows are cleared: a restore
    /// seeds a *fresh* cluster (or a fresh local_disk store) from the
    /// backup's contents, and stale applied/membership markers from the
    /// backup's original cluster would otherwise make the new raft engine
    /// believe it had applied logs (and had members) it has never seen.
    /// The `raft_cluster` identity stamp is cleared for the same reason: a
    /// backup taken on a raft cluster carries the OLD cluster's stamp, and
    /// leaving it in place would make the documented restore flow (restore
    /// onto a fresh bootstrap node) trip the re-bootstrap refusal in
    /// `check_cluster_stamp`. Clearing it here is sound because
    /// `--restore-from` is an explicit operator action whose meaning IS
    /// "seed a fresh cluster from these contents" — the accidental-wipe
    /// path the stamp protects against never goes through a restore.
    /// With both cleared, raft bootstrap adopts the DB contents as its
    /// initial state and stamps its own identity — the documented
    /// `docs/upgrades.md` upgrade path.
    pub fn restore_from_backup(
        store_path: &str,
        backup: &std::path::Path,
        force: bool,
    ) -> R<std::path::PathBuf> {
        let db_path = Self::db_path(store_path)?;
        if db_path.exists() && !force {
            bail!(
                "refusing to restore over existing state at {}: move it aside or pass \
                 --force-restore to overwrite",
                db_path.display()
            );
        }
        // Validate the backup is a SQLite database before touching anything.
        let header = std::fs::read(backup)
            .with_context(|| format!("failed reading backup {}", backup.display()))?;
        if !header.starts_with(b"SQLite format 3\0") {
            bail!(
                "{} is not a SQLite database (expected a file produced by `legion state backup`)",
                backup.display()
            );
        }

        // Stage next to the destination so the final rename is atomic on the
        // same filesystem: a kill mid-restore leaves either no store.sqlite
        // (restore re-runnable) or a complete one.
        let staged = db_path.with_extension("restore-tmp");
        let _ = std::fs::remove_file(&staged);
        std::fs::copy(backup, &staged)
            .with_context(|| format!("failed copying backup to {}", staged.display()))?;
        {
            let conn = Connection::open(&staged)
                .with_context(|| format!("failed opening staged restore {}", staged.display()))?;
            // Older backups predate these tables; ignore their absence.
            for table in ["raft_applied", "raft_cluster"] {
                match conn.execute(&format!("DELETE FROM {table};"), []) {
                    Ok(_) => {}
                    Err(rusqlite::Error::SqliteFailure(_, Some(ref msg)))
                        if msg.contains("no such table") => {}
                    Err(e) => {
                        return Err(e).with_context(|| {
                            format!("failed clearing {table} in restored state db")
                        })
                    }
                }
            }
        }
        let staged_file = std::fs::File::open(&staged)?;
        staged_file.sync_all()?;
        drop(staged_file);
        // Discard any stale WAL/SHM sidecars of the DB being replaced —
        // they belong to the old file and must never be recovered into the
        // restored one.
        let _ = std::fs::remove_file(db_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(db_path.with_extension("sqlite-shm"));
        std::fs::rename(&staged, &db_path).with_context(|| {
            format!("failed installing restored state db {}", db_path.display())
        })?;
        info!(
            backup = %backup.display(),
            path = %db_path.display(),
            "Restored state store from backup"
        );
        Ok(db_path)
    }

    /// Atomically replace the state database with `new_db` and reopen every
    /// connection — the snapshot-install hook (plan §A5). `new_db` must be a
    /// complete, fsynced SQLite database on the same filesystem as the state
    /// DB's directory is reachable from (it is `rename(2)`d into place).
    ///
    /// # Crash safety (the "either old or new" proof)
    ///
    /// Ordered steps, with what a kill after each leaves on disk:
    ///
    /// 1. **Close every connection** (read pool first, write conn last).
    ///    Closing the last connection makes SQLite checkpoint the WAL into
    ///    the main file and remove `-wal`; any leftover sidecars are
    ///    deleted explicitly. Kill here → `store.sqlite` is the OLD
    ///    database, transactionally consistent, whose internal
    ///    `raft_applied` matches its data (they always commit atomically).
    /// 2. **`rename(new_db, store.sqlite)`** — atomic on POSIX. Kill
    ///    before → old DB (case 1); kill after → `store.sqlite` is the NEW
    ///    database, whose `raft_applied` was written into the file (with
    ///    the snapshot's meta) *before* this function was called. There is
    ///    no intermediate state: the WAL sidecars were removed before the
    ///    rename, so a restart can never replay old-WAL frames into the
    ///    new file.
    /// 3. **Reopen + rebuild.** Kill here → same as "after rename": the
    ///    restart path opens `store.sqlite`, reads `raft_applied`, and
    ///    resumes from exactly the applied index the surviving file
    ///    reports — openraft replays or re-installs from there.
    ///
    /// In every case `store.sqlite` is one consistent database whose
    /// applied marker matches its contents, which is all recovery needs.
    pub(crate) fn install_state_db(&self, new_db: &std::path::Path) -> R<()> {
        // Exclude captures for the whole swap: a capture holds an open
        // savepoint on the write connection between statements, and a
        // committed-changeset apply serializes on the connection guard we
        // hold below.
        let _captures = capture::capture_exclusion()?;

        // Hold every connection guard simultaneously (write first, then the
        // read pool — the one fixed acquisition order for multi-connection
        // holds) so no statement can run against a half-swapped store.
        let mut write_guard = Self::lock_conn(&self.write_conn, "install")?;
        let mut read_guards: Vec<MutexGuard<'_, Connection>> = Vec::new();
        for conn in self.read_conns.iter() {
            read_guards.push(Self::lock_conn(conn, "install")?);
        }

        // Step 1: close the old connections (readers first; the write
        // connection closes last so SQLite's last-close WAL checkpoint runs
        // against the old file). The placeholder in-memory connections keep
        // the mutexes valid if any later step fails.
        for guard in read_guards.iter_mut() {
            let old = std::mem::replace(&mut **guard, Connection::open_in_memory()?);
            drop(old);
        }
        let old = std::mem::replace(&mut *write_guard, Connection::open_in_memory()?);
        drop(old);
        let wal = self.db_path.with_extension("sqlite-wal");
        let shm = self.db_path.with_extension("sqlite-shm");
        let _ = std::fs::remove_file(&wal);
        let _ = std::fs::remove_file(&shm);

        // Step 2: the atomic swap.
        std::fs::rename(new_db, &*self.db_path).with_context(|| {
            format!(
                "failed renaming snapshot {} over state db {}",
                new_db.display(),
                self.db_path.display()
            )
        })?;
        // Make the rename itself durable before anyone builds on it.
        #[cfg(unix)]
        if let Some(parent) = self.db_path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }

        // Step 3: reopen against the new file.
        let new_write = Connection::open(&*self.db_path)?;
        Self::configure_connection(&new_write, false, self.durable_sync)?;
        *write_guard = new_write;
        for guard in read_guards.iter_mut() {
            let conn =
                Connection::open_with_flags(&*self.db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            Self::configure_connection(&conn, true, false)?;
            conn.pragma_update(None, "query_only", "ON")?;
            **guard = conn;
        }
        drop(read_guards);
        drop(write_guard);

        // The read model caches rows from the replaced database; rebuild it
        // from the installed one.
        self.refresh_read_model()?;
        info!(path = %self.db_path.display(), "Installed replacement state database");
        Ok(())
    }

    pub(crate) fn read_connection(&self) -> R<MutexGuard<'_, Connection>> {
        let pool = self.read_conns.as_slice();
        let index = self.read_index.fetch_add(1, Ordering::Relaxed);
        let conn = pool.get(index % pool.len()).unwrap_or(&self.write_conn);
        Self::lock_conn(conn, "read")
    }

    fn read_model_reader(&self) -> R<std::sync::RwLockReadGuard<'_, SchedulingReadModel>> {
        self.read_model
            .read()
            .map_err(|_| anyhow!(READ_MODEL_LOCK_ERROR))
    }

    fn read_model_writer(&self) -> R<std::sync::RwLockWriteGuard<'_, SchedulingReadModel>> {
        self.read_model
            .write()
            .map_err(|_| anyhow!(READ_MODEL_LOCK_ERROR))
    }

    fn load_read_model(&self) -> R<SchedulingReadModel> {
        let conn = self.read_connection()?;
        // Epochs are read BEFORE the allocation snapshot (the mirror's
        // coherence invariant — see `SchedulingReadModel::scope_epochs`): a
        // commit landing between the two queries then leaves the mirror
        // BEHIND the allocations, which the plan-apply CAS resolves as a
        // conservative reject, never as phantom capacity.
        let scope_epochs: HashMap<String, i64> = {
            let mut stmt = conn
                .prepare("SELECT scope_type, scope_id, epoch FROM allocation_epoch_partition;")?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;
            let mut epochs = HashMap::new();
            for row in rows {
                let (scope_type, scope_id, epoch) = row?;
                epochs.insert(Self::scope_vector_key(&scope_type, &scope_id), epoch);
            }
            epochs
        };
        let mut nodes_stmt = conn.prepare(GET_NODES.as_str())?;
        let nodes = Self::collect_rows(nodes_stmt.query_map([], |row| self.node_from_row(row))?)?;

        let mut task_stmt = conn.prepare(
            r#"
            SELECT id, name, runtime, count, count_per, group_name, lifecycle, schedule, depends_on, config, resources, constraints, permit, affinities, checks, on_unhealthy, kv_injections, job_config_id, identities, stateful, artifacts, network, service, actions, volumes, connect, restart, stop, disruption
            FROM task_config;
            "#,
        )?;
        let task_rows = task_stmt.query_map([], |row| {
            let id = row.get(0)?;
            let count_num: u32 = row.get::<_, i64>(3)? as u32;
            let count_per: Option<String> = row.get(4)?;
            let group: String = row.get(5)?;
            let lifecycle = self.parse_task_lifecycle_column(6, row.get(6)?)?;
            let schedule = self.parse_schedule_column(7, row.get(7)?)?;
            let depends_on = self.parse_depends_on_column(8, row.get(8)?)?;
            let task = TaskConfig {
                name: row.get(1)?,
                runtime: row.get(2)?,
                count: TaskCount {
                    num: count_num,
                    per: count_per,
                },
                group,
                lifecycle,
                schedule,
                depends_on,
                config: row.get(9)?,
                resources: row.get(10)?,
                constraints: row.get(11)?,
                permit: row.get(12)?,
                affinities: row.get(13)?,
                checks: self.parse_checks_column(14, row.get(14)?)?,
                on_unhealthy: self.parse_on_unhealthy_column(15, row.get(15)?)?,
                kv_injections: self.parse_kv_injections_column(16, row.get(16)?)?,
                identities: self.parse_identities_column(18, row.get(18)?)?,
                stateful: self.parse_stateful_column(19, row.get(19)?)?,
                artifacts: self.parse_artifacts_column(20, row.get(20)?)?,
                network: parse_json_column(21, row.get(21)?)?,
                service: parse_json_column(22, row.get(22)?)?,
                actions: self.parse_actions_column(23, row.get(23)?)?,
                volumes: self.parse_volumes_column(24, row.get(24)?)?,
                connect: parse_json_column(25, row.get(25)?)?,
                restart: parse_json_column(26, row.get(26)?)?,
                stop: parse_json_column(27, row.get(27)?)?,
                disruption: parse_json_column(28, row.get(28)?)?,
            };
            let job_config_id: String = row.get(17)?;
            Ok((job_config_id, id, task))
        })?;

        let mut task_configs: HashMap<String, Vec<(i64, TaskConfig)>> = HashMap::new();
        for row in task_rows {
            let (job_config_id, id, task) = row?;
            task_configs
                .entry(job_config_id)
                .or_default()
                .push((id, task));
        }

        let mut allocs_stmt = conn.prepare(GET_ALLOCS.as_str())?;
        let allocs =
            Self::collect_rows(allocs_stmt.query_map([], |row| self.allocation_from_row(row))?)?;
        let mut grouped: HashMap<String, Vec<Allocation>> = HashMap::new();
        for alloc in allocs {
            grouped
                .entry(alloc.node_id.clone())
                .or_default()
                .push(alloc);
        }
        let allocations_by_node: SharedNodeAllocations = grouped
            .into_iter()
            .map(|(node_id, allocations)| (node_id, Arc::new(allocations)))
            .collect();

        let node_ids: Vec<String> = nodes.iter().map(|node| node.id.clone()).collect();
        let mut model = SchedulingReadModel {
            nodes: nodes
                .into_iter()
                .map(|node| (node.id.clone(), Arc::new(node)))
                .collect(),
            allocations_by_node,
            task_configs,
            indexes: IndexSet::new(Arc::clone(&self.index_settings)),
            node_pools: HashMap::new(),
            scope_epochs,
            // Placeholder: `refresh_read_model` overwrites this with the
            // previous model's counter + 1 when it publishes the rebuild.
            generation: 0,
            available_nodes_cache: None,
        };
        let id_refs: Vec<&str> = node_ids.iter().map(String::as_str).collect();
        Self::reproject_model_nodes(&mut model, &self.resource_policy, &id_refs);

        Ok(model)
    }

    /// Re-derives `node_ids`' cached state — the free resource pool and the
    /// scheduling-index projections, both computed from the same pool so
    /// they cannot disagree. Every read-model mutation that can change
    /// either — node upserts, availability flips, attribute updates,
    /// allocation changes — calls this inside the same write lock as the
    /// mutation, so derived state is never observable out of step with
    /// `nodes`. Unavailable or deleted nodes lose both.
    fn reproject_model_nodes(
        model: &mut SchedulingReadModel,
        policy: &ResourcePolicy,
        node_ids: &[&str],
    ) {
        if node_ids.is_empty() {
            return;
        }
        let started = Instant::now();
        for node_id in node_ids {
            match model.nodes.get(*node_id) {
                Some(node) if node.availability == "available" => {
                    let allocations = model
                        .allocations_by_node
                        .get(*node_id)
                        .map(|allocations| allocations.as_slice())
                        .unwrap_or(&[]);
                    let pool = ResourcePool::free_for_node(node, allocations, policy);
                    model.indexes.reindex_node(node, &pool);
                    model
                        .node_pools
                        .insert((*node_id).to_string(), Arc::new(pool));
                }
                _ => {
                    model.node_pools.remove(*node_id);
                    model.indexes.remove_node(node_id);
                }
            }
        }
        crate::manager::telemetry::metrics()
            .scheduler_index_maintenance_seconds
            .observe(started.elapsed().as_secs_f64());
    }

    pub(crate) fn refresh_read_model(&self) -> R<()> {
        // During capture the mutation is uncommitted (and will be rolled
        // back); reloading would cache pre-mutation state and the apply-time
        // projection rebuilds the affected entries anyway (JC-11).
        if capture::in_capture() {
            return Ok(());
        }
        let model = self.load_read_model()?;
        let mut guard = self.read_model_writer()?;
        // A full rebuild is an allocation publish like any other: carry the
        // counter forward (+1) so a plan spanning the rebuild falls back to
        // the eager sample instead of trusting a pre-rebuild capacity read.
        let next_generation = guard.generation.wrapping_add(1);
        *guard = model;
        guard.generation = next_generation;
        Ok(())
    }

    pub fn scheduling_read_model(&self) -> R<SchedulingReadModel> {
        Ok(self.read_model_reader()?.clone())
    }

    // NOTE on the `capture::in_capture()` early-returns below: read-model
    // cache mutations are side effects of uncommitted state while a
    // changeset capture is active, so they are suppressed during capture and
    // re-projected from the committed changeset at apply time on every
    // replica — see `project_changeset_side_effects` (JC-11).
    fn cache_node(&self, node: Node) -> R<()> {
        if capture::in_capture() {
            return Ok(());
        }
        let mut guard = self.read_model_writer()?;
        let node_id = node.id.clone();
        guard.nodes.insert(node_id.clone(), Arc::new(node));
        guard.available_nodes_cache = None;
        Self::reproject_model_nodes(&mut guard, &self.resource_policy, &[node_id.as_str()]);
        Ok(())
    }

    fn cache_node_dynamic_attrs(&self, node_id: &str, dynamic_attrs: Value) -> R<()> {
        if capture::in_capture() {
            return Ok(());
        }
        let mut guard = self.read_model_writer()?;
        let cached = guard
            .nodes
            .get_mut(node_id)
            .with_context(|| format!("Missing cached node {node_id}"))?;
        Arc::make_mut(cached).dynamic_attrs = dynamic_attrs;
        guard.available_nodes_cache = None;
        Self::reproject_model_nodes(&mut guard, &self.resource_policy, &[node_id]);
        Ok(())
    }

    fn cache_nodes_lost(&self, ids: &[String]) -> R<()> {
        if capture::in_capture() {
            return Ok(());
        }
        let mut guard = self.read_model_writer()?;
        for id in ids {
            if let Some(node) = guard.nodes.get_mut(id) {
                let node = Arc::make_mut(node);
                node.status = "lost".to_string();
                node.availability = "unavailable".to_string();
            }
        }
        guard.available_nodes_cache = None;
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        Self::reproject_model_nodes(&mut guard, &self.resource_policy, &id_refs);
        Ok(())
    }

    fn cache_remove_node(&self, id: &str) -> R<()> {
        if capture::in_capture() {
            return Ok(());
        }
        let mut guard = self.read_model_writer()?;
        guard.nodes.remove(id);
        guard.allocations_by_node.remove(id);
        guard.indexes.remove_node(id);
        guard.node_pools.remove(id);
        guard.available_nodes_cache = None;
        // Removing a node removes its allocations from the capacity view.
        guard.generation = guard.generation.wrapping_add(1);
        Ok(())
    }

    /// Refresh the allocation cache AND the epoch mirror for a set of
    /// scopes, from committed state, coherently (the plan-fence fix — see
    /// `SchedulingReadModel::scope_epochs`).
    ///
    /// Ordering is the invariant: epochs are READ first, allocations
    /// second, and both are PUBLISHED under one write-lock acquisition. A
    /// commit interleaving between the two reads leaves the published
    /// mirror epoch *behind* the published allocations, which the
    /// plan-apply CAS resolves as a conservative reject; the reverse
    /// ordering could publish an epoch *ahead* of the allocations — the
    /// exact phantom-capacity window this exists to close.
    fn cache_refresh_allocation_scopes(
        &self,
        node_ids: &BTreeSet<String>,
        job_config_ids: &BTreeSet<String>,
    ) -> R<()> {
        if capture::in_capture() {
            return Ok(());
        }
        // 1. Epochs first.
        let mut epochs: Vec<(String, i64)> = Vec::new();
        for node_id in node_ids {
            epochs.push((
                Self::scope_vector_key("node", node_id),
                self.allocation_epoch_for_scope("node", node_id)?,
            ));
        }
        for job_config_id in job_config_ids {
            epochs.push((
                Self::scope_vector_key("job_config", job_config_id),
                self.allocation_epoch_for_scope("job_config", job_config_id)?,
            ));
        }
        // 2. Allocations second.
        let mut per_node: Vec<(String, Vec<Allocation>)> = Vec::new();
        for node_id in node_ids {
            per_node.push((node_id.clone(), self.allocs_for_node(node_id)?));
        }
        // 3. Publish atomically, then re-derive the affected nodes' free
        // pools and index projections from the refreshed allocations —
        // inside the same lock, so `node_pools`/`indexes` can never be
        // observed out of step with `allocations_by_node` (the
        // plan-snapshot cache's own invariant).
        let mut guard = self.read_model_writer()?;
        for (node_id, allocs) in per_node {
            guard.allocations_by_node.insert(node_id, Arc::new(allocs));
        }
        for (key, epoch) in epochs {
            guard.scope_epochs.insert(key, epoch);
        }
        // Every allocation/epoch publish funnels through here; the bump is
        // what lets in-flight lazy-sample plans detect this publish (see
        // `SchedulingReadModel::generation`).
        guard.generation = guard.generation.wrapping_add(1);
        let affected: Vec<&str> = node_ids.iter().map(String::as_str).collect();
        Self::reproject_model_nodes(&mut guard, &self.resource_policy, &affected);
        Ok(())
    }

    /// Scope sets (nodes + job configs) touched by a batch of allocations.
    fn allocation_scopes(allocations: &[Allocation]) -> (BTreeSet<String>, BTreeSet<String>) {
        let mut node_ids = BTreeSet::new();
        let mut job_config_ids = BTreeSet::new();
        for allocation in allocations {
            node_ids.insert(allocation.node_id.clone());
            job_config_ids.insert(allocation.job_config_id.clone());
        }
        (node_ids, job_config_ids)
    }

    fn cache_add_allocations(&self, allocations: &[Allocation]) -> R<()> {
        let (node_ids, job_config_ids) = Self::allocation_scopes(allocations);
        self.cache_refresh_allocation_scopes(&node_ids, &job_config_ids)
    }

    fn cache_remove_allocations(&self, allocations: &[Allocation]) -> R<()> {
        let (node_ids, job_config_ids) = Self::allocation_scopes(allocations);
        self.cache_refresh_allocation_scopes(&node_ids, &job_config_ids)
    }

    fn cache_set_task_configs_for_job_config(
        &self,
        job_config_id: &str,
        tasks: Vec<(i64, TaskConfig)>,
    ) -> R<()> {
        if capture::in_capture() {
            return Ok(());
        }
        let mut guard = self.read_model_writer()?;
        guard.task_configs.insert(job_config_id.to_string(), tasks);
        Ok(())
    }

    pub fn cached_task_configs_for_job_config(
        &self,
        job_config_id: &str,
    ) -> R<Vec<(i64, TaskConfig)>> {
        {
            let guard = self.read_model_reader()?;
            if let Some(tasks) = guard.task_configs.get(job_config_id) {
                return Ok(tasks.clone());
            }
        }

        let tasks = self.get_task_configs_for_job_config(job_config_id)?;
        self.cache_set_task_configs_for_job_config(job_config_id, tasks.clone())?;
        Ok(tasks)
    }

    /// Deep-cloned allocations map, for callers that need owned vectors.
    /// The scheduler snapshots the shared (`Arc`'d) map via
    /// [`Store::plan_capacity_snapshot`] instead.
    pub fn cached_allocations_by_node(&self) -> R<HashMap<String, Vec<Allocation>>> {
        Ok(self
            .read_model_reader()?
            .allocations_by_node
            .iter()
            .map(|(node_id, allocations)| (node_id.clone(), Vec::clone(allocations)))
            .collect())
    }

    /// Deep-cloned node list, for callers that need owned `Node`s. The
    /// scheduler uses [`Store::available_node_arcs`] instead — per-plan deep
    /// clones of every node's JSON attributes are exactly the cost the Arc'd
    /// read model exists to avoid.
    pub fn cached_nodes(&self) -> R<Vec<Node>> {
        Ok(self
            .read_model_reader()?
            .nodes
            .values()
            .map(|node| Node::clone(node))
            .collect())
    }

    /// Ids of every available node. For callers that only need membership
    /// (rollout node gating) — no node payload is cloned.
    pub fn available_node_ids(&self) -> R<HashSet<String>> {
        let guard = self.read_model_reader()?;
        Ok(guard
            .nodes
            .values()
            .filter(|node| node.availability == "available")
            .map(|node| node.id.clone())
            .collect())
    }

    /// The scheduler's per-plan node snapshot: every available node, sorted
    /// by id, as `Arc`s. Served from the read model's cached list when the
    /// node set is unchanged since the last plan (the overwhelmingly common
    /// case — one `Vec` of refcount bumps); rebuilt lazily after any node
    /// mutation invalidates it.
    pub fn available_node_arcs(&self) -> R<Vec<Arc<Node>>> {
        {
            let guard = self.read_model_reader()?;
            if let Some(cache) = guard.available_nodes_cache.as_ref() {
                return Ok(Vec::clone(cache));
            }
        }
        let mut guard = self.read_model_writer()?;
        // Re-check: another plan may have rebuilt it while we waited.
        if guard.available_nodes_cache.is_none() {
            let mut nodes: Vec<Arc<Node>> = guard
                .nodes
                .values()
                .filter(|node| node.availability == "available")
                .cloned()
                .collect();
            nodes.sort_by(|a, b| a.id.cmp(&b.id));
            guard.available_nodes_cache = Some(Arc::new(nodes));
        }
        Ok(Vec::clone(
            guard
                .available_nodes_cache
                .as_ref()
                .expect("cache rebuilt above"),
        ))
    }

    /// The scheduler's per-plan capacity snapshot: current allocations and
    /// the cached free pools, cloned under ONE read guard so they can never
    /// disagree with each other, plus the generation they were published at.
    ///
    /// The fence rule is "epoch sample no later than the capacity read".
    /// The eager path satisfies it by sampling before calling this; the
    /// lazy path satisfies it retroactively — it samples *after* planning
    /// via [`Store::expected_versions_if_unchanged`], which only returns a
    /// sample if the generation still equals the one returned here (proving
    /// the late sample equals a pre-capacity one).
    pub(crate) fn plan_capacity_snapshot(&self) -> R<PlanCapacityParts> {
        let guard = self.read_model_reader()?;
        Ok((
            guard.allocations_by_node.clone(),
            guard.node_pools.clone(),
            guard.generation,
        ))
    }

    /// The owning job name and priority band of a job config.
    ///
    /// One row read on the plan path, so it is a single statement rather than
    /// a full `get_job_config` (which also loads every task). A config that
    /// has vanished reads as the default band under its own id — planning for
    /// a config that no longer exists fails for other reasons, and inventing
    /// a high band here would be the dangerous direction to guess in.
    pub fn job_band_for_job_config(&self, job_config_id: &str) -> R<(String, Band)> {
        let conn = self.read_connection()?;
        let row: Option<(String, String)> = conn
            .query_row(
                "SELECT job_id, priority FROM job_config WHERE id = ?;",
                [job_config_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        Ok(match row {
            Some((job_id, band)) => (job_id, Band::from_stored(&band)),
            None => (job_config_id.to_string(), Band::default()),
        })
    }

    /// Per-allocation metadata the preemption pass needs, for the
    /// allocations on a set of candidate nodes.
    ///
    /// Run **lazily**: the planner only calls this after normal placement has
    /// already failed for a task whose band can preempt, so a cluster that
    /// never preempts pays nothing. One statement rather than a per-node loop
    /// — the join is over `allocation` (indexed by node) and two config
    /// tables, and the whole point is to keep the rare path from becoming a
    /// per-node fan-out.
    ///
    /// The band, the `disruption` floor and `stop.killAfter` come from
    /// config rows rather than from the plan's in-memory snapshot, which
    /// carries only [`Allocation`]s.
    pub fn preemption_victim_metadata(&self, node_ids: &[&str]) -> R<Vec<PreemptionVictimRow>> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = std::iter::repeat_n("?", node_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT a.id, a.node_id, a.job_config_id, a.task_config_id, a.created_at_unix, \
                    a.resources, \
                    tc.name, tc.job_id, tc.count, tc.disruption, tc.stop, jc.priority, \
                    EXISTS( \
                        SELECT 1 FROM volume v \
                        WHERE v.instance_id = a.instance_id AND v.node_local = 1 \
                    ) AS node_pinned \
             FROM allocation a \
             INNER JOIN task_config tc ON tc.id = a.task_config_id \
             INNER JOIN job_config jc ON jc.id = a.job_config_id \
             WHERE a.node_id IN ({placeholders});"
        );
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows =
            Self::collect_rows(stmt.query_map(params_from_iter(node_ids.iter()), |row| {
                Ok(PreemptionVictimRow {
                    allocation_id: row.get(0)?,
                    node_id: row.get(1)?,
                    job_config_id: row.get(2)?,
                    task_config_id: row.get(3)?,
                    created_at_unix: row.get(4)?,
                    resources: row.get(5)?,
                    task_name: row.get(6)?,
                    job_id: row.get(7)?,
                    task_count: row.get::<_, i64>(8)? as u32,
                    disruption_json: row.get(9)?,
                    stop_json: row.get(10)?,
                    band: Band::from_stored(&row.get::<_, String>(11)?),
                    node_pinned: row.get::<_, i64>(12)? != 0,
                })
            })?)?;
        Ok(rows)
    }

    /// Runs one scheduling-index candidate query under the read-model lock
    /// (docs/scheduling-indexes.md). Cheap: index selection plus one shard
    /// walk; the caller re-verifies every returned candidate. `ordered:
    /// false` skips the shard's nearest-fit cell sort for callers that
    /// re-order the candidates themselves (the untruncated scheduler path).
    pub fn scheduling_index_query(
        &self,
        bindings: &HashMap<String, Value>,
        request: &HashMap<String, f64>,
        budget: Option<usize>,
        ordered: bool,
    ) -> R<crate::scheduling_index::IndexQueryOutcome> {
        Ok(self
            .read_model_reader()?
            .indexes
            .query_with_order(bindings, request, budget, ordered))
    }

    pub fn allocation_epoch_for_job_config(&self, job_config_id: &str) -> R<i64> {
        self.allocation_epoch_for_scope("job_config", job_config_id)
    }

    pub fn allocation_version_vector(
        &self,
        job_config_id: &str,
        node_ids: &HashSet<String>,
    ) -> R<BTreeMap<String, i64>> {
        // Sampled from the epoch MIRROR, not the DB (see
        // `SchedulingReadModel::scope_epochs`): the DB epoch can be ahead
        // of the allocation cache the planner reads next, and that pairing
        // is exactly the phantom-capacity window the plan-apply CAS cannot
        // catch. A missing mirror entry samples `0`, matching what
        // `ensure_scope_epoch_tx` seeds; a mirror entry older than the DB
        // only makes the CAS reject spuriously (retried), never admit.
        let guard = self.read_model_reader()?;
        let mut versions = BTreeMap::new();
        let job_key = Self::scope_vector_key("job_config", job_config_id);
        versions.insert(
            job_key.clone(),
            guard.scope_epochs.get(&job_key).copied().unwrap_or(0),
        );
        for node_id in node_ids {
            let key = Self::scope_vector_key("node", node_id);
            let epoch = guard.scope_epochs.get(&key).copied().unwrap_or(0);
            versions.insert(key, epoch);
        }
        Ok(versions)
    }

    /// Batched version vector for a whole *candidate* node set.
    ///
    /// The scheduler has to sample the optimistic-concurrency epochs before it
    /// reads the capacity snapshot it plans against (see
    /// `Scheduler::plan_and_apply_internal`), and at that point every node it
    /// might place on is a candidate — the affected subset is only known after
    /// planning. Sampling those per node with
    /// [`Self::allocation_epoch_for_scope`] would be one query per node on the
    /// scheduler hot path, so this issues a single query per chunk of node ids
    /// instead.
    ///
    /// Keys match [`Self::allocation_version_vector`] (`job_config:<id>`,
    /// `node:<id>`) so callers can select the affected subset out of the result
    /// without re-reading. Nodes with no epoch row yet default to `0`, which is
    /// the same value [`Self::allocation_epoch_for_scope`] reports and the same
    /// value `ensure_scope_epoch_tx` seeds when `apply_plan_submission`
    /// compares — so a first-ever allocation on a fresh node still bumps `0 ->
    /// 1` and still rejects a competing plan that sampled `0`.
    pub fn allocation_version_vector_for_candidates(
        &self,
        job_config_id: &str,
        node_ids: &[&str],
    ) -> R<BTreeMap<String, i64>> {
        // Sampled from the epoch MIRROR under one read-lock acquisition,
        // not from the DB (adversarial finding, 2026-07-28): a commit makes
        // the DB epoch visible immediately, but the allocation cache the
        // planner reads next is patched later — under raft, after the whole
        // changeset projection — so a DB sample could pair a NEW epoch with
        // the OLD cache and pass the plan-apply CAS against phantom
        // capacity. The mirror is maintained coherently with the cache
        // (epochs read before allocations, published after them), so the
        // only torn pairing it can produce is epoch-behind-cache — a
        // conservative reject, retried by the eval. Missing entries sample
        // `0`, matching `ensure_scope_epoch_tx`'s seed for genuinely fresh
        // scopes; hydration loads every existing epoch row so restart never
        // leaves a populated scope sampling `0` forever.
        let guard = self.read_model_reader()?;
        let mut versions = BTreeMap::new();
        let job_key = Self::scope_vector_key("job_config", job_config_id);
        versions.insert(
            job_key.clone(),
            guard.scope_epochs.get(&job_key).copied().unwrap_or(0),
        );
        for node_id in node_ids {
            let key = Self::scope_vector_key("node", node_id);
            let epoch = guard.scope_epochs.get(&key).copied().unwrap_or(0);
            versions.insert(key, epoch);
        }
        Ok(versions)
    }

    /// The LAZY optimistic-concurrency sample (plan-path scale levers, D4):
    /// epochs for exactly the scopes a finished plan touches — job config
    /// plus affected nodes — instead of the whole candidate fleet.
    ///
    /// Sampling *after* planning is only sound if nothing was published
    /// between the capacity read the plan was built against and this
    /// sample; the generation check proves exactly that (both reads happen
    /// under the read-model guard, and every publish bumps the counter
    /// under the write guard — see [`SchedulingReadModel::generation`]).
    /// Returns `None` when the generation moved: the caller must fall back
    /// to the eager pre-planning sample
    /// ([`Self::allocation_version_vector_for_candidates`]) and re-plan.
    ///
    /// Missing mirror entries sample `0` with the same conservative
    /// semantics as the eager path.
    pub fn expected_versions_if_unchanged(
        &self,
        job_config_id: &str,
        affected_nodes: &HashSet<String>,
        generation: u64,
    ) -> R<Option<BTreeMap<String, i64>>> {
        let guard = self.read_model_reader()?;
        if guard.generation != generation {
            return Ok(None);
        }
        let mut versions = BTreeMap::new();
        let job_key = Self::scope_vector_key("job_config", job_config_id);
        versions.insert(
            job_key.clone(),
            guard.scope_epochs.get(&job_key).copied().unwrap_or(0),
        );
        for node_id in affected_nodes {
            let key = Self::scope_vector_key("node", node_id);
            let epoch = guard.scope_epochs.get(&key).copied().unwrap_or(0);
            versions.insert(key, epoch);
        }
        Ok(Some(versions))
    }

    /// Current read-model generation (see
    /// [`SchedulingReadModel::generation`]).
    pub fn read_model_generation(&self) -> R<u64> {
        Ok(self.read_model_reader()?.generation)
    }

    fn allocation_epoch_for_scope(&self, scope_type: &str, scope_id: &str) -> R<i64> {
        let conn = self.read_connection()?;
        let epoch: Option<i64> = conn
            .query_row(
                "SELECT epoch FROM allocation_epoch_partition WHERE scope_type = ?1 AND scope_id = ?2;",
                params![scope_type, scope_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(epoch.unwrap_or(0))
    }

    fn scope_vector_key(scope_type: &str, scope_id: &str) -> String {
        format!("{scope_type}:{scope_id}")
    }

    fn parse_scope_vector_key(scope_key: &str) -> R<(&str, &str)> {
        let (scope_type, scope_id) = scope_key
            .split_once(':')
            .with_context(|| format!("invalid scope key '{scope_key}' in plan version vector"))?;
        Ok((scope_type, scope_id))
    }

    fn ensure_scope_epoch_tx(&self, tx: &Connection, scope_type: &str, scope_id: &str) -> R<()> {
        tx.execute(
            "INSERT OR IGNORE INTO allocation_epoch_partition(scope_type, scope_id, epoch) VALUES (?1, ?2, 0);",
            params![scope_type, scope_id],
        )?;
        Ok(())
    }

    fn scope_epoch_tx(&self, tx: &Connection, scope_type: &str, scope_id: &str) -> R<i64> {
        self.ensure_scope_epoch_tx(tx, scope_type, scope_id)?;
        let epoch = tx.query_row(
            "SELECT epoch FROM allocation_epoch_partition WHERE scope_type = ?1 AND scope_id = ?2;",
            params![scope_type, scope_id],
            |row| row.get(0),
        )?;
        Ok(epoch)
    }

    fn scope_vector_epochs_tx(
        &self,
        tx: &Connection,
        scopes: &BTreeMap<String, i64>,
    ) -> R<BTreeMap<String, i64>> {
        let mut latest = BTreeMap::new();
        for scope_key in scopes.keys() {
            let (scope_type, scope_id) = Self::parse_scope_vector_key(scope_key)?;
            let epoch = self.scope_epoch_tx(tx, scope_type, scope_id)?;
            latest.insert(scope_key.clone(), epoch);
        }
        Ok(latest)
    }

    fn bump_scope_epoch_tx(&self, tx: &Connection, scope_type: &str, scope_id: &str) -> R<i64> {
        self.ensure_scope_epoch_tx(tx, scope_type, scope_id)?;
        tx.execute(
            "UPDATE allocation_epoch_partition SET epoch = epoch + 1 WHERE scope_type = ?1 AND scope_id = ?2;",
            params![scope_type, scope_id],
        )?;
        self.scope_epoch_tx(tx, scope_type, scope_id)
    }

    fn bump_job_config_epoch_tx(&self, tx: &Connection, job_config_id: &str) -> R<i64> {
        self.bump_scope_epoch_tx(tx, "job_config", job_config_id)
    }

    fn bump_job_config_epochs_tx(
        &self,
        tx: &Connection,
        job_config_ids: &HashSet<String>,
    ) -> R<()> {
        for job_config_id in job_config_ids {
            self.bump_job_config_epoch_tx(tx, job_config_id)?;
        }
        Ok(())
    }

    // IMPORTANT: any transaction that mutates allocation rows must call this
    // (or bump_job_config_epoch_tx) before commit so stale plans are rejected.
    fn bump_epochs_for_allocations_tx(&self, tx: &Connection, allocations: &[Allocation]) -> R<()> {
        let job_config_ids: HashSet<String> = allocations
            .iter()
            .map(|alloc| alloc.job_config_id.clone())
            .collect();
        self.bump_job_config_epochs_tx(tx, &job_config_ids)?;

        let node_ids: HashSet<String> = allocations
            .iter()
            .map(|alloc| alloc.node_id.clone())
            .collect();
        for node_id in node_ids {
            self.bump_scope_epoch_tx(tx, "node", &node_id)?;
        }
        Ok(())
    }

    pub fn insert_plan_submission(
        &self,
        job_config_id: &str,
        mode: &str,
        placements: &[(String, i64)],
        expected_versions: &BTreeMap<String, i64>,
    ) -> R<PlanSubmission> {
        let job_config_id = job_config_id.to_owned();
        let mode = mode.to_owned();
        let placements = placements.to_vec();
        let expected_versions = expected_versions.clone();
        self.replicated(Lane::Scheduler, "insert_plan_submission", move |s| {
            s.insert_plan_submission_apply(
                &job_config_id,
                &mode,
                &placements,
                &expected_versions,
                &[],
            )
        })
    }

    fn insert_plan_submission_apply(
        &self,
        job_config_id: &str,
        mode: &str,
        placements: &[(String, i64)],
        expected_versions: &BTreeMap<String, i64>,
        evictions: &[PlanEviction],
    ) -> R<PlanSubmission> {
        let created_at = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let placements_json = serde_json::to_string(placements)?;
        let evictions_json = serde_json::to_string(evictions)?;
        let expected_vector_json = serde_json::to_string(expected_versions)?;
        let expected_job_epoch = *expected_versions
            .get(&Self::scope_vector_key("job_config", job_config_id))
            .unwrap_or(&0);
        let conn = self.write_connection()?;
        let id = conn.query_row(
            "INSERT INTO plan_submission(job_config_id, mode, placements, expected_epoch, expected_version_vector, status, created_at, evictions) VALUES (?1, ?2, ?3, ?4, ?5, 'queued', ?6, ?7) RETURNING id;",
            params![
                job_config_id,
                mode,
                placements_json,
                expected_job_epoch,
                expected_vector_json,
                created_at,
                evictions_json
            ],
            |row| row.get(0),
        )?;

        Ok(PlanSubmission {
            id,
            job_config_id: job_config_id.to_string(),
            expected_versions: expected_versions.clone(),
        })
    }

    /// Insert a plan submission and apply it in ONE replicated op (one
    /// write transaction boundary, one raft propose → quorum commit →
    /// apply). The production scheduler path (`plan_and_apply`) always
    /// wants both halves back-to-back, and on raft backends the split
    /// two-step form pays a second full quorum round-trip for a row that
    /// is consumed microseconds after it is written — measured at 0.3-2.1
    /// ms per plan in the 2026-07-29 backend matrix
    /// (`benchmarks/scheduler/README.md`). The two-step API below stays
    /// for callers that genuinely stage submissions.
    ///
    /// Concurrent callers additionally GROUP-COMMIT (plan-path scale
    /// levers, D5): plans queued while a commit is in flight batch into
    /// one replicated op — one capture, one raft propose/quorum round for
    /// the whole batch — with each plan keeping its own epoch-CAS verdict
    /// inside a per-plan savepoint. A lone caller takes the single-plan
    /// path unchanged. `schedulerSettings.plan_apply_coalescing: false`
    /// disables the batching.
    pub fn submit_and_apply_plan(
        &self,
        job_config_id: &str,
        mode: &str,
        placements: &[(String, i64)],
        expected_versions: &BTreeMap<String, i64>,
        evictions: &[PlanEviction],
    ) -> R<(PlanSubmission, PlanApplyOutcome)> {
        let request = PlanApplyRequest {
            job_config_id: job_config_id.to_owned(),
            mode: mode.to_owned(),
            placements: placements.to_vec(),
            expected_versions: expected_versions.clone(),
            evictions: evictions.to_vec(),
        };
        let (submission, outcome) = if self.plan_apply_coalescing_enabled() {
            let coalescer = Arc::clone(&self.plan_coalescer);
            coalescer.submit(request, |batch| self.run_plan_apply_batch(batch))?
        } else {
            self.submit_and_apply_plan_replicated(request)?
        };
        self.plan_apply_bookkeeping(&outcome)?;
        Ok((submission, outcome))
    }

    /// `schedulerSettings.plan_apply_coalescing` — default true.
    fn plan_apply_coalescing_enabled(&self) -> bool {
        self.scheduler_settings
            .as_ref()
            .and_then(|settings| settings.get("plan_apply_coalescing"))
            .and_then(Value::as_bool)
            .unwrap_or(true)
    }

    /// `schedulerSettings.plan_apply_batch_max` — default 16, clamped to
    /// 1..=128. Caps how many plans one group commit may carry, bounding
    /// the size of the replicated changeset (raft append-entries budget).
    fn plan_apply_batch_max(settings: Option<&Value>) -> usize {
        settings
            .and_then(|settings| settings.get("plan_apply_batch_max"))
            .and_then(Value::as_u64)
            .map(|max| (max as usize).clamp(1, 128))
            .unwrap_or(16)
    }

    /// The single-plan replicated op — the group commit's batch-of-one
    /// fast path, its re-run-singly fallback, and the
    /// `plan_apply_coalescing: false` path. Exactly the pre-D5 operation.
    fn submit_and_apply_plan_replicated(&self, request: PlanApplyRequest) -> R<PlanApplyReply> {
        self.replicated(Lane::Scheduler, "submit_and_apply_plan", move |s| {
            s.submit_and_apply_plan_apply(
                &request.job_config_id,
                &request.mode,
                &request.placements,
                &request.expected_versions,
                &request.evictions,
            )
        })
    }

    /// Batch leader body for the plan-apply group commit: run every
    /// drained plan inside ONE replicated op. Under raft this is one
    /// capture → one propose → one quorum round → one apply transaction
    /// for the whole batch; on local backends the plans run back-to-back
    /// exactly as they would have serially (WAL-mode commits are not the
    /// local cost, so no outer transaction is added — see the plan's
    /// judgement calls).
    ///
    /// Failure isolation: under capture, each plan runs inside its own
    /// savepoint, so a failing plan rolls back alone and cannot poison
    /// batch-mates (epoch-CAS rejections are not failures — they commit a
    /// `rejected` submission row exactly as the single-plan op does). If
    /// the batch op itself fails (capture/propose machinery), nothing was
    /// applied — the capture rolled back — so every plan re-runs singly
    /// through the pre-D5 path; any plan the failed propose DID commit
    /// (a lost-leadership race) is then rejected by its own epoch CAS,
    /// never applied twice.
    fn run_plan_apply_batch(&self, batch: Vec<PlanApplyRequest>) -> Vec<R<PlanApplyReply>> {
        if batch.len() == 1 {
            let request = batch.into_iter().next().expect("length checked");
            return vec![self.submit_and_apply_plan_replicated(request)];
        }

        let rerun = batch.clone();
        let batch_size = batch.len();
        let batched = self.replicated(Lane::Scheduler, "submit_and_apply_plans", move |s| {
            let isolate = capture::in_capture();
            let mut results: Vec<R<PlanApplyReply>> = Vec::with_capacity(batch.len());
            for request in &batch {
                if isolate {
                    // Savepoint-machinery errors abort the whole capture
                    // (`?`): a leftover savepoint would corrupt the stack
                    // for batch-mates, and a full rollback + re-run-singly
                    // is the safe recovery.
                    s.exec_on_write_conn("SAVEPOINT legion_plan_batch_item;")?;
                    match s.submit_and_apply_plan_apply(
                        &request.job_config_id,
                        &request.mode,
                        &request.placements,
                        &request.expected_versions,
                        &request.evictions,
                    ) {
                        Ok(reply) => {
                            s.exec_on_write_conn("RELEASE legion_plan_batch_item;")?;
                            results.push(Ok(reply));
                        }
                        Err(err) => {
                            s.exec_on_write_conn(
                                "ROLLBACK TO legion_plan_batch_item; \
                                 RELEASE legion_plan_batch_item;",
                            )?;
                            results.push(Err(err));
                        }
                    }
                } else {
                    results.push(s.submit_and_apply_plan_apply(
                        &request.job_config_id,
                        &request.mode,
                        &request.placements,
                        &request.expected_versions,
                        &request.evictions,
                    ));
                }
            }
            Ok(results)
        });
        match batched {
            Ok(results) => results,
            Err(batch_err) => {
                warn!(
                    batch = batch_size,
                    error = %batch_err,
                    "Batched plan apply failed before commit; re-running each plan singly"
                );
                rerun
                    .into_iter()
                    .map(|request| self.submit_and_apply_plan_replicated(request))
                    .collect()
            }
        }
    }

    /// Run raw SQL on the write connection, acquiring and releasing the
    /// connection lock for just this statement (savepoint control inside
    /// batched ops, where holding a transaction handle across the
    /// per-plan calls would deadlock their own `write_connection` locks).
    fn exec_on_write_conn(&self, sql: &str) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute_batch(sql)?;
        Ok(())
    }

    fn submit_and_apply_plan_apply(
        &self,
        job_config_id: &str,
        mode: &str,
        placements: &[(String, i64)],
        expected_versions: &BTreeMap<String, i64>,
        evictions: &[PlanEviction],
    ) -> R<(PlanSubmission, PlanApplyOutcome)> {
        let submission = self.insert_plan_submission_apply(
            job_config_id,
            mode,
            placements,
            expected_versions,
            evictions,
        )?;
        let outcome = self.apply_plan_submission_apply(&submission)?;
        Ok((submission, outcome))
    }

    pub fn apply_plan_submission(&self, submission: &PlanSubmission) -> R<PlanApplyOutcome> {
        let submission = submission.clone();
        let outcome = self.replicated(Lane::Scheduler, "apply_plan_submission", move |s| {
            s.apply_plan_submission_apply(&submission)
        })?;
        self.plan_apply_bookkeeping(&outcome)?;
        Ok(outcome)
    }

    /// Post-apply bookkeeping shared by `apply_plan_submission` and
    /// `submit_and_apply_plan`: epoch-mirror self-heal on rejection,
    /// status-lane task_status projection on success.
    fn plan_apply_bookkeeping(&self, outcome: &PlanApplyOutcome) -> R<()> {
        // Self-heal the epoch mirror on rejection: the CAS just proved the
        // sampler's mirror was behind the database for these scopes (a
        // competing plan applied, or a write path bumped an epoch without a
        // cache refresh). Refreshing the rejected scopes — epochs first,
        // allocations second, like every mirror update — makes the retry
        // sample fresh state instead of failing on the same stale entries
        // forever.
        if let PlanApplyOutcome::Rejected {
            latest_versions, ..
        } = outcome
        {
            let mut node_ids = BTreeSet::new();
            let mut job_config_ids = BTreeSet::new();
            for key in latest_versions.keys() {
                match Self::parse_scope_vector_key(key) {
                    Ok(("node", id)) => {
                        node_ids.insert(id.to_string());
                    }
                    Ok(("job_config", id)) => {
                        job_config_ids.insert(id.to_string());
                    }
                    _ => {}
                }
            }
            self.cache_refresh_allocation_scopes(&node_ids, &job_config_ids)?;
        }
        // Lifecycle-derived task_status transitions are status-lane writes
        // (plan §7-C2, C-2), so they follow the scheduler-lane apply as a
        // separate single-lane op instead of riding its changeset. Same
        // rows/values as the pre-split in-transaction update
        // (`task_status_for_lifecycle` per placed task).
        //
        // Standing-split tolerance (plan §8, D1): the plan is APPLIED at
        // this point — failing the caller for the status projection would
        // make every plan-applying eval fail-and-retry after mutating
        // allocations (the churn loop: a `job_update` retry removes and
        // re-places the full allocation set every cycle). The statuses are
        // derivable — the status leader's seed reconciler and the workers'
        // own status reports converge them — so a typed `NotRaftLeader`
        // here is warn-and-continue; any other error still fails.
        if let PlanApplyOutcome::Applied(allocations) = outcome {
            if let Err(err) = self.update_task_statuses_for_allocations(allocations) {
                if !crate::storage::raft::driver::NotRaftLeader::in_chain(&err) {
                    return Err(err);
                }
                warn!(
                    allocations = allocations.len(),
                    error = %err,
                    "status-lane task_status projection deferred: this node does not lead \
                     the status lane; worker reports and the status leader's reconciler \
                     will converge the statuses"
                );
            }
        }
        Ok(())
    }

    fn apply_plan_submission_apply(&self, submission: &PlanSubmission) -> R<PlanApplyOutcome> {
        let tx_started = Instant::now();
        debug!(
            submission_id = submission.id,
            job_config_id = %submission.job_config_id,
            expected_scope_count = submission.expected_versions.len(),
            "Applying plan submission"
        );
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;

        let latest_versions = self.scope_vector_epochs_tx(&tx, &submission.expected_versions)?;
        if latest_versions != submission.expected_versions {
            let latest_job_epoch = latest_versions
                .get(&Self::scope_vector_key(
                    "job_config",
                    &submission.job_config_id,
                ))
                .copied()
                .unwrap_or_default();
            let reason = format!(
                "plan epoch for job config {} is stale; expected {}, latest {}",
                submission.job_config_id,
                submission
                    .expected_versions
                    .get(&Self::scope_vector_key(
                        "job_config",
                        &submission.job_config_id
                    ))
                    .copied()
                    .unwrap_or_default(),
                latest_job_epoch
            );
            tx.execute(
                "UPDATE plan_submission SET status = 'rejected', rejection_reason = ?1 WHERE id = ?2;",
                params![reason, submission.id],
            )?;
            let rejected_event = self.stage_event_locked(
                &tx,
                drafts::plan_rejected(&submission.job_config_id, &reason),
            );
            tx.commit()?;
            self.publish_staged(rejected_event);
            warn!(
                submission_id = submission.id,
                job_config_id = %submission.job_config_id,
                latest_scope_count = latest_versions.len(),
                duration_ms = tx_started.elapsed().as_millis() as u64,
                "Rejected plan submission due to stale version vector"
            );
            crate::manager::telemetry::metrics()
                .plan_apply_total
                .inc(&["rejected"]);
            return Ok(PlanApplyOutcome::Rejected {
                reason,
                latest_versions,
            });
        }

        let (placements_json, plan_mode, evictions_json): (String, String, String) = tx.query_row(
            "SELECT placements, mode, evictions FROM plan_submission WHERE id = ?1;",
            [submission.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let placements: Vec<(String, i64)> = serde_json::from_str(&placements_json)?;
        let evictions: Vec<PlanEviction> = serde_json::from_str(&evictions_json)?;

        // ---- Preemption: victims leave before the placements they paid for
        // arrive, inside this same transaction.
        //
        // Order matters twice over. The DELETEs run before the free-pool
        // reconstruction below, so the placement's capacity check sees the
        // capacity the eviction freed — without that, a plan would evict and
        // then reject itself for being over capacity. And they run inside
        // this transaction, so there is no instant in which the freed
        // capacity exists un-owned: the epoch compare-and-set above already
        // proved nothing else touched these nodes, and any competing plan
        // that samples between this commit and the read-model refresh
        // conflicts rather than double-spending. That is the whole
        // correctness claim of the design (Volcano #5022 is the bug this
        // shape cannot have).
        // Fenced by a savepoint, because the per-placement validation below
        // can still REJECT this plan — and a rejection commits (it persists
        // the rejected submission row and its event). Without the savepoint
        // that commit would keep the evictions and drop the placements they
        // paid for, which is precisely the failure this design claims it
        // cannot have. On rejection the savepoint is rolled back, so the
        // victims are still running when the plan is retried.
        let eviction_savepoint = !evictions.is_empty();
        if eviction_savepoint {
            tx.execute_batch("SAVEPOINT legion_plan_evictions;")?;
        }
        let preempted = self.apply_plan_evictions_tx(&tx, submission, &evictions)?;

        let mut nodes_by_id: HashMap<String, Node> = HashMap::new();
        if !placements.is_empty() {
            let node_ids: Vec<String> = placements
                .iter()
                .map(|(node_id, _)| node_id.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            let placeholders = std::iter::repeat_n("?", node_ids.len())
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "SELECT {NODE_JOINED_ATTRS} {NODE_JOINED_FROM} WHERE node.id IN ({placeholders});"
            );
            let mut stmt = tx.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(node_ids.iter()), |row| {
                self.node_from_row(row)
            })?;
            for row in rows {
                let node = row?;
                nodes_by_id.insert(node.id.clone(), node);
            }
        }

        // Free pools reconstructed INSIDE the apply transaction, from the
        // fresh node rows and the allocation rows this transaction can see
        // (the same rows the read model projects — no status filter). This
        // closes the last unfenced plan input (2026-07-28 adversarial
        // pass): a worker re-registration that SHRINKS capacity — or
        // changes `reservedResources` / `maxAllocations` — between the
        // planner's snapshot and this apply bumps no allocation epoch, so
        // the version CAS above structurally cannot see it, and without
        // this recheck the plan commits placements exceeding the node's
        // new totals. Ports were already apply-safe (re-read in
        // `assign_allocation_network`); resources now match.
        let mut free_pools: HashMap<String, ResourcePool> = HashMap::new();
        {
            // Mirror the planner's ledger view: a Fresh plan replaces the
            // JOB's allocations wholesale, so `plan_and_apply` drops every
            // allocation of the planned config's job before planning
            // (`drop_replaced`) — `handle_job_submit` only deletes the
            // previous config's rows after this apply, so those rows are
            // still in the table here and must not be double counted.
            // Reschedule plans drop nothing.
            let mut stmt = tx.prepare(
                "SELECT a.id, a.resources FROM allocation a \
                 WHERE a.node_id = ?1 AND (?2 = 0 OR a.job_config_id NOT IN (\
                     SELECT id FROM job_config WHERE job_id = \
                         (SELECT job_id FROM job_config WHERE id = ?3)));",
            )?;
            let drop_own = i64::from(plan_mode == "fresh");
            for (node_id, node) in &nodes_by_id {
                let rows = Self::collect_rows(stmt.query_map(
                    params![node_id, drop_own, submission.job_config_id],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Value>(1)?)),
                )?)?;
                let pool = ResourcePool::free_from_rows(
                    node,
                    rows.iter().map(|(id, resources)| (*id, resources)),
                    &self.resource_policy,
                );
                free_pools.insert(node_id.clone(), pool);
            }
        }

        let mut allocations = Vec::new();
        let mut task_states: HashMap<i64, AllocationTaskState> = HashMap::new();
        let mut task_requests: HashMap<i64, ResourceRequest> = HashMap::new();
        for (node_id, task_id) in &placements {
            let state = match task_states.entry(*task_id) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert(self.load_allocation_task_state(&tx, *task_id)?)
                }
            };

            let node = nodes_by_id.get(node_id).with_context(|| {
                format!(
                    "Node {node_id} not found while applying plan {}",
                    submission.id
                )
            })?;
            // Availability is re-validated on the FRESH node row, not only
            // constraints/permit (adversarial finding, 2026-07-28): a
            // cordon (`set_node_availability`, control lane) or a node loss
            // landing between the planner's snapshot and this apply bumps
            // no allocation epoch — the CAS structurally cannot see it — so
            // without this check a plan placed onto a node the operator
            // just drained, and nothing ever reconciled the placement away.
            let unavailable = node.availability != "available";
            let request = match task_requests.entry(*task_id) {
                Entry::Occupied(entry) => Some(entry.into_mut()),
                Entry::Vacant(entry) => self
                    .resource_policy
                    .normalize_task_resources(&state.resources, false)
                    .and_then(|normalized| ResourceRequest::from_value(&normalized))
                    .ok()
                    .map(|request| entry.insert(request)),
            };
            let over_capacity = match request.as_ref() {
                Some(request) => !free_pools
                    .get(node_id.as_str())
                    .is_some_and(|pool| pool.can_fit(request)),
                // Unparseable task resources fit nowhere — the same
                // fits-nothing posture as `ResourcePool::free_from_rows`
                // takes for an unparseable node or allocation row.
                None => true,
            };
            if unavailable
                || over_capacity
                || !state
                    .constraints
                    .is_satisfied(node, state.distinct.as_ref())
                || !state.permit.is_permitted(node)
            {
                let reason = if unavailable {
                    format!(
                        "Node {} is {} (cordoned, draining, or lost since the plan snapshot)",
                        node_id, node.availability
                    )
                } else if over_capacity {
                    format!(
                        "Node {} no longer has capacity for task {} (its resource totals changed since the plan snapshot)",
                        node_id, task_id
                    )
                } else {
                    format!(
                        "Node {} does not satisfy constraints for task {}",
                        node_id, task_id
                    )
                };
                // Un-evict before recording the rejection: this plan is not
                // happening, so nothing it would have displaced may go.
                if eviction_savepoint {
                    tx.execute_batch(
                        "ROLLBACK TO legion_plan_evictions; RELEASE legion_plan_evictions;",
                    )?;
                }
                tx.execute(
                    "UPDATE plan_submission SET status = 'rejected', rejection_reason = ?1 WHERE id = ?2;",
                    params![reason, submission.id],
                )?;
                let latest_versions =
                    self.scope_vector_epochs_tx(&tx, &submission.expected_versions)?;
                let rejected_event = self.stage_event_locked(
                    &tx,
                    drafts::plan_rejected(&submission.job_config_id, &reason),
                );
                tx.commit()?;
                self.publish_staged(rejected_event);
                warn!(
                    submission_id = submission.id,
                    job_config_id = %submission.job_config_id,
                    reverted_evictions = preempted.victims.len(),
                    duration_ms = tx_started.elapsed().as_millis() as u64,
                    "Rejected plan submission after validation during apply; evictions reverted"
                );
                crate::manager::telemetry::metrics()
                    .plan_apply_total
                    .inc(&["rejected"]);
                return Ok(PlanApplyOutcome::Rejected {
                    reason,
                    latest_versions,
                });
            }

            // Debit this placement so later placements in the SAME plan see
            // the reduced pool — the in-tx mirror of the planner's ledger
            // consume.
            if let (Some(request), Some(pool)) =
                (request.as_deref(), free_pools.get_mut(node_id.as_str()))
            {
                pool.consume(request);
            }

            if let Some(tracker) = state.distinct.as_mut() {
                tracker.insert_from_node(node);
            }

            let network = self.assign_allocation_network(
                &tx,
                node_id,
                *task_id,
                state.network_raw.clone(),
                state.connect_raw.clone(),
            )?;
            let allocation = self.insert_allocation_with_data(
                &tx,
                node_id,
                *task_id,
                &state.job_config_id,
                &state.resources,
                network.as_ref(),
            )?;
            allocations.push(allocation);
        }

        if !allocations.is_empty() {
            self.bump_epochs_for_allocations_tx(&tx, &allocations)?;
        }
        if eviction_savepoint {
            tx.execute_batch("RELEASE legion_plan_evictions;")?;
        }

        tx.execute(
            "UPDATE plan_submission SET status = 'applied', rejection_reason = NULL WHERE id = ?1;",
            [submission.id],
        )?;
        let placed_event = drafts::allocations_placed(&submission.job_config_id, &allocations)
            .and_then(|draft| self.stage_event_locked(&tx, draft));
        tx.commit()?;
        self.publish_staged(placed_event);
        for event in preempted.events {
            self.publish_staged(Some(event));
        }
        info!(
            submission_id = submission.id,
            job_config_id = %submission.job_config_id,
            placement_count = placements.len(),
            applied_allocations = allocations.len(),
            preempted_allocations = preempted.victims.len(),
            duration_ms = tx_started.elapsed().as_millis(),
            "Applied plan submission transaction"
        );

        // Mirror maintenance in the same order as every other path: drop the
        // evicted rows before adding the placements that replaced them, so
        // the mirror never briefly shows both.
        if !preempted.victims.is_empty() {
            self.cache_remove_allocations(&preempted.victims)?;
        }
        self.cache_add_allocations(&allocations)?;
        crate::manager::telemetry::metrics()
            .plan_apply_total
            .inc(&["applied"]);
        if !preempted.victims.is_empty() {
            let metrics = crate::manager::telemetry::metrics();
            for (pool, band, count) in preempted.metric_rows {
                metrics.preemptions_total.inc_by(&[&pool, &band], count);
            }
            metrics.preemption_plans_total.inc(&["placed"]);
        }

        Ok(PlanApplyOutcome::Applied(allocations))
    }

    /// Commit a plan's evictions: delete the victims, release their
    /// instances, bump their epochs, record their stop notices, requeue their
    /// jobs, and stage the events — all inside the caller's transaction.
    ///
    /// Every one of those is in the same transaction on purpose:
    ///
    /// - **Deletes**, so the freed capacity is spent by this plan's
    ///   placements and by nothing else.
    /// - **Requeue evals**, so a victim's job cannot be forgotten by a crash
    ///   between the eviction and the follow-up. Requeue is driven by the
    ///   job's *desired count*, not by remembering the eviction, which is why
    ///   the Nomad #9890 class of bug (preempted batch allocations never
    ///   replaced) cannot exist here: batch victims requeue exactly like
    ///   service ones, and `regenerate_scheduling_evals` converges any job
    ///   left under-count after a failover.
    /// - **Stop notices**, so the owning worker learns the cause — and the
    ///   preemptor — on its next heartbeat and hands the task a notice file.
    /// - **Events**, staged (never emitted inline) per the event stream's
    ///   performance rules.
    fn apply_plan_evictions_tx(
        &self,
        tx: &Connection,
        submission: &PlanSubmission,
        evictions: &[PlanEviction],
    ) -> R<AppliedEvictions> {
        if evictions.is_empty() {
            return Ok(AppliedEvictions::default());
        }

        // Re-read the victims from the live table. The plan named allocation
        // ids; between planning and apply one may have been stopped or
        // destroyed by another path. Evicting what is already gone is a
        // no-op, and the placement that "paid" for it still has the capacity,
        // because the row it needed freed is gone either way.
        let ids: Vec<i64> = evictions
            .iter()
            .map(|victim| victim.allocation_id)
            .collect();
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let attrs = crate::storage::queries::ALLOCATION_WITH_JOB_ATTRS;
        let sql = format!(
            "SELECT {attrs} FROM allocation \
             INNER JOIN job_config ON job_config.id = allocation.job_config_id \
             WHERE allocation.id IN ({placeholders});"
        );
        let mut stmt = tx.prepare(&sql)?;
        let victims = Self::collect_rows(stmt.query_map(params_from_iter(ids.iter()), |row| {
            self.allocation_from_row(row)
        })?)?;
        drop(stmt);
        if victims.is_empty() {
            return Ok(AppliedEvictions::default());
        }

        for victim in &victims {
            tx.execute("DELETE FROM allocation WHERE id = ?;", [victim.id])?;
        }
        self.release_instance_bindings_tx(tx, &victims)?;
        self.bump_epochs_for_allocations_tx(tx, &victims)?;

        // Who took the capacity, read from the config this plan is placing.
        let (preemptor_job, preemptor_band): (String, Band) = tx
            .query_row(
                "SELECT job_id, priority FROM job_config WHERE id = ?;",
                [&submission.job_config_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        Band::from_stored(&row.get::<_, String>(1)?),
                    ))
                },
            )
            .optional()?
            .unwrap_or_else(|| (submission.job_config_id.clone(), Band::default()));

        self.record_stop_notices_with_preemptor_tx(
            tx,
            &victims,
            StopCause::Preempted,
            Some((preemptor_job.as_str(), preemptor_band)),
        )?;
        let mut events = Vec::new();
        if let Some(event) = self.stage_stopping_event_tx(tx, &victims, StopCause::Preempted) {
            events.push(event);
        }

        // One `job_reschedule` eval per victim job config, at the victim's
        // own band priority so a displaced `low` job does not jump the queue
        // ahead of the work that displaced it.
        let mut requeued: BTreeSet<&str> = BTreeSet::new();
        for victim in &victims {
            requeued.insert(victim.job_config_id.as_str());
        }
        for job_config_id in requeued {
            let band = evictions
                .iter()
                .find(|eviction| eviction.job_config_id == job_config_id)
                .map(|eviction| Band::from_stored(&eviction.band))
                .unwrap_or_default();
            let job_id = victims
                .iter()
                .find(|victim| victim.job_config_id == job_config_id)
                .map(|victim| victim.job_id.clone())
                .unwrap_or_default();
            self.enqueue_eval_tx(
                tx,
                "job_reschedule",
                Some(&job_id),
                Some(job_config_id),
                band.eval_priority(),
            )?;
        }

        // One event per victim job, so the journal reads as "who lost what to
        // whom" rather than as one undifferentiated batch.
        let mut by_job: BTreeMap<&str, Vec<Allocation>> = BTreeMap::new();
        for victim in &victims {
            by_job
                .entry(victim.job_config_id.as_str())
                .or_default()
                .push(victim.clone());
        }
        for (job_config_id, group) in by_job {
            let band = evictions
                .iter()
                .find(|eviction| eviction.job_config_id == job_config_id)
                .map(|eviction| Band::from_stored(&eviction.band))
                .unwrap_or_default();
            if let Some(event) =
                drafts::allocations_preempted(&group, band, &preemptor_job, preemptor_band)
                    .and_then(|draft| self.stage_event_locked(tx, draft))
            {
                events.push(event);
            }
        }

        // Metric rows, keyed by the victim's pool and band — the two axes an
        // operator slices preemption rate by.
        let mut metric_counts: BTreeMap<(String, String), u64> = BTreeMap::new();
        for victim in &victims {
            let pool: String = tx
                .query_row(
                    "SELECT pool FROM node WHERE id = ?;",
                    [&victim.node_id],
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or_else(|| "default".to_string());
            let band = evictions
                .iter()
                .find(|eviction| eviction.allocation_id == victim.id)
                .map(|eviction| eviction.band.clone())
                .unwrap_or_else(|| Band::default().as_str().to_string());
            *metric_counts.entry((pool, band)).or_default() += 1;
        }

        Ok(AppliedEvictions {
            victims,
            events,
            metric_rows: metric_counts
                .into_iter()
                .map(|((pool, band), count)| (pool, band, count))
                .collect(),
        })
    }

    /// The eval-queue priority implied by a job's priority band.
    ///
    /// Prefers the exact config the eval names; falls back to the job's
    /// latest config when only a job id is given (a `job_reschedule` for a
    /// job, say). `None` when neither resolves — the caller then uses the
    /// manager's `default_priority`, which is exactly what an un-banded
    /// cluster did before bands existed.
    fn band_eval_priority_tx(
        &self,
        tx: &Connection,
        job_id: Option<&str>,
        job_config_id: Option<&str>,
    ) -> Option<i64> {
        let band: Option<String> = if let Some(job_config_id) = job_config_id {
            tx.query_row(
                "SELECT priority FROM job_config WHERE id = ?;",
                [job_config_id],
                |row| row.get(0),
            )
            .optional()
            .ok()
            .flatten()
        } else if let Some(job_id) = job_id {
            tx.query_row(
                "SELECT priority FROM job_config WHERE job_id = ? ORDER BY version DESC LIMIT 1;",
                [job_id],
                |row| row.get(0),
            )
            .optional()
            .ok()
            .flatten()
        } else {
            None
        };
        band.map(|band| Band::from_stored(&band).eval_priority())
    }

    /// Enqueue an eval inside an existing transaction.
    ///
    /// The public `enqueue_eval*` family opens its own write transaction,
    /// which would deadlock (and, worse, break atomicity) when called from
    /// inside the plan-apply transaction. Victim requeue must ride that
    /// transaction, so it goes through here.
    fn enqueue_eval_tx(
        &self,
        tx: &Connection,
        kind: &str,
        job_id: Option<&str>,
        job_config_id: Option<&str>,
        priority: i64,
    ) -> R<()> {
        let params = named_params! {
            ":id": None::<i64>,
            ":status": "pending",
            ":type": kind,
            ":job_id": job_id.filter(|value| !value.trim().is_empty()),
            ":job_config_id": job_config_id.filter(|value| !value.trim().is_empty()),
            ":node_id": None::<String>,
            ":priority": priority,
            ":ready_at": Self::sortable_now()?,
        };
        tx.execute(&INSERT_EVAL, params)?;
        Ok(())
    }

    /// Journal what `observe` mode would have done.
    ///
    /// Best-effort and outside any transaction: nothing downstream depends on
    /// it, and a plan must not fail because its evidence event could not be
    /// written.
    pub(crate) fn record_observed_preemptions(
        &self,
        job_config_id: &str,
        preemptor_job: &str,
        preemptor_band: Band,
        victims: &[crate::manager::scheduler::PlannedPreemption],
    ) {
        crate::manager::telemetry::metrics()
            .preemption_plans_total
            .inc(&["observe"]);
        if let Some(draft) =
            drafts::preemption_planned(job_config_id, preemptor_job, preemptor_band, victims)
        {
            self.emit_event(draft);
        }
    }

    pub(crate) fn collect_rows<T, I>(iter: I) -> R<Vec<T>>
    where
        I: Iterator<Item = Result<T, rusqlite::Error>>,
    {
        let values: Result<Vec<_>, rusqlite::Error> = iter.collect();
        let values = values.map_err(anyhow::Error::new)?;
        Ok(values)
    }

    fn ensure_kv_key(key: &str) -> R<()> {
        if key.trim().is_empty() {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "kv key cannot be empty".to_string(),
            }));
        }

        Ok(())
    }

    fn normalize_kv_value(value: &str, value_type: KvValueType) -> R<String> {
        match value_type {
            KvValueType::Unspecified => Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "kv value type must be specified".to_string(),
            })),
            KvValueType::String => Ok(value.to_string()),
            KvValueType::Number => {
                let parsed: Value = from_str(value).map_err(|err| {
                    anyhow::Error::new(StorageError::InvalidInput {
                        message: format!("invalid number value: {err}"),
                    })
                })?;
                match parsed {
                    Value::Number(_) => Ok(parsed.to_string()),
                    _ => Err(anyhow::Error::new(StorageError::InvalidInput {
                        message: "value is not a number".to_string(),
                    })),
                }
            }
            KvValueType::Json => {
                let parsed: Value = from_str(value).map_err(|err| {
                    anyhow::Error::new(StorageError::InvalidInput {
                        message: format!("invalid json value: {err}"),
                    })
                })?;
                Ok(parsed.to_string())
            }
        }
    }

    fn now_rfc3339() -> R<String> {
        Ok(OffsetDateTime::now_utc().format(&Rfc3339)?)
    }

    /// Unix seconds, the storage form of `job.created_at` / `job.updated_at`.
    ///
    /// Safe inside a replicated apply: raft replicates the resulting row
    /// values, not a re-execution of the closure, so followers never compute
    /// their own `now` (see `storage/raft/capture.rs`).
    fn now_unix() -> i64 {
        OffsetDateTime::now_utc().unix_timestamp()
    }

    /// RFC3339 with a fixed-width, nine-digit subsecond field.
    ///
    /// `Rfc3339` trims trailing zeros from the subsecond field, so its output
    /// has a variable length and does NOT sort lexicographically in
    /// chronological order: `…:03.4374Z` compares GREATER than
    /// `…:03.437409Z` because `'Z' > '0'`. Any column SQL orders or ranges
    /// with `<=` must therefore be written through this format instead.
    fn sortable_timestamp(at: OffsetDateTime) -> R<String> {
        Ok(at.format(SORTABLE_TIMESTAMP_FORMAT)?)
    }

    fn sortable_now() -> R<String> {
        Self::sortable_timestamp(OffsetDateTime::now_utc())
    }

    fn parse_rfc3339(value: &str) -> R<OffsetDateTime> {
        Ok(OffsetDateTime::parse(value, &Rfc3339)?)
    }

    fn random_lock_id() -> String {
        let mut rng = thread_rng();
        let bytes: [u8; 16] = rng.gen();
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn load_kv_lock_tx(&self, tx: &Connection, key: &str) -> R<Option<KvLock>> {
        let lock = tx
            .query_row(
                "SELECT key, lock_id, owner, acquired_at, expires_at FROM kv_lock WHERE key = ?;",
                [key],
                |row| self.kv_lock_from_row(row),
            )
            .optional()?;
        Ok(lock)
    }

    fn job_config_ids_with_incomplete_rollouts(&self) -> R<Vec<String>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT to_job_config_id
             FROM rollout
             WHERE status = 'completed_incomplete';",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        Self::collect_rows(rows)
    }

    fn enforce_kv_lock(&self, tx: &Connection, key: &str, lock_id: Option<&str>) -> R<()> {
        let Some(lock) = self.load_kv_lock_tx(tx, key)? else {
            return Ok(());
        };

        let expires_at = Self::parse_rfc3339(&lock.expires_at)?;
        if expires_at <= OffsetDateTime::now_utc() {
            tx.execute("DELETE FROM kv_lock WHERE key = ?;", [key])?;
            return Ok(());
        }

        if lock_id != Some(lock.lock_id.as_str()) {
            return Err(anyhow::Error::new(StorageError::Conflict {
                message: "kv lock is held by another client".to_string(),
            }));
        }

        Ok(())
    }

    //
    // from_row helpers
    //

    fn job_from_row(&self, row: &Row<'_>) -> Result<Job, rusqlite::Error> {
        let created_at: i64 = row.get(3)?;
        let job = Job {
            id: row.get(0)?,
            name: row.get(1)?,
            status: row.get(2)?,
            created_at,
            // NULL only for a row the backfill deliberately skipped (a
            // text `created_at`); creation time is the honest floor for
            // "last changed" when no mtime was ever recorded.
            updated_at: row.get::<_, Option<i64>>(4)?.unwrap_or(created_at),
            // Empty only for a job with no config rows at all — a job row
            // written and not yet given a spec. The default is not asserted
            // here because "no config" and "config says normal" are
            // genuinely different states to a reader.
            priority: row.get::<_, Option<String>>(5)?.unwrap_or_default(),
        };

        Ok(job)
    }

    fn node_from_row(&self, row: &Row<'_>) -> Result<Node, rusqlite::Error> {
        let node = Node {
            id: row.get(0)?,
            pool: row.get(1)?,
            status: row.get(2)?,
            availability: row.get(3)?,
            config_attrs: row.get(4)?,
            fingerprint_attrs: row.get(5)?,
            dynamic_attrs: row.get(6)?,
            health_checks: row.get(7)?,
            last_heartbeat: row.get(8)?,
        };

        Ok(node)
    }

    fn eval_from_row(&self, row: &Row<'_>) -> Result<Eval, rusqlite::Error> {
        let eval = Eval {
            id: row.get::<_, i64>(0)?.to_string(),
            status: row.get(1)?,
            kind: row.get(2)?,
            job_id: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
            job_config_id: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
            node_name: row.get::<_, Option<String>>(5)?.unwrap_or_default(),
            priority: row.get(6)?,
        };
        let _ready_at: String = row.get(7)?;

        Ok(eval)
    }

    fn kv_entry_from_row(&self, row: &Row<'_>) -> Result<KvEntry, rusqlite::Error> {
        let value_type: i32 = row.get(2)?;
        let entry = KvEntry {
            key: row.get(0)?,
            value: row.get(1)?,
            value_type,
            version: row.get(3)?,
            created_at: row.get(4)?,
            updated_at: row.get(5)?,
        };

        Ok(entry)
    }

    fn kv_lock_from_row(&self, row: &Row<'_>) -> Result<KvLock, rusqlite::Error> {
        let lock = KvLock {
            key: row.get(0)?,
            lock_id: row.get(1)?,
            owner: row.get(2)?,
            acquired_at: row.get(3)?,
            expires_at: row.get(4)?,
        };

        Ok(lock)
    }

    fn rollout_from_row(&self, row: &Row<'_>) -> Result<Rollout, rusqlite::Error> {
        let in_place: Option<i64> = row.get(10)?;
        Ok(Rollout {
            id: row.get(0)?,
            job_id: row.get(1)?,
            from_job_config_id: row.get(2)?,
            to_job_config_id: row.get(3)?,
            status: row.get(4)?,
            incomplete: row.get::<_, i64>(5)? != 0,
            message: row.get(6)?,
            strategy: row.get(7)?,
            progression: row.get(8)?,
            replacement: row.get(9)?,
            in_place: in_place.map(|value| value != 0),
            config_json: row.get(11)?,
            created_at: row.get(12)?,
            updated_at: row.get(13)?,
            completed_at: row.get(14)?,
        })
    }

    fn allocation_health_snapshot_from_row(
        &self,
        row: &Row<'_>,
    ) -> Result<AllocationHealthSnapshot, rusqlite::Error> {
        Ok(AllocationHealthSnapshot {
            allocation_id: row.get(0)?,
            node_id: row.get(1)?,
            healthy: row.get::<_, i64>(2)? != 0,
            status: row.get(3)?,
            updated_at: row.get(4)?,
            artifact_states: row.get(5)?,
        })
    }

    fn rollout_event_from_row(&self, row: &Row<'_>) -> Result<RolloutEventRecord, rusqlite::Error> {
        Ok(RolloutEventRecord {
            id: row.get(0)?,
            rollout_id: row.get(1)?,
            job_id: row.get(2)?,
            group_name: row.get(3)?,
            level: row.get(4)?,
            message: row.get(5)?,
            metadata_json: row.get(6)?,
            created_at: row.get(7)?,
        })
    }

    fn job_kv_reference_from_row(&self, row: &Row<'_>) -> Result<JobKvReference, RusqliteError> {
        Ok(JobKvReference {
            job_id: row.get(0)?,
            job_config_id: row.get(1)?,
            kv_key: row.get(2)?,
            json_pointer: row.get(3)?,
        })
    }

    fn worker_type_from_row(&self, row: &Row<'_>) -> Result<WorkerConfig, rusqlite::Error> {
        let af: String = row.get(2)?;
        let attrs: String = row.get(3)?;
        let parsed_attrs = serde_json::from_str(&attrs).unwrap();
        let name_prefix = row.get(4)?;
        let resources = row.get(5)?;
        let reserved_resources = row.get(6)?;
        let max_allocations = row.get(7)?;
        let artifacts = row.get(8)?;

        Ok(WorkerConfig {
            name: row.get(0)?,
            pool: row.get(1)?,
            additional_node_checks: af
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.to_string())
                .collect(),
            attributes: parsed_attrs,
            name_prefix,
            resources,
            reserved_resources,
            max_allocations,
            artifacts,
            host_volumes: row.get(9)?,
            heartbeat_seconds: row.get(10)?,
            resource_enforcement: row.get(11)?,
        })
    }

    fn parse_checks_column(
        &self,
        idx: usize,
        raw: String,
    ) -> Result<Vec<TaskCheck>, rusqlite::Error> {
        serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_on_unhealthy_column(
        &self,
        idx: usize,
        raw: String,
    ) -> Result<TaskOnUnhealthyConfig, rusqlite::Error> {
        serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_schedule_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Option<TaskSchedule>, rusqlite::Error> {
        raw.map(|value| {
            value.parse::<TaskSchedule>().map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    idx,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
                )
            })
        })
        .transpose()
    }

    fn parse_depends_on_column(
        &self,
        idx: usize,
        raw: String,
    ) -> Result<Vec<TaskDependency>, rusqlite::Error> {
        serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_kv_injections_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Option<crate::structs::TaskKvInjections>, rusqlite::Error> {
        let Some(raw) = raw else {
            return Ok(None);
        };
        serde_json::from_str(&raw).map(Some).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_artifacts_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Vec<crate::structs::ArtifactSpec>, rusqlite::Error> {
        let Some(raw) = raw else {
            return Ok(Vec::new());
        };
        serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_volumes_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Vec<crate::structs::VolumeSpec>, rusqlite::Error> {
        let Some(raw) = raw else {
            return Ok(Vec::new());
        };
        serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_actions_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Vec<crate::structs::ActionSpec>, rusqlite::Error> {
        let Some(raw) = raw else {
            return Ok(Vec::new());
        };
        serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_identities_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Vec<crate::structs::WorkloadIdentity>, rusqlite::Error> {
        let Some(raw) = raw else {
            return Ok(Vec::new());
        };
        serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_stateful_column(
        &self,
        idx: usize,
        raw: Option<String>,
    ) -> Result<Option<StatefulConfig>, rusqlite::Error> {
        let Some(raw) = raw else {
            return Ok(None);
        };
        serde_json::from_str(&raw).map(Some).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    }

    fn parse_task_lifecycle_column(
        &self,
        idx: usize,
        raw: String,
    ) -> Result<TaskLifecycle, rusqlite::Error> {
        raw.parse::<TaskLifecycle>().map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
            )
        })
    }

    fn task_config_from_row(&self, row: &Row<'_>) -> Result<TaskConfig, rusqlite::Error> {
        let count_num: u32 = row.get::<_, i64>(3)? as u32;
        let count_per: Option<String> = row.get(4)?;
        let group: String = row.get(5)?;
        let lifecycle = self.parse_task_lifecycle_column(6, row.get(6)?)?;
        let schedule = self.parse_schedule_column(7, row.get(7)?)?;
        let depends_on = self.parse_depends_on_column(8, row.get(8)?)?;
        let task_config = TaskConfig {
            name: row.get(1)?,
            runtime: row.get(2)?,
            count: TaskCount {
                num: count_num,
                per: count_per,
            },
            group,
            lifecycle,
            schedule,
            depends_on,
            config: row.get(9)?,
            resources: row.get(10)?,
            constraints: row.get(11)?,
            permit: row.get(12)?,
            affinities: row.get(13)?,
            checks: self.parse_checks_column(14, row.get(14)?)?,
            on_unhealthy: self.parse_on_unhealthy_column(15, row.get(15)?)?,
            kv_injections: self.parse_kv_injections_column(16, row.get(16)?)?,
            identities: self.parse_identities_column(19, row.get(19)?)?,
            stateful: self.parse_stateful_column(20, row.get(20)?)?,
            artifacts: self.parse_artifacts_column(21, row.get(21)?)?,
            network: parse_json_column(22, row.get(22)?)?,
            service: parse_json_column(23, row.get(23)?)?,
            actions: self.parse_actions_column(24, row.get(24)?)?,
            volumes: self.parse_volumes_column(25, row.get(25)?)?,
            connect: parse_json_column(26, row.get(26)?)?,
            restart: parse_json_column(27, row.get(27)?)?,
            stop: parse_json_column(28, row.get(28)?)?,
            disruption: parse_json_column(29, row.get(29)?)?,
        };

        Ok(task_config)
    }

    fn allocation_from_row(&self, row: &Row<'_>) -> Result<Allocation, rusqlite::Error> {
        let allocation = Allocation {
            id: row.get(0)?,
            status: row.get(1)?,
            resources: row.get(2)?,
            node_id: row.get(3)?,
            job_config_id: row.get(4)?,
            task_config_id: row.get(5)?,
            job_id: row.get(6)?,
            instance_id: row.get(7)?,
            network: parse_json_column(8, row.get(8)?)?,
        };

        Ok(allocation)
    }

    fn instance_from_row(&self, row: &Row<'_>) -> Result<Instance, rusqlite::Error> {
        let mode: String = row.get(4)?;
        let state: String = row.get(5)?;
        let parse_err = |idx: usize, err: String| {
            rusqlite::Error::FromSqlConversionFailure(
                idx,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
            )
        };
        let instance = Instance {
            instance_id: row.get(0)?,
            job_id: row.get(1)?,
            group: row.get(2)?,
            task_name: row.get(3)?,
            mode: mode.parse().map_err(|err| parse_err(4, err))?,
            state: state.parse().map_err(|err| parse_err(5, err))?,
            slot_index: row.get::<_, Option<i64>>(6)?.map(|slot| slot as u32),
            current_alloc_id: row.get(7)?,
            storage_handle: row.get(8)?,
            created_at: row.get(9)?,
            updated_at: row.get(10)?,
        };

        Ok(instance)
    }

    fn manager_config_from_row(&self, row: &Row<'_>) -> Result<ManagerNodeConfig, rusqlite::Error> {
        Ok(ManagerNodeConfig {
            node_id: row.get(0)?,
            config_json: row.get(1)?,
            updated_at: row.get(2)?,
        })
    }

    //
    // MANAGER CONFIG
    //

    #[instrument(err, skip_all)]
    pub fn upsert_manager_config(&self, node_id: &str, config: &ManagerConfigFull) -> R<()> {
        let node_id = node_id.to_owned();
        let config = config.clone();
        self.replicated(Lane::Control, "upsert_manager_config", move |s| {
            s.upsert_manager_config_apply(&node_id, &config)
        })
    }

    fn upsert_manager_config_apply(&self, node_id: &str, config: &ManagerConfigFull) -> R<()> {
        let fmt = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
                 sign:mandatory]:[offset_minute]:[offset_second]"
        );

        let timestamp_now = OffsetDateTime::now_utc().format(fmt)?;
        let config_json = to_string_pretty(config)?;

        let conn = self.write_connection()?;

        let params = named_params! {
            ":node_id": node_id,
            ":config": config_json,
            ":updated_at": timestamp_now,
        };

        conn.execute(&UPSERT_MANAGER_CONFIG, params)?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn get_manager_configs(&self) -> R<Vec<ManagerNodeConfig>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_MANAGER_CONFIGS.as_str())?;
        let configs =
            Self::collect_rows(stmt.query_map([], |row| self.manager_config_from_row(row))?)?;

        Ok(configs)
    }

    //
    // NODES
    //

    #[instrument(err, skip_all)]
    pub fn get_node(&self, id: &str) -> R<Node> {
        let conn = self.read_connection()?;
        let node = conn
            .query_row(&GET_NODE, [id], |row| self.node_from_row(row))
            .map_err(|error| match error {
                RusqliteError::QueryReturnedNoRows => {
                    anyhow::Error::new(StorageError::not_found("node", id))
                }
                other => anyhow::Error::from(other),
            })?;
        Ok(node)
    }

    #[instrument(err, skip_all)]
    pub fn get_nodes(&self) -> R<Vec<Node>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_NODES.as_str())?;
        let nodes = Self::collect_rows(stmt.query_map([], |row| self.node_from_row(row))?)?;

        Ok(nodes)
    }

    pub fn job_config_ids_with_count_per(&self) -> R<Vec<String>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT job_config_id FROM task_config WHERE count_per IS NOT NULL AND count_per != '';",
        )?;
        let rows = stmt.query_map([], |row| row.get(0))?;
        Self::collect_rows(rows)
    }

    /// Mark a node's liveness ready. Composite across the lane boundary of
    /// the node/node_health split (plan §7-C2, C-2): the STATUS lane upserts
    /// the `node_health` liveness row, then the CONTROL lane flips the
    /// schedulability axis (`node.availability`) back to `available` —
    /// `node` is control-owned, so the flip must not ride the status op's
    /// changeset. Two single-lane raft entries, same JC-30 composite shape
    /// as `insert_node`.
    #[instrument(err, skip_all)]
    pub fn mark_node_ready(&self, id: &str, last_heartbeat: &str) -> R<()> {
        // Both halves are forwarding wrappers (plan §8, D2): the composite
        // completes from whichever node runs it — the registration RPC's
        // manager, or the scheduler-lane leader's node_join eval handler —
        // regardless of where the status and control leaders sit.
        match self.mark_node_ready_status_local(id, last_heartbeat) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => {
                self.forward_after_rejection::<()>(
                    crate::storage::raft::forward::ForwardableLaneWrite::MarkNodeReady {
                        id: id.to_owned(),
                        last_heartbeat: last_heartbeat.to_owned(),
                    },
                    err,
                )?;
            }
            Err(err) => return Err(err),
            Ok(()) => {}
        }
        self.sync_node_availability(&[id.to_owned()], "available")
    }

    /// The status half of [`Self::mark_node_ready`], non-forwarding (also
    /// the `ForwardLaneWrite` executor's entry point).
    pub(crate) fn mark_node_ready_status_local(&self, id: &str, last_heartbeat: &str) -> R<()> {
        let id_owned = id.to_owned();
        let last_heartbeat = last_heartbeat.to_owned();
        self.replicated(Lane::Status, "mark_node_ready", move |s| {
            s.mark_node_ready_apply(&id_owned, &last_heartbeat)
        })
    }

    fn mark_node_ready_apply(&self, id: &str, last_heartbeat: &str) -> R<()> {
        let conn = self.write_connection()?;
        // A ready node is by definition not lost: clear lost_at so the
        // node-loss confirmation gate stops counting (its gated allocations
        // keep running, bound to their instances). Liveness upserts into
        // node_health (plan §7-C2, C-1) — the EXISTS guard keeps a ready
        // mark for an unregistered node a no-op, like the pre-split UPDATE.
        let sql = r#"
        INSERT INTO node_health (node_id, status, last_heartbeat, lost_at, health_checks)
        SELECT :id, :status, :last_heartbeat, NULL, '{}'
        WHERE EXISTS (SELECT 1 FROM node WHERE id = :id)
        ON CONFLICT(node_id) DO UPDATE SET
            status = excluded.status, last_heartbeat = excluded.last_heartbeat, lost_at = NULL;
        "#;

        let params = named_params! {
            ":id": id,
            ":status": "ready",
            ":last_heartbeat": last_heartbeat,
        };

        conn.execute(sql, params)?;

        drop(conn);
        let node = self.get_node(id)?;
        self.cache_node(node)?;

        self.events().node_ready(id);

        Ok(())
    }

    /// Mark a node's liveness lost. Same status-then-control composite shape
    /// as [`Self::mark_node_ready`] (plan §7-C2, C-2): liveness rides the
    /// status lane, the `node.availability` flip rides the control lane.
    /// The reschedule tail is NOT here — callers that need it (the node
    /// monitor via [`Self::mark_nodes_as_lost`], the `node_drop` eval)
    /// drive [`Self::reschedule_allocations_for_lost_node`] separately.
    #[instrument(err, skip_all)]
    pub fn mark_node_lost(&self, id: &str) -> R<()> {
        // Same forwarding shape as mark_node_ready: the node_drop eval runs
        // on the scheduler-lane leader, which may lead neither the status
        // nor the control lane.
        match self.mark_node_lost_status_local(id) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => {
                self.forward_after_rejection::<()>(
                    crate::storage::raft::forward::ForwardableLaneWrite::MarkNodeLost {
                        id: id.to_owned(),
                    },
                    err,
                )?;
            }
            Err(err) => return Err(err),
            Ok(()) => {}
        }
        self.sync_node_availability(&[id.to_owned()], "unavailable")
    }

    /// The status half of [`Self::mark_node_lost`], non-forwarding (also
    /// the `ForwardLaneWrite` executor's entry point).
    pub(crate) fn mark_node_lost_status_local(&self, id: &str) -> R<()> {
        let id_owned = id.to_owned();
        self.replicated(Lane::Status, "mark_node_lost", move |s| {
            s.mark_node_lost_apply(&id_owned)
        })
    }

    fn mark_node_lost_apply(&self, id: &str) -> R<()> {
        let conn = self.write_connection()?;
        // lost_at is set once (COALESCE on the conflict path) so repeated
        // marks never push the loss-confirmation clock back; it is cleared
        // when the node heartbeats again or re-registers. Liveness upserts
        // into node_health (plan §7-C2, C-1), guarded on node existence.
        let sql = r#"
        INSERT INTO node_health (node_id, status, last_heartbeat, lost_at, health_checks)
        SELECT :id, :status, '', :lost_at, '{}'
        WHERE EXISTS (SELECT 1 FROM node WHERE id = :id)
        ON CONFLICT(node_id) DO UPDATE SET
            status = excluded.status,
            lost_at = COALESCE(node_health.lost_at, excluded.lost_at);
        "#;

        let params = named_params! {
            ":id": id,
            ":status": "lost",
            ":lost_at": Self::now_rfc3339()?,
        };

        conn.execute(sql, params)?;

        drop(conn);
        let node = self.get_node(id)?;
        self.cache_node(node)?;

        self.events().node_lost(id);

        Ok(())
    }

    /// Control-lane half of the status composites (plan §7-C2, C-2): set
    /// `node.availability` for a batch of nodes WITHOUT events or
    /// validation — this is the internal schedulability sync that
    /// mark-ready/lost perform, not the operator cordon/drain verb
    /// ([`Self::set_node_availability`] stays that). No-op for ids that are
    /// not registered, like the writes it replaces.
    fn sync_node_availability(&self, ids: &[String], availability: &str) -> R<()> {
        if ids.is_empty() {
            return Ok(());
        }
        // Forwarding wrapper (plan §8, D2): this is the control half of the
        // mark-ready/mark-lost composites, run from the STATUS-lane
        // leader's checker — under divergent leadership its rejection left
        // a lost node's `availability` stuck `available`, so the scheduler
        // kept placing replacements onto a node the manager could no longer
        // observe.
        match self.sync_node_availability_local(ids, availability) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::SyncNodeAvailability {
                        ids: ids.to_vec(),
                        availability: availability.to_owned(),
                    },
                    err,
                ),
            other => other,
        }
    }

    pub(crate) fn sync_node_availability_local(&self, ids: &[String], availability: &str) -> R<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let ids = ids.to_vec();
        let availability = availability.to_owned();
        self.replicated(Lane::Control, "sync_node_availability", move |s| {
            s.sync_node_availability_apply(&ids, &availability)
        })
    }

    fn sync_node_availability_apply(&self, ids: &[String], availability: &str) -> R<()> {
        {
            let conn = self.write_connection()?;
            let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            // Leaving 'drained' also clears the drain-eviction intent flag
            // (the sync paths never SET 'drained', only ready/lost flips).
            let sql = format!(
                "UPDATE node SET availability = ?1,
                        drain_evict = CASE WHEN ?1 = 'drained' THEN drain_evict ELSE 0 END
                 WHERE id IN ({placeholders});"
            );
            let mut params: Vec<&dyn rusqlite::ToSql> = vec![&availability];
            for id in ids {
                params.push(id);
            }
            conn.execute(&sql, params.as_slice())?;
        }
        for id in ids {
            // Unregistered ids updated nothing; skip them in the cache too.
            if let Ok(node) = self.get_node(id) {
                self.cache_node(node)?;
            }
        }
        Ok(())
    }

    /// Set ONLY the node's scheduling availability, leaving status,
    /// heartbeat, and `lost_at` untouched. Valid values: `"available"` |
    /// `"drained"`.
    ///
    /// This is the cordon/uncordon primitive: the scheduler's single
    /// placement chokepoint (`Scheduler::available_nodes`) and the store's
    /// reconcile path both filter on `availability == "available"`, so
    /// `"drained"` excludes the node from all new placements while its
    /// existing allocations keep running. Emits `node.cordoned` /
    /// `node.uncordoned`. Idempotent.
    #[instrument(err, skip_all)]
    pub fn set_node_availability(&self, id: &str, availability: &str) -> R<Node> {
        self.set_node_availability_with_evict(id, availability, false)
    }

    /// [`Self::set_node_availability`] carrying the operator's EVICT intent
    /// as replicated state (JC-27 failover regeneration): `drain --evict`
    /// used to be distinguishable from a plain cordon only by the
    /// leader-local `node_drain` eval, so post-failover regeneration evicted
    /// every drained node — including no-evict cordons. The flag is set only
    /// for an evicting drain and cleared whenever availability leaves
    /// `drained`; [`Self::regenerate_scheduling_evals`] regenerates
    /// `node_drain` only for nodes with the flag set.
    #[instrument(err, skip_all)]
    pub fn set_node_availability_with_evict(
        &self,
        id: &str,
        availability: &str,
        evict: bool,
    ) -> R<Node> {
        let id = id.to_owned();
        let availability = availability.to_owned();
        self.replicated(Lane::Control, "set_node_availability", move |s| {
            s.set_node_availability_apply(&id, &availability, evict)
        })
    }

    fn set_node_availability_apply(&self, id: &str, availability: &str, evict: bool) -> R<Node> {
        if availability != "available" && availability != "drained" {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: format!(
                    "invalid availability {availability:?}: expected \"available\" or \"drained\""
                ),
            }));
        }

        let conn = self.write_connection()?;
        conn.execute(
            "UPDATE node SET availability = :availability, drain_evict = :drain_evict
             WHERE id = :id;",
            named_params! {
                ":id": id,
                ":availability": availability,
                ":drain_evict": (availability == "drained" && evict) as i64,
            },
        )?;
        drop(conn);

        // A missing node updated zero rows above; surface it as NotFound.
        let node = self.get_node(id)?;
        self.cache_node(node.clone())?;

        if availability == "drained" {
            self.events().node_cordoned(id);
        } else {
            self.events().node_uncordoned(id);
        }

        Ok(node)
    }

    /// Free a drained node's allocations for replacement elsewhere —
    /// drain's counterpart to
    /// [`Self::reschedule_allocations_for_lost_node`]:
    ///
    /// - Stateless allocations are removed immediately. The node is alive
    ///   and heartbeating, so the next heartbeat reply shrinks its desired
    ///   set and the worker stops the tasks cleanly — the loss-confirmation
    ///   delay of the lost-node path does not apply.
    /// - Stateful allocations run through the exact same protection gate as
    ///   the lost-node path ([`Self::lost_allocation_replaceable`]). A
    ///   drained-but-alive node has no `lost_at`, so the loss duration is
    ///   `None` and every stateful allocation is held: a drain never
    ///   force-moves protected state. Held allocations keep running on the
    ///   drained node until the node is actually lost (at which point
    ///   [`Self::recheck_lost_nodes`] takes over) or an operator replaces
    ///   them explicitly (`ReplaceInstance`).
    ///
    /// Emits `node.drained` with evicted/held counts. Idempotent — a repeat
    /// drain finds nothing removable and records an event with `evicted: 0`.
    /// Since C-2 this is a wrapper-level composition of single-purpose
    /// scheduler-lane ops (`remove_allocations`, then one
    /// `reconcile_job_config_allocations` per affected config) rather than
    /// one composite capture: a capture's reads go through the read pool
    /// and see only COMMITTED state, so a reconcile composed into the same
    /// capture as the eviction could not observe the eviction and placed
    /// nothing (latent raft-mode bug, exposed by
    /// tests/raft_ownership_guard.rs). Sequencing the ops lets each read
    /// its predecessor's committed effects; a crash mid-sequence is healed
    /// by the idempotent drain eval re-running the pass.
    #[instrument(err, skip_all)]
    pub fn reschedule_allocations_for_drained_node(&self, node_id: &str) -> R<()> {
        let allocs = self.allocs_for_node(node_id)?;
        if allocs.is_empty() {
            self.events().node_drained(node_id, 0, 0);
            return Ok(());
        }

        let lost_for = self.node_lost_for(node_id)?;
        let mut removable = Vec::new();
        let mut held = 0usize;
        for alloc in allocs {
            if self.lost_allocation_replaceable(&alloc, lost_for)? {
                removable.push(alloc);
            } else {
                held += 1;
                info!(
                    allocation_id = alloc.id,
                    %node_id,
                    instance_id = alloc.instance_id.as_deref().unwrap_or_default(),
                    "Holding stateful allocation on drained node (protected by onLost policy)"
                );
            }
        }

        let evicted = removable.len();
        let affected_job_configs = self.remove_allocations(&removable, StopCause::Drain)?;
        for job_config_id in affected_job_configs {
            self.reconcile_job_config_allocations(&job_config_id)?;
        }

        self.events().node_drained(node_id, evicted, held);

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn remove_allocs_for_node(&self, node_id: &str) -> R<Vec<Allocation>> {
        let node_id = node_id.to_owned();
        self.replicated(Lane::Scheduler, "remove_allocs_for_node", move |s| {
            s.remove_allocs_for_node_apply(&node_id)
        })
    }

    fn remove_allocs_for_node_apply(&self, node_id: &str) -> R<Vec<Allocation>> {
        let allocs = self.allocs_for_node(node_id)?;

        if allocs.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        tx.execute("DELETE FROM allocation WHERE node_id = ?;", [node_id])?;
        self.release_instance_bindings_tx(&tx, &allocs)?;
        self.bump_epochs_for_allocations_tx(&tx, &allocs)?;
        // The node itself is going away, so these notices will almost never
        // be delivered — recorded anyway because "almost never" is not
        // "never": a node removed while still heartbeating (a decommission
        // racing its own worker) does read them.
        let stopping_event = self.record_stop_notices_tx(&tx, &allocs, StopCause::NodeLost)?;
        tx.commit()?;
        self.publish_staged(stopping_event);
        drop(conn);

        // Refresh by allocation SCOPES, not just the node: the bump above
        // advanced the removed allocations' job_config epochs too, and the
        // epoch mirror must learn them with the cache (plan-fence fix).
        self.cache_remove_allocations(&allocs)?;

        self.events()
            .allocations_removed_from_node(node_id, &allocs, StopCause::NodeLost);

        Ok(allocs)
    }

    /// Remove a node. Composite across lane boundaries (plan §7-C2, C-2):
    /// the SCHEDULER lane removes the node's allocations first (the
    /// `allocation.node_id` FK cascade is gone — a control-lane node delete
    /// must not carry scheduler-lane rows), then the CONTROL lane deletes
    /// the `node` row. `node_health`/`allocation_health` rows left behind
    /// are harmless orphans (every reader hydrates FROM `node`/
    /// `allocation`) swept by the status lane
    /// ([`Self::prune_node_health_orphans`] /
    /// [`Self::prune_allocation_health_orphans`]).
    #[instrument(err, skip_all)]
    pub fn remove_node(&self, id: &str) -> R<()> {
        self.remove_allocs_for_node(id)?;
        let id = id.to_owned();
        self.replicated(Lane::Control, "remove_node", move |s| {
            s.remove_node_apply(&id)
        })
    }

    fn remove_node_apply(&self, id: &str) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute("DELETE FROM node WHERE id = ?;", [id])?;
        drop(conn);

        self.cache_remove_node(id)?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    /// Every active node's id and last heartbeat, for the liveness tick:
    /// the RFC3339 text plus the integer unix clock heartbeats also write
    /// (heartbeat-scale plan §4). The unix value is `None` only for rows
    /// last written before the column existed — the tick falls back to
    /// parsing the text for those, and they age out on their next
    /// heartbeat.
    pub fn get_node_ids_and_heartbeats(&self) -> R<Vec<(String, String, Option<i64>)>> {
        let conn = self.read_connection()?;
        // Anchored on `node` (a node_health row orphaned by node removal
        // must not resurrect a ghost); a registered node without a health
        // row hydrates the registration defaults, exactly as before the
        // split (status 'active', empty heartbeat).
        let sql = "SELECT node.id, COALESCE(node_health.last_heartbeat, ''), node_health.last_heartbeat_unix
                   FROM node LEFT JOIN node_health ON node_health.node_id = node.id
                   WHERE COALESCE(node_health.status, 'active') = 'active'";
        let mut stmt = conn.prepare(sql)?;
        let nodes_id_and_hb = Self::collect_rows(
            stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?,
        )?;

        Ok(nodes_id_and_hb)
    }

    /// Mark a batch of nodes lost after missed heartbeats. Composite across
    /// lane boundaries (plan §7-C2, C-2): the STATUS lane records liveness
    /// (`node_health`), the CONTROL lane flips `node.availability`, and the
    /// reschedule tail moves behind a `node_lost_reschedule` eval — the
    /// eval handler (scheduler lane) runs
    /// [`Self::reschedule_allocations_for_lost_node`], which owns the
    /// stateful-protection gates. The enqueue is its OWN scheduler-lane op,
    /// issued after the status half commits (JC-41 × JC-33 — an eval written
    /// inside this status capture would be a foreign-lane write the
    /// ownership guard rejects), and deduped per node while one is pending.
    #[instrument(err, skip_all)]
    pub fn mark_nodes_as_lost(&self, ids: &[String]) -> R<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let ids_owned = ids.to_vec();
        self.replicated(Lane::Status, "mark_nodes_as_lost", move |s| {
            s.mark_nodes_as_lost_apply(&ids_owned)
        })?;
        self.sync_node_availability(ids, "unavailable")?;
        for node_id in ids {
            self.enqueue_lost_node_reschedule(node_id)?;
        }
        Ok(())
    }

    fn mark_nodes_as_lost_apply(&self, ids: &[String]) -> R<()> {
        {
            let conn = self.write_connection()?;
            let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            // See mark_node_lost: lost_at is set once and never refreshed
            // while the node stays lost. Liveness upserts into node_health
            // (plan §7-C2, C-1); selecting FROM node keeps unregistered ids
            // a no-op, like the pre-split UPDATE.
            let now = Self::now_rfc3339()?;
            let sql = format!(
                "INSERT INTO node_health (node_id, status, last_heartbeat, lost_at, health_checks) \
                 SELECT id, 'lost', '', '{now}', '{{}}' FROM node WHERE id IN ({placeholders}) \
                 ON CONFLICT(node_id) DO UPDATE SET status = 'lost', \
                 lost_at = COALESCE(node_health.lost_at, excluded.lost_at)"
            );
            let mut stmt = conn.prepare(&sql)?;
            stmt.execute(params_from_iter(ids))?;
        }

        self.cache_nodes_lost(ids)?;

        for node_id in ids {
            self.events().node_lost_after_missed_heartbeats(node_id);
        }

        Ok(())
    }
    /// Enqueue the scheduler-lane reschedule pass for a lost node as its own
    /// scheduler-lane eval op (plan §7-C2, C-2), skipping the enqueue while one is
    /// already pending/in-flight for the node (the monitor re-drives every
    /// tick, so a completed pass is re-enqueued on the next tick until the
    /// node recovers or nothing is left to free — the recheck-loop
    /// behavior, now eval-shaped).
    fn enqueue_lost_node_reschedule(&self, node_id: &str) -> R<()> {
        let already_queued: bool = {
            let conn = self.read_connection()?;
            conn.query_row(
                "SELECT EXISTS (
                     SELECT 1 FROM eval
                     WHERE status IN ('pending', 'in_flight')
                       AND type = 'node_lost_reschedule' AND node_id = ?1
                 );",
                [node_id],
                |row| row.get(0),
            )?
        };
        if already_queued {
            return Ok(());
        }
        self.enqueue_eval("node_lost_reschedule", None, None, Some(node_id), None)?;
        Ok(())
    }

    /// When the node was marked lost (RFC3339), or `None` while it is not
    /// lost. Drives the stateful node-loss confirmation gate (see
    /// [`Self::reschedule_allocations_for_lost_node`]).
    #[instrument(err, skip_all)]
    pub fn node_lost_at(&self, node_id: &str) -> R<Option<String>> {
        let conn = self.read_connection()?;
        let lost_at: Option<String> = conn
            .query_row(
                "SELECT lost_at FROM node_health WHERE node_id = ?;",
                [node_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        Ok(lost_at)
    }

    /// Overwrite a node's `lost_at` timestamp. Test/ops helper for the
    /// node-loss confirmation gate: lets a loss be backdated so the
    /// `replaceAfter` threshold can be exercised without real waiting.
    #[instrument(err, skip_all)]
    pub fn set_node_lost_at(&self, node_id: &str, lost_at: &str) -> R<()> {
        let node_id = node_id.to_owned();
        let lost_at = lost_at.to_owned();
        self.replicated(Lane::Status, "set_node_lost_at", move |s| {
            s.set_node_lost_at_apply(&node_id, &lost_at)
        })
    }

    fn set_node_lost_at_apply(&self, node_id: &str, lost_at: &str) -> R<()> {
        let conn = self.write_connection()?;
        // Upsert so the helper works even for a node that never wrote a
        // node_health row; status defaults to the hydration default so the
        // insert path changes nothing else observable.
        conn.execute(
            "INSERT INTO node_health (node_id, status, last_heartbeat, lost_at, health_checks)
             SELECT ?2, 'active', '', ?1, '{}' WHERE EXISTS (SELECT 1 FROM node WHERE id = ?2)
             ON CONFLICT(node_id) DO UPDATE SET lost_at = excluded.lost_at;",
            params![lost_at, node_id],
        )?;
        Ok(())
    }

    /// Ids of all nodes currently marked lost. Anchored on `node` so an
    /// orphaned node_health row (node removed) never resurrects a ghost.
    #[instrument(err, skip_all)]
    pub fn lost_node_ids(&self) -> R<Vec<String>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT node.id FROM node
             INNER JOIN node_health ON node_health.node_id = node.id
             WHERE node_health.status = 'lost';",
        )?;
        let ids = Self::collect_rows(stmt.query_map([], |row| row.get(0))?)?;
        Ok(ids)
    }

    /// Re-drive the lost-node reschedule path for every node still marked
    /// lost, so stateful allocations whose `onLost.replaceAfter` threshold
    /// has since elapsed get replaced WITHOUT operator action. Called by the
    /// manager's node-status monitor on every tick (the same loop that marks
    /// nodes lost). Since C-2 this enqueues the per-node
    /// `node_lost_reschedule` eval (deduped while one is pending) instead of
    /// running the scheduler-lane reschedule inline from a status-lane
    /// context (plan §7-C2); the eval handler performs the actual pass.
    /// Errors are logged per node so one wedged enqueue cannot stall the
    /// sweep.
    #[instrument(skip_all)]
    pub fn recheck_lost_nodes(&self) {
        let node_ids = match self.lost_node_ids() {
            Ok(node_ids) => node_ids,
            Err(err) => {
                warn!(error = ?err, "Failed listing lost nodes for loss-confirmation recheck");
                return;
            }
        };
        if node_ids.is_empty() {
            return;
        }
        // One query for every lost node's pending/in-flight reschedule eval
        // instead of one EXISTS probe per node per tick (heartbeat-scale
        // plan §4): the semantics are identical — re-enqueue whenever no
        // eval is queued — but a 2,000-node partition costs one query per
        // tick, not 2,000.
        let already_queued: HashSet<String> = {
            let queued = (|| -> R<Vec<String>> {
                let conn = self.read_connection()?;
                let mut stmt = conn.prepare(
                    "SELECT DISTINCT node_id FROM eval
                     WHERE status IN ('pending', 'in_flight')
                       AND type = 'node_lost_reschedule' AND node_id IS NOT NULL;",
                )?;
                let queued = Self::collect_rows(stmt.query_map([], |row| row.get(0))?)?;
                Ok(queued)
            })();
            match queued {
                Ok(queued) => queued.into_iter().collect(),
                Err(err) => {
                    warn!(error = ?err, "Failed listing queued lost-node reschedules");
                    return;
                }
            }
        };
        for node_id in node_ids {
            if already_queued.contains(&node_id) {
                continue;
            }
            if let Err(err) = self.enqueue_lost_node_reschedule(&node_id) {
                warn!(
                    %node_id,
                    error = ?err,
                    "Failed enqueueing lost-node reschedule eval"
                );
            }
        }
    }

    /// Register (or re-register) a node. Composite across the lane boundary
    /// of the node/node_health split (plan §7-C2, C-1): the CONTROL lane
    /// records identity/pool/attrs/availability in `node`, then the STATUS
    /// lane seeds the `node_health` liveness row (INSERT OR REPLACE — a
    /// re-registering node is by definition not lost, so a stale `lost_at`
    /// resets). Two single-lane raft entries; cross-lane atomicity was
    /// never promised (§A2), and a crash between them leaves a node row
    /// whose missing health row hydrates the same defaults registration
    /// seeds, until the worker's first heartbeat/health-check upserts it.
    #[instrument(err, skip_all)]
    pub fn insert_node(&self, node: NodeMsg) -> R<Node> {
        let seed = node.clone();
        let registered = self.replicated(Lane::Control, "insert_node", move |s| {
            s.insert_node_apply(node)
        })?;
        self.replicated(Lane::Status, "seed_node_health", move |s| {
            s.seed_node_health_apply(&seed)
        })?;
        Ok(registered)
    }

    fn insert_node_apply(&self, node: NodeMsg) -> R<Node> {
        let conn = self.write_connection()?;
        conn.execute(
            INSERT_NODE,
            named_params! {
                ":id": node.id,
                ":pool": node.pool,
                ":availability": node.availability,
                ":config_attrs": node.config_attrs,
                ":fingerprint_attrs": node.fingerprint_attrs,
                ":dynamic_attrs": node.dynamic_attrs,
            },
        )?;
        drop(conn);

        // The stored row IS the message's contents (liveness included once
        // the status-lane seed lands), so hydrate the returned Node from it.
        let node = Node::from(node);

        self.cache_node(node.clone())?;

        self.events().node_registered(&node);

        Ok(node)
    }

    fn seed_node_health_apply(&self, node: &NodeMsg) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute(
            SEED_NODE_HEALTH,
            named_params! {
                ":id": node.id,
                ":status": node.status,
                ":last_heartbeat": node.last_heartbeat,
                ":health_checks": node.health_checks,
            },
        )?;
        drop(conn);

        let node = self.get_node(&node.id)?;
        self.cache_node(node)?;

        Ok(())
    }

    /// Sweep `node_health` rows orphaned by node removal (`remove_node` is
    /// a control-lane op and must not delete status-lane rows, plan §7-C2).
    /// Orphans are harmless while they exist — every hydration LEFT JOINs
    /// from `node` — so this is periodic maintenance, run by the
    /// node-status checker on the status-lane leader. Skips the replication
    /// seam entirely when there is nothing to sweep.
    ///
    /// **Two-sweep observation grace (adversarial finding F2, 2026-07-28).**
    /// Orphanhood is judged against this node's LOCAL control-lane replica,
    /// which can lag: a freshly failed-over status leader may not yet have
    /// applied recent registrations, and the DELETE it proposes replicates
    /// to every node — erasing a live `node_health` row (and with it
    /// `lost_at`, the stateful loss-confirmation clock). A row is therefore
    /// only deleted after appearing in TWO consecutive sweeps; the
    /// candidate set is process-local, so a leadership change always starts
    /// a fresh observation window.
    #[instrument(err, skip_all)]
    pub fn prune_node_health_orphans(&self) -> R<usize> {
        let orphans: Vec<String> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(
                "SELECT node_health.node_id FROM node_health
                 LEFT JOIN node ON node.id = node_health.node_id
                 WHERE node.id IS NULL;",
            )?;
            let orphans = Self::collect_rows(stmt.query_map([], |row| row.get(0))?)?;
            drop(stmt);
            orphans
        };
        let confirmed: Vec<String> = {
            let mut candidates = self
                .node_health_orphan_candidates
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let current: HashSet<String> = orphans.into_iter().collect();
            let confirmed = current
                .iter()
                .filter(|id| candidates.contains(*id))
                .cloned()
                .collect();
            *candidates = current;
            confirmed
        };
        if confirmed.is_empty() {
            return Ok(0);
        }
        self.replicated(Lane::Status, "prune_node_health_orphans", move |s| {
            s.prune_node_health_orphans_apply(&confirmed)
        })
    }

    fn prune_node_health_orphans_apply(&self, ids: &[String]) -> R<usize> {
        let conn = self.write_connection()?;
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        // Re-check orphanhood inside the write so a node that re-registered
        // between the read and this apply keeps its fresh health row.
        let sql = format!(
            "DELETE FROM node_health WHERE node_id IN ({placeholders})
             AND NOT EXISTS (SELECT 1 FROM node WHERE node.id = node_health.node_id);"
        );
        let mut stmt = conn.prepare(&sql)?;
        let swept = stmt.execute(params_from_iter(ids))?;
        Ok(swept)
    }

    /// Sweep `allocation_health` rows orphaned by allocation removal
    /// (plan §7-C2, C-2): the `allocation_health.allocation_id` FK cascade
    /// is gone — scheduler-lane allocation deletes must not carry
    /// status-lane health rows — so removal paths leave the health row
    /// behind. Orphans are harmless while they exist (every reader joins
    /// FROM `allocation`); this periodic status-lane sweep collects them,
    /// mirroring [`Self::prune_node_health_orphans`] — including its
    /// two-sweep observation grace (the local scheduler-lane replica this
    /// sweep judges orphanhood against can lag the same way).
    #[instrument(err, skip_all)]
    pub fn prune_allocation_health_orphans(&self) -> R<usize> {
        let orphans: Vec<i64> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(
                "SELECT allocation_health.allocation_id FROM allocation_health
                 LEFT JOIN allocation ON allocation.id = allocation_health.allocation_id
                 WHERE allocation.id IS NULL;",
            )?;
            let orphans = Self::collect_rows(stmt.query_map([], |row| row.get(0))?)?;
            drop(stmt);
            orphans
        };
        let confirmed: Vec<i64> = {
            let mut candidates = self
                .allocation_health_orphan_candidates
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let current: HashSet<i64> = orphans.into_iter().collect();
            let confirmed = current
                .iter()
                .filter(|id| candidates.contains(*id))
                .copied()
                .collect();
            *candidates = current;
            confirmed
        };
        if confirmed.is_empty() {
            return Ok(0);
        }
        self.replicated(Lane::Status, "prune_allocation_health_orphans", move |s| {
            s.prune_allocation_health_orphans_apply(&confirmed)
        })
    }

    fn prune_allocation_health_orphans_apply(&self, ids: &[i64]) -> R<usize> {
        let conn = self.write_connection()?;
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        // Re-check orphanhood inside the write so an allocation id reused
        // between the read and this apply keeps its fresh health row.
        let sql = format!(
            "DELETE FROM allocation_health WHERE allocation_id IN ({placeholders})
             AND NOT EXISTS (
                 SELECT 1 FROM allocation
                 WHERE allocation.id = allocation_health.allocation_id
             );"
        );
        let mut stmt = conn.prepare(&sql)?;
        let swept = stmt.execute(params_from_iter(ids))?;
        Ok(swept)
    }

    #[instrument(err, skip_all)]
    pub fn update_node_dyamic(&self, id: &str, dynamic_diff: &Map<String, Value>) -> R<Node> {
        let id = id.to_owned();
        let dynamic_diff = dynamic_diff.clone();
        self.replicated(Lane::Control, "update_node_dyamic", move |s| {
            s.update_node_dyamic_apply(&id, &dynamic_diff)
        })
    }

    fn update_node_dyamic_apply(&self, id: &str, dynamic_diff: &Map<String, Value>) -> R<Node> {
        let node = self.get_node(id)?;
        let default_map = Map::new();

        let mut res = node
            .dynamic_attrs
            .as_object()
            .unwrap_or(&default_map)
            .clone();

        res.extend(dynamic_diff.clone());
        let res_as_string = to_string(&res).context("failed to serialize dynamic attrs")?;

        let conn = self.write_connection()?;

        conn.execute(
            "UPDATE node SET dynamic_attrs = :dynamic_attrs WHERE id = :id;",
            named_params! {
                ":id": id,
                ":dynamic_attrs": res_as_string,
            },
        )?;

        // Hydrate the joined row on the write connection so the update just
        // made is visible even inside a capture savepoint.
        let mut node = conn.query_row(&GET_NODE, [id], |row| self.node_from_row(row))?;
        let dynamic_attrs = Value::Object(res);
        node.dynamic_attrs = dynamic_attrs.clone();

        self.cache_node_dynamic_attrs(&node.id, dynamic_attrs)?;

        Ok(node)
    }

    /// Record one worker heartbeat. Concurrent callers GROUP-COMMIT
    /// (heartbeat-scale plan §2): heartbeats queued while a commit is in
    /// flight batch into one status-lane replicated op — one raft
    /// propose/quorum round stamping every batched node — via the same
    /// leader-elect coalescer the plan-apply path uses. A lone heartbeat
    /// commits exactly as before.
    ///
    /// Forwarding (plan §8, D2) is preserved: a heartbeat is new
    /// information no reconciler can re-derive, and workers dial ONE
    /// manager forever — a standing rejection here marks healthy nodes
    /// lost within seconds and self-fences their stateful allocations.
    /// When this node does not lead the status lane the write is forwarded
    /// to the lane leader; the worker's RPC surface is unchanged.
    #[instrument(err, skip_all)]
    pub fn update_node_heartbeat(&self, id: String) -> R<()> {
        let coalescer = Arc::clone(&self.heartbeat_coalescer);
        coalescer.submit(id, |batch| self.run_heartbeat_batch(batch))
    }

    /// Batch leader body for heartbeat group commit: dedupe the drained
    /// node ids and stamp them all in one replicated op. Per-id verdicts
    /// are preserved: an unregistered node's heartbeat still errors
    /// `NotFound` for exactly that caller (a ghost worker must not think
    /// it is healthy — its self-fence clock feeds off heartbeat success),
    /// while registered batch-mates succeed. On a typed `NotRaftLeader`
    /// rejection every id forwards individually through the existing
    /// single-heartbeat forward (wire-compatible with managers that
    /// predate the batched op).
    fn run_heartbeat_batch(&self, batch: Vec<String>) -> Vec<R<()>> {
        if batch.len() == 1 {
            let id = batch.into_iter().next().expect("length checked");
            return vec![self.update_node_heartbeat_forwarding(id)];
        }
        let unique: Vec<String> = batch
            .iter()
            .cloned()
            .collect::<BTreeSet<String>>()
            .into_iter()
            .collect();
        let ids = unique.clone();
        let shared: Result<BTreeSet<String>, String> =
            match self.replicated(Lane::Status, "update_node_heartbeats", move |s| {
                s.update_node_heartbeats_apply(&ids)
            }) {
                Ok(ghosts) => Ok(ghosts),
                Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => {
                    // Leadership moved: forward per id so the on-wire op stays
                    // the one every manager version understands.
                    let mut ghosts = BTreeSet::new();
                    let mut failures: Vec<String> = Vec::new();
                    for id in &unique {
                        match self.update_node_heartbeat_forwarding(id.clone()) {
                            Ok(()) => {}
                            Err(err)
                                if matches!(
                                    err.downcast_ref::<StorageError>(),
                                    Some(StorageError::NotFound { .. })
                                ) =>
                            {
                                ghosts.insert(id.clone());
                            }
                            Err(err) => failures.push(format!("{id}: {err:#}")),
                        }
                    }
                    if failures.is_empty() {
                        Ok(ghosts)
                    } else {
                        Err(format!(
                            "forwarded heartbeat batch had failures: {}",
                            failures.join("; ")
                        ))
                    }
                }
                Err(err) => Err(format!("{err:#}")),
            };
        batch
            .iter()
            .map(|id| match &shared {
                Ok(ghosts) if ghosts.contains(id) => Err(anyhow::Error::new(
                    StorageError::not_found("node", id.clone()),
                )),
                Ok(_) => Ok(()),
                Err(message) => Err(anyhow!(message.clone())),
            })
            .collect()
    }

    /// The pre-batching single-heartbeat op, with leader forwarding.
    fn update_node_heartbeat_forwarding(&self, id: String) -> R<()> {
        match self.update_node_heartbeat_local(id.clone()) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::NodeHeartbeat { id },
                    err,
                ),
            other => other,
        }
    }

    /// The non-forwarding variant, executed locally and by the
    /// `ForwardLaneWrite` executor (never re-forwards — rule 2 of
    /// `raft::forward`).
    pub(crate) fn update_node_heartbeat_local(&self, id: String) -> R<()> {
        let ghost = {
            let id = id.clone();
            self.replicated(Lane::Status, "update_node_heartbeat", move |s| {
                s.update_node_heartbeats_apply(std::slice::from_ref(&id))
            })?
        };
        if !ghost.is_empty() {
            return Err(anyhow::Error::new(StorageError::not_found("node", id)));
        }
        Ok(())
    }

    /// Stamp one heartbeat timestamp for a batch of nodes, and reproject
    /// only the nodes whose liveness status actually TRANSITIONED. Returns
    /// the ids that are not registered nodes (whose heartbeat is a no-op
    /// the caller reports as `NotFound`, exactly as the single op did).
    ///
    /// Heartbeats upsert node_health (plan §7-C2, C-1): the first
    /// heartbeat after a registration whose seed was lost creates the row;
    /// selecting FROM node keeps heartbeats from unregistered nodes a
    /// no-op, like the pre-split UPDATE. Both timestamp columns are
    /// written: RFC3339 text (the human/API representation) and the unix
    /// integer the liveness tick compares (heartbeat-scale plan §4).
    ///
    /// INVARIANT for future readers (heartbeat-scale plan §3): a heartbeat
    /// that only refreshes the timestamp of an already-active node does
    /// NOT re-read or reproject the cached `Node` — the scheduler read
    /// model does not consume `last_heartbeat`, and every API/liveness
    /// reader gets it from the database. Anything that starts consuming
    /// `last_heartbeat` from the cached read model must remove this skip
    /// (and pay the read-model write lock per heartbeat that it implies).
    /// Status transitions (lost/missing -> active) always reproject.
    fn update_node_heartbeats_apply(&self, ids: &[String]) -> R<BTreeSet<String>> {
        let now = OffsetDateTime::now_utc();
        let timestamp_now = now.format(&Rfc3339)?;
        let unix_now = now.unix_timestamp();

        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        // Pre-read (on the write connection, so an enclosing capture's own
        // earlier writes are visible): which of these ids exist, and which
        // are not currently active (those transition below and must
        // reproject).
        let mut present: BTreeSet<String> = BTreeSet::new();
        let mut transitioning: BTreeSet<String> = BTreeSet::new();
        {
            let conn = self.write_connection()?;
            let sql = format!(
                "SELECT node.id, COALESCE(node_health.status, '') FROM node
                 LEFT JOIN node_health ON node_health.node_id = node.id
                 WHERE node.id IN ({placeholders})"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows: Vec<(String, String)> =
                Self::collect_rows(stmt.query_map(params_from_iter(ids.iter()), |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?)?;
            for (id, status) in rows {
                if status != "active" {
                    transitioning.insert(id.clone());
                }
                present.insert(id);
            }
        }
        let ghosts: BTreeSet<String> = ids
            .iter()
            .filter(|id| !present.contains(*id))
            .cloned()
            .collect();
        // Also reproject any present node whose CACHED view is not
        // available: a concurrent mark-lost can land its cache update after
        // this write (that interleaving pre-dates batching), and the
        // DB-status check alone would then skip forever while the cache
        // still says lost. Consulting the cache makes the next heartbeat
        // converge, exactly as the unconditional reprojection used to.
        if !capture::in_capture() {
            let guard = self.read_model_reader()?;
            for id in &present {
                match guard.nodes.get(id) {
                    Some(node) if node.availability == "available" => {}
                    _ => {
                        transitioning.insert(id.clone());
                    }
                }
            }
        }

        {
            let conn = self.write_connection()?;
            let sql = format!(
                "INSERT INTO node_health (node_id, status, last_heartbeat, last_heartbeat_unix, lost_at, health_checks)
                 SELECT id, 'active', ?1, ?2, NULL, '{{}}' FROM node WHERE id IN ({placeholders})
                 ON CONFLICT(node_id) DO UPDATE SET
                     last_heartbeat = excluded.last_heartbeat,
                     last_heartbeat_unix = excluded.last_heartbeat_unix,
                     status = 'active', lost_at = NULL;"
            );
            let mut stmt = conn.prepare(&sql)?;
            let params = rusqlite::params_from_iter(
                [
                    &timestamp_now as &dyn rusqlite::types::ToSql,
                    &unix_now as &dyn rusqlite::types::ToSql,
                ]
                .into_iter()
                .chain(ids.iter().map(|id| id as &dyn rusqlite::types::ToSql)),
            );
            stmt.execute(params)?;
        }
        debug!(
            heartbeats = ids.len(),
            transitions = transitioning.len(),
            ghosts = ghosts.len(),
            "Stamped node heartbeats"
        );

        for id in &transitioning {
            // A node removed between the pre-read and here is treated the
            // way the projection path treats it: dropped from the cache.
            match self.get_node(id) {
                Ok(node) => self.cache_node(node)?,
                Err(err)
                    if matches!(
                        err.downcast_ref::<StorageError>(),
                        Some(StorageError::NotFound { .. })
                    ) =>
                {
                    self.cache_remove_node(id)?;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(ghosts)
    }

    #[instrument(err, skip_all)]
    pub fn update_node_health_checks(&self, id: &str, health_checks: &str) -> R<()> {
        match self.update_node_health_checks_local(id, health_checks) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::NodeHealthChecks {
                        id: id.to_owned(),
                        health_checks: health_checks.to_owned(),
                    },
                    err,
                ),
            other => other,
        }
    }

    pub(crate) fn update_node_health_checks_local(&self, id: &str, health_checks: &str) -> R<()> {
        let id = id.to_owned();
        let health_checks = health_checks.to_owned();
        self.replicated(Lane::Status, "update_node_health_checks", move |s| {
            s.update_node_health_checks_apply(&id, &health_checks)
        })
    }

    fn update_node_health_checks_apply(&self, id: &str, health_checks: &str) -> R<()> {
        let conn = self.write_connection()?;
        // See update_node_heartbeat_apply: liveness upserts into
        // node_health; the insert path's other columns are the hydration
        // defaults, so creating the row changes nothing else observable.
        let sql = r#"
        INSERT INTO node_health (node_id, status, last_heartbeat, lost_at, health_checks)
        SELECT :id, 'active', '', NULL, :health_checks
        WHERE EXISTS (SELECT 1 FROM node WHERE id = :id)
        ON CONFLICT(node_id) DO UPDATE SET health_checks = excluded.health_checks;
"#;

        let params = named_params! {
            ":id": id,
            ":health_checks": health_checks,
        };

        conn.execute(sql, params)?;

        drop(conn);
        let node = self.get_node(id)?;
        self.cache_node(node)?;

        Ok(())
    }

    //
    // WORKER TYPES
    //

    #[instrument(err, skip_all)]
    pub fn get_worker_type(&self, name: String) -> R<WorkerConfig> {
        let conn = self.read_connection()?;
        let config = conn
            .query_row(&GET_WORKER_TYPE, [name.clone()], |row| {
                self.worker_type_from_row(row)
            })
            .map_err(|error| match error {
                RusqliteError::QueryReturnedNoRows => {
                    anyhow::Error::new(StorageError::not_found("worker type", name))
                }
                other => anyhow::Error::from(other),
            })?;

        Ok(config)
    }

    #[instrument(err, skip_all)]
    pub fn insert_worker_type(&self, config: &WorkerConfig) -> R<WorkerConfig> {
        let config = config.clone();
        self.replicated(Lane::Control, "insert_worker_type", move |s| {
            s.insert_worker_type_apply(&config)
        })
    }

    fn insert_worker_type_apply(&self, config: &WorkerConfig) -> R<WorkerConfig> {
        let conn = self.write_connection()?;
        let node_checks_to_save = config.additional_node_checks.join(",");
        let attrs_to_save = serde_json::to_string(&config.attributes)?;
        let resources_to_save = if config.resources.is_empty() {
            "{}".to_string()
        } else {
            config.resources.clone()
        };
        let reserved_resources_to_save = if config.reserved_resources.is_empty() {
            "{}".to_string()
        } else {
            config.reserved_resources.clone()
        };

        let params = named_params! {
            ":name": config.name,
            ":pool": config.pool,
            ":additional_node_checks": node_checks_to_save,
            ":attributes": attrs_to_save,
            ":name_prefix": config.name_prefix,
            ":resources": resources_to_save,
            ":reserved_resources": reserved_resources_to_save,
            ":max_allocations": config.max_allocations,
            ":artifacts": config.artifacts,
            ":host_volumes": config.host_volumes,
            ":heartbeat_seconds": config.heartbeat_seconds,
            ":resource_enforcement": config.resource_enforcement,
        };

        let config = conn.query_row(&INSERT_WORKER_TYPE, params, |row| {
            self.worker_type_from_row(row)
        })?;

        Ok(config)
    }

    //
    // JOBS
    //

    #[instrument(err, skip_all)]
    pub fn get_job(&self, name: &str) -> R<Job> {
        let conn = self.read_connection()?;
        let job = conn
            .query_row(&GET_JOB, [name], |row| self.job_from_row(row))
            .map_err(|error| match error {
                RusqliteError::QueryReturnedNoRows => {
                    anyhow::Error::new(StorageError::not_found("job", name))
                }
                other => anyhow::Error::from(other),
            })?;
        Ok(job)
    }

    pub fn get_jobs(&self) -> R<Vec<Job>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&GET_JOBS)?;
        let jobs = Self::collect_rows(stmt.query_map([], |row| self.job_from_row(row))?)?;

        Ok(jobs)
    }

    #[instrument(err, skip_all)]
    pub fn stop_job(&self, name: &str) -> R<()> {
        let name = name.to_owned();
        self.replicated(Lane::Control, "stop_job", move |s| s.stop_job_apply(&name))
    }

    fn stop_job_apply(&self, name: &str) -> R<()> {
        let conn = self.write_connection()?;
        // The mtime moves with the status in one statement so the two can
        // never disagree, and so it only moves on rows this write touched.
        let sql = r#"
        UPDATE job
        SET status = 'stopped', updated_at = ?2
        WHERE name = ?1;
"#;
        conn.execute(sql, params![name, Self::now_unix()])?;
        drop(conn);

        // TODO: Create a job_update record
        // that kicks off the eval?
        // ... or maybe just this adds the eval straight up?

        self.events().job_stopped(name);

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn mark_job_failed(&self, job_id: &str) -> R<()> {
        match self.mark_job_failed_local(job_id) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::MarkJobFailed {
                        job_id: job_id.to_owned(),
                    },
                    err,
                ),
            other => other,
        }
    }

    pub(crate) fn mark_job_failed_local(&self, job_id: &str) -> R<()> {
        let job_id = job_id.to_owned();
        self.replicated(Lane::Control, "mark_job_failed", move |s| {
            s.mark_job_failed_apply(&job_id)
        })
    }

    fn mark_job_failed_apply(&self, job_id: &str) -> R<()> {
        let conn = self.write_connection()?;
        let sql = r#"
        UPDATE job
        SET status = 'failed', updated_at = ?2
        WHERE id = ?1;
"#;
        conn.execute(sql, params![job_id, Self::now_unix()])?;
        drop(conn);

        self.events().job_failed(job_id);

        Ok(())
    }

    /// Mark a job failed because a required KV dependency vanished
    /// (`onDelete: fail`). The job status becomes `failed` and the impacted
    /// allocations' status is stamped `failed:<reason>` so the diagnostic is
    /// visible in `job list`, the ledger, and state-store queries.
    ///
    /// Scope: when `impacted_tasks` is `Some`, only allocations whose task
    /// config is one of those (by name, across all of the job's config
    /// versions) are stamped — a missing injection key impacts just the tasks
    /// that depend on it, leaving sibling tasks' allocations untouched. When
    /// `None` (a legacy `$key` reference, which is not scoped to one task and
    /// makes the whole config unrenderable), every allocation of the job is
    /// stamped.
    ///
    /// The allocation rows are retained (the workload is not torn down and no
    /// reschedule storm starts); a later KV put that triggers a restart or
    /// rollout recovers the job (see `docs/kv-store.md`).
    #[instrument(err, skip(self))]
    pub fn mark_job_failed_for_kv(
        &self,
        job_id: &str,
        reason: &str,
        impacted_tasks: Option<&[String]>,
    ) -> R<()> {
        // Composite across the lane boundary (plan §7-C2, C-2): the job
        // status flip is control-lane, the allocation status stamps are
        // scheduler-lane, so they are two single-lane replicated ops (the
        // JC-30 composite shape) instead of one cross-lane transaction.
        let job_id_owned = job_id.to_owned();
        self.replicated(Lane::Control, "mark_job_failed_for_kv", move |s| {
            s.mark_job_failed_for_kv_apply(&job_id_owned)
        })?;
        let job_id = job_id.to_owned();
        let reason = reason.to_owned();
        let impacted_tasks = impacted_tasks.map(<[String]>::to_vec);
        self.replicated(
            Lane::Scheduler,
            "stamp_allocations_failed_for_kv",
            move |s| {
                s.stamp_allocations_failed_for_kv_apply(&job_id, &reason, impacted_tasks.as_deref())
            },
        )
    }

    fn mark_job_failed_for_kv_apply(&self, job_id: &str) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute(
            "UPDATE job SET status = 'failed', updated_at = ?2 WHERE id = ?1;",
            params![job_id, Self::now_unix()],
        )?;
        Ok(())
    }

    fn stamp_allocations_failed_for_kv_apply(
        &self,
        job_id: &str,
        reason: &str,
        impacted_tasks: Option<&[String]>,
    ) -> R<()> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let stamp = format!("failed:{reason}");
        match impacted_tasks {
            Some(tasks) if !tasks.is_empty() => {
                // Build an IN (...) list of task-config ids for the named tasks
                // across every config version of the job. rusqlite has no array
                // binding, so bind the names positionally.
                let placeholders = std::iter::repeat_n("?", tasks.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "UPDATE allocation SET status = ?1 \
                     WHERE task_config_id IN (\
                         SELECT id FROM task_config WHERE job_id = ?2 AND name IN ({placeholders})\
                     );"
                );
                let mut params: Vec<&dyn rusqlite::ToSql> = vec![&stamp, &job_id];
                for task in tasks {
                    params.push(task);
                }
                tx.execute(&sql, params.as_slice())?;
            }
            _ => {
                tx.execute(
                    "UPDATE allocation SET status = ?1 \
                     WHERE job_config_id IN (SELECT id FROM job_config WHERE job_id = ?2);",
                    (stamp.as_str(), job_id),
                )?;
            }
        }
        tx.commit()?;
        self.refresh_read_model()?;
        Ok(())
    }

    /// Look up a job's status by its job id (== name). Returns `None` when the
    /// job row is absent.
    pub fn job_status_by_id(&self, job_id: &str) -> R<Option<String>> {
        let conn = self.read_connection()?;
        let status: Option<String> = conn
            .query_row("SELECT status FROM job WHERE id = ?;", [job_id], |row| {
                row.get(0)
            })
            .optional()?;
        Ok(status)
    }

    /// Clear a `failed` job status (back to `done`) when a KV-triggered
    /// restart or rollout actively recovers the job. No-op when the job is
    /// not currently failed. Returns whether a row was updated.
    #[instrument(err, skip(self))]
    pub fn clear_job_failed_status(&self, job_id: &str) -> R<bool> {
        let job_id = job_id.to_owned();
        self.replicated(Lane::Control, "clear_job_failed_status", move |s| {
            s.clear_job_failed_status_apply(&job_id)
        })
    }

    fn clear_job_failed_status_apply(&self, job_id: &str) -> R<bool> {
        let conn = self.write_connection()?;
        let updated = conn.execute(
            "UPDATE job SET status = 'done', updated_at = ?2 \
             WHERE id = ?1 AND status = 'failed';",
            params![job_id, Self::now_unix()],
        )?;
        Ok(updated > 0)
    }

    /// Clear a `stopped` job status (back to `done`) when the job is
    /// resubmitted via RunJob. A resubmit revives the job, so the
    /// instance-management RPCs (CreateInstance/StartInstance), which reject
    /// stopped jobs, must accept it again. Stopped keyed instances
    /// deliberately stay `stopped` through this — reviving each one takes an
    /// explicit StartInstance. No-op when the job is not currently stopped.
    /// Returns whether a row was updated.
    #[instrument(err, skip(self))]
    pub fn clear_job_stopped_status(&self, job_id: &str) -> R<bool> {
        let job_id = job_id.to_owned();
        self.replicated(Lane::Control, "clear_job_stopped_status", move |s| {
            s.clear_job_stopped_status_apply(&job_id)
        })
    }

    fn clear_job_stopped_status_apply(&self, job_id: &str) -> R<bool> {
        let conn = self.write_connection()?;
        let updated = conn.execute(
            "UPDATE job SET status = 'done', updated_at = ?2 \
             WHERE id = ?1 AND status = 'stopped';",
            params![job_id, Self::now_unix()],
        )?;
        Ok(updated > 0)
    }

    /// Record REPLICATED restart intent for a job (JC-27 failover
    /// regeneration): a KV-triggered restart of an unchanged config is
    /// otherwise represented only by a leader-local `job_update` eval, which
    /// dies with the leader — the acknowledged restart would silently never
    /// happen. The marker survives failover in replicated state;
    /// [`Self::regenerate_scheduling_evals`] re-enqueues `job_update` for
    /// any job whose marker is still set, and the eval clears it (via
    /// [`Self::clear_job_restart_request`]) before executing the restart.
    #[instrument(err, skip(self))]
    pub fn request_job_restart(&self, job_id: &str) -> R<()> {
        let job_id = job_id.to_owned();
        self.replicated(Lane::Control, "request_job_restart", move |s| {
            let conn = s.write_connection()?;
            // An acknowledged restart is a change to the job, so it moves
            // the mtime; the marker's own clear (below) does not — that is
            // the eval consuming its own bookkeeping, not a user action.
            conn.execute(
                "UPDATE job SET restart_requested_at = ?2, updated_at = ?3 WHERE id = ?1;",
                params![job_id, Self::now_rfc3339()?, Self::now_unix()],
            )?;
            Ok(())
        })
    }

    /// Clear the replicated restart marker (see
    /// [`Self::request_job_restart`]). Idempotent: clearing an unset marker
    /// touches no rows, so in raft mode the empty capture skips the raft
    /// entry entirely (JC-26).
    #[instrument(err, skip(self))]
    pub fn clear_job_restart_request(&self, job_id: &str) -> R<()> {
        let job_id = job_id.to_owned();
        self.replicated(Lane::Control, "clear_job_restart_request", move |s| {
            let conn = s.write_connection()?;
            conn.execute(
                "UPDATE job SET restart_requested_at = NULL
                 WHERE id = ?1 AND restart_requested_at IS NOT NULL;",
                [job_id],
            )?;
            Ok(())
        })
    }

    /// Destroy a stopped job. Composite across all three lanes (plan §7-C2,
    /// C-2 — the Nomad deregister shape, executed inline): with the
    /// cross-lane FK cascades gone, each lane deletes its own tables in
    /// sequence — STATUS clears `task_status`/`allocation_health`,
    /// SCHEDULER clears `allocation`/`instance`/`rollout`/`rollout_event`,
    /// then CONTROL deletes the `job` row (cascading only control-lane
    /// children) and emits the event. Cross-op atomicity was never promised
    /// (§A2): a crash mid-sequence leaves a stopped job with some children
    /// already cleared, and re-running destroy finishes the remainder.
    #[instrument(err, skip_all)]
    pub fn destroy_job(&self, name: &str) -> R<()> {
        // Validate first (same checks the control op re-runs in its own
        // transaction) so a bad request deletes nothing from any lane.
        let job = {
            let conn = self.read_connection()?;
            conn.query_row(&GET_JOB, [name], |row| self.job_from_row(row))?
        };
        if job.status != "stopped" {
            bail!("Job must be stopped before it can be destroyed");
        }

        let job_id = job.id;
        let status_job_id = job_id.clone();
        self.replicated(Lane::Status, "destroy_job_status_cleanup", move |s| {
            s.destroy_job_status_cleanup_apply(&status_job_id)
        })?;
        let scheduler_job_id = job_id.clone();
        self.replicated(Lane::Scheduler, "destroy_job_scheduler_cleanup", move |s| {
            s.destroy_job_scheduler_cleanup_apply(&scheduler_job_id)
        })?;
        let name = name.to_owned();
        self.replicated(Lane::Control, "destroy_job", move |s| {
            s.destroy_job_apply(&name)
        })
    }

    /// Status-lane share of a job destroy: worker-reported state for the
    /// job's tasks and allocations.
    fn destroy_job_status_cleanup_apply(&self, job_id: &str) -> R<()> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        tx.execute(
            "DELETE FROM allocation_health WHERE allocation_id IN (
                 SELECT a.id FROM allocation a
                 INNER JOIN job_config jc ON jc.id = a.job_config_id
                 WHERE jc.job_id = ?1
             );",
            [job_id],
        )?;
        tx.execute("DELETE FROM task_status WHERE job_id = ?1;", [job_id])?;
        tx.commit()?;
        Ok(())
    }

    /// Scheduler-lane share of a job destroy: allocations (with epoch bumps
    /// so any queued plan for the config is CAS-rejected), instances
    /// (destroy is the one path that erases keyed instance rows), and the
    /// job's rollouts + rollout events.
    fn destroy_job_scheduler_cleanup_apply(&self, job_id: &str) -> R<()> {
        let allocs = self.allocs_for_job(job_id)?;
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        tx.execute(
            "DELETE FROM allocation WHERE job_config_id IN (
                 SELECT id FROM job_config WHERE job_id = ?1
             );",
            [job_id],
        )?;
        tx.execute("DELETE FROM instance WHERE job_id = ?1;", [job_id])?;
        tx.execute("DELETE FROM rollout_event WHERE job_id = ?1;", [job_id])?;
        tx.execute("DELETE FROM rollout WHERE job_id = ?1;", [job_id])?;
        let mut stopping_event = None;
        if !allocs.is_empty() {
            self.bump_epochs_for_allocations_tx(&tx, &allocs)?;
            stopping_event = self.record_stop_notices_tx(&tx, &allocs, StopCause::Destroy)?;
        }
        tx.commit()?;
        self.publish_staged(stopping_event);
        drop(conn);
        if !allocs.is_empty() {
            self.cache_remove_allocations(&allocs)?;
        }
        Ok(())
    }

    fn destroy_job_apply(&self, name: &str) -> R<()> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;

        let job = tx.query_row(&GET_JOB, [name], |row| self.job_from_row(row))?;
        if job.status != "stopped" {
            bail!("Job must be stopped before it can be destroyed");
        }

        let submission_hashes: Vec<String> = {
            let mut sub_stmt = tx.prepare(
                r#"
                SELECT DISTINCT jcs.submission_hash
                FROM job_config_submission jcs
                INNER JOIN job_config jc ON jc.id = jcs.job_config_id
                WHERE jc.job_id = ?;
            "#,
            )?;

            let rows = sub_stmt.query_map([job.id.clone()], |row| row.get(0))?;
            Self::collect_rows(rows)?
        };

        let delete_sql = r#"
            DELETE FROM job
            WHERE name = ? AND status = 'stopped';
        "#;

        tx.execute(delete_sql, [name])?;
        // Count overrides die with the job (control-owned, same capture).
        tx.execute("DELETE FROM task_scale WHERE job_id = ?;", [&job.id])?;

        for submission_hash in submission_hashes {
            let reference_count: i64 = tx.query_row(
                r#"
                SELECT COUNT(*)
                FROM job_config_submission
                WHERE submission_hash = ?;
            "#,
                [submission_hash.clone()],
                |row| row.get(0),
            )?;

            if reference_count == 0 {
                tx.execute("DELETE FROM submission WHERE hash = ?;", [submission_hash])?;
            }
        }

        tx.commit()
            .map_err(|_| anyhow!("Error committing transaction"))?;
        drop(conn);

        self.events().job_destroyed(name);

        Ok(())
    }

    fn rollout_meta_from_value(
        meta: Value,
    ) -> Result<(Option<RolloutConfig>, BTreeMap<String, RolloutConfig>), RusqliteError> {
        let Some(meta) = meta.as_object() else {
            return Ok((None, BTreeMap::new()));
        };

        let rollout = match meta.get("rollout") {
            Some(value) if !value.is_null() => Some(
                serde_json::from_value::<RolloutConfig>(value.clone()).map_err(|err| {
                    RusqliteError::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?,
            ),
            _ => None,
        };

        let rollout_by_group = match meta.get("rolloutByGroup") {
            Some(value) if !value.is_null() => serde_json::from_value::<
                BTreeMap<String, RolloutConfig>,
            >(value.clone())
            .map_err(|err| {
                RusqliteError::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?,
            _ => BTreeMap::new(),
        };

        Ok((rollout, rollout_by_group))
    }

    pub fn get_job_config_optional(&self, name: &str) -> R<Option<JobConfig>> {
        let conn = self.read_connection()?;
        let maybe_job_config: Option<(JobConfig, String)> = conn
            .query_row(&GET_JOB_CONFIG, [name], |row| {
                let tasks_to_replace = vec![];
                let job_config_id = row.get(0)?;
                let meta: Value = row.get(3)?;
                let (rollout, rollout_by_group) = Self::rollout_meta_from_value(meta)?;
                let jc = JobConfig {
                    name: row.get(1)?,
                    tasks: tasks_to_replace,
                    priority: Band::from_stored(&row.get::<_, String>(7)?),
                    rollout,
                    rollout_by_group,
                    version: Some(row.get(4)?),
                };

                Ok((jc, job_config_id))
            })
            .optional()?;

        let Some((job_config, jc_id)) = maybe_job_config else {
            return Ok(None);
        };

        let mut stmt = conn.prepare(&GET_TASK_FOR_JOB_CONIFG)?;
        let tasks =
            Self::collect_rows(stmt.query_map([jc_id], |row| self.task_config_from_row(row))?)?;

        let jc_2 = JobConfig {
            tasks,
            ..job_config
        };

        Ok(Some(jc_2))
    }

    pub fn get_job_config_by_id_optional(&self, id: &str) -> R<Option<JobConfig>> {
        let conn = self.read_connection()?;
        let maybe_job_config: Option<(JobConfig, String)> = conn
            .query_row(&GET_JOB_CONFIG_BY_ID, [id], |row| {
                let tasks_to_replace = vec![];
                let job_config_id = row.get(0)?;
                let meta: Value = row.get(3)?;
                let (rollout, rollout_by_group) = Self::rollout_meta_from_value(meta)?;
                let jc = JobConfig {
                    name: row.get(1)?,
                    tasks: tasks_to_replace,
                    priority: Band::from_stored(&row.get::<_, String>(7)?),
                    rollout,
                    rollout_by_group,
                    version: Some(row.get(4)?),
                };

                Ok((jc, job_config_id))
            })
            .optional()?;

        let Some((job_config, jc_id)) = maybe_job_config else {
            return Ok(None);
        };

        let mut stmt = conn.prepare(&GET_TASK_FOR_JOB_CONIFG)?;
        let tasks =
            Self::collect_rows(stmt.query_map([jc_id], |row| self.task_config_from_row(row))?)?;

        let jc_2 = JobConfig {
            tasks,
            ..job_config
        };

        Ok(Some(jc_2))
    }

    #[instrument(err, skip_all)]
    pub fn get_job_config(&self, name: &str) -> R<JobConfig> {
        self.get_job_config_optional(name)?
            .ok_or_else(|| StorageError::not_found("job config", name).into())
    }

    #[instrument(err, skip_all)]
    pub fn get_job_config_by_id(&self, id: &str) -> R<JobConfig> {
        self.get_job_config_by_id_optional(id)?
            .ok_or_else(|| StorageError::not_found("job config", id).into())
    }

    /// The owning job id for a job config id, if the config exists.
    #[instrument(err, skip_all)]
    pub fn job_id_for_job_config(&self, job_config_id: &str) -> R<Option<String>> {
        let conn = self.read_connection()?;
        let job_id = conn
            .query_row(
                "SELECT job_id FROM job_config WHERE id = ?;",
                [job_config_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(job_id)
    }

    pub fn list_job_spec_history(&self, name: &str) -> R<Vec<JobSpecHistoryRow>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT jc.id, jc.created_at, jc.version, jc.meta
            FROM job_config jc
            INNER JOIN job j ON j.id = jc.job_id
            WHERE j.name = ?
            ORDER BY jc.created_at DESC;
            "#,
        )?;

        let rows = stmt.query_map([name], |row| {
            Ok(JobSpecHistoryRow {
                config_id: row.get(0)?,
                created_at: row.get(1)?,
                version: row.get::<_, Option<i64>>(2)?,
                template: Self::template_provenance_from_meta(row.get::<_, Option<Value>>(3)?),
            })
        })?;

        Self::collect_rows(rows)
    }

    fn template_provenance_from_meta(meta: Option<Value>) -> Option<JobTemplateProvenance> {
        meta.as_ref()
            .and_then(|meta| meta.get("jobTemplate"))
            .and_then(|value| serde_json::from_value(value.clone()).ok())
    }

    /// Template provenance of a job's latest config, when it was submitted
    /// through RunJobTemplate.
    #[instrument(err, skip_all)]
    pub fn job_template_provenance(&self, job_name: &str) -> R<Option<JobTemplateProvenance>> {
        let conn = self.read_connection()?;
        let meta: Option<Value> = conn
            .query_row(
                r#"
                SELECT jc.meta
                FROM job_config jc
                INNER JOIN job j ON j.id = jc.job_id
                WHERE j.name = ?
                ORDER BY jc.version DESC
                LIMIT 1;
                "#,
                [job_name],
                |row| row.get(0),
            )
            .optional()?;
        Ok(Self::template_provenance_from_meta(meta))
    }

    /// Latest-config template provenance for every job, keyed by job name.
    /// Used to filter list replies for template-scoped identities without a
    /// per-row query.
    #[instrument(err, skip_all)]
    pub fn job_template_provenance_map(&self) -> R<HashMap<String, JobTemplateProvenance>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT j.name, jc.meta
            FROM job j
            INNER JOIN job_config jc ON jc.job_id = j.id
            WHERE jc.version = (
                SELECT MAX(version) FROM job_config WHERE job_id = j.id
            );
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                Self::template_provenance_from_meta(row.get::<_, Option<Value>>(1)?),
            ))
        })?;
        let mut map = HashMap::new();
        for row in rows {
            let (name, provenance) = row?;
            if let Some(provenance) = provenance {
                map.insert(name, provenance);
            }
        }
        Ok(map)
    }

    /// Submit a job config. Composite across the lane boundary (plan §7-C2,
    /// C-2): the CONTROL lane writes the job/config/submission tables, then
    /// the STATUS lane seeds the initial `task_status` rows (its own
    /// table's first write for the new config, `pending` until the first
    /// real status report upserts), then the SCHEDULER lane upserts the
    /// `volume` rows behind the tasks' `volumes[]` claims. All three run
    /// before this method returns, so task list/status views — and
    /// `legion volume list` — observe the same states as the pre-split
    /// single transaction; a crash between the ops leaves task rows whose
    /// missing status simply stays unreported until placement or the first
    /// status write (cross-op atomicity was never promised, §A2).
    ///
    /// **Standing-split tolerance (plan §8, D1):** the control half carries
    /// the operator's intent and MUST commit (a rejection there is the
    /// caller's typed leader-hint error, unchanged). The status seed and
    /// the volume-claim sync are idempotent projections of the committed
    /// control rows, so when this node does not lead those lanes (divergent
    /// lane leadership, JC-40 best-effort co-location) their typed
    /// `NotRaftLeader` rejection is tolerated: the submit succeeds, and the
    /// owning lane's leader completes the half via its reconciler
    /// ([`Self::seed_missing_task_statuses`] /
    /// [`Self::sync_missing_task_volume_claims`], run from the
    /// node-status-checker loop) once the control rows replicate to it.
    /// Every other error on those halves still fails the submit.
    #[allow(dead_code)]
    pub fn handle_job_submit(
        &self,
        config: &JobConfig,
        subs: &[Submission],
    ) -> R<JobSubmitOutcome> {
        self.handle_job_submit_inner(config, subs, None)
    }

    /// Like `handle_job_submit`, stamping the template that rendered this
    /// config into the config's meta for provenance (spec history, the
    /// RunJobTemplate collision guard, and template-scoped ACL reads).
    pub fn handle_job_submit_from_template(
        &self,
        config: &JobConfig,
        subs: &[Submission],
        template: &JobTemplateProvenance,
    ) -> R<JobSubmitOutcome> {
        self.handle_job_submit_inner(config, subs, Some(template))
    }

    fn handle_job_submit_inner(
        &self,
        config: &JobConfig,
        subs: &[Submission],
        template: Option<&JobTemplateProvenance>,
    ) -> R<JobSubmitOutcome> {
        let config = config.clone();
        let subs = subs.to_vec();
        let template = template.cloned();
        let outcome = self.replicated(Lane::Control, "handle_job_submit", move |s| {
            s.handle_job_submit_apply(&config, &subs, template.as_ref())
        })?;
        // The status half runs for EVERY outcome, `Unchanged` included
        // (JC-40 composite-retry idempotency): if a previous attempt's
        // control half committed and its status half was rejected — the
        // node did not lead the status lane yet while lane leadership was
        // converging — the client's retry re-submits the identical config
        // and gets `Unchanged` back. Skipping the seed there would leave the
        // composite permanently half-applied, with no later write to fix it
        // (the config is already stored, so no submit ever reports
        // Created/Updated for it again). Seeding is `ON CONFLICT DO
        // NOTHING`, so the repeat costs one no-op statement — and under raft
        // an empty changeset skips the raft entry entirely (JC-26), so a
        // steady-state resubmit does not even reach the log.
        let job_config_id = outcome.job_config_id().to_string();
        if let Err(err) = self.replicated(Lane::Status, "seed_task_statuses", move |s| {
            s.seed_task_statuses_apply(&job_config_id)
        }) {
            if !crate::storage::raft::driver::NotRaftLeader::in_chain(&err) {
                return Err(err);
            }
            warn!(
                job_config_id = outcome.job_config_id(),
                error = %err,
                "status-lane seed deferred: this node does not lead the status lane; \
                 the status leader's reconciler will seed the task rows"
            );
        }
        // Scheduler half: the `volume` rows behind the tasks' `volumes[]`
        // claims. `volume` is scheduler-owned (its lifecycle is bound to the
        // scheduler-owned `instance`, and the reclaim reaper drives its state
        // machine), so writing it from the control capture above is exactly
        // the cross-lane contamination the capture-time guard rejects — job
        // submit would fail outright in raft mode. It runs here instead, as
        // its own sequential op (JC-33's wrapper-sequential shape), reading
        // the task rows the control half just COMMITTED (a capture's reads see
        // committed state only, JC-35 — composing it into the control capture
        // could not have seen them anyway).
        //
        // Behavior main intended is preserved: the claims exist from submit,
        // before anything is placed, so `legion volume list` shows what a job
        // asked for; and the upsert refreshes driver/params/mode without
        // disturbing a live volume's node pin or state.
        //
        // Runs for EVERY outcome, `Unchanged` included, for the same
        // composite-retry reason as the status seed above (JC-40): a retry
        // after a rejected scheduler half re-submits the identical config and
        // reports `Unchanged`, and skipping the sync there would leave the
        // composite half-applied forever. If this op fails, the control half
        // stays committed and uncorrupted — the job simply has no claim rows
        // yet — and a retry converges because the sync is idempotent by
        // construction: a claim id is a pure function of
        // `(job, group, task, name)` (`claim_volume_id`), so a re-run lands on
        // the same rows and the `ON CONFLICT DO UPDATE` writes the same
        // driver/params/read_only/volume_mode values it would have written the
        // first time (only `updated_at` advances). Placement re-derives its
        // mounts from this table, so a claim that materializes late is still
        // in place before any allocation of the task can mount it.
        let job_config_id = outcome.job_config_id().to_string();
        if let Err(err) = self.replicated(Lane::Scheduler, "sync_task_volume_claims", move |s| {
            s.sync_task_volume_claims_apply(&job_config_id)
        }) {
            if !crate::storage::raft::driver::NotRaftLeader::in_chain(&err) {
                return Err(err);
            }
            warn!(
                job_config_id = outcome.job_config_id(),
                error = %err,
                "scheduler-lane volume-claim sync deferred: this node does not lead the \
                 scheduler lane; the scheduler leader's reconciler will sync the claims"
            );
        }
        Ok(outcome)
    }

    /// Status-lane seed of the submit composite: one `pending` row per task
    /// config of the just-written job config (task identity comes from the
    /// control-lane rows, read here).
    fn seed_task_statuses_apply(&self, job_config_id: &str) -> R<()> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        conn.execute(
            "INSERT INTO task_status (task_config_id, job_id, job_config_id, status, updated_at)
             SELECT id, job_id, job_config_id, ?2, ?3
             FROM task_config WHERE job_config_id = ?1
             ON CONFLICT(task_config_id) DO NOTHING;",
            params![job_config_id, TaskStatusState::Pending.as_str(), now],
        )?;
        Ok(())
    }

    /// Status-lane reconciler for submit seeds deferred under divergent lane
    /// leadership (plan §8, D1): seed a `pending` `task_status` row for
    /// every task of every active job's LATEST config that is missing one.
    /// The submit composite tolerates a status-lane rejection of its seed
    /// half ([`Self::handle_job_submit`]); this pass, run by the
    /// node-status-checker loop on the status-lane leader, completes those
    /// halves once the control rows have replicated here. Idempotent and
    /// empty in steady state — zero missing rows means an empty changeset,
    /// which never reaches the raft log (JC-26). Returns how many rows were
    /// seeded.
    ///
    /// Scoped to each job's latest config deliberately: a superseded config
    /// that never got its seed is dead — placement and status reporting only
    /// ever act on the latest — so seeding it would create junk `pending`
    /// rows no view wants.
    pub fn seed_missing_task_statuses(&self) -> R<u64> {
        // Cheap read-side probe first, so the steady state costs one query
        // and no replicated op at all.
        let missing: i64 = {
            let conn = self.read_connection()?;
            conn.query_row(
                &format!(
                    "SELECT COUNT(*) FROM task_config tc
                     INNER JOIN job j ON j.id = tc.job_id
                     WHERE j.status NOT IN ('stopped', 'failed')
                       AND tc.job_config_id = {LATEST_CONFIG_FOR_JOB}
                       AND NOT EXISTS (
                         SELECT 1 FROM task_status ts WHERE ts.task_config_id = tc.id
                       );"
                ),
                [],
                |row| row.get(0),
            )?
        };
        if missing == 0 {
            return Ok(0);
        }
        self.replicated(Lane::Status, "seed_missing_task_statuses", move |s| {
            let conn = s.write_connection()?;
            let seeded = conn.execute(
                &format!(
                    "INSERT INTO task_status
                         (task_config_id, job_id, job_config_id, status, updated_at)
                     SELECT tc.id, tc.job_id, tc.job_config_id, ?1, ?2
                     FROM task_config tc
                     INNER JOIN job j ON j.id = tc.job_id
                     WHERE j.status NOT IN ('stopped', 'failed')
                       AND tc.job_config_id = {LATEST_CONFIG_FOR_JOB}
                       AND NOT EXISTS (
                         SELECT 1 FROM task_status ts WHERE ts.task_config_id = tc.id
                       )
                     ON CONFLICT(task_config_id) DO NOTHING;"
                ),
                params![TaskStatusState::Pending.as_str(), Self::now_rfc3339()?],
            )?;
            Ok(seeded as u64)
        })
    }

    fn handle_job_submit_apply(
        &self,
        config: &JobConfig,
        subs: &[Submission],
        template: Option<&JobTemplateProvenance>,
    ) -> R<JobSubmitOutcome> {
        let config = self.normalize_job_config(config)?;
        // Note: There is a race condition here with the version
        // I think this should prob be all in a single lock, but
        // I am not going to worry about this for now

        let mut next_version = 1;
        let mut last_versions_id: Option<String> = None;
        let mut was_update = false;

        if let Some(latest_job_config) = self.get_job_config_optional(&config.name)? {
            // if latest_job_config is deep equal to config, return unchanged
            if latest_job_config.is_same_as(&config) {
                return Ok(JobSubmitOutcome::Unchanged {
                    job_config_id: latest_job_config.id(),
                });
            } else {
                next_version = latest_job_config.version.unwrap_or(0) + 1;
                last_versions_id = Some(latest_job_config.id());
                was_update = true;
            }
        }

        let mut conn = self.write_connection()?;
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let tx = Self::write_tx(&mut conn)?;

        let get_job_res = tx.query_row(&GET_JOB, [config.name.clone()], |row| {
            self.job_from_row(row)
        });

        let job = match get_job_res {
            Ok(j) => Ok(j),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                let job_params = named_params! {
                    ":id": config.name,
                    ":name": config.name,
                    ":status": "done",
                    ":created_at": now,
                    ":updated_at": now,
                };

                tx.query_row(&INSERT_JOB, job_params, |row| self.job_from_row(row))
            }
            Err(error) => Err(error),
        }?;

        // A new spec version is a change to the job, so the existing row's
        // mtime moves with it. Skipped on the insert branch, which already
        // stamped `updated_at` — and skipped entirely when the config is
        // unchanged, because that path returned `Unchanged` above.
        tx.execute(
            "UPDATE job SET updated_at = ?2 WHERE id = ?1;",
            params![job.id, now],
        )?;

        let now = time::OffsetDateTime::now_utc();
        let mut meta = if config.rollout.is_none() && config.rollout_by_group.is_empty() {
            json!({})
        } else {
            json!({
                "rollout": config.rollout,
                "rolloutByGroup": config.rollout_by_group,
            })
        };
        if let Some(template) = template {
            meta["jobTemplate"] = serde_json::to_value(template)?;
        }
        let config_params = named_params! {
            ":id": format!("{}-{}", config.name, next_version.to_string()),
            ":name": config.name,
            ":type": "default",
            ":meta": meta,
            ":version": next_version,
            ":created_at": now,
            ":job_id": job.id,
            ":priority": config.priority.as_str(),
        };

        let (_config, job_config_id): (JobConfig, String) =
            tx.query_row(&INSERT_JOB_CONFIG, config_params, |row| {
                let tasks = vec![];
                Ok((
                    JobConfig {
                        name: row.get(1)?,
                        priority: config.priority,
                        rollout: config.rollout.clone(),
                        rollout_by_group: config.rollout_by_group.clone(),
                        version: Some(row.get(4)?),
                        tasks,
                    },
                    row.get(0)?,
                ))
            })?;

        for t in &config.tasks {
            let checks = serde_json::to_string(&t.checks)?;
            let on_unhealthy = serde_json::to_string(&t.on_unhealthy)?;
            let depends_on = serde_json::to_string(&t.depends_on)?;
            let kv_injections = t
                .kv_injections
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            let network = t.network.as_ref().map(serde_json::to_string).transpose()?;
            let service = t.service.as_ref().map(serde_json::to_string).transpose()?;
            let connect = t.connect.as_ref().map(serde_json::to_string).transpose()?;
            let identities = if t.identities.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&t.identities)?)
            };
            let stateful = t.stateful.as_ref().map(serde_json::to_string).transpose()?;
            let artifacts = if t.artifacts.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&t.artifacts)?)
            };
            let actions = if t.actions.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&t.actions)?)
            };
            let volumes = if t.volumes.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&t.volumes)?)
            };
            let restart = t.restart.as_ref().map(serde_json::to_string).transpose()?;
            let stop = t.stop.as_ref().map(serde_json::to_string).transpose()?;
            let disruption = t
                .disruption
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            let schedule = t.schedule.as_ref().map(ToString::to_string);
            let task_params = named_params! {
                ":name": t.name,
                ":runtime": t.runtime,
                ":count": t.count.num,
                ":count_per": t.count.per,
                ":group_name": t.group,
                ":lifecycle": t.lifecycle.to_string(),
                ":schedule": schedule,
                ":depends_on": depends_on,
                ":config": t.config,
                ":resources": t.resources,
                ":constraints": t.constraints,
                ":permit": t.permit,
                ":affinities": t.affinities,
                ":checks": checks,
                ":on_unhealthy": on_unhealthy,
                ":kv_injections": kv_injections,
                ":job_id": job.id,
                ":job_config_id": job_config_id,
                ":identities": identities,
                ":stateful": stateful,
                ":artifacts": artifacts,
                ":network": network,
                ":service": service,
                ":actions": actions,
                ":volumes": volumes,
                ":connect": connect,
                ":restart": restart,
                ":stop": stop,
                ":disruption": disruption,
            };

            let task_id: i64 = tx.query_row(&INSERT_TASK, task_params, |row| row.get(0))?;
            // The `volume` claim rows behind `t.volumes` are written by the
            // SCHEDULER-lane half of this composite (`sync_task_volume_claims`,
            // plan §7-C2 / JC-33) — `volume` is scheduler-owned and must not
            // ride this control-lane changeset.
            let _ = task_id;
        }

        for s in subs {
            let sub_hash = s.hash();

            let sub_params = named_params! {
                ":content": s.content,
                ":hash": sub_hash,
            };

            tx.execute(&INSERT_SUBMISSION, sub_params)?;

            let job_config_sub_params = named_params! {
                ":path": s.path,
                ":is_primary": s.is_primary,
                ":job_config_id": job_config_id,
                ":submission_hash": sub_hash,
            };

            tx.execute(&INSERT_JOB_CONFIG_SUBMISSION, job_config_sub_params)?;
        }

        let job_update_params = named_params! {
            ":id": format!("{}-{}", job.id, next_version.to_string()),
            ":status": "active",
            ":job_id": job.id,
            ":to_job_config_id": job_config_id,
            ":from_job_config_id": last_versions_id.clone(),
        };

        tx.execute(&INSERT_JOB_UPDATE, job_update_params)?;

        // Count-override ownership arbitration (docs/autoscaling.md): an
        // override records the spec count it was laid over (`base_count`).
        // A resubmit whose spec count CHANGED expresses new intent — the
        // override (and any pending scale-in) is cleared and the spec
        // wins; an unchanged spec count leaves the override in place, so
        // "my scale sticks across deploys" and "editing the spec does
        // what it says" are both true. Overrides for tasks the new
        // version no longer declares are orphans and cleared too.
        // `task_scale` is control-owned precisely so this rides the same
        // capture as the config write.
        let mut scale_reset_events = Vec::new();
        for t in &config.tasks {
            let existing: Option<i64> = tx
                .query_row(
                    "SELECT base_count FROM task_scale \
                     WHERE job_id = ?1 AND task_group = ?2 AND task_name = ?3;",
                    params![job.id, t.group, t.name],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(base_count) = existing {
                if i64::from(t.count.num) != base_count {
                    tx.execute(
                        "DELETE FROM task_scale \
                         WHERE job_id = ?1 AND task_group = ?2 AND task_name = ?3;",
                        params![job.id, t.group, t.name],
                    )?;
                    scale_reset_events.push(drafts::scale_reset(
                        &job.id,
                        &t.group,
                        &t.name,
                        "resubmit changed the spec count; the spec owns the count again",
                    ));
                }
            }
        }
        tx.execute(
            "DELETE FROM task_scale WHERE job_id = ?1 AND NOT EXISTS (\
                 SELECT 1 FROM task_config \
                 WHERE task_config.job_config_id = ?2 \
                 AND task_config.group_name = task_scale.task_group \
                 AND task_config.name = task_scale.task_name);",
            params![job.id, job_config_id],
        )?;

        // Initial task_status rows are seeded by the status-lane half of
        // the submit composite (`seed_task_statuses`, plan §7-C2, C-2) —
        // task_status is status-owned and must not ride this control-lane
        // changeset.

        let outcome = if was_update {
            JobSubmitOutcome::Updated { job_config_id }
        } else {
            JobSubmitOutcome::Created { job_config_id }
        };
        let submit_event =
            drafts::job_submit_outcome(&job.id, &outcome, next_version, config.tasks.len())
                .and_then(|draft| self.stage_event_locked(&tx, draft));
        let scale_reset_events: Vec<_> = scale_reset_events
            .into_iter()
            .filter_map(|draft| self.stage_event_locked(&tx, draft))
            .collect();
        tx.commit()
            .map_err(|_| anyhow!("Error committing transaction"))?;
        self.publish_staged(submit_event);
        for staged in scale_reset_events {
            self.publish_staged(Some(staged));
        }
        drop(conn);

        let cached_tasks = self.get_task_configs_for_job_config(outcome.job_config_id())?;
        self.cache_set_task_configs_for_job_config(outcome.job_config_id(), cached_tasks)?;

        Ok(outcome)
    }

    pub fn normalize_job_config(&self, config: &JobConfig) -> R<JobConfig> {
        let tasks = config
            .tasks
            .iter()
            .map(|task| {
                let resources = self
                    .resource_policy
                    .normalize_task_resources(&task.resources, true)?;
                let mut task = TaskConfig {
                    resources,
                    ..task.clone()
                };
                task.normalize_network()?;
                Ok(task)
            })
            .collect::<R<Vec<_>>>()?;

        Ok(JobConfig {
            tasks,
            ..config.clone()
        })
    }

    #[instrument(err, skip_all)]
    pub fn previous_job_config_id(&self, job_config_id: &str) -> R<Option<String>> {
        let conn = self.read_connection()?;
        let previous = conn
            .query_row(&GET_JOB_UPDATE_FOR_CONFIG, [job_config_id], |row| {
                row.get::<_, Option<String>>(4)
            })
            .optional()?;
        Ok(previous.flatten())
    }

    #[instrument(err, skip_all)]
    pub fn create_rollout(
        &self,
        job_id: &str,
        from_job_config_id: Option<&str>,
        to_job_config_id: &str,
        status: &str,
        strategy: Option<&str>,
        progression: Option<&str>,
        replacement: Option<&str>,
        in_place: Option<bool>,
        config_json: &str,
    ) -> R<Rollout> {
        let job_id = job_id.to_owned();
        let from_job_config_id = from_job_config_id.map(str::to_owned);
        let to_job_config_id = to_job_config_id.to_owned();
        let status = status.to_owned();
        let strategy = strategy.map(str::to_owned);
        let progression = progression.map(str::to_owned);
        let replacement = replacement.map(str::to_owned);
        let config_json = config_json.to_owned();
        self.replicated(Lane::Scheduler, "create_rollout", move |s| {
            s.create_rollout_apply(
                &job_id,
                from_job_config_id.as_deref(),
                &to_job_config_id,
                &status,
                strategy.as_deref(),
                progression.as_deref(),
                replacement.as_deref(),
                in_place,
                &config_json,
            )
        })
    }

    fn create_rollout_apply(
        &self,
        job_id: &str,
        from_job_config_id: Option<&str>,
        to_job_config_id: &str,
        status: &str,
        strategy: Option<&str>,
        progression: Option<&str>,
        replacement: Option<&str>,
        in_place: Option<bool>,
        config_json: &str,
    ) -> R<Rollout> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        let in_place = in_place.map(i64::from);
        let rollout = conn.query_row(
            "INSERT INTO rollout (
                job_id,
                from_job_config_id,
                to_job_config_id,
                status,
                strategy,
                progression,
                replacement,
                in_place,
                config_json,
                created_at,
                updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)
            RETURNING id, job_id, from_job_config_id, to_job_config_id, status, incomplete,
                      message, strategy, progression, replacement, in_place, config_json,
                      created_at, updated_at, completed_at;",
            params![
                job_id,
                from_job_config_id,
                to_job_config_id,
                status,
                strategy,
                progression,
                replacement,
                in_place,
                config_json,
                now
            ],
            |row| self.rollout_from_row(row),
        )?;

        Ok(rollout)
    }

    pub fn list_rollouts(&self, job_id: Option<&str>, active_only: bool) -> R<Vec<Rollout>> {
        let conn = self.read_connection()?;
        let base_sql = "SELECT id, job_id, from_job_config_id, to_job_config_id, status,
                incomplete, message, strategy, progression, replacement, in_place,
                config_json, created_at, updated_at, completed_at
            FROM rollout";

        let sql = match (job_id, active_only) {
            (Some(_), true) => {
                format!(
                    "{base_sql} WHERE job_id = ?1 AND status NOT IN ('completed','failed','cancelled','completed_incomplete') ORDER BY id DESC;"
                )
            }
            (Some(_), false) => format!("{base_sql} WHERE job_id = ?1 ORDER BY id DESC;"),
            (None, true) => format!(
                "{base_sql} WHERE status NOT IN ('completed','failed','cancelled','completed_incomplete') ORDER BY id DESC;"
            ),
            (None, false) => format!("{base_sql} ORDER BY id DESC;"),
        };

        let mut stmt = conn.prepare(&sql)?;
        if let Some(job_id) = job_id {
            let rows = stmt.query_map([job_id], |row| self.rollout_from_row(row))?;
            Self::collect_rows(rows)
        } else {
            let rows = stmt.query_map([], |row| self.rollout_from_row(row))?;
            Self::collect_rows(rows)
        }
    }

    pub fn get_rollout(&self, rollout_id: i64) -> R<Option<Rollout>> {
        let conn = self.read_connection()?;
        let rollout = conn
            .query_row(
                "SELECT id, job_id, from_job_config_id, to_job_config_id, status,
                        incomplete, message, strategy, progression, replacement, in_place,
                        config_json, created_at, updated_at, completed_at
                 FROM rollout
                 WHERE id = ?;",
                [rollout_id],
                |row| self.rollout_from_row(row),
            )
            .optional()?;

        Ok(rollout)
    }

    pub fn latest_rollout_for_job_config(&self, job_config_id: &str) -> R<Option<Rollout>> {
        let conn = self.read_connection()?;
        let rollout = conn
            .query_row(
                "SELECT id, job_id, from_job_config_id, to_job_config_id, status,
                        incomplete, message, strategy, progression, replacement, in_place,
                        config_json, created_at, updated_at, completed_at
                 FROM rollout
                 WHERE to_job_config_id = ?
                 ORDER BY id DESC
                 LIMIT 1;",
                [job_config_id],
                |row| self.rollout_from_row(row),
            )
            .optional()?;

        Ok(rollout)
    }

    #[instrument(err, skip_all)]
    pub fn update_rollout_status(
        &self,
        rollout_id: i64,
        status: &str,
        incomplete: bool,
        message: Option<&str>,
    ) -> R<()> {
        let status = status.to_owned();
        let message = message.map(str::to_owned);
        self.replicated(Lane::Scheduler, "update_rollout_status", move |s| {
            s.update_rollout_status_apply(rollout_id, &status, incomplete, message.as_deref())
        })
    }

    fn update_rollout_status_apply(
        &self,
        rollout_id: i64,
        status: &str,
        incomplete: bool,
        message: Option<&str>,
    ) -> R<()> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        let completed_at = match status {
            "completed" | "failed" | "cancelled" | "completed_incomplete" => Some(now.clone()),
            _ => None,
        };
        conn.execute(
            "UPDATE rollout
             SET status = ?1, incomplete = ?2, message = COALESCE(?3, message),
                 updated_at = ?4, completed_at = ?5
             WHERE id = ?6;",
            params![
                status,
                i64::from(incomplete),
                message,
                now,
                completed_at,
                rollout_id
            ],
        )?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn update_rollout_config_json(&self, rollout_id: i64, config_json: &str) -> R<()> {
        let config_json = config_json.to_owned();
        self.replicated(Lane::Scheduler, "update_rollout_config_json", move |s| {
            s.update_rollout_config_json_apply(rollout_id, &config_json)
        })
    }

    fn update_rollout_config_json_apply(&self, rollout_id: i64, config_json: &str) -> R<()> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        conn.execute(
            "UPDATE rollout
             SET config_json = ?1, updated_at = ?2
             WHERE id = ?3;",
            params![config_json, now, rollout_id],
        )?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn append_rollout_event(
        &self,
        rollout_id: i64,
        job_id: &str,
        group_name: Option<&str>,
        level: &str,
        message: &str,
        metadata_json: Option<&str>,
    ) -> R<RolloutEventRecord> {
        let job_id = job_id.to_owned();
        let group_name = group_name.map(str::to_owned);
        let level = level.to_owned();
        let message = message.to_owned();
        let metadata_json = metadata_json.map(str::to_owned);
        self.replicated(Lane::Scheduler, "append_rollout_event", move |s| {
            s.append_rollout_event_apply(
                rollout_id,
                &job_id,
                group_name.as_deref(),
                &level,
                &message,
                metadata_json.as_deref(),
            )
        })
    }

    fn append_rollout_event_apply(
        &self,
        rollout_id: i64,
        job_id: &str,
        group_name: Option<&str>,
        level: &str,
        message: &str,
        metadata_json: Option<&str>,
    ) -> R<RolloutEventRecord> {
        let conn = self.write_connection()?;
        let created_at = Self::now_rfc3339()?;
        let metadata_json = metadata_json.unwrap_or("{}");
        if metadata_json.trim().is_empty() {
            return Err(anyhow::anyhow!(
                "rollout event metadata_json cannot be empty"
            ));
        }
        let _: Value = serde_json::from_str(metadata_json)
            .with_context(|| format!("invalid rollout event metadata_json: {metadata_json}"))?;

        let event = conn.query_row(
            "INSERT INTO rollout_event (
                rollout_id,
                job_id,
                group_name,
                level,
                message,
                metadata_json,
                created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            RETURNING id, rollout_id, job_id, group_name, level, message, metadata_json, created_at;",
            params![
                rollout_id,
                job_id,
                group_name,
                level,
                message,
                metadata_json,
                created_at
            ],
            |row| self.rollout_event_from_row(row),
        )?;
        drop(conn);

        self.events().rollout_event(
            rollout_id,
            job_id,
            level,
            &event.message,
            event.group_name.as_deref(),
            &event.metadata_json,
        );

        Ok(event)
    }

    #[instrument(err, skip_all)]
    pub fn list_rollout_events(
        &self,
        rollout_id: i64,
        limit: Option<u32>,
    ) -> R<Vec<RolloutEventRecord>> {
        let conn = self.read_connection()?;
        let requested = limit.unwrap_or(500).clamp(1, 2_000);
        let mut stmt = conn.prepare(
            "SELECT id, rollout_id, job_id, group_name, level, message, metadata_json, created_at
             FROM (
                SELECT id, rollout_id, job_id, group_name, level, message, metadata_json, created_at
                FROM rollout_event
                WHERE rollout_id = ?1
                ORDER BY id DESC
                LIMIT ?2
             )
             ORDER BY id ASC;",
        )?;
        let rows = stmt.query_map(params![rollout_id, requested as i64], |row| {
            self.rollout_event_from_row(row)
        })?;
        Self::collect_rows(rows)
    }

    /// Record a scheduler event: persist it to the journal and publish it to
    /// live `StreamEvents` subscribers.
    ///
    /// Takes the write lock for the insert, so only use this from code that
    /// is not already holding a write connection; code with an open write
    /// transaction stages the event inside it via `stage_event_locked` +
    /// `publish_staged` instead, which costs no extra lock acquisition or
    /// WAL commit.
    ///
    /// Emission is strictly best-effort — scheduling work must never fail
    /// because the event journal could not be written, so errors are logged
    /// and swallowed here.
    pub fn emit_event(&self, draft: EventDraft) {
        let kind = draft.kind.clone();
        // Standalone emission replicates as its own (tiny) raft entry; calls
        // made while another capture is active compose into that capture
        // (JC-13) and cost nothing extra. `scheduler_event` is commutative
        // (JC-28) — ANY lane may carry the insert — so standalone emission
        // proposes on a lane this node currently LEADS: pinning it to the
        // status lane silently dropped every standalone event on a node
        // that did not lead that lane under divergent lane leadership
        // (plan §8). The choice only decides whose log carries the entry;
        // ids stay lane-residued (JC-42) and the journal is id-ordered.
        // Emission stays best-effort: a replication failure is logged and
        // swallowed, never propagated.
        let lane = [Lane::Status, Lane::Control, Lane::Scheduler]
            .into_iter()
            .find(|lane| self.is_raft_leader(*lane))
            .unwrap_or(Lane::Status);
        if let Err(err) = self.replicated(lane, "emit_event", move |s| {
            s.emit_event_apply(draft);
            Ok(())
        }) {
            warn!(%kind, error = ?err, "Failed replicating scheduler event");
        }
    }

    fn emit_event_apply(&self, draft: EventDraft) {
        let conn = match self.write_connection() {
            Ok(conn) => conn,
            Err(err) => {
                warn!(kind = %draft.kind, error = ?err, "Failed locking store for scheduler event");
                return;
            }
        };
        let event = self.stage_event_locked(&conn, draft);
        self.publish_staged(event);
    }

    /// Insert `draft` through an already-held write connection or open
    /// transaction, returning the persisted record for later publication.
    ///
    /// Best-effort: failures are logged and `None` is returned. When called
    /// with a transaction, the event commits or rolls back atomically with
    /// the state change it describes.
    fn stage_event_locked(
        &self,
        conn: &Connection,
        draft: EventDraft,
    ) -> Option<SchedulerEventRecord> {
        // Operator escape hatch: silences event persistence and streaming
        // entirely. Also how the benches A/B the cost of emission itself
        // (see docs/event-stream.md).
        if std::env::var_os("LEGION_EVENTS_OFF").is_some() {
            return None;
        }
        match self.insert_event(conn, &draft) {
            Ok(event) => Some(event),
            Err(err) => {
                warn!(
                    kind = %draft.kind,
                    error = ?err,
                    "Failed recording scheduler event"
                );
                None
            }
        }
    }

    /// Publish a staged event to live subscribers once its transaction has
    /// committed. Call while still holding the write guard the event was
    /// staged under, so subscribers observe events in strict ID order.
    fn publish_staged(&self, event: Option<SchedulerEventRecord>) {
        // During changeset capture the event ROW was staged (it rides the
        // changeset and commits with the mutation on every replica), but the
        // broadcast must wait for quorum commit: it is re-published from the
        // committed changeset at apply time (JC-11). Broadcasting here would
        // leak an uncommitted — and locally rolled-back — event.
        if capture::in_capture() {
            return;
        }
        if let Some(event) = event {
            // send only fails when there are no live subscribers; fine.
            let _ = self.event_tx.send(event);
        }
    }

    /// Project the side effects of a committed, just-applied changeset:
    /// re-emitting exactly what was suppressed while the changeset was being
    /// captured (JC-11). Runs on EVERY replica after
    /// `capture::apply_changeset` commits, which is how followers get read
    /// models, live event streams, and eval wakes at all.
    ///
    /// - `node` / `node_health` rows: re-read each touched node's joined
    ///   hydration row and patch the cache (or drop it when the node row
    ///   was deleted) — a node_health change patches the cached Node's
    ///   liveness fields;
    /// - `allocation` rows: refresh the per-node allocation lists that were
    ///   touched;
    /// - `task_config` rows: refresh the owning job config's cached task
    ///   list;
    /// - `scheduler_event` inserts: publish the committed rows to live
    ///   `StreamEvents` subscribers, in id order;
    /// - `eval` rows: wake the eval workers.
    ///
    /// Correct-over-clever: values not present in an UPDATE record (sessions
    /// omit unchanged non-PK columns) are recovered by re-reading the row by
    /// primary key, and cache patches re-read from the DB rather than
    /// decoding full rows out of the changeset.
    #[allow(dead_code)] // called from capture::apply_changeset (A-0 tests) and the A-1 state machine
    pub(crate) fn project_changeset_side_effects(&self, bytes: &[u8]) -> R<()> {
        use fallible_streaming_iterator::FallibleStreamingIterator;
        use rusqlite::hooks::Action;
        use rusqlite::session::ChangesetIter;

        if bytes.is_empty() {
            return Ok(());
        }

        // What the scan below collects; resolved against the (committed) DB
        // after the iteration so each affected cache entry is refreshed once.
        let mut node_ids: HashSet<String> = HashSet::new();
        let mut alloc_node_ids: HashSet<String> = HashSet::new();
        let mut alloc_reread_ids: HashSet<i64> = HashSet::new();
        let mut task_job_config_ids: HashSet<String> = HashSet::new();
        // Epoch scopes bumped by this changeset (the plan-fence mirror —
        // see `SchedulingReadModel::scope_epochs`): the changeset's own
        // `allocation_epoch_partition` rows name exactly the (scope_type,
        // scope_id) pairs whose mirror entries must be refreshed with the
        // allocation cache, coherently.
        let mut epoch_node_scopes: BTreeSet<String> = BTreeSet::new();
        let mut epoch_job_scopes: BTreeSet<String> = BTreeSet::new();
        let mut task_reread_ids: HashSet<i64> = HashSet::new();
        let mut event_ids: Vec<i64> = Vec::new();
        let mut eval_touched = false;

        // Column positions come from the live schema (changeset records use
        // the table's declared column order), so this cannot drift from
        // migrations.
        let mut column_cache: HashMap<String, Vec<String>> = HashMap::new();

        let reader: &mut dyn std::io::Read = &mut &*bytes;
        let mut iter = ChangesetIter::start_strm(&reader)
            .context("failed opening changeset iterator for side-effect projection")?;
        while let Some(item) = iter
            .next()
            .context("failed iterating changeset for side-effect projection")?
        {
            let op = item.op()?;
            let table = op.table_name().to_string();
            let action = op.code();
            match table.as_str() {
                "node" | "node_health" => {
                    // A node_health change patches the SAME cached Node —
                    // liveness hydrates from the node ⟕ node_health join
                    // (plan §7-C2, C-1) — so both tables funnel into one
                    // re-read of the joined row by node id.
                    //
                    // EXCEPT a bare heartbeat refresh (heartbeat-scale plan
                    // §3): a node_health UPDATE whose only changed columns
                    // are the heartbeat timestamps is invisible to the
                    // scheduler read model (see the invariant on
                    // `update_node_heartbeats_apply`), and at heartbeat
                    // rates the per-apply reprojection is the cost that
                    // starves read-model readers. Session changesets carry
                    // only columns whose value actually changed, so this is
                    // exact: any status/lost_at/health_checks movement still
                    // reprojects.
                    if table == "node_health" && action == Action::SQLITE_UPDATE {
                        let status_idx =
                            self.changeset_column_index(&mut column_cache, &table, "status")?;
                        let lost_at_idx =
                            self.changeset_column_index(&mut column_cache, &table, "lost_at")?;
                        let checks_idx = self.changeset_column_index(
                            &mut column_cache,
                            &table,
                            "health_checks",
                        )?;
                        let liveness_changed = item.new_value(status_idx).is_ok()
                            || item.new_value(lost_at_idx).is_ok()
                            || item.new_value(checks_idx).is_ok();
                        if !liveness_changed {
                            continue;
                        }
                    }
                    let pk_column = if table == "node" { "id" } else { "node_id" };
                    let idx = self.changeset_column_index(&mut column_cache, &table, pk_column)?;
                    // The PK is present in the insert's new record and in the
                    // old record of updates/deletes.
                    let id = match action {
                        Action::SQLITE_INSERT => item.new_value(idx).ok(),
                        _ => item.old_value(idx).ok(),
                    };
                    if let Some(id) = id.and_then(|value| value.as_str().ok().map(str::to_owned)) {
                        node_ids.insert(id);
                    }
                }
                "allocation" => {
                    let node_idx =
                        self.changeset_column_index(&mut column_cache, "allocation", "node_id")?;
                    match action {
                        Action::SQLITE_INSERT => {
                            if let Ok(value) = item.new_value(node_idx) {
                                if let Ok(node_id) = value.as_str() {
                                    alloc_node_ids.insert(node_id.to_owned());
                                }
                            }
                        }
                        Action::SQLITE_DELETE => {
                            if let Ok(value) = item.old_value(node_idx) {
                                if let Ok(node_id) = value.as_str() {
                                    alloc_node_ids.insert(node_id.to_owned());
                                }
                            }
                        }
                        _ => {
                            // UPDATE: unchanged columns are absent from the
                            // record. Collect whichever sides are present; if
                            // node_id itself did not change, fall back to
                            // re-reading the row by PK.
                            let mut found = false;
                            for value in [item.old_value(node_idx), item.new_value(node_idx)]
                                .into_iter()
                                .flatten()
                            {
                                if let Ok(node_id) = value.as_str() {
                                    alloc_node_ids.insert(node_id.to_owned());
                                    found = true;
                                }
                            }
                            if !found {
                                let id_idx = self.changeset_column_index(
                                    &mut column_cache,
                                    "allocation",
                                    "id",
                                )?;
                                if let Some(id) =
                                    item.old_value(id_idx).ok().and_then(|v| v.as_i64().ok())
                                {
                                    alloc_reread_ids.insert(id);
                                }
                            }
                        }
                    }
                }
                "task_config" => {
                    let jc_idx = self.changeset_column_index(
                        &mut column_cache,
                        "task_config",
                        "job_config_id",
                    )?;
                    let jc = match action {
                        Action::SQLITE_INSERT => item.new_value(jc_idx).ok(),
                        Action::SQLITE_DELETE => item.old_value(jc_idx).ok(),
                        _ => item
                            .old_value(jc_idx)
                            .ok()
                            .or_else(|| item.new_value(jc_idx).ok()),
                    };
                    match jc.and_then(|value| value.as_str().ok().map(str::to_owned)) {
                        Some(jc) => {
                            task_job_config_ids.insert(jc);
                        }
                        None => {
                            let id_idx = self.changeset_column_index(
                                &mut column_cache,
                                "task_config",
                                "id",
                            )?;
                            if let Some(id) =
                                item.old_value(id_idx).ok().and_then(|v| v.as_i64().ok())
                            {
                                task_reread_ids.insert(id);
                            }
                        }
                    }
                }
                "scheduler_event" => {
                    // Only inserts publish; deletes are journal pruning.
                    if action == Action::SQLITE_INSERT {
                        let id_idx = self.changeset_column_index(
                            &mut column_cache,
                            "scheduler_event",
                            "id",
                        )?;
                        if let Some(id) = item.new_value(id_idx).ok().and_then(|v| v.as_i64().ok())
                        {
                            event_ids.push(id);
                        }
                    }
                }
                "eval" => {
                    eval_touched = true;
                }
                "allocation_epoch_partition" => {
                    let type_idx = self.changeset_column_index(
                        &mut column_cache,
                        "allocation_epoch_partition",
                        "scope_type",
                    )?;
                    let id_idx = self.changeset_column_index(
                        &mut column_cache,
                        "allocation_epoch_partition",
                        "scope_id",
                    )?;
                    // Composite PK (scope_type, scope_id): present in the
                    // new record of inserts and the old record of updates.
                    let (scope_type, scope_id) = match action {
                        Action::SQLITE_INSERT => {
                            (item.new_value(type_idx).ok(), item.new_value(id_idx).ok())
                        }
                        _ => (item.old_value(type_idx).ok(), item.old_value(id_idx).ok()),
                    };
                    let scope_type =
                        scope_type.and_then(|value| value.as_str().ok().map(str::to_owned));
                    let scope_id =
                        scope_id.and_then(|value| value.as_str().ok().map(str::to_owned));
                    match (scope_type.as_deref(), scope_id) {
                        (Some("node"), Some(id)) => {
                            epoch_node_scopes.insert(id);
                        }
                        (Some("job_config"), Some(id)) => {
                            epoch_job_scopes.insert(id);
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        drop(iter);

        // Resolve rows whose owning scope was not carried by the update
        // record.
        for id in alloc_reread_ids {
            if let Some(allocation) = self.get_allocation_by_id(id)? {
                alloc_node_ids.insert(allocation.node_id);
            }
        }
        for id in task_reread_ids {
            let conn = self.read_connection()?;
            let jc: Option<String> = conn
                .query_row(
                    "SELECT job_config_id FROM task_config WHERE id = ?;",
                    [id],
                    |row| row.get(0),
                )
                .optional()?;
            drop(conn);
            if let Some(jc) = jc {
                task_job_config_ids.insert(jc);
            }
        }

        // Patch the read model from committed state.
        for id in node_ids {
            let node = {
                let conn = self.read_connection()?;
                conn.query_row(&GET_NODE, [id.as_str()], |row| self.node_from_row(row))
                    .optional()?
            };
            match node {
                Some(node) => self.cache_node(node)?,
                None => self.cache_remove_node(&id)?,
            }
        }
        // Allocation cache + epoch mirror refresh, coherently (the
        // plan-fence fix): the helper reads epochs before allocations,
        // publishes both under one lock, and reprojects the affected
        // nodes' free pools/indexes inside that lock — so a sampler can
        // never pair a new epoch with a stale cache, and the plan-snapshot
        // cache never observes pools out of step with allocations.
        let mut refresh_nodes: BTreeSet<String> = alloc_node_ids.into_iter().collect();
        refresh_nodes.extend(epoch_node_scopes);
        if !refresh_nodes.is_empty() || !epoch_job_scopes.is_empty() {
            self.cache_refresh_allocation_scopes(&refresh_nodes, &epoch_job_scopes)?;
        }
        for job_config_id in task_job_config_ids {
            let tasks = self.get_task_configs_for_job_config(&job_config_id)?;
            self.cache_set_task_configs_for_job_config(&job_config_id, tasks)?;
        }

        // Publish committed scheduler events in id order.
        event_ids.sort_unstable();
        for &id in &event_ids {
            let record = {
                let conn = self.read_connection()?;
                conn.query_row(
                    "SELECT id, kind, level, job_id, node_id, eval_id, allocation_id, \
                     rollout_id, message, metadata_json, created_at \
                     FROM scheduler_event WHERE id = ?;",
                    [id],
                    |row| self.scheduler_event_from_row(row),
                )
                .optional()?
            };
            self.publish_staged(record);
        }

        // Local journal maintenance (JC-28): commutative-table pruning left
        // raft, so each replica prunes its own journal here — the same
        // boundary condition `insert_event` uses opportunistically outside
        // capture. Best-effort: a failed prune only delays pruning.
        if event_ids
            .iter()
            .any(|id| Self::event_id_triggers_prune(*id))
        {
            if let Err(err) = self.prune_scheduler_events_local() {
                warn!(error = ?err, "Failed pruning local scheduler event journal");
            }
        }

        if eval_touched {
            self.eval_notify.notify_waiters();
        }

        Ok(())
    }

    /// Prune this node's scheduler-event journal down to the newest
    /// [`EVENT_RETAINED_ROWS`] rows, as a LOCAL write outside raft (JC-28:
    /// commutative tables replicate INSERTs only; each node maintains its
    /// own journal). Keeps `docs/event-stream.md`'s bounded-journal promise
    /// in raft mode, where `insert_event`'s opportunistic prune is
    /// suppressed under capture. Count-based (not id-distance) because ids
    /// are time-prefixed and sparse (`next_scheduler_event_id`).
    fn prune_scheduler_events_local(&self) -> R<()> {
        self.local_write("prune_scheduler_events", |s| {
            let conn = s.write_connection()?;
            Self::prune_event_journal_on(&conn)?;
            Ok(())
        })
    }

    /// The journal prune statement itself: delete everything older than the
    /// newest [`EVENT_RETAINED_ROWS`] rows (by id, which sorts
    /// ~chronologically). A journal at or under the retention size deletes
    /// nothing.
    fn prune_event_journal_on(conn: &Connection) -> R<()> {
        conn.execute(
            "DELETE FROM scheduler_event WHERE id < (
                 SELECT COALESCE(MIN(id), 0) FROM (
                     SELECT id FROM scheduler_event ORDER BY id DESC LIMIT ?1
                 )
             );",
            [EVENT_RETAINED_ROWS],
        )?;
        Ok(())
    }

    /// Index of `column` within `table`'s declared column order (which is
    /// also the changeset record order), resolved once per table per
    /// projection via `PRAGMA table_info`.
    #[allow(dead_code)] // see project_changeset_side_effects
    fn changeset_column_index(
        &self,
        cache: &mut HashMap<String, Vec<String>>,
        table: &str,
        column: &str,
    ) -> R<usize> {
        if !cache.contains_key(table) {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(&format!("PRAGMA table_info({table});"))?;
            let columns: Vec<String> =
                Self::collect_rows(stmt.query_map([], |row| row.get::<_, String>(1))?)?;
            drop(stmt);
            drop(conn);
            cache.insert(table.to_string(), columns);
        }
        cache[table]
            .iter()
            .position(|name| name == column)
            .with_context(|| format!("column {column} not found in table {table}"))
    }

    /// The id residue commutative-table ids minted on this thread must
    /// carry: the active capture's lane (JC-42), or [`Lane::Control`]'s
    /// residue (`0`) outside any capture — `local_disk` mode and every
    /// non-raft caller, where no second lane is minting concurrently, so
    /// their ids are unchanged in shape from the un-strided scheme.
    fn event_id_residue() -> i64 {
        capture::current_capture_lane()
            .unwrap_or(Lane::Control)
            .id_residue()
    }

    /// A globally-unique, roughly time-ordered `scheduler_event` id:
    /// unix milliseconds in the high bits, a per-process sequence (seeded
    /// from a random value at first use) in the next [`EVENT_ID_SEQ_BITS`]
    /// bits, and the minting lane's residue in the low
    /// [`Lane::ID_STRIDE_BITS`] bits — so `id % Lane::ID_STRIDE` is always
    /// this lane's [`Lane::id_residue`].
    ///
    /// This is what makes the JC-28 commutative-class premise ("insert-only
    /// tables with globally-unique PKs") actually true for the event
    /// journal. The original implicit-rowid insert assigned `max(id) + 1` on
    /// the PROPOSING node, so two lanes staging events from the same base
    /// state minted the SAME id with different content; the OMIT conflict
    /// policy then kept whichever lane's entry applied first — an
    /// order-dependent, per-replica outcome, i.e. silent divergence
    /// invisible to the content hash (adversarial finding, 2026-07-24).
    ///
    /// Two properties, from two independent mechanisms:
    ///
    /// - **Never collides across lanes**, structurally: the lanes' residue
    ///   classes are disjoint, so no pair of ids minted on different lanes
    ///   can ever be equal, whatever their clocks and sequences do (JC-42).
    /// - **Strictly increasing per lane**, within a process: the timestamp
    ///   dominates the sequence, and a per-residue high-water mark bumps by
    ///   a full stride whenever the clock is not ahead (clock skew
    ///   backwards, or two ids inside the same millisecond after a sequence
    ///   wrap), so the minted value never repeats or regresses.
    ///
    /// The residual same-lane hazard is two PROCESSES minting for the same
    /// lane in the same millisecond with the same sequence bits (~4e-6 per
    /// same-millisecond pair, from the random per-process seed) — reachable
    /// only across a leader failover, resolved locally by the PK-conflict
    /// retry in [`Store::insert_event`], and harmless for an advisory
    /// journal. Ids remain ~chronological, which is what the id-cursor
    /// list/stream/prune semantics need (they never needed density — see
    /// `docs/event-stream.md`).
    ///
    /// Upgrade-safe by arithmetic: making room for the residue moved the
    /// millisecond field one bit LEFT, so every id this mints is above every
    /// id either earlier scheme produced (`ms << 20 | seq`, and the original
    /// `max(id) + 1`) for the same instant — an existing journal's cursor
    /// never has to go backwards.
    fn next_scheduler_event_id() -> i64 {
        use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
        static SEQ: std::sync::OnceLock<AtomicU64> = std::sync::OnceLock::new();
        /// Per-residue high-water mark enforcing strict monotonicity per
        /// lane (see above). One slot per possible residue.
        static LAST: [AtomicI64; Lane::ID_STRIDE as usize] =
            [const { AtomicI64::new(0) }; Lane::ID_STRIDE as usize];

        let residue = Self::event_id_residue();
        let seq = SEQ.get_or_init(|| AtomicU64::new(rand::random::<u64>()));
        let sequence = seq.fetch_add(1, Ordering::Relaxed) & ((1u64 << EVENT_ID_SEQ_BITS) - 1);
        let ms = (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        let candidate = (ms << (EVENT_ID_SEQ_BITS + Lane::ID_STRIDE_BITS))
            | ((sequence as i64) << Lane::ID_STRIDE_BITS)
            | residue;

        let slot = &LAST[residue as usize];
        let mut previous = slot.load(Ordering::Relaxed);
        loop {
            // Advancing by a whole stride keeps the residue (and therefore
            // the lane disjointness) intact.
            let next = if candidate > previous {
                candidate
            } else {
                previous + Lane::ID_STRIDE
            };
            match slot.compare_exchange_weak(previous, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return next,
                Err(actual) => previous = actual,
            }
        }
    }

    /// Whether an event id lands on the opportunistic journal-prune
    /// boundary (roughly one insert in [`EVENT_PRUNE_INTERVAL`]).
    ///
    /// The lane residue is divided out FIRST. Testing `id %
    /// EVENT_PRUNE_INTERVAL == 0` directly would be satisfiable only by ids
    /// divisible by [`Lane::ID_STRIDE`] (the interval is a multiple of the
    /// stride), i.e. only ever by the control lane — every other lane's
    /// events would never trigger a prune, and a cluster whose journal is
    /// dominated by status/scheduler events would grow unbounded on the
    /// nodes applying them. Shifting the residue out first makes the
    /// boundary the per-lane sequence, so every lane triggers at the same
    /// rate (JC-42).
    fn event_id_triggers_prune(id: i64) -> bool {
        (id / Lane::ID_STRIDE) % EVENT_PRUNE_INTERVAL == 0
    }

    fn insert_event(&self, conn: &Connection, draft: &EventDraft) -> R<SchedulerEventRecord> {
        let created_at = Self::now_rfc3339()?;
        let metadata_json = if draft.metadata.is_object() {
            draft.metadata.to_string()
        } else {
            json!({ "value": draft.metadata }).to_string()
        };

        // Explicit globally-unique id (never the implicit rowid — see
        // `next_scheduler_event_id`). A local PK collision (same ms + same
        // low bits from an earlier process run) is retried with a fresh id.
        let mut id = Self::next_scheduler_event_id();
        let mut attempts = 0u32;
        loop {
            let inserted = conn.execute(
                "INSERT INTO scheduler_event (
                    id, kind, level, job_id, node_id, eval_id, allocation_id, rollout_id,
                    message, metadata_json, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11);",
                params![
                    id,
                    draft.kind,
                    draft.level,
                    draft.job_id,
                    draft.node_id,
                    draft.eval_id,
                    draft.allocation_id,
                    draft.rollout_id,
                    draft.message,
                    metadata_json,
                    created_at
                ],
            );
            match inserted {
                Ok(_) => break,
                Err(RusqliteError::SqliteFailure(e, _))
                    if e.code == rusqlite::ErrorCode::ConstraintViolation && attempts < 8 =>
                {
                    attempts += 1;
                    id = Self::next_scheduler_event_id();
                }
                Err(err) => return Err(err.into()),
            }
        }

        // Opportunistic journal pruning — but never inside a capture:
        // `scheduler_event` is commutative (JC-28), so only its INSERTs may
        // ride a changeset; pruning is per-node local maintenance. In raft
        // mode every insert here runs under capture, and each replica
        // prunes its own journal from the committed changeset instead
        // (`project_changeset_side_effects` → `prune_scheduler_events_local`).
        // The prune keeps the newest EVENT_RETAINED_ROWS rows by COUNT (ids
        // are time-prefixed and lane-strided, not dense, so an id-distance
        // window would be meaningless).
        if Self::event_id_triggers_prune(id) && !capture::in_capture() {
            Self::prune_event_journal_on(conn)?;
        }

        Ok(SchedulerEventRecord {
            id,
            kind: draft.kind.clone(),
            level: draft.level.clone(),
            job_id: draft.job_id.clone(),
            node_id: draft.node_id.clone(),
            eval_id: draft.eval_id.clone(),
            allocation_id: draft.allocation_id,
            rollout_id: draft.rollout_id,
            message: draft.message.clone(),
            metadata_json,
            created_at,
        })
    }

    /// Subscribe to live scheduler events. Receivers that lag behind the
    /// broadcast buffer should resume from the journal via
    /// `list_scheduler_events` using the last event ID they saw.
    pub fn subscribe_events(&self) -> broadcast::Receiver<SchedulerEventRecord> {
        self.event_tx.subscribe()
    }

    /// List journal events matching `filter`, ordered oldest-to-newest.
    ///
    /// Without an `after_id` cursor this returns the most recent
    /// `filter.effective_limit()` events; with a cursor it pages forward from
    /// `after_id` so pollers never skip events.
    #[instrument(err, skip_all)]
    pub fn list_scheduler_events(
        &self,
        filter: &SchedulerEventFilter,
    ) -> R<Vec<SchedulerEventRecord>> {
        let conn = self.read_connection()?;
        let limit = filter.effective_limit() as i64;

        let mut clauses: Vec<String> = Vec::new();
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        if let Some(after_id) = filter.after_id {
            clauses.push(format!("id > ?{}", params.len() + 1));
            params.push(Box::new(after_id));
        }
        if let Some(kind) = filter.kind.as_deref() {
            clauses.push(format!(
                "(kind = ?{} OR kind LIKE ?{} || '.%')",
                params.len() + 1,
                params.len() + 2
            ));
            params.push(Box::new(kind.to_string()));
            params.push(Box::new(kind.to_string()));
        }
        if let Some(job_id) = filter.job_id.as_deref() {
            clauses.push(format!("job_id = ?{}", params.len() + 1));
            params.push(Box::new(job_id.to_string()));
        }
        if let Some(node_id) = filter.node_id.as_deref() {
            clauses.push(format!("node_id = ?{}", params.len() + 1));
            params.push(Box::new(node_id.to_string()));
        }
        if let Some(level) = filter.level.as_deref() {
            clauses.push(format!("level = ?{}", params.len() + 1));
            params.push(Box::new(level.to_string()));
        }
        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        params.push(Box::new(limit));
        let limit_param = params.len();

        const EVENT_COLUMNS: &str = "id, kind, level, job_id, node_id, eval_id, allocation_id, rollout_id, message, metadata_json, created_at";
        // With a cursor, page forward (oldest first). Without one, take the
        // newest window and re-sort ascending, like list_rollout_events.
        let sql = if filter.after_id.is_some() {
            format!(
                "SELECT {EVENT_COLUMNS} FROM scheduler_event {where_clause} ORDER BY id ASC LIMIT ?{limit_param};"
            )
        } else {
            format!(
                "SELECT {EVENT_COLUMNS} FROM (
                    SELECT {EVENT_COLUMNS} FROM scheduler_event {where_clause} ORDER BY id DESC LIMIT ?{limit_param}
                ) ORDER BY id ASC;"
            )
        };

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            params_from_iter(params.iter().map(|param| param.as_ref())),
            |row| self.scheduler_event_from_row(row),
        )?;
        Self::collect_rows(rows)
    }

    /// Highest event ID currently in the journal (0 when empty). Used by
    /// clients to initialize a polling/streaming cursor.
    #[instrument(err, skip_all)]
    pub fn latest_scheduler_event_id(&self) -> R<i64> {
        let conn = self.read_connection()?;
        let latest: Option<i64> =
            conn.query_row("SELECT MAX(id) FROM scheduler_event;", [], |row| row.get(0))?;
        Ok(latest.unwrap_or(0))
    }

    fn scheduler_event_from_row(
        &self,
        row: &Row<'_>,
    ) -> Result<SchedulerEventRecord, rusqlite::Error> {
        Ok(SchedulerEventRecord {
            id: row.get(0)?,
            kind: row.get(1)?,
            level: row.get(2)?,
            job_id: row.get(3)?,
            node_id: row.get(4)?,
            eval_id: row.get(5)?,
            allocation_id: row.get(6)?,
            rollout_id: row.get(7)?,
            message: row.get(8)?,
            metadata_json: row.get(9)?,
            created_at: row.get(10)?,
        })
    }

    #[instrument(err, skip_all)]
    pub fn prune_rollouts(&self) -> R<u64> {
        self.replicated(Lane::Scheduler, "prune_rollouts", move |s| {
            s.prune_rollouts_apply()
        })
    }

    fn prune_rollouts_apply(&self) -> R<u64> {
        let mut conn = self.write_connection()?;
        let cutoff =
            OffsetDateTime::now_utc() - TimeDuration::days(self.rollout_retention_days as i64);
        let cutoff = cutoff
            .format(&Rfc3339)
            .context("failed to format rollout retention cutoff")?;
        let tx = Self::write_tx(&mut conn)?;
        // The rollout_event FK cascade is gone (C-2 cross-lane FK removal);
        // both tables are scheduler-owned, so the retention delete clears
        // the events explicitly in the same transaction.
        tx.execute(
            "DELETE FROM rollout_event WHERE rollout_id IN (
                 SELECT id FROM rollout WHERE completed_at IS NOT NULL AND completed_at < ?1
             );",
            [cutoff.as_str()],
        )?;
        let changed = tx.execute(
            "DELETE FROM rollout WHERE completed_at IS NOT NULL AND completed_at < ?1;",
            [cutoff.as_str()],
        )?;
        tx.commit()?;
        Ok(changed as u64)
    }

    #[instrument(err, skip_all)]
    pub fn job_configs_schedule_equivalent(&self, from_id: &str, to_id: &str) -> R<bool> {
        let from_tasks = self.get_task_configs_for_job_config(from_id)?;
        let to_tasks = self.get_task_configs_for_job_config(to_id)?;

        if from_tasks.len() != to_tasks.len() {
            return Ok(false);
        }

        let to_map = to_tasks
            .into_iter()
            .map(|(_, task)| (task.name.clone(), task))
            .collect::<HashMap<_, _>>();

        for (_, task) in from_tasks {
            let Some(to_task) = to_map.get(&task.name) else {
                return Ok(false);
            };
            if !task_schedule_equivalent(&task, to_task) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    #[instrument(err, skip_all)]
    pub fn migrate_allocations_to_job_config(
        &self,
        from_job_config_id: &str,
        to_job_config_id: &str,
    ) -> R<Vec<Allocation>> {
        let from_job_config_id = from_job_config_id.to_owned();
        let to_job_config_id = to_job_config_id.to_owned();
        self.replicated(
            Lane::Scheduler,
            "migrate_allocations_to_job_config",
            move |s| {
                s.migrate_allocations_to_job_config_apply(&from_job_config_id, &to_job_config_id)
            },
        )
    }

    fn migrate_allocations_to_job_config_apply(
        &self,
        from_job_config_id: &str,
        to_job_config_id: &str,
    ) -> R<Vec<Allocation>> {
        let from_tasks = self.get_task_configs_for_job_config(from_job_config_id)?;
        let to_tasks = self.get_task_configs_for_job_config(to_job_config_id)?;
        let to_map = to_tasks
            .into_iter()
            .map(|(id, task)| (task.name.clone(), id))
            .collect::<HashMap<_, _>>();

        let mut task_id_map = HashMap::new();
        for (old_id, task) in from_tasks {
            let new_id = to_map
                .get(&task.name)
                .copied()
                .with_context(|| format!("missing updated task config for {}", task.name))?;
            task_id_map.insert(old_id, new_id);
        }

        let allocations = self.allocs_for_job_config(from_job_config_id)?;
        if allocations.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        for allocation in &allocations {
            let new_task_id = task_id_map
                .get(&allocation.task_config_id)
                .copied()
                .context("missing task id mapping for allocation")?;
            tx.execute(
                "UPDATE allocation SET job_config_id = ?, task_config_id = ? WHERE id = ?;",
                (to_job_config_id, new_task_id, allocation.id),
            )?;
        }
        let updated_allocations = allocations
            .into_iter()
            .map(|mut allocation| {
                let new_task_id = task_id_map
                    .get(&allocation.task_config_id)
                    .copied()
                    .unwrap_or(allocation.task_config_id);
                allocation.job_config_id = to_job_config_id.to_string();
                allocation.task_config_id = new_task_id;
                allocation
            })
            .collect::<Vec<_>>();

        // Allocation migration mutates allocation ownership and must invalidate
        // plan submissions for both source and destination job configs.
        self.bump_job_config_epoch_tx(&tx, from_job_config_id)?;
        self.bump_epochs_for_allocations_tx(&tx, &updated_allocations)?;
        tx.commit()?;

        self.cache_remove_allocations(&updated_allocations)?;
        self.cache_add_allocations(&updated_allocations)?;

        Ok(updated_allocations)
    }

    #[instrument(err, skip_all)]
    pub fn get_submissions_for_config(&self, job_config_id: &str) -> R<Vec<Submission>> {
        let conn = self.read_connection()?;

        let sql = r#"
        SELECT jcs.path, jcs.is_primary, s.content
        FROM submission s
        INNER JOIN job_config_submission jcs ON jcs.submission_hash = s.hash
        WHERE jcs.job_config_id = ?;
"#;
        let mut stmt = conn.prepare(sql)?;
        let subs = Self::collect_rows(stmt.query_map([job_config_id], |row| {
            Ok(Submission {
                path: row.get(0)?,
                is_primary: row.get(1)?,
                content: row.get(2)?,
            })
        })?)?;

        Ok(subs)
    }

    #[instrument(err, skip_all)]
    pub fn get_task_instances_for_job_config(&self, job_config_id: &str) -> R<Vec<(i64, u32)>> {
        let conn = self.read_connection()?;
        let sql = r#"
        SELECT id, count
        FROM task_config
        WHERE job_config_id = ?;
        "#;
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([job_config_id], |row| {
            let id: i64 = row.get(0)?;
            let count: u32 = row.get::<_, i64>(1)? as u32;
            Ok((id, count))
        })?;

        let mut task_instances = Vec::new();
        for row in rows {
            task_instances.push(row?);
        }

        Ok(task_instances)
    }

    #[instrument(err, skip_all)]
    pub fn get_task_configs_for_job_config(
        &self,
        job_config_id: &str,
    ) -> R<Vec<(i64, TaskConfig)>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&GET_TASK_FOR_JOB_CONIFG)?;
        let rows = stmt.query_map([job_config_id], |row| {
            let id = row.get(0)?;
            let task = self.task_config_from_row(row)?;
            Ok((id, task))
        })?;

        Self::collect_rows(rows)
    }

    pub fn task_statuses_for_job_config(
        &self,
        job_config_id: &str,
    ) -> R<HashMap<i64, TaskStatusState>> {
        let conn = self.read_connection()?;
        let mut stmt = conn
            .prepare("SELECT task_config_id, status FROM task_status WHERE job_config_id = ?;")?;
        let rows = stmt.query_map([job_config_id], |row| {
            let task_id: i64 = row.get(0)?;
            let status: String = row.get(1)?;
            let status = status.parse::<TaskStatusState>().map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
                )
            })?;
            Ok((task_id, status))
        })?;

        let mut statuses = HashMap::new();
        for row in rows {
            let (task_id, status) = row?;
            statuses.insert(task_id, status);
        }
        Ok(statuses)
    }

    pub fn set_task_status(&self, task_config_id: i64, status: TaskStatusState) -> R<()> {
        self.replicated(Lane::Status, "set_task_status", move |s| {
            s.set_task_status_apply(task_config_id, status)
        })
    }

    fn set_task_status_apply(&self, task_config_id: i64, status: TaskStatusState) -> R<()> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        conn.execute(
            "UPDATE task_status SET status = ?, updated_at = ? WHERE task_config_id = ?;",
            params![status.as_str(), now, task_config_id],
        )?;
        Ok(())
    }

    /// Task status to stamp when an allocation is placed.
    ///
    /// A batch task is `Running` until a worker reports that its process
    /// exited — that report is what makes `dependsOn: { condition: "success" }`
    /// mean "exited 0" rather than "was placed" (see docs/task-supervision.md).
    ///
    /// `supervised_node` is false for workers too old to send exit reports
    /// (protocol version < 2). Those nodes will never report a termination, so
    /// their batch tasks keep the legacy optimistic `Success` — otherwise a
    /// mid-upgrade cluster (managers upgrade first) would stall every DAG
    /// waiting on a batch task.
    fn task_status_for_lifecycle(
        lifecycle: &TaskLifecycle,
        supervised_node: bool,
    ) -> TaskStatusState {
        match lifecycle {
            TaskLifecycle::Batch if supervised_node => TaskStatusState::Running,
            TaskLifecycle::Batch => TaskStatusState::Success,
            TaskLifecycle::Service | TaskLifecycle::Sidecar => TaskStatusState::Running,
        }
    }

    /// Whether a node's worker reports task exits (protocol version 2+). Reads
    /// the version recorded in the node's dynamic attrs at registration;
    /// anything unreadable is treated as an old worker, which is the
    /// conservative direction (keeps today's behavior).
    fn node_reports_exits_tx(tx: &Connection, node_id: &str) -> bool {
        let raw: Option<String> = tx
            .query_row(
                "SELECT dynamic_attrs FROM node WHERE id = ?;",
                params![node_id],
                |row| row.get(0),
            )
            .optional()
            .ok()
            .flatten();
        let Some(raw) = raw else {
            return false;
        };
        serde_json::from_str::<Value>(&raw)
            .ok()
            .and_then(|attrs| attrs.get("protocolVersion").and_then(Value::as_u64))
            .is_some_and(|version| version >= 2)
    }

    fn apply_task_status_updates_tx(
        &self,
        tx: &Connection,
        updates: &HashMap<i64, TaskStatusState>,
    ) -> R<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let now = Self::now_rfc3339()?;
        let mut update_stmt = tx.prepare(
            "UPDATE task_status SET status = ?1, updated_at = ?2 WHERE task_config_id = ?3;",
        )?;
        for (task_config_id, status) in updates {
            update_stmt.execute(params![status.as_str(), now, task_config_id])?;
        }
        Ok(())
    }

    pub fn update_task_statuses_for_allocations(&self, allocations: &[Allocation]) -> R<bool> {
        if allocations.is_empty() {
            return Ok(false);
        }
        let allocations = allocations.to_vec();
        self.replicated(
            Lane::Status,
            "update_task_statuses_for_allocations",
            move |s| s.update_task_statuses_for_allocations_apply(&allocations),
        )
    }

    fn update_task_statuses_for_allocations_apply(&self, allocations: &[Allocation]) -> R<bool> {
        let tx_started = Instant::now();
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let mut task_cache: HashMap<String, HashMap<i64, TaskConfig>> = HashMap::new();
        let mut updates: HashMap<i64, TaskStatusState> = HashMap::new();
        // One node lookup per distinct node, not per allocation.
        let mut node_supervision: HashMap<String, bool> = HashMap::new();

        for allocation in allocations {
            let tasks = task_cache
                .entry(allocation.job_config_id.clone())
                .or_default();
            if tasks.is_empty() {
                let configs = self.get_task_configs_for_job_config(&allocation.job_config_id)?;
                for (id, config) in configs {
                    tasks.insert(id, config);
                }
            }
            let task = tasks
                .get(&allocation.task_config_id)
                .with_context(|| format!("Missing task config {}", allocation.task_config_id))?;

            let supervised_node = *node_supervision
                .entry(allocation.node_id.clone())
                .or_insert_with(|| Self::node_reports_exits_tx(&tx, &allocation.node_id));
            updates.insert(
                allocation.task_config_id,
                Self::task_status_for_lifecycle(&task.lifecycle, supervised_node),
            );
        }

        self.apply_task_status_updates_tx(&tx, &updates)?;
        tx.commit()?;
        debug!(
            allocation_count = allocations.len(),
            updated_tasks = updates.len(),
            duration_ms = tx_started.elapsed().as_millis(),
            "Updated task statuses for allocations"
        );

        Ok(!updates.is_empty())
    }

    //
    // ALLOCS
    //

    #[instrument(err, skip_all)]
    pub fn place_alloc(&self, node_id: &str, task_id: i64) -> R<Allocation> {
        let node_id = node_id.to_owned();
        self.replicated(Lane::Scheduler, "place_alloc", move |s| {
            s.place_alloc_apply(&node_id, task_id)
        })
    }

    fn place_alloc_apply(&self, node_id: &str, task_id: i64) -> R<Allocation> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let allocation = self.insert_allocation(&tx, node_id, task_id)?;
        self.bump_epochs_for_allocations_tx(&tx, std::slice::from_ref(&allocation))?;
        tx.commit()?;

        self.cache_add_allocations(std::slice::from_ref(&allocation))?;

        Ok(allocation)
    }

    // NOTE: this helper only inserts allocation rows; callers must bump
    // job-config epochs for affected configs before committing the tx.
    fn insert_allocation(&self, tx: &Connection, node_id: &str, task_id: i64) -> R<Allocation> {
        // This helper writes a single allocation row. Callers mutating allocation state
        // must call bump_epochs_for_allocations_tx (or bump_job_config_epoch_tx)
        // in the same transaction before commit.
        let (job_config_id, resources, network_raw, connect_raw): (
            String,
            Value,
            Option<String>,
            Option<String>,
        ) = tx.query_row(
            "SELECT job_config_id, resources, network, connect FROM task_config WHERE id = ?;",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;

        let network =
            self.assign_allocation_network(tx, node_id, task_id, network_raw, connect_raw)?;
        self.insert_allocation_with_data(
            tx,
            node_id,
            task_id,
            &job_config_id,
            &resources,
            network.as_ref(),
        )
    }

    /// Assign the node-local ports a new allocation of `task_id` on `node_id`
    /// needs — task host ports and `connect.upstreams` loopback ports — inside
    /// the current transaction. Reads ports already assigned on the node (both
    /// kinds share the node's one port space) so concurrent placements in the
    /// same tx or committed earlier cannot collide. Static conflicts fail
    /// placement with a node-scoped error.
    fn assign_allocation_network(
        &self,
        tx: &Connection,
        node_id: &str,
        task_id: i64,
        network_raw: Option<String>,
        connect_raw: Option<String>,
    ) -> R<Option<crate::structs::AllocNetwork>> {
        let network: Option<crate::structs::NetworkConfig> = match network_raw.as_deref() {
            Some(raw) => Some(
                from_str(raw)
                    .with_context(|| format!("invalid stored network config for task {task_id}"))?,
            ),
            None => None,
        };
        let connect: Option<crate::structs::ConnectConfig> = match connect_raw.as_deref() {
            Some(raw) => Some(
                from_str(raw)
                    .with_context(|| format!("invalid stored connect config for task {task_id}"))?,
            ),
            None => None,
        };
        let has_ports = network
            .as_ref()
            .is_some_and(|network| !network.ports.is_empty());
        let has_upstreams = connect
            .as_ref()
            .is_some_and(|connect| !connect.upstreams.is_empty());
        if !has_ports && !has_upstreams {
            return Ok(None);
        }

        let mut used: HashSet<u16> = HashSet::new();
        let mut stmt = tx
            .prepare("SELECT network FROM allocation WHERE node_id = ? AND network IS NOT NULL;")?;
        let rows = stmt.query_map([node_id], |row| row.get::<_, Option<String>>(0))?;
        for row in rows {
            let Some(raw) = row? else { continue };
            let existing: crate::structs::AllocNetwork = from_str(&raw)
                .with_context(|| format!("invalid stored allocation network on node {node_id}"))?;
            used.extend(existing.ports.iter().map(|port| port.host_port));
            used.extend(
                existing
                    .upstreams
                    .iter()
                    .map(|upstream| upstream.local_port),
            );
        }

        let (ports, upstreams) =
            crate::structs::assign_allocation_ports(network.as_ref(), connect.as_ref(), &used)
                .with_context(|| format!("failed to assign ports on node {node_id}"))?;
        let address = tx
            .query_row(
                "SELECT config_attrs FROM node WHERE id = ?;",
                [node_id],
                |row| row.get::<_, Value>(0),
            )
            .ok()
            .and_then(|attrs| worker_address_host(&attrs));

        Ok(Some(crate::structs::AllocNetwork {
            address,
            ports,
            upstreams,
        }))
    }

    fn insert_allocation_with_data(
        &self,
        tx: &Connection,
        node_id: &str,
        task_id: i64,
        job_config_id: &str,
        resources: &Value,
        network: Option<&crate::structs::AllocNetwork>,
    ) -> R<Allocation> {
        let network_json = network.map(to_string).transpose()?;
        // Stateful tasks: pick (or re-materialize) the instance this
        // allocation will fulfill before writing the row, so the binding is
        // atomic with the placement.
        let instance_id = self.assign_stateful_instance_tx(tx, task_id, node_id)?;

        let params = named_params! {
            ":status": "placed",
            ":resources": resources,
            ":node_id": node_id,
            ":job_config_id": job_config_id,
            ":task_config_id": task_id,
            ":instance_id": instance_id,
            ":network": network_json,
            // Placement time, for `preemption.victimExemptAge`: an
            // allocation that just started is never a victim, which is what
            // keeps a preemption from immediately undoing the placement a
            // previous one made.
            ":created_at_unix": OffsetDateTime::now_utc().unix_timestamp(),
        };

        let allocation = tx.query_row(&INSERT_ALLOCATION, params, |row| {
            self.allocation_from_row(row)
        })?;

        if let Some(instance_id) = allocation.instance_id.as_deref() {
            let now = Self::now_rfc3339()?;
            tx.execute(
                "UPDATE instance SET state = 'active', current_alloc_id = ?1, updated_at = ?2 \
                 WHERE instance_id = ?3;",
                params![allocation.id, now, instance_id],
            )?;
        }

        Ok(allocation)
    }

    /// For a stateful task, pick the instance the new allocation will fulfill.
    /// This is the single choke point all placement paths flow through, so the
    /// instance binding is atomic with the allocation insert. Returns `None`
    /// for stateless tasks. Must run inside the same transaction that inserts
    /// the allocation.
    ///
    /// - Slot mode: the lowest slot `0..count.num-1` not currently fulfilled
    ///   by an allocation of this task config; the instance row is created or
    ///   re-activated as needed (slot ids are reused within the pool).
    /// - Keyed mode: any live (non-terminated) keyed instance of this
    ///   (job, group, task) not currently bound to an allocation of this task
    ///   config. Instances only come into existence via CreateInstance; this
    ///   never creates one. Keyed ids are not ordinals, so "any unbound" is
    ///   correct — a rescheduling instance is simply the unbound one.
    fn assign_stateful_instance_tx(
        &self,
        tx: &Connection,
        task_id: i64,
        node_id: &str,
    ) -> R<Option<String>> {
        let (job_id, group, task_name, count, stateful): (
            String,
            String,
            String,
            i64,
            Option<String>,
        ) = tx.query_row(
            "SELECT job_id, group_name, name, count, stateful FROM task_config WHERE id = ?;",
            [task_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;

        let Some(stateful) = stateful else {
            return Ok(None);
        };
        let stateful: StatefulConfig = serde_json::from_str(&stateful)
            .with_context(|| format!("invalid stateful config for task {task_name}"))?;
        if stateful.mode == InstanceMode::Keyed {
            return self
                .assign_keyed_instance_tx(
                    tx, task_id, &job_id, &group, &task_name, &stateful, node_id,
                )
                .map(Some);
        }

        // Slots already fulfilled by live allocations of this task config.
        // Scoped per config so a create-first rollout's new allocations can
        // take over instances still referenced by the outgoing config.
        let mut taken_stmt = tx.prepare(
            "SELECT instance.slot_index FROM allocation \
             INNER JOIN instance ON instance.instance_id = allocation.instance_id \
             WHERE allocation.task_config_id = ?1 AND allocation.instance_id IS NOT NULL;",
        )?;
        let taken: HashSet<u32> = Self::collect_rows(
            taken_stmt.query_map([task_id], |row| row.get::<_, Option<i64>>(0))?,
        )?
        .into_iter()
        .flatten()
        .map(|slot| slot as u32)
        .collect();

        // Slots occupied by a live allocation of ANY config of this job
        // (e.g. the outgoing config mid-rollout). Preferring slots outside
        // this set makes the binder pick exactly the slot whose old
        // allocation was just retired, which is what lets a sequential
        // (`rollout.slots.order`) rollout replace highest-slot-first and what
        // keeps a staged (`updateFrom`) rollout from double-binding a slot
        // whose instance still runs the old config. The per-config `taken`
        // set above remains the hard constraint; the job-wide set is only a
        // preference so the fresh-plan-then-remove-previous submit flow
        // (which deliberately places new-config allocations while the old
        // config's are still live) keeps working.
        let mut job_taken_stmt = tx.prepare(
            "SELECT instance.slot_index FROM allocation \
             INNER JOIN instance ON instance.instance_id = allocation.instance_id \
             WHERE instance.job_id = ?1 AND instance.group_name = ?2 \
             AND instance.mode = 'slot';",
        )?;
        let job_taken: HashSet<u32> = Self::collect_rows(
            job_taken_stmt.query_map(params![job_id, group], |row| row.get::<_, Option<i64>>(0))?,
        )?
        .into_iter()
        .flatten()
        .map(|slot| slot as u32)
        .collect();

        let desired = count.max(0) as u32;
        // Node-local storage makes slot choice matter: an allocation that
        // landed on node-a1 must bind the slot whose data is ON node-a1, not
        // just the lowest free one. Preferring the pinned slot makes the
        // planner's pin and the binder's choice agree even if their orderings
        // diverge — the allocation always meets its own data.
        let pinned_slot = (0..desired)
            .filter(|slot| !taken.contains(slot))
            .find(|slot| {
                let candidate = slot_instance_id(&job_id, &group, *slot);
                matches!(
                    self.pinned_node_for_instance_tx(tx, &candidate),
                    Ok(Some(pinned)) if pinned == node_id
                )
            });
        let slot = pinned_slot
            .or_else(|| {
                (0..desired).find(|slot| !taken.contains(slot) && !job_taken.contains(slot))
            })
            .or_else(|| (0..desired).find(|slot| !taken.contains(slot)))
            .with_context(|| {
                format!(
                "no free slot for stateful task {task_name} (group {group}); all {desired} slots \
                 already have allocations for this job config"
            )
            })?;

        let instance_id = slot_instance_id(&job_id, &group, slot);
        let now = Self::now_rfc3339()?;
        let existing = tx
            .query_row(&GET_INSTANCE, [instance_id.as_str()], |row| {
                self.instance_from_row(row)
            })
            .optional()?;

        match existing {
            Some(existing) => {
                // The slot id space is EXCLUSIVE: a row carrying this derived id
                // must be this very (job, group, task, slot) slot. If a row with
                // the same id exists but belongs to a different job/group/slot or
                // a different mode (e.g. a keyed instance some caller created with
                // `--id otherjob-default-0`), adopting it would let two jobs share
                // one instance row, storage handle, and workload-identity subject.
                // Refuse — never hijack. (Cross-job collisions are prevented at
                // create time too, see `keyed_id_collides_with_slot_pattern`, so
                // this is defence in depth.)
                if existing.mode != InstanceMode::Slot
                    || existing.job_id != job_id
                    || existing.group != group
                    || existing.slot_index != Some(slot)
                {
                    bail!(StorageError::Conflict {
                        message: format!(
                            "slot instance id {instance_id} is already owned by a different \
                             instance (job {}, group {}, mode {:?}, slot {:?}); slot ids are \
                             exclusive to their (job, group, slot) and cannot be adopted",
                            existing.job_id, existing.group, existing.mode, existing.slot_index
                        ),
                    });
                }
                // Re-activation handles both reschedule (Rescheduling -> Active
                // via the caller's binding update) and scale-out of a retained
                // Terminated slot: the same id and storage handle come back.
                // Mint a handle only if none was retained and the task (still)
                // declares storage.
                let storage_handle = match existing.storage_handle {
                    Some(handle) => Some(handle),
                    None => self.mint_instance_volume_tx(
                        tx,
                        &instance_id,
                        &job_id,
                        &task_name,
                        stateful.storage.as_ref(),
                    )?,
                };
                tx.execute(
                    "UPDATE instance SET task_name = ?1, storage_handle = ?2, updated_at = ?3 \
                     WHERE instance_id = ?4;",
                    params![task_name, storage_handle, now, instance_id],
                )?;
            }
            None => {
                let storage_handle = self.mint_instance_volume_tx(
                    tx,
                    &instance_id,
                    &job_id,
                    &task_name,
                    stateful.storage.as_ref(),
                )?;
                tx.execute(
                    "INSERT INTO instance (instance_id, job_id, group_name, task_name, mode, \
                     state, slot_index, current_alloc_id, storage_handle, created_at, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, 'slot', 'pending', ?5, NULL, ?6, ?7, ?7);",
                    params![
                        instance_id,
                        job_id,
                        group,
                        task_name,
                        slot,
                        storage_handle,
                        now
                    ],
                )?;
            }
        }

        Ok(Some(instance_id))
    }

    /// Keyed-mode arm of [`Self::assign_stateful_instance_tx`]: bind the new
    /// allocation to an unbound live keyed instance of (job, group, task).
    ///
    /// "Unbound" is scoped per task config (like slots) so a create-first
    /// rollout's new allocations can take over instances still referenced by
    /// the outgoing config. The oldest unbound instance is picked for
    /// determinism; since keyed ids are not ordinals, any unbound instance is
    /// a correct choice. Stopped instances are never bound (they are paused,
    /// not awaiting placement), and terminated ones are tombstones. A storage
    /// handle is minted on first placement when the task declares
    /// `stateful.storage`, and the retained handle is reattached on every
    /// re-bind — that is what makes a replacement allocation *the same
    /// sandbox*.
    fn assign_keyed_instance_tx(
        &self,
        tx: &Connection,
        task_id: i64,
        job_id: &str,
        group: &str,
        task_name: &str,
        stateful: &StatefulConfig,
        node_id: &str,
    ) -> R<String> {
        // Unbound live instances of this (job, group, task), oldest first.
        // Ordering by `node_local_pin DESC` first makes an allocation bind the
        // instance whose node-local data already lives on the node it landed
        // on; without a pin the ordering is unchanged (oldest wins), so keyed
        // binding without volumes behaves exactly as before.
        let instance: Option<Instance> = tx
            .query_row(
                "SELECT instance.instance_id, instance.job_id, instance.group_name, \
                 instance.task_name, instance.mode, instance.state, instance.slot_index, \
                 instance.current_alloc_id, instance.storage_handle, instance.created_at, \
                 instance.updated_at FROM instance \
                 LEFT JOIN volume ON volume.instance_id = instance.instance_id \
                 AND volume.node_local = 1 AND volume.state IN ('pending', 'created') \
                 AND instance.storage_handle = volume.driver || '/' || volume.volume_id \
                 WHERE instance.job_id = ?1 AND instance.group_name = ?2 \
                 AND instance.task_name = ?3 \
                 AND instance.mode = 'keyed' AND instance.state NOT IN ('terminated', 'stopped') \
                 AND instance.instance_id NOT IN (\
                     SELECT instance_id FROM allocation \
                     WHERE task_config_id = ?4 AND instance_id IS NOT NULL\
                 ) \
                 ORDER BY (volume.node_id = ?5) DESC, instance.created_at ASC, \
                 instance.instance_id ASC LIMIT 1;",
                params![job_id, group, task_name, task_id, node_id],
                |row| self.instance_from_row(row),
            )
            .optional()?;

        let instance = instance.with_context(|| {
            format!(
                "no unbound keyed instance for task {task_name} (group {group}, job {job_id}); \
                 keyed allocations are driven by CreateInstance, not count"
            )
        })?;

        if instance.storage_handle.is_none() {
            if let Some(handle) = self.mint_instance_volume_tx(
                tx,
                &instance.instance_id,
                job_id,
                task_name,
                stateful.storage.as_ref(),
            )? {
                let now = Self::now_rfc3339()?;
                tx.execute(
                    "UPDATE instance SET storage_handle = ?1, updated_at = ?2 \
                     WHERE instance_id = ?3;",
                    params![handle, now, instance.instance_id],
                )?;
            }
        }

        Ok(instance.instance_id)
    }

    /// Record why a set of allocations is being removed, in the same
    /// transaction that removes them.
    ///
    /// Same-transaction is the whole point: a notice can then never describe
    /// a stop that was rolled back, and never arrive claiming a stop that has
    /// not been decided yet. The heartbeat feed
    /// ([`Self::stop_notices_for_node`]) reads them straight back out.
    ///
    /// [`StopCause::Unknown`] writes nothing — an unknown cause and no
    /// recorded cause are the same fact, and the worker defaults to
    /// `Unknown` anyway.
    /// Record why these allocations are stopping, and stage the
    /// `allocation.stopping` event that announces their grace window.
    ///
    /// Returns the staged event for the caller to publish after commit —
    /// staged rather than emitted inline because these are removal-path
    /// transactions and the event stream's performance rules require it.
    #[must_use = "the staged allocation.stopping event must be published after commit"]
    fn record_stop_notices_tx(
        &self,
        tx: &Connection,
        removed: &[Allocation],
        cause: StopCause,
    ) -> R<Option<SchedulerEventRecord>> {
        self.record_stop_notices_with_preemptor_tx(tx, removed, cause, None)?;
        Ok(self.stage_stopping_event_tx(tx, removed, cause))
    }

    /// [`Self::record_stop_notices_tx`], additionally recording *who* took
    /// the capacity. Only preemption has an answer, so every other caller
    /// goes through the two-argument form.
    fn record_stop_notices_with_preemptor_tx(
        &self,
        tx: &Connection,
        removed: &[Allocation],
        cause: StopCause,
        preemptor: Option<(&str, Band)>,
    ) -> R<()> {
        if removed.is_empty() || !cause.is_known() {
            return Ok(());
        }
        let now = Self::now_rfc3339()?;
        let now_unix = OffsetDateTime::now_utc().unix_timestamp();

        // Expire old notices here rather than on the heartbeat read path.
        // This transaction is already a scheduler-lane write, so the delete
        // costs no extra raft proposal and stays inside the lane that owns
        // the table; pruning on read would have put a write (and, in raft
        // mode, a proposal) on the hottest path in the system. A table with
        // no removals is not growing, so tying the prune to removals cannot
        // let it run away.
        tx.execute(
            "DELETE FROM allocation_stop_notice WHERE created_at_unix < ?1;",
            params![now_unix - STOP_NOTICE_RETENTION_SECS],
        )?;

        let (preemptor_job, preemptor_band) = match preemptor {
            Some((job, band)) => (Some(job.to_string()), Some(band.as_str().to_string())),
            None => (None, None),
        };

        for alloc in removed {
            // REPLACE, not INSERT: an allocation id is unique and a second
            // removal of the same id (a retried apply, say) should carry the
            // latest cause rather than fail the transaction it rides in.
            tx.execute(
                "INSERT OR REPLACE INTO allocation_stop_notice
                 (allocation_id, node_id, cause, created_at, created_at_unix,
                  preemptor_job, preemptor_band)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
                params![
                    alloc.id,
                    alloc.node_id,
                    cause.as_str(),
                    now,
                    now_unix,
                    preemptor_job,
                    preemptor_band
                ],
            )?;
        }
        Ok(())
    }

    /// Stage the `allocation.stopping` event for a set of removals.
    ///
    /// Separate from [`Self::record_stop_notices_tx`] because the caller owns
    /// event publication: staged events must be handed to `publish_staged`
    /// *after* the transaction commits, and the removal paths already do that
    /// for their own `allocation.removed` drafts.
    ///
    /// The grace window in the event is the manager's *expectation*, derived
    /// from each task's `stop.killAfter`. The worker owns the real clock —
    /// its window opens when it actually delivers the signal, which is up to
    /// one heartbeat later.
    fn stage_stopping_event_tx(
        &self,
        tx: &Connection,
        removed: &[Allocation],
        cause: StopCause,
    ) -> Option<SchedulerEventRecord> {
        if removed.is_empty() || !cause.is_known() {
            return None;
        }
        // One statement for the whole batch: `task_config` rows outlive the
        // allocations that reference them (they belong to the job config), so
        // this is safe whether the caller has already issued its DELETEs or
        // not.
        let grace = self
            .stop_grace_by_task_tx(tx, removed)
            .unwrap_or_else(|err| {
                debug!(error = ?err, "Failed reading stop ladders for allocation.stopping");
                HashMap::new()
            });
        drafts::allocations_stopping(removed, cause, &grace)
            .and_then(|draft| self.stage_event_locked(tx, draft))
    }

    /// `stop.killAfter` seconds per task config id, for the allocations
    /// given. Tasks with no `stop` block fall back to the built-in default.
    fn stop_grace_by_task_tx(
        &self,
        tx: &Connection,
        removed: &[Allocation],
    ) -> R<HashMap<i64, u64>> {
        let task_ids: BTreeSet<i64> = removed
            .iter()
            .map(|alloc| alloc.task_config_id)
            .collect::<BTreeSet<_>>();
        if task_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let placeholders = std::iter::repeat_n("?", task_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("SELECT id, stop FROM task_config WHERE id IN ({placeholders});");
        let mut stmt = tx.prepare(&sql)?;
        let rows =
            Self::collect_rows(stmt.query_map(params_from_iter(task_ids.iter()), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?))
            })?)?;
        Ok(rows
            .into_iter()
            .map(|(id, raw)| {
                let stop = raw
                    .and_then(|raw| serde_json::from_str::<crate::structs::StopConfig>(&raw).ok())
                    .unwrap_or_default();
                (id, stop.kill_after.as_secs())
            })
            .collect())
    }

    /// The live stop notices for one node, for its heartbeat reply.
    ///
    /// Read-only by construction: this runs on every heartbeat from every
    /// node, so it must not write (expiry happens in
    /// [`Self::record_stop_notices_tx`], which is already a scheduler-lane
    /// write). Notices past the retention window are filtered out here so a
    /// prune that has not run yet cannot resurrect a stale cause.
    pub fn stop_notices_for_node(&self, node_id: &str) -> R<Vec<StoredStopNotice>> {
        let cutoff = OffsetDateTime::now_utc().unix_timestamp() - STOP_NOTICE_RETENTION_SECS;

        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT allocation_id, cause, preemptor_job, preemptor_band
             FROM allocation_stop_notice
             WHERE node_id = ?1 AND created_at_unix >= ?2;",
        )?;
        let notices = Self::collect_rows(stmt.query_map(params![node_id, cutoff], |row| {
            Ok(StoredStopNotice {
                allocation_id: row.get::<_, i64>(0)?,
                cause: StopCause::from_wire(&row.get::<_, String>(1)?),
                preemptor_job: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                preemptor_band: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
            })
        })?)?;
        drop(stmt);
        drop(conn);

        Ok(notices)
    }

    /// After allocation rows are deleted, transition the instances they
    /// fulfilled. Instances still referenced by another live allocation (for
    /// example the already-placed replacement during a config update) keep
    /// running and just re-point `current_alloc_id`. Orphaned slot instances
    /// become `rescheduling` while their slot is still desired, or
    /// `terminated` when the latest job config no longer wants the slot
    /// (scale-in) — retaining the row, and the storage handle unless
    /// `retain_on_scale_in` is false. Orphaned keyed instances become
    /// `rescheduling`: allocation churn never terminates or stops a keyed
    /// instance, only DestroyInstance/StopInstance (or a job stop) does.
    /// `Stopped` rows are skipped exactly like `Terminated` ones — the
    /// stop/destroy paths set those states in the SAME transaction that
    /// removes the allocations, so this hook can never flip a deliberately
    /// stopped instance back to `rescheduling` (nor can a later, unrelated
    /// allocation death resurrect it). Must run in the same transaction as
    /// the deletion, before commit.
    fn release_instance_bindings_tx(&self, tx: &Connection, removed: &[Allocation]) -> R<()> {
        let instance_ids: HashSet<&str> = removed
            .iter()
            .filter_map(|alloc| alloc.instance_id.as_deref())
            .collect();
        if instance_ids.is_empty() {
            return Ok(());
        }

        let now = Self::now_rfc3339()?;
        for instance_id in instance_ids {
            let surviving_alloc: Option<i64> = tx
                .query_row(
                    "SELECT MAX(id) FROM allocation WHERE instance_id = ?;",
                    [instance_id],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();

            if let Some(alloc_id) = surviving_alloc {
                tx.execute(
                    "UPDATE instance SET current_alloc_id = ?1, updated_at = ?2 \
                     WHERE instance_id = ?3 AND current_alloc_id IS NOT ?1;",
                    params![alloc_id, now, instance_id],
                )?;
                continue;
            }

            let instance = tx
                .query_row(&GET_INSTANCE, [instance_id], |row| {
                    self.instance_from_row(row)
                })
                .optional()?;
            let Some(instance) = instance else {
                continue;
            };
            if matches!(
                instance.state,
                InstanceState::Terminated | InstanceState::Stopped
            ) {
                continue;
            }

            let (still_desired, retain_storage) = self.slot_still_desired_tx(tx, &instance)?;
            if still_desired {
                tx.execute(
                    "UPDATE instance SET state = 'rescheduling', current_alloc_id = NULL, \
                     updated_at = ?1 WHERE instance_id = ?2;",
                    params![now, instance_id],
                )?;
            } else {
                let storage_handle = if retain_storage {
                    instance.storage_handle.clone()
                } else {
                    None
                };
                info!(
                    instance_id,
                    job_id = %instance.job_id,
                    retain_storage,
                    "Terminating scaled-in instance"
                );
                tx.execute(
                    "UPDATE instance SET state = 'terminated', current_alloc_id = NULL, \
                     storage_handle = ?1, updated_at = ?2 WHERE instance_id = ?3;",
                    params![storage_handle, now, instance_id],
                )?;
            }
        }

        Ok(())
    }

    /// Whether the instance's slot is still inside the latest job config's
    /// desired range, plus the effective storage retention policy. Keyed
    /// instances are always "still desired" here — their lifecycle is owned by
    /// the management API (DestroyInstance), never by allocation churn.
    fn slot_still_desired_tx(&self, tx: &Connection, instance: &Instance) -> R<(bool, bool)> {
        let (InstanceMode::Slot, Some(slot)) = (instance.mode, instance.slot_index) else {
            return Ok((true, true));
        };

        // COALESCE overlays a `legion job scale` override onto the spec
        // count (docs/autoscaling.md) — this is how slot scale-in flows:
        // the override shrinks the desired range, `retire_surplus_allocations`
        // deletes the highest slots' allocations, and this check terminates
        // their instances (honoring `retainOnScaleIn` below).
        let latest_task: Option<(i64, Option<String>)> = tx
            .query_row(
                "SELECT COALESCE(task_scale.count, task_config.count), task_config.stateful \
                 FROM task_config \
                 INNER JOIN job_config ON job_config.id = task_config.job_config_id \
                 LEFT JOIN task_scale ON task_scale.job_id = task_config.job_id \
                 AND task_scale.task_group = task_config.group_name \
                 AND task_scale.task_name = task_config.name \
                 WHERE task_config.job_id = ?1 AND task_config.group_name = ?2 \
                 AND task_config.name = ?3 \
                 ORDER BY job_config.version DESC LIMIT 1;",
                params![instance.job_id, instance.group, instance.task_name],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        let Some((count, stateful)) = latest_task else {
            // Task disappeared from the job entirely: terminate, retain storage.
            return Ok((false, true));
        };

        let retain = stateful
            .as_deref()
            .and_then(|raw| serde_json::from_str::<StatefulConfig>(raw).ok())
            .and_then(|stateful| stateful.storage)
            .map(|storage| storage.retains_on_scale_in())
            .unwrap_or(true);

        Ok(((slot as i64) < count, retain))
    }

    fn load_allocation_constraints(
        &self,
        tx: &Connection,
        task_id: i64,
    ) -> R<AllocationConstraintState> {
        let (constraints, permit): (Value, Value) = tx.query_row(
            "SELECT constraints, permit FROM task_config WHERE id = ?;",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let constraint_set = ConstraintSet::new(&constraints)
            .with_context(|| format!("Failed to parse constraints for task {task_id}"))?;
        let permit_set = PermitSet::new(&permit)
            .with_context(|| format!("Failed to parse permit for task {task_id}"))?;
        let distinct_keys = constraint_set.distinct_keys();
        let distinct = if distinct_keys.is_empty() {
            None
        } else {
            let mut tracker = DistinctTracker::new(&distinct_keys);
            let sql = "SELECT node.id, node.pool, COALESCE(node_health.status, 'active'), \
                node.availability, node.config_attrs, node.fingerprint_attrs, \
                node.dynamic_attrs, COALESCE(node_health.health_checks, '{}'), \
                COALESCE(node_health.last_heartbeat, '') \
                FROM node INNER JOIN allocation ON allocation.node_id = node.id \
                LEFT JOIN node_health ON node_health.node_id = node.id \
                WHERE allocation.task_config_id = ?;";
            let mut stmt = tx.prepare(sql)?;
            let rows = stmt.query_map([task_id], |row| self.node_from_row(row))?;
            for row in rows {
                tracker.insert_from_node(&row?);
            }
            Some(tracker)
        };

        Ok(AllocationConstraintState {
            constraints: constraint_set,
            permit: permit_set,
            distinct,
        })
    }

    fn load_allocation_task_state(&self, tx: &Connection, task_id: i64) -> R<AllocationTaskState> {
        let (job_config_id, resources, constraints, permit, network_raw, connect_raw): (
            String,
            Value,
            Value,
            Value,
            Option<String>,
            Option<String>,
        ) = tx.query_row(
            "SELECT job_config_id, resources, constraints, permit, network, connect FROM task_config WHERE id = ?;",
            [task_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )?;

        let constraint_set = ConstraintSet::new(&constraints)
            .with_context(|| format!("Failed to parse constraints for task {task_id}"))?;
        let permit_set = PermitSet::new(&permit)
            .with_context(|| format!("Failed to parse permit for task {task_id}"))?;
        let distinct_keys = constraint_set.distinct_keys();
        let distinct = if distinct_keys.is_empty() {
            None
        } else {
            let mut tracker = DistinctTracker::new(&distinct_keys);
            let sql = "SELECT node.id, node.pool, COALESCE(node_health.status, 'active'), \
                node.availability, node.config_attrs, node.fingerprint_attrs, \
                node.dynamic_attrs, COALESCE(node_health.health_checks, '{}'), \
                COALESCE(node_health.last_heartbeat, '') \
                FROM node INNER JOIN allocation ON allocation.node_id = node.id \
                LEFT JOIN node_health ON node_health.node_id = node.id \
                WHERE allocation.task_config_id = ?;";
            let mut stmt = tx.prepare(sql)?;
            let rows = stmt.query_map([task_id], |row| self.node_from_row(row))?;
            for row in rows {
                tracker.insert_from_node(&row?);
            }
            Some(tracker)
        };

        Ok(AllocationTaskState {
            constraints: constraint_set,
            permit: permit_set,
            distinct,
            job_config_id,
            resources,
            network_raw,
            connect_raw,
        })
    }

    #[instrument(err, skip_all)]
    pub fn apply_allocation_plan(&self, placements: &[(String, i64)]) -> R<Vec<Allocation>> {
        if placements.is_empty() {
            return Ok(Vec::new());
        }
        let placements = placements.to_vec();
        self.replicated(Lane::Scheduler, "apply_allocation_plan", move |s| {
            s.apply_allocation_plan_apply(&placements)
        })
    }

    fn apply_allocation_plan_apply(&self, placements: &[(String, i64)]) -> R<Vec<Allocation>> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let mut allocations = Vec::new();

        let mut constraint_states: HashMap<i64, AllocationConstraintState> = HashMap::new();

        for placement in placements {
            let (node_id, task_id) = (&placement.0, placement.1);
            let state = match constraint_states.entry(task_id) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert(self.load_allocation_constraints(&tx, task_id)?)
                }
            };

            let node = tx.query_row(&GET_NODE, [node_id], |row| self.node_from_row(row))?;
            if !state
                .constraints
                .is_satisfied(&node, state.distinct.as_ref())
                || !state.permit.is_permitted(&node)
            {
                bail!(
                    "Node {} does not satisfy constraints for task {}",
                    node_id,
                    task_id
                );
            }

            if let Some(tracker) = state.distinct.as_mut() {
                tracker.insert_from_node(&node);
            }

            let allocation = self.insert_allocation(&tx, node_id, task_id)?;
            allocations.push(allocation);
        }

        self.bump_epochs_for_allocations_tx(&tx, &allocations)?;
        tx.commit()?;

        self.cache_add_allocations(&allocations)?;

        Ok(allocations)
    }

    #[instrument(err, skip_all)]
    pub fn count_allocations_for_task(&self, task_id: i64) -> R<u32> {
        let conn = self.read_connection()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM allocation WHERE task_config_id = ?;",
            [task_id],
            |row| row.get(0),
        )?;

        Ok(count as u32)
    }

    #[instrument(err, skip_all)]
    pub fn allocs_for_job(&self, job_id: &str) -> R<Vec<Allocation>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_ALLOCS_FOR_JOB.as_str())?;
        let alloc_iter = stmt.query_map([job_id], |row| self.allocation_from_row(row))?;

        let mut allocs = Vec::new();
        for alloc in alloc_iter {
            allocs.push(alloc?);
        }

        Ok(allocs)
    }

    #[instrument(err, skip_all)]
    pub fn allocs_for_job_config(&self, job_config_id: &str) -> R<Vec<Allocation>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_ALLOCS_FOR_JOB_CONFIG.as_str())?;
        let alloc_iter = stmt.query_map([job_config_id], |row| self.allocation_from_row(row))?;

        let mut allocs = Vec::new();
        for alloc in alloc_iter {
            allocs.push(alloc?);
        }

        Ok(allocs)
    }

    #[instrument(err, skip_all)]
    pub fn allocs_for_node(&self, node_id: &str) -> R<Vec<Allocation>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_ALLOCS_FOR_NODE.as_str())?;
        let alloc_iter = stmt.query_map([node_id], |row| self.allocation_from_row(row))?;

        let mut allocs = Vec::new();
        for alloc in alloc_iter {
            allocs.push(alloc?);
        }

        Ok(allocs)
    }

    #[instrument(err, skip_all)]
    pub fn get_allocation_by_id(&self, allocation_id: i64) -> R<Option<Allocation>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_ALLOCATION_BY_ID.as_str())?;
        let allocation = stmt
            .query_row([allocation_id], |row| self.allocation_from_row(row))
            .optional()?;
        Ok(allocation)
    }

    #[instrument(err, skip_all)]
    pub fn upsert_allocation_health_snapshots(
        &self,
        node_id: &str,
        snapshots: &[AllocationHealth],
    ) -> R<()> {
        if snapshots.is_empty() {
            return Ok(());
        }
        match self.upsert_allocation_health_snapshots_local(node_id, snapshots) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                crate::storage::raft::forward::ForwardableLaneWrite::AllocationHealthSnapshots {
                    node_id: node_id.to_owned(),
                    snapshots: snapshots.to_vec(),
                },
                err,
            ),
            other => other,
        }
    }

    pub(crate) fn upsert_allocation_health_snapshots_local(
        &self,
        node_id: &str,
        snapshots: &[AllocationHealth],
    ) -> R<()> {
        if snapshots.is_empty() {
            return Ok(());
        }
        let node_id = node_id.to_owned();
        let snapshots = snapshots.to_vec();
        self.replicated(
            Lane::Status,
            "upsert_allocation_health_snapshots",
            move |s| s.upsert_allocation_health_snapshots_apply(&node_id, &snapshots),
        )
    }

    fn upsert_allocation_health_snapshots_apply(
        &self,
        node_id: &str,
        snapshots: &[AllocationHealth],
    ) -> R<()> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let now = Self::now_rfc3339()?;
        let mut staged_events = Vec::new();
        for snapshot in snapshots {
            // Artifact staging transitions are derived by diffing against
            // the previously persisted snapshot, inside this transaction so
            // events commit atomically with the state they describe. The
            // extra read only happens for snapshots carrying artifact rows;
            // plain tasks stay on the single-statement path.
            let previous_states: Option<String> = if snapshot.artifact_states.is_some() {
                tx.query_row(
                    "SELECT artifact_states FROM allocation_health WHERE allocation_id = ?1;",
                    [snapshot.allocation_id],
                    |row| row.get(0),
                )
                .optional()?
                .flatten()
            } else {
                None
            };

            let inserted = tx.execute(
                "INSERT INTO allocation_health (
                    allocation_id,
                    node_id,
                    healthy,
                    status,
                    updated_at,
                    artifact_states
                )
                SELECT ?1, ?2, ?3, ?4, ?5, ?6
                WHERE EXISTS (SELECT 1 FROM allocation WHERE id = ?1 AND node_id = ?2)
                ON CONFLICT(allocation_id) DO UPDATE
                SET node_id = excluded.node_id,
                    healthy = excluded.healthy,
                    status = excluded.status,
                    updated_at = excluded.updated_at,
                    artifact_states = excluded.artifact_states;",
                params![
                    snapshot.allocation_id,
                    node_id,
                    i64::from(snapshot.healthy),
                    snapshot.status.as_str(),
                    now.as_str(),
                    snapshot.artifact_states.as_deref()
                ],
            )?;

            let Some(new_states) = snapshot.artifact_states.as_deref() else {
                continue;
            };
            // The guarded insert writes nothing for unknown allocations;
            // skip events for those too.
            if inserted == 0 {
                continue;
            }
            let (transitions, rows) = crate::storage::events::artifact_transitions(
                previous_states.as_deref(),
                new_states,
            );
            if !transitions.staging_started && !transitions.all_staged {
                continue;
            }
            let job_id = self
                .job_id_for_allocation_tx(&tx, snapshot.allocation_id)?
                .unwrap_or_default();
            if transitions.staging_started {
                staged_events.push(self.stage_event_locked(
                    &tx,
                    drafts::artifact_staging(snapshot.allocation_id, node_id, &job_id, &rows),
                ));
            }
            if transitions.all_staged {
                staged_events.push(self.stage_event_locked(
                    &tx,
                    drafts::artifact_staged(snapshot.allocation_id, node_id, &job_id, &rows),
                ));
            }
        }
        tx.commit()?;
        for event in staged_events {
            self.publish_staged(event);
        }
        Ok(())
    }

    /// Job ID owning an allocation, resolved inside an open transaction (the
    /// allocation table stores job_config_id; the job comes via join). Only
    /// consulted when an artifact staging transition actually fires.
    fn job_id_for_allocation_tx(&self, tx: &Connection, allocation_id: i64) -> R<Option<String>> {
        let job_id = tx
            .query_row(
                "SELECT job_config.job_id FROM allocation
                 INNER JOIN job_config ON job_config.id = allocation.job_config_id
                 WHERE allocation.id = ?1;",
                [allocation_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(job_id)
    }

    /// Re-run the reconcile pass for every job config whose rollout is
    /// incomplete.
    ///
    /// A wrapper-level SEQUENCE, not a capture (JC-33/JC-35): each
    /// [`Self::reconcile_job_config_allocations`] below is itself a sequence
    /// of replicated ops that must read its predecessor's committed effects,
    /// and one of them writes the status-owned `task_status`. Composing them
    /// into a single scheduler capture would both blind the scheduler's reads
    /// and put a status-lane write inside a scheduler changeset, which the
    /// capture-time ownership guard rejects. Every pass is an idempotent
    /// convergence step, so no cross-op atomicity is needed.
    #[instrument(err, skip_all)]
    pub fn reconcile_incomplete_rollouts(&self) -> R<u64> {
        let mut reconciled = 0_u64;
        for job_config_id in self.job_config_ids_with_incomplete_rollouts()? {
            match self.reconcile_job_config_allocations(&job_config_id) {
                Ok(()) => {
                    reconciled += 1;
                }
                Err(err) => {
                    warn!(
                        job_config_id = %job_config_id,
                        error = ?err,
                        "failed reconciling completed_incomplete rollout"
                    );
                }
            }
        }
        Ok(reconciled)
    }

    #[instrument(err, skip_all)]
    pub fn allocation_health_for_allocations(
        &self,
        allocation_ids: &[i64],
    ) -> R<HashMap<i64, AllocationHealthSnapshot>> {
        if allocation_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let placeholders = std::iter::repeat_n("?", allocation_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT allocation_id, node_id, healthy, status, updated_at, artifact_states
             FROM allocation_health
             WHERE allocation_id IN ({placeholders});"
        );
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(allocation_ids.iter()), |row| {
            self.allocation_health_snapshot_from_row(row)
        })?;
        let snapshots = Self::collect_rows(rows)?;
        Ok(snapshots
            .into_iter()
            .map(|snapshot| (snapshot.allocation_id, snapshot))
            .collect())
    }

    /// The catalog projection behind [`Store::service_endpoints`], optionally
    /// narrowed to one service name (bound as `?1`).
    ///
    /// Split out so the plan-shape test can `EXPLAIN QUERY PLAN` the *real*
    /// query: the name predicate has to stay byte-identical to the expression
    /// `idx_task_config_service_name` indexes, or the planner quietly falls
    /// back to a cluster-wide scan on the heartbeat path.
    fn service_endpoints_sql(filtered: bool) -> String {
        let mut sql = String::from(
            "SELECT tc.name, tc.service, tc.job_id,
                    a.id, a.node_id, a.network,
                    ah.healthy, ah.status,
                    CASE WHEN n.id IS NULL THEN NULL
                         ELSE COALESCE(nh.status, 'active') END,
                    n.config_attrs
             FROM task_config tc
             INNER JOIN allocation a ON a.task_config_id = tc.id
             LEFT JOIN allocation_health ah ON ah.allocation_id = a.id
             LEFT JOIN node n ON n.id = a.node_id
             LEFT JOIN node_health nh ON nh.node_id = n.id
             WHERE tc.service IS NOT NULL",
        );
        if filtered {
            sql.push_str(
                " AND json_valid(tc.service)
                  AND COALESCE(json_extract(tc.service, '$.name'), tc.name) = ?1",
            );
        }
        sql.push(';');
        sql
    }

    #[instrument(err, skip_all)]
    /// One resolvable endpoint of a declared service, derived (never
    /// registered) from task config + allocation + health + node state.
    ///
    /// `service_filter` is applied **in SQL**, not after deserializing the
    /// catalog: `ResolveService` and the per-heartbeat connect upstream feed
    /// only ever want one service, and filtering in Rust made a single-service
    /// lookup cost one cluster-wide scan plus a `ServiceConfig` *and*
    /// `AllocNetwork` JSON parse per catalog row — turning per-node heartbeat
    /// work into O(cluster). `COALESCE(json_extract(service,'$.name'), name)`
    /// is exactly [`crate::structs::ServiceConfig::service_name`], and
    /// `idx_task_config_service_name` indexes that expression, so a filtered
    /// call is an index seek over just that service's rows.
    ///
    /// The filtered form also skips rows whose stored `service` is not valid
    /// JSON (they cannot match a name anyway, and one hand-corrupted row must
    /// not take down every consumer's endpoint feed); the unfiltered form
    /// still surfaces them as an error.
    pub fn service_endpoints(
        &self,
        service_filter: Option<&str>,
    ) -> R<Vec<crate::structs::ServiceEndpointRow>> {
        let conn = self.read_connection()?;
        let sql = Self::service_endpoints_sql(service_filter.is_some());
        let mut stmt = conn.prepare(&sql)?;

        let rows = stmt.query_map(params_from_iter(service_filter.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<bool>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<Value>>(9)?,
            ))
        })?;

        let mut endpoints = Vec::new();
        for row in rows {
            let (
                task_name,
                service_raw,
                job_id,
                allocation_id,
                node_id,
                network_raw,
                alloc_healthy,
                health_status,
                node_status,
                node_config_attrs,
            ) = row?;

            let service: crate::structs::ServiceConfig = from_str(&service_raw)
                .with_context(|| format!("invalid stored service config for task {task_name}"))?;
            let service_name = service.service_name(&task_name).to_string();

            let network: Option<crate::structs::AllocNetwork> = network_raw
                .as_deref()
                .map(from_str)
                .transpose()
                .with_context(|| {
                    format!("invalid stored network for allocation {allocation_id}")
                })?;
            let port = service
                .port_label()
                .and_then(|label| network.as_ref().and_then(|network| network.port(label)))
                .map(|port| port.host_port);
            let address = network
                .as_ref()
                .and_then(|network| network.address.clone())
                .or_else(|| node_config_attrs.as_ref().and_then(worker_address_host));

            // Nodes report `active`/`ready` while alive; `lost` (or a missing
            // row) is the dead state. Endpoint liveness is leased on this.
            let node_alive = matches!(node_status.as_deref(), Some(status) if status != "lost");
            let healthy = alloc_healthy.unwrap_or(false) && node_alive;
            let health_status = if node_alive {
                health_status.unwrap_or_default()
            } else {
                format!(
                    "node {} is {}",
                    node_id,
                    node_status.as_deref().unwrap_or("unknown")
                )
            };

            endpoints.push(crate::structs::ServiceEndpointRow {
                service: service_name,
                job_id,
                task_name,
                allocation_id,
                node_id,
                address,
                port,
                healthy,
                health_status,
                tags: service.tags.clone(),
                // Intentions ride along on the row so the heartbeat upstream
                // feed can filter per consuming job without a second pass over
                // task configs. The catalog RPCs ignore this field.
                allow: service.allow.clone(),
                meta: service.meta_strings(),
                port_label: service.port_label().map(str::to_string),
            });
        }

        Ok(endpoints)
    }

    /// Services declared by the latest job config of each job, including ones
    /// with no allocations yet.
    pub fn declared_services(&self) -> R<Vec<DeclaredService>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT tc.name, tc.service, tc.job_id
             FROM task_config tc
             INNER JOIN job_config jc ON jc.id = tc.job_config_id
             INNER JOIN (
                 SELECT job_id, MAX(version) AS version FROM job_config GROUP BY job_id
             ) latest ON latest.job_id = jc.job_id AND latest.version = jc.version
             WHERE tc.service IS NOT NULL;",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;

        let mut declared = Vec::new();
        for row in rows {
            let (task_name, service_raw, job_id) = row?;
            let service: crate::structs::ServiceConfig = from_str(&service_raw)
                .with_context(|| format!("invalid stored service config for task {task_name}"))?;
            declared.push(DeclaredService {
                service: service.service_name(&task_name).to_string(),
                job_id,
                meta: service.meta_strings(),
                task_name,
                tags: service.tags,
                allow: service.allow,
            });
        }
        Ok(declared)
    }

    pub fn allocation_health_for_job_config(
        &self,
        job_config_id: &str,
    ) -> R<HashMap<i64, AllocationHealthSnapshot>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT ah.allocation_id, ah.node_id, ah.healthy, ah.status, ah.updated_at, \
                    ah.artifact_states
             FROM allocation_health ah
             INNER JOIN allocation a ON a.id = ah.allocation_id
             WHERE a.job_config_id = ?1;",
        )?;
        let rows = stmt.query_map([job_config_id], |row| {
            self.allocation_health_snapshot_from_row(row)
        })?;
        let snapshots = Self::collect_rows(rows)?;
        Ok(snapshots
            .into_iter()
            .map(|snapshot| (snapshot.allocation_id, snapshot))
            .collect())
    }

    /// Remove one allocation without recording why.
    ///
    /// The worker-failure-report path: the task is already gone, so there is
    /// no stop to explain. Callers that *are* stopping live work should use
    /// [`Self::remove_allocation_by_id_because`] so the owning worker learns
    /// the reason.
    #[instrument(err, skip_all)]
    pub fn remove_allocation_by_id(&self, allocation_id: i64) -> R<Option<Allocation>> {
        self.remove_allocation_by_id_because(allocation_id, StopCause::Unknown)
    }

    /// Remove one allocation, recording why for the owning worker.
    #[instrument(err, skip_all)]
    pub fn remove_allocation_by_id_because(
        &self,
        allocation_id: i64,
        cause: StopCause,
    ) -> R<Option<Allocation>> {
        match self.remove_allocation_by_id_local(allocation_id, cause) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::RemoveAllocationById {
                        allocation_id,
                        cause,
                    },
                    err,
                ),
            other => other,
        }
    }

    pub(crate) fn remove_allocation_by_id_local(
        &self,
        allocation_id: i64,
        cause: StopCause,
    ) -> R<Option<Allocation>> {
        self.replicated(Lane::Scheduler, "remove_allocation_by_id", move |s| {
            s.remove_allocation_by_id_apply(allocation_id, cause)
        })
    }

    fn remove_allocation_by_id_apply(
        &self,
        allocation_id: i64,
        cause: StopCause,
    ) -> R<Option<Allocation>> {
        let allocation = self.get_allocation_by_id(allocation_id)?;
        let Some(allocation) = allocation else {
            return Ok(None);
        };

        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        tx.execute("DELETE FROM allocation WHERE id = ?;", [allocation_id])?;
        self.release_instance_bindings_tx(&tx, std::slice::from_ref(&allocation))?;
        self.bump_job_config_epoch_tx(&tx, &allocation.job_config_id)?;
        let stopping_event =
            self.record_stop_notices_tx(&tx, std::slice::from_ref(&allocation), cause)?;
        tx.commit()?;
        self.publish_staged(stopping_event);

        self.cache_remove_allocations(std::slice::from_ref(&allocation))?;

        Ok(Some(allocation))
    }

    /// Record a terminal, non-failure outcome for an allocation: its task's
    /// process exited and the task's `restart` policy says that is the end of
    /// it (see docs/task-supervision.md).
    ///
    /// The row is marked `complete` rather than deleted. Deleting it would
    /// leave the task's desired count unmet, and the very next reconcile would
    /// place the batch task again — an infinite re-run loop. A completed
    /// allocation still occupies its slot; it is the worker that knows not to
    /// relaunch the process.
    ///
    /// The task's status follows the outcome: `Success` for a successful exit,
    /// `Failed` otherwise (a `restart: { policy: "never" }` task that exits
    /// non-zero terminates without going through the reschedule path).
    pub fn complete_allocation(&self, allocation_id: i64, success: bool) -> R<Option<Allocation>> {
        match self.complete_allocation_local(allocation_id, success) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::CompleteAllocation {
                        allocation_id,
                        success,
                    },
                    err,
                ),
            other => other,
        }
    }

    pub(crate) fn complete_allocation_local(
        &self,
        allocation_id: i64,
        success: bool,
    ) -> R<Option<Allocation>> {
        self.replicated(Lane::Scheduler, "complete_allocation", move |s| {
            s.complete_allocation_apply(allocation_id, success)
        })
    }

    fn complete_allocation_apply(
        &self,
        allocation_id: i64,
        success: bool,
    ) -> R<Option<Allocation>> {
        let Some(allocation) = self.get_allocation_by_id(allocation_id)? else {
            return Ok(None);
        };

        let status = if success {
            ALLOCATION_STATUS_COMPLETE
        } else {
            ALLOCATION_STATUS_FAILED
        };
        let task_status = if success {
            TaskStatusState::Success
        } else {
            TaskStatusState::Failed
        };

        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        tx.execute(
            "UPDATE allocation SET status = ? WHERE id = ?;",
            params![status, allocation_id],
        )?;
        let mut updates = HashMap::new();
        updates.insert(allocation.task_config_id, task_status);
        self.apply_task_status_updates_tx(&tx, &updates)?;
        tx.commit()?;

        Ok(Some(Allocation {
            status: status.to_string(),
            ..allocation
        }))
    }

    #[instrument(err, skip_all)]
    pub fn get_allocs(&self) -> R<Vec<Allocation>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_ALLOCS.as_str())?;
        let alloc_iter = stmt.query_map([], |row| self.allocation_from_row(row))?;

        let mut allocs = Vec::new();
        for alloc in alloc_iter {
            allocs.push(alloc?);
        }

        Ok(allocs)
    }

    #[instrument(err, skip_all)]
    pub fn get_allocs_by_node(&self) -> R<HashMap<String, Vec<Allocation>>> {
        let allocs = self.get_allocs()?;
        let mut by_node: HashMap<String, Vec<Allocation>> = HashMap::new();

        for alloc in allocs {
            by_node
                .entry(alloc.node_id.clone())
                .or_default()
                .push(alloc);
        }

        Ok(by_node)
    }

    #[instrument(err, skip_all)]
    pub fn remove_allocs_for_job(&self, job_id: &str) -> R<()> {
        let job_id = job_id.to_owned();
        self.replicated(Lane::Scheduler, "remove_allocs_for_job", move |s| {
            s.remove_allocs_for_job_apply(&job_id)
        })
    }

    fn remove_allocs_for_job_apply(&self, job_id: &str) -> R<()> {
        let allocations = self.allocs_for_job(job_id)?;
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let sql = r#"
        DELETE FROM allocation
        WHERE job_config_id IN (
            SELECT id FROM job_config WHERE job_id = :job_id
        );
        "#;

        let params = named_params! { ":job_id": job_id };

        tx.execute(sql, params)?;
        self.release_instance_bindings_tx(&tx, &allocations)?;
        self.bump_epochs_for_allocations_tx(&tx, &allocations)?;
        let stopping_event = self.record_stop_notices_tx(&tx, &allocations, StopCause::Stop)?;
        tx.commit()?;
        self.publish_staged(stopping_event);
        drop(conn);

        self.cache_remove_allocations(&allocations)?;

        self.events()
            .allocations_removed_for_job(job_id, allocations.len(), StopCause::Stop);

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn remove_allocs_for_job_config(&self, job_config_id: &str) -> R<Vec<Allocation>> {
        let job_config_id = job_config_id.to_owned();
        self.replicated(Lane::Scheduler, "remove_allocs_for_job_config", move |s| {
            s.remove_allocs_for_job_config_apply(&job_config_id)
        })
    }

    fn remove_allocs_for_job_config_apply(&self, job_config_id: &str) -> R<Vec<Allocation>> {
        let allocations = self.allocs_for_job_config(job_config_id)?;
        if allocations.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        tx.execute(
            "DELETE FROM allocation WHERE job_config_id = ?;",
            [job_config_id],
        )?;
        self.release_instance_bindings_tx(&tx, &allocations)?;
        self.bump_job_config_epoch_tx(&tx, job_config_id)?;
        // A superseded job config's allocations are cleaned up because a
        // newer config replaced them.
        let stopping_event = self.record_stop_notices_tx(&tx, &allocations, StopCause::Update)?;
        tx.commit()?;
        self.publish_staged(stopping_event);
        drop(conn);

        self.cache_remove_allocations(&allocations)?;

        self.events()
            .allocations_removed_for_job_config(job_config_id, &allocations);

        Ok(allocations)
    }

    // ── Count overrides (`legion job scale`, docs/autoscaling.md) ────────

    /// Set (or update) a task's count override: the applied count wins over
    /// the spec's `count.num` for every reader of desired count. Clears any
    /// pending scale-in (a fresh override is newer intent). `base_count`
    /// must be the CURRENT spec count — it is the resubmit-ownership
    /// arbiter (`handle_job_submit_apply` clears the override when a
    /// resubmit changes the spec count away from it).
    #[instrument(err, skip(self, reason, principal))]
    pub fn set_task_scale_override(
        &self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
        count: u32,
        base_count: u32,
        source: &str,
        reason: &str,
        principal: &str,
    ) -> R<()> {
        let (job_id, task_group, task_name) = (
            job_id.to_owned(),
            task_group.to_owned(),
            task_name.to_owned(),
        );
        let (source, reason, principal) =
            (source.to_owned(), reason.to_owned(), principal.to_owned());
        self.replicated(Lane::Control, "set_task_scale_override", move |s| {
            s.set_task_scale_override_apply(
                &job_id,
                &task_group,
                &task_name,
                count,
                base_count,
                &source,
                &reason,
                &principal,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn set_task_scale_override_apply(
        &self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
        count: u32,
        base_count: u32,
        source: &str,
        reason: &str,
        principal: &str,
    ) -> R<()> {
        let now = Self::now_rfc3339()?;
        let conn = self.write_connection()?;
        conn.execute(
            "INSERT OR REPLACE INTO task_scale \
             (job_id, task_group, task_name, count, pending_count, base_count, source, reason, principal, updated_at) \
             VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7, ?8, ?9);",
            params![job_id, task_group, task_name, count, base_count, source, reason, principal, now],
        )?;
        Ok(())
    }

    /// Record a scale-in that must WAIT for the active rollout to finish
    /// (docs/autoscaling.md, the ECS rule: scale-out applies mid-rollout,
    /// scale-in defers). Keeps any applied override untouched; the pending
    /// count is promoted by [`Store::promote_pending_task_scales`] when the
    /// rollout reaches a terminal status.
    #[instrument(err, skip(self, reason, principal))]
    pub fn set_task_scale_pending(
        &self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
        pending: u32,
        base_count: u32,
        source: &str,
        reason: &str,
        principal: &str,
    ) -> R<()> {
        let (job_id, task_group, task_name) = (
            job_id.to_owned(),
            task_group.to_owned(),
            task_name.to_owned(),
        );
        let (source, reason, principal) =
            (source.to_owned(), reason.to_owned(), principal.to_owned());
        self.replicated(Lane::Control, "set_task_scale_pending", move |s| {
            s.set_task_scale_pending_apply(
                &job_id,
                &task_group,
                &task_name,
                pending,
                base_count,
                &source,
                &reason,
                &principal,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn set_task_scale_pending_apply(
        &self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
        pending: u32,
        base_count: u32,
        source: &str,
        reason: &str,
        principal: &str,
    ) -> R<()> {
        let now = Self::now_rfc3339()?;
        let conn = self.write_connection()?;
        // Upsert preserving the applied `count` when a row already exists.
        conn.execute(
            "INSERT INTO task_scale \
             (job_id, task_group, task_name, count, pending_count, base_count, source, reason, principal, updated_at) \
             VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6, ?7, ?8, ?9) \
             ON CONFLICT(job_id, task_group, task_name) DO UPDATE SET \
             pending_count = ?4, base_count = ?5, source = ?6, reason = ?7, principal = ?8, updated_at = ?9;",
            params![job_id, task_group, task_name, pending, base_count, source, reason, principal, now],
        )?;
        Ok(())
    }

    /// Drop a task's override (and any pending scale-in), returning count
    /// ownership to the job spec. Returns whether a row existed.
    #[instrument(err, skip(self))]
    pub fn clear_task_scale(&self, job_id: &str, task_group: &str, task_name: &str) -> R<bool> {
        let (job_id, task_group, task_name) = (
            job_id.to_owned(),
            task_group.to_owned(),
            task_name.to_owned(),
        );
        self.replicated(Lane::Control, "clear_task_scale", move |s| {
            s.clear_task_scale_apply(&job_id, &task_group, &task_name)
        })
    }

    fn clear_task_scale_apply(&self, job_id: &str, task_group: &str, task_name: &str) -> R<bool> {
        let conn = self.write_connection()?;
        let removed = conn.execute(
            "DELETE FROM task_scale WHERE job_id = ?1 AND task_group = ?2 AND task_name = ?3;",
            params![job_id, task_group, task_name],
        )?;
        Ok(removed > 0)
    }

    /// Promote every pending scale-in for a job into its applied count —
    /// called when a rollout reaches a terminal status. Returns the
    /// promoted rows (post-promotion), so the caller can emit events and
    /// enqueue the reconciling `job_scale` eval.
    #[instrument(err, skip(self))]
    pub fn promote_pending_task_scales(&self, job_id: &str) -> R<Vec<TaskScale>> {
        let job_id = job_id.to_owned();
        self.replicated(Lane::Control, "promote_pending_task_scales", move |s| {
            s.promote_pending_task_scales_apply(&job_id)
        })
    }

    fn promote_pending_task_scales_apply(&self, job_id: &str) -> R<Vec<TaskScale>> {
        let now = Self::now_rfc3339()?;
        let conn = self.write_connection()?;
        let mut stmt = conn.prepare(
            "UPDATE task_scale SET count = pending_count, pending_count = NULL, updated_at = ?2 \
             WHERE job_id = ?1 AND pending_count IS NOT NULL \
             RETURNING job_id, task_group, task_name, count, pending_count, base_count, source, reason, principal, updated_at;",
        )?;
        let promoted =
            Self::collect_rows(stmt.query_map(params![job_id, now], Self::task_scale_from_row)?)?;
        Ok(promoted)
    }

    fn task_scale_from_row(row: &Row<'_>) -> Result<TaskScale, rusqlite::Error> {
        Ok(TaskScale {
            job_id: row.get(0)?,
            task_group: row.get(1)?,
            task_name: row.get(2)?,
            count: row.get::<_, Option<i64>>(3)?.map(|count| count as u32),
            pending_count: row.get::<_, Option<i64>>(4)?.map(|count| count as u32),
            base_count: row.get::<_, i64>(5)? as u32,
            source: row.get(6)?,
            reason: row.get(7)?,
            principal: row.get(8)?,
            updated_at: row.get(9)?,
        })
    }

    /// The override row for one task, if any.
    pub fn task_scale_for(
        &self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
    ) -> R<Option<TaskScale>> {
        let conn = self.read_connection()?;
        Ok(conn
            .query_row(
                "SELECT job_id, task_group, task_name, count, pending_count, base_count, source, reason, principal, updated_at \
                 FROM task_scale WHERE job_id = ?1 AND task_group = ?2 AND task_name = ?3;",
                params![job_id, task_group, task_name],
                Self::task_scale_from_row,
            )
            .optional()?)
    }

    /// Every override row for a job.
    pub fn task_scales_for_job(&self, job_id: &str) -> R<Vec<TaskScale>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT job_id, task_group, task_name, count, pending_count, base_count, source, reason, principal, updated_at \
             FROM task_scale WHERE job_id = ?1 ORDER BY task_group, task_name;",
        )?;
        let scales = Self::collect_rows(stmt.query_map([job_id], Self::task_scale_from_row)?)?;
        Ok(scales)
    }

    /// EFFECTIVE desired count for a task: the applied override when one
    /// exists, else the given spec count. The one accessor every consumer
    /// of desired count goes through (scheduler plans, slot retirement via
    /// the SQL COALESCE in `slot_still_desired_tx`, rollout group plans,
    /// surplus retirement).
    pub fn effective_task_count(
        &self,
        job_id: &str,
        task_group: &str,
        task_name: &str,
        spec_count: u32,
    ) -> R<u32> {
        Ok(self
            .task_scale_for(job_id, task_group, task_name)?
            // Overrides only bind to the spec count they were laid over
            // (`base_count`); submitted configs always satisfy this (the
            // submit arbitration clears mismatches), and it keeps dry-run
            // plans over an EDITED spec honest about what a submit would
            // converge to.
            .filter(|scale| scale.base_count == spec_count)
            .and_then(|scale| scale.count)
            .unwrap_or(spec_count))
    }

    /// Retire surplus allocations for one job config down to each task's
    /// effective desired count — the scale-in half of the `job_scale` eval.
    /// Slot-mode stateful tasks lose their highest slot indices (the
    /// released instances are terminated or set rescheduling by
    /// `release_instance_bindings_tx` via `slot_still_desired_tx`, which
    /// honors `stateful.storage.retainOnScaleIn`); stateless tasks lose
    /// their newest allocations first. Returns the removed allocations.
    #[instrument(err, skip_all)]
    pub fn retire_surplus_allocations(&self, job_config_id: &str) -> R<Vec<Allocation>> {
        let job_config_id = job_config_id.to_owned();
        self.replicated(Lane::Scheduler, "retire_surplus_allocations", move |s| {
            s.retire_surplus_allocations_apply(&job_config_id)
        })
    }

    fn retire_surplus_allocations_apply(&self, job_config_id: &str) -> R<Vec<Allocation>> {
        let Some(job_id) = self.job_id_for_job_config(job_config_id)? else {
            return Ok(Vec::new());
        };
        let tasks = self.get_task_configs_for_job_config(job_config_id)?;
        let allocations = self.allocs_for_job_config(job_config_id)?;

        // Slot indexes for slot-mode ordering, one query for the config.
        let mut slot_by_allocation: HashMap<i64, i64> = HashMap::new();
        {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(
                "SELECT a.id, i.slot_index FROM allocation a \
                 INNER JOIN instance i ON i.instance_id = a.instance_id \
                 WHERE a.job_config_id = ?1 AND i.slot_index IS NOT NULL;",
            )?;
            let rows = Self::collect_rows(stmt.query_map([job_config_id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })?)?;
            slot_by_allocation.extend(rows);
        }

        let mut surplus: Vec<Allocation> = Vec::new();
        for (task_id, task) in &tasks {
            let keyed = task
                .stateful
                .as_ref()
                .is_some_and(|stateful| stateful.mode == InstanceMode::Keyed);
            // Keyed tasks have no count; count.per converges per group —
            // both are rejected at the ScaleTask RPC and skipped here.
            if keyed || task.count.per.is_some() {
                continue;
            }
            let effective =
                self.effective_task_count(&job_id, &task.group, &task.name, task.count.num)?;
            let mut task_allocs: Vec<&Allocation> = allocations
                .iter()
                .filter(|allocation| allocation.task_config_id == *task_id)
                .collect();
            if task_allocs.len() <= effective as usize {
                continue;
            }
            let slot_mode = task
                .stateful
                .as_ref()
                .is_some_and(|stateful| stateful.mode == InstanceMode::Slot);
            if slot_mode {
                task_allocs.sort_by_key(|allocation| {
                    std::cmp::Reverse(
                        slot_by_allocation
                            .get(&allocation.id)
                            .copied()
                            .unwrap_or(i64::MIN),
                    )
                });
            } else {
                task_allocs.sort_by_key(|allocation| std::cmp::Reverse(allocation.id));
            }
            let excess = task_allocs.len() - effective as usize;
            surplus.extend(task_allocs.into_iter().take(excess).cloned());
        }

        if surplus.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        for allocation in &surplus {
            tx.execute("DELETE FROM allocation WHERE id = ?;", [allocation.id])?;
        }
        // Every removal records WHY, in the same transaction that removes it
        // (preemption plan § Phase 1a). Scale-in gets its own cause rather
        // than borrowing `Update`: no new job config version exists here.
        let stopping_event = self.record_stop_notices_tx(&tx, &surplus, StopCause::ScaleIn)?;
        self.release_instance_bindings_tx(&tx, &surplus)?;
        self.bump_epochs_for_allocations_tx(&tx, &surplus)?;
        tx.commit()?;
        self.publish_staged(stopping_event);
        drop(conn);

        self.cache_remove_allocations(&surplus)?;
        self.events()
            .allocations_removed_for_job_config(job_config_id, &surplus);

        Ok(surplus)
    }

    /// Promote pending scale-ins after a rollout reached a terminal status,
    /// emit their events, and enqueue the reconciling `job_scale` eval.
    /// Composite of two sequential single-lane ops (Control promote →
    /// Scheduler enqueue), the JC-33 wrapper-sequential shape.
    pub fn apply_pending_scales_after_rollout(
        &self,
        job_id: &str,
        job_config_id: &str,
    ) -> R<usize> {
        let promoted = self.promote_pending_task_scales(job_id)?;
        if promoted.is_empty() {
            return Ok(0);
        }
        for scale in &promoted {
            self.events().scale_applied(
                job_id,
                &scale.task_group,
                &scale.task_name,
                None,
                scale.count.unwrap_or_default(),
                &scale.source,
                "deferred scale-in applied after rollout",
            );
        }
        self.enqueue_eval("job_scale", Some(job_id), Some(job_config_id), None, None)?;
        Ok(promoted.len())
    }

    /// Bring a job config back to its desired allocation counts.
    ///
    /// Runs the real scheduler in `Reschedule` mode rather than a second,
    /// simplified placement pass: replacements after a node loss or a node
    /// join have to honor node capacity, distinct constraints, group
    /// co-location and stateful instance counts exactly like every other
    /// placement path, and be scored by the configured policy.
    ///
    /// Deliberately NOT a capture of its own (JC-33/JC-35). `plan_and_apply`
    /// is already a SEQUENCE of replicated ops — `insert_plan_submission` and
    /// `apply_plan_submission` on the scheduler lane, plus the latter's
    /// status-lane `update_task_statuses_for_allocations` follow-up. Wrapping
    /// the sequence in one scheduler capture would compose that status write
    /// into the scheduler changeset, which the capture-time ownership guard
    /// rejects (JC-26), and it would blind the planner: a capture's reads go
    /// through the read pool and see only COMMITTED state, so the plan could
    /// not observe the eviction that preceded it. Each step is idempotent —
    /// the plan submission carries an epoch CAS, so a replay after a crash
    /// mid-sequence is rejected rather than double-placing, and the next
    /// reconcile pass converges.
    #[instrument(err, skip_all)]
    pub fn reconcile_job_config_allocations(&self, job_config_id: &str) -> R<()> {
        if self
            .get_task_configs_for_job_config(job_config_id)?
            .is_empty()
        {
            return Ok(());
        }

        let scheduler = crate::manager::scheduler::Scheduler::new(self)?;
        let outcome = scheduler.plan_and_apply(
            job_config_id,
            crate::manager::scheduler::PlanMode::Reschedule,
        )?;
        if !outcome.allocations.is_empty() {
            debug!(
                %job_config_id,
                placed = outcome.allocations.len(),
                "Reconciled job config allocations"
            );
        }

        Ok(())
    }

    /// Free a lost node's allocations for replacement — gated for stateful
    /// tasks (the at-most-one fencing guarantee, see
    /// docs/stateful-workloads.md "Node loss and fencing"):
    ///
    /// - Stateless allocations are removed and replaced immediately (today's
    ///   eager behavior).
    /// - A stateful allocation is only removed once the node has been lost
    ///   longer than its task's `onLost.replaceAfter` — i.e. provably past
    ///   the worker's `selfTerminateAfter` self-fence window — so the
    ///   replacement can never run concurrently with the original. With
    ///   `replaceAfter: "never"` it is never removed here; only node
    ///   recovery or an operator override (ReplaceInstance) frees it.
    ///
    /// Gated allocations keep their rows: the instance stays `active` and
    /// bound (`current_alloc_id` intact), the scheduler keeps counting the
    /// allocation as placed (so no plan path double-places the instance),
    /// and instance get/list surfaces a `pending-loss-confirmation` detail.
    /// The node-status monitor re-runs this method every tick (see
    /// [`Self::recheck_lost_nodes`]), so replacement proceeds automatically
    /// once the threshold elapses. Idempotent.
    /// Since C-2 this is a wrapper-level composition of single-purpose
    /// scheduler-lane ops (see
    /// [`Self::reschedule_allocations_for_drained_node`] for why the
    /// removal and the reconcile must not share one capture). A crash
    /// between them is healed by the node monitor re-enqueueing the
    /// `node_lost_reschedule` eval every tick.
    #[instrument(err, skip_all)]
    pub fn reschedule_allocations_for_lost_node(&self, node_id: &str) -> R<()> {
        let allocs = self.allocs_for_node(node_id)?;
        if allocs.is_empty() {
            return Ok(());
        }

        let lost_for = self.node_lost_for(node_id)?;
        let mut removable = Vec::new();
        for alloc in allocs {
            if self.lost_allocation_replaceable(&alloc, lost_for)? {
                removable.push(alloc);
            } else {
                info!(
                    allocation_id = alloc.id,
                    %node_id,
                    instance_id = alloc.instance_id.as_deref().unwrap_or_default(),
                    lost_for_secs = lost_for.map(TimeDuration::whole_seconds),
                    "Holding stateful allocation on lost node pending loss confirmation"
                );
            }
        }
        if removable.is_empty() {
            return Ok(());
        }

        let affected_job_configs = self.remove_allocations(&removable, StopCause::NodeLost)?;
        for job_config_id in affected_job_configs {
            self.reconcile_job_config_allocations(&job_config_id)?;
        }

        Ok(())
    }

    /// How long the node has been lost, when known. `None` means the node is
    /// not (or no longer) marked lost — callers treat that as "not
    /// confirmable yet", never as "free to replace".
    fn node_lost_for(&self, node_id: &str) -> R<Option<TimeDuration>> {
        let Some(lost_at) = self.node_lost_at(node_id)? else {
            return Ok(None);
        };
        let lost_at = OffsetDateTime::parse(&lost_at, &Rfc3339)
            .with_context(|| format!("invalid lost_at timestamp {lost_at:?} for node {node_id}"))?;
        Ok(Some(OffsetDateTime::now_utc() - lost_at))
    }

    /// Whether a lost node's allocation may be freed for replacement now.
    /// Stateless tasks: always (eager). Stateful tasks: only once the loss
    /// is confirmed per the task's `onLost` policy.
    fn lost_allocation_replaceable(
        &self,
        alloc: &Allocation,
        lost_for: Option<TimeDuration>,
    ) -> R<bool> {
        let Some(stateful) = self.task_stateful_config(alloc.task_config_id)? else {
            return Ok(true);
        };
        let policy = Self::on_lost_policy_or_safe_default(&alloc.job_id, &stateful);
        match policy.replace_after {
            ReplaceAfter::Never => Ok(false),
            ReplaceAfter::After(threshold) => {
                // Unknown loss duration = unconfirmable = hold (safe side).
                let Some(lost_for) = lost_for else {
                    return Ok(false);
                };
                let threshold = TimeDuration::try_from(threshold)
                    .context("onLost.replaceAfter overflows the comparable duration range")?;
                Ok(lost_for > threshold)
            }
        }
    }

    /// Resolve a stored stateful block's `onLost` policy. Validation rejects
    /// unresolvable policies at submit time, so a failure here means a row
    /// predating validation or hand-edited state — fall back to the safe
    /// defaults rather than wedging the whole lost-node sweep.
    fn on_lost_policy_or_safe_default(
        job_id: &str,
        stateful: &StatefulConfig,
    ) -> ResolvedOnLostPolicy {
        stateful.resolved_on_lost().unwrap_or_else(|err| {
            warn!(
                %job_id,
                error = ?err,
                "Stored stateful onLost policy failed to resolve; using defaults"
            );
            StatefulOnLostConfig::default()
                .resolve()
                .expect("default onLost policy resolves")
        })
    }

    /// The task's stored stateful block, if any (`None` = stateless task).
    fn task_stateful_config(&self, task_config_id: i64) -> R<Option<StatefulConfig>> {
        let conn = self.read_connection()?;
        let raw: Option<String> = conn
            .query_row(
                "SELECT stateful FROM task_config WHERE id = ?;",
                [task_config_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        raw.map(|raw| {
            serde_json::from_str(&raw)
                .with_context(|| format!("invalid stateful config for task {task_config_id}"))
        })
        .transpose()
    }

    /// Remove specific allocation rows (releasing instance bindings and
    /// bumping epochs, like every other allocation-removal path) and return
    /// the affected job config ids.
    pub(crate) fn remove_allocations(
        &self,
        allocs: &[Allocation],
        cause: StopCause,
    ) -> R<HashSet<String>> {
        if allocs.is_empty() {
            return Ok(HashSet::new());
        }
        let allocs = allocs.to_vec();
        self.replicated(Lane::Scheduler, "remove_allocations", move |s| {
            s.remove_allocations_apply(&allocs, cause)
        })
    }

    fn remove_allocations_apply(
        &self,
        allocs: &[Allocation],
        cause: StopCause,
    ) -> R<HashSet<String>> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        for alloc in allocs {
            tx.execute("DELETE FROM allocation WHERE id = ?;", [alloc.id])?;
        }
        self.release_instance_bindings_tx(&tx, allocs)?;
        self.bump_epochs_for_allocations_tx(&tx, allocs)?;
        let stopping_event = self.record_stop_notices_tx(&tx, allocs, cause)?;
        tx.commit()?;
        self.publish_staged(stopping_event);

        self.cache_remove_allocations(allocs)?;

        Ok(allocs
            .iter()
            .map(|alloc| alloc.job_config_id.clone())
            .collect())
    }

    /// Since C-2 each per-config reconcile is its own scheduler-lane op
    /// (see [`Self::reschedule_allocations_for_drained_node`] for the
    /// capture-visibility rationale); every pass is an idempotent
    /// convergence step, so no cross-op atomicity is needed.
    #[instrument(err, skip_all)]
    pub fn reschedule_allocations_for_new_node(&self) -> R<()> {
        let mut job_config_ids = self.job_config_ids_with_count_per()?;
        for job_config_id in self.job_config_ids_with_incomplete_rollouts()? {
            if !job_config_ids.contains(&job_config_id) {
                job_config_ids.push(job_config_id);
            }
        }
        for job_config_id in job_config_ids {
            self.reconcile_job_config_allocations(&job_config_id)?;
        }
        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn get_ledger(&self) -> R<Vec<LedgerEntry>> {
        let nodes = self.get_nodes()?;
        let allocations = self.get_allocs_by_node()?;

        let mut ledger = Vec::new();

        for node in nodes {
            let node_allocations = allocations.get(&node.id).cloned().unwrap_or_default();
            ledger.push(LedgerEntry {
                node,
                allocations: node_allocations,
            });
        }

        Ok(ledger)
    }

    //
    // INSTANCES (stateful workloads; see docs/stateful-workloads.md)
    //

    #[instrument(err, skip_all)]
    pub fn get_instance(&self, instance_id: &str) -> R<Option<Instance>> {
        let conn = self.read_connection()?;
        let instance = conn
            .query_row(&GET_INSTANCE, [instance_id], |row| {
                self.instance_from_row(row)
            })
            .optional()?;
        Ok(instance)
    }

    #[instrument(err, skip_all)]
    pub fn list_instances(&self, job_id: &str) -> R<Vec<Instance>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_INSTANCES_FOR_JOB.as_str())?;
        let rows = stmt.query_map([job_id], |row| self.instance_from_row(row))?;
        Self::collect_rows(rows)
    }

    //
    // SIGNING KEYS (workload-identity OIDC issuer; see docs/workload-identity.md)
    //
    // The private key PEM is persisted in this SQLite store — the same trust
    // domain as the CA material. Persistence is what keeps externally-held
    // tokens verifiable across manager restarts.
    //

    fn signing_key_from_row(row: &Row<'_>) -> Result<SigningKeyRecord, rusqlite::Error> {
        Ok(SigningKeyRecord {
            kid: row.get(0)?,
            alg: row.get(1)?,
            private_key_pem: row.get(2)?,
            public_jwk_x: row.get(3)?,
            public_jwk_y: row.get(4)?,
            created_at: row.get::<_, i64>(5)? as u64,
            not_after: row.get::<_, Option<i64>>(6)?.map(|v| v as u64),
            state: row.get(7)?,
        })
    }

    /// Persist a freshly generated signing key.
    #[instrument(err, skip_all, fields(kid = %record.kid, state = %record.state))]
    pub fn insert_signing_key(&self, record: &SigningKeyRecord) -> R<()> {
        let record = record.clone();
        self.replicated(Lane::Control, "insert_signing_key", move |s| {
            s.insert_signing_key_apply(&record)
        })
    }

    fn insert_signing_key_apply(&self, record: &SigningKeyRecord) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute(
            &INSERT_SIGNING_KEY,
            named_params! {
                ":kid": record.kid,
                ":alg": record.alg,
                ":private_key_pem": record.private_key_pem,
                ":public_jwk_x": record.public_jwk_x,
                ":public_jwk_y": record.public_jwk_y,
                ":created_at": record.created_at as i64,
                ":not_after": record.not_after.map(|v| v as i64),
                ":state": record.state,
            },
        )?;
        Ok(())
    }

    /// All persisted signing keys (including expired rows, newest first).
    #[instrument(err, skip_all)]
    pub fn list_signing_keys(&self) -> R<Vec<SigningKeyRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(GET_SIGNING_KEYS.as_str())?;
        let rows = stmt.query_map([], Self::signing_key_from_row)?;
        Self::collect_rows(rows)
    }

    /// Persist a key lifecycle transition (`active` → `retiring` → `expired`).
    #[instrument(err, skip_all, fields(kid = %kid, state = %state))]
    pub fn update_signing_key_state(
        &self,
        kid: &str,
        state: &str,
        not_after: Option<u64>,
    ) -> R<()> {
        let kid = kid.to_owned();
        let state = state.to_owned();
        self.replicated(Lane::Control, "update_signing_key_state", move |s| {
            s.update_signing_key_state_apply(&kid, &state, not_after)
        })
    }

    fn update_signing_key_state_apply(
        &self,
        kid: &str,
        state: &str,
        not_after: Option<u64>,
    ) -> R<()> {
        let conn = self.write_connection()?;
        let updated = conn.execute(
            &UPDATE_SIGNING_KEY_STATE,
            named_params! {
                ":kid": kid,
                ":state": state,
                ":not_after": not_after.map(|v| v as i64),
            },
        )?;
        if updated == 0 {
            bail!("signing key {kid} not found");
        }
        Ok(())
    }

    /// Remove a signing key row entirely. Normal rotation keeps expired rows
    /// for audit; this exists for operator cleanup.
    #[instrument(err, skip_all, fields(kid = %kid))]
    pub fn delete_signing_key(&self, kid: &str) -> R<()> {
        let kid = kid.to_owned();
        self.replicated(Lane::Control, "delete_signing_key", move |s| {
            s.delete_signing_key_apply(&kid)
        })
    }

    fn delete_signing_key_apply(&self, kid: &str) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute(&DELETE_SIGNING_KEY, named_params! { ":kid": kid })?;
        Ok(())
    }

    /// Insert or fully replace an instance row (terminated keyed rows are
    /// never deleted — the never-recycle guarantee depends on them remaining).
    #[instrument(err, skip_all)]
    pub fn upsert_instance(&self, instance: &Instance) -> R<Instance> {
        let instance = instance.clone();
        self.replicated(Lane::Scheduler, "upsert_instance", move |s| {
            s.upsert_instance_apply(&instance)
        })
    }

    fn upsert_instance_apply(&self, instance: &Instance) -> R<Instance> {
        let now = Self::now_rfc3339()?;
        let created_at = if instance.created_at.is_empty() {
            now.clone()
        } else {
            instance.created_at.clone()
        };
        let conn = self.write_connection()?;
        let params = named_params! {
            ":instance_id": instance.instance_id,
            ":job_id": instance.job_id,
            ":group_name": instance.group,
            ":task_name": instance.task_name,
            ":mode": instance.mode.as_str(),
            ":state": instance.state.as_str(),
            ":slot_index": instance.slot_index,
            ":current_alloc_id": instance.current_alloc_id,
            ":storage_handle": instance.storage_handle,
            ":created_at": created_at,
            ":updated_at": now,
        };
        let instance =
            conn.query_row(&UPSERT_INSTANCE, params, |row| self.instance_from_row(row))?;
        Ok(instance)
    }

    /// Fetch the deterministic slot instance for (job, group, slot), creating
    /// a Pending row (no storage handle) if it does not exist yet. The
    /// allocation-insert path is what activates instances and mints storage
    /// handles; this method exists for introspection and pre-materialization.
    #[instrument(err, skip_all)]
    pub fn get_or_create_slot_instance(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        slot: u32,
    ) -> R<Instance> {
        let job_id = job_id.to_owned();
        let group = group.to_owned();
        let task_name = task_name.to_owned();
        self.replicated(Lane::Scheduler, "get_or_create_slot_instance", move |s| {
            s.get_or_create_slot_instance_apply(&job_id, &group, &task_name, slot)
        })
    }

    fn get_or_create_slot_instance_apply(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        slot: u32,
    ) -> R<Instance> {
        let instance_id = slot_instance_id(job_id, group, slot);
        if let Some(existing) = self.get_instance(&instance_id)? {
            return Ok(existing);
        }

        let now = Self::now_rfc3339()?;
        let conn = self.write_connection()?;
        // INSERT OR IGNORE + re-read keeps this race-safe against the
        // allocation-insert path creating the same slot concurrently.
        conn.execute(
            "INSERT OR IGNORE INTO instance (instance_id, job_id, group_name, task_name, mode, \
             state, slot_index, current_alloc_id, storage_handle, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, 'slot', 'pending', ?5, NULL, NULL, ?6, ?6);",
            params![instance_id, job_id, group, task_name, slot, now],
        )?;
        let instance = conn.query_row(&GET_INSTANCE, [instance_id.as_str()], |row| {
            self.instance_from_row(row)
        })?;
        Ok(instance)
    }

    /// Point an instance at its (new) current allocation, or clear the binding.
    #[instrument(err, skip_all)]
    pub fn set_instance_current_alloc(
        &self,
        instance_id: &str,
        current_alloc_id: Option<i64>,
    ) -> R<()> {
        let instance_id = instance_id.to_owned();
        self.replicated(Lane::Scheduler, "set_instance_current_alloc", move |s| {
            s.set_instance_current_alloc_apply(&instance_id, current_alloc_id)
        })
    }

    fn set_instance_current_alloc_apply(
        &self,
        instance_id: &str,
        current_alloc_id: Option<i64>,
    ) -> R<()> {
        let now = Self::now_rfc3339()?;
        let conn = self.write_connection()?;
        let updated = conn.execute(
            "UPDATE instance SET current_alloc_id = ?1, updated_at = ?2 WHERE instance_id = ?3;",
            params![current_alloc_id, now, instance_id],
        )?;
        if updated == 0 {
            bail!("instance {instance_id} not found");
        }
        Ok(())
    }

    /// Transition an instance's lifecycle state.
    #[instrument(err, skip_all)]
    pub fn set_instance_state(&self, instance_id: &str, state: InstanceState) -> R<()> {
        let instance_id = instance_id.to_owned();
        self.replicated(Lane::Scheduler, "set_instance_state", move |s| {
            s.set_instance_state_apply(&instance_id, state)
        })
    }

    fn set_instance_state_apply(&self, instance_id: &str, state: InstanceState) -> R<()> {
        let now = Self::now_rfc3339()?;
        let conn = self.write_connection()?;
        let updated = conn.execute(
            "UPDATE instance SET state = ?1, updated_at = ?2 WHERE instance_id = ?3;",
            params![state.as_str(), now, instance_id],
        )?;
        if updated == 0 {
            bail!("instance {instance_id} not found");
        }
        Ok(())
    }

    /// Settle every instance of a stopped job. Rows are always retained, but
    /// the two modes land in different states:
    ///
    /// - slot rows become `terminated`, so a later `job run` re-materializes
    ///   the same ordinal identities. Their storage handles are retained by
    ///   default (the re-run reattaches the same data — exactly today's
    ///   scale-out semantics), but a task with
    ///   `stateful.storage.retainOnStop: false` (the K8s
    ///   `whenDeleted: Delete` analog) has its slot handles DROPPED here, so
    ///   the revived slots mint fresh handles;
    /// - keyed rows become `stopped` (with handles retained, always — keyed
    ///   stop semantics are fixed per instance), so resubmitting the job and
    ///   running `legion instance start <id>` revives the same sandbox with
    ///   its persisted state. A job stop is a pause, not a destroy: only
    ///   DestroyInstance / job destroy is forever. Stopped keyed instances
    ///   stay stopped when the job is resubmitted — they are not desired by
    ///   the scheduler until explicitly started.
    ///
    /// `job destroy` never reaches this path — it unconditionally deletes
    /// every instance row (and handle) via the job FK cascade, regardless of
    /// `retainOnStop`.
    ///
    /// Called from the job_stop eval after its allocations are removed.
    /// Idempotent: already-stopped/terminated rows are untouched.
    #[instrument(err, skip_all)]
    pub fn stop_instances_for_job(&self, job_id: &str) -> R<u64> {
        let job_id = job_id.to_owned();
        self.replicated(Lane::Scheduler, "stop_instances_for_job", move |s| {
            s.stop_instances_for_job_apply(&job_id)
        })
    }

    fn stop_instances_for_job_apply(&self, job_id: &str) -> R<u64> {
        let now = Self::now_rfc3339()?;
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;

        // Resolve the per-task job-stop retention policy from the LATEST job
        // config version (mirroring how `slot_still_desired_tx` resolves the
        // scale-in policy) and drop the handles of the slot rows this stop is
        // about to terminate when `retainOnStop: false`. Already-settled
        // (terminated/stopped) rows are untouched, keeping the stop
        // idempotent; keyed rows are never targeted (their stop semantics are
        // fixed, and validation rejects the knob in keyed mode anyway).
        let mut stmt = tx.prepare(
            "SELECT DISTINCT group_name, task_name FROM instance \
             WHERE job_id = ?1 AND mode = 'slot' AND storage_handle IS NOT NULL \
             AND state NOT IN ('terminated', 'stopped');",
        )?;
        let slot_tasks: Vec<(String, String)> =
            Self::collect_rows(stmt.query_map([job_id], |row| Ok((row.get(0)?, row.get(1)?)))?)?;
        drop(stmt);
        for (group, task_name) in slot_tasks {
            let stateful: Option<Option<String>> = tx
                .query_row(
                    "SELECT task_config.stateful FROM task_config \
                     INNER JOIN job_config ON job_config.id = task_config.job_config_id \
                     WHERE task_config.job_id = ?1 AND task_config.group_name = ?2 \
                     AND task_config.name = ?3 \
                     ORDER BY job_config.version DESC LIMIT 1;",
                    params![job_id, group, task_name],
                    |row| row.get(0),
                )
                .optional()?;
            let retain = stateful
                .flatten()
                .as_deref()
                .and_then(|raw| serde_json::from_str::<StatefulConfig>(raw).ok())
                .and_then(|stateful| stateful.storage)
                .map(|storage| storage.retains_on_stop())
                .unwrap_or(true);
            if !retain {
                info!(
                    job_id,
                    group,
                    task = %task_name,
                    "Dropping slot storage handles on job stop (retainOnStop: false)"
                );
                tx.execute(
                    "UPDATE instance SET storage_handle = NULL, updated_at = ?1 \
                     WHERE job_id = ?2 AND group_name = ?3 AND task_name = ?4 \
                     AND mode = 'slot' AND state NOT IN ('terminated', 'stopped');",
                    params![now, job_id, group, task_name],
                )?;
            }
        }

        let updated = tx.execute(
            "UPDATE instance SET \
             state = CASE WHEN mode = 'keyed' THEN 'stopped' ELSE 'terminated' END, \
             current_alloc_id = NULL, updated_at = ?1 \
             WHERE job_id = ?2 AND state NOT IN ('terminated', 'stopped');",
            params![now, job_id],
        )?;
        tx.commit()?;
        Ok(updated as u64)
    }

    /// Whether ANY instance row (live, stopped, or tombstone) owns this id.
    fn instance_id_exists_tx(tx: &Connection, id: &str) -> R<bool> {
        let found: Option<i64> = tx
            .query_row(
                "SELECT 1 FROM instance WHERE instance_id = ?;",
                [id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(found.is_some())
    }

    /// Whether a caller-supplied keyed id structurally aliases a slot id of a
    /// slot-mode stateful task in the SAME job. Slot ids are the deterministic
    /// `{job_id}-{group}-{slot}` pattern and the slot id space is exclusive, so
    /// a keyed id that lands on `job-group-N` for some slot task with
    /// `count.num > N` would, once that slot materializes, fight over one
    /// instance row. We forbid it at create time so the two id spaces stay
    /// disjoint within a job (cross-job aliasing is additionally caught when
    /// the slot binds, see `assign_stateful_instance_tx`). Checks every job
    /// config version's slot tasks so a not-yet-current (or future) slot can't
    /// be aliased either.
    fn keyed_id_collides_with_slot_pattern_tx(tx: &Connection, job_id: &str, id: &str) -> R<bool> {
        let mut stmt = tx.prepare(
            "SELECT DISTINCT group_name, count FROM task_config \
             WHERE job_id = ?1 AND stateful IS NOT NULL \
             AND json_extract(stateful, '$.mode') = 'slot';",
        )?;
        let slot_tasks: Vec<(String, i64)> =
            Self::collect_rows(stmt.query_map([job_id], |row| Ok((row.get(0)?, row.get(1)?)))?)?;
        for (group, count) in slot_tasks {
            for slot in 0..count.max(0) as u32 {
                if slot_instance_id(job_id, &group, slot) == id {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Insert one pending keyed instance row inside `tx`, enforcing the
    /// never-recycle collision rule. `requested_id: None` mints
    /// `{task}-{16 alphanumerics}`. Shared by the single and batch create
    /// paths so both apply identical rules.
    fn insert_keyed_instance_tx(
        &self,
        tx: &Connection,
        job_id: &str,
        group: &str,
        task_name: &str,
        requested_id: Option<&str>,
    ) -> R<String> {
        let now = Self::now_rfc3339()?;
        let instance_id = match requested_id {
            Some(id) => {
                if Self::instance_id_exists_tx(tx, id)? {
                    bail!(StorageError::Conflict {
                        message: format!(
                            "instance id {id} already exists (keyed ids are never recycled, \
                             including stopped and destroyed ones); choose a different id"
                        ),
                    });
                }
                if Self::keyed_id_collides_with_slot_pattern_tx(tx, job_id, id)? {
                    bail!(StorageError::Conflict {
                        message: format!(
                            "instance id {id} collides with the reserved slot-id pattern \
                             {{job}}-{{group}}-{{slot}} of a slot-mode stateful task in job \
                             {job_id}; slot and keyed id spaces are disjoint within a job"
                        ),
                    });
                }
                id.to_string()
            }
            None => loop {
                // Collisions on 16 random alphanumerics are practically
                // impossible; the loop is belt-and-braces.
                let candidate = format!("{task_name}-{}", random_string(16));
                if !Self::instance_id_exists_tx(tx, &candidate)? {
                    break candidate;
                }
            },
        };

        tx.execute(
            "INSERT INTO instance (instance_id, job_id, group_name, task_name, mode, state, \
             slot_index, current_alloc_id, storage_handle, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, 'keyed', 'pending', NULL, NULL, NULL, ?5, ?5);",
            params![instance_id, job_id, group, task_name, now],
        )?;
        Ok(instance_id)
    }

    /// Register a new keyed-mode instance (the CreateInstance RPC's storage
    /// half; task resolution and id policy live in the server handler).
    ///
    /// When `requested_id` is `Some` it must not collide with ANY existing
    /// instance row — including Stopped rows and Terminated tombstones —
    /// enforcing the never-recycle guarantee; collisions surface as
    /// [`StorageError::Conflict`]. When `None`, a random id is minted
    /// (`{task}-{16 alphanumerics}`). The row starts `pending` with no storage
    /// handle; the first placement mints one if the task declares storage.
    #[instrument(err, skip_all)]
    pub fn create_keyed_instance(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        requested_id: Option<&str>,
    ) -> R<Instance> {
        let job_id = job_id.to_owned();
        let group = group.to_owned();
        let task_name = task_name.to_owned();
        let requested_id = requested_id.map(str::to_owned);
        self.replicated(Lane::Scheduler, "create_keyed_instance", move |s| {
            s.create_keyed_instance_apply(&job_id, &group, &task_name, requested_id.as_deref())
        })
    }

    fn create_keyed_instance_apply(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        requested_id: Option<&str>,
    ) -> R<Instance> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let instance_id =
            self.insert_keyed_instance_tx(&tx, job_id, group, task_name, requested_id)?;
        let instance = tx.query_row(&GET_INSTANCE, [instance_id.as_str()], |row| {
            self.instance_from_row(row)
        })?;
        tx.commit()?;
        Ok(instance)
    }

    /// Register a batch of keyed-mode instances ALL-OR-NOTHING (the
    /// CreateInstances RPC's storage half): every id in `requested_ids` plus
    /// `mint_count` server-minted ids, in one transaction. Any collision —
    /// with an existing row (live/stopped/tombstone) or a duplicate within
    /// `requested_ids` itself — aborts the whole batch with
    /// [`StorageError::Conflict`] naming the offending id, creating nothing.
    /// Returns the created ids in request order (explicit first, then
    /// minted).
    #[instrument(err, skip_all)]
    pub fn create_keyed_instances(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        requested_ids: &[String],
        mint_count: u32,
    ) -> R<Vec<String>> {
        let job_id = job_id.to_owned();
        let group = group.to_owned();
        let task_name = task_name.to_owned();
        let requested_ids = requested_ids.to_vec();
        self.replicated(Lane::Scheduler, "create_keyed_instances", move |s| {
            s.create_keyed_instances_apply(&job_id, &group, &task_name, &requested_ids, mint_count)
        })
    }

    fn create_keyed_instances_apply(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        requested_ids: &[String],
        mint_count: u32,
    ) -> R<Vec<String>> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let mut created = Vec::with_capacity(requested_ids.len() + mint_count as usize);
        for id in requested_ids {
            created.push(self.insert_keyed_instance_tx(&tx, job_id, group, task_name, Some(id))?);
        }
        for _ in 0..mint_count {
            created.push(self.insert_keyed_instance_tx(&tx, job_id, group, task_name, None)?);
        }
        tx.commit()?;
        Ok(created)
    }

    /// Ensure each seed id from `stateful.initialInstances` exists (the
    /// RunJob seeding half; see the field docs on
    /// [`crate::structs::StatefulConfig::initial_instances`]).
    ///
    /// Ids that have never existed are created as `pending` keyed rows; ids
    /// that already exist in ANY state — live, stopped, or a terminated
    /// tombstone, even under another job — are skipped silently
    /// (`INSERT OR IGNORE`), so resubmits are idempotent and a deliberately
    /// destroyed id is never resurrected. Returns the ids actually created
    /// by this call.
    #[instrument(err, skip_all)]
    pub fn seed_keyed_instances(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        ids: &[String],
    ) -> R<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        // Forwarding wrapper (plan §8, D2): part of the run_job tail —
        // keyed seeds must exist before the placement eval plans, and the
        // eval itself is forwarded to the scheduler leader, so the seeds
        // must reach it too.
        match self.seed_keyed_instances_local(job_id, group, task_name, ids) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::SeedKeyedInstances {
                        job_id: job_id.to_owned(),
                        group: group.to_owned(),
                        task_name: task_name.to_owned(),
                        ids: ids.to_vec(),
                    },
                    err,
                ),
            other => other,
        }
    }

    pub(crate) fn seed_keyed_instances_local(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        ids: &[String],
    ) -> R<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let job_id = job_id.to_owned();
        let group = group.to_owned();
        let task_name = task_name.to_owned();
        let ids = ids.to_vec();
        self.replicated(Lane::Scheduler, "seed_keyed_instances", move |s| {
            s.seed_keyed_instances_apply(&job_id, &group, &task_name, &ids)
        })
    }

    fn seed_keyed_instances_apply(
        &self,
        job_id: &str,
        group: &str,
        task_name: &str,
        ids: &[String],
    ) -> R<Vec<String>> {
        let now = Self::now_rfc3339()?;
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let mut created = Vec::new();
        for id in ids {
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO instance (instance_id, job_id, group_name, task_name, \
                 mode, state, slot_index, current_alloc_id, storage_handle, created_at, \
                 updated_at) \
                 VALUES (?1, ?2, ?3, ?4, 'keyed', 'pending', NULL, NULL, NULL, ?5, ?5);",
                params![id, job_id, group, task_name, now],
            )?;
            if inserted > 0 {
                created.push(id.clone());
            }
        }
        tx.commit()?;
        if !created.is_empty() {
            info!(
                job_id,
                group,
                task_name,
                seeded = created.len(),
                "Seeded keyed instances from initialInstances"
            );
        }
        Ok(created)
    }

    /// Destroy a keyed-mode instance (the DestroyInstance RPC's storage half):
    /// delete its allocations through the normal allocation-removal path,
    /// transition the row to `terminated`, and drop its storage handle.
    ///
    /// The row is retained forever as a tombstone so the id can never be
    /// created again. Destroying a `stopped` instance works the same way
    /// (stopped -> terminated; the retained handle is dropped) — stop is the
    /// reversible pause, destroy is forever. Destroying an
    /// already-terminated instance is a no-op (returns
    /// [`DestroyInstanceOutcome::AlreadyTerminated`]). Slot-mode instances
    /// are rejected: their lifecycle is owned by the scheduler via
    /// `count.num`.
    #[instrument(err, skip_all)]
    pub fn destroy_keyed_instance(&self, instance_id: &str) -> R<DestroyInstanceOutcome> {
        let instance_id = instance_id.to_owned();
        self.replicated(Lane::Scheduler, "destroy_keyed_instance", move |s| {
            s.destroy_keyed_instance_apply(&instance_id)
        })
    }

    fn destroy_keyed_instance_apply(&self, instance_id: &str) -> R<DestroyInstanceOutcome> {
        let instance = self
            .get_instance(instance_id)?
            .ok_or_else(|| StorageError::not_found("instance", instance_id))?;
        if instance.mode != InstanceMode::Keyed {
            bail!(StorageError::InvalidInput {
                message: format!(
                    "instance {instance_id} is slot-mode; slot instances are managed by the \
                     scheduler via count.num, not DestroyInstance"
                ),
            });
        }
        if instance.state == InstanceState::Terminated {
            return Ok(DestroyInstanceOutcome::AlreadyTerminated);
        }

        let now = Self::now_rfc3339()?;
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;

        // Stop the instance's allocation(s) through the normal removal path
        // (delete row + epoch bump + cache eviction); the worker stops the
        // task on its next heartbeat reconcile. The instance is terminated in
        // the same transaction, so the release hook semantics (which skip
        // terminated instances) can never race this.
        let mut stmt = tx.prepare(
            "SELECT allocation.id, allocation.status, allocation.resources, allocation.node_id, \
             allocation.job_config_id, allocation.task_config_id, job_config.job_id, \
             allocation.instance_id, allocation.network \
             FROM allocation \
             INNER JOIN job_config ON job_config.id = allocation.job_config_id \
             WHERE allocation.instance_id = ?;",
        )?;
        let allocations = Self::collect_rows(
            stmt.query_map([instance_id], |row| self.allocation_from_row(row))?,
        )?;
        drop(stmt);

        tx.execute(
            "DELETE FROM allocation WHERE instance_id = ?;",
            [instance_id],
        )?;
        tx.execute(
            "UPDATE instance SET state = 'terminated', current_alloc_id = NULL, \
             storage_handle = NULL, updated_at = ?1 WHERE instance_id = ?2;",
            params![now, instance_id],
        )?;
        self.bump_epochs_for_allocations_tx(&tx, &allocations)?;
        let stopping_event = self.record_stop_notices_tx(&tx, &allocations, StopCause::Destroy)?;
        tx.commit()?;
        self.publish_staged(stopping_event);

        self.cache_remove_allocations(&allocations)?;

        info!(
            instance_id,
            job_id = %instance.job_id,
            removed_allocations = allocations.len(),
            "Destroyed keyed instance"
        );
        Ok(DestroyInstanceOutcome::Destroyed {
            removed_allocations: allocations.len(),
        })
    }

    /// Stop (pause) a keyed-mode instance (the StopInstance RPC's storage
    /// half): delete its allocations through the normal allocation-removal
    /// path and transition the row to `stopped`, RETAINING the row and its
    /// storage handle. The id stays owned (creates still collide) and
    /// [`Self::start_keyed_instance`] reverses the whole thing — unlike
    /// destroy, stop is not forever.
    ///
    /// The state flips to `stopped` in the SAME transaction that removes the
    /// allocations (mirroring [`Self::destroy_keyed_instance`]), so the
    /// release hook — which skips `stopped` rows — can never race this and
    /// flip the instance to `rescheduling`.
    ///
    /// Stopping an already-stopped instance is a no-op (returns
    /// [`StopInstanceOutcome::AlreadyStopped`]). Slot-mode instances and
    /// terminated tombstones are rejected with
    /// [`StorageError::InvalidInput`]; the RPC layer pre-checks the
    /// terminated case to surface FailedPrecondition.
    #[instrument(err, skip_all)]
    pub fn stop_keyed_instance(&self, instance_id: &str) -> R<StopInstanceOutcome> {
        let instance_id = instance_id.to_owned();
        self.replicated(Lane::Scheduler, "stop_keyed_instance", move |s| {
            s.stop_keyed_instance_apply(&instance_id)
        })
    }

    fn stop_keyed_instance_apply(&self, instance_id: &str) -> R<StopInstanceOutcome> {
        let instance = self
            .get_instance(instance_id)?
            .ok_or_else(|| StorageError::not_found("instance", instance_id))?;
        if instance.mode != InstanceMode::Keyed {
            bail!(StorageError::InvalidInput {
                message: format!(
                    "instance {instance_id} is slot-mode; slot instances are managed by the \
                     scheduler via count.num, not StopInstance"
                ),
            });
        }
        match instance.state {
            InstanceState::Stopped => return Ok(StopInstanceOutcome::AlreadyStopped),
            InstanceState::Terminated => bail!(StorageError::InvalidInput {
                message: format!(
                    "instance {instance_id} is terminated; destroyed instances are tombstoned \
                     forever and cannot be stopped"
                ),
            }),
            InstanceState::Pending | InstanceState::Active | InstanceState::Rescheduling => {}
        }

        let now = Self::now_rfc3339()?;
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;

        // Stop the instance's allocation(s) through the normal removal path
        // (delete row + epoch bump + cache eviction); the worker stops the
        // task on its next heartbeat reconcile. The instance is marked
        // stopped in the same transaction, so the release hook (which skips
        // stopped instances) can never flip it to rescheduling.
        let mut stmt = tx.prepare(
            "SELECT allocation.id, allocation.status, allocation.resources, allocation.node_id, \
             allocation.job_config_id, allocation.task_config_id, job_config.job_id, \
             allocation.instance_id, allocation.network \
             FROM allocation \
             INNER JOIN job_config ON job_config.id = allocation.job_config_id \
             WHERE allocation.instance_id = ?;",
        )?;
        let allocations = Self::collect_rows(
            stmt.query_map([instance_id], |row| self.allocation_from_row(row))?,
        )?;
        drop(stmt);

        tx.execute(
            "DELETE FROM allocation WHERE instance_id = ?;",
            [instance_id],
        )?;
        tx.execute(
            "UPDATE instance SET state = 'stopped', current_alloc_id = NULL, updated_at = ?1 \
             WHERE instance_id = ?2;",
            params![now, instance_id],
        )?;
        self.bump_epochs_for_allocations_tx(&tx, &allocations)?;
        let stopping_event = self.record_stop_notices_tx(&tx, &allocations, StopCause::Stop)?;
        tx.commit()?;
        self.publish_staged(stopping_event);

        self.cache_remove_allocations(&allocations)?;

        info!(
            instance_id,
            job_id = %instance.job_id,
            removed_allocations = allocations.len(),
            "Stopped keyed instance"
        );
        Ok(StopInstanceOutcome::Stopped {
            removed_allocations: allocations.len(),
        })
    }

    /// Start (resume) a stopped keyed-mode instance (the StartInstance RPC's
    /// storage half): transition `stopped -> pending` so the next placement
    /// eval binds a fresh allocation to the same instance id and its
    /// RETAINED storage handle. Job-status gating (a stopped job schedules
    /// nothing) lives in the RPC handler.
    ///
    /// Starting an already-live (pending/active/rescheduling) instance is a
    /// no-op (returns [`StartInstanceOutcome::AlreadyRunning`]). Slot-mode
    /// instances and terminated tombstones are rejected with
    /// [`StorageError::InvalidInput`]; the RPC layer pre-checks the
    /// terminated case to surface FailedPrecondition.
    #[instrument(err, skip_all)]
    pub fn start_keyed_instance(&self, instance_id: &str) -> R<StartInstanceOutcome> {
        let instance_id = instance_id.to_owned();
        self.replicated(Lane::Scheduler, "start_keyed_instance", move |s| {
            s.start_keyed_instance_apply(&instance_id)
        })
    }

    fn start_keyed_instance_apply(&self, instance_id: &str) -> R<StartInstanceOutcome> {
        let instance = self
            .get_instance(instance_id)?
            .ok_or_else(|| StorageError::not_found("instance", instance_id))?;
        if instance.mode != InstanceMode::Keyed {
            bail!(StorageError::InvalidInput {
                message: format!(
                    "instance {instance_id} is slot-mode; slot instances are managed by the \
                     scheduler via count.num, not StartInstance"
                ),
            });
        }
        match instance.state {
            InstanceState::Pending | InstanceState::Active | InstanceState::Rescheduling => {
                return Ok(StartInstanceOutcome::AlreadyRunning);
            }
            InstanceState::Terminated => bail!(StorageError::InvalidInput {
                message: format!(
                    "instance {instance_id} is terminated; destroyed ids are never recycled — \
                     create a new instance instead"
                ),
            }),
            InstanceState::Stopped => {}
        }

        let now = Self::now_rfc3339()?;
        let conn = self.write_connection()?;
        // Guard on the current state so a concurrent destroy/stop cannot be
        // overwritten by this transition.
        let updated = conn.execute(
            "UPDATE instance SET state = 'pending', updated_at = ?1 \
             WHERE instance_id = ?2 AND state = 'stopped';",
            params![now, instance_id],
        )?;
        if updated == 0 {
            bail!(StorageError::Conflict {
                message: format!(
                    "instance {instance_id} changed state concurrently; retry StartInstance"
                ),
            });
        }
        info!(
            instance_id,
            job_id = %instance.job_id,
            "Started keyed instance (stopped -> pending)"
        );
        Ok(StartInstanceOutcome::Started)
    }

    /// Count the live (pending/active/rescheduling) keyed instances of a
    /// (job, group, task). This is the desired allocation count for a
    /// keyed-mode task — `count.num` is ignored for keyed tasks. `stopped`
    /// instances are NOT live: they are paused and get no allocation until
    /// StartInstance; `terminated` rows are tombstones.
    #[instrument(err, skip_all)]
    pub fn count_live_keyed_instances(&self, job_id: &str, group: &str, task_name: &str) -> R<u32> {
        let conn = self.read_connection()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM instance \
             WHERE job_id = ?1 AND group_name = ?2 AND task_name = ?3 \
             AND mode = 'keyed' AND state NOT IN ('terminated', 'stopped');",
            params![job_id, group, task_name],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as u32)
    }

    /// Map of `instance_id -> slot ordinal` for the slot pool of a
    /// (job, group). Used by sequential (`rollout.slots.order`) rollouts to
    /// retire old allocations in descending slot order — an allocation's slot
    /// identity is reachable only through its instance row
    /// (`allocation.instance_id -> instance.slot_index`).
    #[instrument(err, skip_all)]
    pub fn slot_indices_for_job_group(&self, job_id: &str, group: &str) -> R<HashMap<String, u32>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT instance_id, slot_index FROM instance \
             WHERE job_id = ?1 AND group_name = ?2 AND mode = 'slot' \
             AND slot_index IS NOT NULL;",
        )?;
        let rows = stmt.query_map(params![job_id, group], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        let mut indices = HashMap::new();
        for row in rows {
            let (instance_id, slot) = row?;
            indices.insert(instance_id, slot.max(0) as u32);
        }
        Ok(indices)
    }

    /// Count live allocations bound to (job, group)'s slot instances that do
    /// NOT belong to `exclude_job_config_id`.
    ///
    /// This is how a plain (non-rollout) reschedule learns that some slots
    /// are deliberately still occupied by an older config — the staged
    /// (`rollout.slots.updateFrom`) state — and must not be double-placed:
    /// the latest config's effective desired count is `count.num` minus this
    /// number, so a reschedule only replaces slots that are actually empty.
    #[instrument(err, skip_all)]
    pub fn count_slot_allocations_excluding_config(
        &self,
        job_id: &str,
        group: &str,
        exclude_job_config_id: &str,
    ) -> R<u32> {
        let conn = self.read_connection()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM allocation \
             INNER JOIN instance ON instance.instance_id = allocation.instance_id \
             WHERE instance.job_id = ?1 AND instance.group_name = ?2 \
             AND instance.mode = 'slot' AND allocation.job_config_id != ?3;",
            params![job_id, group, exclude_job_config_id],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as u32)
    }

    //
    // EVALS
    //

    // The eval queue is REPLICATED state owned by the scheduler lane
    // (plan §7-C2, JC-41 — reversing JC-27). JC-27 took evals out of raft on
    // the premise that they are derived, regenerable work; the adversarial
    // pass disproved it. An acknowledged `job stop`'s allocation teardown, a
    // mid-flight rollout tick, a config update's previous-version cleanup
    // and a KV-triggered restart each ride an eval whose loss is permanent
    // and user-visible, and `regenerate_scheduling_evals` cannot reconstruct
    // operator intent that was never persisted. So every eval mutation is a
    // `replicated(Lane::Scheduler, …)` op again: followers hold the queue,
    // a new leader inherits it, and only the *processing* loop is
    // leader-gated.
    //
    // Composite shape (JC-33): an op on ANOTHER lane that needs an eval
    // enqueues it as its own sequential scheduler-lane op AFTER its own op
    // commits — never from inside its capture, where the capture-time
    // ownership guard would reject the foreign-lane write (that rejection is
    // the wall keeping this rule true; see
    // `ownership::guard_rejects_commutative_mutations_and_foreign_lane_eval_writes`).
    //
    // Cross-lane FKs stay REMOVED (see the `eval_local` rebuild in
    // migrations.rs): a control-lane `DELETE FROM job` must not cascade into
    // this scheduler-lane table. Evals therefore reference job/job_config/
    // node advisorily. Stale references are absorbed at claim time
    // (`claim_eval_apply` sweeps superseded siblings; `prune_stale_evals`
    // drops rows for superseded/missing configs at leadership acquisition)
    // or by the handler failing — and a failed eval is terminal
    // (`complete_eval(.., false)` sets status `failed`, which the claim
    // query never selects), so a dangling reference cannot spin.

    #[instrument(err, skip_all)]
    pub fn insert_eval(&self, eval: &Eval) -> R<()> {
        let eval = eval.clone();
        self.replicated(Lane::Scheduler, "insert_eval", move |s| {
            s.insert_eval_apply(&eval)
        })
    }

    fn insert_eval_apply(&self, eval: &Eval) -> R<()> {
        let conn = self.write_connection()?;

        let params = named_params! {
            ":id": eval.id,
            ":status": eval.status,
            ":type": eval.kind,
            ":job_id": if eval.job_id.is_empty() { None::<String> } else { Some(eval.job_id.clone()) },
            ":job_config_id": if eval.job_config_id.is_empty() { None::<String> } else { Some(eval.job_config_id.clone()) },
            ":node_id": if eval.node_name.is_empty() { None::<String> } else { Some(eval.node_name.clone()) },
            ":priority": eval.priority,
            ":ready_at": Self::sortable_now()?,
        };

        conn.execute(&INSERT_EVAL, params)?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn set_eval_status(&self, id: &str, status: &str) -> R<()> {
        let id = id.to_owned();
        let status = status.to_owned();
        self.replicated(Lane::Scheduler, "set_eval_status", move |s| {
            s.set_eval_status_apply(&id, &status)
        })
    }

    fn set_eval_status_apply(&self, id: &str, status: &str) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute("UPDATE eval SET status = ? WHERE id = ?;", [status, id])?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn get_evals(&self) -> R<Vec<Eval>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&GET_EVALS)?;
        let rows = stmt.query_map([], |row| self.eval_from_row(row))?;

        Self::collect_rows(rows)
    }

    // #[instrument(err, skip_all)]
    pub fn get_next_eval(&self) -> R<Option<Eval>> {
        let conn = self.read_connection()?;
        let now = Self::sortable_now()?;
        let eval = conn.query_row(&GET_NEXT_EVAL, [now], |row| self.eval_from_row(row));

        match eval {
            Ok(eval) => Ok(Some(eval)),
            Err(RusqliteError::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(err, skip_all)]
    pub fn claim_eval(&self) -> R<Option<Eval>> {
        self.replicated(Lane::Scheduler, "claim_eval", move |s| s.claim_eval_apply())
    }

    fn claim_eval_apply(&self) -> R<Option<Eval>> {
        let tx_started = Instant::now();
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;

        let eval = tx
            .query_row(
                "UPDATE eval
                SET status = 'in_flight'
                WHERE id = (
                    SELECT id
                    FROM eval
                    WHERE status = 'pending' AND ready_at <= :now
                    ORDER BY priority DESC, id ASC
                    LIMIT 1
                )
                RETURNING id, status, type, job_id, job_config_id, node_id, priority, ready_at;",
                named_params! {":now": Self::sortable_now()?},
                |row| self.eval_from_row(row),
            )
            .optional()?;

        let Some(mut eval) = eval else {
            tx.commit()?;
            debug!(
                duration_ms = tx_started.elapsed().as_millis() as u64,
                "No claimable eval was found"
            );
            return Ok(None);
        };

        let eval_id = eval.id.clone();
        // Collapse only work this claim genuinely subsumes: same entity, same
        // kind, and already claimable.
        //
        // Matching on kind matters because different kinds are not
        // interchangeable — dropping a job's `rollout_tick` because a
        // `job_reschedule` for it was claimed stalls the rollout forever
        // (ticks are self-perpetuating, nothing re-drives them), and dropping
        // a `job_stop` the same way loses an operator's request outright.
        //
        // Matching on `ready_at` matters because a delayed eval is a timer,
        // not a queued duplicate: a batch task's next scheduled `job_update`
        // must survive an unrelated `job_update` running now.
        tx.execute(
            "DELETE FROM eval
            WHERE status = 'pending'
              AND id != :id
              AND type = :type
              AND ready_at <= :now
              AND (
                (:job_id IS NOT NULL AND :job_id != '' AND job_id = :job_id)
                OR (:node_id IS NOT NULL AND :node_id != '' AND node_id = :node_id)
              );",
            named_params! {
                ":id": eval_id,
                ":type": eval.kind,
                ":now": Self::sortable_now()?,
                ":job_id": if eval.job_id.trim().is_empty() { None::<String> } else { Some(eval.job_id.clone()) },
                ":node_id": if eval.node_name.trim().is_empty() { None::<String> } else { Some(eval.node_name.clone()) },
            },
        )?;

        // Cheapest accurate point for the queue-depth gauge: the claim (and
        // its superseded-eval sweep) just changed the pending count, and the
        // eval table is small enough that COUNT inside the open tx is cheap.
        let pending = Self::pending_eval_count_tx(&tx)?;
        tx.commit()?;
        crate::manager::telemetry::metrics()
            .eval_queue_depth
            .set(pending);
        info!(
            eval_id = eval.id,
            eval_kind = %eval.kind,
            eval_priority = eval.priority,
            duration_ms = tx_started.elapsed().as_millis(),
            "Claimed eval transaction"
        );
        eval.status = "in_flight".to_string();

        Ok(Some(eval))
    }

    // Expose a clone so runtime worker tasks can await wake notifications.
    pub fn eval_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.eval_notify)
    }

    // Returns how long until the earliest pending eval becomes claimable.
    // None means there is no pending eval row at all.
    pub fn next_pending_eval_delay(&self) -> R<Option<Duration>> {
        let conn = self.read_connection()?;

        let next_ready_at: Option<String> = conn
            .query_row(
                "SELECT ready_at FROM eval WHERE status = 'pending' ORDER BY ready_at ASC LIMIT 1;",
                [],
                |row| row.get(0),
            )
            .optional()?;

        let Some(next_ready_at) = next_ready_at else {
            return Ok(None);
        };

        let next_ready_at = OffsetDateTime::parse(&next_ready_at, &Rfc3339)?;
        let now = OffsetDateTime::now_utc();
        if next_ready_at <= now {
            return Ok(Some(Duration::ZERO));
        }

        Ok(Some((next_ready_at - now).try_into()?))
    }

    #[instrument(err, skip_all)]
    pub fn requeue_in_flight_evals(&self) -> R<u64> {
        self.replicated(Lane::Scheduler, "requeue_in_flight_evals", move |s| {
            s.requeue_in_flight_evals_apply()
        })
    }

    fn requeue_in_flight_evals_apply(&self) -> R<u64> {
        let conn = self.write_connection()?;
        let updated = conn.execute(
            "UPDATE eval SET status = 'pending' WHERE status = 'in_flight';",
            [],
        )?;

        let pending = Self::pending_eval_count_tx(&conn)?;
        drop(conn);
        crate::manager::telemetry::metrics()
            .eval_queue_depth
            .set(pending);

        if updated > 0 && !capture::in_capture() {
            // Recovery moved work from in_flight->pending; wake all waiters.
            // Under capture the rows are not committed yet, so the wake is
            // deferred to apply time — `project_changeset_side_effects`
            // notifies on any eval change, on every replica (JC-11/JC-41).
            self.eval_notify.notify_waiters();
        }

        Ok(updated as u64)
    }

    /// Belt-and-braces reconciliation of pending scheduling work
    /// (plan §7-C2, JC-41).
    ///
    /// This pass is **no longer load-bearing for durability**. Under JC-27
    /// it was: evals were node-local, so a failover lost the whole queue and
    /// this hook had to reconstruct it — which it provably could not do for
    /// work whose intent was never persisted (an acknowledged `job stop`'s
    /// teardown, a mid-flight rollout tick, a KV restart). JC-41 put the
    /// queue back in raft, so a new leader INHERITS the real pending work
    /// and this pass only closes gaps that replicated state can genuinely
    /// re-derive: an eval that was never enqueued because the enqueue op
    /// itself was rejected/lost (a control-lane op acked on a node that did
    /// not lead the scheduler lane), or one dropped by
    /// [`Self::prune_stale_evals`] as superseded.
    ///
    /// Every regenerated eval is an idempotent convergence step, and scopes
    /// that already have a pending/in-flight eval are skipped — so it is
    /// safe to run on every leadership acquisition and periodically.
    /// Per scope (adversarial hardening, 2026-07-24 — the original pass
    /// regenerated only `job_reschedule`/`node_drain`/
    /// `node_lost_reschedule` and silently dropped stop tails, rollout
    /// ticks, config-update cleanups, KV restarts, and no-evict drains):
    ///
    /// - a `rollout_tick` eval for every rollout that is neither terminal
    ///   nor waiting on an operator (the tick chain re-arms itself via a
    ///   delayed eval — replicated since JC-41, so this only matters if the
    ///   re-arm never landed); jobs with any live rollout are EXCLUDED from the blanket
    ///   reschedule below — a plain `job_reschedule` would place the
    ///   rollout's to_config at full desired count, bypassing canary/staged
    ///   gating;
    /// - for every other active job: `job_update` when a replicated restart
    ///   marker is set ([`Self::request_job_restart`]); `job_submit` when a
    ///   superseded config still has live allocations and the latest has
    ///   none (the lost submit tail — the handler migrates or
    ///   places-then-removes exactly as the lost eval would have);
    ///   otherwise finish any half-done config-update cleanup right here
    ///   (migrate schedule-equivalent stale allocations, remove the rest)
    ///   and enqueue `job_reschedule` (a no-op when nothing is missing);
    /// - a `job_stop` eval for every STOPPED job that still has allocations
    ///   or live instance rows (the acknowledged stop's teardown tail;
    ///   JC-41 makes the eval itself survive failover, this is the second
    ///   line of defence);
    /// - a `node_drain` eval for every node still marked drained WITH the
    ///   replicated `drain_evict` intent. This is the over-execution fix
    ///   (JC-41): `availability = 'drained'` alone cannot distinguish
    ///   `node drain --evict` from a plain cordon, so regenerating on
    ///   availability evicted allocations off nodes an operator had only
    ///   cordoned. The intent is now persisted by
    ///   [`Self::set_node_availability_with_evict`] and cleared whenever
    ///   availability leaves `drained`, so a cordon-only node is never
    ///   evicted by reconciliation (JC-16);
    /// - a `node_lost_reschedule` eval for every node still marked lost
    ///   (the C-2 eval-mediated reschedule tail; the node monitor also
    ///   re-enqueues it every tick, so this only closes the gap until the
    ///   monitor's first post-failover pass).
    ///
    /// Scopes that already have a pending/in-flight eval are skipped.
    /// Returns how many evals were enqueued. Also safe to run outside
    /// failover (and it runs periodically on the scheduler-lane leader
    /// under divergent lane leadership): every regenerated eval is an
    /// idempotent reconciliation pass.
    pub fn regenerate_scheduling_evals(&self) -> R<u64> {
        const TERMINAL_ROLLOUTS: &str =
            "('completed', 'failed', 'cancelled', 'completed_incomplete')";
        const OPERATOR_WAIT_ROLLOUTS: &str =
            "('waiting_manual_start', 'waiting_manual_advance', 'paused')";

        let mut enqueued = 0u64;

        // Mid-flight rollouts: re-arm the tick chain.
        let rollout_targets: Vec<(String, String)> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(&format!(
                "SELECT r.job_id, r.to_job_config_id FROM rollout r
                 WHERE r.status NOT IN {TERMINAL_ROLLOUTS}
                   AND r.status NOT IN {OPERATOR_WAIT_ROLLOUTS}
                   AND NOT EXISTS (
                     SELECT 1 FROM eval e
                     WHERE e.status IN ('pending', 'in_flight') AND e.job_id = r.job_id
                   );"
            ))?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            Self::collect_rows(rows)?
        };
        for (job_id, to_config) in rollout_targets {
            self.enqueue_eval("rollout_tick", Some(&job_id), Some(&to_config), None, None)?;
            enqueued += 1;
        }

        // Active jobs without a live rollout.
        let job_targets: Vec<(String, Option<String>, bool)> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(&format!(
                "SELECT j.id, (
                     SELECT jc.id FROM job_config jc
                     WHERE jc.job_id = j.id
                     ORDER BY jc.created_at DESC
                     LIMIT 1
                 ) AS job_config_id,
                 j.restart_requested_at IS NOT NULL
                 FROM job j
                 WHERE j.status NOT IN ('stopped', 'failed')
                   AND NOT EXISTS (
                     SELECT 1 FROM eval e
                     WHERE e.status IN ('pending', 'in_flight') AND e.job_id = j.id
                   )
                   AND NOT EXISTS (
                     SELECT 1 FROM rollout r
                     WHERE r.job_id = j.id AND r.status NOT IN {TERMINAL_ROLLOUTS}
                   );"
            ))?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get::<_, bool>(2)?))
            })?;
            Self::collect_rows(rows)?
        };
        for (job_id, latest_config, restart_requested) in job_targets {
            let Some(latest_config) = latest_config else {
                continue;
            };
            let kind = if restart_requested {
                // A KV-triggered restart was acknowledged but its
                // leader-local job_update eval died: regenerate it (the
                // handler clears the marker before restarting).
                "job_update"
            } else {
                let stale_configs: Vec<String> = {
                    let conn = self.read_connection()?;
                    let mut stmt = conn.prepare(
                        "SELECT DISTINCT a.job_config_id FROM allocation a
                         INNER JOIN job_config jc ON jc.id = a.job_config_id
                         WHERE jc.job_id = ?1 AND a.job_config_id != ?2;",
                    )?;
                    let rows = stmt.query_map(params![job_id, latest_config], |row| row.get(0))?;
                    Self::collect_rows(rows)?
                };
                if stale_configs.is_empty() {
                    "job_reschedule"
                } else {
                    let latest_count: i64 = {
                        let conn = self.read_connection()?;
                        conn.query_row(
                            "SELECT COUNT(*) FROM allocation WHERE job_config_id = ?1;",
                            [&latest_config],
                            |row| row.get(0),
                        )?
                    };
                    if latest_count == 0 {
                        // The acknowledged submit's eval was lost whole:
                        // job_submit replays it (migrate-or-place, THEN
                        // remove the predecessor — no avoidable downtime).
                        "job_submit"
                    } else {
                        // The submit partially ran (latest placed, stale
                        // cleanup lost): finish the cleanup here, then
                        // converge counts.
                        for stale in &stale_configs {
                            if self.job_configs_schedule_equivalent(stale, &latest_config)? {
                                self.migrate_allocations_to_job_config(stale, &latest_config)?;
                            } else {
                                self.remove_allocs_for_job_config(stale)?;
                            }
                        }
                        "job_reschedule"
                    }
                }
            };
            self.enqueue_eval(kind, Some(&job_id), Some(&latest_config), None, None)?;
            enqueued += 1;
        }

        // Stopped jobs whose teardown tail was lost with the old leader.
        let stop_targets: Vec<(String, Option<String>)> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(
                "SELECT j.id, (
                     SELECT jc.id FROM job_config jc
                     WHERE jc.job_id = j.id
                     ORDER BY jc.created_at DESC
                     LIMIT 1
                 ) AS job_config_id
                 FROM job j
                 WHERE j.status = 'stopped'
                   AND NOT EXISTS (
                     SELECT 1 FROM eval e
                     WHERE e.status IN ('pending', 'in_flight') AND e.job_id = j.id
                   )
                   AND (
                     EXISTS (
                       SELECT 1 FROM allocation a
                       INNER JOIN job_config jc ON jc.id = a.job_config_id
                       WHERE jc.job_id = j.id
                     )
                     OR EXISTS (
                       SELECT 1 FROM instance i
                       WHERE i.job_id = j.id AND i.state NOT IN ('stopped', 'terminated')
                     )
                   );",
            )?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            Self::collect_rows(rows)?
        };
        for (job_id, job_config_id) in stop_targets {
            self.enqueue_eval(
                "job_stop",
                Some(&job_id),
                job_config_id.as_deref(),
                None,
                None,
            )?;
            enqueued += 1;
        }

        let drained_nodes: Vec<String> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(
                "SELECT n.id FROM node n
                 WHERE n.availability = 'drained' AND n.drain_evict = 1
                   AND NOT EXISTS (
                     SELECT 1 FROM eval e
                     WHERE e.status IN ('pending', 'in_flight')
                       AND e.type = 'node_drain' AND e.node_id = n.id
                   );",
            )?;
            let nodes = Self::collect_rows(stmt.query_map([], |row| row.get(0))?)?;
            drop(stmt);
            nodes
        };
        let lost_nodes: Vec<String> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(
                "SELECT n.id FROM node n
                 INNER JOIN node_health nh ON nh.node_id = n.id
                 WHERE nh.status = 'lost'
                   AND NOT EXISTS (
                     SELECT 1 FROM eval e
                     WHERE e.status IN ('pending', 'in_flight')
                       AND e.type = 'node_lost_reschedule' AND e.node_id = n.id
                   );",
            )?;
            let nodes = Self::collect_rows(stmt.query_map([], |row| row.get(0))?)?;
            drop(stmt);
            nodes
        };
        for node_id in drained_nodes {
            self.enqueue_eval("node_drain", None, None, Some(&node_id), None)?;
            enqueued += 1;
        }
        for node_id in lost_nodes {
            self.enqueue_eval("node_lost_reschedule", None, None, Some(&node_id), None)?;
            enqueued += 1;
        }
        Ok(enqueued)
    }

    /// Drop eval rows that reference a SUPERSEDED job config (or one that no
    /// longer exists), called on leadership acquisition BEFORE the stale
    /// `in_flight` requeue (adversarial finding, 2026-07-24): rows from a
    /// previous leadership stint would otherwise (a) execute against an old
    /// config — placing old-version allocations nothing cleans up — and (b)
    /// suppress regeneration for the same job (the regeneration queries skip
    /// any job with a pending eval).
    ///
    /// Dropping such a row is safe *because of what it selects*, not because
    /// evals are disposable (JC-41): the row's config has already been
    /// superseded or destroyed by a later replicated write, so whatever work
    /// it described is either obsolete or re-derivable from the current
    /// config. It is a replicated scheduler-lane op like every other eval
    /// mutation, so the queue stays identical on every replica. Returns how
    /// many rows were dropped.
    pub fn prune_stale_evals(&self) -> R<u64> {
        self.replicated(Lane::Scheduler, "prune_stale_evals", move |s| {
            s.prune_stale_evals_apply()
        })
    }

    fn prune_stale_evals_apply(&self) -> R<u64> {
        let conn = self.write_connection()?;
        let removed = conn.execute(
            "DELETE FROM eval
                 WHERE job_config_id IS NOT NULL AND job_config_id != ''
                   AND (
                     NOT EXISTS (
                       SELECT 1 FROM job_config mine WHERE mine.id = eval.job_config_id
                     )
                     OR EXISTS (
                       SELECT 1 FROM job_config mine
                       INNER JOIN job_config newer ON newer.job_id = mine.job_id
                       WHERE mine.id = eval.job_config_id
                         AND newer.created_at > mine.created_at
                     )
                   );",
            [],
        )?;
        Ok(removed as u64)
    }

    /// Pending (unclaimed) eval count, used to keep the
    /// `legion_eval_queue_depth` gauge accurate at the points where the
    /// pending set changes (enqueue, claim, startup requeue).
    fn pending_eval_count_tx(conn: &Connection) -> R<i64> {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM eval WHERE status = 'pending';",
            [],
            |row| row.get(0),
        )?)
    }

    #[instrument(err, skip_all)]
    pub fn complete_eval(&self, eval_id: &str, succeeded: bool) -> R<()> {
        let eval_id = eval_id.to_owned();
        self.replicated(Lane::Scheduler, "complete_eval", move |s| {
            s.complete_eval_impl(&eval_id, succeeded, None)
        })
    }

    /// Complete an eval and record its lifecycle event (`eval.completed` /
    /// `eval.failed`, built via `drafts`) in ONE scheduler-lane op: the
    /// completion and its event ride the same changeset, so a replica can
    /// never show an `eval.failed` event for a row that is still in flight
    /// (`scheduler_event` is commutative — INSERT from any lane is legal,
    /// JC-28). Restored with JC-41: while the queue was node-local the two
    /// halves had to be separate writes.
    #[instrument(err, skip_all)]
    pub fn complete_eval_with_event(
        &self,
        eval_id: &str,
        succeeded: bool,
        event: EventDraft,
    ) -> R<()> {
        let eval_id = eval_id.to_owned();
        self.replicated(Lane::Scheduler, "complete_eval_with_event", move |s| {
            s.complete_eval_impl(&eval_id, succeeded, Some(event))
        })
    }

    /// Put a claimed (in-flight) eval back on the pending queue with a
    /// delay, instead of terminally failing it. Used when an eval's handler
    /// hit a `NotRaftLeader` rejection on a FOREIGN lane (divergent lane
    /// leadership under `lanes` topology): the eval's own work is fine —
    /// only a cross-lane half was rejected — and marking it failed would
    /// drop acknowledged work forever, because failed evals are never
    /// retried and their side effects (e.g. the job_submit prev-config
    /// cleanup) never happen (adversarial finding, 2026-07-24).
    ///
    /// The release itself proposes on the scheduler lane — the lane this
    /// node must already lead to have claimed the eval — so the retry is
    /// durable: a crash between the rejection and the retry leaves the row
    /// `in_flight`, which the next leader's `requeue_in_flight_evals`
    /// recovers.
    #[instrument(err, skip(self))]
    pub fn release_eval_for_retry(&self, eval_id: &str, delay: Duration) -> R<()> {
        let eval_id = eval_id.to_owned();
        self.replicated(Lane::Scheduler, "release_eval_for_retry", move |s| {
            s.release_eval_for_retry_apply(&eval_id, delay)
        })
    }

    fn release_eval_for_retry_apply(&self, eval_id: &str, delay: Duration) -> R<()> {
        let ready_at =
            (OffsetDateTime::now_utc() + time::Duration::try_from(delay)?).format(&Rfc3339)?;
        let conn = self.write_connection()?;
        conn.execute(
            "UPDATE eval SET status = 'pending', ready_at = ?2 WHERE id = ?1;",
            params![eval_id, ready_at],
        )?;
        Ok(())
    }

    fn complete_eval_impl(
        &self,
        eval_id: &str,
        succeeded: bool,
        event: Option<EventDraft>,
    ) -> R<()> {
        let tx_started = Instant::now();
        let mut conn: MutexGuard<'_, Connection> = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;

        if succeeded {
            tx.execute("DELETE FROM eval WHERE id = ?;", [eval_id])?;
        } else {
            tx.execute("UPDATE eval SET status = 'failed' WHERE id = ?;", [eval_id])?;
        }
        let staged = event.and_then(|draft| self.stage_event_locked(&tx, draft));
        tx.commit()?;
        self.publish_staged(staged);
        debug!(
            eval_id,
            succeeded,
            duration_ms = tx_started.elapsed().as_millis(),
            "Completed eval transaction"
        );

        Ok(())
    }

    pub fn enqueue_eval(
        &self,
        kind: &str,
        job_id: Option<&str>,
        job_config_id: Option<&str>,
        node_id: Option<&str>,
        priority: Option<i64>,
    ) -> R<Eval> {
        self.enqueue_eval_after_delay(
            kind,
            job_id,
            job_config_id,
            node_id,
            priority,
            Duration::ZERO,
        )
    }

    pub fn enqueue_eval_after_delay(
        &self,
        kind: &str,
        job_id: Option<&str>,
        job_config_id: Option<&str>,
        node_id: Option<&str>,
        priority: Option<i64>,
        delay: Duration,
    ) -> R<Eval> {
        // Forwarding wrapper (plan §8, D2): the enqueue is the scheduler
        // half of many composites (RPC submit tails, failure-report
        // reschedules, drain evictions). Regeneration re-derives lost
        // enqueues eventually; forwarding lands them immediately, from any
        // node.
        match self.enqueue_eval_after_delay_local(
            kind,
            job_id,
            job_config_id,
            node_id,
            priority,
            delay,
        ) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::EnqueueEval {
                        kind: kind.to_owned(),
                        job_id: job_id.map(str::to_owned),
                        job_config_id: job_config_id.map(str::to_owned),
                        node_id: node_id.map(str::to_owned),
                        priority,
                        delay_ms: delay.as_millis() as u64,
                    },
                    err,
                ),
            other => other,
        }
    }

    pub(crate) fn enqueue_eval_after_delay_local(
        &self,
        kind: &str,
        job_id: Option<&str>,
        job_config_id: Option<&str>,
        node_id: Option<&str>,
        priority: Option<i64>,
        delay: Duration,
    ) -> R<Eval> {
        let kind = kind.to_owned();
        let job_id = job_id.map(str::to_owned);
        let job_config_id = job_config_id.map(str::to_owned);
        let node_id = node_id.map(str::to_owned);
        self.replicated(Lane::Scheduler, "enqueue_eval_after_delay", move |s| {
            s.enqueue_eval_after_delay_apply(
                &kind,
                job_id.as_deref(),
                job_config_id.as_deref(),
                node_id.as_deref(),
                priority,
                delay,
            )
        })
    }

    fn enqueue_eval_after_delay_apply(
        &self,
        kind: &str,
        job_id: Option<&str>,
        job_config_id: Option<&str>,
        node_id: Option<&str>,
        priority: Option<i64>,
        delay: Duration,
    ) -> R<Eval> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;

        let job_id_value = job_id
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string);
        let job_config_id_value = job_config_id
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string);
        let node_id_value = node_id
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string);

        let ready_at =
            Self::sortable_timestamp(OffsetDateTime::now_utc() + time::Duration::try_from(delay)?)?;

        // One field, both meanings: with no explicit override the eval
        // inherits its job's priority band, so importance orders the queue
        // AND governs preemption. An explicit `--priority`/`--asap` still
        // wins — it is a per-submission queue override, not a band change.
        let priority = match priority {
            Some(explicit) => explicit,
            None => self
                .band_eval_priority_tx(&tx, job_id_value.as_deref(), job_config_id_value.as_deref())
                .unwrap_or(self.default_eval_priority),
        };

        let params = named_params! {
            ":id": None::<i64>,
            ":status": "pending",
            ":type": kind,
            ":job_id": job_id_value,
            ":job_config_id": job_config_id_value,
            ":node_id": node_id_value,
            ":priority": priority,
            ":ready_at": ready_at,
        };

        tx.execute(&INSERT_EVAL, params)?;
        let next_id = tx.last_insert_rowid();

        let eval = Eval {
            id: next_id.to_string(),
            status: "pending".into(),
            kind: kind.to_string(),
            job_id: job_id_value.unwrap_or_default(),
            job_config_id: job_config_id_value.unwrap_or_default(),
            node_name: node_id_value.unwrap_or_default(),
            priority,
        };

        // The `eval.enqueued` event rides the SAME transaction as the eval
        // row (JC-41): `scheduler_event` is commutative, so a scheduler-lane
        // INSERT is legal, and the row and its event replicate together.
        let enqueue_event = self.stage_event_locked(&tx, drafts::eval_enqueued(&eval, delay));
        // See `pending_eval_count_tx`: the insert just grew the pending set,
        // so refresh the queue-depth gauge while the tx is still open.
        let pending = Self::pending_eval_count_tx(&tx)?;
        tx.commit()?;
        self.publish_staged(enqueue_event);
        drop(conn);
        crate::manager::telemetry::metrics()
            .eval_queue_depth
            .set(pending);

        if delay.is_zero() && !capture::in_capture() {
            // Zero-delay evals are immediately claimable, so wake one
            // sleeping worker. Under capture the row is uncommitted; the
            // wake happens at apply time from the committed changeset
            // (`project_changeset_side_effects`), on every replica.
            self.eval_notify.notify_one();
        }

        Ok(eval)
    }

    //
    // KV STORE
    //

    #[instrument(err, skip_all)]
    pub fn put_kv(
        &self,
        key: &str,
        value: &str,
        value_type: KvValueType,
        expected_version: Option<i64>,
        lock_id: Option<&str>,
    ) -> R<KvEntry> {
        let key = key.to_owned();
        let value = value.to_owned();
        let lock_id = lock_id.map(str::to_owned);
        self.replicated(Lane::Control, "put_kv", move |s| {
            s.put_kv_apply(
                &key,
                &value,
                value_type,
                expected_version,
                lock_id.as_deref(),
            )
        })
    }

    fn put_kv_apply(
        &self,
        key: &str,
        value: &str,
        value_type: KvValueType,
        expected_version: Option<i64>,
        lock_id: Option<&str>,
    ) -> R<KvEntry> {
        Self::ensure_kv_key(key)?;
        let normalized_value = Self::normalize_kv_value(value, value_type)?;

        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        self.enforce_kv_lock(&tx, key, lock_id)?;

        let existing = tx
            .query_row(
                "SELECT key, value, value_type, version, created_at, updated_at FROM kv_entry WHERE key = ?;",
                [key],
                |row| self.kv_entry_from_row(row),
            )
            .optional()?;

        let now = Self::now_rfc3339()?;
        let value_type_value = value_type as i32;

        let entry = if let Some(existing) = existing {
            if let Some(expected) = expected_version {
                if expected != existing.version {
                    return Err(anyhow::Error::new(StorageError::Conflict {
                        message: format!(
                            "expected version {expected} does not match current version {}",
                            existing.version
                        ),
                    }));
                }
            }

            let next_version = existing.version + 1;
            tx.execute(
                "UPDATE kv_entry SET value = :value, value_type = :value_type, version = :version, updated_at = :updated_at WHERE key = :key;",
                named_params! {
                    ":value": normalized_value,
                    ":value_type": value_type_value,
                    ":version": next_version,
                    ":updated_at": now,
                    ":key": key,
                },
            )?;

            KvEntry {
                key: existing.key,
                value: normalized_value,
                value_type: value_type_value,
                version: next_version,
                created_at: existing.created_at,
                updated_at: now,
            }
        } else {
            if let Some(expected) = expected_version {
                if expected != 0 {
                    return Err(anyhow::Error::new(StorageError::Conflict {
                        message: "kv entry does not exist".to_string(),
                    }));
                }
            }

            tx.execute(
                "INSERT INTO kv_entry (key, value, value_type, version, created_at, updated_at) VALUES (:key, :value, :value_type, :version, :created_at, :updated_at);",
                named_params! {
                    ":key": key,
                    ":value": normalized_value,
                    ":value_type": value_type_value,
                    ":version": 1_i64,
                    ":created_at": now,
                    ":updated_at": now,
                },
            )?;

            KvEntry {
                key: key.to_string(),
                value: normalized_value,
                value_type: value_type_value,
                version: 1,
                created_at: now.clone(),
                updated_at: now,
            }
        };

        tx.commit()?;

        Ok(entry)
    }

    #[instrument(err, skip_all)]
    pub fn get_kv(&self, key: &str) -> R<Option<KvEntry>> {
        Self::ensure_kv_key(key)?;
        let conn = self.read_connection()?;
        let entry = conn
            .query_row(
                "SELECT key, value, value_type, version, created_at, updated_at FROM kv_entry WHERE key = ?;",
                [key],
                |row| self.kv_entry_from_row(row),
            )
            .optional()?;

        Ok(entry)
    }

    #[instrument(err, skip_all)]
    pub fn list_kv(&self, prefix: &str) -> R<Vec<KvEntry>> {
        let conn = self.read_connection()?;
        let pattern = format!("{prefix}%");
        let mut stmt = conn.prepare(
            "SELECT key, value, value_type, version, created_at, updated_at FROM kv_entry WHERE key LIKE ? ORDER BY key ASC;",
        )?;
        let rows = stmt.query_map([pattern], |row| self.kv_entry_from_row(row))?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all)]
    pub fn delete_kv(
        &self,
        key: &str,
        expected_version: Option<i64>,
        lock_id: Option<&str>,
    ) -> R<()> {
        let key = key.to_owned();
        let lock_id = lock_id.map(str::to_owned);
        self.replicated(Lane::Control, "delete_kv", move |s| {
            s.delete_kv_apply(&key, expected_version, lock_id.as_deref())
        })
    }

    fn delete_kv_apply(
        &self,
        key: &str,
        expected_version: Option<i64>,
        lock_id: Option<&str>,
    ) -> R<()> {
        Self::ensure_kv_key(key)?;
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        self.enforce_kv_lock(&tx, key, lock_id)?;

        let existing = tx
            .query_row(
                "SELECT key, value, value_type, version, created_at, updated_at FROM kv_entry WHERE key = ?;",
                [key],
                |row| self.kv_entry_from_row(row),
            )
            .optional()?;

        let Some(existing) = existing else {
            return Err(anyhow::Error::new(StorageError::not_found("kv entry", key)));
        };

        if let Some(expected) = expected_version {
            if expected != existing.version {
                return Err(anyhow::Error::new(StorageError::Conflict {
                    message: format!(
                        "expected version {expected} does not match current version {}",
                        existing.version
                    ),
                }));
            }
        }

        tx.execute("DELETE FROM kv_entry WHERE key = ?;", [key])?;
        tx.commit()?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn patch_kv(
        &self,
        key: &str,
        patch_json: &str,
        expected_version: Option<i64>,
        lock_id: Option<&str>,
    ) -> R<KvEntry> {
        let key = key.to_owned();
        let patch_json = patch_json.to_owned();
        let lock_id = lock_id.map(str::to_owned);
        self.replicated(Lane::Control, "patch_kv", move |s| {
            s.patch_kv_apply(&key, &patch_json, expected_version, lock_id.as_deref())
        })
    }

    fn patch_kv_apply(
        &self,
        key: &str,
        patch_json: &str,
        expected_version: Option<i64>,
        lock_id: Option<&str>,
    ) -> R<KvEntry> {
        Self::ensure_kv_key(key)?;
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        self.enforce_kv_lock(&tx, key, lock_id)?;

        let mut entry = tx
            .query_row(
                "SELECT key, value, value_type, version, created_at, updated_at FROM kv_entry WHERE key = ?;",
                [key],
                |row| self.kv_entry_from_row(row),
            )
            .optional()?
            .ok_or_else(|| anyhow::Error::new(StorageError::not_found("kv entry", key)))?;

        if entry.value_type != KvValueType::Json as i32 {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "kv entry is not JSON".to_string(),
            }));
        }

        if let Some(expected) = expected_version {
            if expected != entry.version {
                return Err(anyhow::Error::new(StorageError::Conflict {
                    message: format!(
                        "expected version {expected} does not match current version {}",
                        entry.version
                    ),
                }));
            }
        }

        let mut current: Value = from_str(&entry.value).map_err(|err| {
            anyhow::Error::new(StorageError::InvalidInput {
                message: format!("invalid stored json value: {err}"),
            })
        })?;
        let patch: json_patch::Patch = from_str(patch_json).map_err(|err| {
            anyhow::Error::new(StorageError::InvalidInput {
                message: format!("invalid json patch: {err}"),
            })
        })?;
        json_patch::patch(&mut current, &patch.0).map_err(|err| {
            anyhow::Error::new(StorageError::InvalidInput {
                message: format!("unable to apply json patch: {err}"),
            })
        })?;

        let updated_value = current.to_string();
        let next_version = entry.version + 1;
        let now = Self::now_rfc3339()?;

        tx.execute(
            "UPDATE kv_entry SET value = :value, version = :version, updated_at = :updated_at WHERE key = :key;",
            named_params! {
                ":value": updated_value,
                ":version": next_version,
                ":updated_at": now,
                ":key": key,
            },
        )?;

        entry.value = updated_value;
        entry.version = next_version;
        entry.updated_at = now;

        tx.commit()?;

        Ok(entry)
    }

    #[instrument(err, skip_all)]
    pub fn acquire_kv_lock(
        &self,
        key: &str,
        lock_id: Option<&str>,
        owner: Option<&str>,
        ttl_seconds: i64,
        force: bool,
    ) -> R<KvLock> {
        let key = key.to_owned();
        let lock_id = lock_id.map(str::to_owned);
        let owner = owner.map(str::to_owned);
        self.replicated(Lane::Control, "acquire_kv_lock", move |s| {
            s.acquire_kv_lock_apply(
                &key,
                lock_id.as_deref(),
                owner.as_deref(),
                ttl_seconds,
                force,
            )
        })
    }

    fn acquire_kv_lock_apply(
        &self,
        key: &str,
        lock_id: Option<&str>,
        owner: Option<&str>,
        ttl_seconds: i64,
        force: bool,
    ) -> R<KvLock> {
        Self::ensure_kv_key(key)?;
        if ttl_seconds <= 0 {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "ttl_seconds must be greater than zero".to_string(),
            }));
        }

        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let now = OffsetDateTime::now_utc();
        let expires_at = now + time::Duration::seconds(ttl_seconds);
        let lock_id_value = lock_id
            .map(str::to_string)
            .unwrap_or_else(Self::random_lock_id);

        if let Some(existing) = self.load_kv_lock_tx(&tx, key)? {
            let expired = Self::parse_rfc3339(&existing.expires_at)? <= now;
            let lock_matches = lock_id
                .map(|id| id == existing.lock_id.as_str())
                .unwrap_or(false);
            if !expired && !lock_matches && !force {
                return Err(anyhow::Error::new(StorageError::Conflict {
                    message: "kv lock is already held".to_string(),
                }));
            }
        }

        let acquired_at = now.format(&Rfc3339)?;
        let expires_at = expires_at.format(&Rfc3339)?;
        let owner_value = owner.map(str::to_string);

        tx.execute(
            "INSERT INTO kv_lock (key, lock_id, owner, acquired_at, expires_at) VALUES (:key, :lock_id, :owner, :acquired_at, :expires_at)\n            ON CONFLICT(key) DO UPDATE SET lock_id = :lock_id, owner = :owner, acquired_at = :acquired_at, expires_at = :expires_at;",
            named_params! {
                ":key": key,
                ":lock_id": lock_id_value,
                ":owner": owner_value,
                ":acquired_at": acquired_at,
                ":expires_at": expires_at,
            },
        )?;

        tx.commit()?;

        Ok(KvLock {
            key: key.to_string(),
            lock_id: lock_id_value,
            owner: owner_value,
            acquired_at,
            expires_at,
        })
    }

    #[instrument(err, skip_all)]
    pub fn renew_kv_lock(&self, key: &str, lock_id: &str, ttl_seconds: i64) -> R<KvLock> {
        let key = key.to_owned();
        let lock_id = lock_id.to_owned();
        self.replicated(Lane::Control, "renew_kv_lock", move |s| {
            s.renew_kv_lock_apply(&key, &lock_id, ttl_seconds)
        })
    }

    fn renew_kv_lock_apply(&self, key: &str, lock_id: &str, ttl_seconds: i64) -> R<KvLock> {
        Self::ensure_kv_key(key)?;
        if ttl_seconds <= 0 {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "ttl_seconds must be greater than zero".to_string(),
            }));
        }

        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let Some(mut lock) = self.load_kv_lock_tx(&tx, key)? else {
            return Err(anyhow::Error::new(StorageError::not_found("kv lock", key)));
        };

        let now = OffsetDateTime::now_utc();
        let expires_at = Self::parse_rfc3339(&lock.expires_at)?;
        if expires_at <= now {
            tx.execute("DELETE FROM kv_lock WHERE key = ?;", [key])?;
            return Err(anyhow::Error::new(StorageError::Conflict {
                message: "kv lock has expired".to_string(),
            }));
        }

        if lock.lock_id != lock_id {
            return Err(anyhow::Error::new(StorageError::Conflict {
                message: "kv lock id does not match".to_string(),
            }));
        }

        let new_expires_at = (now + time::Duration::seconds(ttl_seconds)).format(&Rfc3339)?;
        tx.execute(
            "UPDATE kv_lock SET expires_at = :expires_at WHERE key = :key;",
            named_params! {
                ":expires_at": new_expires_at,
                ":key": key,
            },
        )?;

        lock.expires_at = new_expires_at;
        tx.commit()?;

        Ok(lock)
    }

    #[instrument(err, skip_all)]
    pub fn release_kv_lock(&self, key: &str, lock_id: &str) -> R<()> {
        let key = key.to_owned();
        let lock_id = lock_id.to_owned();
        self.replicated(Lane::Control, "release_kv_lock", move |s| {
            s.release_kv_lock_apply(&key, &lock_id)
        })
    }

    fn release_kv_lock_apply(&self, key: &str, lock_id: &str) -> R<()> {
        Self::ensure_kv_key(key)?;
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let Some(lock) = self.load_kv_lock_tx(&tx, key)? else {
            return Err(anyhow::Error::new(StorageError::not_found("kv lock", key)));
        };

        if lock.lock_id != lock_id {
            return Err(anyhow::Error::new(StorageError::Conflict {
                message: "kv lock id does not match".to_string(),
            }));
        }

        tx.execute("DELETE FROM kv_lock WHERE key = ?;", [key])?;
        tx.commit()?;

        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn get_kv_lock(&self, key: &str) -> R<Option<KvLock>> {
        let key = key.to_owned();
        self.replicated(Lane::Control, "get_kv_lock", move |s| {
            s.get_kv_lock_apply(&key)
        })
    }

    fn get_kv_lock_apply(&self, key: &str) -> R<Option<KvLock>> {
        Self::ensure_kv_key(key)?;
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let lock = self.load_kv_lock_tx(&tx, key)?;
        let Some(lock) = lock else {
            tx.commit()?;
            return Ok(None);
        };

        let expires_at = Self::parse_rfc3339(&lock.expires_at)?;
        if expires_at <= OffsetDateTime::now_utc() {
            tx.execute("DELETE FROM kv_lock WHERE key = ?;", [key])?;
            tx.commit()?;
            return Ok(None);
        }

        tx.commit()?;
        Ok(Some(lock))
    }

    #[instrument(err, skip_all)]
    pub fn query_state_store(&self, sql: &str, allow_write: bool) -> R<StateStoreQueryResult> {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "sql must not be empty".to_string(),
            }));
        }

        let statement_type = Self::sqlite_statement_type(trimmed);
        if !allow_write && !Self::is_read_only_statement(&statement_type) {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: format!(
                    "statement type '{statement_type}' is not allowed in read-only mode; pass allow_write to enable mutating queries"
                ),
            }));
        }

        if Self::is_query_statement(&statement_type) {
            let conn = if allow_write {
                self.write_connection()?
            } else {
                self.read_connection()?
            };
            let mut stmt = conn.prepare(trimmed)?;
            let columns: Vec<String> = stmt
                .column_names()
                .iter()
                .map(|column| (*column).to_string())
                .collect();
            let mut rows = stmt.query([])?;
            let mut output_rows = Vec::new();
            while let Some(row) = rows.next()? {
                let mut map = serde_json::Map::new();
                for (index, column_name) in columns.iter().enumerate() {
                    let value = match row.get_ref(index)? {
                        ValueRef::Null => Value::Null,
                        ValueRef::Integer(value) => Value::from(value),
                        ValueRef::Real(value) => Value::from(value),
                        ValueRef::Text(value) => {
                            Value::from(String::from_utf8_lossy(value).to_string())
                        }
                        ValueRef::Blob(value) => Value::from(format!(
                            "0x{}",
                            value
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>()
                        )),
                    };
                    map.insert(column_name.clone(), value);
                }
                output_rows.push(StateStoreQueryRow {
                    json: Value::Object(map).to_string(),
                });
            }

            return Ok(StateStoreQueryResult {
                statement_type,
                columns,
                rows: output_rows,
                affected_rows: 0,
            });
        }

        // Non-query statements reach here only with `allow_write` (the
        // read-only check above rejected them otherwise): this is the one
        // mutating entry of `query_state_store`, so only it is replicated.
        //
        // …but raft replicates ROW changes captured as SQLite session
        // changesets, and a session records no schema changes at all. In
        // raft mode a DDL statement would therefore run on this node and
        // reach no other — and, because JC-26 skips proposing an empty
        // changeset, it would be reported back as a success that never
        // replicated (adversarial finding, 2026-07-25: the seam lied). Fail
        // loudly with the actionable alternative instead.
        if self.raft_mode() && !Self::is_row_mutating_statement(&statement_type) {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: format!(
                    "statement type '{statement_type}' cannot be executed in raft mode: raft \
                     replicates row changes captured as SQLite session changesets, which do not \
                     include schema changes, so this statement would apply on this node only and \
                     diverge it from the cluster. Schema changes must ship as a migration in \
                     src/storage/migrations.rs (every node runs them at startup); \
                     INSERT/UPDATE/DELETE statements are replicated normally"
                ),
            }));
        }
        let sql = trimmed.to_owned();
        self.replicated(Lane::Control, "query_state_store", move |s| {
            s.query_state_store_apply(&sql, &statement_type)
        })
    }

    fn query_state_store_apply(&self, sql: &str, statement_type: &str) -> R<StateStoreQueryResult> {
        let conn = self.write_connection()?;
        let affected_rows = conn.execute(sql, [])? as u64;

        Ok(StateStoreQueryResult {
            statement_type: statement_type.to_owned(),
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows,
        })
    }

    #[instrument(err, skip_all)]
    pub fn state_store_schema(&self) -> R<Vec<StateStoreSchemaObject>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT type, name, COALESCE(sql, '') FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY type, name;",
        )?;
        let mut objects = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let kind: String = row.get(0)?;
            let name: String = row.get(1)?;
            let create_sql: String = row.get(2)?;
            let pragma = format!("PRAGMA table_info('{}');", name.replace('\'', "''"));
            let mut column_stmt = conn.prepare(&pragma)?;
            let columns_iter = column_stmt.query_map([], |column_row| {
                Ok(StateStoreSchemaColumn {
                    ordinal: column_row.get(0)?,
                    name: column_row.get(1)?,
                    data_type: column_row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                    not_null: column_row.get::<_, i64>(3)? != 0,
                    default_value: column_row.get(4)?,
                    primary_key: column_row.get::<_, i64>(5)? != 0,
                })
            })?;
            objects.push(StateStoreSchemaObject {
                kind,
                name,
                create_sql,
                columns: Self::collect_rows(columns_iter)?,
            });
        }

        Ok(objects)
    }

    #[instrument(err, skip_all)]
    pub fn list_saved_state_queries(&self) -> R<Vec<SavedStateQueryRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT name, sql, created_at, updated_at FROM saved_state_query ORDER BY name ASC;",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(SavedStateQueryRecord {
                name: row.get(0)?,
                sql: row.get(1)?,
                created_at: row.get(2)?,
                updated_at: row.get(3)?,
            })
        })?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all)]
    pub fn upsert_saved_state_query(&self, name: &str, sql: &str) -> R<SavedStateQueryRecord> {
        let name = name.to_owned();
        let sql = sql.to_owned();
        self.replicated(Lane::Control, "upsert_saved_state_query", move |s| {
            s.upsert_saved_state_query_apply(&name, &sql)
        })
    }

    fn upsert_saved_state_query_apply(&self, name: &str, sql: &str) -> R<SavedStateQueryRecord> {
        let name = name.trim();
        let sql = sql.trim();
        if name.is_empty() {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "saved query name must not be empty".to_string(),
            }));
        }
        if sql.is_empty() {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "saved query sql must not be empty".to_string(),
            }));
        }

        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        tx.execute(
            "INSERT INTO saved_state_query (name, sql, created_at, updated_at) VALUES (:name, :sql, :now, :now) ON CONFLICT(name) DO UPDATE SET sql = excluded.sql, updated_at = excluded.updated_at;",
            named_params! {
                ":name": name,
                ":sql": sql,
                ":now": now,
            },
        )?;
        let query = tx.query_row(
            "SELECT name, sql, created_at, updated_at FROM saved_state_query WHERE name = ?;",
            [name],
            |row| {
                Ok(SavedStateQueryRecord {
                    name: row.get(0)?,
                    sql: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                })
            },
        )?;
        tx.commit()?;
        Ok(query)
    }

    /// Look up a previously recorded response for an idempotency `key`.
    ///
    /// Returns `Some(response_message)` when a request with this key was already
    /// applied, allowing the caller to replay the prior outcome instead of
    /// re-applying. An empty key disables deduplication and always returns
    /// `None`.
    pub fn idempotency_lookup(&self, key: &str) -> R<Option<String>> {
        let key = key.trim();
        if key.is_empty() {
            return Ok(None);
        }
        let conn = self.read_connection()?;
        let result = conn
            .query_row(
                "SELECT response_message FROM idempotency_record WHERE key = ?;",
                [key],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(result)
    }

    /// Record the response for an idempotency `key`.
    ///
    /// Uses `INSERT OR IGNORE` so a concurrent duplicate cannot overwrite an
    /// existing record. Returns `true` when the record was newly inserted and
    /// `false` when a record already existed (the caller raced another request
    /// carrying the same key). An empty key is a no-op and returns `true`.
    pub fn idempotency_record(&self, key: &str, operation: &str, message: &str) -> R<bool> {
        let key = key.to_owned();
        let operation = operation.to_owned();
        let message = message.to_owned();
        self.replicated(Lane::Control, "idempotency_record", move |s| {
            s.idempotency_record_apply(&key, &operation, &message)
        })
    }

    fn idempotency_record_apply(&self, key: &str, operation: &str, message: &str) -> R<bool> {
        let key = key.trim();
        if key.is_empty() {
            return Ok(true);
        }
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let conn = self.write_connection()?;
        let changed = conn.execute(
            "INSERT OR IGNORE INTO idempotency_record (key, operation, response_message, created_at) VALUES (:key, :op, :msg, :now);",
            named_params! {
                ":key": key,
                ":op": operation,
                ":msg": message,
                ":now": now,
            },
        )?;
        Ok(changed > 0)
    }

    fn job_template_version_from_row(
        row: &Row<'_>,
    ) -> Result<JobTemplateVersionRecord, RusqliteError> {
        let variables_json: String = row.get(4)?;
        let variables: Vec<JobTemplateVariableRecord> = serde_json::from_str(&variables_json)
            .map_err(|err| {
                RusqliteError::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;
        Ok(JobTemplateVersionRecord {
            name: row.get(0)?,
            version: row.get::<_, i64>(1)? as u32,
            latest_version: row.get::<_, i64>(7)? as u32,
            description: row.get(3)?,
            source_jsonnet: row.get(2)?,
            variables,
            created_at: row.get(5)?,
            created_by: row.get(6)?,
        })
    }

    /// Stores the next immutable version of a job template, creating the
    /// template row on first use.
    #[instrument(err, skip_all)]
    pub fn upsert_job_template_version(
        &self,
        name: &str,
        description: Option<&str>,
        source_jsonnet: &str,
        variables: &[JobTemplateVariableRecord],
        created_by: &str,
    ) -> R<JobTemplateVersionRecord> {
        let name = name.trim();
        if name.is_empty() {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "template name must not be empty".to_string(),
            }));
        }

        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let variables_json = serde_json::to_string(variables)?;
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;

        let current_version: Option<i64> = tx
            .query_row(
                "SELECT latest_version FROM job_template WHERE name = ?;",
                [name],
                |row| row.get(0),
            )
            .optional()?;
        let next_version = current_version.unwrap_or(0) + 1;

        tx.execute(
            "INSERT INTO job_template (name, latest_version, created_at, updated_at) VALUES (:name, :version, :now, :now) ON CONFLICT(name) DO UPDATE SET latest_version = excluded.latest_version, updated_at = excluded.updated_at;",
            named_params! {
                ":name": name,
                ":version": next_version,
                ":now": now,
            },
        )?;
        tx.execute(
            "INSERT INTO job_template_version (template_name, version, description, source_jsonnet, variables, created_by, created_at) VALUES (:name, :version, :description, :source, :variables, :created_by, :now);",
            named_params! {
                ":name": name,
                ":version": next_version,
                ":description": description,
                ":source": source_jsonnet,
                ":variables": variables_json,
                ":created_by": created_by,
                ":now": now,
            },
        )?;
        let stored = tx.query_row(
            "SELECT tv.template_name, tv.version, tv.source_jsonnet, tv.description, tv.variables, tv.created_at, tv.created_by, t.latest_version
             FROM job_template_version tv
             INNER JOIN job_template t ON t.name = tv.template_name
             WHERE tv.template_name = ? AND tv.version = ?;",
            params![name, next_version],
            Self::job_template_version_from_row,
        )?;
        tx.commit()?;
        Ok(stored)
    }

    /// One template version (latest when `version` is `None`).
    #[instrument(err, skip_all)]
    pub fn get_job_template_version(
        &self,
        name: &str,
        version: Option<u32>,
    ) -> R<JobTemplateVersionRecord> {
        let conn = self.read_connection()?;
        let base = "SELECT tv.template_name, tv.version, tv.source_jsonnet, tv.description, tv.variables, tv.created_at, tv.created_by, t.latest_version
             FROM job_template_version tv
             INNER JOIN job_template t ON t.name = tv.template_name
             WHERE tv.template_name = ?";
        let result = match version {
            Some(version) => conn.query_row(
                &format!("{base} AND tv.version = ?;"),
                params![name, i64::from(version)],
                Self::job_template_version_from_row,
            ),
            None => conn.query_row(
                &format!("{base} AND tv.version = t.latest_version;"),
                params![name],
                Self::job_template_version_from_row,
            ),
        };
        result.optional()?.ok_or_else(|| {
            let wanted = match version {
                Some(version) => format!("{name} v{version}"),
                None => name.to_string(),
            };
            anyhow::Error::new(StorageError::not_found("job_template", wanted))
        })
    }

    /// Version-history metadata for one template, newest first.
    #[instrument(err, skip_all)]
    pub fn list_job_template_versions(&self, name: &str) -> R<Vec<JobTemplateVersionInfoRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT version, created_at, created_by FROM job_template_version WHERE template_name = ? ORDER BY version DESC;",
        )?;
        let rows = stmt.query_map([name], |row| {
            Ok(JobTemplateVersionInfoRecord {
                version: row.get::<_, i64>(0)? as u32,
                created_at: row.get(1)?,
                created_by: row.get(2)?,
            })
        })?;
        Self::collect_rows(rows)
    }

    /// Latest-version summaries of every template, most recently updated
    /// first. ACL filtering happens in the caller.
    #[instrument(err, skip_all)]
    pub fn list_job_templates(&self) -> R<Vec<JobTemplateSummaryRecord>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT t.name, t.latest_version, tv.description, tv.variables, t.updated_at
             FROM job_template t
             INNER JOIN job_template_version tv
                ON tv.template_name = t.name AND tv.version = t.latest_version
             ORDER BY t.updated_at DESC, t.name ASC;",
        )?;
        let rows = stmt.query_map([], |row| {
            let variables_json: String = row.get(3)?;
            let variables: Vec<JobTemplateVariableRecord> = serde_json::from_str(&variables_json)
                .map_err(|err| {
                RusqliteError::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;
            Ok(JobTemplateSummaryRecord {
                name: row.get(0)?,
                latest_version: row.get::<_, i64>(1)? as u32,
                description: row.get(2)?,
                variable_count: variables.len() as u32,
                updated_at: row.get(4)?,
            })
        })?;
        Self::collect_rows(rows)
    }

    /// Deletes a template and every stored version.
    #[instrument(err, skip_all)]
    pub fn delete_job_template(&self, name: &str) -> R<()> {
        let mut conn = self.write_connection()?;
        let tx = conn.transaction()?;
        // The FK is declared with ON DELETE CASCADE but SQLite only honors it
        // with foreign_keys=ON, so delete versions explicitly.
        tx.execute(
            "DELETE FROM job_template_version WHERE template_name = ?;",
            [name],
        )?;
        let deleted = tx.execute("DELETE FROM job_template WHERE name = ?;", [name])?;
        tx.commit()?;
        if deleted == 0 {
            return Err(anyhow::Error::new(StorageError::not_found(
                "job_template",
                name,
            )));
        }
        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn delete_saved_state_query(&self, name: &str) -> R<()> {
        let name = name.to_owned();
        self.replicated(Lane::Control, "delete_saved_state_query", move |s| {
            s.delete_saved_state_query_apply(&name)
        })
    }

    fn delete_saved_state_query_apply(&self, name: &str) -> R<()> {
        let name = name.trim();
        if name.is_empty() {
            return Err(anyhow::Error::new(StorageError::InvalidInput {
                message: "saved query name must not be empty".to_string(),
            }));
        }

        let conn = self.write_connection()?;
        let deleted = conn.execute("DELETE FROM saved_state_query WHERE name = ?;", [name])?;
        if deleted == 0 {
            return Err(anyhow::Error::new(StorageError::not_found(
                "saved_state_query",
                name,
            )));
        }
        Ok(())
    }

    fn sqlite_statement_type(sql: &str) -> String {
        sql.split_whitespace()
            .next()
            .unwrap_or("UNKNOWN")
            .trim_matches(|c: char| !c.is_ascii_alphanumeric())
            .to_uppercase()
    }

    fn is_query_statement(statement_type: &str) -> bool {
        matches!(statement_type, "SELECT" | "PRAGMA" | "WITH" | "EXPLAIN")
    }

    fn is_read_only_statement(statement_type: &str) -> bool {
        Self::is_query_statement(statement_type)
    }

    /// Whether a statement mutates ROWS (and is therefore representable as a
    /// session changeset, i.e. replicable). Everything else that is not a
    /// query — DDL, `VACUUM`, `ATTACH`, … — changes state a changeset cannot
    /// carry, which `query_state_store` refuses in raft mode.
    fn is_row_mutating_statement(statement_type: &str) -> bool {
        matches!(statement_type, "INSERT" | "UPDATE" | "DELETE" | "REPLACE")
    }

    #[instrument(err, skip_all)]
    pub fn replace_job_kv_references(&self, job_id: &str, refs: &[JobKvReference]) -> R<()> {
        let job_id = job_id.to_owned();
        let refs = refs.to_vec();
        self.replicated(Lane::Control, "replace_job_kv_references", move |s| {
            s.replace_job_kv_references_apply(&job_id, &refs)
        })
    }

    fn replace_job_kv_references_apply(&self, job_id: &str, refs: &[JobKvReference]) -> R<()> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        tx.execute("DELETE FROM job_kv_reference WHERE job_id = ?;", [job_id])?;
        for reference in refs {
            tx.execute(
                "INSERT INTO job_kv_reference (job_id, job_config_id, kv_key, json_pointer) VALUES (?1, ?2, ?3, ?4);",
                params![
                    reference.job_id,
                    reference.job_config_id,
                    reference.kv_key,
                    reference.json_pointer,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    #[instrument(err, skip_all)]
    pub fn job_kv_references_for_key(&self, key: &str) -> R<Vec<JobKvReference>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT job_id, job_config_id, kv_key, json_pointer FROM job_kv_reference WHERE kv_key = ?;",
        )?;
        let rows = stmt.query_map([key], |row| self.job_kv_reference_from_row(row))?;
        Self::collect_rows(rows)
    }

    #[instrument(err, skip_all)]
    pub fn job_kv_references_for_job(&self, job_id: &str) -> R<Vec<JobKvReference>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT job_id, job_config_id, kv_key, json_pointer FROM job_kv_reference WHERE job_id = ?;",
        )?;
        let rows = stmt.query_map([job_id], |row| self.job_kv_reference_from_row(row))?;
        Self::collect_rows(rows)
    }

    // =====
    // VOLUMES
    // =====
    //
    // The manager owns the volume state machine; workers own the drivers. See
    // docs/volumes.md and internal_docs/plans/volume-plugin-system-plan.md.

    fn volume_from_row(&self, row: &Row<'_>) -> Result<Volume, rusqlite::Error> {
        let state: String = row.get(6)?;
        Ok(Volume {
            volume_id: row.get(0)?,
            name: row.get(1)?,
            driver: row.get(2)?,
            job_id: row.get(3)?,
            instance_id: row.get(4)?,
            task_name: row.get(5)?,
            state: state.parse().map_err(|err: String| {
                rusqlite::Error::FromSqlConversionFailure(
                    6,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
                )
            })?,
            node_id: row.get(7)?,
            node_local: row.get::<_, i64>(8)? != 0,
            read_only: row.get::<_, i64>(9)? != 0,
            params: parse_json_map_column(10, row.get(10)?)?,
            attrs: parse_json_map_column(11, row.get(11)?)?,
            mount_path: row.get(12)?,
            last_error: row.get(13)?,
            created_at: row.get(14)?,
            updated_at: row.get(15)?,
            volume_mode: row.get::<_, String>(16)?.parse().map_err(|err: String| {
                rusqlite::Error::FromSqlConversionFailure(
                    16,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
                )
            })?,
            fs: row.get(17)?,
        })
    }

    /// Mint an instance's storage handle and the volume row behind it.
    ///
    /// Both happen in the SAME transaction as the placement, so a handle can
    /// never exist without a row — which is what lets the reaper treat "no
    /// instance references this volume" as an unambiguous reclaim signal.
    /// Returns `None` for tasks that declare no `stateful.storage`.
    fn mint_instance_volume_tx(
        &self,
        tx: &Connection,
        instance_id: &str,
        job_id: &str,
        task_name: &str,
        storage: Option<&crate::structs::StatefulStorageConfig>,
    ) -> R<Option<String>> {
        let Some(storage) = storage else {
            return Ok(None);
        };
        let driver = storage.driver_name().to_string();
        // Mint the handle first so the format lives in exactly one place, then
        // split the id back out for the row.
        let handle = mint_storage_handle(&driver);
        let volume_id = handle
            .split_once('/')
            .map(|(_, id)| id.to_string())
            .context("minted storage handle is malformed")?;
        let now = Self::now_rfc3339()?;
        let params = serde_json::to_string(&storage.param_map())?;
        // The manager learns node-locality from worker fingerprints. Default
        // to `true` when nothing advertises the plugin yet: wrongly pinning
        // costs a placement delay, while wrongly NOT pinning silently starts
        // the workload on an empty volume. Corrected on first mount, when the
        // worker reports what the driver actually said.
        let node_local = self.driver_is_node_local_tx(tx, &driver)?;
        tx.execute(
            "INSERT INTO volume (volume_id, name, driver, job_id, instance_id, task_name, \
             state, node_id, node_local, read_only, params, attrs, mount_path, last_error, \
             created_at, updated_at, volume_mode, fs) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', NULL, ?7, 0, ?8, '{}', NULL, NULL, ?9, \
             ?9, ?10, ?11);",
            params![
                volume_id,
                crate::structs::INSTANCE_VOLUME_NAME,
                driver,
                job_id,
                instance_id,
                task_name,
                node_local as i64,
                params,
                now,
                storage.volume_mode().as_str(),
                (storage.volume_mode() == crate::volumes::VolumeMode::Block).then(|| storage
                    .param_map()
                    .get("fs")
                    .and_then(Value::as_str)
                    .unwrap_or(crate::volumes::DEFAULT_BLOCK_FS)
                    .to_string()),
            ],
        )?;
        info!(volume_id, driver, instance_id, "Minted instance volume");
        Ok(Some(handle))
    }

    /// Upsert the volume rows behind a task's `volumes[]` claims.
    ///
    /// Claims are **shared**: their id is derived from
    /// `(job, group, task, name)`, so every allocation of the task mounts the
    /// same volume. That is what "claim of storage that already exists" means —
    /// a read-only `/mnt/models` shared by ten replicas is the point. Per-
    /// allocation scratch is `$LEGION_TASK_DIR`, not a volume.
    ///
    /// Unlike instance volumes these have no `instance_id`, so instance churn
    /// never reclaims them; only a job destroy (via the reaper) or an explicit
    /// `legion volume delete` does.
    fn sync_task_volume_claims_tx(
        &self,
        tx: &Connection,
        job_id: &str,
        task: &TaskConfig,
    ) -> R<()> {
        let now = Self::now_rfc3339()?;
        for claim in &task.volumes {
            let volume_id = claim_volume_id(job_id, &task.group, &task.name, &claim.name);
            let driver = claim.driver_name().to_string();
            let node_local = self.driver_is_node_local_tx(tx, &driver)?;
            let params = serde_json::to_string(&claim.param_map())?;
            // Re-submitting a job refreshes params/driver without resetting a
            // live volume's node pin or state — EXCEPT when the row is a
            // tombstone. A claim id is a pure function of (job, group, task,
            // name), so re-declaring one after a job destroy lands on the same
            // row; leaving it `destroyed` would make the claim invisible in
            // `legion volume list` and immune to the reaper, even though the
            // worker happily mounts it.
            tx.execute(
                "INSERT INTO volume (volume_id, name, driver, job_id, instance_id, task_name, \
                 state, node_id, node_local, read_only, params, attrs, mount_path, last_error, \
                 created_at, updated_at, volume_mode) \
                 VALUES (?1, ?2, ?3, ?4, NULL, ?5, 'pending', NULL, ?6, ?7, ?8, '{}', NULL, \
                 NULL, ?9, ?9, ?10) \
                 ON CONFLICT(volume_id) DO UPDATE SET \
                 driver = ?3, job_id = ?4, task_name = ?5, node_local = ?6, read_only = ?7, \
                 params = ?8, updated_at = ?9, volume_mode = ?10, \
                 state = CASE WHEN volume.state = 'destroyed' THEN 'pending' ELSE volume.state END, \
                 last_error = CASE WHEN volume.state = 'destroyed' THEN NULL ELSE volume.last_error END;",
                params![
                    volume_id,
                    claim.name,
                    driver,
                    job_id,
                    task.name,
                    node_local as i64,
                    claim.is_read_only() as i64,
                    params,
                    now,
                    claim.volume_mode().as_str(),
                ],
            )?;
        }
        Ok(())
    }

    /// Scheduler-lane half of the submit composite (plan §7-C2 / JC-33):
    /// upsert the `volume` claim rows for every task of `job_config_id`.
    ///
    /// Split out of `handle_job_submit_apply` because `volume` is
    /// scheduler-owned while job submit proposes on the control lane — see
    /// the sequencing rationale on [`Store::handle_job_submit`]. Task
    /// identity comes from the control-lane rows the first half committed,
    /// read here.
    ///
    /// Idempotent: claim ids are a pure function of
    /// `(job, group, task, name)`, so re-running upserts the same rows with
    /// the same values, and a retry after a failed attempt converges.
    fn sync_task_volume_claims_apply(&self, job_config_id: &str) -> R<()> {
        let Some(job_id) = self.job_id_for_job_config(job_config_id)? else {
            // The config vanished between the halves (a destroy raced the
            // submit); there is nothing to claim storage for.
            return Ok(());
        };
        let tasks = self.get_task_configs_for_job_config(job_config_id)?;
        if tasks.iter().all(|(_, task)| task.volumes.is_empty()) {
            // No claims: skip the write entirely so a submit of a
            // volume-less job produces an empty changeset (JC-26 keeps it
            // off the raft log).
            return Ok(());
        }
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        for (_, task) in &tasks {
            self.sync_task_volume_claims_tx(&tx, &job_id, task)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Scheduler-lane reconciler for volume-claim syncs deferred under
    /// divergent lane leadership (plan §8, D1): for every active job whose
    /// LATEST config declares task volumes, upsert any missing `volume`
    /// claim rows via the same idempotent sync the submit composite runs
    /// ([`Self::handle_job_submit`] tolerates a scheduler-lane rejection of
    /// that half). Run by the node-status-checker loop on the
    /// scheduler-lane leader. Steady state is read-only: a config whose
    /// claims all exist is skipped before any replicated op. Returns how
    /// many configs were synced.
    pub fn sync_missing_task_volume_claims(&self) -> R<u64> {
        let candidates: Vec<(String, String)> = {
            let conn = self.read_connection()?;
            let mut stmt = conn.prepare(&format!(
                "SELECT j.id, {LATEST_CONFIG_FOR_JOB}
                 FROM job j
                 WHERE j.status NOT IN ('stopped', 'failed');"
            ))?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })?;
            Self::collect_rows(rows)?
                .into_iter()
                .filter_map(|(job, config)| config.map(|config| (job, config)))
                .collect()
        };
        let mut synced = 0u64;
        for (job_id, job_config_id) in candidates {
            let tasks = self.get_task_configs_for_job_config(&job_config_id)?;
            let mut missing = false;
            {
                let conn = self.read_connection()?;
                'tasks: for (_, task) in &tasks {
                    for claim in &task.volumes {
                        let id = claim_volume_id(&job_id, &task.group, &task.name, &claim.name);
                        let exists: bool = conn.query_row(
                            "SELECT EXISTS(SELECT 1 FROM volume WHERE volume_id = ?1);",
                            [&id],
                            |row| row.get(0),
                        )?;
                        if !exists {
                            missing = true;
                            break 'tasks;
                        }
                    }
                }
            }
            if !missing {
                continue;
            }
            let config_id = job_config_id.clone();
            self.replicated(Lane::Scheduler, "sync_task_volume_claims", move |s| {
                s.sync_task_volume_claims_apply(&config_id)
            })?;
            synced += 1;
        }
        Ok(synced)
    }

    /// Jobs whose latest config carries an acknowledged UPDATE that never
    /// got its rollout (plan §8, D1): active job, latest config has a
    /// predecessor, NO rollout row targets the latest config (any state —
    /// an operator's cancelled rollout must stay cancelled), the latest
    /// config has no allocations, and an older config still has some. This
    /// is precisely the signature left by a submit whose scheduler-lane
    /// rollout creation was rejected under divergent lane leadership, or by
    /// a crash between the submit and the rollout insert. Returns
    /// `(job_id, latest_config_id, previous_config_id)` rows; consumed by
    /// the manager's `reconcile_missing_rollouts` pass on the
    /// scheduler-lane leader, which runs before every eval regeneration so
    /// the regeneration's `job_submit` fallback cannot bypass a rollout the
    /// job deserved.
    pub fn jobs_with_unrolled_latest_config(&self) -> R<Vec<(String, String, Option<String>)>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT j.id, latest.id, (
                 SELECT jc2.id FROM job_config jc2
                 WHERE jc2.job_id = j.id AND jc2.id != latest.id
                 ORDER BY jc2.created_at DESC LIMIT 1
             )
             FROM job j
             INNER JOIN job_config latest ON latest.id = {LATEST_CONFIG_FOR_JOB}
             WHERE j.status NOT IN ('stopped', 'failed')
               AND EXISTS (
                 SELECT 1 FROM job_config prev
                 WHERE prev.job_id = j.id AND prev.id != latest.id
               )
               AND NOT EXISTS (
                 SELECT 1 FROM rollout r WHERE r.to_job_config_id = latest.id
               )
               AND NOT EXISTS (
                 SELECT 1 FROM allocation a WHERE a.job_config_id = latest.id
               )
               AND EXISTS (
                 SELECT 1 FROM allocation a
                 INNER JOIN job_config jc ON jc.id = a.job_config_id
                 WHERE jc.job_id = j.id AND a.job_config_id != latest.id
               );"
        ))?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get::<_, Option<String>>(2)?))
        })?;
        Self::collect_rows(rows)
    }

    /// The volume mounts a worker must materialize for one allocation.
    ///
    /// Resolves the task's declarations against the `volume` table so the
    /// worker receives concrete ids and the driver attrs from previous mounts —
    /// it never re-derives a volume from the jobspec. Instance storage comes
    /// first so `LEGION_STORAGE_DIR` is unambiguous.
    #[instrument(err, skip_all)]
    pub fn volume_mounts_for_allocation(
        &self,
        job_id: &str,
        task: &TaskConfig,
        instance: Option<&Instance>,
    ) -> R<Vec<crate::structs::VolumeMount>> {
        let mut mounts = Vec::new();

        if let (Some(storage), Some(instance)) = (
            task.stateful
                .as_ref()
                .and_then(|stateful| stateful.storage.as_ref()),
            instance,
        ) {
            if let Some(handle) = instance.storage_handle.as_deref() {
                let handle = crate::volumes::StorageHandle::parse(handle)?;
                let attrs = self
                    .get_volume(&handle.volume_id)?
                    .map(|volume| volume.attrs)
                    .unwrap_or_default();
                mounts.push(crate::structs::VolumeMount {
                    volume_id: handle.volume_id,
                    name: crate::structs::INSTANCE_VOLUME_NAME.to_string(),
                    driver: handle.driver,
                    mode: storage.volume_mode(),
                    params: storage.param_map(),
                    attrs,
                    path: Some(storage.path.clone()),
                    read_only: false,
                    instance_volume: true,
                });
            }
        }

        for claim in &task.volumes {
            let volume_id = claim_volume_id(job_id, &task.group, &task.name, &claim.name);
            let attrs = self
                .get_volume(&volume_id)?
                .map(|volume| volume.attrs)
                .unwrap_or_default();
            mounts.push(crate::structs::VolumeMount {
                volume_id,
                name: claim.name.clone(),
                driver: claim.driver_name().to_string(),
                mode: claim.volume_mode(),
                params: claim.param_map(),
                attrs,
                path: claim.path.clone(),
                read_only: claim.is_read_only(),
                instance_volume: false,
            });
        }

        Ok(mounts)
    }

    /// Whether any node advertises `driver` as node-local. Unknown drivers are
    /// treated as node-local (see [`Self::mint_instance_volume_tx`]).
    fn driver_is_node_local_tx(&self, tx: &Connection, driver: &str) -> R<bool> {
        let mut stmt = tx.prepare("SELECT fingerprint_attrs FROM node;")?;
        let rows = stmt.query_map([], |row| row.get::<_, Value>(0))?;
        for row in rows {
            let Ok(attrs) = row else { continue };
            if let Some(node_local) = attrs
                .get("volumeDrivers")
                .and_then(|drivers| drivers.get(driver))
                .and_then(|caps| caps.get("nodeLocal"))
                .and_then(Value::as_bool)
            {
                return Ok(node_local);
            }
        }
        Ok(true)
    }

    #[instrument(err, skip_all)]
    pub fn get_volume(&self, volume_id: &str) -> R<Option<Volume>> {
        let conn = self.read_connection()?;
        let volume = conn
            .query_row(VOLUME_SELECT.as_str(), [volume_id], |row| {
                self.volume_from_row(row)
            })
            .optional()?;
        Ok(volume)
    }

    /// Volumes matching the given filters, newest first. Every filter is
    /// optional; passing none lists the whole cluster.
    #[instrument(err, skip_all)]
    pub fn list_volumes(&self, filter: &VolumeFilter) -> R<Vec<Volume>> {
        let conn = self.read_connection()?;
        let mut sql = VOLUME_SELECT_ALL.clone();
        let mut clauses: Vec<String> = Vec::new();
        let mut args: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        if let Some(job_id) = filter.job_id.as_deref() {
            clauses.push(format!("job_id = ?{}", args.len() + 1));
            args.push(Box::new(job_id.to_string()));
        }
        if let Some(node_id) = filter.node_id.as_deref() {
            clauses.push(format!("node_id = ?{}", args.len() + 1));
            args.push(Box::new(node_id.to_string()));
        }
        if let Some(driver) = filter.driver.as_deref() {
            clauses.push(format!("driver = ?{}", args.len() + 1));
            args.push(Box::new(driver.to_string()));
        }
        if let Some(instance_id) = filter.instance_id.as_deref() {
            clauses.push(format!("instance_id = ?{}", args.len() + 1));
            args.push(Box::new(instance_id.to_string()));
        }
        if let Some(state) = filter.state {
            clauses.push(format!("state = ?{}", args.len() + 1));
            args.push(Box::new(state.as_str().to_string()));
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY created_at DESC, volume_id ASC;");

        let mut stmt = conn.prepare(&sql)?;
        let refs = args
            .iter()
            .map(|arg| arg.as_ref() as &dyn rusqlite::ToSql)
            .collect::<Vec<_>>();
        let rows = stmt.query_map(refs.as_slice(), |row| self.volume_from_row(row))?;
        Self::collect_rows(rows)
    }

    /// Record a successful mount reported by a worker: pin the node, refresh
    /// the driver's attrs, and clear any previous error.
    ///
    /// This is where `node_local` becomes authoritative — the worker asked the
    /// driver, so its answer beats the manager's mint-time guess.
    #[instrument(err, skip_all)]
    pub fn record_volume_mounted(
        &self,
        volume_id: &str,
        node_id: &str,
        mount_path: &str,
        node_local: bool,
        fs: Option<&str>,
        attrs: &BTreeMap<String, Value>,
    ) -> R<()> {
        // Forwarding wrapper (plan §8, D2): mount confirmations arrive on
        // the worker's heartbeat and — unlike destroys, which are
        // re-dispatched every heartbeat — are reported once, so a rejected
        // write silently lost the node pin and driver attrs.
        match self
            .record_volume_mounted_local(volume_id, node_id, mount_path, node_local, fs, attrs)
        {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::RecordVolumeMounted {
                        volume_id: volume_id.to_owned(),
                        node_id: node_id.to_owned(),
                        mount_path: mount_path.to_owned(),
                        node_local,
                        fs: fs.map(str::to_owned),
                        attrs: attrs.clone(),
                    },
                    err,
                ),
            other => other,
        }
    }

    pub(crate) fn record_volume_mounted_local(
        &self,
        volume_id: &str,
        node_id: &str,
        mount_path: &str,
        node_local: bool,
        fs: Option<&str>,
        attrs: &BTreeMap<String, Value>,
    ) -> R<()> {
        let volume_id = volume_id.to_owned();
        let node_id = node_id.to_owned();
        let mount_path = mount_path.to_owned();
        let fs = fs.map(str::to_owned);
        let attrs = attrs.clone();
        self.replicated(Lane::Scheduler, "record_volume_mounted", move |s| {
            s.record_volume_mounted_apply(
                &volume_id,
                &node_id,
                &mount_path,
                node_local,
                fs.as_deref(),
                &attrs,
            )
        })
    }

    fn record_volume_mounted_apply(
        &self,
        volume_id: &str,
        node_id: &str,
        mount_path: &str,
        node_local: bool,
        fs: Option<&str>,
        attrs: &BTreeMap<String, Value>,
    ) -> R<()> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        let attrs_json = serde_json::to_string(attrs)?;
        // A successful mount is proof the storage exists and is in use, so it
        // is allowed to correct a stale `destroyed` tombstone — a task volume
        // claim re-declared after a job destroy lands on the same deterministic
        // id, and leaving it tombstoned would make a mounted volume invisible
        // in `legion volume list` and immune to the reaper.
        //
        // `destroying` is the one state a mount report must NOT overwrite: a
        // reclaim is deliberately in flight, and resurrecting the row would
        // race the driver's destroy.
        conn.execute(
            "UPDATE volume SET state = 'created', node_id = ?1, mount_path = ?2, \
             node_local = ?3, attrs = ?4, fs = COALESCE(?5, fs), last_error = NULL, \
             updated_at = ?6 WHERE volume_id = ?7 AND state != 'destroying';",
            params![
                node_id,
                mount_path,
                node_local as i64,
                attrs_json,
                fs,
                now,
                volume_id
            ],
        )?;
        Ok(())
    }

    /// Record a driver failure against a volume so it explains itself in
    /// `legion volume list` instead of only in a worker log.
    #[instrument(err, skip_all)]
    pub fn record_volume_error(&self, volume_id: &str, error: &str) -> R<()> {
        match self.record_volume_error_local(volume_id, error) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::RecordVolumeError {
                        volume_id: volume_id.to_owned(),
                        error: error.to_owned(),
                    },
                    err,
                ),
            other => other,
        }
    }

    pub(crate) fn record_volume_error_local(&self, volume_id: &str, error: &str) -> R<()> {
        let volume_id = volume_id.to_owned();
        let error = error.to_owned();
        self.replicated(Lane::Scheduler, "record_volume_error", move |s| {
            s.record_volume_error_apply(&volume_id, &error)
        })
    }

    fn record_volume_error_apply(&self, volume_id: &str, error: &str) -> R<()> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        conn.execute(
            "UPDATE volume SET last_error = ?1, updated_at = ?2 WHERE volume_id = ?3;",
            params![error, now, volume_id],
        )?;
        Ok(())
    }

    /// Mark every live volume no instance references as `destroying`.
    ///
    /// One rule covers every way a handle can be dropped — keyed destroy, slot
    /// scale-in with `retainOnScaleIn: false`, `retainOnStop: false` on job
    /// stop, and a job destroy that cascades the instance rows away. Reclaim
    /// therefore cannot be forgotten by a code path that drops a handle
    /// without knowing about volumes.
    ///
    /// Returns the volumes newly marked, for event emission.
    #[instrument(err, skip_all)]
    pub fn reap_orphaned_volumes(&self) -> R<Vec<Volume>> {
        self.replicated(Lane::Scheduler, "reap_orphaned_volumes", |s| {
            s.reap_orphaned_volumes_apply()
        })
    }

    fn reap_orphaned_volumes_apply(&self) -> R<Vec<Volume>> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let now = Self::now_rfc3339()?;
        let orphans: Vec<Volume> = {
            let mut stmt = tx.prepare(&format!(
                // Two orphan rules, one query:
                //  - an INSTANCE volume no instance references any more (keyed
                //    destroy, scale-in without retention, retainOnStop: false,
                //    or a job destroy that cascaded the instance rows away);
                //  - a task CLAIM whose job no longer exists.
                // Reconciling on "nothing references this" means no handle-drop
                // path can forget to free storage.
                "{} WHERE state IN ('pending', 'created') AND (\
                     (instance_id IS NOT NULL AND NOT EXISTS (\
                         SELECT 1 FROM instance \
                         WHERE instance.instance_id = volume.instance_id \
                         AND instance.storage_handle = volume.driver || '/' || volume.volume_id\
                     )) \
                     OR (instance_id IS NULL AND job_id IS NOT NULL AND NOT EXISTS (\
                         SELECT 1 FROM job WHERE job.id = volume.job_id\
                     ))\
                 );",
                *VOLUME_SELECT_ALL
            ))?;
            let rows = stmt.query_map([], |row| self.volume_from_row(row))?;
            Self::collect_rows(rows)?
        };
        for volume in &orphans {
            tx.execute(
                "UPDATE volume SET state = 'destroying', updated_at = ?1 WHERE volume_id = ?2;",
                params![now, volume.volume_id],
            )?;
        }
        tx.commit()?;
        if !orphans.is_empty() {
            info!(
                count = orphans.len(),
                "Marked orphaned volumes for reclamation"
            );
        }
        Ok(orphans)
    }

    /// Volumes this node should run `destroy` for.
    ///
    /// A node-local volume can only be reclaimed by its own node. A volume
    /// with no pin (never mounted, or a network driver) may be reclaimed by
    /// any node that has the plugin — the caller filters on that.
    #[instrument(err, skip_all)]
    pub fn volumes_to_destroy_for_node(&self, node_id: &str) -> R<Vec<Volume>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(&format!(
            "{} WHERE state = 'destroying' \
             AND (node_id = ?1 OR node_id IS NULL OR node_local = 0) \
             ORDER BY updated_at ASC;",
            *VOLUME_SELECT_ALL
        ))?;
        let rows = stmt.query_map([node_id], |row| self.volume_from_row(row))?;
        Self::collect_rows(rows)
    }

    /// A worker confirmed the driver reclaimed the volume.
    #[instrument(err, skip_all)]
    pub fn mark_volume_destroyed(&self, volume_id: &str) -> R<()> {
        match self.mark_volume_destroyed_local(volume_id) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::MarkVolumeDestroyed {
                        volume_id: volume_id.to_owned(),
                    },
                    err,
                ),
            other => other,
        }
    }

    pub(crate) fn mark_volume_destroyed_local(&self, volume_id: &str) -> R<()> {
        let volume_id = volume_id.to_owned();
        self.replicated(Lane::Scheduler, "mark_volume_destroyed", move |s| {
            s.mark_volume_destroyed_apply(&volume_id)
        })
    }

    fn mark_volume_destroyed_apply(&self, volume_id: &str) -> R<()> {
        let conn = self.write_connection()?;
        let now = Self::now_rfc3339()?;
        conn.execute(
            "UPDATE volume SET state = 'destroyed', node_id = NULL, mount_path = NULL, \
             last_error = NULL, updated_at = ?1 WHERE volume_id = ?2;",
            params![now, volume_id],
        )?;
        info!(volume_id, "Volume destroyed");
        Ok(())
    }

    /// Is this volume actively backing a running workload right now?
    ///
    /// Returns a human-readable "who" when it is. Deleting a mounted volume
    /// would detach the handle and then `rm -rf` under a live database, so
    /// `DeleteVolume` refuses while this returns `Some` (unless forced) — the
    /// same reason Kubernetes puts a protection finalizer on in-use PVCs. The
    /// worker independently refuses to run a driver `destroy` for a volume it
    /// currently has mounted, so even a forced delete cannot pull storage out
    /// from under a running task; force only abandons the bookkeeping.
    #[instrument(err, skip_all)]
    pub fn volume_in_use(&self, volume: &Volume) -> R<Option<String>> {
        let conn = self.read_connection()?;
        // Instance volume: in use iff its instance's binding points at a live
        // allocation and the handle still references this volume.
        if let Some(instance_id) = volume.instance_id.as_deref() {
            let alloc: Option<i64> = conn
                .query_row(
                    "SELECT current_alloc_id FROM instance \
                     WHERE instance_id = ?1 AND storage_handle = ?2 \
                     AND current_alloc_id IS NOT NULL;",
                    params![instance_id, volume.handle()],
                    |row| row.get(0),
                )
                .optional()?;
            return Ok(alloc.map(|alloc_id| {
                format!("instance {instance_id} (allocation {alloc_id}) is running against it")
            }));
        }
        // Task claim: in use while any allocation of the declaring task lives.
        if let (Some(job_id), Some(task_name)) =
            (volume.job_id.as_deref(), volume.task_name.as_deref())
        {
            let alloc: Option<i64> = conn
                .query_row(
                    "SELECT allocation.id FROM allocation \
                     INNER JOIN task_config ON task_config.id = allocation.task_config_id \
                     WHERE task_config.job_id = ?1 AND task_config.name = ?2 LIMIT 1;",
                    params![job_id, task_name],
                    |row| row.get(0),
                )
                .optional()?;
            return Ok(alloc.map(|alloc_id| {
                format!("task {task_name} of job {job_id} (allocation {alloc_id}) mounts it")
            }));
        }
        Ok(None)
    }

    /// Operator-initiated reclaim (`legion volume delete`).
    ///
    /// Without `force` the volume is queued for its driver like any other
    /// reclaim. With `force` it is marked `destroyed` immediately, WITHOUT
    /// driver confirmation — the escape hatch for a node-local volume whose
    /// node is never coming back. Forcing leaves whatever bytes exist on the
    /// (dead) node; that is the operator's explicit call, and the reason it is
    /// not the default.
    #[instrument(err, skip_all)]
    pub fn delete_volume(&self, volume_id: &str, force: bool) -> R<Volume> {
        let volume_id = volume_id.to_owned();
        self.replicated(Lane::Scheduler, "delete_volume", move |s| {
            s.delete_volume_apply(&volume_id, force)
        })
    }

    /// Detaching `instance.storage_handle` here is legal in the same capture:
    /// `instance` is scheduler-owned too, so the detach and the volume state
    /// flip stay one atomic op.
    fn delete_volume_apply(&self, volume_id: &str, force: bool) -> R<Volume> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let volume: Volume = tx
            .query_row(VOLUME_SELECT.as_str(), [volume_id], |row| {
                self.volume_from_row(row)
            })
            .optional()?
            .with_context(|| format!("no volume {volume_id}"))?;
        let now = Self::now_rfc3339()?;

        // Detaching the handle first means the instance mints a fresh volume
        // on its next placement instead of pointing at storage being reclaimed.
        if let Some(instance_id) = volume.instance_id.as_deref() {
            tx.execute(
                "UPDATE instance SET storage_handle = NULL, updated_at = ?1 \
                 WHERE instance_id = ?2 AND storage_handle = ?3;",
                params![now, instance_id, volume.handle()],
            )?;
        }
        let state = if force { "destroyed" } else { "destroying" };
        tx.execute(
            "UPDATE volume SET state = ?1, updated_at = ?2, \
             node_id = CASE WHEN ?1 = 'destroyed' THEN NULL ELSE node_id END \
             WHERE volume_id = ?3;",
            params![state, now, volume_id],
        )?;
        // Re-read through the TRANSACTION, not `get_volume`: inside a raft
        // capture the read pool still sees pre-capture state (JC-35), so a
        // post-commit read would return the row as it was before the flip.
        let deleted: Volume = tx
            .query_row(VOLUME_SELECT.as_str(), [volume_id], |row| {
                self.volume_from_row(row)
            })
            .optional()?
            .with_context(|| format!("volume {volume_id} vanished during delete"))?;
        tx.commit()?;

        warn!(volume_id, force, "Operator requested volume deletion");
        Ok(deleted)
    }

    /// Node pins for the instances of `task_id` that are awaiting placement.
    ///
    /// A node-local volume binds its instance to one node, so a replacement
    /// allocation for that instance must land there. Returns `(instance_id,
    /// node_id)` pairs for live instances of this task that have a pinned
    /// volume and no current allocation — exactly the set the next plan must
    /// place. Ordered so slot pools are deterministic.
    #[instrument(err, skip_all)]
    pub fn pinned_placements_for_task(&self, task_id: i64) -> R<Vec<(String, String)>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT instance.instance_id, volume.node_id \
             FROM instance \
             INNER JOIN volume ON volume.instance_id = instance.instance_id \
             INNER JOIN task_config ON task_config.id = ?1 \
             WHERE instance.job_id = task_config.job_id \
             AND instance.group_name = task_config.group_name \
             AND instance.task_name = task_config.name \
             AND instance.state IN ('pending', 'rescheduling') \
             AND instance.current_alloc_id IS NULL \
             AND instance.storage_handle = volume.driver || '/' || volume.volume_id \
             AND volume.node_local = 1 \
             AND volume.node_id IS NOT NULL \
             AND volume.state IN ('pending', 'created') \
             ORDER BY instance.slot_index ASC, instance.instance_id ASC;",
        )?;
        let rows = stmt.query_map([task_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Self::collect_rows(rows)
    }

    /// The node a specific instance's volume is pinned to, if any. Used by the
    /// allocation binder to pair an allocation with the instance whose data
    /// already lives on the node it landed on.
    fn pinned_node_for_instance_tx(&self, tx: &Connection, instance_id: &str) -> R<Option<String>> {
        let node_id = tx
            .query_row(
                "SELECT volume.node_id FROM volume \
                 INNER JOIN instance ON instance.instance_id = volume.instance_id \
                 WHERE volume.instance_id = ?1 AND volume.node_local = 1 \
                 AND volume.node_id IS NOT NULL AND volume.state IN ('pending', 'created') \
                 AND instance.storage_handle = volume.driver || '/' || volume.volume_id \
                 LIMIT 1;",
                [instance_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?;
        Ok(node_id.flatten())
    }

    /// Volume driver capabilities advertised across the cluster, keyed by
    /// driver name, with the node ids that provide each.
    ///
    /// This is the manager's only source of driver metadata (it loads no
    /// drivers itself), so it backs both admission validation and
    /// `legion volume drivers`.
    #[instrument(err, skip_all)]
    pub fn cluster_volume_drivers(&self) -> R<BTreeMap<String, ClusterVolumeDriver>> {
        let conn = self.read_connection()?;
        // Liveness lives in `node_health` since the phase C-1 node split
        // (plan §7-C2): `node` keeps identity/attrs, so `status` has to be
        // joined in — and a node registered but not yet heartbeated has no
        // health row at all, which reads as `active` like everywhere else.
        let mut stmt = conn.prepare(
            "SELECT node.id, COALESCE(node_health.status, 'active'), node.fingerprint_attrs \
             FROM node LEFT JOIN node_health ON node_health.node_id = node.id;",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Value>(2)?,
            ))
        })?;

        let mut out: BTreeMap<String, ClusterVolumeDriver> = BTreeMap::new();
        for row in rows {
            let (node_id, status, attrs) = row?;
            let Some(drivers) = attrs.get("volumeDrivers").and_then(Value::as_object) else {
                continue;
            };
            for (name, raw) in drivers {
                let Ok(caps) =
                    serde_json::from_value::<crate::volumes::VolumeCapabilities>(raw.clone())
                else {
                    continue;
                };
                let entry = out
                    .entry(name.clone())
                    .or_insert_with(|| ClusterVolumeDriver {
                        capabilities: caps.clone(),
                        nodes: Vec::new(),
                        available_nodes: Vec::new(),
                    });
                entry.nodes.push(node_id.clone());
                // "Available" means a live node could actually mount it, which
                // is the question admission and placement care about.
                if caps.available && status != "lost" {
                    entry.available_nodes.push(node_id.clone());
                    entry.capabilities = caps;
                }
            }
        }
        for entry in out.values_mut() {
            entry.nodes.sort();
            entry.nodes.dedup();
            entry.available_nodes.sort();
            entry.available_nodes.dedup();
        }
        Ok(out)
    }
}

/// Filters for [`Store::list_volumes`].
#[derive(Debug, Clone, Default)]
pub struct VolumeFilter {
    pub job_id: Option<String>,
    pub node_id: Option<String>,
    pub driver: Option<String>,
    pub instance_id: Option<String>,
    pub state: Option<crate::structs::VolumeState>,
}

/// A volume driver as the cluster sees it: one capability record plus the
/// nodes providing it.
#[derive(Debug, Clone)]
pub struct ClusterVolumeDriver {
    pub capabilities: crate::volumes::VolumeCapabilities,
    /// Every node that has the driver installed, including ones where it is
    /// unusable.
    pub nodes: Vec<String>,
    /// Nodes where the driver is installed AND usable — the placement set.
    pub available_nodes: Vec<String>,
}

const VOLUME_COLUMNS: &str = "volume_id, name, driver, job_id, instance_id, task_name, state, \
     node_id, node_local, read_only, params, attrs, mount_path, last_error, created_at, \
     updated_at, volume_mode, fs";

lazy_static! {
    static ref VOLUME_SELECT: String =
        format!("SELECT {VOLUME_COLUMNS} FROM volume WHERE volume_id = ?;");
    static ref VOLUME_SELECT_ALL: String = format!("SELECT {VOLUME_COLUMNS} FROM volume");
}

/// Deterministic id for a task volume claim.
///
/// Claims are shared per `(job, group, task, name)`, so the id must be a pure
/// function of those — a resubmit has to land on the same volume, not orphan
/// the old one. The readable prefix keeps `legion volume list` legible; the
/// hash suffix keeps ids unique after the prefix is sanitized and truncated.
fn claim_volume_id(job_id: &str, group: &str, task: &str, name: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(format!("{job_id}\u{1}{group}\u{1}{task}\u{1}{name}"));
    let digest = format!("{:x}", hasher.finalize());
    let label = format!("{job_id}-{task}-{name}")
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .take(48)
        .collect::<String>();
    format!("claim-{label}-{}", &digest[..12])
}

/// Parse a JSONB object column into a sorted map, tolerating NULL and
/// non-object values (both become empty).
fn parse_json_map_column(
    idx: usize,
    raw: Option<String>,
) -> Result<BTreeMap<String, Value>, rusqlite::Error> {
    let Some(raw) = raw else {
        return Ok(BTreeMap::new());
    };
    let value: Value = serde_json::from_str(&raw).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(idx, rusqlite::types::Type::Text, Box::new(err))
    })?;
    Ok(value
        .as_object()
        .map(|map| {
            map.iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        })
        .unwrap_or_default())
}

/// Mint an opaque storage handle for an instance: `{driver}/{volume_id}`.
///
/// The format predates volume drivers (instances already minted
/// `local-dir/<rand>`), which is exactly why it was kept — every instance row
/// written before this feature still parses. See `docs/volumes.md`.
fn mint_storage_handle(driver: &str) -> String {
    format!("{driver}/{}", random_string(32))
}

/// Whether two versions of a task can share the same allocations, i.e. whether
/// re-submitting is a metadata migration (`migrate_allocations_to_job_config`)
/// or needs fresh placements.
///
/// `connect` is part of the comparison because it is *placement* input, not
/// just runtime config: upstream loopback ports are assigned out of the node's
/// port space in `insert_allocation` and frozen onto `AllocNetwork.upstreams`,
/// and the task reads them from `LEGION_UPSTREAM_*` at exec time. Migrating an
/// allocation in place rewrites only its job/task config ids, so adding (or
/// changing) upstreams on a running job would leave the allocation with no
/// loopback ports and the task with no env — silently, and permanently.
fn task_schedule_equivalent(left: &TaskConfig, right: &TaskConfig) -> bool {
    left.count == right.count
        && left.schedule == right.schedule
        && left.resources == right.resources
        && left.constraints == right.constraints
        && left.permit == right.permit
        && left.affinities == right.affinities
        && left.connect == right.connect
}

// =====
// =====
// TESTS
// =====
// =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::scheduler::{PlanMode, Scheduler};
    use rusqlite::params;
    use tempfile::Builder;

    /// Adversarial regression (durability past the purge horizon): in raft
    /// mode the state DB must run `synchronous = FULL` — raft purges its log
    /// behind snapshots, so a power loss regressing the applied marker below
    /// `last_purged_log_id` (possible under NORMAL until a WAL checkpoint)
    /// would brick the node at startup reapply. local_disk keeps NORMAL.
    #[test]
    fn durable_sync_runs_write_connection_at_synchronous_full() -> R<()> {
        let dir = tempfile::tempdir()?;

        let normal = Connection::open(dir.path().join("normal.sqlite"))?;
        Store::configure_connection(&normal, false, false)?;
        let level: i64 = normal.query_row("PRAGMA synchronous;", [], |r| r.get(0))?;
        assert_eq!(level, 1, "local_disk keeps synchronous=NORMAL");

        let full = Connection::open(dir.path().join("full.sqlite"))?;
        Store::configure_connection(&full, false, true)?;
        let level: i64 = full.query_row("PRAGMA synchronous;", [], |r| r.get(0))?;
        assert_eq!(
            level, 2,
            "raft-mode write connections must run synchronous=FULL so the applied marker \
             never regresses past the purged log horizon on power loss"
        );
        Ok(())
    }

    /// Drain and handle every currently claimable eval. The C-2 composite
    /// splits enqueue their scheduler-lane tails (e.g.
    /// `node_lost_reschedule`) as local evals; store-level tests pump the
    /// queue here, standing in for the daemon's eval worker.
    fn run_pending_evals(store: &Store) -> R<()> {
        while let Some(eval) = store.claim_eval()? {
            eval.handle(store)?;
            store.complete_eval(&eval.id, true)?;
        }
        Ok(())
    }

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("khepri-store-")
            .tempdir()
            .context("Error creating temp store directory")?
            .keep();

        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("Error parsing store path")?
                .to_string(),
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    #[test]
    fn idempotency_record_and_lookup_roundtrip() -> R<()> {
        let store = setup_store()?;

        // Unknown key: no recorded response.
        assert_eq!(store.idempotency_lookup("k1")?, None);

        // First record wins and is replayable.
        assert!(store.idempotency_record("k1", "run_job", "Job submit success")?);
        assert_eq!(
            store.idempotency_lookup("k1")?.as_deref(),
            Some("Job submit success")
        );

        // A second record for the same key is a no-op and keeps the original.
        assert!(!store.idempotency_record("k1", "run_job", "different")?);
        assert_eq!(
            store.idempotency_lookup("k1")?.as_deref(),
            Some("Job submit success")
        );

        // Empty key disables deduplication entirely.
        assert_eq!(store.idempotency_lookup("")?, None);
        assert!(store.idempotency_record("", "run_job", "ignored")?);
        Ok(())
    }

    fn make_node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            config_attrs: json!({}),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    fn make_node_msg(id: &str, status: &str) -> NodeMsg {
        NodeMsg {
            id: id.to_string(),
            pool: "default".into(),
            status: status.to_string(),
            availability: "available".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: "{}".into(),
            dynamic_attrs: "{}".into(),
            health_checks: "{}".into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
        }
    }

    fn task(name: &str) -> TaskConfig {
        TaskConfig::new(name, "noop")
    }

    fn task_id_for(store: &Store, task_name: &str, job_id: &str) -> R<i64> {
        let conn = store.read_connection()?;
        let id = conn.query_row(
            "SELECT id FROM task_config WHERE name = ? AND job_id = ?;",
            [task_name, job_id],
            |row| row.get(0),
        )?;

        Ok(id)
    }

    fn insert_eval(
        store: &Store,
        id: i64,
        status: &str,
        kind: &str,
        job_id: Option<&str>,
        job_config_id: Option<&str>,
        node_id: Option<&str>,
        priority: i64,
    ) -> R<()> {
        let conn = store.write_connection()?;
        conn.execute(
            "INSERT INTO eval (id, status, type, job_id, job_config_id, node_id, priority) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
            params![id, status, kind, job_id, job_config_id, node_id, priority],
        )?;

        Ok(())
    }

    // fn teardown_store(store: &Store) -> R<()> {
    //     let migrations = Store::get_migrations();
    //     let mut conn = store.conn.lock().map_err(|_| anyhow!(CONN_LOCK_ERROR))?;
    //     migrations.to_version(&mut conn, 0)?;

    //     Ok(())
    // }

    #[test]
    fn manager_config_round_trips_and_overwrites() -> R<()> {
        let store = setup_store()?;

        let config = ManagerConfigFull {
            server: crate::structs::ServerConfigFull {
                host: "127.0.0.1".into(),
                port: 1234,
                insecure_port: 4321,
                ..Default::default()
            },
            store_path: "tests/store".into(),
            worker_config_path: "configs/workers".into(),
            worker_check_seconds: 5,
            ..ManagerConfigFull::default()
        };

        store.upsert_manager_config("manager-a", &config)?;

        let configs = store.get_manager_configs()?;
        assert_eq!(configs.len(), 1);

        let parsed: serde_json::Value = serde_json::from_str(&configs[0].config_json)?;
        assert_eq!(parsed["server"]["port"], 1234);

        let mut updated = config.clone();
        updated.worker_check_seconds = 99;
        updated.server.port = 9876;

        store.upsert_manager_config("manager-a", &updated)?;

        let configs = store.get_manager_configs()?;
        assert_eq!(configs.len(), 1);
        let parsed_updated: serde_json::Value = serde_json::from_str(&configs[0].config_json)?;
        assert_eq!(parsed_updated["server"]["port"], 9876);
        assert_eq!(parsed_updated["workerCheckSeconds"], 99);

        Ok(())
    }

    #[test]
    fn read_model_tracks_nodes_and_updates() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "active"))?;
        let model = store.scheduling_read_model()?;
        let node = model.nodes.get("node-one").context("missing cached node")?;
        assert_eq!(node.status, "active");

        let mut diff = Map::new();
        diff.insert("region".to_string(), json!("us-west"));
        store.update_node_dyamic("node-one", &diff)?;
        let model = store.scheduling_read_model()?;
        let node = model.nodes.get("node-one").context("missing cached node")?;
        assert_eq!(node.dynamic_attrs["region"], "us-west");

        store.mark_node_lost("node-one")?;
        let model = store.scheduling_read_model()?;
        let node = model.nodes.get("node-one").context("missing cached node")?;
        assert_eq!(node.status, "lost");
        assert_eq!(node.availability, "unavailable");

        Ok(())
    }

    #[test]
    fn available_node_cache_tracks_node_mutations() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("cache-a", "active"))?;
        store.insert_node(make_node_msg("cache-b", "active"))?;

        let first = store.available_node_arcs()?;
        assert_eq!(
            first.iter().map(|n| n.id.as_str()).collect::<Vec<_>>(),
            vec!["cache-a", "cache-b"],
            "sorted by id"
        );
        // Second read is served from the cache and must be identical.
        let second = store.available_node_arcs()?;
        assert_eq!(second.len(), 2);
        assert!(
            Arc::ptr_eq(&first[0], &second[0]),
            "cache serves the same Arcs"
        );

        // A lost node must drop out on the next read.
        store.mark_nodes_as_lost(&["cache-a".to_string()])?;
        let after_lost = store.available_node_arcs()?;
        assert_eq!(
            after_lost.iter().map(|n| n.id.as_str()).collect::<Vec<_>>(),
            vec!["cache-b"]
        );

        // A removed node must drop out too.
        store.remove_node("cache-b")?;
        assert!(store.available_node_arcs()?.is_empty());

        Ok(())
    }

    #[test]
    fn batched_heartbeats_stamp_every_node_once() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("hb-a", "active"))?;
        store.insert_node(make_node_msg("hb-b", "active"))?;
        store.insert_node(make_node_msg("hb-c", "active"))?;

        // Duplicates dedupe; an unregistered id stays a no-op that errors
        // NotFound for exactly that caller, as the single op always did (a
        // ghost worker must not believe it is healthy).
        let results = store.run_heartbeat_batch(vec![
            "hb-a".to_string(),
            "hb-b".to_string(),
            "hb-b".to_string(),
            "hb-c".to_string(),
            "hb-ghost".to_string(),
        ]);
        assert_eq!(results.len(), 5);
        for result in &results[..4] {
            assert!(result.is_ok(), "batched heartbeat failed: {result:?}");
        }
        assert!(
            matches!(
                results[4]
                    .as_ref()
                    .err()
                    .and_then(|err| err.downcast_ref::<StorageError>()),
                Some(StorageError::NotFound { .. })
            ),
            "ghost heartbeat must keep its typed NotFound: {:?}",
            results[4]
        );

        let conn = store.read_connection()?;
        let stamped: i64 = conn.query_row(
            "SELECT COUNT(*) FROM node_health
             WHERE node_id IN ('hb-a','hb-b','hb-c')
               AND last_heartbeat != '' AND last_heartbeat_unix IS NOT NULL;",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(stamped, 3, "every real node gets both timestamp columns");
        let ghosts: i64 = conn.query_row(
            "SELECT COUNT(*) FROM node_health WHERE node_id = 'hb-ghost';",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(ghosts, 0, "unregistered ids must stay a no-op");
        Ok(())
    }

    #[test]
    fn heartbeat_transition_reprojects_and_bare_refresh_skips() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("hb-t", "active"))?;
        store.update_node_heartbeat("hb-t".to_string())?;

        // Bare refresh of an active node: the cached Node must not be
        // rebuilt (the skip this heartbeat path exists for).
        let before = store.scheduling_read_model()?;
        let cached_before = before.nodes.get("hb-t").cloned().context("cached")?;
        store.update_node_heartbeat("hb-t".to_string())?;
        let after = store.scheduling_read_model()?;
        let cached_after = after.nodes.get("hb-t").cloned().context("cached")?;
        assert!(
            Arc::ptr_eq(&cached_before, &cached_after),
            "an active node's bare heartbeat refresh must not reproject the cached node"
        );

        // Lost -> active transition: the heartbeat must reproject the
        // cached node (its status flips), unlike the bare refresh above.
        // (Availability stays with `mark_node_ready` — heartbeats only own
        // liveness status.)
        store.mark_nodes_as_lost(&["hb-t".to_string()])?;
        let lost_cached = store
            .scheduling_read_model()?
            .nodes
            .get("hb-t")
            .cloned()
            .context("cached")?;
        assert_eq!(lost_cached.status, "lost");
        store.update_node_heartbeat("hb-t".to_string())?;
        let recovered_cached = store
            .scheduling_read_model()?
            .nodes
            .get("hb-t")
            .cloned()
            .context("cached")?;
        assert_eq!(
            recovered_cached.status, "active",
            "a lost node's heartbeat must reproject the cached node"
        );
        assert_eq!(store.node_lost_at("hb-t")?, None);
        Ok(())
    }

    #[test]
    fn concurrent_heartbeats_coalesce_and_all_stamp() -> R<()> {
        let store = setup_store()?;
        let mut ids = Vec::new();
        for index in 0..16 {
            let id = format!("hb-conc-{index}");
            store.insert_node(make_node_msg(&id, "active"))?;
            ids.push(id);
        }
        let handles: Vec<_> = ids
            .iter()
            .cloned()
            .map(|id| {
                let store = store.clone();
                std::thread::spawn(move || store.update_node_heartbeat(id))
            })
            .collect();
        for handle in handles {
            handle.join().expect("heartbeat thread panicked")?;
        }
        let conn = store.read_connection()?;
        let stamped: i64 = conn.query_row(
            "SELECT COUNT(*) FROM node_health
             WHERE node_id LIKE 'hb-conc-%' AND last_heartbeat_unix IS NOT NULL;",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(stamped, 16);
        Ok(())
    }

    /// The cached-pool invariant behind `ResourceLedger::from_snapshot`
    /// (scheduler-plan-snapshot plan): after ANY sequence of read-model
    /// mutations, every available node's cached pool equals a from-scratch
    /// `ResourcePool::free_for_node` computation, and only available nodes
    /// have pools.
    fn assert_pools_match_scratch(store: &Store) -> R<()> {
        let model = store.scheduling_read_model()?;
        for (node_id, node) in &model.nodes {
            let cached = model.node_pools.get(node_id);
            if node.availability != "available" {
                assert!(
                    cached.is_none(),
                    "{node_id}: unavailable nodes have no pool"
                );
                continue;
            }
            let cached = cached.with_context(|| format!("{node_id}: missing cached pool"))?;
            let allocations = model
                .allocations_by_node
                .get(node_id)
                .map(|allocations| allocations.as_slice())
                .unwrap_or(&[]);
            let scratch = crate::resources::ResourcePool::free_for_node(
                node,
                allocations,
                &store.resource_policy,
            );
            let amounts = |pool: &crate::resources::ResourcePool| -> Vec<(String, String)> {
                let mut out: Vec<(String, String)> = pool
                    .values
                    .keys()
                    .map(|name| (name.clone(), format!("{:?}", pool.amount(name))))
                    .collect();
                out.sort();
                out
            };
            assert_eq!(
                amounts(cached),
                amounts(&scratch),
                "{node_id}: cached pool drifted from scratch recompute"
            );
        }
        for node_id in model.node_pools.keys() {
            assert!(
                model.nodes.contains_key(node_id),
                "{node_id}: pool for a node that no longer exists"
            );
        }
        Ok(())
    }

    #[test]
    fn cached_node_pools_track_every_mutation() -> R<()> {
        let store = setup_store()?;

        // Join two nodes, one with fingerprints + custom resources.
        let mut fingerprinted = make_node_msg("node-pool-a", "active");
        fingerprinted.fingerprint_attrs = json!({
            "memory_total": "1073741824",
            "cpus": { "cpu-1": { "frequency": "1000" } },
        })
        .to_string();
        fingerprinted.config_attrs = json!({ "resources": { "vram": 4096 } }).to_string();
        store.insert_node(fingerprinted)?;
        store.insert_node(make_node_msg("node-pool-b", "active"))?;
        assert_pools_match_scratch(&store)?;

        // Place allocations through the real plan/apply path.
        let job = JobConfig {
            priority: Default::default(),
            name: "pool-prop".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                resources: json!({ "memory": 64, "cpu": 100 }),
                ..TaskConfig::new("worker", "noop")
            }],
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
        };
        let submission = Submission {
            path: "pool-prop.jsonnet".into(),
            content: "{}".into(),
            is_primary: true,
        };
        store.handle_job_submit(&job, &[submission])?;
        let job_config_id = store.get_job_config("pool-prop")?.id();
        let scheduler = crate::manager::scheduler::Scheduler::new(&store)?;
        let outcome =
            scheduler.plan_and_apply(&job_config_id, crate::manager::scheduler::PlanMode::Fresh)?;
        assert!(!outcome.allocations.is_empty());
        assert_pools_match_scratch(&store)?;

        // Attribute update, drain, uncordon, allocation removal, node loss,
        // and removal all reproject.
        let dynamic: Map<String, Value> = [("zone".to_string(), json!("a"))].into_iter().collect();
        store.update_node_dyamic("node-pool-a", &dynamic)?;
        assert_pools_match_scratch(&store)?;
        store.set_node_availability("node-pool-b", "drained")?;
        assert_pools_match_scratch(&store)?;
        store.set_node_availability("node-pool-b", "available")?;
        assert_pools_match_scratch(&store)?;
        store.remove_allocs_for_job(&store.get_job("pool-prop")?.id)?;
        assert_pools_match_scratch(&store)?;
        store.mark_node_lost("node-pool-a")?;
        assert_pools_match_scratch(&store)?;
        store.remove_node("node-pool-b")?;
        assert_pools_match_scratch(&store)?;

        Ok(())
    }

    /// Phase C-1 (plan §7-C2): the node/node_health split's row lifecycle.
    /// Registration seeds both halves (control seeds `node`, status seeds
    /// `node_health`); a missing liveness row hydrates the registration
    /// defaults; a heartbeat (re)creates the row via upsert; node removal
    /// (control-lane) orphans the liveness row, which the status-lane
    /// sweep collects.
    #[test]
    fn node_health_row_lifecycle_across_the_split() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "active"))?;
        let (status, heartbeat): (String, String) = {
            let conn = store.read_connection()?;
            conn.query_row(
                "SELECT status, last_heartbeat FROM node_health WHERE node_id = 'node-one';",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?
        };
        assert_eq!(status, "active");
        assert_eq!(heartbeat, "2023-01-01 00:00:00 +00:00:00");

        // A node row WITHOUT a node_health row (a crash between
        // registration's control and status ops) hydrates the registration
        // defaults instead of failing.
        {
            let conn = store.write_connection()?;
            conn.execute("DELETE FROM node_health WHERE node_id = 'node-one';", [])?;
        }
        let node = store.get_node("node-one")?;
        assert_eq!(node.status, "active");
        assert_eq!(node.last_heartbeat, "");
        assert_eq!(node.health_checks, json!({}));
        assert_eq!(store.node_lost_at("node-one")?, None);
        // The defaults keep the node visible to the lost-checker feed...
        assert_eq!(
            store.get_node_ids_and_heartbeats()?,
            vec![("node-one".to_string(), String::new(), None)]
        );

        // ...and the first heartbeat (an upsert) creates the row again.
        store.update_node_heartbeat("node-one".to_string())?;
        let recreated: i64 = {
            let conn = store.read_connection()?;
            conn.query_row(
                "SELECT COUNT(*) FROM node_health WHERE node_id = 'node-one' AND status = 'active' AND last_heartbeat != '';",
                [],
                |row| row.get(0),
            )?
        };
        assert_eq!(recreated, 1, "heartbeat must upsert the liveness row");

        // Liveness writes for unregistered nodes stay no-ops (no row is
        // invented for a node that never registered).
        assert!(store.update_node_heartbeat("ghost".to_string()).is_err());
        let ghosts: i64 = {
            let conn = store.read_connection()?;
            conn.query_row(
                "SELECT COUNT(*) FROM node_health WHERE node_id = 'ghost';",
                [],
                |row| row.get(0),
            )?
        };
        assert_eq!(ghosts, 0);

        // Node removal is control-lane only: the node row goes, the
        // liveness row is orphaned — invisible to every hydration path...
        store.remove_node("node-one")?;
        assert!(store.get_node("node-one").is_err());
        assert!(store.get_node_ids_and_heartbeats()?.is_empty());
        assert!(store.lost_node_ids()?.is_empty());
        let orphans: i64 = {
            let conn = store.read_connection()?;
            conn.query_row("SELECT COUNT(*) FROM node_health;", [], |row| row.get(0))?
        };
        assert_eq!(orphans, 1, "remove_node must not delete status-lane rows");

        // ...and the status-lane sweep collects it without touching the
        // liveness row of a live node. The FIRST sweep only OBSERVES the
        // orphan (two-sweep grace, adversarial finding F2: a freshly
        // failed-over status leader's stale local view must not replicate
        // deletions of live rows); the second confirms and deletes it.
        store.insert_node(make_node_msg("node-two", "active"))?;
        assert_eq!(
            store.prune_node_health_orphans()?,
            0,
            "the first sweep must only observe, never delete"
        );
        assert_eq!(store.prune_node_health_orphans()?, 1);
        let remaining: Vec<String> = {
            let conn = store.read_connection()?;
            let mut stmt = conn.prepare("SELECT node_id FROM node_health ORDER BY node_id;")?;
            let rows = Store::collect_rows(stmt.query_map([], |row| row.get(0))?)?;
            drop(stmt);
            rows
        };
        assert_eq!(remaining, vec!["node-two".to_string()]);
        assert_eq!(store.prune_node_health_orphans()?, 0, "sweep is idempotent");
        Ok(())
    }

    #[test]
    fn task_identities_round_trip_through_persistence() -> R<()> {
        use crate::structs::workload_identity::{WorkloadIdentity, WorkloadIdentityFile};

        let store = setup_store()?;

        let identity = WorkloadIdentity {
            name: "aws".into(),
            audience: vec!["sts.amazonaws.com".into()],
            ttl: Some("15m".into()),
            file: Some(WorkloadIdentityFile {
                path: "/var/run/legion/identity/aws/token".into(),
                mode: Some("0400".into()),
            }),
            ..Default::default()
        };
        let task = TaskConfig {
            identities: vec![identity.clone()],
            ..task("worker")
        };
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "identity-persist".into(),
            tasks: vec![task],
            version: None,
        };

        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;

        let loaded = store.get_job_config(&job.name)?;
        assert_eq!(loaded.tasks.len(), 1);
        assert_eq!(loaded.tasks[0].identities, vec![identity]);
        Ok(())
    }

    #[test]
    fn tasks_with_actions_round_trip() -> R<()> {
        use crate::structs::{ActionParam, ActionParamType, ActionSpec};
        let store = setup_store()?;
        let mut t = task("worker");
        t.actions = vec![ActionSpec {
            name: "flush-cache".into(),
            description: "Flush it".into(),
            command: vec!["/bin/echo".into(), "${param.scope}".into()],
            params: vec![ActionParam {
                name: "scope".into(),
                description: String::new(),
                param_type: ActionParamType::Enum,
                default: Some("all".into()),
                required: None,
                values: vec!["all".into(), "sessions".into()],
                pattern: None,
                min: None,
                max: None,
                sensitive: false,
            }],
            tty: false,
            timeout: None,
        }];
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "actions-persist".into(),
            tasks: vec![t.clone()],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        // Read back through both parsers: get_job_config and the read-model.
        let loaded = store.get_job_config(&job.name)?;
        assert_eq!(loaded.tasks[0].actions, t.actions);
        Ok(())
    }

    #[test]
    fn tasks_without_identities_round_trip_empty() -> R<()> {
        let store = setup_store()?;
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "no-identity-persist".into(),
            tasks: vec![task("worker")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let loaded = store.get_job_config(&job.name)?;
        assert!(loaded.tasks[0].identities.is_empty());
        Ok(())
    }

    #[test]
    fn read_model_tracks_allocations_add_remove() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "ready"))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "cache-allocs".into(),
            tasks: vec![task("worker")],
            version: None,
        };

        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, "worker", &job.name)?;
        let alloc = store.place_alloc("node-one", task_id)?;

        let model = store.scheduling_read_model()?;
        let node_allocs = model
            .allocations_by_node
            .get("node-one")
            .cloned()
            .unwrap_or_default();
        assert_eq!(node_allocs.len(), 1);
        assert_eq!(node_allocs[0].id, alloc.id);
        assert_eq!(node_allocs[0].job_config_id, job_config_id);

        store.remove_allocs_for_node("node-one")?;
        let model = store.scheduling_read_model()?;
        let node_allocs = model
            .allocations_by_node
            .get("node-one")
            .cloned()
            .unwrap_or_default();
        assert!(node_allocs.is_empty());

        Ok(())
    }

    #[test]
    fn read_model_tracks_task_config_changes() -> R<()> {
        let store = setup_store()?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "cache-tasks".into(),
            tasks: vec![task("alpha")],
            version: None,
        };

        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;

        let job_config_id = store.get_job_config(&job.name)?.id();
        let model = store.scheduling_read_model()?;
        let tasks = model
            .task_configs
            .get(&job_config_id)
            .cloned()
            .unwrap_or_default();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].1.name, "alpha");

        let updated_job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job.name.clone(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                ..task("beta")
            }],
            version: None,
        };

        store.handle_job_submit(
            &updated_job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;

        let new_job_config_id = store.get_job_config(&job.name)?.id();
        let model = store.scheduling_read_model()?;
        let tasks = model
            .task_configs
            .get(&new_job_config_id)
            .cloned()
            .unwrap_or_default();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].1.name, "beta");
        assert_eq!(tasks[0].1.count.num, 2);

        Ok(())
    }

    #[test]
    fn adding_updating_and_getting_worker_types() -> R<()> {
        let store = setup_store()?;

        let mut attrs = HashMap::new();
        attrs.insert("test".to_string(), "val".to_string());

        let worker_config = WorkerConfig {
            host_volumes: String::new(),
            resource_enforcement: String::new(),
            name: "test".to_string(),
            pool: "default".to_string(),
            additional_node_checks: vec![],
            attributes: attrs,
            name_prefix: None,
            resources: "{}".into(),
            reserved_resources: "{}".into(),
            max_allocations: None,
            artifacts: String::new(),
            heartbeat_seconds: Some(5),
        };

        let returned_config = store.insert_worker_type(&worker_config)?;
        assert_eq!(returned_config.name, worker_config.name);

        let returned_config = store.get_worker_type("test".to_string())?;
        assert_eq!(returned_config.name, worker_config.name);
        assert_eq!(returned_config.pool, worker_config.pool);
        assert_eq!(returned_config.heartbeat_seconds, Some(5));
        let val = returned_config
            .attributes
            .get("test")
            .context("no test attr")?;

        assert_eq!(val, "val");

        Ok(())
    }

    #[test]
    fn worker_type_with_empty_checks_round_trips_as_empty_vec() -> R<()> {
        let store = setup_store()?;

        let worker_config = WorkerConfig {
            host_volumes: String::new(),
            resource_enforcement: String::new(),
            name: "empty".to_string(),
            pool: "default".to_string(),
            additional_node_checks: vec![],
            attributes: HashMap::new(),
            name_prefix: None,
            resources: "{}".into(),
            reserved_resources: "{}".into(),
            max_allocations: None,
            artifacts: String::new(),
            heartbeat_seconds: None,
        };

        store.insert_worker_type(&worker_config)?;

        let returned_config = store.get_worker_type(worker_config.name.clone())?;
        assert!(returned_config.additional_node_checks.is_empty());
        assert_eq!(returned_config.heartbeat_seconds, None);

        Ok(())
    }

    #[test]
    fn mark_nodes_as_lost_updates_status_and_availability() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        store.mark_nodes_as_lost(&["node-one".to_string()])?;
        run_pending_evals(&store)?;

        let node_one = store.get_node("node-one")?;
        assert_eq!(node_one.status, "lost");
        assert_eq!(node_one.availability, "unavailable");

        let node_two = store.get_node("node-two")?;
        assert_eq!(node_two.status, "active");
        assert_eq!(node_two.availability, "available");

        Ok(())
    }

    #[test]
    fn mark_nodes_as_lost_is_noop_for_empty_input() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "active"))?;

        store.mark_nodes_as_lost(&[])?;

        let node_one = store.get_node("node-one")?;
        assert_eq!(node_one.status, "active");
        assert_eq!(node_one.availability, "available");

        Ok(())
    }

    #[test]
    fn mark_nodes_as_lost_reschedules_allocations() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "lost-reschedule".into(),
            tasks: vec![task("worker")],
            version: None,
        };

        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;

        let task_id = task_id_for(&store, "worker", &job.name)?;
        store.place_alloc("node-one", task_id)?;

        store.mark_nodes_as_lost(&["node-one".to_string()])?;
        run_pending_evals(&store)?;

        let node_one_allocs = store.allocs_for_node("node-one")?;
        let node_two_allocs = store.allocs_for_node("node-two")?;

        assert!(node_one_allocs.is_empty());
        assert_eq!(node_two_allocs.len(), 1);

        Ok(())
    }

    /// Slot-mode stateful task with an explicit onLost policy (None values =
    /// the defaults).
    fn fenced_slot_task(
        name: &str,
        self_terminate_after: Option<&str>,
        replace_after: Option<&str>,
    ) -> TaskConfig {
        let mut task = stateful_slot_task(name, 1, true);
        if self_terminate_after.is_some() || replace_after.is_some() {
            if let Some(stateful) = task.stateful.as_mut() {
                stateful.on_lost = Some(StatefulOnLostConfig {
                    self_terminate_after: self_terminate_after.map(String::from),
                    replace_after: replace_after.map(String::from),
                });
            }
        }
        task
    }

    fn backdated_rfc3339(minutes_ago: i64) -> R<String> {
        Ok((OffsetDateTime::now_utc() - TimeDuration::minutes(minutes_ago)).format(&Rfc3339)?)
    }

    #[test]
    fn mark_node_lost_sets_lost_at_once_and_heartbeat_clears_it() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-one", "active"))?;
        assert_eq!(store.node_lost_at("node-one")?, None);

        store.mark_node_lost("node-one")?;
        let first = store.node_lost_at("node-one")?.context("lost_at set")?;

        // A repeated mark never pushes the loss-confirmation clock back.
        store.mark_node_lost("node-one")?;
        assert_eq!(store.node_lost_at("node-one")?.as_deref(), Some(&*first));

        // A heartbeat means the node is back: lost_at clears, status resets.
        store.update_node_heartbeat("node-one".into())?;
        assert_eq!(store.node_lost_at("node-one")?, None);
        assert_eq!(store.get_node("node-one")?.status, "active");

        // Re-registration (the upsert path) also clears a stale lost_at.
        store.mark_node_lost("node-one")?;
        assert!(store.node_lost_at("node-one")?.is_some());
        store.insert_node(make_node_msg("node-one", "active"))?;
        assert_eq!(store.node_lost_at("node-one")?, None);
        Ok(())
    }

    #[test]
    fn stateful_alloc_on_lost_node_is_held_before_replace_threshold() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        let job_id = "fence-hold";
        submit_job(&store, job_id, vec![fenced_slot_task("db", None, None)])?;
        let task_id = task_id_for(&store, "db", job_id)?;
        let alloc = store.place_alloc("node-one", task_id)?;
        let instance_id = alloc.instance_id.clone().context("instance bound")?;

        // Node just lost: well inside the default 180s replaceAfter window.
        store.mark_nodes_as_lost(&["node-one".to_string()])?;
        run_pending_evals(&store)?;

        // NOT freed: the allocation row stays on the lost node, the instance
        // stays active and bound, and nothing was placed elsewhere.
        let held = store.allocs_for_node("node-one")?;
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].id, alloc.id);
        assert!(store.allocs_for_node("node-two")?.is_empty());
        let instance = store.get_instance(&instance_id)?.context("instance")?;
        assert_eq!(instance.state, InstanceState::Active);
        assert_eq!(instance.current_alloc_id, Some(alloc.id));

        // Re-running the sweep (the monitor tick) is idempotent while the
        // threshold has not elapsed.
        store.recheck_lost_nodes();
        run_pending_evals(&store)?;
        assert_eq!(store.allocs_for_node("node-one")?.len(), 1);
        assert!(store.allocs_for_node("node-two")?.is_empty());
        Ok(())
    }

    #[test]
    fn stateful_alloc_freed_after_threshold_rebinds_same_instance() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        let job_id = "fence-free";
        submit_job(
            &store,
            job_id,
            vec![fenced_slot_task("db", Some("30s"), Some("2m"))],
        )?;
        let task_id = task_id_for(&store, "db", job_id)?;
        let alloc = store.place_alloc("node-one", task_id)?;
        let instance_id = alloc.instance_id.clone().context("instance bound")?;
        let original_handle = store
            .get_instance(&instance_id)?
            .context("instance")?
            .storage_handle;
        assert!(original_handle.is_some());

        store.mark_nodes_as_lost(&["node-one".to_string()])?;
        run_pending_evals(&store)?;
        // Held while inside the window...
        assert_eq!(store.allocs_for_node("node-one")?.len(), 1);

        // ...then the loss is confirmed: backdate lost_at past replaceAfter
        // and run the monitor sweep.
        store.set_node_lost_at("node-one", &backdated_rfc3339(10)?)?;
        store.recheck_lost_nodes();
        run_pending_evals(&store)?;

        assert!(store.allocs_for_node("node-one")?.is_empty());
        let replacements = store.allocs_for_node("node-two")?;
        assert_eq!(replacements.len(), 1);
        // The replacement re-binds the SAME instance id and storage handle.
        assert_eq!(replacements[0].instance_id.as_deref(), Some(&*instance_id));
        let rebound = store.get_instance(&instance_id)?.context("instance")?;
        assert_eq!(rebound.state, InstanceState::Active);
        assert_eq!(rebound.current_alloc_id, Some(replacements[0].id));
        assert_eq!(rebound.storage_handle, original_handle);
        Ok(())
    }

    #[test]
    fn stateful_alloc_with_replace_after_never_blocks_until_node_returns() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        let job_id = "fence-never";
        submit_job(
            &store,
            job_id,
            vec![fenced_slot_task("db", None, Some("never"))],
        )?;
        let task_id = task_id_for(&store, "db", job_id)?;
        let alloc = store.place_alloc("node-one", task_id)?;
        let instance_id = alloc.instance_id.clone().context("instance bound")?;

        store.mark_nodes_as_lost(&["node-one".to_string()])?;
        run_pending_evals(&store)?;
        // Even an ancient loss never auto-replaces with "never".
        store.set_node_lost_at("node-one", &backdated_rfc3339(60 * 24)?)?;
        store.recheck_lost_nodes();
        run_pending_evals(&store)?;
        assert_eq!(store.allocs_for_node("node-one")?.len(), 1);
        assert!(store.allocs_for_node("node-two")?.is_empty());

        // The node returning is what frees it: binding intact, no churn.
        store.update_node_heartbeat("node-one".into())?;
        store.recheck_lost_nodes();
        run_pending_evals(&store)?;
        let kept = store.allocs_for_node("node-one")?;
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].id, alloc.id);
        let instance = store.get_instance(&instance_id)?.context("instance")?;
        assert_eq!(instance.state, InstanceState::Active);
        assert_eq!(instance.current_alloc_id, Some(alloc.id));
        Ok(())
    }

    #[test]
    fn node_recovery_before_threshold_keeps_binding_and_clears_lost_at() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        let job_id = "fence-recover";
        submit_job(&store, job_id, vec![fenced_slot_task("db", None, None)])?;
        let task_id = task_id_for(&store, "db", job_id)?;
        let alloc = store.place_alloc("node-one", task_id)?;
        let instance_id = alloc.instance_id.clone().context("instance bound")?;

        store.mark_nodes_as_lost(&["node-one".to_string()])?;
        run_pending_evals(&store)?;
        assert!(store.node_lost_at("node-one")?.is_some());

        // Node heartbeats again BEFORE the threshold: lost_at clears and the
        // allocation/instance binding is untouched — no replacement ever
        // happened.
        store.update_node_heartbeat("node-one".into())?;
        assert_eq!(store.node_lost_at("node-one")?, None);
        store.recheck_lost_nodes();
        run_pending_evals(&store)?;

        let kept = store.allocs_for_node("node-one")?;
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].id, alloc.id);
        assert!(store.allocs_for_node("node-two")?.is_empty());
        let instance = store.get_instance(&instance_id)?.context("instance")?;
        assert_eq!(instance.state, InstanceState::Active);
        assert_eq!(instance.current_alloc_id, Some(alloc.id));
        Ok(())
    }

    #[test]
    fn stateless_allocs_on_lost_node_replaced_immediately_while_stateful_held() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        let job_id = "fence-mixed";
        submit_job(
            &store,
            job_id,
            vec![task("web"), fenced_slot_task("db", None, None)],
        )?;
        let web_task = task_id_for(&store, "web", job_id)?;
        let db_task = task_id_for(&store, "db", job_id)?;
        store.place_alloc("node-one", web_task)?;
        let db_alloc = store.place_alloc("node-one", db_task)?;

        store.mark_nodes_as_lost(&["node-one".to_string()])?;
        run_pending_evals(&store)?;

        // Stateless web: replaced eagerly on node-two. Stateful db: held on
        // the lost node pending loss confirmation.
        let lost_node_allocs = store.allocs_for_node("node-one")?;
        assert_eq!(lost_node_allocs.len(), 1);
        assert_eq!(lost_node_allocs[0].id, db_alloc.id);
        assert_eq!(lost_node_allocs[0].task_config_id, db_task);

        let replaced = store.allocs_for_node("node-two")?;
        assert_eq!(replaced.len(), 1);
        assert_eq!(replaced[0].task_config_id, web_task);
        Ok(())
    }

    #[test]
    fn dynamic_attrs_can_be_updated_after_join() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "active"))?;

        let mut diff = Map::new();
        diff.insert("foo".to_string(), json!("bar"));

        let updated_node = store.update_node_dyamic("node-one", &diff)?;
        let foo = updated_node
            .dynamic_attrs
            .get("foo")
            .context("foo missing from dynamic attrs")?;

        assert_eq!(foo, "bar");

        Ok(())
    }

    #[test]
    fn get_ledger_includes_lost_nodes() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-one", "active"))?;
        store.insert_node(make_node_msg("node-two", "active"))?;

        store.mark_nodes_as_lost(&["node-two".to_string()])?;

        let ledger = store.get_ledger()?;
        assert_eq!(ledger.len(), 2);

        let lost_entry = ledger
            .iter()
            .find(|entry| entry.node.id == "node-two")
            .context("node-two missing from ledger")?;
        assert_eq!(lost_entry.node.status, "lost");

        Ok(())
    }

    #[test]
    fn handle_job_submit_adds_new_job() -> R<()> {
        let sub_one = Submission {
            path: "sub-1.jsonnet".into(),
            content: "sub-1".into(),
            is_primary: true,
        };
        let sub_two = Submission {
            path: "sub-2.libsonnet".into(),
            content: "sub-2".into(),
            is_primary: false,
        };

        let subs = vec![sub_one.clone(), sub_two.clone()];

        let task_one = TaskConfig {
            config: json!({ "message": "hello-1" }),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("task-1", "runtime-1")
        };

        let task_two = TaskConfig {
            count: TaskCount::new(2),
            config: json!({ "message": "hi-2" }),
            resources: json!({ "memory": 512 }),
            ..TaskConfig::new("task-2", "runtime-2")
        };

        let task_configs = vec![task_one.clone(), task_two.clone()];
        let job_name = "test".to_string();
        let job_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: task_configs.clone(),
            version: None,
        };
        let store = setup_store()?;

        let returned_config = store.get_job(&job_name);
        assert!(returned_config.is_err());
        assert_eq!(
            returned_config.unwrap_err().to_string(),
            "job not found (id: test)"
        );

        // NOTE: What should this return (figure it out in concontent later)
        let submit_response = store.handle_job_submit(&job_config, &subs)?;
        assert!(matches!(submit_response, JobSubmitOutcome::Created { .. }));

        let job = store.get_job(&job_name)?;
        assert_eq!(job.name, job_name);

        let latest_job_config = store.get_job_config(&job_name)?;
        assert_eq!(latest_job_config.name, job_name);

        let t1_from_store = latest_job_config.tasks.first().context("oof")?;
        assert_eq!(task_one.name, t1_from_store.name);
        assert_eq!(task_one.runtime, t1_from_store.runtime);
        assert_eq!(
            task_one.resources.get("memory").context("no memory 1")?,
            t1_from_store
                .resources
                .get("memory")
                .context("no memory 2")?
        );

        assert_eq!(
            task_one.config.get("message").context("no message 1")?,
            t1_from_store
                .config
                .get("message")
                .context("no message 2")?
        );

        let t2_from_store = latest_job_config.tasks.get(1).context("oof")?;
        assert_eq!(task_two.name, t2_from_store.name);

        let subs_from_store = store.get_submissions_for_config(&latest_job_config.id())?;
        assert_eq!(subs_from_store.len(), 2);
        let sub_one_from_store = subs_from_store.first().context("no sub 1")?;
        let sub_two_from_store = subs_from_store.get(1).context("no sub 2")?;
        assert_eq!(sub_one_from_store.path, sub_one.path);
        assert_eq!(sub_two_from_store.path, sub_two.path);

        Ok(())
    }

    #[test]
    fn handle_job_submit_rolls_back_on_failure() -> R<()> {
        let task_one = TaskConfig {
            config: json!({ "message": "hello-1" }),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("task-1", "runtime-1")
        };

        let job_name = "rollback-job".to_string();
        let job_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks: vec![task_one],
            version: None,
        };

        let store = setup_store()?;
        {
            let conn = store.write_connection()?;
            conn.execute("DROP TABLE job_config_submission;", [])?;
        }

        let result = store.handle_job_submit(
            &job_config,
            &[Submission {
                path: "sub-1.jsonnet".into(),
                content: "sub-1".into(),
                is_primary: true,
            }],
        );

        assert!(result.is_err());
        assert!(store.get_job(&job_name).is_err());
        assert!(store.get_job_config(&job_name).is_err());

        Ok(())
    }

    #[test]
    fn handle_job_submit_updates_job_properly() -> R<()> {
        let orib_sub = Submission {
            path: "sub-1.jsonnet".into(),
            content: "sub-1".into(),
            is_primary: true,
        };
        let orig_submissions = vec![orib_sub.clone()];

        let orig_task = TaskConfig {
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("orig-task", "runtime-1")
        };

        let orig_task_configs = vec![orig_task.clone()];
        let job_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "jobname".into(),
            tasks: orig_task_configs.clone(),
            version: None,
        };

        let store = setup_store()?;

        // Original Job is submitted
        let submit_response = store.handle_job_submit(&job_config, &orig_submissions)?;
        assert!(matches!(submit_response, JobSubmitOutcome::Created { .. }));

        // Get config for initial insert
        let first_job_config = store.get_job_config("jobname")?;
        assert_eq!(first_job_config.name, job_config.name);
        assert_eq!(
            1,
            first_job_config
                .version
                .context("missing version on config")?,
        );

        // Submit an updates job config
        // updated: task runtime config

        let new_task = TaskConfig {
            runtime: "runtime-new".to_string(),
            ..orig_task
        };
        let new_task_configs = vec![new_task.clone()];
        let job_config_with_modified_runtime = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            tasks: new_task_configs.clone(),
            ..job_config
        };

        let submit_response = store
            .handle_job_submit(&job_config_with_modified_runtime.clone(), &orig_submissions)?;
        assert!(matches!(submit_response, JobSubmitOutcome::Updated { .. }));

        // Get config for initial insert
        let second_job_config = store.get_job_config("jobname")?;
        assert_eq!(
            2,
            second_job_config
                .version
                .context("missing version on config")?,
        );

        // Submit an updates job config
        // updated: additional task added

        let extra_task = TaskConfig {
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("extra-task", "runtime-extra")
        };
        let new_task_configs = vec![new_task.clone(), extra_task.clone()];

        let job_config_with_extra_task = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            tasks: new_task_configs.clone(),
            ..job_config_with_modified_runtime
        };

        let submit_response =
            store.handle_job_submit(&job_config_with_extra_task, &orig_submissions)?;
        assert!(matches!(submit_response, JobSubmitOutcome::Updated { .. }));

        // Get config for initial insert
        let third_job_config = store.get_job_config("jobname")?;
        assert_eq!(
            3,
            third_job_config
                .version
                .context("missing version on config")?,
        );

        // // Submit an updates job config
        // // updated: metadata on job

        // let job_config_with_new_metadata = JobConfig {
        //     ... TODO ...
        // };

        // let submit_response = store.handle_job_submit(&job_config, &orig_submissions);
        // assert!(submit_response.is_ok());

        // // Get config for initial insert
        // let fourth_job_config = store.get_job_config("jobname")?;
        // assert_eq!(
        //     4,
        //     fourth_job_config
        //         .version
        //         .context("missing version on config")?,
        // );

        Ok(())
    }

    #[test]
    fn handle_job_submit_with_unchanged_job_is_idempotent() -> R<()> {
        let orig_submissions = vec![];
        let orig_task = TaskConfig {
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("dupe-task", "runtime")
        };

        let orig_task_configs = vec![orig_task.clone()];
        let job_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "jobname".into(),
            tasks: orig_task_configs.clone(),
            version: None,
        };

        let store = setup_store()?;

        // Original Job is submitted
        let submit_response = store.handle_job_submit(&job_config, &orig_submissions)?;
        assert!(matches!(submit_response, JobSubmitOutcome::Created { .. }));

        // Resubmitting with unchanged config is idempotent
        let submit_response = store.handle_job_submit(&job_config, &orig_submissions)?;
        assert!(matches!(
            submit_response,
            JobSubmitOutcome::Unchanged { .. }
        ));

        Ok(())
    }

    #[test]
    fn placing_allocations_and_querying_them() -> R<()> {
        let store = setup_store()?;

        let node_one_id = "node-one".to_string();
        let node_two_id = "node-two".to_string();
        store.insert_node(NodeMsg::from(make_node(&node_one_id)))?;
        store.insert_node(NodeMsg::from(make_node(&node_two_id)))?;

        let job_one_name = "job-one".to_string();
        let task_one = TaskConfig {
            config: json!({ "message": "hello" }),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("job-one-task-1", "driver-a")
        };
        let task_two = TaskConfig {
            config: json!({ "message": "world" }),
            resources: json!({ "memory": 512 }),
            ..TaskConfig::new("job-one-task-2", "driver-b")
        };
        let job_one_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_one_name.clone(),
            tasks: vec![task_one.clone(), task_two.clone()],
            version: None,
        };
        let job_one_subs = vec![Submission {
            path: "job-one-sub.jsonnet".into(),
            content: "job-one".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_one_config, &job_one_subs)?;

        let job_one_config_id = store.get_job_config(&job_one_name)?.id();
        let task_one_id = task_id_for(&store, &task_one.name, &job_one_name)?;
        let task_two_id = task_id_for(&store, &task_two.name, &job_one_name)?;

        let job_two_name = "job-two".to_string();
        let task_three = TaskConfig {
            config: json!({ "message": "hola" }),
            resources: json!({ "memory": 128 }),
            ..TaskConfig::new("job-two-task-1", "driver-c")
        };
        let job_two_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_two_name.clone(),
            tasks: vec![task_three.clone()],
            version: None,
        };
        let job_two_subs = vec![Submission {
            path: "job-two-sub.jsonnet".into(),
            content: "job-two".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_two_config, &job_two_subs)?;

        let job_two_config_id = store.get_job_config(&job_two_name)?.id();
        let task_three_id = task_id_for(&store, &task_three.name, &job_two_name)?;

        let alloc_one = store.place_alloc(&node_one_id, task_one_id)?;
        let alloc_two = store.place_alloc(&node_two_id, task_two_id)?;
        let alloc_three = store.place_alloc(&node_one_id, task_three_id)?;

        assert_eq!(alloc_one.status, "placed");
        assert_eq!(alloc_one.node_id, node_one_id);
        assert_eq!(alloc_one.task_config_id, task_one_id);
        assert_eq!(alloc_one.job_config_id, job_one_config_id);
        let expected_one = store
            .resource_policy
            .normalize_task_resources(&task_one.resources, false)?;
        let expected_two = store
            .resource_policy
            .normalize_task_resources(&task_two.resources, false)?;
        let expected_three = store
            .resource_policy
            .normalize_task_resources(&task_three.resources, false)?;

        assert_eq!(alloc_one.resources, expected_one);

        assert_eq!(alloc_two.resources, expected_two);
        assert_eq!(alloc_three.job_config_id, job_two_config_id);
        assert_eq!(alloc_three.resources, expected_three);

        let job_one_allocs = store.allocs_for_job(&job_one_name)?;
        assert_eq!(job_one_allocs.len(), 2);
        let job_one_task_ids: Vec<i64> = job_one_allocs.iter().map(|a| a.task_config_id).collect();
        assert!(job_one_task_ids.contains(&task_one_id));
        assert!(job_one_task_ids.contains(&task_two_id));

        let job_two_allocs = store.allocs_for_job(&job_two_name)?;
        assert_eq!(job_two_allocs.len(), 1);
        assert_eq!(job_two_allocs[0].task_config_id, task_three_id);

        let job_one_config_allocs = store.allocs_for_job_config(&job_one_config_id)?;
        assert_eq!(job_one_config_allocs.len(), 2);
        let job_two_config_allocs = store.allocs_for_job_config(&job_two_config_id)?;
        assert_eq!(job_two_config_allocs.len(), 1);
        assert_eq!(job_two_config_allocs[0].task_config_id, task_three_id);

        let node_one_allocs = store.allocs_for_node(&node_one_id)?;
        assert_eq!(node_one_allocs.len(), 2);
        let node_two_allocs = store.allocs_for_node(&node_two_id)?;
        assert_eq!(node_two_allocs.len(), 1);

        let all_allocs = store.get_allocs()?;
        assert_eq!(all_allocs.len(), 3);

        let allocs_by_node = store.get_allocs_by_node()?;
        let node_one_group = allocs_by_node
            .get(&node_one_id)
            .context("missing node one allocs")?;
        assert_eq!(node_one_group.len(), 2);
        let node_two_group = allocs_by_node
            .get(&node_two_id)
            .context("missing node two allocs")?;
        assert_eq!(node_two_group.len(), 1);

        Ok(())
    }

    #[test]
    fn destroying_a_job_destroys_all_associated_objects() -> R<()> {
        let store = setup_store()?;

        let submissions = vec![
            Submission {
                path: "sub-1.jsonnet".into(),
                content: "sub-1".into(),
                is_primary: true,
            },
            Submission {
                path: "sub-2.jsonnet".into(),
                content: "sub-2".into(),
                is_primary: false,
            },
        ];

        let tasks = vec![TaskConfig::new("task-1", "runtime-1")];

        let job_name = "solo-job".to_string();
        let job_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.clone(),
            tasks,
            version: None,
        };

        store.handle_job_submit(&job_config, &submissions)?;
        store.stop_job(&job_name)?;

        {
            let conn = store.read_connection()?;
            let submission_count: i64 =
                conn.query_row("SELECT COUNT(*) FROM submission;", [], |row| row.get(0))?;
            assert_eq!(submission_count, 2);
        }

        store.destroy_job(&job_name)?;

        let conn = store.read_connection()?;
        let job_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM job WHERE name = ?;",
            [job_name],
            |row| row.get(0),
        )?;
        let job_config_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM job_config WHERE job_id = ?;",
            [job_config.name],
            |row| row.get(0),
        )?;
        let job_config_submission_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM job_config_submission;", [], |row| {
                row.get(0)
            })?;
        let submission_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM submission;", [], |row| row.get(0))?;

        assert_eq!(job_count, 0);
        assert_eq!(job_config_count, 0);
        assert_eq!(job_config_submission_count, 0);
        assert_eq!(submission_count, 0);

        Ok(())
    }

    #[test]
    fn destroying_a_job_wont_destroy_other_used_submissions() -> R<()> {
        let store = setup_store()?;

        let shared_submission = Submission {
            path: "shared.jsonnet".into(),
            content: "shared-content".into(),
            is_primary: true,
        };

        let tasks = vec![TaskConfig::new("task-shared", "runtime")];

        let job_one_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-one".into(),
            tasks: tasks.clone(),
            version: None,
        };

        let job_two_config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-two".into(),
            tasks,
            version: None,
        };

        let job_one_subs = vec![shared_submission.clone()];
        let job_two_subs = vec![shared_submission.clone()];

        store.handle_job_submit(&job_one_config, &job_one_subs)?;
        store.handle_job_submit(&job_two_config, &job_two_subs)?;

        store.stop_job("job-one")?;
        store.destroy_job("job-one")?;

        let job_two = store.get_job("job-two")?;
        assert_eq!(job_two.name, "job-two");

        let conn = store.read_connection()?;
        let submission_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM submission;", [], |row| row.get(0))?;
        let job_one_records: i64 = conn.query_row(
            "SELECT COUNT(*) FROM job WHERE name = ?;",
            ["job-one"],
            |row| row.get(0),
        )?;
        let job_two_submission_refs: i64 = conn.query_row(
            r#"
            SELECT COUNT(*)
            FROM job_config_submission jcs
            INNER JOIN job_config jc ON jc.id = jcs.job_config_id
            WHERE jc.job_id = ?;
        "#,
            ["job-two"],
            |row| row.get(0),
        )?;

        assert_eq!(job_one_records, 0);
        assert_eq!(submission_count, 1);
        assert_eq!(job_two_submission_refs, 1);

        Ok(())
    }

    #[test]
    fn claim_eval_marks_in_flight_and_dedupes_pending() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-1", "ready"))?;

        insert_eval(
            &store,
            1,
            "pending",
            "node_join",
            None,
            None,
            Some("node-1"),
            50,
        )?;
        insert_eval(
            &store,
            2,
            "pending",
            "node_join",
            None,
            None,
            Some("node-1"),
            50,
        )?;

        let claimed = store.claim_eval()?.expect("eval should be claimed");
        assert_eq!(claimed.id, "1");
        assert_eq!(claimed.status, "in_flight");

        let conn = store.read_connection()?;
        let pending: i64 = conn.query_row(
            "SELECT COUNT(*) FROM eval WHERE status = 'pending';",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            pending, 0,
            "duplicates for the same job/node should be removed"
        );

        Ok(())
    }

    #[test]
    fn delayed_eval_is_not_claimed_until_ready() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-1", "ready"))?;
        let delayed = store.enqueue_eval_after_delay(
            "node_join",
            None,
            None,
            Some("node-1"),
            Some(10),
            Duration::from_secs(60),
        )?;

        assert!(store.claim_eval()?.is_none());

        let delayed_id = delayed.id.clone();
        let conn = store.write_connection()?;
        conn.execute(
            "UPDATE eval SET ready_at = '1970-01-01T00:00:00Z' WHERE id = ?;",
            [delayed_id],
        )?;
        drop(conn);

        let claimed = store
            .claim_eval()?
            .context("delayed eval should now be claimable")?;
        assert_eq!(claimed.id, delayed.id);

        Ok(())
    }

    #[test]
    fn claim_eval_dedupe_spares_other_kinds_and_delayed_work() -> R<()> {
        let store = setup_store()?;
        let job_config_id = submit_job(&store, "job-a", vec![task("api")])?;
        let cfg = Some(job_config_id.as_str());

        // A rollout's next tick and a batch task's next scheduled update are
        // both pending for the same job when a reschedule gets claimed.
        store.enqueue_eval("rollout_tick", Some("job-a"), cfg, None, Some(50))?;
        store.enqueue_eval_after_delay(
            "job_update",
            Some("job-a"),
            cfg,
            None,
            Some(50),
            Duration::from_secs(3600),
        )?;
        // Two identical reschedules: the redundant one is what dedupe is for.
        store.enqueue_eval("job_reschedule", Some("job-a"), cfg, None, Some(50))?;
        store.enqueue_eval("job_reschedule", Some("job-a"), cfg, None, Some(50))?;

        let claimed = store.claim_eval()?.context("eval should be claimed")?;
        assert_eq!(claimed.kind, "rollout_tick");
        store.complete_eval(&claimed.id, true)?;
        let claimed = store.claim_eval()?.context("eval should be claimed")?;
        assert_eq!(claimed.kind, "job_reschedule");

        let remaining: Vec<String> = store
            .get_evals()?
            .into_iter()
            .filter(|eval| eval.status == "pending")
            .map(|eval| eval.kind)
            .collect();
        assert_eq!(
            remaining,
            vec!["job_update".to_string()],
            "only the duplicate reschedule should have been collapsed"
        );

        Ok(())
    }

    #[test]
    fn zero_delay_evals_are_immediately_claimable() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-1", "ready"))?;

        // `ready_at` is range-compared as text in SQL, so a timestamp format
        // that does not sort lexicographically makes a freshly enqueued eval
        // intermittently invisible. Roughly one write in ten lands on a
        // trailing-zero subsecond, so a single round would miss the
        // regression.
        for _ in 0..200 {
            let enqueued = store.enqueue_eval("node_join", None, None, Some("node-1"), Some(50))?;
            let claimed = store
                .claim_eval()?
                .context("zero-delay eval should be claimable right away")?;
            assert_eq!(claimed.id, enqueued.id);
            store.complete_eval(&claimed.id, true)?;
        }

        Ok(())
    }

    #[test]
    fn sortable_timestamps_sort_chronologically() -> R<()> {
        let base = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
        let earlier = Store::sortable_timestamp(base + TimeDuration::nanoseconds(437_400_000))?;
        let later = Store::sortable_timestamp(base + TimeDuration::nanoseconds(437_409_000))?;
        assert!(
            earlier < later,
            "expected {earlier} to sort before {later} as text"
        );
        assert_eq!(earlier.len(), later.len());
        // Still a valid RFC3339 instant, so parsing paths are unaffected.
        assert_eq!(
            OffsetDateTime::parse(&later, &Rfc3339)?,
            base + TimeDuration::nanoseconds(437_409_000)
        );

        Ok(())
    }

    #[test]
    fn complete_eval_removes_or_marks_terminal() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-1", "ready"))?;
        store.insert_node(make_node_msg("node-2", "ready"))?;

        insert_eval(
            &store,
            1,
            "pending",
            "node_join",
            None,
            None,
            Some("node-1"),
            50,
        )?;
        insert_eval(
            &store,
            2,
            "pending",
            "node_drop",
            None,
            None,
            Some("node-2"),
            50,
        )?;

        let first = store.claim_eval()?.expect("eval should be claimed");
        store.complete_eval(&first.id, true)?;

        let second = store.claim_eval()?.expect("second eval should be claimed");
        store.complete_eval(&second.id, false)?;

        let conn = store.read_connection()?;
        let remaining: i64 = conn.query_row("SELECT COUNT(*) FROM eval;", [], |row| row.get(0))?;
        assert_eq!(remaining, 1, "successful evals should be removed");

        let failed_status: String = conn.query_row(
            "SELECT status FROM eval WHERE id = ?;",
            [second.id],
            |row| row.get(0),
        )?;
        assert_eq!(failed_status, "failed");

        Ok(())
    }

    #[test]
    fn requeue_in_flight_evals_moves_stale_work_back_to_pending() -> R<()> {
        let store = setup_store()?;

        store.insert_node(make_node_msg("node-1", "ready"))?;

        insert_eval(
            &store,
            1,
            "in_flight",
            "node_join",
            None,
            None,
            Some("node-1"),
            50,
        )?;
        insert_eval(
            &store,
            2,
            "pending",
            "node_join",
            None,
            None,
            Some("node-1"),
            40,
        )?;

        let requeued = store.requeue_in_flight_evals()?;
        assert_eq!(requeued, 1);

        let claimed = store.claim_eval()?.expect("eval should be claimed");
        assert_eq!(
            claimed.id, "1",
            "higher-priority stale eval should run first"
        );

        Ok(())
    }

    #[test]
    fn plan_submission_conflict_is_scoped_to_job_config_epoch() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;

        let job_one = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-one".into(),
            tasks: vec![TaskConfig::new("task-one", "noop")],
            version: None,
        };
        let job_two = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-two".into(),
            tasks: vec![TaskConfig::new("task-two", "noop")],
            version: None,
        };

        let submission = [Submission {
            path: "job.jsonnet".into(),
            content: "job".into(),
            is_primary: true,
        }];

        store.handle_job_submit(&job_one, &submission)?;
        store.handle_job_submit(&job_two, &submission)?;

        let job_one_config_id = store.get_job_config("job-one")?.id();
        let job_two_config_id = store.get_job_config("job-two")?.id();
        let task_one_id = task_id_for(&store, "task-one", "job-one")?;
        let task_two_id = task_id_for(&store, "task-two", "job-two")?;

        let job_one_epoch = store.allocation_epoch_for_job_config(&job_one_config_id)?;
        let job_two_epoch = store.allocation_epoch_for_job_config(&job_two_config_id)?;
        let job_one_versions = BTreeMap::from([
            (
                Store::scope_vector_key("job_config", &job_one_config_id),
                job_one_epoch,
            ),
            (Store::scope_vector_key("node", "node-a"), 0),
        ]);
        let job_two_versions = BTreeMap::from([
            (
                Store::scope_vector_key("job_config", &job_two_config_id),
                job_two_epoch,
            ),
            (Store::scope_vector_key("node", "node-b"), 0),
        ]);

        let job_one_plan = store.insert_plan_submission(
            &job_one_config_id,
            "fresh",
            &[("node-a".to_string(), task_one_id)],
            &job_one_versions,
        )?;
        let job_two_plan = store.insert_plan_submission(
            &job_two_config_id,
            "fresh",
            &[("node-b".to_string(), task_two_id)],
            &job_two_versions,
        )?;

        assert!(matches!(
            store.apply_plan_submission(&job_one_plan)?,
            PlanApplyOutcome::Applied(_)
        ));
        assert!(matches!(
            store.apply_plan_submission(&job_two_plan)?,
            PlanApplyOutcome::Applied(_)
        ));

        Ok(())
    }

    #[test]
    fn place_alloc_bumps_job_config_epoch() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-epoch".into(),
            tasks: vec![TaskConfig::new("task", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;

        let job_config_id = store.get_job_config("job-epoch")?.id();
        let task_id = task_id_for(&store, "task", "job-epoch")?;
        let before = store.allocation_epoch_for_job_config(&job_config_id)?;
        store.place_alloc("node-a", task_id)?;
        let after = store.allocation_epoch_for_job_config(&job_config_id)?;

        assert_eq!(after, before + 1);
        Ok(())
    }

    #[test]
    fn migrate_allocations_bumps_source_and_destination_job_epochs() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let job_v1 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-migrate".into(),
            tasks: vec![TaskConfig::new("task", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job_v1,
            &[Submission {
                path: "v1.jsonnet".into(),
                content: "v1".into(),
                is_primary: true,
            }],
        )?;

        let v1_id = store.get_job_config("job-migrate")?.id();
        let v1_task = task_id_for(&store, "task", "job-migrate")?;
        store.place_alloc("node-a", v1_task)?;

        let mut v2_task = TaskConfig::new("task", "noop");
        v2_task.config = json!({"changed": true});
        let job_v2 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-migrate".into(),
            tasks: vec![v2_task],
            version: None,
        };
        store.handle_job_submit(
            &job_v2,
            &[Submission {
                path: "v2.jsonnet".into(),
                content: "v2".into(),
                is_primary: true,
            }],
        )?;
        let v2_id = store.get_job_config("job-migrate")?.id();
        assert_ne!(v1_id, v2_id);

        let v1_before = store.allocation_epoch_for_job_config(&v1_id)?;
        let v2_before = store.allocation_epoch_for_job_config(&v2_id)?;

        let migrated = store.migrate_allocations_to_job_config(&v1_id, &v2_id)?;
        assert_eq!(migrated.len(), 1);

        let v1_after = store.allocation_epoch_for_job_config(&v1_id)?;
        let v2_after = store.allocation_epoch_for_job_config(&v2_id)?;

        assert_eq!(v1_after, v1_before + 1);
        assert_eq!(v2_after, v2_before + 1);

        Ok(())
    }

    #[test]
    fn candidate_version_vector_matches_per_scope_reads_test() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-candidates".into(),
            tasks: vec![TaskConfig::new("task", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config("job-candidates")?.id();
        let task_id = task_id_for(&store, "task", "job-candidates")?;

        let candidates = vec!["node-a".to_string(), "node-b".to_string()];

        // Nodes that have never held an allocation have no epoch row; the
        // batched read must report the same 0 the per-scope read does, because
        // that is the value `apply_plan_submission` compares against.
        let sampled = store.allocation_version_vector_for_candidates(
            &job_config_id,
            &candidates.iter().map(String::as_str).collect::<Vec<_>>(),
        )?;
        assert_eq!(sampled.get("node:node-a").copied(), Some(0));
        assert_eq!(sampled.get("node:node-b").copied(), Some(0));
        assert_eq!(
            sampled.get(&format!("job_config:{job_config_id}")).copied(),
            Some(0)
        );

        // A plan sampled on the fresh node applies, bumping node-a only.
        let submission = store.insert_plan_submission(
            &job_config_id,
            "fresh",
            &[("node-a".to_string(), task_id)],
            &store.allocation_version_vector(
                &job_config_id,
                &HashSet::from(["node-a".to_string()]),
            )?,
        )?;
        assert!(matches!(
            store.apply_plan_submission(&submission)?,
            PlanApplyOutcome::Applied(_)
        ));

        let after = store.allocation_version_vector_for_candidates(
            &job_config_id,
            &candidates.iter().map(String::as_str).collect::<Vec<_>>(),
        )?;
        let per_scope = store.allocation_version_vector(
            &job_config_id,
            &HashSet::from(["node-a".to_string(), "node-b".to_string()]),
        )?;
        assert_eq!(after, per_scope, "batched read must match per-scope reads");
        assert_eq!(after.get("node:node-a").copied(), Some(1));
        assert_eq!(after.get("node:node-b").copied(), Some(0));

        Ok(())
    }

    #[test]
    fn read_model_generation_bumps_on_allocation_publish() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-generation".into(),
            tasks: vec![TaskConfig::new("task", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;
        let task_id = task_id_for(&store, "task", "job-generation")?;

        let quiet_a = store.read_model_generation()?;
        let quiet_b = store.read_model_generation()?;
        assert_eq!(quiet_a, quiet_b, "reads alone must not move the counter");

        store.place_alloc("node-a", task_id)?;
        let after_place = store.read_model_generation()?;
        assert!(
            after_place > quiet_b,
            "an allocation publish must bump the generation"
        );

        // A full rebuild is a publish too (it may change anything).
        store.refresh_read_model()?;
        let after_refresh = store.read_model_generation()?;
        assert!(after_refresh > after_place);

        Ok(())
    }

    #[test]
    fn lazy_expected_versions_match_eager_when_quiet_and_bail_on_publish() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-lazy".into(),
            tasks: vec![TaskConfig::new("task", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config("job-lazy")?.id();
        let task_id = task_id_for(&store, "task", "job-lazy")?;
        store.place_alloc("node-a", task_id)?;

        // Quiet read model: the late sample is available and equals the
        // eager path's selection for the same affected scopes.
        let generation = store.read_model_generation()?;
        let affected = HashSet::from(["node-a".to_string()]);
        let lazy = store
            .expected_versions_if_unchanged(&job_config_id, &affected, generation)?
            .expect("generation unchanged: the lazy sample must be available");
        let eager = store.allocation_version_vector(&job_config_id, &affected)?;
        assert_eq!(lazy, eager, "lazy sample must equal the eager sample");

        // A publish between the capacity read and the late sample voids it.
        store.place_alloc("node-b", task_id)?;
        assert!(
            store
                .expected_versions_if_unchanged(&job_config_id, &affected, generation)?
                .is_none(),
            "a moved generation must force the eager fallback"
        );

        Ok(())
    }

    fn submit_named_job(store: &Store, name: &str) -> R<(String, i64)> {
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks: vec![TaskConfig::new("task", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: format!("{name}.jsonnet"),
                content: name.into(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config(name)?.id();
        let task_id = task_id_for(store, "task", name)?;
        Ok((job_config_id, task_id))
    }

    #[test]
    fn plan_batch_isolates_batch_mate_outcomes() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;
        store.insert_node(make_node_msg("node-c", "ready"))?;
        let (job_a, task_a) = submit_named_job(&store, "batch-job-a")?;
        let (job_b, _task_b) = submit_named_job(&store, "batch-job-b")?;
        let (job_c, task_c) = submit_named_job(&store, "batch-job-c")?;

        let versions_a =
            store.allocation_version_vector(&job_a, &HashSet::from(["node-a".to_string()]))?;
        // Poisoned: a placement referencing a task id that does not exist,
        // on its own node so the epoch CAS passes and the failure happens
        // mid-apply (the isolation case, not a rejection).
        let versions_b =
            store.allocation_version_vector(&job_b, &HashSet::from(["node-c".to_string()]))?;
        let versions_c =
            store.allocation_version_vector(&job_c, &HashSet::from(["node-b".to_string()]))?;

        let results = store.run_plan_apply_batch(vec![
            PlanApplyRequest {
                job_config_id: job_a.clone(),
                mode: "fresh".into(),
                placements: vec![("node-a".to_string(), task_a)],
                expected_versions: versions_a,
                evictions: Vec::new(),
            },
            PlanApplyRequest {
                job_config_id: job_b.clone(),
                mode: "fresh".into(),
                placements: vec![("node-c".to_string(), 999_999)],
                expected_versions: versions_b,
                evictions: Vec::new(),
            },
            PlanApplyRequest {
                job_config_id: job_c.clone(),
                mode: "fresh".into(),
                placements: vec![("node-b".to_string(), task_c)],
                expected_versions: versions_c,
                evictions: Vec::new(),
            },
        ]);

        assert_eq!(results.len(), 3);
        assert!(
            matches!(results[0], Ok((_, PlanApplyOutcome::Applied(_)))),
            "first plan must apply: {:?}",
            results[0]
        );
        assert!(results[1].is_err(), "poisoned plan must fail alone");
        assert!(
            matches!(results[2], Ok((_, PlanApplyOutcome::Applied(_)))),
            "batch-mate after the failure must still apply: {:?}",
            results[2]
        );
        assert_eq!(store.allocs_for_node("node-a")?.len(), 1);
        assert_eq!(store.allocs_for_node("node-b")?.len(), 1);
        Ok(())
    }

    #[test]
    fn plan_batch_keeps_per_plan_cas_verdicts() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        let (job_a, task_a) = submit_named_job(&store, "cas-job-a")?;
        let (job_b, task_b) = submit_named_job(&store, "cas-job-b")?;

        let fresh =
            store.allocation_version_vector(&job_a, &HashSet::from(["node-a".to_string()]))?;
        // Stale by construction: bump node-a's epoch after sampling.
        let stale =
            store.allocation_version_vector(&job_b, &HashSet::from(["node-a".to_string()]))?;
        store.place_alloc("node-a", task_a)?;

        let results = store.run_plan_apply_batch(vec![PlanApplyRequest {
            job_config_id: job_b.clone(),
            mode: "fresh".into(),
            placements: vec![("node-a".to_string(), task_b)],
            expected_versions: stale,
            evictions: Vec::new(),
        }]);
        assert!(
            matches!(results[0], Ok((_, PlanApplyOutcome::Rejected { .. }))),
            "stale epochs must reject inside a batch exactly as they do alone: {:?}",
            results[0]
        );

        // And a fresh sample for the untouched job still applies.
        let _ = fresh;
        Ok(())
    }

    #[test]
    fn concurrent_plan_submitters_coalesce_and_every_plan_lands() -> R<()> {
        let store = setup_store()?;
        let mut requests = Vec::new();
        for index in 0..8 {
            let node = format!("node-{index}");
            store.insert_node(make_node_msg(&node, "ready"))?;
            let (job_config_id, task_id) =
                submit_named_job(&store, &format!("coalesce-job-{index}"))?;
            let versions =
                store.allocation_version_vector(&job_config_id, &HashSet::from([node.clone()]))?;
            requests.push((job_config_id, node, task_id, versions));
        }

        let handles: Vec<_> = requests
            .into_iter()
            .map(|(job_config_id, node, task_id, versions)| {
                let store = store.clone();
                std::thread::spawn(move || {
                    store.submit_and_apply_plan(
                        &job_config_id,
                        "fresh",
                        &[(node, task_id)],
                        &versions,
                        &[],
                    )
                })
            })
            .collect();

        for handle in handles {
            let (_, outcome) = handle.join().expect("submitter panicked")?;
            assert!(
                matches!(outcome, PlanApplyOutcome::Applied(_)),
                "distinct-scope plans must all apply: {outcome:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn plan_submission_rejects_stale_epoch_for_same_job_config() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-stale".into(),
            tasks: vec![TaskConfig::new("task", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;

        let job_config_id = store.get_job_config("job-stale")?.id();
        let task_id = task_id_for(&store, "task", "job-stale")?;
        let expected_versions = store
            .allocation_version_vector(&job_config_id, &HashSet::from(["node-a".to_string()]))?;

        let first = store.insert_plan_submission(
            &job_config_id,
            "fresh",
            &[("node-a".to_string(), task_id)],
            &expected_versions,
        )?;
        let second = store.insert_plan_submission(
            &job_config_id,
            "fresh",
            &[("node-a".to_string(), task_id)],
            &expected_versions,
        )?;

        assert!(matches!(
            store.apply_plan_submission(&first)?,
            PlanApplyOutcome::Applied(_)
        ));

        match store.apply_plan_submission(&second)? {
            PlanApplyOutcome::Rejected { .. } => {}
            PlanApplyOutcome::Applied(_) => {
                panic!("expected stale plan to be rejected");
            }
        }

        Ok(())
    }

    #[test]
    fn plan_submission_rejects_stale_epoch_for_shared_node_scope() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let make_job = |name: &str, task_name: &str| JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks: vec![TaskConfig::new(task_name, "noop")],
            version: None,
        };

        let submission = [Submission {
            path: "job.jsonnet".into(),
            content: "job".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&make_job("job-scope-a", "task-a"), &submission)?;
        store.handle_job_submit(&make_job("job-scope-b", "task-b"), &submission)?;

        let job_a_config = store.get_job_config("job-scope-a")?.id();
        let job_b_config = store.get_job_config("job-scope-b")?.id();
        let task_a = task_id_for(&store, "task-a", "job-scope-a")?;
        let task_b = task_id_for(&store, "task-b", "job-scope-b")?;

        let job_a_versions = store
            .allocation_version_vector(&job_a_config, &HashSet::from(["node-a".to_string()]))?;
        let job_b_versions = store
            .allocation_version_vector(&job_b_config, &HashSet::from(["node-a".to_string()]))?;

        let first = store.insert_plan_submission(
            &job_a_config,
            "fresh",
            &[("node-a".to_string(), task_a)],
            &job_a_versions,
        )?;
        let second = store.insert_plan_submission(
            &job_b_config,
            "fresh",
            &[("node-a".to_string(), task_b)],
            &job_b_versions,
        )?;

        assert!(matches!(
            store.apply_plan_submission(&first)?,
            PlanApplyOutcome::Applied(_)
        ));
        assert!(matches!(
            store.apply_plan_submission(&second)?,
            PlanApplyOutcome::Rejected { .. }
        ));

        Ok(())
    }

    #[test]
    fn plan_submission_rejects_due_to_constraints_with_latest_epoch() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;

        let constrained_task = {
            let mut task = TaskConfig::new("task", "noop");
            task.constraints = json!({ "node_id": "node-a" });
            task
        };
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-constraints".into(),
            tasks: vec![constrained_task],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;

        let job_config_id = store.get_job_config("job-constraints")?.id();
        let task_id = task_id_for(&store, "task", "job-constraints")?;
        let expected_versions = store
            .allocation_version_vector(&job_config_id, &HashSet::from(["node-b".to_string()]))?;
        let expected_job_epoch = *expected_versions
            .get(&Store::scope_vector_key("job_config", &job_config_id))
            .unwrap_or(&0);

        let submission = store.insert_plan_submission(
            &job_config_id,
            "fresh",
            &[("node-b".to_string(), task_id)],
            &expected_versions,
        )?;
        let outcome = store.apply_plan_submission(&submission)?;
        match outcome {
            PlanApplyOutcome::Rejected {
                reason,
                latest_versions,
            } => {
                assert!(
                    reason.contains("does not satisfy constraints"),
                    "unexpected rejection reason: {reason}"
                );
                let latest_job_epoch = latest_versions
                    .get(&Store::scope_vector_key("job_config", &job_config_id))
                    .copied()
                    .unwrap_or_default();
                assert_eq!(latest_job_epoch, expected_job_epoch);
            }
            PlanApplyOutcome::Applied(_) => {
                bail!("expected constraint-based rejection");
            }
        }

        Ok(())
    }

    #[test]
    fn task_statuses_initialize_and_update() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "status-demo".into(),
            tasks: vec![
                TaskConfig {
                    lifecycle: TaskLifecycle::Batch,
                    ..TaskConfig::new("prep", "exec")
                },
                TaskConfig {
                    lifecycle: TaskLifecycle::Service,
                    ..TaskConfig::new("api", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config(&job.name)?.id();

        let statuses = store.task_statuses_for_job_config(&job_config_id)?;
        assert!(statuses
            .values()
            .all(|status| *status == TaskStatusState::Pending));

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        store.update_task_statuses_for_allocations(&outcome.allocations)?;

        let statuses = store.task_statuses_for_job_config(&job_config_id)?;
        let prep_id = task_id_for(&store, "prep", &job.name)?;
        let api_id = task_id_for(&store, "api", &job.name)?;
        // node-a advertises no protocol version, so it is a pre-exit-report
        // worker and its batch task keeps the legacy optimistic Success.
        assert_eq!(statuses.get(&prep_id), Some(&TaskStatusState::Success));
        assert_eq!(statuses.get(&api_id), Some(&TaskStatusState::Running));

        Ok(())
    }

    /// On a worker that reports exits, a batch task is `Running` until the
    /// process actually exits — that is what makes `condition: "success"`
    /// mean "exited 0" instead of "was placed".
    #[test]
    fn batch_task_status_waits_for_a_real_exit_on_supervising_workers() -> R<()> {
        let store = setup_store()?;
        let mut node = make_node_msg("node-a", "ready");
        node.dynamic_attrs = json!({ "protocolVersion": 2 }).to_string();
        store.insert_node(node)?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "batch-exit-demo".into(),
            tasks: vec![TaskConfig {
                lifecycle: TaskLifecycle::Batch,
                ..TaskConfig::new("prep", "exec")
            }],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let prep_id = task_id_for(&store, "prep", &job.name)?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        store.update_task_statuses_for_allocations(&outcome.allocations)?;

        let statuses = store.task_statuses_for_job_config(&job_config_id)?;
        assert_eq!(
            statuses.get(&prep_id),
            Some(&TaskStatusState::Running),
            "a placed batch task has not succeeded yet"
        );

        // The worker reports the process exited 0.
        let allocation_id = outcome.allocations[0].id;
        let completed = store
            .complete_allocation(allocation_id, true)?
            .expect("allocation exists");
        assert_eq!(completed.status, "complete");
        assert_eq!(
            store
                .task_statuses_for_job_config(&job_config_id)?
                .get(&prep_id),
            Some(&TaskStatusState::Success)
        );
        // The row survives: deleting it would leave the desired count unmet
        // and re-run the batch task immediately.
        assert!(store.get_allocation_by_id(allocation_id)?.is_some());

        // A non-zero exit fails the task instead.
        store.complete_allocation(allocation_id, false)?;
        assert_eq!(
            store
                .task_statuses_for_job_config(&job_config_id)?
                .get(&prep_id),
            Some(&TaskStatusState::Failed)
        );
        assert_eq!(
            store
                .get_allocation_by_id(allocation_id)?
                .expect("allocation exists")
                .status,
            "failed"
        );

        Ok(())
    }

    #[test]
    fn artifact_staging_transitions_emit_events_once() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "artifact-events".into(),
            tasks: vec![TaskConfig::new("web", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let task_id = task_id_for(&store, "web", &job.name)?;
        let allocation = store.place_alloc("node-a", task_id)?;

        let artifact_events = |store: &Store| -> R<Vec<SchedulerEventRecord>> {
            store.list_scheduler_events(&SchedulerEventFilter {
                kind: Some("artifact".into()),
                ..Default::default()
            })
        };

        let snapshot = |states: &str, status: &str| AllocationHealth {
            allocation_id: allocation.id,
            healthy: false,
            status: status.into(),
            artifact_states: Some(states.into()),
        };
        let staging =
            r#"[{"name":"tool","source":"https://example.com/t.tar.gz","state":"fetching"}]"#;
        let staged = r#"[{"name":"tool","source":"https://example.com/t.tar.gz","state":"staged","digest":"sha256:abc","bytesFetched":10}]"#;

        // First staging snapshot announces the run.
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[snapshot(staging, "staging artifacts")],
        )?;
        let events = artifact_events(&store)?;
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0].kind, "artifact.staging");
        assert_eq!(events[0].allocation_id, Some(allocation.id));
        assert_eq!(events[0].job_id.as_deref(), Some("artifact-events"));
        assert_eq!(events[0].node_id.as_deref(), Some("node-a"));

        // Repeated identical heartbeats emit nothing new.
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[snapshot(staging, "staging artifacts")],
        )?;
        assert_eq!(artifact_events(&store)?.len(), 1);

        // Completion emits artifact.staged exactly once.
        store.upsert_allocation_health_snapshots("node-a", &[snapshot(staged, "starting")])?;
        store.upsert_allocation_health_snapshots("node-a", &[snapshot(staged, "healthy")])?;
        let events = artifact_events(&store)?;
        assert_eq!(events.len(), 2, "{events:?}");
        assert_eq!(events[1].kind, "artifact.staged");
        assert!(events[1].metadata_json.contains("sha256:abc"));

        // Snapshots without artifact rows never touch the artifact kinds.
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[AllocationHealth {
                allocation_id: allocation.id,
                healthy: true,
                status: "healthy".into(),
                artifact_states: None,
            }],
        )?;
        assert_eq!(artifact_events(&store)?.len(), 2);
        Ok(())
    }

    /// Seed a one-task job and place an allocation for it on `node_id`.
    fn seed_placed_allocation(store: &Store, job_name: &str, node_id: &str) -> R<Allocation> {
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: job_name.into(),
            tasks: vec![TaskConfig::new("web", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let task_id = task_id_for(store, "web", &job.name)?;
        store.place_alloc(node_id, task_id)
    }

    /// Scale-in is a platform-initiated removal like any other, so it must
    /// record WHY (preemption plan § Phase 1a) — and with its own cause, not
    /// `update`: no new job config version exists, which is the entire point
    /// of the count-override surface (docs/autoscaling.md).
    #[test]
    fn scale_in_records_its_own_stop_cause() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "scale-cause".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                ..TaskConfig::new("web", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config("scale-cause")?.id();
        let task_id = task_id_for(&store, "web", "scale-cause")?;
        let first = store.place_alloc("node-a", task_id)?;
        let second = store.place_alloc("node-a", task_id)?;
        assert!(store.stop_notices_for_node("node-a")?.is_empty());

        // Scale 2 -> 1: the newest allocation is retired.
        store.set_task_scale_override(
            "scale-cause",
            "default",
            "web",
            1,
            2,
            "operator",
            "test",
            "t",
        )?;
        let retired = store.retire_surplus_allocations(&job_config_id)?;
        assert_eq!(retired.len(), 1);
        assert_eq!(retired[0].id, second.id.max(first.id));

        assert_eq!(
            store
                .stop_notices_for_node("node-a")?
                .into_iter()
                .map(|notice| (notice.allocation_id, notice.cause))
                .collect::<Vec<_>>(),
            vec![(retired[0].id, StopCause::ScaleIn)],
            "a scaled-in allocation reports scale_in, not update"
        );
        Ok(())
    }

    #[test]
    fn stop_notice_records_the_cause_and_serves_it_to_the_owning_node() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;
        let allocation = seed_placed_allocation(&store, "notice-demo", "node-a")?;

        // Nothing removed yet: no notices.
        assert!(store.stop_notices_for_node("node-a")?.is_empty());

        store.remove_allocation_by_id_because(allocation.id, StopCause::Drain)?;

        let notices = store
            .stop_notices_for_node("node-a")?
            .into_iter()
            .map(|notice| (notice.allocation_id, notice.cause))
            .collect::<Vec<_>>();
        assert_eq!(
            notices,
            vec![(allocation.id, StopCause::Drain)],
            "the owning node learns why its allocation went away"
        );
        // Notices are addressed to one node; a different node must not see
        // another node's stops.
        assert!(
            store.stop_notices_for_node("node-b")?.is_empty(),
            "notices are scoped to the node that owned the allocation"
        );
        Ok(())
    }

    #[test]
    fn stop_notice_is_absent_when_the_cause_is_unknown() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        let allocation = seed_placed_allocation(&store, "unknown-demo", "node-a")?;

        // The worker-failure-report path: the task died on its own, so there
        // is no platform stop to explain. Recording `unknown` would be noise
        // — the worker defaults to it anyway.
        store.remove_allocation_by_id(allocation.id)?;

        assert!(
            store.stop_notices_for_node("node-a")?.is_empty(),
            "an unknown cause records nothing"
        );
        Ok(())
    }

    #[test]
    fn stop_notices_expire_and_are_pruned_by_later_removals() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        // Both allocations are placed before either is removed: allocation
        // ids are plain rowids and get REUSED after a delete, so seeding them
        // one at a time would hand both the same id and quietly turn this
        // into a test of `INSERT OR REPLACE`.
        let first = seed_placed_allocation(&store, "expiry-demo-1", "node-a")?;
        let second = seed_placed_allocation(&store, "expiry-demo-2", "node-a")?;
        assert_ne!(first.id, second.id);

        store.remove_allocation_by_id_because(first.id, StopCause::Stop)?;
        assert_eq!(store.stop_notices_for_node("node-a")?.len(), 1);

        // Backdate the notice past the retention window. The read path must
        // filter it even before anything prunes it, so a stale cause can
        // never be served.
        {
            let conn = store.write_connection()?;
            conn.execute(
                "UPDATE allocation_stop_notice SET created_at_unix = ?1 WHERE allocation_id = ?2;",
                params![
                    OffsetDateTime::now_utc().unix_timestamp() - STOP_NOTICE_RETENTION_SECS - 1,
                    first.id
                ],
            )?;
        }
        assert!(
            store.stop_notices_for_node("node-a")?.is_empty(),
            "an expired notice is filtered on read"
        );

        // The next removal's transaction is what actually deletes it: expiry
        // rides scheduler-lane writes, never the heartbeat read path.
        store.remove_allocation_by_id_because(second.id, StopCause::Stop)?;

        let remaining: i64 = store.read_connection()?.query_row(
            "SELECT COUNT(*) FROM allocation_stop_notice WHERE allocation_id = ?1;",
            params![first.id],
            |row| row.get(0),
        )?;
        assert_eq!(remaining, 0, "the expired row was pruned by the next write");
        assert_eq!(
            store
                .stop_notices_for_node("node-a")?
                .into_iter()
                .map(|notice| (notice.allocation_id, notice.cause))
                .collect::<Vec<_>>(),
            vec![(second.id, StopCause::Stop)],
            "the live notice survives the prune"
        );
        Ok(())
    }

    #[test]
    fn job_stop_and_drain_record_distinguishable_causes() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        let allocation = seed_placed_allocation(&store, "cause-demo", "node-a")?;

        // `job stop` — the whole point of Phase 1a is that this is no longer
        // indistinguishable from a drain or an update on the wire.
        store.remove_allocs_for_job("cause-demo")?;
        assert_eq!(
            store
                .stop_notices_for_node("node-a")?
                .into_iter()
                .map(|notice| (notice.allocation_id, notice.cause))
                .collect::<Vec<_>>(),
            vec![(allocation.id, StopCause::Stop)]
        );

        // And the event journal carries it too.
        let events = store.list_scheduler_events(&SchedulerEventFilter::default())?;
        let removed = events
            .iter()
            .find(|event| event.kind == "allocation.removed")
            .context("a removal event should have been recorded")?;
        let meta: Value = serde_json::from_str(&removed.metadata_json)?;
        assert_eq!(meta["cause"], json!("stop"));
        Ok(())
    }

    #[test]
    fn allocation_health_snapshots_upsert_and_cleanup() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "health-demo".into(),
            tasks: vec![TaskConfig::new("web", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let task_id = task_id_for(&store, "web", &job.name)?;
        let allocation = store.place_alloc("node-a", task_id)?;

        store.upsert_allocation_health_snapshots(
            "node-a",
            &[AllocationHealth {
                allocation_id: allocation.id,
                healthy: false,
                status: "starting".into(),
                artifact_states: None,
            }],
        )?;
        store.upsert_allocation_health_snapshots(
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
            .context("missing allocation health snapshot")?;
        assert!(entry.healthy);
        assert_eq!(entry.status, "healthy");

        store.upsert_allocation_health_snapshots(
            "node-b",
            &[AllocationHealth {
                allocation_id: allocation.id,
                healthy: false,
                status: "spoofed".into(),
                artifact_states: None,
            }],
        )?;
        let health = store.allocation_health_for_allocations(&[allocation.id])?;
        let entry = health
            .get(&allocation.id)
            .context("missing allocation health snapshot after non-owner write")?;
        assert_eq!(entry.node_id, "node-a");
        assert!(entry.healthy);
        assert_eq!(entry.status, "healthy");

        let removed = store.remove_allocation_by_id(allocation.id)?;
        assert!(removed.is_some());
        // The allocation_health FK cascade is gone (C-2): the row survives
        // the scheduler-lane removal as a harmless orphan until the
        // status-lane sweep collects it — on its SECOND observation (the
        // two-sweep grace, adversarial finding F2).
        assert_eq!(
            store.prune_allocation_health_orphans()?,
            0,
            "the first sweep must only observe, never delete"
        );
        assert_eq!(store.prune_allocation_health_orphans()?, 1);
        let health = store.allocation_health_for_allocations(&[allocation.id])?;
        assert!(health.is_empty());
        assert_eq!(
            store.prune_allocation_health_orphans()?,
            0,
            "sweep is idempotent"
        );

        Ok(())
    }

    #[test]
    fn scheduler_events_emit_and_list_with_filters() -> R<()> {
        let store = setup_store()?;
        assert_eq!(store.latest_scheduler_event_id()?, 0);

        store.emit_event(
            EventDraft::info("eval.completed", "Eval 1 (job_submit) completed")
                .job("demo")
                .eval("1"),
        );
        store.emit_event(EventDraft::warn("node.lost", "Node node-a marked lost").node("node-a"));
        store.emit_event(
            EventDraft::info("allocation.placed", "Placed 2 allocation(s) for job demo")
                .job("demo")
                .meta(json!({ "count": 2 })),
        );

        let all = store.list_scheduler_events(&SchedulerEventFilter::default())?;
        assert_eq!(all.len(), 3);
        assert!(all.windows(2).all(|pair| pair[0].id < pair[1].id));
        assert_eq!(store.latest_scheduler_event_id()?, all[2].id);

        let demo_events = store.list_scheduler_events(&SchedulerEventFilter {
            job_id: Some("demo".into()),
            ..Default::default()
        })?;
        assert_eq!(demo_events.len(), 2);

        let eval_events = store.list_scheduler_events(&SchedulerEventFilter {
            kind: Some("eval".into()),
            ..Default::default()
        })?;
        assert_eq!(eval_events.len(), 1);
        assert_eq!(eval_events[0].kind, "eval.completed");
        assert_eq!(eval_events[0].eval_id.as_deref(), Some("1"));

        let warn_events = store.list_scheduler_events(&SchedulerEventFilter {
            level: Some("warn".into()),
            ..Default::default()
        })?;
        assert_eq!(warn_events.len(), 1);
        assert_eq!(warn_events[0].node_id.as_deref(), Some("node-a"));

        let after = store.list_scheduler_events(&SchedulerEventFilter {
            after_id: Some(all[0].id),
            ..Default::default()
        })?;
        assert_eq!(after.len(), 2);
        assert_eq!(after[0].id, all[1].id);

        let placed = &all[2];
        assert_eq!(placed.metadata_json, json!({ "count": 2 }).to_string());

        Ok(())
    }

    #[test]
    fn event_recorder_emits_expected_kinds_and_skips_empty() -> R<()> {
        let store = setup_store()?;

        // Empty inputs are no-ops, so callers don't need their own guards.
        store
            .events()
            .allocations_removed_from_node("node-a", &[], StopCause::NodeLost);
        store
            .events()
            .allocations_removed_for_job("demo", 0, StopCause::Stop);
        assert!(drafts::allocations_placed("cfg-1", &[]).is_none());
        assert!(drafts::job_submit_outcome(
            "demo",
            &JobSubmitOutcome::Unchanged {
                job_config_id: "cfg-1".into(),
            },
            1,
            1,
        )
        .is_none());
        assert!(store
            .list_scheduler_events(&SchedulerEventFilter::default())?
            .is_empty());

        let submitted = drafts::job_submit_outcome(
            "demo",
            &JobSubmitOutcome::Created {
                job_config_id: "cfg-1".into(),
            },
            1,
            2,
        )
        .context("created outcome should produce an event")?;
        store.emit_event(submitted);
        store
            .events()
            .allocation_failed(7, "node-a", "demo", "cfg-1", 3, "task crashed");
        store.events().node_lost_after_missed_heartbeats("node-a");

        let events = store.list_scheduler_events(&SchedulerEventFilter::default())?;
        let kinds: Vec<&str> = events.iter().map(|event| event.kind.as_str()).collect();
        assert_eq!(
            kinds,
            vec!["job.submitted", "allocation.failed", "node.lost"]
        );

        let failed = &events[1];
        assert_eq!(failed.level, "warn");
        assert_eq!(failed.job_id.as_deref(), Some("demo"));
        assert_eq!(failed.node_id.as_deref(), Some("node-a"));
        assert_eq!(failed.allocation_id, Some(7));
        assert!(failed.message.contains("task crashed"));

        let lost = &events[2];
        assert_eq!(
            lost.metadata_json,
            json!({ "reason": "heartbeat" }).to_string()
        );

        Ok(())
    }

    #[test]
    fn complete_eval_with_event_records_event() -> R<()> {
        let store = setup_store()?;
        let eval = store.enqueue_eval("job_submit", None, None, None, None)?;
        let claimed = store.claim_eval()?.context("eval should be claimable")?;
        assert_eq!(claimed.id, eval.id);

        store.complete_eval_with_event(
            &eval.id,
            true,
            drafts::eval_completed(&claimed, "done", std::time::Duration::from_millis(5)),
        )?;

        assert!(store.get_evals()?.is_empty());
        let events = store.list_scheduler_events(&SchedulerEventFilter {
            kind: Some("eval.completed".into()),
            ..Default::default()
        })?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].eval_id.as_deref(), Some(eval.id.as_str()));
        assert!(events[0].message.contains("done"));

        Ok(())
    }

    #[test]
    fn scheduler_events_list_without_cursor_returns_newest_window() -> R<()> {
        let store = setup_store()?;
        for index in 0..10 {
            store.emit_event(EventDraft::info("eval.enqueued", format!("event {index}")));
        }

        let window = store.list_scheduler_events(&SchedulerEventFilter {
            limit: Some(3),
            ..Default::default()
        })?;
        assert_eq!(window.len(), 3);
        // Newest three, still in ascending order.
        assert_eq!(window[0].message, "event 7");
        assert_eq!(window[2].message, "event 9");

        Ok(())
    }

    #[tokio::test]
    async fn scheduler_events_broadcast_to_subscribers() -> R<()> {
        let store = setup_store()?;
        let mut subscriber = store.subscribe_events();

        store.emit_event(EventDraft::info("job.submitted", "Job demo submitted").job("demo"));

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), subscriber.recv())
            .await
            .context("timed out waiting for broadcast event")??;
        assert_eq!(event.kind, "job.submitted");
        assert_eq!(event.job_id.as_deref(), Some("demo"));
        assert!(event.id > 0);

        Ok(())
    }

    #[test]
    fn store_mutations_emit_scheduler_events() -> R<()> {
        let store = setup_store()?;
        store.insert_node(NodeMsg::from(make_node("events-node")))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "events-job".into(),
            tasks: vec![TaskConfig {
                resources: json!({ "memory": 64 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[])?;
        store.enqueue_eval("job_submit", Some(&job.name), None, None, None)?;
        store.stop_job(&job.name)?;
        store.mark_node_lost("events-node")?;

        let events = store.list_scheduler_events(&SchedulerEventFilter::default())?;
        let kinds: Vec<&str> = events.iter().map(|event| event.kind.as_str()).collect();
        assert!(kinds.contains(&"node.registered"));
        assert!(kinds.contains(&"job.submitted"));
        assert!(kinds.contains(&"eval.enqueued"));
        assert!(kinds.contains(&"job.stopped"));
        assert!(kinds.contains(&"node.lost"));

        let submit = events
            .iter()
            .find(|event| event.kind == "job.submitted")
            .context("missing job.submitted event")?;
        assert_eq!(submit.job_id.as_deref(), Some("events-job"));
        assert_eq!(submit.level, "info");

        Ok(())
    }

    #[test]
    fn rollout_events_append_list_and_prune_with_rollout() -> R<()> {
        let store = setup_store()?;

        let v1 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "rollout-events".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v1, &[])?;
        let v1_id = store.get_job_config(&v1.name)?.id();

        let v2 = JobConfig {
            priority: Default::default(),
            rollout: Some(RolloutConfig {
                replacement: Some(crate::structs::RolloutReplacement::CreateFirst),
                ..RolloutConfig::default()
            }),
            rollout_by_group: Default::default(),
            name: "rollout-events".into(),
            tasks: vec![TaskConfig {
                group: "web".into(),
                config: json!({ "version": 2 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v2, &[])?;
        let v2_id = store.get_job_config(&v2.name)?.id();

        let rollout = store.create_rollout(
            &v2.name,
            Some(&v1_id),
            &v2_id,
            "running",
            Some("default"),
            Some("auto"),
            Some("createFirst"),
            Some(false),
            "{}",
        )?;

        let first = store.append_rollout_event(
            rollout.id,
            &rollout.job_id,
            None,
            "info",
            "Rollout created",
            None,
        )?;
        let second = store.append_rollout_event(
            rollout.id,
            &rollout.job_id,
            Some("web"),
            "info",
            "Group web advanced to step 1",
            Some(r#"{"step":1}"#),
        )?;

        let events = store.list_rollout_events(rollout.id, None)?;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, first.id);
        assert_eq!(events[1].id, second.id);
        assert_eq!(events[1].group_name.as_deref(), Some("web"));

        let limited = store.list_rollout_events(rollout.id, Some(1))?;
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].id, second.id);

        let conn = store.write_connection()?;
        conn.execute(
            "UPDATE rollout SET completed_at = '1970-01-01T00:00:00Z', status = 'completed' WHERE id = ?1;",
            [rollout.id],
        )?;
        drop(conn);

        let pruned = store.prune_rollouts()?;
        assert_eq!(pruned, 1);
        assert!(store.get_rollout(rollout.id)?.is_none());
        assert!(store.list_rollout_events(rollout.id, None)?.is_empty());

        Ok(())
    }

    #[test]
    fn new_node_reconciles_completed_incomplete_rollouts() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;

        let v1 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "reconcile-incomplete".into(),
            tasks: vec![TaskConfig::new("web", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &v1,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let v1_id = store.get_job_config(&v1.name)?.id();

        let v2 = JobConfig {
            priority: Default::default(),
            rollout: Some(RolloutConfig::default()),
            rollout_by_group: Default::default(),
            name: "reconcile-incomplete".into(),
            tasks: vec![TaskConfig {
                config: json!({"version": 2}),
                ..TaskConfig::new("web", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(
            &v2,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        store.remove_allocs_for_job_config(&v2_id)?;

        let rollout = store.create_rollout(
            "reconcile-incomplete",
            Some(&v1_id),
            &v2_id,
            "completed_incomplete",
            Some("default"),
            Some("auto"),
            Some("createFirst"),
            Some(false),
            "{}",
        )?;
        store.append_rollout_event(
            rollout.id,
            &rollout.job_id,
            None,
            "warn",
            "Rollout completed incompletely after capacity timeout",
            None,
        )?;

        assert!(store.allocs_for_job_config(&v2_id)?.is_empty());
        store.reschedule_allocations_for_new_node()?;
        assert_eq!(store.allocs_for_job_config(&v2_id)?.len(), 1);

        Ok(())
    }

    #[test]
    fn incomplete_rollout_reconcile_attempts_placements() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        store.insert_node(make_node_msg("node-b", "ready"))?;

        let v1 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "reconcile-incomplete-loop".into(),
            tasks: vec![TaskConfig::new("web", "noop")],
            version: None,
        };
        store.handle_job_submit(
            &v1,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let v1_id = store.get_job_config(&v1.name)?.id();

        let v2 = JobConfig {
            priority: Default::default(),
            rollout: Some(RolloutConfig::default()),
            rollout_by_group: Default::default(),
            name: "reconcile-incomplete-loop".into(),
            tasks: vec![TaskConfig {
                config: json!({"version": 2}),
                ..TaskConfig::new("web", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(
            &v2,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        store.remove_allocs_for_job_config(&v2_id)?;

        let _ = store.create_rollout(
            "reconcile-incomplete-loop",
            Some(&v1_id),
            &v2_id,
            "completed_incomplete",
            Some("default"),
            Some("auto"),
            Some("createFirst"),
            Some(false),
            "{}",
        )?;

        assert!(store.allocs_for_job_config(&v2_id)?.is_empty());
        let reconciled = store.reconcile_incomplete_rollouts()?;
        assert_eq!(reconciled, 1);
        assert_eq!(store.allocs_for_job_config(&v2_id)?.len(), 1);

        Ok(())
    }

    //
    // INSTANCES (stateful workloads)
    //

    fn stateful_slot_task(name: &str, count: u32, with_storage: bool) -> TaskConfig {
        TaskConfig {
            count: TaskCount::new(count),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: with_storage.then(|| crate::structs::StatefulStorageConfig {
                    driver: None,
                    mode: None,
                    params: None,
                    path: "/var/lib/data".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                }),
            }),
            ..TaskConfig::new(name, "noop")
        }
    }

    fn submit_job(store: &Store, name: &str, tasks: Vec<TaskConfig>) -> R<String> {
        let config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks,
            version: None,
        };
        store.handle_job_submit(
            &config,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        Ok(store.get_job_config(name)?.id())
    }

    #[test]
    fn stateful_task_config_round_trips_through_persistence() -> R<()> {
        let store = setup_store()?;
        let job_config_id = submit_job(
            &store,
            "stateful-rt",
            vec![stateful_slot_task("db", 2, true)],
        )?;

        let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
        let (_, task) = tasks.first().context("missing task")?;
        let stateful = task.stateful.as_ref().context("stateful must persist")?;
        assert_eq!(stateful.mode, InstanceMode::Slot);
        assert_eq!(
            stateful.storage.as_ref().map(|s| s.path.as_str()),
            Some("/var/lib/data")
        );
        Ok(())
    }

    #[test]
    fn stateless_task_config_round_trips_without_stateful() -> R<()> {
        let store = setup_store()?;
        let job_config_id = submit_job(&store, "stateless-rt", vec![task("web")])?;
        let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
        assert!(tasks[0].1.stateful.is_none());
        Ok(())
    }

    #[test]
    fn place_alloc_assigns_lowest_free_slot_and_activates_instance() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "slot-place";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 3, true)])?;
        let task_id = task_id_for(&store, "db", job_id)?;

        let first = store.place_alloc("node-a", task_id)?;
        let second = store.place_alloc("node-a", task_id)?;
        let third = store.place_alloc("node-a", task_id)?;

        assert_eq!(first.instance_id.as_deref(), Some("slot-place-default-0"));
        assert_eq!(second.instance_id.as_deref(), Some("slot-place-default-1"));
        assert_eq!(third.instance_id.as_deref(), Some("slot-place-default-2"));

        let instances = store.list_instances(job_id)?;
        assert_eq!(instances.len(), 3);
        for (slot, instance) in instances.iter().enumerate() {
            assert_eq!(instance.mode, InstanceMode::Slot);
            assert_eq!(instance.state, InstanceState::Active);
            assert_eq!(instance.slot_index, Some(slot as u32));
            assert!(
                instance
                    .storage_handle
                    .as_deref()
                    .is_some_and(|handle| handle.starts_with("local-dir/")),
                "storage handle minted on first placement"
            );
        }
        assert_eq!(instances[0].current_alloc_id, Some(first.id));
        assert_eq!(instances[2].current_alloc_id, Some(third.id));

        // A fourth placement would exceed the slot pool and must fail loudly.
        assert!(store.place_alloc("node-a", task_id).is_err());
        Ok(())
    }

    #[test]
    fn removed_allocation_reuses_slot_instance_and_storage_handle() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "slot-reuse";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 2, true)])?;
        let task_id = task_id_for(&store, "db", job_id)?;

        let slot0 = store.place_alloc("node-a", task_id)?;
        let slot1 = store.place_alloc("node-a", task_id)?;
        let slot1_instance = store
            .get_instance(slot1.instance_id.as_deref().context("instance id")?)?
            .context("instance row")?;
        let original_handle = slot1_instance.storage_handle.clone();
        assert!(original_handle.is_some());

        // Kill slot 1's allocation: instance transitions to rescheduling and
        // keeps its handle.
        store.remove_allocation_by_id(slot1.id)?;
        let rescheduling = store
            .get_instance(&slot1_instance.instance_id)?
            .context("instance retained")?;
        assert_eq!(rescheduling.state, InstanceState::Rescheduling);
        assert_eq!(rescheduling.current_alloc_id, None);
        assert_eq!(rescheduling.storage_handle, original_handle);

        // The replacement reuses slot 1's instance id and storage handle.
        let replacement = store.place_alloc("node-a", task_id)?;
        assert_eq!(
            replacement.instance_id.as_deref(),
            Some(slot1_instance.instance_id.as_str())
        );
        let reactivated = store
            .get_instance(&slot1_instance.instance_id)?
            .context("instance reactivated")?;
        assert_eq!(reactivated.state, InstanceState::Active);
        assert_eq!(reactivated.current_alloc_id, Some(replacement.id));
        assert_eq!(reactivated.storage_handle, original_handle);

        // Slot 0 was never disturbed.
        let slot0_instance = store
            .get_instance(slot0.instance_id.as_deref().context("instance id")?)?
            .context("slot 0 instance")?;
        assert_eq!(slot0_instance.state, InstanceState::Active);
        assert_eq!(slot0_instance.current_alloc_id, Some(slot0.id));
        Ok(())
    }

    #[test]
    fn slot_binding_prefers_slot_with_no_live_allocation_in_job() -> R<()> {
        // Sequential-rollout shape: v1 holds slots 0..2; v2 must bind the
        // slot whose old allocation was just retired (slot 2), not the
        // lowest slot free in v2's own scope (slot 0, still live under v1).
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "slot-seq-bind";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 3, true)])?;
        let v1_task = task_id_for(&store, "db", job_id)?;
        let mut v1_allocs = Vec::new();
        for _ in 0..3 {
            v1_allocs.push(store.place_alloc("node-a", v1_task)?);
        }
        let slot2_handle = store
            .get_instance(&format!("{job_id}-default-2"))?
            .context("slot 2 instance")?
            .storage_handle
            .clone();

        let v2_config_id = submit_job(
            &store,
            job_id,
            vec![TaskConfig {
                config: json!({ "version": 2 }),
                ..stateful_slot_task("db", 3, true)
            }],
        )?;
        let v2_task = task_id_for_config(&store, "db", &v2_config_id)?;

        // Retire slot 2's old allocation, then place a v2 allocation: it
        // must re-bind slot 2's instance (same id and storage handle).
        store.remove_allocation_by_id(v1_allocs[2].id)?;
        let replacement = store.place_alloc("node-a", v2_task)?;
        assert_eq!(
            replacement.instance_id.as_deref(),
            Some(format!("{job_id}-default-2").as_str()),
            "replacement must bind the freed slot, not slot 0"
        );
        let rebound = store
            .get_instance(&format!("{job_id}-default-2"))?
            .context("slot 2 instance after rebind")?;
        assert_eq!(rebound.storage_handle, slot2_handle);

        // Fallback parity with the fresh-plan-then-remove flow: with no
        // job-wide-free slot left, per-config scoping still applies and the
        // next v2 placement takes the lowest slot free in v2's scope.
        let takeover = store.place_alloc("node-a", v2_task)?;
        assert_eq!(
            takeover.instance_id.as_deref(),
            Some(format!("{job_id}-default-0").as_str())
        );
        Ok(())
    }

    #[test]
    fn slot_indices_and_other_config_counts_round_trip() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "slot-helpers";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 2, false)])?;
        let v1_task = task_id_for(&store, "db", job_id)?;
        store.place_alloc("node-a", v1_task)?;
        store.place_alloc("node-a", v1_task)?;

        let indices = store.slot_indices_for_job_group(job_id, "default")?;
        assert_eq!(indices.len(), 2);
        assert_eq!(indices.get(&format!("{job_id}-default-0")), Some(&0));
        assert_eq!(indices.get(&format!("{job_id}-default-1")), Some(&1));
        assert!(store
            .slot_indices_for_job_group(job_id, "other-group")?
            .is_empty());

        // Both v1 allocations count as "other config" from v2's viewpoint,
        // and as zero from v1's own viewpoint.
        let v1_config_id = format!("{job_id}-1");
        let v2_config_id = submit_job(
            &store,
            job_id,
            vec![TaskConfig {
                config: json!({ "version": 2 }),
                ..stateful_slot_task("db", 2, false)
            }],
        )?;
        assert_eq!(
            store.count_slot_allocations_excluding_config(job_id, "default", &v2_config_id)?,
            2
        );
        assert_eq!(
            store.count_slot_allocations_excluding_config(job_id, "default", &v1_config_id)?,
            0
        );
        Ok(())
    }

    #[test]
    fn scale_in_terminates_high_slots_and_scale_out_reattaches_them() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "slot-scale";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 3, true)])?;
        let v1_task = task_id_for(&store, "db", job_id)?;
        for _ in 0..3 {
            store.place_alloc("node-a", v1_task)?;
        }
        let handles_before: Vec<Option<String>> = store
            .list_instances(job_id)?
            .into_iter()
            .map(|instance| instance.storage_handle)
            .collect();

        // Scale in to 2: submit v2, place its allocations, then drop v1's
        // (mirrors the job_submit eval flow: plan fresh, then remove previous).
        let v2_config_id = submit_job(&store, job_id, vec![stateful_slot_task("db", 2, true)])?;
        let v1_config_id = format!("{job_id}-1");
        let v2_task = task_id_for_config(&store, "db", &v2_config_id)?;
        store.place_alloc("node-a", v2_task)?;
        store.place_alloc("node-a", v2_task)?;
        store.remove_allocs_for_job_config(&v1_config_id)?;

        let instances = store.list_instances(job_id)?;
        assert_eq!(instances.len(), 3, "terminated rows are retained");
        assert_eq!(instances[0].state, InstanceState::Active);
        assert_eq!(instances[1].state, InstanceState::Active);
        assert_eq!(instances[2].state, InstanceState::Terminated);
        assert_eq!(
            instances[2].storage_handle, handles_before[2],
            "scale-in retains the storage handle by default"
        );
        // Slots 0/1 kept their identity and storage across the config update.
        assert_eq!(instances[0].storage_handle, handles_before[0]);
        assert_eq!(instances[1].storage_handle, handles_before[1]);

        // Scale back out to 3: the same slot id is re-materialized with the
        // retained handle.
        let v3_config_id = submit_job(&store, job_id, vec![stateful_slot_task("db", 3, true)])?;
        let v3_task = task_id_for_config(&store, "db", &v3_config_id)?;
        for _ in 0..3 {
            store.place_alloc("node-a", v3_task)?;
        }
        store.remove_allocs_for_job_config(&v2_config_id)?;

        let instances = store.list_instances(job_id)?;
        assert_eq!(instances.len(), 3);
        assert_eq!(instances[2].instance_id, "slot-scale-default-2");
        assert_eq!(instances[2].state, InstanceState::Active);
        assert_eq!(instances[2].storage_handle, handles_before[2]);
        Ok(())
    }

    #[test]
    fn scale_in_drops_storage_handle_when_retention_disabled() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "slot-noretain";
        let no_retain_task = |count: u32| TaskConfig {
            count: TaskCount::new(count),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    driver: None,
                    mode: None,
                    params: None,
                    path: "/var/lib/data".into(),
                    retain_on_scale_in: Some(false),
                    retain_on_stop: None,
                }),
            }),
            ..TaskConfig::new("db", "noop")
        };

        submit_job(&store, job_id, vec![no_retain_task(2)])?;
        let v1_task = task_id_for(&store, "db", job_id)?;
        store.place_alloc("node-a", v1_task)?;
        store.place_alloc("node-a", v1_task)?;

        let v2_config_id = submit_job(&store, job_id, vec![no_retain_task(1)])?;
        let v2_task = task_id_for_config(&store, "db", &v2_config_id)?;
        store.place_alloc("node-a", v2_task)?;
        store.remove_allocs_for_job_config(&format!("{job_id}-1"))?;

        let instances = store.list_instances(job_id)?;
        assert_eq!(instances[1].state, InstanceState::Terminated);
        assert_eq!(instances[1].storage_handle, None);
        Ok(())
    }

    #[test]
    fn stateless_jobs_create_no_instance_rows() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "stateless-job";
        submit_job(&store, job_id, vec![task("web")])?;
        let task_id = task_id_for(&store, "web", job_id)?;

        let allocation = store.place_alloc("node-a", task_id)?;
        assert_eq!(allocation.instance_id, None);
        assert!(store.list_instances(job_id)?.is_empty());

        store.remove_allocation_by_id(allocation.id)?;
        assert!(store.list_instances(job_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn instance_store_methods_round_trip() -> R<()> {
        let store = setup_store()?;
        let job_id = "inst-crud";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 2, false)])?;

        assert!(store.get_instance("missing")?.is_none());

        let created = store.get_or_create_slot_instance(job_id, "default", "db", 0)?;
        assert_eq!(created.instance_id, "inst-crud-default-0");
        assert_eq!(created.state, InstanceState::Pending);
        assert_eq!(created.slot_index, Some(0));
        assert_eq!(created.storage_handle, None);

        // Idempotent: a second call returns the same row.
        let again = store.get_or_create_slot_instance(job_id, "default", "db", 0)?;
        assert_eq!(again.instance_id, created.instance_id);
        assert_eq!(again.created_at, created.created_at);

        store.set_instance_state(&created.instance_id, InstanceState::Active)?;
        store.set_instance_current_alloc(&created.instance_id, Some(77))?;
        let updated = store
            .get_instance(&created.instance_id)?
            .context("instance exists")?;
        assert_eq!(updated.state, InstanceState::Active);
        assert_eq!(updated.current_alloc_id, Some(77));

        store.set_instance_current_alloc(&created.instance_id, None)?;
        let cleared = store
            .get_instance(&created.instance_id)?
            .context("instance exists")?;
        assert_eq!(cleared.current_alloc_id, None);

        // Upsert overwrites mutable fields and keeps identity.
        let upserted = store.upsert_instance(&Instance {
            storage_handle: Some("local-dir/manual".into()),
            state: InstanceState::Terminated,
            ..cleared
        })?;
        assert_eq!(upserted.state, InstanceState::Terminated);
        assert_eq!(upserted.storage_handle.as_deref(), Some("local-dir/manual"));

        // Errors on unknown ids.
        assert!(store
            .set_instance_state("missing", InstanceState::Active)
            .is_err());
        assert!(store.set_instance_current_alloc("missing", None).is_err());
        Ok(())
    }

    #[test]
    fn stop_instances_for_job_terminates_slot_rows_and_retains_handles() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "inst-stop";
        submit_job(&store, job_id, vec![stateful_slot_task("db", 2, true)])?;
        let task_id = task_id_for(&store, "db", job_id)?;
        store.place_alloc("node-a", task_id)?;
        store.place_alloc("node-a", task_id)?;

        let handles: Vec<Option<String>> = store
            .list_instances(job_id)?
            .into_iter()
            .map(|instance| instance.storage_handle)
            .collect();

        // Mirror the job_stop eval: allocations are removed first, then the
        // instances are settled. Default retainOnStop (absent = true) keeps
        // every slot handle, like K8s `whenDeleted: Retain`.
        store.remove_allocs_for_job(job_id)?;
        assert_eq!(store.stop_instances_for_job(job_id)?, 2);
        let instances = store.list_instances(job_id)?;
        assert_eq!(instances.len(), 2);
        for (instance, handle) in instances.iter().zip(&handles) {
            assert_eq!(instance.state, InstanceState::Terminated);
            assert_eq!(instance.current_alloc_id, None);
            assert_eq!(&instance.storage_handle, handle);
        }

        // Idempotent.
        assert_eq!(store.stop_instances_for_job(job_id)?, 0);

        // Resubmitting the job re-materializes the SAME ordinal identities
        // with the SAME retained handles.
        let v2_config_id = submit_job(&store, job_id, vec![stateful_slot_task("db", 2, true)])?;
        let v2_task = task_id_for_config(&store, "db", &v2_config_id)?;
        store.place_alloc("node-a", v2_task)?;
        store.place_alloc("node-a", v2_task)?;
        let revived = store.list_instances(job_id)?;
        assert_eq!(revived.len(), 2);
        assert_eq!(revived[0].instance_id, "inst-stop-default-0");
        assert_eq!(revived[1].instance_id, "inst-stop-default-1");
        for (instance, handle) in revived.iter().zip(&handles) {
            assert_eq!(instance.state, InstanceState::Active);
            assert_eq!(
                &instance.storage_handle, handle,
                "re-run reattaches the retained handle"
            );
        }
        Ok(())
    }

    #[test]
    fn job_stop_drops_slot_handles_when_retain_on_stop_disabled() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "stop-noretain";
        let no_stop_retain_task = || TaskConfig {
            count: TaskCount::new(2),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    driver: None,
                    mode: None,
                    params: None,
                    path: "/var/lib/data".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: Some(false),
                }),
            }),
            ..TaskConfig::new("db", "noop")
        };

        submit_job(&store, job_id, vec![no_stop_retain_task()])?;
        let task_id = task_id_for(&store, "db", job_id)?;
        store.place_alloc("node-a", task_id)?;
        store.place_alloc("node-a", task_id)?;
        let original_handles: Vec<Option<String>> = store
            .list_instances(job_id)?
            .into_iter()
            .map(|instance| instance.storage_handle)
            .collect();
        assert!(original_handles.iter().all(Option::is_some));

        // Job stop (the K8s `whenDeleted: Delete` analog): rows are retained
        // — identity comes back — but the data binding is dropped.
        store.remove_allocs_for_job(job_id)?;
        assert_eq!(store.stop_instances_for_job(job_id)?, 2);
        let instances = store.list_instances(job_id)?;
        assert_eq!(instances.len(), 2, "rows retained despite handle drop");
        for instance in &instances {
            assert_eq!(instance.state, InstanceState::Terminated);
            assert_eq!(instance.current_alloc_id, None);
            assert_eq!(
                instance.storage_handle, None,
                "retainOnStop: false drops the handle on job stop"
            );
        }

        // Idempotent.
        assert_eq!(store.stop_instances_for_job(job_id)?, 0);

        // Resubmit: the same ordinal ids come back, but with FRESH handles —
        // the identity survives the stop, the data binding does not.
        let v2_config_id = submit_job(&store, job_id, vec![no_stop_retain_task()])?;
        let v2_task = task_id_for_config(&store, "db", &v2_config_id)?;
        store.place_alloc("node-a", v2_task)?;
        store.place_alloc("node-a", v2_task)?;
        let revived = store.list_instances(job_id)?;
        assert_eq!(revived.len(), 2);
        assert_eq!(revived[0].instance_id, "stop-noretain-default-0");
        assert_eq!(revived[1].instance_id, "stop-noretain-default-1");
        for (instance, original) in revived.iter().zip(&original_handles) {
            assert_eq!(instance.state, InstanceState::Active);
            assert!(instance.storage_handle.is_some(), "fresh handle minted");
            assert_ne!(&instance.storage_handle, original);
        }
        Ok(())
    }

    #[test]
    fn job_stop_drops_handles_only_for_tasks_with_retain_on_stop_disabled() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "stop-mixed";
        // Three groups: a slot task that keeps the default (retain), a slot
        // task that opts out, and a keyed task to prove the slot-only knob
        // never touches keyed handles.
        let keep_task = TaskConfig {
            group: "keep".into(),
            ..stateful_slot_task("keep-db", 1, true)
        };
        let drop_task = TaskConfig {
            group: "drop".into(),
            count: TaskCount::new(1),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    driver: None,
                    mode: None,
                    params: None,
                    path: "/var/lib/data".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: Some(false),
                }),
            }),
            ..TaskConfig::new("drop-db", "noop")
        };
        let keyed_task = TaskConfig {
            group: "sandboxes".into(),
            ..stateful_keyed_task("sandbox", true)
        };
        submit_job(&store, job_id, vec![keep_task, drop_task, keyed_task])?;
        store.create_keyed_instance(job_id, "sandboxes", "sandbox", Some("user-a"))?;
        store.place_alloc("node-a", task_id_for(&store, "keep-db", job_id)?)?;
        store.place_alloc("node-a", task_id_for(&store, "drop-db", job_id)?)?;
        store.place_alloc("node-a", task_id_for(&store, "sandbox", job_id)?)?;

        let keep_id = slot_instance_id(job_id, "keep", 0);
        let drop_id = slot_instance_id(job_id, "drop", 0);
        let keep_handle = store
            .get_instance(&keep_id)?
            .context("keep")?
            .storage_handle;
        let keyed_handle = store
            .get_instance("user-a")?
            .context("keyed")?
            .storage_handle;
        assert!(keep_handle.is_some());
        assert!(keyed_handle.is_some());

        store.remove_allocs_for_job(job_id)?;
        assert_eq!(store.stop_instances_for_job(job_id)?, 3);

        // Only the opted-out task's slot handle is dropped.
        let keep = store.get_instance(&keep_id)?.context("keep")?;
        assert_eq!(keep.state, InstanceState::Terminated);
        assert_eq!(keep.storage_handle, keep_handle, "default task retains");
        let dropped = store.get_instance(&drop_id)?.context("drop")?;
        assert_eq!(dropped.state, InstanceState::Terminated);
        assert_eq!(dropped.storage_handle, None, "opted-out task drops");
        // Keyed instances are paused with handles retained, regardless of any
        // slot task's retainOnStop.
        let keyed = store.get_instance("user-a")?.context("keyed")?;
        assert_eq!(keyed.state, InstanceState::Stopped);
        assert_eq!(keyed.storage_handle, keyed_handle, "keyed always retains");
        Ok(())
    }

    fn task_id_for_config(store: &Store, task_name: &str, job_config_id: &str) -> R<i64> {
        let conn = store.read_connection()?;
        let id = conn.query_row(
            "SELECT id FROM task_config WHERE name = ? AND job_config_id = ?;",
            [task_name, job_config_id],
            |row| row.get(0),
        )?;
        Ok(id)
    }

    //
    // KEYED-MODE INSTANCES (sandbox style; see docs/stateful-workloads.md)
    //

    fn stateful_keyed_task(name: &str, with_storage: bool) -> TaskConfig {
        TaskConfig {
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Keyed,
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: with_storage.then(|| crate::structs::StatefulStorageConfig {
                    driver: None,
                    mode: None,
                    params: None,
                    path: "/workspace".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                }),
            }),
            ..TaskConfig::new(name, "noop")
        }
    }

    #[test]
    fn keyed_placement_binds_each_live_instance_and_mints_handles() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-place";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;
        let task_id = task_id_for(&store, "sandbox", job_id)?;

        let first = store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        let second = store.create_keyed_instance(job_id, "default", "sandbox", Some("user-b"))?;
        assert_eq!(first.state, InstanceState::Pending);
        assert_eq!(first.slot_index, None);
        assert_eq!(first.storage_handle, None, "handle minted at placement");
        assert_eq!(second.instance_id, "user-b");

        let alloc_one = store.place_alloc("node-a", task_id)?;
        let alloc_two = store.place_alloc("node-a", task_id)?;
        let mut bound: Vec<&str> = vec![
            alloc_one.instance_id.as_deref().context("bound")?,
            alloc_two.instance_id.as_deref().context("bound")?,
        ];
        bound.sort_unstable();
        assert_eq!(bound, vec!["user-a", "user-b"]);

        for id in ["user-a", "user-b"] {
            let instance = store.get_instance(id)?.context("instance")?;
            assert_eq!(instance.state, InstanceState::Active);
            assert!(instance
                .storage_handle
                .as_deref()
                .is_some_and(|handle| handle.starts_with("local-dir/")));
            assert!(instance.current_alloc_id.is_some());
        }

        // No unbound live instance left: a third placement must fail loudly
        // instead of inventing an instance.
        let err = store.place_alloc("node-a", task_id).expect_err("no free");
        assert!(
            err.to_string().contains("CreateInstance"),
            "error should point at the management API: {err}"
        );
        Ok(())
    }

    #[test]
    fn keyed_instance_cannot_hijack_another_jobs_slot_id() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;

        // Job A owns slot ids `job-a-default-{0,1,2}`.
        let job_a = "job-a";
        submit_job(&store, job_a, vec![stateful_slot_task("db", 3, true)])?;
        let slot_task = task_id_for(&store, "db", job_a)?;

        // Job B is a keyed sandbox job. A caller tries to create a keyed
        // instance whose id is exactly job A's slot-0 id.
        let job_b = "job-b";
        submit_job(&store, job_b, vec![stateful_keyed_task("sandbox", true)])?;
        let hijack_id = slot_instance_id(job_a, "default", 0); // "job-a-default-0"
        let created = store.create_keyed_instance(job_b, "default", "sandbox", Some(&hijack_id))?;

        // Job A's slot-0 placement must NOT adopt job B's row — it is a hard
        // error, because the slot id space is exclusive.
        let err = store
            .place_alloc("node-a", slot_task)
            .expect_err("slot placement must refuse to adopt a foreign row");
        assert!(
            err.to_string().contains("exclusive"),
            "error should explain slot ids are exclusive: {err}"
        );

        // Job B's row is untouched: still keyed, still owned by job B.
        let row = store.get_instance(&hijack_id)?.context("job B row")?;
        assert_eq!(row.job_id, job_b);
        assert_eq!(row.mode, InstanceMode::Keyed);
        assert_eq!(row.created_at, created.created_at);

        Ok(())
    }

    #[test]
    fn keyed_create_rejects_id_aliasing_same_job_slot_pattern() -> R<()> {
        let store = setup_store()?;
        // One job with both a slot task (slots 0,1) and a keyed task.
        let job_id = "mixed";
        submit_job(
            &store,
            job_id,
            vec![
                stateful_slot_task("db", 2, true),
                TaskConfig {
                    group: "sandboxes".into(),
                    ..stateful_keyed_task("sandbox", true)
                },
            ],
        )?;

        // A keyed id matching the slot pattern of the slot task is rejected.
        let colliding = slot_instance_id(job_id, "default", 1); // "mixed-default-1"
        let err = store
            .create_keyed_instance(job_id, "sandboxes", "sandbox", Some(&colliding))
            .expect_err("keyed id aliasing a slot id must be rejected");
        assert!(
            err.to_string().contains("slot-id pattern"),
            "error should mention the reserved slot-id pattern: {err}"
        );

        // A slot index beyond the pool (slot 2) is NOT a slot id, so it's fine.
        let ok_id = slot_instance_id(job_id, "default", 2); // "mixed-default-2"
        store.create_keyed_instance(job_id, "sandboxes", "sandbox", Some(&ok_id))?;
        Ok(())
    }

    #[test]
    fn keyed_allocation_loss_sets_rescheduling_and_replacement_rebinds_identity() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-resched";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;
        let task_id = task_id_for(&store, "sandbox", job_id)?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;

        let alloc = store.place_alloc("node-a", task_id)?;
        let handle = store
            .get_instance("user-a")?
            .context("instance")?
            .storage_handle;
        assert!(handle.is_some());

        // The release hook must NOT terminate a keyed instance just because
        // its allocation died — only DestroyInstance terminates keyed rows.
        store.remove_allocation_by_id(alloc.id)?;
        let waiting = store.get_instance("user-a")?.context("retained")?;
        assert_eq!(waiting.state, InstanceState::Rescheduling);
        assert_eq!(waiting.current_alloc_id, None);
        assert_eq!(waiting.storage_handle, handle, "handle survives the loss");

        // The replacement re-binds the same id + storage handle.
        let replacement = store.place_alloc("node-a", task_id)?;
        assert_eq!(replacement.instance_id.as_deref(), Some("user-a"));
        let reactivated = store.get_instance("user-a")?.context("reactivated")?;
        assert_eq!(reactivated.state, InstanceState::Active);
        assert_eq!(reactivated.current_alloc_id, Some(replacement.id));
        assert_eq!(reactivated.storage_handle, handle);
        Ok(())
    }

    #[test]
    fn create_keyed_instance_rejects_collisions_including_tombstones() -> R<()> {
        let store = setup_store()?;
        let job_id = "keyed-recycle";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;

        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;

        // Live collision.
        let err = store
            .create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))
            .expect_err("live collision");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::Conflict { .. })
        ));

        // Tombstone collision: a destroyed id can NEVER be created again.
        store.destroy_keyed_instance("user-a")?;
        let err = store
            .create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))
            .expect_err("tombstoned ids are never recycled");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::Conflict { .. })
        ));

        // Server-minted ids are fresh and unique.
        let minted = store.create_keyed_instance(job_id, "default", "sandbox", None)?;
        assert_ne!(minted.instance_id, "user-a");
        assert!(minted.instance_id.starts_with("sandbox-"));
        assert!(crate::structs::validate_keyed_instance_id(&minted.instance_id).is_ok());
        let minted_two = store.create_keyed_instance(job_id, "default", "sandbox", None)?;
        assert_ne!(minted.instance_id, minted_two.instance_id);
        Ok(())
    }

    #[test]
    fn destroy_keyed_instance_stops_alloc_terminates_and_drops_handle() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-destroy";
        submit_job(
            &store,
            job_id,
            vec![
                stateful_keyed_task("sandbox", true),
                stateful_slot_task("db", 1, true),
            ],
        )?;
        let keyed_task = task_id_for(&store, "sandbox", job_id)?;
        let slot_task = task_id_for(&store, "db", job_id)?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        let alloc = store.place_alloc("node-a", keyed_task)?;
        let slot_alloc = store.place_alloc("node-a", slot_task)?;

        let outcome = store.destroy_keyed_instance("user-a")?;
        assert_eq!(
            outcome,
            DestroyInstanceOutcome::Destroyed {
                removed_allocations: 1
            }
        );
        assert!(
            store.get_allocation_by_id(alloc.id)?.is_none(),
            "the allocation is deleted through the normal stop path"
        );
        let tombstone = store.get_instance("user-a")?.context("row retained")?;
        assert_eq!(tombstone.state, InstanceState::Terminated);
        assert_eq!(tombstone.current_alloc_id, None);
        assert_eq!(
            tombstone.storage_handle, None,
            "keyed destroy always drops storage (unlike slot scale-in retention)"
        );

        // Idempotent no-op on an already-terminated instance.
        assert_eq!(
            store.destroy_keyed_instance("user-a")?,
            DestroyInstanceOutcome::AlreadyTerminated
        );

        // Slot instances are owned by the scheduler, not this API.
        let slot_id = slot_alloc.instance_id.context("slot bound")?;
        let err = store
            .destroy_keyed_instance(&slot_id)
            .expect_err("slot rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::InvalidInput { .. })
        ));

        // Unknown ids surface NotFound.
        let err = store
            .destroy_keyed_instance("missing")
            .expect_err("missing rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::NotFound { .. })
        ));
        Ok(())
    }

    #[test]
    fn job_stop_stops_keyed_instances_and_terminates_slot_instances() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-stop";
        submit_job(
            &store,
            job_id,
            vec![
                stateful_keyed_task("sandbox", true),
                stateful_slot_task("db", 1, true),
            ],
        )?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        store.place_alloc("node-a", task_id_for(&store, "sandbox", job_id)?)?;
        store.place_alloc("node-a", task_id_for(&store, "db", job_id)?)?;
        let keyed_handle = store
            .get_instance("user-a")?
            .context("keyed row")?
            .storage_handle;
        assert!(keyed_handle.is_some());

        // Keyed rows are merely STOPPED (handle retained — a job stop is a
        // pause, revivable via start_keyed_instance); slot rows keep today's
        // terminated-with-handle behavior so a re-run re-materializes them.
        assert_eq!(store.stop_instances_for_job(job_id)?, 2);
        let keyed = store.get_instance("user-a")?.context("keyed row")?;
        assert_eq!(keyed.state, InstanceState::Stopped);
        assert_eq!(keyed.current_alloc_id, None);
        assert_eq!(
            keyed.storage_handle, keyed_handle,
            "keyed handles retained on job stop"
        );
        let slot = store
            .get_instance(&slot_instance_id(job_id, "default", 0))?
            .context("slot row")?;
        assert_eq!(slot.state, InstanceState::Terminated);
        assert!(slot.storage_handle.is_some(), "slot handles retained");

        // Idempotent: stopped/terminated rows are not touched again.
        assert_eq!(store.stop_instances_for_job(job_id)?, 0);
        Ok(())
    }

    #[test]
    fn count_live_keyed_instances_counts_only_live_rows_of_the_task() -> R<()> {
        let store = setup_store()?;
        let job_id = "keyed-count";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", false)])?;

        assert_eq!(
            store.count_live_keyed_instances(job_id, "default", "sandbox")?,
            0
        );
        store.create_keyed_instance(job_id, "default", "sandbox", Some("a"))?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("b"))?;
        assert_eq!(
            store.count_live_keyed_instances(job_id, "default", "sandbox")?,
            2
        );

        // Terminated tombstones do not count as desired.
        store.destroy_keyed_instance("a")?;
        assert_eq!(
            store.count_live_keyed_instances(job_id, "default", "sandbox")?,
            1
        );

        // Stopped instances do not count as desired either: a stopped
        // instance gets no allocation until start_keyed_instance.
        store.stop_keyed_instance("b")?;
        assert_eq!(
            store.count_live_keyed_instances(job_id, "default", "sandbox")?,
            0
        );
        store.start_keyed_instance("b")?;
        assert_eq!(
            store.count_live_keyed_instances(job_id, "default", "sandbox")?,
            1
        );

        // Other (job, group, task) scopes are not counted.
        assert_eq!(
            store.count_live_keyed_instances(job_id, "other", "sandbox")?,
            0
        );
        assert_eq!(
            store.count_live_keyed_instances("nope", "default", "sandbox")?,
            0
        );
        Ok(())
    }

    #[test]
    fn stop_keyed_instance_pauses_alloc_retains_row_and_handle() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-pause";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;
        let task_id = task_id_for(&store, "sandbox", job_id)?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        let alloc = store.place_alloc("node-a", task_id)?;
        let handle = store
            .get_instance("user-a")?
            .context("instance")?
            .storage_handle;
        assert!(handle.is_some());

        // Stop: allocation removed through the normal stop path, state
        // stopped — NOT rescheduling (the release hook must not fire on a
        // stop) — and the row + storage handle retained.
        let outcome = store.stop_keyed_instance("user-a")?;
        assert_eq!(
            outcome,
            StopInstanceOutcome::Stopped {
                removed_allocations: 1
            }
        );
        assert!(
            store.get_allocation_by_id(alloc.id)?.is_none(),
            "the allocation is deleted through the normal stop path"
        );
        let stopped = store.get_instance("user-a")?.context("row retained")?;
        assert_eq!(stopped.state, InstanceState::Stopped);
        assert_eq!(stopped.current_alloc_id, None);
        assert_eq!(stopped.storage_handle, handle, "handle retained on stop");

        // The id is still owned: creating it again collides (stopped is not
        // destroyed).
        let err = store
            .create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))
            .expect_err("stopped ids still collide");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::Conflict { .. })
        ));

        // A stopped instance is never bound by placement: with only the
        // stopped instance, there is nothing to place.
        let err = store
            .place_alloc("node-a", task_id)
            .expect_err("stopped instances are not placement candidates");
        assert!(err.to_string().contains("no unbound keyed instance"));

        // Idempotent no-op on an already-stopped instance.
        assert_eq!(
            store.stop_keyed_instance("user-a")?,
            StopInstanceOutcome::AlreadyStopped
        );
        Ok(())
    }

    #[test]
    fn stop_keyed_instance_rejects_slot_terminated_and_missing() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-pause-reject";
        submit_job(
            &store,
            job_id,
            vec![
                stateful_keyed_task("sandbox", true),
                stateful_slot_task("db", 1, true),
            ],
        )?;
        let slot_alloc = store.place_alloc("node-a", task_id_for(&store, "db", job_id)?)?;

        // Slot instances are owned by the scheduler, not this API.
        let slot_id = slot_alloc.instance_id.context("slot bound")?;
        let err = store
            .stop_keyed_instance(&slot_id)
            .expect_err("slot rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::InvalidInput { .. })
        ));

        // Terminated tombstones can never be stopped.
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-gone"))?;
        store.destroy_keyed_instance("user-gone")?;
        let err = store
            .stop_keyed_instance("user-gone")
            .expect_err("terminated rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::InvalidInput { .. })
        ));

        // Unknown ids surface NotFound.
        let err = store
            .stop_keyed_instance("missing")
            .expect_err("missing rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::NotFound { .. })
        ));
        Ok(())
    }

    #[test]
    fn start_keyed_instance_revives_stopped_row_with_same_identity_and_handle() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-revive";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;
        let task_id = task_id_for(&store, "sandbox", job_id)?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        store.place_alloc("node-a", task_id)?;
        let handle = store
            .get_instance("user-a")?
            .context("instance")?
            .storage_handle;

        store.stop_keyed_instance("user-a")?;
        assert_eq!(
            store.start_keyed_instance("user-a")?,
            StartInstanceOutcome::Started
        );
        let pending = store.get_instance("user-a")?.context("instance")?;
        assert_eq!(pending.state, InstanceState::Pending);
        assert_eq!(pending.storage_handle, handle, "handle survives the cycle");

        // The next placement re-binds the SAME id and storage handle: the
        // revived workload is the same logical instance.
        let replacement = store.place_alloc("node-a", task_id)?;
        assert_eq!(replacement.instance_id.as_deref(), Some("user-a"));
        let active = store.get_instance("user-a")?.context("instance")?;
        assert_eq!(active.state, InstanceState::Active);
        assert_eq!(active.storage_handle, handle);

        // Idempotent no-op on a live instance, in every live state.
        assert_eq!(
            store.start_keyed_instance("user-a")?,
            StartInstanceOutcome::AlreadyRunning
        );
        Ok(())
    }

    #[test]
    fn start_keyed_instance_rejects_slot_terminated_and_missing() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-revive-reject";
        submit_job(
            &store,
            job_id,
            vec![
                stateful_keyed_task("sandbox", true),
                stateful_slot_task("db", 1, true),
            ],
        )?;
        let slot_alloc = store.place_alloc("node-a", task_id_for(&store, "db", job_id)?)?;

        let slot_id = slot_alloc.instance_id.context("slot bound")?;
        let err = store
            .start_keyed_instance(&slot_id)
            .expect_err("slot rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::InvalidInput { .. })
        ));

        // Terminated is forever: a destroyed id can never be started again.
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-gone"))?;
        store.destroy_keyed_instance("user-gone")?;
        let err = store
            .start_keyed_instance("user-gone")
            .expect_err("terminated rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::InvalidInput { .. })
        ));

        let err = store
            .start_keyed_instance("missing")
            .expect_err("missing rejected");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::NotFound { .. })
        ));
        Ok(())
    }

    #[test]
    fn destroy_after_stop_terminates_and_drops_retained_handle() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-stop-destroy";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;
        let task_id = task_id_for(&store, "sandbox", job_id)?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        store.place_alloc("node-a", task_id)?;

        store.stop_keyed_instance("user-a")?;
        assert!(store
            .get_instance("user-a")?
            .context("stopped row")?
            .storage_handle
            .is_some());

        // Stop is a pause; destroy is forever — even from stopped, destroy
        // drops the retained handle and tombstones the id.
        let outcome = store.destroy_keyed_instance("user-a")?;
        assert_eq!(
            outcome,
            DestroyInstanceOutcome::Destroyed {
                removed_allocations: 0
            }
        );
        let tombstone = store.get_instance("user-a")?.context("tombstone")?;
        assert_eq!(tombstone.state, InstanceState::Terminated);
        assert_eq!(tombstone.storage_handle, None, "handle dropped on destroy");
        Ok(())
    }

    #[test]
    fn release_hook_leaves_stopped_instances_stopped() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "active"))?;
        let job_id = "keyed-release-stop";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;
        let task_id = task_id_for(&store, "sandbox", job_id)?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        let alloc = store.place_alloc("node-a", task_id)?;

        // Simulate an allocation death racing a stop: the instance is
        // already marked stopped when its (stale) allocation is removed. The
        // release hook must NOT flip it back to rescheduling — that would
        // resurrect a deliberately paused instance.
        store.set_instance_state("user-a", InstanceState::Stopped)?;
        store.remove_allocation_by_id(alloc.id)?;
        let instance = store.get_instance("user-a")?.context("instance")?;
        assert_eq!(instance.state, InstanceState::Stopped);
        let handle = instance.storage_handle;
        assert!(handle.is_some(), "handle untouched by the release hook");
        Ok(())
    }

    #[test]
    fn create_keyed_instances_batch_is_all_or_nothing() -> R<()> {
        let store = setup_store()?;
        let job_id = "keyed-batch";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;

        // Explicit ids + minted extras, in request order.
        let created = store.create_keyed_instances(
            job_id,
            "default",
            "sandbox",
            &["a".to_string(), "b".to_string()],
            2,
        )?;
        assert_eq!(created.len(), 4);
        assert_eq!(&created[..2], &["a".to_string(), "b".to_string()]);
        assert!(created[2..].iter().all(|id| id.starts_with("sandbox-")));
        for id in &created {
            let instance = store.get_instance(id)?.context("created row")?;
            assert_eq!(instance.state, InstanceState::Pending);
            assert_eq!(instance.mode, InstanceMode::Keyed);
        }

        // Any collision — here "b", including with a tombstone — fails the
        // whole batch and creates NOTHING.
        store.destroy_keyed_instance("b")?;
        let err = store
            .create_keyed_instances(
                job_id,
                "default",
                "sandbox",
                &[
                    "fresh-1".to_string(),
                    "b".to_string(),
                    "fresh-2".to_string(),
                ],
                1,
            )
            .expect_err("tombstone collision aborts the batch");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::Conflict { .. })
        ));
        assert!(err.to_string().contains("b"), "offending id named: {err}");
        assert!(store.get_instance("fresh-1")?.is_none(), "nothing created");
        assert!(store.get_instance("fresh-2")?.is_none(), "nothing created");
        assert_eq!(store.list_instances(job_id)?.len(), 4);

        // Duplicates WITHIN the request also abort atomically.
        let err = store
            .create_keyed_instances(
                job_id,
                "default",
                "sandbox",
                &["dupe".to_string(), "dupe".to_string()],
                0,
            )
            .expect_err("in-request duplicate aborts the batch");
        assert!(matches!(
            StorageError::from_anyhow(&err),
            Some(StorageError::Conflict { .. })
        ));
        assert!(store.get_instance("dupe")?.is_none(), "nothing created");

        // Count-only mints N distinct ids.
        let minted = store.create_keyed_instances(job_id, "default", "sandbox", &[], 3)?;
        assert_eq!(minted.len(), 3);
        let unique: HashSet<&String> = minted.iter().collect();
        assert_eq!(unique.len(), 3, "minted ids are distinct");
        Ok(())
    }

    #[test]
    fn seed_keyed_instances_creates_new_and_skips_any_existing() -> R<()> {
        let store = setup_store()?;
        let job_id = "keyed-seed";
        submit_job(&store, job_id, vec![stateful_keyed_task("sandbox", true)])?;

        let ids = vec!["seed-a".to_string(), "seed-b".to_string()];
        let created = store.seed_keyed_instances(job_id, "default", "sandbox", &ids)?;
        assert_eq!(created, ids);
        assert_eq!(
            store.count_live_keyed_instances(job_id, "default", "sandbox")?,
            2
        );

        // Re-seeding is a silent no-op (resubmits are idempotent).
        let created = store.seed_keyed_instances(job_id, "default", "sandbox", &ids)?;
        assert!(created.is_empty());

        // A destroyed id is skipped, NOT resurrected: the seed list never
        // overrides a deliberate destroy.
        store.destroy_keyed_instance("seed-a")?;
        let created = store.seed_keyed_instances(job_id, "default", "sandbox", &ids)?;
        assert!(created.is_empty());
        assert_eq!(
            store
                .get_instance("seed-a")?
                .context("tombstone retained")?
                .state,
            InstanceState::Terminated
        );

        // A stopped id is skipped too: it stays stopped until StartInstance.
        store.stop_keyed_instance("seed-b")?;
        let created = store.seed_keyed_instances(job_id, "default", "sandbox", &ids)?;
        assert!(created.is_empty());
        assert_eq!(
            store
                .get_instance("seed-b")?
                .context("stopped retained")?
                .state,
            InstanceState::Stopped
        );
        Ok(())
    }

    // #[test]
    // fn handle_job_submit_with_unchanged_job_and_force_update_works() -> R<()> {
    //     assert!(true);
    //     let _store = setup_store()?;
    //     Ok(())
    // }

    // #[test]
    // fn handle_job_stop() -> R<()> {
    //     assert!(true);
    //     let _store = setup_store()?;
    //     Ok(())
    // }

    // #[test]
    // fn handle_job_purge() -> R<()> {
    //     assert!(true);
    //     let _store = setup_store()?;
    //     Ok(())
    // }

    //
    // job mtime (`job.updated_at`) — see the job-updated-at plan.
    //

    /// Submit `name` with one noop task whose runtime marker is `marker`, so
    /// two calls with different markers are two distinct spec versions.
    fn submit_versioned_job(store: &Store, name: &str, marker: &str) -> R<()> {
        let task = TaskConfig::new(marker, "noop");
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks: vec![task],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: format!("{{ marker: '{marker}' }}"),
                is_primary: true,
            }],
        )?;
        Ok(())
    }

    /// Drag a job's timestamps into the past so a mtime bump is observable
    /// without sleeping — the column is whole seconds, so a same-second
    /// write is indistinguishable from no write at all.
    fn backdate_job(store: &Store, name: &str, seconds_ago: i64) -> R<i64> {
        let then = OffsetDateTime::now_utc().unix_timestamp() - seconds_ago;
        let conn = store.write_connection()?;
        conn.execute(
            "UPDATE job SET created_at = ?2, updated_at = ?2 WHERE id = ?1;",
            params![name, then],
        )?;
        Ok(then)
    }

    #[test]
    fn a_new_job_reports_the_same_created_and_updated_time() -> R<()> {
        let store = setup_store()?;
        submit_versioned_job(&store, "mtime-new", "v1")?;

        let job = store.get_job("mtime-new")?;
        assert_eq!(
            job.created_at, job.updated_at,
            "a job that has never changed has not been updated since it was created"
        );
        assert!(
            job.created_at > 0,
            "timestamp is real, got {}",
            job.created_at
        );
        Ok(())
    }

    #[test]
    fn submitting_a_new_spec_version_moves_updated_at_but_not_created_at() -> R<()> {
        let store = setup_store()?;
        submit_versioned_job(&store, "mtime-resubmit", "v1")?;
        let then = backdate_job(&store, "mtime-resubmit", 3_600)?;

        submit_versioned_job(&store, "mtime-resubmit", "v2")?;

        let job = store.get_job("mtime-resubmit")?;
        assert_eq!(
            job.created_at, then,
            "created_at is the job's birth time and a resubmit must not move it"
        );
        assert!(
            job.updated_at > then,
            "a new spec version is a change to the job: {} !> {then}",
            job.updated_at
        );
        Ok(())
    }

    #[test]
    fn resubmitting_an_unchanged_spec_leaves_updated_at_alone() -> R<()> {
        let store = setup_store()?;
        submit_versioned_job(&store, "mtime-unchanged", "v1")?;
        let then = backdate_job(&store, "mtime-unchanged", 3_600)?;

        // Byte-identical resubmit: the store short-circuits to `Unchanged`,
        // and nothing about the job actually changed.
        submit_versioned_job(&store, "mtime-unchanged", "v1")?;

        let job = store.get_job("mtime-unchanged")?;
        assert_eq!(
            job.updated_at, then,
            "an unchanged resubmit is not a change"
        );
        Ok(())
    }

    #[test]
    fn job_status_transitions_move_updated_at() -> R<()> {
        let store = setup_store()?;
        submit_versioned_job(&store, "mtime-status", "v1")?;

        let then = backdate_job(&store, "mtime-status", 3_600)?;
        store.stop_job("mtime-status")?;
        let stopped = store.get_job("mtime-status")?;
        assert_eq!(stopped.status, "stopped");
        assert!(stopped.updated_at > then, "stop moves the mtime");
        assert_eq!(stopped.created_at, then, "stop leaves created_at alone");

        let then = backdate_job(&store, "mtime-status", 3_600)?;
        assert!(store.clear_job_stopped_status("mtime-status")?);
        assert!(
            store.get_job("mtime-status")?.updated_at > then,
            "revival moves the mtime"
        );

        let then = backdate_job(&store, "mtime-status", 3_600)?;
        store.mark_job_failed("mtime-status")?;
        assert!(
            store.get_job("mtime-status")?.updated_at > then,
            "failure moves the mtime"
        );

        let then = backdate_job(&store, "mtime-status", 3_600)?;
        assert!(store.clear_job_failed_status("mtime-status")?);
        assert!(
            store.get_job("mtime-status")?.updated_at > then,
            "recovery moves the mtime"
        );
        Ok(())
    }

    #[test]
    fn a_no_op_status_clear_leaves_updated_at_alone() -> R<()> {
        // `clear_job_*_status` is called speculatively on every recovery
        // path. When it matches no row it must not tick the mtime, or every
        // healthy job in the cluster would read as freshly touched.
        let store = setup_store()?;
        submit_versioned_job(&store, "mtime-noop", "v1")?;
        let then = backdate_job(&store, "mtime-noop", 3_600)?;

        assert!(!store.clear_job_failed_status("mtime-noop")?);
        assert!(!store.clear_job_stopped_status("mtime-noop")?);

        assert_eq!(
            store.get_job("mtime-noop")?.updated_at,
            then,
            "a status clear that changed nothing is not an update"
        );
        Ok(())
    }

    #[test]
    fn allocation_churn_does_not_move_a_jobs_updated_at() -> R<()> {
        // The mtime describes the job record, not the workload underneath
        // it: placements come and go on their own clock and would otherwise
        // keep every job permanently "just updated".
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        submit_versioned_job(&store, "mtime-churn", "v1")?;
        let then = backdate_job(&store, "mtime-churn", 3_600)?;

        let task_id = task_id_for(&store, "v1", "mtime-churn")?;
        let alloc = store.place_alloc("node-a", task_id)?;
        store.remove_allocation_by_id(alloc.id)?;
        store.place_alloc("node-a", task_id)?;

        assert_eq!(
            store.get_job("mtime-churn")?.updated_at,
            then,
            "placing and replacing an allocation is not a change to the job"
        );
        Ok(())
    }

    fn service_task(name: &str) -> TaskConfig {
        let mut task = TaskConfig::new(name, "noop");
        task.network = Some(crate::structs::NetworkConfig {
            ports: vec![crate::structs::PortConfig {
                name: "http".into(),
                static_port: None,
                to: Some(8080),
            }],
            ..Default::default()
        });
        task.service = Some(
            serde_json::from_value(json!({ "port": "http", "tags": ["v1"] }))
                .expect("service config"),
        );
        task
    }

    fn submit_service_job(store: &Store, name: &str, tasks: Vec<TaskConfig>) -> R<()> {
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks,
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.jsonnet".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        Ok(())
    }

    #[test]
    fn port_assignment_gives_distinct_dynamic_ports() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        submit_service_job(&store, "svc-ports", vec![service_task("web")])?;
        let task_id = task_id_for(&store, "web", "svc-ports")?;

        let first = store.place_alloc("node-a", task_id)?;
        let second = store.place_alloc("node-a", task_id)?;

        let first_port = first
            .network
            .as_ref()
            .and_then(|network| network.port("http"))
            .map(|port| port.host_port)
            .context("first alloc missing http port")?;
        let second_port = second
            .network
            .as_ref()
            .and_then(|network| network.port("http"))
            .map(|port| port.host_port)
            .context("second alloc missing http port")?;

        assert_ne!(first_port, second_port);
        assert!(first_port >= crate::structs::DYNAMIC_PORT_MIN);
        assert!(second_port <= crate::structs::DYNAMIC_PORT_MAX);
        Ok(())
    }

    #[test]
    fn static_port_conflict_fails_placement_with_clear_error() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        let mut task = TaskConfig::new("redis", "noop");
        task.service =
            Some(serde_json::from_value(json!({ "port": 6379 })).expect("service config"));
        submit_service_job(&store, "svc-conflict", vec![task])?;
        let task_id = task_id_for(&store, "redis", "svc-conflict")?;

        store.place_alloc("node-a", task_id)?;
        let err = store.place_alloc("node-a", task_id).unwrap_err();
        let details = format!("{err:#}");
        assert!(details.contains("static port 6379"), "got: {details}");
        assert!(details.contains("node-a"), "got: {details}");
        Ok(())
    }

    #[test]
    fn service_endpoints_filter_is_pushed_into_an_indexed_seek() -> R<()> {
        // The connect upstream feed resolves one service per node per
        // heartbeat, so a filtered lookup must never scan the whole catalog.
        // Assert the planner really seeks `idx_task_config_service_name`: the
        // SQL predicate and the indexed expression have to stay identical, and
        // a silent drift back to a scan is exactly the regression this guards.
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        let mut renamed = service_task("api");
        renamed.service = Some(serde_json::from_value(
            json!({ "name": "renamed", "port": "http" }),
        )?);
        submit_service_job(&store, "svc-index", vec![service_task("web"), renamed])?;
        store.place_alloc("node-a", task_id_for(&store, "web", "svc-index")?)?;
        store.place_alloc("node-a", task_id_for(&store, "api", "svc-index")?)?;

        let plan = {
            let conn = store.read_connection()?;
            let sql = format!(
                "EXPLAIN QUERY PLAN {}",
                Store::service_endpoints_sql(true).trim_end_matches(';')
            );
            let mut stmt = conn.prepare(&sql)?;
            let steps = stmt.query_map(["web"], |row| row.get::<_, String>(3))?;
            Store::collect_rows(steps)?.join(" | ")
        };
        assert!(
            plan.contains("idx_task_config_service_name"),
            "filtered lookup must seek the service-name index, got: {plan}"
        );

        // The SQL predicate must agree with `ServiceConfig::service_name`,
        // including the `service.name` override and the task-name default.
        let names = |filter: &str| -> R<Vec<String>> {
            Ok(store
                .service_endpoints(Some(filter))?
                .into_iter()
                .map(|row| row.service)
                .collect())
        };
        assert_eq!(names("web")?, vec!["web".to_string()]);
        assert_eq!(names("renamed")?, vec!["renamed".to_string()]);
        // The task name of a renamed service is not the service name.
        assert!(names("api")?.is_empty());
        assert!(names("nope")?.is_empty());

        // …and it matches what filtering the full projection in Rust yields.
        for filter in ["web", "renamed", "api", "nope"] {
            let via_scan = store
                .service_endpoints(None)?
                .into_iter()
                .filter(|row| row.service == filter)
                .map(|row| row.allocation_id)
                .collect::<Vec<_>>();
            let via_sql = store
                .service_endpoints(Some(filter))?
                .into_iter()
                .map(|row| row.allocation_id)
                .collect::<Vec<_>>();
            assert_eq!(via_sql, via_scan, "filter '{filter}' drifted from the scan");
        }
        Ok(())
    }

    #[test]
    fn a_corrupt_service_row_does_not_break_other_services() -> R<()> {
        // One hand-corrupted `service` value must not blind every consumer of
        // every *other* service — the filtered lookup skips it.
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        submit_service_job(&store, "svc-corrupt", vec![service_task("web")])?;
        let good_task = task_id_for(&store, "web", "svc-corrupt")?;
        store.place_alloc("node-a", good_task)?;

        submit_service_job(&store, "svc-corrupt-other", vec![service_task("other")])?;
        let bad_task = task_id_for(&store, "other", "svc-corrupt-other")?;
        store.place_alloc("node-a", bad_task)?;
        {
            let conn = store.write_connection()?;
            conn.execute(
                "UPDATE task_config SET service = 'not json' WHERE id = ?1;",
                [bad_task],
            )?;
        }

        let endpoints = store.service_endpoints(Some("web"))?;
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].service, "web");
        // The corrupt row itself simply resolves to nothing.
        assert!(store.service_endpoints(Some("other"))?.is_empty());
        Ok(())
    }

    #[test]
    fn service_endpoints_projection_gates_on_health_and_node() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        submit_service_job(&store, "svc-proj", vec![service_task("web")])?;
        let task_id = task_id_for(&store, "web", "svc-proj")?;
        let alloc = store.place_alloc("node-a", task_id)?;

        // No health snapshot yet: endpoint exists but is not healthy.
        let endpoints = store.service_endpoints(Some("web"))?;
        assert_eq!(endpoints.len(), 1);
        assert!(!endpoints[0].healthy);
        assert!(endpoints[0].port.is_some());

        // Healthy snapshot: endpoint becomes healthy.
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[AllocationHealth {
                allocation_id: alloc.id,
                healthy: true,
                status: "healthy".into(),
                artifact_states: None,
            }],
        )?;
        let endpoints = store.service_endpoints(Some("web"))?;
        assert!(endpoints[0].healthy);

        // Unhealthy snapshot: gated out again.
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[AllocationHealth {
                allocation_id: alloc.id,
                healthy: false,
                status: "tcp connect refused".into(),
                artifact_states: None,
            }],
        )?;
        let endpoints = store.service_endpoints(Some("web"))?;
        assert!(!endpoints[0].healthy);
        assert_eq!(endpoints[0].health_status, "tcp connect refused");

        // Healthy again but node lost: still not healthy (liveness lease).
        store.upsert_allocation_health_snapshots(
            "node-a",
            &[AllocationHealth {
                allocation_id: alloc.id,
                healthy: true,
                status: "healthy".into(),
                artifact_states: None,
            }],
        )?;
        store.mark_node_lost("node-a")?;
        let endpoints = store.service_endpoints(Some("web"))?;
        assert!(!endpoints[0].healthy);
        assert!(endpoints[0].health_status.contains("node node-a is lost"));

        // Removing the allocation removes the endpoint entirely.
        store.remove_allocation_by_id(alloc.id)?;
        let endpoints = store.service_endpoints(Some("web"))?;
        assert!(endpoints.is_empty());

        // But the declared service is still listed (0 endpoints).
        let declared = store.declared_services()?;
        assert!(declared.iter().any(|entry| entry.service == "web"));
        Ok(())
    }

    #[test]
    fn job_template_versions_are_immutable_with_latest_pointer() -> R<()> {
        let store = setup_store()?;
        let variables = vec![JobTemplateVariableRecord {
            key: "replicas".into(),
            var_type: "number".into(),
            label: None,
            description: None,
            required: None,
            default: Some(json!(2)),
            options: vec![],
        }];

        let v1 = store.upsert_job_template_version(
            "tpl",
            Some("first"),
            "src-v1",
            &variables,
            "alice",
        )?;
        assert_eq!(v1.version, 1);
        assert_eq!(v1.latest_version, 1);
        assert_eq!(v1.created_by, "alice");
        assert!(!v1.variables[0].is_required());

        let v2 = store.upsert_job_template_version("tpl", Some("second"), "src-v2", &[], "bob")?;
        assert_eq!(v2.version, 2);
        assert_eq!(v2.latest_version, 2);

        // Latest fetch returns v2; explicit fetch still returns v1 verbatim.
        let latest = store.get_job_template_version("tpl", None)?;
        assert_eq!(latest.version, 2);
        assert_eq!(latest.source_jsonnet, "src-v2");
        let pinned = store.get_job_template_version("tpl", Some(1))?;
        assert_eq!(pinned.source_jsonnet, "src-v1");
        assert_eq!(pinned.latest_version, 2);
        assert_eq!(pinned.variables.len(), 1);

        let versions = store.list_job_template_versions("tpl")?;
        assert_eq!(
            versions.iter().map(|info| info.version).collect::<Vec<_>>(),
            vec![2, 1]
        );

        let summaries = store.list_job_templates()?;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].latest_version, 2);
        assert_eq!(summaries[0].description.as_deref(), Some("second"));
        assert_eq!(summaries[0].variable_count, 0);

        assert!(store.get_job_template_version("tpl", Some(9)).is_err());
        assert!(store.get_job_template_version("missing", None).is_err());

        store.delete_job_template("tpl")?;
        assert!(store.get_job_template_version("tpl", None).is_err());
        assert!(store.list_job_templates()?.is_empty());
        assert!(store.delete_job_template("tpl").is_err());
        Ok(())
    }

    #[test]
    fn template_provenance_rides_job_config_meta() -> R<()> {
        let store = setup_store()?;
        let config = JobConfig {
            priority: Default::default(),
            name: "tpl-job".into(),
            rollout: None,
            rollout_by_group: BTreeMap::new(),
            tasks: vec![TaskConfig::new("main", "noop")],
            version: None,
        };
        let submission = Submission {
            path: "template://tpl/1".into(),
            content: "src".into(),
            is_primary: true,
        };
        let provenance = JobTemplateProvenance {
            name: "tpl".into(),
            version: 1,
        };
        store.handle_job_submit_from_template(&config, &[submission.clone()], &provenance)?;

        assert_eq!(
            store.job_template_provenance("tpl-job")?,
            Some(provenance.clone())
        );
        let history = store.list_job_spec_history("tpl-job")?;
        assert_eq!(history[0].template, Some(provenance.clone()));
        let map = store.job_template_provenance_map()?;
        assert_eq!(map.get("tpl-job"), Some(&provenance));

        // A raw resubmit (spec change, no template) clears the latest
        // provenance while history keeps the older row.
        let mut updated = config.clone();
        updated.tasks[0].count = TaskCount {
            num: 2,
            ..updated.tasks[0].count.clone()
        };
        store.handle_job_submit(&updated, &[submission])?;
        assert_eq!(store.job_template_provenance("tpl-job")?, None);
        let history = store.list_job_spec_history("tpl-job")?;
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].template, None);
        assert_eq!(history[1].template, Some(provenance));
        assert!(store.job_template_provenance_map()?.is_empty());
        Ok(())
    }

    #[test]
    fn service_port_sugar_persists_explicit_network() -> R<()> {
        let store = setup_store()?;
        let mut task = TaskConfig::new("redis", "noop");
        task.service =
            Some(serde_json::from_value(json!({ "port": 6379 })).expect("service config"));
        submit_service_job(&store, "svc-sugar", vec![task])?;

        let job_config_id = store.get_job_config("svc-sugar")?.id();
        let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
        let (_, stored) = tasks.first().context("missing stored task")?;
        let network = stored
            .network
            .as_ref()
            .context("network not materialized")?;
        assert_eq!(network.ports.len(), 1);
        assert_eq!(network.ports[0].name, "redis");
        assert_eq!(network.ports[0].static_port, Some(6379));
        Ok(())
    }

    //
    // Raft capture seam (phase A-0) — see src/storage/raft/.
    //

    use crate::storage::raft::capture as raft_capture;

    fn kv_value(store: &Store, key: &str) -> R<Option<String>> {
        Ok(store.get_kv(key)?.map(|entry| entry.value))
    }

    #[test]
    fn capture_mutation_rolls_back_and_apply_reproduces_on_a_second_store() -> R<()> {
        let src = setup_store()?;
        let dst = setup_store()?;

        let (entry, changeset) = raft_capture::capture_mutation(&src, Lane::Control, |s| {
            s.put_kv("cfg/db", "postgres://one", KvValueType::String, None, None)
        })?;
        assert_eq!(entry.version, 1, "closure return value survives capture");
        assert!(!changeset.is_empty(), "changeset captured");

        // The proposer's DB is untouched: the capture savepoint rolled back.
        assert_eq!(kv_value(&src, "cfg/db")?, None, "source rolled back");

        // Apply on both replicas: identical row effects, including versions.
        raft_capture::apply_changeset(&dst, &changeset)?;
        raft_capture::apply_changeset(&src, &changeset)?;
        assert_eq!(kv_value(&dst, "cfg/db")?.as_deref(), Some("postgres://one"));
        assert_eq!(kv_value(&src, "cfg/db")?.as_deref(), Some("postgres://one"));
        assert_eq!(dst.get_kv("cfg/db")?.map(|e| e.version), Some(1));
        Ok(())
    }

    #[test]
    fn capture_error_still_rolls_back_and_captures_nothing() -> R<()> {
        let store = setup_store()?;
        store.put_kv("locked", "v1", KvValueType::String, None, None)?;

        let result = raft_capture::capture_mutation(&store, Lane::Control, |s| {
            // Writes, then fails the version check — the whole capture must
            // vanish.
            s.put_kv("other", "x", KvValueType::String, None, None)?;
            s.put_kv("locked", "v2", KvValueType::String, Some(99), None)
        });
        assert!(result.is_err(), "closure error propagates");
        assert_eq!(
            kv_value(&store, "other")?,
            None,
            "partial write rolled back"
        );
        assert_eq!(kv_value(&store, "locked")?.as_deref(), Some("v1"));
        Ok(())
    }

    #[test]
    fn nested_replicated_calls_compose_into_one_changeset() -> R<()> {
        let src = setup_store()?;
        let dst = setup_store()?;

        // Both put_kv calls are public replicated methods; inside an active
        // capture they must run inline (JC-13) and land in ONE changeset.
        let ((), changeset) = raft_capture::capture_mutation(&src, Lane::Control, |s| {
            s.put_kv("a", "1", KvValueType::String, None, None)?;
            s.put_kv("b", "2", KvValueType::String, None, None)?;
            Ok(())
        })?;
        assert_eq!(kv_value(&src, "a")?, None);
        assert_eq!(kv_value(&src, "b")?, None);

        raft_capture::apply_changeset(&dst, &changeset)?;
        assert_eq!(kv_value(&dst, "a")?.as_deref(), Some("1"));
        assert_eq!(kv_value(&dst, "b")?.as_deref(), Some("2"));
        Ok(())
    }

    #[test]
    fn apply_projects_read_model_and_event_side_effects() -> R<()> {
        let src = setup_store()?;
        let dst = setup_store()?;
        // Identical seed state on both replicas (same rows, same rowids).
        src.insert_node(make_node_msg("node-a", "active"))?;
        dst.insert_node(make_node_msg("node-a", "active"))?;

        let mut src_events = src.subscribe_events();
        let mut dst_events = dst.subscribe_events();

        let ((), changeset) = raft_capture::capture_mutation(&src, Lane::Control, |s| {
            s.mark_node_ready("node-a", "2026-01-01T00:00:00Z")?;
            Ok(())
        })?;

        // Suppression during capture: no broadcast, no cache patch, and the
        // rows themselves rolled back on the proposer.
        assert!(src_events.try_recv().is_err(), "no event before commit");
        let src_node = src.get_node("node-a")?;
        assert_eq!(src_node.status, "active", "node row rolled back");
        let cached = src.cached_nodes()?;
        assert_eq!(cached[0].status, "active", "read model not touched");

        raft_capture::apply_changeset(&dst, &changeset)?;

        // Data applied...
        assert_eq!(dst.get_node("node-a")?.status, "ready");
        // ...read model projected from the changeset...
        let dst_cached = dst.cached_nodes()?;
        assert_eq!(dst_cached.len(), 1);
        assert_eq!(dst_cached[0].status, "ready");
        assert_eq!(dst_cached[0].last_heartbeat, "2026-01-01T00:00:00Z");
        // ...and the staged events broadcast at apply time, in id order.
        let kinds: Vec<String> = std::iter::from_fn(|| dst_events.try_recv().ok())
            .map(|event| event.kind)
            .collect();
        assert_eq!(kinds, vec!["node.ready"]);
        Ok(())
    }

    /// JC-41 (reversing JC-27): eval writes are ordinary REPLICATED
    /// scheduler-lane ops. An enqueue captured on the scheduler lane rides
    /// that lane's changeset and passes the ownership guard; the same
    /// enqueue composed into a control-lane capture is a foreign-lane write
    /// the guard rejects — which is exactly what forces cross-lane
    /// composites to enqueue as their own sequential scheduler op after
    /// their own op commits (JC-33's wrapper-sequential shape).
    #[test]
    fn eval_queue_writes_are_scheduler_lane_replicated_ops() -> R<()> {
        use crate::storage::raft::ownership;

        let store = setup_store()?;

        let (eval, changeset) = raft_capture::capture_mutation(&store, Lane::Scheduler, |s| {
            s.enqueue_eval("job_submit", Some("captured-job"), None, None, None)
        })?;
        assert_eq!(eval.kind, "job_submit");
        let mut saw_eval_row = false;
        {
            use fallible_streaming_iterator::FallibleStreamingIterator;
            use rusqlite::session::ChangesetIter;
            let reader: &mut dyn std::io::Read = &mut &*changeset;
            let mut iter = ChangesetIter::start_strm(&reader)?;
            while let Some(item) = iter.next()? {
                if item.op()?.table_name() == "eval" {
                    saw_eval_row = true;
                }
            }
        }
        assert!(
            saw_eval_row,
            "the eval row must ride the scheduler lane's changeset (JC-41) — without it a \
             follower cannot inherit acknowledged work"
        );
        ownership::verify_changeset_ownership("test_eval_own", Lane::Scheduler, false, &changeset)?;

        // The capture above rolled back, so nothing persisted.
        assert!(store.get_evals()?.is_empty());

        // Composed into a control-lane capture, the same enqueue is a
        // foreign-lane write.
        let ((), control_changeset) = raft_capture::capture_mutation(&store, Lane::Control, |s| {
            s.put_kv("evals/foreign", "yes", KvValueType::String, None, None)?;
            s.enqueue_eval("job_submit", Some("foreign-job"), None, None, None)?;
            Ok(())
        })?;
        let err = ownership::verify_changeset_ownership(
            "test_eval_foreign",
            Lane::Control,
            false,
            &control_changeset,
        )
        .expect_err("an eval enqueued inside a control capture must be rejected");
        let rendered = format!("{err:#}");
        assert!(rendered.contains("eval"), "{rendered}");
        assert!(rendered.contains("scheduler"), "{rendered}");
        Ok(())
    }

    /// The replica content hash covers replicated tables and excludes only
    /// what legitimately differs between healthy replicas: raft bookkeeping
    /// and the locally-pruned commutative tables (`scheduler_event`,
    /// `idempotency_record`, JC-28). `eval` is INCLUDED again (JC-41) —
    /// replicas that disagree about pending work have diverged.
    #[test]
    fn content_hash_ignores_commutative_tables_but_covers_evals() -> R<()> {
        let store = setup_store()?;
        store.put_kv("hash/base", "v", KvValueType::String, None, None)?;
        // Flush the events staged by the write above so the baseline hash
        // is taken with a quiet journal.
        let baseline = store.state_content_hash()?;

        store.emit_event(EventDraft::info("test.kind", "local divergence"));
        assert!(store.idempotency_record("hash-key", "op", "message")?);

        assert_eq!(
            store.state_content_hash()?,
            baseline,
            "scheduler_event/idempotency_record rows must not change the replica hash"
        );

        let eval = store.enqueue_eval("job_submit", None, None, None, None)?;
        let with_eval = store.state_content_hash()?;
        assert_ne!(
            with_eval, baseline,
            "the eval queue is replicated state (JC-41): a pending eval MUST change the hash, \
             or replicas could silently disagree about acknowledged work"
        );
        store.complete_eval(&eval.id, true)?;
        assert_eq!(
            store.state_content_hash()?,
            baseline,
            "completing the eval returns the queue — and the hash — to its baseline"
        );

        store.put_kv("hash/other", "v2", KvValueType::String, None, None)?;
        assert_ne!(
            store.state_content_hash()?,
            baseline,
            "replicated rows must still change the hash"
        );
        Ok(())
    }

    /// JC-28: journal pruning is local maintenance. Under capture the
    /// opportunistic prune is suppressed (the changeset stays INSERT-only)
    /// and each replica prunes its own journal when projecting the
    /// committed changeset.
    #[test]
    fn event_journal_prunes_locally_at_apply_time() -> R<()> {
        let src = setup_store()?;
        let dst = setup_store()?;
        // Fill the destination's journal to exactly the retention size, so
        // one more applied event pushes the oldest row (id 1) out of the
        // count-based retained window.
        {
            let conn = dst.write_connection()?;
            conn.execute(
                "WITH RECURSIVE seq(n) AS (
                     SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < ?1
                 )
                 INSERT INTO scheduler_event (id, kind, level, message, metadata_json, created_at)
                 SELECT n, 'old.kind', 'info', 'stale', '{}', '2026-01-01T00:00:00Z' FROM seq;",
                [EVENT_RETAINED_ROWS],
            )?;
        }

        // Capture an event insert whose id lands exactly on the prune
        // boundary (`Store::event_id_triggers_prune`, which divides the lane
        // residue out first — JC-42) and sorts above every seeded row.
        let stride = Lane::ID_STRIDE;
        let period = stride * EVENT_PRUNE_INTERVAL;
        let boundary_id = EVENT_RETAINED_ROWS + period - EVENT_RETAINED_ROWS.rem_euclid(period);
        assert!(Store::event_id_triggers_prune(boundary_id));
        let ((), changeset) = raft_capture::capture_mutation(&src, Lane::Control, |s| {
            let conn = s.write_connection()?;
            conn.execute(
                "INSERT INTO scheduler_event (id, kind, level, message, metadata_json, created_at)
                 VALUES (?1, 'new.kind', 'info', 'boundary', '{}', '2026-01-01T00:00:00Z');",
                [boundary_id],
            )?;
            Ok(())
        })?;
        // The capture recorded ONLY the insert (no DELETE prune rode along).
        {
            use fallible_streaming_iterator::FallibleStreamingIterator;
            use rusqlite::hooks::Action;
            use rusqlite::session::ChangesetIter;
            let reader: &mut dyn std::io::Read = &mut &*changeset;
            let mut iter = ChangesetIter::start_strm(&reader)?;
            while let Some(item) = iter.next()? {
                assert_eq!(item.op()?.code(), Action::SQLITE_INSERT);
            }
        }

        raft_capture::apply_changeset(&dst, &changeset)?;
        let (stale, boundary): (i64, i64) = {
            let conn = dst.read_connection()?;
            (
                conn.query_row(
                    "SELECT COUNT(*) FROM scheduler_event WHERE id = 1;",
                    [],
                    |r| r.get(0),
                )?,
                conn.query_row(
                    "SELECT COUNT(*) FROM scheduler_event WHERE id = ?1;",
                    [boundary_id],
                    |r| r.get(0),
                )?,
            )
        };
        assert_eq!(boundary, 1, "committed event applied");
        assert_eq!(stale, 0, "replica pruned its own journal at apply time");
        Ok(())
    }

    /// JC-42: the opportunistic prune boundary must not systematically
    /// favor one lane. With lane-strided ids the pre-JC-42 test
    /// (`id % EVENT_PRUNE_INTERVAL == 0`) is satisfiable ONLY by ids
    /// divisible by the stride — i.e. only ever by the control lane — so a
    /// cluster whose journal is dominated by status/scheduler events would
    /// never prune on the nodes applying them. Dividing the residue out
    /// first gives every lane the same boundary rate.
    #[test]
    fn prune_boundary_fires_at_the_same_rate_on_every_lane() {
        const PERIODS: i64 = 4;
        for lane in Lane::ALL {
            // Consecutive ids as one lane actually mints them: consecutive
            // sequence values, all inside the lane's residue class.
            let ids: Vec<i64> = (0..PERIODS * EVENT_PRUNE_INTERVAL)
                .map(|sequence| sequence * Lane::ID_STRIDE + lane.id_residue())
                .collect();
            let hits = ids
                .iter()
                .filter(|id| Store::event_id_triggers_prune(**id))
                .count() as i64;
            assert_eq!(
                hits,
                PERIODS,
                "the {} lane must reach the prune boundary once per {EVENT_PRUNE_INTERVAL} \
                 events, like every other lane",
                lane.wire_name()
            );

            // Regression guard: the naive rule this replaced.
            let naive = ids
                .iter()
                .filter(|id| *id % EVENT_PRUNE_INTERVAL == 0)
                .count();
            if lane == Lane::Control {
                assert!(naive > 0);
            } else {
                assert_eq!(
                    naive,
                    0,
                    "the naive `id % EVENT_PRUNE_INTERVAL` boundary is unreachable from the {} \
                     lane — that is the bug event_id_triggers_prune exists to prevent",
                    lane.wire_name()
                );
            }
        }
    }

    /// JC-42, the cursor + publication contract with interleaved lanes: a
    /// replica applying events from all three lanes must still expose them
    /// as ONE `after_id`-walkable stream (every event returned exactly
    /// once, ids strictly increasing) and publish them to live subscribers
    /// in id order. Strided ids are sparse and interleave across lanes, so
    /// this is the property `ListEvents`/`StreamEvents` actually rely on —
    /// density was never part of it.
    #[test]
    fn strided_ids_from_every_lane_walk_a_single_cursor_exactly_once() -> R<()> {
        let store = setup_store()?;
        let mut subscriber = store.subscribe_events();

        // Round-robin the lanes, the way three lane leaders on one node
        // interleave their commits.
        let mut emitted = 0usize;
        for round in 0..4 {
            for lane in Lane::ALL {
                let ((), changeset) = raft_capture::capture_mutation(&store, lane, |s| {
                    s.emit_event(EventDraft::info(
                        "probe.interleaved",
                        format!("{} round {round}", lane.wire_name()),
                    ));
                    Ok(())
                })?;
                raft_capture::apply_changeset(&store, &changeset)?;
                emitted += 1;
            }
        }

        // Walk the journal with a small limit, as a poller does.
        let mut cursor = 0i64;
        let mut walked: Vec<i64> = Vec::new();
        loop {
            let batch = store.list_scheduler_events(&SchedulerEventFilter {
                after_id: Some(cursor),
                limit: Some(2),
                ..Default::default()
            })?;
            if batch.is_empty() {
                break;
            }
            for event in batch {
                assert!(
                    event.id > cursor,
                    "the cursor must advance strictly: {} after {cursor}",
                    event.id
                );
                cursor = event.id;
                walked.push(event.id);
            }
        }
        assert_eq!(
            walked.len(),
            emitted,
            "cursor walk visited every event once"
        );
        let unique: std::collections::BTreeSet<i64> = walked.iter().copied().collect();
        assert_eq!(unique.len(), walked.len(), "no event was visited twice");
        for lane in Lane::ALL {
            assert_eq!(
                walked
                    .iter()
                    .filter(|id| id.rem_euclid(Lane::ID_STRIDE) == lane.id_residue())
                    .count(),
                4,
                "the {} lane's events must all be present",
                lane.wire_name()
            );
        }

        // The publication path (apply → project → broadcast) delivered the
        // same events, in increasing id order — what the StreamEvents
        // lag/backfill dedupe (`event.id <= last_sent`) depends on.
        let mut published: Vec<i64> = Vec::new();
        while let Ok(event) = subscriber.try_recv() {
            published.push(event.id);
        }
        assert_eq!(
            published, walked,
            "live publication order must match id order"
        );
        Ok(())
    }

    /// JC-42 end-to-end through the public emit path: ids minted outside
    /// any capture (`local_disk` mode, i.e. every non-raft caller) keep the
    /// control lane's residue, stay strictly increasing, and remain usable
    /// as `after_id` cursors.
    #[test]
    fn locally_emitted_event_ids_are_strided_and_increasing() -> R<()> {
        let store = setup_store()?;
        for index in 0..16 {
            store.emit_event(EventDraft::info("test.kind", format!("event {index}")));
        }
        let events = store.list_scheduler_events(&SchedulerEventFilter::default())?;
        assert_eq!(events.len(), 16);
        assert!(events.windows(2).all(|pair| pair[0].id < pair[1].id));
        for event in &events {
            assert_eq!(
                event.id.rem_euclid(Lane::ID_STRIDE),
                Lane::Control.id_residue(),
                "ids minted outside a capture use the control residue: {}",
                event.id
            );
        }
        // The cursor contract is ordering, not density: `after_id` returns
        // exactly the events above it even though ids are sparse.
        let after = store.list_scheduler_events(&SchedulerEventFilter {
            after_id: Some(events[3].id),
            ..Default::default()
        })?;
        assert_eq!(after.len(), 12);
        assert_eq!(after[0].id, events[4].id);
        Ok(())
    }

    #[test]
    fn allocation_changes_project_per_node_allocation_lists() -> R<()> {
        let src = setup_store()?;
        let dst = setup_store()?;
        for store in [&src, &dst] {
            store.insert_node(make_node_msg("node-a", "ready"))?;
            let job = JobConfig {
                priority: Default::default(),
                rollout: None,
                rollout_by_group: Default::default(),
                name: "proj-demo".into(),
                tasks: vec![task("web")],
                version: None,
            };
            store.handle_job_submit(
                &job,
                &[Submission {
                    path: "job.jsonnet".into(),
                    content: "job".into(),
                    is_primary: true,
                }],
            )?;
        }
        let task_id = task_id_for(&src, "web", "proj-demo")?;

        let (allocation, changeset) = raft_capture::capture_mutation(&src, Lane::Control, |s| {
            s.place_alloc("node-a", task_id)
        })?;
        assert_eq!(allocation.node_id, "node-a");
        assert!(
            src.cached_allocations_by_node()?
                .get("node-a")
                .map_or(true, |allocs| allocs.is_empty()),
            "no allocation cached during capture"
        );

        raft_capture::apply_changeset(&dst, &changeset)?;
        let by_node = dst.cached_allocations_by_node()?;
        let allocs = by_node.get("node-a").context("node-a allocations")?;
        assert_eq!(allocs.len(), 1);
        assert_eq!(
            allocs[0].id, allocation.id,
            "leader-assigned rowid replicated"
        );
        Ok(())
    }

    /// JC-40 composite-retry idempotency: when the status half of the
    /// submit composite is lost (rejected on a node that did not lead the
    /// status lane yet), the client's retry re-submits the identical config
    /// — which reports `Unchanged` — and must still reseed the task-status
    /// rows. Otherwise the composite stays half-applied forever, since no
    /// later submit of that config ever reports Created/Updated again.
    #[test]
    fn an_unchanged_resubmit_reseeds_missing_task_statuses() -> R<()> {
        let store = setup_store()?;
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "reseed-demo".into(),
            tasks: vec![task("web")],
            version: None,
        };
        let subs = [Submission {
            path: "job.jsonnet".into(),
            content: "job".into(),
            is_primary: true,
        }];
        let outcome = store.handle_job_submit(&job, &subs)?;
        let job_config_id = outcome.job_config_id().to_string();
        assert_eq!(
            store.task_statuses_for_job_config(&job_config_id)?.len(),
            1,
            "the first submit seeds the status rows"
        );

        // Simulate the lost status half: drop the rows the status-lane op
        // would have written.
        store.write_connection()?.execute(
            "DELETE FROM task_status WHERE job_config_id = ?1;",
            [&job_config_id],
        )?;
        assert!(store
            .task_statuses_for_job_config(&job_config_id)?
            .is_empty());

        // The client-style retry: same config, so the control half is a
        // no-op and reports Unchanged.
        let retry = store.handle_job_submit(&job, &subs)?;
        assert!(
            matches!(retry, JobSubmitOutcome::Unchanged { .. }),
            "an identical resubmit must report Unchanged: {retry:?}"
        );
        assert_eq!(
            store.task_statuses_for_job_config(&job_config_id)?.len(),
            1,
            "the retry must reseed the missing status rows"
        );
        Ok(())
    }

    #[test]
    fn raft_mode_write_connection_guard_blocks_unclassified_writes() -> R<()> {
        let store = setup_store()?;
        store.set_raft_mode(true);

        // Outside capture/apply: loud error.
        let err = store.write_connection().err().context("guard must trip")?;
        assert!(
            err.to_string().contains("raft mode"),
            "unexpected guard error: {err}"
        );
        // Public mutators route to the (not yet started) pipeline.
        let err = store
            .stop_job("nope")
            .err()
            .context("must not run inline")?;
        assert!(
            err.to_string().contains("replication pipeline not started"),
            "unexpected replicated error: {err}"
        );

        // Inside a capture the write connection is allowed...
        let ((), changeset) = raft_capture::capture_mutation(&store, Lane::Control, |s| {
            s.put_kv("k", "v", KvValueType::String, None, None)?;
            Ok(())
        })?;
        // ...and so is the apply path.
        raft_capture::apply_changeset(&store, &changeset)?;
        assert_eq!(kv_value(&store, "k")?.as_deref(), Some("v"));

        store.set_raft_mode(false);
        assert!(store.write_connection().is_ok(), "guard disarms");
        Ok(())
    }

    /// Adversarial regression (2026-07-25): `query_state_store --allow-write`
    /// used to report success for DDL in raft mode while replicating
    /// nothing (SQLite sessions do not capture schema changes, and the
    /// resulting empty changeset is skipped before proposal). It must fail
    /// loudly and point at migrations; row DML is still accepted.
    #[test]
    fn raft_mode_refuses_unreplicable_ddl_through_the_query_seam() -> R<()> {
        let store = setup_store()?;

        // local_disk mode: unchanged behavior — DDL runs inline.
        store.query_state_store("CREATE TABLE local_only_ddl (id INTEGER);", true)?;

        store.set_raft_mode(true);
        let err = store
            .query_state_store("CREATE TABLE raft_ddl (id INTEGER);", true)
            .err()
            .context("DDL must be refused in raft mode")?;
        let rendered = format!("{err:#}");
        assert!(rendered.contains("CREATE"), "{rendered}");
        assert!(rendered.contains("migration"), "{rendered}");

        // Row DML is replicable and still routes to the pipeline (which is
        // not started in this unit test — proving it got past the DDL gate).
        let err = store
            .query_state_store("DELETE FROM kv_entry WHERE key = 'nope';", true)
            .err()
            .context("DML must reach the replication pipeline")?;
        assert!(
            format!("{err:#}").contains("replication pipeline not started"),
            "{err:#}"
        );

        // Reads are unaffected.
        store.query_state_store("SELECT 1;", false)?;
        store.set_raft_mode(false);
        Ok(())
    }

    fn connect_task(name: &str, upstreams: Value) -> TaskConfig {
        let mut task = TaskConfig::new(name, "noop");
        task.connect = Some(
            serde_json::from_value(json!({ "upstreams": upstreams })).expect("connect config"),
        );
        task
    }

    #[test]
    fn connect_block_round_trips_through_task_config_row() -> R<()> {
        let store = setup_store()?;
        let task = connect_task(
            "api",
            json!([
                { "service": "db" },
                { "service": "cache", "localPort": 6379, "balance": "least_conn" },
            ]),
        );
        submit_service_job(&store, "svc-connect-roundtrip", vec![task])?;

        let job_config_id = store.get_job_config("svc-connect-roundtrip")?.id();
        let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
        let (_, stored) = tasks.first().context("missing stored task")?;
        let connect = stored.connect.as_ref().context("connect not persisted")?;
        assert_eq!(connect.upstreams.len(), 2);
        assert_eq!(connect.upstreams[0].service, "db");
        assert_eq!(connect.upstreams[0].local_port, None);
        assert_eq!(connect.upstreams[1].local_port, Some(6379));
        assert_eq!(connect.upstreams[1].balance.as_deref(), Some("least_conn"));
        Ok(())
    }

    #[test]
    fn upstream_assignment_gives_distinct_local_ports_per_allocation() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        let mut task = connect_task("api", json!([{ "service": "db" }]));
        task.network = Some(crate::structs::NetworkConfig {
            ports: vec![crate::structs::PortConfig {
                name: "http".into(),
                static_port: None,
                to: None,
            }],
            ..Default::default()
        });
        submit_service_job(&store, "svc-upstreams", vec![task])?;
        let task_id = task_id_for(&store, "api", "svc-upstreams")?;

        let first = store.place_alloc("node-a", task_id)?;
        let second = store.place_alloc("node-a", task_id)?;

        let local_port = |allocation: &Allocation| -> R<u16> {
            allocation
                .network
                .as_ref()
                .and_then(|network| network.upstream("db"))
                .map(|upstream| upstream.local_port)
                .context("alloc missing db upstream")
        };
        let first_local = local_port(&first)?;
        let second_local = local_port(&second)?;
        assert_ne!(first_local, second_local);
        assert!(first_local >= crate::structs::DYNAMIC_PORT_MIN);
        assert!(second_local <= crate::structs::DYNAMIC_PORT_MAX);

        // Upstream ports come from the same per-node set as task ports, so
        // they never land on a port already handed to a task.
        let http_ports: HashSet<u16> = [&first, &second]
            .iter()
            .filter_map(|allocation| {
                allocation
                    .network
                    .as_ref()
                    .and_then(|network| network.port("http"))
                    .map(|port| port.host_port)
            })
            .collect();
        assert_eq!(http_ports.len(), 2);
        assert!(!http_ports.contains(&first_local));
        assert!(!http_ports.contains(&second_local));

        // The resolved strategy is always concrete on the allocation.
        assert_eq!(
            first
                .network
                .as_ref()
                .and_then(|network| network.upstream("db"))
                .map(|upstream| upstream.balance.as_str()),
            Some(crate::structs::DEFAULT_BALANCE)
        );
        Ok(())
    }

    #[test]
    fn upstream_static_local_port_conflict_fails_placement() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        let task = connect_task("api", json!([{ "service": "cache", "localPort": 6379 }]));
        submit_service_job(&store, "svc-upstream-static", vec![task])?;
        let task_id = task_id_for(&store, "api", "svc-upstream-static")?;

        let first = store.place_alloc("node-a", task_id)?;
        assert_eq!(
            first
                .network
                .as_ref()
                .and_then(|network| network.upstream("cache"))
                .map(|upstream| upstream.local_port),
            Some(6379)
        );

        let err = store
            .place_alloc("node-a", task_id)
            .expect_err("second pinned upstream on the same node must conflict");
        assert!(err.to_string().contains("failed to assign ports"), "{err}");
        assert!(
            format!("{err:#}").contains("static local port 6379"),
            "{err:#}"
        );
        Ok(())
    }

    #[test]
    fn tasks_without_connect_get_no_upstreams_on_the_allocation() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;
        submit_service_job(&store, "svc-no-connect", vec![service_task("web")])?;
        let task_id = task_id_for(&store, "web", "svc-no-connect")?;

        let allocation = store.place_alloc("node-a", task_id)?;
        let network = allocation.network.as_ref().context("missing network")?;
        assert!(network.upstreams.is_empty());
        // And the stored JSON stays byte-identical to pre-connect rows.
        let encoded = serde_json::to_value(network)?;
        assert!(encoded.get("upstreams").is_none());
        Ok(())
    }

    #[test]
    fn adding_connect_to_a_running_job_forces_fresh_placement() -> R<()> {
        // Upstream loopback ports are assigned at placement and frozen on the
        // allocation, so a `connect` change must never take the in-place
        // migration path: migrated allocations keep their old (empty) network
        // and the task never sees `LEGION_UPSTREAM_*`.
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", "ready"))?;

        submit_service_job(
            &store,
            "svc-connect-added",
            vec![TaskConfig::new("api", "noop")],
        )?;
        let v1_id = store.get_job_config("svc-connect-added")?.id();
        let v1_task = task_id_for(&store, "api", "svc-connect-added")?;
        let before = store.place_alloc("node-a", v1_task)?;
        assert!(before
            .network
            .as_ref()
            .is_none_or(|network| network.upstreams.is_empty()));

        // Only change: a connect block.
        submit_service_job(
            &store,
            "svc-connect-added",
            vec![connect_task("api", json!([{ "service": "db" }]))],
        )?;
        let v2_id = store.get_job_config("svc-connect-added")?.id();
        assert_ne!(v1_id, v2_id);
        assert!(
            !store.job_configs_schedule_equivalent(&v1_id, &v2_id)?,
            "adding connect must not be treated as schedule-equivalent"
        );

        // …and the fresh placement really does carry an upstream port.
        let v2_task = task_id_for_config(&store, "api", &v2_id)?;
        let after = store.place_alloc("node-a", v2_task)?;
        let upstream = after
            .network
            .as_ref()
            .and_then(|network| network.upstream("db"))
            .context("fresh allocation missing db upstream")?;
        assert!(upstream.local_port >= crate::structs::DYNAMIC_PORT_MIN);
        assert_eq!(upstream.balance, crate::structs::DEFAULT_BALANCE);

        // Changing an existing connect block is equally disqualifying, while
        // an unrelated edit still migrates in place.
        submit_service_job(
            &store,
            "svc-connect-added",
            vec![connect_task(
                "api",
                json!([{ "service": "db", "balance": "random" }]),
            )],
        )?;
        let v3_id = store.get_job_config("svc-connect-added")?.id();
        assert!(!store.job_configs_schedule_equivalent(&v2_id, &v3_id)?);

        let mut unchanged_connect =
            connect_task("api", json!([{ "service": "db", "balance": "random" }]));
        unchanged_connect.config = json!({ "changed": true });
        submit_service_job(&store, "svc-connect-added", vec![unchanged_connect])?;
        let v4_id = store.get_job_config("svc-connect-added")?.id();
        assert!(
            store.job_configs_schedule_equivalent(&v3_id, &v4_id)?,
            "an edit that leaves connect alone still migrates in place"
        );
        Ok(())
    }
}
