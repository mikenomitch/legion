// === Protocol ===
// The worker<->manager wire-protocol version, exchanged during worker
// registration (`WorkerRegister`). Bump this ONLY for breaking wire-contract
// changes to worker-plane RPCs (registration, heartbeats, health checks,
// work dispatch): removed/renamed fields, changed field semantics, or new
// required behavior an old peer cannot satisfy. Additive, backwards-compatible
// fields do NOT bump this. Managers accept workers at or below their own
// version (workers reporting 0 are pre-versioning builds and are accepted
// with a warning); they reject newer workers so operators upgrade managers
// before workers. See docs/upgrades.md for the full skew policy.
pub const PROTOCOL_VERSION: u32 = 2;

// === Worker → task-runtime environment ===
// Task runtimes are in-process dylibs with no config channel of their own, so
// cross-cutting worker settings reach them as environment variables set by the
// worker daemon at startup.
/// Set to `1` when the worker enforces `resources` as cgroup limits.
pub const ENV_RESOURCE_ENFORCEMENT: &str = "LEGION_RESOURCE_ENFORCEMENT";
/// cgroup v2 parent path (relative to /sys/fs/cgroup) for worker-owned cgroups.
pub const ENV_CGROUP_PARENT: &str = "LEGION_CGROUP_PARENT";

// === Task filesystem ===
/// Where a runtime with an isolated filesystem mounts the allocation's host
/// task directory, and therefore the root every file-delivery destination on
/// such a task must resolve under (`kvInjections.files[].path`, identity
/// `file.path`, staged artifacts).
///
/// A constant rather than a per-job setting on purpose: jobs grep for it and
/// hardcode it, so it can never comfortably change. `/legion/task` is
/// unmistakably Legion's and leaves room for siblings (`/legion/secrets`) if
/// the deferred directory splits ever land. See docs/task-runtime-container.md.
pub const CONTAINER_TASK_DIR: &str = "/legion/task";

// === Allocations ===
// Allocation row status values. An allocation is `placed` from the moment the
// scheduler writes its row until a worker reports it terminal; `complete` and
// `failed` are the terminal states a worker's exit report produces (see
// docs/task-supervision.md). Terminal allocations keep their row — the slot is
// still spent — but are never dispatched back to a worker.
pub const ALLOCATION_STATUS_PLACED: &str = "placed";
pub const ALLOCATION_STATUS_COMPLETE: &str = "complete";
pub const ALLOCATION_STATUS_FAILED: &str = "failed";

/// Whether an allocation row is in a terminal state.
pub fn allocation_status_is_terminal(status: &str) -> bool {
    matches!(
        status,
        ALLOCATION_STATUS_COMPLETE | ALLOCATION_STATUS_FAILED
    )
}

// === Manager ===
pub const DEF_CONFIG_PATH: &str = "./configs/manager.ncl";

// Manager config file
pub const DEF_WORKER_CONFIG_PATH: &str = "./configs/workers/";
pub const DEF_STORE_PATH: &str = ".legion/store.sqlite";
pub const DEF_MANAGER_HOSTNAME: &str = "127.0.0.1";
pub const DEF_MANAGER_SERVER_PORT: u64 = 4747;
pub const DEF_MANAGER_INSECURE_PORT: u64 = 4748;
pub const DEF_MANAGER_ADDRESS: &str = "http://127.0.0.1:4747";
// Loop time in manager rechecking nodes
pub const DEF_WORKER_CHECK_SEC: u64 = 2;
pub const DEF_SCHEDULING_POLICY: &str = "bin_pack";
pub const DEF_EVAL_PRIORITY: i64 = 50;
pub const DEF_ROLLOUT_RETENTION_DAYS: u64 = 30;
// Debounce window (ms) for KV-driven job reconciliation: rapid puts/deletes
// to watched keys are coalesced and applied once per quiet window. 0 applies
// every KV write synchronously (see docs/kv-store.md).
pub const DEF_KV_DEBOUNCE_MS: u64 = 500;
// Amount of time since last heartbeat before a node is considered lost.
//
// No longer a global cutoff: a node's window is `missedBeatsBeforeLost x that
// node's heartbeat interval` (heartbeat-cadence plan §6.4), recorded per node
// in `node_health.lost_after_seconds`. This constant is what that arithmetic
// reproduces at the defaults (5 x 2s) and the fallback for a row written
// before the column existed. See docs/heartbeat-cadence-and-scale.md.
pub const DEF_LOST_NODE_AT_SEC: i64 = 10;

// === Heartbeat cadence and node liveness ===
// (docs/heartbeat-cadence-and-scale.md, docs/node-liveness.md)
/// Missed heartbeats before a node is marked lost. The lost window is ALWAYS
/// this times the node's own heartbeat interval, so `5 x 2s` reproduces the
/// historical fixed 10s exactly.
pub const DEF_MISSED_BEATS_BEFORE_LOST: u32 = 5;
/// Floor on a manager-served heartbeat interval, whatever the fleet size.
pub const DEF_HEARTBEAT_MIN_INTERVAL_SEC: u64 = 2;
/// Ceiling on a manager-served interval: past this the manager stops backing
/// off and warns instead, because the answer is more control plane.
pub const DEF_HEARTBEAT_MAX_INTERVAL_SEC: u64 = 120;
/// Headroom factor in the sizing formula `heartbeatSeconds >= H x N / R`.
/// 2 means "run at half the measured ceiling so a burst is not an outage".
pub const DEF_HEARTBEAT_HEADROOM: f64 = 2.0;
/// Upper clamp on a worker type's `heartbeatSeconds`. Widened from the
/// historical 30 once the lost window began moving with the cadence.
pub const MAX_WORKER_HEARTBEAT_SECONDS: u32 = 300;
/// Freshness ratio below which the manager warns that the fleet is not
/// sustaining its cadence (`legion_heartbeat_freshness_ratio`).
pub const HEARTBEAT_FRESHNESS_WARN_RATIO: f64 = 0.5;
/// Consecutive sub-threshold ticks before the freshness warning fires, and
/// how often it may repeat. Debounced so a single slow tick is not a page.
pub const HEARTBEAT_FRESHNESS_WARN_TICKS: u32 = 3;
pub const HEARTBEAT_FRESHNESS_WARN_INTERVAL_SEC: u64 = 60;
/// Fleets smaller than this have too few nodes for the freshness ratio (and
/// the mass-disruption circuit breaker) to mean anything.
pub const HEARTBEAT_FRESHNESS_MIN_FLEET: usize = 20;
/// Fraction of the active fleet that, if it would be marked lost in ONE tick,
/// means the control plane broke rather than the fleet. Mark none, log error.
pub const LIVENESS_MASS_DISRUPTION_FRACTION: f64 = 0.5;
/// After acquiring status-lane leadership (or starting the process), mark
/// nothing lost for this long: a new leader inherits no liveness state and
/// rebuilds it from incoming heartbeats (Nomad's `failover_heartbeat_ttl`).
pub const DEF_LIVENESS_FAILOVER_GRACE_SEC: u64 = 60;
/// How often a manager that does NOT lead the status lane reports the nodes
/// it heard from to the leader. One RPC per manager per interval.
pub const DEF_LIVENESS_DIGEST_INTERVAL_SEC: u64 = 1;
/// How long the leader keeps trusting a silent peer manager's last digest.
/// Nodes that peer last claimed are not eligible to be marked lost inside it.
pub const DEF_LIVENESS_PEER_DIGEST_GRACE_SEC: u64 = 30;
/// How often the status-lane leader replicates a batched heartbeat
/// checkpoint so every manager can show an approximate last-heartbeat time.
/// Display only — a checkpoint is never read back as liveness. 0 disables.
pub const DEF_LIVENESS_CHECKPOINT_INTERVAL_SEC: u64 = 30;
/// Smallest non-zero `checkpointInterval`; anything faster re-creates the
/// per-heartbeat commit load the liveness plan exists to remove.
pub const MIN_LIVENESS_CHECKPOINT_INTERVAL_SEC: u64 = 5;
// Cluster region stamped into workload-identity claims/sub. Legion has no
// multi-region concept yet; "auto" matches the documented default subject
// shape (see docs/workload-identity.md).
pub const DEF_CLUSTER_REGION: &str = "auto";

// === Worker ===
// How often the worker should ping the manager
pub const WORKER_HEARTBEAT_DURATION_SEC: u64 = 2;
// How often the worker should update node to manager
pub const WORKER_NODE_CHECK_DURATION_SEC: u64 = 30;
pub const DEF_WORKER_SERVER_PORT: u64 = 4848;
pub const DEF_WORKER_STATE_DIR: &str = ".legion/workers";
pub const DEF_WORKER_STATE_PATH: &str = ".legion/workers/default/worker_state.json";

// How long the manager keeps a stop notice (why an allocation was removed)
// before pruning it. Notices are advisory metadata delivered on the next
// heartbeat — at a 2s cadence a worker sees one within one tick, so this is
// slack for a partitioned or restarting worker, not a delivery window. See
// `docs/graceful-stop.md`.
pub const STOP_NOTICE_RETENTION_SECS: i64 = 600;

// Default per-stream cap (bytes) when reading captured task logs and no explicit
// limit is requested. 256 KiB keeps replies small enough for an agent context.
pub const DEF_TASK_LOG_MAX_BYTES: i64 = 256 * 1024;

// Absolute per-stream ceiling for a single log read. An explicit `max_bytes`
// larger than this is clamped so a buggy/hostile client cannot drive a huge
// buffer allocation (OOM/DoS) even though the read API is otherwise unbounded.
pub const MAX_TASK_LOG_READ_BYTES: u64 = 16 * 1024 * 1024;

// Task log rotation/retention defaults (worker-side). These keep a chatty task
// from filling a worker's disk while preserving a useful recent window. All are
// overridable via env vars (LEGION_LOG_MAX_FILE_BYTES / LEGION_LOG_MAX_FILES /
// LEGION_LOG_RETENTION_DAYS).
pub const DEF_TASK_LOG_MAX_FILE_BYTES: u64 = 10 * 1024 * 1024; // 10 MiB per stream
pub const DEF_TASK_LOG_MAX_FILES: u32 = 5; // rotated generations kept per stream
pub const DEF_TASK_LOG_RETENTION_DAYS: u64 = 7; // age-based prune of rotated files
pub const DEF_TASK_LOG_MAINTENANCE_SEC: u64 = 10; // rotation/retention sweep cadence
