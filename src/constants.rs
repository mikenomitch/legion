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
pub const DEF_CONFIG_PATH: &str = "./configs/manager.jsonnet";

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
// Amount of time since last heartbeat before a node is considered lost
pub const DEF_LOST_NODE_AT_SEC: i64 = 10;
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
