pub const TMP_SECRET_KEY: [u8; 6] = *b"secret";

// === Manager ===
pub const DEF_CONFIG_PATH: &str = "./configs/manager.jsonnet";

// Manager config file
pub const DEF_WORKER_CONFIG_PATH: &str = "./configs/workers/";
pub const DEF_STORE_PATH: &str = ".legion/store.sqlite";
pub const DEF_MANAGER_HOSTNAME: &str = "127.0.0.1";
pub const DEF_MANAGER_SERVER_PORT: u64 = 4747;
pub const DEF_MANAGER_INSECURE_PORT: u64 = 4748;
pub const DEF_MANAGER_ADDRESS: &str = "http://127.0.0.1:4747";
pub const DEF_INSECURE_ADDR: &str = "http://127.0.0.1:4748";
// Loop time in manager rechecking nodes
pub const DEF_WORKER_CHECK_SEC: u64 = 2;
pub const DEF_SCHEDULING_POLICY: &str = "bin_pack";
pub const DEF_EVAL_PRIORITY: i64 = 50;
pub const DEF_ROLLOUT_RETENTION_DAYS: u64 = 30;
// Amount of time since last heartbeat before a node is considered lost
pub const DEF_LOST_NODE_AT_SEC: i64 = 10;

// === Worker ===
// How often the worker should ping the manager
pub const WORKER_HEARTBEAT_DURATION_SEC: u64 = 2;
// How often the worker should update node to manager
pub const WORKER_NODE_CHECK_DURATION_SEC: u64 = 30;
pub const DEF_WORKER_SERVER_PORT: u64 = 4848;
pub const DEF_WORKER_STATE_DIR: &str = ".legion/workers";
pub const DEF_WORKER_STATE_PATH: &str = ".legion/workers/default/worker_state.json";

// Default per-stream cap (bytes) when reading captured task logs and no explicit
// limit is requested. 256 KiB keeps replies small enough for an agent context.
pub const DEF_TASK_LOG_MAX_BYTES: i64 = 256 * 1024;

// Task log rotation/retention defaults (worker-side). These keep a chatty task
// from filling a worker's disk while preserving a useful recent window. All are
// overridable via env vars (LEGION_LOG_MAX_FILE_BYTES / LEGION_LOG_MAX_FILES /
// LEGION_LOG_RETENTION_DAYS).
pub const DEF_TASK_LOG_MAX_FILE_BYTES: u64 = 10 * 1024 * 1024; // 10 MiB per stream
pub const DEF_TASK_LOG_MAX_FILES: u32 = 5; // rotated generations kept per stream
pub const DEF_TASK_LOG_RETENTION_DAYS: u64 = 7; // age-based prune of rotated files
pub const DEF_TASK_LOG_MAINTENANCE_SEC: u64 = 10; // rotation/retention sweep cadence
