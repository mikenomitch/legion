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
// Amount of time since last heartbeat before a node is considered lost
pub const DEF_LOST_NODE_AT_SEC: i64 = 10;

// === Worker ===
// How often the worker should ping the manager
pub const WORKER_HEARTBEAT_DURATION_SEC: u64 = 2;
// How often the worker should update node to manager
pub const WORKER_NODE_CHECK_DURATION_SEC: u64 = 30;
pub const DEF_WORKER_SERVER_PORT: u64 = 4848;
pub const DEF_WORKER_STATE_PATH: &str = ".legion/worker_state.json";
