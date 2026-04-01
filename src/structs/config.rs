use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const DEF_SCHEDULER_AUTO_ANTI_AFFINITY_WEIGHT: f64 = 0.2;

use crate::prelude::{
    DEF_EVAL_PRIORITY, DEF_MANAGER_HOSTNAME, DEF_MANAGER_INSECURE_PORT, DEF_MANAGER_SERVER_PORT,
    DEF_ROLLOUT_RETENTION_DAYS, DEF_SCHEDULING_POLICY, DEF_STORE_PATH, DEF_WORKER_CHECK_SEC,
    DEF_WORKER_CONFIG_PATH,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    pub port: Option<u64>,
    pub insecure_port: Option<u64>,
    pub host: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ManagerConfig {
    pub server: Option<ServerConfig>,
    pub store_path: Option<String>,
    pub worker_config_path: Option<String>,
    pub worker_check_seconds: Option<u64>,
    pub eval_threads: Option<usize>,
    pub scheduling_policy: Option<String>,
    pub default_priority: Option<i64>,
    pub rollout_retention_days: Option<u64>,
    pub scheduler_settings: Option<Value>,
    pub task_resource_defaults: Option<Value>,
    pub custom_resources: Option<Value>,
    pub persistence: Option<PersistenceConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceType {
    #[default]
    LocalDisk,
    Streaming,
    Raft,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct PersistenceConfig {
    #[serde(default)]
    pub r#type: PersistenceType,
    pub streaming: Option<StreamingPersistenceConfig>,
    pub raft: Option<RaftPersistenceConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RaftPersistenceConfig {
    pub node_id: Option<String>,
    pub bootstrap: Option<bool>,
    pub peers: Option<Vec<String>>,
    pub join_token_ttl_seconds: Option<u64>,
}

impl Default for RaftPersistenceConfig {
    fn default() -> Self {
        Self {
            node_id: None,
            bootstrap: Some(false),
            peers: Some(Vec::new()),
            join_token_ttl_seconds: Some(900),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfigFull {
    pub port: u64,
    pub insecure_port: u64,
    pub host: String,
}

impl From<ServerConfigFull> for ServerConfig {
    fn from(val: ServerConfigFull) -> Self {
        ServerConfig {
            port: Some(val.port),
            insecure_port: Some(val.insecure_port),
            host: Some(val.host),
        }
    }
}

impl ServerConfig {
    pub fn with_defaults(&self) -> ServerConfigFull {
        ServerConfigFull {
            port: self.port.unwrap_or(DEF_MANAGER_SERVER_PORT),
            insecure_port: self.insecure_port.unwrap_or(DEF_MANAGER_INSECURE_PORT),
            host: self
                .host
                .clone()
                .unwrap_or_else(|| DEF_MANAGER_HOSTNAME.to_string()),
        }
    }

    pub fn full_default() -> ServerConfigFull {
        ServerConfigFull {
            port: DEF_MANAGER_SERVER_PORT,
            insecure_port: DEF_MANAGER_INSECURE_PORT,
            host: DEF_MANAGER_HOSTNAME.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ManagerConfigFull {
    pub server: ServerConfigFull,
    pub store_path: String,
    pub worker_config_path: String,
    pub worker_check_seconds: u64,
    #[serde(default = "default_eval_threads")]
    pub eval_threads: usize,
    pub scheduling_policy: String,
    pub default_priority: i64,
    pub rollout_retention_days: u64,
    pub scheduler_settings: Option<Value>,
    pub task_resource_defaults: Option<Value>,
    pub custom_resources: Option<Value>,
    #[serde(default)]
    pub persistence: PersistenceConfigFull,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PersistenceConfigFull {
    pub r#type: PersistenceType,
    pub streaming: StreamingPersistenceConfigFull,
    pub raft: RaftPersistenceConfigFull,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct StreamingPersistenceConfig {
    pub litestream_binary: Option<String>,
    pub replica_url: Option<String>,
    pub sync_interval: Option<String>,
    pub lease: Option<StreamingLeaseConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StreamingPersistenceConfigFull {
    pub litestream_binary: String,
    pub replica_url: String,
    pub sync_interval: String,
    pub lease: StreamingLeaseConfigFull,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct RaftPersistenceConfigFull {
    pub node_id: String,
    pub bootstrap: bool,
    pub peers: Vec<String>,
    pub join_token_ttl_seconds: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum StreamingLeaseProvider {
    #[default]
    None,
    ObjectStorage,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct StreamingLeaseConfig {
    #[serde(default)]
    pub provider: StreamingLeaseProvider,
    pub key: Option<String>,
    pub ttl_seconds: Option<u64>,
    pub renew_interval_seconds: Option<u64>,
    pub object_storage: Option<StreamingObjectStorageLeaseConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StreamingLeaseConfigFull {
    pub provider: StreamingLeaseProvider,
    pub key: String,
    pub ttl_seconds: u64,
    pub renew_interval_seconds: u64,
    pub object_storage: StreamingObjectStorageLeaseConfigFull,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct StreamingObjectStorageLeaseConfig {
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StreamingObjectStorageLeaseConfigFull {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

impl From<ManagerConfigFull> for ManagerConfig {
    fn from(val: ManagerConfigFull) -> Self {
        ManagerConfig {
            server: Some(val.server.into()),
            store_path: Some(val.store_path),
            worker_config_path: Some(val.worker_config_path),
            worker_check_seconds: Some(val.worker_check_seconds),
            eval_threads: Some(val.eval_threads),
            scheduling_policy: Some(val.scheduling_policy),
            default_priority: Some(val.default_priority),
            rollout_retention_days: Some(val.rollout_retention_days),
            scheduler_settings: val.scheduler_settings,
            task_resource_defaults: val.task_resource_defaults,
            custom_resources: val.custom_resources,
            persistence: Some(val.persistence.into()),
        }
    }
}

impl ManagerConfig {
    pub fn with_defaults(&self) -> ManagerConfigFull {
        let server_config = self
            .server
            .as_ref()
            .map(ServerConfig::with_defaults)
            .unwrap_or_else(ServerConfig::full_default);

        ManagerConfigFull {
            server: server_config,
            store_path: self
                .store_path
                .clone()
                .unwrap_or_else(|| DEF_STORE_PATH.into()),
            worker_config_path: self
                .worker_config_path
                .clone()
                .unwrap_or_else(|| DEF_WORKER_CONFIG_PATH.into()),
            worker_check_seconds: self.worker_check_seconds.unwrap_or(DEF_WORKER_CHECK_SEC),
            eval_threads: self.eval_threads.unwrap_or_else(default_eval_threads),
            scheduling_policy: self
                .scheduling_policy
                .clone()
                .unwrap_or_else(|| DEF_SCHEDULING_POLICY.into()),
            default_priority: self.default_priority.unwrap_or(DEF_EVAL_PRIORITY),
            rollout_retention_days: self
                .rollout_retention_days
                .unwrap_or(DEF_ROLLOUT_RETENTION_DAYS),
            scheduler_settings: self.scheduler_settings.clone(),
            task_resource_defaults: self.task_resource_defaults.clone(),
            custom_resources: self.custom_resources.clone(),
            persistence: self
                .persistence
                .as_ref()
                .map(PersistenceConfig::with_defaults)
                .unwrap_or_default(),
        }
    }
}

impl Default for ManagerConfigFull {
    fn default() -> Self {
        ManagerConfigFull {
            server: ServerConfig::full_default(),
            store_path: DEF_STORE_PATH.into(),
            worker_config_path: DEF_WORKER_CONFIG_PATH.into(),
            worker_check_seconds: DEF_WORKER_CHECK_SEC,
            scheduling_policy: DEF_SCHEDULING_POLICY.into(),
            default_priority: DEF_EVAL_PRIORITY,
            rollout_retention_days: DEF_ROLLOUT_RETENTION_DAYS,
            scheduler_settings: None,
            task_resource_defaults: None,
            custom_resources: None,
            eval_threads: default_eval_threads(),
            persistence: PersistenceConfigFull::default(),
        }
    }
}

impl Default for PersistenceConfigFull {
    fn default() -> Self {
        Self {
            r#type: PersistenceType::LocalDisk,
            streaming: StreamingPersistenceConfigFull::default(),
            raft: RaftPersistenceConfigFull::default(),
        }
    }
}

impl PersistenceConfig {
    pub fn with_defaults(&self) -> PersistenceConfigFull {
        PersistenceConfigFull {
            r#type: self.r#type.clone(),
            streaming: self
                .streaming
                .as_ref()
                .map(StreamingPersistenceConfig::with_defaults)
                .unwrap_or_default(),
            raft: self
                .raft
                .as_ref()
                .map(RaftPersistenceConfig::with_defaults)
                .unwrap_or_default(),
        }
    }
}

impl Default for StreamingPersistenceConfigFull {
    fn default() -> Self {
        Self {
            litestream_binary: "litestream".to_string(),
            replica_url: String::new(),
            sync_interval: "1s".to_string(),
            lease: StreamingLeaseConfigFull::default(),
        }
    }
}

impl StreamingPersistenceConfig {
    pub fn with_defaults(&self) -> StreamingPersistenceConfigFull {
        StreamingPersistenceConfigFull {
            litestream_binary: self
                .litestream_binary
                .clone()
                .unwrap_or_else(|| "litestream".to_string()),
            replica_url: self.replica_url.clone().unwrap_or_default(),
            sync_interval: self
                .sync_interval
                .clone()
                .unwrap_or_else(|| "1s".to_string()),
            lease: self
                .lease
                .as_ref()
                .map(StreamingLeaseConfig::with_defaults)
                .unwrap_or_default(),
        }
    }
}

impl RaftPersistenceConfig {
    pub fn with_defaults(&self) -> RaftPersistenceConfigFull {
        RaftPersistenceConfigFull {
            node_id: self.node_id.clone().unwrap_or_default(),
            bootstrap: self.bootstrap.unwrap_or(false),
            peers: self.peers.clone().unwrap_or_default(),
            join_token_ttl_seconds: self.join_token_ttl_seconds.unwrap_or(900),
        }
    }
}

impl Default for StreamingLeaseConfigFull {
    fn default() -> Self {
        Self {
            provider: StreamingLeaseProvider::None,
            key: "leases/default/primary.json".to_string(),
            ttl_seconds: 15,
            renew_interval_seconds: 5,
            object_storage: StreamingObjectStorageLeaseConfigFull::default(),
        }
    }
}

impl StreamingLeaseConfig {
    pub fn with_defaults(&self) -> StreamingLeaseConfigFull {
        StreamingLeaseConfigFull {
            provider: self.provider.clone(),
            key: self
                .key
                .clone()
                .unwrap_or_else(|| "leases/default/primary.json".to_string()),
            ttl_seconds: self.ttl_seconds.unwrap_or(15),
            renew_interval_seconds: self.renew_interval_seconds.unwrap_or(5),
            object_storage: self
                .object_storage
                .as_ref()
                .map(StreamingObjectStorageLeaseConfig::with_defaults)
                .unwrap_or_default(),
        }
    }

    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.ttl_seconds.unwrap_or(15))
    }
}

impl Default for StreamingObjectStorageLeaseConfigFull {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            region: "auto".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        }
    }
}

impl StreamingObjectStorageLeaseConfig {
    pub fn with_defaults(&self) -> StreamingObjectStorageLeaseConfigFull {
        StreamingObjectStorageLeaseConfigFull {
            bucket: self.bucket.clone().unwrap_or_default(),
            region: self.region.clone().unwrap_or_else(|| "auto".to_string()),
            endpoint: self.endpoint.clone(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
        }
    }
}

impl From<PersistenceConfigFull> for PersistenceConfig {
    fn from(value: PersistenceConfigFull) -> Self {
        Self {
            r#type: value.r#type,
            streaming: Some(value.streaming.into()),
            raft: Some(value.raft.into()),
        }
    }
}

impl From<StreamingPersistenceConfigFull> for StreamingPersistenceConfig {
    fn from(value: StreamingPersistenceConfigFull) -> Self {
        Self {
            litestream_binary: Some(value.litestream_binary),
            replica_url: Some(value.replica_url),
            sync_interval: Some(value.sync_interval),
            lease: Some(value.lease.into()),
        }
    }
}

impl From<RaftPersistenceConfigFull> for RaftPersistenceConfig {
    fn from(value: RaftPersistenceConfigFull) -> Self {
        Self {
            node_id: Some(value.node_id),
            bootstrap: Some(value.bootstrap),
            peers: Some(value.peers),
            join_token_ttl_seconds: Some(value.join_token_ttl_seconds),
        }
    }
}

impl From<StreamingLeaseConfigFull> for StreamingLeaseConfig {
    fn from(value: StreamingLeaseConfigFull) -> Self {
        Self {
            provider: value.provider,
            key: Some(value.key),
            ttl_seconds: Some(value.ttl_seconds),
            renew_interval_seconds: Some(value.renew_interval_seconds),
            object_storage: Some(value.object_storage.into()),
        }
    }
}

impl From<StreamingObjectStorageLeaseConfigFull> for StreamingObjectStorageLeaseConfig {
    fn from(value: StreamingObjectStorageLeaseConfigFull) -> Self {
        Self {
            bucket: Some(value.bucket),
            region: Some(value.region),
            endpoint: value.endpoint,
            access_key_id: value.access_key_id,
            secret_access_key: value.secret_access_key,
        }
    }
}

fn default_eval_threads() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct WorkerConfigAlt {
    pub worker_type: String,
    pub additional_node_checks: Option<Vec<String>>,
    pub pool: Option<String>,
    pub attributes: Option<HashMap<String, String>>,
    pub name_prefix: Option<String>,
    pub resources: Option<Value>,
    pub reserved_resources: Option<Value>,
    pub max_allocations: Option<u32>,
}
