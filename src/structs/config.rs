use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::prelude::{
    DEF_EVAL_PRIORITY, DEF_MANAGER_HOSTNAME, DEF_MANAGER_INSECURE_PORT, DEF_MANAGER_SERVER_PORT,
    DEF_SCHEDULING_POLICY, DEF_STORE_PATH, DEF_WORKER_CHECK_SEC, DEF_WORKER_CONFIG_PATH,
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
    pub scheduler_settings: Option<Value>,
    pub task_resource_defaults: Option<Value>,
    pub custom_resources: Option<Value>,
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
    pub scheduler_settings: Option<Value>,
    pub task_resource_defaults: Option<Value>,
    pub custom_resources: Option<Value>,
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
            scheduler_settings: val.scheduler_settings,
            task_resource_defaults: val.task_resource_defaults,
            custom_resources: val.custom_resources,
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
            scheduler_settings: self.scheduler_settings.clone(),
            task_resource_defaults: self.task_resource_defaults.clone(),
            custom_resources: self.custom_resources.clone(),
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
            scheduler_settings: None,
            task_resource_defaults: None,
            custom_resources: None,
            eval_threads: default_eval_threads(),
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
