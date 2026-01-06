use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::prelude::{
    DEF_MANAGER_HOSTNAME, DEF_MANAGER_INSECURE_PORT, DEF_MANAGER_SERVER_PORT,
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
    pub scheduling_policy: Option<String>,
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
    pub scheduling_policy: String,
}

impl From<ManagerConfigFull> for ManagerConfig {
    fn from(val: ManagerConfigFull) -> Self {
        ManagerConfig {
            server: Some(val.server.into()),
            store_path: Some(val.store_path),
            worker_config_path: Some(val.worker_config_path),
            worker_check_seconds: Some(val.worker_check_seconds),
            scheduling_policy: Some(val.scheduling_policy),
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
            scheduling_policy: self
                .scheduling_policy
                .clone()
                .unwrap_or_else(|| DEF_SCHEDULING_POLICY.into()),
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
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct WorkerConfigAlt {
    pub worker_type: String,
    pub additional_node_checks: Option<Vec<String>>,
    pub pool: Option<String>,
    pub attributes: Option<HashMap<String, String>>,
    pub name_prefix: Option<String>,
}
