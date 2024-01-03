use crate::constants::{
    DEF_LOST_NODE_AT_SEC, DEF_MANAGER_HOSTNAME, DEF_MANAGER_INSECURE_PORT, DEF_MANAGER_SERVER_PORT,
    DEF_STORE_PATH, DEF_WORKER_CHECK_SEC, DEF_WORKER_CONFIG_PATH,
};
use crate::util::R;

use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_string, Map, Value};
use std::collections::HashMap;
use time::macros::format_description;

tonic::include_proto!("legion");

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
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfigFull {
    pub port: u64,
    pub insecure_port: u64,
    pub host: String,
}

impl Into<ServerConfig> for ServerConfigFull {
    fn into(self) -> ServerConfig {
        ServerConfig {
            port: Some(self.port),
            insecure_port: Some(self.insecure_port),
            host: Some(self.host),
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
}

impl Into<ManagerConfig> for ManagerConfigFull {
    fn into(self) -> ManagerConfig {
        ManagerConfig {
            server: Some(self.server.into()),
            store_path: Some(self.store_path),
            worker_config_path: Some(self.worker_config_path),
            worker_check_seconds: Some(self.worker_check_seconds),
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

/// Our claims struct, it needs to derive `Serialize` and/or `Deserialize`
#[derive(Debug, Serialize, Deserialize)]
pub struct JoinTokenClaims {
    pub exp: usize,
    pub sub: String,
    pub address: String,
    pub server_config: ServerConfig,
    pub worker_type: String,
    pub skip_expiry_validation: bool,
    // pub cert: String
    // pub key: String
    // pub ca_cert: String
}
// aud: String,         // Optional. Audience
// iat: usize,          // Optional. Issued at (as UTC timestamp)
// iss: String,         // Optional. Issuer
// nbf: usize,          // Optional. Not Before (as UTC timestamp)

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub id: String,
    pub status: String,
    pub pool: String,
    pub availability: String,
    pub last_heartbeat: String,
    pub config_attrs: Value,
    pub fingerprint_attrs: Value,
    pub dynamic_attrs: Value,
    pub health_checks: Value,
}

impl Into<NodeMsg> for Node {
    fn into(self) -> NodeMsg {
        let config_str = to_string(&self.config_attrs).unwrap_or("{}".to_string());
        let hc_str = to_string(&self.health_checks).unwrap_or("{}".to_string());
        let fp_str = to_string(&self.fingerprint_attrs).unwrap_or("{}".to_string());
        let da_str = to_string(&self.dynamic_attrs).unwrap_or("{}".to_string());

        NodeMsg {
            id: self.id,
            status: self.status,
            pool: self.pool,
            availability: self.availability,
            last_heartbeat: self.last_heartbeat,
            config_attrs: config_str,
            fingerprint_attrs: fp_str,
            dynamic_attrs: da_str,
            health_checks: hc_str,
        }
    }
}

impl Into<Node> for NodeMsg {
    fn into(self) -> Node {
        let config_map = from_str::<Value>(&self.config_attrs).unwrap_or(Value::Null);
        let fp_map = from_str::<Value>(&self.fingerprint_attrs).unwrap_or(Value::Null);
        let hc_map = from_str::<Value>(&self.health_checks).unwrap_or(Value::Null);
        let da_map = from_str::<Value>(&self.dynamic_attrs).unwrap_or(Value::Null);

        Node {
            id: self.id,
            status: self.status,
            pool: self.pool,
            availability: self.availability,
            last_heartbeat: self.last_heartbeat,
            config_attrs: config_map,
            fingerprint_attrs: fp_map,
            dynamic_attrs: da_map,
            health_checks: hc_map,
        }
    }
}

impl Node {
    pub fn last_heartbeat_too_slow(last_heartbeat: &str) -> R<bool> {
        let fmt = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
                 sign:mandatory]:[offset_minute]:[offset_second]"
        );

        let last_heartbeat = time::OffsetDateTime::parse(last_heartbeat, fmt)?;
        let now = time::OffsetDateTime::now_utc();
        let diff = now - last_heartbeat;
        let too_slow = diff > time::Duration::seconds(DEF_LOST_NODE_AT_SEC);

        Ok(too_slow)
    }

    pub fn print_short(&self) {
        println!(
            "ID: {} | Pool: {} | Status: {} | Availability: {} | Last Heartbeat: {}",
            self.id, self.pool, self.status, self.availability, self.last_heartbeat
        );
    }

    pub fn print_list_json(nodes: Vec<Node>) {
        if let Ok(json) = to_string(&nodes) {
            println!("{}", json);
        }
    }

    pub fn print_list(nodes: Vec<Node>) {
        nodes.into_iter().for_each(|node| node.print_short());
    }

    pub fn print_long(&self) {
        println!("Node ID: {}", self.id);

        let mut attrs = Map::new();
        let default_map = Map::new();
        let ca = self
            .config_attrs
            .as_object()
            .unwrap_or(&default_map)
            .clone();
        let fa = self
            .fingerprint_attrs
            .as_object()
            .unwrap_or(&default_map)
            .clone();
        let da = self
            .dynamic_attrs
            .as_object()
            .unwrap_or(&default_map)
            .clone();

        attrs.extend(ca);
        attrs.extend(fa);
        attrs.extend(da);

        if let Ok(printable) = to_string(&attrs) {
            println!("Attributes: {}", printable);
        }
    }

    pub fn print_long_json(&self) {
        if let Ok(json) = to_string(self) {
            println!("{}", json);
        };
    }
}

impl ServerConfig {
    pub fn with_defaults(&self) -> R<ServerConfigFull> {
        let cloned = self.clone();

        let config = ServerConfigFull {
            port: self.port.unwrap_or(DEF_MANAGER_SERVER_PORT),
            insecure_port: self.insecure_port.unwrap_or(DEF_MANAGER_INSECURE_PORT),
            host: cloned.host.unwrap_or(DEF_MANAGER_HOSTNAME.to_string()),
        };

        Ok(config)
    }

    pub fn full_default() -> ServerConfigFull {
        ServerConfigFull {
            port: DEF_MANAGER_SERVER_PORT,
            insecure_port: DEF_MANAGER_INSECURE_PORT,
            host: DEF_MANAGER_HOSTNAME.to_string(),
        }
    }
}

impl ManagerConfig {
    pub fn with_defaults(&self) -> R<ManagerConfigFull> {
        let cloned = self.clone();

        let server_config = match cloned.server {
            Some(config) => config.with_defaults()?,
            None => ServerConfig::full_default(),
        };

        let mc = ManagerConfigFull {
            server: server_config,
            store_path: cloned.store_path.unwrap_or(DEF_STORE_PATH.into()),
            worker_config_path: cloned
                .worker_config_path
                .unwrap_or(DEF_WORKER_CONFIG_PATH.into()),
            worker_check_seconds: cloned.worker_check_seconds.unwrap_or(DEF_WORKER_CHECK_SEC),
        };

        Ok(mc)
    }
}
