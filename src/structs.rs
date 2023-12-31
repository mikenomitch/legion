use crate::constants::{
    DEF_MANAGER_SERVER_PORT,
    DEF_MANAGER_INSECURE_PORT,
    DEF_MANAGER_HOSTNAME,
    DEF_STORE_PATH,
    DEF_WORKER_CONFIG_PATH,
    DEF_LOST_NODE_AT_SEC,
    DEF_WORKER_CHECK_SEC,
};
use crate::util::{merge_json_strings, R};

use time::macros::format_description;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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


pub trait Printable {
    fn print_short(&self) -> ();
    fn print_short_json(&self) -> ();
    fn print_long(&self) -> ();
    fn print_long_json(&self) -> ();
    fn print_list_json(nodes: Vec<Node>) -> ();
    fn print_list(nodes: Vec<Node>) -> ();
}

impl Printable for Node {
    fn print_short(&self) {
        println!(
            "ID: {}, Status: {}, Pool: {}, Checkin: {}",
            self.id, self.status, self.pool, self.last_heartbeat
        );
    }

    fn print_list_json(nodes: Vec<Node>) {
        let json = serde_json::to_string(&nodes).unwrap();
        println!("{}", json);
    }

    fn print_list(nodes: Vec<Node>) {
        nodes.into_iter().for_each(|node| node.print_short());
    }

    fn print_short_json(&self) {
        let json = serde_json::to_string(&self).unwrap();
        println!("{}", json);
    }

    fn print_long(&self) {
        println!("Node ID: {}", self.id);
        let full_attrs = merge_json_strings(
            merge_json_strings(self.config_attrs.clone(), self.fingerprint_attrs.clone()),
            self.dynamic_attrs.clone(),
        );

        println!("Attributes: {}", full_attrs);
    }

    fn print_long_json(&self) {
        let json = serde_json::to_string(&self).unwrap();
        println!("{}", json);
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
            Some(config) => { config.with_defaults()? },
            None => { ServerConfig::full_default() },
        };

        let mc = ManagerConfigFull {
            server: server_config,
            store_path: cloned.store_path.unwrap_or(DEF_STORE_PATH.into()),
            worker_config_path: cloned.worker_config_path.unwrap_or(DEF_WORKER_CONFIG_PATH.into()),
            worker_check_seconds: cloned.worker_check_seconds.unwrap_or(DEF_WORKER_CHECK_SEC),
        };

        Ok(mc)
    }
}