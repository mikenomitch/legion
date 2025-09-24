use crate::prelude::*;
use tabled::Tabled;

use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_string};
use sha2::{Digest, Sha256};
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

impl Submission {
    pub fn hash(&self) -> String {
        let content = &self.content;
        let hash = Sha256::digest(content.as_bytes());
        format!("{:x}", hash)
    }
}

impl Eval {
    pub fn handle(&self) -> R<String> {
        // This can have different logic for each eval kind
        // It should clean up after itself most likely
        // But perhaps async clean ups make sense?

        // TODO: Make this an enum!
        // match self.kind.as_str() {
        //     "node_join" => {
        //         println!("Woot - node_join");
        //     }
        //     "node_drop" => {
        //         println!("Woot - node_drop");
        //     }
        //     "job_start" => {
        //         println!("Woot - job_start");
        //     }
        //     "job_stop" => {
        //         println!("Woot - job_stop");
        //     }
        //     "job_update" => {
        //         println!("Woot - job_update");
        //     }
        // }

        println!("Woot - evalled");
        Ok("Doneski".to_string())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ManagerConfigFull {
    pub server: ServerConfigFull,
    pub store_path: String,
    pub worker_config_path: String,
    pub worker_check_seconds: u64,
}

impl From<ManagerConfigFull> for ManagerConfig {
    fn from(val: ManagerConfigFull) -> Self {
        ManagerConfig {
            server: Some(val.server.into()),
            store_path: Some(val.store_path),
            worker_config_path: Some(val.worker_config_path),
            worker_check_seconds: Some(val.worker_check_seconds),
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

// // TODO: TASK KIND CONFIG SHOULD BE CUSTOMIZABLE
// // AND EVENTUALLY ABLE TO HANDLE DEEP NESTING
// #[derive(Serialize, Deserialize, Debug, Clone, Default)]
// #[serde(rename_all = "camelCase")]
// pub struct TypeConfig {
//     pub message: String,
// }

// // TODO: KIND CONFIG SHOULD BE MORE CUSTOMIZABLE
// #[derive(Serialize, Deserialize, Debug, Clone, Default)]
// #[serde(rename_all = "camelCase")]
// pub struct ResourcesConfig {
//     pub memory: u32,
// }
// #[derive(Serialize, Deserialize, Debug, Clone)]
// #[serde(rename_all = "camelCase")]
// pub struct Job {
//     pub name: String,
// }

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct JobPrint {
    pub name: String,
    pub status: String,
    pub created_at: String,
}

impl From<Job> for JobPrint {
    fn from(job: Job) -> Self {
        JobPrint {
            name: job.name,
            status: job.status,
            created_at: job.created_at.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct TaskConfig {
    pub name: String,
    pub count: u32,
    pub kind: String,
    pub config: Value,
    pub resources: Value,
    pub constraints: Value,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Allocation {
    pub id: i64,
    pub status: String,
    pub resources: Value,
    pub node_id: String,
    pub job_config_id: String,
    pub task_config_id: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JobConfig {
    pub name: String,
    pub tasks: Vec<TaskConfig>,
    pub version: Option<u32>,
}

impl JobConfig {
    pub fn is_same_as(&self, other: &JobConfig) -> bool {
        self.name == other.name && self.tasks == other.tasks
    }

    pub fn id(&self) -> String {
        match self.version {
            Some(version) => format!("{}-{}", self.name, version),
            None => format!("{}-{}", self.name, 0),
        }
    }
}

// #[derive(Serialize, Deserialize, Debug, Clone)]
// #[serde(rename_all = "camelCase")]
// pub struct Submission {
//     pub filename: String,
//     pub text: String,
//     pub is_primary: bool,
// }

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

#[derive(Serialize, Deserialize, Debug, Clone, Tabled)]
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

impl From<Node> for NodeMsg {
    fn from(node: Node) -> Self {
        let config_str = to_string(&node.config_attrs).unwrap_or("{}".to_string());
        let hc_str = to_string(&node.health_checks).unwrap_or("{}".to_string());
        let fp_str = to_string(&node.fingerprint_attrs).unwrap_or("{}".to_string());
        let da_str = to_string(&node.dynamic_attrs).unwrap_or("{}".to_string());

        NodeMsg {
            id: node.id,
            status: node.status,
            pool: node.pool,
            availability: node.availability,
            last_heartbeat: node.last_heartbeat,
            config_attrs: config_str,
            fingerprint_attrs: fp_str,
            dynamic_attrs: da_str,
            health_checks: hc_str,
        }
    }
}

impl From<NodeMsg> for Node {
    fn from(node: NodeMsg) -> Self {
        let config_map = from_str::<Value>(&node.config_attrs).unwrap_or(Value::Null);
        let fp_map = from_str::<Value>(&node.fingerprint_attrs).unwrap_or(Value::Null);
        let hc_map = from_str::<Value>(&node.health_checks).unwrap_or(Value::Null);
        let da_map = from_str::<Value>(&node.dynamic_attrs).unwrap_or(Value::Null);

        Node {
            id: node.id,
            status: node.status,
            pool: node.pool,
            availability: node.availability,
            last_heartbeat: node.last_heartbeat,
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
            println!("Attributes: {printable}");
        }
    }

    pub fn print_long_json(&self) {
        if let Ok(json) = to_string(self) {
            println!("{json}");
        };
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct NodePrint {
    pub id: String,
    pub status: String,
    pub pool: String,
    pub availability: String,
    pub last_heartbeat: String,
}

impl From<Node> for NodePrint {
    fn from(node: Node) -> Self {
        NodePrint {
            id: node.id,
            status: node.status,
            pool: node.pool,
            availability: node.availability,
            last_heartbeat: node.last_heartbeat,
        }
    }
}

impl ServerConfig {
    pub fn with_defaults(&self) -> ServerConfigFull {
        let cloned = self.clone();

        let config = ServerConfigFull {
            port: self.port.unwrap_or(DEF_MANAGER_SERVER_PORT),
            insecure_port: self.insecure_port.unwrap_or(DEF_MANAGER_INSECURE_PORT),
            host: cloned.host.unwrap_or(DEF_MANAGER_HOSTNAME.to_string()),
        };

        config
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
    pub fn with_defaults(&self) -> ManagerConfigFull {
        let cloned = self.clone();

        let server_config = match cloned.server {
            Some(config) => config.with_defaults(),
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

        mc
    }
}
