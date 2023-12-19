use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    pub port: u64,
    pub insecure_port: u64,
    pub address: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ManagerConfig {
    pub server: ServerConfig,
    pub store_path: String,
    pub worker_config_path: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct WorkerConfig {
    pub additional_fingerprinters: Vec<String>,
    pub worker_type: String,
    pub pool: String,
    pub attributes: Map<String, Value>,
}


/// Our claims struct, it needs to derive `Serialize` and/or `Deserialize`
#[derive(Debug, Serialize, Deserialize)]
pub struct JoinTokenClaims {
    pub exp: usize, // Required (validate_exp defaults to true in validation). Expiration time (as UTC timestamp)
    pub sub: String, // Optional. Subject (whom token refers to)
    pub address: String, // Optional. Address of the node
    pub server_config: ServerConfig, // Optional. Config of the node
    pub worker_type: String, // Optional. Type of the node (points to node template)
    // pub cert: String, // Optional. Cert of the node
    // pub key: String, // Optional. Private Key of the node
    // pub ca_cert: String, // Optional. CA Cert for the cluster
}
// aud: String,         // Optional. Audience
// iat: usize,          // Optional. Issued at (as UTC timestamp)
// iss: String,         // Optional. Issuer
// nbf: usize,          // Optional. Not Before (as UTC timestamp)
