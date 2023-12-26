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
