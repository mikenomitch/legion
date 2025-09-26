use serde::{Deserialize, Serialize};

use super::config::ServerConfig;

/// Claims embedded in a join token issued by the manager.
#[derive(Debug, Serialize, Deserialize)]
pub struct JoinTokenClaims {
    pub exp: usize,
    pub sub: String,
    pub address: String,
    pub server_config: ServerConfig,
    pub worker_type: String,
    pub skip_expiry_validation: bool,
}
