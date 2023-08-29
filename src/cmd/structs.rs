use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ManagerConfig {
    pub id: u64,
    pub election_tick: usize,
    pub heartbeat_tick: usize,
    pub leader: bool,
        pub leader_address: String,
    pub peers: Vec<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    pub port: u64,
    pub address: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AgentConfig {
    pub server: ServerConfig,
    pub manager: ManagerConfig,
}
