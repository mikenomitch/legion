use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ManagerConfig {
    pub election_tick: usize,
    pub heartbeat_tick: usize,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AgentConfig {
    pub manager: ManagerConfig,
}
