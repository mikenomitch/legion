use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_string, Map, Value};
use tabled::Tabled;
use time::format_description::well_known::Rfc3339;
use time::{macros::format_description, Duration, OffsetDateTime};

use crate::prelude::{DEF_LOST_NODE_AT_SEC, R};

use super::NodeMsg;

#[derive(Serialize, Deserialize, Debug, Clone, Tabled)]
#[serde(rename_all = "camelCase")]
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
        let config_str = to_string(&node.config_attrs).unwrap_or_else(|_| "{}".to_string());
        let hc_str = to_string(&node.health_checks).unwrap_or_else(|_| "{}".to_string());
        let fp_str = to_string(&node.fingerprint_attrs).unwrap_or_else(|_| "{}".to_string());
        let da_str = to_string(&node.dynamic_attrs).unwrap_or_else(|_| "{}".to_string());

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
        let last_heartbeat = OffsetDateTime::parse(last_heartbeat, &Rfc3339).or_else(|_| {
            let fmt = format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
                     sign:mandatory]:[offset_minute]:[offset_second]"
            );
            OffsetDateTime::parse(last_heartbeat, fmt)
        })?;
        let now = OffsetDateTime::now_utc();
        let diff = now - last_heartbeat;
        let too_slow = diff > Duration::seconds(DEF_LOST_NODE_AT_SEC);

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
        }
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
