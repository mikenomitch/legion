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
    /// Where `last_heartbeat` came from on the manager that answered:
    /// `"live"` (this manager owns liveness and saw the beat),
    /// `"checkpoint"` (a replicated checkpoint, accurate to within
    /// `liveness.checkpointInterval`), or empty (never observed, or
    /// checkpoints disabled). See docs/node-liveness.md.
    #[serde(default)]
    pub heartbeat_source: String,
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
            heartbeat_source: node.heartbeat_source,
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
            heartbeat_source: node.heartbeat_source,
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
        for line in self.security_lines() {
            println!("{line}");
        }

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

    /// The worker plane and host guard lines `legion node info` prints
    /// above the raw attributes (worker-plane-auth plan §6.7, §6.13).
    pub fn security_lines(&self) -> Vec<String> {
        let attr = |key: &str| {
            self.config_attrs
                .get(key)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        let mut lines = Vec::new();
        let probe = self.dynamic_attrs.get("workerPlane");
        match (attr("workerAddress"), probe) {
            (Some(address), Some(probe)) if probe["reachable"] == true => lines.push(format!(
                "Worker plane   {address}   reachable from {}",
                probe["from"].as_str().unwrap_or("a manager")
            )),
            (Some(address), Some(probe)) => {
                lines.push(format!(
                    "Worker plane   {address}   unreachable from {}: {}",
                    probe["from"].as_str().unwrap_or("a manager"),
                    probe["error"].as_str().unwrap_or("no answer")
                ));
                lines.push(
                    "               Managers dial workers on this address. Allow it from the \
                     managers, or set"
                        .to_string(),
                );
                lines.push(
                    "               LEGION_WORKER_ADVERTISE_ADDR if this address is wrong."
                        .to_string(),
                );
            }
            (Some(address), None) => {
                lines.push(format!("Worker plane   {address}   not probed yet"))
            }
            (None, _) => {}
        }
        lines.push(format!(
            "Host guard     {}",
            attr("hostGuard").unwrap_or_else(|| "not reported (worker predates it)".into())
        ));
        lines
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
    /// Suffixed `(checkpoint)` on a manager that does not own liveness, so an
    /// operator reading the column knows the clock is approximate and the
    /// STATUS beside it is not. See docs/node-liveness.md.
    pub last_heartbeat: String,
}

impl From<Node> for NodePrint {
    fn from(node: Node) -> Self {
        let last_heartbeat = match node.heartbeat_source.as_str() {
            "checkpoint" if !node.last_heartbeat.is_empty() => {
                format!("{} (checkpoint)", node.last_heartbeat)
            }
            _ => node.last_heartbeat,
        };
        NodePrint {
            id: node.id,
            status: node.status,
            pool: node.pool,
            availability: node.availability,
            last_heartbeat,
        }
    }
}

#[cfg(test)]
mod node_test {
    use super::*;
    use serde_json::json;

    fn node(dynamic: serde_json::Value) -> Node {
        Node {
            id: "wkr-1".into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs: json!({
                "workerAddress": "https://10.0.1.7:4848",
                "hostGuard": "active (nftables)",
            }),
            fingerprint_attrs: json!({}),
            dynamic_attrs: dynamic,
            health_checks: json!({}),
        }
    }

    #[test]
    fn node_info_says_whether_managers_reach_the_worker_test() {
        let lines = node(json!({})).security_lines();
        assert_eq!(
            lines[0],
            "Worker plane   https://10.0.1.7:4848   not probed yet"
        );
        assert_eq!(lines[1], "Host guard     active (nftables)");

        let lines = node(json!({ "workerPlane": {
            "reachable": true, "from": "mgr-a", "address": "https://10.0.1.7:4848",
        }}))
        .security_lines();
        assert_eq!(
            lines[0],
            "Worker plane   https://10.0.1.7:4848   reachable from mgr-a"
        );

        let lines = node(json!({ "workerPlane": {
            "reachable": false, "from": "mgr-b", "error": "connection timed out",
        }}))
        .security_lines();
        assert_eq!(
            lines[0],
            "Worker plane   https://10.0.1.7:4848   unreachable from mgr-b: connection timed out"
        );
        assert!(lines[2].contains("LEGION_WORKER_ADVERTISE_ADDR"));
    }
}
