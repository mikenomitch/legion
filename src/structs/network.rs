use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::prelude::{bail, R};

/// Inclusive dynamic port range used when a port has no `static` value.
pub const DYNAMIC_PORT_MIN: u16 = 20000;
pub const DYNAMIC_PORT_MAX: u16 = 32000;

fn default_network_mode() -> String {
    "auto".to_string()
}

/// Task-level networking declaration: which ports the task uses and how the
/// datapath should be selected. `mode` is `auto` (pick per-platform) or `host`
/// in v1; future datapath plugins add more modes.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NetworkConfig {
    #[serde(default = "default_network_mode")]
    pub mode: String,
    #[serde(default)]
    pub ports: Vec<PortConfig>,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        NetworkConfig {
            mode: default_network_mode(),
            ports: Vec::new(),
        }
    }
}

/// One named port. Without `static`, a host port is assigned from the dynamic
/// range at placement time. `to` is the in-task target port for datapaths that
/// remap (bridge/container); on the host datapath it is advisory.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PortConfig {
    pub name: String,
    #[serde(
        default,
        rename = "static",
        alias = "staticPort",
        skip_serializing_if = "Option::is_none"
    )]
    pub static_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<u16>,
}

/// `service.port` accepts either a named `network.ports` label, a literal port
/// number (sugar: declares a static port inline), or the string `dynamic`
/// (sugar: declares a dynamic port inline).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum ServicePort {
    Number(u16),
    Label(String),
}

/// Service advertisement for a task. Registration is derived by the manager
/// from allocation + check state; tasks never register themselves.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceConfig {
    /// Service name; defaults to the task name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub port: ServicePort,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub meta: Map<String, Value>,
}

impl ServiceConfig {
    pub fn service_name<'a>(&'a self, task_name: &'a str) -> &'a str {
        self.name.as_deref().unwrap_or(task_name)
    }

    pub fn port_label(&self) -> Option<&str> {
        match &self.port {
            ServicePort::Label(label) if label != "dynamic" => Some(label),
            _ => None,
        }
    }

    /// `service.meta` flattened to strings for transport (the wire type is a
    /// `map<string, string>`). String values pass through unchanged; anything
    /// else is rendered as compact JSON so nothing is silently dropped.
    pub fn meta_strings(&self) -> BTreeMap<String, String> {
        self.meta
            .iter()
            .map(|(key, value)| {
                let text = match value {
                    Value::String(text) => text.clone(),
                    other => other.to_string(),
                };
                (key.clone(), text)
            })
            .collect()
    }
}

/// A port assigned to an allocation at placement time.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AllocPort {
    pub name: String,
    pub host_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<u16>,
}

/// Concrete network facts for one allocation: the node address serving its
/// ports plus the assigned ports. Persisted on the allocation row and shipped
/// to the worker for env injection.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct AllocNetwork {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<AllocPort>,
}

impl AllocNetwork {
    pub fn port(&self, label: &str) -> Option<&AllocPort> {
        self.ports.iter().find(|port| port.name == label)
    }

    /// Env pairs for the assigned ports: LEGION_PORT_<L>, LEGION_ADDR_<L>,
    /// LEGION_IP_<L>, LEGION_PORT_TO_<L>.
    pub fn env_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        for port in &self.ports {
            let label = sanitize_env_label(&port.name);
            pairs.push((format!("LEGION_PORT_{label}"), port.host_port.to_string()));
            if let Some(to) = port.to {
                pairs.push((format!("LEGION_PORT_TO_{label}"), to.to_string()));
            }
            if let Some(address) = self.address.as_deref() {
                pairs.push((format!("LEGION_IP_{label}"), address.to_string()));
                pairs.push((
                    format!("LEGION_ADDR_{label}"),
                    format!("{address}:{}", port.host_port),
                ));
            }
        }
        pairs
    }
}

/// A check's target port: either a literal number or the name of a
/// `network.ports` entry (resolved to the assigned host port at runtime, so
/// checks work against dynamic ports).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum CheckPort {
    Number(u16),
    Label(String),
}

impl std::fmt::Display for CheckPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckPort::Number(number) => write!(f, "{number}"),
            CheckPort::Label(label) => write!(f, "{label}"),
        }
    }
}

impl CheckPort {
    /// Resolve to a concrete host port, consulting the allocation's assigned
    /// ports for label references.
    pub fn resolve(&self, network: Option<&AllocNetwork>) -> Option<u16> {
        match self {
            CheckPort::Number(number) => Some(*number),
            CheckPort::Label(label) => network
                .and_then(|network| network.port(label))
                .map(|port| port.host_port),
        }
    }
}

/// One resolvable endpoint of a service, as derived by the catalog projection
/// (task config x allocation x health x node).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceEndpointRow {
    pub service: String,
    pub job_id: String,
    pub task_name: String,
    pub allocation_id: i64,
    pub node_id: String,
    pub address: Option<String>,
    pub port: Option<u16>,
    pub healthy: bool,
    pub health_status: String,
    pub tags: Vec<String>,
    pub meta: BTreeMap<String, String>,
    /// `network.ports` entry the service advertises, when it names a label.
    pub port_label: Option<String>,
}

/// Uppercase a port label for env var use, mapping non-alphanumerics to `_`.
pub fn sanitize_env_label(label: &str) -> String {
    label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// Assign host ports for a task's declared ports. `used` holds host ports
/// already taken on the node. Static conflicts fail; dynamic ports scan the
/// dynamic range.
pub fn assign_ports(network: &NetworkConfig, used: &HashSet<u16>) -> R<Vec<AllocPort>> {
    let mut assigned: Vec<AllocPort> = Vec::new();
    let mut taken: HashSet<u16> = used.clone();

    for port in &network.ports {
        if let Some(static_port) = port.static_port {
            if taken.contains(&static_port) {
                bail!(
                    "static port {static_port} for port '{}' is already in use on this node",
                    port.name
                );
            }
            taken.insert(static_port);
            assigned.push(AllocPort {
                name: port.name.clone(),
                host_port: static_port,
                to: port.to,
            });
        }
    }

    let mut candidate = DYNAMIC_PORT_MIN;
    for port in &network.ports {
        if port.static_port.is_some() {
            continue;
        }
        while candidate <= DYNAMIC_PORT_MAX && taken.contains(&candidate) {
            candidate += 1;
        }
        if candidate > DYNAMIC_PORT_MAX {
            bail!(
                "no free dynamic port available for port '{}' (range {DYNAMIC_PORT_MIN}-{DYNAMIC_PORT_MAX} exhausted on this node)",
                port.name
            );
        }
        taken.insert(candidate);
        assigned.push(AllocPort {
            name: port.name.clone(),
            host_port: candidate,
            to: port.to,
        });
    }

    Ok(assigned)
}

/// Apply the `service.port` shorthand: an integer or `dynamic` declares the
/// matching `network.ports` entry inline and rewrites the ref to a label.
pub fn normalize_task_network(
    task_name: &str,
    network: &mut Option<NetworkConfig>,
    service: &mut Option<ServiceConfig>,
) -> R<()> {
    let Some(service_config) = service.as_mut() else {
        return Ok(());
    };

    let label = service_config.service_name(task_name).to_string();
    let inline_static = match &service_config.port {
        ServicePort::Number(number) => Some(Some(*number)),
        ServicePort::Label(value) if value == "dynamic" => Some(None),
        ServicePort::Label(_) => None,
    };

    let Some(static_port) = inline_static else {
        return Ok(());
    };

    let network_config = network.get_or_insert_with(NetworkConfig::default);
    match network_config.ports.iter().find(|port| port.name == label) {
        Some(existing) if existing.static_port == static_port => {}
        Some(existing) => {
            bail!(
                "task {task_name} service.port declares {:?} but network.ports entry '{label}' declares {:?}; remove one of them",
                static_port,
                existing.static_port
            );
        }
        None => {
            network_config.ports.push(PortConfig {
                name: label.clone(),
                static_port,
                to: None,
            });
        }
    }
    service_config.port = ServicePort::Label(label);

    Ok(())
}

/// Validate a task's network + service declaration. Runs after normalization,
/// so `service.port` is always a label here.
pub fn validate_task_network(
    task_name: &str,
    network: Option<&NetworkConfig>,
    service: Option<&ServiceConfig>,
) -> R<()> {
    let mut labels: HashSet<&str> = HashSet::new();
    let mut static_ports: HashSet<u16> = HashSet::new();

    if let Some(network_config) = network {
        for port in &network_config.ports {
            if port.name.trim().is_empty() {
                bail!("task {task_name} network.ports entries must have a non-empty name");
            }
            if !labels.insert(port.name.as_str()) {
                bail!(
                    "task {task_name} network.ports has duplicate port name '{}'",
                    port.name
                );
            }
            if let Some(static_port) = port.static_port {
                if static_port == 0 {
                    bail!(
                        "task {task_name} network.ports entry '{}' static port must be greater than 0",
                        port.name
                    );
                }
                if !static_ports.insert(static_port) {
                    bail!(
                        "task {task_name} network.ports declares static port {static_port} more than once"
                    );
                }
            }
        }
    }

    if let Some(service_config) = service {
        let service_name = service_config.service_name(task_name);
        if service_name.trim().is_empty() {
            bail!("task {task_name} service.name must not be empty");
        }
        match &service_config.port {
            ServicePort::Label(label) => {
                if !labels.contains(label.as_str()) {
                    bail!(
                        "task {task_name} service.port references port '{label}' which is not declared in network.ports"
                    );
                }
            }
            ServicePort::Number(number) => {
                // normalize_task_network rewrites numbers to labels; reaching
                // here means normalization was skipped.
                bail!(
                    "task {task_name} service.port {number} was not normalized; submit path must call JobConfig::normalize"
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn service(port: Value) -> ServiceConfig {
        serde_json::from_value(json!({ "port": port })).expect("service config")
    }

    #[test]
    fn service_port_number_sugar_declares_static_port() -> R<()> {
        let mut network = None;
        let mut svc = Some(service(json!(6379)));
        normalize_task_network("redis", &mut network, &mut svc)?;

        let network = network.expect("network created");
        assert_eq!(network.ports.len(), 1);
        assert_eq!(network.ports[0].name, "redis");
        assert_eq!(network.ports[0].static_port, Some(6379));
        assert_eq!(
            svc.as_ref().expect("service").port,
            ServicePort::Label("redis".into())
        );
        validate_task_network("redis", Some(&network), svc.as_ref())?;
        Ok(())
    }

    #[test]
    fn service_port_dynamic_sugar_declares_dynamic_port() -> R<()> {
        let mut network = None;
        let mut svc = Some(service(json!("dynamic")));
        normalize_task_network("api", &mut network, &mut svc)?;

        let network = network.expect("network created");
        assert_eq!(network.ports[0].static_port, None);
        validate_task_network("api", Some(&network), svc.as_ref())?;
        Ok(())
    }

    #[test]
    fn service_port_sugar_conflicting_static_rejected() {
        let mut network = Some(NetworkConfig {
            ports: vec![PortConfig {
                name: "web".into(),
                static_port: Some(8080),
                to: None,
            }],
            ..NetworkConfig::default()
        });
        let mut svc = Some(service(json!(9090)));
        let err = normalize_task_network("web", &mut network, &mut svc).unwrap_err();
        assert!(err.to_string().contains("remove one of them"));
    }

    #[test]
    fn dangling_service_port_label_rejected() {
        let network = NetworkConfig {
            ports: vec![PortConfig {
                name: "http".into(),
                static_port: None,
                to: Some(8080),
            }],
            ..NetworkConfig::default()
        };
        let svc = service(json!("grpc"));
        let err = validate_task_network("web", Some(&network), Some(&svc)).unwrap_err();
        assert!(err
            .to_string()
            .contains("references port 'grpc' which is not declared"));
    }

    #[test]
    fn duplicate_port_names_rejected() {
        let network = NetworkConfig {
            ports: vec![
                PortConfig {
                    name: "http".into(),
                    static_port: None,
                    to: None,
                },
                PortConfig {
                    name: "http".into(),
                    static_port: Some(80),
                    to: None,
                },
            ],
            ..NetworkConfig::default()
        };
        let err = validate_task_network("web", Some(&network), None).unwrap_err();
        assert!(err.to_string().contains("duplicate port name 'http'"));
    }

    #[test]
    fn assign_ports_skips_used_and_rejects_static_conflicts() -> R<()> {
        let network = NetworkConfig {
            ports: vec![
                PortConfig {
                    name: "metrics".into(),
                    static_port: Some(9090),
                    to: None,
                },
                PortConfig {
                    name: "http".into(),
                    static_port: None,
                    to: Some(8080),
                },
            ],
            ..NetworkConfig::default()
        };

        let used: HashSet<u16> = [DYNAMIC_PORT_MIN, DYNAMIC_PORT_MIN + 1].into();
        let assigned = assign_ports(&network, &used)?;
        assert_eq!(assigned[0].host_port, 9090);
        assert_eq!(assigned[1].host_port, DYNAMIC_PORT_MIN + 2);

        let conflicting: HashSet<u16> = [9090].into();
        let err = assign_ports(&network, &conflicting).unwrap_err();
        assert!(err.to_string().contains("static port 9090"));
        Ok(())
    }

    #[test]
    fn env_pairs_sanitize_labels_and_include_address() {
        let network = AllocNetwork {
            address: Some("10.0.0.5".into()),
            ports: vec![AllocPort {
                name: "http-2".into(),
                host_port: 20000,
                to: Some(8080),
            }],
        };

        let pairs = network.env_pairs();
        let lookup: std::collections::HashMap<_, _> = pairs.into_iter().collect();
        assert_eq!(
            lookup.get("LEGION_PORT_HTTP_2").map(String::as_str),
            Some("20000")
        );
        assert_eq!(
            lookup.get("LEGION_PORT_TO_HTTP_2").map(String::as_str),
            Some("8080")
        );
        assert_eq!(
            lookup.get("LEGION_IP_HTTP_2").map(String::as_str),
            Some("10.0.0.5")
        );
        assert_eq!(
            lookup.get("LEGION_ADDR_HTTP_2").map(String::as_str),
            Some("10.0.0.5:20000")
        );
    }
}
