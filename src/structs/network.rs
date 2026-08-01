use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::prelude::{bail, R};

/// Inclusive dynamic port range used when a port has no `static` value.
pub const DYNAMIC_PORT_MIN: u16 = 20000;
pub const DYNAMIC_PORT_MAX: u16 = 32000;

/// Loopback address every upstream forwarder binds. Upstreams are a pure
/// userspace TCP hop on the node the task runs on, so the address is fixed and
/// identical on macOS and Linux — no netns, no bridge, no platform branches.
pub const UPSTREAM_LOOPBACK_ADDR: &str = "127.0.0.1";

/// Strategy used when an upstream does not name one.
pub const DEFAULT_BALANCE: &str = "round_robin";

/// Canonicalize a user-written strategy name: trim, lowercase, and treat `-`
/// as `_` so `least-conn` and `Least_Conn` both mean `least_conn`.
pub fn normalize_balance(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace('-', "_")
}

/// Canonical load-balancing strategy names accepted by
/// `connect.upstreams[].balance`.
///
/// Derived from the worker's balancer registry
/// (`crate::worker::balance::BALANCERS`) rather than repeated here: validation
/// must accept exactly the strategies this build can actually run, and a second
/// list is a second thing to forget when adding one.
pub fn balance_strategies() -> Vec<&'static str> {
    crate::worker::balance::balancer_names()
}

/// Whether `name` (already normalized) is a known strategy.
pub fn is_known_balance(name: &str) -> bool {
    balance_strategies().contains(&name)
}

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
    /// Connect intentions: job ids allowed to reach this service through an
    /// upstream. Empty (the default) or `["*"]` means every job may connect.
    /// A denied consumer simply receives an empty endpoint list — enforcement
    /// is control-plane + forwarder, not cryptographic identity.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow: Vec<String>,
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

    /// Whether `job_id` may connect to this service. Empty `allow` (the
    /// default) or a `"*"` entry allows everyone, preserving pre-connect
    /// behavior for services that declare no intentions.
    pub fn allows_job(&self, job_id: &str) -> bool {
        allow_permits(&self.allow, job_id)
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

/// Shared intention check so the catalog projection and `ServiceConfig` agree:
/// an empty list or a `"*"` entry permits every consumer.
pub fn allow_permits(allow: &[String], job_id: &str) -> bool {
    allow.is_empty() || allow.iter().any(|entry| entry == "*" || entry == job_id)
}

/// Task-level `connect` block: the services this task dials through node-local
/// loopback forwarders. Declaring upstreams is the only thing a task needs to
/// do to consume a service — the worker starts one listener per upstream and
/// load-balances across the service's healthy endpoints.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConnectConfig {
    #[serde(default)]
    pub upstreams: Vec<UpstreamConfig>,
}

impl ConnectConfig {
    pub fn is_empty(&self) -> bool {
        self.upstreams.is_empty()
    }
}

/// One declared upstream. `localPort` pins the loopback port the forwarder
/// listens on (handy for clients that cannot read env vars); without it a port
/// is assigned from the dynamic range at placement time, exactly like a task
/// port. `balance` overrides the default strategy for this upstream only.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamConfig {
    pub service: String,
    #[serde(default, alias = "local_port", skip_serializing_if = "Option::is_none")]
    pub local_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance: Option<String>,
}

impl UpstreamConfig {
    /// The strategy name to use for this upstream, normalized. Falls back to
    /// [`DEFAULT_BALANCE`] when unset or blank; never returns an empty string.
    pub fn balance_name(&self) -> String {
        match self.balance.as_deref().map(normalize_balance) {
            Some(name) if !name.is_empty() => name,
            _ => DEFAULT_BALANCE.to_string(),
        }
    }
}

/// An upstream resolved at placement time: which loopback port the node-local
/// forwarder listens on for this allocation, and which strategy it balances
/// with. Persisted on the allocation next to its ports.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AllocUpstream {
    pub service: String,
    pub local_port: u16,
    /// Resolved strategy name, never empty (see [`DEFAULT_BALANCE`]).
    pub balance: String,
}

impl AllocUpstream {
    /// `127.0.0.1:<local_port>` — what `LEGION_UPSTREAM_<SERVICE>` contains.
    pub fn address(&self) -> String {
        format!("{UPSTREAM_LOOPBACK_ADDR}:{}", self.local_port)
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
    /// Loopback ports assigned to this allocation's `connect.upstreams`.
    /// `default` + `skip_serializing_if` so allocation rows written before
    /// connect existed keep parsing and keep serializing unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub upstreams: Vec<AllocUpstream>,
}

impl AllocNetwork {
    pub fn port(&self, label: &str) -> Option<&AllocPort> {
        self.ports.iter().find(|port| port.name == label)
    }

    pub fn upstream(&self, service: &str) -> Option<&AllocUpstream> {
        self.upstreams
            .iter()
            .find(|upstream| upstream.service == service)
    }

    /// Env pairs for the assigned ports: LEGION_PORT_<L>, LEGION_ADDR_<L>,
    /// LEGION_IP_<L>, LEGION_PORT_TO_<L>; plus, per upstream,
    /// LEGION_UPSTREAM_<L> (`127.0.0.1:<port>`) and LEGION_UPSTREAM_PORT_<L>.
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
        for upstream in &self.upstreams {
            let label = sanitize_env_label(&upstream.service);
            pairs.push((format!("LEGION_UPSTREAM_{label}"), upstream.address()));
            pairs.push((
                format!("LEGION_UPSTREAM_PORT_{label}"),
                upstream.local_port.to_string(),
            ));
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
    /// Connect intentions declared by the endpoint's own task
    /// (`service.allow`). Carried on the row so the heartbeat upstream feed can
    /// decide visibility per consuming job without re-reading task configs.
    /// Empty means "any job may connect" — see [`allow_permits`]. The catalog
    /// RPCs (`ListServices` / `ResolveService`) ignore it: intentions gate the
    /// connect datapath, not who may *look at* the catalog.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow: Vec<String>,
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

/// Typed marker for a statically-requested port that is already taken on a
/// node. Kept as a distinct error type so the plan-apply candidate walk can
/// recognize the collision and skip to the next candidate instead of
/// failing the whole plan; every other port-assignment error propagates
/// (docs/late-binding-plan-apply.md).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticPortInUse {
    pub port: u16,
    message: String,
}

impl StaticPortInUse {
    fn for_port(port: u16, name: &str) -> Self {
        StaticPortInUse {
            port,
            message: format!("static port {port} for port '{name}' is already in use on this node"),
        }
    }

    fn for_upstream(port: u16, service: &str) -> Self {
        StaticPortInUse {
            port,
            message: format!(
                "static local port {port} for upstream '{service}' is already in use on this node"
            ),
        }
    }
}

impl std::fmt::Display for StaticPortInUse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StaticPortInUse {}

/// Take the next free port from the dynamic range, advancing the shared
/// cursor. `what` names the offending entry in the exhaustion error.
fn next_dynamic_port(cursor: &mut u16, taken: &mut HashSet<u16>, what: &str) -> R<u16> {
    while *cursor <= DYNAMIC_PORT_MAX && taken.contains(cursor) {
        *cursor += 1;
    }
    if *cursor > DYNAMIC_PORT_MAX {
        bail!(
            "no free dynamic port available for {what} (range {DYNAMIC_PORT_MIN}-{DYNAMIC_PORT_MAX} exhausted on this node)"
        );
    }
    let port = *cursor;
    taken.insert(port);
    Ok(port)
}

/// Assign every node-local port one allocation needs — task host ports and
/// upstream loopback ports — out of a single `used` set, because both live in
/// the node's one port space. Statics are claimed first across *both* lists so
/// a dynamic assignment can never steal a pinned port; dynamics then scan
/// `DYNAMIC_PORT_MIN..=DYNAMIC_PORT_MAX`. Static conflicts fail loudly.
pub fn assign_allocation_ports(
    network: Option<&NetworkConfig>,
    connect: Option<&ConnectConfig>,
    used: &HashSet<u16>,
) -> R<(Vec<AllocPort>, Vec<AllocUpstream>)> {
    const NO_PORTS: &[PortConfig] = &[];
    const NO_UPSTREAMS: &[UpstreamConfig] = &[];
    let ports = network.map_or(NO_PORTS, |network| network.ports.as_slice());
    let upstreams = connect.map_or(NO_UPSTREAMS, |connect| connect.upstreams.as_slice());

    let mut assigned: Vec<AllocPort> = Vec::new();
    let mut assigned_upstreams: Vec<AllocUpstream> = Vec::new();
    let mut taken: HashSet<u16> = used.clone();

    for port in ports {
        if let Some(static_port) = port.static_port {
            if !taken.insert(static_port) {
                return Err(anyhow::Error::new(StaticPortInUse::for_port(
                    static_port,
                    &port.name,
                )));
            }
            assigned.push(AllocPort {
                name: port.name.clone(),
                host_port: static_port,
                to: port.to,
            });
        }
    }
    for upstream in upstreams {
        if let Some(local_port) = upstream.local_port {
            if !taken.insert(local_port) {
                return Err(anyhow::Error::new(StaticPortInUse::for_upstream(
                    local_port,
                    &upstream.service,
                )));
            }
            assigned_upstreams.push(AllocUpstream {
                service: upstream.service.clone(),
                local_port,
                balance: upstream.balance_name(),
            });
        }
    }

    let mut cursor = DYNAMIC_PORT_MIN;
    for port in ports {
        if port.static_port.is_some() {
            continue;
        }
        let host_port =
            next_dynamic_port(&mut cursor, &mut taken, &format!("port '{}'", port.name))?;
        assigned.push(AllocPort {
            name: port.name.clone(),
            host_port,
            to: port.to,
        });
    }
    for upstream in upstreams {
        if upstream.local_port.is_some() {
            continue;
        }
        let local_port = next_dynamic_port(
            &mut cursor,
            &mut taken,
            &format!("upstream '{}' local port", upstream.service),
        )?;
        assigned_upstreams.push(AllocUpstream {
            service: upstream.service.clone(),
            local_port,
            balance: upstream.balance_name(),
        });
    }

    Ok((assigned, assigned_upstreams))
}

/// Assign host ports for a task's declared ports. `used` holds host ports
/// already taken on the node. Static conflicts fail; dynamic ports scan the
/// dynamic range.
pub fn assign_ports(network: &NetworkConfig, used: &HashSet<u16>) -> R<Vec<AllocPort>> {
    let (ports, _) = assign_allocation_ports(Some(network), None, used)?;
    Ok(ports)
}

/// Assign loopback ports for a task's declared upstreams out of the same
/// per-node `used` set task ports come from.
pub fn assign_upstream_ports(
    connect: &ConnectConfig,
    used: &HashSet<u16>,
) -> R<Vec<AllocUpstream>> {
    let (_, upstreams) = assign_allocation_ports(None, Some(connect), used)?;
    Ok(upstreams)
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

/// Validate a task's `connect` block and its `service.allow` intentions.
///
/// Deliberately does **not** check that an upstream service exists: the
/// service it names may be submitted later (or by someone else), and failing
/// submission on ordering would make the feature unusable. An upstream whose
/// service has no endpoints simply gets an empty endpoint list at runtime.
pub fn validate_task_connect(
    task_name: &str,
    network: Option<&NetworkConfig>,
    connect: Option<&ConnectConfig>,
    service: Option<&ServiceConfig>,
) -> R<()> {
    if let Some(service_config) = service {
        for entry in &service_config.allow {
            if entry.trim().is_empty() {
                bail!("task {task_name} service.allow entries must not be empty");
            }
        }
    }

    let Some(connect_config) = connect else {
        return Ok(());
    };

    // Static task ports share the node's port space with upstream loopback
    // ports, so a collision here would only surface as a placement failure.
    let mut static_ports: HashSet<u16> = HashSet::new();
    if let Some(network_config) = network {
        for port in &network_config.ports {
            if let Some(static_port) = port.static_port {
                static_ports.insert(static_port);
            }
        }
    }

    let mut services: HashSet<&str> = HashSet::new();
    let mut local_ports: HashSet<u16> = HashSet::new();
    for upstream in &connect_config.upstreams {
        if upstream.service.trim().is_empty() {
            bail!("task {task_name} connect.upstreams entries must have a non-empty service");
        }
        if !services.insert(upstream.service.as_str()) {
            bail!(
                "task {task_name} connect.upstreams declares service '{}' more than once",
                upstream.service
            );
        }
        if let Some(local_port) = upstream.local_port {
            if local_port == 0 {
                bail!(
                    "task {task_name} connect.upstreams entry '{}' localPort must be greater than 0",
                    upstream.service
                );
            }
            if !local_ports.insert(local_port) {
                bail!(
                    "task {task_name} connect.upstreams declares localPort {local_port} more than once"
                );
            }
            if static_ports.contains(&local_port) {
                bail!(
                    "task {task_name} connect.upstreams entry '{}' localPort {local_port} collides with a static port declared in network.ports",
                    upstream.service
                );
            }
        }
        if let Some(balance) = upstream.balance.as_deref() {
            let normalized = normalize_balance(balance);
            if !is_known_balance(&normalized) {
                bail!(
                    "task {task_name} connect.upstreams entry '{}' has unknown balance '{balance}'; valid strategies are {}",
                    upstream.service,
                    balance_strategies().join(", ")
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
            upstreams: Vec::new(),
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

    //
    // CONNECT / UPSTREAMS
    //

    fn connect(upstreams: Value) -> ConnectConfig {
        serde_json::from_value(json!({ "upstreams": upstreams })).expect("connect config")
    }

    #[test]
    fn upstream_config_parses_camel_case_and_snake_case_local_port() {
        let config = connect(json!([
            { "service": "db" },
            { "service": "cache", "localPort": 6379 },
            { "service": "web", "local_port": 8080, "balance": "least_conn" },
        ]));
        assert_eq!(config.upstreams[0].local_port, None);
        assert_eq!(config.upstreams[0].balance_name(), DEFAULT_BALANCE);
        assert_eq!(config.upstreams[1].local_port, Some(6379));
        assert_eq!(config.upstreams[2].local_port, Some(8080));
        assert_eq!(config.upstreams[2].balance_name(), "least_conn");
    }

    #[test]
    fn balance_name_normalizes_case_and_dashes() {
        let config = connect(json!([{ "service": "db", "balance": " Least-Conn " }]));
        assert_eq!(config.upstreams[0].balance_name(), "least_conn");
        assert!(is_known_balance(&config.upstreams[0].balance_name()));
        assert!(is_known_balance(DEFAULT_BALANCE));
    }

    #[test]
    fn upstream_dynamic_ports_do_not_collide_with_task_ports() -> R<()> {
        let network = NetworkConfig {
            ports: vec![
                PortConfig {
                    name: "http".into(),
                    static_port: None,
                    to: None,
                },
                PortConfig {
                    name: "metrics".into(),
                    static_port: Some(9090),
                    to: None,
                },
            ],
            ..NetworkConfig::default()
        };
        let connect_config = connect(json!([{ "service": "db" }, { "service": "cache" }]));

        let used: HashSet<u16> = [DYNAMIC_PORT_MIN].into();
        let (ports, upstreams) =
            assign_allocation_ports(Some(&network), Some(&connect_config), &used)?;

        assert_eq!(ports.len(), 2);
        assert_eq!(upstreams.len(), 2);
        // Statics first, then dynamics from one shared cursor.
        assert_eq!(ports[0].host_port, 9090);
        assert_eq!(ports[1].host_port, DYNAMIC_PORT_MIN + 1);
        assert_eq!(upstreams[0].service, "db");
        assert_eq!(upstreams[0].local_port, DYNAMIC_PORT_MIN + 2);
        assert_eq!(upstreams[0].balance, DEFAULT_BALANCE);
        assert_eq!(upstreams[1].local_port, DYNAMIC_PORT_MIN + 3);

        let all: HashSet<u16> = ports
            .iter()
            .map(|port| port.host_port)
            .chain(upstreams.iter().map(|upstream| upstream.local_port))
            .collect();
        assert_eq!(all.len(), 4, "assigned ports must all be distinct");
        Ok(())
    }

    #[test]
    fn upstream_static_local_port_is_honored_and_conflicts_name_the_port() -> R<()> {
        let connect_config = connect(json!([
            { "service": "cache", "localPort": 6379 },
            { "service": "db" },
        ]));

        let upstreams = assign_upstream_ports(&connect_config, &HashSet::new())?;
        assert_eq!(upstreams[0].service, "cache");
        assert_eq!(upstreams[0].local_port, 6379);
        assert_eq!(upstreams[1].local_port, DYNAMIC_PORT_MIN);

        let taken: HashSet<u16> = [6379].into();
        let err = assign_upstream_ports(&connect_config, &taken).unwrap_err();
        assert!(err.to_string().contains("static local port 6379"), "{err}");
        assert!(err.to_string().contains("'cache'"), "{err}");
        Ok(())
    }

    #[test]
    fn upstream_static_local_port_conflicting_with_task_static_port_fails() {
        let network = NetworkConfig {
            ports: vec![PortConfig {
                name: "http".into(),
                static_port: Some(6379),
                to: None,
            }],
            ..NetworkConfig::default()
        };
        let connect_config = connect(json!([{ "service": "cache", "localPort": 6379 }]));
        let err = assign_allocation_ports(Some(&network), Some(&connect_config), &HashSet::new())
            .unwrap_err();
        assert!(err.to_string().contains("static local port 6379"), "{err}");
    }

    #[test]
    fn env_pairs_include_upstream_address_and_port() {
        let network = AllocNetwork {
            address: Some("10.0.0.5".into()),
            ports: Vec::new(),
            upstreams: vec![AllocUpstream {
                service: "user-db".into(),
                local_port: 20003,
                balance: DEFAULT_BALANCE.into(),
            }],
        };

        let lookup: std::collections::HashMap<_, _> = network.env_pairs().into_iter().collect();
        assert_eq!(
            lookup.get("LEGION_UPSTREAM_USER_DB").map(String::as_str),
            Some("127.0.0.1:20003")
        );
        assert_eq!(
            lookup
                .get("LEGION_UPSTREAM_PORT_USER_DB")
                .map(String::as_str),
            Some("20003")
        );
    }

    #[test]
    fn alloc_network_json_without_upstreams_still_parses() -> R<()> {
        // Rows written before connect existed carry no `upstreams` key.
        let legacy = json!({
            "address": "10.0.0.5",
            "ports": [{ "name": "http", "hostPort": 20000 }],
        });
        let network: AllocNetwork = serde_json::from_value(legacy)?;
        assert!(network.upstreams.is_empty());
        assert_eq!(network.port("http").map(|port| port.host_port), Some(20000));
        // And an upstream-less allocation round-trips without the key.
        let encoded = serde_json::to_value(&network)?;
        assert!(encoded.get("upstreams").is_none());
        Ok(())
    }

    #[test]
    fn validate_connect_accepts_a_well_formed_block() -> R<()> {
        let network = NetworkConfig {
            ports: vec![PortConfig {
                name: "http".into(),
                static_port: Some(8080),
                to: None,
            }],
            ..NetworkConfig::default()
        };
        let connect_config = connect(json!([
            { "service": "db" },
            { "service": "cache", "localPort": 6379 },
            { "service": "web", "balance": "least_conn" },
        ]));
        let svc: ServiceConfig =
            serde_json::from_value(json!({ "port": "http", "allow": ["gateway"] }))?;
        validate_task_connect("api", Some(&network), Some(&connect_config), Some(&svc))?;
        assert!(svc.allows_job("gateway"));
        assert!(!svc.allows_job("other"));
        Ok(())
    }

    #[test]
    fn validate_connect_rejects_duplicate_upstream_service() {
        let connect_config = connect(json!([{ "service": "db" }, { "service": "db" }]));
        let err = validate_task_connect("api", None, Some(&connect_config), None).unwrap_err();
        assert!(
            err.to_string()
                .contains("declares service 'db' more than once"),
            "{err}"
        );
        assert!(err.to_string().contains("task api"), "{err}");
    }

    #[test]
    fn validate_connect_rejects_empty_upstream_service() {
        let connect_config = connect(json!([{ "service": "  " }]));
        let err = validate_task_connect("api", None, Some(&connect_config), None).unwrap_err();
        assert!(err.to_string().contains("non-empty service"), "{err}");
    }

    #[test]
    fn validate_connect_rejects_duplicate_and_zero_local_ports() {
        let duplicate = connect(json!([
            { "service": "db", "localPort": 6379 },
            { "service": "cache", "localPort": 6379 },
        ]));
        let err = validate_task_connect("api", None, Some(&duplicate), None).unwrap_err();
        assert!(
            err.to_string().contains("localPort 6379 more than once"),
            "{err}"
        );

        let zero = connect(json!([{ "service": "db", "localPort": 0 }]));
        let err = validate_task_connect("api", None, Some(&zero), None).unwrap_err();
        assert!(err.to_string().contains("must be greater than 0"), "{err}");
    }

    #[test]
    fn validate_connect_rejects_local_port_colliding_with_task_static_port() {
        let network = NetworkConfig {
            ports: vec![PortConfig {
                name: "http".into(),
                static_port: Some(8080),
                to: None,
            }],
            ..NetworkConfig::default()
        };
        let connect_config = connect(json!([{ "service": "db", "localPort": 8080 }]));
        let err =
            validate_task_connect("api", Some(&network), Some(&connect_config), None).unwrap_err();
        assert!(
            err.to_string()
                .contains("collides with a static port declared in network.ports"),
            "{err}"
        );
    }

    #[test]
    fn validate_connect_rejects_unknown_balance_and_lists_valid_names() {
        let connect_config = connect(json!([{ "service": "db", "balance": "magic" }]));
        let err = validate_task_connect("api", None, Some(&connect_config), None).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("unknown balance 'magic'"), "{message}");
        for name in balance_strategies() {
            assert!(message.contains(name), "{message} is missing {name}");
        }
    }

    #[test]
    fn every_registered_strategy_is_accepted_by_validation() {
        // Validation and the datapath registry are the same list, so a
        // strategy the worker can build is always submittable — and one it
        // cannot build is always rejected.
        for name in balance_strategies() {
            let connect_config = connect(json!([{ "service": "db", "balance": name }]));
            validate_task_connect("api", None, Some(&connect_config), None)
                .unwrap_or_else(|err| panic!("registered strategy {name} rejected: {err}"));
            assert!(crate::worker::balance::balancer(name).is_ok());
        }
        assert!(crate::worker::balance::balancer("first_healthy").is_err());
        assert!(!is_known_balance("first_healthy"));
    }

    #[test]
    fn validate_connect_rejects_empty_allow_entry() {
        let svc: ServiceConfig =
            serde_json::from_value(json!({ "port": "http", "allow": ["gateway", " "] }))
                .expect("service config");
        let err = validate_task_connect("api", None, None, Some(&svc)).unwrap_err();
        assert!(err.to_string().contains("service.allow entries"), "{err}");
    }

    #[test]
    fn allow_wildcard_and_absent_allow_permit_everyone() {
        let open: ServiceConfig =
            serde_json::from_value(json!({ "port": "http" })).expect("service config");
        assert!(open.allows_job("anyone"));
        let wildcard: ServiceConfig =
            serde_json::from_value(json!({ "port": "http", "allow": ["*"] }))
                .expect("service config");
        assert!(wildcard.allows_job("anyone"));
        assert!(allow_permits(&[], "anyone"));
    }
}
