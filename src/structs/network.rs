use std::collections::{BTreeMap, HashMap, HashSet};

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

/// The DNS zone a task's upstreams are reachable under: `<key>.svc.legion`.
pub const SVC_ZONE: &str = "svc.legion";

/// Second octet of every allocation VIP block: `127.<VIP_SECOND_OCTET>.b`.
/// Fixed so a stray `127.0.x.x` listener (or `127.0.0.1` itself) can never
/// collide with an assigned block; `b` is the per-node counter (0..=255).
pub const VIP_SECOND_OCTET: u8 = 42;

/// Highest per-node VIP block index; 256 allocations with upstreams per
/// node before assignment fails loudly.
pub const VIP_BLOCK_MAX: u8 = 255;

/// The first host octet used for upstream VIPs (`.1` is the resolver).
pub const VIP_FIRST_HOST: u8 = 2;

/// Where the egress proxy listens inside a gated namespace.
pub const GATED_PROXY_ADDR: &str = "127.0.0.1";
pub const GATED_PROXY_PORT: u16 = 1080;

/// Resolve a task's declared `network.mode` for a runtime: `auto` is `host`
/// for every runtime today (`sandbox` moves to `gated` when its vsock
/// transport lands), and an explicit value is itself.
pub fn resolve_network_mode(declared: &str, _runtime: &str) -> String {
    let declared = declared.trim().to_ascii_lowercase();
    if declared.is_empty() || declared == crate::structs::netpolicy::MODE_AUTO {
        crate::structs::netpolicy::MODE_HOST.to_string()
    } else {
        declared
    }
}

/// Whether this platform routes all of 127/8 to loopback without
/// configuration (Linux does; macOS only has `127.0.0.1` on `lo0`).
pub fn platform_supports_vip_blocks() -> bool {
    cfg!(target_os = "linux")
}

/// Pick the next free VIP block (`127.42.<b>`) on a node.
pub fn next_vip_block(used_blocks: &HashSet<String>) -> R<String> {
    for b in 0..=VIP_BLOCK_MAX {
        let block = format!("127.{VIP_SECOND_OCTET}.{b}");
        if !used_blocks.contains(&block) {
            return Ok(block);
        }
    }
    bail!(
        "no free VIP block on this node ({} allocations with upstreams already placed)",
        u16::from(VIP_BLOCK_MAX) + 1
    )
}

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

/// Task-level networking declaration: which ports the task uses, how its
/// datapath is built, and the policy on its ingress and egress paths.
///
/// `mode` selects the datapath (`crate::structs::netpolicy::MODES`): `host`
/// is host networking, `gated` a namespace holding loopback and the datapath
/// only, `auto` picks per runtime and platform. `egress` / `ingress` are the
/// task's inline policy — the same shape as a stored policy object, scoped to
/// this task — and `labels` are attribution keys carried on every event and
/// access-log line (never on a metric).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NetworkConfig {
    #[serde(default = "default_network_mode")]
    pub mode: String,
    #[serde(default)]
    pub ports: Vec<PortConfig>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<crate::structs::netpolicy::ChainConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<crate::structs::netpolicy::ChainConfig>,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        NetworkConfig {
            mode: default_network_mode(),
            ports: Vec::new(),
            labels: BTreeMap::new(),
            egress: None,
            ingress: None,
        }
    }
}

impl NetworkConfig {
    /// Whether this task declares any policy of its own (either direction).
    pub fn declares_policy(&self) -> bool {
        self.egress.is_some() || self.ingress.is_some()
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

/// One declared upstream: a catalog service (`service`) or an external
/// destination (`name` + `to`). Either way the task gets a loopback port and
/// `LEGION_UPSTREAM_<KEY>`, and the node datapath does the dialing.
///
/// `localPort` pins the loopback port (handy for clients that cannot read env
/// vars); without it a port is assigned from the dynamic range at placement
/// time, exactly like a task port. `balance` overrides the default strategy
/// for this upstream only. `to` is `tcp://`, `tls://`, `http://`, or
/// `https://` — the `tls`/`https` forms make the datapath originate TLS, so
/// the task speaks plaintext to a loopback port and the body is visible to
/// `handlers` without intercepting anyone's TLS. `http` opts this upstream
/// into HTTP framing with traffic management.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamConfig {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub service: String,
    /// Key for an external upstream (`LEGION_UPSTREAM_<NAME>`); optional
    /// alias for a service upstream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// External destination URL. Mutually exclusive with `service`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    #[serde(default, alias = "local_port", skip_serializing_if = "Option::is_none")]
    pub local_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance: Option<String>,
    /// HTTP framing and traffic management for this upstream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<UpstreamHttpConfig>,
    /// Handlers on this upstream's path, run after the effective egress
    /// policy and before the dial.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub handlers: Vec<crate::structs::netpolicy::HandlerConfig>,
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

    /// Whether this upstream dials a fixed external destination rather than
    /// a catalog service.
    pub fn is_external(&self) -> bool {
        self.to.as_deref().is_some_and(|to| !to.trim().is_empty())
    }

    /// The key this upstream is known by: `name` when given, else `service`.
    /// It is what `LEGION_UPSTREAM_<KEY>` and `AllocUpstream.service` carry.
    pub fn key(&self) -> &str {
        match self.name.as_deref() {
            Some(name) if !name.trim().is_empty() => name,
            _ => self.service.as_str(),
        }
    }

    /// Whether the datapath parses HTTP on this upstream.
    pub fn is_http(&self) -> bool {
        self.http.is_some()
            || self
                .to
                .as_deref()
                .and_then(|to| parse_upstream_target(to).ok())
                .is_some_and(|target| target.http)
    }
}

/// HTTP traffic management for one upstream (service-mesh-plan §3.4). Every
/// field is optional; an empty block still turns on HTTP framing.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamHttpConfig {
    /// Per-request deadline (`3s`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<RetryConfig>,
    /// Weighted split by service tag; endpoints carrying no listed tag share
    /// the remainder.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub split: Vec<SplitConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outlier: Option<OutlierConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RetryConfig {
    /// Extra attempts after the first (default 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempts: Option<u32>,
    /// Conditions: `5xx`, `connect-failure`, `reset`, `retriable-4xx`.
    /// Default `["connect-failure", "5xx"]`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on: Vec<String>,
    /// Retries as a share of requests (`20%`) so a failing backend is never
    /// retried into the ground. Default `20%`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<String>,
    /// Only these methods are retried (default: idempotent methods).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub methods: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SplitConfig {
    pub tag: String,
    /// Percent of requests, 0–100.
    pub weight: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct OutlierConfig {
    #[serde(
        default,
        rename = "consecutive5xx",
        skip_serializing_if = "Option::is_none"
    )]
    pub consecutive_5xx: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eject_for: Option<String>,
}

/// A parsed `connect.upstreams[].to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamTarget {
    pub host: String,
    pub port: u16,
    /// Originate TLS to the destination.
    pub tls: bool,
    /// Parse HTTP on the path (the `http`/`https` schemes).
    pub http: bool,
}

impl UpstreamTarget {
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// `tcp://host:port`, `tls://host:port`, `http://host[:port]`, or
/// `https://host[:port]`. A path or query on the URL is rejected: the task
/// chooses paths, the upstream chooses the host.
pub fn parse_upstream_target(to: &str) -> R<UpstreamTarget> {
    let to = to.trim();
    let (scheme, rest) = to.split_once("://").ok_or_else(|| {
        anyhow::anyhow!("'{to}' must start with tcp://, tls://, http://, or https://")
    })?;
    let (tls, http, default_port) = match scheme.to_ascii_lowercase().as_str() {
        "tcp" => (false, false, None),
        "tls" => (true, false, None),
        "http" => (false, true, Some(80)),
        "https" => (true, true, Some(443)),
        other => bail!("'{to}': unknown scheme '{other}' (use tcp, tls, http, or https)"),
    };
    let rest = rest.trim_end_matches('/');
    if rest.contains('/') || rest.contains('?') || rest.contains('#') {
        bail!("'{to}' must not carry a path or query; the task chooses paths");
    }
    if rest.is_empty() {
        bail!("'{to}' has no host");
    }
    let (host, port) = if let Some(end) = rest.strip_prefix('[') {
        // IPv6 literal.
        let (host, tail) = end
            .split_once(']')
            .ok_or_else(|| anyhow::anyhow!("'{to}': unterminated IPv6 literal"))?;
        let port = tail
            .strip_prefix(':')
            .map(|p| {
                p.parse::<u16>()
                    .map_err(|_| anyhow::anyhow!("'{to}': invalid port"))
            })
            .transpose()?;
        (host.to_string(), port)
    } else {
        match rest.rsplit_once(':') {
            Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) && !port.is_empty() => (
                host.to_string(),
                Some(
                    port.parse::<u16>()
                        .map_err(|_| anyhow::anyhow!("'{to}': invalid port"))?,
                ),
            ),
            _ => (rest.to_string(), None),
        }
    };
    let port = match port.or(default_port) {
        Some(0) => bail!("'{to}': port must be greater than 0"),
        Some(port) => port,
        None => bail!("'{to}': {scheme}:// needs an explicit port"),
    };
    if host.is_empty() {
        bail!("'{to}' has no host");
    }
    Ok(UpstreamTarget {
        host: host.to_ascii_lowercase(),
        port,
        tls,
        http,
    })
}

/// An upstream resolved at placement time: which loopback port the node-local
/// forwarder listens on for this allocation, and which strategy it balances
/// with. Persisted on the allocation next to its ports.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AllocUpstream {
    /// The upstream's key (`UpstreamConfig::key`): the service name, or the
    /// `name` of an external upstream.
    pub service: String,
    pub local_port: u16,
    /// Resolved strategy name, never empty (see [`DEFAULT_BALANCE`]).
    pub balance: String,
    /// External destination (`UpstreamConfig::to`) when this upstream does
    /// not resolve through the catalog. `default` + `skip_serializing_if` so
    /// rows written before external upstreams existed parse unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    /// Loopback VIP (`127.a.b.n`) the node datapath also listens on, so
    /// `<key>.svc.legion` resolves to it (docs/network-handlers.md,
    /// "Name-is-config"). Assigned at placement from the allocation's
    /// `AllocNetwork::vip_block`; absent for rows placed before VIPs existed
    /// and on platforms without a routable 127/8 (macOS).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vip: Option<String>,
    /// The port the VIP listens on: the upstream's pinned `localPort`, else
    /// the service's declared in-task port, else an external target's port,
    /// else the dynamic loopback port. `db.svc.legion:5432` works because
    /// this is 5432 for a service that declared it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_port: Option<u16>,
}

impl AllocUpstream {
    /// `127.0.0.1:<local_port>` — what `LEGION_UPSTREAM_<SERVICE>` contains.
    pub fn address(&self) -> String {
        format!("{UPSTREAM_LOOPBACK_ADDR}:{}", self.local_port)
    }

    /// `<vip>:<canonical port>` when a VIP was assigned — what
    /// `<key>.svc.legion` resolves to.
    pub fn vip_address(&self) -> Option<String> {
        Some(format!("{}:{}", self.vip.as_ref()?, self.canonical_port?))
    }
}

/// A port assigned to an allocation at placement time.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AllocPort {
    pub name: String,
    /// The advertised port: what the catalog, `resolve`, and health checks
    /// target, and — without an ingress chain — what the task binds.
    pub host_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<u16>,
    /// With an ingress chain (docs/network-handlers.md), the port the task
    /// actually binds: the node datapath owns `host_port` and forwards to
    /// this one through the chain. `LEGION_PORT_<label>` carries this value
    /// so the task's instruction — bind `$LEGION_PORT_<label>` — is unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_port: Option<u16>,
}

impl AllocPort {
    /// The port the task binds.
    pub fn bind_port(&self) -> u16 {
        self.private_port.unwrap_or(self.host_port)
    }
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
    /// The allocation's loopback /24 (`127.a.b`), unique per node, assigned
    /// at placement when the task declares upstreams: `.1` is its DNS
    /// resolver, `.2` onward its upstream VIPs. Linux only — the whole 127/8
    /// is loopback there with no configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vip_block: Option<String>,
    /// The effective `network.mode` resolved at placement: the task's own
    /// (`auto` resolved per runtime) unless a matching `require` policy
    /// imposes one. `gated` means the workload's namespace holds loopback and
    /// the datapath only. Absent on rows placed before modes were resolved
    /// (treated as `host`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
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
            pairs.push((format!("LEGION_PORT_{label}"), port.bind_port().to_string()));
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
            if let Some(vip_address) = upstream.vip_address() {
                // Name-is-config: the same upstream by DNS name, on the port
                // the service declared.
                pairs.push((
                    format!("LEGION_UPSTREAM_HOST_{label}"),
                    format!("{}.{SVC_ZONE}", upstream.service),
                ));
                pairs.push((format!("LEGION_UPSTREAM_VIP_{label}"), vip_address));
            }
        }
        if let Some(resolver) = self.resolver_address() {
            pairs.push(("LEGION_DNS".to_string(), resolver));
        }
        if self.is_gated() {
            // The only door out of a gated namespace. NO_PROXY keeps the
            // task's own loopback, its VIPs, and the zone off the proxy.
            let proxy = format!("http://{GATED_PROXY_ADDR}:{GATED_PROXY_PORT}");
            pairs.push(("LEGION_EGRESS_PROXY".to_string(), proxy.clone()));
            pairs.push(("LEGION_NETWORK_MODE".to_string(), "gated".to_string()));
            pairs.push(("HTTP_PROXY".to_string(), proxy.clone()));
            pairs.push(("HTTPS_PROXY".to_string(), proxy.clone()));
            pairs.push(("ALL_PROXY".to_string(), proxy));
            pairs.push((
                "NO_PROXY".to_string(),
                format!("localhost,127.0.0.0/8,{SVC_ZONE}"),
            ));
        } else if let Some(mode) = self.mode.as_deref() {
            pairs.push(("LEGION_NETWORK_MODE".to_string(), mode.to_string()));
        }
        pairs
    }

    /// Whether the allocation runs in a gated namespace.
    pub fn is_gated(&self) -> bool {
        self.mode.as_deref() == Some(crate::structs::netpolicy::MODE_GATED)
    }

    /// The allocation's resolver address (`127.a.b.1`), when a VIP block was
    /// assigned.
    pub fn resolver_address(&self) -> Option<String> {
        self.vip_block.as_ref().map(|block| format!("{block}.1"))
    }

    /// `<key>` → `(vip, canonical port)` for every upstream with a VIP: what
    /// the allocation's resolver answers for `<key>.svc.legion`.
    pub fn vip_names(&self) -> BTreeMap<String, (String, u16)> {
        self.upstreams
            .iter()
            .filter_map(|upstream| {
                Some((
                    upstream.service.clone(),
                    (upstream.vip.clone()?, upstream.canonical_port?),
                ))
            })
            .collect()
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
    assign_allocation_ports_with_vips(network, connect, used, &HashMap::new(), None)
}

/// [`assign_allocation_ports`] plus VIP assignment: `service_ports` maps a
/// service name to the port it declared (its in-task `to`, or static port),
/// and `vip_block` is the allocation's `127.a.b` block when the platform has
/// one. Upstream `i` gets `<block>.<2 + i>` on its canonical port.
pub fn assign_allocation_ports_with_vips(
    network: Option<&NetworkConfig>,
    connect: Option<&ConnectConfig>,
    used: &HashSet<u16>,
    service_ports: &HashMap<String, u16>,
    vip_block: Option<&str>,
) -> R<(Vec<AllocPort>, Vec<AllocUpstream>)> {
    assign_allocation_ports_full(network, connect, used, service_ports, vip_block, false)
}

/// [`assign_allocation_ports_with_vips`] plus, when `ingress` is set, a
/// second, private port per declared port: the datapath binds the advertised
/// one and the task binds the private one.
pub fn assign_allocation_ports_full(
    network: Option<&NetworkConfig>,
    connect: Option<&ConnectConfig>,
    used: &HashSet<u16>,
    service_ports: &HashMap<String, u16>,
    vip_block: Option<&str>,
    ingress: bool,
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
                private_port: None,
            });
        }
    }
    for upstream in upstreams {
        if let Some(local_port) = upstream.local_port {
            if !taken.insert(local_port) {
                return Err(anyhow::Error::new(StaticPortInUse::for_upstream(
                    local_port,
                    upstream.key(),
                )));
            }
            assigned_upstreams.push(AllocUpstream {
                service: upstream.key().to_string(),
                local_port,
                balance: upstream.balance_name(),
                to: upstream.to.clone(),
                vip: None,
                canonical_port: None,
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
            private_port: None,
        });
    }
    for upstream in upstreams {
        if upstream.local_port.is_some() {
            continue;
        }
        let local_port = next_dynamic_port(
            &mut cursor,
            &mut taken,
            &format!("upstream '{}' local port", upstream.key()),
        )?;
        assigned_upstreams.push(AllocUpstream {
            service: upstream.key().to_string(),
            local_port,
            balance: upstream.balance_name(),
            to: upstream.to.clone(),
            vip: None,
            canonical_port: None,
        });
    }

    // Ingress: every advertised port gets a private twin from the same
    // dynamic range; the datapath owns the advertised one.
    if ingress {
        for port in assigned.iter_mut() {
            let private = next_dynamic_port(
                &mut cursor,
                &mut taken,
                &format!("port '{}' private (ingress) port", port.name),
            )?;
            port.private_port = Some(private);
        }
    }

    // VIPs: in declaration order so `<block>.2` is the first upstream the
    // task wrote, which keeps `legion net doctor` output legible.
    if let Some(block) = vip_block {
        for upstream in upstreams {
            let key = upstream.key();
            let Some(assigned_upstream) = assigned_upstreams.iter_mut().find(|u| u.service == key)
            else {
                continue;
            };
            let index = upstreams.iter().position(|u| u.key() == key).unwrap_or(0);
            let host = usize::from(VIP_FIRST_HOST) + index;
            if host > 254 {
                bail!("upstream '{key}' has no free VIP in block {block} (at most 253 upstreams per task)");
            }
            let canonical_port = upstream
                .local_port
                .or_else(|| service_ports.get(key).copied())
                .or_else(|| {
                    upstream
                        .to
                        .as_deref()
                        .and_then(|to| parse_upstream_target(to).ok())
                        .map(|target| target.port)
                })
                .unwrap_or(assigned_upstream.local_port);
            assigned_upstream.vip = Some(format!("{block}.{host}"));
            assigned_upstream.canonical_port = Some(canonical_port);
        }
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
    use crate::structs::netpolicy::{
        normalize_labels, validate_inline_chain, validate_network_mode, Direction,
    };

    // Mode, labels, and the inline policy chains: normalized first so the
    // stored config is always in its canonical form whether or not the task
    // declares a service.
    if let Some(network_config) = network.as_mut() {
        network_config.mode = network_config.mode.trim().to_ascii_lowercase();
        if network_config.mode.is_empty() {
            network_config.mode = default_network_mode();
        }
        validate_network_mode(&network_config.mode)
            .map_err(|err| anyhow::anyhow!("task {task_name} {err}"))?;
        network_config.labels = normalize_labels(&network_config.labels)
            .map_err(|err| anyhow::anyhow!("task {task_name} {err}"))?;
        if let Some(chain) = network_config.egress.as_mut() {
            validate_inline_chain(task_name, Direction::Egress, chain)?;
        }
        if let Some(chain) = network_config.ingress.as_mut() {
            validate_inline_chain(task_name, Direction::Ingress, chain)?;
        }
    }

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
        let has_service = !upstream.service.trim().is_empty();
        match (has_service, upstream.is_external()) {
            (false, false) => bail!(
                "task {task_name} connect.upstreams entries need a service (a catalog service) or \
                 a name + to (an external destination)"
            ),
            (true, true) => bail!(
                "task {task_name} connect.upstreams entry '{}' sets both service and to; an \
                 upstream is either a catalog service or an external destination",
                upstream.key()
            ),
            (false, true) => {
                if upstream.name.as_deref().is_none_or(|n| n.trim().is_empty()) {
                    bail!(
                        "task {task_name} connect.upstreams entry with to = '{}' needs a name \
                         (it becomes LEGION_UPSTREAM_<NAME>)",
                        upstream.to.as_deref().unwrap_or_default()
                    );
                }
                parse_upstream_target(upstream.to.as_deref().unwrap_or_default()).map_err(
                    |err| {
                        anyhow::anyhow!(
                            "task {task_name} connect.upstreams entry '{}' to: {err}",
                            upstream.key()
                        )
                    },
                )?;
            }
            (true, false) => {}
        }
        if !services.insert(upstream.key()) {
            bail!(
                "task {task_name} connect.upstreams declares '{}' more than once",
                upstream.key()
            );
        }
        if let Some(http) = upstream.http.as_ref() {
            validate_upstream_http(task_name, upstream.key(), http)?;
        }
        for (index, handler) in upstream.handlers.iter().enumerate() {
            let mut handler = handler.clone();
            crate::structs::netpolicy::normalize_handler(
                &format!(
                    "task {task_name} connect.upstreams entry '{}' handlers[{index}]",
                    upstream.key()
                ),
                &mut handler,
                upstream.is_http(),
            )?;
        }
        if let Some(local_port) = upstream.local_port {
            if local_port == 0 {
                bail!(
                    "task {task_name} connect.upstreams entry '{}' localPort must be greater than 0",
                    upstream.key()
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
                    upstream.key()
                );
            }
        }
        if let Some(balance) = upstream.balance.as_deref() {
            let normalized = normalize_balance(balance);
            if !is_known_balance(&normalized) {
                bail!(
                    "task {task_name} connect.upstreams entry '{}' has unknown balance '{balance}'; valid strategies are {}",
                    upstream.key(),
                    balance_strategies().join(", ")
                );
            }
        }
    }

    Ok(())
}

/// Canonicalize a task's `connect` block: handler names and defaults, so the
/// stored form is what the datapath reads.
pub fn normalize_task_connect(task_name: &str, connect: &mut Option<ConnectConfig>) -> R<()> {
    let Some(connect_config) = connect.as_mut() else {
        return Ok(());
    };
    for upstream in connect_config.upstreams.iter_mut() {
        if let Some(name) = upstream.name.as_mut() {
            *name = name.trim().to_string();
        }
        if let Some(to) = upstream.to.as_mut() {
            *to = to.trim().to_string();
        }
        let is_http = upstream.is_http();
        let key = upstream.key().to_string();
        for (index, handler) in upstream.handlers.iter_mut().enumerate() {
            crate::structs::netpolicy::normalize_handler(
                &format!("task {task_name} connect.upstreams entry '{key}' handlers[{index}]"),
                handler,
                is_http,
            )?;
        }
    }
    Ok(())
}

/// Validate an upstream's `http` block.
fn validate_upstream_http(task_name: &str, key: &str, http: &UpstreamHttpConfig) -> R<()> {
    use crate::structs::netpolicy::parse_duration;
    let what = format!("task {task_name} connect.upstreams entry '{key}' http");
    if let Some(timeout) = http.timeout.as_deref() {
        parse_duration(timeout).map_err(|err| anyhow::anyhow!("{what}.timeout: {err}"))?;
    }
    if let Some(retries) = http.retries.as_ref() {
        if retries.attempts.is_some_and(|a| a > 10) {
            bail!("{what}.retries.attempts must be at most 10");
        }
        for on in &retries.on {
            if !["5xx", "connect-failure", "reset", "retriable-4xx"].contains(&on.as_str()) {
                bail!("{what}.retries.on: unknown condition '{on}' (5xx, connect-failure, reset, retriable-4xx)");
            }
        }
        if let Some(budget) = retries.budget.as_deref() {
            let pct: u32 = budget.trim().trim_end_matches('%').parse().map_err(|_| {
                anyhow::anyhow!("{what}.retries.budget must be a percentage like \"20%\"")
            })?;
            if pct > 100 {
                bail!("{what}.retries.budget must be at most 100%");
            }
        }
    }
    let mut total = 0u32;
    let mut tags: HashSet<&str> = HashSet::new();
    for split in &http.split {
        if split.tag.trim().is_empty() {
            bail!("{what}.split entries need a tag");
        }
        if !tags.insert(split.tag.as_str()) {
            bail!("{what}.split lists tag '{}' more than once", split.tag);
        }
        if split.weight > 100 {
            bail!("{what}.split weight for '{}' must be 0..=100", split.tag);
        }
        total += split.weight;
    }
    if total > 100 {
        bail!("{what}.split weights add up to {total}%, which is more than 100%");
    }
    if let Some(outlier) = http.outlier.as_ref() {
        if outlier.consecutive_5xx == Some(0) {
            bail!("{what}.outlier.consecutive5xx must be greater than 0");
        }
        if let Some(eject) = outlier.eject_for.as_deref() {
            parse_duration(eject)
                .map_err(|err| anyhow::anyhow!("{what}.outlier.ejectFor: {err}"))?;
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
                private_port: None,
            }],
            upstreams: Vec::new(),
            vip_block: None,
            mode: None,
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

    /// Ingress: the datapath owns every advertised port and the task binds
    /// a private twin from the same range (docs/network-handlers.md).
    #[test]
    fn ingress_gives_every_advertised_port_a_private_twin() -> R<()> {
        let network = NetworkConfig {
            ports: vec![
                PortConfig {
                    name: "http".into(),
                    static_port: Some(8080),
                    to: None,
                },
                PortConfig {
                    name: "admin".into(),
                    static_port: None,
                    to: None,
                },
            ],
            ..NetworkConfig::default()
        };
        let (ports, _) = assign_allocation_ports_full(
            Some(&network),
            None,
            &HashSet::new(),
            &HashMap::new(),
            None,
            true,
        )?;
        assert_eq!(ports.len(), 2);
        assert_eq!(ports[0].host_port, 8080, "the advertised port is untouched");
        let private = ports[0].private_port.expect("a private twin");
        assert_ne!(private, 8080);
        assert_eq!(ports[0].bind_port(), private, "the task binds the twin");
        let all: HashSet<u16> = ports
            .iter()
            .flat_map(|port| [port.host_port, port.private_port.unwrap()])
            .collect();
        assert_eq!(
            all.len(),
            4,
            "advertised and private ports are all distinct"
        );

        // Without an ingress chain nothing changes: the task binds the
        // advertised port itself.
        let (plain, _) = assign_allocation_ports(Some(&network), None, &HashSet::new())?;
        assert!(plain.iter().all(|port| port.private_port.is_none()));
        assert_eq!(plain[0].bind_port(), 8080);
        Ok(())
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
                to: None,
                vip: None,
                canonical_port: None,
            }],
            vip_block: None,
            mode: None,
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
            err.to_string().contains("declares 'db' more than once"),
            "{err}"
        );
        assert!(err.to_string().contains("task api"), "{err}");
    }

    #[test]
    fn validate_connect_rejects_empty_upstream_service() {
        let connect_config = connect(json!([{ "service": "  " }]));
        let err = validate_task_connect("api", None, Some(&connect_config), None).unwrap_err();
        assert!(err.to_string().contains("need a service"), "{err}");
    }

    #[test]
    fn external_upstreams_parse_validate_and_assign() {
        let target = parse_upstream_target("https://api.openai.com").unwrap();
        assert_eq!(
            (target.host.as_str(), target.port, target.tls, target.http),
            ("api.openai.com", 443, true, true)
        );
        let target = parse_upstream_target("tcp://db.internal:5432").unwrap();
        assert_eq!((target.port, target.tls, target.http), (5432, false, false));
        assert!(
            parse_upstream_target("tcp://db.internal").is_err(),
            "tcp needs a port"
        );
        assert!(
            parse_upstream_target("https://api.openai.com/v1").is_err(),
            "no paths"
        );
        assert!(parse_upstream_target("ftp://x:1").is_err());
        assert_eq!(
            parse_upstream_target("tls://[::1]:8443").unwrap().host,
            "::1"
        );

        // Exactly one of service / to; a name is required with to.
        let bad = connect(json!([{ "to": "https://api.openai.com" }]));
        let err = validate_task_connect("api", None, Some(&bad), None).unwrap_err();
        assert!(err.to_string().contains("needs a name"), "{err}");
        let bad = connect(json!([{ "service": "db", "to": "tcp://x:1" }]));
        let err = validate_task_connect("api", None, Some(&bad), None).unwrap_err();
        assert!(err.to_string().contains("both service and to"), "{err}");
        let bad = connect(json!([{ "name": "x", "to": "nope" }]));
        assert!(validate_task_connect("api", None, Some(&bad), None).is_err());

        // An external upstream keys by name and carries its destination.
        let ok = connect(
            json!([{ "name": "openai", "to": "https://api.openai.com", "localPort": 8443 }]),
        );
        validate_task_connect("api", None, Some(&ok), None).unwrap();
        let (_, upstreams) = assign_allocation_ports(None, Some(&ok), &HashSet::new()).unwrap();
        assert_eq!(upstreams[0].service, "openai");
        assert_eq!(upstreams[0].local_port, 8443);
        assert_eq!(upstreams[0].to.as_deref(), Some("https://api.openai.com"));
        let env = AllocNetwork {
            upstreams: upstreams.clone(),
            ..Default::default()
        }
        .env_pairs();
        assert!(env
            .iter()
            .any(|(k, v)| k == "LEGION_UPSTREAM_OPENAI" && v == "127.0.0.1:8443"));

        // Handlers on an upstream are validated like a chain's.
        let bad = connect(json!([{ "service": "db", "handlers": [{ "handler": "wasm" }] }]));
        assert!(validate_task_connect("api", None, Some(&bad), None).is_err());
        let bad = connect(
            json!([{ "name": "o", "to": "tcp://x:1", "handlers": [{ "handler": "dlp", "config": { "patterns": ["x"] } }] }]),
        );
        let err = validate_task_connect("api", None, Some(&bad), None).unwrap_err();
        assert!(err.to_string().contains("protocol = \"http\""), "{err}");
        let ok = connect(
            json!([{ "name": "o", "to": "https://x", "handlers": [{ "handler": "dlp", "config": { "patterns": ["x"] } }] }]),
        );
        validate_task_connect("api", None, Some(&ok), None).unwrap();

        // http traffic-management block validation.
        let bad = connect(
            json!([{ "service": "api", "http": { "split": [{ "tag": "a", "weight": 60 }, { "tag": "b", "weight": 60 }] } }]),
        );
        let err = validate_task_connect("api", None, Some(&bad), None).unwrap_err();
        assert!(err.to_string().contains("more than 100%"), "{err}");
        let ok = connect(
            json!([{ "service": "api", "http": { "timeout": "3s", "retries": { "attempts": 2, "budget": "20%" }, "outlier": { "consecutive5xx": 5, "ejectFor": "30s" } } }]),
        );
        validate_task_connect("api", None, Some(&ok), None).unwrap();
    }

    #[test]
    fn vip_blocks_and_canonical_ports() {
        let mut used_blocks = HashSet::new();
        assert_eq!(next_vip_block(&used_blocks).unwrap(), "127.42.0");
        used_blocks.insert("127.42.0".to_string());
        assert_eq!(next_vip_block(&used_blocks).unwrap(), "127.42.1");
        for b in 0..=255u16 {
            used_blocks.insert(format!("127.42.{b}"));
        }
        assert!(next_vip_block(&used_blocks).is_err());

        let connect = connect(json!([
            { "service": "db" },
            { "service": "cache", "localPort": 6379 },
            { "name": "openai", "to": "https://api.openai.com" },
            { "service": "unknown" }
        ]));
        let service_ports: HashMap<String, u16> = [("db".to_string(), 5432)].into_iter().collect();
        let (_, upstreams) = assign_allocation_ports_with_vips(
            None,
            Some(&connect),
            &HashSet::new(),
            &service_ports,
            Some("127.42.7"),
        )
        .unwrap();
        let by_key = |key: &str| upstreams.iter().find(|u| u.service == key).unwrap().clone();
        assert_eq!(by_key("db").vip.as_deref(), Some("127.42.7.2"));
        assert_eq!(
            by_key("db").canonical_port,
            Some(5432),
            "the service's declared port"
        );
        assert_eq!(by_key("cache").vip.as_deref(), Some("127.42.7.3"));
        assert_eq!(
            by_key("cache").canonical_port,
            Some(6379),
            "a pinned localPort wins"
        );
        assert_eq!(
            by_key("openai").canonical_port,
            Some(443),
            "an external target's port"
        );
        let unknown = by_key("unknown");
        assert_eq!(
            unknown.canonical_port,
            Some(unknown.local_port),
            "falls back to the dynamic port"
        );

        let network = AllocNetwork {
            upstreams: upstreams.clone(),
            vip_block: Some("127.42.7".into()),
            ..Default::default()
        };
        let env = network.env_pairs();
        assert!(env
            .iter()
            .any(|(k, v)| k == "LEGION_DNS" && v == "127.42.7.1"));
        assert!(env
            .iter()
            .any(|(k, v)| k == "LEGION_UPSTREAM_HOST_DB" && v == "db.svc.legion"));
        assert!(env
            .iter()
            .any(|(k, v)| k == "LEGION_UPSTREAM_VIP_DB" && v == "127.42.7.2:5432"));
        assert_eq!(network.vip_names()["db"], ("127.42.7.2".to_string(), 5432));

        // Without a block nothing changes, including the env.
        let (_, plain) = assign_allocation_ports(None, Some(&connect), &HashSet::new()).unwrap();
        assert!(plain
            .iter()
            .all(|u| u.vip.is_none() && u.canonical_port.is_none()));
        let env = AllocNetwork {
            upstreams: plain,
            ..Default::default()
        }
        .env_pairs();
        assert!(!env
            .iter()
            .any(|(k, _)| k == "LEGION_DNS" || k.starts_with("LEGION_UPSTREAM_HOST_")));
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
