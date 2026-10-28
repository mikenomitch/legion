use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::prelude::{anyhow, bail, R};

/// Inclusive dynamic port range used when a port has no `static` value.
pub const DYNAMIC_PORT_MIN: u16 = 20000;
pub const DYNAMIC_PORT_MAX: u16 = 32000;

/// Loopback address every upstream forwarder binds. Upstreams are a pure
/// userspace TCP hop on the node the task runs on, so the address is fixed and
/// identical on macOS and Linux — no netns, no bridge, no platform branches.
pub const UPSTREAM_LOOPBACK_ADDR: &str = "127.0.0.1";

/// Strategy used when an upstream does not name one. Latency-aware, so a
/// slow-but-alive backend sheds traffic instead of taking its full share
/// (docs/service-discovery.md, "Balancing strategies").
pub const DEFAULT_BALANCE: &str = "least_latency";

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

/// Where a gated `container` or `exec` allocation's resolver listens inside
/// its namespace when it has no VIP block (worker-plane-auth plan §6.12). A
/// fixed address costs no VIP block, and it is the address systemd-resolved
/// hosts already name in `/etc/resolv.conf`, so a gated `exec` task that
/// keeps the host's file reaches its own resolver.
pub const GATED_RESOLVER_ADDR: &str = "127.0.0.53";

/// The node fingerprint attribute naming its OS (`std::env::consts::OS`).
pub const ATTR_NODE_OS: &str = "os";

/// Resolve a task's declared `network.mode` without node facts: an explicit
/// value is itself, and `auto` is `host`. Placement resolves `auto` with the
/// node in hand ([`resolve_network_mode_with`]); submit-time checks that need
/// the same answer use [`task_runs_gated`].
pub fn resolve_network_mode(declared: &str, _runtime: &str) -> String {
    let declared = declared.trim().to_ascii_lowercase();
    if declared.is_empty() || declared == crate::structs::netpolicy::MODE_AUTO {
        crate::structs::netpolicy::MODE_HOST.to_string()
    } else {
        declared
    }
}

/// Gated plan Phase 2: `network.mode = "auto"` (the default) resolves to
/// `gated` for a `sandbox` with no `config.network` on a microVM node, so
/// the safe network is what an author gets by writing nothing.
///
/// **Held at `false` until Phase 0 passes.** Phase 0 is the plan's gate: it
/// boots a gated sandbox on a real VMM for the first time
/// (`internal_docs/suspend-resume-linux-verification.md`, "Gated
/// sandboxes"), and the default must not move to a datapath that has never
/// run. The resolution is implemented and tested with the gate open; flipping
/// this constant ships it. Until then a manager started with
/// [`SANDBOX_GATED_DEFAULT_ENV`]`=1` opens it, for that verification.
pub const SANDBOX_GATED_BY_DEFAULT: bool = false;

/// Manager environment variable that opens [`SANDBOX_GATED_BY_DEFAULT`]'s
/// gate before it ships, for verifying Phase 2 on a KVM host. Not a
/// supported setting.
pub const SANDBOX_GATED_DEFAULT_ENV: &str = "LEGION_SANDBOX_GATED_DEFAULT";

/// Whether `auto` resolves to `gated` for sandboxes in this process: the
/// constant, or the verification override.
pub fn sandbox_gated_by_default() -> bool {
    SANDBOX_GATED_BY_DEFAULT
        || std::env::var(SANDBOX_GATED_DEFAULT_ENV)
            .map(|value| value.trim() == "1")
            .unwrap_or(false)
}

/// What resolving `auto` needs to know about a task and the node it lands
/// on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AutoModeFacts<'a> {
    /// The sandbox declares `config.network` (`none` or `tap`), which is an
    /// explicit choice of its own network.
    pub sandbox_config_network: bool,
    /// The sandbox's `config.hypervisor`, when it sets one.
    pub hypervisor: Option<&'a str>,
    /// The node's `sandbox.vmm` attribute. `None` when there is no node yet
    /// (a submit-time check), which reads as a microVM node, the case the
    /// default exists for.
    pub node_vmm: Option<&'a str>,
    /// The node's `os` fingerprint. A `container` on `linux` is gated by
    /// default. `None` (no node yet, or a node that did not report it) reads
    /// as not Linux, so a submit-time check never assumes a namespace.
    pub node_os: Option<&'a str>,
}

impl<'a> AutoModeFacts<'a> {
    /// The task's half of the facts: `config.network` and `hypervisor`.
    pub fn for_task(task: &'a crate::structs::TaskConfig) -> Self {
        Self::from_config(&task.config)
    }

    /// [`Self::for_task`] from the task's runtime config alone (what the
    /// store keeps in `task_config.config`).
    pub fn from_config(config: &'a Value) -> Self {
        AutoModeFacts {
            sandbox_config_network: config
                .get("network")
                .is_some_and(|network| !network.is_null()),
            hypervisor: config
                .get("hypervisor")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            node_vmm: None,
            node_os: None,
        }
    }

    /// Whether the sandbox runs as a microVM: an explicit VMM, or `auto` on
    /// a node whose default is one. A dev backend (Apple `container`, an
    /// OCI runtime) has no vsock to the datapath.
    pub fn runs_microvm(&self) -> bool {
        match self.hypervisor.map(str::to_ascii_lowercase).as_deref() {
            Some("firecracker" | "fc" | "cloud-hypervisor" | "cloudhypervisor" | "ch") => true,
            Some("dev") => false,
            _ => self
                .node_vmm
                .is_none_or(|vmm| matches!(vmm, "firecracker" | "cloud-hypervisor")),
        }
    }
}

/// Resolve a task's declared `network.mode` at placement. An explicit value
/// is itself, and is never downgraded. `auto` is `gated` for a sandbox with
/// no `config.network` that runs as a microVM, when `sandbox_gated_default`
/// is open ([`sandbox_gated_by_default`]), and `host` everywhere else: other
/// runtimes, a sandbox that declares its own network, and a dev backend,
/// which says so (`egress-unenforced`).
pub fn resolve_network_mode_with(
    declared: &str,
    runtime: &str,
    facts: &AutoModeFacts<'_>,
    sandbox_gated_default: bool,
) -> String {
    let declared = declared.trim().to_ascii_lowercase();
    if !declared.is_empty() && declared != crate::structs::netpolicy::MODE_AUTO {
        return declared;
    }
    // Worker-plane-auth plan §6.12: a Linux container is gated unless it
    // says otherwise. macOS runs each container in its own VM already.
    if runtime == "container"
        && facts
            .node_os
            .is_some_and(|os| os.eq_ignore_ascii_case("linux"))
    {
        return crate::structs::netpolicy::MODE_GATED.to_string();
    }
    if sandbox_gated_default
        && runtime == "sandbox"
        && !facts.sandbox_config_network
        && facts.runs_microvm()
    {
        crate::structs::netpolicy::MODE_GATED.to_string()
    } else {
        crate::structs::netpolicy::MODE_HOST.to_string()
    }
}

/// The OS a task's own constraints pin it to (`os = "linux"`), when they do.
fn constrained_os(task: &crate::structs::TaskConfig) -> Option<String> {
    crate::constraints::ConstraintSet::new(&task.constraints)
        .ok()?
        .equality_bindings()
        .get("os")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
}

/// Warnings for tasks that would run gated with nothing allowing their
/// egress, so an author and an agent see it before the first `403`
/// (worker-plane-auth plan §6.12). `covered` says whether a stored policy
/// object could supply the egress chain; the CLI, which cannot see stored
/// policies, passes `|_| false`.
///
/// A `container` that leaves the mode unset is gated only on Linux nodes
/// ([`resolve_network_mode_with`]); on macOS nodes it runs on host
/// networking. So the warning follows the task's `os` constraint: pinned to
/// macOS it is silent, pinned to Linux it says the task is gated, and
/// unpinned it says the task is gated on Linux nodes.
pub fn gated_egress_warnings(
    config: &crate::structs::JobConfig,
    covered: impl Fn(&crate::structs::TaskConfig) -> bool,
) -> Vec<String> {
    let mut warnings = Vec::new();
    for task in &config.tasks {
        let network = task.network.as_ref();
        if network.is_some_and(|network| network.egress.is_some()) {
            continue;
        }
        let declared = network
            .map(|network| network.mode.trim().to_ascii_lowercase())
            .unwrap_or_default();
        let auto = declared.is_empty() || declared == crate::structs::netpolicy::MODE_AUTO;
        let why = match (task.runtime.as_str(), auto) {
            ("container", true) => match constrained_os(task).as_deref() {
                Some("linux") => "is gated (the default for containers on Linux)",
                // Any other pinned OS (macOS) resolves `auto` to host.
                Some(_) => continue,
                None => "is gated on Linux nodes (the default for containers there)",
            },
            ("container" | "exec", false) if declared == crate::structs::netpolicy::MODE_GATED => {
                "is gated"
            }
            _ => continue,
        };
        if covered(task) {
            continue;
        }
        warnings.push(format!(
            "task {:?} {why} and allows no egress. Outbound connections will be refused \
             with `gated-default`.\nhint: add hosts to network.egress.allow, apply a policy \
             object, or set network.mode = \"host\"",
            task.name
        ));
    }
    warnings
}

/// Warnings for upstreams that do not speak HTTP under an HTTP egress chain
/// whose verdict for them needs a request head: a policy never changes how a
/// stream is parsed, so those connections are refused at connect.
/// `matching` returns the stored policies a task's selector facts match
/// (`job validate` passes none; `job run` and `job plan` pass the stored
/// ones).
pub fn tcp_upstream_http_chain_warnings(
    config: &crate::structs::JobConfig,
    matching: impl Fn(&crate::structs::TaskConfig) -> Vec<crate::structs::netpolicy::NetPolicyObject>,
) -> Vec<String> {
    config
        .tasks
        .iter()
        .flat_map(|task| task_tcp_upstream_warnings(task, &matching(task)))
        .collect()
}

/// [`tcp_upstream_http_chain_warnings`] for one task, under the stored
/// policies `objects` that match it.
pub fn task_tcp_upstream_warnings(
    task: &crate::structs::TaskConfig,
    objects: &[crate::structs::netpolicy::NetPolicyObject],
) -> Vec<String> {
    use crate::structs::netpolicy::{
        deferring_rule, evaluate_connect, merge_policies, ConnectFacts, InlinePolicy, Verdict,
    };
    let mut warnings = Vec::new();
    {
        let Some(connect) = task.connect.as_ref() else {
            return warnings;
        };
        let inline = InlinePolicy {
            egress: task.network.as_ref().and_then(|n| n.egress.as_ref()),
            ..Default::default()
        };
        let policy = merge_policies(objects, inline);
        let Some(chain) = policy.egress.as_ref().filter(|chain| chain.is_http()) else {
            return warnings;
        };
        for upstream in &connect.upstreams {
            let (host, port) = match upstream.destination() {
                Ok(UpstreamDestination::Catalog {
                    service,
                    http: false,
                }) => (service, 0),
                Ok(UpstreamDestination::External(target)) if !target.http => {
                    (target.host.clone(), target.port)
                }
                _ => continue,
            };
            let facts = ConnectFacts {
                host: Some(&host),
                ip: host.parse().ok(),
                port,
                identity: None,
            };
            if evaluate_connect(chain, &facts) != Verdict::Defer {
                continue;
            }
            let rule = deferring_rule(chain, &facts).unwrap_or_else(|| "a rule".into());
            let key = upstream.key();
            warnings.push(format!(
                "task {:?} upstream '{key}' is {}, but the egress policy's rule {rule} needs a \
                 path, method, or header to decide it, so its connections will be refused.\n\
                 hint: allow '{host}' by host ahead of path rules, or write to = \"http://{}\" \
                 if it speaks HTTP",
                task.name,
                if upstream.is_external() {
                    "not HTTP"
                } else {
                    "tcp://"
                },
                upstream.to.trim().trim_start_matches("tcp://"),
            ));
        }
    }
    warnings
}

/// Whether a task's own spec puts it on gated networking on a microVM node:
/// it declares `gated`, or it is a sandbox whose `auto` resolves there.
/// Submit-time checks use this (only the declared mode and the task's
/// config are visible at submit; a stored `require` policy can also impose
/// `gated` at placement).
pub fn task_runs_gated(task: &crate::structs::TaskConfig, sandbox_gated_default: bool) -> bool {
    let declared = task
        .network
        .as_ref()
        .map(|network| network.mode.as_str())
        .unwrap_or(crate::structs::netpolicy::MODE_AUTO);
    resolve_network_mode_with(
        declared,
        &task.runtime,
        &AutoModeFacts::for_task(task),
        sandbox_gated_default,
    ) == crate::structs::netpolicy::MODE_GATED
}

/// Submit-time checks for a `sandbox` on gated networking (gated plan §6.5,
/// §6.8): combinations that cannot work are refused here, with the fix,
/// rather than failing at start or never working at all.
///
/// - An explicit `network.mode = "gated"` with a `config.network`: a gated
///   guest has no NIC (and `none` has no vsock), so the two contradict.
/// - Advertised ports (`network.ports`) on a sandbox whose guest nothing can
///   reach: the node datapath fronts a gated guest's ports over vsock and a
///   TAP guest's on its bridge address, and a guest that is neither has
///   loopback only (`config.network = { mode = "none" }`, or no network
///   config while `auto` resolves to `host`).
pub fn validate_gated_sandbox(
    task: &crate::structs::TaskConfig,
    sandbox_gated_default: bool,
) -> Result<(), String> {
    if task.runtime != "sandbox" {
        return Ok(());
    }
    let declared_gated = task.network.as_ref().is_some_and(|network| {
        network
            .mode
            .trim()
            .eq_ignore_ascii_case(crate::structs::netpolicy::MODE_GATED)
    });
    if declared_gated {
        if let Some(config_network) = task.config.get("network").filter(|value| !value.is_null()) {
            let mode = config_network
                .get("mode")
                .and_then(Value::as_str)
                .unwrap_or("none");
            return Err(format!(
                "task {:?} sets network.mode = \"gated\" and config.network = {{ mode = \"{mode}\" \
                 }}. A gated guest has no NIC\nHint: drop config.network to stay gated, or drop \
                 network.mode to use the guest's own network",
                task.name
            ));
        }
    }
    let runs_gated = task_runs_gated(task, sandbox_gated_default);
    if runs_gated {
        validate_gated_guest_ports(task)?;
    }
    let ports = task
        .network
        .as_ref()
        .map(|network| network.ports.len())
        .unwrap_or(0);
    if ports == 0 || runs_gated {
        return Ok(());
    }
    let config_mode = task
        .config
        .get("network")
        .filter(|value| !value.is_null())
        .map(|network| {
            network
                .get("mode")
                .and_then(Value::as_str)
                .unwrap_or("none")
                .trim()
                .to_ascii_lowercase()
        });
    let why = match config_mode.as_deref() {
        Some("tap") => return Ok(()),
        Some(_) => "config.network = { mode = \"none\" } gives the guest no network device",
        None => {
            "with no config.network and network.mode not \"gated\", the guest has loopback only"
        }
    };
    Err(format!(
        "task {:?} declares network.ports, but nothing can reach a port in its guest: {why}\n\
         Hint: set network.mode = \"gated\" to serve the ports through the node datapath, or \
         wire the guest to a bridge (config.network = {{ mode = \"tap\", bridge = \"br0\" }})",
        task.name
    ))
}

/// Ports a gated guest cannot give its workload, because the guest agent
/// holds them: `127.0.0.1:1080` is its egress proxy relay, guest vsock port
/// 1100 is its exec listener (an advertised port is served on the guest
/// vsock port of the same number), and host vsock ports 1080 and 53 are the
/// egress proxy and the resolver, which an upstream (relayed to the host
/// vsock port of its own `localPort`) would collide with. Dynamic ports come
/// from far above all three.
fn validate_gated_guest_ports(task: &crate::structs::TaskConfig) -> Result<(), String> {
    const PROXY: u16 = 1080;
    const RESOLVER: u16 = 53;
    /// `legion_guest::exec::EXEC_PORT`.
    const EXEC: u16 = 1100;
    let held = task.network.as_ref().and_then(|network| {
        network
            .ports
            .iter()
            .find_map(|port| match port.to.or(port.static_port) {
                Some(PROXY) => Some((
                    port,
                    PROXY,
                    "127.0.0.1:1080 is the guest agent's egress proxy",
                )),
                Some(EXEC) => Some((
                    port,
                    EXEC,
                    "the node serves each port on the guest vsock port of the same number, and \
                     vsock port 1100 is the guest agent's exec listener",
                )),
                _ => None,
            })
    });
    if let Some((port, number, why)) = held {
        return Err(format!(
            "task {:?} binds network.ports {:?} on port {number} in its guest, but in a gated \
             sandbox guest {why}\nHint: pick another port, or drop static and to to take a \
             dynamic one",
            task.name, port.name
        ));
    }
    let upstream = task.connect.as_ref().and_then(|connect| {
        connect
            .upstreams
            .iter()
            .find(|upstream| matches!(upstream.port, Some(PROXY | RESOLVER)))
    });
    if let Some(upstream) = upstream {
        return Err(format!(
            "task {:?} connect.upstreams entry {:?} sets port {}, but a gated sandbox's \
             upstreams are relayed to the node on vsock ports of the same number, and {PROXY} \
             and {RESOLVER} are the guest's egress proxy and resolver\nHint: pick another \
             port, or drop it to take a dynamic one",
            task.name,
            upstream.key(),
            upstream.port.unwrap_or_default()
        ));
    }
    Ok(())
}

/// Why a sandbox that runs without Legion's egress policy because of its
/// backend, not its spec, says so: `auto` on a dev backend while the gated
/// default is open. `None` when this does not apply. Shown as the
/// instance's `detail` and by `legion net doctor`.
pub fn egress_unenforced_detail(
    task: &crate::structs::TaskConfig,
    node_vmm: Option<&str>,
    sandbox_gated_default: bool,
) -> Option<String> {
    if !sandbox_gated_default || task.runtime != "sandbox" {
        return None;
    }
    let declared = task
        .network
        .as_ref()
        .map(|network| network.mode.trim().to_ascii_lowercase())
        .unwrap_or_default();
    if !(declared.is_empty() || declared == crate::structs::netpolicy::MODE_AUTO) {
        return None;
    }
    let facts = AutoModeFacts {
        node_vmm,
        ..AutoModeFacts::for_task(task)
    };
    if facts.sandbox_config_network || facts.runs_microvm() {
        return None;
    }
    let backend = match node_vmm {
        Some("apple-container") => "Apple container has no vsock to the datapath",
        _ => "a dev-mode container has no vsock to the datapath",
    };
    Some(format!(
        "egress-unenforced: dev mode runs sandbox {} without Legion's egress policy ({backend})",
        task.name
    ))
}

/// Whether this platform routes all of 127/8 to loopback without
/// configuration (Linux does; macOS only has `127.0.0.1` on `lo0`).
pub fn platform_supports_vip_blocks() -> bool {
    cfg!(target_os = "linux")
}

/// Whether a node can serve VIP blocks, from its `os` fingerprint: only Linux
/// routes all of 127/8 to loopback. Placement asks this about the node an
/// allocation lands on — never [`platform_supports_vip_blocks`], which
/// answers for the process asking (the manager). A node that reported no
/// `os` gets no block; its upstreams still have their loopback ports.
pub fn node_supports_vip_blocks(node_os: Option<&str>) -> bool {
    node_os.is_some_and(|os| os.eq_ignore_ascii_case("linux"))
}

/// Whether a gated task on a node with this `os` fingerprint is confined
/// rather than namespaced: macOS has no network namespaces, so its runtime
/// keeps the task on the node's loopback instead (Seatbelt for `exec`, pf for
/// an Apple container VM; internal_docs/plans/macos-gated-plan.md).
pub fn node_confines_gated(node_os: Option<&str>) -> bool {
    node_os.is_some_and(|os| os.eq_ignore_ascii_case("macos"))
}

/// The egress proxy address of a confined allocation: the first dynamic
/// port that neither another allocation on the node (`used`) nor this one
/// (`ports`, `upstreams`) holds, on the node's loopback.
pub fn assign_confined_proxy(
    used: &HashSet<u16>,
    ports: &[AllocPort],
    upstreams: &[AllocUpstream],
) -> R<String> {
    let mut taken = used.clone();
    taken.extend(ports.iter().map(|port| port.host_port));
    taken.extend(ports.iter().filter_map(|port| port.private_port));
    taken.extend(upstreams.iter().filter_map(|upstream| upstream.local_port));
    let mut cursor = DYNAMIC_PORT_MIN;
    let port = next_dynamic_port(&mut cursor, &mut taken, "a confined egress proxy")?;
    Ok(format!("{GATED_PROXY_ADDR}:{port}"))
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
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        NetworkConfig {
            mode: default_network_mode(),
            ports: Vec::new(),
            labels: BTreeMap::new(),
            egress: None,
            ingress: None,
            unknown_fields: Default::default(),
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
    /// TLS the node terminates on this port with a certificate that is not
    /// the mesh's (docs/network-handlers.md, "TLS on ports"). The task
    /// speaks plaintext on its private port. Never on the service's mesh
    /// port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<PortTls>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
    /// Unknown keys kept on this extension point under
    /// `allow_unknown_fields`, as written and unvalidated
    /// (`structs::spec_fields`).
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub pending_extensions: crate::structs::spec_fields::PendingExtensions,
}

/// `network.ports[].tls`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct PortTls {
    /// The certificate chain and key: `{ kv = "<path>" }` (one PEM bundle,
    /// swapped live when the key changes) or `{ file = { cert, key } }` (or
    /// one `file` holding both) on the node.
    pub cert: crate::structs::source::Source,
    /// Present: the port is mutual, and clients must present a certificate
    /// that verifies against `ca`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clients: Option<ClientAuth>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

/// `network.ports[].tls.clients`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct ClientAuth {
    /// The CA bundle client certificates are verified against:
    /// `{ kv = "<path>" }` or `{ file = "<path>" }` (PEM).
    pub ca: crate::structs::source::Source,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

impl PortTls {
    /// Validate the sources: a certificate or CA comes from KV or a file,
    /// never an identity.
    pub fn validate(&self, what: &str) -> R<()> {
        use crate::structs::source::SourceKind;
        self.cert
            .check(&format!("{what}.cert"), &[SourceKind::Kv, SourceKind::File])?;
        if let Some(clients) = &self.clients {
            clients.ca.check(
                &format!("{what}.clients.ca"),
                &[SourceKind::Kv, SourceKind::File],
            )?;
        }
        Ok(())
    }
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
    /// Put the service on the mesh: its port terminates node-to-node mTLS
    /// with the node certificate, consumers originate mesh TLS to it, and a
    /// plain client dialing the port directly is refused at the handshake.
    /// A policy object can impose (`require`) or default it. Resolved at
    /// placement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mesh: Option<bool>,
    /// Connect intentions: the jobs allowed to reach this service. Empty (the
    /// default) or `["*"]` means every job in the service's own namespace; a
    /// bare entry (`gateway`, `web-*`) names jobs in that same namespace;
    /// reaching across namespaces takes a qualified entry —
    /// `checkout-prod/api`, `payments-*/api`, `*/*` — so a crossing is always
    /// written on the side being reached (docs/namespaces.md). Always
    /// enforced by the endpoint feed (a consumer not on the list gets no
    /// endpoints); on a mesh service, also by the receiving node against the
    /// verified caller's job.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow: Vec<String>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
    /// Unknown keys kept on this extension point under
    /// `allow_unknown_fields`, as written and unvalidated
    /// (`structs::spec_fields`).
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub pending_extensions: crate::structs::spec_fields::PendingExtensions,
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

    /// Whether `consumer` (a canonical job id) may connect to this service,
    /// declared by job `owner`. See [`allow_permits`].
    pub fn allows_job(&self, owner: &str, consumer: &str) -> bool {
        allow_permits(&self.allow, owner, consumer)
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

/// The workload identity subject of a task: `<region>:<job>:<group>:<task>`,
/// as the mesh names a caller and `service.allow` matches it.
pub fn workload_subject(job_id: &str, group: &str, task_name: &str) -> String {
    format!(
        "{}:{job_id}:{group}:{task_name}",
        crate::constants::DEF_CLUSTER_REGION
    )
}

/// The job id a workload identity subject (`<region>:<job>:<group>:<task>`)
/// names, or `None` for a peer that named no workload (`node:<id>`).
pub fn identity_job(identity: &str) -> Option<&str> {
    let mut parts = identity.split(':');
    let job = parts.nth(1)?;
    (parts.count() == 2 && !job.is_empty()).then_some(job)
}

/// Shared intention check so the catalog projection and `ServiceConfig` agree.
///
/// `owner` is the job that declares the service and `consumer` the job
/// dialing it, both canonical ids. An empty list or a `"*"` entry permits
/// every consumer in the owner's namespace; a bare entry is a job-name glob
/// in that namespace; a qualified entry (`ns-glob/name-glob`) is the only way
/// to admit a consumer from another namespace — the target-side half of a
/// crossing whose consumer-side half is the upstream's `namespace`.
pub fn allow_permits(allow: &[String], owner: &str, consumer: &str) -> bool {
    use crate::structs::acl::glob_match;
    use crate::structs::namespace::split_job_id;
    let (owner_namespace, _) = split_job_id(owner);
    let (consumer_namespace, consumer_name) = split_job_id(consumer);
    let same_namespace = owner_namespace == consumer_namespace;
    if allow.is_empty() {
        return same_namespace;
    }
    allow.iter().any(|entry| match entry.split_once('/') {
        Some((namespace, name)) => {
            glob_match(namespace, consumer_namespace) && glob_match(name, consumer_name)
        }
        None => same_namespace && glob_match(entry, consumer_name),
    })
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
    /// Config files rendered from the upstreams and their endpoint feed
    /// (docs/service-discovery.md, "Templates").
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub templates: Vec<ConnectTemplate>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

impl ConnectConfig {
    pub fn is_empty(&self) -> bool {
        self.upstreams.is_empty()
    }
}

/// One rendered file: `{{ service "x" }}`, `{{ range endpoints "x" }}`,
/// `{{ env "X" }}` over the task's upstreams, re-rendered on every heartbeat
/// that changes the result.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConnectTemplate {
    /// Unique within the task; names the file in status and problems.
    pub name: String,
    /// Where the file lands, relative to the task directory (or an absolute
    /// path inside the runtime's task-dir mount).
    pub destination: String,
    /// The template text.
    pub source: String,
    /// `noop` (default: rewrite the file), `signal` (send `changeSignal`),
    /// or `restart`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_signal: Option<String>,
    /// Random delay before acting on a change, so a fleet does not reload
    /// in lockstep. Default `0s`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub splay: Option<String>,
    /// Wait for the feed to settle this long between actions (the file is
    /// rewritten at once; the signal or restart waits). Default `0s`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coalesce: Option<String>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

/// One declared upstream. `to` names it, and its scheme alone decides the
/// protocol:
///
/// - a catalog service, by its bare name (`db`, or `db.svc.legion`): L4;
///   `tcp://db` is the same; `http://search` parses HTTP;
/// - an external destination, by URL: `tcp://host:port`, `tls://host:port`,
///   `http://host[:port]`, or `https://host[:port]`. The `tls`/`https` forms
///   make the datapath originate TLS, so the task speaks plaintext to its
///   upstream address and the body is visible to `handlers` without
///   intercepting anyone's TLS. An external upstream needs a `name`.
///
/// The task gets one address per upstream, `LEGION_UPSTREAM_<KEY>`: the
/// upstream's VIP and canonical port where the node has a VIP block, else a
/// loopback port. `port` pins the canonical port and also binds
/// `127.0.0.1:<port>`, for a client config that needs a literal loopback
/// address. `balance` overrides the default strategy for this upstream only.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamConfig {
    /// A catalog service name or an external URL (see above).
    pub to: String,
    /// The namespace the catalog service `to` names lives in, when it is not
    /// the consumer's own (docs/namespaces.md, "Crossing a namespace"). The
    /// service must admit the consumer with a qualified `allow` entry, so a
    /// crossing is written on both sides. Not allowed on an external
    /// destination.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// The key (`LEGION_UPSTREAM_<NAME>`): defaults to the service name
    /// (`<service>.<namespace>` across a namespace); required for an
    /// external destination.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Pin the canonical port, and also bind `127.0.0.1:<port>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance: Option<String>,
    /// HTTP traffic management. Only on an upstream whose `to` speaks HTTP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<UpstreamHttpConfig>,
    /// Handlers on this upstream's path, run after the effective egress
    /// policy and before the dial.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub handlers: Vec<crate::structs::hops::HopConfig>,
    /// Credential injection (docs/network-handlers.md, "Credential
    /// injection"): headers the node sets on every request it sends to an
    /// external `https://` upstream, with values the task never sees.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub credentials: Vec<UpstreamCredential>,
    /// The trust bundle an external `https://` or `tls://` upstream's
    /// certificate is verified against, instead of the public roots:
    /// `{ kv = "<path>" }` or `{ file = "<path>" }` (PEM).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca: Option<crate::structs::source::Source>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

/// Where an upstream's `to` points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpstreamDestination {
    /// A catalog service, dialed from the endpoint feed.
    Catalog { service: String, http: bool },
    /// A fixed external destination.
    External(UpstreamTarget),
}

impl UpstreamDestination {
    pub fn is_http(&self) -> bool {
        match self {
            UpstreamDestination::Catalog { http, .. } => *http,
            UpstreamDestination::External(target) => target.http,
        }
    }
}

/// A service name as `to` accepts it bare: letters, digits, `-`, `_`.
fn is_service_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Parse an upstream's `to`: a bare name (no dot, no port) or a
/// `<name>.svc.legion` host is a catalog service; anything else is an
/// external URL, which needs a scheme. A dotless external host is written
/// with its port (`http://wiki:80`).
pub fn parse_upstream_to(to: &str) -> R<UpstreamDestination> {
    let to = to.trim();
    if to.is_empty() {
        bail!(
            "to is required: a catalog service (`db`, `http://search`) or an external URL \
             (`https://api.example.com`)"
        );
    }
    let (scheme, rest) = match to.split_once("://") {
        Some((scheme, rest)) => (Some(scheme.to_ascii_lowercase()), rest),
        None => (None, to),
    };
    let host = rest.trim_end_matches('/');
    let zoned = host.strip_suffix(&format!(".{SVC_ZONE}"));
    let bare = (!host.contains('.') && !host.contains(':') && !host.contains('/')).then_some(host);
    if let Some(name) = zoned.or(bare) {
        if is_service_name(name) {
            return match scheme.as_deref() {
                None | Some("tcp") => Ok(UpstreamDestination::Catalog {
                    service: name.to_string(),
                    http: false,
                }),
                Some("http") => Ok(UpstreamDestination::Catalog {
                    service: name.to_string(),
                    http: true,
                }),
                Some("tls" | "https") => bail!(
                    "'{to}': catalog services are reached over the mesh, so tls:// and \
                     https:// name external destinations. Hint: write to = \"http://{name}\", \
                     and put the {name} service on the mesh (service.mesh = true)"
                ),
                Some(other) => {
                    bail!("'{to}': unknown scheme '{other}' (use tcp, tls, http, or https)")
                }
            };
        }
        if zoned.is_some() || scheme.is_none() {
            bail!(
                "'{to}': '{name}' is not a service name (letters, digits, '-', '_'). Hint: an \
                 external destination is a URL such as tcp://host:port or https://host"
            );
        }
    }
    if scheme.is_none() {
        bail!(
            "'{to}': an external destination needs a scheme: tcp://host:port, tls://host:port, \
             http://host, or https://host. A catalog service is written as its bare name (`db`)"
        );
    }
    Ok(UpstreamDestination::External(parse_upstream_target(to)?))
}

/// One injected header on an external `https://` upstream. Whatever value
/// the task sent for `header` is dropped and replaced with `prefix` + the
/// secret, so a placeholder the task's SDK insists on is harmless.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamCredential {
    /// The request header to set (`x-api-key`, `Authorization`). Stored
    /// lower-cased; HTTP header names are case-insensitive.
    pub header: String,
    /// Text placed before the secret (`"Bearer "`). Optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Where the secret comes from: `{ kv = "<path>" }` or
    /// `{ identity = "<task identity>" }`. Never a `file`: credentials stay
    /// cluster-held, so a node's disk never becomes a secret store.
    pub from: crate::structs::source::Source,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

impl UpstreamCredential {
    /// The KV path this credential reads, when it is KV-backed.
    pub fn kv_path(&self) -> Option<&str> {
        self.from.kv_path()
    }

    /// The task identity this credential injects, when it is
    /// identity-backed.
    pub fn identity_name(&self) -> Option<&str> {
        self.from.identity_name()
    }

    /// Where the value comes from, as the jobspec names it, for status
    /// lines and logs: `kv secrets/openai` or `identity api`.
    pub fn source(&self) -> String {
        self.from.to_string()
    }
}

/// Headers a credential may not name: hop-by-hop headers are stripped
/// between hops, and `host` (plus the framing headers) is the datapath's to
/// set. Injecting any of them would either vanish or corrupt the request.
const CREDENTIAL_RESERVED_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

/// Whether `name` is a syntactically valid HTTP header name (RFC 9110
/// token characters).
fn is_header_token(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

/// Validate where a service upstream points: `namespace` is a namespace
/// name, and only a catalog service has one.
fn validate_upstream_namespace(task_name: &str, upstream: &UpstreamConfig) -> R<()> {
    let Some(namespace) = upstream.namespace.as_deref() else {
        return Ok(());
    };
    let what = format!(
        "task {task_name} connect.upstreams entry '{}'",
        upstream.key()
    );
    if upstream.is_external() {
        bail!(
            "{what} sets namespace on an external destination; namespace names where a catalog \
             service lives"
        );
    }
    crate::structs::namespace::validate_namespace_name(namespace.trim())
        .map_err(|err| anyhow!("{what} namespace: {err}"))
}

/// Validate one upstream's `credentials`: only on an external `https://`
/// upstream, a valid non-reserved header named at most once, a provider.
fn validate_upstream_credentials(task_name: &str, upstream: &UpstreamConfig) -> R<()> {
    if upstream.credentials.is_empty() {
        return Ok(());
    }
    let key = upstream.key();
    let what = format!("task {task_name} connect.upstreams entry '{key}' credentials");
    if !upstream.is_external() {
        bail!(
            "{what}: credentials need an external https:// upstream (name + to); a catalog \
             service upstream is reached over the mesh, not the public internet. Hint: declare \
             {{ name = \"{key}\", to = \"https://<host>\", credentials = [...] }}, or have the \
             service read the secret itself"
        );
    }
    let to = upstream.to.trim();
    let scheme = to
        .split_once("://")
        .map(|(scheme, _)| scheme.to_ascii_lowercase())
        .unwrap_or_default();
    match scheme.as_str() {
        "https" => {}
        "http" => bail!(
            "{what}: to = '{to}' is plaintext, so the injected secret would cross the network \
             in the clear. Hint: use https:// (the node originates TLS; the task still speaks \
             plain HTTP to its loopback port)"
        ),
        _ => bail!(
            "{what}: to = '{to}' is not HTTP, so there is no request head to put a header in. \
             Hint: credentials need an https:// upstream; for a tcp:// or tls:// destination, \
             deliver the secret to the task (env or a KV file) instead"
        ),
    }
    let mut seen: HashSet<String> = HashSet::new();
    for (index, credential) in upstream.credentials.iter().enumerate() {
        let header = credential.header.trim().to_ascii_lowercase();
        let at = format!("{what}[{index}]");
        if header.is_empty() {
            bail!("{at}: header is required (e.g. header = \"x-api-key\")");
        }
        if !is_header_token(&header) {
            bail!(
                "{at}: '{}' is not a valid HTTP header name",
                credential.header
            );
        }
        if CREDENTIAL_RESERVED_HEADERS.contains(&header.as_str()) {
            bail!(
                "{at}: header '{}' is a hop-by-hop or framing header the datapath owns and \
                 cannot be injected",
                credential.header
            );
        }
        if !seen.insert(header.clone()) {
            bail!("{what}: header '{header}' is injected more than once");
        }
        if let Some(prefix) = credential.prefix.as_deref() {
            if prefix.chars().any(|c| c == '\r' || c == '\n' || c == '\0') {
                bail!("{at}: prefix must not contain CR, LF, or NUL");
            }
        }
        use crate::structs::source::SourceKind;
        credential
            .from
            .check(
                &format!("{at}: from"),
                &[SourceKind::Kv, SourceKind::Identity],
            )
            .map_err(|err| {
                anyhow!(
                    "{err}. Hint: from = {{ kv = \"secrets/{key}\" }} or \
                     {{ identity = \"<task identity name>\" }}"
                )
            })?;
        if let Some(name) = credential.identity_name() {
            crate::structs::workload_identity::validate_identity_name(name)
                .map_err(|err| anyhow!("{at}: from.identity: {err}"))?;
            if name == crate::structs::workload_identity::CONTROL_PLANE_IDENTITY_NAME {
                bail!(
                    "{at}: from.identity = \"{name}\" is the control-plane identity, which \
                     authenticates to Legion itself; injecting it would hand a Legion \
                     credential to {to}. Hint: declare an identity whose audience is \
                     that origin"
                );
            }
        }
    }
    Ok(())
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

    /// Where `to` points; `Err` for a `to` validation rejects.
    pub fn destination(&self) -> R<UpstreamDestination> {
        parse_upstream_to(&self.to)
    }

    /// The catalog service this upstream dials, when it dials one.
    pub fn service(&self) -> Option<String> {
        match self.destination() {
            Ok(UpstreamDestination::Catalog { service, .. }) => Some(service),
            _ => None,
        }
    }

    /// Whether this upstream dials a fixed external destination rather than
    /// a catalog service.
    pub fn is_external(&self) -> bool {
        matches!(self.destination(), Ok(UpstreamDestination::External(_)))
    }

    /// The external URL, when this upstream has one.
    pub fn external_to(&self) -> Option<String> {
        self.is_external().then(|| self.to.trim().to_string())
    }

    /// The key this upstream is known by: `name` when given, else the
    /// service, written `<service>.<namespace>` when the upstream crosses
    /// into another namespace. It is what `LEGION_UPSTREAM_<KEY>`,
    /// `<key>.svc.legion`, and `AllocUpstream.key` carry, so a crossing stays
    /// visible wherever the task uses it.
    pub fn key(&self) -> String {
        match self.name.as_deref() {
            Some(name) if !name.trim().is_empty() => name.trim().to_string(),
            _ => {
                let service = self.service().unwrap_or_default();
                match self.target_namespace() {
                    Some(namespace) => format!("{service}.{namespace}"),
                    None => service,
                }
            }
        }
    }

    /// The namespace this upstream names for its service, if any. `None`
    /// means the consumer's own namespace.
    pub fn target_namespace(&self) -> Option<&str> {
        self.namespace
            .as_deref()
            .map(str::trim)
            .filter(|namespace| !namespace.is_empty())
    }

    /// The canonical service reference this upstream dials from
    /// `consumer_job`: the named namespace, else the consumer's own. A bare
    /// service never resolves across a namespace boundary. `None` for an
    /// external destination.
    pub fn service_ref(&self, consumer_job: &str) -> Option<String> {
        let service = self.service()?;
        let namespace = self
            .target_namespace()
            .unwrap_or_else(|| crate::structs::namespace::job_namespace(consumer_job));
        Some(crate::structs::namespace::job_id(namespace, &service))
    }

    /// Whether the datapath parses HTTP on this upstream: its scheme says so.
    pub fn is_http(&self) -> bool {
        self.destination().is_ok_and(|d| d.is_http())
    }
}

/// HTTP traffic management for one HTTP upstream (service-mesh-plan §3.4).
/// Every field is optional. An endpoint that answers 5xx five times in a row
/// is ejected for 30 seconds, with no setting to write.
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
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
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
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SplitConfig {
    pub tag: String,
    /// Percent of requests, 0–100.
    pub weight: u32,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
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
    pub key: String,
    /// The loopback port the datapath also binds (`127.0.0.1:<port>`): a
    /// pinned `port`, or one assigned from the dynamic range where the node
    /// has no VIP block (macOS, or the blocks are used up) or the runtime
    /// needs one (a sandbox's vsock relay). Absent when the VIP is the only
    /// address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_port: Option<u16>,
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
    /// The port the VIP listens on: the upstream's pinned `port`, else the
    /// service's declared in-task port, else an external target's port, else
    /// a port from the dynamic range. `db.svc.legion:5432` works because
    /// this is 5432 for a service that declared it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_port: Option<u16>,
}

impl AllocUpstream {
    /// The one address a task dials for this upstream, as host and port:
    /// the VIP on its canonical port where one was assigned, else the
    /// loopback port. `LEGION_UPSTREAM_<KEY>` and `{{ service "<key>" }}`
    /// both read this.
    pub fn dial(&self) -> Option<(String, u16)> {
        match (self.vip.as_deref(), self.canonical_port) {
            (Some(vip), Some(port)) => Some((vip.to_string(), port)),
            _ => self
                .local_port
                .map(|port| (UPSTREAM_LOOPBACK_ADDR.to_string(), port)),
        }
    }

    /// [`Self::dial`] as `host:port`.
    pub fn address(&self) -> String {
        self.dial()
            .map(|(host, port)| format!("{host}:{port}"))
            .unwrap_or_default()
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
    /// The port terminates mesh mTLS: it is the service's port, and the
    /// effective `service.mesh` (the task's, or a policy's) was on at
    /// placement. Consumers originate mesh TLS to it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub mesh: bool,
    /// The port's own `tls`, copied from the task at placement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<PortTls>,
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
    /// The resolver address of a gated allocation without a VIP block
    /// ([`GATED_RESOLVER_ADDR`], inside its namespace). Absent otherwise: a
    /// VIP block's resolver is its `.1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolver: Option<String>,
    /// The effective `network.mode` resolved at placement: the task's own
    /// (`auto` resolved per runtime) unless a matching `require` policy
    /// imposes one. `gated` means the workload's namespace holds loopback and
    /// the datapath only. Absent on rows placed before modes were resolved
    /// (treated as `host`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// A gated allocation on a node with no network namespaces (macOS):
    /// where its egress proxy listens on the node's loopback
    /// (`127.0.0.1:<port>`, assigned at placement). Its presence is what
    /// makes the allocation *confined* rather than namespaced
    /// (internal_docs/plans/macos-gated-plan.md §6.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress_proxy: Option<String>,
}

impl AllocNetwork {
    /// Whether the allocation is gated without a namespace: its listeners
    /// are on the node's loopback and its runtime confines it (Seatbelt for
    /// `exec`, pf for an Apple container VM).
    pub fn is_confined(&self) -> bool {
        self.is_gated() && self.egress_proxy.is_some()
    }

    /// The node-loopback ports a confined task may reach: its egress proxy
    /// and its upstreams' loopback ports. Its Seatbelt profile or pf rules
    /// allow these and nothing else.
    pub fn confined_ports(&self) -> Vec<u16> {
        let proxy = self
            .egress_proxy
            .as_deref()
            .and_then(|addr| addr.parse::<std::net::SocketAddr>().ok())
            .map(|addr| addr.port());
        let mut ports: Vec<u16> = proxy
            .into_iter()
            .chain(
                self.upstreams
                    .iter()
                    .filter_map(|upstream| upstream.local_port),
            )
            .collect();
        ports.sort_unstable();
        ports.dedup();
        ports
    }

    /// Whether any port terminates mesh mTLS (the node needs its mesh
    /// certificate before the port can serve).
    pub fn serves_mesh(&self) -> bool {
        self.ports.iter().any(|port| port.mesh)
    }

    pub fn port(&self, label: &str) -> Option<&AllocPort> {
        self.ports.iter().find(|port| port.name == label)
    }

    pub fn upstream(&self, key: &str) -> Option<&AllocUpstream> {
        self.upstreams.iter().find(|upstream| upstream.key == key)
    }

    /// Env pairs for the assigned ports: LEGION_PORT_<L>, LEGION_ADDR_<L>,
    /// LEGION_IP_<L>, LEGION_PORT_TO_<L>; plus, per upstream, the one address
    /// to dial: LEGION_UPSTREAM_<K> (`host:port`), LEGION_UPSTREAM_HOST_<K>,
    /// and LEGION_UPSTREAM_PORT_<K>.
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
            let label = sanitize_env_label(&upstream.key);
            let Some((host, port)) = upstream.dial() else {
                continue;
            };
            pairs.push((format!("LEGION_UPSTREAM_{label}"), format!("{host}:{port}")));
            pairs.push((format!("LEGION_UPSTREAM_HOST_{label}"), host));
            pairs.push((format!("LEGION_UPSTREAM_PORT_{label}"), port.to_string()));
        }
        if let Some(resolver) = self.resolver_address() {
            pairs.push(("LEGION_DNS".to_string(), resolver));
        }
        if self.is_gated() {
            // The only door out of a gated namespace. NO_PROXY keeps the
            // task's own loopback, its VIPs, and the zone off the proxy.
            // Both cases, because tools disagree: curl ignores upper-case
            // HTTP_PROXY for plain HTTP, and apt reads only lower case.
            // NODE_USE_ENV_PROXY makes Node's built-in fetch and http honor
            // them (Node 22.21 and 24.5 or later; older Node ignores it).
            let proxy = match self.egress_proxy.as_deref() {
                Some(addr) => format!("http://{addr}"),
                None => format!("http://{GATED_PROXY_ADDR}:{GATED_PROXY_PORT}"),
            };
            let no_proxy = format!("localhost,127.0.0.0/8,{SVC_ZONE}");
            pairs.push(("LEGION_EGRESS_PROXY".to_string(), proxy.clone()));
            pairs.push(("LEGION_NETWORK_MODE".to_string(), "gated".to_string()));
            for (upper, lower) in [
                ("HTTP_PROXY", "http_proxy"),
                ("HTTPS_PROXY", "https_proxy"),
                ("ALL_PROXY", "all_proxy"),
            ] {
                pairs.push((upper.to_string(), proxy.clone()));
                pairs.push((lower.to_string(), proxy.clone()));
            }
            pairs.push(("NO_PROXY".to_string(), no_proxy.clone()));
            pairs.push(("no_proxy".to_string(), no_proxy));
            pairs.push(("NODE_USE_ENV_PROXY".to_string(), "1".to_string()));
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
        self.vip_block
            .as_ref()
            .map(|block| format!("{block}.1"))
            .or_else(|| self.resolver.clone())
    }

    /// `<key>` → `(vip, canonical port)` for every upstream with a VIP: what
    /// the allocation's resolver answers for `<key>.svc.legion`.
    pub fn vip_names(&self) -> BTreeMap<String, (String, u16)> {
        self.upstreams
            .iter()
            .filter_map(|upstream| {
                Some((
                    upstream.key.clone(),
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
    /// The node's `publicAddress`, when its worker declares one: where a
    /// client outside the private network reaches `port`. Display only;
    /// nothing in the cluster dials it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_address: Option<String>,
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
    /// The advertised port terminates mesh TLS (its `AllocPort::mesh`);
    /// consumers must dial it with their node certificate.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub mesh_tls: bool,
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
    assign_allocation_ports_full(
        network,
        connect,
        used,
        service_ports,
        vip_block,
        false,
        false,
        false,
    )
}

/// [`assign_allocation_ports_with_vips`] plus, when `ingress` is set, a
/// second, private port per declared port: the datapath binds the advertised
/// one and the task binds the private one.
///
/// Upstreams: with a VIP block, each upstream's address is its VIP on the
/// canonical port, and it takes no port from the node's range unless it pins
/// one (`port`, which also binds `127.0.0.1:<port>`) or `loopback_upstreams`
/// says the runtime needs a loopback port for each (a sandbox's vsock relay
/// is keyed by it). Without a VIP block every upstream gets a loopback port.
#[allow(clippy::too_many_arguments)]
pub fn assign_allocation_ports_full(
    network: Option<&NetworkConfig>,
    connect: Option<&ConnectConfig>,
    used: &HashSet<u16>,
    service_ports: &HashMap<String, u16>,
    vip_block: Option<&str>,
    ingress: bool,
    gated: bool,
    loopback_upstreams: bool,
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
                mesh: false,
                tls: port.tls.clone(),
            });
        }
    }
    for upstream in upstreams {
        if let Some(local_port) = upstream.port {
            if !taken.insert(local_port) {
                return Err(anyhow::Error::new(StaticPortInUse::for_upstream(
                    local_port,
                    &upstream.key(),
                )));
            }
            assigned_upstreams.push(AllocUpstream {
                key: upstream.key(),
                local_port: Some(local_port),
                balance: upstream.balance_name(),
                to: upstream.external_to(),
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
            mesh: false,
            tls: port.tls.clone(),
        });
    }
    let loopback = loopback_upstreams || vip_block.is_none();
    for upstream in upstreams {
        if upstream.port.is_some() {
            continue;
        }
        let local_port = if loopback {
            Some(next_dynamic_port(
                &mut cursor,
                &mut taken,
                &format!("upstream '{}' local port", upstream.key()),
            )?)
        } else {
            None
        };
        assigned_upstreams.push(AllocUpstream {
            key: upstream.key(),
            local_port,
            balance: upstream.balance_name(),
            to: upstream.external_to(),
            vip: None,
            canonical_port: None,
        });
    }

    // Ingress: every advertised port gets a private twin; the datapath owns
    // the advertised one. Inside a gated namespace the twin is the port's
    // `to`, else the advertised number itself: the namespace has its own
    // ports, so an image that listens on a fixed port needs no change. On
    // the host it comes from the same dynamic range.
    if ingress {
        for port in assigned.iter_mut() {
            let private = if gated {
                port.to.unwrap_or(port.host_port)
            } else {
                next_dynamic_port(
                    &mut cursor,
                    &mut taken,
                    &format!("port '{}' private (ingress) port", port.name),
                )?
            };
            port.private_port = Some(private);
        }
    }

    // VIPs: in declaration order so `<block>.2` is the first upstream the
    // task wrote, which keeps `legion net doctor` output legible.
    if let Some(block) = vip_block {
        for (index, upstream) in upstreams.iter().enumerate() {
            let key = upstream.key();
            let Some(assigned_upstream) = assigned_upstreams.iter_mut().find(|u| u.key == key)
            else {
                continue;
            };
            let host = usize::from(VIP_FIRST_HOST) + index;
            if host > 254 {
                bail!("upstream '{key}' has no free VIP in block {block} (at most 253 upstreams per task)");
            }
            let known = upstream
                .port
                .or_else(|| service_ports.get(&key).copied())
                .or_else(|| match upstream.destination() {
                    Ok(UpstreamDestination::External(target)) => Some(target.port),
                    _ => None,
                })
                .or(assigned_upstream.local_port);
            // A service nobody has declared yet has no port of its own: the
            // VIP listens on one from the dynamic range, which no other
            // allocation on the node will be handed.
            let canonical_port = match known {
                Some(port) => port,
                None => next_dynamic_port(
                    &mut cursor,
                    &mut taken,
                    &format!("upstream '{key}' canonical port"),
                )?,
            };
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
        for tls in network_config
            .ports
            .iter_mut()
            .filter_map(|port| port.tls.as_mut())
        {
            tls.cert.trim();
            if let Some(clients) = tls.clients.as_mut() {
                clients.ca.trim();
            }
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
                tls: None,
                pending_extensions: Default::default(),
                unknown_fields: Default::default(),
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
            if let Some(tls) = &port.tls {
                tls.validate(&format!(
                    "task {task_name} network.ports entry '{}' tls",
                    port.name
                ))?;
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
        if service_config.mesh == Some(true) {
            let label = service_config.port_label().unwrap_or_default();
            let own_tls = network
                .and_then(|network| network.ports.iter().find(|port| port.name == label))
                .is_some_and(|port| port.tls.is_some());
            if own_tls {
                bail!(
                    "task {task_name} service.mesh = true, but its port '{label}' also has its own \
                     tls. A port is either the service's mesh port or carries its own tls, never \
                     both. Hint: drop the port's tls (the mesh terminates with the node \
                     certificate), or set service.mesh = false"
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
            let entry = entry.trim();
            if entry.is_empty() {
                bail!("task {task_name} service.allow entries must not be empty");
            }
            if entry.contains(':') {
                bail!(
                    "task {task_name} service.allow entry '{entry}' names a workload identity; \
                     allow names jobs (`web`, `payments-*/api`). Match a caller's task with an \
                     ingress `identity` rule"
                );
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

    let mut keys: HashSet<String> = HashSet::new();
    let mut local_ports: HashSet<u16> = HashSet::new();
    for upstream in &connect_config.upstreams {
        let destination = upstream.destination().map_err(|err| {
            anyhow::anyhow!(
                "task {task_name} connect.upstreams entry to = {:?}: {err}",
                upstream.to
            )
        })?;
        if matches!(destination, UpstreamDestination::External(_))
            && upstream.name.as_deref().is_none_or(|n| n.trim().is_empty())
        {
            bail!(
                "task {task_name} connect.upstreams entry with to = '{}' needs a name \
                 (it becomes LEGION_UPSTREAM_<NAME>)",
                upstream.to.trim()
            );
        }
        let key = upstream.key();
        if !keys.insert(key.clone()) {
            bail!("task {task_name} connect.upstreams declares '{key}' more than once");
        }
        validate_upstream_namespace(task_name, upstream)?;
        if let Some(http) = upstream.http.as_ref() {
            if !destination.is_http() {
                bail!(
                    "task {task_name} connect.upstreams entry '{key}' to = \"{}\" is reached \
                     over L4, but it has an http block. Hint: write to = \"http://{}\" if it \
                     speaks HTTP",
                    upstream.to.trim(),
                    upstream.to.trim().trim_start_matches("tcp://")
                );
            }
            validate_upstream_http(task_name, &key, http)?;
        }
        validate_upstream_credentials(task_name, upstream)?;
        if let Some(ca) = upstream.ca.as_ref() {
            use crate::structs::source::SourceKind;
            let what = format!("task {task_name} connect.upstreams entry '{key}' ca");
            let originates = matches!(
                &destination,
                UpstreamDestination::External(target) if target.tls
            );
            if !originates {
                bail!(
                    "{what}: ca pins the trust bundle the node verifies an external tls:// or \
                     https:// destination against, and '{}' is not one. A catalog service on \
                     the mesh is verified against the cluster CA",
                    upstream.to.trim()
                );
            }
            ca.check(&what, &[SourceKind::Kv, SourceKind::File])?;
        }
        for (index, handler) in upstream.handlers.iter().enumerate() {
            let mut handler = handler.clone();
            handler.normalize(
                &format!("task {task_name} connect.upstreams entry '{key}' handlers[{index}]"),
                destination.is_http(),
                &format!("upstream '{key}' is not HTTP. Give it an http:// or https:// target"),
            )?;
        }
        if let Some(local_port) = upstream.port {
            if local_port == 0 {
                bail!(
                    "task {task_name} connect.upstreams entry '{key}' port must be greater than 0"
                );
            }
            if !local_ports.insert(local_port) {
                bail!(
                    "task {task_name} connect.upstreams declares port {local_port} more than once"
                );
            }
            if static_ports.contains(&local_port) {
                bail!(
                    "task {task_name} connect.upstreams entry '{key}' port {local_port} collides \
                     with a static port declared in network.ports"
                );
            }
        }
        if let Some(balance) = upstream.balance.as_deref() {
            let normalized = normalize_balance(balance);
            if !is_known_balance(&normalized) {
                bail!(
                    "task {task_name} connect.upstreams entry '{key}' has unknown balance '{balance}'; valid strategies are {}",
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
    let declared: HashSet<String> = connect_config.upstreams.iter().map(|u| u.key()).collect();
    let mut names: HashSet<String> = HashSet::new();
    for template in connect_config.templates.iter_mut() {
        template.name = template.name.trim().to_string();
        let what = format!("task {task_name} connect.templates '{}'", template.name);
        if template.name.is_empty() {
            bail!("task {task_name} connect.templates entries need a name");
        }
        if !names.insert(template.name.clone()) {
            bail!("{what}: duplicate template name");
        }
        if template.destination.trim().is_empty() {
            bail!("{what}: destination is required");
        }
        if std::path::Path::new(template.destination.trim())
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            bail!("{what}: destination must stay inside the task directory (no `..`)");
        }
        if template.source.trim().is_empty() {
            bail!("{what}: source is required");
        }
        crate::worker::templates::validate_source(&template.source)
            .map_err(|err| anyhow::anyhow!("{what}: {err}"))?;
        for key in crate::worker::templates::referenced_upstreams(&template.source) {
            if !declared.contains(&key) {
                bail!(
                    "{what}: refers to upstream {key:?}, which this task does not declare in \
                     connect.upstreams"
                );
            }
        }
        if let Some(mode) = template.change_mode.as_mut() {
            *mode = mode.trim().to_ascii_lowercase();
            if !matches!(mode.as_str(), "noop" | "signal" | "restart") {
                bail!("{what}: changeMode must be noop, signal, or restart");
            }
            if mode == "signal" {
                let signal = template
                    .change_signal
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or("");
                if signal.is_empty() {
                    bail!("{what}: changeMode = \"signal\" needs changeSignal");
                }
                if !crate::structs::STOP_SIGNALS.contains(&signal) {
                    bail!(
                        "{what}: changeSignal must be one of {}",
                        crate::structs::STOP_SIGNALS.join(", ")
                    );
                }
            }
        }
        for (field, value) in [("splay", &template.splay), ("coalesce", &template.coalesce)] {
            if let Some(value) = value.as_deref() {
                crate::structs::netpolicy::parse_duration(value)
                    .map_err(|err| anyhow::anyhow!("{what}.{field}: {err}"))?;
            }
        }
    }
    for upstream in connect_config.upstreams.iter_mut() {
        if let Some(name) = upstream.name.as_mut() {
            *name = name.trim().to_string();
        }
        upstream.to = upstream.to.trim().to_string();
        for credential in upstream.credentials.iter_mut() {
            credential.header = credential.header.trim().to_ascii_lowercase();
            credential.from.trim();
        }
        if let Some(ca) = upstream.ca.as_mut() {
            ca.trim();
        }
        let is_http = upstream.is_http();
        let key = upstream.key();
        for (index, handler) in upstream.handlers.iter_mut().enumerate() {
            handler.normalize(
                &format!("task {task_name} connect.upstreams entry '{key}' handlers[{index}]"),
                is_http,
                &format!("upstream '{key}' is not HTTP. Give it an http:// or https:// target"),
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
    Ok(())
}

// Unknown-field capture (`structs::spec_fields`): the keys each block
// accepts, for did-you-mean, and the blocks nested in it. `egress`,
// `ingress`, `handlers`, and the `{ kv | identity | file }` sources are the
// network policy language shared with `legion net policy` objects, and stay
// strict on their own (`deny_unknown_fields`).

use crate::structs::spec_fields::{extension_point, spec_block, ExtensionPoint};

spec_block!(
    NetworkConfig,
    ["mode", "ports", "labels", "egress", "ingress"],
    |this, walk| {
        for port in this.ports.iter_mut() {
            walk.scoped(format!("port {}", port.name), port);
        }
    }
);
extension_point!(
    PortConfig,
    ExtensionPoint::Port,
    ["name", "static", "to", "tls"],
    |this, walk| {
        walk.opt("tls", this.tls.as_mut());
    }
);
spec_block!(PortTls, ["cert", "clients"], |this, walk| {
    walk.opt("clients", this.clients.as_mut());
});
spec_block!(ClientAuth, ["ca"]);
extension_point!(
    ServiceConfig,
    ExtensionPoint::Service,
    ["name", "port", "tags", "meta", "mesh", "allow"]
);
spec_block!(ConnectConfig, ["upstreams", "templates"], |this, walk| {
    walk.list("upstreams", &mut this.upstreams);
    walk.list("templates", &mut this.templates);
});
spec_block!(
    ConnectTemplate,
    [
        "name",
        "destination",
        "source",
        "changeMode",
        "changeSignal",
        "splay",
        "coalesce"
    ]
);
spec_block!(
    UpstreamConfig,
    [
        "to",
        "namespace",
        "name",
        "port",
        "balance",
        "http",
        "handlers",
        "credentials",
        "ca"
    ],
    |this, walk| {
        walk.opt("http", this.http.as_mut());
        walk.list("credentials", &mut this.credentials);
    }
);
spec_block!(
    UpstreamHttpConfig,
    ["timeout", "retries", "split"],
    |this, walk| {
        walk.opt("retries", this.retries.as_mut());
        walk.list("split", &mut this.split);
    }
);
spec_block!(RetryConfig, ["attempts", "on", "budget", "methods"]);
spec_block!(SplitConfig, ["tag", "weight"]);
spec_block!(UpstreamCredential, ["header", "prefix", "from"]);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn on(os: &str) -> AutoModeFacts<'_> {
        AutoModeFacts {
            node_os: Some(os),
            ..Default::default()
        }
    }

    #[test]
    fn auto_is_gated_for_containers_on_linux_only_test() {
        let resolve = |declared: &str, runtime: &str, os: &str| {
            resolve_network_mode_with(declared, runtime, &on(os), false)
        };
        assert_eq!(resolve("auto", "container", "linux"), "gated");
        assert_eq!(resolve("", "container", "linux"), "gated");
        assert_eq!(resolve("auto", "container", "macos"), "host");
        assert_eq!(resolve("auto", "exec", "linux"), "host");
        assert_eq!(resolve("host", "container", "linux"), "host");
        assert_eq!(resolve("gated", "exec", "macos"), "gated");
        // No node yet (a submit-time check) never assumes a namespace.
        assert_eq!(
            resolve_network_mode_with("auto", "container", &AutoModeFacts::default(), false),
            "host"
        );
    }

    #[test]
    fn gated_twins_are_to_or_the_advertised_port_test() {
        let network: NetworkConfig = serde_json::from_value(json!({
            "ports": [{ "name": "http", "to": 80 }, { "name": "admin", "static": 9000 }],
        }))
        .unwrap();
        let (ports, _) = assign_allocation_ports_full(
            Some(&network),
            None,
            &HashSet::new(),
            &HashMap::new(),
            None,
            true,
            true,
            false,
        )
        .unwrap();
        assert_eq!(ports[0].name, "admin");
        assert_eq!(ports[0].private_port, Some(9000));
        assert_eq!(ports[1].private_port, Some(80));
        // On the host a twin is a fresh dynamic port.
        let (ports, _) = assign_allocation_ports_full(
            Some(&network),
            None,
            &HashSet::new(),
            &HashMap::new(),
            None,
            true,
            false,
            false,
        )
        .unwrap();
        assert!(ports.iter().all(|port| port
            .private_port
            .is_some_and(|p| p != port.host_port && p != 80)));
    }

    #[test]
    fn gated_egress_warnings_name_the_task_and_the_fix_test() {
        let job: crate::structs::JobConfig = serde_json::from_value(json!({
            "name": "web",
            "tasks": [
                { "name": "default", "runtime": "container", "config": { "image": "nginx" } },
                { "name": "linux", "runtime": "container", "config": { "image": "nginx" },
                  "constraints": { "os": "linux" } },
                { "name": "macos", "runtime": "container", "config": { "image": "nginx" },
                  "constraints": { "os": "macos" } },
                { "name": "allowed", "runtime": "container", "config": { "image": "nginx" },
                  "network": { "egress": { "allow": ["api.stripe.com:443"] } } },
                { "name": "host", "runtime": "container", "config": { "image": "nginx" },
                  "network": { "mode": "host" } },
                { "name": "gated-exec", "runtime": "exec", "config": { "command": ["true"] },
                  "network": { "mode": "gated" } },
                { "name": "plain-exec", "runtime": "exec", "config": { "command": ["true"] } },
            ],
        }))
        .unwrap();
        let warnings = gated_egress_warnings(&job, |_| false);
        // The macOS-pinned container is absent: `auto` is host there.
        assert_eq!(warnings.len(), 3, "{warnings:?}");
        assert!(warnings[0].starts_with(
            "task \"default\" is gated on Linux nodes (the default for containers there) and \
             allows no egress"
        ));
        assert!(warnings[0].contains("`gated-default`"));
        assert!(warnings[0].contains("\nhint: add hosts to network.egress.allow"));
        assert!(warnings[1].starts_with(
            "task \"linux\" is gated (the default for containers on Linux) and allows no egress"
        ));
        assert!(warnings[2].starts_with("task \"gated-exec\" is gated and allows no egress"));
        // A stored policy object that covers the task silences it.
        assert!(gated_egress_warnings(&job, |_| true).is_empty());
    }

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
                tls: None,
                pending_extensions: Default::default(),
                unknown_fields: Default::default(),
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
                tls: None,
                pending_extensions: Default::default(),
                unknown_fields: Default::default(),
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
                    tls: None,
                    pending_extensions: Default::default(),
                    unknown_fields: Default::default(),
                },
                PortConfig {
                    name: "http".into(),
                    static_port: Some(80),
                    to: None,
                    tls: None,
                    pending_extensions: Default::default(),
                    unknown_fields: Default::default(),
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
                    tls: None,
                    pending_extensions: Default::default(),
                    unknown_fields: Default::default(),
                },
                PortConfig {
                    name: "http".into(),
                    static_port: None,
                    to: Some(8080),
                    tls: None,
                    pending_extensions: Default::default(),
                    unknown_fields: Default::default(),
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
                mesh: false,
                tls: None,
            }],
            upstreams: Vec::new(),
            vip_block: None,
            resolver: None,
            mode: None,
            egress_proxy: None,
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

    /// Gated plan §6.5: both cases of every proxy variable, because tools
    /// disagree (curl ignores upper-case `HTTP_PROXY` for plain HTTP, apt
    /// reads only lower case), and `NODE_USE_ENV_PROXY` for Node's fetch.
    #[test]
    fn a_gated_allocation_gets_both_cases_of_the_proxy_variables() {
        let network = AllocNetwork {
            mode: Some("gated".into()),
            ..AllocNetwork::default()
        };
        let lookup: std::collections::HashMap<_, _> = network.env_pairs().into_iter().collect();
        for key in [
            "HTTP_PROXY",
            "http_proxy",
            "HTTPS_PROXY",
            "https_proxy",
            "ALL_PROXY",
            "all_proxy",
            "LEGION_EGRESS_PROXY",
        ] {
            assert_eq!(
                lookup.get(key).map(String::as_str),
                Some("http://127.0.0.1:1080"),
                "{key}"
            );
        }
        assert_eq!(lookup["NO_PROXY"], lookup["no_proxy"]);
        assert_eq!(lookup["NO_PROXY"], "localhost,127.0.0.0/8,svc.legion");
        assert_eq!(
            lookup.get("NODE_USE_ENV_PROXY").map(String::as_str),
            Some("1")
        );
        let host = AllocNetwork {
            mode: Some("host".into()),
            ..AllocNetwork::default()
        };
        assert!(!host
            .env_pairs()
            .iter()
            .any(|(key, _)| key.eq_ignore_ascii_case("http_proxy")));
    }

    fn task_of(runtime: &str, network: Option<Value>, config: Value) -> crate::structs::TaskConfig {
        let mut task = json!({ "name": "workspace", "runtime": runtime, "config": config });
        if let Some(network) = network {
            task["network"] = network;
        }
        serde_json::from_value(task).expect("task parses")
    }

    /// Gated plan §6.5's resolution table, with the Phase 2 gate open and
    /// closed: `auto` is `gated` only for a sandbox with no
    /// `config.network` on a microVM, an explicit mode is never changed,
    /// and every other runtime stays `host`.
    #[test]
    fn auto_resolves_per_the_gated_default_table() {
        let plain = json!({ "image": "alpine", "command": ["sh"] });
        let tap = json!({ "image": "alpine", "command": ["sh"], "network": { "mode": "tap" } });
        let dev = json!({ "image": "alpine", "command": ["sh"], "hypervisor": "dev" });
        let fc = json!({ "image": "alpine", "command": ["sh"], "hypervisor": "firecracker" });
        let resolve = |runtime: &str, declared: &str, config: &Value, node: Option<&str>, open| {
            resolve_network_mode_with(
                declared,
                runtime,
                &AutoModeFacts {
                    node_vmm: node,
                    ..AutoModeFacts::from_config(config)
                },
                open,
            )
        };
        let cases: &[(&str, &str, &Value, Option<&str>, &str)] = &[
            ("sandbox", "auto", &plain, Some("firecracker"), "gated"),
            ("sandbox", "", &plain, Some("cloud-hypervisor"), "gated"),
            ("sandbox", "auto", &plain, Some("apple-container"), "host"),
            ("sandbox", "auto", &plain, Some("oci-runtime"), "host"),
            ("sandbox", "auto", &dev, Some("firecracker"), "host"),
            ("sandbox", "auto", &fc, Some("apple-container"), "gated"),
            ("sandbox", "auto", &tap, Some("firecracker"), "host"),
            ("sandbox", "auto", &plain, None, "gated"),
            ("sandbox", "gated", &dev, Some("apple-container"), "gated"),
            ("sandbox", "host", &plain, Some("firecracker"), "host"),
            ("container", "auto", &plain, Some("firecracker"), "host"),
            ("exec", "auto", &plain, Some("firecracker"), "host"),
            ("container", "gated", &plain, None, "gated"),
        ];
        for (runtime, declared, config, node, want) in cases {
            assert_eq!(
                resolve(runtime, declared, config, *node, true),
                *want,
                "{runtime} {declared:?} {config} on {node:?}"
            );
        }
        // Closed (today): `auto` is `host` everywhere, explicit is itself.
        assert_eq!(
            resolve("sandbox", "auto", &plain, Some("firecracker"), false),
            "host"
        );
        assert_eq!(
            resolve("sandbox", "gated", &plain, Some("firecracker"), false),
            "gated"
        );
        assert!(!task_runs_gated(
            &task_of("sandbox", None, plain.clone()),
            false
        ));
        assert!(task_runs_gated(
            &task_of("sandbox", None, plain.clone()),
            true
        ));
        assert!(!task_runs_gated(
            &task_of("sandbox", None, tap.clone()),
            true
        ));
        assert!(task_runs_gated(
            &task_of("exec", Some(json!({ "mode": "gated" })), plain.clone()),
            false
        ));
    }

    /// Gated plan §6.8: gated plus `config.network` is refused at submit
    /// with the fix. Since Phase 5 the datapath serves a gated sandbox's
    /// ports, so only ports nothing can reach are refused: a loopback-only
    /// guest's.
    #[test]
    fn contradictory_gated_sandboxes_are_refused_at_submit() {
        let gated = Some(json!({ "mode": "gated" }));
        let tap = json!({ "image": "a", "command": ["sh"], "network": { "mode": "tap" } });
        let err =
            validate_gated_sandbox(&task_of("sandbox", gated.clone(), tap), false).unwrap_err();
        assert_eq!(
            err,
            "task \"workspace\" sets network.mode = \"gated\" and config.network = { mode = \"tap\" \
             }. A gated guest has no NIC\nHint: drop config.network to stay gated, or drop \
             network.mode to use the guest's own network"
        );
        let none = json!({ "image": "a", "command": ["sh"], "network": { "mode": "none" } });
        assert!(validate_gated_sandbox(&task_of("sandbox", gated.clone(), none), false).is_err());

        let plain = json!({ "image": "a", "command": ["sh"] });
        // Gated plan Phase 5: ports on a gated sandbox are served.
        let ports = Some(json!({ "mode": "gated", "ports": [{ "name": "http" }] }));
        assert!(validate_gated_sandbox(&task_of("sandbox", ports, plain.clone()), false).is_ok());
        // A default sandbox with ports: loopback only while `auto` is
        // `host`, so refused with both ways to serve them; served once the
        // default is gated.
        let auto_ports = Some(json!({ "ports": [{ "name": "http" }] }));
        let default = task_of("sandbox", auto_ports.clone(), plain.clone());
        let err = validate_gated_sandbox(&default, false).unwrap_err();
        assert_eq!(
            err,
            "task \"workspace\" declares network.ports, but nothing can reach a port in its \
             guest: with no config.network and network.mode not \"gated\", the guest has \
             loopback only\nHint: set network.mode = \"gated\" to serve the ports through the \
             node datapath, or wire the guest to a bridge (config.network = { mode = \"tap\", \
             bridge = \"br0\" })"
        );
        assert!(validate_gated_sandbox(&default, true).is_ok());
        // A TAP guest's ports are served on its bridge address, gated
        // default or not.
        let tap = json!({ "image": "a", "command": ["sh"], "network": { "mode": "tap" } });
        let on_tap = task_of("sandbox", auto_ports.clone(), tap);
        assert!(validate_gated_sandbox(&on_tap, false).is_ok());
        assert!(validate_gated_sandbox(&on_tap, true).is_ok());
        // An air-gapped guest has no door at all.
        let air_gap = json!({ "image": "a", "command": ["sh"], "network": { "mode": "none" } });
        let err =
            validate_gated_sandbox(&task_of("sandbox", auto_ports, air_gap), true).unwrap_err();
        assert!(
            err.contains("config.network = { mode = \"none\" } gives the guest no network device"),
            "{err}"
        );
        // Other runtimes and plain gated sandboxes pass.
        assert!(
            validate_gated_sandbox(&task_of("sandbox", gated.clone(), plain.clone()), true).is_ok()
        );
        assert!(validate_gated_sandbox(
            &task_of(
                "container",
                Some(json!({ "mode": "gated", "ports": [{ "name": "http" }] })),
                plain
            ),
            true
        )
        .is_ok());
    }

    /// A gated guest's agent holds 127.0.0.1:1080 (its egress proxy) and
    /// relays each upstream to the host vsock port of its `localPort`, where
    /// 1080 and 53 are the proxy and the resolver: a task that would collide
    /// is refused at submit. The same task off gated networking passes.
    #[test]
    fn gated_sandboxes_refuse_the_guest_agents_ports() {
        let plain = json!({ "image": "a", "command": ["sh"] });
        let with_connect = |network: Value, upstream: Value| {
            let mut task = task_of("sandbox", Some(network), plain.clone());
            task.connect =
                Some(serde_json::from_value(json!({ "upstreams": [upstream] })).unwrap());
            task
        };
        let gated = json!({ "mode": "gated" });
        for port in [1080, 53] {
            let task = with_connect(gated.clone(), json!({ "to": "db", "port": port }));
            let err = validate_gated_sandbox(&task, false).unwrap_err();
            assert_eq!(
                err,
                format!(
                    "task \"workspace\" connect.upstreams entry \"db\" sets port {port}, but a \
                     gated sandbox's upstreams are relayed to the node on vsock ports of the same \
                     number, and 1080 and 53 are the guest's egress proxy and resolver\nHint: pick \
                     another port, or drop it to take a dynamic one"
                )
            );
        }
        let fine = with_connect(gated.clone(), json!({ "to": "db", "port": 5432 }));
        assert!(validate_gated_sandbox(&fine, false).is_ok());
        // Off gated networking the agent relays nothing.
        let tap = json!({ "image": "a", "command": ["sh"], "network": { "mode": "tap" } });
        let mut on_tap = task_of("sandbox", None, tap);
        on_tap.connect = fine.connect.clone();
        on_tap.connect.as_mut().unwrap().upstreams[0].port = Some(1080);
        assert!(validate_gated_sandbox(&on_tap, false).is_ok());

        for port in [
            json!({ "name": "socks", "static": 1080 }),
            json!({ "name": "socks", "to": 1080 }),
        ] {
            let task = task_of(
                "sandbox",
                Some(json!({ "mode": "gated", "ports": [port] })),
                plain.clone(),
            );
            let err = validate_gated_sandbox(&task, false).unwrap_err();
            assert_eq!(
                err,
                "task \"workspace\" binds network.ports \"socks\" on port 1080 in its guest, but in \
                 a gated sandbox guest 127.0.0.1:1080 is the guest agent's egress proxy\nHint: \
                 pick another port, or drop static and to to take a dynamic one"
            );
        }
        let task = task_of(
            "sandbox",
            Some(json!({ "mode": "gated", "ports": [{ "name": "api", "static": 1100 }] })),
            plain.clone(),
        );
        assert_eq!(
            validate_gated_sandbox(&task, false).unwrap_err(),
            "task \"workspace\" binds network.ports \"api\" on port 1100 in its guest, but in a \
             gated sandbox guest the node serves each port on the guest vsock port of the same \
             number, and vsock port 1100 is the guest agent's exec listener\nHint: pick another \
             port, or drop static and to to take a dynamic one"
        );
        assert_eq!(u32::from(1100u16), legion_guest::exec::EXEC_PORT);
        // A default sandbox runs gated once the default is, and is checked.
        let default = task_of(
            "sandbox",
            Some(json!({ "ports": [{ "name": "socks", "static": 1080 }] })),
            plain,
        );
        assert!(validate_gated_sandbox(&default, true).is_err());
    }

    /// Gated plan §6.6: a default sandbox on a dev backend says it runs
    /// without egress policy, once the default is gated; an explicit choice
    /// never does.
    #[test]
    fn a_default_sandbox_on_a_dev_backend_says_egress_is_unenforced() {
        let plain = json!({ "image": "a", "command": ["sh"] });
        let task = task_of("sandbox", None, plain.clone());
        let detail = egress_unenforced_detail(&task, Some("apple-container"), true).unwrap();
        assert_eq!(
            detail,
            "egress-unenforced: dev mode runs sandbox workspace without Legion's egress policy \
             (Apple container has no vsock to the datapath)"
        );
        assert!(egress_unenforced_detail(&task, Some("apple-container"), false).is_none());
        assert!(egress_unenforced_detail(&task, Some("firecracker"), true).is_none());
        let host = task_of("sandbox", Some(json!({ "mode": "host" })), plain.clone());
        assert!(egress_unenforced_detail(&host, Some("apple-container"), true).is_none());
        let tap = json!({ "image": "a", "command": ["sh"], "network": { "mode": "none" } });
        assert!(egress_unenforced_detail(
            &task_of("sandbox", None, tap),
            Some("oci-runtime"),
            true
        )
        .is_none());
    }

    //
    // CONNECT / UPSTREAMS
    //

    fn connect(upstreams: Value) -> ConnectConfig {
        serde_json::from_value(json!({ "upstreams": upstreams })).expect("connect config")
    }

    #[test]
    fn upstream_config_parses_to_and_port_test() {
        let config = connect(json!([
            { "to": "db" },
            { "to": "cache", "port": 6379 },
            { "to": "web", "port": 8080, "balance": "least_conn" },
        ]));
        assert_eq!(config.upstreams[0].port, None);
        assert_eq!(config.upstreams[0].balance_name(), DEFAULT_BALANCE);
        assert_eq!(config.upstreams[1].port, Some(6379));
        assert_eq!(config.upstreams[2].port, Some(8080));
        assert_eq!(config.upstreams[2].balance_name(), "least_conn");
    }

    #[test]
    fn balance_name_normalizes_case_and_dashes() {
        let config = connect(json!([{ "to": "db", "balance": " Least-Conn " }]));
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
                    tls: None,
                    pending_extensions: Default::default(),
                    unknown_fields: Default::default(),
                },
                PortConfig {
                    name: "admin".into(),
                    static_port: None,
                    to: None,
                    tls: None,
                    pending_extensions: Default::default(),
                    unknown_fields: Default::default(),
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
            false,
            false,
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
                    tls: None,
                    pending_extensions: Default::default(),
                    unknown_fields: Default::default(),
                },
                PortConfig {
                    name: "metrics".into(),
                    static_port: Some(9090),
                    to: None,
                    tls: None,
                    pending_extensions: Default::default(),
                    unknown_fields: Default::default(),
                },
            ],
            ..NetworkConfig::default()
        };
        let connect_config = connect(json!([{ "to": "db" }, { "to": "cache" }]));

        let used: HashSet<u16> = [DYNAMIC_PORT_MIN].into();
        let (ports, upstreams) =
            assign_allocation_ports(Some(&network), Some(&connect_config), &used)?;

        assert_eq!(ports.len(), 2);
        assert_eq!(upstreams.len(), 2);
        // Statics first, then dynamics from one shared cursor.
        assert_eq!(ports[0].host_port, 9090);
        assert_eq!(ports[1].host_port, DYNAMIC_PORT_MIN + 1);
        assert_eq!(upstreams[0].key, "db");
        assert_eq!(upstreams[0].local_port, Some(DYNAMIC_PORT_MIN + 2));
        assert_eq!(upstreams[0].balance, DEFAULT_BALANCE);
        assert_eq!(upstreams[1].local_port, Some(DYNAMIC_PORT_MIN + 3));

        let all: HashSet<u16> = ports
            .iter()
            .map(|port| port.host_port)
            .chain(upstreams.iter().filter_map(|upstream| upstream.local_port))
            .collect();
        assert_eq!(all.len(), 4, "assigned ports must all be distinct");
        Ok(())
    }

    #[test]
    fn upstream_static_local_port_is_honored_and_conflicts_name_the_port() -> R<()> {
        let connect_config = connect(json!([
            { "to": "cache", "port": 6379 },
            { "to": "db" },
        ]));

        let upstreams = assign_upstream_ports(&connect_config, &HashSet::new())?;
        assert_eq!(upstreams[0].key, "cache");
        assert_eq!(upstreams[0].local_port, Some(6379));
        assert_eq!(upstreams[1].local_port, Some(DYNAMIC_PORT_MIN));

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
                tls: None,
                pending_extensions: Default::default(),
                unknown_fields: Default::default(),
            }],
            ..NetworkConfig::default()
        };
        let connect_config = connect(json!([{ "to": "cache", "port": 6379 }]));
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
                key: "user-db".into(),
                local_port: Some(20003),
                balance: DEFAULT_BALANCE.into(),
                to: None,
                vip: None,
                canonical_port: None,
            }],
            vip_block: None,
            resolver: None,
            mode: None,
            egress_proxy: None,
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
                tls: None,
                pending_extensions: Default::default(),
                unknown_fields: Default::default(),
            }],
            ..NetworkConfig::default()
        };
        let connect_config = connect(json!([
            { "to": "db" },
            { "to": "cache", "port": 6379 },
            { "to": "web", "balance": "least_conn" },
        ]));
        let svc: ServiceConfig =
            serde_json::from_value(json!({ "port": "http", "allow": ["gateway"] }))?;
        validate_task_connect("api", Some(&network), Some(&connect_config), Some(&svc))?;
        assert!(svc.allows_job("web", "gateway"));
        assert!(!svc.allows_job("web", "other"));
        Ok(())
    }

    #[test]
    fn validate_connect_rejects_duplicate_upstream_service() {
        let connect_config = connect(json!([{ "to": "db" }, { "to": "db" }]));
        let err = validate_task_connect("api", None, Some(&connect_config), None).unwrap_err();
        assert!(
            err.to_string().contains("declares 'db' more than once"),
            "{err}"
        );
        assert!(err.to_string().contains("task api"), "{err}");
    }

    #[test]
    fn crossing_upstreams_key_by_service_and_namespace() {
        let config = connect(json!([
            { "to": "redis" },
            { "to": "auth", "namespace": "shared" },
            { "to": "http://billing", "namespace": "shared" },
            { "to": "ledger", "namespace": "shared", "name": "books" },
        ]));
        let keys: Vec<String> = config.upstreams.iter().map(|u| u.key()).collect();
        assert_eq!(keys, ["redis", "auth.shared", "billing.shared", "books"]);
        let refs: Vec<String> = config
            .upstreams
            .iter()
            .map(|u| u.service_ref("checkout/api").unwrap())
            .collect();
        assert_eq!(
            refs,
            [
                "checkout/redis",
                "shared/auth",
                "shared/billing",
                "shared/ledger"
            ]
        );
        // A bare service from `default` stays in `default`; a crossing into
        // `default` is the bare id.
        assert_eq!(config.upstreams[0].service_ref("api").unwrap(), "redis");
        let into_default = connect(json!([{ "to": "auth", "namespace": "default" }]));
        assert_eq!(
            into_default.upstreams[0]
                .service_ref("checkout/api")
                .unwrap(),
            "auth"
        );
        assert!(validate_task_connect("api", None, Some(&config), None).is_ok());
    }

    #[test]
    fn validate_connect_rejects_bad_or_conflicting_upstream_namespaces() {
        for (upstreams, expect) in [
            (
                json!([{ "to": "auth", "namespace": "Shared" }]),
                "namespace",
            ),
            (
                json!([{ "name": "x", "to": "https://example.com", "namespace": "shared" }]),
                "external destination",
            ),
        ] {
            let err = validate_task_connect("api", None, Some(&connect(upstreams)), None)
                .unwrap_err()
                .to_string();
            assert!(err.contains(expect), "{expect}: {err}");
        }
        // Two upstreams for the same service in two namespaces are distinct.
        let both = connect(json!([
            { "to": "auth" },
            { "to": "auth", "namespace": "shared" },
        ]));
        assert!(validate_task_connect("api", None, Some(&both), None).is_ok());
    }

    #[test]
    fn to_names_a_catalog_service_or_an_external_url_test() {
        let catalog = |to: &str, http: bool| {
            assert_eq!(
                parse_upstream_to(to).unwrap(),
                UpstreamDestination::Catalog {
                    service: "db".into(),
                    http
                },
                "{to}"
            );
        };
        catalog("db", false);
        catalog(" tcp://db ", false);
        catalog("db.svc.legion", false);
        catalog("http://db", true);
        catalog("http://db.svc.legion/", true);
        let external = |to: &str| match parse_upstream_to(to).unwrap() {
            UpstreamDestination::External(target) => target,
            other => panic!("{to}: {other:?}"),
        };
        assert_eq!(external("https://api.openai.com").port, 443);
        assert!(external("https://api.openai.com").http);
        assert_eq!(external("tcp://10.0.0.5:5432").host, "10.0.0.5");
        // A dotless external host carries its port.
        assert_eq!(external("http://wiki:80").host, "wiki");
        for (to, why) in [
            ("https://db", "reached over the mesh"),
            ("tls://db", "reached over the mesh"),
            ("api.example.com", "needs a scheme"),
            ("db:5432", "needs a scheme"),
            ("bad name", "not a service name"),
            ("", "to is required"),
        ] {
            let err = parse_upstream_to(to).unwrap_err().to_string();
            assert!(err.contains(why), "{to}: {err}");
        }
    }

    #[test]
    fn a_tcp_upstream_under_an_http_chain_warns_at_submit_test() {
        let job: crate::structs::JobConfig = serde_json::from_value(json!({
            "name": "api",
            "task": {
                "runtime": "exec",
                "connect": { "upstreams": [{ "to": "db" }, { "to": "http://search" }] },
                "network": { "egress": {
                    "protocol": "http", "policy": "deny",
                    "rules": [{ "when": { "path": "/v1/**" }, "action": "allow" }]
                } }
            }
        }))
        .unwrap();
        let warnings = tcp_upstream_http_chain_warnings(&job, |_| Vec::new());
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("upstream 'db' is tcp://"),
            "{}",
            warnings[0]
        );
        assert!(warnings[0].contains("task#0"), "{}", warnings[0]);
        // A host rule ahead of the path rule decides it at connect.
        let mut job = job;
        job.tasks[0]
            .network
            .as_mut()
            .unwrap()
            .egress
            .as_mut()
            .unwrap()
            .rules
            .insert(
                0,
                serde_json::from_value(json!({ "when": { "host": "db" }, "action": "allow" }))
                    .unwrap(),
            );
        assert!(tcp_upstream_http_chain_warnings(&job, |_| Vec::new()).is_empty());
    }

    #[test]
    fn validate_connect_rejects_empty_upstream_service() {
        let connect_config = connect(json!([{ "to": "  " }]));
        let err = validate_task_connect("api", None, Some(&connect_config), None).unwrap_err();
        assert!(err.to_string().contains("to is required"), "{err}");
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
        // A catalog service is not reached over TLS by the task: that is the
        // mesh's job.
        let bad = connect(json!([{ "to": "https://search" }]));
        let err = validate_task_connect("api", None, Some(&bad), None).unwrap_err();
        assert!(err.to_string().contains("reached over the mesh"), "{err}");
        // A dotted host with no scheme is neither a service nor a URL.
        let bad = connect(json!([{ "name": "x", "to": "api.example.com" }]));
        let err = validate_task_connect("api", None, Some(&bad), None).unwrap_err();
        assert!(err.to_string().contains("needs a scheme"), "{err}");

        // An external upstream keys by name and carries its destination.
        let ok =
            connect(json!([{ "name": "openai", "to": "https://api.openai.com", "port": 8443 }]));
        validate_task_connect("api", None, Some(&ok), None).unwrap();
        let (_, upstreams) = assign_allocation_ports(None, Some(&ok), &HashSet::new()).unwrap();
        assert_eq!(upstreams[0].key, "openai");
        assert_eq!(upstreams[0].local_port, Some(8443));
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
        assert!(serde_json::from_value::<ConnectConfig>(
            json!({ "upstreams": [{ "to": "db", "handlers": [{ "wasm": {} }] }] })
        )
        .is_err());
        let bad = connect(
            json!([{ "name": "o", "to": "tcp://x:1", "handlers": [{ "dlp": { "patterns": ["x"] } }] }]),
        );
        let err = validate_task_connect("api", None, Some(&bad), None).unwrap_err();
        assert!(err.to_string().contains("stage \"body\""), "{err}");
        assert!(
            err.to_string().contains("upstream 'o' is not HTTP"),
            "{err}"
        );
        let ok = connect(
            json!([{ "name": "o", "to": "https://x.example", "handlers": [{ "dlp": { "patterns": ["x"] } }] }]),
        );
        validate_task_connect("api", None, Some(&ok), None).unwrap();

        // http traffic-management block validation.
        let bad = connect(
            json!([{ "to": "http://api", "http": { "split": [{ "tag": "a", "weight": 60 }, { "tag": "b", "weight": 60 }] } }]),
        );
        let err = validate_task_connect("api", None, Some(&bad), None).unwrap_err();
        assert!(err.to_string().contains("more than 100%"), "{err}");
        let ok = connect(
            json!([{ "to": "http://api", "http": { "timeout": "3s", "retries": { "attempts": 2, "budget": "20%" } } }]),
        );
        validate_task_connect("api", None, Some(&ok), None).unwrap();
        // An http block needs an upstream that speaks HTTP; the scheme says so.
        let bad = connect(json!([{ "to": "api", "http": { "timeout": "3s" } }]));
        let err = validate_task_connect("api", None, Some(&bad), None).unwrap_err();
        assert!(
            err.to_string().contains("write to = \"http://api\""),
            "{err}"
        );
        // Outlier ejection has no knob: the key is refused when the jobspec
        // is parsed.
        let err = serde_json::from_value::<crate::structs::JobConfig>(json!({
            "name": "web",
            "task": {
                "name": "web",
                "runtime": "exec",
                "connect": {
                    "upstreams": [{ "to": "http://api", "http": { "outlier": { "consecutive5xx": 5 } } }]
                }
            }
        }))
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("unknown field `connect.upstreams[0].http.outlier`"),
            "{err}"
        );
    }

    #[test]
    fn upstream_credentials_validate_test() {
        let check = |upstreams: Value| -> Result<(), String> {
            let mut config = Some(connect(upstreams));
            normalize_task_connect("api", &mut config).map_err(|e| e.to_string())?;
            validate_task_connect("api", None, config.as_ref(), None).map_err(|e| e.to_string())
        };
        let cred = |header: &str| json!({ "header": header, "from": { "kv": "secrets/openai" } });

        // An external https:// upstream takes credentials, normalized.
        check(json!([{
            "name": "openai", "to": "https://api.openai.com",
            "credentials": [
                { "header": "Authorization", "prefix": "Bearer ", "from": { "kv": "secrets/openai" } },
                cred("x-org"),
            ],
        }]))
        .unwrap();
        let mut normalized = Some(connect(
            json!([{ "name": "a", "to": "https://x.example", "credentials": [{ "header": " X-Api-Key ", "from": { "kv": " secrets/a " } }] }]),
        ));
        normalize_task_connect("api", &mut normalized).unwrap();
        let credential = &normalized.unwrap().upstreams[0].credentials[0];
        assert_eq!(credential.header, "x-api-key");
        assert_eq!(credential.kv_path(), Some("secrets/a"));

        // Plaintext, non-HTTP, and catalog-service upstreams are refused.
        let err = check(json!([{ "name": "o", "to": "http://api.openai.com", "credentials": [cred("x-api-key")] }])).unwrap_err();
        assert!(
            err.contains("in the clear") && err.contains("Hint"),
            "{err}"
        );
        for to in ["tcp://db:5432", "tls://db:5432"] {
            let err = check(json!([{ "name": "o", "to": to, "credentials": [cred("x-api-key")] }]))
                .unwrap_err();
            assert!(err.contains("no request head"), "{err}");
        }
        let err = check(json!([{ "to": "db", "credentials": [cred("x-api-key")] }])).unwrap_err();
        assert!(err.contains("external https://"), "{err}");

        // Header names: valid, not hop-by-hop or host, at most once.
        for header in [
            "connection",
            "Host",
            "proxy-authorization",
            "transfer-encoding",
        ] {
            let err = check(
                json!([{ "name": "o", "to": "https://x.example", "credentials": [cred(header)] }]),
            )
            .unwrap_err();
            assert!(err.contains("cannot be injected"), "{header}: {err}");
        }
        let err =
            check(json!([{ "name": "o", "to": "https://x.example", "credentials": [cred("bad header")] }]))
                .unwrap_err();
        assert!(err.contains("not a valid HTTP header name"), "{err}");
        let err =
            check(json!([{ "name": "o", "to": "https://x.example", "credentials": [cred("")] }]))
                .unwrap_err();
        assert!(err.contains("header is required"), "{err}");
        let err = check(json!([{ "name": "o", "to": "https://x.example", "credentials": [cred("X-Api-Key"), cred("x-api-key")] }])).unwrap_err();
        assert!(err.contains("more than once"), "{err}");

        // A provider is required, and its path non-empty.
        let err = check(json!([{ "name": "o", "to": "https://x.example", "credentials": [{ "header": "x", "from": { "kv": "  " } }] }])).unwrap_err();
        assert!(err.contains("kv is empty"), "{err}");
        // `from` is one source: no key, or two, is a parse error.
        assert!(serde_json::from_value::<ConnectConfig>(json!({ "upstreams": [{ "name": "o", "to": "https://x.example", "credentials": [{ "header": "x", "from": {} }] }] })).is_err());
        assert!(serde_json::from_value::<ConnectConfig>(json!({ "upstreams": [{ "name": "o", "to": "https://x.example", "credentials": [{ "header": "x", "from": { "kv": "a", "identity": "b" } }] }] })).is_err());
        // Credentials never come from a node's disk.
        let err = check(json!([{ "name": "o", "to": "https://x.example", "credentials": [{ "header": "x", "from": { "file": "/etc/key" } }] }])).unwrap_err();
        assert!(err.contains("file source is not allowed"), "{err}");
        // Unknown providers are a parse error, not silently ignored.
        assert!(serde_json::from_value::<ConnectConfig>(json!({ "upstreams": [{ "name": "o", "to": "https://x.example", "credentials": [{ "header": "x", "from": { "vault": "a" } }] }] })).is_err());
        let err = check(json!([{ "name": "o", "to": "https://x.example", "credentials": [{ "header": "x", "prefix": "a\r\nb", "from": { "kv": "k" } }] }])).unwrap_err();
        assert!(err.contains("CR, LF"), "{err}");

        // The identity provider: a valid name, one provider at a time, and
        // never the control-plane identity.
        check(json!([{ "name": "o", "to": "https://x.example", "credentials": [{ "header": "authorization", "prefix": "Bearer ", "from": { "identity": "api" } }] }]))
            .unwrap();
        let mut normalized = Some(connect(
            json!([{ "name": "a", "to": "https://x.example", "credentials": [{ "header": "authorization", "from": { "identity": " api " } }] }]),
        ));
        normalize_task_connect("api", &mut normalized).unwrap();
        let credential = &normalized.unwrap().upstreams[0].credentials[0];
        assert_eq!(credential.identity_name(), Some("api"));
        assert_eq!(credential.source(), "identity api");
        let err = check(json!([{ "name": "o", "to": "https://x.example", "credentials": [{ "header": "x", "from": { "identity": "../x" } }] }])).unwrap_err();
        assert!(err.contains("from.identity"), "{err}");
        let err = check(json!([{ "name": "o", "to": "https://x.example", "credentials": [{ "header": "x", "from": { "identity": "legion" } }] }])).unwrap_err();
        assert!(err.contains("control-plane identity"), "{err}");
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
            { "to": "db" },
            { "to": "cache", "port": 6379 },
            { "name": "openai", "to": "https://api.openai.com" },
            { "to": "unknown" }
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
        let by_key = |key: &str| upstreams.iter().find(|u| u.key == key).unwrap().clone();
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
            "a pinned port wins"
        );
        assert_eq!(
            by_key("openai").canonical_port,
            Some(443),
            "an external target's port"
        );
        let unknown = by_key("unknown");
        assert!(
            unknown.canonical_port >= Some(DYNAMIC_PORT_MIN),
            "falls back to a port from the dynamic range"
        );
        // With a VIP block only a pinned port is bound on loopback.
        assert_eq!(by_key("db").local_port, None);
        assert_eq!(by_key("cache").local_port, Some(6379));

        let network = AllocNetwork {
            upstreams: upstreams.clone(),
            vip_block: Some("127.42.7".into()),
            resolver: None,
            ..Default::default()
        };
        let env = network.env_pairs();
        assert!(env
            .iter()
            .any(|(k, v)| k == "LEGION_DNS" && v == "127.42.7.1"));
        // One address per upstream: the VIP on the canonical port, and its
        // parts.
        assert!(env
            .iter()
            .any(|(k, v)| k == "LEGION_UPSTREAM_DB" && v == "127.42.7.2:5432"));
        assert!(env
            .iter()
            .any(|(k, v)| k == "LEGION_UPSTREAM_HOST_DB" && v == "127.42.7.2"));
        assert!(env
            .iter()
            .any(|(k, v)| k == "LEGION_UPSTREAM_PORT_DB" && v == "5432"));
        assert!(!env
            .iter()
            .any(|(k, _)| k.starts_with("LEGION_UPSTREAM_VIP_")));
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
        assert!(!env.iter().any(|(k, _)| k == "LEGION_DNS"));
        assert!(env
            .iter()
            .any(|(k, v)| k == "LEGION_UPSTREAM_HOST_DB" && v == "127.0.0.1"));
    }

    #[test]
    fn validate_connect_rejects_duplicate_and_zero_local_ports() {
        let duplicate = connect(json!([
            { "to": "db", "port": 6379 },
            { "to": "cache", "port": 6379 },
        ]));
        let err = validate_task_connect("api", None, Some(&duplicate), None).unwrap_err();
        assert!(
            err.to_string().contains("port 6379 more than once"),
            "{err}"
        );

        let zero = connect(json!([{ "to": "db", "port": 0 }]));
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
                tls: None,
                pending_extensions: Default::default(),
                unknown_fields: Default::default(),
            }],
            ..NetworkConfig::default()
        };
        let connect_config = connect(json!([{ "to": "db", "port": 8080 }]));
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
        let connect_config = connect(json!([{ "to": "db", "balance": "magic" }]));
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
            let connect_config = connect(json!([{ "to": "db", "balance": name }]));
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
    fn allow_wildcard_and_absent_allow_permit_everyone_in_the_namespace() {
        let open: ServiceConfig =
            serde_json::from_value(json!({ "port": "http" })).expect("service config");
        assert!(open.allows_job("web", "anyone"));
        let wildcard: ServiceConfig =
            serde_json::from_value(json!({ "port": "http", "allow": ["*"] }))
                .expect("service config");
        assert!(wildcard.allows_job("web", "anyone"));
        assert!(allow_permits(&[], "web", "anyone"));
        // ...but never across a namespace.
        assert!(!allow_permits(&[], "prod/web", "dev/api"));
        assert!(!allow_permits(&["*".into()], "prod/web", "dev/api"));
        assert!(allow_permits(&[], "prod/web", "prod/api"));
    }

    #[test]
    fn crossing_a_namespace_needs_a_qualified_intention() {
        let allow = vec!["gateway".to_string(), "payments-*/api".to_string()];
        assert!(allow_permits(&allow, "shared/auth", "shared/gateway"));
        assert!(!allow_permits(
            &allow,
            "shared/auth",
            "payments-prod/gateway"
        ));
        assert!(allow_permits(&allow, "shared/auth", "payments-prod/api"));
        assert!(!allow_permits(&allow, "shared/auth", "checkout-prod/api"));
        // `*/*` admits everyone everywhere.
        assert!(allow_permits(&["*/*".into()], "shared/auth", "dev/x"));
        // A bare entry still means the owner's namespace, and globs.
        assert!(allow_permits(&["web-*".into()], "web", "web-2"));
        assert!(!allow_permits(&["web".into()], "prod/x", "web"));
    }

    /// `service.mesh` and a port's own `tls` are exclusive, sources are
    /// checked per field, and an upstream `ca` needs an external TLS
    /// destination.
    #[test]
    fn mesh_port_tls_and_upstream_ca_validate_test() {
        let network: NetworkConfig = serde_json::from_value(json!({
            "ports": [
                { "name": "http" },
                { "name": "admin", "tls": {
                    "cert": { "file": { "cert": "/etc/a.crt", "key": "/etc/a.key" } },
                    "clients": { "ca": { "kv": "certs/admin-ca" } },
                } },
            ],
        }))
        .unwrap();
        let mesh: ServiceConfig =
            serde_json::from_value(json!({ "port": "http", "mesh": true })).unwrap();
        validate_task_network("api", Some(&network), Some(&mesh)).unwrap();
        let on_tls_port: ServiceConfig =
            serde_json::from_value(json!({ "port": "admin", "mesh": true })).unwrap();
        let err = validate_task_network("api", Some(&network), Some(&on_tls_port))
            .unwrap_err()
            .to_string();
        assert!(err.contains("never both"), "{err}");

        let bad: NetworkConfig = serde_json::from_value(json!({
            "ports": [{ "name": "https", "tls": { "cert": { "identity": "api" } } }],
        }))
        .unwrap();
        let err = validate_task_network("api", Some(&bad), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("identity source is not allowed"), "{err}");

        let check = |upstreams: Value| {
            validate_task_connect("api", None, Some(&connect(upstreams)), None)
                .map_err(|e| e.to_string())
        };
        check(json!([{ "name": "corp", "to": "https://api.corp.example", "ca": { "kv": "certs/corp-ca" } }]))
            .unwrap();
        let err = check(json!([{ "to": "db", "ca": { "kv": "certs/ca" } }])).unwrap_err();
        assert!(err.contains("not one"), "{err}");
        let err = check(
            json!([{ "name": "x", "to": "http://x.example", "ca": { "file": "/etc/ca.pem" } }]),
        )
        .unwrap_err();
        assert!(err.contains("not one"), "{err}");
    }
}
