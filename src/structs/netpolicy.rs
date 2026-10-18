//! Network policy: the declarative half of the mesh datapath.
//!
//! Three things live here, deliberately kept apart from the code that moves
//! bytes (`crate::worker::netd`):
//!
//! - **Policy objects** ([`NetPolicyObject`]) — stored, selector-scoped rule
//!   documents an operator applies to a pool, a namespace, a runtime, a job
//!   glob, or a label set. A task's inline `network.egress` / `network.ingress`
//!   block is the same shape scoped to that task.
//! - **The merge** ([`merge_policies`]) — deterministic: every matching
//!   `require` policy in priority order, then exactly one `default` (or the
//!   task's own block), and every rule in the result names the policy it came
//!   from. `netd`, `legion net policy show`, and `legion job plan` all call
//!   this one function, so what a user is shown is what the datapath runs.
//! - **Evaluation** ([`evaluate_connect`], [`evaluate_request`]) — a rule is
//!   decided at the earliest point that can decide it. Host/port/CIDR/identity
//!   rules resolve at connect (or DNS) time and never touch the byte path;
//!   method/path/header rules resolve at the HTTP request head. A chain whose
//!   rules are all connect-time never leaves L4.
//!
//! The wire carries names (`protocol`, `handler`, `enforcement`), never enums,
//! so adding a value is one row in a table here and no proto change. Every
//! table is the single list config validation reads, so a name and an
//! implementation cannot drift.

use std::collections::{BTreeMap, HashSet};
use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::prelude::{bail, R};

/// `enforcement = "require"`: always applied, ahead of the task's own chain;
/// a job may append rules and handlers but never remove or widen.
pub const ENFORCEMENT_REQUIRE: &str = "require";
/// `enforcement = "default"`: applied only when the task declares no chain of
/// its own for that direction; the task's block replaces it wholesale.
pub const ENFORCEMENT_DEFAULT: &str = "default";
pub const ENFORCEMENTS: &[&str] = &[ENFORCEMENT_REQUIRE, ENFORCEMENT_DEFAULT];

/// Chain protocols. `tcp` decides everything at connect time; `http` parses
/// HTTP/1.1 so rules can match method, path, and headers. `udp` is reserved.
pub const PROTOCOL_TCP: &str = "tcp";
pub const PROTOCOL_HTTP: &str = "http";
pub const PROTOCOLS: &[&str] = &[PROTOCOL_TCP, PROTOCOL_HTTP];

/// Fallthrough verdicts and rule actions.
pub const ACTION_ALLOW: &str = "allow";
pub const ACTION_DENY: &str = "deny";
pub const ACTIONS: &[&str] = &[ACTION_ALLOW, ACTION_DENY];

/// What a failed handler (timeout, refused, garbage) means for the flow.
pub const ON_ERROR_DENY: &str = "deny";
pub const ON_ERROR_ALLOW: &str = "allow";
pub const ON_ERRORS: &[&str] = &[ON_ERROR_DENY, ON_ERROR_ALLOW];

pub use crate::structs::hops::HopConfig;

/// `network.mode` values. `host` is today's networking; `gated` is a namespace
/// containing loopback and the datapath only; `auto` picks `gated` where the
/// runtime and platform can and `host` otherwise. `bridge` / `cni/<name>` are
/// reserved for the datapath plugin and rejected at submit with that reason.
pub const MODE_AUTO: &str = "auto";
pub const MODE_HOST: &str = "host";
pub const MODE_GATED: &str = "gated";
pub const MODES: &[&str] = &[MODE_AUTO, MODE_HOST, MODE_GATED];

/// The verdict source when a gated allocation's egress reaches a
/// destination and no task block or policy object covers its egress at all
/// (gated plan §6.5, calls entry 3): gated means nothing leaves unless a
/// rule says so, on every runtime. It names itself in the `403`, the
/// `net.denied` event, and `legion net policy test`.
pub const SOURCE_GATED_DEFAULT: &str = "gated-default";

/// Why [`SOURCE_GATED_DEFAULT`] denied `destination`, as the verdict's reason.
pub fn gated_default_reason(destination: &str) -> String {
    format!("the task is gated and nothing allows {destination}")
}

/// The fix [`SOURCE_GATED_DEFAULT`] points at, for `destination` (a host,
/// possibly with a port).
pub fn gated_default_hint(destination: &str) -> String {
    format!(
        "add {destination:?} to network.egress.allow, or apply a policy object; \
         network.egress = {{ policy = \"allow\" }} allows everything"
    )
}

/// TLS modes on a chain. Ingress: `off`, `server`, `mutual`. Egress:
/// `passthrough` (default; rules see SNI only) or `originate` (the task speaks
/// plaintext to the datapath, which adds TLS to the destination).
pub const TLS_OFF: &str = "off";
pub const TLS_SERVER: &str = "server";
pub const TLS_MUTUAL: &str = "mutual";
pub const TLS_PASSTHROUGH: &str = "passthrough";
pub const TLS_ORIGINATE: &str = "originate";
pub const TLS_MODES_INGRESS: &[&str] = &[TLS_OFF, TLS_SERVER, TLS_MUTUAL];
pub const TLS_MODES_EGRESS: &[&str] = &[TLS_PASSTHROUGH, TLS_ORIGINATE];

/// Hard cap on an `authz` verdict cache TTL, so a revoked allow cannot outlive
/// a policy change by more than this.
pub const MAX_AUTHZ_CACHE: &str = "5m";

/// Namespace every job lives in until namespaces land.
pub const DEFAULT_NAMESPACE: &str = "default";

// ---------------------------------------------------------------------------
// Policy objects
// ---------------------------------------------------------------------------

/// Which tasks a stored policy applies to. Every list is a disjunction
/// (`nodePools = ["a", "b"]` matches either); the fields are a conjunction; an
/// empty selector matches every task on the cluster.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct NetSelector {
    /// `node.pool` globs. Real today (`daemon.rs` registration field).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub node_pools: Vec<String>,
    /// Namespace globs. Every job is in `default` until namespaces land, so a
    /// selector written now keeps working when they do.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub namespaces: Vec<String>,
    /// Task runtime names (`sandbox`, `container`, `exec`, ...).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtimes: Vec<String>,
    /// Job id globs (`agent-*`), the same style as `AclRule.names`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jobs: Vec<String>,
    /// Task `network.labels` that must all be present with these values.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

/// The facts a selector is matched against, gathered by the manager for one
/// (job, task, node) at dispatch time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NetSelectorFacts {
    pub node_pool: Option<String>,
    pub namespace: String,
    pub runtime: String,
    pub job: String,
    pub labels: BTreeMap<String, String>,
}

impl NetSelector {
    pub fn is_empty(&self) -> bool {
        self.node_pools.is_empty()
            && self.namespaces.is_empty()
            && self.runtimes.is_empty()
            && self.jobs.is_empty()
            && self.labels.is_empty()
    }

    pub fn matches(&self, facts: &NetSelectorFacts) -> bool {
        let pool_ok = self.node_pools.is_empty()
            || facts
                .node_pool
                .as_deref()
                .is_some_and(|pool| self.node_pools.iter().any(|glob| glob_matches(glob, pool)));
        let namespace = if facts.namespace.is_empty() {
            DEFAULT_NAMESPACE
        } else {
            facts.namespace.as_str()
        };
        let ns_ok = self.namespaces.is_empty()
            || self
                .namespaces
                .iter()
                .any(|glob| glob_matches(glob, namespace));
        let runtime_ok = self.runtimes.is_empty()
            || self
                .runtimes
                .iter()
                .any(|runtime| runtime.eq_ignore_ascii_case(&facts.runtime));
        let job_ok =
            self.jobs.is_empty() || self.jobs.iter().any(|glob| glob_matches(glob, &facts.job));
        let labels_ok = self
            .labels
            .iter()
            .all(|(key, value)| facts.labels.get(key).is_some_and(|have| have == value));
        pool_ok && ns_ok && runtime_ok && job_ok && labels_ok
    }
}

/// A stored, selector-scoped policy object — what `legion net policy apply`
/// writes. A task's inline `network.egress` / `network.ingress` is the same
/// shape with an implicit selector of exactly that task.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NetPolicyObject {
    pub name: String,
    /// Higher applies first among `require` policies and wins among
    /// `default` ones. Ties among matching `default` policies are rejected at
    /// `apply`, never resolved at runtime.
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub selector: NetSelector,
    /// `require` or `default`; see the constants.
    #[serde(default = "default_enforcement")]
    pub enforcement: String,
    /// Optional `network.mode` to impose (`require`) or suggest (`default`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<ChainConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<ChainConfig>,
    /// Free-form, shown by `describe`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
}

fn default_enforcement() -> String {
    ENFORCEMENT_DEFAULT.to_string()
}

impl NetPolicyObject {
    pub fn is_required(&self) -> bool {
        self.enforcement == ENFORCEMENT_REQUIRE
    }

    /// Normalize sugar and validate every field. Called at `apply` and at job
    /// submit (for the inline form, via [`validate_inline_chain`]).
    pub fn normalize_and_validate(&mut self) -> R<()> {
        let what = format!("policy '{}'", self.name);
        if self.name.trim().is_empty() {
            bail!("net policy name must not be empty");
        }
        if !self
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        {
            bail!("{what}: name may contain only letters, digits, '-', '_' and '.'");
        }
        self.enforcement = self.enforcement.trim().to_ascii_lowercase();
        if !ENFORCEMENTS.contains(&self.enforcement.as_str()) {
            bail!(
                "{what}: enforcement must be one of {}, got '{}'",
                ENFORCEMENTS.join(", "),
                self.enforcement
            );
        }
        if let Some(mode) = self.mode.as_mut() {
            *mode = mode.trim().to_ascii_lowercase();
            validate_network_mode(mode)?;
        }
        for glob in self
            .selector
            .node_pools
            .iter()
            .chain(self.selector.namespaces.iter())
            .chain(self.selector.jobs.iter())
        {
            if glob.trim().is_empty() {
                bail!("{what}: selector entries must not be empty");
            }
        }
        if let Some(chain) = self.egress.as_mut() {
            chain.normalize_and_validate(&format!("{what} egress"), Direction::Egress)?;
        }
        if let Some(chain) = self.ingress.as_mut() {
            chain.normalize_and_validate(&format!("{what} ingress"), Direction::Ingress)?;
        }
        if self.egress.is_none() && self.ingress.is_none() && self.mode.is_none() {
            bail!("{what}: declares neither egress, ingress, nor mode — nothing to apply");
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Chains, rules, handlers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Ingress,
    Egress,
}

impl Direction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Direction::Ingress => "ingress",
            Direction::Egress => "egress",
        }
    }
}

/// One direction's policy: the fallthrough verdict, the rules, the handlers
/// that run when policy is not enough, and TLS. `allow` is sugar for a list of
/// allow rules plus `policy = "deny"`; it is desugared in
/// [`ChainConfig::normalize_and_validate`], so downstream code only ever sees
/// `rules`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChainConfig {
    /// `tcp` (default) or `http`. Never inferred from the rules.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    /// Fallthrough when no rule matches: `allow` or `deny`. Defaults to `deny`
    /// when `allow` sugar is used, `allow` otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    /// One NDJSON line per HTTP request in the allocation's
    /// `net-access.log`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub access_log: bool,
    /// Sugar: `host`, `host:port`, `cidr`, `cidr:port`, or (HTTP only)
    /// `host/path`. Desugared into `rules` ahead of any explicit rules.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<RuleConfig>,
    /// Hops that run after the rules allow a flow; each says what a failure
    /// means with its own `onError`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub handlers: Vec<HopConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,
}

/// One rule: first match wins.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RuleConfig {
    /// Written `when` (`match` is a Nickel keyword; it is still accepted as
    /// an alias for JSON callers).
    #[serde(rename = "when", alias = "match", default)]
    pub matcher: RuleMatch,
    /// `allow` or `deny`.
    pub action: String,
    /// Returned to the workload on a denial and recorded in the event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// What a rule matches on. Every present field must match. Host, port, cidr,
/// and identity are connect-time; path, method, and header need HTTP and
/// therefore `protocol = "http"`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RuleMatch {
    /// Host glob: `*` matches one or more labels at a label boundary
    /// (`*.pypi.org` matches `files.pypi.org` and `a.b.pypi.org`, not
    /// `pypi.org`); a bare `*` matches every host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// IPv4/IPv6 CIDR the *resolved* destination (egress) or the peer
    /// (ingress) must fall in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cidr: Option<String>,
    /// Path glob: `*` matches within one segment, `**` matches across.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// HTTP methods, case-insensitive; empty means any.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub method: Vec<String>,
    /// Header name → value glob (`*` = present with any value).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub header: BTreeMap<String, String>,
    /// Workload-identity glob (`prod:*:checkout:*`) of the peer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
}

impl RuleMatch {
    /// Whether deciding this rule needs the HTTP request head.
    pub fn needs_request(&self) -> bool {
        self.path.is_some() || !self.method.is_empty() || !self.header.is_empty()
    }

    pub fn is_empty(&self) -> bool {
        self.host.is_none()
            && self.port.is_none()
            && self.cidr.is_none()
            && self.path.is_none()
            && self.method.is_empty()
            && self.header.is_empty()
            && self.identity.is_none()
    }
}

/// TLS on a chain. Handlers never see TLS; the datapath terminates or
/// originates it and hands handlers the verified peer identity.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    pub mode: String,
    /// `"legion"` (the node's mesh certificate), `{ kv = "<path>" }` (a PEM
    /// bundle held in KV), or absent (use `certPath`/`keyPath`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_from: Option<CertFrom>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_path: Option<String>,
    /// `legion` or a CA bundle path; required for `mutual`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_ca: Option<String>,
    /// Identity globs a `mutual` peer must match.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub require_identity: Vec<String>,
    /// Egress `originate`: verify the destination against the system roots
    /// (default) or this bundle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_path: Option<String>,
}

/// Where a chain's certificate comes from. The string form is the original
/// `certFrom = "legion"` and serializes exactly as before, so existing policy
/// hashes do not move.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum CertFrom {
    /// `"legion"`: the node's mesh certificate.
    Named(String),
    /// `{ kv = "certs/app.example.com" }`: one PEM bundle (certificate chain,
    /// then private key) held in KV, fetched by the node and hot-swapped
    /// when the key's version moves.
    Kv(KvCertSource),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KvCertSource {
    pub kv: String,
}

impl TlsConfig {
    /// Whether the chain presents the node's mesh certificate.
    pub fn cert_from_mesh(&self) -> bool {
        matches!(&self.cert_from, Some(CertFrom::Named(name)) if name == "legion")
    }

    /// The KV path of a `certFrom = { kv = ... }` chain.
    pub fn cert_kv_path(&self) -> Option<&str> {
        match &self.cert_from {
            Some(CertFrom::Kv(source)) => Some(source.kv.as_str()),
            _ => None,
        }
    }
}

impl ChainConfig {
    pub fn protocol_name(&self) -> &str {
        self.protocol.as_deref().unwrap_or(PROTOCOL_TCP)
    }

    pub fn is_http(&self) -> bool {
        self.protocol_name() == PROTOCOL_HTTP
    }

    /// Desugar `allow`, fill defaults, and reject anything the datapath could
    /// not honor — with the field and the fix in the message.
    pub fn normalize_and_validate(&mut self, what: &str, direction: Direction) -> R<()> {
        let protocol = self
            .protocol
            .take()
            .map(|p| p.trim().to_ascii_lowercase())
            .unwrap_or_else(|| PROTOCOL_TCP.to_string());
        if !PROTOCOLS.contains(&protocol.as_str()) {
            let hint = if protocol == "h2"
                || protocol == "http2"
                || protocol == "http3"
                || protocol == "udp"
            {
                " (HTTP/2, HTTP/3 and UDP chains are not implemented yet; see docs/network-handlers.md)"
            } else {
                ""
            };
            bail!(
                "{what}.protocol must be one of {}, got '{protocol}'{hint}",
                PROTOCOLS.join(", ")
            );
        }
        let is_http = protocol == PROTOCOL_HTTP;
        self.protocol = Some(protocol);

        // `allow` sugar: rules first (they are the user's intent), deny
        // fallthrough unless the user said otherwise.
        if !self.allow.is_empty() {
            let mut sugar = Vec::with_capacity(self.allow.len());
            for (index, entry) in self.allow.iter().enumerate() {
                let rule = parse_allow_entry(entry)
                    .map_err(|err| anyhow::anyhow!("{what}.allow[{index}] '{entry}': {err}"))?;
                if rule.matcher.path.is_some() && !is_http {
                    bail!(
                        "{what}.allow[{index}] '{entry}' matches on a path, but {what}.protocol is \
                         'tcp'. Set protocol = \"http\" to match on paths, or drop the path to match \
                         the host only."
                    );
                }
                sugar.push(rule);
            }
            sugar.append(&mut self.rules);
            self.rules = sugar;
            self.allow.clear();
            if self.policy.is_none() {
                self.policy = Some(ACTION_DENY.to_string());
            }
        }

        let policy = self
            .policy
            .take()
            .map(|p| p.trim().to_ascii_lowercase())
            .unwrap_or_else(|| ACTION_ALLOW.to_string());
        if !ACTIONS.contains(&policy.as_str()) {
            bail!(
                "{what}.policy must be one of {}, got '{policy}'",
                ACTIONS.join(", ")
            );
        }
        self.policy = Some(policy);

        for (index, rule) in self.rules.iter_mut().enumerate() {
            rule.action = rule.action.trim().to_ascii_lowercase();
            if !ACTIONS.contains(&rule.action.as_str()) {
                bail!(
                    "{what}.rules[{index}].action must be one of {}, got '{}'",
                    ACTIONS.join(", "),
                    rule.action
                );
            }
            if rule.matcher.needs_request() && !is_http {
                bail!(
                    "{what}.rules[{index}] matches on path, method, or header, but {what}.protocol \
                     is 'tcp'. Set protocol = \"http\" to match on requests."
                );
            }
            if let Some(host) = rule.matcher.host.as_mut() {
                *host = host.trim().to_ascii_lowercase();
                if host.is_empty() {
                    bail!("{what}.rules[{index}].match.host must not be empty");
                }
            }
            if let Some(cidr) = rule.matcher.cidr.as_deref() {
                parse_cidr(cidr)
                    .map_err(|err| anyhow::anyhow!("{what}.rules[{index}].match.cidr: {err}"))?;
            }
            if let Some(port) = rule.matcher.port {
                if port == 0 {
                    bail!("{what}.rules[{index}].match.port must be greater than 0");
                }
            }
            if let Some(path) = rule.matcher.path.as_deref() {
                if !path.starts_with('/') {
                    bail!("{what}.rules[{index}].match.path must start with '/'");
                }
            }
            for method in rule.matcher.method.iter_mut() {
                *method = method.trim().to_ascii_uppercase();
                if method.is_empty() {
                    bail!("{what}.rules[{index}].match.method entries must not be empty");
                }
            }
            let headers: BTreeMap<String, String> = rule
                .matcher
                .header
                .iter()
                .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.clone()))
                .collect();
            rule.matcher.header = headers;
        }

        let not_http = format!("{what}.protocol is \"tcp\". Set protocol = \"http\", or put it on an upstream whose `to` is http:// or https://");
        for (index, handler) in self.handlers.iter_mut().enumerate() {
            handler.normalize(&format!("{what}.handlers[{index}]"), is_http, &not_http)?;
        }

        if let Some(tls) = self.tls.as_mut() {
            tls.mode = tls.mode.trim().to_ascii_lowercase();
            let allowed = match direction {
                Direction::Ingress => TLS_MODES_INGRESS,
                Direction::Egress => TLS_MODES_EGRESS,
            };
            if !allowed.contains(&tls.mode.as_str()) {
                bail!(
                    "{what}.tls.mode must be one of {} for {}, got '{}'",
                    allowed.join(", "),
                    direction.as_str(),
                    tls.mode
                );
            }
            match tls.cert_from.as_mut() {
                Some(CertFrom::Named(from)) => {
                    *from = from.trim().to_ascii_lowercase();
                    if from != "legion" {
                        bail!(
                            "{what}.tls.certFrom must be \"legion\" or {{ kv = \"<path>\" }} \
                             (or omit it and set certPath/keyPath)"
                        );
                    }
                }
                Some(CertFrom::Kv(source)) => {
                    source.kv = source.kv.trim().to_string();
                    if source.kv.is_empty() {
                        bail!("{what}.tls.certFrom.kv cannot be empty");
                    }
                    if !matches!(direction, Direction::Ingress) {
                        bail!("{what}.tls.certFrom = {{ kv }} is only supported on ingress");
                    }
                    if tls.cert_path.is_some() || tls.key_path.is_some() {
                        bail!(
                            "{what}.tls.certFrom = {{ kv }} replaces certPath/keyPath; set one or \
                             the other"
                        );
                    }
                }
                None => {}
            }
            let needs_cert = matches!(tls.mode.as_str(), TLS_SERVER | TLS_MUTUAL);
            if needs_cert
                && tls.cert_from.is_none()
                && (tls.cert_path.is_none() || tls.key_path.is_none())
            {
                bail!(
                    "{what}.tls.mode = \"{}\" needs certFrom (\"legion\" or {{ kv = \"<path>\" }}) \
                     or both certPath and keyPath",
                    tls.mode
                );
            }
            if tls.mode == TLS_MUTUAL && tls.client_ca.is_none() {
                bail!(
                    "{what}.tls.mode = \"mutual\" needs clientCa (\"legion\" or a CA bundle path)"
                );
            }
            if !tls.require_identity.is_empty() && tls.mode != TLS_MUTUAL {
                bail!("{what}.tls.requireIdentity needs tls.mode = \"mutual\"");
            }
        }
        Ok(())
    }
}

/// `host`, `host:port`, `cidr`, `cidr:port`, or `host/path` → one allow rule.
fn parse_allow_entry(entry: &str) -> R<RuleConfig> {
    let entry = entry.trim();
    if entry.is_empty() {
        bail!("empty entry");
    }
    if entry.contains("://") {
        bail!("looks like a URL; write host, host:port, cidr, or (with protocol = \"http\") host/path");
    }
    let mut matcher = RuleMatch::default();

    // CIDR first: `a.b.c.d/n[:port]` or `[v6]/n[:port]` / `v6/n`. Its slash is
    // a prefix length, not a path.
    if let Some((addr, rest)) = entry.split_once('/') {
        let (prefix, port) = match rest.split_once(':') {
            Some((prefix, port)) => (prefix, Some(port)),
            None => (rest, None),
        };
        if !prefix.is_empty() && prefix.chars().all(|c| c.is_ascii_digit()) {
            let addr = addr.trim_matches(|c| c == '[' || c == ']');
            if addr.parse::<IpAddr>().is_ok() {
                let cidr = format!("{addr}/{prefix}");
                parse_cidr(&cidr)?;
                matcher.cidr = Some(cidr);
                if let Some(port) = port {
                    matcher.port = Some(
                        port.parse()
                            .map_err(|_| anyhow::anyhow!("invalid port in '{entry}'"))?,
                    );
                }
                return Ok(RuleConfig {
                    matcher,
                    action: ACTION_ALLOW.to_string(),
                    reason: None,
                });
            }
        }
    }

    // Otherwise: host[:port][/path], or a bare IP[:port].
    let (target, path) = match entry.find('/') {
        Some(slash) => (&entry[..slash], Some(entry[slash..].to_string())),
        None => (entry, None),
    };
    let (target, port) = if let Some(end) = target.strip_prefix('[') {
        // IPv6 literal with optional port.
        let (host, tail) = end
            .split_once(']')
            .ok_or_else(|| anyhow::anyhow!("unterminated IPv6 literal in '{entry}'"))?;
        let port = tail
            .strip_prefix(':')
            .map(|p| {
                p.parse::<u16>()
                    .map_err(|_| anyhow::anyhow!("invalid port in '{entry}'"))
            })
            .transpose()?;
        (host, port)
    } else {
        match target.rfind(':') {
            Some(colon)
                if colon > 0
                    && target[colon + 1..].chars().all(|c| c.is_ascii_digit())
                    && !target[colon + 1..].is_empty()
                    && target[..colon].matches(':').count() == 0 =>
            {
                let port: u16 = target[colon + 1..]
                    .parse()
                    .map_err(|_| anyhow::anyhow!("invalid port in '{entry}'"))?;
                (&target[..colon], Some(port))
            }
            _ => (target, None),
        }
    };
    matcher.port = port;
    matcher.path = path;
    if let Ok(ip) = target.parse::<IpAddr>() {
        let cidr = if ip.is_ipv4() {
            format!("{target}/32")
        } else {
            format!("{target}/128")
        };
        matcher.cidr = Some(cidr);
    } else {
        if target.is_empty() {
            bail!("entry has no host");
        }
        matcher.host = Some(target.to_ascii_lowercase());
    }
    if matcher.is_empty() {
        bail!("entry matches nothing");
    }
    Ok(RuleConfig {
        matcher,
        action: ACTION_ALLOW.to_string(),
        reason: None,
    })
}

/// Validate a task's inline `network.egress` / `network.ingress` at submit.
pub fn validate_inline_chain(
    task_name: &str,
    direction: Direction,
    chain: &mut ChainConfig,
) -> R<()> {
    chain.normalize_and_validate(
        &format!("task {task_name} network.{}", direction.as_str()),
        direction,
    )
}

/// `network.mode` must be a known value; the reserved datapath-plugin modes
/// name the reason they are rejected.
pub fn validate_network_mode(mode: &str) -> R<()> {
    let mode = mode.trim().to_ascii_lowercase();
    if MODES.contains(&mode.as_str()) {
        return Ok(());
    }
    if mode == "bridge" || mode.starts_with("cni/") || mode == "cni" {
        bail!(
            "network.mode '{mode}' is reserved for the datapath plugin, which is not implemented; \
             use one of {}",
            MODES.join(", ")
        );
    }
    bail!(
        "network.mode must be one of {}, got '{mode}'",
        MODES.join(", ")
    );
}

// ---------------------------------------------------------------------------
// The merge
// ---------------------------------------------------------------------------

/// A rule in a merged chain, with the policy it came from.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveRule {
    #[serde(flatten)]
    pub rule: RuleConfig,
    /// `policy/<name>#<index>` or `task#<index>`.
    pub source: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveHandler {
    #[serde(flatten)]
    pub handler: HopConfig,
    pub source: String,
}

/// One direction after the merge — what the datapath runs.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveChain {
    pub protocol: String,
    pub policy: String,
    pub access_log: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<EffectiveRule>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub handlers: Vec<EffectiveHandler>,
}

impl EffectiveChain {
    pub fn is_http(&self) -> bool {
        self.protocol == PROTOCOL_HTTP
    }

    pub fn denies_by_default(&self) -> bool {
        self.policy == ACTION_DENY
    }

    /// True when every rule is decidable at connect time, so the chain never
    /// needs to parse a byte.
    pub fn is_connect_only(&self) -> bool {
        self.rules
            .iter()
            .all(|rule| !rule.rule.matcher.needs_request())
    }

    /// Whether any rule matches on a CIDR — in which case a name cannot be
    /// judged at resolution time (the address is not known yet) and DNS must
    /// answer and let the connect-time check decide.
    pub fn uses_cidr(&self) -> bool {
        self.rules
            .iter()
            .any(|rule| rule.rule.matcher.cidr.is_some())
    }
}

/// The whole merged policy for one allocation.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct EffectivePolicy {
    /// Imposed or suggested `network.mode`, if any policy set one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<EffectiveChain>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<EffectiveChain>,
    /// Policy names that contributed, in application order; `task` for the
    /// inline block.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<String>,
    /// Content hash of everything above — the key the heartbeat carries and
    /// the worker caches by. Stable across field order.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hash: String,
}

impl EffectivePolicy {
    pub fn is_empty(&self) -> bool {
        self.mode.is_none() && self.egress.is_none() && self.ingress.is_none()
    }

    fn compute_hash(&mut self) {
        self.hash.clear();
        let canonical = serde_json::to_vec(&self).expect("effective policy serializes");
        self.hash = format!("{:x}", Sha256::digest(&canonical));
    }
}

/// The task's own inline declaration, as the merge sees it.
#[derive(Debug, Clone, Default)]
pub struct InlinePolicy<'a> {
    pub mode: Option<&'a str>,
    pub egress: Option<&'a ChainConfig>,
    pub ingress: Option<&'a ChainConfig>,
}

/// Merge the policies matching one task into what the datapath runs.
///
/// `matching` is every stored policy whose selector matched, in any order:
/// this function sorts. Rules: every `require` policy in priority order (ties
/// by name); then, per direction, exactly one of — the task's inline chain if
/// it declares one, else the highest-priority matching `default` policy's
/// chain. Two `default` policies of equal priority both matching is a
/// configuration error the caller should have rejected at `apply`; here the
/// name breaks the tie so the result is still deterministic.
pub fn merge_policies(matching: &[NetPolicyObject], inline: InlinePolicy<'_>) -> EffectivePolicy {
    let mut required: Vec<&NetPolicyObject> = matching.iter().filter(|p| p.is_required()).collect();
    required.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then_with(|| a.name.cmp(&b.name))
    });
    let mut defaults: Vec<&NetPolicyObject> =
        matching.iter().filter(|p| !p.is_required()).collect();
    defaults.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then_with(|| a.name.cmp(&b.name))
    });
    let default = defaults.first().copied();

    // Mode: the first required policy that sets one wins; else the task; else
    // the default policy.
    let mut out = EffectivePolicy {
        mode: required
            .iter()
            .find_map(|p| p.mode.clone())
            .or_else(|| inline.mode.map(|m| m.to_string()))
            .or_else(|| default.and_then(|p| p.mode.clone())),
        ..Default::default()
    };

    for policy in &required {
        out.sources.push(format!("policy/{}", policy.name));
    }
    if inline.egress.is_some() || inline.ingress.is_some() || inline.mode.is_some() {
        out.sources.push("task".to_string());
    }
    if let Some(default) = default {
        let used = (inline.egress.is_none() && default.egress.is_some())
            || (inline.ingress.is_none() && default.ingress.is_some())
            || (inline.mode.is_none() && default.mode.is_some());
        if used {
            out.sources.push(format!("policy/{}", default.name));
        }
    }

    out.egress = merge_direction(
        &required,
        required.iter().map(|p| p.egress.as_ref()).collect(),
        inline.egress,
        default
            .and_then(|p| p.egress.as_ref())
            .map(|c| (default.unwrap().name.as_str(), c)),
    );
    out.ingress = merge_direction(
        &required,
        required.iter().map(|p| p.ingress.as_ref()).collect(),
        inline.ingress,
        default
            .and_then(|p| p.ingress.as_ref())
            .map(|c| (default.unwrap().name.as_str(), c)),
    );
    out.compute_hash();
    out
}

fn merge_direction(
    required: &[&NetPolicyObject],
    required_chains: Vec<Option<&ChainConfig>>,
    inline: Option<&ChainConfig>,
    default: Option<(&str, &ChainConfig)>,
) -> Option<EffectiveChain> {
    let tail: Option<(String, &ChainConfig)> = match (inline, default) {
        (Some(chain), _) => Some(("task".to_string(), chain)),
        (None, Some((name, chain))) => Some((format!("policy/{name}"), chain)),
        (None, None) => None,
    };
    let contributing: Vec<(String, &ChainConfig)> = required
        .iter()
        .zip(required_chains)
        .filter_map(|(policy, chain)| chain.map(|c| (format!("policy/{}", policy.name), c)))
        .chain(tail)
        .collect();
    if contributing.is_empty() {
        return None;
    }

    let mut merged = EffectiveChain {
        protocol: PROTOCOL_TCP.to_string(),
        policy: ACTION_ALLOW.to_string(),
        access_log: false,
        tls: None,
        rules: Vec::new(),
        handlers: Vec::new(),
    };
    let mut any_required = false;
    let mut required_denies = false;
    let mut last_policy = ACTION_ALLOW.to_string();
    for (source, chain) in &contributing {
        let is_required = source != "task"
            && required
                .iter()
                .any(|p| format!("policy/{}", p.name) == *source);
        if chain.is_http() {
            merged.protocol = PROTOCOL_HTTP.to_string();
        }
        merged.access_log |= chain.access_log;
        // TLS: a required policy's TLS wins; otherwise the last contributor's
        // — but never let a task or default override what a required policy
        // set.
        if chain.tls.is_some() && (is_required || !any_required) {
            merged.tls = chain.tls.clone();
        }
        if is_required {
            any_required = true;
            if chain.policy.as_deref() == Some(ACTION_DENY) {
                required_denies = true;
            }
        }
        last_policy = chain
            .policy
            .clone()
            .unwrap_or_else(|| ACTION_ALLOW.to_string());
        for (index, rule) in chain.rules.iter().enumerate() {
            merged.rules.push(EffectiveRule {
                rule: rule.clone(),
                source: format!("{source}#{index}"),
            });
        }
        for (index, handler) in chain.handlers.iter().enumerate() {
            merged.handlers.push(EffectiveHandler {
                handler: handler.clone(),
                source: format!("{source}#{index}"),
            });
        }
    }
    // Fallthrough: a required deny cannot be widened by anyone after it.
    merged.policy = if required_denies {
        ACTION_DENY.to_string()
    } else {
        last_policy
    };
    Some(merged)
}

/// Reject two `default` policies that would tie for the same task. Called at
/// `apply` against the existing set; `candidate` is the one being written.
pub fn find_default_tie<'a>(
    candidate: &NetPolicyObject,
    existing: &'a [NetPolicyObject],
) -> Option<&'a NetPolicyObject> {
    if candidate.is_required() {
        return None;
    }
    existing.iter().find(|other| {
        other.name != candidate.name
            && !other.is_required()
            && other.priority == candidate.priority
            && selectors_may_overlap(&other.selector, &candidate.selector)
    })
}

/// Conservative overlap test: two selectors can match the same task unless a
/// field they both constrain has no common value. Globs are treated as
/// overlapping with anything.
fn selectors_may_overlap(a: &NetSelector, b: &NetSelector) -> bool {
    fn lists_overlap(a: &[String], b: &[String]) -> bool {
        if a.is_empty() || b.is_empty() {
            return true;
        }
        a.iter().any(|x| {
            b.iter()
                .any(|y| x.contains('*') || y.contains('*') || x.eq_ignore_ascii_case(y))
        })
    }
    lists_overlap(&a.node_pools, &b.node_pools)
        && lists_overlap(&a.namespaces, &b.namespaces)
        && lists_overlap(&a.runtimes, &b.runtimes)
        && lists_overlap(&a.jobs, &b.jobs)
        && a.labels
            .iter()
            .all(|(k, v)| b.labels.get(k).is_none_or(|bv| bv == v))
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// The outcome of evaluating a chain's rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Allow,
    Deny {
        /// `policy/<name>#<index>`, `task#<index>`, or `fallthrough`.
        source: String,
        reason: Option<String>,
    },
    /// Connect-time only: the first potentially matching rule needs the HTTP
    /// request head. The connection proceeds to the L7 layer, which decides.
    Defer,
}

impl Verdict {
    pub fn is_allow(&self) -> bool {
        matches!(self, Verdict::Allow)
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Verdict::Allow => "allow",
            Verdict::Deny { .. } => "deny",
            Verdict::Defer => "defer",
        }
    }
}

/// What is known at connect (or DNS resolution) time.
#[derive(Debug, Clone, Default)]
pub struct ConnectFacts<'a> {
    /// Destination host name or SNI (egress); `None` when only an IP is known.
    pub host: Option<&'a str>,
    /// Resolved destination IP (egress) or peer IP (ingress).
    pub ip: Option<IpAddr>,
    pub port: u16,
    /// Verified peer identity, when any.
    pub identity: Option<&'a str>,
}

/// What is known at the HTTP request head.
#[derive(Debug, Clone, Default)]
pub struct RequestFacts<'a> {
    pub host: Option<&'a str>,
    pub ip: Option<IpAddr>,
    pub port: u16,
    pub identity: Option<&'a str>,
    pub method: &'a str,
    pub path: &'a str,
    /// Lower-cased header names.
    pub headers: &'a [(String, String)],
}

fn connect_parts_match(
    matcher: &RuleMatch,
    host: Option<&str>,
    ip: Option<IpAddr>,
    port: u16,
    identity: Option<&str>,
) -> bool {
    // Port 0 means "not known yet" (a DNS-time check): a port constraint is
    // then treated as satisfied, so resolution is never refused for a name
    // the connect-time check would allow.
    if let Some(want) = matcher.port {
        if port != 0 && want != port {
            return false;
        }
    }
    if let Some(glob) = matcher.host.as_deref() {
        match host {
            Some(host) if host_matches(glob, host) => {}
            _ => return false,
        }
    }
    if let Some(cidr) = matcher.cidr.as_deref() {
        match (ip, parse_cidr(cidr)) {
            (Some(ip), Ok(net)) if net.contains(ip) => {}
            _ => return false,
        }
    }
    if let Some(glob) = matcher.identity.as_deref() {
        match identity {
            Some(identity) if glob_matches(glob, identity) => {}
            _ => return false,
        }
    }
    true
}

/// Decide a connection from connect-time facts alone. Returns [`Verdict::Defer`]
/// when the first rule that could match needs the request head.
pub fn evaluate_connect(chain: &EffectiveChain, facts: &ConnectFacts<'_>) -> Verdict {
    for entry in &chain.rules {
        let matcher = &entry.rule.matcher;
        if !connect_parts_match(matcher, facts.host, facts.ip, facts.port, facts.identity) {
            continue;
        }
        if matcher.needs_request() {
            return Verdict::Defer;
        }
        return verdict_for(&entry.rule, &entry.source);
    }
    fallthrough(chain)
}

/// The source of the rule that makes [`evaluate_connect`] answer
/// [`Verdict::Defer`]: the first rule whose connect-time parts match and that
/// needs the request head. `None` when the chain decides at connect.
pub fn deferring_rule(chain: &EffectiveChain, facts: &ConnectFacts<'_>) -> Option<String> {
    for entry in &chain.rules {
        let matcher = &entry.rule.matcher;
        if !connect_parts_match(matcher, facts.host, facts.ip, facts.port, facts.identity) {
            continue;
        }
        return matcher.needs_request().then(|| entry.source.clone());
    }
    None
}

/// Decide a request from the HTTP head. Connect-time parts are re-checked so
/// a chain evaluates identically whether or not `evaluate_connect` ran first.
pub fn evaluate_request(chain: &EffectiveChain, facts: &RequestFacts<'_>) -> Verdict {
    for entry in &chain.rules {
        let matcher = &entry.rule.matcher;
        if !connect_parts_match(matcher, facts.host, facts.ip, facts.port, facts.identity) {
            continue;
        }
        if !matcher.method.is_empty()
            && !matcher
                .method
                .iter()
                .any(|m| m.eq_ignore_ascii_case(facts.method))
        {
            continue;
        }
        if let Some(glob) = matcher.path.as_deref() {
            if !path_matches(glob, facts.path) {
                continue;
            }
        }
        let headers_ok = matcher.header.iter().all(|(name, glob)| {
            facts
                .headers
                .iter()
                .any(|(have, value)| have == name && glob_matches(glob, value))
        });
        if !headers_ok {
            continue;
        }
        return verdict_for(&entry.rule, &entry.source);
    }
    fallthrough(chain)
}

fn verdict_for(rule: &RuleConfig, source: &str) -> Verdict {
    if rule.action == ACTION_ALLOW {
        Verdict::Allow
    } else {
        Verdict::Deny {
            source: source.to_string(),
            reason: rule.reason.clone(),
        }
    }
}

fn fallthrough(chain: &EffectiveChain) -> Verdict {
    if chain.denies_by_default() {
        Verdict::Deny {
            source: "fallthrough".to_string(),
            reason: Some(format!("no rule matched and {}.policy is deny", "chain")),
        }
    } else {
        Verdict::Allow
    }
}

// ---------------------------------------------------------------------------
// Matching primitives
// ---------------------------------------------------------------------------

/// Generic `*` glob (any run of characters), case-insensitive. Used for job
/// ids, identities, header values, and selector fields.
pub fn glob_matches(glob: &str, value: &str) -> bool {
    fn inner(g: &[u8], v: &[u8]) -> bool {
        match g.first() {
            None => v.is_empty(),
            Some(b'*') => (0..=v.len()).any(|i| inner(&g[1..], &v[i..])),
            Some(c) => {
                v.first().is_some_and(|x| x.eq_ignore_ascii_case(c)) && inner(&g[1..], &v[1..])
            }
        }
    }
    inner(glob.as_bytes(), value.as_bytes())
}

/// Host glob: `*` matches one or more DNS labels at a label boundary.
/// `*.pypi.org` matches `files.pypi.org` and `a.b.pypi.org`, never `pypi.org`;
/// a bare `*` matches everything; anything else is an exact (case-insensitive)
/// match. A trailing dot on either side is ignored.
pub fn host_matches(glob: &str, host: &str) -> bool {
    let glob = glob.trim_end_matches('.').to_ascii_lowercase();
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if glob == "*" {
        return true;
    }
    if let Some(suffix) = glob.strip_prefix("*.") {
        return host.len() > suffix.len() + 1
            && host.ends_with(suffix)
            && host.as_bytes()[host.len() - suffix.len() - 1] == b'.';
    }
    if glob.contains('*') {
        return glob_matches(&glob, &host);
    }
    glob == host
}

/// Path glob: `*` matches within one segment, `**` matches across segments.
/// A pattern ending in `/**` also matches the bare prefix.
pub fn path_matches(glob: &str, path: &str) -> bool {
    fn inner(g: &[&str], p: &[&str]) -> bool {
        match g.first() {
            None => p.is_empty(),
            Some(&"**") => (0..=p.len()).any(|i| inner(&g[1..], &p[i..])),
            Some(seg) => p.first().is_some_and(|x| glob_matches(seg, x)) && inner(&g[1..], &p[1..]),
        }
    }
    let path = path.split('?').next().unwrap_or(path);
    let g: Vec<&str> = glob
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    let p: Vec<&str> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    inner(&g, &p)
}

/// A parsed CIDR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cidr {
    pub addr: IpAddr,
    pub prefix: u8,
}

impl Cidr {
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, ip) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - u32::from(self.prefix))
                };
                (u32::from(net) & mask) == (u32::from(ip) & mask)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - u32::from(self.prefix))
                };
                (u128::from(net) & mask) == (u128::from(ip) & mask)
            }
            _ => false,
        }
    }
}

pub fn parse_cidr(text: &str) -> R<Cidr> {
    let (addr, prefix) = match text.split_once('/') {
        Some((addr, prefix)) => (addr, Some(prefix)),
        None => (text, None),
    };
    let addr: IpAddr = addr
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("'{text}' is not an IP address or CIDR"))?;
    let max = if addr.is_ipv4() { 32 } else { 128 };
    let prefix = match prefix {
        Some(p) => p
            .trim()
            .parse::<u8>()
            .ok()
            .filter(|p| *p <= max)
            .ok_or_else(|| anyhow::anyhow!("'{text}' has an invalid prefix length (0..={max})"))?,
        None => max,
    };
    Ok(Cidr { addr, prefix })
}

/// `500ms`, `2s`, `5m`, `1h`, or a bare number of seconds.
pub fn parse_duration(text: &str) -> R<std::time::Duration> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(text.len());
    let (num, unit) = text.split_at(split);
    let value: f64 = num
        .parse()
        .map_err(|_| anyhow::anyhow!("'{text}' is not a duration (e.g. 500ms, 2s, 5m)"))?;
    let secs = match unit.trim() {
        "ms" => value / 1000.0,
        "s" | "" => value,
        "m" => value * 60.0,
        "h" => value * 3600.0,
        other => bail!("'{text}': unknown duration unit '{other}' (use ms, s, m, h)"),
    };
    if secs < 0.0 {
        bail!("'{text}': duration must not be negative");
    }
    Ok(std::time::Duration::from_secs_f64(secs))
}

/// `1Mi`, `512Ki`, `1MB`, `4096`, or a JSON number of bytes.
pub fn parse_bytes(value: &Value) -> R<u64> {
    if let Some(n) = value.as_u64() {
        return Ok(n);
    }
    let Some(text) = value.as_str() else {
        bail!("expected a byte count like \"1Mi\" or a number");
    };
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (num, unit) = text.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| anyhow::anyhow!("'{text}' is not a byte count (e.g. 1Mi, 512Ki)"))?;
    let mult = match unit.trim() {
        "" | "B" => 1,
        "Ki" | "KiB" | "K" | "KB" => 1024,
        "Mi" | "MiB" | "M" | "MB" => 1024 * 1024,
        "Gi" | "GiB" | "G" | "GB" => 1024 * 1024 * 1024,
        other => bail!("'{text}': unknown byte unit '{other}'"),
    };
    Ok(n * mult)
}

/// The label set a task contributes to selector matching and attribution.
pub fn normalize_labels(labels: &BTreeMap<String, String>) -> R<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    let mut seen = HashSet::new();
    for (key, value) in labels {
        let key = key.trim().to_string();
        if key.is_empty() {
            bail!("network.labels keys must not be empty");
        }
        if !key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/')
        {
            bail!(
                "network.labels key '{key}' may contain only letters, digits, '-', '_', '.', '/'"
            );
        }
        if !seen.insert(key.to_ascii_lowercase()) {
            bail!("network.labels key '{key}' is declared more than once (case-insensitive)");
        }
        if value.len() > 256 {
            bail!("network.labels['{key}'] is longer than 256 bytes");
        }
        out.insert(key, value.clone());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `onError` lives on each hop; a chain-level one is refused rather than
    /// silently ignored.
    #[test]
    fn on_error_is_a_hop_setting_test() {
        let err = serde_json::from_value::<ChainConfig>(json!({ "onError": "allow" }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("onError"), "{err}");
        let c = chain(json!({
            "handlers": [{ "authz": { "url": "http://x/check" }, "onError": "allow" }]
        }));
        assert!(c.handlers[0].fails_open());
    }
    use serde_json::json;

    fn chain(v: Value) -> ChainConfig {
        let mut c: ChainConfig = serde_json::from_value(v).unwrap();
        c.normalize_and_validate("test", Direction::Egress).unwrap();
        c
    }

    fn eff(v: Value) -> EffectiveChain {
        let c = chain(v);
        merge_direction(&[], vec![], Some(&c), None).unwrap()
    }

    #[test]
    fn host_glob_semantics() {
        assert!(host_matches("*.pypi.org", "files.pypi.org"));
        assert!(host_matches("*.pypi.org", "a.b.pypi.org"));
        assert!(!host_matches("*.pypi.org", "pypi.org"));
        assert!(!host_matches("*.pypi.org", "evilpypi.org"));
        assert!(host_matches("*", "anything"));
        assert!(host_matches("API.GitHub.com", "api.github.com."));
        assert!(!host_matches("api.github.com", "api.github.com.evil"));
    }

    #[test]
    fn path_glob_semantics() {
        assert!(path_matches("/repos/**", "/repos/x/y"));
        assert!(path_matches("/repos/**", "/repos"));
        assert!(!path_matches("/repos/**", "/user"));
        assert!(path_matches("/v1/*/messages", "/v1/abc/messages"));
        assert!(!path_matches("/v1/*/messages", "/v1/a/b/messages"));
        assert!(path_matches("/admin/**", "/admin/users?x=1"));
    }

    #[test]
    fn cidr_contains() {
        let net = parse_cidr("10.0.0.0/8").unwrap();
        assert!(net.contains("10.1.2.3".parse().unwrap()));
        assert!(!net.contains("11.0.0.1".parse().unwrap()));
        assert!(parse_cidr("10.0.0.0/33").is_err());
        assert!(parse_cidr("fd00::/8")
            .unwrap()
            .contains("fd12::1".parse().unwrap()));
        assert!(parse_cidr("nope").is_err());
    }

    #[test]
    fn allow_sugar_desugars_and_denies_by_default() {
        let c = chain(
            json!({ "allow": ["*.pypi.org", "api.github.com:443", "10.0.0.0/8:5432", "1.2.3.4"] }),
        );
        assert_eq!(c.policy.as_deref(), Some("deny"));
        assert!(c.allow.is_empty());
        assert_eq!(c.rules.len(), 4);
        assert_eq!(c.rules[0].matcher.host.as_deref(), Some("*.pypi.org"));
        assert_eq!(c.rules[1].matcher.port, Some(443));
        assert_eq!(c.rules[2].matcher.cidr.as_deref(), Some("10.0.0.0/8"));
        assert_eq!(c.rules[2].matcher.port, Some(5432));
        assert_eq!(c.rules[3].matcher.cidr.as_deref(), Some("1.2.3.4/32"));
        assert!(c.rules.iter().all(|r| r.action == "allow"));
    }

    #[test]
    fn path_in_tcp_chain_is_rejected_with_the_fix() {
        let mut c: ChainConfig =
            serde_json::from_value(json!({ "allow": ["api.github.com/repos/**"] })).unwrap();
        let err = c
            .normalize_and_validate("egress", Direction::Egress)
            .unwrap_err()
            .to_string();
        assert!(err.contains("matches on a path"), "{err}");
        assert!(err.contains("protocol = \"http\""), "{err}");
        // With http it is accepted and the path survives.
        let c = chain(json!({ "protocol": "http", "allow": ["api.github.com/repos/**"] }));
        assert_eq!(c.rules[0].matcher.path.as_deref(), Some("/repos/**"));
    }

    #[test]
    fn protocol_is_never_inferred_and_h2_is_named() {
        let mut c: ChainConfig = serde_json::from_value(json!({ "protocol": "h2" })).unwrap();
        let err = c
            .normalize_and_validate("egress", Direction::Egress)
            .unwrap_err()
            .to_string();
        assert!(err.contains("not implemented"), "{err}");
    }

    #[test]
    fn unknown_handler_lists_valid_ones() {
        let err = serde_json::from_value::<ChainConfig>(json!({ "handlers": [{ "wasm": {} }] }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("authz, proxy, dlp"), "{err}");
    }

    #[test]
    fn handler_kinds_are_validated_per_kind() {
        for bad in [
            json!({ "handlers": [{ "authz": { "url": "" } }] }),
            json!({ "handlers": [{ "authz": { "url": "http://x", "cache": "10m" } }] }),
            json!({ "handlers": [{ "proxy": {} }] }),
            json!({ "handlers": [{ "proxy": { "service": "a", "port": "b" } }] }),
            json!({ "handlers": [{ "dlp": { "patterns": ["x"] } }] }),
            json!({ "protocol": "http", "handlers": [{ "dlp": { "patterns": [] } }] }),
            json!({ "handlers": [{ "authz": { "url": "http://x" }, "timeout": "soon" }] }),
        ] {
            let mut c: ChainConfig = serde_json::from_value(bad.clone()).unwrap();
            assert!(
                c.normalize_and_validate("egress", Direction::Egress)
                    .is_err(),
                "{bad}"
            );
        }
        let c = chain(
            json!({ "handlers": [{ "authz": { "url": "http://policy/check", "cache": "30s" } }] }),
        );
        assert_eq!(
            c.handlers[0].timeout.as_deref(),
            Some(crate::structs::hops::DEFAULT_HOP_TIMEOUT)
        );
    }

    #[test]
    fn tls_modes_are_per_direction() {
        let mut c: ChainConfig =
            serde_json::from_value(json!({ "tls": { "mode": "mutual" } })).unwrap();
        assert!(c
            .normalize_and_validate("egress", Direction::Egress)
            .is_err());
        let mut c: ChainConfig =
            serde_json::from_value(json!({ "tls": { "mode": "mutual" } })).unwrap();
        let err = c
            .normalize_and_validate("ingress", Direction::Ingress)
            .unwrap_err()
            .to_string();
        assert!(err.contains("certFrom"), "{err}");
        let mut c: ChainConfig = serde_json::from_value(
            json!({ "tls": { "mode": "mutual", "certFrom": "legion", "clientCa": "legion",
                              "requireIdentity": ["prod:*"] } }),
        )
        .unwrap();
        c.normalize_and_validate("ingress", Direction::Ingress)
            .unwrap();
    }

    #[test]
    fn cert_from_kv_is_ingress_only_and_keeps_the_legion_spelling_test() {
        let validate = |tls: Value, direction: Direction| {
            let mut c: ChainConfig = serde_json::from_value(json!({ "tls": tls }))?;
            c.normalize_and_validate("chain", direction)?;
            Ok::<_, anyhow::Error>(c)
        };

        let c = validate(
            json!({ "mode": "server", "certFrom": { "kv": " certs/app " } }),
            Direction::Ingress,
        )
        .unwrap();
        let tls = c.tls.unwrap();
        assert_eq!(tls.cert_kv_path(), Some("certs/app"));
        assert!(!tls.cert_from_mesh());

        // The original string form serializes exactly as before, so stored
        // policy hashes do not move.
        let c = validate(
            json!({ "mode": "server", "certFrom": "legion" }),
            Direction::Ingress,
        )
        .unwrap();
        assert!(c.tls.as_ref().unwrap().cert_from_mesh());
        assert_eq!(
            serde_json::to_value(c.tls.unwrap()).unwrap(),
            json!({ "mode": "server", "certFrom": "legion" })
        );

        for (tls, direction, expected) in [
            (
                json!({ "mode": "originate", "certFrom": { "kv": "certs/app" } }),
                Direction::Egress,
                "only supported on ingress",
            ),
            (
                json!({ "mode": "server", "certFrom": { "kv": "" } }),
                Direction::Ingress,
                "cannot be empty",
            ),
            (
                json!({ "mode": "server", "certFrom": { "kv": "certs/app" }, "certPath": "/x" }),
                Direction::Ingress,
                "replaces certPath/keyPath",
            ),
            (
                json!({ "mode": "server", "certFrom": "vault" }),
                Direction::Ingress,
                "must be \"legion\" or { kv",
            ),
        ] {
            let err = validate(tls, direction).unwrap_err().to_string();
            assert!(err.contains(expected), "{err}");
        }
        // A typo inside the object is a parse error, not a silent miss.
        assert!(serde_json::from_value::<ChainConfig>(
            json!({ "tls": { "mode": "server", "certFrom": { "kvv": "certs/app" } } })
        )
        .is_err());
    }

    #[test]
    fn connect_evaluation_first_match_wins_and_defers_for_http_rules() {
        let c = eff(json!({
            "protocol": "http",
            "policy": "deny",
            "rules": [
                { "match": { "host": "*.pypi.org" }, "action": "allow" },
                { "match": { "host": "api.github.com", "path": "/repos/**" }, "action": "allow" },
                { "match": { "cidr": "10.0.0.0/8" }, "action": "deny", "reason": "internal" }
            ]
        }));
        let allow = evaluate_connect(
            &c,
            &ConnectFacts {
                host: Some("files.pypi.org"),
                port: 443,
                ..Default::default()
            },
        );
        assert_eq!(allow, Verdict::Allow);
        let defer = evaluate_connect(
            &c,
            &ConnectFacts {
                host: Some("api.github.com"),
                port: 443,
                ..Default::default()
            },
        );
        assert_eq!(defer, Verdict::Defer);
        let deny = evaluate_connect(
            &c,
            &ConnectFacts {
                ip: Some("10.2.3.4".parse().unwrap()),
                port: 80,
                ..Default::default()
            },
        );
        assert!(matches!(deny, Verdict::Deny { ref source, .. } if source == "task#2"));
        let fall = evaluate_connect(
            &c,
            &ConnectFacts {
                host: Some("evil.example"),
                port: 443,
                ..Default::default()
            },
        );
        assert!(matches!(fall, Verdict::Deny { ref source, .. } if source == "fallthrough"));
    }

    #[test]
    fn request_evaluation_matches_method_path_and_headers() {
        let c = eff(json!({
            "protocol": "http",
            "policy": "deny",
            "rules": [
                { "match": { "header": { "Authorization": "*" } }, "action": "deny", "reason": "no creds" },
                { "match": { "host": "api.github.com", "path": "/repos/**", "method": ["get"] }, "action": "allow" }
            ]
        }));
        let headers = vec![("authorization".to_string(), "Bearer x".to_string())];
        let denied = evaluate_request(
            &c,
            &RequestFacts {
                host: Some("api.github.com"),
                port: 443,
                method: "GET",
                path: "/repos/a",
                headers: &headers,
                ..Default::default()
            },
        );
        assert!(
            matches!(denied, Verdict::Deny { ref reason, .. } if reason.as_deref() == Some("no creds"))
        );
        let ok = evaluate_request(
            &c,
            &RequestFacts {
                host: Some("api.github.com"),
                port: 443,
                method: "GET",
                path: "/repos/a/b",
                headers: &[],
                ..Default::default()
            },
        );
        assert_eq!(ok, Verdict::Allow);
        let post = evaluate_request(
            &c,
            &RequestFacts {
                host: Some("api.github.com"),
                port: 443,
                method: "POST",
                path: "/repos/a",
                headers: &[],
                ..Default::default()
            },
        );
        assert!(matches!(post, Verdict::Deny { .. }));
    }

    fn policy(name: &str, enforcement: &str, priority: i64, egress: Value) -> NetPolicyObject {
        let mut p: NetPolicyObject = serde_json::from_value(json!({
            "name": name, "enforcement": enforcement, "priority": priority, "egress": egress
        }))
        .unwrap();
        p.normalize_and_validate().unwrap();
        p
    }

    #[test]
    fn merge_orders_required_by_priority_then_task_then_default_with_provenance() {
        let req_low = policy(
            "org",
            "require",
            10,
            json!({ "rules": [{ "match": { "host": "bad.example" }, "action": "deny" }] }),
        );
        let req_high = policy(
            "sec",
            "require",
            100,
            json!({ "policy": "deny", "allow": ["*.internal"] }),
        );
        let def = policy(
            "team-default",
            "default",
            0,
            json!({ "allow": ["api.example"] }),
        );
        let task = chain(json!({ "allow": ["extra.example"] }));

        let merged = merge_policies(
            &[req_low.clone(), def.clone(), req_high.clone()],
            InlinePolicy {
                egress: Some(&task),
                ..Default::default()
            },
        );
        let egress = merged.egress.unwrap();
        let sources: Vec<&str> = egress.rules.iter().map(|r| r.source.as_str()).collect();
        assert_eq!(sources, vec!["policy/sec#0", "policy/org#0", "task#0"]);
        assert_eq!(egress.policy, "deny", "a required deny is never widened");
        assert_eq!(merged.sources, vec!["policy/sec", "policy/org", "task"]);
        assert!(!merged.hash.is_empty());

        // Without an inline chain the default policy's chain is the tail.
        let merged = merge_policies(&[req_low, def, req_high], InlinePolicy::default());
        let egress = merged.egress.unwrap();
        assert_eq!(egress.rules.last().unwrap().source, "policy/team-default#0");
        assert!(merged.sources.contains(&"policy/team-default".to_string()));
    }

    #[test]
    fn merge_hash_is_stable_and_distinguishes_content() {
        let a = policy("a", "require", 1, json!({ "allow": ["x.example"] }));
        let m1 = merge_policies(std::slice::from_ref(&a), InlinePolicy::default());
        let m2 = merge_policies(std::slice::from_ref(&a), InlinePolicy::default());
        assert_eq!(m1.hash, m2.hash);
        let b = policy("a", "require", 1, json!({ "allow": ["y.example"] }));
        let m3 = merge_policies(&[b], InlinePolicy::default());
        assert_ne!(m1.hash, m3.hash);
        assert!(merge_policies(&[], InlinePolicy::default()).is_empty());
    }

    #[test]
    fn selector_matching() {
        let sel: NetSelector = serde_json::from_value(json!({
            "nodePools": ["untrusted", "edge-*"], "runtimes": ["sandbox"], "jobs": ["agent-*"],
            "labels": { "tier": "free" }
        }))
        .unwrap();
        let mut facts = NetSelectorFacts {
            node_pool: Some("edge-eu".into()),
            namespace: String::new(),
            runtime: "sandbox".into(),
            job: "agent-42".into(),
            labels: [("tier".to_string(), "free".to_string())]
                .into_iter()
                .collect(),
        };
        assert!(sel.matches(&facts));
        facts.job = "web".into();
        assert!(!sel.matches(&facts));
        assert!(
            NetSelector::default().matches(&facts),
            "empty selector matches everything"
        );
        let ns: NetSelector = serde_json::from_value(json!({ "namespaces": ["default"] })).unwrap();
        assert!(
            ns.matches(&facts),
            "jobs live in default until namespaces land"
        );
    }

    #[test]
    fn default_tie_detection() {
        let a = policy("a", "default", 5, json!({ "allow": ["x"] }));
        let mut b = policy("b", "default", 5, json!({ "allow": ["y"] }));
        assert!(find_default_tie(&b, std::slice::from_ref(&a)).is_some());
        b.selector.node_pools = vec!["gpu".into()];
        let mut a2 = a.clone();
        a2.selector.node_pools = vec!["cpu".into()];
        assert!(find_default_tie(&b, std::slice::from_ref(&a2)).is_none());
        b.priority = 6;
        assert!(find_default_tie(&b, std::slice::from_ref(&a)).is_none());
    }

    #[test]
    fn network_mode_values() {
        for ok in ["auto", "host", "gated", "GATED"] {
            validate_network_mode(ok).unwrap();
        }
        let err = validate_network_mode("bridge").unwrap_err().to_string();
        assert!(err.contains("datapath plugin"), "{err}");
        assert!(validate_network_mode("wat").is_err());
    }

    #[test]
    fn durations_and_bytes() {
        assert_eq!(parse_duration("500ms").unwrap().as_millis(), 500);
        assert_eq!(parse_duration("2s").unwrap().as_secs(), 2);
        assert_eq!(parse_duration("5m").unwrap().as_secs(), 300);
        assert!(parse_duration("soon").is_err());
        assert_eq!(parse_bytes(&json!("1Mi")).unwrap(), 1024 * 1024);
        assert_eq!(parse_bytes(&json!(4096)).unwrap(), 4096);
        assert!(parse_bytes(&json!("1zb")).is_err());
    }

    #[test]
    fn policy_object_validation() {
        let mut p: NetPolicyObject = serde_json::from_value(json!({ "name": "x" })).unwrap();
        assert!(p.normalize_and_validate().is_err(), "nothing to apply");
        let mut p: NetPolicyObject =
            serde_json::from_value(json!({ "name": "bad name", "egress": { "allow": ["a"] } }))
                .unwrap();
        assert!(p.normalize_and_validate().is_err());
        let mut p: NetPolicyObject = serde_json::from_value(
            json!({ "name": "ok", "enforcement": "Require", "mode": "gated", "egress": { "allow": ["a"] } }),
        )
        .unwrap();
        p.normalize_and_validate().unwrap();
        assert!(p.is_required());
        assert_eq!(p.mode.as_deref(), Some("gated"));
    }

    #[test]
    fn effective_chain_reports_connect_only() {
        assert!(eff(json!({ "allow": ["a.example"] })).is_connect_only());
        assert!(!eff(json!({ "protocol": "http", "allow": ["a.example/x"] })).is_connect_only());
    }

    /// Every policy object shipped under `configs/policies/` evaluates and
    /// passes the same validation `legion net policy apply` runs, and the
    /// agent sandbox one stays opt-in (`default`, sandboxes only).
    #[test]
    fn shipped_policy_objects_validate() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("configs/policies");
        let mut seen = Vec::new();
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("ncl") {
                continue;
            }
            let raw = crate::config_lang::eval_file(&path).unwrap();
            let mut object: NetPolicyObject = serde_json::from_str(&raw)
                .unwrap_or_else(|err| panic!("{}: {err}", path.display()));
            object
                .normalize_and_validate()
                .unwrap_or_else(|err| panic!("{}: {err}", path.display()));
            seen.push(object);
        }
        let agent = seen
            .iter()
            .find(|object| object.name == "agent-sandbox-egress")
            .expect("agent-sandbox-egress.ncl ships");
        assert_eq!(agent.enforcement, ENFORCEMENT_DEFAULT);
        assert_eq!(agent.selector.runtimes, vec!["sandbox".to_string()]);
        assert!(
            agent.mode.is_none(),
            "it opens hosts, it does not impose a mode"
        );
    }
}
