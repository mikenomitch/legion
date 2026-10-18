//! Handler hops as written in a chain or on an upstream
//! (docs/network-handlers.md, "Handlers"), and the one registry of kinds.
//!
//! A hop is a keyed variant plus the settings every hop shares:
//!
//! ```nickel
//! { authz = { url = "http://policy.internal/check", cache = "30s" }, onError = "deny" }
//! { proxy = { service = "egress-filter", local = true } }
//! { dlp = { patterns = ["sk-[A-Za-z0-9]{16,}"], maxBytes = "1Mi" } }
//! ```
//!
//! [`HANDLERS`] is the only list of handler kinds. Validation, the datapath
//! (`worker::netd::handlers`), `net policy show`, and the manager's endpoint
//! feed all read it, so a kind cannot be accepted at submit and then not run.
//! Adding a kind is one variant here, one row in [`HANDLERS`], one runtime
//! implementation, a `configs/jobs/maximal.ncl` entry, and a row in the public
//! handler table; `hops_registry_parity_test` fails the build when one of
//! those is missing.

use crate::prelude::*;
use crate::structs::netpolicy::{parse_bytes, parse_duration, MAX_AUTHZ_CACHE, ON_ERRORS};

use serde::de::Error as _;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The timeout a hop gets when it names none.
pub const DEFAULT_HOP_TIMEOUT: &str = "2s";

/// Where in a flow a handler acts. A path runs a hop at every stage it can
/// see: an L4 path only at `connect`, an HTTP path at `request` (with the
/// request head) and `body`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    /// When the flow opens, with the connect-time facts.
    Connect,
    /// Per HTTP request, with the request head.
    Request,
    /// Per HTTP request, with the buffered request body.
    Body,
    /// Per HTTP response.
    Response,
    /// After the exchange.
    Done,
}

impl Stage {
    pub fn as_str(self) -> &'static str {
        match self {
            Stage::Connect => "connect",
            Stage::Request => "request",
            Stage::Body => "body",
            Stage::Response => "response",
            Stage::Done => "done",
        }
    }

    /// Whether the stage needs the path to parse HTTP.
    pub fn needs_http(self) -> bool {
        !matches!(self, Stage::Connect)
    }
}

/// Where a hop's work runs, as `show` and `doctor` print it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    /// In the kernel, at `connect()`.
    Kernel,
    /// In the node datapath, in-process.
    Netd,
    /// In the node datapath, inside a WASM sandbox.
    NetdWasm,
    /// Outside the node datapath: a webhook or another proxy.
    External,
}

impl Placement {
    pub fn as_str(self) -> &'static str {
        match self {
            Placement::Kernel => "kernel",
            Placement::Netd => "netd",
            Placement::NetdWasm => "netd (wasm)",
            Placement::External => "external",
        }
    }
}

/// One handler kind in the registry.
#[derive(Debug)]
pub struct HandlerEntry {
    /// The key it is written under (`{ authz = { ... } }`).
    pub name: &'static str,
    /// The stages it acts at. On a path that does not parse HTTP, a kind
    /// whose stages all need HTTP is a submit error.
    pub stages: &'static [Stage],
    pub placement: Placement,
    /// One line for `show` and the docs.
    pub summary: &'static str,
}

impl HandlerEntry {
    /// Whether the kind can do anything on a path that does not parse HTTP.
    pub fn runs_on_l4(&self) -> bool {
        self.stages.contains(&Stage::Connect)
    }

    /// Whether every stage it acts at needs HTTP.
    pub fn needs_http(&self) -> bool {
        self.stages.iter().all(|stage| stage.needs_http())
    }

    pub fn stage_names(&self) -> String {
        self.stages
            .iter()
            .map(|stage| stage.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// The only list of handler kinds.
pub const HANDLERS: &[HandlerEntry] = &[
    HandlerEntry {
        name: "authz",
        stages: &[Stage::Connect, Stage::Request],
        placement: Placement::External,
        summary: "POST the flow's facts to a webhook and follow its verdict",
    },
    HandlerEntry {
        name: "proxy",
        stages: &[Stage::Connect, Stage::Request],
        placement: Placement::External,
        summary: "hand the flow to another proxy, with attribution",
    },
    HandlerEntry {
        name: "dlp",
        stages: &[Stage::Body],
        placement: Placement::Netd,
        summary: "reject request bodies that match a pattern",
    },
];

/// The registry row for a kind name.
pub fn handler_entry(name: &str) -> Option<&'static HandlerEntry> {
    HANDLERS.iter().find(|entry| entry.name == name)
}

/// Every kind name, for error messages.
pub fn handler_names() -> Vec<&'static str> {
    HANDLERS.iter().map(|entry| entry.name).collect()
}

/// `authz`: a decision webhook.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthzConfig {
    /// `http(s)://` webhook URL.
    pub url: String,
    /// Verdict cache TTL (default 10s, at most [`MAX_AUTHZ_CACHE`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<String>,
}

/// `proxy`: hand the flow to another proxy. Exactly one of `service` or
/// `port`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProxyConfig {
    /// A catalog service: any healthy endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// With `service`: only its endpoints on this node (a per-node filter run
    /// as a system job).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub local: bool,
    /// A sidecar task's port label in the same job, on this node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<String>,
    /// Prepend a PROXY protocol v2 header on TCP hand-offs (default true).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_protocol: Option<bool>,
}

/// `dlp`: reject request bodies that match a pattern.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DlpConfig {
    /// Regular expressions; a body matching any is rejected.
    pub patterns: Vec<String>,
    /// How much of a body is buffered and read (`1Mi`, default 1 MiB). A body
    /// larger than this is rejected unless the hop says `onError = "allow"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<Value>,
}

/// A handler hop's kind and its own settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerKind {
    Authz(AuthzConfig),
    Proxy(ProxyConfig),
    Dlp(DlpConfig),
}

impl HandlerKind {
    pub fn name(&self) -> &'static str {
        match self {
            HandlerKind::Authz(_) => "authz",
            HandlerKind::Proxy(_) => "proxy",
            HandlerKind::Dlp(_) => "dlp",
        }
    }

    /// The registry row. Every variant has one (`hops_registry_parity_test`).
    pub fn entry(&self) -> &'static HandlerEntry {
        handler_entry(self.name()).expect("every handler kind is registered")
    }

    /// Catalog services this hop dials, for the manager's endpoint feed.
    pub fn endpoint_need(&self) -> Option<&str> {
        match self {
            HandlerKind::Proxy(proxy) => proxy.service.as_deref(),
            _ => None,
        }
    }

    /// What it points at, for `show` and status lines.
    pub fn target(&self) -> String {
        match self {
            HandlerKind::Authz(authz) => authz.url.clone(),
            HandlerKind::Proxy(proxy) => match (&proxy.service, &proxy.port) {
                (Some(service), _) if proxy.local => format!("service {service} (node-local)"),
                (Some(service), _) => format!("service {service}"),
                (None, Some(port)) => format!("port {port}"),
                (None, None) => String::new(),
            },
            HandlerKind::Dlp(dlp) => format!("{} pattern(s)", dlp.patterns.len()),
        }
    }

    fn to_value(&self) -> Value {
        let value = match self {
            HandlerKind::Authz(config) => serde_json::to_value(config),
            HandlerKind::Proxy(config) => serde_json::to_value(config),
            HandlerKind::Dlp(config) => serde_json::to_value(config),
        };
        value.unwrap_or(Value::Null)
    }

    fn from_value(name: &str, value: Value) -> Result<HandlerKind, String> {
        let typed = |err: serde_json::Error| format!("{name}: {err}");
        match name {
            "authz" => serde_json::from_value(value)
                .map(HandlerKind::Authz)
                .map_err(typed),
            "proxy" => serde_json::from_value(value)
                .map(HandlerKind::Proxy)
                .map_err(typed),
            "dlp" => serde_json::from_value(value)
                .map(HandlerKind::Dlp)
                .map_err(typed),
            other => Err(format!(
                "unknown handler kind '{other}'; handler kinds are {}",
                handler_names().join(", ")
            )),
        }
    }
}

/// One hop as written: its kind, plus what every hop shares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopConfig {
    pub kind: HandlerKind,
    /// Deadline for the hop (default [`DEFAULT_HOP_TIMEOUT`]).
    pub timeout: Option<String>,
    /// `deny` (default) or `allow`: what a failed hop means.
    pub on_error: Option<String>,
}

impl Serialize for HopConfig {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry(self.kind.name(), &self.kind.to_value())?;
        if let Some(timeout) = &self.timeout {
            map.serialize_entry("timeout", timeout)?;
        }
        if let Some(on_error) = &self.on_error {
            map.serialize_entry("onError", on_error)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for HopConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut map = Map::<String, Value>::deserialize(deserializer)?;
        let timeout = match map.remove("timeout") {
            None | Some(Value::Null) => None,
            Some(Value::String(timeout)) => Some(timeout),
            Some(other) => {
                return Err(D::Error::custom(format!(
                    "timeout must be a duration string, got {other}"
                )))
            }
        };
        let on_error = match map.remove("onError") {
            None | Some(Value::Null) => None,
            Some(Value::String(on_error)) => Some(on_error),
            Some(other) => {
                return Err(D::Error::custom(format!(
                    "onError must be \"deny\" or \"allow\", got {other}"
                )))
            }
        };
        if map.contains_key("handler") {
            return Err(D::Error::custom(
                "a handler is written as its kind, not `handler = \"...\"`: e.g. \
                 { authz = { url = \"...\" } } or { dlp = { patterns = [...] } }",
            ));
        }
        let mut entries = map.into_iter();
        let Some((name, value)) = entries.next() else {
            return Err(D::Error::custom(format!(
                "a handler names its kind: one of {}",
                handler_names().join(", ")
            )));
        };
        if let Some((extra, _)) = entries.next() {
            return Err(D::Error::custom(format!(
                "handler '{name}' has an unexpected field '{extra}' beside it; a handler is \
                 one kind ({}) plus timeout and onError, with the kind's settings inside it",
                handler_names().join(", ")
            )));
        }
        let kind = HandlerKind::from_value(&name, value).map_err(D::Error::custom)?;
        Ok(HopConfig {
            kind,
            timeout,
            on_error,
        })
    }
}

impl HopConfig {
    pub fn new(kind: HandlerKind) -> Self {
        HopConfig {
            kind,
            timeout: None,
            on_error: None,
        }
    }

    pub fn name(&self) -> &'static str {
        self.kind.name()
    }

    /// Whether a failure of this hop lets the flow through.
    pub fn fails_open(&self) -> bool {
        self.on_error.as_deref() == Some(crate::structs::netpolicy::ON_ERROR_ALLOW)
    }

    /// The hop's deadline.
    pub fn timeout(&self) -> std::time::Duration {
        self.timeout
            .as_deref()
            .and_then(|t| parse_duration(t).ok())
            .unwrap_or_else(|| parse_duration(DEFAULT_HOP_TIMEOUT).expect("constant"))
    }

    /// Normalize and validate one hop. Shared by chains and by
    /// `connect.upstreams[].handlers`, so a handler is spelled identically
    /// wherever it appears. `is_http` says whether the surrounding path
    /// parses HTTP; a kind that only acts on HTTP stages is refused on a path
    /// that does not.
    pub fn normalize(&mut self, what: &str, is_http: bool, where_http: &str) -> R<()> {
        let entry = self.kind.entry();
        if !is_http && entry.needs_http() {
            bail!(
                "{what} {} acts at stage \"{}\", which needs HTTP, but {where_http}",
                entry.name,
                entry.stage_names()
            );
        }
        match &mut self.kind {
            HandlerKind::Authz(authz) => {
                authz.url = authz.url.trim().to_string();
                if !(authz.url.starts_with("http://") || authz.url.starts_with("https://")) {
                    bail!("{what}: authz needs url = \"http(s)://...\"");
                }
                if let Some(cache) = authz.cache.as_deref() {
                    let ttl =
                        parse_duration(cache).map_err(|err| anyhow!("{what}.cache: {err}"))?;
                    let cap = parse_duration(MAX_AUTHZ_CACHE).expect("constant");
                    if ttl > cap {
                        bail!("{what}.cache must be at most {MAX_AUTHZ_CACHE}");
                    }
                }
            }
            HandlerKind::Proxy(proxy) => {
                let service = proxy
                    .service
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let port = proxy
                    .port
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                match (service, port) {
                    (Some(_), None) | (None, Some(_)) => {}
                    _ => bail!(
                        "{what}: proxy needs exactly one of service (a catalog service; add \
                         local = true for its endpoints on this node) or port (a sidecar's port \
                         label)"
                    ),
                }
                if proxy.local && service.is_none() {
                    bail!("{what}: proxy local = true applies to a service target");
                }
                proxy.service = service.map(str::to_string);
                proxy.port = port.map(str::to_string);
            }
            HandlerKind::Dlp(dlp) => {
                if let Some(max) = dlp.max_bytes.as_ref() {
                    parse_bytes(max).map_err(|err| anyhow!("{what}.maxBytes: {err}"))?;
                }
                if dlp.patterns.is_empty() {
                    bail!("{what}: dlp needs patterns = [\"<regex>\", ...]");
                }
                for pattern in &dlp.patterns {
                    regex::Regex::new(pattern)
                        .map_err(|err| anyhow!("{what}.patterns '{pattern}': {err}"))?;
                }
            }
        }
        if let Some(timeout) = self.timeout.as_deref() {
            parse_duration(timeout).map_err(|err| anyhow!("{what}.timeout: {err}"))?;
        } else {
            self.timeout = Some(DEFAULT_HOP_TIMEOUT.to_string());
        }
        if let Some(on_error) = self.on_error.as_mut() {
            *on_error = on_error.trim().to_ascii_lowercase();
            if !ON_ERRORS.contains(&on_error.as_str()) {
                bail!(
                    "{what}.onError must be one of {}, got '{on_error}'",
                    ON_ERRORS.join(", ")
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hop(value: Value) -> Result<HopConfig, String> {
        serde_json::from_value(value).map_err(|err| err.to_string())
    }

    #[test]
    fn hops_parse_as_keyed_variants_and_round_trip_test() {
        let parsed = hop(
            json!({ "authz": { "url": "http://x/check", "cache": "30s" }, "onError": "allow" }),
        )
        .unwrap();
        assert_eq!(parsed.name(), "authz");
        assert!(parsed.fails_open());
        let back = serde_json::to_value(&parsed).unwrap();
        assert_eq!(
            back,
            json!({ "authz": { "url": "http://x/check", "cache": "30s" }, "onError": "allow" })
        );
    }

    #[test]
    fn a_misplaced_field_names_the_variant_test() {
        let err = hop(json!({ "proxy": { "service": "f", "url": "http://x" } })).unwrap_err();
        assert!(err.contains("proxy") && err.contains("url"), "{err}");
        let err = hop(json!({ "proxy": { "service": "f" }, "url": "http://x" })).unwrap_err();
        assert!(err.contains("unexpected field 'url'"), "{err}");
        let err = hop(json!({ "handler": "authz", "url": "http://x" })).unwrap_err();
        assert!(err.contains("written as its kind"), "{err}");
        let err = hop(json!({ "log": {} })).unwrap_err();
        assert!(err.contains("unknown handler kind 'log'"), "{err}");
    }

    #[test]
    fn proxy_targets_are_one_of_service_or_port_test() {
        let mut both = hop(json!({ "proxy": { "service": "a", "port": "p" } })).unwrap();
        assert!(both.normalize("h", false, "").is_err());
        let mut local_port = hop(json!({ "proxy": { "port": "p", "local": true } })).unwrap();
        assert!(local_port.normalize("h", false, "").is_err());
        let mut ok = hop(json!({ "proxy": { "service": "a", "local": true } })).unwrap();
        ok.normalize("h", false, "").unwrap();
        assert_eq!(ok.kind.endpoint_need(), Some("a"));
        assert_eq!(ok.timeout.as_deref(), Some(DEFAULT_HOP_TIMEOUT));
    }

    #[test]
    fn a_body_stage_handler_is_refused_on_a_tcp_path_test() {
        let mut dlp = hop(json!({ "dlp": { "patterns": ["x"] } })).unwrap();
        let err = dlp
            .normalize("egress.handlers[0]", false, "egress.protocol is \"tcp\"")
            .unwrap_err()
            .to_string();
        assert!(err.contains("stage \"body\""), "{err}");
        assert!(err.contains("egress.protocol is \"tcp\""), "{err}");
        dlp.normalize("egress.handlers[0]", true, "").unwrap();
    }

    /// Every variant has a registry row, every row parses to a variant, and
    /// every kind is in `maximal.ncl` and in the public handler table.
    #[test]
    fn hops_registry_parity_test() {
        let samples = [
            json!({ "authz": { "url": "http://x" } }),
            json!({ "proxy": { "service": "x" } }),
            json!({ "dlp": { "patterns": ["x"] } }),
        ];
        let parsed: Vec<&'static str> = samples
            .iter()
            .map(|sample| hop(sample.clone()).unwrap().name())
            .collect();
        let registered: Vec<&'static str> = handler_names();
        assert_eq!(parsed, registered, "a sample per registered kind, in order");

        let maximal = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/jobs/maximal.ncl"
        ))
        .unwrap();
        let docs = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/docs/network-handlers.md"
        ))
        .unwrap();
        for entry in HANDLERS {
            assert!(
                maximal.contains(&format!("{} = {{", entry.name)),
                "configs/jobs/maximal.ncl has no `{} = {{ ... }}` handler",
                entry.name
            );
            assert!(
                docs.contains(&format!("| `{}` |", entry.name)),
                "docs/network-handlers.md's handler table has no `{}` row",
                entry.name
            );
        }
    }
}
