//! The scaling plugins an operator installed in the autoscaler's app config,
//! and the rules for who may use them (autoscaler-v2 plan §6.8; calls
//! entries 4 and 5).
//!
//! ```nickel
//! # legion app config set autoscaler -f autoscaler.ncl
//! {
//!   plugins = {
//!     team-prom = {
//!       builtin = "prometheus",
//!       config = { address = "http://prometheus.internal:9090" },
//!       namespaces = ["payments-*"],     # only policies in these namespaces
//!     },
//!     queue-depth = { builtin = "http", config = { url = "http://ingest.internal:8080/stats" } },
//!     render-farm = { builtin = "exec", config = { command = ["/opt/legion/render-farm.sh"] } },
//!     calendar = {
//!       source = "https://example.com/calendar-strategy-0.1.0.tar.gz",
//!       checksum = "sha256:…",
//!       command = ["python3", "calendar_strategy.py"],
//!     },
//!     forecast = { url = "http://forecast.internal:8080/rpc" },
//!   },
//! }
//! ```
//!
//! A policy names a plugin and never carries a command or a URL: a tenant
//! supplies data (a query, a target value, a config record), the operator
//! decides what runs and which namespaces may use it. This module is pure:
//! it parses the catalog and answers "may this policy use that plugin as
//! this kind?". The manager asks at submit and apply time, so a policy that
//! names an uninstalled plugin, or one outside its namespace allowlist, is
//! refused there; the app starts the plugins from the same catalog.
//!
//! Two plugins need no install: the `legion` source (Legion's own
//! measurements) and the `noop` target (decisions and events only).

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::strategies::BUILT_IN_STRATEGIES;

/// The source every policy may use without an install.
pub const LEGION_SOURCE: &str = super::defaults::LEGION_SOURCE;

/// The target every standalone policy may use without an install.
pub const NOOP_TARGET: &str = "noop";

/// The protocol name external plugins speak (`initialize`'s `protocol`).
pub const PROTOCOL: &str = "legion-scaling/1";

/// The longest plugin name.
pub const MAX_NAME_LEN: usize = 63;

/// What a plugin is used as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// A metric source: `source/query`.
    Source,
    /// A strategy: `strategy/recommend`.
    Strategy,
    /// A target: `target/status`, `target/scale`, `target/terminate`.
    Target,
}

impl Kind {
    /// Every kind, in protocol order.
    pub const ALL: [Kind; 3] = [Kind::Source, Kind::Strategy, Kind::Target];

    /// The kind's protocol name.
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Source => "source",
            Kind::Strategy => "strategy",
            Kind::Target => "target",
        }
    }

    /// Parse a protocol name.
    pub fn parse(text: &str) -> Option<Kind> {
        match text {
            "source" => Some(Kind::Source),
            "strategy" => Some(Kind::Strategy),
            "target" => Some(Kind::Target),
            _ => None,
        }
    }

    fn article(self) -> &'static str {
        match self {
            Kind::Source => "a source",
            Kind::Strategy => "a strategy",
            Kind::Target => "a target",
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A plugin built into the autoscaler, installed by `builtin = "<name>"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Builtin {
    /// A Prometheus server: `config = { address }`. A check's `query` is
    /// PromQL; with a `window`, a range query averaged over it.
    Prometheus,
    /// A JSON document over HTTP GET: `config = { url, headers }`. A check's
    /// `query` is a JSON pointer into it (empty: the whole document is the
    /// number).
    Http,
    /// One-shot commands for a target: `config = { command }`. Run once per
    /// operation with the operation as its first argument, a JSON request on
    /// stdin and a JSON reply on stdout (the volume-driver style).
    Exec,
    /// A target that changes nothing: it records decisions and events.
    Noop,
}

impl Builtin {
    /// Every builtin, in docs order.
    pub const ALL: [Builtin; 4] = [
        Builtin::Prometheus,
        Builtin::Http,
        Builtin::Exec,
        Builtin::Noop,
    ];

    /// Its name as `builtin = "…"` writes it.
    pub fn as_str(self) -> &'static str {
        match self {
            Builtin::Prometheus => "prometheus",
            Builtin::Http => "http",
            Builtin::Exec => "exec",
            Builtin::Noop => "noop",
        }
    }

    /// Parse a `builtin` value.
    pub fn parse(text: &str) -> Option<Builtin> {
        Builtin::ALL
            .into_iter()
            .find(|builtin| builtin.as_str() == text)
    }

    /// The kind it implements.
    pub fn kind(self) -> Kind {
        match self {
            Builtin::Prometheus | Builtin::Http => Kind::Source,
            Builtin::Exec | Builtin::Noop => Kind::Target,
        }
    }

    /// Check the operator's `config` for this builtin.
    fn validate_config(self, config: &Value) -> Result<(), String> {
        let object = match config {
            Value::Null => None,
            Value::Object(object) => Some(object),
            other => return Err(format!("config must be a record, got {other}")),
        };
        let get = |key: &str| object.and_then(|object| object.get(key));
        let allowed: &[&str] = match self {
            Builtin::Prometheus => &["address", "headers", "timeout"],
            Builtin::Http => &["url", "headers", "timeout"],
            Builtin::Exec => &["command", "timeout"],
            Builtin::Noop => &[],
        };
        if let Some(object) = object {
            if let Some(unknown) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
                return Err(if allowed.is_empty() {
                    format!("the {self} builtin takes no config (found `{unknown}`)")
                } else {
                    format!(
                        "unknown config field `{unknown}` for the {self} builtin; it takes {}",
                        allowed.join(", ")
                    )
                });
            }
        }
        let url_field = |key: &str| -> Result<(), String> {
            let text = get(key).and_then(Value::as_str).ok_or_else(|| {
                format!("the {self} builtin needs config.{key}, an http:// or https:// URL")
            })?;
            check_http_url(text).map_err(|err| format!("config.{key}: {err}"))
        };
        match self {
            Builtin::Prometheus => url_field("address")?,
            Builtin::Http => url_field("url")?,
            Builtin::Exec => {
                let command = get("command").ok_or_else(|| {
                    "the exec builtin needs config.command: the program to run, as a string or \
                     a list (`[\"/opt/legion/scale.sh\"]`)"
                        .to_string()
                })?;
                command_argv(command).map_err(|err| format!("config.command: {err}"))?;
            }
            Builtin::Noop => {}
        }
        if let Some(headers) = get("headers") {
            let ok = headers
                .as_object()
                .is_some_and(|headers| headers.values().all(Value::is_string));
            if !ok {
                return Err("config.headers must be a record of strings".to_string());
            }
        }
        if let Some(timeout) = get("timeout") {
            let text = timeout
                .as_str()
                .ok_or_else(|| "config.timeout must be a duration like \"5s\"".to_string())?;
            let parsed = humantime::parse_duration(text.trim())
                .map_err(|err| format!("config.timeout {text:?}: {err}"))?;
            if parsed.is_zero() {
                return Err("config.timeout must be longer than 0s".to_string());
            }
        }
        Ok(())
    }
}

impl fmt::Display for Builtin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An `http://` or `https://` URL with a host.
pub fn check_http_url(text: &str) -> Result<(), String> {
    let parsed = url::Url::parse(text.trim()).map_err(|err| format!("{text:?} is not a URL: {err}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(format!("{text:?} must be an http:// or https:// URL"));
    }
    if parsed.host_str().is_none_or(str::is_empty) {
        return Err(format!("{text:?} has no host"));
    }
    Ok(())
}

/// A command as config writes it: a string (one program, no arguments) or a
/// non-empty list of strings.
pub fn command_argv(value: &Value) -> Result<Vec<String>, String> {
    match value {
        Value::String(program) if !program.trim().is_empty() => Ok(vec![program.clone()]),
        Value::Array(items) if !items.is_empty() => items
            .iter()
            .map(|item| {
                item.as_str()
                    .filter(|text| !text.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| "every element must be a non-empty string".to_string())
            })
            .collect(),
        _ => Err("write a program path or a non-empty list of strings".to_string()),
    }
}

/// How an installed plugin runs.
#[derive(Debug, Clone, PartialEq)]
pub enum Install {
    /// Built into the autoscaler.
    Builtin(Builtin),
    /// A program the app starts and keeps running, speaking JSON-RPC on its
    /// stdin and stdout. With `source`, the artifact is fetched into the app
    /// task's `code/plugins/<name>/`, and relative paths in `argv` resolve
    /// there; without `command`, the program is `./<name>` in that
    /// directory.
    Command {
        /// The program and its arguments.
        argv: Vec<String>,
        /// The artifact the program comes from, when it is fetched.
        source: Option<String>,
        /// The artifact's checksum (`sha256:<hex>`).
        checksum: Option<String>,
    },
    /// A plugin served elsewhere: each JSON-RPC request is an HTTP POST.
    Url(String),
}

/// One installed plugin.
#[derive(Debug, Clone, PartialEq)]
pub struct PluginEntry {
    /// Its name, which policies use.
    pub name: String,
    /// How it runs.
    pub install: Install,
    /// The operator's settings, sent with `initialize` (credentials
    /// references, addresses, regions).
    pub config: Value,
    /// Namespaces (names or `*`-globs) whose policies may use it; empty
    /// means every namespace.
    pub namespaces: Vec<String>,
}

impl PluginEntry {
    /// Whether policies in `namespace` may use it.
    pub fn allows(&self, namespace: &str) -> bool {
        self.namespaces.is_empty()
            || self
                .namespaces
                .iter()
                .any(|pattern| crate::structs::acl::namespace_glob_match(pattern, namespace))
    }

    /// The kinds it implements, when known before it starts: a builtin's
    /// kind. An external plugin declares its kinds in `initialize`, so this
    /// is `None` and the app checks the use when it starts the plugin.
    pub fn kinds(&self) -> Option<Vec<Kind>> {
        match &self.install {
            Install::Builtin(builtin) => Some(vec![builtin.kind()]),
            _ => None,
        }
    }

    /// `builtin prometheus`, `command`, `url http://…`, for messages.
    pub fn describe(&self) -> String {
        match &self.install {
            Install::Builtin(builtin) => format!("builtin {builtin}"),
            Install::Command {
                source: Some(_), ..
            } => "an artifact".to_string(),
            Install::Command { .. } => "a command".to_string(),
            Install::Url(url) => format!("served at {url}"),
        }
    }
}

/// The plugins installed in the autoscaler's app config.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PluginCatalog {
    entries: BTreeMap<String, PluginEntry>,
}

/// Names a plugin may not take: the implicit plugins and the built-in
/// strategies, which policies name the same way.
fn reserved(name: &str) -> bool {
    name == LEGION_SOURCE || name == NOOP_TARGET || BUILT_IN_STRATEGIES.contains(&name)
}

/// Whether `name` is a valid plugin name: lowercase letters, digits and
/// `-`, starting with a letter, at most [`MAX_NAME_LEN`].
pub fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && name.len() <= MAX_NAME_LEN
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

const ENTRY_FIELDS: [&str; 7] = [
    "builtin",
    "source",
    "checksum",
    "command",
    "url",
    "config",
    "namespaces",
];

fn parse_entry(name: &str, value: &Value) -> Result<PluginEntry, String> {
    let at = format!("plugins.{name}");
    if !valid_name(name) {
        return Err(format!(
            "{at}: a plugin name is lowercase letters, digits and `-`, starting with a letter, \
             at most {MAX_NAME_LEN} characters"
        ));
    }
    if reserved(name) {
        return Err(format!(
            "{at}: `{name}` is built in (the `legion` source, the `noop` target, and the \
             built-in strategies need no install). Hint: pick another name"
        ));
    }
    let object = value
        .as_object()
        .ok_or_else(|| format!("{at} must be a record"))?;
    if let Some(unknown) = object.keys().find(|key| !ENTRY_FIELDS.contains(&key.as_str())) {
        return Err(format!(
            "{at}: unknown field `{unknown}`; a plugin takes {}",
            ENTRY_FIELDS.join(", ")
        ));
    }
    let text = |key: &str| -> Result<Option<String>, String> {
        match object.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(text)) if !text.trim().is_empty() => Ok(Some(text.trim().to_string())),
            Some(_) => Err(format!("{at}.{key} must be a non-empty string")),
        }
    };
    let builtin = text("builtin")?;
    let source = text("source")?;
    let checksum = text("checksum")?;
    let url = text("url")?;
    let command = match object.get("command") {
        None | Some(Value::Null) => None,
        Some(value) => Some(command_argv(value).map_err(|err| format!("{at}.command: {err}"))?),
    };
    let config = object.get("config").cloned().unwrap_or(Value::Null);
    if !matches!(config, Value::Null | Value::Object(_)) {
        return Err(format!("{at}.config must be a record"));
    }
    let namespaces = match object.get("namespaces") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| format!("{at}.namespaces must list namespace names or globs"))
            })
            .collect::<Result<_, _>>()?,
        Some(_) => return Err(format!("{at}.namespaces must be a list")),
    };
    let modes = [
        builtin.is_some(),
        source.is_some() || command.is_some(),
        url.is_some(),
    ]
    .into_iter()
    .filter(|set| *set)
    .count();
    if modes != 1 {
        return Err(format!(
            "{at} must set exactly one of builtin (a plugin built into the autoscaler), source \
             or command (a program the autoscaler runs), or url (a plugin served over HTTP)"
        ));
    }
    if checksum.is_some() && source.is_none() {
        return Err(format!("{at}.checksum needs source: it checks the fetched artifact"));
    }
    if let Some(checksum) = &checksum {
        let ok = checksum
            .strip_prefix("sha256:")
            .is_some_and(|hex| hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()));
        if !ok {
            return Err(format!(
                "{at}.checksum must be `sha256:` and 64 hex digits, got {checksum:?}"
            ));
        }
    }
    let install = if let Some(builtin) = builtin {
        let parsed = Builtin::parse(&builtin).ok_or_else(|| {
            format!(
                "{at}.builtin: unknown builtin {builtin:?}; the builtins are {}",
                Builtin::ALL.map(Builtin::as_str).join(", ")
            )
        })?;
        parsed
            .validate_config(&config)
            .map_err(|err| format!("{at}: {err}"))?;
        Install::Builtin(parsed)
    } else if let Some(url) = url {
        check_http_url(&url).map_err(|err| format!("{at}.url: {err}"))?;
        Install::Url(url)
    } else {
        let argv = command.unwrap_or_else(|| vec![format!("./{name}")]);
        Install::Command {
            argv,
            source,
            checksum,
        }
    };
    Ok(PluginEntry {
        name: name.to_string(),
        install,
        config,
        namespaces,
    })
}

impl PluginCatalog {
    /// Parse `plugins` from the autoscaler's app config. Missing or empty
    /// `plugins` is an empty catalog.
    pub fn from_app_config(config: &Value) -> Result<Self, String> {
        let plugins = match config.get("plugins") {
            None | Some(Value::Null) => return Ok(Self::default()),
            Some(Value::Object(plugins)) => plugins,
            Some(_) => return Err("plugins must be a record of plugins by name".to_string()),
        };
        let mut entries = BTreeMap::new();
        for (name, value) in plugins {
            entries.insert(name.clone(), parse_entry(name, value)?);
        }
        Ok(Self { entries })
    }

    /// A catalog of the given entries (tests, and callers that build one).
    pub fn from_entries(entries: impl IntoIterator<Item = PluginEntry>) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|entry| (entry.name.clone(), entry))
                .collect(),
        }
    }

    /// The installed plugin `name`.
    pub fn get(&self, name: &str) -> Option<&PluginEntry> {
        self.entries.get(name)
    }

    /// Every installed plugin, by name.
    pub fn entries(&self) -> impl Iterator<Item = &PluginEntry> {
        self.entries.values()
    }

    /// Whether nothing is installed.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Check that a policy in `namespace` may use plugin `name` as `kind`.
    /// `what` names the use in the error, the way the policy wrote it:
    /// `check 'lag' uses source 'team-prom'`.
    ///
    /// The implicit plugins pass (`legion` as a source, `noop` as a
    /// target). An installed plugin must allow the namespace, and a builtin
    /// must be of the kind asked for; an external plugin's kinds are checked
    /// when the app starts it.
    pub fn check_use(&self, name: &str, kind: Kind, namespace: &str, what: &str) -> Result<(), String> {
        if (kind == Kind::Source && name == LEGION_SOURCE)
            || (kind == Kind::Target && name == NOOP_TARGET)
        {
            return Ok(());
        }
        let Some(entry) = self.entries.get(name) else {
            let installed: Vec<&str> = self
                .entries
                .values()
                .filter(|entry| entry.kinds().is_none_or(|kinds| kinds.contains(&kind)))
                .filter(|entry| entry.allows(namespace))
                .map(|entry| entry.name.as_str())
                .collect();
            let implicit = match kind {
                Kind::Source => "the `legion` source needs no install",
                Kind::Strategy => "the built-in strategies need no install",
                Kind::Target => "the `noop` target needs no install",
            };
            let listing = if installed.is_empty() {
                format!("no {} plugin is installed for namespace {namespace}", kind.as_str())
            } else {
                format!("installed for namespace {namespace}: {}", installed.join(", "))
            };
            return Err(format!(
                "{what}, which is not installed. Hint: the operator installs plugins in the \
                 autoscaler's app config (`legion app config set autoscaler`); {listing}; {implicit}"
            ));
        };
        if !entry.allows(namespace) {
            return Err(format!(
                "{what}, which is not available to namespace {namespace} (allowed: {})",
                entry.namespaces.join(", ")
            ));
        }
        if let Some(kinds) = entry.kinds() {
            if !kinds.contains(&kind) {
                return Err(format!(
                    "{what}, but '{name}' is {} ({}), not {}",
                    kinds
                        .first()
                        .map(|kind| kind.article())
                        .unwrap_or("a plugin"),
                    entry.describe(),
                    kind.article()
                ));
            }
        }
        Ok(())
    }

    /// A short digest of the catalog, so a policy stream notices an install
    /// or a removal.
    pub fn fingerprint(&self) -> String {
        self.entries
            .values()
            .map(|entry| {
                format!(
                    "{}={:?}:{}:{}",
                    entry.name,
                    entry.install,
                    entry.config,
                    entry.namespaces.join(",")
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn catalog(plugins: Value) -> Result<PluginCatalog, String> {
        PluginCatalog::from_app_config(&json!({ "plugins": plugins }))
    }

    #[test]
    fn catalog_parses_every_install_mode_test() -> Result<(), String> {
        let catalog = catalog(json!({
            "team-prom": {
                "builtin": "prometheus",
                "config": { "address": "http://prometheus.internal:9090" },
                "namespaces": ["payments-*"],
            },
            "depth": { "builtin": "http", "config": { "url": "https://ingest.internal/stats" } },
            "render": { "builtin": "exec", "config": { "command": "/opt/scale.sh" } },
            "quiet": { "builtin": "noop" },
            "calendar": {
                "source": "https://example.com/calendar.tar.gz",
                "checksum": format!("sha256:{}", "a".repeat(64)),
                "command": ["python3", "calendar.py"],
            },
            "asg": { "source": "https://example.com/asg.tar.gz" },
            "local": { "command": ["/usr/local/bin/forecast"] },
            "remote": { "url": "http://forecast.internal:8080/rpc" },
        }))?;
        assert_eq!(catalog.entries().count(), 8);
        assert_eq!(
            catalog.get("team-prom").map(|e| e.install.clone()),
            Some(Install::Builtin(Builtin::Prometheus))
        );
        match &catalog.get("asg").map(|e| e.install.clone()) {
            Some(Install::Command { argv, source, .. }) => {
                assert_eq!(argv, &vec!["./asg".to_string()]);
                assert!(source.is_some());
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            catalog.get("remote").map(|e| e.install.clone()),
            Some(Install::Url("http://forecast.internal:8080/rpc".into()))
        );
        assert!(PluginCatalog::from_app_config(&json!({}))?.is_empty());
        assert!(PluginCatalog::from_app_config(&json!({ "plugins": {} }))?.is_empty());
        Ok(())
    }

    #[test]
    fn catalog_refuses_bad_entries_with_named_errors_test() {
        let cases: Vec<(Value, &str)> = vec![
            (json!({ "Bad_Name": { "builtin": "noop" } }), "lowercase letters"),
            (json!({ "legion": { "builtin": "noop" } }), "is built in"),
            (json!({ "per-replica": { "url": "http://x/rpc" } }), "is built in"),
            (json!({ "p": {} }), "exactly one of builtin"),
            (
                json!({ "p": { "builtin": "noop", "url": "http://x/rpc" } }),
                "exactly one of builtin",
            ),
            (json!({ "p": { "builtin": "graphite" } }), "unknown builtin"),
            (json!({ "p": { "builtin": "prometheus" } }), "needs config.address"),
            (
                json!({ "p": { "builtin": "prometheus", "config": { "address": "ftp://x" } } }),
                "http:// or https://",
            ),
            (
                json!({ "p": { "builtin": "http", "config": { "url": "http://x", "method": "POST" } } }),
                "unknown config field `method`",
            ),
            (json!({ "p": { "builtin": "exec" } }), "needs config.command"),
            (
                json!({ "p": { "builtin": "exec", "config": { "command": [] } } }),
                "config.command",
            ),
            (json!({ "p": { "builtin": "noop", "config": { "x": 1 } } }), "takes no config"),
            (json!({ "p": { "url": "not a url" } }), "is not a URL"),
            (json!({ "p": { "command": "" } }), "command"),
            (
                json!({ "p": { "command": ["x"], "checksum": "sha256:00" } }),
                "checksum needs source",
            ),
            (
                json!({ "p": { "source": "https://x/a.tgz", "checksum": "md5:00" } }),
                "must be `sha256:`",
            ),
            (json!({ "p": { "builtin": "noop", "colour": "red" } }), "unknown field `colour`"),
            (json!({ "p": { "builtin": "noop", "namespaces": "a" } }), "must be a list"),
            (
                json!({ "p": { "builtin": "http", "config": { "url": "http://x", "timeout": "soon" } } }),
                "config.timeout",
            ),
        ];
        for (plugins, expected) in cases {
            let err = catalog(plugins.clone()).unwrap_err();
            assert!(err.contains(expected), "{plugins}: {err}");
        }
        assert!(PluginCatalog::from_app_config(&json!({ "plugins": [] })).is_err());
    }

    #[test]
    fn check_use_enforces_install_allowlist_and_kind_test() -> Result<(), String> {
        let catalog = catalog(json!({
            "team-prom": {
                "builtin": "prometheus",
                "config": { "address": "http://prometheus.internal:9090" },
                "namespaces": ["analytics-*"],
            },
            "render": { "builtin": "exec", "config": { "command": "/opt/scale.sh" } },
            "calendar": { "command": ["python3", "calendar.py"] },
        }))?;
        let what = "check 'lag' uses source 'team-prom'";
        // The implicit plugins.
        catalog.check_use("legion", Kind::Source, "web", "x")?;
        catalog.check_use("noop", Kind::Target, "web", "x")?;
        // Allowed namespace.
        catalog.check_use("team-prom", Kind::Source, "analytics-eu", what)?;
        // Outside the allowlist: the documented message.
        let err = catalog
            .check_use("team-prom", Kind::Source, "payments-prod", what)
            .unwrap_err();
        assert_eq!(
            err,
            "check 'lag' uses source 'team-prom', which is not available to namespace \
             payments-prod (allowed: analytics-*)"
        );
        // Not installed.
        let err = catalog
            .check_use("nope", Kind::Source, "web", "check 'x' uses source 'nope'")
            .unwrap_err();
        assert!(err.contains("which is not installed"), "{err}");
        assert!(err.contains("legion app config set autoscaler"), "{err}");
        // A builtin of the wrong kind.
        let err = catalog
            .check_use("render", Kind::Source, "web", "check 'x' uses source 'render'")
            .unwrap_err();
        assert!(err.contains("is a target (builtin exec), not a source"), "{err}");
        // An external plugin's kinds are only known when it starts.
        catalog.check_use("calendar", Kind::Strategy, "web", "x")?;
        catalog.check_use("calendar", Kind::Target, "web", "x")?;
        Ok(())
    }

    #[test]
    fn names_and_fingerprints_test() -> Result<(), String> {
        assert!(valid_name("aws-asg"));
        assert!(!valid_name("1abc"));
        assert!(!valid_name("a_b"));
        assert!(!valid_name(&"a".repeat(64)));
        let one = catalog(json!({ "q": { "builtin": "noop" } }))?;
        let two = catalog(json!({ "q": { "builtin": "noop", "namespaces": ["a"] } }))?;
        assert_ne!(one.fingerprint(), two.fingerprint());
        Ok(())
    }
}
