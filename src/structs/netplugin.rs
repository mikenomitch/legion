//! Network plugin objects: a WASM module that runs inside the node datapath
//! as one more handler, on the proxy-wasm ABI (docs/network-plugins.md).
//!
//! A plugin is stored like a policy object and referenced by name from a
//! chain or an upstream's handlers:
//!
//! ```nickel
//! { plugin = { name = "llm-token-meter", config = { budget = 100000 } } }
//! ```
//!
//! What the module is, which stages it runs at, who may reference it, and
//! its limits live on the object ([`NetPluginObject`]), so a reference
//! carries only the plugin's name and its per-reference `config`, which is
//! checked against the object's `configSchema` when the referencing policy
//! or job is submitted.

use crate::prelude::*;
use crate::structs::hops::Stage;
use crate::structs::netpolicy::{glob_matches, parse_bytes, parse_duration};
use crate::structs::source::{Source, SourceKind};

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// `usableBy`: only stored policy objects may reference the plugin.
pub const USABLE_BY_POLICIES: &str = "policies";
/// `usableBy`: jobs may reference the plugin too, from their inline
/// `network.egress` / `network.ingress` chains and their upstreams' handlers.
pub const USABLE_BY_JOBS: &str = "jobs";
pub const USABLE_BY: &[&str] = &[USABLE_BY_POLICIES, USABLE_BY_JOBS];

/// Linear memory a module gets when `limits.memory` names none.
pub const DEFAULT_PLUGIN_MEMORY: &str = "16Mi";
/// The most `limits.memory` may grant.
pub const MAX_PLUGIN_MEMORY: u64 = 256 * 1024 * 1024;
/// How long one callback may run when `limits.deadline` names none.
pub const DEFAULT_PLUGIN_DEADLINE: &str = "10ms";
/// The longest `limits.deadline` may be.
pub const MAX_PLUGIN_DEADLINE: &str = "1s";
/// The most of a request or response body a `body` / `response` plugin
/// buffers when it asks to see a whole body, unless `limits.maxBodyBytes`
/// says otherwise.
pub const DEFAULT_PLUGIN_BODY_BYTES: u64 = 1024 * 1024;
/// The most `limits.maxBodyBytes` may be.
pub const MAX_PLUGIN_BODY_BYTES: u64 = 64 * 1024 * 1024;
/// Fuel (roughly one unit per executed WASM instruction) one callback gets
/// per millisecond of its deadline. A loop that never returns stops when the
/// fuel runs out. wasmi runs about 2.4 million units per millisecond on a
/// 2.1 GHz Xeon core, so a runaway stops within about twice its deadline
/// there, and near the deadline on a core twice as fast; a callback that
/// returns is also held to the deadline by the clock.
pub const PLUGIN_FUEL_PER_MS: u64 = 5_000_000;
/// Most `targets` and `credentials` a plugin may declare, each.
pub const MAX_PLUGIN_DECLARATIONS: usize = 16;

/// A stored plugin object, as `legion net plugin apply` writes it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NetPluginObject {
    pub name: String,
    /// Free-form, shown by `describe`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub module: PluginModule,
    /// The stages the module acts at: `connect`, `request`, `body`,
    /// `response`, `done`.
    pub stages: Vec<String>,
    /// `policies` (default) or `jobs`.
    #[serde(default = "default_usable_by")]
    pub usable_by: String,
    /// Node pool globs the plugin may run on; empty is every pool. Applying a
    /// pool-scoped plugin needs `net:write` on those pools; a cluster-wide
    /// one needs unscoped `net:write`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub node_pools: Vec<String>,
    #[serde(default)]
    pub limits: PluginLimits,
    /// A JSON Schema every reference's `config` must satisfy, checked when
    /// the referencing policy or job is submitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_schema: Option<Value>,
    /// Named HTTP targets the module may call with `dispatch_http_call`
    /// (`http(s)://host[:port]`). The call is judged by the calling
    /// allocation's egress policy like any other flow.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub targets: BTreeMap<String, String>,
    /// Named signing keys the module may sign with (`legion_sign`) and never
    /// read. KV only; applying the plugin needs `kv:read` on each path.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub credentials: BTreeMap<String, Source>,
}

fn default_usable_by() -> String {
    USABLE_BY_POLICIES.to_string()
}

/// Where the module comes from. Fetched by the node's artifact fetchers,
/// checked against `checksum`, and kept in the node content store.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginModule {
    /// `https://`, `http://`, `s3://`, or `file://` URL of one `.wasm` file.
    pub source: String,
    /// `sha256:<64 hex>` of the module bytes. Required: a plugin always runs
    /// the exact module that was applied.
    #[serde(default)]
    pub checksum: String,
}

/// What one plugin may use.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginLimits {
    /// The most linear memory the module may grow to (`16Mi` by default, at
    /// most 256 MiB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Value>,
    /// How long one callback may run (`10ms` by default, at most `1s`). A
    /// callback past it is a handler error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<String>,
    /// How much of a body the module may hold when it buffers one (`1Mi`
    /// by default, at most 64 MiB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_body_bytes: Option<Value>,
}

impl PluginLimits {
    pub fn memory_bytes(&self) -> u64 {
        self.memory
            .as_ref()
            .and_then(|m| parse_bytes(m).ok())
            .unwrap_or(16 * 1024 * 1024)
    }

    pub fn deadline(&self) -> std::time::Duration {
        self.deadline
            .as_deref()
            .and_then(|d| parse_duration(d).ok())
            .unwrap_or_else(|| parse_duration(DEFAULT_PLUGIN_DEADLINE).expect("constant"))
    }

    pub fn max_body_bytes(&self) -> u64 {
        self.max_body_bytes
            .as_ref()
            .and_then(|m| parse_bytes(m).ok())
            .unwrap_or(DEFAULT_PLUGIN_BODY_BYTES)
    }

    /// Fuel for one callback: [`PLUGIN_FUEL_PER_MS`] per millisecond of the
    /// deadline, at least one millisecond's worth.
    pub fn fuel(&self) -> u64 {
        let ms = self.deadline().as_secs_f64() * 1000.0;
        (ms.max(1.0) * PLUGIN_FUEL_PER_MS as f64) as u64
    }
}

/// A stage name as written, to the stage.
pub fn parse_stage(name: &str) -> Option<Stage> {
    Some(match name {
        "connect" => Stage::Connect,
        "request" => Stage::Request,
        "body" => Stage::Body,
        "response" => Stage::Response,
        "done" => Stage::Done,
        _ => return None,
    })
}

pub const PLUGIN_STAGES: &[&str] = &["connect", "request", "body", "response", "done"];

impl NetPluginObject {
    /// The declared stages, parsed. Call after [`Self::normalize_and_validate`].
    pub fn stage_set(&self) -> Vec<Stage> {
        self.stages.iter().filter_map(|s| parse_stage(s)).collect()
    }

    pub fn has_stage(&self, stage: Stage) -> bool {
        self.stage_set().contains(&stage)
    }

    pub fn usable_by_jobs(&self) -> bool {
        self.usable_by == USABLE_BY_JOBS
    }

    /// Whether the plugin may run on a node of `pool`.
    pub fn runs_on_pool(&self, pool: &str) -> bool {
        self.node_pools.is_empty() || self.node_pools.iter().any(|g| glob_matches(g, pool))
    }

    /// Normalize and validate every field. Called at `apply`.
    pub fn normalize_and_validate(&mut self) -> R<()> {
        self.name = self.name.trim().to_string();
        if self.name.is_empty() {
            bail!("net plugin name must not be empty");
        }
        if !self
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        {
            bail!(
                "plugin '{}': name may contain only letters, digits, '-', '_' and '.'",
                self.name
            );
        }
        let what = format!("plugin '{}'", self.name);

        self.module.source = self.module.source.trim().to_string();
        self.module.checksum = self.module.checksum.trim().to_ascii_lowercase();
        if self.module.source.is_empty() {
            bail!("{what}: module.source is required (an https://, s3://, or file:// URL of a .wasm file)");
        }
        let parsed = crate::structs::artifact::parse_artifact_source(&self.module.source)
            .map_err(|err| anyhow!("{what}: module.source: {err:#}"))?;
        if !["http", "https", "s3", "file"].contains(&parsed.scheme.as_str()) {
            bail!(
                "{what}: module.source scheme '{}' cannot serve a module; use https://, http://, \
                 s3://, or file:// (one .wasm file)",
                parsed.scheme
            );
        }
        if self.module.checksum.is_empty() {
            bail!(
                "{what}: module.checksum is required (\"sha256:<hex>\" of the .wasm file; \
                 `sha256sum module.wasm` prints it)"
            );
        }
        crate::structs::artifact::validate_artifact_checksum(&self.module.checksum)
            .map_err(|err| anyhow!("{what}: module.checksum: {err:#}"))?;

        if self.stages.is_empty() {
            bail!(
                "{what}: stages must name at least one of {}",
                PLUGIN_STAGES.join(", ")
            );
        }
        let mut stages: Vec<String> = Vec::new();
        for stage in &self.stages {
            let stage = stage.trim().to_ascii_lowercase();
            if parse_stage(&stage).is_none() {
                bail!(
                    "{what}: unknown stage '{stage}'; stages are {}",
                    PLUGIN_STAGES.join(", ")
                );
            }
            if !stages.contains(&stage) {
                stages.push(stage);
            }
        }
        // Registry order, so the stored object and `show` read the same way.
        stages.sort_by_key(|s| PLUGIN_STAGES.iter().position(|p| p == s));
        self.stages = stages;

        self.usable_by = self.usable_by.trim().to_ascii_lowercase();
        if !USABLE_BY.contains(&self.usable_by.as_str()) {
            bail!(
                "{what}: usableBy must be one of {}, got '{}'",
                USABLE_BY.join(", "),
                self.usable_by
            );
        }
        for glob in &mut self.node_pools {
            *glob = glob.trim().to_string();
            if glob.is_empty() {
                bail!("{what}: nodePools entries must not be empty");
            }
        }

        if let Some(memory) = self.limits.memory.as_ref() {
            let bytes =
                parse_bytes(memory).map_err(|err| anyhow!("{what}.limits.memory: {err}"))?;
            if !(64 * 1024..=MAX_PLUGIN_MEMORY).contains(&bytes) {
                bail!("{what}.limits.memory must be between 64Ki and 256Mi");
            }
        } else {
            self.limits.memory = Some(Value::String(DEFAULT_PLUGIN_MEMORY.to_string()));
        }
        if let Some(deadline) = self.limits.deadline.as_deref() {
            let parsed =
                parse_duration(deadline).map_err(|err| anyhow!("{what}.limits.deadline: {err}"))?;
            if parsed.is_zero() || parsed > parse_duration(MAX_PLUGIN_DEADLINE).expect("constant") {
                bail!(
                    "{what}.limits.deadline must be more than 0 and at most {MAX_PLUGIN_DEADLINE}"
                );
            }
        } else {
            self.limits.deadline = Some(DEFAULT_PLUGIN_DEADLINE.to_string());
        }
        if let Some(max) = self.limits.max_body_bytes.as_ref() {
            let bytes =
                parse_bytes(max).map_err(|err| anyhow!("{what}.limits.maxBodyBytes: {err}"))?;
            if bytes == 0 || bytes > MAX_PLUGIN_BODY_BYTES {
                bail!("{what}.limits.maxBodyBytes must be more than 0 and at most 64Mi");
            }
        }

        if let Some(schema) = self.config_schema.as_ref() {
            if !schema.is_object() && !schema.is_boolean() {
                bail!("{what}.configSchema must be a JSON Schema object");
            }
            jsonschema::JSONSchema::compile(schema)
                .map_err(|err| anyhow!("{what}.configSchema is not a valid JSON Schema: {err}"))?;
        }

        if self.targets.len() > MAX_PLUGIN_DECLARATIONS
            || self.credentials.len() > MAX_PLUGIN_DECLARATIONS
        {
            bail!("{what}: at most {MAX_PLUGIN_DECLARATIONS} targets and {MAX_PLUGIN_DECLARATIONS} credentials");
        }
        let mut targets = BTreeMap::new();
        for (name, url) in std::mem::take(&mut self.targets) {
            validate_declaration_name(&what, "targets", &name)?;
            let url = url.trim().trim_end_matches('/').to_string();
            let parsed =
                parse_target(&url).map_err(|err| anyhow!("{what}.targets.{name}: {err}"))?;
            if !parsed.path.is_empty() {
                bail!(
                    "{what}.targets.{name}: a target is a scheme and authority only \
                     (http(s)://host[:port]); the module sends its own :path"
                );
            }
            targets.insert(name, url);
        }
        self.targets = targets;
        for (name, source) in self.credentials.iter_mut() {
            validate_declaration_name(&what, "credentials", name)?;
            source.trim();
            source.check(&format!("{what}.credentials.{name}"), &[SourceKind::Kv])?;
        }
        Ok(())
    }

    /// Check one reference's `config` against `configSchema`. A plugin with
    /// no schema takes any config.
    pub fn check_config(&self, what: &str, config: Option<&Value>) -> R<()> {
        let Some(schema) = self.config_schema.as_ref() else {
            return Ok(());
        };
        let compiled = jsonschema::JSONSchema::compile(schema)
            .map_err(|err| anyhow!("plugin '{}' has an invalid configSchema: {err}", self.name))?;
        let config = config.cloned().unwrap_or(Value::Null);
        let result = compiled.validate(&config);
        if let Err(errors) = result {
            let details = errors
                .map(|err| {
                    let at = err.instance_path.to_string();
                    if at.is_empty() {
                        err.to_string()
                    } else {
                        format!("{at}: {err}")
                    }
                })
                .collect::<Vec<_>>()
                .join("; ");
            bail!(
                "{what}: config does not match plugin '{}' configSchema: {details}",
                self.name
            );
        }
        Ok(())
    }
}

fn validate_declaration_name(what: &str, field: &str, name: &str) -> R<()> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        bail!("{what}.{field}: name '{name}' may contain only letters, digits, '-' and '_'");
    }
    Ok(())
}

/// A `targets` URL, split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginTarget {
    pub tls: bool,
    pub host: String,
    pub port: u16,
    /// Anything after the authority (empty for a valid target).
    pub path: String,
}

impl PluginTarget {
    pub fn authority(&self) -> String {
        let default = if self.tls { 443 } else { 80 };
        if self.port == default {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

pub fn parse_target(url: &str) -> R<PluginTarget> {
    let (tls, rest) = if let Some(rest) = url.strip_prefix("https://") {
        (true, rest)
    } else if let Some(rest) = url.strip_prefix("http://") {
        (false, rest)
    } else {
        bail!("'{url}' must start with http:// or https://");
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    if authority.is_empty() || authority.contains('@') {
        bail!("'{url}' has no host");
    }
    let default = if tls { 443 } else { 80 };
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) if !host.ends_with(']') || authority.starts_with('[') => {
            let port: u16 = port
                .parse()
                .map_err(|_| anyhow!("'{url}' has an invalid port"))?;
            (host.trim_matches(['[', ']']).to_string(), port)
        }
        _ => (authority.to_string(), default),
    };
    Ok(PluginTarget {
        tls,
        host,
        port,
        path: path.trim_end_matches('/').to_string(),
    })
}

/// `plugin` hop: run a stored plugin object.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginConfig {
    /// The plugin object's name.
    pub name: String,
    /// Handed to the module as its plugin configuration (JSON), checked
    /// against the object's `configSchema` at submit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
}

impl PluginConfig {
    /// The configuration bytes the module reads with
    /// `get_plugin_configuration`: the JSON text of `config`, or nothing.
    pub fn configuration_bytes(&self) -> Vec<u8> {
        match &self.config {
            None | Some(Value::Null) => Vec::new(),
            Some(value) => serde_json::to_vec(value).unwrap_or_default(),
        }
    }
}

/// The plugin a hop runs, if it is a `plugin` hop.
pub fn hop_plugin(hop: &crate::structs::hops::HopConfig) -> Option<&PluginConfig> {
    match &hop.kind {
        crate::structs::hops::HandlerKind::Plugin(plugin) => Some(plugin),
        _ => None,
    }
}

/// Every plugin name an effective policy's chains reference.
pub fn policy_plugin_names(policy: &crate::structs::netpolicy::EffectivePolicy) -> Vec<String> {
    [policy.egress.as_ref(), policy.ingress.as_ref()]
        .into_iter()
        .flatten()
        .flat_map(|chain| chain.handlers.iter())
        .filter_map(|hop| hop_plugin(&hop.handler).map(|p| p.name.clone()))
        .collect()
}

/// Every plugin name a connect block's upstreams reference.
pub fn connect_plugin_names(connect: &crate::structs::ConnectConfig) -> Vec<String> {
    connect
        .upstreams
        .iter()
        .flat_map(|upstream| upstream.handlers.iter())
        .filter_map(|hop| hop_plugin(hop).map(|p| p.name.clone()))
        .collect()
}

/// Where a reference to a plugin was written, for the submit-time checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginUse {
    /// A stored policy object's chain.
    Policy,
    /// A job's inline chain or an upstream's own handlers.
    Job,
}

/// Check one `plugin` hop against its stored object: it exists, the
/// referencing side may use it, the path can run at least one of its stages,
/// and the config matches the schema. `is_http` says whether the path parses
/// HTTP; `where_http` explains the path for the error.
pub fn check_plugin_reference(
    what: &str,
    hop: &PluginConfig,
    object: Option<&NetPluginObject>,
    used_by: PluginUse,
    is_http: bool,
    where_http: &str,
) -> R<()> {
    let Some(object) = object else {
        bail!(
            "{what}: no net plugin named '{}' (legion net plugin list shows them; apply one with \
             legion net plugin apply -f <file>)",
            hop.name
        );
    };
    if used_by == PluginUse::Job && !object.usable_by_jobs() {
        bail!(
            "{what}: plugin '{}' is usableBy = \"policies\"; a job may reference only a plugin \
             with usableBy = \"jobs\" (or apply a policy object that uses it)",
            object.name
        );
    }
    let stages = object.stage_set();
    let runs = if is_http {
        stages.iter().any(|stage| stage.needs_http())
    } else {
        stages.contains(&Stage::Connect)
    };
    if !runs {
        bail!(
            "{what}: plugin '{}' acts at stage(s) {}, none of which run here: {where_http}",
            object.name,
            object.stages.join(", ")
        );
    }
    object.check_config(what, hop.config.as_ref())
}

#[cfg(test)]
mod netplugin_test {
    use super::*;

    fn object(value: Value) -> R<NetPluginObject> {
        let mut object: NetPluginObject = serde_json::from_value(value)?;
        object.normalize_and_validate()?;
        Ok(object)
    }

    fn sample() -> Value {
        json!({
            "name": "meter",
            "module": {
                "source": "https://example.com/meter.wasm",
                "checksum": format!("sha256:{}", "a".repeat(64)),
            },
            "stages": ["done", "request", "request"],
            "configSchema": {
                "type": "object",
                "required": ["budget"],
                "properties": { "budget": { "type": "integer" } },
            },
        })
    }

    /// Every plugin object shipped under `configs/netplugins/` evaluates and
    /// passes the validation `legion net plugin apply` runs, and its checksum
    /// is the module the tests build.
    #[test]
    fn shipped_plugin_objects_validate_test() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut seen = 0;
        for entry in std::fs::read_dir(root.join("configs/netplugins")).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("ncl") {
                continue;
            }
            let raw = crate::config_lang::eval_file(&path).unwrap();
            let mut object: NetPluginObject = serde_json::from_str(&raw)
                .unwrap_or_else(|err| panic!("{}: {err}", path.display()));
            object
                .normalize_and_validate()
                .unwrap_or_else(|err| panic!("{}: {err}", path.display()));
            let module = root.join(format!("tests/netplugins/{}.wasm", object.name));
            if let Ok(bytes) = std::fs::read(&module) {
                crate::worker::netd::plugins::verify_checksum(&bytes, &object.module.checksum)
                    .unwrap_or_else(|err| panic!("{}: {err}", path.display()));
            }
            seen += 1;
        }
        assert!(seen >= 1);
    }

    #[test]
    fn a_plugin_normalizes_its_defaults_and_stage_order_test() {
        let plugin = object(sample()).unwrap();
        assert_eq!(plugin.stages, vec!["request", "done"]);
        assert_eq!(plugin.usable_by, USABLE_BY_POLICIES);
        assert_eq!(
            plugin.limits.deadline.as_deref(),
            Some(DEFAULT_PLUGIN_DEADLINE)
        );
        assert_eq!(plugin.limits.memory_bytes(), 16 * 1024 * 1024);
        assert_eq!(plugin.limits.fuel(), 10 * PLUGIN_FUEL_PER_MS);
        assert!(plugin.runs_on_pool("anything"));
    }

    #[test]
    fn a_module_must_be_pinned_and_fetchable_test() {
        let mut no_sum = sample();
        no_sum["module"]["checksum"] = json!("");
        assert!(object(no_sum)
            .unwrap_err()
            .to_string()
            .contains("checksum is required"));
        let mut oci = sample();
        oci["module"]["source"] = json!("oci://ghcr.io/acme/meter:1");
        assert!(object(oci)
            .unwrap_err()
            .to_string()
            .contains("cannot serve a module"));
        let mut stage = sample();
        stage["stages"] = json!(["headers"]);
        assert!(object(stage)
            .unwrap_err()
            .to_string()
            .contains("unknown stage 'headers'"));
        let mut unknown = sample();
        unknown["kind"] = json!("wasm");
        assert!(object(unknown).is_err());
    }

    #[test]
    fn targets_and_credentials_are_declared_narrowly_test() {
        let mut with = sample();
        with["targets"] = json!({ "quota": "http://quota.internal:8080/" });
        with["credentials"] = json!({ "aws": { "kv": "secrets/aws" } });
        let plugin = object(with).unwrap();
        assert_eq!(plugin.targets["quota"], "http://quota.internal:8080");
        let mut path = sample();
        path["targets"] = json!({ "quota": "http://quota.internal/v1" });
        assert!(object(path)
            .unwrap_err()
            .to_string()
            .contains("scheme and authority"));
        let mut file = sample();
        file["credentials"] = json!({ "k": { "file": "/etc/key" } });
        assert!(object(file).is_err());
        let target = parse_target("https://api.example.com").unwrap();
        assert_eq!(
            (target.port, target.authority()),
            (443, "api.example.com".into())
        );
    }

    #[test]
    fn references_are_checked_against_the_object_test() {
        let plugin = object(sample()).unwrap();
        let good = PluginConfig {
            name: "meter".into(),
            config: Some(json!({ "budget": 5 })),
        };
        check_plugin_reference("h", &good, Some(&plugin), PluginUse::Policy, true, "").unwrap();
        let bad = PluginConfig {
            name: "meter".into(),
            config: Some(json!({ "budget": "lots" })),
        };
        let err = check_plugin_reference("h", &bad, Some(&plugin), PluginUse::Policy, true, "")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("configSchema") && err.contains("/budget"),
            "{err}"
        );
        let err = check_plugin_reference(
            "h",
            &good,
            Some(&plugin),
            PluginUse::Policy,
            false,
            "egress.protocol is \"tcp\"",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("none of which run here"), "{err}");
        let err = check_plugin_reference("h", &good, Some(&plugin), PluginUse::Job, true, "")
            .unwrap_err()
            .to_string();
        assert!(err.contains("usableBy"), "{err}");
        let err = check_plugin_reference("h", &good, None, PluginUse::Policy, true, "")
            .unwrap_err()
            .to_string();
        assert!(err.contains("no net plugin named 'meter'"), "{err}");
    }
}
