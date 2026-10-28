//! The plugin host: every plugin the autoscaler's app config installs, ready
//! to call (docs/autoscaling.md, "Plugins"; autoscaler-v2 plan §6.8).
//!
//! Builtins run in process ([`super::builtins`]); external plugins are
//! [`Transport`]s, started on first use and kept running. Before a call
//! carries a policy's config to a plugin, the host checks the plugin
//! implements the kind the policy uses it as, and that the config matches
//! the plugin's `policyConfigSchema`: a plugin never sees a config its
//! schema refuses.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use super::builtins::{ExecTarget, HttpSource, NoopTarget, PrometheusSource};
use super::rpc::{CallError, Endpoint, PluginInfo, Transport};
use crate::autoscaler::plugins::{Builtin, Install, Kind, PluginCatalog, PluginEntry, NOOP_TARGET};

/// What a source answered.
#[derive(Debug, Clone, PartialEq)]
pub enum SourceReading {
    /// A value. `windowed` when the source already averaged it over the
    /// check's window (a Prometheus range, a plugin's series); otherwise the
    /// app averages its own readings over the window, as for the `legion`
    /// source.
    Value {
        /// The reading.
        value: f64,
        /// Whether it already covers the window.
        windowed: bool,
    },
    /// No data, and why.
    Missing(String),
}

impl SourceReading {
    /// A value.
    pub fn value(value: f64, windowed: bool) -> Self {
        SourceReading::Value { value, windowed }
    }
}

/// What a target holds now.
#[derive(Debug, Clone, PartialEq)]
pub struct TargetState {
    /// Its count.
    pub count: u32,
    /// How many of them are ready.
    pub ready: u32,
    /// Its members, as the plugin described them.
    pub members: Vec<Value>,
}

/// One `source/query`.
#[derive(Debug, Clone)]
pub struct SourceCall<'a> {
    /// The policy's label (`web/default/web`, or a standalone policy's id).
    pub policy: &'a str,
    /// The check.
    pub check: &'a str,
    /// The policy's query.
    pub query: &'a str,
    /// The check's window.
    pub window: Duration,
}

/// One `strategy/recommend`.
#[derive(Debug, Clone)]
pub struct RecommendCall<'a> {
    /// The policy's label.
    pub policy: &'a str,
    /// The check.
    pub check: &'a str,
    /// The target's count.
    pub current: u32,
    /// Its ready replicas.
    pub ready: u32,
    /// The check's reading, when it has a query.
    pub reading: Option<f64>,
    /// The strategy's settings (every field but `name`).
    pub config: Value,
}

enum Loaded {
    External(Transport),
    Prometheus(PrometheusSource),
    Http(HttpSource),
    Exec(ExecTarget),
    Noop(NoopTarget),
}

/// Every installed plugin, plus the implicit `noop` target.
pub struct PluginHost {
    catalog: PluginCatalog,
    plugins: HashMap<String, Loaded>,
    noop: NoopTarget,
    schemas: std::sync::Mutex<HashMap<String, Arc<jsonschema::JSONSchema>>>,
    timeout_override: Option<Duration>,
}

/// Where an artifact plugin's files are: `<code root>/plugins/<name>/`, the
/// directory the app reconciler fetches its `source` into.
pub fn plugin_dir(code_root: &Path, name: &str) -> PathBuf {
    code_root.join("plugins").join(name)
}

/// The endpoint of an external plugin. An artifact plugin runs in its
/// directory, and a relative program path (`./aws-asg`) resolves there.
pub fn endpoint_for(entry: &PluginEntry, code_root: Option<&Path>) -> Option<Endpoint> {
    match &entry.install {
        Install::Url(url) => Some(Endpoint::Url(url.clone())),
        Install::Command { argv, source, .. } => {
            let dir = match (source, code_root) {
                (Some(_), Some(root)) => Some(plugin_dir(root, &entry.name)),
                _ => None,
            };
            let mut argv = argv.clone();
            if let (Some(dir), Some(program)) = (dir.as_ref(), argv.first_mut()) {
                let path = Path::new(program.as_str());
                if path.is_relative() && program.contains('/') {
                    *program = dir.join(path).display().to_string();
                }
            }
            Some(Endpoint::Command { argv, dir })
        }
        Install::Builtin(_) => None,
    }
}

fn call_error(plugin: &str, method: &str, err: CallError) -> String {
    format!("plugin {plugin}: {method}: {err}")
}

/// Why a policy cannot use a plugin.
#[derive(Debug, Clone, PartialEq)]
pub enum Problem {
    /// The policy is wrong for the plugin: it uses it as a kind it does not
    /// implement, or its config does not match the plugin's
    /// `policyConfigSchema`. The policy is held `invalid` until fixed.
    Invalid(String),
    /// The plugin could not be asked (not installed here, not starting, not
    /// answering): its checks have no data, its target reports `error`.
    Unavailable(String),
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Problem::Invalid(why) | Problem::Unavailable(why) => f.write_str(why),
        }
    }
}

impl PluginHost {
    /// A host for `catalog`. `code_root` is where artifact plugins were
    /// fetched (`$LEGION_TASK_DIR/code`). Nothing starts until it is used.
    pub fn new(catalog: PluginCatalog, code_root: Option<&Path>) -> Self {
        Self::build(catalog, code_root, None)
    }

    /// The same, with one timeout for every plugin call (tests).
    pub fn with_timeout(catalog: PluginCatalog, code_root: Option<&Path>, timeout: Duration) -> Self {
        Self::build(catalog, code_root, Some(timeout))
    }

    fn build(catalog: PluginCatalog, code_root: Option<&Path>, timeout: Option<Duration>) -> Self {
        let client = reqwest::Client::new();
        let mut plugins = HashMap::new();
        for entry in catalog.entries() {
            let loaded = match &entry.install {
                Install::Builtin(Builtin::Prometheus) => {
                    Loaded::Prometheus(PrometheusSource::new(&entry.config, client.clone()))
                }
                Install::Builtin(Builtin::Http) => {
                    Loaded::Http(HttpSource::new(&entry.config, client.clone()))
                }
                Install::Builtin(Builtin::Exec) => Loaded::Exec(ExecTarget::new(&entry.config)),
                Install::Builtin(Builtin::Noop) => Loaded::Noop(NoopTarget::default()),
                _ => {
                    let Some(endpoint) = endpoint_for(entry, code_root) else {
                        continue;
                    };
                    let config = if entry.config.is_null() {
                        json!({})
                    } else {
                        entry.config.clone()
                    };
                    let mut transport = Transport::new(&entry.name, endpoint, config);
                    if let Some(timeout) = timeout {
                        transport = transport.with_timeout(timeout);
                    }
                    Loaded::External(transport)
                }
            };
            plugins.insert(entry.name.clone(), loaded);
        }
        Self {
            catalog,
            plugins,
            noop: NoopTarget::default(),
            schemas: std::sync::Mutex::new(HashMap::new()),
            timeout_override: timeout,
        }
    }

    /// The catalog the host was built from.
    pub fn catalog(&self) -> &PluginCatalog {
        &self.catalog
    }

    fn loaded(&self, name: &str) -> Result<&Loaded, String> {
        self.plugins.get(name).ok_or_else(|| {
            format!(
                "plugin {name} is not installed in the autoscaler's app config \
                 (`legion app config set autoscaler`)"
            )
        })
    }

    /// An external plugin's `initialize` answer, starting it if needed.
    pub async fn info(&self, name: &str) -> Result<PluginInfo, String> {
        match self.loaded(name)? {
            Loaded::External(transport) => transport
                .info()
                .await
                .map_err(|err| call_error(name, "initialize", err)),
            _ => Err(format!("plugin {name} is built in")),
        }
    }

    /// The external plugin `name`'s transport (the conformance suite).
    pub fn transport(&self, name: &str) -> Option<&Transport> {
        match self.plugins.get(name) {
            Some(Loaded::External(transport)) => Some(transport),
            _ => None,
        }
    }

    fn compiled(&self, name: &str, schema: &Value) -> Result<Arc<jsonschema::JSONSchema>, String> {
        let key = format!("{name}\u{0}{schema}");
        let mut cache = self.schemas.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(compiled) = cache.get(&key) {
            return Ok(compiled.clone());
        }
        let compiled = Arc::new(
            crate::structs::app::compile_config_schema(schema)
                .map_err(|err| format!("plugin {name}'s policyConfigSchema {err}"))?,
        );
        cache.insert(key, compiled.clone());
        Ok(compiled)
    }

    /// Check that plugin `name` implements `kind`, and that a policy's
    /// `config` for it matches its `policyConfigSchema`. Builtins check
    /// their kind; their policy-level config is free-form.
    pub async fn check(&self, name: &str, kind: Kind, config: &Value) -> Result<(), Problem> {
        if kind == Kind::Target && name == NOOP_TARGET {
            return Ok(());
        }
        match self.loaded(name).map_err(Problem::Unavailable)? {
            Loaded::External(transport) => {
                let info = transport
                    .info()
                    .await
                    .map_err(|err| Problem::Unavailable(call_error(name, "initialize", err)))?;
                if !info.kinds.contains(&kind) {
                    return Err(Problem::Invalid(format!(
                        "plugin {name} is not {} (it implements {})",
                        match kind {
                            Kind::Source => "a source",
                            Kind::Strategy => "a strategy",
                            Kind::Target => "a target",
                        },
                        info.kinds
                            .iter()
                            .map(|kind| kind.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
                // A source has no policy-level config; strategies and
                // targets do.
                if let (Some(schema), false) = (&info.policy_config_schema, kind == Kind::Source) {
                    let compiled = self.compiled(name, schema).map_err(Problem::Invalid)?;
                    crate::structs::app::check_config(&compiled, config).map_err(|err| {
                        Problem::Invalid(format!(
                            "the config for plugin {name} does not match its \
                             policyConfigSchema: {err}"
                        ))
                    })?;
                }
                Ok(())
            }
            loaded => {
                let actual = match loaded {
                    Loaded::Prometheus(_) | Loaded::Http(_) => Kind::Source,
                    _ => Kind::Target,
                };
                if actual == kind {
                    Ok(())
                } else {
                    Err(Problem::Invalid(format!(
                        "plugin {name} is a built-in {actual}, not a {kind}"
                    )))
                }
            }
        }
    }

    /// Read one source.
    pub async fn query(&self, source: &str, call: &SourceCall<'_>, now_ms: i64) -> Result<SourceReading, String> {
        match self.loaded(source)? {
            Loaded::Prometheus(prometheus) => prometheus.query(call.query, call.window, now_ms).await,
            Loaded::Http(http) => http.query(call.query).await,
            Loaded::External(transport) => {
                self.check(source, Kind::Source, &Value::Null)
                    .await
                    .map_err(|problem| problem.to_string())?;
                let params = json!({
                    "policy": call.policy,
                    "check": call.check,
                    "query": call.query,
                    "window": crate::autoscaler::spec::format_duration(call.window),
                    "windowSeconds": call.window.as_secs(),
                });
                let result = transport
                    .call("source/query", params)
                    .await
                    .map_err(|err| call_error(source, "source/query", err))?;
                parse_reading(&result, call.window, now_ms)
                    .map_err(|err| format!("plugin {source}: source/query: {err}"))
            }
            _ => Err(format!("plugin {source} is a target, not a source")),
        }
    }

    /// Ask a strategy plugin for a desired count: `(desired, note)`.
    pub async fn recommend(&self, strategy: &str, call: &RecommendCall<'_>) -> Result<(u32, String), String> {
        let Loaded::External(transport) = self.loaded(strategy)? else {
            return Err(format!(
                "plugin {strategy} is built in, and the builtins are sources and targets"
            ));
        };
        self.check(strategy, Kind::Strategy, &call.config)
            .await
            .map_err(|problem| problem.to_string())?;
        let params = json!({
            "policy": call.policy,
            "check": call.check,
            "current": call.current,
            "ready": call.ready,
            "reading": call.reading,
            "config": call.config,
        });
        let result = transport
            .call("strategy/recommend", params)
            .await
            .map_err(|err| call_error(strategy, "strategy/recommend", err))?;
        parse_recommendation(&result).map_err(|err| format!("plugin {strategy}: strategy/recommend: {err}"))
    }

    /// What a target holds. `start` is the noop target's first count.
    pub async fn status(&self, target: &str, policy: &str, config: &Value, start: u32) -> Result<TargetState, String> {
        if target == NOOP_TARGET {
            return Ok(self.noop.status(policy, start));
        }
        match self.loaded(target)? {
            Loaded::Noop(noop) => Ok(noop.status(policy, start)),
            Loaded::Exec(exec) => {
                let reply = exec
                    .run("status", json!({ "policy": policy, "config": config }))
                    .await
                    .map_err(|err| format!("plugin {target}: status: {err}"))?;
                parse_status(&reply).map_err(|err| format!("plugin {target}: status: {err}"))
            }
            Loaded::External(transport) => {
                self.check(target, Kind::Target, config)
                    .await
                    .map_err(|problem| problem.to_string())?;
                let result = transport
                    .call("target/status", json!({ "policy": policy, "config": config }))
                    .await
                    .map_err(|err| call_error(target, "target/status", err))?;
                parse_status(&result).map_err(|err| format!("plugin {target}: target/status: {err}"))
            }
            _ => Err(format!("plugin {target} is a source, not a target")),
        }
    }

    /// Move a target to `desired`.
    pub async fn scale(&self, target: &str, policy: &str, config: &Value, desired: u32, decision: &str) -> Result<(), String> {
        if target == NOOP_TARGET {
            self.noop.scale(policy, desired);
            return Ok(());
        }
        let request = json!({ "policy": policy, "config": config, "desired": desired, "decision": decision });
        match self.loaded(target)? {
            Loaded::Noop(noop) => {
                noop.scale(policy, desired);
                Ok(())
            }
            Loaded::Exec(exec) => exec
                .run("scale", request)
                .await
                .map(|_| ())
                .map_err(|err| format!("plugin {target}: scale: {err}")),
            Loaded::External(transport) => {
                self.check(target, Kind::Target, config)
                    .await
                    .map_err(|problem| problem.to_string())?;
                transport
                    .call("target/scale", request)
                    .await
                    .map(|_| ())
                    .map_err(|err| call_error(target, "target/scale", err))
            }
            _ => Err(format!("plugin {target} is a source, not a target")),
        }
    }

    /// Whether every call uses one test timeout.
    pub fn timeout_override(&self) -> Option<Duration> {
        self.timeout_override
    }
}

/// A `source/query` result: `{ value }`, `{ series: [[at_ms, value], …] }`
/// (averaged over the window here), or `{ value: null, reason }`.
pub fn parse_reading(result: &Value, window: Duration, now_ms: i64) -> Result<SourceReading, String> {
    let object = result
        .as_object()
        .ok_or_else(|| format!("the result must be an object, got {result}"))?;
    if let Some(series) = object.get("series") {
        let points = series
            .as_array()
            .ok_or_else(|| "series must be a list of [unix ms, value] pairs".to_string())?;
        let since = now_ms - i64::try_from(window.as_millis()).unwrap_or(i64::MAX / 4);
        let mut values = Vec::new();
        let mut last: Option<(i64, f64)> = None;
        for point in points {
            let pair = point
                .as_array()
                .filter(|pair| pair.len() == 2)
                .ok_or_else(|| format!("series point {point} is not a [unix ms, value] pair"))?;
            let at = pair[0]
                .as_i64()
                .ok_or_else(|| format!("series point {point}: the time is not unix ms"))?;
            let value = super::builtins::number(&pair[1])
                .ok_or_else(|| format!("series point {point}: the value is not a number"))?;
            if at > since {
                values.push(value);
            }
            if last.is_none_or(|(when, _)| at >= when) {
                last = Some((at, value));
            }
        }
        if window.is_zero() {
            return Ok(match last {
                Some((_, value)) => SourceReading::value(value, true),
                None => SourceReading::Missing("the series is empty".into()),
            });
        }
        if values.is_empty() {
            return Ok(SourceReading::Missing("no series points inside the window".into()));
        }
        return Ok(SourceReading::value(
            values.iter().sum::<f64>() / values.len() as f64,
            true,
        ));
    }
    match object.get("value") {
        None => Err("the result needs value, series, or value: null with a reason".into()),
        Some(Value::Null) => Ok(SourceReading::Missing(
            object
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("the plugin has no data")
                .to_string(),
        )),
        Some(value) => super::builtins::number(value)
            .map(|value| SourceReading::value(value, true))
            .ok_or_else(|| format!("value {value} is not a finite number")),
    }
}

/// A `strategy/recommend` result: `{ desired, note }`.
pub fn parse_recommendation(result: &Value) -> Result<(u32, String), String> {
    let desired = result
        .get("desired")
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("the result needs `desired`, a replica count; got {result}"))?;
    let desired = u32::try_from(desired).map_err(|_| format!("desired {desired} is too large"))?;
    let note = result
        .get("note")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Ok((desired, note))
}

/// A `target/status` result: `{ count, ready, members }`.
pub fn parse_status(result: &Value) -> Result<TargetState, String> {
    let count = result
        .get("count")
        .and_then(Value::as_u64)
        .and_then(|count| u32::try_from(count).ok())
        .ok_or_else(|| format!("the result needs `count`, a replica count; got {result}"))?;
    let ready = match result.get("ready") {
        None | Some(Value::Null) => count,
        Some(value) => value
            .as_u64()
            .and_then(|ready| u32::try_from(ready).ok())
            .ok_or_else(|| format!("ready {value} is not a count"))?
            .min(count),
    };
    let members = match result.get("members") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(members)) => members.clone(),
        Some(other) => return Err(format!("members must be a list, got {other}")),
    };
    Ok(TargetState {
        count,
        ready,
        members,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readings_parse_values_series_and_missing_test() {
        let window = Duration::from_secs(60);
        let now = 1_000_000;
        assert_eq!(
            parse_reading(&json!({ "value": 2310 }), window, now),
            Ok(SourceReading::value(2310.0, true))
        );
        assert_eq!(
            parse_reading(
                &json!({ "series": [[now - 120_000, 100], [now - 30_000, 2], [now, 4]] }),
                window,
                now
            ),
            Ok(SourceReading::value(3.0, true))
        );
        assert_eq!(
            parse_reading(&json!({ "series": [[now - 120_000, 100]] }), window, now),
            Ok(SourceReading::Missing("no series points inside the window".into()))
        );
        assert_eq!(
            parse_reading(&json!({ "value": null, "reason": "no series" }), window, now),
            Ok(SourceReading::Missing("no series".into()))
        );
        assert!(parse_reading(&json!({}), window, now).is_err());
        assert!(parse_reading(&json!({ "value": "lots" }), window, now).is_err());
        assert!(parse_reading(&json!({ "series": [[1]] }), window, now).is_err());
    }

    #[test]
    fn statuses_and_recommendations_parse_test() {
        assert_eq!(
            parse_status(&json!({ "count": 7, "ready": 9 })).unwrap(),
            TargetState {
                count: 7,
                ready: 7,
                members: Vec::new()
            }
        );
        assert!(parse_status(&json!({ "ready": 1 })).is_err());
        assert!(parse_status(&json!({ "count": -1 })).is_err());
        assert_eq!(
            parse_recommendation(&json!({ "desired": 5, "note": "x" })),
            Ok((5, "x".to_string()))
        );
        assert!(parse_recommendation(&json!({ "desired": 2.5 })).is_err());
    }

    #[test]
    fn artifact_plugins_run_in_their_directory_test() {
        let catalog = PluginCatalog::from_app_config(&json!({ "plugins": {
            "asg": { "source": "https://example.com/asg.tar.gz" },
            "cal": { "source": "https://example.com/cal.tgz", "command": ["python3", "cal.py"] },
            "local": { "command": ["/usr/bin/forecast", "--serve"] },
        } }))
        .unwrap();
        let root = Path::new("/task/code");
        assert_eq!(
            endpoint_for(catalog.get("asg").unwrap(), Some(root)),
            Some(Endpoint::Command {
                argv: vec!["/task/code/plugins/asg/./asg".into()],
                dir: Some(PathBuf::from("/task/code/plugins/asg")),
            })
        );
        assert_eq!(
            endpoint_for(catalog.get("cal").unwrap(), Some(root)),
            Some(Endpoint::Command {
                argv: vec!["python3".into(), "cal.py".into()],
                dir: Some(PathBuf::from("/task/code/plugins/cal")),
            })
        );
        assert_eq!(
            endpoint_for(catalog.get("local").unwrap(), Some(root)),
            Some(Endpoint::Command {
                argv: vec!["/usr/bin/forecast".into(), "--serve".into()],
                dir: None,
            })
        );
    }
}
