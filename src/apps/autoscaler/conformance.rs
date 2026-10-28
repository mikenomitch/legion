//! The plugin conformance suite behind `legion scaling plugin test`
//! (docs/autoscaling-plugins.md, "Testing a plugin").
//!
//! It drives a plugin exactly as the autoscaler does, through the same
//! [`Transport`], and checks what a plugin must get right:
//!
//! | Check | What passes |
//! | --- | --- |
//! | `handshake` | `initialize` answers with a name, a version, and known kinds; a `policyConfigSchema`, if any, compiles |
//! | `schema` | the policy config given (`--policy-config`) matches the schema, which refuses a config that is not a record |
//! | `source/query`, `strategy/recommend`, `target/status`, `target/scale`, `target/terminate` | each method the plugin's kinds declare answers in the documented shape. `target/scale` asks for the count `target/status` reported, so the test changes nothing |
//! | `unknown method` | a method the plugin does not have answers JSON-RPC error -32601, and the plugin keeps running |
//! | `malformed request` | parameters of the wrong type answer an error (ideally -32602), and the plugin keeps running |
//! | `timeouts` | every call answered within its protocol timeout (10s for sources and strategies, 120s for targets) |
//! | `crash and restart` | killed, a command plugin starts again, handshakes again, and answers (URL plugins: skipped) |

use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{json, Value};

use super::rpc::{timeout_for, CallError, Endpoint, PluginInfo, Transport};
use crate::autoscaler::plugins::Kind;

/// How one check came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    /// It holds.
    Pass,
    /// It does not: the plugin is not conformant.
    Fail,
    /// It holds, with a caveat worth fixing.
    Warn,
    /// Not applicable to this plugin.
    Skip,
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Outcome::Pass => "pass",
            Outcome::Fail => "FAIL",
            Outcome::Warn => "warn",
            Outcome::Skip => "skip",
        })
    }
}

/// One check's result.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckResult {
    /// The check.
    pub name: String,
    /// How it came out.
    pub outcome: Outcome,
    /// What was seen.
    pub detail: String,
    /// How long its calls took, in milliseconds.
    pub millis: u64,
}

/// The plugin as it described itself.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginSummary {
    /// Its name.
    pub name: String,
    /// Its version.
    pub version: String,
    /// The kinds it implements.
    pub kinds: Vec<String>,
    /// Whether its target manages nodes.
    pub nodes: bool,
    /// Whether it ships a policy-config schema.
    pub has_schema: bool,
}

/// The suite's report.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SuiteReport {
    /// What was tested (`` `python3 calendar.py` `` or a URL).
    pub plugin_ref: String,
    /// The plugin, when the handshake worked.
    pub plugin: Option<PluginSummary>,
    /// Every check, in order.
    pub checks: Vec<CheckResult>,
    /// No check failed.
    pub passed: bool,
}

/// What to run the suite with.
#[derive(Debug, Clone, Default)]
pub struct SuiteOptions {
    /// The plugin-level config `initialize` sends (what the operator would
    /// install it with).
    pub config: Value,
    /// The policy-level config sent to strategy and target methods (a
    /// strategy's settings, a target's `config`).
    pub policy_config: Value,
    /// The query `source/query` sends.
    pub query: String,
    /// One timeout for every call, instead of the protocol's.
    pub timeout: Option<Duration>,
}

struct Suite {
    transport: Transport,
    checks: Vec<CheckResult>,
    slowest: Option<(String, Duration, Duration)>,
}

impl Suite {
    fn record(&mut self, name: &str, outcome: Outcome, detail: impl Into<String>, took: Duration) {
        self.checks.push(CheckResult {
            name: name.to_string(),
            outcome,
            detail: detail.into(),
            millis: took.as_millis() as u64,
        });
    }

    async fn call(&mut self, method: &str, params: Value) -> (Result<Value, CallError>, Duration) {
        let started = Instant::now();
        let result = self.transport.call(method, params).await;
        let took = started.elapsed();
        let limit = timeout_for(method);
        if self
            .slowest
            .as_ref()
            .is_none_or(|(_, slowest, _)| took > *slowest)
        {
            self.slowest = Some((method.to_string(), took, limit));
        }
        (result, took)
    }
}

fn policy_config(options: &SuiteOptions) -> Value {
    if options.policy_config.is_null() {
        json!({})
    } else {
        options.policy_config.clone()
    }
}

/// Run the suite against `endpoint`.
pub async fn run(endpoint: Endpoint, options: SuiteOptions) -> SuiteReport {
    let plugin_ref = endpoint.to_string();
    let is_command = matches!(endpoint, Endpoint::Command { .. });
    let config = if options.config.is_null() {
        json!({})
    } else {
        options.config.clone()
    };
    let mut transport = Transport::new("conformance", endpoint, config);
    if let Some(timeout) = options.timeout {
        transport = transport.with_timeout(timeout);
    }
    let mut suite = Suite {
        transport,
        checks: Vec::new(),
        slowest: None,
    };

    // ── handshake ──────────────────────────────────────────────────────
    let started = Instant::now();
    let info: PluginInfo = match suite.transport.info().await {
        Ok(info) => {
            let took = started.elapsed();
            suite.slowest = Some(("initialize".into(), took, timeout_for("initialize")));
            suite.record(
                "handshake",
                Outcome::Pass,
                format!(
                    "{} {} implements {}",
                    info.name,
                    info.version,
                    info.kinds
                        .iter()
                        .map(|kind| kind.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                took,
            );
            info
        }
        Err(err) => {
            let tail = suite.transport.stderr_tail();
            let detail = if tail.is_empty() {
                err.to_string()
            } else {
                format!("{err}; stderr: {tail}")
            };
            suite.record("handshake", Outcome::Fail, detail, started.elapsed());
            return SuiteReport {
                plugin_ref,
                plugin: None,
                passed: false,
                checks: suite.checks,
            };
        }
    };
    let summary = PluginSummary {
        name: info.name.clone(),
        version: info.version.clone(),
        kinds: info.kinds.iter().map(|kind| kind.to_string()).collect(),
        nodes: info.nodes,
        has_schema: info.policy_config_schema.is_some(),
    };

    // ── schema ─────────────────────────────────────────────────────────
    let policy = policy_config(&options);
    match &info.policy_config_schema {
        None => suite.record(
            "schema",
            Outcome::Warn,
            "no policyConfigSchema: policy configs are not checked before they reach the plugin",
            Duration::ZERO,
        ),
        Some(schema) => match crate::structs::app::compile_config_schema(schema) {
            Err(err) => suite.record(
                "schema",
                Outcome::Fail,
                format!("policyConfigSchema {err}"),
                Duration::ZERO,
            ),
            Ok(compiled) => {
                if let Err(err) = crate::structs::app::check_config(&compiled, &policy) {
                    suite.record(
                        "schema",
                        Outcome::Fail,
                        format!(
                            "the policy config {policy} does not match the plugin's schema ({err}). \
                             Hint: pass the config a policy would use with --policy-config"
                        ),
                        Duration::ZERO,
                    );
                } else if compiled.is_valid(&json!(42)) {
                    suite.record(
                        "schema",
                        Outcome::Warn,
                        "the schema accepts a config that is not a record (42); give it type: object",
                        Duration::ZERO,
                    );
                } else {
                    suite.record(
                        "schema",
                        Outcome::Pass,
                        "the policy config matches; a config that is not a record is refused before it reaches the plugin",
                        Duration::ZERO,
                    );
                }
            }
        },
    }

    // ── each declared method ───────────────────────────────────────────
    // A method that should keep the plugin alive afterwards, for the
    // malformed-request and crash checks.
    let mut probe: Option<(&'static str, Value)> = None;
    for kind in &info.kinds {
        match kind {
            Kind::Source => {
                let params = json!({
                    "policy": "conformance", "check": "conformance", "query": options.query,
                    "window": "1m", "windowSeconds": 60,
                });
                let (result, took) = suite.call("source/query", params.clone()).await;
                match result.map_err(|err| err.to_string()).and_then(|value| {
                    super::host::parse_reading(
                        &value,
                        Duration::from_secs(60),
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .map(|reading| (value, reading))
                }) {
                    Ok((value, _)) => {
                        suite.record("source/query", Outcome::Pass, format!("{value}"), took);
                        probe.get_or_insert(("source/query", params));
                    }
                    Err(err) => suite.record("source/query", Outcome::Fail, err, took),
                }
            }
            Kind::Strategy => {
                let params = json!({
                    "policy": "conformance", "check": "conformance", "current": 4, "ready": 4,
                    "reading": 2310.0, "config": policy,
                });
                let (result, took) = suite.call("strategy/recommend", params.clone()).await;
                match result
                    .map_err(|err| err.to_string())
                    .and_then(|value| super::host::parse_recommendation(&value))
                {
                    Ok((desired, note)) => {
                        suite.record(
                            "strategy/recommend",
                            Outcome::Pass,
                            format!("at 4 replicas reading 2310 it wants {desired}: {note}"),
                            took,
                        );
                        probe.get_or_insert(("strategy/recommend", params));
                    }
                    Err(err) => suite.record("strategy/recommend", Outcome::Fail, err, took),
                }
            }
            Kind::Target => {
                let params = json!({ "policy": "conformance", "config": policy });
                let (result, took) = suite.call("target/status", params.clone()).await;
                let count = match result
                    .map_err(|err| err.to_string())
                    .and_then(|value| super::host::parse_status(&value))
                {
                    Ok(status) => {
                        suite.record(
                            "target/status",
                            Outcome::Pass,
                            format!("count {}, ready {}", status.count, status.ready),
                            took,
                        );
                        probe.get_or_insert(("target/status", params));
                        Some(status.count)
                    }
                    Err(err) => {
                        suite.record("target/status", Outcome::Fail, err, took);
                        None
                    }
                };
                match count {
                    Some(count) => {
                        let (result, took) = suite
                            .call(
                                "target/scale",
                                json!({ "policy": "conformance", "config": policy,
                                        "desired": count, "decision": "d-conformance" }),
                            )
                            .await;
                        match result {
                            Ok(value) if value.is_object() => suite.record(
                                "target/scale",
                                Outcome::Pass,
                                format!("scaled to the count it holds ({count}): nothing changed"),
                                took,
                            ),
                            Ok(value) => suite.record(
                                "target/scale",
                                Outcome::Fail,
                                format!("the result must be an object, got {value}"),
                                took,
                            ),
                            Err(err) => {
                                suite.record("target/scale", Outcome::Fail, err.to_string(), took)
                            }
                        }
                    }
                    None => suite.record(
                        "target/scale",
                        Outcome::Skip,
                        "target/status failed, so there is no count to scale to",
                        Duration::ZERO,
                    ),
                }
                if info.nodes {
                    let (result, took) = suite
                        .call(
                            "target/terminate",
                            json!({ "policy": "conformance", "config": policy, "ids": [] }),
                        )
                        .await;
                    match result {
                        Ok(_) => suite.record(
                            "target/terminate",
                            Outcome::Pass,
                            "terminating no members succeeded",
                            took,
                        ),
                        Err(err) => {
                            suite.record("target/terminate", Outcome::Fail, err.to_string(), took)
                        }
                    }
                } else {
                    suite.record(
                        "target/terminate",
                        Outcome::Skip,
                        "capabilities.nodes is false: only node-group targets terminate members",
                        Duration::ZERO,
                    );
                }
            }
        }
    }

    // ── unknown method ─────────────────────────────────────────────────
    let (result, took) = suite.call("legion/conformance-unknown", json!({})).await;
    match result {
        Err(CallError::Remote { code: -32601, .. }) => suite.record(
            "unknown method",
            Outcome::Pass,
            "answered -32601 (method not found)",
            took,
        ),
        Err(CallError::Remote { code, message }) => suite.record(
            "unknown method",
            Outcome::Warn,
            format!("answered error {code} ({message}); -32601 is the code for a method it does not have"),
            took,
        ),
        Ok(value) => suite.record(
            "unknown method",
            Outcome::Fail,
            format!("answered {value} to a method it does not have"),
            took,
        ),
        Err(err) => suite.record("unknown method", Outcome::Fail, err.to_string(), took),
    }

    // ── malformed request ──────────────────────────────────────────────
    if let Some((method, good)) = probe.clone() {
        let malformed = match method {
            "source/query" => json!({ "query": 42, "windowSeconds": "a minute" }),
            "strategy/recommend" => json!({ "current": "four", "ready": -1, "config": policy }),
            _ => json!({ "policy": 7, "config": "not a record" }),
        };
        let (result, took) = suite.call(method, malformed).await;
        let (outcome, detail) = match result {
            Err(CallError::Remote { code: -32602, .. }) => (
                Outcome::Pass,
                "answered -32602 (invalid params)".to_string(),
            ),
            Err(CallError::Remote { code, message }) => (
                Outcome::Warn,
                format!("answered error {code} ({message}); -32602 is the code for invalid params"),
            ),
            Ok(value) => (
                Outcome::Warn,
                format!("accepted parameters of the wrong type and answered {value}"),
            ),
            Err(err) => (Outcome::Fail, err.to_string()),
        };
        let (after, after_took) = suite.call(method, good.clone()).await;
        match (outcome, after) {
            (Outcome::Fail, _) => suite.record("malformed request", Outcome::Fail, detail, took),
            (outcome, Ok(_)) => suite.record(
                "malformed request",
                outcome,
                format!("{detail}; still answering after it"),
                took + after_took,
            ),
            (_, Err(err)) => suite.record(
                "malformed request",
                Outcome::Fail,
                format!("{detail}; but the next call failed: {err}"),
                took + after_took,
            ),
        }
    } else {
        suite.record(
            "malformed request",
            Outcome::Skip,
            "no method answered, so there is nothing to compare against",
            Duration::ZERO,
        );
    }

    // ── crash and restart ──────────────────────────────────────────────
    if !is_command {
        suite.record(
            "crash and restart",
            Outcome::Skip,
            "a URL plugin runs elsewhere; its restarts are not the autoscaler's",
            Duration::ZERO,
        );
    } else if let Some((method, good)) = probe.clone() {
        let before = suite.transport.starts().await;
        suite.transport.kill().await;
        let (result, took) = suite.call(method, good).await;
        let after = suite.transport.starts().await;
        match result {
            Ok(_) if after > before => suite.record(
                "crash and restart",
                Outcome::Pass,
                format!("killed, it started again, handshook, and answered {method}"),
                took,
            ),
            Ok(_) => suite.record(
                "crash and restart",
                Outcome::Fail,
                "the plugin answered without being restarted",
                took,
            ),
            Err(err) => suite.record(
                "crash and restart",
                Outcome::Fail,
                format!("after a kill it did not come back: {err}"),
                took,
            ),
        }
    } else {
        suite.record(
            "crash and restart",
            Outcome::Skip,
            "no method answered to retry after a restart",
            Duration::ZERO,
        );
    }

    // ── timeouts ───────────────────────────────────────────────────────
    let timed_out: Vec<String> = suite
        .checks
        .iter()
        .filter(|check| check.detail.contains("timed out after"))
        .map(|check| check.name.clone())
        .collect();
    if !timed_out.is_empty() {
        suite.record(
            "timeouts",
            Outcome::Fail,
            format!("no answer in time from: {}", timed_out.join(", ")),
            Duration::ZERO,
        );
    } else if let Some((method, took, limit)) = suite.slowest.clone() {
        let limit = options.timeout.unwrap_or(limit);
        let outcome = if took * 2 > limit {
            Outcome::Warn
        } else {
            Outcome::Pass
        };
        suite.record(
            "timeouts",
            outcome,
            format!(
                "every call answered in time; the slowest was {method}, {}ms of its {}s",
                took.as_millis(),
                limit.as_secs_f64()
            ),
            Duration::ZERO,
        );
    }

    let passed = suite
        .checks
        .iter()
        .all(|check| check.outcome != Outcome::Fail);
    SuiteReport {
        plugin_ref,
        plugin: Some(summary),
        checks: suite.checks,
        passed,
    }
}
