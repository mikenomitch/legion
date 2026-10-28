//! Write Legion autoscaler plugins in Rust.
//!
//! A plugin is a program the autoscaler starts once and talks to over its
//! stdin and stdout, one JSON-RPC 2.0 message per line: the
//! `legion-scaling/1` protocol (docs/autoscaling-plugins.md). One plugin may
//! be any mix of the three kinds:
//!
//! - a **source** answers `source/query` with a reading (a metric value);
//! - a **strategy** answers `strategy/recommend` with a desired count;
//! - a **target** answers `target/status`, `target/scale`, and (for node
//!   groups) `target/terminate` for something outside Legion.
//!
//! Implement [`Plugin`] for your type, overriding the methods of the kinds
//! you declare in [`Plugin::initialize`], and hand it to [`serve`]:
//!
//! ```no_run
//! use legion_scaling_plugin::{serve, Error, Initialize, Kind, Plugin, PluginInfo, Recommend, Recommendation};
//!
//! struct Double;
//!
//! impl Plugin for Double {
//!     fn initialize(&mut self, _request: Initialize) -> Result<PluginInfo, Error> {
//!         Ok(PluginInfo::new("double", "0.1.0", &[Kind::Strategy]))
//!     }
//!
//!     fn recommend(&mut self, request: Recommend) -> Result<Recommendation, Error> {
//!         Ok(Recommendation::new(request.current * 2, "twice the current count"))
//!     }
//! }
//!
//! fn main() -> std::io::Result<()> {
//!     serve(Double)
//! }
//! ```
//!
//! Test it with `legion scaling plugin test ./target/release/double`. The
//! SDK handles the framing and the error codes: a method you did not
//! implement answers "method not found", parameters that do not parse
//! answer "invalid params", and a panic inside a method answers "internal
//! error" instead of killing the plugin. Log to stderr: stdout is the
//! protocol.

use std::io::{self, BufRead, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The protocol name `initialize` carries.
pub const PROTOCOL: &str = "legion-scaling/1";

/// What a plugin is used as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// A metric source.
    Source,
    /// A strategy.
    Strategy,
    /// A target.
    Target,
}

/// What a target can do beyond scaling a count.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// The target's members are Legion nodes (a node group): it implements
    /// `target/terminate`, and `target/scale` receives a join token.
    #[serde(default)]
    pub nodes: bool,
}

/// The answer to `initialize`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginInfo {
    /// The plugin's own name.
    pub name: String,
    /// Its version.
    pub version: String,
    /// The kinds it implements.
    pub kinds: Vec<Kind>,
    /// What its target can do.
    pub capabilities: Capabilities,
    /// A JSON Schema the policy-level `config` must match (a strategy's
    /// settings, a target's `config`). The autoscaler checks it before
    /// every call, so the plugin never sees a config that does not match.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_config_schema: Option<Value>,
}

impl PluginInfo {
    /// A plugin of `kinds` with no capabilities and no schema.
    pub fn new(name: &str, version: &str, kinds: &[Kind]) -> Self {
        Self {
            name: name.to_string(),
            version: version.to_string(),
            kinds: kinds.to_vec(),
            capabilities: Capabilities::default(),
            policy_config_schema: None,
        }
    }

    /// With a policy-config schema.
    pub fn with_schema(mut self, schema: Value) -> Self {
        self.policy_config_schema = Some(schema);
        self
    }
}

/// `initialize`'s parameters.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Initialize {
    /// `legion-scaling/1`.
    pub protocol: String,
    /// The name the operator installed the plugin under.
    #[serde(default)]
    pub name: String,
    /// The operator's plugin-level config (addresses, regions, credentials
    /// references).
    #[serde(default)]
    pub config: Value,
}

/// `source/query`'s parameters.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceQuery {
    /// The policy asking (`web/default/web`, or a standalone policy's name).
    #[serde(default)]
    pub policy: String,
    /// The check asking.
    #[serde(default)]
    pub check: String,
    /// The policy's query, as written.
    pub query: String,
    /// The window the check averages over, as written (`"2m"`).
    #[serde(default)]
    pub window: String,
    /// The same, in seconds.
    #[serde(default)]
    pub window_seconds: u64,
}

/// A source's answer.
#[derive(Debug, Clone, PartialEq)]
pub enum Reading {
    /// The value, already averaged over the window if the source can.
    Value(f64),
    /// Points (`(unix ms, value)`) the autoscaler averages over the window.
    Series(Vec<(i64, f64)>),
    /// No data, and why. The check counts as missing, which holds scale-in.
    Missing(String),
}

impl Reading {
    fn to_json(&self) -> Value {
        match self {
            Reading::Value(value) => json!({ "value": value }),
            Reading::Series(points) => json!({
                "series": points.iter().map(|(at, value)| json!([at, value])).collect::<Vec<_>>()
            }),
            Reading::Missing(reason) => json!({ "value": null, "reason": reason }),
        }
    }
}

/// `strategy/recommend`'s parameters.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Recommend {
    /// The policy asking.
    #[serde(default)]
    pub policy: String,
    /// The check asking.
    #[serde(default)]
    pub check: String,
    /// The target's current count.
    pub current: u32,
    /// Ready replicas (healthy and past warmup).
    #[serde(default)]
    pub ready: u32,
    /// The check's reading, when it has a query.
    #[serde(default)]
    pub reading: Option<f64>,
    /// The strategy's settings from the policy (every field but `name`),
    /// already checked against the schema.
    #[serde(default)]
    pub config: Value,
}

/// A strategy's answer.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Recommendation {
    /// The desired count.
    pub desired: u32,
    /// What it saw, for `legion scaling explain`.
    pub note: String,
}

impl Recommendation {
    /// A recommendation of `desired`, explained by `note`.
    pub fn new(desired: u32, note: impl Into<String>) -> Self {
        Self {
            desired,
            note: note.into(),
        }
    }
}

/// `target/status`'s parameters.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetRequest {
    /// The policy asking.
    #[serde(default)]
    pub policy: String,
    /// The policy's `target.config`, already checked against the schema.
    #[serde(default)]
    pub config: Value,
}

/// One member of a target (an instance, a VM).
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Member {
    /// The provider's id for it.
    pub id: String,
    /// The Legion node it became, when it joined one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// The provider's state for it (`running`, `pending`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
}

/// A target's answer to `target/status`.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct TargetStatus {
    /// The count the target holds now.
    pub count: u32,
    /// How many of them are ready; `None` means all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready: Option<u32>,
    /// Its members, when it knows them.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<Member>,
}

/// `target/scale`'s parameters.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScaleRequest {
    /// The policy asking.
    #[serde(default)]
    pub policy: String,
    /// The policy's `target.config`.
    #[serde(default)]
    pub config: Value,
    /// The count to move to.
    pub desired: u32,
    /// The decision's id, as `legion scaling history` shows it. The same
    /// decision may be sent twice after a restart: scaling must be
    /// idempotent.
    #[serde(default)]
    pub decision: String,
    /// For node targets: the join token and attributes new members join
    /// with. A secret: never log it.
    #[serde(default)]
    pub join: Option<Value>,
}

/// `target/terminate`'s parameters (node targets only).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminateRequest {
    /// The policy asking.
    #[serde(default)]
    pub policy: String,
    /// The policy's `target.config`.
    #[serde(default)]
    pub config: Value,
    /// The provider ids of the members to stop.
    pub ids: Vec<String>,
}

/// A JSON-RPC error: what a method returns when it cannot answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Error {
    /// The JSON-RPC code: -32601 method not found, -32602 invalid params,
    /// -32603 internal error, -32000..-32099 the plugin's own.
    pub code: i64,
    /// For people: shown in `legion scaling explain` and the plugin test.
    pub message: String,
}

impl Error {
    /// A plugin error (code -32000).
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            code: -32000,
            message: message.into(),
        }
    }

    /// Invalid parameters (-32602).
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }

    /// A method this plugin does not implement (-32601).
    pub fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("method not found: {method}"),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

impl std::error::Error for Error {}

/// A scaling plugin. Implement `initialize` and the methods of the kinds it
/// declares; the rest answer "method not found".
pub trait Plugin {
    /// The handshake: check the protocol, read the operator's config, and
    /// say what the plugin is. Called once per process start.
    fn initialize(&mut self, request: Initialize) -> Result<PluginInfo, Error>;

    /// `source/query`: read one value.
    fn query(&mut self, request: SourceQuery) -> Result<Reading, Error> {
        let _ = request;
        Err(Error::method_not_found("source/query"))
    }

    /// `strategy/recommend`: turn a reading into a desired count.
    fn recommend(&mut self, request: Recommend) -> Result<Recommendation, Error> {
        let _ = request;
        Err(Error::method_not_found("strategy/recommend"))
    }

    /// `target/status`: the count the target holds.
    fn status(&mut self, request: TargetRequest) -> Result<TargetStatus, Error> {
        let _ = request;
        Err(Error::method_not_found("target/status"))
    }

    /// `target/scale`: move the target to `desired`. Must be idempotent.
    fn scale(&mut self, request: ScaleRequest) -> Result<(), Error> {
        let _ = request;
        Err(Error::method_not_found("target/scale"))
    }

    /// `target/terminate`: stop these members (node targets only).
    fn terminate(&mut self, request: TerminateRequest) -> Result<(), Error> {
        let _ = request;
        Err(Error::method_not_found("target/terminate"))
    }
}

fn params<T: DeserializeOwned>(method: &str, params: Value) -> Result<T, Error> {
    serde_json::from_value(params).map_err(|err| Error::invalid_params(format!("{method}: {err}")))
}

fn to_value<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn dispatch<P: Plugin>(plugin: &mut P, method: &str, raw: Value) -> Result<Value, Error> {
    match method {
        "initialize" => {
            let request: Initialize = params(method, raw)?;
            if request.protocol != PROTOCOL {
                return Err(Error::invalid_params(format!(
                    "unsupported protocol {:?}; this plugin speaks {PROTOCOL}",
                    request.protocol
                )));
            }
            plugin.initialize(request).map(|info| to_value(&info))
        }
        "source/query" => plugin.query(params(method, raw)?).map(|r| r.to_json()),
        "strategy/recommend" => plugin
            .recommend(params(method, raw)?)
            .map(|rec| to_value(&rec)),
        "target/status" => plugin
            .status(params(method, raw)?)
            .map(|status| to_value(&status)),
        "target/scale" => plugin.scale(params(method, raw)?).map(|()| json!({})),
        "target/terminate" => plugin.terminate(params(method, raw)?).map(|()| json!({})),
        other => Err(Error::method_not_found(other)),
    }
}

fn error_reply(id: Value, error: &Error) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": error.code, "message": error.message } })
}

/// Answer one request line with one response line (no newline). Useful for
/// serving the same plugin over HTTP: POST bodies are request lines.
pub fn handle_line<P: Plugin>(plugin: &mut P, line: &str) -> String {
    let request: Value = match serde_json::from_str(line) {
        Ok(request) => request,
        Err(err) => {
            return error_reply(
                Value::Null,
                &Error {
                    code: -32700,
                    message: format!("parse error: {err}"),
                },
            )
            .to_string()
        }
    };
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let Some(method) = request.get("method").and_then(Value::as_str) else {
        return error_reply(
            id,
            &Error {
                code: -32600,
                message: "invalid request: no method".to_string(),
            },
        )
        .to_string();
    };
    let raw = request.get("params").cloned().unwrap_or(json!({}));
    let outcome = catch_unwind(AssertUnwindSafe(|| dispatch(plugin, method, raw)));
    let reply = match outcome {
        Ok(Ok(result)) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Ok(Err(error)) => error_reply(id, &error),
        Err(panic) => {
            let what = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panic".to_string());
            error_reply(
                id,
                &Error {
                    code: -32603,
                    message: format!("internal error: {what}"),
                },
            )
        }
    };
    reply.to_string()
}

/// Serve `plugin` over any reader and writer, one message per line, until
/// the input ends.
pub fn serve_io<P: Plugin>(
    plugin: &mut P,
    input: impl BufRead,
    mut output: impl Write,
) -> io::Result<()> {
    for line in input.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let reply = handle_line(plugin, &line);
        output.write_all(reply.as_bytes())?;
        output.write_all(b"\n")?;
        output.flush()?;
    }
    Ok(())
}

/// Serve `plugin` on stdin and stdout until the autoscaler closes stdin.
pub fn serve<P: Plugin>(mut plugin: P) -> io::Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    serve_io(&mut plugin, stdin.lock(), stdout.lock())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        count: u32,
    }

    impl Plugin for Fixture {
        fn initialize(&mut self, request: Initialize) -> Result<PluginInfo, Error> {
            self.count = request.config["start"].as_u64().unwrap_or(0) as u32;
            Ok(
                PluginInfo::new("fixture", "1.0.0", &[Kind::Source, Kind::Target])
                    .with_schema(json!({ "type": "object" })),
            )
        }

        fn query(&mut self, request: SourceQuery) -> Result<Reading, Error> {
            match request.query.as_str() {
                "empty" => Ok(Reading::Missing("no series".into())),
                "series" => Ok(Reading::Series(vec![(1, 2.0), (2, 4.0)])),
                "boom" => panic!("query exploded"),
                _ => Ok(Reading::Value(42.0)),
            }
        }

        fn status(&mut self, _request: TargetRequest) -> Result<TargetStatus, Error> {
            Ok(TargetStatus {
                count: self.count,
                ..Default::default()
            })
        }

        fn scale(&mut self, request: ScaleRequest) -> Result<(), Error> {
            self.count = request.desired;
            Ok(())
        }
    }

    fn call(plugin: &mut Fixture, request: Value) -> Value {
        serde_json::from_str(&handle_line(plugin, &request.to_string())).unwrap()
    }

    #[test]
    fn handshake_and_methods_test() {
        let mut plugin = Fixture { count: 0 };
        let init = call(
            &mut plugin,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": { "protocol": PROTOCOL, "name": "f", "config": { "start": 3 } } }),
        );
        assert_eq!(init["id"], 1);
        assert_eq!(init["result"]["kinds"], json!(["source", "target"]));
        assert_eq!(init["result"]["policyConfigSchema"], json!({ "type": "object" }));
        let status = call(
            &mut plugin,
            json!({ "jsonrpc": "2.0", "id": 2, "method": "target/status", "params": { "policy": "p" } }),
        );
        assert_eq!(status["result"], json!({ "count": 3 }));
        let scale = call(
            &mut plugin,
            json!({ "jsonrpc": "2.0", "id": 3, "method": "target/scale",
                    "params": { "policy": "p", "desired": 5 } }),
        );
        assert_eq!(scale["result"], json!({}));
        assert_eq!(plugin.count, 5);
        let reading = call(
            &mut plugin,
            json!({ "jsonrpc": "2.0", "id": 4, "method": "source/query", "params": { "query": "series" } }),
        );
        assert_eq!(reading["result"], json!({ "series": [[1, 2.0], [2, 4.0]] }));
        let missing = call(
            &mut plugin,
            json!({ "jsonrpc": "2.0", "id": 5, "method": "source/query", "params": { "query": "empty" } }),
        );
        assert_eq!(missing["result"], json!({ "value": null, "reason": "no series" }));
    }

    #[test]
    fn errors_keep_the_plugin_alive_test() {
        let mut plugin = Fixture { count: 0 };
        let parse = call_raw(&mut plugin, "not json");
        assert_eq!(parse["error"]["code"], -32700);
        assert_eq!(parse["id"], Value::Null);
        let unknown = call(&mut plugin, json!({ "jsonrpc": "2.0", "id": 7, "method": "nope" }));
        assert_eq!(unknown["error"]["code"], -32601);
        assert_eq!(unknown["id"], 7);
        let not_implemented = call(
            &mut plugin,
            json!({ "jsonrpc": "2.0", "id": 8, "method": "strategy/recommend",
                    "params": { "current": 1 } }),
        );
        assert_eq!(not_implemented["error"]["code"], -32601);
        let bad = call(
            &mut plugin,
            json!({ "jsonrpc": "2.0", "id": 9, "method": "target/scale",
                    "params": { "desired": "many" } }),
        );
        assert_eq!(bad["error"]["code"], -32602);
        let wrong_protocol = call(
            &mut plugin,
            json!({ "jsonrpc": "2.0", "id": 10, "method": "initialize",
                    "params": { "protocol": "legion-scaling/9" } }),
        );
        assert_eq!(wrong_protocol["error"]["code"], -32602);
        let panicked = call(
            &mut plugin,
            json!({ "jsonrpc": "2.0", "id": 11, "method": "source/query", "params": { "query": "boom" } }),
        );
        assert_eq!(panicked["error"]["code"], -32603);
        assert!(panicked["error"]["message"]
            .as_str()
            .unwrap()
            .contains("query exploded"));
        // Still answering.
        let fine = call(
            &mut plugin,
            json!({ "jsonrpc": "2.0", "id": 12, "method": "source/query", "params": { "query": "q" } }),
        );
        assert_eq!(fine["result"], json!({ "value": 42.0 }));
    }

    fn call_raw(plugin: &mut Fixture, line: &str) -> Value {
        serde_json::from_str(&handle_line(plugin, line)).unwrap()
    }

    #[test]
    fn serve_io_frames_one_message_per_line_test() {
        let mut plugin = Fixture { count: 0 };
        let input = format!(
            "{}\n\n{}\n",
            json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": { "protocol": PROTOCOL } }),
            json!({ "jsonrpc": "2.0", "id": 2, "method": "target/status", "params": {} }),
        );
        let mut output = Vec::new();
        serve_io(&mut plugin, input.as_bytes(), &mut output).unwrap();
        let lines: Vec<Value> = String::from_utf8(output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1]["result"]["count"], 0);
    }
}
