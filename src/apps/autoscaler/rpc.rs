//! The scaling plugin protocol, `legion-scaling/1`, from the autoscaler's
//! side (docs/autoscaling-plugins.md; autoscaler-v2 plan §6.8, calls entry
//! 4): JSON-RPC 2.0 to a program the app runs (one message per line on its
//! stdin and stdout) or to a URL (one HTTP POST per message).
//!
//! A [`Transport`] owns one plugin connection:
//!
//! - **Started lazily and kept.** The first call starts the program (or
//!   reaches the URL) and sends `initialize` with the operator's config; the
//!   answer ([`PluginInfo`]) is kept until the next start.
//! - **One call at a time** per program: a plugin may be single-threaded,
//!   and responses are matched to requests by id.
//! - **Per-call timeouts** ([`SOURCE_TIMEOUT`], [`STRATEGY_TIMEOUT`],
//!   [`TARGET_TIMEOUT`]). A program that misses one is killed, because a
//!   late answer would arrive as the reply to the next call.
//! - **Restarted with backoff.** A program that exits, times out, or
//!   answers with something that is not a JSON-RPC response is restarted on
//!   the next call: at once the first time, then after 1s, 2s, 4s … up to
//!   [`RESTART_BACKOFF_MAX`] while it keeps failing. A successful call
//!   resets the backoff.
//! - **Secrets stay out of traces.** Requests are logged at `debug` through
//!   [`redact`], which blanks values under keys that name credentials (and
//!   a node group's join token).

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use crate::autoscaler::plugins::{Kind, PROTOCOL};
use crate::prelude::*;

/// How long `initialize` may take.
pub const INIT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long `source/query` may take.
pub const SOURCE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long `strategy/recommend` may take.
pub const STRATEGY_TIMEOUT: Duration = Duration::from_secs(10);
/// How long `target/status`, `target/scale` and `target/terminate` may
/// take: a cloud API scaling a group is slow.
pub const TARGET_TIMEOUT: Duration = Duration::from_secs(120);
/// The longest wait between restarts of a plugin that keeps failing.
pub const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(60);
/// How much of a program's stderr is kept for error messages.
const STDERR_TAIL_BYTES: usize = 4096;
/// The longest reply line read from a program.
const MAX_REPLY_BYTES: usize = 8 * 1024 * 1024;

/// The default timeout of a protocol method.
pub fn timeout_for(method: &str) -> Duration {
    match method {
        "initialize" => INIT_TIMEOUT,
        "source/query" => SOURCE_TIMEOUT,
        "strategy/recommend" => STRATEGY_TIMEOUT,
        _ => TARGET_TIMEOUT,
    }
}

/// Why a call failed.
#[derive(Debug, Clone, PartialEq)]
pub enum CallError {
    /// No answer within the method's timeout (the program was killed).
    Timeout {
        /// The method.
        method: String,
        /// The timeout.
        after: Duration,
    },
    /// The program exited or closed its stdout.
    Crashed(String),
    /// The program keeps failing and waits out its backoff before the next
    /// start.
    Restarting {
        /// How long until the next start.
        retry_in: Duration,
        /// What went wrong last.
        last: String,
    },
    /// The answer was not a JSON-RPC response to the request.
    Malformed(String),
    /// The plugin answered with a JSON-RPC error.
    Remote {
        /// The JSON-RPC code (-32601 method not found, -32602 invalid
        /// params, …).
        code: i64,
        /// Its message.
        message: String,
    },
    /// The program could not start, or the URL could not be reached.
    Transport(String),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Timeout { method, after } => {
                write!(f, "{method} timed out after {}s", after.as_secs_f64())
            }
            CallError::Crashed(detail) => write!(f, "the plugin exited: {detail}"),
            CallError::Restarting { retry_in, last } => write!(
                f,
                "the plugin is restarting in {}s (last failure: {last})",
                retry_in.as_secs().max(1)
            ),
            CallError::Malformed(detail) => write!(f, "malformed reply: {detail}"),
            CallError::Remote { code, message } => write!(f, "{message} (error {code})"),
            CallError::Transport(detail) => f.write_str(detail),
        }
    }
}

impl std::error::Error for CallError {}

impl CallError {
    /// Whether the plugin itself answered (an error it chose), rather than
    /// failing to answer.
    pub fn is_remote(&self) -> bool {
        matches!(self, CallError::Remote { .. })
    }
}

/// What a plugin said about itself in `initialize`.
#[derive(Debug, Clone, PartialEq)]
pub struct PluginInfo {
    /// The plugin's own name.
    pub name: String,
    /// Its version.
    pub version: String,
    /// The kinds it implements.
    pub kinds: Vec<Kind>,
    /// Its target manages Legion nodes (`target/terminate`).
    pub nodes: bool,
    /// The JSON Schema the policy-level config must match.
    pub policy_config_schema: Option<Value>,
    /// The whole answer, as it came.
    pub raw: Value,
}

impl PluginInfo {
    /// Parse an `initialize` result, strictly: `name` and `version`
    /// strings, and `kinds`, a non-empty list of `source`, `strategy`,
    /// `target`.
    pub fn parse(result: &Value) -> Result<Self, String> {
        let object = result
            .as_object()
            .ok_or_else(|| format!("the initialize result must be an object, got {result}"))?;
        let text = |key: &str| -> Result<String, String> {
            object
                .get(key)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| format!("the initialize result needs a `{key}` string"))
        };
        let name = text("name")?;
        let version = text("version")?;
        if let Some(protocol) = object.get("protocol") {
            if protocol.as_str() != Some(PROTOCOL) {
                return Err(format!(
                    "the plugin speaks {protocol}, and this autoscaler speaks {PROTOCOL}"
                ));
            }
        }
        let kinds_value = object
            .get("kinds")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                "the initialize result needs `kinds`, a list of source, strategy, target"
                    .to_string()
            })?;
        let mut kinds = Vec::new();
        for kind in kinds_value {
            let parsed = kind.as_str().and_then(Kind::parse).ok_or_else(|| {
                format!("unknown kind {kind} in `kinds`: use source, strategy, target")
            })?;
            if !kinds.contains(&parsed) {
                kinds.push(parsed);
            }
        }
        if kinds.is_empty() {
            return Err("`kinds` is empty: a plugin implements at least one kind".to_string());
        }
        let nodes = match object.get("capabilities") {
            None | Some(Value::Null) => false,
            Some(Value::Object(capabilities)) => match capabilities.get("nodes") {
                None | Some(Value::Null) => false,
                Some(Value::Bool(nodes)) => *nodes,
                Some(other) => {
                    return Err(format!("capabilities.nodes must be a boolean, got {other}"))
                }
            },
            Some(other) => return Err(format!("`capabilities` must be an object, got {other}")),
        };
        let policy_config_schema = match object.get("policyConfigSchema") {
            None | Some(Value::Null) => None,
            Some(schema) => {
                crate::structs::app::compile_config_schema(schema)
                    .map_err(|err| format!("policyConfigSchema {err}"))?;
                Some(schema.clone())
            }
        };
        Ok(Self {
            name,
            version,
            kinds,
            nodes,
            policy_config_schema,
            raw: result.clone(),
        })
    }
}

/// Keys whose values never reach a log line: anything naming a credential,
/// and a node group's join token.
fn secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "token",
        "secret",
        "password",
        "passwd",
        "credential",
        "apikey",
        "api_key",
        "authorization",
        "privatekey",
        "private_key",
        "join",
    ]
    .iter()
    .any(|word| key.contains(word))
}

/// A copy of `value` with every value under a secret-looking key replaced
/// by `"<redacted>"`, for traces.
pub fn redact(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| {
                    if secret_key(key) {
                        (key.clone(), Value::String("<redacted>".into()))
                    } else {
                        (key.clone(), redact(value))
                    }
                })
                .collect::<Map<String, Value>>(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redact).collect()),
        other => other.clone(),
    }
}

/// Turn one JSON-RPC response into the call's result.
fn response_result(response: Value, id: u64) -> Result<Value, CallError> {
    let Value::Object(mut object) = response else {
        return Err(CallError::Malformed(format!(
            "expected a JSON-RPC response object, got {response}"
        )));
    };
    let reply_id = object.get("id").cloned().unwrap_or(Value::Null);
    if let Some(error) = object.remove("error") {
        // A plugin that could not parse the request answers with id null.
        if reply_id != Value::Null && reply_id.as_u64() != Some(id) {
            return Err(CallError::Malformed(format!(
                "a reply for request {reply_id}, while waiting for {id}"
            )));
        }
        let code = error.get("code").and_then(Value::as_i64).ok_or_else(|| {
            CallError::Malformed(format!("an error without a numeric code: {error}"))
        })?;
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        return Err(CallError::Remote { code, message });
    }
    if reply_id.as_u64() != Some(id) {
        return Err(CallError::Malformed(format!(
            "a reply for request {reply_id}, while waiting for {id}"
        )));
    }
    object
        .remove("result")
        .ok_or_else(|| CallError::Malformed("a response with neither result nor error".into()))
}

fn request_line(id: u64, method: &str, params: &Value) -> String {
    let mut line =
        json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string();
    line.push('\n');
    line
}

/// How a plugin is reached.
#[derive(Debug, Clone, PartialEq)]
pub enum Endpoint {
    /// A program and its arguments, run in `dir` when given.
    Command {
        /// The program and its arguments.
        argv: Vec<String>,
        /// Its working directory (relative program paths resolve here).
        dir: Option<PathBuf>,
    },
    /// A URL taking one HTTP POST per message.
    Url(String),
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Endpoint::Command { argv, .. } => write!(f, "`{}`", argv.join(" ")),
            Endpoint::Url(url) => f.write_str(url),
        }
    }
}

struct Running {
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    stdout: BufReader<tokio::process::ChildStdout>,
}

impl Running {
    /// Kill the program and everything it started (its process group).
    async fn kill(mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.child.id() {
            // SAFETY: a plain syscall on the group of a child we spawned
            // with `process_group(0)` and have not reaped.
            unsafe { libc::killpg(pid as libc::pid_t, libc::SIGKILL) };
        }
        let _ = self.child.kill().await;
    }
}

#[derive(Default)]
struct State {
    running: Option<Running>,
    /// The `initialize` answer for the current start (URL plugins: until a
    /// call fails to reach them).
    info: Option<PluginInfo>,
    next_id: u64,
    failures: u32,
    retry_at: Option<Instant>,
    last_failure: String,
    starts: u64,
}

/// One plugin connection. See the module docs.
pub struct Transport {
    /// The installed name, for logs.
    name: String,
    endpoint: Endpoint,
    /// `initialize`'s params.
    init: Value,
    state: tokio::sync::Mutex<State>,
    stderr: Arc<std::sync::Mutex<VecDeque<u8>>>,
    http: reqwest::Client,
    /// Overrides every method's timeout (the conformance suite's
    /// `--timeout`).
    timeout_override: Option<Duration>,
    /// Starts and failures log at `debug` instead of `info`/`warn`: the
    /// conformance suite reports them itself, and provokes some on purpose.
    quiet: bool,
}

impl Transport {
    /// A connection to the plugin installed as `name`, sending `config` in
    /// `initialize`.
    pub fn new(name: &str, endpoint: Endpoint, config: Value) -> Self {
        Self {
            name: name.to_string(),
            endpoint,
            init: json!({ "protocol": PROTOCOL, "name": name, "config": config }),
            state: tokio::sync::Mutex::new(State::default()),
            stderr: Arc::new(std::sync::Mutex::new(VecDeque::new())),
            http: reqwest::Client::new(),
            timeout_override: None,
            quiet: false,
        }
    }

    /// Use `timeout` for every method instead of the protocol defaults.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout_override = Some(timeout);
        self
    }

    /// Log starts and failed calls at `debug` (the conformance suite).
    pub fn quiet(mut self) -> Self {
        self.quiet = true;
        self
    }

    /// The installed name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Where the plugin is.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// How many times the program was started (URL plugins: initialized).
    pub async fn starts(&self) -> u64 {
        self.state.lock().await.starts
    }

    /// The last `initialize` answer, starting the plugin if it is not
    /// running.
    pub async fn info(&self) -> Result<PluginInfo, CallError> {
        let mut state = self.state.lock().await;
        self.ensure_started(&mut state).await?;
        state
            .info
            .clone()
            .ok_or_else(|| CallError::Transport("the plugin has not initialized".into()))
    }

    /// The tail of the program's stderr.
    pub fn stderr_tail(&self) -> String {
        let tail = self.stderr.lock().unwrap_or_else(|p| p.into_inner());
        let bytes: Vec<u8> = tail.iter().copied().collect();
        String::from_utf8_lossy(&bytes).trim().to_string()
    }

    /// Kill the program now (the conformance suite's crash test). The next
    /// call starts it again.
    pub async fn kill(&self) {
        let mut state = self.state.lock().await;
        if let Some(running) = state.running.take() {
            running.kill().await;
        }
        state.info = None;
    }

    fn timeout(&self, method: &str) -> Duration {
        self.timeout_override.unwrap_or_else(|| timeout_for(method))
    }

    /// Record a failure: the program (if any) is gone, and the next start
    /// waits out the backoff after the first consecutive failure.
    async fn fail(&self, state: &mut State, why: String) {
        if let Some(running) = state.running.take() {
            running.kill().await;
        }
        state.info = None;
        state.failures = state.failures.saturating_add(1);
        state.last_failure = why;
        if state.failures > 1 {
            let exponent = (state.failures - 2).min(6);
            let backoff = Duration::from_secs(1u64 << exponent).min(RESTART_BACKOFF_MAX);
            state.retry_at = Some(Instant::now() + backoff);
        }
    }

    async fn spawn(&self, argv: &[String], dir: Option<&PathBuf>) -> Result<Running, CallError> {
        let program = argv
            .first()
            .ok_or_else(|| CallError::Transport("the plugin has an empty command".into()))?;
        let mut command = tokio::process::Command::new(program);
        command
            .args(&argv[1..])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        if let Some(dir) = dir {
            command.current_dir(dir);
        }
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn().map_err(|err| {
            CallError::Transport(format!("cannot start `{}`: {err}", argv.join(" ")))
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| CallError::Transport("no stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| CallError::Transport("no stdout".into()))?;
        if let Some(mut stderr) = child.stderr.take() {
            let tail = self.stderr.clone();
            let name = self.name.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            debug!(plugin = %name, "plugin stderr: {}", String::from_utf8_lossy(&buf[..n]).trim_end());
                            let mut tail = tail.lock().unwrap_or_else(|p| p.into_inner());
                            tail.extend(&buf[..n]);
                            while tail.len() > STDERR_TAIL_BYTES {
                                tail.pop_front();
                            }
                        }
                    }
                }
            });
        }
        Ok(Running {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    /// Start the program (or reach the URL) and run `initialize`, unless
    /// that already happened.
    async fn ensure_started(&self, state: &mut State) -> Result<(), CallError> {
        if state.info.is_some() {
            if let Some(running) = state.running.as_mut() {
                // A program that exited between calls is restarted now.
                if let Ok(Some(status)) = running.child.try_wait() {
                    let why = format!("exited with {status} between calls{}", self.tail_note());
                    self.fail(state, why).await;
                } else {
                    return Ok(());
                }
            } else if matches!(self.endpoint, Endpoint::Url(_)) {
                return Ok(());
            }
        }
        if let Some(at) = state.retry_at {
            let now = Instant::now();
            if now < at {
                return Err(CallError::Restarting {
                    retry_in: at - now,
                    last: state.last_failure.clone(),
                });
            }
        }
        if let Endpoint::Command { argv, dir } = &self.endpoint {
            match self.spawn(argv, dir.as_ref()).await {
                Ok(running) => state.running = Some(running),
                Err(err) => {
                    self.fail(state, err.to_string()).await;
                    return Err(err);
                }
            }
        }
        state.starts += 1;
        let init = self.init.clone();
        let result = match self
            .exchange(state, "initialize", &init, self.timeout("initialize"))
            .await
        {
            Ok(result) => result,
            Err(err) => {
                let why = format!("initialize failed: {err}");
                self.fail(state, why.clone()).await;
                return Err(match err {
                    CallError::Remote { code, message } => CallError::Remote {
                        code,
                        message: format!("initialize: {message}"),
                    },
                    other => other,
                });
            }
        };
        match PluginInfo::parse(&result) {
            Ok(info) => {
                if self.quiet {
                    debug!(plugin = %self.name, name = %info.name, "Scaling plugin started");
                } else {
                    info!(
                        plugin = %self.name,
                        name = %info.name,
                        version = %info.version,
                        kinds = ?info.kinds,
                        "Scaling plugin started"
                    );
                }
                state.info = Some(info);
                Ok(())
            }
            Err(err) => {
                let why = format!("initialize: {err}");
                self.fail(state, why.clone()).await;
                Err(CallError::Malformed(why))
            }
        }
    }

    fn tail_note(&self) -> String {
        let tail = self.stderr_tail();
        if tail.is_empty() {
            String::new()
        } else {
            let start = tail.len().saturating_sub(400);
            let start = (start..=tail.len())
                .find(|i| tail.is_char_boundary(*i))
                .unwrap_or(tail.len());
            format!("; stderr: {}", &tail[start..])
        }
    }

    /// Send one request and read its response, on a started plugin.
    async fn exchange(
        &self,
        state: &mut State,
        method: &str,
        params: &Value,
        timeout: Duration,
    ) -> Result<Value, CallError> {
        state.next_id += 1;
        let id = state.next_id;
        debug!(plugin = %self.name, method, id, params = %redact(params), "plugin call");
        match &self.endpoint {
            Endpoint::Url(url) => {
                let body =
                    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
                let sent = self.http.post(url).json(&body).timeout(timeout).send();
                let response = match sent.await {
                    Ok(response) => response,
                    Err(err) if err.is_timeout() => {
                        return Err(CallError::Timeout {
                            method: method.to_string(),
                            after: timeout,
                        })
                    }
                    Err(err) => return Err(CallError::Transport(format!("POST {url}: {err}"))),
                };
                let status = response.status();
                let text = match tokio::time::timeout(timeout, response.text()).await {
                    Ok(Ok(text)) => text,
                    Ok(Err(err)) => return Err(CallError::Transport(format!("POST {url}: {err}"))),
                    Err(_) => {
                        return Err(CallError::Timeout {
                            method: method.to_string(),
                            after: timeout,
                        })
                    }
                };
                if !status.is_success() {
                    return Err(CallError::Transport(format!(
                        "POST {url}: HTTP {status}: {}",
                        text.chars().take(200).collect::<String>()
                    )));
                }
                let value: Value = serde_json::from_str(&text).map_err(|err| {
                    CallError::Malformed(format!(
                        "not JSON ({err}): {}",
                        text.chars().take(200).collect::<String>()
                    ))
                })?;
                response_result(value, id)
            }
            Endpoint::Command { .. } => {
                let Some(running) = state.running.as_mut() else {
                    return Err(CallError::Crashed("the plugin is not running".into()));
                };
                let line = request_line(id, method, params);
                if let Err(err) = running.stdin.write_all(line.as_bytes()).await {
                    return Err(CallError::Crashed(format!(
                        "cannot write to it ({err}){}",
                        self.tail_note()
                    )));
                }
                let _ = running.stdin.flush().await;
                let mut reply = String::new();
                let read = tokio::time::timeout(timeout, async {
                    loop {
                        reply.clear();
                        let n = running.stdout.read_line(&mut reply).await?;
                        if n == 0 || !reply.trim().is_empty() || reply.len() > MAX_REPLY_BYTES {
                            return Ok::<usize, std::io::Error>(n);
                        }
                    }
                })
                .await;
                match read {
                    Err(_) => Err(CallError::Timeout {
                        method: method.to_string(),
                        after: timeout,
                    }),
                    Ok(Err(err)) => Err(CallError::Crashed(format!("reading its reply: {err}"))),
                    Ok(Ok(0)) => {
                        let status =
                            tokio::time::timeout(Duration::from_millis(500), running.child.wait())
                                .await
                                .ok()
                                .and_then(Result::ok);
                        Err(CallError::Crashed(format!(
                            "{}{}",
                            status
                                .map(|status| format!("it exited with {status}"))
                                .unwrap_or_else(|| "it closed its stdout".to_string()),
                            self.tail_note()
                        )))
                    }
                    Ok(Ok(_)) => {
                        let value: Value = serde_json::from_str(reply.trim()).map_err(|err| {
                            CallError::Malformed(format!(
                                "not JSON ({err}): {}",
                                reply.trim().chars().take(200).collect::<String>()
                            ))
                        })?;
                        response_result(value, id)
                    }
                }
            }
        }
    }

    /// Call `method` with `params`, starting the plugin first if needed.
    /// A failure that leaves the program in doubt (a timeout, a crash, a
    /// malformed reply) restarts it on the next call, with backoff.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, CallError> {
        let mut state = self.state.lock().await;
        self.ensure_started(&mut state).await?;
        let timeout = self.timeout(method);
        match self.exchange(&mut state, method, &params, timeout).await {
            Ok(result) => {
                state.failures = 0;
                state.retry_at = None;
                Ok(result)
            }
            Err(err @ CallError::Remote { .. }) => {
                // The plugin answered: it is healthy.
                state.failures = 0;
                state.retry_at = None;
                Err(err)
            }
            Err(err) => {
                if self.quiet {
                    debug!(plugin = %self.name, method, "Scaling plugin call failed: {err}");
                } else {
                    warn!(plugin = %self.name, method, "Scaling plugin call failed: {err}");
                }
                self.fail(&mut state, err.to_string()).await;
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_blanks_credentials_and_join_tokens_test() {
        let value = json!({
            "config": { "region": "us-east-1", "apiKey": "abc", "nested": [{ "password": "p" }] },
            "join": { "token": "secret", "attributes": {} },
            "desired": 9,
        });
        let redacted = redact(&value);
        assert_eq!(redacted["config"]["region"], "us-east-1");
        assert_eq!(redacted["config"]["apiKey"], "<redacted>");
        assert_eq!(redacted["config"]["nested"][0]["password"], "<redacted>");
        assert_eq!(redacted["join"], "<redacted>");
        assert_eq!(redacted["desired"], 9);
        assert!(!redacted.to_string().contains("secret"));
    }

    #[test]
    fn plugin_info_parses_strictly_test() {
        let ok = PluginInfo::parse(&json!({
            "name": "aws-asg", "version": "0.4.0", "kinds": ["target", "target"],
            "capabilities": { "nodes": true },
            "policyConfigSchema": { "type": "object" },
        }))
        .unwrap();
        assert_eq!(ok.kinds, vec![Kind::Target]);
        assert!(ok.nodes);
        let cases = [
            (json!([]), "must be an object"),
            (json!({ "version": "1", "kinds": ["source"] }), "`name`"),
            (json!({ "name": "x", "version": "1" }), "needs `kinds`"),
            (
                json!({ "name": "x", "version": "1", "kinds": [] }),
                "is empty",
            ),
            (
                json!({ "name": "x", "version": "1", "kinds": ["metric"] }),
                "unknown kind",
            ),
            (
                json!({ "name": "x", "version": "1", "kinds": ["source"], "protocol": "legion-scaling/2" }),
                "this autoscaler speaks",
            ),
            (
                json!({ "name": "x", "version": "1", "kinds": ["source"], "policyConfigSchema": 3 }),
                "policyConfigSchema",
            ),
            (
                json!({ "name": "x", "version": "1", "kinds": ["source"],
                        "policyConfigSchema": { "$ref": "http://evil/schema" } }),
                "not local",
            ),
        ];
        for (value, expected) in cases {
            let err = PluginInfo::parse(&value).unwrap_err();
            assert!(err.contains(expected), "{value}: {err}");
        }
    }

    #[test]
    fn responses_match_their_requests_test() {
        assert_eq!(
            response_result(
                json!({ "jsonrpc": "2.0", "id": 3, "result": { "a": 1 } }),
                3
            ),
            Ok(json!({ "a": 1 }))
        );
        assert_eq!(
            response_result(
                json!({ "jsonrpc": "2.0", "id": 3, "error": { "code": -32601, "message": "nope" } }),
                3
            ),
            Err(CallError::Remote {
                code: -32601,
                message: "nope".into()
            })
        );
        // A parse error answers with id null.
        assert!(matches!(
            response_result(
                json!({ "jsonrpc": "2.0", "id": null, "error": { "code": -32700, "message": "parse" } }),
                3
            ),
            Err(CallError::Remote { code: -32700, .. })
        ));
        assert!(matches!(
            response_result(json!({ "jsonrpc": "2.0", "id": 2, "result": 1 }), 3),
            Err(CallError::Malformed(_))
        ));
        assert!(matches!(
            response_result(json!({ "jsonrpc": "2.0", "id": 3 }), 3),
            Err(CallError::Malformed(_))
        ));
        assert!(matches!(
            response_result(json!("hello"), 3),
            Err(CallError::Malformed(_))
        ));
    }

    fn script(body: &str) -> (tempfile::TempDir, Endpoint) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("plugin.sh");
        std::fs::write(&path, body).expect("write");
        (
            dir,
            Endpoint::Command {
                argv: vec!["sh".into(), path.display().to_string()],
                dir: None,
            },
        )
    }

    const GOOD: &str = r#"
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
  case "$line" in
    *'"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"name":"t","version":"1","kinds":["strategy"]}}\n' "$id" ;;
    *'"strategy/recommend"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"desired":5,"note":"ok"}}\n' "$id" ;;
    *'"sleep"'*) sleep 5 ;;
    *'"garbage"'*) echo 'this is not json' ;;
    *'"exit"'*) exit 3 ;;
    *) printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"method not found"}}\n' "$id" ;;
  esac
done
"#;

    #[tokio::test]
    async fn stdio_calls_restart_after_failures_test() {
        let (_dir, endpoint) = script(GOOD);
        let transport =
            Transport::new("t", endpoint, json!({})).with_timeout(Duration::from_millis(500));
        assert_eq!(
            transport.call("strategy/recommend", json!({})).await,
            Ok(json!({ "desired": 5, "note": "ok" }))
        );
        assert_eq!(transport.starts().await, 1);
        assert!(matches!(
            transport.call("nope", json!({})).await,
            Err(CallError::Remote { code: -32601, .. })
        ));
        // A timeout kills the program; the next call restarts it at once.
        assert!(matches!(
            transport.call("sleep", json!({})).await,
            Err(CallError::Timeout { .. })
        ));
        assert!(transport
            .call("strategy/recommend", json!({}))
            .await
            .is_ok());
        assert_eq!(transport.starts().await, 2);
        // A malformed reply.
        assert!(matches!(
            transport.call("garbage", json!({})).await,
            Err(CallError::Malformed(_))
        ));
        assert!(transport
            .call("strategy/recommend", json!({}))
            .await
            .is_ok());
        // A crash, reported with the exit status.
        let err = transport.call("exit", json!({})).await.unwrap_err();
        assert!(
            matches!(&err, CallError::Crashed(why) if why.contains("exit")),
            "{err}"
        );
        assert!(transport
            .call("strategy/recommend", json!({}))
            .await
            .is_ok());
        assert_eq!(transport.starts().await, 4);
    }

    #[tokio::test]
    async fn repeated_failures_back_off_test() {
        let (_dir, endpoint) = script("exit 1\n");
        let transport = Transport::new("t", endpoint, json!({}));
        // The first failure restarts at once on the next call; the second
        // waits out a backoff.
        assert!(transport
            .call("strategy/recommend", json!({}))
            .await
            .is_err());
        assert!(transport
            .call("strategy/recommend", json!({}))
            .await
            .is_err());
        let err = transport
            .call("strategy/recommend", json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, CallError::Restarting { .. }), "{err}");
        assert_eq!(transport.starts().await, 2);
    }

    #[tokio::test]
    async fn a_missing_program_is_a_transport_error_test() {
        let transport = Transport::new(
            "t",
            Endpoint::Command {
                argv: vec!["/nonexistent/legion-plugin".into()],
                dir: None,
            },
            json!({}),
        );
        let err = transport
            .call("strategy/recommend", json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, CallError::Transport(why) if why.contains("cannot start")),
            "{err}"
        );
    }
}
