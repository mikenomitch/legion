//! The MCP protocol layer (agent-governance plan §6.7, review finding AI4):
//! version negotiation, tool annotations, structured results, elicitation
//! for approval gates, and the two transports (stdio and Streamable HTTP).
//!
//! **Versions.** The server implements MCP `2025-06-18` and negotiates down:
//! a client asking for `2025-03-26` or `2024-11-05` gets that version, and
//! only what it defines — annotations from `2025-03-26`, `title`,
//! `outputSchema`, `structuredContent` and elicitation from `2025-06-18`.
//! A client asking for a version the server does not know gets the latest,
//! which the spec lets it reject.
//!
//! **Results.** Every tool result carries Legion's envelope as
//! `structuredContent` (`{ok: true, data}` or `{ok: false, error: {code,
//! message, hint, retryable, details}}`), matching the tool's `outputSchema`,
//! and the same JSON as text content for older clients.
//!
//! **Elicitation.** When a mutating tool comes back `APPROVAL_REQUIRED` and
//! the client declared the `elicitation` capability, the server asks the
//! client's user (`elicitation/create`) to get the request approved
//! (`legion approval approve <id>` by someone else) and choose "retry"; it
//! then retries the call with the approval. It never asks for a credential:
//! the approval itself is an approver's authorized action. Without
//! elicitation (or over HTTP) the typed error is returned as is.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};

use super::{available_tools, dispatch_tool, tool_defs, tool_schema, ToolDef};
use crate::prelude::R;
use tracing::{info, warn};

/// The newest MCP revision this server implements.
pub(super) const LATEST: &str = "2025-06-18";

/// Every revision the server can serve, newest first.
pub(super) const SUPPORTED: &[&str] = &["2025-06-18", "2025-03-26", "2024-11-05"];

/// How many times one tool call asks the user to approve before giving up.
const MAX_ELICITATIONS: usize = 3;

/// One client's negotiated state.
pub(super) struct Session {
    version: Mutex<String>,
    elicitation: AtomicBool,
}

impl Default for Session {
    fn default() -> Self {
        Session {
            version: Mutex::new(LATEST.to_string()),
            elicitation: AtomicBool::new(false),
        }
    }
}

impl Session {
    /// A session already at `version` (the HTTP transport, which learns the
    /// version from the `MCP-Protocol-Version` header).
    fn at(version: &str) -> Self {
        let session = Session::default();
        *session.version.lock().unwrap_or_else(|p| p.into_inner()) = version.to_string();
        session
    }

    fn version(&self) -> String {
        self.version
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Whether the negotiated version has `feature`'s revision or newer.
    /// Revisions are dates, so string order is age order.
    fn at_least(&self, revision: &str) -> bool {
        self.version().as_str() >= revision
    }
}

/// The client end for server-initiated requests (elicitation) on stdio.
pub(super) struct Peer {
    out: mpsc::Sender<Value>,
    pending: Mutex<HashMap<String, oneshot::Sender<Value>>>,
    next: AtomicU64,
}

impl Peer {
    fn new(out: mpsc::Sender<Value>) -> Self {
        Peer {
            out,
            pending: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
        }
    }

    /// Sends a request to the client and waits for its response message.
    async fn request(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = format!("legion-{}", self.next.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id.clone(), tx);
        self.out
            .send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .await
            .map_err(|_| "the client connection closed".to_string())?;
        rx.await
            .map_err(|_| "the client did not answer".to_string())
    }

    /// Routes a response message to the request waiting for it.
    fn resolve(&self, message: &Value) -> bool {
        let Some(id) = message.get("id").map(|id| match id {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        }) else {
            return false;
        };
        match self
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&id)
        {
            Some(waiter) => {
                let _ = waiter.send(message.clone());
                true
            }
            None => false,
        }
    }
}

/// Run the stdio transport until stdin closes. Requests are handled
/// concurrently, so a tool call waiting on an elicitation does not block
/// the client's answer to it.
pub(super) async fn serve_stdio(allow_write: bool) -> R<()> {
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin).lines();
    let (out_tx, mut out_rx) = mpsc::channel::<Value>(256);
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(message) = out_rx.recv().await {
            let Ok(mut text) = serde_json::to_string(&message) else {
                continue;
            };
            text.push('\n');
            if stdout.write_all(text.as_bytes()).await.is_err() || stdout.flush().await.is_err() {
                break;
            }
        }
    });
    let session = Arc::new(Session::default());
    let peer = Arc::new(Peer::new(out_tx.clone()));
    info!(allow_write, "legion mcp server started (stdio)");
    while let Some(line) = reader.next_line().await? {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        let parsed: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(err) => {
                let _ = out_tx
                    .send(json_rpc_error(
                        Value::Null,
                        -32700,
                        &format!("parse error: {err}"),
                    ))
                    .await;
                continue;
            }
        };
        // A response to one of our requests (no method, has an id).
        if parsed.get("method").is_none() && peer.resolve(&parsed) {
            continue;
        }
        let (session, peer, out_tx) = (session.clone(), peer.clone(), out_tx.clone());
        tokio::spawn(async move {
            if let Some(response) = handle_parsed(&session, Some(&peer), parsed, allow_write).await
            {
                let _ = out_tx.send(response).await;
            }
        });
    }
    drop(out_tx);
    let _ = writer.await;
    Ok(())
}

/// Parse and dispatch one JSON-RPC message on a fresh session (tests and
/// one-shot callers). `None` for notifications.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) async fn handle_line(line: &str, allow_write: bool) -> Option<Value> {
    let session = Session::default();
    match serde_json::from_str::<Value>(line) {
        Ok(parsed) => handle_parsed(&session, None, parsed, allow_write).await,
        Err(err) => Some(json_rpc_error(
            Value::Null,
            -32700,
            &format!("parse error: {err}"),
        )),
    }
}

async fn handle_parsed(
    session: &Session,
    peer: Option<&Peer>,
    parsed: Value,
    allow_write: bool,
) -> Option<Value> {
    let id = parsed.get("id").cloned();
    let method = parsed.get("method").and_then(Value::as_str).unwrap_or("");
    let params = parsed.get("params").cloned().unwrap_or(Value::Null);
    let is_notification = id.is_none();
    match method {
        "initialize" => Some(json_rpc_result(id, initialize(session, &params))),
        "tools/list" => Some(json_rpc_result(id, tools_list(session, allow_write))),
        "tools/call" => Some(json_rpc_result(
            id,
            tools_call(session, peer, params, allow_write).await,
        )),
        "ping" => Some(json_rpc_result(id, json!({}))),
        _ if is_notification => None,
        _ => Some(json_rpc_error(
            id.unwrap_or(Value::Null),
            -32601,
            &format!("method not found: {method}"),
        )),
    }
}

/// `initialize`: negotiate the version and record the client's
/// capabilities.
fn initialize(session: &Session, params: &Value) -> Value {
    let requested = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or("");
    let version = if SUPPORTED.contains(&requested) {
        requested
    } else {
        LATEST
    };
    *session.version.lock().unwrap_or_else(|p| p.into_inner()) = version.to_string();
    let elicitation = version >= "2025-06-18"
        && params
            .get("capabilities")
            .and_then(|caps| caps.get("elicitation"))
            .is_some_and(Value::is_object);
    session.elicitation.store(elicitation, Ordering::Relaxed);
    let mut server_info = json!({ "name": "legion", "version": env!("CARGO_PKG_VERSION") });
    if version >= "2025-06-18" {
        server_info["title"] = json!("Legion");
    }
    json!({
        "protocolVersion": version,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": server_info,
        "instructions": "Legion cluster tools. Read-only tools are always listed; mutating tools \
            need `legion mcp serve --allow-write`. Every result's structuredContent is Legion's \
            envelope ({ok, data} or {ok:false, error:{code, message, hint, retryable, details}}). \
            A write gated by an approval rule fails with code APPROVAL_REQUIRED and \
            details.approvalId: once someone else approves it, retry the same call with \
            approval_id. RATE_LIMITED errors carry details.retryAfterMs.",
    })
}

/// The output schema of every tool: Legion's envelope.
pub(super) fn envelope_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "ok": { "type": "boolean", "description": "true on success" },
            "data": { "description": "The tool's reply (the gRPC reply message as JSON) when ok" },
            "error": {
                "type": "object",
                "properties": {
                    "code": { "type": "string", "description": "gRPC code name or a typed Legion code (APPROVAL_REQUIRED, RATE_LIMITED, ...)" },
                    "message": { "type": "string" },
                    "hint": { "type": "string" },
                    "retryable": { "type": "boolean" },
                    "details": { "type": "object", "description": "Typed details: approvalId, requestDigest, retryAfterMs, ..." }
                },
                "required": ["code", "message", "retryable"]
            }
        },
        "required": ["ok"]
    })
}

/// Words in a tool name that make a mutating tool destructive.
const DESTRUCTIVE: &[&str] = &[
    "destroy",
    "delete",
    "stop",
    "revoke",
    "uninstall",
    "drain",
    "forget",
    "remove",
    "reset",
    "purge",
    "restart",
    "release",
    "rewrap",
    "evict",
    "replace",
    "raft_command",
];
/// Words in a mutating tool's name that make repeating it a no-op.
const IDEMPOTENT: &[&str] = &[
    "apply", "upsert", "set_", "put", "delete", "enable", "pause", "update", "settings", "config",
    "alias",
];
/// Tools that reach outside the cluster (git, app sources).
const OPEN_WORLD: &[&str] = &[
    "legion_gitops_sync",
    "legion_app_install",
    "legion_app_upgrade",
];

/// MCP tool annotations derived from a tool's metadata.
pub(super) fn annotations(tool: &ToolDef) -> Value {
    let schema = (tool.input_schema)();
    let takes_key = schema
        .get("properties")
        .and_then(Value::as_object)
        .is_some_and(|props| props.contains_key("idempotency_key"));
    let destructive = tool.mutates && DESTRUCTIVE.iter().any(|word| tool.name.contains(word));
    let idempotent =
        !tool.mutates || takes_key || IDEMPOTENT.iter().any(|word| tool.name.contains(word));
    json!({
        "title": title(tool.name),
        "readOnlyHint": !tool.mutates,
        "destructiveHint": destructive,
        "idempotentHint": idempotent,
        "openWorldHint": OPEN_WORLD.contains(&tool.name),
    })
}

/// `legion_job_run` -> `Job run`.
fn title(name: &str) -> String {
    let words = name.trim_start_matches("legion_").replace('_', " ");
    let mut chars = words.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// A tool's input schema as listed: mutating tools also take `approval_id`.
fn listed_input_schema(tool: &ToolDef) -> Value {
    let mut schema = tool_schema(tool);
    if tool.mutates {
        if let Some(props) = schema.get_mut("properties").and_then(Value::as_object_mut) {
            props.insert(
                "approval_id".into(),
                json!({
                    "type": "string",
                    "description": "Retry a call an approval rule held (APPROVAL_REQUIRED) with the granted approval's id."
                }),
            );
        }
    }
    schema
}

fn tools_list(session: &Session, allow_write: bool) -> Value {
    let annotate = session.at_least("2025-03-26");
    let structured = session.at_least("2025-06-18");
    let tools = available_tools(allow_write)
        .into_iter()
        .map(|tool| {
            let mut entry = json!({
                "name": tool.name,
                "description": tool.description,
                "inputSchema": listed_input_schema(&tool),
            });
            if annotate {
                entry["annotations"] = annotations(&tool);
            }
            if structured {
                entry["title"] = json!(title(tool.name));
                entry["outputSchema"] = envelope_schema();
            }
            entry
        })
        .collect::<Vec<_>>();
    json!({ "tools": tools })
}

/// An error string from a tool as Legion's error object: tools return the
/// CLI's typed error as JSON; anything else is a local failure.
fn error_object(message: &str) -> Value {
    match serde_json::from_str::<Value>(message) {
        Ok(Value::Object(object)) if object.contains_key("code") => Value::Object(object),
        _ => json!({ "code": "ToolError", "message": message, "retryable": false }),
    }
}

fn result_value(session: &Session, outcome: &Result<Value, String>) -> Value {
    let (envelope, text, is_error) = match outcome {
        Ok(data) => (
            json!({ "ok": true, "data": data }),
            serde_json::to_string_pretty(data).unwrap_or_else(|_| data.to_string()),
            false,
        ),
        Err(message) => (
            json!({ "ok": false, "error": error_object(message) }),
            message.clone(),
            true,
        ),
    };
    let mut result = json!({ "content": [{ "type": "text", "text": text }], "isError": is_error });
    if session.at_least("2025-06-18") {
        result["structuredContent"] = envelope;
    }
    result
}

/// `tools/call`. Tool-level failures are reported inside the result with
/// `isError: true` (per MCP convention), not as JSON-RPC errors.
async fn tools_call(
    session: &Session,
    peer: Option<&Peer>,
    params: Value,
    allow_write: bool,
) -> Value {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let mut arguments = params.get("arguments").cloned().unwrap_or(json!({}));
    // Enforce the write policy regardless of what tools/list advertised.
    if !allow_write && tool_defs().iter().any(|t| t.name == name && t.mutates) {
        return result_value(
            session,
            &Err(format!(
                "tool {name} is mutating and requires the server to be started with --allow-write"
            )),
        );
    }
    let mut approval = arguments
        .as_object_mut()
        .and_then(|args| args.remove("approval_id"))
        .and_then(|id| id.as_str().map(str::to_string));
    let mut asked = 0;
    loop {
        let outcome = Box::pin(call_with_approval(
            &name,
            arguments.clone(),
            approval.clone(),
        ))
        .await;
        let Err(message) = &outcome else {
            return result_value(session, &outcome);
        };
        let error = error_object(message);
        let code = error["code"].as_str().unwrap_or_default();
        let waiting = code == crate::manager::governance::APPROVAL_REQUIRED
            || (code == crate::manager::governance::APPROVAL_INVALID
                && error["message"]
                    .as_str()
                    .is_some_and(|m| m.contains("not been approved yet")));
        let Some(approval_id) = error["details"]["approvalId"]
            .as_str()
            .map(str::to_string)
            .filter(|_| waiting)
        else {
            return result_value(session, &outcome);
        };
        let Some(peer) = peer
            .filter(|_| session.elicitation.load(Ordering::Relaxed) && asked < MAX_ELICITATIONS)
        else {
            return result_value(session, &outcome);
        };
        asked += 1;
        if !elicit_retry(peer, &name, &error, &approval_id).await {
            return result_value(session, &outcome);
        }
        approval = Some(approval_id);
    }
}

async fn call_with_approval(
    name: &str,
    arguments: Value,
    approval: Option<String>,
) -> Result<Value, String> {
    // Boxed: `dispatch_tool`'s future holds every tool's state machine.
    let call = Box::pin(dispatch_tool(name, arguments));
    match approval {
        Some(id) => crate::cmd::governance::TASK_APPROVAL.scope(id, call).await,
        None => call.await,
    }
}

/// Asks the client's user to get `approval_id` approved and retry. True
/// when they chose to retry.
async fn elicit_retry(peer: &Peer, tool: &str, error: &Value, approval_id: &str) -> bool {
    let message = format!(
        "Legion needs a human approval before `{tool}` can run.\n\n{}\n\nAsk someone with \
         approval:write (not this agent's token) to run:\n\n    legion approval approve \
         {approval_id}\n\nthen choose \"Retry now\".",
        error["message"].as_str().unwrap_or_default()
    );
    let params = json!({
        "message": message,
        "requestedSchema": {
            "type": "object",
            "properties": {
                "decision": {
                    "type": "string",
                    "title": "Once the request is approved",
                    "enum": ["retry", "cancel"],
                    "enumNames": ["Retry now", "Cancel"]
                }
            },
            "required": ["decision"]
        }
    });
    match peer.request("elicitation/create", params).await {
        Ok(response) => {
            let result = &response["result"];
            result["action"] == "accept" && result["content"]["decision"] == "retry"
        }
        Err(err) => {
            warn!(error = %err, "elicitation failed");
            false
        }
    }
}

// ── Streamable HTTP ─────────────────────────────────────────────────────

/// Run the Streamable HTTP transport on `addr` (loopback only): `POST /mcp`
/// with one JSON-RPC message answers `application/json` (requests) or
/// `202 Accepted` (notifications and responses); `GET` and `DELETE` answer
/// `405` — the server opens no SSE streams and keeps no sessions, so
/// elicitation is not offered over HTTP. Browsers' cross-origin requests are
/// refused by the `Origin` check (DNS rebinding).
pub(super) async fn serve_http(addr: std::net::SocketAddr, allow_write: bool) -> R<()> {
    use hyper::server::conn::http1;
    use hyper_util::rt::TokioIo;
    if !addr.ip().is_loopback() {
        anyhow::bail!(
            "legion mcp serve --http binds loopback addresses only ({addr} is not); the HTTP \
             transport has no client authentication of its own, so every caller would act \
             with this server's token"
        );
    }
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, allow_write, "legion mcp server started (streamable http at /mcp)");
    eprintln!("legion mcp server listening on http://{addr}/mcp");
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let service =
                hyper::service::service_fn(move |request| http_request(request, allow_write));
            if let Err(err) = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!(error = %err, "mcp http connection ended");
            }
        });
    }
}

type HttpBody = http_body_util::Full<bytes::Bytes>;

fn http_response(
    status: u16,
    content_type: Option<&str>,
    body: String,
) -> http::Response<HttpBody> {
    let mut builder = http::Response::builder().status(status);
    if let Some(content_type) = content_type {
        builder = builder.header(http::header::CONTENT_TYPE, content_type);
    }
    builder
        .body(HttpBody::new(bytes::Bytes::from(body)))
        .unwrap_or_else(|_| http::Response::new(HttpBody::new(bytes::Bytes::new())))
}

/// Whether an `Origin` header (when present) is a loopback origin.
fn origin_allowed(origin: Option<&str>) -> bool {
    let Some(origin) = origin else {
        return true;
    };
    let host = origin
        .split("://")
        .nth(1)
        .unwrap_or(origin)
        .split(['/', ':'])
        .next()
        .unwrap_or_default();
    matches!(host, "localhost" | "127.0.0.1" | "[::1]" | "::1")
}

async fn http_request(
    request: http::Request<hyper::body::Incoming>,
    allow_write: bool,
) -> Result<http::Response<HttpBody>, std::convert::Infallible> {
    use http_body_util::BodyExt;
    let origin = request
        .headers()
        .get(http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    if !origin_allowed(origin.as_deref()) {
        return Ok(http_response(403, None, "origin not allowed".into()));
    }
    if request.uri().path() != "/mcp" {
        return Ok(http_response(
            404,
            None,
            "not found; the MCP endpoint is /mcp".into(),
        ));
    }
    if request.method() != http::Method::POST {
        return Ok(http_response(
            405,
            None,
            "this server opens no SSE stream".into(),
        ));
    }
    // The spec's default for a client that sends no version header.
    let version = request
        .headers()
        .get("mcp-protocol-version")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("2025-03-26")
        .to_string();
    if !SUPPORTED.contains(&version.as_str()) {
        return Ok(http_response(
            400,
            None,
            format!("unsupported MCP-Protocol-Version {version}; this server speaks {SUPPORTED:?}"),
        ));
    }
    let body = match request.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => return Ok(http_response(400, None, format!("unreadable body: {err}"))),
    };
    let parsed: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => {
            let error = json_rpc_error(Value::Null, -32700, &format!("parse error: {err}"));
            return Ok(http_response(
                400,
                Some("application/json"),
                error.to_string(),
            ));
        }
    };
    let session = Session::at(&version);
    match handle_parsed(&session, None, parsed, allow_write).await {
        Some(response) => Ok(http_response(
            200,
            Some("application/json"),
            response.to_string(),
        )),
        None => Ok(http_response(202, None, String::new())),
    }
}

pub(super) fn json_rpc_result(id: Option<Value>, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id.unwrap_or(Value::Null), "result": result })
}

pub(super) fn json_rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

#[cfg(test)]
mod mcp_protocol_test {
    use super::*;

    async fn call(session: &Session, message: Value) -> Value {
        handle_parsed(session, None, message, true)
            .await
            .expect("a response")
    }

    #[tokio::test]
    async fn initialize_negotiates_new_and_old_versions_test() {
        for (asked, expected) in [
            ("2025-06-18", "2025-06-18"),
            ("2025-03-26", "2025-03-26"),
            ("2024-11-05", "2024-11-05"),
            ("2099-01-01", LATEST),
        ] {
            let session = Session::default();
            let reply = call(
                &session,
                json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
                        "params": { "protocolVersion": asked, "capabilities": {} } }),
            )
            .await;
            assert_eq!(
                reply["result"]["protocolVersion"], expected,
                "asked {asked}"
            );
            assert_eq!(session.version(), expected);
        }
        // Elicitation is recorded only when declared, on 2025-06-18.
        let session = Session::default();
        call(
            &session,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": { "protocolVersion": "2025-06-18", "capabilities": { "elicitation": {} } } }),
        )
        .await;
        assert!(session.elicitation.load(Ordering::Relaxed));
        let old = Session::default();
        call(
            &old,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": { "protocolVersion": "2024-11-05", "capabilities": { "elicitation": {} } } }),
        )
        .await;
        assert!(!old.elicitation.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn tools_list_carries_annotations_and_output_schema_by_version_test() {
        let new = Session::at("2025-06-18");
        let reply = call(
            &new,
            json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
        )
        .await;
        let tools = reply["result"]["tools"].as_array().unwrap();
        let find = |name: &str| tools.iter().find(|t| t["name"] == name).cloned().unwrap();
        let destroy = find("legion_job_destroy");
        assert_eq!(destroy["annotations"]["readOnlyHint"], false);
        assert_eq!(destroy["annotations"]["destructiveHint"], true);
        assert_eq!(destroy["annotations"]["idempotentHint"], true);
        assert_eq!(destroy["outputSchema"]["required"], json!(["ok"]));
        assert!(destroy["inputSchema"]["properties"]["approval_id"].is_object());
        let list = find("legion_job_list");
        assert_eq!(list["annotations"]["readOnlyHint"], true);
        assert_eq!(list["annotations"]["destructiveHint"], false);
        assert!(list["inputSchema"]["properties"]
            .get("approval_id")
            .is_none());
        let run = find("legion_job_run");
        assert_eq!(run["annotations"]["destructiveHint"], false);
        assert_eq!(
            find("legion_gitops_sync")["annotations"]["openWorldHint"],
            true
        );
        assert_eq!(list["title"], "Job list");

        let old = Session::at("2024-11-05");
        let reply = call(
            &old,
            json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
        )
        .await;
        let tool = &reply["result"]["tools"][0];
        assert!(tool.get("annotations").is_none());
        assert!(tool.get("outputSchema").is_none());
        let mid = Session::at("2025-03-26");
        let reply = call(
            &mid,
            json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
        )
        .await;
        let tool = &reply["result"]["tools"][0];
        assert!(tool["annotations"].is_object());
        assert!(tool.get("outputSchema").is_none());
    }

    #[tokio::test]
    async fn tool_results_carry_the_envelope_as_structured_content_test() {
        let new = Session::at("2025-06-18");
        let ok = call(
            &new,
            json!({ "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                    "params": { "name": "legion_job_schema", "arguments": {} } }),
        )
        .await;
        assert_eq!(ok["result"]["isError"], false);
        assert_eq!(ok["result"]["structuredContent"]["ok"], true);
        assert!(ok["result"]["structuredContent"]["data"].is_object());
        let err = call(
            &new,
            json!({ "jsonrpc": "2.0", "id": 4, "method": "tools/call",
                    "params": { "name": "legion_job_validate", "arguments": { "spec_json": "{not json}" } } }),
        )
        .await;
        assert_eq!(err["result"]["isError"], true);
        assert_eq!(err["result"]["structuredContent"]["ok"], false);
        assert!(err["result"]["structuredContent"]["error"]["code"].is_string());
        let old = Session::at("2024-11-05");
        let ok = call(
            &old,
            json!({ "jsonrpc": "2.0", "id": 5, "method": "tools/call",
                    "params": { "name": "legion_job_schema", "arguments": {} } }),
        )
        .await;
        assert!(ok["result"].get("structuredContent").is_none());
    }

    #[test]
    fn typed_errors_keep_their_code_and_details_test() {
        let typed = r#"{"code":"APPROVAL_REQUIRED","message":"m","retryable":false,"details":{"approvalId":"apr_1"}}"#;
        let object = error_object(typed);
        assert_eq!(object["code"], "APPROVAL_REQUIRED");
        assert_eq!(object["details"]["approvalId"], "apr_1");
        assert_eq!(error_object("plain failure")["code"], "ToolError");
    }

    #[test]
    fn origins_must_be_loopback_test() {
        assert!(origin_allowed(None));
        assert!(origin_allowed(Some("http://localhost:3000")));
        assert!(origin_allowed(Some("http://127.0.0.1")));
        assert!(!origin_allowed(Some("https://evil.example")));
        assert!(!origin_allowed(Some("http://localhost.evil.example")));
    }

    #[tokio::test]
    async fn elicitation_round_trips_through_the_peer_test() {
        let (tx, mut rx) = mpsc::channel(4);
        let peer = Arc::new(Peer::new(tx));
        let asking = {
            let peer = peer.clone();
            tokio::spawn(async move {
                elicit_retry(
                    &peer,
                    "legion_job_destroy",
                    &json!({ "message": "approval required" }),
                    "apr_1",
                )
                .await
            })
        };
        let request = rx.recv().await.expect("an elicitation request");
        assert_eq!(request["method"], "elicitation/create");
        assert!(request["params"]["message"]
            .as_str()
            .unwrap()
            .contains("legion approval approve apr_1"));
        assert!(peer.resolve(&json!({
            "jsonrpc": "2.0", "id": request["id"],
            "result": { "action": "accept", "content": { "decision": "retry" } }
        })));
        assert!(asking.await.unwrap());
    }
}
