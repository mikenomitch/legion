//! `legion mcp serve` — a Model Context Protocol (MCP) server for Legion.
//!
//! MCP is the emerging standard for exposing tools to LLM agents. This server
//! speaks MCP over stdio (newline-delimited JSON-RPC 2.0) and exposes Legion's
//! operations as MCP tools, so any MCP-capable agent can discover and drive a
//! Legion cluster without hand-rolling a gRPC client or scraping CLI text.
//!
//! ## Transport
//!
//! One JSON-RPC message per line on stdin; one response line on stdout. This
//! is the MCP stdio transport contract (messages must not contain embedded
//! newlines).
//!
//! ## Safety
//!
//! By default only read-only / introspection tools are exposed. Mutating tools
//! (`legion_job_run`, `legion_job_stop`, `legion_job_destroy`,
//! `legion_instance_create`, `legion_instance_destroy`) are gated behind
//! `--allow-write`, mirroring the `read_only_safe` flag in the capabilities
//! catalog. Even with `--allow-write`, `legion_state_query` always runs in
//! read-only mode (no SQL writes).

use crate::cmd::helpers::connect_manager_client;
use crate::cmd::output::CliError;
use crate::prelude::*;
use crate::structs::manager_client::ManagerClient;
use crate::structs::{
    CreateInstanceRequest, DestroyInstanceRequest, DestroyJobRequest, GetInstanceRequest,
    GetJobInfoRequest, GetJobSpecRequest, GetNodesRequest, GetRolloutStatusRequest,
    GetStateStoreSchemaRequest, JobConfig, ListInstancesRequest, ListJobsRequest,
    ListRolloutsRequest, QueryStateStoreRequest, RunJobRequest, StopJobRequest,
};
use crate::task_plugins::validate_job_config;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tonic::transport::Channel;

/// MCP protocol revision this server implements.
const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

/// Build the clap configuration for `legion mcp`.
pub fn config() -> Command {
    let serve_cmd = Command::new("serve")
        .about("Run an MCP server over stdio for agent integrations")
        .arg(
            Arg::new("allow-write")
                .long("allow-write")
                .help("Expose mutating tools (run/stop/destroy). Off by default.")
                .action(ArgAction::SetTrue),
        );

    Command::new("mcp")
        .about("Model Context Protocol server for AI agents")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(serve_cmd)
}

/// Handle `legion mcp ...`.
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("serve", args)) => serve(args.get_flag("allow-write")).await,
        _ => unreachable!(),
    }
}

/// A single MCP tool definition.
struct ToolDef {
    name: &'static str,
    description: &'static str,
    /// JSON Schema for the tool's arguments.
    input_schema: fn() -> Value,
    /// Whether the tool mutates cluster state (and so requires `--allow-write`).
    mutates: bool,
}

fn empty_schema() -> Value {
    json!({ "type": "object", "properties": {}, "additionalProperties": false })
}

fn name_arg_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "name": { "type": "string", "description": "Job name." } },
        "required": ["name"],
        "additionalProperties": false
    })
}

/// The full tool catalog. Read-only tools are always available; mutating tools
/// are filtered out unless `--allow-write` is set.
fn tool_defs() -> Vec<ToolDef> {
    vec![
        ToolDef {
            name: "legion_capabilities",
            description: "List Legion's agent-callable operations and whether each mutates state.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_job_list",
            description: "List all jobs and their status.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_job_info",
            description: "Inspect a single job's current state.",
            input_schema: name_arg_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_job_spec",
            description: "Fetch the source submissions that define a job.",
            input_schema: name_arg_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_job_schema",
            description: "Get the JSON Schema for jobspecs and built-in task kinds. Use this before authoring a jobspec.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_job_validate",
            description: "Validate a rendered jobspec (JSON) without submitting it.",
            input_schema: || json!({
                "type": "object",
                "properties": { "spec_json": { "type": "string", "description": "Rendered jobspec as a JSON string." } },
                "required": ["spec_json"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_rollout_list",
            description: "List rollouts, optionally filtered by job id.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string", "description": "Filter by job id." },
                    "active_only": { "type": "boolean", "description": "Only active rollouts." }
                },
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_rollout_status",
            description: "Inspect rollout progress and allocation health by rollout id.",
            input_schema: || json!({
                "type": "object",
                "properties": { "id": { "type": "integer", "description": "Rollout id." } },
                "required": ["id"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_instance_list",
            description: "List a job's stateful instances (slot and keyed, including terminated tombstones).",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string", "description": "Job name." },
                    "group": { "type": "string", "description": "Optional task-group filter." }
                },
                "required": ["job_id"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_instance_get",
            description: "Inspect one stateful instance by id (state, slot, current allocation, storage handle).",
            input_schema: || json!({
                "type": "object",
                "properties": { "instance_id": { "type": "string", "description": "Instance id." } },
                "required": ["instance_id"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_node_list",
            description: "List worker nodes and their health.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_state_query",
            description: "Run a read-only SQL query against the manager state store. Use legion_state_schema first to learn the tables.",
            input_schema: || json!({
                "type": "object",
                "properties": { "sql": { "type": "string", "description": "Read-only SQL statement." } },
                "required": ["sql"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_state_schema",
            description: "Describe the manager state store's tables and columns.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_job_run",
            description: "Submit or update a job from a rendered jobspec (JSON). Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "spec_json": { "type": "string", "description": "Rendered jobspec as a JSON string." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." },
                    "force_reschedule": { "type": "boolean", "description": "Reschedule even if unchanged." }
                },
                "required": ["spec_json"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_job_stop",
            description: "Stop a running job by name. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Job name." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["name"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_job_destroy",
            description: "Destroy a job and its allocations by name. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Job name." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["name"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_instance_create",
            description: "Create a keyed-mode stateful instance for a job's keyed task (never-recycled id). Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string", "description": "Job name." },
                    "group": { "type": "string", "description": "Task group, when the keyed task is ambiguous." },
                    "task": { "type": "string", "description": "Task name, when the keyed task is ambiguous." },
                    "instance_id": { "type": "string", "description": "Explicit instance key ([A-Za-z0-9._-]{1,128}); server-minted when omitted." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["job_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_instance_destroy",
            description: "Destroy a keyed-mode instance: stop its allocation, drop its storage, tombstone the id forever. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "instance_id": { "type": "string", "description": "Instance id to destroy." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["instance_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
    ]
}

/// Tools available given the write policy.
fn available_tools(allow_write: bool) -> Vec<ToolDef> {
    tool_defs()
        .into_iter()
        .filter(|tool| allow_write || !tool.mutates)
        .collect()
}

/// Run the stdio MCP server loop until stdin closes.
async fn serve(allow_write: bool) -> R<()> {
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();

    info!(allow_write, "legion mcp server started");

    while let Some(line) = reader.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let response = handle_message(line, allow_write).await;
        if let Some(response) = response {
            let mut text = serde_json::to_string(&response)?;
            text.push('\n');
            stdout.write_all(text.as_bytes()).await?;
            stdout.flush().await?;
        }
    }

    Ok(())
}

/// Parse and dispatch one JSON-RPC message. Returns `None` for notifications
/// (no `id`), which must not be answered.
async fn handle_message(line: &str, allow_write: bool) -> Option<Value> {
    let parsed: Value = match serde_json::from_str(line) {
        Ok(value) => value,
        Err(err) => {
            return Some(json_rpc_error(
                Value::Null,
                -32700,
                &format!("parse error: {err}"),
            ));
        }
    };

    let id = parsed.get("id").cloned();
    let method = parsed.get("method").and_then(Value::as_str).unwrap_or("");
    let params = parsed.get("params").cloned().unwrap_or(Value::Null);

    // Notifications carry no id and expect no response.
    let is_notification = id.is_none();

    match method {
        "initialize" => Some(json_rpc_result(id, initialize_result())),
        "tools/list" => Some(json_rpc_result(id, tools_list_result(allow_write))),
        "tools/call" => Some(json_rpc_result(
            id,
            tools_call_result(params, allow_write).await,
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

fn initialize_result() -> Value {
    json!({
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "capabilities": { "tools": {} },
        "serverInfo": { "name": "legion", "version": env!("CARGO_PKG_VERSION") }
    })
}

fn tools_list_result(allow_write: bool) -> Value {
    let tools = available_tools(allow_write)
        .into_iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "inputSchema": (tool.input_schema)(),
            })
        })
        .collect::<Vec<_>>();
    json!({ "tools": tools })
}

/// Execute a `tools/call`. Tool-level failures are reported inside the result
/// with `isError: true` (per MCP convention), not as JSON-RPC errors.
async fn tools_call_result(params: Value, allow_write: bool) -> Value {
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

    // Enforce the write policy regardless of what tools/list advertised.
    if !allow_write && tool_defs().iter().any(|t| t.name == name && t.mutates) {
        return tool_error(&format!(
            "tool {name} is mutating and requires the server to be started with --allow-write"
        ));
    }

    match dispatch_tool(name, arguments).await {
        Ok(value) => tool_success(&value),
        Err(err) => tool_error(&err),
    }
}

/// Run a tool and return its data payload as JSON, or a human-readable error
/// string on failure.
async fn dispatch_tool(name: &str, args: Value) -> Result<Value, String> {
    match name {
        "legion_capabilities" => {
            Ok(serde_json::to_value(crate::cmd::capabilities::catalog())
                .map_err(|e| e.to_string())?)
        }
        "legion_job_schema" => Ok(crate::cmd::job::build_jobspec_schema()),
        "legion_job_validate" => validate_tool(args),
        "legion_job_list" => {
            let mut client = connect().await?;
            grpc(client.list_jobs(ListJobsRequest::default())).await
        }
        "legion_job_info" => {
            let name = str_arg(&args, "name")?;
            let mut client = connect().await?;
            grpc(client.get_job_info(GetJobInfoRequest { name })).await
        }
        "legion_job_spec" => {
            let name = str_arg(&args, "name")?;
            let mut client = connect().await?;
            grpc(client.get_job_spec(GetJobSpecRequest { name })).await
        }
        "legion_rollout_list" => {
            let job_id = opt_str_arg(&args, "job_id");
            let active_only = args
                .get("active_only")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.list_rollouts(ListRolloutsRequest {
                job_id,
                active_only,
                ..Default::default()
            }))
            .await
        }
        "legion_rollout_status" => {
            let id = args
                .get("id")
                .and_then(Value::as_i64)
                .ok_or_else(|| "missing integer argument: id".to_string())?;
            let mut client = connect().await?;
            grpc(client.get_rollout_status(GetRolloutStatusRequest { id })).await
        }
        "legion_node_list" => {
            let mut client = connect().await?;
            grpc(client.get_nodes(GetNodesRequest::default())).await
        }
        "legion_instance_list" => {
            let job_id = str_arg(&args, "job_id")?;
            let group = opt_str_arg(&args, "group");
            let mut client = connect().await?;
            grpc(client.list_instances(ListInstancesRequest { job_id, group })).await
        }
        "legion_instance_get" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let mut client = connect().await?;
            grpc(client.get_instance(GetInstanceRequest { instance_id })).await
        }
        "legion_instance_create" => {
            let job_id = str_arg(&args, "job_id")?;
            let group = opt_str_arg(&args, "group");
            let task = opt_str_arg(&args, "task");
            let instance_id = opt_str_arg(&args, "instance_id");
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.create_instance(CreateInstanceRequest {
                job_id,
                group,
                task,
                instance_id,
                idempotency_key,
            }))
            .await
        }
        "legion_instance_destroy" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.destroy_instance(DestroyInstanceRequest {
                instance_id,
                idempotency_key,
            }))
            .await
        }
        "legion_state_query" => {
            let sql = str_arg(&args, "sql")?;
            let mut client = connect().await?;
            // Always read-only over MCP.
            grpc(client.query_state_store(QueryStateStoreRequest {
                sql,
                allow_write: false,
            }))
            .await
        }
        "legion_state_schema" => {
            let mut client = connect().await?;
            grpc(client.get_state_store_schema(GetStateStoreSchemaRequest {})).await
        }
        "legion_job_run" => {
            let spec_json = str_arg(&args, "spec_json")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let force_reschedule = args
                .get("force_reschedule")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.run_job(RunJobRequest {
                parsed_jobspec: spec_json,
                submissions: vec![],
                priority: None,
                force_reschedule,
                idempotency_key,
            }))
            .await
        }
        "legion_job_stop" => {
            let name = str_arg(&args, "name")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.stop_job(StopJobRequest {
                name,
                priority: None,
                idempotency_key,
            }))
            .await
        }
        "legion_job_destroy" => {
            let name = str_arg(&args, "name")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.destroy_job(DestroyJobRequest {
                name,
                priority: None,
                idempotency_key,
            }))
            .await
        }
        other => Err(format!("unknown tool: {other}")),
    }
}

/// Validate a rendered jobspec locally (no manager required).
fn validate_tool(args: Value) -> Result<Value, String> {
    let spec_json = str_arg(&args, "spec_json")?;
    let config: JobConfig = serde_json::from_str(&spec_json)
        .map_err(|e| format!("jobspec is not a valid job configuration: {e}"))?;
    validate_job_config(&config).map_err(|e| format!("jobspec failed validation: {e}"))?;
    Ok(json!({
        "valid": true,
        "job": config.name,
        "tasks": config.tasks.len(),
    }))
}

/// Connect to the manager, mapping connection failures to a tool error string.
async fn connect() -> Result<ManagerClient<Channel>, String> {
    connect_manager_client().await.map_err(|e| e.to_string())
}

/// Execute a gRPC call and serialize the reply to JSON, mapping a gRPC
/// [`tonic::Status`] into a structured error string for the agent.
async fn grpc<T, F>(call: F) -> Result<Value, String>
where
    T: Serialize,
    F: std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
{
    match call.await {
        Ok(response) => serde_json::to_value(response.into_inner()).map_err(|e| e.to_string()),
        Err(status) => {
            let err = CliError::from_status("request failed", &status);
            Err(serde_json::to_string(&err).unwrap_or_else(|_| status.message().to_string()))
        }
    }
}

fn str_arg(args: &Value, key: &str) -> Result<String, String> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("missing string argument: {key}"))
}

fn opt_str_arg(args: &Value, key: &str) -> Option<String> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn tool_success(data: &Value) -> Value {
    let text = serde_json::to_string_pretty(data).unwrap_or_else(|_| data.to_string());
    json!({ "content": [{ "type": "text", "text": text }], "isError": false })
}

fn tool_error(message: &str) -> Value {
    json!({ "content": [{ "type": "text", "text": message }], "isError": true })
}

fn json_rpc_result(id: Option<Value>, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id.unwrap_or(Value::Null), "result": result })
}

fn json_rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_parses_serve() {
        let m = config()
            .try_get_matches_from(["mcp", "serve", "--allow-write"])
            .unwrap();
        let (name, sub) = m.subcommand().unwrap();
        assert_eq!(name, "serve");
        assert!(sub.get_flag("allow-write"));
    }

    #[test]
    fn read_only_hides_mutating_tools() {
        let names: Vec<&str> = available_tools(false).iter().map(|t| t.name).collect();
        assert!(names.contains(&"legion_job_list"));
        assert!(names.contains(&"legion_instance_list"));
        assert!(names.contains(&"legion_instance_get"));
        assert!(!names.contains(&"legion_job_run"));
        assert!(!names.contains(&"legion_job_destroy"));
        assert!(!names.contains(&"legion_instance_create"));
        assert!(!names.contains(&"legion_instance_destroy"));
    }

    #[test]
    fn allow_write_exposes_mutating_tools() {
        let names: Vec<&str> = available_tools(true).iter().map(|t| t.name).collect();
        assert!(names.contains(&"legion_job_run"));
        assert!(names.contains(&"legion_job_stop"));
        assert!(names.contains(&"legion_instance_create"));
        assert!(names.contains(&"legion_instance_destroy"));
    }

    #[tokio::test]
    async fn initialize_returns_protocol_version() {
        let req = json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize" }).to_string();
        let resp = handle_message(&req, false).await.expect("response");
        assert_eq!(resp["result"]["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert_eq!(resp["result"]["serverInfo"]["name"], "legion");
    }

    #[tokio::test]
    async fn tools_list_respects_write_policy() {
        let req = json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }).to_string();
        let resp = handle_message(&req, false).await.expect("response");
        let tools = resp["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
        assert!(names.contains(&"legion_capabilities"));
        assert!(!names.contains(&"legion_job_run"));
    }

    #[tokio::test]
    async fn notifications_get_no_response() {
        let req = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }).to_string();
        assert!(handle_message(&req, false).await.is_none());
    }

    #[tokio::test]
    async fn job_schema_tool_runs_without_manager() {
        let req = json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": { "name": "legion_job_schema", "arguments": {} }
        })
        .to_string();
        let resp = handle_message(&req, false).await.expect("response");
        assert_eq!(resp["result"]["isError"], false);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("Legion Jobspec"));
    }

    #[tokio::test]
    async fn validate_tool_reports_invalid_spec() {
        let req = json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": { "name": "legion_job_validate", "arguments": { "spec_json": "{not json}" } }
        })
        .to_string();
        let resp = handle_message(&req, false).await.expect("response");
        assert_eq!(resp["result"]["isError"], true);
    }

    #[tokio::test]
    async fn mutating_tool_blocked_without_allow_write() {
        let req = json!({
            "jsonrpc": "2.0", "id": 5, "method": "tools/call",
            "params": { "name": "legion_job_destroy", "arguments": { "name": "demo" } }
        })
        .to_string();
        let resp = handle_message(&req, false).await.expect("response");
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("--allow-write"));
    }
}
