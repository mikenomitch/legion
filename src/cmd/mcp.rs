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
//! `legion_instance_create`, `legion_instance_stop`, `legion_instance_start`,
//! `legion_instance_destroy`) are gated behind `--allow-write`, mirroring the
//! `read_only_safe` flag in the capabilities catalog. Even with
//! `--allow-write`, `legion_state_query` always runs in read-only mode (no SQL
//! writes).

use crate::cmd::helpers::connect_manager_client;
use crate::cmd::output::CliError;
use crate::prelude::*;
use crate::structs::{
    exec_client_frame, exec_server_frame, CreateInstanceRequest, CreateInstancesRequest,
    DestroyInstanceRequest, DestroyJobRequest, ExecClientFrame, ExecInput, ExecStart,
    GetAllocationLogsRequest, GetInstanceRequest, GetJobInfoRequest, GetJobSpecRequest,
    GetLedgerRequest, GetNodesRequest, GetRolloutStatusRequest, GetStateStoreSchemaRequest,
    JobConfig, ListEventsRequest, ListInstancesRequest, ListJobActionsRequest, ListJobsRequest,
    ListRolloutsRequest, ListServicesRequest, QueryStateStoreRequest, ReplaceInstanceRequest,
    ResolveServiceRequest, RunJobRequest, StartInstanceRequest, StopInstanceRequest,
    StopJobRequest,
};
use crate::task_plugins::validate_job_config;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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
            name: "legion_acl_whoami",
            description: "Show whether ACLs are enabled and what access the current credentials grant. Use this to diagnose PermissionDenied errors.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_events",
            description: "List recent scheduler events (evals, placements, node/job lifecycle, rollouts). Poll with after_id set to the returned latest_id to tail the stream incrementally.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "after_id": { "type": "integer", "description": "Only events with ID greater than this cursor." },
                    "limit": { "type": "integer", "description": "Max events to return (default 100, max 1000)." },
                    "kind": { "type": "string", "description": "Kind filter, exact or category prefix (e.g. 'eval' matches eval.*)." },
                    "job_id": { "type": "string", "description": "Filter by exact job ID." },
                    "node_id": { "type": "string", "description": "Filter by exact node ID." },
                    "level": { "type": "string", "enum": ["info", "warn", "error"], "description": "Filter by severity." }
                },
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_job_logs",
            description: "Fetch captured stdout/stderr for a job's allocations (or one allocation). Use this to diagnose task failures.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Job name to gather allocation logs for." },
                    "allocation_id": { "type": "integer", "description": "Optional: read a single allocation id instead of a whole job." },
                    "stream": { "type": "string", "enum": ["stdout", "stderr", "all"], "description": "Which stream to read (default all)." },
                    "max_bytes": { "type": "integer", "description": "Max bytes per stream (0 = worker default cap)." },
                    "tail": { "type": "boolean", "description": "Return trailing bytes instead of leading bytes." }
                },
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_service_list",
            description: "List declared services with their healthy/total endpoint counts. Use this to discover what a job can connect to.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_service_resolve",
            description: "Resolve a service to its endpoint addresses (address, port, allocation, node, health). Healthy endpoints only unless include_unhealthy is set.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "service": { "type": "string", "description": "Service name (service.name, defaulting to the task name)." },
                    "include_unhealthy": { "type": "boolean", "description": "Also return endpoints failing health checks (default false)." }
                },
                "required": ["service"],
                "additionalProperties": false
            }),
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
            description: "Create keyed-mode stateful instance(s) for a job's keyed task (never-recycled ids). A single instance_id (or neither instance_ids nor count) uses CreateInstance; instance_ids and/or count use the atomic all-or-nothing batch CreateInstances. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string", "description": "Job name." },
                    "group": { "type": "string", "description": "Task group, when the keyed task is ambiguous." },
                    "task": { "type": "string", "description": "Task name, when the keyed task is ambiguous." },
                    "instance_id": { "type": "string", "description": "Explicit instance key ([A-Za-z0-9._-]{1,128}); server-minted when omitted." },
                    "instance_ids": { "type": "array", "items": { "type": "string" }, "description": "Batch of explicit instance keys, created atomically (all-or-nothing). Mutually exclusive with instance_id." },
                    "count": { "type": "integer", "minimum": 1, "description": "Create this many additional server-minted instances (atomic with instance_ids)." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["job_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_instance_stop",
            description: "Stop (pause) a keyed-mode instance: stop its allocation, retain the row and storage handle, keep the id owned. Reversible via legion_instance_start. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "instance_id": { "type": "string", "description": "Instance id to stop." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["instance_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_instance_start",
            description: "Start (resume) a stopped keyed-mode instance: re-place it with the same id and retained storage handle. The owning job must be running. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "instance_id": { "type": "string", "description": "Instance id to start." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["instance_id"],
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
        ToolDef {
            name: "legion_instance_replace",
            description: "Operator override for the node-loss confirmation gate: force-replace a stateful instance's allocation stuck on a LOST node without waiting for stateful.onLost.replaceAfter (K8s analog: force-delete a pod). DANGER: split-brain risk if the node is only partitioned and has not self-fenced yet. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "instance_id": { "type": "string", "description": "Instance id whose lost-node allocation to force-replace." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["instance_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_action_list",
            description: "List the declared actions (pre-baked, parameterized commands) on a job's tasks, including each action's parameters.",
            input_schema: || json!({
                "type": "object",
                "properties": { "job": { "type": "string", "description": "Job name." } },
                "required": ["job"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_action_run",
            description: "Run a declared action against a running allocation and return its combined output and exit code. One-shot (no interactive tty). Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job": { "type": "string", "description": "Job name (used to pick the newest running allocation when allocation_id is omitted)." },
                    "action": { "type": "string", "description": "Action name." },
                    "allocation_id": { "type": "integer", "description": "Target allocation id; defaults to the newest running allocation of the job." },
                    "params": { "type": "object", "description": "Action parameters as a string->string map.", "additionalProperties": { "type": "string" } }
                },
                "required": ["job", "action"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_exec_run",
            description: "Run a one-shot command inside a running allocation and return its stdout, stderr, and exit code. No interactive tty. Mutating; requires exec:write.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "allocation_id": { "type": "integer", "description": "Target allocation id." },
                    "command": { "type": "array", "items": { "type": "string" }, "description": "Command argv; first element is the program." },
                    "task": { "type": "string", "description": "Task name, for multi-task allocations." }
                },
                "required": ["allocation_id", "command"],
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
        "legion_acl_whoami" => {
            let mut client = connect().await?;
            grpc(client.who_am_i(crate::structs::WhoAmIRequest {})).await
        }
        "legion_action_list" => {
            let job = str_arg(&args, "job")?;
            let mut client = connect().await?;
            grpc(client.list_job_actions(ListJobActionsRequest { job })).await
        }
        "legion_action_run" => {
            let job = str_arg(&args, "job")?;
            let action = str_arg(&args, "action")?;
            let params: std::collections::HashMap<String, String> = args
                .get("params")
                .and_then(Value::as_object)
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();
            let mut client = connect().await?;
            let allocation_id = match args.get("allocation_id").and_then(Value::as_i64) {
                Some(id) => id,
                None => newest_alloc_for_job(&mut client, &job).await?,
            };
            let start = ExecStart {
                allocation_id,
                task_name: String::new(),
                command: Vec::new(),
                action,
                action_params: params,
                tty: false,
                winsize: None,
                bearer_token: String::new(),
            };
            run_exec_oneshot(&mut client, start).await
        }
        "legion_exec_run" => {
            let allocation_id = args
                .get("allocation_id")
                .and_then(Value::as_i64)
                .ok_or_else(|| "missing integer argument: allocation_id".to_string())?;
            let command: Vec<String> = args
                .get("command")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            if command.is_empty() {
                return Err("command must be a non-empty array".to_string());
            }
            let start = ExecStart {
                allocation_id,
                task_name: opt_str_arg(&args, "task").unwrap_or_default(),
                command,
                action: String::new(),
                action_params: std::collections::HashMap::new(),
                tty: false,
                winsize: None,
                bearer_token: String::new(),
            };
            let mut client = connect().await?;
            run_exec_oneshot(&mut client, start).await
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
            let instance_ids: Vec<String> = args
                .get("instance_ids")
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            let count = args.get("count").and_then(Value::as_u64).unwrap_or(0) as u32;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            // instance_ids/count select the atomic batch RPC; a lone
            // instance_id keeps the single-create semantics (incl. server
            // minting when absent).
            if !instance_ids.is_empty() || count > 0 {
                if instance_id.is_some() {
                    return Err(
                        "pass either instance_id (single create) or instance_ids/count \
                         (batch create), not both"
                            .into(),
                    );
                }
                grpc(client.create_instances(CreateInstancesRequest {
                    job_id,
                    group,
                    task,
                    instance_ids,
                    count,
                    idempotency_key,
                }))
                .await
            } else {
                grpc(client.create_instance(CreateInstanceRequest {
                    job_id,
                    group,
                    task,
                    instance_id,
                    idempotency_key,
                }))
                .await
            }
        }
        "legion_instance_stop" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.stop_instance(StopInstanceRequest {
                instance_id,
                idempotency_key,
            }))
            .await
        }
        "legion_instance_start" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.start_instance(StartInstanceRequest {
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
        "legion_instance_replace" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.replace_instance(ReplaceInstanceRequest {
                instance_id,
                idempotency_key,
            }))
            .await
        }
        "legion_events" => {
            let request = ListEventsRequest {
                after_id: args.get("after_id").and_then(Value::as_i64),
                limit: args
                    .get("limit")
                    .and_then(Value::as_u64)
                    .map(|limit| limit.min(u32::MAX as u64) as u32),
                kind: opt_str_arg(&args, "kind"),
                job_id: opt_str_arg(&args, "job_id"),
                node_id: opt_str_arg(&args, "node_id"),
                level: opt_str_arg(&args, "level"),
            };
            let mut client = connect().await?;
            grpc(client.list_events(request)).await
        }
        "legion_job_logs" => {
            let job = opt_str_arg(&args, "name").unwrap_or_default();
            let allocation_id = args.get("allocation_id").and_then(Value::as_i64);
            if job.is_empty() && allocation_id.is_none() {
                return Err("provide a job name or allocation_id".to_string());
            }
            let stream = opt_str_arg(&args, "stream").unwrap_or_else(|| "all".to_string());
            let max_bytes = args.get("max_bytes").and_then(Value::as_i64).unwrap_or(0);
            let tail = args.get("tail").and_then(Value::as_bool).unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.get_allocation_logs(GetAllocationLogsRequest {
                job,
                allocation_id,
                stream,
                max_bytes,
                tail,
                offset: 0,
            }))
            .await
        }
        "legion_service_list" => {
            let mut client = connect().await?;
            grpc(client.list_services(ListServicesRequest {})).await
        }
        "legion_service_resolve" => {
            let service = str_arg(&args, "service")?;
            let include_unhealthy = args
                .get("include_unhealthy")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.resolve_service(ResolveServiceRequest {
                service,
                include_unhealthy,
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
async fn connect() -> Result<crate::cmd::helpers::AuthedManagerClient, String> {
    connect_manager_client().await.map_err(|e| e.to_string())
}

/// Pick the newest running allocation for a job (for exec/action tools that
/// default the target).
async fn newest_alloc_for_job(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    job: &str,
) -> Result<i64, String> {
    let ledger = client
        .get_ledger(GetLedgerRequest {})
        .await
        .map_err(|s| s.message().to_string())?
        .into_inner();
    let mut best: Option<i64> = None;
    for node in ledger.nodes {
        for alloc in node.allocations {
            if alloc.job_id == job
                && matches!(alloc.status.as_str(), "placed" | "active" | "running")
            {
                best = Some(best.map_or(alloc.id, |b| b.max(alloc.id)));
            }
        }
    }
    best.ok_or_else(|| format!("no running allocation found for job `{job}`"))
}

/// Drive a one-shot exec/action session over `ExecAllocation`, collecting
/// stdout/stderr and the exit code. Interactive PTY is not supported here.
async fn run_exec_oneshot(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    start: ExecStart,
) -> Result<Value, String> {
    let (tx, rx) = tokio::sync::mpsc::channel::<ExecClientFrame>(8);
    tx.send(ExecClientFrame {
        frame: Some(exec_client_frame::Frame::Start(start)),
    })
    .await
    .ok();
    // No stdin; signal EOF immediately.
    tx.send(ExecClientFrame {
        frame: Some(exec_client_frame::Frame::Stdin(ExecInput {
            data: Vec::new(),
            eof: true,
        })),
    })
    .await
    .ok();
    drop(tx);

    let mut inbound = client
        .exec_allocation(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .map_err(|s| s.message().to_string())?
        .into_inner();

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut exit_code = -1i32;
    let mut exit_message = String::new();
    while let Some(frame) = inbound
        .message()
        .await
        .map_err(|s| s.message().to_string())?
    {
        match frame.frame {
            Some(exec_server_frame::Frame::Stdout(o)) => stdout.extend_from_slice(&o.data),
            Some(exec_server_frame::Frame::Stderr(o)) => stderr.extend_from_slice(&o.data),
            Some(exec_server_frame::Frame::Exit(e)) => {
                exit_code = e.code;
                exit_message = e.message;
                break;
            }
            _ => {}
        }
    }

    Ok(json!({
        "exit_code": exit_code,
        "stdout": String::from_utf8_lossy(&stdout),
        "stderr": String::from_utf8_lossy(&stderr),
        "message": exit_message,
    }))
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
        assert!(!names.contains(&"legion_instance_stop"));
        assert!(!names.contains(&"legion_instance_start"));
        assert!(!names.contains(&"legion_instance_destroy"));
    }

    #[test]
    fn job_logs_tool_is_read_only() {
        let names: Vec<&str> = available_tools(false).iter().map(|t| t.name).collect();
        assert!(names.contains(&"legion_job_logs"));
    }

    #[test]
    fn service_discovery_tools_are_read_only() {
        let names: Vec<&str> = available_tools(false).iter().map(|t| t.name).collect();
        assert!(names.contains(&"legion_service_list"));
        assert!(names.contains(&"legion_service_resolve"));
    }

    #[test]
    fn service_resolve_schema_requires_the_service_name() {
        let tool = tool_defs()
            .into_iter()
            .find(|tool| tool.name == "legion_service_resolve")
            .expect("legion_service_resolve");
        let schema = (tool.input_schema)();
        assert_eq!(schema["required"], json!(["service"]));
        assert!(schema["properties"]["include_unhealthy"].is_object());
    }

    #[tokio::test]
    async fn service_resolve_rejects_a_missing_service_argument() {
        let req = json!({
            "jsonrpc": "2.0", "id": 6, "method": "tools/call",
            "params": { "name": "legion_service_resolve", "arguments": {} }
        })
        .to_string();
        let resp = handle_message(&req, false).await.expect("response");
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("service"), "{text}");
    }

    #[test]
    fn allow_write_exposes_mutating_tools() {
        let names: Vec<&str> = available_tools(true).iter().map(|t| t.name).collect();
        assert!(names.contains(&"legion_job_run"));
        assert!(names.contains(&"legion_job_stop"));
        assert!(names.contains(&"legion_instance_create"));
        assert!(names.contains(&"legion_instance_stop"));
        assert!(names.contains(&"legion_instance_start"));
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
