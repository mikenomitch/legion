//! `legion session` — the edge side of session mode (docs/session-mode.md,
//! "Starting at the edge").
//!
//! `nodes` asks a manager which nodes hold an envelope for a session task,
//! in the shape a picker (Envoy, HAProxy, or a platform's own code) needs.
//! `start`, `get`, and `stop` are the edge forms: they talk to one worker
//! directly with a session ticket (`legion boot-ticket create --scope
//! session`), the way a platform's picker does, so a start never touches a
//! manager. They exist for testing and scripts; a platform calls
//! `Worker.StartSession` from its own code.
//!
//! `run` is one-shot code execution (docs/session-runs.md): a fresh,
//! single-use session of a task with a `run` block, the caller's files in,
//! the command's output and result back. Through a manager by default, or
//! at one node with `--node` and a session ticket.

use crate::cmd::helpers::{connect_manager_client, json_flag, worker_plane_client};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    GetSessionRequest, ListSessionNodesReply, ListSessionNodesRequest, RunFile, RunSessionRequest,
    RunSessionResponse, SessionNode, StartSessionRequest, StopSessionRequest,
};

fn edge_args(command: Command) -> Command {
    command
        .arg(
            Arg::new("node")
                .long("node")
                .value_name("ADDR")
                .required(true)
                .help(
                    "Worker address, e.g. https://10.0.1.7:4848 (an `address` from `legion \
                     session nodes`). Verified against the cluster CA",
                ),
        )
        .arg(
            Arg::new("node-name")
                .long("node-name")
                .value_name("NODE")
                .help("Node id the worker at --node must prove it is"),
        )
        .arg(
            Arg::new("ca")
                .long("ca")
                .value_name("FILE")
                .help("Cluster CA bundle (PEM). Default: fetched from the manager with your token"),
        )
        .arg(
            Arg::new("ticket")
                .long("ticket")
                .value_name("TOKEN")
                .required(true)
                .help("Session ticket from `legion boot-ticket create --scope session`"),
        )
        .arg(json_flag())
}

pub fn config() -> Command {
    let nodes_cmd = Command::new("nodes")
        .about("List the nodes that hold an envelope for a session task")
        .arg(Arg::new("job").value_name("JOB").required(true))
        .arg(
            Arg::new("task")
                .long("task")
                .value_name("TASK")
                .help("The session task (default: the job's only one)"),
        )
        .arg(
            Arg::new("format")
                .long("format")
                .value_name("FORMAT")
                .value_parser(["table", "envoy-eds", "haproxy"])
                .help(
                    "envoy-eds: a ClusterLoadAssignment for Envoy's EDS. haproxy: `server` \
                     lines for a backend. Default: a table (or the JSON envelope with -j)",
                ),
        )
        .arg(json_flag());
    let start_cmd = edge_args(
        Command::new("start")
            .about("Start a session on one node with a session ticket (no manager call)")
            .arg(Arg::new("job").value_name("JOB").required(true))
            .arg(
                Arg::new("task")
                    .long("task")
                    .value_name("TASK")
                    .required(true)
                    .help("The session task (must match the ticket)"),
            )
            .arg(
                Arg::new("idempotency-key")
                    .long("idempotency-key")
                    .value_name("KEY")
                    .help(
                        "Node-local retry key: a repeat on the same node returns the same session",
                    ),
            )
            .arg(
                Arg::new("no-wait")
                    .long("no-wait")
                    .action(clap::ArgAction::SetTrue)
                    .help("Return once admitted (state `starting`) instead of once running"),
            )
            .arg(
                Arg::new("resume-id")
                    .long("resume-id")
                    .value_name("SESSION")
                    .help(
                        "Resume this store-durability session on --node from its manifest \
                         (it may have run or slept on another node)",
                    ),
            )
            .arg(
                Arg::new("force-move")
                    .long("force-move")
                    .action(clap::ArgAction::SetTrue)
                    .requires("resume-id")
                    .help(
                        "With --resume-id: move the session even when the node its manifest \
                         names did not answer (only once that node is known to be down). \
                         Without it, a move past a silent node is forced only when the \
                         managers list that node down or lost",
                    ),
            )
            .arg(
                Arg::new("owner")
                    .long("owner")
                    .value_name("ID|self")
                    .conflicts_with("resume-id")
                    .help(
                        "Start the session as a child of this unit (docs/child-sessions.md): \
                         it ends when its owner ends. An id of a session, instance, or \
                         allocation running on --node; or `self` inside a task, which asks \
                         this node's worker over the allocation's API socket with no ticket \
                         (then JOB is read as the task: `legion session start machine --owner \
                         self`). The owner's task must name the task in children.tasks",
                    ),
            )
            .arg(
                Arg::new("owner-kind")
                    .long("owner-kind")
                    .value_name("KIND")
                    .value_parser(["session", "instance", "allocation"])
                    .requires("owner")
                    .help("What --owner names. Default: worked out from the id"),
            ),
    )
    // `--owner self` needs neither a node nor a ticket nor --task.
    .mut_arg("node", |arg| arg.required(false))
    .mut_arg("ticket", |arg| arg.required(false))
    .mut_arg("task", |arg| arg.required(false));
    let children_cmd = Command::new("children")
        .about(
            "Inside a task: list the children this workload owns on its node, over the \
             allocation's API socket (docs/child-sessions.md)",
        )
        .arg(json_flag());
    let handoff_cmd = Command::new("handoff")
        .about(
            "Inside a task: hand an inbound connection this workload accepted to a port of one \
             of its children, so the worker moves the bytes and the workload leaves the path \
             (docs/child-sessions.md, \"Connection handoff\")",
        )
        .after_help(
            "The connection is an open file descriptor this command inherits, e.g. from a \
             shell or a parent process that accepted it:\n\n  \
             legion session handoff machine-q4n7c2rd.01jc0 --port http --fd 5\n\n\
             The command returns once the worker has taken the connection; with --wait it \
             returns when the connection has closed, and reports the bytes moved.",
        )
        .arg(Arg::new("id").value_name("CHILD").required(true).help(
            "The child's session id, as `legion session start <task> --owner self` printed it",
        ))
        .arg(
            Arg::new("port")
                .long("port")
                .value_name("LABEL")
                .required(true)
                .help("The child's port label (network.ports[].name)"),
        )
        .arg(
            Arg::new("fd")
                .long("fd")
                .value_name("N")
                .required(true)
                .value_parser(clap::value_parser!(i32).range(0..))
                .help("The file descriptor of the accepted connection (a TCP or Unix stream)"),
        )
        .arg(
            Arg::new("wait")
                .long("wait")
                .action(clap::ArgAction::SetTrue)
                .help("Return when the handed-off connection has closed"),
        )
        .arg(json_flag());
    let get_cmd = edge_args(
        Command::new("get")
            .about("Read a session from its node with a session ticket")
            .arg(Arg::new("id").value_name("SESSION").required(true)),
    );
    let stop_cmd = edge_args(
        Command::new("stop")
            .about("End a session on its node with a session ticket")
            .arg(Arg::new("id").value_name("SESSION").required(true))
            .arg(
                Arg::new("local")
                    .long("local")
                    .action(clap::ArgAction::SetTrue)
                    .help(
                        "Inside a task: end one of this workload's own children over the \
                         allocation's API socket, with no ticket",
                    ),
            ),
    )
    .mut_arg("node", |arg| {
        arg.required(false).required_unless_present("local")
    })
    .mut_arg("ticket", |arg| {
        arg.required(false).required_unless_present("local")
    });
    let run_cmd = Command::new("run")
        .about("Run once in a fresh, single-use session and return the output")
        .long_about(
            "Run once in a fresh, single-use session of a task with a `run` block \
             (docs/session-runs.md): start the session, stage --file and --input under \
             /legion/run/, execute the task's run.command, print its stdout and stderr, and end \
             the session. Exits with the command's status; 124 when the run was stopped by \
             run.wall, 125 when it was stopped for another reason.\n\nGoes through a manager \
             (job:write) by default. With --node and --ticket it talks to one worker directly, \
             with no manager on the path.",
        )
        .arg(Arg::new("job").value_name("JOB").required(true))
        .arg(
            Arg::new("task")
                .long("task")
                .value_name("TASK")
                .help("The session task (default: the job's only one that takes runs)"),
        )
        .arg(
            Arg::new("file")
                .long("file")
                .value_name("[NAME=]PATH")
                .action(clap::ArgAction::Append)
                .help(
                    "Stage a local file in the run directory, under its own name or NAME \
                     (e.g. --file main.py, --file data/in.csv=./sample.csv). Repeatable",
                ),
        )
        .arg(Arg::new("input").long("input").value_name("JSON").help(
            "A JSON document staged as input.json (its path is in LEGION_RUN_INPUT). \
                     `@file` reads a file, `-` reads stdin",
        ))
        .arg(
            Arg::new("get")
                .long("get")
                .value_name("PATH")
                .action(clap::ArgAction::Append)
                .help(
                    "Return this file from the run directory after the command exits. Repeatable",
                ),
        )
        .arg(
            Arg::new("out")
                .long("out")
                .value_name("DIR")
                .help("Write the --get files into DIR (default: list them)"),
        )
        .arg(
            Arg::new("idempotency-key")
                .long("idempotency-key")
                .value_name("KEY")
                .help(
                    "Retry key, per node: a repeat that reaches the node that ran it returns \
                     the kept reply (10 minutes) instead of running again",
                ),
        )
        .arg(
            Arg::new("dry-run")
                .long("dry-run")
                .action(clap::ArgAction::SetTrue)
                .help("Answer which node would take the run and whether it has room; run nothing"),
        )
        .arg(
            Arg::new("wait-timeout")
                .long("wait-timeout")
                .value_name("SECONDS")
                .value_parser(clap::value_parser!(u32))
                .help("How long to wait for the session to start (default 60, at most 600)"),
        )
        .arg(
            Arg::new("on-node")
                .long("on-node")
                .value_name("NODE")
                .conflicts_with("node")
                .help(
                    "Run on this node id rather than the router's pick (a retry names the \
                     node the first attempt reported)",
                ),
        )
        .arg(
            Arg::new("node")
                .long("node")
                .value_name("ADDR")
                .requires("ticket")
                .help("Edge form: the worker to run on, e.g. https://10.0.1.7:4848"),
        )
        .arg(
            Arg::new("node-name")
                .long("node-name")
                .value_name("NODE")
                .requires("node")
                .help("Node id the worker at --node must prove it is"),
        )
        .arg(
            Arg::new("ca")
                .long("ca")
                .value_name("FILE")
                .requires("node")
                .help("Cluster CA bundle (PEM). Default: fetched from the manager with your token"),
        )
        .arg(
            Arg::new("ticket")
                .long("ticket")
                .value_name("TOKEN")
                .requires("node")
                .help(
                    "Edge form: a session ticket from `legion boot-ticket create --scope session`",
                ),
        )
        .arg(
            Arg::new("local")
                .long("local")
                .action(clap::ArgAction::SetTrue)
                .conflicts_with_all(["node", "on-node", "command"])
                .help(
                    "Inside a task: ask this node's worker over the allocation's API socket \
                     (LEGION_ADDR = unix://...), with no token. The jobspec decides what the \
                     task may run; --task is required",
                ),
        )
        .arg(json_flag())
        .arg(
            Arg::new("command")
                .long("command")
                .value_name("ARG")
                .num_args(1..)
                .allow_hyphen_values(true)
                .help(
                    "Replace the task's run.command with the rest of the line. Needs \
                     run.allowCommand = true on the task and exec:write on the job; put it last",
                ),
        );
    Command::new("session")
        .about("Find session nodes, start sessions at the edge, and run code in one call")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(nodes_cmd)
        .subcommand(run_cmd)
        .subcommand(start_cmd)
        .subcommand(children_cmd)
        .subcommand(handoff_cmd)
        .subcommand(get_cmd)
        .subcommand(stop_cmd)
}

#[instrument(skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("nodes", matches)) => nodes(matches).await,
        Some(("run", matches)) => run(matches).await,
        Some(("start", matches)) => start(matches).await,
        Some(("children", matches)) => children(matches).await,
        Some(("handoff", matches)) => handoff(matches).await,
        Some(("get", matches)) => get(matches).await,
        Some(("stop", matches)) => stop(matches).await,
        _ => {
            println!("Usage: legion session nodes|run|start|children|handoff|get|stop");
            Ok(())
        }
    }
}

async fn nodes(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let job_id = matches
        .get_one::<String>("job")
        .map(|job| crate::cmd::helpers::job_ref(matches, job))
        .transpose()?
        .unwrap_or_default();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "Failed to list session nodes",
        client
            .list_session_nodes(ListSessionNodesRequest {
                job_id,
                task_name: matches
                    .get_one::<String>("task")
                    .cloned()
                    .unwrap_or_default(),
            })
            .await
            .map(tonic::Response::into_inner),
    )?;
    match matches.get_one::<String>("format").map(String::as_str) {
        Some("envoy-eds") => {
            println!("{}", serde_json::to_string_pretty(&envoy_eds(&reply))?);
        }
        Some("haproxy") => print!("{}", haproxy(&reply)),
        _ if format.is_json() => output::print_success(&nodes_json(&reply)),
        _ => {
            println!(
                "{:<24} {:<10} {:<9} {:>7} {:>8}  {:<16} ADDRESS",
                "NODE", "TAG", "ACCEPTING", "WEIGHT", "SESSIONS", "REASON"
            );
            for node in &reply.nodes {
                println!(
                    "{:<24} {:<10} {:<9} {:>7} {:>8}  {:<16} {}",
                    node.node_id,
                    node.tag,
                    node.accepting,
                    weight_text(node.weight),
                    node.sessions,
                    if node.reason.is_empty() {
                        "-"
                    } else {
                        &node.reason
                    },
                    node.address
                );
            }
        }
    }
    Ok(())
}

fn weight_text(weight: u32) -> String {
    if weight == u32::MAX {
        "∞".into()
    } else {
        weight.to_string()
    }
}

fn nodes_json(reply: &ListSessionNodesReply) -> Value {
    json!({
        "job": reply.job_id,
        "task": reply.task_name,
        "asOf": reply.as_of,
        "nodes": reply.nodes.iter().map(|node| json!({
            "node": node.node_id,
            "address": node.address,
            "tag": node.tag,
            "accepting": node.accepting,
            "reason": (!node.reason.is_empty()).then_some(node.reason.as_str()),
            "room": serde_json::from_str::<Value>(&node.room).unwrap_or(Value::Null),
            "weight": node.weight,
            "sessions": node.sessions,
        })).collect::<Vec<_>>(),
    })
}

/// `host` and `port` of a worker address (`https://10.0.1.7:4848`).
fn host_port(address: &str) -> Option<(String, u16)> {
    let rest = address
        .strip_prefix("https://")
        .or_else(|| address.strip_prefix("http://"))
        .unwrap_or(address)
        .trim_end_matches('/');
    let (host, port) = rest.rsplit_once(':')?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    Some((host.to_string(), port.parse().ok()?))
}

/// Balancer weights are bounded (Envoy: 1..=128 by convention here,
/// HAProxy: 0..=256); room is relative, so clamp.
fn balancer_weight(node: &SessionNode, max: u32) -> u32 {
    node.weight.clamp(1, max)
}

/// An Envoy `ClusterLoadAssignment` (EDS). Nodes not accepting are listed
/// as `DRAINING`, so Envoy routes no new requests to them.
fn envoy_eds(reply: &ListSessionNodesReply) -> Value {
    let endpoints: Vec<Value> = reply
        .nodes
        .iter()
        .filter_map(|node| {
            let (host, port) = host_port(&node.address)?;
            Some(json!({
                "endpoint": {
                    "address": { "socket_address": { "address": host, "port_value": port } },
                    "hostname": node.node_id,
                },
                "health_status": if node.accepting { "HEALTHY" } else { "DRAINING" },
                "load_balancing_weight": balancer_weight(node, 128),
                "metadata": { "filter_metadata": { "legion": {
                    "node": node.node_id,
                    "tag": node.tag,
                } } },
            }))
        })
        .collect();
    json!({
        "@type": "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment",
        "cluster_name": format!("{}-{}", reply.job_id, reply.task_name),
        "endpoints": [{ "lb_endpoints": endpoints }],
    })
}

/// HAProxy `server` lines for a backend. Nodes not accepting are `disabled`.
fn haproxy(reply: &ListSessionNodesReply) -> String {
    let mut out = format!(
        "# legion session nodes {} --task {} ({})\n",
        reply.job_id, reply.task_name, reply.as_of
    );
    for node in &reply.nodes {
        let Some((host, port)) = host_port(&node.address) else {
            continue;
        };
        let host = if host.contains(':') {
            format!("[{host}]")
        } else {
            host
        };
        out.push_str(&format!(
            "server {} {host}:{port} weight {} ssl verify required{}\n",
            node.node_id,
            balancer_weight(node, 256),
            if node.accepting { "" } else { " disabled" }
        ));
    }
    out
}

/// One `--file` value as a staged file: `PATH` (staged under its file
/// name) or `NAME=PATH`. Keeps the local file's executable bit.
fn staged_file(spec: &str) -> R<RunFile> {
    let (name, path) = match spec.split_once('=') {
        Some((name, path)) if !name.is_empty() && !path.is_empty() => {
            (name.to_string(), std::path::PathBuf::from(path))
        }
        _ => {
            let path = std::path::PathBuf::from(spec);
            let name = path
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .with_context(|| format!("--file {spec} names no file"))?;
            (name, path)
        }
    };
    let content =
        std::fs::read(&path).with_context(|| format!("--file: cannot read {}", path.display()))?;
    #[cfg(unix)]
    let executable = {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(&path)
            .map(|meta| meta.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    };
    #[cfg(not(unix))]
    let executable = false;
    Ok(RunFile {
        path: name,
        content,
        executable,
    })
}

/// `--input`: the JSON itself, `@file`, or `-` for stdin.
fn run_input(raw: &str) -> R<String> {
    if raw == "-" {
        let mut text = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut text)
            .context("--input -: cannot read stdin")?;
        return Ok(text);
    }
    if let Some(path) = raw.strip_prefix('@') {
        return std::fs::read_to_string(path)
            .with_context(|| format!("--input: cannot read {path}"));
    }
    Ok(raw.to_string())
}

/// The bytes of a run's stream or file as JSON: `text` when they are UTF-8,
/// `base64` otherwise.
fn bytes_json(bytes: &[u8]) -> (Option<String>, Option<String>) {
    use base64::Engine;
    match std::str::from_utf8(bytes) {
        Ok(text) => (Some(text.to_string()), None),
        Err(_) => (
            None,
            Some(base64::engine::general_purpose::STANDARD.encode(bytes)),
        ),
    }
}

/// A run's reply in the envelope's `data` shape (docs/session-runs.md,
/// "The reply"). Shared with the MCP tool.
pub fn run_reply_json(reply: &RunSessionResponse) -> Value {
    if let Some(dry) = reply.dry_run.as_ref() {
        return json!({
            "dryRun": true,
            "task": reply.task_name,
            "node": dry.node_id,
            "hasRoom": dry.has_room,
            "reason": if dry.reason.is_empty() { Value::Null } else { json!(dry.reason) },
            "command": dry.command,
            "wall": dry.wall,
            "outputBytes": dry.output_bytes,
        });
    }
    let stream = |bytes: &[u8]| match bytes_json(bytes) {
        (Some(text), _) => json!(text),
        (None, _) => json!(String::from_utf8_lossy(bytes)),
    };
    let files: Vec<Value> = reply
        .files
        .iter()
        .map(|file| {
            let (text, base64) = bytes_json(&file.content);
            json!({
                "path": file.path,
                "missing": file.missing,
                "truncated": file.truncated,
                "bytes": file.content.len(),
                "content": text,
                "contentBase64": base64,
            })
        })
        .collect();
    json!({
        "task": reply.task_name,
        "node": reply.node_id,
        "sessionId": reply.session_id,
        "runId": reply.run_id,
        "exit": reply.exit,
        "stdout": stream(&reply.stdout),
        "stderr": stream(&reply.stderr),
        "truncated": reply.truncated,
        "result": serde_json::from_str::<Value>(&reply.result).unwrap_or(Value::Null),
        "files": files,
        "wallMs": reply.wall_ms,
        "cpuMs": reply.cpu_ms,
        "stopped": reply.stopped.as_ref().map(|stop| json!({
            "reason": stop.reason,
            "limit": stop.limit,
            "detail": stop.detail,
            "hint": stop.hint,
        })),
        "deduplicated": reply.deduplicated,
    })
}

/// The exit status `legion session run` ends with: the command's own, 124
/// for a run stopped by `run.wall`, 125 for any other stop.
pub fn run_exit_code(reply: &RunSessionResponse) -> i32 {
    match reply.stopped.as_ref() {
        Some(stop) if stop.reason == "wall" => 124,
        Some(_) => 125,
        None => match reply.exit {
            Some(0) | None => 0,
            Some(code) if (1..=255).contains(&code) => code,
            Some(_) => 1,
        },
    }
}

async fn run(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let local = |message: String| {
        output::fail_local(
            format,
            output::CliError::local("invalid_argument", message, None),
        )
    };
    let job_id = matches
        .get_one::<String>("job")
        .map(|job| crate::cmd::helpers::job_ref(matches, job))
        .transpose()?
        .unwrap_or_default();
    let mut files = Vec::new();
    for spec in matches.get_many::<String>("file").into_iter().flatten() {
        files.push(staged_file(spec).map_err(|err| local(format!("{err:#}")))?);
    }
    let input = matches
        .get_one::<String>("input")
        .map(|raw| run_input(raw))
        .transpose()
        .map_err(|err| local(format!("{err:#}")))?;
    let request = RunSessionRequest {
        job_id,
        task_name: matches
            .get_one::<String>("task")
            .cloned()
            .unwrap_or_default(),
        files,
        input,
        get: matches
            .get_many::<String>("get")
            .into_iter()
            .flatten()
            .cloned()
            .collect(),
        command: matches
            .get_many::<String>("command")
            .into_iter()
            .flatten()
            .cloned()
            .collect(),
        idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
        wait_timeout_seconds: matches.get_one::<u32>("wait-timeout").copied(),
        dry_run: matches.get_flag("dry-run"),
        ticket: ticket(matches),
        node_id: matches
            .get_one::<String>("on-node")
            .cloned()
            .unwrap_or_default(),
    };
    let result = if matches.get_flag("local") {
        let address = crate::cmd::helpers::manager_address();
        let path = crate::cmd::helpers::unix_socket_path(&address).ok_or_else(|| {
            local(format!(
                "--local needs the allocation's API socket: LEGION_ADDR is {address}, not a \
                 unix:// address (the task declares no `legion` identity, or this is not a task)"
            ))
        })?;
        // The worker checks the job against the socket's allocation.
        crate::structs::workload_client::WorkloadClient::new(
            crate::cmd::helpers::unix_socket_channel(path, None),
        )
        .run_session(request)
        .await
        .map(tonic::Response::into_inner)
    } else if matches.get_one::<String>("node").is_some() {
        let mut client = edge_client(matches).await?;
        client
            .run_session(request)
            .await
            .map(tonic::Response::into_inner)
    } else {
        let mut client = connect_manager_client().await?;
        client
            .run_session(request)
            .await
            .map(tonic::Response::into_inner)
    };
    let reply = output::resolve(format, "Failed to run", result)?;
    if format.is_json() {
        output::print_success(&run_reply_json(&reply));
    } else if let Some(dry) = reply.dry_run.as_ref() {
        println!(
            "{} would run on {} ({})",
            reply.task_name,
            dry.node_id,
            if dry.has_room {
                "has room".to_string()
            } else {
                format!("no room: {}", dry.reason)
            }
        );
        println!("command: {}", dry.command.join(" "));
        println!("wall: {}, output: {} bytes", dry.wall, dry.output_bytes);
    } else {
        use std::io::Write;
        let _ = std::io::stdout().write_all(&reply.stdout);
        let _ = std::io::stderr().write_all(&reply.stderr);
        if reply.truncated {
            eprintln!("legion: output truncated at the task's run.output");
        }
        if !reply.result.is_empty() {
            eprintln!("legion: result: {}", reply.result);
        }
        let out_dir = matches
            .get_one::<String>("out")
            .map(std::path::PathBuf::from);
        for file in &reply.files {
            if file.missing {
                eprintln!("legion: {}: the run left no such file", file.path);
                continue;
            }
            let note = if file.truncated { " (truncated)" } else { "" };
            match out_dir.as_ref() {
                Some(dir) => {
                    // The reply names files as the caller did; only a plain
                    // relative path is written under --out.
                    let relative = crate::structs::run::run_relative_path(&file.path)
                        .map_err(|err| anyhow!("refusing to write {}: {err}", file.path))?;
                    let target = dir.join(relative);
                    if let Some(parent) = target.parent() {
                        std::fs::create_dir_all(parent)
                            .with_context(|| format!("create {}", parent.display()))?;
                    }
                    std::fs::write(&target, &file.content)
                        .with_context(|| format!("write {}", target.display()))?;
                    eprintln!(
                        "legion: wrote {} ({} bytes{note})",
                        target.display(),
                        file.content.len()
                    );
                }
                None => eprintln!(
                    "legion: {}: {} bytes{note} (pass --out DIR to save it, or -j to read it)",
                    file.path,
                    file.content.len()
                ),
            }
        }
        if let Some(stop) = reply.stopped.as_ref() {
            eprintln!(
                "legion: run stopped ({}): {}\nHint: {}",
                stop.reason, stop.detail, stop.hint
            );
        } else if reply.deduplicated {
            eprintln!("legion: answered from the kept reply of an earlier run (deduplicated)");
        }
    }
    let code = run_exit_code(&reply);
    if code != 0 {
        use std::io::Write;
        let _ = std::io::stdout().flush();
        std::process::exit(code);
    }
    Ok(())
}

async fn edge_client(
    matches: &ArgMatches,
) -> R<crate::structs::worker_client::WorkerClient<tonic::transport::Channel>> {
    worker_plane_client(
        matches
            .get_one::<String>("node")
            .map(String::as_str)
            .context("Must provide --node")?,
        matches.get_one::<String>("node-name").map(String::as_str),
        matches.get_one::<String>("ca").map(String::as_str),
    )
    .await
}

fn ticket(matches: &ArgMatches) -> String {
    matches
        .get_one::<String>("ticket")
        .cloned()
        .unwrap_or_default()
}

/// A request the CLI refuses before sending anything.
pub(crate) fn refuse_locally(format: OutputFormat, message: String) -> anyhow::Error {
    output::fail_local(
        format,
        output::CliError::local("invalid_argument", message, None),
    )
}

/// The `Workload` service on the allocation's API socket (`LEGION_ADDR =
/// unix://...` inside a task whose task declares the `legion` identity).
pub(crate) fn workload_client(
    format: OutputFormat,
    flag: &str,
) -> R<crate::structs::workload_client::WorkloadClient<tonic::transport::Channel>> {
    let address = crate::cmd::helpers::manager_address();
    let path = crate::cmd::helpers::unix_socket_path(&address).ok_or_else(|| {
        refuse_locally(
            format,
            format!(
                "{flag} needs the allocation's API socket: LEGION_ADDR is {address}, not a \
             unix:// address (the task declares no `legion` identity, or this is not a task)"
            ),
        )
    })?;
    Ok(crate::structs::workload_client::WorkloadClient::new(
        crate::cmd::helpers::unix_socket_channel(path, None),
    ))
}

fn owner_json(owner: Option<&crate::structs::OwnerRef>) -> Value {
    match owner {
        Some(owner) => json!({ "kind": owner.kind, "id": owner.id }),
        None => Value::Null,
    }
}

/// `legion session handoff`: pass an accepted connection to a child's port
/// over the allocation's handoff socket.
async fn handoff(matches: &ArgMatches) -> R<()> {
    use crate::worker::handoff;
    let format = OutputFormat::from_matches(matches);
    let address = crate::cmd::helpers::manager_address();
    let api_socket = crate::cmd::helpers::unix_socket_path(&address).ok_or_else(|| {
        refuse_locally(
            format,
            format!(
                "handoff needs the allocation's API socket: LEGION_ADDR is {address}, not a \
                 unix:// address (the task declares no `legion` identity, or this is not a task)"
            ),
        )
    })?;
    let socket = std::path::Path::new(&api_socket).with_file_name(handoff::HANDOFF_SOCKET_NAME);
    let request = handoff::HandoffRequest {
        child: matches.get_one::<String>("id").cloned().unwrap_or_default(),
        port: matches
            .get_one::<String>("port")
            .cloned()
            .unwrap_or_default(),
    };
    let fd = matches.get_one::<i32>("fd").copied().unwrap_or(-1);
    let wait = matches.get_flag("wait");
    let outcome = tokio::task::spawn_blocking(move || -> R<Option<handoff::HandoffClosed>> {
        let control = handoff::hand_off(&socket, &request, fd)?;
        // The worker holds its own copy now; ours would keep the
        // connection open after the child is done with it.
        // SAFETY: `fd` is a descriptor this process inherited for this
        // call and uses nowhere else.
        unsafe {
            libc::close(fd);
        }
        if wait {
            handoff::wait_closed(control).map(Some)
        } else {
            Ok(None)
        }
    })
    .await
    .context("handoff task panicked")?;
    let closed = match outcome {
        Ok(closed) => closed,
        Err(err) => {
            return Err(output::fail_local(
                format,
                output::CliError::local("handoff_refused", format!("{err:#}"), None),
            ))
        }
    };
    if format.is_json() {
        output::print_success(&json!({
            "handedOff": true,
            "closed": closed.as_ref().map(|closed| json!({
                "sent": closed.sent,
                "received": closed.received,
                "error": (!closed.error.is_empty()).then_some(&closed.error),
            })),
        }));
    } else {
        match closed {
            None => println!("Handed the connection to the child"),
            Some(closed) => println!(
                "The handed-off connection closed: {} bytes to the child, {} back{}",
                closed.sent,
                closed.received,
                if closed.error.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", closed.error)
                }
            ),
        }
    }
    Ok(())
}

/// `--owner self`: start a child of the calling workload on its own node.
async fn start_child_of_self(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    // `legion session start machine --owner self`: the positional is the
    // task, the job is the caller's own.
    let task_name = matches
        .get_one::<String>("task")
        .or_else(|| matches.get_one::<String>("job"))
        .cloned()
        .unwrap_or_default();
    let mut client = workload_client(format, "--owner self")?;
    let reply = output::resolve(
        format,
        "Failed to start the child session",
        client
            .start_child_session(crate::structs::StartChildSessionRequest {
                task_name,
                idempotency_key: matches
                    .get_one::<String>("idempotency-key")
                    .cloned()
                    .unwrap_or_default(),
                wait: matches.get_flag("no-wait").then_some(false),
            })
            .await
            .map(tonic::Response::into_inner),
    )?;
    print_started(format, &reply);
    Ok(())
}

/// The node a `running_elsewhere` refusal names: an UNAVAILABLE that the
/// old node did not answer, ending in `[winner node=<id> generation=<g>]`.
fn running_elsewhere_node(status: &tonic::Status) -> Option<String> {
    if status.code() != tonic::Code::Unavailable
        || !status.message().starts_with("running_elsewhere:")
    {
        return None;
    }
    let (_, tail) = status.message().rsplit_once("[winner node=")?;
    let node = tail.split([' ', ']']).next()?;
    (!node.is_empty()).then(|| node.to_string())
}

/// `down` or `lost` when `ListSessionNodes` lists `node` that way for the
/// session task, else `None`, including when no manager answers: without
/// that word the move stays unforced and the node's refusal stands.
async fn node_marked_gone(job_id: &str, task_name: &str, node: &str) -> Option<String> {
    let mut client = connect_manager_client().await.ok()?;
    let reply = client
        .list_session_nodes(ListSessionNodesRequest {
            job_id: job_id.to_string(),
            task_name: task_name.to_string(),
        })
        .await
        .ok()?
        .into_inner();
    gone_reason(&reply, node).map(str::to_string)
}

/// The `down`/`lost` reason `reply` gives `node`, if any.
fn gone_reason<'a>(reply: &'a ListSessionNodesReply, node: &str) -> Option<&'a str> {
    reply
        .nodes
        .iter()
        .find(|entry| entry.node_id == node)
        .map(|entry| entry.reason.as_str())
        .filter(|reason| matches!(*reason, "down" | "lost"))
}

fn print_started(format: OutputFormat, reply: &crate::structs::StartSessionReply) {
    if format.is_json() {
        output::print_success(&json!({
            "sessionId": reply.session_id,
            "node": reply.node_id,
            "state": reply.state,
            "deduplicated": reply.deduplicated,
            "resumedFrom": reply.resumed_from,
            "generation": reply.generation,
            "movedFrom": reply.moved_from,
            "owner": owner_json(reply.owner.as_ref()),
        }));
    } else {
        let moved = if reply.moved_from.is_empty() {
            String::new()
        } else {
            format!(", moved from {}", reply.moved_from)
        };
        let resumed = if reply.resumed_from.is_empty() {
            String::new()
        } else {
            format!(", resumed from {}", reply.resumed_from)
        };
        let owned = match reply.owner.as_ref() {
            Some(owner) => format!(", child of {} {}", owner.kind, owner.id),
            None => String::new(),
        };
        println!(
            "{} on {} ({}{}{}{}{})",
            reply.session_id,
            reply.node_id,
            reply.state,
            if reply.deduplicated {
                ", deduplicated"
            } else {
                ""
            },
            resumed,
            moved,
            owned
        );
    }
}

/// `legion session children`: what the calling workload owns on its node.
async fn children(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = workload_client(format, "session children")?;
    let reply = output::resolve(
        format,
        "Failed to list children",
        client
            .list_children(crate::structs::ListChildrenRequest {})
            .await
            .map(tonic::Response::into_inner),
    )?;
    if format.is_json() {
        output::print_success(&json!({
            "owner": owner_json(reply.owner.as_ref()),
            "max": reply.max,
            "children": reply.children.iter().map(|child| json!({
                "sessionId": child.session_id,
                "task": child.task_name,
                "node": child.node_id,
                "state": child.state,
                "reason": child.reason,
            })).collect::<Vec<_>>(),
        }));
    } else if reply.children.is_empty() {
        println!("no children (max {})", reply.max);
    } else {
        for child in &reply.children {
            let reason = if child.reason.is_empty() {
                String::new()
            } else {
                format!(" ({})", child.reason)
            };
            println!(
                "{}  {}  {}{}",
                child.session_id, child.task_name, child.state, reason
            );
        }
    }
    Ok(())
}

async fn start(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let owner = matches.get_one::<String>("owner").cloned();
    if owner.as_deref() == Some(crate::structs::children::OWNER_SELF) {
        return start_child_of_self(matches).await;
    }
    if matches.get_one::<String>("node").is_none()
        || matches.get_one::<String>("ticket").is_none()
        || matches.get_one::<String>("task").is_none()
    {
        return Err(refuse_locally(
            format,
            "--node, --ticket, and --task are required (only `--owner self`, inside a task, \
             starts a session without them)"
                .to_string(),
        ));
    }
    let owner = owner.map(|id| crate::structs::OwnerRef {
        kind: matches
            .get_one::<String>("owner-kind")
            .cloned()
            .unwrap_or_default(),
        id,
    });
    let mut request = StartSessionRequest {
        owner,
        ticket: ticket(matches),
        job_id: matches
            .get_one::<String>("job")
            .map(|job| crate::cmd::helpers::job_ref(matches, job))
            .transpose()?
            .unwrap_or_default(),
        task_name: matches
            .get_one::<String>("task")
            .cloned()
            .unwrap_or_default(),
        idempotency_key: matches
            .get_one::<String>("idempotency-key")
            .cloned()
            .unwrap_or_default(),
        wait: matches.get_flag("no-wait").then_some(false),
        resume_id: matches
            .get_one::<String>("resume-id")
            .cloned()
            .unwrap_or_default(),
        force_move: matches.get_flag("force-move"),
    };
    let mut client = edge_client(matches).await?;
    let mut result = client
        .start_session(request.clone())
        .await
        .map(tonic::Response::into_inner);
    // A move the old node did not answer: the picker's job is to force it
    // when the managers list that node `down` or `lost` (the manager-routed
    // move does the same), so the caller need not pass --force-move.
    if let Err(status) = &result {
        if !request.resume_id.is_empty() && !request.force_move {
            if let Some(node) = running_elsewhere_node(status) {
                if let Some(reason) =
                    node_marked_gone(&request.job_id, &request.task_name, &node).await
                {
                    if !format.is_json() {
                        eprintln!(
                            "Node {node} did not answer and is listed {reason}; moving the \
                             session with --force-move"
                        );
                    }
                    request.force_move = true;
                    result = client
                        .start_session(request.clone())
                        .await
                        .map(tonic::Response::into_inner);
                }
            }
        }
    }
    let reply = output::resolve(format, "Failed to start the session", result)?;
    print_started(format, &reply);
    Ok(())
}

async fn get(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = edge_client(matches).await?;
    let reply = output::resolve(
        format,
        "Failed to read the session",
        client
            .get_session(GetSessionRequest {
                session_id: matches.get_one::<String>("id").cloned().unwrap_or_default(),
                ticket: ticket(matches),
            })
            .await
            .map(tonic::Response::into_inner),
    )?;
    let session = reply.session.unwrap_or_default();
    let rendered = json!({
        "sessionId": session.session_id,
        "node": reply.node_id,
        "job": session.job_id,
        "task": session.task_name,
        "state": session.state,
        "generation": session.generation,
        "origin": session.origin,
        "leaseIssuedAtUnixMs": session.lease_issued_at_unix_ms,
    });
    if format.is_json() {
        output::print_success(&rendered);
    } else {
        println!("{}", serde_json::to_string_pretty(&rendered)?);
    }
    Ok(())
}

async fn stop(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let id = matches.get_one::<String>("id").cloned().unwrap_or_default();
    if matches.get_flag("local") {
        let mut client = workload_client(format, "--local")?;
        let reply = output::resolve(
            format,
            "Failed to stop the child session",
            client
                .stop_child_session(crate::structs::StopChildSessionRequest {
                    session_id: id.clone(),
                })
                .await
                .map(tonic::Response::into_inner),
        )?;
        if format.is_json() {
            output::print_success(&json!({ "sessionId": id, "stopped": reply.stopped }));
        } else if reply.stopped {
            println!("{id} stopped");
        } else {
            println!("{id} had already ended");
        }
        return Ok(());
    }
    let mut client = edge_client(matches).await?;
    let reply = output::resolve(
        format,
        "Failed to stop the session",
        client
            .stop_session(StopSessionRequest {
                ticket: ticket(matches),
                session_id: id.clone(),
            })
            .await
            .map(tonic::Response::into_inner),
    )?;
    if format.is_json() {
        output::print_success(&json!({
            "sessionId": id,
            "node": reply.node_id,
            "stopped": reply.stopped,
        }));
    } else if reply.stopped {
        println!("{id} stopped on {}", reply.node_id);
    } else {
        println!("{id} was not running on {}", reply.node_id);
    }
    Ok(())
}

#[cfg(test)]
mod session_cmd_test {
    use super::*;

    fn reply() -> ListSessionNodesReply {
        ListSessionNodesReply {
            job_id: "agents".into(),
            task_name: "workspace".into(),
            as_of: "2026-10-12T00:00:00Z".into(),
            nodes: vec![
                SessionNode {
                    node_id: "w-17".into(),
                    address: "https://10.0.1.7:4848".into(),
                    tag: "q4n7c2rd".into(),
                    accepting: true,
                    weight: 43,
                    ..Default::default()
                },
                SessionNode {
                    node_id: "w-18".into(),
                    address: "https://10.0.1.8:4848".into(),
                    tag: "h2m5x9pe".into(),
                    accepting: false,
                    reason: "draining".into(),
                    ..Default::default()
                },
            ],
        }
    }

    #[test]
    fn the_node_list_renders_for_envoy_and_haproxy_test() {
        let eds = envoy_eds(&reply());
        let endpoints = &eds["endpoints"][0]["lb_endpoints"];
        assert_eq!(
            endpoints[0]["endpoint"]["address"]["socket_address"]["port_value"],
            4848
        );
        assert_eq!(endpoints[0]["load_balancing_weight"], 43);
        assert_eq!(endpoints[1]["health_status"], "DRAINING");
        assert_eq!(endpoints[1]["load_balancing_weight"], 1);

        let lines = haproxy(&reply());
        assert!(lines.contains("server w-17 10.0.1.7:4848 weight 43 ssl verify required\n"));
        assert!(lines.contains("server w-18 10.0.1.8:4848 weight 1 ssl verify required disabled\n"));
    }

    #[test]
    fn a_move_is_forced_only_past_a_node_listed_down_or_lost_test() {
        let refusal = tonic::Status::unavailable(
            "running_elsewhere: session workspace-q4n7c2rd.01jbx runs on node w-17 \
             (generation 2), which did not answer within 2s: connect refused. Retry with \
             force_move only once that node is known to be down (`legion session nodes`) \
             [winner node=w-17 generation=2]",
        );
        assert_eq!(running_elsewhere_node(&refusal).as_deref(), Some("w-17"));
        assert_eq!(
            running_elsewhere_node(&tonic::Status::unavailable("store unreachable")),
            None
        );
        assert_eq!(
            running_elsewhere_node(&tonic::Status::failed_precondition(
                "running_elsewhere: [winner node=w-17 generation=2]"
            )),
            None
        );

        let mut nodes = reply();
        assert_eq!(gone_reason(&nodes, "w-17"), None);
        assert_eq!(gone_reason(&nodes, "w-18"), None, "draining is not gone");
        nodes.nodes[0].accepting = false;
        nodes.nodes[0].reason = "lost".into();
        assert_eq!(gone_reason(&nodes, "w-17"), Some("lost"));
        assert_eq!(
            gone_reason(&nodes, "w-99"),
            None,
            "an unlisted node is not gone"
        );
    }

    #[test]
    fn edge_commands_parse_test() {
        let matches = config()
            .try_get_matches_from([
                "session",
                "start",
                "agents",
                "--task",
                "workspace",
                "--node",
                "https://10.0.1.7:4848",
                "--ticket",
                "t",
                "--no-wait",
            ])
            .expect("parses");
        let (_, start) = matches.subcommand().expect("start");
        assert!(start.get_flag("no-wait"));
        assert_eq!(host_port("https://[::1]:4848"), Some(("::1".into(), 4848)));
    }

    #[test]
    fn run_parses_files_and_a_trailing_command_test() {
        let matches = config()
            .try_get_matches_from([
                "session",
                "run",
                "code-runner",
                "--task",
                "machine",
                "--file",
                "main.py",
                "--get",
                "out.csv",
                "-j",
                "--command",
                "python3",
                "-u",
                "/legion/run/main.py",
            ])
            .expect("parses");
        let (_, run) = matches.subcommand().expect("run");
        let command: Vec<&String> = run.get_many::<String>("command").unwrap().collect();
        assert_eq!(command, ["python3", "-u", "/legion/run/main.py"]);
        assert!(run.get_flag("json"));
        // The edge form needs both halves.
        assert!(config()
            .try_get_matches_from(["session", "run", "job", "--node", "https://n:4848"])
            .is_err());
        assert!(config()
            .try_get_matches_from(["session", "run", "job", "--ticket", "t"])
            .is_err());
    }

    #[test]
    fn a_staged_file_keeps_its_name_or_takes_the_given_one_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("main.py");
        std::fs::write(&path, "print(42)\n")?;
        let plain = staged_file(path.to_str().unwrap())?;
        assert_eq!(plain.path, "main.py");
        assert_eq!(plain.content, b"print(42)\n");
        let named = staged_file(&format!("src/app.py={}", path.display()))?;
        assert_eq!(named.path, "src/app.py");
        assert!(staged_file("/nonexistent/x.py").is_err());
        Ok(())
    }

    #[test]
    fn a_run_reply_renders_and_exits_by_outcome_test() {
        let ran = RunSessionResponse {
            task_name: "machine".into(),
            node_id: "w-17".into(),
            session_id: "machine-q4n7c2rd.01jc0".into(),
            exit: Some(3),
            stdout: b"42\n".to_vec(),
            result: "{\"rows\":1204}".into(),
            wall_ms: 412,
            files: vec![crate::structs::RunOutputFile {
                path: "out.bin".into(),
                content: vec![0xff, 0xfe],
                ..Default::default()
            }],
            ..Default::default()
        };
        let data = run_reply_json(&ran);
        assert_eq!(data["exit"], 3);
        assert_eq!(data["stdout"], "42\n");
        assert_eq!(data["result"]["rows"], 1204);
        assert_eq!(data["stopped"], Value::Null);
        assert_eq!(data["files"][0]["contentBase64"], "//4=");
        assert_eq!(data["files"][0]["content"], Value::Null);
        assert_eq!(run_exit_code(&ran), 3);

        let stopped = RunSessionResponse {
            stopped: Some(crate::structs::RunStop {
                reason: "wall".into(),
                limit: "30s".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let data = run_reply_json(&stopped);
        assert_eq!(data["exit"], Value::Null);
        assert_eq!(data["stopped"]["reason"], "wall");
        assert_eq!(run_exit_code(&stopped), 124);
        assert_eq!(run_exit_code(&RunSessionResponse::default()), 0);

        let dry = RunSessionResponse {
            task_name: "machine".into(),
            dry_run: Some(crate::structs::RunDryRun {
                node_id: "w-17".into(),
                has_room: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(run_reply_json(&dry)["hasRoom"], true);
    }
}
