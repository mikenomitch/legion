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

use crate::cmd::helpers::{connect_manager_client, json_flag, worker_plane_client};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    GetSessionRequest, ListSessionNodesReply, ListSessionNodesRequest, SessionNode,
    StartSessionRequest, StopSessionRequest,
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
            ),
    );
    let get_cmd = edge_args(
        Command::new("get")
            .about("Read a session from its node with a session ticket")
            .arg(Arg::new("id").value_name("SESSION").required(true)),
    );
    let stop_cmd = edge_args(
        Command::new("stop")
            .about("End a session on its node with a session ticket")
            .arg(Arg::new("id").value_name("SESSION").required(true)),
    );
    Command::new("session")
        .about("Find session nodes, and start sessions at the edge (docs/session-mode.md)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(nodes_cmd)
        .subcommand(start_cmd)
        .subcommand(get_cmd)
        .subcommand(stop_cmd)
}

#[instrument(err, skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("nodes", matches)) => nodes(matches).await,
        Some(("start", matches)) => start(matches).await,
        Some(("get", matches)) => get(matches).await,
        Some(("stop", matches)) => stop(matches).await,
        _ => {
            println!("Usage: legion session nodes|start|get|stop");
            Ok(())
        }
    }
}

async fn nodes(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let job_id = matches
        .get_one::<String>("job")
        .cloned()
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

async fn start(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = edge_client(matches).await?;
    let reply = output::resolve(
        format,
        "Failed to start the session",
        client
            .start_session(StartSessionRequest {
                ticket: ticket(matches),
                job_id: matches
                    .get_one::<String>("job")
                    .cloned()
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
            })
            .await
            .map(tonic::Response::into_inner),
    )?;
    if format.is_json() {
        output::print_success(&json!({
            "sessionId": reply.session_id,
            "node": reply.node_id,
            "state": reply.state,
            "deduplicated": reply.deduplicated,
        }));
    } else {
        println!(
            "{} on {} ({}{})",
            reply.session_id,
            reply.node_id,
            reply.state,
            if reply.deduplicated {
                ", deduplicated"
            } else {
                ""
            }
        );
    }
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
    let mut client = edge_client(matches).await?;
    let id = matches.get_one::<String>("id").cloned().unwrap_or_default();
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
}
