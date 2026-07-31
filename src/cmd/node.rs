use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    GetNodeRequest, GetNodesRequest, Node, NodePrint, RestartWorkerRequest,
    SetNodeAvailabilityRequest, UpdateNodeDynamicRequest,
};

/// Optional idempotency key for mutating node operations; the manager
/// deduplicates retried requests carrying the same key.
fn idempotency_arg() -> Arg {
    Arg::new("idempotency-key")
        .long("idempotency-key")
        .value_name("KEY")
        .help("Deduplicate retries of this mutating request (safe for agent retries)")
}

pub fn config() -> Command {
    let get_nodes_cmd = Command::new("list").about("List nodes").arg(json_flag());

    let node_claims_cmd = Command::new("claims")
        .about("List a node's live provisional (unratified) allocations")
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to inspect")
                .required(true),
        )
        .arg(json_flag());

    let node_info_cmd = Command::new("info")
        .about("Get node info")
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to inspect")
                .required(true),
        )
        .arg(json_flag());

    let update_node_cmd = Command::new("update")
        .about(
            "\
        Update node attributes\n\
        \n\
        Example: legion node update <id> -a <key1>=<value1> <key2>=<value2>\
        ",
        )
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to update")
                .required(true),
        )
        .arg(
            Arg::new("attr")
                .short('a')
                .long("attr")
                .value_name("KEY=VALUE")
                .help("Dynamic attributes to update on the node")
                .num_args(1..)
                .action(ArgAction::Append),
        )
        .arg(json_flag());

    let restart_node_cmd = Command::new("restart")
        .about("Restart a worker node")
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to restart")
                .required(true),
        )
        .arg(json_flag());

    let cordon_node_cmd = Command::new("cordon")
        .about("Mark a node unschedulable; existing allocations keep running")
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to cordon")
                .required(true),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let drain_node_cmd = Command::new("drain")
        .about(
            "\
        Mark a node unschedulable and evict its allocations\n\
        \n\
        Stateless allocations are moved to other nodes immediately. Stateful\n\
        allocations are protected by their stateful.onLost policy and stay on\n\
        the node (use `legion instance replace` to force-move one).\
        ",
        )
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to drain")
                .required(true),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let uncordon_node_cmd = Command::new("uncordon")
        .about("Restore a cordoned/drained node to the scheduling pool")
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Identifier of the node to uncordon")
                .required(true),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    Command::new("node")
        .about("Interact with nodes")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(get_nodes_cmd)
        .subcommand(update_node_cmd)
        .subcommand(node_info_cmd)
        .subcommand(node_claims_cmd)
        .subcommand(restart_node_cmd)
        .subcommand(cordon_node_cmd)
        .subcommand(drain_node_cmd)
        .subcommand(uncordon_node_cmd)
}

#[instrument(err, skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("list", arg_matches)) => {
            let format = OutputFormat::from_matches(arg_matches);
            get_nodes(format).await
        }
        Some(("update", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide id")?;

            let attrs = arg_matches
                .get_many::<String>("attr")
                .context("Must provide attribute(s) to update")?;

            let mut dynamic_diff = Map::new();

            let mut error_msg = None;
            attrs.into_iter().for_each(|attr| {
                let mut split = attr.split('=');

                if let Some(key_path) = split.next() {
                    if let Some(value) = split.next() {
                        dynamic_diff.insert(key_path.into(), value.into());
                    } else {
                        error_msg = Some(format!(
                            "Invalid attribute: {attr} - must be of format KEY=VALUE"
                        ));
                    }
                } else {
                    error_msg = Some(format!(
                        "Invalid attribute: {attr} - must be of format KEY=VALUE"
                    ));
                }
            });

            if let Some(msg) = error_msg {
                bail!(msg);
            }

            let format = OutputFormat::from_matches(arg_matches);
            update_node(id.into(), dynamic_diff, format).await
        }
        Some(("claims", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide id")?;
            let format = OutputFormat::from_matches(arg_matches);
            get_node_claims(id.to_string(), format).await
        }
        Some(("info", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide id")?;

            let format = OutputFormat::from_matches(arg_matches);
            get_node_info(id.to_string(), format).await
        }
        Some(("restart", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide id")?;

            let format = OutputFormat::from_matches(arg_matches);
            restart_node(id.to_string(), format).await
        }
        Some((command @ ("cordon" | "drain" | "uncordon"), arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide id")?;
            let idempotency_key = arg_matches.get_one::<String>("idempotency-key").cloned();

            // cordon = unschedulable, keep allocations; drain = unschedulable
            // + evict; uncordon = schedulable again.
            let (availability, evict) = match command {
                "cordon" => ("drained", false),
                "drain" => ("drained", true),
                _ => ("available", false),
            };

            let format = OutputFormat::from_matches(arg_matches);
            set_node_availability(id.to_string(), availability, evict, idempotency_key, format)
                .await
        }
        _ => unreachable!(),
    }
}

#[instrument(err, skip_all)]
async fn get_nodes(format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(GetNodesRequest {
        ..Default::default()
    });
    let reply = output::resolve(
        format,
        "failed to list nodes",
        client.get_nodes(request).await,
    )?
    .into_inner();
    let nodes: Vec<Node> = reply.nodes.into_iter().map(Into::into).collect();

    if format.is_json() {
        output::print_success(&nodes);
    } else {
        let printable_nodes: Vec<NodePrint> = nodes.into_iter().map(Into::into).collect();
        print_table(printable_nodes);
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn update_node(
    node_id: String,
    dynamic_diff: Map<String, Value>,
    format: OutputFormat,
) -> R<()> {
    let mut client = connect_manager_client().await?;

    let request = tonic::Request::new(UpdateNodeDynamicRequest {
        id: node_id,
        dynamic_diff: serde_json::to_string(&dynamic_diff)?,
    });

    let reply = output::resolve(
        format,
        "failed to update node",
        client.update_node_dynamic(request).await,
    )?
    .into_inner();
    let node = reply.node.map(Node::from);

    if format.is_json() {
        output::print_success(&node);
    } else {
        match node {
            Some(node) => node.print_long(),
            None => println!("Node not found"),
        }
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn get_node_info(node_id: String, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(GetNodeRequest { id: node_id });
    let reply = output::resolve(
        format,
        "failed to fetch node info",
        client.get_node(request).await,
    )?
    .into_inner();
    let node = reply.node.map(Node::from);

    if format.is_json() {
        output::print_success(&node);
    } else {
        match node {
            Some(node) => node.print_long(),
            None => println!("Node not found"),
        }
    }

    Ok(())
}

/// `legion node claims <NODE>` — what this node has started that the store
/// has not recorded yet (docs/edge-boot.md).
///
/// Deliberately a separate view from `node info`: unratified work is live,
/// short-lived state reported on the last heartbeat, not part of the node
/// record, and mixing the two would suggest a durability it does not have.
#[instrument(err, skip_all)]
async fn get_node_claims(node_id: String, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(GetNodeRequest { id: node_id });
    let reply = output::resolve(
        format,
        "failed to fetch node claims",
        client.get_node(request).await,
    )?
    .into_inner();

    let claims = reply
        .provisional_claims
        .into_iter()
        .map(|claim| {
            json!({
                "claimId": claim.claim_id,
                "jobConfigId": claim.job_config_id,
                "taskName": claim.task_name,
                "instanceId": claim.instance_id,
                "gate": crate::structs::edge_boot::Gate::from_wire(claim.gate).as_str(),
                "stage": claim.stage,
                "leaseDeadlineUnix": claim.lease_deadline_unix,
                "resources": claim.resources,
            })
        })
        .collect::<Vec<_>>();

    if format.is_json() {
        output::print_success(&claims);
    } else if claims.is_empty() {
        println!("No provisional allocations on this node");
    } else {
        for claim in &claims {
            println!(
                "{}  {}  instance={}  gate={}  stage={}  lease_expires_unix={}",
                claim["claimId"].as_str().unwrap_or_default(),
                claim["taskName"].as_str().unwrap_or_default(),
                claim["instanceId"].as_str().unwrap_or_default(),
                claim["gate"].as_str().unwrap_or_default(),
                claim["stage"].as_str().unwrap_or_default(),
                claim["leaseDeadlineUnix"],
            );
        }
    }
    Ok(())
}

#[instrument(err, skip_all)]
async fn restart_node(node_id: String, format: OutputFormat) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(RestartWorkerRequest { node_id });
    let reply = output::resolve(
        format,
        "failed to restart worker",
        client.restart_worker(request).await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&json!({ "message": reply.message }));
    } else {
        println!("{}", reply.message);
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn set_node_availability(
    node_id: String,
    availability: &str,
    evict: bool,
    idempotency_key: Option<String>,
    format: OutputFormat,
) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(SetNodeAvailabilityRequest {
        node_id,
        availability: availability.to_string(),
        evict,
        idempotency_key,
    });
    let reply = output::resolve(
        format,
        "failed to set node availability",
        client.set_node_availability(request).await,
    )?
    .into_inner();
    let node = reply.node.map(Node::from);

    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "deduplicated": reply.deduplicated,
            "node": node,
        }));
    } else {
        println!("{}", reply.message);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_restart_command_parses() -> R<()> {
        let matches = config().try_get_matches_from(["node", "restart", "node-1"])?;
        let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
        assert_eq!(subcommand, "restart");
        let id = args.get_one::<String>("id").context("Missing id")?;
        assert_eq!(id, "node-1");
        Ok(())
    }

    #[test]
    fn node_cordon_drain_uncordon_commands_parse() -> R<()> {
        for command in ["cordon", "drain", "uncordon"] {
            let matches = config().try_get_matches_from([
                "node",
                command,
                "node-1",
                "--idempotency-key",
                "key-1",
                "--json",
            ])?;
            let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
            assert_eq!(subcommand, command);
            let id = args.get_one::<String>("id").context("Missing id")?;
            assert_eq!(id, "node-1");
            let key = args
                .get_one::<String>("idempotency-key")
                .context("Missing idempotency key")?;
            assert_eq!(key, "key-1");
        }
        Ok(())
    }
}
