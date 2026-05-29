use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    GetNodeRequest, GetNodesRequest, Node, NodePrint, RestartWorkerRequest,
    UpdateNodeDynamicRequest,
};

pub fn config() -> Command {
    let get_nodes_cmd = Command::new("list").about("List nodes").arg(json_flag());

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

    Command::new("node")
        .about("Interact with nodes")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(get_nodes_cmd)
        .subcommand(update_node_cmd)
        .subcommand(node_info_cmd)
        .subcommand(restart_node_cmd)
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
}
