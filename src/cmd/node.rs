use crate::cmd::helpers::{json_flag, print_table};
use crate::prelude::*;
use crate::structs::manager_client::ManagerClient;
use crate::structs::{GetNodeRequest, GetNodesRequest, Node, NodePrint, UpdateNodeDynamicRequest};
use serde_json::to_string;

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

    Command::new("node")
        .about("Interact with nodes")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(get_nodes_cmd)
        .subcommand(update_node_cmd)
        .subcommand(node_info_cmd)
}

#[instrument(err, skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("list", arg_matches)) => {
            let is_json = arg_matches.get_flag("json");
            get_nodes(is_json).await
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

            let is_json = arg_matches.get_flag("json");
            update_node(id.into(), dynamic_diff, is_json).await
        }
        Some(("info", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .context("Must provide id")?;

            let is_json = arg_matches.get_flag("json");
            get_node_info(id.to_string(), is_json).await
        }
        _ => unreachable!(),
    }
}

#[instrument(err, skip_all)]
async fn get_nodes(is_json: bool) -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS)
        .await
        .context("failed to connect to manager")?;
    let request = tonic::Request::new(GetNodesRequest {});
    let response = client.get_nodes(request).await?;
    let nodes_msg_list = response.into_inner().nodes;
    let nodes: Vec<Node> = nodes_msg_list.into_iter().map(Into::into).collect();

    if is_json {
        if let Ok(json) = to_string(&nodes) {
            println!("{json}");
        }
    } else {
        let printable_nodes: Vec<NodePrint> = nodes.into_iter().map(Into::into).collect();
        print_table(printable_nodes);
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn update_node(node_id: String, dynamic_diff: Map<String, Value>, is_json: bool) -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS)
        .await
        .context("failed to connect to manager")?;

    let request = tonic::Request::new(UpdateNodeDynamicRequest {
        id: node_id,
        dynamic_diff: serde_json::to_string(&dynamic_diff)?,
    });

    let response = client.update_node_dynamic(request).await?;

    match response.into_inner().node {
        Some(node_msg) => {
            let node: Node = node_msg.into();

            if is_json {
                node.print_long_json();
            } else {
                node.print_long();
            };
        }
        None => println!("Node not found"),
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn get_node_info(node_id: String, is_json: bool) -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS)
        .await
        .context("failed to connect to manager")?;
    let request = tonic::Request::new(GetNodeRequest { id: node_id });
    let response = client.get_node(request).await?;

    match response.into_inner().node {
        Some(node_msg) => {
            let node: Node = node_msg.into();

            if is_json {
                node.print_long_json();
            } else {
                node.print_long();
            };
        }
        None => println!("Node not found"),
    }

    Ok(())
}
