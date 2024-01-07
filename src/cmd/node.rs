use crate::prelude::*;
use crate::structs::manager_client::ManagerClient;
use crate::structs::{GetNodeRequest, GetNodesRequest, Node, UpdateNodeDynamicRequest};

pub fn config() -> Command {
    let json_arg = Arg::new("json")
        .long("json")
        .short('j')
        .required(false)
        .help("returns values in json format")
        .num_args(0);

    let get_nodes_cmd = Command::new("list")
        .about("List nodes")
        .arg(json_arg.clone());

    let node_info_cmd = Command::new("info")
        .about("Get node info")
        .arg(Arg::new("id").value_name("id").required(true))
        .arg(json_arg.clone());

    let update_node_cmd = Command::new("update")
        .about(
            "
        Update node attributes

        Example: legion node update <id> -a <key1>=<value1> <key2>=<value2>
        ",
        )
        .arg(Arg::new("id").value_name("id").required(true))
        .arg(Arg::new("attr").short('a').num_args(1..))
        .arg(json_arg.clone());

    Command::new("node")
        .about("Interact with nodes")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(get_nodes_cmd)
        .subcommand(update_node_cmd)
        .subcommand(node_info_cmd)
}

#[instrument(err, skip_all)]
pub async fn handle(sub_matches: &clap::ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("list", arg_matches)) => {
            let is_json = arg_matches.get_flag("json");
            get_nodes(is_json).await
        }
        Some(("update", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .ok_or("Must provide id")?;

            let attrs = arg_matches
                .get_many::<String>("attr")
                .ok_or("Must provide attribute(s) to update")?;

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
                return Err(msg.into());
            }

            let is_json = arg_matches.get_flag("json");
            update_node(id.into(), dynamic_diff, is_json).await
        }
        Some(("info", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .ok_or("Must provide id")?;

            let is_json = arg_matches.get_flag("json");
            get_node_info(id.to_string(), is_json).await
        }
        _ => unreachable!(),
    }
}

#[instrument(err, skip_all)]
async fn get_nodes(is_json: bool) -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS).await?;
    let request = tonic::Request::new(GetNodesRequest {});
    let response = client.get_nodes(request).await?;
    let nodes_msg_list = response.into_inner().nodes;
    let nodes: Vec<Node> = nodes_msg_list.into_iter().map(Into::into).collect();

    if is_json {
        Node::print_list_json(&nodes);
    } else {
        Node::print_list(&nodes);
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn update_node(node_id: String, dynamic_diff: Map<String, Value>, is_json: bool) -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS).await?;

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
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS).await?;
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
