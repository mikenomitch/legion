use crate::constants::DEFAULT_ADDR;
use crate::util::R;

use clap::{Arg, Command};
use serde_json::Map;

use crate::structs::manager_client::ManagerClient;
use crate::structs::{GetNodeRequest, GetNodesRequest, UpdateNodeDynamicRequest};

use crate::structs::Printable;
// use crate::structs::*;

pub fn config() -> Command {
    let get_nodes_cmd = Command::new("list").about("List nodes");

    let node_info_cmd = Command::new("info")
        .about("Get node info")
        .arg(Arg::new("id").value_name("id").required(true));

    let update_node_cmd = Command::new("update")
        .about("Update node attributes")
        .arg(Arg::new("id").value_name("id").required(true))
        .arg(Arg::new("attr").short('a').num_args(1..));

    Command::new("node")
        .about("Interact with nodes")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(get_nodes_cmd)
        .subcommand(update_node_cmd)
        .subcommand(node_info_cmd)
}

pub async fn handle(sub_matches: &clap::ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("list", _)) => get_nodes().await,
        Some(("update", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .ok_or("Must provide id")?;
            let attrs = arg_matches
                .get_many::<String>("attr")
                .ok_or("Must provide attribute(s) to update")?;
            let mut key_path = "";
            let mut value = "";

            attrs.into_iter().for_each(|attr| {
                let mut split = attr.split('=');
                key_path = split.next().unwrap();
                value = split.next().unwrap();
            });

            update_node(id.into(), key_path.into(), value.into()).await
        }
        Some(("info", arg_matches)) => {
            let id = arg_matches
                .get_one::<String>("id")
                .ok_or("Must provide id")?;

            get_node_info(id.to_string()).await
        }
        _ => unreachable!(),
    }
}

async fn get_nodes() -> R<()> {
    let mut client = ManagerClient::connect(DEFAULT_ADDR).await?;
    let request = tonic::Request::new(GetNodesRequest {});
    let response = client.get_nodes(request).await?;
    response.into_inner().nodes.into_iter().for_each(|node| {
        // print_node_short(node);
        node.print_short();
    });

    Ok(())
}

async fn update_node(node_id: String, key_path: String, value: String) -> R<()> {
    let mut client = ManagerClient::connect(DEFAULT_ADDR).await?;

    let mut dynamic_diff = Map::new();
    dynamic_diff.insert(key_path, value.into());

    let request = tonic::Request::new(UpdateNodeDynamicRequest {
        id: node_id,
        dynamic_diff: serde_json::to_string(&dynamic_diff)?,
    });

    let response = client.update_node_dynamic(request).await?;

    match response.into_inner().node {
        Some(node) => node.print_long(),
        None => println!("Node not found"),
    }

    Ok(())
}

async fn get_node_info(node_id: String) -> R<()> {
    let mut client = ManagerClient::connect(DEFAULT_ADDR).await?;
    let request = tonic::Request::new(GetNodeRequest { id: node_id });
    let response = client.get_node(request).await?;

    match response.into_inner().node {
        Some(node) => node.print_long(),
        None => println!("Node not found"),
    }

    Ok(())
}
