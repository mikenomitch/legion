use crate::constants::DEFAULT_ADDR;
use crate::util::R;

use clap::{Arg, Command};

pub mod legion {
    tonic::include_proto!("legion");
}
use legion::manager_client::ManagerClient;
use legion::{GetNodesRequest, UpdateNodeDynamicRequest};

pub fn config() -> Command {
    let get_nodes_cmd = Command::new("list").about("List nodes");

    let update_node_cmd = Command::new("update")
        .about("Update node attributes")
        .arg(
            Arg::new("id")
                .value_name("id")
                .required(true)
        )
        .arg(
            Arg::new("attr")
                .value_delimiter('=')
                .number_of_values(2)
                .value_names(&["key", "value"])
                .short('a')
                .long("attr")
        );

    Command::new("node")
        .about("Interact with nodes")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(get_nodes_cmd)
        .subcommand(update_node_cmd)
}

pub async fn handle(sub_matches: &clap::ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("list", _)) => get_nodes().await,
        Some(("update", _)) => update_node().await,
        _ => unreachable!(),
    }
}

async fn get_nodes() -> R<()> {
    let mut client = ManagerClient::connect(DEFAULT_ADDR).await?;
    let request = tonic::Request::new(GetNodesRequest {});
    let response = client.get_nodes(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}

async fn update_node() -> R<()> {
    let mut client = ManagerClient::connect(DEFAULT_ADDR).await?;
    let request = tonic::Request::new(UpdateNodeDynamicRequest {
        id: "worker-a".into(),
        dynamic_diff: "{mike: \"nomitch\"}".into()
    });

    let response = client.update_node_dynamic(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
