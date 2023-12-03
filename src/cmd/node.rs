use crate::constants::DEFAULT_ADDR;
use crate::util::R;

use clap::Command;

pub mod legion {
    tonic::include_proto!("legion");
}
use legion::manager_client::ManagerClient;
use legion::GetNodesRequest;

pub fn config() -> Command {
    let get_nodes_cmd = Command::new("list").about("List nodes");

    Command::new("node")
        .about("Interact with nodes")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(get_nodes_cmd)
}

pub async fn handle(sub_matches: &clap::ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("list", _)) => get_nodes().await,
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
