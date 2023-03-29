use legion::manager_node_client::ManagerNodeClient;
use legion::JoinClusterRequest;

use crate::constants::{DEFAULT_ADDR, DEFAULT_PORT, DEFAULT_PROTOCOL};
use crate::types::Res;

use clap::{arg, Command};
pub mod legion {
    tonic::include_proto!("legion");
}

pub fn config() -> Command {
    let job_run_cmd = Command::new("run")
        .about("Run a job")
        .arg(arg!(path: [PATH]));
    let job_status_cmd = Command::new("status").about("Get job status");

    Command::new("job")
        .about("Interact with jobs")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(job_run_cmd)
        .subcommand(job_status_cmd)
}

pub async fn handle(sub_matches: &clap::ArgMatches) -> Res<()> {
    match sub_matches.subcommand() {
        Some(("run", _)) => get_from_server().await,
        Some(("status", _)) => get_from_server().await,
        _ => unreachable!(),
    }
}

async fn get_from_server() -> Res<()> {
    let addr = format!("{}://{}:{}", DEFAULT_PROTOCOL, DEFAULT_ADDR, DEFAULT_PORT);
    let mut client = ManagerNodeClient::connect(addr).await?;

    let request = tonic::Request::new(JoinClusterRequest {
        address: "127.0.0.1:4646".into(),
    });

    let response = client.join_cluster(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
