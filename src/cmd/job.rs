use crate::constants::{DEFAULT_ADDR, DEFAULT_PORT, DEFAULT_PROTOCOL};
use std::error;
use clap::{arg, Command};

#[allow(non_snake_case)]
pub mod legion {
    tonic::include_proto!("legion");
}
use legion::manager_node_client::ManagerNodeClient;
use legion::{RunJobRequest, GetJobStatusRequest};

pub fn config() -> Command {
    let job_run_cmd = Command::new("run")
        .about("Run a job")
        .arg(arg!(path: [PATH]));

    let job_status_cmd = Command::new("status")
        .about("Get job status");

    Command::new("job")
        .about("Interact with jobs")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(job_run_cmd)
        .subcommand(job_status_cmd)
}
pub async fn handle(sub_matches: &clap::ArgMatches) -> Result<(), Box<dyn error::Error>> {
    match sub_matches.subcommand() {
        Some(("run", _)) => run_job().await,
        Some(("status", _)) => get_status().await,
        _ => unreachable!(),
    }
}

async fn run_job() -> Result<(), Box<dyn error::Error>> {
    let addr = format!("{}://{}:{}", DEFAULT_PROTOCOL, DEFAULT_ADDR, DEFAULT_PORT);
    let mut client = ManagerNodeClient::connect(addr).await?;

    let request = tonic::Request::new(RunJobRequest {
        name: "Mike".into(),
    });

    let response = client.run_job(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}

async fn get_status() -> Result<(), Box<dyn error::Error>> {
    let addr = format!("{}://{}:{}", DEFAULT_PROTOCOL, DEFAULT_ADDR, DEFAULT_PORT);
    let mut client = ManagerNodeClient::connect(addr).await?;

    let request = tonic::Request::new(GetJobStatusRequest {
        name: "Mike".into(),
    });

    let response = client.get_job_status(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
