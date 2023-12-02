use crate::constants::{DEFAULT_HOSTNAME, DEFAULT_PORT, DEFAULT_PROTOCOL};
use crate::util::R;

use log::debug;
use clap::{arg, Command};

pub mod legion {
    tonic::include_proto!("legion");
}
use legion::manager_client::ManagerClient;
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
pub async fn handle(sub_matches: &clap::ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("run", _)) => run_job().await,
        Some(("status", _)) => get_status().await,
        _ => unreachable!(),
    }
}

async fn run_job() -> R<()> {
    let addr = format!("{}://{}:{}", DEFAULT_PROTOCOL, DEFAULT_HOSTNAME, DEFAULT_PORT);
    let mut client = ManagerClient::connect(addr).await?;

    let request = tonic::Request::new(RunJobRequest {
        name: "Mike".into(),
    });

    let response = client.run_job(request).await?;

    debug!("RESPONSE={:?}", response);

    Ok(())
}

async fn get_status() -> R<()> {
    let addr = format!("{}://{}:{}", DEFAULT_PROTOCOL, DEFAULT_HOSTNAME, DEFAULT_PORT);
    let mut client = ManagerClient::connect(addr).await?;

    let request = tonic::Request::new(GetJobStatusRequest {
        name: "Mike".into(),
    });

    let response = client.get_job_status(request).await?;

    debug!("RESPONSE={:?}", response);

    Ok(())
}
