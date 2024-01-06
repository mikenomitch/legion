use crate::prelude::*;

use crate::structs::manager_client::ManagerClient;
use crate::structs::{GetJobStatusRequest, RunJobRequest};

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

pub async fn handle(sub_matches: &clap::ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("run", _)) => run_job().await,
        Some(("status", _)) => get_status().await,
        _ => unreachable!(),
    }
}

async fn run_job() -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS).await?;

    let request = tonic::Request::new(RunJobRequest {
        name: "Mike".into(),
    });

    let response = client.run_job(request).await?;

    info!("RESPONSE={:?}", response);

    Ok(())
}

async fn get_status() -> R<()> {
    let mut client = ManagerClient::connect(DEF_MANAGER_ADDRESS).await?;

    let request = tonic::Request::new(GetJobStatusRequest {
        name: "Mike".into(),
    });

    let response = client.get_job_status(request).await?;

    info!("RESPONSE={:?}", response);

    Ok(())
}
