use legion::greeter_client::GreeterClient;
use legion::HelloRequest;

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

pub async fn handle(sub_matches: &clap::ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    match sub_matches.subcommand() {
        Some(("run", _)) => get_from_server().await,
        Some(("status", _)) => get_from_server().await,
        _ => unreachable!(),
    }
}

async fn get_from_server() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = GreeterClient::connect("http://[::1]:50052").await?;

    let request = tonic::Request::new(HelloRequest {
        name: "Mike".into(),
    });

    let response = client.say_hello(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
