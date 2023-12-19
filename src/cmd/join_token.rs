use std::error;
use clap::{arg, Command};
use crate::constants::DEFAULT_INSECURE_ADDR;
use crate::structs::insecure_manager_client::InsecureManagerClient;
use crate::structs::GetJoinTokenRequest;


pub fn config() -> Command {
    Command::new("join-token")
        .about("Creates a join token for a worker node")
        .arg(arg!(-t --type [TYPE] "type of worker to get join token for"))
}
pub async fn handle(sub_matches: &clap::ArgMatches) -> Result<(), Box<dyn error::Error>> {
    let ttl = 100;
    let worker_type = sub_matches
        .get_one::<String>("type")
        .map(|s| s.as_str())
        .unwrap_or("default");

    // TODO: add one-time token or persistent token argument
    let address = std::env::var("LEGION_ADDR").
        unwrap_or(DEFAULT_INSECURE_ADDR.to_owned());

    let mut client = InsecureManagerClient::connect(address).await?;
    let request = tonic::Request::new(GetJoinTokenRequest {
        ttl: Some(ttl),
        worker_type: Some(worker_type.into()),
    });

    let response = client.get_join_token(request).await?;
    let t = &response.get_ref().token;

    println!("To launch a client with this token, run: ");
    println!("cr start worker {}", t);

    Ok(())
}
