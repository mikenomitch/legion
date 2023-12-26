use std::error;
use clap::{arg, Command};
use crate::constants::DEFAULT_INSECURE_ADDR;
use crate::structs::insecure_manager_client::InsecureManagerClient;
use crate::structs::GetJoinTokenRequest;


pub fn config() -> Command {
    Command::new("join-token")
        .about("Creates a join token for a worker node")
        .arg(arg!(-t --type [TYPE] "type of worker to get join token for"))
        .arg(arg!(--ttl [TTL] "ttl of join token in seconds. 0 means infinite. Server adds 1 minute leeway"))
}
pub async fn handle(sub_matches: &clap::ArgMatches) -> Result<(), Box<dyn error::Error>> {
    let ttl = sub_matches
        .get_one::<String>("ttl")
        .map(|s| s.as_str())
        .unwrap_or("1h");

    let worker_type = sub_matches
        .get_one::<String>("type")
        .map(|s| s.as_str())
        .unwrap_or("default");

    let address = std::env::var("LEGION_ADDR").
        unwrap_or(DEFAULT_INSECURE_ADDR.to_owned());

    let mut client = InsecureManagerClient::connect(address).await?;

    let req_ttl: Option<String>;
    // If 0 is specified, then set it to empty string
    // to mean infinite
    if ttl == "0" || ttl == "none" || ttl == "skip" {
        req_ttl = None;
    } else {
        req_ttl = Some(ttl.to_owned());
    }

    let request = tonic::Request::new(GetJoinTokenRequest {
        ttl: req_ttl,
        worker_type: Some(worker_type.into()),
    });

    let response = client.get_join_token(request).await?;
    let t = &response.get_ref().token;

    println!("To launch a client with this token, run: ");
    println!("cr start worker {}", t);

    Ok(())
}
