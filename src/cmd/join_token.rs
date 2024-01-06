use crate::prelude::*;

use crate::structs::insecure_manager_client::InsecureManagerClient;
use crate::structs::GetJoinTokenRequest;
use std::error;

pub fn config() -> Command {
    Command::new("join-token")
        .about("Creates a join token for a worker node")
        .arg(arg!(-t --type [TYPE] "type of worker to get join token for"))
        .arg(arg!(--ttl [TTL] "ttl of join token in seconds. 0 means infinite. Server adds 1 minute leeway"))
}

pub async fn handle(sub_matches: &clap::ArgMatches) -> Result<(), Box<dyn error::Error>> {
    let ttl = sub_matches
        .get_one::<String>("ttl")
        .map_or("1h", String::as_str);

    let worker_type = sub_matches
        .get_one::<String>("type")
        .map_or("default", String::as_str);

    let address = std::env::var("LEGION_ADDR").unwrap_or(DEF_INSECURE_ADDR.to_owned());

    let mut client = InsecureManagerClient::connect(address).await?;

    let req_ttl: Option<String> = if ttl == "0" || ttl == "none" || ttl == "skip" {
        None
    } else {
        Some(ttl.to_owned())
    };

    let request = tonic::Request::new(GetJoinTokenRequest {
        ttl: req_ttl,
        worker_type: Some(worker_type.into()),
    });

    let response = client.get_join_token(request).await?;
    let t = &response.get_ref().token;

    println!("To launch a client with this token, run: ");
    println!("cr start worker {t}");

    Ok(())
}
