use std::error;
use clap::{arg, Command};
use crate::constants::DEFAULT_INSECURE_ADDR;


// use crate::cmd::job::legion::WorkerJoinRequest;

use crate::structs::insecure_manager_client::InsecureManagerClient;
// use crate::structs::manager_server::{Manager, ManagerServer};
use crate::structs::{
    // WorkerJoinReply,
    // WorkerJoinRequest,
    GetJoinTokenRequest
};


pub fn config() -> Command {
    Command::new("join-token")
        .about("Creates a join token for a worker node")
        .arg(arg!(-c --config [CONFIG] "test"))
}
pub async fn handle(_sub_matches: &clap::ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // TODO: add one-time token or persistent token argument

    let address = std::env::var("LEGION_ADDR").
        unwrap_or(DEFAULT_INSECURE_ADDR.to_owned());

    let mut client = InsecureManagerClient::connect(address).await?;
    let request = tonic::Request::new(GetJoinTokenRequest {
        one_time: false,
    });

    let response = client.get_join_token(request).await?;
    println!("RESPONSE={:?}", response);

    let t = &response.get_ref().token;

    // print response & instructions
    println!("Generated token: {}", t);
    println!("To launch a client with this token, run: ");
    println!("legion start worker --join-token TOKEN");

    Ok(())
}
