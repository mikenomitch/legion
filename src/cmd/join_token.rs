use std::error;
use clap::{arg, Command};
// use log::debug;

#[allow(non_snake_case)]
pub mod legion {
    tonic::include_proto!("legion");
}

// use legion::manager_client::ManagerClient;
// use crate::cmd::job::legion::WorkerJoinRequest;

use legion::insecure_manager_client::InsecureManagerClient;
// use legion::manager_server::{Manager, ManagerServer};
use legion::{
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
    // TODO:
    // Add: one-time token or persistent token argument

    // request token from the server
    // TODO: Eventually this should pick up the address from env var
    let address = "http://localhost:4748";
    let mut client = InsecureManagerClient::connect(address).await?;
    let request = tonic::Request::new(GetJoinTokenRequest {
        one_time: false,
    });

    let response = client.get_join_token(request).await?;
    println!("RESPONSE={:?}", response);

    let t = response.get_ref().token.to_string();

    // print response & instructions
    println!("Generated token: {}", t);
    println!("To launch a client with this token, run: ");
    println!("legion start worker --join-token TOKEN");

    Ok(())
}
