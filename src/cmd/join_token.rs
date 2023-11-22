use std::error;
use clap::{arg, Command};

pub fn config() -> Command {
    Command::new("join-token")
        .about("Creates a join token for a worker node")
        .arg(arg!(-c --config [CONFIG] "test"))
}
pub async fn handle(_sub_matches: &clap::ArgMatches) -> Result<(), Box<dyn error::Error>> {

    // TODO:

    // parse arguments
    // one-time token, or persistent token?

    // request token from the server

    // print response & instructions
    println!("Generated token: TOKEN");
    println!("To launch a client with this token, run: ");
    println!("legion start worker --join-token TOKEN");

    Ok(())
}
