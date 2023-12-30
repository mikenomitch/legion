use crate::util::R;
use crate::worker::server::run_worker;
use clap::{arg, Command};


pub fn config() -> Command {
    Command::new("join")
        .about("Join a cluster")
        .arg(arg!(token: [TOKEN]))
}

pub async fn handle(arg_matches: &clap::ArgMatches) -> R<()> {
    let token = arg_matches.get_one::<String>("token").ok_or("Must provide token")?;
    run_worker(token).await
}
