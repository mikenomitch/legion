use clap::{arg, Command};

use crate::constants::DEFAULT_PORT;
use crate::manager::start::{run_follower, run_leader};
use crate::types::Res;

pub mod legion {
    tonic::include_proto!("legion");
}

pub fn config() -> Command {
    let leader_cmd = Command::new("leader").about("Start a leader manager node");
    let follower_cmd = Command::new("follower")
        .about("Start a follower manager node")
        .arg(arg!(port: [PORT]));

    Command::new("start")
        .about("Start an agent")
        .subcommand(leader_cmd)
        .subcommand(follower_cmd)
}

pub async fn handle(sub_matches: &clap::ArgMatches) -> Res<()> {
    match sub_matches.subcommand() {
        Some(("leader", _)) => run_leader().await,
        Some(("follower", sms)) => {
            let port = sms.get_one::<String>("port").map(|s| s.as_str());

            match port {
                Some(port) => run_follower(port).await,
                None => run_follower(DEFAULT_PORT).await,
            }
        }
        _ => run_leader().await,
    }
}
