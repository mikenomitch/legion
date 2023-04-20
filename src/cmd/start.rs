use clap::{arg, Command};

use crate::constants::{DEFAULT_CONFIG_PATH, DEFAULT_PORT};
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
        Some(("leader", sms)) => {
            let config_path = sms.get_one::<String>("config").map(|s| s.as_str());

            match config_path {
                Some(config_path) => run_leader(config_path).await,
                None => run_leader(DEFAULT_CONFIG_PATH).await,
            }
        }

        Some(("follower", sms)) => {
            let port = sms
                .get_one::<String>("port")
                .map(|s| s.as_str())
                .unwrap_or(DEFAULT_PORT);
            let config_path = sms
                .get_one::<String>("config")
                .map(|s| s.as_str())
                .unwrap_or(DEFAULT_CONFIG_PATH);

            run_follower(config_path, port).await
        }

        _ => run_leader(DEFAULT_CONFIG_PATH).await,
    }
}
