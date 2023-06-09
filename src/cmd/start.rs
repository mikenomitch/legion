use crate::cmd::structs::AgentConfig;
use crate::constants::{DEFAULT_CONFIG_PATH};
use crate::manager::server::{run_follower, run_leader};
use crate::types::Res;
use jsonnet::JsonnetVm;

use clap::{arg, Command};

pub mod legion {
    tonic::include_proto!("legion");
}

pub fn config() -> Command {
    // let leader_cmd = Command::new("leader")
    //     .about("Start a leader manager node")
    //     .arg(arg!(-c --config [CONFIG] "test"));

    // let follower_cmd = Command::new("follower")
    //     .about("Start a follower manager node")
    //     .arg(arg!(port: [PORT]))
    //     .arg(arg!(-c --config [CONFIG] "path to configuration file agent"));

    Command::new("start")
        .about("Start an agent")
        .arg(arg!(-c --config [CONFIG] "test"))
        // .subcommand(leader_cmd)
        // .subcommand(follower_cmd)
}

pub async fn handle(sub_matches: &clap::ArgMatches) -> Res<()> {
    let config_path = sub_matches
        .get_one::<String>("config")
        .map(|s| s.as_str())
        .unwrap_or(DEFAULT_CONFIG_PATH);

    let config = parse_config(config_path)?;

    if config.manager.leader {
        run_leader(config).await
    } else {
        run_follower(config).await
    }
}

// run_leader(config).await
// match sub_matches.subcommand() {
//     Some(("leader", sms)) => {
//         let config_path = sms
//             .get_one::<String>("config")
//             .map(|s| s.as_str())
//             .unwrap_or(DEFAULT_CONFIG_PATH);

//         run_leader(config_path).await
//     }

//     Some(("follower", sms)) => {
//         let port = sms
//             .get_one::<String>("port")
//             .map(|s| s.as_str())
//             .unwrap_or(DEFAULT_PORT);

//         let config_path = sms
//             .get_one::<String>("config")
//             .map(|s| s.as_str())
//             .unwrap_or(DEFAULT_CONFIG_PATH);

//         run_follower(config_path, port).await
//     }

//     _ => run_leader(DEFAULT_CONFIG_PATH).await,
// }

fn parse_config(config_path: &str) -> Res<AgentConfig> {
    // Pluginify jsonnet
    let mut vm = JsonnetVm::new();
    let res = vm.evaluate_file(config_path);

    match res {
        Ok(s) => {
            let config: AgentConfig = serde_json::from_str(&s)?;
            Ok(config)
        }
        Err(_) => Err("Failed to parse config file".into()),
    }
}