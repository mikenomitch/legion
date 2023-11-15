use crate::cmd::structs::{AgentConfig, WorkerConfig};
use crate::constants::DEFAULT_CONFIG_PATH;
use crate::manager::server::{run_follower, run_leader};
use crate::worker::agent::run_worker;
use jsonnet::JsonnetVm;
use std::error;

use clap::{arg, Command};

#[allow(non_snake_case)]
pub mod legion {
    tonic::include_proto!("legion");
}

pub fn config() -> Command {
    let leader_cmd = Command::new("leader")
        .about("Start a leader manager node")
        .arg(arg!(-c --config [CONFIG] "test"));

    let follower_cmd = Command::new("follower")
        .about("Start a follower manager node")
        .arg(arg!(port: [PORT]))
        .arg(arg!(-c --config [CONFIG] "path to configuration file agent"));

    let worker_cmd = Command::new("worker")
        .about("Start a worker node")
        .arg(arg!(port: [PORT]))
        .arg(arg!(-c --config [CONFIG] "test"));

    Command::new("start")
        .about("Start an agent")
        .arg(arg!(-c --config [CONFIG] "test"))
        .subcommand(leader_cmd)
        .subcommand(follower_cmd)
        .subcommand(worker_cmd)
}

pub async fn handle(sub_matches: &clap::ArgMatches) -> Result<(), Box<dyn error::Error>> {
    let config_path = sub_matches
        .get_one::<String>("config")
        .map(|s| s.as_str())
        .unwrap_or(DEFAULT_CONFIG_PATH);

    if let Some(("worker", _)) = sub_matches.subcommand() {
        let config = parse_worker_config(config_path)?;
        run_worker(config).await
    } else {
        let config = parse_config(config_path)?;
        if let Some(("leader", _)) = sub_matches.subcommand() {
            run_leader(config).await
        } else if config.manager.leader {
            run_leader(config).await
        } else {
            run_follower(config).await
        }
    }
}

fn parse_config(config_path: &str) -> Result<AgentConfig, Box<dyn error::Error>> {
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

fn parse_worker_config(config_path: &str) -> Result<WorkerConfig, Box<dyn error::Error>> {
    // Pluginify jsonnet
    let mut vm = JsonnetVm::new();
    let res = vm.evaluate_file(config_path);

    match res {
        Ok(s) => {
            let config: WorkerConfig = serde_json::from_str(&s)?;
            Ok(config)
        }
        Err(_) => Err("Failed to parse config file".into()),
    }
}
