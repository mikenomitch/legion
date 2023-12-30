use crate::cmd::structs::ManagerConfig;
use crate::constants::DEFAULT_CONFIG_PATH;
use crate::manager::agent::run_manager;
use crate::util::R;
use crate::worker::agent::run_worker;

use clap::{arg, Command};
use jsonnet::JsonnetVm;

pub fn config() -> Command {
    let manager_cmd = Command::new("manager")
        .about("Start a manager node")
        .arg(arg!(-c --config [CONFIG] "test"));

    let worker_cmd = Command::new("worker")
        .about("Start a worker node")
        .arg(arg!(token: [TOKEN]));

    Command::new("start")
        .about("Start an agent")
        .arg(arg!(-c --config [CONFIG] "test"))
        .subcommand(manager_cmd)
        .subcommand(worker_cmd)
}

pub async fn handle(sub_matches: &clap::ArgMatches) -> R<()> {
    if let Some(("worker", arg_matches)) = sub_matches.subcommand() {
        let token = arg_matches
            .get_one::<String>("token")
            .ok_or("Must provide token")?;
        run_worker(token).await
    } else {
        let config_path = sub_matches
            .get_one::<String>("config")
            .map(|s| s.as_str())
            .unwrap_or(DEFAULT_CONFIG_PATH);

        let config = parse_config(config_path)?;
        run_manager(config).await
    }
}

fn parse_config(config_path: &str) -> R<ManagerConfig> {
    // Pluginify jsonnet
    let mut vm = JsonnetVm::new();
    let res = vm.evaluate_file(config_path);

    match res {
        Ok(s) => {
            let config: ManagerConfig = serde_json::from_str(&s)?;
            Ok(config)
        }
        Err(e) => {
            println!("{}", e.as_str());
            Err("Failed to parse config file".into())
        }
    }
}
