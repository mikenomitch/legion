use crate::prelude::*;

use crate::manager::daemon::run_manager;
use crate::structs::ManagerConfig;
use crate::worker::daemon::run_worker;

use jsonnet::JsonnetVm;

pub fn config() -> Command {
    let manager_cmd = Command::new("manager")
        .about("Start a manager node")
        .arg(config_path_arg())
        .arg(
            Arg::new("o11y")
                .long("o11y")
                .action(ArgAction::SetTrue)
                .help("Enable tracing export for observability UI"),
        )
        .arg(
            Arg::new("run-worker")
                .long("run-worker")
                .action(ArgAction::SetTrue)
                .help("Run a worker node alongside the manager"),
        )
        .arg(
            Arg::new("restore-from")
                .long("restore-from")
                .value_name("BACKUP")
                .value_hint(ValueHint::FilePath)
                .help(
                    "Restore the state store from a `legion state backup` file before starting \
                     (only when no state exists yet, unless --force-restore)",
                ),
        )
        .arg(
            Arg::new("force-restore")
                .long("force-restore")
                .action(ArgAction::SetTrue)
                .requires("restore-from")
                .help("Allow --restore-from to overwrite an existing state store"),
        );

    let worker_cmd = Command::new("worker")
        .about("Start a worker node")
        .arg(
            Arg::new("token")
                .value_name("TOKEN")
                .help("Join token for the worker")
                .required(true),
        )
        .arg(
            Arg::new("name")
                .long("name")
                .value_name("NAME")
                .help("Override the worker name/ID"),
        )
        .arg(
            Arg::new("o11y")
                .long("o11y")
                .action(ArgAction::SetTrue)
                .help("Enable tracing export for observability UI"),
        );

    Command::new("start")
        .about("Start a daemon")
        .arg(config_path_arg())
        .subcommand(manager_cmd)
        .subcommand(worker_cmd)
}

#[instrument(skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("worker", arg_matches)) => {
            let token = arg_matches
                .get_one::<String>("token")
                .context("Must provide token")?;
            let name = arg_matches.get_one::<String>("name").map(String::as_str);
            run_worker(token, name).await
        }
        Some(("manager", arg_matches)) => {
            let config_path = arg_matches
                .get_one::<String>("config")
                .map_or(DEF_CONFIG_PATH, String::as_str);
            let run_worker = arg_matches.get_flag("run-worker");
            let config = parse_config(config_path)?;
            // Restore runs BEFORE the store (and any raft engine) starts:
            // the backup becomes the initial state the engine adopts (plan
            // §A5; docs/upgrades.md "Backup and restore").
            if let Some(backup) = arg_matches.get_one::<String>("restore-from") {
                let force = arg_matches.get_flag("force-restore");
                let store_path = config.with_defaults().store_path;
                let restored = crate::storage::store::Store::restore_from_backup(
                    &store_path,
                    std::path::Path::new(backup),
                    force,
                )?;
                info!(
                    backup,
                    path = %restored.display(),
                    "Restored state store from backup; starting manager"
                );
            }
            run_manager(config, run_worker).await
        }
        _ => {
            let config_path = sub_matches
                .get_one::<String>("config")
                .map_or(DEF_CONFIG_PATH, String::as_str);
            let config = parse_config(config_path)?;
            run_manager(config, false).await
        }
    }
}

#[instrument(skip_all)]
fn parse_config(config_path: &str) -> R<ManagerConfig> {
    // Pluginify jsonnet
    let mut vm = JsonnetVm::new();
    let rendered = vm
        .evaluate_file(config_path)
        .map_err(|e| anyhow!("failed to evaluate config {config_path}: {e}"))?
        .to_string();

    let config: ManagerConfig = serde_json::from_str(&rendered)
        .with_context(|| format!("failed to deserialize manager config from {config_path}"))?;

    Ok(config)
}

fn config_path_arg() -> Arg {
    Arg::new("config")
        .short('c')
        .long("config")
        .value_name("CONFIG")
        .help("Path to the manager configuration file")
        .value_hint(ValueHint::FilePath)
}
