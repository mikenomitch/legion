use crate::prelude::*;

use crate::manager::daemon::run_manager;
use crate::structs::ManagerConfig;
use crate::worker::daemon::run_worker;
use crate::worker::simulate::{run_simulated_workers, SimulateOptions};

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
                .conflicts_with("simulate")
                .help("Override the worker name/ID"),
        )
        .arg(
            Arg::new("o11y")
                .long("o11y")
                .action(ArgAction::SetTrue)
                .help("Enable tracing export for observability UI"),
        )
        .arg(
            Arg::new("simulate")
                .long("simulate")
                .value_name("N")
                .value_parser(clap::value_parser!(usize))
                .help(
                    "Run N simulated nodes in this process instead of one real worker \
                     (benchmarking/load testing; see docs/worker-simulation.md)",
                ),
        )
        .arg(
            Arg::new("profiles")
                .long("profiles")
                .value_name("FILE")
                .value_hint(ValueHint::FilePath)
                .requires("simulate")
                .help(
                    "JSON file with a `node_profiles` array (benchmark scenario schema); \
                     N is split across profiles by their `count` weights",
                ),
        )
        .arg(
            Arg::new("name-prefix")
                .long("name-prefix")
                .value_name("PREFIX")
                .requires("simulate")
                .help("Simulated node id prefix (ids are <prefix>-<profile>-<idx>; default: sim)"),
        )
        .arg(
            Arg::new("ready-after")
                .long("ready-after")
                .value_name("DURATION")
                .requires("simulate")
                .help(
                    "Delay before a dispatched allocation is reported running \
                     (e.g. 500ms, 2s; default: 0s)",
                ),
        )
        .arg(
            Arg::new("register-rate")
                .long("register-rate")
                .value_name("N_PER_SEC")
                .value_parser(clap::value_parser!(f64))
                .requires("simulate")
                .help("Registration ramp in nodes/second (default: 100)"),
        )
        .arg(
            Arg::new("advertise-host")
                .long("advertise-host")
                .value_name("HOST")
                .requires("simulate")
                .help(
                    "Host the manager should dial for task dispatch to this fleet \
                     (default: 127.0.0.1)",
                ),
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
            if let Some(count) = arg_matches.get_one::<usize>("simulate") {
                let opts = simulate_options(*count, arg_matches)?;
                return run_simulated_workers(token, opts).await;
            }
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
    let rendered = crate::config_lang::eval_file(config_path)
        .with_context(|| format!("failed to evaluate manager config {config_path}"))?;

    let config: ManagerConfig = serde_json::from_str(&rendered)
        .with_context(|| format!("failed to deserialize manager config from {config_path}"))?;

    Ok(config)
}

/// Builds `SimulateOptions` from the `start worker --simulate` flag set.
fn simulate_options(count: usize, arg_matches: &ArgMatches) -> R<SimulateOptions> {
    let defaults = SimulateOptions::default();
    let ready_after = match arg_matches.get_one::<String>("ready-after") {
        Some(raw) => humantime::parse_duration(raw.trim())
            .with_context(|| format!("invalid --ready-after duration: {raw}"))?,
        None => defaults.ready_after,
    };
    Ok(SimulateOptions {
        count,
        profiles_path: arg_matches.get_one::<String>("profiles").cloned(),
        name_prefix: arg_matches
            .get_one::<String>("name-prefix")
            .cloned()
            .unwrap_or(defaults.name_prefix),
        ready_after,
        register_rate: arg_matches
            .get_one::<f64>("register-rate")
            .copied()
            .unwrap_or(defaults.register_rate),
        advertise_host: arg_matches
            .get_one::<String>("advertise-host")
            .cloned()
            .unwrap_or(defaults.advertise_host),
    })
}

fn config_path_arg() -> Arg {
    Arg::new("config")
        .short('c')
        .long("config")
        .value_name("CONFIG")
        .help("Path to the manager configuration file")
        .value_hint(ValueHint::FilePath)
}
