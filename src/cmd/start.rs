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
        .arg(advertise_addr_arg())
        .arg(
            Arg::new("sim-no-work-stream")
                .long("sim-no-work-stream")
                .action(ArgAction::SetTrue)
                .requires("simulate")
                .help(
                    "Take work from the heartbeat's desired set instead of `StreamWork`, \
                     the delivery a worker without the work stream gets",
                ),
        )
        .arg(
            Arg::new("sim-checkpoints")
                .long("sim-checkpoints")
                .action(ArgAction::SetTrue)
                .requires("simulate")
                .help(
                    "Fake the checkpoint channel: advertise `checkpoint`, fingerprint as a \
                     sandbox host, and answer every capture, delete, and restore as done, \
                     so suspend, resume, and fork can be load tested",
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
            if let Some(addr) = arg_matches.get_one::<String>("advertise-addr") {
                crate::worker::plane::set_advertise_addr_flag(addr);
            }
            let name = arg_matches.get_one::<String>("name").map(String::as_str);
            run_worker(token, name).await
        }
        Some(("manager", arg_matches)) => {
            let run_worker = arg_matches.get_flag("run-worker");
            let config = load_config(arg_matches.get_one::<String>("config"))?;
            // Restore runs BEFORE the store (and any raft engine) starts:
            // the backup becomes the initial state the engine adopts (plan
            // §A5; docs/upgrades.md "Backup and restore").
            let restore_from = arg_matches.get_one::<String>("restore-from").cloned();
            if let Some(backup) = &restore_from {
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
            crate::manager::daemon::run_manager_with(config, run_worker, restore_from).await
        }
        _ => {
            let config = load_config(sub_matches.get_one::<String>("config"))?;
            run_manager(config, false).await
        }
    }
}

/// The manager config: the file `-c` names, or the built-in defaults when
/// there is no `-c`. Nothing is read from the working directory implicitly,
/// so `legion start manager` behaves the same wherever it is run.
fn load_config(config_path: Option<&String>) -> R<ManagerConfig> {
    match config_path {
        Some(path) => parse_config(path),
        None => Ok(serde_json::from_str("{}")?),
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
        advertise_addr: arg_matches
            .get_one::<String>("advertise-addr")
            .cloned()
            .or(defaults.advertise_addr),
        checkpoints: arg_matches.get_flag("sim-checkpoints"),
        observer: None,
        work_stream: !arg_matches.get_flag("sim-no-work-stream"),
    })
}

/// `--advertise-addr` for `legion start worker` and `legion join`: the host
/// (or `host:port`) managers and routers dial for this worker's plane.
pub(crate) fn advertise_addr_arg() -> Arg {
    Arg::new("advertise-addr")
        .long("advertise-addr")
        .value_name("HOST[:PORT]")
        .help(
            "Address managers dial for this worker's plane (mTLS, port 4848 by default). \
             Default: LEGION_WORKER_ADVERTISE_ADDR, else the local address of the route to the \
             manager",
        )
}

fn config_path_arg() -> Arg {
    Arg::new("config")
        .short('c')
        .long("config")
        .value_name("CONFIG")
        .help("Path to the manager configuration file (default: built-in defaults)")
        .value_hint(ValueHint::FilePath)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// P2-1: `legion start manager` with no `-c` must not read
    /// `./configs/manager.ncl` (which only exists in a repo checkout).
    #[test]
    fn no_config_flag_uses_the_built_in_defaults() -> R<()> {
        let config = load_config(None)?;
        let full = config.with_defaults();
        assert_eq!(full.server.port, DEF_MANAGER_SERVER_PORT);
        assert!(
            full.worker_config_path.is_empty(),
            "no worker config directory unless one is configured"
        );
        Ok(())
    }

    #[test]
    fn an_explicit_config_that_does_not_exist_is_an_error() {
        let missing = "/nonexistent/legion/manager.ncl".to_string();
        let err = load_config(Some(&missing)).expect_err("a named file must exist");
        assert!(format!("{err:#}").contains("manager.ncl"), "{err:#}");
    }
}
