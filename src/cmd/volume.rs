//! `legion volume` — inspect and manage the storage behind stateful workloads.
//!
//! See `docs/volumes.md`. The jobs this covers:
//!
//! - *"Where did my database's data go?"* — `volume list` / `volume get`.
//! - *"Why is this replica stuck pending?"* — the pin and `lastError` columns.
//! - *"That node is never coming back"* — `volume delete --force`.
//! - *"What storage can this cluster do?"* — `volume drivers`.
//! - *"Does my driver behave?"* — `volume driver test`.

use std::collections::BTreeMap;

use tabled::{Table, Tabled};

use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    DeleteVolumeRequest, GetVolumeRequest, ListVolumeDriversRequest, ListVolumesRequest, VolumeInfo,
};

#[derive(Tabled)]
struct VolumeRow {
    #[tabled(rename = "VOLUME")]
    volume: String,
    #[tabled(rename = "DRIVER")]
    driver: String,
    #[tabled(rename = "JOB")]
    job: String,
    #[tabled(rename = "INSTANCE")]
    instance: String,
    #[tabled(rename = "STATE")]
    state: String,
    #[tabled(rename = "NODE")]
    node: String,
}

impl From<&VolumeInfo> for VolumeRow {
    fn from(volume: &VolumeInfo) -> Self {
        VolumeRow {
            volume: volume.volume_id.clone(),
            driver: volume.driver.clone(),
            job: volume.job_id.clone().unwrap_or_else(|| "—".into()),
            instance: volume.instance_id.clone().unwrap_or_else(|| "—".into()),
            state: volume.state.clone(),
            // The pin marker matters at a glance: `node-a1*` means this
            // instance can only be replaced there.
            node: match volume.node_id.as_deref() {
                Some(node) if volume.node_local => format!("{node}*"),
                Some(node) => node.to_string(),
                None => "—".into(),
            },
        }
    }
}

#[derive(Tabled)]
struct DriverRow {
    #[tabled(rename = "DRIVER")]
    driver: String,
    #[tabled(rename = "SCOPE")]
    scope: String,
    #[tabled(rename = "NODES")]
    nodes: String,
    #[tabled(rename = "REQUIRED PARAMS")]
    required: String,
    #[tabled(rename = "DETAIL")]
    detail: String,
}

pub fn config() -> Command {
    Command::new("volume")
        .about("Inspect and manage volumes (persistent storage for stateful tasks)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List volumes, optionally filtered")
                .long_about(
                    "List volumes.\n\n\
                     A `*` after the node id marks a NODE-LOCAL volume: its instance can only \
                     be replaced on that node. Destroyed volumes are hidden: --all (or \
                     --state destroyed) shows their tombstones, so you can still see where a \
                     job's storage went.",
                )
                .arg(Arg::new("job").long("job").short('J').help("Filter by job"))
                .arg(Arg::new("node").long("node").help("Filter by node"))
                .arg(Arg::new("driver").long("driver").help("Filter by driver"))
                .arg(
                    Arg::new("instance")
                        .long("instance")
                        .help("Filter by owning instance"),
                )
                .arg(
                    Arg::new("state")
                        .long("state")
                        .help("Filter by state: pending, created, destroying, destroyed"),
                )
                .arg(
                    Arg::new("all")
                        .long("all")
                        .short('a')
                        .action(ArgAction::SetTrue)
                        .help("Include destroyed volumes (tombstones of reclaimed storage)"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("get")
                .about("Show one volume, including its driver params and last driver error")
                .arg(Arg::new("id").help("Volume id").required(true))
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("delete")
                .about("Delete a volume and reclaim its storage")
                .long_about(
                    "Detach a volume from its instance and queue its storage for the owning \
                     worker's driver.\n\n\
                     The instance mints a fresh (empty) volume on its next placement, so this \
                     DESTROYS DATA. Reclaim is confirmed asynchronously — the volume sits in \
                     `destroying` until the driver reports success, and the manager retries \
                     every heartbeat until it does.\n\n\
                     --force skips driver confirmation and tombstones the volume immediately. \
                     Use it only when the owning node is gone for good: whatever bytes exist \
                     there are left behind.",
                )
                .arg(Arg::new("id").help("Volume id").required(true))
                .arg(
                    Arg::new("force")
                        .long("force")
                        .action(ArgAction::SetTrue)
                        .help("Tombstone without driver confirmation (node is gone for good)"),
                )
                .arg(
                    Arg::new("idempotency-key")
                        .long("idempotency-key")
                        .help("Retry-safe key; a repeat returns the first result"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("drivers")
                .about("List the volume drivers installed across the cluster")
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("driver")
                .about("Volume driver authoring tools")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("test")
                        .about("Run the volume contract conformance suite against a driver")
                        .long_about(
                            "Run a driver through the full contract locally:\n\n  \
                             capabilities -> mount -> write -> unmount -> mount -> read back \
                             -> unmount -> destroy\n\n\
                             plus a double-call idempotency check on every operation. This is \
                             the same suite Legion's own tests run, so a driver that passes \
                             here behaves correctly in a cluster.\n\n\
                             Works against built-ins and against exec drivers under \
                             plugins/volumes/<name>/.",
                        )
                        .arg(Arg::new("name").help("Driver name").required(true))
                        .arg(
                            Arg::new("param")
                                .long("param")
                                .short('p')
                                .action(ArgAction::Append)
                                .value_name("KEY=VALUE")
                                .help("Driver param (repeatable), e.g. -p server=10.0.0.1"),
                        )
                        .arg(
                            Arg::new("mode")
                                .long("mode")
                                .value_parser(["filesystem", "block"])
                                .default_value("filesystem")
                                .help("Volume mode to exercise"),
                        )
                        .arg(
                            Arg::new("dir").long("dir").help(
                                "Scratch directory for the test volume (default: a temp dir)",
                            ),
                        )
                        .arg(json_flag()),
                ),
        )
}

#[instrument(err, skip_all)]
pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("list", sub)) => list(sub).await,
        Some(("get", sub)) => get(sub).await,
        Some(("delete", sub)) => delete(sub).await,
        Some(("drivers", sub)) => drivers(sub).await,
        Some(("driver", sub)) => match sub.subcommand() {
            Some(("test", sub)) => test_driver(sub),
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

async fn list(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let request = tonic::Request::new(ListVolumesRequest {
        job_id: matches.get_one::<String>("job").cloned(),
        node_id: matches.get_one::<String>("node").cloned(),
        driver: matches.get_one::<String>("driver").cloned(),
        instance_id: matches.get_one::<String>("instance").cloned(),
        state: matches.get_one::<String>("state").cloned(),
        include_destroyed: matches.get_flag("all"),
    });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to list volumes",
        client.list_volumes(request).await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&reply.volumes);
        return Ok(());
    }

    if reply.volumes.is_empty() {
        println!(
            "No volumes. Declare storage with `stateful: {{ storage: {{ path: '/data' }} }}` \
             on a task. Destroyed volumes are hidden; --all shows them."
        );
        return Ok(());
    }
    let rows: Vec<VolumeRow> = reply.volumes.iter().map(VolumeRow::from).collect();
    println!("{}", Table::new(rows));
    if reply.volumes.iter().any(|volume| volume.node_local) {
        println!("\n* node-local: the owning instance can only be replaced on that node.");
    }
    // A stuck volume is the thing an operator is usually here to find.
    for volume in reply.volumes.iter().filter(|v| v.last_error.is_some()) {
        println!(
            "! {} last error: {}",
            volume.volume_id,
            volume.last_error.as_deref().unwrap_or_default()
        );
    }
    Ok(())
}

async fn get(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let id = matches
        .get_one::<String>("id")
        .context("volume id")?
        .clone();
    let request = tonic::Request::new(GetVolumeRequest { volume_id: id });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to get volume",
        client.get_volume(request).await,
    )?
    .into_inner();
    let volume = reply.volume.context("manager returned no volume")?;

    if format.is_json() {
        output::print_success(&volume);
        return Ok(());
    }

    println!("Volume:     {}", volume.volume_id);
    println!("Name:       {}", volume.name);
    println!("Driver:     {}", volume.driver);
    println!("State:      {}", volume.state);
    println!(
        "Scope:      {}",
        if volume.node_local {
            "node-local (instance is pinned to its node)"
        } else {
            "network (instance reschedules freely)"
        }
    );
    if let Some(job) = volume.job_id.as_deref() {
        println!("Job:        {job}");
    }
    if let Some(instance) = volume.instance_id.as_deref() {
        println!("Instance:   {instance}");
    }
    if let Some(task) = volume.task_name.as_deref() {
        println!("Task:       {task}");
    }
    if let Some(node) = volume.node_id.as_deref() {
        println!("Node:       {node}");
    }
    if let Some(path) = volume.mount_path.as_deref() {
        println!("Mounted at: {path}");
    }
    if volume.read_only {
        println!("Read-only:  yes");
    }
    if volume.params != "{}" && !volume.params.is_empty() {
        println!("Params:     {}", volume.params);
    }
    if volume.attrs != "{}" && !volume.attrs.is_empty() {
        println!("Metadata:   {}", volume.attrs);
    }
    if let Some(error) = volume.last_error.as_deref() {
        println!("Last error: {error}");
    }
    println!("Created:    {}", volume.created_at);
    println!("Updated:    {}", volume.updated_at);
    Ok(())
}

async fn delete(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let id = matches
        .get_one::<String>("id")
        .context("volume id")?
        .clone();
    let request = tonic::Request::new(DeleteVolumeRequest {
        volume_id: id,
        force: matches.get_flag("force"),
        idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
    });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to delete volume",
        client.delete_volume(request).await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "volume": reply.volume,
            "deduplicated": reply.deduplicated,
        }));
    } else {
        println!("{}", reply.message);
    }
    Ok(())
}

async fn drivers(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to list volume drivers",
        client
            .list_volume_drivers(tonic::Request::new(ListVolumeDriversRequest {}))
            .await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&reply.drivers);
        return Ok(());
    }

    if reply.drivers.is_empty() {
        println!("No volume drivers advertised. Are any workers connected?");
        return Ok(());
    }
    let rows = reply.drivers.iter().map(|driver| DriverRow {
        driver: driver.name.clone(),
        scope: match (driver.node_local, driver.ephemeral) {
            (_, true) => "ephemeral".to_string(),
            (true, false) => "node-local".to_string(),
            (false, false) => "network".to_string(),
        },
        // "2/3" reads as "installed on 3, usable on 2" — the gap is the thing
        // worth investigating.
        nodes: format!(
            "{}/{}",
            driver.available_nodes.len(),
            driver.nodes.len().max(driver.available_nodes.len())
        ),
        required: if driver.required_params.is_empty() {
            "—".to_string()
        } else {
            driver.required_params.join(",")
        },
        detail: driver.detail.clone(),
    });
    println!("{}", Table::new(rows));
    println!("\nNODES is usable/installed. `ephemeral` drivers cannot back stateful.storage.");
    Ok(())
}

/// Run the contract suite locally against a built-in or exec driver. Needs no
/// manager: a driver author iterates against this alone.
fn test_driver(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches
        .get_one::<String>("name")
        .context("driver name")?
        .clone();

    let scratch = match matches.get_one::<String>("dir") {
        Some(dir) => std::path::PathBuf::from(dir),
        None => std::env::temp_dir().join(format!(
            "legion-volume-test-{}",
            crate::util::random_string(8)
        )),
    };
    std::fs::create_dir_all(&scratch)
        .with_context(|| format!("failed to create scratch dir {}", scratch.display()))?;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    for raw in matches.get_many::<String>("param").unwrap_or_default() {
        let (key, value) = raw
            .split_once('=')
            .with_context(|| format!("--param expects KEY=VALUE, got {raw:?}"))?;
        params.insert(key.to_string(), Value::String(value.to_string()));
    }

    // `host` needs the operator's declarations, which live in the worker
    // config; a local run gets none, so it will report itself unavailable —
    // documented in the conformance output rather than silently passing.
    let registry =
        crate::volumes::VolumeDriverRegistry::with_discovery(crate::volumes::VolumeRuntimeConfig {
            volume_root: scratch.join("volumes"),
            data_dir: scratch.clone(),
            host_volumes: HashMap::new(),
            plugin_dir: crate::util::plugin_root_dir(),
        });
    let driver = registry.require(&name)?;
    let mode: crate::volumes::VolumeMode = matches
        .get_one::<String>("mode")
        .map(String::as_str)
        .unwrap_or("filesystem")
        .parse()
        .map_err(|err: String| anyhow!(err))?;
    let report = crate::volumes::conformance::run(driver, &scratch, params, mode);

    if format.is_json() {
        output::print_success(&report);
    } else {
        println!(
            "Volume plugin conformance: {} ({mode} mode)\n",
            report.driver
        );
        for check in &report.checks {
            let mark = match (check.passed, check.skipped) {
                (_, true) => "-",
                (true, _) => "✓",
                (false, _) => "✗",
            };
            println!("  {mark} {:<24} {}", check.name, check.detail);
        }
        println!();
        if report.passed {
            println!("PASS — {} honors the volume contract.", report.driver);
        } else {
            println!(
                "FAIL — {} check(s) failed. See docs/volumes.md \"Writing a volume driver\".",
                report.failures().len()
            );
        }
    }

    // Only clean up what we created, so `--dir` output stays inspectable.
    if matches.get_one::<String>("dir").is_none() {
        let _ = std::fs::remove_dir_all(&scratch);
    }

    if report.passed {
        Ok(())
    } else {
        // Non-zero exit so CI can gate on it.
        bail!("volume driver {} failed conformance", report.driver);
    }
}
