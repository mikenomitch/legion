//! `legion volume` — inspect and manage the storage behind stateful workloads.
//!
//! See `docs/volumes.md`. The jobs this covers:
//!
//! - *"Where did my database's data go?"* — `volume list` / `volume get`.
//! - *"Why is this replica stuck pending?"* — the pin and `lastError` columns.
//! - *"That node is never coming back"* — `volume delete --force`.
//! - *"What storage can this cluster do?"* — `volume plugins`.
//! - *"Does my driver behave?"* — `volume plugin test`.

use std::collections::BTreeMap;

use tabled::{Table, Tabled};

use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    DeleteVolumeRequest, GetVolumeRequest, ListVolumePluginsRequest, ListVolumesRequest, VolumeInfo,
};

#[derive(Tabled)]
struct VolumeRow {
    #[tabled(rename = "VOLUME")]
    volume: String,
    #[tabled(rename = "PLUGIN")]
    plugin: String,
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
            plugin: volume.plugin.clone(),
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
struct PluginRow {
    #[tabled(rename = "PLUGIN")]
    plugin: String,
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
                     be replaced on that node. Destroyed volumes are retained as tombstones \
                     so you can still see where a job's storage went.",
                )
                .arg(Arg::new("job").long("job").short('J').help("Filter by job"))
                .arg(Arg::new("node").long("node").help("Filter by node"))
                .arg(Arg::new("plugin").long("plugin").help("Filter by plugin"))
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
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("get")
                .about("Show one volume, including its plugin params and last driver error")
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
            Command::new("plugins")
                .about("List the volume plugins installed across the cluster")
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("plugin")
                .about("Volume plugin authoring tools")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("test")
                        .about("Run the volume contract conformance suite against a plugin")
                        .long_about(
                            "Run a plugin through the full contract locally:\n\n  \
                             capabilities -> mount -> write -> unmount -> mount -> read back \
                             -> unmount -> destroy\n\n\
                             plus a double-call idempotency check on every operation. This is \
                             the same suite Legion's own tests run, so a plugin that passes \
                             here behaves correctly in a cluster.\n\n\
                             Works against built-ins and against exec plugins under \
                             plugins/volumes/<name>/.",
                        )
                        .arg(Arg::new("name").help("Plugin name").required(true))
                        .arg(
                            Arg::new("param")
                                .long("param")
                                .short('p')
                                .action(ArgAction::Append)
                                .value_name("KEY=VALUE")
                                .help("Plugin param (repeatable), e.g. -p server=10.0.0.1"),
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
        Some(("plugins", sub)) => plugins(sub).await,
        Some(("plugin", sub)) => match sub.subcommand() {
            Some(("test", sub)) => test_plugin(sub),
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
        plugin: matches.get_one::<String>("plugin").cloned(),
        instance_id: matches.get_one::<String>("instance").cloned(),
        state: matches.get_one::<String>("state").cloned(),
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
             on a task."
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
    println!("Plugin:     {}", volume.plugin);
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
        println!("Driver:     {}", volume.attrs);
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

async fn plugins(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to list volume plugins",
        client
            .list_volume_plugins(tonic::Request::new(ListVolumePluginsRequest {}))
            .await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&reply.plugins);
        return Ok(());
    }

    if reply.plugins.is_empty() {
        println!("No volume plugins advertised. Are any workers connected?");
        return Ok(());
    }
    let rows = reply.plugins.iter().map(|plugin| PluginRow {
        plugin: plugin.name.clone(),
        scope: match (plugin.node_local, plugin.ephemeral) {
            (_, true) => "ephemeral".to_string(),
            (true, false) => "node-local".to_string(),
            (false, false) => "network".to_string(),
        },
        // "2/3" reads as "installed on 3, usable on 2" — the gap is the thing
        // worth investigating.
        nodes: format!(
            "{}/{}",
            plugin.available_nodes.len(),
            plugin.nodes.len().max(plugin.available_nodes.len())
        ),
        required: if plugin.required_params.is_empty() {
            "—".to_string()
        } else {
            plugin.required_params.join(",")
        },
        detail: plugin.detail.clone(),
    });
    println!("{}", Table::new(rows));
    println!("\nNODES is usable/installed. `ephemeral` plugins cannot back stateful.storage.");
    Ok(())
}

/// Run the contract suite locally against a built-in or exec plugin. Needs no
/// manager: a plugin author iterates against this alone.
fn test_plugin(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let name = matches
        .get_one::<String>("name")
        .context("plugin name")?
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
        crate::volumes::VolumePluginRegistry::with_discovery(crate::volumes::VolumeRuntimeConfig {
            volume_root: scratch.join("volumes"),
            data_dir: scratch.clone(),
            host_volumes: HashMap::new(),
            plugin_dir: crate::util::plugin_root_dir(),
        });
    let plugin = registry.require(&name)?;
    let mode: crate::volumes::VolumeMode = matches
        .get_one::<String>("mode")
        .map(String::as_str)
        .unwrap_or("filesystem")
        .parse()
        .map_err(|err: String| anyhow!(err))?;
    let report = crate::volumes::conformance::run(plugin, &scratch, params, mode);

    if format.is_json() {
        output::print_success(&report);
    } else {
        println!(
            "Volume plugin conformance: {} ({mode} mode)\n",
            report.plugin
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
            println!("PASS — {} honors the volume contract.", report.plugin);
        } else {
            println!(
                "FAIL — {} check(s) failed. See docs/volumes.md \"Writing a volume plugin\".",
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
        bail!("volume plugin {} failed conformance", report.plugin);
    }
}
