//! `legion snapshot` — capture, list, inspect, delete, and restore instance
//! snapshots (docs/suspend-and-resume.md).
//!
//! The jobs this covers:
//!
//! - *"Checkpoint this workspace before I let the agent loose on it"* —
//!   `snapshot create user-42 --name before-refactor --wait`.
//! - *"Undo what just happened"* — `snapshot restore user-42 before-refactor`.
//! - *"What snapshots exist, and how much disk do they hold?"* — `snapshot
//!   list`.
//! - *"Why did that snapshot fail?"* — `snapshot get`.
//! - *"Point new sandboxes at this week's golden image"* — `snapshot alias
//!   set agents agent-golden snap-...` (Phase 6), then `instance create
//!   agents --from-snapshot agent-golden`.
//! - *"Keep this checkpoint if its node dies"* — `snapshot create user-42
//!   --memory --publish` (Phase 7b).
//! - *"Have the us-west nodes ready to restore it fast"* — `snapshot warm
//!   py-ml-base --job agents --constraint region=us-west`.

use std::time::{Duration, Instant};

use tabled::{Table, Tabled};

use crate::cmd::helpers::{connect_manager_client, json_flag, AuthedManagerClient};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    CreateSnapshotRequest, DeleteSnapshotAliasRequest, DeleteSnapshotRequest, GetInstanceRequest,
    GetSnapshotRequest, ListEventsRequest, ListSnapshotsRequest, RestoreInstanceRequest,
    SetSnapshotAliasRequest, SnapshotInfo, WarmSnapshotRequest,
};

/// Default `--timeout` for `--wait`, in seconds. A memory snapshot of a
/// multi-GiB guest takes seconds; a disk snapshot on a filesystem without
/// reflinks copies every byte.
const DEFAULT_WAIT_SECS: u64 = 300;

#[derive(Tabled)]
struct SnapshotRow {
    #[tabled(rename = "SNAPSHOT")]
    snapshot: String,
    #[tabled(rename = "NAME")]
    name: String,
    #[tabled(rename = "INSTANCE")]
    instance: String,
    #[tabled(rename = "KIND")]
    kind: String,
    #[tabled(rename = "STATE")]
    state: String,
    #[tabled(rename = "NODE")]
    node: String,
    #[tabled(rename = "SIZE")]
    size: String,
    #[tabled(rename = "CREATED")]
    created: String,
}

fn human_bytes(bytes: u64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    if bytes == 0 {
        "-".into()
    } else if (bytes as f64) < MIB {
        format!("{:.0} KiB", bytes as f64 / 1024.0)
    } else if (bytes as f64) < 1024.0 * MIB {
        format!("{:.1} MiB", bytes as f64 / MIB)
    } else {
        format!("{:.2} GiB", bytes as f64 / (1024.0 * MIB))
    }
}

fn dash(value: &str) -> String {
    if value.is_empty() {
        "-".into()
    } else {
        value.to_string()
    }
}

impl From<&SnapshotInfo> for SnapshotRow {
    fn from(snapshot: &SnapshotInfo) -> Self {
        SnapshotRow {
            snapshot: snapshot.snapshot_id.clone(),
            name: dash(&snapshot.name),
            instance: dash(&snapshot.instance_id),
            kind: snapshot.kind.clone(),
            state: snapshot.state.clone(),
            node: dash(&snapshot.node_id),
            size: human_bytes(snapshot.total_bytes),
            created: snapshot.created_at.clone(),
        }
    }
}

fn wait_args(command: Command, what: &str) -> Command {
    command
        .arg(
            Arg::new("wait")
                .long("wait")
                .action(ArgAction::SetTrue)
                .help(format!("Block until {what}")),
        )
        .arg(
            Arg::new("timeout")
                .long("timeout")
                .value_name("SECONDS")
                .value_parser(clap::value_parser!(u64))
                .help("Maximum seconds to wait with --wait (default 300)"),
        )
}

fn idempotency_arg() -> Arg {
    Arg::new("idempotency-key")
        .long("idempotency-key")
        .value_name("KEY")
        .help("Deduplicate retries of this mutating request (safe for agent retries)")
}

pub fn config() -> Command {
    Command::new("snapshot")
        .about("Capture, list, delete, and restore instance snapshots")
        .long_about(
            "Snapshots are point-in-time captures of a running instance, stored on the node \
             that took them (docs/suspend-and-resume.md).\n\n\
             A disk snapshot copies the instance's stateful.storage volume and works for every \
             runtime whose volume driver supports snapshots. A memory snapshot also captures \
             the guest's memory, so a restore resumes running processes instead of booting; it \
             needs a sandbox task on Firecracker or cloud-hypervisor with a `snapshots` block.",
        )
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(wait_args(
            Command::new("create")
                .about("Snapshot a running instance")
                .arg(Arg::new("instance").help("Instance id").required(true))
                .arg(
                    Arg::new("memory")
                        .long("memory")
                        .action(ArgAction::SetTrue)
                        .conflicts_with("disk")
                        .help("Capture guest memory too (sandbox tasks on a real VMM)"),
                )
                .arg(
                    Arg::new("disk")
                        .long("disk")
                        .action(ArgAction::SetTrue)
                        .help("Capture the instance volume only (the default)"),
                )
                .arg(
                    Arg::new("name")
                        .long("name")
                        .value_name("NAME")
                        .help("Name to restore it by, unique within the job"),
                )
                .arg(
                    Arg::new("publish")
                        .long("publish")
                        .action(ArgAction::SetTrue)
                        .help(
                            "Publish it to the node's contentStore once captured, so it \
                             restores on any compatible node and survives the node (memory \
                             snapshots of sandboxes without stateful.storage)",
                        ),
                )
                .arg(idempotency_arg())
                .arg(json_flag()),
            "the capture finishes (state local or failed; published with --publish)",
        ))
        .subcommand(
            Command::new("list")
                .about("List snapshots, newest first")
                .arg(Arg::new("job").long("job").short('J').help("Filter by job"))
                .arg(
                    Arg::new("instance")
                        .long("instance")
                        .help("Filter by instance"),
                )
                .arg(
                    Arg::new("state").long("state").help(
                        "Filter by state: capturing, local, published, failed, deleting, lost",
                    ),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("get")
                .about("Show one snapshot, including its manifest")
                .arg(
                    Arg::new("snapshot")
                        .help("Snapshot id, or a name with --job")
                        .required(true),
                )
                .arg(
                    Arg::new("job")
                        .long("job")
                        .short('J')
                        .help("Job the named snapshot belongs to"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("delete")
                .about("Delete a snapshot and reclaim its bytes on its node")
                .arg(Arg::new("snapshot").help("Snapshot id").required(true))
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(wait_args(
            Command::new("restore")
                .about("Roll a keyed instance back to one of its snapshots, in place")
                .long_about(
                    "Stop the instance's allocation and start a replacement from the snapshot, \
                     on the node that holds it. The instance keeps its id and storage. A memory \
                     snapshot resumes the guest where it was captured. A failed restore fails \
                     the allocation with the reason, and the instance then boots cold on its \
                     retained storage.",
                )
                .arg(Arg::new("instance").help("Instance id").required(true))
                .arg(
                    Arg::new("snapshot")
                        .help("Snapshot id or name")
                        .required(true),
                )
                .arg(idempotency_arg())
                .arg(json_flag()),
            "the instance reports the restore's outcome",
        ))
        .subcommand(
            Command::new("alias")
                .about("Name a snapshot, or move or remove a name")
                .long_about(
                    "A name points at one snapshot of a job and can be moved, so a golden \
                     snapshot is used by name: `legion instance create <JOB> --from-snapshot \
                     <NAME>` restores from whatever the name points at now. Repointing never \
                     touches the snapshot the name moves away from. Only a manual snapshot \
                     that is local or published can be named.",
                )
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("set")
                        .about("Point a name at a snapshot, creating or moving it")
                        .arg(
                            Arg::new("job")
                                .help("Job the name belongs to")
                                .required(true),
                        )
                        .arg(Arg::new("name").help("The name").required(true))
                        .arg(
                            Arg::new("snapshot")
                                .help("Snapshot id, or another name of the job")
                                .required(true),
                        )
                        .arg(idempotency_arg())
                        .arg(json_flag()),
                )
                .subcommand(
                    Command::new("delete")
                        .about("Remove a name (the snapshot is untouched)")
                        .arg(
                            Arg::new("job")
                                .help("Job the name belongs to")
                                .required(true),
                        )
                        .arg(Arg::new("name").help("The name").required(true))
                        .arg(idempotency_arg())
                        .arg(json_flag()),
                ),
        )
        .subcommand(
            Command::new("warm")
                .about("Pre-fetch a published snapshot's parts onto matching nodes")
                .long_about(
                    "Ask every available node that matches the constraints, and could restore \
                     the snapshot, to fetch its parts from the content store into its node \
                     content store. A later resume, restore, or fork there starts from local \
                     bytes, and placement prefers those nodes. Best effort: warming never \
                     blocks placement, warmed parts are ordinary cache entries, and warming \
                     again is a no-op on nodes that already hold them. Only a published \
                     snapshot can be warmed.",
                )
                .arg(
                    Arg::new("snapshot")
                        .help("Snapshot id, or a name with --job")
                        .required(true),
                )
                .arg(
                    Arg::new("job")
                        .long("job")
                        .short('J')
                        .help("Job the named snapshot belongs to"),
                )
                .arg(
                    Arg::new("constraint")
                        .long("constraint")
                        .value_name("KEY=VALUE")
                        .action(ArgAction::Append)
                        .help("Only nodes whose attribute KEY equals VALUE (repeatable)"),
                )
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
}

#[instrument(skip_all)]
pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("create", sub)) => create(sub).await,
        Some(("list", sub)) => list(sub).await,
        Some(("get", sub)) => get(sub).await,
        Some(("delete", sub)) => delete(sub).await,
        Some(("restore", sub)) => restore(sub).await,
        Some(("alias", sub)) => match sub.subcommand() {
            Some(("set", sub)) => alias_set(sub).await,
            Some(("delete", sub)) => alias_delete(sub).await,
            _ => unreachable!(),
        },
        Some(("warm", sub)) => warm(sub).await,
        _ => unreachable!(),
    }
}

async fn alias_set(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let arg = |name: &str| matches.get_one::<String>(name).cloned().unwrap_or_default();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to name snapshot",
        client
            .set_snapshot_alias(tonic::Request::new(SetSnapshotAliasRequest {
                job_id: arg("job"),
                name: arg("name"),
                snapshot: arg("snapshot"),
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
            }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "job": arg("job"),
            "name": arg("name"),
            "snapshot": reply.snapshot_id,
            "previous": (!reply.previous_snapshot_id.is_empty())
                .then_some(reply.previous_snapshot_id.clone()),
            "message": reply.message,
            "deduplicated": reply.deduplicated,
        }));
    } else {
        println!("{}", reply.message);
    }
    Ok(())
}

async fn alias_delete(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let arg = |name: &str| matches.get_one::<String>(name).cloned().unwrap_or_default();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to remove snapshot name",
        client
            .delete_snapshot_alias(tonic::Request::new(DeleteSnapshotAliasRequest {
                job_id: arg("job"),
                name: arg("name"),
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
            }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "deduplicated": reply.deduplicated,
        }));
    } else {
        println!("{}", reply.message);
    }
    Ok(())
}

fn wait_timeout(matches: &ArgMatches) -> Duration {
    Duration::from_secs(
        matches
            .get_one::<u64>("timeout")
            .copied()
            .unwrap_or(DEFAULT_WAIT_SECS),
    )
}

/// Poll a snapshot until it leaves `capturing`, and with `until_published`
/// until its publish settles too (`published`, or `local` with the publish
/// error). `None` on timeout.
pub(crate) async fn wait_for_capture(
    client: &mut AuthedManagerClient,
    snapshot_id: &str,
    timeout: Duration,
    until_published: bool,
    format: OutputFormat,
) -> R<Option<SnapshotInfo>> {
    let start = Instant::now();
    loop {
        let snapshot = output::resolve(
            format,
            "failed to get snapshot while waiting",
            client
                .get_snapshot(tonic::Request::new(GetSnapshotRequest {
                    snapshot: snapshot_id.to_string(),
                    job_id: None,
                }))
                .await,
        )?
        .into_inner()
        .snapshot
        .context("manager returned no snapshot")?;
        // The manager retries a failed publish after a backoff, and says so
        // in the error; only a publish it gave up on ends the wait.
        let publishing = until_published
            && snapshot.state == "local"
            && (snapshot.error.is_empty() || snapshot.error.starts_with("publish attempt "));
        if snapshot.state != "capturing" && !publishing {
            return Ok(Some(snapshot));
        }
        if start.elapsed() >= timeout {
            return Ok(None);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn print_snapshot(snapshot: &SnapshotInfo) {
    println!("Snapshot:  {}", snapshot.snapshot_id);
    if !snapshot.name.is_empty() {
        println!("Name:      {}", snapshot.name);
    }
    println!("Job:       {}", snapshot.job_id);
    println!("Task:      {}", snapshot.task);
    println!("Instance:  {}", dash(&snapshot.instance_id));
    println!("Kind:      {}", snapshot.kind);
    println!("State:     {}", snapshot.state);
    if !snapshot.durability.is_empty() {
        println!("Durability: {}", snapshot.durability);
    }
    println!("Node:      {}", dash(&snapshot.node_id));
    if !snapshot.published_at.is_empty() {
        println!("Published: {}", snapshot.published_at);
    }
    println!("Size:      {}", human_bytes(snapshot.total_bytes));
    if snapshot.duration_ms > 0 {
        println!("Took:      {} ms", snapshot.duration_ms);
    }
    if snapshot.pause_ms > 0 {
        println!("Paused:    {} ms", snapshot.pause_ms);
    }
    if !snapshot.aliases.is_empty() {
        println!("Names:     {}", snapshot.aliases.join(", "));
    }
    println!("Created:   {}", snapshot.created_at);
    if !snapshot.expires_at.is_empty() {
        println!("Expires:   {}", snapshot.expires_at);
    }
    if !snapshot.error.is_empty() {
        println!("Error:     {}", snapshot.error);
    }
}

async fn create(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let instance = matches
        .get_one::<String>("instance")
        .context("instance id")?
        .clone();
    let kind = if matches.get_flag("memory") {
        "memory"
    } else {
        "disk"
    };
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to create snapshot",
        client
            .create_snapshot(tonic::Request::new(CreateSnapshotRequest {
                instance_id: instance,
                kind: kind.to_string(),
                name: matches.get_one::<String>("name").cloned(),
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
                publish: matches.get_flag("publish"),
            }))
            .await,
    )?
    .into_inner();
    let mut snapshot = reply.snapshot.context("manager returned no snapshot")?;
    let publish = matches.get_flag("publish");

    if matches.get_flag("wait") {
        match wait_for_capture(
            &mut client,
            &snapshot.snapshot_id,
            wait_timeout(matches),
            publish,
            format,
        )
        .await?
        {
            Some(done) => snapshot = done,
            None => {
                return Err(output::fail_local(
                    format,
                    CliError {
                        code: "DeadlineExceeded".into(),
                        message: format!(
                            "snapshot {} is still {} after {}s",
                            snapshot.snapshot_id,
                            if publish {
                                "capturing or publishing"
                            } else {
                                "capturing"
                            },
                            wait_timeout(matches).as_secs()
                        ),
                        hint: Some(
                            "check it later with `legion snapshot get`, or raise --timeout".into(),
                        ),
                        retryable: true,
                    },
                ));
            }
        }
        if snapshot.state == "failed" {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "SnapshotFailed",
                    format!(
                        "snapshot {} failed on node {}: {}",
                        snapshot.snapshot_id, snapshot.node_id, snapshot.error
                    ),
                    None,
                ),
            ));
        }
        if publish && snapshot.state == "local" && !snapshot.error.is_empty() {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "PublishFailed",
                    format!(
                        "snapshot {} was captured on node {} but could not be published: {}",
                        snapshot.snapshot_id, snapshot.node_id, snapshot.error
                    ),
                    Some(
                        "the snapshot still works from its node; fix the node's contentStore \
                         and take a new snapshot to publish one",
                    ),
                ),
            ));
        }
    }

    if format.is_json() {
        output::print_success(&json!({
            "snapshot": snapshot,
            "deduplicated": reply.deduplicated,
            "notes": reply.notes,
        }));
        return Ok(());
    }
    if reply.deduplicated {
        println!("(deduplicated: returning the snapshot this idempotency key created)");
    }
    for note in &reply.notes {
        println!("note: {note}");
    }
    print_snapshot(&snapshot);
    if snapshot.state == "capturing" {
        println!(
            "\nThe node captures it on its next heartbeat. Watch it with `legion snapshot get {}`.",
            snapshot.snapshot_id
        );
    }
    Ok(())
}

async fn list(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to list snapshots",
        client
            .list_snapshots(tonic::Request::new(ListSnapshotsRequest {
                job_id: crate::cmd::helpers::job_arg(matches, "job")?,
                instance_id: matches.get_one::<String>("instance").cloned(),
                state: matches.get_one::<String>("state").cloned(),
            }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply.snapshots);
        return Ok(());
    }
    if reply.snapshots.is_empty() {
        println!("No snapshots. Take one with `legion snapshot create <instance>`.");
        return Ok(());
    }
    let rows: Vec<SnapshotRow> = reply.snapshots.iter().map(SnapshotRow::from).collect();
    println!("{}", Table::new(rows));
    for snapshot in reply.snapshots.iter().filter(|s| !s.error.is_empty()) {
        println!(
            "! {} {}: {}",
            snapshot.snapshot_id, snapshot.state, snapshot.error
        );
    }
    Ok(())
}

async fn get(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to get snapshot",
        client
            .get_snapshot(tonic::Request::new(GetSnapshotRequest {
                snapshot: matches
                    .get_one::<String>("snapshot")
                    .context("snapshot")?
                    .clone(),
                job_id: crate::cmd::helpers::job_arg(matches, "job")?,
            }))
            .await,
    )?
    .into_inner();
    let snapshot = reply.snapshot.context("manager returned no snapshot")?;
    if format.is_json() {
        // The manifest is JSON already; nest it rather than double-encode.
        let manifest: Value = serde_json::from_str(&snapshot.manifest_json).unwrap_or(Value::Null);
        let mut data = serde_json::to_value(&snapshot)?;
        if let Some(object) = data.as_object_mut() {
            object.insert("manifest".into(), manifest);
        }
        output::print_success(&data);
        return Ok(());
    }
    print_snapshot(&snapshot);
    if !snapshot.manifest_json.is_empty() {
        let manifest: Value = serde_json::from_str(&snapshot.manifest_json).unwrap_or(Value::Null);
        println!("Manifest:\n{}", serde_json::to_string_pretty(&manifest)?);
    }
    Ok(())
}

async fn delete(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to delete snapshot",
        client
            .delete_snapshot(tonic::Request::new(DeleteSnapshotRequest {
                snapshot_id: matches
                    .get_one::<String>("snapshot")
                    .context("snapshot")?
                    .clone(),
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
            }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply);
    } else {
        println!("{}", reply.message);
    }
    Ok(())
}

/// The newest `snapshot.restored` / `snapshot.restore_failed` event for a
/// snapshot after event `after_id`, as `(ok, error)`.
async fn restore_outcome(
    client: &mut AuthedManagerClient,
    job_id: &str,
    snapshot_id: &str,
    after_id: i64,
    format: OutputFormat,
) -> R<Option<(bool, String)>> {
    let events = output::resolve(
        format,
        "failed to list events while waiting",
        client
            .list_events(tonic::Request::new(ListEventsRequest {
                after_id: Some(after_id),
                kind: Some("snapshot".into()),
                job_id: Some(job_id.to_string()),
                limit: Some(1000),
                ..Default::default()
            }))
            .await,
    )?
    .into_inner()
    .events;
    Ok(events
        .iter()
        .rev()
        .filter(|event| {
            event.kind == "snapshot.restored" || event.kind == "snapshot.restore_failed"
        })
        .find_map(|event| {
            let meta: Value = serde_json::from_str(&event.metadata_json).ok()?;
            (meta.get("snapshotId")?.as_str()? == snapshot_id).then(|| {
                (
                    event.kind == "snapshot.restored",
                    meta.get("error")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                )
            })
        }))
}

async fn restore(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let instance_id = matches
        .get_one::<String>("instance")
        .context("instance id")?
        .clone();
    let mut client = connect_manager_client().await?;
    // The event cursor before the restore, so `--wait` reads this restore's
    // outcome and never an earlier one of the same snapshot.
    let after_id = if matches.get_flag("wait") {
        output::resolve(
            format,
            "failed to read the event cursor",
            client
                .list_events(tonic::Request::new(ListEventsRequest {
                    limit: Some(1),
                    ..Default::default()
                }))
                .await,
        )?
        .into_inner()
        .latest_id
    } else {
        0
    };
    let reply = output::resolve(
        format,
        "failed to restore instance",
        client
            .restore_instance(tonic::Request::new(RestoreInstanceRequest {
                instance_id: instance_id.clone(),
                snapshot: matches
                    .get_one::<String>("snapshot")
                    .context("snapshot")?
                    .clone(),
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
            }))
            .await,
    )?
    .into_inner();

    let mut outcome: Option<(bool, String)> = None;
    if matches.get_flag("wait") {
        let timeout = wait_timeout(matches);
        let start = Instant::now();
        loop {
            let instance = output::resolve(
                format,
                "failed to get instance while waiting",
                client
                    .get_instance(tonic::Request::new(GetInstanceRequest {
                        instance_id: instance_id.clone(),
                    }))
                    .await,
            )?
            .into_inner()
            .instance
            .context("manager returned no instance")?;
            if instance.restore_snapshot_id.is_none() {
                outcome = restore_outcome(
                    &mut client,
                    &instance.job_id,
                    &reply.snapshot_id,
                    after_id,
                    format,
                )
                .await?;
                if outcome.is_some() {
                    break;
                }
            }
            if start.elapsed() >= timeout {
                return Err(output::fail_local(
                    format,
                    CliError {
                        code: "DeadlineExceeded".into(),
                        message: format!(
                            "instance {instance_id} has not reported the restore after {}s",
                            timeout.as_secs()
                        ),
                        hint: Some(
                            "the instance may be waiting for its node; check `legion instance get`"
                                .into(),
                        ),
                        retryable: true,
                    },
                ));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if let Some((false, error)) = outcome.as_ref() {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "RestoreFailed",
                    format!(
                        "instance {instance_id} failed to restore from snapshot {}: {error}",
                        reply.snapshot_id
                    ),
                    Some("the instance boots cold on its retained storage; fix the cause and restore again"),
                ),
            ));
        }
    }

    if format.is_json() {
        output::print_success(&json!({
            "instance": instance_id,
            "snapshot": reply.snapshot_id,
            "message": reply.message,
            "restored": outcome.as_ref().map(|(ok, _)| *ok),
            "deduplicated": reply.deduplicated,
        }));
    } else {
        println!("{}", reply.message);
        if outcome.is_some() {
            println!("Restored.");
        }
    }
    Ok(())
}

async fn warm(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to warm snapshot",
        client
            .warm_snapshot(tonic::Request::new(WarmSnapshotRequest {
                snapshot: matches
                    .get_one::<String>("snapshot")
                    .context("snapshot")?
                    .clone(),
                job_id: crate::cmd::helpers::job_arg(matches, "job")?,
                constraints: matches
                    .get_many::<String>("constraint")
                    .map(|values| values.cloned().collect())
                    .unwrap_or_default(),
                idempotency_key: matches.get_one::<String>("idempotency-key").cloned(),
            }))
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply);
        return Ok(());
    }
    println!("{}", reply.message);
    if !reply.nodes.is_empty() {
        println!("Fetching on: {}", reply.nodes.join(", "));
    }
    if !reply.already_warm.is_empty() {
        println!("Already warm: {}", reply.already_warm.join(", "));
    }
    for skipped in &reply.incompatible {
        println!("Skipped (incompatible): {skipped}");
    }
    if !reply.nodes.is_empty() {
        println!(
            "Each node fetches on its next heartbeat; `snapshot.warmed` and \
             `snapshot.warm_failed` events report the outcome."
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warm_takes_repeatable_constraints_and_publish_parses() {
        let matches = config()
            .try_get_matches_from([
                "snapshot",
                "warm",
                "py-ml-base",
                "--job",
                "agents",
                "--constraint",
                "region=us-west",
                "--constraint",
                "pool=gpu",
                "-j",
            ])
            .unwrap();
        let (name, sub) = matches.subcommand().unwrap();
        assert_eq!(name, "warm");
        let constraints: Vec<&String> = sub.get_many::<String>("constraint").unwrap().collect();
        assert_eq!(constraints, ["region=us-west", "pool=gpu"]);
        let matches = config()
            .try_get_matches_from(["snapshot", "create", "user-42", "--memory", "--publish"])
            .unwrap();
        assert!(matches.subcommand().unwrap().1.get_flag("publish"));
    }

    #[test]
    fn create_parses_kind_name_and_wait() {
        let matches = config()
            .try_get_matches_from([
                "snapshot", "create", "user-42", "--memory", "--name", "before", "--wait", "-j",
            ])
            .unwrap();
        let (name, sub) = matches.subcommand().unwrap();
        assert_eq!(name, "create");
        assert!(sub.get_flag("memory"));
        assert!(sub.get_flag("wait"));
        assert!(sub.get_flag("json"));
        assert_eq!(sub.get_one::<String>("name").unwrap(), "before");
    }

    #[test]
    fn memory_and_disk_are_exclusive() {
        assert!(config()
            .try_get_matches_from(["snapshot", "create", "user-42", "--memory", "--disk"])
            .is_err());
    }

    #[test]
    fn sizes_render_human_units() {
        assert_eq!(human_bytes(0), "-");
        assert_eq!(human_bytes(4096), "4 KiB");
        assert_eq!(human_bytes(3 * 1024 * 1024), "3.0 MiB");
        assert_eq!(human_bytes(2 * 1024 * 1024 * 1024), "2.00 GiB");
    }
}
