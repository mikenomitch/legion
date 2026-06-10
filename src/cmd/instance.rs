//! `legion instance` — manage keyed-mode stateful instances.
//!
//! Keyed instances (see `docs/stateful-workloads.md`) are the sandbox/dispatch
//! primitive: long-lived logical entities created on demand for a task that
//! declares `stateful.mode = "keyed"`. Each instance has a unique,
//! never-recycled id; `count.num` does not drive keyed tasks — the set of
//! live instances created here is the desired allocation count.
//!
//! `list`/`get` also show slot-mode instances for inspection; `create`,
//! `stop`, `start`, and `destroy` only apply to keyed mode (slot lifecycles
//! are owned by the scheduler via `count.num`).
//!
//! `create` speaks two RPCs: a request for exactly one instance uses the
//! single `CreateInstance`; repeated `--id` flags and/or `--count` switch to
//! the atomic batch `CreateInstances` (all-or-nothing).

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    CreateInstanceRequest, CreateInstancesRequest, DestroyInstanceRequest, GetInstanceRequest,
    InstanceInfo, ListInstancesRequest, ReplaceInstanceRequest, StartInstanceRequest,
    StopInstanceRequest,
};
use tabled::Tabled;

/// Optional idempotency key for mutating instance operations. When supplied,
/// the manager deduplicates retried requests carrying the same key, so an
/// agent can safely retry on timeout without risking a double-apply.
fn idempotency_arg() -> Arg {
    Arg::new("idempotency-key")
        .long("idempotency-key")
        .value_name("KEY")
        .help("Deduplicate retries of this mutating request (safe for agent retries)")
}

pub fn config() -> Command {
    let create_cmd = Command::new("create")
        .about(
            "\
            Create keyed-mode instance(s) for a job's keyed task\n\
            \n\
            The target task must declare stateful.mode = \"keyed\". --group/--task\n\
            may be omitted when the job has exactly one keyed task. --id supplies\n\
            an explicit key (required when the task sets idSource = \"caller\";\n\
            otherwise the server mints one) and may repeat; --count N asks the\n\
            server to mint N additional ids. Requests for more than one instance\n\
            are atomic (all-or-nothing): any collision or invalid id creates\n\
            nothing. Ids are never recycled: a destroyed id can never be created\n\
            again, and a stopped instance still owns its id.\n\
            \n\
            Examples:\n\
              legion instance create sandbox-demo --id user-42\n\
              legion instance create sandbox-demo --id a --id b --count 3\
            ",
        )
        .arg(
            Arg::new("job")
                .value_name("JOB")
                .help("Job whose keyed task gets the new instance(s)")
                .required(true),
        )
        .arg(
            Arg::new("group")
                .long("group")
                .short('g')
                .value_name("GROUP")
                .help("Task group, when the keyed task is ambiguous"),
        )
        .arg(
            Arg::new("task")
                .long("task")
                .short('t')
                .value_name("TASK")
                .help("Task name, when the keyed task is ambiguous"),
        )
        .arg(
            Arg::new("id")
                .long("id")
                .value_name("KEY")
                .action(ArgAction::Append)
                .help(
                    "Explicit instance key ([A-Za-z0-9._-]{1,128}); repeatable. \
                     Server-minted when omitted",
                ),
        )
        .arg(
            Arg::new("count")
                .long("count")
                .value_name("N")
                .value_parser(clap::value_parser!(u32))
                .help("Create N additional server-minted instances (atomic with any --id keys)"),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let stop_cmd = Command::new("stop")
        .about(
            "\
            Stop (pause) a keyed-mode instance without destroying it\n\
            \n\
            Stops its allocation but RETAINS the row and storage handle; the id\n\
            stays owned (creates still collide). Revive it later with\n\
            `legion instance start <ID>`. Stopping an already-stopped instance\n\
            is a no-op; terminated (destroyed) instances cannot be stopped.\
            ",
        )
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Instance id to stop")
                .required(true),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let start_cmd = Command::new("start")
        .about(
            "\
            Start (resume) a stopped keyed-mode instance\n\
            \n\
            Moves it back to pending and enqueues placement; the replacement\n\
            allocation re-binds the same instance id and its retained storage\n\
            handle. Starting an already-running instance is a no-op; terminated\n\
            instances can never be started (create a new id instead), and the\n\
            owning job must be running (`legion job run` first).\
            ",
        )
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Instance id to start")
                .required(true),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let destroy_cmd = Command::new("destroy")
        .about(
            "\
            Destroy a keyed-mode instance\n\
            \n\
            Stops its allocation, drops its storage handle, and tombstones the id\n\
            forever (never recycled). Destroying an already-terminated instance\n\
            succeeds as a no-op.\
            ",
        )
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Instance id to destroy")
                .required(true),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let replace_cmd = Command::new("replace")
        .about(
            "\
            Force-replace a stateful instance's allocation on a LOST node\n\
            \n\
            Operator override for the node-loss confirmation gate (K8s analog:\n\
            force-deleting a pod stuck on a lost node). Frees the gated\n\
            allocation immediately — without waiting for stateful.onLost\n\
            .replaceAfter, and despite replaceAfter \"never\" — and reschedules\n\
            the instance elsewhere with the same id and storage handle.\n\
            \n\
            DANGER: this asserts the lost node's workload is dead. If the node\n\
            is merely partitioned and its worker has not yet self-fenced, the\n\
            instance can briefly run twice (split-brain). Works for slot and\n\
            keyed instances; fails unless the allocation's node is lost.\
            ",
        )
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Instance id whose lost-node allocation to force-replace")
                .required(true),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let list_cmd = Command::new("list")
        .about("List a job's stateful instances (slot and keyed, including terminated tombstones)")
        .arg(
            Arg::new("job")
                .value_name("JOB")
                .help("Job whose instances to list")
                .required(true),
        )
        .arg(
            Arg::new("group")
                .long("group")
                .short('g')
                .value_name("GROUP")
                .help("Only show instances in this task group"),
        )
        .arg(json_flag());

    let get_cmd = Command::new("get")
        .about("Show one stateful instance by id")
        .arg(
            Arg::new("id")
                .value_name("ID")
                .help("Instance id to fetch")
                .required(true),
        )
        .arg(json_flag());

    Command::new("instance")
        .about("Manage stateful instances (keyed-mode create/stop/start/destroy, introspection)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(create_cmd)
        .subcommand(stop_cmd)
        .subcommand(start_cmd)
        .subcommand(destroy_cmd)
        .subcommand(replace_cmd)
        .subcommand(list_cmd)
        .subcommand(get_cmd)
}

#[instrument(err, skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("create", arg_matches)) => create_instance(arg_matches).await,
        Some(("stop", arg_matches)) => stop_instance(arg_matches).await,
        Some(("start", arg_matches)) => start_instance(arg_matches).await,
        Some(("destroy", arg_matches)) => destroy_instance(arg_matches).await,
        Some(("replace", arg_matches)) => replace_instance(arg_matches).await,
        Some(("list", arg_matches)) => list_instances(arg_matches).await,
        Some(("get", arg_matches)) => get_instance(arg_matches).await,
        _ => unreachable!(),
    }
}

#[instrument(err, skip_all)]
async fn create_instance(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let job = arg_matches
        .get_one::<String>("job")
        .context("Must provide job")?
        .clone();
    let group = arg_matches.get_one::<String>("group").cloned();
    let task = arg_matches.get_one::<String>("task").cloned();
    let idempotency_key = arg_matches.get_one::<String>("idempotency-key").cloned();
    let ids: Vec<String> = arg_matches
        .get_many::<String>("id")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();
    let count = arg_matches.get_one::<u32>("count").copied().unwrap_or(0);

    let mut client = connect_manager_client().await?;

    // Exactly-one-instance requests keep using the single CreateInstance RPC
    // (which also covers "no --id, no --count" = one server-minted id);
    // anything bigger goes through the atomic batch CreateInstances RPC.
    if count == 0 && ids.len() <= 1 {
        let request = tonic::Request::new(CreateInstanceRequest {
            job_id: job,
            group,
            task,
            instance_id: ids.into_iter().next(),
            idempotency_key,
        });
        let reply = output::resolve(
            format,
            "failed to create instance",
            client.create_instance(request).await,
        )?
        .into_inner();

        if format.is_json() {
            output::print_success(&json!({
                "instanceIds": [reply.instance_id],
                "deduplicated": reply.deduplicated,
            }));
        } else {
            println!("Created instance {}", reply.instance_id);
        }
        return Ok(());
    }

    let request = tonic::Request::new(CreateInstancesRequest {
        job_id: job,
        group,
        task,
        instance_ids: ids,
        count,
        idempotency_key,
    });
    let reply = output::resolve(
        format,
        "failed to create instances",
        client.create_instances(request).await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&json!({
            "instanceIds": reply.instance_ids,
            "deduplicated": reply.deduplicated,
        }));
    } else {
        println!(
            "Created {} instance(s): {}",
            reply.instance_ids.len(),
            reply.instance_ids.join(", ")
        );
    }
    Ok(())
}

#[instrument(err, skip_all)]
async fn stop_instance(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let id = arg_matches
        .get_one::<String>("id")
        .context("Must provide id")?
        .clone();
    let request = tonic::Request::new(StopInstanceRequest {
        instance_id: id,
        idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
    });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to stop instance",
        client.stop_instance(request).await,
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

#[instrument(err, skip_all)]
async fn start_instance(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let id = arg_matches
        .get_one::<String>("id")
        .context("Must provide id")?
        .clone();
    let request = tonic::Request::new(StartInstanceRequest {
        instance_id: id,
        idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
    });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to start instance",
        client.start_instance(request).await,
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

#[instrument(err, skip_all)]
async fn destroy_instance(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let id = arg_matches
        .get_one::<String>("id")
        .context("Must provide id")?
        .clone();
    let request = tonic::Request::new(DestroyInstanceRequest {
        instance_id: id,
        idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
    });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to destroy instance",
        client.destroy_instance(request).await,
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

#[instrument(err, skip_all)]
async fn replace_instance(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let id = arg_matches
        .get_one::<String>("id")
        .context("Must provide id")?
        .clone();
    let request = tonic::Request::new(ReplaceInstanceRequest {
        instance_id: id,
        idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
    });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to replace instance",
        client.replace_instance(request).await,
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

#[instrument(err, skip_all)]
async fn list_instances(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let job = arg_matches
        .get_one::<String>("job")
        .context("Must provide job")?
        .clone();
    let request = tonic::Request::new(ListInstancesRequest {
        job_id: job,
        group: arg_matches.get_one::<String>("group").cloned(),
    });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to list instances",
        client.list_instances(request).await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&reply.instances);
    } else {
        let rows: Vec<InstanceRow> = reply.instances.iter().map(InstanceRow::from).collect();
        print_table(rows);
    }
    Ok(())
}

#[instrument(err, skip_all)]
async fn get_instance(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let id = arg_matches
        .get_one::<String>("id")
        .context("Must provide id")?
        .clone();
    let request = tonic::Request::new(GetInstanceRequest { instance_id: id });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to fetch instance",
        client.get_instance(request).await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&reply.instance);
    } else {
        match reply.instance {
            Some(instance) => print_table(vec![InstanceRow::from(&instance)]),
            None => println!("Instance not found"),
        }
    }
    Ok(())
}

/// Human-readable table row for an instance.
#[derive(Tabled)]
struct InstanceRow {
    id: String,
    group: String,
    task: String,
    mode: String,
    state: String,
    slot: String,
    #[tabled(rename = "alloc")]
    current_alloc: String,
    storage: String,
}

impl From<&InstanceInfo> for InstanceRow {
    fn from(instance: &InstanceInfo) -> Self {
        InstanceRow {
            id: instance.instance_id.clone(),
            group: instance.group.clone(),
            task: instance.task.clone(),
            mode: instance.mode.clone(),
            state: instance.state.clone(),
            slot: instance
                .slot_index
                .map(|slot| slot.to_string())
                .unwrap_or_else(|| "-".into()),
            current_alloc: instance
                .current_alloc_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "-".into()),
            storage: instance
                .storage_handle
                .clone()
                .unwrap_or_else(|| "-".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_create_command_parses_all_flags() -> R<()> {
        let matches = config().try_get_matches_from([
            "instance",
            "create",
            "sandbox-demo",
            "--group",
            "sandboxes",
            "--task",
            "sandbox",
            "--id",
            "user-42",
            "--idempotency-key",
            "k1",
            "--json",
        ])?;
        let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
        assert_eq!(subcommand, "create");
        assert_eq!(
            args.get_one::<String>("job").map(String::as_str),
            Some("sandbox-demo")
        );
        assert_eq!(
            args.get_one::<String>("group").map(String::as_str),
            Some("sandboxes")
        );
        assert_eq!(
            args.get_one::<String>("task").map(String::as_str),
            Some("sandbox")
        );
        assert_eq!(
            args.get_one::<String>("id").map(String::as_str),
            Some("user-42")
        );
        assert_eq!(
            args.get_one::<String>("idempotency-key")
                .map(String::as_str),
            Some("k1")
        );
        assert!(args.get_flag("json"));
        Ok(())
    }

    #[test]
    fn instance_destroy_list_get_commands_parse() -> R<()> {
        let matches = config().try_get_matches_from(["instance", "destroy", "sandbox-abc"])?;
        let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
        assert_eq!(subcommand, "destroy");
        assert_eq!(
            args.get_one::<String>("id").map(String::as_str),
            Some("sandbox-abc")
        );

        let matches =
            config().try_get_matches_from(["instance", "list", "sandbox-demo", "-g", "default"])?;
        let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
        assert_eq!(subcommand, "list");
        assert_eq!(
            args.get_one::<String>("group").map(String::as_str),
            Some("default")
        );

        let matches = config().try_get_matches_from(["instance", "get", "sandbox-abc", "-j"])?;
        let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
        assert_eq!(subcommand, "get");
        assert!(args.get_flag("json"));
        Ok(())
    }

    #[test]
    fn instance_replace_command_parses() -> R<()> {
        let matches = config().try_get_matches_from([
            "instance",
            "replace",
            "db-default-2",
            "--idempotency-key",
            "k1",
            "-j",
        ])?;
        let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
        assert_eq!(subcommand, "replace");
        assert_eq!(
            args.get_one::<String>("id").map(String::as_str),
            Some("db-default-2")
        );
        assert_eq!(
            args.get_one::<String>("idempotency-key")
                .map(String::as_str),
            Some("k1")
        );
        assert!(args.get_flag("json"));
        Ok(())
    }

    #[test]
    fn instance_row_renders_optional_fields_as_dashes() {
        let info = InstanceInfo {
            instance_id: "sandbox-1".into(),
            job_id: "sandbox-demo".into(),
            group: "default".into(),
            task: "sandbox".into(),
            mode: "keyed".into(),
            state: "pending".into(),
            slot_index: None,
            current_alloc_id: None,
            storage_handle: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
            detail: None,
        };
        let row = InstanceRow::from(&info);
        assert_eq!(row.slot, "-");
        assert_eq!(row.current_alloc, "-");
        assert_eq!(row.storage, "-");
    }
}
