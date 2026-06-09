//! `legion instance` — manage keyed-mode stateful instances.
//!
//! Keyed instances (see `docs/stateful-workloads.md`) are the sandbox/dispatch
//! primitive: long-lived logical entities created on demand for a task that
//! declares `stateful.mode = "keyed"`. Each instance has a unique,
//! never-recycled id; `count.num` does not drive keyed tasks — the set of
//! live instances created here is the desired allocation count.
//!
//! `list`/`get` also show slot-mode instances for inspection; `create` and
//! `destroy` only apply to keyed mode (slot lifecycles are owned by the
//! scheduler via `count.num`).

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    CreateInstanceRequest, DestroyInstanceRequest, GetInstanceRequest, InstanceInfo,
    ListInstancesRequest,
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
            Create a keyed-mode instance for a job's keyed task\n\
            \n\
            The target task must declare stateful.mode = \"keyed\". --group/--task\n\
            may be omitted when the job has exactly one keyed task. --id supplies\n\
            an explicit key (required when the task sets idSource = \"caller\";\n\
            otherwise the server mints one). Ids are never recycled: a destroyed\n\
            id can never be created again.\n\
            \n\
            Example: legion instance create sandbox-demo --id user-42\
            ",
        )
        .arg(
            Arg::new("job")
                .value_name("JOB")
                .help("Job whose keyed task gets the new instance")
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
                .help("Explicit instance key ([A-Za-z0-9._-]{1,128}); server-minted when omitted"),
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
        .about("Manage stateful instances (keyed-mode create/destroy, introspection)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(create_cmd)
        .subcommand(destroy_cmd)
        .subcommand(list_cmd)
        .subcommand(get_cmd)
}

#[instrument(err, skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("create", arg_matches)) => create_instance(arg_matches).await,
        Some(("destroy", arg_matches)) => destroy_instance(arg_matches).await,
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
    let request = tonic::Request::new(CreateInstanceRequest {
        job_id: job,
        group: arg_matches.get_one::<String>("group").cloned(),
        task: arg_matches.get_one::<String>("task").cloned(),
        instance_id: arg_matches.get_one::<String>("id").cloned(),
        idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
    });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to create instance",
        client.create_instance(request).await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&json!({
            "instanceId": reply.instance_id,
            "deduplicated": reply.deduplicated,
        }));
    } else {
        println!("Created instance {}", reply.instance_id);
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
        };
        let row = InstanceRow::from(&info);
        assert_eq!(row.slot, "-");
        assert_eq!(row.current_alloc, "-");
        assert_eq!(row.storage, "-");
    }
}
