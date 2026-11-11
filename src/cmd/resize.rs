//! `legion alloc resize`, `legion instance resize`, and `legion job resize`
//! (docs/resize.md): change a running unit's, or every unit of a task's,
//! `memory`, `memoryMax`, or `cpu` in place, as an override over the spec.

use clap::{Arg, ArgAction, ArgMatches, Command};

use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::resize::{json_amount, parse_memory_mib, ResourceSize};
use crate::structs::{
    ResizeAllocationRequest, ResizeInstanceRequest, ResizeReply, ResizeResources, ResizeTaskReply,
    ResizeTaskRequest,
};

/// The flags every resize form takes.
fn resize_args(command: Command) -> Command {
    command
        .arg(
            Arg::new("memory")
                .long("memory")
                .value_name("SIZE")
                .help("The reservation: what the scheduler packs on (512, 512MiB, 2GiB; a bare number is MiB)"),
        )
        .arg(
            Arg::new("memory-max")
                .long("memory-max")
                .value_name("SIZE")
                .help("The ceiling the unit may use (never below --memory)"),
        )
        .arg(
            Arg::new("cpu")
                .long("cpu")
                .value_name("MHZ")
                .value_parser(clap::value_parser!(f64))
                .help("CPU in MHz"),
        )
        .arg(
            Arg::new("reason")
                .long("reason")
                .value_name("TEXT")
                .help("Why, recorded on the override and the resize event"),
        )
        .arg(
            Arg::new("force")
                .long("force")
                .action(ArgAction::SetTrue)
                .help("Allow a ceiling below what the unit uses now (the kernel may reclaim or kill it)"),
        )
        .arg(
            Arg::new("dry-run")
                .long("dry-run")
                .action(ArgAction::SetTrue)
                .help("Answer whether the resize would fit and whether the runtime can make it, without applying it"),
        )
        .arg(
            Arg::new("idempotency-key")
                .long("idempotency-key")
                .value_name("KEY")
                .help("Retry key: a repeat with the same key returns the first reply and changes nothing"),
        )
        .arg(json_flag())
}

/// `legion alloc`.
pub fn alloc_config() -> Command {
    Command::new("alloc")
        .about("Operate on one allocation")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(resize_args(
            Command::new("resize")
                .about(
                    "\
                    Resize a running allocation in place\n\
                    \n\
                    Changes the allocation's memory (reservation), memoryMax\n\
                    (ceiling), or cpu as an override over its task's spec: no\n\
                    new job version and no rollout. A grow takes only free room\n\
                    on the allocation's node and is refused, with the room the\n\
                    node has, when it does not fit. A runtime that cannot make\n\
                    the change while the unit runs answers needs_restart; nothing\n\
                    is restarted. Resizing back to the spec's amounts removes\n\
                    the override. Needs scale:write on the job.\n\
                    \n\
                    Examples:\n\
                      legion alloc resize 88213 --cpu 2000 --reason \"batch week\"\n\
                      legion alloc resize 88213 --memory 2GiB --memory-max 4GiB -j\n\
                      legion alloc resize 88213 --memory 64GiB --dry-run\
                    ",
                )
                .arg(
                    Arg::new("allocation")
                        .value_name("ALLOCATION")
                        .value_parser(clap::value_parser!(i64))
                        .help("Allocation id (legion job status lists them)")
                        .required(true),
                ),
        ))
}

/// `legion instance resize`.
pub fn instance_resize_command() -> Command {
    resize_args(
        Command::new("resize")
            .about(
                "\
                Resize a running instance or session in place\n\
                \n\
                Changes memory (reservation), memoryMax (ceiling), or cpu\n\
                without restarting. A keyed instance's resize is one\n\
                compare-and-set on its allocation and takes only free room on\n\
                its node. A session's resize is its node's alone: a grow is\n\
                admitted against the session task's envelope and nothing is\n\
                written to the cluster store. A sandbox's ceiling moves within\n\
                the size its guest booted with; past that the answer is\n\
                needs_restart. Needs scale:write on the job.\n\
                \n\
                Examples:\n\
                  legion instance resize user-42 --memory 2GiB --memory-max 4GiB -j\n\
                  legion instance resize workspace-7k2qd9x4m1b8c3rj5t0v6wye2h --cpu 2000\
                ",
            )
            .arg(
                Arg::new("id")
                    .value_name("INSTANCE")
                    .help("Keyed instance id or session id")
                    .required(true),
            )
            .arg(
                Arg::new("local")
                    .long("local")
                    .action(ArgAction::SetTrue)
                    .help("From inside a task: resize one of this workload's own child sessions through its node's API socket, with no token (docs/child-sessions.md)"),
            )
            .arg(
                Arg::new("move")
                    .long("move")
                    .action(ArgAction::SetTrue)
                    .conflicts_with("local")
                    .help("Resize by moving: suspend the unit and resume it at the new size on a node with room. For a keyed instance whose task has a suspend block, or a session with suspend.durability = \"store\"; also needs job:write"),
            ),
    )
}

/// `legion job resize`.
pub fn job_resize_command() -> Command {
    resize_args(
        Command::new("resize")
            .about(
                "\
                Resize every allocation of a task, without a deploy\n\
                \n\
                Sets a size override on the task (memory, memoryMax, cpu),\n\
                laid over its spec the way `legion job scale` lays a count\n\
                over it: no new job version and no rollout. Each running\n\
                allocation is resized in place where its node and runtime\n\
                allow; one that cannot change in place keeps its old size\n\
                until it is next replaced, or is stopped and replaced now\n\
                with --replace. New allocations start at the overridden\n\
                size. A submit that changes the task's resources drops the\n\
                override. --reset removes it. Needs scale:write on the job.\n\
                \n\
                Examples:\n\
                  legion job resize web --memory 1GiB\n\
                  legion job resize web --task api --cpu 2000 --replace -j\n\
                  legion job resize web --reset\
                ",
            )
            .arg(
                Arg::new("job")
                    .value_name("JOB")
                    .help("Job name")
                    .required(true),
            )
            .arg(
                Arg::new("task")
                    .long("task")
                    .value_name("TASK")
                    .help("The task to resize (optional when the job has one task)"),
            )
            .arg(
                Arg::new("group")
                    .long("group")
                    .value_name("GROUP")
                    .help("The task's group, when the task name is not unique in the job"),
            )
            .arg(
                Arg::new("reset")
                    .long("reset")
                    .action(ArgAction::SetTrue)
                    .conflicts_with_all(["memory", "memory-max", "cpu"])
                    .help("Remove the override: back to the spec's size"),
            )
            .arg(
                Arg::new("replace")
                    .long("replace")
                    .action(ArgAction::SetTrue)
                    .help("Stop each allocation that cannot be resized in place, so its replacement is placed at the new size"),
            ),
    )
}

pub async fn resize_job(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let job_id = arg_matches
        .get_one::<String>("job")
        .context("Must provide a job name")?
        .clone();
    let reset = arg_matches.get_flag("reset");
    let request = tonic::Request::new(ResizeTaskRequest {
        job_id: job_id.clone(),
        task_group: arg_matches
            .get_one::<String>("group")
            .cloned()
            .unwrap_or_default(),
        task_name: arg_matches
            .get_one::<String>("task")
            .cloned()
            .unwrap_or_default(),
        to: if reset {
            None
        } else {
            Some(requested(format, arg_matches)?)
        },
        reset,
        replace: arg_matches.get_flag("replace"),
        reason: arg_matches
            .get_one::<String>("reason")
            .cloned()
            .unwrap_or_default(),
        force: arg_matches.get_flag("force"),
        dry_run: arg_matches.get_flag("dry-run"),
        idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
    });
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to resize task",
        client.resize_task(request).await,
    )?
    .into_inner();
    report_task(format, &reply);
    Ok(())
}

/// One allocation's line in a task resize.
fn allocation_entry(reply: &ResizeReply) -> Value {
    let mut entry = json!({
        "allocation": reply.allocation_id,
        "node": reply.node_id,
        "outcome": reply.outcome,
        "from": parsed(&reply.from),
        "to": parsed(&reply.to),
    });
    if !reply.instance_id.is_empty() {
        entry["instance"] = json!(reply.instance_id);
    }
    if !reply.message.is_empty() {
        entry["message"] = json!(reply.message);
    }
    if !reply.free.is_empty() {
        entry["free"] = parsed(&reply.free);
    }
    entry
}

fn report_task(format: OutputFormat, reply: &ResizeTaskReply) {
    let (from, to, spec) = (parsed(&reply.from), parsed(&reply.to), parsed(&reply.spec));
    if format.is_json() {
        let mut data = json!({
            "job": reply.job_id,
            "group": reply.task_group,
            "task": reply.task_name,
            "from": from,
            "to": to,
            "spec": spec,
            "overrideActive": reply.override_active,
            "applied": reply.applied,
            "pending": reply.pending,
            "replaced": reply.replaced,
            "allocations": reply.allocations.iter().map(allocation_entry).collect::<Vec<_>>(),
            "deduplicated": reply.deduplicated,
        });
        if reply.dry_run {
            data["dryRun"] = json!(true);
        }
        if !reply.hint.is_empty() {
            data["hint"] = json!(reply.hint);
        }
        output::print_success(&data);
        return;
    }
    let label = format!("{}/{}", reply.job_id, reply.task_name);
    let change = describe_change(&from, &to);
    println!(
        "{label}: {}{}{}",
        if reply.dry_run { "would resize: " } else { "" },
        if change.is_empty() {
            "size unchanged".to_string()
        } else {
            change
        },
        if reply.override_active {
            format!(" (override active; {})", describe_spec(&spec, &to))
        } else {
            " (at the spec's size; no override)".to_string()
        }
    );
    println!(
        "  allocations: {} resized in place, {} kept their size, {} replaced{}",
        reply.applied,
        reply.pending,
        reply.replaced,
        if reply.deduplicated {
            " [deduplicated]"
        } else {
            ""
        }
    );
    for allocation in &reply.allocations {
        if allocation.outcome == "applied" {
            continue;
        }
        println!(
            "  alloc {} on {}: {}: {}",
            allocation.allocation_id, allocation.node_id, allocation.outcome, allocation.message
        );
    }
    if !reply.hint.is_empty() {
        println!("  hint: {}", reply.hint);
    }
}

pub async fn handle_alloc(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("resize", arg_matches)) => resize_allocation(arg_matches).await,
        _ => unreachable!(),
    }
}

/// The amounts the flags name.
fn requested(format: OutputFormat, arg_matches: &ArgMatches) -> R<ResizeResources> {
    let memory = |flag: &str| -> R<Option<f64>> {
        arg_matches
            .get_one::<String>(flag)
            .map(|raw| parse_memory_mib(raw))
            .transpose()
            .map_err(|err| {
                output::fail_local(
                    format,
                    CliError::local("InvalidArgument", format!("--{flag}: {err}"), None),
                )
            })
    };
    let to = ResizeResources {
        memory: memory("memory")?,
        memory_max: memory("memory-max")?,
        cpu: arg_matches.get_one::<f64>("cpu").copied(),
    };
    if to.memory.is_none() && to.memory_max.is_none() && to.cpu.is_none() {
        return Err(output::fail_local(
            format,
            CliError::local(
                "InvalidArgument",
                "nothing to resize",
                Some("pass at least one of --memory, --memory-max, and --cpu"),
            ),
        ));
    }
    Ok(to)
}

async fn resize_allocation(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let allocation_id = *arg_matches
        .get_one::<i64>("allocation")
        .context("Must provide an allocation id")?;
    let request = tonic::Request::new(ResizeAllocationRequest {
        allocation_id,
        to: Some(requested(format, arg_matches)?),
        reason: arg_matches
            .get_one::<String>("reason")
            .cloned()
            .unwrap_or_default(),
        force: arg_matches.get_flag("force"),
        dry_run: arg_matches.get_flag("dry-run"),
        idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
    });
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to resize allocation",
        client.resize_allocation(request).await,
    )?
    .into_inner();
    report(format, "alloc", &allocation_id.to_string(), &reply)
}

pub async fn resize_instance(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let instance_id = arg_matches
        .get_one::<String>("id")
        .context("Must provide an instance id")?
        .clone();
    if arg_matches.get_flag("local") {
        // The workload channel: the socket's attribution names the owner,
        // and only its own children are reachable.
        let mut client = crate::cmd::session::workload_client(format, "--local")?;
        let done = output::resolve(
            format,
            "failed to resize child session",
            client
                .resize_child_session(crate::structs::ResizeChildSessionRequest {
                    session_id: instance_id.clone(),
                    to: Some(requested(format, arg_matches)?),
                    force: arg_matches.get_flag("force"),
                    dry_run: arg_matches.get_flag("dry-run"),
                })
                .await,
        )?
        .into_inner();
        let reply = ResizeReply {
            retryable: done.outcome == "deferred"
                || (done.outcome == "refused" && !done.free.is_empty()),
            outcome: done.outcome,
            from: done.from.clone(),
            to: done.to,
            // The node knows the session's size, not its task's spec.
            spec: done.from,
            node_id: done.node_id,
            message: done.message,
            free: done.free,
            instance_id: instance_id.clone(),
            job_id: done.job_id,
            dry_run: arg_matches.get_flag("dry-run"),
            ..Default::default()
        };
        return report(format, "instance", &instance_id, &reply);
    }
    let request = tonic::Request::new(ResizeInstanceRequest {
        instance_id: instance_id.clone(),
        to: Some(requested(format, arg_matches)?),
        reason: arg_matches
            .get_one::<String>("reason")
            .cloned()
            .unwrap_or_default(),
        force: arg_matches.get_flag("force"),
        dry_run: arg_matches.get_flag("dry-run"),
        idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
        move_unit: arg_matches.get_flag("move"),
    });
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to resize instance",
        client.resize_instance(request).await,
    )?
    .into_inner();
    report(format, "instance", &instance_id, &reply)
}

fn parsed(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| json!({}))
}

/// The error code a reply that changed nothing is reported under: the
/// outcome, except a grow that found no room, which is the same
/// `resource_exhausted` every other full-node answer uses.
pub fn failure_code(reply: &ResizeReply) -> &str {
    match reply.outcome.as_str() {
        "refused" if reply.retryable => "resource_exhausted",
        other => other,
    }
}

/// `cpu 1000 -> 2000, memory 1024 -> 2048`: only what changed.
pub fn describe_change(from: &Value, to: &Value) -> String {
    let (from, to) = (ResourceSize::of(from), ResourceSize::of(to));
    let mut parts = Vec::new();
    for (name, before, after) in [
        ("cpu", from.cpu, to.cpu),
        ("memory", from.memory, to.memory),
        ("memoryMax", from.memory_max, to.memory_max),
    ] {
        if before != after {
            let show = |amount: Option<f64>| {
                amount
                    .map(|amount| json_amount(amount).to_string())
                    .unwrap_or_else(|| "-".to_string())
            };
            parts.push(format!("{name} {} -> {}", show(before), show(after)));
        }
    }
    if parts.is_empty() {
        "no change".to_string()
    } else {
        parts.join(", ")
    }
}

/// `override active; spec 1000` or `at spec`.
fn describe_spec(spec: &Value, to: &Value) -> String {
    let (spec, to) = (ResourceSize::of(spec), ResourceSize::of(to));
    let mut differs = Vec::new();
    for (name, spec, to) in [
        ("cpu", spec.cpu, to.cpu),
        ("memory", spec.memory, to.memory),
        ("memoryMax", spec.memory_max, to.memory_max),
    ] {
        if spec != to {
            differs.push(match spec {
                Some(amount) => format!("{name} {}", json_amount(amount)),
                None => format!("{name} unset"),
            });
        }
    }
    if differs.is_empty() {
        "at spec".to_string()
    } else {
        format!("override active; spec {}", differs.join(", "))
    }
}

/// The lines `legion instance get` prints for a unit's size: its effective
/// resources, and the override when a resize left one.
pub fn resource_lines(resources: &str, resources_override: &str) -> Vec<String> {
    let mut lines = Vec::new();
    let effective = ResourceSize::of(&parsed(resources));
    if effective.is_empty() {
        return lines;
    }
    let show = |name: &str, amount: Option<f64>| {
        amount.map(|amount| format!("{name} {}", json_amount(amount)))
    };
    let amounts: Vec<String> = [
        show("cpu", effective.cpu),
        show("memory", effective.memory),
        show("memoryMax", effective.memory_max),
    ]
    .into_iter()
    .flatten()
    .collect();
    lines.push(format!("\nresources: {} (effective)", amounts.join(", ")));
    let record = parsed(resources_override);
    if let Ok(record) = serde_json::from_value::<crate::structs::resize::ResourcesOverride>(record)
    {
        if !record.size.is_empty() {
            let spec: Vec<String> = [
                record.size.cpu.and(show("cpu", record.spec.cpu)),
                record.size.memory.and(show("memory", record.spec.memory)),
                record
                    .size
                    .memory_max
                    .and(show("memoryMax", record.spec.memory_max)),
            ]
            .into_iter()
            .flatten()
            .collect();
            let mut line = format!("resized: override active; spec {}", spec.join(", "));
            if !record.reason.is_empty() {
                line.push_str(&format!("; reason: {}", record.reason));
            }
            if !record.by.is_empty() {
                line.push_str(&format!("; by {}", record.by));
            }
            if !record.at.is_empty() {
                line.push_str(&format!(" at {}", record.at));
            }
            lines.push(line);
        }
    }
    lines
}

fn report(format: OutputFormat, kind: &str, id: &str, reply: &ResizeReply) -> R<()> {
    let (from, to, spec) = (parsed(&reply.from), parsed(&reply.to), parsed(&reply.spec));
    let applied = reply.outcome == "applied";
    // A dry run is a question, and its answer is data whatever it is.
    if !applied && !reply.dry_run {
        return Err(output::fail_local(
            format,
            CliError {
                code: failure_code(reply).to_string(),
                message: reply.message.clone(),
                hint: (!reply.hint.is_empty()).then(|| reply.hint.clone()),
                retryable: reply.retryable,
                details: Some(json!({
                    "outcome": reply.outcome,
                    kind: id,
                    "node": reply.node_id,
                    "from": from,
                    "to": to,
                    "free": (!reply.free.is_empty()).then(|| parsed(&reply.free)),
                })),
            },
        ));
    }
    if format.is_json() {
        let key = if kind == "alloc" {
            "allocation"
        } else {
            "instance"
        };
        let mut data = json!({
            key: if kind == "alloc" { json!(reply.allocation_id) } else { json!(id) },
            "node": reply.node_id,
            "from": from,
            "to": to,
            "spec": spec,
            "outcome": reply.outcome,
            "deduplicated": reply.deduplicated,
        });
        if reply.dry_run {
            data["dryRun"] = json!(true);
            if !applied {
                data["message"] = json!(reply.message);
                data["hint"] = json!(reply.hint);
                data["retryable"] = json!(reply.retryable);
                if !reply.free.is_empty() {
                    data["free"] = parsed(&reply.free);
                }
            }
        }
        if kind == "instance" && reply.allocation_id != 0 {
            data["allocation"] = json!(reply.allocation_id);
        }
        if !reply.moved_to.is_empty() {
            data["movedTo"] = json!(reply.moved_to);
        }
        if reply.dry_run && applied && !reply.message.is_empty() {
            data["message"] = json!(reply.message);
        }
        output::print_success(&data);
        return Ok(());
    }
    if reply.dry_run {
        if applied {
            println!(
                "{kind} {id}: would resize: {} (dry run; nothing changed)",
                describe_change(&from, &to)
            );
        } else {
            println!(
                "{kind} {id}: would be {}: {}{}",
                reply.outcome,
                reply.message,
                if reply.hint.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", reply.hint)
                }
            );
        }
        return Ok(());
    }
    println!(
        "{kind} {id}: {} ({}){}{}",
        describe_change(&from, &to),
        describe_spec(&spec, &to),
        if reply.moved_to.is_empty() {
            String::new()
        } else {
            format!(" [moved to {}]", reply.moved_to)
        },
        if reply.deduplicated {
            " [deduplicated]"
        } else {
            ""
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_resize_parses_sizes_reset_and_replace_test() -> R<()> {
        let matches = job_resize_command().try_get_matches_from([
            "resize",
            "web",
            "--task",
            "api",
            "--memory",
            "1GiB",
            "--replace",
            "-j",
        ])?;
        assert_eq!(
            matches.get_one::<String>("job").map(String::as_str),
            Some("web")
        );
        assert_eq!(
            matches.get_one::<String>("task").map(String::as_str),
            Some("api")
        );
        assert!(matches.get_flag("replace"));
        let reset = job_resize_command().try_get_matches_from(["resize", "web", "--reset"])?;
        assert!(reset.get_flag("reset"));
        // A reset names no size.
        assert!(job_resize_command()
            .try_get_matches_from(["resize", "web", "--reset", "--memory", "1GiB"])
            .is_err());
        Ok(())
    }

    #[test]
    fn instance_resize_takes_move_but_not_with_local_test() -> R<()> {
        let matches = instance_resize_command()
            .try_get_matches_from(["resize", "user-42", "--memory", "4GiB", "--move"])?;
        assert!(matches.get_flag("move"));
        assert!(instance_resize_command()
            .try_get_matches_from(["resize", "user-42", "--memory", "4GiB", "--move", "--local"])
            .is_err());
        Ok(())
    }

    #[test]
    fn a_task_resize_lists_each_allocation_that_kept_its_size_test() {
        let entry = allocation_entry(&ResizeReply {
            allocation_id: 7,
            node_id: "w-1".into(),
            outcome: "refused".into(),
            from: r#"{"memory":256}"#.into(),
            to: r#"{"memory":4096}"#.into(),
            message: "node w-1 has 1GiB free".into(),
            free: r#"{"memory":1024}"#.into(),
            ..Default::default()
        });
        assert_eq!(entry["allocation"], json!(7));
        assert_eq!(entry["outcome"], json!("refused"));
        assert_eq!(entry["free"]["memory"], json!(1024));
    }

    #[test]
    fn alloc_resize_parses_sizes_and_flags_test() -> R<()> {
        let matches = alloc_config().try_get_matches_from([
            "alloc",
            "resize",
            "88213",
            "--cpu",
            "2000",
            "--memory",
            "2GiB",
            "--reason",
            "batch week",
            "--dry-run",
            "-j",
        ])?;
        let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
        assert_eq!(subcommand, "resize");
        assert_eq!(args.get_one::<i64>("allocation"), Some(&88213));
        let to = requested(OutputFormat::from_matches(args), args)?;
        assert_eq!(to.cpu, Some(2000.0));
        assert_eq!(to.memory, Some(2048.0));
        assert_eq!(to.memory_max, None);
        assert!(args.get_flag("dry-run"));
        assert!(!args.get_flag("force"));
        Ok(())
    }

    #[test]
    fn instance_resize_parses_test() -> R<()> {
        let matches = instance_resize_command().try_get_matches_from([
            "resize",
            "user-42",
            "--memory-max",
            "4GiB",
            "--idempotency-key",
            "k1",
        ])?;
        assert_eq!(
            matches.get_one::<String>("id").map(String::as_str),
            Some("user-42")
        );
        let to = requested(OutputFormat::from_matches(&matches), &matches)?;
        assert_eq!(to.memory_max, Some(4096.0));
        Ok(())
    }

    #[test]
    fn a_change_is_described_by_what_moved_and_how_it_differs_from_the_spec_test() {
        let spec = json!({ "cpu": 1000, "memory": 1024 });
        let to = json!({ "cpu": 2000, "memory": 1024 });
        assert_eq!(describe_change(&spec, &to), "cpu 1000 -> 2000");
        assert_eq!(describe_spec(&spec, &to), "override active; spec cpu 1000");
        assert_eq!(describe_spec(&spec, &spec), "at spec");
        assert_eq!(describe_change(&spec, &spec), "no change");
    }

    #[test]
    fn instance_get_shows_effective_resources_and_the_override_test() {
        assert!(resource_lines("", "").is_empty());
        let lines = resource_lines(r#"{"cpu":2000,"memory":1024}"#, "");
        assert_eq!(lines, ["\nresources: cpu 2000, memory 1024 (effective)"]);
        let lines = resource_lines(
            r#"{"cpu":2000,"memory":1024}"#,
            r#"{"cpu":2000.0,"spec":{"cpu":1000.0,"memory":1024.0},"reason":"batch week","by":"accessor:7f3c","at":"2026-11-10T12:00:00Z"}"#,
        );
        assert_eq!(
            lines[1],
            "resized: override active; spec cpu 1000; reason: batch week; by accessor:7f3c at \
             2026-11-10T12:00:00Z"
        );
    }

    #[test]
    fn a_grow_with_no_room_is_reported_as_resource_exhausted_test() {
        let mut reply = ResizeReply {
            outcome: "refused".into(),
            retryable: true,
            ..Default::default()
        };
        assert_eq!(failure_code(&reply), "resource_exhausted");
        reply.retryable = false;
        assert_eq!(failure_code(&reply), "refused");
        reply.outcome = "needs_restart".into();
        assert_eq!(failure_code(&reply), "needs_restart");
    }
}
