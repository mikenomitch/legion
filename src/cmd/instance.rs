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
//! On a session task (`stateful.mode = "session"`, docs/session-mode.md),
//! `create` without `--id` starts an unnamed session that has no row,
//! `list`/`get` read running sessions from the managers' session index, `stop`
//! and `destroy` end one, and `promote` turns one into a keyed instance in
//! place.
//!
//! `create` speaks two RPCs: a request for exactly one instance uses the
//! single `CreateInstance`; repeated `--id` flags and/or `--count` switch to
//! the atomic batch `CreateInstances` (all-or-nothing).

use std::time::{Duration, Instant};

use crate::cmd::helpers::{connect_manager_client, json_flag, print_table, AuthedManagerClient};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    CreateInstanceRequest, CreateInstancesRequest, DestroyInstanceRequest, ForkInstanceRequest,
    GetInstanceRequest, InstanceInfo, ListInstancesRequest, PromoteInstanceRequest,
    ReplaceInstanceRequest, StartInstanceRequest, StopInstanceRequest, SuspendInstanceRequest,
};

/// Default `--timeout` for `suspend --wait` and `start --wait`, in seconds.
/// A memory checkpoint of a multi-GiB guest takes seconds; a resume can wait
/// out `suspend.resumeTimeout` before booting cold.
const DEFAULT_WAIT_SECS: u64 = 300;

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
            an explicit key (required when the task sets requireId = true;\n\
            otherwise the server mints one) and may repeat; --count N asks the\n\
            server to mint N additional ids. Requests for more than one instance\n\
            are atomic (all-or-nothing): any collision or invalid id creates\n\
            nothing. Ids are never recycled: a destroyed --id can never be created\n\
            again, and a stopped instance still owns its id. A minted id\n\
            (<task>-<26 base32>) is unique by randomness and leaves no tombstone;\n\
            --id may not take that shape.\n\
            \n\
            --from-snapshot restores every new instance from a snapshot of the\n\
            job (an id or a name; see `legion snapshot alias`) instead of booting\n\
            it empty: each gets its own copy of the snapshot's volume and its own\n\
            identity, on the node holding the snapshot (at most 64 per call).\n\
            \n\
            On a session task (stateful.mode = \"session\"), a create without --id\n\
            starts an unnamed session straight on a node with envelope room and\n\
            writes nothing to the replicated store (mode session, durable false).\n\
            With --idempotency-key, a retry finds the same session. --id creates\n\
            an ordinary keyed instance; `legion instance promote` turns a running\n\
            session into one.\n\
            \n\
            Examples:\n\
              legion instance create sandbox-demo --id user-42\n\
              legion instance create sandbox-demo --id a --id b --count 3\n\
              legion instance create agents --from-snapshot agent-golden --count 50\
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
        .arg(
            Arg::new("from-snapshot")
                .long("from-snapshot")
                .value_name("NAME|ID")
                .help("Restore the new instance(s) from this snapshot of the job (needs snapshot:read)"),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let fork_cmd = wait_args(
        Command::new("fork")
            .about(
                "\
            Fork a running keyed instance into N new instances\n\
            \n\
            The origin is checkpointed live and keeps running: a memory\n\
            checkpoint pauses it only while its memory and disks are captured\n\
            (pause_ms), a disk checkpoint copies its volume without pausing.\n\
            Each fork is a new instance (`<origin>-f<n>`, never recycled) with\n\
            its own copy of the volume and its own identity: allocation id,\n\
            hostname, workload identity token, entropy, and for a TAP-wired\n\
            sandbox its own MAC and TAP device. Forks run on the origin's node.\n\
            Needs job:write and snapshot:write. See docs/suspend-and-resume.md.\n\
            \n\
            Examples:\n\
              legion instance fork run-17 --count 3 --wait -j\n\
              legion instance fork run-17 --checkpoint disk\
            ",
            )
            .arg(
                Arg::new("id")
                    .value_name("ID")
                    .help("Instance id to fork")
                    .required(true),
            )
            .arg(
                Arg::new("count")
                    .long("count")
                    .short('n')
                    .value_name("N")
                    .value_parser(clap::value_parser!(u32).range(1..=64))
                    .help("How many forks to create, 1 to 64 (default 1)"),
            )
            .arg(
                Arg::new("spread")
                    .long("spread")
                    .action(ArgAction::SetTrue)
                    .help("Place forks on distinct nodes: publishes the memory checkpoint to the node's contentStore first"),
            )
            .arg(
                Arg::new("checkpoint")
                    .long("checkpoint")
                    .value_name("KIND")
                    .value_parser(["memory", "disk"])
                    .help("Checkpoint kind; default memory when the origin can take one, else disk"),
            )
            .arg(idempotency_arg())
            .arg(json_flag()),
        "the origin's checkpoint lands and the forks are released for placement",
    );

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

    let start_cmd = wait_args(
        Command::new("start")
            .about(
                "\
            Start a stopped keyed-mode instance, or resume a suspended one\n\
            \n\
            A stopped instance moves back to pending and boots fresh; the\n\
            replacement allocation re-binds the same instance id and its\n\
            retained storage handle. A suspended instance restores from its\n\
            checkpoint on the node that holds it (`--cold` discards the\n\
            checkpoint and boots fresh instead). Starting an already-running\n\
            instance is a no-op; terminated instances can never be started\n\
            (create a new id instead), and the owning job must be running\n\
            (`legion job run` first).\
            ",
            )
            .arg(
                Arg::new("id")
                    .value_name("ID")
                    .help("Instance id to start")
                    .required(true),
            )
            .arg(
                Arg::new("cold")
                    .long("cold")
                    .action(ArgAction::SetTrue)
                    .help("Suspended instances: discard the checkpoint and boot fresh"),
            )
            .arg(
                Arg::new("force-move")
                    .long("force-move")
                    .action(ArgAction::SetTrue)
                    .help(
                        "Store sessions: move the session to another node even when its old \
                         node did not answer the probe (only once that node is known to be down)",
                    ),
            )
            .arg(idempotency_arg())
            .arg(json_flag()),
        "the instance is running (and a resume has restored)",
    );

    let suspend_cmd = wait_args(
        Command::new("suspend")
            .about(
                "\
            Suspend a keyed-mode instance: checkpoint it, stop it, free its reservation\n\
            \n\
            The node captures the running allocation (memory for a sandbox\n\
            launched for checkpoints, else disk) and stops it. The instance\n\
            becomes `suspended`, holds no allocation, and `legion instance\n\
            start` restores it where it left off. A failed capture leaves the\n\
            instance running. See docs/suspend-and-resume.md.\
            ",
            )
            .arg(
                Arg::new("id")
                    .value_name("ID")
                    .help("Instance id to suspend")
                    .required(true),
            )
            .arg(
                Arg::new("checkpoint")
                    .long("checkpoint")
                    .value_name("KIND")
                    .value_parser(["memory", "disk"])
                    .help("Checkpoint kind; default the task's suspend.checkpoint, else the best the instance can take"),
            )
            .arg(idempotency_arg())
            .arg(json_flag()),
        "the instance is suspended",
    );

    let destroy_cmd = Command::new("destroy")
        .about(
            "\
            Destroy a keyed-mode instance\n\
            \n\
            Stops its allocation, drops its storage handle, and tombstones a\n\
            named id for the life of the job (never recycled). A server-minted\n\
            id leaves no row at all. Destroying an already-terminated instance\n\
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

    let promote_cmd = Command::new("promote")
        .about(
            "\
            Promote a running session to a keyed instance, in place\n\
            \n\
            A session (stateful.mode = \"session\") has no row: it is lost with\n\
            its node and listed from the managers' session index. Promotion\n\
            writes one raft entry, an instance row and an allocation the node\n\
            adopts the running session as, so nothing restarts. From then on it\n\
            is an ordinary keyed instance: listed exactly, rescheduled on node\n\
            loss, suspendable and forkable. --id names it (a keyed id no\n\
            instance has ever had; refused when stateful.promote.onName =\n\
            false); without --id it keeps the session id. Promoting again\n\
            reports the same instance as deduplicated.\n\
            \n\
            Examples:\n\
              legion instance promote workspace-7k2qd9x4m1b8c3rj5t0v6wye2h\n\
              legion instance promote workspace-7k2qd9x4m1b8c3rj5t0v6wye2h --id user-42\
            ",
        )
        .arg(
            Arg::new("session")
                .value_name("SESSION")
                .help("Id of the running session to promote")
                .required(true),
        )
        .arg(
            Arg::new("id")
                .long("id")
                .value_name("KEY")
                .help("Name for the promoted instance ([A-Za-z0-9._-]{1,128}); keeps the session id when omitted"),
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
        .arg(
            Arg::new("include-idle")
                .long("include-idle")
                .action(ArgAction::SetTrue)
                .help(
                    "Also list archived instances that have no row in the store \
                     (suspend.archive = \"manifestOnly\"), read from the job's archive \
                     manifests in the contentStore, one page at a time",
                ),
        )
        .arg(
            Arg::new("session-limit")
                .long("session-limit")
                .value_name("N")
                .value_parser(clap::value_parser!(u32))
                .help(
                    "The most running sessions to list, in id order (default 5000, at most \
                     50000). 0 lists none and prints only the session counts",
                ),
        )
        .arg(
            Arg::new("session-page-token")
                .long("session-page-token")
                .value_name("TOKEN")
                .help("Resume the session listing at this token (from the previous page)"),
        )
        .arg(
            Arg::new("idle-page-token")
                .long("idle-page-token")
                .value_name("TOKEN")
                .requires("include-idle")
                .help("With --include-idle: resume the bucket listing at this token"),
        )
        .arg(
            Arg::new("idle-page-size")
                .long("idle-page-size")
                .value_name("N")
                .value_parser(clap::value_parser!(u32))
                .requires("include-idle")
                .help("With --include-idle: the most archived instances to list (default 1000)"),
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
        .subcommand(suspend_cmd)
        .subcommand(fork_cmd)
        .subcommand(destroy_cmd)
        .subcommand(promote_cmd)
        .subcommand(replace_cmd)
        .subcommand(list_cmd)
        .subcommand(get_cmd)
}

#[instrument(skip_all)]
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("create", arg_matches)) => create_instance(arg_matches).await,
        Some(("stop", arg_matches)) => stop_instance(arg_matches).await,
        Some(("start", arg_matches)) => start_instance(arg_matches).await,
        Some(("suspend", arg_matches)) => suspend_instance(arg_matches).await,
        Some(("fork", arg_matches)) => fork_instance(arg_matches).await,
        Some(("destroy", arg_matches)) => destroy_instance(arg_matches).await,
        Some(("promote", arg_matches)) => promote_instance(arg_matches).await,
        Some(("replace", arg_matches)) => replace_instance(arg_matches).await,
        Some(("list", arg_matches)) => list_instances(arg_matches).await,
        Some(("get", arg_matches)) => get_instance(arg_matches).await,
        _ => unreachable!(),
    }
}

#[instrument(skip_all)]
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
    let from_snapshot = arg_matches.get_one::<String>("from-snapshot").cloned();

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
            from_snapshot,
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
                "restoringFrom": (!reply.restoring_from.is_empty()).then_some(&reply.restoring_from),
                "mode": reply.mode,
                "durable": reply.durable,
                "node": (!reply.node_id.is_empty()).then_some(&reply.node_id),
                "state": (!reply.state.is_empty()).then_some(&reply.state),
                "deduplicated": reply.deduplicated,
            }));
        } else if !reply.durable {
            println!(
                "{} session {} on {} (not durable; `legion instance promote {}` keeps it)",
                if reply.deduplicated {
                    "Found"
                } else {
                    "Started"
                },
                reply.instance_id,
                reply.node_id,
                reply.instance_id
            );
        } else if reply.restoring_from.is_empty() {
            println!("Created instance {}", reply.instance_id);
        } else {
            println!(
                "Created instance {}, restoring from {}",
                reply.instance_id, reply.restoring_from
            );
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
        from_snapshot,
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
            "restoringFrom": (!reply.restoring_from.is_empty()).then_some(&reply.restoring_from),
            "deduplicated": reply.deduplicated,
        }));
    } else {
        println!(
            "Created {} instance(s): {}",
            reply.instance_ids.len(),
            reply.instance_ids.join(", ")
        );
        if !reply.restoring_from.is_empty() {
            println!("Each restores from {}", reply.restoring_from);
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn fork_instance(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let id = arg_matches
        .get_one::<String>("id")
        .context("Must provide id")?
        .clone();
    let request = tonic::Request::new(ForkInstanceRequest {
        instance_id: id.clone(),
        count: arg_matches.get_one::<u32>("count").copied().unwrap_or(1),
        spread: arg_matches.get_flag("spread"),
        checkpoint: arg_matches.get_one::<String>("checkpoint").cloned(),
        idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
    });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to fork instance",
        client.fork_instance(request).await,
    )?
    .into_inner();

    let mut pause_ms = (reply.pause_ms > 0).then_some(reply.pause_ms);
    let mut state = None;
    // A spread fork's forks wait for the publish, so --wait does too.
    let spread = reply.durability == "store";
    if arg_matches.get_flag("wait") {
        let snapshot = crate::cmd::snapshot::wait_for_capture(
            &mut client,
            &reply.snapshot,
            wait_timeout(arg_matches),
            spread,
            format,
        )
        .await?;
        let Some(snapshot) = snapshot else {
            return Err(timeout_error(
                format,
                &id,
                if spread {
                    "still being captured or published for the fork"
                } else {
                    "still being captured for the fork"
                },
                arg_matches,
            ));
        };
        if snapshot.state == "failed" || (spread && snapshot.state != "published") {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "ForkFailed",
                    format!(
                        "instance {id} could not be forked and keeps running; forks {} were \
                         terminated: {}",
                        reply.instances.join(", "),
                        snapshot.error
                    ),
                    Some("see `legion events list --kind instance.fork_failed`"),
                ),
            ));
        }
        pause_ms = Some(snapshot.pause_ms);
        state = Some(snapshot.state);
    }

    if format.is_json() {
        output::print_success(&json!({
            "origin": reply.origin,
            "snapshot": reply.snapshot,
            "pauseMs": pause_ms,
            "instances": reply.instances,
            "kind": reply.kind,
            "node": reply.node_id,
            "durability": reply.durability,
            "checkpointState": state.unwrap_or_else(|| "capturing".into()),
            "deduplicated": reply.deduplicated,
            "notes": reply.notes,
        }));
    } else {
        for note in &reply.notes {
            println!("note: {note}");
        }
        println!(
            "Forking {} into {} ({} checkpoint {} on {})",
            reply.origin,
            reply.instances.join(", "),
            reply.kind,
            reply.snapshot,
            reply.node_id
        );
        if spread {
            println!(
                "Spread: the checkpoint is published to {}'s content store, then each fork runs on its own node.",
                reply.node_id
            );
        }
        match pause_ms {
            Some(ms) => println!("Checkpoint landed; {} paused {ms} ms. The forks are being placed.", reply.origin),
            None => println!(
                "The node captures {} on its next heartbeat. Add --wait to block until the forks are released.",
                reply.origin
            ),
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn promote_instance(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let session = arg_matches
        .get_one::<String>("session")
        .context("Must provide a session id")?
        .clone();
    let request = tonic::Request::new(PromoteInstanceRequest {
        instance_id: session,
        new_id: arg_matches
            .get_one::<String>("id")
            .cloned()
            .unwrap_or_default(),
        idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
    });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to promote session",
        client.promote_instance(request).await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&json!({
            "instanceId": reply.instance_id,
            "promotedFrom": reply.promoted_from,
            "node": reply.node_id,
            "allocationId": reply.allocation_id,
            "durable": reply.durable,
            "deduplicated": reply.deduplicated,
        }));
    } else {
        println!(
            "{} session {} as instance {} (allocation {} on {})",
            if reply.deduplicated {
                "Already promoted"
            } else {
                "Promoted"
            },
            reply.promoted_from,
            reply.instance_id,
            reply.allocation_id,
            reply.node_id
        );
    }
    Ok(())
}

#[instrument(skip_all)]
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

#[instrument(skip_all)]
async fn start_instance(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let id = arg_matches
        .get_one::<String>("id")
        .context("Must provide id")?
        .clone();
    let request = tonic::Request::new(StartInstanceRequest {
        instance_id: id.clone(),
        idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
        cold: arg_matches.get_flag("cold"),
        force_move: arg_matches.get_flag("force-move"),
    });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to start instance",
        client.start_instance(request).await,
    )?
    .into_inner();

    let resuming = !reply.resuming_from.is_empty();
    let mut instance = None;
    if arg_matches.get_flag("wait") {
        let settled = wait_for_instance(
            &mut client,
            &id,
            wait_timeout(arg_matches),
            format,
            |info| info.state == "active" && info.pending_op.is_none() || info.state == "suspended",
        )
        .await?;
        let Some(settled) = settled else {
            return Err(timeout_error(format, &id, "still starting", arg_matches));
        };
        if resuming && settled.state == "suspended" {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "RestoreFailed",
                    format!(
                        "instance {id} could not restore {}: {}",
                        reply.resuming_from,
                        settled.detail.clone().unwrap_or_default()
                    ),
                    Some(&format!(
                        "`legion instance start {id} --cold` boots it fresh"
                    )),
                ),
            ));
        }
        instance = Some(settled);
    }

    if format.is_json() {
        let state = instance
            .as_ref()
            .map(|info| info.state.clone())
            .unwrap_or_else(|| "pending".into());
        let restore_ms = instance
            .as_ref()
            .filter(|info| info.restored_from_snapshot_id.as_deref() == Some(&reply.resuming_from))
            .and_then(|info| info.last_restore_ms);
        output::print_success(&json!({
            "instance": id,
            "state": state,
            "resumedFrom": (resuming).then_some(reply.resuming_from.clone()),
            "node": (!reply.node_id.is_empty()).then_some(reply.node_id.clone()),
            "restoreMs": restore_ms,
            "coldBoot": instance.as_ref().and_then(|info| info.cold_boot.as_ref()).map(|cold| {
                json!({ "snapshotId": cold.snapshot_id, "reason": cold.reason, "at": cold.at })
            }),
            "detail": instance.as_ref().and_then(|info| info.detail.clone()),
            "message": reply.message,
            "deduplicated": reply.deduplicated,
        }));
    } else {
        println!("{}", reply.message);
        if let Some(info) = instance.as_ref() {
            match (resuming, info.last_restore_ms, info.cold_boot.as_ref()) {
                // Set only when this start's restore fell back to a cold
                // boot: a start clears the previous record.
                (_, _, Some(cold)) => println!("{}", cold_boot_line(cold)),
                (true, Some(ms), None) => println!("Restored in {ms} ms on {}", reply.node_id),
                _ => println!("Instance {id} is {}", info.state),
            }
            if let Some(detail) = info.detail.as_deref() {
                println!("detail: {detail}");
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn suspend_instance(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let id = arg_matches
        .get_one::<String>("id")
        .context("Must provide id")?
        .clone();
    let request = tonic::Request::new(SuspendInstanceRequest {
        instance_id: id.clone(),
        checkpoint: arg_matches.get_one::<String>("checkpoint").cloned(),
        idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
    });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to suspend instance",
        client.suspend_instance(request).await,
    )?
    .into_inner();

    let mut instance = None;
    if arg_matches.get_flag("wait") {
        let settled = wait_for_instance(
            &mut client,
            &id,
            wait_timeout(arg_matches),
            format,
            |info| info.state == "suspended" || info.pending_op.is_none(),
        )
        .await?;
        let Some(settled) = settled else {
            return Err(timeout_error(format, &id, "still suspending", arg_matches));
        };
        if settled.state != "suspended" {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "SuspendFailed",
                    format!(
                        "instance {id} was not suspended and keeps running: {}",
                        settled
                            .detail
                            .clone()
                            .unwrap_or_else(|| format!("it is {}", settled.state))
                    ),
                    Some("see `legion events list --kind instance.suspend_failed`"),
                ),
            ));
        }
        instance = Some(settled);
    }

    if format.is_json() {
        let state = instance
            .as_ref()
            .map(|info| info.state.clone())
            .unwrap_or_else(|| "suspending".into());
        output::print_success(&json!({
            "instance": id,
            "state": state,
            "checkpoint": reply.snapshot_id,
            "kind": reply.kind,
            "bytes": instance.as_ref().and_then(|info| info.checkpoint_bytes),
            "durationMs": instance.as_ref().and_then(|info| info.last_suspend_ms),
            "node": reply.node_id,
            "durability": if reply.durability.is_empty() { "local" } else { reply.durability.as_str() },
            "checkpointState": instance.as_ref().and_then(|info| info.checkpoint_state.clone()),
            "message": reply.message,
            "deduplicated": reply.deduplicated,
        }));
    } else {
        println!("{}", reply.message);
        if let Some(info) = instance.as_ref() {
            println!(
                "Suspended in {} ms ({} bytes)",
                info.last_suspend_ms.unwrap_or_default(),
                info.checkpoint_bytes.unwrap_or_default()
            );
        } else {
            println!("The node captures it on its next heartbeat. Add --wait to block until it is suspended.");
        }
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

fn timeout_error(
    format: OutputFormat,
    id: &str,
    what: &str,
    matches: &ArgMatches,
) -> anyhow::Error {
    output::fail_local(
        format,
        CliError {
            code: "DeadlineExceeded".into(),
            message: format!(
                "instance {id} is {what} after {}s",
                wait_timeout(matches).as_secs()
            ),
            hint: Some("check it later with `legion instance get`, or raise --timeout".into()),
            retryable: true,
        },
    )
}

/// Poll an instance until `done` holds. `None` on timeout.
async fn wait_for_instance(
    client: &mut AuthedManagerClient,
    instance_id: &str,
    timeout: Duration,
    format: OutputFormat,
    done: impl Fn(&InstanceInfo) -> bool,
) -> R<Option<InstanceInfo>> {
    let start = Instant::now();
    loop {
        let instance = output::resolve(
            format,
            "failed to get instance while waiting",
            client
                .get_instance(tonic::Request::new(GetInstanceRequest {
                    instance_id: instance_id.to_string(),
                }))
                .await,
        )?
        .into_inner()
        .instance
        .context("manager returned no instance")?;
        if done(&instance) {
            return Ok(Some(instance));
        }
        if start.elapsed() >= timeout {
            return Ok(None);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[instrument(skip_all)]
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

#[instrument(skip_all)]
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

#[instrument(skip_all)]
async fn list_instances(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let job = arg_matches
        .get_one::<String>("job")
        .context("Must provide job")?
        .clone();
    let include_idle = arg_matches.get_flag("include-idle");
    let request = tonic::Request::new(ListInstancesRequest {
        job_id: job,
        group: arg_matches.get_one::<String>("group").cloned(),
        include_idle,
        idle_page_token: arg_matches
            .get_one::<String>("idle-page-token")
            .cloned()
            .unwrap_or_default(),
        idle_page_size: arg_matches
            .get_one::<u32>("idle-page-size")
            .copied()
            .unwrap_or_default(),
        session_limit: arg_matches.get_one::<u32>("session-limit").copied(),
        session_page_token: arg_matches
            .get_one::<String>("session-page-token")
            .cloned()
            .unwrap_or_default(),
    });

    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to list instances",
        client.list_instances(request).await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&json!({
            "instances": reply.instances,
            "nextIdlePageToken": reply.next_idle_page_token,
            "complete": reply.complete,
            "asOf": (!reply.as_of.is_empty()).then_some(&reply.as_of),
            "nextSessionPageToken": reply.next_session_page_token,
            "sessionCount": reply.session_count,
            "sessionsByNode": reply.sessions_by_node,
        }));
    } else {
        let rows: Vec<InstanceRow> = reply.instances.iter().map(InstanceRow::from).collect();
        print_table(rows);
        let backoff_notes = backoff_notes(&reply.instances);
        if !backoff_notes.is_empty() {
            println!();
            for note in backoff_notes {
                println!("{note}");
            }
        }
        if !reply.next_idle_page_token.is_empty() {
            println!(
                "\nmore archived instances: --include-idle --idle-page-token {}",
                reply.next_idle_page_token
            );
        }
        if reply.session_count > 0 {
            println!(
                "\n{} running sessions on {} nodes",
                reply.session_count,
                reply.sessions_by_node.len()
            );
        }
        if !reply.next_session_page_token.is_empty() {
            println!(
                "more sessions: --session-page-token {}",
                reply.next_session_page_token
            );
        }
        if !reply.complete {
            println!(
                "\nincomplete: some nodes that run this job's sessions have not reported to \
                 any manager yet (as of {}); their sessions are missing above",
                reply.as_of
            );
        }
    }
    Ok(())
}

#[instrument(skip_all)]
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

    // A live, unratified claim (docs/edge-boot.md): a node has started this
    // sandbox but the placement is not durable yet. Reported alongside the
    // instance rather than folded into its state, because it is live node
    // state and not part of the durable record.
    let provisional = reply.provisional.as_ref().map(|claim| {
        json!({
            "claimId": claim.claim_id,
            "nodeId": reply.provisional_node_id,
            "gate": crate::structs::edge_boot::Gate::from_wire(claim.gate).as_str(),
            "stage": claim.stage,
            "leaseDeadlineUnix": claim.lease_deadline_unix,
        })
    });

    if format.is_json() {
        output::print_success(&json!({
            "instance": reply.instance,
            "provisional": provisional,
        }));
    } else {
        match reply.instance {
            Some(instance) => {
                print_table(vec![InstanceRow::from(&instance)]);
                if let Some(line) = session_line(&instance) {
                    println!("\n{line}");
                }
                if let Some(address) = address_line(&instance) {
                    println!("\n{address}");
                }
                for line in endpoint_lines(&instance) {
                    println!("{line}");
                }
                if let Some(snapshot) = instance.restore_snapshot_id.as_deref() {
                    println!(
                        "\nrestoring: from snapshot {snapshot}, until its node reports the outcome"
                    );
                }
                for line in suspend_lines(&instance) {
                    println!("{line}");
                }
            }
            None => println!("Instance not found"),
        }
        if let Some(provisional) = provisional.as_ref() {
            println!(
                "\nprovisional: started on {} at gate {} (stage {}), not yet ratified; \
                 the node kills it at unix {} if no verdict arrives",
                provisional["nodeId"].as_str().unwrap_or_default(),
                provisional["gate"].as_str().unwrap_or_default(),
                provisional["stage"].as_str().unwrap_or_default(),
                provisional["leaseDeadlineUnix"],
            );
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
    /// The current allocation's node (or a session's node).
    node: String,
    storage: String,
    /// Measured memory of the current allocation, MiB.
    #[tabled(rename = "mem MiB")]
    memory: String,
    /// Measured CPU of the current allocation, in cores.
    cpu: String,
    /// Time since the current allocation last did anything.
    idle: String,
    /// The suspend checkpoint, its kind, and where it lives.
    checkpoint: String,
}

impl From<&InstanceInfo> for InstanceRow {
    fn from(instance: &InstanceInfo) -> Self {
        let (memory, cpu, idle) = crate::cmd::helpers::usage_display::columns(
            instance.usage.as_ref(),
            chrono::Utc::now().timestamp(),
        );
        InstanceRow {
            checkpoint: checkpoint_column(instance),
            memory,
            cpu,
            idle,
            id: instance.instance_id.clone(),
            group: instance.group.clone(),
            task: instance.task.clone(),
            mode: instance.mode.clone(),
            state: match (
                instance.pending_op.as_deref(),
                instance.cold_boot.as_ref(),
                instance.reschedule_backoff.as_ref(),
            ) {
                (Some(op), _, _) => format!("{} ({op})", instance.state),
                // Loud on purpose: the restore failed, so the workload's
                // running processes are gone.
                (None, Some(_), _) => format!("{} (resumed cold)", instance.state),
                // Waiting out a reschedule backoff: the note under the table
                // says why and until when.
                (None, None, Some(backoff)) => {
                    format!("{} (backoff, attempt {})", instance.state, backoff.attempt)
                }
                (None, None, None) => instance.state.clone(),
            },
            slot: instance
                .slot_index
                .map(|slot| slot.to_string())
                .unwrap_or_else(|| "-".into()),
            current_alloc: instance
                .current_alloc_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "-".into()),
            node: instance.node_id.clone().unwrap_or_else(|| "-".into()),
            storage: instance
                .storage_handle
                .clone()
                .unwrap_or_else(|| "-".into()),
        }
    }
}

/// `instance list`'s notes under the table, one per instance waiting out a
/// reschedule backoff: `<id>: rescheduling: attempt 4 failed (...); next
/// attempt at ... (backoff 4m); ... retries now` (the manager's `detail`).
fn backoff_notes(instances: &[InstanceInfo]) -> Vec<String> {
    instances
        .iter()
        .filter(|instance| instance.reschedule_backoff.is_some())
        .map(|instance| match instance.detail.as_deref() {
            Some(detail) if detail.starts_with("rescheduling:") => {
                format!("{}: {detail}", instance.instance_id)
            }
            // Another diagnostic took `detail` (a lost node); the backoff
            // still gets its line.
            _ => format!(
                "{}: rescheduling: {}",
                instance.instance_id,
                instance
                    .reschedule_backoff
                    .as_ref()
                    .map(|backoff| backoff.summary.as_str())
                    .unwrap_or_default()
            ),
        })
        .collect()
}

/// `snap-77e0 (memory, local w-03)`, `snap-5b10 (memory, published)`,
/// `snap-1a4d (disk)`, or `-`.
fn checkpoint_column(instance: &InstanceInfo) -> String {
    if instance.archived {
        return match instance.checkpoint_id.as_deref() {
            Some(id) => format!("{id} (archived)"),
            None => "archived".into(),
        };
    }
    let Some(id) = instance.checkpoint_id.as_deref() else {
        return "-".into();
    };
    if instance.checkpoint_state.as_deref() == Some("published") {
        return format!(
            "{id} ({}, published)",
            instance.checkpoint_kind.as_deref().unwrap_or("-")
        );
    }
    match (
        instance.checkpoint_kind.as_deref(),
        instance.checkpoint_node_id.as_deref(),
    ) {
        (Some("memory"), Some(node)) => format!("{id} (memory, local {node})"),
        (Some(kind), _) => format!("{id} ({kind})"),
        (None, _) => id.to_string(),
    }
}

/// Where a session runs and how to keep it, or where a promoted instance
/// came from, for `instance get`.
fn session_line(instance: &InstanceInfo) -> Option<String> {
    if instance.mode == "session" {
        return Some(format!(
            "session: on {} inside envelope allocation {}, not durable (`legion instance \
             promote {}` gives it a row)",
            instance.node_id.as_deref().unwrap_or("-"),
            instance
                .envelope_alloc_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "-".into()),
            instance.instance_id
        ));
    }
    instance
        .promoted_from
        .as_deref()
        .map(|from| format!("promoted from session {from}"))
}

/// A TAP sandbox's leased guest address, for `instance get`
/// (`address: 10.88.128.7 (br0)`).
fn address_line(instance: &InstanceInfo) -> Option<String> {
    let address = instance.guest_address.as_deref()?;
    Some(match instance.guest_bridge.as_deref() {
        Some(bridge) if !bridge.is_empty() => format!("address: {address} ({bridge})"),
        _ => format!("address: {address}"),
    })
}

/// Where each port is served, and whether the gateway may wake the
/// instance (docs/gateway.md), for `instance get`:
/// `wake: on request to port http (hold up to 30s)` and
/// `port http: 10.0.3.17:24312`.
fn endpoint_lines(instance: &InstanceInfo) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(wake) = instance.wake.as_ref() {
        lines.push(format!(
            "\nwake: on request to port {} (hold up to {}s)",
            wake.port,
            wake.timeout_ms.div_ceil(1000)
        ));
    }
    for endpoint in &instance.endpoints {
        if endpoint.address.is_empty() {
            continue;
        }
        lines.push(format!(
            "port {}: {}:{}",
            endpoint.port, endpoint.address, endpoint.number
        ));
    }
    lines
}

/// The last suspend and restore, for `instance get`.
fn suspend_lines(instance: &InstanceInfo) -> Vec<String> {
    let mut lines = Vec::new();
    if instance.archived {
        lines.push(format!(
            "\narchived: {} (the checkpoint's snapshot row lives in this manifest; `instance \
             start` reads it back)",
            instance.manifest_url.as_deref().unwrap_or("-")
        ));
    } else if let Some(checkpoint) = instance.checkpoint_id.as_deref() {
        lines.push(format!(
            "\ncheckpoint: {checkpoint} ({}, {} on {}, {} bytes, durability {})",
            instance.checkpoint_kind.as_deref().unwrap_or("-"),
            instance.checkpoint_state.as_deref().unwrap_or("local"),
            instance.checkpoint_node_id.as_deref().unwrap_or("-"),
            instance.checkpoint_bytes.unwrap_or_default(),
            instance.checkpoint_durability.as_deref().unwrap_or("local"),
        ));
    }
    // What it keeps charged to its node while suspended (docs/
    // suspend-and-resume.md, "Capacity for resumes").
    if let Some(memory) = instance.held_memory_mib {
        lines.push(format!(
            "held: {memory:.0} MiB and {:.0} MHz reserved on {} for the resume",
            instance.held_cpu_mhz.unwrap_or_default(),
            instance.checkpoint_node_id.as_deref().unwrap_or("its node"),
        ));
    }
    if let Some(at) = instance.suspended_at.as_deref() {
        lines.push(format!(
            "last suspend: {at} ({}) in {} ms",
            instance.suspend_reason.as_deref().unwrap_or("manual"),
            instance
                .last_suspend_ms
                .map(|ms| ms.to_string())
                .unwrap_or_else(|| "-".into())
        ));
    }
    if let Some(snapshot) = instance.restored_from_snapshot_id.as_deref() {
        lines.push(format!(
            "last restore: from {snapshot} in {} ms",
            instance
                .last_restore_ms
                .map(|ms| ms.to_string())
                .unwrap_or_else(|| "-".into())
        ));
    }
    if let (Some(since), Some(after)) =
        (instance.idle_since.as_deref(), instance.idle_after_seconds)
    {
        lines.push(format!(
            "idle since: {since} (suspends after {})",
            crate::cmd::helpers::usage_display::idle(after as i64)
        ));
    }
    if let Some(cold) = instance.cold_boot.as_ref() {
        lines.push(cold_boot_line(cold));
    }
    if let Some(op) = instance.pending_op.as_deref() {
        lines.push(format!("in progress: {op}"));
    }
    if let Some(detail) = instance.detail.as_deref() {
        lines.push(format!("detail: {detail}"));
    }
    lines
}

/// `resumed cold at <time>: ...`: a restore failed and the instance booted
/// fresh, so its running processes are gone (`InstanceInfo.cold_boot`).
fn cold_boot_line(cold: &crate::structs::InstanceColdBoot) -> String {
    format!(
        "resumed cold at {}: restore from {} failed ({}). Running processes were lost, the \
         volume was kept",
        cold.at, cold.snapshot_id, cold.reason
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_list_flags_and_explains_a_backoff() {
        let detail = "rescheduling: attempt 2 failed (boom); next attempt at \
                      2026-10-12T02:31:05Z (backoff 1m); `legion instance start sb-1` retries now";
        let waiting = InstanceInfo {
            instance_id: "sb-1".into(),
            state: "rescheduling".into(),
            detail: Some(detail.into()),
            reschedule_backoff: Some(crate::structs::RescheduleBackoff {
                attempt: 2,
                summary: "attempt 2 failed (boom); next attempt at 2026-10-12T02:31:05Z \
                          (backoff 1m)"
                    .into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let running = InstanceInfo {
            instance_id: "sb-2".into(),
            state: "active".into(),
            ..Default::default()
        };
        assert_eq!(
            InstanceRow::from(&waiting).state,
            "rescheduling (backoff, attempt 2)"
        );
        assert_eq!(InstanceRow::from(&running).state, "active");
        assert_eq!(
            backoff_notes(&[waiting.clone(), running]),
            vec![format!("sb-1: {detail}")]
        );
        // A lost-node diagnostic in `detail` does not hide the backoff.
        let gated = InstanceInfo {
            detail: Some("pending-loss-confirmation: ...".into()),
            ..waiting
        };
        assert_eq!(
            backoff_notes(&[gated]),
            vec![
                "sb-1: rescheduling: attempt 2 failed (boom); next attempt at \
                 2026-10-12T02:31:05Z (backoff 1m)"
                    .to_string()
            ]
        );
    }

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
    fn instance_promote_command_parses_test() -> R<()> {
        let matches = config().try_get_matches_from([
            "instance",
            "promote",
            "workspace-7k2qd9x4m1b8c3rj5t0v6wye2h",
            "--id",
            "user-42",
            "--idempotency-key",
            "k1",
            "-j",
        ])?;
        let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
        assert_eq!(subcommand, "promote");
        assert_eq!(
            args.get_one::<String>("session").map(String::as_str),
            Some("workspace-7k2qd9x4m1b8c3rj5t0v6wye2h")
        );
        assert_eq!(
            args.get_one::<String>("id").map(String::as_str),
            Some("user-42")
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
            usage: None,
            restore_snapshot_id: None,
            ..Default::default()
        };
        let row = InstanceRow::from(&info);
        assert_eq!(row.slot, "-");
        assert_eq!(row.current_alloc, "-");
        assert_eq!(row.node, "-");
        assert_eq!(row.storage, "-");
        assert_eq!(row.memory, "-");
        assert_eq!(row.idle, "-");
        assert_eq!(row.checkpoint, "-");

        let active = InstanceInfo {
            state: "active".into(),
            current_alloc_id: Some(7),
            node_id: Some("w-02".into()),
            ..info
        };
        let row = InstanceRow::from(&active);
        assert_eq!(row.current_alloc, "7");
        assert_eq!(
            row.node, "w-02",
            "the NODE column names the allocation's node"
        );
    }

    /// The CHECKPOINT column reads like the plan's §6.6 example.
    #[test]
    fn checkpoint_column_names_kind_and_node() {
        let memory = InstanceInfo {
            state: "suspended".into(),
            checkpoint_id: Some("snap-77e0".into()),
            checkpoint_kind: Some("memory".into()),
            checkpoint_node_id: Some("w-03".into()),
            ..Default::default()
        };
        assert_eq!(checkpoint_column(&memory), "snap-77e0 (memory, local w-03)");
        let disk = InstanceInfo {
            checkpoint_id: Some("snap-1a4d".into()),
            checkpoint_kind: Some("disk".into()),
            checkpoint_node_id: Some("w-03".into()),
            ..Default::default()
        };
        assert_eq!(checkpoint_column(&disk), "snap-1a4d (disk)");
        let resuming = InstanceInfo {
            state: "pending".into(),
            pending_op: Some("resuming".into()),
            ..Default::default()
        };
        assert_eq!(InstanceRow::from(&resuming).state, "pending (resuming)");
    }

    /// A cold boot in place of a failed restore shows in `instance list`'s
    /// STATE column and as its own `instance get` line.
    #[test]
    fn a_cold_boot_is_named_in_the_state_and_the_get_lines() {
        let cold = InstanceInfo {
            state: "active".into(),
            cold_boot: Some(crate::structs::InstanceColdBoot {
                snapshot_id: "snap-77e0".into(),
                reason: "restore needs TAP device lgf1: creating it failed".into(),
                at: "2026-10-10T12:00:00Z".into(),
            }),
            ..Default::default()
        };
        assert_eq!(InstanceRow::from(&cold).state, "active (resumed cold)");
        let lines = suspend_lines(&cold);
        assert!(
            lines.iter().any(|line| line
                == "resumed cold at 2026-10-10T12:00:00Z: restore from snap-77e0 failed (restore \
                    needs TAP device lgf1: creating it failed). Running processes were lost, the \
                    volume was kept"),
            "{lines:?}"
        );
    }

    /// Gated plan §6.7: a TAP sandbox shows the address Legion leased it.
    #[test]
    fn instance_get_shows_a_tap_guest_address() {
        let tap = InstanceInfo {
            guest_address: Some("10.88.128.7".into()),
            guest_bridge: Some("br0".into()),
            ..Default::default()
        };
        assert_eq!(
            address_line(&tap).as_deref(),
            Some("address: 10.88.128.7 (br0)")
        );
        assert_eq!(address_line(&InstanceInfo::default()), None);
    }

    #[test]
    fn instance_get_shows_wake_and_served_ports_test() {
        let info = InstanceInfo {
            wake: Some(crate::structs::InstanceWake {
                port: "http".into(),
                timeout_ms: 30_000,
            }),
            endpoints: vec![crate::structs::InstanceEndpoint {
                port: "http".into(),
                address: "10.0.3.17".into(),
                number: 24312,
            }],
            ..Default::default()
        };
        assert_eq!(
            endpoint_lines(&info),
            vec![
                "\nwake: on request to port http (hold up to 30s)".to_string(),
                "port http: 10.0.3.17:24312".to_string(),
            ]
        );
        assert!(endpoint_lines(&InstanceInfo::default()).is_empty());
    }

    /// `instance get` names why the last suspend happened and, while the
    /// idle policy is watching, since when and for how long.
    #[test]
    fn suspend_lines_show_the_reason_and_the_idle_window() {
        let idle = InstanceInfo {
            state: "active".into(),
            idle_since: Some("2026-10-10T12:00:00Z".into()),
            idle_after_seconds: Some(180),
            suspended_at: Some("2026-10-10T11:00:00Z".into()),
            suspend_reason: Some("pressure".into()),
            last_suspend_ms: Some(740),
            ..Default::default()
        };
        let lines = suspend_lines(&idle);
        assert!(
            lines.contains(&"last suspend: 2026-10-10T11:00:00Z (pressure) in 740 ms".to_string()),
            "{lines:?}"
        );
        assert!(
            lines.contains(&"idle since: 2026-10-10T12:00:00Z (suspends after 3m0s)".to_string()),
            "{lines:?}"
        );
        let held = InstanceInfo {
            state: "suspended".into(),
            checkpoint_node_id: Some("w-03".into()),
            held_memory_mib: Some(512.0),
            held_cpu_mhz: Some(250.0),
            ..Default::default()
        };
        assert!(
            suspend_lines(&held)
                .contains(&"held: 512 MiB and 250 MHz reserved on w-03 for the resume".to_string()),
            "{:?}",
            suspend_lines(&held)
        );
    }

    #[test]
    fn suspend_and_start_flags_parse() -> R<()> {
        let matches = config().try_get_matches_from([
            "instance",
            "suspend",
            "user-42",
            "--checkpoint",
            "disk",
            "--wait",
            "--timeout",
            "9",
            "-j",
        ])?;
        let (subcommand, args) = matches.subcommand().context("Missing subcommand")?;
        assert_eq!(subcommand, "suspend");
        assert_eq!(args.get_one::<String>("checkpoint").unwrap(), "disk");
        assert!(args.get_flag("wait"));
        assert!(config()
            .try_get_matches_from(["instance", "suspend", "x", "--checkpoint", "ram"])
            .is_err());
        let matches = config().try_get_matches_from(["instance", "start", "user-42", "--cold"])?;
        let (_, args) = matches.subcommand().context("Missing subcommand")?;
        assert!(args.get_flag("cold"));
        Ok(())
    }
}
