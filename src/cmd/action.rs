//! `legion action` — list and run declared actions against an allocation.
//!
//! Actions are pre-baked, parameterized command templates declared on a task.
//! `legion action list <job>` shows what's available; `legion action run <job>
//! <action> --alloc <id> --param k=v` runs one, streaming output like exec.

use std::collections::HashMap;

use clap::{Arg, ArgAction, ArgMatches, Command};

use crate::cmd::exec::{newest_running_allocation, reject_interactive_json, run_action_session};
use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{ExecStart, ListJobActionsRequest};

pub fn config() -> Command {
    Command::new("action")
        .about("List and run declared actions against an allocation")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List actions declared on a job's tasks")
                .arg(Arg::new("job").help("Job name").required(true))
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("run")
                .about("Run an action against a running allocation")
                .arg(Arg::new("job").help("Job name").required(true))
                .arg(Arg::new("action").help("Action name").required(true))
                .arg(
                    Arg::new("alloc")
                        .long("alloc")
                        .help("Allocation id (defaults to the newest running one)")
                        .required(false),
                )
                .arg(
                    Arg::new("instance")
                        .long("instance")
                        .value_name("ID")
                        .conflicts_with("alloc")
                        .help("Target an instance: its current allocation, across resumes")
                        .required(false),
                )
                .arg(
                    Arg::new("param")
                        .long("param")
                        .help("Parameter as key=value (repeatable)")
                        .action(ArgAction::Append)
                        .required(false),
                )
                .arg(
                    Arg::new("tty")
                        .short('t')
                        .long("tty")
                        .help("Force an interactive pseudo-terminal")
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("stdin")
                        .short('i')
                        .long("stdin")
                        .help("Forward local stdin to the action")
                        .action(ArgAction::SetTrue),
                )
                .arg(json_flag().help(
                    "Collect the output and print one JSON envelope with the exit code \
                     (not with --tty or --stdin)",
                )),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("list", sub)) => list(sub).await,
        Some(("run", sub)) => run(sub).await,
        _ => bail!("unknown action subcommand"),
    }
}

async fn list(matches: &ArgMatches) -> R<()> {
    let job = &crate::cmd::helpers::required_job_arg(matches, "job")?;
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to list actions",
        client
            .list_job_actions(ListJobActionsRequest { job: job.clone() })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply.actions);
        return Ok(());
    }

    if reply.actions.is_empty() {
        println!("No actions declared on job `{job}`.");
        return Ok(());
    }

    for action in reply.actions {
        println!(
            "{}  (task: {}){}",
            action.name,
            action.task_name,
            if action.tty { "  [tty]" } else { "" }
        );
        if !action.description.is_empty() {
            println!("    {}", action.description);
        }
        for p in action.params {
            let mut bits = vec![p.r#type.clone()];
            if p.required {
                bits.push("required".into());
            }
            if !p.default.is_empty() {
                bits.push(format!("default={}", p.default));
            }
            if !p.values.is_empty() {
                bits.push(format!("one of [{}]", p.values.join(", ")));
            }
            if !p.pattern.is_empty() {
                bits.push(format!("pattern={}", p.pattern));
            }
            if p.sensitive {
                bits.push("sensitive".into());
            }
            println!("      - {} ({})", p.name, bits.join(", "));
        }
    }
    Ok(())
}

async fn run(matches: &ArgMatches) -> R<()> {
    let job = crate::cmd::helpers::required_job_arg(matches, "job")?;
    let action = matches
        .get_one::<String>("action")
        .expect("required")
        .clone();
    let tty = matches.get_flag("tty");
    let format = OutputFormat::from_matches(matches);
    reject_interactive_json(format, tty, matches.get_flag("stdin"))?;
    let want_stdin = matches.get_flag("stdin") || tty;
    let invalid = |message: String| {
        output::fail_local(format, CliError::local("InvalidArgument", message, None))
    };

    let mut params: HashMap<String, String> = HashMap::new();
    if let Some(values) = matches.get_many::<String>("param") {
        for raw in values {
            let (k, v) = raw
                .split_once('=')
                .ok_or_else(|| invalid(format!("--param must be key=value, got `{raw}`")))?;
            params.insert(k.to_string(), v.to_string());
        }
    }

    let mut client = connect_manager_client().await?;

    let instance_id = crate::cmd::helpers::instance_arg(matches, "instance")?.unwrap_or_default();
    let allocation_id = match matches.get_one::<String>("alloc") {
        Some(raw) => raw
            .parse::<i64>()
            .map_err(|_| invalid(format!("invalid allocation id `{raw}`")))?,
        None if !instance_id.is_empty() => 0,
        None => newest_running_allocation(&mut client, &job, format).await?,
    };

    let start = ExecStart {
        allocation_id,
        task_name: String::new(),
        command: Vec::new(),
        action,
        action_params: params,
        tty,
        winsize: None,
        bearer_token: String::new(),
        instance_id,
        exec_ticket: String::new(),
    };

    run_action_session(&mut client, start, want_stdin, tty, format).await
}
