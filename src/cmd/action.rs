//! `legion action` — list and run declared actions against an allocation.
//!
//! Actions are pre-baked, parameterized command templates declared on a task.
//! `legion action list <job>` shows what's available; `legion action run <job>
//! <action> --alloc <id> --param k=v` runs one, streaming output like exec.

use std::collections::HashMap;

use clap::{Arg, ArgAction, ArgMatches, Command};

use crate::cmd::exec::run_action_session;
use crate::cmd::helpers::{connect_manager_client, friendly_status};
use crate::prelude::*;
use crate::structs::{ExecStart, GetLedgerRequest, ListJobActionsRequest};

pub fn config() -> Command {
    Command::new("action")
        .about("List and run declared actions against an allocation")
        .subcommand_required(true)
        .subcommand(
            Command::new("list")
                .about("List actions declared on a job's tasks")
                .arg(Arg::new("job").help("Job name").required(true)),
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
                ),
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
    let job = matches.get_one::<String>("job").expect("required");
    let mut client = connect_manager_client().await?;
    let reply = client
        .list_job_actions(ListJobActionsRequest { job: job.clone() })
        .await
        .map_err(|e| friendly_status("action", e))?
        .into_inner();

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
    let job = matches.get_one::<String>("job").expect("required").clone();
    let action = matches
        .get_one::<String>("action")
        .expect("required")
        .clone();
    let tty = matches.get_flag("tty");
    let want_stdin = matches.get_flag("stdin") || tty;

    let mut params: HashMap<String, String> = HashMap::new();
    if let Some(values) = matches.get_many::<String>("param") {
        for raw in values {
            let (k, v) = raw
                .split_once('=')
                .ok_or_else(|| anyhow!("--param must be key=value, got `{raw}`"))?;
            params.insert(k.to_string(), v.to_string());
        }
    }

    let mut client = connect_manager_client().await?;

    let instance_id = matches
        .get_one::<String>("instance")
        .cloned()
        .unwrap_or_default();
    let allocation_id = match matches.get_one::<String>("alloc") {
        Some(raw) => raw
            .parse::<i64>()
            .with_context(|| format!("invalid allocation id `{raw}`"))?,
        None if !instance_id.is_empty() => 0,
        None => newest_alloc_for_job(&mut client, &job).await?,
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

    run_action_session(&mut client, start, want_stdin, tty).await
}

async fn newest_alloc_for_job(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    job: &str,
) -> R<i64> {
    let ledger = client
        .get_ledger(GetLedgerRequest {})
        .await
        .map_err(|e| friendly_status("action", e))?
        .into_inner();
    let mut best: Option<i64> = None;
    for node in ledger.nodes {
        for alloc in node.allocations {
            if alloc.job_id == job
                && (alloc.status == "placed"
                    || alloc.status == "active"
                    || alloc.status == "running")
            {
                best = Some(best.map_or(alloc.id, |b| b.max(alloc.id)));
            }
        }
    }
    best.ok_or_else(|| {
        anyhow!("no running allocation found for job `{job}`; pass --alloc <id> explicitly")
    })
}
