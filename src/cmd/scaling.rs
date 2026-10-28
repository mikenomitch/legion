//! `legion scaling` — task autoscaling (docs/autoscaling.md).
//!
//! - `status`: every autoscaled task's count, what the autoscaler wants,
//!   bounds, and state, and whether the autoscaler is running at all;
//! - `explain`: why one task is at its count, stage by stage, and with
//!   `--policy` where every field of its effective policy came from;
//! - `history`: the durable record of changes (people's and the
//!   autoscaler's, and a dry-run policy's "would" rows);
//! - `pause` / `resume`: freeze autoscaling for a task, a job, or the whole
//!   cluster (`--all`);
//! - `defaults get` / `defaults apply`: the cluster's scaling defaults, and
//!   the effective defaults for a namespace with provenance;
//! - `demand`: how many nodes of each node-group template the pending work
//!   needs ([`crate::cmd::scaling_demand`]);
//! - `apply` / `list` / `delete`: standalone policies, for targets that are
//!   not Legion tasks, and node groups ([`crate::cmd::scaling_policy`]);
//! - `simulate`: a policy over the last hour of history or a CSV series
//!   ([`crate::cmd::scaling_simulate`]);
//! - `plugin test`: the scaling-plugin conformance suite
//!   ([`crate::cmd::scaling_plugin`]).
//!
//! `explain`, `history`, `pause` and `resume` take `--standalone` to name a
//! standalone policy (or a node group) instead of a job.
//!
//! Every subcommand takes `--json`/`-j` and emits the standard
//! [`crate::cmd::output`] envelope.

use crate::autoscaler::nodes::{NodeDecision, NodeIntent};
use crate::autoscaler::{Decision, Stage};
use crate::cmd::helpers::{
    active_namespace, connect_manager_client, json_flag, list_namespace_flags, namespace_flag,
    namespace_selection, print_table, required_job_arg,
};
use crate::cmd::job::idempotency_arg;
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    ApplyScalingDefaultsRequest, GetScaleHistoryRequest, GetScalingDefaultsRequest,
    GetScalingStatusRequest, PauseScalingRequest, ScaleHistoryEntry, ScalingAppStatus,
    ScalingTargetStatus,
};
use clap::value_parser;
use std::path::Path;
use tabled::Tabled;

/// `14:05 UTC` for a unix-ms time; `-` for 0.
pub fn clock(at_ms: i64) -> String {
    if at_ms <= 0 {
        return "-".to_string();
    }
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(at_ms)
        .map(|at| at.format("%H:%M UTC").to_string())
        .unwrap_or_else(|| at_ms.to_string())
}

fn clock_seconds(at_ms: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(at_ms)
        .map(|at| at.format("%H:%M:%S").to_string())
        .unwrap_or_else(|| at_ms.to_string())
}

fn ago(at_ms: i64) -> String {
    if at_ms <= 0 {
        return "never".to_string();
    }
    let now = chrono::Utc::now().timestamp_millis();
    let seconds = ((now - at_ms) / 1000).max(0);
    match seconds {
        0..=119 => format!("{seconds}s ago"),
        120..=7199 => format!("{}m ago", seconds / 60),
        _ => format!("{}h ago", seconds / 3600),
    }
}

fn standalone_arg() -> Arg {
    Arg::new("standalone")
        .long("standalone")
        .action(ArgAction::SetTrue)
        .conflicts_with_all(["group", "task"])
        .help("NAME is a standalone policy or a node group (legion scaling apply), not a job")
}

/// The standalone policy the command names, when `--standalone` is set.
fn standalone_policy(matches: &ArgMatches) -> R<Option<String>> {
    if matches
        .try_get_one::<bool>("standalone")
        .ok()
        .flatten()
        .copied()
        .unwrap_or(false)
    {
        Ok(Some(required_job_arg(matches, "job")?))
    } else {
        Ok(None)
    }
}

fn target_arg() -> [Arg; 2] {
    [
        Arg::new("group")
            .long("group")
            .value_name("GROUP")
            .help("Task group, when the job has more than one autoscaled task"),
        Arg::new("task")
            .long("task")
            .value_name("TASK")
            .help("Task name, when the job has more than one autoscaled task"),
    ]
}

pub fn config() -> Command {
    let job_arg = |required: bool| {
        Arg::new("job")
            .value_name("JOB")
            .required(required)
            .help("Job name (`name` or `namespace/name`); with --standalone, a standalone policy's")
    };
    Command::new("scaling")
        .about("Autoscaling: status, explanations, history, pauses, defaults, standalone policies, simulation, plugins, and node-group capacity demand (docs/autoscaling.md)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(crate::cmd::scaling_demand::demand_command())
        .subcommand(crate::cmd::scaling_policy::apply_command())
        .subcommand(crate::cmd::scaling_policy::list_command())
        .subcommand(crate::cmd::scaling_policy::delete_command())
        .subcommand(crate::cmd::scaling_simulate::simulate_command())
        .subcommand(crate::cmd::scaling_plugin::plugin_command())
        .subcommand(
            Command::new("status")
                .about("Every autoscaled task's count, wanted count, bounds, and state, and whether the autoscaler runs")
                .arg(job_arg(false))
                .args(list_namespace_flags())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("explain")
                .about("Why an autoscaled task is at its count: the last evaluation, stage by stage")
                .arg(job_arg(true))
                .args(target_arg())
                .arg(standalone_arg())
                .arg(namespace_flag())
                .arg(
                    Arg::new("policy")
                        .long("policy")
                        .action(ArgAction::SetTrue)
                        .help("Show the effective policy and where each field came from"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("history")
                .about("The durable record of a job's count changes, newest first")
                .arg(job_arg(true))
                .args(target_arg())
                .arg(standalone_arg())
                .arg(namespace_flag())
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .value_name("N")
                        .value_parser(value_parser!(u32))
                        .help("Rows to show (default 50, at most 1000)"),
                )
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("pause")
                .about("Pause autoscaling for a task, a job, or the cluster (--all)")
                .arg(job_arg(false).required_unless_present("all"))
                .args(target_arg())
                .arg(standalone_arg())
                .arg(namespace_flag())
                .arg(
                    Arg::new("all")
                        .long("all")
                        .action(ArgAction::SetTrue)
                        .conflicts_with_all(["job", "group", "task", "standalone"])
                        .help("Pause every autoscaled task and standalone policy in the cluster (needs operator:write)"),
                )
                .arg(
                    Arg::new("for")
                        .long("for")
                        .value_name("DURATION")
                        .help("How long, e.g. 1h (default: until `legion scaling resume`)"),
                )
                .arg(
                    Arg::new("reason")
                        .long("reason")
                        .value_name("REASON")
                        .help("Why, shown on every status surface"),
                )
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("resume")
                .about("Resume autoscaling paused at the same scope")
                .arg(job_arg(false).required_unless_present("all"))
                .args(target_arg())
                .arg(standalone_arg())
                .arg(namespace_flag())
                .arg(
                    Arg::new("all")
                        .long("all")
                        .action(ArgAction::SetTrue)
                        .conflicts_with_all(["job", "group", "task", "standalone"])
                        .help("Lift the cluster-wide pause"),
                )
                .arg(idempotency_arg())
                .arg(json_flag()),
        )
        .subcommand(
            Command::new("defaults")
                .about("The cluster's scaling defaults")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("get")
                        .about("Show the cluster's defaults and the effective defaults for a namespace, with where each field came from")
                        .arg(namespace_flag())
                        .arg(json_flag()),
                )
                .subcommand(
                    Command::new("apply")
                        .about("Set the cluster's defaults from a Nickel or JSON file (the autoscale record, without checks)")
                        .arg(
                            Arg::new("file")
                                .required(true)
                                .value_name("FILE")
                                .help("Defaults file (.ncl or .json); `{}` clears every cluster default")
                                .value_hint(ValueHint::FilePath),
                        )
                        .arg(
                            Arg::new("dry-run")
                                .long("dry-run")
                                .action(ArgAction::SetTrue)
                                .help("Report which autoscaled tasks' policies would change, without writing"),
                        )
                        .arg(
                            Arg::new("force")
                                .long("force")
                                .action(ArgAction::SetTrue)
                                .help("Apply even when some tasks' policies become invalid; they are held until fixed"),
                        )
                        .arg(idempotency_arg())
                        .arg(json_flag()),
                ),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    match matches.subcommand() {
        Some(("status", sub)) => status(sub).await,
        Some(("explain", sub)) => explain(sub).await,
        Some(("history", sub)) => history(sub).await,
        Some(("pause", sub)) => pause(sub, false).await,
        Some(("resume", sub)) => pause(sub, true).await,
        Some(("defaults", sub)) => match sub.subcommand() {
            Some(("get", sub)) => defaults_get(sub).await,
            Some(("apply", sub)) => defaults_apply(sub).await,
            _ => unreachable!("subcommand required"),
        },
        Some(("demand", sub)) => crate::cmd::scaling_demand::handle_demand(sub).await,
        Some(("apply", sub)) => crate::cmd::scaling_policy::apply(sub).await,
        Some(("list", sub)) => crate::cmd::scaling_policy::list(sub).await,
        Some(("delete", sub)) => crate::cmd::scaling_policy::delete(sub).await,
        Some(("simulate", sub)) => crate::cmd::scaling_simulate::handle_simulate(sub).await,
        Some(("plugin", sub)) => crate::cmd::scaling_plugin::handle_plugin(sub).await,
        _ => unreachable!("subcommand required"),
    }
}

fn opt(matches: &ArgMatches, key: &str) -> Option<String> {
    matches.try_get_one::<String>(key).ok().flatten().cloned()
}

fn label(target: &ScalingTargetStatus) -> String {
    if !target.policy_id.is_empty() {
        return target.policy_id.clone();
    }
    format!(
        "{}/{}/{}",
        target.job_id, target.task_group, target.task_name
    )
}

fn app_line(app: &ScalingAppStatus) -> String {
    let mut line = format!("autoscaler: {}", app.message);
    if let Some(pause) = app.paused_all.as_ref() {
        line.push_str(&format!(
            "\nautoscaling is PAUSED cluster-wide by {}{}{}",
            pause.by,
            pause
                .until_unix_ms
                .map(|until| format!(" until {}", clock(until)))
                .unwrap_or_default(),
            if pause.reason.is_empty() {
                String::new()
            } else {
                format!(": \"{}\"", pause.reason)
            }
        ));
    }
    line
}

fn state_words(target: &ScalingTargetStatus) -> String {
    if target.pause.is_some() {
        return "paused".to_string();
    }
    if target.pin.is_some() {
        return "pinned".to_string();
    }
    target.state.clone()
}

fn last_words(target: &ScalingTargetStatus) -> String {
    match target.last_change.as_ref() {
        Some(change) if change.kind == "would" => {
            format!(
                "would {} → {} {}",
                change.from,
                change.to,
                ago(change.at_unix_ms)
            )
        }
        Some(change) => format!(
            "{} → {} {} ({})",
            change.from,
            change.to,
            ago(change.at_unix_ms),
            change.source
        ),
        None => "-".to_string(),
    }
}

#[derive(Tabled)]
struct StatusRow {
    #[tabled(rename = "TARGET")]
    target: String,
    #[tabled(rename = "KIND")]
    kind: String,
    #[tabled(rename = "NOW")]
    now: u32,
    #[tabled(rename = "WANT")]
    want: u32,
    #[tabled(rename = "BOUNDS")]
    bounds: String,
    #[tabled(rename = "MODE")]
    mode: String,
    #[tabled(rename = "STATE")]
    state: String,
    #[tabled(rename = "LAST")]
    last: String,
}

async fn status(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let job = if opt(matches, "job").is_some() {
        Some(required_job_arg(matches, "job")?)
    } else {
        None
    };
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to read scaling status",
        client
            .get_scaling_status(GetScalingStatusRequest {
                namespaces: if job.is_some() {
                    Vec::new()
                } else {
                    namespace_selection(matches)
                },
                job: job.unwrap_or_default(),
                ..Default::default()
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply);
        return Ok(());
    }
    let rows: Vec<StatusRow> = reply
        .targets
        .iter()
        .map(|target| StatusRow {
            target: label(target),
            kind: if !target.kind.is_empty() {
                target.kind.clone()
            } else if target.members.len() > 1 {
                "group".to_string()
            } else {
                "task".to_string()
            },
            now: target.now,
            want: target.want,
            bounds: if target.error.is_empty() {
                format!("{}..{}", target.min, target.max)
            } else {
                "-".to_string()
            },
            mode: target.mode.clone(),
            state: state_words(target),
            last: last_words(target),
        })
        .collect();
    if rows.is_empty() {
        println!("no autoscaled tasks or standalone policies");
    } else {
        print_table(rows);
    }
    if let Some(app) = reply.app.as_ref() {
        println!("{}", app_line(app));
    }
    Ok(())
}

#[derive(Tabled)]
struct CheckRow {
    #[tabled(rename = "CHECK")]
    check: String,
    #[tabled(rename = "INPUT")]
    input: String,
    #[tabled(rename = "WANTS")]
    wants: String,
}

#[derive(Tabled)]
struct StageRow {
    #[tabled(rename = "STAGE")]
    stage: String,
    #[tabled(rename = "RESULT")]
    result: String,
    #[tabled(rename = "NOTE")]
    note: String,
}

#[derive(Tabled)]
struct FieldRow {
    #[tabled(rename = "FIELD")]
    field: String,
    #[tabled(rename = "VALUE")]
    value: String,
    #[tabled(rename = "FROM")]
    from: String,
}

/// The stages worth a line: every one that changed the number or has
/// something to say.
fn stage_rows(decision: &Decision, decision_id: &str) -> Vec<StageRow> {
    let mut rows: Vec<StageRow> = decision
        .stages
        .iter()
        .filter(|note| note.stage != Stage::Mode)
        .filter(|note| note.changed() || !note.note.is_empty())
        .map(|note| StageRow {
            stage: note.stage.to_string(),
            result: note.after.to_string(),
            note: note.note.clone(),
        })
        .collect();
    let outcome = match decision.kind {
        crate::autoscaler::DecisionKind::Applied => format!(
            "applied {}{}",
            clock_seconds(decision.at_ms),
            if decision_id.is_empty() {
                String::new()
            } else {
                format!(" ({decision_id})")
            }
        ),
        crate::autoscaler::DecisionKind::Would => "dry-run: would scale, wrote nothing".to_string(),
        crate::autoscaler::DecisionKind::Held => "held".to_string(),
    };
    rows.push(StageRow {
        stage: "decision".to_string(),
        result: if decision.from == decision.to {
            decision.to.to_string()
        } else {
            format!("{} → {}", decision.from, decision.to)
        },
        note: outcome,
    });
    rows
}

async fn explain(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let standalone = standalone_policy(matches)?;
    let job = required_job_arg(matches, "job")?;
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to read scaling status",
        client
            .get_scaling_status(match &standalone {
                Some(policy) => GetScalingStatusRequest {
                    policy: policy.clone(),
                    detail: true,
                    ..Default::default()
                },
                None => GetScalingStatusRequest {
                    job: job.clone(),
                    task_group: opt(matches, "group"),
                    task_name: opt(matches, "task"),
                    detail: true,
                    ..Default::default()
                },
            })
            .await,
    )?
    .into_inner();
    let show_policy = matches.get_flag("policy");
    if reply.targets.is_empty() {
        return Err(output::fail_local(
            format,
            CliError::local(
                "not_autoscaled",
                format!("{job} has no autoscaled task matching that selection"),
                Some("add `autoscale = true` to a task; `legion scaling status` lists autoscaled tasks"),
            ),
        ));
    }
    if format.is_json() {
        let targets: Vec<Value> = reply
            .targets
            .iter()
            .map(|target| {
                json!({
                    "target": label(target),
                    "members": target.members,
                    "mode": target.mode,
                    "now": target.now,
                    "want": target.want,
                    "min": target.min,
                    "max": target.max,
                    "state": target.state,
                    "summary": target.summary,
                    "lastEvaluationUnixMs": target.last_evaluation_unix_ms,
                    "decision": serde_json::from_str::<Value>(&target.decision_json).ok(),
                    "policy": target.policy.iter().map(|row| json!({
                        "field": row.field, "value": row.value, "origin": row.origin,
                    })).collect::<Vec<_>>(),
                    "effectivePolicy": serde_json::from_str::<Value>(&target.policy_json).ok(),
                    "policyDigest": target.policy_digest,
                    "pin": target.pin,
                    "pause": target.pause,
                    "error": target.error,
                })
            })
            .collect();
        output::print_success(&json!({ "app": reply.app, "targets": targets }));
        return Ok(());
    }
    let app = reply.app.clone().unwrap_or_default();
    for (index, target) in reply.targets.iter().enumerate() {
        if index > 0 {
            println!();
        }
        println!(
            "{}   {}   {}   now {}   bounds {}..{}   evaluated {}",
            label(target),
            if target.kind == "nodes" {
                "node group".to_string()
            } else if !target.policy_id.is_empty() {
                "standalone policy".to_string()
            } else if target.members.len() > 1 {
                format!("group ({})", target.members.join(", "))
            } else {
                "task".to_string()
            },
            if target.mode.is_empty() {
                "-"
            } else {
                &target.mode
            },
            target.now,
            target.min,
            target.max,
            ago(target.last_evaluation_unix_ms),
        );
        if !target.error.is_empty() {
            println!("  policy is INVALID and held: {}", target.error);
        }
        if let Some(pause) = target.pause.as_ref() {
            println!(
                "  paused ({}) by {}{}{}",
                pause.scope,
                pause.by,
                pause
                    .until_unix_ms
                    .map(|until| format!(" until {}", clock(until)))
                    .unwrap_or_default(),
                if pause.reason.is_empty() {
                    String::new()
                } else {
                    format!(": \"{}\"", pause.reason)
                }
            );
        }
        if let Some(pin) = target.pin.as_ref() {
            println!(
                "  pinned {} by {} until {}{}",
                match (pin.min, pin.max) {
                    (Some(min), Some(max)) if min == max => format!("at {min}"),
                    (min, max) => format!(
                        "to {}..{}",
                        min.map_or("-".to_string(), |n| n.to_string()),
                        max.map_or("-".to_string(), |n| n.to_string())
                    ),
                },
                pin.by,
                clock(pin.until_unix_ms),
                if pin.reason.is_empty() {
                    String::new()
                } else {
                    format!(": \"{}\"", pin.reason)
                }
            );
        }
        let node_decision = (target.kind == "nodes")
            .then(|| serde_json::from_str::<NodeDecision>(&target.decision_json).ok())
            .flatten();
        if let Some(decision) = node_decision {
            println!();
            print_node_decision(&decision);
        } else {
            match serde_json::from_str::<Decision>(&target.decision_json) {
                Ok(decision) => {
                    println!();
                    let checks: Vec<CheckRow> = decision
                        .checks
                        .iter()
                        .map(|check| CheckRow {
                            check: check.name.clone(),
                            input: check.detail.clone(),
                            wants: check
                                .desired
                                .map_or("-".to_string(), |desired| desired.to_string()),
                        })
                        .collect();
                    if !checks.is_empty() {
                        print_table(checks);
                    }
                    // The history row of this decision, when it acted.
                    let decision_id = target
                        .last_change
                        .as_ref()
                        .filter(|change| {
                            change.from == decision.from
                                && change.to == decision.to
                                && change.at_unix_ms >= decision.at_ms
                        })
                        .map(|change| change.decision_id.as_str())
                        .unwrap_or_default();
                    print_table(stage_rows(&decision, decision_id));
                }
                Err(_) => println!(
                    "  no evaluation yet ({}): {}",
                    target.state,
                    if target.summary.is_empty() {
                        "the autoscaler has not reported this task"
                    } else {
                        &target.summary
                    }
                ),
            }
        }
        if show_policy {
            println!(
                "\n  effective policy {}",
                target
                    .policy_digest
                    .get(..12)
                    .unwrap_or(&target.policy_digest)
            );
            print_table(
                target
                    .policy
                    .iter()
                    .map(|row| FieldRow {
                        field: row.field.clone(),
                        value: row.value.clone(),
                        from: row.origin.clone(),
                    })
                    .collect(),
            );
        }
    }
    println!("{}", app_line(&app));
    Ok(())
}

/// `legion scaling explain` for a node group: the stages, the victims and
/// protected members, and what the last evaluation did.
fn print_node_decision(decision: &NodeDecision) {
    let rows: Vec<StageRow> = decision
        .stages
        .iter()
        .filter(|note| note.changed() || !note.note.is_empty())
        .map(|note| StageRow {
            stage: note.stage.to_string(),
            result: note.after.to_string(),
            note: note.note.clone(),
        })
        .collect();
    if !rows.is_empty() {
        print_table(rows);
    }
    for victim in &decision.victims {
        println!("  scale-in: {} ({})", victim.node, victim.reason);
    }
    for protected in &decision.protected {
        println!(
            "  never a victim: {} ({})",
            protected.node, protected.protection
        );
    }
    if !decision.intents.is_empty() {
        let actions: Vec<String> = decision
            .intents
            .iter()
            .map(|intent| match intent {
                NodeIntent::SetDesired { count } => format!("target/scale to {count}"),
                NodeIntent::Cordon { node } => format!("drain {node}"),
                NodeIntent::Uncordon { node } => format!("uncordon {node}"),
                NodeIntent::Terminate { node, provider_id } => format!(
                    "terminate {node} ({})",
                    provider_id.as_deref().unwrap_or("no provider id")
                ),
                NodeIntent::Forget { node } => format!("forget {node}"),
            })
            .collect();
        println!("  actions: {}", actions.join(", "));
    }
    println!("  {}", decision.summary);
}

#[derive(Tabled)]
struct HistoryRow {
    #[tabled(rename = "TIME")]
    time: String,
    #[tabled(rename = "TASK")]
    task: String,
    #[tabled(rename = "CHANGE")]
    change: String,
    #[tabled(rename = "BY")]
    by: String,
    #[tabled(rename = "SUMMARY")]
    summary: String,
}

fn history_change(entry: &ScaleHistoryEntry) -> String {
    match entry.kind.as_str() {
        "would" => format!("would {} → {}", entry.from, entry.to),
        "deferred" => format!("{} → {} (deferred)", entry.from, entry.to),
        "released" => "pin released".to_string(),
        _ if entry.from == entry.to => "-".to_string(),
        _ => format!("{} → {}", entry.from, entry.to),
    }
}

async fn history(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let standalone = standalone_policy(matches)?;
    let job = required_job_arg(matches, "job")?;
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to read scale history",
        client
            .get_scale_history(GetScaleHistoryRequest {
                job: if standalone.is_some() {
                    String::new()
                } else {
                    job
                },
                task_group: opt(matches, "group"),
                task_name: opt(matches, "task"),
                limit: matches.get_one::<u32>("limit").copied().unwrap_or(0),
                detail: format.is_json(),
                policy: standalone.unwrap_or_default(),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        let entries: Vec<Value> = reply
            .entries
            .iter()
            .map(|entry| {
                let mut value = serde_json::to_value(entry).unwrap_or_default();
                if let Value::Object(object) = &mut value {
                    object.insert(
                        "decision".into(),
                        serde_json::from_str::<Value>(&entry.decision_json).unwrap_or(Value::Null),
                    );
                    object.insert(
                        "policy".into(),
                        serde_json::from_str::<Value>(&entry.policy_json).unwrap_or(Value::Null),
                    );
                    object.remove("decisionJson");
                    object.remove("policyJson");
                }
                value
            })
            .collect();
        output::print_success(&json!({ "entries": entries }));
        return Ok(());
    }
    if reply.entries.is_empty() {
        println!("no scaling history");
        return Ok(());
    }
    print_table(
        reply
            .entries
            .iter()
            .map(|entry| HistoryRow {
                time: chrono::DateTime::<chrono::Utc>::from_timestamp_millis(entry.at_unix_ms)
                    .map(|at| at.format("%m-%d %H:%M:%S").to_string())
                    .unwrap_or_default(),
                task: if entry.policy.is_empty() {
                    format!("{}/{}", entry.task_group, entry.task_name)
                } else {
                    entry.policy.clone()
                },
                change: history_change(entry),
                by: if entry.source == "operator" && !entry.principal.is_empty() {
                    entry.principal.clone()
                } else {
                    entry.source.clone()
                },
                summary: entry.summary.clone(),
            })
            .collect(),
    );
    Ok(())
}

async fn pause(matches: &ArgMatches, resume: bool) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let all = matches.get_flag("all");
    let standalone = if all {
        None
    } else {
        standalone_policy(matches)?
    };
    let job = if all || standalone.is_some() {
        String::new()
    } else {
        required_job_arg(matches, "job")?
    };
    let for_seconds = match opt(matches, "for") {
        Some(text) => Some(
            humantime::parse_duration(text.trim())
                .map_err(|err| {
                    output::fail_local(
                        format,
                        CliError::local(
                            "invalid_duration",
                            format!("--for {text:?}: {err}"),
                            Some("write it like 30m or 2h"),
                        ),
                    )
                })?
                .as_secs()
                .max(1),
        ),
        None => None,
    };
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        if resume {
            "failed to resume autoscaling"
        } else {
            "failed to pause autoscaling"
        },
        client
            .pause_scaling(PauseScalingRequest {
                job,
                task_group: opt(matches, "group"),
                task_name: opt(matches, "task"),
                all,
                for_seconds,
                reason: opt(matches, "reason").unwrap_or_default(),
                resume,
                idempotency_key: opt(matches, "idempotency-key"),
                policy: standalone.unwrap_or_default(),
            })
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

async fn defaults_get(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let namespace = active_namespace(matches).unwrap_or_default();
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to read scaling defaults",
        client
            .get_scaling_defaults(GetScalingDefaultsRequest { namespace })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        let mut value = serde_json::to_value(&reply).unwrap_or_default();
        if let Value::Object(object) = &mut value {
            object.insert(
                "cluster".into(),
                serde_json::from_str::<Value>(&reply.cluster_json).unwrap_or(Value::Null),
            );
            object.remove("clusterJson");
        }
        output::print_success(&value);
        return Ok(());
    }
    if reply.version == 0 {
        println!("cluster defaults: none set (built-in defaults apply)");
    } else {
        println!(
            "cluster defaults v{} (by {}, {}): {}",
            reply.version, reply.updated_by, reply.updated_at, reply.cluster_json
        );
    }
    if !reply.policies.is_empty() {
        println!(
            "namespace policies with autoscale for {}: {}",
            reply.namespace,
            reply.policies.join(", ")
        );
    }
    if !reply.error.is_empty() {
        println!(
            "effective defaults for {}: UNRESOLVABLE: {}",
            reply.namespace, reply.error
        );
        return Ok(());
    }
    println!("effective defaults for namespace {}:", reply.namespace);
    print_table(
        reply
            .effective
            .iter()
            .map(|row| FieldRow {
                field: row.field.clone(),
                value: row.value.clone(),
                from: row.origin.clone(),
            })
            .collect(),
    );
    Ok(())
}

#[derive(Tabled)]
struct ChangeRow {
    #[tabled(rename = "TARGET")]
    target: String,
    #[tabled(rename = "CHANGE")]
    change: String,
}

async fn defaults_apply(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let path = Path::new(matches.get_one::<String>("file").expect("required"));
    let text = crate::config_lang::eval_file(path).map_err(|err| {
        output::fail_local(
            format,
            CliError::local(
                "invalid_scaling_defaults_file",
                format!("failed to evaluate {}: {err:#}", path.display()),
                None,
            ),
        )
    })?;
    // Validated here too, so a typo fails before any network call.
    let parsed = serde_json::from_str::<crate::autoscaler::AutoscaleRecord>(&text)
        .map_err(|err| err.to_string())
        .and_then(|record| record.validate_inherited("cluster defaults"));
    if let Err(err) = parsed {
        return Err(output::fail_local(
            format,
            CliError::local(
                "invalid_scaling_defaults",
                format!("{}: {err}", path.display()),
                Some("see docs/autoscaling.md, \"Overriding the defaults\""),
            ),
        ));
    }
    let mut client = connect_manager_client().await?;
    let reply = output::resolve(
        format,
        "failed to apply scaling defaults",
        client
            .apply_scaling_defaults(ApplyScalingDefaultsRequest {
                defaults_json: text,
                dry_run: matches.get_flag("dry-run"),
                force: matches.get_flag("force"),
                idempotency_key: opt(matches, "idempotency-key"),
            })
            .await,
    )?
    .into_inner();
    if format.is_json() {
        output::print_success(&reply);
        return Ok(());
    }
    println!("{}", reply.message);
    if !reply.changes.is_empty() {
        print_table(
            reply
                .changes
                .iter()
                .map(|change| ChangeRow {
                    target: change.target.clone(),
                    change: if change.error.is_empty() {
                        if change.fields.is_empty() {
                            "changes".to_string()
                        } else {
                            change.fields.join(", ")
                        }
                    } else {
                        format!("INVALID: {}", change.error)
                    },
                })
                .collect(),
        );
    }
    Ok(())
}
