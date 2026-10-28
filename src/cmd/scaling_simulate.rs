//! `legion scaling simulate`: run a policy over recent history or a CSV
//! series and show the decisions it makes (docs/autoscaling.md,
//! "Simulating a policy"; plan §6.6 "Dry-run against history").
//!
//! ```console
//! $ legion scaling simulate web --since 1h          # the live policy over the last hour
//! $ legion scaling simulate web.ncl --since 6h      # an edited jobspec over the same hour
//! $ legion scaling simulate web.ncl --series load.csv
//! $ legion scaling simulate render-farm.ncl --series queue.csv   # a standalone policy
//! ```
//!
//! Nothing is written. Over history (the status-lane leader's hour of
//! 10-second points, `GetScalingInputs`), the replay feeds the policy what
//! the live engine read: the target's count, its healthy replicas through
//! the same warmup rule, and the `legion` source's readings averaged over
//! each check's window. Its own decisions count against the step limits at
//! the moment it makes them, and every other change of the count (a
//! person's scale, a pin, the live autoscaler's writes) when it is seen, as
//! the live engine does. So the same policy over the same hour makes the
//! decisions the live engine made. Checks on other sources have no history
//! here: they read as missing, unless `--series` supplies them.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use serde::Serialize;

use crate::autoscaler::simulate::{history_rows, read_csv, simulate, HistorySample, SimOptions, SimRow};
use crate::autoscaler::{Decision, DecisionKind, EffectivePolicy};
use crate::cmd::helpers::{
    active_namespace, connect_manager_client, json_flag, namespace_flag, print_table,
    AuthedManagerClient,
};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    ApplyScalingPolicyRequest, GetScalingInputsRequest, GetScalingStatusRequest, PlanJobRequest,
    ScalingInputsPoint, ScalingTargetSelector,
};
use tabled::Tabled;

/// The most history the manager keeps.
pub const HISTORY_MAX: Duration = Duration::from_secs(3600);

/// What to simulate, as the CLI and the MCP tool take it.
#[derive(Debug, Clone, Default)]
pub struct SimulateArgs {
    /// A running job (`web`, `payments/api`), whose effective policy is used
    /// unless `spec_json` gives another.
    pub job: Option<String>,
    /// A jobspec (as JSON) whose autoscaled task's policy to simulate
    /// instead, or a standalone policy (as JSON).
    pub spec_json: Option<String>,
    /// The namespace for a bare job name or a spec that names none.
    pub namespace: Option<String>,
    /// Narrow to one task group.
    pub task_group: Option<String>,
    /// Narrow to one task.
    pub task_name: Option<String>,
    /// How much history to replay (at most an hour).
    pub since: Duration,
    /// A CSV series instead of history.
    pub series_csv: Option<String>,
    /// Follow the policy's own count instead of replaying the real one
    /// (over history; a series always follows).
    pub follow: bool,
    /// Start the simulated engine cold (the scale-in hold of a restart).
    pub cold: bool,
    /// The starting count in follow mode.
    pub start: Option<u32>,
}

/// One decision that changed (or would change) the count, or every one.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SimulatedDecision {
    /// When, unix ms.
    pub at_unix_ms: i64,
    /// The count before.
    pub from: u32,
    /// The count after.
    pub to: u32,
    /// `applied`, `would`, or `held`.
    pub kind: String,
    /// The one-line why.
    pub summary: String,
}

/// A simulation's result.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SimulationReport {
    /// What was simulated (`web/default/web`, or a standalone policy's id).
    pub target: String,
    /// The effective policy's digest.
    pub policy_digest: String,
    /// `history` or `series`.
    pub input: String,
    /// `replay` (the real count) or `follow` (the policy's own).
    pub mode: String,
    /// Samples evaluated.
    pub evaluations: usize,
    /// The first and last sample, unix ms.
    pub from_unix_ms: i64,
    pub to_unix_ms: i64,
    /// Checks the input has no readings for (they read as missing).
    pub missing_checks: Vec<String>,
    /// Every decision that changed the count (or would have).
    pub changes: Vec<SimulatedDecision>,
    /// With `all`: every evaluation, held ones included.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub decisions: Vec<Decision>,
}

/// `legion scaling simulate`.
pub fn simulate_command() -> Command {
    Command::new("simulate")
        .about("Run a policy over the last hour of history, or a CSV series, and show the decisions it makes")
        .long_about(
            "TARGET is a running job (its live policy), a jobspec file (the policy you are \
             about to submit), or a standalone policy file. Over history the replay sees what \
             the live autoscaler saw, so the live policy makes the decisions it made; an edited \
             policy shows what it would have done instead. Nothing is written.",
        )
        .arg(
            Arg::new("target")
                .required(true)
                .value_name("JOB | FILE")
                .help("A job name, a jobspec (.ncl/.json), or a standalone policy file"),
        )
        .arg(namespace_flag())
        .arg(Arg::new("group").long("group").value_name("GROUP").help("Task group, when the job has more than one autoscaled task"))
        .arg(Arg::new("task").long("task").value_name("TASK").help("Task name, when the job has more than one autoscaled task"))
        .arg(
            Arg::new("since")
                .long("since")
                .value_name("DURATION")
                .default_value("1h")
                .conflicts_with("series")
                .help("How much history to replay (the manager keeps one hour)"),
        )
        .arg(
            Arg::new("series")
                .long("series")
                .value_name("CSV")
                .value_hint(ValueHint::FilePath)
                .help("Replay a CSV series instead: time, ready, total, and one column per check"),
        )
        .arg(
            Arg::new("follow")
                .long("follow")
                .action(ArgAction::SetTrue)
                .help("Move the count with the policy's own decisions (the default for a file and for --series)"),
        )
        .arg(
            Arg::new("replay")
                .long("replay")
                .action(ArgAction::SetTrue)
                .conflicts_with_all(["follow", "series"])
                .help("Replay the count the target really had (the default for a job): the live policy then makes the decisions the live autoscaler made"),
        )
        .arg(
            Arg::new("cold")
                .long("cold")
                .action(ArgAction::SetTrue)
                .help("Start the simulated engine cold: no scale-in for one down.stabilize, as after a restart"),
        )
        .arg(
            Arg::new("start")
                .long("start")
                .value_name("N")
                .value_parser(clap::value_parser!(u32))
                .help("The starting count when following"),
        )
        .arg(
            Arg::new("all")
                .long("all")
                .action(ArgAction::SetTrue)
                .help("Show every evaluation, not only the changes"),
        )
        .arg(json_flag())
}

fn local_error(format: OutputFormat, message: impl Into<String>, hint: Option<&str>) -> anyhow::Error {
    output::fail_local(format, CliError::local("InvalidArgument", message, hint))
}

/// `legion scaling simulate`.
pub async fn handle_simulate(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let target = matches.get_one::<String>("target").expect("required").clone();
    let path = Path::new(&target);
    let spec_json = if path.is_file() {
        Some(crate::config_lang::eval_file(path).map_err(|err| {
            local_error(format, format!("failed to evaluate {}: {err:#}", path.display()), None)
        })?)
    } else {
        None
    };
    let since = humantime::parse_duration(
        matches
            .get_one::<String>("since")
            .map(String::as_str)
            .unwrap_or("1h")
            .trim(),
    )
    .map_err(|err| local_error(format, format!("--since: {err}"), Some("write it like 30m")))?;
    let series_csv = match matches.get_one::<String>("series") {
        Some(file) => Some(std::fs::read_to_string(file).map_err(|err| {
            local_error(format, format!("cannot read {file}: {err}"), None)
        })?),
        None => None,
    };
    // A job replays its own history by default; a file (an edited policy)
    // follows its own decisions.
    let follow = if matches.get_flag("replay") {
        false
    } else {
        matches.get_flag("follow") || series_csv.is_some() || spec_json.is_some()
    };
    let args = SimulateArgs {
        job: spec_json.is_none().then(|| target.clone()),
        spec_json,
        namespace: active_namespace(matches),
        task_group: matches.get_one::<String>("group").cloned(),
        task_name: matches.get_one::<String>("task").cloned(),
        since,
        follow,
        series_csv,
        cold: matches.get_flag("cold"),
        start: matches.get_one::<u32>("start").copied(),
    };
    let mut client = connect_manager_client().await?;
    let mut report = match run(&mut client, &args).await {
        Ok(report) => report,
        Err(err) => {
            return Err(output::fail_local(
                format,
                CliError::local("FailedPrecondition", format!("{err:#}"), None),
            ))
        }
    };
    if !matches.get_flag("all") {
        report.decisions.clear();
    }
    if format.is_json() {
        output::print_success(&report);
        return Ok(());
    }
    print_report(&report, matches.get_flag("all"));
    Ok(())
}

#[derive(Tabled)]
struct ChangeRow {
    #[tabled(rename = "TIME")]
    time: String,
    #[tabled(rename = "CHANGE")]
    change: String,
    #[tabled(rename = "KIND")]
    kind: String,
    #[tabled(rename = "SUMMARY")]
    summary: String,
}

fn print_report(report: &SimulationReport, all: bool) {
    println!(
        "{}: {} {} evaluation(s) from {} to {} ({}), policy {}",
        report.target,
        report.mode,
        report.evaluations,
        crate::cmd::scaling::clock(report.from_unix_ms),
        crate::cmd::scaling::clock(report.to_unix_ms),
        report.input,
        report.policy_digest.chars().take(12).collect::<String>()
    );
    if !report.missing_checks.is_empty() {
        println!(
            "note: no readings for {} in this input; they read as missing data",
            report.missing_checks.join(", ")
        );
    }
    let rows: Vec<ChangeRow> = if all {
        report
            .decisions
            .iter()
            .map(|decision| ChangeRow {
                time: crate::cmd::scaling::clock(decision.at_ms),
                change: if decision.from == decision.to {
                    "-".to_string()
                } else {
                    format!("{} → {}", decision.from, decision.to)
                },
                kind: decision.kind.to_string(),
                summary: decision.summary.clone(),
            })
            .collect()
    } else {
        report
            .changes
            .iter()
            .map(|change| ChangeRow {
                time: crate::cmd::scaling::clock(change.at_unix_ms),
                change: format!("{} → {}", change.from, change.to),
                kind: change.kind.clone(),
                summary: change.summary.clone(),
            })
            .collect()
    };
    if rows.is_empty() {
        println!("no changes: the policy held the count throughout");
    } else {
        print_table(rows);
    }
}

/// The effective policy to simulate, and the target it belongs to:
/// `(label, policy, job selector)`. The selector is `None` for a standalone
/// policy, which has no history.
async fn resolve_policy(
    client: &mut AuthedManagerClient,
    args: &SimulateArgs,
) -> R<(String, EffectivePolicy, Option<ScalingTargetSelector>)> {
    let narrow = |value: &Option<String>| value.clone().filter(|v| !v.trim().is_empty());
    let parse = |json: &str| -> R<EffectivePolicy> {
        serde_json::from_str(json).context("the effective policy does not parse in this build")
    };
    if let Some(spec) = &args.spec_json {
        let value: Value = serde_json::from_str(spec).context("the file is not JSON")?;
        if value.get("target").is_some() {
            // A standalone policy: the manager resolves it without storing it.
            let reply = client
                .apply_scaling_policy(ApplyScalingPolicyRequest {
                    spec_json: spec.clone(),
                    namespace: args.namespace.clone().unwrap_or_default(),
                    dry_run: true,
                    idempotency_key: None,
                })
                .await
                .map_err(|status| anyhow!("resolving the policy: {}", status.message()))?
                .into_inner();
            return Ok((reply.id.clone(), parse(&reply.policy_json)?, None));
        }
        let reply = client
            .plan_job(PlanJobRequest {
                parsed_jobspec: spec.clone(),
                namespace: args.namespace.clone(),
                allow_unknown_fields: false,
            })
            .await
            .map_err(|status| anyhow!("planning the jobspec: {}", status.message()))?
            .into_inner();
        let matching: Vec<_> = reply
            .autoscale
            .iter()
            .filter(|plan| {
                let parts: Vec<&str> = plan.target.rsplitn(3, '/').collect();
                let (task, group) = (parts.first().copied(), parts.get(1).copied());
                narrow(&args.task_group).is_none_or(|g| group == Some(g.as_str()))
                    && narrow(&args.task_name).is_none_or(|t| task == Some(t.as_str()))
            })
            .collect();
        let plan = match matching.as_slice() {
            [] => bail!("the jobspec has no autoscaled task{}", if args.task_group.is_some() || args.task_name.is_some() { " matching --group/--task" } else { "" }),
            [plan] => *plan,
            many => bail!(
                "the jobspec has {} autoscaled tasks ({}); pick one with --group/--task",
                many.len(),
                many.iter().map(|p| p.target.as_str()).collect::<Vec<_>>().join(", ")
            ),
        };
        let parts: Vec<&str> = plan.target.rsplitn(3, '/').collect();
        let (task, group, job) = (parts[0], parts[1], parts[2]);
        return Ok((
            plan.target.clone(),
            parse(&plan.policy_json)?,
            Some(ScalingTargetSelector {
                job: job.to_string(),
                task_group: Some(group.to_string()),
                task_name: Some(task.to_string()),
            }),
        ));
    }
    let job = args.job.clone().context("name a job or a file")?;
    let job = match (&args.namespace, job.contains('/')) {
        (Some(namespace), false) => crate::structs::namespace::job_id(namespace, &job),
        _ => job,
    };
    let status = client
        .get_scaling_status(GetScalingStatusRequest {
            job: job.clone(),
            task_group: narrow(&args.task_group),
            task_name: narrow(&args.task_name),
            detail: true,
            ..Default::default()
        })
        .await
        .map_err(|status| anyhow!("reading the job's policy: {}", status.message()))?
        .into_inner();
    let target = match status.targets.as_slice() {
        [] => bail!("{job} has no autoscaled task (legion scaling status lists them)"),
        [target] => target.clone(),
        many => bail!(
            "{job} has {} autoscaled tasks; pick one with --group/--task",
            many.len()
        ),
    };
    if target.policy_json.is_empty() {
        bail!(
            "{}/{}/{}'s policy does not resolve: {}",
            target.job_id,
            target.task_group,
            target.task_name,
            target.error
        );
    }
    Ok((
        format!("{}/{}/{}", target.job_id, target.task_group, target.task_name),
        parse(&target.policy_json)?,
        Some(ScalingTargetSelector {
            job: target.job_id.clone(),
            task_group: Some(target.task_group.clone()),
            task_name: Some(target.task_name.clone()),
        }),
    ))
}

/// The manager's history points, as the simulator's samples.
pub fn samples(points: &[ScalingInputsPoint]) -> Vec<HistorySample> {
    points
        .iter()
        .map(|point| HistorySample {
            at_ms: point.at_unix_ms,
            desired: point.desired_replicas,
            healthy: point.ready_replicas,
            unplaced: point.unplaced_replicas,
            measured: point.measured_replicas,
            cpu_utilization: point.cpu_utilization,
            memory_utilization: point.memory_utilization,
            requests_per_second: point.requests_per_second,
            errors_per_second: point.errors_per_second,
            latency_ms_mean: point.latency_ms_mean,
        })
        .collect()
}

/// Run a simulation (the CLI and `legion_scaling_simulate`).
pub async fn run(client: &mut AuthedManagerClient, args: &SimulateArgs) -> R<SimulationReport> {
    let (label, policy, selector) = resolve_policy(client, args).await?;
    let (rows, input): (Vec<SimRow>, &str) = match &args.series_csv {
        Some(csv) => (read_csv(csv.as_bytes()).map_err(|err| anyhow!("the series: {err}"))?, "series"),
        None => {
            let Some(selector) = selector else {
                bail!(
                    "{label} is a standalone policy: its sources have no history in Legion. \
                     Hint: pass --series with a CSV of their readings"
                );
            };
            let seconds = args.since.min(HISTORY_MAX).as_secs().max(10) as u32;
            let reply = client
                .get_scaling_inputs(GetScalingInputsRequest {
                    targets: vec![selector],
                    namespaces: Vec::new(),
                    history_seconds: seconds,
                })
                .await
                .map_err(|status| anyhow!("reading history: {}", status.message()))?
                .into_inner();
            let input = reply
                .inputs
                .into_iter()
                .next()
                .context("the manager returned no inputs")?;
            if !input.error.is_empty() {
                bail!("reading history: {}", input.error);
            }
            (history_rows(&policy, &samples(&input.history)), "history")
        }
    };
    let Some(first) = rows.first() else {
        bail!(
            "no samples to simulate over: {}",
            if input == "history" {
                "the manager holds no history for this task yet (it keeps an hour, from when it \
                 took the status lane)"
            } else {
                "the series is empty"
            }
        );
    };
    let replay = input == "history" && !args.follow;
    let started_at_ms = if args.cold {
        first.at_ms
    } else {
        // A warm engine: one down.stabilize of samples behind it.
        first.at_ms - i64::try_from(policy.down.stabilize.as_millis()).unwrap_or(0) - 1
    };
    let options = SimOptions {
        start_count: args.start,
        follow: !replay,
        started_at_ms: Some(started_at_ms),
        window_readings: input == "history",
        conserve_load: input == "history" && !replay,
    };
    let decisions = simulate(&policy, &rows, &options).map_err(|err| anyhow!(err))?;
    let mut missing: BTreeMap<String, ()> = BTreeMap::new();
    for check in policy.all_checks() {
        if check.strategy.needs_reading() || !check.query.is_empty() {
            let any = rows.iter().any(|row| {
                matches!(
                    row.readings.get(&check.name),
                    Some(crate::autoscaler::Reading::Value(_))
                )
            });
            if !any {
                missing.insert(check.name.clone(), ());
            }
        }
    }
    Ok(SimulationReport {
        target: label,
        policy_digest: policy.digest(),
        input: input.to_string(),
        mode: if replay { "replay" } else { "follow" }.to_string(),
        evaluations: decisions.len(),
        from_unix_ms: rows.first().map_or(0, |row| row.at_ms),
        to_unix_ms: rows.last().map_or(0, |row| row.at_ms),
        missing_checks: missing.into_keys().collect(),
        changes: decisions
            .iter()
            .filter(|decision| decision.kind != DecisionKind::Held)
            .map(|decision| SimulatedDecision {
                at_unix_ms: decision.at_ms,
                from: decision.from,
                to: decision.to,
                kind: decision.kind.to_string(),
                summary: decision.summary.clone(),
            })
            .collect(),
        decisions,
    })
}
