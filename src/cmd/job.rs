use std::path::{Path, PathBuf};
use std::vec;

use clap::value_parser;

use crate::cmd::helpers::{connect_manager_client, friendly_status, json_flag, print_table};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::{
    AdvanceRolloutRequest, DestroyJobRequest, GetAllocationLogsRequest, GetJobInfoRequest,
    GetJobScaleStatusRequest, GetJobSpecRequest, GetKvRequest, GetRolloutStatusRequest, JobConfig,
    JobPlanPrint, JobPrint, KvEntry, KvValueType, ListJobsRequest, ListRolloutEventsRequest,
    ListRolloutsRequest, PlanJobRequest, RunJobRequest, ScaleTaskRequest, StopJobRequest,
    Submission,
};
use crate::task_plugins::{builtin_task_plugin_schemas, validate_job_config};
use serde::Serialize;
use tabled::Tabled;
use tempfile::TempDir;
use url::Url;

pub fn config() -> Command {
    let run_cmd = Command::new("run")
        .about("Run a job")
        .arg(
            Arg::new("path")
                .value_name("PATH")
                .help("Path or URL to the job specification to run (.ncl Nickel, or .json)")
                .value_hint(ValueHint::AnyPath),
        )
        .arg(priority_arg())
        .arg(asap_flag())
        .arg(
            Arg::new("force-reschedule")
                .long("force-reschedule")
                .help("Force a reschedule even if the job spec is unchanged")
                .action(ArgAction::SetTrue),
        )
        .arg(idempotency_arg())
        .arg(wait_flag())
        .arg(timeout_arg())
        .arg(json_flag())
        .args(config_var_args());

    let stop_cmd = Command::new("stop")
        .about("Stop a job")
        .arg(
            Arg::new("name")
                .value_name("NAME")
                .help("Name of the job to stop"),
        )
        .arg(priority_arg())
        .arg(asap_flag())
        .arg(idempotency_arg())
        .arg(json_flag());

    let scale_cmd = Command::new("scale")
        .about("Change a task's count without a new job version or rollout (docs/autoscaling.md)")
        .arg(
            Arg::new("name")
                .value_name("NAME")
                .required(true)
                .help("Name of the job to scale"),
        )
        .arg(
            Arg::new("count")
                .long("count")
                .value_name("COUNT")
                .value_parser(value_parser!(u32))
                .help("Desired replica count; omit to print the job's scale status"),
        )
        .arg(
            Arg::new("reset")
                .long("reset")
                .action(ArgAction::SetTrue)
                .conflicts_with("count")
                .help("Drop the override and return count ownership to the job spec"),
        )
        .arg(
            Arg::new("group")
                .long("group")
                .value_name("GROUP")
                .help("Task group, when the job has more than one scalable task"),
        )
        .arg(
            Arg::new("task")
                .long("task")
                .value_name("TASK")
                .help("Task name, when the job has more than one scalable task"),
        )
        .arg(
            Arg::new("reason")
                .long("reason")
                .value_name("REASON")
                .help("Audit note recorded on the override and in the event stream"),
        )
        .arg(idempotency_arg())
        .arg(json_flag());

    let destroy_cmd = Command::new("destroy")
        .about("Destroy a job")
        .arg(
            Arg::new("name")
                .value_name("NAME")
                .help("Name of the job to remove"),
        )
        .arg(priority_arg())
        .arg(asap_flag())
        .arg(idempotency_arg())
        .arg(json_flag());

    let validate_cmd = Command::new("validate")
        .about("Render and validate a jobspec locally without submitting it")
        .arg(
            Arg::new("path")
                .value_name("PATH")
                .help("Path or URL to the job specification to validate (.ncl Nickel, or .json)")
                .required(true)
                .value_hint(ValueHint::AnyPath),
        )
        .arg(json_flag())
        .args(config_var_args());

    let schema_cmd = Command::new("schema")
        .about("Emit the JSON Schema for jobspecs and built-in task runtimes")
        .arg(json_flag());

    let wait_cmd = Command::new("wait")
        .about("Block until a job's active rollout converges or times out")
        .arg(
            Arg::new("name")
                .value_name("NAME")
                .help("Name of the job to wait on")
                .required(true),
        )
        .arg(timeout_arg())
        .arg(json_flag());

    let info_cmd = Command::new("info")
        .about("Get job info")
        .arg(
            Arg::new("name")
                .value_name("NAME")
                .help("Name of the job to inspect")
                .required(true),
        )
        .arg(json_flag());

    let plan_cmd = Command::new("plan")
        .about("Preview which nodes allocations would land on")
        .arg(
            Arg::new("path")
                .value_name("PATH")
                .help("Path or URL to the job specification to plan (.ncl Nickel, or .json)")
                .required(true)
                .value_hint(ValueHint::AnyPath),
        )
        .arg(json_flag())
        .args(config_var_args());

    let spec_cmd = Command::new("spec")
        .about("Get job spec used to define the job")
        .arg(
            Arg::new("name")
                .value_name("NAME")
                .help("Name of the job to fetch the spec for"),
        )
        .arg(json_flag());

    let list_cmd = Command::new("list")
        .about("List all jobs")
        .arg(
            Arg::new("status")
                .long("status")
                .value_name("STATUS")
                .help("Only jobs with this exact status: running, pending, failed, scheduled, complete, suspended, idle, or stopped"),
        )
        .arg(json_flag());

    let logs_cmd = Command::new("logs")
        .about("Fetch captured stdout/stderr for a job's allocations")
        .arg(
            Arg::new("name")
                .value_name("NAME")
                .help("Name of the job whose allocation logs to fetch"),
        )
        .arg(
            Arg::new("allocation")
                .long("allocation")
                .value_name("ID")
                .help("Fetch logs for a single allocation id instead of a whole job")
                .value_parser(value_parser!(i64)),
        )
        .arg(
            Arg::new("stream")
                .long("stream")
                .value_name("STREAM")
                .help("Which stream to read: stdout, stderr, or all")
                .value_parser(["stdout", "stderr", "all"])
                .default_value("all"),
        )
        .arg(
            Arg::new("max-bytes")
                .long("max-bytes")
                .value_name("N")
                .help("Maximum bytes to return per stream (0 = worker default cap)")
                .value_parser(value_parser!(i64)),
        )
        .arg(
            Arg::new("tail")
                .long("tail")
                .help("Return the trailing bytes of each stream instead of the leading bytes")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("follow")
                .long("follow")
                .short('f')
                .help("Stream new log output until interrupted (incompatible with --json)")
                .action(ArgAction::SetTrue),
        )
        .arg(json_flag());

    let rollout_list_cmd = Command::new("list")
        .about("List rollouts")
        .arg(
            Arg::new("job")
                .long("job")
                .value_name("JOB")
                .help("Filter rollouts by job id"),
        )
        .arg(
            Arg::new("active")
                .long("active")
                .help("Show only active rollouts")
                .action(ArgAction::SetTrue),
        )
        .arg(json_flag());

    let rollout_advance_cmd = Command::new("advance")
        .about("Manually advance a rollout")
        .arg(
            Arg::new("id")
                .long("id")
                .value_name("ID")
                .help("Rollout id")
                .required(true)
                .value_parser(value_parser!(i64)),
        )
        .arg(
            Arg::new("group")
                .long("group")
                .value_name("GROUP")
                .help("Advance only specific task group(s); repeatable")
                .action(ArgAction::Append),
        )
        .arg(priority_arg())
        .arg(asap_flag())
        .arg(json_flag());

    let rollout_status_cmd = Command::new("status")
        .about("Inspect rollout progress and allocations")
        .arg(
            Arg::new("id")
                .long("id")
                .value_name("ID")
                .help("Rollout id")
                .required(true)
                .value_parser(value_parser!(i64)),
        )
        .arg(json_flag());

    let rollout_events_cmd = Command::new("events")
        .about("Show rollout event history")
        .arg(
            Arg::new("id")
                .long("id")
                .value_name("ID")
                .help("Rollout id")
                .required(true)
                .value_parser(value_parser!(i64)),
        )
        .arg(
            Arg::new("limit")
                .long("limit")
                .value_name("N")
                .help("Maximum number of events to return (default 200)")
                .value_parser(value_parser!(u32)),
        )
        .arg(json_flag());

    let rollout_cmd = Command::new("rollout")
        .about("Inspect rollouts")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(rollout_list_cmd)
        .subcommand(rollout_advance_cmd)
        .subcommand(rollout_status_cmd)
        .subcommand(rollout_events_cmd);

    Command::new("job")
        .about("Interact with jobs")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(run_cmd)
        .subcommand(info_cmd)
        .subcommand(plan_cmd)
        .subcommand(validate_cmd)
        .subcommand(schema_cmd)
        .subcommand(wait_cmd)
        .subcommand(list_cmd)
        .subcommand(stop_cmd)
        .subcommand(scale_cmd)
        .subcommand(spec_cmd)
        .subcommand(destroy_cmd)
        .subcommand(logs_cmd)
        .subcommand(rollout_cmd)
}

/// Optional idempotency key for mutating job operations. When supplied, the
/// manager deduplicates retried requests carrying the same key, so an agent
/// can safely retry on timeout without risking a double-apply.
pub(crate) fn idempotency_arg() -> Arg {
    Arg::new("idempotency-key")
        .long("idempotency-key")
        .value_name("KEY")
        .help("Deduplicate retries of this mutating request (safe for agent retries)")
}

/// Flag requesting that `job run` block until the resulting rollout converges.
pub(crate) fn wait_flag() -> Arg {
    Arg::new("wait")
        .long("wait")
        .help("Block until the job's rollout converges to a terminal state")
        .action(ArgAction::SetTrue)
}

/// Timeout (seconds) for convergence waits.
pub(crate) fn timeout_arg() -> Arg {
    Arg::new("timeout")
        .long("timeout")
        .value_name("SECONDS")
        .help("Maximum seconds to wait for convergence (default 120)")
        .value_parser(value_parser!(u64))
}

/// Default convergence-wait timeout when `--timeout` is not supplied.
const DEFAULT_WAIT_SECS: u64 = 120;

pub(crate) fn wait_timeout(matches: &ArgMatches) -> std::time::Duration {
    let secs = matches
        .get_one::<u64>("timeout")
        .copied()
        .unwrap_or(DEFAULT_WAIT_SECS);
    std::time::Duration::from_secs(secs)
}

/// Outcome of waiting for a job's rollout to converge.
///
/// `wait` tracks the job's most recent rollout (the canonical signal for
/// whether an update has finished applying). A fresh job with no prior version
/// has no rollout to track, in which case `status` is `no_rollout`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WaitResult {
    /// True when the rollout reached a settled state (terminal or paused), or
    /// there was no rollout to wait on.
    pub converged: bool,
    /// True only when the rollout completed successfully (or there was nothing
    /// to wait on).
    pub success: bool,
    /// Final rollout status, or `no_rollout` / `timeout`.
    pub status: String,
    /// Rollout id that was tracked, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollout_id: Option<i64>,
    /// Seconds spent waiting.
    pub waited_secs: u64,
}

impl WaitResult {
    fn human_summary(&self) -> String {
        match (&self.rollout_id, self.converged) {
            (Some(id), true) => format!(
                "Rollout {id} converged: {} (waited {}s)",
                self.status, self.waited_secs
            ),
            (Some(id), false) => format!(
                "Timed out after {}s waiting for rollout {id} (status={})",
                self.waited_secs, self.status
            ),
            (None, _) => format!("No rollout to wait on (status={})", self.status),
        }
    }
}

/// Rollout statuses past which no further automatic progress will happen.
fn is_settled_rollout_status(status: &str) -> bool {
    matches!(
        status,
        "completed" | "failed" | "cancelled" | "completed_incomplete" | "paused"
    )
}

/// Poll the job's most recent rollout until it settles or the timeout elapses.
pub(crate) async fn wait_for_convergence(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    job_name: &str,
    timeout: std::time::Duration,
    format: OutputFormat,
) -> R<WaitResult> {
    let start = std::time::Instant::now();

    loop {
        // Find the most recent rollout for this job.
        let list = output::resolve(
            format,
            "failed to list rollouts while waiting",
            client
                .list_rollouts(tonic::Request::new(ListRolloutsRequest {
                    job_id: Some(job_name.to_string()),
                    active_only: false,
                    ..Default::default()
                }))
                .await,
        )?
        .into_inner();

        let current = list.rollouts.into_iter().next();

        // No rollout exists to track (e.g. a fresh job with no prior version).
        let Some(rollout) = current else {
            return Ok(WaitResult {
                converged: true,
                success: true,
                status: "no_rollout".to_string(),
                rollout_id: None,
                waited_secs: start.elapsed().as_secs(),
            });
        };

        // Settled: terminal or paused (no further automatic progress).
        if is_settled_rollout_status(&rollout.status) {
            return Ok(WaitResult {
                converged: true,
                success: rollout.status == "completed",
                status: rollout.status,
                rollout_id: Some(rollout.id),
                waited_secs: start.elapsed().as_secs(),
            });
        }

        // Still in progress: give up once the deadline passes.
        if start.elapsed() >= timeout {
            return Ok(WaitResult {
                converged: false,
                success: false,
                status: rollout.status,
                rollout_id: Some(rollout.id),
                waited_secs: start.elapsed().as_secs(),
            });
        }

        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
}

#[instrument(err, skip_all)]
async fn wait_job(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let job_name = arg_matches
        .get_one::<String>("name")
        .context("missing job name")?;
    let timeout = wait_timeout(arg_matches);
    let mut client = connect_manager_client().await?;

    let result = wait_for_convergence(&mut client, job_name, timeout, format).await?;

    if format.is_json() {
        output::print_success(&result);
    } else {
        println!("{}", result.human_summary());
    }

    // A non-converged wait is a failure for scripting/agent purposes.
    if !result.converged {
        std::process::exit(1);
    }
    Ok(())
}

#[instrument(err, skip_all)]
async fn validate_job(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let path = arg_matches
        .get_one::<String>("path")
        .context("missing jobspec path")?;

    // Resolve configuration vars. KV-backed vars require the manager; plain
    // vars do not, so validation stays usable offline for the common case.
    let vars = config_vars_from_matches(arg_matches)?;
    let vars = if kv_keys_from_vars(&vars).is_empty() {
        vars
    } else {
        let mut client = connect_manager_client().await?;
        resolve_kv_config_vars_from_manager(&mut client, vars).await?
    };

    let resolved = resolve_jobspec_source_async(path).await?;
    let (rendered, _submissions) = render_jobspec(&resolved.path, &vars)?;

    // Parse the rendered jobspec into a JobConfig.
    let config: JobConfig = match serde_json::from_str(&rendered) {
        Ok(config) => config,
        Err(err) => {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "InvalidArgument",
                    format!("jobspec is not a valid job configuration: {err}"),
                    Some("check field names and types against `legion job schema`"),
                ),
            ));
        }
    };

    // Run the same deep validation the manager applies on submit.
    if let Err(err) = validate_job_config(&config) {
        // `{err:#}` so the whole context chain survives. Plain `{err}`
        // prints only the outermost frame — "task web config invalid" —
        // and drops the runtime's actual reason, which is the only part
        // that tells the user what to change.
        let text = format!("jobspec failed validation: {err:#}");
        let (message, hint) = crate::cmd::helpers::split_hint(&text);
        return Err(output::fail_local(
            format,
            CliError::local("InvalidArgument", message, hint),
        ));
    }

    let task_count = config.tasks.len();
    let groups: std::collections::BTreeSet<&str> = config
        .tasks
        .iter()
        .map(|task| task.group.as_str())
        .collect();

    // Edge boot gate downgrades (docs/edge-boot.md). A task whose declared
    // `speculativeStart.mode` cannot be honored is capped at `stage` — never
    // silently, because the symptom otherwise is a speedup that quietly did
    // not happen.
    let mut warnings = speculative_start_warnings(&config);
    // Valid settings that will not do what the author most likely expects
    // (an idle signal a microVM cannot see, an identity refresh that reboots
    // the guest). `job run` prints the same lines.
    warnings.extend(crate::structs::suspend::sandbox_submit_warnings(&config));

    if format.is_json() {
        output::print_success(&json!({
            "valid": true,
            "job": config.name,
            "tasks": task_count,
            "groups": groups.iter().collect::<Vec<_>>(),
            "warnings": warnings,
        }));
    } else {
        println!(
            "Jobspec '{}' is valid: {} task(s) across {} group(s)",
            config.name,
            task_count,
            groups.len()
        );
        for warning in &warnings {
            println!("warning: {warning}");
        }
    }

    Ok(())
}

/// Gate downgrades this jobspec would take at admission, as human-readable
/// lines. Empty for every job that did not declare `speculativeStart`, and for
/// every one whose declared gate is honored as written.
fn speculative_start_warnings(config: &JobConfig) -> Vec<String> {
    config
        .tasks
        .iter()
        .filter_map(|task| {
            let declared = task.speculative_start.as_ref()?.resolve().ok()?.mode;
            let (effective, reason) = crate::manager::dispatcher::effective_gate(task, declared);
            (effective != declared).then(|| {
                format!(
                    "speculativeStart.mode `{declared}` is downgraded to `{effective}`: {}",
                    reason.unwrap_or_else(|| "an invariant does not permit it".into())
                )
            })
        })
        .collect()
}

#[instrument(err, skip_all)]
async fn job_schema(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let schema = build_jobspec_schema();

    if format.is_json() {
        output::print_success(&schema);
    } else {
        println!("{}", serde_json::to_string_pretty(&schema)?);
    }
    Ok(())
}

/// Assemble the jobspec JSON Schema: a documented top-level envelope plus the
/// live, per-runtime task config schemas derived from the built-in plugins.
pub(crate) fn build_jobspec_schema() -> Value {
    let mut task_runtimes = serde_json::Map::new();
    for plugin in builtin_task_plugin_schemas() {
        task_runtimes.insert(plugin.name, plugin.schema);
    }

    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Legion Jobspec",
        "description": "A Legion job specification (the JSON a Nickel jobspec renders to). \
            A job has a name and one or more tasks (use `task` for a single task or `tasks` for many). \
            Each task's `config` object is validated against the schema for its `runtime` under `taskRuntimes`.",
        "type": "object",
        "required": ["name"],
        "properties": {
            "name": { "type": "string", "description": "Unique job name." },
            "task": { "$ref": "#/$defs/task", "description": "A single task (shorthand for tasks: [task])." },
            "tasks": { "type": "array", "items": { "$ref": "#/$defs/task" }, "description": "List of tasks." },
            "version": { "type": "integer", "description": "Optional jobspec version." },
            "priority": {
                "type": "string",
                "enum": ["critical", "high", "elevated", "normal", "low", "background"],
                "default": "normal",
                "description": "Priority band, most important first. Orders the eval queue AND governs preemption: an allocation is only ever displaced by a job in a STRICTLY higher band, never a peer. Choosing a band below `normal` is the consent to be displaced. Claiming a band may require an ACL `bands` grant. See docs/preemption-and-eviction.md."
            },
            "rollout": { "$ref": "#/$defs/rollout", "description": "Default rollout strategy for the job." },
            "rolloutByGroup": {
                "type": "object",
                "additionalProperties": { "$ref": "#/$defs/rollout" },
                "description": "Per-group rollout overrides keyed by group name."
            }
        },
        "$defs": {
            "task": {
                "type": "object",
                "required": ["runtime"],
                "properties": {
                    "name": { "type": "string", "description": "Task name (unique within the job)." },
                    "runtime": { "type": "string", "description": "Task runtime plugin that runs this task (e.g. exec, container, noop)." },
                    "group": { "type": "string", "description": "Task group; tasks in a group are placed together." },
                    "lifecycle": { "type": "string", "enum": ["service", "batch", "sidecar"], "description": "Task lifecycle." },
                    "count": {
                        "description": "Number of instances, or {num, per} for spread placement.",
                        "oneOf": [
                            { "type": "integer", "minimum": 0 },
                            { "type": "object", "required": ["num", "per"], "properties": {
                                "num": { "type": "integer" },
                                "per": { "type": "string" }
                            }}
                        ]
                    },
                    "config": { "type": "object", "description": "Runtime-specific config; validated against taskRuntimes[runtime]." },
                    "dependsOn": {
                        "type": "array",
                        "items": { "type": "object", "required": ["task"], "properties": {
                            "task": { "type": "string" },
                            "condition": { "type": "string", "enum": ["running", "healthy", "complete", "success"] }
                        }},
                        "description": "Task dependencies that gate start."
                    },
                    "schedule": { "description": "Batch-only schedule (cron string or {every: duration})." },
                    "stop": {
                        "type": "object",
                        "description": "How the platform terminates this task, for every cause (job stop, rollout replacement, drain, reschedule, scale-in, preemption). See docs/graceful-stop.md.",
                        "properties": {
                            "signal": {
                                "type": "string",
                                "enum": ["SIGTERM", "SIGINT", "SIGQUIT", "SIGHUP", "SIGUSR1", "SIGUSR2"],
                                "default": "SIGTERM",
                                "description": "Notice signal, sent to the task's process group. SIGKILL is deliberately not offered: it is the end of the ladder, not a notice."
                            },
                            "killAfter": {
                                "type": "string",
                                "default": "30s",
                                "description": "Grace between the signal and SIGKILL, e.g. \"30s\". Capped at 5m — grace may slow a drain, never wedge one."
                            }
                        }
                    },
                    "disruption": {
                        "type": "object",
                        "description": "Floor on how many of this task's allocations VOLUNTARY disruption (preemption) may leave running. Does not bind drain, node loss or job stop. See docs/preemption-and-eviction.md.",
                        "properties": {
                            "minAvailable": {
                                "description": "Absolute count (2) or percent of count (\"50%\", rounded up). Default 0.",
                                "oneOf": [
                                    { "type": "integer", "minimum": 0 },
                                    { "type": "string" }
                                ]
                            }
                        }
                    },
                    "speculativeStart": {
                        "type": "object",
                        "description": "How far a node may get on a start for this task BEFORE the placement is durable. Absent = off, today's behavior. A task declaring network ports, connect upstreams, or storage whose driver cannot fence a concurrent attach is downgraded to `stage` (reported by `job validate`). See docs/edge-boot.md.",
                        "properties": {
                            "mode": {
                                "type": "string",
                                "enum": ["off", "stage", "boot", "serve"],
                                "description": "off (default) = no speculation. stage = fetch artifacts and build the root disk, do not start. boot = also boot, with the network held shut. serve = also open the network; the only mode where a workload that loses its race may already have had externally visible side effects."
                            },
                            "ratifyTimeout": {
                                "type": "string",
                                "description": "Lease: unratified work is killed at expiry, unconditionally. Default 5s, capped at 2m."
                            },
                            "onRefused": {
                                "type": "string",
                                "enum": ["kill", "stop"],
                                "description": "What to do when ratification is refused. kill (default) is a hard destroy; stop runs the normal graceful-stop ladder."
                            }
                        }
                    }
                }
            },
            "rollout": {
                "type": "object",
                "properties": {
                    "strategy": { "type": "string", "description": "Rollout strategy name (e.g. default)." },
                    "maxParallel": { "type": "integer", "description": "Max allocations updated in parallel." },
                    "autoPromote": { "type": "boolean", "description": "Advance automatically between steps." }
                }
            }
        },
        "taskRuntimes": task_runtimes
    })
}

pub(crate) fn priority_arg() -> Arg {
    Arg::new("priority")
        .long("priority")
        .value_name("PRIORITY")
        .help(
            "Eval-queue priority for this request only (higher runs sooner). Overrides the \
             position implied by the job's `priority` band; it does not change the band, so \
             it has no effect on preemption",
        )
        .value_parser(value_parser!(i64))
        .conflicts_with("asap")
}

pub(crate) fn asap_flag() -> Arg {
    Arg::new("asap")
        .long("asap")
        .help("Force the request to run ASAP by setting priority to 100")
        .action(ArgAction::SetTrue)
        .conflicts_with("priority")
}

pub(crate) fn priority_from_matches(matches: &ArgMatches) -> Option<i64> {
    if matches.get_flag("asap") {
        Some(100)
    } else {
        matches.get_one::<i64>("priority").copied()
    }
}

/// The `--var` family, before KV lookups are resolved.
///
/// Every entry ends up as one field of the `vars` record the jobspec is
/// evaluated with (see `crate::config_lang`). `code` entries carry a Nickel
/// expression rather than a literal string, so `--var-code count=3` gives a
/// number and `--var count=3` gives `"3"`.
#[derive(Default)]
struct ConfigVars {
    strings: Vec<(String, String)>,
    code: Vec<(String, String)>,
    kv_strings: Vec<(String, String)>,
    kv_code: Vec<(String, String)>,
}

fn config_var_args() -> Vec<Arg> {
    vec![
        Arg::new("var")
            .long("var")
            .value_name("KEY=VALUE")
            .help("Set a string field on the jobspec's `vars` record")
            .action(ArgAction::Append),
        Arg::new("kv-var")
            .long("kv-var")
            .value_name("KEY=KV_PATH")
            .help("Set a string field on `vars` from a KV entry")
            .action(ArgAction::Append),
        Arg::new("var-code")
            .long("var-code")
            .value_name("KEY=NICKEL")
            .help("Set a field on `vars` to the value of a Nickel expression")
            .action(ArgAction::Append),
        Arg::new("kv-var-code")
            .long("kv-var-code")
            .value_name("KEY=KV_PATH")
            .help("Set a field on `vars` from a KV entry, keeping its JSON type")
            .action(ArgAction::Append),
    ]
}

fn parse_key_value(input: &str) -> R<(String, String)> {
    let (key, value) = input.split_once('=').context("expected KEY=VALUE")?;
    if key.is_empty() {
        bail!("configuration variable key cannot be empty");
    }
    Ok((key.to_string(), value.to_string()))
}

fn collect_config_vars(
    matches: &ArgMatches,
    flag: &str,
    target: &mut Vec<(String, String)>,
) -> R<()> {
    if let Some(values) = matches.get_many::<String>(flag) {
        for value in values {
            target.push(parse_key_value(value)?);
        }
    }

    Ok(())
}

fn config_vars_from_matches(matches: &ArgMatches) -> R<ConfigVars> {
    let mut vars = ConfigVars::default();

    collect_config_vars(matches, "var", &mut vars.strings)?;
    collect_config_vars(matches, "kv-var", &mut vars.kv_strings)?;
    collect_config_vars(matches, "var-code", &mut vars.code)?;
    collect_config_vars(matches, "kv-var-code", &mut vars.kv_code)?;

    Ok(vars)
}

/// Evaluates the `--var-code` expressions and assembles the `vars` record.
///
/// Later flags win on a repeated key, and code entries are evaluated in the
/// order they were given so a bad expression names itself in the error.
fn build_vars(vars: &ConfigVars) -> R<Map<String, Value>> {
    let mut resolved = Map::new();
    for (key, value) in &vars.strings {
        resolved.insert(key.clone(), Value::String(value.clone()));
    }
    for (key, expr) in &vars.code {
        let value = crate::config_lang::eval_expression(expr)
            .with_context(|| format!("failed to evaluate --var-code {key}"))?;
        resolved.insert(key.clone(), value);
    }
    Ok(resolved)
}

fn kv_entry_string_value(entry: &KvEntry) -> R<String> {
    let value_type = KvValueType::try_from(entry.value_type).unwrap_or(KvValueType::Unspecified);
    match value_type {
        KvValueType::Unspecified => bail!("kv entry {} has unspecified value type", entry.key),
        _ => Ok(entry.value.clone()),
    }
}

/// A KV entry as a Nickel expression preserving its declared type.
///
/// JSON is not a subset of Nickel (records use `=`, not `:`), so a JSON or
/// number entry is threaded through `std.deserialize` rather than pasted in
/// as source.
fn kv_entry_code_value(entry: &KvEntry) -> R<String> {
    let value_type = KvValueType::try_from(entry.value_type).unwrap_or(KvValueType::Unspecified);
    match value_type {
        KvValueType::Unspecified => bail!("kv entry {} has unspecified value type", entry.key),
        KvValueType::String => Ok(nickel_string_literal(&entry.value)),
        KvValueType::Number | KvValueType::Json => Ok(format!(
            "std.deserialize 'Json {}",
            nickel_string_literal(&entry.value)
        )),
    }
}

/// Renders a Rust string as a Nickel string literal.
///
/// Nickel's escapes are JSON's minus `\u`, plus `%` — which starts string
/// interpolation when followed by `{`.
fn nickel_string_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '%' => out.push_str("\\%"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

fn resolve_kv_config_vars<F>(vars: ConfigVars, mut lookup: F) -> R<ConfigVars>
where
    F: FnMut(&str) -> R<KvEntry>,
{
    let mut vars = vars;
    let kv_strings = std::mem::take(&mut vars.kv_strings);
    for (key, kv_key) in kv_strings {
        let entry = lookup(&kv_key)?;
        vars.strings.push((key, kv_entry_string_value(&entry)?));
    }

    let kv_code = std::mem::take(&mut vars.kv_code);
    for (key, kv_key) in kv_code {
        let entry = lookup(&kv_key)?;
        vars.code.push((key, kv_entry_code_value(&entry)?));
    }

    Ok(vars)
}

fn kv_keys_from_vars(vars: &ConfigVars) -> Vec<String> {
    let mut keys = std::collections::HashSet::new();
    for (_, key) in vars.kv_strings.iter().chain(vars.kv_code.iter()) {
        keys.insert(key.clone());
    }
    keys.into_iter().collect()
}

async fn fetch_kv_entry(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    key: &str,
) -> R<KvEntry> {
    let request = tonic::Request::new(GetKvRequest {
        key: key.to_string(),
    });
    let response = client
        .get_kv(request)
        .await
        .map_err(|status| friendly_status("failed to get kv entry", status))?;
    response.into_inner().entry.context("kv entry not found")
}

async fn resolve_kv_config_vars_from_manager(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    vars: ConfigVars,
) -> R<ConfigVars> {
    let mut cache = HashMap::new();
    for key in kv_keys_from_vars(&vars) {
        let entry = fetch_kv_entry(client, &key).await?;
        cache.insert(key, entry);
    }

    resolve_kv_config_vars(vars, |key| {
        cache.get(key).cloned().context("kv entry not loaded")
    })
}

/// Emit a value as the standard agent-friendly success envelope
/// (`{"ok":true,"data":...}`). Kept as a thin wrapper so every job read
/// command shares the one shape that mutating commands use.
fn print_json<T: Serialize>(value: &T) {
    output::print_success(value);
}

struct ResolvedJobspec {
    /// Holds the staging directory open for as long as `path` points into it.
    /// `None` for a local jobspec, which is read where it already lives.
    _temp_dir: Option<TempDir>,
    path: PathBuf,
}

/// The local file a jobspec source names, if it names one.
///
/// A bare path is local; so is a `file://` URL, which reaches the same copy in
/// the artifact fetcher. `Url::to_file_path` does the percent-decoding and
/// rejects a URL with a foreign host. Everything else (`http(s)://`, `git::`,
/// `s3://`) has to be fetched.
fn local_jobspec_path(source: &str) -> Option<PathBuf> {
    if source.starts_with("file://") {
        return Url::parse(source).ok()?.to_file_path().ok();
    }
    if source.contains("://") || source.starts_with("git::") {
        return None;
    }
    Some(PathBuf::from(source))
}

fn resolve_jobspec_source(source: &str) -> R<ResolvedJobspec> {
    // A local jobspec is evaluated in place. Copying it to a temp directory
    // would strand its relative imports: `import "lib/task.ncl"` resolves
    // against the file's own directory, and only the one file gets copied, so
    // staging turns a working spec into "could not find import".
    //
    // Only a plain existing file short-circuits. A directory, a `#fragment`
    // selector, or a missing path falls through to the fetch path below,
    // which already handles and reports each of those.
    if let Some(path) = local_jobspec_path(source) {
        if path.is_file() {
            return Ok(ResolvedJobspec {
                _temp_dir: None,
                path,
            });
        }
    }

    let temp_dir = tempfile::tempdir().context("create temporary directory for jobspec")?;
    let fetched_path = crate::artifacts::fetch_source_to_dir(source, temp_dir.path())
        .with_context(|| format!("fetch jobspec from {source}"))?;
    let resolved_path = ensure_jobspec_file(source, &fetched_path)?;

    Ok(ResolvedJobspec {
        _temp_dir: Some(temp_dir),
        path: resolved_path,
    })
}

async fn resolve_jobspec_source_async(source: &str) -> R<ResolvedJobspec> {
    let source = source.to_string();
    tokio::task::spawn_blocking(move || resolve_jobspec_source(&source))
        .await
        .context("resolve jobspec source task panicked")?
}

fn ensure_jobspec_file(source: &str, fetched_path: &Path) -> R<PathBuf> {
    if fetched_path.is_file() {
        return Ok(fetched_path.to_path_buf());
    }

    if fetched_path.is_dir() {
        if let Some(subpath) = extract_jobspec_subpath(source)? {
            let candidate = fetched_path.join(subpath);
            if candidate.is_file() {
                return Ok(candidate);
            }
            bail!(
                "jobspec path {} does not exist in fetched directory",
                candidate.display()
            );
        }
    }

    bail!(
        "jobspec source {source} resolved to a directory; append #<relative path> to select a file"
    )
}

fn extract_jobspec_subpath(source: &str) -> R<Option<PathBuf>> {
    if !source.contains("://") {
        return Ok(None);
    }

    let url = Url::parse(source).context("parse jobspec URL")?;
    let Some(fragment) = url.fragment() else {
        return Ok(None);
    };

    let fragment = fragment.trim_start_matches('/');
    if fragment.is_empty() {
        return Ok(None);
    }

    Ok(Some(PathBuf::from(fragment)))
}

/// Evaluates a jobspec to JSON, along with every source file it read.
///
/// The submissions are stored with the job, so `legion job spec` can show the
/// exact Nickel a running job was submitted from — imports included.
fn render_jobspec(job_path: &Path, vars: &ConfigVars) -> R<(String, Vec<Submission>)> {
    let values = build_vars(vars)?;
    let (rendered, sources) = crate::config_lang::eval_file_tracked(job_path, Some(&values))
        .with_context(|| format!("failed to evaluate job spec {}", job_path.display()))?;

    let submissions = sources
        .into_iter()
        .map(|source| Submission {
            path: source.path,
            content: source.content,
            is_primary: source.is_primary,
        })
        .collect();

    Ok((rendered, submissions))
}

pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("run", matches)) => run_job(matches).await,
        Some(("stop", matches)) => stop_job(matches).await,
        Some(("scale", matches)) => scale_job(matches).await,
        Some(("spec", matches)) => get_job_spec(matches).await,
        Some(("destroy", matches)) => destroy_job(matches).await,
        Some(("list", matches)) => list_jobs(matches).await,
        Some(("info", matches)) => get_info(matches).await,
        Some(("plan", matches)) => plan_job(matches).await,
        Some(("validate", matches)) => validate_job(matches).await,
        Some(("schema", matches)) => job_schema(matches).await,
        Some(("wait", matches)) => wait_job(matches).await,
        Some(("logs", matches)) => job_logs(matches).await,
        Some(("rollout", matches)) => handle_rollout(matches).await,
        _ => unreachable!(),
    }
}

#[derive(Debug, Clone, Tabled, Serialize)]
#[serde(rename_all = "camelCase")]
struct RolloutPrint {
    pub id: i64,
    pub job_id: String,
    pub status: String,
    pub to_job_config_id: String,
    pub from_job_config_id: String,
    pub created_at: String,
    pub incomplete: bool,
}

#[instrument(err, skip_all)]
async fn job_logs(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let name = arg_matches.get_one::<String>("name").cloned();
    let allocation_id = arg_matches.get_one::<i64>("allocation").copied();
    let stream = arg_matches
        .get_one::<String>("stream")
        .cloned()
        .unwrap_or_else(|| "all".to_string());
    let max_bytes = arg_matches
        .get_one::<i64>("max-bytes")
        .copied()
        .unwrap_or(0);
    let tail = arg_matches.get_flag("tail");
    let follow = arg_matches.get_flag("follow");

    if name.is_none() && allocation_id.is_none() {
        return Err(output::fail_local(
            format,
            CliError::local(
                "InvalidArgument",
                "provide a job NAME or --allocation <ID>",
                Some("usage: legion job logs <NAME> | legion job logs --allocation <ID>"),
            ),
        ));
    }

    let job = name.unwrap_or_default();
    let mut client = connect_manager_client().await?;

    if follow {
        if format.is_json() {
            return Err(output::fail_local(
                format,
                CliError::local(
                    "InvalidArgument",
                    "--follow cannot be combined with --json",
                    Some("drop --json to stream logs, or drop --follow for a one-shot JSON read"),
                ),
            ));
        }
        return follow_logs(&mut client, &job, allocation_id, &stream).await;
    }

    let request = tonic::Request::new(GetAllocationLogsRequest {
        job,
        allocation_id,
        stream,
        max_bytes,
        tail,
        offset: 0,
    });
    let response = output::resolve(
        format,
        "failed to fetch allocation logs",
        client.get_allocation_logs(request).await,
    )?
    .into_inner();

    if format.is_json() {
        print_json(&response.allocations);
        return Ok(());
    }

    if response.allocations.is_empty() {
        println!("No allocations found");
        return Ok(());
    }

    for alloc in &response.allocations {
        println!(
            "== allocation {} (task {}) on {} ==",
            alloc.allocation_id,
            task_label(&alloc.task_name),
            alloc.node_id
        );
        if !alloc.error.is_empty() {
            println!("  error: {}", alloc.error);
            continue;
        }
        if alloc.streams.is_empty() {
            println!("  (no captured logs)");
            continue;
        }
        for stream in &alloc.streams {
            let suffix = if stream.truncated {
                format!(" (truncated, {} bytes total)", stream.size_bytes)
            } else {
                String::new()
            };
            println!("--- {}{} ---", stream.stream, suffix);
            print_stream_content(&stream.content);
        }
    }
    Ok(())
}

fn task_label(task_name: &str) -> String {
    if task_name.is_empty() {
        "?".to_string()
    } else {
        task_name.to_string()
    }
}

fn print_stream_content(content: &str) {
    use std::io::Write;
    let mut stdout = std::io::stdout();
    let _ = stdout.write_all(content.as_bytes());
    if !content.is_empty() && !content.ends_with('\n') {
        let _ = stdout.write_all(b"\n");
    }
    let _ = stdout.flush();
}

/// Live-tail logs by polling `GetAllocationLogs` for each stream's trailing
/// window and printing only the bytes beyond what was already shown, tracked by
/// the per-(allocation, stream) `next_offset`. Runs until interrupted (Ctrl-C).
async fn follow_logs(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    job: &str,
    allocation_id: Option<i64>,
    stream: &str,
) -> R<()> {
    use std::collections::{HashMap, HashSet};

    // Last printed byte offset per (allocation, stream).
    let mut printed_to: HashMap<(i64, String), i64> = HashMap::new();
    let mut header_shown: HashSet<i64> = HashSet::new();
    let mut error_shown: HashSet<i64> = HashSet::new();

    loop {
        let request = tonic::Request::new(GetAllocationLogsRequest {
            job: job.to_string(),
            allocation_id,
            stream: stream.to_string(),
            max_bytes: 0,
            tail: true,
            offset: 0,
        });
        let response = match client.get_allocation_logs(request).await {
            Ok(resp) => resp.into_inner(),
            Err(status) => {
                return Err(crate::cmd::helpers::friendly_status(
                    "failed to fetch allocation logs",
                    status,
                ))
            }
        };

        for alloc in &response.allocations {
            if !alloc.error.is_empty() {
                if error_shown.insert(alloc.allocation_id) {
                    eprintln!(
                        "== allocation {} on {}: {} ==",
                        alloc.allocation_id, alloc.node_id, alloc.error
                    );
                }
                continue;
            }
            for s in &alloc.streams {
                let key = (alloc.allocation_id, s.stream.clone());
                let prev = printed_to.get(&key).copied();

                // Determine which slice of the returned window is new.
                let new_text = match prev {
                    None => s.content.clone(),
                    Some(prev) if s.next_offset <= prev => String::new(),
                    Some(prev) if prev >= s.start_offset => {
                        let skip = (prev - s.start_offset) as usize;
                        s.content.get(skip..).unwrap_or("").to_string()
                    }
                    // Window slid past what we last printed (very chatty stream):
                    // emit the whole window; a small gap may have been dropped.
                    Some(_) => s.content.clone(),
                };

                if !new_text.is_empty() {
                    if header_shown.insert(alloc.allocation_id) {
                        println!(
                            "== allocation {} (task {}) on {} ==",
                            alloc.allocation_id,
                            task_label(&alloc.task_name),
                            alloc.node_id
                        );
                    }
                    print!("[{}] ", s.stream);
                    print_stream_content(&new_text);
                }
                printed_to.insert(key, s.next_offset.max(prev.unwrap_or(0)));
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
}

async fn handle_rollout(arg_matches: &ArgMatches) -> R<()> {
    match arg_matches.subcommand() {
        Some(("list", matches)) => list_rollouts(matches).await,
        Some(("advance", matches)) => advance_rollout_cmd(matches).await,
        Some(("status", matches)) => rollout_status_cmd(matches).await,
        Some(("events", matches)) => rollout_events_cmd(matches).await,
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::JobConfig;
    use tempfile::tempdir;

    #[test]
    fn priority_conflicts_with_asap() {
        let cmd = config();
        let result = cmd.try_get_matches_from(["job", "run", "--priority", "75", "--asap"]);
        assert!(result.is_err());
    }

    #[test]
    fn logs_subcommand_parses_options() {
        let matches = config()
            .try_get_matches_from([
                "job",
                "logs",
                "demo",
                "--stream",
                "stderr",
                "--max-bytes",
                "1024",
                "--tail",
            ])
            .expect("logs args parse");
        let (name, sub) = matches.subcommand().expect("logs subcommand");
        assert_eq!(name, "logs");
        assert_eq!(sub.get_one::<String>("name").unwrap(), "demo");
        assert_eq!(sub.get_one::<String>("stream").unwrap(), "stderr");
        assert_eq!(*sub.get_one::<i64>("max-bytes").unwrap(), 1024);
        assert!(sub.get_flag("tail"));
    }

    #[test]
    fn logs_subcommand_rejects_unknown_stream() {
        let result = config().try_get_matches_from(["job", "logs", "demo", "--stream", "bogus"]);
        assert!(result.is_err());
    }

    #[test]
    fn settled_rollout_statuses() {
        for status in [
            "completed",
            "failed",
            "cancelled",
            "completed_incomplete",
            "paused",
        ] {
            assert!(is_settled_rollout_status(status), "{status} should settle");
        }
        for status in ["pending", "running", "active"] {
            assert!(
                !is_settled_rollout_status(status),
                "{status} should not settle"
            );
        }
    }

    #[test]
    fn jobspec_schema_embeds_task_runtimes() {
        let schema = build_jobspec_schema();
        assert_eq!(schema["title"], "Legion Jobspec");
        let runtimes = schema["taskRuntimes"]
            .as_object()
            .expect("taskRuntimes object");
        // Built-in runtimes must be present and carry real derived schemas.
        assert!(runtimes.contains_key("exec"));
        assert!(runtimes.contains_key("container"));
        assert!(runtimes["exec"]["properties"].is_object());
    }

    #[test]
    fn validate_and_schema_subcommands_parse() {
        let cmd = config();
        assert!(cmd
            .clone()
            .try_get_matches_from(["job", "validate", "spec.ncl", "--json"])
            .is_ok());
        assert!(cmd
            .clone()
            .try_get_matches_from(["job", "wait", "demo", "--timeout", "5"])
            .is_ok());
        assert!(cmd
            .try_get_matches_from(["job", "run", "x.ncl", "--wait", "--idempotency-key", "abc"])
            .is_ok());
    }

    fn kv_entry(key: &str, value: &str, value_type: KvValueType) -> KvEntry {
        KvEntry {
            key: key.to_string(),
            value: value.into(),
            value_type: value_type as i32,
            version: 1,
            created_at: "now".into(),
            updated_at: "now".into(),
            encrypted: false,
        }
    }

    #[test]
    fn kv_vars_render_into_jobspec() -> R<()> {
        let dir = tempdir()?;
        let job_path = dir.path().join("job.ncl");
        std::fs::write(
            &job_path,
            r#"
            {
              name = vars.job_name,
              task = {
                runtime = "noop",
              },
            }
            "#,
        )?;

        let mut vars = ConfigVars::default();
        vars.kv_strings
            .push(("job_name".into(), "jobs/name".into()));

        let resolved =
            resolve_kv_config_vars(vars, |key| Ok(kv_entry(key, "kv-job", KvValueType::String)))?;

        let (rendered, _subs) = render_jobspec(&job_path, &resolved)?;
        let config: JobConfig = serde_json::from_str(&rendered)?;
        assert_eq!(config.name, "kv-job");
        Ok(())
    }

    #[test]
    fn local_jobspec_resolves_in_place_so_imports_still_work() -> R<()> {
        // Regression: the resolver used to copy a local spec into a temp
        // directory on its own, which left `import "lib/task.ncl"` looking in
        // a directory that held nothing but the copy.
        let dir = tempdir()?;
        std::fs::create_dir(dir.path().join("lib"))?;
        std::fs::write(
            dir.path().join("lib").join("task.ncl"),
            r#"{ name = "t", runtime = "noop" }"#,
        )?;
        let job_path = dir.path().join("job.ncl");
        std::fs::write(
            &job_path,
            r#"
            let task = import "lib/task.ncl" in
            { name = "imports", tasks = [task] }
            "#,
        )?;

        let resolved = resolve_jobspec_source(job_path.to_str().unwrap())?;
        assert_eq!(resolved.path, job_path, "spec should be read where it sits");

        let (rendered, subs) = render_jobspec(&resolved.path, &ConfigVars::default())?;
        let config: JobConfig = serde_json::from_str(&rendered)?;
        assert_eq!(config.name, "imports");
        // The imported file is stored with the job, not just the entrypoint.
        assert_eq!(subs.len(), 2);
        assert!(subs
            .iter()
            .any(|s| s.is_primary && s.path.ends_with("job.ncl")));
        assert!(subs
            .iter()
            .any(|s| !s.is_primary && s.path.ends_with("task.ncl")));
        Ok(())
    }

    #[test]
    fn local_jobspec_paths_are_recognized_without_fetching() {
        assert_eq!(
            local_jobspec_path("configs/jobs/demo.ncl"),
            Some(PathBuf::from("configs/jobs/demo.ncl"))
        );
        assert_eq!(
            local_jobspec_path("/abs/demo.ncl"),
            Some(PathBuf::from("/abs/demo.ncl"))
        );
        assert_eq!(
            local_jobspec_path("file:///abs/demo.ncl"),
            Some(PathBuf::from("/abs/demo.ncl"))
        );
        // Percent-decoded, which is why this goes through `Url`.
        assert_eq!(
            local_jobspec_path("file:///abs/with%20space.ncl"),
            Some(PathBuf::from("/abs/with space.ncl"))
        );
        // Remote sources still stage into a temp directory, which is correct
        // for them: the fetcher brings the whole tree, imports included.
        assert_eq!(local_jobspec_path("https://example.com/demo.ncl"), None);
        assert_eq!(local_jobspec_path("git::https://example.com/r.git"), None);
        assert_eq!(local_jobspec_path("s3://bucket/demo.ncl"), None);
    }

    #[test]
    fn a_missing_local_jobspec_is_reported_by_path() {
        let Err(err) = resolve_jobspec_source("does/not/exist.ncl") else {
            panic!("a missing jobspec should not resolve");
        };
        assert!(err.to_string().contains("does/not/exist.ncl"), "{err}");
    }

    #[test]
    fn json_jobspecs_render_without_nickel() -> R<()> {
        // Machine-emitted config: the same spec a generator would write out.
        let dir = tempdir()?;
        let job_path = dir.path().join("job.json");
        std::fs::write(
            &job_path,
            r#"{"name": "generated", "tasks": [{"name": "t", "runtime": "noop"}]}"#,
        )?;

        let (rendered, subs) = render_jobspec(&job_path, &ConfigVars::default())?;
        let config: JobConfig = serde_json::from_str(&rendered)?;
        assert_eq!(config.name, "generated");
        assert_eq!(subs.len(), 1);
        assert!(subs[0].is_primary);
        Ok(())
    }

    #[test]
    fn json_jobspecs_reject_vars() -> R<()> {
        let dir = tempdir()?;
        let job_path = dir.path().join("job.json");
        std::fs::write(&job_path, r#"{"name": "generated"}"#)?;

        let mut vars = ConfigVars::default();
        vars.strings.push(("image".into(), "nginx".into()));
        let err = render_jobspec(&job_path, &vars).unwrap_err();
        assert!(format!("{err:#}").contains("has no variables"), "{err:#}");
        Ok(())
    }

    #[test]
    fn kv_code_vars_keep_their_json_type() -> R<()> {
        let mut vars = ConfigVars::default();
        vars.kv_code.push(("count".into(), "jobs/count".into()));
        vars.kv_code.push(("tags".into(), "jobs/tags".into()));
        vars.kv_strings.push(("plain".into(), "jobs/plain".into()));

        let resolved = resolve_kv_config_vars(vars, |key| match key {
            "jobs/count" => Ok(kv_entry(key, "4", KvValueType::Number)),
            "jobs/tags" => Ok(kv_entry(key, r#"["a","b"]"#, KvValueType::Json)),
            _ => Ok(kv_entry(key, "text", KvValueType::String)),
        })?;

        let values = build_vars(&resolved)?;
        assert_eq!(values["count"], json!(4));
        assert_eq!(values["tags"], json!(["a", "b"]));
        assert_eq!(values["plain"], json!("text"));
        Ok(())
    }

    #[test]
    fn var_code_evaluates_nickel_expressions() -> R<()> {
        let mut vars = ConfigVars::default();
        vars.strings.push(("image".into(), "nginx".into()));
        vars.code.push(("count".into(), "1 + 2".into()));
        vars.code.push(("limits".into(), "{ memory = 512 }".into()));

        let values = build_vars(&vars)?;
        assert_eq!(values["image"], json!("nginx"));
        assert_eq!(values["count"], json!(3));
        assert_eq!(values["limits"], json!({ "memory": 512 }));
        Ok(())
    }

    #[test]
    fn nickel_string_literals_escape_interpolation() {
        assert_eq!(nickel_string_literal("a%{b}"), r#""a\%{b}""#);
        assert_eq!(nickel_string_literal("say \"hi\""), r#""say \"hi\"""#);
    }
}

#[instrument(err, skip_all)]
async fn stop_job(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let mut client = connect_manager_client().await?;
    let job_name = arg_matches
        .get_one::<String>("name")
        .map_or("demo", String::as_str);
    let priority = priority_from_matches(arg_matches);
    let idempotency_key = arg_matches.get_one::<String>("idempotency-key").cloned();

    let request = tonic::Request::new(StopJobRequest {
        name: job_name.to_owned(),
        priority,
        idempotency_key,
    });

    let reply =
        output::resolve(format, "failed to stop job", client.stop_job(request).await)?.into_inner();

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

/// `legion job scale <NAME> [--count N | --reset]` (docs/autoscaling.md):
/// with `--count`/`--reset` it calls ScaleTask; with neither it prints the
/// job's scale status (spec vs override vs effective, per task).
#[instrument(err, skip_all)]
async fn scale_job(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let mut client = connect_manager_client().await?;
    let job_name = arg_matches
        .get_one::<String>("name")
        .context("job name is required")?
        .clone();
    let count = arg_matches.get_one::<u32>("count").copied();
    let reset = arg_matches.get_flag("reset");

    if count.is_none() && !reset {
        // Status mode.
        let request = tonic::Request::new(GetJobScaleStatusRequest {
            name: job_name.clone(),
        });
        let reply = output::resolve(
            format,
            "failed to fetch scale status",
            client.get_job_scale_status(request).await,
        )?
        .into_inner();
        if format.is_json() {
            output::print_success(&json!({ "tasks": reply
                .tasks
                .iter()
                .map(|task| {
                    json!({
                        "group": task.task_group,
                        "task": task.task_name,
                        "specCount": task.spec_count,
                        "overrideCount": task.override_count,
                        "pendingCount": task.pending_count,
                        "effectiveCount": task.effective_count,
                        "source": task.source,
                        "reason": task.reason,
                        "updatedAt": task.updated_at,
                    })
                })
                .collect::<Vec<_>>() }));
        } else {
            for task in reply.tasks {
                let mut line = format!(
                    "{}/{}/{}: count {} (spec {}",
                    job_name,
                    task.task_group,
                    task.task_name,
                    task.effective_count,
                    task.spec_count
                );
                if task.override_count.is_some() {
                    line.push_str(&format!(", scaled by {}", task.source));
                }
                if let Some(pending) = task.pending_count {
                    line.push_str(&format!(", scale-in to {pending} pending"));
                }
                line.push(')');
                println!("{line}");
            }
        }
        return Ok(());
    }

    let request = tonic::Request::new(ScaleTaskRequest {
        name: job_name,
        task_group: arg_matches.get_one::<String>("group").cloned(),
        task_name: arg_matches.get_one::<String>("task").cloned(),
        count,
        reset,
        reason: arg_matches.get_one::<String>("reason").cloned(),
        idempotency_key: arg_matches.get_one::<String>("idempotency-key").cloned(),
    });
    let reply = output::resolve(
        format,
        "failed to scale job",
        client.scale_task(request).await,
    )?
    .into_inner();

    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "deduplicated": reply.deduplicated,
            "deferred": reply.deferred,
        }));
    } else {
        println!("{}", reply.message);
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn get_job_spec(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);

    let mut client = connect_manager_client().await?;
    let job_name = arg_matches
        .get_one::<String>("name")
        .map_or("demo", String::as_str);

    let request = tonic::Request::new(GetJobSpecRequest {
        name: job_name.to_owned(),
    });

    let response = output::resolve(
        format,
        "failed to fetch job spec",
        client.get_job_spec(request).await,
    )?;

    let subs = response.into_inner().submissions;

    if format.is_json() {
        print_json(&subs);
    } else {
        subs.iter().for_each(|s| {
            println!("=============================");
            println!("{}:\n", s.path);
            println!("{}", s.content);
            println!("=============================");
        });
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn destroy_job(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let mut client = connect_manager_client().await?;
    let job_name = arg_matches
        .get_one::<String>("name")
        .map_or("demo", String::as_str);
    let priority = priority_from_matches(arg_matches);
    let idempotency_key = arg_matches.get_one::<String>("idempotency-key").cloned();

    let request = tonic::Request::new(DestroyJobRequest {
        name: job_name.to_owned(),
        priority,
        idempotency_key,
    });

    let reply = output::resolve(
        format,
        "failed to destroy job",
        client.destroy_job(request).await,
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
async fn plan_job(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let mut client = connect_manager_client().await?;
    let path = arg_matches
        .get_one::<String>("path")
        .map(String::as_str)
        .context("missing jobspec path")?;
    let vars = config_vars_from_matches(arg_matches)?;
    let vars = resolve_kv_config_vars_from_manager(&mut client, vars).await?;
    let resolved = resolve_jobspec_source_async(path).await?;
    let (rendered, _submissions) = render_jobspec(&resolved.path, &vars)?;

    let request = tonic::Request::new(PlanJobRequest {
        parsed_jobspec: rendered,
    });

    let response = output::resolve(format, "failed to plan job", client.plan_job(request).await)?;
    if format.is_json() {
        let reply = response.into_inner();
        print_json(&reply);
        return Ok(());
    } else {
        let reply = response.into_inner();
        let allocations = reply.allocations;
        if !reply.message.is_empty() {
            println!("{}", reply.message);
        }
        // One line per placement that carries fallback candidates: the
        // ranked list the apply walk may bind instead of the first choice
        // under contention (docs/late-binding-plan-apply.md). Quiet when
        // every placement is a singleton (plan_candidates: 0, coupled
        // placements, or a pre-candidate manager).
        let candidate_lines: Vec<String> = allocations
            .iter()
            .filter(|alloc| alloc.candidate_nodes.len() > 1)
            .map(|alloc| format!("{}: {}", alloc.task_name, alloc.candidate_nodes.join(" > ")))
            .collect();
        let printable_allocations: Vec<JobPlanPrint> =
            allocations.into_iter().map(Into::into).collect();
        if printable_allocations.is_empty() {
            println!("No allocations planned");
        } else {
            print_table(printable_allocations);
        }
        if !candidate_lines.is_empty() {
            println!("Placement candidates (best first; a race falls back down the list):");
            for line in candidate_lines {
                println!("  {line}");
            }
        }
        // One line per task on how candidates were generated: which
        // scheduling index served (or why none did). Quiet when there is
        // nothing to say (indexes unarmed and no reason recorded).
        let index_lines: Vec<String> = reply
            .index_usage
            .iter()
            .filter_map(|usage| {
                if usage.index.is_empty() {
                    if usage.reason.is_empty() && usage.score_sampled == 0 {
                        return None;
                    }
                    let mut line = format!("{}: scan", usage.task_name);
                    if !usage.reason.is_empty() {
                        line.push_str(&format!(" — {}", usage.reason));
                    }
                    if usage.score_sampled > 0 {
                        line.push_str(&format!(
                            "; scored best of {} sampled nodes",
                            usage.score_sampled
                        ));
                    }
                    return Some(line);
                }
                let mut notes = Vec::new();
                if usage.score_sampled > 0 {
                    notes.push(format!(
                        "scored best of {} sampled nodes",
                        usage.score_sampled
                    ));
                }
                if usage.truncated {
                    notes.push("truncated".to_string());
                }
                if usage.refills > 0 {
                    notes.push(format!("refills {}", usage.refills));
                }
                if usage.fell_back {
                    notes.push("fell back to scan".to_string());
                }
                if let Some(agreement) = usage.shadow_agreement {
                    notes.push(format!(
                        "shadow {}",
                        if agreement { "agrees" } else { "DISAGREES" }
                    ));
                }
                let notes = if notes.is_empty() {
                    String::new()
                } else {
                    format!("; {}", notes.join(", "))
                };
                Some(format!(
                    "{}: index {} ({} mode, {} candidates{})",
                    usage.task_name, usage.index, usage.mode, usage.candidates, notes
                ))
            })
            .collect();
        if !index_lines.is_empty() {
            println!("Scheduling index:");
            for line in index_lines {
                println!("  {line}");
            }
        }
        // Who this submit would displace. Printed last and unmissably,
        // because it is the one thing in a plan that costs somebody else
        // something (S3 in the preemption plan: the submit becomes a
        // decision, not a surprise).
        if reply.preemptions.is_empty() {
            if reply.preemption_mode == "off" {
                println!(
                    "Preemption: off for this cluster; a higher-band job cannot displace \
                     lower-band work"
                );
            }
        } else {
            let verb = if reply.preemption_mode == "observe" {
                "would preempt (mode: observe — nothing is evicted)"
            } else {
                "would preempt"
            };
            println!("Preemption: {verb}");
            // Group by node so the output reads the way an operator thinks
            // about it: "on node-d, these go".
            let mut by_node: std::collections::BTreeMap<&str, Vec<&_>> =
                std::collections::BTreeMap::new();
            for victim in &reply.preemptions {
                by_node
                    .entry(victim.node_id.as_str())
                    .or_default()
                    .push(victim);
            }
            for (node_id, victims) in by_node {
                for victim in victims {
                    println!(
                        "  {node_id}: {}/{} (alloc {}, priority {}) — {}s grace, requeued",
                        victim.job_id,
                        victim.task_name,
                        victim.allocation_id,
                        victim.band,
                        victim.kill_after_seconds
                    );
                }
            }
        }
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn run_job(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let mut client = connect_manager_client().await?;

    let job_path = arg_matches
        .get_one::<String>("path")
        .map_or("./configs/jobs/demo.ncl", String::as_str);
    let priority = priority_from_matches(arg_matches);
    let idempotency_key = arg_matches.get_one::<String>("idempotency-key").cloned();
    let vars = config_vars_from_matches(arg_matches)?;
    let vars = resolve_kv_config_vars_from_manager(&mut client, vars).await?;
    let resolved = resolve_jobspec_source_async(job_path).await?;
    let (rendered, submissions) = render_jobspec(&resolved.path, &vars)?;

    // Capture the job name so we can optionally wait for convergence below.
    let job_name = serde_json::from_str::<JobConfig>(&rendered)
        .map(|config| config.name)
        .ok();

    let request = tonic::Request::new(RunJobRequest {
        parsed_jobspec: rendered,
        submissions,
        priority,
        force_reschedule: arg_matches.get_flag("force-reschedule"),
        idempotency_key,
    });

    let reply =
        output::resolve(format, "failed to run job", client.run_job(request).await)?.into_inner();

    let wait = arg_matches.get_flag("wait");
    let timeout = wait_timeout(arg_matches);

    let wait_result = if wait {
        match job_name.as_deref() {
            Some(name) => Some(wait_for_convergence(&mut client, name, timeout, format).await?),
            None => None,
        }
    } else {
        None
    };

    if format.is_json() {
        output::print_success(&json!({
            "message": reply.message,
            "deduplicated": reply.deduplicated,
            "wait": wait_result,
        }));
    } else {
        println!("{}", reply.message);
        if let Some(result) = &wait_result {
            println!("{}", result.human_summary());
        }
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn list_jobs(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);

    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(ListJobsRequest {
        status: arg_matches.get_one::<String>("status").cloned(),
        ..Default::default()
    });
    let response = output::resolve(
        format,
        "failed to list jobs",
        client.list_jobs(request).await,
    )?;
    let jobs_list = response.into_inner().jobs;

    if format.is_json() {
        print_json(&jobs_list);
    } else {
        let printable_jobs: Vec<JobPrint> = jobs_list.into_iter().map(Into::into).collect();
        print_table(printable_jobs);
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn list_rollouts(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);
    let active_only = arg_matches.get_flag("active");
    let job_id = arg_matches.get_one::<String>("job").cloned();

    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(ListRolloutsRequest {
        job_id,
        active_only,
        ..Default::default()
    });
    let response = output::resolve(
        format,
        "failed to list rollouts",
        client.list_rollouts(request).await,
    )?;
    let rollouts = response.into_inner().rollouts;

    if format.is_json() {
        print_json(&rollouts);
    } else {
        let printable = rollouts
            .into_iter()
            .map(|rollout| RolloutPrint {
                id: rollout.id,
                job_id: rollout.job_id,
                status: rollout.status,
                to_job_config_id: rollout.to_job_config_id,
                from_job_config_id: rollout.from_job_config_id.unwrap_or_default(),
                created_at: rollout.created_at,
                incomplete: rollout.incomplete,
            })
            .collect::<Vec<_>>();
        if printable.is_empty() {
            println!("No rollouts found");
        } else {
            print_table(printable);
        }
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn advance_rollout_cmd(arg_matches: &ArgMatches) -> R<()> {
    let rollout_id = *arg_matches
        .get_one::<i64>("id")
        .context("missing rollout id")?;
    let groups = arg_matches
        .get_many::<String>("group")
        .map(|values| values.cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let priority = priority_from_matches(arg_matches);
    let format = OutputFormat::from_matches(arg_matches);

    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(AdvanceRolloutRequest {
        id: rollout_id,
        groups,
        priority,
    });
    let response = output::resolve(
        format,
        "failed to advance rollout",
        client.advance_rollout(request).await,
    )?
    .into_inner();

    if format.is_json() {
        print_json(&response.rollout);
    } else if let Some(rollout) = response.rollout {
        println!(
            "{} (id={}, status={})",
            response.message, rollout.id, rollout.status
        );
    } else {
        println!("{}", response.message);
    }

    Ok(())
}

#[derive(Debug, Clone, Tabled, Serialize)]
#[serde(rename_all = "camelCase")]
struct RolloutGroupStatusPrint {
    group: String,
    step: String,
    pending_manual_advances: u32,
    allocations: usize,
}

#[derive(Debug, Clone, Tabled, Serialize)]
#[serde(rename_all = "camelCase")]
struct RolloutAllocationStatusPrint {
    allocation_id: i64,
    task: String,
    group: String,
    lifecycle: String,
    role: String,
    status: String,
    allocation_health: String,
    node_id: String,
    node_health: String,
}

#[derive(Debug, Clone, Tabled, Serialize)]
#[serde(rename_all = "camelCase")]
struct RolloutEventPrint {
    id: i64,
    created_at: String,
    level: String,
    group: String,
    message: String,
}

#[instrument(err, skip_all)]
async fn rollout_status_cmd(arg_matches: &ArgMatches) -> R<()> {
    let rollout_id = *arg_matches
        .get_one::<i64>("id")
        .context("missing rollout id")?;
    let format = OutputFormat::from_matches(arg_matches);

    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(GetRolloutStatusRequest { id: rollout_id });
    let response = output::resolve(
        format,
        "failed to load rollout status",
        client.get_rollout_status(request).await,
    )?
    .into_inner();

    if format.is_json() {
        print_json(&response);
        return Ok(());
    }

    let Some(rollout) = response.rollout else {
        println!("Rollout {rollout_id} not found");
        return Ok(());
    };

    println!(
        "Rollout {} for job {} is {}{}",
        rollout.id,
        rollout.job_id,
        rollout.status,
        if rollout.incomplete {
            " (incomplete)"
        } else {
            ""
        }
    );
    if !rollout.message.is_empty() {
        println!("Message: {}", rollout.message);
    }

    let group_rows = response
        .groups
        .iter()
        .map(|group| RolloutGroupStatusPrint {
            group: group.group.clone(),
            step: format!(
                "{}/{}",
                group.step_index.min(group.total_steps),
                group.total_steps.max(1)
            ),
            pending_manual_advances: group.pending_manual_advances,
            allocations: group.allocations.len(),
        })
        .collect::<Vec<_>>();
    if !group_rows.is_empty() {
        print_table(group_rows);
    }

    let allocation_rows = response
        .groups
        .into_iter()
        .flat_map(|group| {
            group
                .allocations
                .into_iter()
                .map(move |allocation| RolloutAllocationStatusPrint {
                    allocation_id: allocation.allocation_id,
                    task: allocation.task_name,
                    group: group.group.clone(),
                    lifecycle: allocation.lifecycle,
                    role: allocation.role,
                    status: allocation.status,
                    allocation_health: allocation
                        .allocation_health
                        .or_else(|| {
                            allocation.allocation_healthy.map(|healthy| {
                                if healthy {
                                    "healthy".to_string()
                                } else {
                                    "unhealthy".to_string()
                                }
                            })
                        })
                        .unwrap_or_else(|| "unknown".to_string()),
                    node_id: allocation.node_id,
                    node_health: allocation
                        .node_healthy
                        .map(|healthy| {
                            if healthy {
                                "healthy".to_string()
                            } else {
                                "unhealthy".to_string()
                            }
                        })
                        .unwrap_or_else(|| "unknown".to_string()),
                })
        })
        .collect::<Vec<_>>();
    if !allocation_rows.is_empty() {
        print_table(allocation_rows);
    }

    Ok(())
}

#[instrument(err, skip_all)]
async fn rollout_events_cmd(arg_matches: &ArgMatches) -> R<()> {
    let rollout_id = *arg_matches
        .get_one::<i64>("id")
        .context("missing rollout id")?;
    let limit = arg_matches.get_one::<u32>("limit").copied();
    let format = OutputFormat::from_matches(arg_matches);

    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(ListRolloutEventsRequest { rollout_id, limit });
    let response = output::resolve(
        format,
        "failed to load rollout events",
        client.list_rollout_events(request).await,
    )?
    .into_inner();

    if format.is_json() {
        print_json(&response.events);
        return Ok(());
    }

    if response.events.is_empty() {
        println!("No rollout events found for rollout {rollout_id}");
        return Ok(());
    }

    let rows = response
        .events
        .into_iter()
        .map(|event| RolloutEventPrint {
            id: event.id,
            created_at: event.created_at,
            level: event.level,
            group: event.group_name.unwrap_or_else(|| "all".to_string()),
            message: event.message,
        })
        .collect::<Vec<_>>();
    print_table(rows);
    Ok(())
}

#[instrument(err, skip_all)]
async fn get_info(arg_matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(arg_matches);

    let mut client = connect_manager_client().await?;
    let job_name = arg_matches.get_one::<String>("name").context("need name")?;
    let request = tonic::Request::new(GetJobInfoRequest {
        name: job_name.to_owned(),
    });
    let response = output::resolve(
        format,
        "failed to fetch job info",
        client.get_job_info(request).await,
    )?;

    if format.is_json() {
        print_json(&response.into_inner().job);
    } else {
        let printable_job: JobPrint = response.into_inner().job.context("no job found")?.into();
        print_table(vec![printable_job]);
    }

    Ok(())
}
