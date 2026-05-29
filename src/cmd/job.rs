use std::path::{Path, PathBuf};
use std::vec;

use clap::value_parser;

use crate::cmd::helpers::{connect_manager_client, friendly_status, json_flag, print_table};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::getter_rs::Client as GetterClient;
use crate::prelude::*;
use crate::structs::{
    AdvanceRolloutRequest, DestroyJobRequest, GetAllocationLogsRequest, GetJobInfoRequest,
    GetJobSpecRequest, GetKvRequest, GetRolloutStatusRequest, JobConfig, JobPlanPrint, JobPrint,
    KvEntry, KvValueType, ListJobsRequest, ListRolloutEventsRequest, ListRolloutsRequest,
    PlanJobRequest, RunJobRequest, StopJobRequest, Submission,
};
use crate::task_plugins::{builtin_task_plugin_schemas, validate_job_config};
use jsonnet::JsonnetVm;
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
                .help("Path or URL to the job specification to run")
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
        .args(jsonnet_var_args());

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
                .help("Path or URL to the job specification to validate")
                .required(true)
                .value_hint(ValueHint::AnyPath),
        )
        .arg(json_flag())
        .args(jsonnet_var_args());

    let schema_cmd = Command::new("schema")
        .about("Emit the JSON Schema for jobspecs and built-in task kinds")
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
                .help("Path or URL to the job specification to plan")
                .required(true)
                .value_hint(ValueHint::AnyPath),
        )
        .arg(json_flag())
        .args(jsonnet_var_args());

    let spec_cmd = Command::new("spec")
        .about("Get job spec used to define the job")
        .arg(
            Arg::new("name")
                .value_name("NAME")
                .help("Name of the job to fetch the spec for"),
        )
        .arg(json_flag());

    let list_cmd = Command::new("list").about("List all jobs").arg(json_flag());

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
        .subcommand(spec_cmd)
        .subcommand(destroy_cmd)
        .subcommand(logs_cmd)
        .subcommand(rollout_cmd)
}

/// Optional idempotency key for mutating job operations. When supplied, the
/// manager deduplicates retried requests carrying the same key, so an agent
/// can safely retry on timeout without risking a double-apply.
fn idempotency_arg() -> Arg {
    Arg::new("idempotency-key")
        .long("idempotency-key")
        .value_name("KEY")
        .help("Deduplicate retries of this mutating request (safe for agent retries)")
}

/// Flag requesting that `job run` block until the resulting rollout converges.
fn wait_flag() -> Arg {
    Arg::new("wait")
        .long("wait")
        .help("Block until the job's rollout converges to a terminal state")
        .action(ArgAction::SetTrue)
}

/// Timeout (seconds) for convergence waits.
fn timeout_arg() -> Arg {
    Arg::new("timeout")
        .long("timeout")
        .value_name("SECONDS")
        .help("Maximum seconds to wait for convergence (default 120)")
        .value_parser(value_parser!(u64))
}

/// Default convergence-wait timeout when `--timeout` is not supplied.
const DEFAULT_WAIT_SECS: u64 = 120;

fn wait_timeout(matches: &ArgMatches) -> std::time::Duration {
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
async fn wait_for_convergence(
    client: &mut crate::structs::manager_client::ManagerClient<tonic::transport::Channel>,
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

    // Resolve jsonnet vars. KV-backed vars require the manager; plain vars do
    // not, so validation stays usable offline for the common case.
    let vars = jsonnet_vars_from_matches(arg_matches)?;
    let vars = if kv_keys_from_vars(&vars).is_empty() {
        vars
    } else {
        let mut client = connect_manager_client().await?;
        resolve_kv_jsonnet_vars_from_manager(&mut client, vars).await?
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
        return Err(output::fail_local(
            format,
            CliError::local(
                "InvalidArgument",
                format!("jobspec failed validation: {err}"),
                None,
            ),
        ));
    }

    let task_count = config.tasks.len();
    let groups: std::collections::BTreeSet<&str> = config
        .tasks
        .iter()
        .map(|task| task.group.as_str())
        .collect();

    if format.is_json() {
        output::print_success(&json!({
            "valid": true,
            "job": config.name,
            "tasks": task_count,
            "groups": groups.iter().collect::<Vec<_>>(),
        }));
    } else {
        println!(
            "Jobspec '{}' is valid: {} task(s) across {} group(s)",
            config.name,
            task_count,
            groups.len()
        );
    }

    Ok(())
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
/// live, per-kind task config schemas derived from the built-in plugins.
pub(crate) fn build_jobspec_schema() -> Value {
    let mut task_kinds = serde_json::Map::new();
    for plugin in builtin_task_plugin_schemas() {
        task_kinds.insert(plugin.name, plugin.schema);
    }

    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Legion Jobspec",
        "description": "A Legion job specification (the JSON a jsonnet jobspec renders to). \
            A job has a name and one or more tasks (use `task` for a single task or `tasks` for many). \
            Each task's `config` object is validated against the schema for its `kind` under `taskKinds`.",
        "type": "object",
        "required": ["name"],
        "properties": {
            "name": { "type": "string", "description": "Unique job name." },
            "task": { "$ref": "#/$defs/task", "description": "A single task (shorthand for tasks: [task])." },
            "tasks": { "type": "array", "items": { "$ref": "#/$defs/task" }, "description": "List of tasks." },
            "version": { "type": "integer", "description": "Optional jobspec version." },
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
                "required": ["kind"],
                "properties": {
                    "name": { "type": "string", "description": "Task name (unique within the job)." },
                    "kind": { "type": "string", "description": "Task kind/plugin (e.g. exec, container, noop)." },
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
                    "config": { "type": "object", "description": "Kind-specific config; validated against taskKinds[kind]." },
                    "dependsOn": {
                        "type": "array",
                        "items": { "type": "object", "required": ["task"], "properties": {
                            "task": { "type": "string" },
                            "condition": { "type": "string", "enum": ["running", "healthy", "complete", "success"] }
                        }},
                        "description": "Task dependencies that gate start."
                    },
                    "schedule": { "description": "Batch-only schedule (cron string or {every: duration})." }
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
        "taskKinds": task_kinds
    })
}

fn priority_arg() -> Arg {
    Arg::new("priority")
        .long("priority")
        .value_name("PRIORITY")
        .help("Priority to use for the request (higher runs sooner)")
        .value_parser(value_parser!(i64))
        .conflicts_with("asap")
}

fn asap_flag() -> Arg {
    Arg::new("asap")
        .long("asap")
        .help("Force the request to run ASAP by setting priority to 100")
        .action(ArgAction::SetTrue)
        .conflicts_with("priority")
}

fn priority_from_matches(matches: &ArgMatches) -> Option<i64> {
    if matches.get_flag("asap") {
        Some(100)
    } else {
        matches.get_one::<i64>("priority").copied()
    }
}

#[derive(Default)]
struct JsonnetVars {
    ext_vars: Vec<(String, String)>,
    ext_codes: Vec<(String, String)>,
    tla_vars: Vec<(String, String)>,
    tla_codes: Vec<(String, String)>,
    kv_ext_vars: Vec<(String, String)>,
    kv_ext_codes: Vec<(String, String)>,
    kv_tla_vars: Vec<(String, String)>,
    kv_tla_codes: Vec<(String, String)>,
}

impl JsonnetVars {}

fn jsonnet_var_args() -> Vec<Arg> {
    vec![
        Arg::new("var")
            .long("var")
            .value_name("KEY=VALUE")
            .help("Set a Jsonnet external string variable (std.extVar)")
            .action(ArgAction::Append),
        Arg::new("kv-var")
            .long("kv-var")
            .value_name("KEY=KV_PATH")
            .help("Set a Jsonnet external variable from a KV entry (std.extVar)")
            .action(ArgAction::Append),
        Arg::new("var-code")
            .long("var-code")
            .value_name("KEY=CODE")
            .help("Set a Jsonnet external code variable (std.extVar)")
            .action(ArgAction::Append),
        Arg::new("kv-var-code")
            .long("kv-var-code")
            .value_name("KEY=KV_PATH")
            .help("Set a Jsonnet external code variable from a KV entry (std.extVar)")
            .action(ArgAction::Append),
        Arg::new("arg")
            .long("arg")
            .value_name("KEY=VALUE")
            .help("Set a Jsonnet top-level string argument")
            .action(ArgAction::Append),
        Arg::new("kv-arg")
            .long("kv-arg")
            .value_name("KEY=KV_PATH")
            .help("Set a Jsonnet top-level argument from a KV entry")
            .action(ArgAction::Append),
        Arg::new("arg-code")
            .long("arg-code")
            .value_name("KEY=CODE")
            .help("Set a Jsonnet top-level code argument")
            .action(ArgAction::Append),
        Arg::new("kv-arg-code")
            .long("kv-arg-code")
            .value_name("KEY=KV_PATH")
            .help("Set a Jsonnet top-level code argument from a KV entry")
            .action(ArgAction::Append),
    ]
}

fn parse_key_value(input: &str) -> R<(String, String)> {
    let (key, value) = input.split_once('=').context("expected KEY=VALUE")?;
    if key.is_empty() {
        bail!("jsonnet variable key cannot be empty");
    }
    Ok((key.to_string(), value.to_string()))
}

fn collect_jsonnet_vars(
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

fn jsonnet_vars_from_matches(matches: &ArgMatches) -> R<JsonnetVars> {
    let mut vars = JsonnetVars::default();

    collect_jsonnet_vars(matches, "var", &mut vars.ext_vars)?;
    collect_jsonnet_vars(matches, "kv-var", &mut vars.kv_ext_vars)?;
    collect_jsonnet_vars(matches, "var-code", &mut vars.ext_codes)?;
    collect_jsonnet_vars(matches, "kv-var-code", &mut vars.kv_ext_codes)?;
    collect_jsonnet_vars(matches, "arg", &mut vars.tla_vars)?;
    collect_jsonnet_vars(matches, "kv-arg", &mut vars.kv_tla_vars)?;
    collect_jsonnet_vars(matches, "arg-code", &mut vars.tla_codes)?;
    collect_jsonnet_vars(matches, "kv-arg-code", &mut vars.kv_tla_codes)?;

    Ok(vars)
}

fn apply_jsonnet_vars(vm: &mut JsonnetVm, vars: &JsonnetVars) {
    for (key, value) in &vars.ext_vars {
        vm.ext_var(key, value);
    }
    for (key, value) in &vars.ext_codes {
        vm.ext_code(key, value);
    }
    for (key, value) in &vars.tla_vars {
        vm.tla_var(key, value);
    }
    for (key, value) in &vars.tla_codes {
        vm.tla_code(key, value);
    }
}

fn kv_entry_string_value(entry: &KvEntry) -> R<String> {
    let value_type = KvValueType::try_from(entry.value_type).unwrap_or(KvValueType::Unspecified);
    match value_type {
        KvValueType::Unspecified => bail!("kv entry {} has unspecified value type", entry.key),
        _ => Ok(entry.value.clone()),
    }
}

fn kv_entry_code_value(entry: &KvEntry) -> R<String> {
    let value_type = KvValueType::try_from(entry.value_type).unwrap_or(KvValueType::Unspecified);
    match value_type {
        KvValueType::Unspecified => bail!("kv entry {} has unspecified value type", entry.key),
        KvValueType::String => Ok(serde_json::to_string(&entry.value)?),
        KvValueType::Number | KvValueType::Json => Ok(entry.value.clone()),
    }
}

fn resolve_kv_jsonnet_vars<F>(vars: JsonnetVars, mut lookup: F) -> R<JsonnetVars>
where
    F: FnMut(&str) -> R<KvEntry>,
{
    let mut vars = vars;
    let kv_ext_vars = std::mem::take(&mut vars.kv_ext_vars);
    for (key, kv_key) in kv_ext_vars {
        let entry = lookup(&kv_key)?;
        vars.ext_vars.push((key, kv_entry_string_value(&entry)?));
    }

    let kv_ext_codes = std::mem::take(&mut vars.kv_ext_codes);
    for (key, kv_key) in kv_ext_codes {
        let entry = lookup(&kv_key)?;
        vars.ext_codes.push((key, kv_entry_code_value(&entry)?));
    }

    let kv_tla_vars = std::mem::take(&mut vars.kv_tla_vars);
    for (key, kv_key) in kv_tla_vars {
        let entry = lookup(&kv_key)?;
        vars.tla_vars.push((key, kv_entry_string_value(&entry)?));
    }

    let kv_tla_codes = std::mem::take(&mut vars.kv_tla_codes);
    for (key, kv_key) in kv_tla_codes {
        let entry = lookup(&kv_key)?;
        vars.tla_codes.push((key, kv_entry_code_value(&entry)?));
    }

    Ok(vars)
}

fn kv_keys_from_vars(vars: &JsonnetVars) -> Vec<String> {
    let mut keys = std::collections::HashSet::new();
    for (_, key) in vars
        .kv_ext_vars
        .iter()
        .chain(vars.kv_ext_codes.iter())
        .chain(vars.kv_tla_vars.iter())
        .chain(vars.kv_tla_codes.iter())
    {
        keys.insert(key.clone());
    }
    keys.into_iter().collect()
}

async fn fetch_kv_entry(
    client: &mut crate::structs::manager_client::ManagerClient<tonic::transport::Channel>,
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

async fn resolve_kv_jsonnet_vars_from_manager(
    client: &mut crate::structs::manager_client::ManagerClient<tonic::transport::Channel>,
    vars: JsonnetVars,
) -> R<JsonnetVars> {
    let mut cache = HashMap::new();
    for key in kv_keys_from_vars(&vars) {
        let entry = fetch_kv_entry(client, &key).await?;
        cache.insert(key, entry);
    }

    resolve_kv_jsonnet_vars(vars, |key| {
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
    _temp_dir: TempDir,
    path: PathBuf,
}

fn resolve_jobspec_source(source: &str) -> R<ResolvedJobspec> {
    let temp_dir = tempfile::tempdir().context("create temporary directory for jobspec")?;
    let client = GetterClient::default();
    let fetched_path = client
        .get(source, temp_dir.path())
        .with_context(|| format!("fetch jobspec from {source}"))?;
    let resolved_path = ensure_jobspec_file(source, &fetched_path)?;

    Ok(ResolvedJobspec {
        _temp_dir: temp_dir,
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

fn render_jobspec(job_path: &Path, vars: &JsonnetVars) -> R<(String, Vec<Submission>)> {
    let mut vm = JsonnetVm::new();
    let mut submissions = vec![];
    let job_path_str = job_path.to_string_lossy();

    let content = std::fs::read_to_string(job_path)?;
    let initial_sub = Submission {
        path: job_path_str.to_string(),
        content,
        is_primary: true,
    };
    submissions.push(initial_sub);

    vm.import_callback(|_vm, base, rel| {
        let path = std::path::Path::new(base).join(rel);
        let path_str = path.to_str().unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();

        let sub = Submission {
            path: path_str.to_string(),
            content: contents.to_string(),
            is_primary: false,
        };

        submissions.push(sub);

        Ok((base.into(), contents.to_string()))
    });

    apply_jsonnet_vars(&mut vm, vars);

    let rendered = vm
        .evaluate_file(job_path)
        .map_err(|e| anyhow!("failed to evaluate job spec {}: {e}", job_path.display()))?
        .to_string();

    Ok((rendered, submissions))
}

pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("run", matches)) => run_job(matches).await,
        Some(("stop", matches)) => stop_job(matches).await,
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

    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(GetAllocationLogsRequest {
        job: name.unwrap_or_default(),
        allocation_id,
        stream,
        max_bytes,
        tail,
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
        let task = if alloc.task_name.is_empty() {
            "?".to_string()
        } else {
            alloc.task_name.clone()
        };
        println!(
            "== allocation {} (task {}) on {} ==",
            alloc.allocation_id, task, alloc.node_id
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
            print!("{}", stream.content);
            if !stream.content.ends_with('\n') {
                println!();
            }
        }
    }
    Ok(())
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
    fn jobspec_schema_embeds_task_kinds() {
        let schema = build_jobspec_schema();
        assert_eq!(schema["title"], "Legion Jobspec");
        let kinds = schema["taskKinds"].as_object().expect("taskKinds object");
        // Built-in kinds must be present and carry real derived schemas.
        assert!(kinds.contains_key("exec"));
        assert!(kinds.contains_key("container"));
        assert!(kinds["exec"]["properties"].is_object());
    }

    #[test]
    fn validate_and_schema_subcommands_parse() {
        let cmd = config();
        assert!(cmd
            .clone()
            .try_get_matches_from(["job", "validate", "spec.jsonnet", "--json"])
            .is_ok());
        assert!(cmd
            .clone()
            .try_get_matches_from(["job", "wait", "demo", "--timeout", "5"])
            .is_ok());
        assert!(cmd
            .try_get_matches_from([
                "job",
                "run",
                "x.jsonnet",
                "--wait",
                "--idempotency-key",
                "abc"
            ])
            .is_ok());
    }

    #[test]
    fn kv_vars_render_into_jobspec() -> R<()> {
        let dir = tempdir()?;
        let job_path = dir.path().join("job.jsonnet");
        std::fs::write(
            &job_path,
            r#"
            {
              name: std.extVar("job_name"),
              task: {
                kind: "noop",
              },
            }
            "#,
        )?;

        let mut vars = JsonnetVars::default();
        vars.kv_ext_vars
            .push(("job_name".into(), "jobs/name".into()));

        let resolved = resolve_kv_jsonnet_vars(vars, |key| {
            Ok(KvEntry {
                key: key.to_string(),
                value: "kv-job".into(),
                value_type: KvValueType::String as i32,
                version: 1,
                created_at: "now".into(),
                updated_at: "now".into(),
            })
        })?;

        let (rendered, _subs) = render_jobspec(&job_path, &resolved)?;
        let config: JobConfig = serde_json::from_str(&rendered)?;
        assert_eq!(config.name, "kv-job");
        Ok(())
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
    let vars = jsonnet_vars_from_matches(arg_matches)?;
    let vars = resolve_kv_jsonnet_vars_from_manager(&mut client, vars).await?;
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
        let printable_allocations: Vec<JobPlanPrint> =
            allocations.into_iter().map(Into::into).collect();
        if printable_allocations.is_empty() {
            println!("No allocations planned");
        } else {
            print_table(printable_allocations);
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
        .map_or("./configs/jobs/demo.jsonnet", String::as_str);
    let priority = priority_from_matches(arg_matches);
    let idempotency_key = arg_matches.get_one::<String>("idempotency-key").cloned();
    let vars = jsonnet_vars_from_matches(arg_matches)?;
    let vars = resolve_kv_jsonnet_vars_from_manager(&mut client, vars).await?;
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
