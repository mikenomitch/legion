use chrono::Utc;
use serde::ser::SerializeStruct;
use serde::{de::Error, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::time::Duration;
use tabled::Tabled;

use crate::prelude::{anyhow, bail, R};
use crate::volumes::VolumeMode;
use tracing::warn;

use super::artifact::{validate_task_artifacts, ArtifactSpec};
use super::band::Band;
use super::instance::InstanceMode;
use super::spec_fields::{
    extension_point, spec_block, ExtensionPoint, FieldWalk, PendingExtensions, UnknownFieldMode,
    UnknownFields, UnknownFieldsRecord,
};
use super::workload_identity::WorkloadIdentity;
use super::{Job, PlannedAllocation, Submission};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct JobPrint {
    pub name: String,
    pub status: String,
    /// Priority band, so contention outcomes are legible from the job list
    /// rather than only from the spec.
    pub priority: String,
    pub created_at: String,
    pub updated_at: String,
}

/// Render a job timestamp (unix seconds on the wire) for the human table.
///
/// The table used to print the raw epoch integer, which is not a timestamp
/// anyone can read at 3am. `--json` is unaffected: it serializes the proto
/// message, which still carries the integer.
fn format_job_timestamp(unix_seconds: i64) -> String {
    chrono::DateTime::from_timestamp(unix_seconds, 0)
        .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| unix_seconds.to_string())
}

impl From<Job> for JobPrint {
    fn from(job: Job) -> Self {
        JobPrint {
            name: job.name,
            status: job.status,
            // A manager that predates bands sends nothing; render the
            // default rather than a blank cell, because that is what such a
            // cluster's scheduler is actually doing.
            priority: if job.priority.is_empty() {
                Band::default().as_str().to_string()
            } else {
                job.priority
            },
            created_at: format_job_timestamp(job.created_at),
            updated_at: format_job_timestamp(job.updated_at),
        }
    }
}

/// The job table for a list that spans namespaces (`legion job list -A`):
/// [`JobPrint`] with a leading NAMESPACE column. A single-namespace list
/// keeps the narrower table, so a single-team cluster never sees one.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct JobPrintNamespaced {
    pub namespace: String,
    pub name: String,
    pub status: String,
    pub priority: String,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Job> for JobPrintNamespaced {
    fn from(job: Job) -> Self {
        let namespace = if job.namespace.is_empty() {
            crate::structs::namespace::job_namespace(&job.id).to_string()
        } else {
            job.namespace.clone()
        };
        let print = JobPrint::from(job);
        JobPrintNamespaced {
            namespace,
            name: print.name,
            status: print.status,
            priority: print.priority,
            created_at: print.created_at,
            updated_at: print.updated_at,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct JobPlanPrint {
    pub node_id: String,
    pub task_config_id: i64,
    pub task_name: String,
}

impl From<PlannedAllocation> for JobPlanPrint {
    fn from(allocation: PlannedAllocation) -> Self {
        JobPlanPrint {
            node_id: allocation.node_id,
            task_config_id: allocation.task_config_id,
            task_name: allocation.task_name,
        }
    }
}

fn empty_object() -> Value {
    Value::Object(Default::default())
}

fn default_task_count() -> TaskCount {
    TaskCount::default()
}

pub fn default_task_group() -> String {
    "default".to_string()
}

fn default_task_lifecycle() -> TaskLifecycle {
    TaskLifecycle::Service
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskSchedule {
    Cron(String),
    Every(Duration),
}

impl TaskSchedule {
    pub fn next_delay(&self) -> Result<Duration, String> {
        match self {
            TaskSchedule::Every(duration) => Ok(*duration),
            TaskSchedule::Cron(expression) => {
                let schedule = cron::Schedule::from_str(expression)
                    .map_err(|err| format!("Invalid cron expression {expression}: {err}"))?;
                let now = Utc::now();
                let next = schedule
                    .upcoming(Utc)
                    .next()
                    .ok_or_else(|| format!("Cron expression {expression} has no future run"))?;
                let delay = (next - now).to_std().unwrap_or(Duration::from_secs(0));
                Ok(delay)
            }
        }
    }
}

impl std::fmt::Display for TaskSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskSchedule::Cron(expr) => write!(f, "{expr}"),
            TaskSchedule::Every(duration) => {
                write!(f, "every {}", humantime::format_duration(*duration))
            }
        }
    }
}

impl std::str::FromStr for TaskSchedule {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_schedule_text(value)
    }
}

impl Serialize for TaskSchedule {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            TaskSchedule::Cron(expr) => serializer.serialize_str(expr),
            TaskSchedule::Every(duration) => serializer
                .serialize_str(&format!("every {}", humantime::format_duration(*duration))),
        }
    }
}

impl<'de> Deserialize<'de> for TaskSchedule {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        parse_schedule_value(&value).map_err(D::Error::custom)
    }
}

/// Refuse a key a hand-parsed value shorthand (`schedule`, `count`) does not
/// know, with the closest known key as a suggestion. These shorthands have
/// no capture map, so they are strict even under `allow_unknown_fields`.
fn reject_unknown_keys(
    what: &str,
    map: &serde_json::Map<String, Value>,
    known: &[&'static str],
) -> Result<(), String> {
    let Some(key) = map.keys().find(|key| !known.contains(&key.as_str())) else {
        return Ok(());
    };
    let hint = crate::structs::spec_fields::suggest(key, known)
        .map(|known| format!(" (did you mean `{what}.{known}`?)"))
        .unwrap_or_default();
    let expected: Vec<String> = known.iter().map(|key| format!("`{key}`")).collect();
    Err(format!(
        "unknown field `{what}.{key}`{hint}; {what} takes {}",
        expected.join(" or ")
    ))
}

fn parse_schedule_value(value: &Value) -> Result<TaskSchedule, String> {
    match value {
        Value::String(text) => parse_schedule_text(text),
        Value::Object(map) => {
            reject_unknown_keys("schedule", map, &["cron", "every"])?;
            if let Some(cron_value) = map.get("cron") {
                let cron_text = cron_value
                    .as_str()
                    .ok_or_else(|| "schedule.cron must be a string".to_string())?;
                return parse_schedule_text(cron_text);
            }
            if let Some(every_value) = map.get("every") {
                let duration = parse_duration_value(every_value)?;
                if duration.is_zero() {
                    return Err("schedule.every must be greater than 0".to_string());
                }
                return Ok(TaskSchedule::Every(duration));
            }
            Err("schedule object must include cron or every".to_string())
        }
        _ => Err("schedule must be a string or object".to_string()),
    }
}

fn parse_schedule_text(text: &str) -> Result<TaskSchedule, String> {
    let schedule_text = text.trim();
    if let Some(every) = schedule_text.strip_prefix("every ") {
        let duration = humantime::parse_duration(every)
            .map_err(|err| format!("Invalid schedule duration {every}: {err}"))?;
        if duration.is_zero() {
            return Err("schedule duration must be greater than 0".to_string());
        }
        return Ok(TaskSchedule::Every(duration));
    }

    if let Ok(duration) = humantime::parse_duration(schedule_text) {
        if duration.is_zero() {
            return Err("schedule duration must be greater than 0".to_string());
        }
        return Ok(TaskSchedule::Every(duration));
    }

    cron::Schedule::from_str(schedule_text)
        .map_err(|err| format!("Invalid schedule value {schedule_text}: {err}"))?;
    Ok(TaskSchedule::Cron(schedule_text.to_string()))
}

fn default_dependency_condition() -> TaskDependencyCondition {
    TaskDependencyCondition::Start
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCount {
    pub num: u32,
    pub per: Option<String>,
}

impl TaskCount {
    pub fn new(num: u32) -> Self {
        Self { num, per: None }
    }

    pub fn per(num: u32, per: impl Into<String>) -> Self {
        Self {
            num,
            per: Some(per.into()),
        }
    }
}

impl Default for TaskCount {
    fn default() -> Self {
        Self { num: 1, per: None }
    }
}

impl std::fmt::Display for TaskCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.per {
            Some(per) => write!(f, "{} per {}", self.num, per),
            None => write!(f, "{}", self.num),
        }
    }
}

impl Serialize for TaskCount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.per {
            Some(per) => {
                let mut state = serializer.serialize_struct("TaskCount", 2)?;
                state.serialize_field("num", &self.num)?;
                state.serialize_field("per", per)?;
                state.end()
            }
            None => serializer.serialize_u32(self.num),
        }
    }
}

/// `count` is a whole number, or `{ num, per }` for spread placement.
/// Parsed by hand rather than as an untagged enum, so a wrong shape says
/// what `count` takes instead of "did not match any variant", and an
/// unknown key in the object form is an error.
impl<'de> Deserialize<'de> for TaskCount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let expected = "count must be a whole number, or { num = <count>, per = \"<attribute>\" }";
        let whole = |value: &Value| value.as_u64().and_then(|num| u32::try_from(num).ok());
        match Value::deserialize(deserializer)? {
            value @ Value::Number(_) => whole(&value)
                .map(TaskCount::new)
                .ok_or_else(|| D::Error::custom(format!("{expected}, got {value}"))),
            Value::Object(map) => {
                reject_unknown_keys("count", &map, &["num", "per"]).map_err(D::Error::custom)?;
                let num = map.get("num").and_then(whole).ok_or_else(|| {
                    D::Error::custom(format!(
                        "{expected}: count.num is missing or not a whole number"
                    ))
                })?;
                let per = map.get("per").and_then(Value::as_str).ok_or_else(|| {
                    D::Error::custom(format!("{expected}: count.per is missing or not a string"))
                })?;
                Ok(TaskCount::per(num, per))
            }
            other => Err(D::Error::custom(format!("{expected}, got {other}"))),
        }
    }
}

fn default_check_every() -> Duration {
    Duration::from_secs(10)
}

fn default_check_timeout() -> Duration {
    Duration::from_secs(20)
}

fn default_unhealthy_after_count() -> u32 {
    1
}

#[allow(dead_code)]
fn default_on_unhealthy() -> TaskOnUnhealthyConfig {
    TaskOnUnhealthyConfig::default()
}

fn default_restart_count() -> u32 {
    3
}

fn default_restart_period() -> Duration {
    Duration::from_secs(30 * 60)
}

fn default_restart_delay() -> Duration {
    Duration::from_secs(30)
}

fn default_move_period() -> Duration {
    Duration::from_secs(30 * 60)
}

fn default_move_delay() -> Duration {
    Duration::from_secs(30)
}

fn serialize_duration<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&humantime::format_duration(*duration).to_string())
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    parse_duration_value(&value).map_err(serde::de::Error::custom)
}

fn deserialize_optional_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    if value.is_null() {
        return Ok(None);
    }
    parse_duration_value(&value)
        .map(Some)
        .map_err(serde::de::Error::custom)
}

fn serialize_optional_duration<S>(
    duration: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match duration {
        Some(duration) => {
            serializer.serialize_str(&humantime::format_duration(*duration).to_string())
        }
        None => serializer.serialize_none(),
    }
}

fn parse_duration_value(value: &Value) -> Result<Duration, String> {
    match value {
        Value::Number(num) => {
            let secs = num
                .as_u64()
                .ok_or_else(|| "Duration value must be a positive integer".to_string())?;
            Ok(Duration::from_secs(secs))
        }
        Value::String(text) => {
            humantime::parse_duration(text).map_err(|err| format!("Invalid duration {text}: {err}"))
        }
        _ => Err("Duration must be a string or number of seconds".to_string()),
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TaskCheckType {
    Health,
    Readiness,
    Boot,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TaskCheckKind {
    Http,
    Tcp,
    Exec,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TaskDependencyCondition {
    Start,
    Complete,
    Success,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDependency {
    pub task: String,
    #[serde(default = "default_dependency_condition")]
    pub condition: TaskDependencyCondition,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum TaskLifecycle {
    #[default]
    Service,
    Batch,
    Sidecar,
}

// Serialized (same spelling as `as_str`) only to cross a forwarded lane
// write (`storage::raft::forward`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatusState {
    Pending,
    Running,
    Complete,
    Success,
    Failed,
}

impl TaskStatusState {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatusState::Pending => "pending",
            TaskStatusState::Running => "running",
            TaskStatusState::Complete => "complete",
            TaskStatusState::Success => "success",
            TaskStatusState::Failed => "failed",
        }
    }

    pub fn is_started(self) -> bool {
        matches!(
            self,
            TaskStatusState::Running
                | TaskStatusState::Complete
                | TaskStatusState::Success
                | TaskStatusState::Failed
        )
    }

    pub fn is_complete(self) -> bool {
        matches!(self, TaskStatusState::Complete | TaskStatusState::Success)
    }

    pub fn is_success(self) -> bool {
        matches!(self, TaskStatusState::Success)
    }
}

impl std::str::FromStr for TaskStatusState {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(TaskStatusState::Pending),
            "running" => Ok(TaskStatusState::Running),
            "complete" => Ok(TaskStatusState::Complete),
            "success" => Ok(TaskStatusState::Success),
            "failed" => Ok(TaskStatusState::Failed),
            other => Err(format!("Unknown task status {other}")),
        }
    }
}

impl std::fmt::Display for TaskLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            TaskLifecycle::Service => "service",
            TaskLifecycle::Batch => "batch",
            TaskLifecycle::Sidecar => "sidecar",
        };
        write!(f, "{value}")
    }
}

impl std::str::FromStr for TaskLifecycle {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "service" => Ok(TaskLifecycle::Service),
            "batch" => Ok(TaskLifecycle::Batch),
            "sidecar" => Ok(TaskLifecycle::Sidecar),
            other => Err(format!("Unknown lifecycle {other}")),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskCheck {
    #[serde(rename = "type")]
    pub check_type: TaskCheckType,
    #[serde(default)]
    pub name: Option<String>,
    pub kind: TaskCheckKind,
    #[serde(default)]
    pub path: Option<String>,
    /// Port number, or the name of a `network.ports` entry to resolve at
    /// runtime (supports dynamic ports).
    #[serde(default)]
    pub port: Option<crate::structs::network::CheckPort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,
    #[serde(
        default = "default_check_every",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub every: Duration,
    #[serde(
        default = "default_check_timeout",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub timeout: Duration,
    #[serde(default = "default_unhealthy_after_count")]
    pub unhealthy_after_count: u32,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration"
    )]
    pub boot_grace_time: Option<Duration>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration"
    )]
    pub unhealthy_after_count_timespan: Option<Duration>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskOnUnhealthyConfig {
    #[serde(default = "default_restart_count")]
    pub restart_count: u32,
    #[serde(
        default = "default_restart_period",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub restart_period: Duration,
    #[serde(
        default = "default_restart_delay",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub restart_delay: Duration,
    #[serde(default)]
    pub move_count: Option<u32>,
    #[serde(
        default = "default_move_period",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub move_period: Duration,
    #[serde(
        default = "default_move_delay",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub move_delay: Duration,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

/// What to do when a task's process exits (see
/// `internal_docs/plans/host-process-supervision-plan.md`). This is the
/// exit-status half of restart handling; health-check failures continue to be
/// governed by [`TaskOnUnhealthyConfig`], and both share one restart budget.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum RestartPolicy {
    /// Restart on any exit, successful or not. Default for `service` and
    /// `sidecar` tasks: a service that exits is not doing its job.
    #[default]
    Always,
    /// Restart only when the exit status is not in `successExitCodes`.
    /// Default for `batch` tasks.
    OnFailure,
    /// Never restart. The allocation reaches a terminal state on first exit.
    Never,
}

fn default_success_exit_codes() -> Vec<i32> {
    vec![0]
}

/// Task `restart` block: how the worker reacts to the task process exiting.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRestartConfig {
    /// `always` | `onFailure` | `never`. `None` (the key omitted) is the
    /// lifecycle default — see [`RestartPolicy`] and
    /// [`TaskConfig::restart_policy`] — so a block that only tunes `delay`
    /// or `successExitCodes` does not quietly turn a batch task into one
    /// that restarts on success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<RestartPolicy>,
    /// Wait before relaunching. Defaults to `on_unhealthy.restart_delay` when
    /// unset, so a task that already tuned its restart pacing keeps it.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration"
    )]
    pub delay: Option<Duration>,
    /// Exit codes counted as success. Default `[0]`.
    #[serde(default = "default_success_exit_codes")]
    pub success_exit_codes: Vec<i32>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

impl Default for TaskRestartConfig {
    fn default() -> Self {
        Self {
            policy: None,
            delay: None,
            success_exit_codes: default_success_exit_codes(),
            unknown_fields: Default::default(),
        }
    }
}

impl Default for TaskOnUnhealthyConfig {
    fn default() -> Self {
        Self {
            restart_count: default_restart_count(),
            restart_period: default_restart_period(),
            restart_delay: default_restart_delay(),
            move_count: None,
            move_period: default_move_period(),
            move_delay: default_move_delay(),
            unknown_fields: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RolloutProgression {
    Auto,
    Manual,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RolloutReplacement {
    DestroyFirst,
    CreateFirst,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RolloutCapacityTimeoutAction {
    Rollback,
    Fail,
    CompleteIncomplete,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RolloutDeadlineAction {
    Rollback,
    Fail,
    Pause,
}

/// How a slot-mode stateful group's slots are replaced during a rollout
/// (see `docs/stateful-workloads.md` "Ordered rollouts for slot pools").
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RolloutSlotOrder {
    /// Today's behavior (the default): slot replacements are driven by the
    /// regular step machinery with no ordinal ordering.
    Parallel,
    /// One slot at a time, highest ordinal first (`count-1 .. 0`), each step
    /// gated on the previous slot's replacement passing the rollout's health
    /// criteria. The K8s StatefulSet `OrderedReady` analog for updates.
    Sequential,
}

/// Ordinal-aware rollout options for groups with a slot-mode stateful task
/// (`rollout.slots` / `rolloutByGroup.<group>.slots` in the jobspec).
///
/// Only valid for groups whose stateful task is slot mode; rejected at
/// submit time otherwise. A group override replaces the global `slots`
/// block as a unit (like the step-mode fields, the block is one coherent
/// progression choice).
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RolloutSlotsConfig {
    /// `'parallel'` (default) or `'sequential'` (one slot at a time, highest
    /// first, health-gated). `sequential` defines its own one-slot-per-step
    /// progression and therefore cannot be combined with the step-mode
    /// fields (`percentEachStep`, `countEachStep`, `percentSteps`,
    /// `countSteps`) in the same scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<RolloutSlotOrder>,
    /// Staged ("partitioned") rollout floor: only slots `>= updateFrom` are
    /// updated to the new config; slots `0..updateFrom` keep running their
    /// current allocations untouched. Resubmitting with a lower value
    /// extends the new config to more slots; `0` (or omitting the field)
    /// updates everything. Requires `order: 'sequential'` and must be less
    /// than the slot task's `count.num`. The K8s StatefulSet
    /// `rollingUpdate.partition` analog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_from: Option<u32>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

impl RolloutSlotsConfig {
    /// Whether this block asks for ordered (one-slot-at-a-time) updates.
    pub fn is_sequential(&self) -> bool {
        self.order == Some(RolloutSlotOrder::Sequential)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RolloutConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progression: Option<RolloutProgression>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement: Option<RolloutReplacement>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_place: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percent_each_step: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count_each_step: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percent_steps: Option<Vec<u32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count_steps: Option<Vec<u32>>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration"
    )]
    pub step_stagger: Option<Duration>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration"
    )]
    pub allocation_stagger: Option<Duration>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration"
    )]
    pub min_healthy_time: Option<Duration>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration"
    )]
    pub healthy_deadline: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_health_checks: Option<bool>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration"
    )]
    pub capacity_block_timeout: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_capacity_timeout: Option<RolloutCapacityTimeoutAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_on_failure: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_health_deadline: Option<RolloutDeadlineAction>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration"
    )]
    pub batch_kill_after: Option<Duration>,
    /// Ordinal-aware options for slot-mode stateful groups: ordered
    /// (`order: 'sequential'`) and staged (`updateFrom`) slot replacement.
    /// See [`RolloutSlotsConfig`] and docs/stateful-workloads.md.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slots: Option<RolloutSlotsConfig>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

impl RolloutConfig {
    fn step_mode_count(&self) -> usize {
        usize::from(self.percent_each_step.is_some())
            + usize::from(self.count_each_step.is_some())
            + usize::from(self.percent_steps.is_some())
            + usize::from(self.count_steps.is_some())
    }

    fn has_step_mode(&self) -> bool {
        self.step_mode_count() > 0
    }

    /// Whether the config asks for ordered one-slot-at-a-time updates
    /// (`slots.order: 'sequential'`). Sequential is a progression mode of
    /// its own and is mutually exclusive with the step-mode fields.
    pub(crate) fn has_sequential_slots(&self) -> bool {
        self.slots
            .as_ref()
            .is_some_and(RolloutSlotsConfig::is_sequential)
    }

    fn clear_step_modes(&mut self) {
        self.percent_each_step = None;
        self.count_each_step = None;
        self.percent_steps = None;
        self.count_steps = None;
    }

    fn apply_override(&mut self, override_config: &RolloutConfig) {
        if override_config.strategy.is_some() {
            self.strategy = override_config.strategy.clone();
        }
        if override_config.progression.is_some() {
            self.progression = override_config.progression.clone();
        }
        if override_config.replacement.is_some() {
            self.replacement = override_config.replacement.clone();
        }
        if override_config.in_place.is_some() {
            self.in_place = override_config.in_place;
        }
        // A group override that picks a progression mode replaces the global
        // selection wholesale: step modes clear a global step mode AND a
        // global sequential-slots choice; a sequential `slots` block clears
        // the global step modes. This keeps the merged config from ever
        // mixing two progression modes the group never asked for together.
        if override_config.has_step_mode() {
            self.clear_step_modes();
            if self.has_sequential_slots() {
                self.slots = None;
            }
        }
        if override_config.has_sequential_slots() {
            self.clear_step_modes();
        }
        if override_config.slots.is_some() {
            self.slots = override_config.slots.clone();
        }
        if override_config.percent_each_step.is_some() {
            self.percent_each_step = override_config.percent_each_step;
        }
        if override_config.count_each_step.is_some() {
            self.count_each_step = override_config.count_each_step;
        }
        if override_config.percent_steps.is_some() {
            self.percent_steps = override_config.percent_steps.clone();
        }
        if override_config.count_steps.is_some() {
            self.count_steps = override_config.count_steps.clone();
        }
        if override_config.step_stagger.is_some() {
            self.step_stagger = override_config.step_stagger;
        }
        if override_config.allocation_stagger.is_some() {
            self.allocation_stagger = override_config.allocation_stagger;
        }
        if override_config.min_healthy_time.is_some() {
            self.min_healthy_time = override_config.min_healthy_time;
        }
        if override_config.healthy_deadline.is_some() {
            self.healthy_deadline = override_config.healthy_deadline;
        }
        if override_config.ignore_health_checks.is_some() {
            self.ignore_health_checks = override_config.ignore_health_checks;
        }
        if override_config.capacity_block_timeout.is_some() {
            self.capacity_block_timeout = override_config.capacity_block_timeout;
        }
        if override_config.on_capacity_timeout.is_some() {
            self.on_capacity_timeout = override_config.on_capacity_timeout.clone();
        }
        if override_config.rollback_on_failure.is_some() {
            self.rollback_on_failure = override_config.rollback_on_failure;
        }
        if override_config.on_health_deadline.is_some() {
            self.on_health_deadline = override_config.on_health_deadline.clone();
        }
        if override_config.batch_kill_after.is_some() {
            self.batch_kill_after = override_config.batch_kill_after;
        }
    }

    fn validate(&self, scope: &str) -> R<()> {
        if self.step_mode_count() > 1 {
            bail!(
                "{} defines conflicting step modes; set only one of percentEachStep, countEachStep, percentSteps, countSteps",
                scope
            );
        }

        if let Some(percent_each_step) = self.percent_each_step {
            if percent_each_step == 0 || percent_each_step > 100 {
                bail!("{scope}.percentEachStep must be between 1 and 100");
            }
        }

        if let Some(count_each_step) = self.count_each_step {
            if count_each_step == 0 {
                bail!("{scope}.countEachStep must be at least 1");
            }
        }

        if let Some(percent_steps) = self.percent_steps.as_ref() {
            if percent_steps.is_empty() {
                bail!("{scope}.percentSteps must not be empty");
            }

            let mut previous = 0;
            for step in percent_steps {
                if *step == 0 || *step > 100 {
                    bail!("{scope}.percentSteps values must be between 1 and 100");
                }
                if *step <= previous {
                    bail!("{scope}.percentSteps must be strictly increasing");
                }
                previous = *step;
            }

            if percent_steps.last().copied() != Some(100) {
                bail!("{scope}.percentSteps must end with 100");
            }
        }

        if let Some(count_steps) = self.count_steps.as_ref() {
            if count_steps.is_empty() {
                bail!("{scope}.countSteps must not be empty");
            }
            if count_steps.contains(&0) {
                bail!("{scope}.countSteps values must be at least 1");
            }
        }

        if self
            .strategy
            .as_ref()
            .is_some_and(|strategy| strategy.trim().is_empty())
        {
            bail!("{scope}.strategy cannot be empty");
        }

        if self
            .capacity_block_timeout
            .as_ref()
            .is_some_and(Duration::is_zero)
        {
            bail!("{scope}.capacityBlockTimeout must be greater than 0 when set");
        }

        if self
            .healthy_deadline
            .as_ref()
            .is_some_and(Duration::is_zero)
        {
            bail!("{scope}.healthyDeadline must be greater than 0 when set");
        }

        if self.has_sequential_slots() && self.has_step_mode() {
            bail!(
                "{scope} cannot combine slots.order: \"sequential\" with a step mode \
                 (percentEachStep, countEachStep, percentSteps, countSteps); sequential slot \
                 rollouts define their own one-slot-per-step progression"
            );
        }

        if let Some(slots) = self.slots.as_ref() {
            if slots.update_from.is_some() && !slots.is_sequential() {
                bail!(
                    "{scope}.slots.updateFrom requires slots.order: \"sequential\"; staged \
                     rollouts only have defined semantics when slots are replaced in ordinal \
                     order"
                );
            }
        }

        Ok(())
    }
}

/// Declares a task stateful: its replicas become stable *instances* that
/// survive reschedules (see `docs/stateful-workloads.md`). Absent = today's
/// stateless behavior; the feature is opt-in per task.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StatefulConfig {
    /// Instance id assignment mode: `slot` (fixed ordinal pool `0..count-1`,
    /// ids reused within the pool — the default) or `keyed` (created on demand
    /// via the CreateInstance/DestroyInstance RPCs, ids never recycled). In
    /// keyed mode `count.num` is ignored: the set of live instances created
    /// via the API is the desired allocation count.
    ///
    /// Defaults to `slot`, so the minimal stateful task is just
    /// `stateful: { storage: { path: '/data' } }`. Keyed mode changes who owns
    /// cardinality (the instance API, not the spec), so it is always explicit.
    #[serde(default)]
    pub mode: InstanceMode,
    /// Keyed mode only: reject creates that don't supply an instance id
    /// (`requireId` in the jobspec). Default false, where the server mints
    /// an id when the request omits one. A valid caller-supplied id is
    /// always honored either way. Slot ids are always derived from the
    /// ordinal, so slot mode rejects it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub require_id: bool,
    /// Keyed mode only: instance ids to SEED on job submit
    /// (`initialInstances` in the jobspec).
    ///
    /// On every submit, after the config is persisted and before placement
    /// is evaluated, the manager ensures each listed id exists: ids that
    /// have NEVER existed are created (`pending`) and flow into the submit's
    /// normal placement eval; ids that already exist in ANY state — live,
    /// stopped, or a terminated tombstone — are skipped silently. Resubmits
    /// are therefore idempotent, and a deliberately destroyed id is never
    /// resurrected by a resubmit.
    ///
    /// This is a SEED list, not a reconciled desired set: removing an id
    /// from the list on resubmit does not stop or destroy that instance —
    /// only `legion instance stop/destroy` (or a job stop/destroy) does.
    ///
    /// Each id must match `[A-Za-z0-9._-]{1,128}` and the list must not
    /// contain duplicates (both enforced at validation time). Invalid in
    /// slot mode (slot ids are derived from ordinals).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_instances: Option<Vec<String>>,
    /// Optional persistent storage bound to each instance (contract only in
    /// v1; drivers are a follow-up, see
    /// `internal_docs/plans/volume-plugin-system-plan.md`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StatefulStorageConfig>,
    /// Node-loss fencing policy (`onLost`): worker self-fence window plus
    /// the manager's loss-confirmation gate. Absent = the safe defaults
    /// (`selfTerminateAfter: 90s`, `replaceAfter: 2 x selfTerminateAfter`)
    /// — see [`StatefulOnLostConfig`] and docs/stateful-workloads.md
    /// "Node loss and fencing".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_lost: Option<StatefulOnLostConfig>,
    /// Session mode only: the per-node capacity reservation this task's
    /// unnamed sessions run inside (`stateful.envelope`; see
    /// docs/session-mode.md). Its presence on a stored keyed task is what
    /// marks the task a session task: parsing normalizes
    /// `mode = "session"` to `keyed` and keeps this block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope: Option<crate::structs::session::SessionEnvelopeConfig>,
    /// Session mode only: when a session becomes a keyed instance
    /// (`stateful.promote`). Absent = the defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promote: Option<crate::structs::session::SessionPromoteConfig>,
    /// Session mode only: how a manager's session router picks the node a
    /// session starts on (`stateful.placement`): `"spread"` (the default,
    /// the node with the most room left) or `"fast"` (the roomier of two
    /// random nodes with room, constant cost at any fleet size). See
    /// docs/session-mode.md, "Picking a node".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<crate::structs::session::SessionPlacement>,
    /// Session tasks with `suspend.durability = "store"` only (session
    /// durability plan §6.7, mechanism 4): how long a node that has reached
    /// neither a manager nor its content store keeps the task's sessions
    /// running. Past it the node suspends them on its own disk and stops
    /// serving them, because a newer generation may be running elsewhere;
    /// once it reconnects it resumes only the ones the content store says
    /// it still owns. A duration such as `"2m"` (the default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fence_after: Option<String>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

/// Default worker self-fence window for stateful tasks (see
/// [`StatefulOnLostConfig::self_terminate_after`]). Applies whenever a
/// stateful task declares no `onLost` block — fencing is on by default.
pub const DEFAULT_SELF_TERMINATE_AFTER: Duration = Duration::from_secs(90);

/// Fencing policy for manager<->worker partitions (the at-most-one
/// guarantee; see docs/stateful-workloads.md "Node loss and fencing").
///
/// Two cooperating timers make a node loss *confirmable*: the worker kills
/// its stateful allocations after losing manager contact for
/// `selfTerminateAfter`, and the manager refuses to plan a replacement until
/// the node has been lost for `replaceAfter` (> `selfTerminateAfter`). By
/// the time the replacement binds the instance id and storage handle, the
/// original allocation has provably self-fenced or its node is truly dead.
///
/// Every stateful task gets this policy — an absent block means the defaults
/// below (safe by default; tune `selfTerminateAfter` down for faster
/// failover).
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StatefulOnLostConfig {
    /// How long the worker tolerates failed heartbeats before killing this
    /// task's allocations locally (humantime string, e.g. `"90s"`). Default
    /// `"90s"`. Must be greater than zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub self_terminate_after: Option<String>,
    /// How long a node must have been lost before the manager replaces this
    /// task's allocations on it (humantime string, e.g. `"3m"`), or the
    /// special value `"never"` (K8s StatefulSet-style: the allocation is
    /// only freed when the node returns or an operator forces it via
    /// ReplaceInstance). Default: `2 x selfTerminateAfter`. Must be greater
    /// than `selfTerminateAfter` — otherwise a replacement could start
    /// before the worker has provably self-fenced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace_after: Option<String>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

/// Resolved manager-side replacement gate for allocations on a lost node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaceAfter {
    /// Replace once the node has been lost longer than this.
    After(Duration),
    /// Never auto-replace; node recovery or operator override required.
    Never,
}

/// [`StatefulOnLostConfig`] with defaults applied and durations parsed — the
/// only shape the manager gate and the dispatcher reason about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedOnLostPolicy {
    pub self_terminate_after: Duration,
    pub replace_after: ReplaceAfter,
}

impl StatefulOnLostConfig {
    /// Parse and default the policy. Errors carry actionable messages and
    /// surface at `job validate`/submit time via
    /// [`TaskConfig::validate_stateful`].
    pub fn resolve(&self) -> R<ResolvedOnLostPolicy> {
        let self_terminate_after = match self.self_terminate_after.as_deref() {
            None => DEFAULT_SELF_TERMINATE_AFTER,
            Some(raw) => humantime::parse_duration(raw).map_err(|err| {
                anyhow::anyhow!(
                    "stateful.onLost.selfTerminateAfter {raw:?} is not a valid duration \
                     (use e.g. \"90s\"): {err}"
                )
            })?,
        };
        if self_terminate_after.is_zero() {
            bail!("stateful.onLost.selfTerminateAfter must be greater than 0");
        }

        let replace_after = match self.replace_after.as_deref() {
            None => ReplaceAfter::After(self_terminate_after * 2),
            Some("never") => ReplaceAfter::Never,
            Some(raw) => {
                let parsed = humantime::parse_duration(raw).map_err(|err| {
                    anyhow::anyhow!(
                        "stateful.onLost.replaceAfter {raw:?} is not a valid duration \
                         (use e.g. \"3m\", or the special value \"never\"): {err}"
                    )
                })?;
                if parsed <= self_terminate_after {
                    bail!(
                        "stateful.onLost.replaceAfter ({raw}) must be greater than \
                         selfTerminateAfter ({}): the manager must wait until the worker has \
                         provably self-fenced before re-binding the instance, otherwise the \
                         workload can run twice. Raise replaceAfter, lower selfTerminateAfter, \
                         or use replaceAfter: \"never\"",
                        humantime::format_duration(self_terminate_after)
                    );
                }
                ReplaceAfter::After(parsed)
            }
        };

        Ok(ResolvedOnLostPolicy {
            self_terminate_after,
            replace_after,
        })
    }
}

impl StatefulConfig {
    /// The effective node-loss fencing policy: the declared `onLost` block,
    /// or the safe defaults when absent. Stateful tasks ALWAYS have one —
    /// only stateless tasks are replaced eagerly.
    pub fn resolved_on_lost(&self) -> R<ResolvedOnLostPolicy> {
        self.on_lost.clone().unwrap_or_default().resolve()
    }
}

/// Persistent storage contract for a stateful task. Each instance gets an
/// opaque `storage_handle` minted on first placement and reattached on every
/// subsequent allocation for that instance, and a `volume` row materialized by
/// the named plugin on whatever node the instance lands on (see
/// `docs/volumes.md`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StatefulStorageConfig {
    /// Where the task sees the storage.
    ///
    /// **container**: the bind-mount destination inside the container's
    /// filesystem (required for container tasks).
    /// **exec**: informational only — an exec task runs in the host mount
    /// namespace, so there is no filesystem view to mount into; read
    /// `$LEGION_STORAGE_DIR` instead. Documented in docs/volumes.md.
    pub path: String,
    /// Volume driver that materializes this storage. Defaults to `local-dir`
    /// (a directory on the worker's disk). The driver must be advertised by at
    /// least one node or the job is rejected at admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver: Option<String>,
    /// How the task consumes it: `filesystem` (a directory — the default, and
    /// what `exec` and `container` want) or `block` (a raw device or disk
    /// image, which is what a microVM kind attaches). Admission rejects a mode
    /// the task kind or the driver cannot serve.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<VolumeMode>,
    /// Driver-specific parameters, passed through verbatim (`size` for
    /// `local-dir`, `server`/`export` for `nfs`, …). The driver declares which
    /// are required; admission names any that are missing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    /// Whether to keep an instance's storage handle when its slot is scaled
    /// in (so a later scale-out reattaches the same data). Defaults to true,
    /// mirroring K8s PVC retention (`whenScaled: Retain`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_on_scale_in: Option<bool>,
    /// Whether to keep slot instances' storage handles when the JOB is
    /// stopped (`legion job stop` — Legion's analog of deleting a
    /// StatefulSet, i.e. K8s `whenDeleted`). Defaults to true (K8s Retain).
    /// With `false`, a job stop still terminates the slot instances and
    /// retains their rows (a later `job run` re-materializes the same
    /// ordinal identities), but their storage handles are DROPPED, so the
    /// revived slots mint fresh handles. Slot mode only: keyed stop/destroy
    /// semantics are fixed per instance (stop retains, destroy drops).
    /// `job destroy` is unaffected — it always purges every instance row
    /// (and handle) with the job, regardless of this flag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_on_stop: Option<bool>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

/// The volume name a task's `stateful.storage` is exposed under
/// (`LEGION_VOLUME_DATA`, plus the `LEGION_STORAGE_DIR` alias).
pub const INSTANCE_VOLUME_NAME: &str = "data";

impl StatefulStorageConfig {
    /// Effective scale-in retention policy (defaults to retain).
    pub fn retains_on_scale_in(&self) -> bool {
        self.retain_on_scale_in.unwrap_or(true)
    }

    /// Effective job-stop retention policy (defaults to retain).
    pub fn retains_on_stop(&self) -> bool {
        self.retain_on_stop.unwrap_or(true)
    }

    /// How the task consumes this storage, defaulting to `filesystem`.
    pub fn volume_mode(&self) -> VolumeMode {
        self.mode.unwrap_or_default()
    }

    /// The driver that materializes this storage, defaulting to `local-dir`.
    pub fn driver_name(&self) -> &str {
        self.driver
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .unwrap_or(crate::volumes::DEFAULT_VOLUME_DRIVER)
    }

    /// Params as a map, tolerating an absent or non-object `params`.
    pub fn param_map(&self) -> BTreeMap<String, Value> {
        volume_param_map(self.params.as_ref())
    }

    /// The wire form the worker mounts: a per-instance volume named
    /// [`INSTANCE_VOLUME_NAME`].
    pub fn as_volume_spec(&self) -> VolumeSpec {
        VolumeSpec {
            name: INSTANCE_VOLUME_NAME.to_string(),
            driver: Some(self.driver_name().to_string()),
            mode: self.mode,
            params: self.params.clone(),
            path: Some(self.path.clone()),
            read_only: None,
            pending_extensions: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

/// Hard ceiling on `stop.killAfter`. Grace may slow a drain; it must never
/// wedge one. This is the K8s PDB lesson inverted — a budget an operator
/// cannot cross is the thing that turns "graceful" into "stuck" — so the
/// window is an operator-owned maximum, not an application-owned one.
pub const MAX_KILL_AFTER: Duration = Duration::from_secs(5 * 60);

/// Ceiling on `stop.drain`. Same reasoning as [`MAX_KILL_AFTER`]: a drain
/// may slow a rollout, never wedge one.
pub const MAX_DRAIN: Duration = Duration::from_secs(5 * 60);

fn default_stop_signal() -> String {
    "SIGTERM".to_string()
}

fn default_kill_after() -> Duration {
    Duration::from_secs(30)
}

/// Signals a task may ask for as its stop notice. Restricted to the portable
/// termination signals: `stop.signal` exists so a task can be told to shut
/// down, not as a general signal-delivery API.
pub const STOP_SIGNALS: &[&str] = &[
    "SIGTERM", "SIGINT", "SIGQUIT", "SIGHUP", "SIGUSR1", "SIGUSR2",
];

/// How this task is stopped, whoever initiates it — `job stop`, a rollout
/// replacement, a drain, a reschedule, a scale-in, or preemption. One ladder
/// for every cause (see `docs/graceful-stop.md`).
///
/// Absent means the defaults below, which are a deliberate behavior change
/// from the pre-1b hard kill: every stop now signals first and waits.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StopConfig {
    /// The notice signal, delivered before the grace window opens. One of
    /// [`STOP_SIGNALS`]; default `SIGTERM`.
    #[serde(default = "default_stop_signal")]
    pub signal: String,
    /// How long the task has after the signal before it is SIGKILLed.
    /// Default `30s`, capped at [`MAX_KILL_AFTER`].
    #[serde(
        default = "default_kill_after",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub kill_after: Duration,
    /// How long the allocation keeps serving *after* it has left the service
    /// catalog and *before* the notice signal is sent, so consumers on other
    /// nodes learn it is gone before it stops accepting
    /// (docs/graceful-stop.md). Applies only to tasks with a `service`
    /// block. Unset: one heartbeat interval of the node running it. `0s`
    /// disables. Capped at [`MAX_DRAIN`].
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration"
    )]
    pub drain: Option<Duration>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

impl Default for StopConfig {
    fn default() -> Self {
        Self {
            signal: default_stop_signal(),
            kill_after: default_kill_after(),
            drain: None,
            unknown_fields: Default::default(),
        }
    }
}

impl StopConfig {
    pub fn validate(&self, task: &str) -> R<()> {
        if !STOP_SIGNALS.iter().any(|known| *known == self.signal) {
            bail!(
                "task {task} stop.signal {:?} is not one of {}",
                self.signal,
                STOP_SIGNALS.join(", ")
            );
        }
        if self.kill_after > MAX_KILL_AFTER {
            bail!(
                "task {task} stop.killAfter {} exceeds the {} maximum: grace may slow a \
                 drain, never wedge it (see docs/graceful-stop.md)",
                humantime::format_duration(self.kill_after),
                humantime::format_duration(MAX_KILL_AFTER)
            );
        }
        if let Some(drain) = self.drain {
            if drain > MAX_DRAIN {
                bail!(
                    "task {task} stop.drain {} exceeds the {} maximum: a drain may slow a \
                     rollout, never wedge it (see docs/graceful-stop.md)",
                    humantime::format_duration(drain),
                    humantime::format_duration(MAX_DRAIN)
                );
            }
        }
        Ok(())
    }
}

/// A floor on how many of a task's allocations *voluntary* disruption may
/// leave running. Preemption is the only consumer today; a future rebalancer
/// or scale-in policy is meant to read the same field (see
/// `docs/preemption-and-eviction.md`).
///
/// Deliberately does **not** bind mandatory disruption: drain, node loss and
/// `job stop` ignore it. A floor that can block maintenance is the documented
/// cluster-wedge; a floor that only shrinks preemption's options can never
/// wedge anything — the preemptor simply does not place.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct DisruptionConfig {
    /// Absolute count (`2`) or percent of the task's count (`"50%"`).
    /// Default 0 — choosing a preemptible band donates the whole task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_available: Option<Value>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

impl DisruptionConfig {
    /// The floor in allocations, resolved against a concrete desired count.
    /// A percent rounds **up**: `minAvailable: '50%'` of 5 protects 3.
    pub fn min_available_for(&self, count: u32) -> u32 {
        match self.min_available.as_ref() {
            None => 0,
            Some(Value::Number(number)) => number
                .as_f64()
                .map(|value| value.max(0.0).round() as u32)
                .unwrap_or(0)
                .min(count),
            Some(Value::String(raw)) => match parse_percent(raw) {
                Some(percent) => {
                    let floor = (f64::from(count) * percent / 100.0).ceil();
                    (floor.max(0.0) as u32).min(count)
                }
                None => 0,
            },
            Some(_) => 0,
        }
    }

    pub fn validate(&self, task: &str) -> R<()> {
        match self.min_available.as_ref() {
            None => Ok(()),
            Some(Value::Number(number)) => {
                let value = number.as_f64().ok_or_else(|| {
                    anyhow!("disruption.minAvailable must be a number or percent string")
                })?;
                if value < 0.0 || value.fract() != 0.0 {
                    bail!(
                        "task {task} disruption.minAvailable must be a non-negative whole number"
                    );
                }
                Ok(())
            }
            Some(Value::String(raw)) => match parse_percent(raw) {
                Some(percent) if (0.0..=100.0).contains(&percent) => Ok(()),
                _ => bail!(
                    "task {task} disruption.minAvailable {raw:?} must be a percent between \
                     \"0%\" and \"100%\", or a whole number"
                ),
            },
            Some(_) => bail!(
                "task {task} disruption.minAvailable must be a whole number or a percent \
                 string like \"50%\""
            ),
        }
    }
}

/// `"50%"` -> `Some(50.0)`. Anything else is `None`.
fn parse_percent(raw: &str) -> Option<f64> {
    raw.trim().strip_suffix('%')?.trim().parse::<f64>().ok()
}

/// A task's `gang` block: its replicas place together or not at all
/// (`docs/gang-scheduling.md`).
///
/// `gang = true` is `{ min = count }`: every replica places in one plan, or
/// none does and the eval blocks until a plan can place them all. `min`
/// lowers the quorum: the plan places when at least `min` fit, and the rest
/// follow as room appears. A running gang tops up the same way — missing
/// members place together, or wait — and preemption never takes it below
/// its quorum.
///
/// `placementTimeout` bounds the wait: a gang still short of its quorum that
/// long after it first blocked fails its job. `onMemberLoss` decides what a
/// lost member does to the rest when the loss leaves fewer than `min`
/// running ([`MemberLossPolicy`]).
///
/// Spelled `true`, `false` (no gang), or `{ min = N, placementTimeout =
/// "30m", onMemberLoss = "fail" }`. It serializes back to the short form when
/// nothing but the defaults is set, like [`TaskCount`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GangConfig {
    /// Replicas that must run together. `None` = the task's whole count.
    pub min: Option<u32>,
    /// How long the gang may wait, short of its quorum, before its job is
    /// failed. `None` = wait indefinitely.
    pub placement_timeout: Option<Duration>,
    /// What a member loss that breaks the quorum does to the survivors.
    pub on_member_loss: MemberLossPolicy,
}

/// What happens to a gang's other members when one is lost — it failed, its
/// node was lost or drained, or memory pressure evicted it — and fewer than
/// the gang's quorum still run (docs/gang-scheduling.md, "When a member is
/// lost"). A loss that leaves the quorum running is replaced alone,
/// whatever the policy.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum MemberLossPolicy {
    /// Stop the survivors and place the whole gang again, as one unit. A
    /// restart caused by a member failing spends the task's move budget
    /// (`on_unhealthy.moveCount`); node loss and drain spend nothing.
    #[default]
    Restart,
    /// Keep the survivors, and place the missing members together once they
    /// fit.
    Replace,
    /// Fail the job and stop every allocation of it. It places nothing until
    /// `legion job run` restarts it.
    Fail,
}

impl MemberLossPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            MemberLossPolicy::Restart => "restart",
            MemberLossPolicy::Replace => "replace",
            MemberLossPolicy::Fail => "fail",
        }
    }
}

impl GangConfig {
    /// The replicas that must run together for a task whose desired count
    /// in this pass is `desired`: `min`, capped at `desired`, so a scale-in
    /// below `min` lowers the quorum with it.
    pub fn quorum(&self, desired: u32) -> u32 {
        self.min.unwrap_or(desired).min(desired)
    }

    /// The floor preemption may not take the gang below. `gang = true`
    /// protects every member, whatever the task's count is scaled to;
    /// `min` protects the quorum and leaves the members above it fair game.
    pub fn preemption_floor(&self) -> u32 {
        self.min.unwrap_or(u32::MAX)
    }

    /// Spec-shape rules for a task with this block and `count`.
    pub fn validate(&self, task: &str, count: u32) -> R<()> {
        if self
            .placement_timeout
            .is_some_and(|timeout| timeout < Duration::from_secs(1))
        {
            bail!(
                "task {task} gang.placementTimeout must be at least 1s; omit it to wait until \
                 the gang fits"
            );
        }
        if let Some(min) = self.min {
            if min == 0 || min > count {
                bail!(
                    "task {task} gang.min is {min}, but must be between 1 and the task's count \
                     ({count}); omit min (or write `gang = true`) to require every replica"
                );
            }
        }
        Ok(())
    }

    /// Parse `true` / `{ min = N }`. `false` and `null` mean no gang and are
    /// handled by [`deserialize_gang`], which wraps this.
    fn from_value(value: Value) -> Result<GangConfig, String> {
        match value {
            Value::Bool(true) => Ok(GangConfig::default()),
            Value::Object(fields) => {
                let mut gang = GangConfig::default();
                for (key, value) in fields {
                    match key.as_str() {
                        "min" => {
                            let min = value
                                .as_u64()
                                .and_then(|min| u32::try_from(min).ok())
                                .ok_or_else(|| {
                                    format!("gang.min must be a whole number, got {value}")
                                })?;
                            gang.min = Some(min);
                        }
                        "placementTimeout" => {
                            gang.placement_timeout = Some(
                                parse_duration_value(&value)
                                    .map_err(|err| format!("gang.placementTimeout: {err}"))?,
                            );
                        }
                        "onMemberLoss" => {
                            gang.on_member_loss =
                                serde_json::from_value(value.clone()).map_err(|_| {
                                    format!(
                                        "gang.onMemberLoss must be one of \"restart\", \
                                         \"replace\", \"fail\", got {value}"
                                    )
                                })?;
                        }
                        other => {
                            return Err(format!(
                                "unknown field `{other}` in gang, expected one of `min`, \
                                 `placementTimeout`, `onMemberLoss` (see \
                                 docs/gang-scheduling.md)"
                            ))
                        }
                    }
                }
                Ok(gang)
            }
            other => Err(format!(
                "gang must be true, false, or an object like {{ min = <count>, \
                 placementTimeout = \"30m\" }}, got {other}"
            )),
        }
    }
}

impl Serialize for GangConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if *self == GangConfig::default() {
            return serializer.serialize_bool(true);
        }
        let mut state = serializer.serialize_struct("GangConfig", 3)?;
        if let Some(min) = self.min {
            state.serialize_field("min", &min)?;
        }
        if let Some(timeout) = self.placement_timeout {
            state.serialize_field(
                "placementTimeout",
                &humantime::format_duration(timeout).to_string(),
            )?;
        }
        if self.on_member_loss != MemberLossPolicy::default() {
            state.serialize_field("onMemberLoss", &self.on_member_loss)?;
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for GangConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        GangConfig::from_value(Value::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// `gang` on a task: `false` and `null` are no gang at all, so a stored or
/// round-tripped config never carries an empty block.
fn deserialize_gang<'de, D>(deserializer: D) -> Result<Option<GangConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::Null | Value::Bool(false) => Ok(None),
        value => GangConfig::from_value(value)
            .map(Some)
            .map_err(D::Error::custom),
    }
}

/// Volume names flow into env var names and CLI arguments, so they carry the
/// same charset restriction instance ids do.
pub fn validate_volume_name(task: &str, name: &str) -> R<()> {
    if name.is_empty() || name.len() > 64 {
        bail!("task {task} volume name {name:?} must be 1-64 characters");
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
    {
        bail!("task {task} volume name {name:?} may only contain [A-Za-z0-9._-]");
    }
    Ok(())
}

/// Longest job, group, or task name admission accepts.
pub const MAX_OBJECT_NAME_LEN: usize = 128;

/// Whether `name` is safe as one host path component: non-empty,
/// `[A-Za-z0-9._-]` only, and not starting with `.`. The worker, which runs
/// as root, joins task names onto allocation directories, so a `/` or a `..`
/// would name a path outside the allocation, and a leading `.` could name
/// one of the worker's own entries beside the task directories
/// (`.container`, `.sandbox`).
pub fn is_safe_path_component(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
}

/// Job, group, and task names become host path components on the worker
/// (`allocs/<alloc>/<task>/`), OIDC subject parts, and DNS-style labels, so
/// admission restricts them to `[A-Za-z0-9._-]`, 1-128 characters, not
/// starting with `.`.
pub fn validate_object_name(label: &str, name: &str) -> R<()> {
    if name.is_empty() || name.len() > MAX_OBJECT_NAME_LEN {
        bail!("{label} {name:?} must be 1-{MAX_OBJECT_NAME_LEN} characters");
    }
    if !is_safe_path_component(name) {
        bail!(
            "{label} {name:?} may only contain [A-Za-z0-9._-] and may not start with \".\" \
             (the name becomes a directory on every worker that runs it)"
        );
    }
    Ok(())
}

/// The `size` param of a volume declaration in bytes, when present and
/// parseable. Used for placement-time disk accounting; a driver still receives
/// the raw string.
pub fn volume_param_size_bytes(params: &BTreeMap<String, Value>) -> Option<u64> {
    let raw = crate::volumes::param_str(params, "size")?;
    crate::structs::config::parse_byte_size(&raw).ok()
}

/// Params as a sorted map. Anything that is not a JSON object (including
/// absent) is an empty map — the plugin's `requiredParams` check then produces
/// a message naming the missing keys, which is more useful than a type error.
pub fn volume_param_map(params: Option<&Value>) -> BTreeMap<String, Value> {
    params
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// A claim on storage that exists independently of any instance: an operator's
/// host path, an NFS export, a scratch tmpfs. Declared per task as
/// `volumes: [...]` and valid on **stateless tasks too** — mounting
/// `/mnt/models` read-only is not a stateful workload.
///
/// The difference from [`StatefulStorageConfig`] is lifecycle, not mechanism:
/// these are never provisioned or destroyed by the instance lifecycle, and
/// several tasks may hold the same claim. Both compile to the same mount list
/// on the wire and share one worker pipeline (see docs/volumes.md).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSpec {
    /// Volume name within the task, `[A-Za-z0-9._-]{1,64}` and unique per
    /// task. Becomes `LEGION_VOLUME_<NAME>` in the task env.
    pub name: String,
    /// Volume driver. Defaults to `local-dir`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver: Option<String>,
    /// `filesystem` (default) or `block` — see
    /// [`StatefulStorageConfig::mode`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<VolumeMode>,
    /// Driver-specific parameters, passed through verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    /// Where the task sees it. Required for container tasks (the bind
    /// destination); informational for exec tasks, which read
    /// `$LEGION_VOLUME_<NAME>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Mount read-only. Rejected on `stateful.storage` (an instance volume
    /// nobody can write to is always a mistake).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
    /// Unknown keys kept on this extension point under
    /// `allow_unknown_fields`, as written and unvalidated
    /// (`structs::spec_fields`).
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub pending_extensions: crate::structs::spec_fields::PendingExtensions,
}

impl VolumeSpec {
    pub fn volume_mode(&self) -> VolumeMode {
        self.mode.unwrap_or_default()
    }

    pub fn driver_name(&self) -> &str {
        self.driver
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .unwrap_or(crate::volumes::DEFAULT_VOLUME_DRIVER)
    }

    pub fn param_map(&self) -> BTreeMap<String, Value> {
        volume_param_map(self.params.as_ref())
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only.unwrap_or(false)
    }
}

/// One resolved mount handed to a worker in `TaskAllocation.task_volumes`.
///
/// This is the post-admission form: the driver is resolved, the volume id is
/// known (minted from the instance's storage handle for instance volumes, or
/// derived deterministically for task claims), and the worker mounts exactly
/// this list without re-deriving anything.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMount {
    /// Volume id — the second half of the `driver/volume_id` handle.
    pub volume_id: String,
    /// Declared name within the task.
    pub name: String,
    pub driver: String,
    /// Resolved consumption mode. The worker asks the driver for exactly this,
    /// and a task kind reads it to know whether it got a path or a device.
    #[serde(default)]
    pub mode: VolumeMode,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, Value>,
    /// Driver metadata recorded by a previous mount, handed back so drivers
    /// stay stateless between calls.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attrs: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default)]
    pub read_only: bool,
    /// True when this mount is the instance's `stateful.storage`. The worker
    /// uses it to also export `LEGION_STORAGE_DIR`, and the manager to know
    /// which mount owns a lifecycle.
    #[serde(default)]
    pub instance_volume: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct TaskConfig {
    pub name: String,
    #[serde(default = "default_task_count")]
    pub count: TaskCount,
    /// Name of the task runtime plugin that runs this task (`exec`,
    /// `container`, `noop`, or a plugin under `plugins/tasks/<name>/`).
    pub runtime: String,
    #[serde(default = "default_task_group")]
    pub group: String,
    #[serde(default)]
    #[tabled(skip)]
    pub depends_on: Vec<TaskDependency>,
    #[serde(default = "default_task_lifecycle")]
    #[tabled(skip)]
    pub lifecycle: TaskLifecycle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub schedule: Option<TaskSchedule>,
    #[serde(default = "empty_object", alias = "options")]
    pub config: Value,
    #[serde(default = "empty_object")]
    pub resources: Value,
    #[serde(default = "empty_object")]
    pub constraints: Value,
    #[serde(default = "empty_object")]
    pub permit: Value,
    #[serde(default = "empty_object")]
    pub affinities: Value,
    #[serde(default)]
    #[tabled(skip)]
    pub checks: Vec<TaskCheck>,
    #[serde(default = "default_on_unhealthy")]
    #[tabled(skip)]
    pub on_unhealthy: TaskOnUnhealthyConfig,
    /// How the worker reacts to the task process exiting. None = the
    /// lifecycle default (`always` for service/sidecar, `onFailure` for
    /// batch) — see `docs/task-supervision.md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub restart: Option<TaskRestartConfig>,
    /// How the platform terminates this task — signal, then grace, then
    /// SIGKILL. None = the defaults (`SIGTERM`, `30s`); see
    /// `docs/graceful-stop.md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub stop: Option<StopConfig>,
    /// Floor on how many allocations *voluntary* disruption (preemption
    /// today) may leave running. None = floor 0, today's behavior; see
    /// `docs/preemption-and-eviction.md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub disruption: Option<DisruptionConfig>,
    /// All-or-nothing placement for this task's replicas (see
    /// `docs/gang-scheduling.md`). None = replicas place independently, and
    /// a plan places whatever fits.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_gang"
    )]
    #[tabled(skip)]
    pub gang: Option<GangConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub kv_injections: Option<TaskKvInjections>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub network: Option<crate::structs::network::NetworkConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub service: Option<crate::structs::network::ServiceConfig>,
    /// Services this task dials through node-local loopback forwarders (see
    /// `docs/service-discovery.md`). None = today's behavior: no forwarders,
    /// no extra env, zero cost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub connect: Option<crate::structs::network::ConnectConfig>,
    /// Workload identities issued to this task (workload-identity OIDC plan).
    /// Empty = today's behavior; the feature is opt-in per task.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[tabled(skip)]
    pub identities: Vec<WorkloadIdentity>,
    /// Stable instance identity for stateful workloads (see
    /// `docs/stateful-workloads.md`). None = stateless, today's behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub stateful: Option<StatefulConfig>,
    /// Claims on storage that exists independently of any instance — a host
    /// path, an NFS export, a scratch tmpfs (see `docs/volumes.md`). Valid on
    /// stateless tasks too; empty = no mounts, today's behavior.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[tabled(skip)]
    pub volumes: Vec<VolumeSpec>,
    /// Artifacts fetched into the task directory before start (artifact
    /// system plan). Empty = today's behavior; staging is skipped entirely.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[tabled(skip)]
    pub artifacts: Vec<ArtifactSpec>,
    /// Pre-baked, parameterized command templates runnable against a running
    /// allocation of this task (see `internal_docs/plans/exec-and-actions-plan.md`).
    /// Empty = no actions. Guarded by the `action` ACL resource, separate
    /// from raw `exec`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[tabled(skip)]
    pub actions: Vec<ActionSpec>,
    /// How far a node may get on a start for this task before the placement
    /// is durable (see `docs/edge-boot.md`). None = `mode: 'off'`, today's
    /// behavior: dispatch happens after the commit and the wire payload
    /// carries no provisional block at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub speculative_start: Option<crate::structs::edge_boot::SpeculativeStartConfig>,
    /// Opts the task into snapshots (`legion snapshot`, see
    /// `docs/suspend-and-resume.md`) and sets their expiry. None = no
    /// snapshots block: disk snapshots still work for an instance volume that
    /// supports them, memory snapshots do not, and snapshots never expire on
    /// their own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub snapshots: Option<crate::structs::snapshot::SnapshotsConfig>,
    /// How `legion instance suspend` checkpoints this task and how a resume
    /// behaves (see `docs/suspend-and-resume.md`). Keyed stateful tasks only.
    /// None = suspend with the runtime's best checkpoint and the defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub suspend: Option<crate::structs::suspend::SuspendConfig>,
    /// Task autoscaling (docs/autoscaling.md): `true` for the effective
    /// defaults, or a sparse record overriding some of them. Stored exactly
    /// as written; the defaults it inherits (cluster, namespace policies)
    /// are resolved live. None (or `false`) = the count is the spec's, or a
    /// `legion job scale` override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub autoscale: Option<crate::autoscaler::AutoscaleSpec>,
    /// Unknown keys kept on this task under `allow_unknown_fields`, as
    /// written and unvalidated (`structs::spec_fields`). Nothing acts on
    /// them; `legion job info` lists them. Empty for every task submitted
    /// without the flag.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[tabled(skip)]
    pub pending_extensions: PendingExtensions,
}

/// A declared action: a named command template with typed parameters.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ActionSpec {
    /// Action name, `[a-z0-9-]{1,128}`, unique within its task.
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// Command argv. `${param.<name>}` placeholders are substituted at
    /// invocation, element-by-element, with no shell evaluation.
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<ActionParam>,
    /// Allocate a PTY when run (default false).
    #[serde(default)]
    pub tty: bool,
    /// Optional wall-clock cap, e.g. `"60s"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

/// Parameter type for an action parameter.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum ActionParamType {
    #[default]
    String,
    Int,
    Bool,
    Enum,
}

impl ActionParamType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ActionParamType::String => "string",
            ActionParamType::Int => "int",
            ActionParamType::Bool => "bool",
            ActionParamType::Enum => "enum",
        }
    }
}

/// A typed, validated parameter of an action.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ActionParam {
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, rename = "type")]
    pub param_type: ActionParamType,
    /// When set, the param is optional and this value is used if omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// Explicit required flag; defaults to `true` unless a `default` is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    /// Allowed values for `enum` params.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<String>,
    /// Anchored regex constraint for `string` params.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    /// Inclusive bounds for `int` params.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<i64>,
    /// Redact the value in audit events and UI history.
    #[serde(default)]
    pub sensitive: bool,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

impl ActionParam {
    /// Whether this parameter must be supplied at invocation.
    pub fn is_required(&self) -> bool {
        self.required.unwrap_or_else(|| self.default.is_none())
    }

    /// Validates one supplied value against this parameter's constraints.
    pub fn validate_value(&self, value: &str) -> Result<(), String> {
        match self.param_type {
            ActionParamType::String => {
                if let Some(pattern) = &self.pattern {
                    let re = regex::Regex::new(pattern)
                        .map_err(|e| format!("param `{}` bad pattern: {e}", self.name))?;
                    if !re.is_match(value) {
                        return Err(format!(
                            "param `{}` value does not match pattern",
                            self.name
                        ));
                    }
                }
            }
            ActionParamType::Int => {
                let n: i64 = value
                    .parse()
                    .map_err(|_| format!("param `{}` must be an integer", self.name))?;
                if let Some(min) = self.min {
                    if n < min {
                        return Err(format!("param `{}` must be >= {min}", self.name));
                    }
                }
                if let Some(max) = self.max {
                    if n > max {
                        return Err(format!("param `{}` must be <= {max}", self.name));
                    }
                }
            }
            ActionParamType::Bool => {
                if value != "true" && value != "false" {
                    return Err(format!("param `{}` must be `true` or `false`", self.name));
                }
            }
            ActionParamType::Enum => {
                if !self.values.iter().any(|v| v == value) {
                    return Err(format!(
                        "param `{}` must be one of: {}",
                        self.name,
                        self.values.join(", ")
                    ));
                }
            }
        }
        Ok(())
    }
}

impl ActionSpec {
    /// Validates the action's static shape at job submission. Runtime param
    /// validation happens separately at invocation via [`render_command`].
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() || self.name.len() > 128 {
            return Err(format!("action name `{}` must be 1..=128 chars", self.name));
        }
        if !self
            .name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(format!("action name `{}` must match [a-z0-9-]", self.name));
        }
        if self.command.is_empty() {
            return Err(format!("action `{}` command must not be empty", self.name));
        }
        let mut seen = std::collections::HashSet::new();
        for param in &self.params {
            if !seen.insert(param.name.as_str()) {
                return Err(format!(
                    "action `{}` has duplicate param `{}`",
                    self.name, param.name
                ));
            }
            if matches!(param.param_type, ActionParamType::Enum) && param.values.is_empty() {
                return Err(format!(
                    "action `{}` enum param `{}` must list `values`",
                    self.name, param.name
                ));
            }
            if let Some(pattern) = &param.pattern {
                regex::Regex::new(pattern).map_err(|e| {
                    format!(
                        "action `{}` param `{}` has invalid pattern: {e}",
                        self.name, param.name
                    )
                })?;
            }
        }
        Ok(())
    }

    /// Validates supplied params and renders the final argv. Every argv
    /// element has its `${param.<name>}` placeholders replaced with the
    /// validated value; there is no shell evaluation and no word-splitting.
    ///
    /// Returns the rendered argv plus the effective (defaulted) parameter
    /// values for audit logging.
    pub fn render_command(
        &self,
        supplied: &std::collections::HashMap<String, String>,
    ) -> Result<(Vec<String>, std::collections::BTreeMap<String, String>), String> {
        // Reject params that were not declared (strict allowlist).
        for key in supplied.keys() {
            if !self.params.iter().any(|p| &p.name == key) {
                return Err(format!("unknown parameter `{key}`"));
            }
        }

        let mut resolved: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        for param in &self.params {
            let value = match supplied.get(&param.name) {
                Some(v) => v.clone(),
                None => match &param.default {
                    Some(d) => d.clone(),
                    None => {
                        if param.is_required() {
                            return Err(format!("missing required parameter `{}`", param.name));
                        }
                        String::new()
                    }
                },
            };
            param.validate_value(&value)?;
            resolved.insert(param.name.clone(), value);
        }

        let mut argv = Vec::with_capacity(self.command.len());
        for element in &self.command {
            argv.push(substitute_params(element, &resolved)?);
        }
        Ok((argv, resolved))
    }
}

/// Replaces `${param.<name>}` placeholders inside a single argv element.
/// Unknown placeholders are an error; the replacement is literal (no shell).
fn substitute_params(
    element: &str,
    resolved: &std::collections::BTreeMap<String, String>,
) -> Result<String, String> {
    let mut out = String::with_capacity(element.len());
    let mut rest = element;
    while let Some(start) = rest.find("${param.") {
        out.push_str(&rest[..start]);
        let after = &rest[start + "${param.".len()..];
        let end = after
            .find('}')
            .ok_or_else(|| format!("unterminated `${{param.` in `{element}`"))?;
        let name = &after[..end];
        let value = resolved
            .get(name)
            .ok_or_else(|| format!("unknown parameter `{name}` in command template"))?;
        out.push_str(value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum KvInjectionAction {
    None,
    /// Rewrite a KV `files` entry under the running task, then apply the
    /// entry's `changeMode`. Files only: a live process's env cannot change.
    Update,
    Restart,
    Rollout,
    Fail,
}

/// Per-reference KV reactivity policy. Every field is optional so an entry
/// can override the task-level defaults field-by-field: a field left unset
/// on the entry inherits the defaults block, and a field unset in both
/// places falls back to the documented hard defaults (`watch=false`,
/// `onChange=none`, `onDelete=fail`). See `docs/kv-store.md`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct KvInjectionPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_change: Option<KvInjectionAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_delete: Option<KvInjectionAction>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

/// Fully-resolved KV policy for one injection edge after defaults and hard
/// fallbacks are applied (the only shape reconciliation reasons about).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedKvInjectionPolicy {
    pub watch: bool,
    pub on_change: KvInjectionAction,
    pub on_delete: KvInjectionAction,
}

impl KvInjectionPolicy {
    /// Resolve this entry-level policy against the task-level defaults
    /// block. Precedence per field: entry value, then defaults value, then
    /// the hard default (`watch=false`, `onChange=none`, `onDelete=fail`).
    pub fn resolve_with_defaults(&self, defaults: &KvInjectionPolicy) -> ResolvedKvInjectionPolicy {
        ResolvedKvInjectionPolicy {
            watch: self.watch.or(defaults.watch).unwrap_or(false),
            on_change: self
                .on_change
                .or(defaults.on_change)
                .unwrap_or(KvInjectionAction::None),
            on_delete: self
                .on_delete
                .or(defaults.on_delete)
                .unwrap_or(KvInjectionAction::Fail),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct KvTemplateSource {
    #[serde(default)]
    pub template: Option<String>,
    #[serde(default)]
    pub template_kv: Option<String>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskKvEnvInjection {
    pub name: String,
    pub value: KvTemplateSource,
    #[serde(default)]
    pub kv: KvInjectionPolicy,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskKvFileInjection {
    pub path: String,
    #[serde(default)]
    pub mode: Option<String>,
    pub file: KvTemplateSource,
    #[serde(default)]
    pub kv: KvInjectionPolicy,
    /// What the running task receives after an `onChange = "update"`
    /// rewrite: nothing (default), `changeSignal`, or an in-place restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_mode: Option<crate::structs::workload_identity::ChangeMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_signal: Option<String>,
    /// Random delay of up to this long before acting, so a fleet does not
    /// reload at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub splay: Option<String>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

/// The `liveUpdate` marker on a rendered `config.files` entry, written by the
/// manager for `onChange = "update"` files. Its presence is what lets the
/// worker rewrite the file under a running task; the fields say what the task
/// receives afterwards.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct LiveFileUpdate {
    #[serde(default)]
    pub change_mode: crate::structs::workload_identity::ChangeMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_signal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub splay: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskKvInjections {
    #[serde(default)]
    pub kv: Option<KvInjectionPolicy>,
    #[serde(default)]
    pub env: Vec<TaskKvEnvInjection>,
    #[serde(default)]
    pub files: Vec<TaskKvFileInjection>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct JobConfig {
    /// The job's canonical id: `namespace/name`, or the bare name for a job
    /// in `default` (`structs::namespace`). The jobspec spells it as `name`
    /// plus an optional `namespace`; parsing folds the two together.
    pub name: String,
    pub tasks: Vec<TaskConfig>,
    /// How important this job is, relative to everything else on the
    /// cluster. Orders the eval queue and governs preemption; absent means
    /// [`Band::Normal`]. See `docs/preemption-and-eviction.md`.
    #[serde(default)]
    pub priority: Band,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollout: Option<RolloutConfig>,
    // Serialized as the jobspec spells it, so a stored config re-parses with
    // its group overrides (it used to serialize as `rollout_by_group`, which
    // the parser silently ignored).
    #[serde(
        default,
        rename = "rolloutByGroup",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub rollout_by_group: BTreeMap<String, RolloutConfig>,
    pub version: Option<u32>,
    /// Unknown top-level keys kept under `allow_unknown_fields`, as written
    /// and unvalidated (`structs::spec_fields`). Nothing acts on them;
    /// `legion job info` lists them. Empty for every job submitted without
    /// the flag.
    #[serde(
        default,
        rename = "pendingExtensions",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub pending_extensions: PendingExtensions,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobConfigInput {
    name: String,
    /// The namespace the job lives in (docs/namespaces.md). Optional; the
    /// submit path resolves it against the request's namespace. Folded into
    /// `JobConfig.name`, which holds the canonical job id (`ns/name`, or the
    /// bare name in `default`).
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    tasks: Option<TaskConfigList>,
    #[serde(default, deserialize_with = "deserialize_single_task")]
    task: Option<TaskConfigInput>,
    /// `critical | high | elevated | normal | low | background`. Absent =
    /// `normal`. Deserialized as a raw string so an unknown band produces
    /// the vocabulary-listing message from [`Band::parse`] rather than
    /// serde's bare "unknown variant".
    #[serde(default)]
    priority: Option<String>,
    #[serde(default)]
    rollout: Option<RolloutConfig>,
    #[serde(default)]
    rollout_by_group: BTreeMap<String, RolloutConfig>,
    version: Option<u32>,
    #[serde(default)]
    pending_extensions: PendingExtensions,
    #[serde(flatten)]
    unknown_fields: UnknownFields,
}

extension_point!(
    JobConfigInput,
    ExtensionPoint::Job,
    [
        "name",
        "namespace",
        "tasks",
        "task",
        "priority",
        "rollout",
        "rolloutByGroup",
        "version"
    ],
    |this, walk| {
        walk.opt("rollout", this.rollout.as_mut());
        walk.map("rolloutByGroup", &mut this.rollout_by_group);
        if let Some(task) = this.task.as_mut() {
            let label = task.name.clone().unwrap_or_else(|| "task".to_string());
            walk.scoped(format!("task {label}"), task);
        }
        match this.tasks.as_mut() {
            Some(TaskConfigList::List(tasks)) => {
                for (index, task) in tasks.iter_mut().enumerate() {
                    let label = task
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("task-{}", index + 1));
                    walk.scoped(format!("task {label}"), task);
                }
            }
            Some(TaskConfigList::Map(tasks)) => {
                for (key, task) in tasks.iter_mut() {
                    let label = task.name.clone().unwrap_or_else(|| key.clone());
                    walk.scoped(format!("task {label}"), task);
                }
            }
            None => {}
        }
    }
);

/// `tasks`, parsed by its shape: a list of tasks, or a map of task name to
/// task. Not an untagged enum: that would try both shapes and replace a
/// task's own error (a wrong type, a bad value) with serde's generic "data
/// did not match any variant". Each task's error names the task instead.
enum TaskConfigList {
    List(Vec<TaskConfigInput>),
    Map(BTreeMap<String, TaskConfigInput>),
}

impl<'de> Deserialize<'de> for TaskConfigList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ShapeVisitor;

        impl<'de> serde::de::Visitor<'de> for ShapeVisitor {
            type Value = TaskConfigList;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a list of tasks, or a map of task name to task")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut tasks = Vec::new();
                loop {
                    match seq.next_element::<TaskConfigInput>() {
                        Ok(Some(task)) => tasks.push(task),
                        Ok(None) => return Ok(TaskConfigList::List(tasks)),
                        Err(err) => {
                            return Err(A::Error::custom(format!("tasks[{}]: {err}", tasks.len())))
                        }
                    }
                }
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut tasks = BTreeMap::new();
                while let Some(name) = map.next_key::<String>()? {
                    let task = map
                        .next_value::<TaskConfigInput>()
                        .map_err(|err| A::Error::custom(format!("task {name}: {err}")))?;
                    tasks.insert(name, task);
                }
                Ok(TaskConfigList::Map(tasks))
            }
        }

        deserializer.deserialize_any(ShapeVisitor)
    }
}

/// The single-`task` shorthand, with its errors naming it.
fn deserialize_single_task<'de, D>(deserializer: D) -> Result<Option<TaskConfigInput>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<TaskConfigInput>::deserialize(deserializer)
        .map_err(|err| D::Error::custom(format!("task: {err}")))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskConfigInput {
    #[serde(default)]
    name: Option<String>,
    // Optional so keyed-mode validation can tell an explicit `count` apart
    // from the default; resolved via `unwrap_or_default()` in conversion.
    #[serde(default)]
    count: Option<TaskCount>,
    // Optional so the `kind` trap below can produce a rename hint instead of
    // serde's bare "missing field `runtime`".
    #[serde(default)]
    runtime: Option<String>,
    // `kind` was the pre-rename spelling of `runtime`. Serde would silently
    // ignore it and then report `runtime` as missing, which tells the user
    // nothing about what happened — so capture it and reject with the fix.
    #[serde(default)]
    kind: Option<String>,
    #[serde(default = "default_task_group")]
    group: String,
    #[serde(default)]
    depends_on: Vec<TaskDependencyInput>,
    #[serde(default)]
    lifecycle: TaskLifecycle,
    #[serde(default)]
    schedule: Option<TaskSchedule>,
    #[serde(default = "empty_object", alias = "options")]
    config: Value,
    #[serde(default = "empty_object")]
    resources: Value,
    #[serde(default = "empty_object")]
    constraints: Value,
    #[serde(default = "empty_object")]
    permit: Value,
    #[serde(default = "empty_object")]
    affinities: Value,
    #[serde(default)]
    checks: Vec<TaskCheck>,
    #[serde(default)]
    on_unhealthy: Option<TaskOnUnhealthyConfig>,
    #[serde(default)]
    restart: Option<TaskRestartConfig>,
    #[serde(default)]
    stop: Option<StopConfig>,
    #[serde(default)]
    disruption: Option<DisruptionConfig>,
    #[serde(default, deserialize_with = "deserialize_gang")]
    gang: Option<GangConfig>,
    #[serde(default)]
    kv: Option<KvInjectionPolicy>,
    #[serde(default)]
    env: Vec<TaskKvEnvInjection>,
    #[serde(default)]
    files: Vec<TaskKvFileInjection>,
    #[serde(default)]
    kv_injections: Option<TaskKvInjections>,
    #[serde(default)]
    network: Option<crate::structs::network::NetworkConfig>,
    #[serde(default)]
    service: Option<crate::structs::network::ServiceConfig>,
    #[serde(default)]
    connect: Option<crate::structs::network::ConnectConfig>,
    #[serde(default)]
    identities: Vec<WorkloadIdentity>,
    #[serde(default)]
    stateful: Option<StatefulConfig>,
    #[serde(default)]
    volumes: Vec<VolumeSpec>,
    // `onLost` is only valid INSIDE the stateful block. Without this field
    // serde would silently ignore a task-level `onLost`, which is exactly
    // the misplacement users will type (and on a stateless task there is
    // nothing to fence) — so catch it and reject loudly at parse time.
    #[serde(default)]
    on_lost: Option<Value>,
    #[serde(default)]
    artifacts: Vec<ArtifactSpec>,
    #[serde(default)]
    actions: Vec<ActionSpec>,
    #[serde(default)]
    speculative_start: Option<crate::structs::edge_boot::SpeculativeStartConfig>,
    #[serde(default)]
    snapshots: Option<crate::structs::snapshot::SnapshotsConfig>,
    #[serde(default)]
    suspend: Option<crate::structs::suspend::SuspendConfig>,
    // Strict on its own (`deny_unknown_fields` on the record): a typo in an
    // autoscale record is an error even under `allow_unknown_fields`.
    #[serde(default)]
    autoscale: Option<crate::autoscaler::AutoscaleSpec>,
    #[serde(default)]
    pending_extensions: PendingExtensions,
    #[serde(flatten)]
    unknown_fields: UnknownFields,
}

extension_point!(
    TaskConfigInput,
    ExtensionPoint::Task,
    [
        "name",
        "count",
        "runtime",
        "group",
        "dependsOn",
        "lifecycle",
        "schedule",
        "config",
        "resources",
        "constraints",
        "permit",
        "affinities",
        "checks",
        "onUnhealthy",
        "restart",
        "stop",
        "disruption",
        "gang",
        "kv",
        "env",
        "files",
        "kvInjections",
        "network",
        "service",
        "connect",
        "identities",
        "stateful",
        "volumes",
        "artifacts",
        "actions",
        "speculativeStart",
        "snapshots",
        "suspend",
        "autoscale"
    ],
    |this, walk| {
        for (index, dependency) in this.depends_on.iter_mut().enumerate() {
            if let TaskDependencyInput::Detailed(detailed) = dependency {
                walk.block(&format!("dependsOn[{index}]"), detailed);
            }
        }
        walk.list("checks", &mut this.checks);
        walk.opt("onUnhealthy", this.on_unhealthy.as_mut());
        walk.opt("restart", this.restart.as_mut());
        walk.opt("stop", this.stop.as_mut());
        walk.opt("disruption", this.disruption.as_mut());
        walk.opt("kv", this.kv.as_mut());
        walk.list("env", &mut this.env);
        walk.list("files", &mut this.files);
        walk.opt("kvInjections", this.kv_injections.as_mut());
        walk.opt("network", this.network.as_mut());
        if let Some(service) = this.service.as_mut() {
            let label = match service.name.as_deref() {
                Some(name) => format!("service {name}"),
                None => "service".to_string(),
            };
            walk.scoped(label, service);
        }
        walk.opt("connect", this.connect.as_mut());
        walk.list("identities", &mut this.identities);
        walk.opt("stateful", this.stateful.as_mut());
        for volume in this.volumes.iter_mut() {
            walk.scoped(format!("volume {}", volume.name), volume);
        }
        walk.list("artifacts", &mut this.artifacts);
        walk.list("actions", &mut this.actions);
        walk.opt("speculativeStart", this.speculative_start.as_mut());
        walk.opt("snapshots", this.snapshots.as_mut());
        walk.opt("suspend", this.suspend.as_mut());
    }
);

/// A `dependsOn` entry: a task name, or `{ task, condition }`.
#[derive(Deserialize)]
#[serde(untagged)]
enum TaskDependencyInput {
    Name(String),
    Detailed(TaskDependencyDetailed),
}

#[derive(Deserialize)]
struct TaskDependencyDetailed {
    task: String,
    #[serde(default = "default_dependency_condition")]
    condition: TaskDependencyCondition,
    #[serde(flatten)]
    unknown_fields: UnknownFields,
}

spec_block!(TaskDependencyDetailed, ["task", "condition"]);

impl TaskConfigInput {
    fn into_task_config(self, name: String) -> Result<TaskConfig, String> {
        let name = self.name.unwrap_or(name);
        // The task runtime axis was called `kind` before it was renamed to
        // `runtime`. There is deliberately no alias (see
        // internal_docs/calls/task-runtime-rename-plan_calls.md §1), so name
        // the exact substitution rather than failing on a missing field.
        let runtime = match (self.runtime, self.kind) {
            (Some(_), Some(_)) => {
                return Err(format!(
                    "task {name} sets both runtime and kind; `kind` was renamed to \
                     `runtime` — remove the kind field \
                     (see docs/task-runtime-plugins.md)"
                ))
            }
            (None, Some(kind)) => {
                return Err(format!(
                    "task {name}: unknown field `kind`, expected `runtime`. `kind` was \
                     renamed to `runtime`; replace `kind: \"{kind}\"` with \
                     `runtime: \"{kind}\"` (see docs/task-runtime-plugins.md)"
                ))
            }
            (None, None) => {
                return Err(format!(
                    "task {name} is missing the required field `runtime`, which names \
                     the task runtime plugin that runs it (for example \
                     `runtime: \"exec\"`; see docs/task-runtime-plugins.md)"
                ))
            }
            (Some(runtime), None) => runtime,
        };
        // Keyed-mode cardinality is owned by the instance management API
        // (CreateInstance/DestroyInstance), never by the job spec. An explicit
        // `count` on a keyed task would be silently meaningless, so reject it
        // loudly at parse time (only here do we still know it was explicit).
        if let Some(stateful) = self.stateful.as_ref() {
            if matches!(stateful.mode, InstanceMode::Keyed | InstanceMode::Session)
                && self.count.is_some()
            {
                return Err(format!(
                    "task {name} is stateful keyed mode and cannot define count: keyed \
                     instance cardinality is driven by the instance management API \
                     (legion instance create/destroy), not the job spec; remove the \
                     count field (see docs/stateful-workloads.md)"
                ));
            }
        }
        if self.on_lost.is_some() {
            return Err(if self.stateful.is_some() {
                format!(
                    "task {name} declares onLost at the task level; the node-loss fencing \
                     policy belongs inside the stateful block — move it to \
                     stateful.onLost (see docs/stateful-workloads.md)"
                )
            } else {
                format!(
                    "task {name} declares onLost but is not stateful; onLost is the \
                     node-loss fencing policy for stateful instances and is only valid \
                     inside a stateful block. Stateless tasks are always replaced eagerly \
                     on node loss (see docs/stateful-workloads.md)"
                )
            });
        }
        let depends_on = self
            .depends_on
            .into_iter()
            .map(|entry| match entry {
                TaskDependencyInput::Name(task) => TaskDependency {
                    task,
                    condition: default_dependency_condition(),
                },
                TaskDependencyInput::Detailed(TaskDependencyDetailed {
                    task, condition, ..
                }) => TaskDependency { task, condition },
            })
            .collect();
        Ok(TaskConfig {
            name,
            count: self.count.unwrap_or_default(),
            runtime,
            group: self.group,
            depends_on,
            lifecycle: self.lifecycle,
            schedule: self.schedule,
            config: self.config,
            resources: self.resources,
            constraints: self.constraints,
            permit: self.permit,
            affinities: self.affinities,
            checks: self.checks,
            on_unhealthy: self.on_unhealthy.unwrap_or_default(),
            restart: self.restart,
            stop: self.stop,
            disruption: self.disruption,
            gang: self.gang,
            kv_injections: Some(if let Some(existing) = self.kv_injections {
                TaskKvInjections {
                    kv: self.kv.or(existing.kv),
                    env: if self.env.is_empty() {
                        existing.env
                    } else {
                        self.env
                    },
                    files: if self.files.is_empty() {
                        existing.files
                    } else {
                        self.files
                    },
                    unknown_fields: UnknownFields::new(),
                }
            } else {
                TaskKvInjections {
                    kv: self.kv,
                    env: self.env,
                    files: self.files,
                    unknown_fields: UnknownFields::new(),
                }
            }),
            network: self.network,
            service: self.service,
            connect: self.connect,
            identities: self.identities,
            stateful: self.stateful,
            volumes: self.volumes,
            artifacts: self.artifacts,
            actions: self.actions,
            speculative_start: self.speculative_start,
            snapshots: self.snapshots,
            suspend: self.suspend,
            // `autoscale = false` is the same as leaving it out; store it so.
            autoscale: self.autoscale.filter(|spec| spec.is_enabled()),
            pending_extensions: self.pending_extensions,
        })
    }
}

fn task_list_to_vec(configs: TaskConfigList) -> Result<Vec<TaskConfig>, String> {
    match configs {
        TaskConfigList::List(tasks) => tasks
            .into_iter()
            .enumerate()
            .map(|(index, task)| task.into_task_config(format!("task-{}", index + 1)))
            .collect(),
        TaskConfigList::Map(map) => map
            .into_iter()
            .map(|(name, task)| task.into_task_config(name))
            .collect(),
    }
}

/// Check a parsed jobspec's unknown keys with `walk`, then build the config.
/// Every way a `JobConfig` is parsed goes through here, so no path can
/// accept a key it does not know without saying so.
fn job_config_from_input(
    mut input: JobConfigInput,
    mut walk: FieldWalk,
) -> Result<(JobConfig, UnknownFieldsRecord), String> {
    walk.root(&mut input);
    let record = walk.finish(&input.name).map_err(|err| err.to_string())?;

    let tasks = match (input.tasks, input.task) {
        (Some(_), Some(_)) => return Err("jobspec must define only one of task or tasks".into()),
        (Some(tasks), None) => task_list_to_vec(tasks)?,
        (None, Some(task)) => vec![task.into_task_config(String::from("task"))?],
        (None, None) => return Err("jobspec must define at least one task".into()),
    };

    let priority = match input.priority.as_deref() {
        None => Band::default(),
        Some(raw) => Band::parse(raw)?,
    };

    // `name` may itself be qualified (`prod/web`, the form every reply
    // prints); a qualified name that disagrees with `namespace` is an
    // error, never a precedence rule.
    let name = crate::structs::namespace::resolve_job_ref(&input.name, input.namespace.as_deref())
        .map_err(|err| format!("jobspec: {err}"))?;
    let mut config = JobConfig {
        name,
        tasks,
        priority,
        rollout: input.rollout,
        rollout_by_group: input.rollout_by_group,
        version: input.version,
        pending_extensions: input.pending_extensions,
    };
    // Session tasks become a keyed task plus a generated envelope task
    // here, at parse, so validation, storage, and every reader see one
    // shape. Idempotent, so a stored config parses back unchanged.
    crate::structs::session::normalize_session_tasks(&mut config)
        .map_err(|err| format!("{err:#}"))?;
    Ok((config, record))
}

/// Plain deserialization is strict: an unknown key anywhere in the spec is
/// an error. It accepts a stored config's `pendingExtensions`, so a config
/// Legion serialized parses back unchanged. A submit goes through
/// [`JobConfig::parse_submitted`] instead, which honors the request's
/// `allow_unknown_fields`.
impl<'de> Deserialize<'de> for JobConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = JobConfigInput::deserialize(deserializer)?;
        job_config_from_input(input, FieldWalk::stored())
            .map(|(config, _)| config)
            .map_err(D::Error::custom)
    }
}

impl JobConfig {
    /// Parse a rendered jobspec someone submitted (`RunJob`, `PlanJob`,
    /// `RunJobTemplate`, `legion job validate`).
    ///
    /// With [`UnknownFieldMode::Reject`], every key the jobspec does not
    /// know is an error naming its path and the closest known key. With
    /// [`UnknownFieldMode::Allow`] (`allow_unknown_fields`), a key on an
    /// extension point (the job, a task, a volume, a port, a service) is kept
    /// in that block's `pendingExtensions`, a key anywhere else is dropped,
    /// and the returned record says which. A value Legion knows is wrong is
    /// an error either way. See docs/jobs-tasks-and-scheduling.md, "Unknown
    /// fields".
    pub fn parse_submitted(
        value: Value,
        mode: UnknownFieldMode,
    ) -> R<(JobConfig, UnknownFieldsRecord)> {
        let input: JobConfigInput = serde_json::from_value(value)?;
        job_config_from_input(input, FieldWalk::submitted(mode)).map_err(|err| anyhow!(err))
    }

    /// [`Self::parse_submitted`] on JSON text.
    pub fn parse_submitted_str(
        text: &str,
        mode: UnknownFieldMode,
    ) -> R<(JobConfig, UnknownFieldsRecord)> {
        let input: JobConfigInput = serde_json::from_str(text)?;
        job_config_from_input(input, FieldWalk::submitted(mode)).map_err(|err| anyhow!(err))
    }

    /// Normalize spec shorthands (currently the `service.port` inline port
    /// sugar) so stored configs are always in explicit form. Must run before
    /// validation and storage on every submit path.
    pub fn normalize(&mut self) -> R<()> {
        for task in &mut self.tasks {
            task.normalize_network()?;
        }
        Ok(())
    }

    pub fn is_same_as(&self, other: &JobConfig) -> bool {
        self.name == other.name
            && self.tasks == other.tasks
            // A band change is a real change: it moves the job's queue
            // position and its preemption relationships, so it must mint a
            // new config version rather than being swallowed as "unchanged".
            && self.priority == other.priority
            && self.rollout == other.rollout
            && self.rollout_by_group == other.rollout_by_group
            // Pending keys are part of what was submitted: dropping or
            // changing one is a new version, or it would be lost.
            && self.pending_extensions == other.pending_extensions
    }

    pub fn id(&self) -> String {
        match self.version {
            Some(version) => format!("{}-{}", self.name, version),
            None => format!("{}-{}", self.name, 0),
        }
    }

    pub fn rollout_config_for_group(&self, group: &str) -> Option<RolloutConfig> {
        match (&self.rollout, self.rollout_by_group.get(group)) {
            (None, None) => None,
            (Some(global), None) => Some(global.clone()),
            (None, Some(group_override)) => Some(group_override.clone()),
            (Some(global), Some(group_override)) => {
                let mut merged = global.clone();
                merged.apply_override(group_override);
                Some(merged)
            }
        }
    }

    pub fn validate_rollout(&self) -> R<()> {
        if self.rollout.is_none() && self.rollout_by_group.is_empty() {
            return Ok(());
        }

        if let Some(global) = self.rollout.as_ref() {
            global.validate("rollout")?;
        }

        let groups = self
            .tasks
            .iter()
            .map(|task| task.group.clone())
            .collect::<BTreeSet<_>>();

        for (group, config) in &self.rollout_by_group {
            if !groups.contains(group) {
                bail!("rolloutByGroup references unknown group {group}");
            }
            config.validate(&format!("rolloutByGroup.{group}"))?;

            let merged = self
                .rollout_config_for_group(group)
                .expect("group rollout should exist");
            merged.validate(&format!("rolloutByGroup.{group}"))?;
        }

        Ok(())
    }
}

impl TaskCheck {
    pub fn default_name(&self) -> String {
        if let Some(name) = self.name.as_ref() {
            return name.clone();
        }

        match self.kind {
            TaskCheckKind::Http => {
                let path = self.path.as_deref().unwrap_or("/");
                format!("HTTP Check {path}")
            }
            TaskCheckKind::Tcp => {
                let port = self
                    .port
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "unknown".into());
                format!("TCP Check {port}")
            }
            TaskCheckKind::Exec => {
                let command = self
                    .command
                    .as_ref()
                    .and_then(|command| command.first())
                    .cloned()
                    .unwrap_or_else(|| "Exec".into());
                format!("Exec Check {command}")
            }
        }
    }
}

impl TaskConfig {
    pub fn new(name: impl Into<String>, runtime: impl Into<String>) -> Self {
        TaskConfig {
            speculative_start: None,
            snapshots: None,
            suspend: None,
            autoscale: None,
            name: name.into(),
            count: default_task_count(),
            runtime: runtime.into(),
            group: default_task_group(),
            depends_on: Vec::new(),
            lifecycle: default_task_lifecycle(),
            schedule: None,
            config: empty_object(),
            resources: empty_object(),
            constraints: empty_object(),
            permit: empty_object(),
            affinities: empty_object(),
            checks: Vec::new(),
            on_unhealthy: TaskOnUnhealthyConfig::default(),
            restart: None,
            stop: None,
            disruption: None,
            gang: None,
            kv_injections: None,
            network: None,
            service: None,
            connect: None,
            identities: Vec::new(),
            stateful: None,
            volumes: Vec::new(),
            artifacts: Vec::new(),
            actions: Vec::new(),
            pending_extensions: PendingExtensions::new(),
        }
    }

    /// Effective restart policy for this task: the explicit `restart.policy`
    /// when set, else the lifecycle default (`always` for service/sidecar,
    /// `onFailure` for batch) — also when a `restart` block is declared
    /// without a `policy`.
    pub fn restart_policy(&self) -> RestartPolicy {
        match self.restart.as_ref().and_then(|restart| restart.policy) {
            Some(policy) => policy,
            None => match self.lifecycle {
                TaskLifecycle::Batch => RestartPolicy::OnFailure,
                TaskLifecycle::Service | TaskLifecycle::Sidecar => RestartPolicy::Always,
            },
        }
    }

    /// Delay before relaunching an exited task. Falls back to the shared
    /// `on_unhealthy.restart_delay` so a task that already tuned its restart
    /// pacing does not have to say it twice.
    pub fn restart_delay(&self) -> Duration {
        self.restart
            .as_ref()
            .and_then(|restart| restart.delay)
            .unwrap_or(self.on_unhealthy.restart_delay)
    }

    /// Whether an exit code counts as success for this task (`[0]` unless the
    /// task lists others). An exit with no code — killed by a signal, or a
    /// process re-adopted after a worker restart whose status we never saw —
    /// is never a success.
    pub fn exit_code_is_success(&self, exit_code: Option<i32>) -> bool {
        let Some(code) = exit_code else {
            return false;
        };
        match self.restart.as_ref() {
            Some(restart) => restart.success_exit_codes.contains(&code),
            None => code == 0,
        }
    }

    /// Apply the `service.port` number/`dynamic` shorthand, materializing the
    /// implied `network.ports` entry so stored configs are always explicit.
    pub fn normalize_network(&mut self) -> R<()> {
        let name = self.name.clone();
        crate::structs::network::normalize_task_network(
            &name,
            &mut self.network,
            &mut self.service,
        )?;
        crate::structs::network::normalize_task_connect(&name, &mut self.connect)
    }

    pub fn validate_network(&self) -> R<()> {
        // Validate the desugared form so this is correct whether or not the
        // submit path has normalized yet.
        let mut network = self.network.clone();
        let mut service = self.service.clone();
        crate::structs::network::normalize_task_network(&self.name, &mut network, &mut service)?;
        crate::structs::network::validate_task_network(
            &self.name,
            network.as_ref(),
            service.as_ref(),
        )?;

        // Check port labels must reference a declared network port.
        for check in &self.checks {
            if let Some(crate::structs::network::CheckPort::Label(label)) = check.port.as_ref() {
                let declared = network
                    .as_ref()
                    .is_some_and(|network| network.ports.iter().any(|port| &port.name == label));
                if !declared {
                    bail!(
                        "task {} check {} references port '{label}' which is not declared in network.ports",
                        self.name,
                        check.default_name()
                    );
                }
            }
        }
        Ok(())
    }

    /// Validate the task's `connect` block and `service.allow` intentions.
    /// No connect block and no allow list = today's behavior, always valid.
    /// Deliberately does not require the upstream services to exist yet.
    pub fn validate_connect(&self) -> R<()> {
        // Validate against the desugared network so a `service.port` number
        // shorthand still counts as a declared static port.
        let mut network = self.network.clone();
        let mut service = self.service.clone();
        let mut connect = self.connect.clone();
        crate::structs::network::normalize_task_network(&self.name, &mut network, &mut service)?;
        crate::structs::network::normalize_task_connect(&self.name, &mut connect)?;
        crate::structs::network::validate_task_connect(
            &self.name,
            network.as_ref(),
            connect.as_ref(),
            service.as_ref(),
        )
    }

    /// Whether this task declares a stateful block. Gates the stateful-only
    /// `{instance}`/`{slot}` placeholders in a `sub` template and the
    /// instance-assignment path in the scheduler/store.
    pub fn is_stateful(&self) -> bool {
        self.stateful.is_some()
    }

    /// Validate the task's stateful block (see `docs/stateful-workloads.md`).
    /// No stateful block = today's stateless behavior, always valid.
    pub fn validate_stateful(&self) -> R<()> {
        let Some(stateful) = self.stateful.as_ref() else {
            return Ok(());
        };

        match stateful.mode {
            InstanceMode::Slot => {
                if stateful.require_id {
                    bail!(
                        "task {} stateful.requireId is only valid in keyed mode; slot ids are \
                         always derived from the ordinal",
                        self.name
                    );
                }
                if self.count.num < 1 {
                    bail!(
                        "task {} stateful slot mode requires count.num >= 1 (slots are 0..count-1)",
                        self.name
                    );
                }
                if self.count.per.is_some() {
                    bail!(
                        "task {} stateful slot mode cannot be combined with count.per; per-attribute \
                         pools would produce colliding slot instance ids",
                        self.name
                    );
                }
                if stateful.initial_instances.is_some() {
                    bail!(
                        "task {} stateful.initialInstances is only valid in keyed mode; slot ids \
                         are always derived from the ordinal",
                        self.name
                    );
                }
            }
            InstanceMode::Session => bail!(
                "task {} sets stateful.mode = \"session\" but was not normalized into a keyed \
                 task and its envelope task; build it from a jobspec, which normalizes it",
                self.name
            ),
            InstanceMode::Keyed => {
                if stateful
                    .storage
                    .as_ref()
                    .is_some_and(|storage| storage.retain_on_stop.is_some())
                {
                    bail!(
                        "task {} stateful.storage.retainOnStop is only valid in slot mode; keyed \
                         stop/destroy semantics are fixed per instance (instance stop / job stop \
                         always retains the storage handle, instance destroy always drops it)",
                        self.name
                    );
                }
                // Keyed cardinality is driven entirely by the management API
                // (CreateInstance/DestroyInstance). An explicit `count` in the
                // job spec is rejected at parse time (see TaskConfigInput);
                // this guard re-checks `count.per` for directly-constructed
                // configs, which would otherwise be silently meaningless.
                if self.count.per.is_some() {
                    bail!(
                        "task {} stateful keyed mode cannot be combined with count.per; keyed \
                         instance cardinality is driven by the instance management API, not count",
                        self.name
                    );
                }
                // The seed list is validated here (not at seeding time) so a
                // bad jobspec fails `job validate`/submit before any state
                // changes. Duplicates are rejected because the list is a set
                // of identities — a duplicate is always a spec mistake.
                if let Some(initial) = stateful.initial_instances.as_ref() {
                    let mut seen = std::collections::BTreeSet::new();
                    for id in initial {
                        crate::structs::validate_keyed_instance_id(id).map_err(|err| {
                            anyhow::anyhow!(
                                "task {} stateful.initialInstances entry {:?}: {err}",
                                self.name,
                                id
                            )
                        })?;
                        if !seen.insert(id.as_str()) {
                            bail!(
                                "task {} stateful.initialInstances contains duplicate id {:?}",
                                self.name,
                                id
                            );
                        }
                    }
                }
            }
        }

        if let Some(storage) = stateful.storage.as_ref() {
            if storage.path.trim().is_empty() {
                bail!("task {} stateful.storage.path cannot be empty", self.name);
            }
            if !storage.path.starts_with('/') {
                bail!(
                    "task {} stateful.storage.path must be an absolute path, got {:?}",
                    self.name,
                    storage.path
                );
            }
            if storage
                .params
                .as_ref()
                .is_some_and(|params| !params.is_object())
            {
                bail!(
                    "task {} stateful.storage.params must be an object of driver parameters",
                    self.name
                );
            }
        }

        // Node-loss fencing: durations must parse and replaceAfter must
        // exceed selfTerminateAfter (or be "never"). Resolving here also
        // covers the defaulted case, so a stateful task can never reach the
        // scheduler with an unresolvable policy.
        stateful
            .resolved_on_lost()
            .map_err(|err| anyhow::anyhow!("task {}: {err}", self.name))?;

        crate::structs::session::validate_session_task(self)?;

        Ok(())
    }

    /// Validate volume declarations — `volumes[]` claims and the instance
    /// volume from `stateful.storage` (see `docs/volumes.md`).
    ///
    /// Only spec-shape rules live here. Whether the named plugin exists, and
    /// whether its required params are present, needs the cluster's advertised
    /// plugin capabilities and so is enforced at admission
    /// ([`crate::volumes` capability checks in the manager]).
    pub fn validate_volumes(&self) -> R<()> {
        let mut seen = std::collections::BTreeSet::new();

        // The instance volume owns the reserved name, so a task claim cannot
        // collide with it and quietly shadow the instance's storage.
        if let Some(storage) = self
            .stateful
            .as_ref()
            .and_then(|stateful| stateful.storage.as_ref())
        {
            seen.insert(INSTANCE_VOLUME_NAME.to_string());
            if storage
                .driver
                .as_deref()
                .is_some_and(|name| name.trim().is_empty())
            {
                bail!(
                    "task {} stateful.storage.driver cannot be empty; omit it to use the default \
                     ({})",
                    self.name,
                    crate::volumes::DEFAULT_VOLUME_DRIVER
                );
            }
        }

        for volume in &self.volumes {
            validate_volume_name(&self.name, &volume.name)?;
            if !seen.insert(volume.name.clone()) {
                bail!(
                    "task {} declares volume {:?} more than once (names become \
                     LEGION_VOLUME_* and must be unique)",
                    self.name,
                    volume.name
                );
            }
            if volume
                .driver
                .as_deref()
                .is_some_and(|name| name.trim().is_empty())
            {
                bail!(
                    "task {} volume {:?} has an empty driver; omit it to use the default ({})",
                    self.name,
                    volume.name,
                    crate::volumes::DEFAULT_VOLUME_DRIVER
                );
            }
            if volume
                .params
                .as_ref()
                .is_some_and(|params| !params.is_object())
            {
                bail!(
                    "task {} volume {:?} params must be an object of driver parameters",
                    self.name,
                    volume.name
                );
            }
            if let Some(path) = volume.path.as_deref() {
                if !path.starts_with('/') {
                    bail!(
                        "task {} volume {:?} path must be absolute, got {:?}",
                        self.name,
                        volume.name,
                        path
                    );
                }
            }
        }

        // A container has a filesystem view, so a volume with no destination
        // would be mounted nowhere: the task would silently not see it. Exec
        // tasks have no such view and read `$LEGION_VOLUME_<NAME>` instead, so
        // `path` is optional there.
        if self.runtime == "container" {
            for volume in &self.volumes {
                if volume.path.is_none() {
                    bail!(
                        "task {} volume {:?} needs a `path`: container tasks mount volumes into \
                         the container filesystem, so Legion needs a destination",
                        self.name,
                        volume.name
                    );
                }
            }
        }

        Ok(())
    }

    /// Volume drivers this task needs a node to provide, across its
    /// `stateful.storage` and its `volumes[]` claims.
    ///
    /// Placement filters candidate nodes on this set, and job admission checks
    /// the cluster advertises every entry — the two ways a job with unavailable
    /// storage is stopped before a task ever fails to start.
    pub fn required_volume_drivers(&self) -> std::collections::BTreeSet<String> {
        let mut drivers = std::collections::BTreeSet::new();
        if let Some(storage) = self
            .stateful
            .as_ref()
            .and_then(|stateful| stateful.storage.as_ref())
        {
            drivers.insert(storage.driver_name().to_string());
        }
        for volume in &self.volumes {
            drivers.insert(volume.driver_name().to_string());
        }
        drivers
    }

    /// The volume declarations of this task as `(driver, size param)` pairs,
    /// for placement-time disk accounting. Instance storage first, then
    /// claims. Sizes that are absent or unparseable yield `None` — the
    /// scheduler skips those rather than guessing.
    pub fn volume_size_declarations(&self) -> Vec<(String, Option<u64>)> {
        let size_of = |params: &std::collections::BTreeMap<String, Value>| {
            crate::structs::job::volume_param_size_bytes(params)
        };
        let mut out = Vec::new();
        if let Some(storage) = self
            .stateful
            .as_ref()
            .and_then(|stateful| stateful.storage.as_ref())
        {
            out.push((
                storage.driver_name().to_string(),
                size_of(&storage.param_map()),
            ));
        }
        for volume in &self.volumes {
            out.push((
                volume.driver_name().to_string(),
                size_of(&volume.param_map()),
            ));
        }
        out
    }

    /// Validate workload identities declared on this task (see the
    /// workload-identity OIDC plan). No identities = today's behavior.
    /// Whether this task runs as a sandbox that keeps every env-delivered
    /// identity token fresh at `/legion/identity/<ENV>` in its guest: one
    /// launched for memory checkpoints, so it has an identity disk and a
    /// resume handler (run by the guest agent on a gated sandbox). Its env
    /// identities may use `changeMode = "noop"` (docs/workload-identity.md,
    /// "Sandboxes").
    pub fn sandbox_refreshes_env_tokens(&self) -> bool {
        self.runtime == "sandbox" && crate::structs::snapshot::memory_checkpoint_ask(self).is_some()
    }

    pub fn validate_identities(&self) -> R<()> {
        crate::structs::workload_identity::validate_identities_with(
            &self.name,
            &self.identities,
            self.is_stateful(),
            self.sandbox_refreshes_env_tokens(),
            &self.injected_identity_names(),
        )
    }

    /// The identities this task's `connect.upstreams` credentials inject
    /// (`from = { identity }`), each once.
    pub fn injected_identity_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self
            .connect
            .iter()
            .flat_map(|connect| connect.upstreams.iter())
            .flat_map(|upstream| upstream.credentials.iter())
            .filter_map(|credential| credential.identity_name())
            .collect();
        names.sort_unstable();
        names.dedup();
        names
    }

    /// Validate artifacts declared on this task (artifact system plan):
    /// spec shape, name uniqueness, `${artifact.<name>}` references in the
    /// kind config, and `auth.identity` references. No artifacts = today's
    /// behavior. Cluster-scoped rules (scheme allowlist, nodeIdentity names)
    /// are enforced at admission where manager config is available.
    pub fn validate_artifacts(&self) -> R<()> {
        if self.artifacts.is_empty() {
            return Ok(());
        }
        validate_task_artifacts(&self.name, &self.artifacts, &self.config, &self.identities)
    }

    /// The task's effective stop ladder — the declared block, else the
    /// defaults. There is no "no ladder" case: every stop signals first.
    pub fn stop_config(&self) -> StopConfig {
        self.stop.clone().unwrap_or_default()
    }

    /// How many of this task's allocations voluntary disruption must leave
    /// running, resolved against its declared count. 0 when undeclared.
    pub fn min_available(&self) -> u32 {
        self.disruption
            .as_ref()
            .map(|disruption| disruption.min_available_for(self.count.num))
            .unwrap_or(0)
    }

    pub fn validate_stop(&self) -> R<()> {
        match self.stop.as_ref() {
            Some(stop) => stop.validate(&self.name),
            None => Ok(()),
        }
    }

    /// Validate the task's `speculativeStart` block (docs/edge-boot.md).
    ///
    /// Run at admission rather than at dispatch on purpose: a typo in `mode`
    /// that was silently read as `off` would be discovered as a latency
    /// regression in production, weeks later, by someone who was not there
    /// when it was written.
    pub fn validate_speculative_start(&self) -> Result<(), String> {
        let Some(block) = self.speculative_start.as_ref() else {
            return Ok(());
        };
        block
            .resolve()
            .map(|_| ())
            .map_err(|err| format!("task {}: {err:#}", self.name))
    }

    pub fn validate_disruption(&self) -> R<()> {
        match self.disruption.as_ref() {
            Some(disruption) => disruption.validate(&self.name),
            None => Ok(()),
        }
    }

    /// Validate the task's `gang` block (docs/gang-scheduling.md). The rules
    /// that need the rest of the job — the task's group and its rollout —
    /// live in `task_plugins::validate_job_config`.
    pub fn validate_gang(&self) -> R<()> {
        let Some(gang) = self.gang.as_ref() else {
            return Ok(());
        };
        if self.count.per.is_some() {
            bail!(
                "task {} sets gang, which cannot be combined with count.per: a per-attribute \
                 count has no single membership to place together. Use a fixed count and \
                 constraints instead",
                self.name
            );
        }
        if self.stateful.as_ref().is_some_and(|stateful| {
            matches!(stateful.mode, InstanceMode::Keyed | InstanceMode::Session)
        }) {
            bail!(
                "task {} sets gang, but its stateful mode is keyed: a gang needs a fixed \
                 membership, and a keyed task's instances come and go through the instance \
                 API. Slot-mode stateful tasks can be gangs",
                self.name
            );
        }
        gang.validate(&self.name, self.count.num)
    }

    /// Validate declared actions: unique names, well-formed params, and that
    /// every `${param.<name>}` placeholder in a command refers to a declared
    /// param. Also warns (does not reject) when a param is interpolated into a
    /// `sh -c`-style command element.
    pub fn validate_actions(&self) -> R<()> {
        let mut seen = std::collections::HashSet::new();
        for action in &self.actions {
            action
                .validate()
                .map_err(|e| anyhow!("task {} action invalid: {e}", self.name))?;
            if !seen.insert(action.name.as_str()) {
                bail!(
                    "task {} declares duplicate action `{}`",
                    self.name,
                    action.name
                );
            }
            // Every placeholder must reference a declared param.
            for element in &action.command {
                let mut rest = element.as_str();
                while let Some(idx) = rest.find("${param.") {
                    let after = &rest[idx + "${param.".len()..];
                    let Some(end) = after.find('}') else {
                        bail!(
                            "task {} action `{}` has an unterminated ${{param.}} placeholder",
                            self.name,
                            action.name
                        );
                    };
                    let param_name = &after[..end];
                    if !action.params.iter().any(|p| p.name == param_name) {
                        bail!(
                            "task {} action `{}` references undeclared param `{}`",
                            self.name,
                            action.name,
                            param_name
                        );
                    }
                    rest = &after[end + 1..];
                }
            }
            // Warn on shell-interpolation footgun (param inside `sh -c`).
            let is_shell = matches!(action.command.first().map(String::as_str), Some(cmd)
                if cmd.ends_with("sh") || cmd == "bash" || cmd == "zsh")
                && action.command.iter().any(|a| a == "-c");
            if is_shell && action.command.iter().any(|a| a.contains("${param.")) {
                warn!(
                    task = %self.name,
                    action = %action.name,
                    "action interpolates a param inside a shell command; \
                     values are validated but shell quoting is the author's responsibility"
                );
            }
        }
        Ok(())
    }

    pub fn validate_checks(&self) -> R<()> {
        let has_boot_checks = self
            .checks
            .iter()
            .any(|check| matches!(check.check_type, TaskCheckType::Boot));
        if has_boot_checks
            && self
                .checks
                .iter()
                .any(|check| check.boot_grace_time.is_some())
        {
            bail!("boot_grace_time cannot be set when boot checks are configured");
        }

        for check in &self.checks {
            if check.unhealthy_after_count == 0 {
                bail!(
                    "health check {} unhealthy_after_count must be at least 1",
                    check.default_name()
                );
            }

            match check.kind {
                TaskCheckKind::Exec => {
                    if check.path.is_some() || check.port.is_some() {
                        bail!(
                            "exec health check {} cannot specify path or port",
                            check.default_name()
                        );
                    }
                    let command = check
                        .command
                        .as_ref()
                        .and_then(|command| command.first())
                        .map(String::as_str)
                        .unwrap_or_default();
                    if command.is_empty() {
                        bail!(
                            "exec health check {} requires a command",
                            check.default_name()
                        );
                    }
                }
                TaskCheckKind::Http | TaskCheckKind::Tcp => {
                    if check.port.is_none() {
                        bail!(
                            "{} check {} requires a port",
                            match check.kind {
                                TaskCheckKind::Http => "http",
                                TaskCheckKind::Tcp => "tcp",
                                TaskCheckKind::Exec => "exec",
                            },
                            check.default_name()
                        );
                    }
                    // TCP checks only dial the port; a path is meaningless for them.
                    if check.path.is_none() && matches!(check.kind, TaskCheckKind::Http) {
                        bail!("http check {} requires a path", check.default_name());
                    }
                    if check.command.is_some() {
                        bail!(
                            "{} check {} cannot include command",
                            match check.kind {
                                TaskCheckKind::Http => "http",
                                TaskCheckKind::Tcp => "tcp",
                                TaskCheckKind::Exec => "exec",
                            },
                            check.default_name()
                        );
                    }
                }
            }
        }

        Ok(())
    }

    pub fn validate_kv_injections(&self) -> R<()> {
        let Some(injections) = self.kv_injections.as_ref() else {
            return Ok(());
        };

        let defaults = injections.kv.clone().unwrap_or_default();
        for env in &injections.env {
            if env.name.trim().is_empty() {
                bail!("task {} kv_injections.env name cannot be empty", self.name);
            }
            let scope = format!("task {} env {}", self.name, env.name);
            validate_template_source(&scope, &env.value)?;
            let policy = env.kv.resolve_with_defaults(&defaults);
            if policy.on_change == KvInjectionAction::Update {
                bail!(
                    "{scope}: onChange = \"update\" cannot change a running process's \
                     environment; use \"restart\" or \"rollout\""
                );
            }
            if policy.on_delete == KvInjectionAction::Update {
                bail!(
                    "{scope}: onDelete cannot be \"update\"; a deleted key has nothing to render"
                );
            }
        }

        for file in &injections.files {
            if file.path.trim().is_empty() {
                bail!(
                    "task {} kv_injections.files path cannot be empty",
                    self.name
                );
            }
            let scope = format!("task {} file {}", self.name, file.path);
            validate_template_source(&scope, &file.file)?;
            let policy = file.kv.resolve_with_defaults(&defaults);
            if policy.on_delete == KvInjectionAction::Update {
                bail!(
                    "{scope}: onDelete cannot be \"update\"; a deleted key has nothing to render"
                );
            }
            let live = policy.on_change == KvInjectionAction::Update;
            for (field, set) in [
                ("changeMode", file.change_mode.is_some()),
                ("changeSignal", file.change_signal.is_some()),
                ("splay", file.splay.is_some()),
            ] {
                if set && !live {
                    bail!("{scope}: {field} only applies with kv.onChange = \"update\"");
                }
            }
            if file.change_mode == Some(crate::structs::workload_identity::ChangeMode::Signal) {
                let signal = file.change_signal.as_deref().unwrap_or("").trim();
                if signal.is_empty() {
                    bail!("{scope}: changeMode = \"signal\" needs changeSignal");
                }
                crate::structs::workload_identity::normalize_change_signal(signal)
                    .map_err(|err| anyhow!("{scope}: {err}"))?;
            } else if file.change_signal.is_some() {
                bail!("{scope}: changeSignal needs changeMode = \"signal\"");
            }
            if let Some(splay) = file.splay.as_deref() {
                crate::structs::netpolicy::parse_duration(splay)
                    .map_err(|err| anyhow!("{scope}: splay: {err}"))?;
            }
        }

        Ok(())
    }
}

fn validate_template_source(scope: &str, source: &KvTemplateSource) -> R<()> {
    let has_template = source
        .template
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());
    let has_template_kv = source
        .template_kv
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());

    match (has_template, has_template_kv) {
        (true, false) | (false, true) => Ok(()),
        (true, true) => bail!("{scope} cannot set both template and template_kv"),
        (false, false) => bail!("{scope} must set exactly one of template or template_kv"),
    }
}

impl Submission {
    pub fn hash(&self) -> String {
        let hash = Sha256::digest(self.content.as_bytes());
        format!("{:x}", hash)
    }
}

// Unknown-field capture (`structs::spec_fields`): the keys each block
// accepts, for did-you-mean, and the blocks nested in it. `gang`, `count`,
// and `schedule` are parsed by hand and refuse unknown keys on their own.

spec_block!(
    TaskCheck,
    [
        "type",
        "name",
        "kind",
        "path",
        "port",
        "command",
        "every",
        "timeout",
        "unhealthyAfterCount",
        "bootGraceTime",
        "unhealthyAfterCountTimespan"
    ]
);
spec_block!(
    TaskOnUnhealthyConfig,
    [
        "restartCount",
        "restartPeriod",
        "restartDelay",
        "moveCount",
        "movePeriod",
        "moveDelay"
    ]
);
spec_block!(TaskRestartConfig, ["policy", "delay", "successExitCodes"]);
spec_block!(RolloutSlotsConfig, ["order", "updateFrom"]);
spec_block!(
    RolloutConfig,
    [
        "strategy",
        "progression",
        "replacement",
        "inPlace",
        "percentEachStep",
        "countEachStep",
        "percentSteps",
        "countSteps",
        "stepStagger",
        "allocationStagger",
        "minHealthyTime",
        "healthyDeadline",
        "ignoreHealthChecks",
        "capacityBlockTimeout",
        "onCapacityTimeout",
        "rollbackOnFailure",
        "onHealthDeadline",
        "batchKillAfter",
        "slots"
    ],
    |this, walk| {
        walk.opt("slots", this.slots.as_mut());
    }
);
spec_block!(
    StatefulConfig,
    [
        "mode",
        "requireId",
        "initialInstances",
        "storage",
        "onLost",
        "envelope",
        "promote",
        "placement",
        "fenceAfter"
    ],
    |this, walk| {
        walk.opt("storage", this.storage.as_mut());
        walk.opt("onLost", this.on_lost.as_mut());
        walk.opt("envelope", this.envelope.as_mut());
        walk.opt("promote", this.promote.as_mut());
    }
);
spec_block!(StatefulOnLostConfig, ["selfTerminateAfter", "replaceAfter"]);
spec_block!(
    StatefulStorageConfig,
    [
        "path",
        "driver",
        "mode",
        "params",
        "retainOnScaleIn",
        "retainOnStop"
    ]
);
spec_block!(StopConfig, ["signal", "killAfter", "drain"]);
spec_block!(DisruptionConfig, ["minAvailable"]);
extension_point!(
    VolumeSpec,
    ExtensionPoint::Volume,
    ["name", "driver", "mode", "params", "path", "readOnly"]
);
spec_block!(
    ActionSpec,
    ["name", "description", "command", "params", "tty", "timeout"],
    |this, walk| {
        walk.list("params", &mut this.params);
    }
);
spec_block!(
    ActionParam,
    [
        "name",
        "description",
        "type",
        "default",
        "required",
        "values",
        "pattern",
        "min",
        "max",
        "sensitive"
    ]
);
spec_block!(KvInjectionPolicy, ["watch", "onChange", "onDelete"]);
spec_block!(KvTemplateSource, ["template", "templateKv"]);
spec_block!(TaskKvEnvInjection, ["name", "value", "kv"], |this, walk| {
    walk.block("value", &mut this.value);
    walk.block("kv", &mut this.kv);
});
spec_block!(
    TaskKvFileInjection,
    [
        "path",
        "mode",
        "file",
        "kv",
        "changeMode",
        "changeSignal",
        "splay"
    ],
    |this, walk| {
        walk.block("file", &mut this.file);
        walk.block("kv", &mut this.kv);
    }
);
spec_block!(TaskKvInjections, ["kv", "env", "files"], |this, walk| {
    walk.opt("kv", this.kv.as_mut());
    walk.list("env", &mut this.env);
    walk.list("files", &mut this.files);
});

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_action() -> ActionSpec {
        ActionSpec {
            name: "flush-cache".into(),
            description: String::new(),
            command: vec![
                "/srv/api".into(),
                "flush".into(),
                "--scope".into(),
                "${param.scope}".into(),
            ],
            params: vec![ActionParam {
                name: "scope".into(),
                description: String::new(),
                param_type: ActionParamType::Enum,
                default: Some("sessions".into()),
                required: None,
                values: vec!["all".into(), "sessions".into()],
                pattern: None,
                min: None,
                max: None,
                sensitive: false,
                unknown_fields: Default::default(),
            }],
            tty: false,
            timeout: None,
            unknown_fields: Default::default(),
        }
    }

    #[test]
    fn action_render_substitutes_validated_params() {
        let action = sample_action();
        let mut supplied = std::collections::HashMap::new();
        supplied.insert("scope".to_string(), "all".to_string());
        let (argv, resolved) = action.render_command(&supplied).unwrap();
        assert_eq!(argv, vec!["/srv/api", "flush", "--scope", "all"]);
        assert_eq!(resolved.get("scope").unwrap(), "all");
    }

    #[test]
    fn action_render_uses_default_when_param_omitted() {
        let action = sample_action();
        let (argv, _) = action
            .render_command(&std::collections::HashMap::new())
            .unwrap();
        assert_eq!(argv.last().unwrap(), "sessions");
    }

    #[test]
    fn action_render_rejects_invalid_enum_and_unknown_params() {
        let action = sample_action();
        let mut bad = std::collections::HashMap::new();
        bad.insert("scope".to_string(), "nope".to_string());
        assert!(action.render_command(&bad).unwrap_err().contains("one of"));

        let mut unknown = std::collections::HashMap::new();
        unknown.insert("bogus".to_string(), "x".to_string());
        assert!(action
            .render_command(&unknown)
            .unwrap_err()
            .contains("unknown parameter"));
    }

    #[test]
    fn action_int_bounds_enforced() {
        let param = ActionParam {
            name: "lines".into(),
            description: String::new(),
            param_type: ActionParamType::Int,
            default: None,
            required: None,
            values: vec![],
            pattern: None,
            min: Some(1),
            max: Some(10),
            sensitive: false,
            unknown_fields: Default::default(),
        };
        assert!(param.validate_value("5").is_ok());
        assert!(param.validate_value("0").unwrap_err().contains(">= 1"));
        assert!(param.validate_value("11").unwrap_err().contains("<= 10"));
        assert!(param.validate_value("x").unwrap_err().contains("integer"));
    }

    #[test]
    fn action_validate_rejects_undeclared_placeholder() {
        let mut task = TaskConfig::new("t", "exec");
        task.actions = vec![ActionSpec {
            name: "bad".into(),
            description: String::new(),
            command: vec!["/bin/echo".into(), "${param.missing}".into()],
            params: vec![],
            tty: false,
            timeout: None,
            unknown_fields: Default::default(),
        }];
        let err = task.validate_actions().unwrap_err().to_string();
        assert!(err.contains("undeclared param"), "got: {err}");
    }

    #[test]
    fn deserializes_task_list_jobspec() {
        let spec = serde_json::json!({
            "name": "demo",
            "tasks": [
                {
                    "runtime": "noop",
                    "count": 1,
                    "config": {},
                    "resources": {},
                    "constraints": {},
                }
            ],
        });

        let config: JobConfig =
            serde_json::from_value(spec).expect("tasks list should deserialize");

        assert_eq!(config.name, "demo");
        assert_eq!(config.tasks.len(), 1);
        assert_eq!(config.tasks[0].name, "task-1");
    }

    #[test]
    fn deserializes_task_map_jobspec() {
        let spec = serde_json::json!({
            "name": "demo",
            "tasks": {
                "first": {
                    "runtime": "noop",
                    "config": {},
                    "resources": {},
                }
            }
        });

        let config: JobConfig = serde_json::from_value(spec).expect("tasks map should deserialize");

        assert_eq!(config.name, "demo");
        assert_eq!(config.tasks.len(), 1);
        assert_eq!(config.tasks[0].name, "first");
        assert_eq!(config.tasks[0].count.num, 1);
    }

    #[test]
    fn deserializes_single_task_jobspec() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "noop",
            },
        });

        let config: JobConfig = serde_json::from_value(spec).expect("task should deserialize");

        assert_eq!(config.name, "demo");
        assert_eq!(config.tasks.len(), 1);
        assert_eq!(config.tasks[0].name, "task");
    }

    #[test]
    fn deserializes_named_task_list_jobspec() {
        let spec = serde_json::json!({
            "name": "demo",
            "tasks": [
                {
                    "name": "named",
                    "runtime": "noop",
                }
            ],
        });

        let config: JobConfig =
            serde_json::from_value(spec).expect("named tasks list should deserialize");

        assert_eq!(config.name, "demo");
        assert_eq!(config.tasks.len(), 1);
        assert_eq!(config.tasks[0].name, "named");
    }

    #[test]
    fn deserializes_task_count_per_attribute() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "noop",
                "count": { "num": 3, "per": "region" }
            }
        });

        let config: JobConfig = serde_json::from_value(spec).expect("task should deserialize");

        assert_eq!(config.tasks.len(), 1);
        assert_eq!(config.tasks[0].count.num, 3);
        assert_eq!(config.tasks[0].count.per.as_deref(), Some("region"));
    }

    #[test]
    fn deserializes_task_group_lifecycle_defaults() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "noop",
            }
        });

        let config: JobConfig = serde_json::from_value(spec).expect("task should deserialize");

        assert_eq!(config.tasks[0].group, "default");
        assert_eq!(config.tasks[0].lifecycle, TaskLifecycle::Service);
        assert!(config.tasks[0].depends_on.is_empty());
    }

    #[test]
    fn deserializes_batch_schedule_every() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "noop",
                "lifecycle": "batch",
                "schedule": "every 5m"
            }
        });

        let config: JobConfig = serde_json::from_value(spec).expect("task should deserialize");

        assert_eq!(
            config.tasks[0].schedule,
            Some(TaskSchedule::Every(Duration::from_secs(300)))
        );
    }

    #[test]
    fn deserializes_batch_schedule_cron() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "noop",
                "lifecycle": "batch",
                "schedule": "0 * * * * *"
            }
        });

        let config: JobConfig = serde_json::from_value(spec).expect("task should deserialize");

        assert_eq!(
            config.tasks[0].schedule,
            Some(TaskSchedule::Cron("0 * * * * *".to_string()))
        );
    }

    #[test]
    fn deserializes_task_dependencies() {
        let spec = serde_json::json!({
            "name": "demo",
            "tasks": {
                "build": {
                    "runtime": "noop"
                },
                "deploy": {
                    "runtime": "noop",
                    "dependsOn": [
                        "build",
                        { "task": "build", "condition": "success" }
                    ]
                }
            }
        });

        let config: JobConfig = serde_json::from_value(spec).expect("task should deserialize");
        let deploy = config
            .tasks
            .iter()
            .find(|task| task.name == "deploy")
            .unwrap();
        assert_eq!(deploy.depends_on.len(), 2);
        assert_eq!(deploy.depends_on[0].task, "build");
        assert_eq!(
            deploy.depends_on[0].condition,
            TaskDependencyCondition::Start
        );
        assert_eq!(
            deploy.depends_on[1].condition,
            TaskDependencyCondition::Success
        );
    }

    #[test]
    fn deserializes_rollout_with_group_overrides() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "tasks": {
                "web": { "runtime": "noop", "group": "frontend" },
                "api": { "runtime": "noop", "group": "backend" }
            },
            "rollout": {
                "percentEachStep": 10,
                "replacement": "createFirst",
                "inPlace": false
            },
            "rolloutByGroup": {
                "frontend": {
                    "countEachStep": 2,
                    "inPlace": true
                }
            }
        });

        let config: JobConfig = serde_json::from_value(spec)?;
        let frontend = config
            .rollout_config_for_group("frontend")
            .expect("missing frontend rollout");
        assert_eq!(frontend.count_each_step, Some(2));
        assert!(frontend.percent_each_step.is_none());
        assert_eq!(frontend.in_place, Some(true));

        let backend = config
            .rollout_config_for_group("backend")
            .expect("missing backend rollout");
        assert_eq!(backend.percent_each_step, Some(10));
        assert!(backend.count_each_step.is_none());

        Ok(())
    }

    #[test]
    fn rollout_validation_rejects_conflicting_step_modes() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": { "runtime": "noop" },
            "rollout": {
                "countEachStep": 1,
                "percentEachStep": 10
            }
        });

        let config: JobConfig = serde_json::from_value(spec).expect("config should deserialize");
        let err = config
            .validate_rollout()
            .expect_err("conflicting rollout step modes should fail");
        assert!(
            err.to_string().contains("conflicting step modes"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rollout_validation_rejects_invalid_percent_steps() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": { "runtime": "noop" },
            "rollout": {
                "percentSteps": [10, 20, 90]
            }
        });

        let config: JobConfig = serde_json::from_value(spec).expect("config should deserialize");
        let err = config
            .validate_rollout()
            .expect_err("percent steps without 100 should fail");
        assert!(
            err.to_string().contains("must end with 100"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rollout_validation_rejects_unknown_group() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "noop",
                "group": "frontend"
            },
            "rolloutByGroup": {
                "backend": {
                    "countEachStep": 1
                }
            }
        });

        let config: JobConfig = serde_json::from_value(spec).expect("config should deserialize");
        let err = config
            .validate_rollout()
            .expect_err("unknown rollout group should fail");
        assert!(
            err.to_string().contains("unknown group"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rollout_slots_deserialize_and_round_trip() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": { "runtime": "noop" },
            "rollout": {
                "slots": { "order": "sequential", "updateFrom": 2 }
            }
        });

        let config: JobConfig = serde_json::from_value(spec)?;
        let slots = config
            .rollout
            .as_ref()
            .and_then(|rollout| rollout.slots.as_ref())
            .expect("missing slots block");
        assert_eq!(slots.order, Some(RolloutSlotOrder::Sequential));
        assert_eq!(slots.update_from, Some(2));
        assert!(slots.is_sequential());

        // Round trip preserves the block (it is persisted in the rollout
        // row's config_json and re-parsed every tick).
        let raw = serde_json::to_string(config.rollout.as_ref().unwrap())?;
        let parsed: RolloutConfig = serde_json::from_str(&raw)?;
        assert_eq!(parsed.slots, config.rollout.as_ref().unwrap().slots);
        Ok(())
    }

    #[test]
    fn rollout_slots_reject_garbage_at_parse() {
        // Unknown order value.
        let err = serde_json::from_value::<RolloutConfig>(serde_json::json!({
            "slots": { "order": "alphabetical" }
        }))
        .expect_err("garbage order should fail to parse");
        assert!(err.to_string().contains("alphabetical"));

        // Negative updateFrom.
        assert!(serde_json::from_value::<RolloutConfig>(serde_json::json!({
            "slots": { "updateFrom": -1 }
        }))
        .is_err());

        // Non-numeric updateFrom.
        assert!(serde_json::from_value::<RolloutConfig>(serde_json::json!({
            "slots": { "updateFrom": "two" }
        }))
        .is_err());
    }

    #[test]
    fn rollout_validation_rejects_update_from_without_sequential() {
        for slots in [
            serde_json::json!({ "updateFrom": 1 }),
            serde_json::json!({ "order": "parallel", "updateFrom": 1 }),
        ] {
            let spec = serde_json::json!({
                "name": "demo",
                "task": { "runtime": "noop" },
                "rollout": { "slots": slots }
            });
            let config: JobConfig =
                serde_json::from_value(spec).expect("config should deserialize");
            let err = config
                .validate_rollout()
                .expect_err("updateFrom without sequential should fail");
            assert!(
                err.to_string().contains("requires slots.order"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn rollout_validation_rejects_sequential_with_step_mode() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": { "runtime": "noop" },
            "rollout": {
                "percentSteps": [50, 100],
                "slots": { "order": "sequential" }
            }
        });

        let config: JobConfig = serde_json::from_value(spec).expect("config should deserialize");
        let err = config
            .validate_rollout()
            .expect_err("sequential plus a step mode should fail");
        assert!(
            err.to_string().contains("cannot combine slots.order"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn group_sequential_slots_override_replaces_global_step_mode() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "tasks": {
                "db": { "runtime": "noop", "group": "data" },
                "api": { "runtime": "noop", "group": "backend" }
            },
            "rollout": { "percentSteps": [25, 100] },
            "rolloutByGroup": {
                "data": { "slots": { "order": "sequential", "updateFrom": 1 } }
            }
        });

        let config: JobConfig = serde_json::from_value(spec)?;
        // The sequential slots override is a progression mode of its own:
        // it replaces the global percentSteps for this group (mirroring how
        // a group step-mode override replaces the global step mode).
        let data = config
            .rollout_config_for_group("data")
            .expect("missing data rollout");
        assert!(data.percent_steps.is_none());
        assert_eq!(
            data.slots.as_ref().and_then(|slots| slots.update_from),
            Some(1)
        );
        config.validate_rollout()?;

        // Untouched group keeps the global step mode and gains no slots.
        let backend = config
            .rollout_config_for_group("backend")
            .expect("missing backend rollout");
        assert_eq!(backend.percent_steps, Some(vec![25, 100]));
        assert!(backend.slots.is_none());

        // The reverse: a group step-mode override clears a global
        // sequential slots block instead of merging into a conflict.
        let reverse: JobConfig = serde_json::from_value(serde_json::json!({
            "name": "demo",
            "tasks": { "db": { "runtime": "noop", "group": "data" } },
            "rollout": { "slots": { "order": "sequential" } },
            "rolloutByGroup": { "data": { "countEachStep": 1 } }
        }))?;
        let data = reverse
            .rollout_config_for_group("data")
            .expect("missing data rollout");
        assert_eq!(data.count_each_step, Some(1));
        assert!(data.slots.is_none());
        reverse.validate_rollout()?;
        Ok(())
    }

    #[test]
    fn kv_injection_validation_rejects_both_template_sources() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "exec",
                "env": [{
                    "name": "A",
                    "value": { "template": "x", "templateKv": "templates/x" }
                }]
            }
        });
        let config: JobConfig = serde_json::from_value(spec).expect("config should deserialize");
        let err = config.tasks[0]
            .validate_kv_injections()
            .expect_err("expected validation failure");
        assert!(err
            .to_string()
            .contains("cannot set both template and template_kv"));
    }

    #[test]
    fn kv_policy_resolution_uses_hard_defaults_when_everything_is_silent() {
        let resolved =
            KvInjectionPolicy::default().resolve_with_defaults(&KvInjectionPolicy::default());
        assert!(!resolved.watch);
        assert_eq!(resolved.on_change, KvInjectionAction::None);
        assert_eq!(resolved.on_delete, KvInjectionAction::Fail);
    }

    #[test]
    fn kv_policy_resolution_inherits_defaults_for_silent_entry_fields() {
        let defaults = KvInjectionPolicy {
            watch: Some(true),
            on_change: Some(KvInjectionAction::Restart),
            on_delete: Some(KvInjectionAction::None),
            unknown_fields: Default::default(),
        };
        // Entry only overrides on_change; watch and on_delete inherit.
        let entry = KvInjectionPolicy {
            watch: None,
            on_change: Some(KvInjectionAction::Rollout),
            on_delete: None,
            unknown_fields: Default::default(),
        };
        let resolved = entry.resolve_with_defaults(&defaults);
        assert!(resolved.watch);
        assert_eq!(resolved.on_change, KvInjectionAction::Rollout);
        assert_eq!(resolved.on_delete, KvInjectionAction::None);
    }

    #[test]
    fn kv_policy_resolution_lets_entry_disable_watch_explicitly() {
        let defaults = KvInjectionPolicy {
            watch: Some(true),
            on_change: Some(KvInjectionAction::Rollout),
            on_delete: Some(KvInjectionAction::Fail),
            unknown_fields: Default::default(),
        };
        let entry = KvInjectionPolicy {
            watch: Some(false),
            on_change: None,
            on_delete: None,
            unknown_fields: Default::default(),
        };
        let resolved = entry.resolve_with_defaults(&defaults);
        assert!(!resolved.watch);
        assert_eq!(resolved.on_change, KvInjectionAction::Rollout);
    }

    #[test]
    fn identities_deserialize_from_camel_case_spec() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "exec",
                "identities": [
                    {
                        "name": "aws",
                        "audience": ["sts.amazonaws.com"],
                        "ttl": "15m",
                        "file": { "path": "/var/run/legion/identity/aws/token", "mode": "0400" },
                        "changeMode": "noop"
                    },
                    {
                        "name": "vault",
                        "audience": ["vault.internal"],
                        "ttl": "5m",
                        "env": "VAULT_WORKLOAD_TOKEN",
                        "changeMode": "signal",
                        "changeSignal": "SIGHUP",
                        "extraClaims": { "app.example.com/tier": "backend" }
                    }
                ]
            }
        });
        let config: JobConfig = serde_json::from_value(spec)?;
        let task = &config.tasks[0];
        assert_eq!(task.identities.len(), 2);
        assert_eq!(task.identities[0].name, "aws");
        assert!(task.identities[0].delivers_file());
        assert_eq!(task.identities[1].change_signal.as_deref(), Some("SIGHUP"));
        assert_eq!(
            task.identities[1].extra_claims["app.example.com/tier"],
            "backend"
        );
        task.validate_identities()?;
        Ok(())
    }

    #[test]
    fn identity_with_empty_audience_fails_validation() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "exec",
                "identities": [
                    { "name": "aws", "audience": [], "env": "TOKEN", "changeMode": "restart" }
                ]
            }
        });
        let config: JobConfig = serde_json::from_value(spec).expect("should deserialize");
        let err = config.tasks[0]
            .validate_identities()
            .expect_err("empty audience must be rejected");
        assert!(err.to_string().contains("audience"), "{err}");
    }

    #[test]
    fn an_identity_named_by_an_upstream_credential_needs_no_delivery_test() {
        let task = |identities: serde_json::Value, from: serde_json::Value| -> TaskConfig {
            let spec = serde_json::json!({
                "name": "demo",
                "task": {
                    "runtime": "exec",
                    "identities": identities,
                    "connect": { "upstreams": [{
                        "name": "api", "to": "https://api.example.com",
                        "credentials": [{ "header": "authorization", "prefix": "Bearer ", "from": from }],
                    }] },
                }
            });
            let config: JobConfig = serde_json::from_value(spec).expect("should deserialize");
            config.tasks[0].clone()
        };

        // Injection only: no env, no file.
        let ok = task(
            serde_json::json!([{ "name": "api", "audience": ["https://api.example.com"] }]),
            serde_json::json!({ "identity": "api" }),
        );
        assert_eq!(ok.injected_identity_names(), vec!["api"]);
        ok.validate_identities()
            .expect("an injected identity needs no delivery");
        ok.validate_connect().expect("connect validates");

        // Not injected and not delivered is still an error, naming the way out.
        let err = task(
            serde_json::json!([{ "name": "api", "audience": ["https://api.example.com"] }]),
            serde_json::json!({ "kv": "secrets/api" }),
        )
        .validate_identities()
        .expect_err("an identity nobody receives");
        assert!(err.to_string().contains("from = { identity"), "{err}");

        // The credential must name a declared identity.
        let err = task(
            serde_json::json!([]),
            serde_json::json!({ "identity": "api" }),
        )
        .validate_identities()
        .expect_err("undeclared identity");
        assert!(err.to_string().contains("declares no identities"), "{err}");

        // A change mode has nothing to act on when only the node holds the token.
        let err = task(
            serde_json::json!([{ "name": "api", "audience": ["a"], "changeMode": "restart" }]),
            serde_json::json!({ "identity": "api" }),
        )
        .validate_identities()
        .expect_err("change mode without delivery");
        assert!(err.to_string().contains("nothing to act on"), "{err}");
    }

    #[test]
    fn stateful_block_deserializes_and_round_trips() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "exec",
                "count": 3,
                "stateful": {
                    "mode": "slot",
                    "storage": { "path": "/var/lib/db", "retainOnScaleIn": true }
                }
            }
        });
        let config: JobConfig = serde_json::from_value(spec)?;
        let task = &config.tasks[0];
        let stateful = task.stateful.as_ref().expect("stateful block parsed");
        assert_eq!(stateful.mode, InstanceMode::Slot);
        assert!(!stateful.require_id);
        let storage = stateful.storage.as_ref().expect("storage parsed");
        assert_eq!(storage.path, "/var/lib/db");
        assert!(storage.retains_on_scale_in());
        task.validate_stateful()?;
        assert!(task.is_stateful());

        // Round trip: serialize the task config and re-parse.
        let serialized = serde_json::to_value(task)?;
        assert_eq!(serialized["stateful"]["mode"], "slot");
        let reparsed: TaskConfig = serde_json::from_value(serialized)?;
        assert_eq!(reparsed.stateful, task.stateful);
        Ok(())
    }

    #[test]
    fn stateful_keyed_block_accepts_require_id() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "container",
                "stateful": {
                    "mode": "keyed",
                    "requireId": true,
                    "storage": { "path": "/workspace" }
                }
            }
        });
        let config: JobConfig = serde_json::from_value(spec)?;
        let stateful = config.tasks[0].stateful.as_ref().expect("stateful parsed");
        assert_eq!(stateful.mode, InstanceMode::Keyed);
        assert!(stateful.require_id);
        config.tasks[0].validate_stateful()?;
        // The old `idSource` name fails to parse instead of being ignored.
        let old = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "container",
                "stateful": { "mode": "keyed", "idSource": "caller" }
            }
        });
        assert!(serde_json::from_value::<JobConfig>(old).is_err());
        Ok(())
    }

    #[test]
    fn pre_rename_kind_field_is_rejected_with_a_runtime_hint() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": { "kind": "container" }
        });
        let err = serde_json::from_value::<JobConfig>(spec)
            .expect_err("the pre-rename `kind` spelling must fail to parse");
        let message = err.to_string();
        // The whole point of rejecting rather than aliasing is that the error
        // has to teach the substitution (calls doc §1).
        assert!(
            message.contains("unknown field `kind`")
                && message.contains("expected `runtime`")
                && message.contains("runtime: \"container\""),
            "error must name the exact fix, got: {message}"
        );
    }

    #[test]
    fn declaring_both_runtime_and_kind_is_rejected() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": { "runtime": "exec", "kind": "exec" }
        });
        let err = serde_json::from_value::<JobConfig>(spec)
            .expect_err("declaring both spellings must fail to parse");
        assert!(
            err.to_string().contains("remove the kind field"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn missing_runtime_names_the_field_and_its_purpose() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": { "group": "web" }
        });
        let err = serde_json::from_value::<JobConfig>(spec)
            .expect_err("a task with no runtime must fail to parse");
        let message = err.to_string();
        assert!(
            message.contains("missing the required field `runtime`")
                && message.contains("runtime: \"exec\""),
            "unexpected error message: {message}"
        );
    }

    #[test]
    fn stateful_keyed_mode_rejects_explicit_count_at_parse_time() {
        for count in [
            serde_json::json!(3),
            serde_json::json!({ "num": 2, "per": "arch" }),
        ] {
            let spec = serde_json::json!({
                "name": "demo",
                "task": {
                    "runtime": "exec",
                    "count": count,
                    "stateful": { "mode": "keyed" }
                }
            });
            let err = serde_json::from_value::<JobConfig>(spec)
                .expect_err("keyed task with explicit count must fail to parse");
            let message = err.to_string();
            assert!(
                message.contains("cannot define count")
                    && message.contains("instance management API"),
                "unexpected error message: {message}"
            );
        }
    }

    #[test]
    fn stateful_keyed_mode_rejects_explicit_count_in_task_list() {
        let spec = serde_json::json!({
            "name": "demo",
            "tasks": {
                "sandbox": {
                    "runtime": "exec",
                    "count": 1,
                    "stateful": { "mode": "keyed" }
                }
            }
        });
        let err = serde_json::from_value::<JobConfig>(spec)
            .expect_err("keyed task with explicit count must fail to parse");
        assert!(err.to_string().contains("task sandbox"));
    }

    #[test]
    fn stateful_keyed_mode_parses_without_count_and_slot_mode_keeps_count() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "tasks": [
                { "runtime": "exec", "stateful": { "mode": "keyed" } },
                { "runtime": "exec", "count": 3, "stateful": { "mode": "slot" } },
                { "runtime": "exec", "group": "plain" }
            ]
        });
        let config: JobConfig = serde_json::from_value(spec)?;
        assert_eq!(config.tasks[0].count, TaskCount::new(1));
        assert_eq!(config.tasks[1].count, TaskCount::new(3));
        assert_eq!(config.tasks[2].count, TaskCount::new(1));
        Ok(())
    }

    #[test]
    fn stateful_mode_defaults_to_slot() -> R<()> {
        // The minimal stateful task: no `mode`, just storage. Slot is the
        // StatefulSet-shaped common case; keyed changes who owns cardinality
        // and must be asked for explicitly.
        let config: JobConfig = serde_json::from_value(serde_json::json!({
            "name": "job",
            "tasks": [{
                "name": "db", "runtime": "exec", "count": 2,
                "stateful": { "storage": { "path": "/data" } }
            }]
        }))?;
        let stateful = config.tasks[0].stateful.as_ref().expect("stateful parsed");
        assert_eq!(stateful.mode, InstanceMode::Slot);
        config.tasks[0].validate_stateful()?;
        Ok(())
    }

    #[test]
    fn stateful_storage_retain_defaults_to_true() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "exec",
                "stateful": { "mode": "slot", "storage": { "path": "/data" } }
            }
        });
        let config: JobConfig = serde_json::from_value(spec)?;
        let storage = config.tasks[0]
            .stateful
            .as_ref()
            .and_then(|stateful| stateful.storage.as_ref())
            .expect("storage parsed");
        assert!(storage.retain_on_scale_in.is_none());
        assert!(storage.retains_on_scale_in());
        // Both retention knobs default to Retain, like K8s
        // persistentVolumeClaimRetentionPolicy.
        assert!(storage.retain_on_stop.is_none());
        assert!(storage.retains_on_stop());
        Ok(())
    }

    #[test]
    fn stateful_storage_retain_on_stop_parses_in_slot_mode() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "exec",
                "count": 2,
                "stateful": {
                    "mode": "slot",
                    "storage": {
                        "path": "/data",
                        "retainOnScaleIn": true,
                        "retainOnStop": false
                    }
                }
            }
        });
        let config: JobConfig = serde_json::from_value(spec)?;
        let task = &config.tasks[0];
        let storage = task
            .stateful
            .as_ref()
            .and_then(|stateful| stateful.storage.as_ref())
            .expect("storage parsed");
        assert_eq!(storage.retain_on_stop, Some(false));
        assert!(!storage.retains_on_stop());
        assert!(storage.retains_on_scale_in(), "the knobs are independent");
        task.validate_stateful()?;

        // Round trip: the camelCase key survives serialization.
        let serialized = serde_json::to_value(task)?;
        assert_eq!(serialized["stateful"]["storage"]["retainOnStop"], false);
        let reparsed: TaskConfig = serde_json::from_value(serialized)?;
        assert_eq!(reparsed.stateful, task.stateful);
        Ok(())
    }

    #[test]
    fn stateful_validation_rejects_retain_on_stop_in_keyed_mode() {
        for value in [true, false] {
            let task = TaskConfig {
                stateful: Some(StatefulConfig {
                    mode: InstanceMode::Keyed,
                    require_id: false,
                    initial_instances: None,
                    on_lost: None,
                    storage: Some(StatefulStorageConfig {
                        driver: None,
                        mode: None,
                        params: None,
                        path: "/workspace".into(),
                        retain_on_scale_in: None,
                        retain_on_stop: Some(value),
                        unknown_fields: Default::default(),
                    }),
                    envelope: None,
                    promote: None,
                    placement: None,
                    fence_after: None,
                    unknown_fields: Default::default(),
                }),
                ..TaskConfig::new("sandbox", "container")
            };
            let err = task
                .validate_stateful()
                .expect_err("retainOnStop must be rejected in keyed mode");
            assert!(
                err.to_string().contains("retainOnStop")
                    && err.to_string().contains("slot mode")
                    && err.to_string().contains("fixed per instance"),
                "{err}"
            );
        }
    }

    #[test]
    fn stateful_validation_rejects_require_id_in_slot_mode() {
        let task = TaskConfig {
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                require_id: true,
                initial_instances: None,
                on_lost: None,
                storage: None,
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("db", "exec")
        };
        let err = task
            .validate_stateful()
            .expect_err("requireId must be rejected in slot mode");
        assert!(err.to_string().contains("requireId"), "{err}");
    }

    #[test]
    fn stateful_validation_rejects_zero_count_in_slot_mode() {
        let task = TaskConfig {
            count: TaskCount::new(0),
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("db", "exec")
        };
        let err = task
            .validate_stateful()
            .expect_err("slot mode requires count >= 1");
        assert!(err.to_string().contains("count.num >= 1"), "{err}");
    }

    #[test]
    fn stateful_validation_rejects_count_per_in_slot_mode() {
        let task = TaskConfig {
            count: TaskCount::per(2, "region"),
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("db", "exec")
        };
        let err = task
            .validate_stateful()
            .expect_err("slot mode cannot combine with count.per");
        assert!(err.to_string().contains("count.per"), "{err}");
    }

    #[test]
    fn stateful_validation_rejects_bad_storage_paths() {
        for bad_path in ["", "   ", "relative/path"] {
            let task = TaskConfig {
                stateful: Some(StatefulConfig {
                    mode: InstanceMode::Slot,
                    require_id: false,
                    initial_instances: None,
                    on_lost: None,
                    storage: Some(StatefulStorageConfig {
                        driver: None,
                        mode: None,
                        params: None,
                        path: bad_path.into(),
                        retain_on_scale_in: None,
                        retain_on_stop: None,
                        unknown_fields: Default::default(),
                    }),
                    envelope: None,
                    promote: None,
                    placement: None,
                    fence_after: None,
                    unknown_fields: Default::default(),
                }),
                ..TaskConfig::new("db", "exec")
            };
            let err = task
                .validate_stateful()
                .expect_err("storage path must be rejected");
            assert!(
                err.to_string().contains("path"),
                "{err} (path {bad_path:?})"
            );
        }
    }

    #[test]
    fn stateful_validation_rejects_count_per_in_keyed_mode() {
        let task = TaskConfig {
            count: TaskCount::per(2, "region"),
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Keyed,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("sandbox", "container")
        };
        let err = task
            .validate_stateful()
            .expect_err("keyed mode cannot combine with count.per");
        assert!(err.to_string().contains("count.per"), "{err}");
    }

    #[test]
    fn stateful_keyed_mode_does_not_require_count() {
        // count.num is ignored for keyed tasks (the management API drives
        // cardinality), so any count.num — including 0 — validates.
        for count in [0, 1, 7] {
            let task = TaskConfig {
                count: TaskCount::new(count),
                stateful: Some(StatefulConfig {
                    mode: InstanceMode::Keyed,
                    require_id: true,
                    initial_instances: None,
                    on_lost: None,
                    storage: None,
                    envelope: None,
                    promote: None,
                    placement: None,
                    fence_after: None,
                    unknown_fields: Default::default(),
                }),
                ..TaskConfig::new("sandbox", "container")
            };
            task.validate_stateful()
                .expect("keyed tasks must not require count.num");
        }
    }

    #[test]
    fn stateless_task_passes_stateful_validation_and_is_not_stateful() -> R<()> {
        let task = TaskConfig::new("web", "exec");
        assert!(!task.is_stateful());
        task.validate_stateful()?;
        Ok(())
    }

    #[test]
    fn stateful_validation_rejects_unknown_mode_at_parse_time() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "exec",
                "stateful": { "mode": "ordinal" }
            }
        });
        let err = serde_json::from_value::<JobConfig>(spec)
            .expect_err("unknown stateful mode must fail to parse");
        assert!(err.to_string().contains("ordinal") || err.to_string().contains("variant"));
    }

    #[test]
    fn kv_injection_validation_accepts_single_template_source() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "exec",
                "env": [{
                    "name": "A",
                    "value": { "template": "{{ kv \"x/y\" }}" }
                }]
            }
        });
        let config: JobConfig = serde_json::from_value(spec)?;
        config.tasks[0].validate_kv_injections()?;
        Ok(())
    }

    #[test]
    fn connect_block_parses_from_the_jobspec_and_validates() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "exec",
                "connect": {
                    "upstreams": [
                        { "to": "db" },
                        { "to": "cache", "port": 6379 },
                        { "to": "web", "balance": "least_conn" },
                    ]
                },
                "service": { "port": 8080, "allow": ["gateway"] }
            }
        });
        let config: JobConfig = serde_json::from_value(spec)?;
        let task = &config.tasks[0];
        let connect = task.connect.as_ref().expect("connect not parsed");
        assert_eq!(connect.upstreams.len(), 3);
        assert_eq!(connect.upstreams[1].port, Some(6379));
        assert_eq!(
            task.service
                .as_ref()
                .map(|service| service.allow.as_slice()),
            Some(["gateway".to_string()].as_slice())
        );
        task.validate_connect()?;
        Ok(())
    }

    #[test]
    fn connect_validation_sees_the_service_port_shorthand_as_a_static_port() {
        // `service.port: 6379` desugars to a static network port, so pinning
        // an upstream to the same loopback port must be rejected.
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "exec",
                "connect": { "upstreams": [{ "to": "cache", "port": 6379 }] },
                "service": { "port": 6379 }
            }
        });
        let config: JobConfig = serde_json::from_value(spec).expect("job config");
        let err = config.tasks[0]
            .validate_connect()
            .expect_err("upstream localPort colliding with the task port must be rejected");
        assert!(
            err.to_string()
                .contains("collides with a static port declared in network.ports"),
            "{err}"
        );
    }

    #[test]
    fn tasks_without_connect_stay_unchanged() -> R<()> {
        let spec = serde_json::json!({ "name": "demo", "task": { "runtime": "exec" } });
        let config: JobConfig = serde_json::from_value(spec)?;
        assert!(config.tasks[0].connect.is_none());
        config.tasks[0].validate_connect()?;
        // No connect block must not appear in the serialized task config.
        let encoded = serde_json::to_value(&config.tasks[0])?;
        assert!(encoded.get("connect").is_none());
        Ok(())
    }

    fn slot_task_with_on_lost(on_lost: StatefulOnLostConfig) -> TaskConfig {
        TaskConfig {
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                require_id: false,
                initial_instances: None,
                on_lost: Some(on_lost),
                storage: None,
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("db", "exec")
        }
    }

    #[test]
    fn on_lost_defaults_apply_to_every_stateful_task() -> R<()> {
        // No onLost block: the safe defaults still apply (90s self-fence,
        // 2x that for the manager gate) — fencing is on by default.
        let task = TaskConfig {
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("db", "exec")
        };
        let policy = task.stateful.as_ref().unwrap().resolved_on_lost()?;
        assert_eq!(policy.self_terminate_after, Duration::from_secs(90));
        assert_eq!(
            policy.replace_after,
            ReplaceAfter::After(Duration::from_secs(180))
        );
        task.validate_stateful()?;
        Ok(())
    }

    #[test]
    fn on_lost_replace_after_defaults_to_twice_self_terminate_after() -> R<()> {
        let policy = StatefulOnLostConfig {
            self_terminate_after: Some("45s".into()),
            replace_after: None,
            unknown_fields: Default::default(),
        }
        .resolve()?;
        assert_eq!(policy.self_terminate_after, Duration::from_secs(45));
        assert_eq!(
            policy.replace_after,
            ReplaceAfter::After(Duration::from_secs(90))
        );
        Ok(())
    }

    #[test]
    fn on_lost_parses_explicit_durations_and_never() -> R<()> {
        let task = slot_task_with_on_lost(StatefulOnLostConfig {
            self_terminate_after: Some("30s".into()),
            replace_after: Some("3m".into()),
            unknown_fields: Default::default(),
        });
        task.validate_stateful()?;
        let policy = task.stateful.as_ref().unwrap().resolved_on_lost()?;
        assert_eq!(policy.self_terminate_after, Duration::from_secs(30));
        assert_eq!(
            policy.replace_after,
            ReplaceAfter::After(Duration::from_secs(180))
        );

        // The special value "never" is accepted: K8s StatefulSet-style
        // manual recovery (node return or operator override).
        let never = slot_task_with_on_lost(StatefulOnLostConfig {
            self_terminate_after: None,
            replace_after: Some("never".into()),
            unknown_fields: Default::default(),
        });
        never.validate_stateful()?;
        let policy = never.stateful.as_ref().unwrap().resolved_on_lost()?;
        assert_eq!(policy.replace_after, ReplaceAfter::Never);
        Ok(())
    }

    #[test]
    fn on_lost_rejects_replace_after_not_exceeding_self_terminate_after() {
        for replace_after in ["90s", "60s"] {
            let task = slot_task_with_on_lost(StatefulOnLostConfig {
                self_terminate_after: Some("90s".into()),
                replace_after: Some(replace_after.into()),
                unknown_fields: Default::default(),
            });
            let err = task
                .validate_stateful()
                .expect_err("replaceAfter <= selfTerminateAfter must be rejected");
            let message = err.to_string();
            assert!(
                message.contains("must be greater than")
                    && message.contains("self-fenced")
                    && message.contains("task db"),
                "unexpected error: {message}"
            );
        }
    }

    #[test]
    fn on_lost_rejects_bad_duration_strings() {
        // selfTerminateAfter: not a duration / missing unit / zero.
        for bad in ["soon", "90", "0s"] {
            let task = slot_task_with_on_lost(StatefulOnLostConfig {
                self_terminate_after: Some(bad.into()),
                replace_after: None,
                unknown_fields: Default::default(),
            });
            let err = task
                .validate_stateful()
                .expect_err("bad selfTerminateAfter must be rejected");
            assert!(err.to_string().contains("selfTerminateAfter"), "{err}");
        }

        // replaceAfter: anything unparsable that is not the literal "never".
        for bad in ["later", "180", "Never"] {
            let task = slot_task_with_on_lost(StatefulOnLostConfig {
                self_terminate_after: None,
                replace_after: Some(bad.into()),
                unknown_fields: Default::default(),
            });
            let err = task
                .validate_stateful()
                .expect_err("bad replaceAfter must be rejected");
            assert!(err.to_string().contains("replaceAfter"), "{err}");
        }
    }

    #[test]
    fn on_lost_at_task_level_is_rejected_at_parse_time() {
        // Stateless task: there is nothing to fence.
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "exec",
                "onLost": { "selfTerminateAfter": "90s" }
            }
        });
        let err = serde_json::from_value::<JobConfig>(spec)
            .expect_err("onLost on a stateless task must be rejected");
        let message = err.to_string();
        assert!(
            message.contains("not stateful") && message.contains("onLost"),
            "unexpected error: {message}"
        );

        // Stateful task with onLost misplaced at the task level: point the
        // user at the right nesting instead of silently ignoring it.
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "exec",
                "count": 2,
                "stateful": { "mode": "slot" },
                "onLost": { "selfTerminateAfter": "90s" }
            }
        });
        let err = serde_json::from_value::<JobConfig>(spec)
            .expect_err("task-level onLost must be rejected");
        assert!(err.to_string().contains("stateful.onLost"), "{err}");
    }

    #[test]
    fn on_lost_deserializes_inside_stateful_and_round_trips() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "runtime": "exec",
                "count": 2,
                "stateful": {
                    "mode": "slot",
                    "onLost": {
                        "selfTerminateAfter": "30s",
                        "replaceAfter": "2m"
                    }
                }
            }
        });
        let config: JobConfig = serde_json::from_value(spec)?;
        let task = &config.tasks[0];
        task.validate_stateful()?;
        let on_lost = task
            .stateful
            .as_ref()
            .and_then(|stateful| stateful.on_lost.as_ref())
            .expect("onLost parsed");
        assert_eq!(on_lost.self_terminate_after.as_deref(), Some("30s"));
        assert_eq!(on_lost.replace_after.as_deref(), Some("2m"));

        let serialized = serde_json::to_value(task)?;
        assert_eq!(
            serialized["stateful"]["onLost"]["selfTerminateAfter"],
            "30s"
        );
        let reparsed: TaskConfig = serde_json::from_value(serialized)?;
        assert_eq!(reparsed.stateful, task.stateful);
        Ok(())
    }

    fn gang_task(gang: Value) -> Result<TaskConfig, serde_json::Error> {
        let spec = serde_json::json!({
            "name": "trainer",
            "task": { "runtime": "noop", "count": 8, "gang": gang }
        });
        serde_json::from_value::<JobConfig>(spec).map(|config| config.tasks[0].clone())
    }

    #[test]
    fn gang_parses_as_a_flag_or_a_quorum_and_round_trips() -> R<()> {
        let all = gang_task(serde_json::json!(true))?;
        assert_eq!(
            all.gang,
            Some(GangConfig {
                min: None,
                ..Default::default()
            })
        );
        assert_eq!(all.gang.map(|gang| gang.quorum(8)), Some(8));
        assert_eq!(serde_json::to_value(&all)?["gang"], serde_json::json!(true));

        let quorum = gang_task(serde_json::json!({ "min": 6 }))?;
        assert_eq!(
            quorum.gang,
            Some(GangConfig {
                min: Some(6),
                ..Default::default()
            })
        );
        assert_eq!(
            serde_json::to_value(&quorum)?["gang"],
            serde_json::json!({ "min": 6 })
        );
        // The stored form reads back the same.
        let reparsed: TaskConfig = serde_json::from_value(serde_json::to_value(&quorum)?)?;
        assert_eq!(reparsed.gang, quorum.gang);

        // `false` is no gang, and leaves nothing behind.
        let none = gang_task(serde_json::json!(false))?;
        assert_eq!(none.gang, None);
        assert!(serde_json::to_value(&none)?.get("gang").is_none());
        Ok(())
    }

    #[test]
    fn gang_placement_timeout_parses_and_round_trips() -> R<()> {
        let task = gang_task(serde_json::json!({ "min": 6, "placementTimeout": "30m" }))?;
        let gang = task.gang.expect("gang");
        assert_eq!(gang.placement_timeout, Some(Duration::from_secs(1800)));
        let stored = serde_json::to_value(&task)?["gang"].clone();
        assert_eq!(
            stored,
            serde_json::json!({ "min": 6, "placementTimeout": "30m" })
        );
        let reparsed: TaskConfig = serde_json::from_value(serde_json::to_value(&task)?)?;
        assert_eq!(reparsed.gang, task.gang);
        // Seconds work too, and a sub-second timeout is rejected at submit.
        let seconds = gang_task(serde_json::json!({ "placementTimeout": 90 }))?;
        assert_eq!(
            seconds.gang.and_then(|gang| gang.placement_timeout),
            Some(Duration::from_secs(90))
        );
        let zero = gang_task(serde_json::json!({ "placementTimeout": 0 }))?;
        let error = zero.validate_gang().expect_err("a zero timeout");
        assert!(error.to_string().contains("at least 1s"), "{error}");
        Ok(())
    }

    #[test]
    fn gang_on_member_loss_parses_and_round_trips() -> R<()> {
        let default = gang_task(serde_json::json!({ "min": 2 }))?;
        assert_eq!(
            default.gang.as_ref().map(|gang| gang.on_member_loss),
            Some(MemberLossPolicy::Restart)
        );
        // The default is not written back.
        assert_eq!(
            serde_json::to_value(&default)?["gang"],
            serde_json::json!({ "min": 2 })
        );
        for (policy, name) in [
            (MemberLossPolicy::Replace, "replace"),
            (MemberLossPolicy::Fail, "fail"),
        ] {
            let task = gang_task(serde_json::json!({ "onMemberLoss": name }))?;
            assert_eq!(
                task.gang.as_ref().map(|gang| gang.on_member_loss),
                Some(policy)
            );
            let stored = serde_json::to_value(&task)?["gang"].clone();
            assert_eq!(stored, serde_json::json!({ "onMemberLoss": name }));
            let reparsed: TaskConfig = serde_json::from_value(serde_json::to_value(&task)?)?;
            assert_eq!(reparsed.gang, task.gang);
        }
        let error = gang_task(serde_json::json!({ "onMemberLoss": "ignore" }))
            .expect_err("an unknown policy is rejected");
        assert!(
            error
                .to_string()
                .contains("gang.onMemberLoss must be one of"),
            "{error}"
        );
        Ok(())
    }

    #[test]
    fn gang_rejects_fields_it_does_not_have() {
        let error = gang_task(serde_json::json!({ "minimum": 3 }))
            .expect_err("an unknown field is rejected, not ignored");
        assert!(
            error
                .to_string()
                .contains("unknown field `minimum` in gang"),
            "{error}"
        );
        let error = gang_task(serde_json::json!(8)).expect_err("a number is not a gang");
        assert!(
            error.to_string().contains("gang must be true, false"),
            "{error}"
        );
        let error = gang_task(serde_json::json!({ "min": -1 })).expect_err("min is a whole number");
        assert!(error.to_string().contains("gang.min"), "{error}");
    }

    #[test]
    fn the_quorum_follows_a_scaled_count_down() {
        let gang = GangConfig {
            min: Some(6),
            ..Default::default()
        };
        assert_eq!(gang.quorum(8), 6);
        assert_eq!(gang.quorum(4), 4, "a scale-in below min lowers the quorum");
        assert_eq!(GangConfig::default().quorum(10), 10);
    }

    #[test]
    fn gang_validation_rejects_what_has_no_fixed_membership() -> R<()> {
        let mut task = TaskConfig {
            count: TaskCount::new(8),
            gang: Some(GangConfig {
                min: Some(9),
                ..Default::default()
            }),
            ..TaskConfig::new("trainer", "noop")
        };
        let error = task.validate_gang().expect_err("min above count");
        assert!(
            error
                .to_string()
                .contains("must be between 1 and the task's count (8)"),
            "{error}"
        );
        task.gang = Some(GangConfig {
            min: Some(0),
            ..Default::default()
        });
        assert!(task.validate_gang().is_err(), "min of zero");
        task.gang = Some(GangConfig {
            min: Some(8),
            ..Default::default()
        });
        task.validate_gang()?;

        let per = TaskConfig {
            count: TaskCount::per(2, "zone"),
            gang: Some(GangConfig::default()),
            ..TaskConfig::new("trainer", "noop")
        };
        let error = per.validate_gang().expect_err("count.per");
        assert!(error.to_string().contains("count.per"), "{error}");

        let keyed = TaskConfig {
            gang: Some(GangConfig::default()),
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Keyed,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("sandbox", "noop")
        };
        let error = keyed.validate_gang().expect_err("keyed");
        assert!(error.to_string().contains("keyed"), "{error}");

        let slots = TaskConfig {
            count: TaskCount::new(3),
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                ..keyed.stateful.clone().expect("stateful")
            }),
            ..keyed.clone()
        };
        slots.validate_gang()?;
        Ok(())
    }
}

#[cfg(test)]
mod unknown_fields_test {
    //! Strict jobspec fields (docs/jobs-tasks-and-scheduling.md, "Unknown
    //! fields"): a typo fails with a suggestion at every level, and
    //! `allow_unknown_fields` keeps extension-point keys and drops the rest.

    use super::*;
    use crate::structs::network::{
        ConnectConfig, ConnectTemplate, NetworkConfig, PortConfig, PortTls, ServiceConfig,
        UpstreamConfig, UpstreamHttpConfig,
    };
    use crate::structs::spec_fields::{ExtensionPoint, SpecBlock, UnknownFieldEntry};
    use serde_json::json;

    fn strict_error(spec: Value) -> String {
        match JobConfig::parse_submitted(spec, UnknownFieldMode::Reject) {
            Ok(_) => panic!("a strict parse should have failed"),
            Err(err) => format!("{err:#}"),
        }
    }

    fn allowed(spec: Value) -> (JobConfig, UnknownFieldsRecord) {
        JobConfig::parse_submitted(spec, UnknownFieldMode::Allow).expect("allowed parse")
    }

    fn entry(scope: &str, path: &str, point: Option<ExtensionPoint>) -> UnknownFieldEntry {
        UnknownFieldEntry {
            scope: scope.into(),
            path: path.into(),
            point,
        }
    }

    #[test]
    fn a_job_level_typo_fails_with_a_suggestion_test() {
        let err = strict_error(json!({
            "name": "web",
            "prioritty": "high",
            "task": { "runtime": "noop" }
        }));
        assert!(
            err.contains("web: unknown field `prioritty` (did you mean `priority`?)"),
            "{err}"
        );
        assert!(err.contains("\nHint: "), "{err}");
        assert!(err.contains("--allow-unknown-fields"), "{err}");
    }

    /// The plan's acceptance case is `autoscael` suggesting `autoscale`.
    /// `autoscale` is not a field yet (it lands with the autoscaler), so the
    /// same transposition is exercised on `count`.
    #[test]
    fn a_task_level_typo_fails_with_a_suggestion_test() {
        let err = strict_error(json!({
            "name": "web",
            "tasks": { "web": { "runtime": "noop", "cuont": 3 } }
        }));
        assert!(
            err.contains("web: task web: unknown field `cuont` (did you mean `count`?)"),
            "{err}"
        );
    }

    #[test]
    fn nested_typos_name_their_path_and_suggest_test() {
        let err = strict_error(json!({
            "name": "web",
            "rollout": { "stepStager": "5s" },
            "task": {
                "name": "web",
                "runtime": "noop",
                "stop": { "signall": "SIGINT" },
                "stateful": { "storage": { "path": "/data", "retainOnStp": false } },
                "volumes": [{ "name": "data", "readonly": true }],
                "network": {
                    "ports": [{
                        "name": "http",
                        "tls": { "cert": { "kv": "certs/web" }, "client": {} }
                    }]
                },
                "checks": [{ "type": "health", "kind": "tcp", "port": 80, "evry": "5s" }],
                "connect": {
                    "upstreams": [{ "to": "http://search", "http": { "timout": "3s" } }]
                },
                "identities": [{ "name": "aws", "audience": ["sts"], "file": { "path": "/t", "mod": "0400" } }],
                "dependsOn": [{ "task": "db", "conditon": "healthy" }],
                "artifacts": [{ "source": "https://example.com/tool.tar.gz", "dset": "bin" }]
            }
        }));
        for line in [
            "web: unknown field `rollout.stepStager` (did you mean `rollout.stepStagger`?)",
            "web: task web: unknown field `stop.signall` (did you mean `stop.signal`?)",
            "web: task web: unknown field `stateful.storage.retainOnStp` (did you mean \
             `stateful.storage.retainOnStop`?)",
            "web: task web: volume data: unknown field `readonly` (did you mean `readOnly`?)",
            "web: task web: port http: unknown field `tls.client` (did you mean `tls.clients`?)",
            "web: task web: unknown field `checks[0].evry` (did you mean `checks[0].every`?)",
            "web: task web: unknown field `connect.upstreams[0].http.timout` (did you mean \
             `connect.upstreams[0].http.timeout`?)",
            "web: task web: unknown field `identities[0].file.mod` (did you mean \
             `identities[0].file.mode`?)",
            "web: task web: unknown field `dependsOn[0].conditon` (did you mean \
             `dependsOn[0].condition`?)",
            "web: task web: unknown field `artifacts[0].dset` (did you mean `artifacts[0].dest`?)",
        ] {
            assert!(err.contains(line), "missing {line:?} in:\n{err}");
        }
    }

    #[test]
    fn a_volume_still_rejects_an_unknown_key_after_capture_test() {
        // `VolumeSpec` used `deny_unknown_fields`; it now captures, and a
        // strict submit still refuses the key, with the volume named.
        let err = strict_error(json!({
            "name": "orders-db",
            "task": {
                "name": "postgres",
                "runtime": "noop",
                "volumes": [{ "name": "data", "backup": { "schedule": "0 3 * * *" } }]
            }
        }));
        assert!(
            err.contains("orders-db: task postgres: volume data: unknown field `backup`"),
            "{err}"
        );
    }

    #[test]
    fn a_tasks_own_error_reaches_the_user_test() {
        // A list task: the error names it and says what `count` takes, not
        // serde's "did not match any variant of untagged enum".
        let err = strict_error(json!({
            "name": "web",
            "tasks": [{ "name": "a", "runtime": "noop", "count": "three" }]
        }));
        assert!(
            err.contains("tasks[0]: count must be a whole number"),
            "{err}"
        );
        assert!(!err.contains("did not match any variant"), "{err}");

        // A map task.
        let err = strict_error(json!({
            "name": "web",
            "tasks": { "a": { "runtime": "noop", "lifecycle": "forever" } }
        }));
        assert!(err.contains("task a: unknown variant `forever`"), "{err}");
        assert!(!err.contains("did not match any variant"), "{err}");

        // The single-task shorthand.
        let err = strict_error(json!({
            "name": "web",
            "task": { "runtime": "noop", "checks": "often" }
        }));
        assert!(err.starts_with("task: invalid type"), "{err}");

        // A value shorthand parsed by hand stays strict and suggests.
        let err = strict_error(json!({
            "name": "web",
            "task": { "runtime": "noop", "count": { "num": 2, "pre": "zone" } }
        }));
        assert!(
            err.contains("unknown field `count.pre` (did you mean `count.per`?)"),
            "{err}"
        );
        let err = strict_error(json!({
            "name": "web",
            "task": {
                "runtime": "noop",
                "lifecycle": "batch",
                "schedule": { "every": "5m", "jitter": "1m" }
            }
        }));
        assert!(err.contains("unknown field `schedule.jitter`"), "{err}");
    }

    #[test]
    fn the_kind_error_reads_as_before_test() {
        let spec = json!({ "name": "web", "task": { "kind": "exec" } });
        let expected = "task task: unknown field `kind`, expected `runtime`. `kind` was renamed \
                        to `runtime`; replace `kind: \"exec\"` with `runtime: \"exec\"` (see \
                        docs/task-runtime-plugins.md)";
        assert_eq!(strict_error(spec.clone()), expected);
        // The flag is about keys Legion does not know; `kind` it knows.
        let err = JobConfig::parse_submitted(spec.clone(), UnknownFieldMode::Allow)
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.to_string(), expected);
        let err = serde_json::from_value::<JobConfig>(spec).unwrap_err();
        assert_eq!(err.to_string(), expected);
    }

    #[test]
    fn allow_keeps_extension_point_keys_and_drops_the_rest_test() {
        let (config, record) = allowed(json!({
            "name": "orders-db",
            "uptime": { "url": "https://orders.acme.dev/healthz" },
            "rollout": { "stepStager": "5s" },
            "tasks": {
                "postgres": {
                    "runtime": "noop",
                    "autoscael": true,
                    "stop": { "signall": "SIGINT" },
                    "volumes": [{ "name": "data", "backup": { "schedule": "0 3 * * *" } }],
                    "network": { "ports": [{ "name": "http", "dns": { "hostname": "orders" } }] },
                    "service": { "port": "http", "uptime": true }
                }
            }
        }));

        // Kept, as written, on the block they were written on.
        assert_eq!(
            config.pending_extensions,
            BTreeMap::from([(
                "uptime".to_string(),
                json!({ "url": "https://orders.acme.dev/healthz" })
            )])
        );
        let task = &config.tasks[0];
        assert_eq!(task.pending_extensions["autoscael"], json!(true));
        assert_eq!(
            task.volumes[0].pending_extensions["backup"],
            json!({ "schedule": "0 3 * * *" })
        );
        let network = task.network.as_ref().expect("network");
        assert_eq!(
            network.ports[0].pending_extensions["dns"],
            json!({ "hostname": "orders" })
        );
        let service = task.service.as_ref().expect("service");
        assert_eq!(service.pending_extensions["uptime"], json!(true));

        // Dropped, and nothing captured is left behind.
        let stop = task.stop.as_ref().expect("stop");
        assert!(stop.unknown_fields.is_empty());
        assert_eq!(stop.signal, "SIGTERM");
        assert!(config
            .rollout
            .as_ref()
            .is_some_and(|rollout| rollout.unknown_fields.is_empty()));

        assert_eq!(
            record.dropped,
            vec![
                entry("", "rollout.stepStager", None),
                entry("task postgres", "stop.signall", None),
            ]
        );
        // Walk order: the job, then each task and its blocks in field order.
        assert_eq!(
            record.pending,
            vec![
                entry("", "uptime", Some(ExtensionPoint::Job)),
                entry("task postgres", "autoscael", Some(ExtensionPoint::Task)),
                entry(
                    "task postgres: port http",
                    "dns",
                    Some(ExtensionPoint::Port)
                ),
                entry(
                    "task postgres: service",
                    "uptime",
                    Some(ExtensionPoint::Service)
                ),
                entry(
                    "task postgres: volume data",
                    "backup",
                    Some(ExtensionPoint::Volume)
                ),
            ]
        );
        let warnings = record.warnings();
        assert_eq!(
            warnings[0],
            "dropped unknown field `rollout.stepStager`".to_string()
        );
        assert!(
            warnings.contains(
                &"task postgres: kept `autoscael` as pending (no installed app declares it)"
                    .to_string()
            ),
            "{warnings:?}"
        );
    }

    #[test]
    fn a_known_but_invalid_value_fails_even_with_the_flag_test() {
        let err = JobConfig::parse_submitted(
            json!({ "name": "web", "task": { "runtime": "noop", "count": "three", "extra": 1 } }),
            UnknownFieldMode::Allow,
        )
        .map(|_| ())
        .unwrap_err();
        assert!(
            err.to_string().contains("count must be a whole number"),
            "{err}"
        );
    }

    #[test]
    fn pending_maps_round_trip_and_hand_written_ones_are_refused_test() {
        let (config, _) = allowed(json!({
            "name": "web",
            "uptime": true,
            "rolloutByGroup": { "web": { "minHealthyTime": "10s" } },
            "task": {
                "name": "web",
                "group": "web",
                "runtime": "noop",
                "autoscael": true,
                "volumes": [{ "name": "data", "backup": true }]
            }
        }));
        // Legion's own serialization parses back unchanged, pending maps and
        // `rolloutByGroup` included (it used to serialize as
        // `rollout_by_group`, which the parser ignored).
        let stored = serde_json::to_value(&config).expect("serialize");
        assert_eq!(stored["pendingExtensions"], json!({ "uptime": true }));
        assert!(stored.get("rolloutByGroup").is_some(), "{stored}");
        let back: JobConfig = serde_json::from_value(stored.clone()).expect("round trip");
        assert_eq!(back, config);
        assert!(back.is_same_as(&config));

        // A strict submit of that JSON is refused: `pendingExtensions` is
        // Legion's to write.
        let err = strict_error(stored.clone());
        assert!(
            err.contains("web: `pendingExtensions` holds keys Legion kept"),
            "{err}"
        );
        assert!(
            err.contains("web: task web: `pendingExtensions` holds keys"),
            "{err}"
        );
        // With the flag it is accepted, and reported as pending.
        let (resubmitted, record) = allowed(stored);
        assert_eq!(resubmitted, config);
        assert_eq!(record.pending.len(), 3, "{record:?}");
        assert!(record.dropped.is_empty());

        // A pending-only change is a change.
        let (changed, _) = allowed(json!({
            "name": "web",
            "uptime": false,
            "rolloutByGroup": { "web": { "minHealthyTime": "10s" } },
            "task": {
                "name": "web",
                "group": "web",
                "runtime": "noop",
                "autoscael": true,
                "volumes": [{ "name": "data", "backup": true }]
            }
        }));
        assert!(!changed.is_same_as(&config));
    }

    #[test]
    fn plain_deserialization_is_strict_test() {
        let err = serde_json::from_value::<JobConfig>(json!({
            "name": "web",
            "task": { "name": "web", "runtime": "noop", "stop": { "kill_after": "5s" } }
        }))
        .unwrap_err();
        assert!(
            err.to_string().contains(
                "web: task web: unknown field `stop.kill_after` (did you mean `stop.killAfter`?)"
            ),
            "{err}"
        );
    }

    fn serialized_keys<T: Serialize>(value: &T) -> Vec<String> {
        match serde_json::to_value(value).expect("serialize") {
            Value::Object(map) => map.keys().cloned().collect(),
            _ => Vec::new(),
        }
    }

    /// Every key a block serializes is in its suggestion list. Run over every
    /// shipped example (maximal.ncl sets nearly every field), so a field
    /// added to a struct and to maximal.ncl but not to the block's `FIELDS`
    /// fails here instead of silently missing from did-you-mean.
    #[test]
    fn suggestion_lists_cover_every_serialized_field_test() {
        fn cover<T: SpecBlock + Serialize>(block: &T, what: &str, seen: &mut BTreeSet<String>) {
            for key in serialized_keys(block) {
                if key == "pendingExtensions" {
                    continue;
                }
                assert!(
                    T::FIELDS.contains(&key.as_str()),
                    "{what}: `{key}` is serialized but missing from FIELDS"
                );
                seen.insert(format!("{what}.{key}"));
            }
        }
        fn cover_opt<T: SpecBlock + Serialize>(
            block: Option<&T>,
            what: &str,
            seen: &mut BTreeSet<String>,
        ) {
            if let Some(block) = block {
                cover(block, what, seen);
            }
        }

        let jobs_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("configs")
            .join("jobs");
        let mut vars = serde_json::Map::new();
        vars.insert("job_name".into(), Value::from("vars-default"));
        vars.insert("task_runtime".into(), Value::from("exec"));
        vars.insert("task_count".into(), Value::from(1));
        vars.insert("message".into(), Value::from("hello"));
        vars.insert("cpu".into(), Value::from(100));
        let mut seen = BTreeSet::new();
        for entry in std::fs::read_dir(&jobs_dir).expect("jobs dir") {
            let path = entry.expect("entry").path();
            if !crate::config_lang::is_config_file(&path) {
                continue;
            }
            let vars = match crate::config_lang::format_for(&path) {
                crate::config_lang::ConfigFormat::Json => None,
                crate::config_lang::ConfigFormat::Nickel => Some(&vars),
            };
            let rendered = crate::config_lang::eval_file_with_vars(&path, vars)
                .unwrap_or_else(|err| panic!("{} should evaluate: {err:#}", path.display()));
            let config: JobConfig = serde_json::from_str(&rendered)
                .unwrap_or_else(|err| panic!("{} should parse: {err}", path.display()));

            cover_opt(config.rollout.as_ref(), "rollout", &mut seen);
            if let Some(slots) = config.rollout.as_ref().and_then(|r| r.slots.as_ref()) {
                cover(slots, "rollout.slots", &mut seen);
            }
            for rollout in config.rollout_by_group.values() {
                cover(rollout, "rollout", &mut seen);
            }
            for task in &config.tasks {
                for key in serialized_keys(task) {
                    if key == "pendingExtensions" {
                        continue;
                    }
                    assert!(
                        TaskConfigInput::FIELDS.contains(&key.as_str()),
                        "task: `{key}` is serialized but missing from FIELDS"
                    );
                }
                for check in &task.checks {
                    cover(check, "checks", &mut seen);
                }
                cover(&task.on_unhealthy, "onUnhealthy", &mut seen);
                cover_opt(task.restart.as_ref(), "restart", &mut seen);
                cover_opt(task.stop.as_ref(), "stop", &mut seen);
                cover_opt(task.disruption.as_ref(), "disruption", &mut seen);
                if let Some(kv) = task.kv_injections.as_ref() {
                    cover(kv, "kvInjections", &mut seen);
                    cover_opt(kv.kv.as_ref(), "kv", &mut seen);
                    for env in &kv.env {
                        cover(env, "env", &mut seen);
                        cover(&env.value, "env.value", &mut seen);
                        cover(&env.kv, "env.kv", &mut seen);
                    }
                    for file in &kv.files {
                        cover(file, "files", &mut seen);
                        cover(&file.file, "files.file", &mut seen);
                        cover(&file.kv, "files.kv", &mut seen);
                    }
                }
                if let Some(network) = task.network.as_ref() {
                    cover::<NetworkConfig>(network, "network", &mut seen);
                    for port in &network.ports {
                        cover::<PortConfig>(port, "port", &mut seen);
                        if let Some(tls) = port.tls.as_ref() {
                            cover::<PortTls>(tls, "port.tls", &mut seen);
                            cover_opt(tls.clients.as_ref(), "port.tls.clients", &mut seen);
                        }
                    }
                }
                cover_opt::<ServiceConfig>(task.service.as_ref(), "service", &mut seen);
                if let Some(connect) = task.connect.as_ref() {
                    cover::<ConnectConfig>(connect, "connect", &mut seen);
                    for template in &connect.templates {
                        cover::<ConnectTemplate>(template, "connect.templates", &mut seen);
                    }
                    for upstream in &connect.upstreams {
                        cover::<UpstreamConfig>(upstream, "upstream", &mut seen);
                        for credential in &upstream.credentials {
                            cover(credential, "upstream.credentials", &mut seen);
                        }
                        if let Some(http) = upstream.http.as_ref() {
                            cover::<UpstreamHttpConfig>(http, "upstream.http", &mut seen);
                            cover_opt(http.retries.as_ref(), "upstream.http.retries", &mut seen);
                            for split in &http.split {
                                cover(split, "upstream.http.split", &mut seen);
                            }
                        }
                    }
                }
                for identity in &task.identities {
                    cover(identity, "identities", &mut seen);
                    cover_opt(identity.file.as_ref(), "identities.file", &mut seen);
                }
                if let Some(stateful) = task.stateful.as_ref() {
                    cover(stateful, "stateful", &mut seen);
                    cover_opt(stateful.storage.as_ref(), "stateful.storage", &mut seen);
                    cover_opt(stateful.on_lost.as_ref(), "stateful.onLost", &mut seen);
                    cover_opt(stateful.envelope.as_ref(), "stateful.envelope", &mut seen);
                    cover_opt(stateful.promote.as_ref(), "stateful.promote", &mut seen);
                }
                for volume in &task.volumes {
                    cover(volume, "volumes", &mut seen);
                }
                for artifact in &task.artifacts {
                    // `pinnedFrom`/`authOrigin` are written by admission,
                    // never by hand, so they are not suggested.
                    for key in serialized_keys(artifact) {
                        assert!(
                            ArtifactSpec::FIELDS.contains(&key.as_str())
                                || key == "pinnedFrom"
                                || key == "authOrigin",
                            "artifacts: `{key}` is serialized but missing from FIELDS"
                        );
                    }
                    cover_opt(artifact.auth.as_ref(), "artifacts.auth", &mut seen);
                }
                for action in &task.actions {
                    cover(action, "actions", &mut seen);
                    for param in &action.params {
                        cover(param, "actions.params", &mut seen);
                    }
                }
                cover_opt(
                    task.speculative_start.as_ref(),
                    "speculativeStart",
                    &mut seen,
                );
                cover_opt(task.snapshots.as_ref(), "snapshots", &mut seen);
                if let Some(suspend) = task.suspend.as_ref() {
                    cover(suspend, "suspend", &mut seen);
                    cover_opt(suspend.idle.as_ref(), "suspend.idle", &mut seen);
                    cover_opt(suspend.wake.as_ref(), "suspend.wake", &mut seen);
                }
            }
        }
        // The examples really do exercise the nested blocks.
        for key in [
            "stop.signal",
            "stateful.storage.path",
            "volumes.name",
            "port.name",
        ] {
            assert!(seen.contains(key), "no example sets {key}: {seen:?}");
        }
    }
}
