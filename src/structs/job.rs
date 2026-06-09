use chrono::Utc;
use serde::ser::SerializeStruct;
use serde::{de::Error, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::time::Duration;
use tabled::Tabled;

use crate::prelude::{bail, R};

use super::instance::InstanceMode;
use super::workload_identity::{validate_identities, WorkloadIdentity};
use super::{Job, PlannedAllocation, Submission};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct JobPrint {
    pub name: String,
    pub status: String,
    pub created_at: String,
}

impl From<Job> for JobPrint {
    fn from(job: Job) -> Self {
        JobPrint {
            name: job.name,
            status: job.status,
            created_at: job.created_at.to_string(),
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

fn default_task_group() -> String {
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

fn parse_schedule_value(value: &Value) -> Result<TaskSchedule, String> {
    match value {
        Value::String(text) => parse_schedule_text(text),
        Value::Object(map) => {
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

#[derive(Deserialize)]
#[serde(untagged)]
enum TaskCountInput {
    Number(u32),
    Per { num: u32, per: String },
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

impl<'de> Deserialize<'de> for TaskCount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match TaskCountInput::deserialize(deserializer)? {
            TaskCountInput::Number(num) => Ok(TaskCount::new(num)),
            TaskCountInput::Per { num, per } => Ok(TaskCount::per(num, per)),
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
pub enum TaskLifecycle {
    Service,
    Batch,
    Sidecar,
}

impl Default for TaskLifecycle {
    fn default() -> Self {
        TaskLifecycle::Service
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    #[serde(default)]
    pub port: Option<u16>,
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
        if override_config.has_step_mode() {
            self.clear_step_modes();
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
            if count_steps.iter().any(|step| *step == 0) {
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
    /// ids reused within the pool) or `keyed` (created on demand via the
    /// CreateInstance/DestroyInstance RPCs, ids never recycled). In keyed
    /// mode `count.num` is ignored: the set of live instances created via
    /// the API is the desired allocation count.
    pub mode: InstanceMode,
    /// Where keyed instance ids come from by default: `caller` (the
    /// CreateInstance request must supply one) or `uuid` (server-minted when
    /// the request omits one). A valid caller-supplied id is always honored
    /// regardless of this setting. Only valid in keyed mode; slot ids are
    /// always derived from the ordinal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_source: Option<String>,
    /// Optional persistent storage bound to each instance (contract only in
    /// v1; drivers are a follow-up, see
    /// `docs/plans/volume-plugin-system-plan.md`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StatefulStorageConfig>,
}

/// Persistent storage contract for a stateful task. Each instance gets an
/// opaque `storage_handle` minted on first placement and reattached on every
/// subsequent allocation for that instance.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StatefulStorageConfig {
    /// Absolute path inside the task where the instance's storage is exposed.
    pub path: String,
    /// Whether to keep an instance's storage handle when its slot is scaled
    /// in (so a later scale-out reattaches the same data). Defaults to true,
    /// mirroring K8s PVC retention.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_on_scale_in: Option<bool>,
}

impl StatefulStorageConfig {
    /// Effective retention policy (defaults to retain).
    pub fn retains_on_scale_in(&self) -> bool {
        self.retain_on_scale_in.unwrap_or(true)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct TaskConfig {
    pub name: String,
    #[serde(default = "default_task_count")]
    pub count: TaskCount,
    pub kind: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub kv_injections: Option<TaskKvInjections>,
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
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum KvInjectionAction {
    None,
    Restart,
    Rollout,
    Fail,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct KvInjectionPolicy {
    #[serde(default)]
    pub watch: bool,
    #[serde(default)]
    pub on_change: Option<KvInjectionAction>,
    #[serde(default)]
    pub on_delete: Option<KvInjectionAction>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct KvTemplateSource {
    #[serde(default)]
    pub template: Option<String>,
    #[serde(default)]
    pub template_kv: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskKvEnvInjection {
    pub name: String,
    pub value: KvTemplateSource,
    #[serde(default)]
    pub kv: KvInjectionPolicy,
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
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct JobConfig {
    pub name: String,
    pub tasks: Vec<TaskConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollout: Option<RolloutConfig>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub rollout_by_group: BTreeMap<String, RolloutConfig>,
    pub version: Option<u32>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobConfigInput {
    name: String,
    #[serde(default)]
    tasks: Option<TaskConfigList>,
    #[serde(default)]
    task: Option<TaskConfigInput>,
    #[serde(default)]
    rollout: Option<RolloutConfig>,
    #[serde(default)]
    rollout_by_group: BTreeMap<String, RolloutConfig>,
    version: Option<u32>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum TaskConfigList {
    List(Vec<TaskConfigInput>),
    Map(BTreeMap<String, TaskConfigInput>),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskConfigInput {
    #[serde(default)]
    name: Option<String>,
    #[serde(default = "default_task_count")]
    count: TaskCount,
    kind: String,
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
    kv: Option<KvInjectionPolicy>,
    #[serde(default)]
    env: Vec<TaskKvEnvInjection>,
    #[serde(default)]
    files: Vec<TaskKvFileInjection>,
    #[serde(default)]
    kv_injections: Option<TaskKvInjections>,
    #[serde(default)]
    identities: Vec<WorkloadIdentity>,
    #[serde(default)]
    stateful: Option<StatefulConfig>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum TaskDependencyInput {
    Name(String),
    Detailed {
        task: String,
        #[serde(default = "default_dependency_condition")]
        condition: TaskDependencyCondition,
    },
}

impl TaskConfigInput {
    fn into_task_config(self, name: String) -> TaskConfig {
        let depends_on = self
            .depends_on
            .into_iter()
            .map(|entry| match entry {
                TaskDependencyInput::Name(task) => TaskDependency {
                    task,
                    condition: default_dependency_condition(),
                },
                TaskDependencyInput::Detailed { task, condition } => {
                    TaskDependency { task, condition }
                }
            })
            .collect();
        TaskConfig {
            name: self.name.unwrap_or(name),
            count: self.count,
            kind: self.kind,
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
                }
            } else {
                TaskKvInjections {
                    kv: self.kv,
                    env: self.env,
                    files: self.files,
                }
            }),
            identities: self.identities,
            stateful: self.stateful,
        }
    }
}

fn task_list_to_vec(configs: TaskConfigList) -> Vec<TaskConfig> {
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

impl<'de> Deserialize<'de> for JobConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = JobConfigInput::deserialize(deserializer)?;
        let tasks = match (input.tasks, input.task) {
            (Some(_), Some(_)) => {
                return Err(D::Error::custom(
                    "jobspec must define only one of task or tasks",
                ))
            }
            (Some(tasks), None) => task_list_to_vec(tasks),
            (None, Some(task)) => vec![task.into_task_config(String::from("task"))],
            (None, None) => return Err(D::Error::custom("jobspec must define at least one task")),
        };

        Ok(JobConfig {
            name: input.name,
            tasks,
            rollout: input.rollout,
            rollout_by_group: input.rollout_by_group,
            version: input.version,
        })
    }
}

impl JobConfig {
    pub fn is_same_as(&self, other: &JobConfig) -> bool {
        self.name == other.name
            && self.tasks == other.tasks
            && self.rollout == other.rollout
            && self.rollout_by_group == other.rollout_by_group
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
                    .map(|value| value.to_string())
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
    pub fn new(name: impl Into<String>, kind: impl Into<String>) -> Self {
        TaskConfig {
            name: name.into(),
            count: default_task_count(),
            kind: kind.into(),
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
            kv_injections: None,
            identities: Vec::new(),
            stateful: None,
        }
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
                if stateful.id_source.is_some() {
                    bail!(
                        "task {} stateful.idSource is only valid in keyed mode; slot ids are \
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
            }
            InstanceMode::Keyed => {
                if let Some(id_source) = stateful.id_source.as_deref() {
                    if !matches!(id_source, "caller" | "uuid") {
                        bail!(
                            "task {} stateful.idSource must be \"caller\" or \"uuid\", got {:?}",
                            self.name,
                            id_source
                        );
                    }
                }
                // Keyed cardinality is driven entirely by the management API
                // (CreateInstance/DestroyInstance); count.num is ignored and
                // count.per (per-attribute counts) has no meaningful keyed
                // interpretation, so reject it rather than silently ignore.
                if self.count.per.is_some() {
                    bail!(
                        "task {} stateful keyed mode cannot be combined with count.per; keyed \
                         instance cardinality is driven by the instance management API, not count",
                        self.name
                    );
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
        }

        Ok(())
    }

    /// Validate workload identities declared on this task (see the
    /// workload-identity OIDC plan). No identities = today's behavior.
    pub fn validate_identities(&self) -> R<()> {
        validate_identities(&self.name, &self.identities, self.is_stateful())
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
                    if check.path.is_none() {
                        bail!(
                            "{} check {} requires a path",
                            match check.kind {
                                TaskCheckKind::Http => "http",
                                TaskCheckKind::Tcp => "tcp",
                                TaskCheckKind::Exec => "exec",
                            },
                            check.default_name()
                        );
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

        for env in &injections.env {
            if env.name.trim().is_empty() {
                bail!("task {} kv_injections.env name cannot be empty", self.name);
            }
            validate_template_source(
                &format!("task {} kv_injections.env {}", self.name, env.name),
                &env.value,
            )?;
        }

        for file in &injections.files {
            if file.path.trim().is_empty() {
                bail!(
                    "task {} kv_injections.files path cannot be empty",
                    self.name
                );
            }
            validate_template_source(
                &format!("task {} kv_injections.files {}", self.name, file.path),
                &file.file,
            )?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_task_list_jobspec() {
        let spec = serde_json::json!({
            "name": "demo",
            "tasks": [
                {
                    "kind": "noop",
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
                    "kind": "noop",
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
                "kind": "noop",
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
                    "kind": "noop",
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
                "kind": "noop",
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
                "kind": "noop",
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
                "kind": "noop",
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
                "kind": "noop",
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
                    "kind": "noop"
                },
                "deploy": {
                    "kind": "noop",
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
                "web": { "kind": "noop", "group": "frontend" },
                "api": { "kind": "noop", "group": "backend" }
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
            "task": { "kind": "noop" },
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
            "task": { "kind": "noop" },
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
                "kind": "noop",
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
    fn kv_injection_validation_rejects_both_template_sources() {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "kind": "exec",
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
    fn identities_deserialize_from_camel_case_spec() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "kind": "exec",
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
                "kind": "exec",
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
    fn stateful_block_deserializes_and_round_trips() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "kind": "exec",
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
        assert!(stateful.id_source.is_none());
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
    fn stateful_keyed_block_accepts_id_source() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "kind": "container",
                "stateful": {
                    "mode": "keyed",
                    "idSource": "caller",
                    "storage": { "path": "/workspace" }
                }
            }
        });
        let config: JobConfig = serde_json::from_value(spec)?;
        let stateful = config.tasks[0].stateful.as_ref().expect("stateful parsed");
        assert_eq!(stateful.mode, InstanceMode::Keyed);
        assert_eq!(stateful.id_source.as_deref(), Some("caller"));
        config.tasks[0].validate_stateful()?;
        Ok(())
    }

    #[test]
    fn stateful_storage_retain_defaults_to_true() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": {
                "kind": "exec",
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
        Ok(())
    }

    #[test]
    fn stateful_validation_rejects_id_source_in_slot_mode() {
        let task = TaskConfig {
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: Some("uuid".into()),
                storage: None,
            }),
            ..TaskConfig::new("db", "exec")
        };
        let err = task
            .validate_stateful()
            .expect_err("idSource must be rejected in slot mode");
        assert!(err.to_string().contains("idSource"), "{err}");
    }

    #[test]
    fn stateful_validation_rejects_zero_count_in_slot_mode() {
        let task = TaskConfig {
            count: TaskCount::new(0),
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: None,
                storage: None,
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
                id_source: None,
                storage: None,
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
                    id_source: None,
                    storage: Some(StatefulStorageConfig {
                        path: bad_path.into(),
                        retain_on_scale_in: None,
                    }),
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
    fn stateful_validation_rejects_unknown_keyed_id_source() {
        let task = TaskConfig {
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Keyed,
                id_source: Some("dice-roll".into()),
                storage: None,
            }),
            ..TaskConfig::new("sandbox", "container")
        };
        let err = task
            .validate_stateful()
            .expect_err("unknown idSource must be rejected");
        assert!(err.to_string().contains("idSource"), "{err}");
    }

    #[test]
    fn stateful_validation_rejects_count_per_in_keyed_mode() {
        let task = TaskConfig {
            count: TaskCount::per(2, "region"),
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Keyed,
                id_source: None,
                storage: None,
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
                    id_source: Some("uuid".into()),
                    storage: None,
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
                "kind": "exec",
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
                "kind": "exec",
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
}
