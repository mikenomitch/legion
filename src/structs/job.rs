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
use tracing::warn;

use super::artifact::{validate_task_artifacts, ArtifactSpec};
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
#[derive(Default)]
pub enum TaskLifecycle {
    #[default]
    Service,
    Batch,
    Sidecar,
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
    fn has_sequential_slots(&self) -> bool {
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
    /// `docs/plans/volume-plugin-system-plan.md`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StatefulStorageConfig>,
    /// Node-loss fencing policy (`onLost`): worker self-fence window plus
    /// the manager's loss-confirmation gate. Absent = the safe defaults
    /// (`selfTerminateAfter: 90s`, `replaceAfter: 2 x selfTerminateAfter`)
    /// — see [`StatefulOnLostConfig`] and docs/stateful-workloads.md
    /// "Node loss and fencing".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_lost: Option<StatefulOnLostConfig>,
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
/// subsequent allocation for that instance.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StatefulStorageConfig {
    /// Absolute path inside the task where the instance's storage is exposed.
    pub path: String,
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
}

impl StatefulStorageConfig {
    /// Effective scale-in retention policy (defaults to retain).
    pub fn retains_on_scale_in(&self) -> bool {
        self.retain_on_scale_in.unwrap_or(true)
    }

    /// Effective job-stop retention policy (defaults to retain).
    pub fn retains_on_stop(&self) -> bool {
        self.retain_on_stop.unwrap_or(true)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub network: Option<crate::structs::network::NetworkConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tabled(skip)]
    pub service: Option<crate::structs::network::ServiceConfig>,
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
    /// Artifacts fetched into the task directory before start (artifact
    /// system plan). Empty = today's behavior; staging is skipped entirely.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[tabled(skip)]
    pub artifacts: Vec<ArtifactSpec>,
    /// Pre-baked, parameterized command templates runnable against a running
    /// allocation of this task (see `docs/plans/exec-and-actions-plan.md`).
    /// Empty = no actions. Guarded by the `action` ACL resource, separate
    /// from raw `exec`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[tabled(skip)]
    pub actions: Vec<ActionSpec>,
}

/// A declared action: a named command template with typed parameters.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
    // Optional so keyed-mode validation can tell an explicit `count` apart
    // from the default; resolved via `unwrap_or_default()` in conversion.
    #[serde(default)]
    count: Option<TaskCount>,
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
    network: Option<crate::structs::network::NetworkConfig>,
    #[serde(default)]
    service: Option<crate::structs::network::ServiceConfig>,
    #[serde(default)]
    identities: Vec<WorkloadIdentity>,
    #[serde(default)]
    stateful: Option<StatefulConfig>,
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
    fn into_task_config(self, name: String) -> Result<TaskConfig, String> {
        let name = self.name.unwrap_or(name);
        // Keyed-mode cardinality is owned by the instance management API
        // (CreateInstance/DestroyInstance), never by the job spec. An explicit
        // `count` on a keyed task would be silently meaningless, so reject it
        // loudly at parse time (only here do we still know it was explicit).
        if let Some(stateful) = self.stateful.as_ref() {
            if stateful.mode == InstanceMode::Keyed && self.count.is_some() {
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
                TaskDependencyInput::Detailed { task, condition } => {
                    TaskDependency { task, condition }
                }
            })
            .collect();
        Ok(TaskConfig {
            name,
            count: self.count.unwrap_or_default(),
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
            network: self.network,
            service: self.service,
            identities: self.identities,
            stateful: self.stateful,
            artifacts: self.artifacts,
            actions: self.actions,
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
            (Some(tasks), None) => task_list_to_vec(tasks).map_err(D::Error::custom)?,
            (None, Some(task)) => vec![task
                .into_task_config(String::from("task"))
                .map_err(D::Error::custom)?],
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
            network: None,
            service: None,
            identities: Vec::new(),
            stateful: None,
            artifacts: Vec::new(),
            actions: Vec::new(),
        }
    }

    /// Apply the `service.port` number/`dynamic` shorthand, materializing the
    /// implied `network.ports` entry so stored configs are always explicit.
    pub fn normalize_network(&mut self) -> R<()> {
        let name = self.name.clone();
        crate::structs::network::normalize_task_network(&name, &mut self.network, &mut self.service)
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
                if stateful.initial_instances.is_some() {
                    bail!(
                        "task {} stateful.initialInstances is only valid in keyed mode; slot ids \
                         are always derived from the ordinal",
                        self.name
                    );
                }
            }
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
        }

        // Node-loss fencing: durations must parse and replaceAfter must
        // exceed selfTerminateAfter (or be "never"). Resolving here also
        // covers the defaulted case, so a stateful task can never reach the
        // scheduler with an unresolvable policy.
        stateful
            .resolved_on_lost()
            .map_err(|err| anyhow::anyhow!("task {}: {err}", self.name))?;

        Ok(())
    }

    /// Validate workload identities declared on this task (see the
    /// workload-identity OIDC plan). No identities = today's behavior.
    pub fn validate_identities(&self) -> R<()> {
        validate_identities(&self.name, &self.identities, self.is_stateful())
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
            if is_shell
                && action
                    .command
                    .iter()
                    .any(|a| a.contains("${param."))
            {
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
    fn rollout_slots_deserialize_and_round_trip() -> R<()> {
        let spec = serde_json::json!({
            "name": "demo",
            "task": { "kind": "noop" },
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
                "task": { "kind": "noop" },
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
            "task": { "kind": "noop" },
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
                "db": { "kind": "noop", "group": "data" },
                "api": { "kind": "noop", "group": "backend" }
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
            "tasks": { "db": { "kind": "noop", "group": "data" } },
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
        };
        // Entry only overrides on_change; watch and on_delete inherit.
        let entry = KvInjectionPolicy {
            watch: None,
            on_change: Some(KvInjectionAction::Rollout),
            on_delete: None,
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
        };
        let entry = KvInjectionPolicy {
            watch: Some(false),
            on_change: None,
            on_delete: None,
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
    fn stateful_keyed_mode_rejects_explicit_count_at_parse_time() {
        for count in [
            serde_json::json!(3),
            serde_json::json!({ "num": 2, "per": "arch" }),
        ] {
            let spec = serde_json::json!({
                "name": "demo",
                "task": {
                    "kind": "exec",
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
                    "kind": "exec",
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
                { "kind": "exec", "stateful": { "mode": "keyed" } },
                { "kind": "exec", "count": 3, "stateful": { "mode": "slot" } },
                { "kind": "exec", "group": "plain" }
            ]
        });
        let config: JobConfig = serde_json::from_value(spec)?;
        assert_eq!(config.tasks[0].count, TaskCount::new(1));
        assert_eq!(config.tasks[1].count, TaskCount::new(3));
        assert_eq!(config.tasks[2].count, TaskCount::new(1));
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
                "kind": "exec",
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
                    id_source: None,
                    initial_instances: None,
                    on_lost: None,
                    storage: Some(StatefulStorageConfig {
                        path: "/workspace".into(),
                        retain_on_scale_in: None,
                        retain_on_stop: Some(value),
                    }),
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
    fn stateful_validation_rejects_id_source_in_slot_mode() {
        let task = TaskConfig {
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: Some("uuid".into()),
                initial_instances: None,
                on_lost: None,
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
                initial_instances: None,
                on_lost: None,
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
                initial_instances: None,
                on_lost: None,
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
                    initial_instances: None,
                    on_lost: None,
                    storage: Some(StatefulStorageConfig {
                        path: bad_path.into(),
                        retain_on_scale_in: None,
                        retain_on_stop: None,
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
                initial_instances: None,
                on_lost: None,
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
                initial_instances: None,
                on_lost: None,
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
                    initial_instances: None,
                    on_lost: None,
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

    fn slot_task_with_on_lost(on_lost: StatefulOnLostConfig) -> TaskConfig {
        TaskConfig {
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: None,
                initial_instances: None,
                on_lost: Some(on_lost),
                storage: None,
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
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: None,
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
                "kind": "exec",
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
                "kind": "exec",
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
                "kind": "exec",
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
}
