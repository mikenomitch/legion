use chrono::Utc;
use serde::ser::SerializeStruct;
use serde::{de::Error, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;
use tabled::Tabled;

use crate::prelude::{bail, R};

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
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct JobConfig {
    pub name: String,
    pub tasks: Vec<TaskConfig>,
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
            version: input.version,
        })
    }
}

impl JobConfig {
    pub fn is_same_as(&self, other: &JobConfig) -> bool {
        self.name == other.name && self.tasks == other.tasks
    }

    pub fn id(&self) -> String {
        match self.version {
            Some(version) => format!("{}-{}", self.name, version),
            None => format!("{}-{}", self.name, 0),
        }
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
        }
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
}
