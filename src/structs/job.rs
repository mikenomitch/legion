use serde::{de::Error, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
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

fn default_task_count() -> u32 {
    1
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

#[derive(Serialize, Debug, Clone, Default, PartialEq, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct TaskConfig {
    pub name: String,
    #[serde(default = "default_task_count")]
    pub count: u32,
    pub kind: String,
    #[serde(default = "empty_object", alias = "options")]
    pub config: Value,
    #[serde(default = "empty_object")]
    pub resources: Value,
    #[serde(default = "empty_object")]
    pub constraints: Value,
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
    count: u32,
    kind: String,
    #[serde(default = "empty_object", alias = "options")]
    config: Value,
    #[serde(default = "empty_object")]
    resources: Value,
    #[serde(default = "empty_object")]
    constraints: Value,
    #[serde(default = "empty_object")]
    affinities: Value,
    #[serde(default)]
    checks: Vec<TaskCheck>,
    #[serde(default)]
    on_unhealthy: Option<TaskOnUnhealthyConfig>,
}

impl TaskConfigInput {
    fn into_task_config(self, name: String) -> TaskConfig {
        TaskConfig {
            name: self.name.unwrap_or(name),
            count: self.count,
            kind: self.kind,
            config: self.config,
            resources: self.resources,
            constraints: self.constraints,
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
            config: empty_object(),
            resources: empty_object(),
            constraints: empty_object(),
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
        assert_eq!(config.tasks[0].count, 1);
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
}
