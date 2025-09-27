use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tabled::Tabled;

use super::{Job, Submission};

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

fn empty_object() -> Value {
    Value::Object(Default::default())
}

fn default_task_count() -> u32 {
    1
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct TaskConfig {
    pub name: String,
    #[serde(default = "default_task_count")]
    pub count: u32,
    pub kind: String,
    #[serde(default = "empty_object")]
    pub config: Value,
    #[serde(default = "empty_object")]
    pub resources: Value,
    #[serde(default = "empty_object")]
    pub constraints: Value,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JobConfig {
    pub name: String,
    #[serde(deserialize_with = "deserialize_tasks")]
    pub tasks: Vec<TaskConfig>,
    pub version: Option<u32>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum TaskConfigList {
    List(Vec<TaskConfig>),
    Map(BTreeMap<String, TaskConfigEntry>),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskConfigEntry {
    #[serde(default)]
    name: Option<String>,
    #[serde(default = "default_task_count")]
    count: u32,
    kind: String,
    #[serde(default = "empty_object")]
    config: Value,
    #[serde(default = "empty_object")]
    resources: Value,
    #[serde(default = "empty_object")]
    constraints: Value,
}

fn deserialize_tasks<'de, D>(deserializer: D) -> Result<Vec<TaskConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    let configs = TaskConfigList::deserialize(deserializer)?;

    Ok(match configs {
        TaskConfigList::List(tasks) => tasks,
        TaskConfigList::Map(map) => map
            .into_iter()
            .map(|(name, task)| TaskConfig {
                name: task.name.unwrap_or(name),
                count: task.count,
                kind: task.kind,
                config: task.config,
                resources: task.resources,
                constraints: task.constraints,
            })
            .collect(),
    })
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
                    "name": "first",
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
        assert_eq!(config.tasks[0].name, "first");
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
}
