use std::collections::HashSet;

use serde_json::{json, Map, Value};

use crate::prelude::*;
use crate::structs::config::ManagerConfigFull;

const DEFAULT_TASK_MEMORY_MIB: f64 = 256.0;
const DEFAULT_TASK_CPU_MHZ: f64 = 100.0;
const DEFAULT_TASK_ALLOCATIONS: f64 = 1.0;

#[derive(Debug, Clone)]
pub struct ResourcePolicy {
    pub task_defaults: Map<String, Value>,
    pub required_custom_resources: HashSet<String>,
}

impl ResourcePolicy {
    pub fn from_manager_config(config: &ManagerConfigFull) -> R<Self> {
        let mut defaults = match config.task_resource_defaults.as_ref() {
            Some(Value::Object(map)) => map.clone(),
            Some(_) => bail!("task_resource_defaults must be an object"),
            None => default_task_resource_defaults(),
        };

        normalize_cpu_aliases(&mut defaults)?;

        if defaults.contains_key("cpu") && defaults.contains_key("cores") {
            bail!("task_resource_defaults cannot set both cpu and cores");
        }

        let required_custom_resources =
            parse_required_custom_resources(config.custom_resources.as_ref())?;

        Ok(ResourcePolicy {
            task_defaults: defaults,
            required_custom_resources,
        })
    }

    pub fn normalize_task_resources(&self, resources: &Value, enforce_required: bool) -> R<Value> {
        let map = match resources {
            Value::Null => Map::new(),
            Value::Object(map) => map.clone(),
            _ => bail!("task resources must be an object"),
        };

        let has_cpu = map.contains_key("cpu") || map.contains_key("cpus");
        let has_cores = map.contains_key("cores");

        if map.contains_key("cpu") && map.contains_key("cpus") {
            bail!("task resources cannot define both cpu and cpus");
        }

        if has_cpu && has_cores {
            bail!("task resources cannot define both cpu and cores");
        }

        let mut normalized = self.task_defaults.clone();
        normalize_cpu_aliases(&mut normalized)?;

        if map.contains_key("cpus") {
            normalized.insert("cpu".to_string(), map["cpus"].clone());
        }

        for (key, value) in map {
            if key == "cpus" {
                continue;
            }
            normalized.insert(key, value);
        }

        if has_cores {
            normalized.remove("cpu");
        } else if has_cpu {
            normalized.remove("cores");
        }

        if enforce_required {
            for required in &self.required_custom_resources {
                if !normalized.contains_key(required) {
                    bail!("task resources missing required custom resource {required}");
                }
            }
        }

        Ok(Value::Object(normalized))
    }
}

fn default_task_resource_defaults() -> Map<String, Value> {
    let mut defaults = Map::new();
    defaults.insert("memory".to_string(), json!(DEFAULT_TASK_MEMORY_MIB));
    defaults.insert("cpu".to_string(), json!(DEFAULT_TASK_CPU_MHZ));
    defaults.insert("allocations".to_string(), json!(DEFAULT_TASK_ALLOCATIONS));
    defaults
}

fn normalize_cpu_aliases(resources: &mut Map<String, Value>) -> R<()> {
    if resources.contains_key("cpu") && resources.contains_key("cpus") {
        bail!("resources cannot include both cpu and cpus");
    }

    if let Some(cpus) = resources.remove("cpus") {
        resources.insert("cpu".to_string(), cpus);
    }

    Ok(())
}

fn parse_required_custom_resources(resources: Option<&Value>) -> R<HashSet<String>> {
    let mut required = HashSet::new();
    let Some(resources) = resources else {
        return Ok(required);
    };

    let map = match resources {
        Value::Null => return Ok(required),
        Value::Object(map) => map,
        _ => bail!("custom_resources must be an object"),
    };

    for (name, value) in map {
        let is_required = match value {
            Value::Bool(required) => *required,
            Value::String(raw) => match raw.as_str() {
                "required" => true,
                "optional" => false,
                _ => bail!("custom_resources {name} must be 'required' or 'optional'"),
            },
            Value::Object(obj) => obj
                .get("required")
                .and_then(|val| val.as_bool())
                .context("custom_resources entries must include boolean required field")?,
            _ => bail!("custom_resources {name} must be a boolean or object"),
        };

        if is_required {
            required.insert(name.clone());
        }
    }

    Ok(required)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_defaults_apply_with_aliases() -> R<()> {
        let config = ManagerConfigFull {
            task_resource_defaults: Some(json!({
                "memory": 512,
                "cpus": 200,
            })),
            ..Default::default()
        };
        let policy = ResourcePolicy::from_manager_config(&config)?;
        let normalized = policy.normalize_task_resources(&json!({}), true)?;
        let normalized = normalized.as_object().context("missing object")?;

        assert_eq!(normalized["memory"], json!(512));
        assert_eq!(normalized["cpu"], json!(200));
        assert!(!normalized.contains_key("cpus"));

        Ok(())
    }

    #[test]
    fn required_custom_resources_are_enforced() -> R<()> {
        let config = ManagerConfigFull {
            custom_resources: Some(json!({
                "gpu": "required",
                "disk": "optional",
            })),
            ..Default::default()
        };
        let policy = ResourcePolicy::from_manager_config(&config)?;
        let err = policy
            .normalize_task_resources(&json!({}), true)
            .expect_err("expected required resource failure");

        assert!(err
            .to_string()
            .contains("missing required custom resource gpu"));

        Ok(())
    }
}
