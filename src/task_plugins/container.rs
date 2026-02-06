use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::{TaskPlugin, TaskRuntime};
use std::collections::HashMap;
use std::process::Command;

#[derive(Debug, Clone)]
struct ContainerTaskConfig {
    image: String,
    command: Vec<String>,
    env: HashMap<String, String>,
}

impl ContainerTaskConfig {
    fn from_value(config: &Value) -> R<Self> {
        let obj = config
            .as_object()
            .context("container task config must be an object")?;
        let image = obj
            .get("image")
            .and_then(|value| value.as_str())
            .context("container task config requires image")?
            .to_string();
        let command = obj
            .get("command")
            .context("container task config requires command")?
            .as_array()
            .context("container task command must be a list of strings")?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(|text| text.to_string())
                    .context("container task command values must be strings")
            })
            .collect::<R<Vec<_>>>()?;
        if command.is_empty() {
            bail!("container task command must include at least one entry");
        }

        let env = match obj.get("env") {
            None => HashMap::new(),
            Some(value) => value
                .as_object()
                .context("container task env must be an object")?
                .iter()
                .map(|(key, value)| {
                    let value = value
                        .as_str()
                        .context("container task env values must be strings")?;
                    Ok((key.clone(), value.to_string()))
                })
                .collect::<R<HashMap<_, _>>>()?,
        };

        Ok(Self {
            image,
            command,
            env,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContainerRuntimeKind {
    AppleContainer,
    Youki,
}

impl ContainerRuntimeKind {
    fn detect() -> R<Self> {
        if cfg!(target_os = "macos") {
            Ok(Self::AppleContainer)
        } else if cfg!(target_os = "linux") {
            Ok(Self::Youki)
        } else {
            bail!("container tasks are not supported on this OS");
        }
    }

    fn binary(&self) -> &'static str {
        match self {
            Self::AppleContainer => "container",
            Self::Youki => "youki",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ContainerLaunch {
    program: String,
    args: Vec<String>,
}

fn build_launch(
    runtime: ContainerRuntimeKind,
    allocation: &Allocation,
    config: &ContainerTaskConfig,
) -> ContainerLaunch {
    let name = format!("legion-{}", allocation.id);
    let mut args = Vec::new();

    match runtime {
        ContainerRuntimeKind::AppleContainer => {
            args.extend([
                "run".to_string(),
                "--rm".to_string(),
                "--name".to_string(),
                name,
            ]);
            for (key, value) in &config.env {
                args.push("--env".to_string());
                args.push(format!("{key}={value}"));
            }
            args.push(config.image.clone());
            args.push("--".to_string());
            args.extend(config.command.clone());
        }
        ContainerRuntimeKind::Youki => {
            args.extend([
                "run".to_string(),
                "--name".to_string(),
                name,
                "--image".to_string(),
                config.image.clone(),
            ]);
            for (key, value) in &config.env {
                args.push("--env".to_string());
                args.push(format!("{key}={value}"));
            }
            args.push("--".to_string());
            args.extend(config.command.clone());
        }
    }

    ContainerLaunch {
        program: runtime.binary().to_string(),
        args,
    }
}

#[derive(Default)]
pub struct ContainerTaskPlugin;

impl TaskPlugin for ContainerTaskPlugin {
    fn name(&self) -> &'static str {
        "container"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["image", "command"],
            "properties": {
                "image": { "type": "string" },
                "command": {
                    "type": "array",
                    "minItems": 1,
                    "items": { "type": "string" }
                },
                "env": {
                    "type": "object",
                    "additionalProperties": { "type": "string" }
                }
            },
            "additionalProperties": true
        })
    }

    fn readiness(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("container".into(), Value::String("pass".into()));
        map
    }

    fn prepare(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _runtime: &mut TaskRuntime,
    ) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Preparing container task"
        );
        Ok(())
    }

    fn start(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        runtime: &mut TaskRuntime,
    ) -> R<()> {
        let config = ContainerTaskConfig::from_value(&task.config)?;
        let runtime_kind = ContainerRuntimeKind::detect()?;
        let launch = build_launch(runtime_kind, allocation, &config);

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            program = %launch.program,
            "Starting container task"
        );

        let mut cmd = Command::new(&launch.program);
        cmd.args(&launch.args);

        let child = cmd.spawn().with_context(|| {
            format!(
                "Failed to spawn container task {} for allocation {}",
                task.name, allocation.id
            )
        })?;
        runtime.pid = Some(child.id());
        runtime.child = Some(child);
        Ok(())
    }

    fn stop(&self, allocation: &Allocation, task: &TaskConfig, runtime: &mut TaskRuntime) -> R<()> {
        if let Some(mut child) = runtime.child.take() {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                "Stopping container task"
            );
            let _ = child.kill();
            let _ = child.wait();
            runtime.pid = None;
        } else if let Some(pid) = runtime.pid.take() {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                pid,
                "Stopping container task by pid"
            );
            let mut system = sysinfo::System::new();
            system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
            if let Some(process) = system.process(sysinfo::Pid::from_u32(pid)) {
                process.kill();
            }
        }
        Ok(())
    }

    fn destroy(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        runtime: &mut TaskRuntime,
    ) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Destroying container task"
        );
        self.stop(allocation, task, runtime)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn alloc() -> Allocation {
        Allocation {
            id: 7,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
        }
    }

    #[test]
    fn container_config_parses_required_fields() -> R<()> {
        let config = json!({
            "image": "alpine:latest",
            "command": ["echo", "hello"],
            "env": {
                "MODE": "test"
            }
        });

        let parsed = ContainerTaskConfig::from_value(&config)?;
        assert_eq!(parsed.image, "alpine:latest");
        assert_eq!(parsed.command, vec!["echo".to_string(), "hello".to_string()]);
        assert_eq!(parsed.env.get("MODE"), Some(&"test".to_string()));
        Ok(())
    }

    #[test]
    fn container_config_requires_image() {
        let config = json!({"command": ["echo"]});
        let err = ContainerTaskConfig::from_value(&config).expect_err("missing image");
        assert!(err.to_string().contains("requires image"));
    }

    #[test]
    fn container_config_requires_command() {
        let config = json!({"image": "alpine"});
        let err = ContainerTaskConfig::from_value(&config).expect_err("missing command");
        assert!(err.to_string().contains("requires command"));
    }

    #[test]
    fn container_config_rejects_empty_command() {
        let config = json!({"image": "alpine", "command": []});
        let err = ContainerTaskConfig::from_value(&config).expect_err("empty command");
        assert!(err.to_string().contains("must include at least one entry"));
    }

    #[test]
    fn container_config_rejects_non_string_env() {
        let config = json!({
            "image": "alpine",
            "command": ["echo"],
            "env": { "COUNT": 1 }
        });
        let err = ContainerTaskConfig::from_value(&config).expect_err("bad env");
        assert!(err.to_string().contains("env values must be strings"));
    }

    #[test]
    fn container_plugin_validates_schema() -> R<()> {
        let plugin = ContainerTaskPlugin::default();
        let config = json!({
            "image": "alpine",
            "command": ["echo", "ok"]
        });
        plugin.validate_config(&config)?;
        Ok(())
    }

    #[test]
    fn container_builds_launch_for_apple_container() -> R<()> {
        let config = ContainerTaskConfig {
            image: "alpine".into(),
            command: vec!["echo".into(), "hello".into()],
            env: HashMap::from([("HELLO".into(), "world".into())]),
        };
        let launch = build_launch(ContainerRuntimeKind::AppleContainer, &alloc(), &config);
        assert_eq!(launch.program, "container");
        assert!(launch.args.contains(&"--env".to_string()));
        assert!(launch.args.contains(&"HELLO=world".to_string()));
        assert!(launch.args.contains(&"alpine".to_string()));
        Ok(())
    }

    #[test]
    fn container_builds_launch_for_youki() -> R<()> {
        let config = ContainerTaskConfig {
            image: "alpine".into(),
            command: vec!["echo".into(), "hello".into()],
            env: HashMap::new(),
        };
        let launch = build_launch(ContainerRuntimeKind::Youki, &alloc(), &config);
        assert_eq!(launch.program, "youki");
        assert!(launch.args.contains(&"--image".to_string()));
        assert!(launch.args.contains(&"alpine".to_string()));
        Ok(())
    }
}
