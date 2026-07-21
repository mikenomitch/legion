use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::{derived_schema, spawn_task_process, TaskPlugin, TaskRuntime};
use schemars::JsonSchema;
use serde::Deserialize;
use std::process::Command;

fn default_container_env() -> HashMap<String, String> {
    HashMap::new()
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct ContainerTaskConfig {
    /// Container image reference used for task execution.
    image: String,

    /// Command and arguments to run inside the container.
    command: Vec<String>,

    /// Environment variables passed through to the container runtime.
    #[serde(default = "default_container_env")]
    #[schemars(default = "default_container_env")]
    env: HashMap<String, String>,

    /// Additional plugin-specific fields reserved for forward compatibility.
    #[serde(default, flatten)]
    #[schemars(skip)]
    _extra: HashMap<String, Value>,
}

impl ContainerTaskConfig {
    fn from_value(config: &Value) -> R<Self> {
        let parsed: Self = serde_json::from_value(config.clone())
            .context("container task config must be an object")?;
        parsed.validate()?;
        Ok(parsed)
    }

    fn validate(&self) -> R<()> {
        if self.command.is_empty() {
            bail!("container task command must include at least one entry");
        }
        Ok(())
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
    legion_env: &[(String, String)],
) -> ContainerLaunch {
    let name = format!("legion-{}", allocation.id);
    let mut args = Vec::new();

    // Legion-provided env first so user-declared env wins on key conflicts.
    let mut env_args = Vec::new();
    for (key, value) in legion_env {
        if config.env.contains_key(key) {
            continue;
        }
        env_args.push("--env".to_string());
        env_args.push(format!("{key}={value}"));
    }
    for (key, value) in &config.env {
        env_args.push("--env".to_string());
        env_args.push(format!("{key}={value}"));
    }

    match runtime {
        ContainerRuntimeKind::AppleContainer => {
            args.extend([
                "run".to_string(),
                "--rm".to_string(),
                "--name".to_string(),
                name,
            ]);
            args.extend(env_args);
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
            args.extend(env_args);
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
        derived_schema::<ContainerTaskConfig>()
    }

    fn readiness(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("container".into(), Value::String("pass".into()));
        map
    }

    fn supports_kv_env_injection(&self) -> bool {
        true
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
        let legion_env = crate::task_plugins::allocation_env(allocation, task);
        let launch = build_launch(runtime_kind, allocation, &config, &legion_env);

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            program = %launch.program,
            "Starting container task"
        );

        let mut cmd = Command::new(&launch.program);
        cmd.args(&launch.args);

        let child = spawn_task_process(&mut cmd).with_context(|| {
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
            network: None,
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
        assert_eq!(
            parsed.command,
            vec!["echo".to_string(), "hello".to_string()]
        );
        assert_eq!(parsed.env.get("MODE"), Some(&"test".to_string()));
        Ok(())
    }

    #[test]
    fn container_config_requires_image() {
        let config = json!({"command": ["echo"]});
        let err = ContainerTaskConfig::from_value(&config).expect_err("missing image");
        let details = format!("{err:#}");
        assert!(details.contains("missing field `image`"));
    }

    #[test]
    fn container_config_requires_command() {
        let config = json!({"image": "alpine"});
        let err = ContainerTaskConfig::from_value(&config).expect_err("missing command");
        let details = format!("{err:#}");
        assert!(details.contains("missing field `command`"));
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
        let details = format!("{err:#}");
        assert!(details.contains("invalid type"));
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
    fn launch_command_uses_runtime_binary() -> R<()> {
        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["echo", "ok"]
        }))?;
        let launch = build_launch(ContainerRuntimeKind::Youki, &alloc(), &config, &[]);
        assert_eq!(launch.program, "youki");
        assert!(launch.args.contains(&"--image".to_string()));
        Ok(())
    }

    #[test]
    fn launch_command_includes_legion_env() -> R<()> {
        let config = ContainerTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["echo", "ok"]
        }))?;
        let legion_env = vec![("LEGION_PORT_HTTP".to_string(), "20000".to_string())];
        let launch = build_launch(ContainerRuntimeKind::Youki, &alloc(), &config, &legion_env);
        assert!(launch.args.contains(&"LEGION_PORT_HTTP=20000".to_string()));
        Ok(())
    }
}
