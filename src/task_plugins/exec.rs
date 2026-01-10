use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::{TaskPlugin, TaskRuntime};
use std::process::Command;

#[derive(Default)]
pub struct ExecTaskPlugin;

impl ExecTaskPlugin {
    fn config_command<'a>(&self, config: &'a Value) -> R<&'a str> {
        let obj = config
            .as_object()
            .context("exec task config must be an object")?;
        let command = obj
            .get("command")
            .and_then(|value| value.as_str())
            .context("exec task config requires string command")?;
        Ok(command)
    }

    fn config_args(&self, config: &Value) -> R<Vec<String>> {
        let obj = config
            .as_object()
            .context("exec task config must be an object")?;
        let Some(args) = obj.get("args") else {
            return Ok(Vec::new());
        };
        let args = args
            .as_array()
            .context("exec task args must be a list of strings")?;
        args.iter()
            .map(|value| {
                value
                    .as_str()
                    .map(|val| val.to_string())
                    .context("exec task args must be strings")
            })
            .collect::<R<Vec<_>>>()
    }

    fn config_env(&self, config: &Value) -> R<HashMap<String, String>> {
        let obj = config
            .as_object()
            .context("exec task config must be an object")?;
        let Some(env) = obj.get("env") else {
            return Ok(HashMap::new());
        };
        let env = env
            .as_object()
            .context("exec task env must be an object")?;
        env.iter()
            .map(|(key, value)| {
                let value = value
                    .as_str()
                    .context("exec task env values must be strings")?;
                Ok((key.clone(), value.to_string()))
            })
            .collect::<R<HashMap<_, _>>>()
    }

    fn config_cwd(&self, config: &Value) -> R<Option<String>> {
        let obj = config
            .as_object()
            .context("exec task config must be an object")?;
        let cwd = obj.get("cwd");
        match cwd {
            None => Ok(None),
            Some(value) => Ok(Some(
                value
                    .as_str()
                    .context("exec task cwd must be a string")?
                    .to_string(),
            )),
        }
    }
}

impl TaskPlugin for ExecTaskPlugin {
    fn name(&self) -> &'static str {
        "exec"
    }

    fn validate_config(&self, config: &Value) -> R<()> {
        self.config_command(config)?;
        self.config_args(config)?;
        self.config_env(config)?;
        self.config_cwd(config)?;
        Ok(())
    }

    fn readiness(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("exec".into(), Value::String("pass".into()));
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
            "Preparing exec task"
        );
        Ok(())
    }

    fn start(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        runtime: &mut TaskRuntime,
    ) -> R<()> {
        let command = self.config_command(&task.config)?;
        let args = self.config_args(&task.config)?;
        let env = self.config_env(&task.config)?;
        let cwd = self.config_cwd(&task.config)?;

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            command,
            "Starting exec task"
        );

        let mut cmd = Command::new(command);
        cmd.args(&args);
        if let Some(cwd) = cwd {
            cmd.current_dir(cwd);
        }
        if !env.is_empty() {
            cmd.envs(env);
        }

        let child = cmd.spawn().with_context(|| {
            format!("Failed to spawn exec task {} for allocation {}", task.name, allocation.id)
        })?;
        runtime.child = Some(child);
        Ok(())
    }

    fn stop(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        runtime: &mut TaskRuntime,
    ) -> R<()> {
        if let Some(mut child) = runtime.child.take() {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                "Stopping exec task"
            );
            let _ = child.kill();
            let _ = child.wait();
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
            "Destroying exec task"
        );
        self.stop(allocation, task, runtime)
    }
}
