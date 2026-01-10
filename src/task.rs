use std::path::PathBuf;
use std::process::{Child, Command};

use crate::prelude::*;
use crate::structs::TaskWork;
use libloading::{Library, Symbol};

#[derive(Debug, Clone)]
pub struct TaskAllocation {
    pub allocation_id: i64,
    pub node_id: String,
    pub job_id: String,
    pub job_config_id: String,
    pub task_config_id: i64,
    pub task_name: String,
    pub task_kind: String,
    pub task_config: Value,
    pub task_resources: Value,
}

impl TaskAllocation {
    pub fn from_work(work: TaskWork) -> R<Self> {
        let task_config = serde_json::from_str(&work.task_config)
            .context("failed to parse task config from work request")?;
        let task_resources = serde_json::from_str(&work.task_resources)
            .context("failed to parse task resources from work request")?;

        Ok(TaskAllocation {
            allocation_id: work.allocation_id,
            node_id: work.node_id,
            job_id: work.job_id,
            job_config_id: work.job_config_id,
            task_config_id: work.task_config_id,
            task_name: work.task_name,
            task_kind: work.task_kind,
            task_config,
            task_resources,
        })
    }
}

pub trait TaskHandle: Send {
    fn stop(&mut self) -> R<()> {
        Ok(())
    }

    fn destroy(&mut self) -> R<()> {
        Ok(())
    }
}

pub trait TaskPlugin: Send + Sync {
    fn name(&self) -> &'static str;

    fn validate_config(&self, config: &Value) -> R<()>;

    fn setup(&self) -> R<()> {
        Ok(())
    }

    fn readiness(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert(
            format!("task.{}", self.name()),
            Value::String("pass".into()),
        );
        map
    }

    fn prepare(&self, _allocation: &TaskAllocation) -> R<()> {
        Ok(())
    }

    fn start(&self, allocation: &TaskAllocation) -> R<Box<dyn TaskHandle>>;
}

pub struct LoadedTaskPlugin {
    pub plugin: Arc<dyn TaskPlugin>,
    #[allow(dead_code)]
    lib: Option<Library>,
}

pub fn load_task_plugin(kind: &str) -> R<LoadedTaskPlugin> {
    match kind {
        "exec" => Ok(LoadedTaskPlugin {
            plugin: Arc::new(ExecTaskPlugin::default()),
            lib: None,
        }),
        "noop" => match load_external_task_plugin(kind) {
            Ok(plugin) => Ok(plugin),
            Err(err) => {
                warn!("Noop plugin load failed; falling back to builtin: {err}");
                Ok(LoadedTaskPlugin {
                    plugin: Arc::new(NoopTaskPlugin::default()),
                    lib: None,
                })
            }
        },
        other => load_external_task_plugin(other),
    }
}

pub fn discover_task_kinds() -> Vec<String> {
    let mut kinds = vec!["exec".to_string(), "noop".to_string()];
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("plugins");
    path.push("tasks");

    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                    kinds.push(name.to_string());
                }
            }
        }
    }

    kinds.sort();
    kinds.dedup();
    kinds
}

fn load_external_task_plugin(name: &str) -> R<LoadedTaskPlugin> {
    let path = task_plugin_name_to_path(name);
    unsafe {
        let lib = Library::new(&path)?;
        let plugin = {
            let func: Symbol<unsafe extern "C" fn() -> Box<dyn TaskPlugin>> = lib.get(b"get")?;
            func()
        };
        Ok(LoadedTaskPlugin {
            plugin: Arc::from(plugin),
            lib: Some(lib),
        })
    }
}

fn task_plugin_name_to_path(name: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("plugins");
    path.push("tasks");
    path.push(name);
    path.push("target");
    path.push("release");
    path.push(format!(
        "{}{}{}",
        std::env::consts::DLL_PREFIX,
        name,
        std::env::consts::DLL_SUFFIX
    ));

    path.to_string_lossy().into_owned()
}

#[derive(Default)]
pub struct ExecTaskPlugin;

impl ExecTaskPlugin {
    fn command_from_config(
        config: &Value,
    ) -> R<(String, Vec<String>, HashMap<String, String>, Option<String>)> {
        let config = config
            .as_object()
            .context("exec task config must be an object")?;
        let command = config
            .get("command")
            .and_then(|value| value.as_str())
            .context("exec task config requires command")?
            .to_string();

        let args = match config.get("args") {
            Some(Value::Array(values)) => values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(|s| s.to_string())
                        .context("exec task args must be strings")
                })
                .collect::<R<Vec<_>>>()?,
            Some(_) => bail!("exec task args must be a list of strings"),
            None => Vec::new(),
        };

        let env = match config.get("env") {
            Some(Value::Object(map)) => map
                .iter()
                .map(|(key, value)| {
                    let value = value
                        .as_str()
                        .context("exec task env values must be strings")?;
                    Ok((key.clone(), value.to_string()))
                })
                .collect::<R<HashMap<_, _>>>()?,
            Some(_) => bail!("exec task env must be an object of strings"),
            None => HashMap::new(),
        };

        let cwd = match config.get("cwd") {
            Some(Value::String(path)) => Some(path.clone()),
            Some(_) => bail!("exec task cwd must be a string"),
            None => None,
        };

        Ok((command, args, env, cwd))
    }
}

impl TaskPlugin for ExecTaskPlugin {
    fn name(&self) -> &'static str {
        "exec"
    }

    fn validate_config(&self, config: &Value) -> R<()> {
        Self::command_from_config(config).map(|_| ())
    }

    fn start(&self, allocation: &TaskAllocation) -> R<Box<dyn TaskHandle>> {
        let (command, args, env, cwd) = Self::command_from_config(&allocation.task_config)?;
        info!(
            allocation_id = allocation.allocation_id,
            task_name = allocation.task_name,
            task_kind = allocation.task_kind,
            command = %command,
            "Starting exec task"
        );

        let mut cmd = Command::new(command);
        cmd.args(args);
        for (key, value) in env {
            cmd.env(key, value);
        }
        if let Some(cwd) = cwd {
            cmd.current_dir(cwd);
        }

        let child = cmd.spawn().context("failed to spawn exec task")?;
        Ok(Box::new(ExecTaskHandle { child }))
    }
}

pub struct ExecTaskHandle {
    child: Child,
}

impl TaskHandle for ExecTaskHandle {
    fn stop(&mut self) -> R<()> {
        if self.child.try_wait()?.is_some() {
            return Ok(());
        }
        self.child.kill().context("failed to stop exec task")?;
        Ok(())
    }

    fn destroy(&mut self) -> R<()> {
        self.stop()
    }
}

#[derive(Default)]
pub struct NoopTaskPlugin;

impl TaskPlugin for NoopTaskPlugin {
    fn name(&self) -> &'static str {
        "noop"
    }

    fn validate_config(&self, _config: &Value) -> R<()> {
        Ok(())
    }

    fn start(&self, allocation: &TaskAllocation) -> R<Box<dyn TaskHandle>> {
        info!(
            allocation_id = allocation.allocation_id,
            task_name = allocation.task_name,
            task_kind = allocation.task_kind,
            task_config = ?allocation.task_config,
            "Noop task started"
        );
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        let handle = std::thread::spawn(move || {
            let _ = stop_rx.recv();
        });
        Ok(Box::new(NoopTaskHandle {
            stop_tx: Some(stop_tx),
            thread: Some(handle),
        }))
    }
}

pub struct NoopTaskHandle {
    stop_tx: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl TaskHandle for NoopTaskHandle {
    fn stop(&mut self) -> R<()> {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
        Ok(())
    }

    fn destroy(&mut self) -> R<()> {
        self.stop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::Path;

    #[test]
    fn exec_plugin_requires_command() {
        let plugin = ExecTaskPlugin::default();
        let err = plugin
            .validate_config(&json!({}))
            .expect_err("expected error");
        assert!(err.to_string().contains("command"));
    }

    #[test]
    fn exec_plugin_accepts_config() -> R<()> {
        let plugin = ExecTaskPlugin::default();
        plugin.validate_config(&json!({
            "command": "echo",
            "args": ["hello"],
            "env": { "FOO": "bar" },
            "cwd": "/tmp"
        }))?;
        Ok(())
    }

    #[test]
    fn noop_plugin_loads_when_built() -> R<()> {
        let plugin_path = task_plugin_name_to_path("noop");
        if std::env::var("LEGION_SKIP_PLUGIN_BUILD").is_err() && Path::new(&plugin_path).exists() {
            let plugin = load_task_plugin("noop")?;
            assert_eq!(plugin.plugin.name(), "noop");
        }
        Ok(())
    }
}
