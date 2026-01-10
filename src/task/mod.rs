use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};

use jsonschema::{Draft, JSONSchema};
use lazy_static::lazy_static;
use libloading::{Library, Symbol};
use std::collections::HashMap as StdHashMap;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

lazy_static! {
    static ref DEFAULT_TASK_SCHEMA: Value = json!({});
    static ref EXEC_TASK_SCHEMA: Value = json!({
        "type": "object",
        "properties": {
            "command": { "type": "string" },
            "args": { "type": "array", "items": { "type": "string" } },
            "env": { "type": "object", "additionalProperties": { "type": "string" } },
            "workdir": { "type": "string" }
        },
        "required": ["command"],
        "additionalProperties": false
    });
    static ref NOOP_TASK_SCHEMA: Value = json!({
        "type": "object",
        "properties": {
            "message": { "type": "string" },
            "waitSeconds": { "type": "integer", "minimum": 0 }
        },
        "additionalProperties": false
    });
}

pub trait TaskRunner: Send {
    fn start(&mut self) -> R<()>;
    fn stop(&mut self) -> R<()>;
    fn destroy(&mut self) -> R<()>;
}

pub trait TaskPlugin: Send + Sync {
    fn name(&self) -> &'static str;

    fn config_schema(&self) -> &'static Value {
        &DEFAULT_TASK_SCHEMA
    }

    fn validate_config(&self, config: &Value) -> R<()> {
        let compiled = JSONSchema::options()
            .with_draft(Draft::Draft7)
            .compile(self.config_schema())
            .context("failed to compile task config schema")?;
        if let Err(errors) = compiled.validate(config) {
            let messages = errors.map(|err| err.to_string()).collect::<Vec<_>>();
            bail!("task config validation failed: {}", messages.join(", "));
        }
        Ok(())
    }

    fn setup(&self) -> R<()> {
        Ok(())
    }

    fn readiness_check(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("ready".into(), Value::String("pass".into()));
        map
    }

    fn prepare(&self, allocation: &Allocation, task: &TaskConfig) -> R<Box<dyn TaskRunner>>;
}

pub struct LoadedTaskPlugin {
    _lib: Option<Library>,
    plugin: Box<dyn TaskPlugin>,
}

impl LoadedTaskPlugin {
    fn new(lib: Option<Library>, plugin: Box<dyn TaskPlugin>) -> Self {
        Self { _lib: lib, plugin }
    }
}

pub struct TaskPluginRegistry {
    plugins: StdHashMap<String, LoadedTaskPlugin>,
}

impl TaskPluginRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            plugins: StdHashMap::new(),
        };
        registry.register_builtin(Box::new(ExecTaskPlugin));
        registry
    }

    pub fn load_installed(&mut self) -> R<()> {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("plugins");
        path.push("tasks");

        let Ok(entries) = std::fs::read_dir(&path) else {
            return Ok(());
        };

        for entry in entries.flatten() {
            let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
                continue;
            };
            if self.plugins.contains_key(&name) {
                continue;
            }
            if let Err(err) = self.load_plugin(&name) {
                warn!(task_kind = %name, "failed to load task plugin: {err}");
            }
        }

        Ok(())
    }

    pub fn setup_plugins(&self) -> R<()> {
        for plugin in self.plugins.values() {
            plugin.plugin.setup()?;
        }
        Ok(())
    }

    pub fn readiness_checks(&self) -> Map<String, Value> {
        let mut readiness = Map::new();
        for plugin in self.plugins.values() {
            let details = plugin.plugin.readiness_check();
            let ok = details
                .values()
                .all(|val| matches!(val.as_str(), Some("pass")));
            let status = if ok { "pass" } else { "fail" };
            readiness.insert(plugin.plugin.name().into(), Value::String(status.into()));
        }

        let mut map = Map::new();
        map.insert("taskReadiness".into(), Value::Object(readiness));
        map
    }

    pub fn validate_task_config(&mut self, task: &TaskConfig) -> R<()> {
        let plugin = self.plugin_for(&task.kind)?;
        plugin.validate_config(&task.config)
    }

    pub fn prepare_task(
        &mut self,
        allocation: &Allocation,
        task: &TaskConfig,
    ) -> R<Box<dyn TaskRunner>> {
        let plugin = self.plugin_for(&task.kind)?;
        plugin.validate_config(&task.config)?;
        plugin.prepare(allocation, task)
    }

    fn plugin_for(&mut self, name: &str) -> R<&Box<dyn TaskPlugin>> {
        if !self.plugins.contains_key(name) {
            if let Err(err) = self.load_plugin(name) {
                if name == "noop" {
                    warn!("falling back to built-in noop plugin: {err}");
                    self.register_builtin(Box::new(NoopTaskPlugin::default()));
                } else {
                    return Err(err);
                }
            }
        }
        self.plugins
            .get(name)
            .map(|loaded| &loaded.plugin)
            .context("task plugin missing after load")
    }

    fn register_builtin(&mut self, plugin: Box<dyn TaskPlugin>) {
        let name = plugin.name().to_string();
        self.plugins
            .entry(name)
            .or_insert_with(|| LoadedTaskPlugin::new(None, plugin));
    }

    fn load_plugin(&mut self, name: &str) -> R<()> {
        if self.plugins.contains_key(name) {
            return Ok(());
        }

        let path = task_plugin_name_to_path(name);
        unsafe {
            let lib = Library::new(&path)
                .with_context(|| format!("failed to load task plugin library at {path}"))?;
            let plugin = {
                let func: Symbol<unsafe extern "C" fn() -> Box<dyn TaskPlugin>> =
                    lib.get(b"get")?;
                func()
            };
            self.plugins
                .insert(name.to_string(), LoadedTaskPlugin::new(Some(lib), plugin));
        }

        Ok(())
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

pub struct ExecTaskPlugin;

impl TaskPlugin for ExecTaskPlugin {
    fn name(&self) -> &'static str {
        "exec"
    }

    fn config_schema(&self) -> &'static Value {
        &EXEC_TASK_SCHEMA
    }

    fn prepare(&self, allocation: &Allocation, task: &TaskConfig) -> R<Box<dyn TaskRunner>> {
        let command = task
            .config
            .get("command")
            .and_then(|val| val.as_str())
            .context("exec task missing command")?
            .to_string();
        let args = task
            .config
            .get("args")
            .and_then(|val| val.as_array())
            .map(|vals| {
                vals.iter()
                    .filter_map(|item| item.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let env = task
            .config
            .get("env")
            .and_then(|val| val.as_object())
            .map(|map| {
                map.iter()
                    .filter_map(|(key, value)| {
                        value.as_str().map(|val| (key.to_string(), val.to_string()))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let workdir = task
            .config
            .get("workdir")
            .and_then(|val| val.as_str())
            .map(|val| val.to_string());

        Ok(Box::new(ExecTaskRunner::new(
            allocation.id,
            task.name.clone(),
            command,
            args,
            env,
            workdir,
        )))
    }
}

pub struct ExecTaskRunner {
    allocation_id: i64,
    task_name: String,
    command: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
    workdir: Option<String>,
    child: Option<Child>,
}

impl ExecTaskRunner {
    fn new(
        allocation_id: i64,
        task_name: String,
        command: String,
        args: Vec<String>,
        env: Vec<(String, String)>,
        workdir: Option<String>,
    ) -> Self {
        Self {
            allocation_id,
            task_name,
            command,
            args,
            env,
            workdir,
            child: None,
        }
    }
}

impl TaskRunner for ExecTaskRunner {
    fn start(&mut self) -> R<()> {
        info!(
            allocation_id = self.allocation_id,
            task = %self.task_name,
            command = %self.command,
            "starting exec task"
        );
        let mut cmd = Command::new(&self.command);
        cmd.args(&self.args);
        for (key, value) in &self.env {
            cmd.env(key, value);
        }
        if let Some(workdir) = &self.workdir {
            cmd.current_dir(workdir);
        }
        let child = cmd.spawn().context("failed to spawn exec task")?;
        self.child = Some(child);
        Ok(())
    }

    fn stop(&mut self) -> R<()> {
        info!(
            allocation_id = self.allocation_id,
            task = %self.task_name,
            "stopping exec task"
        );
        if let Some(mut child) = self.child.take() {
            child.kill().ok();
            let _ = child.wait();
        }
        Ok(())
    }

    fn destroy(&mut self) -> R<()> {
        info!(
            allocation_id = self.allocation_id,
            task = %self.task_name,
            "destroying exec task"
        );
        self.stop()
    }
}

#[derive(Default)]
pub struct NoopTaskPlugin;

impl TaskPlugin for NoopTaskPlugin {
    fn name(&self) -> &'static str {
        "noop"
    }

    fn config_schema(&self) -> &'static Value {
        &NOOP_TASK_SCHEMA
    }

    fn prepare(&self, allocation: &Allocation, task: &TaskConfig) -> R<Box<dyn TaskRunner>> {
        let message = task
            .config
            .get("message")
            .and_then(|val| val.as_str())
            .unwrap_or("task started")
            .to_string();
        let wait_seconds = task
            .config
            .get("waitSeconds")
            .and_then(|val| val.as_u64())
            .unwrap_or(5);

        Ok(Box::new(NoopTaskRunner::new(
            allocation.id,
            task.name.clone(),
            message,
            wait_seconds,
        )))
    }
}

pub struct NoopTaskRunner {
    allocation_id: i64,
    task_name: String,
    message: String,
    wait_seconds: u64,
    running: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl NoopTaskRunner {
    fn new(allocation_id: i64, task_name: String, message: String, wait_seconds: u64) -> Self {
        Self {
            allocation_id,
            task_name,
            message,
            wait_seconds,
            running: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }
}

impl TaskRunner for NoopTaskRunner {
    fn start(&mut self) -> R<()> {
        info!(
            allocation_id = self.allocation_id,
            task = %self.task_name,
            "noop task started: {}",
            self.message
        );
        let running = self.running.clone();
        running.store(true, Ordering::SeqCst);
        let wait = self.wait_seconds;
        self.handle = Some(thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_secs(wait));
            }
        }));
        Ok(())
    }

    fn stop(&mut self) -> R<()> {
        info!(
            allocation_id = self.allocation_id,
            task = %self.task_name,
            "noop task stopping"
        );
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        Ok(())
    }

    fn destroy(&mut self) -> R<()> {
        info!(
            allocation_id = self.allocation_id,
            task = %self.task_name,
            "noop task destroying"
        );
        self.stop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::Path;

    fn sample_allocation() -> Allocation {
        Allocation {
            id: 1,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-a".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job-1".into(),
        }
    }

    fn sample_task(kind: &str, config: Value) -> TaskConfig {
        TaskConfig {
            name: "task".into(),
            count: 1,
            kind: kind.into(),
            config,
            resources: json!({}),
            constraints: json!({}),
            affinities: json!({}),
        }
    }

    #[test]
    fn exec_task_validation_requires_command() -> R<()> {
        let mut registry = TaskPluginRegistry::new();
        let task = sample_task("exec", json!({}));
        assert!(registry.validate_task_config(&task).is_err());
        Ok(())
    }

    #[test]
    fn exec_task_prepare_works() -> R<()> {
        let mut registry = TaskPluginRegistry::new();
        let task = sample_task("exec", json!({"command": "echo", "args": ["hello"]}));
        let allocation = sample_allocation();
        let runner = registry.prepare_task(&allocation, &task)?;
        drop(runner);
        Ok(())
    }

    #[test]
    fn noop_plugin_loads_when_built() -> R<()> {
        let mut registry = TaskPluginRegistry::new();
        let plugin_path = task_plugin_name_to_path("noop");
        if std::env::var("LEGION_SKIP_PLUGIN_BUILD").is_err() && Path::new(&plugin_path).exists() {
            registry.load_plugin("noop")?;
            assert!(registry.plugins.contains_key("noop"));
        }
        Ok(())
    }
}
