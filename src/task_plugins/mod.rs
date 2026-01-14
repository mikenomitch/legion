use crate::prelude::*;
use crate::structs::{Allocation, JobConfig, TaskConfig};
use crate::util::plugin_root_dir;
use jsonschema::JSONSchema;
use libloading::{Library, Symbol};
use std::collections::HashMap;
use std::process::Child;

mod exec;

pub use exec::ExecTaskPlugin;

#[derive(Default)]
pub struct TaskRuntime {
    pub state: Value,
    pub child: Option<Child>,
    pub pid: Option<u32>,
}

pub trait TaskPlugin: Send + Sync {
    fn name(&self) -> &'static str;

    fn schema(&self) -> Value {
        json!({"type": "object"})
    }

    fn validate_config(&self, _config: &Value) -> R<()> {
        let schema = self.schema();
        validate_schema(&schema, _config)
    }

    fn setup(&self) -> R<()> {
        Ok(())
    }

    fn readiness(&self) -> Map<String, Value> {
        Map::new()
    }

    fn prepare(
        &self,
        _allocation: &Allocation,
        _task: &TaskConfig,
        _runtime: &mut TaskRuntime,
    ) -> R<()> {
        Ok(())
    }

    fn start(
        &self,
        _allocation: &Allocation,
        _task: &TaskConfig,
        _runtime: &mut TaskRuntime,
    ) -> R<()> {
        Ok(())
    }

    fn stop(
        &self,
        _allocation: &Allocation,
        _task: &TaskConfig,
        _runtime: &mut TaskRuntime,
    ) -> R<()> {
        Ok(())
    }

    fn destroy(
        &self,
        _allocation: &Allocation,
        _task: &TaskConfig,
        _runtime: &mut TaskRuntime,
    ) -> R<()> {
        Ok(())
    }
}

pub struct LoadedTaskPlugin {
    pub plugin: Box<dyn TaskPlugin>,
    _lib: Option<Library>,
}

impl LoadedTaskPlugin {
    fn new(plugin: Box<dyn TaskPlugin>, lib: Option<Library>) -> Self {
        Self { plugin, _lib: lib }
    }
}

#[derive(Default)]
pub struct TaskPluginRegistry {
    plugins: HashMap<String, LoadedTaskPlugin>,
}

impl TaskPluginRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load_plugin(&mut self, kind: &str, run_setup: bool) -> R<()> {
        if self.plugins.contains_key(kind) {
            return Ok(());
        }

        let plugin = load_task_plugin(kind)?;
        if run_setup {
            plugin.plugin.setup()?;
        }
        self.plugins.insert(kind.to_string(), plugin);
        Ok(())
    }

    pub fn plugin(&self, kind: &str) -> Option<&dyn TaskPlugin> {
        self.plugins.get(kind).map(|entry| entry.plugin.as_ref())
    }

    pub fn readiness_checks(&self) -> Map<String, Value> {
        let mut checks = Map::new();
        for plugin in self.plugins.values() {
            checks.extend(plugin.plugin.readiness());
        }
        checks
    }

    pub fn validate_task_config(&mut self, task: &TaskConfig) -> R<()> {
        self.load_plugin(&task.kind, false)?;
        let plugin = self
            .plugin(&task.kind)
            .context("task plugin loaded but missing")?;
        plugin
            .validate_config(&task.config)
            .with_context(|| format!("task {} config invalid", task.name))?;
        Ok(())
    }
}

#[instrument(err, skip_all)]
pub fn validate_job_config(config: &JobConfig) -> R<()> {
    let mut registry = TaskPluginRegistry::new();
    for task in &config.tasks {
        registry.validate_task_config(task)?;
        task.validate_checks()?;
    }
    Ok(())
}

fn load_task_plugin(name: &str) -> R<LoadedTaskPlugin> {
    match name {
        "exec" => Ok(LoadedTaskPlugin::new(
            Box::new(ExecTaskPlugin::default()),
            None,
        )),
        other => {
            let path = task_plugin_name_to_path(other);
            unsafe {
                let lib = Library::new(&path)?;
                let plugin = {
                    let func: Symbol<unsafe extern "C" fn() -> Box<dyn TaskPlugin>> =
                        lib.get(b"get")?;
                    func()
                };
                Ok(LoadedTaskPlugin::new(plugin, Some(lib)))
            }
        }
    }
}

fn task_plugin_name_to_path(name: &str) -> String {
    let mut path = plugin_root_dir();
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

fn validate_schema(schema: &Value, config: &Value) -> R<()> {
    let schema = Box::leak(Box::new(schema.clone()));
    let compiled = JSONSchema::compile(schema).context("Invalid task config schema")?;
    if let Err(errors) = compiled.validate(config) {
        let details = errors
            .map(|err| err.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        bail!("Task config failed schema validation: {details}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::Path;

    fn alloc() -> Allocation {
        Allocation {
            id: 1,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
        }
    }

    fn exec_task() -> TaskConfig {
        TaskConfig {
            config: json!({"command": "true"}),
            ..TaskConfig::new("exec-task", "exec")
        }
    }

    #[test]
    fn task_registry_loads_exec_plugin() -> R<()> {
        let mut registry = TaskPluginRegistry::new();
        registry.validate_task_config(&exec_task())?;
        assert!(registry.plugin("exec").is_some());
        Ok(())
    }

    #[test]
    fn exec_plugin_runs_command() -> R<()> {
        let mut registry = TaskPluginRegistry::new();
        registry.load_plugin("exec", true)?;
        let plugin = registry.plugin("exec").context("missing exec plugin")?;
        let mut runtime = TaskRuntime::default();
        let task = exec_task();
        plugin.prepare(&alloc(), &task, &mut runtime)?;
        plugin.start(&alloc(), &task, &mut runtime)?;
        assert!(runtime.child.is_some());
        plugin.stop(&alloc(), &task, &mut runtime)?;
        Ok(())
    }

    #[test]
    fn noop_plugin_loads_when_built() -> R<()> {
        if std::env::var("LEGION_SKIP_PLUGIN_BUILD").is_err() {
            let path = task_plugin_name_to_path("noop");
            if Path::new(&path).exists() {
                let mut registry = TaskPluginRegistry::new();
                registry.load_plugin("noop", true)?;
                let plugin = registry.plugin("noop").context("missing noop plugin")?;
                let readiness = plugin.readiness();
                if !readiness.is_empty() {
                    assert!(readiness.contains_key("noop"));
                }
            }
        }
        Ok(())
    }
}
