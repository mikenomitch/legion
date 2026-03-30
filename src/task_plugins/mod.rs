use crate::manager::rollout_strategy::validate_rollout_strategy_name;
use crate::plugin_runtime::{plugin_paths, validate_manifest};
use crate::prelude::*;
use crate::structs::{Allocation, JobConfig, TaskConfig, TaskDependencyCondition, TaskLifecycle};
use crate::util::plugin_root_dir;
use jsonschema::JSONSchema;
use libloading::{Library, Symbol};
use std::collections::HashMap;
use std::process::{Child, Command};

mod container;
mod exec;
mod schema_docs;

pub use container::ContainerTaskPlugin;
pub use exec::ExecTaskPlugin;
pub use schema_docs::{
    builtin_task_plugin_schemas, render_builtin_task_plugin_schemas_markdown,
    validate_builtin_task_plugin_schemas,
};

pub fn spawn_task_process(cmd: &mut Command) -> R<Child> {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        // Run task processes in their own session so they survive worker restarts
        // and to keep worker signal handling independent from task signal handling.
        unsafe {
            cmd.pre_exec(|| {
                unsafe extern "C" {
                    fn setsid() -> i32;
                }
                if setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    cmd.spawn().context("Failed to spawn task process")
}

pub(crate) fn derived_schema<T: schemars::JsonSchema>() -> Value {
    serde_json::to_value(schemars::schema_for!(T))
        .expect("derived task plugin schema should serialize")
}

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

    fn supports_kv_env_injection(&self) -> bool {
        false
    }

    fn supports_kv_file_injection(&self) -> bool {
        false
    }

    fn supports_live_restart(&self) -> bool {
        true
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
        task.validate_kv_injections()?;
        if task.schedule.is_some() && task.lifecycle != TaskLifecycle::Batch {
            bail!(
                "Task {} can only set schedule when lifecycle is batch",
                task.name
            );
        }
        if let Some(injections) = task.kv_injections.as_ref() {
            registry.load_plugin(&task.kind, false)?;
            let plugin = registry
                .plugin(&task.kind)
                .context("task plugin loaded but missing")?;
            if !injections.env.is_empty() && !plugin.supports_kv_env_injection() {
                bail!(
                    "Task {} kind {} does not support kv env injection",
                    task.name,
                    task.kind
                );
            }
            if !injections.files.is_empty() && !plugin.supports_kv_file_injection() {
                bail!(
                    "Task {} kind {} does not support kv file injection",
                    task.name,
                    task.kind
                );
            }
        }
    }
    config.validate_rollout()?;
    if let Some(rollout) = config.rollout.as_ref() {
        if let Some(strategy) = rollout.strategy.as_ref() {
            validate_rollout_strategy_name(strategy).with_context(|| "invalid rollout.strategy")?;
        }
    }
    for (group, rollout) in &config.rollout_by_group {
        if let Some(strategy) = rollout.strategy.as_ref() {
            validate_rollout_strategy_name(strategy)
                .with_context(|| format!("invalid rolloutByGroup.{}.strategy", group))?;
        }
    }
    validate_task_dependencies(config)?;
    Ok(())
}

fn validate_task_dependencies(config: &JobConfig) -> R<()> {
    let mut task_lookup: HashMap<String, &TaskConfig> = HashMap::new();
    for task in &config.tasks {
        if task_lookup.contains_key(&task.name) {
            bail!("Duplicate task name {}", task.name);
        }
        task_lookup.insert(task.name.clone(), task);
    }

    let mut group_per_key: HashMap<String, Option<String>> = HashMap::new();
    let mut group_has_primary: HashMap<String, bool> = HashMap::new();
    for task in &config.tasks {
        let per_key = task.count.per.clone();
        match group_per_key.get(&task.group) {
            Some(existing) if existing.as_deref() != per_key.as_deref() => {
                bail!(
                    "Group {} mixes count.per values ({:?} vs {:?})",
                    task.group,
                    existing,
                    per_key
                );
            }
            None => {
                group_per_key.insert(task.group.clone(), per_key);
            }
            _ => {}
        }
        if task.lifecycle != TaskLifecycle::Sidecar {
            group_has_primary.insert(task.group.clone(), true);
        }
    }

    for task in &config.tasks {
        if task.lifecycle == TaskLifecycle::Sidecar {
            let has_primary = group_has_primary.get(&task.group).copied().unwrap_or(false);
            if !has_primary {
                bail!(
                    "Sidecar task {} has no primary task in group {}",
                    task.name,
                    task.group
                );
            }
        }
        for dependency in &task.depends_on {
            let target = task_lookup.get(&dependency.task).with_context(|| {
                format!(
                    "Task {} depends on unknown task {}",
                    task.name, dependency.task
                )
            })?;
            if task.lifecycle == TaskLifecycle::Sidecar && target.group != task.group {
                bail!(
                    "Sidecar task {} depends on task {} outside group {}",
                    task.name,
                    dependency.task,
                    task.group
                );
            }
            if task.lifecycle == TaskLifecycle::Service
                && matches!(
                    dependency.condition,
                    TaskDependencyCondition::Complete | TaskDependencyCondition::Success
                )
                && target.lifecycle == TaskLifecycle::Service
            {
                bail!(
                    "Service task {} cannot depend on {} completion or success",
                    task.name,
                    dependency.task
                );
            }
        }
    }

    #[derive(Clone, Copy)]
    enum VisitState {
        Visiting,
        Visited,
    }

    fn visit(
        name: &str,
        task_lookup: &HashMap<String, &TaskConfig>,
        visited: &mut HashMap<String, VisitState>,
    ) -> R<()> {
        if let Some(state) = visited.get(name) {
            if matches!(state, VisitState::Visiting) {
                bail!("Task dependencies contain a cycle at {}", name);
            }
            return Ok(());
        }

        visited.insert(name.to_string(), VisitState::Visiting);
        if let Some(task) = task_lookup.get(name) {
            for dependency in &task.depends_on {
                visit(&dependency.task, task_lookup, visited)?;
            }
        }
        visited.insert(name.to_string(), VisitState::Visited);
        Ok(())
    }

    let mut visited = HashMap::new();
    for name in task_lookup.keys() {
        visit(name, &task_lookup, &mut visited)?;
    }

    Ok(())
}

fn load_task_plugin(name: &str) -> R<LoadedTaskPlugin> {
    match name {
        "exec" => Ok(LoadedTaskPlugin::new(Box::new(ExecTaskPlugin), None)),
        "container" => Ok(LoadedTaskPlugin::new(Box::new(ContainerTaskPlugin), None)),
        other => {
            let plugin_root = plugin_root_dir();
            let paths = plugin_paths("tasks", other, &plugin_root);
            validate_manifest("tasks", other, &paths.root)
                .with_context(|| format!("invalid task plugin manifest for {}", other))?;
            let path = paths.dylib.to_string_lossy().into_owned();
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

#[cfg(test)]
fn task_plugin_name_to_path(name: &str) -> String {
    let plugin_root = plugin_root_dir();
    plugin_paths("tasks", name, &plugin_root)
        .dylib
        .to_string_lossy()
        .into_owned()
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
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("exec-task", "exec")
        }
    }

    fn container_task() -> TaskConfig {
        TaskConfig {
            config: json!({
                "image": "alpine",
                "command": ["echo", "hello"],
                "env": { "MODE": "test" }
            }),
            ..TaskConfig::new("container-task", "container")
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
    fn task_registry_loads_container_plugin() -> R<()> {
        let mut registry = TaskPluginRegistry::new();
        registry.validate_task_config(&container_task())?;
        assert!(registry.plugin("container").is_some());
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

    #[test]
    fn service_task_cannot_depend_on_service_completion() {
        let config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "invalid".into(),
            tasks: vec![
                TaskConfig {
                    lifecycle: TaskLifecycle::Service,
                    config: json!({"command": ["true"]}),
                    ..TaskConfig::new("api", "exec")
                },
                TaskConfig {
                    lifecycle: TaskLifecycle::Service,
                    depends_on: vec![crate::structs::TaskDependency {
                        task: "api".into(),
                        condition: TaskDependencyCondition::Complete,
                    }],
                    config: json!({"command": ["true"]}),
                    ..TaskConfig::new("worker", "exec")
                },
            ],
            version: None,
        };

        let err = validate_job_config(&config).unwrap_err();
        assert!(err.to_string().contains("cannot depend"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn spawned_task_process_runs_in_own_session() -> R<()> {
        let mut cmd = std::process::Command::new("sleep");
        cmd.arg("1");
        let mut child = spawn_task_process(&mut cmd)?;

        let stat = std::fs::read_to_string(format!("/proc/{}/stat", child.id()))?;
        let after_comm = stat
            .rsplit_once(") ")
            .context("unexpected /proc stat format")?
            .1;
        let fields = after_comm.split_whitespace().collect::<Vec<_>>();
        let pgrp: u32 = fields
            .get(2)
            .context("missing pgrp in /proc stat")?
            .parse()
            .context("invalid pgrp in /proc stat")?;
        let session: u32 = fields
            .get(3)
            .context("missing session in /proc stat")?
            .parse()
            .context("invalid session in /proc stat")?;

        assert_eq!(pgrp, child.id());
        assert_eq!(session, child.id());

        let _ = child.kill();
        let _ = child.wait();
        Ok(())
    }

    #[test]
    fn non_batch_task_cannot_define_schedule() {
        let config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "invalid".into(),
            tasks: vec![TaskConfig {
                lifecycle: TaskLifecycle::Service,
                schedule: Some(crate::structs::TaskSchedule::Every(
                    std::time::Duration::from_secs(60),
                )),
                config: json!({"command": ["true"]}),
                ..TaskConfig::new("api", "exec")
            }],
            version: None,
        };

        let err = validate_job_config(&config).unwrap_err();
        assert!(err.to_string().contains("can only set schedule"));
    }

    #[test]
    fn sidecar_requires_primary_in_group() {
        let config = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "invalid".into(),
            tasks: vec![TaskConfig {
                lifecycle: TaskLifecycle::Sidecar,
                group: "helpers".into(),
                config: json!({"command": ["true"]}),
                ..TaskConfig::new("logger", "exec")
            }],
            version: None,
        };

        let err = validate_job_config(&config).unwrap_err();
        assert!(err.to_string().contains("Sidecar task"));
    }

    #[test]
    fn validates_default_rollout_strategy() -> R<()> {
        let config = JobConfig {
            rollout: Some(crate::structs::RolloutConfig {
                strategy: Some("default".into()),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "valid-rollout-strategy".into(),
            tasks: vec![TaskConfig {
                config: json!({"command": ["true"]}),
                ..TaskConfig::new("api", "exec")
            }],
            version: None,
        };

        validate_job_config(&config)?;
        Ok(())
    }

    #[test]
    fn rejects_unknown_rollout_strategy() {
        let config = JobConfig {
            rollout: Some(crate::structs::RolloutConfig {
                strategy: Some("does-not-exist".into()),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "invalid-rollout-strategy".into(),
            tasks: vec![TaskConfig {
                config: json!({"command": ["true"]}),
                ..TaskConfig::new("api", "exec")
            }],
            version: None,
        };

        let err = validate_job_config(&config).expect_err("unknown strategy should fail");
        assert!(err.to_string().contains("invalid rollout.strategy"));
    }

    #[test]
    fn builtin_plugin_schemas_have_descriptions_and_defaults() -> R<()> {
        validate_builtin_task_plugin_schemas()?;
        Ok(())
    }
}
