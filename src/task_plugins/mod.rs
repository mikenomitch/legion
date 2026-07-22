use crate::manager::rollout_strategy::validate_rollout_strategy_name;
use crate::plugin_runtime::{plugin_paths, validate_manifest};
use crate::prelude::*;
use crate::structs::{
    Allocation, InstanceMode, JobConfig, RolloutReplacement, TaskConfig, TaskDependencyCondition,
    TaskLifecycle,
};
use crate::util::plugin_root_dir;
use jsonschema::JSONSchema;
use libloading::{Library, Symbol};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command};

mod container;
mod exec;
mod oci_bundle;
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

/// Legion-provided environment for a task: allocation identity plus any
/// assigned ports (`LEGION_PORT_*`, `LEGION_ADDR_*`, ...). Task plugins apply
/// these before user-declared env so user values win on key conflicts.
pub fn allocation_env(allocation: &Allocation, task: &TaskConfig) -> Vec<(String, String)> {
    let mut pairs = vec![
        ("LEGION_ALLOC_ID".to_string(), allocation.id.to_string()),
        ("LEGION_JOB_ID".to_string(), allocation.job_id.clone()),
        ("LEGION_TASK_NAME".to_string(), task.name.clone()),
    ];
    if let Some(network) = allocation.network.as_ref() {
        pairs.extend(network.env_pairs());
    }
    pairs
}

/// File name for a captured stdout stream within an allocation log directory.
pub const STDOUT_LOG_FILE: &str = "stdout.log";
/// File name for a captured stderr stream within an allocation log directory.
pub const STDERR_LOG_FILE: &str = "stderr.log";

/// Redirect a task process's stdout/stderr to per-allocation capture files.
///
/// When `runtime.log_dir` is set (the worker threads an allocation log
/// directory through the runtime), the directory is created and the command's
/// stdout/stderr are pointed at append-mode `stdout.log` / `stderr.log` files.
/// When no log directory is configured this is a no-op and the child inherits
/// the worker's standard streams, preserving prior behaviour.
pub fn configure_task_log_capture(cmd: &mut Command, runtime: &TaskRuntime) -> R<()> {
    let Some(log_dir) = runtime.log_dir.as_ref() else {
        return Ok(());
    };
    std::fs::create_dir_all(log_dir)
        .with_context(|| format!("failed to create log dir {}", log_dir.display()))?;

    let stdout = open_log_file(&log_dir.join(STDOUT_LOG_FILE))?;
    let stderr = open_log_file(&log_dir.join(STDERR_LOG_FILE))?;
    cmd.stdout(stdout);
    cmd.stderr(stderr);
    Ok(())
}

fn open_log_file(path: &std::path::Path) -> R<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open log file {}", path.display()))
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
    /// Private per-allocation directory (`allocs/<alloc_id>/`), created by
    /// the worker before `prepare()` and removed on destroy.
    pub alloc_dir: Option<std::path::PathBuf>,
    /// Private task directory (`allocs/<alloc_id>/<task_name>/`). Artifacts
    /// stage here; the exec kind's `cwd` defaults here.
    pub task_dir: Option<std::path::PathBuf>,
    /// Env vars injected by the worker core (`LEGION_ALLOC_DIR`,
    /// `LEGION_TASK_DIR`, `LEGION_ARTIFACT_<NAME>`). Kinds that run host
    /// processes apply these at start; kinds with isolated filesystems
    /// (container) skip them until they grow volume plumbing.
    pub extra_env: std::collections::BTreeMap<String, String>,
    /// Typed staged artifacts (artifact system plan §8), attached before
    /// `prepare()` so plugins consume payloads instead of guessing paths.
    pub staged_artifacts: Vec<crate::artifacts::StagedArtifact>,
    /// Directory where the task's captured stdout/stderr are written, when log
    /// capture is enabled by the worker. `None` leaves the child attached to
    /// the worker's standard streams.
    pub log_dir: Option<PathBuf>,
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

    /// Whether this kind can deliver a workload-identity token as an env var.
    fn supports_identity_env(&self) -> bool {
        false
    }

    /// Whether this kind can deliver a workload-identity token as a file.
    fn supports_identity_file(&self) -> bool {
        false
    }

    /// Whether this kind can deliver a Unix signal to the running task (needed
    /// for an identity with `changeMode: signal`).
    fn supports_signal(&self) -> bool {
        false
    }

    /// Capability gate for the `artifacts` task field, mirroring
    /// `supports_kv_env_injection` and friends. Admission rejects
    /// artifact-bearing specs for kinds that return false. Kinds that return
    /// true get File/Tree payloads with zero further code: the worker core
    /// stages them into the task dir and exposes `${artifact.<name>}` /
    /// `LEGION_ARTIFACT_<NAME>`.
    fn supports_artifacts(&self) -> bool {
        false
    }

    /// Desugars kind-config fields into artifacts (artifact system plan §8).
    /// The manager calls this at admission; this is how `container.image`
    /// joins the shared pipeline without changing its user-facing spec.
    fn implicit_artifacts(&self, task_config: &Value) -> R<Vec<crate::structs::ArtifactSpec>> {
        let _ = task_config;
        Ok(Vec::new())
    }

    /// Consumes non-file payloads during prepare (OCI images, ORAS layers).
    /// The default handles nothing because File/Tree need no consumption.
    fn consume_artifact(
        &self,
        staged: &crate::artifacts::StagedArtifact,
        runtime: &mut TaskRuntime,
    ) -> R<()> {
        let _ = (staged, runtime);
        Ok(())
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
        task.validate_network()?;
        task.validate_stateful()?;
        task.validate_identities()?;
        task.validate_artifacts()?;
        if !task.artifacts.is_empty() {
            registry.load_plugin(&task.kind, false)?;
            let plugin = registry
                .plugin(&task.kind)
                .context("task plugin loaded but missing")?;
            if !plugin.supports_artifacts() {
                bail!(
                    "Task {} kind {} does not support artifacts",
                    task.name,
                    task.kind
                );
            }
        }
        if !task.identities.is_empty() {
            registry.load_plugin(&task.kind, false)?;
            let plugin = registry
                .plugin(&task.kind)
                .context("task plugin loaded but missing")?;
            for identity in &task.identities {
                if identity.delivers_env() && !plugin.supports_identity_env() {
                    bail!(
                        "Task {} kind {} does not support identity env delivery (identity {})",
                        task.name,
                        task.kind,
                        identity.name
                    );
                }
                if identity.delivers_file() && !plugin.supports_identity_file() {
                    bail!(
                        "Task {} kind {} does not support identity file delivery (identity {})",
                        task.name,
                        task.kind,
                        identity.name
                    );
                }
                if identity.change_mode == crate::structs::workload_identity::ChangeMode::Signal
                    && !plugin.supports_signal()
                {
                    bail!(
                        "Task {} kind {} does not support signaling required by identity {} \
                         changeMode signal",
                        task.name,
                        task.kind,
                        identity.name
                    );
                }
            }
        }
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
    // Slot instance ids are derived from (job, group, slot), so two slot-mode
    // stateful tasks in one group would collide on instance identity.
    let mut slot_stateful_by_group: HashMap<&str, &str> = HashMap::new();
    for task in &config.tasks {
        if task
            .stateful
            .as_ref()
            .is_some_and(|stateful| stateful.mode == InstanceMode::Slot)
        {
            if let Some(existing) =
                slot_stateful_by_group.insert(task.group.as_str(), task.name.as_str())
            {
                bail!(
                    "Group {} declares multiple slot-mode stateful tasks ({} and {}); slot \
                     instance ids are derived from (job, group, slot) and would collide",
                    task.group,
                    existing,
                    task.name
                );
            }
        }
    }
    // A `createFirst` (replace-before-stop) rollout runs the incoming and
    // outgoing allocations concurrently. For a stateful task that means two
    // live allocations sharing one instance id, storage handle, and workload
    // identity at once — violating the "at most one live allocation per
    // instance" invariant (docs/stateful-workloads.md) with no fencing to make
    // it safe (docs/stateful-vs-nomad-k8s.md). Reject the combo at admission:
    // stateful groups must roll with the default stop-then-start strategy until
    // fencing exists.
    for task in &config.tasks {
        if !task.is_stateful() {
            continue;
        }
        let replacement = config
            .rollout_config_for_group(&task.group)
            .and_then(|rollout| rollout.replacement);
        if replacement == Some(RolloutReplacement::CreateFirst) {
            bail!(
                "task {} (group {}) is stateful and cannot use a createFirst \
                 (replace-before-stop) rollout: it would run two live allocations on the same \
                 instance id and storage handle at once, breaking the at-most-one-live-allocation \
                 invariant. Use the default stop-first replacement for stateful groups.",
                task.name,
                task.group
            );
        }
    }
    // Ordinal-aware rollout options (`rollout.slots`: `order: sequential`,
    // `updateFrom`) only have meaning for a fixed pool of ordinal slots, i.e.
    // a group whose stateful task is slot mode. Reject the block anywhere
    // else (stateless groups, keyed groups) with a pointer at the per-group
    // scoping so mixed jobs can still use it.
    let job_groups = config
        .tasks
        .iter()
        .map(|task| task.group.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for group in job_groups {
        let Some(slots) = config
            .rollout_config_for_group(group)
            .and_then(|rollout| rollout.slots)
        else {
            continue;
        };
        let slot_task = config.tasks.iter().find(|task| {
            task.group == group
                && task
                    .stateful
                    .as_ref()
                    .is_some_and(|stateful| stateful.mode == InstanceMode::Slot)
        });
        let Some(slot_task) = slot_task else {
            bail!(
                "rollout slots options (order/updateFrom) are only valid for groups with a \
                 slot-mode stateful task, and group {} has none. Scope them via \
                 rolloutByGroup.{} on a slot-mode group instead.",
                group,
                group
            );
        };
        if let Some(update_from) = slots.update_from {
            // K8s allows partition >= replicas to mean "update nothing", but
            // a Legion rollout that completes instantly while silently
            // pinning every slot to the old config is almost always a typo.
            // Reject it and point at the intent-revealing alternatives.
            if update_from >= slot_task.count.num {
                bail!(
                    "rollout slots.updateFrom ({}) must be less than task {}'s count ({}): a \
                     value >= count would update nothing. Omit updateFrom to update every slot, \
                     or lower it to stage the update at a higher slot.",
                    update_from,
                    slot_task.name,
                    slot_task.count.num
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
            instance_id: None,
            network: None,
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

    fn stateful_slot_task() -> TaskConfig {
        TaskConfig {
            count: crate::structs::TaskCount::new(2),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: None,
            }),
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("db", "exec")
        }
    }

    #[test]
    fn rejects_create_first_rollout_on_stateful_task() {
        let config = JobConfig {
            rollout: Some(crate::structs::RolloutConfig {
                replacement: Some(RolloutReplacement::CreateFirst),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "stateful-createfirst".into(),
            tasks: vec![stateful_slot_task()],
            version: None,
        };

        let err = validate_job_config(&config)
            .expect_err("createFirst over a stateful task must be rejected");
        assert!(
            err.to_string().contains("createFirst"),
            "error should name the createFirst combo: {err}"
        );
    }

    #[test]
    fn allows_stop_first_rollout_on_stateful_task() -> R<()> {
        // Default (stop-first) replacement on a stateful task is fine.
        let config = JobConfig {
            rollout: Some(crate::structs::RolloutConfig {
                replacement: Some(RolloutReplacement::DestroyFirst),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "stateful-stopfirst".into(),
            tasks: vec![stateful_slot_task()],
            version: None,
        };
        validate_job_config(&config)?;

        // No rollout block at all is also fine.
        let no_rollout = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "stateful-norollout".into(),
            tasks: vec![stateful_slot_task()],
            version: None,
        };
        validate_job_config(&no_rollout)?;
        Ok(())
    }

    fn sequential_slots(update_from: Option<u32>) -> crate::structs::RolloutSlotsConfig {
        crate::structs::RolloutSlotsConfig {
            order: Some(crate::structs::RolloutSlotOrder::Sequential),
            update_from,
        }
    }

    #[test]
    fn accepts_sequential_slots_rollout_on_slot_group() -> R<()> {
        let config = JobConfig {
            rollout: Some(crate::structs::RolloutConfig {
                slots: Some(sequential_slots(Some(1))),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "stateful-sequential".into(),
            tasks: vec![stateful_slot_task()],
            version: None,
        };
        validate_job_config(&config)
    }

    #[test]
    fn rejects_slots_rollout_on_group_without_slot_task() {
        // Stateless group.
        let config = JobConfig {
            rollout: Some(crate::structs::RolloutConfig {
                slots: Some(sequential_slots(None)),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "stateless-sequential".into(),
            tasks: vec![TaskConfig {
                config: json!({"command": ["true"]}),
                ..TaskConfig::new("api", "exec")
            }],
            version: None,
        };
        let err = validate_job_config(&config).expect_err("slots on a stateless group");
        assert!(
            err.to_string().contains("slot-mode stateful task"),
            "error should explain the slot-mode requirement: {err}"
        );

        // Keyed-mode stateful group is also not a slot pool.
        let keyed = JobConfig {
            rollout: Some(crate::structs::RolloutConfig {
                slots: Some(sequential_slots(None)),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "keyed-sequential".into(),
            tasks: vec![TaskConfig {
                stateful: Some(crate::structs::StatefulConfig {
                    mode: InstanceMode::Keyed,
                    id_source: None,
                    initial_instances: None,
                    on_lost: None,
                    storage: None,
                }),
                config: json!({"command": ["true"]}),
                ..TaskConfig::new("sandbox", "exec")
            }],
            version: None,
        };
        let err = validate_job_config(&keyed).expect_err("slots on a keyed group");
        assert!(err.to_string().contains("slot-mode stateful task"));
    }

    #[test]
    fn rejects_update_from_at_or_above_slot_count() {
        // stateful_slot_task() has count 2; updateFrom 2 would update nothing.
        let config = JobConfig {
            rollout: Some(crate::structs::RolloutConfig {
                slots: Some(sequential_slots(Some(2))),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "updatefrom-too-high".into(),
            tasks: vec![stateful_slot_task()],
            version: None,
        };
        let err = validate_job_config(&config).expect_err("updateFrom >= count");
        assert!(
            err.to_string().contains("would update nothing"),
            "error should explain the no-op and suggest omission: {err}"
        );
    }

    #[test]
    fn rejects_global_slots_when_any_group_lacks_slot_task() {
        // Global slots apply to every group; the stateless `web` group makes
        // the spec invalid, with a pointer at rolloutByGroup scoping.
        let config = JobConfig {
            rollout: Some(crate::structs::RolloutConfig {
                slots: Some(sequential_slots(None)),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "mixed-groups".into(),
            tasks: vec![
                stateful_slot_task(),
                TaskConfig {
                    group: "web".into(),
                    config: json!({"command": ["true"]}),
                    ..TaskConfig::new("api", "exec")
                },
            ],
            version: None,
        };
        let err = validate_job_config(&config).expect_err("global slots over a stateless group");
        assert!(
            err.to_string().contains("rolloutByGroup"),
            "error should point at per-group scoping: {err}"
        );

        // Scoping the slots block to the slot group fixes it.
        let scoped = JobConfig {
            rollout: None,
            rollout_by_group: std::collections::BTreeMap::from([(
                "default".to_string(),
                crate::structs::RolloutConfig {
                    slots: Some(sequential_slots(None)),
                    ..Default::default()
                },
            )]),
            name: "mixed-groups-scoped".into(),
            tasks: vec![
                stateful_slot_task(),
                TaskConfig {
                    group: "web".into(),
                    config: json!({"command": ["true"]}),
                    ..TaskConfig::new("api", "exec")
                },
            ],
            version: None,
        };
        validate_job_config(&scoped).expect("group-scoped slots block is valid");
    }

    #[test]
    fn builtin_plugin_schemas_have_descriptions_and_defaults() -> R<()> {
        validate_builtin_task_plugin_schemas()?;
        Ok(())
    }

    // --- workload identity capability gating ---

    use crate::structs::workload_identity::{ChangeMode, WorkloadIdentity, WorkloadIdentityFile};

    fn job_with(task: TaskConfig) -> JobConfig {
        JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "identity-job".into(),
            tasks: vec![task],
            version: None,
        }
    }

    fn file_identity(name: &str) -> WorkloadIdentity {
        WorkloadIdentity {
            name: name.into(),
            audience: vec!["sts.amazonaws.com".into()],
            file: Some(WorkloadIdentityFile {
                path: "/var/run/legion/identity/token".into(),
                mode: Some("0400".into()),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn exec_task_accepts_file_identity() -> R<()> {
        let task = TaskConfig {
            identities: vec![file_identity("aws")],
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("api", "exec")
        };
        validate_job_config(&job_with(task))
    }

    #[test]
    fn container_task_rejects_file_identity() {
        let task = TaskConfig {
            identities: vec![file_identity("aws")],
            config: json!({"image": "alpine", "command": ["echo", "hi"]}),
            ..TaskConfig::new("api", "container")
        };
        let err = validate_job_config(&job_with(task)).expect_err("container has no file delivery");
        assert!(err.to_string().contains("identity file delivery"), "{err}");
    }

    #[test]
    fn container_task_accepts_env_identity_with_restart() -> R<()> {
        let identity = WorkloadIdentity {
            name: "vault".into(),
            audience: vec!["vault.internal".into()],
            env: Some("VAULT_TOKEN".into()),
            change_mode: ChangeMode::Restart,
            ..Default::default()
        };
        let task = TaskConfig {
            identities: vec![identity],
            config: json!({"image": "alpine", "command": ["echo", "hi"]}),
            ..TaskConfig::new("api", "container")
        };
        validate_job_config(&job_with(task))
    }

    #[test]
    fn container_task_rejects_signal_change_mode() {
        let identity = WorkloadIdentity {
            name: "vault".into(),
            audience: vec!["vault.internal".into()],
            env: Some("VAULT_TOKEN".into()),
            change_mode: ChangeMode::Signal,
            change_signal: Some("SIGHUP".into()),
            ..Default::default()
        };
        let task = TaskConfig {
            identities: vec![identity],
            config: json!({"image": "alpine", "command": ["echo", "hi"]}),
            ..TaskConfig::new("api", "container")
        };
        let err = validate_job_config(&job_with(task)).expect_err("container cannot signal");
        assert!(err.to_string().contains("signaling"), "{err}");
    }

    #[test]
    fn duplicate_identity_names_rejected_in_job_validation() {
        let task = TaskConfig {
            identities: vec![file_identity("aws"), file_identity("aws")],
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("api", "exec")
        };
        let err = validate_job_config(&job_with(task)).expect_err("duplicate identity name");
        assert!(err.to_string().contains("duplicate identity"), "{err}");
    }

    // --- artifact capability gating (artifact system plan §8) ---

    use crate::structs::ArtifactSpec;

    #[test]
    fn exec_task_accepts_artifacts_with_valid_references() -> R<()> {
        let task = TaskConfig {
            artifacts: vec![
                ArtifactSpec::from_source("https://releases.example.com/tool.tar.gz").named("tool"),
            ],
            config: json!({"command": ["${artifact.tool}/run", "--once"]}),
            ..TaskConfig::new("batch", "exec")
        };
        validate_job_config(&job_with(task))
    }

    #[test]
    fn container_task_accepts_artifacts() -> R<()> {
        let task = TaskConfig {
            artifacts: vec![ArtifactSpec::from_source("oci://ghcr.io/acme/extra:v1")],
            config: json!({"image": "alpine", "command": ["echo", "hi"]}),
            ..TaskConfig::new("api", "container")
        };
        validate_job_config(&job_with(task))
    }

    #[test]
    fn unknown_artifact_interpolation_fails_validation() {
        let task = TaskConfig {
            artifacts: vec![
                ArtifactSpec::from_source("https://releases.example.com/tool.tar.gz").named("tool"),
            ],
            config: json!({"command": ["${artifact.tool}/run", "--w", "${artifact.modle}/w"]}),
            ..TaskConfig::new("batch", "exec")
        };
        let err = validate_job_config(&job_with(task)).expect_err("typo'd reference");
        assert!(err.to_string().contains("artifact.modle"), "{err}");
    }

    #[test]
    fn noop_kind_rejects_artifacts_when_plugin_available() {
        // The noop plugin is a dylib; gate the assertion on its presence like
        // the other noop tests.
        if std::env::var("LEGION_SKIP_PLUGIN_BUILD").is_ok() {
            return;
        }
        let path = task_plugin_name_to_path("noop");
        if !Path::new(&path).exists() {
            return;
        }
        let task = TaskConfig {
            artifacts: vec![ArtifactSpec::from_source("https://example.com/x.tar.gz")],
            config: json!({}),
            ..TaskConfig::new("noop-task", "noop")
        };
        let err = validate_job_config(&job_with(task)).expect_err("noop has no artifacts");
        assert!(
            err.to_string().contains("does not support artifacts"),
            "{err}"
        );
    }

    #[test]
    fn container_implicit_artifacts_desugar_image() -> R<()> {
        let plugin = ContainerTaskPlugin;
        assert!(plugin.supports_artifacts());
        let implicit = plugin
            .implicit_artifacts(&json!({"image": "ghcr.io/acme/api:v42", "command": ["x"]}))?;
        assert_eq!(implicit.len(), 1);
        assert_eq!(implicit[0].source, "oci://ghcr.io/acme/api:v42");
        assert_eq!(implicit[0].name.as_deref(), Some("image"));

        // Docker Hub shorthand expands to a fully-qualified reference.
        let implicit = plugin.implicit_artifacts(&json!({"image": "alpine:latest"}))?;
        assert_eq!(implicit[0].source, "oci://docker.io/library/alpine:latest");
        Ok(())
    }
}
