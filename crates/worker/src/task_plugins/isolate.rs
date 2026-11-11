//! The built-in `isolate` runtime's submit-time surface
//! (docs/task-runtime-isolate.md, isolate-runtime plan §7.1).
//!
//! An isolate task is a session task whose units are WebAssembly isolates
//! in a node's `legion-isolate` host, started and run by the worker's
//! isolate supervisor against the task's generated envelope. Nothing ever
//! places an isolate task as an allocation (only its envelope is placed),
//! so this plugin is the schema, the validation, and the submit-time
//! defaults; `start` refuses.
//!
//! The typed config and the rules that need the whole job live in
//! [`crate::structs::isolate`], where the manager and the worker read them
//! too.

use crate::prelude::*;
use crate::structs::isolate::{IsolateTaskConfig, ISOLATE_RUNTIME};
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::{derived_schema, validate_schema, TaskInstance, TaskPlugin};

pub struct IsolateTaskPlugin;

/// What `start` answers: an isolate task is never an allocation.
const NOT_AN_ALLOCATION: &str = "isolate tasks are run by the node's isolate supervisor, not \
                                 started as allocations; only their envelope is placed";

impl TaskPlugin for IsolateTaskPlugin {
    fn name(&self) -> &'static str {
        ISOLATE_RUNTIME
    }

    fn schema(&self) -> Value {
        derived_schema::<IsolateTaskConfig>()
    }

    /// The typed parse and the single-block rules first, so a missing
    /// `limits.cpu` or two of `image`/`module` get the plan's message, then
    /// the derived schema.
    fn validate_config(&self, config: &Value) -> R<()> {
        IsolateTaskConfig::from_value(config)
            .and_then(|parsed| parsed.resolve())
            .map_err(|err| anyhow!(err))?;
        validate_schema(&self.schema(), config)
    }

    /// The artifacts deliver `config.entry` (the supervisor stages them for
    /// the task, not for an allocation); `validate_isolate_tasks` refuses
    /// artifacts on a task without an entry.
    fn supports_artifacts(&self) -> bool {
        true
    }

    /// `memoryMax` 128 MiB, `memory` the image's idle size, `cpu` 10 MHz
    /// (plan §6.11), ahead of the cluster's `taskResourceDefaults`.
    fn normalize_task(&self, task: &mut TaskConfig) {
        crate::structs::isolate::normalize_task(task);
    }

    /// An isolate cannot take devices, and its ceiling is bounded by a
    /// 32-bit linear memory (`validate_isolate_tasks` names the worker's
    /// `isolate.maxMemory` and the image's floor).
    fn check_resources(&self, task: &TaskConfig) -> R<Vec<String>> {
        if crate::devices::request_specs(&task.resources).is_ok_and(|specs| !specs.is_empty()) {
            bail!(
                "task {} asks for resources.devices, which an isolate cannot use (it has no \
                 device files and no system calls)\nHint: give the VM task the device, or drop \
                 resources.devices",
                task.name
            );
        }
        let ceiling = crate::structs::isolate::memory_max_mib(task);
        if ceiling > crate::structs::isolate::MAX_MEMORY_MAX_MIB {
            bail!(
                "task {}: resources.memoryMax {ceiling} MiB is above the isolate maximum {} MiB \
                 (a 32-bit linear memory)\nHint: run it with tier = \"vm\" for more",
                task.name,
                crate::structs::isolate::MAX_MEMORY_MAX_MIB
            );
        }
        Ok(Vec::new())
    }

    /// The reservation is re-admitted against the envelope and the ceiling
    /// is a limiter change, both live; the CPU is a scheduler weight
    /// (plan §6.11, resize Phase 1). The supervisor applies them.
    fn resize_support(&self) -> crate::structs::resize::ResizeSupport {
        crate::structs::resize::ResizeSupport::ALL
    }

    fn start(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _instance: &mut TaskInstance,
    ) -> R<()> {
        bail!(
            "allocation {} of task {}: {NOT_AN_ALLOCATION}",
            allocation.id,
            task.name
        )
    }

    fn stop(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _instance: &mut TaskInstance,
    ) -> R<()> {
        bail!(
            "allocation {} of task {}: {NOT_AN_ALLOCATION}",
            allocation.id,
            task.name
        )
    }
}

#[cfg(test)]
mod isolate_plugin_test {
    use super::*;

    fn config() -> Value {
        json!({ "image": "python", "limits": { "cpu": "100ms", "wall": "30s" } })
    }

    #[test]
    fn the_config_validates_through_the_typed_parse_and_the_schema_test() {
        let plugin = IsolateTaskPlugin;
        plugin.validate_config(&config()).unwrap();
        let err = plugin
            .validate_config(&json!({ "image": "python" }))
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("config.limits.cpu and config.limits.wall are required"),
            "{err}"
        );
        let mut typo = config();
        typo["cordonn"] = json!("job");
        assert!(plugin.validate_config(&typo).is_err());
        let schema = plugin.schema();
        for key in [
            "image", "entry", "module", "lifetime", "limits", "cordon", "vm", "env",
        ] {
            assert!(schema["properties"].get(key).is_some(), "{key}");
        }
    }

    #[test]
    fn start_refuses_and_resources_are_bounded_test() {
        let plugin = IsolateTaskPlugin;
        let mut task = TaskConfig::new("run", ISOLATE_RUNTIME);
        task.config = config();
        let allocation = Allocation {
            id: 1,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
            instance_id: None,
            network: None,
        };
        let err = plugin
            .start(&allocation, &task, &mut TaskInstance::default())
            .unwrap_err();
        assert!(err.to_string().contains("isolate supervisor"), "{err}");

        plugin.normalize_task(&mut task);
        assert_eq!(task.resources["memoryMax"], json!(128));
        assert!(plugin.check_resources(&task).unwrap().is_empty());
        task.resources["memoryMax"] = json!(8192);
        assert!(plugin.check_resources(&task).is_err());
        assert!(plugin.resize_support().memory);
        assert!(!plugin.exec_support().exec);
    }
}
