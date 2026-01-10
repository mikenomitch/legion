use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::{TaskPluginRegistry, TaskRuntime};
use std::collections::HashMap;

pub struct WorkerTaskManager {
    registry: TaskPluginRegistry,
    runtimes: HashMap<i64, TaskRuntime>,
}

impl WorkerTaskManager {
    pub fn new() -> Self {
        Self {
            registry: TaskPluginRegistry::new(),
            runtimes: HashMap::new(),
        }
    }

    pub fn load_builtin_plugins(&mut self) {
        if let Err(err) = self.registry.load_plugin("exec") {
            error!(?err, "Failed to load exec task plugin");
        }
        if let Err(err) = self.registry.load_plugin("noop") {
            debug!(?err, "Noop task plugin not available");
        }
    }

    pub fn readiness_checks(&self) -> Map<String, Value> {
        self.registry.readiness_checks()
    }

    #[instrument(err, skip_all)]
    pub fn run_allocation(&mut self, allocation: &Allocation, task: &TaskConfig) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            task_kind = %task.kind,
            "Received task allocation"
        );

        self.registry
            .load_plugin(&task.kind)
            .with_context(|| format!("Failed to load task plugin {}", task.kind))?;
        let plugin = self
            .registry
            .plugin(&task.kind)
            .context("Task plugin not loaded")?;
        let runtime = self
            .runtimes
            .entry(allocation.id)
            .or_insert_with(TaskRuntime::default);

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Preparing task allocation"
        );
        plugin.prepare(allocation, task, runtime)?;

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Starting task allocation"
        );
        plugin.start(allocation, task, runtime)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn alloc() -> Allocation {
        Allocation {
            id: 5,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
        }
    }

    #[test]
    fn worker_task_manager_runs_exec_plugin() -> R<()> {
        let mut manager = WorkerTaskManager::new();
        manager.load_builtin_plugins();
        let task = TaskConfig {
            name: "exec-task".into(),
            count: 1,
            kind: "exec".into(),
            config: json!({"command": "true"}),
            resources: json!({}),
            constraints: json!({}),
            affinities: json!({}),
        };

        manager.run_allocation(&alloc(), &task)?;
        Ok(())
    }
}
