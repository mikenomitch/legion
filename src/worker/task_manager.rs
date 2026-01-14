use crate::prelude::*;
use crate::structs::{Allocation, TaskAllocation, TaskConfig};
use crate::task_plugins::{TaskPluginRegistry, TaskRuntime};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct FailedAllocationReport {
    pub allocation_id: i64,
    pub reason: String,
}

pub struct WorkerTaskManager {
    registry: TaskPluginRegistry,
    runtimes: HashMap<i64, TaskRuntime>,
    failed: Vec<FailedAllocationReport>,
}

impl WorkerTaskManager {
    pub fn new() -> Self {
        Self {
            registry: TaskPluginRegistry::new(),
            runtimes: HashMap::new(),
            failed: Vec::new(),
        }
    }

    pub fn load_builtin_plugins(&mut self) {
        if let Err(err) = self.registry.load_plugin("exec", true) {
            error!(?err, "Failed to load exec task plugin");
        }
        if let Err(err) = self.registry.load_plugin("noop", true) {
            debug!(?err, "Noop task plugin not available");
        }
    }

    pub fn readiness_checks(&self) -> Map<String, Value> {
        self.registry.readiness_checks()
    }

    pub fn allocations_snapshot(&self) -> Vec<i64> {
        let mut allocations = self.runtimes.keys().copied().collect::<Vec<_>>();
        allocations.sort_unstable();
        allocations
    }

    pub fn drain_failures(&mut self) -> Vec<FailedAllocationReport> {
        std::mem::take(&mut self.failed)
    }

    pub fn record_failure(&mut self, allocation_id: i64, reason: impl Into<String>) {
        self.runtimes.remove(&allocation_id);
        self.failed.push(FailedAllocationReport {
            allocation_id,
            reason: reason.into(),
        });
    }

    pub fn reconcile_allocations(&mut self, allocations: &[TaskAllocation]) {
        for allocation in allocations {
            if self.runtimes.contains_key(&allocation.allocation_id) {
                continue;
            }
            match self.run_task_allocation(allocation) {
                Ok(()) => {}
                Err(err) => {
                    self.record_failure(allocation.allocation_id, err.to_string());
                }
            }
        }
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
            .load_plugin(&task.kind, true)
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

    pub fn run_task_allocation(&mut self, allocation: &TaskAllocation) -> R<()> {
        let (alloc, task) = task_from_allocation(allocation)?;
        self.run_allocation(&alloc, &task)
    }
}

fn task_from_allocation(allocation: &TaskAllocation) -> R<(Allocation, TaskConfig)> {
    let task_config: serde_json::Value =
        serde_json::from_str(&allocation.task_config).context("Invalid task configuration JSON")?;

    let task = TaskConfig {
        name: allocation.task_name.clone(),
        count: 1,
        kind: allocation.task_kind.clone(),
        config: task_config,
        resources: serde_json::json!({}),
        constraints: serde_json::json!({}),
        affinities: serde_json::json!({}),
    };
    let alloc = Allocation {
        id: allocation.allocation_id,
        status: "running".into(),
        resources: serde_json::json!({}),
        node_id: allocation.node_id.clone(),
        job_config_id: allocation.job_config_id.clone(),
        task_config_id: allocation.task_config_id,
        job_id: allocation.job_id.clone(),
    };

    Ok((alloc, task))
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
