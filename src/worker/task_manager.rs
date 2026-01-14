use crate::prelude::*;
use crate::structs::{Allocation, TaskAllocation, TaskConfig, TaskOnUnhealthyConfig};
use crate::task_plugins::{TaskPluginRegistry, TaskRuntime};
use crate::worker::task_health::HealthMonitorHandle;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct FailedAllocationReport {
    pub allocation_id: i64,
    pub reason: String,
}

pub struct WorkerTaskManager {
    registry: TaskPluginRegistry,
    runtimes: HashMap<i64, TaskRuntime>,
    allocations: HashMap<i64, Allocation>,
    task_configs: HashMap<i64, TaskConfig>,
    health_monitors: HashMap<i64, HealthMonitorHandle>,
    failed: Vec<FailedAllocationReport>,
}

impl WorkerTaskManager {
    pub fn new() -> Self {
        Self {
            registry: TaskPluginRegistry::new(),
            runtimes: HashMap::new(),
            allocations: HashMap::new(),
            task_configs: HashMap::new(),
            health_monitors: HashMap::new(),
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
        if let Some(monitor) = self.health_monitors.remove(&allocation_id) {
            monitor.shutdown();
        }
        if let (Some(allocation), Some(task)) = (
            self.allocations.get(&allocation_id),
            self.task_configs.get(&allocation_id),
        ) {
            if let Some(plugin) = self.registry.plugin(&task.kind) {
                if let Some(runtime) = self.runtimes.get_mut(&allocation_id) {
                    if let Err(err) = plugin.destroy(allocation, task, runtime) {
                        warn!(
                            allocation_id,
                            task_name = %task.name,
                            error = ?err,
                            "Failed to destroy allocation runtime"
                        );
                    }
                }
            }
        }
        self.runtimes.remove(&allocation_id);
        self.allocations.remove(&allocation_id);
        self.task_configs.remove(&allocation_id);
        self.failed.push(FailedAllocationReport {
            allocation_id,
            reason: reason.into(),
        });
    }

    pub fn reconcile_allocations(
        &mut self,
        allocations: &[TaskAllocation],
    ) -> Vec<(Allocation, TaskConfig)> {
        let mut started = Vec::new();
        for allocation in allocations {
            if self.runtimes.contains_key(&allocation.allocation_id) {
                continue;
            }
            match self.run_task_allocation(allocation) {
                Ok((alloc, task)) => {
                    started.push((alloc, task));
                }
                Err(err) => {
                    self.record_failure(allocation.allocation_id, err.to_string());
                }
            }
        }
        started
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

        self.allocations.insert(allocation.id, allocation.clone());
        self.task_configs.insert(allocation.id, task.clone());

        Ok(())
    }

    pub fn run_task_allocation(
        &mut self,
        allocation: &TaskAllocation,
    ) -> R<(Allocation, TaskConfig)> {
        let (alloc, task) = task_from_allocation(allocation)?;
        self.run_allocation(&alloc, &task)?;
        Ok((alloc, task))
    }

    pub fn restart_allocation(&mut self, allocation_id: i64) -> R<()> {
        let allocation = self
            .allocations
            .get(&allocation_id)
            .context("Allocation not found for restart")?;
        let task = self
            .task_configs
            .get(&allocation_id)
            .context("Task config missing for restart")?;
        let runtime = self
            .runtimes
            .get_mut(&allocation_id)
            .context("Task runtime missing for restart")?;
        let plugin = self
            .registry
            .plugin(&task.kind)
            .context("Task plugin missing for restart")?;

        plugin.stop(allocation, task, runtime)?;
        plugin.start(allocation, task, runtime)?;
        Ok(())
    }

    pub fn register_health_monitor(&mut self, allocation_id: i64, handle: HealthMonitorHandle) {
        self.health_monitors.insert(allocation_id, handle);
    }
}

fn task_from_allocation(allocation: &TaskAllocation) -> R<(Allocation, TaskConfig)> {
    let task_config: serde_json::Value =
        serde_json::from_str(&allocation.task_config).context("Invalid task configuration JSON")?;
    let checks = if allocation.task_checks.trim().is_empty() {
        Vec::new()
    } else {
        serde_json::from_str(&allocation.task_checks).context("Invalid task health checks JSON")?
    };
    let on_unhealthy = if allocation.task_on_unhealthy.trim().is_empty() {
        TaskOnUnhealthyConfig::default()
    } else {
        serde_json::from_str(&allocation.task_on_unhealthy)
            .context("Invalid task on_unhealthy JSON")?
    };

    let task = TaskConfig {
        config: task_config,
        checks,
        on_unhealthy,
        ..TaskConfig::new(&allocation.task_name, &allocation.task_kind)
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
            config: json!({"command": "true"}),
            ..TaskConfig::new("exec-task", "exec")
        };

        manager.run_allocation(&alloc(), &task)?;
        Ok(())
    }
}
