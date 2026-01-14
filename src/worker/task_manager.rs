use crate::prelude::*;
use crate::structs::{manager_client::ManagerClient, Allocation, TaskAllocation, TaskConfig};
use crate::task_plugins::{TaskPluginRegistry, TaskRuntime};
use std::collections::{HashMap, HashSet};
use tonic::transport::Channel;

pub struct WorkerTaskManager {
    registry: TaskPluginRegistry,
    allocations: HashMap<i64, ManagedAllocation>,
    manager_client: Option<ManagerClient<Channel>>,
}

struct ManagedAllocation {
    allocation: Allocation,
    task: TaskConfig,
    runtime: TaskRuntime,
}

impl ManagedAllocation {
    fn new(allocation: Allocation, task: TaskConfig) -> Self {
        Self {
            allocation,
            task,
            runtime: TaskRuntime::default(),
        }
    }
}

impl WorkerTaskManager {
    pub fn new(manager_client: Option<ManagerClient<Channel>>) -> Self {
        Self {
            registry: TaskPluginRegistry::new(),
            allocations: HashMap::new(),
            manager_client,
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
        let entry = self
            .allocations
            .entry(allocation.id)
            .or_insert_with(|| ManagedAllocation::new(allocation.clone(), task.clone()));
        let runtime = &mut entry.runtime;

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Preparing task allocation"
        );
        if let Err(err) = plugin.prepare(allocation, task, runtime) {
            self.report_allocation_failed(allocation, task, &err)?;
            self.allocations.remove(&allocation.id);
            return Err(err);
        }

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Starting task allocation"
        );
        if let Err(err) = plugin.start(allocation, task, runtime) {
            self.report_allocation_failed(allocation, task, &err)?;
            self.allocations.remove(&allocation.id);
            return Err(err);
        }

        Ok(())
    }

    pub fn allocation_ids(&self) -> Vec<i64> {
        self.allocations.keys().copied().collect()
    }

    #[instrument(err, skip_all)]
    pub fn reconcile_allocations(&mut self, desired: Vec<TaskAllocation>) -> R<()> {
        let desired_ids = desired
            .iter()
            .map(|alloc| alloc.allocation_id)
            .collect::<HashSet<_>>();
        let current_ids = self
            .allocations
            .keys()
            .copied()
            .collect::<HashSet<_>>();

        for allocation in desired {
            if current_ids.contains(&allocation.allocation_id) {
                continue;
            }
            let task_config: Value = serde_json::from_str(&allocation.task_config)?;
            let task = TaskConfig {
                name: allocation.task_name.clone(),
                count: 1,
                kind: allocation.task_kind.clone(),
                config: task_config,
                resources: json!({}),
                constraints: json!({}),
                affinities: json!({}),
            };
            let alloc = Allocation {
                id: allocation.allocation_id,
                status: "running".into(),
                resources: json!({}),
                node_id: allocation.node_id.clone(),
                job_config_id: allocation.job_config_id.clone(),
                task_config_id: allocation.task_config_id,
                job_id: allocation.job_id.clone(),
            };
            self.run_allocation(&alloc, &task)?;
        }

        for alloc_id in current_ids.difference(&desired_ids).copied().collect::<Vec<_>>() {
            if let Some(mut alloc) = self.allocations.remove(&alloc_id) {
                if let Some(plugin) = self.registry.plugin(&alloc.task.kind) {
                    plugin.stop(&alloc.allocation, &alloc.task, &mut alloc.runtime)?;
                    plugin.destroy(&alloc.allocation, &alloc.task, &mut alloc.runtime)?;
                }
            }
        }

        Ok(())
    }

    fn report_allocation_failed(
        &mut self,
        allocation: &Allocation,
        task: &TaskConfig,
        err: &anyhow::Error,
    ) -> R<()> {
        let task_config = serde_json::to_string(&task.config)?;
        let request = crate::structs::AllocationFailureRequest {
            allocation: Some(TaskAllocation {
                allocation_id: allocation.id,
                job_id: allocation.job_id.clone(),
                job_config_id: allocation.job_config_id.clone(),
                node_id: allocation.node_id.clone(),
                task_config_id: allocation.task_config_id,
                task_name: task.name.clone(),
                task_kind: task.kind.clone(),
                task_config,
            }),
            reason: err.to_string(),
        };

        let Some(mut client) = self.manager_client.clone() else {
            warn!("Skipping allocation failure report; manager client missing");
            return Ok(());
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(err) = client.report_allocation_failed(request).await {
                    error!(?err, "Failed to report allocation failure");
                }
            });
        } else {
            let response =
                futures::executor::block_on(async move { client.report_allocation_failed(request).await })?;
            info!(?response, "Reported allocation failure to manager");
        }
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
        let mut manager = WorkerTaskManager::new(None);
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
