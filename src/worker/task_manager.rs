use crate::prelude::*;
use crate::structs::{Allocation, JobType, TaskAllocation, TaskConfig, TaskOnUnhealthyConfig};
use crate::task_plugins::{TaskPluginRegistry, TaskRuntime};
use crate::worker::state::{pid_is_running, PersistedAllocation, WorkerStateStore};
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
    job_types: HashMap<i64, JobType>,
    health_monitors: HashMap<i64, HealthMonitorHandle>,
    failed: Vec<FailedAllocationReport>,
    completed: Vec<i64>,
    state_store: WorkerStateStore,
}

impl WorkerTaskManager {
    pub fn new() -> Self {
        Self::with_state_store(WorkerStateStore::new(WorkerStateStore::default_path()))
    }

    pub fn with_state_store(state_store: WorkerStateStore) -> Self {
        Self {
            registry: TaskPluginRegistry::new(),
            runtimes: HashMap::new(),
            allocations: HashMap::new(),
            task_configs: HashMap::new(),
            job_types: HashMap::new(),
            health_monitors: HashMap::new(),
            failed: Vec::new(),
            completed: Vec::new(),
            state_store,
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

    pub fn drain_completions(&mut self) -> Vec<i64> {
        std::mem::take(&mut self.completed)
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
        self.job_types.remove(&allocation_id);
        self.failed.push(FailedAllocationReport {
            allocation_id,
            reason: reason.into(),
        });
        self.persist_state();
    }

    pub fn record_completion(&mut self, allocation_id: i64) {
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
        self.job_types.remove(&allocation_id);
        self.completed.push(allocation_id);
        self.persist_state();
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
        self.persist_state();

        Ok(())
    }

    pub fn run_task_allocation(
        &mut self,
        allocation: &TaskAllocation,
    ) -> R<(Allocation, TaskConfig)> {
        let (alloc, task, job_type) = task_from_allocation(allocation)?;
        self.run_allocation(&alloc, &task)?;
        self.job_types.insert(alloc.id, job_type);
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
        self.persist_state();
        Ok(())
    }

    pub fn register_health_monitor(&mut self, allocation_id: i64, handle: HealthMonitorHandle) {
        self.health_monitors.insert(allocation_id, handle);
    }

    pub fn restore_allocations(&mut self) -> R<Vec<(Allocation, TaskConfig)>> {
        let persisted = self.state_store.load()?;
        let mut active = Vec::new();
        let mut restored = Vec::new();

        for record in persisted {
            let Some(pid) = record.pid else {
                warn!(
                    allocation_id = record.allocation.id,
                    task_name = %record.task.name,
                    "Skipping persisted allocation without pid"
                );
                continue;
            };
            if !pid_is_running(pid) {
                warn!(
                    allocation_id = record.allocation.id,
                    task_name = %record.task.name,
                    pid,
                    "Skipping persisted allocation with dead pid"
                );
                continue;
            }

            let runtime = TaskRuntime {
                pid: Some(pid),
                ..TaskRuntime::default()
            };
            self.runtimes.insert(record.allocation.id, runtime);
            self.allocations
                .insert(record.allocation.id, record.allocation.clone());
            self.task_configs
                .insert(record.allocation.id, record.task.clone());
            self.job_types.insert(record.allocation.id, record.job_type);
            active.push(record.clone());
            restored.push((record.allocation, record.task));
        }

        self.persist_state_with(&active);

        Ok(restored)
    }

    fn persist_state(&self) {
        let mut records = Vec::new();
        for (allocation_id, allocation) in &self.allocations {
            let Some(task) = self.task_configs.get(allocation_id) else {
                continue;
            };
            let job_type = self
                .job_types
                .get(allocation_id)
                .copied()
                .unwrap_or_default();
            let pid = self
                .runtimes
                .get(allocation_id)
                .and_then(|runtime| runtime.pid);
            if pid.is_none() {
                warn!(
                    allocation_id = allocation_id,
                    task_name = %task.name,
                    "Skipping persistence for allocation without pid"
                );
                continue;
            }
            records.push(PersistedAllocation {
                allocation: allocation.clone(),
                task: task.clone(),
                job_type,
                pid,
            });
        }

        self.persist_state_with(&records);
    }

    fn persist_state_with(&self, records: &[PersistedAllocation]) {
        if let Err(err) = self.state_store.save(records) {
            warn!(error = ?err, "Failed to persist worker allocation state");
        }
    }

    pub fn poll_run_completions(&mut self) {
        let mut finished = Vec::new();
        for allocation_id in self.runtimes.keys().copied().collect::<Vec<_>>() {
            if !matches!(self.job_types.get(&allocation_id), Some(JobType::Run)) {
                continue;
            }

            let exit_status = {
                let runtime = match self.runtimes.get_mut(&allocation_id) {
                    Some(runtime) => runtime,
                    None => continue,
                };
                if let Some(child) = runtime.child.as_mut() {
                    match child.try_wait() {
                        Ok(Some(status)) => Some(Ok(status)),
                        Ok(None) => None,
                        Err(err) => Some(Err(format!("run task exit check failed: {err}"))),
                    }
                } else if let Some(pid) = runtime.pid {
                    if pid_is_running(pid) {
                        None
                    } else {
                        Some(Err(
                            "run task exited but exit status is unavailable".to_string()
                        ))
                    }
                } else {
                    None
                }
            };

            if let Some(result) = exit_status {
                finished.push((allocation_id, result));
            }
        }

        for (allocation_id, result) in finished {
            match result {
                Ok(status) => {
                    if status.success() {
                        self.record_completion(allocation_id);
                    } else {
                        self.record_failure(
                            allocation_id,
                            format!("run task exited with status {status}"),
                        );
                    }
                }
                Err(reason) => self.record_failure(allocation_id, reason),
            }
        }
    }
}

fn task_from_allocation(allocation: &TaskAllocation) -> R<(Allocation, TaskConfig, JobType)> {
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

    let job_type = if allocation.job_type.trim().is_empty() {
        JobType::default()
    } else {
        JobType::parse(&allocation.job_type)?
    };

    Ok((alloc, task, job_type))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{TaskCheck, TaskCheckKind, TaskCheckType};
    use crate::worker::task_health::spawn_health_monitor;
    use serde_json::json;
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

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
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);
        manager.load_builtin_plugins();
        let task = TaskConfig {
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("exec-task", "exec")
        };

        manager.run_allocation(&alloc(), &task)?;
        Ok(())
    }

    #[tokio::test]
    async fn worker_restores_allocations_and_monitors() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_path = temp_dir.path().join("worker_state.json");

        let state_store = WorkerStateStore::new(state_path.clone());
        let mut manager = WorkerTaskManager::with_state_store(state_store);
        manager.load_builtin_plugins();

        let allocation = alloc();
        let task = TaskConfig {
            config: json!({"command": ["sleep", "5"]}),
            checks: vec![TaskCheck {
                check_type: TaskCheckType::Health,
                name: Some("heartbeat".into()),
                kind: TaskCheckKind::Exec,
                command: Some(vec!["true".into()]),
                path: None,
                port: None,
                every: Duration::from_millis(50),
                timeout: Duration::from_millis(50),
                unhealthy_after_count: 3,
                boot_grace_time: None,
                unhealthy_after_count_timespan: None,
            }],
            ..TaskConfig::new("exec-task", "exec")
        };

        manager.run_allocation(&allocation, &task)?;

        thread::sleep(Duration::from_millis(50));

        let mut restarted_manager =
            WorkerTaskManager::with_state_store(WorkerStateStore::new(state_path));
        restarted_manager.load_builtin_plugins();

        let restored = restarted_manager.restore_allocations()?;
        assert_eq!(restored.len(), 1);

        let manager = Arc::new(Mutex::new(restarted_manager));
        let (restored_alloc, restored_task) = restored.into_iter().next().unwrap();
        spawn_health_monitor(manager.clone(), restored_alloc, restored_task);

        {
            let guard = manager.lock().expect("task manager");
            assert!(guard.health_monitors.contains_key(&allocation.id));
        }

        let pid = {
            let guard = manager.lock().expect("task manager");
            guard
                .runtimes
                .get(&allocation.id)
                .and_then(|runtime| runtime.pid)
                .context("pid missing")?
        };

        let mut system = sysinfo::System::new();
        system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
        if let Some(process) = system.process(sysinfo::Pid::from_u32(pid)) {
            process.kill();
        }

        Ok(())
    }
}
