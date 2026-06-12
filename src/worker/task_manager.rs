use crate::prelude::*;
use crate::structs::{
    Allocation, AllocationHealth, TaskAllocation, TaskConfig, TaskOnUnhealthyConfig,
};
use crate::task_plugins::{TaskPluginRegistry, TaskRuntime};
use crate::worker::log_sink::{default_sinks, LogResource, LogSink};
use crate::worker::logs::{read_allocation_logs, CapturedStream};
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
    health_monitors: HashMap<i64, HealthMonitorHandle>,
    allocation_health: HashMap<i64, AllocationHealth>,
    failed: Vec<FailedAllocationReport>,
    state_store: WorkerStateStore,
    log_sinks: Vec<Box<dyn LogSink>>,
}

impl Default for WorkerTaskManager {
    fn default() -> Self {
        Self::new()
    }
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
            health_monitors: HashMap::new(),
            allocation_health: HashMap::new(),
            failed: Vec::new(),
            state_store,
            log_sinks: default_sinks(),
        }
    }

    /// Build the [`LogResource`] describing an allocation's captured logs.
    fn log_resource(&self, allocation: &Allocation, task: &TaskConfig) -> LogResource {
        LogResource {
            allocation_id: allocation.id,
            job_id: allocation.job_id.clone(),
            node_id: allocation.node_id.clone(),
            task_name: task.name.clone(),
            task_kind: task.kind.clone(),
            log_dir: self.state_store.allocation_log_dir(allocation.id),
        }
    }

    /// Notify every log sink that an allocation has started (writes manifests,
    /// etc.). Failures are logged, never fatal to task startup.
    fn notify_log_sinks_started(&self, allocation: &Allocation, task: &TaskConfig) {
        let resource = self.log_resource(allocation, task);
        for sink in &self.log_sinks {
            if let Err(err) = sink.on_start(&resource) {
                warn!(
                    allocation_id = allocation.id,
                    sink = sink.name(),
                    error = ?err,
                    "Log sink on_start failed"
                );
            }
        }
    }

    /// Run rotation/retention (and any other sink upkeep) across all active
    /// allocations. Invoked periodically by the worker maintenance loop.
    pub fn maintain_logs(&self) {
        for (allocation_id, allocation) in &self.allocations {
            let Some(task) = self.task_configs.get(allocation_id) else {
                continue;
            };
            let resource = self.log_resource(allocation, task);
            for sink in &self.log_sinks {
                if let Err(err) = sink.maintain(&resource) {
                    warn!(
                        allocation_id = *allocation_id,
                        sink = sink.name(),
                        error = ?err,
                        "Log sink maintenance failed"
                    );
                }
            }
        }
    }

    pub fn load_builtin_plugins(&mut self) {
        if let Err(err) = self.registry.load_plugin("exec", true) {
            error!(?err, "Failed to load exec task plugin");
        }
        if let Err(err) = self.registry.load_plugin("container", true) {
            error!(?err, "Failed to load container task plugin");
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

    pub fn allocation_health_snapshot(&self) -> Vec<AllocationHealth> {
        let mut entries = self.allocation_health.values().cloned().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.allocation_id);
        entries
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
        self.allocation_health.remove(&allocation_id);
        self.failed.push(FailedAllocationReport {
            allocation_id,
            reason: reason.into(),
        });
        self.persist_state();
    }

    pub fn set_allocation_health(
        &mut self,
        allocation_id: i64,
        healthy: bool,
        status: impl Into<String>,
    ) {
        self.allocation_health.insert(
            allocation_id,
            AllocationHealth {
                allocation_id,
                healthy,
                status: status.into(),
            },
        );
    }

    pub fn reconcile_allocations(
        &mut self,
        allocations: &[TaskAllocation],
    ) -> Vec<(Allocation, TaskConfig)> {
        let desired_ids = allocations
            .iter()
            .map(|allocation| allocation.allocation_id)
            .collect::<std::collections::HashSet<_>>();
        let existing_ids = self.runtimes.keys().copied().collect::<Vec<_>>();
        for allocation_id in existing_ids {
            if desired_ids.contains(&allocation_id) {
                continue;
            }
            if let Err(err) = self.stop_allocation(allocation_id) {
                warn!(
                    allocation_id,
                    error = ?err,
                    "Failed to stop undesired allocation"
                );
            }
        }

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

    fn stop_allocation(&mut self, allocation_id: i64) -> R<()> {
        if let Some(monitor) = self.health_monitors.remove(&allocation_id) {
            monitor.shutdown();
        }

        let allocation = self.allocations.get(&allocation_id).cloned();
        let task = self.task_configs.get(&allocation_id).cloned();
        if let (Some(allocation), Some(task)) = (allocation.as_ref(), task.as_ref()) {
            if let Some(plugin) = self.registry.plugin(&task.kind) {
                if let Some(runtime) = self.runtimes.get_mut(&allocation_id) {
                    if let Err(err) = plugin.stop(allocation, task, runtime) {
                        warn!(
                            allocation_id,
                            task_name = %task.name,
                            error = ?err,
                            "Failed to stop allocation runtime"
                        );
                    }
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
        self.allocation_health.remove(&allocation_id);
        self.persist_state();

        Ok(())
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
        let log_dir = self.state_store.allocation_log_dir(allocation.id);
        let runtime = self.runtimes.entry(allocation.id).or_default();
        runtime.log_dir = Some(log_dir);

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
        self.set_allocation_health(allocation.id, false, "starting");
        self.notify_log_sinks_started(allocation, task);
        self.persist_state();

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
        self.set_allocation_health(allocation_id, false, "restarting");
        self.persist_state();
        Ok(())
    }

    /// Read captured stdout/stderr for an allocation hosted on this worker.
    pub fn read_allocation_logs(
        &self,
        allocation_id: i64,
        stream: &str,
        offset: i64,
        max_bytes: i64,
        tail: bool,
    ) -> R<Vec<CapturedStream>> {
        let log_dir = self.state_store.allocation_log_dir(allocation_id);
        read_allocation_logs(&log_dir, stream, offset, max_bytes, tail)
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
                log_dir: Some(self.state_store.allocation_log_dir(record.allocation.id)),
                ..TaskRuntime::default()
            };
            self.runtimes.insert(record.allocation.id, runtime);
            self.allocations
                .insert(record.allocation.id, record.allocation.clone());
            self.task_configs
                .insert(record.allocation.id, record.task.clone());
            self.set_allocation_health(record.allocation.id, false, "restored");
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

    #[test]
    fn captures_and_reads_exec_task_logs() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);
        manager.load_builtin_plugins();

        let task = TaskConfig {
            config: json!({"command": ["sh", "-c", "echo captured-stdout"]}),
            ..TaskConfig::new("exec-task", "exec")
        };
        let allocation = alloc();
        manager.run_allocation(&allocation, &task)?;

        // Poll for the captured output rather than sleeping a fixed duration,
        // which races on slow/loaded CI and varying filesystems.
        let mut streams = Vec::new();
        for _ in 0..100 {
            streams = manager.read_allocation_logs(allocation.id, "stdout", 0, 0, false)?;
            if streams
                .first()
                .is_some_and(|s| s.content.contains("captured-stdout"))
            {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].stream, "stdout");
        assert!(
            streams[0].content.contains("captured-stdout"),
            "unexpected stdout: {:?}",
            streams[0].content
        );
        Ok(())
    }

    #[test]
    fn reconcile_removes_undesired_allocations() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);

        let allocation = alloc();
        manager
            .runtimes
            .insert(allocation.id, TaskRuntime::default());
        manager
            .allocations
            .insert(allocation.id, allocation.clone());
        manager
            .task_configs
            .insert(allocation.id, TaskConfig::new("noop-task", "noop"));
        manager.set_allocation_health(allocation.id, true, "healthy");

        assert!(manager.allocations.contains_key(&allocation.id));
        assert!(manager.allocation_health.contains_key(&allocation.id));
        let _ = manager.reconcile_allocations(&[]);
        assert!(!manager.allocations.contains_key(&allocation.id));
        assert!(!manager.runtimes.contains_key(&allocation.id));
        assert!(!manager.allocation_health.contains_key(&allocation.id));

        Ok(())
    }

    #[test]
    fn allocation_health_snapshot_sorted() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);

        manager.set_allocation_health(9, true, "healthy");
        manager.set_allocation_health(3, false, "unhealthy");

        let snapshot = manager.allocation_health_snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].allocation_id, 3);
        assert_eq!(snapshot[0].healthy, false);
        assert_eq!(snapshot[1].allocation_id, 9);
        assert_eq!(snapshot[1].healthy, true);

        Ok(())
    }

    #[test]
    fn spawn_health_monitor_without_checks_marks_allocation_healthy() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let manager = Arc::new(Mutex::new(WorkerTaskManager::with_state_store(state_store)));
        let allocation = alloc();
        let task = TaskConfig::new("noop-task", "noop");

        spawn_health_monitor(manager.clone(), allocation.clone(), task);

        let guard = manager.lock().expect("task manager");
        let health = guard
            .allocation_health
            .get(&allocation.id)
            .context("missing allocation health")?;
        assert!(health.healthy);
        assert_eq!(health.status, "healthy (no checks configured)");
        assert!(!guard.health_monitors.contains_key(&allocation.id));

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
