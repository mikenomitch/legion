use crate::prelude::*;
use crate::structs::{Allocation, TaskAllocation, TaskConfig};
use crate::task_plugins::{TaskPluginRegistry, TaskRuntime};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct FailedAllocationReport {
    pub allocation_id: i64,
    pub reason: String,
}

pub struct WorkerTaskManager {
    registry: TaskPluginRegistry,
    runtimes: HashMap<i64, TaskRuntime>,
    failed: Vec<FailedAllocationReport>,
    state_store: WorkerStateStore,
}

impl WorkerTaskManager {
    pub fn new() -> Self {
        Self {
            registry: TaskPluginRegistry::new(),
            runtimes: HashMap::new(),
            failed: Vec::new(),
            state_store: WorkerStateStore::new(default_worker_state_dir()),
        }
    }

    pub fn with_state_dir(path: PathBuf) -> Self {
        Self {
            registry: TaskPluginRegistry::new(),
            runtimes: HashMap::new(),
            failed: Vec::new(),
            state_store: WorkerStateStore::new(path),
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

    pub fn restore_allocations(&mut self) -> R<()> {
        let states = self.state_store.load_allocations()?;
        for state in states {
            if self.runtimes.contains_key(&state.allocation_id) {
                continue;
            }
            if !pid_is_alive(state.pid) {
                self.state_store.remove_allocation(state.allocation_id)?;
                self.record_failure(
                    state.allocation_id,
                    format!("Allocation process {} not running", state.pid),
                );
                continue;
            }
            let runtime = TaskRuntime::with_pid(state.pid);
            self.runtimes.insert(state.allocation_id, runtime);
        }
        Ok(())
    }

    pub fn allocations_snapshot(&mut self) -> Vec<i64> {
        let mut dead = Vec::new();
        for (allocation_id, runtime) in &self.runtimes {
            if let Some(pid) = runtime.pid {
                if !pid_is_alive(pid) {
                    dead.push((*allocation_id, pid));
                }
            }
        }
        for (allocation_id, pid) in dead {
            if let Err(err) = self
                .state_store
                .remove_allocation(allocation_id)
                .map_err(|e| format!("state cleanup failed: {e}"))
            {
                warn!(
                    allocation_id,
                    error = %err,
                    "Failed to clean up allocation state"
                );
            }
            self.record_failure(
                allocation_id,
                format!("Allocation process {} not running", pid),
            );
        }

        let mut allocations = self.runtimes.keys().copied().collect::<Vec<_>>();
        allocations.sort_unstable();
        allocations
    }

    pub fn drain_failures(&mut self) -> Vec<FailedAllocationReport> {
        std::mem::take(&mut self.failed)
    }

    pub fn record_failure(&mut self, allocation_id: i64, reason: impl Into<String>) {
        self.runtimes.remove(&allocation_id);
        if let Err(err) = self.state_store.remove_allocation(allocation_id) {
            warn!(
                allocation_id,
                error = ?err,
                "Failed to remove allocation state"
            );
        }
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
        runtime.pid = runtime.child.as_ref().map(|child| child.id());
        if let Some(pid) = runtime.pid {
            self.state_store
                .write_allocation(AllocationState::new(allocation.id, pid))?;
        }

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

#[derive(Debug, Serialize, Deserialize)]
struct AllocationState {
    allocation_id: i64,
    pid: u32,
}

impl AllocationState {
    fn new(allocation_id: i64, pid: u32) -> Self {
        Self { allocation_id, pid }
    }
}

struct WorkerStateStore {
    root: PathBuf,
}

impl WorkerStateStore {
    fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn write_allocation(&self, state: AllocationState) -> R<()> {
        fs::create_dir_all(&self.root)?;
        let path = self.allocation_state_path(state.allocation_id);
        let payload = serde_json::to_string(&state)?;
        fs::write(path, payload)?;
        Ok(())
    }

    fn load_allocations(&self) -> R<Vec<AllocationState>> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let mut states = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if !metadata.is_file() {
                continue;
            }
            let content = fs::read_to_string(entry.path())?;
            let state: AllocationState = serde_json::from_str(&content)?;
            states.push(state);
        }
        Ok(states)
    }

    fn remove_allocation(&self, allocation_id: i64) -> R<()> {
        let path = self.allocation_state_path(allocation_id);
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    fn allocation_state_path(&self, allocation_id: i64) -> PathBuf {
        let filename = format!("{allocation_id}.json");
        self.root.join(filename)
    }
}

fn default_worker_state_dir() -> PathBuf {
    std::env::var("LEGION_WORKER_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| Path::new("logs").join("worker-state"))
}

fn pid_is_alive(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    {
        Path::new(&format!("/proc/{pid}")).exists()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::process::Command;
    use tempfile::Builder;

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

    #[test]
    fn worker_tasks_survive_manager_drop() -> R<()> {
        let dir = Builder::new()
            .prefix("legion-worker-state-")
            .tempdir()
            .context("error creating temp dir")?;
        let mut manager = WorkerTaskManager::with_state_dir(dir.path().to_path_buf());
        manager.load_builtin_plugins();
        let task = TaskConfig {
            name: "exec-task".into(),
            count: 1,
            kind: "exec".into(),
            config: json!({"command": "sleep", "args": ["10"]}),
            resources: json!({}),
            constraints: json!({}),
            affinities: json!({}),
        };

        manager.run_allocation(&alloc(), &task)?;
        let pid = manager
            .runtimes
            .get(&alloc().id)
            .and_then(|runtime| runtime.pid)
            .context("missing pid")?;
        drop(manager);

        assert!(pid_is_alive(pid));
        let _ = Command::new("kill").arg("-9").arg(pid.to_string()).status();
        Ok(())
    }

    #[test]
    fn worker_restores_allocations_from_state() -> R<()> {
        let dir = Builder::new()
            .prefix("legion-worker-state-")
            .tempdir()
            .context("error creating temp dir")?;
        let allocation = alloc();
        {
            let mut manager = WorkerTaskManager::with_state_dir(dir.path().to_path_buf());
            manager.load_builtin_plugins();
            let task = TaskConfig {
                name: "exec-task".into(),
                count: 1,
                kind: "exec".into(),
                config: json!({"command": "sleep", "args": ["10"]}),
                resources: json!({}),
                constraints: json!({}),
                affinities: json!({}),
            };
            manager.run_allocation(&allocation, &task)?;
        }

        let mut restored = WorkerTaskManager::with_state_dir(dir.path().to_path_buf());
        restored.restore_allocations()?;
        let allocations = restored.allocations_snapshot();
        assert_eq!(allocations, vec![allocation.id]);
        let pid = restored
            .runtimes
            .get(&allocation.id)
            .and_then(|runtime| runtime.pid)
            .context("missing pid after restore")?;
        assert!(pid_is_alive(pid));
        let _ = Command::new("kill").arg("-9").arg(pid.to_string()).status();
        Ok(())
    }
}
