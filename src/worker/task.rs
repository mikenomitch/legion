use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task::{TaskPluginRegistry, TaskRunner};

use std::collections::HashMap as StdHashMap;

pub struct TaskManager {
    registry: Mutex<TaskPluginRegistry>,
    running: Mutex<StdHashMap<i64, RunningTask>>,
}

#[allow(dead_code)]
struct RunningTask {
    allocation: Allocation,
    task_config: TaskConfig,
    runner: Box<dyn TaskRunner>,
}

impl TaskManager {
    pub fn new() -> R<Self> {
        let mut registry = TaskPluginRegistry::new();
        registry.load_installed()?;
        registry.setup_plugins()?;

        Ok(Self {
            registry: Mutex::new(registry),
            running: Mutex::new(StdHashMap::new()),
        })
    }

    pub fn readiness_checks(&self) -> R<Map<String, Value>> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| anyhow!("task plugin registry lock poisoned"))?;
        Ok(registry.readiness_checks())
    }

    pub fn prepare_and_start(&self, allocation: Allocation, task_config: TaskConfig) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task = %task_config.name,
            kind = %task_config.kind,
            "received task allocation"
        );
        let runner = {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| anyhow!("task plugin registry lock poisoned"))?;
            registry.prepare_task(&allocation, &task_config)?
        };
        let mut runner = runner;
        runner.start()?;

        let mut running = self
            .running
            .lock()
            .map_err(|_| anyhow!("task manager lock poisoned"))?;
        running.insert(
            allocation.id,
            RunningTask {
                allocation,
                task_config,
                runner,
            },
        );
        Ok(())
    }
}
