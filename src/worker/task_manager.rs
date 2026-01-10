use crate::prelude::*;
use crate::task::{discover_task_kinds, load_task_plugin, TaskAllocation, TaskHandle, TaskPlugin};
use std::collections::HashMap;

#[derive(Default)]
pub struct TaskManager {
    plugins: Mutex<HashMap<String, crate::task::LoadedTaskPlugin>>,
    handles: Mutex<HashMap<i64, Box<dyn TaskHandle>>>,
}

impl TaskManager {
    pub fn new() -> Self {
        TaskManager::default()
    }

    pub fn readiness_checks(&self) -> Map<String, Value> {
        let mut checks = Map::new();
        for kind in discover_task_kinds() {
            match self.load_plugin(&kind) {
                Ok(plugin) => {
                    checks.extend(plugin.readiness());
                }
                Err(err) => {
                    warn!(task_kind = %kind, "Task plugin readiness failed: {err}");
                    checks.insert(
                        format!("task.{kind}"),
                        Value::String(format!("fail: {err}")),
                    );
                }
            }
        }

        checks
    }

    pub fn prepare_and_start(&self, allocation: TaskAllocation) -> R<()> {
        let plugin = self.load_plugin(&allocation.task_kind)?;
        info!(
            allocation_id = allocation.allocation_id,
            task_name = allocation.task_name,
            task_kind = allocation.task_kind,
            "Preparing task"
        );
        plugin.prepare(&allocation)?;

        info!(
            allocation_id = allocation.allocation_id,
            task_name = allocation.task_name,
            task_kind = allocation.task_kind,
            "Starting task"
        );
        let handle = plugin.start(&allocation)?;
        let mut handles = self.handles.lock().unwrap();
        handles.insert(allocation.allocation_id, handle);
        Ok(())
    }

    pub fn stop(&self, allocation_id: i64) -> R<()> {
        let mut handles = self.handles.lock().unwrap();
        if let Some(handle) = handles.get_mut(&allocation_id) {
            info!(allocation_id, "Stopping task");
            handle.stop()?;
        }
        Ok(())
    }

    pub fn destroy(&self, allocation_id: i64) -> R<()> {
        let mut handles = self.handles.lock().unwrap();
        if let Some(mut handle) = handles.remove(&allocation_id) {
            info!(allocation_id, "Destroying task");
            handle.destroy()?;
        }
        Ok(())
    }

    fn load_plugin(&self, kind: &str) -> R<Arc<dyn TaskPlugin>> {
        let mut plugins = self.plugins.lock().unwrap();
        if let Some(plugin) = plugins.get(kind) {
            return Ok(plugin.plugin.clone());
        }

        let loaded = load_task_plugin(kind)?;
        loaded
            .plugin
            .setup()
            .with_context(|| format!("failed to setup task plugin {kind}"))?;
        let plugin = loaded.plugin.clone();
        plugins.insert(kind.to_string(), loaded);
        Ok(plugin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn task_manager_loads_exec_plugin() -> R<()> {
        let manager = TaskManager::new();
        let plugin = manager.load_plugin("exec")?;
        assert_eq!(plugin.name(), "exec");
        plugin.validate_config(&json!({"command": "echo"}))?;
        Ok(())
    }
}
