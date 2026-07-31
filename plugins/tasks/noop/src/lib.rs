#![crate_type = "cdylib"]

use legion::prelude::*;
use legion::structs::{Allocation, TaskConfig};
use legion::task_plugins::{TaskPlugin, TaskInstance};

#[derive(Default)]
struct NoopTaskPlugin;

impl TaskPlugin for NoopTaskPlugin {
    fn name(&self) -> &'static str {
        "noop"
    }

    fn schema(&self) -> Value {
        json!({ "type": "object" })
    }

    fn readiness(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("noop".into(), Value::String("pass".into()));
        map
    }

    fn validate_config(&self, _config: &Value) -> legion::util::R<()> {
        Ok(())
    }

    fn prepare(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _instance: &mut TaskInstance,
    ) -> legion::util::R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Noop prepare"
        );
        Ok(())
    }

    fn start(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
    ) -> legion::util::R<()> {
        instance.state = task.config.clone();
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            config = ?task.config,
            "task started"
        );
        Ok(())
    }

    fn stop(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _instance: &mut TaskInstance,
    ) -> legion::util::R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Noop stop"
        );
        Ok(())
    }

    fn destroy(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _instance: &mut TaskInstance,
    ) -> legion::util::R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Noop destroy"
        );
        Ok(())
    }
}

// Declares which TaskPlugin vtable this dylib was built against. Without it
// the worker refuses to load the plugin rather than calling into a layout it
// cannot verify (see docs/task-runtime-plugins.md).
legion::declare_task_plugin_abi!();

#[no_mangle]
pub extern "C" fn get() -> Box<dyn TaskPlugin> {
    Box::new(NoopTaskPlugin::default())
}
