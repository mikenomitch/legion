#![crate_type = "cdylib"]

use legion::prelude::*;
use legion::task::{TaskAllocation, TaskHandle, TaskPlugin};

struct NoopTaskHandle {
    stop_tx: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl TaskHandle for NoopTaskHandle {
    fn stop(&mut self) -> R<()> {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
        Ok(())
    }

    fn destroy(&mut self) -> R<()> {
        self.stop()
    }
}

pub struct NoopTaskPlugin;

impl TaskPlugin for NoopTaskPlugin {
    fn name(&self) -> &'static str {
        "noop"
    }

    fn validate_config(&self, _config: &Value) -> R<()> {
        Ok(())
    }

    fn start(&self, allocation: &TaskAllocation) -> R<Box<dyn TaskHandle>> {
        info!(
            allocation_id = allocation.allocation_id,
            task_name = allocation.task_name,
            task_kind = allocation.task_kind,
            task_config = ?allocation.task_config,
            "Noop task started"
        );
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        let handle = std::thread::spawn(move || {
            let _ = stop_rx.recv();
        });
        Ok(Box::new(NoopTaskHandle {
            stop_tx: Some(stop_tx),
            thread: Some(handle),
        }))
    }
}

#[no_mangle]
pub extern "C" fn get() -> Box<dyn TaskPlugin> {
    Box::new(NoopTaskPlugin)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_plugin_starts() -> R<()> {
        let plugin = NoopTaskPlugin;
        let allocation = TaskAllocation {
            allocation_id: 1,
            node_id: "node-1".into(),
            job_id: "job".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            task_name: "noop".into(),
            task_kind: "noop".into(),
            task_config: json!({ "message": "hello" }),
            task_resources: json!({}),
        };

        let _handle = plugin.start(&allocation)?;
        Ok(())
    }
}
