//! The built-in `envelope` runtime (docs/session-mode.md).
//!
//! A session task's generated envelope task runs on it: one allocation per
//! eligible node that reserves the room the node's sessions fill. The
//! allocation is the reservation, so there is nothing to start. The worker
//! admits sessions against it (`worker/task_manager.rs`, session admission)
//! and ends them when the envelope leaves the node.

use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::{TaskInstance, TaskPlugin};

pub struct EnvelopeTaskPlugin;

impl TaskPlugin for EnvelopeTaskPlugin {
    fn name(&self) -> &'static str {
        crate::structs::session::ENVELOPE_RUNTIME
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "description": "Generated for a session task's envelope; takes no config.",
            "additionalProperties": false,
        })
    }

    fn start(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _instance: &mut TaskInstance,
    ) -> R<()> {
        debug!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Envelope reserved"
        );
        Ok(())
    }
}
