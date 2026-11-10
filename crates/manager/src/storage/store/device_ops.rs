//! Device assignment at allocation insert (docs/devices.md, "Assignment";
//! device-model plan §6.3).
//!
//! Every placement path writes its allocation through
//! `insert_allocation_with_data`, inside the transaction that commits it.
//! That is where a task's unassigned device requests become ids: read
//! against the node row and the allocation rows this same transaction sees
//! (this plan's earlier binds included), so two placements can never be
//! handed one device, and the ids land in the row itself
//! (`resources.devices[i].assigned`), where usage, quota, and the worker
//! read them.

use super::*;

impl Store {
    /// The `resources` to write for a new allocation on `node_id`: `None`
    /// when `resources` asks for no unassigned devices (write it as is),
    /// else a copy with every request assigned. Fails with
    /// [`crate::devices::DevicesUnavailable`] when the node cannot serve
    /// them now.
    pub(super) fn assign_devices_tx(
        &self,
        tx: &Connection,
        node_id: &str,
        resources: &Value,
    ) -> R<Option<Value>> {
        if !crate::devices::has_unassigned(resources) {
            return Ok(None);
        }
        let node = tx
            .prepare_cached(&GET_NODE)?
            .query_row([node_id], |row| self.node_from_row(row))
            .optional()?
            .with_context(|| format!("node {node_id} does not exist"))?;
        let rows: Vec<(i64, Value)> = tx
            .prepare_cached(&format!(
                "SELECT id, resources FROM allocation WHERE node_id = ?1 \
                 AND status NOT IN ('{}', '{}');",
                crate::constants::ALLOCATION_STATUS_COMPLETE,
                crate::constants::ALLOCATION_STATUS_FAILED,
            ))?
            .query_map([node_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        let pool = ResourcePool::free_from_rows(
            &node,
            rows.iter().map(|(id, resources)| (*id, resources)),
            &self.resource_policy,
        );
        let normalized = self
            .resource_policy
            .normalize_task_resources(resources, false)?;
        let request = ResourceRequest::from_value(&normalized)?;
        let Some(assigned) = pool.assign_devices(&request.devices) else {
            return Err(anyhow::Error::new(crate::devices::DevicesUnavailable::new(
                node_id,
                &request.devices,
            )));
        };
        debug!(
            node_id,
            assigned = ?assigned
                .iter()
                .map(|(_, assignment)| format!("{}={}", assignment.device, assignment.ids.join(",")))
                .collect::<Vec<_>>(),
            "Assigned devices"
        );
        Ok(Some(crate::devices::write_assignments(
            resources, &assigned,
        )))
    }
}
