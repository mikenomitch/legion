use serde::{Deserialize, Serialize};
use serde_json::{to_string, Value};
use tabled::Tabled;

use crate::prelude::{Context, R};

use super::node::Node;
use super::{LedgerAllocation, LedgerNode};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Allocation {
    pub id: i64,
    pub status: String,
    pub resources: Value,
    pub node_id: String,
    pub job_config_id: String,
    pub task_config_id: i64,
    pub job_id: String,
    /// Stable instance this allocation fulfills, when the task is stateful
    /// (see `docs/stateful-workloads.md`). None for stateless tasks.
    pub instance_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LedgerEntry {
    pub node: Node,
    pub allocations: Vec<Allocation>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Tabled)]
#[serde(rename_all = "camelCase")]
pub struct LedgerNodePrint {
    pub node_id: String,
    pub status: String,
    pub allocations: String,
}

impl From<&LedgerEntry> for LedgerNodePrint {
    fn from(entry: &LedgerEntry) -> Self {
        let allocations = if entry.allocations.is_empty() {
            "None".to_string()
        } else {
            entry
                .allocations
                .iter()
                .map(|alloc| {
                    format!(
                        "{} (task {}, job {})",
                        alloc.id, alloc.task_config_id, alloc.job_config_id
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        };

        LedgerNodePrint {
            node_id: entry.node.id.clone(),
            status: entry.node.status.clone(),
            allocations,
        }
    }
}

impl From<LedgerEntry> for LedgerNode {
    fn from(entry: LedgerEntry) -> Self {
        let allocations = entry
            .allocations
            .into_iter()
            .map(LedgerAllocation::from)
            .collect();

        LedgerNode {
            node: Some(entry.node.into()),
            allocations,
        }
    }
}

impl LedgerNode {
    pub fn into_entry(self) -> R<LedgerEntry> {
        let node_msg = self.node.context("Ledger node missing node information")?;
        let node: Node = node_msg.into();

        let allocations = self
            .allocations
            .into_iter()
            .map(|allocation| allocation.into_allocation())
            .collect::<R<Vec<_>>>()?;

        Ok(LedgerEntry { node, allocations })
    }
}

impl From<Allocation> for LedgerAllocation {
    fn from(allocation: Allocation) -> Self {
        let Allocation {
            id,
            status,
            resources,
            node_id,
            job_config_id,
            task_config_id,
            job_id,
            instance_id,
        } = allocation;

        let resources = to_string(&resources).unwrap_or_else(|_| "{}".into());

        LedgerAllocation {
            id,
            status,
            resources,
            node_id,
            job_config_id,
            task_config_id,
            job_id,
            instance_id,
        }
    }
}

#[cfg(test)]
mod ledger_tests {
    use super::*;
    use serde_json::json;

    fn allocation(instance_id: Option<&str>) -> Allocation {
        Allocation {
            id: 7,
            status: "running".into(),
            resources: json!({"cpu": 100}),
            node_id: "node-a".into(),
            job_config_id: "jc-1".into(),
            task_config_id: 3,
            job_id: "db".into(),
            instance_id: instance_id.map(Into::into),
        }
    }

    #[test]
    fn ledger_allocation_round_trips_instance_id() {
        let msg = LedgerAllocation::from(allocation(Some("db-default-2")));
        assert_eq!(msg.instance_id.as_deref(), Some("db-default-2"));
        let back = msg.into_allocation().unwrap();
        assert_eq!(back.instance_id.as_deref(), Some("db-default-2"));
    }

    #[test]
    fn ledger_allocation_omits_instance_id_for_stateless() {
        let msg = LedgerAllocation::from(allocation(None));
        assert_eq!(msg.instance_id, None);
        let back = msg.into_allocation().unwrap();
        assert_eq!(back.instance_id, None);
    }
}

impl LedgerAllocation {
    pub fn into_allocation(self) -> R<Allocation> {
        let LedgerAllocation {
            id,
            status,
            resources,
            node_id,
            job_config_id,
            task_config_id,
            job_id,
            instance_id,
        } = self;

        let resources: Value =
            serde_json::from_str(&resources).context("failed to parse allocation resources")?;

        Ok(Allocation {
            id,
            status,
            resources,
            node_id,
            job_config_id,
            task_config_id,
            job_id,
            instance_id,
        })
    }
}
