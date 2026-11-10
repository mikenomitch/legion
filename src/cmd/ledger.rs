use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use crate::structs::{Allocation, GetLedgerRequest, LedgerEntry, LedgerNodeExt, Node};
use serde::Serialize;
use serde_json::Value;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LedgerJsonNode {
    id: String,
    status: String,
    availability: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LedgerJsonAllocation {
    id: i64,
    status: String,
    resources: Value,
    task_config_id: i64,
    job_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    instance_id: Option<String>,
    /// The resize override on the allocation (docs/resize.md): what
    /// differs from its task's spec, who set it, and why. Absent at spec.
    #[serde(skip_serializing_if = "Option::is_none")]
    resources_override: Option<Value>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LedgerJsonEntry {
    node: LedgerJsonNode,
    allocations: Vec<LedgerJsonAllocation>,
}

impl From<&Node> for LedgerJsonNode {
    fn from(node: &Node) -> Self {
        Self {
            id: node.id.clone(),
            status: node.status.clone(),
            availability: node.availability.clone(),
        }
    }
}

impl From<&Allocation> for LedgerJsonAllocation {
    fn from(allocation: &Allocation) -> Self {
        Self {
            id: allocation.id,
            status: allocation.status.clone(),
            resources: allocation.resources.clone(),
            task_config_id: allocation.task_config_id,
            job_id: allocation.job_id.clone(),
            instance_id: allocation.instance_id.clone(),
            resources_override: None,
        }
    }
}

impl From<&LedgerEntry> for LedgerJsonEntry {
    fn from(entry: &LedgerEntry) -> Self {
        Self {
            node: LedgerJsonNode::from(&entry.node),
            allocations: entry
                .allocations
                .iter()
                .map(LedgerJsonAllocation::from)
                .collect(),
        }
    }
}

pub fn config() -> Command {
    Command::new("ledger")
        .about("Shows the current state of the ledger")
        .arg(json_flag())
}

#[instrument(skip_all)]
pub async fn handle(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(GetLedgerRequest {});
    let response = output::resolve(
        format,
        "failed to request ledger state",
        client.get_ledger(request).await,
    )?;
    let ledger_nodes = response.into_inner().nodes;
    // The override rides on the wire allocation only; keep it by id before
    // the rows are decoded into the core type, which carries none.
    let overrides: std::collections::HashMap<i64, Value> = ledger_nodes
        .iter()
        .flat_map(|node| node.allocations.iter())
        .filter(|allocation| !allocation.resources_override.trim().is_empty())
        .filter_map(|allocation| {
            serde_json::from_str(&allocation.resources_override)
                .ok()
                .map(|record| (allocation.id, record))
        })
        .collect();

    let ledger: Vec<LedgerEntry> = ledger_nodes
        .into_iter()
        .map(|node| node.into_entry())
        .collect::<R<Vec<_>>>()?;

    if format.is_json() {
        let mut json_entries: Vec<LedgerJsonEntry> =
            ledger.iter().map(LedgerJsonEntry::from).collect();
        for allocation in json_entries
            .iter_mut()
            .flat_map(|entry| entry.allocations.iter_mut())
        {
            allocation.resources_override = overrides.get(&allocation.id).cloned();
        }
        output::print_success(&json_entries);
    } else if ledger.is_empty() {
        println!("No nodes registered in the ledger.");
    } else {
        println!("Ledger:");
        for entry in ledger {
            println!(
                "- Node {} (status: {}, availability: {})",
                entry.node.id, entry.node.status, entry.node.availability
            );

            if entry.allocations.is_empty() {
                println!("  No allocations on this node.\n");
                continue;
            }

            for alloc in entry.allocations {
                let resources = serde_json::to_string_pretty(&alloc.resources)
                    .unwrap_or_else(|_| "{}".to_string());
                println!(
                    "  • Allocation {} (task {}, job {}): status {}\n    Resources: {}{}\n",
                    alloc.id,
                    alloc.task_config_id,
                    alloc.job_id,
                    alloc.status,
                    resources,
                    overrides
                        .get(&alloc.id)
                        .map(|record| format!("\n    Resized (override over the spec): {record}"))
                        .unwrap_or_default()
                );
            }
        }
    }

    Ok(())
}
