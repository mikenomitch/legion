use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::prelude::*;
use crate::structs::{GetLedgerRequest, LedgerEntry};
pub fn config() -> Command {
    Command::new("ledger")
        .about("Shows the current state of the ledger")
        .arg(json_flag())
}

#[instrument(err, skip_all)]
pub async fn handle(matches: &ArgMatches) -> R<()> {
    let mut client = connect_manager_client().await?;
    let request = tonic::Request::new(GetLedgerRequest {});
    let response = client
        .get_ledger(request)
        .await
        .context("failed to request ledger state")?;
    let ledger_nodes = response.into_inner().nodes;

    let ledger: Vec<LedgerEntry> = ledger_nodes
        .into_iter()
        .map(|node| node.into_entry())
        .collect::<R<Vec<_>>>()?;

    if matches.get_flag("json") {
        println!(
            "{}",
            serde_json::to_string(&ledger).context("failed to serialize ledger to JSON")?
        );
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
                    "  • Allocation {} (task {}, job {}): status {}\n    Resources: {}\n",
                    alloc.id, alloc.task_config_id, alloc.job_id, alloc.status, resources
                );
            }
        }
    }

    Ok(())
}
