use crate::cmd::helpers::{connect_manager_client, print_table, json_flag};
use crate::prelude::*;
use crate::structs::{GetLedgerRequest, LedgerEntry, LedgerNodePrint};
pub fn config() -> Command {
    Command::new("ledger")
        .about("Shows the current state of the ledger")
        .arg(json_flag())
}

#[instrument(err, skip_all)]
pub async fn handle(matches: &clap::ArgMatches) -> R<()> {
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
        let printable: Vec<LedgerNodePrint> = ledger.iter().map(Into::into).collect();
        print_table(printable);
    }

    Ok(())
}
