use crate::cmd::helpers::print_table;
use crate::prelude::*;

use crate::structs::manager_client::ManagerClient;
use crate::structs::{GetLedgerRequest, LedgerEntry, LedgerNodePrint};
use std::error;

pub fn config() -> Command {
    Command::new("ledger")
        .about("Shows the current state of the ledger")
        .arg(
            Arg::new("json")
                .long("json")
                .short('j')
                .help("returns values in json format")
                .num_args(0),
        )
}

#[instrument(err, skip_all)]
pub async fn handle(matches: &clap::ArgMatches) -> Result<(), Box<dyn error::Error>> {
    let address = std::env::var("LEGION_ADDR").unwrap_or(DEF_MANAGER_ADDRESS.to_owned());
    let mut client = ManagerClient::connect(address).await?;
    let request = tonic::Request::new(GetLedgerRequest {});
    let response = client.get_ledger(request).await?;
    let ledger_nodes = response.into_inner().nodes;

    let ledger: Vec<LedgerEntry> = ledger_nodes
        .into_iter()
        .map(|node| node.into_entry())
        .collect::<Result<Vec<_>, _>>()?;

    if matches.get_flag("json") {
        println!("{}", serde_json::to_string(&ledger)?);
    } else if ledger.is_empty() {
        println!("No nodes registered in the ledger.");
    } else {
        let printable: Vec<LedgerNodePrint> = ledger.iter().map(Into::into).collect();
        print_table(printable);
    }

    Ok(())
}
