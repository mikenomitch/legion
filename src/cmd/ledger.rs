use crate::prelude::*;

use crate::structs::manager_client::ManagerClient;
use crate::structs::GetLedgerRequest;
use std::error;

pub fn config() -> Command {
    Command::new("ledger").about("Shows the current state of the ledger")
}

#[instrument(err, skip_all)]
pub async fn handle(_matches: &clap::ArgMatches) -> Result<(), Box<dyn error::Error>> {
    let address = std::env::var("LEGION_ADDR").unwrap_or(DEF_MANAGER_ADDRESS.to_owned());
    let mut client = ManagerClient::connect(address).await?;
    let request = tonic::Request::new(GetLedgerRequest {});
    let response = client.get_ledger(request).await?;
    let message = response.into_inner().message;

    println!("Cluster Ledger: {}", message);

    Ok(())
}
