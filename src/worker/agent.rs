use crate::cmd::structs::WorkerConfig;
use crate::constants::{DEFAULT_ADDR, DEFAULT_PORT};

use log::debug;
use std::error;

pub async fn run_worker(_config: WorkerConfig) -> Result<(), Box<dyn error::Error>> {
    let addr: String = format!("{}:{}", DEFAULT_ADDR, DEFAULT_PORT)
        .parse()
        .unwrap();

    // TODO:
    // - Read the config file (if there)
    // - Fingerprint the node (after or before?)
        // - Run custom fingerprinters
    // - Register with the manager
        // - parse JWT from config or cmd
        // - send request to manager
        // - receive config bundle from manager
    // - Boot client RPC server


    debug!("Cleaning Up");
    debug!("Worker node starting on {}", addr);

    Ok(())
}