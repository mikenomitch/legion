use crate::cmd::structs::WorkerConfig;
use crate::constants::{DEFAULT_ADDR, DEFAULT_PORT};

use log::{debug, error};
use std::error;

pub async fn run_worker(config: WorkerConfig) -> Result<(), Box<dyn error::Error>> {
    let addr: String = format!("{}:{}", DEFAULT_ADDR, DEFAULT_PORT)
        .parse()
        .unwrap();

    config.worker_config.fingerprinters.iter().for_each(|f| {
        error!("Fingerprinter: {}", f);
    });

    // TODO: USE THIS GUIDE
    // https://michael-f-bryan.github.io/rust-ffi-guide/dynamic_loading.html

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

// fn call_dynamic() -> Result<u32, Box<dyn std::error::Error>> {
//     unsafe {
//         let lib = libloading::Library::new("/path/to/liblibrary.so")?;
//         let func: libloading::Symbol<unsafe extern fn() -> u32> = lib.get(b"my_func")?;
//         Ok(func())
//     }
// }