use crate::cmd::structs::WorkerConfig;
use crate::constants::{DEFAULT_ADDR, DEFAULT_PORT};

use log::{debug, error};
use std::error;
use serde_json::Value;
use std::collections::HashMap;

pub async fn run_worker(config: WorkerConfig) -> Result<(), Box<dyn error::Error>> {
    let addr: String = format!("{}:{}", DEFAULT_ADDR, DEFAULT_PORT)
        .parse()
        .unwrap();

    let _fingerprint_data = fingerprint_node(&config);

    // TODO: USE THIS GUIDE
    // https://michael-f-bryan.github.io/rust-ffi-guide/dynamic_loading.html
    // using dylib crate type: https://doc.rust-lang.org/reference/linkage.html

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

fn fingerprint_node(config: &WorkerConfig) -> HashMap<String, Value> {
    let mut fps: Vec<Box<dyn Fingerprinter>> = Vec::new();
    fps.push(Box::new(ArchFingerprinter{}));
    fps.push(Box::new(OSFingerprinter{}));

    config.worker_config.additional_fingerprinters.iter().for_each(|f| {
        error!("Fingerprinter: {}", f);
    });

    fps.iter().fold(HashMap::new(), |acc, f| {
        let data = f.fingerprint();
        acc.into_iter().chain(data).collect()
    })
}


trait Fingerprinter {
    fn fingerprint(&self) -> HashMap<String, Value> {
        let mut map = HashMap::new();
        map.insert("data".to_string(), serde_json::json!("{}"));
        map
    }
}

struct OSFingerprinter {}

impl Fingerprinter for OSFingerprinter {
    fn fingerprint(&self) -> HashMap<String, Value> {
        let mut map = HashMap::new();
        map.insert("data".to_string(), serde_json::json!({ "os": std::env::consts::OS}));
        map
    }
}

struct ArchFingerprinter {}

impl Fingerprinter for ArchFingerprinter {
    fn fingerprint(&self) -> HashMap<String, Value> {
        let mut map = HashMap::new();
        map.insert("data".to_string(), serde_json::json!({ "arch": std::env::consts::ARCH}));
        map
    }
}

#[cfg(test)]
mod tests {
    use crate::cmd::structs::{WorkerConfigFromManager, ServerConfig};

    use super::*;

    #[test]
    fn fingerprint_node_works() {
        let config = &WorkerConfig{
            server: ServerConfig{
                port: 1234,
                address: "duper".to_string(),
            },
            worker_config: WorkerConfigFromManager{
                additional_fingerprinters: vec![],
            }
        };
        let res = fingerprint_node(config);

        println!("whoa {:?}", res);

        assert_eq!(res["data"]["os"], "macos");
        assert_eq!(res["data"]["arch"], "aarch64");
    }
}