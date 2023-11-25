use crate::cmd::structs::WorkerConfig;
use std::error;

extern crate libloading;
use libloading::{Library, Symbol};
use std::env::consts::{ARCH, OS};
use sysinfo::{CpuExt, DiskExt, System, SystemExt, NetworkExt};
use serde_json::{Map, Value};

// NOTE: Improve current fingerprinters:
// Network - speed, IP addresses, etc

// NOTE: Potential additional fingerprinters:
// CNI - which plugins are on the machine
// Landlock - is it available?
// Cloud provider fingerprinters?

// Helpful links for reference later:
// https://michael-f-bryan.github.io/rust-ffi-guide/dynamic_loading.html
// https://crates.io/crates/sysinfo

pub fn fingerprint_node(config: &WorkerConfig) -> Map<String, Value> {
    let fps: Vec<Box<dyn Fingerprinter>> = vec![
        Box::new(ArchFingerprinter {}),
        Box::new(OSFingerprinter {}),
        Box::new(KitchenSinkFingerprinter {}),
    ];

    let mut ret = Map::new();

    fps.iter().for_each(|f| {
        ret.extend(f.fingerprint());
    });

    config
        .worker_config
        .additional_fingerprinters
        .iter()
        .for_each(|path| match call_dynamic(path) {
            Ok(fp) => {
                ret.extend(fp.fingerprint());
            }
            Err(e) => {
                println!("Error loading fingerprinter: {}", e);
            }
        });

    ret
}

fn call_dynamic(path: &str) -> Result<Box<dyn Fingerprinter>, Box<dyn error::Error>> {
    unsafe {
        let lib = Library::new(path)?;
        let func: Symbol<unsafe extern "C" fn() -> Box<dyn Fingerprinter>> = lib.get(b"get")?;
        Ok(func())
    }
}

trait Fingerprinter {
    fn name(&self) -> &'static str;

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("foo".to_string(), Value::String("bar".to_string()));
        map
    }
}

struct OSFingerprinter {}

impl Fingerprinter for OSFingerprinter {
    fn name(&self) -> &'static str {
        "os"
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("os".to_string(), Value::String(OS.to_string()));
        map
    }
}

struct ArchFingerprinter {}

impl Fingerprinter for ArchFingerprinter {
    fn name(&self) -> &'static str {
        "arch"
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("arch".to_string(), Value::String(ARCH.to_string()));
        map
    }
}

struct KitchenSinkFingerprinter {}

impl Fingerprinter for KitchenSinkFingerprinter {
    fn name(&self) -> &'static str {
        "kitchen-sink"
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut sys = System::new_all();
        sys.refresh_all();

        let mut fingerprint = Map::new();

        // === NETWORKS ===

        let mut networks_map: Map<String, Value> = Map::new();
        // Network interfaces name, data received and data transmitted:
        for (interface_name, data) in sys.networks() {
            let mut network_map: Map<String, Value> = Map::new();

            network_map.insert(
                "name".to_string(),
                Value::String(interface_name.to_string()),
            );

            network_map.insert(
                "mac_address".to_string(),
                Value::String(data.mac_address().to_string()),
            );

            networks_map.insert(interface_name.to_string(), Value::Object(network_map));
        }

        fingerprint.insert("networks".to_string(), Value::Object(networks_map));

        // === CPUS ===

        let mut cpus_hash: Map<String, Value> = Map::new();
        sys.cpus().iter().for_each(|cpu| {
            let mut cpu_hash: Map<String, Value> = Map::new();
            cpu_hash.insert("name".to_string(), Value::String(cpu.name().to_string()));
            cpu_hash.insert(
                "usage".to_string(),
                Value::String(cpu.cpu_usage().to_string()),
            );
            cpu_hash.insert(
                "frequency".to_string(),
                Value::String(cpu.frequency().to_string()),
            );
            cpu_hash.insert(
                "vendor_id".to_string(),
                Value::String(cpu.vendor_id().to_string()),
            );
            cpus_hash.insert(cpu.name().to_string(), Value::Object(cpu_hash));
        });

        fingerprint.insert("cpus".to_string(), Value::Object(cpus_hash));

        // === Disks ===

        let mut disks_hash: Map<String, Value> = Map::new();
        sys.disks().iter().for_each(|disk| {
            let mut disk_hash: Map<String, Value> = Map::new();
            disk_hash.insert(
                "disk_name".to_string(),
                Value::String(disk.name().to_str().unwrap_or("").to_string()),
            );
            // disk_hash.insert(
            //     "disk_file_system".to_string(),
            //     disk.file_system(),
            // );
            disk_hash.insert(
                "disk_mount_point".to_string(),
                Value::String(disk.mount_point().to_str().unwrap_or("").to_string()),
            );

            fingerprint.insert(
                "disk_total_space".to_string(),
                Value::String(disk.total_space().to_string()),
            );
            fingerprint.insert(
                "disk_available_space".to_string(),
                Value::String(disk.available_space().to_string()),
            );
            fingerprint.insert(
                "disk_is_removable".to_string(),
                Value::String(disk.is_removable().to_string()),
            );

            disks_hash.insert(
                disk.name().to_str().unwrap_or("").to_string(),
                Value::Object(disk_hash)
            );
        });

        fingerprint.insert("disks".to_string(), Value::Object(disks_hash));

        // === Memory ===

        fingerprint.insert(
            "memory_total".to_string(),
            Value::String(sys.total_memory().to_string()),
        );
        fingerprint.insert(
            "memory_free".to_string(),
            Value::String(sys.free_memory().to_string()),
        );
        fingerprint.insert(
            "memory_used".to_string(),
            Value::String(sys.used_memory().to_string()),
        );
        fingerprint.insert(
            "memory_swap_total".to_string(),
            Value::String(sys.total_swap().to_string()),
        );
        fingerprint.insert(
            "memory_swap_free".to_string(),
            Value::String(sys.free_swap().to_string()),
        );

        // === OTHER ===

        let signals = System::SUPPORTED_SIGNALS;
        fingerprint.insert(
            "signals".to_string(),
            Value::Array(signals.iter().map(|s| Value::String(s.to_string())).collect()),
        );

        fingerprint
    }
}

#[cfg(test)]
mod tests {
    use crate::cmd::structs::{ServerConfig, WorkerConfigFromManager};

    use super::*;

    #[test]
    fn fingerprint_node_works() {
        let config = &WorkerConfig{
            server: ServerConfig{
                port: 9876,
                insecure_port: 9999,
                address: "1.1.1.1".to_string(),
            },
            worker_config: WorkerConfigFromManager{
                additional_fingerprinters: vec!["/Users/mike/Code/legion/src/plugins/fingerprinters/example/target/release/libexample.dylib".to_string()],
            }
        };

        let res = fingerprint_node(config);
        res.values().for_each(|v| {
            println!("v: {:?}", v.to_string());
        });

        let os = res.get("os").unwrap().as_str().unwrap();
        assert_eq!("macos", os);


        // // Test that the plugin works
        // let basic = res.get("basic").unwrap().as_str().unwrap();
        // assert_eq!("output", basic);
    }
}
