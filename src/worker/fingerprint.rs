use crate::cmd::structs::WorkerConfig;
// use log::error;
use std::error;
use std::collections::HashMap;

extern crate libloading;
use std::env::consts::{ARCH, OS};
use libloading::{Library, Symbol};
use sysinfo::{DiskExt, ComponentExt, System, SystemExt, CpuExt};

// Helpful links for reference later:
// https://michael-f-bryan.github.io/rust-ffi-guide/dynamic_loading.html
// https://crates.io/crates/sysinfo

pub fn fingerprint_node(config: &WorkerConfig) -> HashMap<String, String> {
    let fps : Vec<Box<dyn Fingerprinter>> = vec![
        Box::new(ArchFingerprinter{}),
        Box::new(OSFingerprinter{}),
        Box::new(KitchenSinkFingerprinter{}),
    ];

    let mut ret = HashMap::new();

    fps.iter().for_each( |f| {
        ret.extend(f.fingerprint());
    });

    config.worker_config.additional_fingerprinters.iter().for_each(|path| {
        match call_dynamic(path) {
            Ok(fp) => {
                ret.extend(fp.fingerprint());
            },
            Err(e) => {
                println!("Error loading fingerprinter: {}", e);
            }
        }
    });

    ret
}

fn call_dynamic(path: &str) -> Result<Box<dyn Fingerprinter>, Box<dyn error::Error>> {
    unsafe {
        let lib = Library::new(path)?;
        let func: Symbol<unsafe extern fn() -> Box<dyn Fingerprinter>> = lib.get(b"get")?;
        Ok(func())
    }
}

trait Fingerprinter {
    fn name(&self) -> &'static str;

    fn fingerprint(&self) -> HashMap<String, String> {
        HashMap::from([("foo".to_string(), "bar".to_string())])
    }
}

struct OSFingerprinter {}

impl Fingerprinter for OSFingerprinter {
    fn name(&self) -> &'static str { "os" }

    fn fingerprint(&self) -> HashMap<String, String> {
        HashMap::from([
            ("os".to_string(), OS.to_string())
        ])
    }
}

struct ArchFingerprinter {}

impl Fingerprinter for ArchFingerprinter {
    fn name(&self) -> &'static str { "arch" }

    fn fingerprint(&self) -> HashMap<String, String> {
        HashMap::from([
            ("arch".to_string(), ARCH.to_string())
        ])
    }
}

struct KitchenSinkFingerprinter {}

impl Fingerprinter for KitchenSinkFingerprinter {
    fn name(&self) -> &'static str { "kitchen-sink" }

    fn fingerprint(&self) -> HashMap<String, String> {
        let mut sys = System::new_all();
        sys.refresh_all();

        let mut fingerprint = HashMap::new();

        // Network interfaces name, data received and data transmitted:
        for (interface_name, _data) in sys.networks() {
            fingerprint.insert("interface_name".to_string(), interface_name.to_string());
        }

        // Components:
        for component in sys.components() {
            fingerprint.insert("component_label".to_string(), component.label().to_string());
        }

        fingerprint.insert("supported_signals".to_string(), format!("{:?}", sysinfo::System::SUPPORTED_SIGNALS));

        fingerprint.insert("total_memory".to_string(), sys.total_memory().to_string());
        fingerprint.insert("available_memory".to_string(), sys.available_memory().to_string());
        fingerprint.insert("used_memory".to_string(), sys.used_memory().to_string());
        fingerprint.insert("total_swap".to_string(), sys.total_swap().to_string());
        fingerprint.insert("used_swap".to_string(), sys.used_swap().to_string());

        fingerprint.insert("system_name".to_string(), sys.name().unwrap_or("".to_string()));
        fingerprint.insert("system_kernel_version".to_string(), sys.kernel_version().unwrap_or("".to_string()));
        fingerprint.insert("system_os_version".to_string(), sys.os_version().unwrap_or("".to_string()));
        fingerprint.insert("system_host_name".to_string(), sys.host_name().unwrap_or("".to_string()));

        let mut cpus_hash: HashMap<String, HashMap<String, String>> = HashMap::new();
        sys.cpus().iter().for_each(|cpu| {
            let mut cpu_hash: HashMap<String, String> = HashMap::new();
            cpu_hash.insert("name".to_string(), cpu.name().to_string());
            cpu_hash.insert("usage".to_string(), cpu.cpu_usage().to_string());
            cpu_hash.insert("frequency".to_string(), cpu.frequency().to_string());
            cpu_hash.insert("vendor_id".to_string(), cpu.vendor_id().to_string());
            cpus_hash.insert(cpu.name().to_string(), cpu_hash);
        });
        fingerprint.insert("cpus".to_string(), cpus_hash);

        sys.disks().iter().for_each(|disk| {

            fingerprint.insert("disk_name".to_string(), disk.name().to_str().unwrap_or("").to_string());
            // fingerprint.insert("disk_file_system".to_string(), disk.file_system().to_str().unwrap_or("").to_string());
            fingerprint.insert("disk_mount_point".to_string(), disk.mount_point().to_str().unwrap_or("").to_string());
            fingerprint.insert("disk_total_space".to_string(), disk.total_space().to_string());
            fingerprint.insert("disk_available_space".to_string(), disk.available_space().to_string());
            fingerprint.insert("disk_is_removable".to_string(), disk.is_removable().to_string());
        });

        fingerprint
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
                port: 9876,
                address: "1.1.1.1".to_string(),
            },
            worker_config: WorkerConfigFromManager{
                additional_fingerprinters: vec!["/Users/mike/Code/legion/src/plugins/fingerprinters/example/target/release/libexample.dylib".to_string()],
            }
        };
        let res = fingerprint_node(config);

        println!("res: {:?}", res);

        assert_eq!(res.get("os"), Some(&"macos".to_string()));
        assert_eq!(res.get("arch"), Some(&"aarch64".to_string()));
        assert_eq!(res.get("basic"), Some(&"output".to_string()));
    }
}

// SOME OF THE FINGERPRINTERS TO ADD

// arch
    // arch
// storage
    // bytes total, bytes free, volumes
// signals
    // os.signals
// CNI
    // various CNI plugins available
// Network
    // speed, interfaces, IP addresses, etc
// CPU
    // cores, speed, etc
// Memory
    // total, free, etc
// Landlock
    // available
// CPU
    // cores, modelname, numcores, numcores.perf, numcores.efficiency, totalcompute, 
// Fingerprinters specific to cloud providers
