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

        // Network interfaces name, data received and data transmitted:
        println!("=> networks:");
        for (interface_name, data) in sys.networks() {
            println!("interface name: {:?}", interface_name);
        }

        // Components :
        for component in sys.components() {
            println!("label: {:?}", component.label());
        }

        println!("signals: {:?}:", sysinfo::System::SUPPORTED_SIGNALS);

        println!("=> system:");
        // RAM and swap information:
        println!("total memory: {} bytes", sys.total_memory());
        println!("available memory: {} bytes", sys.available_memory());
        println!("used memory : {} bytes", sys.used_memory());
        println!("total swap  : {} bytes", sys.total_swap());
        println!("used swap   : {} bytes", sys.used_swap());

        // Display system information:
        println!("System name:             {:?}", sys.name());
        println!("System kernel version:   {:?}", sys.kernel_version());
        println!("System OS version:       {:?}", sys.os_version());
        println!("System host name:        {:?}", sys.host_name());

        sys.cpus().iter().for_each(|cpu| {
            println!("name: {:?}", cpu.name());
            println!("cpu_usage: {:?}", cpu.cpu_usage());
            println!("frequency: {:?}", cpu.frequency());
            println!("vendor_id: {:?}", cpu.vendor_id());
            println!("brand: {:?}", cpu.brand());
        });

        sys.disks().iter().for_each(|disk| {
            println!("name: {:?}", disk.name());
            println!("file_system: {:?}", disk.file_system());
            println!("mount_point: {:?}", disk.mount_point());
            println!("total_space: {:?}", disk.total_space());
            println!("available_space: {:?}", disk.available_space());
            println!("is_removable: {:?}", disk.is_removable());
        });

        // Number of CPUs:
        println!("NB CPUs: {}", sys.cpus().len());

        HashMap::from([
            ("kernel".to_string(), sys.host_name().unwrap_or("".to_string())),
            ("host".to_string(), sys.kernel_version().unwrap_or("".to_string())),
        ])
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
