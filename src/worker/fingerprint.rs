use crate::util::R;

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

pub fn fingerprint_node(additional_fingerprinters: Vec<String>) -> Map<String, Value> {
    let fps: Vec<Box<dyn Fingerprinter>> = vec![
        Box::new(ArchFingerprinter {}),
        Box::new(OSFingerprinter {}),
        Box::new(KitchenSinkFingerprinter {}),
    ];

    let mut ret = Map::new();

    fps.iter().for_each(|f| {
        ret.extend(f.fingerprint());
    });

    additional_fingerprinters
        .iter()
        .map(|name| fp_plugin_name_to_path(name))
        .for_each(|path| match call_dynamic(&path) {
            Ok(fp) => {
                ret.extend(fp.fingerprint());
            }
            Err(e) => {
                println!("Error loading fingerprinter: {}", e);
            }
        });

    ret
}

fn fp_plugin_name_to_path(name: &String) -> String {
    format!("/Users/mike/Code/legion/plugins/fingerprinters/{}/target/release/libexample.dylib", name)
}

fn call_dynamic(path: &str) -> R<Box<dyn Fingerprinter>> {
    unsafe {
        let lib = Library::new(path)?;
        let func: Symbol<unsafe extern "C" fn() -> Box<dyn Fingerprinter>> = lib.get(b"get")?;
        Ok(func())
    }
}

pub trait Fingerprinter {
    fn name(&self) -> &'static str;

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("foo".into(), Value::String("bar".into()));
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
        map.insert("os".into(), Value::String(OS.into()));
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
        map.insert("arch".into(), Value::String(ARCH.to_string()));
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
                "name".into(),
                Value::String(interface_name.to_string()),
            );

            network_map.insert(
                "mac_address".into(),
                Value::String(data.mac_address().to_string()),
            );

            networks_map.insert(interface_name.into(), Value::Object(network_map));
        }

        fingerprint.insert("networks".into(), Value::Object(networks_map));

        // === CPUS ===

        let mut cpus_hash: Map<String, Value> = Map::new();
        sys.cpus().iter().for_each(|cpu| {
            let mut cpu_hash: Map<String, Value> = Map::new();
            cpu_hash.insert("name".into(), Value::String(cpu.name().to_string()));
            cpu_hash.insert(
                "usage".into(),
                Value::String(cpu.cpu_usage().to_string()),
            );
            cpu_hash.insert(
                "frequency".into(),
                Value::String(cpu.frequency().to_string()),
            );
            cpu_hash.insert(
                "vendor_id".into(),
                Value::String(cpu.vendor_id().to_string()),
            );
            cpus_hash.insert(cpu.name().into(), Value::Object(cpu_hash));
        });

        fingerprint.insert("cpus".into(), Value::Object(cpus_hash));

        // === Disks ===

        let mut disks_hash: Map<String, Value> = Map::new();
        sys.disks().iter().for_each(|disk| {
            let mut disk_hash: Map<String, Value> = Map::new();
            disk_hash.insert(
                "disk_name".into(),
                Value::String(disk.name().to_str().unwrap_or("").to_string()),
            );
            // disk_hash.insert(
            //     "disk_file_system".into(),
            //     disk.file_system(),
            // );
            disk_hash.insert(
                "disk_mount_point".into(),
                Value::String(disk.mount_point().to_str().unwrap_or("").to_string()),
            );

            fingerprint.insert(
                "disk_total_space".into(),
                Value::String(disk.total_space().to_string()),
            );
            fingerprint.insert(
                "disk_available_space".into(),
                Value::String(disk.available_space().to_string()),
            );
            fingerprint.insert(
                "disk_is_removable".into(),
                Value::String(disk.is_removable().to_string()),
            );

            disks_hash.insert(
                disk.name().to_str().unwrap_or("").into(),
                Value::Object(disk_hash)
            );
        });

        fingerprint.insert("disks".into(), Value::Object(disks_hash));

        // === Memory ===

        fingerprint.insert(
            "memory_total".into(),
            Value::String(sys.total_memory().to_string()),
        );
        fingerprint.insert(
            "memory_free".into(),
            Value::String(sys.free_memory().to_string()),
        );
        fingerprint.insert(
            "memory_used".into(),
            Value::String(sys.used_memory().to_string()),
        );
        fingerprint.insert(
            "memory_swap_total".into(),
            Value::String(sys.total_swap().to_string()),
        );
        fingerprint.insert(
            "memory_swap_free".into(),
            Value::String(sys.free_swap().to_string()),
        );

        // === OTHER ===

        let signals = System::SUPPORTED_SIGNALS;
        fingerprint.insert(
            "signals".into(),
            Value::Array(signals.iter().map(|s| Value::String(s.to_string())).collect()),
        );

        fingerprint
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_node_works() -> R<()> {
        let fps =vec!["example".into()];
        let res = fingerprint_node(fps);

        // res.values().for_each(|v| {
        //     println!("v: {:?}", v.to_string());
        // });

        let os = res.get("os").ok_or("no os")?.as_str().ok_or("no os")?;
        assert_eq!("macos", os);


        // Test that the plugin works
        let basic = res.get("basic").ok_or("no basic")?.as_str().ok_or("no basic")?;
        assert_eq!("output", basic);

        Ok(())
    }
}
