use crate::prelude::*;

use libloading::{Library, Symbol};
use std::env::consts::{ARCH, OS};
use sysinfo::{Disks, Networks, System, SUPPORTED_SIGNALS};

// NOTE: Improve current fingerprinters:
// Network - speed, IP addresses, etc

// NOTE: Potential additional fingerprinters:
// CNI - which plugins are on the machine
// Landlock - is it available?
// Cloud provider fingerprinters?

// Helpful links for reference later:
// https://michael-f-bryan.github.io/rust-ffi-guide/dynamic_loading.html
// https://crates.io/crates/sysinfo

#[instrument(skip_all)]
pub fn fingerprint_node(additional_node_checks: Vec<String>) -> Map<String, Value> {
    let fps: Vec<Box<dyn NodeChecker>> = vec![
        Box::new(ArchNodeChecker {}),
        Box::new(OSNodeChecker {}),
        Box::new(KitchenSinkNodeChecker {}),
    ];

    let mut ret = Map::new();

    fps.iter().filter(|f| f.has_fingerprinter()).for_each(|f| {
        ret.extend(f.fingerprint());
    });

    additional_node_checks
        .iter()
        .map(fp_plugin_name_to_path)
        .for_each(|path| match call_dynamic(&path) {
            Ok(fp) => {
                ret.extend(fp.fingerprint());
            }
            Err(e) => {
                warn!("Error loading fingerprinter at path {path}, {e}");
            }
        });

    ret
}

#[instrument(skip_all)]
pub fn run_health_checks(additional_node_checks: Vec<String>) -> Map<String, Value> {
    let fps: Vec<Box<dyn NodeChecker>> = vec![Box::new(DiskChecker {})];

    let mut ret = Map::new();

    fps.iter().filter(|f| f.has_health_check()).for_each(|f| {
        ret.extend(f.health_check());
    });

    additional_node_checks
        .iter()
        .map(fp_plugin_name_to_path)
        .for_each(|path| match call_dynamic(&path) {
            Ok(fp) => {
                ret.extend(fp.health_check());
            }
            Err(e) => {
                error!("Error loading fingerprinter: {e}");
            }
        });

    ret
}

fn fp_plugin_name_to_path(name: &String) -> String {
    format!(
        "/Users/mike/Code/legion/plugins/node_checks/{}/target/release/libexample.dylib",
        name
    )
}

#[instrument(err, skip_all)]
fn call_dynamic(path: &str) -> R<Box<dyn NodeChecker>> {
    unsafe {
        let lib = Library::new(path)?;
        let func: Symbol<unsafe extern "C" fn() -> Box<dyn NodeChecker>> = lib.get(b"get")?;
        Ok(func())
    }
}

pub trait NodeChecker {
    fn name(&self) -> &'static str;

    fn has_fingerprinter(&self) -> bool {
        false
    }

    fn has_health_check(&self) -> bool {
        false
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("foo".into(), Value::String("bar".into()));
        map
    }

    fn health_check(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("alive".into(), Value::String("pass".into()));
        map
    }
}

struct OSNodeChecker {}

impl NodeChecker for OSNodeChecker {
    fn name(&self) -> &'static str {
        "os"
    }

    fn has_fingerprinter(&self) -> bool {
        true
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("os".into(), Value::String(OS.into()));
        map
    }
}

struct ArchNodeChecker {}

impl NodeChecker for ArchNodeChecker {
    fn name(&self) -> &'static str {
        "arch"
    }

    fn has_fingerprinter(&self) -> bool {
        true
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("arch".into(), Value::String(ARCH.to_string()));
        map
    }
}

struct DiskChecker {}
impl NodeChecker for DiskChecker {
    fn name(&self) -> &'static str {
        "disk"
    }

    fn has_health_check(&self) -> bool {
        true
    }

    fn health_check(&self) -> Map<String, Value> {
        let mut disks = Disks::new_with_refreshed_list();
        disks.refresh();

        let usage_below = |threshold: f64| {
            disks.list().iter().any(|disk| {
                let total = disk.total_space();
                if total == 0 {
                    return false;
                }
                let percent_free = disk.available_space() as f64 / total as f64;
                percent_free < threshold
            })
        };

        let any_disk_under_50_free = usage_below(0.5);
        let any_disk_under_20_free = usage_below(0.2);

        // TODO: Ensure this works on MacOS
        let fifty_val = if any_disk_under_50_free {
            "fail"
        } else {
            "pass"
        };
        let twenty_val = if any_disk_under_20_free {
            "fail"
        } else {
            "pass"
        };

        let mut map = Map::new();
        map.insert(
            "disk_under_fifty_free".into(),
            Value::String(fifty_val.into()),
        );
        map.insert(
            "disk_under_twenty_free".into(),
            Value::String(twenty_val.into()),
        );
        map
    }
}

struct KitchenSinkNodeChecker {}

impl NodeChecker for KitchenSinkNodeChecker {
    fn name(&self) -> &'static str {
        "kitchen-sink"
    }

    fn has_fingerprinter(&self) -> bool {
        true
    }

    fn fingerprint(&self) -> Map<String, Value> {
        let mut sys = System::new_all();
        sys.refresh_all();

        let mut fingerprint = Map::new();

        // === NETWORKS ===

        let mut networks_map: Map<String, Value> = Map::new();
        // Network interfaces name, data received and data transmitted:
        let mut networks = Networks::new_with_refreshed_list();
        networks.refresh();

        for (interface_name, data) in &networks {
            let mut network_map: Map<String, Value> = Map::new();

            network_map.insert("name".into(), Value::String(interface_name.to_string()));

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
            cpu_hash.insert("usage".into(), Value::String(cpu.cpu_usage().to_string()));
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
        let mut disks = Disks::new_with_refreshed_list();
        disks.refresh();

        disks.list().iter().for_each(|disk| {
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
                Value::Object(disk_hash),
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

        let signals = SUPPORTED_SIGNALS;
        fingerprint.insert(
            "signals".into(),
            Value::Array(
                signals
                    .iter()
                    .map(|s| Value::String(s.to_string()))
                    .collect(),
            ),
        );

        fingerprint
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_node_works() -> R<()> {
        let fps = vec!["example".into()];
        let res = fingerprint_node(fps);

        let os = res.get("os").ok_or("no os")?.as_str().ok_or("no os")?;
        assert_eq!("macos", os);

        // Test that the plugin works
        let basic = res
            .get("basic")
            .ok_or("no basic")?
            .as_str()
            .ok_or("no basic")?;
        assert_eq!("output", basic);

        Ok(())
    }
}
