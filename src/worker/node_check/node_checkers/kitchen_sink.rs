use crate::prelude::*;
use crate::worker::node_check::NodeChecker;
use crate::worker::state::WorkerStateStore;
use std::path::{Path, PathBuf};
use sysinfo::{Disks, Networks, System, SUPPORTED_SIGNALS};

pub struct KitchenSinkNodeChecker {}

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
            disks_hash.insert(
                disk.name().to_str().unwrap_or("").into(),
                Value::Object(disk_hash),
            );
        });

        // The top-level `disk_*` keys are what the scheduler's disk-fit check
        // reads, so they describe the disk holding the worker's state dir,
        // where allocations and `local-dir` volumes live. They used to come
        // from whichever disk was listed last (`/boot/efi` on some hosts).
        let mounts: Vec<&Path> = disks.list().iter().map(|disk| disk.mount_point()).collect();
        let anchor = existing_ancestor(&worker_disk_anchor());
        let chosen = disk_holding(&mounts, &anchor).or(mounts.len().checked_sub(1));
        if let Some(disk) = chosen.map(|index| &disks.list()[index]) {
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
        }

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

/// The directory whose disk the top-level `disk_*` fingerprint keys describe:
/// the worker state file's directory, which also holds `allocs/` and
/// `volumes/`.
fn worker_disk_anchor() -> PathBuf {
    let state = WorkerStateStore::default_path();
    let dir = state.parent().map(Path::to_path_buf).unwrap_or(state);
    std::path::absolute(&dir).unwrap_or(dir)
}

/// `path`, or its nearest ancestor that exists, with symlinks resolved, so a
/// state dir that is not created yet still finds the disk it will land on.
fn existing_ancestor(path: &Path) -> PathBuf {
    path.ancestors()
        .find_map(|dir| dir.canonicalize().ok())
        .unwrap_or_else(|| path.to_path_buf())
}

/// Index of the mount point that holds `path`: the longest one that is a
/// prefix of it, by whole path components.
fn disk_holding(mounts: &[&Path], path: &Path) -> Option<usize> {
    mounts
        .iter()
        .enumerate()
        .filter(|(_, mount)| path.starts_with(mount))
        .max_by_key(|(_, mount)| mount.components().count())
        .map(|(index, _)| index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_disk_fingerprint_follows_the_state_dir_not_the_last_disk_test() {
        let mounts = [
            Path::new("/"),
            Path::new("/var/lib/legion"),
            Path::new("/var/lib/legion-other"),
            Path::new("/boot/efi"),
        ];
        assert_eq!(
            disk_holding(&mounts, Path::new("/var/lib/legion/workers/default")),
            Some(1)
        );
        assert_eq!(
            disk_holding(&mounts, Path::new("/home/legion/.legion")),
            Some(0)
        );
        // A prefix by characters is not a prefix by components.
        assert_eq!(
            disk_holding(&mounts, Path::new("/var/lib/legion2")),
            Some(0)
        );
        assert_eq!(
            disk_holding(&[Path::new("/boot/efi")], Path::new("/srv")),
            None
        );
    }

    #[test]
    fn a_missing_state_dir_resolves_to_its_nearest_existing_ancestor_test() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("workers/default");
        assert_eq!(
            existing_ancestor(&missing),
            dir.path().canonicalize().expect("canonical")
        );
    }
}
