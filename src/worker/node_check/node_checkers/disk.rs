use crate::prelude::*;
use crate::worker::node_check::NodeChecker;
use sysinfo::Disks;

pub struct DiskChecker {}

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
