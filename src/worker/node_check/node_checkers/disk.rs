use crate::prelude::*;
use crate::worker::node_check::{CheckStatus, NodeCheckReport, NodeChecker};
use sysinfo::Disks;

pub struct DiskChecker {}

/// Below this share of free space on any disk the check fails.
const FAIL_BELOW_FREE: f64 = 0.2;
/// Below this share it warns.
const WARN_BELOW_FREE: f64 = 0.5;

impl NodeChecker for DiskChecker {
    fn name(&self) -> &'static str {
        "disk"
    }

    fn health_check(&self) -> Option<NodeCheckReport> {
        let mut disks = Disks::new_with_refreshed_list();
        disks.refresh();
        // The fullest disk decides.
        let fullest = disks
            .list()
            .iter()
            .filter(|disk| disk.total_space() > 0)
            .map(|disk| {
                (
                    disk.available_space() as f64 / disk.total_space() as f64,
                    disk.mount_point().display().to_string(),
                )
            })
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        Some(disk_report(fullest))
    }
}

/// The report for the fullest disk (`(share free, mount point)`), or a
/// pass when no disk could be read.
fn disk_report(fullest: Option<(f64, String)>) -> NodeCheckReport {
    let Some((free, mount)) = fullest else {
        return NodeCheckReport::pass("no disks reported");
    };
    let percent = (free * 100.0).round();
    let status = if free < FAIL_BELOW_FREE {
        CheckStatus::Fail
    } else if free < WARN_BELOW_FREE {
        CheckStatus::Warn
    } else {
        CheckStatus::Pass
    };
    let mut attributes = Map::new();
    attributes.insert("lowestFreePercent".into(), json!(percent));
    attributes.insert("mountPoint".into(), json!(mount));
    NodeCheckReport {
        status,
        message: format!("{mount} has {percent}% free"),
        attributes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_fullest_disk_decides_the_status_test() {
        assert_eq!(
            disk_report(Some((0.9, "/".into()))).status,
            CheckStatus::Pass
        );
        assert_eq!(
            disk_report(Some((0.3, "/data".into()))).status,
            CheckStatus::Warn
        );
        let full = disk_report(Some((0.05, "/var".into())));
        assert_eq!(full.status, CheckStatus::Fail);
        assert_eq!(full.message, "/var has 5% free");
        assert_eq!(full.attributes["lowestFreePercent"], json!(5.0));
        assert_eq!(disk_report(None).status, CheckStatus::Pass);
    }
}
