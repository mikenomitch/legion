//! Advertising the node content store to the manager (suspend plan §6.8,
//! amendment 2). The manager side and the protocol are documented in
//! [`crate::storage::content_index`].
//!
//! The worker keeps the set the manager last acknowledged and its epoch. Each
//! heartbeat diffs the store's current set against it and sends the
//! difference, so a failed heartbeat needs no restore step: nothing advances
//! until the manager acknowledges, and the next diff simply includes what the
//! lost one carried.

use std::collections::HashMap;

use crate::content_store::{ContentClass, ContentStore};
use crate::prelude::*;
use crate::structs::worker_protocol::MIN_ADVERTISED_BYTES;
use crate::structs::{ContentLocation, ContentLocationReport, SnapshotBudget};

/// What this node advertises right now: every store entry of at least
/// [`MIN_ADVERTISED_BYTES`], with its size. No store means an empty set.
pub fn advertised_set(store: Option<&ContentStore>) -> R<HashMap<String, u64>> {
    let Some(store) = store else {
        return Ok(HashMap::new());
    };
    Ok(store
        .sizes_at_least(MIN_ADVERTISED_BYTES)?
        .into_iter()
        .map(|(digest, size)| (digest.to_string(), size))
        .collect())
}

/// The store's `snapshots` budget class and the disk under it, for the
/// report's `snapshot_budget` (docs/suspend-and-resume.md, "Capacity for
/// resumes"). `None` without a store, or when the store cannot be read: the
/// manager then treats the node as having room rather than as full.
pub fn snapshot_budget(store: Option<&ContentStore>) -> Option<SnapshotBudget> {
    let store = store?;
    let usage = store.usage().ok()?;
    // Unpinned content of every class is what `minFreeDisk` eviction can
    // free, so the manager counts it as room on the disk.
    let reclaimable_bytes = usage
        .iter()
        .map(|class| class.used_bytes.saturating_sub(class.pinned_bytes))
        .sum();
    usage
        .into_iter()
        .find(|class| class.class == ContentClass::Snapshots)
        .map(|class| SnapshotBudget {
            budget_bytes: class.budget,
            used_bytes: class.used_bytes,
            pinned_bytes: class.pinned_bytes,
            disk_free_bytes: store.free_disk_bytes(),
            min_free_disk_bytes: store.config().min_free_disk,
            reclaimable_bytes,
        })
}

/// One report in flight, with the set it describes, so it can be
/// acknowledged without recomputing.
#[derive(Debug, Clone)]
pub struct PendingReport {
    pub report: ContentLocationReport,
    snapshot: HashMap<String, u64>,
}

/// The worker half of the epoch protocol.
#[derive(Debug, Clone)]
pub struct ContentAdvertiser {
    acked: HashMap<String, u64>,
    acked_epoch: u64,
    needs_full: bool,
}

impl Default for ContentAdvertiser {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentAdvertiser {
    /// A fresh worker process always opens with a full report: that is the
    /// "full set on registration".
    pub fn new() -> Self {
        Self {
            acked: HashMap::new(),
            acked_epoch: 0,
            needs_full: true,
        }
    }

    /// The report for this heartbeat, given the store's current set.
    pub fn prepare(&self, current: HashMap<String, u64>) -> PendingReport {
        let next = self.acked_epoch + 1;
        if self.needs_full {
            let mut added: Vec<ContentLocation> = current
                .iter()
                .map(|(digest, bytes)| ContentLocation {
                    digest: digest.clone(),
                    bytes: *bytes,
                })
                .collect();
            added.sort_by(|a, b| a.digest.cmp(&b.digest));
            return PendingReport {
                report: ContentLocationReport {
                    epoch: next,
                    base_epoch: 0,
                    full: true,
                    added,
                    removed: Vec::new(),
                    snapshot_budget: None,
                },
                snapshot: current,
            };
        }
        let mut added: Vec<ContentLocation> = current
            .iter()
            .filter(|(digest, bytes)| self.acked.get(*digest) != Some(*bytes))
            .map(|(digest, bytes)| ContentLocation {
                digest: digest.clone(),
                bytes: *bytes,
            })
            .collect();
        added.sort_by(|a, b| a.digest.cmp(&b.digest));
        let mut removed: Vec<String> = self
            .acked
            .keys()
            .filter(|digest| !current.contains_key(*digest))
            .cloned()
            .collect();
        removed.sort();
        // Unchanged: the probe that lets a manager with no state notice.
        let epoch = if added.is_empty() && removed.is_empty() {
            self.acked_epoch
        } else {
            next
        };
        PendingReport {
            report: ContentLocationReport {
                epoch,
                base_epoch: self.acked_epoch,
                full: false,
                added,
                removed,
                snapshot_budget: None,
            },
            snapshot: current,
        }
    }

    /// The manager answered. `resync` means it could not apply the report
    /// and wants a full one next.
    pub fn acknowledge(&mut self, pending: PendingReport, resync: bool) {
        if resync {
            self.needs_full = true;
            return;
        }
        self.acked = pending.snapshot;
        self.acked_epoch = pending.report.epoch;
        self.needs_full = false;
    }
}
