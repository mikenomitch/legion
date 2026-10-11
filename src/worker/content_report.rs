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
use crate::storage::content_index::MIN_ADVERTISED_BYTES;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::content_index::{ContentLocationIndex, IngestOutcome};
    use std::time::Instant;

    const MIB: u64 = 1024 * 1024;

    fn set(items: &[(&str, u64)]) -> HashMap<String, u64> {
        items.iter().map(|(d, b)| (d.to_string(), *b)).collect()
    }

    /// Drive one heartbeat end to end: the worker prepares, the manager
    /// ingests, the worker acknowledges (or the beat is lost).
    fn beat(
        worker: &mut ContentAdvertiser,
        manager: &ContentLocationIndex,
        current: HashMap<String, u64>,
        delivered: bool,
    ) -> Option<IngestOutcome> {
        let pending = worker.prepare(current);
        if !delivered {
            return None;
        }
        let outcome = manager.ingest("n1", &pending.report, Instant::now());
        worker.acknowledge(pending, outcome == IngestOutcome::Resync);
        Some(outcome)
    }

    #[test]
    fn the_snapshot_budget_reports_the_snapshots_class_and_what_is_pinned() -> R<()> {
        use crate::content_store::{ContentBudgets, ContentStoreConfig, PinOwner, PutOptions};
        assert_eq!(snapshot_budget(None), None, "no store, no budget");
        let dir = tempfile::tempdir()?;
        let store = ContentStore::open(
            dir.path().join("content"),
            ContentStoreConfig {
                budgets: ContentBudgets {
                    artifacts: 10 * MIB,
                    snapshots: 8 * MIB,
                },
                ttl: None,
                min_free_disk: 3 * MIB,
            },
        )?;
        store.put_bytes(
            ContentClass::Snapshots,
            &vec![1u8; MIB as usize],
            PutOptions::default().pin(PinOwner::snapshot("s1")),
        )?;
        store.put_bytes(
            ContentClass::Snapshots,
            &vec![2u8; MIB as usize],
            PutOptions::default(),
        )?;
        store.put_bytes(
            ContentClass::Artifacts,
            &vec![3u8; MIB as usize],
            PutOptions::default(),
        )?;
        let budget = snapshot_budget(Some(&store)).expect("a budget");
        assert_eq!(
            budget,
            SnapshotBudget {
                budget_bytes: 8 * MIB,
                used_bytes: 2 * MIB,
                pinned_bytes: MIB,
                // The real disk's free space, read alongside.
                disk_free_bytes: budget.disk_free_bytes,
                min_free_disk_bytes: 3 * MIB,
                // The unpinned snapshot part and the artifact.
                reclaimable_bytes: 2 * MIB,
            }
        );
        #[cfg(unix)]
        assert!(budget.disk_free_bytes.is_some_and(|free| free > 0));
        Ok(())
    }

    #[test]
    fn deltas_track_the_store_and_survive_lost_heartbeats() {
        let manager = ContentLocationIndex::new();
        let mut worker = ContentAdvertiser::new();

        // Registration: the first report is full.
        let first = worker.prepare(set(&[("a", 2 * MIB)]));
        assert!(first.report.full);
        assert_eq!(
            beat(&mut worker, &manager, set(&[("a", 2 * MIB)]), true),
            Some(IngestOutcome::Replaced)
        );

        // Steady state: a probe, no entries.
        let probe = worker.prepare(set(&[("a", 2 * MIB)]));
        assert!(!probe.report.full);
        assert!(probe.report.added.is_empty() && probe.report.removed.is_empty());
        assert_eq!(probe.report.epoch, probe.report.base_epoch);

        // A change whose heartbeat is lost: the next diff still carries it.
        assert_eq!(
            beat(
                &mut worker,
                &manager,
                set(&[("a", 2 * MIB), ("b", 3 * MIB)]),
                false
            ),
            None
        );
        let retry = worker.prepare(set(&[("b", 3 * MIB)]));
        assert_eq!(retry.report.added.len(), 1);
        assert_eq!(retry.report.removed, vec!["a".to_string()]);
        assert_eq!(
            beat(&mut worker, &manager, set(&[("b", 3 * MIB)]), true),
            Some(IngestOutcome::Applied)
        );
        assert_eq!(manager.bytes_on("a", "n1"), None);
        assert_eq!(manager.bytes_on("b", "n1"), Some(3 * MIB));
    }

    #[test]
    fn a_manager_restart_triggers_a_full_resync() {
        let manager = ContentLocationIndex::new();
        let mut worker = ContentAdvertiser::new();
        beat(&mut worker, &manager, set(&[("a", 2 * MIB)]), true);
        beat(
            &mut worker,
            &manager,
            set(&[("a", 2 * MIB), ("b", 2 * MIB)]),
            true,
        );
        assert_eq!(manager.bytes_on("b", "n1"), Some(2 * MIB));

        // Failover: the new leader holds nothing.
        manager.clear();
        assert_eq!(
            beat(
                &mut worker,
                &manager,
                set(&[("a", 2 * MIB), ("b", 2 * MIB)]),
                true
            ),
            Some(IngestOutcome::Resync)
        );
        assert_eq!(
            manager.bytes_on("a", "n1"),
            None,
            "scores 0 until it reports"
        );
        assert_eq!(
            beat(
                &mut worker,
                &manager,
                set(&[("a", 2 * MIB), ("b", 2 * MIB)]),
                true
            ),
            Some(IngestOutcome::Replaced)
        );
        assert_eq!(manager.bytes_on("a", "n1"), Some(2 * MIB));
        assert_eq!(manager.bytes_on("b", "n1"), Some(2 * MIB));
    }

    #[test]
    fn a_lost_reply_after_the_manager_applied_resyncs() {
        let manager = ContentLocationIndex::new();
        let mut worker = ContentAdvertiser::new();
        beat(&mut worker, &manager, set(&[("a", 2 * MIB)]), true);
        // The manager applies epoch 2 but the reply never arrives.
        let pending = worker.prepare(set(&[("a", 2 * MIB), ("b", 2 * MIB)]));
        assert_eq!(
            manager.ingest("n1", &pending.report, Instant::now()),
            IngestOutcome::Applied
        );
        // The worker re-sends 1 -> 2 with a different change; the manager is
        // at 2, so the bases disagree and it asks for a full report.
        assert_eq!(
            beat(&mut worker, &manager, set(&[("c", 2 * MIB)]), true),
            Some(IngestOutcome::Resync)
        );
        beat(&mut worker, &manager, set(&[("c", 2 * MIB)]), true);
        assert_eq!(manager.bytes_on("b", "n1"), None);
        assert_eq!(manager.bytes_on("c", "n1"), Some(2 * MIB));
    }

    #[test]
    fn the_store_advertises_only_large_entries() -> R<()> {
        use crate::content_store::{ContentClass, ContentStoreConfig, PutOptions};
        let dir = tempfile::tempdir()?;
        let store = ContentStore::open(
            dir.path(),
            ContentStoreConfig {
                min_free_disk: 0,
                ..Default::default()
            },
        )?;
        let big = store.put_bytes(
            ContentClass::Artifacts,
            &vec![7u8; MIB as usize],
            PutOptions::default(),
        )?;
        store.put_bytes(ContentClass::Artifacts, b"small", PutOptions::default())?;
        let advertised = advertised_set(Some(&store))?;
        assert_eq!(advertised.len(), 1);
        assert_eq!(advertised.get(big.digest.as_str()), Some(&MIB));
        assert!(advertised_set(None)?.is_empty());
        Ok(())
    }
}
