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

use crate::content_store::ContentStore;
use crate::prelude::*;
use crate::storage::content_index::MIN_ADVERTISED_BYTES;
use crate::structs::{ContentLocation, ContentLocationReport};

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
