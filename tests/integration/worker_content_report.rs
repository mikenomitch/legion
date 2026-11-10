//! The worker's content reports against the manager's content index: deltas,
//! resyncs, and what is advertised (docs/artifacts.md).

use legion::content_store::{ContentClass, ContentStore};
use legion::prelude::*;
use legion::storage::content_index::{ContentLocationIndex, IngestOutcome};
use legion::structs::SnapshotBudget;
use legion::worker::content_report::*;
use std::collections::HashMap;
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
    use legion::content_store::{ContentBudgets, ContentStoreConfig, PinOwner, PutOptions};
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
    use legion::content_store::{ContentClass, ContentStoreConfig, PutOptions};
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
