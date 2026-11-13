//! The replicated write path end to end on a real single-node raft engine
//! (raft-write-path plan, `internal_docs/plans/raft-write-path-plan.md`):
//!
//! - **group commit** (§6.2): concurrent writers share raft entries, under
//!   both topologies, and every write still reads back;
//! - **speculation** (§6.3): under `single` the leader's batches commit
//!   locally and resolve on apply, leaving no undo record behind;
//! - **purge barrier** (§6.1): the state DB is fsynced before every log
//!   purge;
//! - **replicated data migrations** (§6.5): a pending data step runs once,
//!   through the log, on restart.
//!
//! The telemetry counters are process-global and other modules in this
//! binary move them too, so assertions on them are lower bounds.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::KvValueType;

fn raft_config(root: &Path, name: &str, topology: &str) -> R<ManagerConfigFull> {
    let mut config = ManagerConfigFull {
        store_path: root
            .join("store")
            .to_str()
            .context("store path is not utf-8")?
            .to_string(),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    config.persistence.raft.topology = topology.to_string();
    config.persistence.raft.bootstrap = true;
    config.persistence.raft.node_id = name.to_string();
    config.persistence.raft.data_dir = root
        .join("raft")
        .to_str()
        .context("raft data dir is not utf-8")?
        .to_string();
    Ok(config)
}

fn state_db(root: &Path) -> PathBuf {
    root.join("store").join("store.sqlite")
}

fn read_only(path: &Path) -> R<rusqlite::Connection> {
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(conn)
}

fn wait_for(timeout: Duration, what: &str, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out waiting for {what}");
}

/// Concurrent writers on one group share raft entries: ops that queue
/// behind an in-flight entry commit together in the next one. Every write
/// reads back, and under `single` (which speculates) no undo record is
/// left once the writers return.
fn concurrent_writes_share_entries(topology: &str) -> R<()> {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 40;
    let dir = tempfile::tempdir()?;
    let store = Store::start_storage(&raft_config(dir.path(), "mgr-gc", topology)?)?;
    // Settle: the first writes after election pay one-time costs.
    for i in 0..10 {
        store.put_kv(
            &format!("gc/warm/{i}"),
            "w",
            KvValueType::String,
            None,
            None,
        )?;
    }
    let telemetry = legion::manager::telemetry::metrics();
    let matched_before = telemetry
        .raft_speculation_total
        .get(&["control", "matched"]);
    let entries_before = store.raft_applied_entries()?;

    let handles: Vec<_> = (0..THREADS)
        .map(|thread| {
            let store = store.clone();
            std::thread::spawn(move || -> R<()> {
                for i in 0..PER_THREAD {
                    store.put_kv(
                        &format!("gc/{thread}/{i}"),
                        &format!("{thread}-{i}"),
                        KvValueType::String,
                        None,
                        None,
                    )?;
                }
                Ok(())
            })
        })
        .collect();
    for handle in handles {
        handle
            .join()
            .map_err(|panic| anyhow::anyhow!("writer panicked: {panic:?}"))??;
    }

    let entries = store.raft_applied_entries()? - entries_before;
    let ops = (THREADS * PER_THREAD) as u64;
    assert!(
        entries < ops,
        "{ops} concurrent writes took {entries} raft entries: queued ops must share entries"
    );
    for thread in 0..THREADS {
        for i in 0..PER_THREAD {
            let value = store
                .get_kv(&format!("gc/{thread}/{i}"))?
                .context("every acknowledged write reads back")?
                .value;
            assert_eq!(value, format!("{thread}-{i}"));
        }
    }
    if topology == "single" {
        assert!(
            telemetry
                .raft_speculation_total
                .get(&["control", "matched"])
                > matched_before,
            "the single-topology leader must resolve its batches by matching, not re-applying"
        );
        let leftover: i64 = read_only(&state_db(dir.path()))?.query_row(
            "SELECT COUNT(*) FROM raft_speculative;",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            leftover, 0,
            "every acknowledged batch resolved its undo record"
        );
    }
    println!("raft_write_path {topology}: {ops} ops in {entries} entries");
    Ok(())
}

#[test]
fn concurrent_writes_share_entries_single() -> R<()> {
    concurrent_writes_share_entries("single")
}

#[test]
fn concurrent_writes_share_entries_lanes() -> R<()> {
    concurrent_writes_share_entries("lanes")
}

/// The purge barrier: with no log kept behind a snapshot, every snapshot
/// purges, and each purge first fsyncs the state DB.
#[test]
fn every_log_purge_fsyncs_the_state_db_first() -> R<()> {
    let dir = tempfile::tempdir()?;
    let mut config = raft_config(dir.path(), "mgr-purge", "single")?;
    config.persistence.raft.snapshot_entries = 20;
    config.persistence.raft.log_retain_bytes = 0;
    let telemetry = legion::manager::telemetry::metrics();
    let syncs_before = telemetry.raft_state_syncs_total.get(&["control", "ok"]);
    let store = Store::start_storage(&config)?;
    for i in 0..120 {
        store.put_kv(&format!("purge/{i}"), "v", KvValueType::String, None, None)?;
    }
    let log_db = dir.path().join("raft").join("control").join("log.sqlite");
    // openraft checks the snapshot policy only when an entry commits, and
    // drops the check while a snapshot is still building. On a loaded
    // runner one slow build can swallow every trigger from the writes
    // above, leaving the last snapshot below the purge point with nothing
    // left to commit. Keep committing while waiting so the policy is
    // checked again once the build finishes.
    let mut tick = 0;
    wait_for(
        Duration::from_secs(60),
        "the retention task to purge behind a snapshot",
        || {
            tick += 1;
            if store
                .put_kv(
                    &format!("purge/tick/{tick}"),
                    "v",
                    KvValueType::String,
                    None,
                    None,
                )
                .is_err()
            {
                return false;
            }
            let Ok(conn) = read_only(&log_db) else {
                return false;
            };
            let first: Option<i64> = conn
                .query_row("SELECT MIN(idx) FROM log;", [], |row| row.get(0))
                .ok()
                .flatten();
            first.is_some_and(|first| first > 60)
        },
    );
    assert!(
        telemetry.raft_state_syncs_total.get(&["control", "ok"]) > syncs_before,
        "a purge must be preceded by a state DB fsync"
    );
    // Everything is still there, and the store keeps working.
    assert_eq!(store.get_kv("purge/0")?.context("purge/0")?.value, "v");
    store.put_kv("purge/after", "ok", KvValueType::String, None, None)?;
    Ok(())
}

/// A data step pending on an existing database runs once, through the log,
/// when the cluster restarts on a build that has it: the rewrite and its
/// record land together, and nothing runs it again.
#[test]
fn a_pending_data_step_runs_once_through_the_log() -> R<()> {
    let dir = tempfile::tempdir()?;
    let config = raft_config(dir.path(), "mgr-data", "single")?;
    {
        let store = Store::start_storage(&config)?;
        store.put_kv("data/probe", "v", KvValueType::String, None, None)?;
    }
    // Stand in for a database written by an older build: a job without an
    // mtime, and no record of the backfill having run.
    {
        let conn = rusqlite::Connection::open(state_db(dir.path()))?;
        conn.execute(
            "INSERT INTO job (id, name, status, created_at)
             VALUES ('legacy', 'legacy', 'done', 1735689600);",
            [],
        )?;
        conn.execute(
            "DELETE FROM data_migration WHERE id = 'job-updated-at-backfill';",
            [],
        )?;
    }

    let store = Store::start_storage(&config)?;
    let db = state_db(dir.path());
    wait_for(
        Duration::from_secs(60),
        "the control-lane leader to run the pending data step",
        || {
            read_only(&db)
                .and_then(|conn| {
                    Ok(conn.query_row(
                        "SELECT COUNT(*) FROM data_migration WHERE id = 'job-updated-at-backfill';",
                        [],
                        |row| row.get::<_, i64>(0),
                    )?)
                })
                .is_ok_and(|count| count == 1)
        },
    );
    let updated_at: Option<i64> = read_only(&db)?.query_row(
        "SELECT updated_at FROM job WHERE id = 'legacy';",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(updated_at, Some(1_735_689_600));
    assert!(
        store.run_data_migrations()?.is_empty(),
        "a recorded step never runs again"
    );
    Ok(())
}
