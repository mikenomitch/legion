//! SQLite-backed raft log store (phase A-1 of
//! `docs/plans/durability-and-ops-execution-plan.md` §2, deliverable 1).
//!
//! Each lane owns a small dedicated database at
//! `<data_dir>/<lane>/log.sqlite`, entirely separate from the state DB:
//!
//! - `log(idx INTEGER PRIMARY KEY, payload BLOB)` — one row per raft log
//!   entry, keyed by the entry's log index; the payload is the serde_json
//!   encoding of `Entry<TypeConfig>` (the same encoding the transport uses
//!   on the wire, see `network.rs`).
//! - `meta(key TEXT PRIMARY KEY, value BLOB)` — vote, committed log id, and
//!   last purged log id, each a serde_json blob of the openraft type.
//!
//! # Durability
//!
//! The connection runs `journal_mode = WAL` with **`synchronous = FULL`**:
//! a raft log entry (and a vote) must be fsynced to disk before it is
//! acknowledged — acking an unsynced entry would let a crashed node deny a
//! vote it granted or drop log entries it confirmed, which breaks raft's
//! core safety argument. `FULL` makes every commit fsync the WAL, so when
//! `append`'s `LogFlushed` callback fires (and when `save_vote` returns)
//! the bytes are on disk. This is deliberately stricter than the state DB's
//! `synchronous = NORMAL` (state is recoverable from the log; the log is
//! recoverable from nothing).
//!
//! # Sync-inside-async
//!
//! openraft calls these async trait methods from its core loop, and the
//! implementation does the rusqlite work inline behind a mutex instead of
//! `spawn_blocking`. That is acceptable here, and intentional: this DB is
//! dedicated to the lane's log (no contention with the state DB or any
//! other lane), every operation touches a handful of tiny rows, and the
//! fsync that dominates append latency is work the raft core *must* wait
//! for before acking anyway. Bouncing each call through a threadpool would
//! add latency and reordering hazards (openraft requires all write IO to be
//! serialized) for no throughput gain.

// `openraft::StorageError` is large; the trait signatures are fixed by
// openraft, so (like the gRPC surface with `tonic::Status`) the big Err
// variant cannot be boxed away here.
#![allow(clippy::result_large_err)]

use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::MutexGuard;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, RaftLogReader, StorageError, StorageIOError, Vote};
use rusqlite::{params, Connection, OptionalExtension};

use crate::prelude::*;
use crate::storage::raft::types::TypeConfig;
use crate::storage::raft::Lane;

type NodeId = u64;

/// SQLite integers are i64; clamp a raft log index for use as a bound
/// parameter. Real log indexes never approach i64::MAX — only the synthetic
/// `u64::MAX` upper bound from an unbounded range hits the clamp.
fn sql_idx(index: u64) -> i64 {
    i64::try_from(index).unwrap_or(i64::MAX)
}

const META_VOTE: &str = "vote";
const META_COMMITTED: &str = "committed";
const META_PURGED: &str = "purged";

/// The per-lane raft log store. Cheap to clone (the driver hands one clone
/// to openraft as the log store and `get_log_reader` hands out more); all
/// clones share the same connection, and the mutex serializes every
/// operation — openraft's "all write-IO must be serialized" requirement
/// holds trivially.
#[derive(Clone)]
pub(crate) struct RaftLogStore {
    conn: Arc<Mutex<Connection>>,
}

impl Debug for RaftLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftLogStore").finish_non_exhaustive()
    }
}

impl RaftLogStore {
    /// Opens (creating if needed) the lane's log database at
    /// `<data_dir>/<lane>/log.sqlite`.
    pub(crate) fn open(data_dir: &Path, lane: Lane) -> R<Self> {
        let dir = data_dir.join(lane.wire_name());
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed creating raft lane directory {}", dir.display()))?;
        let path = dir.join("log.sqlite");
        let conn = Connection::open(&path)
            .with_context(|| format!("failed opening raft log db {}", path.display()))?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        // FULL, not NORMAL: every commit fsyncs, so an acked log entry or
        // vote survives power loss. See the module docs.
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS log (
                idx INTEGER PRIMARY KEY,
                payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );",
        )
        .context("failed creating raft log tables")?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        // A poisoned mutex means another thread panicked mid-operation; the
        // connection itself stays transactionally consistent (an open tx
        // rolls back on drop), so continuing is safe.
        self.conn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn read_meta<T: serde::de::DeserializeOwned>(
        conn: &Connection,
        key: &str,
    ) -> Result<Option<T>, rusqlite::Error> {
        let blob: Option<Vec<u8>> = conn
            .query_row("SELECT value FROM meta WHERE key = ?1;", [key], |row| {
                row.get(0)
            })
            .optional()?;
        match blob {
            None => Ok(None),
            Some(bytes) => {
                let value = serde_json::from_slice(&bytes).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Blob,
                        Box::new(e),
                    )
                })?;
                Ok(Some(value))
            }
        }
    }

    fn write_meta<T: serde::Serialize>(
        conn: &Connection,
        key: &str,
        value: &T,
    ) -> Result<(), rusqlite::Error> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value;",
            params![key, bytes],
        )?;
        Ok(())
    }

    /// The log id of the last present entry, if any.
    fn last_log_id(conn: &Connection) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let payload: Option<Vec<u8>> = conn
            .query_row(
                "SELECT payload FROM log ORDER BY idx DESC LIMIT 1;",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| StorageIOError::read_logs(&e))?;
        match payload {
            None => Ok(None),
            Some(bytes) => {
                let entry: Entry<TypeConfig> =
                    serde_json::from_slice(&bytes).map_err(|e| StorageIOError::read_logs(&e))?;
                Ok(Some(entry.log_id))
            }
        }
    }
}

impl RaftLogReader<TypeConfig> for RaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let start = match range.start_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => x.saturating_add(1),
            Bound::Unbounded => 0,
        };
        // End is exclusive; a log index of u64::MAX is unreachable in
        // practice, so saturating an inclusive-MAX bound is fine.
        let end = match range.end_bound() {
            Bound::Included(x) => x.saturating_add(1),
            Bound::Excluded(x) => *x,
            Bound::Unbounded => u64::MAX,
        };
        let conn = self.lock();
        let mut stmt = conn
            .prepare_cached(
                "SELECT payload FROM log WHERE idx >= ?1 AND idx < ?2 ORDER BY idx ASC;",
            )
            .map_err(|e| StorageIOError::read_logs(&e))?;
        let rows = stmt
            .query_map(params![sql_idx(start), sql_idx(end)], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .map_err(|e| StorageIOError::read_logs(&e))?;
        let mut entries = Vec::new();
        for row in rows {
            let bytes = row.map_err(|e| StorageIOError::read_logs(&e))?;
            let entry: Entry<TypeConfig> =
                serde_json::from_slice(&bytes).map_err(|e| StorageIOError::read_logs(&e))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for RaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let conn = self.lock();
        let last_purged_log_id: Option<LogId<NodeId>> =
            Self::read_meta(&conn, META_PURGED).map_err(|e| StorageIOError::read_logs(&e))?;
        let last_log_id = Self::last_log_id(&conn)?.or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        // One autocommit upsert; with synchronous=FULL the vote is fsynced
        // before this returns, as save_vote's contract requires.
        let conn = self.lock();
        Self::write_meta(&conn, META_VOTE, vote).map_err(|e| StorageIOError::write_vote(&e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let conn = self.lock();
        Self::read_meta(&conn, META_VOTE).map_err(|e| StorageIOError::read_vote(&e).into())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let conn = self.lock();
        match committed {
            Some(log_id) => Self::write_meta(&conn, META_COMMITTED, &log_id)
                .map_err(|e| StorageIOError::write_logs(&e))?,
            None => {
                conn.execute("DELETE FROM meta WHERE key = ?1;", [META_COMMITTED])
                    .map_err(|e| StorageIOError::write_logs(&e))?;
            }
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let conn = self.lock();
        Self::read_meta(&conn, META_COMMITTED).map_err(|e| StorageIOError::read_logs(&e).into())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut conn = self.lock();
            let tx = conn
                .transaction()
                .map_err(|e| StorageIOError::write_logs(&e))?;
            {
                // OR REPLACE: after a truncate-then-append raft may rewrite
                // an index with a different entry; last write wins.
                let mut stmt = tx
                    .prepare_cached("INSERT OR REPLACE INTO log (idx, payload) VALUES (?1, ?2);")
                    .map_err(|e| StorageIOError::write_logs(&e))?;
                for entry in entries {
                    let payload =
                        serde_json::to_vec(&entry).map_err(|e| StorageIOError::write_logs(&e))?;
                    stmt.execute(params![sql_idx(entry.log_id.index), payload])
                        .map_err(|e| StorageIOError::write_logs(&e))?;
                }
            }
            // The commit fsyncs (synchronous=FULL): after this the entries
            // are durable, which is exactly what the callback asserts. On
            // error the callback is dropped instead, which openraft's core
            // observes as a failed log flush.
            tx.commit().map_err(|e| StorageIOError::write_logs(&e))?;
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let conn = self.lock();
        conn.execute("DELETE FROM log WHERE idx >= ?1;", [sql_idx(log_id.index)])
            .map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let conn = self.lock();
        // Record the purge marker before deleting rows: if we crash between
        // the two statements, a marker with leftover rows below it is
        // harmless (they are re-deleted by the next purge; reads start above
        // the marker), whereas rows deleted before the marker lands would
        // make `get_log_state` under-report `last_purged_log_id`.
        let prev: Option<LogId<NodeId>> =
            Self::read_meta(&conn, META_PURGED).map_err(|e| StorageIOError::write_logs(&e))?;
        if prev.is_none_or(|prev| prev < log_id) {
            Self::write_meta(&conn, META_PURGED, &log_id)
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }
        conn.execute("DELETE FROM log WHERE idx <= ?1;", [sql_idx(log_id.index)])
            .map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::raft::StoreCommand;
    use openraft::storage::RaftLogStorageExt;
    use openraft::{CommittedLeaderId, EntryPayload};

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    fn entry(term: u64, index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(term, index),
            payload: EntryPayload::Normal(StoreCommand {
                lane: Lane::Control,
                op: "test".to_string(),
                changeset: vec![index as u8],
            }),
        }
    }

    #[tokio::test]
    async fn blank_slate_reports_empty_state() -> R<()> {
        let dir = tempfile::tempdir()?;
        let mut store = RaftLogStore::open(dir.path(), Lane::Control)?;

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, None);
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(store.read_vote().await.unwrap(), None);
        assert_eq!(store.read_committed().await.unwrap(), None);
        assert!(store.try_get_log_entries(..).await.unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn append_read_truncate_purge_and_reopen_round_trip() -> R<()> {
        let dir = tempfile::tempdir()?;
        let mut store = RaftLogStore::open(dir.path(), Lane::Control)?;

        store
            .blocking_append([entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4)])
            .await
            .unwrap();

        // Range reads: inclusive/exclusive bounds behave like [start, end).
        let entries = store.try_get_log_entries(1..=4).await.unwrap();
        assert_eq!(
            entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        let entries = store.try_get_log_entries(2..4).await.unwrap();
        assert_eq!(
            entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![2, 3]
        );
        // Payload round-trips exactly.
        match &entries[0].payload {
            EntryPayload::Normal(cmd) => {
                assert_eq!(cmd.op, "test");
                assert_eq!(cmd.changeset, vec![2u8]);
                assert_eq!(cmd.lane, Lane::Control);
            }
            other => panic!("unexpected payload: {other:?}"),
        }

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(log_id(1, 4)));
        assert_eq!(state.last_purged_log_id, None);

        // Truncate drops the suffix (>= 4).
        store.truncate(log_id(1, 4)).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(log_id(1, 3)));
        assert!(store.try_get_log_entries(4..).await.unwrap().is_empty());

        // Purge drops the prefix (<= 1) and records the marker.
        store.purge(log_id(1, 1)).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 1)));
        assert_eq!(state.last_log_id, Some(log_id(1, 3)));
        let entries = store.try_get_log_entries(..).await.unwrap();
        assert_eq!(
            entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![2, 3]
        );

        // Vote + committed persist.
        let vote = Vote::new_committed(2, 7);
        store.save_vote(&vote).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
        store.save_committed(Some(log_id(1, 3))).await.unwrap();
        assert_eq!(store.read_committed().await.unwrap(), Some(log_id(1, 3)));

        // Reopen from the same directory: everything survives (this is the
        // recovery path openraft replays on restart).
        drop(store);
        let mut reopened = RaftLogStore::open(dir.path(), Lane::Control)?;
        let state = reopened.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(log_id(1, 3)));
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 1)));
        assert_eq!(reopened.read_vote().await.unwrap(), Some(vote));
        assert_eq!(reopened.read_committed().await.unwrap(), Some(log_id(1, 3)));
        let entries = reopened.try_get_log_entries(..).await.unwrap();
        assert_eq!(
            entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![2, 3]
        );
        Ok(())
    }

    #[tokio::test]
    async fn purge_everything_leaves_last_log_id_at_purge_marker() -> R<()> {
        let dir = tempfile::tempdir()?;
        let mut store = RaftLogStore::open(dir.path(), Lane::Scheduler)?;
        store
            .blocking_append([entry(1, 1), entry(1, 2)])
            .await
            .unwrap();
        store.purge(log_id(1, 2)).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 2)));
        // Contract: with no entries left, last_log_id == last_purged_log_id.
        assert_eq!(state.last_log_id, Some(log_id(1, 2)));
        Ok(())
    }

    #[tokio::test]
    async fn lanes_use_disjoint_databases() -> R<()> {
        let dir = tempfile::tempdir()?;
        let mut control = RaftLogStore::open(dir.path(), Lane::Control)?;
        let mut status = RaftLogStore::open(dir.path(), Lane::Status)?;
        control.blocking_append([entry(1, 1)]).await.unwrap();
        assert!(status.try_get_log_entries(..).await.unwrap().is_empty());
        assert!(dir.path().join("control/log.sqlite").exists());
        assert!(dir.path().join("status/log.sqlite").exists());
        Ok(())
    }
}
