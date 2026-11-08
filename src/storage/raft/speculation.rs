//! Speculative leader execution: the leader executes each write once
//! (raft-write-path plan §6.3).
//!
//! Without speculation the leader runs a write twice: it captures the
//! changeset inside a savepoint it then rolls back, and applies the
//! changeset again when the entry commits. With speculation the pipeline
//! **commits** the captured batch locally, in the same transaction as a
//! node-local undo record (the batch's inverse changeset) in
//! `raft_speculative`, and proposes the entry tagged with a [`Proposal`] id.
//! When the state machine applies that entry it recognizes it as this
//! node's oldest outstanding batch, skips the changeset, and — in one
//! transaction — advances the applied marker and deletes the undo record.
//!
//! **Any other entry** arriving while batches are outstanding means the log
//! went another way: a new term's blank entry after a leadership change, a
//! membership entry, another leader's entry at a truncated index, or an
//! entry this node proposed in an earlier process. The state machine then
//! undoes every outstanding batch, newest first, and applies the entry
//! normally. A leader's own log is append-only within its term, so its
//! proposals arrive in proposal order or not at all; matching only the
//! oldest outstanding batch is therefore exact.
//!
//! Invariants the rest of the engine keeps:
//!
//! - **Atomicity with the mutex.** The pipeline commits the batch, writes
//!   the undo record and pushes the ledger entry while holding the capture
//!   mutex, and the state machine checks the ledger while holding it, so an
//!   apply never sees a committed batch the ledger does not know about.
//! - **No stacking on unknown fate.** The pipeline captures only when the
//!   ledger is empty: a successful proposal resolves its batch before
//!   `client_write` returns, and a failed one blocks new captures until an
//!   apply resolves it.
//! - **Crash recovery.** [`recover`] undoes leftover records at startup,
//!   before openraft reads the applied marker; if the entry did commit,
//!   openraft re-applies it from the log like any other entry.
//! - **No copy of the database carries speculative rows.** [`scrub_image`]
//!   reverts them in a `VACUUM INTO` image (snapshot build, state backup);
//!   the undo record and the rows it reverts always commit together, so the
//!   scrubbed image is exactly committed state.
//! - **`single` topology only.** Under `lanes`, three pipelines share one
//!   database, so one lane's capture could read another lane's in-flight
//!   speculative rows and commit a decision a leadership loss later undoes
//!   (plan §6.3.6). `lanes` keeps capture → roll back → apply.
//!
//! What speculation does NOT hide: between the local commit and the apply
//! (one quorum round trip) a direct SQL read on the leader can see the
//! batch. The scheduling read model, the event stream and the worker stream
//! are projected at apply time, as before, and see committed state only.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex as StdMutex;

use rusqlite::{params, Connection, OptionalExtension};

use crate::prelude::*;
use crate::storage::raft::{ownership, Lane};
use crate::storage::store::Store;

/// The node-local table holding one undo record per outstanding batch
/// (`ownership::Owner::Internal`: never in a changeset, never applied from
/// one, never in a lane dump, excluded from the content hash).
pub(crate) const SPECULATION_TABLE: &str = "raft_speculative";

/// The outstanding speculative batches of each raft group, in proposal
/// order, shared by the group's pipeline thread and its state machine.
#[derive(Debug)]
pub(crate) struct SpeculationLedger {
    outstanding: StdMutex<HashMap<Lane, VecDeque<u64>>>,
    next_seq: AtomicU64,
}

impl SpeculationLedger {
    /// A ledger whose sequence numbers start at the current wall clock in
    /// nanoseconds, so a restarted process never reuses a sequence number an
    /// earlier incarnation put in the log (the ledger starts empty either
    /// way, so a reused one could only fail to match, never match wrongly —
    /// this just keeps log readers unambiguous).
    pub(crate) fn new() -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or(1);
        Self {
            outstanding: StdMutex::new(HashMap::new()),
            next_seq: AtomicU64::new(seed.max(1)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Lane, VecDeque<u64>>> {
        self.outstanding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A fresh sequence number for the next batch.
    pub(crate) fn next_seq(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Record a batch as committed locally. Call under the capture mutex,
    /// right after the commit.
    pub(crate) fn push(&self, lane: Lane, seq: u64) {
        self.lock().entry(lane).or_default().push_back(seq);
    }

    /// The oldest outstanding batch of `lane`.
    pub(crate) fn oldest(&self, lane: Lane) -> Option<u64> {
        self.lock()
            .get(&lane)
            .and_then(|queue| queue.front().copied())
    }

    /// Whether `lane` has no outstanding batch.
    pub(crate) fn is_empty(&self, lane: Lane) -> bool {
        self.lock().get(&lane).is_none_or(VecDeque::is_empty)
    }

    /// Drop the oldest outstanding batch (its entry applied).
    pub(crate) fn pop_oldest(&self, lane: Lane) {
        if let Some(queue) = self.lock().get_mut(&lane) {
            queue.pop_front();
        }
    }

    /// Forget every outstanding batch of `lane` (undone, or replaced by a
    /// snapshot install).
    pub(crate) fn clear(&self, lane: Lane) {
        self.lock().remove(&lane);
    }
}

/// The inverse of `changeset`: applying it after the changeset restores the
/// state before it.
pub(crate) fn invert(changeset: &[u8]) -> R<Vec<u8>> {
    let mut input: &[u8] = changeset;
    let mut output = Vec::with_capacity(changeset.len());
    rusqlite::session::invert_strm(&mut input, &mut output)
        .context("failed inverting a captured changeset")?;
    Ok(output)
}

/// Write the undo record of a batch being committed locally. Runs inside the
/// capture savepoint, after every op, so it commits (or rolls back) with the
/// batch. The pipeline's sessions are gone by then, so it is never part of
/// a changeset.
pub(crate) fn record_undo(conn: &Connection, lane: Lane, seq: u64, undo: &[u8]) -> R<()> {
    conn.execute(
        &format!("INSERT INTO {SPECULATION_TABLE} (lane, seq, undo) VALUES (?1, ?2, ?3);"),
        params![lane.wire_name(), seq as i64, undo],
    )
    .context("failed recording a speculative batch's undo record")?;
    Ok(())
}

/// Delete the undo record of a batch whose entry just applied (in the
/// apply's transaction). Exactly one row must go: anything else means the
/// ledger and the table disagree, which is a bug worth stopping on.
pub(crate) fn resolve_matched(conn: &Connection, lane: Lane, seq: u64) -> R<()> {
    let deleted = conn
        .execute(
            &format!("DELETE FROM {SPECULATION_TABLE} WHERE lane = ?1 AND seq = ?2;"),
            params![lane.wire_name(), seq as i64],
        )
        .context("failed resolving a speculative batch")?;
    if deleted != 1 {
        bail!(
            "speculative batch {seq} on the {} lane has {deleted} undo records, expected 1; \
             the speculation ledger and the state db disagree",
            lane.wire_name()
        );
    }
    Ok(())
}

/// Apply every undo record (of `lane`, or of every lane when `None`) on
/// `conn`, newest first, and delete them. Returns the inverse changesets it
/// applied, in the order applied, so the caller can re-project what changed.
///
/// The caller provides the transaction and `foreign_keys = OFF` (the undo
/// records every cascaded row explicitly, like any changeset). Conflicts
/// follow the apply policy: commutative tables OMIT (a speculatively
/// inserted journal row may already have been pruned locally), everything
/// else aborts — an undo that conflicts means something wrote a speculative
/// row, which nothing may.
pub(crate) fn undo_on(conn: &Connection, lane: Option<Lane>) -> R<Vec<Vec<u8>>> {
    let records: Vec<(String, i64, Vec<u8>)> = {
        let mut stmt = conn.prepare(&format!(
            "SELECT lane, seq, undo FROM {SPECULATION_TABLE}
             WHERE (?1 IS NULL OR lane = ?1) ORDER BY lane, seq DESC;"
        ))?;
        let rows = stmt.query_map([lane.map(Lane::wire_name)], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
        Store::collect_rows(rows)?
    };
    let mut undone = Vec::with_capacity(records.len());
    for (record_lane, seq, undo) in records {
        if !undo.is_empty() {
            let mut input: &[u8] = &undo;
            conn.apply_strm(
                &mut input,
                Some(ownership::table_applies_from_changeset),
                |conflict_type, item| ownership::resolve_apply_conflict(conflict_type, &item),
            )
            .with_context(|| {
                format!("failed undoing speculative batch {seq} on the {record_lane} lane")
            })?;
        }
        conn.execute(
            &format!("DELETE FROM {SPECULATION_TABLE} WHERE lane = ?1 AND seq = ?2;"),
            params![record_lane, seq],
        )?;
        undone.push(undo);
    }
    Ok(undone)
}

/// Undo `lane`'s outstanding batches right away: the pipeline calls this
/// when a proposal fails (the leader lost its leadership mid-flight), so a
/// deposed leader does not keep serving rows that may never commit — for
/// as long as a partition lasts — until another leader's entry reaches it.
///
/// Safe whether or not the entry commits later: once undone, the ledger no
/// longer knows the batch, so if its entry does commit, the state machine
/// applies its changeset normally onto the state without it. And no capture
/// can be taken on top of the undone state before that entry applies: this
/// node captures again only as leader of a later term, after
/// `ensure_linearizable` has applied everything that term's blank entry
/// follows. Takes the capture mutex, so it serializes with an apply that
/// may be matching the same batch (whichever runs first wins; both
/// outcomes are correct). Returns how many batches were undone.
pub(crate) fn undo_outstanding(store: &Store, ledger: &SpeculationLedger, lane: Lane) -> R<usize> {
    let undone = {
        let _captures = crate::storage::raft::capture::capture_exclusion()?;
        if ledger.is_empty(lane) {
            return Ok(0);
        }
        let undone = crate::storage::raft::capture::apply_tx_locked(store, |conn| {
            undo_on(conn, Some(lane))
        })?;
        ledger.clear(lane);
        undone
    };
    crate::manager::telemetry::metrics()
        .raft_speculation_total
        .inc_by(&[lane.wire_name(), "undone"], undone.len() as u64);
    // A cache that loaded a speculative row in the window is corrected.
    for changeset in &undone {
        store.project_changeset_side_effects(changeset);
    }
    Ok(undone.len())
}

/// Undo every leftover speculative batch in the state DB — the startup half
/// of the crash story. A crash between a batch's local commit and its
/// apply leaves the rows and their undo record; this reverts both, so the
/// state is exactly what the applied marker describes, and openraft
/// re-applies the entry from the log if it committed. Runs before the raft
/// engine reads the applied marker and before the read model is built.
/// Returns how many batches were undone.
pub(crate) fn recover(store: &Store) -> R<usize> {
    let _captures = crate::storage::raft::capture::capture_exclusion()?;
    let _in_apply = crate::storage::raft::capture::ApplyGuard::new();
    let mut conn = store.write_connection()?;
    let pending: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM {SPECULATION_TABLE};"),
        [],
        |row| row.get(0),
    )?;
    if pending == 0 {
        return Ok(0);
    }
    conn.pragma_update(None, "foreign_keys", "OFF")?;
    let result = (|| -> R<usize> {
        let sp = Store::write_tx(&mut conn)?;
        let undone = undo_on(&sp, None)?;
        sp.commit()?;
        Ok(undone.len())
    })();
    let restore = conn.pragma_update(None, "foreign_keys", "ON");
    let undone = result?;
    restore?;
    warn!(
        batches = undone,
        "Undid speculative raft batches left by a crash before their entries applied; any that \
         committed are re-applied from the raft log"
    );
    Ok(undone)
}

/// Revert every speculative batch recorded in the SQLite image at `path`
/// (a `VACUUM INTO` copy of the state DB) and clear its undo records, so the
/// image holds committed state only. Returns how many batches were
/// reverted. An image without the table (none of this build's) is left
/// alone.
pub(crate) fn scrub_image(path: &Path) -> R<usize> {
    let mut conn = Connection::open(path)
        .with_context(|| format!("failed opening image {} to scrub", path.display()))?;
    let has_table: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1);",
        [SPECULATION_TABLE],
        |row| row.get(0),
    )?;
    if !has_table {
        return Ok(0);
    }
    let pending: Option<i64> = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {SPECULATION_TABLE};"),
            [],
            |row| row.get(0),
        )
        .optional()?;
    if pending.unwrap_or(0) == 0 {
        return Ok(0);
    }
    conn.pragma_update(None, "foreign_keys", "OFF")?;
    let tx = conn.transaction()?;
    let undone = undo_on(&tx, None)?;
    tx.commit()?;
    Ok(undone.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_tracks_batches_in_proposal_order_per_lane() {
        let ledger = SpeculationLedger::new();
        let (a, b, c) = (ledger.next_seq(), ledger.next_seq(), ledger.next_seq());
        assert!(a < b && b < c);
        assert!(ledger.is_empty(Lane::Control));
        ledger.push(Lane::Control, a);
        ledger.push(Lane::Control, b);
        ledger.push(Lane::Status, c);
        assert_eq!(ledger.oldest(Lane::Control), Some(a));
        ledger.pop_oldest(Lane::Control);
        assert_eq!(ledger.oldest(Lane::Control), Some(b));
        ledger.clear(Lane::Control);
        assert!(ledger.is_empty(Lane::Control));
        assert_eq!(ledger.oldest(Lane::Status), Some(c));
    }
}
