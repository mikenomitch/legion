//! Raft state machine over the Legion state store (phases A-1/A-3 of
//! `docs/plans/durability-and-ops-execution-plan.md` §2).
//!
//! `apply()` takes committed entries and, for each [`StoreCommand`], applies
//! the captured changeset to this node's state DB via
//! [`capture::apply_changeset_with`] — which also projects the read-model /
//! event / notify side effects from the changeset (JC-11) — and records the
//! applied log id in the `raft_applied` table **inside the same savepoint**
//! as the changeset rows. Data and applied-index therefore commit
//! atomically: a crash at any point either has both or neither, so recovery
//! resumes from exactly the right log position and a replica can never
//! re-apply or skip an entry (plan §A5 "state and index can never
//! diverge"). Blank and membership entries advance the same marker (the
//! membership additionally persisting alongside it), in their own
//! transaction on the same table.
//!
//! `applied_state()` reads that table back; a brand-new node with no
//! `raft_applied` row reports `(None, StoredMembership::default())` — the
//! blank-slate contract openraft expects.
//!
//! # Snapshots (plan §A5; per-lane scope from §7-C3)
//!
//! Two snapshot scopes exist, selected by topology
//! ([`SnapshotScope`]):
//!
//! **`WholeDb` (topology `single`).** A snapshot is a whole-database
//! image: `VACUUM INTO` a file under `<data_dir>/<lane>/snapshots/`, run on
//! a dedicated read-only connection. In WAL mode that is one read
//! transaction over committed state only: an apply commits its rows and
//! its `raft_applied` marker together, and a capture's savepoint never
//! commits, so the image is a transactionally consistent snapshot **that
//! already contains its own `raft_applied` row** (the applied marker
//! travels inside the snapshot and the build reads the meta back from the
//! image; see `verify` in [`StoreStateMachine::install_snapshot`]). Writes
//! keep flowing while it is built (R5). Install
//! replaces the state DB wholesale: the received bytes are staged to a
//! temp file, the snapshot's meta is written into the staged file's
//! `raft_applied` table, and [`Store::install_state_db`] atomically swaps
//! the state DB and reopens every connection (write + read pool) and the
//! read model. Crash safety of the swap is proven step-by-step in
//! `install_state_db`'s docs.
//!
//! **`LaneTables` (topology `lanes`).** Whole-DB images are unsound when
//! three groups share the database file — installing one lane's image
//! would clobber the sibling lanes' tables. Single-writer table ownership
//! (plan §7-C2, enforced at capture time) makes the sound alternative
//! trivial: a lane's snapshot is a **logical dump of only its owned
//! tables** (`ownership::snapshot_tables_for_lane` — the control lane
//! additionally carries the commutative tables, JC-28). The dump is a
//! small standalone SQLite database (row values round-trip exactly —
//! SQLite storage classes, no text encoding ambiguity) holding one table
//! per dumped table plus a manifest (`legion_lane_snapshot` /
//! `legion_lane_snapshot_tables`), read in one read transaction together
//! with the lane's `raft_applied` marker, so it is transactionally
//! consistent without blocking writers. Install is **one transaction** on the live
//! state DB with the apply guard set: delete every LANE-OWNED dumped
//! table's rows and insert the dump's rows (commutative tables are instead
//! applied additively with `INSERT OR IGNORE` — sibling lanes' already-
//! applied inserts must survive the install), write the lane's
//! `raft_applied` marker from the snapshot meta, commit — tables owned by
//! other lanes are never touched (the manifest is validated against the
//! local ownership map before any delete). Snapshot policy, compaction, retention, and tmp cleanup are
//! identical to the whole-DB path, per lane.
//!
//! In both scopes, snapshot metadata (`SnapshotMeta`: last applied log id
//! plus membership) is persisted as a `.meta.json` sidecar so
//! `get_current_snapshot` can serve the newest image across restarts.
//!
//! # Sync-inside-async
//!
//! Like the log store, `apply()` runs inline on the async caller: it is
//! synchronous rusqlite against the state DB and must complete before the
//! entry is acknowledged anyway. The state-machine apply path is the
//! replication pipeline's backpressure point by design, and one entry's
//! changeset is bounded work (openraft runs exactly ONE state-machine
//! worker task per lane, so at most one worker thread per lane is inside an
//! apply at any moment — see the runtime sizing in `driver::start_engine`).
//!
//! Snapshot BUILD and INSTALL are the exception: they `VACUUM INTO`, dump,
//! or bulk-replace whole tables, i.e. for seconds on a large state DB.
//! Install also holds the global capture mutex for its transaction, so
//! every lane's writes wait for it. Build reads on its own connection and
//! holds no lock. Left inline they would pin async
//! worker threads (adversarial finding, 2026-07-25: with three lanes, two
//! concurrent multi-second snapshot operations starved a 2-thread runtime
//! of heartbeat/election/replication progress). Both therefore run on
//! tokio's BLOCKING pool via `spawn_blocking`; the async method still
//! awaits completion, so ordering and backpressure are unchanged — only
//! the thread the blocking work sits on differs.

use std::io::Cursor;
use std::path::{Path, PathBuf};

use openraft::storage::{RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder,
    StorageError, StorageIOError, StoredMembership,
};
use rusqlite::{params, Connection, OptionalExtension};

use crate::prelude::*;
use crate::storage::raft::capture;
use crate::storage::raft::ownership;
use crate::storage::raft::types::TypeConfig;
use crate::storage::raft::{Lane, StoreResponse};
use crate::storage::store::Store;

type NodeId = u64;

/// What a snapshot of this lane's state machine covers (module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotScope {
    /// Whole-database `VACUUM INTO` image — topology `single`, where the
    /// one group owns every table.
    WholeDb,
    /// Logical dump of the lane's owned tables only — topology `lanes`,
    /// where sibling groups own the other tables (plan §7-C3).
    LaneTables,
}

/// Manifest table names inside a `LaneTables` snapshot database.
const LANE_SNAPSHOT_META_TABLE: &str = "legion_lane_snapshot";
const LANE_SNAPSHOT_TABLES_TABLE: &str = "legion_lane_snapshot_tables";
/// AUTOINCREMENT high-water marks (`sqlite_sequence` rows) of the dumped
/// tables. A lane dump copies rows, and a row copy only re-seeds a table's
/// sequence to its largest *surviving* id; carrying the mark keeps a replica
/// that installs the dump (and may lead later) from handing out an id the
/// leader already used and deleted — allocation ids are never reused.
const LANE_SNAPSHOT_SEQUENCES_TABLE: &str = "legion_lane_snapshot_sequences";
/// Format marker stored under `format` in [`LANE_SNAPSHOT_META_TABLE`].
const LANE_SNAPSHOT_FORMAT: &str = "legion-lane-snapshot-v1";

/// A lane's applied position as stored in `raft_applied`: last applied log
/// id (None = blank slate) and last applied membership.
type AppliedRow = (Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>);

/// How many finished snapshots to retain on disk. The newest serves
/// laggards; one predecessor is kept as a hedge against a torn latest
/// (e.g. a crash between the image rename and its meta sidecar landing).
const SNAPSHOTS_RETAINED: usize = 2;

/// One lane's state machine. All lanes share the same underlying `Store`
/// (state DB); their applied positions are tracked as separate rows of
/// `raft_applied`, keyed by the lane's wire name.
pub(crate) struct StoreStateMachine {
    lane: Lane,
    store: Store,
    /// `<data_dir>/<lane>/snapshots/` — images + `.meta.json` sidecars.
    snapshots_dir: PathBuf,
    /// Whole-DB image (`single`) or lane-table dump (`lanes`) — module docs.
    scope: SnapshotScope,
    /// Test hook (`Store::raft_set_apply_paused`): while set, `apply` waits
    /// before touching the state DB, so a test can hold a replica's applied
    /// index behind its log (the R1 failover regression). Never set in
    /// production.
    apply_paused: Arc<std::sync::atomic::AtomicBool>,
}

impl std::fmt::Debug for StoreStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreStateMachine")
            .field("lane", &self.lane)
            .field("snapshots_dir", &self.snapshots_dir)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl StoreStateMachine {
    pub(crate) fn new(
        lane: Lane,
        store: Store,
        snapshots_dir: PathBuf,
        scope: SnapshotScope,
    ) -> Self {
        Self {
            lane,
            store,
            snapshots_dir,
            scope,
            apply_paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Share the driver's apply-pause test hook (see the field docs).
    pub(crate) fn with_apply_pause(mut self, paused: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.apply_paused = paused;
        self
    }

    /// A second handle to the same lane/state DB, owned and `'static`, for
    /// moving the blocking snapshot-install work onto tokio's blocking pool
    /// (module docs "sync-inside-async"). Every field is a cheap handle:
    /// `Store` is a clone of the shared connections/read model, the rest is
    /// a lane tag, a path, and a scope.
    fn detached(&self) -> Self {
        Self {
            lane: self.lane,
            store: self.store.clone(),
            snapshots_dir: self.snapshots_dir.clone(),
            scope: self.scope,
            apply_paused: self.apply_paused.clone(),
        }
    }

    /// Upserts this lane's `raft_applied` row on an already-open
    /// transaction/savepoint. `membership` is only written when a membership
    /// entry is being applied; otherwise the stored membership is preserved.
    fn record_applied_on(
        &self,
        conn: &Connection,
        log_id: LogId<NodeId>,
        membership: Option<&StoredMembership<NodeId, BasicNode>>,
    ) -> R<()> {
        let last_applied = serde_json::to_vec(&log_id).context("failed encoding applied log id")?;
        match membership {
            Some(stored) => {
                let membership =
                    serde_json::to_vec(stored).context("failed encoding membership")?;
                conn.execute(
                    "INSERT INTO raft_applied (lane, last_applied, membership)
                     VALUES (?1, ?2, ?3)
                     ON CONFLICT(lane) DO UPDATE SET
                         last_applied = excluded.last_applied,
                         membership = excluded.membership;",
                    params![self.lane.wire_name(), last_applied, membership],
                )?;
            }
            None => {
                conn.execute(
                    "INSERT INTO raft_applied (lane, last_applied, membership)
                     VALUES (?1, ?2, NULL)
                     ON CONFLICT(lane) DO UPDATE SET
                         last_applied = excluded.last_applied;",
                    params![self.lane.wire_name(), last_applied],
                )?;
            }
        }
        Ok(())
    }

    /// Records the applied marker (and optionally membership) in its own
    /// transaction — used for blank/membership entries, which carry no
    /// changeset.
    fn record_applied(
        &self,
        log_id: LogId<NodeId>,
        membership: Option<&StoredMembership<NodeId, BasicNode>>,
    ) -> R<()> {
        // Applies are mutually exclusive with captures: writing the marker
        // while a capture savepoint is open on the shared connection would
        // nest this savepoint inside it and the capture's rollback would
        // silently erase the marker (adversarial finding: capture/apply
        // interleave).
        let _captures = capture::capture_exclusion()?;
        // The apply guard admits this write through the raft-mode
        // `write_connection` bypass guard (JC-14).
        let _in_apply = capture::ApplyGuard::new();
        let mut conn = self.store.write_connection()?;
        let sp = Store::write_tx(&mut conn)?;
        self.record_applied_on(&sp, log_id, membership)?;
        sp.commit()?;
        Ok(())
    }

    /// Reads this lane's `raft_applied` row from `conn`.
    fn read_applied_row(conn: &Connection, lane: Lane) -> R<AppliedRow> {
        let row: Option<(Vec<u8>, Option<Vec<u8>>)> = conn
            .query_row(
                "SELECT last_applied, membership FROM raft_applied WHERE lane = ?1;",
                [lane.wire_name()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .context("failed reading raft_applied")?;
        match row {
            None => Ok((None, StoredMembership::default())),
            Some((last_applied, membership)) => {
                let log_id: LogId<NodeId> = serde_json::from_slice(&last_applied)
                    .context("failed decoding raft_applied log id")?;
                let membership = match membership {
                    None => StoredMembership::default(),
                    Some(bytes) => serde_json::from_slice(&bytes)
                        .context("failed decoding raft_applied membership")?,
                };
                Ok((Some(log_id), membership))
            }
        }
    }

    /// The real install path (see the module docs): stage → adopt meta →
    /// persist as current snapshot → install (atomic swap for `WholeDb`,
    /// one owned-tables transaction for `LaneTables`) → verify.
    fn install_snapshot_inner(
        &self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        bytes: &[u8],
    ) -> R<()> {
        std::fs::create_dir_all(&self.snapshots_dir).with_context(|| {
            format!(
                "failed creating snapshots dir {}",
                self.snapshots_dir.display()
            )
        })?;
        if !bytes.starts_with(b"SQLite format 3\0") {
            bail!(
                "received snapshot {} is not a SQLite database image",
                meta.snapshot_id
            );
        }
        if self.scope == SnapshotScope::LaneTables {
            return self.install_lane_snapshot(meta, bytes);
        }

        // Stage the received image and write the snapshot's applied
        // marker/membership INTO it before it can become the state DB: after
        // the swap, `raft_applied` inside the file matches `meta` exactly,
        // no matter where a crash lands (see `Store::install_state_db`).
        let id = sanitize_snapshot_id(&meta.snapshot_id);
        let staged = self.snapshots_dir.join(format!("{id}.db.tmp"));
        let _ = std::fs::remove_file(&staged);
        std::fs::write(&staged, bytes)
            .with_context(|| format!("failed staging snapshot {}", staged.display()))?;
        {
            let conn = Connection::open(&staged)
                .with_context(|| format!("failed opening staged snapshot {}", staged.display()))?;
            match &meta.last_log_id {
                Some(log_id) => {
                    let last_applied = serde_json::to_vec(log_id)?;
                    let membership = serde_json::to_vec(&meta.last_membership)?;
                    conn.execute(
                        "INSERT INTO raft_applied (lane, last_applied, membership)
                         VALUES (?1, ?2, ?3)
                         ON CONFLICT(lane) DO UPDATE SET
                             last_applied = excluded.last_applied,
                             membership = excluded.membership;",
                        params![self.lane.wire_name(), last_applied, membership],
                    )
                    .context(
                        "failed adopting snapshot meta into staged snapshot (is the image a \
                         migrated Legion state db?)",
                    )?;
                }
                // A snapshot of nothing: blank slate.
                None => {
                    conn.execute(
                        "DELETE FROM raft_applied WHERE lane = ?1;",
                        [self.lane.wire_name()],
                    )?;
                }
            }
            // NOTE (JC-41, reversing JC-27): the staged image's `eval` rows
            // are KEPT. The eval queue is replicated scheduler-lane state
            // again, so a whole-DB snapshot carries the queue exactly like
            // every other replicated table — stripping it here is what made
            // a snapshot-caught-up follower lose acknowledged work (a
            // pending `job_stop` teardown, a mid-flight rollout tick) the
            // moment it won an election.
        }
        fsync_file(&staged)?;

        // Persist as the current snapshot (image + meta sidecar) BEFORE the
        // swap, so `get_current_snapshot` can serve it even if the process
        // dies right after the state DB is replaced.
        let final_db = self.snapshots_dir.join(format!("{id}.db"));
        std::fs::copy(&staged, &final_db)
            .with_context(|| format!("failed persisting snapshot {}", final_db.display()))?;
        fsync_file(&final_db)?;
        write_meta_sidecar(&self.snapshots_dir, &id, meta)?;

        // Atomic swap + reopen. Staged file lives in the raft data dir; the
        // state DB may be on another filesystem, so move the staged file
        // next to it first (rename(2) requires same-fs).
        let store_dir = self
            .store
            .state_db_path()
            .parent()
            .context("state db path has no parent")?;
        let incoming = store_dir.join(format!("incoming-snapshot-{id}.tmp"));
        let _ = std::fs::remove_file(&incoming);
        std::fs::copy(&staged, &incoming)
            .with_context(|| format!("failed staging snapshot at {}", incoming.display()))?;
        fsync_file(&incoming)?;
        let _ = std::fs::remove_file(&staged);
        self.store.install_state_db(&incoming)?;

        // Verify: the applied marker the swapped-in DB reports must equal
        // the snapshot meta (it was written into the file above). Anything
        // else means the swap did not land the file we prepared.
        let conn = self.store.read_connection()?;
        let (applied, _) = Self::read_applied_row(&conn, self.lane)?;
        drop(conn);
        if applied != meta.last_log_id {
            bail!(
                "post-install consistency check failed: state db reports applied {:?} but \
                 snapshot meta is {:?}",
                applied,
                meta.last_log_id
            );
        }

        prune_snapshots(&self.snapshots_dir);
        info!(
            lane = self.lane.wire_name(),
            snapshot_id = %meta.snapshot_id,
            last_log_id = ?meta.last_log_id,
            "Installed raft snapshot: state db replaced and connections reopened"
        );
        Ok(())
    }

    /// `LaneTables` install (plan §7-C3, module docs): validate the dump's
    /// manifest against the local ownership map, persist it as the current
    /// snapshot, then — in ONE transaction with the apply guard set —
    /// replace the dumped tables' rows and write the lane's applied marker.
    /// Other lanes' tables are never touched.
    fn install_lane_snapshot(&self, meta: &SnapshotMeta<NodeId, BasicNode>, bytes: &[u8]) -> R<()> {
        let id = sanitize_snapshot_id(&meta.snapshot_id);
        let staged = self.snapshots_dir.join(format!("{id}.db.tmp"));
        let _ = std::fs::remove_file(&staged);
        std::fs::write(&staged, bytes)
            .with_context(|| format!("failed staging snapshot {}", staged.display()))?;

        // Read the manifest through a read-only connection; refuse anything
        // that is not a lane dump for THIS lane, and refuse a dump listing a
        // table this node's ownership map does not assign to this lane —
        // deleting such a table would clobber a sibling lane.
        let snap = Connection::open_with_flags(&staged, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("failed opening staged snapshot {}", staged.display()))?;
        let manifest_value = |key: &str| -> R<String> {
            snap.query_row(
                &format!("SELECT value FROM {LANE_SNAPSHOT_META_TABLE} WHERE key = ?1;"),
                [key],
                |row| row.get::<_, String>(0),
            )
            .with_context(|| {
                format!(
                    "snapshot {} is not a lane-table dump (missing manifest key `{key}`); \
                     was it built by a `single`-topology (whole-DB) node?",
                    meta.snapshot_id
                )
            })
        };
        let format = manifest_value("format")?;
        if format != LANE_SNAPSHOT_FORMAT {
            bail!(
                "snapshot {} has unsupported lane-dump format `{format}` (this node speaks \
                 `{LANE_SNAPSHOT_FORMAT}`)",
                meta.snapshot_id
            );
        }
        let dump_lane = manifest_value("lane")?;
        if dump_lane != self.lane.wire_name() {
            bail!(
                "snapshot {} was dumped for lane `{dump_lane}` but this state machine is lane \
                 `{}`",
                meta.snapshot_id,
                self.lane.wire_name()
            );
        }
        let tables: Vec<String> = {
            let mut stmt = snap.prepare(&format!(
                "SELECT name FROM {LANE_SNAPSHOT_TABLES_TABLE} ORDER BY name;"
            ))?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            Store::collect_rows(rows)?
        };
        let allowed: std::collections::BTreeSet<&'static str> =
            ownership::snapshot_tables_for_lane(self.lane)
                .into_iter()
                .collect();
        for table in &tables {
            if !allowed.contains(table.as_str()) {
                bail!(
                    "lane snapshot {} lists table `{table}`, which this node's ownership map \
                     does not assign to the {} lane's snapshot — refusing to clobber a sibling \
                     lane's table. This is version skew: the sender classifies `{table}` \
                     differently than this node does. Moving a table between lanes is NOT a \
                     rolling upgrade (a mixed-version cluster cannot agree on what a lane's \
                     snapshot contains); it requires the JC-43 fresh-cluster path — back up \
                     (`legion state backup`), start a fresh cluster on the new version, restore \
                     (`legion start manager --restore-from <backup>`). See the table-ownership \
                     note in docs/multi-raft.md.",
                    meta.snapshot_id,
                    self.lane.wire_name()
                );
            }
        }
        // AUTOINCREMENT high-water marks carried by the dump (absent from a
        // dump built before they were carried: nothing to raise then).
        let sequences: Vec<(String, i64)> = {
            let carried: bool = snap.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1);",
                [LANE_SNAPSHOT_SEQUENCES_TABLE],
                |row| row.get(0),
            )?;
            if carried {
                let mut stmt = snap.prepare(&format!(
                    "SELECT name, seq FROM {LANE_SNAPSHOT_SEQUENCES_TABLE};"
                ))?;
                let rows = stmt.query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })?;
                Store::collect_rows(rows)?
                    .into_iter()
                    .filter(|(table, _)| tables.contains(table))
                    .collect()
            } else {
                Vec::new()
            }
        };
        // Column lists per dumped table, read up front so the install
        // transaction below only moves rows.
        let mut columns: Vec<(String, Vec<String>)> = Vec::with_capacity(tables.len());
        for table in &tables {
            let stmt = snap.prepare(&format!("SELECT * FROM \"{table}\" LIMIT 0;"))?;
            let cols: Vec<String> = stmt.column_names().into_iter().map(str::to_owned).collect();
            columns.push((table.clone(), cols));
        }

        // Persist as the current snapshot (image + meta sidecar) BEFORE
        // mutating the state DB, so `get_current_snapshot` can serve it even
        // if the process dies mid-install (the install transaction below is
        // atomic, so a crash re-runs it from the log/snapshot).
        let final_db = self.snapshots_dir.join(format!("{id}.db"));
        std::fs::copy(&staged, &final_db)
            .with_context(|| format!("failed persisting snapshot {}", final_db.display()))?;
        fsync_file(&final_db)?;
        write_meta_sidecar(&self.snapshots_dir, &id, meta)?;

        // The install transaction: capture exclusion + apply guard, exactly
        // like a committed-changeset apply (which this is the bulk form of).
        {
            let _captures = capture::capture_exclusion()?;
            let _in_apply = capture::ApplyGuard::new();
            let mut conn = self.store.write_connection()?;
            if !conn.is_autocommit() {
                bail!(
                    "lane snapshot install found the write connection inside an open \
                     transaction; installing would nest and could be silently rolled back"
                );
            }
            // Same-lane FK cascades must not fire while tables are swapped
            // row-set for row-set (the dump is internally consistent; firing
            // cascades mid-replace would delete rows the dump re-inserts) —
            // the JC-18 rule, applied to the bulk path. No-op inside a
            // transaction, so it brackets the savepoint.
            conn.pragma_update(None, "foreign_keys", "OFF")
                .context("failed disabling foreign_keys for lane snapshot install")?;
            let install_result = (|| -> R<()> {
                let sp = Store::write_tx(&mut conn)?;
                for (table, cols) in &columns {
                    // Commutative tables (JC-28) install as a MERGE (union),
                    // not a replace: they are inserted into from EVERY lane,
                    // so rows this replica already applied from sibling-lane
                    // log entries may be absent from the (control-lane)
                    // snapshot source. A delete-and-reseed would permanently
                    // erase them — those entries sit at or below the sibling
                    // lanes' untouched applied markers and are never
                    // replayed, so nothing would ever put them back
                    // (adversarial finding, 2026-07-24).
                    //
                    // Why union is the CORRECT install semantics here, not
                    // just the safe one: commutative tables are insert-only
                    // (the guard rejects UPDATE/DELETE inside a capture —
                    // ownership.rs) and their primary keys are globally
                    // unique across lanes by construction (JC-42: minted ids
                    // carry a per-lane residue, caller-supplied ids are
                    // already unique). So a given PK denotes one and only one
                    // row cluster-wide, "row present" is monotonic, and the
                    // union of two replicas' rows is exactly the set of rows
                    // that were ever inserted — order-independent, which is
                    // the same property that lets apply resolve conflicts
                    // with OMIT. Hence INSERT OR IGNORE and no DELETE.
                    //
                    // Lane-owned tables keep delete-and-replace: they have a
                    // single writing lane, so that lane's snapshot + log
                    // fully reconstruct them.
                    let additive =
                        ownership::owner_lane(table) == Some(ownership::Owner::Commutative);
                    if !additive {
                        sp.execute(&format!("DELETE FROM \"{table}\";"), [])
                            .with_context(|| format!("failed clearing `{table}` for install"))?;
                    }
                    let quoted: Vec<String> = cols.iter().map(|c| format!("\"{c}\"")).collect();
                    let placeholders: Vec<String> =
                        (1..=cols.len()).map(|i| format!("?{i}")).collect();
                    let verb = if additive {
                        "INSERT OR IGNORE"
                    } else {
                        "INSERT"
                    };
                    let mut insert = sp.prepare(&format!(
                        "{verb} INTO \"{table}\" ({}) VALUES ({});",
                        quoted.join(", "),
                        placeholders.join(", ")
                    ))?;
                    let mut read = snap.prepare(&format!("SELECT * FROM \"{table}\";"))?;
                    let mut rows = read.query([])?;
                    while let Some(row) = rows.next()? {
                        let values: Vec<rusqlite::types::Value> = (0..cols.len())
                            .map(|i| row.get(i))
                            .collect::<Result<_, _>>()?;
                        insert.execute(rusqlite::params_from_iter(values))?;
                    }
                }
                // Raise (never lower) each table's AUTOINCREMENT mark to the
                // leader's, so ids deleted on the leader are not reissued
                // here. The row copies above already raised it to the
                // largest surviving id.
                for (table, seq) in &sequences {
                    let updated = sp.execute(
                        "UPDATE sqlite_sequence SET seq = MAX(seq, ?2) WHERE name = ?1;",
                        rusqlite::params![table, seq],
                    )?;
                    if updated == 0 {
                        sp.execute(
                            "INSERT INTO sqlite_sequence (name, seq) VALUES (?1, ?2);",
                            rusqlite::params![table, seq],
                        )?;
                    }
                }
                // The lane's applied marker commits atomically with the rows
                // it describes (plan §A5 "state and index can never
                // diverge").
                match &meta.last_log_id {
                    Some(log_id) => {
                        self.record_applied_on(&sp, *log_id, Some(&meta.last_membership))?
                    }
                    None => {
                        sp.execute(
                            "DELETE FROM raft_applied WHERE lane = ?1;",
                            [self.lane.wire_name()],
                        )?;
                    }
                }
                sp.commit()?;
                Ok(())
            })();
            let restore_result = conn.pragma_update(None, "foreign_keys", "ON");
            install_result?;
            restore_result
                .context("failed re-enabling foreign_keys after lane snapshot install")?;
        }
        drop(snap);
        let _ = std::fs::remove_file(&staged);

        // The read model and the work-stream logs describe tables this
        // install may have replaced, and the control lane's dump carries
        // `keyring_key`: rebuild them from the committed state. Infallible,
        // so a cache that cannot reload never stops the raft core over an
        // install that committed (the check below still catches a DB that
        // cannot be read at all).
        self.store.rebuild_node_local_state(true);

        // Verify: the applied marker now reported for this lane must equal
        // the snapshot meta.
        let conn = self.store.read_connection()?;
        let (applied, _) = Self::read_applied_row(&conn, self.lane)?;
        drop(conn);
        if applied != meta.last_log_id {
            bail!(
                "post-install consistency check failed: state db reports applied {:?} but \
                 snapshot meta is {:?}",
                applied,
                meta.last_log_id
            );
        }

        prune_snapshots(&self.snapshots_dir);
        info!(
            lane = self.lane.wire_name(),
            snapshot_id = %meta.snapshot_id,
            last_log_id = ?meta.last_log_id,
            tables = tables.len(),
            "Installed lane-table raft snapshot: owned tables replaced in one transaction"
        );
        Ok(())
    }

    /// Newest complete snapshot (image + sidecar) on disk, if any.
    fn current_snapshot(&self) -> R<Option<Snapshot<TypeConfig>>> {
        let Some((meta, db_path)) = newest_snapshot(&self.snapshots_dir)? else {
            return Ok(None);
        };
        let data = std::fs::read(&db_path)
            .with_context(|| format!("failed reading snapshot image {}", db_path.display()))?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

impl RaftStateMachine<TypeConfig> for StoreStateMachine {
    type SnapshotBuilder = StoreSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let conn = self
            .store
            .read_connection()
            .map_err(|e| StorageIOError::read_state_machine(AnyError::error(format!("{e:#}"))))?;
        Self::read_applied_row(&conn, self.lane).map_err(|e| {
            StorageIOError::read_state_machine(AnyError::error(format!("{e:#}"))).into()
        })
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<StoreResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        while self.apply_paused.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // Apply-side index-skip guard: entries at or below the recorded
        // applied index were already applied (typically re-delivered around
        // a snapshot install or a restart replay); re-applying a changeset
        // would conflict-abort on its own effects. Read once per batch and
        // track as we go — entries within a batch are ordered.
        let mut last_applied = {
            let conn = self.store.read_connection().map_err(|e| {
                StorageIOError::read_state_machine(AnyError::error(format!("{e:#}")))
            })?;
            Self::read_applied_row(&conn, self.lane)
                .map_err(|e| StorageIOError::read_state_machine(AnyError::error(format!("{e:#}"))))?
                .0
        };
        let mut replies = Vec::new();
        for entry in entries {
            let log_id = entry.log_id;
            if last_applied.is_some_and(|applied| log_id.index <= applied.index) {
                debug!(
                    lane = self.lane.wire_name(),
                    index = log_id.index,
                    applied = ?last_applied,
                    "skipping already-applied raft entry"
                );
                replies.push(StoreResponse {});
                continue;
            }
            let result: R<()> = match entry.payload {
                // Blank entries (leader no-op commits) only advance the
                // applied marker.
                EntryPayload::Blank => self.record_applied(log_id, None),
                // The core path: changeset rows + applied marker in ONE
                // savepoint, then side-effect projection from the committed
                // changeset (JC-11).
                EntryPayload::Normal(command) => {
                    debug!(
                        lane = self.lane.wire_name(),
                        op = %command.op,
                        index = log_id.index,
                        "applying replicated store command"
                    );
                    capture::apply_changeset_with(&self.store, &command.changeset, |conn| {
                        self.record_applied_on(conn, log_id, None)
                    })
                }
                // Membership entries persist the config next to the marker
                // (openraft: "storing it" is all an implementation must do).
                EntryPayload::Membership(membership) => {
                    let stored = StoredMembership::new(Some(log_id), membership);
                    self.record_applied(log_id, Some(&stored))
                }
            };
            result.map_err(|e| StorageIOError::apply(log_id, AnyError::error(format!("{e:#}"))))?;
            last_applied = Some(log_id);
            replies.push(StoreResponse {});
        }
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        StoreSnapshotBuilder {
            lane: self.lane,
            store: self.store.clone(),
            snapshots_dir: self.snapshots_dir.clone(),
            scope: self.scope,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        // Off the async worker threads (module docs "sync-inside-async"):
        // an install bulk-replaces tables under the global capture mutex.
        // The state machine's fields are cheap handles, so the blocking task
        // gets its own copy; the received image moves in without a copy.
        let sm = self.detached();
        let meta_owned = meta.clone();
        let bytes = snapshot.into_inner();
        let signature = meta.signature();
        tokio::task::spawn_blocking(move || sm.install_snapshot_inner(&meta_owned, &bytes))
            .await
            .map_err(|e| -> StorageError<NodeId> {
                StorageIOError::write_snapshot(
                    Some(signature.clone()),
                    AnyError::error(format!("snapshot install task failed: {e}")),
                )
                .into()
            })?
            .map_err(|e| {
                StorageIOError::write_snapshot(Some(signature), AnyError::error(format!("{e:#}")))
                    .into()
            })
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        self.current_snapshot().map_err(|e| {
            StorageIOError::read_snapshot(None, AnyError::error(format!("{e:#}"))).into()
        })
    }
}

/// Builds snapshots: whole-DB `VACUUM INTO` images (`single` topology) or
/// lane-table dumps (`lanes` topology) — plan §A5 / §7-C3, module docs.
///
/// `Clone` is cheap (a lane tag, a `Store` handle clone, a path, a scope)
/// and exists so `build_snapshot` can hand the blocking build to tokio's
/// blocking pool.
#[derive(Debug, Clone)]
pub(crate) struct StoreSnapshotBuilder {
    lane: Lane,
    store: Store,
    snapshots_dir: PathBuf,
    scope: SnapshotScope,
}

impl StoreSnapshotBuilder {
    /// Dump the lane's owned tables (plus manifest) into a standalone
    /// SQLite database at `tmp`. Runs inside the caller's read
    /// transaction, so the row reads are transactionally consistent with
    /// the `raft_applied` row the caller reads as the snapshot meta.
    fn dump_lane_tables(conn: &Connection, lane: Lane, tmp: &Path) -> R<()> {
        let snap = Connection::open(tmp)
            .with_context(|| format!("failed creating lane snapshot db {}", tmp.display()))?;
        // Durability comes from the caller's fsync + rename; journaling the
        // build file would only slow it down.
        snap.pragma_update(None, "journal_mode", "OFF")?;
        snap.pragma_update(None, "synchronous", "OFF")?;
        snap.execute_batch(&format!(
            "BEGIN;
             CREATE TABLE {LANE_SNAPSHOT_META_TABLE} (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE {LANE_SNAPSHOT_TABLES_TABLE} (name TEXT PRIMARY KEY);"
        ))?;
        snap.execute(
            &format!("INSERT INTO {LANE_SNAPSHOT_META_TABLE} (key, value) VALUES ('format', ?1), ('lane', ?2);"),
            rusqlite::params![LANE_SNAPSHOT_FORMAT, lane.wire_name()],
        )?;
        for table in ownership::snapshot_tables_for_lane(lane) {
            let cols: Vec<String> = {
                let stmt = conn.prepare(&format!("SELECT * FROM \"{table}\" LIMIT 0;"))?;
                stmt.column_names().into_iter().map(str::to_owned).collect()
            };
            let quoted: Vec<String> = cols.iter().map(|c| format!("\"{c}\"")).collect();
            snap.execute(
                &format!("CREATE TABLE \"{table}\" ({});", quoted.join(", ")),
                [],
            )?;
            snap.execute(
                &format!("INSERT INTO {LANE_SNAPSHOT_TABLES_TABLE} (name) VALUES (?1);"),
                [table],
            )?;
            let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!("?{i}")).collect();
            let mut insert = snap.prepare(&format!(
                "INSERT INTO \"{table}\" VALUES ({});",
                placeholders.join(", ")
            ))?;
            let mut read = conn.prepare(&format!("SELECT * FROM \"{table}\";"))?;
            let mut rows = read.query([])?;
            while let Some(row) = rows.next()? {
                let values: Vec<rusqlite::types::Value> = (0..cols.len())
                    .map(|i| row.get(i))
                    .collect::<Result<_, _>>()?;
                insert.execute(rusqlite::params_from_iter(values))?;
            }
        }
        // The dumped tables' AUTOINCREMENT high-water marks (see
        // LANE_SNAPSHOT_SEQUENCES_TABLE): sqlite_sequence is not a lane table,
        // so it would otherwise never reach the installing replica.
        snap.execute(
            &format!(
                "CREATE TABLE {LANE_SNAPSHOT_SEQUENCES_TABLE} \
                 (name TEXT PRIMARY KEY, seq INTEGER NOT NULL);"
            ),
            [],
        )?;
        let has_sequences: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' \
             AND name = 'sqlite_sequence');",
            [],
            |row| row.get(0),
        )?;
        if has_sequences {
            let mut insert = snap.prepare(&format!(
                "INSERT INTO {LANE_SNAPSHOT_SEQUENCES_TABLE} (name, seq) VALUES (?1, ?2);"
            ))?;
            for table in ownership::snapshot_tables_for_lane(lane) {
                let seq: Option<i64> = conn
                    .query_row(
                        "SELECT seq FROM sqlite_sequence WHERE name = ?1;",
                        [table],
                        |row| row.get(0),
                    )
                    .optional()?;
                if let Some(seq) = seq {
                    insert.execute(rusqlite::params![table, seq])?;
                }
            }
        }
        snap.execute_batch("COMMIT;")?;
        Ok(())
    }

    fn build(&self) -> R<Snapshot<TypeConfig>> {
        std::fs::create_dir_all(&self.snapshots_dir).with_context(|| {
            format!(
                "failed creating snapshots dir {}",
                self.snapshots_dir.display()
            )
        })?;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let tmp = self.snapshots_dir.join(format!("build-{nanos}.db.tmp"));
        let _ = std::fs::remove_file(&tmp);
        let tmp_str = tmp
            .to_str()
            .context("snapshot temp path is not utf-8")?
            .to_string();

        // The image and its meta come from ONE consistent read of the state
        // DB on a dedicated read-only connection, so writes keep flowing
        // while a large image is built (R5). In WAL mode a reader sees only
        // committed transactions: every apply commits its rows and the
        // lane's `raft_applied` marker together, and a capture's savepoint
        // never commits at all, so the image always matches the marker read
        // with it. For `WholeDb` the marker travels inside the image and is
        // read back from it; for `LaneTables` the marker and the dump share
        // one read transaction, and install writes the marker into the live
        // DB from the meta.
        let lane = self.lane;
        let scope = self.scope;
        let source = Connection::open_with_flags(
            self.store.state_db_path(),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .context("failed opening the state db for a snapshot build")?;
        let (last_log_id, last_membership) = match scope {
            SnapshotScope::WholeDb => {
                // VACUUM INTO is itself one read transaction.
                source
                    .execute("VACUUM INTO ?1;", [&tmp_str])
                    .with_context(|| format!("failed running VACUUM INTO {}", tmp.display()))?;
                drop(source);
                let image =
                    Connection::open_with_flags(&tmp, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                        .with_context(|| format!("failed opening snapshot {}", tmp.display()))?;
                StoreStateMachine::read_applied_row(&image, lane)?
            }
            SnapshotScope::LaneTables => {
                source.execute_batch("BEGIN;")?;
                let dumped =
                    StoreStateMachine::read_applied_row(&source, lane).and_then(|applied| {
                        Self::dump_lane_tables(&source, lane, &tmp)?;
                        Ok(applied)
                    });
                let _ = source.execute_batch("COMMIT;");
                dumped?
            }
        };
        fsync_file(&tmp)?;

        let id = format!(
            "{}-{}-{}",
            lane.wire_name(),
            last_log_id
                .map(|l| format!("{}-{}", l.leader_id, l.index))
                .unwrap_or_else(|| "none".to_string()),
            nanos
        );
        let id = sanitize_snapshot_id(&id);
        let final_db = self.snapshots_dir.join(format!("{id}.db"));
        std::fs::rename(&tmp, &final_db)
            .with_context(|| format!("failed finalizing snapshot {}", final_db.display()))?;
        let meta = SnapshotMeta {
            last_log_id,
            last_membership,
            snapshot_id: id.clone(),
        };
        // The sidecar lands last: a snapshot without its sidecar is
        // incomplete and invisible to `get_current_snapshot`.
        write_meta_sidecar(&self.snapshots_dir, &id, &meta)?;
        prune_snapshots(&self.snapshots_dir);

        let data = std::fs::read(&final_db)
            .with_context(|| format!("failed reading snapshot image {}", final_db.display()))?;
        info!(
            lane = lane.wire_name(),
            snapshot_id = %id,
            last_log_id = ?meta.last_log_id,
            bytes = data.len(),
            scope = ?scope,
            "Built raft snapshot"
        );
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StoreSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // Off the async worker threads (module docs "sync-inside-async"): a
        // build is a multi-second VACUUM/dump on a large state DB.
        // openraft already runs this in a detached task; putting it on the
        // blocking pool keeps that task from occupying a worker thread.
        let builder = self.clone();
        tokio::task::spawn_blocking(move || builder.build())
            .await
            .map_err(|e| -> StorageError<NodeId> {
                StorageIOError::write_snapshot(
                    None,
                    AnyError::error(format!("snapshot build task failed: {e}")),
                )
                .into()
            })?
            .map_err(|e| {
                StorageIOError::write_snapshot(None, AnyError::error(format!("{e:#}"))).into()
            })
    }
}

/// Restrict a snapshot id to filesystem-safe characters.
fn sanitize_snapshot_id(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn fsync_file(path: &Path) -> R<()> {
    std::fs::File::open(path)
        .and_then(|f| f.sync_all())
        .with_context(|| format!("failed fsyncing {}", path.display()))
}

/// Writes `<id>.meta.json` atomically (tmp + rename) and fsyncs the dir so
/// a finished snapshot survives a crash.
fn write_meta_sidecar(dir: &Path, id: &str, meta: &SnapshotMeta<NodeId, BasicNode>) -> R<()> {
    let tmp = dir.join(format!("{id}.meta.json.tmp"));
    let final_path = dir.join(format!("{id}.meta.json"));
    let bytes = serde_json::to_vec_pretty(meta).context("failed encoding snapshot meta")?;
    std::fs::write(&tmp, bytes)
        .with_context(|| format!("failed writing snapshot meta {}", tmp.display()))?;
    fsync_file(&tmp)?;
    std::fs::rename(&tmp, &final_path)
        .with_context(|| format!("failed finalizing snapshot meta {}", final_path.display()))?;
    #[cfg(unix)]
    if let Ok(dir_file) = std::fs::File::open(dir) {
        let _ = dir_file.sync_all();
    }
    Ok(())
}

/// All complete snapshots in `dir` (meta sidecar present AND image
/// present), sorted ascending by applied log id.
fn list_snapshots(dir: &Path) -> R<Vec<(SnapshotMeta<NodeId, BasicNode>, PathBuf)>> {
    let mut found = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        // No dir yet == no snapshots.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(found),
        Err(e) => {
            return Err(e)
                .with_context(|| format!("failed listing snapshots dir {}", dir.display()))
        }
    };
    for entry in entries {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(id) = name.strip_suffix(".meta.json") else {
            continue;
        };
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let Ok(meta) = serde_json::from_slice::<SnapshotMeta<NodeId, BasicNode>>(&bytes) else {
            warn!(path = %path.display(), "Skipping undecodable snapshot meta sidecar");
            continue;
        };
        let db_path = dir.join(format!("{id}.db"));
        if db_path.exists() {
            found.push((meta, db_path));
        }
    }
    found.sort_by_key(|(meta, _)| meta.last_log_id);
    Ok(found)
}

/// The newest complete snapshot in `dir`, by applied log id.
fn newest_snapshot(dir: &Path) -> R<Option<(SnapshotMeta<NodeId, BasicNode>, PathBuf)>> {
    Ok(list_snapshots(dir)?.pop())
}

/// Deletes all but the newest [`SNAPSHOTS_RETAINED`] snapshots (and any
/// leftover `.tmp` staging files). Best-effort: pruning failures only warn —
/// stale snapshots waste disk but harm nothing.
fn prune_snapshots(dir: &Path) {
    let snapshots = match list_snapshots(dir) {
        Ok(snapshots) => snapshots,
        Err(_) => return,
    };
    if snapshots.len() > SNAPSHOTS_RETAINED {
        for (meta, db_path) in &snapshots[..snapshots.len() - SNAPSHOTS_RETAINED] {
            let _ = std::fs::remove_file(db_path);
            let _ = std::fs::remove_file(dir.join(format!(
                "{}.meta.json",
                sanitize_snapshot_id(&meta.snapshot_id)
            )));
        }
    }
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "tmp") {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{KvValueType, ManagerConfigFull};
    use openraft::{CommittedLeaderId, Membership};
    use std::collections::{BTreeMap, BTreeSet};
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("khepri-raft-sm-")
            .tempdir_in(crate::test_scratch::root())
            .context("Error creating temp store directory")?
            .keep();
        let mut store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("Error parsing store path")?
                .to_string(),
            ..Default::default()
        };
        // Snapshots built on one store install on another: replicas of one
        // cluster share a root key (docs/kv-encryption.md).
        store_config.encryption.root_key.path =
            Some(crate::storage::keyring::test_support::shared_root_key_path());
        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    fn snapshots_dir() -> PathBuf {
        Builder::new()
            .prefix("khepri-raft-snap-")
            .tempdir_in(crate::test_scratch::root())
            .expect("temp snapshots dir")
            .keep()
    }

    fn state_machine(lane: Lane, store: Store) -> StoreStateMachine {
        StoreStateMachine::new(lane, store, snapshots_dir(), SnapshotScope::WholeDb)
    }

    fn lane_state_machine(lane: Lane, store: Store) -> StoreStateMachine {
        StoreStateMachine::new(lane, store, snapshots_dir(), SnapshotScope::LaneTables)
    }

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    fn kv_value(store: &Store, key: &str) -> R<Option<String>> {
        Ok(store.get_kv(key)?.map(|entry| entry.value))
    }

    #[tokio::test]
    async fn blank_slate_reports_none_and_default_membership() -> R<()> {
        let store = setup_store()?;
        let mut sm = state_machine(Lane::Control, store);
        let (applied, membership) = sm.applied_state().await.unwrap();
        assert_eq!(applied, None, "fresh node has applied nothing");
        assert_eq!(membership, StoredMembership::default());
        assert!(
            sm.get_current_snapshot().await.unwrap().is_none(),
            "fresh node has no snapshot"
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_replays_a_real_captured_changeset_and_advances_applied_state() -> R<()> {
        let leader = setup_store()?;
        let follower = setup_store()?;
        // A follower has applied the leader's `initialize_keyring` entry
        // before any KV entry: same data keys (values replicate encrypted).
        follower.adopt_keyring_from(&leader)?;

        // Capture a real mutation on the leader (rolled back locally).
        let (kv, changeset) = capture::capture_mutation(&leader, Lane::Control, |s| {
            s.put_kv("cfg/answer", "42", KvValueType::String, None, None)
        })?;
        assert_eq!(kv.version, 1);
        assert_eq!(kv_value(&leader, "cfg/answer")?, None, "leader rolled back");

        let entry = Entry {
            log_id: log_id(1, 5),
            payload: EntryPayload::Normal(crate::storage::raft::StoreCommand {
                lane: Lane::Control,
                op: "put_kv".to_string(),
                changeset,
            }),
        };

        // The follower's state machine applies the committed entry.
        let mut sm = state_machine(Lane::Control, follower.clone());
        let replies = sm.apply([entry]).await.unwrap();
        assert_eq!(replies.len(), 1);

        assert_eq!(
            kv_value(&follower, "cfg/answer")?.as_deref(),
            Some("42"),
            "changeset applied to follower state"
        );
        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 5)), "applied marker advanced");

        // A fresh state machine over the same store (simulated restart)
        // reads the same applied state back from the DB.
        let mut reopened = state_machine(Lane::Control, follower);
        let (applied, _) = reopened.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 5)), "applied state is durable");
        Ok(())
    }

    #[tokio::test]
    async fn blank_and_membership_entries_advance_and_persist() -> R<()> {
        let store = setup_store()?;
        let mut sm = state_machine(Lane::Control, store.clone());

        let blank = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Blank,
        };
        let mut nodes = BTreeMap::new();
        nodes.insert(1u64, BasicNode::new("m1@10.0.0.1:4646"));
        let membership = Membership::new(vec![BTreeSet::from([1u64])], nodes);
        let member_entry = Entry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Membership(membership.clone()),
        };
        sm.apply([blank, member_entry]).await.unwrap();

        let (applied, stored) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 2)));
        assert_eq!(stored.log_id(), &Some(log_id(1, 2)));
        assert_eq!(stored.membership(), &membership);

        // A later normal entry advances the marker but keeps the stored
        // membership (membership column untouched by non-membership writes).
        let ((), changeset) = capture::capture_mutation(&store, Lane::Control, |s| {
            s.put_kv("k", "v", KvValueType::String, None, None)?;
            Ok(())
        })?;
        let normal = Entry {
            log_id: log_id(1, 3),
            payload: EntryPayload::Normal(crate::storage::raft::StoreCommand {
                lane: Lane::Control,
                op: "put_kv".to_string(),
                changeset,
            }),
        };
        sm.apply([normal]).await.unwrap();
        let (applied, stored) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 3)));
        assert_eq!(stored.membership(), &membership, "membership preserved");
        Ok(())
    }

    #[tokio::test]
    async fn changeset_and_applied_marker_commit_atomically() -> R<()> {
        let store = setup_store()?;
        let mut sm = state_machine(Lane::Control, store.clone());

        let ((), changeset) = capture::capture_mutation(&store, Lane::Control, |s| {
            s.put_kv("atomic", "yes", KvValueType::String, None, None)?;
            Ok(())
        })?;

        // Simulated failure AFTER the changeset rows are staged but BEFORE
        // the transaction commits (exactly where a crash between "apply
        // data" and "record applied" would land if they were separate
        // transactions): the shared savepoint must roll back BOTH.
        let err = capture::apply_changeset_with(&store, &changeset, |conn| {
            // The changeset's rows are visible inside the savepoint...
            let staged: i64 = conn.query_row(
                "SELECT count(*) FROM kv_entry WHERE key = 'atomic';",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(staged, 1, "changeset rows staged inside the savepoint");
            bail!("simulated crash before commit")
        })
        .unwrap_err();
        assert!(err.to_string().contains("simulated crash"));

        assert_eq!(
            kv_value(&store, "atomic")?,
            None,
            "data rolled back with the failed marker write"
        );
        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(applied, None, "applied marker rolled back with the data");

        // The successful path lands both together.
        let entry = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(crate::storage::raft::StoreCommand {
                lane: Lane::Control,
                op: "put_kv".to_string(),
                changeset,
            }),
        };
        sm.apply([entry]).await.unwrap();
        assert_eq!(kv_value(&store, "atomic")?.as_deref(), Some("yes"));
        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 1)));
        Ok(())
    }

    #[tokio::test]
    async fn lanes_track_applied_state_independently() -> R<()> {
        let store = setup_store()?;
        let mut control = state_machine(Lane::Control, store.clone());
        let mut status = state_machine(Lane::Status, store);

        control
            .apply([Entry {
                log_id: log_id(1, 9),
                payload: EntryPayload::Blank,
            }])
            .await
            .unwrap();

        let (applied, _) = control.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 9)));
        let (applied, _) = status.applied_state().await.unwrap();
        assert_eq!(applied, None, "other lanes unaffected");
        Ok(())
    }

    /// The A-3 headline: build a real `VACUUM INTO` snapshot on one node,
    /// install it on another, and the receiver's state + applied marker
    /// match the snapshot exactly (the marker travels inside the image).
    /// A snapshot build reads on its own connection and takes no lock that
    /// writes need (R5): it completes while the capture mutex is held, and
    /// its meta still matches the applied marker inside the image.
    #[tokio::test]
    async fn snapshot_build_does_not_block_writes() -> R<()> {
        for lane_scoped in [false, true] {
            let store = setup_store()?;
            let mut sm = if lane_scoped {
                lane_state_machine(Lane::Control, store.clone())
            } else {
                state_machine(Lane::Control, store.clone())
            };
            let ((), changeset) = capture::capture_mutation(&store, Lane::Control, |s| {
                s.put_kv("r5/key", "v", KvValueType::String, None, None)?;
                Ok(())
            })?;
            sm.apply([Entry {
                log_id: log_id(1, 1),
                payload: EntryPayload::Normal(crate::storage::raft::StoreCommand {
                    lane: Lane::Control,
                    op: "put_kv".to_string(),
                    changeset,
                }),
            }])
            .await
            .unwrap();

            // Hold the lock every capture and apply takes, as a long write
            // would, and build on another thread. Taken after the (async)
            // builder handoff so no guard is held across an await.
            let builder = sm.get_snapshot_builder().await;
            let held = capture::capture_exclusion()?;
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = tx.send(builder.build().map(|snapshot| snapshot.meta));
            });
            let meta = rx
                .recv_timeout(std::time::Duration::from_secs(30))
                .expect("a snapshot build must not wait on the capture mutex")?;
            drop(held);
            assert_eq!(meta.last_log_id, Some(log_id(1, 1)));
        }
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_build_and_install_round_trip() -> R<()> {
        let leader = setup_store()?;
        let mut leader_sm = state_machine(Lane::Control, leader.clone());

        // Two applied entries on the leader.
        for (i, (key, value)) in [("cfg/a", "1"), ("cfg/b", "2")].iter().enumerate() {
            let ((), changeset) = capture::capture_mutation(&leader, Lane::Control, |s| {
                s.put_kv(key, value, KvValueType::String, None, None)?;
                Ok(())
            })?;
            leader_sm
                .apply([Entry {
                    log_id: log_id(1, (i + 1) as u64),
                    payload: EntryPayload::Normal(crate::storage::raft::StoreCommand {
                        lane: Lane::Control,
                        op: "put_kv".to_string(),
                        changeset,
                    }),
                }])
                .await
                .unwrap();
        }

        // Build: meta matches applied state; snapshot is served back.
        let mut builder = leader_sm.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        assert_eq!(snapshot.meta.last_log_id, Some(log_id(1, 2)));
        let served = leader_sm
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("newest snapshot is served");
        assert_eq!(served.meta.snapshot_id, snapshot.meta.snapshot_id);
        assert_eq!(
            served.snapshot.get_ref(),
            snapshot.snapshot.get_ref(),
            "served bytes equal built bytes"
        );

        // Install on a follower that has its own unrelated state (which the
        // whole-DB install must replace).
        let follower = setup_store()?;
        follower.put_kv("stale", "gone", KvValueType::String, None, None)?;
        let mut follower_sm = state_machine(Lane::Control, follower.clone());
        let mut receiving = follower_sm.begin_receiving_snapshot().await.unwrap();
        *receiving = Cursor::new(snapshot.snapshot.get_ref().clone());
        follower_sm
            .install_snapshot(&snapshot.meta, receiving)
            .await
            .unwrap();

        assert_eq!(kv_value(&follower, "cfg/a")?.as_deref(), Some("1"));
        assert_eq!(kv_value(&follower, "cfg/b")?.as_deref(), Some("2"));
        assert_eq!(
            kv_value(&follower, "stale")?,
            None,
            "pre-install state fully replaced"
        );
        let (applied, _) = follower_sm.applied_state().await.unwrap();
        assert_eq!(
            applied,
            Some(log_id(1, 2)),
            "applied marker adopted from the snapshot meta"
        );
        // Replicated content converged (raft_applied excluded from hash).
        assert_eq!(
            leader.state_content_hash()?,
            follower.state_content_hash()?,
            "post-install replicas are byte-identical"
        );
        // The receiver also serves the installed snapshot from now on.
        let reserved = follower_sm
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("installed snapshot is retained and served");
        assert_eq!(reserved.meta.snapshot_id, snapshot.meta.snapshot_id);
        // And the store keeps working after the connection swap.
        follower.put_kv("post-install", "ok", KvValueType::String, None, None)?;
        assert_eq!(kv_value(&follower, "post-install")?.as_deref(), Some("ok"));
        Ok(())
    }

    /// The §7-C3 headline: a lane snapshot is a table-scoped dump — build
    /// one on a control-lane state machine, install it on another store,
    /// and (1) the control-owned tables are replaced exactly, (2) tables
    /// owned by OTHER lanes on the receiver are untouched (the clobber a
    /// whole-DB image would have caused), (3) the applied marker is adopted
    /// atomically with the rows.
    #[tokio::test]
    async fn lane_snapshot_install_replaces_only_the_owned_tables() -> R<()> {
        let src = setup_store()?;
        let mut src_sm = lane_state_machine(Lane::Control, src.clone());

        for (i, (key, value)) in [("cfg/a", "1"), ("cfg/b", "2")].iter().enumerate() {
            let ((), changeset) = capture::capture_mutation(&src, Lane::Control, |s| {
                s.put_kv(key, value, KvValueType::String, None, None)?;
                Ok(())
            })?;
            src_sm
                .apply([Entry {
                    log_id: log_id(1, (i + 1) as u64),
                    payload: EntryPayload::Normal(crate::storage::raft::StoreCommand {
                        lane: Lane::Control,
                        op: "put_kv".to_string(),
                        changeset,
                    }),
                }])
                .await
                .unwrap();
        }

        let mut builder = src_sm.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        assert_eq!(snapshot.meta.last_log_id, Some(log_id(1, 2)));
        // The image is a lane dump, not a whole-DB image: it carries a
        // manifest and only control-snapshot tables.
        {
            let img = snapshots_dir().join("probe.db");
            std::fs::write(&img, snapshot.snapshot.get_ref())?;
            let conn = Connection::open(&img)?;
            let lane: String = conn.query_row(
                &format!("SELECT value FROM {LANE_SNAPSHOT_META_TABLE} WHERE key = 'lane';"),
                [],
                |row| row.get(0),
            )?;
            assert_eq!(lane, "control");
            let has_scheduler_table: i64 = conn.query_row(
                &format!(
                    "SELECT COUNT(*) FROM {LANE_SNAPSHOT_TABLES_TABLE} \
                     WHERE name IN ('allocation', 'plan_submission', 'node_health');"
                ),
                [],
                |row| row.get(0),
            )?;
            assert_eq!(
                has_scheduler_table, 0,
                "control dump must not list sibling lanes' tables"
            );
        }

        // Receiver: stale control state (must be replaced) plus
        // scheduler/status-lane rows the SOURCE does not have (must
        // survive — this is exactly what a whole-DB install would destroy).
        let dst = setup_store()?;
        dst.put_kv("stale", "gone", KvValueType::String, None, None)?;
        {
            let conn = dst.write_connection()?;
            conn.execute(
                "INSERT INTO plan_submission (job_config_id, mode, placements, expected_epoch,
                                              status, created_at)
                 VALUES ('jc-keep', 'fresh', '[]', 0, 'pending', '2026-01-01T00:00:00Z');",
                [],
            )?;
            conn.execute(
                "INSERT INTO node_health (node_id, status, last_heartbeat, lost_at, health_checks)
                 VALUES ('node-keep', 'active', '2026-01-01T00:00:00Z', NULL, '{}');",
                [],
            )?;
        }

        let mut dst_sm = lane_state_machine(Lane::Control, dst.clone());
        let mut receiving = dst_sm.begin_receiving_snapshot().await.unwrap();
        *receiving = Cursor::new(snapshot.snapshot.get_ref().clone());
        dst_sm
            .install_snapshot(&snapshot.meta, receiving)
            .await
            .unwrap();

        assert_eq!(kv_value(&dst, "cfg/a")?.as_deref(), Some("1"));
        assert_eq!(kv_value(&dst, "cfg/b")?.as_deref(), Some("2"));
        assert_eq!(
            kv_value(&dst, "stale")?,
            None,
            "owned tables fully replaced"
        );
        {
            let conn = dst.read_connection()?;
            let plans: i64 =
                conn.query_row("SELECT COUNT(*) FROM plan_submission;", [], |r| r.get(0))?;
            assert_eq!(
                plans, 1,
                "scheduler-lane table untouched by control install"
            );
            let health: i64 = conn.query_row(
                "SELECT COUNT(*) FROM node_health WHERE node_id = 'node-keep';",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(health, 1, "status-lane table untouched by control install");
        }
        let (applied, _) = dst_sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 2)), "applied marker adopted");
        // The receiver serves the installed snapshot and keeps working.
        let served = dst_sm
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("installed snapshot is retained and served");
        assert_eq!(served.meta.snapshot_id, snapshot.meta.snapshot_id);
        dst.put_kv("post-install", "ok", KvValueType::String, None, None)?;
        assert_eq!(kv_value(&dst, "post-install")?.as_deref(), Some("ok"));
        Ok(())
    }

    /// Insert one allocation row with an implicit id, returning the id the
    /// store handed out.
    fn insert_allocation_row(store: &Store) -> R<i64> {
        let conn = store.write_connection()?;
        conn.execute(
            "INSERT INTO allocation (status, resources, node_id, job_config_id, task_config_id)
             VALUES ('placed', '{}', 'n1', 'jc-1', 1);",
            [],
        )?;
        Ok(conn.last_insert_rowid())
    }

    fn delete_allocation_row(store: &Store, id: i64) -> R<()> {
        store
            .write_connection()?
            .execute("DELETE FROM allocation WHERE id = ?1;", [id])?;
        Ok(())
    }

    /// P1-7 regression: allocation ids are never reused across a snapshot
    /// install. The leader hands out 1..=3 and deletes 2 and 3; a replica
    /// that catches up from a snapshot (and may lead next) must hand out 4,
    /// not 2 — a reused id points a client's stale id at a different job's
    /// task, and mixes the old allocation's output into the new one's logs
    /// on the worker. Both snapshot scopes: whole-DB (`single`) and lane
    /// dump (`lanes`), whose row copy alone would re-seed the sequence to
    /// the largest surviving id.
    #[tokio::test]
    async fn snapshot_install_never_reissues_a_deleted_allocation_id() -> R<()> {
        for scope in [SnapshotScope::WholeDb, SnapshotScope::LaneTables] {
            let src = setup_store()?;
            assert_eq!(insert_allocation_row(&src)?, 1);
            assert_eq!(insert_allocation_row(&src)?, 2);
            assert_eq!(insert_allocation_row(&src)?, 3);
            delete_allocation_row(&src, 3)?;
            delete_allocation_row(&src, 2)?;
            // Deletes never lower the mark on the store itself.
            assert_eq!(insert_allocation_row(&src)?, 4, "{scope:?}: local delete");
            delete_allocation_row(&src, 4)?;

            let mut src_sm =
                StoreStateMachine::new(Lane::Scheduler, src.clone(), snapshots_dir(), scope);
            let mut builder = src_sm.get_snapshot_builder().await;
            let snapshot = builder.build_snapshot().await.unwrap();

            let dst = setup_store()?;
            let mut dst_sm =
                StoreStateMachine::new(Lane::Scheduler, dst.clone(), snapshots_dir(), scope);
            let mut receiving = dst_sm.begin_receiving_snapshot().await.unwrap();
            *receiving = Cursor::new(snapshot.snapshot.get_ref().clone());
            dst_sm
                .install_snapshot(&snapshot.meta, receiving)
                .await
                .unwrap();

            let next = insert_allocation_row(&dst)?;
            assert_eq!(
                next, 5,
                "{scope:?}: the installing replica must continue after the leader's \
                 high-water mark, not after the largest surviving id"
            );
            // A mark the receiver already had above the leader's is kept.
            assert!(insert_allocation_row(&dst)? > next);
        }
        Ok(())
    }

    /// P1-7 regression, the log path: a follower that applied the leader's
    /// changesets (inserts with explicit ids, then deletes of the newest
    /// rows) and then wins an election hands out the next id after the
    /// leader's high-water mark. Changeset inserts carry explicit ids, which
    /// raise the follower's AUTOINCREMENT mark exactly as the leader's own
    /// insert did; deletes never lower it.
    #[tokio::test]
    async fn a_new_leader_never_reissues_a_deleted_allocation_id() -> R<()> {
        let leader = setup_store()?;
        let follower = setup_store()?;
        let mut leader_sm = state_machine(Lane::Scheduler, leader.clone());
        let mut follower_sm = state_machine(Lane::Scheduler, follower.clone());
        let mut index = 0;
        let mut replicate = |changeset: Vec<u8>| {
            index += 1;
            let entry = Entry {
                log_id: log_id(1, index),
                payload: EntryPayload::Normal(crate::storage::raft::StoreCommand {
                    lane: Lane::Scheduler,
                    op: "place_alloc".to_string(),
                    changeset,
                }),
            };
            (entry.clone(), entry)
        };

        for expected in 1..=3 {
            let (id, changeset) =
                capture::capture_mutation(&leader, Lane::Scheduler, insert_allocation_row)?;
            assert_eq!(id, expected);
            let (a, b) = replicate(changeset);
            leader_sm.apply([a]).await.unwrap();
            follower_sm.apply([b]).await.unwrap();
        }
        for id in [3, 2] {
            let ((), changeset) = capture::capture_mutation(&leader, Lane::Scheduler, |s| {
                delete_allocation_row(s, id)
            })?;
            let (a, b) = replicate(changeset);
            leader_sm.apply([a]).await.unwrap();
            follower_sm.apply([b]).await.unwrap();
        }

        // Leadership moves: the follower proposes the next placement.
        let (id, _) = capture::capture_mutation(&follower, Lane::Scheduler, insert_allocation_row)?;
        assert_eq!(id, 4, "the new leader must not reissue a deleted id");
        // And a restart of either replica keeps the mark (it lives in the
        // state DB, in the same file as the rows).
        let (id, _) = capture::capture_mutation(&leader, Lane::Scheduler, insert_allocation_row)?;
        assert_eq!(id, 4);
        Ok(())
    }

    /// Adversarial regression (commutative tables vs. lane snapshot
    /// install): the control lane's snapshot carries the commutative tables
    /// (JC-28), but the RECEIVER may already hold commutative rows applied
    /// from SIBLING-lane log entries that the snapshot source never had —
    /// those entries sit at or below the receiver's untouched sibling-lane
    /// applied markers and are never replayed, so a delete-and-replace
    /// install would erase them permanently. Install must be additive for
    /// commutative tables: dump rows land with INSERT OR IGNORE, existing
    /// rows survive.
    #[tokio::test]
    async fn lane_snapshot_install_preserves_existing_commutative_rows() -> R<()> {
        let src = setup_store()?;
        let mut src_sm = lane_state_machine(Lane::Control, src.clone());

        // The source's snapshot carries one control-lane event (id 777).
        let ((), changeset) = capture::capture_mutation(&src, Lane::Control, |s| {
            s.put_kv("k", "v", KvValueType::String, None, None)?;
            let conn = s.write_connection()?;
            conn.execute(
                "INSERT INTO scheduler_event (id, kind, level, message, metadata_json, created_at)
                 VALUES (777, 'src.kind', 'info', 'from-source', '{}', '2026-01-01T00:00:00Z');",
                [],
            )?;
            Ok(())
        })?;
        src_sm
            .apply([Entry {
                log_id: log_id(1, 1),
                payload: EntryPayload::Normal(crate::storage::raft::StoreCommand {
                    lane: Lane::Control,
                    op: "put_kv".to_string(),
                    changeset,
                }),
            }])
            .await
            .unwrap();
        let mut builder = src_sm.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();

        // The receiver already applied a STATUS-lane event (id 999) the
        // source does not have, plus a duplicate of the dump's own row.
        let dst = setup_store()?;
        {
            let conn = dst.write_connection()?;
            conn.execute(
                "INSERT INTO scheduler_event (id, kind, level, message, metadata_json, created_at)
                 VALUES (999, 'status.kind', 'info', 'from-status-lane', '{}',
                         '2026-01-01T00:00:00Z'),
                        (777, 'src.kind', 'info', 'from-source', '{}',
                         '2026-01-01T00:00:00Z');",
                [],
            )?;
        }
        // A receiver-only row in an OWNED (non-commutative) control table:
        // the other half of the rule — owned tables keep replace semantics,
        // so this one must NOT survive.
        dst.put_kv("stale-owned", "gone", KvValueType::String, None, None)?;
        let mut dst_sm = lane_state_machine(Lane::Control, dst.clone());
        dst_sm
            .install_snapshot(
                &snapshot.meta,
                Box::new(Cursor::new(snapshot.snapshot.get_ref().clone())),
            )
            .await
            .unwrap();

        let conn = dst.read_connection()?;
        let sibling: i64 = conn.query_row(
            "SELECT COUNT(*) FROM scheduler_event WHERE id = 999;",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(
            sibling, 1,
            "a sibling lane's already-applied commutative row must survive the control \
             lane's snapshot install"
        );
        let dumped: i64 = conn.query_row(
            "SELECT COUNT(*) FROM scheduler_event WHERE id = 777;",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(dumped, 1, "the dump's row lands once (INSERT OR IGNORE)");
        drop(conn);
        assert!(
            dst.get_kv("stale-owned")?.is_none(),
            "an OWNED table's receiver-only row must be replaced away by the install — merge \
             semantics are for the commutative class only"
        );
        assert_eq!(
            dst.get_kv("k")?.map(|entry| entry.value).as_deref(),
            Some("v"),
            "the dump's owned-table rows must land"
        );
        Ok(())
    }

    /// A lane state machine refuses a foreign lane's dump and a whole-DB
    /// image (no manifest) instead of clobbering tables it does not own.
    #[tokio::test]
    async fn lane_snapshot_install_refuses_mismatched_images() -> R<()> {
        let src = setup_store()?;
        let mut control_sm = lane_state_machine(Lane::Control, src.clone());
        let ((), changeset) = capture::capture_mutation(&src, Lane::Control, |s| {
            s.put_kv("k", "v", KvValueType::String, None, None)?;
            Ok(())
        })?;
        control_sm
            .apply([Entry {
                log_id: log_id(1, 1),
                payload: EntryPayload::Normal(crate::storage::raft::StoreCommand {
                    lane: Lane::Control,
                    op: "put_kv".to_string(),
                    changeset,
                }),
            }])
            .await
            .unwrap();
        let mut builder = control_sm.get_snapshot_builder().await;
        let control_snapshot = builder.build_snapshot().await.unwrap();

        // Control dump offered to a STATUS lane state machine: refused.
        let dst = setup_store()?;
        let mut status_sm = lane_state_machine(Lane::Status, dst.clone());
        let err = status_sm
            .install_snapshot(
                &control_snapshot.meta,
                Box::new(Cursor::new(control_snapshot.snapshot.get_ref().clone())),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("lane `control`"),
            "foreign-lane dump must be refused: {err}"
        );

        // A whole-DB image (VACUUM INTO — no lane manifest) offered to a
        // lane state machine: refused, not installed wholesale.
        let mut whole_sm = state_machine(Lane::Control, src.clone());
        let mut whole_builder = whole_sm.get_snapshot_builder().await;
        let whole = whole_builder.build_snapshot().await.unwrap();
        let mut lane_sm = lane_state_machine(Lane::Control, dst);
        let err = lane_sm
            .install_snapshot(
                &whole.meta,
                Box::new(Cursor::new(whole.snapshot.get_ref().clone())),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not a lane-table dump"),
            "whole-DB image must be refused by a lane state machine: {err}"
        );
        Ok(())
    }

    /// Adversarial regression (2026-07-25 sweep): a dump whose manifest
    /// lists a table THIS node's ownership map assigns to another lane is
    /// the signature of a member that reclassified the table. The refusal
    /// must say so and name the only supported path (JC-43: back up → fresh
    /// cluster → restore), because a lane move is not a rolling upgrade.
    #[tokio::test]
    async fn lane_snapshot_manifest_mismatch_names_the_fresh_cluster_path() -> R<()> {
        let src = setup_store()?;
        let mut control_sm = lane_state_machine(Lane::Control, src.clone());
        let ((), changeset) = capture_put_kv(&src, "skew/key", "v")?;
        control_sm
            .apply([Entry {
                log_id: log_id(1, 1),
                payload: EntryPayload::Normal(crate::storage::raft::StoreCommand {
                    lane: Lane::Control,
                    op: "put_kv".to_string(),
                    changeset,
                }),
            }])
            .await
            .unwrap();
        let mut builder = control_sm.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();

        // Doctor the manifest the way a version that had moved `eval` onto
        // the control lane would have written it.
        let dir = Builder::new().prefix("legion-lane-skew-").tempdir()?;
        let doctored = dir.path().join("skewed-snapshot.db");
        std::fs::write(&doctored, snapshot.snapshot.get_ref())?;
        {
            let conn = Connection::open(&doctored)?;
            conn.execute(
                &format!("INSERT INTO {LANE_SNAPSHOT_TABLES_TABLE} (name) VALUES ('eval');"),
                [],
            )?;
        }
        let bytes = std::fs::read(&doctored)?;

        let dst = setup_store()?;
        let mut dst_sm = lane_state_machine(Lane::Control, dst);
        let err = dst_sm
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(bytes)))
            .await
            .unwrap_err();
        let rendered = err.to_string();
        for needle in [
            "eval",
            "NOT a rolling upgrade",
            "JC-43",
            "legion state backup",
            "--restore-from",
        ] {
            assert!(
                rendered.contains(needle),
                "reclassification refusal must mention `{needle}`: {rendered}"
            );
        }
        Ok(())
    }

    /// Apply-side index-skip guard: entries at or below the recorded
    /// applied index (re-delivered around a snapshot install or restart
    /// replay) are skipped instead of conflict-aborting on their own
    /// already-applied effects.
    #[tokio::test]
    async fn apply_skips_entries_at_or_below_the_applied_index() -> R<()> {
        let store = setup_store()?;
        let mut sm = state_machine(Lane::Control, store.clone());

        let ((), changeset) = capture_put_kv(&store, "skip/key", "v1")?;
        let entry = Entry {
            log_id: log_id(1, 5),
            payload: EntryPayload::Normal(crate::storage::raft::StoreCommand {
                lane: Lane::Control,
                op: "put_kv".to_string(),
                changeset,
            }),
        };
        sm.apply([entry.clone()]).await.unwrap();
        assert_eq!(kv_value(&store, "skip/key")?.as_deref(), Some("v1"));

        // Re-delivery of the same entry: skipped, not conflict-aborted.
        sm.apply([entry]).await.unwrap();
        assert_eq!(kv_value(&store, "skip/key")?.as_deref(), Some("v1"));

        // An older entry (below the applied index) is skipped too, and a
        // newer one still applies.
        let ((), old_changeset) = capture_put_kv(&store, "skip/old", "old")?;
        let ((), new_changeset) = capture_put_kv(&store, "skip/new", "new")?;
        sm.apply([
            Entry {
                log_id: log_id(1, 3),
                payload: EntryPayload::Normal(crate::storage::raft::StoreCommand {
                    lane: Lane::Control,
                    op: "put_kv".to_string(),
                    changeset: old_changeset,
                }),
            },
            Entry {
                log_id: log_id(1, 6),
                payload: EntryPayload::Normal(crate::storage::raft::StoreCommand {
                    lane: Lane::Control,
                    op: "put_kv".to_string(),
                    changeset: new_changeset,
                }),
            },
        ])
        .await
        .unwrap();
        assert_eq!(
            kv_value(&store, "skip/old")?,
            None,
            "stale entry below the applied index must not apply"
        );
        assert_eq!(kv_value(&store, "skip/new")?.as_deref(), Some("new"));
        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 6)));
        Ok(())
    }

    fn capture_put_kv(store: &Store, key: &str, value: &str) -> R<((), Vec<u8>)> {
        capture::capture_mutation(store, Lane::Control, |s| {
            s.put_kv(key, value, KvValueType::String, None, None)?;
            Ok(())
        })
    }
}
