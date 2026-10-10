//! Thread-local capture/apply context and the changeset capture/apply
//! primitives (plan §2, JC-12..15; mechanism validated by
//! `tests/raft_changeset_capture.rs`).
//!
//! A mutation in raft mode runs on a lane's pipeline thread as:
//! global capture mutex -> `SAVEPOINT legion_capture` + session attach ->
//! run the existing method body (its own transactions become nested
//! savepoints) -> capture the changeset -> `ROLLBACK` -> propose the
//! changeset via raft -> on quorum commit every node applies it via
//! [`apply_changeset`]. Side effects (event broadcast, eval notify,
//! read-model cache) are suppressed while `in_capture` is set and are
//! re-projected from the committed changeset at apply time (JC-11).

use std::cell::Cell;
use std::collections::BTreeSet;
use std::sync::Mutex as StdMutex;

use rusqlite::session::Session;
use rusqlite::Connection;

use crate::prelude::*;
use crate::storage::raft::ownership;
use crate::storage::raft::Lane;
use crate::storage::store::Store;

thread_local! {
    /// Set while this thread is executing a store mutation inside the
    /// capture savepoint. Gates side-effect suppression, nested
    /// `Store::replicated` composition (JC-13), and the raft-mode
    /// `write_connection` bypass guard (JC-14).
    static IN_CAPTURE: Cell<bool> = const { Cell::new(false) };
    /// The lane the active capture will propose on, set for the same scope
    /// as `IN_CAPTURE`. Read by id minters for commutative tables so each
    /// lane stays inside its own id residue class (JC-42); `None` outside a
    /// capture (`local_disk` mode and every non-raft caller).
    static CAPTURE_LANE: Cell<Option<Lane>> = const { Cell::new(None) };
    /// Set while this thread is applying a committed changeset (the raft
    /// state-machine apply path). Lets `write_connection` through the
    /// raft-mode bypass guard.
    static IN_APPLY: Cell<bool> = const { Cell::new(false) };
    /// Set while this thread is inside [`local_write`] — a scoped,
    /// deliberately-unreplicated write to LocalOnly tables (none today,
    /// JC-41) or Commutative-table pruning (JC-28, plan §7-C2). Lets
    /// `write_connection` through the raft-mode bypass guard.
    static IN_LOCAL_WRITE: Cell<bool> = const { Cell::new(false) };
    /// Set while this thread runs a `local_disk` mutation body under
    /// [`local_projected_mutation`]: the body commits for real, and the
    /// read model is projected from its changeset afterwards, so read-model
    /// hooks defer exactly as they do under capture
    /// (read-model-coherence plan § 5.4).
    static IN_LOCAL_PROJECTION: Cell<bool> = const { Cell::new(false) };
}

/// Whether the current thread is inside an active changeset capture.
pub(crate) fn in_capture() -> bool {
    IN_CAPTURE.with(Cell::get)
}

/// Whether the current thread is inside a `local_disk` mutation body whose
/// read-model effects are projected from its changeset afterwards.
pub(crate) fn in_local_projection() -> bool {
    IN_LOCAL_PROJECTION.with(Cell::get)
}

/// Whether read-model updates must wait for changeset projection: inside a
/// raft capture (rolled back, re-projected at apply) or a `local_disk`
/// projected mutation (projected when the body returns). Every `cache_*`
/// hook and `refresh_read_model` checks this, so the changeset is the only
/// way into the read model in both modes.
pub(crate) fn read_model_deferred() -> bool {
    in_capture() || in_local_projection()
}

/// Whether the current thread is inside a mutation body that holds the
/// capture mutex (a raft capture or a `local_disk` projected mutation).
/// Code that would otherwise hand work to another thread and wait for it
/// (the job-intake coalescers) must run inline instead: the other thread
/// needs the mutex this one holds.
pub(crate) fn in_mutation_body() -> bool {
    in_capture() || in_local_projection()
}

/// Whether the current thread is applying a committed changeset.
pub(crate) fn in_apply() -> bool {
    IN_APPLY.with(Cell::get)
}

/// Whether the current thread is inside a scoped local (unreplicated) write.
pub(crate) fn in_local_write() -> bool {
    IN_LOCAL_WRITE.with(Cell::get)
}

/// The lane the capture running on this thread will propose on, or `None`
/// when no capture is active.
///
/// The id minter for commutative tables with writer-minted primary keys
/// (`Store::next_scheduler_event_id`) reads this to stay inside its lane's
/// id residue class (JC-42). Outside a capture there is no lane and no
/// concurrent second writer, so callers use the [`Lane::Control`] residue
/// and `local_disk` behavior is unchanged.
pub(crate) fn current_capture_lane() -> Option<Lane> {
    CAPTURE_LANE.with(Cell::get)
}

/// RAII guard that marks the current thread as capturing on `lane`.
/// Restores the previous values on drop so guards nest safely.
pub(crate) struct CaptureGuard {
    prev: bool,
    prev_lane: Option<Lane>,
}

impl CaptureGuard {
    pub(crate) fn new(lane: Lane) -> Self {
        let prev = IN_CAPTURE.with(|flag| flag.replace(true));
        let prev_lane = CAPTURE_LANE.with(|cell| cell.replace(Some(lane)));
        CaptureGuard { prev, prev_lane }
    }
}

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        let prev = self.prev;
        IN_CAPTURE.with(|flag| flag.set(prev));
        let prev_lane = self.prev_lane;
        CAPTURE_LANE.with(|cell| cell.set(prev_lane));
    }
}

/// RAII guard that marks the current thread as applying a changeset.
pub(crate) struct ApplyGuard {
    prev: bool,
}

impl ApplyGuard {
    pub(crate) fn new() -> Self {
        let prev = IN_APPLY.with(|flag| flag.replace(true));
        ApplyGuard { prev }
    }
}

impl Drop for ApplyGuard {
    fn drop(&mut self) {
        let prev = self.prev;
        IN_APPLY.with(|flag| flag.set(prev));
    }
}

/// Serializes captures across all lanes/threads — AND serializes captures
/// against committed-changeset applies. A capture spans multiple
/// acquisitions of the store's write-connection mutex (the method body locks
/// and unlocks freely) while `SAVEPOINT legion_capture` stays open on the
/// connection, so a coarser lock is required to keep the session's view of
/// the connection exclusive for the capture's whole duration (JC-12).
///
/// The apply path ([`apply_changeset_with`] and the state machine's
/// blank/membership marker writes) MUST hold this mutex too: an apply that
/// interleaved between a capture's statements would open its savepoint
/// *nested inside* `legion_capture`, its "commit" would only RELEASE, and
/// the capture's unconditional `ROLLBACK TO legion_capture` would silently
/// erase a quorum-committed entry from this replica (adversarial finding:
/// capture/apply interleave). Lock ordering everywhere is capture mutex
/// first, write-connection mutex second.
static CAPTURE_MUTEX: StdMutex<()> = StdMutex::new(());

/// Lock the capture mutex, tolerating poisoning: [`capture_mutation`]'s
/// teardown is unwind-safe (the capture savepoint is rolled back even when
/// the captured body panics), so a poisoned mutex only means a sibling
/// thread panicked *after* its connection state was restored — continuing
/// is safe, whereas failing closed would wedge the whole write path.
fn capture_lock() -> std::sync::MutexGuard<'static, ()> {
    CAPTURE_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Acquire the global capture mutex, excluding every changeset capture (and
/// every committed-changeset apply) for the guard's lifetime. Used by the
/// snapshot/backup paths (plan §A5): a capture leaves an *open savepoint* on
/// the write connection between its statement executions, and `VACUUM INTO`
/// (or a wholesale state-DB swap) must never observe — or run inside — that
/// transaction.
pub(crate) fn capture_exclusion() -> R<std::sync::MutexGuard<'static, ()>> {
    Ok(capture_lock())
}

/// Run `f` with the write connection held exclusively and all captures
/// excluded — the access mode snapshot build and state backup need (plan
/// §A5): the capture mutex guarantees no capture savepoint is open on the
/// connection, and holding the connection guard for the whole closure
/// guarantees no committed-changeset apply interleaves either. The
/// [`ApplyGuard`] admits this path through the raft-mode `write_connection`
/// bypass guard (JC-14); that is sound because callers only *read* state or
/// write to files *outside* the state DB (`VACUUM INTO`) — they never mutate
/// replicated tables.
pub(crate) fn exclusive_state_access<T>(
    store: &Store,
    f: impl FnOnce(&Connection) -> R<T>,
) -> R<T> {
    let _captures = capture_exclusion()?;
    let _in_apply = ApplyGuard::new();
    let conn = store.write_connection()?;
    f(&conn)
}

const CAPTURE_SAVEPOINT_SQL: &str = "SAVEPOINT legion_capture;";
const CAPTURE_ROLLBACK_SQL: &str = "ROLLBACK TO legion_capture; RELEASE legion_capture;";

/// Run `f` inside a capture savepoint with a SQLite session attached and
/// return its result together with the captured changeset bytes. The local
/// database is left exactly as it was — the savepoint is always rolled
/// back, whether `f` succeeds or fails (on failure the error is returned
/// and no changeset).
///
/// `lane` is the raft group this capture will be proposed on. It is
/// published as the thread's capture lane ([`current_capture_lane`]) for
/// the body's duration so writer-minted commutative ids can carry their
/// lane's stride (JC-42); it is NOT a substitute for the capture-time
/// ownership guard, which still checks the finished changeset.
///
/// The `in_capture` flag is set for the full duration (including the brief
/// setup/teardown locks, so the raft-mode `write_connection` guard admits
/// this function's own locking); `f` locks and unlocks the write connection
/// freely, and its `Store::write_tx` savepoints nest inside the capture
/// savepoint.
pub(crate) fn capture_mutation<T>(
    store: &Store,
    lane: Lane,
    f: impl FnOnce(&Store) -> R<T>,
) -> R<(T, Vec<u8>)> {
    let _capture_lock = capture_lock();
    let _in_capture = CaptureGuard::new(lane);

    // Phase 1: with the write connection locked briefly, open the capture
    // savepoint and attach a session recording every table.
    let mut session = {
        let guard = store.write_connection()?;
        let conn: &Connection = &guard;
        // SAFETY: `Session<'conn>` borrows the connection, but the
        // connection lives behind the store's `Arc<Mutex<Connection>>` and
        // must be unlockable while the session exists (the method body
        // re-locks it for each statement). Extending the borrow to `'static`
        // is sound here because:
        //   - the `Connection` is owned by a `Mutex` inside an `Arc` held by
        //     `store`, which outlives this function body (`store: &Store`),
        //     and it is never moved or dropped while the `Arc` is alive;
        //   - `CAPTURE_MUTEX` (held for this whole function) serializes all
        //     captures, and in raft mode the `write_connection` bypass guard
        //     confines every other writer to capture/apply paths, so no
        //     concurrent writer can race the session — access to the
        //     connection is sequential on this thread;
        //   - the session is dropped before this function returns, so the
        //     fabricated `'static` borrow never escapes.
        let static_conn: &'static Connection =
            unsafe { std::mem::transmute::<&Connection, &'static Connection>(conn) };
        let mut session =
            Session::new(static_conn).context("failed creating sqlite session for capture")?;
        session
            .attach(None)
            .context("failed attaching sqlite session to all tables")?;
        conn.execute_batch(CAPTURE_SAVEPOINT_SQL)
            .context("failed opening capture savepoint")?;
        drop(guard);
        session
    };

    // Phase 2: run the method body. It locks/unlocks the write connection
    // itself; its transactions become savepoints nested inside
    // `legion_capture`. The body runs under `catch_unwind` because a panic
    // must not skip phase 3: an orphaned open `legion_capture` savepoint
    // would make every later "committed" write on this connection a nested
    // RELEASE (never durable, erased at process exit) — the write-dead
    // zombie failure mode from the adversarial review.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(store)));

    // Phase 3 (unconditional): re-lock, extract the changeset (only on
    // success), and always roll the savepoint back so the local DB is
    // untouched — on success, error, and panic alike.
    let guard = store.write_connection()?;
    let changeset_result = match &result {
        Ok(Ok(_)) => {
            let mut changeset: Vec<u8> = Vec::new();
            session
                .changeset_strm(&mut changeset)
                .context("failed extracting capture changeset")
                .map(|()| changeset)
        }
        _ => Ok(Vec::new()),
    };
    let rollback_result = guard
        .execute_batch(CAPTURE_ROLLBACK_SQL)
        .context("failed rolling back capture savepoint");
    drop(session);
    drop(guard);

    let value = match result {
        Ok(inner) => inner?,
        Err(panic) => {
            // Teardown ran: the savepoint is rolled back and the connection
            // is back in autocommit, so resuming the unwind is safe (the
            // pipeline thread converts it into an error reply).
            if let Err(err) = &rollback_result {
                error!(
                    error = %err,
                    "capture savepoint rollback failed while unwinding a panicked captured body"
                );
            }
            std::panic::resume_unwind(panic);
        }
    };
    rollback_result?;
    let changeset = changeset_result?;
    Ok((value, changeset))
}

/// A scoped LOCAL write: committed to this node's database only, never
/// captured, never proposed, admitted through the raft-mode
/// `write_connection` bypass guard (plan §7-C2). This is the ONLY legal
/// write path for [`ownership::Owner::LocalOnly`] tables (a class JC-41
/// emptied — the eval queue is replicated again — but whose mechanism is
/// retained) and for local pruning of [`ownership::Owner::Commutative`]
/// tables (JC-28, today the `scheduler_event` journal, which is the only
/// live caller) — anything else must go through `Store::replicated`.
///
/// Locking: takes the global capture mutex for the write's duration, so a
/// local write can never interleave between the statements of an open
/// capture savepoint on the shared connection (where it would be recorded
/// into — and rolled back with — the capture; the JC-19 hazard) and never
/// races a committed-changeset apply. Nested `local_write` calls compose
/// (run inline); a call from inside an active CAPTURE is a design error and
/// fails loudly — a captured body must not perform local writes (C-2 splits
/// composites so lane ops enqueue work outside their capture).
///
/// Scope enforcement (raft mode, EVERY build — adversarial finding,
/// 2026-07-25: a debug-only assertion left release builds admitting a write
/// to any replicated table through the seam). A SQLite session is attached
/// for the write's duration with a table filter, which SQLite invokes with
/// the name of each table the write touches; every such table must be
/// LocalOnly or Commutative or the write fails. The filter is the cheap half
/// of the check: it records one table name per distinct table and, in
/// release builds, no changeset at all (the filter answers "don't track
/// this", so the session buffers nothing). Debug
/// builds additionally buffer the changeset and assert the finer per-op rule
/// — Commutative tables may only be DELETEd (pruning), never inserted into
/// or updated through this seam. The exemption can therefore never silently
/// grow scope: a local write that strays into a replicated table fails in
/// production, not just under test — which is exactly how a re-localised
/// eval queue would be caught today.
///
/// Enforcement is confined to raft mode because only there does the bypass
/// guard confine every other writer to capture/apply/local paths, making the
/// session's view race-free; in `local_disk` mode concurrent unrelated
/// writers share the connection and would be misattributed to this session.
pub(crate) fn local_write<T>(
    store: &Store,
    op: &'static str,
    f: impl FnOnce(&Store) -> R<T>,
) -> R<T> {
    if in_capture() {
        bail!(
            "local write `{op}` invoked inside a replicated capture: local-only tables must \
             never ride a raft changeset (plan §7-C2, JC-27); perform the local write before \
             or after the replicated operation instead"
        );
    }
    if in_local_write() || in_local_projection() {
        // Compose: the outer local_write (or local projected mutation)
        // already holds the capture mutex. In `local_disk` mode there is no
        // scope assertion to run.
        return f(store);
    }
    let _capture_lock = capture_lock();
    let _in_local = LocalWriteGuard::new();

    if store.raft_mode() {
        return local_write_with_scope_check(store, op, f);
    }
    f(store)
}

/// RAII guard for the local-write thread flag.
struct LocalWriteGuard {
    prev: bool,
}

impl LocalWriteGuard {
    fn new() -> Self {
        let prev = IN_LOCAL_WRITE.with(|flag| flag.replace(true));
        LocalWriteGuard { prev }
    }
}

impl Drop for LocalWriteGuard {
    fn drop(&mut self) {
        let prev = self.prev;
        IN_LOCAL_WRITE.with(|flag| flag.set(prev));
    }
}

/// Raft-mode body of [`local_write`]: run `f` with a session attached and
/// fail if its changes leave the local-write scope. Caller holds the capture
/// mutex and the local-write flag. Active in EVERY build (see `local_write`
/// for the split between the always-on table check and the debug-only
/// per-op check).
fn local_write_with_scope_check<T>(
    store: &Store,
    op: &'static str,
    f: impl FnOnce(&Store) -> R<T>,
) -> R<T> {
    // Release builds record no changeset at all: the table filter answers
    // "don't track this table", so the session buffers nothing and only the
    // table names flow back through `touched`.
    let record_changeset = cfg!(debug_assertions);
    let touched: Arc<StdMutex<BTreeSet<String>>> = Arc::new(StdMutex::new(BTreeSet::new()));
    let mut session = {
        let guard = store.write_connection()?;
        let conn: &Connection = &guard;
        // SAFETY: same argument as in `capture_mutation` — the connection
        // lives in an `Arc<Mutex<..>>` on `store`, which outlives this
        // function; the capture mutex (held by our caller) plus the
        // raft-mode bypass guard confine all other writers, so access is
        // exclusive to this thread; and the session is dropped before this
        // function returns.
        let static_conn: &'static Connection =
            unsafe { std::mem::transmute::<&Connection, &'static Connection>(conn) };
        let mut session =
            Session::new(static_conn).context("failed creating session for local-write check")?;
        // The filter fires once per table the write touches, before the
        // session decides whether to track it — so it sees every table,
        // including ones a changeset could not represent anyway.
        let sink = Arc::clone(&touched);
        session.table_filter(Some(move |table: &str| {
            // SQLite re-invokes the filter for every row change to a table
            // it was told not to track (the release case), so keep this
            // allocation-free once a table is known.
            if let Ok(mut names) = sink.lock() {
                if !names.contains(table) {
                    names.insert(table.to_string());
                }
            }
            record_changeset
        }));
        session
            .attach(None)
            .context("failed attaching session for local-write check")?;
        session
    };
    let result = f(store);
    let mut changeset: Vec<u8> = Vec::new();
    let extract = if record_changeset {
        session
            .changeset_strm(&mut changeset)
            .context("failed extracting local-write scope changeset")
    } else {
        Ok(())
    };
    drop(session);
    extract?;
    let value = result?;
    let tables = touched
        .lock()
        .map_err(|_| anyhow!("local-write scope table set was poisoned"))?
        .clone();
    assert_local_write_tables(op, &tables)?;
    #[cfg(debug_assertions)]
    assert_local_write_scope(op, &changeset)?;
    Ok(value)
}

/// Always-on half of the [`local_write`] scope check: every table the write
/// touched must be LocalOnly (none today) or Commutative (JC-28 journal
/// pruning). Table names only — the per-op rule (Commutative = DELETE only)
/// needs the changeset and stays in [`assert_local_write_scope`].
fn assert_local_write_tables(op: &str, tables: &BTreeSet<String>) -> R<()> {
    for table in tables {
        let allowed = matches!(
            ownership::owner_lane(table),
            Some(ownership::Owner::LocalOnly) | Some(ownership::Owner::Commutative)
        );
        if !allowed {
            bail!(
                "local write `{op}` touched table `{table}`, which is outside the local-write \
                 scope (LocalOnly tables, or DELETE-pruning of Commutative tables). Replicated \
                 tables must go through Store::replicated — see plan §7-C2 (JC-27/JC-28)"
            );
        }
    }
    Ok(())
}

/// Debug assertion behind [`local_write`]: every touched table is LocalOnly
/// (any op) or Commutative with DELETE (local pruning, JC-28).
#[cfg(debug_assertions)]
fn assert_local_write_scope(op: &str, changeset: &[u8]) -> R<()> {
    use fallible_streaming_iterator::FallibleStreamingIterator;
    use rusqlite::hooks::Action;
    use rusqlite::session::ChangesetIter;

    if changeset.is_empty() {
        return Ok(());
    }
    let reader: &mut dyn std::io::Read = &mut &*changeset;
    let mut iter = ChangesetIter::start_strm(&reader)
        .context("failed opening changeset iterator for the local-write scope check")?;
    while let Some(item) = iter
        .next()
        .context("failed iterating changeset for the local-write scope check")?
    {
        let item_op = item.op()?;
        let table = item_op.table_name();
        let allowed = match ownership::owner_lane(table) {
            Some(ownership::Owner::LocalOnly) => true,
            Some(ownership::Owner::Commutative) => item_op.code() == Action::SQLITE_DELETE,
            _ => false,
        };
        if !allowed {
            bail!(
                "local write `{op}` touched table `{table}`, which is outside the local-write \
                 scope (LocalOnly tables, or DELETE-pruning of Commutative tables). Replicated \
                 tables must go through Store::replicated — see plan §7-C2 (JC-27/JC-28)"
            );
        }
    }
    Ok(())
}

/// Tables whose rows the read model holds or keys off: the only ones a
/// `local_disk` projected mutation records. Everything else (events, evals,
/// jobs) keeps its existing direct side effects in `local_disk` mode.
const READ_MODEL_TABLES: [&str; 5] = [
    "node",
    "node_health",
    "allocation",
    "task_config",
    "allocation_epoch_partition",
];

/// RAII guard for the local-projection thread flag.
struct LocalProjectionGuard {
    prev: bool,
}

impl LocalProjectionGuard {
    fn new() -> Self {
        let prev = IN_LOCAL_PROJECTION.with(|flag| flag.replace(true));
        LocalProjectionGuard { prev }
    }
}

impl Drop for LocalProjectionGuard {
    fn drop(&mut self) {
        let prev = self.prev;
        IN_LOCAL_PROJECTION.with(|flag| flag.set(prev));
    }
}

/// Run a `local_disk` mutation body so the read model follows from what it
/// committed, the same way raft replicas follow from a committed changeset
/// (read-model-coherence plan § 5.4):
///
/// 1. take the capture mutex, so no other local mutation's rows land in
///    this body's session;
/// 2. set [`in_local_projection`], which defers every read-model hook;
/// 3. attach a session filtered to [`READ_MODEL_TABLES`];
/// 4. run the body, which commits normally;
/// 5. extract the changeset whether or not the body succeeded, because a
///    body that fails after a commit has still changed state;
/// 6. project the changeset's read-model effects, still under the mutex,
///    so projections land in commit order (the delta path assumes it);
/// 7. run `after` (the shadow check) while nothing else can write.
///
/// A panicking body skips projection: the scrub repairs whatever it
/// committed. Callers nested inside the body compose inline (see
/// `Store::replicated`).
pub(crate) fn local_projected_mutation<T>(
    store: &Store,
    f: impl FnOnce(&Store) -> R<T>,
    after: impl FnOnce(&Store),
) -> R<T> {
    let _capture_lock = capture_lock();
    let _in_local = LocalProjectionGuard::new();

    let mut session = {
        let guard = store.write_connection()?;
        let conn: &Connection = &guard;
        // SAFETY: same argument as `capture_mutation`. The connection lives
        // in an `Arc<Mutex<..>>` on `store`, which outlives this function,
        // and is never moved while the Arc is alive. The capture mutex
        // (held for this whole function) excludes every other replicated
        // op and local write; a write outside those seams in `local_disk`
        // mode would be recorded here too, which is harmless because
        // projection re-reads committed rows rather than trusting the
        // changeset's values. The session is dropped before return.
        let static_conn: &'static Connection =
            unsafe { std::mem::transmute::<&Connection, &'static Connection>(conn) };
        let mut session = Session::new(static_conn)
            .context("failed creating sqlite session for local projection")?;
        session.table_filter(Some(|table: &str| READ_MODEL_TABLES.contains(&table)));
        session
            .attach(None)
            .context("failed attaching sqlite session for local projection")?;
        session
    };

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(store)));

    // Changeset generation reads the current rows through the connection,
    // and deleting the session detaches it from the connection, so both
    // happen under the connection's mutex.
    let mut changeset: Vec<u8> = Vec::new();
    let extract = match store.write_connection() {
        Ok(guard) => {
            let extracted = session
                .changeset_strm(&mut changeset)
                .context("failed extracting local projection changeset");
            drop(session);
            drop(guard);
            extracted
        }
        Err(err) => {
            // Without the mutex the session cannot be deleted safely; leak
            // it rather than touch the connection unguarded. Only reachable
            // if the connection mutex is poisoned, which already wedges
            // every writer.
            std::mem::forget(session);
            Err(err)
        }
    };
    drop(_in_local);

    let result = match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    extract?;
    // Projection failures never un-commit the write; surface them after the
    // body's own error, which is the more useful one when both happen.
    let projected = store.project_read_model_from_changeset(&changeset);
    let value = result?;
    projected?;
    after(store);
    Ok(value)
}

/// Primary-key flags per column for every table a changeset touches, as the
/// changeset recorded them (R2). Its length is the table's column count on
/// the node that captured it. Tables the apply filter skips
/// ([`ownership::table_applies_from_changeset`]) are left out.
pub(crate) fn changeset_table_shapes(
    changeset: &[u8],
) -> R<std::collections::BTreeMap<String, Vec<bool>>> {
    use fallible_streaming_iterator::FallibleStreamingIterator;
    use rusqlite::session::ChangesetIter;

    let mut shapes = std::collections::BTreeMap::new();
    if changeset.is_empty() {
        return Ok(shapes);
    }
    let reader: &mut dyn std::io::Read = &mut &*changeset;
    let mut iter = ChangesetIter::start_strm(&reader)
        .context("failed opening changeset iterator for the schema check")?;
    while let Some(item) = iter
        .next()
        .context("failed iterating changeset for the schema check")?
    {
        let table = item.op()?.table_name().to_string();
        if shapes.contains_key(&table) || !ownership::table_applies_from_changeset(&table) {
            continue;
        }
        let pk: Vec<bool> = item.pk()?.iter().map(|flag| *flag != 0).collect();
        shapes.insert(table, pk);
    }
    Ok(shapes)
}

/// Primary-key flags per column of `table` on `conn`, or `None` when the
/// table does not exist.
pub(crate) fn table_pk_flags(conn: &Connection, table: &str) -> R<Option<Vec<bool>>> {
    let mut stmt = conn.prepare_cached("SELECT pk FROM pragma_table_info(?1) ORDER BY cid;")?;
    let flags: Vec<bool> = stmt
        .query_map([table], |row| row.get::<_, i64>(0))?
        .map(|pk| pk.map(|pk| pk != 0))
        .collect::<Result<_, _>>()?;
    Ok((!flags.is_empty()).then_some(flags))
}

/// Why a changeset's rows for `table` cannot apply to a table with the
/// `local` layout, or `None` when they can. Mirrors SQLite's own
/// compatibility rule for `sqlite3changeset_apply`: the table must exist,
/// have at least as many columns as the changeset, and have its primary key
/// in the same positions. SQLite does not fail on an incompatible table. It
/// skips every change for it, logs a warning, and reports success, which
/// would let a replica advance its applied index past rows it never wrote.
pub(crate) fn table_shape_mismatch(
    table: &str,
    changeset_pk: &[bool],
    local: Option<&[bool]>,
) -> Option<String> {
    match local {
        None => Some(format!("table `{table}` does not exist")),
        Some(local) if local.len() < changeset_pk.len() => Some(format!(
            "table `{table}` has {} columns where the change has {}",
            local.len(),
            changeset_pk.len()
        )),
        Some(local) if local[..changeset_pk.len()] != *changeset_pk => Some(format!(
            "table `{table}` has a different primary key than the change"
        )),
        Some(_) => None,
    }
}

/// Fail when any table in `changeset` cannot apply to `conn`'s schema (see
/// [`table_shape_mismatch`]). The apply path runs this before applying, so
/// a manager running an older schema than the leader stops on the entry
/// with an error naming the fix instead of silently dropping the rows.
fn check_changeset_fits_schema(conn: &Connection, changeset: &[u8]) -> R<()> {
    for (table, pk) in changeset_table_shapes(changeset)? {
        let local = table_pk_flags(conn, &table)?;
        if let Some(reason) = table_shape_mismatch(&table, &pk, local.as_deref()) {
            bail!(
                "this manager's schema cannot apply a replicated change: {reason}. This \
                 manager runs an older build (schema version {}) than the leader that wrote \
                 the entry. Upgrade it to the leader's release and restart it; the entry was \
                 not applied and this lane resumes from it after the upgrade \
                 (docs/upgrades.md)",
                crate::storage::migrations::schema_version()
            );
        }
    }
    Ok(())
}

/// Apply a committed changeset to this node's database inside a transaction,
/// then project its read-model/event/notify side effects. Any conflict means
/// the replica diverged from the leader — impossible by construction under
/// changeset replication — so it aborts the apply loudly rather than
/// "resolving" anything.
#[cfg_attr(not(test), allow(dead_code))] // exercised by the capture round-trip tests in store.rs
pub(crate) fn apply_changeset(store: &Store, bytes: &[u8]) -> R<()> {
    apply_changeset_with(store, bytes, |_| Ok(()))
}

/// [`apply_changeset`], with an extra `within_tx` closure that runs INSIDE
/// the same savepoint as the changeset apply, after the changeset's rows are
/// in place and before commit. The raft state machine uses it to record the
/// applied log id (and membership) in the `raft_applied` table atomically
/// with the data it describes — applied state and data can never diverge,
/// even on a crash between them (plan §A5 "state and index can never
/// diverge"). If `within_tx` fails, the whole savepoint (changeset included)
/// rolls back.
pub(crate) fn apply_changeset_with(
    store: &Store,
    bytes: &[u8],
    within_tx: impl FnOnce(&Connection) -> R<()>,
) -> R<()> {
    {
        // Captures and applies are mutually exclusive (see CAPTURE_MUTEX):
        // an apply must never run while a capture savepoint is open on the
        // shared connection, or the capture's rollback would erase it.
        let _captures = capture_lock();
        let _in_apply = ApplyGuard::new();
        let mut conn = store.write_connection()?;
        // Belt-and-braces: the capture mutex guarantees no capture savepoint
        // is open, so the connection must be in autocommit. If it ever is
        // not, opening a savepoint here would NEST — its "commit" would be a
        // mere RELEASE and an outer rollback would silently destroy this
        // quorum-committed entry. Fail loudly instead.
        if !conn.is_autocommit() {
            bail!(
                "changeset apply found the write connection inside an open transaction; \
                 applying would nest and could be silently rolled back"
            );
        }
        // Foreign-key enforcement must be OFF while the changeset applies:
        // the changeset already records every cascaded child row exactly as
        // the leader's SQL produced it, and re-firing `ON DELETE CASCADE`
        // during the apply would delete those children early, making the
        // changeset's own child-DELETE records hit SQLITE_CHANGESET_NOTFOUND
        // and abort a committed entry forever (adversarial finding:
        // cascade-reliant deletes). The pragma is a no-op inside a
        // transaction, so it brackets the savepoint.
        conn.pragma_update(None, "foreign_keys", "OFF")
            .context("failed disabling foreign_keys for changeset apply")?;
        let apply_result = (|| -> R<()> {
            let sp = Store::write_tx(&mut conn)?;
            // A blank/no-op capture yields zero bytes; skip the stream apply
            // (there is nothing to iterate) but still run `within_tx` so the
            // applied-log marker advances.
            if !bytes.is_empty() {
                check_changeset_fits_schema(&sp, bytes)?;
                let mut input: &[u8] = bytes;
                // Table filter: LocalOnly/Internal tables never apply from a
                // changeset — their rows are node-local raft bookkeeping or
                // (a class JC-41 emptied) deliberately unreplicated state.
                // `eval` is NOT filtered: the queue is replicated
                // scheduler-lane state again, so eval rows must land on
                // every replica or a failover loses acknowledged work.
                // Conflict policy: per-table via the ownership map —
                // commutative tables OMIT insert-exists / delete-missing
                // (JC-28), everything else ABORTs loudly.
                sp.apply_strm(
                    &mut input,
                    Some(ownership::table_applies_from_changeset),
                    |conflict_type, item| ownership::resolve_apply_conflict(conflict_type, &item),
                )
                .context("failed applying replicated changeset")?;
            }
            within_tx(&sp)?;
            sp.commit()?;
            Ok(())
        })();
        let restore_result = conn.pragma_update(None, "foreign_keys", "ON");
        apply_result?;
        restore_result.context("failed re-enabling foreign_keys after changeset apply")?;
    }

    // Side effects are driven by the committed changeset on every replica
    // (JC-11). Projection failures must not un-commit the state change;
    // they are surfaced to the caller after the data is durable.
    store.project_changeset_side_effects(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{KvValueType, ManagerConfigFull};
    use std::sync::mpsc;
    use std::time::Duration;

    fn setup_store() -> R<Store> {
        let dir = tempfile::Builder::new()
            .prefix("khepri-raft-capture-")
            .tempdir()
            .context("Error creating temp store directory")?
            .keep();
        let mut config = ManagerConfigFull {
            store_path: dir
                .to_str()
                .context("Error parsing store path")?
                .to_string(),
            ..Default::default()
        };
        // Captured on one store, applied on another: replicas of one
        // cluster share a root key (docs/kv-encryption.md).
        config.encryption.root_key.path =
            Some(crate::storage::keyring::test_support::shared_root_key_path());
        Store::start_storage(&config)
    }

    fn kv_value(store: &Store, key: &str) -> R<Option<String>> {
        Ok(store.get_kv(key)?.map(|entry| entry.value))
    }

    /// Wakes for the eval pool are metered by WORK, not by the eval table
    /// moving.
    ///
    /// Every eval crosses the projection at least three times (enqueued,
    /// claimed, completed) and the pool used to be woken for all three, with
    /// `notify_waiters`. Each woken worker answers with a `claim_eval` — a
    /// capture on the write connection, and a scheduler-lane quorum round
    /// whenever it finds work — so a pool of N paid N claims per transition
    /// and N-1 came back empty.
    /// The AWS phase-3 campaign measured 2.8x the scheduler-lane raft ops at
    /// 16 workers versus 4, for identical work, and LOWER throughput.
    #[test]
    fn projection_wakes_one_worker_per_claimable_eval() -> R<()> {
        let store = setup_store()?;
        let notifier = store.eval_notifier();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;

        // Three parked probes stand in for three idle eval workers, enabled
        // before the transition so none can be missed. The old behavior woke
        // all three for any eval row change; the fix must wake exactly as
        // many as there are newly claimable evals.
        macro_rules! wakes_from {
            ($action:stmt) => {{
                let a = notifier.notified();
                let b = notifier.notified();
                let c = notifier.notified();
                tokio::pin!(a, b, c);
                runtime.block_on(async {
                    a.as_mut().enable();
                    b.as_mut().enable();
                    c.as_mut().enable();
                });
                $action
                runtime.block_on(async {
                    let mut count = 0usize;
                    for fut in [a, b, c] {
                        if tokio::time::timeout(Duration::from_millis(120), fut)
                            .await
                            .is_ok()
                        {
                            count += 1;
                        }
                    }
                    count
                })
            }};
        }

        // One enqueued eval is one unit of work nobody is doing yet.
        let ((), enqueued) = capture_mutation(&store, Lane::Scheduler, |s| {
            s.enqueue_eval("job_reschedule", Some("a"), Some("ac"), None, None)?;
            Ok(())
        })?;
        let woke = wakes_from!(apply_changeset(&store, &enqueued)?);
        assert_eq!(
            woke, 1,
            "one claimable eval must wake exactly one worker; waking the pool \
             spends a quorum round per worker to discover an empty queue"
        );

        // Claiming moves pending -> in_flight. That is a worker TAKING work,
        // not work appearing, so nobody may be woken for it.
        let ((), claimed) = capture_mutation(&store, Lane::Scheduler, |s| {
            s.claim_eval()?;
            Ok(())
        })?;
        let woke = wakes_from!(apply_changeset(&store, &claimed)?);
        assert_eq!(
            woke, 0,
            "a claim takes work off the queue; it must wake nobody"
        );

        Ok(())
    }

    /// Adversarial regression (capture/apply interleave): a committed
    /// changeset applied WHILE a capture is in progress on the same store
    /// must survive the capture's rollback. Before the fix, the apply took
    /// only the connection mutex, opened its savepoint nested inside the
    /// open `legion_capture` savepoint between the capture's statements,
    /// and the capture's `ROLLBACK TO legion_capture` silently erased the
    /// quorum-committed rows.
    #[test]
    fn committed_apply_survives_a_concurrent_capture_rollback() -> R<()> {
        let store = setup_store()?;

        // Pre-capture the "committed entry" a peer would replicate to us.
        let ((), committed) = capture_mutation(&store, Lane::Control, |s| {
            s.put_kv("committed/entry", "quorum", KvValueType::String, None, None)?;
            Ok(())
        })?;
        assert!(!committed.is_empty());

        let apply_store = store.clone();
        let (started_tx, started_rx) = mpsc::channel::<()>();
        let applier = std::thread::spawn(move || -> R<()> {
            started_rx.recv().expect("capture must have started");
            // Attempted mid-capture: must serialize AFTER the capture's
            // rollback, not nest inside its savepoint.
            apply_changeset(&apply_store, &committed)
        });

        let ((), capture_changeset) = capture_mutation(&store, Lane::Control, |s| {
            let _ = started_tx.send(());
            // Window in which the apply thread races the open capture.
            std::thread::sleep(Duration::from_millis(300));
            s.put_kv(
                "captured/local",
                "rolled-back",
                KvValueType::String,
                None,
                None,
            )?;
            Ok(())
        })?;
        applier.join().expect("apply thread panicked")?;
        assert!(!capture_changeset.is_empty());

        // The committed entry survived the capture rollback...
        assert_eq!(
            kv_value(&store, "committed/entry")?.as_deref(),
            Some("quorum"),
            "a quorum-committed apply must never be destroyed by a concurrent capture rollback"
        );
        // ...and the capture's own local writes rolled back as always.
        assert_eq!(kv_value(&store, "captured/local")?, None);
        Ok(())
    }

    /// Adversarial regression (panic-orphaned capture savepoint): a panic
    /// inside a captured body must still roll the capture savepoint back
    /// and must not wedge the capture mutex. Before the fix, the unwind
    /// skipped phase 3, leaving `legion_capture` open forever: later
    /// applies "committed" only a nested RELEASE (never durable) and every
    /// later capture failed on the poisoned mutex.
    #[test]
    fn panicking_captured_body_rolls_back_and_leaves_the_store_writable() -> R<()> {
        let store = setup_store()?;

        let panic_store = store.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            capture_mutation(&panic_store, Lane::Control, |s| -> R<()> {
                s.put_kv("poison/key", "boom", KvValueType::String, None, None)?;
                panic!("captured body exploded");
            })
        }));
        assert!(result.is_err(), "the body's panic must propagate");

        // The capture savepoint was rolled back despite the panic.
        assert_eq!(kv_value(&store, "poison/key")?, None);

        // The capture mutex is usable (poison tolerated) — a later capture
        // works...
        let ((), changeset) = capture_mutation(&store, Lane::Control, |s| {
            s.put_kv("after/panic", "captured", KvValueType::String, None, None)?;
            Ok(())
        })?;
        // ...and a committed apply lands DURABLY: visible through the
        // read-only pool, i.e. actually committed at the top level rather
        // than parked inside an orphaned open savepoint.
        apply_changeset(&store, &changeset)?;
        assert_eq!(
            kv_value(&store, "after/panic")?.as_deref(),
            Some("captured")
        );
        Ok(())
    }

    fn insert_event_row(store: &Store, id: i64) -> R<()> {
        let conn = store.write_connection()?;
        conn.execute(
            "INSERT INTO scheduler_event (id, kind, level, message, metadata_json, created_at)
             VALUES (?1, 'test.kind', 'info', 'm', '{}', '2026-01-01T00:00:00Z');",
            [id],
        )?;
        Ok(())
    }

    /// JC-28: commutative tables resolve INSERT-exists and DELETE-missing
    /// apply conflicts with OMIT — insert-if-absent / delete-if-present is
    /// order-independent, so replays and locally-pruned rows never abort a
    /// committed entry.
    #[test]
    fn commutative_conflicts_resolve_with_omit() -> R<()> {
        let src = setup_store()?;
        let dst = setup_store()?;

        // INSERT-exists: the same commutative insert applied twice.
        let ((), insert_cs) = capture_mutation(&src, Lane::Control, |s| insert_event_row(s, 777))?;
        apply_changeset(&dst, &insert_cs)?;
        apply_changeset(&dst, &insert_cs).context("second apply must OMIT, not abort")?;
        let count: i64 = {
            let conn = dst.read_connection()?;
            conn.query_row(
                "SELECT COUNT(*) FROM scheduler_event WHERE id = 777;",
                [],
                |r| r.get(0),
            )?
        };
        assert_eq!(count, 1);

        // DELETE-missing: a commutative delete whose row was locally pruned
        // on this replica (old-log replay shape).
        insert_event_row(&src, 778)?;
        let ((), delete_cs) = capture_mutation(&src, Lane::Control, |s| {
            let conn = s.write_connection()?;
            conn.execute("DELETE FROM scheduler_event WHERE id = 778;", [])?;
            Ok(())
        })?;
        apply_changeset(&dst, &delete_cs)
            .context("delete of a locally-absent commutative row must OMIT, not abort")?;
        Ok(())
    }

    /// Read the whole journal as `(id, kind)` pairs.
    fn journal_of(store: &Store) -> R<Vec<(i64, String)>> {
        let conn = store.read_connection()?;
        let mut stmt = conn.prepare("SELECT id, kind FROM scheduler_event ORDER BY id, kind;")?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Store::collect_rows(rows)
    }

    /// Adversarial regression (JC-28 premise, fixed by JC-42): event rows
    /// staged by ops on DIFFERENT lanes from the same base state must carry
    /// globally-unique ids, so applying the two changesets in opposite
    /// orders on two replicas converges. Before the fix, `insert_event` used
    /// the implicit rowid (`max(id)+1` at capture time on the proposer), so
    /// both captures minted the SAME id with different content; OMIT then
    /// kept whichever applied first — a different row per replica, silent
    /// divergence (and invisible to the content hash, which excludes
    /// commutative tables).
    #[test]
    fn cross_lane_event_inserts_from_the_same_base_state_converge() -> R<()> {
        use crate::storage::events::EventDraft;

        // Two proposers with identical (empty) journals — the same base
        // state two lane leaders would capture from.
        let proposer_control = setup_store()?;
        let proposer_status = setup_store()?;
        let ((), cs_control) = capture_mutation(&proposer_control, Lane::Control, |s| {
            s.emit_event(EventDraft::info("probe.control", "control lane event"));
            Ok(())
        })?;
        let ((), cs_status) = capture_mutation(&proposer_status, Lane::Status, |s| {
            s.emit_event(EventDraft::info("probe.status", "status lane event"));
            Ok(())
        })?;
        assert!(!cs_control.is_empty() && !cs_status.is_empty());

        // Two replicas apply the two committed changesets in OPPOSITE
        // orders — exactly what independent lane logs do.
        let replica_1 = setup_store()?;
        let replica_2 = setup_store()?;
        apply_changeset(&replica_1, &cs_control)?;
        apply_changeset(&replica_1, &cs_status)?;
        apply_changeset(&replica_2, &cs_status)?;
        apply_changeset(&replica_2, &cs_control)?;

        let journal_1 = journal_of(&replica_1)?;
        let journal_2 = journal_of(&replica_2)?;
        assert_eq!(
            journal_1.len(),
            2,
            "both lanes' events must survive on every replica (ids must be globally \
             unique, not max+1): {journal_1:?}"
        );
        assert_eq!(
            journal_1, journal_2,
            "opposite apply orders must converge to the identical journal"
        );
        // Each id sits in its own lane's residue class (JC-42) — which is
        // WHY the two ids cannot collide, whatever the clock does.
        let residues: Vec<i64> = journal_1
            .iter()
            .map(|(id, _)| id.rem_euclid(Lane::ID_STRIDE))
            .collect();
        assert!(
            residues.contains(&Lane::Control.id_residue())
                && residues.contains(&Lane::Status.id_residue()),
            "each lane's event must carry its own id residue: {journal_1:?}"
        );
        Ok(())
    }

    /// JC-42, the concurrency case: two lanes capturing events at the SAME
    /// TIME (racing threads, so their millisecond stamps and sequence
    /// numbers interleave arbitrarily) mint provably disjoint id sets, and
    /// every one of those events survives on a replica that applies both
    /// lanes' changesets. Strided residues make this structural — no
    /// timing, retry, or ordering assumption is involved.
    #[test]
    fn concurrent_lanes_mint_disjoint_event_ids_and_all_survive_apply() -> R<()> {
        use crate::storage::events::EventDraft;
        use std::sync::mpsc;

        const PER_LANE: usize = 24;
        let replica = setup_store()?;

        // Each lane proposes from its own store, all starting from the same
        // (empty) journal — the capture mutex serializes the captures the
        // way the pipeline threads do, but the lanes never see each other's
        // rows, exactly like two independent raft logs.
        let (tx, rx) = mpsc::channel::<R<Vec<(Lane, Vec<u8>)>>>();
        let mut handles = Vec::new();
        for lane in [Lane::Control, Lane::Scheduler, Lane::Status] {
            let tx = tx.clone();
            handles.push(std::thread::spawn(move || {
                let result = (|| -> R<Vec<(Lane, Vec<u8>)>> {
                    let proposer = setup_store()?;
                    let mut changesets = Vec::new();
                    for index in 0..PER_LANE {
                        let ((), changeset) = capture_mutation(&proposer, lane, |s| {
                            s.emit_event(EventDraft::info(
                                "probe.concurrent",
                                format!("{} #{index}", lane.wire_name()),
                            ));
                            Ok(())
                        })?;
                        changesets.push((lane, changeset));
                    }
                    Ok(changesets)
                })();
                let _ = tx.send(result);
            }));
        }
        drop(tx);
        let mut staged: Vec<(Lane, Vec<u8>)> = Vec::new();
        for batch in rx {
            staged.extend(batch?);
        }
        for handle in handles {
            handle.join().expect("lane thread panicked");
        }
        assert_eq!(staged.len(), 3 * PER_LANE);

        // Every committed changeset applies on one replica. A collision
        // between lanes would show up here as an OMITted (silently lost)
        // event.
        for (_, changeset) in &staged {
            apply_changeset(&replica, changeset)?;
        }
        let journal = journal_of(&replica)?;
        assert_eq!(
            journal.len(),
            3 * PER_LANE,
            "every lane's events must survive: no id was minted twice"
        );

        // Disjointness, stated directly: each id's residue identifies its
        // minting lane, so the three lanes' id sets cannot intersect.
        let ids: Vec<i64> = journal.iter().map(|(id, _)| *id).collect();
        let unique: std::collections::BTreeSet<i64> = ids.iter().copied().collect();
        assert_eq!(unique.len(), ids.len(), "ids must be globally unique");
        for lane in Lane::ALL {
            let lane_ids: Vec<i64> = ids
                .iter()
                .copied()
                .filter(|id| id.rem_euclid(Lane::ID_STRIDE) == lane.id_residue())
                .collect();
            assert_eq!(
                lane_ids.len(),
                PER_LANE,
                "lane {} must own exactly its own events' ids",
                lane.wire_name()
            );
        }
        Ok(())
    }

    /// JC-42, monotonicity: ids minted for one lane strictly increase, so
    /// the `after_id` cursor contract (and the stream's increasing-id
    /// dedupe) still holds per lane. Ids are NOT contiguous — pruning
    /// always made them gappy and striding widens the gaps — so the
    /// assertion is ordering, not density.
    #[test]
    fn event_ids_strictly_increase_within_a_lane() -> R<()> {
        use crate::storage::events::EventDraft;

        for lane in Lane::ALL {
            let store = setup_store()?;
            let mut previous = 0i64;
            for index in 0..32 {
                let ((), changeset) = capture_mutation(&store, lane, |s| {
                    s.emit_event(EventDraft::info("probe.monotonic", format!("#{index}")));
                    Ok(())
                })?;
                apply_changeset(&store, &changeset)?;
                // The journal's high-water mark advanced, i.e. the id just
                // minted is greater than every id this lane minted before.
                let latest = store.latest_scheduler_event_id()?;
                assert!(
                    latest > previous,
                    "event {index} on the {} lane minted id {latest}, not above {previous}",
                    lane.wire_name()
                );
                assert_eq!(
                    latest.rem_euclid(Lane::ID_STRIDE),
                    lane.id_residue(),
                    "id {latest} left the {} lane's residue class",
                    lane.wire_name()
                );
                previous = latest;
            }
            // Nothing was lost to a colliding id along the way (an OMITted
            // insert would leave fewer rows than events emitted).
            assert_eq!(journal_of(&store)?.len(), 32);
        }
        Ok(())
    }

    /// Everything that is NOT commutative keeps the ABORT policy: a
    /// conflict on an owned table means replica divergence and must fail
    /// the apply loudly.
    #[test]
    fn non_commutative_conflicts_still_abort() -> R<()> {
        let src = setup_store()?;
        let dst = setup_store()?;
        let (_, changeset) = capture_mutation(&src, Lane::Control, |s| {
            s.put_kv("abort/key", "v", KvValueType::String, None, None)
        })?;
        apply_changeset(&dst, &changeset)?;
        let err = apply_changeset(&dst, &changeset)
            .expect_err("re-applying an owned-table insert must conflict-abort");
        assert!(
            format!("{err:#}").contains("failed applying replicated changeset"),
            "{err:#}"
        );
        Ok(())
    }

    /// SQLite's changeset apply silently skips every change for a table
    /// narrower than the changeset (a column a newer migration added) and
    /// reports success. That is how an older replica would drop a newer
    /// leader's rows while advancing its applied index (R2). The apply path
    /// must refuse such an entry instead.
    #[test]
    fn a_change_for_a_wider_table_is_refused_not_silently_skipped() -> R<()> {
        // The raw SQLite behavior the check exists for.
        let wide = Connection::open_in_memory()?;
        wide.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT);")?;
        let narrow = Connection::open_in_memory()?;
        narrow.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT);")?;
        let mut changeset = Vec::new();
        {
            let mut session = Session::new(&wide)?;
            session.attach(None)?;
            wide.execute("INSERT INTO t VALUES (1, 'x', 'y');", [])?;
            session.changeset_strm(&mut changeset)?;
        }
        let mut input: &[u8] = &changeset;
        narrow.apply_strm(&mut input, None::<fn(&str) -> bool>, |_, _| {
            rusqlite::session::ConflictAction::SQLITE_CHANGESET_ABORT
        })?;
        let rows: i64 = narrow.query_row("SELECT count(*) FROM t;", [], |row| row.get(0))?;
        assert_eq!(rows, 0, "SQLite skips the change and still reports success");
        let err = check_changeset_fits_schema(&narrow, &changeset)
            .expect_err("a narrower table must fail the schema check");
        assert!(
            format!("{err:#}").contains("has 2 columns where the change has 3"),
            "{err:#}"
        );
        assert!(check_changeset_fits_schema(&wide, &changeset).is_ok());

        // End to end through the store's apply path: a replica missing a
        // column the leader's schema has refuses the entry and applies
        // nothing.
        let src = setup_store()?;
        let dst = setup_store()?;
        let (_, changeset) = capture_mutation(&src, Lane::Control, |s| {
            s.put_kv("skew/key", "v", KvValueType::String, None, None)
        })?;
        dst.write_connection()?
            .execute_batch("ALTER TABLE kv_entry DROP COLUMN enc_key_id;")?;
        let err = apply_changeset(&dst, &changeset)
            .expect_err("an entry this schema cannot hold must not apply");
        let rendered = format!("{err:#}");
        assert!(rendered.contains("table `kv_entry` has"), "{rendered}");
        assert!(rendered.contains("older build"), "{rendered}");
        let rows: i64 = dst.read_connection()?.query_row(
            "SELECT count(*) FROM kv_entry WHERE key = 'skew/key';",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(rows, 0);
        Ok(())
    }

    /// Node-local tables never apply from a changeset: a contaminated entry
    /// must not overwrite this replica's own raft bookkeeping (`raft_applied`
    /// is [`ownership::Owner::Internal`]; the LocalOnly class is empty since
    /// JC-41 but runs through the same filter).
    ///
    /// The converse — `eval` rows DO apply, because JC-41 put the queue back
    /// in raft — is asserted in the same pass: filtering it here is precisely
    /// the JC-27 bug that lost acknowledged work on failover.
    #[test]
    fn node_local_tables_are_filtered_out_of_apply_but_evals_are_not() -> R<()> {
        let src = setup_store()?;
        let dst = setup_store()?;
        // Replicas share the cluster's data keys (KV values replicate as
        // ciphertext).
        dst.adopt_keyring_from(&src)?;
        let ((), changeset) = capture_mutation(&src, Lane::Scheduler, |s| {
            s.put_kv("filtered/key", "v", KvValueType::String, None, None)?;
            let conn = s.write_connection()?;
            conn.execute(
                "INSERT INTO eval (status, type, priority, ready_at)
                 VALUES ('pending', 'job_submit', 50, '2026-01-01T00:00:00Z');",
                [],
            )?;
            conn.execute(
                "INSERT INTO raft_applied (lane, last_applied, membership)
                 VALUES ('bogus-lane', x'00', x'00');",
                [],
            )?;
            Ok(())
        })?;
        apply_changeset(&dst, &changeset)?;
        assert_eq!(kv_value(&dst, "filtered/key")?.as_deref(), Some("v"));
        let internal: i64 = {
            let conn = dst.read_connection()?;
            conn.query_row(
                "SELECT COUNT(*) FROM raft_applied WHERE lane = 'bogus-lane';",
                [],
                |r| r.get(0),
            )?
        };
        assert_eq!(
            internal, 0,
            "raft-internal rows must never apply from a changeset"
        );
        let evals: i64 = {
            let conn = dst.read_connection()?;
            conn.query_row("SELECT COUNT(*) FROM eval;", [], |r| r.get(0))?
        };
        assert_eq!(
            evals, 1,
            "eval rows are replicated scheduler-lane state and MUST apply on replicas (JC-41)"
        );
        Ok(())
    }

    /// The local-write seam: admitted through the raft-mode bypass guard,
    /// commits durably, refuses to run inside a capture, and (debug builds,
    /// raft mode) rejects writes that stray outside the local-only scope.
    ///
    /// Its one live caller since JC-41 is the JC-28 commutative journal
    /// prune, so that is what the admitted-and-durable case exercises; the
    /// eval queue is deliberately NOT in scope any more and is covered by
    /// the stray-write assertion below.
    #[test]
    fn local_write_scope_and_guards() -> R<()> {
        let store = setup_store()?;
        store.set_raft_mode(true);

        // Commutative DELETE (local journal pruning) is the scope: admitted
        // and durable.
        insert_event_row_direct(&store, 300)?;
        local_write(&store, "test_local_prune", |s| {
            let conn = s.write_connection()?;
            conn.execute("DELETE FROM scheduler_event WHERE id = 300;", [])?;
            Ok(())
        })?;
        let remaining: i64 = {
            let conn = store.read_connection()?;
            conn.query_row(
                "SELECT COUNT(*) FROM scheduler_event WHERE id = 300;",
                [],
                |r| r.get(0),
            )?
        };
        assert_eq!(remaining, 0, "the local prune must commit durably");

        // EVERY build: a local write that touches a replicated table is a
        // scope violation and fails loudly — including the eval queue, which
        // JC-41 took back OUT of the local-write scope. (Release builds run
        // the table-name half of the check; the same two writes must be
        // rejected there, which is what the 2026-07-25 adversarial finding
        // was about.)
        let err = local_write(&store, "test_local_eval", |s| {
            let conn = s.write_connection()?;
            conn.execute(
                "INSERT INTO eval (status, type, priority, ready_at)
                 VALUES ('pending', 'job_submit', 50, '2026-01-01T00:00:00Z');",
                [],
            )?;
            Ok(())
        })
        .expect_err("the eval queue is replicated again (JC-41): local writes must fail");
        let rendered = format!("{err:#}");
        assert!(rendered.contains("eval"), "{rendered}");
        assert!(rendered.contains("local-write"), "{rendered}");

        let err = local_write(&store, "test_local_stray", |s| {
            let conn = s.write_connection()?;
            conn.execute(
                "INSERT INTO kv_entry (key, value, value_type, version, created_at, updated_at)
                 VALUES ('stray', 'v', 0, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');",
                [],
            )?;
            Ok(())
        })
        .expect_err("a local write into a replicated table must fail the scope check");
        let rendered = format!("{err:#}");
        assert!(rendered.contains("kv_entry"), "{rendered}");
        assert!(rendered.contains("local-write"), "{rendered}");

        // Debug builds keep the finer per-op rule: a Commutative table may
        // be pruned (DELETE) through the seam but never written into.
        #[cfg(debug_assertions)]
        {
            let err = local_write(&store, "test_local_event_insert", |s| {
                let conn = s.write_connection()?;
                conn.execute(
                    "INSERT INTO scheduler_event (id, kind, level, message, metadata_json, \
                     created_at) VALUES (301, 'test.kind', 'info', 'm', '{}', \
                     '2026-01-01T00:00:00Z');",
                    [],
                )?;
                Ok(())
            })
            .expect_err("a commutative INSERT is not local maintenance and must fail");
            let rendered = format!("{err:#}");
            assert!(rendered.contains("scheduler_event"), "{rendered}");
            assert!(rendered.contains("local-write"), "{rendered}");
        }

        store.set_raft_mode(false);

        // Inside a capture, local writes are a design error.
        let result = capture_mutation(&store, Lane::Control, |s| {
            local_write(s, "test_local_nested", |_| Ok(()))
        });
        let err = result.expect_err("local_write inside a capture must fail");
        assert!(
            format!("{err:#}").contains("inside a replicated capture"),
            "{err:#}"
        );
        Ok(())
    }

    /// Raft-mode helper for [`local_write_scope_and_guards`]: seed a row
    /// through the apply path (capture mutex + apply guard), the way a
    /// committed changeset would land it, so the bypass guard admits it
    /// while raft mode is armed.
    fn insert_event_row_direct(store: &Store, id: i64) -> R<()> {
        let _captures = capture_exclusion()?;
        let _in_apply = ApplyGuard::new();
        let conn = store.write_connection()?;
        conn.execute(
            "INSERT INTO scheduler_event (id, kind, level, message, metadata_json, created_at)
             VALUES (?1, 'test.kind', 'info', 'm', '{}', '2026-01-01T00:00:00Z');",
            [id],
        )?;
        Ok(())
    }
}
