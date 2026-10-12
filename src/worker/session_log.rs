//! The node's session log, `sessions.db` (session durability plan §6.2).
//!
//! Sessions have no raft rows and are never written to the worker state
//! file. This log is what lets them outlive the worker process: one row per
//! live or sleeping session on this node, written to a SQLite database
//! beside the state file in WAL mode.
//!
//! Writes never block a start. Callers hand a row to a background writer
//! over a channel; the writer commits whatever arrived within
//! [`GROUP_COMMIT`] in one transaction, so a burst of starts costs one
//! fsync, not one per start. A start the writer has not committed when the
//! worker dies is lost from the log (its process keeps running and ends as
//! an orphan of the restart), which is the trade the plan accepts for
//! keeping a disk flush off the start path.
//!
//! On restart the worker reads the log ([`SessionLog::load`]) before
//! anything else touches it, re-adopts the sessions whose process is still
//! the one it started, and drops the rows whose process is gone.

use crate::prelude::*;
use crate::worker::state::PersistedAllocation;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// How long the writer gathers rows before committing them together.
pub const GROUP_COMMIT: Duration = Duration::from_millis(50);

/// What a session row says about the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SessionLogState {
    /// A process runs it; `PersistedSession::process` says which.
    Running,
    /// Suspended to a checkpoint on this node; nothing runs.
    Suspended,
    /// Its local checkpoint was evicted. Kept for `suspend.keepFor` so the
    /// node answers `evicted` rather than `not found`.
    Evicted,
}

impl SessionLogState {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionLogState::Running => "running",
            SessionLogState::Suspended => "suspended",
            SessionLogState::Evicted => "evicted",
        }
    }
}

/// One session, as the log keeps it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistedSession {
    pub session_id: String,
    pub claim_id: String,
    /// The worker-local key its directories and process run under.
    pub local_key: i64,
    pub job_id: String,
    pub job_config_id: String,
    pub task_name: String,
    pub resources: Value,
    pub gate: String,
    pub origin: String,
    pub on_refused: String,
    #[serde(default)]
    pub ticket_jti: String,
    pub lease_issued_at_unix_ms: i64,
    /// The session's generation (plan §6.7): 1 at start, one more on every
    /// resume that claims the session's manifest.
    #[serde(default = "first_generation")]
    pub generation: u64,
    pub state: SessionLogState,
    /// The running process, for re-adoption. `None` unless `Running`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<PersistedAllocation>,
    /// The checkpoint the session resumes from. Always set on a
    /// `Suspended` row. A `Running` row carries the latest periodic or
    /// worker-stop checkpoint, if one was taken, so a session whose process
    /// did not survive a worker restart is restored from it instead of
    /// being dropped. `None` on an `Evicted` row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<SessionCheckpoint>,
    /// The launch payload the session started from (provisional block
    /// included), so a resume on this node launches the same spec. Dropped
    /// on eviction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<crate::structs::TaskAllocation>,
    /// The task's `suspend.keepFor`, in milliseconds. 0: the default.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub keep_for_ms: u64,
    /// Why a checkpoint was evicted, for the `evicted` answer.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub evicted_reason: String,
    /// Unix ms of the last change: when it started, suspended, or was
    /// evicted. `keepFor` counts from here for non-running rows.
    pub updated_at_unix_ms: i64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// A session checkpoint held on this node: memory parts pinned in the node
/// content store under `snapshot_id` (none for a disk checkpoint, which of
/// a session holds only its id and spec).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionCheckpoint {
    /// The snapshot id its parts are pinned under (`snap-...`).
    pub snapshot_id: String,
    /// `memory` or `disk`.
    pub kind: String,
    /// The snapshot manifest, as the restore reads it.
    pub manifest_json: String,
    /// Bytes the checkpoint holds in the node content store.
    #[serde(default)]
    pub total_bytes: u64,
    /// When it was taken, unix ms.
    pub taken_at_unix_ms: i64,
}

fn first_generation() -> u64 {
    1
}

impl PersistedSession {
    /// How long a suspended or evicted row is kept (`suspend.keepFor`).
    pub fn keep_for(&self) -> Duration {
        if self.keep_for_ms == 0 {
            crate::structs::suspend::DEFAULT_KEEP_FOR
        } else {
            Duration::from_millis(self.keep_for_ms)
        }
    }

    /// The row once the session is suspended to `checkpoint` at `now_ms`:
    /// nothing runs, and the envelope share is free.
    pub fn into_suspended(mut self, checkpoint: SessionCheckpoint, now_ms: i64) -> Self {
        self.state = SessionLogState::Suspended;
        self.process = None;
        self.checkpoint = Some(checkpoint);
        self.evicted_reason.clear();
        self.updated_at_unix_ms = now_ms;
        self
    }

    /// The row once its checkpoint is evicted at `now_ms`: only what the
    /// `evicted` answer needs is kept, for another `keepFor`.
    pub fn into_evicted(mut self, reason: &str, now_ms: i64) -> Self {
        self.state = SessionLogState::Evicted;
        self.process = None;
        self.checkpoint = None;
        self.payload = None;
        self.evicted_reason = reason.to_string();
        self.updated_at_unix_ms = now_ms;
        self
    }

    /// Whether a row that runs nothing has outlived `keepFor` at `now_ms`:
    /// a suspended row is evicted then, and an evicted row forgotten.
    pub fn expired(&self, now_ms: i64) -> bool {
        self.state != SessionLogState::Running
            && now_ms.saturating_sub(self.updated_at_unix_ms) >= self.keep_for().as_millis() as i64
    }
}

/// The suspended sessions disk pressure evicts, in order: oldest suspend
/// first (ties by id, so the order is stable), and only those whose
/// checkpoint holds bytes, since evicting one that holds none frees
/// nothing.
pub fn eviction_order<'a>(rows: impl IntoIterator<Item = &'a PersistedSession>) -> Vec<String> {
    let mut candidates: Vec<(i64, &str)> = rows
        .into_iter()
        .filter(|row| row.state == SessionLogState::Suspended)
        .filter(|row| {
            row.checkpoint
                .as_ref()
                .is_some_and(|checkpoint| checkpoint.total_bytes > 0)
        })
        .map(|row| (row.updated_at_unix_ms, row.session_id.as_str()))
        .collect();
    candidates.sort();
    candidates
        .into_iter()
        .map(|(_, session_id)| session_id.to_string())
        .collect()
}

enum Op {
    Put(Box<PersistedSession>),
    Delete(String),
    Sync(mpsc::Sender<()>),
}

/// Handle to the node's session log. Cheap to clone; every clone feeds the
/// same writer.
#[derive(Clone)]
pub struct SessionLog {
    path: PathBuf,
    tx: mpsc::Sender<Op>,
}

impl std::fmt::Debug for SessionLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionLog")
            .field("path", &self.path)
            .finish()
    }
}

fn connect(path: &Path) -> R<Connection> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
    }
    let conn =
        Connection::open(path).with_context(|| format!("open session log {}", path.display()))?;
    // WAL: a commit appends to the log and fsyncs once; readers never block
    // the writer. FULL so a committed batch survives power loss.
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            state TEXT NOT NULL,
            row TEXT NOT NULL
        )",
    )?;
    // Rows embed task env, which may hold credentials: owner-only, like the
    // state file.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(conn)
}

impl SessionLog {
    /// Open (or create) the log and start its writer.
    pub fn open(path: impl Into<PathBuf>) -> R<SessionLog> {
        let path = path.into();
        let conn = connect(&path)?;
        let (tx, rx) = mpsc::channel();
        let thread_path = path.clone();
        std::thread::Builder::new()
            .name("session-log".into())
            .spawn(move || writer(conn, rx, &thread_path))
            .context("spawn the session log writer")?;
        Ok(SessionLog { path, tx })
    }

    /// Every row in the log, read synchronously. Called once on restart,
    /// before the writer has anything to write.
    pub fn load(path: &Path) -> R<Vec<PersistedSession>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let conn = connect(path)?;
        let mut stmt = conn.prepare("SELECT session_id, row FROM sessions")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut sessions = Vec::new();
        for row in rows {
            let (session_id, json) = row?;
            match serde_json::from_str::<PersistedSession>(&json) {
                Ok(session) => sessions.push(session),
                // One unreadable row must not cost the node every other
                // session; it is dropped on the next write of that id.
                Err(err) => warn!(session_id, error = ?err, "Skipping unreadable session log row"),
            }
        }
        sessions.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        Ok(sessions)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Record a session. Returns at once; the row is committed with the
    /// next group.
    pub fn put(&self, session: PersistedSession) {
        let _ = self.tx.send(Op::Put(Box::new(session)));
    }

    /// Forget a session.
    pub fn delete(&self, session_id: &str) {
        let _ = self.tx.send(Op::Delete(session_id.to_string()));
    }

    /// Wait until everything handed to the log so far is committed. For a
    /// worker shutting down gracefully, and tests.
    pub fn sync(&self, timeout: Duration) -> bool {
        let (done_tx, done_rx) = mpsc::channel();
        if self.tx.send(Op::Sync(done_tx)).is_err() {
            return false;
        }
        done_rx.recv_timeout(timeout).is_ok()
    }
}

fn writer(mut conn: Connection, rx: mpsc::Receiver<Op>, path: &Path) {
    loop {
        // Block for the first op of a group; a closed channel ends the
        // writer once everything sent has been written.
        let Ok(first) = rx.recv() else {
            return;
        };
        let deadline = Instant::now() + GROUP_COMMIT;
        let mut pending: HashMap<String, Option<Box<PersistedSession>>> = HashMap::new();
        let mut waiters = Vec::new();
        let mut closed = false;
        let mut next = Some(first);
        loop {
            match next.take() {
                Some(Op::Put(session)) => {
                    pending.insert(session.session_id.clone(), Some(session));
                }
                Some(Op::Delete(session_id)) => {
                    pending.insert(session_id, None);
                }
                // A sync commits now rather than at the deadline.
                Some(Op::Sync(done)) => {
                    waiters.push(done);
                    break;
                }
                None => {}
            }
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match rx.recv_timeout(deadline - now) {
                Ok(op) => next = Some(op),
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    closed = true;
                    break;
                }
            }
        }
        if !pending.is_empty() {
            if let Err(err) = commit(&mut conn, pending) {
                warn!(path = %path.display(), error = ?err, "Failed to commit the session log");
            }
        }
        for done in waiters {
            let _ = done.send(());
        }
        if closed {
            return;
        }
    }
}

fn commit(conn: &mut Connection, pending: HashMap<String, Option<Box<PersistedSession>>>) -> R<()> {
    let tx = conn.transaction()?;
    {
        let mut upsert = tx.prepare_cached(
            "INSERT INTO sessions (session_id, state, row) VALUES (?1, ?2, ?3)
             ON CONFLICT(session_id) DO UPDATE SET state = excluded.state, row = excluded.row",
        )?;
        let mut delete = tx.prepare_cached("DELETE FROM sessions WHERE session_id = ?1")?;
        for (session_id, row) in pending {
            match row {
                Some(session) => {
                    let json = serde_json::to_string(&session)?;
                    upsert.execute(params![session_id, session.state.as_str(), json])?;
                }
                None => {
                    delete.execute(params![session_id])?;
                }
            }
        }
    }
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod session_log_test {
    use super::*;

    fn session(id: &str, state: SessionLogState) -> PersistedSession {
        PersistedSession {
            session_id: id.into(),
            claim_id: format!("ses_{id}"),
            local_key: -3,
            job_id: "agents".into(),
            job_config_id: "agents-1".into(),
            task_name: "workspace".into(),
            resources: serde_json::json!({ "cpu": 250 }),
            gate: "serve".into(),
            origin: "manager".into(),
            on_refused: "stop".into(),
            ticket_jti: String::new(),
            lease_issued_at_unix_ms: 7,
            generation: 1,
            state,
            process: None,
            checkpoint: None,
            payload: None,
            keep_for_ms: 0,
            evicted_reason: String::new(),
            updated_at_unix_ms: 9,
        }
    }

    fn checkpoint(bytes: u64) -> SessionCheckpoint {
        SessionCheckpoint {
            snapshot_id: format!("snap-{bytes}"),
            kind: "memory".into(),
            manifest_json: "{}".into(),
            total_bytes: bytes,
            taken_at_unix_ms: 5,
        }
    }

    /// Suspend, evict, and expire: the rows a sleeping session goes through,
    /// and that each survives a reopen of the log.
    #[test]
    fn suspended_and_evicted_rows_survive_a_reopen_and_expire_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("sessions.db");
        let log = SessionLog::open(&path)?;
        let mut running = session("a", SessionLogState::Running);
        running.payload = Some(crate::structs::TaskAllocation {
            task_name: "workspace".into(),
            ..Default::default()
        });
        running.keep_for_ms = 60_000;
        let suspended = running.clone().into_suspended(checkpoint(4096), 1_000);
        assert_eq!(suspended.state, SessionLogState::Suspended);
        assert!(suspended.payload.is_some(), "a resume needs the payload");
        log.put(suspended.clone());
        let evicted = session("b", SessionLogState::Running)
            .into_suspended(checkpoint(1), 10)
            .into_evicted("disk pressure", 2_000);
        assert_eq!(evicted.state, SessionLogState::Evicted);
        assert!(evicted.checkpoint.is_none() && evicted.payload.is_none());
        log.put(evicted);
        assert!(log.sync(Duration::from_secs(5)));
        drop(log);

        let rows = SessionLog::load(&path)?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].checkpoint, Some(checkpoint(4096)));
        assert_eq!(rows[0].keep_for(), Duration::from_secs(60));
        assert_eq!(rows[1].evicted_reason, "disk pressure");
        // The default keepFor is the plan's 24h.
        assert_eq!(
            rows[1].keep_for(),
            crate::structs::suspend::DEFAULT_KEEP_FOR
        );

        // keepFor counts from the last change.
        assert!(!rows[0].expired(1_000 + 59_999));
        assert!(rows[0].expired(1_000 + 60_000));
        assert!(!rows[1].expired(2_000 + 86_399_999));
        assert!(rows[1].expired(2_000 + 86_400_000));
        // A running session never expires.
        assert!(!session("c", SessionLogState::Running).expired(i64::MAX));
        Ok(())
    }

    #[test]
    fn disk_pressure_evicts_the_oldest_suspend_first_test() {
        let at = |id: &str, when: i64, bytes: u64| {
            session(id, SessionLogState::Running).into_suspended(checkpoint(bytes), when)
        };
        let rows = vec![
            at("new", 300, 10),
            at("old", 100, 10),
            at("tie-b", 200, 10),
            at("tie-a", 200, 10),
            // Holds no bytes (a disk checkpoint): evicting it frees nothing.
            at("empty", 50, 0),
            session("running", SessionLogState::Running),
            at("gone", 10, 10).into_evicted("keepFor expired", 20),
        ];
        assert_eq!(
            eviction_order(&rows),
            vec!["old", "tie-a", "tie-b", "new"],
            "oldest suspend first, ties by id, nothing that frees no bytes"
        );
    }

    #[test]
    fn rows_survive_a_reopen_and_deletes_stick_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("sessions.db");
        let log = SessionLog::open(&path)?;
        log.put(session("a", SessionLogState::Running));
        log.put(session("b", SessionLogState::Running));
        log.put(session("c", SessionLogState::Suspended));
        log.delete("b");
        assert!(log.sync(Duration::from_secs(5)));
        drop(log);

        let rows = SessionLog::load(&path)?;
        let ids: Vec<_> = rows.iter().map(|row| row.session_id.as_str()).collect();
        assert_eq!(ids, vec!["a", "c"]);
        assert_eq!(rows[1].state, SessionLogState::Suspended);
        Ok(())
    }

    #[test]
    fn a_burst_commits_in_groups_not_per_row_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("sessions.db");
        let log = SessionLog::open(&path)?;
        let started = Instant::now();
        for n in 0..5_000 {
            log.put(session(&format!("s{n:05}"), SessionLogState::Running));
        }
        // Handing rows over never waits on the disk.
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(log.sync(Duration::from_secs(30)));
        assert_eq!(SessionLog::load(&path)?.len(), 5_000);
        Ok(())
    }

    #[test]
    fn the_last_write_of_an_id_in_a_group_wins_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("sessions.db");
        let log = SessionLog::open(&path)?;
        log.put(session("a", SessionLogState::Running));
        log.delete("a");
        log.put(session("a", SessionLogState::Suspended));
        assert!(log.sync(Duration::from_secs(5)));
        let rows = SessionLog::load(&path)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, SessionLogState::Suspended);
        Ok(())
    }

    #[test]
    fn a_missing_log_loads_empty_and_a_bad_row_is_skipped_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("sessions.db");
        assert!(SessionLog::load(&path)?.is_empty());
        let log = SessionLog::open(&path)?;
        log.put(session("a", SessionLogState::Running));
        assert!(log.sync(Duration::from_secs(5)));
        connect(&path)?.execute(
            "INSERT INTO sessions (session_id, state, row) VALUES ('bad', 'running', '{')",
            [],
        )?;
        assert_eq!(SessionLog::load(&path)?.len(), 1);
        Ok(())
    }
}
