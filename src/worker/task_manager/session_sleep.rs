//! Sessions sleeping on their own node (session durability plan §6.6,
//! Phase 5a; docs/session-mode.md, "Idle sessions").
//!
//! A session task whose `suspend` block keeps `durability = "local"` may
//! suspend on the node that runs it:
//!
//! - **Suspend** captures a checkpoint into the node content store (pinned
//!   under its snapshot id, charged to the `snapshots` budget), stops the
//!   process, frees the session's share of the envelope (its claim leaves
//!   the claim table), and records a `suspended` row in `sessions.db`, all
//!   in one critical section so no report ever sees the session neither
//!   running nor suspended. Triggered by the task's `suspend.idle` policy on
//!   the node, or by `Worker.SuspendSessions` (a manager routing `legion
//!   instance suspend <id>` by the id's node tag).
//! - **Resume** (`Worker.ResumeSession`) admits the session into its
//!   envelope again under the same id, and restores it from the local
//!   checkpoint (a memory checkpoint), or boots it fresh (a disk checkpoint,
//!   which of a session holds only its spec, or a `cold` resume). A full
//!   envelope refuses it with the checkpoint kept.
//! - **Eviction**: a suspended session's checkpoint is pinned until
//!   `suspend.keepFor` expires, its envelope leaves the node (the job
//!   stopped), or `content.minFreeDisk` pressure forces it out, oldest
//!   first. The id then stays as an `evicted` row for another `keepFor`.
//! - **Live checkpoints**: `suspend.checkpointEvery` checkpoints running
//!   sessions periodically without stopping them, and
//!   `suspend.onWorkerStop = "checkpoint"` checkpoints them when the worker
//!   stops gracefully. Each replaces the session's previous live
//!   checkpoint, and a worker that restarts without the session's process
//!   restores it from that checkpoint here.
//!
//! Nothing here writes to raft or talks to a manager. `durability =
//! "store"` for unpromoted sessions (publishing, generations, moves) is
//! Phase 5b and refused at the node until then.

use super::*;
use crate::content_store::PinOwner;
use crate::structs::checkpoint::SnapshotKind;
use crate::structs::snapshot::SnapshotDurability;
use crate::structs::suspend::{OnWorkerStop, ResolvedSuspend};
use crate::structs::{CheckpointRequest, SessionStatus};
use crate::task_plugins::AfterCheckpoint;
use crate::worker::session_log::{SessionCheckpoint, SessionLogState};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Why a session could not be suspended, checkpointed, or resumed on this
/// node. The worker's RPCs map each to a gRPC code.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionSleepError {
    /// The node holds no such session (never had it, or it ended).
    NotHere,
    /// Its local checkpoint was evicted: `at <time> (<reason>)`.
    Evicted(String),
    /// The session cannot sleep here: its task sets no `suspend` block,
    /// asks for `durability = "store"`, or it was promoted.
    NotSuspendable(String),
    /// Another capture or resume of the session is in progress.
    Busy,
}

impl std::fmt::Display for SessionSleepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionSleepError::NotHere => write!(f, "no such session on this node"),
            SessionSleepError::Evicted(when) => {
                write!(f, "the session's local checkpoint was evicted {when}")
            }
            SessionSleepError::NotSuspendable(why) => write!(f, "{why}"),
            SessionSleepError::Busy => write!(
                f,
                "another suspend, checkpoint, or resume of this session is in progress; retry \
                 shortly"
            ),
        }
    }
}

impl std::error::Error for SessionSleepError {}

/// What a capture of a running session needs, taken under the lock.
#[derive(Debug, Clone)]
pub struct CapturePlan {
    pub session_id: String,
    pub local_key: i64,
    pub kind: SnapshotKind,
}

/// What [`suspend_session`] did.
#[derive(Debug, Clone, PartialEq)]
pub enum SuspendOutcome {
    Suspended(SessionCheckpoint),
    AlreadySuspended(SessionCheckpoint),
}

/// What [`resume_session`] did.
#[derive(Debug, Clone, PartialEq)]
pub enum ResumeOutcome {
    /// Restored from its checkpoint (`local`), or booted fresh (`cold`).
    Resumed { from: &'static str },
    /// It was running already.
    AlreadyRunning,
}

/// One running session that is due a periodic checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueCheckpoint {
    pub session_id: String,
    /// When it fell due, unix ms.
    pub due_at_ms: i64,
    /// Its task's `suspend.checkpointEvery`.
    pub every: Duration,
}

/// What one periodic checkpoint round decided.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckpointRound {
    /// Captured this round, most overdue first.
    pub run: Vec<String>,
    /// Skipped: the node's backlog would not let them be captured within
    /// one of their own intervals, so they wait for the next one instead of
    /// queueing (`legion_session_checkpoints_skipped_total`).
    pub skipped: Vec<String>,
}

/// Decide a periodic checkpoint round (session durability plan §6.6):
/// most overdue first, one capture at a time, and a session whose turn
/// would come later than one of its own intervals from now (at the node's
/// recent capture time `per_capture`) is skipped rather than queued, so a
/// node that cannot keep up never falls further behind.
pub fn plan_checkpoint_round(
    mut due: Vec<DueCheckpoint>,
    per_capture: Duration,
) -> CheckpointRound {
    due.sort_by(|a, b| {
        a.due_at_ms
            .cmp(&b.due_at_ms)
            .then_with(|| a.session_id.cmp(&b.session_id))
    });
    let mut round = CheckpointRound::default();
    let mut backlog = Duration::ZERO;
    for entry in due {
        if backlog > entry.every {
            round.skipped.push(entry.session_id);
            continue;
        }
        backlog += per_capture;
        round.run.push(entry.session_id);
    }
    round
}

/// When a session's first periodic checkpoint falls due: between half an
/// interval and a whole one after it started, spread by its id, so sessions
/// that started together do not all checkpoint together.
pub fn first_checkpoint_due(session_id: &str, started_at_ms: i64, every: Duration) -> i64 {
    use sha2::{Digest, Sha256};
    let every_ms = every.as_millis().max(2) as i64;
    let digest = Sha256::digest(session_id.as_bytes());
    let spread = u64::from_be_bytes(digest[..8].try_into().expect("8 bytes")) as i64;
    started_at_ms + every_ms / 2 + spread.rem_euclid(every_ms / 2)
}

/// The sessions an idle report says to suspend: entries for sessions
/// (negative local keys) whose policy's `after` has passed since they went
/// quiet. `now_unix` in seconds.
pub fn idle_sessions_to_suspend(
    allocations: &[crate::structs::AllocationUsage],
    now_unix: i64,
) -> Vec<i64> {
    allocations
        .iter()
        .filter(|usage| usage.allocation_id < 0)
        .filter(|usage| usage.idle_since_unix > 0 && usage.idle_after_seconds > 0)
        .filter(|usage| now_unix - usage.idle_since_unix >= usage.idle_after_seconds as i64)
        .map(|usage| usage.allocation_id)
        .collect()
}

impl WorkerTaskManager {
    /// The resolved `suspend` block of the task running under `local_key`.
    pub(crate) fn session_suspend_config(&self, local_key: i64) -> Option<ResolvedSuspend> {
        self.task_configs
            .get(&local_key)?
            .suspend
            .as_ref()?
            .resolve()
            .ok()
    }

    /// The running, unpromoted session with this id.
    fn running_session(&self, session_id: &str) -> Option<ClaimRecord> {
        self.claims
            .by_instance(session_id)
            .filter(|claim| claim.is_session() && !claim.is_ratified())
            .cloned()
    }

    /// The session id running under `local_key`, for an idle report entry.
    pub fn session_id_for_local_key(&self, local_key: i64) -> Option<String> {
        self.claims
            .by_local_key(local_key)
            .filter(|claim| claim.is_session() && !claim.is_ratified())
            .map(|claim| claim.instance_id.clone())
    }

    /// Check that a running session may be captured, and mark the capture
    /// in progress. `for_suspend` also needs the task to sleep locally; a
    /// live checkpoint needs only a `suspend` block.
    pub fn begin_session_capture(
        &mut self,
        session_id: &str,
        for_suspend: bool,
    ) -> Result<CapturePlan, SessionSleepError> {
        if self.session_captures.contains(session_id)
            || self.resuming_sessions.contains_key(session_id)
        {
            return Err(SessionSleepError::Busy);
        }
        let claim = match self.running_session(session_id) {
            Some(claim) => claim,
            None => {
                if self.claims.by_instance(session_id).is_some() {
                    return Err(SessionSleepError::NotSuspendable(format!(
                        "session {session_id} was promoted to a keyed instance; suspend it \
                         with `legion instance suspend`, which goes through its row"
                    )));
                }
                return Err(self.idle_answer(session_id));
            }
        };
        if !matches!(claim.stage, ClaimStage::Serving | ClaimStage::Booted) {
            return Err(SessionSleepError::NotSuspendable(format!(
                "session {session_id} is still starting; suspend it once it is running"
            )));
        }
        let Some(suspend) = self.session_suspend_config(claim.local_key) else {
            return Err(SessionSleepError::NotSuspendable(format!(
                "task {} sets no `suspend` block, so its sessions cannot sleep. Add \
                 `suspend = {{ durability = \"local\" }}` to the task, or promote the session \
                 first: `legion instance promote {session_id}`",
                claim.task_name
            )));
        };
        if for_suspend && suspend.durability == SnapshotDurability::Store {
            // Phase 5b seam: a `store` session publishes its checkpoint and
            // claims a generation with a conditional manifest write here.
            return Err(SessionSleepError::NotSuspendable(format!(
                "task {} sets suspend.durability = \"store\", which unpromoted sessions do not \
                 support yet. Use durability = \"local\" (the checkpoint stays on this node), \
                 or promote the session first: `legion instance promote {session_id}`",
                claim.task_name
            )));
        }
        let kind = self
            .local_suspend_kind(claim.local_key)
            .map_err(|err| SessionSleepError::NotSuspendable(format!("{err:#}")))?;
        self.session_captures.insert(session_id.to_string());
        Ok(CapturePlan {
            session_id: session_id.to_string(),
            local_key: claim.local_key,
            kind,
        })
    }

    /// Clear a capture mark after a capture that did not finish.
    pub fn abort_session_capture(&mut self, session_id: &str) {
        self.session_captures.remove(session_id);
    }

    /// The error for a session id that runs nothing here.
    fn idle_answer(&self, session_id: &str) -> SessionSleepError {
        match self.idle_sessions.get(session_id) {
            Some(row) if row.state == SessionLogState::Evicted => {
                SessionSleepError::Evicted(self.evicted_session(session_id).unwrap_or_default())
            }
            _ => SessionSleepError::NotHere,
        }
    }

    /// The job and task of a session the node holds that runs nothing
    /// (suspended or evicted), for a ticket check.
    pub fn idle_session_owner(&self, session_id: &str) -> Option<(String, String)> {
        self.idle_sessions
            .get(session_id)
            .map(|row| (row.job_id.clone(), row.task_name.clone()))
    }

    /// The checkpoint a suspended session holds, when it is suspended here.
    pub fn suspended_checkpoint(&self, session_id: &str) -> Option<SessionCheckpoint> {
        self.idle_sessions
            .get(session_id)
            .filter(|row| row.state == SessionLogState::Suspended)
            .and_then(|row| row.checkpoint.clone())
    }

    /// The last step of a session suspend, under the lock, once its
    /// checkpoint is stored: stop the process, free its envelope share, and
    /// record the `suspended` row. Fails, touching nothing, when the session
    /// ended or changed while it was captured.
    pub(crate) fn finish_session_suspend(
        &mut self,
        plan: &CapturePlan,
        paused: bool,
        checkpoint: SessionCheckpoint,
    ) -> R<()> {
        self.session_captures.remove(&plan.session_id);
        let claim = self
            .running_session(&plan.session_id)
            .filter(|claim| claim.local_key == plan.local_key)
            .with_context(|| {
                format!(
                    "session {} ended while its checkpoint was captured",
                    plan.session_id
                )
            })?;
        let keep_for = self
            .session_suspend_config(plan.local_key)
            .map(|suspend| suspend.keep_for)
            .unwrap_or(crate::structs::suspend::DEFAULT_KEEP_FOR);
        let mut row = self.session_row(&claim, SessionLogState::Suspended);
        row.keep_for_ms = keep_for.as_millis() as u64;
        if let Err(err) = self.checkpoint_finish_suspend(plan.local_key, paused) {
            // The capture is whole; a teardown hiccup must not lose it.
            warn!(
                session_id = %plan.session_id,
                error = ?err,
                "Suspended session did not stop cleanly"
            );
        }
        self.claims.remove(&claim.claim_id);
        self.orphaned_sessions.remove(&claim.claim_id);
        self.failed
            .retain(|failure| failure.allocation_id != plan.local_key);
        self.terminated_reports
            .retain(|report| report.allocation_id != plan.local_key);
        self.allocation_health.remove(&plan.local_key);
        self.session_payloads.remove(&plan.session_id);
        self.session_checkpoint_due.remove(&plan.session_id);
        if let Some(previous) = self.session_checkpoints.remove(&plan.session_id) {
            if previous.snapshot_id != checkpoint.snapshot_id {
                self.release_session_checkpoint(&previous);
            }
        }
        let row = row.into_suspended(checkpoint, crate::util::now_unix_ms());
        info!(
            session_id = %plan.session_id,
            snapshot_id = %row.checkpoint.as_ref().map(|c| c.snapshot_id.as_str()).unwrap_or(""),
            "Session suspended"
        );
        if let Some(log) = self.session_log() {
            log.put(row.clone());
        }
        self.idle_sessions.insert(plan.session_id.clone(), row);
        Ok(())
    }

    /// Record a live checkpoint of a running session (periodic or
    /// worker-stop), replacing its previous one, and write it to the
    /// session's row so a restart without the process restores from it.
    /// Returns false (and releases the new checkpoint) when the session
    /// ended meanwhile.
    pub(crate) fn record_session_checkpoint(
        &mut self,
        plan: &CapturePlan,
        checkpoint: SessionCheckpoint,
    ) -> bool {
        self.session_captures.remove(&plan.session_id);
        let live = self
            .running_session(&plan.session_id)
            .is_some_and(|claim| claim.local_key == plan.local_key);
        if !live {
            self.release_session_checkpoint(&checkpoint);
            return false;
        }
        if let Some(previous) = self
            .session_checkpoints
            .insert(plan.session_id.clone(), checkpoint)
        {
            self.release_session_checkpoint(&previous);
        }
        self.log_running_session(plan.local_key);
        true
    }

    /// Drop a checkpoint's pinned parts from the node content store.
    pub(crate) fn release_session_checkpoint(&self, checkpoint: &SessionCheckpoint) {
        if let Some(store) = self.content_store.as_deref() {
            crate::worker::checkpoints::release_snapshot_pins(
                store,
                &PinOwner::snapshot(&checkpoint.snapshot_id),
            );
        }
    }

    /// Forget what a running session kept beside its claim: its launch
    /// payload, its live checkpoint (pins released), its checkpoint
    /// schedule. Called when it ends or is promoted.
    pub(crate) fn forget_session_extras(&mut self, session_id: &str) {
        self.session_payloads.remove(session_id);
        self.session_checkpoint_due.remove(session_id);
        self.session_captures.remove(session_id);
        if let Some(checkpoint) = self.session_checkpoints.remove(session_id) {
            self.release_session_checkpoint(&checkpoint);
        }
    }

    /// End a suspended or evicted session: drop its checkpoint and its row.
    /// Returns whether the node held it.
    pub(crate) fn forget_idle_session(&mut self, session_id: &str, reason: &str) -> bool {
        let Some(row) = self.idle_sessions.remove(session_id) else {
            return false;
        };
        self.orphaned_idle_sessions.remove(session_id);
        if let Some(checkpoint) = row.checkpoint.as_ref() {
            self.release_session_checkpoint(checkpoint);
        }
        self.unlog_session(session_id);
        info!(
            session_id,
            state = row.state.as_str(),
            reason,
            "Ending idle session"
        );
        true
    }

    /// Evict a suspended session's checkpoint: its parts are released, and
    /// the id stays as an `evicted` row for another `keepFor`, so the node
    /// answers `evicted` rather than `not found`. Returns whether it was
    /// suspended here.
    pub fn evict_session(&mut self, session_id: &str, reason: &str) -> bool {
        let Some(row) = self.idle_sessions.remove(session_id) else {
            return false;
        };
        if row.state != SessionLogState::Suspended {
            self.idle_sessions.insert(session_id.to_string(), row);
            return false;
        }
        self.orphaned_idle_sessions.remove(session_id);
        if let Some(checkpoint) = row.checkpoint.as_ref() {
            self.release_session_checkpoint(checkpoint);
        }
        let row = row.into_evicted(reason, crate::util::now_unix_ms());
        warn!(
            session_id,
            reason, "Evicted a suspended session's local checkpoint"
        );
        crate::manager::telemetry::worker_metrics()
            .session_evictions_total
            .inc(&[eviction_label(reason)]);
        if let Some(log) = self.session_log() {
            log.put(row.clone());
        }
        self.idle_sessions.insert(session_id.to_string(), row);
        true
    }

    /// The idle-session sweep (every few seconds): evict suspended sessions
    /// past `keepFor` or whose envelope has been gone for
    /// [`SESSION_ORPHAN_GRACE`], forget evicted ones past `keepFor`, and
    /// under `content.minFreeDisk` pressure evict suspended checkpoints
    /// oldest first until the disk is back above the floor. Returns the
    /// evicted ids with their reasons.
    pub fn sweep_idle_sessions(&mut self, now: Instant, now_ms: i64) -> Vec<(String, String)> {
        let mut evicted = Vec::new();
        let mut ids: Vec<String> = self.idle_sessions.keys().cloned().collect();
        ids.sort();
        let mut envelopes: HashMap<(String, String), bool> = HashMap::new();
        for session_id in ids {
            let Some(row) = self.idle_sessions.get(&session_id) else {
                continue;
            };
            let (state, expired) = (row.state, row.expired(now_ms));
            let key = (row.job_id.clone(), row.task_name.clone());
            match state {
                SessionLogState::Evicted if expired => {
                    self.forget_idle_session(&session_id, "keepFor expired");
                }
                SessionLogState::Suspended if expired => {
                    let reason = "keepFor expired".to_string();
                    if self.evict_session(&session_id, &reason) {
                        evicted.push((session_id, reason));
                    }
                }
                SessionLogState::Suspended => {
                    let has_envelope = *envelopes
                        .entry(key.clone())
                        .or_insert_with(|| self.envelope_for(&key.0, &key.1).is_some());
                    if has_envelope {
                        self.orphaned_idle_sessions.remove(&session_id);
                        continue;
                    }
                    let since = *self
                        .orphaned_idle_sessions
                        .entry(session_id.clone())
                        .or_insert(now);
                    if now.duration_since(since) >= SESSION_ORPHAN_GRACE {
                        let reason =
                            "its envelope left the node (the job stopped or the node was drained)"
                                .to_string();
                        if self.evict_session(&session_id, &reason) {
                            evicted.push((session_id, reason));
                        }
                    }
                }
                _ => {}
            }
        }
        evicted.extend(self.evict_for_disk_pressure());
        evicted
    }

    /// Under `content.minFreeDisk` pressure, evict suspended sessions'
    /// checkpoints oldest first until the content filesystem is back above
    /// the floor or none is left.
    fn evict_for_disk_pressure(&mut self) -> Vec<(String, String)> {
        let Some(store) = self.content_store.clone() else {
            return Vec::new();
        };
        let floor = store.config().min_free_disk;
        let below = || {
            floor > 0
                && store
                    .free_disk_bytes()
                    .is_some_and(|free_bytes| free_bytes < floor)
        };
        if !below() {
            return Vec::new();
        }
        let mut evicted = Vec::new();
        let order = crate::worker::session_log::eviction_order(self.idle_sessions.values());
        for session_id in order {
            let reason = "disk pressure (content.minFreeDisk)".to_string();
            if self.evict_session(&session_id, &reason) {
                evicted.push((session_id, reason));
            }
            if !below() {
                break;
            }
        }
        evicted
    }

    /// Take a suspended session out for a resume: admit it into its
    /// envelope again under the same id, and build the payload that
    /// restores it (or boots it fresh, `cold`). A full envelope is a
    /// [`SessionRefusal`] and leaves the session suspended.
    pub fn begin_session_resume(
        &mut self,
        session_id: &str,
        cold: bool,
    ) -> R<Option<(ClaimRecord, TaskAllocation, bool)>> {
        if self.running_session(session_id).is_some() {
            return Ok(None);
        }
        if self.resuming_sessions.contains_key(session_id)
            || self.session_captures.contains(session_id)
        {
            return Err(SessionSleepError::Busy.into());
        }
        let Some(row) = self
            .idle_sessions
            .get(session_id)
            .filter(|row| row.state == SessionLogState::Suspended)
            .cloned()
        else {
            return Err(self.idle_answer(session_id).into());
        };
        let (Some(payload), Some(checkpoint)) = (row.payload.clone(), row.checkpoint.clone())
        else {
            return Err(SessionSleepError::NotSuspendable(format!(
                "session {session_id} has no launch payload or checkpoint recorded on this node"
            ))
            .into());
        };
        let provisional = payload
            .provisional
            .clone()
            .context("a session payload carries its provisional block")?;
        let resources: Value = if payload.task_resources.trim().is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_str(&payload.task_resources).unwrap_or_else(|_| serde_json::json!({}))
        };
        let mut record =
            crate::worker::edge::record_from_payload(&payload, &provisional, resources);
        // The same session: its original lease and generation (a local
        // session never moves, so its generation stays 1).
        record.session = Some(crate::worker::claims::SessionClaim {
            lease_issued_at_unix_ms: row.lease_issued_at_unix_ms,
            generation: row.generation,
        });
        let admitted = self.admit_session(record)?;
        let mut launch = payload;
        launch.allocation_id = admitted.local_key;
        let restoring = !cold && checkpoint.kind == SnapshotKind::Memory.as_str();
        if restoring {
            let on_resume = launch_suspend(&launch)
                .map(|suspend| suspend.on_resume)
                .unwrap_or_default();
            launch.restore = Some(crate::structs::SnapshotRestore {
                snapshot_id: checkpoint.snapshot_id.clone(),
                kind: checkpoint.kind.clone(),
                manifest_json: checkpoint.manifest_json.clone(),
                on_resume,
                ..Default::default()
            });
        }
        self.idle_sessions.remove(session_id);
        self.orphaned_idle_sessions.remove(session_id);
        self.resuming_sessions.insert(session_id.to_string(), row);
        Ok(Some((admitted, launch, restoring)))
    }

    /// Settle a resume once its launch returned. On success the checkpoint
    /// is released (the session's running row replaced the suspended one);
    /// on failure the session is suspended again with its checkpoint, so a
    /// retry can try once more.
    pub(crate) fn finish_session_resume(&mut self, session_id: &str, local_key: i64, ok: bool) {
        self.checkpoints.forget_restore(local_key);
        let Some(row) = self.resuming_sessions.remove(session_id) else {
            return;
        };
        if ok {
            if let Some(checkpoint) = row.checkpoint.as_ref() {
                self.release_session_checkpoint(checkpoint);
            }
            info!(session_id, "Session resumed");
            return;
        }
        // The failed launch ended its claim, which deleted the row from the
        // log; the log applies writes in order, so this puts it back.
        if let Some(log) = self.session_log() {
            log.put(row.clone());
        }
        self.idle_sessions.insert(session_id.to_string(), row);
    }

    /// The running sessions due a periodic checkpoint at `now_ms`
    /// (`suspend.checkpointEvery`), and the first due time of any that has
    /// none yet.
    pub fn sessions_due_for_checkpoint(&mut self, now_ms: i64) -> Vec<DueCheckpoint> {
        let mut due = Vec::new();
        for claim in self.claims.sessions() {
            if claim.is_ratified()
                || !matches!(claim.stage, ClaimStage::Serving | ClaimStage::Booted)
                || self.session_captures.contains(&claim.instance_id)
            {
                continue;
            }
            let Some(suspend) = self.session_suspend_config(claim.local_key) else {
                continue;
            };
            let (Some(every), SnapshotDurability::Local) =
                (suspend.checkpoint_every, suspend.durability)
            else {
                continue;
            };
            let started = claim
                .session
                .map(|session| session.lease_issued_at_unix_ms)
                .unwrap_or(now_ms);
            let due_at = *self
                .session_checkpoint_due
                .entry(claim.instance_id.clone())
                .or_insert_with(|| first_checkpoint_due(&claim.instance_id, started, every));
            if due_at <= now_ms {
                due.push(DueCheckpoint {
                    session_id: claim.instance_id.clone(),
                    due_at_ms: due_at,
                    every,
                });
            }
        }
        due
    }

    /// Push a session's next periodic checkpoint one interval past `now_ms`
    /// (after it ran, failed, or was skipped).
    pub fn reschedule_session_checkpoint(
        &mut self,
        session_id: &str,
        every: Duration,
        now_ms: i64,
    ) {
        if let Some(due) = self.session_checkpoint_due.get_mut(session_id) {
            *due = now_ms + every.as_millis() as i64;
        }
    }

    /// The running sessions a stopping worker checkpoints
    /// (`suspend.onWorkerStop = "checkpoint"`), least recently checkpointed
    /// first.
    pub fn sessions_to_checkpoint_on_stop(&self) -> Vec<String> {
        let mut sessions: Vec<(i64, String)> = self
            .claims
            .sessions()
            .into_iter()
            .filter(|claim| !claim.is_ratified())
            .filter(|claim| matches!(claim.stage, ClaimStage::Serving | ClaimStage::Booted))
            .filter(|claim| {
                self.session_suspend_config(claim.local_key)
                    .is_some_and(|suspend| {
                        suspend.on_worker_stop == OnWorkerStop::Checkpoint
                            && suspend.durability == SnapshotDurability::Local
                    })
            })
            .map(|claim| {
                let last = self
                    .session_checkpoints
                    .get(&claim.instance_id)
                    .map(|checkpoint| checkpoint.taken_at_unix_ms)
                    .unwrap_or(0);
                (last, claim.instance_id)
            })
            .collect();
        sessions.sort();
        sessions.into_iter().map(|(_, id)| id).collect()
    }

    /// Usage targets for the running sessions whose task has an idle policy
    /// and sleeps locally, under their (negative) local keys. The daemon
    /// samples them with the node's allocations and acts on their idle
    /// times itself; they never reach the manager's usage report.
    pub fn session_idle_targets(&self) -> Vec<crate::worker::usage::UsageTarget> {
        use crate::worker::usage::UsageSource;
        let mut targets = Vec::new();
        for claim in self.claims.sessions() {
            if claim.is_ratified() || self.session_captures.contains(&claim.instance_id) {
                continue;
            }
            let local_key = claim.local_key;
            let (Some(task), Some(instance)) = (
                self.task_configs.get(&local_key),
                self.instances.get(&local_key),
            ) else {
                continue;
            };
            let Some(suspend) = self.session_suspend_config(local_key) else {
                continue;
            };
            if suspend.idle.is_none() || suspend.durability != SnapshotDurability::Local {
                continue;
            }
            let sandbox = (task.runtime == "sandbox")
                .then(|| crate::task_plugins::sandbox_usage_source(local_key, instance))
                .unwrap_or_default();
            let source = match (
                instance.resource_assignment.as_ref(),
                sandbox.apple_container,
            ) {
                (Some(assignment), _) => UsageSource::Cgroup(assignment.absolute_cgroup_path()),
                (None, Some(name)) => UsageSource::AppleContainer(name),
                (None, None) => continue,
            };
            targets.push(crate::worker::usage::UsageTarget {
                allocation_id: local_key,
                source,
                tap_device: sandbox.tap_device,
                pressure: Default::default(),
                idle: crate::worker::idle::IdlePolicy::for_task(
                    task,
                    crate::worker::idle::local_mhz_per_core(),
                ),
            });
        }
        targets
    }

    /// Sessions the previous worker process checkpointed whose process did
    /// not survive: the daemon restores each on this node once it is up.
    pub fn take_pending_session_restores(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending_session_restores)
    }

    /// The status of a session the node holds, running or suspended, after
    /// a suspend or a resume.
    pub fn sleep_status(&self, session_id: &str) -> Option<SessionStatus> {
        self.session_status(session_id)
    }
}

/// The `suspend` block a launch payload carries.
fn launch_suspend(payload: &TaskAllocation) -> Option<ResolvedSuspend> {
    if payload.task_suspend.trim().is_empty() {
        return None;
    }
    serde_json::from_str::<crate::structs::suspend::SuspendConfig>(&payload.task_suspend)
        .ok()?
        .resolve()
        .ok()
}

/// The bounded `reason` label of `legion_session_evictions_total`.
fn eviction_label(reason: &str) -> &'static str {
    if reason.starts_with("keepFor") {
        "keep_for"
    } else if reason.starts_with("disk pressure") {
        "disk_pressure"
    } else if reason.starts_with("its envelope") {
        "envelope_gone"
    } else {
        "other"
    }
}

/// A checkpoint record from a finished capture's manifest.
fn checkpoint_from(manifest: &crate::structs::checkpoint::SnapshotManifest) -> SessionCheckpoint {
    SessionCheckpoint {
        snapshot_id: manifest.id.clone(),
        kind: manifest.kind.as_str().to_string(),
        manifest_json: serde_json::to_string(manifest).unwrap_or_default(),
        total_bytes: manifest.total_bytes(),
        taken_at_unix_ms: crate::util::now_unix_ms(),
    }
}

/// Suspend a running session on this node: capture its checkpoint, stop
/// it, free its envelope share, and record it `suspended`. A failed capture
/// leaves it running. `trigger` (`idle`, `manual`) is for the log.
pub fn suspend_session(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    session_id: &str,
    trigger: &str,
) -> R<SuspendOutcome> {
    let plan = {
        let mut guard = manager.lock().expect("task manager");
        if let Some(checkpoint) = guard.suspended_checkpoint(session_id) {
            return Ok(SuspendOutcome::AlreadySuspended(checkpoint));
        }
        guard.begin_session_capture(session_id, true)?
    };
    let request = CheckpointRequest {
        snapshot_id: crate::structs::snapshot::new_snapshot_id(),
        op: "capture".into(),
        allocation_id: plan.local_key,
        kind: plan.kind.as_str().to_string(),
        then: AfterCheckpoint::Stop.as_str().to_string(),
        purpose: "suspend".into(),
        ..Default::default()
    };
    let mut recorded = None;
    let captured = crate::worker::checkpoints::capture_for_suspend_then(
        manager,
        &request,
        plan.kind,
        |guard, context, manifest| {
            let checkpoint = checkpoint_from(manifest);
            recorded = Some(checkpoint.clone());
            guard.finish_session_suspend(&plan, context.paused, checkpoint)
        },
    );
    match captured {
        Ok(_) => {
            info!(
                session_id,
                trigger,
                kind = plan.kind.as_str(),
                "Suspended session"
            );
            Ok(SuspendOutcome::Suspended(
                recorded.context("suspend finished without a checkpoint")?,
            ))
        }
        Err(err) => {
            manager
                .lock()
                .expect("task manager")
                .abort_session_capture(session_id);
            warn!(session_id, trigger, error = %format!("{err:#}"), "Session suspend failed; it keeps running");
            Err(err)
        }
    }
}

/// Take a live checkpoint of a running session without stopping it (a
/// pause for a memory capture), replacing its previous one.
pub fn checkpoint_session(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    session_id: &str,
) -> R<SessionCheckpoint> {
    let plan = manager
        .lock()
        .expect("task manager")
        .begin_session_capture(session_id, false)?;
    let request = CheckpointRequest {
        snapshot_id: crate::structs::snapshot::new_snapshot_id(),
        op: "capture".into(),
        allocation_id: plan.local_key,
        kind: plan.kind.as_str().to_string(),
        then: AfterCheckpoint::Resume.as_str().to_string(),
        purpose: "session-checkpoint".into(),
        ..Default::default()
    };
    let report = crate::worker::checkpoints::run_capture(manager, &request);
    let mut guard = manager.lock().expect("task manager");
    if !report.ok {
        guard.abort_session_capture(session_id);
        bail!("session checkpoint failed: {}", report.error);
    }
    let manifest = crate::structs::checkpoint::SnapshotManifest::from_json(&report.manifest_json)?;
    let checkpoint = checkpoint_from(&manifest);
    if !guard.record_session_checkpoint(&plan, checkpoint.clone()) {
        bail!("session {session_id} ended while it was checkpointed");
    }
    Ok(checkpoint)
}

/// Resume a suspended session on this node under the same id, from its
/// local checkpoint (or fresh, `cold`). The caller holds a launch slot.
pub fn resume_session(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    session_id: &str,
    cold: bool,
) -> R<ResumeOutcome> {
    let Some((claim, payload, restoring)) = manager
        .lock()
        .expect("task manager")
        .begin_session_resume(session_id, cold)?
    else {
        return Ok(ResumeOutcome::AlreadyRunning);
    };
    let launched = launch_provisional_allocation(manager, &payload, &claim.claim_id, claim.gate);
    // A launch stopped while it started returns Ok with nothing running.
    let running = launched.is_ok()
        && manager
            .lock()
            .expect("task manager")
            .running_session(session_id)
            .is_some();
    manager.lock().expect("task manager").finish_session_resume(
        session_id,
        claim.local_key,
        running,
    );
    match launched {
        Ok(()) if running => Ok(ResumeOutcome::Resumed {
            from: if restoring { "local" } else { "cold" },
        }),
        Ok(()) => bail!("session {session_id} was stopped while it resumed"),
        Err(err) => Err(err.context(format!(
            "session {session_id} could not be resumed; it stays suspended with its checkpoint"
        ))),
    }
}

/// One periodic checkpoint round (`suspend.checkpointEvery`): plan it, run
/// the captures one at a time, and reschedule every session it touched.
/// `per_capture` is the node's recent capture time, updated here. Returns
/// how many ran and how many were skipped.
pub fn run_checkpoint_round(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    per_capture: &mut Duration,
) -> (usize, usize) {
    let now_ms = crate::util::now_unix_ms();
    let due = manager
        .lock()
        .expect("task manager")
        .sessions_due_for_checkpoint(now_ms);
    if due.is_empty() {
        return (0, 0);
    }
    let every: HashMap<String, Duration> = due
        .iter()
        .map(|entry| (entry.session_id.clone(), entry.every))
        .collect();
    let round = plan_checkpoint_round(due, *per_capture);
    if !round.skipped.is_empty() {
        let mut guard = manager.lock().expect("task manager");
        for session_id in &round.skipped {
            guard.reschedule_session_checkpoint(session_id, every[session_id], now_ms);
        }
        crate::manager::telemetry::worker_metrics()
            .session_checkpoints_skipped_total
            .add(round.skipped.len() as u64);
        warn!(
            skipped = round.skipped.len(),
            running = round.run.len(),
            "Skipping periodic session checkpoints: the node cannot capture them within one \
             interval"
        );
    }
    for session_id in &round.run {
        let started = Instant::now();
        if let Err(err) = checkpoint_session(manager, session_id) {
            debug!(session_id, error = %format!("{err:#}"), "Periodic session checkpoint failed");
        }
        // A moving average, so one slow capture does not skip a round.
        *per_capture = (*per_capture * 3 + started.elapsed()) / 4;
        manager
            .lock()
            .expect("task manager")
            .reschedule_session_checkpoint(
                session_id,
                every[session_id],
                crate::util::now_unix_ms(),
            );
    }
    (round.run.len(), round.skipped.len())
}

/// Checkpoint every running session whose task sets `suspend.onWorkerStop
/// = "checkpoint"`, least recently checkpointed first, until `deadline`.
/// The sessions keep running. Returns how many were checkpointed and how
/// many were left when time ran out.
pub fn checkpoint_sessions_on_stop(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    deadline: Instant,
) -> (usize, usize) {
    let sessions = manager
        .lock()
        .expect("task manager")
        .sessions_to_checkpoint_on_stop();
    let total = sessions.len();
    let mut done = 0;
    for session_id in sessions {
        if Instant::now() >= deadline {
            break;
        }
        match checkpoint_session(manager, &session_id) {
            Ok(_) => done += 1,
            Err(err) => warn!(
                session_id = %session_id,
                error = %format!("{err:#}"),
                "Could not checkpoint a session before the worker stopped"
            ),
        }
    }
    (done, total - done)
}

#[cfg(test)]
mod session_sleep_test {
    use super::*;

    fn due(id: &str, at: i64, every_secs: u64) -> DueCheckpoint {
        DueCheckpoint {
            session_id: id.into(),
            due_at_ms: at,
            every: Duration::from_secs(every_secs),
        }
    }

    #[test]
    fn a_round_runs_the_most_overdue_first_and_skips_past_one_interval_test() {
        // Ten seconds a capture, a sixty-second interval: seven start before
        // the backlog passes one interval, the rest wait for the next round.
        let sessions: Vec<_> = (0..10)
            .map(|n| due(&format!("s{n}"), 1_000 - n as i64, 60))
            .collect();
        let round = plan_checkpoint_round(sessions, Duration::from_secs(10));
        assert_eq!(round.run, vec!["s9", "s8", "s7", "s6", "s5", "s4", "s3"]);
        assert_eq!(round.skipped, vec!["s2", "s1", "s0"]);

        // A node that keeps up runs everything.
        let round = plan_checkpoint_round(
            vec![due("a", 5, 600), due("b", 5, 600)],
            Duration::from_secs(1),
        );
        assert_eq!(round.run, vec!["a", "b"]);
        assert!(round.skipped.is_empty());
    }

    #[test]
    fn first_checkpoints_are_spread_over_the_second_half_interval_test() {
        let every = Duration::from_secs(600);
        let mut dues: Vec<i64> = (0..50)
            .map(|n| first_checkpoint_due(&format!("workspace-{n}"), 1_000_000, every))
            .collect();
        for due in &dues {
            assert!((1_300_000..1_600_000).contains(due), "{due}");
        }
        dues.sort();
        dues.dedup();
        assert!(dues.len() > 40, "sessions started together are spread out");
        assert_eq!(
            first_checkpoint_due("workspace-1", 0, every),
            first_checkpoint_due("workspace-1", 0, every),
            "and the spread is stable"
        );
    }

    #[test]
    fn only_sessions_quiet_past_their_window_are_suspended_test() {
        let usage = |id: i64, since: i64, after: u64| crate::structs::AllocationUsage {
            allocation_id: id,
            idle_since_unix: since,
            idle_after_seconds: after,
            ..Default::default()
        };
        let report = vec![
            usage(-1, 1_000, 180), // quiet 200s, window 180s: suspend
            usage(-2, 1_100, 180), // quiet 100s: not yet
            usage(-3, 0, 0),       // no idle policy
            usage(7, 1_000, 180),  // an allocation: the manager's to decide
        ];
        assert_eq!(idle_sessions_to_suspend(&report, 1_200), vec![-1]);
    }
}
