//! Sessions whose task sets `suspend.durability = "store"` (session
//! durability plan §6.6 and §6.7, Phase 5b; docs/session-mode.md, "Idle
//! sessions", "Moving a session", and "No split brain").
//!
//! Everything in [`super::session_sleep`] applies, and on top of it:
//!
//! - **Publish.** A suspend, a periodic checkpoint, and a checkpoint at
//!   worker stop publish the checkpoint's memory parts sealed to the
//!   worker's `contentStore` (part keys derived from the job's session
//!   sealing key, which rides on the envelope payload, so no manager is on
//!   the path), then write the session manifest
//!   ([`crate::session_manifest`]) with a conditional write. After that the
//!   local parts are an ordinary cache entry.
//! - **Generations.** Every resume claims generation g+1 with a conditional
//!   write before its gate opens; a lost write aborts the resume and names
//!   the winner. An idempotent (derived-id) start creates the manifest with
//!   `If-None-Match: *` before its gate opens.
//! - **Moves.** A node that does not hold the session resumes it from its
//!   manifest: admit, claim, fence the old node, launch.
//! - **Convergence.** A copy whose own write finds a newer generation ends
//!   itself (`superseded`); the fence list stops copies a move left behind;
//!   a node cut off from managers and the bucket for `stateful.fenceAfter`
//!   suspends its running `store` sessions locally and, on reconnect,
//!   resumes only those the bucket still names it for.
//!
//! Every bucket call here runs off the task-manager lock.

use super::*;
use crate::session_manifest::{ManifestError, ManifestState, SessionManifest, SessionManifests};
use crate::structs::checkpoint::{SnapshotKind, SnapshotManifest};
use crate::structs::snapshot::SnapshotDurability;
use crate::worker::session_log::{PersistedSession, SessionCheckpoint, SessionLogState};
use crate::worker::task_manager::session_sleep::SessionSleepError;
use std::sync::{Arc, Mutex};

/// Whether a stop reason leaves the session's manifest in place: the copy
/// that ended was stale (superseded, fenced, a duplicate), so the manifest
/// belongs to the copy that won. Every other end of a `store` session
/// (stopped, destroyed, exited) deletes the manifest it still owns.
pub(crate) fn reason_keeps_manifest(reason: &str) -> bool {
    ["superseded", "duplicate", "fence"]
        .iter()
        .any(|word| reason.contains(word))
}

/// A manifest error as the session-sleep error the RPCs map to gRPC codes.
pub fn sleep_error(err: ManifestError) -> SessionSleepError {
    match err {
        ManifestError::Superseded { .. } | ManifestError::Lost { .. } => {
            SessionSleepError::Superseded(err.to_string())
        }
        ManifestError::AlreadyExists { .. } => SessionSleepError::AlreadyExists(err.to_string()),
        ManifestError::NotFound => SessionSleepError::NotHere,
        ManifestError::Unavailable(_) => SessionSleepError::StoreUnavailable(err.to_string()),
    }
}

/// A hook a move runs once its generation claim is won, before it launches
/// (an edge move stops the old copy it probed there).
pub type AfterClaim = Box<dyn FnOnce(&SessionManifest) + Send>;

/// What a resume on a node that did not hold the session did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveOutcome {
    /// `store` (restored from the published memory checkpoint) or `cold`
    /// (a disk checkpoint, or none yet: booted fresh under the same id).
    /// Empty when it was already running here.
    pub from: &'static str,
    pub generation: u64,
    /// The node the manifest named before the move ("" when it was this
    /// node).
    pub moved_from: String,
}

/// A resume of a `store` session on this node from its manifest.
#[derive(Debug, Clone)]
pub struct MoveRequest {
    pub session_id: String,
    pub job_id: String,
    pub task_name: String,
    /// Move even when the manifest names another node as running it.
    pub force_move: bool,
    pub cold: bool,
    /// `manager` or `edge`, and the ticket's `jti` for an edge resume.
    pub origin: String,
    pub ticket_jti: String,
}

impl WorkerTaskManager {
    /// The session manifests, or why a `store` session cannot use them.
    pub(crate) fn require_manifests(&self) -> Result<Arc<SessionManifests>, SessionSleepError> {
        self.session_manifests.clone().ok_or_else(|| {
            SessionSleepError::NotSuspendable(
                "this worker has no contentStore configured, so it cannot keep sessions with \
                 suspend.durability = \"store\"\nHint: add contentStore { url = \"s3://...\" } \
                 to this worker's config (docs/artifacts.md, \"Publishing content\"), or use \
                 durability = \"local\""
                    .to_string(),
            )
        })
    }

    /// A copy of a session's idle row.
    pub(crate) fn idle_row(&self, session_id: &str) -> Option<PersistedSession> {
        self.idle_sessions.get(session_id).cloned()
    }

    /// The manifest URL a suspended `store` session was published to, when
    /// it was.
    pub fn published_url(&self, session_id: &str) -> String {
        self.idle_sessions
            .get(session_id)
            .map(|row| row.published_url.clone())
            .unwrap_or_default()
    }

    /// Record that a suspended session's checkpoint is published: its row
    /// carries the manifest URL and the part URLs, and the local parts are
    /// unpinned (an ordinary cache entry from now on). False when the row
    /// changed meanwhile (resumed, ended, re-suspended).
    pub(crate) fn mark_published(
        &mut self,
        session_id: &str,
        published: SessionCheckpoint,
        url: &str,
    ) -> bool {
        let Some(row) = self.idle_sessions.get_mut(session_id) else {
            return false;
        };
        let same = row
            .checkpoint
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.snapshot_id == published.snapshot_id);
        if !same || row.state != SessionLogState::Suspended {
            return false;
        }
        row.checkpoint = Some(published.clone());
        row.published_url = url.to_string();
        let row = row.clone();
        if let Some(log) = self.session_log() {
            log.put(row);
        }
        self.unpin_published(&published);
        true
    }

    /// Unpin a published checkpoint's parts on this node, keeping the bytes
    /// (docs/suspend-and-resume.md, "Durability").
    fn unpin_published(&self, checkpoint: &SessionCheckpoint) {
        if let Some(store) = self.content_store.as_deref() {
            if let Err(err) = store.release_owner(&crate::content_store::PinOwner::snapshot(
                &checkpoint.snapshot_id,
            )) {
                debug!(snapshot_id = %checkpoint.snapshot_id, error = ?err, "Failed to unpin published session parts");
            }
        }
    }

    /// End this node's copy of a session because a newer generation runs
    /// elsewhere (`via`: write | fence_list | resume | reconnect). Its
    /// manifest stays: it belongs to the winner. Returns whether the node
    /// held the session.
    pub fn end_superseded(&mut self, session_id: &str, via: &str) -> bool {
        let ended = if self.idle_sessions.contains_key(session_id) {
            self.forget_idle_session(session_id, &format!("superseded ({via})"))
        } else {
            !self
                .stop_sessions(&[session_id.to_string()], &format!("superseded ({via})"))
                .is_empty()
        };
        if ended {
            warn!(
                session_id,
                via, "Ended a superseded session copy: a newer generation runs elsewhere"
            );
            crate::manager::telemetry::worker_metrics()
                .session_superseded_total
                .inc(&[via]);
        }
        ended
    }

    /// Queue the manifest delete of a `store` session that ended for good
    /// on this node. Skipped for a copy that ended because it was stale.
    pub(crate) fn queue_manifest_delete(&mut self, claim: &ClaimRecord) {
        if self.session_manifests.is_none() {
            return;
        }
        if self.keep_manifest.remove(&claim.instance_id) {
            return;
        }
        // A resume whose launch failed: the session goes back to suspended,
        // and its manifest with it.
        if self.resuming_sessions.contains_key(&claim.instance_id) {
            return;
        }
        let store = self.session_is_store(claim.local_key)
            || self
                .session_payloads
                .get(&claim.instance_id)
                .and_then(payload_suspend)
                .is_some_and(|suspend| suspend.durability == SnapshotDurability::Store);
        if !store {
            return;
        }
        let generation = claim.session.map(|session| session.generation).unwrap_or(1);
        self.pending_manifest_deletes.push((
            claim.job_id.clone(),
            claim.instance_id.clone(),
            generation,
        ));
    }

    /// The manifest deletes queued since the last call.
    pub fn take_manifest_deletes(&mut self) -> Vec<(String, String, u64)> {
        std::mem::take(&mut self.pending_manifest_deletes)
    }

    /// Suspended `store` sessions whose publish has not landed yet (the
    /// content store was unreachable, or no sealing key had arrived), and
    /// that were not self-fenced.
    pub fn unpublished_store_sessions(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .idle_sessions
            .values()
            .filter(|row| {
                row.state == SessionLogState::Suspended
                    && row.store
                    && !row.fenced
                    && row.published_url.is_empty()
            })
            .map(|row| row.session_id.clone())
            .collect();
        ids.sort();
        ids
    }

    /// Sessions this node suspended by self-fencing, waiting for the
    /// content store to say whether it still owns them.
    pub fn fenced_sessions(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .idle_sessions
            .values()
            .filter(|row| row.state == SessionLogState::Suspended && row.fenced)
            .map(|row| row.session_id.clone())
            .collect();
        ids.sort();
        ids
    }

    /// Clear a session's self-fence mark (it is an ordinary suspended
    /// `store` session again, whose publish the maintenance loop retries).
    pub(crate) fn clear_fenced(&mut self, session_id: &str) {
        if let Some(row) = self.idle_sessions.get_mut(session_id) {
            row.fenced = false;
            let row = row.clone();
            if let Some(log) = self.session_log() {
                log.put(row);
            }
        }
    }

    /// Mark a just-suspended session as self-fenced.
    pub(crate) fn mark_fenced(&mut self, session_id: &str) {
        if let Some(row) = self.idle_sessions.get_mut(session_id) {
            row.fenced = true;
            let row = row.clone();
            if let Some(log) = self.session_log() {
                log.put(row);
            }
        }
    }

    /// The running `store` sessions a node silent for `since_manager` (from
    /// every manager) and `since_store` (from its content store) must fence
    /// now: those whose task's `stateful.fenceAfter` has passed on both
    /// (mechanism 4).
    pub fn store_sessions_to_fence(
        &self,
        since_manager: Option<std::time::Duration>,
        since_store: Option<std::time::Duration>,
    ) -> Vec<String> {
        let mut ids: Vec<String> = self
            .claims
            .sessions()
            .into_iter()
            .filter(|claim| !claim.is_ratified())
            .filter(|claim| matches!(claim.stage, ClaimStage::Serving | ClaimStage::Booted))
            .filter(|claim| !self.session_captures.contains(&claim.instance_id))
            .filter(|claim| self.session_is_store(claim.local_key))
            .filter(|claim| {
                let after = self
                    .task_configs
                    .get(&claim.local_key)
                    .map(crate::structs::session::fence_after)
                    .unwrap_or(crate::structs::session::DEFAULT_FENCE_AFTER);
                crate::session_manifest::self_fence_due(since_manager, since_store, after)
            })
            .map(|claim| claim.instance_id)
            .collect();
        ids.sort();
        ids
    }

    /// Act on a fence marker (mechanism 2): stop this node's copy of the
    /// session when it is at or below the fenced generation. Returns
    /// whether a copy was ended.
    pub fn apply_fence_marker(&mut self, session_id: &str, fenced_generation: u64) -> bool {
        let generation = if let Some(row) = self.idle_sessions.get(session_id) {
            Some(row.generation)
        } else {
            self.claims
                .by_instance(session_id)
                .filter(|claim| claim.is_session() && !claim.is_ratified())
                .map(|claim| claim.session.map(|session| session.generation).unwrap_or(1))
        };
        match generation {
            Some(generation) if generation <= fenced_generation => {
                self.end_superseded(session_id, "fence_list")
            }
            _ => false,
        }
    }

    /// The manifest a session this node runs (or holds suspended) would
    /// write now.
    fn manifest_of_claim(
        &self,
        manifests: &SessionManifests,
        claim: &ClaimRecord,
        state: ManifestState,
        checkpoint: Option<SessionCheckpoint>,
    ) -> SessionManifest {
        manifests.manifest_for(
            &claim.instance_id,
            &claim.job_id,
            &claim.job_config_id,
            &claim.task_name,
            claim.session.map(|session| session.generation).unwrap_or(1),
            state,
            claim
                .session
                .map(|session| session.lease_issued_at_unix_ms)
                .unwrap_or_default(),
            &claim.resources.to_string(),
            checkpoint,
        )
    }
}

/// The resolved `suspend` block a payload carries.
fn payload_suspend(payload: &TaskAllocation) -> Option<crate::structs::suspend::ResolvedSuspend> {
    if payload.task_suspend.trim().is_empty() {
        return None;
    }
    serde_json::from_str::<crate::structs::suspend::SuspendConfig>(&payload.task_suspend)
        .ok()?
        .resolve()
        .ok()
}

/// The manifest a suspended row describes.
fn manifest_of_row(
    manifests: &SessionManifests,
    row: &PersistedSession,
    state: ManifestState,
    checkpoint: Option<SessionCheckpoint>,
) -> SessionManifest {
    manifests.manifest_for(
        &row.session_id,
        &row.job_id,
        &row.job_config_id,
        &row.task_name,
        row.generation,
        state,
        row.lease_issued_at_unix_ms,
        &row.resources.to_string(),
        checkpoint,
    )
}

/// Publish a checkpoint's memory parts sealed to the content store, and
/// return it with every part's `url` and `keyId` in its manifest. A disk
/// checkpoint (which of a session holds only its spec) has no parts and is
/// returned as is. Blocking.
pub fn publish_checkpoint_parts(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    job_id: &str,
    checkpoint: &SessionCheckpoint,
) -> R<SessionCheckpoint> {
    let mut snapshot =
        SnapshotManifest::from_json(&checkpoint.manifest_json).with_context(|| {
            format!(
                "checkpoint {} has an unreadable manifest",
                checkpoint.snapshot_id
            )
        })?;
    if snapshot.memory.is_empty() {
        return Ok(checkpoint.clone());
    }
    let (publishing, keys) = {
        let guard = manager.lock().expect("task manager");
        (guard.content_publishing(), guard.session_seal_keys(job_id))
    };
    let publishing = publishing.context(
        "this worker has no contentStore configured (or its node content store is disabled), \
         so it cannot publish session checkpoints",
    )?;
    for part in &mut snapshot.memory {
        let digest = crate::content_store::ContentDigest::parse(&part.digest)?;
        let key = keys.seal_key(&digest)?;
        let published = publishing.publish(&key).with_context(|| {
            format!(
                "publishing part {} of session checkpoint {}",
                part.role, checkpoint.snapshot_id
            )
        })?;
        part.url = Some(published.url);
        part.key_id = Some(published.key_id);
    }
    let mut published = checkpoint.clone();
    published.manifest_json = serde_json::to_string(&snapshot)?;
    Ok(published)
}

/// Publish a suspended `store` session's checkpoint and write its manifest
/// (`state = suspended`), then record it as published. Returns the
/// manifest URL. A manifest that names a newer generation ends the
/// session's row here (it was superseded) and answers
/// [`SessionSleepError::Superseded`]. Blocking.
pub fn publish_suspended(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    session_id: &str,
) -> Result<String, SessionSleepError> {
    let (row, manifests) = {
        let guard = manager.lock().expect("task manager");
        let row = guard
            .idle_row(session_id)
            .filter(|row| row.state == SessionLogState::Suspended && row.store)
            .ok_or(SessionSleepError::NotHere)?;
        (row, guard.require_manifests()?)
    };
    let Some(checkpoint) = row.checkpoint.clone() else {
        return Err(SessionSleepError::NotSuspendable(format!(
            "session {session_id} has no checkpoint recorded on this node"
        )));
    };
    let published = publish_checkpoint_parts(manager, &row.job_id, &checkpoint)
        .map_err(|err| SessionSleepError::StoreUnavailable(format!("{err:#}")))?;
    let manifest = manifest_of_row(
        &manifests,
        &row,
        ManifestState::Suspended,
        Some(published.clone()),
    );
    match manifests.write_own(&manifest) {
        Ok(_) => {
            let url = manifests.manifest_url(&row.job_id, session_id);
            manager
                .lock()
                .expect("task manager")
                .mark_published(session_id, published, &url);
            info!(session_id, url = %url, "Published suspended session");
            Ok(url)
        }
        Err(err @ ManifestError::Superseded { .. }) => {
            manager
                .lock()
                .expect("task manager")
                .end_superseded(session_id, "write");
            Err(sleep_error(err))
        }
        Err(err) => Err(sleep_error(err)),
    }
}

/// Publish a running `store` session's live checkpoint (periodic or at
/// worker stop) and write its manifest (`state = running`). That write is a
/// fence too (mechanism 1): a copy whose generation was superseded fails it
/// and is stopped here. Blocking.
pub fn publish_live(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    session_id: &str,
    checkpoint: &SessionCheckpoint,
) -> Result<(), SessionSleepError> {
    let (claim, manifests) = {
        let guard = manager.lock().expect("task manager");
        let claim = guard
            .claims
            .by_instance(session_id)
            .filter(|claim| claim.is_session() && !claim.is_ratified())
            .cloned()
            .ok_or(SessionSleepError::NotHere)?;
        (claim, guard.require_manifests()?)
    };
    let published = publish_checkpoint_parts(manager, &claim.job_id, checkpoint)
        .map_err(|err| SessionSleepError::StoreUnavailable(format!("{err:#}")))?;
    let manifest = {
        let guard = manager.lock().expect("task manager");
        guard.manifest_of_claim(
            &manifests,
            &claim,
            ManifestState::Running,
            Some(published.clone()),
        )
    };
    match manifests.write_own(&manifest) {
        Ok(_) => {
            let url = manifests.manifest_url(&claim.job_id, session_id);
            let mut guard = manager.lock().expect("task manager");
            let current = guard
                .session_checkpoints
                .get(session_id)
                .is_some_and(|live| live.snapshot_id == published.snapshot_id);
            if current {
                guard
                    .session_checkpoints
                    .insert(session_id.to_string(), published.clone());
                guard.unpin_published(&published);
                guard
                    .session_live_published
                    .insert(session_id.to_string(), url);
                guard.log_running_session(claim.local_key);
            }
            Ok(())
        }
        Err(err @ ManifestError::Superseded { .. }) => {
            manager
                .lock()
                .expect("task manager")
                .end_superseded(session_id, "write");
            Err(sleep_error(err))
        }
        Err(err) => Err(sleep_error(err)),
    }
}

/// An idempotent (derived-id) start of a `store` session creates its
/// manifest before its gate opens (plan §6.7 row 3): of two nodes that
/// derived the same id, one create wins and the other refuses with
/// [`SessionSleepError::AlreadyExists`] naming the winner. A no-op for a
/// minted id (it cannot collide), a task that is not `store`, or a node
/// without a content store. Blocking.
pub fn claim_new_store_session(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    payload: &TaskAllocation,
) -> R<()> {
    let Some(provisional) = payload.provisional.as_ref() else {
        return Ok(());
    };
    let Some(start) = provisional.session.as_ref() else {
        return Ok(());
    };
    let session_id = provisional.instance_id.as_str();
    if crate::structs::session_node_tag(session_id).is_some() {
        return Ok(());
    }
    if !payload_suspend(payload)
        .is_some_and(|suspend| suspend.durability == SnapshotDurability::Store)
    {
        return Ok(());
    }
    let manifests = {
        let guard = manager.lock().expect("task manager");
        let Some(manifests) = guard.session_manifests() else {
            return Ok(());
        };
        // Held here already: the start deduplicates against it.
        if guard.session_status(session_id).is_some() || guard.idle_row(session_id).is_some() {
            return Ok(());
        }
        manifests
    };
    let manifest = manifests.manifest_for(
        session_id,
        &payload.job_id,
        &payload.job_config_id,
        &payload.task_name,
        1,
        ManifestState::Running,
        start.lease_issued_at_unix_ms,
        &payload.task_resources,
        None,
    );
    manifests
        .create(&manifest, true)
        .map(|_| ())
        .map_err(|err| sleep_error(err).into())
}

/// Before a suspended `store` session resumes on the node holding it, claim
/// the next generation (plan §6.7): the bucket must still name this node at
/// the row's generation. Returns the claimed generation, or `None` for a
/// session that is not a suspended `store` row here. A row the bucket has
/// moved past is ended (superseded). Blocking.
pub fn claim_held_for_resume(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    session_id: &str,
) -> Result<Option<u64>, SessionSleepError> {
    let (row, manifests) = {
        let guard = manager.lock().expect("task manager");
        let Some(row) = guard
            .idle_row(session_id)
            .filter(|row| row.state == SessionLogState::Suspended && row.store)
        else {
            return Ok(None);
        };
        (row, guard.require_manifests()?)
    };
    // An unpublished checkpoint is not written into the manifest (its parts
    // have no URLs); the claim keeps whatever checkpoint the manifest names.
    let checkpoint = (!row.published_url.is_empty())
        .then(|| row.checkpoint.clone())
        .flatten();
    let held = manifest_of_row(&manifests, &row, ManifestState::Running, checkpoint);
    match manifests.claim_held(&held) {
        Ok(claimed) => Ok(Some(claimed.generation)),
        Err(err @ ManifestError::Superseded { .. }) => {
            manager
                .lock()
                .expect("task manager")
                .end_superseded(session_id, "resume");
            Err(sleep_error(err))
        }
        Err(err) => Err(sleep_error(err)),
    }
}

/// Hand a session back after a resume that claimed a generation but did
/// not start: the manifest says `suspended` at the claimed generation, so a
/// retry (here or anywhere) claims the next one without probing. Best
/// effort.
pub fn hand_back(manifests: &SessionManifests, manifest: &SessionManifest) {
    let mut back = manifest.clone();
    back.state = ManifestState::Suspended;
    back.updated_at_unix_ms = crate::util::now_unix_ms();
    if let Err(err) = manifests.write_own(&back) {
        warn!(session_id = %manifest.session_id, error = %err, "Could not hand a session back after a failed resume");
    }
}

/// Resume a `store` session on this node from its manifest (plan §6.6,
/// "Resume", and §6.7 row 5): the session ran or slept on another node (or
/// this node no longer holds it). Admits it into the envelope first (a full
/// envelope refuses with nothing claimed), claims generation g+1 on the
/// ETag the manifest was read at, leaves a fence marker for the old node,
/// and launches it restoring from the published checkpoint.
///
/// A manifest naming another node as *running* the session refuses with
/// [`SessionSleepError::RunningElsewhere`] unless `force_move` is set: the
/// caller probes that node first (plan §6.7, "Moving is cautious for
/// running sessions"). `after_claim` runs once the claim is won and before
/// the launch; an edge resume uses it to stop the old copy it probed.
/// Blocking; the caller holds a launch slot.
pub fn resume_moved(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    request: &MoveRequest,
    after_claim: Option<AfterClaim>,
) -> R<MoveOutcome> {
    let session_id = request.session_id.as_str();
    let (spec, manifests, keys, node_id) = {
        let guard = manager.lock().expect("task manager");
        if let Some(status) = guard.session_status(session_id) {
            if status.state != "suspended" {
                return Ok(MoveOutcome {
                    from: "",
                    generation: status.generation,
                    moved_from: String::new(),
                });
            }
        }
        let manifests = guard.require_manifests()?;
        let node_id = manifests.node_id().to_string();
        (
            guard.session_spec(&request.job_id, &request.task_name),
            manifests,
            guard.session_seal_keys(&request.job_id),
            node_id,
        )
    };
    // A session this node holds suspended resumes from its own row.
    if manager
        .lock()
        .expect("task manager")
        .idle_row(session_id)
        .is_some_and(|row| row.state == SessionLogState::Suspended)
    {
        let outcome = crate::worker::task_manager::session_sleep::resume_session(
            manager,
            session_id,
            request.cold,
        )?;
        let generation = manager
            .lock()
            .expect("task manager")
            .session_status(session_id)
            .map(|status| status.generation)
            .unwrap_or(1);
        return Ok(MoveOutcome {
            from: match outcome {
                crate::worker::task_manager::session_sleep::ResumeOutcome::Resumed { from } => from,
                crate::worker::task_manager::session_sleep::ResumeOutcome::AlreadyRunning => "",
            },
            generation,
            moved_from: String::new(),
        });
    }
    let Some(spec) = spec else {
        return Err(SessionRefusal::NoEnvelope {
            job_id: request.job_id.clone(),
            task_name: request.task_name.clone(),
        }
        .into());
    };
    if !crate::structs::session::publishes_checkpoints(&spec.task) {
        // A `local` session lives only on the node that started it.
        return Err(SessionSleepError::NotHere.into());
    }
    let (current, etag) = manifests
        .read(&request.job_id, session_id)
        .map_err(sleep_error)?
        .ok_or(SessionSleepError::NotHere)?;
    if current.state == ManifestState::Running && current.node != node_id && !request.force_move {
        return Err(SessionSleepError::RunningElsewhere {
            node: current.node.clone(),
            address: current.node_address.clone(),
            generation: current.generation,
        }
        .into());
    }

    // A local copy this node evicted is stale now: the manifest is the
    // session.
    {
        let mut guard = manager.lock().expect("task manager");
        if guard
            .idle_row(session_id)
            .is_some_and(|row| row.state == SessionLogState::Evicted)
        {
            guard.idle_sessions.remove(session_id);
            guard.unlog_session(session_id);
        }
    }
    // Admit first: a full envelope refuses with nothing claimed.
    let (mut payload, provisional) = crate::worker::edge::session_payload(
        &spec,
        &request.job_id,
        session_id,
        &node_id,
        &request.origin,
        &request.ticket_jti,
        current.lease_issued_at_unix_ms,
    )?;
    let restore = current
        .checkpoint
        .as_ref()
        .filter(|checkpoint| !request.cold && checkpoint.kind == SnapshotKind::Memory.as_str());
    if let Some(checkpoint) = restore {
        let snapshot = SnapshotManifest::from_json(&checkpoint.manifest_json)?;
        let (part_keys, part_key_error) = keys.part_keys_for(&snapshot);
        payload.restore = Some(crate::structs::SnapshotRestore {
            snapshot_id: checkpoint.snapshot_id.clone(),
            kind: checkpoint.kind.clone(),
            manifest_json: checkpoint.manifest_json.clone(),
            on_resume: spec
                .task
                .suspend
                .as_ref()
                .and_then(|suspend| suspend.resolve().ok())
                .map(|suspend| suspend.on_resume)
                .unwrap_or_default(),
            part_keys,
            part_key_error,
            ..Default::default()
        });
    }
    let resources: Value =
        serde_json::from_str(&payload.task_resources).unwrap_or_else(|_| serde_json::json!({}));
    let mut record = crate::worker::edge::record_from_payload(&payload, &provisional, resources);
    record.session = Some(crate::worker::claims::SessionClaim {
        lease_issued_at_unix_ms: current.lease_issued_at_unix_ms,
        generation: current.generation + 1,
    });
    let admitted = manager
        .lock()
        .expect("task manager")
        .admit_session(record)?;

    // Claim g+1 on the ETag read above: anything that changed since (another
    // resume, the old copy's own write) makes this lose.
    let claimed = match manifests.claim_from(&current, &etag) {
        Ok(claimed) => claimed,
        Err(err) => {
            manager
                .lock()
                .expect("task manager")
                .claims
                .remove(&admitted.claim_id);
            return Err(sleep_error(err).into());
        }
    };
    if current.node != node_id {
        if let Err(err) = manifests.fence(&current.node, session_id, current.generation) {
            // The other three mechanisms still converge it.
            warn!(session_id, old_node = %current.node, error = %err, "Could not leave a fence marker for the session's old node");
        }
    }
    if let Some(hook) = after_claim {
        hook(&current);
    }
    payload.allocation_id = admitted.local_key;
    // A launch that fails ends the claim; the manifest is handed back below,
    // not deleted.
    manager
        .lock()
        .expect("task manager")
        .keep_manifest
        .insert(session_id.to_string());
    let launched =
        launch_provisional_allocation(manager, &payload, &admitted.claim_id, admitted.gate);
    manager
        .lock()
        .expect("task manager")
        .keep_manifest
        .remove(session_id);
    let running = launched.is_ok()
        && manager
            .lock()
            .expect("task manager")
            .claims
            .by_instance(session_id)
            .is_some();
    manager
        .lock()
        .expect("task manager")
        .checkpoints
        .forget_restore(admitted.local_key);
    if !running {
        hand_back(&manifests, &claimed);
        return Err(match launched {
            Err(err) => err.context(format!(
                "session {session_id} claimed generation {} here but did not start; its \
                 manifest says suspended, so a retry resumes it",
                claimed.generation
            )),
            Ok(()) => anyhow!("session {session_id} was stopped while it resumed"),
        });
    }
    if current.node != node_id {
        crate::manager::telemetry::worker_metrics()
            .session_moves_total
            .inc();
    }
    info!(
        session_id,
        generation = claimed.generation,
        moved_from = %current.node,
        "Resumed a store session from its manifest"
    );
    Ok(MoveOutcome {
        from: if restore.is_some() { "store" } else { "cold" },
        generation: claimed.generation,
        moved_from: if current.node == node_id {
            String::new()
        } else {
            current.node.clone()
        },
    })
}

/// One pass of the fence list (mechanism 2): list this node's fence
/// prefix, stop every listed session at or below the fenced generation,
/// and delete the markers. Returns how many copies it ended. Blocking.
pub fn process_fence_list(manager: &Arc<Mutex<WorkerTaskManager>>) -> Result<usize, ManifestError> {
    let Some(manifests) = manager.lock().expect("task manager").session_manifests() else {
        return Ok(0);
    };
    let markers = manifests.fences()?;
    let mut ended = 0;
    for marker in markers {
        if manager
            .lock()
            .expect("task manager")
            .apply_fence_marker(&marker.session_id, marker.generation)
        {
            ended += 1;
        }
        if let Err(err) = manifests.clear_fence(&marker.session_id) {
            debug!(session_id = %marker.session_id, error = %err, "Could not delete a fence marker; it is acted on again next round");
        }
    }
    Ok(ended)
}

/// Retry the publish of suspended `store` sessions whose publish did not
/// land (plan §6.7 row 12). Blocking.
pub fn retry_unpublished(manager: &Arc<Mutex<WorkerTaskManager>>) -> usize {
    let ids = manager
        .lock()
        .expect("task manager")
        .unpublished_store_sessions();
    let mut published = 0;
    for session_id in ids {
        match publish_suspended(manager, &session_id) {
            Ok(_) => published += 1,
            Err(err) => {
                debug!(session_id = %session_id, error = %err, "Session publish still pending")
            }
        }
    }
    published
}

/// Delete the manifests of `store` sessions that ended for good here.
/// Blocking.
pub fn run_manifest_deletes(manager: &Arc<Mutex<WorkerTaskManager>>) {
    let (deletes, manifests) = {
        let mut guard = manager.lock().expect("task manager");
        (guard.take_manifest_deletes(), guard.session_manifests())
    };
    let Some(manifests) = manifests else {
        return;
    };
    let mut retry = Vec::new();
    for (job_id, session_id, generation) in deletes {
        match manifests.delete_own(&job_id, &session_id, generation) {
            Ok(_) => {}
            Err(ManifestError::Unavailable(why)) => {
                debug!(session_id = %session_id, error = %why, "Session manifest delete deferred");
                retry.push((job_id, session_id, generation));
            }
            Err(err) => {
                debug!(session_id = %session_id, error = %err, "Session manifest not deleted")
            }
        }
    }
    if !retry.is_empty() {
        manager
            .lock()
            .expect("task manager")
            .pending_manifest_deletes
            .extend(retry);
    }
}

/// Self-fencing (mechanism 4): suspend, on this node only, every running
/// `store` session whose `stateful.fenceAfter` has passed with neither a
/// manager nor the content store reached. A session whose capture fails is
/// stopped instead, so nothing stale keeps serving. Blocking.
pub fn self_fence(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    since_manager: Option<std::time::Duration>,
    since_store: Option<std::time::Duration>,
) -> Vec<String> {
    let ids = {
        let guard = manager.lock().expect("task manager");
        if guard.session_manifests.is_none() {
            return Vec::new();
        }
        guard.store_sessions_to_fence(since_manager, since_store)
    };
    let mut fenced = Vec::new();
    for session_id in ids {
        let outcome = crate::worker::task_manager::session_sleep::suspend_session_with(
            manager,
            &session_id,
            "self-fence",
            crate::worker::task_manager::session_sleep::SuspendMode::Fence,
        );
        match outcome {
            Ok(_) => {}
            Err(err) => {
                warn!(session_id = %session_id, error = %format!("{err:#}"), "Self-fence capture failed; stopping the session instead");
                manager.lock().expect("task manager").stop_sessions(
                    std::slice::from_ref(&session_id),
                    "self-fence (capture failed)",
                );
            }
        }
        crate::manager::telemetry::worker_metrics()
            .session_self_fenced_total
            .inc();
        fenced.push(session_id);
    }
    if !fenced.is_empty() {
        warn!(
            sessions = fenced.len(),
            "Self-fenced store sessions: this node reached neither a manager nor its content \
             store for stateful.fenceAfter"
        );
    }
    fenced
}

/// After a self-fence, once the content store answers again: resume the
/// fenced sessions the bucket still names this node for, and end the ones
/// that moved meanwhile. Each resume claims the next generation as any
/// resume does. The caller holds no launch slot; one is taken per resume
/// by `acquire`. Blocking.
pub fn unfence(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    mut acquire: impl FnMut() -> Option<crate::worker::launch_pool::LaunchSlot>,
) -> (usize, usize) {
    let (ids, manifests) = {
        let guard = manager.lock().expect("task manager");
        (guard.fenced_sessions(), guard.session_manifests())
    };
    let Some(manifests) = manifests else {
        return (0, 0);
    };
    let (mut resumed, mut ended) = (0, 0);
    for session_id in ids {
        let Some(row) = manager.lock().expect("task manager").idle_row(&session_id) else {
            continue;
        };
        let owned = match manifests.read(&row.job_id, &session_id) {
            Ok(None) => true,
            Ok(Some((manifest, _))) => manifest.owned_by(manifests.node_id(), row.generation),
            Err(err) => {
                debug!(session_id = %session_id, error = %err, "Content store still unreachable; the session stays fenced");
                return (resumed, ended);
            }
        };
        if !owned {
            if manager
                .lock()
                .expect("task manager")
                .end_superseded(&session_id, "reconnect")
            {
                ended += 1;
            }
            continue;
        }
        manager
            .lock()
            .expect("task manager")
            .clear_fenced(&session_id);
        let Some(_slot) = acquire() else {
            // No slot now: it stays suspended (and is published by the
            // retry loop); a later `instance start` resumes it.
            continue;
        };
        match crate::worker::task_manager::session_sleep::resume_session(
            manager,
            &session_id,
            false,
        ) {
            Ok(_) => {
                resumed += 1;
                info!(session_id = %session_id, "Resumed a self-fenced session the content store still names this node for");
            }
            Err(err) => {
                warn!(session_id = %session_id, error = %format!("{err:#}"), "Could not resume a self-fenced session; it stays suspended");
            }
        }
    }
    (resumed, ended)
}

#[cfg(test)]
mod session_store_test {
    use super::*;

    #[test]
    fn only_stale_endings_keep_the_manifest_test() {
        assert!(reason_keeps_manifest("superseded (fence_list)"));
        assert!(reason_keeps_manifest(
            "duplicate of a session on another node"
        ));
        assert!(reason_keeps_manifest("requested (duplicate)"));
        assert!(reason_keeps_manifest("self-fence (capture failed)"));
        assert!(!reason_keeps_manifest("requested (stop)"));
        assert!(!reason_keeps_manifest("requested (edge stop)"));
    }
}
