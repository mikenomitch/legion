use crate::prelude::*;
use crate::structs::{
    PersistenceConfigFull, PersistenceType, RaftPersistenceConfigFull, StreamingLeaseProvider,
    StreamingObjectStorageLeaseConfigFull, StreamingPersistenceConfigFull,
};

use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use openraft::Config as OpenRaftConfig;
use rustyriver::replica_url::parse_replica_url_with_query;
use rustyriver::{
    Db, FileReplicaClient, Lease, Leaser, ObjectStoreClient, ObjectStoreConfig, Replica,
    ReplicaClient, S3Leaser,
};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration as StdDuration;
use time::{Duration as TimeDuration, OffsetDateTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceRole {
    Primary,
    Standby,
    Candidate,
}

impl PersistenceRole {
    pub fn as_str(self) -> &'static str {
        match self {
            PersistenceRole::Primary => "primary",
            PersistenceRole::Standby => "standby",
            PersistenceRole::Candidate => "candidate",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistenceStatus {
    pub backend: String,
    pub role: PersistenceRole,
    pub lease_held: bool,
}

impl PersistenceStatus {
    pub fn local_disk_primary() -> Self {
        Self {
            backend: "local_disk".to_string(),
            role: PersistenceRole::Primary,
            lease_held: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftStatus {
    pub enabled: bool,
    pub backend: String,
    pub node_id: String,
    pub cluster_id: String,
    pub role: PersistenceRole,
    pub leader_id: Option<String>,
    pub term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub lease_held: bool,
    pub voters: Vec<String>,
    pub learners: Vec<String>,
    pub peers: Vec<String>,
    pub election_epoch_ms: i64,
    pub join_token_ttl_seconds: u64,
    pub raft_library: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftHistoryEvent {
    pub event_id: u64,
    pub event_type: String,
    pub message: String,
    pub actor: String,
    pub target: Option<String>,
    pub term: u64,
    pub leader_id: Option<String>,
    pub created_at: String,
}

pub trait PersistenceDriver: Send + Sync + Debug {
    fn start(&self) -> R<()>;

    fn status(&self) -> PersistenceStatus;

    fn raft_status(&self) -> Option<RaftStatus> {
        None
    }

    fn raft_history(&self, _limit: usize) -> Vec<RaftHistoryEvent> {
        Vec::new()
    }

    fn issue_raft_command(
        &self,
        _command: &str,
        _target: Option<&str>,
        _actor: &str,
    ) -> R<RaftHistoryEvent> {
        bail!("raft backend is not enabled")
    }

    fn issue_raft_join_token(&self, _actor: &str) -> R<(String, String)> {
        bail!("raft backend is not enabled")
    }
}

#[derive(Debug, Default)]
pub struct LocalDiskPersistenceDriver;

impl LocalDiskPersistenceDriver {
    pub fn new() -> Self {
        Self
    }
}

impl PersistenceDriver for LocalDiskPersistenceDriver {
    fn start(&self) -> R<()> {
        Ok(())
    }

    fn status(&self) -> PersistenceStatus {
        PersistenceStatus::local_disk_primary()
    }
}

/// Streaming persistence driver backed by [`rustyriver`].
///
/// Replaces the former out-of-process `litestream` sidecar with rustyriver's
/// in-process embedding API: a managed [`Db`] takes over SQLite checkpointing
/// and ships each WAL segment to object storage through a [`Replica`], while an
/// optional [`S3Leaser`] provides single-primary fencing.
#[derive(Debug)]
pub struct StreamingPersistenceDriver {
    db_path: std::path::PathBuf,
    cfg: StreamingPersistenceConfigFull,
    role_primary: Arc<AtomicBool>,
    lease_held: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
}

impl StreamingPersistenceDriver {
    pub fn new(db_path: std::path::PathBuf, cfg: StreamingPersistenceConfigFull) -> Self {
        Self {
            db_path,
            cfg,
            role_primary: Arc::new(AtomicBool::new(false)),
            lease_held: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    fn host_identity() -> String {
        std::env::var("HOSTNAME").unwrap_or_else(|_| "legion-manager".to_string())
    }

    /// Open the managed database and drive the rustyriver WAL→object-storage
    /// sync loop on a dedicated thread (the in-process replacement for the old
    /// `litestream replicate` subprocess).
    fn start_replication(&self) -> R<()> {
        if self.cfg.replica_url.trim().is_empty() {
            info!(
                "Streaming persistence enabled without replica URL; skipping rustyriver replication"
            );
            return Ok(());
        }

        // Open synchronously so the long-running read lock that takes over
        // checkpointing is held before `start()` returns. The `Db` is `!Sync`,
        // so it is moved onto — and stays on — the worker thread from here.
        let db = Db::open(&self.db_path).with_context(|| {
            format!(
                "rustyriver failed to open managed database at {}",
                self.db_path.display()
            )
        })?;

        let replica_url = self.cfg.replica_url.clone();
        let interval = parse_sync_interval(&self.cfg.sync_interval);
        let shutdown = Arc::clone(&self.shutdown);

        std::thread::Builder::new()
            .name("persistence-replica-sync".to_string())
            .spawn(move || {
                let rt = match build_loop_runtime() {
                    Ok(rt) => rt,
                    Err(err) => {
                        error!(error = ?err, "failed to build runtime for rustyriver replication");
                        return;
                    }
                };
                rt.block_on(async move {
                    if let Err(err) = run_replica_loop(db, replica_url, interval, shutdown).await {
                        error!(error = ?err, "rustyriver replication loop exited");
                    }
                });
            })
            .context("failed to spawn rustyriver replication thread")?;

        info!(
            interval_secs = interval.as_secs_f64(),
            "Started rustyriver streaming replication"
        );
        Ok(())
    }

    fn start_object_storage_lease_loop(&self) -> R<()> {
        let lease_cfg = self.cfg.lease.clone();
        let object_cfg = lease_cfg.object_storage.clone();
        let role_primary = Arc::clone(&self.role_primary);
        let lease_held = Arc::clone(&self.lease_held);
        let shutdown = Arc::clone(&self.shutdown);
        let owner = Self::host_identity();

        std::thread::Builder::new()
            .name("persistence-object-lease".to_string())
            .spawn(move || {
                let rt = match build_loop_runtime() {
                    Ok(rt) => rt,
                    Err(err) => {
                        error!(error = ?err, "failed to build runtime for object-storage lease loop");
                        return;
                    }
                };
                rt.block_on(async move {
                    if let Err(err) = object_storage_lease_loop(
                        object_cfg,
                        lease_cfg.key,
                        owner,
                        lease_cfg.ttl_seconds,
                        lease_cfg.renew_interval_seconds,
                        role_primary,
                        lease_held,
                        shutdown,
                    )
                    .await
                    {
                        error!(error = ?err, "Object-storage lease loop exited");
                    }
                });
            })
            .context("failed to spawn object-storage lease loop thread")?;

        Ok(())
    }
}

impl Drop for StreamingPersistenceDriver {
    fn drop(&mut self) {
        // Signal the replication + lease loops to wind down. Best-effort: the
        // detached worker threads observe this flag on their next cycle and
        // release the WAL read lock / object-storage lease as they exit.
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl PersistenceDriver for StreamingPersistenceDriver {
    fn start(&self) -> R<()> {
        self.start_replication()?;

        match self.cfg.lease.provider {
            StreamingLeaseProvider::None => {
                self.role_primary.store(true, Ordering::Relaxed);
                self.lease_held.store(true, Ordering::Relaxed);
            }
            StreamingLeaseProvider::ObjectStorage => {
                self.start_object_storage_lease_loop()?;
            }
        }

        Ok(())
    }

    fn status(&self) -> PersistenceStatus {
        let is_primary = self.role_primary.load(Ordering::Relaxed);
        let lease_held = self.lease_held.load(Ordering::Relaxed);
        PersistenceStatus {
            backend: "streaming".to_string(),
            role: if is_primary {
                PersistenceRole::Primary
            } else {
                PersistenceRole::Standby
            },
            lease_held,
        }
    }
}

// ── rustyriver replication helpers ────────────────────────────────────────────

/// Build the single-threaded Tokio runtime that drives a persistence worker
/// loop. Each loop owns its runtime on its own thread, so the driver does not
/// require an ambient runtime in the caller (rustyriver's `Db` is `!Sync` and
/// stays pinned to this thread).
fn build_loop_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

/// Parse the configured sync interval (e.g. `"1s"`, `"500ms"`), defaulting to 1s
/// when the value is empty or unparseable.
fn parse_sync_interval(raw: &str) -> StdDuration {
    humantime::parse_duration(raw.trim()).unwrap_or_else(|_| StdDuration::from_secs(1))
}

/// Whether the replica URL targets S3/R2/MinIO (handled by [`ObjectStoreClient`]).
/// Everything else — a `file://` URL or a bare filesystem path — uses the local
/// [`FileReplicaClient`].
fn is_object_store_url(url: &str) -> bool {
    url.trim().to_ascii_lowercase().starts_with("s3://")
}

/// Strip a `file://` scheme prefix to recover a local replica directory path.
fn file_replica_path(url: &str) -> String {
    let u = url.trim();
    u.strip_prefix("file://").unwrap_or(u).to_string()
}

/// Select the rustyriver replica client for the URL and drive the sync loop.
async fn run_replica_loop(
    db: Db,
    replica_url: String,
    interval: StdDuration,
    shutdown: Arc<AtomicBool>,
) -> R<()> {
    if is_object_store_url(&replica_url) {
        let parsed = parse_replica_url_with_query(&replica_url)
            .with_context(|| format!("invalid streaming replica URL: {replica_url}"))?;
        let config = ObjectStoreConfig::from_url(&parsed)
            .with_context(|| format!("invalid streaming replica URL: {replica_url}"))?;
        replica_sync_loop(db, ObjectStoreClient::new(config), interval, shutdown).await
    } else {
        let client = FileReplicaClient::new(file_replica_path(&replica_url));
        replica_sync_loop(db, client, interval, shutdown).await
    }
}

/// Generic sync loop: capture the WAL into a local LTX file, upload new LTX
/// files to the replica, sleep, repeat — until `shutdown` is signalled.
async fn replica_sync_loop<C: ReplicaClient>(
    db: Db,
    client: C,
    interval: StdDuration,
    shutdown: Arc<AtomicBool>,
) -> R<()> {
    let mut replica = Replica::new(db, client);
    while !shutdown.load(Ordering::Relaxed) {
        if let Err(err) = sync_once(&mut replica).await {
            warn!(error = ?err, "rustyriver replica sync cycle failed");
        }
        tokio::time::sleep(interval).await;
    }
    // Release the long-running read lock so SQLite can checkpoint freely again.
    if let Some(db) = replica.into_db() {
        if let Err(err) = db.close() {
            warn!(error = ?err, "rustyriver database close failed during shutdown");
        }
    }
    Ok(())
}

/// One capture (`Db::sync`) + upload (`Replica::sync`) cycle. Extracted so tests
/// can drive a deterministic sync without the surrounding loop.
async fn sync_once<C: ReplicaClient>(replica: &mut Replica<C>) -> R<()> {
    if let Some(db) = replica.db_mut() {
        db.sync().context("rustyriver Db::sync failed")?;
    }
    replica
        .sync()
        .await
        .context("rustyriver Replica::sync failed")?;
    Ok(())
}

// ── Object-storage lease fencing (rustyriver S3Leaser) ────────────────────────

/// Build the S3-compatible object store that backs the lease lock object.
fn build_lease_store(
    object_cfg: &StreamingObjectStorageLeaseConfigFull,
) -> R<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(object_cfg.bucket.clone())
        .with_region(object_cfg.region.clone());

    if let Some(endpoint) = &object_cfg.endpoint {
        builder = builder.with_endpoint(endpoint.clone());
    }
    if let Some(access_key) = &object_cfg.access_key_id {
        builder = builder.with_access_key_id(access_key.clone());
    }
    if let Some(secret) = &object_cfg.secret_access_key {
        builder = builder.with_secret_access_key(secret.clone());
    }

    let store = builder
        .build()
        .context("failed to build S3 store for persistence lease")?;
    Ok(Arc::new(store))
}

/// rustyriver's `S3Leaser` owns the `lock.json` lock filename and writes it under
/// a configurable prefix, so we map the configured lease `key` to its directory.
/// e.g. `"leases/default/primary.json"` → prefix `"leases/default"` → lock object
/// `"leases/default/lock.json"`. See docs/rustyriver-migration.md.
fn lease_path_prefix(key: &str) -> String {
    match key.trim().rsplit_once('/') {
        Some((dir, _file)) => dir.to_string(),
        None => String::new(),
    }
}

/// The outcome of a single lease acquire/renew attempt, separated from the
/// loop's side effects so the decision logic is unit-testable.
#[derive(Debug)]
enum LeaseTransition {
    /// Hold the lease — became, or stayed, primary.
    Held(Lease),
    /// Another node holds a live lease; stand by.
    Blocked,
    /// A lease we held was taken over by another node; stand by, re-acquire later.
    Lost,
    /// Transient error renewing a lease we still hold; keep it and our role.
    Retain(Lease),
}

/// Perform one acquire-or-renew step against the leaser and classify the result.
async fn lease_step<L: Leaser>(leaser: &L, held: Option<Lease>) -> LeaseTransition {
    match held {
        None => match leaser.acquire_lease().await {
            Ok(lease) => LeaseTransition::Held(lease),
            Err(err) => {
                if err.as_lease_exists().is_some() {
                    debug!(error = ?err, "another node holds the persistence lease");
                } else {
                    warn!(error = ?err, "failed to acquire persistence lease");
                }
                LeaseTransition::Blocked
            }
        },
        Some(current) => match leaser.renew_lease(&current).await {
            Ok(lease) => LeaseTransition::Held(lease),
            Err(err) if err.is_lease_not_held() => {
                debug!(error = ?err, "persistence lease taken over by another node");
                LeaseTransition::Lost
            }
            Err(err) => {
                warn!(error = ?err, "failed to renew persistence lease; will retry");
                LeaseTransition::Retain(current)
            }
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn object_storage_lease_loop(
    object_cfg: StreamingObjectStorageLeaseConfigFull,
    key: String,
    owner: String,
    ttl_seconds: u64,
    renew_interval_seconds: u64,
    role_primary: Arc<AtomicBool>,
    lease_held: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
) -> R<()> {
    if object_cfg.bucket.trim().is_empty() {
        bail!("streaming.objectStorage lease requires bucket");
    }

    let store = build_lease_store(&object_cfg)?;
    let ttl = StdDuration::from_secs(ttl_seconds.max(2));
    let renew = StdDuration::from_secs(renew_interval_seconds.max(1));
    let leaser = S3Leaser::new(store)
        .with_path(lease_path_prefix(&key))
        .with_ttl(ttl)
        .with_owner(owner);

    let mut held: Option<Lease> = None;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            if let Some(lease) = held.take() {
                let _ = leaser.release_lease(&lease).await;
            }
            role_primary.store(false, Ordering::Relaxed);
            lease_held.store(false, Ordering::Relaxed);
            break;
        }

        match lease_step(&leaser, held.take()).await {
            LeaseTransition::Held(lease) => {
                held = Some(lease);
                role_primary.store(true, Ordering::Relaxed);
                lease_held.store(true, Ordering::Relaxed);
            }
            LeaseTransition::Retain(lease) => {
                // Keep the lease and our current role; retry on the next cycle.
                held = Some(lease);
            }
            LeaseTransition::Blocked | LeaseTransition::Lost => {
                role_primary.store(false, Ordering::Relaxed);
                lease_held.store(false, Ordering::Relaxed);
            }
        }

        tokio::time::sleep(renew).await;
    }

    Ok(())
}

#[derive(Debug)]
pub struct RaftPersistenceDriver {
    state: Arc<Mutex<RaftState>>,
    _raft_config: OpenRaftConfig,
}

#[derive(Debug)]
struct RaftState {
    node_id: String,
    cluster_id: String,
    role: PersistenceRole,
    leader_id: Option<String>,
    term: u64,
    commit_index: u64,
    applied_index: u64,
    voters: Vec<String>,
    learners: Vec<String>,
    peers: Vec<String>,
    election_epoch_ms: i64,
    join_token_ttl_seconds: u64,
    history: VecDeque<RaftHistoryEvent>,
    next_event_id: u64,
}

impl RaftPersistenceDriver {
    pub fn new(node_id: String, raft: &RaftPersistenceConfigFull) -> Self {
        let mut voters = raft.peers.clone();
        if !voters.iter().any(|peer| peer == &node_id) {
            voters.push(node_id.clone());
        }
        voters.sort();
        voters.dedup();
        let now = OffsetDateTime::now_utc();

        Self {
            state: Arc::new(Mutex::new(RaftState {
                node_id: node_id.clone(),
                cluster_id: format!("legion-raft-{node_id}"),
                role: if raft.bootstrap {
                    PersistenceRole::Primary
                } else {
                    PersistenceRole::Standby
                },
                leader_id: if raft.bootstrap {
                    Some(node_id.clone())
                } else {
                    None
                },
                term: 1,
                commit_index: 0,
                applied_index: 0,
                peers: voters.clone(),
                voters,
                learners: Vec::new(),
                election_epoch_ms: (now.unix_timestamp_nanos() / 1_000_000) as i64,
                join_token_ttl_seconds: raft.join_token_ttl_seconds,
                history: VecDeque::with_capacity(512),
                next_event_id: 1,
            })),
            _raft_config: OpenRaftConfig::default(),
        }
    }

    fn push_event(
        state: &mut RaftState,
        event_type: &str,
        message: String,
        actor: &str,
        target: Option<String>,
    ) -> RaftHistoryEvent {
        let event = RaftHistoryEvent {
            event_id: state.next_event_id,
            event_type: event_type.to_string(),
            message,
            actor: actor.to_string(),
            target,
            term: state.term,
            leader_id: state.leader_id.clone(),
            created_at: OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| "unknown".to_string()),
        };
        state.next_event_id += 1;
        if state.history.len() == 512 {
            state.history.pop_front();
        }
        state.history.push_back(event.clone());
        event
    }

    fn choose_new_leader(state: &RaftState) -> Option<String> {
        state
            .voters
            .iter()
            .find(|member| member.as_str() != state.node_id)
            .cloned()
            .or_else(|| Some(state.node_id.clone()))
    }
}

impl PersistenceDriver for RaftPersistenceDriver {
    fn start(&self) -> R<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("Error locking raft state"))?;
        let mode = match state.role {
            PersistenceRole::Primary => "primary bootstrap",
            PersistenceRole::Standby => "standby join",
            PersistenceRole::Candidate => "candidate",
        };
        let node_id = state.node_id.clone();
        Self::push_event(
            &mut state,
            "startup",
            format!("Raft node {node_id} started in {mode} mode"),
            "system",
            None,
        );
        info!(node_id = %state.node_id, role = state.role.as_str(), "Started raft persistence driver");
        Ok(())
    }

    fn status(&self) -> PersistenceStatus {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("Error locking raft state"))
            .expect("raft state lock poisoned");
        PersistenceStatus {
            backend: "raft".to_string(),
            role: state.role,
            lease_held: state.role == PersistenceRole::Primary,
        }
    }

    fn raft_status(&self) -> Option<RaftStatus> {
        let state = self.state.lock().ok()?;
        Some(RaftStatus {
            enabled: true,
            backend: "raft".to_string(),
            node_id: state.node_id.clone(),
            cluster_id: state.cluster_id.clone(),
            role: state.role,
            leader_id: state.leader_id.clone(),
            term: state.term,
            commit_index: state.commit_index,
            applied_index: state.applied_index,
            lease_held: state.role == PersistenceRole::Primary,
            voters: state.voters.clone(),
            learners: state.learners.clone(),
            peers: state.peers.clone(),
            election_epoch_ms: state.election_epoch_ms,
            join_token_ttl_seconds: state.join_token_ttl_seconds,
            raft_library: "openraft".to_string(),
        })
    }

    fn raft_history(&self, limit: usize) -> Vec<RaftHistoryEvent> {
        let Ok(state) = self.state.lock() else {
            return Vec::new();
        };
        state
            .history
            .iter()
            .rev()
            .take(limit.max(1).min(500))
            .cloned()
            .collect()
    }

    fn issue_raft_command(
        &self,
        command: &str,
        target: Option<&str>,
        actor: &str,
    ) -> R<RaftHistoryEvent> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("Error locking raft state"))?;

        let target_owned = target.map(str::to_string);
        let event = match command {
            "trigger-election" => {
                state.term += 1;
                state.role = PersistenceRole::Primary;
                state.leader_id = Some(state.node_id.clone());
                state.commit_index += 1;
                state.applied_index = state.commit_index;
                state.election_epoch_ms =
                    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
                let node_id = state.node_id.clone();
                let term = state.term;
                Self::push_event(
                    &mut state,
                    "election",
                    format!(
                        "Manual election triggered by {actor}; {} became leader for term {}",
                        node_id, term
                    ),
                    actor,
                    target_owned,
                )
            }
            "step-down" => {
                state.term += 1;
                state.role = PersistenceRole::Standby;
                state.leader_id = Self::choose_new_leader(&state);
                state.election_epoch_ms =
                    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
                let leader = state
                    .leader_id
                    .clone()
                    .unwrap_or_else(|| "none".to_string());
                Self::push_event(
                    &mut state,
                    "failover",
                    format!(
                        "Leader step-down requested by {actor}; new leader: {}",
                        leader
                    ),
                    actor,
                    target_owned,
                )
            }
            "transfer-leader" => {
                let target = target.context("transfer-leader command requires target node id")?;
                if !state.voters.iter().any(|member| member == target) {
                    bail!("target node {target} is not a raft voter");
                }
                state.term += 1;
                state.leader_id = Some(target.to_string());
                state.role = if target == state.node_id {
                    PersistenceRole::Primary
                } else {
                    PersistenceRole::Standby
                };
                state.election_epoch_ms =
                    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
                Self::push_event(
                    &mut state,
                    "leadership_transfer",
                    format!("Leadership transferred to {target} by {actor}"),
                    actor,
                    Some(target.to_string()),
                )
            }
            "add-voter" => {
                let target = target.context("add-voter command requires target node id")?;
                if !state.voters.iter().any(|member| member == target) {
                    state.voters.push(target.to_string());
                    state.voters.sort();
                }
                Self::push_event(
                    &mut state,
                    "membership",
                    format!("Added voter {target}"),
                    actor,
                    Some(target.to_string()),
                )
            }
            "add-learner" => {
                let target = target.context("add-learner command requires target node id")?;
                if !state.learners.iter().any(|member| member == target) {
                    state.learners.push(target.to_string());
                    state.learners.sort();
                }
                Self::push_event(
                    &mut state,
                    "membership",
                    format!("Added learner {target}"),
                    actor,
                    Some(target.to_string()),
                )
            }
            "remove-member" => {
                let target = target.context("remove-member command requires target node id")?;
                state.voters.retain(|member| member != target);
                state.learners.retain(|member| member != target);
                state.peers.retain(|member| member != target);
                if state.leader_id.as_deref() == Some(target) {
                    state.leader_id = Self::choose_new_leader(&state);
                    state.role = if state.leader_id.as_deref() == Some(state.node_id.as_str()) {
                        PersistenceRole::Primary
                    } else {
                        PersistenceRole::Standby
                    };
                }
                Self::push_event(
                    &mut state,
                    "membership",
                    format!("Removed raft member {target}"),
                    actor,
                    Some(target.to_string()),
                )
            }
            other => bail!("unsupported raft command: {other}"),
        };

        info!(command, actor, target = ?target, role = state.role.as_str(), term = state.term, "Applied raft command");
        Ok(event)
    }

    fn issue_raft_join_token(&self, actor: &str) -> R<(String, String)> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("Error locking raft state"))?;
        let expires_at =
            OffsetDateTime::now_utc() + TimeDuration::seconds(state.join_token_ttl_seconds as i64);
        let token = format!(
            "raft:{}:{}:{}",
            state.cluster_id,
            state.node_id,
            expires_at.unix_timestamp()
        );
        let expires = expires_at.format(&time::format_description::well_known::Rfc3339)?;
        let ttl = state.join_token_ttl_seconds;
        let _ = Self::push_event(
            &mut state,
            "join_token",
            format!("Issued raft join token (ttl={}s)", ttl),
            actor,
            None,
        );
        Ok((token, expires))
    }
}

pub fn build_persistence_driver(
    config: &PersistenceConfigFull,
    db_path: std::path::PathBuf,
    manager_node_id: &str,
) -> Arc<dyn PersistenceDriver> {
    match config.r#type {
        PersistenceType::LocalDisk => Arc::new(LocalDiskPersistenceDriver::new()),
        PersistenceType::Streaming => Arc::new(StreamingPersistenceDriver::new(
            db_path,
            config.streaming.clone(),
        )),
        PersistenceType::Raft => {
            let node_id = if config.raft.node_id.trim().is_empty() {
                manager_node_id.to_string()
            } else {
                config.raft.node_id.clone()
            };
            Arc::new(RaftPersistenceDriver::new(node_id, &config.raft))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_disk_status_is_primary() {
        let d = LocalDiskPersistenceDriver::new();
        let status = d.status();
        assert_eq!(status.backend, "local_disk");
        assert_eq!(status.role, PersistenceRole::Primary);
        assert!(status.lease_held);
    }

    #[test]
    fn streaming_status_defaults_to_standby_before_start() {
        let d = StreamingPersistenceDriver::new(
            std::path::PathBuf::from("/tmp/store.sqlite"),
            StreamingPersistenceConfigFull::default(),
        );
        let status = d.status();
        assert_eq!(status.backend, "streaming");
        assert_eq!(status.role, PersistenceRole::Standby);
        assert!(!status.lease_held);
    }

    #[test]
    fn raft_status_reports_bootstrap_node() {
        let raft = RaftPersistenceConfigFull {
            node_id: "mgr-a".to_string(),
            bootstrap: true,
            peers: vec!["mgr-a".to_string(), "mgr-b".to_string()],
            join_token_ttl_seconds: 900,
        };
        let d = RaftPersistenceDriver::new("mgr-a".to_string(), &raft);
        d.start().unwrap();

        let status = d.raft_status().unwrap();
        assert!(status.enabled);
        assert_eq!(status.node_id, "mgr-a");
        assert_eq!(status.role, PersistenceRole::Primary);
        assert_eq!(
            status.voters,
            vec!["mgr-a".to_string(), "mgr-b".to_string()]
        );
    }

    #[test]
    fn parse_sync_interval_parses_and_defaults() {
        assert_eq!(parse_sync_interval("1s"), StdDuration::from_secs(1));
        assert_eq!(
            parse_sync_interval(" 500ms "),
            StdDuration::from_millis(500)
        );
        // Empty / unparseable values fall back to the 1s default.
        assert_eq!(parse_sync_interval(""), StdDuration::from_secs(1));
        assert_eq!(
            parse_sync_interval("not-a-duration"),
            StdDuration::from_secs(1)
        );
    }

    #[test]
    fn replica_url_dispatch_helpers() {
        assert!(is_object_store_url("s3://bucket/prefix"));
        assert!(is_object_store_url("S3://bucket/prefix"));
        assert!(!is_object_store_url("file:///var/replica"));
        assert!(!is_object_store_url("/var/replica"));

        assert_eq!(file_replica_path("file:///var/replica"), "/var/replica");
        assert_eq!(file_replica_path("/var/replica"), "/var/replica");
    }

    #[test]
    fn lease_path_prefix_derives_directory() {
        assert_eq!(
            lease_path_prefix("leases/default/primary.json"),
            "leases/default"
        );
        assert_eq!(lease_path_prefix("primary.json"), "");
        assert_eq!(lease_path_prefix("a/b/c"), "a/b");
    }

    // End-to-end proof that the rustyriver-backed driver captures WAL segments
    // and can reconstruct the database after total loss of the primary file.
    #[tokio::test]
    async fn streaming_replicates_and_restores_via_rustyriver() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("store.sqlite");
        let replica_dir = tmp.path().join("replica");

        // Open the managed db first (read-lock / checkpoint takeover), then write
        // through a separate connection — exactly how a host app sits alongside
        // the embedded replicator.
        let db = Db::open(&db_path).unwrap();
        let client = FileReplicaClient::new(replica_dir.to_string_lossy().into_owned());
        let mut replica = Replica::new(db, client);

        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);",
            )
            .unwrap();
            conn.execute("INSERT INTO events (name) VALUES (?1)", ["alpha"])
                .unwrap();
        }
        sync_once(&mut replica).await.unwrap();

        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute("INSERT INTO events (name) VALUES (?1)", ["beta"])
                .unwrap();
        }
        sync_once(&mut replica).await.unwrap();

        // Cleanly release the read lock, then simulate host loss by deleting the
        // primary database and its WAL/SHM sidecars.
        let db = replica.into_db().unwrap();
        db.close().unwrap();
        for suffix in ["", "-wal", "-shm"] {
            let p = tmp.path().join(format!("store.sqlite{suffix}"));
            let _ = std::fs::remove_file(p);
        }

        // Restore the most-recent state to a fresh path and verify both rows.
        let restored = tmp.path().join("restored.sqlite");
        let restore_client = FileReplicaClient::new(replica_dir.to_string_lossy().into_owned());
        rustyriver::restore(&restore_client, &restored, rustyriver::TXID(0))
            .await
            .unwrap();

        let conn = rusqlite::Connection::open(&restored).unwrap();
        let mut stmt = conn.prepare("SELECT name FROM events ORDER BY id").unwrap();
        let names: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(names, vec!["alpha".to_string(), "beta".to_string()]);
    }

    // The lease state machine: one owner acquires + renews, a second owner is
    // fenced out while the first holds a live lease.
    #[tokio::test]
    async fn lease_step_acquires_then_fences_second_owner() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let node_a = S3Leaser::new(Arc::clone(&store))
            .with_owner("node-a")
            .with_ttl(StdDuration::from_secs(30));
        let node_b = S3Leaser::new(Arc::clone(&store))
            .with_owner("node-b")
            .with_ttl(StdDuration::from_secs(30));

        // node-a acquires the lease.
        let lease_a = match lease_step(&node_a, None).await {
            LeaseTransition::Held(lease) => lease,
            other => panic!("expected node-a to acquire, got {other:?}"),
        };

        // node-b is fenced out while node-a's lease is live.
        match lease_step(&node_b, None).await {
            LeaseTransition::Blocked => {}
            other => panic!("expected node-b to be blocked, got {other:?}"),
        }

        // node-a renews successfully.
        match lease_step(&node_a, Some(lease_a)).await {
            LeaseTransition::Held(_) => {}
            other => panic!("expected node-a to renew, got {other:?}"),
        }
    }
}
