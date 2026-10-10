use crate::prelude::*;
use crate::storage::raft::driver::MultiRaftDriver;
use crate::storage::store::Store;
use crate::structs::{
    PersistenceConfigFull, PersistenceType, StreamingLeaseProvider,
    StreamingObjectStorageLeaseConfigFull, StreamingPersistenceConfigFull,
};

use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use rustyriver::replica_url::parse_replica_url_with_query;
use rustyriver::{
    Db, FileReplicaClient, Lease, Leaser, ObjectStoreClient, ObjectStoreConfig, Replica,
    ReplicaClient, S3Leaser,
};
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration as StdDuration;

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
    /// Per-lane detail, one entry per active raft group (`single` topology:
    /// one `control` entry; `lanes` topology: three). Empty before the
    /// engine starts.
    pub lanes: Vec<RaftLaneStatus>,
}

/// Status of one raft group ("lane") on this manager.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftLaneStatus {
    pub lane: String,
    pub role: PersistenceRole,
    pub leader_id: Option<String>,
    pub term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    /// Voting members of this lane's group, as known locally.
    pub voters: u64,
    /// Majority size required to commit on this lane (`voters / 2 + 1`).
    pub quorum: u64,
    /// Voter failures this lane survives while still committing
    /// (`(voters - 1) / 2`).
    pub survives: u64,
    /// Plain-language advisory when the voter count is even or below
    /// `raft.expectedVoters` (plan §7-C3, JC-29); `None` when healthy.
    pub warning: Option<String>,
}

/// Result of an operator raft command: the audit event it recorded, plus an
/// optional plain-language quorum advisory (e.g. a membership change that
/// left the cluster at an even voter count — JC-29 warns loudly instead of
/// blocking). The warning travels through `IssueRaftCommandReply.warning`
/// so the CLI/UI print it on the command's own output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftCommandOutcome {
    pub event: RaftHistoryEvent,
    pub warning: Option<String>,
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

    /// Start the driver with access to the (migrated) store. The raft driver
    /// overrides this — its engine needs the state DB for its state machines
    /// and installs the replication pipeline onto the store — and is called
    /// by `Store::start_storage` *after* migrations but *before* the manager
    /// serves traffic. Other backends fall back to [`Self::start`].
    fn start_engine(&self, _store: &Store) -> R<()> {
        self.start()
    }

    fn status(&self) -> PersistenceStatus;

    /// Why this manager must not write right now, or `None` when it may.
    /// `Store::replicated` refuses every non-raft write while this is set.
    /// Only a streaming standby (or a fenced ex-primary) refuses; raft
    /// enforces leadership in its own pipeline.
    fn write_refusal(&self) -> Option<String> {
        None
    }

    /// Concrete-type escape hatch (e.g. to reach the raft driver's transport
    /// registry from the daemon wiring).
    fn as_any(&self) -> &dyn std::any::Any;

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
    ) -> R<RaftCommandOutcome> {
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Streaming persistence driver backed by [`rustyriver`].
///
/// Replaces the former out-of-process `litestream` sidecar with rustyriver's
/// in-process embedding API: a managed [`Db`] takes over SQLite checkpointing
/// and ships each WAL segment to object storage through a [`Replica`], while an
/// optional [`S3Leaser`] provides single-primary fencing.
///
/// Only the primary writes and ships WAL (R4). With the object-storage lease
/// a manager starts as a standby: its leader-only loops idle
/// (`Store::is_raft_leader` reads the role) and the store refuses writes
/// ([`PersistenceDriver::write_refusal`]). When it acquires the lease it
/// first restores the store from the replica, then starts shipping, and only
/// then reports itself primary. A primary that loses the lease (or cannot
/// renew it for a whole TTL) is fenced for the rest of the process: it stops
/// shipping, refuses writes, and does not compete for the lease again until
/// it is restarted.
#[derive(Debug)]
pub struct StreamingPersistenceDriver {
    db_path: std::path::PathBuf,
    cfg: StreamingPersistenceConfigFull,
    role_primary: Arc<AtomicBool>,
    lease_held: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    /// Set once this process lost the lease after holding it. Never cleared.
    fenced: Arc<AtomicBool>,
    /// The store a promotion restores into. A clone with a placeholder
    /// driver, so it holds no reference back to this driver.
    store: Arc<std::sync::OnceLock<Store>>,
}

impl StreamingPersistenceDriver {
    pub fn new(db_path: std::path::PathBuf, cfg: StreamingPersistenceConfigFull) -> Self {
        Self {
            db_path,
            cfg,
            role_primary: Arc::new(AtomicBool::new(false)),
            lease_held: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            fenced: Arc::new(AtomicBool::new(false)),
            store: Arc::new(std::sync::OnceLock::new()),
        }
    }

    fn host_identity() -> String {
        std::env::var("HOSTNAME").unwrap_or_else(|_| "legion-manager".to_string())
    }

    fn promotion(&self) -> Promotion {
        Promotion {
            db_path: self.db_path.clone(),
            replica_url: self.cfg.replica_url.clone(),
            interval: parse_sync_interval(&self.cfg.sync_interval),
            store: Arc::clone(&self.store),
            shutdown: Arc::clone(&self.shutdown),
            fenced: Arc::clone(&self.fenced),
        }
    }

    fn start_object_storage_lease_loop(&self) -> R<()> {
        let lease_cfg = self.cfg.lease.clone();
        let object_cfg = lease_cfg.object_storage.clone();
        let role_primary = Arc::clone(&self.role_primary);
        let lease_held = Arc::clone(&self.lease_held);
        let promotion = self.promotion();
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
                        promotion,
                    )
                    .await
                    {
                        error!(error = ?err, "Object-storage lease loop exited; this manager stays a standby");
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
        match self.cfg.lease.provider {
            // Manual failover: this manager is the primary from the start,
            // and the operator keeps it the only one.
            StreamingLeaseProvider::None => {
                start_replication(
                    &self.db_path,
                    &self.cfg.replica_url,
                    parse_sync_interval(&self.cfg.sync_interval),
                    Arc::clone(&self.shutdown),
                    Arc::clone(&self.fenced),
                )?;
                self.role_primary.store(true, Ordering::Relaxed);
                self.lease_held.store(true, Ordering::Relaxed);
            }
            // Standby until the lease loop acquires the lease and promotes.
            StreamingLeaseProvider::ObjectStorage => {
                self.start_object_storage_lease_loop()?;
            }
        }

        Ok(())
    }

    fn start_engine(&self, store: &Store) -> R<()> {
        let _ = self.store.set(store.raft_engine_clone());
        self.start()
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

    fn write_refusal(&self) -> Option<String> {
        if self.role_primary.load(Ordering::Relaxed) {
            return None;
        }
        Some(if self.fenced.load(Ordering::Relaxed) {
            "this streaming manager lost the persistence lease and is fenced: it no longer \
             writes. Send writes to the current primary, and restart this manager to rejoin \
             as a standby (docs/persistence-streaming.md)"
                .to_string()
        } else {
            "this streaming manager is a standby (it does not hold the persistence lease) and \
             does not accept writes. Send writes to the primary (docs/persistence-streaming.md)"
                .to_string()
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Everything a lease holder needs to turn into the primary (R4).
#[derive(Debug, Clone)]
struct Promotion {
    db_path: std::path::PathBuf,
    replica_url: String,
    interval: StdDuration,
    store: Arc<std::sync::OnceLock<Store>>,
    shutdown: Arc<AtomicBool>,
    fenced: Arc<AtomicBool>,
}

impl Promotion {
    /// Bring this manager's store up to the replica, then start shipping.
    ///
    /// A standby's local database is whatever it had when it last ran, which
    /// can be far behind what the previous primary shipped. Serving or
    /// shipping from it would put stale state in front of clients and
    /// overwrite newer history in the replica. So a new primary first
    /// restores the replica's latest state into its store, migrates it to
    /// this build's schema, and swaps it in. An empty replica (a brand-new
    /// deployment) leaves the local database as it is. rustyriver's local
    /// tracking state is reset either way, so shipping starts with a fresh
    /// snapshot that continues the replica's history.
    async fn run(&self) -> R<()> {
        let store = self
            .store
            .get()
            .context("streaming store is not started yet")?;
        if !self.replica_url.trim().is_empty() {
            let staged = staged_restore_path(&self.db_path);
            for path in [&staged, &staged.with_extension("tmp")] {
                let _ = std::fs::remove_file(path);
            }
            if restore_replica_to(&self.replica_url, &staged).await? {
                {
                    let mut conn = rusqlite::Connection::open(&staged).with_context(|| {
                        format!("failed opening restored store {}", staged.display())
                    })?;
                    crate::storage::migrations::get_migrations()
                        .to_latest(&mut conn)
                        .context("failed migrating the restored store")?;
                }
                store.install_state_db(&staged)?;
                info!(path = %self.db_path.display(), "Restored the store from the streaming replica before taking over as primary");
            } else {
                info!("Streaming replica is empty; the new primary keeps its local store");
            }
        }
        match std::fs::remove_dir_all(Db::meta_path_for_path(&self.db_path)) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err).context("failed resetting rustyriver's local state"),
        }
        start_replication(
            &self.db_path,
            &self.replica_url,
            self.interval,
            Arc::clone(&self.shutdown),
            Arc::clone(&self.fenced),
        )
    }
}

/// Where a promotion stages the restored database: next to the store, so
/// the swap is a same-filesystem rename.
fn staged_restore_path(db_path: &std::path::Path) -> std::path::PathBuf {
    let mut name = db_path.file_name().unwrap_or_default().to_os_string();
    name.push(".promote-restore");
    db_path.with_file_name(name)
}

/// Restore the replica's latest state to `output`. `Ok(false)` when the
/// replica holds nothing yet.
async fn restore_replica_to(replica_url: &str, output: &std::path::Path) -> R<bool> {
    async fn restore_with<C: ReplicaClient>(client: C, output: &std::path::Path) -> R<bool> {
        if client
            .ltx_files(0, rustyriver::TXID(0), false)
            .await
            .context("failed listing the streaming replica")?
            .is_empty()
        {
            return Ok(false);
        }
        rustyriver::restore(&client, output, rustyriver::TXID(0))
            .await
            .context("failed restoring the streaming replica")?;
        Ok(true)
    }
    if is_object_store_url(replica_url) {
        let parsed = parse_replica_url_with_query(replica_url)
            .with_context(|| format!("invalid streaming replica URL: {replica_url}"))?;
        let config = ObjectStoreConfig::from_url(&parsed)
            .with_context(|| format!("invalid streaming replica URL: {replica_url}"))?;
        restore_with(ObjectStoreClient::new(config), output).await
    } else {
        restore_with(
            FileReplicaClient::new(file_replica_path(replica_url)),
            output,
        )
        .await
    }
}

/// Open the managed database and drive the rustyriver WAL→object-storage
/// sync loop on a dedicated thread (the in-process replacement for the old
/// `litestream replicate` subprocess). Only the primary calls this. The loop
/// stops at shutdown or when this manager is fenced.
fn start_replication(
    db_path: &std::path::Path,
    replica_url: &str,
    interval: StdDuration,
    shutdown: Arc<AtomicBool>,
    fenced: Arc<AtomicBool>,
) -> R<()> {
    if replica_url.trim().is_empty() {
        info!("Streaming persistence enabled without replica URL; skipping rustyriver replication");
        return Ok(());
    }

    // Open synchronously so the long-running read lock that takes over
    // checkpointing is held before this returns. The `Db` is `!Sync`, so it
    // is moved onto — and stays on — the worker thread from here.
    let db = Db::open(db_path).with_context(|| {
        format!(
            "rustyriver failed to open managed database at {}",
            db_path.display()
        )
    })?;

    let replica_url = replica_url.to_string();
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
                if let Err(err) =
                    run_replica_loop(db, replica_url, interval, shutdown, fenced).await
                {
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
    fenced: Arc<AtomicBool>,
) -> R<()> {
    if is_object_store_url(&replica_url) {
        let parsed = parse_replica_url_with_query(&replica_url)
            .with_context(|| format!("invalid streaming replica URL: {replica_url}"))?;
        let config = ObjectStoreConfig::from_url(&parsed)
            .with_context(|| format!("invalid streaming replica URL: {replica_url}"))?;
        let client = ObjectStoreClient::new(config);
        replica_sync_loop(db, client, interval, shutdown, fenced).await
    } else {
        let client = FileReplicaClient::new(file_replica_path(&replica_url));
        replica_sync_loop(db, client, interval, shutdown, fenced).await
    }
}

/// Generic sync loop: capture the WAL into a local LTX file, upload new LTX
/// files to the replica, sleep, repeat — until `shutdown` is signalled.
async fn replica_sync_loop<C: ReplicaClient>(
    db: Db,
    client: C,
    interval: StdDuration,
    shutdown: Arc<AtomicBool>,
    fenced: Arc<AtomicBool>,
) -> R<()> {
    let mut replica = Replica::new(db, client);
    // Continue the replica's history when the local database is behind it
    // (a fresh primary that just restored from it).
    if let Err(err) = replica.check_database_behind_replica().await {
        warn!(error = ?err, "rustyriver could not compare the database with the replica");
    }
    // A fenced ex-primary must never ship again: another primary owns the
    // replica now.
    while !shutdown.load(Ordering::Relaxed) && !fenced.load(Ordering::Relaxed) {
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
/// `"leases/default/lock.json"`. See internal_docs/rustyriver-migration.md.
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
    promotion: Promotion,
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
    lease_loop(&leaser, ttl, renew, role_primary, lease_held, promotion).await;
    Ok(())
}

/// Acquire, renew, promote, and fence (see [`StreamingPersistenceDriver`]).
/// Returns at shutdown, or once this manager is fenced.
async fn lease_loop<L: Leaser>(
    leaser: &L,
    ttl: StdDuration,
    renew: StdDuration,
    role_primary: Arc<AtomicBool>,
    lease_held: Arc<AtomicBool>,
    promotion: Promotion,
) {
    let mut held: Option<Lease> = None;
    let mut last_renewed = std::time::Instant::now();
    let fence = |reason: &str| {
        promotion.fenced.store(true, Ordering::Relaxed);
        role_primary.store(false, Ordering::Relaxed);
        lease_held.store(false, Ordering::Relaxed);
        error!(
            "Streaming primary fenced: {reason}. This manager stopped shipping WAL and refuses \
             writes; restart it to rejoin as a standby"
        );
    };

    loop {
        if promotion.shutdown.load(Ordering::Relaxed) {
            if let Some(lease) = held.take() {
                let _ = leaser.release_lease(&lease).await;
            }
            role_primary.store(false, Ordering::Relaxed);
            lease_held.store(false, Ordering::Relaxed);
            return;
        }

        let was_primary = role_primary.load(Ordering::Relaxed);
        match lease_step(leaser, held.take()).await {
            LeaseTransition::Held(lease) if was_primary => {
                held = Some(lease);
                last_renewed = std::time::Instant::now();
            }
            LeaseTransition::Held(lease) => {
                match promotion.run().await {
                    Ok(()) => {
                        held = Some(lease);
                        last_renewed = std::time::Instant::now();
                        lease_held.store(true, Ordering::Relaxed);
                        role_primary.store(true, Ordering::Relaxed);
                        info!("Acquired the persistence lease; this manager is now the streaming primary");
                    }
                    Err(err) => {
                        // Give the lease back so another standby can try.
                        error!(error = ?err, "Could not take over as streaming primary; staying a standby");
                        let _ = leaser.release_lease(&lease).await;
                    }
                }
            }
            LeaseTransition::Retain(lease) => {
                // A renewal failed but the lease may still be ours. Past one
                // TTL without a renewal another manager may hold it, so stop
                // acting as primary.
                if was_primary && last_renewed.elapsed() >= ttl {
                    fence("the lease could not be renewed within its TTL");
                    return;
                }
                held = Some(lease);
            }
            LeaseTransition::Lost if was_primary => {
                fence("another manager took over the lease");
                return;
            }
            LeaseTransition::Blocked | LeaseTransition::Lost => {
                role_primary.store(false, Ordering::Relaxed);
                lease_held.store(false, Ordering::Relaxed);
            }
        }

        tokio::time::sleep(renew).await;
    }
}

/// Builds the persistence driver for the configured backend. The raft
/// driver is only *constructed* here — its engine starts later via
/// [`PersistenceDriver::start_engine`], once `Store::start_storage` has run
/// migrations (the engine's state machines need the migrated DB).
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
            Arc::new(MultiRaftDriver::new(node_id, manager_node_id, &config.raft))
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

    // Real-driver bootstrap status: a single-node raft store on a tempdir
    // elects itself and reports leader status through the driver. The full
    // propose->commit->apply path is covered by tests/raft_apply_roundtrip.rs.
    #[test]
    fn raft_status_reports_bootstrap_node() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::structs::ManagerConfigFull {
            store_path: dir.path().join("store").to_str().unwrap().to_string(),
            ..Default::default()
        };
        config.persistence.r#type = PersistenceType::Raft;
        config.persistence.raft.bootstrap = true;
        config.persistence.raft.node_id = "mgr-a".to_string();
        config.persistence.raft.data_dir = dir.path().join("raft").to_str().unwrap().to_string();
        config.persistence.raft.peers = vec!["mgr-b=127.0.0.1:9999".to_string()];

        let store = Store::start_storage(&config).unwrap();
        let status = store.raft_status().unwrap();
        assert!(status.enabled);
        assert_eq!(status.backend, "raft");
        assert_eq!(status.raft_library, "openraft-0.9");
        assert_eq!(status.node_id, "mgr-a");
        assert_eq!(status.role, PersistenceRole::Primary);
        assert_eq!(status.leader_id.as_deref(), Some("mgr-a"));
        assert_eq!(status.voters, vec!["mgr-a".to_string()]);
        assert_eq!(status.peers, vec!["mgr-b".to_string()]);
        assert!(status.term >= 1);
        assert!(!status.cluster_id.is_empty());
        // Per-lane quorum math (plan §7-C3): one voter, quorum 1, survives
        // nothing, and no parity/expectation warning.
        let lane = status
            .lanes
            .first()
            .expect("single topology reports the control lane");
        assert_eq!(lane.voters, 1);
        assert_eq!(lane.quorum, 1);
        assert_eq!(lane.survives, 0);
        assert!(lane.warning.is_none(), "warning: {:?}", lane.warning);
        // The cluster secret was minted and persisted at bootstrap.
        assert!(dir.path().join("raft/cluster_secret").exists());
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

    fn local_store(dir: &std::path::Path) -> Store {
        let config = crate::structs::ManagerConfigFull {
            store_path: dir.to_str().unwrap().to_string(),
            ..Default::default()
        };
        Store::start_storage(&config).unwrap()
    }

    // A streaming standby (lease provider set, lease not held) neither acts
    // as the writer nor accepts writes (R4). The lease loop here can never
    // acquire (no bucket), so the manager stays a standby for good.
    #[test]
    fn streaming_standby_idles_and_refuses_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::structs::ManagerConfigFull {
            store_path: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };
        config.persistence.r#type = PersistenceType::Streaming;
        config.persistence.streaming.lease.provider = StreamingLeaseProvider::ObjectStorage;
        let store = Store::start_storage(&config).unwrap();

        assert_eq!(store.persistence_role(), PersistenceRole::Standby);
        assert!(!store.is_raft_leader(crate::storage::Lane::Control));
        assert!(!store.is_raft_leader(crate::storage::Lane::Scheduler));
        let err = store
            .put_kv(
                "standby/key",
                "v",
                crate::structs::KvValueType::String,
                None,
                None,
            )
            .expect_err("a standby must refuse writes");
        assert!(format!("{err:#}").contains("standby"), "{err:#}");

        // local_disk is always the writer.
        let local = local_store(&dir.path().join("local"));
        assert!(local.is_raft_leader(crate::storage::Lane::Control));
    }

    // A manager that takes the lease restores the replica into its store
    // before it reports itself primary (R4): its stale local database never
    // serves or ships.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn taking_the_lease_restores_the_replica_before_serving() {
        let tmp = tempfile::tempdir().unwrap();
        let replica_dir = tmp.path().join("replica");

        // The old primary writes and ships.
        let old = local_store(&tmp.path().join("old"));
        old.put_kv(
            "ha/key",
            "shipped",
            crate::structs::KvValueType::String,
            None,
            None,
        )
        .unwrap();
        let db = Db::open(old.state_db_path()).unwrap();
        let mut replica = Replica::new(
            db,
            FileReplicaClient::new(replica_dir.to_string_lossy().into_owned()),
        );
        sync_once(&mut replica).await.unwrap();
        replica.into_db().unwrap().close().unwrap();
        let shipped = old.kv_stored_form("ha/key").unwrap();
        assert!(shipped.is_some());

        // A standby with an unrelated local store takes the lease.
        let standby = local_store(&tmp.path().join("standby"));
        assert_eq!(standby.kv_stored_form("ha/key").unwrap(), None);
        let cell = Arc::new(std::sync::OnceLock::new());
        let _ = cell.set(standby.raft_engine_clone());
        let promotion = Promotion {
            db_path: standby.state_db_path().to_path_buf(),
            replica_url: replica_dir.to_string_lossy().into_owned(),
            interval: StdDuration::from_millis(200),
            store: cell,
            shutdown: Arc::new(AtomicBool::new(false)),
            fenced: Arc::new(AtomicBool::new(false)),
        };
        let role_primary = Arc::new(AtomicBool::new(false));
        let lease_held = Arc::new(AtomicBool::new(false));
        let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let leaser = S3Leaser::new(objects)
            .with_owner("standby")
            .with_ttl(StdDuration::from_secs(30));
        let shutdown = Arc::clone(&promotion.shutdown);
        let primary_flag = Arc::clone(&role_primary);
        let task = tokio::spawn(async move {
            lease_loop(
                &leaser,
                StdDuration::from_secs(30),
                StdDuration::from_millis(100),
                primary_flag,
                lease_held,
                promotion,
            )
            .await;
        });
        let deadline = std::time::Instant::now() + StdDuration::from_secs(30);
        while !role_primary.load(Ordering::Relaxed) {
            assert!(
                std::time::Instant::now() < deadline,
                "standby never promoted"
            );
            tokio::time::sleep(StdDuration::from_millis(50)).await;
        }
        assert_eq!(standby.kv_stored_form("ha/key").unwrap(), shipped);
        shutdown.store(true, Ordering::Relaxed);
        task.await.unwrap();
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
