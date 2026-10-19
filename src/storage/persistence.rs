use crate::prelude::*;
use crate::storage::objects::{
    self, AwsCredential, AwsKeys, ObjectCredential, OpenOptions, Principal, RegionDefault,
    S3Location, UrlOrPath,
};
use crate::storage::raft::driver::MultiRaftDriver;
use crate::storage::store::Store;
use crate::structs::{
    PersistenceConfigFull, PersistenceType, StreamingCredentialsConfig, StreamingLeaseProvider,
    StreamingObjectStorageLeaseConfigFull, StreamingPersistenceConfigFull,
};

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

    /// Orderly stop on process shutdown (SIGTERM/SIGINT): flush what must be
    /// durable and release what peers are waiting on, so a clean stop loses
    /// nothing a crash would not. Blocks, so call it from a blocking
    /// context, and returns within roughly `timeout`. The default has
    /// nothing to do (local disk is durable on every commit).
    fn shutdown(&self, _timeout: StdDuration) {}

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
    /// Stops the object-storage lease loop (which releases the lease).
    shutdown: Arc<AtomicBool>,
    /// Stops the replica sync loop, which runs one final sync on the way out.
    /// Separate from `shutdown` so an orderly stop can finish the final sync
    /// before giving up the lease: a standby that takes over must find every
    /// committed write in the replica.
    replica_shutdown: Arc<AtomicBool>,
    /// Fires when the replica sync thread has exited (final sync done). Shared
    /// with the promotion, which is what starts that thread under a lease.
    replica_exited: ExitSlot,
    /// Fires when the lease loop thread has exited (lease released).
    lease_exited: ExitSlot,
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
            replica_shutdown: Arc::new(AtomicBool::new(false)),
            replica_exited: ExitSlot::default(),
            lease_exited: ExitSlot::default(),
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
            credentials: self.cfg.credentials.clone(),
            interval: parse_sync_interval(&self.cfg.sync_interval),
            store: Arc::clone(&self.store),
            shutdown: Arc::clone(&self.shutdown),
            replica_shutdown: Arc::clone(&self.replica_shutdown),
            replica_exited: Arc::clone(&self.replica_exited),
            fenced: Arc::clone(&self.fenced),
        }
    }

    fn start_object_storage_lease_loop(&self) -> R<()> {
        let lease_cfg = self.cfg.lease.clone();
        let object_cfg = lease_cfg.object_storage.clone();
        let credentials = self.cfg.credentials.clone();
        let role_primary = Arc::clone(&self.role_primary);
        let lease_held = Arc::clone(&self.lease_held);
        let promotion = self.promotion();
        let owner = Self::host_identity();
        let (exited_tx, exited_rx) = std::sync::mpsc::channel();
        if let Ok(mut slot) = self.lease_exited.lock() {
            *slot = Some(exited_rx);
        }

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
                        credentials,
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
                let _ = exited_tx.send(());
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
        self.replica_shutdown.store(true, Ordering::Relaxed);
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

/// Where a loop thread reports that it has exited. Empty until the thread
/// starts, so a loop that never ran counts as exited.
type ExitSlot = Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>;

/// Waits for a loop thread's exit signal until `deadline`. A thread that
/// never started (no receiver) counts as exited.
fn wait_for_exit(slot: &ExitSlot, deadline: std::time::Instant) -> bool {
    let Some(receiver) = slot.lock().ok().and_then(|mut r| r.take()) else {
        return true;
    };
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    // A closed channel means the thread is gone too.
    !matches!(
        receiver.recv_timeout(remaining),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    )
}

/// Resolve the object-storage credential source once, before any loop
/// starts, so a misconfiguration fails the boot instead of surfacing as a
/// sync loop that never uploads. `None` when nothing in this config talks to
/// object storage. Each store re-resolves from the same config and
/// environment when it is built.
fn resolve_streaming_credentials(
    cfg: &StreamingPersistenceConfigFull,
    env: &dyn Fn(&str) -> Option<String>,
) -> R<Option<Principal>> {
    let uses_lease = matches!(cfg.lease.provider, StreamingLeaseProvider::ObjectStorage);
    let replica = objects::url_or_path(&cfg.replica_url).with_context(|| {
        format!(
            "invalid persistence.streaming.replicaUrl {:?}",
            cfg.replica_url
        )
    })?;
    if !matches!(replica, UrlOrPath::S3(_)) && !uses_lease {
        return Ok(None);
    }
    let source = objects::resolve_principal(&streaming_credential(cfg.credentials.as_ref()), env)?;
    // object_store derives the STS endpoint from the store's region, and
    // `auto` (the lease's default, which R2 wants) is not an AWS region.
    if uses_lease
        && matches!(source, Principal::WebIdentity { .. })
        && cfg.lease.object_storage.region.trim() == "auto"
    {
        bail!(
            "persistence.streaming.lease.objectStorage.region is 'auto', but the manager \
             authenticates with web identity, which needs the bucket's real AWS region to \
             reach STS; set the region"
        );
    }
    Ok(Some(source))
}

/// Who streaming persistence signs as before resolution: the
/// `persistence.streaming.credentials` keys when set, else the manager's
/// ambient chain. Every store it opens (replica, restore, lease) uses this.
fn streaming_credential(config: Option<&StreamingCredentialsConfig>) -> ObjectCredential {
    match config {
        Some(keys) => ObjectCredential::Aws(AwsCredential {
            keys: AwsKeys {
                access_key_id: keys.access_key_id.clone().unwrap_or_default(),
                secret_access_key: keys.secret_access_key.clone().unwrap_or_default(),
                session_token: keys.session_token.clone(),
            },
            region: None,
            endpoint: None,
            origin: "persistence.streaming.credentials".to_string(),
        }),
        None => ObjectCredential::Ambient,
    }
}

/// How streaming persistence opens a store: its config and replica URL
/// describe the store completely, so the process's other `AWS_*` settings
/// (an `AWS_ENDPOINT` meant for something else) never redirect it.
fn streaming_open_options(conditional_put: bool) -> OpenOptions {
    OpenOptions {
        conditional_put,
        // Every store here names its region; an empty one falls back to
        // object_store's own default, never to AWS_REGION.
        region_default: RegionDefault::Fixed(objects::FALLBACK_REGION),
        aws_env_settings: false,
        // An ambiguous environment fails the boot instead of signing the
        // cluster's backups as the node's instance role
        // (manager-workload-identity calls #5).
        ambient_fallthrough: false,
    }
}

/// The boot line that says which principal the manager is about to act as.
fn log_streaming_credentials(source: &Principal) {
    match source {
        Principal::WebIdentity {
            role_arn,
            token_file,
        } => info!(
            credential_source = source.as_str(),
            role_arn = %role_arn,
            token_file = %token_file,
            "Streaming persistence authenticates to object storage"
        ),
        _ => info!(
            credential_source = source.as_str(),
            "Streaming persistence authenticates to object storage"
        ),
    }
}

impl PersistenceDriver for StreamingPersistenceDriver {
    fn start(&self) -> R<()> {
        if let Some(source) =
            resolve_streaming_credentials(&self.cfg, &|name| std::env::var(name).ok())?
        {
            log_streaming_credentials(&source);
        }
        match self.cfg.lease.provider {
            // Manual failover: this manager is the primary from the start,
            // and the operator keeps it the only one.
            StreamingLeaseProvider::None => {
                start_replication(
                    &self.db_path,
                    &self.cfg.replica_url,
                    self.cfg.credentials.clone(),
                    parse_sync_interval(&self.cfg.sync_interval),
                    Arc::clone(&self.replica_shutdown),
                    Arc::clone(&self.fenced),
                    &self.replica_exited,
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

    /// Final replica sync first, then lease release, so a standby that wins
    /// the lease restores every write this primary committed.
    fn shutdown(&self, timeout: StdDuration) {
        let deadline = std::time::Instant::now() + timeout;
        self.replica_shutdown.store(true, Ordering::Relaxed);
        if wait_for_exit(&self.replica_exited, deadline) {
            info!("Streaming persistence: final replica sync finished");
        } else {
            warn!(
                timeout_secs = timeout.as_secs(),
                "Streaming persistence: final replica sync did not finish in time; \
                 the newest writes may be missing from the replica"
            );
        }
        self.shutdown.store(true, Ordering::Relaxed);
        if !wait_for_exit(&self.lease_exited, deadline) {
            warn!("Streaming persistence: lease release did not finish in time; a standby takes over once the lease TTL expires");
        }
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
    credentials: Option<StreamingCredentialsConfig>,
    interval: StdDuration,
    store: Arc<std::sync::OnceLock<Store>>,
    /// The lease loop's stop flag.
    shutdown: Arc<AtomicBool>,
    /// The replica sync loop's stop flag (see the driver's field).
    replica_shutdown: Arc<AtomicBool>,
    replica_exited: ExitSlot,
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
            if restore_replica_to(&self.replica_url, self.credentials.as_ref(), &staged).await? {
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
            self.credentials.clone(),
            self.interval,
            Arc::clone(&self.replica_shutdown),
            Arc::clone(&self.fenced),
            &self.replica_exited,
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
async fn restore_replica_to(
    replica_url: &str,
    credentials: Option<&StreamingCredentialsConfig>,
    output: &std::path::Path,
) -> R<bool> {
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
    match objects::url_or_path(replica_url)? {
        UrlOrPath::S3(url) => {
            restore_with(replica_object_store_client(url, credentials)?, output).await
        }
        UrlOrPath::Local(path) => restore_with(file_replica_client(&path), output).await,
    }
}

/// Open the managed database and drive the rustyriver WAL→object-storage
/// sync loop on a dedicated thread (the in-process replacement for the old
/// `litestream replicate` subprocess). Only the primary calls this. The loop
/// stops at shutdown or when this manager is fenced.
fn start_replication(
    db_path: &std::path::Path,
    replica_url: &str,
    credentials: Option<StreamingCredentialsConfig>,
    interval: StdDuration,
    shutdown: Arc<AtomicBool>,
    fenced: Arc<AtomicBool>,
    exited: &ExitSlot,
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
    let (exited_tx, exited_rx) = std::sync::mpsc::channel();
    if let Ok(mut slot) = exited.lock() {
        *slot = Some(exited_rx);
    }
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
                    run_replica_loop(db, replica_url, credentials, interval, shutdown, fenced).await
                {
                    error!(error = ?err, "rustyriver replication loop exited");
                }
            });
            let _ = exited_tx.send(());
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

/// rustyriver's local replica client for a `file://` (or bare-path) replica
/// URL: the object-storage layer classifies the URL, and rustyriver owns the
/// on-disk layout (`<root>/ltx/<level>/…`).
fn file_replica_client(path: &std::path::Path) -> FileReplicaClient {
    FileReplicaClient::new(path.to_string_lossy().into_owned())
}

/// Select the rustyriver replica client for the URL and drive the sync loop.
async fn run_replica_loop(
    db: Db,
    replica_url: String,
    credentials: Option<StreamingCredentialsConfig>,
    interval: StdDuration,
    shutdown: Arc<AtomicBool>,
    fenced: Arc<AtomicBool>,
) -> R<()> {
    match objects::url_or_path(&replica_url)? {
        UrlOrPath::S3(url) => {
            let client = replica_object_store_client(url, credentials.as_ref())?;
            replica_sync_loop(db, client, interval, shutdown, fenced).await
        }
        UrlOrPath::Local(path) => {
            let client = file_replica_client(&path);
            replica_sync_loop(db, client, interval, shutdown, fenced).await
        }
    }
}

/// The rustyriver client for an `s3://` replica URL, on a store the
/// object-storage layer opens, so the replica authenticates exactly like the
/// lease does.
///
/// rustyriver's own `ObjectStoreConfig::from_url` still parses the URL
/// (bucket, region, endpoint, path-style and the provider defaults: its
/// dialect accepts more than a plain `s3://` URL), but its credential
/// handling — `AWS_*`/`LITESTREAM_*` static keys only — is replaced: the
/// keys it read are cleared so the client never holds them.
fn replica_object_store_client(
    replica_url: &str,
    credentials: Option<&StreamingCredentialsConfig>,
) -> R<ObjectStoreClient> {
    let parsed = parse_replica_url_with_query(replica_url)
        .with_context(|| format!("invalid streaming replica URL: {replica_url}"))?;
    let mut config = ObjectStoreConfig::from_url(&parsed)
        .with_context(|| format!("invalid streaming replica URL: {replica_url}"))?;
    config.access_key_id.clear();
    config.secret_access_key.clear();

    let location = S3Location {
        bucket: config.bucket.clone(),
        region: Some(if config.region.is_empty() {
            rustyriver::client::object_store::DEFAULT_REGION.to_string()
        } else {
            config.region.clone()
        }),
        endpoint: (!config.endpoint.is_empty()).then(|| config.endpoint.clone()),
        force_path_style: Some(config.force_path_style),
        skip_verify: config.skip_verify,
    };
    let store = objects::open_s3(
        &location,
        &streaming_credential(credentials),
        &streaming_open_options(false),
    )
    .with_context(|| format!("streaming replica {replica_url}"))?;
    Ok(ObjectStoreClient::with_store(config, store))
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
        sleep_unless_shutdown(interval, &shutdown).await;
    }
    // Final sync: ship whatever was committed since the last cycle, so an
    // orderly stop leaves the replica as current as the local database. Never
    // when fenced: another primary owns the replica now.
    if !fenced.load(Ordering::Relaxed) {
        if let Err(err) = sync_once(&mut replica).await {
            warn!(error = ?err, "rustyriver final replica sync failed during shutdown");
        }
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

/// Build the S3-compatible object store that backs the lease lock object,
/// with the same credentials as the replica.
fn build_lease_store(
    object_cfg: &StreamingObjectStorageLeaseConfigFull,
    credentials: Option<&StreamingCredentialsConfig>,
) -> R<Arc<dyn ObjectStore>> {
    let location = S3Location {
        bucket: object_cfg.bucket.clone(),
        region: Some(object_cfg.region.clone()),
        endpoint: object_cfg.endpoint.clone(),
        // The lease has always used path-style addressing.
        force_path_style: Some(true),
        skip_verify: false,
    };
    // S3Leaser acquires with `PutMode::Create` and renews with
    // `PutMode::Update`; both need ETag preconditions on the wire.
    objects::open_s3(
        &location,
        &streaming_credential(credentials),
        &streaming_open_options(true),
    )
    .context("failed to build S3 store for persistence lease")
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
    credentials: Option<StreamingCredentialsConfig>,
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

    let store = build_lease_store(&object_cfg, credentials.as_ref())?;
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

        sleep_unless_shutdown(renew, &promotion.shutdown).await;
    }
}

/// Sleeps for `duration`, waking early (within ~100ms) once `shutdown` is
/// set, so an orderly stop is not held up by a long sync or renew interval.
async fn sleep_unless_shutdown(duration: StdDuration, shutdown: &AtomicBool) {
    const STEP: StdDuration = StdDuration::from_millis(100);
    let deadline = tokio::time::Instant::now() + duration;
    while !shutdown.load(Ordering::Relaxed) {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return;
        }
        tokio::time::sleep(STEP.min(deadline - now)).await;
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
    // propose->commit->apply path is covered by tests/integration/raft_apply_roundtrip.rs.
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
        let is_object_store_url =
            |url: &str| matches!(objects::url_or_path(url), Ok(UrlOrPath::S3(_)));
        let file_replica_path = |url: &str| match objects::url_or_path(url) {
            Ok(UrlOrPath::Local(path)) => path.to_string_lossy().into_owned(),
            other => panic!("{url}: {other:?}"),
        };
        assert!(is_object_store_url("s3://bucket/prefix"));
        assert!(is_object_store_url("S3://bucket/prefix"));
        assert!(!is_object_store_url("file:///var/replica"));
        assert!(!is_object_store_url("/var/replica"));

        assert_eq!(file_replica_path("file:///var/replica"), "/var/replica");
        assert_eq!(file_replica_path("/var/replica"), "/var/replica");
    }

    fn s3_streaming_config(lease: StreamingLeaseProvider) -> StreamingPersistenceConfigFull {
        let mut cfg = StreamingPersistenceConfigFull {
            replica_url: "s3://state/prod".into(),
            ..Default::default()
        };
        cfg.lease.provider = lease;
        cfg.lease.object_storage.bucket = "state".into();
        cfg
    }

    #[test]
    fn credentials_are_only_resolved_when_object_storage_is_used_test() {
        // A half key pair is fatal for an S3 replica, and irrelevant to a
        // file replica with no lease.
        let half = |name: &str| (name == "AWS_ACCESS_KEY_ID").then(|| "AKIA".to_string());
        let mut cfg = s3_streaming_config(StreamingLeaseProvider::None);
        let err = resolve_streaming_credentials(&cfg, &half).unwrap_err();
        assert!(err.to_string().contains("no secret access key"), "{err}");

        cfg.replica_url = "file:///var/lib/legion/replica".into();
        assert!(resolve_streaming_credentials(&cfg, &half)
            .unwrap()
            .is_none());

        // The lease alone is enough to need credentials.
        cfg.lease.provider = StreamingLeaseProvider::ObjectStorage;
        assert!(resolve_streaming_credentials(&cfg, &half).is_err());
    }

    #[test]
    fn web_identity_lease_needs_a_real_region_test() {
        let irsa = |name: &str| match name {
            "AWS_WEB_IDENTITY_TOKEN_FILE" => Some("/var/run/token".to_string()),
            "AWS_ROLE_ARN" => Some("arn:aws:iam::1:role/legion-state".to_string()),
            _ => None,
        };
        let mut cfg = s3_streaming_config(StreamingLeaseProvider::ObjectStorage);
        assert_eq!(cfg.lease.object_storage.region, "auto");
        let err = resolve_streaming_credentials(&cfg, &irsa).unwrap_err();
        assert!(err.to_string().contains("region is 'auto'"), "{err}");

        cfg.lease.object_storage.region = "us-west-2".into();
        assert!(matches!(
            resolve_streaming_credentials(&cfg, &irsa).unwrap(),
            Some(Principal::WebIdentity { .. })
        ));

        // `auto` stays fine for keys (R2 wants exactly that).
        cfg.lease.object_storage.region = "auto".into();
        cfg.credentials = Some(crate::structs::StreamingCredentialsConfig {
            access_key_id: Some("AKIA".into()),
            secret_access_key: Some("s".into()),
            session_token: None,
        });
        assert!(matches!(
            resolve_streaming_credentials(&cfg, &irsa).unwrap(),
            Some(Principal::Explicit(_))
        ));
    }

    // Regression: the lease store was built without conditional puts, so on
    // real S3/R2/MinIO `S3Leaser::acquire_lease` got `NotImplemented` and
    // every manager stayed a standby forever. Only the in-memory store the
    // lease tests use honoured `PutMode::Create` without it.
    #[test]
    fn lease_store_sends_etag_preconditions_test() {
        let cfg = s3_streaming_config(StreamingLeaseProvider::ObjectStorage);
        let creds = crate::structs::StreamingCredentialsConfig {
            access_key_id: Some("AKIA".into()),
            secret_access_key: Some("s".into()),
            session_token: None,
        };
        let store = build_lease_store(&cfg.lease.object_storage, Some(&creds)).unwrap();
        let rendered = format!("{store:?}");
        assert!(rendered.contains("ETagMatch"), "{rendered}");
    }

    // The replica client is built on Legion's store, so rustyriver never holds
    // the env keys it parsed for itself.
    #[test]
    fn replica_client_uses_the_shared_credentials_test() {
        let creds = crate::structs::StreamingCredentialsConfig {
            access_key_id: Some("AKIACONFIG".into()),
            secret_access_key: Some("configsecret".into()),
            session_token: None,
        };
        let client = replica_object_store_client(
            "s3://state/prod?endpoint=http://minio.internal:9000",
            Some(&creds),
        )
        .unwrap();
        // The store is already built (Legion's, not one rustyriver would
        // build lazily from env keys), and the client's config holds no keys.
        assert!(format!("{client:?}").contains("initialized: true"));
        assert!(client.config().access_key_id.is_empty());
        assert!(client.config().secret_access_key.is_empty());
        assert_eq!(client.config().endpoint, "http://minio.internal:9000");
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
            credentials: None,
            interval: StdDuration::from_millis(200),
            store: cell,
            shutdown: Arc::new(AtomicBool::new(false)),
            replica_shutdown: Arc::new(AtomicBool::new(false)),
            replica_exited: ExitSlot::default(),
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

    // M3: the raft orderly stop shuts every lane's core down within its
    // budget, after which the node no longer accepts replicated writes.
    #[test]
    fn raft_shutdown_stops_the_lanes() {
        use crate::structs::KvValueType;
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::structs::ManagerConfigFull {
            store_path: dir.path().join("store").to_str().unwrap().to_string(),
            ..Default::default()
        };
        config.persistence.r#type = PersistenceType::Raft;
        config.persistence.raft.bootstrap = true;
        config.persistence.raft.node_id = "mgr-a".to_string();
        config.persistence.raft.data_dir = dir.path().join("raft").to_str().unwrap().to_string();

        let store = Store::start_storage(&config).unwrap();
        store
            .put_kv("before", "1", KvValueType::String, None, None)
            .expect("write before shutdown");

        let started = std::time::Instant::now();
        store.shutdown_persistence(StdDuration::from_secs(10));
        assert!(started.elapsed() < StdDuration::from_secs(10));
        assert!(
            store
                .put_kv("after", "1", KvValueType::String, None, None)
                .is_err(),
            "a shut-down raft node must not accept writes"
        );
    }

    // M3 regression: an orderly shutdown ships writes made since the last
    // sync cycle (the final sync), and returns promptly even with a long
    // sync interval instead of waiting out the sleep.
    #[tokio::test]
    async fn streaming_shutdown_runs_a_final_sync() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("store.sqlite");
        let replica_dir = tmp.path().join("replica");
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);
                 INSERT INTO events (name) VALUES ('alpha');",
            )
            .unwrap();
        }
        let cfg = StreamingPersistenceConfigFull {
            replica_url: replica_dir.to_string_lossy().into_owned(),
            // Long enough that only the first cycle and the final sync run.
            sync_interval: "1h".to_string(),
            ..StreamingPersistenceConfigFull::default()
        };
        let driver = StreamingPersistenceDriver::new(db_path.clone(), cfg);
        driver.start().unwrap();
        // Let the first cycle ship `alpha`, then commit after it.
        tokio::time::sleep(StdDuration::from_millis(500)).await;
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute("INSERT INTO events (name) VALUES (?1)", ["beta"])
                .unwrap();
        }

        let started = std::time::Instant::now();
        driver.shutdown(StdDuration::from_secs(10));
        assert!(
            started.elapsed() < StdDuration::from_secs(5),
            "shutdown waited out the sync interval: {:?}",
            started.elapsed()
        );

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
}
