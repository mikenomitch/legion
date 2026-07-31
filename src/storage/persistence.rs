use crate::prelude::*;
use crate::storage::raft::driver::MultiRaftDriver;
use crate::storage::s3_credentials::build_credential_provider;
use crate::storage::store::Store;
use crate::structs::{
    PersistenceConfigFull, PersistenceType, StreamingCredentialsConfigFull, StreamingLeaseProvider,
    StreamingObjectStorageLeaseConfigFull, StreamingPersistenceConfigFull,
};

use object_store::aws::AwsCredentialProvider;

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
        let credentials = self.cfg.credentials.clone();

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
                        run_replica_loop(db, replica_url, interval, credentials, shutdown).await
                    {
                        error!(error = ?err, "rustyriver replication loop exited");
                    }
                });
            })
            .context("failed to spawn rustyriver replication thread")?;

        info!(
            interval_secs = interval.as_secs_f64(),
            credentials_provider = self.cfg.credentials.provider.as_str(),
            "Started rustyriver streaming replication"
        );
        Ok(())
    }

    fn start_object_storage_lease_loop(&self) -> R<()> {
        let lease_cfg = self.cfg.lease.clone();
        let object_cfg = lease_cfg.object_storage.clone();
        let credentials = self.cfg.credentials.clone();
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
                        credentials,
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
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
    credentials: StreamingCredentialsConfigFull,
    shutdown: Arc<AtomicBool>,
) -> R<()> {
    if is_object_store_url(&replica_url) {
        let parsed = parse_replica_url_with_query(&replica_url)
            .with_context(|| format!("invalid streaming replica URL: {replica_url}"))?;
        let config = ObjectStoreConfig::from_url(&parsed)
            .with_context(|| format!("invalid streaming replica URL: {replica_url}"))?;
        let client = match build_credential_provider(&credentials)? {
            // `environment`: leave rustyriver to build its own store from the
            // URL, so configs written before this block behave identically.
            None => ObjectStoreClient::new(config),
            // Otherwise Legion owns the store, so the same credential provider
            // signs both the replica uploads and the lease CAS.
            Some(provider) => {
                let store = build_replica_store(&config, provider)?;
                ObjectStoreClient::with_store(config, store)
            }
        };
        replica_sync_loop(db, client, interval, shutdown).await
    } else {
        let client = FileReplicaClient::new(file_replica_path(&replica_url));
        replica_sync_loop(db, client, interval, shutdown).await
    }
}

/// Build the replica's S3 store with a Legion-owned credential provider.
///
/// This mirrors rustyriver's own private `ObjectStoreConfig::build_store`
/// (bucket, region default, path-style toggle, endpoint + `allow_http`) — it
/// has to, because the only public seam for injecting credentials is
/// `ObjectStoreClient::with_store`, which takes a fully built store. The
/// static keys in the config are deliberately *not* applied here: reaching
/// this function at all means an explicit provider outranks them.
fn build_replica_store(
    config: &ObjectStoreConfig,
    provider: AwsCredentialProvider,
) -> R<Arc<dyn ObjectStore>> {
    if config.bucket.is_empty() {
        bail!("streaming replica URL has no bucket");
    }
    let region = if config.region.is_empty() {
        "us-east-1".to_string()
    } else {
        config.region.clone()
    };

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(region)
        .with_virtual_hosted_style_request(!config.force_path_style)
        .with_credentials(provider);

    if !config.endpoint.is_empty() {
        let allow_http = config.endpoint.starts_with("http://")
            || config.skip_verify
            || rustyriver::replica_url::is_local_endpoint(&config.endpoint);
        builder = builder
            .with_endpoint(&config.endpoint)
            .with_allow_http(allow_http);
    }

    let store = builder
        .build()
        .context("failed to build S3 store for streaming replication")?;
    Ok(Arc::new(store))
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
///
/// Credential precedence, narrowest first: the lease's own static keys win for
/// the lease, then the shared `credentials` block, then whatever
/// `object_store` resolves itself. The narrow-wins order is what keeps every
/// pre-existing `lease.objectStorage` config working untouched;
/// [`StreamingPersistenceConfigFull::validate`] separately rejects the one
/// combination where the split is a bug rather than a preference (federated
/// replica, static-keyed lease).
fn build_lease_store(
    object_cfg: &StreamingObjectStorageLeaseConfigFull,
    credentials: &StreamingCredentialsConfigFull,
) -> R<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(object_cfg.bucket.clone())
        .with_region(object_cfg.region.clone());

    if let Some(endpoint) = &object_cfg.endpoint {
        builder = builder.with_endpoint(endpoint.clone());
    }

    let lease_has_static_keys =
        object_cfg.access_key_id.is_some() || object_cfg.secret_access_key.is_some();
    if lease_has_static_keys {
        if let Some(access_key) = &object_cfg.access_key_id {
            builder = builder.with_access_key_id(access_key.clone());
        }
        if let Some(secret) = &object_cfg.secret_access_key {
            builder = builder.with_secret_access_key(secret.clone());
        }
    } else if let Some(provider) = build_credential_provider(credentials)? {
        builder = builder.with_credentials(provider);
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
    credentials: StreamingCredentialsConfigFull,
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

    let store = build_lease_store(&object_cfg, &credentials)?;
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
