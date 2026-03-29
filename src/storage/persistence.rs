use crate::prelude::*;
use crate::structs::{
    PersistenceType, StreamingLeaseProvider, StreamingObjectStorageLeaseConfigFull,
    StreamingPersistenceConfigFull,
};

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::PutMode;
use object_store::PutOptions;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;

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

pub trait PersistenceDriver: Send + Sync + Debug {
    fn start(&self) -> R<()>;

    fn status(&self) -> PersistenceStatus;
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

#[derive(Debug)]
pub struct StreamingPersistenceDriver {
    db_path: std::path::PathBuf,
    cfg: StreamingPersistenceConfigFull,
    litestream_process: Arc<Mutex<Option<Child>>>,
    role_primary: Arc<AtomicBool>,
    lease_held: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseDocument {
    holder_id: String,
    lease_id: String,
    expires_at_unix_ms: u64,
}

impl StreamingPersistenceDriver {
    pub fn new(db_path: std::path::PathBuf, cfg: StreamingPersistenceConfigFull) -> Self {
        Self {
            db_path,
            cfg,
            litestream_process: Arc::new(Mutex::new(None)),
            role_primary: Arc::new(AtomicBool::new(false)),
            lease_held: Arc::new(AtomicBool::new(false)),
        }
    }

    fn now_unix_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_millis() as u64
    }

    fn host_identity() -> String {
        std::env::var("HOSTNAME").unwrap_or_else(|_| "legion-manager".to_string())
    }

    fn start_litestream(&self) -> R<()> {
        if self.cfg.replica_url.trim().is_empty() {
            info!("Streaming persistence enabled without replica URL; skipping litestream start");
            return Ok(());
        }

        let litestream_bin = self.cfg.litestream_binary.as_str();
        let db_path = self.db_path.to_string_lossy().to_string();
        let mut cmd = Command::new(litestream_bin);
        cmd.arg("replicate")
            .arg("-exec")
            .arg("sleep infinity")
            .arg("-url")
            .arg(self.cfg.replica_url.clone())
            .arg("-interval")
            .arg(self.cfg.sync_interval.clone())
            .arg(db_path);

        let child = cmd.spawn().with_context(|| {
            format!(
                "failed to spawn litestream process using binary '{}'",
                litestream_bin
            )
        })?;

        let mut guard = self
            .litestream_process
            .lock()
            .map_err(|_| anyhow!("failed to lock litestream process slot"))?;
        *guard = Some(child);
        info!(
            binary = litestream_bin,
            "Started litestream replication process"
        );
        Ok(())
    }

    fn start_object_storage_lease_loop(&self) -> R<()> {
        let lease_cfg = self.cfg.lease.clone();
        let key = lease_cfg.key.clone();
        let object_cfg = lease_cfg.object_storage.clone();
        let role_primary = Arc::clone(&self.role_primary);
        let lease_held = Arc::clone(&self.lease_held);
        let holder = Self::host_identity();

        let handle = Handle::current();
        std::thread::Builder::new()
            .name("persistence-object-lease".to_string())
            .spawn(move || {
                handle.block_on(async move {
                    if let Err(err) = object_storage_lease_loop(
                        object_cfg,
                        key,
                        holder,
                        lease_cfg.ttl_seconds,
                        lease_cfg.renew_interval_seconds,
                        role_primary,
                        lease_held,
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

impl PersistenceDriver for StreamingPersistenceDriver {
    fn start(&self) -> R<()> {
        self.start_litestream()?;

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

async fn object_storage_lease_loop(
    object_cfg: StreamingObjectStorageLeaseConfigFull,
    key: String,
    holder_id: String,
    ttl_seconds: u64,
    renew_interval_seconds: u64,
    role_primary: Arc<AtomicBool>,
    lease_held: Arc<AtomicBool>,
) -> R<()> {
    if object_cfg.bucket.trim().is_empty() {
        bail!("streaming.objectStorage lease requires bucket");
    }

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

    let store = builder.build()?;
    let object_path = ObjectPath::from(key.clone());
    let ttl = Duration::from_secs(ttl_seconds.max(2));
    let renew = Duration::from_secs(renew_interval_seconds.max(1));
    let lease_id = format!("{}-{}", holder_id, rand::random::<u64>());

    loop {
        let now = StreamingPersistenceDriver::now_unix_ms();
        let expires_at = now + ttl.as_millis() as u64;
        let doc = LeaseDocument {
            holder_id: holder_id.clone(),
            lease_id: lease_id.clone(),
            expires_at_unix_ms: expires_at,
        };
        let body = serde_json::to_vec(&doc)?;

        let existing = store.get(&object_path).await;
        match existing {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let remote_doc: LeaseDocument = serde_json::from_slice(&bytes)?;
                if remote_doc.holder_id == holder_id || remote_doc.expires_at_unix_ms <= now {
                    let opts = PutOptions {
                        mode: PutMode::Overwrite,
                        ..Default::default()
                    };
                    if let Err(err) = store
                        .put_opts(&object_path, body.clone().into(), opts)
                        .await
                    {
                        warn!(error = ?err, "Failed to update object-storage lease");
                        role_primary.store(false, Ordering::Relaxed);
                        lease_held.store(false, Ordering::Relaxed);
                    } else {
                        role_primary.store(true, Ordering::Relaxed);
                        lease_held.store(true, Ordering::Relaxed);
                    }
                } else {
                    role_primary.store(false, Ordering::Relaxed);
                    lease_held.store(false, Ordering::Relaxed);
                }
            }
            Err(_) => {
                let opts = PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                };
                if let Err(err) = store.put_opts(&object_path, body.into(), opts).await {
                    debug!(error = ?err, key = %key, "Lease create failed; another primary likely owns lease");
                    role_primary.store(false, Ordering::Relaxed);
                    lease_held.store(false, Ordering::Relaxed);
                } else {
                    role_primary.store(true, Ordering::Relaxed);
                    lease_held.store(true, Ordering::Relaxed);
                }
            }
        }

        tokio::time::sleep(renew).await;
    }
}

pub fn build_persistence_driver(
    config_type: &PersistenceType,
    db_path: std::path::PathBuf,
    streaming: StreamingPersistenceConfigFull,
) -> Arc<dyn PersistenceDriver> {
    match config_type {
        PersistenceType::LocalDisk | PersistenceType::Raft => {
            Arc::new(LocalDiskPersistenceDriver::new())
        }
        PersistenceType::Streaming => Arc::new(StreamingPersistenceDriver::new(db_path, streaming)),
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
}
