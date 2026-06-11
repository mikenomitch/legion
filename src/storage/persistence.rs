use crate::prelude::*;
use crate::structs::{
    PersistenceConfigFull, PersistenceType, RaftPersistenceConfigFull, StreamingLeaseProvider,
    StreamingObjectStorageLeaseConfigFull, StreamingPersistenceConfigFull,
};

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode, PutOptions};
use openraft::Config as OpenRaftConfig;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};
use time::{Duration as TimeDuration, OffsetDateTime};
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
            .unwrap_or_else(|_| StdDuration::from_secs(0))
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
    let ttl = StdDuration::from_secs(ttl_seconds.max(2));
    let renew = StdDuration::from_secs(renew_interval_seconds.max(1));
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
            .take(limit.clamp(1, 500))
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
}
