use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const DEF_SCHEDULER_AUTO_ANTI_AFFINITY_WEIGHT: f64 = 0.2;

use crate::prelude::{
    bail, Context, DEF_EVAL_PRIORITY, DEF_MANAGER_HOSTNAME, DEF_MANAGER_INSECURE_PORT,
    DEF_MANAGER_SERVER_PORT, DEF_ROLLOUT_RETENTION_DAYS, DEF_SCHEDULING_POLICY, DEF_STORE_PATH,
    DEF_WORKER_CHECK_SEC, DEF_WORKER_CONFIG_PATH, R,
};
use crate::structs::workload_identity::NodeIdentityConfig;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    pub port: Option<u64>,
    pub insecure_port: Option<u64>,
    pub host: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ManagerConfig {
    pub server: Option<ServerConfig>,
    pub store_path: Option<String>,
    pub worker_config_path: Option<String>,
    pub worker_check_seconds: Option<u64>,
    pub eval_threads: Option<usize>,
    pub scheduling_policy: Option<String>,
    pub default_priority: Option<i64>,
    pub rollout_retention_days: Option<u64>,
    pub scheduler_settings: Option<Value>,
    pub task_resource_defaults: Option<Value>,
    pub custom_resources: Option<Value>,
    pub persistence: Option<PersistenceConfig>,
    #[serde(default)]
    pub workload_identity: Option<WorkloadIdentityConfig>,
    #[serde(default)]
    pub artifacts: Option<ArtifactsManagerConfig>,
}

/// Default artifact scheme allowlist. Plain `http` requires explicit opt-in.
pub const DEF_ARTIFACT_ALLOWED_SCHEMES: &[&str] = &["https", "s3", "oci", "git", "file"];

/// One cluster-level artifact auth matcher: routes sources matching a glob to
/// a KV credential or a node identity. First match wins. Cluster entries may
/// never attach a *task* identity — task identities are a per-task security
/// boundary (artifact plan judgement call 6); node identity exists precisely
/// to be the cluster-grain credential.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactAuthMatcher {
    /// Source glob: `scheme://host/path` segments where `*` matches within a
    /// path segment and `**` crosses segments (for example
    /// `oci://ghcr.io/acme/**`).
    pub r#match: String,
    /// KV path holding the credential JSON for matching sources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv: Option<String>,
    /// Node identity name (from `workloadIdentity.nodeIdentities`) used for
    /// matching sources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_identity: Option<String>,
}

impl ArtifactAuthMatcher {
    pub fn validate(&self) -> R<()> {
        if self.r#match.trim().is_empty() {
            bail!("artifacts.auth matcher requires a non-empty match pattern");
        }
        match (self.kv.as_deref(), self.node_identity.as_deref()) {
            (Some(kv), None) if !kv.trim().is_empty() => Ok(()),
            (None, Some(node_identity)) if !node_identity.trim().is_empty() => Ok(()),
            (Some(_), Some(_)) => bail!(
                "artifacts.auth matcher {:?} must set exactly one of kv or nodeIdentity",
                self.r#match
            ),
            _ => bail!(
                "artifacts.auth matcher {:?} must set one of kv or nodeIdentity (task \
                 identities are per-task and cannot be attached cluster-wide)",
                self.r#match
            ),
        }
    }
}

/// Cluster-level artifact settings (manager config `artifacts` block).
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactsManagerConfig {
    /// Source schemes jobs may use. Defaults to
    /// [`DEF_ARTIFACT_ALLOWED_SCHEMES`] (everything except plain `http`).
    pub allowed_schemes: Option<Vec<String>>,
    /// Cluster auth matchers applied when a task artifact carries no `auth`.
    pub auth: Option<Vec<ArtifactAuthMatcher>>,
    /// Whether container `image` fields desugar into Legion-managed `oci://`
    /// artifacts. Off by default until the artifact cache lands (without a
    /// cache this would re-download images per allocation and regress
    /// runtimes that cache images themselves).
    pub container_image_desugar: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactsManagerConfigFull {
    pub allowed_schemes: Vec<String>,
    pub auth: Vec<ArtifactAuthMatcher>,
    pub container_image_desugar: bool,
}

impl Default for ArtifactsManagerConfigFull {
    fn default() -> Self {
        ArtifactsManagerConfigFull {
            allowed_schemes: DEF_ARTIFACT_ALLOWED_SCHEMES
                .iter()
                .map(|s| s.to_string())
                .collect(),
            auth: Vec::new(),
            container_image_desugar: false,
        }
    }
}

impl ArtifactsManagerConfig {
    pub fn with_defaults(&self) -> ArtifactsManagerConfigFull {
        let d = ArtifactsManagerConfigFull::default();
        ArtifactsManagerConfigFull {
            allowed_schemes: self.allowed_schemes.clone().unwrap_or(d.allowed_schemes),
            auth: self.auth.clone().unwrap_or(d.auth),
            container_image_desugar: self
                .container_image_desugar
                .unwrap_or(d.container_image_desugar),
        }
    }
}

impl From<ArtifactsManagerConfigFull> for ArtifactsManagerConfig {
    fn from(val: ArtifactsManagerConfigFull) -> Self {
        ArtifactsManagerConfig {
            allowed_schemes: Some(val.allowed_schemes),
            auth: Some(val.auth),
            container_image_desugar: Some(val.container_image_desugar),
        }
    }
}

/// Default token TTL when an identity does not set its own.
pub const DEF_WORKLOAD_IDENTITY_TTL: &str = "15m";
/// Default signing-key rotation age.
pub const DEF_WORKLOAD_IDENTITY_ROTATION_AGE: &str = "24h";
/// Default JWKS route relative to the issuer URL.
pub const DEF_WORKLOAD_IDENTITY_JWKS_PATH: &str = "/.well-known/jwks.json";

/// Manager OIDC issuer / workload-identity configuration. Off by default; the
/// OIDC HTTP listener only binds when `enabled` is true (see the plan rollout).
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadIdentityConfig {
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Public HTTPS issuer URL relying parties trust (must match discovery doc).
    pub issuer: Option<String>,
    pub jwks_path: Option<String>,
    pub signing_alg: Option<String>,
    pub default_ttl: Option<String>,
    pub rotation_age: Option<String>,
    pub default_audience: Option<Vec<String>>,
    /// Cluster-default `sub` template; per-identity `subTemplate` overrides it.
    pub sub_template: Option<String>,
    /// Identities issued to worker nodes rather than tasks (artifact system
    /// plan §6). Referenced by `auth.nodeIdentity` and cluster artifact-auth
    /// matchers.
    #[serde(default)]
    pub node_identities: Option<Vec<NodeIdentityConfig>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadIdentityConfigFull {
    pub enabled: bool,
    pub issuer: Option<String>,
    pub jwks_path: String,
    pub signing_alg: String,
    pub default_ttl: String,
    pub rotation_age: String,
    pub default_audience: Vec<String>,
    pub sub_template: String,
    #[serde(default)]
    pub node_identities: Vec<NodeIdentityConfig>,
}

impl Default for WorkloadIdentityConfigFull {
    fn default() -> Self {
        WorkloadIdentityConfigFull {
            enabled: false,
            issuer: None,
            jwks_path: DEF_WORKLOAD_IDENTITY_JWKS_PATH.into(),
            signing_alg: "ES256".into(),
            default_ttl: DEF_WORKLOAD_IDENTITY_TTL.into(),
            rotation_age: DEF_WORKLOAD_IDENTITY_ROTATION_AGE.into(),
            default_audience: Vec::new(),
            sub_template: crate::structs::workload_identity::DEFAULT_SUB_TEMPLATE.into(),
            node_identities: Vec::new(),
        }
    }
}

impl From<WorkloadIdentityConfigFull> for WorkloadIdentityConfig {
    fn from(val: WorkloadIdentityConfigFull) -> Self {
        WorkloadIdentityConfig {
            enabled: Some(val.enabled),
            issuer: val.issuer,
            jwks_path: Some(val.jwks_path),
            signing_alg: Some(val.signing_alg),
            default_ttl: Some(val.default_ttl),
            rotation_age: Some(val.rotation_age),
            default_audience: Some(val.default_audience),
            sub_template: Some(val.sub_template),
            node_identities: Some(val.node_identities),
        }
    }
}

impl WorkloadIdentityConfig {
    pub fn with_defaults(&self) -> WorkloadIdentityConfigFull {
        let d = WorkloadIdentityConfigFull::default();
        WorkloadIdentityConfigFull {
            enabled: self.enabled.unwrap_or(d.enabled),
            issuer: self.issuer.clone(),
            jwks_path: self.jwks_path.clone().unwrap_or(d.jwks_path),
            signing_alg: self.signing_alg.clone().unwrap_or(d.signing_alg),
            default_ttl: self.default_ttl.clone().unwrap_or(d.default_ttl),
            rotation_age: self.rotation_age.clone().unwrap_or(d.rotation_age),
            default_audience: self.default_audience.clone().unwrap_or(d.default_audience),
            sub_template: self.sub_template.clone().unwrap_or(d.sub_template),
            node_identities: self.node_identities.clone().unwrap_or(d.node_identities),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceType {
    #[default]
    LocalDisk,
    Streaming,
    Raft,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct PersistenceConfig {
    #[serde(default)]
    pub r#type: PersistenceType,
    pub streaming: Option<StreamingPersistenceConfig>,
    pub raft: Option<RaftPersistenceConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RaftPersistenceConfig {
    pub node_id: Option<String>,
    pub bootstrap: Option<bool>,
    pub peers: Option<Vec<String>>,
    pub join_token_ttl_seconds: Option<u64>,
}

impl Default for RaftPersistenceConfig {
    fn default() -> Self {
        Self {
            node_id: None,
            bootstrap: Some(false),
            peers: Some(Vec::new()),
            join_token_ttl_seconds: Some(900),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfigFull {
    pub port: u64,
    pub insecure_port: u64,
    pub host: String,
}

impl From<ServerConfigFull> for ServerConfig {
    fn from(val: ServerConfigFull) -> Self {
        ServerConfig {
            port: Some(val.port),
            insecure_port: Some(val.insecure_port),
            host: Some(val.host),
        }
    }
}

impl ServerConfig {
    pub fn with_defaults(&self) -> ServerConfigFull {
        ServerConfigFull {
            port: self.port.unwrap_or(DEF_MANAGER_SERVER_PORT),
            insecure_port: self.insecure_port.unwrap_or(DEF_MANAGER_INSECURE_PORT),
            host: self
                .host
                .clone()
                .unwrap_or_else(|| DEF_MANAGER_HOSTNAME.to_string()),
        }
    }

    pub fn full_default() -> ServerConfigFull {
        ServerConfigFull {
            port: DEF_MANAGER_SERVER_PORT,
            insecure_port: DEF_MANAGER_INSECURE_PORT,
            host: DEF_MANAGER_HOSTNAME.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ManagerConfigFull {
    pub server: ServerConfigFull,
    pub store_path: String,
    pub worker_config_path: String,
    pub worker_check_seconds: u64,
    #[serde(default = "default_eval_threads")]
    pub eval_threads: usize,
    pub scheduling_policy: String,
    pub default_priority: i64,
    pub rollout_retention_days: u64,
    pub scheduler_settings: Option<Value>,
    pub task_resource_defaults: Option<Value>,
    pub custom_resources: Option<Value>,
    #[serde(default)]
    pub persistence: PersistenceConfigFull,
    #[serde(default)]
    pub workload_identity: WorkloadIdentityConfigFull,
    #[serde(default)]
    pub artifacts: ArtifactsManagerConfigFull,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PersistenceConfigFull {
    pub r#type: PersistenceType,
    pub streaming: StreamingPersistenceConfigFull,
    pub raft: RaftPersistenceConfigFull,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct StreamingPersistenceConfig {
    pub litestream_binary: Option<String>,
    pub replica_url: Option<String>,
    pub sync_interval: Option<String>,
    pub lease: Option<StreamingLeaseConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StreamingPersistenceConfigFull {
    pub litestream_binary: String,
    pub replica_url: String,
    pub sync_interval: String,
    pub lease: StreamingLeaseConfigFull,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct RaftPersistenceConfigFull {
    pub node_id: String,
    pub bootstrap: bool,
    pub peers: Vec<String>,
    pub join_token_ttl_seconds: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum StreamingLeaseProvider {
    #[default]
    None,
    ObjectStorage,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct StreamingLeaseConfig {
    #[serde(default)]
    pub provider: StreamingLeaseProvider,
    pub key: Option<String>,
    pub ttl_seconds: Option<u64>,
    pub renew_interval_seconds: Option<u64>,
    pub object_storage: Option<StreamingObjectStorageLeaseConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StreamingLeaseConfigFull {
    pub provider: StreamingLeaseProvider,
    pub key: String,
    pub ttl_seconds: u64,
    pub renew_interval_seconds: u64,
    pub object_storage: StreamingObjectStorageLeaseConfigFull,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct StreamingObjectStorageLeaseConfig {
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StreamingObjectStorageLeaseConfigFull {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

impl From<ManagerConfigFull> for ManagerConfig {
    fn from(val: ManagerConfigFull) -> Self {
        ManagerConfig {
            server: Some(val.server.into()),
            store_path: Some(val.store_path),
            worker_config_path: Some(val.worker_config_path),
            worker_check_seconds: Some(val.worker_check_seconds),
            eval_threads: Some(val.eval_threads),
            scheduling_policy: Some(val.scheduling_policy),
            default_priority: Some(val.default_priority),
            rollout_retention_days: Some(val.rollout_retention_days),
            scheduler_settings: val.scheduler_settings,
            task_resource_defaults: val.task_resource_defaults,
            custom_resources: val.custom_resources,
            persistence: Some(val.persistence.into()),
            workload_identity: Some(val.workload_identity.into()),
            artifacts: Some(val.artifacts.into()),
        }
    }
}

impl ManagerConfig {
    pub fn with_defaults(&self) -> ManagerConfigFull {
        let server_config = self
            .server
            .as_ref()
            .map(ServerConfig::with_defaults)
            .unwrap_or_else(ServerConfig::full_default);

        ManagerConfigFull {
            server: server_config,
            store_path: self
                .store_path
                .clone()
                .unwrap_or_else(|| DEF_STORE_PATH.into()),
            worker_config_path: self
                .worker_config_path
                .clone()
                .unwrap_or_else(|| DEF_WORKER_CONFIG_PATH.into()),
            worker_check_seconds: self.worker_check_seconds.unwrap_or(DEF_WORKER_CHECK_SEC),
            eval_threads: self.eval_threads.unwrap_or_else(default_eval_threads),
            scheduling_policy: self
                .scheduling_policy
                .clone()
                .unwrap_or_else(|| DEF_SCHEDULING_POLICY.into()),
            default_priority: self.default_priority.unwrap_or(DEF_EVAL_PRIORITY),
            rollout_retention_days: self
                .rollout_retention_days
                .unwrap_or(DEF_ROLLOUT_RETENTION_DAYS),
            scheduler_settings: self.scheduler_settings.clone(),
            task_resource_defaults: self.task_resource_defaults.clone(),
            custom_resources: self.custom_resources.clone(),
            persistence: self
                .persistence
                .as_ref()
                .map(PersistenceConfig::with_defaults)
                .unwrap_or_default(),
            workload_identity: self
                .workload_identity
                .as_ref()
                .map(WorkloadIdentityConfig::with_defaults)
                .unwrap_or_default(),
            artifacts: self
                .artifacts
                .as_ref()
                .map(ArtifactsManagerConfig::with_defaults)
                .unwrap_or_default(),
        }
    }
}

impl Default for ManagerConfigFull {
    fn default() -> Self {
        ManagerConfigFull {
            server: ServerConfig::full_default(),
            store_path: DEF_STORE_PATH.into(),
            worker_config_path: DEF_WORKER_CONFIG_PATH.into(),
            worker_check_seconds: DEF_WORKER_CHECK_SEC,
            scheduling_policy: DEF_SCHEDULING_POLICY.into(),
            default_priority: DEF_EVAL_PRIORITY,
            rollout_retention_days: DEF_ROLLOUT_RETENTION_DAYS,
            scheduler_settings: None,
            task_resource_defaults: None,
            custom_resources: None,
            eval_threads: default_eval_threads(),
            persistence: PersistenceConfigFull::default(),
            workload_identity: WorkloadIdentityConfigFull::default(),
            artifacts: ArtifactsManagerConfigFull::default(),
        }
    }
}

impl Default for PersistenceConfigFull {
    fn default() -> Self {
        Self {
            r#type: PersistenceType::LocalDisk,
            streaming: StreamingPersistenceConfigFull::default(),
            raft: RaftPersistenceConfigFull::default(),
        }
    }
}

impl PersistenceConfig {
    pub fn with_defaults(&self) -> PersistenceConfigFull {
        PersistenceConfigFull {
            r#type: self.r#type.clone(),
            streaming: self
                .streaming
                .as_ref()
                .map(StreamingPersistenceConfig::with_defaults)
                .unwrap_or_default(),
            raft: self
                .raft
                .as_ref()
                .map(RaftPersistenceConfig::with_defaults)
                .unwrap_or_default(),
        }
    }
}

impl Default for StreamingPersistenceConfigFull {
    fn default() -> Self {
        Self {
            litestream_binary: "litestream".to_string(),
            replica_url: String::new(),
            sync_interval: "1s".to_string(),
            lease: StreamingLeaseConfigFull::default(),
        }
    }
}

impl StreamingPersistenceConfig {
    pub fn with_defaults(&self) -> StreamingPersistenceConfigFull {
        StreamingPersistenceConfigFull {
            litestream_binary: self
                .litestream_binary
                .clone()
                .unwrap_or_else(|| "litestream".to_string()),
            replica_url: self.replica_url.clone().unwrap_or_default(),
            sync_interval: self
                .sync_interval
                .clone()
                .unwrap_or_else(|| "1s".to_string()),
            lease: self
                .lease
                .as_ref()
                .map(StreamingLeaseConfig::with_defaults)
                .unwrap_or_default(),
        }
    }
}

impl RaftPersistenceConfig {
    pub fn with_defaults(&self) -> RaftPersistenceConfigFull {
        RaftPersistenceConfigFull {
            node_id: self.node_id.clone().unwrap_or_default(),
            bootstrap: self.bootstrap.unwrap_or(false),
            peers: self.peers.clone().unwrap_or_default(),
            join_token_ttl_seconds: self.join_token_ttl_seconds.unwrap_or(900),
        }
    }
}

impl Default for StreamingLeaseConfigFull {
    fn default() -> Self {
        Self {
            provider: StreamingLeaseProvider::None,
            key: "leases/default/primary.json".to_string(),
            ttl_seconds: 15,
            renew_interval_seconds: 5,
            object_storage: StreamingObjectStorageLeaseConfigFull::default(),
        }
    }
}

impl StreamingLeaseConfig {
    pub fn with_defaults(&self) -> StreamingLeaseConfigFull {
        StreamingLeaseConfigFull {
            provider: self.provider.clone(),
            key: self
                .key
                .clone()
                .unwrap_or_else(|| "leases/default/primary.json".to_string()),
            ttl_seconds: self.ttl_seconds.unwrap_or(15),
            renew_interval_seconds: self.renew_interval_seconds.unwrap_or(5),
            object_storage: self
                .object_storage
                .as_ref()
                .map(StreamingObjectStorageLeaseConfig::with_defaults)
                .unwrap_or_default(),
        }
    }

    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.ttl_seconds.unwrap_or(15))
    }
}

impl Default for StreamingObjectStorageLeaseConfigFull {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            region: "auto".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        }
    }
}

impl StreamingObjectStorageLeaseConfig {
    pub fn with_defaults(&self) -> StreamingObjectStorageLeaseConfigFull {
        StreamingObjectStorageLeaseConfigFull {
            bucket: self.bucket.clone().unwrap_or_default(),
            region: self.region.clone().unwrap_or_else(|| "auto".to_string()),
            endpoint: self.endpoint.clone(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
        }
    }
}

impl From<PersistenceConfigFull> for PersistenceConfig {
    fn from(value: PersistenceConfigFull) -> Self {
        Self {
            r#type: value.r#type,
            streaming: Some(value.streaming.into()),
            raft: Some(value.raft.into()),
        }
    }
}

impl From<StreamingPersistenceConfigFull> for StreamingPersistenceConfig {
    fn from(value: StreamingPersistenceConfigFull) -> Self {
        Self {
            litestream_binary: Some(value.litestream_binary),
            replica_url: Some(value.replica_url),
            sync_interval: Some(value.sync_interval),
            lease: Some(value.lease.into()),
        }
    }
}

impl From<RaftPersistenceConfigFull> for RaftPersistenceConfig {
    fn from(value: RaftPersistenceConfigFull) -> Self {
        Self {
            node_id: Some(value.node_id),
            bootstrap: Some(value.bootstrap),
            peers: Some(value.peers),
            join_token_ttl_seconds: Some(value.join_token_ttl_seconds),
        }
    }
}

impl From<StreamingLeaseConfigFull> for StreamingLeaseConfig {
    fn from(value: StreamingLeaseConfigFull) -> Self {
        Self {
            provider: value.provider,
            key: Some(value.key),
            ttl_seconds: Some(value.ttl_seconds),
            renew_interval_seconds: Some(value.renew_interval_seconds),
            object_storage: Some(value.object_storage.into()),
        }
    }
}

impl From<StreamingObjectStorageLeaseConfigFull> for StreamingObjectStorageLeaseConfig {
    fn from(value: StreamingObjectStorageLeaseConfigFull) -> Self {
        Self {
            bucket: Some(value.bucket),
            region: Some(value.region),
            endpoint: value.endpoint,
            access_key_id: value.access_key_id,
            secret_access_key: value.secret_access_key,
        }
    }
}

fn default_eval_threads() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct WorkerConfigAlt {
    pub worker_type: String,
    pub additional_node_checks: Option<Vec<String>>,
    pub pool: Option<String>,
    pub attributes: Option<HashMap<String, String>>,
    pub name_prefix: Option<String>,
    pub resources: Option<Value>,
    pub reserved_resources: Option<Value>,
    pub max_allocations: Option<u32>,
    #[serde(default)]
    pub artifacts: Option<WorkerArtifactsConfig>,
}

/// Default concurrent artifact fetches per worker.
pub const DEF_ARTIFACT_FETCH_PARALLELISM: usize = 4;
/// Default per-artifact fetch attempts before the allocation fails.
pub const DEF_ARTIFACT_FETCH_ATTEMPTS: u32 = 3;
/// Default overall fetch deadline per artifact.
pub const DEF_ARTIFACT_FETCH_TIMEOUT: &str = "10m";
/// Default per-artifact size cap (also bounds decompression).
pub const DEF_ARTIFACT_MAX_SIZE: &str = "50GiB";

/// Worker-level artifact settings (worker config `artifacts` block). The
/// cache plan later adds a sibling `cache` block under the same key.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct WorkerArtifactsConfig {
    pub fetch: Option<WorkerArtifactFetchConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct WorkerArtifactFetchConfig {
    /// Concurrent fetches per worker.
    pub parallelism: Option<usize>,
    /// Per-artifact retries before the allocation fails.
    pub attempts: Option<u32>,
    /// Overall fetch deadline per artifact (humantime, e.g. "10m").
    pub timeout: Option<String>,
    /// Per-artifact size cap (e.g. "50GiB"); also bounds decompression.
    pub max_artifact_size: Option<String>,
    /// Worker-side scheme allowlist (defense in depth alongside admission).
    pub allowed_schemes: Option<Vec<String>>,
}

/// Resolved worker fetch settings with defaults applied.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkerArtifactFetchConfigFull {
    pub parallelism: usize,
    pub attempts: u32,
    pub timeout_secs: u64,
    pub max_artifact_size_bytes: u64,
    pub allowed_schemes: Vec<String>,
}

impl Default for WorkerArtifactFetchConfigFull {
    fn default() -> Self {
        WorkerArtifactFetchConfigFull {
            parallelism: DEF_ARTIFACT_FETCH_PARALLELISM,
            attempts: DEF_ARTIFACT_FETCH_ATTEMPTS,
            timeout_secs: 600,
            max_artifact_size_bytes: 50 * 1024 * 1024 * 1024,
            allowed_schemes: DEF_ARTIFACT_ALLOWED_SCHEMES
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }
    }
}

impl WorkerArtifactsConfig {
    pub fn fetch_with_defaults(&self) -> R<WorkerArtifactFetchConfigFull> {
        let d = WorkerArtifactFetchConfigFull::default();
        let Some(fetch) = self.fetch.as_ref() else {
            return Ok(d);
        };
        let timeout_secs = match fetch.timeout.as_deref() {
            None => d.timeout_secs,
            Some(raw) => raw
                .parse::<humantime::Duration>()
                .with_context(|| format!("invalid artifacts.fetch.timeout {raw:?}"))?
                .as_secs(),
        };
        let max_artifact_size_bytes = match fetch.max_artifact_size.as_deref() {
            None => d.max_artifact_size_bytes,
            Some(raw) => parse_byte_size(raw)
                .with_context(|| format!("invalid artifacts.fetch.maxArtifactSize {raw:?}"))?,
        };
        if fetch.parallelism == Some(0) {
            bail!("artifacts.fetch.parallelism must be at least 1");
        }
        if fetch.attempts == Some(0) {
            bail!("artifacts.fetch.attempts must be at least 1");
        }
        Ok(WorkerArtifactFetchConfigFull {
            parallelism: fetch.parallelism.unwrap_or(d.parallelism),
            attempts: fetch.attempts.unwrap_or(d.attempts),
            timeout_secs,
            max_artifact_size_bytes,
            allowed_schemes: fetch.allowed_schemes.clone().unwrap_or(d.allowed_schemes),
        })
    }
}

/// Parses human byte sizes like "512MiB", "50GiB", "10MB", or plain bytes.
/// Decimal (KB/MB/GB/TB) and binary (KiB/MiB/GiB/TiB) suffixes supported.
pub fn parse_byte_size(raw: &str) -> R<u64> {
    let trimmed = raw.trim();
    let split = trimmed
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(trimmed.len());
    let (number, suffix) = trimmed.split_at(split);
    let value: f64 = number
        .parse()
        .with_context(|| format!("invalid byte size {raw:?}"))?;
    let multiplier: u64 = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" => 1000,
        "mb" => 1000 * 1000,
        "gb" => 1000 * 1000 * 1000,
        "tb" => 1000 * 1000 * 1000 * 1000,
        "kib" | "k" => 1 << 10,
        "mib" | "m" => 1 << 20,
        "gib" | "g" => 1 << 30,
        "tib" | "t" => 1 << 40,
        other => bail!("invalid byte size suffix {other:?} in {raw:?}"),
    };
    if value < 0.0 {
        bail!("byte size {raw:?} cannot be negative");
    }
    Ok((value * multiplier as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_sizes_parse_decimal_and_binary_suffixes() -> R<()> {
        assert_eq!(parse_byte_size("1024")?, 1024);
        assert_eq!(parse_byte_size("10MB")?, 10_000_000);
        assert_eq!(parse_byte_size("512MiB")?, 512 * 1024 * 1024);
        assert_eq!(parse_byte_size("50GiB")?, 50 * 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("1.5KiB")?, 1536);
        assert!(parse_byte_size("10XB").is_err());
        assert!(parse_byte_size("abc").is_err());
        Ok(())
    }

    #[test]
    fn worker_artifact_fetch_defaults_and_overrides() -> R<()> {
        let defaults = WorkerArtifactsConfig::default().fetch_with_defaults()?;
        assert_eq!(defaults.parallelism, DEF_ARTIFACT_FETCH_PARALLELISM);
        assert_eq!(defaults.attempts, DEF_ARTIFACT_FETCH_ATTEMPTS);
        assert_eq!(defaults.timeout_secs, 600);
        assert!(!defaults.allowed_schemes.contains(&"http".to_string()));

        let config: WorkerArtifactsConfig = serde_json::from_value(serde_json::json!({
            "fetch": {
                "parallelism": 8,
                "attempts": 5,
                "timeout": "2m",
                "maxArtifactSize": "1GiB",
                "allowedSchemes": ["https", "http"],
            }
        }))?;
        let full = config.fetch_with_defaults()?;
        assert_eq!(full.parallelism, 8);
        assert_eq!(full.attempts, 5);
        assert_eq!(full.timeout_secs, 120);
        assert_eq!(full.max_artifact_size_bytes, 1024 * 1024 * 1024);
        assert!(full.allowed_schemes.contains(&"http".to_string()));

        let zero: WorkerArtifactsConfig =
            serde_json::from_value(serde_json::json!({"fetch": {"parallelism": 0}}))?;
        assert!(zero.fetch_with_defaults().is_err());
        Ok(())
    }

    #[test]
    fn artifact_auth_matcher_one_of_enforced() {
        let both = ArtifactAuthMatcher {
            r#match: "oci://x/**".into(),
            kv: Some("secrets/x".into()),
            node_identity: Some("pull".into()),
        };
        assert!(both.validate().is_err());

        let neither = ArtifactAuthMatcher {
            r#match: "oci://x/**".into(),
            kv: None,
            node_identity: None,
        };
        assert!(neither.validate().is_err());

        let kv_only = ArtifactAuthMatcher {
            r#match: "oci://x/**".into(),
            kv: Some("secrets/x".into()),
            node_identity: None,
        };
        kv_only.validate().expect("kv matcher valid");
    }

    #[test]
    fn manager_artifacts_defaults_exclude_plain_http() {
        let full = ArtifactsManagerConfigFull::default();
        assert!(!full.allowed_schemes.contains(&"http".to_string()));
        for scheme in ["https", "s3", "oci", "git", "file"] {
            assert!(
                full.allowed_schemes.contains(&scheme.to_string()),
                "{scheme}"
            );
        }
        assert!(
            !full.container_image_desugar,
            "desugar off until cache plan"
        );
    }
}
