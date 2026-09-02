use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const DEF_SCHEDULER_AUTO_ANTI_AFFINITY_WEIGHT: f64 = 0.2;

use crate::prelude::{
    bail, Context, DEF_EVAL_PRIORITY, DEF_HEARTBEAT_HEADROOM, DEF_HEARTBEAT_MAX_INTERVAL_SEC,
    DEF_HEARTBEAT_MIN_INTERVAL_SEC, DEF_KV_DEBOUNCE_MS, DEF_LIVENESS_CHECKPOINT_INTERVAL_SEC,
    DEF_LIVENESS_DIGEST_INTERVAL_SEC, DEF_LIVENESS_FAILOVER_GRACE_SEC,
    DEF_LIVENESS_PEER_DIGEST_GRACE_SEC, DEF_MANAGER_HOSTNAME, DEF_MANAGER_INSECURE_PORT,
    DEF_MANAGER_SERVER_PORT, DEF_MISSED_BEATS_BEFORE_LOST, DEF_ROLLOUT_RETENTION_DAYS,
    DEF_SCHEDULING_POLICY, DEF_STORE_PATH, DEF_WORKER_CHECK_SEC, DEF_WORKER_CONFIG_PATH,
    MIN_LIVENESS_CHECKPOINT_INTERVAL_SEC, R,
};
use crate::structs::workload_identity::NodeIdentityConfig;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    pub port: Option<u64>,
    /// Deprecated: the insecure bootstrap listener was removed (join tokens
    /// are minted via the authenticated CreateJoinToken RPC). Parsed for
    /// config compatibility; nothing binds this port.
    pub insecure_port: Option<u64>,
    pub host: Option<String>,
    /// Externally reachable manager address embedded in join tokens
    /// (for example `http://10.0.0.5:4747`). Default: derived from
    /// host/port, with 0.0.0.0 mapped to 127.0.0.1.
    pub advertise_addr: Option<String>,
    /// TLS for the manager listener. Off unless `enabled` is true.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

/// TLS settings for the manager's gRPC listener.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    /// Master switch; certPath/keyPath are required when true.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// PEM-encoded server certificate path.
    pub cert_path: Option<String>,
    /// PEM-encoded private key path.
    pub key_path: Option<String>,
}

/// TlsConfig with defaults applied.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfigFull {
    pub enabled: bool,
    pub cert_path: String,
    pub key_path: String,
}

impl TlsConfig {
    pub fn with_defaults(&self) -> TlsConfigFull {
        TlsConfigFull {
            enabled: self.enabled.unwrap_or(false),
            cert_path: self.cert_path.clone().unwrap_or_default(),
            key_path: self.key_path.clone().unwrap_or_default(),
        }
    }
}

impl From<TlsConfigFull> for TlsConfig {
    fn from(val: TlsConfigFull) -> Self {
        TlsConfig {
            enabled: Some(val.enabled),
            cert_path: Some(val.cert_path),
            key_path: Some(val.key_path),
        }
    }
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
    /// Debounce window in milliseconds for KV-driven job reconciliation.
    /// Rapid puts/deletes to watched keys within the window are coalesced
    /// into a single update per job. `0` disables debouncing (every KV write
    /// reconciles synchronously). Defaults to [`DEF_KV_DEBOUNCE_MS`].
    pub kv_debounce_ms: Option<u64>,
    pub scheduler_settings: Option<Value>,
    pub task_resource_defaults: Option<Value>,
    pub custom_resources: Option<Value>,
    pub persistence: Option<PersistenceConfig>,
    #[serde(default)]
    pub workload_identity: Option<WorkloadIdentityConfig>,
    #[serde(default)]
    pub acl: Option<AclConfig>,
    pub artifacts: Option<ArtifactsManagerConfig>,
    /// Exec/actions settings (see docs/exec-and-actions.md).
    #[serde(default)]
    pub exec: Option<ExecConfig>,
    /// Telemetry listener settings (/metrics, /healthz, /readyz — see
    /// docs/metrics.md).
    #[serde(default)]
    pub telemetry: Option<TelemetryConfig>,
    /// Edge boot: provisional allocations and boot tickets (see
    /// docs/edge-boot.md). Off by default — enabling it opens a new
    /// authorization surface and must be an explicit operator decision.
    #[serde(default)]
    pub edge_boot: Option<EdgeBootConfig>,
    /// Heartbeat cadence and the lost-node window derived from it (see
    /// docs/heartbeat-cadence-and-scale.md). Omitting it reproduces the
    /// historical 2s cadence and 10s window exactly.
    #[serde(default)]
    pub heartbeat: Option<HeartbeatConfig>,
    /// Node liveness: the failover grace, the peer digest, and the display
    /// checkpoint (see docs/node-liveness.md).
    #[serde(default)]
    pub liveness: Option<LivenessConfig>,
}

/// Heartbeat cadence and node liveness windows
/// (docs/heartbeat-cadence-and-scale.md).
///
/// Omitting the whole block reproduces Legion's historical behavior exactly:
/// workers keep their worker-type `heartbeatSeconds`, nothing adapts, and
/// `missedBeatsBeforeLost: 5` x the 2s default is the historical fixed 10s
/// lost window.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatConfig {
    /// Aggregate heartbeat rate this control plane will accept, per second.
    ///
    /// When set, the manager computes each worker's interval from FLEET SIZE
    /// (`headroom x nodes / rate`) and serves it on the heartbeat reply, so
    /// cadence backs off as the fleet grows instead of healthy nodes being
    /// marked lost. Unset (the default) means workers keep their worker-type
    /// `heartbeatSeconds` and nothing adapts.
    ///
    /// Off by default on purpose: a system that quietly slows its own
    /// heartbeats to score better on a throughput benchmark is cheating.
    /// Measure yours with `legion node cadence`.
    #[serde(default)]
    pub target_rate_per_second: Option<f64>,
    /// Floor on the served interval, never crossed however small the fleet.
    /// Default `2s`.
    #[serde(default)]
    pub min_interval: Option<String>,
    /// Ceiling on the served interval. Where the manager gives up backing off
    /// and warns instead. Default `120s`.
    #[serde(default)]
    pub max_interval: Option<String>,
    /// Missed beats before a node is lost. The lost window is ALWAYS this
    /// times that node's own interval — `5 x 2s` is the historical 10s.
    /// Default 5.
    #[serde(default)]
    pub missed_beats_before_lost: Option<u32>,
    /// Headroom factor in the sizing formula. 2 means "run at half the
    /// measured ceiling so a burst is not an outage". Default 2.
    #[serde(default)]
    pub headroom: Option<f64>,
}

/// Resolved [`HeartbeatConfig`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatConfigFull {
    pub target_rate_per_second: Option<f64>,
    pub min_interval: String,
    pub max_interval: String,
    pub missed_beats_before_lost: u32,
    pub headroom: f64,
}

impl Default for HeartbeatConfigFull {
    fn default() -> Self {
        HeartbeatConfigFull {
            target_rate_per_second: None,
            min_interval: format!("{DEF_HEARTBEAT_MIN_INTERVAL_SEC}s"),
            max_interval: format!("{DEF_HEARTBEAT_MAX_INTERVAL_SEC}s"),
            missed_beats_before_lost: DEF_MISSED_BEATS_BEFORE_LOST,
            headroom: DEF_HEARTBEAT_HEADROOM,
        }
    }
}

impl HeartbeatConfig {
    pub fn with_defaults(&self) -> HeartbeatConfigFull {
        let defaults = HeartbeatConfigFull::default();
        HeartbeatConfigFull {
            target_rate_per_second: self.target_rate_per_second,
            min_interval: self.min_interval.clone().unwrap_or(defaults.min_interval),
            max_interval: self.max_interval.clone().unwrap_or(defaults.max_interval),
            missed_beats_before_lost: self
                .missed_beats_before_lost
                .unwrap_or(defaults.missed_beats_before_lost),
            headroom: self.headroom.unwrap_or(defaults.headroom),
        }
    }
}

impl From<HeartbeatConfigFull> for HeartbeatConfig {
    fn from(val: HeartbeatConfigFull) -> Self {
        HeartbeatConfig {
            target_rate_per_second: val.target_rate_per_second,
            min_interval: Some(val.min_interval),
            max_interval: Some(val.max_interval),
            missed_beats_before_lost: Some(val.missed_beats_before_lost),
            headroom: Some(val.headroom),
        }
    }
}

impl HeartbeatConfigFull {
    pub fn parsed_min_interval(&self) -> Duration {
        parse_humantime_or(
            &self.min_interval,
            &format!("{DEF_HEARTBEAT_MIN_INTERVAL_SEC}s"),
        )
    }

    pub fn parsed_max_interval(&self) -> Duration {
        parse_humantime_or(
            &self.max_interval,
            &format!("{DEF_HEARTBEAT_MAX_INTERVAL_SEC}s"),
        )
    }

    /// Whether the manager dictates cadence at all.
    pub fn adaptive_enabled(&self) -> bool {
        self.target_rate_per_second.is_some_and(|rate| rate > 0.0)
    }

    /// Reject configurations whose numbers cannot hold together, naming the
    /// offending values. Checked at manager startup, not discovered later as
    /// a fleet that will not converge.
    pub fn validate(&self) -> R<()> {
        if let Some(rate) = self.target_rate_per_second {
            if !(rate.is_finite() && rate > 0.0) {
                bail!(
                    "heartbeat.targetRatePerSecond must be a positive number (got {rate}); \
                     omit it entirely to disable adaptive cadence"
                );
            }
        }
        if self.missed_beats_before_lost == 0 {
            bail!(
                "heartbeat.missedBeatsBeforeLost must be at least 1: a node cannot be lost \
                 before it has missed a beat"
            );
        }
        if !(self.headroom.is_finite() && self.headroom >= 1.0) {
            bail!(
                "heartbeat.headroom must be at least 1.0 (got {}); 2 runs the control plane \
                 at half its measured ceiling",
                self.headroom
            );
        }
        let min = self.parsed_min_interval();
        let max = self.parsed_max_interval();
        if min.is_zero() {
            bail!(
                "heartbeat.minInterval must be greater than zero (got {})",
                self.min_interval
            );
        }
        if max < min {
            bail!(
                "heartbeat.maxInterval ({}) is below heartbeat.minInterval ({})",
                self.max_interval,
                self.min_interval
            );
        }
        Ok(())
    }
}

/// Node liveness: where heartbeat state lives and how it survives an election
/// (docs/node-liveness.md).
///
/// Heartbeats are tracked in the status-lane leader's MEMORY; only status
/// transitions are replicated. Every field is optional and omitting the block
/// reproduces the built-in defaults.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LivenessConfig {
    /// Accumulation window for the heartbeat group commit. `0ms` (the
    /// default) is commit-while-you-can batching. Once liveness is in leader
    /// memory a steady-state heartbeat does not commit at all, so this
    /// applies only to the checkpoint and to the mixed-version forwarding
    /// fallback.
    #[serde(default)]
    pub batch_window: Option<String>,
    /// After acquiring status-lane leadership (or starting the process), mark
    /// nothing lost for this long. Default `60s`. A new leader inherits no
    /// liveness state and rebuilds it from incoming heartbeats, so an
    /// election must not be a fleet-wide outage. Nomad uses 5m; Legion's lost
    /// window is 10s rather than 30s, so a sixth of that is enough.
    #[serde(default)]
    pub failover_grace: Option<String>,
    /// How often a manager that does NOT lead the status lane reports the
    /// nodes it heard from to the leader. ONE RPC per manager per interval,
    /// whatever the fleet size. Default `1s`.
    #[serde(default)]
    pub digest_interval: Option<String>,
    /// How long the leader keeps trusting a peer manager's last digest.
    /// Default `30s`. Nodes last claimed by a silent peer are not marked lost
    /// inside this window: a manager-to-manager problem must not present as
    /// fleet-wide node loss.
    #[serde(default)]
    pub peer_digest_grace: Option<String>,
    /// How often the leader replicates a batched heartbeat checkpoint so
    /// every manager can show an approximate "last heartbeat" time. Default
    /// `30s`. DISPLAY ONLY — a checkpoint is never read back as liveness,
    /// including on election. `0s` disables checkpoints entirely; non-leaders
    /// then show no heartbeat time rather than an approximate one.
    #[serde(default)]
    pub checkpoint_interval: Option<String>,
}

/// Resolved [`LivenessConfig`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LivenessConfigFull {
    pub batch_window: String,
    pub failover_grace: String,
    pub digest_interval: String,
    pub peer_digest_grace: String,
    pub checkpoint_interval: String,
}

impl Default for LivenessConfigFull {
    fn default() -> Self {
        LivenessConfigFull {
            batch_window: "0ms".into(),
            failover_grace: format!("{DEF_LIVENESS_FAILOVER_GRACE_SEC}s"),
            digest_interval: format!("{DEF_LIVENESS_DIGEST_INTERVAL_SEC}s"),
            peer_digest_grace: format!("{DEF_LIVENESS_PEER_DIGEST_GRACE_SEC}s"),
            checkpoint_interval: format!("{DEF_LIVENESS_CHECKPOINT_INTERVAL_SEC}s"),
        }
    }
}

impl LivenessConfig {
    pub fn with_defaults(&self) -> LivenessConfigFull {
        let defaults = LivenessConfigFull::default();
        LivenessConfigFull {
            batch_window: self.batch_window.clone().unwrap_or(defaults.batch_window),
            failover_grace: self
                .failover_grace
                .clone()
                .unwrap_or(defaults.failover_grace),
            digest_interval: self
                .digest_interval
                .clone()
                .unwrap_or(defaults.digest_interval),
            peer_digest_grace: self
                .peer_digest_grace
                .clone()
                .unwrap_or(defaults.peer_digest_grace),
            checkpoint_interval: self
                .checkpoint_interval
                .clone()
                .unwrap_or(defaults.checkpoint_interval),
        }
    }
}

impl From<LivenessConfigFull> for LivenessConfig {
    fn from(val: LivenessConfigFull) -> Self {
        LivenessConfig {
            batch_window: Some(val.batch_window),
            failover_grace: Some(val.failover_grace),
            digest_interval: Some(val.digest_interval),
            peer_digest_grace: Some(val.peer_digest_grace),
            checkpoint_interval: Some(val.checkpoint_interval),
        }
    }
}

impl LivenessConfigFull {
    pub fn parsed_batch_window(&self) -> Duration {
        parse_humantime_or(&self.batch_window, "0ms")
    }

    pub fn parsed_failover_grace(&self) -> Duration {
        parse_humantime_or(
            &self.failover_grace,
            &format!("{DEF_LIVENESS_FAILOVER_GRACE_SEC}s"),
        )
    }

    pub fn parsed_digest_interval(&self) -> Duration {
        parse_humantime_or(
            &self.digest_interval,
            &format!("{DEF_LIVENESS_DIGEST_INTERVAL_SEC}s"),
        )
    }

    pub fn parsed_peer_digest_grace(&self) -> Duration {
        parse_humantime_or(
            &self.peer_digest_grace,
            &format!("{DEF_LIVENESS_PEER_DIGEST_GRACE_SEC}s"),
        )
    }

    /// `None` when checkpoints are disabled (`checkpointInterval: "0s"`).
    pub fn parsed_checkpoint_interval(&self) -> Option<Duration> {
        let parsed = parse_humantime_or(
            &self.checkpoint_interval,
            &format!("{DEF_LIVENESS_CHECKPOINT_INTERVAL_SEC}s"),
        );
        (!parsed.is_zero()).then_some(parsed)
    }

    /// The runtime view of this block.
    pub fn settings(&self) -> crate::storage::liveness::LivenessSettings {
        crate::storage::liveness::LivenessSettings {
            failover_grace: self.parsed_failover_grace(),
            digest_interval: self.parsed_digest_interval(),
            peer_digest_grace: self.parsed_peer_digest_grace(),
            checkpoint_interval: self.parsed_checkpoint_interval(),
            batch_window: self.parsed_batch_window(),
        }
    }

    /// Reject combinations that cannot hold together, naming the values.
    ///
    /// `lost_window` is the smallest window any node in this cluster runs
    /// with, so the caller passes it in: a grace shorter than the window it is
    /// protecting is not a grace.
    pub fn validate(&self, lost_window: Duration) -> R<()> {
        let grace = self.parsed_failover_grace();
        if grace < lost_window {
            bail!(
                "liveness.failoverGrace ({}) is shorter than the lost-node window ({}s): a \
                 grace shorter than the window it protects gives the fleet no time to check \
                 in after an election",
                self.failover_grace,
                lost_window.as_secs()
            );
        }
        let digest = self.parsed_digest_interval();
        let peer_grace = self.parsed_peer_digest_grace();
        if digest.is_zero() {
            bail!(
                "liveness.digestInterval must be greater than zero (got {})",
                self.digest_interval
            );
        }
        if digest >= peer_grace {
            bail!(
                "liveness.digestInterval ({}) must be shorter than liveness.peerDigestGrace \
                 ({}): a peer must get several chances to report before it is considered quiet",
                self.digest_interval,
                self.peer_digest_grace
            );
        }
        if let Some(checkpoint) = self.parsed_checkpoint_interval() {
            if checkpoint < Duration::from_secs(MIN_LIVENESS_CHECKPOINT_INTERVAL_SEC) {
                bail!(
                    "liveness.checkpointInterval ({}) must be either 0s (disabled) or at least \
                     {MIN_LIVENESS_CHECKPOINT_INTERVAL_SEC}s: anything faster re-creates the \
                     per-heartbeat commit load that keeping liveness off the raft path removes",
                    self.checkpoint_interval
                );
            }
        }
        Ok(())
    }
}

/// Manager-side edge boot settings (docs/edge-boot.md).
///
/// `enabled: false` is the default and it is a real switch, not a formality:
/// with it off, `IssueBootTicket` and `RatifyClaim` are refused and no node
/// can start work on a capability. Turning it on grows a compromised node's
/// blast radius from "runs what it was given" to "runs anything a live ticket
/// names", which is why it is never on by accident.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EdgeBootConfig {
    /// Master switch for boot tickets and edge-initiated starts. Default
    /// false. Manager-side speculative dispatch (a job's own
    /// `speculativeStart` block) does NOT need this — it opens no new
    /// authorization surface.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Ticket lifetime when a mint does not ask for one. Default `5m`.
    #[serde(default)]
    pub default_ticket_ttl: Option<String>,
    /// Hard cap on a ticket's lifetime. Default `1h`. Revocation is by TTL
    /// only, so this is the real bound on how long a leaked ticket is
    /// dangerous.
    #[serde(default)]
    pub max_ticket_ttl: Option<String>,
    /// Cap on concurrent unratified claims per node. Default 32.
    #[serde(default)]
    pub max_provisional_per_node: Option<u32>,
}

/// Resolved [`EdgeBootConfig`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EdgeBootConfigFull {
    pub enabled: bool,
    pub default_ticket_ttl: String,
    pub max_ticket_ttl: String,
    pub max_provisional_per_node: u32,
}

impl Default for EdgeBootConfigFull {
    fn default() -> Self {
        EdgeBootConfigFull {
            enabled: false,
            default_ticket_ttl: DEF_BOOT_TICKET_TTL.into(),
            max_ticket_ttl: DEF_BOOT_TICKET_MAX_TTL.into(),
            max_provisional_per_node: DEF_MAX_PROVISIONAL_PER_NODE,
        }
    }
}

impl EdgeBootConfig {
    pub fn with_defaults(&self) -> EdgeBootConfigFull {
        let defaults = EdgeBootConfigFull::default();
        EdgeBootConfigFull {
            enabled: self.enabled.unwrap_or(defaults.enabled),
            default_ticket_ttl: self
                .default_ticket_ttl
                .clone()
                .unwrap_or(defaults.default_ticket_ttl),
            max_ticket_ttl: self
                .max_ticket_ttl
                .clone()
                .unwrap_or(defaults.max_ticket_ttl),
            max_provisional_per_node: self
                .max_provisional_per_node
                .unwrap_or(defaults.max_provisional_per_node),
        }
    }
}

impl From<EdgeBootConfigFull> for EdgeBootConfig {
    fn from(val: EdgeBootConfigFull) -> Self {
        EdgeBootConfig {
            enabled: Some(val.enabled),
            default_ticket_ttl: Some(val.default_ticket_ttl),
            max_ticket_ttl: Some(val.max_ticket_ttl),
            max_provisional_per_node: Some(val.max_provisional_per_node),
        }
    }
}

/// Default boot-ticket lifetime.
pub const DEF_BOOT_TICKET_TTL: &str = "5m";
/// Hard cap on boot-ticket lifetime.
pub const DEF_BOOT_TICKET_MAX_TTL: &str = "1h";
/// Default cap on concurrent unratified claims per node.
pub const DEF_MAX_PROVISIONAL_PER_NODE: u32 = 32;

/// Worker-side edge boot settings (docs/edge-boot.md).
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkerEdgeBootConfig {
    /// Whether this node serves `Worker.StartClaim` — that is, whether it
    /// will start work on a boot ticket at all. Default false, independently
    /// of the manager's switch: a node opts in to being an edge, and an
    /// operator who enables the feature cluster-wide still chooses which
    /// nodes accept claims.
    #[serde(default)]
    pub accept_claims: Option<bool>,
    /// How often the cached JWKS is proactively refreshed. Default `5m`.
    #[serde(default)]
    pub jwks_refresh: Option<String>,
    /// Tolerance for clock disagreement with the minting manager. Default
    /// `30s`.
    #[serde(default)]
    pub clock_skew: Option<String>,
}

/// Resolved [`WorkerEdgeBootConfig`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkerEdgeBootConfigFull {
    pub accept_claims: bool,
    pub jwks_refresh: String,
    pub clock_skew: String,
}

impl Default for WorkerEdgeBootConfigFull {
    fn default() -> Self {
        WorkerEdgeBootConfigFull {
            accept_claims: false,
            jwks_refresh: "5m".into(),
            clock_skew: "30s".into(),
        }
    }
}

impl WorkerEdgeBootConfig {
    pub fn with_defaults(&self) -> WorkerEdgeBootConfigFull {
        let defaults = WorkerEdgeBootConfigFull::default();
        WorkerEdgeBootConfigFull {
            accept_claims: self.accept_claims.unwrap_or(defaults.accept_claims),
            jwks_refresh: self.jwks_refresh.clone().unwrap_or(defaults.jwks_refresh),
            clock_skew: self.clock_skew.clone().unwrap_or(defaults.clock_skew),
        }
    }
}

/// Telemetry configuration: the manager's Prometheus metrics + health-probe
/// HTTP listener. Enabled by default; it serves only read-only operational
/// data (no secrets), but a hardened site can disable it or move the port.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryConfig {
    /// Master switch for the telemetry listener. Default: true.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Port the listener binds (on `server.host`). Default: `server.port + 2`
    /// (the exec WebSocket holds `+ 1`).
    pub port: Option<u64>,
}

/// Resolved telemetry configuration. `port: None` means "derive from
/// `server.port + 2` at spawn time" — the default is not baked in here
/// because the server port itself is configurable.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryConfigFull {
    pub enabled: bool,
    pub port: Option<u64>,
}

impl Default for TelemetryConfigFull {
    fn default() -> Self {
        TelemetryConfigFull {
            enabled: true,
            port: None,
        }
    }
}

impl TelemetryConfig {
    pub fn with_defaults(&self) -> TelemetryConfigFull {
        TelemetryConfigFull {
            enabled: self.enabled.unwrap_or(true),
            port: self.port,
        }
    }
}

impl From<TelemetryConfigFull> for TelemetryConfig {
    fn from(val: TelemetryConfigFull) -> Self {
        TelemetryConfig {
            enabled: Some(val.enabled),
            port: val.port,
        }
    }
}

/// Exec/actions configuration. Enabled by default (exec is still per-caller
/// default-deny once ACLs are on); a hardened site can disable it entirely.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ExecConfig {
    /// Master switch for `ExecAllocation` + the exec WebSocket bridge.
    /// Default: true.
    #[serde(default)]
    pub enabled: Option<bool>,
}

/// Resolved exec configuration.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExecConfigFull {
    pub enabled: bool,
}

impl Default for ExecConfigFull {
    fn default() -> Self {
        ExecConfigFull { enabled: true }
    }
}

impl ExecConfig {
    pub fn with_defaults(&self) -> ExecConfigFull {
        ExecConfigFull {
            enabled: self.enabled.unwrap_or(true),
        }
    }
}

/// Default TTL for tokens minted via Login (userpass or JWT exchange).
pub const DEF_ACL_LOGIN_TTL: &str = "24h";
/// Default refresh interval for auth-method JWKS documents.
pub const DEF_ACL_JWKS_REFRESH: &str = "5m";

/// ACL / auth configuration. Off by default: when `enabled` is false every
/// caller has full access and the UI shows a dismissable warning.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct AclConfig {
    /// Master switch. Default: false (no enforcement).
    #[serde(default)]
    pub enabled: Option<bool>,
    /// When true (and `enabled` is true), denials are logged with the
    /// `acl_audit` tracing target but requests are allowed. Use this to
    /// stage policies before flipping to full enforcement.
    #[serde(default)]
    pub audit_only: Option<bool>,
    /// TTL applied to tokens minted via Login when the request omits one.
    pub login_ttl: Option<String>,
    /// Upper bound on the TTL of client tokens minted via CreateAclToken and
    /// Login. When set, client tokens must expire and may not outlive this
    /// bound; management tokens are exempt. Default: unbounded.
    pub token_max_ttl: Option<String>,
    /// JWT/OIDC auth methods that convert external identities into policies.
    #[serde(default)]
    pub auth_methods: Option<Vec<AclAuthMethodConfig>>,
    /// How often to re-fetch remote JWKS documents.
    pub jwks_refresh_interval: Option<String>,
}

/// One JWT/OIDC auth method.
///
/// A method validates externally issued JWTs (signature via JWKS or a shared
/// secret, plus issuer/audience bounds) and maps verified claims onto Legion
/// ACL policies via binding rules.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct AclAuthMethodConfig {
    /// Unique method name, referenced by `legion login --method <name>`.
    /// `userpass` is reserved for the built-in username/password method.
    pub name: String,
    /// `jwt` (keys provided explicitly), `oidc` (keys discovered from the
    /// issuer's /.well-known/openid-configuration document), or `webhook`
    /// (credentials verified by an external HTTP endpoint).
    #[serde(default)]
    pub r#type: Option<String>,
    /// JWKS URL to fetch signing keys from (jwt type).
    pub jwks_url: Option<String>,
    /// Inline JWKS JSON document (jwt type; useful for air-gapped setups and tests).
    pub jwks_json: Option<String>,
    /// HMAC shared secret for HS256-signed tokens (dev/test setups).
    pub hmac_secret: Option<String>,
    /// OIDC issuer URL for discovery (oidc type) and `iss` validation.
    pub issuer: Option<String>,
    /// Accepted `aud` values. Strongly recommended; when empty, audience is
    /// not validated.
    #[serde(default)]
    pub bound_audiences: Option<Vec<String>>,
    /// Required `iss` value (defaults to `issuer` when set).
    pub bound_issuer: Option<String>,
    /// Claim used as the principal name. Default: `sub`.
    pub user_claim: Option<String>,
    /// Rules mapping verified claims to policy names.
    #[serde(default)]
    pub binding_rules: Option<Vec<AclBindingRule>>,
    /// TTL for ACL tokens minted when exchanging a JWT via Login.
    pub token_ttl: Option<String>,
    /// OAuth client ID registered for Legion at the identity provider
    /// (oidc type). Enables the browser/CLI redirect login flow.
    pub oidc_client_id: Option<String>,
    /// OAuth scopes requested during redirect login (oidc type).
    /// Default: ["openid"].
    #[serde(default)]
    pub oidc_scopes: Option<Vec<String>>,
    /// Verification endpoint for `webhook` methods. Legion POSTs the login
    /// credentials as JSON and expects `{"principal": "...", "policies":
    /// ["..."]}` on success (HTTP 200). 401/403 reject the credential.
    pub webhook_url: Option<String>,
    /// Optional bearer token Legion attaches when calling `webhookUrl`.
    pub webhook_bearer: Option<String>,
}

/// Maps a verified JWT claim onto Legion policies.
///
/// When `claim` is omitted the rule is unconditional (every login through the
/// method receives `policies`). When `claim` is set, the rule applies when the
/// claim equals `value` (string compare) or, for array claims, contains
/// `value`.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct AclBindingRule {
    /// JWT claim to inspect (for example "groups"). Omit for unconditional.
    pub claim: Option<String>,
    /// Value the claim must equal or contain.
    pub value: Option<String>,
    /// Policy names granted when the rule matches.
    pub policies: Vec<String>,
}

/// AclConfig with defaults applied.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AclConfigFull {
    pub enabled: bool,
    pub audit_only: bool,
    pub login_ttl: String,
    pub token_max_ttl: Option<String>,
    pub auth_methods: Vec<AclAuthMethodConfig>,
    pub jwks_refresh_interval: String,
}

impl Default for AclConfigFull {
    fn default() -> Self {
        AclConfigFull {
            enabled: false,
            audit_only: false,
            login_ttl: DEF_ACL_LOGIN_TTL.into(),
            token_max_ttl: None,
            auth_methods: Vec::new(),
            jwks_refresh_interval: DEF_ACL_JWKS_REFRESH.into(),
        }
    }
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

impl From<AclConfigFull> for AclConfig {
    fn from(val: AclConfigFull) -> Self {
        AclConfig {
            enabled: Some(val.enabled),
            audit_only: Some(val.audit_only),
            login_ttl: Some(val.login_ttl),
            token_max_ttl: val.token_max_ttl,
            auth_methods: Some(val.auth_methods),
            jwks_refresh_interval: Some(val.jwks_refresh_interval),
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

impl AclConfig {
    pub fn with_defaults(&self) -> AclConfigFull {
        let d = AclConfigFull::default();
        AclConfigFull {
            enabled: self.enabled.unwrap_or(d.enabled),
            audit_only: self.audit_only.unwrap_or(d.audit_only),
            login_ttl: self.login_ttl.clone().unwrap_or(d.login_ttl),
            token_max_ttl: self.token_max_ttl.clone(),
            auth_methods: self.auth_methods.clone().unwrap_or(d.auth_methods),
            jwks_refresh_interval: self
                .jwks_refresh_interval
                .clone()
                .unwrap_or(d.jwks_refresh_interval),
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
    /// Cluster-default `sub` template; per-identity `subTemplate` overrides it.
    pub sub_template: Option<String>,
    /// Identities issued to worker nodes rather than tasks (artifact system
    /// plan §6). Referenced by `auth.nodeIdentity` and cluster artifact-auth
    /// matchers.
    #[serde(default)]
    pub node_identities: Option<Vec<NodeIdentityConfig>>,
    /// Binding rules mapping a verified control-plane token's claims (e.g.
    /// `legion.io/job`) to ACL policy names, so a workload authenticating back
    /// to Legion with its reserved `legion` identity carries policies. Empty
    /// (the default) means control-plane tokens verify to a principal but grant
    /// no policies — the identity primitive only.
    #[serde(default)]
    pub control_plane_bindings: Option<Vec<AclBindingRule>>,
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
    pub sub_template: String,
    #[serde(default)]
    pub node_identities: Vec<NodeIdentityConfig>,
    #[serde(default)]
    pub control_plane_bindings: Vec<AclBindingRule>,
}

impl WorkloadIdentityConfigFull {
    /// Default token lifetime when an identity does not set its own `ttl`.
    pub fn parsed_default_ttl(&self) -> Duration {
        parse_humantime_or(&self.default_ttl, DEF_WORKLOAD_IDENTITY_TTL)
    }

    /// How old the active signing key may get before the rotation loop
    /// rotates it.
    pub fn parsed_rotation_age(&self) -> Duration {
        parse_humantime_or(&self.rotation_age, DEF_WORKLOAD_IDENTITY_ROTATION_AGE)
    }
}

fn parse_humantime_or(raw: &str, fallback: &str) -> Duration {
    raw.parse::<humantime::Duration>()
        .or_else(|_| fallback.parse::<humantime::Duration>())
        .map(Into::into)
        .unwrap_or(Duration::from_secs(900))
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
            sub_template: crate::structs::workload_identity::DEFAULT_SUB_TEMPLATE.into(),
            node_identities: Vec::new(),
            control_plane_bindings: Vec::new(),
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
            sub_template: Some(val.sub_template),
            node_identities: Some(val.node_identities),
            control_plane_bindings: Some(val.control_plane_bindings),
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
            sub_template: self.sub_template.clone().unwrap_or(d.sub_template),
            node_identities: self.node_identities.clone().unwrap_or(d.node_identities),
            control_plane_bindings: self
                .control_plane_bindings
                .clone()
                .unwrap_or(d.control_plane_bindings),
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
    /// Stable string identity of this manager inside the raft cluster.
    /// Defaults to the manager's `host:port` id. The numeric raft node id is
    /// derived from it deterministically (`fnv1a64(nodeId)`), so it must be
    /// unique per manager and stable across restarts.
    pub node_id: Option<String>,
    /// `true` on the *first* manager of a new cluster only: it initializes
    /// the raft group(s) with itself as the sole voter. Safe across restarts
    /// (re-initialization of an existing cluster is a swallowed no-op).
    pub bootstrap: Option<bool>,
    /// Peer managers as `"name=host:port"` entries (the address is the
    /// peer's manager gRPC listener, which also serves the raft transport).
    /// Bare `"name"` entries are still accepted for config compatibility;
    /// such peers can only be targeted by raft commands that do not need an
    /// address (their address is otherwise learned from replicated
    /// membership).
    pub peers: Option<Vec<String>>,
    /// Lifetime of minted `legion raft join-token` tokens, seconds.
    pub join_token_ttl_seconds: Option<u64>,
    /// Root directory for raft engine state: per-lane log databases
    /// (`<dataDir>/<lane>/log.sqlite`), the cluster identity file
    /// (`<dataDir>/cluster_secret`), and the topology stamp
    /// (`<dataDir>/topology.json`, JC-43). Default: `.legion/raft`.
    pub data_dir: Option<String>,
    /// Raft group topology: `"single"` (default — one group replicates every
    /// table) or `"lanes"` (three independent raft groups: control /
    /// scheduler / status, each electing its own leader and snapshotting
    /// its own tables). `"lanes"` is runnable since the single-writer
    /// table-ownership work landed (plan §7-C2/C-3): every table has
    /// exactly one writing lane, enforced at capture time, and each lane's
    /// snapshot covers only its owned tables. The topology is fixed at
    /// first engine start for the cluster's lifetime — there is no
    /// migration between topologies, and this is ENFORCED (plan JC-43):
    /// first start records `{topology, lanes}` in
    /// `<dataDir>/topology.json`, every later start must match, and a
    /// joiner presenting a different topology is refused at enrollment.
    /// Changing it means backup → empty raft data dir → restore into a
    /// fresh bootstrap.
    pub topology: Option<String>,
    /// `"host:port"` other managers dial to reach this manager's raft
    /// transport. Defaults to the manager server `host:port` (with
    /// `0.0.0.0`/`::` normalized to `127.0.0.1` — set this explicitly for
    /// any real multi-node deployment).
    pub advertise_addr: Option<String>,
    /// Shared secret authenticating manager-plane raft RPCs (plan JC-7).
    /// When absent it is minted at bootstrap and persisted to
    /// `<dataDir>/cluster_secret`; joining nodes receive it inside the join
    /// token.
    pub cluster_secret: Option<String>,
    /// Join token (from `legion raft join-token` on the leader) used by a
    /// new manager to join an existing cluster (phase A-2 wiring).
    pub join_token: Option<String>,
    /// The number of voting managers this cluster is SUPPOSED to have (1, 3,
    /// or 5). Optional and purely ergonomic: `legion raft status` warns when
    /// the observed voter count is below it, and config load warns when it
    /// is even ("2 voters need both alive to commit — this cluster survives
    /// zero failures"). `0` is rejected at load. Membership itself is driven
    /// by joins and operator commands, never by this knob.
    pub expected_voters: Option<u32>,
    /// Log entries between snapshots (compaction threshold, default 10000,
    /// applied PER RAFT GROUP). A snapshot is built every this-many applied
    /// entries and the raft log is purged behind it (a tail of
    /// `snapshotEntries / 2`, capped at 1000, is kept for cheap follower
    /// catch-up). `single` topology builds whole-DB `VACUUM INTO` images;
    /// `lanes` topology builds per-lane table-scoped dumps with the same
    /// policy per lane.
    pub snapshot_entries: Option<u64>,
    /// Explicit override for re-stamping a state DB onto a NEW raft cluster
    /// identity. The state DB is stamped with its cluster id at first engine
    /// start; a later start whose derived cluster id differs (e.g. the raft
    /// dataDir — cluster secret included — was wiped and `bootstrap=true`
    /// minted a fresh one) is refused, because bootstrapping a new cluster
    /// over an existing cluster's data split-brains against any surviving
    /// members. Set `true` only when the old cluster is fully retired.
    pub force_new_cluster: Option<bool>,
    /// How often a leader sends heartbeats (append-entries) to every
    /// follower, milliseconds. Default
    /// [`DEF_RAFT_HEARTBEAT_INTERVAL_MS`].
    ///
    /// This is the clock the whole failover story is built on: a follower
    /// only learns the leader is alive by receiving one, so the heartbeat
    /// interval sets the FLOOR on how fast a dead leader can be noticed,
    /// and — because openraft reuses it as the append-entries RPC deadline
    /// — it also bounds how long a single replication batch may take. A
    /// batch that cannot be encoded, shipped and acked inside the interval
    /// is reported as an unreachable peer, so the interval and
    /// `MAX_PAYLOAD_ENTRIES` (see `storage::raft::driver`) are a matched
    /// pair.
    ///
    /// **Lower** = faster failure detection, more constant RPC traffic and
    /// less tolerance for a slow round trip. **Higher** = quieter,
    /// steadier clusters that take proportionally longer to notice a dead
    /// leader.
    ///
    /// Raise this (together with the election window below) when heartbeats
    /// cannot reliably complete a round trip inside the default:
    /// CPU-starved or heavily loaded managers, WAN / cross-region links,
    /// virtualised or otherwise noisy hosts where the scheduler can stall a
    /// process for hundreds of milliseconds, and CI runners — especially
    /// in-process multi-node test harnesses on few cores in a debug build,
    /// where several raft nodes contend for the same CPU and a missed
    /// heartbeat is a scheduling artifact rather than a real failure.
    pub heartbeat_interval_ms: Option<u64>,
    /// Lower bound of the randomized election timeout, milliseconds.
    /// Default [`DEF_RAFT_ELECTION_TIMEOUT_MIN_MS`].
    ///
    /// A follower that has heard nothing from a leader for a random
    /// duration drawn from `[electionTimeoutMinMs, electionTimeoutMaxMs)`
    /// campaigns to replace it. The *minimum* is the stability knob: it
    /// must sit comfortably above `heartbeatIntervalMs` so that a couple of
    /// merely slow or dropped heartbeats never look like a dead leader. Set
    /// it too close to the heartbeat interval and the cluster FLAPS —
    /// followers time out under transient load, campaign, split the vote,
    /// and the term climbs while no leader ever holds power long enough to
    /// commit anything. Config load rejects that arrangement outright (see
    /// `storage::raft::quorum::validate_raft_config`).
    ///
    /// The randomized *width* between min and max is what breaks up split
    /// votes: candidates wake at different times, so one reliably gets its
    /// ballot in first. Keep a real gap between the two.
    ///
    /// Raise this in the same situations as `heartbeatIntervalMs` — loaded
    /// managers, WAN links, virtualised or noisy hosts, and CI — and keep
    /// the ratio to the heartbeat interval intact when you do.
    pub election_timeout_min_ms: Option<u64>,
    /// Upper bound of the randomized election timeout, milliseconds.
    /// Default [`DEF_RAFT_ELECTION_TIMEOUT_MAX_MS`]. Must be strictly
    /// greater than `electionTimeoutMinMs`.
    ///
    /// Together with the minimum this bounds worst-case failover: a dead
    /// leader is replaced within roughly one election timeout plus a
    /// round trip. It is ALSO the follower *leader-lease* duration — a
    /// follower will not grant its vote to a new candidate while it still
    /// believes the current leader's lease is live. That makes this knob
    /// cut both ways: a larger maximum buys stability under load, but it
    /// slows every DELIBERATE leadership move (`trigger_lane_election`,
    /// the daemon's lane-convergence loop) by the same proportion, since
    /// those must wait out the incumbent's lease. Prefer widening the
    /// window over inflating it: keep the maximum a small multiple of the
    /// minimum (the default is 2x) rather than pushing it to seconds.
    ///
    /// Raise it, with the two knobs above, for loaded managers, WAN links,
    /// virtualised or noisy hosts, and CI — and expect lane convergence and
    /// deliberate failovers to take correspondingly longer.
    pub election_timeout_max_ms: Option<u64>,
}

impl Default for RaftPersistenceConfig {
    fn default() -> Self {
        Self {
            node_id: None,
            bootstrap: Some(false),
            peers: Some(Vec::new()),
            join_token_ttl_seconds: Some(900),
            data_dir: None,
            topology: None,
            advertise_addr: None,
            cluster_secret: None,
            join_token: None,
            expected_voters: None,
            snapshot_entries: None,
            force_new_cluster: Some(false),
            heartbeat_interval_ms: None,
            election_timeout_min_ms: None,
            election_timeout_max_ms: None,
        }
    }
}

/// Default root directory for raft engine state (logs + cluster identity).
pub const DEF_RAFT_DATA_DIR: &str = ".legion/raft";
/// Default raft topology: one group carries every lane (plan JC-2).
pub const DEF_RAFT_TOPOLOGY: &str = "single";
/// Default snapshot/compaction threshold: applied log entries between
/// whole-DB snapshots in `single` topology (plan §A5).
pub const DEF_RAFT_SNAPSHOT_ENTRIES: u64 = 10_000;

/// Default leader heartbeat interval (`raft.heartbeatIntervalMs`).
///
/// 250 ms gives a worst-case `MAX_PAYLOAD_ENTRIES` batch ~2.5x headroom in
/// a debug build, since openraft reuses the heartbeat interval as the
/// append-entries deadline. See the field docs on
/// [`RaftPersistenceConfig::heartbeat_interval_ms`].
pub const DEF_RAFT_HEARTBEAT_INTERVAL_MS: u64 = 250;
/// Default lower bound of the randomized election timeout
/// (`raft.electionTimeoutMinMs`).
///
/// The election window is kept as TIGHT as the heartbeat interval allows
/// (600-1200 ms, ~2.5-5x the heartbeat) rather than scaled up
/// proportionally, because openraft derives the follower *leader lease*
/// from `election_timeout_max`: a live leader refreshes the lease on every
/// heartbeat, so a larger max makes every deliberate leadership move take
/// proportionally longer to win. Failover still completes in ~1s, in the
/// range of Consul's and etcd's defaults and far below any
/// operator-visible timeout.
pub const DEF_RAFT_ELECTION_TIMEOUT_MIN_MS: u64 = 600;
/// Default upper bound of the randomized election timeout
/// (`raft.electionTimeoutMaxMs`). See
/// [`DEF_RAFT_ELECTION_TIMEOUT_MIN_MS`].
pub const DEF_RAFT_ELECTION_TIMEOUT_MAX_MS: u64 = 1200;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfigFull {
    pub port: u64,
    pub insecure_port: u64,
    pub host: String,
    pub advertise_addr: Option<String>,
    #[serde(default)]
    pub tls: TlsConfigFull,
}

impl ServerConfigFull {
    /// The address embedded in join tokens: `advertiseAddr` when set,
    /// otherwise derived from scheme/host/port (0.0.0.0 -> 127.0.0.1).
    pub fn advertised_address(&self) -> String {
        if let Some(addr) = &self.advertise_addr {
            return addr.clone();
        }
        let host = match self.host.as_str() {
            "0.0.0.0" | "::" => "127.0.0.1",
            other => other,
        };
        let scheme = if self.tls.enabled { "https" } else { "http" };
        format!("{scheme}://{host}:{}", self.port)
    }
}

impl From<ServerConfigFull> for ServerConfig {
    fn from(val: ServerConfigFull) -> Self {
        ServerConfig {
            port: Some(val.port),
            insecure_port: Some(val.insecure_port),
            host: Some(val.host),
            advertise_addr: val.advertise_addr,
            tls: Some(val.tls.into()),
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
            advertise_addr: self.advertise_addr.clone(),
            tls: self
                .tls
                .as_ref()
                .map(TlsConfig::with_defaults)
                .unwrap_or_default(),
        }
    }

    pub fn full_default() -> ServerConfigFull {
        ServerConfigFull {
            port: DEF_MANAGER_SERVER_PORT,
            insecure_port: DEF_MANAGER_INSECURE_PORT,
            host: DEF_MANAGER_HOSTNAME.to_string(),
            advertise_addr: None,
            tls: TlsConfigFull::default(),
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
    #[serde(default = "default_kv_debounce_ms")]
    pub kv_debounce_ms: u64,
    pub scheduler_settings: Option<Value>,
    pub task_resource_defaults: Option<Value>,
    pub custom_resources: Option<Value>,
    #[serde(default)]
    pub persistence: PersistenceConfigFull,
    #[serde(default)]
    pub workload_identity: WorkloadIdentityConfigFull,
    #[serde(default)]
    pub acl: AclConfigFull,
    pub artifacts: ArtifactsManagerConfigFull,
    #[serde(default)]
    pub exec: ExecConfigFull,
    #[serde(default)]
    pub telemetry: TelemetryConfigFull,
    #[serde(default)]
    pub edge_boot: EdgeBootConfigFull,
    #[serde(default)]
    pub heartbeat: HeartbeatConfigFull,
    #[serde(default)]
    pub liveness: LivenessConfigFull,
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
    pub replica_url: Option<String>,
    pub sync_interval: Option<String>,
    pub lease: Option<StreamingLeaseConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StreamingPersistenceConfigFull {
    pub replica_url: String,
    pub sync_interval: String,
    pub lease: StreamingLeaseConfigFull,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RaftPersistenceConfigFull {
    pub node_id: String,
    pub bootstrap: bool,
    pub peers: Vec<String>,
    pub join_token_ttl_seconds: u64,
    pub data_dir: String,
    pub topology: String,
    pub advertise_addr: Option<String>,
    pub cluster_secret: Option<String>,
    pub join_token: Option<String>,
    /// Expected voting-manager count (ergonomic warnings only); stays
    /// optional in the Full config — unset means "no expectation declared".
    pub expected_voters: Option<u32>,
    pub snapshot_entries: u64,
    pub force_new_cluster: bool,
    /// Leader heartbeat interval, ms. See
    /// [`RaftPersistenceConfig::heartbeat_interval_ms`].
    pub heartbeat_interval_ms: u64,
    /// Randomized election timeout, lower bound, ms. See
    /// [`RaftPersistenceConfig::election_timeout_min_ms`].
    pub election_timeout_min_ms: u64,
    /// Randomized election timeout, upper bound, ms. See
    /// [`RaftPersistenceConfig::election_timeout_max_ms`].
    pub election_timeout_max_ms: u64,
}

impl Default for RaftPersistenceConfigFull {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            bootstrap: false,
            peers: Vec::new(),
            join_token_ttl_seconds: 900,
            data_dir: DEF_RAFT_DATA_DIR.to_string(),
            topology: DEF_RAFT_TOPOLOGY.to_string(),
            advertise_addr: None,
            cluster_secret: None,
            join_token: None,
            expected_voters: None,
            snapshot_entries: DEF_RAFT_SNAPSHOT_ENTRIES,
            force_new_cluster: false,
            heartbeat_interval_ms: DEF_RAFT_HEARTBEAT_INTERVAL_MS,
            election_timeout_min_ms: DEF_RAFT_ELECTION_TIMEOUT_MIN_MS,
            election_timeout_max_ms: DEF_RAFT_ELECTION_TIMEOUT_MAX_MS,
        }
    }
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
            kv_debounce_ms: Some(val.kv_debounce_ms),
            scheduler_settings: val.scheduler_settings,
            task_resource_defaults: val.task_resource_defaults,
            custom_resources: val.custom_resources,
            persistence: Some(val.persistence.into()),
            workload_identity: Some(val.workload_identity.into()),
            acl: Some(val.acl.into()),
            artifacts: Some(val.artifacts.into()),
            exec: Some(ExecConfig {
                enabled: Some(val.exec.enabled),
            }),
            telemetry: Some(val.telemetry.into()),
            edge_boot: Some(val.edge_boot.into()),
            heartbeat: Some(val.heartbeat.into()),
            liveness: Some(val.liveness.into()),
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
            kv_debounce_ms: self.kv_debounce_ms.unwrap_or(DEF_KV_DEBOUNCE_MS),
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
            acl: self
                .acl
                .as_ref()
                .map(AclConfig::with_defaults)
                .unwrap_or_default(),
            artifacts: self
                .artifacts
                .as_ref()
                .map(ArtifactsManagerConfig::with_defaults)
                .unwrap_or_default(),
            exec: self
                .exec
                .as_ref()
                .map(ExecConfig::with_defaults)
                .unwrap_or_default(),
            telemetry: self
                .telemetry
                .as_ref()
                .map(TelemetryConfig::with_defaults)
                .unwrap_or_default(),
            edge_boot: self
                .edge_boot
                .as_ref()
                .map(EdgeBootConfig::with_defaults)
                .unwrap_or_default(),
            heartbeat: self
                .heartbeat
                .as_ref()
                .map(HeartbeatConfig::with_defaults)
                .unwrap_or_default(),
            liveness: self
                .liveness
                .as_ref()
                .map(LivenessConfig::with_defaults)
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
            kv_debounce_ms: DEF_KV_DEBOUNCE_MS,
            scheduler_settings: None,
            task_resource_defaults: None,
            custom_resources: None,
            eval_threads: default_eval_threads(),
            persistence: PersistenceConfigFull::default(),
            workload_identity: WorkloadIdentityConfigFull::default(),
            acl: AclConfigFull::default(),
            artifacts: ArtifactsManagerConfigFull::default(),
            exec: ExecConfigFull::default(),
            telemetry: TelemetryConfigFull::default(),
            edge_boot: EdgeBootConfigFull::default(),
            heartbeat: HeartbeatConfigFull::default(),
            liveness: LivenessConfigFull::default(),
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
            replica_url: String::new(),
            sync_interval: "1s".to_string(),
            lease: StreamingLeaseConfigFull::default(),
        }
    }
}

impl StreamingPersistenceConfig {
    pub fn with_defaults(&self) -> StreamingPersistenceConfigFull {
        StreamingPersistenceConfigFull {
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
            data_dir: self
                .data_dir
                .clone()
                .unwrap_or_else(|| DEF_RAFT_DATA_DIR.to_string()),
            topology: self
                .topology
                .clone()
                .unwrap_or_else(|| DEF_RAFT_TOPOLOGY.to_string()),
            advertise_addr: self.advertise_addr.clone(),
            cluster_secret: self.cluster_secret.clone(),
            join_token: self.join_token.clone(),
            expected_voters: self.expected_voters,
            snapshot_entries: self.snapshot_entries.unwrap_or(DEF_RAFT_SNAPSHOT_ENTRIES),
            force_new_cluster: self.force_new_cluster.unwrap_or(false),
            heartbeat_interval_ms: self
                .heartbeat_interval_ms
                .unwrap_or(DEF_RAFT_HEARTBEAT_INTERVAL_MS),
            election_timeout_min_ms: self
                .election_timeout_min_ms
                .unwrap_or(DEF_RAFT_ELECTION_TIMEOUT_MIN_MS),
            election_timeout_max_ms: self
                .election_timeout_max_ms
                .unwrap_or(DEF_RAFT_ELECTION_TIMEOUT_MAX_MS),
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
            data_dir: Some(value.data_dir),
            topology: Some(value.topology),
            advertise_addr: value.advertise_addr,
            cluster_secret: value.cluster_secret,
            join_token: value.join_token,
            expected_voters: value.expected_voters,
            snapshot_entries: Some(value.snapshot_entries),
            force_new_cluster: Some(value.force_new_cluster),
            heartbeat_interval_ms: Some(value.heartbeat_interval_ms),
            election_timeout_min_ms: Some(value.election_timeout_min_ms),
            election_timeout_max_ms: Some(value.election_timeout_max_ms),
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

fn default_kv_debounce_ms() -> u64 {
    DEF_KV_DEBOUNCE_MS
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
    /// Host paths this worker offers to jobs through the `host` volume driver
    /// (see docs/volumes.md), keyed by the name jobs claim.
    #[serde(default)]
    pub host_volumes: Option<HashMap<String, crate::volumes::HostVolumeConfig>>,
    /// Heartbeat cadence in seconds (default 2, clamped by workers to
    /// 1..=30). The manager's lost-node window is 10s, so values above ~3
    /// trade detection margin for manager load; the manager warns when
    /// loading such a config (heartbeat-scale plan §5).
    #[serde(default)]
    pub heartbeat_seconds: Option<u32>,
    /// Whether this worker turns a task's declared `resources` into real
    /// kernel limits for host processes (see docs/task-supervision.md). Off
    /// unless declared.
    #[serde(default)]
    pub resource_enforcement: Option<WorkerResourceEnforcementConfig>,
    /// Whether this node accepts edge-initiated starts, and how it verifies
    /// the tickets that authorize them (see docs/edge-boot.md). Absent = off.
    #[serde(default)]
    pub edge_boot: Option<WorkerEdgeBootConfig>,
}

/// Worker `resourceEnforcement` block: cgroup v2 limits for `exec` tasks.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkerResourceEnforcementConfig {
    /// Enforce `resources` as cgroup limits. Default false — a declaration
    /// that was only ever a scheduling estimate should not start OOM-killing
    /// workloads because the cluster upgraded.
    #[serde(default)]
    pub enabled: bool,
    /// cgroup v2 path (relative to `/sys/fs/cgroup`) under which this worker
    /// creates one subtree per allocation. Must already exist, with `+memory`
    /// and `+cpu` in its `cgroup.subtree_control`.
    #[serde(default = "default_cgroup_parent")]
    pub cgroup_parent: String,
}

fn default_cgroup_parent() -> String {
    "legion".to_string()
}

impl Default for WorkerResourceEnforcementConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cgroup_parent: default_cgroup_parent(),
        }
    }
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
    fn edge_boot_is_off_by_default_on_both_planes() {
        // The promise the public doc makes three times over: a cluster that
        // upgrades and changes nothing grows no new authorization surface.
        let manager = ManagerConfigFull::default().edge_boot;
        assert!(!manager.enabled);
        assert_eq!(manager.default_ticket_ttl, DEF_BOOT_TICKET_TTL);
        assert_eq!(manager.max_ticket_ttl, DEF_BOOT_TICKET_MAX_TTL);
        assert_eq!(
            manager.max_provisional_per_node,
            DEF_MAX_PROVISIONAL_PER_NODE
        );

        let worker = WorkerEdgeBootConfigFull::default();
        assert!(!worker.accept_claims);

        // An absent block resolves to the same thing as an empty one.
        assert_eq!(EdgeBootConfig::default().with_defaults(), manager);
        assert_eq!(WorkerEdgeBootConfig::default().with_defaults(), worker);
    }

    #[test]
    fn edge_boot_overrides_survive_the_round_trip_to_the_wire_form() {
        let full = EdgeBootConfig {
            enabled: Some(true),
            default_ticket_ttl: Some("30s".into()),
            max_ticket_ttl: Some("10m".into()),
            max_provisional_per_node: Some(4),
        }
        .with_defaults();
        assert!(full.enabled);
        assert_eq!(full.max_provisional_per_node, 4);
        // `ManagerConfigFull -> ManagerConfig` is what `GetManagerConfigs`
        // serves, so a setting that vanished there would be invisible to an
        // operator checking what the cluster actually thinks it is running.
        let round_tripped: EdgeBootConfig = full.clone().into();
        assert_eq!(round_tripped.with_defaults(), full);
    }

    #[test]
    fn a_worker_edge_boot_block_parses_from_its_json_wire_form() -> R<()> {
        // The block rides to the worker as a JSON string on `WorkerConfig`.
        let parsed: WorkerEdgeBootConfigFull =
            serde_json::from_str(r#"{"acceptClaims":true,"jwksRefresh":"1m","clockSkew":"5s"}"#)?;
        assert!(parsed.accept_claims);
        assert_eq!(parsed.jwks_refresh, "1m");
        assert_eq!(parsed.clock_skew, "5s");
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
