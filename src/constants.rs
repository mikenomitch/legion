use std::path::PathBuf;

// === Protocol ===
// The worker<->manager wire-protocol version, exchanged during worker
// registration (`WorkerRegister`). Bump this ONLY for breaking wire-contract
// changes to worker-plane RPCs (registration, heartbeats, health checks,
// work dispatch): removed/renamed fields, changed field semantics, or new
// required behavior an old peer cannot satisfy. Additive, backwards-compatible
// fields do NOT bump this. Managers accept workers at or below their own
// version (workers reporting 0 are pre-versioning builds and are accepted
// with a warning); they reject newer workers so operators upgrade managers
// before workers. See docs/upgrades.md for the full skew policy.
//
// Version 3: the worker serves the mTLS worker plane (its own node
// certificate from a CSR, manager-only RPCs need a manager certificate) and
// advertises a reachable `https://` workerAddress. Managers never dial a
// plaintext (`http://`) worker address, so exec, logs, and restart against a
// protocol-2 worker fail with an error naming its version until it is
// upgraded; dispatch still reaches it through heartbeat replies.
pub const PROTOCOL_VERSION: u32 = 3;

// === Worker → task-runtime environment ===
// Task runtimes are in-process dylibs with no config channel of their own, so
// cross-cutting worker settings reach them as environment variables set by the
// worker daemon at startup.
/// Set to `1` when the worker enforces `resources` as cgroup limits.
pub const ENV_RESOURCE_ENFORCEMENT: &str = "LEGION_RESOURCE_ENFORCEMENT";
/// cgroup v2 parent path (relative to /sys/fs/cgroup) for worker-owned cgroups.
pub const ENV_CGROUP_PARENT: &str = "LEGION_CGROUP_PARENT";
/// Extra memory, in MiB, a `sandbox` allocation's cgroup allows on top of
/// its guest size, for the VMM process itself (worker `sandbox.vmmOverheadMiB`).
pub const ENV_SANDBOX_VMM_OVERHEAD_MIB: &str = "LEGION_SANDBOX_VMM_OVERHEAD_MIB";
/// Default for [`ENV_SANDBOX_VMM_OVERHEAD_MIB`].
pub const DEF_SANDBOX_VMM_OVERHEAD_MIB: u64 = 64;
/// The capabilities `container` tasks on this node may end up with (worker
/// type `container.allowCaps`), comma-separated, or `ALL`. Unset means the
/// default set (docs/task-runtime-container.md, "Capabilities").
pub const ENV_CONTAINER_ALLOW_CAPS: &str = "LEGION_CONTAINER_ALLOW_CAPS";

// === Task filesystem ===
/// Where a runtime with an isolated filesystem mounts the allocation's host
/// task directory, and therefore the root every file-delivery destination on
/// such a task must resolve under (`kvInjections.files[].path`, identity
/// `file.path`, staged artifacts).
///
/// A constant rather than a per-job setting on purpose: jobs grep for it and
/// hardcode it, so it can never comfortably change. `/legion/task` is
/// unmistakably Legion's and leaves room for siblings (`/legion/secrets`) if
/// the deferred directory splits ever land. See docs/task-runtime-container.md.
pub const CONTAINER_TASK_DIR: &str = "/legion/task";

// === Allocations ===
// Allocation row status values. An allocation is `placed` from the moment the
// scheduler writes its row until a worker reports it terminal; `complete` and
// `failed` are the terminal states a worker's exit report produces (see
// docs/task-supervision.md). Terminal allocations keep their row — the slot is
// still spent — but are never dispatched back to a worker.
pub const ALLOCATION_STATUS_PLACED: &str = "placed";
pub const ALLOCATION_STATUS_COMPLETE: &str = "complete";
pub const ALLOCATION_STATUS_FAILED: &str = "failed";
/// A suspended instance's allocation kept only to hold its reservation
/// (`suspend.reserve = true`). Never dispatched to a worker.
pub const ALLOCATION_STATUS_SUSPENDED: &str = "suspended";

/// Whether an allocation row must never be sent to a worker: it finished, or
/// it only holds a suspended instance's reservation.
pub fn allocation_status_is_dispatchable(status: &str) -> bool {
    !allocation_status_is_terminal(status) && status != ALLOCATION_STATUS_SUSPENDED
}

/// Whether an allocation row is in a terminal state.
pub fn allocation_status_is_terminal(status: &str) -> bool {
    matches!(
        status,
        ALLOCATION_STATUS_COMPLETE | ALLOCATION_STATUS_FAILED
    )
}

// === Manager ===
// `legion start manager` reads a config file only when `-c` names one;
// without it the manager runs on built-in defaults, wherever it is started.
//
// `workerConfigPath` default: none. With no worker config directory the
// manager registers the built-in `default` worker type (pool `default`,
// every setting at its default) so `legion join` works out of the box; a
// configured directory replaces it with the types it declares.
pub const DEF_WORKER_CONFIG_PATH: &str = "";
// The worker type registered when no worker config directory is configured.
pub const DEF_WORKER_TYPE: &str = "default";
pub const DEF_MANAGER_HOSTNAME: &str = "127.0.0.1";
pub const DEF_MANAGER_SERVER_PORT: u64 = 4747;
pub const DEF_MANAGER_INSECURE_PORT: u64 = 4748;
pub const DEF_MANAGER_ADDRESS: &str = "http://127.0.0.1:4747";
// Loop time in manager rechecking nodes
pub const DEF_WORKER_CHECK_SEC: u64 = 2;
pub const DEF_SCHEDULING_POLICY: &str = "bin_pack";
pub const DEF_EVAL_PRIORITY: i64 = 50;
// How far below the cluster's default eval priority a `node_lost_reschedule`
// eval is enqueued. The eval queue is strict `priority DESC, id ASC`, so at
// EQUAL priority a mass node-loss event's recovery backlog runs strictly
// ahead of every newer submission (measured on the 2026-08-04 AWS rig: ~3,000
// lost nodes slowed job submission to ~6s per job behind 2,752 queued
// reschedules). Recovery is bulk background work that the monitor re-drives
// every tick; a submission is someone waiting. One band below the default
// lets submissions overtake the backlog while reschedules still outrank
// anything an operator explicitly deprioritized.
pub const NODE_LOST_RESCHEDULE_PRIORITY_DROP: i64 = 10;
pub const DEF_ROLLOUT_RETENTION_DAYS: u64 = 30;
// Read-model maintenance (docs/state-store.md, "Read model coherence"):
// every tick each manager checks for outside commits to its state database
// and reloads a slice of nodes to repair any cache drift. 128 nodes every 5s
// sweeps a 10,000-node fleet in about 6.5 minutes for ~26 indexed reads a
// second.
pub const READ_MODEL_MAINTENANCE_INTERVAL_SECS: u64 = 5;
pub const READ_MODEL_SCRUB_NODES_PER_TICK: usize = 128;
// Debounce window (ms) for KV-driven job reconciliation: rapid puts/deletes
// to watched keys are coalesced and applied once per quiet window. 0 applies
// every KV write synchronously (see docs/kv-store.md).
pub const DEF_KV_DEBOUNCE_MS: u64 = 500;
// Amount of time since last heartbeat before a node is considered lost.
//
// No longer a global cutoff: a node's window is `missedBeatsBeforeLost x that
// node's heartbeat interval` (heartbeat-cadence plan §6.4), recorded per node
// in `node_health.lost_after_seconds`. This constant is what that arithmetic
// reproduces at the defaults (5 x 2s) and the fallback for a row written
// before the column existed. See docs/heartbeat-cadence-and-scale.md.
pub const DEF_LOST_NODE_AT_SEC: i64 = 10;

// === Heartbeat cadence and node liveness ===
// (docs/heartbeat-cadence-and-scale.md, docs/node-liveness.md)
/// Missed heartbeats before a node is marked lost. The lost window is ALWAYS
/// this times the node's own heartbeat interval, so `5 x 2s` reproduces the
/// historical fixed 10s exactly.
pub const DEF_MISSED_BEATS_BEFORE_LOST: u32 = 5;
/// Floor on a manager-served heartbeat interval, whatever the fleet size.
pub const DEF_HEARTBEAT_MIN_INTERVAL_SEC: u64 = 2;
/// Ceiling on a manager-served interval: past this the manager stops backing
/// off and warns instead, because the answer is more control plane.
pub const DEF_HEARTBEAT_MAX_INTERVAL_SEC: u64 = 120;
/// Default aggregate heartbeat rate the manager sizes adaptive cadence for
/// (`heartbeat.targetRatePerSecond`). With the default headroom of 2 the
/// interval is `ceil(N / 1250)` seconds, so every fleet up to 2,500 nodes
/// stays on the 2s floor and its historical 10s lost window, and the
/// steady-state load never exceeds `rate / headroom` = 1,250 beats/s. That is
/// about an eighth of the ~10,000 beats/s that took more than a core of a
/// 4-vCPU leader on the AWS rig at 20k nodes, so the budget is deliberately
/// small. See docs/heartbeat-cadence-and-scale.md.
pub const DEF_HEARTBEAT_TARGET_RATE_PER_SECOND: f64 = 2500.0;
/// Headroom factor in the sizing formula `heartbeatSeconds >= H x N / R`.
/// 2 means "run at half the measured ceiling so a burst is not an outage".
pub const DEF_HEARTBEAT_HEADROOM: f64 = 2.0;
/// Upper clamp on a worker type's `heartbeatSeconds`. Widened from the
/// historical 30 once the lost window began moving with the cadence.
pub const MAX_WORKER_HEARTBEAT_SECONDS: u32 = 300;
/// Freshness ratio below which the manager warns that the fleet is not
/// sustaining its cadence (`legion_heartbeat_freshness_ratio`).
pub const HEARTBEAT_FRESHNESS_WARN_RATIO: f64 = 0.5;
/// Consecutive sub-threshold ticks before the freshness warning fires, and
/// how often it may repeat. Debounced so a single slow tick is not a page.
pub const HEARTBEAT_FRESHNESS_WARN_TICKS: u32 = 3;
pub const HEARTBEAT_FRESHNESS_WARN_INTERVAL_SEC: u64 = 60;
/// Fleets smaller than this have too few nodes for the freshness ratio (and
/// the mass-disruption circuit breaker) to mean anything.
pub const HEARTBEAT_FRESHNESS_MIN_FLEET: usize = 20;
/// Fraction of the active fleet that, if it would be marked lost in ONE tick,
/// means the control plane broke rather than the fleet. Mark none, log error.
pub const LIVENESS_MASS_DISRUPTION_FRACTION: f64 = 0.5;
/// After acquiring status-lane leadership (or starting the process), mark
/// nothing lost for this long: a new leader inherits no liveness state and
/// rebuilds it from incoming heartbeats (Nomad's `failover_heartbeat_ttl`).
pub const DEF_LIVENESS_FAILOVER_GRACE_SEC: u64 = 60;
/// How often a manager that does NOT lead the status lane reports the nodes
/// it heard from to the leader. One RPC per manager per interval.
pub const DEF_LIVENESS_DIGEST_INTERVAL_SEC: u64 = 1;
/// How long the leader keeps trusting a silent peer manager's last digest.
/// Nodes that peer last claimed are not eligible to be marked lost inside it.
pub const DEF_LIVENESS_PEER_DIGEST_GRACE_SEC: u64 = 30;
/// How often the status-lane leader replicates a batched heartbeat
/// checkpoint so every manager can show an approximate last-heartbeat time.
/// Display only — a checkpoint is never read back as liveness. 0 disables.
pub const DEF_LIVENESS_CHECKPOINT_INTERVAL_SEC: u64 = 30;
/// Smallest non-zero `checkpointInterval`; anything faster re-creates the
/// per-heartbeat commit load the liveness plan exists to remove.
pub const MIN_LIVENESS_CHECKPOINT_INTERVAL_SEC: u64 = 5;
// Cluster region stamped into workload-identity claims/sub. Legion has no
// multi-region concept yet; "auto" matches the documented default subject
// shape (see docs/workload-identity.md).
pub const DEF_CLUSTER_REGION: &str = "auto";

// === Worker ===
// How often the worker should ping the manager
pub const WORKER_HEARTBEAT_DURATION_SEC: u64 = 2;
// How often the worker should update node to manager
pub const WORKER_NODE_CHECK_DURATION_SEC: u64 = 30;
pub const DEF_WORKER_SERVER_PORT: u64 = 4848;

/// Directory holding one subdirectory per worker ID on this host.
pub fn worker_state_dir() -> PathBuf {
    data_dir().join("workers")
}

// === Local state ===
/// Env var that relocates every `.legion/*` default, so two Legion processes
/// can share a working directory without sharing state. Explicit config
/// (`storePath`, `raft.dataDir`, `LEGION_WORKER_STATE_PATH`) still wins. See
/// docs/data-directory.md.
pub const DATA_DIR_ENV: &str = "LEGION_DATA_DIR";
pub const DEF_DATA_DIR: &str = ".legion";

/// Root for this process's on-disk state: `$LEGION_DATA_DIR`, else `./.legion`.
/// An empty value counts as unset.
pub fn data_dir() -> PathBuf {
    resolve_data_dir(std::env::var_os(DATA_DIR_ENV))
}

fn resolve_data_dir(var: Option<std::ffi::OsString>) -> PathBuf {
    match var {
        Some(dir) if !dir.is_empty() => dir.into(),
        _ => DEF_DATA_DIR.into(),
    }
}

/// Default manager `storePath`: the data dir itself, which then holds
/// `store.sqlite`, `keys/` and `backups/`.
///
/// Earlier builds defaulted to `<data dir>/store.sqlite`, and since the store
/// path is a directory, a manager that ran with no `storePath` has its db one
/// level down at `.legion/store.sqlite/store.sqlite`. Keep opening that layout
/// rather than silently starting an empty cluster beside it.
pub fn default_store_path() -> String {
    store_path_under(data_dir()).to_string_lossy().into_owned()
}

fn store_path_under(dir: PathBuf) -> PathBuf {
    let legacy = dir.join("store.sqlite");
    if legacy.is_dir() {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            tracing::warn!(
                path = %legacy.display(),
                "Using the legacy default state store location; set `storePath` to this path to keep using it"
            );
        });
        legacy
    } else {
        dir
    }
}

/// Default raft `dataDir`: `<data dir>/raft`.
pub fn default_raft_data_dir() -> String {
    data_dir().join("raft").to_string_lossy().into_owned()
}

// How long the manager keeps a stop notice (why an allocation was removed)
// before pruning it. Notices are advisory metadata delivered on the next
// heartbeat — at a 2s cadence a worker sees one within one tick, so this is
// slack for a partitioned or restarting worker, not a delivery window. See
// `docs/graceful-stop.md`.
pub const STOP_NOTICE_RETENTION_SECS: i64 = 600;

// How long the manager remembers a removed allocation (its job, task, and
// node) so `legion job logs` can still fetch its output from the worker,
// which keeps the files for `LEGION_LOG_RETENTION_DAYS`. See
// `docs/logging.md`, "Logs of removed allocations".
pub const ALLOCATION_HISTORY_RETENTION_SECS: i64 = 24 * 60 * 60;
// How many removed allocations `legion job logs <job>` returns at most when
// it falls back to (or is asked for) the job's removed allocations, newest
// first. `--allocation <id>` reaches any one of them.
pub const REMOVED_ALLOCATION_LOGS_LIMIT: usize = 10;

// Default per-stream cap (bytes) when reading captured task logs and no explicit
// limit is requested. 256 KiB keeps replies small enough for an agent context.
pub const DEF_TASK_LOG_MAX_BYTES: i64 = 256 * 1024;

// Absolute per-stream ceiling for a single log read. An explicit `max_bytes`
// larger than this is clamped so a buggy/hostile client cannot drive a huge
// buffer allocation (OOM/DoS) even though the read API is otherwise unbounded.
pub const MAX_TASK_LOG_READ_BYTES: u64 = 16 * 1024 * 1024;

// Task log rotation/retention defaults (worker-side). These keep a chatty task
// from filling a worker's disk while preserving a useful recent window. All are
// overridable via env vars (LEGION_LOG_MAX_FILE_BYTES / LEGION_LOG_MAX_FILES /
// LEGION_LOG_RETENTION_DAYS).
pub const DEF_TASK_LOG_MAX_FILE_BYTES: u64 = 10 * 1024 * 1024; // 10 MiB per stream
pub const DEF_TASK_LOG_MAX_FILES: u32 = 5; // rotated generations kept per stream
pub const DEF_TASK_LOG_RETENTION_DAYS: u64 = 7; // age-based prune of rotated files
pub const DEF_TASK_LOG_MAINTENANCE_SEC: u64 = 10; // rotation/retention sweep cadence

#[cfg(test)]
mod data_dir_test {
    use super::*;

    #[test]
    fn data_dir_defaults_to_dot_legion_when_unset_or_empty() {
        assert_eq!(resolve_data_dir(None), PathBuf::from(".legion"));
        assert_eq!(resolve_data_dir(Some("".into())), PathBuf::from(".legion"));
        assert_eq!(
            resolve_data_dir(Some("/tmp/x".into())),
            PathBuf::from("/tmp/x")
        );
    }

    #[test]
    fn store_path_is_the_data_dir_unless_the_legacy_layout_exists() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        assert_eq!(store_path_under(root.clone()), root);

        // A db file at the new location is not mistaken for the legacy dir.
        std::fs::write(root.join("store.sqlite"), b"").unwrap();
        assert_eq!(store_path_under(root.clone()), root);

        let legacy_root = dir.path().join("old");
        std::fs::create_dir_all(legacy_root.join("store.sqlite")).unwrap();
        assert_eq!(
            store_path_under(legacy_root.clone()),
            legacy_root.join("store.sqlite")
        );
    }
}
