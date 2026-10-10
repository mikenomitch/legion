//! Snapshots as the manager records them (suspend plan §6.5, §7), and the
//! task-level `snapshots { ttl, keep, memory }` jobspec block.
//!
//! The capture mechanics and the manifest format live in
//! [`crate::structs::checkpoint`]. This module is the control-plane half: the
//! `snapshot` row's state machine, and the jobspec knobs that decide whether
//! a task is launched ready for memory checkpoints and how long its
//! snapshots live. See `docs/suspend-and-resume.md`.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::structs::checkpoint::SnapshotKind;

/// Lifecycle of a `snapshot` row.
///
/// ```text
///  CreateSnapshot                    worker reports ok          durability = "store":
///  ──────────────▶ capturing ──────────────────────▶ local ──── worker publishes ───▶ published
///                      │ worker reports an error        │ DeleteSnapshot / ttl / keep     │
///                      ▼                                ▼                                 │
///                    failed ── ttl / DeleteSnapshot ─▶ deleting ◀─────────────────────────┘
///                                                        │ worker confirms
///                                                        ▼
///                                                  (row removed, snapshot.deleted)
///  node removed: capturing / local ──▶ lost     (published survives its node)
/// ```
///
/// `capturing` and `deleting` are states with work outstanding on a node,
/// and so is a `local` row with `durability = "store"` that has not been
/// published yet: the manager re-sends a [`crate::structs::CheckpointRequest`]
/// for each on every heartbeat until the node reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotState {
    Capturing,
    Local,
    /// Every part is in the content store (`durability = "store"`): the
    /// snapshot restores on any compatible node and survives its node.
    Published,
    Failed,
    Deleting,
    Lost,
}

impl SnapshotState {
    pub fn as_str(&self) -> &'static str {
        match self {
            SnapshotState::Capturing => "capturing",
            SnapshotState::Local => "local",
            SnapshotState::Published => "published",
            SnapshotState::Failed => "failed",
            SnapshotState::Deleting => "deleting",
            SnapshotState::Lost => "lost",
        }
    }

    pub const ALL: [SnapshotState; 6] = [
        SnapshotState::Capturing,
        SnapshotState::Local,
        SnapshotState::Published,
        SnapshotState::Failed,
        SnapshotState::Deleting,
        SnapshotState::Lost,
    ];
}

impl SnapshotState {
    /// Captured and restorable: `local` (from its node) or `published`
    /// (from any compatible node).
    pub fn is_restorable(&self) -> bool {
        matches!(self, SnapshotState::Local | SnapshotState::Published)
    }
}

/// Where a snapshot's bytes are meant to live (suspend plan §6.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotDurability {
    /// On the node that captured it, and nowhere else. A restore is pinned
    /// to that node, and losing the node loses the snapshot. The default.
    #[default]
    Local,
    /// Also published to the capturing worker's `contentStore`. The row is
    /// `local` until the upload finishes, then `published`.
    Store,
}

impl SnapshotDurability {
    pub fn as_str(&self) -> &'static str {
        match self {
            SnapshotDurability::Local => "local",
            SnapshotDurability::Store => "store",
        }
    }
}

impl FromStr for SnapshotDurability {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim() {
            "" | "local" => Ok(SnapshotDurability::Local),
            "store" => Ok(SnapshotDurability::Store),
            other => Err(format!(
                "durability {other:?} is not valid (expected \"local\" or \"store\")"
            )),
        }
    }
}

/// The worker capability (`WorkerRegisterRequest.capabilities`) that says a
/// worker has a `contentStore` configured and can publish snapshot parts.
pub const CONTENT_PUBLISH_CAPABILITY: &str = "content-publish";

/// The worker capability that says a worker can restore a `published`
/// snapshot it never held: it fetches the parts by URL into its node content
/// store first. Every worker since published checkpoints shipped advertises
/// it. Placement of a published restore only considers workers that do, so
/// an older worker in a mixed fleet is never picked for one.
pub const CHECKPOINT_FETCH_CAPABILITY: &str = "checkpoint-fetch";

/// Whether a node's worker advertised `capability` at registration
/// (`dynamic_attrs.capabilities`).
pub fn node_advertises(node: &crate::structs::Node, capability: &str) -> bool {
    node.dynamic_attrs
        .get("capabilities")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|caps| caps.iter().any(|cap| cap.as_str() == Some(capability)))
}

/// Built-in volume drivers whose data lives on one node's disk. A plugin
/// driver says so itself (`nodeLocal` in its capabilities), which admission
/// checks against the cluster's fingerprints.
pub const NODE_LOCAL_BUILTIN_DRIVERS: &[&str] = &["local-dir", "host", "tmpfs"];

/// The error for `durability = "store"` on a task whose instance volume
/// cannot leave its node. `field` names what asked (`suspend.durability`,
/// `--publish`).
pub fn store_durability_volume_error(task_name: &str, driver: &str, field: &str) -> String {
    format!(
        "task {task_name:?} sets {field} = \"store\", but its stateful.storage uses volume \
         driver `{driver}`, which keeps the data on one node. A published checkpoint could \
         resume on another node without its volume\nHint: use a network volume driver (such \
         as `nfs`) for stateful.storage, drop stateful.storage, or keep durability = \"local\""
    )
}

/// Submit-time check: a task that publishes its checkpoints must not keep
/// its instance data on a node-local built-in driver. Plugin drivers are
/// checked at admission against what they advertise.
pub fn validate_store_durability_storage(
    task: &crate::structs::TaskConfig,
    field: &str,
) -> Result<(), String> {
    let Some(storage) = task
        .stateful
        .as_ref()
        .and_then(|stateful| stateful.storage.as_ref())
    else {
        return Ok(());
    };
    let driver = storage.driver_name();
    if NODE_LOCAL_BUILTIN_DRIVERS.contains(&driver) {
        return Err(store_durability_volume_error(&task.name, driver, field));
    }
    Ok(())
}

impl fmt::Display for SnapshotState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SnapshotState {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        SnapshotState::ALL
            .into_iter()
            .find(|state| state.as_str() == raw)
            .ok_or_else(|| {
                format!(
                    "unknown snapshot state {raw:?} (expected capturing, local, published, \
                     failed, deleting, or lost)"
                )
            })
    }
}

/// Why a snapshot exists: `manual` (CreateSnapshot), `suspend` (an
/// instance's suspend checkpoint, spent when it resumes), or `fork` (a
/// ForkInstance checkpoint, spent once every fork has restored from it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotPurpose {
    Manual,
    Suspend,
    Fork,
}

impl SnapshotPurpose {
    pub fn as_str(&self) -> &'static str {
        match self {
            SnapshotPurpose::Manual => "manual",
            SnapshotPurpose::Suspend => "suspend",
            SnapshotPurpose::Fork => "fork",
        }
    }
}

impl FromStr for SnapshotPurpose {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "manual" => Ok(SnapshotPurpose::Manual),
            "suspend" => Ok(SnapshotPurpose::Suspend),
            "fork" => Ok(SnapshotPurpose::Fork),
            other => Err(format!("unknown snapshot purpose {other:?}")),
        }
    }
}

impl FromStr for SnapshotKind {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "disk" | "" => Ok(SnapshotKind::Disk),
            "memory" => Ok(SnapshotKind::Memory),
            other => Err(format!(
                "unknown snapshot kind {other:?} (expected `memory` or `disk`)"
            )),
        }
    }
}

/// One `snapshot` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotRecord {
    pub snapshot_id: String,
    pub job_id: String,
    pub task_name: String,
    /// The instance it was taken from. Always set for `manual` snapshots.
    pub instance_id: Option<String>,
    pub name: Option<String>,
    pub kind: SnapshotKind,
    pub purpose: SnapshotPurpose,
    pub state: SnapshotState,
    /// `local`, or `store` when the snapshot is published once captured.
    pub durability: SnapshotDurability,
    /// Where the bytes were captured, and where a local copy lives. A
    /// `published` snapshot keeps the field after its node is gone.
    pub node_id: Option<String>,
    /// The allocation it was captured from.
    pub allocation_id: Option<i64>,
    /// The manifest JSON the node reported. Empty while `capturing`.
    pub manifest_json: String,
    pub total_bytes: u64,
    pub duration_ms: u64,
    pub error: Option<String>,
    /// Unix seconds.
    pub created_at: i64,
    /// Unix seconds; `None` never expires on its own.
    pub expires_at: Option<i64>,
    /// How long a live memory capture paused the workload, in milliseconds.
    /// 0 for a disk capture and until the capture lands.
    #[serde(default)]
    pub pause_ms: u64,
    /// Unix seconds when the upload finished (`published`).
    pub published_at: Option<i64>,
}

impl SnapshotRecord {
    /// The parsed manifest, once captured.
    pub fn manifest(&self) -> anyhow::Result<Option<crate::structs::checkpoint::SnapshotManifest>> {
        if self.manifest_json.trim().is_empty() {
            return Ok(None);
        }
        crate::structs::checkpoint::SnapshotManifest::from_json(&self.manifest_json).map(Some)
    }
}

/// Mint a snapshot id: `snap-` plus 16 lowercase hex characters. Also used as
/// the content-store pin owner and the volume snapshot id on the node, so it
/// stays inside the charset both accept.
pub fn new_snapshot_id() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut bytes);
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("snap-{hex}")
}

/// Validate an operator-chosen snapshot name: `[A-Za-z0-9._-]{1,64}`, and not
/// shaped like a minted id, so a name can never shadow one.
pub fn validate_snapshot_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 64 {
        return Err(format!("snapshot name {name:?} must be 1 to 64 characters"));
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
    {
        return Err(format!(
            "snapshot name {name:?} may only contain letters, digits, `.`, `_`, and `-`"
        ));
    }
    if name.starts_with("snap-") {
        return Err(format!(
            "snapshot name {name:?} may not start with `snap-`, which is reserved for snapshot ids"
        ));
    }
    Ok(())
}

/// The worker capability (`WorkerRegisterRequest.capabilities`) that says a
/// worker takes and reclaims snapshots and starts allocations from them. The
/// manager sends checkpoint work only to workers that advertise it.
pub const CHECKPOINT_CAPABILITY: &str = "checkpoint";

/// Default `snapshots.keep`: how many snapshots of one instance survive
/// before the oldest is deleted.
pub const DEFAULT_SNAPSHOT_KEEP: u32 = 5;

/// The task-level `snapshots` block.
///
/// ```nickel
/// snapshots = { ttl = "7d", keep = 5, memory = true },
/// ```
///
/// Declaring the block is what opts a task into snapshots. For a `sandbox`
/// task on a real VMM it also launches the sandbox ready for memory
/// checkpoints (an API socket on Firecracker, stable device links, an
/// identity disk), unless `memory = false`.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotsConfig {
    /// How long a snapshot lives before it is deleted automatically, as a
    /// duration (`"7d"`, `"12h"`). Unset: snapshots never expire on their
    /// own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    /// How many snapshots of one instance to keep. Taking one more deletes
    /// the oldest. Default 5.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep: Option<u32>,
    /// Whether the task may take memory snapshots. Default `true`. `false`
    /// limits it to disk snapshots and launches a sandbox exactly as it would
    /// without a `snapshots` block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<bool>,
}

/// [`SnapshotsConfig`] with defaults applied and durations parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSnapshots {
    pub ttl: Option<Duration>,
    pub keep: u32,
    pub memory: bool,
}

impl SnapshotsConfig {
    pub fn resolve(&self) -> Result<ResolvedSnapshots, String> {
        let ttl = match self.ttl.as_deref().map(str::trim) {
            None | Some("") => None,
            Some(raw) => {
                let ttl = humantime::parse_duration(raw)
                    .map_err(|err| format!("snapshots.ttl {raw:?} is not a duration: {err}"))?;
                if ttl < Duration::from_secs(60) {
                    return Err(format!(
                        "snapshots.ttl {raw:?} is shorter than one minute; a snapshot must \
                         outlive the capture that takes it"
                    ));
                }
                Some(ttl)
            }
        };
        let keep = self.keep.unwrap_or(DEFAULT_SNAPSHOT_KEEP);
        if keep == 0 {
            return Err(
                "snapshots.keep must be at least 1 (remove the snapshots block to disable \
                 snapshots)"
                    .to_string(),
            );
        }
        Ok(ResolvedSnapshots {
            ttl,
            keep,
            memory: self.memory.unwrap_or(true),
        })
    }
}

/// What in a task's spec asks for memory checkpoints, if anything.
///
/// This is the single predicate behind "launch this task ready for memory
/// checkpoints" (the worker sets `TaskInstance.checkpoint_launch` from it)
/// and behind the submit-time refusal of combinations that cannot honor it.
/// A `suspend` block asks too, unless it sets `checkpoint = "disk"` (a
/// sandbox's default suspend checkpoint is memory).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryCheckpointAsk {
    /// The jobspec field that asks, for error messages.
    pub field: &'static str,
    /// How to stop asking, for the hint.
    pub opt_out: &'static str,
}

pub fn memory_checkpoint_ask(task: &crate::structs::TaskConfig) -> Option<MemoryCheckpointAsk> {
    if let Some(snapshots) = task.snapshots.as_ref() {
        if snapshots.memory != Some(false) {
            return Some(MemoryCheckpointAsk {
                field: "snapshots",
                opt_out: "set snapshots.memory = false for disk-only snapshots",
            });
        }
    }
    if task.suspend.is_some()
        && crate::structs::suspend::default_checkpoint_kind(task) == SnapshotKind::Memory
    {
        return Some(MemoryCheckpointAsk {
            field: "suspend",
            opt_out: "set suspend.checkpoint = \"disk\" to suspend with a disk checkpoint",
        });
    }
    None
}

/// Whether the task's own declaration puts it on gated networking.
///
/// Only the declared mode is visible at submit time. A stored network policy
/// can also force `gated`; that case is caught when a memory snapshot is
/// requested (CreateSnapshot fails with the same hint) and at launch (the
/// sandbox starts without checkpoint support and says so).
pub fn task_declares_gated_network(task: &crate::structs::TaskConfig) -> bool {
    task.network.as_ref().is_some_and(|network| {
        crate::structs::network::resolve_network_mode(&network.mode, &task.runtime)
            == crate::structs::netpolicy::MODE_GATED
    })
}

/// The error for a memory checkpoint on gated networking, shared by job
/// validation and CreateSnapshot so both say the same thing.
pub fn gated_memory_checkpoint_error(task_name: &str, ask: Option<MemoryCheckpointAsk>) -> String {
    let opt_out = ask
        .map(|ask| ask.opt_out)
        .unwrap_or("take a disk snapshot instead (--disk)");
    format!(
        "task {task_name:?} asks for memory checkpoints, but sandbox memory checkpoints don't \
         support gated networking yet\nHint: {opt_out}, or drop network gating for this task"
    )
}

/// Submit-time check: a `sandbox` task that asks for memory checkpoints must
/// not declare gated networking. Returns the actionable error.
pub fn validate_memory_checkpoint_network(task: &crate::structs::TaskConfig) -> Result<(), String> {
    if task.runtime != "sandbox" {
        return Ok(());
    }
    let Some(ask) = memory_checkpoint_ask(task) else {
        return Ok(());
    };
    if task_declares_gated_network(task) {
        return Err(gated_memory_checkpoint_error(&task.name, Some(ask)));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn states_round_trip_through_their_wire_names() {
        for state in SnapshotState::ALL {
            assert_eq!(state.as_str().parse::<SnapshotState>(), Ok(state));
        }
        assert!("paused".parse::<SnapshotState>().is_err());
    }

    #[test]
    fn snapshot_ids_are_valid_volume_snapshot_and_pin_names() {
        let id = new_snapshot_id();
        assert!(id.starts_with("snap-"));
        assert_eq!(id.len(), "snap-".len() + 16);
        assert!(crate::content_store::PinOwner::new(format!("snapshot:{id}")).is_ok());
    }

    #[test]
    fn names_reject_separators_and_the_id_prefix() {
        assert!(validate_snapshot_name("before-upgrade.v2").is_ok());
        assert!(validate_snapshot_name("a/b").is_err());
        assert!(validate_snapshot_name("").is_err());
        assert!(validate_snapshot_name("snap-123").is_err());
        assert!(validate_snapshot_name(&"x".repeat(65)).is_err());
    }

    #[test]
    fn snapshots_block_defaults_and_validation() {
        let resolved = SnapshotsConfig::default().resolve().unwrap();
        assert_eq!(resolved.keep, DEFAULT_SNAPSHOT_KEEP);
        assert_eq!(resolved.ttl, None);
        assert!(resolved.memory);

        let resolved = SnapshotsConfig {
            ttl: Some("7d".into()),
            keep: Some(2),
            memory: Some(false),
        }
        .resolve()
        .unwrap();
        assert_eq!(resolved.ttl, Some(Duration::from_secs(7 * 86_400)));
        assert_eq!(resolved.keep, 2);
        assert!(!resolved.memory);

        let err = SnapshotsConfig {
            keep: Some(0),
            ..Default::default()
        }
        .resolve()
        .unwrap_err();
        assert!(err.contains("keep"), "{err}");
        let err = SnapshotsConfig {
            ttl: Some("soon".into()),
            ..Default::default()
        }
        .resolve()
        .unwrap_err();
        assert!(err.contains("snapshots.ttl"), "{err}");
        assert!(SnapshotsConfig {
            ttl: Some("5s".into()),
            ..Default::default()
        }
        .resolve()
        .is_err());
    }

    fn sandbox_task(
        network_mode: Option<&str>,
        snapshots: Option<SnapshotsConfig>,
    ) -> crate::structs::TaskConfig {
        let mut task: crate::structs::TaskConfig = serde_json::from_value(serde_json::json!({
            "name": "workspace",
            "runtime": "sandbox",
            "config": { "image": "alpine", "command": ["sleep", "1"] },
        }))
        .unwrap();
        task.network = network_mode
            .map(|mode| serde_json::from_value(serde_json::json!({ "mode": mode })).unwrap());
        task.snapshots = snapshots;
        task
    }

    #[test]
    fn a_gated_sandbox_that_asks_for_memory_checkpoints_is_refused_with_a_hint() {
        let task = sandbox_task(Some("gated"), Some(SnapshotsConfig::default()));
        let err = validate_memory_checkpoint_network(&task).unwrap_err();
        assert!(
            err.starts_with(
                "task \"workspace\" asks for memory checkpoints, but sandbox memory checkpoints \
                 don't support gated networking yet"
            ),
            "{err}"
        );
        assert!(err.contains("Hint: set snapshots.memory = false"), "{err}");
        assert!(err.contains("drop network gating"), "{err}");
    }

    #[test]
    fn a_gated_sandbox_that_suspends_to_memory_is_refused_with_a_hint() {
        let mut task = sandbox_task(Some("gated"), None);
        task.stateful =
            Some(serde_json::from_value(serde_json::json!({ "mode": "keyed" })).unwrap());
        task.suspend = Some(Default::default());
        let err = validate_memory_checkpoint_network(&task).unwrap_err();
        assert!(err.contains("don't support gated networking"), "{err}");
        assert!(err.contains("suspend.checkpoint = \"disk\""), "{err}");
        task.suspend = Some(crate::structs::suspend::SuspendConfig {
            checkpoint: Some("disk".into()),
            ..Default::default()
        });
        assert!(validate_memory_checkpoint_network(&task).is_ok());
    }

    #[test]
    fn disk_only_snapshots_host_networking_and_other_runtimes_pass() {
        let disk_only = SnapshotsConfig {
            memory: Some(false),
            ..Default::default()
        };
        assert!(
            validate_memory_checkpoint_network(&sandbox_task(Some("gated"), Some(disk_only)))
                .is_ok()
        );
        assert!(validate_memory_checkpoint_network(&sandbox_task(
            Some("host"),
            Some(SnapshotsConfig::default())
        ))
        .is_ok());
        assert!(validate_memory_checkpoint_network(&sandbox_task(
            None,
            Some(SnapshotsConfig::default())
        ))
        .is_ok());
        assert!(validate_memory_checkpoint_network(&sandbox_task(Some("gated"), None)).is_ok());
        let mut exec = sandbox_task(Some("gated"), Some(SnapshotsConfig::default()));
        exec.runtime = "exec".into();
        assert!(validate_memory_checkpoint_network(&exec).is_ok());
    }
}
