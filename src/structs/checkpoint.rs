//! The checkpoint manifest: what a captured allocation is made of and where
//! it can be restored.
//!
//! A **checkpoint** is a point-in-time capture of one allocation. Its disk
//! half is a volume snapshot (the volume driver's `snapshot` op). Its memory
//! half, present only for runtimes that report
//! [`CheckpointSupport::Memory`](crate::task_plugins::CheckpointSupport), is a
//! set of content-addressed parts the task runtime wrote: guest memory, VMM
//! device state, the VMM config, and the guest's ephemeral root disk.
//!
//! These types are shared by the worker (which writes a manifest when a
//! runtime checkpoints) and the manager (which persists it on a `snapshot`
//! row and turns its [`SnapshotCompat`] block into placement constraints).
//! They are plain serde types with no worker dependencies for that reason.
//!
//! The shape follows `internal_docs/plans/sandbox-snapshots-and-fast-start-plan.md`
//! Part I §2, with `kind` narrowed to `disk | memory` per the suspend plan §6.5.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Manifest schema version. Bump on an incompatible change, and teach the
/// reader to refuse versions it does not know.
pub const MANIFEST_API_VERSION: u32 = 1;

/// Node fingerprint attribute: the sandbox VMM this node runs by default
/// (`cloud-hypervisor`, `firecracker`, `apple-container`, `oci-runtime`).
pub const ATTR_SANDBOX_VMM: &str = "sandbox.vmm";
/// Node fingerprint attribute: the version of [`ATTR_SANDBOX_VMM`].
pub const ATTR_SANDBOX_VMM_VERSION: &str = "sandbox.vmm.version";
/// Node fingerprint attribute: CPU vendor (`GenuineIntel`, `AuthenticAMD`,
/// an ARM implementer code, `Apple`).
pub const ATTR_CPU_VENDOR: &str = "cpu.vendor";
/// Node fingerprint attribute: CPU model (`family:model:stepping` on x86,
/// `implementer:part:variant:revision` on ARM Linux).
pub const ATTR_CPU_MODEL: &str = "cpu.model";
/// Node fingerprint attribute: a short digest of the sorted CPU feature flag
/// set, so a guest that saw `avx512f` is never resumed where it is missing.
pub const ATTR_CPU_FEATURES: &str = "cpu.features";

/// The per-VMM version attribute (`sandbox.vmm.firecracker.version`). A node
/// with both VMMs installed reports both, so a memory checkpoint taken on
/// the non-default VMM can still find a compatible node.
pub fn vmm_version_attr(vmm: &str) -> String {
    format!("sandbox.vmm.{vmm}.version")
}

/// What a checkpoint captured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotKind {
    /// Volume state only. Restoring boots the task fresh on the captured
    /// volume. Every runtime with a snapshot-capable volume can do this.
    Disk,
    /// Volume state plus guest memory and device state, captured under one
    /// pause. Restoring resumes the guest where it was.
    Memory,
}

impl SnapshotKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            SnapshotKind::Disk => "disk",
            SnapshotKind::Memory => "memory",
        }
    }
}

/// One content-addressed piece of a memory checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryPart {
    /// What the bytes are: `memory`, `vm-state`, `vm-config`, `rootfs`, or a
    /// runtime-specific role. The runtime that wrote the part is the only
    /// reader of the role.
    pub role: String,
    /// File name inside the checkpoint directory. Restore materializes each
    /// part back under this name before handing the directory to the runtime.
    pub file: String,
    /// `sha256:<hex>` of the part's bytes.
    pub digest: String,
    /// Logical size in bytes (a sparse part may use fewer on disk).
    pub size: u64,
    /// Where the part was published (`durability = "store"`), for example
    /// `s3://bucket/prefix/sealed/<name>`. The object is sealed
    /// ([`crate::checkpoint_seal`]): any node that can read the URL and is
    /// sent the part's key restores it through its ordinary artifact
    /// fetcher, opened and verified against `digest`. Unset while the
    /// snapshot is only on its node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// The checkpoint key (`ck_…`) the published object is sealed under.
    /// Set exactly when `url` is. A part with a `url` and no `keyId` was
    /// published before checkpoint encryption and is no longer readable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
}

/// The memory half of a checkpoint, as returned by
/// [`TaskPlugin::checkpoint`](crate::task_plugins::TaskPlugin::checkpoint).
/// Empty for a disk checkpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryParts {
    pub parts: Vec<MemoryPart>,
    /// Runtime-private data needed to restore, opaque to everything else (the
    /// sandbox runtime records its device layout here). Copied verbatim into
    /// [`SnapshotManifest::runtime`].
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub runtime: Value,
    /// Compatibility facts the runtime observed while capturing (VMM type and
    /// version, guest shape). Merged into [`SnapshotManifest::compat`].
    #[serde(default)]
    pub compat: SnapshotCompat,
}

impl MemoryParts {
    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    /// Total logical bytes across every part.
    pub fn total_bytes(&self) -> u64 {
        self.parts.iter().map(|part| part.size).sum()
    }

    pub fn part(&self, role: &str) -> Option<&MemoryPart> {
        self.parts.iter().find(|part| part.role == role)
    }
}

/// Reference to a volume snapshot: what a volume driver's `snapshot` op
/// returned. The manifest records it; restore and clone hand it back to the
/// same driver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSnapshotRef {
    /// Snapshot id chosen by the caller (the checkpoint id), so a retried
    /// `snapshot` call lands on the same snapshot instead of a second one.
    pub id: String,
    /// The volume that was captured.
    pub volume_id: String,
    /// Driver that owns the snapshot.
    pub driver: String,
    /// `filesystem` or `block`, the mode the volume was captured in.
    pub mode: crate::volumes::VolumeMode,
    /// Driver-owned locator (`path` for `local-dir`, a dataset name for ZFS).
    #[serde(default)]
    pub attrs: BTreeMap<String, Value>,
    /// Logical bytes captured, when the driver knows.
    #[serde(default)]
    pub size: u64,
}

/// VMM facts a memory checkpoint depends on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VmmCompat {
    /// `firecracker` or `cloud-hypervisor`.
    #[serde(rename = "type")]
    pub kind: String,
    /// The VMM version that wrote the state. Both projects change their
    /// snapshot format between releases, so restore requires an exact match.
    pub version: String,
}

/// Guest shape. Not a node constraint (any node can host any shape that
/// fits), but restore refuses a manifest whose guest does not match the task
/// it is being restored into.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuestCompat {
    pub vcpus: u32,
    pub memory_mib: u32,
}

/// CPU facts a memory checkpoint depends on. A guest that probed CPU
/// features at boot crashes, or silently corrupts data, when resumed on a CPU
/// without them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CpuCompat {
    pub vendor: String,
    pub model: String,
    /// Digest of the sorted feature flag set ([`ATTR_CPU_FEATURES`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub features: Option<String>,
}

/// The `compat` block: where a checkpoint can be restored.
///
/// Empty for a disk checkpoint, which restores anywhere its volume can be
/// mounted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotCompat {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vmm: Option<VmmCompat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest: Option<GuestCompat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<CpuCompat>,
}

impl SnapshotCompat {
    pub fn is_empty(&self) -> bool {
        self.vmm.is_none() && self.guest.is_none() && self.cpu.is_none()
    }

    /// Fill in the CPU block from a node's fingerprint attributes (the node
    /// that took the checkpoint). Missing attributes leave it unset, which
    /// means "no CPU constraint", the honest answer for a node that could
    /// not fingerprint its CPU.
    pub fn with_cpu_from_attrs(mut self, attrs: &Map<String, Value>) -> Self {
        let vendor = attr_str(attrs, ATTR_CPU_VENDOR);
        let model = attr_str(attrs, ATTR_CPU_MODEL);
        if let (Some(vendor), Some(model)) = (vendor, model) {
            self.cpu = Some(CpuCompat {
                vendor: vendor.to_string(),
                model: model.to_string(),
                features: attr_str(attrs, ATTR_CPU_FEATURES).map(str::to_string),
            });
        }
        self
    }
}

/// A captured allocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotManifest {
    #[serde(default = "default_manifest_version")]
    pub api_version: u32,
    /// Snapshot id (`snap-…`).
    pub id: String,
    pub job_id: String,
    pub task: String,
    /// The instance the checkpoint was taken from, when it had one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_instance: Option<String>,
    /// The allocation that was captured.
    pub origin_alloc_id: i64,
    /// Task runtime that wrote the memory parts (`sandbox`).
    pub runtime: String,
    pub kind: SnapshotKind,
    /// RFC 3339 capture time.
    pub created_at: String,
    /// The disk half: one entry per captured volume, in mount order.
    #[serde(default)]
    pub volumes: Vec<VolumeSnapshotRef>,
    /// The memory half. Empty for [`SnapshotKind::Disk`].
    #[serde(default)]
    pub memory: Vec<MemoryPart>,
    #[serde(default)]
    pub compat: SnapshotCompat,
    /// Runtime-private restore data ([`MemoryParts::runtime`]).
    #[serde(
        default,
        skip_serializing_if = "Value::is_null",
        rename = "runtimeState"
    )]
    pub runtime_state: Value,
}

fn default_manifest_version() -> u32 {
    MANIFEST_API_VERSION
}

impl SnapshotManifest {
    /// Total logical bytes: memory parts plus captured volumes.
    pub fn total_bytes(&self) -> u64 {
        self.memory.iter().map(|part| part.size).sum::<u64>()
            + self.volumes.iter().map(|volume| volume.size).sum::<u64>()
    }

    /// The memory half as [`MemoryParts`], the shape a runtime reads back.
    pub fn memory_parts(&self) -> MemoryParts {
        MemoryParts {
            parts: self.memory.clone(),
            runtime: self.runtime_state.clone(),
            compat: self.compat.clone(),
        }
    }

    /// Parse a stored manifest, refusing a schema version this binary does
    /// not understand rather than misreading it.
    pub fn from_json(raw: &str) -> anyhow::Result<Self> {
        let manifest: SnapshotManifest = serde_json::from_str(raw)?;
        if manifest.api_version != MANIFEST_API_VERSION {
            anyhow::bail!(
                "snapshot manifest {} is version {}, this binary reads version \
                 {MANIFEST_API_VERSION}",
                manifest.id,
                manifest.api_version
            );
        }
        Ok(manifest)
    }
}

/// Who a restored allocation is, and one token file inside its guest. The
/// guest's resume handler reads these off the identity disk, so they live in
/// the `legion-guest` crate the guest binary is built from.
pub use legion_guest::identity::{GuestTokenFile, ResumeIdentity};

/// Never prints the key bytes (build.rs turns off the generated Debug).
impl fmt::Debug for crate::structs::CheckpointPartKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CheckpointPartKey")
            .field("digest", &self.digest)
            .field("key_id", &self.key_id)
            .field("key", &format_args!("<{} bytes redacted>", self.key.len()))
            .finish()
    }
}

/// Why a node cannot restore a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatMismatch {
    /// The node attribute that failed (`cpu.model`).
    pub attribute: String,
    /// What the checkpoint needs.
    pub wanted: String,
    /// What the node reported, or `None` when it reported nothing.
    pub found: Option<String>,
}

impl fmt::Display for CompatMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.found.as_deref() {
            Some(found) => write!(
                f,
                "{} is {found:?}, the checkpoint needs {:?}",
                self.attribute, self.wanted
            ),
            None => write!(
                f,
                "{} is not reported by this node, the checkpoint needs {:?}",
                self.attribute, self.wanted
            ),
        }
    }
}

impl std::error::Error for CompatMismatch {}

fn attr_str<'a>(attrs: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    attrs
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn require(
    attrs: &Map<String, Value>,
    attribute: &str,
    wanted: &str,
) -> Result<(), CompatMismatch> {
    match attr_str(attrs, attribute) {
        Some(found) if found == wanted => Ok(()),
        found => Err(CompatMismatch {
            attribute: attribute.to_string(),
            wanted: wanted.to_string(),
            found: found.map(str::to_string),
        }),
    }
}

/// Can a node with these fingerprint attributes restore this checkpoint?
///
/// Pure, so the manager can call it per candidate node at placement time and
/// name the failing attribute in the placement diagnostic. A disk checkpoint
/// (empty `compat`) matches every node; the volume's own node pinning is the
/// constraint there.
///
/// The VMM check accepts either the per-VMM attribute
/// (`sandbox.vmm.firecracker.version`) or the node's default VMM
/// (`sandbox.vmm` + `sandbox.vmm.version`). CPU vendor and model must match
/// exactly, and so must the feature digest when the checkpoint recorded one.
/// Firecracker CPU templates could widen this later.
pub fn compat_matches(
    manifest: &SnapshotManifest,
    node_attrs: &Map<String, Value>,
) -> Result<(), CompatMismatch> {
    let compat = &manifest.compat;
    if let Some(vmm) = compat.vmm.as_ref() {
        let per_vmm = vmm_version_attr(&vmm.kind);
        if attr_str(node_attrs, &per_vmm).is_some() {
            require(node_attrs, &per_vmm, &vmm.version)?;
        } else {
            require(node_attrs, ATTR_SANDBOX_VMM, &vmm.kind)?;
            require(node_attrs, ATTR_SANDBOX_VMM_VERSION, &vmm.version)?;
        }
    }
    if let Some(cpu) = compat.cpu.as_ref() {
        require(node_attrs, ATTR_CPU_VENDOR, &cpu.vendor)?;
        require(node_attrs, ATTR_CPU_MODEL, &cpu.model)?;
        if let Some(features) = cpu.features.as_deref() {
            require(node_attrs, ATTR_CPU_FEATURES, features)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn memory_manifest() -> SnapshotManifest {
        SnapshotManifest {
            api_version: MANIFEST_API_VERSION,
            id: "snap-7f3a91".into(),
            job_id: "agent-sandboxes".into(),
            task: "workspace".into(),
            origin_instance: Some("user-42".into()),
            origin_alloc_id: 17,
            runtime: "sandbox".into(),
            kind: SnapshotKind::Memory,
            created_at: "2026-10-10T09:14:03Z".into(),
            volumes: vec![VolumeSnapshotRef {
                id: "snap-7f3a91".into(),
                volume_id: "vol-9f2c".into(),
                driver: "local-dir".into(),
                mode: crate::volumes::VolumeMode::Block,
                attrs: BTreeMap::from([("path".to_string(), json!("/v/snap/disk.img"))]),
                size: 4096,
            }],
            memory: vec![MemoryPart {
                role: "memory".into(),
                file: "memory".into(),
                digest: "sha256:9c".into(),
                size: 1024,
                url: None,
                key_id: None,
            }],
            compat: SnapshotCompat {
                vmm: Some(VmmCompat {
                    kind: "firecracker".into(),
                    version: "1.7.0".into(),
                }),
                guest: Some(GuestCompat {
                    vcpus: 2,
                    memory_mib: 1024,
                }),
                cpu: Some(CpuCompat {
                    vendor: "GenuineIntel".into(),
                    model: "6:106:6".into(),
                    features: Some("3f9a0c21".into()),
                }),
            },
            runtime_state: json!({ "linkKey": "abc" }),
        }
    }

    fn matching_node() -> Map<String, Value> {
        json!({
            "sandbox.vmm": "firecracker",
            "sandbox.vmm.version": "1.7.0",
            "cpu.vendor": "GenuineIntel",
            "cpu.model": "6:106:6",
            "cpu.features": "3f9a0c21",
        })
        .as_object()
        .cloned()
        .unwrap()
    }

    #[test]
    fn manifest_round_trips_through_json_with_the_documented_field_names() -> anyhow::Result<()> {
        let manifest = memory_manifest();
        let raw = serde_json::to_string(&manifest)?;
        let value: Value = serde_json::from_str(&raw)?;
        assert_eq!(value["kind"], "memory");
        assert_eq!(value["compat"]["vmm"]["type"], "firecracker");
        assert_eq!(value["compat"]["cpu"]["model"], "6:106:6");
        assert_eq!(value["volumes"][0]["mode"], "block");
        assert_eq!(value["runtimeState"]["linkKey"], "abc");
        assert_eq!(SnapshotManifest::from_json(&raw)?, manifest);
        assert_eq!(manifest.total_bytes(), 4096 + 1024);
        Ok(())
    }

    #[test]
    fn an_unknown_manifest_version_is_refused() {
        let mut value = serde_json::to_value(memory_manifest()).unwrap();
        value["apiVersion"] = json!(99);
        let err = SnapshotManifest::from_json(&value.to_string()).unwrap_err();
        assert!(err.to_string().contains("version 99"), "{err}");
    }

    #[test]
    fn compat_matches_an_identical_node() {
        assert_eq!(compat_matches(&memory_manifest(), &matching_node()), Ok(()));
    }

    #[test]
    fn a_mismatched_cpu_model_names_the_attribute() {
        let mut node = matching_node();
        node.insert("cpu.model".into(), json!("6:143:8"));
        let err = compat_matches(&memory_manifest(), &node).unwrap_err();
        assert_eq!(err.attribute, "cpu.model");
        assert_eq!(err.found.as_deref(), Some("6:143:8"));
        assert!(err.to_string().contains("cpu.model"), "{err}");
    }

    #[test]
    fn a_missing_attribute_is_a_mismatch_not_a_pass() {
        let mut node = matching_node();
        node.remove("cpu.features");
        let err = compat_matches(&memory_manifest(), &node).unwrap_err();
        assert_eq!(err.attribute, "cpu.features");
        assert_eq!(err.found, None);
        assert!(err.to_string().contains("not reported"), "{err}");
    }

    #[test]
    fn vmm_version_must_match_exactly() {
        let mut node = matching_node();
        node.insert("sandbox.vmm.version".into(), json!("1.8.0"));
        let err = compat_matches(&memory_manifest(), &node).unwrap_err();
        assert_eq!(err.attribute, "sandbox.vmm.version");
    }

    #[test]
    fn a_non_default_vmm_matches_through_its_own_version_attribute() {
        let mut node = matching_node();
        node.insert("sandbox.vmm".into(), json!("cloud-hypervisor"));
        node.insert("sandbox.vmm.version".into(), json!("41.0"));
        node.insert("sandbox.vmm.firecracker.version".into(), json!("1.7.0"));
        assert_eq!(compat_matches(&memory_manifest(), &node), Ok(()));

        node.insert("sandbox.vmm.firecracker.version".into(), json!("1.6.0"));
        let err = compat_matches(&memory_manifest(), &node).unwrap_err();
        assert_eq!(err.attribute, "sandbox.vmm.firecracker.version");
    }

    #[test]
    fn a_disk_checkpoint_matches_every_node() {
        let mut manifest = memory_manifest();
        manifest.kind = SnapshotKind::Disk;
        manifest.memory.clear();
        manifest.compat = SnapshotCompat::default();
        assert_eq!(compat_matches(&manifest, &Map::new()), Ok(()));
    }

    #[test]
    fn cpu_compat_is_taken_from_node_attrs_only_when_complete() {
        let compat = SnapshotCompat::default().with_cpu_from_attrs(&matching_node());
        assert_eq!(compat.cpu.unwrap().features.as_deref(), Some("3f9a0c21"));

        let mut partial = matching_node();
        partial.remove("cpu.model");
        assert!(SnapshotCompat::default()
            .with_cpu_from_attrs(&partial)
            .cpu
            .is_none());
    }
}
