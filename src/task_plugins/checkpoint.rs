//! Runtime checkpoint types: what a task runtime can capture, where it writes
//! the bytes, and the identity disk a restored microVM learns who it is from.
//!
//! The manifest these feed lives in [`crate::structs::checkpoint`] so the
//! manager can read it without depending on worker code. See
//! `internal_docs/plans/suspend-resume-and-oversubscription-plan.md` §6.5.

use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::prelude::*;
use crate::structs::checkpoint::MemoryPart;

/// What a task runtime can capture for one task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckpointSupport {
    /// Nothing: the task has no snapshot-capable volume and the runtime
    /// cannot capture memory. Suspend means a cold start with empty state.
    None,
    /// Volume state only. Restore boots the task fresh on the captured (or
    /// retained) volume.
    Disk,
    /// Volume state plus guest memory and device state. Restore resumes the
    /// running guest.
    Memory,
}

impl CheckpointSupport {
    pub fn as_str(&self) -> &'static str {
        match self {
            CheckpointSupport::None => "none",
            CheckpointSupport::Disk => "disk",
            CheckpointSupport::Memory => "memory",
        }
    }
}

/// What the runtime does after a checkpoint is captured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AfterCheckpoint {
    /// Keep running (fork, manual snapshot). The guest sees a pause of a few
    /// hundred milliseconds.
    Resume,
    /// Leave the task stopped (suspend). The worker tears the allocation down
    /// afterward as usual.
    Stop,
    /// Leave the task paused after the capture. Worker core snapshots the
    /// volumes while nothing writes to them, then calls
    /// [`TaskPlugin::resume_after_checkpoint`](super::TaskPlugin::resume_after_checkpoint),
    /// so a live memory snapshot's memory and disk halves are one instant. A
    /// runtime that cannot pause treats it as [`AfterCheckpoint::Resume`] (its
    /// disk half is then crash-consistent).
    Pause,
}

impl AfterCheckpoint {
    pub fn as_str(&self) -> &'static str {
        match self {
            AfterCheckpoint::Resume => "resume",
            AfterCheckpoint::Stop => "stop",
            AfterCheckpoint::Pause => "pause",
        }
    }

    /// Parse the wire form (`CheckpointRequest.then`). Empty is `resume`.
    pub fn from_wire(raw: &str) -> R<Self> {
        match raw.trim() {
            "" | "resume" => Ok(AfterCheckpoint::Resume),
            "stop" => Ok(AfterCheckpoint::Stop),
            "pause" => Ok(AfterCheckpoint::Pause),
            other => bail!("unknown checkpoint follow-up {other:?} (expected resume or stop)"),
        }
    }
}

/// What [`TaskPlugin::checkpoint_support`](super::TaskPlugin::checkpoint_support)
/// is asked about one task.
#[derive(Debug, Clone, Copy)]
pub struct CheckpointQuery<'a> {
    /// The task's runtime config (`task.config`).
    pub config: &'a Value,
    /// True when the task has a stateful volume whose driver advertises
    /// `snapshot` (see [`crate::volumes::VolumeCapabilities::supports`]). On
    /// the worker that is the local driver's capabilities, on the manager the
    /// node fingerprint's `volumeDrivers`.
    pub snapshot_volume: bool,
    /// The VMM a sandbox with `hypervisor = "auto"` resolves to, when the
    /// caller already knows it (the manager reads the node's `sandbox.vmm`
    /// attribute). `None` resolves it on this machine.
    pub node_vmm: Option<&'a str>,
}

impl<'a> CheckpointQuery<'a> {
    pub fn new(config: &'a Value) -> Self {
        CheckpointQuery {
            config,
            snapshot_volume: false,
            node_vmm: None,
        }
    }

    pub fn with_snapshot_volume(mut self, snapshot_volume: bool) -> Self {
        self.snapshot_volume = snapshot_volume;
        self
    }

    pub fn with_node_vmm(mut self, vmm: Option<&'a str>) -> Self {
        self.node_vmm = vmm;
        self
    }
}

/// The directory a runtime writes a checkpoint's memory parts into, and reads
/// them back from on restore.
///
/// Until the node content store lands, this directory *is* the checkpoint's
/// storage: the worker creates it, the runtime fills it, and
/// [`CheckpointDir::seal`] records digests and sizes. When the content store
/// is wired in, the worker ingests the sealed parts by digest after
/// `checkpoint` and materializes them back into a fresh `CheckpointDir`
/// (hardlink or reflink by digest) before `restore`. The runtime never knows
/// the difference.
#[derive(Debug, Clone)]
pub struct CheckpointDir {
    path: PathBuf,
}

impl CheckpointDir {
    /// Create (or reuse) `path`, owner-only: a memory image is a RAM dump.
    pub fn create(path: impl Into<PathBuf>) -> R<Self> {
        let path = path.into();
        std::fs::create_dir_all(&path)
            .with_context(|| format!("failed to create checkpoint dir {}", path.display()))?;
        crate::util::restrict_dir_to_owner(&path)?;
        Ok(CheckpointDir { path })
    }

    /// Open an existing directory for restore.
    pub fn open(path: impl Into<PathBuf>) -> R<Self> {
        let path = path.into();
        if !path.is_dir() {
            bail!("checkpoint dir {} does not exist", path.display());
        }
        Ok(CheckpointDir { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Where the part stored as `file` lives.
    pub fn part_path(&self, file: &str) -> PathBuf {
        self.path.join(file)
    }

    /// Hash and size each `(role, file)` the runtime wrote. A file that does
    /// not exist is an error: the runtime promised it.
    pub fn seal(&self, parts: &[(&str, &str)]) -> R<Vec<MemoryPart>> {
        parts
            .iter()
            .map(|(role, file)| {
                let path = self.part_path(file);
                let (digest, size) = sha256_file(&path)?;
                Ok(MemoryPart {
                    role: (*role).to_string(),
                    file: (*file).to_string(),
                    digest,
                    size,
                    url: None,
                    key_id: None,
                })
            })
            .collect()
    }

    /// Check that every part is present at its recorded size, and, when
    /// `digests` is set, that its bytes hash to the recorded digest. Restore
    /// checks sizes always (cheap) and digests when the bytes came from
    /// somewhere they could have been damaged.
    pub fn verify(&self, parts: &[MemoryPart], digests: bool) -> R<()> {
        for part in parts {
            let path = self.part_path(&part.file);
            let size = std::fs::metadata(&path)
                .with_context(|| {
                    format!(
                        "checkpoint part {} ({}) is missing from {}",
                        part.role,
                        part.file,
                        self.path.display()
                    )
                })?
                .len();
            if size != part.size {
                bail!(
                    "checkpoint part {} is {size} bytes, the manifest says {}",
                    part.role,
                    part.size
                );
            }
            if digests {
                let (digest, _) = sha256_file(&path)?;
                if digest != part.digest {
                    bail!(
                        "checkpoint part {} hashes to {digest}, the manifest says {}",
                        part.role,
                        part.digest
                    );
                }
            }
        }
        Ok(())
    }
}

/// `sha256:<hex>` and length of a file, streamed.
pub fn sha256_file(path: &Path) -> R<(String, u64)> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024];
    let mut total = 0u64;
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        total += read as u64;
    }
    Ok((format!("sha256:{:x}", hasher.finalize()), total))
}

// ---------------------------------------------------------------------------
// Identity disk
// ---------------------------------------------------------------------------

/// The identity disk's format is shared with the guest, which reads it, so
/// it lives in the `legion-guest` crate (see `legion_guest::identity`).
pub use legion_guest::identity::{
    read_identity_disk, rewrite_identity_disk_in_place, write_identity_disk, IDENTITY_DISK_BYTES,
    IDENTITY_DISK_MAGIC, IDENTITY_DISK_VERSION,
};

/// Fresh entropy for a restored guest, hex-encoded. Forks of one snapshot
/// share an RNG state until something reseeds it; this is what the guest
/// agent mixes in.
pub fn fresh_entropy() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::checkpoint::ResumeIdentity;

    /// An in-place rewrite keeps the file (the inode a running VMM holds
    /// open) and reads back as the new identity.
    #[cfg(unix)]
    #[test]
    fn an_in_place_rewrite_keeps_the_open_file() -> R<()> {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("identity.img");
        let mut first = identity();
        first
            .env
            .insert("VAULT".into(), "a-much-longer-first-token".into());
        write_identity_disk(&path, &first)?;
        let inode = std::fs::metadata(&path)?.ino();
        let mut refreshed = first.clone();
        refreshed.env.insert("VAULT".into(), "short".into());
        rewrite_identity_disk_in_place(&path, &refreshed)?;
        assert_eq!(std::fs::metadata(&path)?.ino(), inode);
        assert_eq!(std::fs::metadata(&path)?.len(), IDENTITY_DISK_BYTES);
        assert_eq!(read_identity_disk(&path)?, refreshed);
        Ok(())
    }

    fn identity() -> ResumeIdentity {
        ResumeIdentity {
            alloc_id: 42,
            instance_id: Some("user-42".into()),
            job_id: "agents".into(),
            task_name: "workspace".into(),
            hostname: "legion-sbx-42".into(),
            token: Some("eyJ.secret".into()),
            mac: Some("06:00:ac:10:00:02".into()),
            env: [("REGION".to_string(), "us-west".to_string())].into(),
            entropy: fresh_entropy(),
            host_time_unix_ms: 1_760_000_000_000,
            on_resume: vec!["/bin/reconnect".into()],
            fork: false,
            files: Vec::new(),
        }
    }

    #[test]
    fn identity_disk_round_trips_at_a_fixed_size() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("identity.img");
        let written = identity();
        write_identity_disk(&path, &written)?;
        assert_eq!(std::fs::metadata(&path)?.len(), IDENTITY_DISK_BYTES);
        assert_eq!(read_identity_disk(&path)?, written);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path)?.permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "the disk can carry a token");
        }

        // Rewriting for a new allocation replaces the old identity whole.
        let mut next = identity();
        next.alloc_id = 43;
        next.token = None;
        write_identity_disk(&path, &next)?;
        assert_eq!(read_identity_disk(&path)?, next);
        Ok(())
    }

    #[test]
    fn identity_disk_rejects_foreign_bytes_and_oversized_payloads() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("not-identity.img");
        std::fs::write(&path, vec![0u8; 64])?;
        assert!(read_identity_disk(&path)
            .unwrap_err()
            .to_string()
            .contains("not a Legion identity disk"));

        let mut huge = identity();
        huge.env.insert("BIG".into(), "x".repeat(2 * 1024 * 1024));
        assert!(write_identity_disk(&tmp.path().join("big.img"), &huge).is_err());
        Ok(())
    }

    #[test]
    fn entropy_differs_per_call() {
        assert_ne!(fresh_entropy(), fresh_entropy());
        assert_eq!(fresh_entropy().len(), 64);
    }

    #[test]
    fn checkpoint_dir_seals_and_verifies_parts() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let dir = CheckpointDir::create(tmp.path().join("ckpt"))?;
        std::fs::write(dir.part_path("memory"), b"guest ram")?;
        std::fs::write(dir.part_path("vm-state"), b"devices")?;
        let parts = dir.seal(&[("memory", "memory"), ("vm-state", "vm-state")])?;
        assert_eq!(parts[0].size, 9);
        assert!(parts[0].digest.starts_with("sha256:"));
        dir.verify(&parts, true)?;

        std::fs::write(dir.part_path("memory"), b"guest RAM")?;
        let err = dir.verify(&parts, true).unwrap_err().to_string();
        assert!(err.contains("hashes to"), "{err}");
        dir.verify(&parts, false)?; // same size, so the cheap check passes

        std::fs::remove_file(dir.part_path("vm-state"))?;
        assert!(dir.verify(&parts, false).is_err());
        assert!(dir.seal(&[("x", "missing")]).is_err());
        Ok(())
    }

    #[test]
    fn checkpoint_support_orders_by_capability() {
        assert!(CheckpointSupport::Memory > CheckpointSupport::Disk);
        assert!(CheckpointSupport::Disk > CheckpointSupport::None);
        assert_eq!(
            serde_json::to_value(CheckpointSupport::Memory).unwrap(),
            json!("memory")
        );
    }
}
