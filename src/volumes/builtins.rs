//! Built-in volume drivers — the zero-dependency path.
//!
//! These ship inside the Legion binary so a single-binary cluster has working
//! stateful storage with nothing installed. They implement the same
//! [`VolumeDriver`] trait the exec dispatcher does, so nothing downstream knows
//! whether a driver is in-process or a subprocess.
//!
//! | Driver | Node-local | Summary |
//! | --- | --- | --- |
//! | `local-dir` | yes | A directory under the worker's state dir. The default. |
//! | `host` | yes | Claims an operator-declared host path by name. |
//! | `nfs` | **no** | Mounts an NFS export; instances reschedule anywhere. |
//! | `tmpfs` | yes | RAM-backed scratch, wiped on unmount. Linux only. |

use std::path::{Path, PathBuf};

use crate::prelude::*;
use crate::volumes::{
    create_dir_with_mode, is_mount_point, run_mount_command, MountResult, VolumeCapabilities,
    VolumeDriver, VolumeMode, VolumeRequest, VolumeRuntimeConfig, ACCESS_MODE_MULTI_READER,
    ACCESS_MODE_SINGLE_WRITER,
};

/// Every built-in, constructed against one worker's runtime config.
pub fn all(config: &VolumeRuntimeConfig) -> Vec<Box<dyn VolumeDriver>> {
    vec![
        Box::new(LocalDirDriver::new(config.volume_root.clone())),
        Box::new(HostDriver::new(config.clone())),
        Box::new(NfsDriver::new(config.volume_root.clone())),
        Box::new(TmpfsDriver::new(config.volume_root.clone())),
    ]
}

// =====
// local-dir
// =====

/// The default driver: one directory per volume under
/// `<worker state dir>/volumes/local-dir/<volume_id>`, created on first mount
/// and removed on destroy.
///
/// Node-local by definition — the bytes live on one machine's disk, so an
/// instance holding one can only be replaced on that machine. `size` is
/// recorded as an attribute but **not enforced**: a directory has no quota, and
/// pretending otherwise would be worse than being honest about it.
pub struct LocalDirDriver {
    root: PathBuf,
}

impl LocalDirDriver {
    pub fn new(volume_root: PathBuf) -> Self {
        LocalDirDriver {
            root: volume_root.join("local-dir"),
        }
    }

    fn dir_for(&self, volume_id: &str) -> PathBuf {
        self.root.join(volume_id)
    }

    /// Block mode keeps the image inside the same per-volume directory, so
    /// `destroy` stays one `remove_dir_all` regardless of mode.
    fn image_for(&self, volume_id: &str) -> PathBuf {
        self.dir_for(volume_id).join("disk.img")
    }

    /// Create the backing image on first mount, formatted with the requested
    /// filesystem. Returns whether it created one — the `fresh` flag.
    ///
    /// **Never reformats.** An existing image is reattached as-is, which is the
    /// whole point of a persistent volume; the idempotency rule makes this
    /// mandatory rather than merely polite.
    fn ensure_image(&self, request: &VolumeRequest) -> R<(PathBuf, bool)> {
        let image = self.image_for(&request.volume.id);
        if image.is_file() {
            return Ok((image, false));
        }
        let dir = self.dir_for(&request.volume.id);
        create_dir_with_mode(&dir, request.param_str("mode").as_deref())?;

        let size = request.block_size_bytes()?;
        let fs = request.block_fs();
        let mkfs = mkfs_binary(&fs).with_context(|| {
            format!("no mkfs.{fs} on PATH; install it or pick a filesystem this node can format")
        })?;

        // A sparse file, so a 20G volume costs 20G of *address space* and only
        // the blocks actually written. Built in a temp name and renamed, so a
        // crash mid-format never leaves a half-formatted image that the
        // `is_file` check above would happily reattach.
        let staging = image.with_extension("img.new");
        let _ = std::fs::remove_file(&staging);
        let file = std::fs::File::create(&staging)
            .with_context(|| format!("failed to create disk image {}", staging.display()))?;
        file.set_len(size)
            .with_context(|| format!("failed to size disk image to {size} bytes"))?;
        drop(file);

        // `-F` because the target is a plain file rather than a block device.
        let output = std::process::Command::new(&mkfs)
            .arg("-F")
            .arg(&staging)
            .output()
            .with_context(|| format!("failed to run {}", mkfs.display()))?;
        if !output.status.success() {
            let _ = std::fs::remove_file(&staging);
            bail!(
                "mkfs.{fs} failed on {}: {}",
                staging.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        std::fs::rename(&staging, &image)
            .with_context(|| format!("failed to publish disk image {}", image.display()))?;
        info!(
            volume_id = %request.volume.id,
            fs = %fs,
            size_bytes = size,
            "Created block volume image"
        );
        Ok((image, true))
    }
}

impl VolumeDriver for LocalDirDriver {
    fn name(&self) -> &str {
        "local-dir"
    }

    fn capabilities(&self) -> VolumeCapabilities {
        // Block mode needs a `mkfs` for the filesystem being asked for. Rather
        // than guess, advertise it only when the default one is present — a
        // node without e2fsprogs says so instead of failing at task start.
        let modes = if mkfs_binary(crate::volumes::DEFAULT_BLOCK_FS).is_some() {
            vec![VolumeMode::Filesystem, VolumeMode::Block]
        } else {
            vec![VolumeMode::Filesystem]
        };
        let detail = if modes.len() == 2 {
            "Directory (or a disk image in block mode) on the worker's disk. Survives restarts; \
             pins the instance to this node."
        } else {
            "Directory on the worker's disk. Survives restarts; pins the instance to this node. \
             Block mode unavailable: no mkfs.ext4 on PATH."
        };
        VolumeCapabilities::new("local-dir", true)
            .with_params(&[], &["size", "mode", "fs"])
            .with_volume_modes(&modes)
            .with_detail(detail)
    }

    fn mount(&self, request: &VolumeRequest) -> R<MountResult> {
        if request.mode == VolumeMode::Block {
            let (image, fresh) = self.ensure_image(request)?;
            return Ok(MountResult::device(&image, request.block_fs())
                .fresh(fresh)
                .with_attr("size", request.block_size_bytes()?)
                .with_attr("fs", request.block_fs()));
        }

        let dir = self.dir_for(&request.volume.id);
        let fresh = !dir.exists();
        create_dir_with_mode(&dir, request.param_str("mode").as_deref())?;
        let mut result = MountResult::at(&dir).fresh(fresh);
        if let Some(size) = request.param_str("size") {
            // Recorded for the UI/CLI so an operator can see what was asked
            // for. Nothing enforces it — see the type docs.
            result = result.with_attr("size", size);
        }
        Ok(result)
    }

    fn unmount(&self, _request: &VolumeRequest) -> R<()> {
        // Nothing is mounted: the task read the directory directly.
        Ok(())
    }

    fn destroy(&self, request: &VolumeRequest) -> R<()> {
        let dir = self.dir_for(&request.volume.id);
        if !dir.exists() {
            return Ok(());
        }
        std::fs::remove_dir_all(&dir)
            .with_context(|| format!("failed to remove volume directory {}", dir.display()))?;
        Ok(())
    }
}

// =====
// host
// =====

/// Claims a path the operator declared in the worker config:
///
/// ```nickel
/// hostVolumes: {
///   models: { path: '/mnt/models', readOnly: true },
///   pgdata: { path: '/mnt/nvme0/pg', create: true },
/// }
/// ```
///
/// No provisioning, and **`destroy` is deliberately a no-op**: the data belongs
/// to the operator, not to Legion, so a scaled-in job must never delete
/// `/mnt/models`. This is the escape hatch for "the bytes are already on the
/// machine" and the way to put a database on a specific NVMe.
pub struct HostDriver {
    config: VolumeRuntimeConfig,
}

impl HostDriver {
    pub fn new(config: VolumeRuntimeConfig) -> Self {
        HostDriver { config }
    }
}

impl VolumeDriver for HostDriver {
    fn name(&self) -> &str {
        "host"
    }

    fn capabilities(&self) -> VolumeCapabilities {
        let declared = {
            let mut names = self.config.host_volumes.keys().cloned().collect::<Vec<_>>();
            names.sort();
            names
        };
        // Which modes this node can serve follows directly from what the
        // operator declared: a `device` entry serves block, a `path` entry
        // serves filesystem.
        let mut modes = Vec::new();
        if self
            .config
            .host_volumes
            .values()
            .any(|volume| volume.device.is_none())
        {
            modes.push(VolumeMode::Filesystem);
        }
        if self
            .config
            .host_volumes
            .values()
            .any(|volume| volume.device.is_some())
        {
            modes.push(VolumeMode::Block);
        }
        if modes.is_empty() {
            modes.push(VolumeMode::Filesystem);
        }
        let caps = VolumeCapabilities::new("host", true)
            .with_params(&["name"], &["readOnly"])
            .with_volume_modes(&modes)
            // Several tasks may mount the same host path read-only: there is
            // no per-instance lifecycle to serialize.
            .with_access_modes(&[ACCESS_MODE_SINGLE_WRITER, ACCESS_MODE_MULTI_READER]);
        if declared.is_empty() {
            return caps.unavailable(
                "no hostVolumes declared in this worker's config; add \
                 `hostVolumes: { <name>: { path: '/…' } }` to use this driver",
            );
        }
        caps.with_detail(format!("declared host volumes: {}", declared.join(", ")))
    }

    fn mount(&self, request: &VolumeRequest) -> R<MountResult> {
        let claim = request.require_param("name")?;
        let declared = self.config.host_volumes.get(&claim).with_context(|| {
            let mut available = self.config.host_volumes.keys().cloned().collect::<Vec<_>>();
            available.sort();
            format!(
                "this node declares no host volume named `{claim}` (declared: {})",
                if available.is_empty() {
                    "none".to_string()
                } else {
                    available.join(", ")
                }
            )
        })?;

        // A read-only declaration is a hard boundary, not a default: silently
        // handing out write access to a path the operator marked read-only
        // would defeat the point of declaring it.
        if declared.read_only && !request.volume.read_only {
            bail!(
                "host volume `{claim}` is declared readOnly on this node; \
                 mount it with `readOnly: true`"
            );
        }

        if request.mode == VolumeMode::Block {
            let device = declared.device.as_deref().with_context(|| {
                format!(
                    "host volume `{claim}` declares a `path`, not a `device`, so it cannot serve \
                     a block-mode claim. Declare `device: '/dev/…'` on this worker's \
                     hostVolumes entry"
                )
            })?;
            if !PathBuf::from(device).exists() {
                bail!("host volume `{claim}` points at device {device} which does not exist on this node");
            }
            let fs = declared
                .fs
                .clone()
                .unwrap_or_else(|| crate::volumes::DEFAULT_BLOCK_FS.to_string());
            // Never fresh and never formatted: the operator's device already
            // has whatever is on it, and mkfs-ing it would be catastrophic.
            return Ok(MountResult::device(device, fs).with_attr("hostDevice", device.to_string()));
        }

        if declared.device.is_some() {
            bail!(
                "host volume `{claim}` declares a `device`, so it only serves block-mode claims; \
                 declare a `path` for filesystem mode"
            );
        }
        let path = PathBuf::from(&declared.path);
        if declared.create {
            create_dir_with_mode(&path, None)?;
        }
        if !path.exists() {
            bail!(
                "host volume `{claim}` points at {} which does not exist on this node \
                 (set `create: true` on the worker's hostVolumes entry to create it)",
                path.display()
            );
        }

        Ok(MountResult::at(&path).with_attr("hostPath", declared.path.clone()))
    }

    fn unmount(&self, _request: &VolumeRequest) -> R<()> {
        Ok(())
    }

    fn destroy(&self, _request: &VolumeRequest) -> R<()> {
        // Never delete operator data. The claim goes away; the bytes stay.
        Ok(())
    }
}

// =====
// nfs
// =====

/// Mounts an NFS export and hands out a per-volume subdirectory inside it.
///
/// The first **non-node-local** driver, which is what makes it interesting:
/// because any node can reach the export, an instance holding an `nfs` volume
/// reschedules freely and the scheduler applies no pin. That is the difference
/// between "my database comes back on the same machine" and "my database comes
/// back".
///
/// The subdirectory (default: the volume id) is created *inside* the mounted
/// export, which sidesteps the chicken-and-egg of provisioning on a filesystem
/// you have to mount first.
pub struct NfsDriver {
    root: PathBuf,
}

impl NfsDriver {
    pub fn new(volume_root: PathBuf) -> Self {
        NfsDriver {
            root: volume_root.join("nfs"),
        }
    }

    fn mountpoint_for(&self, volume_id: &str) -> PathBuf {
        self.root.join(volume_id)
    }

    fn subdir(request: &VolumeRequest) -> String {
        request
            .param_str("subdir")
            .unwrap_or_else(|| request.volume.id.clone())
    }

    /// Mount the export at `mountpoint` unless it is already mounted.
    fn ensure_mounted(&self, request: &VolumeRequest, mountpoint: &std::path::Path) -> R<String> {
        let server = request.require_param("server")?;
        let export = request.require_param("export")?;
        let source = format!("{server}:{export}");

        std::fs::create_dir_all(mountpoint)
            .with_context(|| format!("failed to create NFS mountpoint {}", mountpoint.display()))?;
        if is_mount_point(mountpoint) {
            return Ok(source);
        }

        let mut options = request
            .param_str("options")
            .unwrap_or_else(|| default_nfs_options().to_string());
        if request.volume.read_only && !options.split(',').any(|opt| opt == "ro") {
            options.push_str(",ro");
        }

        let args = vec![
            "-t".to_string(),
            "nfs".to_string(),
            "-o".to_string(),
            options,
            source.clone(),
            mountpoint.display().to_string(),
        ];
        run_mount_command("mount", &args).with_context(|| {
            format!(
                "failed to mount NFS export {source} at {} (the worker usually needs to run as \
                 root for this)",
                mountpoint.display()
            )
        })?;
        Ok(source)
    }
}

/// macOS's NFS client rejects mounts from unprivileged source ports unless
/// told otherwise; Linux does not need the flag and does not accept it.
fn default_nfs_options() -> &'static str {
    if cfg!(target_os = "macos") {
        "resvport"
    } else {
        "vers=4"
    }
}

impl VolumeDriver for NfsDriver {
    fn name(&self) -> &str {
        "nfs"
    }

    fn capabilities(&self) -> VolumeCapabilities {
        let caps = VolumeCapabilities::new("nfs", false)
            .with_params(&["server", "export"], &["subdir", "options"])
            .with_volume_modes(&[VolumeMode::Filesystem])
            .with_detail(
                "NFS export mounted per volume. Not node-local: instances reschedule anywhere \
                 that can reach the server.",
            );
        if which("mount").is_none() {
            return caps.unavailable("no `mount` binary on PATH");
        }
        caps
    }

    fn mount(&self, request: &VolumeRequest) -> R<MountResult> {
        let mountpoint = self.mountpoint_for(&request.volume.id);
        let source = self.ensure_mounted(request, &mountpoint)?;
        let subdir = Self::subdir(request);
        let data_dir = mountpoint.join(&subdir);
        if !request.volume.read_only {
            create_dir_with_mode(&data_dir, request.param_str("mode").as_deref())?;
        } else if !data_dir.exists() {
            bail!(
                "NFS volume subdirectory {} does not exist on {source} and the volume is \
                 read-only",
                subdir
            );
        }
        Ok(MountResult::at(&data_dir)
            .with_attr("source", source)
            .with_attr("subdir", subdir))
    }

    fn unmount(&self, request: &VolumeRequest) -> R<()> {
        let mountpoint = self.mountpoint_for(&request.volume.id);
        if !is_mount_point(&mountpoint) {
            return Ok(());
        }
        run_mount_command("umount", &[mountpoint.display().to_string()])?;
        Ok(())
    }

    fn destroy(&self, request: &VolumeRequest) -> R<()> {
        // Destroy has to reach the bytes, which live on the server: mount the
        // export, remove just this volume's subdirectory, unmount again. The
        // export itself is never touched.
        let mountpoint = self.mountpoint_for(&request.volume.id);
        if !is_mount_point(&mountpoint) && self.ensure_mounted(request, &mountpoint).is_err() {
            // The server is unreachable. Leave the row `destroying` so the
            // reaper retries; `legion volume delete --force` is the operator's
            // out if it never comes back.
            bail!(
                "cannot reach the NFS server to destroy volume {}; retrying",
                request.volume.id
            );
        }
        let data_dir = mountpoint.join(Self::subdir(request));
        if data_dir.exists() {
            std::fs::remove_dir_all(&data_dir).with_context(|| {
                format!("failed to remove NFS volume data {}", data_dir.display())
            })?;
        }
        run_mount_command("umount", &[mountpoint.display().to_string()])?;
        let _ = std::fs::remove_dir(&mountpoint);
        Ok(())
    }
}

// =====
// tmpfs
// =====

/// RAM-backed scratch space, sized by the `size` param and wiped on unmount.
///
/// Advertised as **ephemeral**, which admission uses to reject it for
/// `stateful.storage`: the instance contract promises data survives a
/// reschedule and a tmpfs cannot keep that promise. It is the right answer for
/// a build cache, a scratch dir, or a Redis that is genuinely a cache.
///
/// Linux only — macOS has no tmpfs, and quietly substituting a disk-backed
/// directory would turn "this is in RAM and capped at 512M" into a lie.
pub struct TmpfsDriver {
    root: PathBuf,
}

impl TmpfsDriver {
    pub fn new(volume_root: PathBuf) -> Self {
        TmpfsDriver {
            root: volume_root.join("tmpfs"),
        }
    }

    fn mountpoint_for(&self, volume_id: &str) -> PathBuf {
        self.root.join(volume_id)
    }
}

const DEFAULT_TMPFS_SIZE: &str = "64M";

impl VolumeDriver for TmpfsDriver {
    fn name(&self) -> &str {
        "tmpfs"
    }

    fn capabilities(&self) -> VolumeCapabilities {
        let caps = VolumeCapabilities::new("tmpfs", true)
            .ephemeral()
            .with_params(&[], &["size", "mode"])
            .with_volume_modes(&[VolumeMode::Filesystem])
            .with_detail(
                "RAM-backed scratch space, wiped on unmount. Not valid for stateful.storage.",
            );
        if !cfg!(target_os = "linux") {
            return caps.unavailable("tmpfs mounts are Linux-only");
        }
        if which("mount").is_none() {
            return caps.unavailable("no `mount` binary on PATH");
        }
        caps
    }

    fn mount(&self, request: &VolumeRequest) -> R<MountResult> {
        if !cfg!(target_os = "linux") {
            bail!("the tmpfs volume driver is Linux-only");
        }
        let mountpoint = self.mountpoint_for(&request.volume.id);
        std::fs::create_dir_all(&mountpoint).with_context(|| {
            format!("failed to create tmpfs mountpoint {}", mountpoint.display())
        })?;
        if is_mount_point(&mountpoint) {
            return Ok(MountResult::at(&mountpoint));
        }
        let size = request
            .param_str("size")
            .unwrap_or_else(|| DEFAULT_TMPFS_SIZE.to_string());
        let mut options = format!("size={size}");
        if let Some(mode) = request.param_str("mode") {
            options.push_str(&format!(",mode={mode}"));
        }
        run_mount_command(
            "mount",
            &[
                "-t".to_string(),
                "tmpfs".to_string(),
                "-o".to_string(),
                options,
                "tmpfs".to_string(),
                mountpoint.display().to_string(),
            ],
        )
        .with_context(|| "mounting a tmpfs usually requires the worker to run as root")?;
        Ok(MountResult::at(&mountpoint).with_attr("size", size))
    }

    fn unmount(&self, request: &VolumeRequest) -> R<()> {
        let mountpoint = self.mountpoint_for(&request.volume.id);
        if is_mount_point(&mountpoint) {
            run_mount_command("umount", &[mountpoint.display().to_string()])?;
        }
        Ok(())
    }

    fn destroy(&self, request: &VolumeRequest) -> R<()> {
        self.unmount(request)?;
        let mountpoint = self.mountpoint_for(&request.volume.id);
        if mountpoint.exists() {
            let _ = std::fs::remove_dir(&mountpoint);
        }
        Ok(())
    }
}

/// The `mkfs.<fs>` binary for a filesystem, if this node has one. Drives both
/// the block-mode capability probe and the format itself, so what a driver
/// advertises and what it can actually do never diverge. Also the lookup the
/// sandbox kind uses to build its root disks, so "can this node format ext4"
/// has exactly one answer.
///
/// `mkfs.*` commonly lives in `/sbin` or `/usr/sbin`, which are not on a
/// non-root PATH — the well-known locations are probed too.
pub(crate) fn mkfs_binary(fs: &str) -> Option<PathBuf> {
    // A jobspec-supplied `fs` becomes part of a binary name, so constrain it
    // rather than letting `fs: "../../bin/sh"` pick the program.
    if fs.is_empty()
        || !fs
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '-')
    {
        return None;
    }
    let name = format!("mkfs.{fs}");
    which(&name).or_else(|| {
        ["/sbin", "/usr/sbin"]
            .iter()
            .map(|dir| Path::new(dir).join(&name))
            .find(|candidate| candidate.is_file())
    })
}

/// First match for `name` on `PATH`. Used by capability probes so a driver
/// reports itself unavailable up front instead of failing at task start.
fn which(name: &str) -> Option<PathBuf> {
    crate::util::find_on_path(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volumes::{HostVolumeConfig, VolumeNodeRef, VolumeOp, VolumeRef};
    use std::collections::BTreeMap;

    fn temp_root(label: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("legion-vol-{label}-{unique}"));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    fn request(op: VolumeOp, driver: &str, root: &std::path::Path) -> VolumeRequest {
        VolumeRequest::new(
            op,
            VolumeRef {
                id: "vol-test".into(),
                name: "data".into(),
                driver: driver.into(),
                attrs: BTreeMap::new(),
                read_only: false,
            },
            VolumeNodeRef {
                id: "node-1".into(),
                data_dir: root.to_path_buf(),
                volume_dir: root.join("volumes"),
            },
        )
    }

    #[test]
    fn local_dir_mounts_creates_and_destroys() -> R<()> {
        let root = temp_root("local-dir");
        let plugin = LocalDirDriver::new(root.join("volumes"));
        let req = request(VolumeOp::Mount, "local-dir", &root);

        let mounted = plugin.mount(&req)?;
        assert!(mounted
            .mount_path
            .clone()
            .expect("filesystem mount")
            .is_dir());
        std::fs::write(
            mounted
                .mount_path
                .clone()
                .expect("filesystem mount")
                .join("data.txt"),
            "hello",
        )?;

        // Idempotent: a second mount returns the same path with data intact.
        let again = plugin.mount(&req)?;
        assert_eq!(again.mount_path, mounted.mount_path);
        assert_eq!(
            std::fs::read_to_string(
                mounted
                    .mount_path
                    .clone()
                    .expect("filesystem mount")
                    .join("data.txt")
            )?,
            "hello"
        );

        plugin.unmount(&req)?;
        assert!(
            mounted
                .mount_path
                .clone()
                .expect("filesystem mount")
                .is_dir(),
            "unmount must keep the data"
        );

        plugin.destroy(&req)?;
        assert!(!mounted
            .mount_path
            .clone()
            .expect("filesystem mount")
            .exists());
        // Destroying twice is a no-op, not an error.
        plugin.destroy(&req)?;

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn local_dir_records_size_without_enforcing_it() -> R<()> {
        let root = temp_root("local-dir-size");
        let plugin = LocalDirDriver::new(root.join("volumes"));
        let mut params = BTreeMap::new();
        params.insert("size".to_string(), json!("10G"));
        let req = request(VolumeOp::Mount, "local-dir", &root).with_params(params);

        let mounted = plugin.mount(&req)?;
        assert_eq!(mounted.attrs.get("size"), Some(&json!("10G")));

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    /// Block mode needs a real `mkfs`, which not every dev machine has.
    /// Skipping loudly beats a test that silently never runs.
    fn block_mode_available() -> bool {
        if mkfs_binary(crate::volumes::DEFAULT_BLOCK_FS).is_some() {
            return true;
        }
        eprintln!("skipping block-mode test: no mkfs.ext4 on PATH");
        false
    }

    #[test]
    fn local_dir_block_mode_creates_formats_and_never_reformats() -> R<()> {
        if !block_mode_available() {
            return Ok(());
        }
        let root = temp_root("local-dir-block");
        let plugin = LocalDirDriver::new(root.join("volumes"));
        let mut params = BTreeMap::new();
        params.insert("size".to_string(), json!("16MiB"));
        let req = request(VolumeOp::Mount, "local-dir", &root)
            .with_params(params)
            .with_mode(VolumeMode::Block);

        let mounted = plugin.mount(&req)?;
        assert!(mounted.mount_path.is_none(), "block mode returns no path");
        let image = mounted.device_path.clone().expect("device path");
        assert!(
            image.is_file(),
            "the image is a plain file — no privileges needed"
        );
        assert_eq!(mounted.fs.as_deref(), Some("ext4"));
        assert!(mounted.fresh, "the first mount created it");

        // Stamp the image so a reformat is detectable, then remount.
        let before = std::fs::read(&image)?;
        let again = plugin.mount(&req)?;
        assert_eq!(again.device_path, mounted.device_path);
        assert!(!again.fresh, "a remount must not re-create the volume");
        assert_eq!(
            std::fs::read(&image)?,
            before,
            "remount reformatted an existing volume"
        );

        plugin.destroy(&req)?;
        assert!(!image.exists());

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn local_dir_block_mode_passes_the_contract_suite() {
        if !block_mode_available() {
            return;
        }
        let root = temp_root("local-dir-block-conformance");
        let plugin = LocalDirDriver::new(root.join("volumes"));
        let mut params = BTreeMap::new();
        params.insert("size".to_string(), json!("16MiB"));
        let report = crate::volumes::conformance::run(&plugin, &root, params, VolumeMode::Block);
        assert!(
            report.passed,
            "local-dir block mode failed conformance: {:?}",
            report.failures()
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn filesystem_only_plugins_refuse_block_mode() {
        // nfs and tmpfs hand out directories; admission and the worker both
        // gate on this rather than letting a microVM get a path it cannot use.
        assert!(!NfsDriver::new(PathBuf::from("/tmp/v"))
            .capabilities()
            .serves_mode(VolumeMode::Block));
        assert!(!TmpfsDriver::new(PathBuf::from("/tmp/v"))
            .capabilities()
            .serves_mode(VolumeMode::Block));
    }

    #[test]
    fn host_block_mode_needs_a_declared_device() -> R<()> {
        let root = temp_root("host-block");
        let mut host_volumes = HashMap::new();
        host_volumes.insert(
            "models".to_string(),
            HostVolumeConfig {
                path: root.display().to_string(),
                device: None,
                fs: None,
                read_only: false,
                create: false,
            },
        );
        let plugin = HostDriver::new(VolumeRuntimeConfig {
            volume_root: root.join("volumes"),
            data_dir: root.clone(),
            host_volumes,
            plugin_dir: root.join("plugins"),
        });
        // Only a `path` is declared, so this node serves filesystem mode only.
        assert!(!plugin.capabilities().serves_mode(VolumeMode::Block));

        let mut params = BTreeMap::new();
        params.insert("name".to_string(), json!("models"));
        let req = request(VolumeOp::Mount, "host", &root)
            .with_params(params)
            .with_mode(VolumeMode::Block);
        let err = plugin.mount(&req).unwrap_err().to_string();
        assert!(err.contains("declares a `path`, not a `device`"), "{err}");

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn host_plugin_requires_a_declared_claim() -> R<()> {
        let root = temp_root("host");
        let data = root.join("models");
        std::fs::create_dir_all(&data)?;
        let mut host_volumes = HashMap::new();
        host_volumes.insert(
            "models".to_string(),
            HostVolumeConfig {
                path: data.display().to_string(),
                device: None,
                fs: None,
                read_only: false,
                create: false,
            },
        );
        let plugin = HostDriver::new(VolumeRuntimeConfig {
            volume_root: root.join("volumes"),
            data_dir: root.clone(),
            host_volumes,
            plugin_dir: root.join("plugins"),
        });

        let mut params = BTreeMap::new();
        params.insert("name".to_string(), json!("models"));
        let req = request(VolumeOp::Mount, "host", &root).with_params(params);
        assert_eq!(plugin.mount(&req)?.mount_path, Some(data.clone()));

        // Destroy never removes operator data.
        plugin.destroy(&req)?;
        assert!(data.is_dir());

        let mut missing = BTreeMap::new();
        missing.insert("name".to_string(), json!("nope"));
        let bad = request(VolumeOp::Mount, "host", &root).with_params(missing);
        let err = plugin.mount(&bad).unwrap_err().to_string();
        assert!(
            err.contains("declares no host volume named `nope`"),
            "{err}"
        );

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn host_plugin_refuses_write_access_to_readonly_declarations() -> R<()> {
        let root = temp_root("host-ro");
        let data = root.join("models");
        std::fs::create_dir_all(&data)?;
        let mut host_volumes = HashMap::new();
        host_volumes.insert(
            "models".to_string(),
            HostVolumeConfig {
                path: data.display().to_string(),
                device: None,
                fs: None,
                read_only: true,
                create: false,
            },
        );
        let plugin = HostDriver::new(VolumeRuntimeConfig {
            volume_root: root.join("volumes"),
            data_dir: root.clone(),
            host_volumes,
            plugin_dir: root.join("plugins"),
        });

        let mut params = BTreeMap::new();
        params.insert("name".to_string(), json!("models"));
        let mut req = request(VolumeOp::Mount, "host", &root).with_params(params);
        let err = plugin.mount(&req).unwrap_err().to_string();
        assert!(err.contains("declared readOnly"), "{err}");

        req.volume.read_only = true;
        assert_eq!(plugin.mount(&req)?.mount_path, Some(data.clone()));

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn host_plugin_is_unavailable_without_declarations() {
        let plugin = HostDriver::new(VolumeRuntimeConfig::default());
        let caps = plugin.capabilities();
        assert!(!caps.available);
        assert!(caps.detail.contains("hostVolumes"));
    }

    #[test]
    fn nfs_is_not_node_local() {
        let plugin = NfsDriver::new(PathBuf::from("/tmp/volumes"));
        let caps = plugin.capabilities();
        assert!(!caps.node_local, "nfs volumes must not pin instances");
        assert_eq!(caps.required_params, vec!["server", "export"]);
    }

    #[test]
    fn tmpfs_declares_itself_ephemeral() {
        let plugin = TmpfsDriver::new(PathBuf::from("/tmp/volumes"));
        let caps = plugin.capabilities();
        assert!(caps.ephemeral);
        assert!(caps.node_local);
    }
}
