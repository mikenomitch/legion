//! The `sandbox` task runtime: an OCI image running inside a **microVM**.
//!
//! `container` isolates with namespaces; `sandbox` isolates with a virtual
//! machine. The workload gets its own kernel, so a guest compromise has to
//! escape a hypervisor rather than a syscall filter — the isolation level you
//! want for untrusted code, agent workspaces, and multi-tenant build/exec
//! sandboxes.
//!
//! The user-facing shape is deliberately close to `container` (`image`,
//! `command`, `env`), plus what a VM needs and a container does not: guest
//! size (`vcpus`, `memory_mib`) and a kernel.
//!
//! # Stateful sandboxes
//!
//! Sandboxes are usually stateful (a dev workspace, a database, an agent's
//! home directory). Persistent state is the volume system's job, not this
//! runtime's: a stateful sandbox declares `stateful.storage` with
//! `mode: 'block'` (see docs/volumes.md), the volume driver serves a
//! formatted disk image bound to the **instance**, and the worker mounts it
//! before `start`. This kind only consumes the result:
//!
//! - **block volumes** become extra virtio-blk devices after the root disk
//!   (`/dev/vdb`, `/dev/vdc`, …), mounted by the generated guest init at the
//!   volume's declared `path`;
//! - **filesystem volumes** are for the dev-mode backends, which run where a
//!   host directory is consumable and bind-mount it. A microVM cannot use one
//!   (a guest kernel wants a block device) and says so at start.
//!
//! Instance binding, at-most-one-live-allocation, retention
//! (`retainOnScaleIn` / `retainOnStop`), node pinning, and reclamation are all
//! owned by the core + volume system; nothing here duplicates them.
//!
//! See `docs/task-kind-sandbox.md`.

use crate::artifacts::{StagedArtifact, StagedPayload};
use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::microvm::{self, Backend, GuestDiskMount, GuestPayload, TapNet, VmSpec};
use crate::task_plugins::{
    configure_task_log_capture, derived_schema, oci_bundle, oci_image, reap_task_process,
    spawn_pipe, spawn_pty, spawn_task_process, validate_schema, ExecOptions, ExecSession,
    ExecSupport, MountedVolume, TaskInstance, TaskPlugin,
};
use crate::volumes::VolumeMode;
use schemars::JsonSchema;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

const DEFAULT_VCPUS: u32 = 1;
const DEFAULT_MEMORY_MIB: u32 = 512;
const DEFAULT_ROOTFS_HEADROOM_MIB: u32 = 256;
/// Guest RAM below this cannot boot a Linux kernel with any userspace.
const MIN_MEMORY_MIB: u32 = 64;

fn default_sandbox_env() -> HashMap<String, String> {
    HashMap::new()
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct SandboxTaskConfig {
    /// Container image reference providing the sandbox's root filesystem.
    image: String,

    /// Command and arguments to run inside the sandbox.
    command: Vec<String>,

    /// Environment variables exported inside the sandbox.
    #[serde(default = "default_sandbox_env")]
    #[schemars(default = "default_sandbox_env")]
    env: HashMap<String, String>,

    /// Working directory inside the sandbox. Defaults to the image's.
    #[serde(default)]
    cwd: Option<String>,

    /// Virtual CPUs given to the microVM (default 1).
    #[serde(default)]
    vcpus: Option<u32>,

    /// Guest memory in MiB (default 512).
    #[serde(default)]
    memory_mib: Option<u32>,

    /// Writable space in MiB added to the ephemeral root disk on top of the
    /// image contents (default 256). Data written outside a volume mount
    /// lives here and is discarded when the allocation ends.
    #[serde(default)]
    rootfs_headroom_mib: Option<u32>,

    /// microVM monitor to use: `auto` (default), `cloud-hypervisor`,
    /// `firecracker`, or `dev` (no microVM; macOS Apple `container`, or a
    /// Linux OCI runtime when KVM is unavailable).
    #[serde(default)]
    hypervisor: Option<String>,

    /// Path to the uncompressed guest kernel. Defaults to the worker's
    /// `LEGION_SANDBOX_KERNEL`, then `/var/lib/legion/kernels/vmlinux`. An
    /// `${artifact.<name>}` reference works here.
    #[serde(default)]
    kernel: Option<String>,

    /// Extra arguments appended to the guest kernel command line.
    #[serde(default)]
    kernel_args: Option<String>,

    /// Guest network interface backed by a pre-created host TAP device.
    #[serde(default)]
    network: Option<SandboxNetworkConfig>,

    /// Additional plugin-specific fields reserved for forward compatibility.
    #[serde(default, flatten)]
    #[schemars(skip)]
    _extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SandboxNetworkConfig {
    /// `none` (default) or `tap` to attach the guest to a host TAP device.
    #[serde(default)]
    mode: Option<String>,

    /// Name of the pre-created host TAP device (required with `mode: tap`).
    /// Legion does not create TAP devices; an operator or network plugin does.
    #[serde(default)]
    tap: Option<String>,

    /// MAC address handed to the guest interface.
    #[serde(default)]
    mac: Option<String>,
}

impl SandboxTaskConfig {
    fn from_value(config: &Value) -> R<Self> {
        let parsed: Self = serde_json::from_value(config.clone())
            .context("sandbox task config must be an object")?;
        parsed.validate()?;
        Ok(parsed)
    }

    fn validate(&self) -> R<()> {
        if self.command.is_empty() {
            bail!("sandbox task command must include at least one entry");
        }
        // `state` was this runtime's pre-volume-system storage field. The flatten
        // catch-all would silently swallow it, and a sandbox that quietly runs
        // WITHOUT the persistence its spec asked for is the worst outcome.
        if self._extra.contains_key("state") {
            bail!(
                "sandbox `config.state` no longer exists: persistent state is a volume now. \
                 Declare `stateful: {{ storage: {{ path: '…', mode: 'block' }} }}` on the task \
                 instead (docs/volumes.md)"
            );
        }
        if self.vcpus == Some(0) {
            bail!("sandbox task vcpus must be at least 1");
        }
        if let Some(memory) = self.memory_mib {
            if memory < MIN_MEMORY_MIB {
                bail!("sandbox task memory_mib must be at least {MIN_MEMORY_MIB}");
            }
        }
        if let Some(network) = self.network.as_ref() {
            match network.mode.as_deref().unwrap_or("none") {
                "none" => {}
                "tap" => {
                    if network
                        .tap
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .is_none()
                    {
                        bail!("sandbox task network.mode `tap` requires network.tap (the host TAP device name)");
                    }
                }
                other => bail!("sandbox task network.mode must be `none` or `tap`, got `{other}`"),
            }
        }
        Ok(())
    }

    fn vcpus(&self) -> u32 {
        self.vcpus.unwrap_or(DEFAULT_VCPUS)
    }

    fn memory_mib(&self) -> u32 {
        self.memory_mib.unwrap_or(DEFAULT_MEMORY_MIB)
    }

    fn rootfs_headroom_mib(&self) -> u32 {
        self.rootfs_headroom_mib
            .unwrap_or(DEFAULT_ROOTFS_HEADROOM_MIB)
    }

    fn tap(&self) -> Option<TapNet> {
        let network = self.network.as_ref()?;
        if network.mode.as_deref().unwrap_or("none") != "tap" {
            return None;
        }
        Some(TapNet {
            device: network.tap.clone()?,
            mac: network.mac.clone(),
        })
    }
}

/// The task's `hypervisor` selection, read without failing on an otherwise
/// invalid config: `stop` and `exec` must resolve the same backend `start`
/// used, but neither is a good place to reject a bad spec.
fn configured_backend(task: &TaskConfig) -> Option<String> {
    task.config
        .get("hypervisor")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Deterministic per-allocation sandbox name (VM name, guest hostname, and the
/// dev-mode container name).
fn sandbox_name(allocation: &Allocation) -> String {
    format!("legion-sbx-{}", allocation.id)
}

/// Per-allocation scratch: image layout, extracted rootfs, root disk, and the
/// VMM control socket. Removed on destroy.
fn work_root(allocation: &Allocation, instance: &TaskInstance) -> PathBuf {
    instance
        .alloc_dir
        .clone()
        .or_else(|| instance.task_dir.clone())
        .unwrap_or_else(|| std::env::temp_dir().join(format!("legion-sandbox-{}", allocation.id)))
        .join(".sandbox")
}

/// Bring a host TAP device administratively up or down.
///
/// This is the mechanism behind the `boot` gate (docs/edge-boot.md): a guest
/// that has booted but is not yet ratified sits behind a shut link, and
/// ratification is the only thing that raises it. The device itself is
/// operator-precreated — Legion never creates or destroys TAP devices, it only
/// changes their link state, so a failure here is an operator's capability
/// problem (`CAP_NET_ADMIN`) rather than a missing device.
///
/// Deliberately shells out to `ip` rather than talking netlink: it is the same
/// tool an operator debugging the gate would reach for, its failure text is
/// the failure text they would see by hand, and the call happens twice per
/// sandbox lifetime rather than on any hot path.
fn set_tap_link(device: &str, open: bool) -> R<()> {
    if !cfg!(target_os = "linux") {
        bail!("the `boot` network gate needs Linux TAP link control; this host is not Linux");
    }
    let state = if open { "up" } else { "down" };
    let output = Command::new("ip")
        .args(["link", "set", "dev", device, state])
        .output()
        .with_context(|| format!("failed to run `ip link set dev {device} {state}`"))?;
    if !output.status.success() {
        bail!(
            "`ip link set dev {device} {state}` failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    debug!(device, state, "Set sandbox TAP link state");
    Ok(())
}

/// Path-safe form of an id used as a directory name. Instance ids are already
/// constrained to `[A-Za-z0-9._-]` by the core, but a task name is not, and
/// neither may ever produce a traversal.
fn path_safe(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() || sanitized.chars().all(|c| c == '.') {
        return "_".to_string();
    }
    sanitized
}

/// The block-mode volumes a microVM attaches, split into VMM disks and their
/// guest mounts. Attach order — and therefore guest device naming — is the
/// worker's mount order, which preserves the jobspec's declaration order
/// (instance volume first, then `volumes[]`).
#[derive(Debug)]
struct GuestVolumeDisks {
    disks: Vec<microvm::VmDisk>,
    mounts: Vec<GuestDiskMount>,
}

/// Partitions the worker-mounted volumes for a microVM boot. Every volume
/// must be block mode: the guest kernel consumes virtio-blk devices, and a
/// host directory is unreachable from inside a VM (until virtio-fs support).
fn guest_volume_disks(volumes: &[MountedVolume]) -> R<GuestVolumeDisks> {
    let mut disks = Vec::new();
    let mut mounts = Vec::new();
    for (index, volume) in volumes.iter().enumerate() {
        if volume.mode != VolumeMode::Block {
            bail!(
                "sandbox volume `{}` is a {} volume, which a microVM cannot consume: a guest \
                 kernel attaches block devices, not host directories. Declare the volume with \
                 `mode: 'block'` (docs/volumes.md), or run this task with `hypervisor: 'dev'` \
                 on a node where dev mode can bind-mount it",
                volume.name,
                volume.mode,
            );
        }
        disks.push(microvm::VmDisk {
            path: volume.host_path.clone(),
            read_only: volume.read_only,
        });
        mounts.push(GuestDiskMount {
            device: microvm::guest_volume_device(index),
            path: volume.path.clone(),
            fs: volume
                .fs
                .clone()
                .unwrap_or_else(|| crate::volumes::DEFAULT_BLOCK_FS.to_string()),
            read_only: volume.read_only,
        });
    }
    Ok(GuestVolumeDisks { disks, mounts })
}

/// Dev-mode backends bind-mount filesystem volumes; a block volume has no
/// consumable shape there (no VMM to attach it to, and loop-mounting a raw
/// image is a privileged host operation this kind refuses to do implicitly).
fn reject_block_volumes_in_dev_mode(volumes: &[MountedVolume], backend: &Backend) -> R<()> {
    if let Some(volume) = volumes
        .iter()
        .find(|volume| volume.mode == VolumeMode::Block)
    {
        bail!(
            "sandbox volume `{}` is a block volume, but this task is running on {} (dev mode), \
             which cannot attach block devices. Use a filesystem volume (the default `mode`) \
             for dev-mode sandboxes, or run on a Linux node with KVM",
            volume.name,
            backend.label(),
        );
    }
    Ok(())
}

/// Best-effort removal of a dev-mode container left behind under this
/// allocation's name — `run --name` fails outright when the name is taken,
/// which happens whenever a previous start was not torn down cleanly (worker
/// crash, or a `--rm` run killed before the runtime could reap it). Mirrors
/// the container runtime's `remove_stale_container`; the microVM backends need
/// no equivalent, since their VM dies with the VMM child process. Errors are
/// ignored: "no such container" is the normal case.
fn remove_stale_dev_sandbox(backend: &Backend, name: &str) {
    let (program, args): (String, Vec<&str>) = match backend {
        Backend::AppleContainer => ("container".to_string(), vec!["rm", "--force", name]),
        Backend::OciRuntime { binary } => (binary.clone(), vec!["delete", "--force", name]),
        Backend::CloudHypervisor { .. } | Backend::Firecracker { .. } => return,
    };
    let _ = Command::new(program).args(&args).output();
}

/// Copies staged file/tree artifacts into the guest filesystem at
/// `/legion/artifacts/<name>`, returning `(host path, guest path)` pairs.
///
/// Unlike a container — whose isolated filesystem cannot see the host task dir
/// at all — a sandbox builds its own root disk, so staged artifacts can simply
/// be written into it. The pairs let the caller rewrite host paths that
/// `${artifact.<name>}` interpolation already substituted into the command.
fn install_artifacts(rootfs: &Path, staged: &[StagedArtifact]) -> R<Vec<(String, String)>> {
    let mut mapped = Vec::new();
    for artifact in staged {
        let Some(name) = artifact.name.as_deref() else {
            continue;
        };
        // The image artifact *is* this rootfs; copying it into itself would be
        // both circular and enormous.
        if matches!(artifact.payload, StagedPayload::Image { .. }) {
            continue;
        }
        let host_ref = artifact.reference_path();
        let dir_name = path_safe(name);
        let guest_dir = format!("{}/{dir_name}", microvm::GUEST_ARTIFACT_DIR);
        let host_dir = rootfs.join(guest_dir.trim_start_matches('/'));
        if host_dir.exists() {
            std::fs::remove_dir_all(&host_dir)
                .with_context(|| format!("clear artifact dir {}", host_dir.display()))?;
        }
        std::fs::create_dir_all(&host_dir)
            .with_context(|| format!("create artifact dir {}", host_dir.display()))?;

        let guest_ref = match &artifact.payload {
            StagedPayload::File { path } => {
                let file_name = path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|| dir_name.clone());
                std::fs::copy(path, host_dir.join(&file_name)).with_context(|| {
                    format!("copy artifact {name} into the sandbox root filesystem")
                })?;
                format!("{guest_dir}/{file_name}")
            }
            StagedPayload::Tree { dir } | StagedPayload::Layers { dir, .. } => {
                copy_tree(dir, &host_dir)
                    .with_context(|| format!("copy artifact {name} into the sandbox"))?;
                guest_dir
            }
            StagedPayload::Image { .. } => continue,
        };
        mapped.push((host_ref.display().to_string(), guest_ref));
    }
    Ok(mapped)
}

/// Recursive directory copy that does not follow symlinks (a link is recreated
/// as a link, so a staged tree cannot smuggle host files into the guest).
fn copy_tree(source: &Path, dest: &Path) -> R<()> {
    std::fs::create_dir_all(dest).with_context(|| format!("create {}", dest.display()))?;
    for entry in std::fs::read_dir(source).with_context(|| format!("read {}", source.display()))? {
        let entry = entry?;
        let from = entry.path();
        let to = dest.join(entry.file_name());
        let metadata = std::fs::symlink_metadata(&from)?;
        if metadata.is_dir() {
            copy_tree(&from, &to)?;
        } else if metadata.is_symlink() {
            #[cfg(unix)]
            {
                let target = std::fs::read_link(&from)?;
                let _ = std::fs::remove_file(&to);
                std::os::unix::fs::symlink(target, &to)?;
            }
        } else {
            std::fs::copy(&from, &to)
                .with_context(|| format!("copy {} -> {}", from.display(), to.display()))?;
        }
    }
    Ok(())
}

/// Applies `${artifact.*}` host→guest path rewrites to a command line. The
/// worker interpolated host paths before start; inside the guest those paths
/// do not exist, but the same content does — under `/legion/artifacts`.
fn rewrite_artifact_paths(command: &[String], mapped: &[(String, String)]) -> Vec<String> {
    command
        .iter()
        .map(|arg| {
            let mut rewritten = arg.clone();
            for (host, guest) in mapped {
                if rewritten.contains(host.as_str()) {
                    rewritten = rewritten.replace(host.as_str(), guest);
                }
            }
            rewritten
        })
        .collect()
}

/// Fingerprint of everything baked into the root disk. Rebuilding a
/// multi-hundred-MiB ext4 image on every restart of a crash-looping sandbox
/// would dominate its restart time, so an unchanged fingerprint reuses the
/// image built by the previous start. Env values may be secrets — they are
/// hashed, never stored or logged.
fn build_stamp(layout: &Path, payload: &GuestPayload, artifacts: &[StagedArtifact]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(layout.display().to_string().as_bytes());
    for arg in &payload.command {
        hasher.update(b"\0arg\0");
        hasher.update(arg.as_bytes());
    }
    for (key, value) in &payload.env {
        hasher.update(b"\0env\0");
        hasher.update(key.as_bytes());
        hasher.update(b"=");
        hasher.update(value.as_bytes());
    }
    hasher.update(b"\0cwd\0");
    hasher.update(payload.cwd.as_deref().unwrap_or("").as_bytes());
    for mount in &payload.disk_mounts {
        hasher.update(b"\0disk\0");
        hasher.update(mount.device.as_bytes());
        hasher.update(b"@");
        hasher.update(mount.path.as_deref().unwrap_or("").as_bytes());
        hasher.update(b"@");
        hasher.update(mount.fs.as_bytes());
        hasher.update(if mount.read_only { b"@ro" } else { b"@rw" });
    }
    for artifact in artifacts {
        hasher.update(b"\0artifact\0");
        hasher.update(artifact.reference_path().display().to_string().as_bytes());
        hasher.update(artifact.digest.as_deref().unwrap_or("").as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

/// Builds the dev-mode Apple `container` launch (macOS). Same shape as the
/// container runtime's, plus the sandbox's guest sizing, with filesystem volumes
/// bind-mounted where the microVM path would attach block devices.
fn build_apple_launch(
    name: &str,
    config: &SandboxTaskConfig,
    env: &BTreeMap<String, String>,
    volumes: &[MountedVolume],
) -> Vec<String> {
    let mut args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--name".to_string(),
        name.to_string(),
    ];
    // Only forwarded when the jobspec asked for them: the microVM defaults are
    // Legion's, and silently reshaping a dev-mode container is worse than
    // letting the backend pick.
    if let Some(vcpus) = config.vcpus {
        args.push("--cpus".to_string());
        args.push(vcpus.to_string());
    }
    if let Some(memory) = config.memory_mib {
        args.push("--memory".to_string());
        args.push(format!("{memory}M"));
    }
    // Docker-compatible `--volume host:dest[:ro]`, exactly like the container
    // kind. Volumes with no declared path have no mountable destination.
    for volume in volumes {
        let Some(destination) = volume.path.as_deref() else {
            continue;
        };
        args.push("--volume".to_string());
        let suffix = if volume.read_only { ":ro" } else { "" };
        args.push(format!(
            "{}:{destination}{suffix}",
            volume.host_path.display()
        ));
    }
    if let Some(cwd) = config.cwd.as_deref().filter(|cwd| !cwd.is_empty()) {
        args.push("--workdir".to_string());
        args.push(cwd.to_string());
    }
    for (key, value) in env {
        args.push("--env".to_string());
        args.push(format!("{key}={value}"));
    }
    args.push(config.image.clone());
    args.push("--".to_string());
    args.extend(config.command.clone());
    args
}

/// Builds the argv that execs `command` inside a running dev-mode sandbox.
fn build_exec_command(name: &str, tty: bool, command: &[String]) -> Vec<String> {
    let mut argv = vec!["container".to_string(), "exec".to_string()];
    if tty {
        argv.push("--tty".to_string());
        argv.push("--interactive".to_string());
    }
    argv.push(name.to_string());
    argv.extend(command.iter().cloned());
    argv
}

#[derive(Default)]
pub struct SandboxTaskPlugin;

impl SandboxTaskPlugin {
    /// Boots the real thing: build the guest root disk from the image, attach
    /// the worker-mounted block volumes, then hand the VM to the monitor.
    fn start_microvm(
        &self,
        hypervisor: &Backend,
        allocation: &Allocation,
        task: &TaskConfig,
        config: &SandboxTaskConfig,
        env: &HashMap<String, String>,
        instance: &mut TaskInstance,
    ) -> R<Command> {
        let scratch = work_root(allocation, instance);
        let name = sandbox_name(allocation);
        let kernel = microvm::resolve_kernel(config.kernel.as_deref())?;
        let layout = oci_image::resolve_layout(&config.image, instance, &scratch)?;

        // Volumes were mounted by the worker before `prepare` (docs/volumes.md);
        // a microVM consumes the block-mode ones as extra virtio-blk devices.
        let volumes = guest_volume_disks(&instance.volumes)?;
        for (volume, mount) in instance.volumes.iter().zip(&volumes.mounts) {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                volume = %volume.name,
                device = %mount.device,
                guest_path = mount.path.as_deref().unwrap_or("<attach-only>"),
                fresh = volume.fresh,
                "Attaching volume to sandbox"
            );
        }

        let rootfs_dir = scratch.join("rootfs");
        let root_disk = scratch.join("rootfs.img");
        let stamp_path = scratch.join("rootfs.stamp");

        let image_config = oci_bundle::read_image_config(&layout)?;
        let mut guest_env = oci_bundle::merge_env_map(&image_config.env, env);
        guest_env.extend(instance.volume_env());
        let payload_cwd = config
            .cwd
            .clone()
            .or_else(|| image_config.working_dir.clone());
        let mut payload = GuestPayload {
            command: config.command.clone(),
            env: guest_env,
            cwd: payload_cwd,
            disk_mounts: volumes.mounts,
        };

        let staged = instance.staged_artifacts.clone();
        let stamp = build_stamp(&layout, &payload, &staged);
        let reusable = root_disk.is_file()
            && std::fs::read_to_string(&stamp_path)
                .map(|previous| previous.trim() == stamp)
                .unwrap_or(false);

        if reusable {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                "Reusing sandbox root disk from the previous start"
            );
        } else {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                image = %config.image,
                "Building sandbox root disk"
            );
            let image = oci_bundle::materialize_rootfs(&layout, &rootfs_dir)?;
            if let Some(user) = image.user.as_deref() {
                // In a microVM the isolation boundary is the VM, so the payload
                // runs as root in the guest; say so rather than silently
                // ignoring the image's declared user.
                debug!(
                    allocation_id = allocation.id,
                    image_user = user,
                    "sandbox runs the guest payload as root; image user is not applied"
                );
            }
            let mapped = install_artifacts(&rootfs_dir, &staged)?;
            if !mapped.is_empty() {
                payload.command = rewrite_artifact_paths(&payload.command, &mapped);
                for (host, guest) in &mapped {
                    for value in payload.env.values_mut() {
                        if value.contains(host.as_str()) {
                            *value = value.replace(host.as_str(), guest);
                        }
                    }
                }
            }
            microvm::install_guest_payload(&rootfs_dir, &payload, &name)?;

            let used = microvm::dir_size_bytes(&rootfs_dir)?;
            let size = microvm::rootfs_image_bytes(used, config.rootfs_headroom_mib() as u64);
            microvm::create_ext4_image(&root_disk, size, Some(&rootfs_dir), "legion-root")?;
            std::fs::write(&stamp_path, &stamp)
                .with_context(|| format!("write {}", stamp_path.display()))?;
            // The extracted tree has served its purpose; the disk image is the
            // artifact from here on, and keeping both doubles the footprint.
            let _ = std::fs::remove_dir_all(&rootfs_dir);
        }

        let spec = VmSpec {
            name: name.clone(),
            kernel,
            extra_cmdline: config.kernel_args.clone(),
            root_disk,
            volume_disks: volumes.disks,
            vcpus: config.vcpus(),
            memory_mib: config.memory_mib(),
            tap: config.tap(),
            api_socket: scratch.join("vmm.sock"),
        };
        // A stale socket from a previous boot makes the VMM refuse to start.
        let _ = std::fs::remove_file(&spec.api_socket);

        match hypervisor {
            Backend::CloudHypervisor { binary } => {
                let mut cmd = Command::new(binary);
                cmd.args(microvm::cloud_hypervisor_args(&spec));
                Ok(cmd)
            }
            Backend::Firecracker { binary } => {
                let config_path = scratch.join("firecracker.json");
                std::fs::write(
                    &config_path,
                    serde_json::to_vec_pretty(&microvm::firecracker_config(&spec))?,
                )
                .with_context(|| format!("write {}", config_path.display()))?;
                let mut cmd = Command::new(binary);
                // `--id` labels the VM in Firecracker's own logs/metrics;
                // `--no-api` boots straight from the config with no API dance.
                cmd.arg("--id")
                    .arg(&spec.name)
                    .arg("--no-api")
                    .arg("--config-file")
                    .arg(&config_path);
                Ok(cmd)
            }
            other => bail!("{} is not a microVM monitor", other.label()),
        }
    }

    /// macOS dev mode: Apple `container` (still VM-isolated, but Apple owns
    /// the kernel and disks, so filesystem volumes become bind mounts).
    fn start_apple_container(
        &self,
        allocation: &Allocation,
        config: &SandboxTaskConfig,
        env: &HashMap<String, String>,
        instance: &TaskInstance,
        backend: &Backend,
    ) -> R<Command> {
        reject_block_volumes_in_dev_mode(&instance.volumes, backend)?;
        let name = sandbox_name(allocation);
        remove_stale_dev_sandbox(backend, &name);
        let mut env: BTreeMap<String, String> = env.clone().into_iter().collect();
        env.extend(instance.volume_env());

        let mut cmd = Command::new("container");
        cmd.args(build_apple_launch(&name, config, &env, &instance.volumes));
        Ok(cmd)
    }

    /// Linux dev mode without KVM: an OCI runtime bundle. Namespace isolation
    /// only — the guest kernel is the host kernel.
    fn start_oci_runtime(
        &self,
        binary: &str,
        allocation: &Allocation,
        config: &SandboxTaskConfig,
        env: &HashMap<String, String>,
        instance: &TaskInstance,
        backend: &Backend,
    ) -> R<Command> {
        reject_block_volumes_in_dev_mode(&instance.volumes, backend)?;
        let scratch = work_root(allocation, instance);
        let name = sandbox_name(allocation);
        remove_stale_dev_sandbox(backend, &name);
        let layout = oci_image::resolve_layout(&config.image, instance, &scratch)?;

        let mut env = env.clone();
        env.extend(instance.volume_env());

        let bundle = scratch.join("bundle");
        oci_bundle::build_bundle(
            &layout,
            &bundle,
            &config.command,
            &env,
            &name,
            &instance.volumes,
        )?;
        let mut cmd = Command::new(binary);
        cmd.arg("run").arg("--bundle").arg(&bundle).arg(&name);
        Ok(cmd)
    }
}

impl TaskPlugin for SandboxTaskPlugin {
    fn name(&self) -> &'static str {
        "sandbox"
    }

    /// The tracked process is the VMM; killing it takes the guest with it, and
    /// its exit is the guest's exit.
    fn supervises_process(&self) -> bool {
        true
    }

    fn schema(&self) -> Value {
        derived_schema::<SandboxTaskConfig>()
    }

    /// Runs the typed parse, not just the derived JSON schema.
    ///
    /// The config carries a `#[serde(flatten)]` catch-all for forward
    /// compatibility, which makes the derived schema accept *any* extra
    /// property — so a schema-only check would pass a retired `state` block, a
    /// guest too small to boot, or `network.mode: 'bridge'`, and the job would
    /// only fail when a task tried to start on some node. Parsing here moves
    /// every check in [`SandboxTaskConfig::validate`] to submit time, which is
    /// where an agent or a human can still act on it.
    fn validate_config(&self, config: &Value) -> R<()> {
        validate_schema(&self.schema(), config)?;
        SandboxTaskConfig::from_value(config)?;
        Ok(())
    }

    /// Logs the backend this node resolved to. Workers load every built-in
    /// runtime at startup, so this is where an operator learns "sandboxes here
    /// run on cloud-hypervisor" — or why they can't — without submitting a job.
    fn setup(&self) -> R<()> {
        match Backend::detect(None) {
            Ok(backend) => info!(
                backend = backend.label(),
                microvm = backend.is_microvm(),
                "Sandbox task runtime ready"
            ),
            Err(err) => warn!(
                error = %err,
                "Sandbox task runtime loaded, but this node cannot start sandboxes yet"
            ),
        }
        Ok(())
    }

    fn readiness(&self) -> Map<String, Value> {
        let mut map = Map::new();
        // Node health is the AND of every readiness value, and any value other
        // than "pass" marks the whole node unhealthy (`derive_node_health` in
        // src/manager/server.rs), which in turn gates rollouts. So this reports
        // *plugin* readiness only, never backend availability: workers load
        // this kind unconditionally, and reporting "no hypervisor here" would
        // fail deploys on every node that never runs a sandbox. A task that
        // cannot boot on this node fails at start with an actionable error.
        map.insert("sandbox".into(), Value::String("pass".into()));
        map
    }

    fn supports_kv_env_injection(&self) -> bool {
        true
    }

    // File delivery into the guest happens at root-disk build time, so a
    // *changed* file needs a restart to land — the same contract the container
    // kind has for env. Until KV file injection can be refreshed live, only
    // env injection is advertised.
    fn supports_identity_env(&self) -> bool {
        true
    }

    fn supports_artifacts(&self) -> bool {
        true
    }

    /// Both modes, because the runtime spans two worlds (docs/volumes.md):
    /// **block** is the microVM shape — the driver's device or disk image is
    /// attached as virtio-blk and mounted by the guest init — and
    /// **filesystem** is the dev-mode shape, bind-mounted by the Apple
    /// `container` / OCI-runtime backends exactly like the container kind.
    /// A backend handed the mode it cannot consume fails at start with an
    /// error naming the fix; placement usually prevents that pairing, since a
    /// node without `mkfs` never advertises block-capable `local-dir`.
    fn supported_volume_modes(&self) -> &'static [VolumeMode] {
        &[VolumeMode::Block, VolumeMode::Filesystem]
    }

    fn exec_support(&self) -> ExecSupport {
        // Exec into a microVM needs a vsock guest agent (deferred); dev mode
        // borrows Apple `container exec`. Either way the session is sandboxed,
        // never host-level.
        let supported = Backend::detect(None)
            .map(|hypervisor| hypervisor.supports_exec())
            .unwrap_or(false);
        ExecSupport {
            exec: supported,
            pty: supported,
            host_level: false,
        }
    }

    fn exec(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _runtime: &mut TaskInstance,
        mut opts: ExecOptions,
    ) -> R<Box<dyn ExecSession>> {
        let hypervisor = Backend::detect(configured_backend(task).as_deref())?;
        if !hypervisor.supports_exec() {
            bail!(
                "sandbox exec is not supported on {}: exec into a microVM needs a guest agent \
                 (not yet implemented)",
                hypervisor.label()
            );
        }
        opts.command = build_exec_command(&sandbox_name(allocation), opts.tty, &opts.command);

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            tty = opts.tty,
            "Opening exec session for sandbox task"
        );

        if opts.tty {
            spawn_pty(&opts)
        } else {
            spawn_pipe(&opts)
        }
    }

    /// Desugars `config.image` into an implicit `oci://` artifact, exactly like
    /// the container kind, so a sandbox image flows through the shared artifact
    /// pipeline (cluster auth, digest pinning, status rows) when the cluster's
    /// `artifacts.containerImageDesugar` flag is on.
    fn implicit_artifacts(&self, task_config: &Value) -> R<Vec<crate::structs::ArtifactSpec>> {
        let image = task_config
            .get("image")
            .and_then(Value::as_str)
            .context("sandbox task config requires image")?;
        Ok(vec![crate::structs::ArtifactSpec::oci_image(image)])
    }

    fn consume_artifact(&self, staged: &StagedArtifact, instance: &mut TaskInstance) -> R<()> {
        if let StagedPayload::Image {
            oci_layout,
            reference,
        } = &staged.payload
        {
            if matches!(Backend::detect(None), Ok(Backend::AppleContainer)) {
                oci_image::apple_load_image(oci_layout, reference)?;
            }
            oci_image::record_staged_layout(
                instance,
                oci_layout,
                reference,
                staged.digest.as_deref(),
            );
        }
        Ok(())
    }

    /// A sandbox is the one runtime whose `boot` gate is a real network
    /// boundary: the guest reaches the outside world through exactly one host
    /// TAP device (or, in `mode: 'none'`, through nothing at all), so holding
    /// that device down before ratification physically isolates it.
    fn network_gate(&self) -> crate::task_plugins::NetworkGate {
        crate::task_plugins::NetworkGate::Enforced
    }

    /// Hold the guest's TAP device down (and drop its traffic) until the
    /// placement is ratified, then bring it up.
    ///
    /// The device is attached at boot and only *administratively* shut,
    /// because Firecracker cannot hotplug a NIC — attaching the interface
    /// later is not an option, so the gate has to be on the link rather than
    /// on the attachment. Both operations need `CAP_NET_ADMIN`, which
    /// `mode: 'tap'` already requires of the operator.
    ///
    /// `mode: 'none'` (the default) needs no work: the guest has loopback and
    /// nothing else, which is strictly stronger than a shut link.
    fn set_network_gate(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _instance: &mut TaskInstance,
        open: bool,
    ) -> R<()> {
        let config = SandboxTaskConfig::from_value(&task.config)?;
        let Some(tap) = config.tap() else {
            // Loopback-only guest: nothing to gate, and nothing to open.
            return Ok(());
        };
        set_tap_link(&tap.device, open).with_context(|| {
            format!(
                "failed to {} TAP device {} for allocation {}",
                if open { "open" } else { "shut" },
                tap.device,
                allocation.id
            )
        })
    }

    fn prepare(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
    ) -> R<()> {
        let scratch = work_root(allocation, instance);
        std::fs::create_dir_all(&scratch)
            .with_context(|| format!("create sandbox scratch {}", scratch.display()))?;
        crate::util::restrict_dir_to_owner(&scratch)?;
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Preparing sandbox task"
        );
        Ok(())
    }

    fn start(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
    ) -> R<()> {
        let config = SandboxTaskConfig::from_value(&task.config)?;
        let hypervisor = Backend::detect(config.hypervisor.as_deref())?;
        if !hypervisor.is_microvm() {
            warn!(
                allocation_id = allocation.id,
                task_name = %task.name,
                backend = hypervisor.label(),
                "Sandbox running in dev mode; this is not a Legion-managed microVM"
            );
        }

        // Legion-provided env (allocation identity + assigned ports) first;
        // user-declared env overrides on key conflicts. Worker-injected host
        // paths (LEGION_ALLOC_DIR/LEGION_TASK_DIR) are deliberately skipped:
        // they do not exist inside the guest.
        let mut env: HashMap<String, String> =
            crate::task_plugins::allocation_env(allocation, task)
                .into_iter()
                .collect();
        env.extend(config.env.clone());

        let mut cmd = match &hypervisor {
            Backend::CloudHypervisor { .. } | Backend::Firecracker { .. } => {
                self.start_microvm(&hypervisor, allocation, task, &config, &env, instance)?
            }
            Backend::AppleContainer => {
                self.start_apple_container(allocation, &config, &env, instance, &hypervisor)?
            }
            Backend::OciRuntime { binary } => {
                let binary = binary.clone();
                self.start_oci_runtime(&binary, allocation, &config, &env, instance, &hypervisor)?
            }
        };

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            backend = hypervisor.label(),
            vcpus = config.vcpus(),
            memory_mib = config.memory_mib(),
            "Starting sandbox task"
        );
        configure_task_log_capture(&mut cmd, instance)?;
        let child = spawn_task_process(&mut cmd).with_context(|| {
            format!(
                "Failed to spawn sandbox task {} for allocation {}",
                task.name, allocation.id
            )
        })?;
        instance.pid = Some(child.id());
        instance.child = Some(child);
        Ok(())
    }

    /// **No graceful stop.** A sandbox is a guest OS behind a VMM process;
    /// signalling the VMM terminates the hypervisor, it does not ask the
    /// workload inside to shut down, and Legion has no in-guest agent to
    /// relay a notice to. Rather than pretend — signalling the VMM and
    /// calling the resulting hard stop "graceful" — this runtime declines the
    /// notice, and the worker tears the allocation down immediately, exactly
    /// as it did before graceful stop existed.
    ///
    /// The consequence is user-visible and documented in
    /// `docs/graceful-stop.md`: `stop { signal, killAfter }` has no effect on
    /// `sandbox` tasks.
    fn stop_with(
        &self,
        _allocation: &Allocation,
        _task: &TaskConfig,
        _instance: &mut TaskInstance,
        _ctx: &crate::task_plugins::StopContext,
    ) -> R<bool> {
        Ok(false)
    }

    fn stop(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
    ) -> R<()> {
        // A microVM *is* the VMM process, so killing the child takes the guest
        // with it. The dev-mode backends are the ones that outlive their
        // launcher (Apple `container` is daemon-backed; an OCI runtime
        // reparents the container init), so those get torn down by name first.
        //
        // Resolve the backend the way `start` did — from the task's own
        // `hypervisor` field. Re-detecting from scratch would pick the node
        // default and silently skip the teardown for a task that opted into
        // dev mode on a KVM-capable node, leaving its container running.
        if let Ok(hypervisor) = Backend::detect(configured_backend(task).as_deref()) {
            let name = sandbox_name(allocation);
            let teardown: Option<(String, Vec<String>)> = match &hypervisor {
                Backend::AppleContainer => {
                    Some(("container".to_string(), vec!["stop".into(), name.clone()]))
                }
                Backend::OciRuntime { binary } => Some((
                    binary.clone(),
                    vec!["delete".into(), "--force".into(), name.clone()],
                )),
                Backend::CloudHypervisor { .. } | Backend::Firecracker { .. } => None,
            };
            if let Some((program, args)) = teardown {
                info!(
                    allocation_id = allocation.id,
                    task_name = %task.name,
                    backend = hypervisor.label(),
                    "Tearing down dev-mode sandbox by name"
                );
                let _ = Command::new(program).args(&args).output();
            }
        }

        reap_task_process(allocation.id, &task.name, instance, "sandbox");
        Ok(())
    }

    fn destroy(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
    ) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Destroying sandbox task"
        );
        self.stop(allocation, task, instance)?;
        // Drop the allocation's scratch (image layout, root disk, bundle).
        // Volume-backed data is never under this tree: volumes live in the
        // volume system's own root, are unmounted by the worker on stop, and
        // are reclaimed by the manager's volume lifecycle (docs/volumes.md) —
        // never by a task runtime.
        let scratch = work_root(allocation, instance);
        if scratch.exists() {
            if let Err(err) = std::fs::remove_dir_all(&scratch) {
                warn!(
                    allocation_id = allocation.id,
                    error = %err,
                    "Failed to remove sandbox scratch dir"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn alloc() -> Allocation {
        Allocation {
            id: 7,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
            instance_id: None,
            network: None,
        }
    }

    fn task_with(config: Value) -> TaskConfig {
        TaskConfig {
            config,
            ..TaskConfig::new("workspace", "sandbox")
        }
    }

    #[test]
    fn config_parses_required_and_optional_fields() -> R<()> {
        let parsed = SandboxTaskConfig::from_value(&json!({
            "image": "alpine:latest",
            "command": ["/bin/sh", "-c", "sleep 60"],
            "env": { "MODE": "test" },
            "vcpus": 2,
            "memory_mib": 1024,
            "network": { "mode": "tap", "tap": "tap0", "mac": "02:00:00:00:00:01" },
        }))?;

        assert_eq!(parsed.image, "alpine:latest");
        assert_eq!(parsed.vcpus(), 2);
        assert_eq!(parsed.memory_mib(), 1024);
        assert_eq!(
            parsed.tap(),
            Some(TapNet {
                device: "tap0".into(),
                mac: Some("02:00:00:00:00:01".into())
            })
        );
        Ok(())
    }

    #[test]
    fn config_defaults_size_the_guest() -> R<()> {
        let parsed = SandboxTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["true"],
        }))?;
        assert_eq!(parsed.vcpus(), DEFAULT_VCPUS);
        assert_eq!(parsed.memory_mib(), DEFAULT_MEMORY_MIB);
        assert_eq!(parsed.rootfs_headroom_mib(), DEFAULT_ROOTFS_HEADROOM_MIB);
        assert_eq!(parsed.tap(), None);
        Ok(())
    }

    #[test]
    fn config_requires_image_and_command() {
        let err =
            SandboxTaskConfig::from_value(&json!({"command": ["x"]})).expect_err("missing image");
        assert!(format!("{err:#}").contains("missing field `image`"));

        let err =
            SandboxTaskConfig::from_value(&json!({"image": "alpine"})).expect_err("no command");
        assert!(format!("{err:#}").contains("missing field `command`"));

        let err = SandboxTaskConfig::from_value(&json!({"image": "alpine", "command": []}))
            .expect_err("empty command");
        assert!(err.to_string().contains("at least one entry"));
    }

    #[test]
    fn config_rejects_unusable_guest_sizes() {
        let err = SandboxTaskConfig::from_value(
            &json!({"image": "a", "command": ["x"], "memory_mib": 16}),
        )
        .expect_err("memory too small");
        assert!(err.to_string().contains("memory_mib must be at least"));

        let err =
            SandboxTaskConfig::from_value(&json!({"image": "a", "command": ["x"], "vcpus": 0}))
                .expect_err("zero vcpus");
        assert!(err.to_string().contains("vcpus must be at least 1"));
    }

    /// `config.state` predates the volume system. The forward-compat flatten
    /// would silently swallow it — and a sandbox that quietly runs without the
    /// persistence its spec asked for is the worst outcome — so it is named
    /// and rejected with the migration.
    #[test]
    fn config_rejects_the_retired_state_field() {
        let err = SandboxTaskConfig::from_value(
            &json!({"image": "a", "command": ["x"], "state": {"path": "/workspace"}}),
        )
        .expect_err("config.state was retired");
        assert!(err.to_string().contains("stateful"), "{err}");
        assert!(err.to_string().contains("volume"), "{err}");
    }

    /// Admission must run the *typed* parse, not just the derived JSON schema.
    /// The config's `#[serde(flatten)]` catch-all makes the schema accept any
    /// extra property, so a schema-only check passes every one of these and the
    /// job fails later, at task start on some node, instead of at submit.
    #[test]
    fn validate_config_enforces_more_than_the_json_schema() {
        for (label, config) in [
            (
                "retired state block",
                json!({"image": "a", "command": ["x"], "state": {"path": "/w"}}),
            ),
            (
                "guest too small to boot",
                json!({"image": "a", "command": ["x"], "memory_mib": 16}),
            ),
            (
                "zero vcpus",
                json!({"image": "a", "command": ["x"], "vcpus": 0}),
            ),
            (
                "unknown network mode",
                json!({"image": "a", "command": ["x"], "network": {"mode": "bridge"}}),
            ),
        ] {
            // The schema alone lets all of these through — that is the bug this
            // guards, so assert it rather than assuming it.
            validate_schema(&SandboxTaskPlugin.schema(), &config)
                .unwrap_or_else(|err| panic!("{label}: schema unexpectedly rejected it: {err}"));
            SandboxTaskPlugin
                .validate_config(&config)
                .expect_err(&format!("{label} must be rejected at admission"));
        }

        SandboxTaskPlugin
            .validate_config(&json!({"image": "alpine", "command": ["true"]}))
            .expect("a valid sandbox config still passes");
    }

    #[test]
    fn config_rejects_tap_without_a_device() {
        let err = SandboxTaskConfig::from_value(
            &json!({"image": "a", "command": ["x"], "network": {"mode": "tap"}}),
        )
        .expect_err("tap without device");
        assert!(err.to_string().contains("requires network.tap"));

        let err = SandboxTaskConfig::from_value(
            &json!({"image": "a", "command": ["x"], "network": {"mode": "bridge"}}),
        )
        .expect_err("unknown mode");
        assert!(err.to_string().contains("must be `none` or `tap`"));
    }

    #[test]
    fn plugin_validates_schema() -> R<()> {
        SandboxTaskPlugin.validate_config(&json!({
            "image": "alpine",
            "command": ["/bin/sh", "-c", "sleep 1"],
        }))
    }

    /// Readiness values are ANDed into node health, and anything but "pass"
    /// makes a node unhealthy — which gates rollouts. Since every worker loads
    /// this kind whether or not it has a hypervisor, readiness must never
    /// report backend availability.
    #[test]
    fn readiness_reports_plugin_health_not_backend_availability() {
        let readiness = SandboxTaskPlugin.readiness();
        assert_eq!(readiness["sandbox"], Value::String("pass".into()));
        for value in readiness.values() {
            assert_eq!(
                value.as_str(),
                Some("pass"),
                "a non-pass readiness value would mark the whole node unhealthy"
            );
        }
    }

    #[test]
    fn implicit_artifacts_desugar_the_image() -> R<()> {
        let plugin = SandboxTaskPlugin;
        assert!(plugin.supports_artifacts());
        let implicit = plugin
            .implicit_artifacts(&json!({"image": "ghcr.io/acme/box:v1", "command": ["x"]}))?;
        assert_eq!(implicit.len(), 1);
        assert_eq!(implicit[0].source, "oci://ghcr.io/acme/box:v1");
        assert_eq!(implicit[0].name.as_deref(), Some("image"));
        Ok(())
    }

    // --- the stateful story: volumes in, disks + guest mounts out ---

    fn instance_in(dir: &Path) -> TaskInstance {
        TaskInstance {
            alloc_dir: Some(dir.join("allocs").join("7")),
            ..TaskInstance::default()
        }
    }

    fn block_volume(name: &str, host: &Path, path: Option<&str>, read_only: bool) -> MountedVolume {
        MountedVolume {
            name: name.to_string(),
            mode: VolumeMode::Block,
            host_path: host.to_path_buf(),
            path: path.map(str::to_string),
            fs: Some("ext4".to_string()),
            fresh: false,
            read_only,
        }
    }

    fn fs_volume(name: &str, host: &Path, path: Option<&str>) -> MountedVolume {
        MountedVolume {
            name: name.to_string(),
            mode: VolumeMode::Filesystem,
            host_path: host.to_path_buf(),
            path: path.map(str::to_string),
            fs: None,
            fresh: false,
            read_only: false,
        }
    }

    /// The volume contract for microVMs: worker-mounted block volumes become
    /// virtio-blk devices in declaration order, with guest mounts to match.
    #[test]
    fn block_volumes_become_guest_disks_in_declaration_order() -> R<()> {
        let volumes = vec![
            block_volume(
                "data",
                Path::new("/volumes/local-dir/9f2c/disk.img"),
                Some("/workspace"),
                false,
            ),
            block_volume(
                "models",
                Path::new("/dev/mapper/legion-models"),
                Some("/models"),
                true,
            ),
            // Attach-only: no path means the guest gets the raw device.
            block_volume(
                "raw",
                Path::new("/volumes/local-dir/aa11/disk.img"),
                None,
                false,
            ),
        ];

        let guest = guest_volume_disks(&volumes)?;
        assert_eq!(guest.disks.len(), 3);
        assert_eq!(
            guest.disks[0].path,
            PathBuf::from("/volumes/local-dir/9f2c/disk.img")
        );
        assert!(guest.disks[1].read_only, "read-only reaches the VMM");

        assert_eq!(guest.mounts[0].device, "/dev/vdb");
        assert_eq!(guest.mounts[0].path.as_deref(), Some("/workspace"));
        assert_eq!(guest.mounts[0].fs, "ext4");
        assert_eq!(guest.mounts[1].device, "/dev/vdc");
        assert!(
            guest.mounts[1].read_only,
            "read-only reaches the guest mount"
        );
        assert_eq!(guest.mounts[2].path, None, "pathless volume is attach-only");
        Ok(())
    }

    /// A filesystem volume has no shape a microVM can consume; the error must
    /// name the volume and both ways out.
    #[test]
    fn microvm_rejects_filesystem_volumes() {
        let volumes = vec![fs_volume(
            "data",
            Path::new("/volumes/local-dir/9f2c"),
            Some("/workspace"),
        )];
        let err = guest_volume_disks(&volumes).expect_err("filesystem volume in a microVM");
        let message = err.to_string();
        assert!(message.contains("`data`"), "{message}");
        assert!(message.contains("mode: 'block'"), "{message}");
        assert!(message.contains("hypervisor: 'dev'"), "{message}");
    }

    /// The mirror image: dev-mode backends bind-mount directories and cannot
    /// attach a block device.
    #[test]
    fn dev_mode_rejects_block_volumes() {
        let volumes = vec![block_volume(
            "data",
            Path::new("/volumes/local-dir/9f2c/disk.img"),
            Some("/workspace"),
            false,
        )];
        let backend = Backend::OciRuntime {
            binary: "youki".into(),
        };
        let err = reject_block_volumes_in_dev_mode(&volumes, &backend)
            .expect_err("block volume in dev mode");
        assert!(err.to_string().contains("`data`"), "{err}");
        assert!(err.to_string().contains("filesystem volume"), "{err}");

        // Filesystem volumes are exactly what dev mode wants.
        let volumes = vec![fs_volume(
            "data",
            Path::new("/host/dir"),
            Some("/workspace"),
        )];
        reject_block_volumes_in_dev_mode(&volumes, &backend).expect("filesystem volumes pass");
    }

    /// Only the worker-resolved volume vars go into the guest: the values are
    /// in-guest destinations, unlike LEGION_ALLOC_DIR and friends, which name
    /// host paths the guest cannot see.
    #[test]
    fn volume_env_forwards_only_volume_vars() {
        let mut instance = TaskInstance::default();
        instance
            .extra_env
            .insert("LEGION_VOLUME_DATA".into(), "/workspace".into());
        instance
            .extra_env
            .insert("LEGION_STORAGE_DIR".into(), "/workspace".into());
        instance
            .extra_env
            .insert("LEGION_ALLOC_DIR".into(), "/host/allocs/7".into());
        instance
            .extra_env
            .insert("LEGION_ARTIFACT_MODEL".into(), "/host/staged/model".into());

        let env: BTreeMap<String, String> = instance.volume_env().collect();
        assert_eq!(
            env.get("LEGION_VOLUME_DATA").map(String::as_str),
            Some("/workspace")
        );
        assert_eq!(
            env.get("LEGION_STORAGE_DIR").map(String::as_str),
            Some("/workspace")
        );
        assert!(!env.contains_key("LEGION_ALLOC_DIR"));
        assert!(!env.contains_key("LEGION_ARTIFACT_MODEL"));
    }

    #[test]
    fn path_safe_neutralizes_separators_and_dot_names() {
        assert_eq!(path_safe("user-42"), "user-42");
        assert_eq!(path_safe("a/b"), "a_b");
        assert_eq!(path_safe(".."), "_");
        assert_eq!(path_safe(""), "_");
        assert_eq!(path_safe("../../etc"), ".._.._etc");
    }

    /// Destroy reclaims the allocation's scratch tree and nothing else —
    /// volume data lives in the volume system's own root and is reclaimed by
    /// the manager's volume lifecycle, never by a task runtime.
    #[test]
    fn destroy_drops_scratch_but_never_touches_volume_data() -> R<()> {
        let dir = tempfile::tempdir()?;
        let mut instance = instance_in(dir.path());
        let task = task_with(json!({
            "image": "alpine",
            "command": ["true"],
        }));
        let allocation = alloc();

        let scratch = work_root(&allocation, &instance);
        std::fs::create_dir_all(&scratch)?;
        std::fs::write(scratch.join("rootfs.img"), b"ephemeral")?;

        // Stand-in for a local-dir block volume: outside the alloc tree.
        let volume_img = dir.path().join("volumes/local-dir/9f2c/disk.img");
        std::fs::create_dir_all(volume_img.parent().unwrap())?;
        std::fs::write(&volume_img, b"instance data")?;
        instance.volumes = vec![block_volume("data", &volume_img, Some("/workspace"), false)];

        SandboxTaskPlugin.destroy(&allocation, &task, &mut instance)?;

        assert!(!scratch.exists(), "allocation scratch is reclaimed");
        assert!(
            volume_img.exists(),
            "volume data must survive destroy: reclaiming it is the volume system's job"
        );
        Ok(())
    }

    // --- artifacts land inside the guest, not on the host task dir ---

    #[test]
    fn artifacts_are_copied_into_the_guest_and_paths_rewritten() -> R<()> {
        let work = tempfile::tempdir()?;
        let staged_dir = work.path().join("staged");
        std::fs::create_dir_all(staged_dir.join("model"))?;
        std::fs::write(staged_dir.join("model/weights.bin"), b"w")?;
        let tool = staged_dir.join("tool.sh");
        std::fs::write(&tool, b"#!/bin/sh\n")?;

        let staged = vec![
            StagedArtifact {
                name: Some("model".into()),
                digest: None,
                bytes: 1,
                payload: StagedPayload::Tree {
                    dir: staged_dir.join("model"),
                },
            },
            StagedArtifact {
                name: Some("tool".into()),
                digest: None,
                bytes: 1,
                payload: StagedPayload::File { path: tool.clone() },
            },
            // The image artifact is the rootfs itself and must be skipped.
            StagedArtifact {
                name: Some("image".into()),
                digest: None,
                bytes: 1,
                payload: StagedPayload::Image {
                    oci_layout: staged_dir.join("layout"),
                    reference: "alpine".into(),
                },
            },
        ];

        let rootfs = work.path().join("rootfs");
        std::fs::create_dir_all(&rootfs)?;
        let mapped = install_artifacts(&rootfs, &staged)?;

        assert!(rootfs.join("legion/artifacts/model/weights.bin").is_file());
        assert!(rootfs.join("legion/artifacts/tool/tool.sh").is_file());
        assert!(!rootfs.join("legion/artifacts/image").exists());

        // A command the worker interpolated with host paths is rewritten to the
        // in-guest locations of the same content.
        let command = vec![
            format!("{}/run", staged_dir.join("model").display()),
            tool.display().to_string(),
            "--once".to_string(),
        ];
        let rewritten = rewrite_artifact_paths(&command, &mapped);
        assert_eq!(rewritten[0], "/legion/artifacts/model/run");
        assert_eq!(rewritten[1], "/legion/artifacts/tool/tool.sh");
        assert_eq!(rewritten[2], "--once");
        Ok(())
    }

    #[test]
    fn build_stamp_changes_with_command_env_and_artifacts() {
        let layout = PathBuf::from("/layout");
        let payload = GuestPayload {
            command: vec!["a".into()],
            env: BTreeMap::from([("A".to_string(), "1".to_string())]),
            ..GuestPayload::default()
        };
        let base = build_stamp(&layout, &payload, &[]);
        assert_eq!(
            base,
            build_stamp(&layout, &payload, &[]),
            "identical inputs must reuse the disk"
        );

        let changed_command = GuestPayload {
            command: vec!["b".into()],
            ..payload.clone()
        };
        assert_ne!(base, build_stamp(&layout, &changed_command, &[]));

        // A refreshed identity token or KV value lands in env, and must
        // rebuild the disk that bakes it in.
        let changed_env = GuestPayload {
            env: BTreeMap::from([("A".to_string(), "2".to_string())]),
            ..payload.clone()
        };
        assert_ne!(base, build_stamp(&layout, &changed_env, &[]));

        // Volume mounts are baked into the generated init.
        let changed_mounts = GuestPayload {
            disk_mounts: vec![GuestDiskMount {
                device: "/dev/vdb".into(),
                path: Some("/workspace".into()),
                fs: "ext4".into(),
                read_only: false,
            }],
            ..payload
        };
        assert_ne!(base, build_stamp(&layout, &changed_mounts, &[]));
    }

    // --- dev mode (macOS) launch surface ---

    #[test]
    fn apple_launch_binds_volumes_and_terminates_args() -> R<()> {
        let config = SandboxTaskConfig::from_value(&json!({
            "image": "alpine",
            "command": ["-l", "ls"],
            "vcpus": 2,
            "memory_mib": 1024,
            "cwd": "/workspace",
        }))?;
        let env = BTreeMap::from([("MODE".to_string(), "test".to_string())]);
        let mut models = fs_volume("models", Path::new("/mnt/models"), Some("/models"));
        models.read_only = true;
        let volumes = vec![
            fs_volume(
                "data",
                Path::new("/var/legion/volumes/local-dir/9f2c"),
                Some("/workspace"),
            ),
            models,
        ];
        let args = build_apple_launch("legion-sbx-7", &config, &env, &volumes);

        assert_eq!(args[0], "run");
        assert!(args.contains(&"--rm".to_string()));
        assert!(args.contains(&"legion-sbx-7".to_string()));
        assert!(args.contains(&"2".to_string()));
        assert!(args.contains(&"1024M".to_string()));
        assert!(args.contains(&"/var/legion/volumes/local-dir/9f2c:/workspace".to_string()));
        assert!(
            args.contains(&"/mnt/models:/models:ro".to_string()),
            "read-only volumes bind read-only"
        );
        assert!(args.contains(&"MODE=test".to_string()));
        // `--` guards a command whose first token looks like a flag.
        let sep = args.iter().position(|a| a == "--").expect("separator");
        let image = args.iter().position(|a| a == "alpine").expect("image");
        assert_eq!(sep, image + 1);
        assert_eq!(args[sep + 1], "-l");
        Ok(())
    }

    #[test]
    fn apple_launch_omits_sizing_flags_when_unset() -> R<()> {
        let config =
            SandboxTaskConfig::from_value(&json!({"image": "alpine", "command": ["true"]}))?;
        let args = build_apple_launch("legion-sbx-7", &config, &BTreeMap::new(), &[]);
        assert!(!args.contains(&"--cpus".to_string()));
        assert!(!args.contains(&"--memory".to_string()));
        assert!(!args.contains(&"--volume".to_string()));
        Ok(())
    }

    /// `stop` and `exec` must resolve the same backend `start` launched with.
    /// Re-detecting the node default would skip the by-name teardown for a task
    /// that opted into dev mode on a KVM-capable node, leaving it running.
    #[test]
    fn configured_backend_is_read_from_the_task_spec() {
        let dev = task_with(json!({
            "image": "alpine",
            "command": ["true"],
            "hypervisor": "dev",
        }));
        assert_eq!(configured_backend(&dev).as_deref(), Some("dev"));

        let auto = task_with(json!({"image": "alpine", "command": ["true"]}));
        assert_eq!(configured_backend(&auto), None);
    }

    #[test]
    fn exec_command_targets_the_sandbox_by_name() {
        let argv = build_exec_command("legion-sbx-7", true, &["/bin/sh".to_string()]);
        assert_eq!(argv[0], "container");
        assert_eq!(argv[1], "exec");
        assert!(argv.contains(&"--tty".to_string()));
        assert!(argv.contains(&"legion-sbx-7".to_string()));
        assert_eq!(argv.last().unwrap(), "/bin/sh");
    }

    // --- the microVM start path: image layout -> bootable root disk ---

    /// Writes `bytes` into the layout's blob store, returning its digest.
    fn write_blob(layout: &Path, bytes: &[u8]) -> R<String> {
        let hex = format!("{:x}", Sha256::digest(bytes));
        let dir = layout.join("blobs").join("sha256");
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join(&hex), bytes)?;
        Ok(format!("sha256:{hex}"))
    }

    /// A minimal but genuine OCI image layout — one uncompressed layer tarball
    /// plus the index/manifest/config chain — so the disk-build path can be
    /// exercised hermetically, with no registry and no network.
    fn synthetic_oci_layout(root: &Path) -> R<PathBuf> {
        let layout = root.join("layout");
        std::fs::create_dir_all(&layout)?;

        let mut layer = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut layer);
            for (path, content, mode) in [
                ("bin/sh", &b"#!/fake/shell\n"[..], 0o755),
                ("etc/passwd", &b"root:x:0:0:root:/root:/bin/sh\n"[..], 0o644),
                ("app/data.txt", &b"payload\n"[..], 0o644),
            ] {
                let mut header = tar::Header::new_gnu();
                header.set_size(content.len() as u64);
                header.set_mode(mode);
                builder.append_data(&mut header, path, content)?;
            }
            builder.finish()?;
        }
        let layer_digest = write_blob(&layout, &layer)?;

        let config = serde_json::to_vec(&json!({
            "config": { "Env": ["FROM_IMAGE=1"], "WorkingDir": "/app" }
        }))?;
        let config_digest = write_blob(&layout, &config)?;

        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "config": { "digest": config_digest, "size": config.len() },
            "layers": [{ "digest": layer_digest, "size": layer.len() }],
        }))?;
        let manifest_digest = write_blob(&layout, &manifest)?;

        std::fs::write(
            layout.join("index.json"),
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "manifests": [{ "digest": manifest_digest, "size": manifest.len() }],
            }))?,
        )?;
        Ok(layout)
    }

    /// Lists a directory inside an ext4 image with `debugfs` (read-only, no
    /// mount, no root). Returns `None` when e2fsprogs' debugfs is unavailable.
    fn debugfs_ls(image: &Path, dir: &str) -> Option<String> {
        let debugfs = ["debugfs", "/sbin/debugfs", "/usr/sbin/debugfs"]
            .into_iter()
            .find_map(microvm::find_on_path)?;
        let output = Command::new(debugfs)
            .arg("-R")
            .arg(format!("ls -l {dir}"))
            .arg(image)
            .output()
            .ok()?;
        Some(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// The whole microVM start path, minus the boot: a real image layout is
    /// turned into an ext4 root disk carrying both the image's files and the
    /// generated guest init, and the VMM is invoked with it plus the
    /// worker-mounted block volume.
    #[test]
    fn microvm_start_builds_a_bootable_root_disk() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let layout = synthetic_oci_layout(tmp.path())?;
        let kernel = tmp.path().join("vmlinux");
        std::fs::write(&kernel, b"not really a kernel")?;

        let plugin = SandboxTaskPlugin;
        let allocation = alloc();
        let task = task_with(json!({
            "image": "example.test/box:v1",
            "command": ["/bin/sh", "-c", "echo hello"],
            "kernel": kernel.display().to_string(),
            "env": { "LEGION_INSTANCE_ID": "disk-test", "MODE": "test" },
        }));
        let config = SandboxTaskConfig::from_value(&task.config)?;
        let env: HashMap<String, String> = config.env.clone();

        // Stand-in for the volume system: the worker mounted a block-mode
        // volume before start and recorded it on the instance.
        let volume_img = tmp.path().join("volumes/local-dir/9f2c/disk.img");
        std::fs::create_dir_all(volume_img.parent().unwrap())?;
        std::fs::write(&volume_img, b"formatted by the volume driver")?;

        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().join("allocs").join("7")),
            ..TaskInstance::default()
        };
        instance.volumes = vec![block_volume("data", &volume_img, Some("/workspace"), false)];
        instance
            .extra_env
            .insert("LEGION_STORAGE_DIR".into(), "/workspace".into());
        // Stand in for the artifact pipeline having staged the image.
        oci_image::record_staged_layout(&mut instance, &layout, "example.test/box:v1", None);
        plugin.prepare(&allocation, &task, &mut instance)?;

        if microvm::create_ext4_image(&tmp.path().join("probe.img"), 32 * 1024 * 1024, None, "p")
            .is_err()
        {
            return Ok(()); // no e2fsprogs on this machine
        }

        let backend = Backend::CloudHypervisor {
            binary: "/bin/true".into(),
        };
        let cmd =
            plugin.start_microvm(&backend, &allocation, &task, &config, &env, &mut instance)?;

        let args = cmd
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let scratch = work_root(&allocation, &instance);
        let root_disk = scratch.join("rootfs.img");

        assert!(root_disk.is_file(), "root disk built");
        assert!(args.contains(&format!("path={}", root_disk.display())));
        assert!(
            args.contains(&format!("path={}", volume_img.display())),
            "the volume's disk image is attached as a virtio-blk device"
        );
        assert!(args
            .iter()
            .any(|arg| arg.contains("init=/legion/init") && arg.contains("root=/dev/vda")));

        // The image's own files and Legion's generated init must both be in
        // the filesystem — that is what makes the disk bootable *and* useful.
        if let Some(listing) = debugfs_ls(&root_disk, "/legion") {
            assert!(listing.contains("init"), "guest init in image: {listing}");
            assert!(listing.contains("env"), "env file in image: {listing}");
        }
        if let Some(listing) = debugfs_ls(&root_disk, "/app") {
            assert!(listing.contains("data.txt"), "image layer files: {listing}");
        }

        // A second start with unchanged inputs must reuse the disk instead of
        // re-extracting and re-formatting hundreds of MiB.
        let built_at = std::fs::metadata(&root_disk)?.modified()?;
        plugin.start_microvm(&backend, &allocation, &task, &config, &env, &mut instance)?;
        assert_eq!(
            std::fs::metadata(&root_disk)?.modified()?,
            built_at,
            "unchanged inputs must reuse the root disk"
        );

        // Changing the command changes the fingerprint, so the disk is rebuilt.
        let mut changed = config.clone();
        changed.command = vec!["/bin/sh".into(), "-c".into(), "echo different".into()];
        plugin.start_microvm(&backend, &allocation, &task, &changed, &env, &mut instance)?;
        assert_ne!(
            std::fs::metadata(&root_disk)?.modified()?,
            built_at,
            "changed inputs must rebuild the root disk"
        );
        Ok(())
    }

    /// End-to-end microVM boot. Needs KVM, a VMM, an uncompressed kernel, and
    /// network access for the image pull, so it is opt-in:
    ///
    /// `LEGION_SANDBOX_KERNEL=/path/to/vmlinux cargo test --lib \
    ///   sandbox_microvm_boots -- --ignored --nocapture`
    #[test]
    #[ignore = "requires KVM + cloud-hypervisor/firecracker + a guest kernel"]
    fn sandbox_microvm_boots_and_captures_console() -> R<()> {
        let plugin = SandboxTaskPlugin;
        let allocation = alloc();
        let image = std::env::var("LEGION_SANDBOX_TEST_IMAGE")
            .unwrap_or_else(|_| "alpine:latest".to_string());
        let task = task_with(json!({
            "image": image,
            "command": ["/bin/sh", "-c",
                        "echo sandbox-hello; test -d /workspace && echo state-mounted"],
            "env": { "LEGION_INSTANCE_ID": "e2e-instance" },
        }));

        let tmp = tempfile::tempdir()?;
        // Stand in for the local-dir driver's block mode: an empty formatted
        // ext4 image the guest init mounts at /workspace.
        let volume_img = tmp.path().join("volumes/disk.img");
        microvm::create_ext4_image(&volume_img, 64 * 1024 * 1024, None, "legion-vol")?;

        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().join("allocs").join("7")),
            log_dir: Some(tmp.path().join("logs")),
            ..TaskInstance::default()
        };
        instance.volumes = vec![block_volume("data", &volume_img, Some("/workspace"), false)];

        plugin.prepare(&allocation, &task, &mut instance)?;
        plugin.start(&allocation, &task, &mut instance)?;
        let mut child = instance.child.take().expect("VMM child handle");
        let status = child.wait()?;
        assert!(status.success(), "VMM exited cleanly: {status:?}");

        let console =
            std::fs::read_to_string(instance.log_dir.as_ref().unwrap().join("stdout.log"))?;
        assert!(console.contains("sandbox-hello"), "console: {console}");
        assert!(console.contains("state-mounted"), "console: {console}");
        assert!(
            console.contains("legion: sandbox command exited with status 0"),
            "init must report the payload's exit status: {console}"
        );
        Ok(())
    }
}
