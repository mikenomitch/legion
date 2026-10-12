//! Memory checkpoint and restore for the `sandbox` runtime's microVMs.
//!
//! The capture sequence and its rationale are in
//! `internal_docs/plans/sandbox-snapshots-and-fast-start-plan.md` Part I §3
//! and Part III. In short:
//!
//! - **Capture** pauses the VM, has the VMM write guest memory and device
//!   state into the checkpoint directory, copies the guest's ephemeral root
//!   disk beside them (reflink where possible), then resumes or kills the VMM.
//! - **Restore** launches a fresh VMM, loads that state, repoints the
//!   identity disk at a freshly written one, and resumes.
//!
//! # Why device links
//!
//! Both VMMs record the host path of every disk in the snapshot and reopen
//! those exact paths on restore. A sandbox's disks live under its allocation
//! directory, and a restore is a *new* allocation with a new directory. So a
//! checkpointable sandbox never hands the VMM its real disk paths. It hands
//! it stable symlinks under `<allocs>/.vmdev/<link_key>/` (`rootfs.img`,
//! `vol0.img`, …, `identity.img`), and a restore repoints those links at the
//! new allocation's files before the VMM opens them. The VMM holds open file
//! descriptors from then on, so repointing the links for a later restore (a
//! fork, say) never disturbs a running guest. Restores that share a link key
//! are serialized by [`LINK_LOCK`] across the repoint-and-load window.
//!
//! # Seams left for later phases
//!
//! - The guest's resume handler (`legion guest-resume`,
//!   `crate::task_plugins::guest_resume`) polls the identity disk named by
//!   `legion.identity=` on the kernel cmdline, and refreshes the guest's RNG,
//!   clock, hostname, and identity files after every restore.
//!
//! # Gated sandboxes: the vsock socket moves with the allocation
//!
//! A gated guest's only door is its vsock device, whose host end is
//! `<alloc dir>/.sandbox/vsock.sock` (the datapath listens on `<base>_1080`
//! and `<base>_53`). Both VMMs record that path in the saved state. Unlike a
//! disk, which a VMM opens once at load, the VMM resolves `<base>_<port>` on
//! every guest connect for the VM's whole life, so the path can never live
//! in the shared device-link directory: after a fork repointed it, the
//! origin's next connection would reach the fork's listener and be judged as
//! the fork. Instead every restore moves the VM onto the restoring
//! allocation's own path: Firecracker through `vsock_override` on
//! `PUT /snapshot/load` (1.16.1 and later, checked at capture), and
//! cloud-hypervisor through the restore's own copy of `config.json`, the
//! same rewrite fork TAPs use. See
//! `internal_docs/plans/gated-sandbox-checkpoints-plan.md` §6.2.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::prelude::*;
use crate::structs::checkpoint::{
    GuestCompat, MemoryParts, ResumeIdentity, SnapshotCompat, SnapshotKind, SnapshotManifest,
    VmmCompat,
};
use crate::structs::Allocation;
use crate::task_plugins::checkpoint::{
    fresh_entropy, write_identity_disk, AfterCheckpoint, CheckpointDir, CheckpointQuery,
    CheckpointSupport,
};
use crate::task_plugins::microvm::{self, Backend, VmSpec};
use crate::task_plugins::vmm_api::{wait_for_api_socket, CloudHypervisorApi, FirecrackerApi};
use crate::task_plugins::TaskInstance;
use crate::volumes::copy::clone_file;

/// Drive id of the identity disk on both VMMs (Firecracker `drive_id`,
/// cloud-hypervisor disk `id`).
pub const IDENTITY_DRIVE_ID: &str = "identity";

/// Per-allocation record of how a checkpointable sandbox was launched, kept
/// in its scratch dir so it survives a worker restart (`instance.state` does
/// not).
const BOOT_RECORD: &str = "checkpoint-boot.json";
const ROOT_LINK: &str = "rootfs.img";
const IDENTITY_LINK: &str = "identity.img";
const IDENTITY_FILE: &str = "identity.img";

/// The identity disk of the guest whose scratch dir is `scratch`.
pub(crate) fn identity_disk_path(scratch: &Path) -> PathBuf {
    scratch.join(IDENTITY_FILE)
}

/// How long a freshly spawned VMM gets to open its API socket.
const API_SOCKET_TIMEOUT: Duration = Duration::from_secs(10);

/// Serializes repoint-links-then-load, so two restores sharing a link key
/// (forks of one checkpoint) never see each other's links.
static LINK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn volume_link(index: usize) -> String {
    format!("vol{index}.img")
}

/// Memory-part file names each VMM writes.
mod parts {
    pub const FC_MEMORY: &str = "memory";
    pub const FC_STATE: &str = "vm-state";
    /// cloud-hypervisor writes these three into the destination directory.
    pub const CH_MEMORY: &str = "memory-ranges";
    pub const CH_STATE: &str = "state.json";
    pub const CH_CONFIG: &str = "config.json";
    /// The guest's ephemeral root disk, captured beside the memory because
    /// the guest's page cache and journal describe *that* disk.
    pub const ROOTFS: &str = "rootfs.img";
}

/// The shape of a guest's network, which a restore must match: both VMMs
/// restore exactly the devices they saved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum GuestNetwork {
    /// Loopback only: no NIC and no vsock.
    None,
    /// A NIC on a host TAP device.
    Tap,
    /// No NIC, and a vsock device whose host end is the node datapath.
    Gated,
}

impl GuestNetwork {
    /// How a refusal describes this shape.
    fn describe(self) -> &'static str {
        match self {
            GuestNetwork::None => "has no network device",
            GuestNetwork::Tap => "has a NIC on a TAP device",
            GuestNetwork::Gated => "has a vsock datapath and no NIC",
        }
    }
}

/// What a checkpointable launch recorded, and what a checkpoint carries in
/// [`MemoryParts::runtime`] so a restore can rebuild the same device layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SandboxCheckpointState {
    /// `firecracker` or `cloud-hypervisor`.
    pub vmm: String,
    /// The link directory name under `<allocs>/.vmdev/`.
    pub link_key: String,
    /// Volume disks attached, in order (`vol0.img`, …).
    pub volume_count: usize,
    pub vcpus: u32,
    pub memory_mib: u32,
    /// The host TAP device the guest's NIC was attached to, when it had one.
    /// Both VMMs record the device name in the saved state, so a restore
    /// needs a TAP of exactly this name (see [`ensure_restore_tap`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tap: Option<String>,
    /// The guest's network shape. Absent on checkpoints taken before gated
    /// sandboxes could be checkpointed, which read as `tap` when they record
    /// a TAP and `none` otherwise ([`Self::network`]): no gated sandbox was
    /// ever checkpointed before the field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<GuestNetwork>,
    /// The address a TAP guest held from its bridge's pool, so a resume
    /// asks for it again first (docs/task-runtime-sandbox.md, "Addressing").
    /// Absent for gated and loopback-only guests, and on checkpoints taken
    /// before Legion addressed TAP guests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest_address: Option<String>,
    /// The port the guest's workload listens on per advertised port label
    /// (`LEGION_PORT_<label>` inside the guest). Fixed at the guest's first
    /// boot and carried through every checkpoint, because the processes
    /// listening on them ride the guest's memory: a restore into an
    /// allocation with different port numbers still has its workload on
    /// these, so the datapath dials these. Empty on checkpoints taken
    /// before sandbox ingress, which then use the restoring allocation's.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub guest_ports: BTreeMap<String, u16>,
}

/// The ports a freshly booted guest's workload binds: each advertised
/// port's bind port (`AllocPort::bind_port`), by label.
pub(crate) fn boot_guest_ports(
    network: Option<&crate::structs::AllocNetwork>,
) -> BTreeMap<String, u16> {
    network
        .map(|network| {
            network
                .ports
                .iter()
                .map(|port| (port.name.clone(), port.bind_port()))
                .collect()
        })
        .unwrap_or_default()
}

/// The ports a guest restored from `manifest` into an allocation with
/// `network` listens on: what the checkpoint recorded for each label the
/// allocation still advertises, and the allocation's own bind port for a
/// label the checkpoint does not know (a port the job added while the
/// instance was suspended, which only something `onResume` starts can
/// serve).
pub(crate) fn restored_guest_ports(
    manifest: &SnapshotManifest,
    network: Option<&crate::structs::AllocNetwork>,
) -> BTreeMap<String, u16> {
    let recorded = serde_json::from_value::<SandboxCheckpointState>(manifest.runtime_state.clone())
        .map(|state| state.guest_ports)
        .unwrap_or_default();
    boot_guest_ports(network)
        .into_iter()
        .map(|(label, port)| {
            let port = recorded.get(&label).copied().unwrap_or(port);
            (label, port)
        })
        .collect()
}

/// The port the guest in `scratch` listens on for `label`, when its boot
/// record says (a sandbox launched for memory checkpoints, booted or
/// restored). `None` means the allocation's own bind port.
pub(crate) fn recorded_guest_port(scratch: &Path, label: &str) -> Option<u16> {
    read_boot_record(scratch)?.guest_ports.get(label).copied()
}

/// For tests elsewhere in the crate (the datapath's): a boot record that
/// says only which ports the guest in `scratch` listens on.
#[cfg(test)]
pub(crate) fn write_guest_ports_record(scratch: &Path, ports: &[(&str, u16)]) -> R<()> {
    write_boot_record(
        scratch,
        &SandboxCheckpointState {
            vmm: "firecracker".into(),
            link_key: "sbx-test".into(),
            volume_count: 0,
            vcpus: 1,
            memory_mib: 128,
            tap: None,
            network: Some(GuestNetwork::Gated),
            guest_address: None,
            guest_ports: ports
                .iter()
                .map(|(label, port)| (label.to_string(), *port))
                .collect(),
        },
    )
}

impl SandboxCheckpointState {
    /// The guest's network shape, reading an older record by its TAP.
    pub(crate) fn network(&self) -> GuestNetwork {
        self.network.unwrap_or(if self.tap.is_some() {
            GuestNetwork::Tap
        } else {
            GuestNetwork::None
        })
    }
}

/// The sandbox runtime's answer to
/// [`TaskPlugin::checkpoint_support`](super::TaskPlugin::checkpoint_support).
///
/// Pure in the task config plus the query, so the manager can ask it too:
/// Firecracker and cloud-hypervisor capture memory, the dev backends (Apple
/// `container`, an OCI runtime) capture the volume only. `hypervisor =
/// "auto"` resolves through [`CheckpointQuery::node_vmm`] when the caller
/// knows the node's VMM, else on this machine.
pub fn sandbox_checkpoint_support(query: &CheckpointQuery<'_>) -> CheckpointSupport {
    let selection = query
        .config
        .get("hypervisor")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "auto".to_string());
    let resolved = match selection.as_str() {
        "auto" => match query.node_vmm {
            Some(vmm) => vmm.to_string(),
            None => match Backend::detect(Some("auto")) {
                Ok(backend) => backend.vmm_name().to_string(),
                Err(_) => return CheckpointSupport::None,
            },
        },
        other => other.to_string(),
    };
    match resolved.as_str() {
        "firecracker" | "fc" | "cloud-hypervisor" | "cloudhypervisor" | "ch" => {
            CheckpointSupport::Memory
        }
        "dev" | "apple-container" | "oci-runtime" => CheckpointSupport::Disk,
        _ => CheckpointSupport::None,
    }
}

/// The `sandbox.vmm*` fingerprint attributes for this node: the VMM
/// `hypervisor = "auto"` resolves to, its version, and a version per
/// installed microVM monitor. A node where nothing resolves reports none,
/// which no memory checkpoint matches.
pub fn sandbox_vmm_attrs() -> Map<String, Value> {
    use crate::structs::checkpoint::{
        vmm_version_attr, ATTR_SANDBOX_VMM, ATTR_SANDBOX_VMM_VERSION,
    };
    let mut attrs = Map::new();
    let installed: Vec<(Backend, Option<String>)> = microvm::installed_vmms()
        .into_iter()
        .map(|backend| {
            let version = backend.binary().and_then(microvm::vmm_version);
            (backend, version)
        })
        .collect();
    for (backend, version) in &installed {
        if let Some(version) = version {
            attrs.insert(vmm_version_attr(backend.vmm_name()), json!(version));
        }
    }
    if let Ok(default) = Backend::detect(None) {
        attrs.insert(ATTR_SANDBOX_VMM.into(), json!(default.vmm_name()));
        if let Some((_, Some(version))) = installed
            .iter()
            .find(|(backend, _)| backend.vmm_name() == default.vmm_name())
        {
            attrs.insert(ATTR_SANDBOX_VMM_VERSION.into(), json!(version));
        }
    }
    attrs
}

/// Whether a Firecracker sandbox boots with `--api-sock` alongside
/// `--config-file`. Only when it can be memory-checkpointed *and* the job
/// will ever ask for it (suspend or snapshots enabled): an API socket is a
/// control channel into the VMM, and a sandbox that never needs one should
/// not have one.
pub fn firecracker_needs_api_socket(support: CheckpointSupport, checkpoints_enabled: bool) -> bool {
    support == CheckpointSupport::Memory && checkpoints_enabled
}

/// The stable device-link directory of one checkpoint lineage.
#[derive(Debug, Clone)]
pub(crate) struct DeviceLinks {
    dir: PathBuf,
}

impl DeviceLinks {
    pub(crate) fn new(root: &Path, link_key: &str) -> Self {
        DeviceLinks {
            dir: root.join(link_key),
        }
    }

    pub(crate) fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    /// Atomically point link `name` at `target` (create a temp symlink, then
    /// rename over the old one).
    pub(crate) fn point(&self, name: &str, target: &Path) -> R<PathBuf> {
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("failed to create {}", self.dir.display()))?;
        let link = self.path(name);
        let staging = self.path(&format!(".{name}.{}", crate::util::random_string(6)));
        std::os::unix::fs::symlink(target, &staging)
            .with_context(|| format!("failed to link {}", staging.display()))?;
        std::fs::rename(&staging, &link)
            .with_context(|| format!("failed to publish link {}", link.display()))?;
        Ok(link)
    }

    /// True when the root-disk link points into `dir`: the allocation owning
    /// `dir` was the last to use these links and may remove them.
    pub(crate) fn points_into(&self, dir: &Path) -> bool {
        std::fs::read_link(self.path(ROOT_LINK))
            .map(|target| target.starts_with(dir))
            .unwrap_or(false)
    }

    pub(crate) fn remove(&self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Where link directories live: beside the allocation directories, so they
/// share a filesystem with every disk they point at.
pub(crate) fn vmdev_root(instance: &TaskInstance, scratch: &Path) -> PathBuf {
    instance
        .alloc_dir
        .as_deref()
        .and_then(Path::parent)
        .map(|allocs| allocs.join(".vmdev"))
        .unwrap_or_else(|| scratch.join("vmdev"))
}

pub(crate) fn read_boot_record(scratch: &Path) -> Option<SandboxCheckpointState> {
    let raw = std::fs::read_to_string(scratch.join(BOOT_RECORD)).ok()?;
    serde_json::from_str(&raw).ok()
}

fn write_boot_record(scratch: &Path, state: &SandboxCheckpointState) -> R<()> {
    std::fs::write(scratch.join(BOOT_RECORD), serde_json::to_vec_pretty(state)?)
        .with_context(|| format!("failed to write {}", scratch.join(BOOT_RECORD).display()))
}

/// The identity a sandbox boots with: written to its identity disk at first
/// boot so the disk is never empty, then rewritten on every restore.
pub(crate) fn boot_identity(
    allocation: &Allocation,
    task_name: &str,
    hostname: &str,
    tokens: &GuestTokens,
) -> ResumeIdentity {
    ResumeIdentity {
        alloc_id: allocation.id,
        instance_id: allocation.instance_id.clone(),
        job_id: allocation.job_id.clone(),
        task_name: task_name.to_string(),
        hostname: hostname.to_string(),
        token: None,
        mac: None,
        env: tokens.env.clone(),
        entropy: fresh_entropy(),
        host_time_unix_ms: chrono::Utc::now().timestamp_millis(),
        on_resume: Vec::new(),
        fork: false,
        files: tokens.files.clone(),
        guest_addr: None,
        // The boot agent learns its ingress ports from its command line; the
        // identity only carries them on a restore.
        ingress_ports: Vec::new(),
    }
}

/// The workload-identity tokens a sandbox keeps in its guest through the
/// identity disk (docs/workload-identity.md, "Sandboxes"): env-delivered
/// ones by env key (published at `/legion/identity/<ENV>`) and
/// file-delivered ones at their guest paths.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GuestTokens {
    pub env: std::collections::BTreeMap<String, String>,
    pub files: Vec<crate::structs::checkpoint::GuestTokenFile>,
}

/// Put `tokens` on a running guest's identity disk at `path`, keeping the
/// rest of what the disk says (allocation, host time), so the guest's
/// handler sees a token refresh and not a restore.
pub(crate) fn refresh_identity_tokens(path: &Path, tokens: &GuestTokens) -> R<bool> {
    let mut identity = crate::task_plugins::checkpoint::read_identity_disk(path)?;
    let mut env = identity
        .env
        .iter()
        .filter(|(key, _)| key.starts_with("LEGION_"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    env.extend(tokens.env.clone());
    let refreshed = ResumeIdentity {
        env,
        files: tokens.files.clone(),
        ..identity.clone()
    };
    if !refreshed.tokens_differ(&identity) {
        return Ok(false);
    }
    identity = refreshed;
    crate::task_plugins::checkpoint::rewrite_identity_disk_in_place(path, &identity)?;
    Ok(true)
}

/// Rewrite `spec` so the VMM sees stable link paths and an identity disk,
/// write the first identity, and record the layout. Called by the sandbox's
/// microVM launch when [`TaskInstance::checkpoint_launch`] is set.
pub(crate) fn prepare_checkpoint_launch(
    backend: &Backend,
    allocation: &Allocation,
    task_name: &str,
    instance: &TaskInstance,
    scratch: &Path,
    spec: &mut VmSpec,
    tokens: &GuestTokens,
) -> R<SandboxCheckpointState> {
    let state = SandboxCheckpointState {
        vmm: backend.vmm_name().to_string(),
        link_key: format!(
            "sbx-{}",
            crate::util::random_string(16).to_ascii_lowercase()
        ),
        volume_count: spec.volume_disks.len(),
        vcpus: spec.vcpus,
        memory_mib: spec.memory_mib,
        tap: spec.tap.as_ref().map(|tap| tap.device.clone()),
        network: Some(if spec.vsock.is_some() {
            GuestNetwork::Gated
        } else if spec.tap.is_some() {
            GuestNetwork::Tap
        } else {
            GuestNetwork::None
        }),
        guest_address: crate::task_plugins::sandbox_addr::read_lease(scratch)
            .map(|lease| lease.address.to_string()),
        guest_ports: boot_guest_ports(allocation.network.as_ref()),
    };
    let identity_path = scratch.join(IDENTITY_FILE);
    write_identity_disk(
        &identity_path,
        &boot_identity(allocation, task_name, &spec.name, tokens),
    )?;

    let links = DeviceLinks::new(&vmdev_root(instance, scratch), &state.link_key);
    let _guard = LINK_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    spec.root_disk = links.point(ROOT_LINK, &spec.root_disk)?;
    for (index, disk) in spec.volume_disks.iter_mut().enumerate() {
        disk.path = links.point(&volume_link(index), &disk.path)?;
    }
    spec.identity_disk = Some(links.point(IDENTITY_LINK, &identity_path)?);
    write_boot_record(scratch, &state)?;
    info!(
        allocation_id = allocation.id,
        vmm = %state.vmm,
        link_key = %state.link_key,
        "Launching sandbox for memory checkpoints (device links + identity disk)"
    );
    Ok(state)
}

/// Remove this allocation's device links, unless a later restore has
/// already repointed them at its own files.
pub(crate) fn release_links(instance: &TaskInstance, scratch: &Path) {
    let Some(state) = read_boot_record(scratch) else {
        return;
    };
    let links = DeviceLinks::new(&vmdev_root(instance, scratch), &state.link_key);
    let _guard = LINK_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if links.points_into(scratch) {
        links.remove();
    }
}

/// The VMM version as the API reports it, normalized like the fingerprint's.
fn api_version(backend: &Backend, socket: &Path) -> R<String> {
    let raw = match backend {
        Backend::Firecracker { .. } => FirecrackerApi::new(socket).version()?,
        Backend::CloudHypervisor { .. } => CloudHypervisorApi::new(socket).version()?,
        other => bail!("{} has no checkpoint API", other.label()),
    };
    Ok(microvm::parse_vmm_version(&raw).unwrap_or(raw))
}

/// Capture a running microVM into `out`.
///
/// On any failure after the pause, the VM is resumed before the error is
/// returned, so a failed checkpoint leaves the allocation running exactly as
/// it was (plan §9, Phase 4: "checkpoint failure leaves the allocation
/// running").
pub(crate) fn checkpoint_microvm(
    backend: &Backend,
    allocation: &Allocation,
    task_name: &str,
    instance: &mut TaskInstance,
    scratch: &Path,
    out: &CheckpointDir,
    then: AfterCheckpoint,
) -> R<MemoryParts> {
    let state = read_boot_record(scratch).with_context(|| {
        format!(
            "sandbox allocation {} was not launched for memory checkpoints; enable suspend or \
             snapshots on the job and restart it",
            allocation.id
        )
    })?;
    if state.vmm != backend.vmm_name() {
        bail!(
            "sandbox allocation {} runs {}, but this node resolved {}",
            allocation.id,
            state.vmm,
            backend.vmm_name()
        );
    }
    let socket = scratch.join("vmm.sock");
    let version = api_version(backend, &socket)?;
    // A gated guest's restore must move its vsock to a new socket, which an
    // old Firecracker cannot do. Refused before the pause, so the guest
    // never stops for a checkpoint that could not be restored. The manager
    // refuses the same thing first; this covers the node's own suspends.
    if state.network() == GuestNetwork::Gated {
        if let Some(refusal) = crate::structs::fork::gated_memory_checkpoint_refusal(
            &format!("sandbox allocation {}", allocation.id),
            &allocation.node_id,
            backend.vmm_name(),
            Some(&version),
            "use a disk checkpoint",
        ) {
            bail!("{refusal}");
        }
    }

    let started = std::time::Instant::now();
    let pause = || match backend {
        Backend::Firecracker { .. } => FirecrackerApi::new(&socket).pause(),
        _ => CloudHypervisorApi::new(&socket).pause(),
    };
    let resume = || match backend {
        Backend::Firecracker { .. } => FirecrackerApi::new(&socket).resume(),
        _ => CloudHypervisorApi::new(&socket).resume(),
    };
    pause()?;

    let captured = (|| -> R<Vec<(&'static str, &'static str)>> {
        let roles = match backend {
            Backend::Firecracker { .. } => {
                FirecrackerApi::new(&socket).create_snapshot(
                    &out.part_path(parts::FC_STATE),
                    &out.part_path(parts::FC_MEMORY),
                )?;
                vec![("memory", parts::FC_MEMORY), ("vm-state", parts::FC_STATE)]
            }
            _ => {
                CloudHypervisorApi::new(&socket).snapshot(out.path())?;
                vec![
                    ("memory", parts::CH_MEMORY),
                    ("vm-state", parts::CH_STATE),
                    ("vm-config", parts::CH_CONFIG),
                ]
            }
        };
        // Copied while paused, so the disk and the page cache that describes
        // it are one instant.
        let rootfs = out.part_path(parts::ROOTFS);
        let _ = std::fs::remove_file(&rootfs);
        clone_file(&scratch.join("rootfs.img"), &rootfs)?;
        let mut roles = roles;
        roles.push(("rootfs", parts::ROOTFS));
        Ok(roles)
    })();

    let roles = match captured {
        Ok(roles) => roles,
        Err(err) => {
            if let Err(resume_err) = resume() {
                warn!(
                    allocation_id = allocation.id,
                    error = %resume_err,
                    "Failed to resume sandbox after a failed checkpoint"
                );
            }
            return Err(err.context("sandbox memory checkpoint failed; the guest was resumed"));
        }
    };

    match then {
        AfterCheckpoint::Resume => resume()?,
        // Worker core snapshots the volumes while the guest cannot write,
        // then resumes it through `resume_microvm`.
        AfterCheckpoint::Pause => {}
        // Kill the paused VMM: the guest never runs past the instant the
        // checkpoint captured.
        AfterCheckpoint::Stop => {
            crate::task_plugins::reap_task_process(allocation.id, task_name, instance, "sandbox")
        }
    }
    let paused_ms = started.elapsed().as_millis() as u64;

    let sealed = out.seal(&roles)?;
    let memory = MemoryParts {
        parts: sealed,
        runtime: serde_json::to_value(&state)?,
        compat: SnapshotCompat {
            vmm: Some(VmmCompat {
                kind: state.vmm.clone(),
                version,
            }),
            guest: Some(GuestCompat {
                vcpus: state.vcpus,
                memory_mib: state.memory_mib,
            }),
            cpu: None,
        },
    };
    info!(
        allocation_id = allocation.id,
        vmm = %state.vmm,
        paused_ms,
        total_bytes = memory.total_bytes(),
        then = ?then,
        "Checkpointed sandbox memory"
    );
    Ok(memory)
}

/// Resume a guest that [`checkpoint_microvm`] left paused
/// ([`AfterCheckpoint::Pause`]).
pub(crate) fn resume_microvm(backend: &Backend, scratch: &Path) -> R<()> {
    let socket = scratch.join("vmm.sock");
    match backend {
        Backend::Firecracker { .. } => FirecrackerApi::new(&socket).resume(),
        Backend::CloudHypervisor { .. } => CloudHypervisorApi::new(&socket).resume(),
        other => bail!("{} has no checkpoint API", other.label()),
    }
}

/// What a host TAP device looks like right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// Only the Linux host constructs these outside tests.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) enum TapState {
    /// No network interface has this name.
    Missing,
    /// A TAP device with this name exists and nothing holds it.
    Free,
    /// A TAP device with this name exists and another process holds it.
    Busy,
    /// An interface with this name exists but is not a TAP device.
    NotTap,
}

/// The host's TAP devices, behind a seam so restore's TAP handling is
/// testable without root or Linux.
pub(crate) trait TapHost {
    fn state(&self, device: &str) -> R<TapState>;
    /// Create a persistent TAP device named `device`.
    fn create(&self, device: &str) -> R<()>;
    /// Bring `device` administratively up.
    fn set_up(&self, device: &str) -> R<()>;
    /// Delete `device`.
    fn delete(&self, device: &str) -> R<()>;
    /// The bridge (or other master) `device` is enslaved to, if any.
    fn master(&self, device: &str) -> R<Option<String>>;
    /// Enslave `device` to `master`.
    fn set_master(&self, device: &str, master: &str) -> R<()>;
}

/// The real host: `/sys/class/net`, a `TUNSETIFF` probe, and `ip`.
pub(crate) struct HostTaps;

/// Written into the scratch dir when a restore created the allocation's TAP
/// device, so destroy removes exactly what Legion made and nothing an
/// operator made.
const RESTORED_TAP_RECORD: &str = "restored-tap";

/// Written into the scratch dir whenever a guest is wired to a TAP device
/// (any mode), naming it. The usage sampler counts the device's bytes as
/// the allocation's network activity, and the `boot` network gate shuts and
/// opens it, so neither has to re-derive an auto or fork TAP's name.
const TAP_DEVICE_RECORD: &str = "tap-device";

/// Record the TAP device the guest in `scratch` is wired to.
pub(crate) fn record_attached_tap(scratch: &Path, device: &str) -> R<()> {
    std::fs::write(scratch.join(TAP_DEVICE_RECORD), device)
        .with_context(|| format!("failed to record TAP device {device}"))
}

/// The TAP device the guest in `scratch` is wired to, if any.
pub fn attached_tap(scratch: &Path) -> Option<String> {
    let device = std::fs::read_to_string(scratch.join(TAP_DEVICE_RECORD)).ok()?;
    let device = device.trim();
    (!device.is_empty()).then(|| device.to_string())
}

#[cfg(target_os = "linux")]
fn run_ip(args: &[&str]) -> R<()> {
    let output = Command::new("ip")
        .args(args)
        .output()
        .with_context(|| format!("failed to run `ip {}`", args.join(" ")))?;
    if !output.status.success() {
        bail!(
            "`ip {}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Attach to `device` through `/dev/net/tun` and let go again. `EBUSY` means
/// another process (another VMM) holds it; a single-queue TAP takes one
/// holder only, so the restored VMM's own attach would fail the same way.
#[cfg(target_os = "linux")]
fn tap_is_free(device: &str) -> R<bool> {
    use std::os::fd::AsRawFd;
    #[repr(C)]
    struct IfReq {
        name: [libc::c_char; libc::IFNAMSIZ],
        flags: libc::c_short,
        _pad: [u8; 22],
    }
    // _IOW('T', 202, int) on every architecture Legion ships for.
    const TUNSETIFF: u64 = 0x4004_54ca;
    if device.len() >= libc::IFNAMSIZ {
        bail!("TAP device name {device:?} is longer than the kernel allows");
    }
    let tun = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/net/tun")
        .context("failed to open /dev/net/tun")?;
    let mut request = IfReq {
        name: [0; libc::IFNAMSIZ],
        flags: (libc::IFF_TAP | libc::IFF_NO_PI) as libc::c_short,
        _pad: [0; 22],
    };
    for (slot, byte) in request.name.iter_mut().zip(device.bytes()) {
        *slot = byte as libc::c_char;
    }
    // SAFETY: `request` is a valid, initialized `struct ifreq` prefix the
    // kernel reads and writes in place, and `tun` stays open for the call.
    let rc = unsafe {
        libc::ioctl(
            tun.as_raw_fd(),
            TUNSETIFF as _,
            std::ptr::addr_of_mut!(request),
        )
    };
    if rc == 0 {
        return Ok(true);
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EBUSY) {
        return Ok(false);
    }
    Err(anyhow::Error::new(err).context(format!("failed to probe TAP device {device}")))
}

impl TapHost for HostTaps {
    #[cfg(target_os = "linux")]
    fn state(&self, device: &str) -> R<TapState> {
        let sys = Path::new("/sys/class/net").join(device);
        if !sys.exists() {
            return Ok(TapState::Missing);
        }
        if !sys.join("tun_flags").exists() {
            return Ok(TapState::NotTap);
        }
        Ok(if tap_is_free(device)? {
            TapState::Free
        } else {
            TapState::Busy
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn state(&self, _device: &str) -> R<TapState> {
        bail!("TAP devices need Linux; this host is not Linux")
    }

    #[cfg(target_os = "linux")]
    fn create(&self, device: &str) -> R<()> {
        run_ip(&["tuntap", "add", "dev", device, "mode", "tap"])
    }

    #[cfg(not(target_os = "linux"))]
    fn create(&self, _device: &str) -> R<()> {
        bail!("TAP devices need Linux; this host is not Linux")
    }

    #[cfg(target_os = "linux")]
    fn set_up(&self, device: &str) -> R<()> {
        run_ip(&["link", "set", "dev", device, "up"])
    }

    #[cfg(not(target_os = "linux"))]
    fn set_up(&self, _device: &str) -> R<()> {
        bail!("TAP devices need Linux; this host is not Linux")
    }

    #[cfg(target_os = "linux")]
    fn delete(&self, device: &str) -> R<()> {
        run_ip(&["link", "delete", device])
    }

    #[cfg(not(target_os = "linux"))]
    fn delete(&self, _device: &str) -> R<()> {
        bail!("TAP devices need Linux; this host is not Linux")
    }

    #[cfg(target_os = "linux")]
    fn master(&self, device: &str) -> R<Option<String>> {
        let link = Path::new("/sys/class/net").join(device).join("master");
        Ok(std::fs::read_link(&link).ok().and_then(|target| {
            target
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        }))
    }

    #[cfg(not(target_os = "linux"))]
    fn master(&self, _device: &str) -> R<Option<String>> {
        bail!("TAP devices need Linux; this host is not Linux")
    }

    #[cfg(target_os = "linux")]
    fn set_master(&self, device: &str, master: &str) -> R<()> {
        run_ip(&["link", "set", "dev", device, "master", master])
    }

    #[cfg(not(target_os = "linux"))]
    fn set_master(&self, _device: &str, _master: &str) -> R<()> {
        bail!("TAP devices need Linux; this host is not Linux")
    }
}

/// Create `device` as a persistent TAP Legion owns: recorded in `scratch`
/// (so destroy removes exactly what Legion made), enslaved to `master` when
/// one is given, and brought up. `need` names the device in every error
/// (`restore needs TAP device <name>`, `sandbox needs TAP device <name>`).
///
/// One path for the three places Legion makes a TAP: a fork's own device, a
/// restore whose recorded device is gone, and an auto TAP at boot.
fn create_owned_tap(
    taps: &dyn TapHost,
    device: &str,
    master: Option<&str>,
    scratch: &Path,
    need: &str,
) -> R<()> {
    taps.create(device)
        .with_context(|| format!("{need}: creating it failed"))?;
    std::fs::write(scratch.join(RESTORED_TAP_RECORD), device)?;
    if let Some(master) = master {
        taps.set_master(device, master)
            .with_context(|| format!("{need}: attaching it to bridge {master} failed"))?;
    }
    taps.set_up(device)
        .with_context(|| format!("{need}: bringing it up failed"))?;
    Ok(())
}

/// Phase 6: give a guest restored into a NEW instance (a fork, or a create
/// from a snapshot) a TAP device of its own, `own`, instead of the `origin`
/// TAP its checkpoint recorded (which the origin, or a sibling fork, holds).
///
/// `own` is created when missing (and recorded, so destroy removes it),
/// brought up, and enslaved to `bridge` when the task names one (an auto
/// TAP), else to the same bridge as `origin`, so the fork lands on the
/// origin's network segment. An operator may pre-create `own` to wire it
/// differently: a free existing TAP is used as is. Every failure is a named
/// `restore needs TAP device <own>: ...` error.
pub(crate) fn ensure_fork_tap(
    taps: &dyn TapHost,
    origin: &str,
    configured: Option<&str>,
    bridge: Option<&str>,
    own: &str,
    scratch: &Path,
) -> R<()> {
    let state = taps
        .state(own)
        .with_context(|| format!("restore needs TAP device {own}"))?;
    match state {
        TapState::Free => Ok(()),
        TapState::Busy => bail!(
            "restore needs TAP device {own} (this fork's own TAP): it exists and another \
             process holds it"
        ),
        TapState::NotTap => bail!(
            "restore needs TAP device {own} (this fork's own TAP): a network interface with \
             that name exists but is not a TAP device"
        ),
        TapState::Missing => {
            // An auto TAP names its bridge, which wins. Otherwise the
            // origin's TAP is how the operator wired the network, and a
            // fork joins the same bridge. On another node (a spread fork)
            // the origin's TAP may not exist, so the task's configured TAP
            // there says which bridge to join. A TAP with no master stays
            // standalone, but a fork that finds neither device would have
            // no network at all, which is refused by name.
            let master = bridge.map(str::to_string).or_else(|| {
                taps.master(origin).ok().flatten().or_else(|| {
                    configured
                        .filter(|device| *device != origin)
                        .and_then(|device| taps.master(device).ok().flatten())
                })
            });
            if master.is_none() && matches!(taps.state(origin), Ok(TapState::Missing)) {
                let configured = configured.unwrap_or(origin);
                bail!(
                    "restore needs TAP device {own}: the origin's TAP {origin} is not on this \
                     node, and the task's TAP {configured} has no bridge here for this fork to \
                     join, so the fork would have no network. Create {configured} on a bridge on \
                     every node a spread fork can land on"
                );
            }
            create_owned_tap(
                taps,
                own,
                master.as_deref(),
                scratch,
                &format!("restore needs TAP device {own} (this fork's own TAP)"),
            )?;
            info!(
                device = own,
                origin, "Created a forked sandbox's own TAP device"
            );
            Ok(())
        }
    }
}

/// Wire a fresh boot to its TAP (`config.network = { mode = "tap", bridge }`):
/// `device` is this instance's own TAP ([`crate::structs::fork::own_tap_name`]),
/// created and attached to `bridge` when missing.
///
/// A free device of that name is reused and re-attached to `bridge` if it
/// drifted: Legion owns `lgf` names, and one left behind by an allocation
/// that never got to clean up (a worker crash) belongs to this instance. A
/// busy one means this instance already runs here, which is refused by name.
pub(crate) fn ensure_auto_tap(
    taps: &dyn TapHost,
    device: &str,
    bridge: &str,
    scratch: &Path,
) -> R<()> {
    let need = format!("sandbox needs TAP device {device} on bridge {bridge}");
    let state = taps.state(device).with_context(|| need.clone())?;
    match state {
        TapState::Free => {
            std::fs::write(scratch.join(RESTORED_TAP_RECORD), device)?;
            if taps.master(device).ok().flatten().as_deref() != Some(bridge) {
                taps.set_master(device, bridge)
                    .with_context(|| format!("{need}: attaching it to bridge {bridge} failed"))?;
            }
            taps.set_up(device)
                .with_context(|| format!("{need}: bringing it up failed"))?;
            Ok(())
        }
        TapState::Busy => bail!(
            "{need}: it exists and another process holds it, so this instance already runs a \
             guest on this node"
        ),
        TapState::NotTap => {
            bail!("{need}: a network interface with that name exists but is not a TAP device")
        }
        TapState::Missing => {
            create_owned_tap(taps, device, Some(bridge), scratch, &need)?;
            info!(device, bridge, "Created a sandbox's auto TAP device");
            Ok(())
        }
    }
}

/// Make sure the TAP device a memory checkpoint recorded exists, is free,
/// and is the one the task's config names, before the VMM loads the saved
/// state that refers to it.
///
/// Both VMMs reopen the recorded device name on restore, so a missing,
/// busy, or different TAP would either fail the load deep inside the VMM or,
/// worse, come up without the network the guest thinks it has. Every
/// failure here is a named error (`restore needs TAP device <name>: ...`)
/// that fails the restore and reaches the snapshot report and its event.
///
/// A missing device is created the way an operator creates one for `mode:
/// 'tap'` (a persistent TAP, administratively up) and recorded in `scratch`
/// so destroy removes it again.
pub(crate) fn ensure_restore_tap(
    taps: &dyn TapHost,
    recorded: Option<&str>,
    configured: Option<&str>,
    bridge: Option<&str>,
    scratch: &Path,
) -> R<()> {
    let device = match (recorded, configured) {
        (None, None) => return Ok(()),
        (None, Some(configured)) => bail!(
            "restore needs the guest's network layout: the checkpoint was taken without a TAP \
             device, but the task now attaches TAP {configured}; restore a snapshot taken with \
             this network config, or restart the instance cold"
        ),
        (Some(recorded), configured) if configured != Some(recorded) => bail!(
            "restore needs TAP device {recorded}: the checkpoint's guest is wired to it, but the \
             task now names {}; restore a snapshot taken with this network config, or restart \
             the instance cold",
            configured
                .map(|device| format!("TAP {device}"))
                .unwrap_or_else(|| "no TAP device".to_string())
        ),
        (Some(recorded), _) => recorded,
    };
    let state = taps
        .state(device)
        .with_context(|| format!("restore needs TAP device {device}"))?;
    match state {
        TapState::Free => Ok(()),
        TapState::Busy => bail!(
            "restore needs TAP device {device}: it exists and another process holds it (most \
             likely another VM); stop that workload or give this task its own TAP"
        ),
        TapState::NotTap => bail!(
            "restore needs TAP device {device}: a network interface with that name exists but is \
             not a TAP device"
        ),
        TapState::Missing => {
            // An auto TAP is recreated on its bridge, under the name the
            // checkpoint recorded. A fixed one comes back standalone, the
            // way an operator creates one for `mode: 'tap'`.
            create_owned_tap(
                taps,
                device,
                bridge,
                scratch,
                &format!("restore needs TAP device {device}"),
            )?;
            info!(
                device,
                bridge, "Created the TAP device a sandbox restore needs"
            );
            Ok(())
        }
    }
}

/// Remove a TAP device that [`ensure_restore_tap`] created for this
/// allocation. Best effort: a leftover device only costs a name.
pub(crate) fn release_restore_tap(taps: &dyn TapHost, scratch: &Path) {
    let record = scratch.join(RESTORED_TAP_RECORD);
    let Ok(device) = std::fs::read_to_string(&record) else {
        return;
    };
    let device = device.trim();
    if let Err(err) = taps.delete(device) {
        warn!(device, error = %err, "Failed to remove the TAP device a restore created");
    }
    let _ = std::fs::remove_file(&record);
}

/// cloud-hypervisor reopens the TAP devices and the vsock socket named in
/// the snapshot's `config.json`. Rewrite that part, in the restore's own
/// source directory (where it is a link into the content store, so it is
/// replaced rather than edited), to name this restore's own TAP (`tap`, a
/// fork) and its own vsock socket (`vsock`, every gated restore), in one
/// pass.
fn retarget_cloud_hypervisor_config(
    source: &CheckpointDir,
    tap: Option<&str>,
    vsock: Option<&Path>,
) -> R<()> {
    if tap.is_none() && vsock.is_none() {
        return Ok(());
    }
    let path = source.part_path(parts::CH_CONFIG);
    let raw = std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut config: serde_json::Value =
        serde_json::from_slice(&raw).with_context(|| format!("{} is not JSON", path.display()))?;
    if let Some(own) = tap {
        let Some(nets) = config
            .get_mut("net")
            .and_then(serde_json::Value::as_array_mut)
        else {
            bail!(
                "restore needs TAP device {own}: the checkpoint's config.json has no network \
                 device"
            );
        };
        for net in nets {
            net["tap"] = serde_json::Value::String(own.to_string());
        }
    }
    if let Some(socket) = vsock {
        let Some(device) = config.get_mut("vsock").filter(|device| device.is_object()) else {
            bail!(
                "restore needs gated networking: the checkpoint's config.json has no vsock device"
            );
        };
        device["socket"] = serde_json::Value::String(socket.display().to_string());
    }
    let _ = std::fs::remove_file(&path);
    std::fs::write(&path, serde_json::to_vec(&config)?)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

/// How long a gated restore waits for the datapath's egress proxy socket
/// before resuming the guest anyway (gated plan §6.3, calls entry 30).
pub(crate) const DATAPATH_WAIT: Duration = Duration::from_secs(5);

/// Wait up to `timeout` for the egress proxy's socket beside a gated guest's
/// vsock base (`<base>_1080`). The heartbeat that delivered the restore
/// binds it before reconcile starts the restore, so this is normally
/// immediate. False when it never appeared.
pub(crate) fn wait_for_datapath(base: &Path, timeout: Duration) -> bool {
    let proxy = crate::worker::netd::guest_agent::host_socket_path(
        base,
        crate::worker::netd::guest_agent::PROXY_PORT,
    );
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if proxy.exists() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// What [`restore_microvm`] needs about the task, resolved by the caller.
pub(crate) struct RestoreTarget<'a> {
    pub backend: &'a Backend,
    pub allocation: &'a Allocation,
    pub task_name: &'a str,
    pub hostname: String,
    pub vcpus: u32,
    pub memory_mib: u32,
    pub scratch: PathBuf,
    /// The TAP device the task's current config attaches, if any. Restore
    /// uses the destination's config, never the manifest's, and refuses when
    /// the two disagree. For an auto TAP this is the instance's own device.
    pub tap: Option<String>,
    /// The bridge an auto TAP joins (`config.network.bridge`). A restore
    /// that has to recreate the device attaches it here.
    pub bridge: Option<String>,
    /// The restoring allocation's own vsock socket base, when it is gated
    /// (`<alloc>/.sandbox/vsock.sock`). Every gated restore moves the guest
    /// onto it, and a gated checkpoint restores only into a gated task.
    pub vsock: Option<PathBuf>,
    /// How long to wait for the datapath's socket before resuming a gated
    /// guest fail-closed ([`DATAPATH_WAIT`]; tests shorten it).
    pub datapath_wait: Duration,
}

impl RestoreTarget<'_> {
    /// The network shape the task's current config gives a guest.
    fn network(&self) -> GuestNetwork {
        if self.vsock.is_some() {
            GuestNetwork::Gated
        } else if self.tap.is_some() {
            GuestNetwork::Tap
        } else {
            GuestNetwork::None
        }
    }
}

/// Phase 6: which TAP device a restore attaches the guest NIC to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RestoreTap {
    /// The device the checkpoint recorded, checked against the task's config
    /// (a resume or a rollback of the same instance).
    Recorded { configured: Option<String> },
    /// A restore into a new instance: move the NIC from the recorded device
    /// to this instance's own one.
    Own { origin: String, own: String },
}

/// Decide the TAP a restore uses. A new instance (`identity.fork`) whose
/// task wires a TAP gets its own device, named from its instance id. An
/// instance that was itself a fork records that own device in its later
/// checkpoints, so resuming one of those keeps it. A checkpoint wired to
/// any other device (a hand-named TAP from before every TAP was per
/// instance) also moves onto the instance's own device.
pub(crate) fn restore_tap(
    recorded: Option<&str>,
    configured: Option<&str>,
    identity: &ResumeIdentity,
) -> RestoreTap {
    let own = identity
        .instance_id
        .as_deref()
        .map(crate::structs::fork::own_tap_name);
    match (recorded, configured, own) {
        (Some(recorded), Some(_), Some(own)) if recorded == own && !identity.fork => {
            RestoreTap::Recorded {
                configured: Some(own),
            }
        }
        // A fork, and a checkpoint wired to a device that is not this
        // instance's own (one taken on a hand-named TAP before every TAP
        // was per instance), move the NIC onto the instance's own device.
        (Some(origin), Some(_), Some(own)) => RestoreTap::Own {
            origin: origin.to_string(),
            own,
        },
        _ => RestoreTap::Recorded {
            configured: configured.map(str::to_string),
        },
    }
}

/// The address a TAP guest held when `manifest` was captured, which a resume
/// asks its bridge's pool for first.
pub(crate) fn recorded_guest_address(manifest: &SnapshotManifest) -> Option<std::net::Ipv4Addr> {
    serde_json::from_value::<SandboxCheckpointState>(manifest.runtime_state.clone())
        .ok()?
        .guest_address?
        .parse()
        .ok()
}

/// Check a manifest against the task it is about to be restored into.
pub(crate) fn check_restorable(
    manifest: &SnapshotManifest,
    target: &RestoreTarget<'_>,
) -> R<SandboxCheckpointState> {
    if manifest.kind != SnapshotKind::Memory || manifest.runtime != "sandbox" {
        bail!(
            "checkpoint {} is a {} checkpoint from runtime `{}`, not a sandbox memory checkpoint",
            manifest.id,
            manifest.kind.as_str(),
            manifest.runtime
        );
    }
    let state: SandboxCheckpointState = serde_json::from_value(manifest.runtime_state.clone())
        .with_context(|| format!("checkpoint {} has no sandbox restore state", manifest.id))?;
    if state.vmm != target.backend.vmm_name() {
        bail!(
            "checkpoint {} was taken on {}, this task resolves {} on this node",
            manifest.id,
            state.vmm,
            target.backend.vmm_name()
        );
    }
    // A gated guest has no NIC to attach and a TAP guest has no vsock to
    // move, so a restore into the other shape is refused by name and
    // `onRestoreFailure` decides what happens next. TAP against no network
    // is `ensure_restore_tap`'s, which names the devices.
    let (saved, wanted) = (state.network(), target.network());
    if saved != wanted && (saved == GuestNetwork::Gated || wanted == GuestNetwork::Gated) {
        let now = match (wanted, target.bridge.as_deref(), target.tap.as_deref()) {
            (GuestNetwork::Gated, _, _) => "is now gated (a vsock datapath and no NIC)".to_string(),
            (_, Some(bridge), _) => format!("is now wired to bridge {bridge}"),
            (_, None, Some(tap)) => format!("is now wired to TAP {tap}"),
            _ => "now has no network device".to_string(),
        };
        let need = if saved == GuestNetwork::Gated {
            "gated networking"
        } else {
            "the guest's network layout"
        };
        bail!(
            "restore needs {need}: the checkpoint's guest {}, but task {} {now}; restore a \
             snapshot taken with this network config, or restart the instance cold",
            saved.describe(),
            target.task_name
        );
    }
    if (state.vcpus, state.memory_mib) != (target.vcpus, target.memory_mib) {
        bail!(
            "checkpoint {} holds a {} vCPU / {} MiB guest, the task now asks for {} vCPU / {} MiB; \
             a memory checkpoint restores only into the same guest shape",
            manifest.id,
            state.vcpus,
            state.memory_mib,
            target.vcpus,
            target.memory_mib
        );
    }
    Ok(state)
}

/// Before resuming a restored gated guest, give the datapath up to
/// `target.datapath_wait` to bind the egress proxy on the new allocation's
/// socket. A guest that resumes without it fails closed (each dial is
/// reset) and the node reports the unbound socket on its next heartbeat,
/// which is better than turning a transient bind error into a cold boot.
fn await_datapath(target: &RestoreTarget<'_>) {
    let Some(base) = target.vsock.as_deref() else {
        return;
    };
    if !wait_for_datapath(base, target.datapath_wait) {
        warn!(
            allocation_id = target.allocation.id,
            socket = %crate::worker::netd::guest_agent::host_socket_path(
                base,
                crate::worker::netd::guest_agent::PROXY_PORT
            )
            .display(),
            "Resuming a restored gated sandbox before its egress proxy socket is bound; it has no network until the datapath binds it"
        );
    }
}

/// Launch a VMM from a memory checkpoint. Returns the spawned VMM command
/// already started, recorded on `instance` like a normal start.
pub(crate) fn restore_microvm(
    target: &RestoreTarget<'_>,
    instance: &mut TaskInstance,
    manifest: &SnapshotManifest,
    source: &CheckpointDir,
    identity: &ResumeIdentity,
    taps: &dyn TapHost,
) -> R<()> {
    let state = check_restorable(manifest, target)?;
    source.verify(&manifest.memory, false)?;
    if instance.volumes.len() != state.volume_count {
        bail!(
            "checkpoint {} attached {} volume disks, this allocation mounted {}",
            manifest.id,
            state.volume_count,
            instance.volumes.len()
        );
    }

    let scratch = &target.scratch;
    std::fs::create_dir_all(scratch)?;
    crate::util::restrict_dir_to_owner(scratch)?;

    // This allocation's own copies: the root disk the guest was using, and
    // an identity disk naming the new allocation.
    let root_disk = scratch.join("rootfs.img");
    let _ = std::fs::remove_file(&root_disk);
    clone_file(&source.part_path(parts::ROOTFS), &root_disk)?;
    // The part may come from the node content store, whose blobs are
    // read-only; this copy is the guest's writable root disk.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root_disk, std::fs::Permissions::from_mode(0o600))?;
    }
    // Before the VMM opens anything: the saved state names the TAP device.
    // A restore into a new instance moves the NIC to a TAP of its own.
    let tap = restore_tap(state.tap.as_deref(), target.tap.as_deref(), identity);
    let moved_to = match &tap {
        RestoreTap::Recorded { configured } => {
            ensure_restore_tap(
                taps,
                state.tap.as_deref(),
                configured.as_deref(),
                target.bridge.as_deref(),
                scratch,
            )?;
            None
        }
        RestoreTap::Own { origin, own } => {
            ensure_fork_tap(
                taps,
                origin,
                target.tap.as_deref(),
                target.bridge.as_deref(),
                own,
                scratch,
            )?;
            Some(own.clone())
        }
    };
    // A gated guest moves onto this allocation's own vsock socket. The
    // VMM binds the base itself, so a stale one from an earlier start is
    // cleared; the datapath's `<base>_<port>` listeners are left alone.
    if let Some(base) = target.vsock.as_deref() {
        microvm::check_vsock_socket_path(base)?;
        let _ = std::fs::remove_file(base);
    }
    if matches!(target.backend, Backend::CloudHypervisor { .. }) {
        retarget_cloud_hypervisor_config(source, moved_to.as_deref(), target.vsock.as_deref())?;
    }
    if let Some(device) = moved_to.as_deref().or(state.tap.as_deref()) {
        record_attached_tap(scratch, device)?;
    }
    let identity_path = scratch.join(IDENTITY_FILE);
    write_identity_disk(&identity_path, identity)?;

    let socket = scratch.join("vmm.sock");
    let _ = std::fs::remove_file(&socket);
    let links = DeviceLinks::new(&vmdev_root(instance, scratch), &state.link_key);
    let started = std::time::Instant::now();

    let _guard = LINK_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    links.point(ROOT_LINK, &root_disk)?;
    for (index, volume) in instance.volumes.iter().enumerate() {
        links.point(&volume_link(index), &volume.host_path)?;
    }
    let identity_link = links.point(IDENTITY_LINK, &identity_path)?;

    let mut cmd = match target.backend {
        Backend::Firecracker { binary } => {
            let mut cmd = Command::new(binary);
            cmd.arg("--id")
                .arg(&target.hostname)
                .arg("--api-sock")
                .arg(&socket);
            cmd
        }
        Backend::CloudHypervisor { binary } => {
            let mut cmd = Command::new(binary);
            cmd.arg("--api-socket")
                .arg(&socket)
                .arg("--restore")
                .arg(format!(
                    "source_url={}",
                    crate::task_plugins::vmm_api::file_url(source.path())
                ));
            cmd
        }
        other => bail!("{} cannot restore a memory checkpoint", other.label()),
    };
    crate::task_plugins::configure_task_log_capture(&mut cmd, instance)?;
    // A restored VMM is governed exactly like a booted one: guest RAM under
    // the allocation's `memory.max`.
    crate::task_plugins::join_assignment_cgroup(&mut cmd, instance)?;
    let child = crate::task_plugins::spawn_task_process(&mut cmd)
        .context("failed to spawn the VMM for a restore")?;
    instance.pid = Some(child.id());
    instance.child = Some(child);

    // What a later checkpoint of this guest records: the TAP it is wired to
    // now, its new address, and the ports its workload still listens on.
    // Written before the guest resumes, because the datapath reads the ports
    // from it per connection: the first ingress connection after the resume
    // must already reach the port the restored workload holds.
    let next = SandboxCheckpointState {
        tap: moved_to.clone().or_else(|| state.tap.clone()),
        network: Some(state.network()),
        guest_address: identity
            .guest_addr
            .as_ref()
            .map(|addr| addr.address.clone()),
        guest_ports: restored_guest_ports(manifest, target.allocation.network.as_ref()),
        ..state.clone()
    };

    let loaded = (|| -> R<()> {
        wait_for_api_socket(&socket, API_SOCKET_TIMEOUT)?;
        match target.backend {
            Backend::Firecracker { .. } => {
                let api = FirecrackerApi::new(&socket);
                // Firecracker maps the memory file MAP_PRIVATE for the life of
                // the VM, so it gets this allocation's own link to the bytes
                // (a hard link, so it costs nothing and outlives the source).
                let memory = scratch.join(parts::FC_MEMORY);
                let _ = std::fs::remove_file(&memory);
                if std::fs::hard_link(source.part_path(parts::FC_MEMORY), &memory).is_err() {
                    clone_file(&source.part_path(parts::FC_MEMORY), &memory)?;
                }
                let overrides: Vec<(&str, &str)> = moved_to
                    .as_deref()
                    .map(|own| vec![(microvm::GUEST_NIC_ID, own)])
                    .unwrap_or_default();
                api.load_snapshot(
                    &source.part_path(parts::FC_STATE),
                    &memory,
                    false,
                    &overrides,
                    target.vsock.as_deref(),
                )?;
                // Belt and braces: the link already points at the new disk,
                // and the explicit repoint is what a fork relies on.
                api.update_drive(IDENTITY_DRIVE_ID, &identity_link)?;
                write_boot_record(scratch, &next)?;
                await_datapath(target);
                api.resume()
            }
            // `--restore` loaded the state at launch, paused.
            _ => {
                write_boot_record(scratch, &next)?;
                await_datapath(target);
                CloudHypervisorApi::new(&socket).resume()
            }
        }
    })();
    if let Err(err) = loaded {
        crate::task_plugins::reap_task_process(
            target.allocation.id,
            target.task_name,
            instance,
            "sandbox",
        );
        return Err(err.context(format!("restoring sandbox from checkpoint {}", manifest.id)));
    }
    info!(
        allocation_id = target.allocation.id,
        snapshot_id = %manifest.id,
        vmm = %next.vmm,
        restore_ms = started.elapsed().as_millis() as u64,
        "Restored sandbox from memory checkpoint"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_plugins::vmm_api::fake::{FakeVmm, Seen};
    use serde_json::json;

    #[test]
    fn microvm_backends_capture_memory_and_dev_backends_capture_disk() {
        let support = |config: Value, node: Option<&str>| {
            sandbox_checkpoint_support(&CheckpointQuery::new(&config).with_node_vmm(node))
        };
        assert_eq!(
            support(json!({"hypervisor": "firecracker"}), None),
            CheckpointSupport::Memory
        );
        assert_eq!(
            support(json!({"hypervisor": "cloud-hypervisor"}), None),
            CheckpointSupport::Memory
        );
        assert_eq!(
            support(json!({"hypervisor": "dev"}), None),
            CheckpointSupport::Disk
        );
        // `auto` follows the node's VMM when the caller knows it.
        assert_eq!(
            support(json!({}), Some("cloud-hypervisor")),
            CheckpointSupport::Memory
        );
        assert_eq!(
            support(json!({"hypervisor": "auto"}), Some("apple-container")),
            CheckpointSupport::Disk
        );
        assert_eq!(
            support(json!({"hypervisor": "qemu"}), None),
            CheckpointSupport::None
        );
    }

    #[test]
    fn firecracker_gets_an_api_socket_only_when_it_will_be_checkpointed() {
        assert!(firecracker_needs_api_socket(
            CheckpointSupport::Memory,
            true
        ));
        assert!(!firecracker_needs_api_socket(
            CheckpointSupport::Memory,
            false
        ));
        assert!(!firecracker_needs_api_socket(CheckpointSupport::Disk, true));
        assert!(!firecracker_needs_api_socket(CheckpointSupport::None, true));
    }

    #[test]
    fn device_links_repoint_atomically_and_release_only_their_own() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let first = tmp.path().join("allocs/1/.sandbox");
        let second = tmp.path().join("allocs/2/.sandbox");
        std::fs::create_dir_all(&first)?;
        std::fs::create_dir_all(&second)?;
        std::fs::write(first.join("rootfs.img"), b"one")?;
        std::fs::write(second.join("rootfs.img"), b"two")?;

        let links = DeviceLinks::new(&tmp.path().join("allocs/.vmdev"), "sbx-key");
        let link = links.point(ROOT_LINK, &first.join("rootfs.img"))?;
        assert_eq!(std::fs::read(&link)?, b"one");
        // A file opened through the link before the repoint keeps reading
        // the old target, which is what a running VMM relies on.
        let held = std::fs::File::open(&link)?;
        links.point(ROOT_LINK, &second.join("rootfs.img"))?;
        assert_eq!(std::fs::read(&link)?, b"two");
        let mut old = String::new();
        std::io::Read::read_to_string(&mut &held, &mut old)?;
        assert_eq!(old, "one");

        assert!(!links.points_into(&first));
        assert!(links.points_into(&second));
        Ok(())
    }

    fn alloc(id: i64) -> Allocation {
        Allocation {
            id,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
            instance_id: Some("user-42".into()),
            network: None,
        }
    }

    fn manifest_for(state: &SandboxCheckpointState, memory: MemoryParts) -> SnapshotManifest {
        SnapshotManifest {
            api_version: crate::structs::checkpoint::MANIFEST_API_VERSION,
            id: "snap-1".into(),
            job_id: "job".into(),
            task: "box".into(),
            origin_instance: Some("user-42".into()),
            origin_alloc_id: 1,
            runtime: "sandbox".into(),
            kind: SnapshotKind::Memory,
            created_at: "2026-10-10T00:00:00Z".into(),
            volumes: Vec::new(),
            memory: memory.parts,
            compat: memory.compat,
            runtime_state: serde_json::to_value(state).unwrap(),
        }
    }

    #[test]
    fn restore_refuses_another_vmm_or_guest_shape() {
        let state = SandboxCheckpointState {
            vmm: "firecracker".into(),
            link_key: "sbx-k".into(),
            volume_count: 0,
            vcpus: 2,
            memory_mib: 1024,
            tap: None,
            network: None,
            guest_address: None,
            guest_ports: BTreeMap::new(),
        };
        let allocation = alloc(9);
        let fc = Backend::Firecracker {
            binary: "firecracker".into(),
        };
        let ch = Backend::CloudHypervisor {
            binary: "cloud-hypervisor".into(),
        };
        let target = |backend, vcpus, memory_mib| RestoreTarget {
            backend,
            allocation: &allocation,
            task_name: "box",
            hostname: "legion-sbx-9".into(),
            vcpus,
            memory_mib,
            scratch: PathBuf::from("/nonexistent"),
            tap: None,
            bridge: None,
            vsock: None,
            datapath_wait: DATAPATH_WAIT,
        };
        let manifest = manifest_for(&state, MemoryParts::default());

        assert_eq!(
            check_restorable(&manifest, &target(&fc, 2, 1024)).unwrap(),
            state
        );
        let err = check_restorable(&manifest, &target(&ch, 2, 1024)).unwrap_err();
        assert!(err.to_string().contains("taken on firecracker"), "{err}");
        let err = check_restorable(&manifest, &target(&fc, 2, 2048)).unwrap_err();
        assert!(err.to_string().contains("same guest shape"), "{err}");

        let mut disk = manifest.clone();
        disk.kind = SnapshotKind::Disk;
        assert!(check_restorable(&disk, &target(&fc, 2, 1024)).is_err());
    }

    /// A sandbox launched for checkpoints, minus the VMM: scratch dir, root
    /// disk, links, identity disk, boot record.
    fn launched(root: &Path, id: i64, backend: &Backend) -> R<(TaskInstance, PathBuf, VmSpec)> {
        let instance = TaskInstance {
            alloc_dir: Some(root.join("allocs").join(id.to_string())),
            checkpoint_launch: true,
            ..TaskInstance::default()
        };
        let scratch = instance.alloc_dir.clone().unwrap().join(".sandbox");
        std::fs::create_dir_all(&scratch)?;
        std::fs::write(scratch.join("rootfs.img"), b"root disk contents")?;
        let mut spec = VmSpec {
            name: format!("legion-sbx-{id}"),
            kernel: PathBuf::from("/vmlinux"),
            extra_cmdline: None,
            root_disk: scratch.join("rootfs.img"),
            volume_disks: Vec::new(),
            vcpus: 1,
            memory_mib: 256,
            tap: None,
            api_socket: scratch.join("vmm.sock"),
            vsock: None,
            identity_disk: None,
            balloon: None,
        };
        prepare_checkpoint_launch(
            backend,
            &alloc(id),
            "box",
            &instance,
            &scratch,
            &mut spec,
            &GuestTokens::default(),
        )?;
        Ok((instance, scratch, spec))
    }

    #[test]
    fn a_checkpoint_launch_points_the_vmm_at_links_and_an_identity_disk() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let fc = Backend::Firecracker {
            binary: "/usr/bin/true".into(),
        };
        let (_instance, scratch, spec) = launched(tmp.path(), 7, &fc)?;
        let vmdev = tmp.path().join("allocs/.vmdev");
        assert!(
            spec.root_disk.starts_with(&vmdev),
            "{}",
            spec.root_disk.display()
        );
        assert_eq!(std::fs::read(&spec.root_disk)?, b"root disk contents");
        let identity = crate::task_plugins::checkpoint::read_identity_disk(
            spec.identity_disk.as_ref().unwrap(),
        )?;
        assert_eq!(identity.alloc_id, 7);
        assert_eq!(identity.instance_id.as_deref(), Some("user-42"));
        let record = read_boot_record(&scratch).expect("boot record");
        assert_eq!(record.vmm, "firecracker");
        assert_eq!((record.vcpus, record.memory_mib), (1, 256));
        Ok(())
    }

    /// The whole Firecracker capture-then-restore sequence against a fake
    /// VMM: pause, snapshot, kill; then a new allocation loads the state,
    /// repoints the identity drive, and resumes, with its own identity.
    #[test]
    fn firecracker_checkpoint_and_restore_drive_the_api_in_order() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let fc = Backend::Firecracker {
            binary: "/usr/bin/true".into(),
        };
        let (mut origin, origin_scratch, _) = launched(tmp.path(), 7, &fc)?;

        let vmm = FakeVmm::start_at(origin_scratch.join("vmm.sock"), false, fake_firecracker);
        let out = CheckpointDir::create(tmp.path().join("ckpt"))?;
        let parts = checkpoint_microvm(
            &fc,
            &alloc(7),
            "box",
            &mut origin,
            &origin_scratch,
            &out,
            AfterCheckpoint::Stop,
        )?;
        let calls = vmm.drain();
        assert_eq!(
            calls
                .iter()
                .map(|c| (c.method.as_str(), c.path.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("GET", "/version"),
                ("PATCH", "/vm"),
                ("PUT", "/snapshot/create")
            ],
            "Stop never resumes the guest"
        );
        let roles: Vec<&str> = parts.parts.iter().map(|p| p.role.as_str()).collect();
        assert_eq!(roles, vec!["memory", "vm-state", "rootfs"]);
        assert_eq!(parts.compat.vmm.as_ref().unwrap().version, "1.7.0");
        assert_eq!(parts.compat.guest.as_ref().unwrap().memory_mib, 256);
        assert_eq!(
            std::fs::read(out.part_path("rootfs.img"))?,
            b"root disk contents"
        );

        // Restore into allocation 8. The fake binds the new socket once the
        // restore has cleared the stale path (standing in for the VMM).
        let mut restored = TaskInstance {
            alloc_dir: Some(tmp.path().join("allocs/8")),
            ..TaskInstance::default()
        };
        let scratch = tmp.path().join("allocs/8/.sandbox");
        std::fs::create_dir_all(&scratch)?;
        std::fs::write(scratch.join("vmm.sock"), b"stale")?;
        let vmm = FakeVmm::start_at(scratch.join("vmm.sock"), true, fake_firecracker);
        let state: SandboxCheckpointState = serde_json::from_value(parts.runtime.clone())?;
        let manifest = manifest_for(&state, parts);
        let allocation = alloc(8);
        let identity = ResumeIdentity {
            alloc_id: 8,
            instance_id: Some("user-42".into()),
            job_id: "job".into(),
            task_name: "box".into(),
            hostname: "legion-sbx-8".into(),
            token: Some("fresh-token".into()),
            ..ResumeIdentity::default()
        };
        let target = RestoreTarget {
            backend: &fc,
            allocation: &allocation,
            task_name: "box",
            hostname: "legion-sbx-8".into(),
            vcpus: 1,
            memory_mib: 256,
            scratch: scratch.clone(),
            tap: None,
            bridge: None,
            vsock: None,
            datapath_wait: DATAPATH_WAIT,
        };
        restore_microvm(
            &target,
            &mut restored,
            &manifest,
            &out,
            &identity,
            &FakeTaps::default(),
        )?;

        let calls = vmm.drain();
        assert_eq!(
            calls
                .iter()
                .map(|c| (c.method.as_str(), c.path.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("PUT", "/snapshot/load"),
                ("PATCH", "/drives/identity"),
                ("PATCH", "/vm"),
            ]
        );
        assert_eq!(calls[0].body.as_ref().unwrap()["resume_vm"], false);
        assert_eq!(calls[2].body, Some(json!({"state": "Resumed"})));

        // The links now serve allocation 8's files, and its identity disk
        // names allocation 8 with the fresh token.
        let links = DeviceLinks::new(&tmp.path().join("allocs/.vmdev"), &state.link_key);
        assert!(links.points_into(&scratch));
        let written =
            crate::task_plugins::checkpoint::read_identity_disk(&scratch.join(IDENTITY_FILE))?;
        assert_eq!(written, identity);
        assert_eq!(
            std::fs::read(scratch.join("rootfs.img"))?,
            b"root disk contents"
        );
        assert!(
            read_boot_record(&scratch).is_some(),
            "restored VM can checkpoint again"
        );

        // Destroying the origin must not take allocation 8's links with it.
        release_links(&origin, &origin_scratch);
        assert!(links.points_into(&scratch));
        release_links(&restored, &scratch);
        assert!(!links.path(ROOT_LINK).exists());
        Ok(())
    }

    /// Phase 6: three forks of one TAP-wired Firecracker checkpoint. Each
    /// gets its own TAP (created, enslaved to the origin TAP's bridge, and
    /// named in `network_overrides`), its own MAC, hostname, and leased
    /// address on the identity disk (gated plan §6.7), and records its own
    /// TAP and address for its next checkpoint.
    #[test]
    fn forks_of_a_tap_wired_checkpoint_get_their_own_taps_and_macs() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let fc = Backend::Firecracker {
            binary: "/usr/bin/true".into(),
        };
        let allocs = tmp.path().join("allocs");
        let pool = crate::task_plugins::sandbox_addr::BridgePool::from_config(
            "br0",
            &crate::structs::config::SandboxBridgeConfig {
                subnet: "10.88.0.0/16".into(),
                gateway: Some("10.88.0.1".into()),
                range: Some("10.88.128.0/24".into()),
                dns: Vec::new(),
            },
        )?;
        // The origin leased an address before its launch, which records it
        // for its checkpoints.
        let origin_lease = crate::task_plugins::sandbox_addr::lease(
            &pool,
            &allocs.join("7").join(".sandbox"),
            Some(&allocs),
            None,
            "w-01",
        )?;
        let (mut origin, origin_scratch, _) = launched(tmp.path(), 7, &fc)?;
        let vmm = FakeVmm::start_at(origin_scratch.join("vmm.sock"), false, fake_firecracker);
        let out = CheckpointDir::create(tmp.path().join("ckpt"))?;
        let parts = checkpoint_microvm(
            &fc,
            &alloc(7),
            "box",
            &mut origin,
            &origin_scratch,
            &out,
            AfterCheckpoint::Resume,
        )?;
        drop(vmm);
        let mut state: SandboxCheckpointState = serde_json::from_value(parts.runtime.clone())?;
        assert_eq!(state.guest_address, Some(origin_lease.address.to_string()));
        state.tap = Some("tap0".into());
        let manifest = manifest_for(&state, parts);
        assert_eq!(
            recorded_guest_address(&manifest),
            Some(origin_lease.address)
        );
        let taps = FakeTaps {
            masters: [("tap0".to_string(), "br0".to_string())].into(),
            ..FakeTaps::default()
        };

        let mut own_taps = std::collections::HashSet::new();
        let mut macs = std::collections::HashSet::new();
        let mut addresses = std::collections::HashSet::from([origin_lease.address]);
        for (n, id) in [(1, 8), (2, 9), (3, 10)] {
            let instance_id = format!("run-17-f{n}");
            let own = crate::structs::fork::own_tap_name(&instance_id);
            let mut restored = TaskInstance {
                alloc_dir: Some(tmp.path().join(format!("allocs/{id}"))),
                ..TaskInstance::default()
            };
            let scratch = tmp.path().join(format!("allocs/{id}/.sandbox"));
            std::fs::create_dir_all(&scratch)?;
            std::fs::write(scratch.join("vmm.sock"), b"stale")?;
            let vmm = FakeVmm::start_at(scratch.join("vmm.sock"), true, fake_firecracker);
            let allocation = alloc(id);
            let identity = ResumeIdentity {
                alloc_id: id,
                instance_id: Some(instance_id.clone()),
                job_id: "job".into(),
                task_name: "box".into(),
                hostname: format!("legion-sbx-{id}"),
                mac: Some(crate::structs::fork::fork_mac(id)),
                fork: true,
                ..ResumeIdentity::default()
            };
            let target = RestoreTarget {
                backend: &fc,
                allocation: &allocation,
                task_name: "box",
                hostname: format!("legion-sbx-{id}"),
                vcpus: 1,
                memory_mib: 256,
                scratch: scratch.clone(),
                tap: Some("tap0".into()),
                bridge: None,
                vsock: None,
                datapath_wait: DATAPATH_WAIT,
            };
            // A fork asks for no previous address, so it never gets the
            // origin's.
            let lease = crate::task_plugins::sandbox_addr::lease(
                &pool,
                &scratch,
                Some(&allocs),
                None,
                "w-01",
            )?;
            let identity = ResumeIdentity {
                guest_addr: Some(lease.guest_addr()),
                ..identity
            };
            restore_microvm(&target, &mut restored, &manifest, &out, &identity, &taps)?;
            let calls = vmm.drain();
            assert_eq!(calls[0].path, "/snapshot/load");
            assert_eq!(
                calls[0].body.as_ref().unwrap()["network_overrides"],
                json!([{ "iface_id": "eth0", "host_dev_name": own }])
            );
            assert!(
                addresses.insert(lease.address),
                "fork {n} shares an address"
            );
            assert!(own_taps.insert(own.clone()), "fork {n} shares a TAP");
            let written =
                crate::task_plugins::checkpoint::read_identity_disk(&scratch.join(IDENTITY_FILE))?;
            assert!(macs.insert(written.mac.clone().expect("a fork gets a MAC")));
            assert_eq!(written.guest_addr, identity.guest_addr);
            assert_eq!(
                read_boot_record(&scratch).and_then(|record| record.guest_address),
                Some(lease.address.to_string()),
                "the fork's next checkpoint records its own address"
            );
            assert_eq!(written.hostname, format!("legion-sbx-{id}"));
            assert_eq!(
                read_boot_record(&scratch).and_then(|record| record.tap),
                Some(own.clone()),
                "the fork's next checkpoint records its own TAP"
            );
            // A later resume of this fork keeps its own TAP, even though the
            // task config names the origin's.
            assert_eq!(
                restore_tap(
                    Some(&own),
                    Some("tap0"),
                    &ResumeIdentity {
                        fork: false,
                        ..identity.clone()
                    }
                ),
                RestoreTap::Recorded {
                    configured: Some(own.clone())
                }
            );
            release_restore_tap(&taps, &scratch);
        }
        let log = taps.log.lock().unwrap().clone();
        for own in &own_taps {
            for step in [
                format!("create {own}"),
                format!("master {own} br0"),
                format!("up {own}"),
                format!("delete {own}"),
            ] {
                assert!(log.contains(&step), "missing {step:?} in {log:?}");
            }
        }
        assert!(
            !log.iter().any(|step| step.ends_with("tap0")),
            "the origin's TAP is never touched: {log:?}"
        );
        Ok(())
    }

    #[test]
    fn restore_tap_moves_forks_and_foreign_devices_onto_the_instance_tap() {
        let fork = ResumeIdentity {
            instance_id: Some("run-17-f1".into()),
            fork: true,
            ..ResumeIdentity::default()
        };
        assert_eq!(
            restore_tap(Some("tap0"), Some("tap0"), &fork),
            RestoreTap::Own {
                origin: "tap0".into(),
                own: crate::structs::fork::own_tap_name("run-17-f1"),
            }
        );
        // No NIC: nothing to move.
        assert_eq!(
            restore_tap(None, None, &fork),
            RestoreTap::Recorded { configured: None }
        );
        // A resume of the same instance keeps the recorded device.
        let resume = ResumeIdentity {
            fork: false,
            ..fork
        };
        let own = crate::structs::fork::own_tap_name("run-17-f1");
        assert_eq!(
            restore_tap(Some(&own), Some(&own), &resume),
            RestoreTap::Recorded {
                configured: Some(own.clone())
            }
        );
        // A checkpoint taken on a hand-named TAP (before every TAP was per
        // instance) moves onto the instance's own device on resume.
        assert_eq!(
            restore_tap(Some("tap0"), Some(&own), &resume),
            RestoreTap::Own {
                origin: "tap0".into(),
                own: own.clone(),
            }
        );
    }

    #[test]
    fn a_busy_fork_tap_is_a_named_failure() -> R<()> {
        let dir = tempfile::tempdir()?;
        let taps = FakeTaps {
            states: [("lgf0".to_string(), TapState::Busy)].into(),
            ..FakeTaps::default()
        };
        let err =
            ensure_fork_tap(&taps, "tap0", Some("tap0"), None, "lgf0", dir.path()).unwrap_err();
        assert!(
            format!("{err:#}").starts_with("restore needs TAP device lgf0"),
            "{err:#}"
        );
        // A standalone origin TAP (no bridge): the fork's TAP is created up,
        // with no master.
        let taps = FakeTaps {
            states: [("tap0".to_string(), TapState::Busy)].into(),
            ..FakeTaps::default()
        };
        ensure_fork_tap(&taps, "tap0", Some("tap0"), None, "lgf1", dir.path())?;
        assert_eq!(*taps.log.lock().unwrap(), vec!["create lgf1", "up lgf1"]);
        Ok(())
    }

    /// A spread fork restores on a node where the origin's TAP does not
    /// exist: it joins the bridge of the task's TAP there, and with neither
    /// device to follow it fails by name instead of booting with no network.
    #[test]
    fn a_fork_on_another_node_joins_the_configured_taps_bridge_or_fails() -> R<()> {
        let dir = tempfile::tempdir()?;
        // The origin was a fork itself (its TAP is lgf-named), and this node
        // has the task's tap0 on br1.
        let taps = FakeTaps {
            states: [("tap0".to_string(), TapState::Free)].into(),
            masters: [("tap0".to_string(), "br1".to_string())].into(),
            ..FakeTaps::default()
        };
        ensure_fork_tap(
            &taps,
            "lgf0aaaaaaaaaaa",
            Some("tap0"),
            None,
            "lgf2",
            dir.path(),
        )?;
        assert_eq!(
            *taps.log.lock().unwrap(),
            vec!["create lgf2", "master lgf2 br1", "up lgf2"]
        );

        // Neither the origin's TAP nor a bridged tap0 on this node.
        let taps = FakeTaps::default();
        let err =
            ensure_fork_tap(&taps, "tap0", Some("tap0"), None, "lgf3", dir.path()).unwrap_err();
        let message = format!("{err:#}");
        assert!(
            message.starts_with("restore needs TAP device lgf3")
                && message.contains("would have no network"),
            "{message}"
        );
        assert!(
            taps.log.lock().unwrap().is_empty(),
            "nothing is created for a fork that cannot be wired"
        );
        Ok(())
    }

    #[test]
    fn cloud_hypervisor_forks_rewrite_the_tap_in_their_own_config_copy() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let store_blob = tmp.path().join("blob-config.json");
        std::fs::write(
            &store_blob,
            serde_json::to_vec(&json!({ "net": [{ "tap": "tap0", "mac": "06:00:00:00:00:01" }] }))?,
        )?;
        let source = CheckpointDir::create(tmp.path().join("restore"))?;
        std::fs::hard_link(&store_blob, source.part_path(parts::CH_CONFIG))?;
        retarget_cloud_hypervisor_config(&source, Some("lgf0123456789ab"), None)?;
        let rewritten: serde_json::Value =
            serde_json::from_slice(&std::fs::read(source.part_path(parts::CH_CONFIG))?)?;
        assert_eq!(rewritten["net"][0]["tap"], "lgf0123456789ab");
        let original: serde_json::Value = serde_json::from_slice(&std::fs::read(&store_blob)?)?;
        assert_eq!(
            original["net"][0]["tap"], "tap0",
            "the content-store blob the part links to is never edited"
        );
        Ok(())
    }

    /// Gated plan §6.2: one rewrite of a restore's own `config.json` names
    /// the fork's TAP and the restoring allocation's vsock socket, and a
    /// gated restore of a config with no vsock device is refused by name.
    #[test]
    fn cloud_hypervisor_restores_rewrite_the_vsock_and_the_tap_in_one_pass() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let store_blob = tmp.path().join("blob-config.json");
        std::fs::write(
            &store_blob,
            serde_json::to_vec(&json!({
                "net": [{ "tap": "tap0" }],
                "vsock": { "cid": 3, "socket": "/allocs/7/.sandbox/vsock.sock", "id": "_vsock0" },
            }))?,
        )?;
        let source = CheckpointDir::create(tmp.path().join("restore"))?;
        std::fs::hard_link(&store_blob, source.part_path(parts::CH_CONFIG))?;
        retarget_cloud_hypervisor_config(
            &source,
            Some("lgf0123456789ab"),
            Some(Path::new("/allocs/9/.sandbox/vsock.sock")),
        )?;
        let rewritten: serde_json::Value =
            serde_json::from_slice(&std::fs::read(source.part_path(parts::CH_CONFIG))?)?;
        assert_eq!(rewritten["net"][0]["tap"], "lgf0123456789ab");
        assert_eq!(
            rewritten["vsock"]["socket"],
            "/allocs/9/.sandbox/vsock.sock"
        );
        assert_eq!(rewritten["vsock"]["cid"], 3, "the guest CID is left alone");
        let original: serde_json::Value = serde_json::from_slice(&std::fs::read(&store_blob)?)?;
        assert_eq!(
            original["vsock"]["socket"], "/allocs/7/.sandbox/vsock.sock",
            "the content-store blob the part links to is never edited"
        );

        let no_vsock = CheckpointDir::create(tmp.path().join("plain"))?;
        std::fs::write(
            no_vsock.part_path(parts::CH_CONFIG),
            serde_json::to_vec(&json!({ "net": [{ "tap": "tap0" }] }))?,
        )?;
        let err = retarget_cloud_hypervisor_config(
            &no_vsock,
            None,
            Some(Path::new("/allocs/9/.sandbox/vsock.sock")),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .starts_with("restore needs gated networking: the checkpoint's config.json"),
            "{err}"
        );
        Ok(())
    }

    /// A gated checkpoint restores only into a gated task, and a TAP or
    /// loopback-only one never into a gated task: both VMMs restore exactly
    /// the devices they saved.
    #[test]
    fn a_gated_checkpoint_refuses_a_tap_config_and_the_reverse() {
        let fc = Backend::Firecracker {
            binary: "firecracker".into(),
        };
        let allocation = alloc(9);
        let state = |network: Option<GuestNetwork>, tap: Option<&str>| SandboxCheckpointState {
            vmm: "firecracker".into(),
            link_key: "sbx-k".into(),
            volume_count: 0,
            vcpus: 1,
            memory_mib: 256,
            tap: tap.map(str::to_string),
            network,
            guest_address: None,
            guest_ports: BTreeMap::new(),
        };
        let target = |tap: Option<&str>, bridge: Option<&str>, gated: bool| RestoreTarget {
            backend: &fc,
            allocation: &allocation,
            task_name: "workspace",
            hostname: "legion-sbx-9".into(),
            vcpus: 1,
            memory_mib: 256,
            scratch: PathBuf::from("/allocs/9/.sandbox"),
            tap: tap.map(str::to_string),
            bridge: bridge.map(str::to_string),
            vsock: gated.then(|| PathBuf::from("/allocs/9/.sandbox/vsock.sock")),
            datapath_wait: DATAPATH_WAIT,
        };
        let gated = manifest_for(
            &state(Some(GuestNetwork::Gated), None),
            MemoryParts::default(),
        );
        assert!(check_restorable(&gated, &target(None, None, true)).is_ok());
        let err = check_restorable(&gated, &target(Some("lgf0"), Some("br0"), false)).unwrap_err();
        assert_eq!(
            err.to_string(),
            "restore needs gated networking: the checkpoint's guest has a vsock datapath and no \
             NIC, but task workspace is now wired to bridge br0; restore a snapshot taken with \
             this network config, or restart the instance cold"
        );
        let err = check_restorable(&gated, &target(None, None, false)).unwrap_err();
        assert!(
            err.to_string().contains("now has no network device"),
            "{err}"
        );

        let tap = manifest_for(&state(None, Some("lgf0")), MemoryParts::default());
        let err = check_restorable(&tap, &target(None, None, true)).unwrap_err();
        assert!(
            err.to_string().starts_with(
                "restore needs the guest's network layout: the checkpoint's guest has a NIC on \
                 a TAP device, but task workspace is now gated (a vsock datapath and no NIC)"
            ),
            "{err}"
        );
        let loopback = manifest_for(&state(None, None), MemoryParts::default());
        assert!(check_restorable(&loopback, &target(None, None, true)).is_err());
        assert!(check_restorable(&loopback, &target(None, None, false)).is_ok());
    }

    /// Checkpoints taken before the network field existed read as `tap`
    /// when they recorded a TAP and `none` otherwise.
    #[test]
    fn older_checkpoint_states_read_their_network_from_the_tap() -> R<()> {
        let old: SandboxCheckpointState = serde_json::from_value(json!({
            "vmm": "firecracker", "linkKey": "sbx-k", "volumeCount": 0, "vcpus": 1,
            "memoryMib": 256, "tap": "tap0",
        }))?;
        assert_eq!(old.network(), GuestNetwork::Tap);
        let old: SandboxCheckpointState = serde_json::from_value(json!({
            "vmm": "firecracker", "linkKey": "sbx-k", "volumeCount": 0, "vcpus": 1,
            "memoryMib": 256,
        }))?;
        assert_eq!(old.network(), GuestNetwork::None);
        let gated = SandboxCheckpointState {
            network: Some(GuestNetwork::Gated),
            ..old
        };
        assert_eq!(serde_json::to_value(&gated)?["network"], "gated");
        Ok(())
    }

    fn fake_firecracker_at(version: &'static str) -> impl Fn(&Seen) -> (u16, String) + Send {
        move |seen| match seen.path.as_str() {
            "/version" => (200, format!(r#"{{"firecracker_version":"{version}"}}"#)),
            _ => fake_firecracker(seen),
        }
    }

    /// A gated sandbox launched for checkpoints: [`launched`] with the vsock
    /// device a gated launch adds.
    fn launched_gated(
        root: &Path,
        id: i64,
        backend: &Backend,
    ) -> R<(TaskInstance, PathBuf, VmSpec)> {
        let instance = TaskInstance {
            alloc_dir: Some(root.join("allocs").join(id.to_string())),
            checkpoint_launch: true,
            ..TaskInstance::default()
        };
        let scratch = instance.alloc_dir.clone().unwrap().join(".sandbox");
        std::fs::create_dir_all(&scratch)?;
        std::fs::write(scratch.join("rootfs.img"), b"root disk contents")?;
        let mut spec = VmSpec {
            name: format!("legion-sbx-{id}"),
            kernel: PathBuf::from("/vmlinux"),
            extra_cmdline: None,
            root_disk: scratch.join("rootfs.img"),
            volume_disks: Vec::new(),
            vcpus: 1,
            memory_mib: 256,
            tap: None,
            api_socket: scratch.join("vmm.sock"),
            vsock: Some(microvm::VsockSpec {
                socket: scratch.join("vsock.sock"),
            }),
            identity_disk: None,
            balloon: None,
        };
        prepare_checkpoint_launch(
            backend,
            &alloc(id),
            "box",
            &instance,
            &scratch,
            &mut spec,
            &GuestTokens::default(),
        )?;
        Ok((instance, scratch, spec))
    }

    /// Gated plan §6.2 and §9, the device-link trap: fork one gated
    /// Firecracker checkpoint twice. Each restore moves the guest's vsock
    /// onto its own allocation's socket (where its own datapath listens),
    /// never into the shared `.vmdev/<key>/` directory, so a later fork can
    /// never redirect another guest's connections. Each waits for its
    /// datapath before resuming, and records `gated` for its next capture.
    #[test]
    fn gated_forks_restore_onto_their_own_allocations_sockets() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let fc = Backend::Firecracker {
            binary: "/usr/bin/true".into(),
        };
        let (mut origin, origin_scratch, spec) = launched_gated(tmp.path(), 7, &fc)?;
        assert_eq!(
            spec.vsock.as_ref().unwrap().socket,
            origin_scratch.join("vsock.sock"),
            "the vsock is never behind a device link"
        );
        let vmm = FakeVmm::start_at(
            origin_scratch.join("vmm.sock"),
            false,
            fake_firecracker_at("1.16.1"),
        );
        let out = CheckpointDir::create(tmp.path().join("ckpt"))?;
        let parts = checkpoint_microvm(
            &fc,
            &alloc(7),
            "box",
            &mut origin,
            &origin_scratch,
            &out,
            AfterCheckpoint::Resume,
        )?;
        drop(vmm);
        let state: SandboxCheckpointState = serde_json::from_value(parts.runtime.clone())?;
        assert_eq!(state.network(), GuestNetwork::Gated);
        let manifest = manifest_for(&state, parts);
        let vmdev = tmp.path().join("allocs/.vmdev");

        let mut sockets = std::collections::HashSet::new();
        for (n, id) in [(1, 8), (2, 9)] {
            let scratch = tmp.path().join(format!("allocs/{id}/.sandbox"));
            std::fs::create_dir_all(&scratch)?;
            std::fs::write(scratch.join("vmm.sock"), b"stale")?;
            // A stale base from an earlier boot is cleared for the VMM; the
            // datapath's own listener beside it is left alone.
            std::fs::write(scratch.join("vsock.sock"), b"stale")?;
            std::fs::write(scratch.join("vsock.sock_1080"), b"proxy listener")?;
            let vmm = FakeVmm::start_at(scratch.join("vmm.sock"), true, fake_firecracker);
            let allocation = alloc(id);
            let identity = ResumeIdentity {
                alloc_id: id,
                instance_id: Some(format!("user-42-f{n}")),
                job_id: "job".into(),
                task_name: "box".into(),
                hostname: format!("legion-sbx-{id}"),
                fork: true,
                ..ResumeIdentity::default()
            };
            let target = RestoreTarget {
                backend: &fc,
                allocation: &allocation,
                task_name: "box",
                hostname: format!("legion-sbx-{id}"),
                vcpus: 1,
                memory_mib: 256,
                scratch: scratch.clone(),
                tap: None,
                bridge: None,
                vsock: Some(scratch.join("vsock.sock")),
                datapath_wait: DATAPATH_WAIT,
            };
            let mut restored = TaskInstance {
                alloc_dir: Some(tmp.path().join(format!("allocs/{id}"))),
                ..TaskInstance::default()
            };
            restore_microvm(
                &target,
                &mut restored,
                &manifest,
                &out,
                &identity,
                &FakeTaps::default(),
            )?;
            let calls = vmm.drain();
            let load = calls
                .iter()
                .find(|call| call.path == "/snapshot/load")
                .and_then(|call| call.body.clone())
                .expect("snapshot load");
            let uds_path = PathBuf::from(load["vsock_override"]["uds_path"].as_str().unwrap());
            assert_eq!(uds_path, scratch.join("vsock.sock"));
            assert!(!uds_path.starts_with(&vmdev), "{}", uds_path.display());
            assert!(sockets.insert(uds_path), "fork {n} shares a vsock socket");
            assert!(load.get("network_overrides").is_none(), "no NIC to move");
            assert_eq!(
                calls.last().map(|call| call.body.clone()),
                Some(Some(json!({"state": "Resumed"})))
            );
            assert!(!scratch.join("vsock.sock").exists(), "stale base cleared");
            assert!(scratch.join("vsock.sock_1080").exists(), "listener kept");
            assert_eq!(
                read_boot_record(&scratch).map(|record| record.network()),
                Some(GuestNetwork::Gated),
                "the fork's next checkpoint is gated too"
            );
        }
        Ok(())
    }

    /// Gated plan §6.3, calls entry 30: a restore whose datapath has not
    /// bound the egress proxy socket waits, then resumes the guest anyway
    /// (fail-closed: each dial is reset) instead of failing into a cold boot.
    #[test]
    fn a_gated_restore_resumes_fail_closed_when_the_datapath_is_not_bound() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let ch = Backend::CloudHypervisor {
            binary: "/usr/bin/true".into(),
        };
        let out = CheckpointDir::create(tmp.path().join("ckpt"))?;
        std::fs::write(out.part_path(parts::ROOTFS), b"root")?;
        std::fs::write(
            out.part_path(parts::CH_CONFIG),
            serde_json::to_vec(
                &json!({ "vsock": { "cid": 3, "socket": "/allocs/7/.sandbox/vsock.sock" } }),
            )?,
        )?;
        let state = SandboxCheckpointState {
            vmm: "cloud-hypervisor".into(),
            link_key: "sbx-gated".into(),
            volume_count: 0,
            vcpus: 1,
            memory_mib: 256,
            tap: None,
            network: Some(GuestNetwork::Gated),
            guest_address: None,
            guest_ports: BTreeMap::new(),
        };
        let memory = MemoryParts {
            parts: out.seal(&[("rootfs", parts::ROOTFS), ("vm-config", parts::CH_CONFIG)])?,
            ..MemoryParts::default()
        };
        let manifest = manifest_for(&state, memory);
        let scratch = tmp.path().join("allocs/8/.sandbox");
        std::fs::create_dir_all(&scratch)?;
        std::fs::write(scratch.join("vmm.sock"), b"stale")?;
        let vmm = FakeVmm::start_at(scratch.join("vmm.sock"), true, |_| (204, String::new()));
        let allocation = alloc(8);
        let target = RestoreTarget {
            backend: &ch,
            allocation: &allocation,
            task_name: "box",
            hostname: "legion-sbx-8".into(),
            vcpus: 1,
            memory_mib: 256,
            scratch: scratch.clone(),
            tap: None,
            bridge: None,
            vsock: Some(scratch.join("vsock.sock")),
            datapath_wait: Duration::from_millis(100),
        };
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().join("allocs/8")),
            ..TaskInstance::default()
        };
        let started = std::time::Instant::now();
        restore_microvm(
            &target,
            &mut instance,
            &manifest,
            &out,
            &ResumeIdentity {
                alloc_id: 8,
                ..ResumeIdentity::default()
            },
            &FakeTaps::default(),
        )?;
        assert!(started.elapsed() >= Duration::from_millis(100), "it waited");
        let calls: Vec<String> = vmm.drain().into_iter().map(|call| call.path).collect();
        assert_eq!(calls, vec!["/api/v1/vm.resume"], "and then resumed anyway");
        let config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(out.part_path(parts::CH_CONFIG))?)?;
        assert_eq!(
            config["vsock"]["socket"],
            scratch.join("vsock.sock").display().to_string(),
            "cloud-hypervisor restores onto the new allocation's socket"
        );
        assert!(!wait_for_datapath(
            &scratch.join("vsock.sock"),
            Duration::ZERO
        ));
        std::fs::write(scratch.join("vsock.sock_1080"), b"bound")?;
        assert!(wait_for_datapath(
            &scratch.join("vsock.sock"),
            Duration::ZERO
        ));
        Ok(())
    }

    /// Gated plan §6.2, calls entry 15: a Firecracker too old to move the
    /// vsock refuses the capture before pausing, so the guest never stops
    /// for a checkpoint it could not restore.
    #[test]
    fn an_old_firecracker_refuses_a_gated_capture_before_pausing() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let fc = Backend::Firecracker {
            binary: "/usr/bin/true".into(),
        };
        let (mut origin, scratch, _) = launched_gated(tmp.path(), 7, &fc)?;
        let vmm = FakeVmm::start_at(
            scratch.join("vmm.sock"),
            false,
            fake_firecracker_at("1.15.0"),
        );
        let out = CheckpointDir::create(tmp.path().join("ckpt"))?;
        let err = checkpoint_microvm(
            &fc,
            &alloc(7),
            "box",
            &mut origin,
            &scratch,
            &out,
            AfterCheckpoint::Stop,
        )
        .unwrap_err();
        assert!(
            err.to_string().starts_with(
                "sandbox allocation 7 is gated, and node node-1 runs Firecracker 1.15.0"
            ),
            "{err}"
        );
        let calls: Vec<String> = vmm.drain().into_iter().map(|call| call.path).collect();
        assert_eq!(calls, vec!["/version"], "never paused");
        Ok(())
    }

    #[test]
    fn cloud_hypervisor_live_checkpoint_resumes_the_guest() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let ch = Backend::CloudHypervisor {
            binary: "/usr/bin/true".into(),
        };
        let (mut instance, scratch, _) = launched(tmp.path(), 7, &ch)?;
        let vmm = FakeVmm::start_at(scratch.join("vmm.sock"), false, |seen| {
            match seen.path.as_str() {
                "/api/v1/vmm.ping" => (200, r#"{"version":"v41.0.0"}"#.into()),
                "/api/v1/vm.snapshot" => {
                    let url = seen.body.as_ref().unwrap()["destination_url"]
                        .as_str()
                        .unwrap();
                    let dir = PathBuf::from(url.trim_start_matches("file://"));
                    for file in [parts::CH_MEMORY, parts::CH_STATE, parts::CH_CONFIG] {
                        std::fs::write(dir.join(file), file).unwrap();
                    }
                    (204, String::new())
                }
                _ => (204, String::new()),
            }
        });
        let out = CheckpointDir::create(tmp.path().join("ckpt"))?;
        let parts = checkpoint_microvm(
            &ch,
            &alloc(7),
            "box",
            &mut instance,
            &scratch,
            &out,
            AfterCheckpoint::Resume,
        )?;
        let calls: Vec<String> = vmm.drain().into_iter().map(|c| c.path).collect();
        assert_eq!(
            calls,
            vec![
                "/api/v1/vmm.ping",
                "/api/v1/vm.pause",
                "/api/v1/vm.snapshot",
                "/api/v1/vm.resume"
            ]
        );
        assert_eq!(parts.parts.len(), 4);
        assert_eq!(parts.compat.vmm.unwrap().version, "41.0.0");
        Ok(())
    }

    #[test]
    fn a_failed_capture_resumes_the_guest_and_reports_why() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let fc = Backend::Firecracker {
            binary: "/usr/bin/true".into(),
        };
        let (mut instance, scratch, _) = launched(tmp.path(), 7, &fc)?;
        let vmm = FakeVmm::start_at(scratch.join("vmm.sock"), false, |seen| {
            match seen.path.as_str() {
                "/version" => (200, r#"{"firecracker_version":"1.7.0"}"#.into()),
                "/snapshot/create" => (400, r#"{"fault_message":"disk full"}"#.into()),
                _ => (204, String::new()),
            }
        });
        let out = CheckpointDir::create(tmp.path().join("ckpt"))?;
        let err = checkpoint_microvm(
            &fc,
            &alloc(7),
            "box",
            &mut instance,
            &scratch,
            &out,
            AfterCheckpoint::Stop,
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("disk full"), "{err:#}");
        let last = vmm.drain().pop().unwrap();
        assert_eq!(last.body, Some(json!({"state": "Resumed"})));
        Ok(())
    }

    #[test]
    fn a_sandbox_not_launched_for_checkpoints_says_so() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let fc = Backend::Firecracker {
            binary: "/usr/bin/true".into(),
        };
        let out = CheckpointDir::create(tmp.path().join("ckpt"))?;
        let err = checkpoint_microvm(
            &fc,
            &alloc(7),
            "box",
            &mut TaskInstance::default(),
            tmp.path(),
            &out,
            AfterCheckpoint::Resume,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("not launched for memory checkpoints"),
            "{err}"
        );
        Ok(())
    }

    /// Firecracker's half of the fake: answer `/version`, and write the files
    /// `/snapshot/create` names, like the real VMM.
    fn fake_firecracker(seen: &Seen) -> (u16, String) {
        match seen.path.as_str() {
            "/version" => (200, r#"{"firecracker_version":"1.7.0"}"#.into()),
            "/snapshot/create" => {
                let body = seen.body.as_ref().unwrap();
                for key in ["snapshot_path", "mem_file_path"] {
                    std::fs::write(body[key].as_str().unwrap(), key).unwrap();
                }
                (204, String::new())
            }
            _ => (204, String::new()),
        }
    }

    /// A scripted host: the state each device is in, and what was done.
    #[derive(Default)]
    struct FakeTaps {
        states: std::collections::HashMap<String, TapState>,
        /// Device -> the bridge it is enslaved to.
        masters: std::collections::HashMap<String, String>,
        fail_create: Option<String>,
        log: std::sync::Mutex<Vec<String>>,
    }

    /// An auto TAP at boot: a missing device is created on its bridge and
    /// recorded for destroy; a leftover free one is reused and re-attached;
    /// a busy one is this instance running here already, refused by name.
    #[test]
    fn an_auto_tap_is_created_on_its_bridge_reused_or_refused() -> R<()> {
        let dir = tempfile::tempdir()?;
        let taps = FakeTaps::default();
        ensure_auto_tap(&taps, "lgf0123456789ab", "br0", dir.path())?;
        assert_eq!(
            *taps.log.lock().unwrap(),
            vec![
                "create lgf0123456789ab",
                "master lgf0123456789ab br0",
                "up lgf0123456789ab"
            ]
        );
        release_restore_tap(&taps, dir.path());
        assert_eq!(
            taps.log.lock().unwrap().last().map(String::as_str),
            Some("delete lgf0123456789ab"),
            "destroy removes the TAP Legion made"
        );

        let leftover = FakeTaps {
            states: [("lgf0".to_string(), TapState::Free)].into(),
            masters: [("lgf0".to_string(), "br9".to_string())].into(),
            ..Default::default()
        };
        ensure_auto_tap(&leftover, "lgf0", "br0", dir.path())?;
        assert_eq!(
            *leftover.log.lock().unwrap(),
            vec!["master lgf0 br0", "up lgf0"]
        );

        let busy = FakeTaps {
            states: [("lgf0".to_string(), TapState::Busy)].into(),
            ..Default::default()
        };
        let err = ensure_auto_tap(&busy, "lgf0", "br0", dir.path()).unwrap_err();
        assert!(
            format!("{err:#}").contains("already runs a guest on this node"),
            "{err:#}"
        );
        Ok(())
    }

    /// Restore of an auto-TAP sandbox whose device is gone (the node
    /// rebooted, or the suspended allocation's destroy removed it):
    /// recreated under the recorded name, on the task's bridge. A fork of
    /// one joins the declared bridge even when the origin's TAP is gone.
    #[test]
    fn an_auto_tap_is_recreated_on_its_bridge_on_restore_and_fork() -> R<()> {
        let dir = tempfile::tempdir()?;
        let taps = FakeTaps::default();
        ensure_restore_tap(
            &taps,
            Some("lgfaaaaaaaaaaaa"),
            Some("lgfaaaaaaaaaaaa"),
            Some("br0"),
            dir.path(),
        )?;
        assert_eq!(
            *taps.log.lock().unwrap(),
            vec![
                "create lgfaaaaaaaaaaaa",
                "master lgfaaaaaaaaaaaa br0",
                "up lgfaaaaaaaaaaaa"
            ]
        );

        let fork = FakeTaps::default();
        ensure_fork_tap(
            &fork,
            "lgfaaaaaaaaaaaa",
            Some("lgfbbbbbbbbbbbb"),
            Some("br0"),
            "lgfbbbbbbbbbbbb",
            dir.path(),
        )?;
        assert_eq!(
            *fork.log.lock().unwrap(),
            vec![
                "create lgfbbbbbbbbbbbb",
                "master lgfbbbbbbbbbbbb br0",
                "up lgfbbbbbbbbbbbb"
            ]
        );
        Ok(())
    }

    #[test]
    fn the_attached_tap_is_recorded_beside_the_vmm() -> R<()> {
        let dir = tempfile::tempdir()?;
        assert_eq!(attached_tap(dir.path()), None);
        record_attached_tap(dir.path(), "lgf0123456789ab")?;
        assert_eq!(attached_tap(dir.path()).as_deref(), Some("lgf0123456789ab"));
        Ok(())
    }

    /// A token refresh rewrites the tokens on a running guest's disk and
    /// keeps what makes it the same allocation, so the guest's handler sees
    /// a refresh and not a restore. Unchanged tokens leave the disk alone.
    #[test]
    fn a_token_refresh_keeps_the_allocation_on_the_identity_disk() -> R<()> {
        let dir = tempfile::tempdir()?;
        let path = identity_disk_path(dir.path());
        let boot_tokens = GuestTokens {
            env: [("VAULT_TOKEN".to_string(), "old".to_string())].into(),
            files: vec![crate::structs::checkpoint::GuestTokenFile {
                path: "/legion/identity/aws".into(),
                mode: 0o400,
                token: "old-aws".into(),
            }],
        };
        let mut boot = boot_identity(&alloc(7), "box", "legion-sbx-7", &boot_tokens);
        boot.env.insert("LEGION_ALLOC_ID".into(), "7".into());
        write_identity_disk(&path, &boot)?;

        assert!(
            !refresh_identity_tokens(&path, &boot_tokens)?,
            "nothing new"
        );
        let fresh = GuestTokens {
            env: [("VAULT_TOKEN".to_string(), "new".to_string())].into(),
            files: vec![crate::structs::checkpoint::GuestTokenFile {
                token: "new-aws".into(),
                ..boot_tokens.files[0].clone()
            }],
        };
        assert!(refresh_identity_tokens(&path, &fresh)?);
        let on_disk = crate::task_plugins::checkpoint::read_identity_disk(&path)?;
        assert_eq!(on_disk.alloc_id, 7);
        assert_eq!(on_disk.host_time_unix_ms, boot.host_time_unix_ms);
        assert_eq!(on_disk.env["VAULT_TOKEN"], "new");
        assert_eq!(on_disk.env["LEGION_ALLOC_ID"], "7", "LEGION_ env is kept");
        assert_eq!(on_disk.files[0].token, "new-aws");
        assert!(crate::task_plugins::guest_resume::is_token_refresh(
            &boot, &on_disk
        ));
        Ok(())
    }

    impl TapHost for FakeTaps {
        fn state(&self, device: &str) -> R<TapState> {
            Ok(*self.states.get(device).unwrap_or(&TapState::Missing))
        }
        fn create(&self, device: &str) -> R<()> {
            self.log.lock().unwrap().push(format!("create {device}"));
            match self.fail_create.as_deref() {
                Some(reason) => bail!("{reason}"),
                None => Ok(()),
            }
        }
        fn set_up(&self, device: &str) -> R<()> {
            self.log.lock().unwrap().push(format!("up {device}"));
            Ok(())
        }
        fn delete(&self, device: &str) -> R<()> {
            self.log.lock().unwrap().push(format!("delete {device}"));
            Ok(())
        }
        fn master(&self, device: &str) -> R<Option<String>> {
            Ok(self.masters.get(device).cloned())
        }
        fn set_master(&self, device: &str, master: &str) -> R<()> {
            self.log
                .lock()
                .unwrap()
                .push(format!("master {device} {master}"));
            Ok(())
        }
    }

    #[test]
    fn a_missing_restore_tap_is_created_up_and_removed_on_release() -> R<()> {
        let dir = tempfile::tempdir()?;
        let taps = FakeTaps::default();
        ensure_restore_tap(&taps, Some("tap7"), Some("tap7"), None, dir.path())?;
        assert_eq!(*taps.log.lock().unwrap(), vec!["create tap7", "up tap7"]);
        release_restore_tap(&taps, dir.path());
        assert_eq!(taps.log.lock().unwrap().last().unwrap(), "delete tap7");
        // Released once: a second release is a no-op.
        release_restore_tap(&taps, dir.path());
        assert_eq!(taps.log.lock().unwrap().len(), 3);
        Ok(())
    }

    #[test]
    fn a_free_operator_tap_is_used_as_is_and_never_deleted() -> R<()> {
        let dir = tempfile::tempdir()?;
        let taps = FakeTaps {
            states: [("tap7".to_string(), TapState::Free)].into(),
            ..FakeTaps::default()
        };
        ensure_restore_tap(&taps, Some("tap7"), Some("tap7"), None, dir.path())?;
        release_restore_tap(&taps, dir.path());
        assert!(taps.log.lock().unwrap().is_empty());
        Ok(())
    }

    #[test]
    fn restore_tap_failures_are_named_and_specific() {
        let dir = tempfile::tempdir().unwrap();
        let busy = FakeTaps {
            states: [("tap7".to_string(), TapState::Busy)].into(),
            ..FakeTaps::default()
        };
        let err =
            ensure_restore_tap(&busy, Some("tap7"), Some("tap7"), None, dir.path()).unwrap_err();
        assert!(
            format!("{err:#}").starts_with("restore needs TAP device tap7: it exists and another"),
            "{err:#}"
        );

        let not_tap = FakeTaps {
            states: [("tap7".to_string(), TapState::NotTap)].into(),
            ..FakeTaps::default()
        };
        let err =
            ensure_restore_tap(&not_tap, Some("tap7"), Some("tap7"), None, dir.path()).unwrap_err();
        assert!(format!("{err:#}").contains("not a TAP device"), "{err:#}");

        let broken = FakeTaps {
            fail_create: Some("operation not permitted".into()),
            ..FakeTaps::default()
        };
        let err =
            ensure_restore_tap(&broken, Some("tap7"), Some("tap7"), None, dir.path()).unwrap_err();
        let text = format!("{err:#}");
        assert!(
            text.starts_with("restore needs TAP device tap7: creating it failed"),
            "{text}"
        );
        assert!(text.contains("operation not permitted"), "{text}");
        // A failed create records nothing for destroy to delete.
        release_restore_tap(&broken, dir.path());
        assert_eq!(*broken.log.lock().unwrap(), vec!["create tap7"]);

        let err = ensure_restore_tap(
            &FakeTaps::default(),
            Some("tap7"),
            Some("tap9"),
            None,
            dir.path(),
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("now names TAP tap9"), "{err:#}");
        let err = ensure_restore_tap(&FakeTaps::default(), None, Some("tap9"), None, dir.path())
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("without a TAP device"),
            "{err:#}"
        );
        assert!(ensure_restore_tap(&FakeTaps::default(), None, None, None, dir.path()).is_ok());
    }

    /// The failure reaches the caller before any VMM starts, so a restore
    /// never comes up without the network its guest expects.
    #[test]
    fn a_restore_whose_tap_is_busy_fails_before_spawning_a_vmm() -> R<()> {
        let dir = tempfile::tempdir()?;
        let scratch = dir.path().join("allocs/8/.sandbox");
        let out = CheckpointDir::create(dir.path().join("ckpt"))?;
        std::fs::write(out.part_path(parts::ROOTFS), b"root")?;
        let state = SandboxCheckpointState {
            vmm: "firecracker".into(),
            link_key: "sbx-tap".into(),
            volume_count: 0,
            vcpus: 1,
            memory_mib: 256,
            tap: Some("tap7".into()),
            network: None,
            guest_address: None,
            guest_ports: BTreeMap::new(),
        };
        let memory = MemoryParts {
            parts: out.seal(&[("rootfs", parts::ROOTFS)])?,
            ..MemoryParts::default()
        };
        let manifest = manifest_for(&state, memory);
        let fc = Backend::Firecracker {
            binary: "/nonexistent/firecracker".into(),
        };
        let allocation = alloc(8);
        let target = RestoreTarget {
            backend: &fc,
            allocation: &allocation,
            task_name: "box",
            hostname: "legion-sbx-8".into(),
            vcpus: 1,
            memory_mib: 256,
            scratch: scratch.clone(),
            tap: Some("tap7".into()),
            bridge: None,
            vsock: None,
            datapath_wait: DATAPATH_WAIT,
        };
        let mut instance = TaskInstance {
            alloc_dir: Some(dir.path().join("allocs/8")),
            ..TaskInstance::default()
        };
        let busy = FakeTaps {
            states: [("tap7".to_string(), TapState::Busy)].into(),
            ..FakeTaps::default()
        };
        let err = restore_microvm(
            &target,
            &mut instance,
            &manifest,
            &out,
            &ResumeIdentity::default(),
            &busy,
        )
        .unwrap_err();
        assert!(
            format!("{err:#}").contains("restore needs TAP device tap7"),
            "{err:#}"
        );
        assert!(instance.child.is_none() && instance.pid.is_none());
        Ok(())
    }
}
