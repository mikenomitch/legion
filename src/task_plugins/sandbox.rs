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
use crate::structs::checkpoint::{MemoryParts, ResumeIdentity, SnapshotKind, SnapshotManifest};
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::checkpoint::{
    AfterCheckpoint, CheckpointDir, CheckpointQuery, CheckpointSupport,
};
use crate::task_plugins::microvm::{self, Backend, GuestDiskMount, GuestPayload, TapNet, VmSpec};
use crate::task_plugins::{
    apple_container, configure_task_log_capture, derived_schema, oci_bundle, oci_image,
    reap_task_process, spawn_pipe, spawn_pty, spawn_task_process, validate_schema, ExecOptions,
    ExecSession, ExecSupport, MountedVolume, TaskInstance, TaskPlugin,
};
use crate::task_plugins::{rootfs_cache, sandbox_addr, sandbox_checkpoint};
use crate::volumes::VolumeMode;
use crate::worker::start_timing::stage as start_stage;
use schemars::JsonSchema;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

const DEFAULT_VCPUS: u32 = 1;
const DEFAULT_MEMORY_MIB: u32 = 512;
pub(crate) const DEFAULT_ROOTFS_HEADROOM_MIB: u32 = 256;
/// Guest RAM below this cannot boot a Linux kernel with any userspace.
const MIN_MEMORY_MIB: u32 = 64;

/// The worker's `sandbox.vmmOverheadMiB`, exported by the daemon, falling
/// back to the default when unset or unparseable.
fn vmm_overhead_mib() -> u64 {
    std::env::var(crate::constants::ENV_SANDBOX_VMM_OVERHEAD_MIB)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|mib| *mib > 0)
        .unwrap_or(crate::constants::DEF_SANDBOX_VMM_OVERHEAD_MIB)
}

/// `memory.max` for a microVM's cgroup: guest RAM plus VMM overhead.
fn vmm_cgroup_memory_bytes(guest_mib: u32, overhead_mib: u64) -> u64 {
    (u64::from(guest_mib) + overhead_mib) * 1024 * 1024
}

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

    /// Guest network interface on a host bridge: Legion creates a TAP
    /// device per instance, attaches it to `bridge`, and leases the guest an
    /// address from the bridge's pool in the worker config.
    #[serde(default)]
    network: Option<SandboxNetworkConfig>,

    /// How a memory checkpoint's guest RAM comes back on a restore (a
    /// resume, a snapshot restore, a fork, a create from a snapshot):
    /// `lazy` (default) resumes the guest before its memory is read and
    /// loads each page on first touch, `eager` reads all of it first.
    /// cloud-hypervisor needs v52 or later and a userfaultfd for `lazy`, and
    /// restores eagerly without them.
    #[serde(default)]
    memory_restore: Option<String>,

    /// Additional plugin-specific fields reserved for forward compatibility.
    #[serde(default, flatten)]
    #[schemars(skip)]
    _extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SandboxNetworkConfig {
    /// `none` (default) or `tap` to give the guest a NIC on a host bridge.
    #[serde(default)]
    mode: Option<String>,

    /// The host bridge the guest's NIC joins (required with `mode: tap`).
    /// Legion creates a TAP device per instance (`lgf` plus a hash of the
    /// instance id), attaches it here, recreates it under the same name on
    /// restore, and deletes it when the allocation is destroyed. The guest's
    /// address comes from this bridge's pool in the worker config
    /// (`sandbox.bridges.<bridge>`), and the task only places on nodes that
    /// declare one.
    #[serde(default)]
    bridge: Option<String>,

    /// Removed: Legion creates the TAP device. Kept only to refuse it with
    /// the replacement.
    #[serde(default)]
    #[schemars(skip)]
    tap: Option<Value>,

    /// Removed: Legion derives the guest MAC from the allocation id. Kept
    /// only to refuse it with the replacement.
    #[serde(default)]
    #[schemars(skip)]
    mac: Option<Value>,
}

/// Where a sandbox guest reads its workload-identity token files
/// (`identities[].file.path` resolves under it).
pub const GUEST_IDENTITY_ROOT: &str = crate::task_plugins::guest_resume::GUEST_TOKEN_DIR;

/// The host side of [`GUEST_IDENTITY_ROOT`]: file delivery writes tokens to
/// `<task dir>/identity/`, and the runtime projects them into the guest
/// (through the identity disk on a microVM, a read-only bind mount in dev
/// mode).
const HOST_IDENTITY_SUBDIR: &str = "identity";

fn has_file_identities(task: &TaskConfig) -> bool {
    task.identities
        .iter()
        .any(crate::structs::workload_identity::WorkloadIdentity::delivers_file)
}

/// The tokens a microVM guest keeps on its identity disk: each env-delivered
/// identity's current value (from `env`), and each file-delivered one read
/// from the host file the worker keeps fresh under `task_dir`. A token the
/// worker has not delivered (identities disabled on the manager) is left
/// out, exactly as the task would start without it.
fn guest_tokens(
    task: &TaskConfig,
    env: &dyn Fn(&str) -> Option<String>,
    task_dir: Option<&Path>,
) -> sandbox_checkpoint::GuestTokens {
    let mut tokens = sandbox_checkpoint::GuestTokens::default();
    for identity in &task.identities {
        if let Some(key) = identity.env.as_deref().filter(|key| !key.trim().is_empty()) {
            if let Some(value) = env(key) {
                tokens.env.insert(key.to_string(), value);
            }
        }
        let (Some(file), Some(task_dir)) = (
            identity
                .file
                .as_ref()
                .filter(|file| !file.path.trim().is_empty()),
            task_dir,
        ) else {
            continue;
        };
        // Re-resolved here, never trusted from admission alone, so no path
        // can name anything outside the identity directory on either side.
        let relative = match crate::task_plugins::resolve_task_dir_destination(
            GUEST_IDENTITY_ROOT,
            &file.path,
        ) {
            Ok(relative) => relative,
            Err(err) => {
                warn!(identity = %identity.name, error = %err, "Skipping a sandbox identity file outside /legion/identity");
                continue;
            }
        };
        let Ok(token) =
            std::fs::read_to_string(task_dir.join(HOST_IDENTITY_SUBDIR).join(&relative))
        else {
            continue;
        };
        let mode = file
            .mode
            .as_deref()
            .and_then(|mode| crate::structs::workload_identity::parse_file_mode(mode).ok())
            .unwrap_or(crate::worker::identity::DEFAULT_TOKEN_FILE_MODE);
        tokens
            .files
            .push(crate::structs::checkpoint::GuestTokenFile {
                path: Path::new(GUEST_IDENTITY_ROOT)
                    .join(&relative)
                    .to_string_lossy()
                    .into_owned(),
                mode,
                token,
            });
    }
    tokens
}

/// Whether `name` can be a Linux network interface name: 1 to 15 bytes, no
/// `/`, whitespace, or `:`, and not `.` or `..`.
fn valid_interface_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() < 16
        && name != "."
        && name != ".."
        && !name
            .chars()
            .any(|c| c == '/' || c == ':' || c.is_whitespace() || c.is_control())
}

/// Beside the VMM socket: the balloon's bounds, written at boot for a
/// ballooned guest.
const BALLOON_RECORD: &str = "balloon.json";

/// What `reclaim` needs to drive a guest's balloon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct BalloonRecord {
    guest_mib: u64,
    reserved_mib: u64,
}

fn write_balloon_record(scratch: &Path, record: &BalloonRecord) -> R<()> {
    let path = scratch.join(BALLOON_RECORD);
    std::fs::write(&path, serde_json::to_vec(record)?)
        .with_context(|| format!("write {}", path.display()))
}

fn read_balloon_record(scratch: &Path) -> Option<BalloonRecord> {
    serde_json::from_slice(&std::fs::read(scratch.join(BALLOON_RECORD)).ok()?).ok()
}

/// The task's `resources.memory` reservation in MiB, when declared.
fn memory_reservation_mib(resources: &Value) -> Option<u32> {
    resources
        .get("memory")
        .and_then(Value::as_f64)
        .filter(|mib| mib.is_finite() && *mib > 0.0)
        .map(|mib| mib.ceil().min(f64::from(u32::MAX)) as u32)
}

/// Inflate a ballooned guest by up to `bytes`, never below its reservation.
/// Returns the bytes the balloon grew by.
fn inflate_balloon(backend: &Backend, scratch: &Path, bytes: u64) -> R<Option<u64>> {
    let Some(record) = read_balloon_record(scratch) else {
        return Ok(None);
    };
    const MIB: u64 = 1024 * 1024;
    let socket = scratch.join("vmm.sock");
    let (current, set): (u64, Box<dyn Fn(u64) -> R<()>>) = match backend {
        Backend::Firecracker { .. } => {
            let api = crate::task_plugins::vmm_api::FirecrackerApi::new(&socket);
            (
                api.balloon_mib()?,
                Box::new(move |mib| api.set_balloon_mib(mib)),
            )
        }
        Backend::CloudHypervisor { .. } => {
            let api = crate::task_plugins::vmm_api::CloudHypervisorApi::new(&socket);
            (
                api.balloon_bytes()? / MIB,
                Box::new(move |mib| api.set_balloon_bytes(mib * MIB)),
            )
        }
        _ => return Ok(None),
    };
    let target = microvm::balloon_target_mib(current, bytes, record.guest_mib, record.reserved_mib);
    if target > current {
        set(target)?;
    }
    Ok(Some((target - current) * MIB))
}

/// Deflate a ballooned guest's balloon.
fn deflate_balloon(backend: &Backend, scratch: &Path) -> R<()> {
    if read_balloon_record(scratch).is_none() {
        return Ok(());
    }
    let socket = scratch.join("vmm.sock");
    match backend {
        Backend::Firecracker { .. } => {
            crate::task_plugins::vmm_api::FirecrackerApi::new(&socket).set_balloon_mib(0)
        }
        Backend::CloudHypervisor { .. } => {
            crate::task_plugins::vmm_api::CloudHypervisorApi::new(&socket).set_balloon_bytes(0)
        }
        _ => Ok(()),
    }
}

/// The guest size, in MiB, a task's `resources` imply: `memoryMax` when
/// set, else `memory` (docs/task-runtime-sandbox.md, "Guest size"). `None`
/// when neither is declared. Applied once, at submit, by
/// [`SandboxTaskPlugin::normalize_task`].
fn resources_guest_mib(resources: &Value) -> Option<u32> {
    let declared = |key: &str| {
        resources
            .get(key)
            .and_then(Value::as_f64)
            .filter(|mib| mib.is_finite() && *mib > 0.0)
    };
    let mib = declared(crate::resources::MEMORY_MAX_KEY).or_else(|| declared("memory"))?;
    Some(mib.ceil().min(f64::from(u32::MAX)) as u32)
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
        if let Some(raw) = self.memory_restore.as_deref() {
            if sandbox_checkpoint::MemoryRestore::parse(raw).is_none() {
                bail!(
                    "sandbox task memory_restore must be `lazy` or `eager`, got `{raw}`\nHint: \
                     `lazy` (the default) resumes a restored guest before its memory is read, and \
                     `eager` reads all of it first (docs/task-runtime-sandbox.md, \"How memory \
                     comes back\")"
                );
            }
        }
        if let Some(network) = self.network.as_ref() {
            if let Some(tap) = network.tap.as_ref() {
                let was = tap.as_str().unwrap_or_default();
                let hint = if was == "auto" || was.is_empty() {
                    "Drop tap: Legion always creates a TAP per instance on bridge".to_string()
                } else {
                    format!("Set bridge = \"<the bridge {was} was on>\"")
                };
                bail!(
                    "config.network.tap is gone: Legion creates a TAP per instance on \
                     network.bridge and leases the guest an address from the bridge's pool\nHint: \
                     {hint}, and declare sandbox.bridges.<bridge> in the worker config \
                     (docs/task-runtime-sandbox.md, \"Addressing\")"
                );
            }
            if network.mac.is_some() {
                bail!(
                    "config.network.mac is gone: Legion derives each guest's MAC from its \
                     allocation id, so instances and forks on one bridge never collide\nHint: \
                     drop mac"
                );
            }
            match network.mode.as_deref().unwrap_or("none") {
                "none" => {
                    if network.bridge.is_some() {
                        bail!(
                            "sandbox task network.bridge applies only to mode = \"tap\"\nHint: \
                             set mode = \"tap\", or drop bridge"
                        );
                    }
                }
                "tap" => match network.bridge.as_deref().map(str::trim) {
                    None | Some("") => bail!(
                        "sandbox task network.mode `tap` requires network.bridge: the host \
                         bridge each instance's TAP joins\nHint: set bridge = \"br0\" (or \
                         whichever bridge carries your sandbox network)"
                    ),
                    Some(bridge) if !valid_interface_name(bridge) => bail!(
                        "sandbox task network.bridge {bridge:?} is not a valid interface name (1 \
                         to 15 characters, no `/`, `:`, or whitespace)"
                    ),
                    Some(_) => {}
                },
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

    /// `memory_restore`, already checked by [`Self::validate`].
    fn memory_restore(&self) -> sandbox_checkpoint::MemoryRestore {
        self.memory_restore
            .as_deref()
            .and_then(sandbox_checkpoint::MemoryRestore::parse)
            .unwrap_or_default()
    }

    /// The bridge the guest's NIC joins, or `None` for a guest with no NIC.
    fn tap_bridge(&self) -> Option<String> {
        let network = self.network.as_ref()?;
        if network.mode.as_deref().unwrap_or("none") != "tap" {
            return None;
        }
        Some(network.bridge.as_deref()?.trim().to_string()).filter(|bridge| !bridge.is_empty())
    }

    /// The TAP device (and MAC) this allocation's guest attaches: the
    /// instance's own device ([`crate::structs::fork::own_tap_name`], the
    /// name a fork of it would get too), or the allocation's when it serves
    /// no instance, with a MAC derived from the allocation id.
    fn tap_for(&self, instance_id: Option<&str>, allocation_id: i64) -> Option<TapNet> {
        self.tap_bridge()?;
        let owner = instance_id
            .map(str::to_string)
            .unwrap_or_else(|| crate::structs::fork::allocation_tap_owner(allocation_id));
        Some(TapNet {
            device: crate::structs::fork::own_tap_name(&owner),
            mac: Some(crate::structs::fork::fork_mac(allocation_id)),
        })
    }
}

/// Give a TAP sandbox the implicit constraint on its bridge: the node
/// attribute [`sandbox_addr::BRIDGES_ATTR`] must contain it. Without it the
/// task could place on a node with no such bridge (or no address pool for
/// it) and fail at start. Constraints in the object form become the
/// equivalent list. Idempotent, so a resubmit does not stack copies.
fn constrain_to_bridge(task: &mut TaskConfig) {
    let Ok(config) = SandboxTaskConfig::from_value(&task.config) else {
        // Validation reports the parse error.
        return;
    };
    let Some(bridge) = config.tap_bridge() else {
        return;
    };
    let rule = serde_json::json!({
        "key": sandbox_addr::BRIDGES_ATTR,
        "operator": "contains",
        "value": bridge,
    });
    let mut rules = match std::mem::take(&mut task.constraints) {
        Value::Array(rules) => rules,
        Value::Object(map) => map
            .into_iter()
            .map(|(key, value)| serde_json::json!({ "key": key, "value": value }))
            .collect(),
        Value::Null => Vec::new(),
        other => {
            // Not a constraint shape: leave it for validation to report.
            task.constraints = other;
            return;
        }
    };
    if !rules.contains(&rule) {
        rules.push(rule);
    }
    task.constraints = Value::Array(rules);
}

/// The instance an allocation serves, from the `LEGION_INSTANCE_ID` the
/// manager put in its env (the worker's allocation carries no instance
/// binding of its own).
fn instance_id_from_env(env: &HashMap<String, String>) -> Option<&str> {
    env.get("LEGION_INSTANCE_ID")
        .map(String::as_str)
        .filter(|id| !id.is_empty())
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

/// The Apple `container` name of a macOS dev-mode sandbox. Container names
/// are global to the Mac, and two local clusters both number allocations
/// from 1, so the name carries the node too: otherwise one cluster's
/// stale-container cleanup (`container rm --force`) would remove the other's
/// running sandbox. The `container` runtime names its containers the same
/// way ([`apple_container::node_scoped_name`]).
fn apple_container_name(allocation: &Allocation) -> String {
    apple_container::node_scoped_name("legion-sbx", allocation)
}

/// The host-side vsock socket base of a gated sandbox: the VMM multiplexes
/// guest ports onto `<base>_<port>`, which is where the node datapath
/// listens. Derived from the allocation directory alone so the worker can
/// bind the listeners before the guest boots.
pub fn vsock_socket_base(scratch: &Path) -> PathBuf {
    scratch.join("vsock.sock")
}

/// [`vsock_socket_base`] from the allocation directory the worker owns.
pub fn vsock_socket_base_for(alloc_dir: &Path) -> PathBuf {
    vsock_socket_base(&sandbox_dir_for(alloc_dir))
}

/// A sandbox's scratch dir (`<alloc>/.sandbox`): its VMM sockets, root disk,
/// boot record, and address lease.
pub fn sandbox_dir_for(alloc_dir: &Path) -> PathBuf {
    alloc_dir.join(".sandbox")
}

/// How long the stop request may take to reach the guest's init. Short,
/// because a guest that cannot take a one-line exec in this long will not
/// shut itself down cleanly either.
const GUEST_STOP_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// How long a hard stop (and a memory capture) waits for the guest to flush
/// its page cache before the VMM is killed or paused. Bounded for the same
/// lock, and generous enough for a guest with a lot of dirty data.
pub(crate) const GUEST_FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The argv the guest agent runs to deliver a stop request: `signal` (a
/// `stop.signal` name, `SIGTERM`) to PID 1, whose trap flushes the disks and
/// passes it to the command (`microvm::render_command_supervision`). The
/// name travels as an argument, never inside the script.
fn guest_stop_command(signal: &str) -> R<Vec<String>> {
    if crate::task_plugins::stop_signal_number(signal).is_none() {
        bail!("{signal} is not a stop signal Legion delivers");
    }
    Ok(vec![
        "/bin/sh".into(),
        "-c".into(),
        "kill -s \"$1\" 1".into(),
        "legion-stop".into(),
        signal.trim_start_matches("SIG").to_string(),
    ])
}

/// Asks the guest behind `vsock` to stop, through its agent (see
/// [`guest_stop_command`]). `Ok` means init has the signal; the VMM exits once
/// the command has and init has unmounted the volumes and powered off.
fn request_guest_stop(vsock: &Path, signal: &str, timeout: std::time::Duration) -> R<()> {
    let command = guest_stop_command(signal)?;
    let code = crate::task_plugins::vsock_exec::run(vsock, command, timeout)?;
    anyhow::ensure!(
        code == 0,
        "the guest's stop request exited with status {code}"
    );
    Ok(())
}

/// Flushes the page cache of the guest behind `vsock` to its disks (`sync`
/// in the guest), so a VMM killed or captured next loses nothing already
/// written. A write is only in the guest's memory until the guest kernel
/// writes it back (up to about 30s later), and killing the VMM drops it.
pub(crate) fn flush_guest(vsock: &Path, timeout: std::time::Duration) -> R<()> {
    let code = crate::task_plugins::vsock_exec::run(vsock, vec!["sync".into()], timeout)?;
    anyhow::ensure!(code == 0, "`sync` in the guest exited with status {code}");
    Ok(())
}

/// Whether the VMM may still be running. A child handle answers exactly; a
/// bare pid (a worker restart restores no `Child`) is assumed alive, and a
/// dead one only costs a refused dial.
fn vmm_may_be_running(instance: &mut TaskInstance) -> bool {
    match instance.child.as_mut() {
        Some(child) => matches!(child.try_wait(), Ok(None)),
        None => instance.pid.is_some(),
    }
}

/// The port the guest in `scratch` listens on for the advertised port
/// `label`, when its boot record says: a guest restored from a memory
/// checkpoint keeps the ports it was captured with, even when the restoring
/// allocation was assigned others. `None` means the allocation's own bind
/// port. The node datapath reads this per ingress connection, since the
/// record is written as the guest boots or resumes, after the datapath
/// already listens (docs/network-handlers.md, "Ingress into a sandbox").
pub fn sandbox_guest_port(scratch: &Path, label: &str) -> Option<u16> {
    sandbox_checkpoint::recorded_guest_port(scratch, label)
}

/// The address a TAP-wired guest in `scratch` holds on its bridge, from
/// its lease. `None` before the worker leased one (the guest has not
/// started) and for gated and loopback-only guests.
pub fn sandbox_guest_address(scratch: &Path) -> Option<std::net::Ipv4Addr> {
    sandbox_addr::read_lease(scratch).map(|lease| lease.address)
}

/// The upstreams a gated guest's agent relays: each declared upstream's
/// loopback port (`ports`, by key, else its local port), the host vsock
/// port behind it (always the allocation's local port, where the node
/// datapath listens), and its VIP, so `<key>.svc.legion` works in the
/// guest. Sorted, so the init script and the disk fingerprint are stable.
fn gated_guest_upstreams(
    network: Option<&crate::structs::AllocNetwork>,
    ports: &BTreeMap<String, u16>,
) -> Vec<legion_guest::identity::GuestUpstream> {
    let Some(network) = network.filter(|network| network.is_gated()) else {
        return Vec::new();
    };
    let mut upstreams: Vec<legion_guest::identity::GuestUpstream> = network
        .upstreams
        .iter()
        // A sandbox's upstreams always carry a local port (placement gives
        // each one): it names the vsock port the relay runs on.
        .filter_map(|upstream| {
            let local = upstream.local_port?;
            let port = ports.get(&upstream.key).copied().unwrap_or(local);
            Some(legion_guest::identity::GuestUpstream {
                port,
                vsock_port: (port != local).then_some(local),
                vip: upstream
                    .vip_address()
                    .and_then(|address| address.parse().ok()),
            })
        })
        .collect();
    upstreams.sort();
    upstreams.dedup();
    upstreams
}

/// The ports a gated guest's agent serves ingress on at boot: every
/// advertised port's bind port, sorted and unique.
fn gated_ingress_ports(allocation: &Allocation) -> Vec<u16> {
    let mut ports: Vec<u16> = sandbox_checkpoint::boot_guest_ports(allocation.network.as_ref())
        .into_values()
        .collect();
    ports.sort_unstable();
    ports.dedup();
    ports
}

/// The port env a restored guest's `/legion/resume.env` carries:
/// `LEGION_PORT_<label>` is the port the guest listens on (unchanged by the
/// restore for a port it was captured with), and `LEGION_IP_<label>` and
/// `LEGION_ADDR_<label>` are where the restoring allocation advertises it.
/// For a gated guest's upstreams, `LEGION_UPSTREAM_<KEY>` (with its
/// `_HOST_` and `_PORT_` parts) names the one loopback address the guest
/// dials, likewise unchanged. The agent also listens on the restoring
/// allocation's VIPs, which `<key>.svc.legion` resolves to.
fn restored_port_env(
    allocation: &Allocation,
    guest_ports: &BTreeMap<String, u16>,
    guest_upstreams: &BTreeMap<String, u16>,
) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    let Some(network) = allocation.network.as_ref() else {
        return env;
    };
    for port in &network.ports {
        let label = crate::structs::sanitize_env_label(&port.name);
        let guest = guest_ports
            .get(&port.name)
            .copied()
            .unwrap_or(port.bind_port());
        env.insert(format!("LEGION_PORT_{label}"), guest.to_string());
        if let Some(address) = network.address.as_deref() {
            env.insert(format!("LEGION_IP_{label}"), address.to_string());
            env.insert(
                format!("LEGION_ADDR_{label}"),
                format!("{address}:{}", port.host_port),
            );
        }
    }
    for upstream in &network.upstreams {
        let Some(port) = guest_upstreams.get(&upstream.key).copied() else {
            continue;
        };
        let label = crate::structs::sanitize_env_label(&upstream.key);
        env.insert(
            format!("LEGION_UPSTREAM_{label}"),
            format!("{}:{port}", crate::structs::UPSTREAM_LOOPBACK_ADDR),
        );
        env.insert(
            format!("LEGION_UPSTREAM_HOST_{label}"),
            crate::structs::UPSTREAM_LOOPBACK_ADDR.to_string(),
        );
        env.insert(format!("LEGION_UPSTREAM_PORT_{label}"), port.to_string());
    }
    env
}

/// Whether a sandbox's runtime config wires its guest to a bridge
/// (`config.network = { mode = "tap", ... }`), the one sandbox shape whose
/// guest has an address the node can dial.
pub fn sandbox_config_is_tap(config: &Value) -> bool {
    config
        .get("network")
        .and_then(|network| network.get("mode"))
        .and_then(Value::as_str)
        == Some("tap")
}

/// The env var a TAP guest reads its leased address from (and its
/// `/legion/resume.env` after a restore).
pub const GUEST_ADDR_ENV: &str = "LEGION_GUEST_ADDR";

/// The worker's allocation root (`<allocs>/`), which the address leases of
/// every sandbox on the node live under.
fn alloc_root(instance: &TaskInstance) -> Option<PathBuf> {
    instance
        .alloc_dir
        .as_deref()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
}

/// The extra kernel command line: a TAP guest's `ip=` autoconfiguration
/// first, then the task's own `kernel_args`, which may override it.
fn guest_cmdline(ip: Option<String>, kernel_args: Option<&str>) -> Option<String> {
    let parts: Vec<&str> = ip
        .as_deref()
        .into_iter()
        .chain(kernel_args.map(str::trim).filter(|args| !args.is_empty()))
        .collect();
    (!parts.is_empty()).then(|| parts.join(" "))
}

/// Per-allocation scratch: image layout, extracted rootfs, root disk, and the
/// VMM control socket. Removed on destroy.
fn work_root(allocation: &Allocation, instance: &TaskInstance) -> PathBuf {
    work_root_for(allocation.id, instance)
}

fn work_root_for(allocation_id: i64, instance: &TaskInstance) -> PathBuf {
    instance
        .alloc_dir
        .clone()
        .or_else(|| instance.task_dir.clone())
        .unwrap_or_else(|| std::env::temp_dir().join(format!("legion-sandbox-{allocation_id}")))
        .join(".sandbox")
}

/// Written into the scratch dir by a dev-mode start on Apple `container`,
/// naming the container, so the usage sampler knows to read it.
const APPLE_CONTAINER_RECORD: &str = "apple-container";

/// Where the usage sampler reads a running sandbox's activity besides its
/// cgroup (`src/worker/usage.rs`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SandboxUsageSource {
    /// The host TAP device the guest's NIC is wired to.
    pub tap_device: Option<String>,
    /// The Apple container a macOS dev-mode sandbox runs as.
    pub apple_container: Option<String>,
}

/// What [`SandboxUsageSource`] the sandbox in `instance` recorded when it
/// started or was restored.
pub fn sandbox_usage_source(allocation_id: i64, instance: &TaskInstance) -> SandboxUsageSource {
    let scratch = work_root_for(allocation_id, instance);
    SandboxUsageSource {
        tap_device: sandbox_checkpoint::attached_tap(&scratch),
        apple_container: std::fs::read_to_string(scratch.join(APPLE_CONTAINER_RECORD))
            .ok()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty()),
    }
}

/// Bring a host TAP device administratively up or down.
///
/// This is the mechanism behind the `boot` gate (docs/edge-boot.md): a guest
/// that has booted but is not yet ratified sits behind a shut link, and
/// ratification is the only thing that raises it. The device exists by now:
/// an operator created a fixed one, and an auto TAP was created at boot. So a
/// failure here is a capability problem (`CAP_NET_ADMIN`) rather than a
/// missing device.
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

/// Removal of a dev-mode container left behind under this allocation's name
/// — `run --name` fails outright when the name is taken, which happens
/// whenever a previous start was not torn down cleanly (worker crash, or a
/// `--rm` run killed before the runtime could reap it). Mirrors the container
/// runtime's `remove_stale_container`; the microVM backends need no
/// equivalent, since their VM dies with the VMM child process.
///
/// On Apple `container` only a container labeled as this node's is removed
/// ([`apple_container::remove_stale`]); an OCI runtime deletes by name,
/// ignoring "no such container". A hung CLI fails the start, naming the
/// command, instead of blocking it.
fn remove_stale_dev_sandbox(backend: &Backend, name: &str, allocation: &Allocation) -> R<()> {
    match backend {
        Backend::AppleContainer => apple_container::remove_stale(name, allocation),
        Backend::OciRuntime { binary } => {
            crate::util::output_with_timeout(
                Command::new(binary).args(["delete", "--force", name]),
                crate::util::RUNTIME_CLI_TIMEOUT,
            )?;
            Ok(())
        }
        Backend::CloudHypervisor { .. } | Backend::Firecracker { .. } => Ok(()),
    }
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
    if payload.resume_watch {
        hasher.update(b"\0resume-watch\0");
    }
    for port in &payload.ingress_ports {
        // The init script hands these to the agent.
        hasher.update(b"\0ingress-port\0");
        hasher.update(port.to_string().as_bytes());
    }
    for upstream in &payload.upstreams {
        hasher.update(b"\0upstream\0");
        hasher.update(upstream.to_arg().as_bytes());
    }
    if let Some(binary) = payload.guest_binary.as_ref() {
        // A new guest binary (a worker upgrade, the static build installed)
        // must reach the guest, so it rebuilds the disk.
        hasher.update(b"\0guest-binary\0");
        hasher.update(binary.fingerprint.as_bytes());
    }
    for resolver in &payload.resolvers {
        hasher.update(b"\0resolver\0");
        hasher.update(resolver.to_string().as_bytes());
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

/// Fails a macOS dev-mode start whose VM would be under Apple `container`'s
/// floor, naming the floor and the field to raise, instead of letting
/// `container run` fail with the reason only in the task's stderr. Only an
/// explicit size is checked: an unsized guest gets the runtime's default.
fn check_apple_memory_floor(task: &str, config: &SandboxTaskConfig) -> R<()> {
    match config.memory_mib {
        Some(mib)
            if u64::from(mib) < crate::task_plugins::container::APPLE_CONTAINER_MIN_MEMORY_MIB =>
        {
            Err(crate::task_plugins::container::apple_memory_floor_error(
                task,
                "config.memory_mib, else resources.memoryMax or resources.memory",
                f64::from(mib),
            ))
        }
        _ => Ok(()),
    }
}

/// Builds the dev-mode Apple `container` launch (macOS). Same shape as the
/// container runtime's, plus the sandbox's guest sizing, with filesystem volumes
/// bind-mounted where the microVM path would attach block devices.
fn build_apple_launch(
    name: &str,
    labels: &[String],
    config: &SandboxTaskConfig,
    env: &BTreeMap<String, String>,
    volumes: &[MountedVolume],
    identity_dir: Option<&Path>,
) -> Vec<String> {
    let mut args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--name".to_string(),
        name.to_string(),
    ];
    args.extend_from_slice(labels);
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
    // File-delivered identity tokens: the host directory the worker keeps
    // fresh, read-only at the path a microVM guest would read them.
    if let Some(dir) = identity_dir {
        args.push("--volume".to_string());
        args.push(format!("{}:{GUEST_IDENTITY_ROOT}:ro", dir.display()));
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
    // No `--` before the command: Apple `container` 1.1 passes everything
    // after the image through verbatim, and takes a `--` there as the
    // executable to run ("failed to find target executable --"). The
    // container runtime's Apple launch does the same.
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
        let tap = config.tap_for(instance_id_from_env(env), allocation.id);
        // A gated sandbox has no NIC: its vsock device is the only way out,
        // and the guest agent makes the host datapath reachable on the
        // guest's loopback. Checked before anything is created. Submit
        // refuses the combination, so only a job stored before that rule (or
        // a `require` policy imposing `gated` on a TAP task) reaches it.
        let gated = crate::task_plugins::allocation_is_gated(allocation);
        if gated && tap.is_some() {
            bail!(
                "sandbox task {} is network.mode = \"gated\" but its config declares a TAP \
                 device; a gated guest has no NIC (drop config.network, or set mode = \"host\")",
                task.name
            );
        }
        std::fs::create_dir_all(&scratch)?;
        let network_started = std::time::Instant::now();
        // The guest's NIC: this instance's own TAP on its bridge (created,
        // and recorded so destroy deletes it), and an address leased from
        // the bridge's pool (docs/task-runtime-sandbox.md, "Addressing").
        // The pool is checked first, so a node that cannot address the
        // guest fails the start before it creates anything.
        let lease = match (config.tap_bridge(), tap.as_ref()) {
            (Some(bridge), Some(tap)) => {
                let pool = sandbox_addr::pool_for(&bridge)?;
                sandbox_checkpoint::ensure_auto_tap(
                    &sandbox_checkpoint::HostTaps,
                    &tap.device,
                    &bridge,
                    &scratch,
                )?;
                Some(sandbox_addr::lease(
                    &pool,
                    &scratch,
                    alloc_root(instance).as_deref(),
                    None,
                    &allocation.node_id,
                )?)
            }
            _ => None,
        };
        if let Some(tap) = tap.as_ref() {
            sandbox_checkpoint::record_attached_tap(&scratch, &tap.device)?;
        }
        if lease.is_some() {
            instance.start_timings.record(
                &task.runtime,
                start_stage::NETWORK,
                network_started.elapsed(),
            );
        }
        let image_started = std::time::Instant::now();
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
        instance
            .start_timings
            .record(&task.runtime, start_stage::IMAGE, image_started.elapsed());
        let root_disk_started = std::time::Instant::now();
        let mut guest_env = oci_bundle::merge_env_map(&image_config.env, env);
        guest_env.extend(instance.volume_env());
        if let Some(lease) = lease.as_ref() {
            guest_env.insert(GUEST_ADDR_ENV.to_string(), lease.address.to_string());
        }
        let payload_cwd = config
            .cwd
            .clone()
            .or_else(|| image_config.working_dir.clone());
        // Workload-identity tokens the guest keeps fresh through its
        // identity disk: file-delivered ones always, env-delivered ones
        // whenever the guest has the disk anyway (a checkpoint launch).
        let tokens = guest_tokens(
            task,
            &|key| env.get(key).cloned(),
            instance.task_dir.as_deref(),
        );
        let file_tokens = has_file_identities(task);
        // Every microVM has a vsock device and the agent, for exec
        // (sandbox-exec plan §6.1). Only a gated one gets the relays.
        microvm::check_vsock_socket_path(&vsock_socket_base(&scratch))?;
        // A guest launched for checkpoints, or with token files, watches its
        // identity disk. The agent runs the watch.
        let resume_watch = instance.checkpoint_launch || file_tokens;
        let mut payload = GuestPayload {
            command: config.command.clone(),
            env: guest_env,
            cwd: payload_cwd,
            disk_mounts: volumes.mounts,
            guest_agent: true,
            relay: gated,
            // A gated guest's advertised ports: the agent listens on each
            // as a vsock port and relays to the workload, which binds the
            // same number (`LEGION_PORT_<label>`).
            ingress_ports: if gated {
                gated_ingress_ports(allocation)
            } else {
                Vec::new()
            },
            // Its upstreams: the agent listens on each one's loopback port
            // and VIP and relays to the datapath's vsock socket for it.
            upstreams: gated_guest_upstreams(allocation.network.as_ref(), &BTreeMap::new()),
            resume_watch,
            guest_binary: Some(microvm::guest_binary()?),
            resolvers: lease
                .as_ref()
                .map(|lease| lease.dns.clone())
                .unwrap_or_default(),
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
            let started = std::time::Instant::now();
            if let Some(user) = image_config.user.as_deref() {
                // In a microVM the isolation boundary is the VM, so the payload
                // runs as root in the guest; say so rather than silently
                // ignoring the image's declared user.
                debug!(
                    allocation_id = allocation.id,
                    image_user = user,
                    "sandbox runs the guest payload as root; image user is not applied"
                );
            }
            // A stale stamp must not vouch for a half-built disk.
            let _ = std::fs::remove_file(&stamp_path);
            let base = rootfs_cache::ImageBase {
                layout: layout.clone(),
                guest_binary: payload
                    .guest_binary
                    .clone()
                    .context("the guest binary is resolved above")?,
                headroom_mib: u64::from(config.rootfs_headroom_mib()),
            };
            // The disk is the image's base (shared by every sandbox of the
            // image on this node, so cached) plus this sandbox's own files.
            // Artifacts are copied into the sandbox's own disk, so a sandbox
            // with any, like every sandbox on a node with the cache off,
            // builds its disk alone.
            let copies_artifacts = staged.iter().any(|artifact| {
                artifact.name.is_some() && !matches!(artifact.payload, StagedPayload::Image { .. })
            });
            let cache = (!copies_artifacts).then(rootfs_cache::node_cache).flatten();
            let outcome = if let Some(cache) = cache {
                // A clone of (or the source of) the shared base: its own
                // files are written into it after.
                let inputs = base.inputs(&image_config);
                let (facts, outcome) =
                    cache.materialize(Arc::new(base), &inputs, &root_disk, &rootfs_dir)?;
                let files = microvm::guest_payload_files(&payload, &name, &facts);
                microvm::inject_guest_payload(&root_disk, &files, &scratch.join("payload"))?;
                outcome
            } else {
                // Built for this sandbox only: its own files go into the
                // tree, and one mkfs makes the disk, as with no cache.
                let built = (|| {
                    use rootfs_cache::BaseBuilder;
                    let facts = base.tree(&rootfs_dir)?;
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
                    let files = microvm::guest_payload_files(&payload, &name, &facts);
                    microvm::write_guest_payload_into_tree(&rootfs_dir, &files)?;
                    base.format(&rootfs_dir, &root_disk)
                })();
                // The extracted tree has served its purpose; the disk image is
                // the artifact from here on, and keeping both doubles the
                // footprint.
                let _ = std::fs::remove_dir_all(&rootfs_dir);
                built?;
                rootfs_cache::Outcome::Built(rootfs_cache::Fill::Skipped(if copies_artifacts {
                    "the sandbox copies artifacts into its root disk"
                } else {
                    "the cache is off"
                }))
            };
            // The disk carries this sandbox's env, which can hold secrets.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&root_disk, std::fs::Permissions::from_mode(0o600))
                    .with_context(|| format!("chmod {}", root_disk.display()))?;
            }
            std::fs::write(&stamp_path, &stamp)
                .with_context(|| format!("write {}", stamp_path.display()))?;
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                image = %config.image,
                root_disk = %outcome,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Sandbox root disk ready"
            );
        }
        instance.start_timings.record(
            &task.runtime,
            start_stage::ROOT_DISK,
            root_disk_started.elapsed(),
        );
        let vmm_config_started = std::time::Instant::now();

        let mut spec = VmSpec {
            name: name.clone(),
            kernel,
            extra_cmdline: guest_cmdline(
                lease.as_ref().map(|lease| lease.kernel_ip_arg(&name)),
                config.kernel_args.as_deref(),
            ),
            root_disk,
            volume_disks: volumes.disks,
            vcpus: config.vcpus(),
            memory_mib: config.memory_mib(),
            tap,
            api_socket: scratch.join("vmm.sock"),
            vsock: Some(microvm::VsockSpec {
                socket: vsock_socket_base(&scratch),
            }),
            identity_disk: None,
            balloon: None,
        };
        // A sandbox the job may suspend or snapshot boots behind stable
        // device links with an identity disk, so a memory checkpoint of it
        // can be restored into a later allocation (see sandbox_checkpoint).
        // A gated one too: every restore moves its vsock to the restoring
        // allocation's socket.
        let checkpointable = if instance.checkpoint_launch {
            sandbox_checkpoint::prepare_checkpoint_launch(
                hypervisor, allocation, &task.name, instance, &scratch, &mut spec, &tokens,
            )?;
            true
        } else {
            false
        };
        if !checkpointable && file_tokens {
            // No checkpoint support, but token files to keep fresh: the
            // identity disk alone, attached directly (no device links, since
            // nothing restores this guest elsewhere).
            let path = sandbox_checkpoint::identity_disk_path(&scratch);
            crate::task_plugins::checkpoint::write_identity_disk(
                &path,
                &sandbox_checkpoint::boot_identity(allocation, &task.name, &spec.name, &tokens),
            )?;
            spec.identity_disk = Some(path);
        }
        // A balloon for the guests that run above a reservation: the
        // `reclaim` pressure step's lever for guest RAM. Recorded beside the
        // socket so reclaim knows the bounds without re-reading the task.
        let _ = std::fs::remove_file(scratch.join(BALLOON_RECORD));
        let reserved_mib = memory_reservation_mib(&task.resources);
        if checkpointable || reserved_mib.is_some_and(|reserved| reserved < spec.memory_mib) {
            spec.balloon = Some(microvm::BalloonSpec {
                free_page_reporting: matches!(hypervisor, Backend::CloudHypervisor { .. }),
            });
            write_balloon_record(
                &scratch,
                &BalloonRecord {
                    guest_mib: u64::from(spec.memory_mib),
                    reserved_mib: u64::from(reserved_mib.unwrap_or(spec.memory_mib)),
                },
            )?;
        }
        // A stale socket from a previous boot makes the VMM refuse to start.
        let _ = std::fs::remove_file(&spec.api_socket);
        if let Some(vsock) = spec.vsock.as_ref() {
            let _ = std::fs::remove_file(&vsock.socket);
        }

        let mut cmd = match hypervisor {
            Backend::CloudHypervisor { binary } => {
                let mut cmd = Command::new(binary);
                cmd.args(microvm::cloud_hypervisor_args(&spec));
                cmd
            }
            Backend::Firecracker { binary } => {
                let config_path = scratch.join("firecracker.json");
                std::fs::write(
                    &config_path,
                    serde_json::to_vec_pretty(&microvm::firecracker_config(&spec))?,
                )
                .with_context(|| format!("write {}", config_path.display()))?;
                let mut cmd = Command::new(binary);
                // `--id` labels the VM in Firecracker's own logs/metrics. The
                // VM always boots straight from the config file. A sandbox
                // that may be memory-checkpointed also gets an API socket
                // (the checkpoint's control channel); every other one runs
                // `--no-api`, with no control channel at all.
                cmd.arg("--id").arg(&spec.name);
                // A ballooned guest needs the socket too: the balloon is
                // driven over the API.
                if sandbox_checkpoint::firecracker_needs_api_socket(
                    CheckpointSupport::Memory,
                    checkpointable,
                ) || spec.balloon.is_some()
                {
                    cmd.arg("--api-sock").arg(&spec.api_socket);
                } else {
                    cmd.arg("--no-api");
                }
                cmd.arg("--config-file").arg(&config_path);
                cmd
            }
            other => bail!("{} is not a microVM monitor", other.label()),
        };
        // The VMM process *is* the guest, as far as the host kernel is
        // concerned: joining it to the allocation's cgroup before it execs
        // is what puts guest RAM under `memory.max` and makes it measurable.
        // A no-op when the worker governs nothing.
        crate::task_plugins::join_assignment_cgroup(&mut cmd, instance)?;
        instance.start_timings.record(
            &task.runtime,
            start_stage::VMM_CONFIG,
            vmm_config_started.elapsed(),
        );
        Ok(cmd)
    }

    /// macOS dev mode: Apple `container` (still VM-isolated, but Apple owns
    /// the kernel and disks, so filesystem volumes become bind mounts).
    fn start_apple_container(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        config: &SandboxTaskConfig,
        env: &HashMap<String, String>,
        instance: &TaskInstance,
        backend: &Backend,
    ) -> R<Command> {
        reject_block_volumes_in_dev_mode(&instance.volumes, backend)?;
        check_apple_memory_floor(&task.name, config)?;
        let name = apple_container_name(allocation);
        remove_stale_dev_sandbox(backend, &name, allocation)?;
        // Named for the usage sampler: `container stats` is how an idle
        // policy sees this sandbox on macOS, where there is no cgroup.
        let scratch = work_root(allocation, instance);
        std::fs::create_dir_all(&scratch)?;
        std::fs::write(scratch.join(APPLE_CONTAINER_RECORD), &name)
            .with_context(|| format!("failed to record Apple container {name}"))?;
        let mut env: BTreeMap<String, String> = env.clone().into_iter().collect();
        env.extend(instance.volume_env());

        let identity_dir = match (has_file_identities(task), instance.task_dir.as_ref()) {
            (true, Some(task_dir)) => {
                let dir = task_dir.join(HOST_IDENTITY_SUBDIR);
                std::fs::create_dir_all(&dir)?;
                Some(dir)
            }
            _ => None,
        };
        let mut cmd = Command::new("container");
        cmd.args(build_apple_launch(
            &name,
            // What stale-container cleanup checks before it removes anything.
            &apple_container::ownership_labels(allocation),
            config,
            &env,
            &instance.volumes,
            identity_dir.as_deref(),
        ));
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
    ) -> R<(Command, oci_bundle::BuiltBundle)> {
        reject_block_volumes_in_dev_mode(&instance.volumes, backend)?;
        // The same runtimes the container task runs on (runc or crun), with
        // the same error for a missing or unsupported one.
        crate::task_plugins::container::probe_oci_runtime(binary)?;
        let scratch = work_root(allocation, instance);
        let name = sandbox_name(allocation);
        remove_stale_dev_sandbox(backend, &name, allocation)?;
        let layout = oci_image::resolve_layout(&config.image, instance, &scratch)?;

        let mut env = env.clone();
        env.extend(instance.volume_env());

        let bundle = scratch.join("bundle");
        let built = oci_bundle::build_bundle(
            &layout,
            &bundle,
            &config.command,
            &env,
            &oci_bundle::BundleContext {
                hostname: &name,
                volumes: &instance.volumes,
                // The task dir is not mounted for the sandbox runtime: a
                // microVM has no host bind mounts, and its dev-mode container
                // must behave the same way so a jobspec does not change
                // meaning per backend.
                task_dir: None,
                // Worker core already created this allocation's cgroup; the
                // OCI runtime places the container in it and re-applies the
                // same limits from the spec. The launcher itself stays
                // outside, so only the workload is charged.
                assignment: instance.resource_assignment.as_ref(),
                resolver: None,
                resolv_conf: None,
                netns: None,
                security: oci_bundle::BundleSecurity::default(),
                // Docker's defaults, like a `container` task that adjusts
                // nothing: a dev-mode sandbox runs the same stock images. A
                // sandbox cannot drop capabilities, so the node's container
                // allowlist trims the set instead of failing the start.
                policy: &oci_bundle::ProcessPolicy {
                    capabilities: oci_bundle::CapabilityAllowlist::from_env()
                        .intersect(oci_bundle::default_capabilities()),
                    rlimits: oci_bundle::default_rlimits(),
                },
            },
        )?;
        let mut cmd = Command::new(binary);
        cmd.arg("run").arg("--bundle").arg(&bundle).arg(&name);
        Ok((cmd, built))
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

    /// The VMM process runs in the allocation's worker-owned cgroup, so
    /// guest memory is charged, limited, and measured there like any host
    /// process. Dev-mode backends hand the same cgroup to their OCI runtime.
    fn resource_enforcement(&self) -> crate::task_plugins::ResourceEnforcement {
        crate::task_plugins::ResourceEnforcement::WorkerCgroup
    }

    /// A microVM's cgroup holds the whole VMM: guest RAM plus the monitor's
    /// own memory. Its ceiling is the guest size plus the worker's
    /// `sandbox.vmmOverheadMiB`, so a guest using all of its memory is not
    /// OOM-killed for the VMM's share. Dev-mode backends run the workload
    /// directly and keep the declared `resources.memory`.
    fn enforced_memory_limit_bytes(&self, task: &TaskConfig) -> Option<u64> {
        let config = SandboxTaskConfig::from_value(&task.config).ok()?;
        let backend = Backend::detect(config.hypervisor.as_deref()).ok()?;
        backend
            .is_microvm()
            .then(|| vmm_cgroup_memory_bytes(config.memory_mib(), vmm_overhead_mib()))
    }

    /// Fix the guest size at submit, so it can never change under a stored
    /// job (docs/task-runtime-sandbox.md, "Guest size").
    ///
    /// - An undeclared `resources.memory` becomes this runtime's own 512 MiB
    ///   default, ahead of the manager's `taskResourceDefaults`. That is the
    ///   size a sandbox always booted with, so the scheduler now reserves
    ///   what the guest gets, and a cluster default tuned for small host
    ///   processes cannot silently shrink every guest.
    /// - An unset `config.memory_mib` is pinned to the derived guest size
    ///   (`memoryMax`, else `memory`). The worker reads only `memory_mib`, so
    ///   a job stored before this rule (no `memory_mib`) keeps booting at
    ///   512 MiB exactly as it did, rather than shrinking to a defaulted
    ///   reservation it never asked for.
    /// - A TAP sandbox gains `sandbox.bridges contains "<bridge>"`, so it
    ///   only places on nodes whose worker config gives its bridge an
    ///   address pool ([`constrain_to_bridge`]).
    fn normalize_task(&self, task: &mut TaskConfig) {
        constrain_to_bridge(task);
        if !task.resources.is_object() {
            task.resources = Value::Object(Default::default());
        }
        if let Some(resources) = task.resources.as_object_mut() {
            resources
                .entry("memory")
                .or_insert_with(|| Value::from(DEFAULT_MEMORY_MIB));
        }
        let Some(config) = task.config.as_object_mut() else {
            return;
        };
        if config.get("memory_mib").is_none_or(Value::is_null) {
            if let Some(guest) = resources_guest_mib(&task.resources) {
                config.insert("memory_mib".into(), Value::from(guest));
            }
        }
    }

    /// A guest too small to boot is an error, and an explicit `memory_mib`
    /// that disagrees with the size `resources` imply is a warning this
    /// release (a future release rejects it).
    fn check_resources(&self, task: &TaskConfig) -> R<Vec<String>> {
        let Ok(config) = SandboxTaskConfig::from_value(&task.config) else {
            // `validate_config` reports the parse error with its own message.
            return Ok(Vec::new());
        };
        // An undeclared `memory` becomes this runtime's 512 MiB at submit.
        let derived = resources_guest_mib(&task.resources).or(Some(DEFAULT_MEMORY_MIB));
        // A task pinned to macOS runs in dev mode on Apple `container`, which
        // refuses a VM under its floor. Known at submit only through the
        // task's constraints; any other task meets the same check at start
        // on a macOS worker.
        if let Some(guest) = config.memory_mib.or(derived) {
            if crate::task_plugins::container::constrained_to_macos(task)
                && u64::from(guest) < crate::task_plugins::container::APPLE_CONTAINER_MIN_MEMORY_MIB
            {
                return Err(crate::task_plugins::container::apple_memory_floor_error(
                    &task.name,
                    "config.memory_mib, else resources.memoryMax or resources.memory",
                    f64::from(guest),
                ));
            }
        }
        let mut warnings = Vec::new();
        match (config.memory_mib, derived) {
            (None, Some(guest)) if guest < MIN_MEMORY_MIB => bail!(
                "task {} would boot a {guest} MiB sandbox guest (its resources.memoryMax, else \
                 resources.memory), below the {MIN_MEMORY_MIB} MiB a Linux guest needs. Raise \
                 resources.memory or resources.memoryMax",
                task.name
            ),
            (Some(explicit), Some(guest)) if explicit != guest => warnings.push(format!(
                "task {} sets config.memory_mib = {explicit}, but its resources size the guest at \
                 {guest} MiB (resources.memoryMax, else resources.memory). This release still \
                 boots {explicit} MiB, and a future release rejects the mismatch. Drop memory_mib and \
                 set resources.memoryMax = {explicit} to burst to it, or resources.memory = \
                 {explicit} to reserve it. See docs/task-runtime-sandbox.md",
                task.name
            )),
            _ => {}
        }
        Ok(warnings)
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
        // The typed parse first: it names the replacement for a removed
        // field (`network.tap`, `network.mac`), which the schema would only
        // call unknown.
        SandboxTaskConfig::from_value(config)?;
        validate_schema(&self.schema(), config)?;
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

    /// Identity files reach the guest through its identity disk (a microVM)
    /// or a read-only bind mount (dev mode), both under `/legion/identity`,
    /// and a refresh rewrites them with no restart.
    fn supports_identity_file(&self) -> bool {
        true
    }

    fn file_delivery_root(&self) -> Option<&'static str> {
        Some(GUEST_IDENTITY_ROOT)
    }

    fn file_delivery_host_subdir(&self) -> Option<&'static str> {
        Some(HOST_IDENTITY_SUBDIR)
    }

    /// A refreshed token reaches a microVM guest by rewriting its identity
    /// disk in place: the guest's handler sees new tokens with the same
    /// allocation and rewrites `/legion/identity/` with no restart. Dev mode
    /// needs nothing: the bind mount already shows the rewritten host file.
    fn identity_tokens_refreshed(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
    ) -> R<()> {
        let backend = Backend::detect(configured_backend(task).as_deref())?;
        if !backend.is_microvm() {
            return Ok(());
        }
        let disk = sandbox_checkpoint::identity_disk_path(&work_root(allocation, instance));
        if !disk.exists() {
            // Launched without an identity disk (no checkpoint support and
            // no token files): env tokens refresh by restart only.
            return Ok(());
        }
        let task_env = task.config.get("env").and_then(Value::as_object);
        let tokens = guest_tokens(
            task,
            &|key| {
                task_env
                    .and_then(|env| env.get(key))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            },
            instance.task_dir.as_deref(),
        );
        if sandbox_checkpoint::refresh_identity_tokens(&disk, &tokens)? {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                "Refreshed workload identity tokens on the sandbox's identity disk"
            );
        }
        Ok(())
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
        // A microVM takes exec through its guest agent over vsock; dev mode
        // on macOS borrows Apple `container exec`. Either way the session is
        // sandboxed, never host-level.
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
                "sandbox exec is not supported on {}: the Linux dev path runs no guest agent",
                hypervisor.label()
            );
        }
        if hypervisor.is_microvm() {
            // The guest agent's exec listener, through the VMM's vsock host
            // socket. The guest runs it as root in the task's environment.
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                tty = opts.tty,
                "Opening exec session in a microVM over vsock"
            );
            return crate::task_plugins::vsock_exec::open(
                &vsock_socket_base(&work_root(allocation, _runtime)),
                &opts,
            );
        }
        let name = match hypervisor {
            Backend::AppleContainer => apple_container_name(allocation),
            _ => sandbox_name(allocation),
        };
        opts.command = build_exec_command(&name, opts.tty, &opts.command);

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
        _task: &TaskConfig,
        instance: &mut TaskInstance,
        open: bool,
    ) -> R<()> {
        if crate::task_plugins::allocation_is_gated(allocation) {
            // A gated guest's only door is the datapath: the gate is the
            // policy-withheld marker every vsock listener checks.
            return crate::task_plugins::set_gated_namespace_gate(allocation, open);
        }
        // The device the guest was wired to at boot (per instance).
        let Some(device) = sandbox_checkpoint::attached_tap(&work_root(allocation, instance))
        else {
            // Loopback-only guest: nothing to gate, and nothing to open.
            return Ok(());
        };
        set_tap_link(&device, open).with_context(|| {
            format!(
                "failed to {} TAP device {} for allocation {}",
                if open { "open" } else { "shut" },
                device,
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
            if crate::task_plugins::allocation_is_gated(allocation) {
                bail!(
                    "sandbox task {} is network.mode = \"gated\", which needs a microVM backend \
                     (cloud-hypervisor or firecracker) for its vsock datapath; this node resolved \
                     `{}` — set the task's hypervisor, or use network.mode = \"host\"",
                    task.name,
                    hypervisor.label()
                );
            }
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

        // The uid/gid a dev-mode OCI bundle runs as, to hand it its logs.
        let mut log_owner: Option<(u32, u32)> = None;
        let mut cmd = match &hypervisor {
            Backend::CloudHypervisor { .. } | Backend::Firecracker { .. } => {
                self.start_microvm(&hypervisor, allocation, task, &config, &env, instance)?
            }
            Backend::AppleContainer => {
                self.start_apple_container(allocation, task, &config, &env, instance, &hypervisor)?
            }
            Backend::OciRuntime { binary } => {
                let binary = binary.clone();
                let (cmd, built) = self.start_oci_runtime(
                    &binary,
                    allocation,
                    &config,
                    &env,
                    instance,
                    &hypervisor,
                )?;
                log_owner = Some((built.uid, built.gid));
                cmd
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
        // A non-root image user must be able to reopen /dev/stdout and
        // /dev/stderr, which are these files (see `hand_task_logs_to`).
        if let Some((uid, gid)) = log_owner {
            oci_bundle::hand_task_logs_to(instance.log_dir.as_deref(), uid, gid);
        }
        let spawn_started = std::time::Instant::now();
        let child = spawn_task_process(&mut cmd).with_context(|| {
            format!(
                "Failed to spawn sandbox task {} for allocation {}",
                task.name, allocation.id
            )
        })?;
        instance.start_timings.record(
            &task.runtime,
            start_stage::VMM_SPAWN,
            spawn_started.elapsed(),
        );
        instance.pid = Some(child.id());
        instance.child = Some(child);
        Ok(())
    }

    /// A microVM is ready once its guest agent accepts exec over vsock: the
    /// generated init starts the agent's listener just before the task's
    /// command, so this is the moment a caller can run code in the guest.
    /// Dev backends (Apple `container`, a Linux OCI runtime) are ready when
    /// started.
    fn wait_ready(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &TaskInstance,
        timeout: std::time::Duration,
    ) -> R<()> {
        let config = SandboxTaskConfig::from_value(&task.config)?;
        if !Backend::detect(config.hypervisor.as_deref())?.is_microvm() {
            return Ok(());
        }
        let socket = vsock_socket_base(&work_root(allocation, instance));
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if crate::task_plugins::vsock_exec::agent_listening(
                &socket,
                std::time::Duration::from_millis(100),
            ) {
                return Ok(());
            }
            if instance
                .pid
                .is_some_and(|pid| !crate::signals::pid_alive(pid))
            {
                bail!("the VMM exited before its guest's agent was listening");
            }
            if std::time::Instant::now() >= deadline {
                bail!(
                    "the guest's agent did not accept exec within {}s (is the static \
                     legion-guest installed beside the worker?)",
                    timeout.as_secs()
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    /// A microVM is shut down **from inside**. Signalling the VMM would kill
    /// the hypervisor with the guest's page cache in it, so writes the
    /// workload made in the last ~30s would never reach its volume. Instead
    /// the guest agent signals the guest's init with `ctx.signal`, and init
    /// flushes the disks, passes the signal to the command, waits for it,
    /// unmounts the volumes, and powers off. The worker holds the allocation
    /// (and its volumes) until the VMM exits, or until the deadline, where
    /// `stop` flushes once more and kills it.
    ///
    /// Declined (the worker stops it at once, through `stop`) for the dev
    /// backends, which run no guest, and when the agent cannot be reached.
    fn stop_with(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
        ctx: &crate::task_plugins::StopContext,
    ) -> R<bool> {
        let Ok(backend) = Backend::detect(configured_backend(task).as_deref()) else {
            return Ok(false);
        };
        if !backend.is_microvm() || !vmm_may_be_running(instance) {
            return Ok(false);
        }
        let vsock = vsock_socket_base(&work_root(allocation, instance));
        // The request is an exec round trip into the guest, which takes tens
        // of milliseconds on a busy node, and this runs under the worker's
        // task-manager lock: every other start, stop, and heartbeat on the
        // node waited on it. It goes out on its own thread instead, and the
        // worker waits for the VMM to exit as for any graceful stop. A
        // request the guest cannot take stops the VMM hard from that thread,
        // as `stop` would (whose flush such a guest could not take either).
        let (allocation_id, task_name) = (allocation.id, task.name.clone());
        let (signal, grace, cause) = (ctx.signal.clone(), ctx.grace, ctx.cause.to_string());
        let pid = instance.pid;
        let request = move || match request_guest_stop(&vsock, &signal, GUEST_STOP_REQUEST_TIMEOUT)
        {
            Ok(()) => info!(
                allocation_id,
                task_name = %task_name,
                signal = %signal,
                grace_secs = grace.as_secs(),
                cause = %cause,
                "Asked the sandbox guest to shut down"
            ),
            Err(err) => {
                warn!(
                    allocation_id,
                    task_name = %task_name,
                    error = %format!("{err:#}"),
                    "Could not deliver a stop request into the sandbox guest; stopping \
                     it hard"
                );
                if let Some(pid) = pid.filter(|pid| crate::signals::pid_alive(*pid)) {
                    // SAFETY: a plain kill(2) of this allocation's VMM.
                    unsafe {
                        libc::kill(pid as libc::pid_t, libc::SIGKILL);
                    }
                }
            }
        };
        // A pooled blocking thread where there is a runtime, for the same
        // reason as a session's launch (`admit_and_launch`).
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn_blocking(request);
            }
            Err(_) => {
                std::thread::Builder::new()
                    .name(format!("sandbox-stop-{allocation_id}"))
                    .spawn(request)
                    .context("spawn the sandbox stop request")?;
            }
        }
        Ok(true)
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
            let teardown: Option<(String, Vec<String>, std::time::Duration)> = match &hypervisor {
                Backend::AppleContainer => Some((
                    apple_container::BIN.to_string(),
                    vec!["stop".into(), apple_container_name(allocation)],
                    apple_container::STOP_TIMEOUT,
                )),
                Backend::OciRuntime { binary } => Some((
                    binary.clone(),
                    vec!["delete".into(), "--force".into(), name.clone()],
                    crate::util::RUNTIME_CLI_TIMEOUT,
                )),
                Backend::CloudHypervisor { .. } | Backend::Firecracker { .. } => {
                    // A VMM still running here is being stopped hard: the
                    // guest ignored its stop request past the deadline, or
                    // nothing asked (a restart, a disk suspend, a destroy).
                    // Flush the guest first, or killing the VMM drops every
                    // write still in its page cache.
                    if vmm_may_be_running(instance) {
                        let vsock = vsock_socket_base(&work_root(allocation, instance));
                        match flush_guest(&vsock, GUEST_FLUSH_TIMEOUT) {
                            Ok(()) => info!(
                                allocation_id = allocation.id,
                                task_name = %task.name,
                                "Flushed the sandbox guest's disks before stopping its VMM"
                            ),
                            Err(err) => warn!(
                                allocation_id = allocation.id,
                                task_name = %task.name,
                                error = %format!("{err:#}"),
                                "Could not flush the sandbox guest's disks before stopping its \
                                 VMM; writes it had not written back are lost"
                            ),
                        }
                    }
                    None
                }
            };
            if let Some((program, args, timeout)) = teardown {
                info!(
                    allocation_id = allocation.id,
                    task_name = %task.name,
                    backend = hypervisor.label(),
                    "Tearing down dev-mode sandbox by name"
                );
                // Bounded: a hung runtime must not hold the worker's
                // task-manager lock, which teardown can run under.
                if let Err(err) =
                    crate::util::output_with_timeout(Command::new(program).args(&args), timeout)
                {
                    warn!(
                        allocation_id = allocation.id,
                        task_name = %task.name,
                        error = %err,
                        "Dev-mode sandbox teardown by name failed"
                    );
                }
            }
        }

        reap_task_process(allocation.id, &task.name, instance, "sandbox");
        // Whatever the VMM left behind (a helper it forked, a dev-mode
        // container init) is still in the allocation's cgroup.
        if let Some(assignment) = instance.resource_assignment.as_ref() {
            crate::worker::resources::kill_cgroup(assignment);
        }
        Ok(())
    }

    fn checkpoint_support(&self, query: &CheckpointQuery<'_>) -> CheckpointSupport {
        sandbox_checkpoint::sandbox_checkpoint_support(query)
    }

    /// A real VMM captures memory (see `sandbox_checkpoint`). The dev
    /// backends capture only the volume half, which worker core owns, so
    /// they behave like the trait default: stop when asked, no parts.
    fn checkpoint(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
        out: &CheckpointDir,
        then: AfterCheckpoint,
    ) -> R<MemoryParts> {
        let backend = Backend::detect(configured_backend(task).as_deref())?;
        if !backend.is_microvm() {
            if then == AfterCheckpoint::Stop {
                self.stop(allocation, task, instance)?;
            }
            return Ok(MemoryParts::default());
        }
        let scratch = work_root(allocation, instance);
        sandbox_checkpoint::checkpoint_microvm(
            &backend, allocation, &task.name, instance, &scratch, out, then,
        )
    }

    fn resume_after_checkpoint(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
    ) -> R<()> {
        let backend = Backend::detect(configured_backend(task).as_deref())?;
        if !backend.is_microvm() {
            return Ok(());
        }
        sandbox_checkpoint::resume_microvm(&backend, &work_root(allocation, instance))
    }

    /// The balloon, on a guest booted with one. A dev backend, or a guest
    /// booted without a balloon, has no lever (`None`).
    fn reclaim_guest_memory(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &TaskInstance,
        bytes: u64,
    ) -> R<Option<u64>> {
        let backend = Backend::detect(configured_backend(task).as_deref())?;
        if !backend.is_microvm() {
            return Ok(None);
        }
        inflate_balloon(&backend, &work_root(allocation, instance), bytes)
    }

    fn release_guest_memory(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &TaskInstance,
    ) -> R<()> {
        let backend = Backend::detect(configured_backend(task).as_deref())?;
        if !backend.is_microvm() {
            return Ok(());
        }
        deflate_balloon(&backend, &work_root(allocation, instance))
    }

    fn restore(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
        manifest: &SnapshotManifest,
        source: &CheckpointDir,
        identity: &ResumeIdentity,
    ) -> R<()> {
        if manifest.kind == SnapshotKind::Disk {
            // The volumes already hold the captured state: boot on them.
            return self.start(allocation, task, instance);
        }
        let config = SandboxTaskConfig::from_value(&task.config)?;
        let backend = Backend::detect(config.hypervisor.as_deref())?;
        let target = sandbox_checkpoint::RestoreTarget {
            backend: &backend,
            allocation,
            task_name: &task.name,
            hostname: sandbox_name(allocation),
            vcpus: config.vcpus(),
            memory_mib: config.memory_mib(),
            scratch: work_root(allocation, instance),
            memory_restore: config.memory_restore(),
            userfaultfd: sandbox_checkpoint::userfaultfd_available(),
            tap: config
                .tap_for(identity.instance_id.as_deref(), allocation.id)
                .map(|tap| tap.device),
            bridge: config.tap_bridge(),
            // Every restore moves the guest's vsock onto this allocation's
            // own socket: exec dials it, and on a gated guest the datapath
            // already listens beside it.
            vsock: Some(vsock_socket_base(&work_root(allocation, instance))),
            // Only a gated allocation's datapath binds an egress proxy there;
            // waiting on any other would cost the full wait every restore.
            gated: allocation
                .network
                .as_ref()
                .is_some_and(crate::structs::AllocNetwork::is_gated),
            datapath_wait: sandbox_checkpoint::DATAPATH_WAIT,
        };
        let scratch = target.scratch.clone();
        let guest_mib = u64::from(target.memory_mib);
        // The new allocation's file-delivered tokens ride the identity disk
        // the restore writes, beside its env tokens.
        let mut identity = identity.clone();
        identity.files = guest_tokens(task, &|_| None, instance.task_dir.as_deref()).files;
        // A TAP guest takes a fresh address from its bridge's pool on this
        // node, and the resume handler applies it (replacing whatever the
        // captured guest had). A resume asks for its previous address first.
        if let Some(bridge) = config.tap_bridge() {
            let pool = sandbox_addr::pool_for(&bridge)?;
            let previous = (!identity.fork)
                .then(|| sandbox_checkpoint::recorded_guest_address(manifest))
                .flatten();
            let lease = sandbox_addr::lease(
                &pool,
                &scratch,
                alloc_root(instance).as_deref(),
                previous,
                &allocation.node_id,
            )?;
            identity.guest_addr = Some(lease.guest_addr());
        }
        // The restored workload still listens on the ports it was captured
        // with. A gated guest's agent serves exactly those (plus any port
        // the job added while it was suspended), and the refreshed env says
        // where each one is advertised now.
        let guest_ports =
            sandbox_checkpoint::restored_guest_ports(manifest, allocation.network.as_ref());
        // Likewise its upstreams: the workload dials the loopback ports it
        // was captured with, and the agent relays each to this allocation's
        // local port for it, where its datapath listens.
        let guest_upstreams =
            sandbox_checkpoint::restored_guest_upstreams(manifest, allocation.network.as_ref());
        if target.vsock.is_some() {
            let mut ports: Vec<u16> = guest_ports.values().copied().collect();
            ports.sort_unstable();
            ports.dedup();
            identity.ingress_ports = ports;
            identity.upstreams =
                gated_guest_upstreams(allocation.network.as_ref(), &guest_upstreams);
        }
        identity.env.extend(restored_port_env(
            allocation,
            &guest_ports,
            &guest_upstreams,
        ));
        sandbox_checkpoint::restore_microvm(
            &target,
            instance,
            manifest,
            source,
            &identity,
            &sandbox_checkpoint::HostTaps,
        )?;
        // Every memory-checkpointable guest boots with a balloon, and the
        // device rides the snapshot, so the restored guest has one too.
        write_balloon_record(
            &scratch,
            &BalloonRecord {
                guest_mib,
                reserved_mib: memory_reservation_mib(&task.resources)
                    .map(u64::from)
                    .unwrap_or(guest_mib),
            },
        )
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
        sandbox_checkpoint::release_links(instance, &work_root(allocation, instance));
        sandbox_checkpoint::release_restore_tap(
            &sandbox_checkpoint::HostTaps,
            &work_root(allocation, instance),
        );
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

    /// `reclaim` on a ballooned Firecracker guest inflates the balloon by
    /// the request, capped at the guest's room above its reservation, and
    /// the episode's end deflates it. A guest booted without a balloon has
    /// no lever, so the worker falls back to `memory.reclaim`.
    #[test]
    fn reclaim_drives_the_balloon_within_the_reservation() -> R<()> {
        use crate::task_plugins::vmm_api::fake::FakeVmm;
        let scratch = tempfile::tempdir()?;
        let backend = Backend::Firecracker {
            binary: "firecracker".into(),
        };
        assert_eq!(inflate_balloon(&backend, scratch.path(), 1 << 30)?, None);

        write_balloon_record(
            scratch.path(),
            &BalloonRecord {
                guest_mib: 4096,
                reserved_mib: 512,
            },
        )?;
        let vmm = FakeVmm::start_at(scratch.path().join("vmm.sock"), false, |seen| {
            match (seen.method.as_str(), seen.path.as_str()) {
                ("GET", "/balloon") => (200, r#"{"amount_mib":3000,"deflate_on_oom":true}"#.into()),
                _ => (204, String::new()),
            }
        });
        // 1 GiB asked, 584 MiB of room left above the 512 MiB reservation.
        let freed = inflate_balloon(&backend, scratch.path(), 1 << 30)?;
        assert_eq!(freed, Some(584 * 1024 * 1024));
        deflate_balloon(&backend, scratch.path())?;
        let seen = vmm.drain();
        assert_eq!(seen[1].body, Some(json!({ "amount_mib": 3584 })));
        assert_eq!(seen[2].body, Some(json!({ "amount_mib": 0 })));
        Ok(())
    }

    #[test]
    fn reclaim_drives_a_cloud_hypervisor_balloon_in_bytes() -> R<()> {
        use crate::task_plugins::vmm_api::fake::FakeVmm;
        let scratch = tempfile::tempdir()?;
        let backend = Backend::CloudHypervisor {
            binary: "cloud-hypervisor".into(),
        };
        write_balloon_record(
            scratch.path(),
            &BalloonRecord {
                guest_mib: 2048,
                reserved_mib: 512,
            },
        )?;
        let vmm = FakeVmm::start_at(scratch.path().join("vmm.sock"), false, |seen| {
            match seen.path.as_str() {
                "/api/v1/vm.info" => (200, r#"{"config":{"balloon":{"size":0}}}"#.into()),
                _ => (204, String::new()),
            }
        });
        let freed = inflate_balloon(&backend, scratch.path(), 256 * 1024 * 1024)?;
        assert_eq!(freed, Some(256 * 1024 * 1024));
        let seen = vmm.drain();
        assert_eq!(seen[1].path, "/api/v1/vm.resize");
        assert_eq!(
            seen[1].body,
            Some(json!({ "desired_balloon": 256u64 * 1024 * 1024 }))
        );
        Ok(())
    }

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
            "network": { "mode": "tap", "bridge": "br0" },
        }))?;

        assert_eq!(parsed.image, "alpine:latest");
        assert_eq!(parsed.vcpus(), 2);
        assert_eq!(parsed.memory_mib(), 1024);
        assert_eq!(
            parsed.tap_for(Some("user-42"), 7),
            Some(TapNet {
                device: crate::structs::fork::own_tap_name("user-42"),
                mac: Some(crate::structs::fork::fork_mac(7))
            })
        );
        Ok(())
    }

    /// Gated plan §6.7: a TAP sandbox places only where its bridge has an
    /// address pool. The implicit constraint joins the task's own (an
    /// object-form constraint becomes the equivalent list), once.
    #[test]
    fn a_tap_sandbox_is_constrained_to_nodes_with_its_bridge() -> R<()> {
        let tap =
            json!({"image": "a", "command": ["x"], "network": { "mode": "tap", "bridge": "br0" }});
        let mut task = task_with(tap);
        task.constraints = json!({ "kernel.name": "linux" });
        SandboxTaskPlugin.normalize_task(&mut task);
        SandboxTaskPlugin.normalize_task(&mut task);
        assert_eq!(
            task.constraints,
            json!([
                { "key": "kernel.name", "value": "linux" },
                { "key": "sandbox.bridges", "operator": "contains", "value": "br0" },
            ])
        );
        let rules = crate::constraints::ConstraintSet::new(&task.constraints)?;
        let node = |config_attrs: Value| crate::structs::Node {
            id: "w-03".into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs,
            fingerprint_attrs: json!({ "kernel.name": "linux" }),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        };
        assert!(rules.is_satisfied(&node(json!({ "sandbox.bridges": ["br0", "br1"] })), None));
        assert!(!rules.is_satisfied(&node(json!({ "sandbox.bridges": ["br1"] })), None));
        assert!(!rules.is_satisfied(&node(json!({})), None));

        // A sandbox with no NIC gains nothing.
        let none = normalized(json!({"image": "a", "command": ["x"]}), json!({}));
        assert_eq!(none.constraints, json!({}));
        Ok(())
    }

    fn normalized(config: Value, resources: Value) -> TaskConfig {
        let mut task = task_with(config);
        task.resources = resources;
        SandboxTaskPlugin.normalize_task(&mut task);
        task
    }

    fn guest(task: &TaskConfig) -> R<u32> {
        Ok(SandboxTaskConfig::from_value(&task.config)?.memory_mib())
    }

    /// Guest size follows `resources` (`memoryMax`, else `memory`), fixed at
    /// submit. An undeclared `memory` is the runtime's 512 MiB, never the
    /// cluster default. An explicit `memory_mib` still wins this release,
    /// with a warning when it disagrees.
    #[test]
    fn guest_size_follows_resources_and_is_fixed_at_submit() -> R<()> {
        let config = json!({"image": "a", "command": ["x"]});
        let task = normalized(config.clone(), json!({ "memory": 512, "memoryMax": 4096 }));
        assert_eq!(guest(&task)?, 4096);
        assert!(SandboxTaskPlugin.check_resources(&task)?.is_empty());

        assert_eq!(
            guest(&normalized(config.clone(), json!({ "memory": 768 })))?,
            768
        );

        // Nothing declared: reserve and boot the runtime's own 512 MiB.
        let task = normalized(config.clone(), json!({}));
        assert_eq!(task.resources["memory"], json!(512));
        assert_eq!(guest(&task)?, 512);
        let task = normalized(config.clone(), Value::Null);
        assert_eq!(task.resources["memory"], json!(512));

        // A job stored before the rule (no memory_mib, a defaulted 256 MiB
        // reservation) keeps booting at 512 MiB on the worker.
        let mut legacy = task_with(config.clone());
        legacy.resources = json!({ "memory": 256 });
        assert_eq!(guest(&legacy)?, DEFAULT_MEMORY_MIB);

        // Explicit memory_mib wins, and a mismatch warns.
        let explicit = json!({"image": "a", "command": ["x"], "memory_mib": 1024});
        let mut declared = task_with(explicit.clone());
        declared.resources = json!({ "memory": 512 });
        let warnings = SandboxTaskPlugin.check_resources(&declared)?;
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("memory_mib = 1024"), "{}", warnings[0]);
        assert!(warnings[0].contains("resources.memoryMax = 1024"));
        assert_eq!(
            guest(&normalized(explicit.clone(), json!({ "memory": 512 })))?,
            1024
        );
        // With no memory declared, the implied size is the runtime default.
        let mut bare = task_with(explicit);
        bare.resources = json!({});
        assert_eq!(SandboxTaskPlugin.check_resources(&bare)?.len(), 1);

        // Agreeing is fine.
        let mut agreeing = task_with(json!({"image": "a", "command": ["x"], "memory_mib": 1024}));
        agreeing.resources = json!({ "memory": 256, "memoryMax": 1024 });
        assert!(SandboxTaskPlugin.check_resources(&agreeing)?.is_empty());

        // Too small to boot is an error, not a warning.
        let mut tiny = task_with(config);
        tiny.resources = json!({ "memory": 32 });
        assert!(SandboxTaskPlugin.check_resources(&tiny).is_err());
        Ok(())
    }

    #[test]
    fn vmm_cgroup_ceiling_is_guest_plus_overhead() {
        assert_eq!(
            vmm_cgroup_memory_bytes(512, 64),
            576 * 1024 * 1024,
            "the default guest with the default overhead"
        );
        assert_eq!(vmm_cgroup_memory_bytes(4096, 128), 4224 * 1024 * 1024);
    }

    #[test]
    fn sandbox_is_governed_through_the_worker_cgroup() {
        assert_eq!(
            SandboxTaskPlugin.resource_enforcement(),
            crate::task_plugins::ResourceEnforcement::WorkerCgroup
        );
        // A config that does not parse never overrides the ceiling; the task
        // fails at start with the parse error instead.
        assert_eq!(
            SandboxTaskPlugin.enforced_memory_limit_bytes(&task_with(json!({}))),
            None
        );
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
        assert_eq!(parsed.tap_for(None, 1), None);
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
            (
                "unknown memory restore",
                json!({"image": "a", "command": ["x"], "memory_restore": "fast"}),
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
    fn memory_restore_defaults_to_lazy_and_takes_eager() {
        let parsed = |config: Value| SandboxTaskConfig::from_value(&config).unwrap();
        assert_eq!(
            parsed(json!({"image": "a", "command": ["x"]})).memory_restore(),
            sandbox_checkpoint::MemoryRestore::Lazy
        );
        assert_eq!(
            parsed(json!({"image": "a", "command": ["x"], "memory_restore": "eager"}))
                .memory_restore(),
            sandbox_checkpoint::MemoryRestore::Eager
        );
        let err = SandboxTaskConfig::from_value(
            &json!({"image": "a", "command": ["x"], "memory_restore": "fast"}),
        )
        .expect_err("an unknown mode");
        assert!(
            err.to_string()
                .contains("memory_restore must be `lazy` or `eager`, got `fast`"),
            "{err}"
        );
    }

    #[test]
    fn config_rejects_tap_without_a_bridge() {
        let err = SandboxTaskConfig::from_value(
            &json!({"image": "a", "command": ["x"], "network": {"mode": "tap"}}),
        )
        .expect_err("tap without a bridge");
        assert!(err.to_string().contains("requires network.bridge"));

        let err = SandboxTaskConfig::from_value(
            &json!({"image": "a", "command": ["x"], "network": {"mode": "bridge"}}),
        )
        .expect_err("unknown mode");
        assert!(err.to_string().contains("must be `none` or `tap`"));
    }

    /// The guest's identity disk carries env tokens by key and file tokens
    /// read from the host's `<task dir>/identity/`, at their guest paths and
    /// modes. An undelivered token is left out.
    #[test]
    fn guest_tokens_read_env_and_the_host_identity_dir() -> R<()> {
        let task_dir = tempfile::tempdir()?;
        let host = task_dir.path().join(HOST_IDENTITY_SUBDIR);
        std::fs::create_dir_all(host.join("cloud"))?;
        std::fs::write(host.join("cloud").join("aws"), "aws-token")?;
        let mut task = task_with(json!({"image": "a", "command": ["x"]}));
        task.identities = serde_json::from_value(json!([
            { "name": "vault", "audience": ["v"], "env": "VAULT_TOKEN" },
            { "name": "aws", "audience": ["a"], "file": { "path": "cloud/aws", "mode": "0440" } },
            { "name": "gcp", "audience": ["g"], "file": { "path": "/legion/identity/gcp" } },
        ]))?;
        let env = |key: &str| (key == "VAULT_TOKEN").then(|| "vault-token".to_string());
        let tokens = guest_tokens(&task, &env, Some(task_dir.path()));
        assert_eq!(tokens.env["VAULT_TOKEN"], "vault-token");
        assert_eq!(tokens.files.len(), 1, "gcp was never delivered");
        assert_eq!(tokens.files[0].path, "/legion/identity/cloud/aws");
        assert_eq!(tokens.files[0].mode, 0o440);
        assert_eq!(tokens.files[0].token, "aws-token");
        Ok(())
    }

    #[test]
    fn apple_container_names_carry_the_node() {
        let mut allocation = Allocation {
            id: 7,
            node_id: "Mike's Mac.local".into(),
            ..alloc()
        };
        assert_eq!(
            apple_container_name(&allocation),
            "legion-sbx-mike-s-mac-local-7"
        );
        allocation.node_id.clear();
        assert_eq!(apple_container_name(&allocation), "legion-sbx-7");
    }

    #[test]
    fn apple_launch_mounts_the_identity_dir_read_only() -> R<()> {
        let config = SandboxTaskConfig::from_value(&json!({"image": "a", "command": ["x"]}))?;
        let args = build_apple_launch(
            "legion-sbx-7",
            &[],
            &config,
            &BTreeMap::new(),
            &[],
            Some(Path::new("/allocs/7/box/identity")),
        );
        assert!(
            args.windows(2).any(|pair| pair[0] == "--volume"
                && pair[1] == "/allocs/7/box/identity:/legion/identity:ro"),
            "{args:?}"
        );
        Ok(())
    }

    /// A TAP sandbox gets a device per instance (the name a fork of it
    /// would get), attached to the declared bridge, with a MAC per
    /// allocation.
    #[test]
    fn a_tap_is_named_per_instance_and_needs_a_bridge() -> R<()> {
        let tap = SandboxTaskConfig::from_value(&json!({
            "image": "a", "command": ["x"],
            "network": { "mode": "tap", "bridge": "br0" },
        }))?;
        assert_eq!(tap.tap_bridge().as_deref(), Some("br0"));
        let user = tap.tap_for(Some("user-42"), 7).expect("a TAP");
        assert_eq!(user.device, crate::structs::fork::own_tap_name("user-42"));
        assert_eq!(user.mac.as_deref(), Some("02:4c:00:00:00:07"));
        // Another instance, another device; the same instance on a later
        // allocation (a resume), the same device.
        assert_ne!(tap.tap_for(Some("user-43"), 8).unwrap().device, user.device);
        assert_eq!(
            tap.tap_for(Some("user-42"), 99).unwrap().device,
            user.device
        );
        // No instance: named after the allocation.
        assert_eq!(
            tap.tap_for(None, 7).unwrap().device,
            crate::structs::fork::own_tap_name("alloc-7")
        );
        let none = SandboxTaskConfig::from_value(&json!({"image": "a", "command": ["x"]}))?;
        assert!(none.tap_for(Some("user-42"), 7).is_none());

        let refused = |network: Value| {
            let err = SandboxTaskConfig::from_value(&json!({
                "image": "a", "command": ["x"], "network": network,
            }))
            .expect_err("refused");
            format!("{err:#}")
        };
        assert!(refused(json!({ "mode": "tap" })).contains("requires network.bridge"));
        assert!(
            refused(json!({ "mode": "tap", "bridge": "a-bridge-name-too-long" }))
                .contains("not a valid interface name")
        );
        assert!(refused(json!({ "bridge": "br0" })).contains("applies only to mode = \"tap\""));
        Ok(())
    }

    /// Gated plan Phase 4: `tap` and `mac` are gone, and submit names the
    /// replacement rather than "unknown field".
    #[test]
    fn a_hand_named_tap_or_mac_is_refused_with_the_replacement() {
        let refused = |network: Value| {
            let err = SandboxTaskConfig::from_value(&json!({
                "image": "a", "command": ["x"], "network": network,
            }))
            .expect_err("refused");
            format!("{err:#}")
        };
        let err = refused(json!({ "mode": "tap", "tap": "tap0" }));
        assert!(
            err.starts_with("config.network.tap is gone: Legion creates a TAP per instance"),
            "{err}"
        );
        assert!(
            err.contains("Hint: Set bridge = \"<the bridge tap0 was on>\""),
            "{err}"
        );
        assert!(err.contains("sandbox.bridges.<bridge>"), "{err}");
        let err = refused(json!({ "mode": "tap", "tap": "auto", "bridge": "br0" }));
        assert!(err.contains("Hint: Drop tap"), "{err}");
        let err = refused(json!({ "mode": "tap", "bridge": "br0", "mac": "02:00:00:00:00:01" }));
        assert!(err.starts_with("config.network.mac is gone"), "{err}");
    }

    /// The guest's kernel brings `eth0` up with the lease, ahead of the
    /// task's own `kernel_args` (which can override it).
    #[test]
    fn the_lease_rides_the_kernel_command_line() {
        assert_eq!(guest_cmdline(None, None), None);
        assert_eq!(guest_cmdline(None, Some("  ")), None);
        assert_eq!(guest_cmdline(None, Some("quiet")).as_deref(), Some("quiet"));
        assert_eq!(
            guest_cmdline(
                Some("ip=10.88.128.7::10.88.0.1:255.255.0.0:h:eth0:off".into()),
                Some("quiet")
            )
            .as_deref(),
            Some("ip=10.88.128.7::10.88.0.1:255.255.0.0:h:eth0:off quiet")
        );
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
            binary: "runc".into(),
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

    /// The stop request signals the guest's PID 1 with the task's stop
    /// signal, passed as an argument (never spliced into the script), and
    /// only a signal Legion delivers.
    #[test]
    fn the_stop_request_signals_init_with_the_stop_signal() -> R<()> {
        assert_eq!(
            guest_stop_command("SIGUSR1")?,
            vec!["/bin/sh", "-c", "kill -s \"$1\" 1", "legion-stop", "USR1"]
        );
        assert!(guest_stop_command("SIGKILL; reboot").is_err());
        Ok(())
    }

    /// The stop sequence, host side: the request travels through the VMM's
    /// vsock socket to the guest agent, which runs it.
    #[test]
    fn the_stop_request_reaches_the_guest_agent() -> R<()> {
        let scratch = tempfile::tempdir()?;
        let vsock = vsock_socket_base(scratch.path());
        let seen = crate::task_plugins::vsock_exec::fake_agent::listen(&vsock, Some(0));
        request_guest_stop(&vsock, "SIGTERM", GUEST_STOP_REQUEST_TIMEOUT)?;
        assert_eq!(seen.recv()?, guest_stop_command("SIGTERM")?);
        Ok(())
    }

    /// No agent to take the request (a VMM that is gone, or a guest without
    /// one) fails at once, so the worker falls back to a hard stop instead
    /// of waiting out the grace window for a guest that was never told.
    #[test]
    fn a_stop_request_with_no_agent_fails_fast() {
        let scratch = tempfile::tempdir().unwrap();
        let started = std::time::Instant::now();
        let err = request_guest_stop(
            &vsock_socket_base(scratch.path()),
            "SIGTERM",
            GUEST_STOP_REQUEST_TIMEOUT,
        )
        .expect_err("nothing listens");
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        assert!(
            format!("{err:#}").contains("needs the guest agent"),
            "{err:#}"
        );
    }

    /// The hard stop's flush is `sync` in the guest, and a failed one is
    /// reported rather than taken as done.
    #[test]
    fn the_flush_runs_sync_in_the_guest() -> R<()> {
        let scratch = tempfile::tempdir()?;
        let vsock = vsock_socket_base(scratch.path());
        let seen = crate::task_plugins::vsock_exec::fake_agent::listen(&vsock, Some(0));
        flush_guest(&vsock, GUEST_FLUSH_TIMEOUT)?;
        assert_eq!(seen.recv()?, vec!["sync".to_string()]);

        let failing = tempfile::tempdir()?;
        let vsock = vsock_socket_base(failing.path());
        let _seen = crate::task_plugins::vsock_exec::fake_agent::listen(&vsock, Some(1));
        let err = flush_guest(&vsock, GUEST_FLUSH_TIMEOUT).expect_err("sync failed");
        assert!(
            format!("{err:#}").contains("exited with status 1"),
            "{err:#}"
        );
        Ok(())
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
    fn apple_launch_binds_volumes_and_passes_the_command_through() -> R<()> {
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
        let args = build_apple_launch("legion-sbx-7", &[], &config, &env, &volumes, None);

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
        // The command follows the image verbatim, with no `--` separator,
        // which Apple `container` would run as the executable.
        assert!(!args.contains(&"--".to_string()), "no separator: {args:?}");
        let image = args.iter().position(|a| a == "alpine").expect("image");
        assert_eq!(args[image + 1], "-l");
        Ok(())
    }

    #[test]
    fn apple_launch_omits_sizing_flags_when_unset() -> R<()> {
        let config =
            SandboxTaskConfig::from_value(&json!({"image": "alpine", "command": ["true"]}))?;
        let args = build_apple_launch("legion-sbx-7", &[], &config, &BTreeMap::new(), &[], None);
        assert!(!args.contains(&"--cpus".to_string()));
        assert!(!args.contains(&"--memory".to_string()));
        assert!(!args.contains(&"--volume".to_string()));
        Ok(())
    }

    /// P1-6: Apple `container` runs a `--` after the image as the command
    /// ("failed to find target executable --"). The exec argv into a dev-mode
    /// sandbox must not carry one either, even for a command that starts with
    /// a dash.
    #[test]
    fn apple_exec_passes_the_command_without_a_separator() {
        let argv = build_exec_command("legion-sbx-7", true, &["-l".to_string()]);
        assert_eq!(
            argv,
            vec![
                "container",
                "exec",
                "--tty",
                "--interactive",
                "legion-sbx-7",
                "-l"
            ]
        );
        assert!(!argv.contains(&"--".to_string()), "{argv:?}");
        let argv = build_exec_command("legion-sbx-7", false, &["/bin/true".to_string()]);
        assert_eq!(argv, vec!["container", "exec", "legion-sbx-7", "/bin/true"]);
    }

    /// P2-6: a dev-mode sandbox on macOS is an Apple `container` VM, which
    /// refuses less than 200 MiB. Pinned to macOS, that fails at submit;
    /// otherwise the macOS start fails early with the same message. The
    /// microVM floor (64 MiB) is unchanged for everything else.
    #[test]
    fn apple_memory_floor_applies_to_dev_mode_on_macos() -> R<()> {
        let small = json!({ "image": "a", "command": ["x"], "memory_mib": 128 });
        let config = SandboxTaskConfig::from_value(&small)?;
        let err = check_apple_memory_floor("workspace", &config).expect_err("below the floor");
        assert!(err.to_string().contains("200 MiB"), "{err}");
        assert!(err.to_string().contains("memory_mib"), "{err}");
        check_apple_memory_floor(
            "workspace",
            &SandboxTaskConfig::from_value(
                &json!({ "image": "a", "command": ["x"], "memory_mib": 256 }),
            )?,
        )?;

        // Not pinned to macOS: 128 MiB is a valid microVM guest (sized by
        // `resources`, so no memory_mib mismatch warning either).
        let mut task = task_with(small);
        task.resources = json!({ "memory": 128 });
        assert!(SandboxTaskPlugin.check_resources(&task)?.is_empty());
        // Pinned to macOS: rejected at submit, naming the floor.
        task.constraints = json!({ "os": "macos" });
        let err = SandboxTaskPlugin
            .check_resources(&task)
            .expect_err("rejected at submit");
        assert!(err.to_string().contains("200 MiB"), "{err}");
        // The default 512 MiB guest clears it.
        let mut default_sized = task_with(json!({ "image": "a", "command": ["x"] }));
        default_sized.constraints = json!({ "os": "macos" });
        assert!(SandboxTaskPlugin
            .check_resources(&default_sized)?
            .is_empty());
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
        assert!(args.contains(&format!("path={},image_type=raw", root_disk.display())));
        assert!(
            args.contains(&format!("path={},image_type=raw", volume_img.display())),
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

    /// Gated plan Phase 0, G0.1: boot a gated sandbox on a real VMM with the
    /// node datapath bound on its vsock host sockets, and prove from inside
    /// the guest that an allowed fetch succeeds through the proxy, a denied
    /// one gets `403` with `Legion-Denied-By`, a name resolves through the
    /// relay, and a raw IP is unreachable without the proxy. Opt-in, like
    /// `sandbox_microvm_boots_and_captures_console`: it needs KVM, the VMM,
    /// a guest kernel with `CONFIG_VIRTIO_VSOCKETS=y`, the static guest
    /// binary, and outbound HTTP from the host. See
    /// internal_docs/suspend-resume-linux-verification.md, "Gated
    /// sandboxes".
    ///
    /// ```text
    /// LEGION_SANDBOX_KERNEL=/var/lib/legion/kernels/vmlinux \
    /// LEGION_GUEST_BINARY=$PWD/target/x86_64-unknown-linux-musl/dist/legion-guest \
    /// cargo test --lib sandbox_gated_boots -- --ignored --nocapture --test-threads 1
    /// ```
    fn gated_boot(hypervisor: &str) -> R<()> {
        use crate::structs::netpolicy::{merge_policies, InlinePolicy, NetPolicyObject};
        use crate::worker::netd::{Denials, NetPolicyCache};

        if std::env::var_os(microvm::GUEST_BINARY_ENV).is_none() {
            bail!(
                "set {} to the static legion-guest build: the test binary cannot serve as the \
                 guest agent",
                microvm::GUEST_BINARY_ENV
            );
        }
        let allowed =
            std::env::var("LEGION_GATED_TEST_ALLOW").unwrap_or_else(|_| "example.com".into());
        let denied =
            std::env::var("LEGION_GATED_TEST_DENY").unwrap_or_else(|_| "example.org".into());
        let image = std::env::var("LEGION_SANDBOX_TEST_IMAGE")
            .unwrap_or_else(|_| "alpine:latest".to_string());
        let script = format!(
            "wget -q -O /dev/null http://{allowed}/ && echo ALLOWED-OK; \
             wget -S -O /dev/null http://{denied}/ 2>&1 | grep -i 'legion-denied-by' && echo DENIED-OK; \
             nslookup {allowed} 127.0.0.1 >/dev/null 2>&1 && echo DNS-OK; \
             if nc -w 3 1.1.1.1 443 </dev/null >/dev/null 2>&1; then echo RAW-REACHED; else echo RAW-BLOCKED; fi"
        );
        let task = task_with(json!({
            "image": image,
            "hypervisor": hypervisor,
            "command": ["/bin/sh", "-c", script],
            "env": {
                "http_proxy": "http://127.0.0.1:1080",
                "HTTP_PROXY": "http://127.0.0.1:1080",
            },
        }));
        let allocation = Allocation {
            network: Some(crate::structs::AllocNetwork {
                mode: Some(crate::structs::netpolicy::MODE_GATED.to_string()),
                ..Default::default()
            }),
            ..alloc()
        };
        assert!(crate::task_plugins::allocation_is_gated(&allocation));

        let tmp = tempfile::tempdir()?;
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().join("allocs").join(allocation.id.to_string())),
            log_dir: Some(tmp.path().join("logs")),
            ..TaskInstance::default()
        };
        let scratch = work_root(&allocation, &instance);
        std::fs::create_dir_all(&scratch)?;

        // The node datapath for this one allocation: its policy, the egress
        // proxy on `<scratch>/vsock.sock_1080`, and the resolver on
        // `<scratch>/vsock.sock_53`.
        let mut object: NetPolicyObject = serde_json::from_value(json!({
            "name": "phase0", "enforcement": "require",
            "egress": { "protocol": "http", "allow": [format!("{allowed}/**")] },
        }))?;
        object.normalize_and_validate()?;
        let policy = merge_policies(&[object], InlinePolicy::default());
        let cache = Arc::new(NetPolicyCache::new());
        cache.apply_assignments(&[crate::structs::AllocNetPolicy {
            allocation_id: allocation.id,
            policy_hash: policy.hash.clone(),
        }]);
        cache.insert_body(&policy.hash.clone(), policy)?;
        let denials = Arc::new(Denials::new());
        let context = Arc::new(crate::worker::forwarder::ForwarderContext::new(
            cache,
            denials.clone(),
        ));
        let runtime = tokio::runtime::Runtime::new()?;
        let proxies = crate::worker::netd::proxy::EgressProxies::new(context.clone());
        let dns = crate::worker::netd::dns::NetDns::new(
            context,
            crate::worker::netd::dns::NetDns::host_forwarders(),
        );
        runtime.block_on(async {
            proxies
                .sync(&[crate::worker::netd::proxy::ProxySpec {
                    allocation_id: allocation.id,
                    attachment: crate::worker::netd::attach::Attachment::GuestVsock {
                        scratch: scratch.clone(),
                    },
                    listen: None,
                }])
                .await;
            dns.sync(&[crate::worker::netd::dns::DnsSpec {
                allocation_id: allocation.id,
                resolver: None,
                names: BTreeMap::new(),
                attachment: crate::worker::netd::attach::Attachment::GuestVsock {
                    scratch: scratch.clone(),
                },
            }])
            .await;
        });
        assert!(
            proxies.is_running(allocation.id),
            "the egress proxy is bound"
        );

        let plugin = SandboxTaskPlugin;
        plugin.prepare(&allocation, &task, &mut instance)?;
        plugin.start(&allocation, &task, &mut instance)?;
        let mut child = instance.child.take().expect("VMM child handle");
        let status = child.wait()?;
        let console =
            std::fs::read_to_string(instance.log_dir.as_ref().unwrap().join("stdout.log"))?;
        eprintln!("{console}");
        assert!(status.success(), "VMM exited cleanly: {status:?}");
        assert!(
            console.contains("guest agent started"),
            "the agent ran: {console}"
        );
        for marker in ["ALLOWED-OK", "DENIED-OK", "DNS-OK", "RAW-BLOCKED"] {
            assert!(console.contains(marker), "missing {marker}: {console}");
        }
        assert!(
            denials
                .drain()
                .iter()
                .any(|denial| denial.allocation_id == allocation.id),
            "the denied fetch was recorded against the allocation"
        );
        drop(runtime);
        Ok(())
    }

    #[test]
    #[ignore = "Phase 0: requires KVM + Firecracker 1.16.1+ + a vsock guest kernel + legion-guest"]
    fn sandbox_gated_boots_firecracker() -> R<()> {
        gated_boot("firecracker")
    }

    #[test]
    #[ignore = "Phase 0: requires KVM + cloud-hypervisor + a vsock guest kernel + legion-guest"]
    fn sandbox_gated_boots_cloud_hypervisor() -> R<()> {
        gated_boot("cloud-hypervisor")
    }

    /// Sandbox-exec plan Phase 1 on a real VMM: a TAP-less, ungated microVM
    /// (network `none`) takes a pipe session with its own exit status and a
    /// pty session sized as asked, while the guest still has no listener it
    /// could dial out through.
    ///
    /// ```text
    /// LEGION_SANDBOX_KERNEL=/var/lib/legion/kernels/vmlinux \
    /// LEGION_GUEST_BINARY=$PWD/target/x86_64-unknown-linux-musl/dist/legion-guest \
    /// cargo test --lib sandbox_exec_over_vsock -- --ignored --nocapture --test-threads 1
    /// ```
    fn exec_over_vsock(hypervisor: &str) -> R<()> {
        if std::env::var_os(microvm::GUEST_BINARY_ENV).is_none() {
            bail!(
                "set {} to the static legion-guest build: the test binary cannot serve as the \
                 guest agent",
                microvm::GUEST_BINARY_ENV
            );
        }
        let image = std::env::var("LEGION_SANDBOX_TEST_IMAGE")
            .unwrap_or_else(|_| "alpine:latest".to_string());
        let task = task_with(json!({
            "image": image,
            "hypervisor": hypervisor,
            "command": ["/bin/sh", "-c", "echo booted; sleep 600"],
            "env": { "GREETING": "from-the-task-env" },
        }));
        let allocation = alloc();
        let tmp = tempfile::tempdir()?;
        let mut instance = TaskInstance {
            alloc_dir: Some(tmp.path().join("allocs").join(allocation.id.to_string())),
            log_dir: Some(tmp.path().join("logs")),
            ..TaskInstance::default()
        };
        let plugin = SandboxTaskPlugin;
        plugin.prepare(&allocation, &task, &mut instance)?;
        plugin.start(&allocation, &task, &mut instance)?;
        let console = instance.log_dir.as_ref().unwrap().join("stdout.log");
        wait_for("the guest to boot", || {
            std::fs::read_to_string(&console).is_ok_and(|log| log.contains("booted"))
        })?;
        assert!(plugin.exec_support().exec, "a microVM takes exec");

        let opts = crate::task_plugins::ExecOptions::new(
            vec![
                "sh".into(),
                "-c".into(),
                "echo $GREETING; ls /proc/self/fd >/dev/null; exit 3".into(),
            ],
            false,
        );
        let mut session = plugin.exec(&allocation, &task, &mut instance, opts)?;
        let mut output = session.take_output()?;
        session.close_stdin();
        let mut text = String::new();
        output.stdout.read_to_string(&mut text)?;
        assert_eq!(session.wait()?, 3, "the process's own exit status");
        assert!(text.contains("from-the-task-env"), "{text}");

        let mut tty = crate::task_plugins::ExecOptions::new(
            vec!["sh".into(), "-c".into(), "stty size".into()],
            true,
        );
        tty.winsize = (120, 40);
        let mut session = plugin.exec(&allocation, &task, &mut instance, tty)?;
        let mut output = session.take_output()?;
        let mut text = String::new();
        output.stdout.read_to_string(&mut text)?;
        assert_eq!(session.wait()?, 0);
        assert!(text.contains("40 120"), "{text}");

        plugin.stop(&allocation, &task, &mut instance)?;
        Ok(())
    }

    #[test]
    #[ignore = "Sandbox exec Phase 1: requires KVM + Firecracker 1.17.0+ + a vsock guest kernel + legion-guest"]
    fn sandbox_exec_over_vsock_firecracker() -> R<()> {
        exec_over_vsock("firecracker")
    }

    #[test]
    #[ignore = "Sandbox exec Phase 1: requires KVM + cloud-hypervisor + a vsock guest kernel + legion-guest"]
    fn sandbox_exec_over_vsock_cloud_hypervisor() -> R<()> {
        exec_over_vsock("cloud-hypervisor")
    }

    /// The highest `counter=N` a guest has printed to its console so far.
    fn last_counter(log: &Path) -> Option<u64> {
        std::fs::read_to_string(log)
            .ok()?
            .lines()
            .filter_map(|line| line.trim().strip_prefix("counter="))
            .filter_map(|n| n.trim().parse().ok())
            .max()
    }

    fn first_counter(log: &Path) -> Option<u64> {
        std::fs::read_to_string(log)
            .ok()?
            .lines()
            .find_map(|line| line.trim().strip_prefix("counter=")?.trim().parse().ok())
    }

    fn wait_for(what: &str, mut check: impl FnMut() -> bool) -> R<()> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
        while !check() {
            if std::time::Instant::now() > deadline {
                bail!("timed out waiting for {what}");
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        Ok(())
    }

    /// A guest keeps a counter in a shell variable (RAM only, nothing on
    /// disk). Checkpoint it with `Stop`, restore into a new allocation, and
    /// the counter continues from where it was instead of starting over.
    fn memory_checkpoint_round_trip(hypervisor: &str) -> R<()> {
        let plugin = SandboxTaskPlugin;
        let image = std::env::var("LEGION_SANDBOX_TEST_IMAGE")
            .unwrap_or_else(|_| "alpine:latest".to_string());
        let task = task_with(json!({
            "image": image,
            "hypervisor": hypervisor,
            "command": ["/bin/sh", "-c",
                        "i=0; while true; do i=$((i+1)); echo counter=$i; sleep 0.2; done"],
        }));
        let tmp = tempfile::tempdir()?;
        let origin_alloc = alloc();
        let mut origin = TaskInstance {
            alloc_dir: Some(tmp.path().join("allocs/7")),
            log_dir: Some(tmp.path().join("logs7")),
            checkpoint_launch: true,
            ..TaskInstance::default()
        };
        let origin_log = tmp.path().join("logs7/stdout.log");
        plugin.prepare(&origin_alloc, &task, &mut origin)?;
        plugin.start(&origin_alloc, &task, &mut origin)?;
        wait_for("the guest to count to 10", || {
            last_counter(&origin_log).is_some_and(|n| n >= 10)
        })?;

        let out = CheckpointDir::create(tmp.path().join("ckpt"))?;
        let parts = plugin.checkpoint(
            &origin_alloc,
            &task,
            &mut origin,
            &out,
            AfterCheckpoint::Stop,
        )?;
        let before = last_counter(&origin_log).expect("counter before checkpoint");
        assert!(origin.child.is_none(), "Stop killed the VMM");

        let manifest = SnapshotManifest {
            api_version: crate::structs::checkpoint::MANIFEST_API_VERSION,
            id: "snap-kvm".into(),
            job_id: "job".into(),
            task: task.name.clone(),
            origin_instance: None,
            origin_alloc_id: origin_alloc.id,
            runtime: "sandbox".into(),
            kind: SnapshotKind::Memory,
            created_at: "2026-10-10T00:00:00Z".into(),
            volumes: Vec::new(),
            memory: parts.parts.clone(),
            compat: parts.compat.clone(),
            runtime_state: parts.runtime.clone(),
        };
        let restored_alloc = Allocation { id: 8, ..alloc() };
        let mut restored = TaskInstance {
            alloc_dir: Some(tmp.path().join("allocs/8")),
            log_dir: Some(tmp.path().join("logs8")),
            ..TaskInstance::default()
        };
        let restored_log = tmp.path().join("logs8/stdout.log");
        let identity = ResumeIdentity {
            alloc_id: 8,
            job_id: "job".into(),
            task_name: task.name.clone(),
            hostname: sandbox_name(&restored_alloc),
            ..ResumeIdentity::default()
        };
        plugin.prepare(&restored_alloc, &task, &mut restored)?;
        let started = std::time::Instant::now();
        plugin.restore(
            &restored_alloc,
            &task,
            &mut restored,
            &manifest,
            &out,
            &identity,
        )?;
        eprintln!("{hypervisor}: restore returned in {:?}", started.elapsed());
        wait_for("the restored guest to keep counting", || {
            last_counter(&restored_log).is_some_and(|n| n > before)
        })?;
        let resumed_at = first_counter(&restored_log).expect("counter after restore");
        assert!(
            resumed_at > before.saturating_sub(1) && resumed_at > 1,
            "the counter continued from RAM ({before} before, {resumed_at} first after) instead \
             of restarting"
        );

        plugin.destroy(&restored_alloc, &task, &mut restored)?;
        plugin.destroy(&origin_alloc, &task, &mut origin)?;
        Ok(())
    }

    /// `LEGION_SANDBOX_KERNEL=/path/to/vmlinux cargo test --lib \
    ///   sandbox_memory_checkpoint_round_trip -- --ignored --nocapture`
    #[test]
    #[ignore = "requires KVM + firecracker + a guest kernel"]
    fn sandbox_memory_checkpoint_round_trip_firecracker() -> R<()> {
        memory_checkpoint_round_trip("firecracker")
    }

    #[test]
    #[ignore = "requires KVM + cloud-hypervisor + a guest kernel"]
    fn sandbox_memory_checkpoint_round_trip_cloud_hypervisor() -> R<()> {
        memory_checkpoint_round_trip("cloud-hypervisor")
    }
    /// The last `<key>=<value>` a guest printed.
    fn last_value(log: &Path, key: &str) -> Option<String> {
        std::fs::read_to_string(log)
            .ok()?
            .lines()
            .filter_map(|line| {
                line.trim()
                    .strip_prefix(&format!("{key}="))
                    .map(str::to_string)
            })
            .next_back()
    }

    /// Phase 6 (plan §13): fork a running guest live into three new
    /// allocations. The origin keeps running, and the forks differ in
    /// hostname and in what they read from `/dev/urandom` (the resume
    /// handler reseeds from per-fork entropy; VMGenID helps where the VMM
    /// exposes it). Needs a rootfs whose init runs `legion guest-resume`.
    fn fork_divergence(hypervisor: &str) -> R<()> {
        let plugin = SandboxTaskPlugin;
        let image = std::env::var("LEGION_SANDBOX_TEST_IMAGE")
            .unwrap_or_else(|_| "alpine:latest".to_string());
        let task = task_with(json!({
            "image": image,
            "hypervisor": hypervisor,
            "command": ["/bin/sh", "-c",
                        "while true; do echo rand=$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \\n'); \
                         echo host=$(hostname); sleep 0.3; done"],
        }));
        let tmp = tempfile::tempdir()?;
        let origin_alloc = alloc();
        let mut origin = TaskInstance {
            alloc_dir: Some(tmp.path().join("allocs/7")),
            log_dir: Some(tmp.path().join("logs7")),
            checkpoint_launch: true,
            ..TaskInstance::default()
        };
        let origin_log = tmp.path().join("logs7/stdout.log");
        plugin.prepare(&origin_alloc, &task, &mut origin)?;
        plugin.start(&origin_alloc, &task, &mut origin)?;
        wait_for("the origin to print", || {
            last_value(&origin_log, "rand").is_some()
        })?;

        let out = CheckpointDir::create(tmp.path().join("ckpt"))?;
        let parts = plugin.checkpoint(
            &origin_alloc,
            &task,
            &mut origin,
            &out,
            AfterCheckpoint::Resume,
        )?;
        let manifest = SnapshotManifest {
            api_version: crate::structs::checkpoint::MANIFEST_API_VERSION,
            id: "snap-fork-kvm".into(),
            job_id: "job".into(),
            task: task.name.clone(),
            origin_instance: Some("run-17".into()),
            origin_alloc_id: origin_alloc.id,
            runtime: "sandbox".into(),
            kind: SnapshotKind::Memory,
            created_at: "2026-10-10T00:00:00Z".into(),
            volumes: Vec::new(),
            memory: parts.parts.clone(),
            compat: parts.compat.clone(),
            runtime_state: parts.runtime.clone(),
        };

        let mut forks = Vec::new();
        for (n, id) in [(1, 9), (2, 10), (3, 11)] {
            let fork_alloc = Allocation { id, ..alloc() };
            let mut fork = TaskInstance {
                alloc_dir: Some(tmp.path().join(format!("allocs/{id}"))),
                log_dir: Some(tmp.path().join(format!("logs{id}"))),
                ..TaskInstance::default()
            };
            let identity = ResumeIdentity {
                alloc_id: id,
                instance_id: Some(format!("run-17-f{n}")),
                job_id: "job".into(),
                task_name: task.name.clone(),
                hostname: sandbox_name(&fork_alloc),
                mac: Some(crate::structs::fork::fork_mac(id)),
                entropy: crate::task_plugins::checkpoint::fresh_entropy(),
                host_time_unix_ms: chrono::Utc::now().timestamp_millis(),
                fork: true,
                ..ResumeIdentity::default()
            };
            plugin.prepare(&fork_alloc, &task, &mut fork)?;
            plugin.restore(&fork_alloc, &task, &mut fork, &manifest, &out, &identity)?;
            forks.push((
                fork_alloc,
                fork,
                tmp.path().join(format!("logs{id}/stdout.log")),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for (fork_alloc, _, log) in &forks {
            let want = sandbox_name(fork_alloc);
            wait_for("the fork to take its own hostname", || {
                last_value(log, "host").as_deref() == Some(want.as_str())
            })?;
            let rand = last_value(log, "rand").expect("fork printed");
            assert!(
                seen.insert(rand.clone()),
                "two forks read the same RNG output {rand}"
            );
        }
        let before = std::fs::metadata(&origin_log)?.len();
        wait_for("the origin to keep running", || {
            std::fs::metadata(&origin_log)
                .map(|m| m.len() > before)
                .unwrap_or(false)
        })?;
        for (fork_alloc, mut fork, _) in forks {
            plugin.destroy(&fork_alloc, &task, &mut fork)?;
        }
        plugin.destroy(&origin_alloc, &task, &mut origin)?;
        Ok(())
    }

    /// `LEGION_SANDBOX_KERNEL=/path/to/vmlinux cargo test --lib \
    ///   sandbox_fork_divergence -- --ignored --nocapture`
    #[test]
    #[ignore = "requires KVM + firecracker + a guest kernel"]
    fn sandbox_fork_divergence_firecracker() -> R<()> {
        fork_divergence("firecracker")
    }

    #[test]
    #[ignore = "requires KVM + cloud-hypervisor + a guest kernel"]
    fn sandbox_fork_divergence_cloud_hypervisor() -> R<()> {
        fork_divergence("cloud-hypervisor")
    }

    /// A gated guest's agent relays each upstream on the loopback port its
    /// workload dials (the captured one after a restore) and on its VIP, to
    /// the allocation's local port, and a restored guest's env names both.
    /// A guest that is not gated relays nothing.
    #[test]
    fn a_gated_guest_relays_its_upstreams_on_the_ports_it_dials() {
        use legion_guest::identity::GuestUpstream;
        let network: crate::structs::AllocNetwork = serde_json::from_value(json!({
            "mode": "gated",
            "upstreams": [
                {
                    "key": "db", "localPort": 20003, "balance": "least_conn",
                    "vip": "127.42.0.2", "canonicalPort": 5432,
                },
                {
                    "key": "api", "localPort": 20004, "balance": "least_conn",
                    "to": "https://api.example.com",
                },
            ],
        }))
        .unwrap();
        let db = |port: u16, vsock_port: Option<u16>| GuestUpstream {
            port,
            vsock_port,
            vip: Some("127.42.0.2:5432".parse().unwrap()),
        };
        let api = GuestUpstream {
            port: 20004,
            vsock_port: None,
            vip: None,
        };
        assert_eq!(
            gated_guest_upstreams(Some(&network), &BTreeMap::new()),
            vec![db(20003, None), api]
        );
        // Restored: the workload keeps dialing 20011, which the agent now
        // relays to this allocation's local port, 20003.
        let captured: BTreeMap<String, u16> =
            [("db".to_string(), 20011), ("api".to_string(), 20004)].into();
        assert_eq!(
            gated_guest_upstreams(Some(&network), &captured),
            vec![api, db(20011, Some(20003))]
        );

        let allocation = Allocation {
            network: Some(network.clone()),
            ..alloc()
        };
        let env = restored_port_env(&allocation, &BTreeMap::new(), &captured);
        assert_eq!(env["LEGION_UPSTREAM_DB"], "127.0.0.1:20011");
        assert_eq!(env["LEGION_UPSTREAM_PORT_DB"], "20011");
        assert_eq!(env["LEGION_UPSTREAM_HOST_DB"], "127.0.0.1");
        assert_eq!(env["LEGION_UPSTREAM_API"], "127.0.0.1:20004");
        assert!(!env
            .keys()
            .any(|key| key.starts_with("LEGION_UPSTREAM_VIP_")));

        let host = crate::structs::AllocNetwork {
            mode: None,
            ..network
        };
        assert!(gated_guest_upstreams(Some(&host), &BTreeMap::new()).is_empty());
    }
}
