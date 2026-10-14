//! microVM mechanics for the `sandbox` task runtime.
//!
//! This module owns everything between "an OCI image layout on disk" and "a
//! booted microVM running the task's command":
//!
//! - [`Backend`] — which VMM (or dev-mode backend) this node can use.
//! - [`VmSpec`] + [`cloud_hypervisor_args`] / [`firecracker_config`] — the
//!   launch surface for each VMM. Both are single-process VMMs, so the guest
//!   dies with the child Legion spawns (no daemon to chase, unlike containers).
//! - [`GuestPayload`] + [`install_guest_payload`] — the generated `/legion/init`
//!   that becomes PID 1 in the guest: mounts the pseudo filesystems and the
//!   attached volume disks, exports the task env, runs the command, then powers
//!   the VM off.
//! - ext4 disk-image helpers — the ephemeral root disk, rebuilt from the image.
//!   Persistent disks are NOT built here: those are block-mode volumes served
//!   by a volume driver (see docs/volumes.md), and this module only attaches
//!   what the worker already mounted.
//!
//! Everything here is deliberately pure-ish (build a string, build a spec,
//! write a file) so the launch surface is unit-testable without a hypervisor.

use crate::prelude::*;
pub(crate) use crate::util::find_on_path;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Selects the hypervisor without touching the jobspec (`auto`,
/// `cloud-hypervisor`, `firecracker`, `dev`). The task's `hypervisor` field
/// wins over this.
pub(crate) const HYPERVISOR_ENV: &str = "LEGION_SANDBOX_HYPERVISOR";
/// Guest kernel used when the task sets no `kernel`.
pub(crate) const KERNEL_ENV: &str = "LEGION_SANDBOX_KERNEL";
/// Overrides for the VMM binaries, for nodes that install them off-PATH.
const CLOUD_HYPERVISOR_BIN_ENV: &str = "LEGION_CLOUD_HYPERVISOR_BIN";
const FIRECRACKER_BIN_ENV: &str = "LEGION_FIRECRACKER_BIN";
// Linux dev mode reuses the container runtime's OCI runtime selection, so a
// node's `LEGION_OCI_RUNTIME` (and the default) means the same for both.
use crate::task_plugins::container::{DEFAULT_OCI_RUNTIME, OCI_RUNTIME_ENV};
/// Overrides the `mkfs.ext4` binary (some distros keep it out of a
/// non-root PATH).
const MKFS_ENV: &str = "LEGION_MKFS_EXT4";

/// Kernel image looked up when neither the task nor [`KERNEL_ENV`] names one.
const DEFAULT_KERNEL_PATH: &str = "/var/lib/legion/kernels/vmlinux";

const KVM_DEVICE: &str = "/dev/kvm";

/// Where the generated init, env file, and staged artifacts land *inside* the
/// guest. `/legion` is Legion's namespace in the guest filesystem; images do
/// not use it.
pub(crate) const GUEST_INIT_PATH: &str = "/legion/init";
pub(crate) const GUEST_ENV_PATH: &str = "/legion/env";
/// The guest binary (the static `legion-guest`, or the worker's own binary
/// as a fallback), copied in for the guest agent and the resume handler.
pub(crate) const GUEST_AGENT_PATH: &str = "/legion/legion";

/// Names the static guest binary, overriding the default: `legion-guest`
/// beside the worker's own executable.
pub(crate) const GUEST_BINARY_ENV: &str = "LEGION_GUEST_BINARY";
/// The static guest binary's file name, beside the worker.
const GUEST_BINARY_NAME: &str = "legion-guest";
pub(crate) const GUEST_ARTIFACT_DIR: &str = "/legion/artifacts";

/// Guest block devices, in attach order: root disk first, then every attached
/// volume disk in [`VmSpec::volume_disks`] order.
const GUEST_ROOT_DEVICE: &str = "/dev/vda";

/// The guest device name of the volume disk at `index` in attach order
/// (`/dev/vdb`, `/dev/vdc`, …). Both VMMs enumerate virtio-blk devices in the
/// order they were attached, which is what makes this deterministic.
pub(crate) fn guest_volume_device(index: usize) -> String {
    // 25 disks ought to be enough for anybody; past `vdz` the naming scheme
    // changes (`vdaa`), and a sandbox attaching 26 volumes has other problems.
    let letter = char::from(b'b' + u8::try_from(index.min(24)).expect("bounded index"));
    format!("/dev/vd{letter}")
}

/// A microVM disk smaller than this is not worth formatting (mkfs needs room
/// for its own metadata).
const MIN_IMAGE_MIB: u64 = 32;
const MIB: u64 = 1024 * 1024;

/// The VMM (or dev-mode backend) a node runs sandboxes with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Backend {
    /// Linux + KVM. Legion's default: richer device model than Firecracker
    /// (PCI, hotplug) while staying a single-process VMM.
    CloudHypervisor { binary: String },
    /// Linux + KVM. The minimal alternative; identical contract here.
    Firecracker { binary: String },
    /// **macOS dev mode.** Apple `container` runs each container inside its own
    /// lightweight VM (Virtualization.framework), so the isolation boundary is
    /// still a VM — but the kernel, root disk, and state device are Apple's,
    /// not Legion's. Sizes, kernel selection, and the generated guest init do
    /// not apply.
    AppleContainer,
    /// **Linux dev mode without KVM** (CI containers, nested-virt-less VMs):
    /// an OCI runtime bundle. Namespace isolation only — never auto-selected,
    /// only reachable by asking for `hypervisor: 'dev'`.
    OciRuntime { binary: String },
}

impl Backend {
    /// Resolves `requested` (the task's `hypervisor` field, else
    /// [`HYPERVISOR_ENV`], else `auto`) against what this node can actually do.
    pub(crate) fn detect(requested: Option<&str>) -> R<Self> {
        let selection = requested
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| {
                std::env::var(HYPERVISOR_ENV)
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or_else(|| "auto".to_string());

        match selection.to_ascii_lowercase().as_str() {
            "auto" => Self::detect_auto(),
            "cloud-hypervisor" | "cloudhypervisor" | "ch" => Self::cloud_hypervisor(),
            "firecracker" | "fc" => Self::firecracker(),
            "dev" => Self::dev(),
            other => bail!(
                "unknown sandbox hypervisor `{other}`; expected auto, cloud-hypervisor, \
                 firecracker, or dev"
            ),
        }
    }

    /// macOS gets the dev-mode VM backend (nothing else runs there); Linux
    /// requires KVM and prefers cloud-hypervisor when both VMMs are installed.
    fn detect_auto() -> R<Self> {
        if cfg!(target_os = "macos") {
            return Self::apple_container();
        }
        if !cfg!(target_os = "linux") {
            bail!("sandbox tasks are not supported on this OS");
        }
        require_kvm()?;
        if let Some(binary) = vmm_binary(CLOUD_HYPERVISOR_BIN_ENV, "cloud-hypervisor") {
            return Ok(Self::CloudHypervisor { binary });
        }
        if let Some(binary) = vmm_binary(FIRECRACKER_BIN_ENV, "firecracker") {
            return Ok(Self::Firecracker { binary });
        }
        bail!(
            "no microVM monitor found: install `cloud-hypervisor` or `firecracker` on PATH \
             (or set {CLOUD_HYPERVISOR_BIN_ENV}/{FIRECRACKER_BIN_ENV}). To run without a VM on \
             this node, set the task's `hypervisor` to \"dev\" — namespace isolation only."
        );
    }

    fn cloud_hypervisor() -> R<Self> {
        require_linux("cloud-hypervisor")?;
        require_kvm()?;
        let binary =
            vmm_binary(CLOUD_HYPERVISOR_BIN_ENV, "cloud-hypervisor").with_context(|| {
                format!("cloud-hypervisor not found on PATH (set {CLOUD_HYPERVISOR_BIN_ENV})")
            })?;
        Ok(Self::CloudHypervisor { binary })
    }

    fn firecracker() -> R<Self> {
        require_linux("firecracker")?;
        require_kvm()?;
        let binary = vmm_binary(FIRECRACKER_BIN_ENV, "firecracker").with_context(|| {
            format!("firecracker not found on PATH (set {FIRECRACKER_BIN_ENV})")
        })?;
        Ok(Self::Firecracker { binary })
    }

    fn apple_container() -> R<Self> {
        if !cfg!(target_os = "macos") {
            bail!("Apple `container` dev mode is macOS-only");
        }
        Ok(Self::AppleContainer)
    }

    /// Dev mode is per-OS: macOS gets Apple `container` (still a VM), Linux
    /// gets an OCI runtime (namespaces only).
    fn dev() -> R<Self> {
        if cfg!(target_os = "macos") {
            return Self::apple_container();
        }
        if !cfg!(target_os = "linux") {
            bail!("sandbox dev mode is only available on macOS and Linux");
        }
        let binary = std::env::var(OCI_RUNTIME_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_OCI_RUNTIME.to_string());
        Ok(Self::OciRuntime { binary })
    }

    /// The value this backend reports as the `sandbox.vmm` node attribute
    /// and records in a checkpoint's `compat.vmm.type`.
    pub(crate) fn vmm_name(&self) -> &'static str {
        match self {
            Self::CloudHypervisor { .. } => "cloud-hypervisor",
            Self::Firecracker { .. } => "firecracker",
            Self::AppleContainer => "apple-container",
            Self::OciRuntime { .. } => "oci-runtime",
        }
    }

    /// The VMM binary, for the real microVM backends.
    pub(crate) fn binary(&self) -> Option<&str> {
        match self {
            Self::CloudHypervisor { binary } | Self::Firecracker { binary } => Some(binary),
            Self::AppleContainer | Self::OciRuntime { .. } => None,
        }
    }

    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::CloudHypervisor { .. } => "cloud-hypervisor",
            Self::Firecracker { .. } => "firecracker",
            Self::AppleContainer => "apple-container (dev)",
            Self::OciRuntime { .. } => "oci-runtime (dev)",
        }
    }

    /// True for the real VMM paths: the ones that boot a Legion-built kernel +
    /// root disk and therefore use everything else in this module.
    pub(crate) fn is_microvm(&self) -> bool {
        matches!(
            self,
            Self::CloudHypervisor { .. } | Self::Firecracker { .. }
        )
    }

    /// Whether a live exec session can be opened into a running sandbox.
    /// The VMM paths reach the guest agent's exec listener over vsock
    /// (sandbox-exec plan §6.1); Apple `container` exposes `container exec`.
    /// The Linux dev path (an OCI runtime) has neither.
    pub(crate) fn supports_exec(&self) -> bool {
        matches!(
            self,
            Self::AppleContainer | Self::CloudHypervisor { .. } | Self::Firecracker { .. }
        )
    }
}

fn require_linux(what: &str) -> R<()> {
    if cfg!(target_os = "linux") {
        return Ok(());
    }
    bail!("{what} only runs on Linux; use `hypervisor: \"dev\"` on this OS");
}

fn require_kvm() -> R<()> {
    if Path::new(KVM_DEVICE).exists() {
        return Ok(());
    }
    bail!(
        "{KVM_DEVICE} is missing: microVM sandboxes need KVM (enable nested virtualization, or \
         set the task's `hypervisor` to \"dev\" for a non-VM fallback)"
    );
}

/// Every microVM monitor installed on this node, whether or not KVM is
/// usable. The fingerprint reports a version for each, so a memory checkpoint
/// taken on the non-default VMM can still find a node that runs it.
pub(crate) fn installed_vmms() -> Vec<Backend> {
    let mut found = Vec::new();
    if !cfg!(target_os = "linux") {
        return found;
    }
    if let Some(binary) = vmm_binary(CLOUD_HYPERVISOR_BIN_ENV, "cloud-hypervisor") {
        found.push(Backend::CloudHypervisor { binary });
    }
    if let Some(binary) = vmm_binary(FIRECRACKER_BIN_ENV, "firecracker") {
        found.push(Backend::Firecracker { binary });
    }
    found
}

/// The version a VMM binary reports for `--version`, normalized to bare
/// `X.Y.Z` (Firecracker prints `Firecracker v1.7.0`, cloud-hypervisor
/// `cloud-hypervisor v51.2`). `None` when the binary cannot be run.
pub(crate) fn vmm_version(binary: &str) -> Option<String> {
    let output = Command::new(binary).arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    parse_vmm_version(&String::from_utf8_lossy(&output.stdout))
}

/// The first whitespace-separated token that looks like a version, in
/// canonical form ([`canonical_vmm_version`]). The node fingerprint (from
/// the `--version` banner) and a checkpoint's `compat.vmm.version` (from
/// the VMM API) both go through here, so one binary records one string.
///
/// [`canonical_vmm_version`]: crate::structs::checkpoint::canonical_vmm_version
pub(crate) fn parse_vmm_version(text: &str) -> Option<String> {
    text.split_whitespace()
        .map(|token| token.trim_start_matches('v'))
        .find(|token| {
            token.chars().next().is_some_and(|ch| ch.is_ascii_digit()) && token.contains('.')
        })
        .map(|token| {
            crate::structs::checkpoint::canonical_vmm_version(
                token.trim_end_matches(|ch: char| !ch.is_ascii_alphanumeric()),
            )
        })
}

/// The configured binary if set, else the name resolved on `PATH`.
fn vmm_binary(env_var: &str, default_name: &str) -> Option<String> {
    if let Some(configured) = std::env::var(env_var)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return Some(configured);
    }
    find_on_path(default_name).map(|path| path.display().to_string())
}

/// Everything a VMM needs to boot one sandbox.
#[derive(Debug, Clone)]
pub(crate) struct VmSpec {
    /// Per-allocation VM name (also the guest hostname).
    pub name: String,
    pub kernel: PathBuf,
    /// Appended to Legion's base kernel cmdline.
    pub extra_cmdline: Option<String>,
    /// Ephemeral root disk (`/dev/vda`), rebuilt from the image.
    pub root_disk: PathBuf,
    /// Block-mode volumes attached after the root disk, in order (`/dev/vdb`,
    /// `/dev/vdc`, …). The images are served by volume drivers; this spec only
    /// points the VMM at them.
    pub volume_disks: Vec<VmDisk>,
    pub vcpus: u32,
    pub memory_mib: u32,
    pub tap: Option<TapNet>,
    /// cloud-hypervisor control socket (`ch-remote` attaches to it).
    pub api_socket: PathBuf,
    /// A virtio-vsock device: the gated sandbox's only way out. The host
    /// end is Legion's datapath listening on `<socket>_<port>`
    /// (docs/network-handlers.md, "Sandboxes").
    pub vsock: Option<VsockSpec>,
    /// The identity disk of a memory-checkpointable sandbox, attached after
    /// the volume disks as drive `identity`. Its guest device is passed on
    /// the kernel cmdline as `legion.identity=`, so the guest agent finds it
    /// without probing.
    pub identity_disk: Option<PathBuf>,
    /// A virtio-balloon device, the memory pressure response's `reclaim`
    /// lever for a guest (docs/resource-enforcement.md, "Memory pressure").
    /// Configured only for sandboxes that can be memory-checkpointed or that
    /// set `resources.memoryMax`: the ones that run above a reservation.
    pub balloon: Option<BalloonSpec>,
}

/// The balloon device a guest boots with. It starts deflated (the guest
/// sees all of its memory) and `deflate_on_oom` lets the guest take memory
/// back before its own OOM killer would act, so inflating it can slow a
/// guest down but never kill it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BalloonSpec {
    /// Free page reporting: the guest tells the host about pages it freed,
    /// so the host reclaims them with no balloon target at all. Needs
    /// `CONFIG_PAGE_REPORTING` in the guest kernel, and is inert without it.
    /// cloud-hypervisor only: not every Firecracker release Legion supports
    /// accepts the field, and an unknown config field fails the boot.
    pub free_page_reporting: bool,
}

impl VmSpec {
    /// Guest device of the identity disk: the next virtio-blk slot after the
    /// volume disks.
    pub(crate) fn identity_device(&self) -> Option<String> {
        self.identity_disk
            .as_ref()
            .map(|_| guest_volume_device(self.volume_disks.len()))
    }
}

/// The vsock device of a gated sandbox: the host-side Unix socket base both
/// VMMs multiplex guest ports onto (`<socket>_<port>`). The guest's context
/// id is always [`GATED_GUEST_CID`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VsockSpec {
    pub socket: PathBuf,
}

/// Every gated guest's vsock context id (0 to 2 are reserved). Both VMMs
/// implement vsock in user space over Unix sockets, so a CID is visible only
/// inside its own guest and needs no host-wide uniqueness, and the guest
/// agent dials the host (CID 2) without ever reading its own. Firecracker
/// cannot change a CID on snapshot load, so a per-allocation CID would be
/// stale after every restore and shared by every fork anyway (gated plan
/// §6.2).
pub(crate) const GATED_GUEST_CID: u32 = 3;

/// The longest path a Unix socket can bind: `sun_path` holds 108 bytes,
/// one of them the terminating NUL.
pub(crate) const MAX_UNIX_SOCKET_PATH: usize = 107;

/// Refuse a gated sandbox whose datapath sockets cannot exist: the longest
/// is `<base>_1080`, and a path over [`MAX_UNIX_SOCKET_PATH`] bytes fails
/// inside the VMM or the worker's listener with an opaque error. Checked at
/// launch and at restore, where the base is the allocation's own.
pub(crate) fn check_vsock_socket_path(base: &Path) -> R<()> {
    let longest = crate::worker::netd::guest_agent::host_socket_path(
        base,
        crate::worker::netd::guest_agent::PROXY_PORT,
    );
    let bytes = longest.as_os_str().len();
    if bytes > MAX_UNIX_SOCKET_PATH {
        bail!(
            "gated sandbox socket path {} is {bytes} bytes, over the {MAX_UNIX_SOCKET_PATH}-byte \
             Unix socket limit: shorten the worker's state path",
            longest.display()
        );
    }
    Ok(())
}

/// One block device handed to the guest: the host path of a raw device or
/// disk image (a block-mode volume's `device_path`) plus its access mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VmDisk {
    pub path: PathBuf,
    pub read_only: bool,
}

/// Firecracker's id for the guest NIC. A fork's restore names it to move
/// the NIC to another TAP device (`network_overrides`).
pub(crate) const GUEST_NIC_ID: &str = "eth0";

/// A pre-created host TAP device handed to the guest as its NIC. Legion does
/// not create TAP devices (that needs CAP_NET_ADMIN and a host network plan);
/// the operator or a network plugin does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TapNet {
    pub device: String,
    pub mac: Option<String>,
}

/// Legion's guest kernel cmdline: the console (so guest output lands in the
/// allocation's captured stdout), reboot-on-panic (so a wedged guest exits the
/// VMM instead of hanging), the root disk, and our generated init.
///
/// The console is where boot time goes. Every byte written to an emulated
/// 16550 serial port is a VM exit, which is costly under nested
/// virtualization: a default kernel's ~300 boot lines cost about 700 ms of a
/// 4-vCPU c8i guest's boot, more than the rest of the boot together.
///
/// - cloud-hypervisor boots on virtio-console (`hvc0`), which batches output
///   through a virtqueue. The kernel keeps its full boot log, and messages
///   from before the console driver loads are replayed when it registers.
/// - Firecracker has only the serial port, so its guests boot `quiet`:
///   warnings, errors, and panics still print. A task that wants the full
///   log sets `kernel_args = "loglevel=7"`, which overrides it.
///
/// Firecracker has no PCI bus and no i8042 controller, so it needs the extra
/// probe suppressions; cloud-hypervisor does not.
pub(crate) fn kernel_cmdline(spec: &VmSpec, firecracker: bool) -> String {
    let mut cmdline = if firecracker {
        String::from(
            "console=ttyS0 quiet reboot=k panic=1 pci=off i8042.noaux i8042.nomux i8042.nopnp \
             i8042.dumbkbd",
        )
    } else {
        String::from("console=hvc0 reboot=k panic=1")
    };
    cmdline.push_str(&format!(
        " root={GUEST_ROOT_DEVICE} rw init={GUEST_INIT_PATH}"
    ));
    if let Some(device) = spec.identity_device() {
        cmdline.push_str(&format!(" legion.identity={device}"));
    }
    if let Some(extra) = spec
        .extra_cmdline
        .as_deref()
        .map(str::trim)
        .filter(|extra| !extra.is_empty())
    {
        cmdline.push(' ');
        cmdline.push_str(extra);
    }
    cmdline
}

/// `cloud-hypervisor` argv. `--serial off --console tty` puts the guest's
/// virtio-console (`hvc0`, see [`kernel_cmdline`]) on the VMM's stdout, which
/// the worker captures into the allocation's `stdout.log`.
///
/// Every disk is declared `image_type=raw`. Every disk Legion attaches is
/// raw (an ext4 image, a raw volume, or the identity disk), and without the
/// declaration cloud-hypervisor probes the format and then refuses guest
/// writes to sector 0 of the probed image, so a guest cannot rewrite it into
/// another format. ext4 writes its superblock through block 0 at mount, so
/// the root mount failed with an I/O error, the kernel panicked, and the
/// guest rebooted after `panic=1`'s one second: every boot paid about 1.5 s
/// for it.
pub(crate) fn cloud_hypervisor_args(spec: &VmSpec) -> Vec<String> {
    let mut args = vec![
        "--kernel".to_string(),
        spec.kernel.display().to_string(),
        "--cmdline".to_string(),
        kernel_cmdline(spec, false),
        "--disk".to_string(),
        format!("path={},image_type=raw", spec.root_disk.display()),
    ];
    for disk in &spec.volume_disks {
        let mut arg = format!("path={},image_type=raw", disk.path.display());
        if disk.read_only {
            arg.push_str(",readonly=on");
        }
        args.push(arg);
    }
    if let Some(identity) = spec.identity_disk.as_ref() {
        args.push(format!(
            "path={},image_type=raw,readonly=on,id={}",
            identity.display(),
            crate::task_plugins::IDENTITY_DRIVE_ID
        ));
    }
    args.push("--cpus".to_string());
    args.push(format!("boot={}", spec.vcpus));
    args.push("--memory".to_string());
    args.push(format!("size={}M", spec.memory_mib));
    args.push("--serial".to_string());
    args.push("off".to_string());
    args.push("--console".to_string());
    args.push("tty".to_string());
    args.push("--api-socket".to_string());
    args.push(spec.api_socket.display().to_string());
    if let Some(balloon) = spec.balloon.as_ref() {
        args.push("--balloon".to_string());
        args.push(format!(
            "size=0,deflate_on_oom=on{}",
            if balloon.free_page_reporting {
                ",free_page_reporting=on"
            } else {
                ""
            }
        ));
    }
    if let Some(tap) = spec.tap.as_ref() {
        let mut net = format!("tap={}", tap.device);
        if let Some(mac) = tap.mac.as_deref() {
            net.push_str(&format!(",mac={mac}"));
        }
        args.push("--net".to_string());
        args.push(net);
    }
    if let Some(vsock) = spec.vsock.as_ref() {
        args.push("--vsock".to_string());
        args.push(format!(
            "cid={GATED_GUEST_CID},socket={}",
            vsock.socket.display()
        ));
    }
    args
}

/// Firecracker VM config (`firecracker --no-api --config-file <path>`), which
/// boots straight from JSON with no API dance. Firecracker also writes the
/// guest serial console to its own stdout.
pub(crate) fn firecracker_config(spec: &VmSpec) -> Value {
    let mut drives = vec![json!({
        "drive_id": "rootfs",
        "path_on_host": spec.root_disk.display().to_string(),
        "is_root_device": true,
        "is_read_only": false,
    })];
    for (index, disk) in spec.volume_disks.iter().enumerate() {
        drives.push(json!({
            "drive_id": format!("volume{index}"),
            "path_on_host": disk.path.display().to_string(),
            "is_root_device": false,
            "is_read_only": disk.read_only,
        }));
    }
    if let Some(identity) = spec.identity_disk.as_ref() {
        drives.push(json!({
            "drive_id": crate::task_plugins::IDENTITY_DRIVE_ID,
            "path_on_host": identity.display().to_string(),
            "is_root_device": false,
            "is_read_only": true,
        }));
    }

    let mut config = json!({
        "boot-source": {
            "kernel_image_path": spec.kernel.display().to_string(),
            "boot_args": kernel_cmdline(spec, true),
        },
        "drives": drives,
        "machine-config": {
            "vcpu_count": spec.vcpus,
            "mem_size_mib": spec.memory_mib,
        },
    });
    if let Some(tap) = spec.tap.as_ref() {
        let mut iface = json!({
            "iface_id": GUEST_NIC_ID,
            "host_dev_name": tap.device,
        });
        if let Some(mac) = tap.mac.as_deref() {
            iface["guest_mac"] = json!(mac);
        }
        config["network-interfaces"] = json!([iface]);
    }
    if let Some(vsock) = spec.vsock.as_ref() {
        config["vsock"] = json!({
            "guest_cid": GATED_GUEST_CID,
            "uds_path": vsock.socket.display().to_string(),
        });
    }
    if spec.balloon.is_some() {
        // `free_page_reporting` is deliberately not sent (see BalloonSpec).
        config["balloon"] = json!({
            "amount_mib": 0,
            "deflate_on_oom": true,
            "stats_polling_interval_s": 0,
        });
    }
    config
}

/// How far to inflate a balloon to take back `request_bytes` more: from
/// `current_mib`, never past `guest_mib - reserved_mib`, so a guest is never
/// squeezed below the memory it reserved. Returns the new target in MiB.
pub(crate) fn balloon_target_mib(
    current_mib: u64,
    request_bytes: u64,
    guest_mib: u64,
    reserved_mib: u64,
) -> u64 {
    let ceiling = guest_mib.saturating_sub(reserved_mib);
    let request_mib = request_bytes.div_ceil(1024 * 1024);
    current_mib
        .saturating_add(request_mib)
        .min(ceiling.max(current_mib))
}

/// What the generated guest init runs.
#[derive(Debug, Clone, Default)]
pub(crate) struct GuestPayload {
    /// argv of the sandbox command (run by init, not `exec`'d, so init can
    /// power the VM off afterwards).
    pub command: Vec<String>,
    /// Env exported before the command runs (image env merged with task env).
    pub env: BTreeMap<String, String>,
    /// Working directory inside the guest.
    pub cwd: Option<String>,
    /// Volume disks init mounts before the command runs, in
    /// [`VmSpec::volume_disks`] attach order.
    pub disk_mounts: Vec<GuestDiskMount>,
    /// Init starts `/legion/legion guest-agent` (the guest binary, copied
    /// into the rootfs) before the command. Every microVM gets it, for its
    /// exec listener on vsock port 1100 (sandbox-exec plan §6.1).
    pub guest_agent: bool,
    /// A gated sandbox: the agent also relays the guest's loopback proxy and
    /// DNS ports to the node, and the resolver is the agent on loopback.
    /// Off, the agent runs `--no-relay`: the guest still cannot dial the
    /// host, and the vsock device carries host-initiated exec only.
    pub relay: bool,
    /// A gated sandbox's advertised ports, passed to the agent as
    /// `--ingress-port`: it listens on each as a vsock port and relays the
    /// node datapath's connections to the workload on the same port.
    pub ingress_ports: Vec<u16>,
    /// A gated sandbox's upstreams, passed to the agent as `--upstream
    /// PORT[=VIP:PORT]`: it listens on each loopback port (and VIP) and
    /// relays to the node datapath's vsock socket for the port.
    pub upstreams: Vec<legion_guest::identity::GuestUpstream>,
    /// A sandbox launched for memory checkpoints (or with token files):
    /// init starts the resume handler in the background, which watches the
    /// identity disk and refreshes the guest after every restore (new
    /// allocation id and token, reseeded RNG, resynced clock, `onResume`).
    /// On a gated sandbox the agent runs it (`guest-agent --resume-watch`),
    /// so a restore closes the agent's dead relays in the same process.
    pub resume_watch: bool,
    /// The binary copied in as `/legion/legion` when the guest needs the
    /// agent or the resume handler. `None` resolves it at install
    /// ([`guest_binary`]).
    pub guest_binary: Option<GuestBinary>,
    /// A TAP guest's resolvers, from its bridge's pool (`sandbox.bridges.<name>.dns`),
    /// written to `/etc/resolv.conf`. Empty leaves the image's own file.
    pub resolvers: Vec<std::net::Ipv4Addr>,
}

/// One volume disk mount inside the guest. A volume with no declared `path`
/// is attached but not mounted — the workload sees the raw device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GuestDiskMount {
    /// Guest device ([`guest_volume_device`] of the disk's attach index).
    pub device: String,
    /// Guest path to mount at, or `None` for attach-only (raw device access).
    pub path: Option<String>,
    /// The filesystem the volume driver formatted the device with.
    pub fs: String,
    pub read_only: bool,
}

/// `/legion/env`'s format and quoting are shared with the guest's resume
/// handler, which writes `/legion/resume.env` the same way.
pub(crate) use legion_guest::env::{render_env_file, sh_quote};

/// `/legion/init` — PID 1 in the guest.
///
/// A microVM boots a kernel, not a container runtime: nothing mounts `/proc`,
/// nothing exports env, nothing mounts the volume disks, and nothing shuts the
/// machine down when the workload is finished. This script is that missing
/// layer, and it is deliberately plain POSIX shell so it runs unmodified on
/// whatever `/bin/sh` the image ships (busybox, dash, bash).
pub(crate) fn render_init_script(payload: &GuestPayload) -> String {
    let mut script = String::new();
    script.push_str("#!/bin/sh\n");
    script.push_str("# Generated by Legion for the `sandbox` task runtime. PID 1 in the guest.\n");
    // The kernel hands init no environment at all, so PATH comes first —
    // every command below depends on it. /legion/env may override it.
    script.push_str("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n");
    script.push_str("export PATH\n\n");
    script.push_str("mount -t proc proc /proc 2>/dev/null\n");
    script.push_str("mount -t sysfs sysfs /sys 2>/dev/null\n");
    script.push_str("mount -t devtmpfs devtmpfs /dev 2>/dev/null\n");
    script.push_str("mkdir -p /dev/pts /dev/shm 2>/dev/null\n");
    script.push_str("mount -t devpts devpts /dev/pts 2>/dev/null\n");
    script.push_str("mount -t tmpfs tmpfs /dev/shm 2>/dev/null\n");
    script.push_str("mount -t tmpfs tmpfs /tmp 2>/dev/null\n");
    script.push_str("hostname -F /etc/hostname 2>/dev/null\n");
    // Loopback up so the workload can talk to itself even with no NIC.
    script.push_str("ip link set lo up 2>/dev/null || ifconfig lo up 2>/dev/null\n\n");

    // (index, quoted path) of every volume init mounts, for the unmount at
    // shutdown. `legion_vol_<index>=1` records that the mount succeeded.
    let mut mounted: Vec<(usize, String)> = Vec::new();
    for (index, mount) in payload.disk_mounts.iter().enumerate() {
        let Some(path) = mount.path.as_deref() else {
            // Attach-only: the workload consumes the raw device.
            continue;
        };
        let quoted = sh_quote(path);
        let device = &mount.device;
        let fs = &mount.fs;
        let options = if mount.read_only { " -o ro" } else { "" };
        script.push_str("# Volume disk: its data belongs to the volume, not this boot.\n");
        script.push_str(&format!("mkdir -p {quoted} 2>/dev/null\n"));
        script.push_str(&format!(
            "if mount -t {fs}{options} {device} {quoted}; then\n\
             \x20   echo \"legion: mounted volume {device} at {path}\"\n\
             \x20   legion_vol_{index}=1\n\
             else\n\
             \x20   echo \"legion: FAILED to mount volume {device} at {path}\" >&2\n\
             fi\n\n"
        ));
        mounted.push((index, quoted));
    }

    if payload.guest_agent {
        // The agent takes exec sessions on vsock port 1100. On a gated guest
        // it is also the datapath's foothold: it relays 127.0.0.1:1080 and
        // :53 to the host over vsock, and the host's ingress connections in
        // to the workload. Without it a gated guest has loopback and nothing
        // else, which is the fail-closed outcome the mode promises. When the
        // guest also needs the resume watch, the agent runs it.
        let mut flags = String::new();
        if !payload.relay {
            flags.push_str(" --no-relay");
        }
        // Each advertised port: the agent listens on it as a vsock port and
        // relays the datapath's connections to the workload's loopback port.
        for port in &payload.ingress_ports {
            flags.push_str(&format!(" --ingress-port {port}"));
        }
        // Each upstream: the agent listens on its loopback port and VIP and
        // relays to the datapath's vsock socket for the port. The argument
        // is digits, dots, a colon, `@`, and `=`, so it needs no quoting.
        for upstream in &payload.upstreams {
            flags.push_str(&format!(" --upstream {}", upstream.to_arg()));
        }
        if payload.resume_watch {
            flags.push_str(" --resume-watch");
        }
        let (started, missing) = if payload.relay {
            (
                "gated network and exec over vsock",
                "the sandbox has no network and takes no exec sessions",
            )
        } else {
            ("exec over vsock", "the sandbox takes no exec sessions")
        };
        script.push_str(&format!(
            "if [ -x {GUEST_AGENT_PATH} ]; then\n\
             \x20   {GUEST_AGENT_PATH} guest-agent{flags} >/dev/console 2>&1 &\n\
             \x20   echo \"legion: guest agent started ({started})\"\n\
             else\n\
             \x20   echo \"legion: guest agent missing at {GUEST_AGENT_PATH}; {missing}\" >&2\n\
             fi\n\n"
        ));
    }
    if payload.resume_watch && !payload.guest_agent {
        // Restores happen with the guest frozen mid-instruction, so nothing
        // in the guest runs on its own afterward. This watcher notices the
        // identity disk change and refreshes what a restore made stale.
        script.push_str(&format!(
            "if [ -x {GUEST_AGENT_PATH} ]; then\n\
             \x20   {GUEST_AGENT_PATH} guest-resume >/dev/console 2>&1 &\n\
             else\n\
             \x20   echo \"legion: resume handler missing at {GUEST_AGENT_PATH}; a restored guest keeps its old identity\" >&2\n\
             fi\n\n"
        ));
    }
    script.push_str(&format!(
        "if [ -r {GUEST_ENV_PATH} ]; then . {GUEST_ENV_PATH}; fi\n\n"
    ));
    if let Some(cwd) = payload.cwd.as_deref().filter(|cwd| !cwd.is_empty()) {
        let quoted = sh_quote(cwd);
        script.push_str(&format!(
            "cd {quoted} 2>/dev/null || echo \"legion: working directory {cwd} is missing\" >&2\n\n"
        ));
    }

    let command = payload
        .command
        .iter()
        .map(|arg| sh_quote(arg))
        .collect::<Vec<_>>()
        .join(" ");
    script.push_str(&render_command_supervision(&command));
    script.push_str("echo \"legion: sandbox command exited with status $status\"\n");
    script.push_str("sync\n\n");
    if !mounted.is_empty() {
        // Unmount the volume disks before the power goes, so every write has
        // reached the VMM and each filesystem is clean for the next boot.
        // Whatever still holds them open (a stray the command left behind,
        // an exec session, the agent) is killed first: the machine is going
        // down either way, and `poweroff -f` would not ask them nicely
        // either.
        script.push_str(
            "# Shut down cleanly: the volume disks outlive this boot.\n\
             kill -9 -1 2>/dev/null\n\
             legion_umount() {\n\
             \x20   umount \"$1\" 2>/dev/null && return 0\n\
             \x20   sleep 1 2>/dev/null\n\
             \x20   umount \"$1\" 2>/dev/null && return 0\n\
             \x20   mount -o remount,ro \"$1\" 2>/dev/null && return 0\n\
             \x20   echo \"legion: could not unmount volume at $1; its data was synced\" >&2\n\
             }\n",
        );
        // Reverse declaration order, so a volume mounted inside another
        // comes off first.
        for (index, quoted) in mounted.iter().rev() {
            script.push_str(&format!(
                "if [ -n \"$legion_vol_{index}\" ]; then legion_umount {quoted}; fi\n"
            ));
        }
        script.push_str("sync\n\n");
    }
    // A microVM has no service manager to stop: init must halt the machine so
    // the VMM process exits and the worker sees the task finish.
    script.push_str("poweroff -f 2>/dev/null || halt -f 2>/dev/null || reboot -f 2>/dev/null\n");
    script.push_str("echo o > /proc/sysrq-trigger 2>/dev/null\n");
    // Still alive? Exiting PID 1 panics the kernel, and `panic=1` on the
    // cmdline turns that panic into a reboot, which the VMM treats as exit.
    script.push_str("exit $status\n");
    script
}

/// The signals init answers as a stop request: every `stop.signal` Legion
/// accepts ([`crate::structs::STOP_SIGNALS`]), without the `SIG` prefix.
const GUEST_STOP_SIGNALS: [&str; 6] = ["TERM", "INT", "HUP", "QUIT", "USR1", "USR2"];

/// The part of `/legion/init` that runs the sandbox command (already
/// shell-quoted) and answers a stop request, leaving the command's exit
/// status in `$status`.
///
/// A stop request is a signal to PID 1, which the worker has the guest agent
/// send with the task's `stop.signal` (see `SandboxTaskPlugin::stop_with`).
/// Init flushes the guest's page cache at once, so everything written before
/// the request reaches the disks even if the workload then ignores the
/// signal and the VMM is killed at the deadline, and passes the signal on to
/// the command.
///
/// The command runs in the background because a shell runs a trap only
/// between commands: `wait` is the one place it can be interrupted. A
/// background command would otherwise read `/dev/null`, so the console is
/// handed through as fd 3 when init has one. The probe for "has one" runs in
/// a subshell because dash treats a failed redirection on `exec` (or any
/// builtin) as fatal, and a dead PID 1 panics the kernel. It dups to fd 3,
/// not onto fd 0, because dash skips a `<&0` as a no-op, and it redirects
/// nothing else, because opening `/dev/null` would land on a closed fd 0 and
/// fake the answer. `exec` in the function makes `$!` the command itself,
/// not a subshell around it.
///
/// Split out so a test can run it under a real shell: the rest of init
/// mounts, kills every process, and powers off.
pub(crate) fn render_command_supervision(command: &str) -> String {
    let mut block = String::from(
        "# A stop request is a signal to PID 1, sent by the guest agent for the worker.\n\
         legion_child=\n\
         legion_stop() {\n\
         \x20   echo \"legion: stop requested (SIG$1); flushing disks and signalling the sandbox command\"\n\
         \x20   sync\n\
         \x20   if [ -n \"$legion_child\" ]; then kill -s \"$1\" \"$legion_child\" 2>/dev/null; fi\n\
         }\n",
    );
    for signal in GUEST_STOP_SIGNALS {
        block.push_str(&format!("trap 'legion_stop {signal}' {signal}\n"));
    }
    block.push_str(&format!(
        "legion_run() {{\n\
         \x20   exec {command}\n\
         }}\n\
         if (true 3<&0); then\n\
         \x20   exec 3<&0\n\
         \x20   legion_run <&3 3<&- &\n\
         else\n\
         \x20   legion_run &\n\
         fi\n\
         legion_child=$!\n\
         exec 3<&-\n\
         while :; do\n\
         \x20   wait \"$legion_child\"\n\
         \x20   status=$?\n\
         \x20   # A trapped signal interrupts wait (status over 128) while the command runs on.\n\
         \x20   kill -0 \"$legion_child\" 2>/dev/null || break\n\
         done\n\
         legion_child=\n"
    ));
    block
}

/// Writes `/legion/init` (executable) and `/legion/env` into an extracted
/// rootfs, plus `/etc/hostname` for guest legibility.
pub(crate) fn install_guest_payload(
    rootfs: &Path,
    payload: &GuestPayload,
    hostname: &str,
) -> R<()> {
    let legion_dir = rootfs.join("legion");
    std::fs::create_dir_all(&legion_dir)
        .with_context(|| format!("create {}", legion_dir.display()))?;

    let init_path = legion_dir.join("init");
    std::fs::write(&init_path, render_init_script(payload))
        .with_context(|| format!("write {}", init_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&init_path, std::fs::Permissions::from_mode(0o755))
            .with_context(|| format!("chmod {}", init_path.display()))?;
    }

    let env_path = legion_dir.join("env");
    std::fs::write(&env_path, render_env_file(&payload.env))
        .with_context(|| format!("write {}", env_path.display()))?;
    // Where exec sessions start: the command's working directory.
    if let Some(cwd) = payload.cwd.as_deref().filter(|cwd| !cwd.is_empty()) {
        let cwd_path = legion_dir.join("cwd");
        std::fs::write(&cwd_path, cwd).with_context(|| format!("write {}", cwd_path.display()))?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // The env file can carry KV-injected secrets and identity tokens.
        std::fs::set_permissions(&env_path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("chmod {}", env_path.display()))?;
    }

    let etc = rootfs.join("etc");
    if std::fs::create_dir_all(&etc).is_ok() {
        let _ = std::fs::write(etc.join("hostname"), format!("{hostname}\n"));
        if payload.relay {
            // Names resolve through the agent's relay to the node resolver.
            let _ = std::fs::write(
                etc.join("resolv.conf"),
                "# Generated by Legion: the guest agent relays DNS to the node datapath.\nnameserver 127.0.0.1\noptions ndots:1\n",
            );
        } else if !payload.resolvers.is_empty() {
            // A TAP guest resolves through its bridge's resolvers. The
            // image's file may be a symlink (systemd-resolved), so it is
            // replaced rather than written through.
            let path = etc.join("resolv.conf");
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::write(
                &path,
                legion_guest::netconf::resolv_conf(&payload.resolvers),
            );
        }
    }
    if payload.guest_agent || payload.resume_watch {
        let binary = match payload.guest_binary.clone() {
            Some(binary) => binary,
            None => guest_binary()?,
        };
        install_guest_agent(rootfs, &binary)?;
    }
    Ok(())
}

/// The binary a guest runs as `/legion/legion`: its guest agent and resume
/// handler (calls entry 10 of the gated plan).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GuestBinary {
    pub path: PathBuf,
    /// The static `legion-guest`, which runs on any image (busybox, Alpine).
    /// False for the fallback, the worker's own binary, which needs the
    /// image to have a compatible glibc.
    pub is_static: bool,
    /// Size and modification time, so a new binary rebuilds the root disk
    /// rather than reusing one that carries the old agent.
    pub fingerprint: String,
}

impl GuestBinary {
    fn at(path: PathBuf, is_static: bool) -> R<Self> {
        let metadata =
            std::fs::metadata(&path).with_context(|| format!("reading {}", path.display()))?;
        let modified = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        Ok(GuestBinary {
            fingerprint: format!("{}:{}:{modified}", path.display(), metadata.len()),
            path,
            is_static,
        })
    }
}

/// Find the guest binary: [`GUEST_BINARY_ENV`] when set (and it must exist),
/// else `legion-guest` beside the worker's executable, else the worker's
/// own executable, which every guest can still try to run.
pub(crate) fn guest_binary() -> R<GuestBinary> {
    let worker = std::env::current_exe().context("locating the legion binary")?;
    guest_binary_from(
        std::env::var(GUEST_BINARY_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from),
        &worker,
    )
}

fn guest_binary_from(configured: Option<PathBuf>, worker: &Path) -> R<GuestBinary> {
    if let Some(path) = configured {
        if !path.is_file() {
            bail!(
                "{GUEST_BINARY_ENV} names {}, which does not exist: install the static \
                 legion-guest binary there, or unset it",
                path.display()
            );
        }
        return GuestBinary::at(path, true);
    }
    let beside = worker.with_file_name(GUEST_BINARY_NAME);
    if beside.is_file() {
        return GuestBinary::at(beside, true);
    }
    GuestBinary::at(worker.to_path_buf(), false)
}

/// At worker start: say once when sandbox guests will get the worker's own
/// binary, which a musl image (`alpine:latest`) cannot run, so its gated
/// guest has no network and its restores keep their old identity.
pub(crate) fn warn_if_no_static_guest_binary() {
    if installed_vmms().is_empty() {
        // No microVM monitor: this node runs dev-mode sandboxes only, which
        // never get the guest binary.
        return;
    }
    match guest_binary() {
        Ok(binary) if binary.is_static => {
            info!(path = %binary.path.display(), "Sandbox guests run the static legion-guest binary");
        }
        Ok(binary) => warn!(
            worker = %binary.path.display(),
            "No static legion-guest binary beside the worker (or {GUEST_BINARY_ENV}); sandbox guests get a copy of the worker itself, which runs only on images with a compatible glibc. On a musl image a gated sandbox has no network and a restore keeps its old identity"
        ),
        Err(err) => {
            warn!(error = %err, "The sandbox guest binary is unusable; sandbox guests that need it will fail to build")
        }
    }
}

/// Copy the guest binary into the rootfs as `/legion/legion`. An image that
/// cannot run it is reported by init at boot (a gated guest then has no
/// network, which is the gated mode's promise).
fn install_guest_agent(rootfs: &Path, binary: &GuestBinary) -> R<()> {
    let target = rootfs.join(GUEST_AGENT_PATH.trim_start_matches('/'));
    std::fs::copy(&binary.path, &target)
        .with_context(|| format!("copy {} to {}", binary.path.display(), target.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755))
            .with_context(|| format!("chmod {}", target.display()))?;
    }
    Ok(())
}

/// Bytes used by a directory tree, without following symlinks (a symlink's
/// target is not part of the tree we are about to copy into a disk image).
pub(crate) fn dir_size_bytes(dir: &Path) -> R<u64> {
    let mut total = 0_u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            // Racing removal of a scratch file is not a sizing error.
            Err(_) => continue,
        };
        if metadata.is_dir() {
            for entry in
                std::fs::read_dir(&path).with_context(|| format!("read dir {}", path.display()))?
            {
                stack.push(entry?.path());
            }
            // Directory entries themselves cost inodes/blocks in ext4.
            total = total.saturating_add(4096);
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

/// Sizes the ephemeral root disk: the extracted rootfs, plus ~12% for ext4
/// metadata, plus the task's requested headroom (the writable space the
/// workload gets), with a floor that keeps mkfs happy.
pub(crate) fn rootfs_image_bytes(used_bytes: u64, headroom_mib: u64) -> u64 {
    let metadata = used_bytes / 8;
    let headroom = headroom_mib.saturating_mul(MIB);
    let total = used_bytes
        .saturating_add(metadata)
        .saturating_add(headroom)
        .max(MIN_IMAGE_MIB * MIB);
    // Round up to a whole MiB: partial-MiB disks confuse nothing but read badly
    // in logs and `ls -l`.
    total.div_ceil(MIB) * MIB
}

/// Locates `mkfs.ext4` for root-disk builds: the [`MKFS_ENV`] override, else
/// the same discovery the `local-dir` volume driver's block mode uses
/// (`PATH` plus the well-known sbin locations) — one answer to "can this node
/// format ext4", shared with the volume system.
fn mkfs_binary() -> R<PathBuf> {
    if let Some(configured) = std::env::var(MKFS_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return Ok(PathBuf::from(configured));
    }
    crate::volumes::builtins::mkfs_binary("ext4").with_context(|| {
        format!("mkfs.ext4 not found: install e2fsprogs, or set {MKFS_ENV} to its path")
    })
}

/// Creates a sparse raw disk image at `image` of exactly `size_bytes` and
/// formats it ext4, optionally populating it from `source_dir`.
///
/// `mkfs.ext4 -d` writes the directory tree into the filesystem image directly,
/// so building a bootable root disk needs no loop device, no mount, and no
/// root — which is what makes this work in an ordinary worker process.
pub(crate) fn create_ext4_image(
    image: &Path,
    size_bytes: u64,
    source_dir: Option<&Path>,
    label: &str,
) -> R<()> {
    if let Some(parent) = image.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create image dir {}", parent.display()))?;
    }
    let file = std::fs::File::create(image)
        .with_context(|| format!("create disk image {}", image.display()))?;
    file.set_len(size_bytes)
        .with_context(|| format!("size disk image {} to {size_bytes} bytes", image.display()))?;
    drop(file);

    let mkfs = mkfs_binary()?;
    let mut cmd = Command::new(&mkfs);
    cmd.arg("-F").arg("-q").arg("-L").arg(label);
    // Files extracted by a non-root worker carry the worker's uid; the guest
    // expects root to own its filesystem root at minimum.
    cmd.arg("-E").arg("root_owner=0:0");
    if let Some(source) = source_dir {
        cmd.arg("-d").arg(source);
    }
    cmd.arg(image);

    let output = cmd
        .output()
        .with_context(|| format!("spawn {}", mkfs.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let _ = std::fs::remove_file(image);
        bail!(
            "mkfs.ext4 failed for {}: {}{}",
            image.display(),
            stderr.trim(),
            stdout.trim()
        );
    }
    Ok(())
}

/// Resolves the guest kernel: the task's `kernel`, else [`KERNEL_ENV`], else
/// the well-known node path. Errors name every place that was checked, since
/// "no kernel" is the most common first-run failure for microVMs.
pub(crate) fn resolve_kernel(configured: Option<&str>) -> R<PathBuf> {
    let candidate = configured
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            std::env::var(KERNEL_ENV)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| DEFAULT_KERNEL_PATH.to_string());

    let path = PathBuf::from(&candidate);
    if path.is_file() {
        return Ok(path);
    }
    bail!(
        "sandbox guest kernel not found at {candidate}: set the task's `kernel` (an \
         `${{artifact.<name>}}` reference works), set {KERNEL_ENV} on the worker, or install an \
         uncompressed kernel at {DEFAULT_KERNEL_PATH}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> VmSpec {
        VmSpec {
            name: "legion-sbx-7".into(),
            kernel: PathBuf::from("/var/lib/legion/kernels/vmlinux"),
            extra_cmdline: None,
            root_disk: PathBuf::from("/allocs/7/.sandbox/rootfs.img"),
            volume_disks: Vec::new(),
            vcpus: 2,
            memory_mib: 512,
            tap: None,
            api_socket: PathBuf::from("/allocs/7/.sandbox/ch.sock"),
            vsock: None,
            identity_disk: None,
            balloon: None,
        }
    }

    #[test]
    fn the_identity_disk_follows_the_volumes_on_both_vmms() {
        let mut spec = spec();
        spec.volume_disks = vec![VmDisk {
            path: PathBuf::from("/v/disk.img"),
            read_only: false,
        }];
        spec.identity_disk = Some(PathBuf::from("/links/identity.img"));
        assert_eq!(spec.identity_device().as_deref(), Some("/dev/vdc"));
        assert!(kernel_cmdline(&spec, true).contains("legion.identity=/dev/vdc"));

        let args = cloud_hypervisor_args(&spec);
        let disk_at = args.iter().position(|a| a == "--disk").expect("--disk");
        assert_eq!(args[disk_at + 2], "path=/v/disk.img,image_type=raw");
        assert_eq!(
            args[disk_at + 3],
            "path=/links/identity.img,image_type=raw,readonly=on,id=identity"
        );

        let drives = firecracker_config(&spec)["drives"].clone();
        assert_eq!(drives[2]["drive_id"], "identity");
        assert_eq!(drives[2]["path_on_host"], "/links/identity.img");
        assert_eq!(drives[2]["is_read_only"], true);

        // Without one, nothing changes: no extra drive, no cmdline arg.
        spec.identity_disk = None;
        assert!(!kernel_cmdline(&spec, true).contains("legion.identity"));
        assert_eq!(
            firecracker_config(&spec)["drives"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn a_balloon_is_configured_on_both_vmms_only_when_asked() {
        let mut spec = spec();
        assert!(!cloud_hypervisor_args(&spec).contains(&"--balloon".to_string()));
        assert!(firecracker_config(&spec).get("balloon").is_none());

        spec.balloon = Some(BalloonSpec {
            free_page_reporting: true,
        });
        let args = cloud_hypervisor_args(&spec);
        let at = args
            .iter()
            .position(|arg| arg == "--balloon")
            .expect("--balloon");
        assert_eq!(
            args[at + 1],
            "size=0,deflate_on_oom=on,free_page_reporting=on"
        );
        let balloon = firecracker_config(&spec)["balloon"].clone();
        assert_eq!(balloon["amount_mib"], 0);
        assert_eq!(balloon["deflate_on_oom"], true);
        assert!(balloon.get("free_page_reporting").is_none());

        spec.balloon = Some(BalloonSpec {
            free_page_reporting: false,
        });
        let args = cloud_hypervisor_args(&spec);
        let at = args.iter().position(|arg| arg == "--balloon").unwrap();
        assert_eq!(args[at + 1], "size=0,deflate_on_oom=on");
    }

    #[test]
    fn balloon_targets_never_squeeze_below_the_reservation() {
        const MIB: u64 = 1024 * 1024;
        // 4 GiB guest, 512 MiB reserved: at most 3584 MiB in the balloon.
        assert_eq!(balloon_target_mib(0, 1000 * MIB, 4096, 512), 1000);
        assert_eq!(balloon_target_mib(3000, 1000 * MIB, 4096, 512), 3584);
        assert_eq!(balloon_target_mib(3584, MIB, 4096, 512), 3584);
        // Partial MiB rounds up; a guest with no room above its
        // reservation gets nothing.
        assert_eq!(balloon_target_mib(0, 1, 4096, 512), 1);
        assert_eq!(balloon_target_mib(0, 100 * MIB, 512, 512), 0);
    }

    #[test]
    fn vmm_versions_parse_from_both_banners() {
        assert_eq!(
            parse_vmm_version("Firecracker v1.7.0\n").as_deref(),
            Some("1.7.0")
        );
        assert_eq!(
            parse_vmm_version("cloud-hypervisor v41.0.0-dirty\n").as_deref(),
            Some("41.0.0")
        );
        assert_eq!(parse_vmm_version("no version here"), None);
    }

    /// cloud-hypervisor's banner prints `v51.2` while its `vmm.ping` says
    /// `51.2.0`: both must record the same string, or a node refuses its own
    /// memory checkpoints.
    #[test]
    fn the_banner_and_the_api_record_one_version() {
        assert_eq!(
            parse_vmm_version("cloud-hypervisor v51.2\n"),
            parse_vmm_version("51.2.0")
        );
        assert_eq!(parse_vmm_version("51.2.0").as_deref(), Some("51.2.0"));
        assert_eq!(
            parse_vmm_version("cloud-hypervisor v51.2-5-gabc1234-dirty\n").as_deref(),
            Some("51.2.0")
        );
        assert_eq!(parse_vmm_version("v1.7.0").as_deref(), Some("1.7.0"));
    }

    #[test]
    fn cmdline_boots_our_init_off_the_root_disk() {
        let cmdline = kernel_cmdline(&spec(), false);
        assert!(cmdline.contains("root=/dev/vda rw"));
        assert!(cmdline.contains("init=/legion/init"));
        // cloud-hypervisor's console is virtio (fast, full boot log);
        // Firecracker has only serial, so it boots quiet.
        assert!(cmdline.contains("console=hvc0"));
        assert!(!cmdline.contains("quiet"));
        let firecracker = kernel_cmdline(&spec(), true);
        assert!(firecracker.contains("console=ttyS0 quiet"));
        // Panic must reboot, or a wedged guest would hang the allocation.
        assert!(cmdline.contains("panic=1"));
        // PCI suppression is a Firecracker-only need; it would break
        // cloud-hypervisor's virtio-pci devices.
        assert!(!cmdline.contains("pci=off"));
        assert!(kernel_cmdline(&spec(), true).contains("pci=off"));
    }

    #[test]
    fn cmdline_appends_extra_args() {
        let mut spec = spec();
        spec.extra_cmdline = Some("  quiet loglevel=1 ".into());
        let cmdline = kernel_cmdline(&spec, false);
        assert!(cmdline.ends_with("quiet loglevel=1"));
        // The task's args come after Legion's, so `loglevel=7` brings back
        // a Firecracker guest's full boot log (the last loglevel wins).
        spec.extra_cmdline = Some("loglevel=7".into());
        let cmdline = kernel_cmdline(&spec, true);
        assert!(cmdline.find("quiet") < cmdline.find("loglevel=7"));
    }

    #[test]
    fn cloud_hypervisor_args_carry_disks_cpus_memory_and_console() {
        let mut spec = spec();
        spec.volume_disks = vec![
            VmDisk {
                path: PathBuf::from("/volumes/local-dir/9f2c/disk.img"),
                read_only: false,
            },
            VmDisk {
                path: PathBuf::from("/dev/mapper/legion-models"),
                read_only: true,
            },
        ];
        spec.tap = Some(TapNet {
            device: "tap0".into(),
            mac: Some("02:00:00:00:00:01".into()),
        });
        let args = cloud_hypervisor_args(&spec);

        let disk_at = args.iter().position(|a| a == "--disk").expect("--disk");
        // Every disk is declared raw, or cloud-hypervisor refuses guest
        // writes to its sector 0 and the root mount fails.
        assert_eq!(
            args[disk_at + 1],
            "path=/allocs/7/.sandbox/rootfs.img,image_type=raw"
        );
        // Volume disks follow in attach order: /dev/vdb, then /dev/vdc.
        assert_eq!(
            args[disk_at + 2],
            "path=/volumes/local-dir/9f2c/disk.img,image_type=raw"
        );
        // A read-only volume must be read-only at the VMM, not just in-guest.
        assert_eq!(
            args[disk_at + 3],
            "path=/dev/mapper/legion-models,image_type=raw,readonly=on"
        );
        assert!(args.contains(&"boot=2".to_string()));
        assert!(args.contains(&"size=512M".to_string()));
        // The guest's virtio-console on the VMM's stdout is what makes log
        // capture work; the serial port is off.
        let console_at = args
            .iter()
            .position(|a| a == "--console")
            .expect("--console");
        assert_eq!(args[console_at + 1], "tty");
        let serial_at = args.iter().position(|a| a == "--serial").expect("--serial");
        assert_eq!(args[serial_at + 1], "off");
        assert!(args.contains(&"tap=tap0,mac=02:00:00:00:00:01".to_string()));

        // A gated sandbox gets its vsock device on both VMMs, always as
        // guest CID 3.
        spec.vsock = Some(VsockSpec {
            socket: PathBuf::from("/allocs/7/.sandbox/vsock.sock"),
        });
        let args = cloud_hypervisor_args(&spec);
        let at = args.iter().position(|a| a == "--vsock").expect("--vsock");
        assert_eq!(args[at + 1], "cid=3,socket=/allocs/7/.sandbox/vsock.sock");
        let config = firecracker_config(&spec);
        assert_eq!(config["vsock"]["guest_cid"], 3);
        assert_eq!(config["vsock"]["uds_path"], "/allocs/7/.sandbox/vsock.sock");
    }

    /// `sun_path` holds 107 bytes plus a NUL, and the longest datapath
    /// socket is `<base>_1080`.
    #[test]
    fn a_gated_socket_path_over_the_unix_limit_is_refused_by_name() {
        assert!(
            check_vsock_socket_path(Path::new("/var/lib/legion/allocs/7/.sandbox/vsock.sock"))
                .is_ok()
        );
        // 102 bytes of base + "_1080" is exactly 107.
        let base = format!("/{}", "a".repeat(101));
        assert_eq!(base.len(), 102);
        assert!(check_vsock_socket_path(Path::new(&base)).is_ok());
        let base = format!("/{}", "a".repeat(102));
        let err = check_vsock_socket_path(Path::new(&base)).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("is 108 bytes, over the 107-byte Unix socket limit"),
            "{message}"
        );
        assert!(
            message.contains("shorten the worker's state path"),
            "{message}"
        );
    }

    #[test]
    fn a_checkpointable_guest_starts_the_resume_handler_before_the_command() {
        let payload = GuestPayload {
            command: vec!["/bin/sh".into()],
            resume_watch: true,
            ..GuestPayload::default()
        };
        let script = render_init_script(&payload);
        let watch_at = script
            .find("/legion/legion guest-resume")
            .expect("resume handler line");
        let command_at = script.find("'/bin/sh'").expect("command");
        assert!(watch_at < command_at, "the handler starts first");
        assert!(!script.contains("guest-agent"));
        let plain = render_init_script(&GuestPayload {
            command: vec!["/bin/sh".into()],
            ..GuestPayload::default()
        });
        assert!(!plain.contains("guest-resume"));
    }

    #[test]
    fn a_gated_guest_starts_the_agent_before_the_command() {
        let payload = GuestPayload {
            command: vec!["/bin/sh".into()],
            guest_agent: true,
            relay: true,
            resume_watch: false,
            ..GuestPayload::default()
        };
        let script = render_init_script(&payload);
        let agent_at = script
            .find("/legion/legion guest-agent")
            .expect("agent line");
        let command_at = script.find("'/bin/sh'").expect("command");
        assert!(agent_at < command_at, "the agent starts first");
        assert!(script.contains("the sandbox has no network"));
        assert!(!script.contains("--resume-watch"));
        assert!(!script.contains("--no-relay"));
        let plain = render_init_script(&GuestPayload::default());
        assert!(!plain.contains("guest-agent"));
    }

    /// A TAP or `none` microVM runs the agent for exec only: no relays, so
    /// the guest still cannot dial the host (sandbox-exec plan §6.1).
    #[test]
    fn an_ungated_guest_runs_the_agent_for_exec_only() {
        let script = render_init_script(&GuestPayload {
            command: vec!["/bin/sh".into()],
            guest_agent: true,
            relay: false,
            ..GuestPayload::default()
        });
        assert!(
            script.contains("/legion/legion guest-agent --no-relay "),
            "{script}"
        );
        assert!(script.contains("the sandbox takes no exec sessions"));
        assert!(!script.contains("has no network"));
    }

    /// A gated sandbox launched for checkpoints runs one process: the agent
    /// owns the resume watch, so a restore can close its relays.
    /// Gated plan §6.5, calls entry 10: the static `legion-guest` beside the
    /// worker wins, an explicit override must exist, and with neither the
    /// worker copies itself (the glibc fallback).
    #[test]
    fn the_static_guest_binary_is_preferred_over_the_worker() -> R<()> {
        let dir = tempfile::tempdir()?;
        let worker = dir.path().join("legion");
        std::fs::write(&worker, b"worker")?;
        let fallback = guest_binary_from(None, &worker)?;
        assert_eq!(fallback.path, worker);
        assert!(!fallback.is_static);

        std::fs::write(dir.path().join("legion-guest"), b"static guest")?;
        let beside = guest_binary_from(None, &worker)?;
        assert_eq!(beside.path, dir.path().join("legion-guest"));
        assert!(beside.is_static);
        assert_ne!(beside.fingerprint, fallback.fingerprint);

        let elsewhere = dir.path().join("opt-legion-guest");
        std::fs::write(&elsewhere, b"another static guest")?;
        assert_eq!(
            guest_binary_from(Some(elsewhere.clone()), &worker)?.path,
            elsewhere
        );
        let err = guest_binary_from(Some(dir.path().join("missing")), &worker).unwrap_err();
        assert!(
            err.to_string().contains("LEGION_GUEST_BINARY names"),
            "{err}"
        );

        // What lands in the guest is the chosen binary.
        let rootfs = tempfile::tempdir()?;
        install_guest_payload(
            rootfs.path(),
            &GuestPayload {
                command: vec!["/bin/sh".into()],
                guest_agent: true,
                guest_binary: Some(beside),
                ..GuestPayload::default()
            },
            "legion-sbx-3",
        )?;
        assert_eq!(
            std::fs::read(rootfs.path().join("legion/legion"))?,
            b"static guest"
        );
        Ok(())
    }

    #[test]
    fn a_gated_checkpointable_guest_runs_the_watch_inside_the_agent() {
        let script = render_init_script(&GuestPayload {
            command: vec!["/bin/sh".into()],
            guest_agent: true,
            relay: true,
            resume_watch: true,
            ..GuestPayload::default()
        });
        assert!(
            script.contains("/legion/legion guest-agent --resume-watch"),
            "{script}"
        );
        assert!(!script.contains("guest-resume"), "{script}");
    }

    /// Gated plan §6.10: a gated guest's advertised ports and upstreams
    /// reach the agent as flags, which the guest crate's own parser accepts.
    #[test]
    fn a_gated_guest_hands_its_ports_to_the_agent() {
        let upstreams = vec![
            legion_guest::identity::GuestUpstream {
                port: 20003,
                vsock_port: None,
                vip: Some("127.42.0.2:5432".parse().unwrap()),
            },
            legion_guest::identity::GuestUpstream {
                port: 20004,
                vsock_port: None,
                vip: None,
            },
        ];
        let script = render_init_script(&GuestPayload {
            command: vec!["/bin/sh".into()],
            guest_agent: true,
            relay: true,
            ingress_ports: vec![8080, 20001],
            upstreams: upstreams.clone(),
            resume_watch: true,
            ..GuestPayload::default()
        });
        let line = script
            .lines()
            .find(|line| line.contains("/legion/legion guest-agent"))
            .expect("agent line");
        assert_eq!(
            line.trim(),
            "/legion/legion guest-agent --ingress-port 8080 --ingress-port 20001 \
             --upstream 20003=127.42.0.2:5432 --upstream 20004 --resume-watch \
             >/dev/console 2>&1 &"
        );
        let words: Vec<String> = line
            .split_whitespace()
            .skip(1)
            .take_while(|word| !word.starts_with('>'))
            .map(str::to_string)
            .collect();
        match legion_guest::cli::parse(&words).unwrap() {
            legion_guest::cli::Invocation::Agent(args) => {
                assert!(args.relay);
                assert_eq!(args.ingress_ports, vec![8080, 20001]);
                assert_eq!(args.upstreams, upstreams);
                assert!(args.resume_watch.is_some());
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn firecracker_config_matches_vm_spec() {
        let mut spec = spec();
        spec.volume_disks = vec![VmDisk {
            path: PathBuf::from("/volumes/local-dir/9f2c/disk.img"),
            read_only: true,
        }];
        spec.tap = Some(TapNet {
            device: "tap9".into(),
            mac: None,
        });
        let config = firecracker_config(&spec);

        assert_eq!(
            config["boot-source"]["kernel_image_path"],
            "/var/lib/legion/kernels/vmlinux"
        );
        assert!(config["boot-source"]["boot_args"]
            .as_str()
            .unwrap()
            .contains("init=/legion/init"));
        let drives = config["drives"].as_array().expect("drives");
        assert_eq!(drives.len(), 2);
        assert_eq!(drives[0]["is_root_device"], true);
        assert_eq!(
            drives[1]["path_on_host"],
            "/volumes/local-dir/9f2c/disk.img"
        );
        assert_eq!(drives[1]["is_root_device"], false);
        assert_eq!(drives[1]["is_read_only"], true);
        assert_eq!(config["machine-config"]["vcpu_count"], 2);
        assert_eq!(config["machine-config"]["mem_size_mib"], 512);
        assert_eq!(config["network-interfaces"][0]["host_dev_name"], "tap9");
        assert!(config["network-interfaces"][0].get("guest_mac").is_none());
    }

    #[test]
    fn init_script_mounts_volume_disks_and_powers_off() {
        let payload = GuestPayload {
            command: vec!["/bin/sh".into(), "-c".into(), "echo 'hi there'".into()],
            env: BTreeMap::from([("MODE".to_string(), "test".to_string())]),
            cwd: Some("/srv".into()),
            disk_mounts: vec![
                GuestDiskMount {
                    device: guest_volume_device(0),
                    path: Some("/workspace".into()),
                    fs: "ext4".into(),
                    read_only: false,
                },
                GuestDiskMount {
                    device: guest_volume_device(1),
                    path: Some("/models".into()),
                    fs: "ext4".into(),
                    read_only: true,
                },
            ],
            guest_agent: false,
            relay: false,
            resume_watch: false,
            guest_binary: None,
            resolvers: Vec::new(),
            ingress_ports: Vec::new(),
            upstreams: Vec::new(),
        };
        let script = render_init_script(&payload);

        assert!(script.starts_with("#!/bin/sh\n"));
        assert!(script.contains("mount -t proc proc /proc"));
        assert!(script.contains("mount -t ext4 /dev/vdb '/workspace'"));
        // Read-only volumes mount read-only; devices follow attach order.
        assert!(script.contains("mount -t ext4 -o ro /dev/vdc '/models'"));
        assert!(script.contains(". /legion/env"));
        assert!(script.contains("cd '/srv'"));
        // Command args are single-quoted, so embedded quotes/spaces survive.
        assert!(script.contains("'/bin/sh' '-c' 'echo '\\''hi there'\\'''"));
        // Without the poweroff the VMM would outlive the workload.
        assert!(script.contains("poweroff -f"));
        assert!(script.contains("sysrq-trigger"));
        assert!(script.contains("exit $status"));

        // The volumes come off before the power goes, innermost (last
        // declared) first, after whatever holds them open is gone, and only
        // when init actually mounted them.
        let at = |needle: &str| {
            script
                .find(needle)
                .unwrap_or_else(|| panic!("{needle:?} in {script}"))
        };
        let killed = at("kill -9 -1");
        let models = at("legion_umount '/models'");
        let workspace = at("legion_umount '/workspace'");
        let poweroff = at("poweroff -f");
        assert!(at("status=$?") < killed);
        assert!(killed < models && models < workspace && workspace < poweroff);
        assert!(script.contains("legion_vol_0=1"));
        assert!(script.contains("if [ -n \"$legion_vol_0\" ]; then legion_umount '/workspace'; fi"));
    }

    /// A guest with no volumes has nothing to unmount, so its shutdown stays
    /// what it was: sync, then power off.
    #[test]
    fn init_script_without_volumes_skips_the_unmount() {
        let script = render_init_script(&GuestPayload {
            command: vec!["true".into()],
            ..GuestPayload::default()
        });
        assert!(!script.contains("kill -9 -1"));
        assert!(!script.contains("legion_umount"));
        // Every guest still answers a stop request.
        assert!(script.contains("trap 'legion_stop TERM' TERM"));
    }

    /// Init must answer every signal a jobspec's `stop.signal` can name, or a
    /// stop request with that signal is silently dropped by the kernel (PID 1
    /// gets only the signals it has a handler for) and the guest is killed at
    /// the deadline instead.
    #[test]
    fn init_traps_every_stop_signal() {
        let block = render_command_supervision("true");
        for signal in crate::structs::STOP_SIGNALS {
            let name = signal.trim_start_matches("SIG");
            assert!(
                block.contains(&format!("trap 'legion_stop {name}' {name}\n")),
                "init does not trap {signal}"
            );
        }
    }

    /// The shells init may run under that this host has: `sh`, plus dash
    /// and bash by name. Images ship all of these as `/bin/sh` (Debian and
    /// Ubuntu dash, Fedora bash), and they differ exactly where init's
    /// supervision is delicate (traps, background jobs, fd redirections).
    #[cfg(unix)]
    fn shells() -> Vec<PathBuf> {
        let mut shells: Vec<PathBuf> = ["sh", "dash", "bash"]
            .into_iter()
            .filter_map(find_on_path)
            .collect();
        shells.dedup();
        shells
    }

    /// How init's stdin looks to the supervision under test.
    #[cfg(unix)]
    #[derive(Clone, Copy)]
    enum InitStdin {
        Piped,
        Closed,
    }

    /// Runs init's command supervision under `shell`, as init would with
    /// `command`, and returns the shell with its stdout piped.
    #[cfg(unix)]
    fn supervise(
        shell: &Path,
        dir: &Path,
        command: &str,
        stdin: InitStdin,
    ) -> R<std::process::Child> {
        use std::os::unix::process::CommandExt as _;
        let script = dir.join("supervise.sh");
        std::fs::write(
            &script,
            format!(
                "{}echo \"status=$status\"\n",
                render_command_supervision(command)
            ),
        )?;
        let mut cmd = Command::new(shell);
        cmd.arg(&script)
            .current_dir(dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        if let InitStdin::Closed = stdin {
            // A kernel that could not open /dev/console starts init this way.
            // SAFETY: close is async-signal-safe.
            unsafe {
                cmd.pre_exec(|| {
                    libc::close(0);
                    Ok(())
                });
            }
        }
        Ok(cmd.spawn()?)
    }

    /// The stop sequence inside the guest: a signal to init (the shell here)
    /// reaches the command, init waits for the command to finish its own
    /// shutdown, and reports the command's status, not the signal's.
    #[cfg(unix)]
    #[test]
    fn init_passes_a_stop_signal_to_the_command_and_waits_for_it() -> R<()> {
        for shell in shells() {
            let dir = tempfile::tempdir()?;
            let command = sh_quote(
                "trap 'echo command-got-usr1; exit 7' USR1; touch ready; \
                 while :; do sleep 0.05; done",
            );
            let init = supervise(
                &shell,
                dir.path(),
                &format!("sh -c {command}"),
                InitStdin::Piped,
            )?;
            let ready = dir.path().join("ready");
            let started = std::time::Instant::now();
            while !ready.exists() {
                assert!(
                    started.elapsed() < std::time::Duration::from_secs(10),
                    "the command never started under {}",
                    shell.display()
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            // What the guest agent does for the worker: signal init.
            let pid = init.id() as libc::pid_t;
            assert_eq!(unsafe { libc::kill(pid, libc::SIGUSR1) }, 0);

            let output = init.wait_with_output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            let shell = shell.display();
            assert!(output.status.success(), "{shell}: {stdout}");
            assert!(
                stdout.contains("legion: stop requested (SIGUSR1)"),
                "{shell}: {stdout}"
            );
            assert!(stdout.contains("command-got-usr1"), "{shell}: {stdout}");
            assert!(stdout.ends_with("status=7\n"), "{shell}: {stdout}");
        }
        Ok(())
    }

    /// The command still reads init's stdin (the console), as it did when
    /// init ran it in the foreground, and its own exit status comes through.
    #[cfg(unix)]
    #[test]
    fn init_hands_the_command_its_stdin_and_status() -> R<()> {
        use std::io::Write as _;
        for shell in shells() {
            let dir = tempfile::tempdir()?;
            let command = sh_quote("read line; echo got-$line; exit 3");
            let mut init = supervise(
                &shell,
                dir.path(),
                &format!("sh -c {command}"),
                InitStdin::Piped,
            )?;
            init.stdin.take().expect("stdin").write_all(b"hello\n")?;
            let output = init.wait_with_output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert_eq!(stdout, "got-hello\nstatus=3\n", "{}", shell.display());
        }
        Ok(())
    }

    /// Init with no stdin at all still runs the command and lives on. A
    /// failed redirection on `exec` is fatal to dash, and init dying is a
    /// kernel panic.
    #[cfg(unix)]
    #[test]
    fn init_survives_a_closed_stdin() -> R<()> {
        for shell in shells() {
            let dir = tempfile::tempdir()?;
            let init = supervise(&shell, dir.path(), "sh -c 'exit 4'", InitStdin::Closed)?;
            let output = init.wait_with_output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert_eq!(stdout, "status=4\n", "{}", shell.display());
        }
        Ok(())
    }

    /// The init script is generated code that only ever runs inside a guest,
    /// where a syntax error surfaces as an unexplained kernel panic. Parse it
    /// with a real shell here instead.
    #[test]
    fn init_script_is_valid_posix_shell() -> R<()> {
        let Some(shell) = find_on_path("sh") else {
            return Ok(());
        };
        // Values chosen to break naive quoting: quotes, spaces, `$`, newlines.
        let payload = GuestPayload {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                "echo \"it's $HOME\" && printf 'a\nb'".into(),
            ],
            env: BTreeMap::from([
                ("QUOTED".to_string(), "a'b \"c\" $d `e`".to_string()),
                ("MULTILINE".to_string(), "line1\nline2".to_string()),
            ]),
            cwd: Some("/opt/my app".into()),
            disk_mounts: vec![GuestDiskMount {
                device: guest_volume_device(0),
                path: Some("/work space".into()),
                fs: "ext4".into(),
                read_only: false,
            }],
            guest_agent: false,
            relay: false,
            resume_watch: false,
            guest_binary: None,
            resolvers: Vec::new(),
            ingress_ports: Vec::new(),
            upstreams: Vec::new(),
        };

        let dir = tempfile::tempdir()?;
        let init = dir.path().join("init");
        std::fs::write(&init, render_init_script(&payload))?;
        let env = dir.path().join("env");
        std::fs::write(&env, render_env_file(&payload.env))?;

        for script in [&init, &env] {
            let output = Command::new(&shell).arg("-n").arg(script).output()?;
            assert!(
                output.status.success(),
                "generated {} is not valid shell: {}",
                script.display(),
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // The env file must round-trip its values through a real shell.
        let output = Command::new(&shell)
            .arg("-c")
            .arg(format!(". {} && printf %s \"$QUOTED\"", env.display()))
            .output()?;
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            "a'b \"c\" $d `e`",
            "env values must survive sourcing verbatim"
        );
        Ok(())
    }

    #[test]
    fn init_script_omits_state_and_cwd_when_unset() {
        let script = render_init_script(&GuestPayload {
            command: vec!["true".into()],
            ..GuestPayload::default()
        });
        assert!(!script.contains("/dev/vdb"));
        assert!(!script.contains("cd "));
    }

    #[test]
    fn env_file_quotes_values_safely() {
        let env = BTreeMap::from([
            ("TOKEN".to_string(), "a'b c$d".to_string()),
            ("EMPTY".to_string(), String::new()),
        ]);
        let rendered = render_env_file(&env);
        assert!(rendered.contains("export TOKEN='a'\\''b c$d'"));
        assert!(rendered.contains("export EMPTY=''"));
    }

    #[test]
    fn rootfs_sizing_adds_metadata_and_headroom() {
        // 100 MiB of content + 256 MiB headroom + 12.5% metadata.
        let used = 100 * MIB;
        let size = rootfs_image_bytes(used, 256);
        assert_eq!(size, (100 + 12 + 256 + 1) * MIB - MIB + MIB);
        assert!(size >= used + 256 * MIB);
        // Tiny images are floored so mkfs has room for its own structures.
        assert_eq!(rootfs_image_bytes(0, 0), MIN_IMAGE_MIB * MIB);
        // Always a whole number of MiB.
        assert_eq!(rootfs_image_bytes(1234567, 1) % MIB, 0);
    }

    #[test]
    fn dir_size_counts_files_without_following_symlinks() -> R<()> {
        let dir = tempfile::tempdir()?;
        std::fs::create_dir_all(dir.path().join("nested"))?;
        std::fs::write(dir.path().join("nested/a"), vec![0u8; 2048])?;
        #[cfg(unix)]
        std::os::unix::fs::symlink("/etc/passwd", dir.path().join("link"))?;

        let size = dir_size_bytes(dir.path())?;
        // Two directories (4 KiB each) + the 2 KiB file + a small symlink.
        assert!(size >= 2048 + 8192, "got {size}");
        assert!(
            size < 64 * 1024,
            "symlink target must not be counted: {size}"
        );
        Ok(())
    }

    #[test]
    fn install_guest_payload_writes_init_and_env() -> R<()> {
        let rootfs = tempfile::tempdir()?;
        let payload = GuestPayload {
            command: vec!["/bin/true".into()],
            env: BTreeMap::from([("A".to_string(), "1".to_string())]),
            ..GuestPayload::default()
        };
        install_guest_payload(rootfs.path(), &payload, "legion-sbx-3")?;

        let init = std::fs::read_to_string(rootfs.path().join("legion/init"))?;
        assert!(init.contains("/bin/true"));
        assert!(std::fs::read_to_string(rootfs.path().join("legion/env"))?.contains("export A='1'"));
        assert_eq!(
            std::fs::read_to_string(rootfs.path().join("etc/hostname"))?.trim(),
            "legion-sbx-3"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(rootfs.path().join("legion/init"))?
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o755, "init must be executable, got {mode:o}");
            // Env can carry secrets; it must not be world-readable.
            let env_mode = std::fs::metadata(rootfs.path().join("legion/env"))?
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(env_mode, 0o600, "env must be owner-only, got {env_mode:o}");
        }
        Ok(())
    }

    /// Gated plan §6.7: a TAP guest resolves through its bridge's
    /// resolvers, replacing the image's file even when it is a symlink. A
    /// guest with no resolvers keeps the image's own.
    #[test]
    fn a_tap_guest_gets_its_bridge_resolvers() -> R<()> {
        let rootfs = tempfile::tempdir()?;
        std::fs::create_dir_all(rootfs.path().join("etc"))?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            "/run/systemd/resolve/stub-resolv.conf",
            rootfs.path().join("etc/resolv.conf"),
        )?;
        // A TAP guest runs the agent for exec, with no relay: its names
        // still resolve through its bridge, not the agent.
        let agent = rootfs.path().join("agent-binary");
        std::fs::write(&agent, b"#!/bin/sh\n")?;
        let payload = GuestPayload {
            command: vec!["/bin/true".into()],
            resolvers: vec!["10.88.0.1".parse()?, "1.1.1.1".parse()?],
            guest_agent: true,
            relay: false,
            guest_binary: Some(GuestBinary::at(agent, true)?),
            cwd: Some("/workspace".into()),
            ..GuestPayload::default()
        };
        install_guest_payload(rootfs.path(), &payload, "legion-sbx-3")?;
        let resolv = std::fs::read_to_string(rootfs.path().join("etc/resolv.conf"))?;
        assert!(
            resolv.contains("nameserver 10.88.0.1\nnameserver 1.1.1.1\n"),
            "{resolv}"
        );
        // Exec sessions start where the command does.
        assert_eq!(
            std::fs::read_to_string(rootfs.path().join("legion/cwd"))?,
            "/workspace"
        );

        let untouched = tempfile::tempdir()?;
        std::fs::create_dir_all(untouched.path().join("etc"))?;
        std::fs::write(
            untouched.path().join("etc/resolv.conf"),
            "nameserver 9.9.9.9\n",
        )?;
        install_guest_payload(
            untouched.path(),
            &GuestPayload {
                command: vec!["/bin/true".into()],
                ..GuestPayload::default()
            },
            "legion-sbx-4",
        )?;
        assert_eq!(
            std::fs::read_to_string(untouched.path().join("etc/resolv.conf"))?,
            "nameserver 9.9.9.9\n"
        );
        Ok(())
    }

    #[test]
    fn unknown_hypervisor_selection_is_rejected() {
        let err = Backend::detect(Some("qemu")).expect_err("unknown hypervisor");
        assert!(
            err.to_string().contains("unknown sandbox hypervisor"),
            "{err}"
        );
    }

    #[test]
    fn kernel_resolution_reports_every_place_it_looked() {
        let err = resolve_kernel(Some("/definitely/not/a/kernel")).expect_err("missing kernel");
        let message = err.to_string();
        assert!(message.contains("/definitely/not/a/kernel"), "{message}");
        assert!(message.contains(KERNEL_ENV), "{message}");
    }

    #[test]
    fn kernel_resolution_accepts_an_existing_file() -> R<()> {
        let dir = tempfile::tempdir()?;
        let kernel = dir.path().join("vmlinux");
        std::fs::write(&kernel, b"not really a kernel")?;
        assert_eq!(resolve_kernel(Some(kernel.to_str().unwrap()))?, kernel);
        Ok(())
    }

    /// The real disk-image path: format a populated ext4 image with no root
    /// and no loop device, then confirm the bytes look like ext4.
    #[test]
    fn builds_a_populated_ext4_image() -> R<()> {
        if mkfs_binary().is_err() {
            return Ok(()); // e2fsprogs not installed on this machine.
        }
        let work = tempfile::tempdir()?;
        let source = work.path().join("rootfs");
        std::fs::create_dir_all(source.join("legion"))?;
        std::fs::write(source.join("legion/init"), b"#!/bin/sh\ntrue\n")?;

        let image = work.path().join("rootfs.img");
        let size = rootfs_image_bytes(dir_size_bytes(&source)?, 8);
        create_ext4_image(&image, size, Some(&source), "legion-root")?;

        assert_eq!(std::fs::metadata(&image)?.len(), size);
        // ext2/3/4 superblock magic 0xEF53 lives at offset 0x438 (little endian).
        let bytes = std::fs::read(&image)?;
        assert_eq!(
            &bytes[0x438..0x43A],
            &[0x53, 0xEF],
            "image should carry an ext4 superblock"
        );
        Ok(())
    }

    #[test]
    fn guest_volume_devices_follow_attach_order() {
        assert_eq!(guest_volume_device(0), "/dev/vdb");
        assert_eq!(guest_volume_device(1), "/dev/vdc");
        assert_eq!(guest_volume_device(24), "/dev/vdz");
        // Out-of-range indexes clamp rather than panic or emit `vd{`.
        assert_eq!(guest_volume_device(99), "/dev/vdz");
    }
}
