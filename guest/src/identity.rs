//! Who a restored guest is, and the identity disk that tells it.
//!
//! A restored guest wakes up believing it is the allocation that was
//! captured. [`ResumeIdentity`] is the identity it must take on instead. The
//! worker writes one to the guest's identity disk at boot and on every
//! restore, and the guest's resume handler ([`crate::resume`]) re-reads it.
//!
//! The disk is a raw 1 MiB image rather than a filesystem, so the host can
//! write one per restore without `mkfs` or a mount, and the guest can read it
//! with a plain `read(2)` of the block device: `LEGIONID`, a format version
//! and a payload length (both little-endian `u32`), and the identity as JSON,
//! zero-padded. The worker and the guest share this module, so the two sides
//! cannot disagree about the format.

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

/// Who a restored allocation is. See the module docs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumeIdentity {
    /// The new allocation id (`LEGION_ALLOC_ID`).
    pub alloc_id: i64,
    /// The instance the allocation serves (`LEGION_INSTANCE_ID`). A fork has
    /// a new one, a resume keeps the old one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    pub job_id: String,
    pub task_name: String,
    /// Guest hostname.
    pub hostname: String,
    /// A fresh workload-identity token, when the task requests one. Secret:
    /// never logged, and the identity disk is mode 0600 on the host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// MAC for the guest NIC, when the restore assigns a new one (forks).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mac: Option<String>,
    /// Extra env the guest agent re-exports after resume.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Fresh bytes for the guest to mix into its entropy pool, so forks of
    /// one snapshot never share an RNG state. Hex-encoded.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub entropy: String,
    /// The host's wall clock when this identity was written, in Unix
    /// milliseconds. A restored guest's clock is wherever it was at capture;
    /// the resume handler steps it forward to this (plus however long the
    /// handler took to notice). 0 on identities written before the field.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub host_time_unix_ms: i64,
    /// argv the guest agent runs after refreshing identity on a restore
    /// (`suspend.onResume`). Empty at first boot and when the task sets none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_resume: Vec<String>,
    /// The restore is into a NEW instance (a fork, or a create from a
    /// snapshot), so the host gives the guest NIC its own TAP device. Host
    /// side only: never written to the identity disk.
    #[serde(skip)]
    pub fork: bool,
    /// File-delivered workload-identity tokens the guest keeps under
    /// `/legion/identity/` (`identities[].file` on a sandbox). The worker
    /// rewrites the identity disk in place on every refresh, and the guest's
    /// handler rewrites these files with no restart. Secret, like `token`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<GuestTokenFile>,
    /// The address a TAP-wired guest takes after this restore, from its
    /// bridge's pool on the restoring node. The handler flushes `eth0`'s
    /// addresses and applies this one, its default route, and its resolvers,
    /// so forks never share an address. Absent for gated and loopback-only
    /// guests, which have no NIC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest_addr: Option<GuestAddr>,
    /// The guest ports a gated guest serves ingress on after this restore
    /// (docs/network-handlers.md, "Ingress into a sandbox"). For each port
    /// P, the guest agent listens on vsock port P and relays every
    /// connection the node datapath opens to the workload's `127.0.0.1:P`.
    /// A restored guest keeps the ports it was captured with, so this
    /// normally repeats them; a port the job added while the instance was
    /// suspended is new here, and the agent starts listening on it. Empty
    /// for TAP and loopback-only guests, and on the boot identity (the init
    /// script hands the agent its first ports).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ingress_ports: Vec<u16>,
    /// The upstreams a gated guest reaches through the node datapath after
    /// this restore (`connect.upstreams`). For each, the guest agent
    /// listens on `127.0.0.1:<port>` (and on the VIP, so `<key>.svc.legion`
    /// works) and relays every connection to the host vsock port where the
    /// restoring allocation's upstream listener is. Like
    /// [`Self::ingress_ports`], a restored guest keeps the loopback ports it
    /// was captured with; what moves is the vsock port behind each, and the
    /// agent retargets the listeners it already has. Empty for TAP and
    /// loopback-only guests and on the boot identity.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub upstreams: Vec<GuestUpstream>,
}

/// One upstream a gated guest's agent relays to the node datapath.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuestUpstream {
    /// The loopback port the workload dials (`LEGION_UPSTREAM_<KEY>` is
    /// `127.0.0.1:<port>`). It names the upstream for the life of the
    /// guest: a restore never changes it.
    pub port: u16,
    /// The host vsock port the agent relays the upstream to: the current
    /// allocation's local port for it. Absent means `port`, which is always
    /// so at boot; a guest restored into an allocation that was assigned
    /// another local port carries it here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vsock_port: Option<u16>,
    /// The upstream's VIP and canonical port (`<key>.svc.legion` resolves
    /// to the VIP), which the agent also listens on. Absent when the
    /// allocation has no VIP block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vip: Option<std::net::SocketAddrV4>,
}

impl GuestUpstream {
    /// The host vsock port the agent dials for this upstream.
    pub fn host_port(&self) -> u16 {
        self.vsock_port.unwrap_or(self.port)
    }

    /// The `--upstream` argument form: `PORT[@VSOCKPORT][=VIP:VIPPORT]`,
    /// with `@VSOCKPORT` only when it differs from `PORT`.
    pub fn to_arg(&self) -> String {
        let mut arg = self.port.to_string();
        if self.host_port() != self.port {
            arg.push_str(&format!("@{}", self.host_port()));
        }
        if let Some(vip) = self.vip {
            arg.push_str(&format!("={vip}"));
        }
        arg
    }

    /// Parse [`Self::to_arg`]'s form.
    pub fn parse_arg(raw: &str) -> Result<GuestUpstream> {
        let (ports, vip) = match raw.split_once('=') {
            Some((ports, vip)) => (ports, Some(vip)),
            None => (raw, None),
        };
        let (port, vsock) = match ports.split_once('@') {
            Some((port, vsock)) => (port, Some(vsock)),
            None => (ports, None),
        };
        let port = parse_upstream_port(raw, port)?;
        let vsock_port = vsock
            .map(|vsock| parse_upstream_port(raw, vsock))
            .transpose()?
            .filter(|vsock| *vsock != port);
        let vip = vip
            .map(|vip| {
                vip.trim()
                    .parse::<std::net::SocketAddrV4>()
                    .with_context(|| {
                        format!("--upstream {raw:?}: {vip:?} is not an IPv4 address and port")
                    })
            })
            .transpose()?;
        Ok(GuestUpstream {
            port,
            vsock_port,
            vip,
        })
    }
}

fn parse_upstream_port(raw: &str, port: &str) -> Result<u16> {
    let parsed: u16 = port
        .trim()
        .parse()
        .with_context(|| format!("--upstream {raw:?}: {port:?} is not a port"))?;
    if parsed == 0 {
        bail!("--upstream {raw:?}: the port must be from 1 to 65535");
    }
    Ok(parsed)
}

/// One IPv4 address Legion leased a TAP guest from its bridge's pool
/// (docs/task-runtime-sandbox.md, "Addressing").
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuestAddr {
    /// The guest's address, dotted quad.
    pub address: String,
    /// The bridge network's prefix length (`/16`), for the guest's netmask.
    pub prefix: u8,
    /// The default route, when the bridge has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway: Option<String>,
    /// Resolvers for the guest's `/etc/resolv.conf`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dns: Vec<String>,
    /// The bridge the address belongs to, for display.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub bridge: String,
}

impl ResumeIdentity {
    /// The workload-identity tokens this identity carries: the env-delivered
    /// ones (keys not starting `LEGION_`) and the file-delivered ones. A
    /// change in these alone, with the same allocation and host time, is a
    /// token refresh rather than a restore.
    pub fn tokens_differ(&self, other: &ResumeIdentity) -> bool {
        let tokens = |identity: &ResumeIdentity| {
            identity
                .env
                .iter()
                .filter(|(key, _)| !key.starts_with("LEGION_"))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>()
        };
        tokens(self) != tokens(other) || self.files != other.files
    }
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

/// One token file inside a sandbox guest.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuestTokenFile {
    /// Absolute guest path, under `/legion/identity/`.
    pub path: String,
    /// File mode, `0o400` unless the identity's `file.mode` says otherwise.
    pub mode: u32,
    pub token: String,
}

impl std::fmt::Debug for GuestTokenFile {
    // Never print the token: these structs reach debug logs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GuestTokenFile")
            .field("path", &self.path)
            .field("mode", &format_args!("{:o}", self.mode))
            .finish_non_exhaustive()
    }
}

/// Size of the identity disk every memory-checkpointable sandbox boots with.
/// Large enough for a token and some env, small enough to write per restore.
pub const IDENTITY_DISK_BYTES: u64 = 1024 * 1024;
/// First bytes of an identity disk, so the guest agent can find it among the
/// attached devices and refuse anything else.
pub const IDENTITY_DISK_MAGIC: &[u8; 8] = b"LEGIONID";
/// Identity disk format version.
pub const IDENTITY_DISK_VERSION: u32 = 1;
const IDENTITY_HEADER_BYTES: usize = 16;

fn payload_for(identity: &ResumeIdentity) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(identity)?;
    let capacity = IDENTITY_DISK_BYTES as usize - IDENTITY_HEADER_BYTES;
    if payload.len() > capacity {
        bail!(
            "identity payload is {} bytes, the identity disk holds {capacity}",
            payload.len()
        );
    }
    Ok(payload)
}

/// Write a fresh identity disk at `path` (see the module docs for the
/// format). Written to a temp name and renamed into place, owner-only,
/// because it can carry a workload-identity token.
pub fn write_identity_disk(path: &Path, identity: &ResumeIdentity) -> Result<()> {
    let payload = payload_for(identity)?;
    let staging = path.with_extension("img.new");
    let _ = std::fs::remove_file(&staging);
    {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&staging)
            .with_context(|| format!("failed to create identity disk {}", staging.display()))?;
        file.write_all(IDENTITY_DISK_MAGIC)?;
        file.write_all(&IDENTITY_DISK_VERSION.to_le_bytes())?;
        file.write_all(&(payload.len() as u32).to_le_bytes())?;
        file.write_all(&payload)?;
        file.set_len(IDENTITY_DISK_BYTES)?;
        file.sync_all()?;
    }
    std::fs::rename(&staging, path)
        .with_context(|| format!("failed to publish identity disk {}", path.display()))?;
    Ok(())
}

/// Rewrite a running guest's identity disk in place, for a token refresh.
///
/// [`write_identity_disk`] renames a new file over the path, which a VMM
/// that already opened the old file never sees. This writes into the same
/// file (the VMM's open disk), so the guest's next uncached read of the
/// device returns the new identity. A read that races the write gets a
/// length that does not match its JSON and fails to parse, and the guest's
/// handler simply reads again on its next poll.
pub fn rewrite_identity_disk_in_place(path: &Path, identity: &ResumeIdentity) -> Result<()> {
    let payload = payload_for(identity)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open identity disk {}", path.display()))?;
    // Payload first, then the header that points at it.
    file.seek(SeekFrom::Start(IDENTITY_HEADER_BYTES as u64))?;
    file.write_all(&payload)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(IDENTITY_DISK_MAGIC)?;
    file.write_all(&IDENTITY_DISK_VERSION.to_le_bytes())?;
    file.write_all(&(payload.len() as u32).to_le_bytes())?;
    file.sync_all()?;
    Ok(())
}

/// Read an identity disk (or the block device it is attached as). The guest
/// agent's resume handler uses this; the host uses it in tests.
pub fn read_identity_disk(path: &Path) -> Result<ResumeIdentity> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut header = [0u8; IDENTITY_HEADER_BYTES];
    file.read_exact(&mut header)?;
    if &header[..8] != IDENTITY_DISK_MAGIC {
        bail!("{} is not a Legion identity disk", path.display());
    }
    let version = u32::from_le_bytes(header[8..12].try_into().expect("4 bytes"));
    if version != IDENTITY_DISK_VERSION {
        bail!(
            "identity disk {} is format {version}, this binary reads {IDENTITY_DISK_VERSION}",
            path.display()
        );
    }
    let len = u32::from_le_bytes(header[12..16].try_into().expect("4 bytes")) as usize;
    if len > IDENTITY_DISK_BYTES as usize - IDENTITY_HEADER_BYTES {
        bail!(
            "identity disk {} claims an impossible payload length",
            path.display()
        );
    }
    let mut payload = vec![0u8; len];
    file.read_exact(&mut payload)?;
    serde_json::from_slice(&payload).context("identity disk payload is not valid JSON")
}
