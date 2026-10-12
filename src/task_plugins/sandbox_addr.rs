//! Guest addresses for TAP-wired sandboxes (gated plan §6.7;
//! docs/task-runtime-sandbox.md, "Addressing").
//!
//! Every TAP guest gets its IPv4 address from a pool the worker config
//! declares per bridge (`sandbox.bridges.<name> = { subnet, gateway, range,
//! dns }`). The worker leases one at boot and passes it on the kernel
//! command line (`ip=`), and leases a fresh one for every restore and fork,
//! which the guest's resume handler applies. So forks never answer on their
//! origin's address, and no image needs a DHCP client.
//!
//! A lease is a small file in the allocation's scratch dir
//! (`guest-addr.json`). The set of leases on a node is the set of those
//! files under the worker's allocation root, so it is rebuilt from what is
//! on disk after a worker restart with no state of its own, and an
//! allocation's lease is released when its scratch dir is removed on
//! destroy. Allocating takes a process-wide lock and scans the root.
//! Pools are node-local: two nodes whose bridges are one L2 segment need
//! disjoint ranges.

use std::collections::{BTreeMap, HashSet};
use std::net::Ipv4Addr;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::prelude::*;
use crate::structs::config::SandboxBridgeConfig;

/// The worker's `sandbox.bridges`, as JSON, published into its environment
/// for the sandbox runtime (the channel every worker setting the runtime
/// reads uses, like `LEGION_SANDBOX_VMM_OVERHEAD_MIB`).
pub const BRIDGES_ENV: &str = "LEGION_SANDBOX_BRIDGES";

/// The lease file in a sandbox's scratch dir.
pub(crate) const LEASE_FILE: &str = "guest-addr.json";

/// The node attribute listing the bridges a worker declares
/// (`sandbox.bridges = ["br0"]`). A TAP sandbox is constrained on it.
pub const BRIDGES_ATTR: &str = "sandbox.bridges";

/// Serializes leasing across the sandboxes this worker starts at once.
static LEASE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Parse `a.b.c.d/n`.
fn parse_cidr(text: &str) -> R<(Ipv4Addr, u8)> {
    let (address, prefix) = text
        .trim()
        .split_once('/')
        .with_context(|| format!("{text:?} is not an IPv4 CIDR (a.b.c.d/n)"))?;
    let address: Ipv4Addr = address
        .parse()
        .with_context(|| format!("{text:?} is not an IPv4 CIDR (a.b.c.d/n)"))?;
    let prefix: u8 = prefix
        .parse()
        .ok()
        .filter(|prefix| *prefix <= 32)
        .with_context(|| format!("{text:?} has an invalid prefix length"))?;
    Ok((address, prefix))
}

fn mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}

/// One bridge's pool, parsed and checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BridgePool {
    pub bridge: String,
    /// The subnet's network address and prefix length.
    pub network: u32,
    pub prefix: u8,
    pub gateway: Option<Ipv4Addr>,
    /// The leasable range, inclusive.
    pub first: u32,
    pub last: u32,
    /// How the operator wrote the range, for messages.
    pub range_text: String,
    pub dns: Vec<Ipv4Addr>,
}

impl BridgePool {
    /// Parse and check one `sandbox.bridges.<name>` entry: the subnet and
    /// range parse, the range lies inside the subnet, and the gateway is in
    /// the subnet.
    pub(crate) fn from_config(bridge: &str, config: &SandboxBridgeConfig) -> R<Self> {
        let name = format!("sandbox.bridges.{bridge}");
        if bridge.is_empty()
            || bridge.len() > 15
            || bridge
                .chars()
                .any(|c| c == '/' || c == ':' || c.is_whitespace() || c.is_control())
        {
            bail!("{name}: {bridge:?} is not a valid interface name (1 to 15 characters)");
        }
        let (address, prefix) =
            parse_cidr(&config.subnet).map_err(|err| anyhow!("{name}.subnet: {err}"))?;
        if prefix > 30 {
            bail!(
                "{name}.subnet {} leaves no room for guests (use /30 or wider)",
                config.subnet
            );
        }
        let network = u32::from(address) & mask(prefix);
        let inside = |ip: u32| ip & mask(prefix) == network;
        let gateway = config
            .gateway
            .as_deref()
            .map(|gateway| {
                gateway
                    .trim()
                    .parse::<Ipv4Addr>()
                    .with_context(|| format!("{name}.gateway {gateway:?} is not IPv4"))
            })
            .transpose()?;
        if let Some(gateway) = gateway {
            if !inside(u32::from(gateway)) {
                bail!(
                    "{name}.gateway {gateway} is outside subnet {}",
                    config.subnet
                );
            }
        }
        let range_text = config
            .range
            .clone()
            .unwrap_or_else(|| config.subnet.clone());
        let (range_address, range_prefix) =
            parse_cidr(&range_text).map_err(|err| anyhow!("{name}.range: {err}"))?;
        if range_prefix < prefix {
            bail!(
                "{name}.range {range_text} is wider than subnet {}",
                config.subnet
            );
        }
        let range_start = u32::from(range_address) & mask(range_prefix);
        if !inside(range_start) {
            bail!(
                "{name}.range {range_text} is outside subnet {}",
                config.subnet
            );
        }
        let range_end = range_start | !mask(range_prefix);
        let dns = if config.dns.is_empty() {
            gateway.into_iter().collect()
        } else {
            config
                .dns
                .iter()
                .map(|server| {
                    server
                        .trim()
                        .parse::<Ipv4Addr>()
                        .with_context(|| format!("{name}.dns {server:?} is not IPv4"))
                })
                .collect::<R<Vec<_>>>()?
        };
        Ok(BridgePool {
            bridge: bridge.to_string(),
            network,
            prefix,
            gateway,
            first: range_start,
            last: range_end,
            range_text,
            dns,
        })
    }

    /// Whether `ip` can be leased: inside the range, and not the subnet's
    /// network or broadcast address, the gateway, or a resolver.
    fn usable(&self, ip: u32) -> bool {
        let broadcast = self.network | !mask(self.prefix);
        (self.first..=self.last).contains(&ip)
            && ip != self.network
            && ip != broadcast
            && self.gateway.map(u32::from) != Some(ip)
            && !self.dns.iter().any(|server| u32::from(*server) == ip)
    }

    fn usable_count(&self) -> u64 {
        (self.first..=self.last)
            .filter(|ip| self.usable(*ip))
            .count() as u64
    }
}

/// Check a worker type's `sandbox.bridges` at manager start, so a typo
/// fails there rather than at a sandbox's first boot.
pub fn validate_bridges(bridges: &BTreeMap<String, SandboxBridgeConfig>) -> R<()> {
    for (bridge, config) in bridges {
        BridgePool::from_config(bridge, config)?;
    }
    Ok(())
}

/// The pool for `bridge` on this worker (its `sandbox.bridges`, published
/// in [`BRIDGES_ENV`]), or a named failure: a TAP guest on a bridge with no
/// pool would boot with no address.
pub(crate) fn pool_for(bridge: &str) -> R<BridgePool> {
    pool_from(std::env::var(BRIDGES_ENV).ok().as_deref(), bridge)
}

/// [`pool_for`] against the published JSON.
fn pool_from(published: Option<&str>, bridge: &str) -> R<BridgePool> {
    let bridges: BTreeMap<String, SandboxBridgeConfig> =
        match published.map(str::trim).filter(|raw| !raw.is_empty()) {
            Some(raw) => {
                serde_json::from_str(raw).with_context(|| format!("{BRIDGES_ENV} is not valid"))?
            }
            None => BTreeMap::new(),
        };
    let config = bridges.get(bridge).with_context(|| {
        format!(
            "sandbox needs bridge {bridge}'s address pool, and this worker's config declares no \
             sandbox.bridges.{bridge}\nHint: add sandbox.bridges.{bridge} = {{ subnet, gateway, \
             range }} to the worker config"
        )
    })?;
    BridgePool::from_config(bridge, config)
}

/// One leased address, as the lease file and the identity disk carry it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GuestLease {
    pub bridge: String,
    pub address: Ipv4Addr,
    pub prefix: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway: Option<Ipv4Addr>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dns: Vec<Ipv4Addr>,
}

impl GuestLease {
    /// The kernel's IP autoconfiguration argument that brings `eth0` up
    /// with this address at boot (`CONFIG_IP_PNP`):
    /// `ip=<addr>::<gateway>:<netmask>:<hostname>:eth0:off`.
    pub(crate) fn kernel_ip_arg(&self, hostname: &str) -> String {
        let netmask = Ipv4Addr::from(mask(self.prefix));
        let gateway = self.gateway.map(|ip| ip.to_string()).unwrap_or_default();
        format!(
            "ip={}::{gateway}:{netmask}:{hostname}:{}:off",
            self.address,
            crate::task_plugins::microvm::GUEST_NIC_ID
        )
    }

    /// What the guest's resume handler applies after a restore.
    pub(crate) fn guest_addr(&self) -> crate::structs::checkpoint::GuestAddr {
        crate::structs::checkpoint::GuestAddr {
            address: self.address.to_string(),
            prefix: self.prefix,
            gateway: self.gateway.map(|ip| ip.to_string()),
            dns: self.dns.iter().map(ToString::to_string).collect(),
            bridge: self.bridge.clone(),
        }
    }
}

/// The lease the guest in `scratch` holds, if any.
pub(crate) fn read_lease(scratch: &Path) -> Option<GuestLease> {
    serde_json::from_slice(&std::fs::read(scratch.join(LEASE_FILE)).ok()?).ok()
}

/// Addresses on `bridge` that other allocations under `alloc_root` hold.
fn leased_addresses(alloc_root: Option<&Path>, bridge: &str, own: &Path) -> HashSet<u32> {
    let Some(root) = alloc_root else {
        return HashSet::new();
    };
    let Ok(entries) = std::fs::read_dir(root) else {
        return HashSet::new();
    };
    entries
        .flatten()
        .map(|entry| entry.path().join(".sandbox"))
        .filter(|scratch| scratch != own)
        .filter_map(|scratch| read_lease(&scratch))
        .filter(|lease| lease.bridge == bridge)
        .map(|lease| u32::from(lease.address))
        .collect()
}

/// Lease an address on `pool` for the guest whose scratch dir is `scratch`.
///
/// The allocation's own lease wins (a restart keeps its address). Then
/// `prefer` (a resume asks for the address its checkpoint recorded), when it
/// is still free. Then the lowest free address. `alloc_root` is the
/// worker's allocation root, scanned for every other lease on the node.
pub(crate) fn lease(
    pool: &BridgePool,
    scratch: &Path,
    alloc_root: Option<&Path>,
    prefer: Option<Ipv4Addr>,
    node: &str,
) -> R<GuestLease> {
    let _guard = LEASE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = read_lease(scratch) {
        if existing.bridge == pool.bridge && pool.usable(u32::from(existing.address)) {
            return Ok(existing);
        }
    }
    let used = leased_addresses(alloc_root, &pool.bridge, scratch);
    let free = |ip: u32| pool.usable(ip) && !used.contains(&ip);
    let address = prefer
        .map(u32::from)
        .filter(|ip| free(*ip))
        .or_else(|| (pool.first..=pool.last).find(|ip| free(*ip)))
        .with_context(|| {
            format!(
                "bridge {}'s sandbox range {} is full ({} addresses leased on node {node})",
                pool.bridge,
                pool.range_text,
                pool.usable_count()
            )
        })?;
    let lease = GuestLease {
        bridge: pool.bridge.clone(),
        address: Ipv4Addr::from(address),
        prefix: pool.prefix,
        gateway: pool.gateway,
        dns: pool.dns.clone(),
    };
    std::fs::create_dir_all(scratch)?;
    std::fs::write(scratch.join(LEASE_FILE), serde_json::to_vec_pretty(&lease)?)
        .with_context(|| format!("failed to record guest address in {}", scratch.display()))?;
    Ok(lease)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(range: Option<&str>) -> BridgePool {
        BridgePool::from_config(
            "br0",
            &SandboxBridgeConfig {
                subnet: "10.88.0.0/16".into(),
                gateway: Some("10.88.0.1".into()),
                range: range.map(str::to_string),
                dns: Vec::new(),
            },
        )
        .unwrap()
    }

    #[test]
    fn pools_are_checked_when_the_worker_config_loads() {
        let ok = pool(Some("10.88.128.0/17"));
        assert_eq!(Ipv4Addr::from(ok.first), Ipv4Addr::new(10, 88, 128, 0));
        assert_eq!(Ipv4Addr::from(ok.last), Ipv4Addr::new(10, 88, 255, 255));
        assert_eq!(
            ok.dns,
            vec![Ipv4Addr::new(10, 88, 0, 1)],
            "dns defaults to the gateway"
        );
        let bad = |config: SandboxBridgeConfig| {
            BridgePool::from_config("br0", &config)
                .unwrap_err()
                .to_string()
        };
        let base = SandboxBridgeConfig {
            subnet: "10.88.0.0/16".into(),
            gateway: Some("10.88.0.1".into()),
            range: None,
            dns: Vec::new(),
        };
        assert!(bad(SandboxBridgeConfig {
            range: Some("10.99.0.0/24".into()),
            ..base.clone()
        })
        .contains("outside subnet"));
        assert!(bad(SandboxBridgeConfig {
            range: Some("10.0.0.0/8".into()),
            ..base.clone()
        })
        .contains("wider than subnet"));
        assert!(bad(SandboxBridgeConfig {
            gateway: Some("192.168.1.1".into()),
            ..base.clone()
        })
        .contains("outside subnet"));
        assert!(bad(SandboxBridgeConfig {
            subnet: "10.88.0.0".into(),
            ..base.clone()
        })
        .contains("not an IPv4 CIDR"));
        assert!(BridgePool::from_config("a-bridge-name-too-long", &base).is_err());
    }

    /// Gated plan §9, Phase 4: allocate, prefer the previous address, keep
    /// a restart's own lease, release on destroy (the scratch dir goes),
    /// rebuild from disk, and fail by name when the range is full.
    #[test]
    fn leases_are_per_allocation_files_rebuilt_from_disk() -> R<()> {
        let root = tempfile::tempdir()?;
        let scratch = |id: i64| root.path().join(id.to_string()).join(".sandbox");
        let pool = pool(Some("10.88.128.0/30"));
        // .0 is in range but not the subnet's network address; .1-.3 are
        // all usable here (the gateway is outside the range).
        let a = lease(&pool, &scratch(1), Some(root.path()), None, "w-03")?;
        assert_eq!(a.address, Ipv4Addr::new(10, 88, 128, 0));
        let b = lease(&pool, &scratch(2), Some(root.path()), None, "w-03")?;
        assert_eq!(b.address, Ipv4Addr::new(10, 88, 128, 1));
        // A restart of allocation 1 keeps its address.
        assert_eq!(
            lease(&pool, &scratch(1), Some(root.path()), None, "w-03")?,
            a
        );
        // A resume prefers what its checkpoint recorded, when free...
        let c = lease(
            &pool,
            &scratch(3),
            Some(root.path()),
            Some(Ipv4Addr::new(10, 88, 128, 3)),
            "w-03",
        )?;
        assert_eq!(c.address, Ipv4Addr::new(10, 88, 128, 3));
        // ...and takes the next free one when it is not.
        let d = lease(
            &pool,
            &scratch(4),
            Some(root.path()),
            Some(Ipv4Addr::new(10, 88, 128, 1)),
            "w-03",
        )?;
        assert_eq!(d.address, Ipv4Addr::new(10, 88, 128, 2));
        let err = lease(&pool, &scratch(5), Some(root.path()), None, "w-03").unwrap_err();
        assert_eq!(
            err.to_string(),
            "bridge br0's sandbox range 10.88.128.0/30 is full (4 addresses leased on node w-03)"
        );
        // Destroy removes the scratch dir, which is the release.
        std::fs::remove_dir_all(root.path().join("2"))?;
        let e = lease(&pool, &scratch(5), Some(root.path()), None, "w-03")?;
        assert_eq!(e.address, Ipv4Addr::new(10, 88, 128, 1));
        assert_eq!(read_lease(&scratch(5)), Some(e));
        Ok(())
    }

    /// The worker publishes its `sandbox.bridges` as JSON, and the runtime
    /// finds a bridge's pool there, or fails the start by name.
    #[test]
    fn the_runtime_reads_the_pools_the_worker_publishes() -> R<()> {
        let published = serde_json::to_string(&BTreeMap::from([(
            "br0".to_string(),
            SandboxBridgeConfig {
                subnet: "10.88.0.0/16".into(),
                gateway: Some("10.88.0.1".into()),
                range: Some("10.88.128.0/17".into()),
                dns: vec!["1.1.1.1".into()],
            },
        )]))?;
        let pool = pool_from(Some(&published), "br0")?;
        assert_eq!(pool.prefix, 16);
        assert_eq!(pool.dns, vec![Ipv4Addr::new(1, 1, 1, 1)]);
        let missing = pool_from(Some(&published), "br1").unwrap_err().to_string();
        assert!(
            missing.starts_with(
                "sandbox needs bridge br1's address pool, and this worker's config declares no \
                 sandbox.bridges.br1"
            ),
            "{missing}"
        );
        assert!(pool_from(None, "br0").is_err());
        Ok(())
    }

    #[test]
    fn the_gateway_resolvers_and_subnet_edges_are_never_leased() {
        let pool = pool(None);
        assert!(!pool.usable(u32::from(Ipv4Addr::new(10, 88, 0, 0))));
        assert!(!pool.usable(u32::from(Ipv4Addr::new(10, 88, 0, 1))));
        assert!(!pool.usable(u32::from(Ipv4Addr::new(10, 88, 255, 255))));
        assert!(pool.usable(u32::from(Ipv4Addr::new(10, 88, 0, 2))));
    }

    #[test]
    fn the_kernel_brings_eth0_up_with_the_lease() {
        let lease = GuestLease {
            bridge: "br0".into(),
            address: Ipv4Addr::new(10, 88, 128, 7),
            prefix: 16,
            gateway: Some(Ipv4Addr::new(10, 88, 0, 1)),
            dns: vec![Ipv4Addr::new(10, 88, 0, 1)],
        };
        assert_eq!(
            lease.kernel_ip_arg("legion-sbx-7"),
            "ip=10.88.128.7::10.88.0.1:255.255.0.0:legion-sbx-7:eth0:off"
        );
        let addr = lease.guest_addr();
        assert_eq!(addr.address, "10.88.128.7");
        assert_eq!(addr.gateway.as_deref(), Some("10.88.0.1"));
        assert_eq!(addr.dns, vec!["10.88.0.1".to_string()]);
    }
}
