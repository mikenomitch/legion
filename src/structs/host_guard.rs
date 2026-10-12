//! The host guard: a fixed floor of destinations no allocation may reach on
//! its own (worker-plane-auth plan §6.11; docs/network-handlers.md, "The host
//! guard").
//!
//! It sits *below* policy. A job's rules and stored policy objects can't
//! widen it; only the node's operator can, one `ip:port` at a time, in the
//! worker config (`hostGuard.allow`). What it denies:
//!
//! | Class | Addresses | Why |
//! | --- | --- | --- |
//! | loopback | `127.0.0.0/8`, `::1` | the node's own services: Legion's ports, other allocations' upstreams |
//! | unspecified | `0.0.0.0/8`, `::` | the same services by another name |
//! | node address | every address on the node's interfaces | the same services by the node's IP |
//! | link-local | `169.254.0.0/16`, `fe80::/10` | AWS, GCP, Azure, OCI, DigitalOcean metadata; ECS task metadata; the VPC resolver |
//! | cloud metadata | `fd00:ec2::254`, `100.100.100.200` | AWS IPv6 metadata, Alibaba Cloud metadata |
//! | operator deny | `hostGuard.deny` CIDRs | extra destinations to treat like metadata |
//!
//! IPv4-mapped IPv6 addresses (`::ffff:127.0.0.1`) are judged as the IPv4
//! address they map. The classification lives here, outside the worker, so
//! the manager's `TestNetPolicy` explains a host-guard denial with the same
//! words the datapath uses.

use crate::prelude::*;
use crate::structs::netpolicy::{parse_cidr, Cidr};
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::RwLock;

/// The verdict source a host-guard denial carries (`Legion-Denied-By`,
/// `net.denied` events, `legion net policy test`).
pub const HOST_GUARD_SOURCE: &str = "host-guard";

/// AWS's IPv6 instance metadata endpoint.
pub const AWS_METADATA_V6: Ipv6Addr = Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x254);
/// Alibaba Cloud's instance metadata endpoint.
pub const ALIBABA_METADATA_V4: Ipv4Addr = Ipv4Addr::new(100, 100, 100, 200);

/// Worker config: `hostGuard = { allow = [...], deny = [...] }`.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostGuardConfig {
    /// Holes in the floor, each one service: `ip:port` or `cidr:port`
    /// (`10.0.1.7:9100`, `10.0.0.0/24:9100`, `[fd00::7]:9100`). Never a bare
    /// address or range, so an override opens one service and not the node.
    #[serde(default)]
    pub allow: Vec<String>,
    /// Extra destinations to deny like cloud metadata: IPs or CIDRs.
    #[serde(default)]
    pub deny: Vec<String>,
}

/// Why the guard refused an address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardClass {
    Loopback,
    Unspecified,
    NodeAddress,
    LinkLocal,
    CloudMetadata,
    OperatorDeny,
}

impl GuardClass {
    /// The class's short name (`resolves to 169.254.169.254 (link-local)`).
    pub fn short(self) -> &'static str {
        match self {
            GuardClass::Loopback => "loopback",
            GuardClass::Unspecified => "unspecified",
            GuardClass::NodeAddress => "this node",
            GuardClass::LinkLocal => "link-local",
            GuardClass::CloudMetadata => "cloud metadata",
            GuardClass::OperatorDeny => "hostGuard.deny",
        }
    }

    /// How a denial names the class (`169.254.169.254 is <this>`).
    pub fn describe(self) -> &'static str {
        match self {
            GuardClass::Loopback => "loopback",
            GuardClass::Unspecified => "the unspecified address",
            GuardClass::NodeAddress => "an address of this node",
            GuardClass::LinkLocal => "link-local (cloud metadata)",
            GuardClass::CloudMetadata => "cloud metadata",
            GuardClass::OperatorDeny => "denied by this node's hostGuard.deny",
        }
    }
}

/// A host-guard refusal: the address and its class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardDenial {
    pub ip: IpAddr,
    pub class: GuardClass,
}

impl GuardDenial {
    /// The `Legion-Denied-Reason` text. `name` is the hostname the caller
    /// asked for, when it was not the address itself.
    pub fn reason(&self, name: Option<&str>) -> String {
        match name.filter(|name| {
            name.parse::<IpAddr>()
                .map(|ip| ip != self.ip)
                .unwrap_or(true)
        }) {
            Some(name) => format!(
                "{name} resolves to {} ({}); allocations may not reach it",
                self.ip,
                self.class.short()
            ),
            None => format!(
                "{} is {}; allocations may not reach it",
                self.ip,
                self.class.describe()
            ),
        }
    }

    /// The hint that goes with a denial.
    pub fn hint() -> &'static str {
        "the host guard is set on the worker (hostGuard.allow), not in job policy"
    }
}

/// `::ffff:a.b.c.d` judged as `a.b.c.d`.
pub fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        v4 => v4,
    }
}

/// The built-in class of an address, ignoring node addresses and operator
/// settings.
pub fn builtin_class(ip: IpAddr) -> Option<GuardClass> {
    match canonical(ip) {
        IpAddr::V4(v4) => {
            if v4.is_loopback() {
                Some(GuardClass::Loopback)
            } else if v4.octets()[0] == 0 {
                Some(GuardClass::Unspecified)
            } else if v4.is_link_local() {
                Some(GuardClass::LinkLocal)
            } else if v4 == ALIBABA_METADATA_V4 {
                Some(GuardClass::CloudMetadata)
            } else {
                None
            }
        }
        IpAddr::V6(v6) => {
            if v6.is_loopback() {
                Some(GuardClass::Loopback)
            } else if v6.is_unspecified() {
                Some(GuardClass::Unspecified)
            } else if (v6.segments()[0] & 0xffc0) == 0xfe80 {
                Some(GuardClass::LinkLocal)
            } else if v6 == AWS_METADATA_V6 {
                Some(GuardClass::CloudMetadata)
            } else {
                None
            }
        }
    }
}

/// One `hostGuard.allow` entry: an address or range, and one port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuardAllow {
    pub cidr: Cidr,
    pub port: u16,
}

/// Parses `ip:port`, `cidr:port`, `[ipv6]:port`, or `[ipv6/len]:port`.
pub fn parse_allow(entry: &str) -> R<GuardAllow> {
    let entry = entry.trim();
    let (addr, port) = if let Some(rest) = entry.strip_prefix('[') {
        let (addr, tail) = rest
            .split_once(']')
            .ok_or_else(|| anyhow!("hostGuard.allow entry {entry:?} has an unclosed '['"))?;
        let port = tail.strip_prefix(':').ok_or_else(|| {
            anyhow!("hostGuard.allow entry {entry:?} needs a port: [address]:port")
        })?;
        (addr, port)
    } else {
        let (addr, port) = entry.rsplit_once(':').ok_or_else(|| {
            anyhow!(
                "hostGuard.allow entry {entry:?} needs a port (ip:port or cidr:port); an \
                 override opens one service, never a whole address or range"
            )
        })?;
        if addr.contains(':') {
            bail!("hostGuard.allow entry {entry:?}: write IPv6 addresses as [address]:port");
        }
        (addr, port)
    };
    let port: u16 = port
        .parse()
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| anyhow!("hostGuard.allow entry {entry:?} has an invalid port"))?;
    let cidr = parse_cidr(addr).with_context(|| format!("hostGuard.allow entry {entry:?}"))?;
    Ok(GuardAllow { cidr, port })
}

/// The guard one worker applies.
#[derive(Debug, Default)]
pub struct HostGuard {
    allow: Vec<GuardAllow>,
    deny: Vec<Cidr>,
    node_addrs: RwLock<Vec<IpAddr>>,
}

impl HostGuard {
    /// The guard for a worker config. Invalid entries fail.
    pub fn new(config: &HostGuardConfig) -> R<Self> {
        let allow = config
            .allow
            .iter()
            .map(|entry| parse_allow(entry))
            .collect::<R<Vec<_>>>()?;
        let deny = config
            .deny
            .iter()
            .map(|entry| {
                parse_cidr(entry).with_context(|| format!("hostGuard.deny entry {entry:?}"))
            })
            .collect::<R<Vec<_>>>()?;
        Ok(HostGuard {
            allow,
            deny,
            node_addrs: RwLock::new(Vec::new()),
        })
    }

    /// Replaces the node's own addresses (refreshed as interfaces change).
    pub fn set_node_addrs(&self, addrs: Vec<IpAddr>) {
        let addrs: Vec<IpAddr> = addrs.into_iter().map(canonical).collect();
        *self.node_addrs.write().unwrap_or_else(|p| p.into_inner()) = addrs;
    }

    pub fn node_addrs(&self) -> Vec<IpAddr> {
        self.node_addrs
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// The class that denies `ip`, before any `allow` entry is applied.
    pub fn classify(&self, ip: IpAddr) -> Option<GuardClass> {
        let ip = canonical(ip);
        if let Some(class) = builtin_class(ip) {
            return Some(class);
        }
        if self
            .node_addrs
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .contains(&ip)
        {
            return Some(GuardClass::NodeAddress);
        }
        if self.deny.iter().any(|cidr| cidr.contains(ip)) {
            return Some(GuardClass::OperatorDeny);
        }
        None
    }

    /// The guard's verdict for one destination. An `allow` entry naming the
    /// exact port opens a guarded address; nothing else does.
    pub fn judge(&self, ip: IpAddr, port: u16) -> Result<(), GuardDenial> {
        let ip = canonical(ip);
        let Some(class) = self.classify(ip) else {
            return Ok(());
        };
        if self
            .allow
            .iter()
            .any(|allow| allow.port == port && allow.cidr.contains(ip))
        {
            return Ok(());
        }
        Err(GuardDenial { ip, class })
    }
}

/// Every address on this host's interfaces, for the node-address class.
#[cfg(unix)]
pub fn interface_addresses() -> Vec<IpAddr> {
    let mut out = Vec::new();
    let mut addrs: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: getifaddrs allocates a linked list we walk read-only and free
    // with freeifaddrs exactly once.
    unsafe {
        if libc::getifaddrs(&mut addrs) != 0 {
            return out;
        }
        let mut cursor = addrs;
        while !cursor.is_null() {
            let entry = &*cursor;
            if !entry.ifa_addr.is_null() {
                match i32::from((*entry.ifa_addr).sa_family) {
                    libc::AF_INET => {
                        let sin = &*(entry.ifa_addr as *const libc::sockaddr_in);
                        out.push(IpAddr::V4(Ipv4Addr::from(u32::from_be(
                            sin.sin_addr.s_addr,
                        ))));
                    }
                    libc::AF_INET6 => {
                        let sin6 = &*(entry.ifa_addr as *const libc::sockaddr_in6);
                        out.push(IpAddr::V6(Ipv6Addr::from(sin6.sin6_addr.s6_addr)));
                    }
                    _ => {}
                }
            }
            cursor = entry.ifa_next;
        }
        libc::freeifaddrs(addrs);
    }
    out.sort();
    out.dedup();
    out
}

#[cfg(not(unix))]
pub fn interface_addresses() -> Vec<IpAddr> {
    Vec::new()
}

#[cfg(test)]
mod host_guard_test {
    use super::*;

    fn ip(text: &str) -> IpAddr {
        text.parse().expect("ip")
    }

    #[test]
    fn every_default_class_is_denied_test() {
        let guard = HostGuard::new(&HostGuardConfig::default()).expect("guard");
        guard.set_node_addrs(vec![ip("10.0.1.7")]);
        for (addr, class) in [
            ("127.0.0.1", GuardClass::Loopback),
            ("127.42.7.1", GuardClass::Loopback),
            ("::1", GuardClass::Loopback),
            ("::ffff:127.0.0.1", GuardClass::Loopback),
            ("0.0.0.0", GuardClass::Unspecified),
            ("::", GuardClass::Unspecified),
            ("10.0.1.7", GuardClass::NodeAddress),
            ("169.254.169.254", GuardClass::LinkLocal),
            ("169.254.170.2", GuardClass::LinkLocal),
            ("fe80::1", GuardClass::LinkLocal),
            ("fd00:ec2::254", GuardClass::CloudMetadata),
            ("100.100.100.200", GuardClass::CloudMetadata),
        ] {
            let denial = guard.judge(ip(addr), 80).expect_err(addr);
            assert_eq!(denial.class, class, "{addr}");
        }
        for open in ["93.184.216.34", "10.0.1.8", "2606:4700::1111"] {
            assert!(guard.judge(ip(open), 443).is_ok(), "{open}");
        }
    }

    #[test]
    fn allow_opens_exactly_one_port_test() -> R<()> {
        let guard = HostGuard::new(&HostGuardConfig {
            allow: vec!["10.0.1.7:9100".into(), "[::1]:7000".into()],
            deny: vec![],
        })?;
        guard.set_node_addrs(vec![ip("10.0.1.7")]);
        assert!(guard.judge(ip("10.0.1.7"), 9100).is_ok());
        assert!(guard.judge(ip("10.0.1.7"), 9101).is_err());
        assert!(guard.judge(ip("10.0.1.7"), 4848).is_err());
        assert!(guard.judge(ip("::1"), 7000).is_ok());
        assert!(guard.judge(ip("127.0.0.1"), 7000).is_err());
        Ok(())
    }

    #[test]
    fn allow_entries_need_a_port_test() {
        for bad in [
            "10.0.1.7",
            "10.0.0.0/8",
            "::1",
            "[::1]",
            "10.0.1.7:0",
            "x:80",
        ] {
            assert!(
                parse_allow(bad).is_err(),
                "{bad} must be refused: an override opens one service"
            );
        }
        assert_eq!(
            parse_allow("10.0.0.0/24:9100").expect("cidr:port").port,
            9100
        );
    }

    #[test]
    fn operator_deny_adds_destinations_test() -> R<()> {
        let guard = HostGuard::new(&HostGuardConfig {
            allow: vec![],
            deny: vec!["192.0.2.10/32".into()],
        })?;
        assert_eq!(
            guard.judge(ip("192.0.2.10"), 80).expect_err("denied").class,
            GuardClass::OperatorDeny
        );
        assert!(guard.judge(ip("192.0.2.11"), 80).is_ok());
        Ok(())
    }

    #[test]
    fn reasons_name_the_address_and_the_name_test() {
        let denial = GuardDenial {
            ip: ip("169.254.169.254"),
            class: GuardClass::LinkLocal,
        };
        assert_eq!(
            denial.reason(None),
            "169.254.169.254 is link-local (cloud metadata); allocations may not reach it"
        );
        assert_eq!(
            denial.reason(Some("instance-data.ec2.internal")),
            "instance-data.ec2.internal resolves to 169.254.169.254 (link-local); allocations may not reach it"
        );
        assert_eq!(
            denial.reason(Some("169.254.169.254")),
            denial.reason(None),
            "an IP literal is not repeated as a name"
        );
    }

    #[cfg(unix)]
    #[test]
    fn interfaces_include_loopback_test() {
        assert!(interface_addresses().iter().any(IpAddr::is_loopback));
    }
}
