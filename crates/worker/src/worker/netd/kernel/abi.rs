//! The structs the eBPF programs share with the worker, field for field as
//! `bpf/legion_net.h` declares them. Addresses are network byte order; a
//! port is network order unless a field says otherwise. A size test pins
//! each layout, so a change to one side without the other fails the build.

pub const MAX_RULES: usize = 64;
pub const MAX_ALLOW: usize = 32;

pub const A_GUARD: u32 = 1 << 0;
pub const A_ENFORCE: u32 = 1 << 1;
pub const A_GATED: u32 = 1 << 2;
pub const A_CAPTURE: u32 = 1 << 3;
pub const A_DNS: u32 = 1 << 4;
pub const A_INSPECT: u32 = 1 << 5;
pub const A_DEFAULT_ALLOW: u32 = 1 << 6;

pub const R_PORT: u8 = 1 << 0;
pub const R_CIDR: u8 = 1 << 1;
pub const R_HOST: u8 = 1 << 2;
pub const R_DEFER: u8 = 1 << 3;

pub const DENY: u8 = 0;
pub const ALLOW: u8 = 1;

pub const EV_METADATA: u32 = 1;
pub const EV_DENY_LIST: u32 = 2;
pub const EV_CONTROL: u32 = 3;
pub const EV_DATAPATH: u32 = 4;
pub const EV_RULE: u32 = 16;
pub const EV_FALLTHROUGH: u32 = 17;
pub const EV_NEEDS_NETD: u32 = 18;
pub const EV_RAW_SOCKET: u32 = 19;
pub const EV_REDIRECT: u32 = 20;

/// The `index` of an `EV_NEEDS_NETD` event for a host rule whose address no
/// resolver answer taught the kernel.
pub const INDEX_UNLEARNED: u32 = 255;

pub const SLOT_FALLTHROUGH: u32 = 64;
pub const SLOT_GUARD: u32 = 65;
pub const SLOT_NEEDS_NETD: u32 = 66;
pub const SLOT_REDIRECT: u32 = 67;
pub const SLOT_DNS: u32 = 68;

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Config {
    pub leaf_level: u32,
    pub flags: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Alloc {
    pub allocation_id: i64,
    pub flags: u32,
    pub vip_block: u32,
    pub resolver4: u32,
    pub capture4: u32,
    pub capture_port: u16,
    pub nrules: u16,
    pub generation: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Rule {
    pub addr: [u32; 4],
    pub port: u16,
    pub flags: u8,
    pub prefix: u8,
    pub action: u8,
    pub family: u8,
    pub host_bit: u8,
    pub pad: u8,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rules {
    pub rule: [Rule; MAX_RULES],
}

impl Default for Rules {
    fn default() -> Self {
        Rules {
            rule: [Rule::default(); MAX_RULES],
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct LearnedKey {
    pub leaf: u64,
    pub addr: [u32; 4],
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Learned {
    pub bits: u64,
    pub expires_ns: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Allow {
    pub addr: [u32; 4],
    pub port: u16,
    pub prefix: u8,
    pub family: u8,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct LocalKey {
    pub addr: [u32; 4],
    pub family: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct OwnPort {
    pub leaf: u64,
    pub port: u32,
    pub pad: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct CounterKey {
    pub leaf: u64,
    pub slot: u32,
    pub verdict: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Orig {
    pub addr: [u32; 4],
    pub port: u16,
    pub family: u8,
    pub pad: u8,
    pub pad2: u32,
    pub allocation_id: i64,
    pub netns: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Endpoint {
    pub netns: u64,
    pub addr: [u32; 4],
    /// Host byte order, as sock_ops reports it.
    pub port: u32,
    pub pad: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Bytes {
    pub tx: u64,
    pub rx: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Event {
    pub allocation_id: i64,
    pub kind: u32,
    pub index: u32,
    pub addr: [u32; 4],
    pub port: u16,
    pub family: u8,
    pub protocol: u8,
    pub pid: u32,
    pub generation: u32,
}

impl Event {
    /// Decode one ring-buffer record. `None` for a record of the wrong size
    /// (an object from another build).
    pub fn from_bytes(bytes: &[u8]) -> Option<Event> {
        if bytes.len() < std::mem::size_of::<Event>() {
            return None;
        }
        // SAFETY: `Event` is `repr(C)` plain data with no invalid bit
        // patterns, and the length was checked; the read is unaligned-safe.
        Some(unsafe { std::ptr::read_unaligned(bytes.as_ptr().cast::<Event>()) })
    }

    pub fn destination(&self) -> std::net::SocketAddr {
        socket_addr(&self.addr, self.family, self.port)
    }
}

/// An address from its network-order words.
pub fn ip_from_words(addr: &[u32; 4], family: u8) -> std::net::IpAddr {
    if family == 6 {
        let mut octets = [0u8; 16];
        for (i, word) in addr.iter().enumerate() {
            octets[i * 4..i * 4 + 4].copy_from_slice(&word.to_ne_bytes());
        }
        std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets))
    } else {
        std::net::IpAddr::V4(std::net::Ipv4Addr::from(addr[0].to_ne_bytes()))
    }
}

/// Network-order words (and the family, 4 or 6) of an address. An
/// IPv4-mapped IPv6 address is IPv4, as the programs judge it.
pub fn words_from_ip(ip: std::net::IpAddr) -> ([u32; 4], u8) {
    match ip {
        std::net::IpAddr::V4(v4) => ([u32::from_ne_bytes(v4.octets()), 0, 0, 0], 4),
        std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => words_from_ip(std::net::IpAddr::V4(v4)),
            None => {
                let octets = v6.octets();
                let mut words = [0u32; 4];
                for (i, word) in words.iter_mut().enumerate() {
                    *word = u32::from_ne_bytes(octets[i * 4..i * 4 + 4].try_into().unwrap());
                }
                (words, 6)
            }
        },
    }
}

/// A network-order port as the programs store it.
pub fn port_ne(port: u16) -> u16 {
    port.to_be()
}

pub fn socket_addr(addr: &[u32; 4], family: u8, port_ne: u16) -> std::net::SocketAddr {
    std::net::SocketAddr::new(ip_from_words(addr, family), u16::from_be(port_ne))
}

#[cfg(target_os = "linux")]
mod pod {
    use super::*;
    // SAFETY: each is `repr(C)`, `Copy`, and made only of integers, so
    // every bit pattern is valid and there is no padding the kernel could
    // leave uninitialised that Rust would read as a niche.
    unsafe impl aya::Pod for Config {}
    unsafe impl aya::Pod for Alloc {}
    unsafe impl aya::Pod for Rule {}
    unsafe impl aya::Pod for Rules {}
    unsafe impl aya::Pod for LearnedKey {}
    unsafe impl aya::Pod for Learned {}
    unsafe impl aya::Pod for Allow {}
    unsafe impl aya::Pod for LocalKey {}
    unsafe impl aya::Pod for OwnPort {}
    unsafe impl aya::Pod for CounterKey {}
    unsafe impl aya::Pod for Orig {}
    unsafe impl aya::Pod for Endpoint {}
    unsafe impl aya::Pod for Bytes {}
}

#[cfg(test)]
mod abi_test {
    use super::*;
    use std::mem::size_of;

    /// The sizes `bpf/legion_net.h` gives these structs (clang, bpf target).
    #[test]
    fn layouts_match_the_c_header_test() {
        assert_eq!(size_of::<Config>(), 8);
        assert_eq!(size_of::<Alloc>(), 32);
        assert_eq!(size_of::<Rule>(), 24);
        assert_eq!(size_of::<Rules>(), 24 * MAX_RULES);
        assert_eq!(size_of::<LearnedKey>(), 24);
        assert_eq!(size_of::<Learned>(), 16);
        assert_eq!(size_of::<Allow>(), 20);
        assert_eq!(size_of::<LocalKey>(), 20);
        assert_eq!(size_of::<OwnPort>(), 16);
        assert_eq!(size_of::<CounterKey>(), 16);
        assert_eq!(size_of::<Orig>(), 40);
        assert_eq!(size_of::<Endpoint>(), 32);
        assert_eq!(size_of::<Bytes>(), 16);
        assert_eq!(size_of::<Event>(), 48);
    }

    #[test]
    fn addresses_round_trip_test() {
        for ip in ["10.1.2.3", "2001:db8::7", "::ffff:192.0.2.1"] {
            let ip: std::net::IpAddr = ip.parse().unwrap();
            let (words, family) = words_from_ip(ip);
            let back = ip_from_words(&words, family);
            match ip {
                std::net::IpAddr::V6(v6) if v6.to_ipv4_mapped().is_some() => {
                    assert_eq!(back, std::net::IpAddr::V4(v6.to_ipv4_mapped().unwrap()))
                }
                _ => assert_eq!(back, ip),
            }
        }
        let addr = socket_addr(
            &words_from_ip("10.0.0.1".parse().unwrap()).0,
            4,
            port_ne(443),
        );
        assert_eq!(addr.to_string(), "10.0.0.1:443");
    }
}
