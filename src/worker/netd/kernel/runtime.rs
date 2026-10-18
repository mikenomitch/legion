//! Loading the kernel classifier and keeping its maps current (Linux).
//!
//! The programs are attached once, to the worker's cgroup parent, as BPF
//! links (multi-attach), so every allocation leaf under it inherits them and
//! other BPF users of the hierarchy are untouched. Each program looks up the
//! calling task's leaf (its ancestor at the leaf level) in `ln_allocs`; a
//! leaf the worker has not described gets the unattributed entry (key 0):
//! the guard floor, no exemptions.
//!
//! **Restart survival.** With a pin directory (`/sys/fs/bpf/legion/abi-N`),
//! maps and links are pinned. A restarted worker opens the same maps, attaches
//! fresh programs, then drops the old links, so the kernel never stops
//! enforcing the last policy it was given, and allowed flows that never
//! entered netd are untouched (plan §6.A.7). A clean shutdown removes the
//! pins. Without a pin directory, the links live as long as the worker.

use crate::prelude::*;
use crate::worker::netd::kernel::abi;
use crate::worker::netd::kernel::compile::Compiled;
use crate::worker::netd::kernel_guard::{GuardTable, METADATA_V4, METADATA_V6};
use aya::maps::lpm_trie::{Key as LpmKey, LpmTrie};
use aya::maps::{Array, HashMap as BpfHashMap, MapData, PerCpuHashMap, RingBuf};
use aya::programs::links::{FdLink, PinnedLink};
use aya::programs::{CgroupAttachMode, CgroupSkbAttachType};
use aya::{Ebpf, EbpfLoader};
use std::collections::{BTreeSet, HashMap};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Bumped whenever a map layout in `bpf/legion_net.h` changes, so a worker
/// never opens maps pinned by an incompatible build.
pub const ABI_VERSION: u32 = 1;

/// Every map the object defines, so each can be pinned and re-opened.
const MAPS: &[&str] = &[
    "ln_config",
    "ln_allocs",
    "ln_rules",
    "ln_learned",
    "ln_guard4",
    "ln_guard6",
    "ln_allow",
    "ln_local",
    "ln_control_ports",
    "ln_datapath_ports",
    "ln_own_ports",
    "ln_counters",
    "ln_pending",
    "ln_orig",
    "ln_asked",
    "ln_bytes",
    "ln_events",
];

/// Every program and how it attaches.
const PROGRAMS: &[&str] = &[
    "legion_connect4",
    "legion_connect6",
    "legion_sendmsg4",
    "legion_sendmsg6",
    "legion_recvmsg4",
    "legion_recvmsg6",
    "legion_sock_create",
    "legion_sockops",
    "legion_skb_egress",
    "legion_skb_ingress",
];

/// Where the classifier attaches and pins.
#[derive(Debug, Clone)]
pub struct KernelSettings {
    /// The cgroup v2 mount (`/sys/fs/cgroup`).
    pub cgroup_root: PathBuf,
    /// The worker's cgroup parent, relative to the root (`legion`).
    pub cgroup_parent: String,
    /// A bpffs directory to pin under, or none to keep everything in the
    /// worker's lifetime.
    pub pin_dir: Option<PathBuf>,
}

impl KernelSettings {
    fn parent_path(&self) -> PathBuf {
        self.cgroup_root.join(self.cgroup_parent.trim_matches('/'))
    }

    /// The cgroup level of an allocation leaf: `<parent>/<slice>/legion-<id>`.
    fn leaf_level(&self) -> u32 {
        let parent = self
            .cgroup_parent
            .split('/')
            .filter(|part| !part.is_empty())
            .count();
        (parent + 2) as u32
    }

    fn versioned_pins(&self) -> Option<PathBuf> {
        self.pin_dir
            .as_ref()
            .map(|dir| dir.join(format!("abi-{ABI_VERSION}")))
    }
}

/// One allocation as the classifier sees it this heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KernelAllocation {
    pub allocation_id: i64,
    /// Its cgroup leaf, relative to the cgroup root
    /// (`legion/share/legion-88213`). Only leaves that exist are listed.
    pub cgroup: String,
    pub vip_block: Option<String>,
    pub loopback_ports: Vec<u16>,
    /// Its resolver, when port-53 traffic should go there.
    pub resolver: Option<Ipv4Addr>,
    pub gated: bool,
    /// The capture listener for flows only netd can decide.
    pub capture: Option<SocketAddrV4>,
    /// Its compiled egress chain, when the kernel enforces it.
    pub chain: Option<Compiled>,
}

/// What the worker holds for one installed allocation.
#[derive(Debug, Clone)]
struct Installed {
    leaf: u64,
    spec: KernelAllocation,
}

/// The loaded classifier.
pub struct KernelNet {
    settings: KernelSettings,
    // Holds the programs (and, without pins, their links).
    _ebpf: Ebpf,
    links: Vec<FdLink>,
    pinned: Vec<PinnedLink>,
    allocs: BpfHashMap<MapData, u64, abi::Alloc>,
    rules: BpfHashMap<MapData, u64, abi::Rules>,
    learned: BpfHashMap<MapData, abi::LearnedKey, abi::Learned>,
    guard4: LpmTrie<MapData, u32, u32>,
    guard6: LpmTrie<MapData, [u32; 4], u32>,
    allow: Array<MapData, abi::Allow>,
    local: BpfHashMap<MapData, abi::LocalKey, u32>,
    control_ports: BpfHashMap<MapData, u32, u32>,
    datapath_ports: BpfHashMap<MapData, u32, u32>,
    own_ports: BpfHashMap<MapData, abi::OwnPort, u32>,
    counters: BpfHashMap<MapData, abi::CounterKey, u64>,
    orig: BpfHashMap<MapData, abi::Endpoint, abi::Orig>,
    bytes: PerCpuHashMap<MapData, u64, abi::Bytes>,
    events: RingBuf<MapData>,
    installed: HashMap<i64, Installed>,
    /// Leaves found in pinned maps at start that no allocation has claimed
    /// yet; the first reconcile removes what it does not list.
    adopted: BTreeSet<u64>,
    generation: u32,
    bytes_seen: HashMap<u64, (u64, u64)>,
    counters_seen: HashMap<abi::CounterKey, u64>,
}

/// The cgroup id of a cgroup v2 directory: its inode number.
pub fn cgroup_id(path: &Path) -> R<u64> {
    Ok(std::fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?
        .ino())
}

fn take<T>(ebpf: &mut Ebpf, name: &str) -> R<T>
where
    T: TryFrom<aya::maps::Map>,
    <T as TryFrom<aya::maps::Map>>::Error: std::fmt::Display,
{
    let map = ebpf
        .take_map(name)
        .with_context(|| format!("the object has no map {name}"))?;
    T::try_from(map).map_err(|err| anyhow!("map {name}: {err}"))
}

fn u32_from_v4(addr: Ipv4Addr) -> u32 {
    u32::from_ne_bytes(addr.octets())
}

fn vip_block_word(block: &str) -> Option<u32> {
    let parsed: Ipv4Addr = format!("{block}.0").parse().ok()?;
    Some(u32_from_v4(parsed))
}

impl KernelNet {
    /// Load and attach the classifier. Re-opens pinned maps and replaces
    /// pinned links when a previous worker left them.
    pub fn open(settings: KernelSettings) -> R<Self> {
        let pins = settings.versioned_pins();
        if let Some(pins) = &pins {
            std::fs::create_dir_all(pins.join("maps"))
                .with_context(|| format!("create {}", pins.display()))?;
            std::fs::create_dir_all(pins.join("links"))?;
        }
        let map_paths: Vec<(&str, PathBuf)> = match &pins {
            Some(pins) => MAPS
                .iter()
                .map(|name| (*name, pins.join("maps").join(name)))
                .collect(),
            None => Vec::new(),
        };
        let mut loader = EbpfLoader::new();
        for (name, path) in &map_paths {
            loader.map_pin_path(name, path.as_path());
        }
        let mut ebpf = loader
            .load(crate::worker::netd::kernel::OBJECT)
            .context("load the kernel classifier object")?;

        let parent = settings.parent_path();
        std::fs::create_dir_all(&parent)
            .with_context(|| format!("create cgroup {}", parent.display()))?;
        let cgroup = std::fs::File::open(&parent)
            .with_context(|| format!("open cgroup {}", parent.display()))?;

        let mut links = Vec::new();
        for name in PROGRAMS {
            let link = attach(&mut ebpf, name, &cgroup)
                .with_context(|| format!("attach {name} to {}", parent.display()))?;
            links.push((*name, link));
        }

        // Pin the new links, then drop whatever a previous worker pinned:
        // between the two, both sets run against the same maps.
        let mut pinned = Vec::new();
        let mut kept = Vec::new();
        match &pins {
            Some(pins) => {
                for (name, link) in links {
                    // bpffs refuses names with a dot.
                    let fresh = pins.join("links").join(format!("{name}_new"));
                    let _ = std::fs::remove_file(&fresh);
                    let link = link
                        .pin(&fresh)
                        .map_err(|err| anyhow!("pin {name}: {err}"))?;
                    let final_path = pins.join("links").join(name);
                    if let Ok(old) = PinnedLink::from_pin(&final_path) {
                        if let Ok(old) = old.unpin() {
                            drop(old);
                        }
                    }
                    let _ = std::fs::remove_file(&final_path);
                    std::fs::rename(&fresh, &final_path)
                        .with_context(|| format!("rename {}", fresh.display()))?;
                    pinned.push(link);
                }
            }
            None => kept = links.into_iter().map(|(_, link)| link).collect(),
        }

        let mut config: Array<MapData, abi::Config> = take(&mut ebpf, "ln_config")?;
        config
            .set(
                0,
                abi::Config {
                    leaf_level: settings.leaf_level(),
                    flags: 0,
                },
                0,
            )
            .context("write ln_config")?;
        // Keep the map's fd alive with the others.
        drop(config);

        let allocs: BpfHashMap<MapData, u64, abi::Alloc> = take(&mut ebpf, "ln_allocs")?;
        let adopted: BTreeSet<u64> = allocs.keys().filter_map(Result::ok).collect();
        let mut net = KernelNet {
            settings,
            rules: take(&mut ebpf, "ln_rules")?,
            learned: take(&mut ebpf, "ln_learned")?,
            guard4: take(&mut ebpf, "ln_guard4")?,
            guard6: take(&mut ebpf, "ln_guard6")?,
            allow: take(&mut ebpf, "ln_allow")?,
            local: take(&mut ebpf, "ln_local")?,
            control_ports: take(&mut ebpf, "ln_control_ports")?,
            datapath_ports: take(&mut ebpf, "ln_datapath_ports")?,
            own_ports: take(&mut ebpf, "ln_own_ports")?,
            counters: take(&mut ebpf, "ln_counters")?,
            orig: take(&mut ebpf, "ln_orig")?,
            bytes: take(&mut ebpf, "ln_bytes")?,
            events: take(&mut ebpf, "ln_events")?,
            allocs,
            _ebpf: ebpf,
            links: kept,
            pinned,
            installed: HashMap::new(),
            adopted,
            generation: 0,
            bytes_seen: HashMap::new(),
            counters_seen: HashMap::new(),
        };
        // The unattributed entry: the floor, no exemptions.
        net.allocs
            .insert(
                0,
                abi::Alloc {
                    flags: abi::A_GUARD,
                    ..Default::default()
                },
                0,
            )
            .context("write the unattributed entry")?;
        net.adopted.remove(&0);
        Ok(net)
    }

    /// The node-wide guard: Legion's ports, cloud metadata, and the
    /// operator's `hostGuard` entries (the allocations come from
    /// [`Self::reconcile`]).
    pub fn set_guard(&mut self, table: &GuardTable) -> R<()> {
        for key in self
            .guard4
            .keys()
            .filter_map(Result::ok)
            .collect::<Vec<_>>()
        {
            let _ = self.guard4.remove(&key);
        }
        for key in self
            .guard6
            .keys()
            .filter_map(Result::ok)
            .collect::<Vec<_>>()
        {
            let _ = self.guard6.remove(&key);
        }
        let classify =
            |entries: &[String], class: u32| -> Vec<(crate::structs::netpolicy::Cidr, u32)> {
                entries
                    .iter()
                    .filter_map(|entry| crate::structs::netpolicy::parse_cidr(entry).ok())
                    .map(|cidr| (cidr, class))
                    .collect()
            };
        let mut ranges = classify(&METADATA_V4.map(String::from), abi::EV_METADATA);
        ranges.extend(classify(&METADATA_V6.map(String::from), abi::EV_METADATA));
        ranges.extend(classify(&table.deny_v4, abi::EV_DENY_LIST));
        ranges.extend(classify(&table.deny_v6, abi::EV_DENY_LIST));
        for (cidr, class) in ranges {
            match cidr.addr {
                IpAddr::V4(v4) => {
                    self.guard4.insert(
                        &LpmKey::new(u32::from(cidr.prefix), u32_from_v4(v4)),
                        class,
                        0,
                    )?;
                }
                IpAddr::V6(v6) => {
                    let (words, _) = v6_words(v6);
                    self.guard6
                        .insert(&LpmKey::new(u32::from(cidr.prefix), words), class, 0)?;
                }
            }
        }
        for slot in 0..abi::MAX_ALLOW as u32 {
            self.allow.set(slot, abi::Allow::default(), 0)?;
        }
        for (slot, (address, port)) in table.allow.iter().take(abi::MAX_ALLOW).enumerate() {
            let Ok(cidr) = crate::structs::netpolicy::parse_cidr(address) else {
                continue;
            };
            let (addr, family) = match cidr.addr {
                IpAddr::V4(v4) => ([u32_from_v4(v4), 0, 0, 0], 4),
                IpAddr::V6(v6) => (v6_words(v6).0, 6),
            };
            self.allow.set(
                slot as u32,
                abi::Allow {
                    addr,
                    port: abi::port_ne(*port),
                    prefix: cidr.prefix,
                    family,
                },
                0,
            )?;
        }
        for key in self
            .control_ports
            .keys()
            .filter_map(Result::ok)
            .collect::<Vec<_>>()
        {
            let _ = self.control_ports.remove(&key);
        }
        for port in &table.control_ports {
            self.control_ports
                .insert(u32::from(abi::port_ne(*port)), 1, 0)?;
        }
        Ok(())
    }

    /// The node's own addresses (a Legion control port is refused on any
    /// of them).
    pub fn set_local_addresses(&mut self, addresses: &[IpAddr]) -> R<()> {
        let wanted: std::collections::HashSet<abi::LocalKey> = addresses
            .iter()
            .map(|ip| {
                let (addr, family) = abi::words_from_ip(*ip);
                abi::LocalKey {
                    addr,
                    family: u32::from(family),
                }
            })
            .collect();
        for key in self.local.keys().filter_map(Result::ok).collect::<Vec<_>>() {
            if !wanted.contains(&key) {
                let _ = self.local.remove(&key);
            }
        }
        for key in wanted {
            self.local.insert(key, 1, 0)?;
        }
        Ok(())
    }

    /// Describe every governed allocation for this heartbeat. Entries for
    /// allocations not listed are removed, and so is what they learned.
    pub fn reconcile(&mut self, allocations: &[KernelAllocation]) -> R<()> {
        let root = self.settings.cgroup_root.clone();
        let mut live: HashMap<i64, (u64, &KernelAllocation)> = HashMap::new();
        for allocation in allocations {
            let leaf = match cgroup_id(&root.join(allocation.cgroup.trim_matches('/'))) {
                Ok(leaf) => leaf,
                Err(err) => {
                    debug!(allocation_id = allocation.allocation_id, error = %format!("{err:#}"), "Kernel classifier: leaf not found yet");
                    continue;
                }
            };
            live.insert(allocation.allocation_id, (leaf, allocation));
        }

        // Departed allocations, and leaves a previous worker installed.
        let gone: Vec<(i64, u64)> = self
            .installed
            .iter()
            .filter(|(id, installed)| live.get(id).map(|(leaf, _)| *leaf) != Some(installed.leaf))
            .map(|(id, installed)| (*id, installed.leaf))
            .collect();
        let claimed: BTreeSet<u64> = live.values().map(|(leaf, _)| *leaf).collect();
        let adopted: Vec<u64> = self
            .adopted
            .iter()
            .filter(|leaf| !claimed.contains(leaf))
            .copied()
            .collect();
        for (id, leaf) in gone {
            self.remove_leaf(leaf);
            self.installed.remove(&id);
        }
        for leaf in adopted {
            self.remove_leaf(leaf);
        }
        self.adopted.clear();

        // Every allocation's loopback ports are datapath ports to the others.
        let datapath: BTreeSet<u32> = live
            .values()
            .flat_map(|(_, allocation)| allocation.loopback_ports.iter())
            .map(|port| u32::from(abi::port_ne(*port)))
            .collect();
        for key in self
            .datapath_ports
            .keys()
            .filter_map(Result::ok)
            .collect::<Vec<_>>()
        {
            if !datapath.contains(&key) {
                let _ = self.datapath_ports.remove(&key);
            }
        }
        for port in &datapath {
            self.datapath_ports.insert(*port, 1, 0)?;
        }

        for (id, (leaf, allocation)) in live {
            let unchanged = self
                .installed
                .get(&id)
                .is_some_and(|installed| installed.leaf == leaf && installed.spec == *allocation);
            if unchanged {
                continue;
            }
            self.install(leaf, allocation)?;
            self.installed.insert(
                id,
                Installed {
                    leaf,
                    spec: allocation.clone(),
                },
            );
        }
        Ok(())
    }

    fn install(&mut self, leaf: u64, allocation: &KernelAllocation) -> R<()> {
        self.generation = self.generation.wrapping_add(1);
        let mut flags = abi::A_GUARD;
        if allocation.gated {
            flags |= abi::A_GATED;
        }
        if allocation.resolver.is_some() {
            flags |= abi::A_DNS;
        }
        if allocation.capture.is_some() {
            flags |= abi::A_CAPTURE;
        }
        let mut nrules = 0;
        match &allocation.chain {
            Some(chain) if !allocation.gated => {
                flags |= abi::A_ENFORCE;
                if chain.default_allow {
                    flags |= abi::A_DEFAULT_ALLOW;
                }
                if chain.inspect {
                    flags |= abi::A_INSPECT;
                }
                nrules = chain.rules.len() as u16;
                self.rules.insert(leaf, chain.rules_value(), 0)?;
            }
            _ => {
                let _ = self.rules.remove(&leaf);
            }
        }
        // Own loopback ports: replace the set.
        for key in self
            .own_ports
            .keys()
            .filter_map(Result::ok)
            .collect::<Vec<_>>()
        {
            if key.leaf == leaf {
                let _ = self.own_ports.remove(&key);
            }
        }
        for port in &allocation.loopback_ports {
            self.own_ports.insert(
                abi::OwnPort {
                    leaf,
                    port: u32::from(abi::port_ne(*port)),
                    pad: 0,
                },
                1,
                0,
            )?;
        }
        let entry = abi::Alloc {
            allocation_id: allocation.allocation_id,
            flags,
            vip_block: allocation
                .vip_block
                .as_deref()
                .and_then(vip_block_word)
                .unwrap_or(0),
            resolver4: allocation.resolver.map(u32_from_v4).unwrap_or(0),
            capture4: allocation
                .capture
                .map(|addr| u32_from_v4(*addr.ip()))
                .unwrap_or(0),
            capture_port: allocation
                .capture
                .map(|addr| abi::port_ne(addr.port()))
                .unwrap_or(0),
            nrules,
            generation: self.generation,
        };
        self.allocs.insert(leaf, entry, 0)?;
        Ok(())
    }

    fn remove_leaf(&mut self, leaf: u64) {
        let _ = self.allocs.remove(&leaf);
        let _ = self.rules.remove(&leaf);
        for key in self
            .own_ports
            .keys()
            .filter_map(Result::ok)
            .collect::<Vec<_>>()
        {
            if key.leaf == leaf {
                let _ = self.own_ports.remove(&key);
            }
        }
        for key in self
            .learned
            .keys()
            .filter_map(Result::ok)
            .collect::<Vec<_>>()
        {
            if key.leaf == leaf {
                let _ = self.learned.remove(&key);
            }
        }
        self.bytes_seen.remove(&leaf);
    }

    /// Teach the kernel that `addresses` answered a name whose host rules
    /// are `bits`, for `ttl`. Bits from several names for one address add
    /// up until the entry expires.
    pub fn learn(&mut self, allocation_id: i64, addresses: &[IpAddr], bits: u64, ttl: Duration) {
        let Some(leaf) = self.installed.get(&allocation_id).map(|i| i.leaf) else {
            return;
        };
        let expires_ns = monotonic_ns().saturating_add(ttl.as_nanos() as u64);
        for ip in addresses {
            let (addr, _) = abi::words_from_ip(*ip);
            let key = abi::LearnedKey { leaf, addr };
            let mut value = abi::Learned { bits, expires_ns };
            if let Ok(held) = self.learned.get(&key, 0) {
                if held.expires_ns > monotonic_ns() {
                    value.bits |= held.bits;
                    value.expires_ns = value.expires_ns.max(held.expires_ns);
                }
            }
            let _ = self.learned.insert(key, value, 0);
        }
    }

    /// Denials (and redirects) since the last call.
    pub fn events(&mut self) -> Vec<abi::Event> {
        let mut out = Vec::new();
        while let Some(item) = self.events.next() {
            if let Some(event) = abi::Event::from_bytes(&item) {
                out.push(event);
            }
        }
        out
    }

    /// Bytes each installed allocation moved since the last call.
    pub fn bytes(&mut self) -> HashMap<i64, (u64, u64)> {
        let mut out = HashMap::new();
        let installed: Vec<(i64, u64)> = self
            .installed
            .iter()
            .map(|(id, installed)| (*id, installed.leaf))
            .collect();
        for (id, leaf) in installed {
            let Ok(values) = self.bytes.get(&leaf, 0) else {
                continue;
            };
            let (tx, rx) = values
                .iter()
                .fold((0u64, 0u64), |(tx, rx), v| (tx + v.tx, rx + v.rx));
            let (seen_tx, seen_rx) = self.bytes_seen.insert(leaf, (tx, rx)).unwrap_or((0, 0));
            let delta = (tx.saturating_sub(seen_tx), rx.saturating_sub(seen_rx));
            if delta != (0, 0) {
                out.insert(id, delta);
            }
        }
        out
    }

    /// Connect verdicts per (allocation, slot, allowed) since the last call:
    /// slot is a compiled rule index or an `abi::SLOT_*` value.
    pub fn counters(&mut self) -> Vec<(i64, u32, bool, u64)> {
        let leaves: HashMap<u64, i64> = self
            .installed
            .iter()
            .map(|(id, installed)| (installed.leaf, *id))
            .collect();
        let mut out = Vec::new();
        let entries: Vec<(abi::CounterKey, u64)> =
            self.counters.iter().filter_map(Result::ok).collect();
        for (key, total) in entries {
            let seen = self.counters_seen.insert(key, total).unwrap_or(0);
            if total <= seen {
                continue;
            }
            let id = if key.leaf == 0 {
                0
            } else {
                match leaves.get(&key.leaf) {
                    Some(id) => *id,
                    None => continue,
                }
            };
            out.push((
                id,
                key.slot,
                key.verdict == u32::from(abi::ALLOW),
                total - seen,
            ));
        }
        out
    }

    /// The destination a redirected connection was dialing, by what the
    /// capture listener sees as the peer, and the allocation it came from.
    pub fn original_destination(
        &mut self,
        netns: u64,
        peer: SocketAddr,
    ) -> Option<(SocketAddr, i64)> {
        let (addr, _) = abi::words_from_ip(peer.ip());
        let key = abi::Endpoint {
            netns,
            addr,
            port: u32::from(peer.port()),
            pad: 0,
        };
        let orig = self.orig.get(&key, 0).ok()?;
        let _ = self.orig.remove(&key);
        Some((
            abi::socket_addr(&orig.addr, orig.family, orig.port),
            orig.allocation_id,
        ))
    }

    /// The compiled chain installed for an allocation.
    pub fn chain(&self, allocation_id: i64) -> Option<&Compiled> {
        self.installed
            .get(&allocation_id)
            .and_then(|installed| installed.spec.chain.as_ref())
    }

    /// Whether the kernel enforces the allocation's egress chain, and
    /// whether it captures.
    pub fn status(&self, allocation_id: i64) -> Option<(bool, bool)> {
        self.installed.get(&allocation_id).map(|installed| {
            (
                installed.spec.chain.is_some() && !installed.spec.gated,
                installed.spec.capture.is_some(),
            )
        })
    }

    /// Detach everything and remove the pins (clean shutdown).
    pub fn shutdown(self) {
        let pins = self.settings.versioned_pins();
        for link in self.pinned {
            if let Ok(link) = link.unpin() {
                drop(link);
            }
        }
        drop(self.links);
        if let Some(pins) = pins {
            let _ = std::fs::remove_dir_all(&pins);
        }
    }
}

fn attach(ebpf: &mut Ebpf, name: &str, cgroup: &std::fs::File) -> R<FdLink> {
    use aya::programs::{CgroupSkb, CgroupSock, CgroupSockAddr, SockOps};
    let program = ebpf
        .program_mut(name)
        .with_context(|| format!("the object has no program {name}"))?;
    // A bpf_link attachment (kernel 5.7+) is always multi-attach in the
    // kernel, and link_create refuses the explicit flag; `Single` sends none.
    let mode = CgroupAttachMode::Single;
    let link: FdLink = match program {
        aya::programs::Program::CgroupSockAddr(program) => {
            let program: &mut CgroupSockAddr = program;
            program.load()?;
            let id = program.attach(cgroup, mode)?;
            program.take_link(id)?.try_into()?
        }
        aya::programs::Program::CgroupSock(program) => {
            let program: &mut CgroupSock = program;
            program.load()?;
            let id = program.attach(cgroup, mode)?;
            program.take_link(id)?.try_into()?
        }
        aya::programs::Program::SockOps(program) => {
            let program: &mut SockOps = program;
            program.load()?;
            let id = program.attach(cgroup, mode)?;
            program.take_link(id)?.try_into()?
        }
        aya::programs::Program::CgroupSkb(program) => {
            let program: &mut CgroupSkb = program;
            program.load()?;
            let direction = if name.ends_with("egress") {
                CgroupSkbAttachType::Egress
            } else {
                CgroupSkbAttachType::Ingress
            };
            let id = program.attach(cgroup, direction, mode)?;
            program.take_link(id)?.try_into()?
        }
        other => bail!("{name}: unexpected program type {:?}", other.prog_type()),
    };
    Ok(link)
}

fn v6_words(v6: std::net::Ipv6Addr) -> ([u32; 4], u8) {
    let octets = v6.octets();
    let mut words = [0u32; 4];
    for (i, word) in words.iter_mut().enumerate() {
        *word = u32::from_ne_bytes([
            octets[i * 4],
            octets[i * 4 + 1],
            octets[i * 4 + 2],
            octets[i * 4 + 3],
        ]);
    }
    (words, 6)
}

/// CLOCK_MONOTONIC in nanoseconds, the clock `bpf_ktime_get_ns` reads.
pub fn monotonic_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: a valid out-pointer to a timespec.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

/// The network-namespace cookie of a socket (`SO_NETNS_COOKIE`), which the
/// classifier records with each redirected flow.
pub fn netns_cookie(fd: std::os::fd::BorrowedFd<'_>) -> R<u64> {
    use std::os::fd::AsRawFd as _;
    const SO_NETNS_COOKIE: libc::c_int = 71;
    let mut cookie: u64 = 0;
    let mut len = std::mem::size_of::<u64>() as libc::socklen_t;
    // SAFETY: a valid fd and out-pointers sized for a u64.
    let rc = unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            SO_NETNS_COOKIE,
            (&mut cookie as *mut u64).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        bail!(
            "getsockopt(SO_NETNS_COOKIE): {}",
            std::io::Error::last_os_error()
        );
    }
    Ok(cookie)
}

/// The cgroup v2 mount on this host, from `/proc/mounts`: the unified
/// hierarchy at `/sys/fs/cgroup`, or a hybrid host's `unified` mount.
pub fn cgroup2_mount() -> Option<PathBuf> {
    let mounts = std::fs::read_to_string("/proc/mounts").ok()?;
    mounts
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let _source = fields.next()?;
            let target = fields.next()?;
            let kind = fields.next()?;
            (kind == "cgroup2").then(|| PathBuf::from(target))
        })
        .min_by_key(|path| path.as_os_str().len())
}

#[cfg(test)]
mod runtime_test {
    use super::*;
    use crate::worker::netd::kernel::compile::compile;
    use std::process::Command;

    /// A scratch cgroup parent with one allocation leaf, removed on drop.
    struct Scratch {
        root: PathBuf,
        parent: String,
        leaf: String,
    }

    impl Scratch {
        fn new(tag: &str) -> Option<Scratch> {
            let root = cgroup2_mount()?;
            let parent = format!("legion-kn-{tag}-{}", std::process::id());
            let leaf = format!("{parent}/share/legion-1");
            std::fs::create_dir_all(root.join(&leaf)).ok()?;
            Some(Scratch { root, parent, leaf })
        }

        fn settings(&self, pin_dir: Option<PathBuf>) -> KernelSettings {
            KernelSettings {
                cgroup_root: self.root.clone(),
                cgroup_parent: self.parent.clone(),
                pin_dir,
            }
        }

        /// Run a Python snippet inside the leaf; its stdout.
        fn run(&self, script: &str) -> String {
            let procs = self.root.join(&self.leaf).join("cgroup.procs");
            let output = Command::new("sh")
                .arg("-c")
                .arg(format!(
                    "echo $$ > {} && exec python3 -c \"$0\"",
                    procs.display()
                ))
                .arg(script)
                .output()
                .expect("run the child");
            String::from_utf8_lossy(&output.stdout).trim().to_string()
                + &String::from_utf8_lossy(&output.stderr)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir(self.root.join(&self.leaf));
            let _ = std::fs::remove_dir(self.root.join(&self.parent).join("share"));
            let _ = std::fs::remove_dir(self.root.join(&self.parent));
        }
    }

    fn capable() -> bool {
        // SAFETY: geteuid has no preconditions.
        let root = unsafe { libc::geteuid() } == 0;
        let python = Command::new("python3").arg("-V").output().is_ok();
        root && python && cgroup2_mount().is_some()
    }

    /// A local, non-loopback IPv4 address of this host: what the chain
    /// judges (loopback is the guard's alone).
    fn node_ip() -> Option<Ipv4Addr> {
        crate::structs::host_guard::interface_addresses()
            .into_iter()
            .find_map(|ip| match ip {
                IpAddr::V4(v4) if !v4.is_loopback() => Some(v4),
                _ => None,
            })
    }

    fn chain(value: serde_json::Value) -> Compiled {
        let mut config: crate::structs::netpolicy::ChainConfig =
            serde_json::from_value(value).unwrap();
        config.normalize_and_validate("egress").unwrap();
        let merged = crate::structs::netpolicy::merge_policies(
            &[],
            crate::structs::netpolicy::InlinePolicy {
                egress: Some(&config),
                ..Default::default()
            },
        );
        compile(merged.egress.as_ref().unwrap()).unwrap()
    }

    const DIAL: &str = r#"
import socket, sys
def dial(host, port):
    s = socket.socket(socket.AF_INET6 if ':' in host else socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    try:
        s.connect((host, port)); return 'ok'
    except PermissionError: return 'eperm'
    except Exception as e: return type(e).__name__
"#;

    /// The guard, the compiled chain, learned hosts, the loopback
    /// exemption, raw sockets, and the events they raise, on a real kernel.
    ///
    /// ```text
    /// sudo -E cargo test --lib kernel::runtime -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "requires root, cgroup v2, and BPF"]
    fn the_classifier_enforces_guard_and_chain_on_a_real_kernel_test() -> R<()> {
        if !capable() {
            eprintln!("skipping: needs root, python3, and cgroup v2");
            return Ok(());
        }
        let Some(ip) = node_ip() else {
            eprintln!("skipping: no non-loopback IPv4 address");
            return Ok(());
        };
        let scratch = Scratch::new("enforce").context("scratch cgroup")?;
        let mut net = KernelNet::open(scratch.settings(None))?;
        net.set_guard(&GuardTable {
            control_ports: [4848].into_iter().collect(),
            ..Default::default()
        })?;
        net.set_local_addresses(&[IpAddr::V4(ip)])?;
        let open = std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        let open_port = open.local_addr()?.port();
        let other = std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        let other_port = other.local_addr()?.port();
        let compiled = chain(serde_json::json!({
            "policy": "deny",
            "rules": [
                { "when": { "port": 25 }, "action": "deny", "reason": "no mail" },
                { "when": { "cidr": format!("{ip}/32"), "port": open_port }, "action": "allow" },
                { "when": { "host": "svc.example" }, "action": "allow" },
            ],
        }));
        let bits = compiled.learned_bits("svc.example");
        net.reconcile(&[KernelAllocation {
            allocation_id: 1,
            cgroup: scratch.leaf.clone(),
            vip_block: None,
            loopback_ports: vec![],
            resolver: None,
            gated: false,
            capture: None,
            chain: Some(compiled),
        }])?;

        let said = scratch.run(&format!(
            "{DIAL}\nprint(dial('169.254.169.254', 80), dial('{ip}', {open_port}), \
             dial('{ip}', {other_port}), dial('{ip}', 25), dial('127.0.0.1', {other_port}))"
        ));
        assert_eq!(
            said, "eperm ok eperm eperm ok",
            "guard, allow, host unlearned, rule, loopback"
        );

        net.learn(1, &[IpAddr::V4(ip)], bits, Duration::from_secs(60));
        let said = scratch.run(&format!("{DIAL}\nprint(dial('{ip}', {other_port}))"));
        assert_eq!(said, "ok", "a learned address satisfies the host rule");

        let said = scratch.run(
            "import socket\ntry:\n  socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP); print('raw')\nexcept PermissionError: print('eperm')",
        );
        assert_eq!(said, "eperm", "no raw sockets for a governed task");

        let events = net.events();
        let kinds: Vec<u32> = events.iter().map(|event| event.kind).collect();
        assert!(kinds.contains(&abi::EV_METADATA), "{kinds:?}");
        assert!(kinds.contains(&abi::EV_RULE), "{kinds:?}");
        assert!(kinds.contains(&abi::EV_NEEDS_NETD), "{kinds:?}");
        assert!(kinds.contains(&abi::EV_RAW_SOCKET), "{kinds:?}");
        let rule = events
            .iter()
            .find(|event| event.kind == abi::EV_RULE)
            .unwrap();
        assert_eq!(rule.allocation_id, 1);
        assert_eq!(
            net.chain(1)
                .unwrap()
                .label(rule.index)
                .unwrap()
                .1
                .as_deref(),
            Some("no mail")
        );
        assert_eq!(rule.destination(), SocketAddr::from((ip, 25)));
        let counters = net.counters();
        assert!(
            counters
                .iter()
                .any(|(id, slot, allowed, n)| *id == 1 && *slot == 1 && *allowed && *n >= 1),
            "{counters:?}"
        );

        // The allocation leaves: its entry goes, and it is unattributed
        // (the guard floor only) from then on.
        net.reconcile(&[])?;
        let said = scratch.run(&format!(
            "{DIAL}\nprint(dial('{ip}', {other_port}), dial('169.254.169.254', 80))"
        ));
        assert_eq!(said, "ok eperm");
        net.shutdown();
        Ok(())
    }

    /// DNS goes to the allocation's resolver whatever the task asked, and
    /// the reply reads as if it came from the nameserver it asked; a flow
    /// only netd can decide is redirected, and its original destination is
    /// recovered from what the capture listener sees.
    #[test]
    #[ignore = "requires root, cgroup v2, and BPF"]
    fn dns_and_capture_redirect_on_a_real_kernel_test() -> R<()> {
        if !capable() {
            eprintln!("skipping: needs root, python3, and cgroup v2");
            return Ok(());
        }
        let Some(ip) = node_ip() else {
            return Ok(());
        };
        let scratch = Scratch::new("capture").context("scratch cgroup")?;
        let mut net = KernelNet::open(scratch.settings(None))?;
        net.set_guard(&GuardTable::default())?;
        let resolver = Ipv4Addr::new(127, 7, 7, 7);
        let dns = std::net::UdpSocket::bind((resolver, 53))?;
        dns.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
        let capture = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let capture_addr = match capture.local_addr()? {
            SocketAddr::V4(addr) => addr,
            _ => unreachable!(),
        };
        net.reconcile(&[KernelAllocation {
            allocation_id: 7,
            cgroup: scratch.leaf.clone(),
            vip_block: None,
            loopback_ports: vec![],
            resolver: Some(resolver),
            gated: false,
            capture: Some(capture_addr),
            chain: Some(chain(serde_json::json!({
                "protocol": "http",
                "rules": [{ "when": { "port": 8080, "path": "/admin/**" }, "action": "deny" }],
            }))),
        }])?;

        let replier = std::thread::spawn(move || {
            let mut buf = [0u8; 512];
            let (len, from) = dns.recv_from(&mut buf).expect("a query arrives");
            dns.send_to(&buf[..len], from).expect("reply");
            buf[..len].to_vec()
        });
        let said = scratch.run(
            "import socket\ns = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)\ns.settimeout(3)\ns.sendto(b'hello', ('8.8.8.8', 53))\ndata, src = s.recvfrom(512)\nprint(data.decode(), src[0], src[1])",
        );
        assert_eq!(replier.join().unwrap(), b"hello");
        assert_eq!(
            said, "hello 8.8.8.8 53",
            "the reply reads as the nameserver's"
        );

        capture.set_nonblocking(false)?;
        let netns = netns_cookie(std::os::fd::AsFd::as_fd(&capture))?;
        let child = {
            let script = format!(
                "import socket\ns = socket.create_connection(('{ip}', 8080), timeout=3)\ns.sendall(b'GET / HTTP/1.1\\r\\n\\r\\n')\nprint('sent')"
            );
            let leaf = scratch.root.join(&scratch.leaf).join("cgroup.procs");
            std::thread::spawn(move || {
                Command::new("sh")
                    .arg("-c")
                    .arg(format!(
                        "echo $$ > {} && exec python3 -c \"$0\"",
                        leaf.display()
                    ))
                    .arg(script)
                    .output()
                    .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
            })
        };
        let (_stream, peer) = capture.accept()?;
        let original = net.original_destination(netns, peer);
        assert_eq!(child.join().unwrap()?, "sent");
        assert_eq!(original, Some((SocketAddr::from((ip, 8080)), 7)));
        net.shutdown();
        Ok(())
    }

    /// Pinned maps and links outlive the worker that made them: a second
    /// open re-adopts them, and the kernel keeps enforcing in between.
    #[test]
    #[ignore = "requires root, cgroup v2, BPF, and bpffs"]
    fn pins_survive_a_restart_test() -> R<()> {
        if !capable() {
            return Ok(());
        }
        let bpffs = PathBuf::from("/sys/fs/bpf");
        let mounted = std::fs::read_to_string("/proc/mounts")?
            .lines()
            .any(|line| line.contains(" /sys/fs/bpf bpf "));
        if !mounted {
            let _ = std::fs::create_dir_all(&bpffs);
            let status = Command::new("mount")
                .args(["-t", "bpf", "bpf", "/sys/fs/bpf"])
                .status();
            if !status.is_ok_and(|status| status.success()) {
                eprintln!("skipping: no bpffs");
                return Ok(());
            }
        }
        let scratch = Scratch::new("pins").context("scratch cgroup")?;
        let pins = bpffs.join(format!("legion-test-{}", std::process::id()));
        let mut first = KernelNet::open(scratch.settings(Some(pins.clone())))?;
        first.set_guard(&GuardTable::default())?;
        first.reconcile(&[KernelAllocation {
            allocation_id: 3,
            cgroup: scratch.leaf.clone(),
            vip_block: None,
            loopback_ports: vec![],
            resolver: None,
            gated: false,
            capture: None,
            chain: Some(chain(
                serde_json::json!({ "rules": [{ "when": { "port": 25 }, "action": "deny" }] }),
            )),
        }])?;
        // The worker goes away without a clean shutdown.
        drop(first);
        let said = scratch.run(&format!("{DIAL}\nprint(dial('192.0.2.1', 25))"));
        assert_eq!(said, "eperm", "enforced with no worker running");
        let second = KernelNet::open(scratch.settings(Some(pins.clone())))?;
        let said = scratch.run(&format!("{DIAL}\nprint(dial('192.0.2.1', 25))"));
        assert_eq!(said, "eperm", "still enforced after re-adoption");
        second.shutdown();
        assert!(!pins.join(format!("abi-{ABI_VERSION}")).exists());
        let _ = std::fs::remove_dir(&pins);
        Ok(())
    }
}
