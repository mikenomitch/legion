//! The host guard in the kernel (worker-plane-auth plan §6.13; docs:
//! network-handlers.md, "The host guard in the kernel").
//!
//! A `host`-mode task never touches the datapath unless it wants to, so the
//! datapath guard (`super::guard`) does nothing for it. On Linux the worker
//! installs one `nftables` table that applies the same floor to sockets
//! owned by allocations, matched by their cgroup leaf
//! (`<cgroupParent>/{share,reserve}/legion-<alloc>`):
//!
//! - the cloud metadata service (link-local, `fd00:ec2::254`,
//!   `100.100.100.200`) and the worker's `hostGuard.deny` entries;
//! - Legion's own control ports on any local address (the worker plane, and
//!   the manager's API, exec, and telemetry ports when one is co-located);
//! - other allocations' datapath listeners: their VIP blocks and their
//!   upstream loopback ports. An allocation keeps its own.
//!
//! `hostGuard.allow` entries are accepted first. A drop in the output hook
//! fails `connect()` at once with `EPERM`. Each allocation's rules carry
//! their own counters, which the worker reads every heartbeat and reports
//! as `net.denied` rows with source `host-guard`.
//!
//! The worker owns the table: it is replaced atomically on every change
//! (`nft -f -` with an add-delete-add transaction). It shells out to `nft`,
//! as the container runtime shells out to its binaries, so there is no new
//! dependency.
//!
//! **One per worker, not per host.** The table, and the kernel
//! classifier's bpffs pin directory, are named after the worker's cgroup
//! parent ([`table_name`], [`bpf_pin_dir`]), so a manager's system worker
//! (`legion-system`) and a standalone worker (`legion`) on one host each
//! keep their own instead of overwriting each other's with only their own
//! allocations. A worker with the default parent keeps the historical names
//! (`legion_host_guard`, `/sys/fs/bpf/legion`).
//!
//! What it does not cover is stated in the public doc: a root `exec` task
//! (it holds `CAP_NET_ADMIN` and can delete the table), other daemons on the
//! node's addresses, and nodes without cgroup v2 or `nft`.

use crate::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

/// The table's name in the `inet` family, for a worker with the default
/// cgroup parent; other workers append their parent ([`table_name`]).
pub const TABLE: &str = "legion_host_guard";

/// The cgroup parent whose worker keeps the historical, unsuffixed names.
pub const DEFAULT_CGROUP_PARENT: &str = "legion";

/// Where workers pin the kernel classifier, under bpffs.
pub const BPF_PIN_ROOT: &str = "/sys/fs/bpf";

/// What sets a worker's kernel objects apart from another worker's on the
/// same host: nothing for the default cgroup parent (`legion`), so a lone
/// worker keeps the names it always had, and the parent otherwise, with
/// `/` and anything but letters, digits, `_`, `-`, and `.` turned into `_`
/// (a name `nft` and bpffs both take).
pub fn instance_suffix(cgroup_parent: &str) -> Option<String> {
    let parent = cgroup_parent.trim_matches('/');
    if parent.is_empty() || parent == DEFAULT_CGROUP_PARENT {
        return None;
    }
    Some(
        parent
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                    c
                } else {
                    '_'
                }
            })
            .collect(),
    )
}

/// The `inet` table the host guard of a worker with `cgroup_parent` loads:
/// `legion_host_guard`, or `legion_host_guard_<parent>`
/// (`legion_host_guard_legion-system` for a manager's system worker).
pub fn table_name(cgroup_parent: &str) -> String {
    match instance_suffix(cgroup_parent) {
        None => TABLE.to_string(),
        Some(suffix) => format!("{TABLE}_{suffix}"),
    }
}

/// The bpffs directory the kernel classifier of a worker with
/// `cgroup_parent` pins under: `/sys/fs/bpf/legion`, or
/// `/sys/fs/bpf/legion_<parent>`.
pub fn bpf_pin_dir(cgroup_parent: &str) -> std::path::PathBuf {
    let root = std::path::Path::new(BPF_PIN_ROOT);
    match instance_suffix(cgroup_parent) {
        None => root.join(DEFAULT_CGROUP_PARENT),
        Some(suffix) => root.join(format!("{DEFAULT_CGROUP_PARENT}_{suffix}")),
    }
}

/// The `hostGuard` node attribute: `active (nftables)` or
/// `unavailable (<reason>)`.
pub const NODE_ATTR: &str = "hostGuard";

/// Destinations every allocation is refused, before operator additions.
pub const METADATA_V4: [&str; 2] = ["169.254.0.0/16", "100.100.100.200"];
pub const METADATA_V6: [&str; 2] = ["fe80::/10", "fd00:ec2::254"];

/// What the kernel guard needs to know about one allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardedAllocation {
    pub allocation_id: i64,
    /// Its cgroup leaf, relative to the cgroup v2 root
    /// (`legion/share/legion-88213`). Only leaves that exist are listed:
    /// `nft` resolves the path when the table loads.
    pub cgroup: String,
    /// Its VIP block (`127.42.7`), when it has one.
    pub vip_block: Option<String>,
    /// Its upstream loopback ports.
    pub loopback_ports: Vec<u16>,
    /// Its resolver address. The kernel classifier sends its port-53
    /// traffic there; the `nftables` guard ignores it.
    pub resolver: Option<std::net::Ipv4Addr>,
    /// Whether it runs in a gated namespace (the classifier only captures
    /// for it, never passes).
    pub gated: bool,
    /// Whether its sockets are the task's own (`exec`, `container`). A
    /// sandbox's live in its guest, so netd judges them.
    pub host_sockets: bool,
}

/// Everything one rendering of the table depends on. Equal specs render
/// equal tables, so the worker reloads only on change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GuardTable {
    /// The cgroup parent every allocation leaf lives under (`legion`).
    pub cgroup_parent: String,
    /// Legion's ports on this node.
    pub control_ports: BTreeSet<u16>,
    /// `hostGuard.deny` entries, split by family.
    pub deny_v4: Vec<String>,
    pub deny_v6: Vec<String>,
    /// `hostGuard.allow` entries: (address or CIDR, port).
    pub allow: Vec<(String, u16)>,
    pub allocations: Vec<GuardedAllocation>,
}

/// Why a counter dropped, as the rule's comment names it and a `net.denied`
/// row reports it.
pub const DROP_METADATA: &str = "cloud metadata";
pub const DROP_DENY: &str = "hostGuard.deny";
pub const DROP_CONTROL: &str = "a Legion control port";
pub const DROP_DATAPATH: &str = "another allocation's datapath";

impl GuardTable {
    /// The `inet` table this one loads as ([`table_name`]).
    pub fn name(&self) -> String {
        table_name(&self.cgroup_parent)
    }

    /// The cgroup v2 `level` of a path: its number of components.
    fn level(path: &str) -> usize {
        path.split('/').filter(|part| !part.is_empty()).count()
    }

    /// Every allocation's upstream loopback ports, for the datapath set.
    fn datapath_ports(&self) -> BTreeSet<u16> {
        self.allocations
            .iter()
            .flat_map(|allocation| allocation.loopback_ports.iter().copied())
            .collect()
    }

    /// The floor every allocation chain ends with: accept the operator's
    /// holes, then drop each class with its own counter.
    fn floor(&self, out: &mut String) {
        for (address, port) in &self.allow {
            let family = if address.contains(':') { "ip6" } else { "ip" };
            let _ = writeln!(
                out,
                "    {family} daddr {address} tcp dport {port} accept comment \"hostGuard.allow\""
            );
        }
        let _ = writeln!(
            out,
            "    ip daddr @metadata_v4 counter drop comment \"{DROP_METADATA}\""
        );
        let _ = writeln!(
            out,
            "    ip6 daddr @metadata_v6 counter drop comment \"{DROP_METADATA}\""
        );
        if !self.deny_v4.is_empty() {
            let _ = writeln!(
                out,
                "    ip daddr @deny_v4 counter drop comment \"{DROP_DENY}\""
            );
        }
        if !self.deny_v6.is_empty() {
            let _ = writeln!(
                out,
                "    ip6 daddr @deny_v6 counter drop comment \"{DROP_DENY}\""
            );
        }
        if !self.control_ports.is_empty() {
            let _ = writeln!(
                out,
                "    fib daddr type local tcp dport @control_ports counter drop comment \"{DROP_CONTROL}\""
            );
        }
        let _ = writeln!(
            out,
            "    ip daddr 127.42.0.0/16 counter drop comment \"{DROP_DATAPATH}\""
        );
        if !self.datapath_ports().is_empty() {
            let _ = writeln!(
                out,
                "    ip daddr 127.0.0.0/8 tcp dport @datapath_ports counter drop comment \"{DROP_DATAPATH}\""
            );
        }
    }

    /// The `nft -f` script that replaces the table in one transaction.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let table = self.name();
        // Add-delete-add: `delete` fails on a table that does not exist, so
        // the first `add` makes it exist; all three apply atomically.
        let _ = writeln!(out, "table inet {table}");
        let _ = writeln!(out, "delete table inet {table}");
        let _ = writeln!(out, "table inet {table} {{");
        let set = |out: &mut String, name: &str, kind: &str, interval: bool, items: &[String]| {
            if items.is_empty() {
                return;
            }
            let flags = if interval { " flags interval;" } else { "" };
            let _ = writeln!(
                out,
                "  set {name} {{ type {kind};{flags} elements = {{ {} }} }}",
                items.join(", ")
            );
        };
        let ports = |ports: &BTreeSet<u16>| ports.iter().map(u16::to_string).collect::<Vec<_>>();
        set(
            &mut out,
            "control_ports",
            "inet_service",
            false,
            &ports(&self.control_ports),
        );
        set(
            &mut out,
            "metadata_v4",
            "ipv4_addr",
            true,
            &METADATA_V4.map(String::from),
        );
        set(
            &mut out,
            "metadata_v6",
            "ipv6_addr",
            true,
            &METADATA_V6.map(String::from),
        );
        set(&mut out, "deny_v4", "ipv4_addr", true, &self.deny_v4);
        set(&mut out, "deny_v6", "ipv6_addr", true, &self.deny_v6);
        set(
            &mut out,
            "datapath_ports",
            "inet_service",
            false,
            &ports(&self.datapath_ports()),
        );

        let parent = self.cgroup_parent.trim_matches('/');
        let _ = writeln!(out, "  chain output {{");
        let _ = writeln!(
            out,
            "    type filter hook output priority filter; policy accept;"
        );
        let _ = writeln!(
            out,
            "    socket cgroupv2 level {} \"{parent}\" jump allocations",
            Self::level(parent)
        );
        let _ = writeln!(out, "  }}");

        let _ = writeln!(out, "  chain allocations {{");
        for allocation in &self.allocations {
            let cgroup = allocation.cgroup.trim_matches('/');
            let _ = writeln!(
                out,
                "    socket cgroupv2 level {} \"{cgroup}\" jump alloc_{}",
                Self::level(cgroup),
                allocation.allocation_id
            );
        }
        // A socket in the parent that is no known allocation's (a leaf the
        // worker has not listed yet) gets the floor with no exemptions.
        let _ = writeln!(out, "    jump unattributed");
        let _ = writeln!(out, "  }}");

        for allocation in &self.allocations {
            let _ = writeln!(out, "  chain alloc_{} {{", allocation.allocation_id);
            if let Some(block) = &allocation.vip_block {
                let _ = writeln!(out, "    ip daddr {block}.0/24 accept");
            }
            if !allocation.loopback_ports.is_empty() {
                let own: BTreeSet<u16> = allocation.loopback_ports.iter().copied().collect();
                let _ = writeln!(
                    out,
                    "    ip daddr 127.0.0.1 tcp dport {{ {} }} accept",
                    ports(&own).join(", ")
                );
            }
            self.floor(&mut out);
            let _ = writeln!(out, "  }}");
        }
        let _ = writeln!(out, "  chain unattributed {{");
        self.floor(&mut out);
        let _ = writeln!(out, "  }}");
        let _ = writeln!(out, "}}");
        out
    }
}

/// Whether this node can run the kernel guard, and why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Availability {
    Active,
    Unavailable(String),
}

impl Availability {
    /// The `hostGuard` node attribute value.
    pub fn attribute(&self) -> String {
        match self {
            Availability::Active => "active (nftables)".to_string(),
            Availability::Unavailable(reason) => format!("unavailable ({reason})"),
        }
    }
}

/// Probes this node once at start: Linux, the cgroup v2 unified hierarchy,
/// root, and an `nft` binary that answers.
pub fn probe() -> Availability {
    if !cfg!(target_os = "linux") {
        return Availability::Unavailable("not Linux".into());
    }
    if !std::path::Path::new(crate::task_plugins::CGROUP_ROOT)
        .join("cgroup.controllers")
        .exists()
    {
        return Availability::Unavailable(format!(
            "no cgroup v2 unified hierarchy at {}",
            crate::task_plugins::CGROUP_ROOT
        ));
    }
    #[cfg(unix)]
    if unsafe { libc::geteuid() } != 0 {
        return Availability::Unavailable("the worker is not root".into());
    }
    match std::process::Command::new(nft_binary())
        .arg("--version")
        .output()
    {
        Ok(output) if output.status.success() => Availability::Active,
        Ok(output) => Availability::Unavailable(format!(
            "`nft --version` failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )),
        Err(err) => Availability::Unavailable(format!("no `nft` binary ({err})")),
    }
}

/// `nft`, or `LEGION_NFT` when set (tests, unusual installs).
fn nft_binary() -> String {
    std::env::var("LEGION_NFT").unwrap_or_else(|_| "nft".to_string())
}

/// Loads `table` (replacing whatever was there) through `nft -f -`.
pub fn apply(table: &GuardTable) -> R<()> {
    use std::io::Write as _;
    let mut child = std::process::Command::new(nft_binary())
        .args(["-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("spawn nft")?;
    child
        .stdin
        .take()
        .context("nft stdin")?
        .write_all(table.render().as_bytes())?;
    let output = child.wait_with_output().context("wait for nft")?;
    if !output.status.success() {
        bail!(
            "nft refused the host-guard table: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Deletes the table named `table` ([`table_name`]). Missing is fine.
pub fn remove(table: &str) {
    let _ = std::process::Command::new(nft_binary())
        .args(["delete", "table", "inet", table])
        .stderr(std::process::Stdio::null())
        .status();
}

/// Packets dropped per (allocation, reason) since the table named `table`
/// loaded, read from `nft -j list table`. Drops in the `unattributed` chain
/// are keyed by allocation 0.
pub fn read_counters(table: &str) -> R<BTreeMap<(i64, String), u64>> {
    let output = std::process::Command::new(nft_binary())
        .args(["-j", "list", "table", "inet", table])
        .output()
        .context("run nft -j list")?;
    if !output.status.success() {
        bail!(
            "nft -j list failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let json: Value = serde_json::from_slice(&output.stdout).context("parse nft -j output")?;
    Ok(parse_counters(&json))
}

/// The pure half of [`read_counters`].
pub fn parse_counters(json: &Value) -> BTreeMap<(i64, String), u64> {
    let mut out = BTreeMap::new();
    let Some(items) = json.get("nftables").and_then(Value::as_array) else {
        return out;
    };
    for item in items {
        let Some(rule) = item.get("rule") else {
            continue;
        };
        let chain = rule
            .get("chain")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let allocation_id = match chain.strip_prefix("alloc_") {
            Some(id) => match id.parse::<i64>() {
                Ok(id) => id,
                Err(_) => continue,
            },
            None if chain == "unattributed" => 0,
            None => continue,
        };
        let packets: u64 = rule
            .get("expr")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|expr| expr.get("counter"))
            .filter_map(|counter| counter.get("packets").and_then(Value::as_u64))
            .sum();
        if packets == 0 {
            continue;
        }
        let reason = rule
            .get("comment")
            .and_then(Value::as_str)
            .unwrap_or("host guard")
            .to_string();
        *out.entry((allocation_id, reason)).or_insert(0) += packets;
    }
    out
}

/// The worker's kernel guard: what it loaded last, and the counters already
/// reported, so each heartbeat reports only new drops.
#[derive(Debug, Default)]
pub struct KernelGuard {
    loaded: Option<GuardTable>,
    reported: BTreeMap<(i64, String), u64>,
}

impl KernelGuard {
    /// Loads `table` when it differs from what is loaded. A reload resets
    /// the kernel's counters, so the reported baseline resets with it.
    pub fn reconcile(&mut self, table: GuardTable) -> R<bool> {
        if self.loaded.as_ref() == Some(&table) {
            return Ok(false);
        }
        apply(&table)?;
        self.loaded = Some(table);
        self.reported.clear();
        Ok(true)
    }

    /// New drops since the last call: `(allocation, reason, packets)`.
    pub fn new_drops(&mut self, current: BTreeMap<(i64, String), u64>) -> Vec<(i64, String, u64)> {
        let mut out = Vec::new();
        for (key, total) in &current {
            let before = self.reported.get(key).copied().unwrap_or(0);
            if *total > before {
                out.push((key.0, key.1.clone(), total - before));
            }
        }
        self.reported = current;
        out
    }

    /// Allocations in the loaded table.
    pub fn allocations(&self) -> usize {
        self.loaded
            .as_ref()
            .map(|table| table.allocations.len())
            .unwrap_or(0)
    }
}

/// The cgroup leaf an allocation's tasks run in, when it exists on this
/// node: `<parent>/share/legion-<id>` or `<parent>/reserve/legion-<id>`.
pub fn existing_leaf(cgroup_parent: &str, allocation_id: i64) -> Option<String> {
    let parent = cgroup_parent.trim_matches('/');
    ["share", "reserve"].into_iter().find_map(|slice| {
        let relative = format!("{parent}/{slice}/legion-{allocation_id}");
        std::path::Path::new(crate::task_plugins::CGROUP_ROOT)
            .join(&relative)
            .is_dir()
            .then_some(relative)
    })
}

#[cfg(test)]
mod kernel_guard_test {
    use super::*;

    fn table() -> GuardTable {
        GuardTable {
            cgroup_parent: "legion".into(),
            control_ports: [4747, 4748, 4749, 4848].into_iter().collect(),
            deny_v4: vec!["192.0.2.10/32".into()],
            deny_v6: vec![],
            allow: vec![("10.0.1.7".into(), 9100)],
            allocations: vec![
                GuardedAllocation {
                    allocation_id: 88213,
                    cgroup: "legion/share/legion-88213".into(),
                    vip_block: Some("127.42.7".into()),
                    loopback_ports: vec![20417],
                    resolver: None,
                    gated: false,
                    host_sockets: true,
                },
                GuardedAllocation {
                    allocation_id: 9,
                    cgroup: "legion/reserve/legion-9".into(),
                    vip_block: None,
                    loopback_ports: vec![],
                    resolver: None,
                    gated: false,
                    host_sockets: true,
                },
            ],
        }
    }

    /// A manager's system worker and a standalone worker on one host load
    /// different tables and pin under different directories; the default
    /// parent keeps the historical names.
    #[test]
    fn kernel_objects_are_named_after_the_cgroup_parent_test() {
        assert_eq!(table_name("legion"), "legion_host_guard");
        assert_eq!(table_name("/legion/"), "legion_host_guard");
        assert_eq!(table_name(""), "legion_host_guard");
        assert_eq!(
            bpf_pin_dir("legion"),
            std::path::PathBuf::from("/sys/fs/bpf/legion")
        );
        assert_eq!(
            table_name("legion-system"),
            "legion_host_guard_legion-system"
        );
        assert_eq!(
            bpf_pin_dir("legion-system"),
            std::path::PathBuf::from("/sys/fs/bpf/legion_legion-system")
        );
        assert_ne!(table_name("legion-system"), table_name("legion"));
        assert_ne!(bpf_pin_dir("legion-system"), bpf_pin_dir("legion"));
        // Nested and odd parents become one safe name.
        assert_eq!(table_name("ci/workers"), "legion_host_guard_ci_workers");
        assert_eq!(instance_suffix("a b$c"), Some("a_b_c".to_string()));

        // The rendered transaction loads, deletes, and reloads its own table.
        let mut system = table();
        system.cgroup_parent = "legion-system".into();
        let script = system.render();
        assert!(script.starts_with(
            "table inet legion_host_guard_legion-system\ndelete table inet \
             legion_host_guard_legion-system\ntable inet legion_host_guard_legion-system {"
        ));
        assert!(!script.contains("table inet legion_host_guard\n"));
    }

    #[test]
    fn the_table_matches_the_golden_rendering_test() {
        let expected = r#"table inet legion_host_guard
delete table inet legion_host_guard
table inet legion_host_guard {
  set control_ports { type inet_service; elements = { 4747, 4748, 4749, 4848 } }
  set metadata_v4 { type ipv4_addr; flags interval; elements = { 169.254.0.0/16, 100.100.100.200 } }
  set metadata_v6 { type ipv6_addr; flags interval; elements = { fe80::/10, fd00:ec2::254 } }
  set deny_v4 { type ipv4_addr; flags interval; elements = { 192.0.2.10/32 } }
  set datapath_ports { type inet_service; elements = { 20417 } }
  chain output {
    type filter hook output priority filter; policy accept;
    socket cgroupv2 level 1 "legion" jump allocations
  }
  chain allocations {
    socket cgroupv2 level 3 "legion/share/legion-88213" jump alloc_88213
    socket cgroupv2 level 3 "legion/reserve/legion-9" jump alloc_9
    jump unattributed
  }
  chain alloc_88213 {
    ip daddr 127.42.7.0/24 accept
    ip daddr 127.0.0.1 tcp dport { 20417 } accept
    ip daddr 10.0.1.7 tcp dport 9100 accept comment "hostGuard.allow"
    ip daddr @metadata_v4 counter drop comment "cloud metadata"
    ip6 daddr @metadata_v6 counter drop comment "cloud metadata"
    ip daddr @deny_v4 counter drop comment "hostGuard.deny"
    fib daddr type local tcp dport @control_ports counter drop comment "a Legion control port"
    ip daddr 127.42.0.0/16 counter drop comment "another allocation's datapath"
    ip daddr 127.0.0.0/8 tcp dport @datapath_ports counter drop comment "another allocation's datapath"
  }
  chain alloc_9 {
    ip daddr 10.0.1.7 tcp dport 9100 accept comment "hostGuard.allow"
    ip daddr @metadata_v4 counter drop comment "cloud metadata"
    ip6 daddr @metadata_v6 counter drop comment "cloud metadata"
    ip daddr @deny_v4 counter drop comment "hostGuard.deny"
    fib daddr type local tcp dport @control_ports counter drop comment "a Legion control port"
    ip daddr 127.42.0.0/16 counter drop comment "another allocation's datapath"
    ip daddr 127.0.0.0/8 tcp dport @datapath_ports counter drop comment "another allocation's datapath"
  }
  chain unattributed {
    ip daddr 10.0.1.7 tcp dport 9100 accept comment "hostGuard.allow"
    ip daddr @metadata_v4 counter drop comment "cloud metadata"
    ip6 daddr @metadata_v6 counter drop comment "cloud metadata"
    ip daddr @deny_v4 counter drop comment "hostGuard.deny"
    fib daddr type local tcp dport @control_ports counter drop comment "a Legion control port"
    ip daddr 127.42.0.0/16 counter drop comment "another allocation's datapath"
    ip daddr 127.0.0.0/8 tcp dport @datapath_ports counter drop comment "another allocation's datapath"
  }
}
"#;
        assert_eq!(table().render(), expected);
    }

    #[test]
    fn a_nested_cgroup_parent_matches_at_its_own_level_test() {
        let mut table = table();
        table.cgroup_parent = "system.slice/legion".into();
        table.allocations[0].cgroup = "system.slice/legion/share/legion-88213".into();
        let rendered = table.render();
        assert!(
            rendered.contains("socket cgroupv2 level 2 \"system.slice/legion\" jump allocations")
        );
        assert!(rendered.contains(
            "socket cgroupv2 level 4 \"system.slice/legion/share/legion-88213\" jump alloc_88213"
        ));
    }

    #[test]
    fn counters_are_read_per_allocation_and_reason_test() {
        let json = serde_json::json!({ "nftables": [
            { "metainfo": { "json_schema_version": 1 } },
            { "rule": { "family": "inet", "table": TABLE, "chain": "alloc_88213",
                "comment": DROP_METADATA,
                "expr": [ { "when": {} }, { "counter": { "packets": 3, "bytes": 180 } }, { "drop": null } ] } },
            { "rule": { "family": "inet", "table": TABLE, "chain": "alloc_88213",
                "comment": DROP_METADATA,
                "expr": [ { "counter": { "packets": 1, "bytes": 80 } } ] } },
            { "rule": { "family": "inet", "table": TABLE, "chain": "alloc_88213",
                "comment": DROP_CONTROL,
                "expr": [ { "counter": { "packets": 0, "bytes": 0 } } ] } },
            { "rule": { "family": "inet", "table": TABLE, "chain": "unattributed",
                "comment": DROP_CONTROL,
                "expr": [ { "counter": { "packets": 2, "bytes": 120 } } ] } },
            { "rule": { "family": "inet", "table": TABLE, "chain": "allocations",
                "expr": [ { "jump": { "target": "alloc_88213" } } ] } },
        ]});
        let counters = parse_counters(&json);
        assert_eq!(counters.get(&(88213, DROP_METADATA.to_string())), Some(&4));
        assert_eq!(counters.get(&(0, DROP_CONTROL.to_string())), Some(&2));
        assert_eq!(counters.len(), 2, "{counters:?}");

        // Only new drops are reported each heartbeat.
        let mut guard = KernelGuard::default();
        assert_eq!(
            guard.new_drops(counters.clone()),
            vec![
                (0, DROP_CONTROL.to_string(), 2),
                (88213, DROP_METADATA.to_string(), 4)
            ]
        );
        assert!(guard.new_drops(counters.clone()).is_empty());
        let mut more = counters;
        more.insert((88213, DROP_METADATA.to_string()), 6);
        assert_eq!(
            guard.new_drops(more),
            vec![(88213, DROP_METADATA.to_string(), 2)]
        );
    }

    /// `nft -c` (parse and check, load nothing) accepts the rendered
    /// transaction. Needs root and `nft`; skips itself otherwise. The cgroup
    /// paths must exist, so it borrows a top-level cgroup the host has.
    #[test]
    fn the_rendered_table_passes_nft_check_test() -> R<()> {
        let root = std::path::Path::new(crate::task_plugins::CGROUP_ROOT);
        let parent = ["unified", "system.slice", "init.scope", "user.slice"]
            .into_iter()
            .find(|dir| root.join(dir).is_dir());
        let nft_works = std::process::Command::new(nft_binary())
            .args(["-c", "list", "tables"])
            .output()
            .is_ok_and(|output| output.status.success());
        let (Some(parent), true) = (parent, nft_works) else {
            eprintln!("skipping: needs root, nft, and a cgroup to borrow");
            return Ok(());
        };
        let mut table = table();
        table.cgroup_parent = parent.to_string();
        // Allocation leaves must exist for `nft` to resolve them, so every
        // allocation borrows the same cgroup here.
        for allocation in &mut table.allocations {
            allocation.cgroup = parent.to_string();
        }
        let script = table.render();
        let script = script.replace(&table.name(), "legion_host_guard_check");
        let mut child = std::process::Command::new(nft_binary())
            .args(["-c", "-f", "-"])
            .stdin(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        use std::io::Write as _;
        child
            .stdin
            .take()
            .context("stdin")?
            .write_all(script.as_bytes())?;
        let output = child.wait_with_output()?;
        assert!(
            output.status.success(),
            "nft -c refused the table: {}\n{script}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    /// Loads the rendered table into a real kernel and drops a metadata
    /// connection from inside an allocation's cgroup, then reads the
    /// counter back. Needs root, cgroup v2, and `nft`:
    ///
    /// ```text
    /// sudo -E cargo test --lib kernel_guard_loads -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "requires root, cgroup v2, and nft"]
    fn kernel_guard_loads_and_drops_metadata_from_an_allocation_test() -> R<()> {
        if probe() != Availability::Active {
            eprintln!("skipping: {:?}", probe());
            return Ok(());
        }
        let parent = format!("legion-kg-test-{}", std::process::id());
        let leaf = format!("{parent}/share/legion-1");
        let root = std::path::Path::new(crate::task_plugins::CGROUP_ROOT);
        std::fs::create_dir_all(root.join(&leaf))?;
        let table = GuardTable {
            cgroup_parent: parent.clone(),
            control_ports: [4848].into_iter().collect(),
            allocations: vec![GuardedAllocation {
                allocation_id: 1,
                cgroup: leaf.clone(),
                vip_block: None,
                loopback_ports: vec![],
                resolver: None,
                gated: false,
                host_sockets: true,
            }],
            ..Default::default()
        };
        let mut guard = KernelGuard::default();
        guard.reconcile(table)?;
        // A child in the leaf dials the metadata address: EPERM at once.
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "echo $$ > {}/cgroup.procs && exec timeout 3 bash -c 'exec 3<>/dev/tcp/169.254.169.254/80' 2>&1",
                root.join(&leaf).display()
            ))
            .output()?;
        let said = String::from_utf8_lossy(&status.stdout).to_string();
        let name = table_name(&parent);
        let drops = guard.new_drops(read_counters(&name)?);
        remove(&name);
        let _ = std::fs::remove_dir(root.join(&leaf));
        let _ = std::fs::remove_dir(root.join(&parent).join("share"));
        let _ = std::fs::remove_dir(root.join(&parent));
        assert!(!status.status.success(), "the dial must fail: {said}");
        assert!(
            drops
                .iter()
                .any(|(id, reason, packets)| *id == 1 && reason == DROP_METADATA && *packets > 0),
            "{drops:?} / {said}"
        );
        Ok(())
    }
}
