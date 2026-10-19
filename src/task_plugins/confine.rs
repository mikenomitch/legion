//! Confining a gated task on macOS, which has no network namespaces
//! (internal_docs/plans/macos-gated-plan.md; docs/network-handlers.md,
//! "Gated mode on macOS").
//!
//! A confined allocation's datapath listens on the node's loopback, at
//! ports placement assigned (`AllocNetwork::confined_ports`). The runtime
//! makes those the only places the task can reach:
//!
//! - an `exec` task runs under a Seatbelt profile ([`seatbelt_command`]);
//! - an Apple `container` VM runs on Legion's gated network, where pf drops
//!   everything from the VM but `rdr`s to its own ports ([`pf_anchor`],
//!   [`PfConfiner`]).
//!
//! Everything that decides *what* to run is a pure function here, tested on
//! any platform; only [`PfConfiner`]'s command runner touches the system.

use crate::prelude::*;
use std::collections::BTreeMap;
use std::net::Ipv4Addr;

/// The Seatbelt launcher. Deprecated by Apple and present through current
/// macOS (calls entry 3).
pub const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

/// The SBPL profile a confined `exec` task runs under: outbound only to
/// `ports` on the loopback (its egress proxy and upstreams), no DNS (the
/// `mDNSResponder` socket), and listeners on the loopback only, where the
/// datapath's ingress listeners front them.
pub fn seatbelt_profile(ports: &[u16]) -> String {
    let mut profile = String::from(
        "(version 1)\n\
         (allow default)\n\
         (deny network-outbound)\n\
         (allow network-outbound (remote unix-socket))\n\
         (deny network-outbound (remote unix-socket (path-literal \"/private/var/run/mDNSResponder\")))\n",
    );
    for port in ports {
        profile.push_str(&format!(
            "(allow network-outbound (remote ip \"localhost:{port}\"))\n"
        ));
    }
    profile.push_str(
        "(deny network-bind (local ip \"*:*\"))\n\
         (allow network-bind (local ip \"localhost:*\"))\n",
    );
    profile
}

/// The command a confined `exec` task runs as: `sandbox-exec -p <profile>`
/// in front of its own. `sandbox-exec` applies the profile and `exec`s the
/// command, so the process Legion supervises is the task.
pub fn seatbelt_command(program: &str, args: &[String], ports: &[u16]) -> (String, Vec<String>) {
    let mut wrapped = vec![
        "-p".to_string(),
        seatbelt_profile(ports),
        program.to_string(),
    ];
    wrapped.extend(args.iter().cloned());
    (SANDBOX_EXEC.to_string(), wrapped)
}

/// Fail a confined start when Seatbelt is missing, never run unconfined.
pub fn require_seatbelt(allocation_id: i64) -> R<()> {
    if std::path::Path::new(SANDBOX_EXEC).exists() {
        return Ok(());
    }
    bail!(
        "allocation {allocation_id} is network.mode = \"gated\" but {SANDBOX_EXEC} is missing, \
         so it cannot be confined on macOS"
    )
}

// ---------------------------------------------------------------------------
// Apple container VMs: the gated network and pf
// ---------------------------------------------------------------------------

/// The Apple `container` network confined VMs run on (macOS 26 and later).
pub const GATED_NETWORK: &str = "legion-gated";
/// Its subnet; the gateway, `.1`, is the Mac (calls entry 4: a constant for
/// now).
pub const GATED_SUBNET: &str = "192.168.239.0/24";
pub const GATED_GATEWAY: Ipv4Addr = Ipv4Addr::new(192, 168, 239, 1);
/// Legion's pf anchor. macOS's stock `/etc/pf.conf` evaluates `com.apple/*`
/// for both translation and filter rules, so no system file is edited.
pub const PF_ANCHOR: &str = "com.apple/legion";

/// The anchor's rules for the confined VMs `vms` (address → the host ports
/// it may reach): one `rdr pass` per VM to its own listeners on the Mac's
/// loopback, then a block that drops everything else from the gated subnet
/// (DHCP to the gateway aside). The block alone is what a VM gets before its
/// address is known.
pub fn pf_anchor(vms: &BTreeMap<Ipv4Addr, Vec<u16>>) -> String {
    let mut rules = String::from(
        "# Legion: gated Apple container VMs (internal_docs/plans/macos-gated-plan.md)\n",
    );
    for (address, ports) in vms {
        if ports.is_empty() {
            continue;
        }
        let ports: Vec<String> = ports.iter().map(u16::to_string).collect();
        rules.push_str(&format!(
            "rdr pass inet proto tcp from {address} to {GATED_GATEWAY} port {{ {} }} -> 127.0.0.1\n",
            ports.join(" ")
        ));
    }
    rules.push_str(&format!(
        "pass in quick inet proto udp from {GATED_SUBNET} to any port 67\n\
         block drop in quick inet from {GATED_SUBNET} to any\n"
    ));
    rules
}

/// The Apple `container run` arguments that put a VM on the gated network.
pub fn gated_network_args() -> [String; 2] {
    ["--network".to_string(), GATED_NETWORK.to_string()]
}

/// Point a VM's datapath addresses at the Mac: inside the VM `127.0.0.1` is
/// the VM's own loopback, so `LEGION_UPSTREAM_*`, `LEGION_EGRESS_PROXY`, and
/// the proxy variables name the gated network's gateway instead, which pf
/// `rdr`s back to the Mac's loopback.
pub fn rewrite_datapath_env<'a>(env: impl IntoIterator<Item = (&'a String, &'a mut String)>) {
    let gateway = GATED_GATEWAY.to_string();
    for (key, value) in env {
        let datapath = key.starts_with("LEGION_UPSTREAM_")
            || key == "LEGION_EGRESS_PROXY"
            || matches!(
                key.as_str(),
                "HTTP_PROXY"
                    | "HTTPS_PROXY"
                    | "ALL_PROXY"
                    | "http_proxy"
                    | "https_proxy"
                    | "all_proxy"
            );
        if !datapath {
            continue;
        }
        if value == "127.0.0.1" {
            *value = gateway.clone();
        } else if value.contains("127.0.0.1:") {
            *value = value.replace("127.0.0.1:", &format!("{gateway}:"));
        }
    }
}

/// A VM's address on the gated network, from `container inspect` JSON: the
/// IPv4 address of its attachment to [`GATED_NETWORK`], without its prefix
/// length. Read leniently, since the output is Apple's to change: any
/// object under a `networks` array that names the network and carries an
/// `address` (or `ipv4Address`) counts.
pub fn inspect_address(inspect: &Value) -> Option<Ipv4Addr> {
    fn walk(value: &Value) -> Option<Ipv4Addr> {
        match value {
            Value::Array(items) => items.iter().find_map(walk),
            Value::Object(map) => {
                if let Some(networks) = map.get("networks").and_then(Value::as_array) {
                    for network in networks {
                        let named = ["network", "name", "id"].iter().any(|key| {
                            network.get(*key).and_then(Value::as_str) == Some(GATED_NETWORK)
                        });
                        let address = ["address", "ipv4Address"]
                            .iter()
                            .find_map(|key| network.get(*key).and_then(Value::as_str));
                        if let (true, Some(address)) = (named, address) {
                            let bare = address.split('/').next().unwrap_or(address);
                            if let Ok(ip) = bare.parse() {
                                return Some(ip);
                            }
                        }
                    }
                }
                map.values().find_map(walk)
            }
            _ => None,
        }
    }
    walk(inspect)
}

/// The confined VMs on this node and the pf anchor that confines them.
///
/// The first VM creates the gated network, enables pf (holding its
/// reference token), and loads the base block before the VM starts. Each VM's
/// `rdr` follows once `container inspect` reports its address. The last
/// one to go flushes the anchor and releases pf. Every step that fails
/// fails the VM's start: a gated VM never runs unconfined.
pub struct PfConfiner {
    state: Mutex<PfState>,
    run: Box<dyn Fn(&str, &[&str], Option<&str>) -> R<String> + Send + Sync>,
}

#[derive(Default)]
struct PfState {
    /// allocation → (its VM's address once known, its host ports).
    vms: BTreeMap<i64, (Option<Ipv4Addr>, Vec<u16>)>,
    /// pf's enable reference, held while any confined VM exists.
    token: Option<String>,
    network_ready: bool,
}

impl PfConfiner {
    /// The node's confiner, running real commands.
    pub fn node() -> &'static PfConfiner {
        static NODE: std::sync::OnceLock<PfConfiner> = std::sync::OnceLock::new();
        NODE.get_or_init(|| PfConfiner::with_runner(run_command))
    }

    /// A confiner whose commands go to `run(program, args, stdin)`, which
    /// returns stdout (tests).
    pub fn with_runner(
        run: impl Fn(&str, &[&str], Option<&str>) -> R<String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            state: Mutex::default(),
            run: Box::new(run),
        }
    }

    /// Before a confined VM starts: the gated network exists, pf is on, and
    /// the anchor blocks the subnet.
    pub fn prepare(&self, allocation_id: i64, ports: Vec<u16>) -> R<()> {
        if !is_root() {
            bail!(
                "allocation {allocation_id}: gated Apple containers need the worker to run as root \
                 to load the pf rules that confine them"
            );
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.network_ready {
            let listed = (self.run)("container", &["network", "list", "--format", "json"], None)
                .unwrap_or_default();
            if !listed.contains(&format!("\"{GATED_NETWORK}\"")) {
                (self.run)(
                    "container",
                    &["network", "create", GATED_NETWORK, "--subnet", GATED_SUBNET],
                    None,
                )
                .map_err(|err| {
                    anyhow!(
                        "allocation {allocation_id}: gated Apple containers need macOS 26 or \
                         later (container network create failed: {err:#})"
                    )
                })?;
            }
            state.network_ready = true;
        }
        if state.token.is_none() {
            let enabled = (self.run)("pfctl", &["-E"], None)
                .with_context(|| format!("allocation {allocation_id}: enabling pf"))?;
            state.token = Some(pf_token(&enabled).unwrap_or_default());
        }
        state.vms.insert(allocation_id, (None, ports));
        self.load(&state)
            .with_context(|| format!("allocation {allocation_id}: loading the pf anchor"))
    }

    /// The VM's address is known: let it reach its own ports.
    pub fn admit(&self, allocation_id: i64, address: Ipv4Addr) -> R<()> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let Some(entry) = state.vms.get_mut(&allocation_id) else {
            bail!("allocation {allocation_id} was not prepared for confinement");
        };
        entry.0 = Some(address);
        self.load(&state)
    }

    /// The VM stopped: drop its rule, and release pf with the last one.
    pub fn release(&self, allocation_id: i64) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.vms.remove(&allocation_id).is_none() {
            return;
        }
        if state.vms.is_empty() {
            let _ = (self.run)("pfctl", &["-a", PF_ANCHOR, "-F", "all"], None);
            if let Some(token) = state.token.take().filter(|token| !token.is_empty()) {
                let _ = (self.run)("pfctl", &["-X", &token], None);
            }
        } else if let Err(err) = self.load(&state) {
            warn!(allocation_id, error = ?err, "Failed to reload the pf anchor after a confined VM stopped");
        }
    }

    /// The anchor as it stands (tests, `net doctor`).
    pub fn anchor(&self) -> String {
        anchor_text(&self.state.lock().unwrap_or_else(|p| p.into_inner()))
    }

    fn load(&self, state: &PfState) -> R<()> {
        (self.run)(
            "pfctl",
            &["-a", PF_ANCHOR, "-f", "-"],
            Some(&anchor_text(state)),
        )
        .map(|_| ())
    }
}

fn anchor_text(state: &PfState) -> String {
    let vms: BTreeMap<Ipv4Addr, Vec<u16>> = state
        .vms
        .values()
        .filter_map(|(address, ports)| Some(((*address)?, ports.clone())))
        .collect();
    pf_anchor(&vms)
}

/// Before a confined Apple container VM launches: pf blocks the gated
/// subnet, the VM's datapath env vars name the gateway, and the returned
/// arguments put it on the gated network. `None` for an allocation that is
/// not confined, which launches as before.
pub fn prepare_vm<'a>(
    allocation: &crate::structs::Allocation,
    env: impl IntoIterator<Item = (&'a String, &'a mut String)>,
) -> R<Option<[String; 2]>> {
    let Some(network) = allocation.network.as_ref().filter(|n| n.is_confined()) else {
        return Ok(None);
    };
    PfConfiner::node().prepare(allocation.id, network.confined_ports())?;
    rewrite_datapath_env(env);
    Ok(Some(gated_network_args()))
}

/// How long a confined VM may take to report its address before the worker
/// stops waiting (it stays blocked, reaching nothing).
pub const ADMIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// After a confined VM launched: read its address from `container inspect`
/// and let it reach its own ports. Runs in the background, because the
/// address exists only once the VM is up; until then the base block keeps it
/// confined.
pub fn admit_when_ready(allocation_id: i64, container: String) {
    std::thread::spawn(move || {
        let started = std::time::Instant::now();
        let mut warned = std::time::Instant::now();
        while started.elapsed() < ADMIT_TIMEOUT {
            let inspected = run_command("container", &["inspect", &container], None)
                .ok()
                .and_then(|text| serde_json::from_str::<Value>(&text).ok());
            if let Some(address) = inspected.as_ref().and_then(inspect_address) {
                match PfConfiner::node().admit(allocation_id, address) {
                    Ok(()) => {
                        info!(allocation_id, %address, "Confined Apple container admitted to its datapath ports")
                    }
                    Err(err) => {
                        warn!(allocation_id, error = ?err, "Failed to admit a confined Apple container; it stays blocked")
                    }
                }
                return;
            }
            if warned.elapsed() >= std::time::Duration::from_secs(10) {
                warn!(allocation_id, %container, "Confined Apple container has no address on the gated network yet; it stays blocked");
                warned = std::time::Instant::now();
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        warn!(allocation_id, %container, "Confined Apple container never reported an address; it stays blocked and reaches nothing");
    });
}

/// A confined VM stopped.
pub fn release_vm(allocation: &crate::structs::Allocation) {
    if allocation.network.as_ref().is_some_and(|n| n.is_confined()) {
        PfConfiner::node().release(allocation.id);
    }
}

/// `pfctl -E` prints `Token : <n>` (to stderr or stdout, by version).
fn pf_token(output: &str) -> Option<String> {
    output
        .lines()
        .find_map(|line| line.trim().strip_prefix("Token :"))
        .map(|token| token.trim().to_string())
}

fn is_root() -> bool {
    #[cfg(unix)]
    {
        // SAFETY: geteuid has no preconditions and cannot fail.
        unsafe { libc::geteuid() == 0 }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

fn run_command(program: &str, args: &[&str], stdin: Option<&str>) -> R<String> {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let mut child = Command::new(program)
        .args(args)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("running {program}"))?;
    if let (Some(input), Some(mut pipe)) = (stdin, child.stdin.take()) {
        pipe.write_all(input.as_bytes())?;
    }
    let output = child.wait_with_output()?;
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    if !output.status.success() {
        bail!("{program} {}: {}", args.join(" "), text.trim());
    }
    Ok(text)
}

#[cfg(test)]
mod confine_test {
    use super::*;

    #[test]
    fn the_seatbelt_profile_allows_exactly_the_listener_ports_test() {
        let profile = seatbelt_profile(&[20017, 20018]);
        assert!(profile.starts_with("(version 1)\n(allow default)\n(deny network-outbound)\n"));
        assert!(profile.contains("(allow network-outbound (remote ip \"localhost:20017\"))"));
        assert!(profile.contains("(allow network-outbound (remote ip \"localhost:20018\"))"));
        assert_eq!(profile.matches("(remote ip ").count(), 2, "{profile}");
        assert!(profile.contains("mDNSResponder"), "no DNS: {profile}");
        assert!(profile.contains("(allow network-bind (local ip \"localhost:*\"))"));

        let (program, args) = seatbelt_command("python3", &["-m".into(), "app".into()], &[20017]);
        assert_eq!(program, SANDBOX_EXEC);
        assert_eq!(args[0], "-p");
        assert_eq!(args[1], seatbelt_profile(&[20017]));
        assert_eq!(&args[2..], ["python3", "-m", "app"]);
    }

    #[test]
    fn the_pf_anchor_rdrs_each_vm_to_its_own_ports_ahead_of_the_block_test() {
        let base = pf_anchor(&BTreeMap::new());
        assert!(base.contains("block drop in quick inet from 192.168.239.0/24 to any"));
        assert!(!base.contains("rdr"));
        let vms: BTreeMap<Ipv4Addr, Vec<u16>> = [
            ("192.168.239.5".parse().unwrap(), vec![20017, 20018]),
            ("192.168.239.6".parse().unwrap(), vec![20030]),
        ]
        .into_iter()
        .collect();
        let anchor = pf_anchor(&vms);
        let rdr = anchor
            .find("rdr pass inet proto tcp from 192.168.239.5 to 192.168.239.1 port { 20017 20018 } -> 127.0.0.1")
            .expect(&anchor);
        assert!(anchor.contains("from 192.168.239.6 to 192.168.239.1 port { 20030 }"));
        // Translation rules come before filter rules in a pf ruleset.
        assert!(rdr < anchor.find("block drop").unwrap(), "{anchor}");
    }

    #[test]
    fn datapath_env_names_the_gateway_inside_a_vm_test() {
        let mut env: BTreeMap<String, String> = [
            ("LEGION_UPSTREAM_DB", "127.0.0.1:20018"),
            ("LEGION_UPSTREAM_HOST_DB", "127.0.0.1"),
            ("LEGION_UPSTREAM_PORT_DB", "20018"),
            ("HTTPS_PROXY", "http://127.0.0.1:20017"),
            ("https_proxy", "http://127.0.0.1:20017"),
            ("NO_PROXY", "localhost,127.0.0.0/8,svc.legion"),
            ("APP_URL", "http://127.0.0.1:8080"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        rewrite_datapath_env(env.iter_mut());
        assert_eq!(env["LEGION_UPSTREAM_DB"], "192.168.239.1:20018");
        assert_eq!(env["LEGION_UPSTREAM_HOST_DB"], "192.168.239.1");
        assert_eq!(env["LEGION_UPSTREAM_PORT_DB"], "20018");
        assert_eq!(env["HTTPS_PROXY"], "http://192.168.239.1:20017");
        assert_eq!(env["https_proxy"], "http://192.168.239.1:20017");
        // Not a datapath address: the task's own config is left alone.
        assert_eq!(env["NO_PROXY"], "localhost,127.0.0.0/8,svc.legion");
        assert_eq!(env["APP_URL"], "http://127.0.0.1:8080");
    }

    #[test]
    fn the_vm_address_is_read_from_container_inspect_test() {
        // The shape `container inspect` prints (an array of containers, each
        // with its network attachments); fields around it vary by version.
        let inspect = json!([{
            "configuration": { "id": "legion-n1-7" },
            "status": "running",
            "networks": [
                { "network": "default", "address": "192.168.64.9/24", "gateway": "192.168.64.1" },
                { "network": GATED_NETWORK, "address": "192.168.239.5/24", "gateway": "192.168.239.1" }
            ]
        }]);
        assert_eq!(
            inspect_address(&inspect),
            Some("192.168.239.5".parse().unwrap())
        );
        assert_eq!(
            inspect_address(&json!([{ "status": "running", "networks": [] }])),
            None
        );
    }

    #[test]
    fn the_confiner_blocks_first_admits_by_address_and_releases_pf_test() {
        let calls: Arc<Mutex<Vec<String>>> = Arc::default();
        let log = calls.clone();
        let confiner = PfConfiner::with_runner(move |program, args, stdin| {
            log.lock().unwrap().push(format!(
                "{program} {}{}",
                args.join(" "),
                stdin.map(|s| format!("\n{s}")).unwrap_or_default()
            ));
            Ok(match (program, args.first().copied()) {
                ("pfctl", Some("-E")) => "pf enabled\nToken : 1234\n".to_string(),
                ("container", _) => "[]".to_string(),
                _ => String::new(),
            })
        });
        if !is_root() {
            let err = confiner.prepare(7, vec![20017]).unwrap_err().to_string();
            assert!(err.contains("run as root"), "{err}");
            return;
        }
        confiner.prepare(7, vec![20017, 20018]).unwrap();
        // Before the address is known, the VM is only blocked.
        assert!(!confiner.anchor().contains("rdr"));
        confiner.admit(7, "192.168.239.5".parse().unwrap()).unwrap();
        assert!(confiner
            .anchor()
            .contains("from 192.168.239.5 to 192.168.239.1 port { 20017 20018 }"));
        confiner.release(7);
        let calls = calls.lock().unwrap().join("\n");
        assert!(
            calls.contains("container network create legion-gated --subnet 192.168.239.0/24"),
            "{calls}"
        );
        assert!(calls.contains("pfctl -E"), "{calls}");
        assert!(calls.contains("pfctl -a com.apple/legion -f -"), "{calls}");
        assert!(
            calls.contains("pfctl -a com.apple/legion -F all"),
            "{calls}"
        );
        assert!(calls.contains("pfctl -X 1234"), "{calls}");
    }
}
