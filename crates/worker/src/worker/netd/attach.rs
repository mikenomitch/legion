//! How one allocation is attached to the node datapath.
//!
//! Every listener the datapath runs for an allocation — its upstreams and
//! VIPs, its resolver, its egress proxy, and the ingress listeners in front of
//! its ports — has to know two things about the allocation: *where* to bind
//! (the host's loopback, inside a gated namespace, or on a sandbox guest's
//! vsock host socket) and *whether to fail closed* while its policy is
//! pending or its edge-boot gate is shut. Both follow from one fact, decided
//! once per allocation by [`Attachment::of`] from its network payload and
//! runtime, and carried in every spec.
//!
//! A new capture mode (a bridge, a TUN device, transparent capture) is a new
//! variant here plus its arms in [`Attachment::listen`] and
//! [`Attachment::task_dial`]; the listener code does not change.

use crate::structs::{AllocNetwork, TaskAllocation};
use crate::worker::netd::ingress::TaskDial;
use crate::worker::netd::listen::ListenAddr;

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

/// How an allocation's traffic reaches the datapath, and how the datapath
/// reaches the allocation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Attachment {
    /// `network.mode = "host"`: the datapath binds on the node's loopback, and
    /// egress policy is cooperative (a task that dials a raw address goes
    /// around it). Also a sandbox with neither a gate nor a TAP device, whose
    /// guest has only its own loopback: nothing on the node reaches it, and
    /// its listeners on the node's loopback go unused.
    #[default]
    Host,
    /// `network.mode = "gated"` for `exec` and `container`: an empty network
    /// namespace whose only routes are the datapath's listeners, bound inside
    /// it. Fails closed.
    Netns { path: PathBuf },
    /// `network.mode = "gated"` for a `sandbox`: a microVM with no NIC. The
    /// datapath binds the VMM's vsock host sockets in the guest's sandbox dir
    /// (`<scratch>/vsock.sock_<port>`), which the guest agent relays the
    /// workload's loopback ports to. Fails closed.
    GuestVsock { scratch: PathBuf },
    /// A TAP-wired `sandbox` (`config.network.mode = "tap"`): the guest has its
    /// own address on a bridge and host networking for policy. The datapath
    /// reaches its ports at that address.
    GuestTap { scratch: PathBuf },
    /// `network.mode = "gated"` on a node without network namespaces
    /// (macOS): the datapath binds on the node's loopback, the egress proxy
    /// at `proxy`, and the runtime confines the task to those listeners
    /// (Seatbelt for `exec`, pf for an Apple container VM). Fails closed.
    Confined { proxy: SocketAddr },
    /// An isolate task's envelope allocation (runtime `envelope`, its
    /// network carried over from the isolate task and always gated): no
    /// process, no namespace, no guest. The datapath binds Unix sockets in
    /// `dir` (`<allocs>/<id>/.isolate`), one per task-facing port — the
    /// egress proxy and each `connect` upstream — which the isolate
    /// supervisor dials on its isolates' behalf, and ingress dials the
    /// socket the supervisor serves per advertised port
    /// ([`isolate_ingress_socket`]). Fails closed.
    Isolate { dir: PathBuf },
}

impl Attachment {
    /// The attachment of one desired allocation, from its network payload,
    /// its runtime, and (for a sandbox) its runtime config. `alloc_root` is
    /// the worker's allocation directory root, which holds each sandbox's
    /// scratch dir.
    pub fn of(allocation: &TaskAllocation, network: &AllocNetwork, alloc_root: &Path) -> Self {
        let sandbox = allocation.task_runtime == "sandbox";
        // An isolate envelope has no process to confine and no guest: its
        // listeners are the supervisor's sockets, whatever the node is
        // (`Confined` included). Checked first so a macOS node's assigned
        // egress-proxy port is not bound for nothing.
        if allocation.task_runtime == crate::structs::session::ENVELOPE_RUNTIME
            && network.is_gated()
        {
            return Attachment::Isolate {
                dir: isolate_dir(alloc_root, allocation.allocation_id),
            };
        }
        if let Some(proxy) = network
            .is_confined()
            .then(|| network.egress_proxy.as_deref()?.parse().ok())
            .flatten()
        {
            return Attachment::Confined { proxy };
        }
        if network.is_gated() {
            return if sandbox {
                Attachment::GuestVsock {
                    scratch: sandbox_scratch(alloc_root, allocation.allocation_id),
                }
            } else {
                Attachment::Netns {
                    path: crate::worker::netd::netns::netns_path(allocation.allocation_id),
                }
            };
        }
        if sandbox && sandbox_is_tap(&allocation.task_config) {
            return Attachment::GuestTap {
                scratch: sandbox_scratch(alloc_root, allocation.allocation_id),
            };
        }
        Attachment::Host
    }

    /// Whether the allocation's listeners fail closed while its policy is
    /// pending or its edge-boot gate is shut: the datapath is the
    /// allocation's only way out, so "allow until told otherwise" would be a
    /// hole.
    pub fn is_gated(&self) -> bool {
        matches!(
            self,
            Attachment::Netns { .. }
                | Attachment::GuestVsock { .. }
                | Attachment::Confined { .. }
                | Attachment::Isolate { .. }
        )
    }

    /// Where the egress proxy listens, when it is not the namespace's or the
    /// guest's own `:1080`: a confined allocation's assigned port.
    pub fn egress_proxy(&self) -> Option<SocketAddr> {
        match self {
            Attachment::Confined { proxy } => Some(*proxy),
            _ => None,
        }
    }

    /// The gated namespace listeners bind inside, when there is one.
    pub fn netns(&self) -> Option<&Path> {
        match self {
            Attachment::Netns { path } => Some(path),
            _ => None,
        }
    }

    /// A gated sandbox guest's scratch dir, which holds its VMM's sockets.
    pub fn guest_vsock(&self) -> Option<&Path> {
        match self {
            Attachment::GuestVsock { scratch } => Some(scratch),
            _ => None,
        }
    }

    /// An isolate envelope's socket dir, which holds its datapath sockets.
    pub fn isolate_dir(&self) -> Option<&Path> {
        match self {
            Attachment::Isolate { dir } => Some(dir),
            _ => None,
        }
    }

    /// Whether the allocation's upstream VIPs are bound on the node. A gated
    /// sandbox guest serves its VIPs itself (its agent relays them), so the
    /// node binds none for it; an isolate addresses an upstream by name, so
    /// it has no use for one.
    pub fn serves_vips(&self) -> bool {
        !matches!(
            self,
            Attachment::GuestVsock { .. } | Attachment::Isolate { .. }
        )
    }

    /// Whether the allocation gets a per-allocation resolver. An isolate
    /// has no resolver of its own: the worker resolves on its behalf.
    pub fn wants_resolver(&self) -> bool {
        !matches!(self, Attachment::Isolate { .. })
    }

    /// Whether every port the allocation advertises is fronted by an ingress
    /// listener, chain or not: a sandbox guest has its own port space, which
    /// nothing else on the node reaches.
    pub fn fronts_every_port(&self) -> bool {
        matches!(
            self,
            Attachment::GuestVsock { .. } | Attachment::GuestTap { .. } | Attachment::Isolate { .. }
        )
    }

    /// Where a listener for the task-facing address `ip:port` binds: that
    /// address on the node's loopback or inside the namespace, or — for a
    /// guest reached over vsock, which has no address on the node — the
    /// VMM's host socket for `port`, whatever `ip` is.
    pub fn listen(&self, ip: IpAddr, port: u16) -> ListenAddr {
        match self {
            Attachment::GuestVsock { scratch } => ListenAddr::Unix(guest_socket(scratch, port)),
            Attachment::Isolate { dir } => ListenAddr::Unix(isolate_socket(dir, port)),
            Attachment::Netns { path } => ListenAddr::Tcp {
                netns: Some(path.clone()),
                addr: SocketAddr::new(ip, port),
            },
            Attachment::Host | Attachment::GuestTap { .. } | Attachment::Confined { .. } => {
                ListenAddr::Tcp {
                    netns: None,
                    addr: SocketAddr::new(ip, port),
                }
            }
        }
    }

    /// How an ingress listener in the host namespace reaches the task.
    pub fn task_dial(&self) -> TaskDial {
        match self {
            Attachment::Host | Attachment::Confined { .. } => TaskDial::Loopback { netns: None },
            Attachment::Netns { path } => TaskDial::Loopback {
                netns: Some(path.clone()),
            },
            Attachment::GuestVsock { scratch } => TaskDial::Vsock {
                scratch: scratch.clone(),
            },
            Attachment::GuestTap { scratch } => TaskDial::Bridge {
                scratch: scratch.clone(),
            },
            Attachment::Isolate { dir } => TaskDial::Isolate { dir: dir.clone() },
        }
    }
}

/// A sandbox's scratch dir (`<allocs>/<id>/.sandbox`).
fn sandbox_scratch(alloc_root: &Path, allocation_id: i64) -> PathBuf {
    crate::task_plugins::sandbox_dir_for(&alloc_root.join(allocation_id.to_string()))
}

/// An isolate envelope's socket dir (`<allocs>/<id>/.isolate`).
pub fn isolate_dir(alloc_root: &Path, allocation_id: i64) -> PathBuf {
    alloc_root
        .join(allocation_id.to_string())
        .join(ISOLATE_DIR_NAME)
}

/// The name of an isolate envelope's socket dir inside its allocation dir.
pub const ISOLATE_DIR_NAME: &str = ".isolate";

/// The socket an isolate envelope's listener for task-facing port `port`
/// binds (the egress proxy's 1080, an upstream's local port).
pub fn isolate_socket(dir: &Path, port: u16) -> PathBuf {
    dir.join(format!("net.sock_{port}"))
}

/// The socket the isolate supervisor serves for an advertised port of an
/// isolate task, by its label, which ingress dials.
pub fn isolate_ingress_socket(dir: &Path, label: &str) -> PathBuf {
    dir.join(format!("ingress.sock_{label}"))
}

/// The VMM's host socket for a guest-initiated stream to host port `port`.
pub fn guest_socket(scratch: &Path, port: u16) -> PathBuf {
    crate::worker::netd::guest_agent::host_socket_path(
        &crate::task_plugins::vsock_socket_base(scratch),
        u32::from(port),
    )
}

/// Whether a sandbox's runtime config wires it to a TAP device.
fn sandbox_is_tap(task_config: &str) -> bool {
    // Cheap filter first: almost no sandbox config names a TAP device.
    task_config.contains("tap")
        && serde_json::from_str::<serde_json::Value>(task_config)
            .is_ok_and(|config| crate::task_plugins::sandbox_config_is_tap(&config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::netpolicy::{MODE_GATED, MODE_HOST};
    use std::net::Ipv4Addr;

    fn allocation(id: i64, runtime: &str, config: &str) -> TaskAllocation {
        TaskAllocation {
            allocation_id: id,
            task_runtime: runtime.into(),
            task_config: config.into(),
            ..Default::default()
        }
    }

    fn network(mode: &str) -> AllocNetwork {
        AllocNetwork {
            mode: Some(mode.into()),
            ..Default::default()
        }
    }

    #[test]
    fn each_runtime_and_mode_attaches_one_way_test() {
        let root = Path::new("/allocs");
        let scratch = Path::new("/allocs/7/.sandbox").to_path_buf();
        assert_eq!(
            Attachment::of(&allocation(7, "exec", "{}"), &network(MODE_HOST), root),
            Attachment::Host
        );
        assert_eq!(
            Attachment::of(
                &allocation(7, "container", "{}"),
                &network(MODE_GATED),
                root
            ),
            Attachment::Netns {
                path: crate::worker::netd::netns::netns_path(7)
            }
        );
        assert_eq!(
            Attachment::of(&allocation(7, "sandbox", "{}"), &network(MODE_GATED), root),
            Attachment::GuestVsock {
                scratch: scratch.clone()
            }
        );
        let tap = r#"{"network":{"mode":"tap"}}"#;
        assert_eq!(
            Attachment::of(&allocation(7, "sandbox", tap), &network(MODE_HOST), root),
            Attachment::GuestTap { scratch }
        );
        // A loopback-only sandbox has nothing the node reaches.
        assert_eq!(
            Attachment::of(&allocation(7, "sandbox", "{}"), &network(MODE_HOST), root),
            Attachment::Host
        );
        // A row placed before modes were resolved reads as host.
        assert_eq!(
            Attachment::of(
                &allocation(7, "container", "{}"),
                &AllocNetwork::default(),
                root
            ),
            Attachment::Host
        );
    }

    /// A gated allocation placed with an egress proxy (a macOS node) is
    /// confined: host listeners, the proxy at its own port, failing closed.
    #[test]
    fn a_gated_allocation_with_an_egress_proxy_is_confined_test() {
        let root = Path::new("/allocs");
        let network = AllocNetwork {
            mode: Some(MODE_GATED.into()),
            egress_proxy: Some("127.0.0.1:20017".into()),
            ..Default::default()
        };
        for runtime in ["exec", "container", "sandbox"] {
            let attachment = Attachment::of(&allocation(7, runtime, "{}"), &network, root);
            assert_eq!(
                attachment,
                Attachment::Confined {
                    proxy: "127.0.0.1:20017".parse().unwrap()
                },
                "{runtime}"
            );
            assert!(attachment.is_gated());
            assert!(attachment.netns().is_none());
            assert_eq!(
                attachment.egress_proxy(),
                Some("127.0.0.1:20017".parse().unwrap())
            );
            assert_eq!(attachment.task_dial(), TaskDial::Loopback { netns: None });
        }
        // An egress proxy on a host-mode row means nothing.
        let host = AllocNetwork {
            mode: Some(MODE_HOST.into()),
            egress_proxy: Some("127.0.0.1:20017".into()),
            ..Default::default()
        };
        assert_eq!(
            Attachment::of(&allocation(7, "exec", "{}"), &host, root),
            Attachment::Host
        );
    }

    /// An isolate task's envelope (runtime `envelope`, its gated network
    /// carried over from the isolate task) gets Unix sockets in its own dir,
    /// on any node, fails closed, serves no VIPs and needs no resolver, and
    /// ingress dials the supervisor's socket for the port's label. An
    /// envelope without a network block stays as it is.
    #[test]
    fn an_isolate_envelope_attaches_through_its_socket_dir_test() {
        let root = Path::new("/allocs");
        let envelope = |id| allocation(id, crate::structs::session::ENVELOPE_RUNTIME, "{}");
        let dir = PathBuf::from("/allocs/9/.isolate");
        let attachment = Attachment::of(&envelope(9), &network(MODE_GATED), root);
        assert_eq!(attachment, Attachment::Isolate { dir: dir.clone() });
        assert_eq!(attachment.isolate_dir(), Some(dir.as_path()));
        assert!(attachment.is_gated());
        assert!(!attachment.serves_vips());
        assert!(!attachment.wants_resolver());
        assert!(attachment.fronts_every_port());
        assert!(attachment.netns().is_none() && attachment.guest_vsock().is_none());
        assert_eq!(attachment.egress_proxy(), None);
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert_eq!(
            attachment.listen(ip, 1080),
            ListenAddr::Unix("/allocs/9/.isolate/net.sock_1080".into())
        );
        assert_eq!(attachment.task_dial(), TaskDial::Isolate { dir: dir.clone() });
        assert!(attachment.task_dial().gated());
        assert_eq!(
            isolate_ingress_socket(&dir, "http"),
            PathBuf::from("/allocs/9/.isolate/ingress.sock_http")
        );
        // On a node without namespaces too: no assigned proxy port.
        let confined = AllocNetwork {
            mode: Some(MODE_GATED.into()),
            egress_proxy: Some("127.0.0.1:20017".into()),
            ..Default::default()
        };
        assert_eq!(
            Attachment::of(&envelope(9), &confined, root),
            Attachment::Isolate { dir }
        );
        // An envelope with no network (every session task but an isolate
        // task) and every other runtime are as before.
        assert_eq!(
            Attachment::of(&envelope(9), &AllocNetwork::default(), root),
            Attachment::Host
        );
        assert_eq!(
            Attachment::of(&envelope(9), &network(MODE_HOST), root),
            Attachment::Host
        );
        assert!(Attachment::Host.wants_resolver());
        assert!(Attachment::GuestVsock { scratch: "/s".into() }.wants_resolver());
    }

    #[test]
    fn only_namespaces_and_vsock_guests_fail_closed_test() {
        let scratch = PathBuf::from("/s");
        assert!(!Attachment::Host.is_gated());
        assert!(Attachment::Netns { path: "/n".into() }.is_gated());
        assert!(Attachment::GuestVsock {
            scratch: scratch.clone()
        }
        .is_gated());
        assert!(!Attachment::GuestTap { scratch }.is_gated());
    }

    #[test]
    fn listeners_bind_where_the_attachment_says_test() {
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert_eq!(
            Attachment::Host.listen(ip, 20001),
            ListenAddr::Tcp {
                netns: None,
                addr: "127.0.0.1:20001".parse().unwrap()
            }
        );
        assert_eq!(
            Attachment::Netns {
                path: "/var/run/netns/x".into()
            }
            .listen(ip, 1080),
            ListenAddr::Tcp {
                netns: Some("/var/run/netns/x".into()),
                addr: "127.0.0.1:1080".parse().unwrap()
            }
        );
        assert_eq!(
            Attachment::GuestVsock {
                scratch: "/a/.sandbox".into()
            }
            .listen(ip, 53),
            ListenAddr::Unix("/a/.sandbox/vsock.sock_53".into())
        );
    }

    #[test]
    fn ingress_dials_the_task_the_attachment_way_test() {
        assert_eq!(
            Attachment::Host.task_dial(),
            TaskDial::Loopback { netns: None }
        );
        assert_eq!(
            Attachment::GuestTap {
                scratch: "/s".into()
            }
            .task_dial(),
            TaskDial::Bridge {
                scratch: "/s".into()
            }
        );
        assert!(Attachment::GuestTap {
            scratch: "/s".into()
        }
        .fronts_every_port());
        assert!(!Attachment::Netns { path: "/n".into() }.fronts_every_port());
        // Only a gated guest serves its own VIPs (its agent relays them).
        assert!(!Attachment::GuestVsock {
            scratch: "/s".into()
        }
        .serves_vips());
        assert!(Attachment::GuestTap {
            scratch: "/s".into()
        }
        .serves_vips());
        assert!(Attachment::Host.serves_vips());
    }
}
