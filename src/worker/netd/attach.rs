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
}

impl Attachment {
    /// The attachment of one desired allocation, from its network payload,
    /// its runtime, and (for a sandbox) its runtime config. `alloc_root` is
    /// the worker's allocation directory root, which holds each sandbox's
    /// scratch dir.
    pub fn of(allocation: &TaskAllocation, network: &AllocNetwork, alloc_root: &Path) -> Self {
        let sandbox = allocation.task_runtime == "sandbox";
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
            Attachment::Netns { .. } | Attachment::GuestVsock { .. }
        )
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

    /// Whether the allocation's upstream VIPs are bound on the node. A gated
    /// sandbox guest serves its VIPs itself (its agent relays them), so the
    /// node binds none for it.
    pub fn serves_vips(&self) -> bool {
        !matches!(self, Attachment::GuestVsock { .. })
    }

    /// Whether every port the allocation advertises is fronted by an ingress
    /// listener, chain or not: a sandbox guest has its own port space, which
    /// nothing else on the node reaches.
    pub fn fronts_every_port(&self) -> bool {
        matches!(
            self,
            Attachment::GuestVsock { .. } | Attachment::GuestTap { .. }
        )
    }

    /// Where a listener for the task-facing address `ip:port` binds: that
    /// address on the node's loopback or inside the namespace, or — for a
    /// guest reached over vsock, which has no address on the node — the
    /// VMM's host socket for `port`, whatever `ip` is.
    pub fn listen(&self, ip: IpAddr, port: u16) -> ListenAddr {
        match self {
            Attachment::GuestVsock { scratch } => ListenAddr::Unix(guest_socket(scratch, port)),
            Attachment::Netns { path } => ListenAddr::Tcp {
                netns: Some(path.clone()),
                addr: SocketAddr::new(ip, port),
            },
            Attachment::Host | Attachment::GuestTap { .. } => ListenAddr::Tcp {
                netns: None,
                addr: SocketAddr::new(ip, port),
            },
        }
    }

    /// How an ingress listener in the host namespace reaches the task.
    pub fn task_dial(&self) -> TaskDial {
        match self {
            Attachment::Host => TaskDial::Loopback { netns: None },
            Attachment::Netns { path } => TaskDial::Loopback {
                netns: Some(path.clone()),
            },
            Attachment::GuestVsock { scratch } => TaskDial::Vsock {
                scratch: scratch.clone(),
            },
            Attachment::GuestTap { scratch } => TaskDial::Bridge {
                scratch: scratch.clone(),
            },
        }
    }
}

/// A sandbox's scratch dir (`<allocs>/<id>/.sandbox`).
fn sandbox_scratch(alloc_root: &Path, allocation_id: i64) -> PathBuf {
    crate::task_plugins::sandbox_dir_for(&alloc_root.join(allocation_id.to_string()))
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
