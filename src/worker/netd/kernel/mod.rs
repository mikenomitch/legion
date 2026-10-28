//! The kernel classifier (mesh interfaces plan §6.A; docs:
//! network-handlers.md, "The kernel classifier").
//!
//! One fixed set of Legion-owned eBPF programs (`bpf/legion_net.bpf.c`,
//! checked in as an object) is attached to the worker's cgroup parent, so
//! every allocation leaf inherits it. The policy is data in maps that the
//! worker fills from each heartbeat:
//!
//! - **the host guard** (A1): the same classes, `EPERM`, and `net.denied`
//!   events as the `nftables` table it replaces on capable nodes;
//! - **the egress chain** (A2): compiled to a rule array ([`compile`]) and
//!   walked at `connect()`, with host rules satisfied by the addresses the
//!   allocation's resolver returned for allowed names. Allowed flows that
//!   nothing reads connect directly;
//! - **capture** (A3): what only netd can decide is redirected to a capture
//!   listener, which recovers the original destination and reads the name
//!   off the flow (SNI or `Host`).
//!
//! [`probe`] decides once at start whether the node can run it (the
//! `netKernel` node attribute). A node that cannot behaves exactly as
//! before, `nftables` guard included.

pub mod abi;
pub mod compile;
pub mod probe;
#[cfg(target_os = "linux")]
pub mod runtime;

/// The node attribute: `active (ebpf: …)` or `unavailable (<why>)`.
pub const NODE_ATTR: &str = "netKernel";

/// The compiled programs, built by `bpf/build.sh`.
pub static OBJECT: &[u8] = include_bytes!("../../../../bpf/legion_net.bpf.o");
