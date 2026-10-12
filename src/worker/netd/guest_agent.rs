//! The sandbox guest agent — the datapath's foothold inside a microVM
//! (docs/network-handlers.md, "Sandboxes").
//!
//! The agent runs inside the guest, so it lives in the `legion-guest` crate
//! the static guest binary is built from. This module re-exports it for the
//! worker, which binds the host ends of the same vsock ports
//! ([`host_socket_path`]) and tests its listeners with [`UnixDial`].

pub use legion_guest::agent::*;
