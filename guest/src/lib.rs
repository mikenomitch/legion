//! The in-guest half of Legion's `sandbox` runtime.
//!
//! Everything here runs inside a microVM guest, started by the init script
//! the worker generates (`/legion/init`), or is a format the worker and the
//! guest must agree on:
//!
//! - [`agent`]: the gated datapath's foothold, relaying the guest's loopback
//!   proxy and DNS ports to the node over vsock.
//! - [`resume`]: the resume handler, which refreshes a restored guest's
//!   identity, RNG, clock, and token files, then runs `suspend.onResume`.
//! - [`identity`]: the identity disk the worker writes and the handler reads.
//! - [`env`]: the `/legion/env` and `/legion/resume.env` format.
//! - [`cli`]: the `guest-agent` and `guest-resume` command lines.
//!
//! The worker's `legion` binary links this crate (its `legion guest-agent`
//! and `legion guest-resume` subcommands pass straight through to [`cli`]),
//! and the `legion-guest` binary built from it is the static artifact the
//! worker copies into a guest. See `docs/task-runtime-sandbox.md`, "The
//! guest agent".

pub mod agent;
pub mod cli;
pub mod env;
pub mod identity;
pub mod resume;
