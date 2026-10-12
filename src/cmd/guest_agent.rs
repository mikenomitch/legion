//! `legion guest-agent` — the datapath's foothold inside a gated sandbox
//! (docs/network-handlers.md, "Sandboxes"). Started by the generated guest
//! init, never run by an operator. Relays the guest's loopback proxy and
//! resolver ports and its upstreams to the host datapath over vsock, and the
//! datapath's inbound connections to the workload, and then parks.
//!
//! The guest normally runs the static `legion-guest` binary instead; this
//! subcommand is the fallback when the worker has none, so its arguments go
//! straight to the same parser (`legion_guest::cli`).

use crate::prelude::*;
use clap::{Arg, ArgMatches, Command};

pub fn config() -> Command {
    Command::new("guest-agent")
        .about("Sandbox guest agent: relay loopback proxy, DNS, upstream, and ingress ports between the guest and the host over vsock (started by the guest init, not by hand)")
        .hide(true)
        .disable_help_flag(true)
        .arg(
            Arg::new("args")
                .num_args(0..)
                .trailing_var_arg(true)
                .allow_hyphen_values(true)
                .help(
                    "--proxy-port PORT, --dns-port PORT (0 disables), --host-cid CID, \
                     --ingress-port PORT, --upstream PORT[@VSOCK][=VIP:PORT], --resume-watch",
                ),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    passthrough("guest-agent", matches)
}

/// Run a guest subcommand through the guest crate's own parser. It never
/// returns on success, so it gets a blocking thread of its own.
pub(crate) fn passthrough(command: &str, matches: &ArgMatches) -> R<()> {
    let mut args = vec![command.to_string()];
    args.extend(
        matches
            .get_many::<String>("args")
            .into_iter()
            .flatten()
            .cloned(),
    );
    tokio::task::block_in_place(|| legion_guest::cli::run(&args))
}
