//! `legion guest-resume` — the resume handler inside a sandbox launched for
//! memory checkpoints (docs/suspend-and-resume.md, "Identity after a
//! resume"). Started by the generated guest init, never run by an operator.
//! Watches the identity disk and refreshes the guest after every restore.
//!
//! Like `legion guest-agent`, the fallback for a worker without the static
//! `legion-guest` binary: its arguments go straight to `legion_guest::cli`.

use crate::prelude::*;
use clap::{Arg, ArgMatches, Command};

pub fn config() -> Command {
    Command::new("guest-resume")
        .about("Sandbox resume handler: refresh identity, RNG, clock, and run onResume after a restore (started by the guest init, not by hand)")
        .hide(true)
        .disable_help_flag(true)
        .arg(
            Arg::new("args")
                .num_args(0..)
                .trailing_var_arg(true)
                .allow_hyphen_values(true)
                .help("--identity DEVICE (default: legion.identity= on the kernel cmdline), --poll-ms MS"),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    crate::cmd::guest_agent::passthrough("guest-resume", matches)
}
