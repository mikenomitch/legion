//! `legion guest-resume` — the resume handler inside a sandbox launched for
//! memory checkpoints (docs/suspend-and-resume.md, "Identity after a
//! resume"). Started by the generated guest init from the worker's own binary
//! copied into the rootfs; never run by an operator. Watches the identity
//! disk and refreshes the guest after every restore. The logic lives in
//! `crate::task_plugins::guest_resume`.

use crate::prelude::*;
use clap::{Arg, ArgMatches, Command};

pub fn config() -> Command {
    Command::new("guest-resume")
        .about("Sandbox resume handler: refresh identity, RNG, clock, and run onResume after a restore (started by the guest init, not by hand)")
        .hide(true)
        .arg(
            Arg::new("identity")
                .long("identity")
                .value_name("DEVICE")
                .help("The identity disk; default the legion.identity= kernel argument"),
        )
        .arg(
            Arg::new("poll-ms")
                .long("poll-ms")
                .default_value("200")
                .help("How often to check the identity disk, in milliseconds"),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    let device = match matches.get_one::<String>("identity") {
        Some(device) => std::path::PathBuf::from(device),
        None => {
            let cmdline = std::fs::read_to_string("/proc/cmdline").unwrap_or_default();
            crate::task_plugins::guest_resume::identity_device_from_cmdline(&cmdline).context(
                "no legion.identity= on the kernel cmdline; this sandbox was not launched for \
                 memory checkpoints",
            )?
        }
    };
    let poll = std::time::Duration::from_millis(
        matches
            .get_one::<String>("poll-ms")
            .and_then(|value| value.parse().ok())
            .unwrap_or(200),
    );
    eprintln!("legion-guest-resume: watching {}", device.display());
    run(device, poll)
}

#[cfg(target_os = "linux")]
fn run(device: std::path::PathBuf, poll: std::time::Duration) -> R<()> {
    crate::task_plugins::guest_resume::watch(
        &device,
        &crate::task_plugins::guest_resume::LinuxGuest,
        poll,
    )
}

#[cfg(not(target_os = "linux"))]
fn run(_device: std::path::PathBuf, _poll: std::time::Duration) -> R<()> {
    bail!("legion guest-resume runs inside a Linux sandbox guest only")
}
