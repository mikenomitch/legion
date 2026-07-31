//! The Unix signal vocabulary Legion accepts, in one place.
//!
//! Three surfaces share it, and they must agree or a job that validates at
//! submit fails at teardown:
//!
//! - **admission** — `stopSignal` and an identity's `changeSignal` are checked
//!   against [`parse_signal`] when a job config is validated;
//! - **the worker** — [`send_signal`] delivers to a tracked pid (the default
//!   [`crate::task_plugins::TaskPlugin::signal`] path);
//! - **container runtimes** — the *name* is passed through to
//!   `container kill --signal` / `<runtime> kill`, so
//!   [`canonical_signal_name`] normalizes the spelling first.
//!
//! Deliberately small: these are the signals a workload realistically asks to
//! be told things with. Adding one is a one-line change here and nowhere else.

use crate::prelude::*;

/// The default signal Legion delivers when it stops a task — docker's, and
/// now Legion's, muscle memory.
pub const DEFAULT_STOP_SIGNAL: &str = "SIGTERM";

/// Every accepted signal name, canonical spelling, for error messages and
/// documentation. `changeSignal` exposes the same set as bare short names
/// (see [`crate::structs::workload_identity::SUPPORTED_CHANGE_SIGNALS`]),
/// which is that field's own long-standing convention.
pub const SUPPORTED_SIGNALS: [&str; 8] = [
    "SIGHUP", "SIGINT", "SIGQUIT", "SIGTERM", "SIGUSR1", "SIGUSR2", "SIGCONT", "SIGWINCH",
];

/// Normalize a signal name to its canonical `SIG…` spelling, rejecting
/// anything outside [`SUPPORTED_SIGNALS`]. Accepts `hup`, `HUP`, `SIGHUP`.
///
/// This is what gets handed to a container runtime CLI, which does its own
/// name → number resolution.
pub fn canonical_signal_name(name: &str) -> R<String> {
    let normalized = name.trim().to_ascii_uppercase();
    let stripped = normalized.strip_prefix("SIG").unwrap_or(&normalized);
    let canonical = format!("SIG{stripped}");
    if SUPPORTED_SIGNALS.contains(&canonical.as_str()) {
        Ok(canonical)
    } else {
        bail!(
            "unsupported signal {name:?}; supported signals are {}",
            SUPPORTED_SIGNALS.join(", ")
        )
    }
}

/// Resolve a signal name to its number. Validation and delivery share this,
/// so a name accepted at submit is a name the worker can deliver.
#[cfg(unix)]
pub fn parse_signal(name: &str) -> R<libc::c_int> {
    Ok(match canonical_signal_name(name)?.as_str() {
        "SIGHUP" => libc::SIGHUP,
        "SIGINT" => libc::SIGINT,
        "SIGQUIT" => libc::SIGQUIT,
        "SIGTERM" => libc::SIGTERM,
        "SIGUSR1" => libc::SIGUSR1,
        "SIGUSR2" => libc::SIGUSR2,
        "SIGCONT" => libc::SIGCONT,
        "SIGWINCH" => libc::SIGWINCH,
        // canonical_signal_name already rejected everything else.
        other => bail!("unsupported signal {other:?}"),
    })
}

/// Validate a signal name on a platform that cannot deliver one. Admission
/// runs on the manager, which may not be a Unix host, so the *vocabulary*
/// check must work everywhere even where `kill(2)` does not exist.
#[cfg(not(unix))]
pub fn parse_signal(name: &str) -> R<i32> {
    canonical_signal_name(name)?;
    Ok(0)
}

/// Deliver `signal` to `pid`.
#[cfg(unix)]
pub fn send_signal(pid: u32, signal: &str) -> R<()> {
    let signum = parse_signal(signal)?;
    // SAFETY: plain kill(2) with a validated signal number.
    let rc = unsafe { libc::kill(pid as libc::pid_t, signum) };
    if rc != 0 {
        bail!(
            "kill({pid}, {signal}) failed: {}",
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
pub fn send_signal(_pid: u32, signal: &str) -> R<()> {
    bail!("signal delivery ({signal}) is not supported on this platform");
}

/// Whether `pid` is still alive, used by the graceful-stop ladder to stop
/// waiting the moment the process is gone.
#[cfg(unix)]
pub fn pid_alive(pid: u32) -> bool {
    // SAFETY: signal 0 performs the permission/existence check and delivers
    // nothing.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[cfg(not(unix))]
pub fn pid_alive(_pid: u32) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_names_normalize_in_any_spelling() -> R<()> {
        assert_eq!(canonical_signal_name("hup")?, "SIGHUP");
        assert_eq!(canonical_signal_name("SIGHUP")?, "SIGHUP");
        assert_eq!(canonical_signal_name(" term ")?, "SIGTERM");
        Ok(())
    }

    /// The error names the offending signal *and* the vocabulary, so a
    /// jobspec author can fix it without opening the docs.
    #[test]
    fn unsupported_signal_names_are_rejected_with_the_vocabulary() {
        let err = canonical_signal_name("SIGKILL").expect_err("SIGKILL is not declarable");
        let message = err.to_string();
        assert!(message.contains("SIGKILL"), "{message}");
        assert!(message.contains("SIGTERM"), "{message}");

        assert!(parse_signal("nope").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn parse_signal_agrees_with_libc() -> R<()> {
        assert_eq!(parse_signal("SIGTERM")?, libc::SIGTERM);
        assert_eq!(parse_signal("hup")?, libc::SIGHUP);
        Ok(())
    }
}
