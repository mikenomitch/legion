//! `legion <cmd> | head` must end quietly when the reader goes away.
//!
//! The AWS spot-check re-run saw `legion raft status | head` panic with
//! "failed printing to stdout: Broken pipe". The CLI now restores the default
//! SIGPIPE disposition, so it ends like `cat` does: killed by SIGPIPE (shell
//! status 141), with nothing on stderr.

#![cfg(unix)]

use std::io::Read;
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};

#[test]
fn a_closed_stdout_ends_the_cli_without_a_panic_test() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut child = Command::new(env!("CARGO_BIN_EXE_legion"))
        // `capabilities` prints a long table and needs no manager.
        .arg("capabilities")
        // The CLI writes its log files under the working directory.
        .current_dir(dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn legion");
    // Close the read end before the CLI has started writing.
    drop(child.stdout.take());
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("stderr")
        .read_to_string(&mut stderr)
        .expect("read stderr");
    let status = child.wait().expect("wait for legion");

    assert!(!stderr.contains("panicked"), "the CLI panicked: {stderr}");
    assert_eq!(
        status.signal(),
        Some(libc::SIGPIPE),
        "expected death by SIGPIPE, got {status:?}; stderr: {stderr}"
    );
}
