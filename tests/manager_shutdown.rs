//! Manager process lifecycle: `legion start manager` exits cleanly (status
//! 0) on SIGTERM, after draining the API and stopping persistence, instead
//! of dying mid-write or ignoring the signal.
//!
//! Runs the real binary on an ephemeral port with an isolated store, exec
//! and telemetry listeners off, so it never touches a developer's cluster.

#![cfg(unix)]

use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("bind an ephemeral port")
        .port()
}

#[test]
fn manager_exits_cleanly_on_sigterm_test() {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let config_path = dir.path().join("manager.json");
    let config = serde_json::json!({
        "server": { "host": "127.0.0.1", "port": port },
        "storePath": dir.path().join("store").to_str().expect("utf-8 path"),
        "exec": { "enabled": false },
        "telemetry": { "enabled": false },
    });
    std::fs::write(&config_path, config.to_string()).expect("write config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_legion"))
        .args(["start", "manager", "-c"])
        .arg(&config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn legion manager");

    // Wait for the gRPC listener, so the signal lands on a serving manager.
    let started = Instant::now();
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        if let Some(status) = child.try_wait().expect("poll manager") {
            panic!("manager exited before serving: {status}");
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "manager never started listening on {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // SAFETY: plain kill(2) on our own child's pid.
    let rc = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(rc, 0, "kill(SIGTERM) failed");

    let signalled = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll manager") {
            break status;
        }
        if signalled.elapsed() > Duration::from_secs(40) {
            let _ = child.kill();
            panic!("manager did not exit within 40s of SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(
        status.success(),
        "manager exited with {status} after SIGTERM"
    );
}
