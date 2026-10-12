//! The authenticated worker plane, end to end (worker-plane-auth plan,
//! Phase 1): the real binary as a manager with an in-process worker, on
//! isolated ports, with ACLs on by default.
//!
//! - The worker serves its plane over mTLS and the manager reaches it through
//!   the worker-plane client factory: `legion job logs` (GetTaskLogs) and
//!   `legion exec` (ExecTask) work, and exec returns the process's own exit
//!   status.
//! - `legion net ca rotate` breaks nothing: exec keeps working right after
//!   the rotation (the old CA is still trusted, the new one cross-signed),
//!   and again after the worker re-issues under the new CA.
//! - The node advertises `https://` and the worker plane version.
//!
//! Unix only (the exec runtime and signals); it never touches a developer's
//! cluster or `~/.legion`.

#![cfg(unix)]

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

/// Two adjacent free ports: the manager's gRPC port and its exec WebSocket
/// (`+1`).
fn free_port_pair() -> u16 {
    for _ in 0..50 {
        let first = TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("bind")
            .port();
        if first < u16::MAX && TcpListener::bind(("127.0.0.1", first + 1)).is_ok() {
            return first;
        }
    }
    panic!("no adjacent free ports");
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("bind")
        .port()
}

struct Cluster {
    dir: tempfile::TempDir,
    port: u16,
    manager: Child,
}

impl Cluster {
    fn token_file(&self) -> PathBuf {
        self.dir.path().join("home").join(".legion").join("token")
    }

    fn cli(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_legion"))
            .args(args)
            .env("LEGION_ADDR", format!("http://127.0.0.1:{}", self.port))
            .env("LEGION_TOKEN_FILE", self.token_file())
            .env("HOME", self.dir.path().join("home"))
            .env_remove("LEGION_TOKEN")
            .stdin(Stdio::null())
            .output()
            .expect("run legion")
    }

    fn cli_json(&self, args: &[&str]) -> serde_json::Value {
        let mut with_json = args.to_vec();
        with_json.push("-j");
        let out = self.cli(&with_json);
        let text = String::from_utf8_lossy(&out.stdout);
        serde_json::from_str(text.trim()).unwrap_or_else(|err| {
            panic!(
                "legion {args:?} printed no JSON ({err}): {text} / {}",
                String::from_utf8_lossy(&out.stderr)
            )
        })
    }

    fn wait_for(&self, what: &str, mut ok: impl FnMut() -> bool) {
        let started = Instant::now();
        while !ok() {
            if started.elapsed() > Duration::from_secs(90) {
                let log = std::fs::read_to_string(self.dir.path().join("manager.log"))
                    .unwrap_or_default();
                let tail: Vec<&str> = log.lines().rev().take(40).collect();
                panic!(
                    "timed out waiting for {what}; manager log tail:\n{}",
                    tail.into_iter().rev().collect::<Vec<_>>().join("\n")
                );
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // SAFETY: plain kill(2) on our own child's pid.
        unsafe { libc::kill(self.manager.id() as libc::pid_t, libc::SIGTERM) };
        let started = Instant::now();
        while self.manager.try_wait().ok().flatten().is_none() {
            if started.elapsed() > Duration::from_secs(30) {
                let _ = self.manager.kill();
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

fn start(dir: tempfile::TempDir) -> Cluster {
    let port = free_port_pair();
    let worker_port = free_port();
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
    let config = serde_json::json!({
        "server": { "host": "127.0.0.1", "port": port },
        "storePath": dir.path().join("store").to_str().expect("utf-8"),
        "workerConfigPath": repo.join("configs/workers").to_str().expect("utf-8"),
        "telemetry": { "enabled": false },
        "taskResourceDefaults": { "cpu": 0, "memory": 0, "allocations": 100 },
    });
    let config_path = dir.path().join("manager.json");
    std::fs::write(&config_path, config.to_string()).expect("write config");
    std::fs::create_dir_all(dir.path().join("home")).expect("home");
    let log = std::fs::File::create(dir.path().join("manager.log")).expect("log");
    let manager = Command::new(env!("CARGO_BIN_EXE_legion"))
        .args(["start", "manager", "--run-worker", "-c"])
        .arg(&config_path)
        .current_dir(dir.path())
        .env("LEGION_DATA_DIR", dir.path().join("data"))
        .env("LEGION_TOKEN_FILE", dir.path().join("home/.legion/token"))
        .env("HOME", dir.path().join("home"))
        .env("LEGION_WORKER_PORT", worker_port.to_string())
        .env("LEGION_WORKER_HOST", "127.0.0.1")
        .env("RUST_LOG", "info")
        .env_remove("LEGION_TOKEN")
        .stdout(log.try_clone().expect("log"))
        .stderr(log)
        .spawn()
        .expect("spawn manager");
    let cluster = Cluster { dir, port, manager };
    cluster.wait_for("the manager to listen", || {
        TcpStream::connect(("127.0.0.1", port)).is_ok()
    });
    cluster
}

#[test]
fn logs_exec_and_ca_rotation_work_over_the_mtls_worker_plane_test() {
    let cluster = start(tempfile::tempdir().expect("tempdir"));

    // The in-process worker registers with an https:// address and the
    // worker plane protocol, and its listener comes up with its certificate.
    cluster.wait_for("the worker to register", || {
        let nodes = cluster.cli_json(&["node", "list"]);
        nodes["ok"] == true
            && nodes["data"]
                .as_array()
                .or_else(|| nodes["data"]["nodes"].as_array())
                .is_some_and(|nodes| !nodes.is_empty())
    });
    cluster.wait_for("the worker plane to listen", || {
        std::fs::read_to_string(cluster.dir.path().join("manager.log"))
            .unwrap_or_default()
            .contains("Worker plane listening on 127.0.0.1")
    });
    let log = std::fs::read_to_string(cluster.dir.path().join("manager.log")).unwrap_or_default();
    let plane_line = log
        .lines()
        .find(|line| line.contains("Worker plane listening"))
        .unwrap_or_default();
    assert!(
        plane_line.contains("(mTLS)"),
        "the worker plane is mTLS: {plane_line}"
    );
    // The in-process worker reached the manager over TLS: it registered and
    // was issued its certificate through that channel.
    assert!(log.contains("Node certificate issued"), "{log}");

    // The manager probed the worker plane back and recorded it on the node
    // (`legion node info`, worker-plane-auth plan §6.7).
    cluster.wait_for("the worker plane probe to land on the node", || {
        let nodes = cluster.cli_json(&["node", "list"]);
        let Some(id) = nodes["data"]
            .as_array()
            .or_else(|| nodes["data"]["nodes"].as_array())
            .and_then(|nodes| nodes.first())
            .and_then(|node| node["id"].as_str())
            .map(str::to_string)
        else {
            return false;
        };
        let info = cluster.cli_json(&["node", "info", &id]);
        info["data"]["dynamicAttrs"]["workerPlane"]["reachable"] == true
    });

    let job = cluster.dir.path().join("e2e.json");
    std::fs::write(
        &job,
        serde_json::json!({
            "name": "e2e",
            "task": {
                "name": "main",
                "runtime": "exec",
                "lifecycle": "service",
                "config": { "command": ["sh", "-c", "echo hello-from-task; exec sleep 600"] },
                "resources": { "cpus": 0.1, "memory": 32 },
            },
        })
        .to_string(),
    )
    .expect("write job");
    let run = cluster.cli_json(&["job", "run", job.to_str().expect("utf-8")]);
    assert_eq!(run["ok"], true, "{run}");

    // GetTaskLogs through the worker plane.
    cluster.wait_for("task logs over the worker plane", || {
        let logs = cluster.cli(&["job", "logs", "e2e"]);
        String::from_utf8_lossy(&logs.stdout).contains("hello-from-task")
    });

    // ExecTask through the worker plane, with the process's own status.
    let exec = |label: &str| {
        let out = cluster.cli(&[
            "exec",
            "--job",
            "e2e",
            "--",
            "sh",
            "-c",
            "echo exec-ok; exit 3",
        ]);
        assert!(
            String::from_utf8_lossy(&out.stdout).contains("exec-ok"),
            "{label}: stdout={} stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(3), "{label}: exit status");
    };
    exec("before rotation");

    // Rotate the CA: nothing breaks during the overlap.
    let rotated = cluster.cli_json(&["net", "ca", "rotate"]);
    assert_eq!(rotated["ok"], true, "{rotated}");
    let show = cluster.cli_json(&["net", "ca", "show"]);
    assert_eq!(show["data"]["phase"], "overlap", "{show}");
    assert_eq!(show["data"]["pin"], rotated["data"]["pin"], "{show}");
    exec("right after rotation");

    // The worker re-issues under the new CA within a heartbeat or two.
    cluster.wait_for("the worker to re-issue under the new CA", || {
        std::fs::read_to_string(cluster.dir.path().join("manager.log"))
            .unwrap_or_default()
            .matches("Node certificate issued")
            .count()
            >= 2
    });
    exec("after the worker re-issued");
}
