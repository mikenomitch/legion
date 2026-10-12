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
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
    start_with(dir, serde_json::json!({}), &repo.join("configs/workers"))
}

/// [`start`] with extra top-level manager config and a worker config dir.
fn start_with(dir: tempfile::TempDir, extra: serde_json::Value, workers: &Path) -> Cluster {
    let port = free_port_pair();
    let worker_port = free_port();
    let mut config = serde_json::json!({
        "server": { "host": "127.0.0.1", "port": port },
        "storePath": dir.path().join("store").to_str().expect("utf-8"),
        "workerConfigPath": workers.to_str().expect("utf-8"),
        "telemetry": { "enabled": false },
        "taskResourceDefaults": { "cpu": 0, "memory": 0, "allocations": 100 },
    });
    for (key, value) in extra.as_object().expect("object") {
        config[key] = value.clone();
    }
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

/// The repo's default worker type with `exec = { direct = true }`.
fn direct_exec_workers(dir: &Path) -> PathBuf {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
    let default = std::fs::read_to_string(repo.join("configs/workers/default.ncl"))
        .expect("default worker config");
    let end = default.rfind('}').expect("record");
    let workers = dir.join("workers");
    std::fs::create_dir_all(&workers).expect("workers dir");
    std::fs::write(
        workers.join("default.ncl"),
        format!("{}    exec = {{ direct = true }},\n}}\n", &default[..end]),
    )
    .expect("write worker config");
    workers
}

fn first_node(cluster: &Cluster) -> Option<String> {
    let nodes = cluster.cli_json(&["node", "list"]);
    nodes["data"]
        .as_array()
        .or_else(|| nodes["data"]["nodes"].as_array())
        .and_then(|nodes| nodes.first())
        .and_then(|node| node["id"].as_str())
        .map(str::to_string)
}

fn running_allocation(cluster: &Cluster, job: &str) -> Option<i64> {
    // `legion ledger -j` prints a bare array of `{node, allocations}`.
    let out = cluster.cli(&["ledger", "-j"]);
    let ledger: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    ledger
        .as_array()?
        .iter()
        .flat_map(|node| node["allocations"].as_array().cloned().unwrap_or_default())
        .filter(|alloc| alloc["jobId"] == job && alloc["status"] == "placed")
        .filter_map(|alloc| alloc["id"].as_i64())
        .max()
}

#[test]
fn direct_exec_runs_on_the_worker_and_ends_when_its_token_is_revoked_test() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workers = direct_exec_workers(dir.path());
    let cluster = start_with(
        dir,
        serde_json::json!({ "workloadIdentity": { "enabled": true } }),
        &workers,
    );
    cluster.wait_for("the worker to register", || first_node(&cluster).is_some());
    cluster.wait_for("the worker plane to listen", || {
        std::fs::read_to_string(cluster.dir.path().join("manager.log"))
            .unwrap_or_default()
            .contains("Direct exec enabled")
    });

    let job = cluster.dir.path().join("direct.json");
    std::fs::write(
        &job,
        serde_json::json!({
            "name": "direct",
            "task": {
                "name": "main",
                "runtime": "exec",
                "lifecycle": "service",
                "config": { "command": ["sh", "-c", "exec sleep 600"] },
                "resources": { "cpus": 0.1, "memory": 32 },
            },
        })
        .to_string(),
    )
    .expect("write job");
    let run = cluster.cli_json(&["job", "run", job.to_str().expect("utf-8")]);
    assert_eq!(run["ok"], true, "{run}");
    cluster.wait_for("the allocation to run", || {
        running_allocation(&cluster, "direct").is_some()
    });
    let allocation = running_allocation(&cluster, "direct").expect("allocation");

    // `legion exec --direct`: one mint, then the session runs against the
    // worker, with the process's own exit status.
    let mut ran = false;
    let mut last = String::new();
    for _ in 0..40 {
        let out = cluster.cli(&[
            "exec",
            "--job",
            "direct",
            "--direct",
            "--",
            "sh",
            "-c",
            "echo direct-ok; exit 4",
        ]);
        if String::from_utf8_lossy(&out.stdout).contains("direct-ok") {
            assert_eq!(out.status.code(), Some(4), "exit status");
            ran = true;
            break;
        }
        last = format!(
            "stdout={} stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        // The allocation can be placed before its process is up.
        std::thread::sleep(Duration::from_millis(250));
    }
    if !ran {
        let log =
            std::fs::read_to_string(cluster.dir.path().join("manager.log")).unwrap_or_default();
        let tail: Vec<&str> = log
            .lines()
            .filter(|line| line.contains("panic") || line.contains("rror") || line.contains("exec"))
            .collect();
        panic!(
            "legion exec --direct never ran: {last}\n{}",
            tail.join("\n")
        );
    }
    let log = std::fs::read_to_string(cluster.dir.path().join("manager.log")).unwrap_or_default();
    assert!(log.contains("exec ticket minted"), "the mint is audited");
    cluster.wait_for("the redemption to be audited on a heartbeat", || {
        std::fs::read_to_string(cluster.dir.path().join("manager.log"))
            .unwrap_or_default()
            .contains("direct exec session ended")
    });

    // A ticket names its command: the mint reply carries everything a
    // client needs, and nothing that lets it run something else.
    // `-j` before `--`: everything after it is the ticket's argv.
    let out = cluster.cli(&[
        "exec-ticket",
        "mint",
        "-j",
        &allocation.to_string(),
        "--",
        "true",
    ]);
    let minted: serde_json::Value = serde_json::from_slice(&out.stdout).expect("mint JSON");
    assert_eq!(minted["ok"], true, "{minted}");
    assert!(minted["data"]["worker"]
        .as_str()
        .is_some_and(|worker| worker.starts_with("https://")));
    assert!(minted["data"]["caPem"]
        .as_str()
        .is_some_and(|pem| pem.contains("BEGIN CERTIFICATE")));

    // Revocation: a token that may exec, a long direct session opened with
    // it, then the token deleted. The session ends within a heartbeat or two.
    let policy = cluster.cli_json(&[
        "acl",
        "policy",
        "apply",
        "direct-exec",
        "--rules",
        r#"[{"resources":["exec"],"actions":["write"],"names":["direct"],"host":true},{"resources":["job"],"actions":["read"]}]"#,
    ]);
    assert_eq!(policy["ok"], true, "{policy}");
    let token = cluster.cli_json(&[
        "acl",
        "token",
        "create",
        "--name",
        "direct-exec-user",
        "--policy",
        "direct-exec",
    ]);
    assert_eq!(token["ok"], true, "{token}");
    let secret = token["data"]["secret"]
        .as_str()
        .or_else(|| token["data"]["secretId"].as_str())
        .expect("token secret")
        .to_string();
    let accessor = token["data"]["accessorId"]
        .as_str()
        .expect("accessor")
        .to_string();

    let session = Command::new(env!("CARGO_BIN_EXE_legion"))
        .args([
            "exec",
            &allocation.to_string(),
            "--direct",
            "--",
            "sh",
            "-c",
            "echo session-open; exec sleep 300",
        ])
        .env("LEGION_ADDR", format!("http://127.0.0.1:{}", cluster.port))
        .env("LEGION_TOKEN", &secret)
        .env("HOME", cluster.dir.path().join("home"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn direct session");
    // An `exec` task is host-level, so the rule needs `host: true`.
    let mut session = session;
    cluster.wait_for("the direct session to open", || {
        if let Ok(Some(status)) = session.try_wait() {
            panic!("the direct session exited early: {status}");
        }
        std::fs::read_to_string(cluster.dir.path().join("manager.log"))
            .unwrap_or_default()
            .contains("Direct exec session opened")
            && std::fs::read_to_string(cluster.dir.path().join("manager.log"))
                .unwrap_or_default()
                .matches("Direct exec session opened")
                .count()
                >= 2
    });
    std::thread::sleep(Duration::from_secs(2));
    let deleted = cluster.cli_json(&["acl", "token", "delete", &accessor]);
    assert_eq!(deleted["ok"], true, "{deleted}");

    let started = Instant::now();
    let status = loop {
        if let Some(status) = session.try_wait().expect("wait") {
            break status;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the revoked direct session is still running"
        );
        std::thread::sleep(Duration::from_millis(200));
    };
    let output = session.wait_with_output().expect("output");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!status.success(), "a revoked session fails");
    assert!(
        stderr.contains("the token that minted this session was deleted"),
        "stdout={} stderr={stderr}",
        String::from_utf8_lossy(&output.stdout)
    );
}
