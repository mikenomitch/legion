//! macOS dev loop, end to end (docs/suspend-and-resume.md, "macOS dev
//! loop"): an idle sandbox on Apple `container` suspends to disk after a
//! short `idle.after`, then resumes cold on its retained volume.
//!
//! Runs the real binary: an isolated manager and worker on ephemeral ports,
//! with their own store, state file, and data dir under a tempdir, so it
//! never talks to a developer's long-running cluster on :4747/:4848. The
//! worker measures the sandbox through `container stats` (there are no
//! cgroups on macOS), and the manager's idle sweep suspends it.
//!
//! `#[ignore]`d because it needs macOS with the Apple `container` CLI and a
//! local `alpine:latest` image (or network to pull one). It starts the
//! `container` service when it is down, and stops it again afterwards.
//!
//! ```text
//! cargo test --test apple_container_idle -- --ignored --nocapture
//! ```

#![cfg(target_os = "macos")]

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

const LEGION: &str = env!("CARGO_BIN_EXE_legion");

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("bind an ephemeral port")
        .port()
}

fn container(args: &[&str]) -> Option<std::process::Output> {
    Command::new("container")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .ok()
}

fn container_service_running() -> bool {
    container(&["system", "status"]).is_some_and(|out| {
        out.status.success() && String::from_utf8_lossy(&out.stdout).contains("running")
    })
}

/// Everything the test started, torn down on drop even when an assertion
/// fails: the worker, the manager, any `legion-sbx-*` container the worker
/// left, and the container service if this test started it.
struct Cluster {
    /// Kept when the test fails, so its logs can be read.
    dir: PathBuf,
    addr: String,
    manager: Option<Child>,
    worker: Option<Child>,
    started_service: bool,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in [self.worker.take(), self.manager.take()]
            .into_iter()
            .flatten()
        {
            let mut child = child;
            // SAFETY: plain kill(2) on our own child's pid.
            unsafe {
                libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
            }
            let deadline = Instant::now() + Duration::from_secs(20);
            while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(100));
            }
            let _ = child.kill();
            let _ = child.wait();
        }
        // Only the containers this cluster's worker started (each sandbox
        // records its name in its allocation dir): other local clusters
        // name theirs `legion-sbx-<id>` too.
        for entry in std::fs::read_dir(self.dir.join("allocs"))
            .into_iter()
            .flatten()
            .flatten()
        {
            if let Ok(name) =
                std::fs::read_to_string(entry.path().join(".sandbox").join("apple-container"))
            {
                let _ = container(&["rm", "--force", name.trim()]);
            }
        }
        if self.started_service {
            let _ = container(&["system", "stop"]);
        }
        if std::thread::panicking() {
            eprintln!("cluster logs kept in {}", self.dir.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

impl Cluster {
    fn legion(&self, args: &[&str]) -> (bool, String) {
        let out = Command::new(LEGION)
            .args(args)
            .env("LEGION_ADDR", &self.addr)
            .current_dir(self.dir.as_path())
            .stdin(Stdio::null())
            .output()
            .expect("run legion");
        let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
        text.push_str(&String::from_utf8_lossy(&out.stderr));
        (out.status.success(), text)
    }

    fn legion_json(&self, args: &[&str]) -> Value {
        let mut args = args.to_vec();
        args.push("-j");
        let (_, text) = self.legion(&args);
        serde_json::from_str(text.trim()).unwrap_or(Value::Null)
    }

    fn instance(&self, id: &str) -> Value {
        self.legion_json(&["instance", "get", id])
            .pointer("/data/instance")
            .cloned()
            .unwrap_or(Value::Null)
    }

    /// Poll `what` until it returns `Some`, or fail naming `goal`.
    fn wait_for<T>(&self, goal: &str, timeout: Duration, what: impl Fn() -> Option<T>) -> T {
        let started = Instant::now();
        loop {
            if let Some(value) = what() {
                return value;
            }
            assert!(
                started.elapsed() < timeout,
                "timed out after {timeout:?} waiting for {goal}; logs in {}",
                self.dir.as_path().display()
            );
            std::thread::sleep(Duration::from_secs(1));
        }
    }
}

/// Every `boots` file the instance's volume holds (one line per cold boot).
fn boot_lines(root: &Path) -> usize {
    fn find(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                find(&path, out);
            } else if path.file_name().is_some_and(|name| name == "boots") {
                out.push(path);
            }
        }
    }
    let mut found = Vec::new();
    find(root, &mut found);
    found
        .iter()
        .map(|path| {
            std::fs::read_to_string(path)
                .unwrap_or_default()
                .lines()
                .count()
        })
        .sum()
}

#[test]
#[ignore = "needs macOS with the Apple `container` CLI and an alpine image; starts the container service if it is down"]
fn an_idle_apple_container_sandbox_suspends_and_resumes_cold_on_its_volume() {
    if container(&["--version"]).is_none() {
        eprintln!("skipping: the Apple `container` CLI is not installed");
        return;
    }
    let started_service = if container_service_running() {
        false
    } else {
        let out = container(&[
            "system",
            "start",
            "--enable-kernel-install",
            "--timeout",
            "120",
        ])
        .expect("container system start");
        assert!(
            out.status.success(),
            "container system start failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        true
    };

    let dir = tempfile::Builder::new()
        .prefix("legion-mac-idle-")
        .tempdir()
        .expect("tempdir")
        .keep();
    let manager_port = free_port();
    let worker_port = free_port();
    let root = dir.clone();
    let path = |name: &str| root.join(name).to_string_lossy().into_owned();
    std::fs::create_dir_all(dir.as_path().join("workers")).unwrap();
    std::fs::write(
        dir.as_path().join("manager.json"),
        serde_json::json!({
            "server": { "host": "127.0.0.1", "port": manager_port },
            "storePath": path("store"),
            "workerConfigPath": format!("{}/", path("workers")),
            // Off, so the manager binds only `port` (exec would take
            // port + 1 and telemetry port + 2, which nobody reserved).
            "telemetry": { "enabled": false },
            "exec": { "enabled": false },
            "suspend": { "idleSweep": "5s" },
        })
        .to_string(),
    )
    .unwrap();
    // `resourceEnforcement.enabled` is the measuring switch. On macOS it
    // enforces nothing and measures Apple containers with `container stats`.
    std::fs::write(
        dir.as_path().join("workers").join("devsandbox.ncl"),
        "{ workerType = \"devsandbox\", resourceEnforcement = { enabled = true } }\n",
    )
    .unwrap();

    let log = |name: &str| std::fs::File::create(root.join(name)).unwrap();
    let manager = Command::new(LEGION)
        .args(["start", "manager", "-c"])
        .arg(dir.as_path().join("manager.json"))
        .env("LEGION_DATA_DIR", path("manager-data"))
        .current_dir(dir.as_path())
        .stdout(log("manager.log"))
        .stderr(log("manager.err"))
        .spawn()
        .expect("spawn manager");
    let mut cluster = Cluster {
        addr: format!("http://127.0.0.1:{manager_port}"),
        manager: Some(manager),
        worker: None,
        started_service,
        dir,
    };
    let started = Instant::now();
    while TcpStream::connect(("127.0.0.1", manager_port)).is_err() {
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "manager never listened"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    let worker = Command::new(LEGION)
        .args([
            "join",
            "-a",
            "--type",
            "devsandbox",
            "--name",
            "mac-idle-it",
        ])
        .env("LEGION_ADDR", &cluster.addr)
        .env("LEGION_DATA_DIR", path("worker-data"))
        .env("LEGION_WORKER_PORT", worker_port.to_string())
        .env("LEGION_WORKER_STATE_PATH", path("worker-state.json"))
        .current_dir(cluster.dir.as_path())
        .stdout(log("worker.log"))
        .stderr(log("worker.err"))
        .spawn()
        .expect("spawn worker");
    cluster.worker = Some(worker);
    cluster.wait_for("the worker to register", Duration::from_secs(60), || {
        let (_, text) = cluster.legion(&["node", "list"]);
        text.contains("mac-idle-it").then_some(())
    });

    std::fs::write(
        cluster.dir.as_path().join("idle.ncl"),
        r#"{
  name = "mac-idle",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    stateful = {
      mode = "keyed",
      requireId = true,
      storage = { path = "/workspace" },
    },
    config = {
      image = "alpine:latest",
      command = ["/bin/sh", "-c", "echo boot >> /workspace/boots; exec sleep 100000"],
    },
    resources = { cpu = 100, memory = 256 },
    suspend = {
      checkpoint = "disk",
      idle = { after = "30s", signals = ["network", "cpu", "exec"] },
    },
  }],
}
"#,
    )
    .unwrap();
    let (ok, text) = cluster.legion(&["job", "run", "idle.ncl"]);
    assert!(ok, "job run failed: {text}");
    let (ok, text) = cluster.legion(&["instance", "create", "mac-idle", "--id", "user-1"]);
    assert!(ok, "instance create failed: {text}");

    // Running, booted once on its volume, and measured: the idle window
    // shows up, which means `container stats` reached the manager.
    cluster.wait_for("the first boot", Duration::from_secs(120), || {
        (boot_lines(cluster.dir.as_path()) >= 1).then_some(())
    });
    let measured = cluster.wait_for("an idle window", Duration::from_secs(60), || {
        let instance = cluster.instance("user-1");
        instance
            .get("idleSince")
            .and_then(Value::as_str)
            .map(str::to_string)
    });
    eprintln!("user-1 idle since {measured}");

    // Quiet for `after` (30s), then the 5s sweep suspends it to disk.
    let suspended = cluster.wait_for("an idle suspend", Duration::from_secs(150), || {
        let instance = cluster.instance("user-1");
        (instance.get("state").and_then(Value::as_str) == Some("suspended")).then_some(instance)
    });
    assert_eq!(
        suspended.get("suspendReason").and_then(Value::as_str),
        Some("idle"),
        "{suspended}"
    );
    assert_eq!(
        suspended.get("checkpointKind").and_then(Value::as_str),
        Some("disk"),
        "{suspended}"
    );

    // Resume: a cold boot on the retained volume, so the file written by the
    // first boot is still there and gains a second line.
    let (ok, text) = cluster.legion(&["instance", "start", "user-1", "--wait"]);
    assert!(ok, "instance start failed: {text}");
    cluster.wait_for(
        "the cold boot on the retained volume",
        Duration::from_secs(120),
        || (boot_lines(cluster.dir.as_path()) >= 2).then_some(()),
    );
    let resumed = cluster.instance("user-1");
    assert_eq!(
        resumed.get("state").and_then(Value::as_str),
        Some("active"),
        "{resumed}"
    );

    let _ = cluster.legion(&["job", "stop", "mac-idle"]);
}
