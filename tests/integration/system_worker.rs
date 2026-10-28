//! The system worker (apps plan §6.2, phase A1;
//! docs/architecture-managers-workers.md, "The system worker"): the real
//! binary as a single manager on isolated ports, with ACLs on by default.
//!
//! - With nothing configured, the manager runs a system worker: one node in
//!   pool `legion-system`, accepting only `legion-system`, marked
//!   `legion.role = "manager"`, offering the 1 CPU / 1 GiB lend as its
//!   capacity. A `legion-system` job places there with no worker joined; a
//!   `default` job that permits the pool does not, and `job plan` names the
//!   namespace restriction; a root exec task in `legion-system` is refused.
//! - `--run-worker` keeps its meaning: the manager's worker runs every
//!   namespace's jobs in the `default` pool, and is still marked as the
//!   manager's, so a job placed the way bundled apps will be (permit both
//!   pools, constraint `legion.role = "manager"`) lands there too.
//!
//! Unix only (signals); it never touches a developer's cluster or
//! `~/.legion`.

#![cfg(unix)]

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

/// One cluster of this module at a time: each is a whole manager plus a
/// worker, and two at once on a loaded machine only add port races.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
    fn start(run_worker: bool) -> Cluster {
        let dir = tempfile::Builder::new()
            .prefix("legion-system-worker-")
            .tempdir_in(crate::support::scratch_root())
            .expect("tempdir");
        let port = free_port();
        let worker_port = free_port();
        let config = serde_json::json!({
            "server": { "host": "127.0.0.1", "port": port },
            "storePath": dir.path().join("store").to_str().expect("utf-8"),
            "telemetry": { "enabled": false },
            "exec": { "enabled": false },
            // Under --run-worker the block is unused: the flag's worker binds
            // LEGION_WORKER_PORT, as it always has.
            "systemWorker": { "port": worker_port },
        });
        let config_path = dir.path().join("manager.json");
        std::fs::write(&config_path, config.to_string()).expect("write config");
        std::fs::create_dir_all(dir.path().join("home")).expect("home");
        let log = std::fs::File::create(dir.path().join("manager.log")).expect("log");
        let mut command = Command::new(env!("CARGO_BIN_EXE_legion"));
        command.args(["start", "manager"]);
        if run_worker {
            command.arg("--run-worker");
        }
        let manager = command
            .arg("-c")
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
            .env_remove("LEGION_NAMESPACE")
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

    fn write_job(&self, name: &str, spec: serde_json::Value) -> String {
        let path = self.dir.path().join(format!("{name}.json"));
        std::fs::write(&path, spec.to_string()).expect("write job");
        path.to_str().expect("utf-8").to_string()
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

    /// The single registered node, once it is there: `node info`'s data.
    fn the_node(&self) -> serde_json::Value {
        let mut id = String::new();
        self.wait_for("the in-process worker to register", || {
            let nodes = self.cli_json(&["node", "list"]);
            let Some(first) = nodes["data"]
                .as_array()
                .or_else(|| nodes["data"]["nodes"].as_array())
                .and_then(|nodes| nodes.first())
            else {
                return false;
            };
            id = first["id"].as_str().unwrap_or_default().to_string();
            !id.is_empty()
        });
        let info = self.cli_json(&["node", "info", &id]);
        assert_eq!(info["ok"], true, "{info}");
        info["data"].clone()
    }

    /// `node usage`'s data for `node`: its capacity and allocations.
    fn usage(&self, node: &str) -> serde_json::Value {
        let usage = self.cli_json(&["node", "usage", node]);
        assert_eq!(usage["ok"], true, "{usage}");
        usage["data"].clone()
    }

    /// Wait for `job` (canonical id) to have an allocation on `node`. The
    /// placement is what is under test, not the process: an exec task that
    /// runs as `nobody` needs a worker that can switch users, which an
    /// unprivileged CI runner is not.
    fn wait_placed(&self, job: &str, node: &str) {
        self.wait_for(&format!("{job} to place on {node}"), || {
            self.usage(node)["allocations"]
                .as_array()
                .is_some_and(|allocations| {
                    allocations
                        .iter()
                        .any(|allocation| allocation["jobId"] == job)
                })
        });
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

/// A one-task exec service job, as `nobody` (tasks in `legion-system` may
/// not run as root), with `extra` merged into the task, or the job for
/// `namespace`.
fn job(name: &str, extra: serde_json::Value) -> serde_json::Value {
    let mut spec = serde_json::json!({
        "name": name,
        "task": {
            "name": "main",
            "runtime": "exec",
            "lifecycle": "service",
            "config": { "command": ["sleep", "600"], "user": "nobody" },
            "resources": { "cpu": 100, "memory": 64 },
        },
    });
    for (key, value) in extra.as_object().expect("object") {
        if key == "namespace" {
            spec[key] = value.clone();
        } else {
            spec["task"][key] = value.clone();
        }
    }
    spec
}

#[test]
fn a_single_manager_runs_legion_system_jobs_on_its_system_worker_test() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let cluster = Cluster::start(false);
    let node = cluster.the_node();
    assert_eq!(node["pool"], "legion-system", "{node}");
    let attrs = &node["configAttrs"];
    assert_eq!(
        attrs["namespaces"],
        serde_json::json!(["legion-system"]),
        "{node}"
    );
    assert_eq!(attrs["legion.role"], "manager", "{node}");
    assert_eq!(attrs["capacity"]["cpu"], 1000, "{node}");
    assert_eq!(attrs["capacity"]["memory"], 1024, "{node}");
    assert!(
        node["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("legion-system-")),
        "{node}"
    );
    let namespaces = cluster.cli_json(&["namespace", "list"]);
    assert!(
        namespaces.to_string().contains("\"legion-system\""),
        "{namespaces}"
    );
    // The scheduler sees the lend, not the host.
    let id = node["id"].as_str().expect("node id").to_string();
    let usage = cluster.usage(&id);
    assert_eq!(usage["cpu"]["capacity"], 1000.0, "{usage}");
    assert_eq!(usage["memory"]["capacity"], 1024.0, "{usage}");

    // No worker joined, and a legion-system job that names no pool places
    // on the manager's system worker.
    let agent = cluster.write_job(
        "agent",
        job("agent", serde_json::json!({ "namespace": "legion-system" })),
    );
    let run = cluster.cli_json(&["job", "run", &agent]);
    assert_eq!(run["ok"], true, "{run}");
    cluster.wait_placed("legion-system/agent", &id);

    // A default job that permits the pool still does not place, and the
    // plan says the namespace is why.
    let web = cluster.write_job(
        "web",
        job(
            "web",
            serde_json::json!({ "permit": { "pool": "legion-system" } }),
        ),
    );
    let plan = cluster.cli(&["job", "plan", &web]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&plan.stdout),
        String::from_utf8_lossy(&plan.stderr)
    );
    assert!(
        said.contains("1 node accepts only namespaces [legion-system]"),
        "{said}"
    );

    // Tasks there run unprivileged: a root exec task is refused at submit.
    let root = cluster.write_job(
        "root",
        serde_json::json!({
            "name": "root",
            "namespace": "legion-system",
            "task": { "name": "main", "runtime": "exec", "config": { "command": ["true"] } },
        }),
    );
    let refused = cluster.cli(&["job", "run", &root]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&refused.stdout),
        String::from_utf8_lossy(&refused.stderr)
    );
    assert!(!refused.status.success(), "{said}");
    assert!(said.contains("runs as root"), "{said}");
}

#[test]
fn run_worker_keeps_running_every_namespace_on_the_manager_test() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let cluster = Cluster::start(true);
    let node = cluster.the_node();
    assert_eq!(node["pool"], "default", "{node}");
    let attrs = &node["configAttrs"];
    assert_eq!(
        attrs["namespaces"],
        serde_json::json!(["*", "legion-*"]),
        "{node}"
    );
    assert_eq!(attrs["legion.role"], "manager", "{node}");
    assert!(attrs.get("capacity").is_none(), "no lend cap: {node}");

    let id = node["id"].as_str().expect("node id").to_string();
    let mut web = job("web", serde_json::json!({}));
    web["task"]["config"]
        .as_object_mut()
        .expect("config")
        .remove("user");
    let web = cluster.write_job("web", web);
    let run = cluster.cli_json(&["job", "run", &web]);
    assert_eq!(run["ok"], true, "{run}");
    cluster.wait_placed("web", &id);

    // Placed the way bundled apps will be: either pool, on a manager.
    let agent = cluster.write_job(
        "agent",
        job(
            "agent",
            serde_json::json!({
                "namespace": "legion-system",
                "permit": [{
                    "key": "pool", "operator": "matches",
                    "value": "^(legion-system|default)$",
                }],
                "constraints": { "legion.role": "manager" },
            }),
        ),
    );
    let run = cluster.cli_json(&["job", "run", &agent]);
    assert_eq!(run["ok"], true, "{run}");
    cluster.wait_placed("legion-system/agent", &id);
}
