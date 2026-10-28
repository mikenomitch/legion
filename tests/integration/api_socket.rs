//! The per-allocation API socket (apps plan §6.4 and §9 A2;
//! docs/workload-identity.md, "The API socket").
//!
//! - In process: the worker's proxy forwards calls on the socket to a real
//!   manager service, attaches the allocation's current token when the
//!   caller sends none, keeps a caller's own credential, follows a token
//!   refresh, and is gone when the allocation stops.
//! - End to end, with the real binary: a `--run-worker` manager with
//!   workload identity on and no manager-config binding runs an `exec` task
//!   whose `legion` identity asks for `kv` on `certs/*`. Inside the task the
//!   worker's ambient env points `legion` at the socket: it writes
//!   `certs/*`, is refused anywhere else, `legion login` leaves the identity
//!   token file alone (and refuses to write over it), and, when the kernel
//!   host guard runs, a direct dial of the manager's port is dropped while
//!   the socket works.

#![cfg(unix)]

use anyhow::{anyhow, Context};
use legion::cmd::helpers::unix_socket_channel;
use legion::manager::acl::Authenticator;
use legion::manager::server::ManagerDaemonService;
use legion::storage::store::Store;
use legion::structs::manager_client::ManagerClient;
use legion::structs::manager_server::ManagerServer;
use legion::structs::*;
use legion::worker::api_socket::{serve, ApiSocketContext, PeerCheck};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

type R<T> = anyhow::Result<T>;

/// A manager service with ACLs on, served on a loopback port. Returns its
/// address, the management token, and a second token named `other`.
async fn manager() -> R<(String, String, String)> {
    let dir = tempfile::Builder::new()
        .prefix("legion-api-socket-")
        .tempdir_in(crate::support::scratch_root())?
        .keep();
    let store = Store::start_storage(&ManagerConfigFull {
        store_path: dir.to_str().context("store path")?.to_string(),
        ..Default::default()
    })?;
    let auth = Arc::new(Authenticator::from_config(
        &AclConfig {
            enabled: Some(true),
            ..Default::default()
        }
        .with_defaults(),
    )?);
    let (_, admin) = auth
        .mint_initial_management_token(&store, None)
        .map_err(|s| anyhow!(s.to_string()))?
        .context("a fresh store has no management token")?;
    let (_, other) = auth
        .mint_token(&store, "other", "client", &[], None)
        .map_err(|s| anyhow!(s.to_string()))?;
    let svc = ManagerDaemonService::with_acl(store, auth);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = format!("http://{}", listener.local_addr()?);
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ManagerServer::new(svc))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    Ok((address, admin, other))
}

async fn whoami(
    client: &mut ManagerClient<tonic::transport::Channel>,
    token: Option<&str>,
) -> R<String> {
    let mut request = tonic::Request::new(WhoAmIRequest {});
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse()?);
    }
    Ok(client.who_am_i(request).await?.into_inner().principal)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_socket_proxies_calls_with_the_allocations_token_test() -> R<()> {
    let (address, admin, other) = manager().await?;
    let context = ApiSocketContext {
        channel: tonic::transport::Endpoint::from_shared(address)?.connect_lazy(),
        managers: None,
        peer_check: PeerCheck::FileMode,
    };
    // A deep task directory: the socket path is longer than a socket
    // address holds, which the worker and the client both get around.
    let root = tempfile::Builder::new()
        .prefix("legion-api-socket-task-")
        .tempdir_in(crate::support::scratch_root())?;
    let task_dir = root
        .path()
        .join("workers/legion-system-x8f2k1qz/allocs/1234567/a-task-with-a-long-name");
    std::fs::create_dir_all(&task_dir)?;
    let socket = serve(context, 1234567, &task_dir, Some(admin.clone()))?;
    assert!(
        socket.path().as_os_str().len() > 108,
        "{}",
        socket.path().display()
    );
    assert_eq!(socket.path(), task_dir.join("secrets/legion.sock"));

    let mut client = ManagerClient::new(unix_socket_channel(
        socket.path().to_path_buf(),
        Some(Duration::from_secs(5)),
    ));
    // No credential of its own: the allocation's token is attached.
    assert_eq!(whoami(&mut client, None).await?, "bootstrap");
    // A caller's own credential is kept.
    assert_eq!(whoami(&mut client, Some(&other)).await?, "other");
    // A refreshed token is used from the next call.
    socket.set_token(&other);
    assert_eq!(whoami(&mut client, None).await?, "other");

    // The allocation stops: the socket goes with it.
    let path = socket.path().to_path_buf();
    drop(socket);
    assert!(!path.exists());
    let mut fresh = ManagerClient::new(unix_socket_channel(path, Some(Duration::from_secs(2))));
    assert!(whoami(&mut fresh, None).await.is_err());
    Ok(())
}

/// The proxy counts a manager it cannot reach toward the worker's failover,
/// like a failed heartbeat: the second unreachable call moves the worker to
/// the next manager it knows (`ManagerEndpoints`, whose own tests cover the
/// rotation), so the socket's next calls go there.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_manager_fails_the_socket_over_test() -> R<()> {
    let dead = TcpListener::bind("127.0.0.1:0")?;
    let dead_address = format!("http://{}", dead.local_addr()?);
    drop(dead);
    let managers = legion::worker::managers::ManagerEndpoints::new(
        &dead_address,
        vec!["http://127.0.0.1:9".into()],
    )?;
    let context = ApiSocketContext {
        channel: tonic::transport::Endpoint::from_shared(dead_address.clone())?
            .connect_timeout(Duration::from_secs(2))
            .connect_lazy(),
        managers: Some(managers.clone()),
        peer_check: PeerCheck::FileMode,
    };
    let root = tempfile::Builder::new()
        .prefix("legion-api-socket-failover-")
        .tempdir_in(crate::support::scratch_root())?;
    let socket = serve(context, 7, root.path(), Some("token".into()))?;
    let mut client = ManagerClient::new(unix_socket_channel(
        socket.path().to_path_buf(),
        Some(Duration::from_secs(5)),
    ));
    assert_eq!(managers.active(), dead_address);
    for _ in 0..2 {
        let status = client
            .who_am_i(WhoAmIRequest {})
            .await
            .expect_err("nothing answers");
        assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
        assert!(
            status.message().contains("could not reach a manager"),
            "{status:?}"
        );
    }
    assert_eq!(managers.active(), "http://127.0.0.1:9");
    Ok(())
}

/// A process in another allocation's cgroup leaf, under the same uid, is
/// refused; one in the allocation's own leaf is admitted.
#[test]
#[ignore = "needs root and a delegated cgroup v2 hierarchy at /sys/fs/cgroup, which CI and the \
            dev container lack; run with --ignored on such a host"]
fn another_allocations_process_is_refused_by_the_peer_cgroup_check_test() -> R<()> {
    let parent = format!("legion-api-socket-test-{}", std::process::id());
    let root = std::path::Path::new("/sys/fs/cgroup").join(&parent);
    for leaf in ["share/legion-7", "share/legion-8"] {
        std::fs::create_dir_all(root.join(leaf))?;
    }
    let runtime = tokio::runtime::Runtime::new()?;
    let dir = tempfile::Builder::new()
        .prefix("legion-api-socket-cgroup-")
        .tempdir_in(crate::support::scratch_root())?;
    let result = runtime.block_on(async {
        let (address, admin, _) = manager().await?;
        let context = ApiSocketContext {
            channel: tonic::transport::Endpoint::from_shared(address)?.connect_lazy(),
            managers: None,
            peer_check: PeerCheck::Cgroup {
                parent: parent.clone(),
            },
        };
        let socket = serve(context, 7, dir.path(), Some(admin))?;
        let connect_from = |leaf: &str| -> R<bool> {
            let script = format!(
                "echo $$ > {}/cgroup.procs && exec {} acl whoami -j",
                root.join(leaf).display(),
                env!("CARGO_BIN_EXE_legion")
            );
            let out = Command::new("sh")
                .args(["-c", &script])
                .env("LEGION_ADDR", format!("unix://{}", socket.path().display()))
                .env("LEGION_CONNECT_TIMEOUT", "3")
                .env_remove("LEGION_TOKEN")
                .output()?;
            Ok(out.status.success())
        };
        assert!(connect_from("share/legion-7")?, "its own leaf");
        assert!(
            !connect_from("share/legion-8")?,
            "another allocation's leaf"
        );
        Ok::<_, anyhow::Error>(())
    });
    for leaf in ["share/legion-7", "share/legion-8", "share", ""] {
        let _ = std::fs::remove_dir(root.join(leaf));
    }
    result
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("bind")
        .port()
}

/// A `--run-worker` manager with workload identity on, on fresh ports.
struct Cluster {
    dir: tempfile::TempDir,
    port: u16,
    manager: Child,
}

impl Cluster {
    fn start() -> Cluster {
        for attempt in 1..=3 {
            let cluster = Self::spawn();
            let deadline = Instant::now() + Duration::from_secs(90);
            loop {
                let lost_a_bind = cluster.log().contains("Address already in use");
                if lost_a_bind || Instant::now() > deadline {
                    break;
                }
                if TcpStream::connect(("127.0.0.1", cluster.port)).is_ok() {
                    std::thread::sleep(Duration::from_secs(1));
                    let mut cluster = cluster;
                    if cluster.manager.try_wait().ok().flatten().is_some() {
                        break;
                    }
                    let registered = cluster.wait_for("the worker to register", || {
                        cluster.log().contains("Address already in use")
                            || cluster.cli_json(&["node", "list"])["data"]
                                .to_string()
                                .contains("\"id\"")
                    });
                    if registered && !cluster.log().contains("Address already in use") {
                        return cluster;
                    }
                    break;
                }
                std::thread::sleep(Duration::from_millis(250));
            }
            eprintln!("api_socket: start attempt {attempt} lost a port bind; retrying");
        }
        panic!("the manager could not bind its ports in three attempts");
    }

    fn spawn() -> Cluster {
        let dir = tempfile::Builder::new()
            .prefix("legion-api-socket-e2e-")
            .tempdir_in(crate::support::scratch_root())
            .expect("tempdir");
        let port = free_port();
        let worker_port = free_port();
        let config = serde_json::json!({
            "server": { "host": "127.0.0.1", "port": port },
            "storePath": dir.path().join("store").to_str().expect("utf-8"),
            "telemetry": { "enabled": false },
            "exec": { "enabled": false },
            "systemWorker": { "port": worker_port },
            // Nothing else: no controlPlaneBindings, no policy for the job.
            "workloadIdentity": { "enabled": true },
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
            .env_remove("LEGION_ADDR")
            .stdout(log.try_clone().expect("log"))
            .stderr(log)
            .spawn()
            .expect("spawn manager");
        Cluster { dir, port, manager }
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("manager.log")).unwrap_or_default()
    }

    fn cli(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_legion"))
            .args(args)
            .env("LEGION_ADDR", format!("http://127.0.0.1:{}", self.port))
            .env(
                "LEGION_TOKEN_FILE",
                self.dir.path().join("home/.legion/token"),
            )
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
        serde_json::from_slice(&out.stdout).unwrap_or(serde_json::Value::Null)
    }

    fn wait_for(&self, what: &str, mut ok: impl FnMut() -> bool) -> bool {
        let started = Instant::now();
        while !ok() {
            if started.elapsed() > Duration::from_secs(120) {
                let log = self.log();
                let tail: Vec<&str> = log.lines().rev().take(60).collect();
                panic!(
                    "timed out waiting for {what}; manager log tail:\n{}",
                    tail.into_iter().rev().collect::<Vec<_>>().join("\n")
                );
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        true
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

#[test]
fn a_task_reaches_the_api_through_its_socket_with_no_setup_test() {
    let _gate = crate::support::cluster_gate();
    let cluster = Cluster::start();
    let legion = env!("CARGO_BIN_EXE_legion");
    let script = format!(
        r#"set -u
L={legion}
export LEGION_CONNECT_TIMEOUT=3 LEGION_REQUEST_TIMEOUT=10
$L kv put certs/e2e from-task >/dev/null 2>&1 || exit 11
if $L kv put other/x nope >/dev/null 2>&1; then other=allowed; else other=denied; fi
before=$(cat "$LEGION_IDENTITY_TOKEN_FILE")
if LEGION_TOKEN_FILE="$LEGION_IDENTITY_TOKEN_FILE" $L login --method legion --jwt "$before" >/dev/null 2>&1; then guarded=no; else guarded=yes; fi
$L login --method legion --jwt "$before" >/dev/null 2>&1 && login=ok || login=failed
after=$(cat "$LEGION_IDENTITY_TOKEN_FILE")
[ "$before" = "$after" ] && same=yes || same=no
if LEGION_ADDR=http://127.0.0.1:{port} LEGION_TOKEN="$before" $L kv get certs/e2e >/dev/null 2>&1; then direct=reached; else direct=dropped; fi
$L kv put certs/result "addr=$LEGION_ADDR other=$other login=$login same=$same guarded=$guarded direct=$direct" >/dev/null 2>&1 || exit 12
"#,
        port = cluster.port
    );
    let spec = serde_json::json!({
        "name": "socket-e2e",
        "task": {
            "name": "main",
            "runtime": "exec",
            "lifecycle": "batch",
            "config": { "command": ["sh", "-c", script] },
            "resources": { "cpu": 100, "memory": 64 },
            "identities": [{
                "name": "legion",
                "permissions": [
                    { "resources": ["kv"], "actions": ["read", "write"], "paths": ["certs/*"] },
                ],
            }],
        },
    });
    let path = cluster.dir.path().join("socket-e2e.json");
    std::fs::write(&path, spec.to_string()).expect("write job");
    let run = cluster.cli_json(&["job", "run", path.to_str().expect("utf-8")]);
    assert_eq!(run["ok"], true, "{run}");

    let mut result = String::new();
    cluster.wait_for("the task to report through its socket", || {
        let got = cluster.cli_json(&["kv", "get", "certs/result"]);
        result = got["data"]["value"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        !result.is_empty()
    });
    assert!(result.contains("addr=unix://"), "{result}");
    assert!(result.contains("legion.sock"), "{result}");
    assert!(result.contains("other=denied"), "{result}");
    assert!(result.contains("login=ok"), "{result}");
    assert!(result.contains("same=yes"), "{result}");
    assert!(result.contains("guarded=yes"), "{result}");
    let written = cluster.cli_json(&["kv", "get", "certs/e2e"]);
    assert_eq!(written["data"]["value"], "from-task", "{written}");
    if cluster.log().contains("Host guard active in the kernel") {
        assert!(result.contains("direct=dropped"), "{result}");
    } else {
        eprintln!(
            "api_socket: no kernel host guard on this machine; the direct-port half is \
             unchecked ({result})"
        );
    }
}
