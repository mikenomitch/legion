//! The real `legion-isolate` binary: started with socketpairs as a worker
//! would, confined, driven over gRPC, with a fake `IsolateWorker` served on
//! the worker socket.

mod common;

use std::io::Write;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::net::UnixStream as StdUnix;
use std::os::unix::process::CommandExt;
#[cfg(feature = "confinement-test")]
use std::os::unix::process::ExitStatusExt;
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use legion_isolate::service::{channel_over, inherited};
use legion_isolate::worker_api::WorkerApi;
use legion_proto::isolate_host::isolate_host_client::IsolateHostClient;
use legion_proto::isolate_host::isolate_worker_server::{IsolateWorker, IsolateWorkerServer};
use legion_proto::isolate_host::*;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

const BIN: &str = env!("CARGO_BIN_EXE_legion-isolate");

/// `FakeWorker` behind the real `IsolateWorker` service.
struct Grpc(Arc<FakeWorker>);

type ChunkStream = Pin<Box<dyn Stream<Item = Result<CheckpointChunk, Status>> + Send>>;

#[tonic::async_trait]
impl IsolateWorker for Grpc {
    async fn egress(&self, r: Request<EgressRequest>) -> Result<Response<EgressReply>, Status> {
        self.0.egress(r.into_inner()).await.map(Response::new)
    }
    async fn vm(&self, r: Request<VmRequest>) -> Result<Response<VmReply>, Status> {
        self.0.vm(r.into_inner()).await.map(Response::new)
    }
    async fn save_checkpoint(
        &self,
        r: Request<Streaming<CheckpointChunk>>,
    ) -> Result<Response<SaveCheckpointReply>, Status> {
        let mut s = r.into_inner();
        let mut header = None;
        let mut body = Vec::new();
        while let Some(c) = s.next().await {
            match c?.part {
                Some(checkpoint_chunk::Part::Header(h)) => header = Some(h),
                Some(checkpoint_chunk::Part::Data(d)) => body.extend(d),
                None => {}
            }
        }
        let header = header.ok_or_else(|| Status::invalid_argument("no header"))?;
        self.0
            .save_checkpoint(header, body)
            .await
            .map(Response::new)
    }
    type LoadCheckpointStream = ChunkStream;
    async fn load_checkpoint(
        &self,
        r: Request<LoadCheckpointRequest>,
    ) -> Result<Response<ChunkStream>, Status> {
        let origin = r.into_inner().origin.unwrap_or_default();
        match self.0.load_checkpoint(origin).await? {
            None => Err(Status::not_found("no checkpoint")),
            Some((h, b)) => {
                let chunks = vec![
                    Ok(CheckpointChunk {
                        part: Some(checkpoint_chunk::Part::Header(h)),
                    }),
                    Ok(CheckpointChunk {
                        part: Some(checkpoint_chunk::Part::Data(b)),
                    }),
                ];
                Ok(Response::new(Box::pin(tokio_stream::iter(chunks))))
            }
        }
    }
    async fn log(&self, r: Request<Streaming<LogBatch>>) -> Result<Response<LogReply>, Status> {
        let mut s = r.into_inner();
        while let Some(b) = s.next().await {
            self.0.log(b?).await?;
        }
        Ok(Response::new(LogReply {}))
    }
}

struct Spawned {
    child: Child,
    client: IsolateHostClient<Channel>,
    worker: Arc<FakeWorker>,
}

impl Drop for Spawned {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start the binary as a worker would: control socket on fd 3, worker
/// socket on fd 4, the configuration on fd 5. Drop the returned
/// `ChildEnds` once the child is spawned, so its death closes the sockets.
struct ChildEnds(#[allow(dead_code)] [OwnedFd; 3]);

fn command(cfg: &HostConfig, extra: &[&str]) -> (Command, StdUnix, StdUnix, ChildEnds) {
    let (control_ours, control_theirs) = StdUnix::pair().unwrap();
    let (worker_ours, worker_theirs) = StdUnix::pair().unwrap();
    let (cfg_read, cfg_write) = std::io::pipe().unwrap();
    let mut w = cfg_write;
    w.write_all(&serde_json::to_vec(cfg).unwrap()).unwrap();
    drop(w);
    let fds: [OwnedFd; 3] = [control_theirs.into(), worker_theirs.into(), cfg_read.into()];
    let raw: Vec<i32> = fds.iter().map(|f| f.as_raw_fd()).collect();
    let mut cmd = Command::new(BIN);
    cmd.args([
        "host",
        "--control-fd",
        "3",
        "--worker-fd",
        "4",
        "--config-fd",
        "5",
    ])
    .args(extra)
    .env("LEGION_ISOLATE_LOG", "warn")
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    // SAFETY: dup2 into the child's fd 3..5 (clearing CLOEXEC) between fork
    // and exec; only async-signal-safe calls. Each source is first moved
    // above 200, so a source numbered 3..5 is not overwritten before it is
    // placed.
    unsafe {
        cmd.pre_exec(move || {
            let mut high = [0i32; 3];
            for (i, fd) in raw.iter().enumerate() {
                high[i] = libc::fcntl(*fd, libc::F_DUPFD_CLOEXEC, 200);
                if high[i] < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            for (i, fd) in high.iter().enumerate() {
                if libc::dup2(*fd, 3 + i as i32) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    (cmd, control_ours, worker_ours, ChildEnds(fds))
}

async fn spawn(cfg: HostConfig) -> Spawned {
    let (mut cmd, control, worker_sock, ends) = command(&cfg, &[]);
    let child = cmd.spawn().unwrap();
    drop(ends);
    let worker = Arc::new(FakeWorker::default());
    let svc = IsolateWorkerServer::new(Grpc(worker.clone()));
    // SAFETY: we own these sockets.
    let ws = unsafe { inherited(OwnedFd::from(worker_sock).into_raw_fd_for_test()) }.unwrap();
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(ws)))
            .await;
    });
    let cs = unsafe { inherited(OwnedFd::from(control).into_raw_fd_for_test()) }.unwrap();
    let client = IsolateHostClient::new(channel_over(cs).await.unwrap());
    Spawned {
        child,
        client,
        worker,
    }
}

trait IntoRaw {
    fn into_raw_fd_for_test(self) -> i32;
}

impl IntoRaw for OwnedFd {
    fn into_raw_fd_for_test(self) -> i32 {
        use std::os::fd::IntoRawFd;
        self.into_raw_fd()
    }
}

fn bin_config(confine: Confine) -> HostConfig {
    let mut c = config();
    c.confine = confine as i32;
    c.shards = 2;
    c
}

fn stderr_of(child: &mut Child) -> String {
    let mut s = String::new();
    if let Some(mut e) = child.stderr.take() {
        use std::io::Read;
        let _ = e.read_to_string(&mut s);
    }
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_confined_host_serves_runs_sessions_and_egress_over_grpc_test() {
    let mut h = spawn(bin_config(Confine::BestEffort)).await;
    let hello = h.client.hello(HelloRequest {}).await.unwrap().into_inner();
    assert_eq!(hello.protocol_version, PROTOCOL_VERSION);
    assert_eq!(hello.shards, 2);
    assert_eq!(hello.pid, h.child.id());
    let c = hello.confinement.unwrap();
    eprintln!("confinement in this container: {c:?}");
    // What applies here (kernel 6.18, root in a Firecracker guest) is
    // asserted by `confinement_layers_report_what_the_kernel_offers_test`.
    let info = std::process::Command::new(BIN)
        .arg("engine-info")
        .output()
        .unwrap();
    let info: serde_json::Value = serde_json::from_slice(&info.stdout).unwrap();
    assert_eq!(info["compatKey"], hello.compat_key);

    // A run, through the seccomp allowlist.
    for m in ["cmd", "runner"] {
        h.client
            .load_module(LoadModuleRequest {
                module_id: m.into(),
                artifact_path: artifact(m).display().to_string(),
            })
            .await
            .unwrap();
    }
    h.client
        .configure_task(spec("j", "cmd", "cmd"))
        .await
        .unwrap();
    let inv = |task: &str, session: &str, args: &[&str]| InvokeRequest {
        job_id: "j".into(),
        task_name: task.into(),
        invocation_id: next_id(),
        session_id: session.into(),
        call: Some(invoke_request::Call::Run(run(args))),
        ..Default::default()
    };
    let r = h
        .client
        .invoke(inv("cmd", "", &["echo", "a"]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(exit(&r), Some(0), "{:?}", r);
    assert!(stdout(&r).contains("args=echo,a"));
    // Limits work the same in the confined process.
    let mut s = spec("j", "spin", "cmd");
    s.limits.as_mut().unwrap().cpu_micros = 50_000;
    h.client.configure_task(s).await.unwrap();
    let r = h
        .client
        .invoke(inv("spin", "", &["spin"]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stopped(&r).1, "cpu");
    let r = h
        .client
        .invoke(inv("cmd", "", &["trap"]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stopped(&r).0, "trap");
    let r = h
        .client
        .invoke(inv("cmd", "", &["sleep", "20"]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stdout(&r), "slept\n");

    // Egress over the worker socket.
    let r = h
        .client
        .invoke(inv("cmd", "", &["fetch", "http://up/x"]))
        .await
        .unwrap()
        .into_inner();
    assert!(
        stdout(&r).contains("body=hello from http://up/x"),
        "{}",
        stdout(&r)
    );
    assert_eq!(h.worker.egress_seen.lock().unwrap().len(), 1);

    // A session: hibernate through SaveCheckpoint, wake through
    // LoadCheckpoint.
    let mut s = spec("j", "sess", "runner");
    s.lifetime = Lifetime::Session as i32;
    h.client.configure_task(s).await.unwrap();
    h.client
        .open_session(OpenSessionRequest {
            job_id: "j".into(),
            task_name: "sess".into(),
            session_id: "s1".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    h.client
        .invoke(inv("sess", "s1", &["set", "k", "v"]))
        .await
        .unwrap();
    let rec = h
        .client
        .reclaim(ReclaimRequest {
            level: ReclaimLevel::ReclaimHibernate as i32,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rec.hibernated, 1);
    let r = h
        .client
        .invoke(inv("sess", "s1", &["get", "k"]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(outcome(&r).result, "\"v\"");
    assert_eq!(r.start, "hibernated");
    let st = h.client.stats(StatsRequest {}).await.unwrap().into_inner();
    assert!(st.rss_bytes > 0, "statm was opened before confinement");
    assert!(
        h.child.try_wait().unwrap().is_none(),
        "{}",
        stderr_of(&mut h.child)
    );
}

/// Confinement on, as a worker runs it: Hello and one Invoke over the
/// inherited sockets, under best effort and under enforce (every layer
/// applies in this container; on a kernel without one, enforce exits
/// before serving and this test says so).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hello_and_invoke_work_with_confinement_on_test() {
    for level in [Confine::BestEffort, Confine::Enforce] {
        let (mut cmd, control, _worker, ends) = command(&bin_config(level), &[]);
        let mut child = cmd.spawn().unwrap();
        drop(ends);
        // SAFETY: we own the socket.
        let cs = unsafe { inherited(OwnedFd::from(control).into_raw_fd_for_test()) }.unwrap();
        let mut client = IsolateHostClient::new(channel_over(cs).await.unwrap());
        let hello = match client.hello(HelloRequest {}).await {
            Ok(h) => h.into_inner(),
            Err(e) => {
                let status = child.wait().unwrap();
                let err = stderr_of(&mut child);
                assert!(
                    level == Confine::Enforce && status.code() == Some(3),
                    "{level:?}: Hello failed: {e}; host {status:?}: {err}"
                );
                eprintln!("LAYER UNAVAILABLE HERE: enforce refused to start: {err}");
                continue;
            }
        };
        let c = hello.confinement.unwrap();
        eprintln!("{level:?}: {c:?}");
        client
            .load_module(LoadModuleRequest {
                module_id: "cmd".into(),
                artifact_path: artifact("cmd").display().to_string(),
            })
            .await
            .unwrap();
        client
            .configure_task(spec("j", "cmd", "cmd"))
            .await
            .unwrap();
        let r = client
            .invoke(InvokeRequest {
                job_id: "j".into(),
                task_name: "cmd".into(),
                invocation_id: next_id(),
                call: Some(invoke_request::Call::Run(run(&["echo", "x"]))),
                ..Default::default()
            })
            .await
            .unwrap_or_else(|e| panic!("{level:?}: {e}: {}", stderr_of(&mut child)))
            .into_inner();
        assert_eq!(exit(&r), Some(0), "{level:?}");
        if level == Confine::Enforce {
            assert!(
                c.user_namespace
                    && c.network_namespace
                    && c.no_new_privs
                    && c.landlock
                    && c.seccomp
                    && c.pid_namespace
                    && c.mount_root,
                "{c:?}"
            );
        }
        drop(client);
        let _ = child.kill();
        let _ = child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_host_exits_when_the_control_socket_closes_test() {
    let (mut cmd, control, _worker, ends) = command(&bin_config(Confine::BestEffort), &[]);
    let mut child = cmd.spawn().unwrap();
    drop(ends);
    {
        // SAFETY: we own the socket.
        let cs = unsafe { inherited(OwnedFd::from(control).into_raw_fd_for_test()) }.unwrap();
        let mut client = IsolateHostClient::new(channel_over(cs).await.unwrap());
        client.hello(HelloRequest {}).await.unwrap();
    }
    // The client (and with it our end of the socket) is gone.
    let t = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "{status:?}: {}", stderr_of(&mut child));
            break;
        }
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "the host outlived its control socket"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[test]
fn a_protocol_version_mismatch_exits_before_serving_test() {
    let mut cfg = bin_config(Confine::Off);
    cfg.protocol_version = PROTOCOL_VERSION + 7;
    let (mut cmd, _c, _w, _ends) = command(&cfg, &[]);
    let out = cmd.output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("protocol"), "{err}");
}

#[test]
fn compile_refuses_and_reports_as_json_test() {
    let dir = tempfile::tempdir().unwrap();
    let core = dir.path().join("core.wasm");
    std::fs::write(&core, wat::parse_str("(module)").unwrap()).unwrap();
    let out = Command::new(BIN)
        .args(["compile", "--input"])
        .arg(&core)
        .arg("--output")
        .arg(dir.path().join("x.cwasm"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["ok"], false);
    assert_eq!(v["kind"], "invalid");
    let ok = Command::new(BIN)
        .args(["compile", "--input"])
        .arg(fixture("cmd"))
        .arg("--output")
        .arg(dir.path().join("cmd.cwasm"))
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&ok.stdout).unwrap();
    assert_eq!(v["ok"], true, "{v}");
    assert_eq!(v["exports"]["cliRun"], true);
    assert!(v["confinement"]["noNewPrivs"].as_bool().unwrap());
}

#[test]
fn compat_key_changes_with_layout_test() {
    let key = |layout: &str| {
        let o = Command::new(BIN)
            .args(["engine-info", "--layout", layout])
            .output()
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
        v["compatKey"].as_str().unwrap().to_string()
    };
    assert_ne!(key("guard"), key("small"));
    assert_ne!(key("guard"), key("mpk"));
}

// ---------------------------------------------------------------------------
// Confinement: needs the binary built with `--features confinement-test`.
// ---------------------------------------------------------------------------

/// Run `host --test-attempt <what>` under full confinement and return
/// (exit status, stdout, stderr).
#[cfg(feature = "confinement-test")]
fn attempt(what: &str) -> (std::process::ExitStatus, String, String) {
    attempt_skipping(what, &[])
}

/// The same with some confinement layers left out (`--test-skip`), to show
/// that each remaining layer stops an attack on its own.
#[cfg(feature = "confinement-test")]
fn attempt_skipping(what: &str, skip: &[&str]) -> (std::process::ExitStatus, String, String) {
    let cfg = bin_config(Confine::BestEffort);
    let mut extra = vec!["--test-attempt", what];
    let skip = skip.join(",");
    if !skip.is_empty() {
        extra.extend(["--test-skip", &skip]);
    }
    let (mut cmd, _c, _w, _ends) = command(&cfg, &extra);
    let out = cmd.output().unwrap();
    (
        out.status,
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[cfg(feature = "confinement-test")]
fn hello_confinement() -> Confinement {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut h = spawn(bin_config(Confine::BestEffort)).await;
        h.client
            .hello(HelloRequest {})
            .await
            .unwrap()
            .into_inner()
            .confinement
            .unwrap()
    })
}

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs `cargo test -p legion-isolate --features confinement-test`"
)]
fn confinement_layers_report_what_the_kernel_offers_test() {
    #[cfg(feature = "confinement-test")]
    {
        let c = hello_confinement();
        eprintln!("layers: {c:?}");
        // Every layer applies in this container; on a kernel that lacks one
        // the host must say so in `notes` rather than claim it.
        for (on, name) in [
            (c.user_namespace, "user namespace"),
            (c.network_namespace, "network namespace"),
            (c.no_new_privs, "no_new_privs"),
            (c.landlock, "landlock"),
            (c.seccomp, "seccomp"),
            (c.pid_namespace, "pid namespace"),
            (c.mount_root, "mount root"),
            (c.landlock_scopes, "landlock scopes"),
        ] {
            if !on {
                eprintln!("LAYER UNAVAILABLE HERE: {name}");
                assert!(
                    c.notes
                        .iter()
                        .any(|n| n.to_lowercase().contains(&name[..4].to_lowercase())),
                    "{name} missing without a note: {c:?}"
                );
            }
        }
    }
}

#[cfg(feature = "confinement-test")]
fn assert_killed_by_seccomp(what: &str) {
    let c = hello_confinement();
    let (status, out, err) = attempt(what);
    if !c.seccomp {
        eprintln!("LAYER UNAVAILABLE HERE: seccomp; {what} not refused by it: {out}");
        return;
    }
    assert_eq!(
        status.signal(),
        Some(libc::SIGSYS),
        "{what} was not killed by seccomp: {status:?} out {out} err {err}"
    );
}

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs --features confinement-test"
)]
fn seccomp_kills_a_socket_call_test() {
    #[cfg(feature = "confinement-test")]
    assert_killed_by_seccomp("socket");
}

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs --features confinement-test"
)]
fn seccomp_kills_execve_and_fork_test() {
    #[cfg(feature = "confinement-test")]
    {
        assert_killed_by_seccomp("execve");
        assert_killed_by_seccomp("fork");
    }
}

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs --features confinement-test"
)]
fn seccomp_kills_ptrace_test() {
    #[cfg(feature = "confinement-test")]
    assert_killed_by_seccomp("ptrace");
}

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs --features confinement-test"
)]
fn the_filesystem_outside_read_dirs_does_not_exist_for_the_host_test() {
    #[cfg(feature = "confinement-test")]
    {
        let c = hello_confinement();
        artifact("cmd");
        let (_, out, _) = attempt("read-inside");
        assert!(out.contains("bytes"), "{out}");
        let (_, out, _) = attempt("write-inside");
        assert!(
            out.contains(&format!("Some({})", libc::EROFS))
                || out.contains(&format!("Some({})", libc::EACCES)),
            "{out}"
        );
        if !c.mount_root {
            eprintln!("LAYER UNAVAILABLE HERE: mount root ({:?})", c.notes);
        } else {
            // Not "permission denied": the node's files are not there.
            let enoent = format!("Some({})", libc::ENOENT);
            for what in [
                "open-outside",
                "open-proc",
                "stat-outside",
                "readlink-outside",
            ] {
                let (status, out, err) = attempt(what);
                assert!(status.success(), "{what}: {status:?} {err}");
                assert!(out.contains(&enoent), "{what}: {out}");
            }
            // The mount root alone (no Landlock) hides them too.
            let (_, out, _) = attempt_skipping("open-outside", &["landlock"]);
            assert!(out.contains(&enoent), "{out}");
        }
        if !c.landlock {
            eprintln!("LAYER UNAVAILABLE HERE: landlock ({:?})", c.notes);
        } else {
            // Landlock alone (no mount root) refuses them.
            let (_, out, _) = attempt_skipping("open-outside", &["mount"]);
            assert!(out.contains(&format!("Some({})", libc::EACCES)), "{out}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_host_network_namespace_has_no_interface_test() {
    let h = spawn(bin_config(Confine::BestEffort)).await;
    let mut client = h.client.clone();
    let c = client
        .hello(HelloRequest {})
        .await
        .unwrap()
        .into_inner()
        .confinement
        .unwrap();
    if !c.network_namespace {
        eprintln!("LAYER UNAVAILABLE HERE: network namespace ({:?})", c.notes);
        return;
    }
    let pid = h.child.id();
    let ours = std::fs::read_link("/proc/self/ns/net").unwrap();
    let theirs = std::fs::read_link(format!("/proc/{pid}/ns/net")).unwrap();
    assert_ne!(ours, theirs, "the host has its own network namespace");
    let dev = std::fs::read_to_string(format!("/proc/{pid}/net/dev")).unwrap();
    let ifaces: Vec<&str> = dev
        .lines()
        .skip(2)
        .filter_map(|l| l.split(':').next())
        .map(str::trim)
        .collect();
    assert_eq!(ifaces, ["lo"], "{dev}");
    // Loopback exists in every namespace but is not configured (down).
    let flags = std::fs::read_to_string(format!("/proc/{pid}/net/if_inet6")).unwrap_or_default();
    assert!(flags.is_empty(), "no addresses: {flags}");
    // Its user namespace maps only our uid, to 65534 inside.
    let map = std::fs::read_to_string(format!("/proc/{pid}/uid_map")).unwrap();
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    assert_eq!(
        map.split_whitespace().collect::<Vec<_>>(),
        ["65534", &uid.to_string(), "1"],
        "{map}"
    );
}

// ---------------------------------------------------------------------------
// Signals, descriptors, the PID namespace (review findings 2 and 10).
// ---------------------------------------------------------------------------

/// A process outside the host that a compromised host would like to kill.
#[cfg(feature = "confinement-test")]
struct Victim(Child);

#[cfg(feature = "confinement-test")]
impl Victim {
    fn new() -> Self {
        Victim(Command::new("sleep").arg("60").spawn().unwrap())
    }
    fn pid(&self) -> u32 {
        self.0.id()
    }
    fn alive(&mut self) -> bool {
        self.0.try_wait().unwrap().is_none()
    }
}

#[cfg(feature = "confinement-test")]
impl Drop for Victim {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Every route by which a process can make the kernel signal another one
/// through its own descriptors.
#[cfg(feature = "confinement-test")]
const SIGNAL_ROUTES: &[&str] = &[
    "sigio-fcntl",
    "sigio-setown-ex",
    "sigio-ioctl",
    "sigio-pgrp",
    "kill",
];

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs --features confinement-test"
)]
fn seccomp_kills_every_signal_route_and_the_victim_survives_test() {
    #[cfg(feature = "confinement-test")]
    {
        let c = hello_confinement();
        if !c.seccomp {
            eprintln!("LAYER UNAVAILABLE HERE: seccomp");
            return;
        }
        for route in SIGNAL_ROUTES.iter().chain(&["fsetfl-async"]) {
            let mut v = Victim::new();
            let (status, out, err) = attempt(&format!("{route}:{}", v.pid()));
            assert_eq!(
                status.signal(),
                Some(libc::SIGSYS),
                "{route}: {status:?} {out} {err}"
            );
            assert!(v.alive(), "{route} killed the victim");
        }
        // The flags the runtime does use stay allowed.
        let (status, out, err) = attempt("fsetfl-nonblock");
        assert!(status.success(), "{status:?} {err}");
        assert!(out.contains("returned 0"), "{out}");
    }
}

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs --features confinement-test"
)]
fn the_pid_namespace_alone_hides_every_outside_process_test() {
    #[cfg(feature = "confinement-test")]
    {
        let c = hello_confinement();
        if !c.pid_namespace {
            eprintln!("LAYER UNAVAILABLE HERE: pid namespace ({:?})", c.notes);
            return;
        }
        let (_, out, _) = attempt("pid");
        assert_eq!(out.trim(), "pid 1", "the host is PID 1 of its namespace");
        for route in SIGNAL_ROUTES {
            let mut v = Victim::new();
            let (status, out, err) =
                attempt_skipping(&format!("{route}:{}", v.pid()), &["seccomp", "scopes"]);
            assert!(status.success(), "{route}: {status:?} {err}");
            // The pid does not exist in the host's namespace.
            assert!(
                out.contains(&format!("/{}", libc::ESRCH))
                    || out.contains(&format!("errno {}", libc::ESRCH)),
                "{route}: {out}"
            );
            assert!(v.alive(), "{route} killed the victim: {out}");
        }
    }
}

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs --features confinement-test"
)]
fn landlock_scopes_alone_stop_signals_to_outside_processes_test() {
    #[cfg(feature = "confinement-test")]
    {
        let c = hello_confinement();
        if !c.landlock_scopes {
            eprintln!("LAYER UNAVAILABLE HERE: landlock scopes ({:?})", c.notes);
            return;
        }
        for route in SIGNAL_ROUTES {
            let mut v = Victim::new();
            let (status, out, err) =
                attempt_skipping(&format!("{route}:{}", v.pid()), &["seccomp", "pidns"]);
            assert!(status.success(), "{route}: {status:?} {err}");
            assert!(v.alive(), "{route} killed the victim: {out}");
        }
        // Without any of the three layers the attack is real here: the
        // test above is not passing by accident.
        let mut v = Victim::new();
        let (_, out, _) = attempt_skipping(
            &format!("sigio-fcntl:{}", v.pid()),
            &["seccomp", "pidns", "scopes"],
        );
        std::thread::sleep(Duration::from_millis(200));
        assert!(!v.alive(), "the unconfined route should kill: {out}");
    }
}

/// Start the binary with one more inherited descriptor (number 100) that
/// the worker did not mean to pass, as a missing `CLOEXEC` would.
#[cfg(feature = "confinement-test")]
fn attempt_with_stray_fd(what: &str) -> String {
    let (stray, _keep) = StdUnix::pair().unwrap();
    // Numbered above anything `command` places, so its placing cannot
    // overwrite it.
    // SAFETY: duplicating a descriptor we own; closed when `_high` drops.
    let high = unsafe { libc::fcntl(stray.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 300) };
    assert!(high >= 300);
    // SAFETY: we own `high`.
    let _high = unsafe { <OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(high) };
    let cfg = bin_config(Confine::BestEffort);
    let (mut cmd, _c, _w, _ends) = command(&cfg, &["--test-attempt", what]);
    // SAFETY: dup2 between fork and exec.
    unsafe {
        cmd.pre_exec(move || {
            if libc::dup2(high, 100) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let out = cmd.output().unwrap();
    assert!(out.status.success(), "{:?}", out);
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs --features confinement-test"
)]
fn a_stray_inherited_descriptor_is_closed_test() {
    #[cfg(feature = "confinement-test")]
    {
        let out = attempt_with_stray_fd("fds");
        let fds: Vec<i32> = out
            .trim()
            .trim_start_matches("fds: ")
            .split(',')
            .map(|f| f.parse().unwrap())
            .collect();
        assert!(!fds.contains(&100), "fd 100 reached the host: {out}");
        // Stdio, the two sockets, and /proc/self/statm (in the config
        // pipe's place: it was read and closed).
        assert_eq!(fds, [0, 1, 2, 3, 4, 5], "{out}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killing_the_pid_the_worker_knows_kills_the_host_test() {
    let (mut cmd, control, _worker, ends) = command(&bin_config(Confine::BestEffort), &[]);
    let mut child = cmd.spawn().unwrap();
    drop(ends);
    // SAFETY: we own the socket.
    let cs = unsafe { inherited(OwnedFd::from(control).into_raw_fd_for_test()) }.unwrap();
    let mut client = IsolateHostClient::new(channel_over(cs).await.unwrap());
    let hello = client.hello(HelloRequest {}).await.unwrap().into_inner();
    assert_eq!(
        hello.pid,
        child.id(),
        "Hello reports the pid the worker spawned"
    );
    let c = hello.confinement.unwrap();
    if !c.pid_namespace {
        eprintln!("LAYER UNAVAILABLE HERE: pid namespace ({:?})", c.notes);
        let _ = child.kill();
        let _ = child.wait();
        return;
    }
    // The serving process is the spawned one's only child.
    let pid = child.id();
    let children = std::fs::read_to_string(format!("/proc/{pid}/task/{pid}/children")).unwrap();
    let host: u32 = children.trim().parse().unwrap();
    let ns = |p: u32| std::fs::read_link(format!("/proc/{p}/ns/pid")).ok();
    assert_ne!(
        ns(host),
        ns(std::process::id()),
        "the host has its own PID namespace"
    );
    child.kill().unwrap();
    child.wait().unwrap();
    let t = Instant::now();
    loop {
        let state = std::fs::read_to_string(format!("/proc/{host}/stat")).unwrap_or_default();
        // Gone, or a zombie waiting for init to reap it.
        if state.is_empty() || state.contains(") Z") {
            break;
        }
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "the host outlived its parent: {state}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(client.hello(HelloRequest {}).await.is_err());
}

// ---------------------------------------------------------------------------
// The compiler is confined on the same terms (review findings 3 and 10).
// ---------------------------------------------------------------------------

#[cfg(feature = "confinement-test")]
fn compile_attempt(what: &str) -> (std::process::ExitStatus, String) {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.wasm");
    std::fs::copy(fixture("cmd"), &input).unwrap();
    std::fs::create_dir(dir.path().join("out")).unwrap();
    let out = Command::new(BIN)
        .args(["compile", "--confine", "best-effort", "--input"])
        .arg(&input)
        .arg("--output")
        .arg(dir.path().join("out").join("x.cwasm"))
        .args(["--test-attempt", what])
        .output()
        .unwrap();
    (
        out.status,
        String::from_utf8_lossy(&out.stdout).into_owned(),
    )
}

#[test]
fn the_compiler_reports_every_layer_and_honours_confine_test() {
    let dir = tempfile::tempdir().unwrap();
    let run = |confine: &str| {
        let o = Command::new(BIN)
            .args(["compile", "--confine", confine, "--input"])
            .arg(fixture("cmd"))
            .arg("--output")
            .arg(dir.path().join(format!("{confine}.cwasm")))
            .output()
            .unwrap();
        (o.status, String::from_utf8_lossy(&o.stdout).into_owned())
    };
    let (status, out) = run("enforce");
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    if v["kind"] == "confinement" {
        eprintln!("LAYER UNAVAILABLE HERE: compiler enforce refused: {v}");
        assert_eq!(status.code(), Some(3));
    } else {
        assert_eq!(v["ok"], true, "{v}");
        let c = &v["confinement"];
        for layer in [
            "userNamespace",
            "networkNamespace",
            "pidNamespace",
            "mountRoot",
            "noNewPrivs",
            "landlock",
            "seccomp",
        ] {
            assert_eq!(c[layer], true, "{layer}: {v}");
        }
        assert!(dir.path().join("enforce.cwasm").is_file());
    }
    let (status, out) = run("off");
    assert!(status.success(), "{out}");
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(v["confinement"]["seccomp"], false, "{v}");
    let (status, _) = run("sometimes");
    assert_eq!(status.code(), Some(2), "clap refuses an unknown level");
}

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs --features confinement-test"
)]
fn the_compiler_is_refused_what_the_host_is_refused_test() {
    #[cfg(feature = "confinement-test")]
    {
        let c = hello_confinement();
        if c.seccomp {
            for what in [
                "socket",
                "execve",
                "fork",
                "ptrace",
                "sigio-fcntl:1",
                "kill:1",
            ] {
                let (status, out) = compile_attempt(what);
                assert_eq!(
                    status.signal(),
                    Some(libc::SIGSYS),
                    "{what}: {status:?} {out}"
                );
            }
        }
        let enoent = format!("Some({})", libc::ENOENT);
        if c.mount_root {
            for what in [
                "open-outside",
                "open-proc",
                "stat-outside",
                "readlink-outside",
            ] {
                let (status, out) = compile_attempt(what);
                // `readlink` is not on the compiler's allowlist at all.
                if what == "readlink-outside" && c.seccomp {
                    assert_eq!(status.signal(), Some(libc::SIGSYS), "{what}: {out}");
                    continue;
                }
                assert!(status.success(), "{what}: {status:?}");
                assert!(out.contains(&enoent), "{what}: {out}");
            }
        }
        let (_, out) = compile_attempt("read-inside");
        assert!(out.contains("bytes"), "the input is readable: {out}");
        let (_, out) = compile_attempt("write-inside");
        assert!(
            out.contains(&format!("Some({})", libc::EROFS))
                || out.contains(&format!("Some({})", libc::EACCES)),
            "the input is read-only: {out}"
        );
        let (_, out) = compile_attempt("write-output");
        assert!(
            out.contains("wrote"),
            "the output directory is writable: {out}"
        );
        let (_, out) = compile_attempt("pid");
        if c.pid_namespace {
            assert_eq!(out.trim(), "pid 1");
        }
        let (_, out) = compile_attempt("fds");
        assert_eq!(out.trim(), "fds: 0,1,2", "{out}");
    }
}

// ---------------------------------------------------------------------------
// The language images through the real, confined binary (compiler and host).
// ---------------------------------------------------------------------------

fn images_dir() -> std::path::PathBuf {
    match std::env::var_os("LEGION_ISOLATE_IMAGES") {
        Some(d) => d.into(),
        None => {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/isolate-images")
        }
    }
}

/// `tests/images.rs` drives the images in-process; this runs them where a
/// worker does: compiled by `legion-isolate compile --confine enforce`,
/// served by a host under `CONFINE_ENFORCE` (every seccomp argument filter,
/// the mount root, the PID namespace), with file I/O, an exception, and an
/// outgoing request over the worker socket. A syscall either path needs
/// that the allowlists lack shows up here as a SIGSYS.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn both_language_images_run_in_the_confined_binary_test() {
    let images = images_dir();
    let out = tempfile::tempdir().unwrap();
    for name in ["python", "js"] {
        let o = Command::new(BIN)
            .args(["compile", "--confine", "enforce", "--input"])
            .arg(images.join(format!("{name}.wasm")))
            .arg("--output")
            .arg(out.path().join(format!("{name}.cwasm")))
            .output()
            .unwrap();
        let v: serde_json::Value =
            serde_json::from_slice(&o.stdout).unwrap_or_else(|e| panic!("{name}: {e}: {o:?}"));
        assert_eq!(v["ok"], true, "{name}: {v} {o:?}");
        assert_eq!(v["confinement"]["seccomp"], true, "{v}");
    }
    let mut cfg = bin_config(Confine::Enforce);
    cfg.read_dirs = vec![out.path().display().to_string()];
    cfg.async_stack_bytes = 1 << 20;
    let mut h = spawn(cfg).await;
    let hello = h.client.hello(HelloRequest {}).await.unwrap().into_inner();
    eprintln!("confinement: {:?}", hello.confinement);
    for (name, task) in [("python", "py"), ("js", "js")] {
        h.client
            .load_module(LoadModuleRequest {
                module_id: name.into(),
                artifact_path: out
                    .path()
                    .join(format!("{name}.cwasm"))
                    .display()
                    .to_string(),
            })
            .await
            .unwrap();
        let mut s = spec("j", task, name);
        s.memory_max_bytes = 128 << 20;
        s.limits.as_mut().unwrap().cpu_micros = 20_000_000;
        s.limits.as_mut().unwrap().wall_micros = 30_000_000;
        h.client.configure_task(s).await.unwrap();
    }
    let run_src = |task: &str, file: &str, src: &str| InvokeRequest {
        job_id: "j".into(),
        task_name: task.into(),
        invocation_id: next_id(),
        call: Some(invoke_request::Call::Run(RunCall {
            files: vec![File {
                path: file.into(),
                content: src.as_bytes().to_vec(),
                ..Default::default()
            }],
            entry: file.into(),
            input: Some("{\"n\":2}".into()),
            ..Default::default()
        })),
        ..Default::default()
    };
    let py = r#"
import json, os, urllib.request
with open("note.txt", "w") as f:
    f.write("written")
os.makedirs("out/deep", exist_ok=True)
os.rename("note.txt", "out/deep/note.txt")
try:
    1 / 0
except ZeroDivisionError:
    caught = True
body = urllib.request.urlopen("http://up/py").read().decode()
print("hello from python")
result = {"read": open("out/deep/note.txt").read(), "caught": caught, "body": body,
          "n": json.load(open("input.json"))["n"]}
"#;
    let r = h.client.invoke(run_src("py", "main.py", py)).await;
    let r = r
        .unwrap_or_else(|e| panic!("{e}: {}", stderr_of(&mut h.child)))
        .into_inner();
    assert!(r.stopped.is_none(), "{r:?}");
    assert_eq!(stdout(&r), "hello from python\n", "{r:?}");
    let v: serde_json::Value = serde_json::from_str(&outcome(&r).result).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"read": "written", "caught": true, "body": "hello from http://up/py", "n": 2})
    );
    let js = "let caught = false;\n\
              try { JSON.parse('{'); } catch (e) { caught = e instanceof SyntaxError; }\n\
              const r = await fetch('http://up/js');\n\
              console.log('hello from js');\n\
              return { status: r.status, body: await r.text(), caught };\n";
    let r = h.client.invoke(run_src("js", "main.js", js)).await;
    let r = r
        .unwrap_or_else(|e| panic!("{e}: {}", stderr_of(&mut h.child)))
        .into_inner();
    assert!(r.stopped.is_none(), "{r:?}");
    assert!(stdout(&r).contains("hello from js"), "{r:?}");
    let v: serde_json::Value = serde_json::from_str(&outcome(&r).result).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"status": 200, "body": "hello from http://up/js", "caught": true})
    );
    assert!(
        h.child.try_wait().unwrap().is_none(),
        "{}",
        stderr_of(&mut h.child)
    );
}

// ---------------------------------------------------------------------------
// Second review: submounts, capabilities without a user namespace, symlinked
// read dirs, the compile deadline, and the test hooks in shipping builds.
// ---------------------------------------------------------------------------

/// `host --test-attempt <what>` with `cfg` and some layers left out.
#[cfg(feature = "confinement-test")]
fn attempt_with(cfg: &HostConfig, what: &str, skip: &[&str]) -> (std::process::ExitStatus, String) {
    let mut extra = vec!["--test-attempt", what];
    let skip = skip.join(",");
    if !skip.is_empty() {
        extra.extend(["--test-skip", &skip]);
    }
    let (mut cmd, _c, _w, _ends) = command(cfg, &extra);
    let out = cmd.output().unwrap();
    (
        out.status,
        format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
    )
}

/// A tmpfs mounted at `<dir>/sub` for the test's lifetime.
#[cfg(feature = "confinement-test")]
struct SubMount(std::path::PathBuf);

#[cfg(feature = "confinement-test")]
impl SubMount {
    fn new(dir: &std::path::Path) -> Option<Self> {
        let sub = dir.join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        let c =
            |p: &std::path::Path| std::ffi::CString::new(p.as_os_str().as_encoded_bytes()).unwrap();
        let (src, fs, at) = (c("tmpfs".as_ref()), c("tmpfs".as_ref()), c(&sub));
        // SAFETY: NUL-terminated strings, no data.
        let r = unsafe { libc::mount(src.as_ptr(), at.as_ptr(), fs.as_ptr(), 0, std::ptr::null()) };
        (r == 0).then(|| SubMount(sub))
    }
}

#[cfg(feature = "confinement-test")]
impl Drop for SubMount {
    fn drop(&mut self) {
        let c = std::ffi::CString::new(self.0.as_os_str().as_encoded_bytes()).unwrap();
        // SAFETY: a NUL-terminated path.
        unsafe { libc::umount2(c.as_ptr(), libc::MNT_DETACH) };
    }
}

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs --features confinement-test"
)]
fn a_mount_inside_a_read_dir_is_read_only_for_the_host_test() {
    #[cfg(feature = "confinement-test")]
    {
        let c = hello_confinement();
        if !c.mount_root {
            eprintln!("LAYER UNAVAILABLE HERE: mount root ({:?})", c.notes);
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let Some(_sub) = SubMount::new(dir.path()) else {
            eprintln!(
                "cannot mount a tmpfs here ({}); not tested",
                std::io::Error::last_os_error()
            );
            return;
        };
        let mut cfg = bin_config(Confine::BestEffort);
        cfg.read_dirs = vec![dir.path().display().to_string()];
        // Without the mount root (and Landlock) the submount is writable:
        // the checks below are not passing by accident.
        let (_, out) = attempt_with(&cfg, "write-in:sub", &["landlock", "mount"]);
        assert!(out.contains("write-in: wrote"), "{out}");
        let erofs = format!("Some({})", libc::EROFS);
        // The mount root alone (Landlock left out, or it would refuse the
        // write itself): mount_setattr(AT_RECURSIVE).
        let (status, out) = attempt_with(&cfg, "write-in:sub", &["landlock"]);
        assert!(status.success(), "{status:?} {out}");
        assert!(out.contains(&erofs), "the submount is read-only: {out}");
        // The fallback that walks the mount table.
        let (status, out) = attempt_with(&cfg, "write-in:sub", &["landlock", "setattr"]);
        assert!(status.success(), "{status:?} {out}");
        assert!(out.contains(&erofs), "the submount is read-only: {out}");
        // The directory itself stays read-only too.
        let (_, out) = attempt_with(&cfg, "write-in:.", &["landlock"]);
        assert!(out.contains(&erofs), "{out}");
    }
}

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs --features confinement-test"
)]
fn without_a_user_namespace_every_capability_is_dropped_test() {
    #[cfg(feature = "confinement-test")]
    {
        let cfg = bin_config(Confine::BestEffort);
        // seccomp would kill capget itself; Landlock does not filter it.
        let (status, out) = attempt_with(&cfg, "caps", &["userns", "seccomp"]);
        assert!(status.success(), "{status:?} {out}");
        assert!(
            out.contains(
                "caps: capget=0 effective=0 permitted=0 inheritable=0 bounding=0 ambient=0"
            ),
            "{out}"
        );
        // And under enforce the missing namespace still refuses to start.
        let cfg = bin_config(Confine::Enforce);
        let (status, out) = attempt_with(&cfg, "caps", &["userns"]);
        assert_eq!(status.code(), Some(3), "{out}");
        assert!(out.contains("user namespace"), "{out}");
    }
}

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs --features confinement-test"
)]
fn a_symlinked_read_dir_stays_readable_test() {
    #[cfg(feature = "confinement-test")]
    {
        artifact("cmd");
        let links = tempfile::tempdir().unwrap();
        let link = links.path().join("artifacts-link");
        std::os::unix::fs::symlink(artifacts(), &link).unwrap();
        let mut cfg = bin_config(Confine::BestEffort);
        cfg.read_dirs = vec![link.display().to_string()];
        // Every layer, then Landlock alone, then the mount root alone.
        for skip in [&[][..], &["mount"][..], &["landlock"][..]] {
            let (status, out) = attempt_with(&cfg, "read-inside", skip);
            assert!(status.success(), "{skip:?}: {status:?} {out}");
            assert!(out.contains("read-inside: "), "{skip:?}: {out}");
            assert!(out.contains(" bytes"), "{skip:?}: {out}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_loads_a_module_through_a_symlinked_read_dir_test() {
    artifact("cmd");
    let links = tempfile::tempdir().unwrap();
    let link = links.path().join("artifacts-link");
    std::os::unix::fs::symlink(artifacts(), &link).unwrap();
    let mut cfg = bin_config(Confine::BestEffort);
    cfg.read_dirs = vec![link.display().to_string()];
    let mut h = spawn(cfg).await;
    // The worker passes canonical paths (the symlink does not exist in the
    // host's mount root).
    let path = std::fs::canonicalize(artifacts().join("cmd.cwasm")).unwrap();
    h.client
        .load_module(LoadModuleRequest {
            module_id: "cmd".into(),
            artifact_path: path.display().to_string(),
        })
        .await
        .unwrap_or_else(|e| panic!("{e}: {}", stderr_of(&mut h.child)));
}

#[test]
#[cfg_attr(
    not(feature = "confinement-test"),
    ignore = "needs --features confinement-test"
)]
fn a_compile_ends_at_its_deadline_test() {
    #[cfg(feature = "confinement-test")]
    {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.wasm");
        std::fs::copy(fixture("cmd"), &input).unwrap();
        let started = Instant::now();
        // The compiler's allowlist has no sleep; seccomp is left out.
        let out = Command::new(BIN)
            .args([
                "compile",
                "--confine",
                "best-effort",
                "--deadline-secs",
                "1",
            ])
            .arg("--input")
            .arg(&input)
            .arg("--output")
            .arg(dir.path().join("x.cwasm"))
            .args(["--test-skip", "seccomp", "--test-attempt", "sleep:30"])
            .output()
            .unwrap();
        assert_eq!(out.status.signal(), Some(libc::SIGALRM), "{out:?}");
        assert!(started.elapsed() < Duration::from_secs(20), "{out:?}");
        assert!(!String::from_utf8_lossy(&out.stdout).contains("woke"));
    }
}

/// A shipping build (no `confinement-test` feature) has no test hooks: the
/// flags are unknown arguments.
/// Compiled only without the feature (with it, the flags are accepted and
/// the host would serve).
#[cfg(not(feature = "confinement-test"))]
#[test]
fn a_shipping_build_rejects_the_test_hooks_test() {
    let cfg = bin_config(Confine::BestEffort);
    for extra in [
        &["--test-skip", "seccomp"][..],
        &["--test-attempt", "socket"][..],
    ] {
        let (mut cmd, _c, _w, _ends) = command(&cfg, extra);
        let out = cmd.output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{extra:?}: {out:?}");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("unexpected argument"),
            "{out:?}"
        );
    }
    let out = Command::new(BIN)
        .args([
            "compile",
            "--input",
            "x",
            "--output",
            "y",
            "--test-attempt",
            "socket",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2), "{out:?}");
}
