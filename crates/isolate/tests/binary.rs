//! The real `legion-isolate` binary: started with socketpairs as a worker
//! would, confined, driven over gRPC, with a fake `IsolateWorker` served on
//! the worker socket.

mod common;

use std::io::Write;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::net::UnixStream as StdUnix;
use std::os::unix::process::{CommandExt, ExitStatusExt};
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
        self.0.save_checkpoint(header, body).await.map(Response::new)
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
    cmd.args(["host", "--control-fd", "3", "--worker-fd", "4", "--config-fd", "5"])
        .args(extra)
        .env("LEGION_ISOLATE_LOG", "warn")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: dup2 into the child's fd 3..5 (clearing CLOEXEC) between fork
    // and exec; only async-signal-safe calls.
    unsafe {
        cmd.pre_exec(move || {
            for (i, fd) in raw.iter().enumerate() {
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
    let info = std::process::Command::new(BIN).arg("engine-info").output().unwrap();
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
    h.client.configure_task(spec("j", "cmd", "cmd")).await.unwrap();
    let inv = |task: &str, session: &str, args: &[&str]| InvokeRequest {
        job_id: "j".into(),
        task_name: task.into(),
        invocation_id: next_id(),
        session_id: session.into(),
        call: Some(invoke_request::Call::Run(run(args))),
        ..Default::default()
    };
    let r = h.client.invoke(inv("cmd", "", &["echo", "a"])).await.unwrap().into_inner();
    assert_eq!(exit(&r), Some(0), "{:?}", r);
    assert!(stdout(&r).contains("args=echo,a"));
    // Limits work the same in the confined process.
    let mut s = spec("j", "spin", "cmd");
    s.limits.as_mut().unwrap().cpu_micros = 50_000;
    h.client.configure_task(s).await.unwrap();
    let r = h.client.invoke(inv("spin", "", &["spin"])).await.unwrap().into_inner();
    assert_eq!(stopped(&r).1, "cpu");
    let r = h.client.invoke(inv("cmd", "", &["trap"])).await.unwrap().into_inner();
    assert_eq!(stopped(&r).0, "trap");
    let r = h.client.invoke(inv("cmd", "", &["sleep", "20"])).await.unwrap().into_inner();
    assert_eq!(stdout(&r), "slept\n");

    // Egress over the worker socket.
    let r = h
        .client
        .invoke(inv("cmd", "", &["fetch", "http://up/x"]))
        .await
        .unwrap()
        .into_inner();
    assert!(stdout(&r).contains("body=hello from http://up/x"), "{}", stdout(&r));
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
    h.client.invoke(inv("sess", "s1", &["set", "k", "v"])).await.unwrap();
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
    let r = h.client.invoke(inv("sess", "s1", &["get", "k"])).await.unwrap().into_inner();
    assert_eq!(outcome(&r).result, "\"v\"");
    assert_eq!(r.start, "hibernated");
    let st = h.client.stats(StatsRequest {}).await.unwrap().into_inner();
    assert!(st.rss_bytes > 0, "statm was opened before confinement");
    assert!(h.child.try_wait().unwrap().is_none(), "{}", stderr_of(&mut h.child));
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
        client.configure_task(spec("j", "cmd", "cmd")).await.unwrap();
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
            assert!(c.user_namespace && c.network_namespace && c.no_new_privs && c.landlock && c.seccomp);
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
/// (exit status, stdout).
#[cfg(feature = "confinement-test")]
fn attempt(what: &str) -> (std::process::ExitStatus, String, String) {
    let cfg = bin_config(Confine::BestEffort);
    let (mut cmd, _c, _w, _ends) = command(&cfg, &["--test-attempt", what]);
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
        ] {
            if !on {
                eprintln!("LAYER UNAVAILABLE HERE: {name}");
                assert!(
                    c.notes.iter().any(|n| n.to_lowercase().contains(&name[..4].to_lowercase())),
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
#[cfg_attr(not(feature = "confinement-test"), ignore = "needs --features confinement-test")]
fn seccomp_kills_a_socket_call_test() {
    #[cfg(feature = "confinement-test")]
    assert_killed_by_seccomp("socket");
}

#[test]
#[cfg_attr(not(feature = "confinement-test"), ignore = "needs --features confinement-test")]
fn seccomp_kills_execve_and_fork_test() {
    #[cfg(feature = "confinement-test")]
    {
        assert_killed_by_seccomp("execve");
        assert_killed_by_seccomp("fork");
    }
}

#[test]
#[cfg_attr(not(feature = "confinement-test"), ignore = "needs --features confinement-test")]
fn seccomp_kills_ptrace_test() {
    #[cfg(feature = "confinement-test")]
    assert_killed_by_seccomp("ptrace");
}

#[test]
#[cfg_attr(not(feature = "confinement-test"), ignore = "needs --features confinement-test")]
fn landlock_denies_files_outside_read_dirs_test() {
    #[cfg(feature = "confinement-test")]
    {
        let c = hello_confinement();
        let (status, out, _) = attempt("open-outside");
        assert!(status.success());
        if !c.landlock {
            eprintln!("LAYER UNAVAILABLE HERE: landlock ({out})");
            return;
        }
        assert!(out.contains(&format!("Some({})", libc::EACCES)), "{out}");
        artifact("cmd");
        let (_, out, _) = attempt("read-inside");
        assert!(out.contains("bytes"), "{out}");
        let (_, out, _) = attempt("write-inside");
        assert!(out.contains(&format!("Some({})", libc::EACCES)), "{out}");
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
