//! `legion-isolate host`: the startup order of plan §6.10 layer 7.
//!
//! 1. Read `HostConfig` from `--config-fd`; refuse another protocol version.
//! 2. Open what must be open before confinement (`/proc/self/statm`).
//! 3. Confine (single-threaded until here).
//! 4. Build the engine, the epoch ticker, the shards, and a small tokio
//!    runtime for the two sockets.
//! 5. Serve `IsolateHost` until the control socket closes (the worker is
//!    gone or stopped the host), then exit at once: a host never outlives
//!    its worker.

use std::io::Read;
use std::os::fd::FromRawFd;
use std::path::PathBuf;
use std::process::ExitCode;

use legion_proto::isolate_host::{Confine, HostConfig, PROTOCOL_VERSION};
use tracing::{error, info};

use crate::confine;
use crate::host::{Host, HostSetup};
use crate::service;
use crate::worker_api::GrpcWorker;

pub struct Args {
    pub control_fd: i32,
    pub worker_fd: i32,
    pub config_fd: i32,
    #[cfg(feature = "confinement-test")]
    pub test_attempt: Option<String>,
}

/// Exit statuses a worker can tell apart.
pub const EXIT_CONFIG: u8 = 2;
pub const EXIT_CONFINE: u8 = 3;
pub const EXIT_START: u8 = 4;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("LEGION_ISOLATE_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_ansi(false)
        .try_init();
}

pub fn read_config(fd: i32) -> Result<HostConfig, String> {
    // SAFETY: the worker passes an owned descriptor for this purpose.
    let mut f = unsafe { std::fs::File::from_raw_fd(fd) };
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)
        .map_err(|e| format!("reading the host configuration from fd {fd}: {e}"))?;
    let cfg: HostConfig = serde_json::from_slice(&buf)
        .map_err(|e| format!("the host configuration is not valid JSON: {e}"))?;
    if cfg.protocol_version != PROTOCOL_VERSION {
        return Err(format!(
            "the worker speaks isolate host protocol {}, this host speaks {}; install the \
             legion-isolate that ships with the worker",
            cfg.protocol_version, PROTOCOL_VERSION
        ));
    }
    Ok(cfg)
}

pub fn run(args: Args) -> ExitCode {
    init_tracing();
    let cfg = match read_config(args.config_fd) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("legion-isolate host: {e}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };
    let statm = std::fs::File::open("/proc/self/statm").ok();
    let level = Confine::try_from(cfg.confine).unwrap_or(Confine::Enforce);
    let plan = confine::Plan {
        read_dirs: cfg.read_dirs.iter().map(PathBuf::from).collect(),
        write_dirs: vec![],
        seccomp: true,
        namespaces: true,
    };
    let confinement = match confine::confine(level, &plan) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("legion-isolate host: {e}");
            return ExitCode::from(EXIT_CONFINE);
        }
    };
    #[cfg(feature = "confinement-test")]
    if let Some(what) = &args.test_attempt {
        return crate::run_host::attempt(what, &plan);
    }
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("isolate-io")
        .enable_io()
        .enable_time()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("legion-isolate host: starting the I/O runtime: {e}");
            return ExitCode::from(EXIT_START);
        }
    };
    let cordon = cfg.cordon.clone();
    let result: Result<(), String> = rt.block_on(async move {
        // SAFETY: the worker passed these descriptors for this purpose.
        let control = unsafe { service::inherited(args.control_fd) }
            .map_err(|e| format!("control socket: {e}"))?;
        let worker_sock = unsafe { service::inherited(args.worker_fd) }
            .map_err(|e| format!("worker socket: {e}"))?;
        let channel = service::channel_over(worker_sock)
            .await
            .map_err(|e| format!("worker socket: {e}"))?;
        let host = Host::new(HostSetup {
            config: cfg,
            worker: GrpcWorker::new(channel, tokio::runtime::Handle::current()),
            rt: tokio::runtime::Handle::current(),
            confinement,
            statm,
        })
        .map_err(|e| format!("{e:#}"))?;
        info!(cordon = %cordon, pid = std::process::id(), "isolate host serving");
        service::serve(host.clone(), control)
            .await
            .map_err(|e| format!("serving: {e}"))?;
        host.shutdown();
        Ok(())
    });
    match result {
        Ok(()) => {
            info!("control socket closed; exiting");
            // Do not wait for shard threads or in-flight guests.
            std::process::exit(0)
        }
        Err(e) => {
            error!(error = %e, "isolate host failed");
            eprintln!("legion-isolate host: {e}");
            std::process::exit(EXIT_START as i32)
        }
    }
}

/// Test hook: perform a forbidden action after confinement, print what
/// happened, and exit 0. A seccomp-forbidden action kills the process with
/// SIGSYS before it can print.
#[cfg(feature = "confinement-test")]
pub fn attempt(what: &str, plan: &confine::Plan) -> ExitCode {
    let report = |s: String| {
        println!("{s}");
        ExitCode::SUCCESS
    };
    match what {
        "socket" => {
            // SAFETY: plain syscall.
            let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
            report(format!("socket returned {fd}"))
        }
        "execve" => {
            let e = std::process::Command::new("/bin/true").status();
            report(format!("execve: {e:?}"))
        }
        "fork" => {
            // SAFETY: plain syscall; the child exits at once.
            let pid = unsafe { libc::fork() };
            if pid == 0 {
                unsafe { libc::_exit(0) };
            }
            report(format!("fork returned {pid}"))
        }
        "ptrace" => {
            // SAFETY: PTRACE_TRACEME has no pointer arguments.
            let r = unsafe { libc::ptrace(libc::PTRACE_TRACEME, 0, 0, 0) };
            report(format!("ptrace returned {r}"))
        }
        "open-outside" => match std::fs::File::open("/etc/hostname") {
            Ok(_) => report("open-outside: opened".into()),
            Err(e) => report(format!(
                "open-outside: {} ({:?})",
                e,
                e.raw_os_error()
            )),
        },
        // Reads `cmd.cwasm` in the first read dir (what LoadModule does).
        "read-inside" => {
            let dir = plan.read_dirs.first().cloned().unwrap_or_default();
            match std::fs::read(dir.join("cmd.cwasm")) {
                Ok(b) => report(format!("read-inside: {} bytes", b.len())),
                Err(e) => report(format!("read-inside: {e}")),
            }
        }
        "write-inside" => {
            let dir = plan.read_dirs.first().cloned().unwrap_or_default();
            match std::fs::write(dir.join("x"), b"x") {
                Ok(_) => report("write-inside: wrote".into()),
                Err(e) => report(format!("write-inside: {} ({:?})", e, e.raw_os_error())),
            }
        }
        "anon-exec" => {
            // Documented, not denied: seccomp cannot tell a file mapping from
            // an anonymous one in mprotect (plan §6.10 item 8).
            report("anon-exec: not tested (needs an LSM)".into())
        }
        other => report(format!("unknown attempt {other}")),
    }
}
