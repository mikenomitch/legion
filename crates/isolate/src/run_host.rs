//! `legion-isolate host`: the startup order of plan §6.10 layer 7.
//!
//! 1. Close every inherited descriptor but stdio and the three passed ones
//!    (a worker descriptor leaked without `CLOEXEC` never reaches a
//!    multi-tenant process).
//! 2. Read `HostConfig` from `--config-fd`; refuse another protocol version.
//! 3. Confine (single-threaded until here; see `confine`): namespaces, the
//!    PID-namespace fork, then, in the process that carries on as the host,
//!    open what must be open before the mount root (`/proc/self/statm`),
//!    and the rest of the layers.
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
    /// Test only: layers to leave out (`confine::Plan::skip`).
    #[cfg(feature = "confinement-test")]
    pub test_skip: Vec<String>,
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
    confine::close_fds_except(&[0, 1, 2, args.control_fd, args.worker_fd, args.config_fd]);
    init_tracing();
    let cfg = match read_config(args.config_fd) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("legion-isolate host: {e}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };
    let level = Confine::try_from(cfg.confine).unwrap_or(Confine::Enforce);
    #[allow(unused_mut)]
    let mut plan = confine::Plan {
        read_dirs: cfg.read_dirs.iter().map(PathBuf::from).collect(),
        seccomp: Some(confine::Filter::Host),
        namespaces: true,
        ..Default::default()
    };
    #[cfg(feature = "confinement-test")]
    {
        plan.skip = args.test_skip.clone();
    }
    // Opened in the process that serves (after the PID-namespace fork) and
    // before the mount root hides /proc.
    let mut statm = None;
    let confinement = match confine::confine(level, &plan, || {
        statm = std::fs::File::open("/proc/self/statm").ok();
    }) {
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
        info!(cordon = %cordon, pid = confine::worker_visible_pid(), "isolate host serving");
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
/// SIGSYS before it can print. Shared by `host` and `compile`. An action
/// may take an argument after `:` (a victim pid for the signal routes).
#[cfg(feature = "confinement-test")]
pub fn attempt(what: &str, plan: &confine::Plan) -> ExitCode {
    use std::io::Write;
    let report = |s: String| {
        println!("{s}");
        let _ = std::io::stdout().flush();
        ExitCode::SUCCESS
    };
    let errno = || std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    // Not in the libc crate (Linux UAPI values).
    const F_SETSIG: i32 = 10;
    const F_SETOWN_EX: i32 = 15;
    const F_OWNER_PID: i32 = 1;
    const FIOSETOWN: u64 = 0x8901;
    const SIOCSPGRP: u64 = 0x8902;
    #[repr(C)]
    struct FOwnerEx {
        kind: i32,
        pid: i32,
    }
    let (what, arg) = what.split_once(':').unwrap_or((what, ""));
    let victim: libc::pid_t = arg.parse().unwrap_or(0);
    let first_read = || {
        plan.read_dirs
            .first()
            .cloned()
            .or_else(|| plan.read_files.first().cloned())
            .unwrap_or_default()
    };
    // A connected pair whose first end signals `victim` when the second is
    // written to, set up through `setup`; reports each step's result.
    let sigio = |setup: &dyn Fn(i32) -> Vec<(&'static str, i32, i32)>| {
        let mut sv = [0i32; 2];
        // SAFETY: valid two-int array.
        if unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, sv.as_mut_ptr()) } != 0 {
            return report(format!("socketpair failed errno {}", errno()));
        }
        let steps = setup(sv[0]);
        // SAFETY: writing one byte from a local buffer.
        let w = unsafe { libc::write(sv[1], b"x".as_ptr().cast(), 1) };
        std::thread::sleep(std::time::Duration::from_millis(200));
        let steps: Vec<String> = steps
            .iter()
            .map(|(n, r, e)| format!("{n}={r}/{e}"))
            .collect();
        report(format!("sigio: {} write={w}", steps.join(" ")))
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
        "kill" => {
            // SAFETY: plain syscall.
            let r = unsafe { libc::kill(victim, libc::SIGKILL) };
            report(format!("kill returned {r} errno {}", errno()))
        }
        "pid" => report(format!("pid {}", std::process::id())),
        "open-outside" => match std::fs::File::open("/etc/hostname") {
            Ok(_) => report("open-outside: opened".into()),
            Err(e) => report(format!("open-outside: {} ({:?})", e, e.raw_os_error())),
        },
        "open-proc" => match std::fs::File::open("/proc/1/status") {
            Ok(_) => report("open-proc: opened".into()),
            Err(e) => report(format!("open-proc: {} ({:?})", e, e.raw_os_error())),
        },
        "stat-outside" => match std::fs::metadata("/etc/hostname") {
            Ok(m) => report(format!("stat-outside: {} bytes", m.len())),
            Err(e) => report(format!("stat-outside: {} ({:?})", e, e.raw_os_error())),
        },
        // A regular file that exists answers EINVAL; a hidden one ENOENT.
        "readlink-outside" => match std::fs::read_link("/etc/hostname") {
            Ok(t) => report(format!("readlink-outside: {}", t.display())),
            Err(e) => report(format!("readlink-outside: {} ({:?})", e, e.raw_os_error())),
        },
        // Reads `cmd.cwasm` in the first read dir (what LoadModule does), or
        // the compiler's input file.
        "read-inside" => {
            let p = first_read();
            let p = if p.is_dir() { p.join("cmd.cwasm") } else { p };
            match std::fs::read(&p) {
                Ok(b) => report(format!("read-inside: {} bytes", b.len())),
                Err(e) => report(format!("read-inside: {e}")),
            }
        }
        "write-inside" => {
            let p = first_read();
            let p = if p.is_dir() { p.join("x") } else { p };
            match std::fs::write(p, b"x") {
                Ok(_) => report("write-inside: wrote".into()),
                Err(e) => report(format!("write-inside: {} ({:?})", e, e.raw_os_error())),
            }
        }
        "write-output" => {
            let dir = plan.write_dirs.first().cloned().unwrap_or_default();
            match std::fs::write(dir.join("probe"), b"x") {
                Ok(_) => {
                    let _ = std::fs::remove_file(dir.join("probe"));
                    report("write-output: wrote".into())
                }
                Err(e) => report(format!("write-output: {} ({:?})", e, e.raw_os_error())),
            }
        }
        // The descriptors open in the confined process.
        "fds" => {
            let open: Vec<String> = (0..1024)
                // SAFETY: F_GETFD on a number; EBADF when it is not open.
                .filter(|fd| unsafe { libc::fcntl(*fd, libc::F_GETFD) } >= 0)
                .map(|fd| fd.to_string())
                .collect();
            report(format!("fds: {}", open.join(",")))
        }
        "anon-exec" => {
            // Documented, not denied: seccomp cannot tell a file mapping from
            // an anonymous one in mprotect (plan §6.10 item 8).
            report("anon-exec: not tested (needs an LSM)".into())
        }
        // SIGIO routes (review finding 2).
        "fsetfl-nonblock" => {
            let mut sv = [0i32; 2];
            // SAFETY: plain syscalls on our own descriptors.
            let r = unsafe {
                libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, sv.as_mut_ptr());
                libc::fcntl(sv[0], libc::F_SETFL, libc::O_NONBLOCK)
            };
            report(format!("fsetfl-nonblock returned {r}"))
        }
        "fsetfl-async" => {
            let mut sv = [0i32; 2];
            // SAFETY: plain syscalls on our own descriptors.
            let r = unsafe {
                libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, sv.as_mut_ptr());
                libc::fcntl(sv[0], libc::F_SETFL, libc::O_ASYNC)
            };
            report(format!("fsetfl-async returned {r}"))
        }
        "sigio-fcntl" => sigio(&|fd| {
            // SAFETY: fcntl on our own socket.
            unsafe {
                let a = libc::fcntl(fd, libc::F_SETOWN, victim);
                let ea = errno();
                let b = libc::fcntl(fd, F_SETSIG, libc::SIGKILL);
                let eb = errno();
                let c = libc::fcntl(fd, libc::F_SETFL, libc::O_ASYNC);
                vec![("setown", a, ea), ("setsig", b, eb), ("async", c, errno())]
            }
        }),
        "sigio-setown-ex" => sigio(&|fd| {
            let owner = FOwnerEx {
                kind: F_OWNER_PID,
                pid: victim,
            };
            // SAFETY: fcntl on our own socket with a valid struct.
            unsafe {
                let a = libc::fcntl(fd, F_SETOWN_EX, &owner);
                let ea = errno();
                let c = libc::fcntl(fd, libc::F_SETFL, libc::O_ASYNC);
                vec![("setown_ex", a, ea), ("async", c, errno())]
            }
        }),
        "sigio-ioctl" => sigio(&|fd| {
            let on: libc::c_int = 1;
            // SAFETY: ioctl on our own socket with valid int pointers.
            unsafe {
                let a = libc::ioctl(fd, FIOSETOWN, &victim);
                let ea = errno();
                let c = libc::ioctl(fd, libc::FIOASYNC, &on);
                vec![("fiosetown", a, ea), ("fioasync", c, errno())]
            }
        }),
        "sigio-pgrp" => sigio(&|fd| {
            let on: libc::c_int = 1;
            let pgrp = victim;
            // SAFETY: ioctl on our own socket with valid int pointers.
            unsafe {
                let a = libc::ioctl(fd, SIOCSPGRP, &pgrp);
                let ea = errno();
                let c = libc::ioctl(fd, libc::FIOASYNC, &on);
                vec![("siocspgrp", a, ea), ("fioasync", c, errno())]
            }
        }),
        other => report(format!("unknown attempt {other}")),
    }
}
