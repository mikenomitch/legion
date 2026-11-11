//! The host confines itself before it starts a thread or loads a module
//! (plan §6.10 layer 7):
//!
//! 1. **Namespaces.** A new user namespace (the process's uid and gid map to
//!    65534 inside, and every capability is dropped), a new network
//!    namespace (no interface configured, not even loopback), and new mount,
//!    IPC, and UTS namespaces.
//! 2. **`no_new_privs`.**
//! 3. **Landlock.** Read-only access beneath `read_dirs`, nothing else on
//!    the filesystem; no TCP bind or connect where the ABI covers them.
//! 4. **seccomp.** An allowlist of what the host needs; anything else kills
//!    the process. `clone` is allowed only for threads, and `clone3` fails
//!    with `ENOSYS` so libc falls back to `clone`.
//!
//! Each layer reports whether it applied. Under `CONFINE_ENFORCE` a layer
//! that did not apply is fatal; under `CONFINE_BEST_EFFORT` it becomes a note
//! in `Hello`; `CONFINE_OFF` applies nothing.
//!
//! Must run while the process is single-threaded: `unshare(CLONE_NEWUSER)`
//! refuses a multi-threaded process, and Landlock and seccomp apply to the
//! calling thread and the threads it starts afterwards.

use std::collections::BTreeMap;
use std::path::PathBuf;

use legion_proto::isolate_host::{Confine, Confinement};

/// What to confine to.
#[derive(Debug, Clone, Default)]
pub struct Plan {
    /// Directories readable (beneath) after Landlock.
    pub read_dirs: Vec<PathBuf>,
    /// Directories writable (beneath): only the compiler's output dir.
    pub write_dirs: Vec<PathBuf>,
    /// Install the seccomp allowlist (the host); the compiler does not.
    pub seccomp: bool,
    /// New user, network, mount, IPC, and UTS namespaces.
    pub namespaces: bool,
}

/// Apply `plan` at `level`. `Err` only under `Confine::Enforce`, naming
/// what did not apply.
pub fn confine(level: Confine, plan: &Plan) -> Result<Confinement, String> {
    let mut c = Confinement::default();
    if level == Confine::Off {
        c.notes.push("confinement is off (development only)".into());
        return Ok(c);
    }
    if plan.namespaces {
        match namespaces() {
            Ok(()) => {
                c.user_namespace = true;
                c.network_namespace = true;
            }
            Err(e) => c.notes.push(format!("namespaces: {e}")),
        }
    }
    // SAFETY: prctl with integer arguments.
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } == 0 {
        c.no_new_privs = true;
    } else {
        c.notes.push(format!(
            "no_new_privs: {}",
            std::io::Error::last_os_error()
        ));
    }
    match landlock(plan) {
        Ok(()) => c.landlock = true,
        Err(e) => c.notes.push(format!("landlock: {e}")),
    }
    if plan.seccomp {
        match seccomp() {
            Ok(()) => c.seccomp = true,
            Err(e) => c.notes.push(format!("seccomp: {e}")),
        }
    }
    if level == Confine::Enforce {
        let missing: Vec<&str> = [
            (plan.namespaces && !c.user_namespace, "user namespace"),
            (plan.namespaces && !c.network_namespace, "network namespace"),
            (!c.no_new_privs, "no_new_privs"),
            (!c.landlock, "landlock"),
            (plan.seccomp && !c.seccomp, "seccomp"),
        ]
        .into_iter()
        .filter(|(m, _)| *m)
        .map(|(_, n)| n)
        .collect();
        if !missing.is_empty() {
            return Err(format!(
                "confinement is enforced but these layers did not apply: {} ({}); run on a \
                 kernel that offers them or set the cordon's confinement to best-effort",
                missing.join(", "),
                c.notes.join("; ")
            ));
        }
    }
    Ok(c)
}

fn namespaces() -> Result<(), String> {
    // SAFETY: getuid/getgid have no preconditions.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    let flags = libc::CLONE_NEWUSER
        | libc::CLONE_NEWNET
        | libc::CLONE_NEWNS
        | libc::CLONE_NEWIPC
        | libc::CLONE_NEWUTS;
    // SAFETY: unshare with flags; the process is single-threaded here.
    if unsafe { libc::unshare(flags) } != 0 {
        return Err(format!("unshare: {}", std::io::Error::last_os_error()));
    }
    let write = |path: &str, s: String| {
        std::fs::write(path, s).map_err(|e| format!("writing {path}: {e}"))
    };
    write("/proc/self/setgroups", "deny".into())?;
    write("/proc/self/uid_map", format!("65534 {uid} 1"))?;
    write("/proc/self/gid_map", format!("65534 {gid} 1"))?;
    drop_capabilities()
}

/// Drop every capability (the new user namespace grants all of them).
fn drop_capabilities() -> Result<(), String> {
    #[repr(C)]
    struct Header {
        version: u32,
        pid: i32,
    }
    #[repr(C)]
    #[derive(Default, Clone, Copy)]
    struct Data {
        effective: u32,
        permitted: u32,
        inheritable: u32,
    }
    const VERSION_3: u32 = 0x2008_0522;
    let mut h = Header {
        version: VERSION_3,
        pid: 0,
    };
    let d = [Data::default(); 2];
    for cap in 0..64 {
        // SAFETY: prctl with integer arguments; unknown caps fail harmlessly.
        unsafe { libc::prctl(libc::PR_CAPBSET_DROP, cap, 0, 0, 0) };
    }
    // SAFETY: valid header and two data structs, as capset v3 requires.
    let r = unsafe { libc::syscall(libc::SYS_capset, &mut h as *mut Header, d.as_ptr()) };
    if r != 0 {
        return Err(format!("capset: {}", std::io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn landlock(plan: &Plan) -> Result<(), String> {
    use landlock::{
        path_beneath_rules, Access, AccessFs, AccessNet, CompatLevel, Compatible, Ruleset,
        RulesetAttr, RulesetCreatedAttr, RulesetStatus, ABI,
    };
    let abi = ABI::V6;
    let mut ruleset = Ruleset::default()
        .set_compatibility(CompatLevel::BestEffort)
        .handle_access(AccessFs::from_all(abi))
        .map_err(|e| e.to_string())?
        .handle_access(AccessNet::from_all(abi))
        .map_err(|e| e.to_string())?
        .create()
        .map_err(|e| e.to_string())?;
    ruleset = ruleset
        .add_rules(path_beneath_rules(&plan.read_dirs, AccessFs::from_read(abi)))
        .map_err(|e| e.to_string())?;
    ruleset = ruleset
        .add_rules(path_beneath_rules(&plan.write_dirs, AccessFs::from_all(abi)))
        .map_err(|e| e.to_string())?;
    let status = ruleset.restrict_self().map_err(|e| e.to_string())?;
    match status.ruleset {
        RulesetStatus::FullyEnforced | RulesetStatus::PartiallyEnforced => Ok(()),
        RulesetStatus::NotEnforced => Err(format!(
            "the kernel does not enforce Landlock ({:?})",
            status.landlock
        )),
    }
}

#[cfg(not(target_os = "linux"))]
fn landlock(_plan: &Plan) -> Result<(), String> {
    Err("Landlock is Linux-only".into())
}

/// The syscalls the host makes after confinement, found by running the
/// host's tests under strace (x86-64). Grouped by who needs them.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub const ALLOWED: &[(&str, i64)] = &[
    // Memory: the allocator, wasmtime's pooling allocator and fiber stacks,
    // copy-on-write images, MPK.
    ("mmap", libc::SYS_mmap),
    ("munmap", libc::SYS_munmap),
    ("mremap", libc::SYS_mremap),
    ("mprotect", libc::SYS_mprotect),
    ("madvise", libc::SYS_madvise),
    ("brk", libc::SYS_brk),
    ("pkey_alloc", libc::SYS_pkey_alloc),
    ("pkey_free", libc::SYS_pkey_free),
    ("pkey_mprotect", libc::SYS_pkey_mprotect),
    // Threads and their synchronization.
    ("futex", libc::SYS_futex),
    ("set_robust_list", libc::SYS_set_robust_list),
    ("rseq", libc::SYS_rseq),
    ("sched_yield", libc::SYS_sched_yield),
    ("sched_getaffinity", libc::SYS_sched_getaffinity),
    ("sched_setaffinity", libc::SYS_sched_setaffinity),
    ("gettid", libc::SYS_gettid),
    ("getpid", libc::SYS_getpid),
    ("prctl", libc::SYS_prctl),
    ("exit", libc::SYS_exit),
    ("exit_group", libc::SYS_exit_group),
    // Signals: wasmtime turns guest faults into traps with signal handlers
    // on an alternate stack; tokio and std mask signals on threads.
    ("rt_sigaction", libc::SYS_rt_sigaction),
    ("rt_sigprocmask", libc::SYS_rt_sigprocmask),
    ("rt_sigreturn", libc::SYS_rt_sigreturn),
    ("sigaltstack", libc::SYS_sigaltstack),
    // Time and randomness.
    ("clock_gettime", libc::SYS_clock_gettime),
    ("clock_nanosleep", libc::SYS_clock_nanosleep),
    ("nanosleep", libc::SYS_nanosleep),
    ("getrandom", libc::SYS_getrandom),
    // I/O on inherited descriptors and the tokio reactor.
    ("read", libc::SYS_read),
    ("write", libc::SYS_write),
    ("readv", libc::SYS_readv),
    ("writev", libc::SYS_writev),
    ("pread64", libc::SYS_pread64),
    ("recvfrom", libc::SYS_recvfrom),
    ("sendto", libc::SYS_sendto),
    ("recvmsg", libc::SYS_recvmsg),
    ("sendmsg", libc::SYS_sendmsg),
    ("shutdown", libc::SYS_shutdown),
    ("close", libc::SYS_close),
    ("fcntl", libc::SYS_fcntl),
    ("ioctl", libc::SYS_ioctl),
    ("epoll_create1", libc::SYS_epoll_create1),
    ("epoll_ctl", libc::SYS_epoll_ctl),
    ("epoll_wait", libc::SYS_epoll_wait),
    ("epoll_pwait", libc::SYS_epoll_pwait),
    ("epoll_pwait2", libc::SYS_epoll_pwait2),
    ("eventfd2", libc::SYS_eventfd2),
    ("poll", libc::SYS_poll),
    ("ppoll", libc::SYS_ppoll),
    // Reading artifacts (Landlock decides which) and /proc/self/statm.
    ("openat", libc::SYS_openat),
    ("fstat", libc::SYS_fstat),
    ("newfstatat", libc::SYS_newfstatat),
    ("statx", libc::SYS_statx),
    ("lseek", libc::SYS_lseek),
    ("readlink", libc::SYS_readlink),
    ("getcwd", libc::SYS_getcwd),
    ("sysinfo", libc::SYS_sysinfo),
    ("uname", libc::SYS_uname),
    // clone3 is answered ENOSYS by a second filter (glibc then uses clone).
    ("clone3", libc::SYS_clone3),
];

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn seccomp() -> Result<(), String> {
    use seccompiler::{
        apply_filter, BpfProgram, SeccompAction, SeccompCmpArgLen, SeccompCmpOp,
        SeccompCondition, SeccompFilter, SeccompRule, TargetArch,
    };
    // `LEGION_ISOLATE_SECCOMP=log` logs instead of killing (development:
    // find a missing syscall in the kernel log).
    let mismatch = match std::env::var("LEGION_ISOLATE_SECCOMP").as_deref() {
        Ok("log") => SeccompAction::Log,
        _ => SeccompAction::KillProcess,
    };
    let mut rules: BTreeMap<i64, Vec<SeccompRule>> =
        ALLOWED.iter().map(|(_, nr)| (*nr, vec![])).collect();
    // clone: threads only (CLONE_THREAD set), never a new process.
    let thread = SeccompCondition::new(
        0,
        SeccompCmpArgLen::Qword,
        SeccompCmpOp::MaskedEq(libc::CLONE_THREAD as u64),
        libc::CLONE_THREAD as u64,
    )
    .map_err(|e| e.to_string())?;
    rules.insert(
        libc::SYS_clone,
        vec![SeccompRule::new(vec![thread]).map_err(|e| e.to_string())?],
    );
    // socketpair: Unix pairs only. tokio's signal driver makes one at
    // start (it is part of the I/O driver whenever tokio has signal
    // support compiled in); a connected pair reaches nothing.
    let unix = SeccompCondition::new(
        0,
        SeccompCmpArgLen::Dword,
        SeccompCmpOp::Eq,
        libc::AF_UNIX as u64,
    )
    .map_err(|e| e.to_string())?;
    rules.insert(
        libc::SYS_socketpair,
        vec![SeccompRule::new(vec![unix]).map_err(|e| e.to_string())?],
    );
    let allow: BpfProgram = SeccompFilter::new(
        rules,
        mismatch,
        SeccompAction::Allow,
        TargetArch::x86_64,
    )
    .and_then(|f| f.try_into())
    .map_err(|e| e.to_string())?;
    let clone3: BpfProgram = SeccompFilter::new(
        BTreeMap::from([(libc::SYS_clone3, vec![])]),
        SeccompAction::Allow,
        SeccompAction::Errno(libc::ENOSYS as u32),
        TargetArch::x86_64,
    )
    .and_then(|f| f.try_into())
    .map_err(|e| e.to_string())?;
    // Installed while single-threaded: every later thread inherits both.
    apply_filter(&clone3).map_err(|e| e.to_string())?;
    apply_filter(&allow).map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
fn seccomp() -> Result<(), String> {
    Err("the seccomp allowlist is written for x86-64 Linux".into())
}

/// Cap the compiler's address space (a pathological module cannot take the
/// node's memory with it).
pub fn limit_address_space(bytes: u64) -> Result<(), String> {
    let lim = libc::rlimit {
        rlim_cur: bytes,
        rlim_max: bytes,
    };
    // SAFETY: valid rlimit pointer.
    if unsafe { libc::setrlimit(libc::RLIMIT_AS, &lim) } == 0 {
        Ok(())
    } else {
        Err(format!("RLIMIT_AS: {}", std::io::Error::last_os_error()))
    }
}
