//! The host (and the compiler) confine themselves before they start a
//! thread or read a module (plan §6.10 layer 7). In order:
//!
//! 1. **Namespaces.** A new user namespace (the process's uid and gid map to
//!    65534 inside), a new network namespace (no interface configured, not
//!    even loopback), and new mount, IPC, and UTS namespaces.
//! 2. **A PID namespace.** `unshare(CLONE_NEWPID)` then `fork`: the child
//!    is PID 1 of the new namespace and carries on as the host; the parent
//!    (the process the worker started) only waits for it and exits with its
//!    status (a signal death is re-raised). The child dies with the parent
//!    (`PR_SET_PDEATHSIG`), so killing the pid the worker knows kills the
//!    host. No process outside the namespace can be named, so no signal
//!    route (`kill`, `F_SETOWN`, `FIOSETOWN`, …) can reach one.
//! 3. **A mount root.** An empty, read-only tmpfs becomes `/` (`pivot_root`,
//!    the old root detached): it holds only `read_dirs` (bound read-only,
//!    nosuid, nodev, at the same absolute paths), `read_files` (the
//!    compiler's input), and `write_dirs` (the compiler's output directory,
//!    nosuid, nodev, noexec). There is no `/proc`, `/sys`, or `/dev`: what
//!    the process needs from them is opened before (`before_root`).
//! 4. **Capabilities** are all dropped (after the mounts, which need them).
//! 5. **`no_new_privs`.**
//! 6. **Landlock.** Read-only access beneath `read_dirs`, read-write beneath
//!    `write_dirs`, nothing else; no TCP bind or connect where the ABI covers
//!    them; signals and abstract Unix sockets scoped to the process's own
//!    domain where the ABI has scopes (6+).
//! 7. **seccomp.** An allowlist with argument filters; anything else kills
//!    the process. `clone` is allowed only for threads, `clone3` fails with
//!    `ENOSYS` so libc falls back to `clone`; `fcntl`, `ioctl`, and `prctl`
//!    only for the commands the runtime uses (no `F_SETOWN`, `F_SETSIG`,
//!    `O_ASYNC`, `FIOSETOWN`, `FIOASYNC`, `SIOCSPGRP`, …).
//!
//! Each layer reports whether it applied. Under `CONFINE_ENFORCE` a layer
//! that did not apply is fatal (Landlock scopes only when the kernel's ABI
//! offers them); under `CONFINE_BEST_EFFORT` it becomes a note; `CONFINE_OFF`
//! applies nothing.
//!
//! Must run while the process is single-threaded: `unshare(CLONE_NEWUSER)`
//! refuses a multi-threaded process, `fork` copies only the calling thread,
//! and Landlock and seccomp apply to the calling thread and the threads it
//! starts afterwards.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use legion_proto::isolate_host::{Confine, Confinement};

/// Which seccomp allowlist to install.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Filter {
    /// The isolate host: threads, the tokio reactor, wasmtime.
    Host,
    /// The compiler: threads (Cranelift's parallel compilation), reading
    /// the input, writing and renaming the output.
    Compiler,
}

/// What to confine to.
#[derive(Debug, Clone, Default)]
pub struct Plan {
    /// Directories readable (beneath): bound read-only into the mount root
    /// at the same path, and Landlock read.
    pub read_dirs: Vec<PathBuf>,
    /// Single files readable (the compiler's input), bound the same way.
    pub read_files: Vec<PathBuf>,
    /// Directories writable (beneath): only the compiler's output dir.
    pub write_dirs: Vec<PathBuf>,
    /// Which seccomp allowlist, if any.
    pub seccomp: Option<Filter>,
    /// New user, network, mount, IPC, UTS, and PID namespaces and the
    /// mount root.
    pub namespaces: bool,
    /// Test only (`--features confinement-test`): layers to leave out so a
    /// test can show that each remaining layer stops an attack on its own.
    /// Names: `seccomp`, `pidns`, `scopes`, `mount`, `landlock`.
    pub skip: Vec<String>,
}

impl Plan {
    fn skips(&self, layer: &str) -> bool {
        self.skip.iter().any(|s| s == layer)
    }
}

/// The pid of the process the worker started, when the host runs as the
/// child of a PID-namespace fork (its own pid is then 1). `Hello` reports
/// this one: it is the pid the worker supervises.
static SUPERVISOR_PID: OnceLock<u32> = OnceLock::new();

/// The pid the worker knows this process by: the supervising parent's
/// after a PID-namespace fork, else our own.
pub fn worker_visible_pid() -> u32 {
    SUPERVISOR_PID
        .get()
        .copied()
        .unwrap_or_else(std::process::id)
}

/// Apply `plan` at `level`. `before_root` runs in the final process (after
/// the PID-namespace fork) while the node's filesystem is still visible:
/// open what the process needs later there. `Err` only under
/// `Confine::Enforce`, naming what did not apply.
pub fn confine(
    level: Confine,
    plan: &Plan,
    before_root: impl FnOnce(),
) -> Result<Confinement, String> {
    let mut c = Confinement::default();
    if level == Confine::Off {
        before_root();
        c.notes.push("confinement is off (development only)".into());
        return Ok(c);
    }
    let mut in_userns = false;
    if plan.namespaces {
        match namespaces() {
            Ok(()) => {
                in_userns = true;
                c.network_namespace = true;
            }
            Err(e) => c.notes.push(format!("namespaces: {e}")),
        }
    }
    if in_userns && !plan.skips("pidns") {
        match pid_namespace() {
            Ok(()) => c.pid_namespace = true,
            Err(e) => c.notes.push(format!("pid namespace: {e}")),
        }
    }
    before_root();
    if in_userns && !plan.skips("mount") {
        match mount_root(plan) {
            Ok(()) => c.mount_root = true,
            Err(e) => c.notes.push(format!("mount root: {e}")),
        }
    }
    if in_userns {
        match drop_capabilities() {
            Ok(()) => c.user_namespace = true,
            Err(e) => c.notes.push(format!("user namespace: {e}")),
        }
    }
    // SAFETY: prctl with integer arguments.
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } == 0 {
        c.no_new_privs = true;
    } else {
        c.notes
            .push(format!("no_new_privs: {}", std::io::Error::last_os_error()));
    }
    let mut scopes_offered = false;
    if !plan.skips("landlock") {
        match landlock(plan, !plan.skips("scopes")) {
            Ok(l) => {
                c.landlock = true;
                c.landlock_scopes = l.scoped;
                scopes_offered = l.scopes_offered;
                if !l.scoped && !l.scopes_offered {
                    c.notes.push(
                        "landlock scopes: the kernel's Landlock ABI has no signal or abstract \
                         socket scopes (needs ABI 6, Linux 6.12); the seccomp argument filters \
                         and the PID namespace close the same routes"
                            .into(),
                    );
                }
            }
            Err(e) => c.notes.push(format!("landlock: {e}")),
        }
    }
    if let Some(filter) = plan.seccomp.filter(|_| !plan.skips("seccomp")) {
        match seccomp(filter) {
            Ok(()) => c.seccomp = true,
            Err(e) => c.notes.push(format!("seccomp: {e}")),
        }
    }
    if level == Confine::Enforce {
        let missing: Vec<&str> = [
            (plan.namespaces && !c.user_namespace, "user namespace"),
            (plan.namespaces && !c.network_namespace, "network namespace"),
            (plan.namespaces && !c.pid_namespace, "pid namespace"),
            (plan.namespaces && !c.mount_root, "mount root"),
            (!c.no_new_privs, "no_new_privs"),
            (!c.landlock, "landlock"),
            (scopes_offered && !c.landlock_scopes, "landlock scopes"),
            (plan.seccomp.is_some() && !c.seccomp, "seccomp"),
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

fn os_err(what: &str) -> String {
    format!("{what}: {}", std::io::Error::last_os_error())
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
        return Err(os_err("unshare"));
    }
    let write =
        |path: &str, s: String| std::fs::write(path, s).map_err(|e| format!("writing {path}: {e}"));
    write("/proc/self/setgroups", "deny".into())?;
    write("/proc/self/uid_map", format!("65534 {uid} 1"))?;
    write("/proc/self/gid_map", format!("65534 {gid} 1"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// PID namespace
// ---------------------------------------------------------------------------

/// `unshare(CLONE_NEWPID)` and fork. Returns in the child (PID 1 of the new
/// namespace); the parent never returns from here.
fn pid_namespace() -> Result<(), String> {
    // SAFETY: getpid has no preconditions.
    let outer = unsafe { libc::getpid() } as u32;
    // SAFETY: unshare with a flag; single-threaded.
    if unsafe { libc::unshare(libc::CLONE_NEWPID) } != 0 {
        return Err(os_err("unshare"));
    }
    // The child learns that the parent died before it set its death
    // signal from the pipe: the parent holds the write end until it exits.
    let mut p = [0i32; 2];
    // SAFETY: valid two-int array.
    if unsafe { libc::pipe2(p.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(os_err("pipe2"));
    }
    let (r, w) = (p[0], p[1]);
    // SAFETY: single-threaded, so the child is a complete copy.
    match unsafe { libc::fork() } {
        -1 => Err(os_err("fork")),
        0 => {
            // SAFETY: plain syscalls on our own descriptors.
            unsafe {
                libc::close(w);
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0);
                let mut pfd = libc::pollfd {
                    fd: r,
                    events: libc::POLLIN,
                    revents: 0,
                };
                if libc::poll(&mut pfd, 1, 0) > 0 && pfd.revents & libc::POLLHUP != 0 {
                    libc::_exit(1);
                }
                libc::close(r);
            }
            let _ = SUPERVISOR_PID.set(outer);
            Ok(())
        }
        child => {
            // SAFETY: closing our copy of the read end.
            unsafe { libc::close(r) };
            supervise(child, w, outer)
        }
    }
}

/// The parent after the PID-namespace fork: hold nothing, confine itself
/// to waiting, and leave with the child's status.
fn supervise(child: libc::pid_t, keep: i32, me: u32) -> ! {
    close_fds_except(&[0, 1, 2, keep]);
    let mut notes = Vec::new();
    if let Err(e) = drop_capabilities() {
        notes.push(e);
    }
    // SAFETY: prctl with integer arguments.
    unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) };
    // No filesystem access at all. Signals are not scoped here: the kernel
    // checks the dying parent's right to send the child its death signal
    // against the parent's domain, and the child's is not nested in it.
    // seccomp leaves this process only `kill` of itself.
    if let Err(e) = landlock(&Plan::default(), false) {
        notes.push(format!("landlock: {e}"));
    }
    if let Err(e) = supervisor_seccomp(me) {
        notes.push(format!("seccomp: {e}"));
    }
    if !notes.is_empty() {
        let msg = format!(
            "legion-isolate: the supervising process could not confine itself: {}\n",
            notes.join("; ")
        );
        // SAFETY: writing a buffer we own to stderr.
        unsafe { libc::write(2, msg.as_ptr().cast(), msg.len()) };
    }
    let mut status = 0;
    loop {
        // SAFETY: valid status pointer.
        let r = unsafe { libc::waitpid(child, &mut status, 0) };
        if r == child {
            break;
        }
        if r < 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            // SAFETY: exiting.
            unsafe { libc::_exit(crate::run_host::EXIT_START as i32) };
        }
    }
    // SAFETY: plain syscalls; exiting the process either way.
    unsafe {
        if libc::WIFSIGNALED(status) {
            let sig = libc::WTERMSIG(status);
            libc::signal(sig, libc::SIG_DFL);
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, sig);
            libc::sigprocmask(libc::SIG_UNBLOCK, &set, std::ptr::null_mut());
            libc::kill(me as libc::pid_t, sig);
            libc::_exit(128 + sig);
        }
        libc::_exit(libc::WEXITSTATUS(status))
    }
}

// ---------------------------------------------------------------------------
// Descriptors
// ---------------------------------------------------------------------------

/// Close every descriptor not in `keep` (`close_range`, else one by one up
/// to the descriptor limit).
pub fn close_fds_except(keep: &[i32]) {
    let mut keep: Vec<u32> = keep
        .iter()
        .filter(|f| **f >= 0)
        .map(|f| *f as u32)
        .collect();
    keep.sort_unstable();
    keep.dedup();
    let mut lo: u32 = 0;
    for k in keep {
        if k > lo {
            close_range(lo, k - 1);
        }
        lo = k + 1;
    }
    close_range(lo, u32::MAX);
}

fn close_range(first: u32, last: u32) {
    // SAFETY: closes descriptors in a range; nothing in this process holds
    // them (called before any thread or library has opened one, or in the
    // supervising parent, which uses none).
    let r = unsafe { libc::syscall(libc::SYS_close_range, first, last, 0u32) };
    if r == 0 {
        return;
    }
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: valid pointer.
    unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) };
    let end = lim.rlim_cur.min(1 << 20).min(last as u64 + 1);
    for fd in first as u64..end {
        // SAFETY: as above.
        unsafe { libc::close(fd as i32) };
    }
}

// ---------------------------------------------------------------------------
// Mount root
// ---------------------------------------------------------------------------

/// Whether read-only artifact directories are mounted `noexec`. wasmtime
/// maps a `.cwasm` from its file and makes the text section executable
/// with `mprotect`, which a `noexec` mount refuses (`VM_MAYEXEC` is cleared
/// for mappings of its files: LoadModule then fails with "unable to make
/// memory executable: Permission denied", tried), so artifact directories
/// keep exec. They are read-only, and only the worker's compiler writes
/// them.
const READ_DIRS_NOEXEC: bool = false;

struct Bind {
    target: PathBuf,
    source: std::os::fd::OwnedFd,
    file: bool,
    writable: bool,
}

fn cstr(p: &Path) -> Result<std::ffi::CString, String> {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::CString::new(p.as_os_str().as_bytes())
        .map_err(|_| format!("{} contains a NUL", p.display()))
}

fn mount(
    src: Option<&Path>,
    target: &Path,
    fstype: Option<&str>,
    flags: libc::c_ulong,
    data: Option<&str>,
) -> Result<(), String> {
    let src_c = src.map(cstr).transpose()?;
    let target_c = cstr(target)?;
    let fstype_c = fstype.map(|s| std::ffi::CString::new(s).unwrap());
    let data_c = data.map(|s| std::ffi::CString::new(s).unwrap());
    // SAFETY: NUL-terminated strings or null pointers, as mount(2) takes.
    let r = unsafe {
        libc::mount(
            src_c.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
            target_c.as_ptr(),
            fstype_c.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
            flags,
            data_c
                .as_ref()
                .map_or(std::ptr::null(), |s| s.as_ptr().cast()),
        )
    };
    if r != 0 {
        return Err(os_err(&format!("mount {}", target.display())));
    }
    Ok(())
}

/// The flags a bind's source mount carries that a remount inside a user
/// namespace must keep (they are locked).
fn locked_flags(fd: i32) -> libc::c_ulong {
    // SAFETY: zeroed statvfs is a valid out-parameter.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: valid fd and pointer.
    if unsafe { libc::fstatvfs(fd, &mut st) } != 0 {
        return 0;
    }
    let f = st.f_flag;
    let mut out = 0;
    for (st_flag, ms) in [
        (libc::ST_RDONLY, libc::MS_RDONLY),
        (libc::ST_NOSUID, libc::MS_NOSUID),
        (libc::ST_NODEV, libc::MS_NODEV),
        (libc::ST_NOEXEC, libc::MS_NOEXEC),
        (libc::ST_NOATIME, libc::MS_NOATIME),
        (libc::ST_NODIRATIME, libc::MS_NODIRATIME),
        (libc::ST_RELATIME, libc::MS_RELATIME),
    ] {
        if f & st_flag != 0 {
            out |= ms;
        }
    }
    if f & (libc::ST_NOATIME | libc::ST_RELATIME) == 0 {
        out |= libc::MS_STRICTATIME;
    }
    out
}

/// Build an empty root holding only the plan's paths and pivot into it.
fn mount_root(plan: &Plan) -> Result<(), String> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    // Nothing we do here propagates back to the node.
    mount(
        None,
        Path::new("/"),
        None,
        libc::MS_REC | libc::MS_PRIVATE,
        None,
    )?;
    // Open every source before anything is covered.
    let mut binds = Vec::new();
    let mut open = |p: &Path, file: bool, writable: bool| -> Result<(), String> {
        let canon =
            std::fs::canonicalize(p).map_err(|e| format!("resolving {}: {e}", p.display()))?;
        let c = cstr(&canon)?;
        // SAFETY: NUL-terminated path; the fd is owned below.
        let fd = unsafe { libc::open(c.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
        if fd < 0 {
            return Err(os_err(&format!("opening {}", canon.display())));
        }
        // SAFETY: just opened.
        let source = unsafe { OwnedFd::from_raw_fd(fd) };
        binds.push(Bind {
            target: canon,
            source,
            file,
            writable,
        });
        Ok(())
    };
    for d in &plan.read_dirs {
        open(d, false, false)?;
    }
    for f in &plan.read_files {
        open(f, true, false)?;
    }
    for d in &plan.write_dirs {
        open(d, false, true)?;
    }
    // The new root: a small tmpfs over a directory every system has. Our
    // mount namespace is private, so covering it changes nothing outside.
    let base = ["/tmp", "/mnt", "/run", "/var/tmp", "/dev"]
        .into_iter()
        .map(Path::new)
        .find(|p| p.is_dir())
        .ok_or("no directory to build the root on")?;
    mount(
        Some(Path::new("tmpfs")),
        base,
        Some("tmpfs"),
        libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
        Some("mode=0755,size=64k,nr_inodes=4096"),
    )?;
    for b in &binds {
        let rel = b.target.strip_prefix("/").unwrap_or(&b.target);
        let at = base.join(rel);
        if b.file {
            if let Some(parent) = at.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("creating {}: {e}", parent.display()))?;
            }
            std::fs::write(&at, b"").map_err(|e| format!("creating {}: {e}", at.display()))?;
        } else {
            std::fs::create_dir_all(&at).map_err(|e| format!("creating {}: {e}", at.display()))?;
        }
        let src = PathBuf::from(format!("/proc/self/fd/{}", b.source.as_raw_fd()));
        mount(Some(&src), &at, None, libc::MS_BIND | libc::MS_REC, None)?;
        let mut flags = libc::MS_REMOUNT
            | libc::MS_BIND
            | libc::MS_NOSUID
            | libc::MS_NODEV
            | locked_flags(b.source.as_raw_fd());
        if !b.writable {
            flags |= libc::MS_RDONLY;
        }
        if b.writable || b.file || READ_DIRS_NOEXEC {
            flags |= libc::MS_NOEXEC;
        }
        mount(None, &at, None, flags, None)?;
    }
    drop(binds);
    // The root itself is read-only.
    mount(
        None,
        base,
        None,
        libc::MS_REMOUNT
            | libc::MS_BIND
            | libc::MS_RDONLY
            | libc::MS_NOSUID
            | libc::MS_NODEV
            | libc::MS_NOEXEC,
        None,
    )?;
    std::env::set_current_dir(base).map_err(|e| format!("chdir {}: {e}", base.display()))?;
    let dot = std::ffi::CString::new(".").unwrap();
    // SAFETY: two NUL-terminated paths. pivot_root(".", ".") stacks the old
    // root on the new one; the unmount below detaches it.
    if unsafe { libc::syscall(libc::SYS_pivot_root, dot.as_ptr(), dot.as_ptr()) } != 0 {
        return Err(os_err("pivot_root"));
    }
    // SAFETY: NUL-terminated path.
    if unsafe { libc::umount2(dot.as_ptr(), libc::MNT_DETACH) } != 0 {
        return Err(os_err("detaching the old root"));
    }
    std::env::set_current_dir("/").map_err(|e| format!("chdir /: {e}"))?;
    Ok(())
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
        return Err(os_err("capset"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Landlock
// ---------------------------------------------------------------------------

struct LandlockOutcome {
    /// Signal and abstract-socket scopes are in force.
    scoped: bool,
    /// The kernel's ABI has scopes (so not applying them is a failure).
    scopes_offered: bool,
}

#[cfg(target_os = "linux")]
fn landlock(plan: &Plan, scopes: bool) -> Result<LandlockOutcome, String> {
    use landlock::{
        path_beneath_rules, Access, AccessFs, AccessNet, CompatLevel, Compatible, LandlockStatus,
        Ruleset, RulesetAttr, RulesetCreatedAttr, RulesetStatus, Scope, ABI,
    };
    let abi = ABI::V6;
    let mut ruleset = Ruleset::default()
        .set_compatibility(CompatLevel::BestEffort)
        .handle_access(AccessFs::from_all(abi))
        .map_err(|e| e.to_string())?
        .handle_access(AccessNet::from_all(abi))
        .map_err(|e| e.to_string())?;
    if scopes {
        ruleset = ruleset
            .scope(Scope::from_all(abi))
            .map_err(|e| e.to_string())?;
    }
    let mut created = ruleset.create().map_err(|e| e.to_string())?;
    let readable: Vec<&PathBuf> = plan.read_dirs.iter().chain(&plan.read_files).collect();
    created = created
        .add_rules(path_beneath_rules(readable, AccessFs::from_read(abi)))
        .map_err(|e| e.to_string())?;
    created = created
        .add_rules(path_beneath_rules(
            &plan.write_dirs,
            AccessFs::from_all(abi),
        ))
        .map_err(|e| e.to_string())?;
    let status = created.restrict_self().map_err(|e| e.to_string())?;
    let scopes_offered = matches!(
        status.landlock,
        LandlockStatus::Available { effective_abi, .. } if effective_abi >= ABI::V6
    );
    match status.ruleset {
        RulesetStatus::FullyEnforced | RulesetStatus::PartiallyEnforced => Ok(LandlockOutcome {
            scoped: scopes && scopes_offered,
            scopes_offered,
        }),
        RulesetStatus::NotEnforced => Err(format!(
            "the kernel does not enforce Landlock ({:?})",
            status.landlock
        )),
    }
}

#[cfg(not(target_os = "linux"))]
fn landlock(_plan: &Plan, _scopes: bool) -> Result<LandlockOutcome, String> {
    Err("Landlock is Linux-only".into())
}

// ---------------------------------------------------------------------------
// seccomp
// ---------------------------------------------------------------------------

/// The syscalls the host makes after confinement (x86-64) that need no
/// argument filter, found by running the whole test suite (spawn tests,
/// HTTP, egress, checkpoints, the MPK layout, both language images through
/// the real binary) under `strace -ff` and keeping what the host's threads
/// called after the filter went on, plus the few a path the tests do not
/// reach needs (MPK's `pkey_*`, `mremap` for a large reallocation,
/// `sched_setaffinity` for pinned shards). `clone`, `socketpair`, `fcntl`,
/// `ioctl`, and `prctl` are allowed only with the arguments in
/// [`seccomp`]. Grouped by who needs them.
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
    ("epoll_create1", libc::SYS_epoll_create1),
    ("epoll_ctl", libc::SYS_epoll_ctl),
    ("epoll_wait", libc::SYS_epoll_wait),
    ("epoll_pwait", libc::SYS_epoll_pwait),
    ("epoll_pwait2", libc::SYS_epoll_pwait2),
    ("eventfd2", libc::SYS_eventfd2),
    // Reading artifacts (the mount root and Landlock decide which) and
    // /proc/self/statm (opened before confinement).
    ("openat", libc::SYS_openat),
    ("fstat", libc::SYS_fstat),
    ("newfstatat", libc::SYS_newfstatat),
    ("statx", libc::SYS_statx),
    ("lseek", libc::SYS_lseek),
    ("readlink", libc::SYS_readlink),
    // clone3 is answered ENOSYS by a second filter (glibc then uses clone).
    ("clone3", libc::SYS_clone3),
];

/// The compiler's allowlist (x86-64), found the same way from
/// `legion-isolate compile` runs over the test fixtures and both language
/// images: Cranelift's worker threads, reading the input, writing the
/// output to a temporary file, and renaming it into place (unlinking the
/// temporary file on failure). `exit`, `rt_sigreturn`, `clock_gettime`
/// (the vDSO's fallback), `writev`, and `fstat`/`newfstatat` are kept for
/// paths the runs did not take.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub const COMPILER_ALLOWED: &[(&str, i64)] = &[
    ("mmap", libc::SYS_mmap),
    ("munmap", libc::SYS_munmap),
    ("mremap", libc::SYS_mremap),
    ("mprotect", libc::SYS_mprotect),
    ("madvise", libc::SYS_madvise),
    ("brk", libc::SYS_brk),
    ("futex", libc::SYS_futex),
    ("set_robust_list", libc::SYS_set_robust_list),
    ("rseq", libc::SYS_rseq),
    ("sched_yield", libc::SYS_sched_yield),
    ("sched_getaffinity", libc::SYS_sched_getaffinity),
    ("gettid", libc::SYS_gettid),
    ("getpid", libc::SYS_getpid),
    ("exit", libc::SYS_exit),
    ("exit_group", libc::SYS_exit_group),
    ("rt_sigaction", libc::SYS_rt_sigaction),
    ("rt_sigprocmask", libc::SYS_rt_sigprocmask),
    ("rt_sigreturn", libc::SYS_rt_sigreturn),
    ("sigaltstack", libc::SYS_sigaltstack),
    ("clock_gettime", libc::SYS_clock_gettime),
    ("getrandom", libc::SYS_getrandom),
    ("read", libc::SYS_read),
    ("write", libc::SYS_write),
    ("writev", libc::SYS_writev),
    ("close", libc::SYS_close),
    ("openat", libc::SYS_openat),
    ("fstat", libc::SYS_fstat),
    ("newfstatat", libc::SYS_newfstatat),
    ("statx", libc::SYS_statx),
    ("fsync", libc::SYS_fsync),
    ("rename", libc::SYS_rename),
    ("unlink", libc::SYS_unlink),
    ("clone3", libc::SYS_clone3),
];

/// `fcntl` commands either process may use (`F_SETFL` separately: only
/// without `O_ASYNC`).
pub const FCNTL_ALLOWED: &[(&str, i32)] = &[
    ("F_GETFD", libc::F_GETFD),
    ("F_SETFD", libc::F_SETFD),
    ("F_GETFL", libc::F_GETFL),
    ("F_DUPFD_CLOEXEC", libc::F_DUPFD_CLOEXEC),
];

/// `ioctl` requests either process may use.
pub const IOCTL_ALLOWED: &[(&str, u64)] = &[("FIONBIO", libc::FIONBIO), ("FIOCLEX", libc::FIOCLEX)];

/// `prctl` options either process may use.
pub const PRCTL_ALLOWED: &[(&str, i32)] = &[("PR_SET_NAME", libc::PR_SET_NAME)];

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod filters {
    use super::*;
    use seccompiler::{
        apply_filter, BpfProgram, SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition,
        SeccompFilter, SeccompRule, TargetArch,
    };

    pub type Rules = BTreeMap<i64, Vec<SeccompRule>>;

    fn cond(
        arg: u8,
        len: SeccompCmpArgLen,
        op: SeccompCmpOp,
        v: u64,
    ) -> Result<SeccompCondition, String> {
        SeccompCondition::new(arg, len, op, v).map_err(|e| e.to_string())
    }

    fn rule(conds: Vec<SeccompCondition>) -> Result<SeccompRule, String> {
        SeccompRule::new(conds).map_err(|e| e.to_string())
    }

    fn eq(arg: u8, v: u64) -> Result<SeccompRule, String> {
        rule(vec![cond(
            arg,
            SeccompCmpArgLen::Dword,
            SeccompCmpOp::Eq,
            v,
        )?])
    }

    /// The argument-filtered calls both the host and the compiler make.
    pub fn filtered(rules: &mut Rules) -> Result<(), String> {
        // clone: threads only (CLONE_THREAD set), never a new process.
        rules.insert(
            libc::SYS_clone,
            vec![rule(vec![cond(
                0,
                SeccompCmpArgLen::Qword,
                SeccompCmpOp::MaskedEq(libc::CLONE_THREAD as u64),
                libc::CLONE_THREAD as u64,
            )?])?],
        );
        // fcntl: the listed commands, and F_SETFL without O_ASYNC (no
        // F_SETOWN*, F_SETSIG, F_SETLEASE, F_NOTIFY: no signal delivery to
        // a chosen process).
        let mut fcntl = FCNTL_ALLOWED
            .iter()
            .map(|(_, c)| eq(1, *c as u64))
            .collect::<Result<Vec<_>, _>>()?;
        fcntl.push(rule(vec![
            cond(
                1,
                SeccompCmpArgLen::Dword,
                SeccompCmpOp::Eq,
                libc::F_SETFL as u64,
            )?,
            cond(
                2,
                SeccompCmpArgLen::Dword,
                SeccompCmpOp::MaskedEq(libc::O_ASYNC as u64),
                0,
            )?,
        ])?);
        rules.insert(libc::SYS_fcntl, fcntl);
        // ioctl: the listed requests (no FIOSETOWN, SIOCSPGRP, FIOASYNC).
        rules.insert(
            libc::SYS_ioctl,
            IOCTL_ALLOWED
                .iter()
                .map(|(_, r)| eq(1, *r))
                .collect::<Result<Vec<_>, _>>()?,
        );
        rules.insert(
            libc::SYS_prctl,
            PRCTL_ALLOWED
                .iter()
                .map(|(_, o)| eq(0, *o as u64))
                .collect::<Result<Vec<_>, _>>()?,
        );
        Ok(())
    }

    pub fn host_rules() -> Result<Rules, String> {
        let mut rules: Rules = ALLOWED.iter().map(|(_, nr)| (*nr, vec![])).collect();
        filtered(&mut rules)?;
        // socketpair: Unix pairs only. tokio's signal driver makes one at
        // start (it is part of the I/O driver whenever tokio has signal
        // support compiled in); a connected pair reaches nothing.
        rules.insert(libc::SYS_socketpair, vec![eq(0, libc::AF_UNIX as u64)?]);
        Ok(rules)
    }

    pub fn compiler_rules() -> Result<Rules, String> {
        let mut rules: Rules = COMPILER_ALLOWED
            .iter()
            .map(|(_, nr)| (*nr, vec![]))
            .collect();
        filtered(&mut rules)?;
        Ok(rules)
    }

    /// The supervising parent: wait for the child, re-raise its signal on
    /// itself, exit.
    pub fn supervisor_rules(me: u32) -> Result<Rules, String> {
        let mut rules: Rules = [
            libc::SYS_wait4,
            libc::SYS_exit_group,
            libc::SYS_exit,
            libc::SYS_rt_sigaction,
            libc::SYS_rt_sigprocmask,
            libc::SYS_rt_sigreturn,
            libc::SYS_restart_syscall,
            libc::SYS_write,
        ]
        .into_iter()
        .map(|nr| (nr, vec![]))
        .collect();
        rules.insert(libc::SYS_kill, vec![eq(0, me as u64)?]);
        Ok(rules)
    }

    pub fn install(rules: Rules) -> Result<(), String> {
        // `LEGION_ISOLATE_SECCOMP=log` logs instead of killing (development:
        // find a missing syscall in the kernel log).
        let mismatch = match std::env::var("LEGION_ISOLATE_SECCOMP").as_deref() {
            Ok("log") => SeccompAction::Log,
            _ => SeccompAction::KillProcess,
        };
        let allow: BpfProgram =
            SeccompFilter::new(rules, mismatch, SeccompAction::Allow, TargetArch::x86_64)
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
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn seccomp(filter: Filter) -> Result<(), String> {
    filters::install(match filter {
        Filter::Host => filters::host_rules()?,
        Filter::Compiler => filters::compiler_rules()?,
    })
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn supervisor_seccomp(me: u32) -> Result<(), String> {
    filters::install(filters::supervisor_rules(me)?)
}

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
fn seccomp(_filter: Filter) -> Result<(), String> {
    Err("the seccomp allowlist is written for x86-64 Linux".into())
}

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
fn supervisor_seccomp(_me: u32) -> Result<(), String> {
    Err("the seccomp allowlist is written for x86-64 Linux".into())
}

/// Cap the address space (a pathological module cannot take the node's
/// memory with the compiler). Never raises a lower limit already set.
pub fn limit_address_space(bytes: u64) -> Result<(), String> {
    let mut cur = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: valid pointer.
    unsafe { libc::getrlimit(libc::RLIMIT_AS, &mut cur) };
    let bytes = bytes.min(cur.rlim_max);
    let lim = libc::rlimit {
        rlim_cur: bytes,
        rlim_max: bytes,
    };
    // SAFETY: valid rlimit pointer.
    if unsafe { libc::setrlimit(libc::RLIMIT_AS, &lim) } == 0 {
        Ok(())
    } else {
        Err(os_err("RLIMIT_AS"))
    }
}
