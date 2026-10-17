use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::path::{Path, PathBuf};

// Adds convenience for Result for brevity
pub type R<T> = anyhow::Result<T>;

pub fn random_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

/// Restrict `dir` so only its owner can enter it (`0o700`; no-op off Unix).
///
/// Worker-created allocation, task, and log directories use this as the
/// on-disk isolation boundary between workloads sharing a node: left to the
/// process umask (commonly producing `0755`) any local process — including
/// other tasks — could read staged artifacts, injected secrets, and captured
/// task output. Task runtimes that drop to a dedicated `user` hand the
/// allocation tree over via chown so the boundary holds for them too.
pub fn restrict_dir_to_owner(dir: &std::path::Path) -> crate::util::R<()> {
    set_dir_mode(dir, 0o700, "owner-only")
}

/// Let any user pass through `dir`, but only its owner list or change it
/// (`0o711`; no-op off Unix).
///
/// For the worker directories *above* the per-allocation ones (the worker
/// state dir and `allocs/`). A user-namespaced container's root (a shifted
/// host id) and an `exec` task's dedicated `user` both have to traverse them
/// to reach their own allocation directory, which keeps its own, tighter
/// guard. This is Docker's `userns-remap` layout (`/var/lib/docker` is
/// `0711`). Without listing, nothing below is discoverable, so everything
/// under such a directory must still guard itself: state files are `0600`,
/// and every other subdirectory is owner-only.
pub fn restrict_dir_to_traverse_only(dir: &std::path::Path) -> crate::util::R<()> {
    set_dir_mode(dir, 0o711, "traverse-only")
}

fn set_dir_mode(dir: &std::path::Path, mode: u32, what: &str) -> crate::util::R<()> {
    #[cfg(unix)]
    {
        use anyhow::Context;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode))
            .with_context(|| format!("failed to restrict {} to {what}", dir.display()))?;
    }
    #[cfg(not(unix))]
    let _ = (dir, mode, what);
    Ok(())
}

/// Write a secret to `path`, readable by its owner only from the moment it
/// exists (`0600` at creation; a pre-existing file is tightened before the
/// write), then fsync. Creating it at the umask and tightening afterwards
/// left a window in which another local user could open it and keep reading.
pub fn write_private_file(path: &Path, content: &[u8]) -> R<()> {
    use anyhow::Context;
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("failed to write {}", path.display()))?;
    #[cfg(unix)]
    {
        // `mode` applies only on creation; tighten a pre-existing file too.
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to chmod {}", path.display()))?;
    }
    file.write_all(content)
        .with_context(|| format!("failed to write {}", path.display()))?;
    file.sync_all().ok();
    Ok(())
}

/// `create_dir_all`, with any directory it creates owner-only (`0700`).
pub fn create_private_dir_all(dir: &Path) -> R<()> {
    use anyhow::Context;
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(dir)
        .with_context(|| format!("failed to create {}", dir.display()))
}

/// Write `content` to `root/relative` the way a reader should see it (temp
/// file at `mode`, fsync, rename), never following a symlink at any step.
///
/// For files the root worker writes into a directory a task can also write
/// (its task directory): templates, identity tokens, and KV files, which are
/// rewritten while the task runs. Each directory below `root` is opened with
/// `O_NOFOLLOW` and created when missing, the temp file is created with
/// `O_EXCL | O_NOFOLLOW`, and the rename stays inside the opened directory,
/// so whatever the task has put in its own directory, the write lands below
/// `root` or fails. `relative` must be relative and free of `..`. Only the
/// permission bits of `mode` apply: a root-owned setuid or setgid file in a
/// directory the task's user can reach would be a way to root.
pub fn write_file_beneath(root: &Path, relative: &Path, content: &[u8], mode: u32) -> R<()> {
    use anyhow::{bail, Context};
    use std::path::Component;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => parts.push(part),
            Component::CurDir => {}
            _ => bail!(
                "{} is not a plain relative path below {}",
                relative.display(),
                root.display()
            ),
        }
    }
    let Some(file_name) = parts.pop() else {
        bail!("{} names no file", relative.display());
    };
    #[cfg(unix)]
    {
        beneath::write(root, &parts, file_name, content, mode)
            .with_context(|| format!("write {} below {}", relative.display(), root.display()))
    }
    #[cfg(not(unix))]
    {
        let _ = (file_name, mode);
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, content).with_context(|| format!("write {}", path.display()))
    }
}

#[cfg(unix)]
mod beneath {
    use std::ffi::{CString, OsStr};
    use std::io::Write;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    fn cstr(name: &OsStr) -> std::io::Result<CString> {
        CString::new(name.as_bytes())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "NUL in path"))
    }

    fn open_dir_at(dir: libc::c_int, name: &CString) -> std::io::Result<OwnedFd> {
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
        // SAFETY: `name` is a valid C string and `dir` an open descriptor.
        let fd = unsafe { libc::openat(dir, name.as_ptr(), flags) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `fd` was just returned by openat and is owned here.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    /// The directory `name` in `dir`, made (`0755`) when missing, never
    /// followed when it is a link.
    fn open_or_make_dir_at(dir: &OwnedFd, name: &CString) -> std::io::Result<OwnedFd> {
        match open_dir_at(dir.as_raw_fd(), name) {
            Err(err) if err.raw_os_error() == Some(libc::ENOENT) => {
                // SAFETY: valid C string and descriptor.
                let made = unsafe { libc::mkdirat(dir.as_raw_fd(), name.as_ptr(), 0o755) };
                if made < 0 {
                    let err = std::io::Error::last_os_error();
                    if err.raw_os_error() != Some(libc::EEXIST) {
                        return Err(err);
                    }
                }
                open_dir_at(dir.as_raw_fd(), name)
            }
            other => other,
        }
    }

    pub(super) fn write(
        root: &Path,
        parents: &[&OsStr],
        file_name: &OsStr,
        content: &[u8],
        mode: u32,
    ) -> std::io::Result<()> {
        // `root` itself may sit in a directory the task owns (an `exec`
        // task's user owns its allocation directory), so its last component
        // is not followed either. It may not exist yet (a runtime's delivery
        // subdirectory, written before the task starts): then it is made in
        // its parent, which must.
        let mut dir = match open_dir_at(libc::AT_FDCWD, &cstr(root.as_os_str())?) {
            Err(err) if err.raw_os_error() == Some(libc::ENOENT) => {
                let (Some(parent), Some(name)) = (root.parent(), root.file_name()) else {
                    return Err(err);
                };
                let parent = open_dir_at(libc::AT_FDCWD, &cstr(parent.as_os_str())?)?;
                open_or_make_dir_at(&parent, &cstr(name)?)?
            }
            other => other?,
        };
        for part in parents {
            dir = open_or_make_dir_at(&dir, &cstr(part)?)?;
        }
        let tmp_name = cstr(OsStr::new(&format!(
            ".{}.legion-tmp-{}",
            file_name.to_string_lossy(),
            super::random_string(8)
        )))?;
        let final_name = cstr(file_name)?;
        let flags =
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC;
        // SAFETY: valid C string and descriptor; the mode is passed as the
        // variadic argument openat expects with O_CREAT.
        let fd = unsafe {
            libc::openat(
                dir.as_raw_fd(),
                tmp_name.as_ptr(),
                flags,
                (mode & 0o777) as libc::c_uint,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `fd` was just returned by openat and is owned here.
        let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
        let written = (|| {
            // The umask applied at create; set the exact mode before any
            // reader can see the file under its final name.
            // SAFETY: `file` holds an open descriptor.
            if unsafe { libc::fchmod(file.as_raw_fd(), (mode & 0o777) as libc::mode_t) } < 0 {
                return Err(std::io::Error::last_os_error());
            }
            file.write_all(content)?;
            file.sync_all()?;
            // SAFETY: valid C strings and descriptors; both names are in `dir`.
            let renamed = unsafe {
                libc::renameat(
                    dir.as_raw_fd(),
                    tmp_name.as_ptr(),
                    dir.as_raw_fd(),
                    final_name.as_ptr(),
                )
            };
            if renamed < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        })();
        if written.is_err() {
            // SAFETY: valid C string and descriptor.
            unsafe { libc::unlinkat(dir.as_raw_fd(), tmp_name.as_ptr(), 0) };
        }
        written
    }
}

/// First match for `name` on `PATH`. A name containing a path separator is
/// treated as an explicit path and only checked for existence.
///
/// The single lookup shared by everything that shells out to discovered
/// binaries (VMM monitors, `mkfs.*`, `mount`), so availability probes and the
/// actual invocation can never disagree about what was found.
pub fn find_on_path(name: &str) -> Option<PathBuf> {
    if name.contains(std::path::MAIN_SEPARATOR) {
        let path = PathBuf::from(name);
        return path.exists().then_some(path);
    }
    let path_var = std::env::var_os("PATH")?;
    std::env::split_paths(&path_var)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

/// Root of the dynamically loaded plugins (`<root>/<kind>/<name>/`, see
/// docs/plugin-system.md).
///
/// `LEGION_PLUGIN_DIR` when set. Otherwise the first of these that exists:
///
/// 1. `<dir of the legion binary>/plugins` — a bundle unpacked next to the
///    binary;
/// 2. `<dir of the legion binary>/../lib/legion/plugins` — the FHS layout of
///    an installed binary (`/usr/local/bin/legion` →
///    `/usr/local/lib/legion/plugins`);
/// 3. the source checkout the binary was built from — `cargo run` and
///    `cargo test`, whose binaries live under `target/`.
///
/// When none exists the first candidate is returned. The build checkout is
/// deliberately last and only used when it is really there: a binary built
/// in a container at `/src` and installed elsewhere used to look for
/// plugins under `/src/plugins` on every host it ran on.
pub fn plugin_root_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("LEGION_PLUGIN_DIR") {
        return PathBuf::from(dir);
    }
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf));
    default_plugin_root(exe_dir.as_deref(), Path::new(env!("CARGO_MANIFEST_DIR")))
}

/// [`plugin_root_dir`]'s search, without the environment: `exe_dir` is the
/// directory holding the running binary, `build_dir` the checkout it was
/// built from.
fn default_plugin_root(exe_dir: Option<&Path>, build_dir: &Path) -> PathBuf {
    let mut candidates = Vec::new();
    if let Some(exe_dir) = exe_dir {
        candidates.push(exe_dir.join("plugins"));
        candidates.push(
            exe_dir
                .join("..")
                .join("lib")
                .join("legion")
                .join("plugins"),
        );
    }
    candidates.push(build_dir.join("plugins"));
    candidates
        .iter()
        .find(|candidate| candidate.is_dir())
        .or_else(|| candidates.first())
        .cloned()
        .unwrap_or_else(|| build_dir.join("plugins"))
}

/// Wall-clock now, in unix milliseconds.
pub fn now_unix_ms() -> i64 {
    (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}

/// The bound on a runtime CLI call that only queries or nudges state
/// (`container inspect`, `rm --force`, `kill`, `system status`, `runc
/// delete`). These answer in well under a second when the runtime is healthy.
pub const RUNTIME_CLI_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// `cmd` as an operator would type it, for errors and logs.
pub fn describe_command(cmd: &std::process::Command) -> String {
    std::iter::once(cmd.get_program())
        .chain(cmd.get_args())
        .map(|part| part.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(" ")
}

/// [`std::process::Command::output`] with a deadline.
///
/// A runtime CLI can hang forever: Apple's `container rm --force` once sat
/// for 40 minutes on a container another `container run` owned, and every
/// caller waiting on it (a launch, a heartbeat's `container stats`, a
/// teardown under the task-manager lock) hung with it. Past `timeout` the
/// child's whole process group is killed and the error names the command, so
/// the caller fails with something an operator can act on.
///
/// stdin is closed. The child gets its own process group so a helper it
/// forked dies with it, rather than holding the output pipes open after the
/// kill. Output is read on helper threads, so a chatty child cannot fill a
/// pipe and deadlock against the wait.
pub fn output_with_timeout(
    cmd: &mut std::process::Command,
    timeout: std::time::Duration,
) -> R<std::process::Output> {
    use anyhow::Context;
    use std::io::Read;
    use std::process::Stdio;

    let described = describe_command(cmd);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to run `{described}`"))?;

    // Each reader sends its stream once it hits EOF. A channel rather than
    // a join, so the wait for them can be bounded too.
    let (tx, rx) = std::sync::mpsc::channel::<(bool, Vec<u8>)>();
    fn read_into<S: Read + Send + 'static>(
        stream: Option<S>,
        is_stdout: bool,
        tx: std::sync::mpsc::Sender<(bool, Vec<u8>)>,
    ) {
        std::thread::spawn(move || {
            let mut buf = Vec::new();
            if let Some(mut stream) = stream {
                let _ = stream.read_to_end(&mut buf);
            }
            let _ = tx.send((is_stdout, buf));
        });
    }
    read_into(child.stdout.take(), true, tx.clone());
    read_into(child.stderr.take(), false, tx);

    let deadline = std::time::Instant::now() + timeout;
    let timed_out = |child: &mut std::process::Child| {
        #[cfg(unix)]
        {
            // The group id is the child's pid (`process_group(0)`).
            // SAFETY: plain syscall on a pid we spawned and have not reaped.
            unsafe { libc::killpg(child.id() as libc::pid_t, libc::SIGKILL) };
        }
        let _ = child.kill();
        let _ = child.wait();
        anyhow::anyhow!(
            "`{described}` did not finish within {}s and was killed",
            timeout.as_secs()
        )
    };
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .with_context(|| format!("failed to wait on `{described}`"))?
        {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            return Err(timed_out(&mut child));
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };

    // The child exited, but something it spawned may still hold the pipes.
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    for _ in 0..2 {
        let remaining = deadline
            .saturating_duration_since(std::time::Instant::now())
            .max(std::time::Duration::from_millis(100));
        match rx.recv_timeout(remaining) {
            Ok((true, buf)) => stdout = buf,
            Ok((false, buf)) => stderr = buf,
            Err(_) => anyhow::bail!(
                "`{described}` exited ({status}) but its output stayed open past {}s",
                timeout.as_secs()
            ),
        }
    }
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes land below the root, creating directories on the way, and a
    /// link the task left in its directory is never followed: a linked
    /// directory fails the write, and a linked file name is replaced, not
    /// written through.
    #[cfg(unix)]
    #[test]
    fn write_file_beneath_never_follows_links_test() -> R<()> {
        use std::os::unix::fs::PermissionsExt;
        let scratch = tempfile::tempdir()?;
        let root = scratch.path().join("task");
        let outside = scratch.path().join("outside");
        std::fs::create_dir_all(&root)?;
        std::fs::create_dir_all(&outside)?;

        write_file_beneath(&root, Path::new("secrets/nested/token"), b"one", 0o600)?;
        let written = root.join("secrets/nested/token");
        assert_eq!(std::fs::read(&written)?, b"one");
        assert_eq!(
            std::fs::metadata(&written)?.permissions().mode() & 0o777,
            0o600
        );
        write_file_beneath(&root, Path::new("tool"), b"#!/bin/sh\n", 0o4755)?;
        assert_eq!(
            std::fs::metadata(root.join("tool"))?.permissions().mode() & 0o7777,
            0o755,
            "no setuid bit"
        );

        std::os::unix::fs::symlink(&outside, root.join("linked"))?;
        assert!(write_file_beneath(&root, Path::new("linked/token"), b"two", 0o600).is_err());
        assert!(!outside.join("token").exists());

        std::fs::write(outside.join("target"), b"untouched")?;
        std::os::unix::fs::symlink(outside.join("target"), root.join("file"))?;
        write_file_beneath(&root, Path::new("file"), b"three", 0o644)?;
        assert_eq!(std::fs::read(outside.join("target"))?, b"untouched");
        assert!(!std::fs::symlink_metadata(root.join("file"))?
            .file_type()
            .is_symlink());

        // A missing root is made inside its existing parent.
        write_file_beneath(&root.join("identity"), Path::new("token"), b"six", 0o600)?;
        assert_eq!(std::fs::read(root.join("identity/token"))?, b"six");

        let linked_root = scratch.path().join("linked-root");
        std::os::unix::fs::symlink(&outside, &linked_root)?;
        assert!(write_file_beneath(&linked_root, Path::new("token"), b"four", 0o600).is_err());
        assert!(write_file_beneath(&root, Path::new("../escape"), b"five", 0o600).is_err());
        Ok(())
    }

    #[test]
    fn plugin_root_prefers_a_bundle_next_to_the_binary_test() {
        let root = tempfile::tempdir().expect("temp dir");
        let bin = root.path().join("bin");
        let build = root.path().join("src");
        std::fs::create_dir_all(bin.join("plugins")).expect("bundle dir");
        std::fs::create_dir_all(build.join("plugins")).expect("checkout dir");
        assert_eq!(default_plugin_root(Some(&bin), &build), bin.join("plugins"));
    }

    #[test]
    fn plugin_root_finds_the_installed_lib_layout_test() {
        let root = tempfile::tempdir().expect("temp dir");
        let bin = root.path().join("usr/local/bin");
        let lib = root.path().join("usr/local/lib/legion/plugins");
        std::fs::create_dir_all(&bin).expect("bin dir");
        std::fs::create_dir_all(&lib).expect("lib dir");
        let found = default_plugin_root(Some(&bin), &root.path().join("nowhere"));
        assert_eq!(
            found.canonicalize().expect("exists"),
            lib.canonicalize().expect("exists")
        );
    }

    /// The finding this guards: a binary built at `/src` and installed on
    /// another host must not look for plugins in a checkout that is not
    /// there.
    #[test]
    fn plugin_root_ignores_a_missing_build_checkout_test() {
        let root = tempfile::tempdir().expect("temp dir");
        let bin = root.path().join("bin");
        std::fs::create_dir_all(&bin).expect("bin dir");
        let missing_checkout = root.path().join("src");
        assert_eq!(
            default_plugin_root(Some(&bin), &missing_checkout),
            bin.join("plugins")
        );
    }

    #[test]
    fn output_with_timeout_returns_output_like_command_output_test() {
        let output = output_with_timeout(
            std::process::Command::new("sh").args(["-c", "echo out; echo err >&2; exit 3"]),
            std::time::Duration::from_secs(10),
        )
        .expect("ran");
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(output.stdout, b"out\n");
        assert_eq!(output.stderr, b"err\n");
    }

    /// The hang this exists for: a CLI that never returns. The error names
    /// the command, and the child is gone when it returns.
    #[test]
    fn output_with_timeout_kills_a_hung_child_and_names_it_test() {
        let started = std::time::Instant::now();
        let err = output_with_timeout(
            std::process::Command::new("sleep").arg("30"),
            std::time::Duration::from_millis(200),
        )
        .unwrap_err()
        .to_string();
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        assert!(err.contains("`sleep 30`"), "{err}");
        assert!(err.contains("did not finish"), "{err}");
    }

    /// A child that forks a helper holding stdout open: killing the group
    /// takes the helper too, so the call still returns on time.
    #[test]
    fn output_with_timeout_kills_the_childs_helpers_test() {
        let started = std::time::Instant::now();
        let err = output_with_timeout(
            std::process::Command::new("sh").args(["-c", "sleep 30 & sleep 30"]),
            std::time::Duration::from_millis(200),
        )
        .unwrap_err();
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        assert!(err.to_string().contains("did not finish"), "{err}");
    }

    #[test]
    fn plugin_root_uses_the_checkout_for_cargo_builds_test() {
        let root = tempfile::tempdir().expect("temp dir");
        let bin = root.path().join("target/debug");
        let build = root.path().to_path_buf();
        std::fs::create_dir_all(&bin).expect("target dir");
        std::fs::create_dir_all(build.join("plugins")).expect("checkout dir");
        assert_eq!(
            default_plugin_root(Some(&bin), &build),
            build.join("plugins")
        );
    }
}

/// Runs an external helper once the way Legion's `exec` providers do (the
/// KV root key, the cluster CA): one JSON document on stdin, one JSON
/// document read from stdout, killed after `timeout`. A non-zero exit, a
/// timeout, or anything but JSON on stdout is an error naming `what` and the
/// program, with the helper's stderr.
pub fn run_json_command(
    what: &str,
    command: &[String],
    request: &serde_json::Value,
    timeout: std::time::Duration,
) -> R<serde_json::Value> {
    use anyhow::{anyhow, bail, Context};
    use std::io::{Read, Write};
    use std::process::{Command, Stdio};

    let program = command
        .first()
        .ok_or_else(|| anyhow!("the {what} has an empty command"))?;
    let mut child = Command::new(program)
        .args(&command[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed starting {what} `{program}`"))?;
    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("{what} stdin unavailable"))?;
        // A helper that exits without reading its request closes the pipe
        // first; its exit status below is the error worth reporting, not
        // the broken pipe.
        let written = stdin
            .write_all(request.to_string().as_bytes())
            .and_then(|()| stdin.write_all(b"\n"));
        match written {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => {}
            Err(err) => return Err(err.into()),
        }
    }
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("{what} stdout unavailable"))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("{what} stderr unavailable"))?;
    let out_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout.read_to_end(&mut buf);
        buf
    });
    let err_reader = std::thread::spawn(move || {
        let mut buf = String::new();
        let _ = stderr.read_to_string(&mut buf);
        buf
    });
    let deadline = std::time::Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!(
                "{what} `{program}` did not answer within {}",
                humantime::format_duration(timeout)
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    let stdout = out_reader.join().unwrap_or_default();
    let stderr = err_reader.join().unwrap_or_default();
    if !status.success() {
        bail!("{what} `{program}` exited with {status}: {}", stderr.trim());
    }
    let reply: serde_json::Value = serde_json::from_slice(&stdout).with_context(|| {
        format!(
            "{what} `{program}` printed something other than a JSON object: {}",
            String::from_utf8_lossy(&stdout).trim()
        )
    })?;
    Ok(reply)
}
