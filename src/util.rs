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
