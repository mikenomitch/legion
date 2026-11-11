//! Scratch space for unit tests (in this crate and the ones above it)
//! that must outlive a `TempDir` guard. Test support: nothing outside a
//! test calls it.
//!
//! A store helper hands back a `Store` whose files live in a directory the
//! helper created, so it calls `TempDir::keep()` and the directory is never
//! removed. With hundreds of such stores per run (~1.7 MB each), every
//! `cargo test` left over a gigabyte in the system temp dir. Kept directories
//! go under one per-process root instead, and the root is removed when the
//! test process exits.
//!
//! A run killed before exit (Ctrl-C, a timeout, an aborting test) skips
//! that removal and leaves its root behind. So every root is named for the
//! process that owns it, `legion-tests-<pid>-<random>`, and creating a root
//! first sweeps the ones whose process is gone ([`create_root`]). The
//! integration tests' root (`tests/integration/support.rs`) is made the same
//! way, so either kind of test run cleans up after both.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

static ROOT: OnceLock<PathBuf> = OnceLock::new();

/// The prefix every scratch root's name starts with.
pub const ROOT_PREFIX: &str = "legion-tests-";

/// How long a dead process's root must have gone untouched before a sweep
/// removes it. The pid check is only meaningful inside one pid namespace; a
/// container sharing this temp dir sees other pids, so a root must also have
/// gone quiet (a live run creates directories in it constantly).
const DEAD_OWNER_GRACE: Duration = Duration::from_secs(3600);

/// A root named before roots carried their owner's pid. Nothing can say
/// whether its run is still going, so only age removes it.
const UNOWNED_GRACE: Duration = Duration::from_secs(24 * 3600);

/// The per-process scratch root: create kept directories inside it with
/// `tempfile::Builder::tempdir_in` or `tempfile::tempdir_in`.
pub fn root() -> &'static Path {
    ROOT.get_or_init(|| {
        let root = create_root();
        // SAFETY: registers a plain `extern "C"` function with no captured
        // state; it only touches the already-initialized `ROOT`.
        unsafe {
            libc::atexit(remove_root);
        }
        root
    })
}

/// Sweep stale roots from the system temp dir, then create and keep this
/// process's root there. The caller removes it at exit.
pub fn create_root() -> PathBuf {
    let temp = std::env::temp_dir();
    sweep_stale_roots(&temp, SystemTime::now());
    tempfile::Builder::new()
        .prefix(&format!("{ROOT_PREFIX}{}-", std::process::id()))
        .tempdir_in(&temp)
        .expect("create the test scratch root")
        .keep()
}

/// Remove every scratch root in `dir` left by a test process that is gone.
/// Best effort: a root that cannot be read or removed is skipped.
pub fn sweep_stale_roots(dir: &Path, now: SystemTime) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        // `symlink_metadata`: never follow a link out of the temp dir.
        let Ok(meta) = std::fs::symlink_metadata(entry.path()) else {
            continue;
        };
        if !meta.is_dir() {
            continue;
        }
        let idle = meta
            .modified()
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .unwrap_or_default();
        if is_stale(name, idle, process_is_alive) {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

/// Whether the root named `name`, untouched for `idle`, belongs to no
/// running test process.
fn is_stale(name: &str, idle: Duration, alive: impl Fn(u32) -> bool) -> bool {
    let Some(rest) = name.strip_prefix(ROOT_PREFIX) else {
        return false;
    };
    match owner_pid(rest) {
        Some(pid) if pid == std::process::id() => false,
        Some(pid) => !alive(pid) && idle >= DEAD_OWNER_GRACE,
        None => idle >= UNOWNED_GRACE,
    }
}

/// The pid in `<pid>-<random>`, the part of a root's name after the prefix.
fn owner_pid(rest: &str) -> Option<u32> {
    let (pid, random) = rest.split_once('-')?;
    if random.is_empty() || pid.is_empty() || !pid.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    pid.parse().ok().filter(|pid| *pid > 0)
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return true;
    };
    // SAFETY: signal 0 only checks that the process exists and may be
    // signalled; nothing is delivered.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    // EPERM: it exists, under another user.
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(not(unix))]
fn process_is_alive(_pid: u32) -> bool {
    true
}

extern "C" fn remove_root() {
    if let Some(root) = ROOT.get() {
        // Best effort: a background thread still writing into a store can
        // race the removal, and the process is exiting either way.
        let _ = std::fs::remove_dir_all(root);
    }
}

#[cfg(test)]
mod test_scratch_test {
    use super::*;

    const HOUR: Duration = Duration::from_secs(3600);

    #[test]
    fn a_dead_owners_quiet_root_is_stale_test() {
        let dead = |_| false;
        assert!(is_stale("legion-tests-4242-aB3xYz", 2 * HOUR, dead));
        // Too recent: maybe a live run in another pid namespace.
        assert!(!is_stale("legion-tests-4242-aB3xYz", HOUR / 2, dead));
    }

    #[test]
    fn a_live_owners_root_is_kept_test() {
        let alive = |_| true;
        assert!(!is_stale("legion-tests-4242-aB3xYz", 48 * HOUR, alive));
        let own = format!("legion-tests-{}-aB3xYz", std::process::id());
        assert!(!is_stale(&own, 48 * HOUR, |_| false));
    }

    #[test]
    fn an_unowned_root_goes_only_by_age_test() {
        let dead = |_| false;
        // The pre-pid name: `legion-tests-<random>`.
        assert!(!is_stale("legion-tests-aB3xYz", 2 * HOUR, dead));
        assert!(is_stale("legion-tests-aB3xYz", 25 * HOUR, dead));
        assert!(is_stale("legion-tests-123456", 25 * HOUR, dead));
        assert!(!is_stale("legion-tests-123456", 2 * HOUR, dead));
    }

    #[test]
    fn other_names_are_never_stale_test() {
        let dead = |_| false;
        assert!(!is_stale(".tmpAbC123", 48 * HOUR, dead));
        assert!(!is_stale("legion-net-plugin-store-x", 48 * HOUR, dead));
        // Pid 0 names no owner, so only age decides.
        assert!(!is_stale("legion-tests-0-abc", 2 * HOUR, |_| true));
    }

    #[cfg(unix)]
    #[test]
    fn this_process_is_alive_and_a_reaped_child_is_not_test() -> std::io::Result<()> {
        assert!(process_is_alive(std::process::id()));
        let mut child = std::process::Command::new("true").spawn()?;
        let pid = child.id();
        child.wait()?;
        assert!(!process_is_alive(pid));
        Ok(())
    }

    #[test]
    fn a_sweep_removes_only_stale_roots_test() -> std::io::Result<()> {
        let dir = tempfile::tempdir_in(root())?;
        let stale = dir.path().join("legion-tests-unowned");
        let fresh = dir
            .path()
            .join(format!("legion-tests-{}-mine", std::process::id()));
        let other = dir.path().join(".tmpOther");
        for path in [&stale, &fresh, &other] {
            std::fs::create_dir(path)?;
            std::fs::write(path.join("store.db"), b"x")?;
        }
        // Two days on, the unowned root is stale; this process's is not.
        sweep_stale_roots(dir.path(), SystemTime::now() + 48 * HOUR);
        assert!(!stale.exists());
        assert!(fresh.exists());
        assert!(other.exists());
        Ok(())
    }
}
