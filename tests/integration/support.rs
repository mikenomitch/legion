//! Helpers shared across the integration test modules.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

static CLUSTER_GATE: Mutex<()> = Mutex::new(());
static SCRATCH_ROOT: OnceLock<PathBuf> = OnceLock::new();

/// Serializes the multi-node raft cluster tests across every module in this
/// binary.
///
/// Each of those tests runs a 3-node cluster in-process (nine raft cores in
/// the lanes topology) with tight 150–300ms election timeouts. Two at once on
/// a loaded CI machine starve each other's heartbeats and trigger spurious
/// elections that move leadership mid-assertion. One at a time keeps each
/// test's timing assumptions valid, and each is fast in isolation. Before the
/// tests shared a binary each file had its own gate and the files ran one
/// after another; one gate across all of them keeps that guarantee.
///
/// A panicking test poisons the lock; the next test takes it anyway, since
/// the gate guards no data.
pub fn cluster_gate() -> MutexGuard<'static, ()> {
    CLUSTER_GATE.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Serializes the tests that bind ports from the scheduler's dynamic range
/// (from 20000) on this host. Every cluster in this binary hands out the
/// same ports, so two such tests at once take each other's: the wake test
/// binds its instance's advertised port, and the apps end-to-end test runs
/// the bundled autoscaler, which binds its own. Async tests `.lock().await`
/// it; plain tests `.blocking_lock()` it.
pub static DYNAMIC_PORT_GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The per-process root for test directories that must outlive their
/// `TempDir` guard: create them with `tempdir_in(scratch_root())` before
/// calling `keep()`.
///
/// A store helper returns a `Store` whose files live in the directory it
/// made, so it keeps the directory, and nothing ever removed it: every run
/// left hundreds of megabytes of SQLite stores in the system temp dir. The
/// root is removed when the test process exits (the library's unit tests do
/// the same in `src/test_scratch.rs`). A run killed before exit leaves its
/// one `legion-tests-*` root behind.
pub fn scratch_root() -> &'static Path {
    SCRATCH_ROOT.get_or_init(|| {
        let root = tempfile::Builder::new()
            .prefix("legion-tests-")
            .tempdir()
            .expect("create the test scratch root")
            .keep();
        // SAFETY: registers a plain `extern "C"` function with no captured
        // state; it only touches the already-initialized `SCRATCH_ROOT`.
        unsafe {
            libc::atexit(remove_scratch_root);
        }
        root
    })
}

extern "C" fn remove_scratch_root() {
    if let Some(root) = SCRATCH_ROOT.get() {
        kill_task_processes_under(root);
        // Best effort: a background thread still writing into a store can
        // race the removal, and the process is exiting either way.
        let _ = std::fs::remove_dir_all(root);
    }
}

/// Kill the task processes a test cluster under `dir` started: those whose
/// `LEGION_ALLOC_DIR` is inside it. A worker that stops leaves its tasks
/// running on purpose (the next start re-adopts them;
/// docs/architecture-managers-workers.md), so a test that starts a real
/// worker calls this once the worker has exited, or its tasks outlive the
/// test and keep holding their ports. Linux only (it reads `/proc`); a no-op
/// elsewhere.
pub fn kill_task_processes_under(dir: &Path) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;
        let Ok(entries) = std::fs::read_dir("/proc") else {
            return;
        };
        for entry in entries.flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<libc::pid_t>().ok())
            else {
                continue;
            };
            let Ok(environ) = std::fs::read(format!("/proc/{pid}/environ")) else {
                continue;
            };
            let started_here = environ
                .split(|byte| *byte == 0)
                .filter_map(|var| var.strip_prefix(b"LEGION_ALLOC_DIR="))
                .any(|value| Path::new(std::ffi::OsStr::from_bytes(value)).starts_with(dir));
            if started_here {
                // SAFETY: plain kill(2) on a task process this test started.
                unsafe { libc::kill(pid, libc::SIGKILL) };
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = dir;
}
