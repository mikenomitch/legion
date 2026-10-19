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
        // Best effort: a background thread still writing into a store can
        // race the removal, and the process is exiting either way.
        let _ = std::fs::remove_dir_all(root);
    }
}
