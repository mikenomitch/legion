//! Scratch space for the library's unit tests that must outlive a
//! `TempDir` guard.
//!
//! A store helper hands back a `Store` whose files live in a directory the
//! helper created, so it calls `TempDir::keep()` and the directory is never
//! removed. With hundreds of such stores per run (~1.7 MB each), every
//! `cargo test` left over a gigabyte in the system temp dir. Kept directories
//! go under one per-process root instead, and the root is removed when the
//! test process exits. A run killed before exit leaves its one
//! `legion-tests-*` root behind.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

static ROOT: OnceLock<PathBuf> = OnceLock::new();

/// The per-process scratch root: create kept directories inside it with
/// `tempfile::Builder::tempdir_in` or `tempfile::tempdir_in`.
pub(crate) fn root() -> &'static Path {
    ROOT.get_or_init(|| {
        let root = tempfile::Builder::new()
            .prefix("legion-tests-")
            .tempdir()
            .expect("create the test scratch root")
            .keep();
        // SAFETY: registers a plain `extern "C"` function with no captured
        // state; it only touches the already-initialized `ROOT`.
        unsafe {
            libc::atexit(remove_root);
        }
        root
    })
}

extern "C" fn remove_root() {
    if let Some(root) = ROOT.get() {
        // Best effort: a background thread still writing into a store can
        // race the removal, and the process is exiting either way.
        let _ = std::fs::remove_dir_all(root);
    }
}
