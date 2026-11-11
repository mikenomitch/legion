//! The in-memory scratch filesystem an isolate sees as `wasi:filesystem`.
//!
//! wasmtime-wasi's own filesystem is backed by host directories, and an
//! isolate host process has no writable directory to give a guest (it is
//! confined read-only), so the host implements `wasi:filesystem/types` and
//! `wasi:filesystem/preopens` itself over a tree in memory: one tree per
//! isolate, holding the run directory (`/legion/run`) and whatever the guest
//! writes. Every other WASI interface comes from wasmtime-wasi.
//!
//! What it is:
//! - **Directories and regular files only.** `symlink-at` and `link-at`
//!   return `not-permitted` (what Linux says on a filesystem without them),
//!   `readlink-at` returns `invalid` for whatever the path names.
//! - **Paths resolve POSIX-style inside the tree.** `.` and `..` work at any
//!   depth, and `..` at the root is the root, so nothing outside the tree can
//!   be named. Absolute paths given to a guest-facing function are
//!   `not-permitted`, as WASI requires; guests reach them through the `/`
//!   preopen (wasi-libc does this for them).
//! - **Bounded.** File bytes are charged to the isolate's
//!   [`MemoryAccount`](crate::account::MemoryAccount), the same ceiling its
//!   linear memory is charged to; the number of nodes is capped at
//!   [`MAX_NODES`]. Past either, the guest gets `insufficient-space` and the
//!   file is left as it was.
//! - **Immediate.** Everything is in memory and nothing blocks; streams are
//!   always ready.
//! - **Timestamps never come from a clock the tree reads.** Every time the
//!   tree sets is the embedder's: [`ScratchCtx::now`] for guest calls,
//!   [`Scratch::set_now`] for host-side staging (and for file streams
//!   between filesystem calls). The embedder passes its coarse, turn-frozen
//!   wall clock, so `write` then `stat` cannot serve a guest as a timer finer
//!   than the clocks it is given. Reads do not update the access time.

mod bindings;
mod host;
mod tree;

#[cfg(test)]
mod scratch_test;

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use wasmtime::component::{HasData, Linker, ResourceTable};

use crate::account::MemoryAccount;
pub use bindings::wasi::clocks::wall_clock::Datetime;
pub use bindings::wasi::filesystem::types::ErrorCode;
pub use host::{Descriptor, DirEntries, FsError, StreamFsError};
pub use tree::{EPOCH, MAX_NODES, NAME_MAX};

/// The run directory a guest starts in: preopened as `.` and the
/// working directory a host should give `wasi:cli/environment.initial-cwd`.
pub const RUN_DIR: &str = "/legion/run";

/// One isolate's scratch tree.
#[derive(Debug)]
pub struct Scratch {
    tree: tree::Tree,
    /// Distinguishes trees in `metadata-hash` (wasi-libc's `st_dev`).
    id: u64,
}

/// A host-side scratch operation failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScratchError {
    /// The path the operation was given.
    pub path: String,
    /// What a guest would have seen.
    pub code: ErrorCode,
}

impl fmt::Display for ScratchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match self.code {
            ErrorCode::InsufficientSpace => {
                "out of scratch space (the isolate's memory limit or the node cap)"
            }
            ErrorCode::NoEntry => "a parent directory does not exist",
            ErrorCode::NotDirectory => "a parent is a file, not a directory",
            ErrorCode::IsDirectory => "it is a directory",
            ErrorCode::Exist => "it already exists",
            ErrorCode::NameTooLong => "a path component is longer than 255 bytes",
            _ => "the scratch filesystem refused it",
        };
        write!(f, "scratch path {}: {what} ({})", self.path, self.code)
    }
}

impl std::error::Error for ScratchError {}

static NEXT_TREE_ID: AtomicU64 = AtomicU64::new(1);

impl Scratch {
    /// An empty tree charging file bytes to `account`; a refused charge is
    /// `insufficient-space` (ENOSPC) to the guest. Its clock starts at the
    /// Unix epoch ([`EPOCH`]) until [`set_now`](Self::set_now).
    pub fn new(account: Arc<MemoryAccount>) -> Self {
        Self {
            tree: tree::Tree::new(account),
            id: NEXT_TREE_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Set the time timestamps are taken from: host-side staging after this
    /// call, and file streams' writes until the next guest filesystem call
    /// (which sets it from [`ScratchCtx::now`]). Pass the same coarse,
    /// turn-frozen wall clock the guest sees; set it at the start of each
    /// turn.
    pub fn set_now(&mut self, now: Datetime) {
        self.tree.set_now(now);
    }

    /// The time the tree stamps with now.
    pub fn now(&self) -> Datetime {
        self.tree.now()
    }

    /// Host-side staging: create parents, write the file (replacing what
    /// was there). Paths are absolute within the tree
    /// (`/legion/run/main.py`). A refused charge leaves an existing file as
    /// it was and makes no new one.
    pub fn write_file(&mut self, path: &str, bytes: &[u8]) -> Result<(), ScratchError> {
        self.tree
            .write_file(tree::ROOT, path, bytes)
            .map_err(|code| err(path, code))
    }

    /// Host-side read of at most `max` bytes: `(bytes, truncated)`. `None`
    /// when there is no regular file there.
    pub fn read_file(&self, path: &str, max: usize) -> Option<(Vec<u8>, bool)> {
        match self.tree.lookup(tree::ROOT, path).ok()? {
            tree::Node::File(f) => {
                let file = tree::lock(&f);
                let bytes = file.bytes();
                let n = bytes.len().min(max);
                Some((bytes[..n].to_vec(), bytes.len() > max))
            }
            tree::Node::Dir(_) => None,
        }
    }

    /// Make `path` and any missing parents; fine when it already is a
    /// directory.
    pub fn create_dir_all(&mut self, path: &str) -> Result<(), ScratchError> {
        self.tree
            .create_dir_all(tree::ROOT, path)
            .map(|_| ())
            .map_err(|code| err(path, code))
    }

    /// File bytes charged to the account, including files a guest unlinked
    /// but still holds open.
    pub fn used_bytes(&self) -> u64 {
        self.tree.used_bytes()
    }

    /// Directories and files in the tree (the root included), counted
    /// against [`MAX_NODES`].
    pub fn nodes(&self) -> usize {
        self.tree.nodes()
    }

    /// Drop everything and give the bytes back to the account (an instance
    /// reused across calls with a fresh run directory). A file a live
    /// descriptor still holds stays charged until that descriptor is
    /// dropped, so clear after the guest's resources are gone.
    pub fn clear(&mut self) {
        self.tree.clear();
    }
}

fn err(path: &str, code: ErrorCode) -> ScratchError {
    ScratchError {
        path: path.to_string(),
        code,
    }
}

/// What a store's data must provide for the filesystem host functions.
pub trait ScratchView: Send {
    fn scratch(&mut self) -> ScratchCtx<'_>;
}

/// The borrowed state the filesystem functions work on.
pub struct ScratchCtx<'a> {
    pub scratch: &'a mut Scratch,
    /// The store's resource table (shared with the rest of WASI, so file
    /// streams are ordinary `wasi:io` streams).
    pub table: &'a mut ResourceTable,
    /// The directory preopened as `.`, absolute within the tree; normally
    /// [`RUN_DIR`].
    pub cwd: &'a str,
    /// The time every timestamp set by this call takes (created, modified,
    /// accessed, status-changed, and `new-timestamp.now`). The embedder
    /// passes its coarse, turn-frozen wall clock (the same value the guest's
    /// `wasi:clocks/wall-clock.now` returns this turn); the tree never reads
    /// a clock itself. It is also stored on the tree, so file streams' writes
    /// that follow use it.
    pub now: Datetime,
}

struct HasScratch;

impl HasData for HasScratch {
    type Data<'a> = ScratchCtx<'a>;
}

/// Every filesystem call starts here: the tree takes its time from the
/// context's `now`.
fn scratch_ctx<T: ScratchView>(t: &mut T) -> ScratchCtx<'_> {
    let ctx = t.scratch();
    ctx.scratch.set_now(ctx.now);
    ctx
}

/// Link `wasi:filesystem/types` and `wasi:filesystem/preopens` (and
/// nothing else). The functions are synchronous, so the linker serves both
/// `call` and `call_async`.
pub fn add_to_linker<T: ScratchView + 'static>(linker: &mut Linker<T>) -> anyhow::Result<()> {
    bindings::wasi::filesystem::types::add_to_linker::<T, HasScratch>(linker, scratch_ctx::<T>)?;
    bindings::wasi::filesystem::preopens::add_to_linker::<T, HasScratch>(linker, scratch_ctx::<T>)?;
    Ok(())
}
