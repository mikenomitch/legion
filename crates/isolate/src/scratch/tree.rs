//! The tree behind the scratch filesystem: directories, regular files, path
//! resolution, and the byte and node accounting.
//!
//! Directories live in a map keyed by inode number; regular files live in
//! their directory's entries as shared handles, so an open descriptor or
//! stream keeps a file's contents alive after it is unlinked (as POSIX
//! does) and the bytes go back to the account when the last handle drops.
//! There are no symbolic or hard links: every file has at most one name.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use super::bindings::wasi::clocks::wall_clock::Datetime;
use super::bindings::wasi::filesystem::types::{DescriptorStat, DescriptorType, ErrorCode};
use crate::account::MemoryAccount;

/// An inode number: unique within one tree for its whole life, never reused.
pub type Ino = u64;

/// The root directory's inode number.
pub const ROOT: Ino = 1;

/// The most nodes (directories plus files, including unlinked files a
/// descriptor still holds) one tree may have. File contents are charged to
/// the memory account; this bounds what directory entries and metadata can
/// cost the host. Creating a node past it is `insufficient-space`.
pub const MAX_NODES: usize = 16_384;

/// The longest name one path component may have, in bytes (POSIX
/// `NAME_MAX`). Longer is `name-too-long`.
pub const NAME_MAX: usize = 255;

/// The largest a file may grow, in bytes. Far above any memory account; it
/// only keeps offsets in range of `usize` and `i64` (`file-too-large`).
pub const MAX_FILE_SIZE: u64 = i64::MAX as u64;

type FsResult<T> = Result<T, ErrorCode>;

/// What the tree and every handle into it share: the account file bytes are
/// charged to, and the running totals.
#[derive(Debug)]
pub struct Shared {
    account: Arc<MemoryAccount>,
    bytes: AtomicU64,
    nodes: AtomicUsize,
}

impl Shared {
    fn charge(&self, bytes: u64) -> FsResult<()> {
        if bytes == 0 {
            return Ok(());
        }
        if !self.account.grow_scratch(bytes) {
            return Err(ErrorCode::InsufficientSpace);
        }
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }

    fn give_back(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.account.shrink_scratch(bytes);
        self.bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    fn take_node(&self) -> FsResult<()> {
        if self.nodes.load(Ordering::Relaxed) >= MAX_NODES {
            return Err(ErrorCode::InsufficientSpace);
        }
        self.nodes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn free_node(&self) {
        self.nodes.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The wall-clock time now, as WASI's `datetime`. Timestamps on scratch
/// nodes come from the host's real-time clock.
pub fn now() -> Datetime {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Datetime {
        seconds: d.as_secs(),
        nanoseconds: d.subsec_nanos(),
    }
}

/// A node's three timestamps.
#[derive(Debug, Clone, Copy)]
pub struct Times {
    pub access: Datetime,
    pub modify: Datetime,
    pub change: Datetime,
}

impl Times {
    fn new() -> Self {
        let t = now();
        Self {
            access: t,
            modify: t,
            change: t,
        }
    }

    fn touch(&mut self) {
        let t = now();
        self.modify = t;
        self.change = t;
    }
}

/// A regular file's contents and metadata.
#[derive(Debug)]
pub struct FileNode {
    ino: Ino,
    data: Vec<u8>,
    pub times: Times,
    linked: bool,
    shared: Arc<Shared>,
}

/// A shared handle to a regular file. Held by its directory entry, by
/// descriptors, and by streams.
pub type FileRef = Arc<Mutex<FileNode>>;

/// Lock a file. Every lock is held for one in-memory operation and never
/// across an await, so it never waits on anything but another such
/// operation; a poisoned lock is taken over, since the file's state is
/// consistent between operations.
pub fn lock(file: &FileRef) -> MutexGuard<'_, FileNode> {
    file.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl FileNode {
    pub fn ino(&self) -> Ino {
        self.ino
    }

    pub fn len(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn bytes(&self) -> &[u8] {
        &self.data
    }

    /// At most `len` bytes from `offset`; empty at or past the end.
    pub fn read_at(&self, offset: u64, len: u64) -> &[u8] {
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(self.data.len());
        let len = usize::try_from(len).unwrap_or(usize::MAX);
        let end = start.saturating_add(len).min(self.data.len());
        &self.data[start..end]
    }

    /// Write `buf` at `offset`, growing the file (zero-filled) as needed.
    /// Growth is charged first; a refused charge leaves the file unchanged.
    pub fn write_at(&mut self, offset: u64, buf: &[u8]) -> FsResult<u64> {
        if buf.is_empty() {
            return Ok(0);
        }
        let end = offset
            .checked_add(buf.len() as u64)
            .filter(|end| *end <= MAX_FILE_SIZE)
            .ok_or(ErrorCode::FileTooLarge)?;
        if end > self.len() {
            self.grow_to(end)?;
        }
        let start = offset as usize;
        self.data[start..start + buf.len()].copy_from_slice(buf);
        self.times.touch();
        Ok(buf.len() as u64)
    }

    /// Append `buf` at the current end.
    pub fn append(&mut self, buf: &[u8]) -> FsResult<u64> {
        let end = self.len();
        self.write_at(end, buf)
    }

    /// Set the length: growth is zero-filled and charged, shrinking gives
    /// the bytes back.
    pub fn set_size(&mut self, size: u64) -> FsResult<()> {
        if size > MAX_FILE_SIZE {
            return Err(ErrorCode::FileTooLarge);
        }
        let len = self.len();
        if size > len {
            self.grow_to(size)?;
        } else if size < len {
            self.data.truncate(size as usize);
            // Give memory back to the host as well as to the account when
            // most of the buffer is now unused.
            if self.data.capacity() / 2 > self.data.len() {
                self.data.shrink_to_fit();
            }
            self.shared.give_back(len - size);
        }
        self.times.touch();
        Ok(())
    }

    fn grow_to(&mut self, size: u64) -> FsResult<()> {
        let more = size - self.len();
        let new_len = usize::try_from(size).map_err(|_| ErrorCode::FileTooLarge)?;
        self.shared.charge(more)?;
        if self.data.try_reserve(new_len - self.data.len()).is_err() {
            self.shared.give_back(more);
            return Err(ErrorCode::InsufficientMemory);
        }
        self.data.resize(new_len, 0);
        Ok(())
    }

    fn stat(&self) -> DescriptorStat {
        DescriptorStat {
            type_: DescriptorType::RegularFile,
            link_count: u64::from(self.linked),
            size: self.len(),
            data_access_timestamp: Some(self.times.access),
            data_modification_timestamp: Some(self.times.modify),
            status_change_timestamp: Some(self.times.change),
        }
    }
}

impl Drop for FileNode {
    fn drop(&mut self) {
        self.shared.give_back(self.len());
        self.shared.free_node();
    }
}

#[derive(Debug, Clone)]
enum Entry {
    Dir(Ino),
    File(FileRef),
}

#[derive(Debug)]
struct DirNode {
    parent: Ino,
    entries: BTreeMap<String, Entry>,
    times: Times,
}

/// What a path names.
#[derive(Debug, Clone)]
pub enum Node {
    Dir(Ino),
    File(FileRef),
}

impl Node {
    pub fn is_same(&self, other: &Node) -> bool {
        match (self, other) {
            (Node::Dir(a), Node::Dir(b)) => a == b,
            (Node::File(a), Node::File(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

/// The last component of a resolved path.
enum Last<'p> {
    /// The path names the directory it resolved to (`.`, `..`, or a path
    /// that is only slashes).
    Dot,
    Name(&'p str),
}

struct Walk<'p> {
    /// The directory the last component is in (or, for `Dot`, the target).
    dir: Ino,
    last: Last<'p>,
    /// The path ended in `/`: it must name a directory.
    trailing_slash: bool,
}

/// How `open` should treat the path's last component.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenOptions {
    pub create: bool,
    pub exclusive: bool,
    pub directory: bool,
    pub truncate: bool,
    /// The descriptor will be opened for writing (refused on a directory).
    pub write: bool,
}

/// The tree. Paths are resolved POSIX-style: `.` is the directory itself,
/// `..` its parent, and `..` at the root is the root, so no path can name
/// anything outside the tree. A path that starts with `/` is resolved from
/// the root (host-side calls only; the guest-facing layer refuses absolute
/// paths as WASI requires).
#[derive(Debug)]
pub struct Tree {
    shared: Arc<Shared>,
    dirs: HashMap<Ino, DirNode>,
    next_ino: Ino,
}

impl Tree {
    pub fn new(account: Arc<MemoryAccount>) -> Self {
        let shared = Arc::new(Shared {
            account,
            bytes: AtomicU64::new(0),
            nodes: AtomicUsize::new(1),
        });
        let mut dirs = HashMap::new();
        dirs.insert(ROOT, new_dir(ROOT));
        Self {
            shared,
            dirs,
            next_ino: ROOT + 1,
        }
    }

    /// File bytes charged to the account, including files that are unlinked
    /// but still open.
    pub fn used_bytes(&self) -> u64 {
        self.shared.bytes.load(Ordering::Relaxed)
    }

    /// Nodes in use, including the root.
    pub fn nodes(&self) -> usize {
        self.shared.nodes.load(Ordering::Relaxed)
    }

    /// Remove everything under the root. A file a descriptor still holds
    /// keeps its bytes charged until that descriptor is dropped.
    pub fn clear(&mut self) {
        let removed = self.dirs.len() - 1;
        self.dirs.retain(|ino, _| *ino == ROOT);
        for _ in 0..removed {
            self.shared.free_node();
        }
        if let Some(root) = self.dirs.get_mut(&ROOT) {
            let entries = std::mem::take(&mut root.entries);
            for entry in entries.into_values() {
                if let Entry::File(f) = entry {
                    lock(&f).linked = false;
                }
            }
            root.times.touch();
        }
    }

    fn alloc_ino(&mut self) -> Ino {
        let ino = self.next_ino;
        self.next_ino += 1;
        ino
    }

    fn dir(&self, ino: Ino) -> FsResult<&DirNode> {
        // A directory removed while a descriptor still names it.
        self.dirs.get(&ino).ok_or(ErrorCode::NoEntry)
    }

    fn dir_mut(&mut self, ino: Ino) -> FsResult<&mut DirNode> {
        self.dirs.get_mut(&ino).ok_or(ErrorCode::NoEntry)
    }

    fn step(&self, dir: Ino, component: &str) -> FsResult<Ino> {
        let node = self.dir(dir)?;
        match component {
            "." => Ok(dir),
            ".." => Ok(node.parent),
            name => {
                check_name(name)?;
                match node.entries.get(name) {
                    Some(Entry::Dir(ino)) => Ok(*ino),
                    Some(Entry::File(_)) => Err(ErrorCode::NotDirectory),
                    None => Err(ErrorCode::NoEntry),
                }
            }
        }
    }

    fn walk<'p>(&self, base: Ino, path: &'p str) -> FsResult<Walk<'p>> {
        if path.is_empty() {
            return Err(ErrorCode::NoEntry);
        }
        if path.contains('\0') {
            return Err(ErrorCode::Invalid);
        }
        let mut dir = if path.starts_with('/') { ROOT } else { base };
        self.dir(dir)?;
        let mut components = path.split('/').filter(|c| !c.is_empty()).peekable();
        let mut last = Last::Dot;
        while let Some(component) = components.next() {
            if components.peek().is_some() {
                dir = self.step(dir, component)?;
                continue;
            }
            last = match component {
                "." => Last::Dot,
                ".." => {
                    dir = self.dir(dir)?.parent;
                    Last::Dot
                }
                name => {
                    check_name(name)?;
                    Last::Name(name)
                }
            };
        }
        Ok(Walk {
            dir,
            last,
            trailing_slash: path.ends_with('/'),
        })
    }

    /// What `path` (relative to directory `base`) names.
    pub fn lookup(&self, base: Ino, path: &str) -> FsResult<Node> {
        let walk = self.walk(base, path)?;
        let dir = self.dir(walk.dir)?;
        match walk.last {
            Last::Dot => Ok(Node::Dir(walk.dir)),
            Last::Name(name) => match dir.entries.get(name) {
                Some(Entry::Dir(ino)) => Ok(Node::Dir(*ino)),
                Some(Entry::File(_)) if walk.trailing_slash => Err(ErrorCode::NotDirectory),
                Some(Entry::File(f)) => Ok(Node::File(f.clone())),
                None => Err(ErrorCode::NoEntry),
            },
        }
    }

    /// Open (and with `create`, make) what `path` names.
    pub fn open(&mut self, base: Ino, path: &str, opts: OpenOptions) -> FsResult<Node> {
        let walk = self.walk(base, path)?;
        let existing = match walk.last {
            Last::Dot => {
                self.dir(walk.dir)?;
                Some(Node::Dir(walk.dir))
            }
            Last::Name(name) => match self.dir(walk.dir)?.entries.get(name) {
                Some(Entry::Dir(ino)) => Some(Node::Dir(*ino)),
                Some(Entry::File(f)) => Some(Node::File(f.clone())),
                None => None,
            },
        };
        match existing {
            Some(_) if opts.create && opts.exclusive => Err(ErrorCode::Exist),
            Some(Node::Dir(ino)) => {
                if opts.write || opts.truncate {
                    return Err(ErrorCode::IsDirectory);
                }
                Ok(Node::Dir(ino))
            }
            Some(Node::File(f)) => {
                if opts.directory || walk.trailing_slash {
                    return Err(ErrorCode::NotDirectory);
                }
                if opts.truncate {
                    lock(&f).set_size(0)?;
                }
                Ok(Node::File(f))
            }
            None => {
                let Last::Name(name) = walk.last else {
                    unreachable!("a `Dot` path always names an existing directory");
                };
                if !opts.create {
                    return Err(ErrorCode::NoEntry);
                }
                if opts.directory {
                    // open(O_CREAT | O_DIRECTORY) cannot make a directory.
                    return Err(ErrorCode::Invalid);
                }
                if walk.trailing_slash {
                    return Err(ErrorCode::IsDirectory);
                }
                let file = self.new_file()?;
                let dir = self.dir_mut(walk.dir)?;
                dir.entries
                    .insert(name.to_string(), Entry::File(file.clone()));
                dir.times.touch();
                Ok(Node::File(file))
            }
        }
    }

    fn new_file(&mut self) -> FsResult<FileRef> {
        self.shared.take_node()?;
        let ino = self.alloc_ino();
        Ok(Arc::new(Mutex::new(FileNode {
            ino,
            data: Vec::new(),
            times: Times::new(),
            linked: true,
            shared: self.shared.clone(),
        })))
    }

    /// Make the directory `path` names; its parent must exist.
    pub fn create_dir(&mut self, base: Ino, path: &str) -> FsResult<Ino> {
        let walk = self.walk(base, path)?;
        let Last::Name(name) = walk.last else {
            return Err(ErrorCode::Exist);
        };
        if self.dir(walk.dir)?.entries.contains_key(name) {
            return Err(ErrorCode::Exist);
        }
        self.shared.take_node()?;
        let ino = self.alloc_ino();
        self.dirs.insert(ino, new_dir(walk.dir));
        let parent = self.dir_mut(walk.dir)?;
        parent.entries.insert(name.to_string(), Entry::Dir(ino));
        parent.times.touch();
        Ok(ino)
    }

    /// Make `path` and every missing parent; succeeds when it is already a
    /// directory.
    pub fn create_dir_all(&mut self, base: Ino, path: &str) -> FsResult<Ino> {
        let mut dir = if path.starts_with('/') { ROOT } else { base };
        self.dir(dir)?;
        for component in path.split('/').filter(|c| !c.is_empty()) {
            dir = match self.step(dir, component) {
                Ok(ino) => ino,
                Err(ErrorCode::NoEntry) => {
                    check_name(component)?;
                    self.create_dir(dir, component)?
                }
                Err(e) => return Err(e),
            };
        }
        Ok(dir)
    }

    /// Remove the empty directory `path` names.
    pub fn remove_dir(&mut self, base: Ino, path: &str) -> FsResult<()> {
        let walk = self.walk(base, path)?;
        let Last::Name(name) = walk.last else {
            // rmdir(".") is EINVAL; rmdir("..") and "/" are busy on Linux,
            // but no guest can remove its own ancestor either way.
            return Err(ErrorCode::Invalid);
        };
        let ino = match self.dir(walk.dir)?.entries.get(name) {
            Some(Entry::Dir(ino)) => *ino,
            Some(Entry::File(_)) => return Err(ErrorCode::NotDirectory),
            None => return Err(ErrorCode::NoEntry),
        };
        if !self.dir(ino)?.entries.is_empty() {
            return Err(ErrorCode::NotEmpty);
        }
        self.dirs.remove(&ino);
        self.shared.free_node();
        let parent = self.dir_mut(walk.dir)?;
        parent.entries.remove(name);
        parent.times.touch();
        Ok(())
    }

    /// Remove the regular file `path` names. Its contents live on while a
    /// descriptor holds it.
    pub fn unlink(&mut self, base: Ino, path: &str) -> FsResult<()> {
        let walk = self.walk(base, path)?;
        let Last::Name(name) = walk.last else {
            return Err(ErrorCode::IsDirectory);
        };
        let parent = self.dir_mut(walk.dir)?;
        match parent.entries.get(name) {
            Some(Entry::Dir(_)) => Err(ErrorCode::IsDirectory),
            Some(Entry::File(_)) if walk.trailing_slash => Err(ErrorCode::NotDirectory),
            Some(Entry::File(_)) => {
                if let Some(Entry::File(f)) = parent.entries.remove(name) {
                    lock(&f).linked = false;
                }
                parent.times.touch();
                Ok(())
            }
            None => Err(ErrorCode::NoEntry),
        }
    }

    /// Move `old` (relative to `old_base`) to `new` (relative to
    /// `new_base`), replacing what is there as POSIX `rename` does.
    pub fn rename(&mut self, old_base: Ino, old: &str, new_base: Ino, new: &str) -> FsResult<()> {
        let from = self.walk(old_base, old)?;
        let to = self.walk(new_base, new)?;
        let (Last::Name(from_name), Last::Name(to_name)) = (&from.last, &to.last) else {
            // Renaming `.` or `..`, or onto them.
            return Err(ErrorCode::Invalid);
        };
        let (from_name, to_name) = (*from_name, *to_name);
        let source = self
            .dir(from.dir)?
            .entries
            .get(from_name)
            .cloned()
            .ok_or(ErrorCode::NoEntry)?;
        let target = self.dir(to.dir)?.entries.get(to_name).cloned();
        match &source {
            Entry::File(_) => {
                if from.trailing_slash || to.trailing_slash {
                    return Err(ErrorCode::NotDirectory);
                }
            }
            Entry::Dir(ino) => {
                // A directory cannot move under itself.
                let mut at = to.dir;
                loop {
                    if at == *ino {
                        return Err(ErrorCode::Invalid);
                    }
                    if at == ROOT {
                        break;
                    }
                    at = self.dir(at)?.parent;
                }
            }
        }
        match (&source, &target) {
            (Entry::File(a), Some(Entry::File(b))) if Arc::ptr_eq(a, b) => return Ok(()),
            (Entry::Dir(a), Some(Entry::Dir(b))) if a == b => return Ok(()),
            (Entry::File(_), Some(Entry::Dir(_))) => return Err(ErrorCode::IsDirectory),
            (Entry::Dir(_), Some(Entry::File(_))) => return Err(ErrorCode::NotDirectory),
            (Entry::Dir(_), Some(Entry::Dir(b))) => {
                if !self.dir(*b)?.entries.is_empty() {
                    return Err(ErrorCode::NotEmpty);
                }
                self.dirs.remove(b);
                self.shared.free_node();
            }
            (Entry::File(_), Some(Entry::File(b))) => lock(b).linked = false,
            (_, None) => {}
        }
        let from_dir = self.dir_mut(from.dir)?;
        from_dir.entries.remove(from_name);
        from_dir.times.touch();
        let to_dir = self.dir_mut(to.dir)?;
        to_dir.entries.insert(to_name.to_string(), source.clone());
        to_dir.times.touch();
        match &source {
            Entry::Dir(ino) => {
                let moved = self.dir_mut(*ino)?;
                moved.parent = to.dir;
                moved.times.change = now();
            }
            Entry::File(f) => lock(f).times.change = now(),
        }
        Ok(())
    }

    /// A directory's entries, in name order, without `.` and `..`.
    pub fn list(&self, dir: Ino) -> FsResult<Vec<(String, DescriptorType)>> {
        Ok(self
            .dir(dir)?
            .entries
            .iter()
            .map(|(name, entry)| {
                let ty = match entry {
                    Entry::Dir(_) => DescriptorType::Directory,
                    Entry::File(_) => DescriptorType::RegularFile,
                };
                (name.clone(), ty)
            })
            .collect())
    }

    pub fn stat(&self, node: &Node) -> FsResult<DescriptorStat> {
        match node {
            Node::File(f) => Ok(lock(f).stat()),
            Node::Dir(ino) => {
                let dir = self.dir(*ino)?;
                let subdirs = dir
                    .entries
                    .values()
                    .filter(|e| matches!(e, Entry::Dir(_)))
                    .count() as u64;
                Ok(DescriptorStat {
                    type_: DescriptorType::Directory,
                    link_count: 2 + subdirs,
                    size: 0,
                    data_access_timestamp: Some(dir.times.access),
                    data_modification_timestamp: Some(dir.times.modify),
                    status_change_timestamp: Some(dir.times.change),
                })
            }
        }
    }

    /// Set a node's access and modification times (`None` leaves one as
    /// it is); the status-change time becomes now.
    pub fn set_times(
        &mut self,
        node: &Node,
        access: Option<Datetime>,
        modify: Option<Datetime>,
    ) -> FsResult<()> {
        let apply = |times: &mut Times| {
            if let Some(t) = access {
                times.access = t;
            }
            if let Some(t) = modify {
                times.modify = t;
            }
            times.change = now();
        };
        match node {
            Node::File(f) => apply(&mut lock(f).times),
            Node::Dir(ino) => apply(&mut self.dir_mut(*ino)?.times),
        }
        Ok(())
    }

    /// Write a whole file at `path`, making its parents and replacing its
    /// contents. A refused charge leaves an existing file as it was.
    pub fn write_file(&mut self, base: Ino, path: &str, bytes: &[u8]) -> FsResult<()> {
        let new = bytes.len() as u64;
        let f = match self.lookup(base, path) {
            Ok(Node::Dir(_)) => return Err(ErrorCode::IsDirectory),
            Ok(Node::File(f)) => {
                let old = lock(&f).len();
                // Charge the growth first, so a refusal leaves the old
                // contents in place.
                if new > old {
                    self.shared.charge(new - old)?;
                } else {
                    self.shared.give_back(old - new);
                }
                f
            }
            Err(ErrorCode::NoEntry) => {
                // Charge before making anything, so a refusal makes no file.
                self.shared.charge(new)?;
                let made = self.make_file(base, path);
                match made {
                    Ok(f) => f,
                    Err(e) => {
                        self.shared.give_back(new);
                        return Err(e);
                    }
                }
            }
            Err(e) => return Err(e),
        };
        let mut file = lock(&f);
        file.data.clear();
        file.data.extend_from_slice(bytes);
        file.data.shrink_to_fit();
        file.times.touch();
        Ok(())
    }

    fn make_file(&mut self, base: Ino, path: &str) -> FsResult<FileRef> {
        let parent = parent_of(path);
        if !parent.is_empty() {
            self.create_dir_all(base, parent)?;
        }
        let opts = OpenOptions {
            create: true,
            write: true,
            ..Default::default()
        };
        match self.open(base, path, opts)? {
            Node::File(f) => Ok(f),
            Node::Dir(_) => Err(ErrorCode::IsDirectory),
        }
    }
}

fn new_dir(parent: Ino) -> DirNode {
    DirNode {
        parent,
        entries: BTreeMap::new(),
        times: Times::new(),
    }
}

fn check_name(name: &str) -> FsResult<()> {
    if name.len() > NAME_MAX {
        return Err(ErrorCode::NameTooLong);
    }
    Ok(())
}

/// Everything before the last component of `path` (`""` when there is
/// none).
fn parent_of(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(0) => "/",
        Some(i) => &trimmed[..i],
        None => "",
    }
}
