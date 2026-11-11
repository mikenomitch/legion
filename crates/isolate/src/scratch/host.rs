//! The `wasi:filesystem` host functions: descriptors, directory streams,
//! file streams, and preopens over the scratch tree.

use std::fmt;

use wasmtime::component::{Resource, ResourceTable, ResourceTableError};
use wasmtime_wasi_io::bytes::Bytes;
use wasmtime_wasi_io::streams::{
    DynInputStream, DynOutputStream, InputStream, OutputStream, StreamError, StreamResult,
};

use super::bindings::wasi::clocks::wall_clock::Datetime;
use super::bindings::wasi::filesystem::{preopens, types};
use super::tree::{self, FileRef, Node, OpenOptions};
use super::ScratchCtx;
use types::{
    Advice, DescriptorFlags, DescriptorStat, DescriptorType, DirectoryEntry, ErrorCode, Filesize,
    MetadataHashValue, NewTimestamp, OpenFlags, PathFlags,
};

/// How much one `check-write` on a file stream permits. Writes complete in
/// memory immediately, so this only bounds one guest-to-host copy.
const WRITE_PERMIT: usize = 1 << 20;

/// The `descriptor` resource: an open directory or regular file.
#[derive(Debug)]
pub struct Descriptor {
    node: Node,
    flags: DescriptorFlags,
}

impl Descriptor {
    fn dir(&self) -> Result<tree::Ino, FsError> {
        match &self.node {
            Node::Dir(ino) => Ok(*ino),
            Node::File(_) => Err(ErrorCode::NotDirectory.into()),
        }
    }

    fn file(&self) -> Result<&FileRef, FsError> {
        match &self.node {
            Node::File(f) => Ok(f),
            Node::Dir(_) => Err(ErrorCode::IsDirectory.into()),
        }
    }

    fn readable_file(&self) -> Result<&FileRef, FsError> {
        let f = self.file()?;
        if !self.flags.contains(DescriptorFlags::READ) {
            return Err(ErrorCode::BadDescriptor.into());
        }
        Ok(f)
    }

    fn writable_file(&self) -> Result<&FileRef, FsError> {
        let f = self.file()?;
        if !self.flags.contains(DescriptorFlags::WRITE) {
            return Err(ErrorCode::BadDescriptor.into());
        }
        Ok(f)
    }
}

/// The `directory-entry-stream` resource: a snapshot of a directory's
/// entries taken when it was opened.
#[derive(Debug)]
pub struct DirEntries {
    entries: std::vec::IntoIter<DirectoryEntry>,
}

/// A filesystem function's failure: an `error-code` for the guest, or a
/// host error that traps it (a bad resource handle).
#[derive(Debug)]
pub struct FsError(FsErrorKind);

#[derive(Debug)]
enum FsErrorKind {
    Code(ErrorCode),
    Trap(wasmtime::Error),
}

impl From<ErrorCode> for FsError {
    fn from(code: ErrorCode) -> Self {
        FsError(FsErrorKind::Code(code))
    }
}

impl From<ResourceTableError> for FsError {
    fn from(e: ResourceTableError) -> Self {
        FsError(FsErrorKind::Trap(e.into()))
    }
}

impl FsError {
    /// The code a guest sees, or `None` for a trapping host error.
    pub fn code(&self) -> Option<ErrorCode> {
        match &self.0 {
            FsErrorKind::Code(code) => Some(*code),
            FsErrorKind::Trap(_) => None,
        }
    }
}

type FsResult<T> = Result<T, FsError>;

/// The error a file stream reports as `last-operation-failed`;
/// `filesystem-error-code` turns it back into its code.
#[derive(Debug, Clone, Copy)]
pub struct StreamFsError(pub ErrorCode);

impl fmt::Display for StreamFsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "scratch filesystem: {}", self.0)
    }
}

impl std::error::Error for StreamFsError {}

fn stream_failed(code: ErrorCode) -> StreamError {
    StreamError::LastOperationFailed(wasmtime::Error::new(StreamFsError(code)))
}

impl ScratchCtx<'_> {
    fn descriptor(&self, fd: &Resource<Descriptor>) -> FsResult<&Descriptor> {
        Ok(self.table.get(fd)?)
    }

    /// The directory a path is resolved from. WASI paths are relative to a
    /// descriptor; an absolute one is refused as wasmtime-wasi refuses it.
    fn base(&self, fd: &Resource<Descriptor>, path: &str) -> FsResult<tree::Ino> {
        let ino = self.descriptor(fd)?.dir()?;
        if path.starts_with('/') {
            return Err(ErrorCode::NotPermitted.into());
        }
        Ok(ino)
    }

    fn push(
        table: &mut ResourceTable,
        node: Node,
        flags: DescriptorFlags,
    ) -> FsResult<Resource<Descriptor>> {
        Ok(table.push(Descriptor { node, flags })?)
    }
}

impl types::Host for ScratchCtx<'_> {
    fn convert_error_code(&mut self, err: FsError) -> wasmtime::Result<ErrorCode> {
        match err.0 {
            FsErrorKind::Code(code) => Ok(code),
            FsErrorKind::Trap(e) => Err(e),
        }
    }

    fn filesystem_error_code(
        &mut self,
        err: Resource<wasmtime::Error>,
    ) -> wasmtime::Result<Option<ErrorCode>> {
        let err = self.table.get(&err)?;
        Ok(err.downcast_ref::<StreamFsError>().map(|e| e.0))
    }
}

impl types::HostDescriptor for ScratchCtx<'_> {
    fn read_via_stream(
        &mut self,
        fd: Resource<Descriptor>,
        offset: Filesize,
    ) -> FsResult<Resource<DynInputStream>> {
        let file = self.descriptor(&fd)?.readable_file()?.clone();
        let stream: DynInputStream = Box::new(FileReader { file, pos: offset });
        Ok(self.table.push(stream)?)
    }

    fn write_via_stream(
        &mut self,
        fd: Resource<Descriptor>,
        offset: Filesize,
    ) -> FsResult<Resource<DynOutputStream>> {
        let file = self.descriptor(&fd)?.writable_file()?.clone();
        let stream: DynOutputStream = Box::new(FileWriter {
            file,
            pos: Some(offset),
            closed: false,
        });
        Ok(self.table.push(stream)?)
    }

    fn append_via_stream(
        &mut self,
        fd: Resource<Descriptor>,
    ) -> FsResult<Resource<DynOutputStream>> {
        let file = self.descriptor(&fd)?.writable_file()?.clone();
        let stream: DynOutputStream = Box::new(FileWriter {
            file,
            pos: None,
            closed: false,
        });
        Ok(self.table.push(stream)?)
    }

    fn advise(
        &mut self,
        fd: Resource<Descriptor>,
        _offset: Filesize,
        _length: Filesize,
        _advice: Advice,
    ) -> FsResult<()> {
        self.descriptor(&fd)?.file()?;
        Ok(())
    }

    fn sync_data(&mut self, fd: Resource<Descriptor>) -> FsResult<()> {
        self.descriptor(&fd)?;
        Ok(())
    }

    fn get_flags(&mut self, fd: Resource<Descriptor>) -> FsResult<DescriptorFlags> {
        Ok(self.descriptor(&fd)?.flags)
    }

    fn get_type(&mut self, fd: Resource<Descriptor>) -> FsResult<DescriptorType> {
        Ok(match self.descriptor(&fd)?.node {
            Node::Dir(_) => DescriptorType::Directory,
            Node::File(_) => DescriptorType::RegularFile,
        })
    }

    fn set_size(&mut self, fd: Resource<Descriptor>, size: Filesize) -> FsResult<()> {
        let file = self.descriptor(&fd)?.writable_file()?;
        tree::lock(file).set_size(size)?;
        Ok(())
    }

    fn set_times(
        &mut self,
        fd: Resource<Descriptor>,
        access: NewTimestamp,
        modify: NewTimestamp,
    ) -> FsResult<()> {
        let node = self.descriptor(&fd)?.node.clone();
        self.scratch
            .tree
            .set_times(&node, timestamp(access), timestamp(modify))?;
        Ok(())
    }

    fn read(
        &mut self,
        fd: Resource<Descriptor>,
        length: Filesize,
        offset: Filesize,
    ) -> FsResult<(Vec<u8>, bool)> {
        let file = tree::lock(self.descriptor(&fd)?.readable_file()?);
        let bytes = file.read_at(offset, length).to_vec();
        let end = offset.saturating_add(bytes.len() as u64) >= file.len();
        Ok((bytes, end))
    }

    fn write(
        &mut self,
        fd: Resource<Descriptor>,
        buffer: Vec<u8>,
        offset: Filesize,
    ) -> FsResult<Filesize> {
        let mut file = tree::lock(self.descriptor(&fd)?.writable_file()?);
        Ok(file.write_at(offset, &buffer)?)
    }

    fn read_directory(&mut self, fd: Resource<Descriptor>) -> FsResult<Resource<DirEntries>> {
        let ino = self.descriptor(&fd)?.dir()?;
        let entries: Vec<DirectoryEntry> = self
            .scratch
            .tree
            .list(ino)?
            .into_iter()
            .map(|(name, type_)| DirectoryEntry { type_, name })
            .collect();
        Ok(self.table.push(DirEntries {
            entries: entries.into_iter(),
        })?)
    }

    fn sync(&mut self, fd: Resource<Descriptor>) -> FsResult<()> {
        self.descriptor(&fd)?;
        Ok(())
    }

    fn create_directory_at(&mut self, fd: Resource<Descriptor>, path: String) -> FsResult<()> {
        let base = self.base(&fd, &path)?;
        self.scratch.tree.create_dir(base, &path)?;
        Ok(())
    }

    fn stat(&mut self, fd: Resource<Descriptor>) -> FsResult<DescriptorStat> {
        let node = self.descriptor(&fd)?.node.clone();
        Ok(self.scratch.tree.stat(&node)?)
    }

    fn stat_at(
        &mut self,
        fd: Resource<Descriptor>,
        _path_flags: PathFlags,
        path: String,
    ) -> FsResult<DescriptorStat> {
        let base = self.base(&fd, &path)?;
        let node = self.scratch.tree.lookup(base, &path)?;
        Ok(self.scratch.tree.stat(&node)?)
    }

    fn set_times_at(
        &mut self,
        fd: Resource<Descriptor>,
        _path_flags: PathFlags,
        path: String,
        access: NewTimestamp,
        modify: NewTimestamp,
    ) -> FsResult<()> {
        let base = self.base(&fd, &path)?;
        let node = self.scratch.tree.lookup(base, &path)?;
        self.scratch
            .tree
            .set_times(&node, timestamp(access), timestamp(modify))?;
        Ok(())
    }

    /// Hard links are not supported: every scratch file has one name.
    /// `not-permitted` is what Linux returns from `link` on a filesystem
    /// without them.
    fn link_at(
        &mut self,
        fd: Resource<Descriptor>,
        _old_path_flags: PathFlags,
        old_path: String,
        new_fd: Resource<Descriptor>,
        new_path: String,
    ) -> FsResult<()> {
        let old_base = self.base(&fd, &old_path)?;
        let new_base = self.base(&new_fd, &new_path)?;
        self.scratch.tree.lookup(old_base, &old_path)?;
        if self.scratch.tree.lookup(new_base, &new_path).is_ok() {
            return Err(ErrorCode::Exist.into());
        }
        Err(ErrorCode::NotPermitted.into())
    }

    fn open_at(
        &mut self,
        fd: Resource<Descriptor>,
        _path_flags: PathFlags,
        path: String,
        open_flags: OpenFlags,
        flags: DescriptorFlags,
    ) -> FsResult<Resource<Descriptor>> {
        let base = self.base(&fd, &path)?;
        let opts = OpenOptions {
            create: open_flags.contains(OpenFlags::CREATE),
            exclusive: open_flags.contains(OpenFlags::EXCLUSIVE),
            directory: open_flags.contains(OpenFlags::DIRECTORY),
            truncate: open_flags.contains(OpenFlags::TRUNCATE),
            write: flags.contains(DescriptorFlags::WRITE),
        };
        let node = self.scratch.tree.open(base, &path, opts)?;
        // Every scratch directory is writable by the guest.
        let flags = match node {
            Node::Dir(_) => flags | DescriptorFlags::MUTATE_DIRECTORY,
            Node::File(_) => flags,
        };
        Self::push(self.table, node, flags)
    }

    fn readlink_at(&mut self, fd: Resource<Descriptor>, path: String) -> FsResult<String> {
        // There are no symbolic links, so whatever the path names is not
        // one: `invalid`, as POSIX readlink says for a non-link.
        let base = self.base(&fd, &path)?;
        self.scratch.tree.lookup(base, &path)?;
        Err(ErrorCode::Invalid.into())
    }

    fn remove_directory_at(&mut self, fd: Resource<Descriptor>, path: String) -> FsResult<()> {
        let base = self.base(&fd, &path)?;
        self.scratch.tree.remove_dir(base, &path)?;
        Ok(())
    }

    fn rename_at(
        &mut self,
        fd: Resource<Descriptor>,
        old_path: String,
        new_fd: Resource<Descriptor>,
        new_path: String,
    ) -> FsResult<()> {
        let old_base = self.base(&fd, &old_path)?;
        let new_base = self.base(&new_fd, &new_path)?;
        self.scratch
            .tree
            .rename(old_base, &old_path, new_base, &new_path)?;
        Ok(())
    }

    /// Symbolic links are not supported. `not-permitted` is what Linux
    /// returns from `symlink` on a filesystem without them.
    fn symlink_at(
        &mut self,
        fd: Resource<Descriptor>,
        _old_path: String,
        new_path: String,
    ) -> FsResult<()> {
        let base = self.base(&fd, &new_path)?;
        if self.scratch.tree.lookup(base, &new_path).is_ok() {
            return Err(ErrorCode::Exist.into());
        }
        Err(ErrorCode::NotPermitted.into())
    }

    fn unlink_file_at(&mut self, fd: Resource<Descriptor>, path: String) -> FsResult<()> {
        let base = self.base(&fd, &path)?;
        self.scratch.tree.unlink(base, &path)?;
        Ok(())
    }

    fn is_same_object(
        &mut self,
        a: Resource<Descriptor>,
        b: Resource<Descriptor>,
    ) -> wasmtime::Result<bool> {
        let a = &self.table.get(&a)?.node;
        let b = &self.table.get(&b)?.node;
        Ok(a.is_same(b))
    }

    fn metadata_hash(&mut self, fd: Resource<Descriptor>) -> FsResult<MetadataHashValue> {
        let node = self.descriptor(&fd)?.node.clone();
        Ok(self.metadata_hash_of(&node))
    }

    fn metadata_hash_at(
        &mut self,
        fd: Resource<Descriptor>,
        _path_flags: PathFlags,
        path: String,
    ) -> FsResult<MetadataHashValue> {
        let base = self.base(&fd, &path)?;
        let node = self.scratch.tree.lookup(base, &path)?;
        Ok(self.metadata_hash_of(&node))
    }

    fn drop(&mut self, fd: Resource<Descriptor>) -> wasmtime::Result<()> {
        self.table.delete(fd)?;
        Ok(())
    }
}

impl ScratchCtx<'_> {
    /// The inode number and this tree's id: equal for the same object, and
    /// what wasi-libc turns into `st_dev`/`st_ino`.
    fn metadata_hash_of(&self, node: &Node) -> MetadataHashValue {
        let ino = match node {
            Node::Dir(ino) => *ino,
            Node::File(f) => tree::lock(f).ino(),
        };
        MetadataHashValue {
            lower: ino,
            upper: self.scratch.id,
        }
    }
}

impl types::HostDirectoryEntryStream for ScratchCtx<'_> {
    fn read_directory_entry(
        &mut self,
        stream: Resource<DirEntries>,
    ) -> FsResult<Option<DirectoryEntry>> {
        Ok(self.table.get_mut(&stream)?.entries.next())
    }

    fn drop(&mut self, stream: Resource<DirEntries>) -> wasmtime::Result<()> {
        self.table.delete(stream)?;
        Ok(())
    }
}

impl preopens::Host for ScratchCtx<'_> {
    /// `/` is the tree's root. When the working directory exists it is
    /// offered too, as `.`, for guests that resolve relative paths against
    /// a `.` preopen rather than through `wasi:cli/environment.initial-cwd`.
    fn get_directories(&mut self) -> wasmtime::Result<Vec<(Resource<Descriptor>, String)>> {
        let dir_flags = DescriptorFlags::READ | DescriptorFlags::MUTATE_DIRECTORY;
        let mut out = Vec::with_capacity(2);
        if let Ok(Node::Dir(cwd)) = self.scratch.tree.lookup(tree::ROOT, self.cwd) {
            if cwd != tree::ROOT {
                let fd = self.table.push(Descriptor {
                    node: Node::Dir(cwd),
                    flags: dir_flags,
                })?;
                out.push((fd, ".".to_string()));
            }
        }
        let root = self.table.push(Descriptor {
            node: Node::Dir(tree::ROOT),
            flags: dir_flags,
        })?;
        out.push((root, "/".to_string()));
        Ok(out)
    }
}

fn timestamp(t: NewTimestamp) -> Option<Datetime> {
    match t {
        NewTimestamp::NoChange => None,
        NewTimestamp::Now => Some(tree::now()),
        NewTimestamp::Timestamp(t) => Some(t),
    }
}

/// `read-via-stream`: reads the file's live contents from a position, so
/// writes through other descriptors are seen. End of file is `closed`.
struct FileReader {
    file: FileRef,
    pos: u64,
}

#[wasmtime_wasi_io::async_trait]
impl wasmtime_wasi_io::poll::Pollable for FileReader {
    async fn ready(&mut self) {}
}

#[wasmtime_wasi_io::async_trait]
impl InputStream for FileReader {
    fn read(&mut self, size: usize) -> StreamResult<Bytes> {
        let file = tree::lock(&self.file);
        if self.pos >= file.len() {
            return Err(StreamError::Closed);
        }
        let bytes = Bytes::copy_from_slice(file.read_at(self.pos, size as u64));
        self.pos += bytes.len() as u64;
        Ok(bytes)
    }
}

/// `write-via-stream` (`pos` is `Some`) and `append-via-stream` (`None`:
/// every write goes at the current end). A failed write (out of space)
/// closes the stream, as `last-operation-failed` requires.
struct FileWriter {
    file: FileRef,
    pos: Option<u64>,
    closed: bool,
}

impl FileWriter {
    fn check_open(&self) -> StreamResult<()> {
        if self.closed {
            return Err(StreamError::Closed);
        }
        Ok(())
    }
}

#[wasmtime_wasi_io::async_trait]
impl wasmtime_wasi_io::poll::Pollable for FileWriter {
    async fn ready(&mut self) {}
}

#[wasmtime_wasi_io::async_trait]
impl OutputStream for FileWriter {
    fn write(&mut self, bytes: Bytes) -> StreamResult<()> {
        self.check_open()?;
        let mut file = tree::lock(&self.file);
        let result = match self.pos {
            Some(pos) => file.write_at(pos, &bytes),
            None => file.append(&bytes),
        };
        match result {
            Ok(n) => {
                if let Some(pos) = &mut self.pos {
                    *pos += n;
                }
                Ok(())
            }
            Err(code) => {
                self.closed = true;
                Err(stream_failed(code))
            }
        }
    }

    fn flush(&mut self) -> StreamResult<()> {
        self.check_open()
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        self.check_open()?;
        Ok(WRITE_PERMIT)
    }
}
