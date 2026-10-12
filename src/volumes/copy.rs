//! Copy-on-write file and tree copies, with a sparse-copy fallback.
//!
//! Volume snapshots, volume clones, and sandbox checkpoints all need "a copy
//! of these bytes that is independent of the original". On a filesystem that
//! supports reflinks (btrfs, XFS with `reflink=1`, bcachefs, APFS) that copy
//! is instant and shares blocks until either side writes: Linux `FICLONE`, or
//! `clonefile(2)` on macOS. Everywhere else it is a sparse copy: runs of zero
//! bytes become holes, so a mostly-empty 20 GiB disk image costs what was
//! actually written, not 20 GiB.
//!
//! The caller learns which one happened ([`CopyMethod`]) so it can report it.
//! A reflink copy is never a correctness requirement, only a speed one.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::prelude::*;

/// How a copy was made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyMethod {
    /// Copy-on-write clone: instant, shares blocks with the source.
    Reflink,
    /// Byte copy that skips zero runs (holes stay holes).
    SparseCopy,
}

impl CopyMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            CopyMethod::Reflink => "reflink",
            CopyMethod::SparseCopy => "copy",
        }
    }

    /// The weaker of two methods, for a tree where some files cloned and some
    /// did not.
    fn weakest(self, other: CopyMethod) -> CopyMethod {
        if self == CopyMethod::SparseCopy || other == CopyMethod::SparseCopy {
            CopyMethod::SparseCopy
        } else {
            CopyMethod::Reflink
        }
    }
}

/// Block size the sparse copy inspects for zeros. Matches the common
/// filesystem block size, so a hole is never smaller than what the
/// filesystem could represent.
const SPARSE_BLOCK: usize = 4096;
/// Bytes read per I/O call during a sparse copy.
const COPY_CHUNK: usize = 1024 * 1024;

/// Copy `src` to `dst` (which must not exist), preferring a reflink.
pub fn clone_file(src: &Path, dst: &Path) -> R<CopyMethod> {
    clone_file_with(src, dst, true)
}

/// [`clone_file`] with the reflink attempt switchable, so tests can exercise
/// the sparse path on a filesystem that would otherwise reflink.
pub(crate) fn clone_file_with(src: &Path, dst: &Path, allow_reflink: bool) -> R<CopyMethod> {
    if allow_reflink && try_reflink(src, dst) {
        return Ok(CopyMethod::Reflink);
    }
    sparse_copy(src, dst)?;
    Ok(CopyMethod::SparseCopy)
}

/// Copy a directory tree. Regular files are cloned, symlinks are recreated
/// (never followed), and directory permissions are preserved. `dst` must not
/// exist.
pub fn clone_tree(src: &Path, dst: &Path) -> R<CopyMethod> {
    clone_tree_with(src, dst, true)
}

pub(crate) fn clone_tree_with(src: &Path, dst: &Path, allow_reflink: bool) -> R<CopyMethod> {
    let meta = std::fs::symlink_metadata(src)
        .with_context(|| format!("failed to stat {}", src.display()))?;
    if !meta.is_dir() {
        bail!("{} is not a directory", src.display());
    }
    std::fs::create_dir_all(dst).with_context(|| format!("failed to create {}", dst.display()))?;
    let mut method = CopyMethod::Reflink;
    for entry in
        std::fs::read_dir(src).with_context(|| format!("failed to read {}", src.display()))?
    {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let kind = entry.file_type()?;
        if kind.is_dir() {
            method = method.weakest(clone_tree_with(&from, &to, allow_reflink)?);
        } else if kind.is_symlink() {
            let target = std::fs::read_link(&from)?;
            #[cfg(unix)]
            std::os::unix::fs::symlink(&target, &to)
                .with_context(|| format!("failed to recreate symlink {}", to.display()))?;
            #[cfg(not(unix))]
            bail!("cannot recreate symlink {} on this OS", to.display());
        } else if kind.is_file() {
            method = method.weakest(clone_file_with(&from, &to, allow_reflink)?);
        }
        // Sockets, FIFOs, and device nodes are skipped: none of them carry
        // data a snapshot could restore.
    }
    std::fs::set_permissions(dst, meta.permissions())
        .with_context(|| format!("failed to chmod {}", dst.display()))?;
    Ok(method)
}

#[cfg(target_os = "linux")]
fn try_reflink(src: &Path, dst: &Path) -> bool {
    use std::os::unix::io::AsRawFd;
    let Ok(source) = File::open(src) else {
        return false;
    };
    let Ok(dest) = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dst)
    else {
        return false;
    };
    // SAFETY: both descriptors are open for the duration of the call, and
    // FICLONE takes the source fd as its integer argument.
    let rc = unsafe { libc::ioctl(dest.as_raw_fd(), libc::FICLONE, source.as_raw_fd()) };
    if rc == 0 {
        if let Ok(meta) = source.metadata() {
            let _ = dest.set_permissions(meta.permissions());
        }
        return true;
    }
    // EOPNOTSUPP / EXDEV / EINVAL: the filesystem cannot clone. Remove the
    // empty destination so the sparse copy can create it.
    drop(dest);
    let _ = std::fs::remove_file(dst);
    false
}

#[cfg(target_os = "macos")]
fn try_reflink(src: &Path, dst: &Path) -> bool {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let (Ok(from), Ok(to)) = (
        CString::new(src.as_os_str().as_bytes()),
        CString::new(dst.as_os_str().as_bytes()),
    ) else {
        return false;
    };
    // SAFETY: both arguments are valid NUL-terminated paths. clonefile
    // preserves mode and fails (without side effects) off APFS.
    unsafe { libc::clonefile(from.as_ptr(), to.as_ptr(), 0) == 0 }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn try_reflink(_src: &Path, _dst: &Path) -> bool {
    false
}

/// Byte copy that leaves zero blocks as holes.
fn sparse_copy(src: &Path, dst: &Path) -> R<()> {
    let mut source =
        File::open(src).with_context(|| format!("failed to open {}", src.display()))?;
    let len = source.metadata()?.len();
    let mut dest = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dst)
        .with_context(|| format!("failed to create {}", dst.display()))?;
    let mut buf = vec![0u8; COPY_CHUNK];
    let zero = [0u8; SPARSE_BLOCK];
    loop {
        let read = read_full(&mut source, &mut buf)?;
        if read == 0 {
            break;
        }
        for block in buf[..read].chunks(SPARSE_BLOCK) {
            if block == &zero[..block.len()] {
                dest.seek(SeekFrom::Current(block.len() as i64))?;
            } else {
                dest.write_all(block)?;
            }
        }
    }
    // A trailing hole is only a seek so far; setting the length makes it
    // real (and is a no-op when the file ended on data).
    dest.set_len(len)?;
    dest.sync_all()?;
    let perms = source.metadata()?.permissions();
    dest.set_permissions(perms)?;
    Ok(())
}

/// Logical bytes of the regular files under `path` (symlinks not followed).
/// Best effort: unreadable entries count as zero.
pub fn tree_size_bytes(path: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(next) = stack.pop() {
        let Ok(meta) = std::fs::symlink_metadata(&next) else {
            continue;
        };
        if meta.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&next) {
                stack.extend(entries.flatten().map(|entry| entry.path()));
            }
        } else if meta.is_file() {
            total += meta.len();
        }
    }
    total
}

fn read_full(source: &mut File, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match source.read(&mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    Ok(filled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_copy_preserves_content_and_length() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let src = tmp.path().join("disk.img");
        let mut file = File::create(&src)?;
        file.write_all(b"head")?;
        file.seek(SeekFrom::Start(3 * 1024 * 1024))?;
        file.write_all(b"middle")?;
        file.set_len(8 * 1024 * 1024)?;
        drop(file);

        let dst = tmp.path().join("copy.img");
        assert_eq!(clone_file_with(&src, &dst, false)?, CopyMethod::SparseCopy);
        assert_eq!(std::fs::read(&dst)?, std::fs::read(&src)?);
        Ok(())
    }

    #[test]
    fn clone_file_refuses_to_overwrite() -> R<()> {
        let tmp = tempfile::tempdir()?;
        let src = tmp.path().join("a");
        let dst = tmp.path().join("b");
        std::fs::write(&src, b"a")?;
        std::fs::write(&dst, b"b")?;
        assert!(clone_file(&src, &dst).is_err());
        assert_eq!(std::fs::read(&dst)?, b"b");
        Ok(())
    }

    #[test]
    fn clone_tree_copies_files_dirs_and_symlinks_either_way() -> R<()> {
        for allow_reflink in [true, false] {
            let tmp = tempfile::tempdir()?;
            let src = tmp.path().join("src");
            std::fs::create_dir_all(src.join("nested"))?;
            std::fs::write(src.join("marker"), b"v1")?;
            std::fs::write(src.join("nested/deep"), b"deep")?;
            #[cfg(unix)]
            std::os::unix::fs::symlink("marker", src.join("link"))?;

            let dst = tmp.path().join("dst");
            let method = clone_tree_with(&src, &dst, allow_reflink)?;
            if !allow_reflink {
                assert_eq!(method, CopyMethod::SparseCopy);
            }
            assert_eq!(std::fs::read(dst.join("marker"))?, b"v1");
            assert_eq!(std::fs::read(dst.join("nested/deep"))?, b"deep");
            #[cfg(unix)]
            assert_eq!(
                std::fs::read_link(dst.join("link"))?,
                Path::new("marker").to_path_buf()
            );

            // The copy is independent: writing one side leaves the other.
            std::fs::write(src.join("marker"), b"v2")?;
            assert_eq!(std::fs::read(dst.join("marker"))?, b"v1");
        }
        Ok(())
    }
}
