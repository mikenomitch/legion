//! Per-allocation user namespace ranges for Linux containers
//! (worker-plane-auth plan §6.14; docs/task-runtime-container.md,
//! "Security").
//!
//! Container ids `0-65535` map to a 65,536-id range on the host that belongs
//! to one allocation alone, so container root is an unprivileged host id that
//! no other allocation, and no host user, shares. The worker owns the pool:
//! ids from [`ID_POOL_BASE`] for [`ID_POOL_SLOTS`] ranges. An allocation's
//! slot is recorded in its private directory (`<alloc dir>/.userns-slot`), so
//! an in-place restart keeps it, and it is freed with that directory. Claims
//! take a node-wide lock (`<allocs root>/.userns.lock`) and pick the lowest
//! slot no live allocation holds.

use crate::prelude::*;
use std::path::Path;

/// Ids per allocation: the container's `0-65535`.
pub const ID_RANGE_SIZE: u32 = 65_536;
/// The first host id the worker hands out. Above the ranges distributions
/// give to users in `/etc/subuid` (100000 and up, a few at a time).
pub const ID_POOL_BASE: u32 = 1 << 20;
/// How many allocations on one node can hold a range at once.
pub const ID_POOL_SLOTS: u32 = 4096;

const SLOT_FILE: &str = ".userns-slot";
const LOCK_FILE: &str = ".userns.lock";

/// One allocation's host id range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdRange {
    pub host_base: u32,
    pub size: u32,
}

impl IdRange {
    pub fn for_slot(slot: u32) -> Self {
        IdRange {
            host_base: ID_POOL_BASE + slot * ID_RANGE_SIZE,
            size: ID_RANGE_SIZE,
        }
    }

    /// The OCI `uidMappings`/`gidMappings` entry.
    pub fn oci_mapping(&self) -> Value {
        json!([{ "containerID": 0, "hostID": self.host_base, "size": self.size }])
    }

    /// A container-side id as the host sees it.
    pub fn shift(&self, id: u64) -> Option<u32> {
        let id = u32::try_from(id).ok()?;
        (id < self.size).then(|| self.host_base + id)
    }
}

/// The slot recorded in an allocation directory, if any.
fn read_slot(alloc_dir: &Path) -> Option<u32> {
    std::fs::read_to_string(alloc_dir.join(SLOT_FILE))
        .ok()?
        .trim()
        .parse()
        .ok()
        .filter(|slot| *slot < ID_POOL_SLOTS)
}

/// The lowest slot not in `used`.
pub fn lowest_free(used: &std::collections::BTreeSet<u32>) -> Option<u32> {
    (0..ID_POOL_SLOTS).find(|slot| !used.contains(slot))
}

/// Claims this allocation's range: the slot already recorded in
/// `alloc_dir` (an in-place restart), or the lowest slot no sibling
/// allocation directory holds.
pub fn claim(alloc_dir: &Path) -> R<IdRange> {
    if let Some(slot) = read_slot(alloc_dir) {
        return Ok(IdRange::for_slot(slot));
    }
    let root = alloc_dir
        .parent()
        .context("allocation directory has no parent to keep the id pool in")?;
    std::fs::create_dir_all(alloc_dir)
        .with_context(|| format!("create {}", alloc_dir.display()))?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join(LOCK_FILE))
        .context("open the user namespace pool lock")?;
    let _held = FileLock::exclusive(&lock)?;
    // Re-read under the lock: a concurrent start of the same allocation may
    // have claimed already.
    if let Some(slot) = read_slot(alloc_dir) {
        return Ok(IdRange::for_slot(slot));
    }
    let mut used = std::collections::BTreeSet::new();
    for entry in std::fs::read_dir(root)?.flatten() {
        if entry.path() != alloc_dir {
            if let Some(slot) = read_slot(&entry.path()) {
                used.insert(slot);
            }
        }
    }
    let slot = lowest_free(&used).ok_or_else(|| {
        anyhow!(
            "no free user namespace range on this node: all {ID_POOL_SLOTS} are held by \
             running allocations"
        )
    })?;
    std::fs::write(alloc_dir.join(SLOT_FILE), slot.to_string())
        .context("record the allocation's user namespace slot")?;
    Ok(IdRange::for_slot(slot))
}

/// An exclusive `flock`, released on drop.
struct FileLock<'a>(&'a std::fs::File);

impl<'a> FileLock<'a> {
    fn exclusive(file: &'a std::fs::File) -> R<Self> {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: flock on a file descriptor we own for the lock's life.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
                return Err(std::io::Error::last_os_error()).context("lock the id pool");
            }
        }
        Ok(FileLock(file))
    }
}

impl Drop for FileLock<'_> {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: as above.
            unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
        }
    }
}

#[cfg(test)]
mod userns_test {
    use super::*;

    #[test]
    fn ranges_are_disjoint_and_above_the_user_ranges_test() {
        let first = IdRange::for_slot(0);
        let second = IdRange::for_slot(1);
        assert_eq!(first.host_base, 1_048_576);
        assert_eq!(second.host_base, first.host_base + ID_RANGE_SIZE);
        assert_eq!(first.shift(0), Some(1_048_576));
        assert_eq!(first.shift(65_535), Some(1_048_576 + 65_535));
        assert_eq!(first.shift(65_536), None, "outside the container's ids");
        let last = IdRange::for_slot(ID_POOL_SLOTS - 1);
        assert!(u64::from(last.host_base) + u64::from(last.size) < u64::from(u32::MAX));
    }

    #[test]
    fn claims_take_the_lowest_free_slot_and_survive_a_restart_test() -> R<()> {
        let root = tempfile::tempdir()?;
        let a = root.path().join("1");
        let b = root.path().join("2");
        let c = root.path().join("3");
        assert_eq!(claim(&a)?, IdRange::for_slot(0));
        assert_eq!(claim(&b)?, IdRange::for_slot(1));
        // An in-place restart of `a` keeps its range.
        assert_eq!(claim(&a)?, IdRange::for_slot(0));
        // `a` is destroyed: its slot is free again.
        std::fs::remove_dir_all(&a)?;
        assert_eq!(claim(&c)?, IdRange::for_slot(0));
        Ok(())
    }
}
