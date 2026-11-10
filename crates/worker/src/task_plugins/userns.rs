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

/// Lets this allocation's container root, and only it, traverse the
/// allocation directory: group = the range's base gid (container gid 0),
/// mode `0710`. Docker's `userns-remap` guards its per-range root the same
/// way.
///
/// The OCI runtime resolves the bundle's `rootfs` path *as container root*
/// (runc's `remount-propagation` of the rootfs runs inside the user
/// namespace), so container root needs search permission on every directory
/// above it. The worker owns that whole chain: the state dir and `allocs/`
/// are traverse-only, and this is the one directory that holds the
/// allocation's private files. Other allocations' roots are other gids, so
/// they still cannot enter. The task directory inside stays owner-only: it
/// reaches the container as an idmapped mount, which the runtime opens with
/// the worker's privilege.
///
/// Re-run at every start, because the worker resets the directory to
/// owner-only before a start (an in-place restart may turn the user
/// namespace off).
pub fn grant_traversal(alloc_dir: &Path, range: IdRange) -> R<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::os::unix::fs::chown(alloc_dir, None, Some(range.host_base)).with_context(|| {
            format!(
                "hand {} to the allocation's user namespace group {}",
                alloc_dir.display(),
                range.host_base
            )
        })?;
        std::fs::set_permissions(alloc_dir, std::fs::Permissions::from_mode(0o710))
            .with_context(|| format!("restrict {} to 0710", alloc_dir.display()))?;
    }
    #[cfg(not(unix))]
    let _ = (alloc_dir, range);
    Ok(())
}

/// Fails, naming the directory and the fix, when this range's container root
/// cannot search every directory above `path`.
///
/// The worker sets the modes of the directories it creates
/// ([`grant_traversal`], `WorkerStateStore::prepare_layout`), but not of the
/// ones above its data directory. A data directory under `/root` (`0700`)
/// would otherwise surface as the runtime's bare `permission denied` from
/// deep inside its rootfs setup.
pub fn check_traversable(path: &Path, range: IdRange) -> R<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let mut chain: Vec<&Path> = path.ancestors().skip(1).collect();
        chain.reverse();
        for dir in chain {
            let Ok(meta) = std::fs::metadata(dir) else {
                continue;
            };
            let mode = meta.mode();
            if !may_search(mode, meta.uid(), meta.gid(), range.host_base) {
                bail!(
                    "{dir} (mode {mode:o}) does not let the container's user namespace root \
                     (host uid {uid}) through, so the OCI runtime cannot reach the bundle under \
                     it. Make it traversable (`chmod o+x {dir}`), move the worker's data \
                     directory ({env}) somewhere that is, or set security.userNamespace = false \
                     on the task",
                    dir = dir.display(),
                    mode = mode & 0o7777,
                    uid = range.host_base,
                    env = crate::constants::DATA_DIR_ENV,
                );
            }
        }
    }
    #[cfg(not(unix))]
    let _ = (path, range);
    Ok(())
}

/// Whether a process whose uid and gid are both `id` (container root of a
/// range, which has no supplementary groups) may search a directory with
/// this `mode`, `owner`, and `group`. The kernel checks one class only: the
/// owner bits for the owner, else the group bits for the group, else other.
fn may_search(mode: u32, owner: u32, group: u32, id: u32) -> bool {
    let bit = if owner == id {
        0o100
    } else if group == id {
        0o010
    } else {
        0o001
    };
    mode & bit != 0
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

    /// The layout the worker builds lets a range's root through to its own
    /// allocation and nowhere else (spot check: runc failed every userns
    /// start with EACCES under the old owner-only chain).
    #[test]
    fn only_the_allocation_s_own_root_may_search_its_dir_test() {
        let own = IdRange::for_slot(0).host_base;
        let other = IdRange::for_slot(1).host_base;
        // State dir and allocs/: traverse-only, owned by the worker (root).
        assert!(may_search(0o711, 0, 0, own));
        // The old owner-only chain.
        assert!(!may_search(0o700, 0, 0, own));
        // The allocation dir after `grant_traversal`: 0710, group = range.
        assert!(may_search(0o710, 0, own, own));
        assert!(
            !may_search(0o710, 0, own, other),
            "another allocation's root"
        );
        // The owner class wins even when it denies and other would allow.
        assert!(!may_search(0o011, own, 0, own));
    }

    #[cfg(unix)]
    #[test]
    fn check_traversable_names_the_blocking_dir_test() -> R<()> {
        use std::os::unix::fs::PermissionsExt;
        // A range whose root is this test's own uid, so the temp dir's
        // ancestors (owned by this user or world-searchable) pass and only
        // the directory under test decides.
        let me = IdRange {
            // SAFETY: geteuid has no preconditions and cannot fail.
            host_base: unsafe { libc::geteuid() },
            size: ID_RANGE_SIZE,
        };
        let root = tempfile::tempdir()?;
        let blocking = root.path().join("blocking");
        let rootfs = blocking.join("bundle").join("rootfs");
        std::fs::create_dir_all(&rootfs)?;
        check_traversable(&rootfs, me)?;

        std::fs::set_permissions(&blocking, std::fs::Permissions::from_mode(0o600))?;
        let err = check_traversable(&rootfs, me).expect_err("no search bit");
        std::fs::set_permissions(&blocking, std::fs::Permissions::from_mode(0o700))?;
        let message = err.to_string();
        assert!(
            message.contains(&blocking.display().to_string()),
            "{message}"
        );
        assert!(
            message.contains("security.userNamespace = false"),
            "{message}"
        );
        Ok(())
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
