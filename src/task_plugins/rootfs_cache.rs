//! The node's cache of sandbox **base root disks** (docs/task-runtime-sandbox.md,
//! "The root disk cache"; plan: internal_docs/plans/rootfs-base-cache-plan.md).
//!
//! A microVM boots from an ext4 root disk that Legion builds from the image:
//! extract every layer, then `mkfs.ext4 -d` the tree into a raw image. That is
//! seconds to tens of seconds of CPU and disk per start, and without this
//! cache every sandbox of an image paid it, though all of them carry the same
//! image bytes.
//!
//! # The split
//!
//! A root disk is two parts:
//!
//! - **The base**: the image's layers, Legion's `/legion` directory, and the
//!   guest binary. It is the same for every sandbox of the image on this node,
//!   so it is built once and kept here.
//! - **The sandbox's own files**: `/legion/init`, `/legion/env`, `/legion/cwd`,
//!   `/etc/hostname`, and a guest's own `/etc/resolv.conf`. They carry the
//!   allocation's identity and its env, which can hold secrets, so they are
//!   written into each sandbox's own copy after it is made
//!   ([`microvm::inject_guest_payload`]), and never into a base.
//!
//! A start clones the base (a reflink on btrfs, XFS, and APFS, a sparse copy
//! elsewhere) and writes its own files into the clone. The base is never
//! written after it is committed, and a clone shares nothing with it that a
//! write can reach.
//!
//! # Layout
//!
//! ```text
//! <worker state dir>/rootfs-cache/
//!   <key>/rootfs.img   the base (mode 0444)
//!   <key>/base.json    what it was built from; its mtime is the last use
//!   tmp/               fills in progress, cleared when the worker starts
//! ```
//!
//! It sits beside the allocation dirs so a clone can be a reflink. The key is
//! a hash of everything that decides a base's bytes ([`BaseInputs`]): the
//! image's layer digests, the guest binary, and the task's
//! `rootfs_headroom_mib`. Env, hostname, command, and volumes are not in it,
//! because none of them are in the base.
//!
//! # Filling it
//!
//! The cache fills on demand, from real starts. A miss is never slower than a
//! start without the cache: the start builds its disk exactly as before, and
//! then the cache keeps a copy of it as a reflink (instant). Where the
//! filesystem cannot reflink, the cache formats the extracted tree a second
//! time on a background thread, after the start has its disk.
//!
//! Fills are single-flight per key. Starts that arrive while one is in flight
//! wait for it and then clone, rather than each building its own disk at the
//! same time. [`ensure_image_base`] uses the same path, so a background
//! prefetch and a real start of the same image can run at once.
//!
//! # Bounds
//!
//! The cache holds at most its byte budget (`LEGION_SANDBOX_ROOTFS_CACHE`,
//! default 10% of the filesystem, clamped to 1 to 20 GiB), counted in blocks
//! actually allocated. A commit evicts the least recently used bases first,
//! a base larger than the whole budget is never kept, and none is kept while
//! the disk is below the content store's `minFreeDisk`. A clone is
//! independent of its base, so evicting a base never touches a running
//! sandbox.

use crate::content_store::BudgetSpec;
use crate::prelude::*;
use crate::task_plugins::microvm::{self, BaseFacts, GuestBinary};
use crate::task_plugins::oci_bundle;
use crate::volumes::copy::{self, CopyMethod};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Condvar, RwLock};
use std::time::{Instant, SystemTime};

/// The worker's root disk cache budget: a size (`"20GiB"`), a share of the
/// filesystem holding the worker state dir (`"10%"`), or `0` / `off` to turn
/// the cache off.
pub(crate) const CACHE_ENV: &str = "LEGION_SANDBOX_ROOTFS_CACHE";

const GIB: u64 = 1024 * 1024 * 1024;

/// The default budget: 10% of the filesystem, clamped to 1..20 GiB. A base
/// costs about its image's unpacked size, so 20 GiB holds a dozen or more
/// typical sandbox images, and 1 GiB still holds a slim one.
pub(crate) const DEF_BUDGET: BudgetSpec = BudgetSpec::Share {
    percent: 10.0,
    min: GIB,
    max: 20 * GIB,
};

/// Bumped whenever what goes into a base changes, so a worker never clones a
/// base that an older build laid out differently.
const FORMAT: &str = "1";
const IMAGE_FILE: &str = "rootfs.img";
const META_FILE: &str = "base.json";
const TMP_DIR: &str = "tmp";

/// Everything that decides a base's bytes, and so its cache key.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BaseInputs {
    /// The image's layer digests, bottom first.
    pub layers: Vec<String>,
    /// The guest binary's fingerprint (path, size, mtime): a new binary is a
    /// new base, so it reaches the guest.
    pub guest_binary: String,
    /// The task's `rootfs_headroom_mib`, which sizes the disk.
    pub headroom_mib: u64,
}

impl BaseInputs {
    /// The cache key: a digest of the inputs and the base format.
    pub(crate) fn key(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"legion-rootfs-base\0");
        hasher.update(FORMAT.as_bytes());
        for layer in &self.layers {
            hasher.update(b"\0layer\0");
            hasher.update(layer.as_bytes());
        }
        hasher.update(b"\0guest-binary\0");
        hasher.update(self.guest_binary.as_bytes());
        hasher.update(b"\0headroom\0");
        hasher.update(self.headroom_mib.to_string().as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

/// How a base is produced. The real one ([`ImageBase`]) extracts an image
/// layout and runs `mkfs.ext4`; tests substitute one that needs neither.
pub(crate) trait BaseBuilder: Send + Sync {
    /// Lays the base's files out in `tree`, which does not exist yet.
    fn tree(&self, tree: &Path) -> R<BaseFacts>;
    /// Formats `tree` into a disk image at `image`.
    fn format(&self, tree: &Path, image: &Path) -> R<()>;
}

/// The base of one image: its layers, plus Legion's `/legion` and guest
/// binary, formatted with the task's headroom.
pub(crate) struct ImageBase {
    pub layout: PathBuf,
    pub guest_binary: GuestBinary,
    pub headroom_mib: u64,
}

impl BaseBuilder for ImageBase {
    fn tree(&self, tree: &Path) -> R<BaseFacts> {
        oci_bundle::materialize_rootfs(&self.layout, tree)?;
        microvm::install_guest_base(tree, &self.guest_binary)
    }

    fn format(&self, tree: &Path, image: &Path) -> R<()> {
        microvm::format_root_disk(tree, image, self.headroom_mib)
    }
}

impl ImageBase {
    /// The cache inputs for this base, given the image's config (already
    /// read from the same layout).
    pub(crate) fn inputs(&self, image: &oci_bundle::ImageConfig) -> BaseInputs {
        BaseInputs {
            layers: image.layer_digests(),
            guest_binary: self.guest_binary.fingerprint.clone(),
            headroom_mib: self.headroom_mib,
        }
    }
}

/// Where a sandbox's root disk came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// Cloned from the node's cached base.
    Hit(CopyMethod),
    /// Built for this start, and what the cache did with it.
    Built(Fill),
}

/// What a miss did for the next start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fill {
    /// The cache kept a reflink of the new disk: no bytes copied.
    Reflink,
    /// The filesystem cannot reflink, so the cache is formatting the
    /// extracted tree again on a background thread.
    Background,
    /// Nothing was kept, for the reason given.
    Skipped(&'static str),
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Outcome::Hit(method) => write!(f, "cached base ({})", method.as_str()),
            Outcome::Built(Fill::Reflink) => f.write_str("built, kept as the cached base"),
            Outcome::Built(Fill::Background) => {
                f.write_str("built, caching the base in the background")
            }
            Outcome::Built(Fill::Skipped(reason)) => write!(f, "built, not cached: {reason}"),
        }
    }
}

/// What [`ensure_image_base`] found or did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ensured {
    /// The base was already on this node (or a concurrent fill just made it).
    Present,
    /// This call built and kept it.
    Built,
    /// The base is not cached, for the reason given.
    Skipped(&'static str),
    /// This node has no root disk cache.
    Disabled,
}

/// One fill in flight. Waiters block until it is done, whatever happened.
#[derive(Default)]
struct Flight {
    done: Mutex<bool>,
    finished: Condvar,
}

impl Flight {
    fn wait(&self) {
        let mut done = self.done.lock().unwrap_or_else(|p| p.into_inner());
        while !*done {
            done = self.finished.wait(done).unwrap_or_else(|p| p.into_inner());
        }
    }
}

/// The right to fill one key. Dropping it (on success, failure, or a skip)
/// wakes everyone waiting for that key.
struct FlightGuard {
    cache: Arc<RootfsCache>,
    key: String,
    flight: Arc<Flight>,
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        self.cache
            .flights
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.key);
        *self.flight.done.lock().unwrap_or_else(|p| p.into_inner()) = true;
        self.flight.finished.notify_all();
    }
}

/// A committed base.
struct Entry {
    image: PathBuf,
    meta_path: PathBuf,
    meta: BaseMeta,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct BaseMeta {
    format: String,
    inputs: BaseInputs,
    facts: BaseFacts,
}

/// The node's base root disk cache.
pub(crate) struct RootfsCache {
    root: PathBuf,
    budget: u64,
    min_free: u64,
    /// Serializes commits and eviction. Lookups and clones do not take it:
    /// a base evicted between a lookup and its clone fails the clone, and
    /// the start falls back to a build.
    commit_lock: Mutex<()>,
    flights: Mutex<BTreeMap<String, Arc<Flight>>>,
    /// Keys whose base is larger than the whole budget. Their starts build
    /// side by side instead of queueing behind a fill that cannot succeed.
    oversized: Mutex<BTreeSet<String>>,
    /// Try reflinks. Always on outside tests, which turn it off to take the
    /// path a filesystem without them takes, wherever they run.
    reflinks: bool,
}

impl std::fmt::Debug for RootfsCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RootfsCache")
            .field("root", &self.root)
            .field("budget", &self.budget)
            .field("min_free", &self.min_free)
            .finish()
    }
}

static NODE_CACHE: RwLock<Option<Arc<RootfsCache>>> = RwLock::new(None);

/// This worker's cache, once [`init`] opened it.
pub(crate) fn node_cache() -> Option<Arc<RootfsCache>> {
    NODE_CACHE.read().unwrap_or_else(|p| p.into_inner()).clone()
}

/// Reads [`CACHE_ENV`]: `None` turns the cache off.
fn budget_spec_from_env() -> Option<BudgetSpec> {
    let Ok(raw) = std::env::var(CACHE_ENV) else {
        return Some(DEF_BUDGET);
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Some(DEF_BUDGET);
    }
    if raw.eq_ignore_ascii_case("off") || raw == "0" {
        return None;
    }
    match BudgetSpec::parse(raw) {
        Ok(BudgetSpec::Bytes(0)) => None,
        Ok(spec) => Some(spec),
        Err(err) => {
            warn!(
                value = raw,
                error = %format!("{err:#}"),
                "Invalid {CACHE_ENV} (expected a size such as \"20GiB\", a share such as \
                 \"10%\", or \"off\"); using the default"
            );
            Some(DEF_BUDGET)
        }
    }
}

/// Opens the worker's cache at `root` (beside its allocation dirs) and
/// registers it for sandbox starts. `min_free` is the content store's
/// `minFreeDisk`. A cache that cannot open is logged and left off: starts
/// then build every root disk, as they would without it.
pub(crate) fn init(root: &Path, min_free: u64) {
    let Some(spec) = budget_spec_from_env() else {
        info!("Sandbox root disk cache off ({CACHE_ENV})");
        return;
    };
    let _ = std::fs::create_dir_all(root);
    let budget = match crate::content_store::filesystem_bytes(root) {
        Some(total) => spec.resolve(total),
        None => match spec {
            BudgetSpec::Bytes(bytes) => bytes,
            BudgetSpec::Share { min, .. } => min,
        },
    };
    match RootfsCache::open(root, budget, min_free) {
        Ok(cache) => {
            info!(
                dir = %root.display(),
                budget_bytes = budget,
                "Sandbox root disk cache open"
            );
            *NODE_CACHE.write().unwrap_or_else(|p| p.into_inner()) = Some(cache);
        }
        Err(err) => warn!(
            dir = %root.display(),
            error = %format!("{err:#}"),
            "Could not open the sandbox root disk cache; every sandbox start builds its root disk"
        ),
    }
}

/// Makes sure this node holds the base root disk of `image` with
/// `headroom_mib` (the sandbox default when `None`): a no-op when it does,
/// a pull and a build when it does not.
///
/// This is the seam for warming a node ahead of its starts. It is
/// idempotent and safe to call from a background prefetch while real starts
/// of the same image run: a fill in flight for the same base, from either
/// side, is waited for, never repeated. It fills the same budget-bounded
/// cache as starts do, and needs the same e2fsprogs. Blocking: call it from
/// a blocking thread.
pub fn ensure_image_base(image: &str, headroom_mib: Option<u32>) -> R<Ensured> {
    let Some(cache) = node_cache() else {
        return Ok(Ensured::Disabled);
    };
    let pull = cache.stage()?;
    let result = (|| {
        let layout = crate::task_plugins::oci_image::pull_layout(image, &pull)?;
        ensure_layout_base(&layout, headroom_mib)
    })();
    let _ = std::fs::remove_dir_all(&pull);
    result
}

/// [`ensure_image_base`] for an image already on disk as an OCI layout
/// (one the artifact pipeline staged, say).
pub fn ensure_layout_base(layout: &Path, headroom_mib: Option<u32>) -> R<Ensured> {
    let Some(cache) = node_cache() else {
        return Ok(Ensured::Disabled);
    };
    let image = oci_bundle::read_image_config(layout)?;
    let base = ImageBase {
        layout: layout.to_path_buf(),
        guest_binary: microvm::guest_binary()?,
        headroom_mib: u64::from(
            headroom_mib.unwrap_or(crate::task_plugins::sandbox::DEFAULT_ROOTFS_HEADROOM_MIB),
        ),
    };
    let inputs = base.inputs(&image);
    cache.ensure(Arc::new(base), &inputs)
}

/// Extracts the base into `tree` and formats it at `dest`. The caller
/// removes `tree`.
fn build_at(builder: &dyn BaseBuilder, dest: &Path, tree: &Path) -> R<BaseFacts> {
    let _ = std::fs::remove_dir_all(tree);
    let facts = builder.tree(tree)?;
    let _ = std::fs::remove_file(dest);
    builder.format(tree, dest)?;
    Ok(facts)
}

/// Blocks actually allocated to `path`: what a sparse image costs the disk.
fn allocated_bytes(path: &Path) -> u64 {
    let Ok(meta) = std::fs::metadata(path) else {
        return 0;
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        meta.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    meta.len()
}

fn set_mode(path: &Path, mode: u32) -> R<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .with_context(|| format!("chmod {}", path.display()))?;
    }
    #[cfg(not(unix))]
    let _ = (path, mode);
    Ok(())
}

impl RootfsCache {
    /// Opens the cache at `root` with a byte budget, clearing fills a
    /// previous worker left half done and bases this build cannot use.
    pub(crate) fn open(root: &Path, budget: u64, min_free: u64) -> R<Arc<Self>> {
        Self::open_with(root, budget, min_free, true)
    }

    fn open_with(root: &Path, budget: u64, min_free: u64, reflinks: bool) -> R<Arc<Self>> {
        std::fs::create_dir_all(root)
            .with_context(|| format!("create root disk cache {}", root.display()))?;
        let tmp = root.join(TMP_DIR);
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        let cache = Arc::new(RootfsCache {
            root: root.to_path_buf(),
            budget,
            min_free,
            commit_lock: Mutex::new(()),
            flights: Mutex::new(BTreeMap::new()),
            oversized: Mutex::new(BTreeSet::new()),
            reflinks,
        });
        for entry in std::fs::read_dir(root)?.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == TMP_DIR {
                continue;
            }
            if cache.lookup(&name).is_none() {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
        // The budget may have shrunk since the last run.
        {
            let _guard = cache.commit_lock.lock().unwrap_or_else(|p| p.into_inner());
            cache.evict_to(budget);
        }
        Ok(cache)
    }

    /// A fresh directory under `tmp/`, on the cache's filesystem.
    fn stage(&self) -> R<PathBuf> {
        let dir = self.root.join(TMP_DIR).join(crate::util::random_string(12));
        std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        Ok(dir)
    }

    fn lookup(&self, key: &str) -> Option<Entry> {
        let dir = self.root.join(key);
        let meta_path = dir.join(META_FILE);
        let meta: BaseMeta = serde_json::from_slice(&std::fs::read(&meta_path).ok()?).ok()?;
        let image = dir.join(IMAGE_FILE);
        (meta.format == FORMAT && meta.inputs.key() == key && image.is_file()).then_some(Entry {
            image,
            meta_path,
            meta,
        })
    }

    /// Marks a base used now: eviction goes by this.
    fn touch(entry: &Entry) {
        if let Ok(file) = std::fs::File::options().write(true).open(&entry.meta_path) {
            let _ = file.set_modified(SystemTime::now());
        }
    }

    /// Clones a base to `dest` for one sandbox.
    fn clone_entry(&self, entry: &Entry, dest: &Path) -> R<CopyMethod> {
        let _ = std::fs::remove_file(dest);
        let method = copy::clone_file_with(&entry.image, dest, self.reflinks)?;
        // The base is read-only. The clone is this sandbox's disk.
        set_mode(dest, 0o600)?;
        Self::touch(entry);
        Ok(method)
    }

    /// Takes the right to fill `key`, or, when a fill is already in flight,
    /// waits for it to finish and returns `None`.
    fn claim(self: &Arc<Self>, key: &str) -> Option<FlightGuard> {
        let mut flights = self.flights.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(flight) = flights.get(key).cloned() {
            drop(flights);
            flight.wait();
            return None;
        }
        let flight = Arc::new(Flight::default());
        flights.insert(key.to_string(), flight.clone());
        Some(FlightGuard {
            cache: self.clone(),
            key: key.to_string(),
            flight,
        })
    }

    fn is_oversized(&self, key: &str) -> bool {
        self.oversized
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(key)
    }

    /// Puts a base root disk at `dest` for a sandbox start: cloned from the
    /// cache when it holds one, built (and offered to the cache) otherwise.
    /// `tree` is scratch space for the extracted image. The caller writes
    /// the sandbox's own files into `dest` next.
    pub(crate) fn materialize(
        self: &Arc<Self>,
        builder: Arc<dyn BaseBuilder>,
        inputs: &BaseInputs,
        dest: &Path,
        tree: &Path,
    ) -> R<(BaseFacts, Outcome)> {
        let key = inputs.key();
        let mut waited = false;
        loop {
            if let Some(entry) = self.lookup(&key) {
                match self.clone_entry(&entry, dest) {
                    Ok(method) => return Ok((entry.meta.facts, Outcome::Hit(method))),
                    Err(err) => {
                        // Evicted between the lookup and the clone, or the
                        // copy failed: build this one instead.
                        debug!(key, error = %format!("{err:#}"), "Could not clone the cached root disk base");
                        let _ = std::fs::remove_file(dest);
                    }
                }
            }
            if waited || self.is_oversized(&key) {
                break;
            }
            match self.claim(&key) {
                Some(guard) => return self.build_and_fill(guard, builder, inputs, dest, tree),
                // A fill for this base was in flight and has finished: look
                // again, once.
                None => waited = true,
            }
        }
        let facts = build_at(&*builder, dest, tree);
        let _ = std::fs::remove_dir_all(tree);
        let reason = if self.is_oversized(&key) {
            "the base is larger than the cache budget"
        } else {
            "the concurrent fill did not keep it"
        };
        Ok((facts?, Outcome::Built(Fill::Skipped(reason))))
    }

    /// The miss path of a start: build at `dest` exactly as an uncached
    /// start would, then keep a copy for the next one.
    fn build_and_fill(
        self: &Arc<Self>,
        guard: FlightGuard,
        builder: Arc<dyn BaseBuilder>,
        inputs: &BaseInputs,
        dest: &Path,
        tree: &Path,
    ) -> R<(BaseFacts, Outcome)> {
        let facts = match build_at(&*builder, dest, tree) {
            Ok(facts) => facts,
            Err(err) => {
                let _ = std::fs::remove_dir_all(tree);
                return Err(err);
            }
        };
        let fill = self.fill_from_build(guard, builder, inputs, facts, dest, tree);
        Ok((facts, Outcome::Built(fill)))
    }

    fn fill_from_build(
        self: &Arc<Self>,
        guard: FlightGuard,
        builder: Arc<dyn BaseBuilder>,
        inputs: &BaseInputs,
        facts: BaseFacts,
        dest: &Path,
        tree: &Path,
    ) -> Fill {
        if allocated_bytes(dest) > self.budget {
            self.oversized
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(guard.key.clone());
            let _ = std::fs::remove_dir_all(tree);
            return Fill::Skipped("the base is larger than the cache budget");
        }
        let staged = match self.stage() {
            Ok(staged) => staged,
            Err(err) => {
                warn!(error = %format!("{err:#}"), "Could not stage a root disk base");
                let _ = std::fs::remove_dir_all(tree);
                return Fill::Skipped("the cache could not stage it");
            }
        };
        // The disk was just built from exactly the base's files, before the
        // sandbox's own were written: a reflink of it *is* the base.
        if self.reflinks && copy::reflink_file(dest, &staged.join(IMAGE_FILE)) {
            let _ = std::fs::remove_dir_all(tree);
            return match self.commit(&guard.key, &staged, inputs, facts) {
                Ok(()) => Fill::Reflink,
                Err(reason) => Fill::Skipped(reason),
            };
        }
        // No reflinks on this filesystem. Copying the disk now would put the
        // copy on this start's critical path, and the disk is about to change.
        // Format the tree again off to the side instead.
        let staged_tree = staged.join("tree");
        if std::fs::rename(tree, &staged_tree).is_err() {
            let _ = std::fs::remove_dir_all(tree);
            let _ = std::fs::remove_dir_all(&staged);
            return Fill::Skipped("the extracted image is on another filesystem");
        }
        let cache = self.clone();
        let inputs = inputs.clone();
        let spawned = std::thread::Builder::new()
            .name("rootfs-cache-fill".into())
            .spawn(move || {
                let started = Instant::now();
                let image = staged.join(IMAGE_FILE);
                let formatted = builder.format(&staged_tree, &image);
                let _ = std::fs::remove_dir_all(&staged_tree);
                match formatted {
                    Ok(()) => match cache.commit(&guard.key, &staged, &inputs, facts) {
                        Ok(()) => info!(
                            key = %guard.key,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "Cached a sandbox root disk base"
                        ),
                        Err(reason) => debug!(key = %guard.key, reason, "Root disk base not cached"),
                    },
                    Err(err) => {
                        let _ = std::fs::remove_dir_all(&staged);
                        warn!(key = %guard.key, error = %format!("{err:#}"), "Could not format a root disk base for the cache");
                    }
                }
                drop(guard);
            });
        match spawned {
            Ok(_) => Fill::Background,
            Err(_) => Fill::Skipped("could not start the background fill"),
        }
    }

    /// See [`ensure_image_base`].
    pub(crate) fn ensure(
        self: &Arc<Self>,
        builder: Arc<dyn BaseBuilder>,
        inputs: &BaseInputs,
    ) -> R<Ensured> {
        let key = inputs.key();
        if let Some(entry) = self.lookup(&key) {
            Self::touch(&entry);
            return Ok(Ensured::Present);
        }
        if self.is_oversized(&key) {
            return Ok(Ensured::Skipped("the base is larger than the cache budget"));
        }
        let Some(guard) = self.claim(&key) else {
            return Ok(match self.lookup(&key) {
                Some(_) => Ensured::Present,
                None => Ensured::Skipped("the concurrent fill did not keep it"),
            });
        };
        let staged = self.stage()?;
        let tree = staged.join("tree");
        let image = staged.join(IMAGE_FILE);
        let built = build_at(&*builder, &image, &tree);
        let _ = std::fs::remove_dir_all(&tree);
        let facts = match built {
            Ok(facts) => facts,
            Err(err) => {
                let _ = std::fs::remove_dir_all(&staged);
                return Err(err);
            }
        };
        if allocated_bytes(&image) > self.budget {
            self.oversized
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(key.clone());
            let _ = std::fs::remove_dir_all(&staged);
            return Ok(Ensured::Skipped("the base is larger than the cache budget"));
        }
        let result = match self.commit(&key, &staged, inputs, facts) {
            Ok(()) => Ensured::Built,
            Err(reason) => Ensured::Skipped(reason),
        };
        drop(guard);
        Ok(result)
    }

    /// Moves a staged base (`staged/rootfs.img`) into the cache under `key`,
    /// evicting least recently used bases to make room. The reason it was
    /// not kept, on failure; `staged` is gone either way.
    fn commit(
        &self,
        key: &str,
        staged: &Path,
        inputs: &BaseInputs,
        facts: BaseFacts,
    ) -> Result<(), &'static str> {
        let _guard = self.commit_lock.lock().unwrap_or_else(|p| p.into_inner());
        let discard = |reason| {
            let _ = std::fs::remove_dir_all(staged);
            Err(reason)
        };
        let image = staged.join(IMAGE_FILE);
        let bytes = allocated_bytes(&image);
        if bytes > self.budget {
            return discard("the base is larger than the cache budget");
        }
        if crate::content_store::free_bytes(&self.root).is_some_and(|free| free < self.min_free) {
            return discard("the disk is below minFreeDisk");
        }
        self.evict_to(self.budget.saturating_sub(bytes));
        let meta = BaseMeta {
            format: FORMAT.to_string(),
            inputs: inputs.clone(),
            facts,
        };
        let Ok(json) = serde_json::to_vec_pretty(&meta) else {
            return discard("could not record it");
        };
        if std::fs::write(staged.join(META_FILE), json).is_err() || set_mode(&image, 0o444).is_err()
        {
            return discard("could not record it");
        }
        let dest = self.root.join(key);
        if dest.exists() {
            return discard("it was already cached");
        }
        if std::fs::rename(staged, &dest).is_err() {
            return discard("could not move it into place");
        }
        Ok(())
    }

    /// Bases, oldest use first, with their allocated bytes.
    fn entries_by_age(&self) -> Vec<(PathBuf, u64, SystemTime)> {
        let mut entries: Vec<(PathBuf, u64, SystemTime)> = std::fs::read_dir(&self.root)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|entry| entry.file_name() != TMP_DIR)
            .map(|entry| {
                let dir = entry.path();
                let used = std::fs::metadata(dir.join(META_FILE))
                    .and_then(|meta| meta.modified())
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                (dir.clone(), allocated_bytes(&dir.join(IMAGE_FILE)), used)
            })
            .collect();
        entries.sort_by_key(|(_, _, used)| *used);
        entries
    }

    /// Evicts least recently used bases until the cache holds at most
    /// `target` bytes. The caller holds `commit_lock`.
    fn evict_to(&self, target: u64) {
        let entries = self.entries_by_age();
        let mut total: u64 = entries.iter().map(|(_, bytes, _)| *bytes).sum();
        for (dir, bytes, _) in entries {
            if total <= target {
                break;
            }
            if std::fs::remove_dir_all(&dir).is_ok() {
                total = total.saturating_sub(bytes);
                debug!(dir = %dir.display(), bytes, "Evicted a sandbox root disk base");
            }
        }
    }

    /// Bytes held, for tests and diagnostics.
    #[cfg(test)]
    fn held_bytes(&self) -> u64 {
        self.entries_by_age()
            .iter()
            .map(|(_, bytes, _)| *bytes)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// A base made of plain files: the "disk" is the tree's one file copied
    /// out, padded to `size` bytes of real data, so budgets see real blocks.
    struct FakeBase {
        content: Vec<u8>,
        size: usize,
        builds: AtomicUsize,
        formats: AtomicUsize,
        /// Slows each format down, to hold a fill in flight.
        delay: Duration,
    }

    impl FakeBase {
        fn new(content: &[u8], size: usize) -> Arc<Self> {
            Arc::new(FakeBase {
                content: content.to_vec(),
                size,
                builds: AtomicUsize::new(0),
                formats: AtomicUsize::new(0),
                delay: Duration::ZERO,
            })
        }
    }

    impl BaseBuilder for FakeBase {
        fn tree(&self, tree: &Path) -> R<BaseFacts> {
            self.builds.fetch_add(1, Ordering::SeqCst);
            std::fs::create_dir_all(tree)?;
            std::fs::write(tree.join("content"), &self.content)?;
            Ok(BaseFacts {
                etc_dir: true,
                resolv_conf: false,
            })
        }

        fn format(&self, tree: &Path, image: &Path) -> R<()> {
            self.formats.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(self.delay);
            let mut bytes = std::fs::read(tree.join("content"))?;
            bytes.resize(self.size.max(bytes.len()), 0xAB);
            std::fs::write(image, bytes)?;
            Ok(())
        }
    }

    fn inputs(layer: &str) -> BaseInputs {
        BaseInputs {
            layers: vec![format!("sha256:{layer}")],
            guest_binary: "/opt/legion-guest:100:1".into(),
            headroom_mib: 256,
        }
    }

    /// Waits for a background fill to land.
    fn wait_for_entry(cache: &RootfsCache, key: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while cache.lookup(key).is_none() {
            assert!(
                Instant::now() < deadline,
                "the background fill never landed"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn the_key_covers_what_is_in_the_base_and_nothing_else() {
        let base = inputs("aaa");
        assert_eq!(base.key(), inputs("aaa").key());
        assert_ne!(base.key(), inputs("bbb").key(), "another image");
        let mut other = base.clone();
        other.layers.push("sha256:ccc".into());
        assert_ne!(base.key(), other.key(), "another layer on top");
        let mut other = base.clone();
        other.guest_binary = "/opt/legion-guest:101:2".into();
        assert_ne!(base.key(), other.key(), "a new guest binary");
        let mut other = base.clone();
        other.headroom_mib = 512;
        assert_ne!(base.key(), other.key(), "another headroom");
    }

    #[test]
    fn a_miss_builds_then_the_next_start_clones() -> R<()> {
        // With reflinks where the filesystem has them, and the path a
        // filesystem without them takes: a background fill and byte copies.
        a_miss_builds_then_the_next_start_clones_with(true)?;
        a_miss_builds_then_the_next_start_clones_with(false)
    }

    fn a_miss_builds_then_the_next_start_clones_with(reflinks: bool) -> R<()> {
        let dir = tempfile::tempdir()?;
        let cache = RootfsCache::open_with(&dir.path().join("cache"), 64 << 20, 0, reflinks)?;
        let base = FakeBase::new(b"image bytes", 4096);
        let key_inputs = inputs("aaa");

        let first = dir.path().join("alloc-1/rootfs.img");
        std::fs::create_dir_all(first.parent().unwrap())?;
        let (_, outcome) = cache.materialize(
            base.clone(),
            &key_inputs,
            &first,
            &dir.path().join("alloc-1/tree"),
        )?;
        if reflinks {
            assert!(
                matches!(outcome, Outcome::Built(Fill::Reflink | Fill::Background)),
                "{outcome:?}"
            );
        } else {
            assert_eq!(outcome, Outcome::Built(Fill::Background));
        }
        assert!(
            !dir.path().join("alloc-1/tree").exists(),
            "the tree is cleaned up"
        );
        wait_for_entry(&cache, &key_inputs.key());

        // The first start's disk is its own: writing into it (the sandbox's
        // init and env) must not reach the cached base.
        std::fs::write(&first, b"sandbox 1 secrets")?;

        let second = dir.path().join("alloc-2/rootfs.img");
        std::fs::create_dir_all(second.parent().unwrap())?;
        let (facts, outcome) = cache.materialize(
            base.clone(),
            &key_inputs,
            &second,
            &dir.path().join("alloc-2/tree"),
        )?;
        assert!(matches!(outcome, Outcome::Hit(_)), "{outcome:?}");
        if !reflinks {
            assert_eq!(outcome, Outcome::Hit(CopyMethod::SparseCopy));
        }
        assert!(facts.etc_dir);
        let bytes = std::fs::read(&second)?;
        assert!(
            bytes.starts_with(b"image bytes"),
            "the clone carries the base"
        );
        assert_eq!(
            base.builds.load(Ordering::SeqCst),
            1,
            "the image was extracted once"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&second)?.permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "a sandbox's disk is owner-only");
            let entry = cache.lookup(&key_inputs.key()).expect("cached");
            let mode = std::fs::metadata(&entry.image)?.permissions().mode() & 0o777;
            assert_eq!(mode, 0o444, "the base is read-only");
        }

        // Another image misses.
        let third = dir.path().join("alloc-3/rootfs.img");
        std::fs::create_dir_all(third.parent().unwrap())?;
        let (_, outcome) = cache.materialize(
            FakeBase::new(b"other image", 4096),
            &inputs("bbb"),
            &third,
            &dir.path().join("alloc-3/tree"),
        )?;
        assert!(matches!(outcome, Outcome::Built(_)), "{outcome:?}");
        Ok(())
    }

    #[test]
    fn concurrent_starts_of_one_image_build_it_once() -> R<()> {
        concurrent_starts_of_one_image_build_it_once_with(true)?;
        // Without reflinks the waiters wait for the background fill.
        concurrent_starts_of_one_image_build_it_once_with(false)
    }

    fn concurrent_starts_of_one_image_build_it_once_with(reflinks: bool) -> R<()> {
        let dir = tempfile::tempdir()?;
        let cache = RootfsCache::open_with(&dir.path().join("cache"), 64 << 20, 0, reflinks)?;
        let base = Arc::new(FakeBase {
            content: b"image bytes".to_vec(),
            size: 4096,
            builds: AtomicUsize::new(0),
            formats: AtomicUsize::new(0),
            delay: Duration::from_millis(200),
        });
        let key_inputs = inputs("aaa");
        let outcomes: Vec<Outcome> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|n| {
                    let cache = cache.clone();
                    let base = base.clone();
                    let key_inputs = key_inputs.clone();
                    let root = dir.path().to_path_buf();
                    scope.spawn(move || {
                        let alloc = root.join(format!("alloc-{n}"));
                        std::fs::create_dir_all(&alloc).unwrap();
                        cache
                            .materialize(
                                base,
                                &key_inputs,
                                &alloc.join("rootfs.img"),
                                &alloc.join("tree"),
                            )
                            .unwrap()
                            .1
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(
            base.builds.load(Ordering::SeqCst),
            1,
            "one extraction: {outcomes:?}"
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|o| matches!(o, Outcome::Hit(_)))
                .count(),
            7,
            "{outcomes:?}"
        );
        // A prefetch of the same base now is a no-op.
        assert_eq!(cache.ensure(base.clone(), &key_inputs)?, Ensured::Present);
        assert_eq!(base.builds.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[test]
    fn ensure_fills_once_and_starts_then_hit() -> R<()> {
        let dir = tempfile::tempdir()?;
        let cache = RootfsCache::open(&dir.path().join("cache"), 64 << 20, 0)?;
        let base = FakeBase::new(b"image bytes", 4096);
        let key_inputs = inputs("aaa");
        assert_eq!(cache.ensure(base.clone(), &key_inputs)?, Ensured::Built);
        assert_eq!(cache.ensure(base.clone(), &key_inputs)?, Ensured::Present);
        let dest = dir.path().join("rootfs.img");
        let (_, outcome) =
            cache.materialize(base.clone(), &key_inputs, &dest, &dir.path().join("tree"))?;
        assert!(matches!(outcome, Outcome::Hit(_)), "{outcome:?}");
        assert_eq!(base.builds.load(Ordering::SeqCst), 1);
        assert!(
            std::fs::read_dir(dir.path().join("cache/tmp"))?
                .next()
                .is_none(),
            "nothing is left staged"
        );
        Ok(())
    }

    #[test]
    fn the_budget_evicts_the_least_recently_used_base() -> R<()> {
        let dir = tempfile::tempdir()?;
        // Room for two of these bases, not three.
        let size = 1 << 20;
        let cache = RootfsCache::open(&dir.path().join("cache"), (5 * size / 2) as u64, 0)?;
        let (a, b, c) = (inputs("aaa"), inputs("bbb"), inputs("ccc"));
        cache.ensure(FakeBase::new(b"a", size), &a)?;
        cache.ensure(FakeBase::new(b"b", size), &b)?;
        // Use `a`, so `b` is the least recently used.
        let entry = cache.lookup(&a.key()).expect("a cached");
        let file = std::fs::File::options()
            .write(true)
            .open(&entry.meta_path)?;
        file.set_modified(SystemTime::now() + Duration::from_secs(60))?;

        assert_eq!(cache.ensure(FakeBase::new(b"c", size), &c)?, Ensured::Built);
        assert!(cache.lookup(&a.key()).is_some(), "recently used base kept");
        assert!(
            cache.lookup(&b.key()).is_none(),
            "least recently used base evicted"
        );
        assert!(cache.lookup(&c.key()).is_some());
        assert!(cache.held_bytes() <= (5 * size / 2) as u64);
        Ok(())
    }

    #[test]
    fn a_base_larger_than_the_budget_is_never_kept() -> R<()> {
        let dir = tempfile::tempdir()?;
        let cache = RootfsCache::open(&dir.path().join("cache"), 64 * 1024, 0)?;
        let base = FakeBase::new(b"big", 1 << 20);
        let key_inputs = inputs("big");
        let dest = dir.path().join("rootfs.img");
        let (_, outcome) =
            cache.materialize(base.clone(), &key_inputs, &dest, &dir.path().join("tree"))?;
        assert_eq!(
            outcome,
            Outcome::Built(Fill::Skipped("the base is larger than the cache budget"))
        );
        assert!(dest.is_file(), "the start still gets its disk");
        assert_eq!(
            cache.ensure(base, &key_inputs)?,
            Ensured::Skipped("the base is larger than the cache budget")
        );
        assert_eq!(cache.held_bytes(), 0);
        Ok(())
    }

    #[test]
    fn reopening_drops_half_done_fills_and_foreign_entries() -> R<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().join("cache");
        let key_inputs = inputs("aaa");
        {
            let cache = RootfsCache::open(&root, 64 << 20, 0)?;
            cache.ensure(FakeBase::new(b"a", 4096), &key_inputs)?;
        }
        std::fs::create_dir_all(root.join("tmp/half-done"))?;
        std::fs::create_dir_all(root.join("not-a-base"))?;
        let cache = RootfsCache::open(&root, 64 << 20, 0)?;
        assert!(
            cache.lookup(&key_inputs.key()).is_some(),
            "a good base survives"
        );
        assert!(!root.join("tmp/half-done").exists());
        assert!(!root.join("not-a-base").exists());
        // A budget that shrank to nothing empties it.
        drop(cache);
        let cache = RootfsCache::open(&root, 0, 0)?;
        assert!(cache.lookup(&key_inputs.key()).is_none());
        Ok(())
    }

    /// A one-layer OCI layout with an `/etc/hostname` and a `/legion` the
    /// base must take over.
    fn image_layout(root: &Path) -> R<PathBuf> {
        let layout = root.join("layout");
        let blob = |bytes: &[u8]| -> R<String> {
            let hex = format!("{:x}", Sha256::digest(bytes));
            let dir = layout.join("blobs/sha256");
            std::fs::create_dir_all(&dir)?;
            std::fs::write(dir.join(&hex), bytes)?;
            Ok(format!("sha256:{hex}"))
        };
        let mut layer = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut layer);
            for (path, content) in [
                ("bin/sh", &b"#!/fake/shell\n"[..]),
                ("etc/hostname", &b"image-host\n"[..]),
                ("legion/init", &b"image's own init\n"[..]),
            ] {
                let mut header = tar::Header::new_gnu();
                header.set_size(content.len() as u64);
                header.set_mode(0o755);
                builder.append_data(&mut header, path, content)?;
            }
            builder.finish()?;
        }
        let layer_digest = blob(&layer)?;
        let config = serde_json::to_vec(&json!({ "config": {} }))?;
        let config_digest = blob(&config)?;
        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "config": { "digest": config_digest, "size": config.len() },
            "layers": [{ "digest": layer_digest, "size": layer.len() }],
        }))?;
        let manifest_digest = blob(&manifest)?;
        std::fs::write(
            layout.join("index.json"),
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "manifests": [{ "digest": manifest_digest, "size": manifest.len() }],
            }))?,
        )?;
        Ok(layout)
    }

    fn debugfs(image: &Path, request: &str) -> R<String> {
        let output = std::process::Command::new(microvm::debugfs_binary()?)
            .arg("-R")
            .arg(request)
            .arg(image)
            .output()?;
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// The real base: an image layout, mkfs, a clone per sandbox, and each
    /// sandbox's env written into its own clone only. Needs e2fsprogs.
    #[test]
    fn sandboxes_share_the_base_and_never_each_others_env() -> R<()> {
        if microvm::debugfs_binary().is_err()
            || crate::volumes::builtins::mkfs_binary("ext4").is_none()
        {
            return Ok(()); // e2fsprogs not installed on this machine.
        }
        let dir = tempfile::tempdir()?;
        let layout = image_layout(dir.path())?;
        let agent = dir.path().join("legion-guest");
        std::fs::write(&agent, b"static guest")?;
        let base = Arc::new(ImageBase {
            layout: layout.clone(),
            guest_binary: GuestBinary::at(agent, true)?,
            headroom_mib: 8,
        });
        let key_inputs = base.inputs(&oci_bundle::read_image_config(&layout)?);
        let cache = RootfsCache::open(&dir.path().join("cache"), 1 << 30, 0)?;

        let mut disks = Vec::new();
        for (n, secret) in ["first-secret", "second-secret"].into_iter().enumerate() {
            let alloc = dir.path().join(format!("alloc-{n}"));
            std::fs::create_dir_all(&alloc)?;
            let disk = alloc.join("rootfs.img");
            let (facts, outcome) =
                cache.materialize(base.clone(), &key_inputs, &disk, &alloc.join("rootfs"))?;
            if n == 1 {
                assert!(matches!(outcome, Outcome::Hit(_)), "{outcome:?}");
            } else {
                wait_for_entry(&cache, &key_inputs.key());
            }
            let payload = microvm::GuestPayload {
                command: vec!["/bin/sh".into()],
                env: std::collections::BTreeMap::from([("TOKEN".into(), secret.into())]),
                ..microvm::GuestPayload::default()
            };
            let files = microvm::guest_payload_files(&payload, &format!("sbx-{n}"), &facts);
            microvm::inject_guest_payload(&disk, &files, &alloc.join("payload"))?;
            disks.push(disk);
        }

        assert!(debugfs(&disks[0], "cat /legion/env")?.contains("first-secret"));
        assert!(debugfs(&disks[1], "cat /legion/env")?.contains("second-secret"));
        assert!(!debugfs(&disks[1], "cat /legion/env")?.contains("first-secret"));
        assert_eq!(debugfs(&disks[1], "cat /etc/hostname")?, "sbx-1\n");
        assert_eq!(debugfs(&disks[1], "cat /legion/legion")?, "static guest");
        assert_eq!(debugfs(&disks[1], "cat /bin/sh")?, "#!/fake/shell\n");

        // The cached base holds the image and the guest binary, and nothing
        // of either sandbox: no env, no init, no hostname.
        let entry = cache.lookup(&key_inputs.key()).expect("cached");
        let legion = debugfs(&entry.image, "ls -p /legion")?;
        assert!(legion.contains("/legion/"), "{legion}");
        assert!(
            !legion.contains("/env/") && !legion.contains("/init/"),
            "{legion}"
        );
        assert!(!debugfs(&entry.image, "ls -p /etc")?.contains("/hostname/"));
        Ok(())
    }

    /// Times a sandbox's root disk three ways on a real image: the build as
    /// it was before the cache (payload written into the tree, one mkfs),
    /// a cache miss, and a cache hit. Opt-in, Linux with e2fsprogs:
    ///
    /// `LEGION_ROOTFS_BENCH_LAYOUT=/path/to/oci-layout LEGION_ROOTFS_BENCH_DIR=/work \
    ///   cargo test --lib bench_root_disk_paths -- --ignored --nocapture`
    #[test]
    #[ignore = "benchmark: needs an OCI layout and e2fsprogs"]
    fn bench_root_disk_paths() -> R<()> {
        let layout = PathBuf::from(std::env::var("LEGION_ROOTFS_BENCH_LAYOUT")?);
        let work_root = std::env::var("LEGION_ROOTFS_BENCH_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        let runs: usize = std::env::var("LEGION_ROOTFS_BENCH_RUNS")
            .ok()
            .and_then(|runs| runs.parse().ok())
            .unwrap_or(5);
        let dir = tempfile::tempdir_in(&work_root)?;
        let agent = dir.path().join("legion-guest");
        std::fs::write(&agent, vec![0x5a; 1 << 20])?;
        let base = Arc::new(ImageBase {
            layout: layout.clone(),
            guest_binary: GuestBinary::at(agent, true)?,
            headroom_mib: 256,
        });
        let key_inputs = base.inputs(&oci_bundle::read_image_config(&layout)?);
        let payload = microvm::GuestPayload {
            command: vec!["/bin/sh".into(), "-c".into(), "sleep 1".into()],
            env: std::collections::BTreeMap::from([("TOKEN".into(), "secret".into())]),
            cwd: Some("/".into()),
            ..microvm::GuestPayload::default()
        };
        let ms = |since: Instant| since.elapsed().as_secs_f64() * 1000.0;
        let median = |mut samples: Vec<f64>| {
            samples.sort_by(|a, b| a.total_cmp(b));
            samples[samples.len() / 2]
        };

        // Before the cache: the payload is written into the tree and the
        // whole thing formatted once.
        let mut before = Vec::new();
        for n in 0..runs {
            let alloc = dir.path().join(format!("before-{n}"));
            let started = Instant::now();
            let facts = base.tree(&alloc.join("rootfs"))?;
            let files = microvm::guest_payload_files(&payload, "sbx", &facts);
            microvm::write_guest_payload_into_tree(&alloc.join("rootfs"), &files)?;
            base.format(&alloc.join("rootfs"), &alloc.join("rootfs.img"))?;
            let _ = std::fs::remove_dir_all(alloc.join("rootfs"));
            before.push(ms(started));
            let _ = std::fs::remove_dir_all(&alloc);
        }

        let start = |cache: &Arc<RootfsCache>, name: &str| -> R<(f64, Outcome)> {
            let alloc = dir.path().join(name);
            std::fs::create_dir_all(&alloc)?;
            let disk = alloc.join("rootfs.img");
            let started = Instant::now();
            let (facts, outcome) =
                cache.materialize(base.clone(), &key_inputs, &disk, &alloc.join("rootfs"))?;
            let files = microvm::guest_payload_files(&payload, "sbx", &facts);
            microvm::inject_guest_payload(&disk, &files, &alloc.join("payload"))?;
            Ok((ms(started), outcome))
        };
        let mut misses = Vec::new();
        let mut hits = Vec::new();
        let mut outcomes = Vec::new();
        let reflinks = std::env::var("LEGION_ROOTFS_BENCH_NO_REFLINK").is_err();
        for n in 0..runs {
            let cache = RootfsCache::open_with(
                &dir.path().join(format!("cache-{n}")),
                1 << 40,
                0,
                reflinks,
            )?;
            let (miss, outcome) = start(&cache, &format!("miss-{n}"))?;
            misses.push(miss);
            outcomes.push(outcome);
            wait_for_entry(&cache, &key_inputs.key());
            let (hit, outcome) = start(&cache, &format!("hit-{n}"))?;
            hits.push(hit);
            outcomes.push(outcome);
        }
        println!(
            "root disk, median of {runs}: before the cache {:.1} ms, miss {:.1} ms, hit {:.1} ms ({:?})",
            median(before),
            median(misses),
            median(hits),
            outcomes
        );

        // A burst: eight starts of an image new to the node, at once.
        let burst = 8;
        let started = Instant::now();
        std::thread::scope(|scope| -> R<()> {
            let handles: Vec<_> = (0..burst)
                .map(|n| {
                    let base = base.clone();
                    let payload = &payload;
                    let alloc = dir.path().join(format!("burst-before-{n}"));
                    scope.spawn(move || -> R<()> {
                        let facts = base.tree(&alloc.join("rootfs"))?;
                        let files = microvm::guest_payload_files(payload, "sbx", &facts);
                        microvm::write_guest_payload_into_tree(&alloc.join("rootfs"), &files)?;
                        base.format(&alloc.join("rootfs"), &alloc.join("rootfs.img"))?;
                        let _ = std::fs::remove_dir_all(&alloc);
                        Ok(())
                    })
                })
                .collect();
            for handle in handles {
                handle.join().expect("burst thread")?;
            }
            Ok(())
        })?;
        let burst_before = ms(started);
        let cache = RootfsCache::open_with(&dir.path().join("cache-burst"), 1 << 40, 0, reflinks)?;
        let started = Instant::now();
        std::thread::scope(|scope| -> R<()> {
            let handles: Vec<_> = (0..burst)
                .map(|n| {
                    let cache = &cache;
                    let start = &start;
                    scope.spawn(move || start(cache, &format!("burst-{n}")))
                })
                .collect();
            for handle in handles {
                handle.join().expect("burst thread")?;
            }
            Ok(())
        })?;
        println!(
            "burst of {burst} starts of a new image: before the cache {:.1} ms, with it {:.1} ms",
            burst_before,
            ms(started)
        );
        Ok(())
    }

    #[test]
    fn a_failed_build_fails_the_start_and_releases_waiters() -> R<()> {
        struct Broken;
        impl BaseBuilder for Broken {
            fn tree(&self, _tree: &Path) -> R<BaseFacts> {
                bail!("layer is corrupt")
            }
            fn format(&self, _tree: &Path, _image: &Path) -> R<()> {
                unreachable!()
            }
        }
        let dir = tempfile::tempdir()?;
        let cache = RootfsCache::open(&dir.path().join("cache"), 64 << 20, 0)?;
        let err = cache
            .materialize(
                Arc::new(Broken),
                &inputs("aaa"),
                &dir.path().join("rootfs.img"),
                &dir.path().join("tree"),
            )
            .unwrap_err();
        assert!(err.to_string().contains("layer is corrupt"), "{err}");
        assert!(
            cache.flights.lock().unwrap().is_empty(),
            "the claim is released"
        );
        Ok(())
    }
}
