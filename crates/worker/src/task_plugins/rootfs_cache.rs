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
//! <worker state dir>/content/derived/
//!   rootfs/<key>/rootfs.img   the base (mode 0444)
//!   rootfs/<key>/base.json    what it was built from
//!   tmp/                      fills in progress, cleared when the worker starts
//! ```
//!
//! Bases are **derived content** (docs/artifacts.md, "Derived content"): the
//! `rootfs` kind of the node content store's `derived` class, which owns the
//! index, the budget, eviction, and the single-flight claim. This module
//! holds what is specific to root disks: how one is built, how a start gets
//! its own copy, and how a miss fills the cache without slowing the start.
//!
//! The store sits beside the allocation dirs so a clone can be a reflink.
//! The key is the recipe's: a hash of everything that decides a base's bytes
//! ([`BaseInputs`]), which is the image's layer digests, the guest binary,
//! and the task's `rootfs_headroom_mib`, plus the builder's version. Env,
//! hostname, command, and volumes are not in it, because none of them are in
//! the base.
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
//! Bases share the derived class's byte budget (`content.budgets.derived`,
//! default 10% of the filesystem, clamped to 1 to 20 GiB, `"0"` for off),
//! counted in blocks actually allocated. A commit evicts the least recently
//! used derived entries first, a base larger than the whole budget is never
//! kept, and none is kept while the disk is below the content store's
//! `minFreeDisk`. A clone is independent of its base, so evicting a base
//! never touches a running sandbox.

use crate::content_store::{ContentStore, DerivedFlight, DerivedKind, DerivedSkip, Recipe};
use crate::prelude::*;
use crate::task_plugins::microvm::{self, BaseFacts, GuestBinary};
use crate::task_plugins::oci_bundle;
use crate::volumes::copy::{self, CopyMethod};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::Instant;

/// Bumped whenever what goes into a base changes, so a worker never clones a
/// base that an older build laid out differently.
const FORMAT: &str = "1";
const IMAGE_FILE: &str = "rootfs.img";
const META_FILE: &str = "base.json";

/// The derived-content kind bases are kept under.
const KIND: DerivedKind = DerivedKind::Rootfs;

/// The builder version in a base's recipe: this build of Legion and the
/// base format. A new release rebuilds each base once, on first use.
fn builder_version() -> String {
    format!("legion-{}+rootfs.{FORMAT}", env!("CARGO_PKG_VERSION"))
}

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
    /// The derived-content recipe for this base. It carries the layer
    /// digests, a fingerprint of the guest binary's identity, and the
    /// headroom: nothing of any one sandbox.
    pub(crate) fn recipe(&self) -> Recipe {
        let mut recipe = Recipe::new(KIND, builder_version());
        for (index, layer) in self.layers.iter().enumerate() {
            recipe = recipe.listed("layer", index, layer);
        }
        recipe
            .fingerprint("guestBinary", self.guest_binary.as_bytes())
            .number("headroomMib", self.headroom_mib)
    }

    /// The cache key: the recipe's.
    pub(crate) fn key(&self) -> String {
        self.recipe().key()
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

/// A committed base.
struct Entry {
    key: String,
    image: PathBuf,
    meta: BaseMeta,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct BaseMeta {
    format: String,
    inputs: BaseInputs,
    facts: BaseFacts,
}

/// The node's base root disk cache: the `rootfs` kind of the content
/// store's derived class.
pub(crate) struct RootfsCache {
    store: Arc<ContentStore>,
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
            .field("root", &self.store.root())
            .field("budget", &self.store.derived_budget())
            .finish()
    }
}

static NODE_CACHE: RwLock<Option<Arc<RootfsCache>>> = RwLock::new(None);

/// This worker's cache, once [`init`] opened it.
pub(crate) fn node_cache() -> Option<Arc<RootfsCache>> {
    NODE_CACHE.read().unwrap_or_else(|p| p.into_inner()).clone()
}

/// Registers the worker's root disk cache on its node content store, and
/// removes `legacy_dir`: the `rootfs-cache/` directory of a worker from
/// before bases were derived content. Bases rebuild on first use.
///
/// With no store (the content store is disabled or could not open) or a
/// derived budget of `0`, the cache is off: starts then build every root
/// disk, as they would without it.
pub(crate) fn init(store: Option<Arc<ContentStore>>, legacy_dir: &Path) {
    remove_legacy_cache(legacy_dir);
    let cache = match store {
        None => {
            info!("Sandbox root disk cache off (this node has no content store)");
            None
        }
        Some(store) if store.derived_budget() == 0 => {
            info!("Sandbox root disk cache off (content.budgets.derived is 0)");
            None
        }
        Some(store) => {
            info!(
                dir = %store.derived_path(KIND, "").display(),
                budget_bytes = store.derived_budget(),
                "Sandbox root disk cache open"
            );
            Some(RootfsCache::on(store))
        }
    };
    *NODE_CACHE.write().unwrap_or_else(|p| p.into_inner()) = cache;
}

/// Deletes the pre-derived-content cache directory, when one is left.
fn remove_legacy_cache(legacy_dir: &Path) {
    if !legacy_dir.is_dir() {
        return;
    }
    match std::fs::remove_dir_all(legacy_dir) {
        Ok(()) => info!(
            dir = %legacy_dir.display(),
            "Removed the old sandbox root disk cache; bases are kept as derived content now \
             and rebuild on first use"
        ),
        Err(err) => warn!(
            dir = %legacy_dir.display(),
            error = %err,
            "Could not remove the old sandbox root disk cache; delete it to free its disk"
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
    crate::content_store::allocated_bytes(path)
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

const OVERSIZED: &str = "the base is larger than the cache budget";

/// Why the store did not keep a base, in this cache's words.
fn skip_reason(skip: DerivedSkip) -> &'static str {
    match skip {
        DerivedSkip::Oversized => OVERSIZED,
        DerivedSkip::BelowMinFree => "the disk is below minFreeDisk",
        DerivedSkip::AlreadyPresent => "it was already cached",
        DerivedSkip::NoRoom => "pinned derived content leaves no room for it",
        DerivedSkip::Disabled => "the cache is off",
        DerivedSkip::Failed => "could not record it",
    }
}

impl RootfsCache {
    /// The cache on a node content store.
    pub(crate) fn on(store: Arc<ContentStore>) -> Arc<Self> {
        Self::on_with(store, true)
    }

    fn on_with(store: Arc<ContentStore>, reflinks: bool) -> Arc<Self> {
        Arc::new(RootfsCache {
            store,
            oversized: Mutex::new(BTreeSet::new()),
            reflinks,
        })
    }

    /// A cache on its own content store at `root` whose derived budget is
    /// `budget`: what a worker's cache is, without the worker.
    #[cfg(test)]
    pub(crate) fn open(root: &Path, budget: u64, min_free: u64) -> R<Arc<Self>> {
        Self::open_with(root, budget, min_free, true)
    }

    #[cfg(test)]
    fn open_with(root: &Path, budget: u64, min_free: u64, reflinks: bool) -> R<Arc<Self>> {
        use crate::content_store::{ContentBudgets, ContentStoreConfig};
        let store = ContentStore::open(
            root,
            ContentStoreConfig {
                budgets: ContentBudgets {
                    artifacts: 0,
                    snapshots: 0,
                    derived: budget,
                },
                ttl: None,
                min_free_disk: min_free,
            },
        )?;
        Ok(Self::on_with(Arc::new(store), reflinks))
    }

    fn budget(&self) -> u64 {
        self.store.derived_budget()
    }

    /// A fresh directory under the store's `derived/tmp/`, on its filesystem.
    fn stage(&self) -> R<PathBuf> {
        self.store.derived_stage()
    }

    fn lookup(&self, key: &str) -> Option<Entry> {
        let entry = self.store.derived_lookup(KIND, key, false).ok().flatten()?;
        let image = entry.path.join(IMAGE_FILE);
        let meta = std::fs::read(entry.path.join(META_FILE))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<BaseMeta>(&bytes).ok())
            .filter(|meta| meta.format == FORMAT && meta.inputs.key() == key && image.is_file());
        let Some(meta) = meta else {
            // Indexed, and not a base this build can use: drop it, so the
            // next fill can take its place.
            let _ = self.store.derived_remove(KIND, key);
            return None;
        };
        Some(Entry {
            key: key.to_string(),
            image,
            meta,
        })
    }

    /// Marks a base used now: eviction goes by this.
    fn touch(&self, entry: &Entry) {
        let _ = self.store.derived_lookup(KIND, &entry.key, true);
    }

    /// Clones a base to `dest` for one sandbox.
    fn clone_entry(&self, entry: &Entry, dest: &Path) -> R<CopyMethod> {
        let _ = std::fs::remove_file(dest);
        let method = copy::clone_file_with(&entry.image, dest, self.reflinks)?;
        // The base is read-only. The clone is this sandbox's disk.
        set_mode(dest, 0o600)?;
        self.touch(entry);
        crate::telemetry::worker_metrics()
            .derived_hits_total
            .inc(&[KIND.as_str()]);
        Ok(method)
    }

    /// Takes the right to fill `key`, or, when a fill is already in flight,
    /// waits for it to finish and returns `None`.
    fn claim(&self, key: &str) -> Option<DerivedFlight> {
        self.store.derived_claim(KIND, key)
    }

    fn is_oversized(&self, key: &str) -> bool {
        self.oversized
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(key)
    }

    fn note_oversized(&self, key: &str) {
        self.oversized
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key.to_string());
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
            OVERSIZED
        } else {
            "the concurrent fill did not keep it"
        };
        Ok((facts?, Outcome::Built(Fill::Skipped(reason))))
    }

    /// The miss path of a start: build at `dest` exactly as an uncached
    /// start would, then keep a copy for the next one.
    fn build_and_fill(
        self: &Arc<Self>,
        guard: DerivedFlight,
        builder: Arc<dyn BaseBuilder>,
        inputs: &BaseInputs,
        dest: &Path,
        tree: &Path,
    ) -> R<(BaseFacts, Outcome)> {
        let started = Instant::now();
        let facts = match build_at(&*builder, dest, tree) {
            Ok(facts) => facts,
            Err(err) => {
                let _ = std::fs::remove_dir_all(tree);
                return Err(err);
            }
        };
        self.store
            .derived_observe_build(KIND, started.elapsed().as_secs_f64());
        let fill = self.fill_from_build(guard, builder, inputs, facts, dest, tree);
        Ok((facts, Outcome::Built(fill)))
    }

    fn fill_from_build(
        self: &Arc<Self>,
        guard: DerivedFlight,
        builder: Arc<dyn BaseBuilder>,
        inputs: &BaseInputs,
        facts: BaseFacts,
        dest: &Path,
        tree: &Path,
    ) -> Fill {
        let key = inputs.key();
        if allocated_bytes(dest) > self.budget() {
            self.note_oversized(&key);
            let _ = std::fs::remove_dir_all(tree);
            return Fill::Skipped(OVERSIZED);
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
            return match self.commit(&staged, inputs, facts) {
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
                    Ok(()) => match cache.commit(&staged, &inputs, facts) {
                        Ok(()) => info!(
                            key = %key,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "Cached a sandbox root disk base"
                        ),
                        Err(reason) => debug!(key = %key, reason, "Root disk base not cached"),
                    },
                    Err(err) => {
                        let _ = std::fs::remove_dir_all(&staged);
                        warn!(key = %key, error = %format!("{err:#}"), "Could not format a root disk base for the cache");
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
            self.touch(&entry);
            return Ok(Ensured::Present);
        }
        if self.is_oversized(&key) {
            return Ok(Ensured::Skipped(OVERSIZED));
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
        let started = Instant::now();
        let built = build_at(&*builder, &image, &tree);
        let _ = std::fs::remove_dir_all(&tree);
        let facts = match built {
            Ok(facts) => facts,
            Err(err) => {
                let _ = std::fs::remove_dir_all(&staged);
                return Err(err);
            }
        };
        self.store
            .derived_observe_build(KIND, started.elapsed().as_secs_f64());
        if allocated_bytes(&image) > self.budget() {
            self.note_oversized(&key);
            let _ = std::fs::remove_dir_all(&staged);
            return Ok(Ensured::Skipped(OVERSIZED));
        }
        let result = match self.commit(&staged, inputs, facts) {
            Ok(()) => Ensured::Built,
            Err(reason) => Ensured::Skipped(reason),
        };
        drop(guard);
        Ok(result)
    }

    /// Hands a staged base (`staged/rootfs.img`) to the content store, which
    /// moves it into place under its recipe key and evicts least recently
    /// used derived content to make room. The reason it was not kept, on
    /// failure; `staged` is gone either way.
    fn commit(
        &self,
        staged: &Path,
        inputs: &BaseInputs,
        facts: BaseFacts,
    ) -> Result<(), &'static str> {
        let discard = |reason| {
            let _ = std::fs::remove_dir_all(staged);
            Err(reason)
        };
        let image = staged.join(IMAGE_FILE);
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
        match self.store.derived_commit(&inputs.recipe(), staged) {
            Ok(_) => Ok(()),
            Err(skip) => {
                if skip == DerivedSkip::Oversized {
                    self.note_oversized(&inputs.key());
                }
                Err(skip_reason(skip))
            }
        }
    }

    /// Bytes held, for tests and diagnostics.
    #[cfg(test)]
    fn held_bytes(&self) -> u64 {
        self.store
            .derived_list()
            .unwrap_or_default()
            .iter()
            .map(|entry| entry.size)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
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
        // Let a background fill land before `dir` goes: one still running
        // would write the cache entry into a deleted temp dir, recreating it.
        if matches!(outcome, Outcome::Built(Fill::Background)) {
            wait_for_entry(&cache, &inputs("bbb").key());
        }
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
            std::fs::read_dir(dir.path().join("cache/derived/tmp"))?
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
        cache.touch(&entry);

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
        std::fs::create_dir_all(root.join("derived/tmp/half-done"))?;
        std::fs::create_dir_all(root.join("derived/rootfs/not-a-base"))?;
        let cache = RootfsCache::open(&root, 64 << 20, 0)?;
        assert!(
            cache.lookup(&key_inputs.key()).is_some(),
            "a good base survives"
        );
        assert!(!root.join("derived/tmp/half-done").exists());
        assert!(!root.join("derived/rootfs/not-a-base").exists());
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
            cache.store.derived_builds_in_flight() == 0,
            "the claim is released"
        );
        Ok(())
    }

    #[test]
    fn bases_are_derived_content_in_the_node_store_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let cache = RootfsCache::open(&dir.path().join("content"), 64 << 20, 0)?;
        let key_inputs = inputs("aaa");
        cache.ensure(FakeBase::new(b"image bytes", 4096), &key_inputs)?;
        let listed = cache.store.derived_list()?;
        assert_eq!(listed.len(), 1);
        let entry = &listed[0];
        assert_eq!(entry.kind, DerivedKind::Rootfs);
        assert_eq!(entry.key, key_inputs.key());
        assert_eq!(
            entry.path,
            dir.path()
                .join("content/derived/rootfs")
                .join(key_inputs.key())
        );
        assert!(entry.builder_version.contains("rootfs.1"), "{entry:?}");
        // The recipe names what the base was built from, and holds none of
        // it verbatim but content digests: the guest binary's path is a
        // fingerprint.
        assert_eq!(entry.inputs["headroomMib"], "256");
        assert!(entry.inputs.contains_key("layer.0000"));
        let recorded = serde_json::to_string(&entry.inputs)?;
        assert!(!recorded.contains("/opt/legion-guest"), "{recorded}");
        Ok(())
    }

    #[test]
    fn a_full_budget_takes_the_least_recently_used_entry_of_any_kind_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let size = 1 << 20;
        let cache = RootfsCache::open(&dir.path().join("content"), (5 * size / 2) as u64, 0)?;
        // Another kind's entry is the oldest thing in the class.
        let other = Recipe::new(DerivedKind::VmBase, "vmm-1").number("guestMib", 512);
        cache.store.ensure_derived(&other, |out| {
            std::fs::write(out.join("memory"), vec![0xCD; size])?;
            Ok(())
        })?;
        let (a, b) = (inputs("aaa"), inputs("bbb"));
        cache.ensure(FakeBase::new(b"a", size), &a)?;
        assert_eq!(cache.ensure(FakeBase::new(b"b", size), &b)?, Ensured::Built);
        assert!(
            cache
                .store
                .derived_lookup(other.kind(), &other.key(), false)?
                .is_none(),
            "one budget, one LRU: the other kind's entry went first"
        );
        assert!(cache.lookup(&a.key()).is_some());
        assert!(cache.lookup(&b.key()).is_some());
        Ok(())
    }

    #[test]
    fn init_removes_the_old_cache_directory_and_honors_a_zero_budget_test() -> R<()> {
        use crate::content_store::{ContentBudgets, ContentStoreConfig};
        let dir = tempfile::tempdir()?;
        // What a worker from before derived content left behind.
        let legacy = dir.path().join("rootfs-cache");
        std::fs::create_dir_all(legacy.join("0123abcd"))?;
        std::fs::write(legacy.join("0123abcd/rootfs.img"), vec![0u8; 4096])?;
        std::fs::create_dir_all(legacy.join("tmp"))?;
        remove_legacy_cache(&legacy);
        assert!(!legacy.exists(), "the old cache is deleted at first start");
        // A second start finds nothing to do.
        remove_legacy_cache(&legacy);

        // The budget decides whether a store carries a cache at all.
        let store = |derived: u64, name: &str| -> R<Arc<ContentStore>> {
            Ok(Arc::new(ContentStore::open(
                dir.path().join(name),
                ContentStoreConfig {
                    budgets: ContentBudgets {
                        artifacts: 0,
                        snapshots: 0,
                        derived,
                    },
                    ttl: None,
                    min_free_disk: 0,
                },
            )?))
        };
        assert_eq!(store(0, "off")?.derived_budget(), 0);
        let cache = RootfsCache::on(store(64 << 20, "on")?);
        assert_eq!(
            cache.ensure(FakeBase::new(b"a", 4096), &inputs("aaa"))?,
            Ensured::Built
        );
        Ok(())
    }
}
