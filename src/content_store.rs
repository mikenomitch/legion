//! The node content store: one content-addressed store per worker, shared by
//! everything that keeps immutable bytes on a node (suspend plan §6.8,
//! executing the artifact cache plan's Phase 1 with per-class budgets).
//!
//! # Layout
//!
//! ```text
//! <worker state dir>/content/
//!   index.sqlite        entries, pins, sizes, last use (crash-safe)
//!   sha256/<hex>        immutable blobs, named by their sha256 digest
//!   tmp/                in-flight writes; same filesystem, so commit is a rename
//! ```
//!
//! # Model
//!
//! - **Content addressed.** Every blob is named by `sha256:<hex>` of its
//!   bytes. Putting the same bytes twice stores them once (puts are
//!   idempotent), and a blob is hashed before it is committed, so corrupt
//!   bytes never enter the store.
//! - **Classes with their own budgets.** Each entry belongs to one
//!   [`ContentClass`] (`artifacts` or `snapshots`), and each class has its own
//!   byte budget. Eviction for a class only ever removes entries of that
//!   class, so a 4 GiB memory file cannot push every cached artifact off the
//!   node. A budget is configured as a size or a share of the filesystem
//!   ([`BudgetSpec`]), and the worker sizes shares when it opens the store.
//! - **LRU within a class.** When a put would take a class over budget, the
//!   least recently used *unpinned* entries of that class are evicted first.
//! - **TTL.** [`ContentStore::gc`] also removes unpinned entries that have not
//!   been used for the configured TTL, even under budget.
//! - **Pins.** A [`PinOwner`] (a local snapshot, an allocation mid-restore)
//!   pins a digest. A snapshot's pins are released once it is published.
//!   Neither LRU, TTL, nor [`ContentStore::remove`] touches a pinned entry.
//!   Pinned bytes still count toward their class budget, so a class whose
//!   budget is entirely pinned refuses new puts with an explicit error.
//! - **Minimum free disk.** A put that would leave the filesystem below
//!   `min_free_disk` first evicts unpinned entries of any class, then fails.
//!
//! # Integrity
//!
//! [`ContentStore::get`] is cheap: it checks that the blob exists with the
//! recorded size. [`ContentStore::verify`] and [`ContentStore::copy_out`]
//! rehash the bytes. Any entry found corrupt is dropped from the store (its
//! pins are kept, so re-putting the same digest restores the protection) and
//! the call fails, which callers treat like a miss.
//!
//! # Example: snapshot parts (suspend plan Phase 3b and later)
//!
//! ```no_run
//! # use legion::content_store::*;
//! # fn demo(store: &ContentStore) -> legion::prelude::R<()> {
//! let owner = PinOwner::snapshot("snap-7f3a");
//! // Let the VMM write straight into the store's tmp dir (same filesystem),
//! // then move the file in. The put pins it atomically, so a concurrent GC
//! // can never evict it between the put and the pin.
//! let mem = store.staging_path("memory")?;
//! // ... VMM writes `mem` ...
//! let part = store.import_file(
//!     ContentClass::Snapshots,
//!     &mem,
//!     PutOptions::default().pin(owner.clone()).label("memory"),
//! )?;
//! // Later, on restore: verify and use in place, or copy out.
//! let entry = store.verify(&part.digest)?;
//! let _path = entry.path;
//! // When the snapshot is deleted, drop every pin it held.
//! store.release_owner(&owner)?;
//! # Ok(()) }
//! ```

use std::fmt;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest as _, Sha256};

use crate::prelude::*;

const GIB: u64 = 1024 * 1024 * 1024;

/// Default `content.budgets.artifacts`: 10% of the content filesystem,
/// clamped to 1 GiB..20 GiB. Artifacts are a cache whose misses cost one
/// refetch, so a large disk gains little past 20 GiB, and below 1 GiB a
/// single model or toolchain image would not fit.
pub const DEF_CONTENT_ARTIFACTS_BUDGET: BudgetSpec = BudgetSpec::Share {
    percent: 10.0,
    min: GIB,
    max: 20 * GIB,
};
/// Default `content.budgets.snapshots`: 20% of the content filesystem,
/// clamped to 2 GiB..200 GiB. Memory checkpoints are pinned while their
/// instance is suspended, so this is the share a node gives to suspended
/// guests. 2 GiB holds one small guest's checkpoint. The 200 GiB cap keeps a
/// multi-terabyte data disk mostly for volumes unless the operator says
/// otherwise.
pub const DEF_CONTENT_SNAPSHOTS_BUDGET: BudgetSpec = BudgetSpec::Share {
    percent: 20.0,
    min: 2 * GIB,
    max: 200 * GIB,
};
/// Default `content.ttl`: unpinned entries unused this long are removed.
pub const DEF_CONTENT_TTL: &str = "72h";
/// Default `content.minFreeDisk`.
pub const DEF_CONTENT_MIN_FREE_DISK: &str = "5GiB";

const INDEX_FILE: &str = "index.sqlite";
const BLOB_DIR: &str = "sha256";
const TMP_DIR: &str = "tmp";

/// The budget class an entry belongs to. Each class has its own byte budget
/// and its own LRU, so one kind of content cannot evict another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ContentClass {
    /// Fetched artifacts (digest-pinned files staged into task directories).
    Artifacts,
    /// Checkpoint parts: VMM memory and device-state files, disk images.
    Snapshots,
}

impl ContentClass {
    pub const ALL: [ContentClass; 2] = [ContentClass::Artifacts, ContentClass::Snapshots];

    /// The name used in config (`content.budgets.<name>`) and the index.
    pub fn as_str(self) -> &'static str {
        match self {
            ContentClass::Artifacts => "artifacts",
            ContentClass::Snapshots => "snapshots",
        }
    }
}

impl fmt::Display for ContentClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ContentClass {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> R<Self> {
        match raw {
            "artifacts" => Ok(ContentClass::Artifacts),
            "snapshots" => Ok(ContentClass::Snapshots),
            other => bail!("unknown content class {other:?} (expected artifacts or snapshots)"),
        }
    }
}

/// A validated `sha256:<64 lowercase hex>` digest. The only key the store
/// accepts.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContentDigest(String);

impl ContentDigest {
    /// Parses `sha256:<hex>`. Uppercase hex is normalized to lowercase.
    pub fn parse(raw: &str) -> R<Self> {
        let hex = raw
            .strip_prefix("sha256:")
            .with_context(|| format!("content digest {raw:?} must start with sha256:"))?;
        if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            bail!("content digest {raw:?} must be sha256: followed by 64 hex characters");
        }
        Ok(ContentDigest(format!(
            "sha256:{}",
            hex.to_ascii_lowercase()
        )))
    }

    /// Digest of an in-memory buffer.
    pub fn of_bytes(bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Self::from_hasher(hasher)
    }

    /// Digest of a file's current bytes (streams; does not load it whole).
    pub fn of_file(path: &Path) -> R<Self> {
        let mut file =
            fs::File::open(path).with_context(|| format!("open {} to hash", path.display()))?;
        let mut hasher = Sha256::new();
        std::io::copy(&mut file, &mut HashWriter(&mut hasher))
            .with_context(|| format!("hash {}", path.display()))?;
        Ok(Self::from_hasher(hasher))
    }

    fn from_hasher(hasher: Sha256) -> Self {
        ContentDigest(format!("sha256:{:x}", hasher.finalize()))
    }

    /// The full `sha256:<hex>` form.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Just the hex part, which is the blob's file name.
    pub fn hex(&self) -> &str {
        &self.0["sha256:".len()..]
    }
}

impl fmt::Display for ContentDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ContentDigest {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> R<Self> {
        ContentDigest::parse(raw)
    }
}

/// Who holds a pin. Free-form but conventionally `<kind>:<id>`; use the
/// constructors so every caller spells owners the same way.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PinOwner(String);

impl PinOwner {
    /// An arbitrary owner. Must be non-empty and free of whitespace and
    /// control characters.
    pub fn new(raw: impl Into<String>) -> R<Self> {
        let raw = raw.into();
        if raw.is_empty() || raw.chars().any(|c| c.is_whitespace() || c.is_control()) {
            bail!("pin owner {raw:?} must be non-empty with no whitespace");
        }
        Ok(PinOwner(raw))
    }

    /// The owner for a snapshot's parts: `snapshot:<id>`.
    pub fn snapshot(snapshot_id: &str) -> Self {
        PinOwner(format!("snapshot:{snapshot_id}"))
    }

    /// The owner for content an allocation holds: `allocation:<id>`.
    pub fn allocation(allocation_id: i64) -> Self {
        PinOwner(format!("allocation:{allocation_id}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PinOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One class budget as configured: a fixed size (`"50GiB"`) or a share of
/// the filesystem holding the store (`"20%"`). A share is resolved once, when
/// the worker opens its store, against that filesystem's total size.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BudgetSpec {
    /// A fixed number of bytes.
    Bytes(u64),
    /// `percent` of the filesystem's total size, clamped to `min..=max`. An
    /// operator's `"20%"` has no clamp (`0..=u64::MAX`), only the built-in
    /// defaults carry one.
    Share { percent: f64, min: u64, max: u64 },
}

impl BudgetSpec {
    /// Parses a budget: a percentage (`"20%"`, `"12.5%"`, above 0 and at
    /// most 100) or a byte size (`"50GiB"`).
    pub fn parse(raw: &str) -> R<Self> {
        let trimmed = raw.trim();
        if let Some(number) = trimmed.strip_suffix('%') {
            let percent: f64 = number
                .trim()
                .parse()
                .with_context(|| format!("invalid percentage {raw:?}"))?;
            if !(percent > 0.0 && percent <= 100.0) {
                bail!("percentage {raw:?} must be above 0% and at most 100%");
            }
            return Ok(BudgetSpec::Share {
                percent,
                min: 0,
                max: u64::MAX,
            });
        }
        Ok(BudgetSpec::Bytes(crate::structs::parse_byte_size(trimmed)?))
    }

    /// The share of the filesystem this budget claims, if it is a share.
    pub fn percent(&self) -> Option<f64> {
        match self {
            BudgetSpec::Bytes(_) => None,
            BudgetSpec::Share { percent, .. } => Some(*percent),
        }
    }

    /// The budget in bytes on a filesystem of `filesystem_bytes` total.
    pub fn resolve(&self, filesystem_bytes: u64) -> u64 {
        match *self {
            BudgetSpec::Bytes(bytes) => bytes,
            BudgetSpec::Share { percent, min, max } => {
                let share = (filesystem_bytes as f64 * percent / 100.0) as u64;
                share.clamp(min, max.max(min))
            }
        }
    }
}

impl fmt::Display for BudgetSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            BudgetSpec::Bytes(bytes) => write!(f, "{bytes} bytes"),
            BudgetSpec::Share { percent, min, max } if min == 0 && max == u64::MAX => {
                write!(f, "{percent}% of the disk")
            }
            BudgetSpec::Share { percent, min, max } => write!(
                f,
                "{percent}% of the disk, clamped to {} GiB..{} GiB",
                min / GIB,
                max / GIB
            ),
        }
    }
}

/// Budgets as configured, before they are sized against a filesystem.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ContentBudgetSpecs {
    pub artifacts: BudgetSpec,
    pub snapshots: BudgetSpec,
}

impl Default for ContentBudgetSpecs {
    fn default() -> Self {
        ContentBudgetSpecs {
            artifacts: DEF_CONTENT_ARTIFACTS_BUDGET,
            snapshots: DEF_CONTENT_SNAPSHOTS_BUDGET,
        }
    }
}

/// Store settings as configured (worker config `content`, defaults applied,
/// validated), before shares are sized against the content filesystem.
#[derive(Debug, Clone, PartialEq)]
pub struct ContentStoreSettings {
    pub budgets: ContentBudgetSpecs,
    pub ttl: Option<Duration>,
    pub min_free_disk: u64,
}

impl ContentStoreSettings {
    /// The store config for a filesystem of `filesystem_bytes` total. `None`
    /// (the size could not be read) is only an error when a budget is a
    /// share.
    pub fn config_for(&self, filesystem_bytes: Option<u64>) -> R<ContentStoreConfig> {
        let size = |class: ContentClass, spec: BudgetSpec| -> R<u64> {
            match (spec, filesystem_bytes) {
                (BudgetSpec::Bytes(bytes), _) => Ok(bytes),
                (spec, Some(total)) => Ok(spec.resolve(total)),
                (spec, None) => bail!(
                    "content.budgets.{class} is {spec}, but the size of the content \
                     filesystem could not be read. Set a fixed size such as \"20GiB\""
                ),
            }
        };
        Ok(ContentStoreConfig {
            budgets: ContentBudgets {
                artifacts: size(ContentClass::Artifacts, self.budgets.artifacts)?,
                snapshots: size(ContentClass::Snapshots, self.budgets.snapshots)?,
            },
            ttl: self.ttl,
            min_free_disk: self.min_free_disk,
        })
    }
}

/// Byte budget per class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentBudgets {
    pub artifacts: u64,
    pub snapshots: u64,
}

impl ContentBudgets {
    pub fn for_class(&self, class: ContentClass) -> u64 {
        match class {
            ContentClass::Artifacts => self.artifacts,
            ContentClass::Snapshots => self.snapshots,
        }
    }
}

/// Resolved store settings: [`ContentStoreSettings`] sized against the
/// content filesystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentStoreConfig {
    pub budgets: ContentBudgets,
    /// Unpinned entries unused for this long are removed by [`ContentStore::gc`].
    /// `None` disables TTL expiry (LRU under budget still applies).
    pub ttl: Option<Duration>,
    /// A put that would leave the filesystem with less free space than this
    /// evicts unpinned entries first, then fails.
    pub min_free_disk: u64,
}

/// The built-in defaults as they resolve on a 1 TiB filesystem (both shares
/// hit their caps). Fixed numbers, so tests do not depend on the disk they
/// run on.
impl Default for ContentStoreConfig {
    fn default() -> Self {
        ContentStoreConfig {
            budgets: ContentBudgets {
                artifacts: 20 * GIB,
                snapshots: 200 * GIB,
            },
            ttl: Some(Duration::from_secs(72 * 3600)),
            min_free_disk: 5 * GIB,
        }
    }
}

/// One committed blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentEntry {
    pub digest: ContentDigest,
    pub class: ContentClass,
    pub size: u64,
    /// Absolute path of the blob. Treat it as read-only: the store makes it
    /// mode 0444, and a modified blob fails [`ContentStore::verify`].
    pub path: PathBuf,
    /// Display label from the first put (an artifact file name, a snapshot
    /// part name). Informational only; never part of the key.
    pub label: Option<String>,
    /// Unix seconds.
    pub created_at: i64,
    /// Unix seconds of the last put or [`ContentStore::get`].
    pub last_used_at: i64,
    /// Whether any owner pins this entry.
    pub pinned: bool,
}

/// Options for every put flavor.
#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    /// Fail (and discard the bytes) unless they hash to this digest.
    pub expected: Option<ContentDigest>,
    /// Pin the entry for this owner in the same step as the commit.
    pub pin: Option<PinOwner>,
    /// Display label recorded on first put.
    pub label: Option<String>,
}

impl PutOptions {
    pub fn expect(mut self, digest: ContentDigest) -> Self {
        self.expected = Some(digest);
        self
    }

    pub fn pin(mut self, owner: PinOwner) -> Self {
        self.pin = Some(owner);
        self
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
}

/// What a [`ContentStore::gc`] pass removed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Removed for being unused longer than the TTL.
    pub expired: Vec<ContentDigest>,
    /// Removed to bring a class back under its budget.
    pub evicted: Vec<ContentDigest>,
    pub bytes_freed: u64,
}

/// Per-class accounting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassUsage {
    pub class: ContentClass,
    pub budget: u64,
    pub used_bytes: u64,
    pub pinned_bytes: u64,
    pub entries: u64,
}

/// Probe for free bytes on the filesystem holding a path.
type FreeSpaceProbe = Box<dyn Fn(&Path) -> Option<u64> + Send + Sync>;

/// The node content store. Cheap to share behind an `Arc`; every method takes
/// `&self` and is safe to call from many threads. Methods block (file I/O and
/// sqlite), so call them from blocking threads, not the async runtime.
/// The worker's node content store, registered once it opens, for code that
/// runs below the staging pipeline and has no handle threaded to it: the
/// image pull a `container` or `sandbox` task makes when its image was not
/// staged as an artifact ([`crate::task_plugins`]' `oci_image`). `None` when
/// the store is disabled or this process is not a worker.
static NODE_STORE: std::sync::RwLock<Option<std::sync::Arc<ContentStore>>> =
    std::sync::RwLock::new(None);

/// Registers (or, with `None`, clears) this process's node content store.
pub fn set_node_store(store: Option<std::sync::Arc<ContentStore>>) {
    *NODE_STORE
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = store;
}

/// This process's node content store, when a worker registered one.
pub fn node_store() -> Option<std::sync::Arc<ContentStore>> {
    NODE_STORE
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

pub struct ContentStore {
    root: PathBuf,
    config: ContentStoreConfig,
    inner: Mutex<Inner>,
    free_space: FreeSpaceProbe,
}

struct Inner {
    db: Connection,
    /// Monotonic use counter: LRU order without relying on clock resolution.
    seq: i64,
}

impl fmt::Debug for ContentStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContentStore")
            .field("root", &self.root)
            .field("config", &self.config)
            .finish()
    }
}

impl ContentStore {
    /// Opens (or creates) the store rooted at `root`. Clears leftover
    /// in-flight writes, then reconciles the index with the blobs on disk:
    /// rows whose blob is missing or the wrong size are dropped, and blobs
    /// without a row are deleted.
    pub fn open(root: impl Into<PathBuf>, config: ContentStoreConfig) -> R<Self> {
        let root = root.into();
        fs::create_dir_all(root.join(BLOB_DIR))
            .with_context(|| format!("create content store {}", root.display()))?;
        let tmp = root.join(TMP_DIR);
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp)?;

        let db = Connection::open(root.join(INDEX_FILE))
            .with_context(|| format!("open content index in {}", root.display()))?;
        db.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE IF NOT EXISTS entry(
               digest TEXT PRIMARY KEY,
               class TEXT NOT NULL,
               size INTEGER NOT NULL,
               label TEXT,
               created_at INTEGER NOT NULL,
               last_used_at INTEGER NOT NULL,
               use_seq INTEGER NOT NULL);
             CREATE INDEX IF NOT EXISTS entry_class_lru ON entry(class, use_seq);
             CREATE TABLE IF NOT EXISTS pin(
               digest TEXT NOT NULL,
               owner TEXT NOT NULL,
               created_at INTEGER NOT NULL,
               PRIMARY KEY(digest, owner));
             CREATE INDEX IF NOT EXISTS pin_owner ON pin(owner);
             -- Where this node published each digest (src/content_publish.rs).
             -- Independent of `entry`: a published copy outlives local eviction.
             CREATE TABLE IF NOT EXISTS published(
               digest TEXT PRIMARY KEY,
               url TEXT NOT NULL,
               published_at INTEGER NOT NULL);
             -- The manifest digest each image tag last resolved to on this
             -- node (src/artifacts/fetchers/oci.rs). Lets a start reuse
             -- cached layers when the registry cannot be reached.
             CREATE TABLE IF NOT EXISTS image_ref(
               reference TEXT PRIMARY KEY,
               digest TEXT NOT NULL,
               resolved_at INTEGER NOT NULL);",
        )
        .context("initialize content index")?;
        let seq: i64 = db.query_row("SELECT COALESCE(MAX(use_seq), 0) FROM entry", [], |r| {
            r.get(0)
        })?;
        let store = ContentStore {
            root,
            config,
            inner: Mutex::new(Inner { db, seq }),
            free_space: Box::new(free_bytes),
        };
        store.reconcile()?;
        Ok(store)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn config(&self) -> &ContentStoreConfig {
        &self.config
    }

    /// Free bytes on the filesystem holding the store, as `minFreeDisk`
    /// measures them. `None` when the platform cannot say.
    pub fn free_disk_bytes(&self) -> Option<u64> {
        (self.free_space)(&self.root)
    }

    /// Where the blob for `digest` lives (whether or not it is present).
    pub fn path_of(&self, digest: &ContentDigest) -> PathBuf {
        self.root.join(BLOB_DIR).join(digest.hex())
    }

    /// A fresh path in the store's tmp dir, on the same filesystem as the
    /// blobs, for a producer that must write a file itself (a VMM writing a
    /// memory snapshot). Hand the finished file to [`Self::import_file`],
    /// which then only renames. Files left in tmp are deleted on next open.
    pub fn staging_path(&self, name_hint: &str) -> R<PathBuf> {
        let safe: String = name_hint
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .take(64)
            .collect();
        Ok(self.root.join(TMP_DIR).join(format!(
            "{}-{}",
            crate::util::random_string(12),
            if safe.is_empty() { "part".into() } else { safe }
        )))
    }

    // ---- puts ------------------------------------------------------------

    /// A streaming writer: write bytes, then [`ContentWriter::finish`] to
    /// hash-and-commit. Dropping it without finishing discards the bytes.
    pub fn writer(&self, class: ContentClass, opts: PutOptions) -> R<ContentWriter<'_>> {
        let tmp = self.staging_path("write")?;
        let file = fs::File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        Ok(ContentWriter {
            store: self,
            class,
            opts,
            tmp,
            file: Some(file),
            hasher: Sha256::new(),
            bytes: 0,
        })
    }

    /// Streams `reader` into the store.
    pub fn put_reader(
        &self,
        class: ContentClass,
        mut reader: impl Read,
        opts: PutOptions,
    ) -> R<ContentEntry> {
        let mut writer = self.writer(class, opts)?;
        std::io::copy(&mut reader, &mut writer).context("write into content store")?;
        writer.finish()
    }

    /// Stores an in-memory buffer.
    pub fn put_bytes(
        &self,
        class: ContentClass,
        bytes: &[u8],
        opts: PutOptions,
    ) -> R<ContentEntry> {
        self.put_reader(class, bytes, opts)
    }

    /// Copies `src` into the store, leaving `src` in place. Uses
    /// `std::fs::copy`, which clones the file on filesystems that support it
    /// (APFS, btrfs, XFS) instead of copying bytes.
    pub fn put_file(&self, class: ContentClass, src: &Path, opts: PutOptions) -> R<ContentEntry> {
        let tmp = self.staging_path("copy")?;
        fs::copy(src, &tmp)
            .with_context(|| format!("copy {} into content store", src.display()))?;
        self.hash_and_commit(tmp, class, opts)
    }

    /// Moves `src` into the store. A rename when `src` is on the store's
    /// filesystem (always true for [`Self::staging_path`]), else copy and
    /// delete. Once the bytes are moved, `src` is gone whether the commit
    /// succeeds or not (a digest mismatch or full budget discards them). If
    /// the move itself fails, `src` is left in place.
    pub fn import_file(
        &self,
        class: ContentClass,
        src: &Path,
        opts: PutOptions,
    ) -> R<ContentEntry> {
        let tmp = self.staging_path("import")?;
        if fs::rename(src, &tmp).is_err() {
            if let Err(err) = fs::copy(src, &tmp) {
                let _ = fs::remove_file(&tmp);
                return Err(err)
                    .with_context(|| format!("move {} into content store", src.display()));
            }
            let _ = fs::remove_file(src);
        }
        self.hash_and_commit(tmp, class, opts)
    }

    fn hash_and_commit(
        &self,
        tmp: PathBuf,
        class: ContentClass,
        opts: PutOptions,
    ) -> R<ContentEntry> {
        // Restores check a blob's size, not its hash, so the bytes must be on
        // disk before the rename commits them. Otherwise a power loss can leave
        // a right-sized blob of zeros that passes `blob_intact`.
        let hashed = fs::File::open(&tmp)
            .and_then(|file| file.sync_all())
            .map_err(anyhow::Error::from)
            .and_then(|()| ContentDigest::of_file(&tmp))
            .and_then(|digest| {
                let size = fs::metadata(&tmp)?.len();
                Ok((digest, size))
            });
        match hashed {
            Ok((digest, size)) => self.commit(tmp, digest, size, class, opts),
            Err(err) => {
                let _ = fs::remove_file(&tmp);
                Err(err)
            }
        }
    }

    /// Moves a fully written, hashed tmp file into place. Idempotent: if the
    /// digest is already present and intact, the new bytes are discarded and
    /// the existing entry is touched (and pinned, if asked).
    fn commit(
        &self,
        tmp: PathBuf,
        digest: ContentDigest,
        size: u64,
        class: ContentClass,
        opts: PutOptions,
    ) -> R<ContentEntry> {
        let result = self.commit_inner(&tmp, &digest, size, class, &opts);
        // On success tmp was renamed away or is a duplicate; either way it
        // must not linger.
        let _ = fs::remove_file(&tmp);
        result
    }

    fn commit_inner(
        &self,
        tmp: &Path,
        digest: &ContentDigest,
        size: u64,
        class: ContentClass,
        opts: &PutOptions,
    ) -> R<ContentEntry> {
        if let Some(expected) = opts.expected.as_ref() {
            if expected != digest {
                bail!(
                    "content digest mismatch: expected {expected}, bytes hash to {digest} \
                     (discarded)"
                );
            }
        }
        let now = now_secs();
        let mut inner = self.inner.lock().expect("content store");

        if let Some(existing) = self.load_entry(&inner.db, digest)? {
            if self.blob_intact(&existing) {
                self.touch(&mut inner, digest, now)?;
                if let Some(owner) = opts.pin.as_ref() {
                    insert_pin(&inner.db, digest, owner, now)?;
                }
                return self
                    .load_entry(&inner.db, digest)?
                    .context("content entry vanished during put");
            }
            // The recorded blob is damaged; these bytes repair it.
            warn!(digest = %digest, "Replacing damaged content store blob");
            self.drop_entry(&inner.db, digest)?;
        }

        let budget = self.config.budgets.for_class(class);
        if size > budget {
            bail!(
                "content of {size} bytes exceeds the whole {class} budget ({budget} bytes); \
                 raise content.budgets.{class}"
            );
        }
        self.make_room(&mut inner, class, size, digest)?;
        self.ensure_free_disk(&mut inner, digest)?;

        let blob = self.path_of(digest);
        fs::rename(tmp, &blob)
            .with_context(|| format!("commit content blob {}", blob.display()))?;
        set_read_only(&blob);
        inner.seq += 1;
        inner.db.execute(
            "INSERT INTO entry(digest, class, size, label, created_at, last_used_at, use_seq)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6)",
            params![
                digest.as_str(),
                class.as_str(),
                size as i64,
                opts.label,
                now,
                inner.seq
            ],
        )?;
        if let Some(owner) = opts.pin.as_ref() {
            insert_pin(&inner.db, digest, owner, now)?;
        }
        debug!(digest = %digest, %class, size, "Stored content");
        self.load_entry(&inner.db, digest)?
            .context("content entry vanished after commit")
    }

    /// Evicts LRU unpinned entries of `class` until `incoming` more bytes fit.
    fn make_room(
        &self,
        inner: &mut Inner,
        class: ContentClass,
        incoming: u64,
        keep: &ContentDigest,
    ) -> R<()> {
        let budget = self.config.budgets.for_class(class);
        let mut used = class_used(&inner.db, class)?;
        if used + incoming <= budget {
            return Ok(());
        }
        for (digest, size) in lru_unpinned(&inner.db, Some(class))? {
            if &digest == keep {
                continue;
            }
            self.remove_blob_and_row(&inner.db, &digest)?;
            info!(digest = %digest, %class, size, "Evicted content (budget)");
            used = used.saturating_sub(size);
            if used + incoming <= budget {
                return Ok(());
            }
        }
        bail!(
            "content budget exhausted: {class} holds {used} pinned bytes of a {budget}-byte \
             budget, no room for {incoming} more; raise content.budgets.{class} or release \
             pinned content"
        )
    }

    /// Evicts LRU unpinned entries of any class while the filesystem is below
    /// `min_free_disk`.
    fn ensure_free_disk(&self, inner: &mut Inner, keep: &ContentDigest) -> R<()> {
        let floor = self.config.min_free_disk;
        if floor == 0 {
            return Ok(());
        }
        let below =
            |store: &ContentStore| (store.free_space)(&store.root).is_some_and(|free| free < floor);
        if !below(self) {
            return Ok(());
        }
        for (digest, size) in lru_unpinned(&inner.db, None)? {
            if &digest == keep {
                continue;
            }
            self.remove_blob_and_row(&inner.db, &digest)?;
            info!(digest = %digest, size, "Evicted content (minFreeDisk)");
            if !below(self) {
                return Ok(());
            }
        }
        // Name what statvfs reported, so the refusal can be checked against
        // `df -B1` on the node.
        let free = (self.free_space)(&self.root)
            .map(|free| format!("{free} bytes free"))
            .unwrap_or_else(|| "free space unknown".to_string());
        bail!(
            "node disk is below content.minFreeDisk ({free}, floor {floor} bytes) and no \
             unpinned content is left to evict"
        )
    }

    // ---- reads -----------------------------------------------------------

    /// Looks up a digest and marks it used (LRU). Cheap: checks the blob
    /// exists with the recorded size, but does not rehash. A blob that is
    /// missing or the wrong size is dropped and reported as absent.
    pub fn get(&self, digest: &ContentDigest) -> R<Option<ContentEntry>> {
        let mut inner = self.inner.lock().expect("content store");
        let Some(entry) = self.load_entry(&inner.db, digest)? else {
            return Ok(None);
        };
        if !self.blob_intact(&entry) {
            warn!(digest = %digest, "Content store blob missing or truncated; dropping entry");
            self.drop_entry(&inner.db, digest)?;
            return Ok(None);
        }
        self.touch(&mut inner, digest, now_secs())?;
        self.load_entry(&inner.db, digest)
    }

    /// Whether the index has the digest. Does not touch LRU or check bytes.
    pub fn contains(&self, digest: &ContentDigest) -> R<bool> {
        let inner = self.inner.lock().expect("content store");
        Ok(self.load_entry(&inner.db, digest)?.is_some())
    }

    /// Rehashes the blob. On a mismatch the entry is dropped (pins are kept)
    /// and this returns an error naming both digests.
    pub fn verify(&self, digest: &ContentDigest) -> R<ContentEntry> {
        let entry = self
            .get(digest)?
            .with_context(|| format!("content {digest} is not in the node store"))?;
        let actual = ContentDigest::of_file(&entry.path)?;
        if &actual != digest {
            self.quarantine(digest)?;
            bail!("content {digest} is corrupt (blob hashes to {actual}); dropped from the store");
        }
        Ok(entry)
    }

    /// Copies the blob to `dest` (a clone where the filesystem supports it),
    /// verifies the copy's digest, and makes it writable (0644) like a fresh
    /// download. On a mismatch, `dest` is removed, the store entry is
    /// dropped, and this fails.
    pub fn copy_out(&self, digest: &ContentDigest, dest: &Path) -> R<ContentEntry> {
        let entry = self
            .get(digest)?
            .with_context(|| format!("content {digest} is not in the node store"))?;
        fs::copy(&entry.path, dest)
            .with_context(|| format!("copy content {digest} to {}", dest.display()))?;
        let actual = ContentDigest::of_file(dest)?;
        if &actual != digest {
            let _ = fs::remove_file(dest);
            self.quarantine(digest)?;
            bail!("content {digest} is corrupt (blob hashes to {actual}); dropped from the store");
        }
        set_writable(dest);
        Ok(entry)
    }

    /// Every entry, most recently used first.
    pub fn list(&self) -> R<Vec<ContentEntry>> {
        let inner = self.inner.lock().expect("content store");
        let mut stmt = inner
            .db
            .prepare("SELECT digest FROM entry ORDER BY use_seq DESC")?;
        let digests = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let mut entries = Vec::with_capacity(digests.len());
        for raw in digests {
            if let Some(entry) = self.load_entry(&inner.db, &ContentDigest::parse(&raw)?)? {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    /// Every entry of at least `min_bytes`, with its size, in one query.
    /// What the worker advertises to the manager's location index on each
    /// heartbeat, so it stays cheap at thousands of entries.
    pub fn sizes_at_least(&self, min_bytes: u64) -> R<Vec<(ContentDigest, u64)>> {
        let inner = self.inner.lock().expect("content store");
        let mut stmt = inner
            .db
            .prepare_cached("SELECT digest, size FROM entry WHERE size >= ?1")?;
        let rows = stmt
            .query_map(params![min_bytes as i64], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(digest, size)| Ok((ContentDigest::parse(&digest)?, size.max(0) as u64)))
            .collect()
    }

    /// Record that `digest` is published at `url` (replacing any earlier
    /// record). See [`crate::content_publish`].
    pub fn record_published(&self, digest: &ContentDigest, url: &str) -> R<()> {
        let inner = self.inner.lock().expect("content store");
        inner.db.execute(
            "INSERT INTO published(digest, url, published_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(digest) DO UPDATE SET url = excluded.url,
               published_at = excluded.published_at",
            params![digest.as_str(), url, now_secs()],
        )?;
        Ok(())
    }

    /// Where `digest` was published from this node, if it was.
    pub fn published_url(&self, digest: &ContentDigest) -> R<Option<String>> {
        let inner = self.inner.lock().expect("content store");
        Ok(inner
            .db
            .query_row(
                "SELECT url FROM published WHERE digest = ?1",
                params![digest.as_str()],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Record that the image `reference` (a tag) resolved to the manifest
    /// `digest` just now, replacing any earlier record.
    pub fn record_image_ref(&self, reference: &str, digest: &ContentDigest) -> R<()> {
        let inner = self.inner.lock().expect("content store");
        inner.db.execute(
            "INSERT INTO image_ref(reference, digest, resolved_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(reference) DO UPDATE SET digest = excluded.digest,
               resolved_at = excluded.resolved_at",
            params![reference, digest.as_str(), now_secs()],
        )?;
        Ok(())
    }

    /// The manifest digest `reference` last resolved to on this node, and
    /// when (Unix seconds). Says nothing about whether its blobs are still
    /// held; look each one up.
    pub fn image_ref(&self, reference: &str) -> R<Option<(ContentDigest, i64)>> {
        let inner = self.inner.lock().expect("content store");
        let row: Option<(String, i64)> = inner
            .db
            .query_row(
                "SELECT digest, resolved_at FROM image_ref WHERE reference = ?1",
                params![reference],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        row.map(|(digest, at)| Ok((ContentDigest::parse(&digest)?, at)))
            .transpose()
    }

    /// Bytes used, pinned, and budgeted per class.
    pub fn usage(&self) -> R<Vec<ClassUsage>> {
        let inner = self.inner.lock().expect("content store");
        ContentClass::ALL
            .iter()
            .map(|&class| {
                let (entries, used): (i64, i64) = inner.db.query_row(
                    "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM entry WHERE class = ?1",
                    [class.as_str()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                let pinned: i64 = inner.db.query_row(
                    "SELECT COALESCE(SUM(size), 0) FROM entry WHERE class = ?1
                     AND EXISTS (SELECT 1 FROM pin WHERE pin.digest = entry.digest)",
                    [class.as_str()],
                    |r| r.get(0),
                )?;
                Ok(ClassUsage {
                    class,
                    budget: self.config.budgets.for_class(class),
                    used_bytes: used as u64,
                    pinned_bytes: pinned as u64,
                    entries: entries as u64,
                })
            })
            .collect()
    }

    // ---- pins ------------------------------------------------------------

    /// Pins a present digest for `owner`. Idempotent. Fails if the digest is
    /// not in the store: pin in the put itself ([`PutOptions::pin`]) to avoid
    /// a window where GC could remove fresh content.
    pub fn pin(&self, digest: &ContentDigest, owner: &PinOwner) -> R<()> {
        let inner = self.inner.lock().expect("content store");
        if self.load_entry(&inner.db, digest)?.is_none() {
            bail!("cannot pin {digest} for {owner}: not in the node store");
        }
        insert_pin(&inner.db, digest, owner, now_secs())
    }

    /// Pins `digest` for `owner` if the store holds it, in one step, so
    /// eviction cannot slip between a presence check and the pin. Returns
    /// whether it pinned. Unlike [`Self::pin`], absence is not an error: the
    /// caller fetches the bytes instead.
    pub fn pin_if_present(&self, digest: &ContentDigest, owner: &PinOwner) -> R<bool> {
        let inner = self.inner.lock().expect("content store");
        if self.load_entry(&inner.db, digest)?.is_none() {
            return Ok(false);
        }
        insert_pin(&inner.db, digest, owner, now_secs())?;
        Ok(true)
    }

    /// Removes one pin. Returns whether it existed. The entry stays and
    /// becomes an eviction candidate once no pins remain.
    pub fn unpin(&self, digest: &ContentDigest, owner: &PinOwner) -> R<bool> {
        let inner = self.inner.lock().expect("content store");
        let removed = inner.db.execute(
            "DELETE FROM pin WHERE digest = ?1 AND owner = ?2",
            params![digest.as_str(), owner.as_str()],
        )?;
        Ok(removed > 0)
    }

    /// Removes every pin `owner` holds (a snapshot deleted, an allocation
    /// destroyed). Returns how many pins were removed.
    pub fn release_owner(&self, owner: &PinOwner) -> R<usize> {
        let inner = self.inner.lock().expect("content store");
        Ok(inner
            .db
            .execute("DELETE FROM pin WHERE owner = ?1", [owner.as_str()])?)
    }

    /// Owners pinning `digest`.
    pub fn pins(&self, digest: &ContentDigest) -> R<Vec<PinOwner>> {
        let inner = self.inner.lock().expect("content store");
        let mut stmt = inner
            .db
            .prepare("SELECT owner FROM pin WHERE digest = ?1 ORDER BY owner")?;
        let owners = stmt
            .query_map([digest.as_str()], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(owners.into_iter().map(PinOwner).collect())
    }

    /// Digests `owner` pins. Includes digests whose bytes were since lost
    /// (dropped as corrupt or missing), so a caller can tell a snapshot is
    /// incomplete by checking each with [`Self::contains`].
    pub fn pinned_by(&self, owner: &PinOwner) -> R<Vec<ContentDigest>> {
        let inner = self.inner.lock().expect("content store");
        let mut stmt = inner
            .db
            .prepare("SELECT digest FROM pin WHERE owner = ?1 ORDER BY digest")?;
        let digests = stmt
            .query_map([owner.as_str()], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        digests
            .iter()
            .map(|raw| ContentDigest::parse(raw))
            .collect()
    }

    // ---- removal ---------------------------------------------------------

    /// Removes an unpinned entry now. Fails for a pinned entry. Returns
    /// whether anything was removed.
    pub fn remove(&self, digest: &ContentDigest) -> R<bool> {
        let inner = self.inner.lock().expect("content store");
        if self.load_entry(&inner.db, digest)?.is_none() {
            return Ok(false);
        }
        if is_pinned(&inner.db, digest)? {
            bail!("content {digest} is pinned; release its pins before removing it");
        }
        self.remove_blob_and_row(&inner.db, digest)?;
        Ok(true)
    }

    /// Expires unpinned entries unused longer than the TTL, then evicts LRU
    /// unpinned entries from any class over its budget (a budget lowered
    /// across a restart). The worker runs this on an interval.
    pub fn gc(&self) -> R<GcReport> {
        self.gc_at(now_secs())
    }

    fn gc_at(&self, now: i64) -> R<GcReport> {
        let inner = self.inner.lock().expect("content store");
        let mut report = GcReport::default();
        if let Some(ttl) = self.config.ttl {
            let cutoff = now - ttl.as_secs() as i64;
            for (digest, size) in lru_unpinned(&inner.db, None)? {
                let last_used: i64 = inner.db.query_row(
                    "SELECT last_used_at FROM entry WHERE digest = ?1",
                    [digest.as_str()],
                    |r| r.get(0),
                )?;
                if last_used <= cutoff {
                    self.remove_blob_and_row(&inner.db, &digest)?;
                    report.bytes_freed += size;
                    report.expired.push(digest);
                }
            }
        }
        for class in ContentClass::ALL {
            let budget = self.config.budgets.for_class(class);
            let mut used = class_used(&inner.db, class)?;
            if used <= budget {
                continue;
            }
            for (digest, size) in lru_unpinned(&inner.db, Some(class))? {
                self.remove_blob_and_row(&inner.db, &digest)?;
                report.bytes_freed += size;
                report.evicted.push(digest);
                used = used.saturating_sub(size);
                if used <= budget {
                    break;
                }
            }
        }
        if !report.expired.is_empty() || !report.evicted.is_empty() {
            info!(
                expired = report.expired.len(),
                evicted = report.evicted.len(),
                bytes_freed = report.bytes_freed,
                "Content store GC"
            );
        }
        Ok(report)
    }

    // ---- internals -------------------------------------------------------

    fn reconcile(&self) -> R<()> {
        let inner = self.inner.lock().expect("content store");
        let mut stmt = inner.db.prepare("SELECT digest FROM entry")?;
        let known = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(stmt);
        let mut known_hex = std::collections::HashSet::new();
        for raw in known {
            let digest = ContentDigest::parse(&raw)?;
            let entry = self.load_entry(&inner.db, &digest)?;
            match entry {
                Some(entry) if self.blob_intact(&entry) => {
                    known_hex.insert(digest.hex().to_string());
                }
                Some(entry) => {
                    if entry.pinned {
                        error!(digest = %digest, "Pinned content blob missing or truncated");
                    } else {
                        warn!(digest = %digest, "Content blob missing or truncated; dropping");
                    }
                    self.drop_entry(&inner.db, &digest)?;
                }
                None => {}
            }
        }
        for dirent in fs::read_dir(self.root.join(BLOB_DIR))? {
            let dirent = dirent?;
            let name = dirent.file_name().to_string_lossy().into_owned();
            if !known_hex.contains(&name) {
                debug!(blob = %name, "Removing unindexed content blob");
                let _ = fs::remove_file(dirent.path());
            }
        }
        Ok(())
    }

    fn blob_intact(&self, entry: &ContentEntry) -> bool {
        fs::metadata(&entry.path).is_ok_and(|meta| meta.is_file() && meta.len() == entry.size)
    }

    fn touch(&self, inner: &mut Inner, digest: &ContentDigest, now: i64) -> R<()> {
        inner.seq += 1;
        inner.db.execute(
            "UPDATE entry SET last_used_at = ?2, use_seq = ?3 WHERE digest = ?1",
            params![digest.as_str(), now, inner.seq],
        )?;
        Ok(())
    }

    /// Drops a damaged entry: blob and row go, pins stay.
    fn quarantine(&self, digest: &ContentDigest) -> R<()> {
        let inner = self.inner.lock().expect("content store");
        error!(digest = %digest, "Content store blob failed digest verification; dropping");
        self.drop_entry(&inner.db, digest)
    }

    fn drop_entry(&self, db: &Connection, digest: &ContentDigest) -> R<()> {
        let _ = fs::remove_file(self.path_of(digest));
        db.execute("DELETE FROM entry WHERE digest = ?1", [digest.as_str()])?;
        Ok(())
    }

    fn remove_blob_and_row(&self, db: &Connection, digest: &ContentDigest) -> R<()> {
        self.drop_entry(db, digest)?;
        // Only unpinned entries reach here, but stay tidy.
        db.execute("DELETE FROM pin WHERE digest = ?1", [digest.as_str()])?;
        Ok(())
    }

    fn load_entry(&self, db: &Connection, digest: &ContentDigest) -> R<Option<ContentEntry>> {
        let row = db
            .query_row(
                "SELECT class, size, label, created_at, last_used_at,
                        EXISTS (SELECT 1 FROM pin WHERE pin.digest = entry.digest)
                 FROM entry WHERE digest = ?1",
                [digest.as_str()],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, i64>(4)?,
                        r.get::<_, bool>(5)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(class, size, label, created_at, last_used_at, pinned)| {
            Ok(ContentEntry {
                digest: digest.clone(),
                class: class.parse()?,
                size: size as u64,
                path: self.path_of(digest),
                label,
                created_at,
                last_used_at,
                pinned,
            })
        })
        .transpose()
    }

    #[cfg(test)]
    fn with_free_space_probe(mut self, probe: FreeSpaceProbe) -> Self {
        self.free_space = probe;
        self
    }
}

/// Streaming put handle from [`ContentStore::writer`].
pub struct ContentWriter<'a> {
    store: &'a ContentStore,
    class: ContentClass,
    opts: PutOptions,
    tmp: PathBuf,
    file: Option<fs::File>,
    hasher: Sha256,
    bytes: u64,
}

impl ContentWriter<'_> {
    /// Bytes written so far.
    pub fn bytes_written(&self) -> u64 {
        self.bytes
    }

    /// Flushes, hashes, and commits the bytes as one entry.
    pub fn finish(mut self) -> R<ContentEntry> {
        let file = self
            .file
            .take()
            .context("content writer already finished")?;
        file.sync_all().ok();
        drop(file);
        let digest = ContentDigest::from_hasher(std::mem::take(&mut self.hasher));
        let tmp = std::mem::take(&mut self.tmp);
        let opts = std::mem::take(&mut self.opts);
        self.store.commit(tmp, digest, self.bytes, self.class, opts)
    }
}

impl Write for ContentWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| std::io::Error::other("content writer already finished"))?;
        let written = file.write(buf)?;
        self.hasher.update(&buf[..written]);
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self.file.as_mut() {
            Some(file) => file.flush(),
            None => Ok(()),
        }
    }
}

impl Drop for ContentWriter<'_> {
    fn drop(&mut self) {
        if self.file.is_some() {
            let _ = fs::remove_file(&self.tmp);
        }
    }
}

struct HashWriter<'a>(&'a mut Sha256);

impl Write for HashWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn insert_pin(db: &Connection, digest: &ContentDigest, owner: &PinOwner, now: i64) -> R<()> {
    db.execute(
        "INSERT OR IGNORE INTO pin(digest, owner, created_at) VALUES (?1, ?2, ?3)",
        params![digest.as_str(), owner.as_str(), now],
    )?;
    Ok(())
}

fn is_pinned(db: &Connection, digest: &ContentDigest) -> R<bool> {
    Ok(db.query_row(
        "SELECT EXISTS (SELECT 1 FROM pin WHERE digest = ?1)",
        [digest.as_str()],
        |r| r.get(0),
    )?)
}

fn class_used(db: &Connection, class: ContentClass) -> R<u64> {
    let used: i64 = db.query_row(
        "SELECT COALESCE(SUM(size), 0) FROM entry WHERE class = ?1",
        [class.as_str()],
        |r| r.get(0),
    )?;
    Ok(used as u64)
}

/// Unpinned entries, least recently used first, optionally for one class.
fn lru_unpinned(db: &Connection, class: Option<ContentClass>) -> R<Vec<(ContentDigest, u64)>> {
    let mut stmt = db.prepare(
        "SELECT digest, size FROM entry
         WHERE (?1 IS NULL OR class = ?1)
           AND NOT EXISTS (SELECT 1 FROM pin WHERE pin.digest = entry.digest)
         ORDER BY use_seq ASC",
    )?;
    let rows = stmt
        .query_map([class.map(ContentClass::as_str)], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(raw, size)| Ok((ContentDigest::parse(&raw)?, size as u64)))
        .collect()
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Bytes available to unprivileged users on the filesystem holding `path`,
/// as `minFreeDisk` measures them. `None` when the platform cannot say.
pub fn free_bytes(path: &Path) -> Option<u64> {
    statvfs(path).map(|(_, free)| free)
}

/// Total size in bytes of the filesystem holding `path`, which must exist.
/// What share budgets (`content.budgets.snapshots = "20%"`) are sized
/// against. `None` when the platform cannot say.
pub fn filesystem_bytes(path: &Path) -> Option<u64> {
    statvfs(path).map(|(total, _)| total)
}

/// `(total, available to unprivileged users)` bytes of the filesystem
/// holding `path`.
#[cfg(unix)]
fn statvfs(path: &Path) -> Option<(u64, u64)> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c_path` is a valid NUL-terminated string and `stat` is a
    // properly sized, writable out-parameter.
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if rc != 0 {
        return None;
    }
    #[allow(clippy::unnecessary_cast)]
    let frsize = stat.f_frsize as u64;
    #[allow(clippy::unnecessary_cast)]
    Some((stat.f_blocks as u64 * frsize, stat.f_bavail as u64 * frsize))
}

#[cfg(not(unix))]
fn statvfs(_path: &Path) -> Option<(u64, u64)> {
    None
}

/// Blobs are immutable. Read-only mode is a guard against accidents, not a
/// security boundary; digest verification is what catches damage.
fn set_read_only(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o444));
    }
    #[cfg(not(unix))]
    let _ = path;
}

fn set_writable(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o644));
    }
    #[cfg(not(unix))]
    let _ = path;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(artifacts: u64, snapshots: u64) -> ContentStoreConfig {
        ContentStoreConfig {
            budgets: ContentBudgets {
                artifacts,
                snapshots,
            },
            ttl: Some(Duration::from_secs(3600)),
            min_free_disk: 0,
        }
    }

    fn open(dir: &Path, cfg: ContentStoreConfig) -> ContentStore {
        ContentStore::open(dir.join("content"), cfg).expect("open store")
    }

    fn blob(fill: u8, len: usize) -> Vec<u8> {
        vec![fill; len]
    }

    #[test]
    fn digest_parsing_validates_and_normalizes() -> R<()> {
        let hex = "AB".repeat(32);
        let digest = ContentDigest::parse(&format!("sha256:{hex}"))?;
        assert_eq!(digest.hex(), "ab".repeat(32));
        assert!(ContentDigest::parse("sha256:abc").is_err());
        assert!(ContentDigest::parse(&"a".repeat(64)).is_err());
        assert!(ContentDigest::parse(&format!("sha256:{}", "g".repeat(64))).is_err());
        assert!(PinOwner::new("bad owner").is_err());
        assert_eq!(PinOwner::snapshot("s1").as_str(), "snapshot:s1");
        Ok(())
    }

    #[test]
    fn puts_are_idempotent() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = open(dir.path(), config(1000, 1000));
        let bytes = blob(1, 100);
        let first = store.put_bytes(ContentClass::Artifacts, &bytes, PutOptions::default())?;
        let second = store.put_bytes(
            ContentClass::Artifacts,
            &bytes,
            PutOptions::default().pin(PinOwner::snapshot("s")),
        )?;
        assert_eq!(first.digest, second.digest);
        assert_eq!(first.digest, ContentDigest::of_bytes(&bytes));
        assert!(second.pinned, "a repeat put can add a pin");
        let usage = store.usage()?;
        assert_eq!(usage[0].used_bytes, 100, "stored once");
        assert_eq!(usage[0].entries, 1);
        assert_eq!(fs::read(&first.path)?, bytes);
        // Nothing left behind in tmp.
        assert_eq!(fs::read_dir(store.root().join(TMP_DIR))?.count(), 0);
        Ok(())
    }

    #[test]
    fn expected_digest_mismatch_is_refused() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = open(dir.path(), config(1000, 1000));
        let wrong = ContentDigest::of_bytes(b"other");
        let err = store
            .put_bytes(
                ContentClass::Artifacts,
                b"bytes",
                PutOptions::default().expect(wrong),
            )
            .unwrap_err();
        assert!(format!("{err:#}").contains("digest mismatch"), "{err:#}");
        assert!(store.list()?.is_empty());
        Ok(())
    }

    #[test]
    fn lru_evicts_least_recently_used_first() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = open(dir.path(), config(100, 1000));
        let a = store.put_bytes(ContentClass::Artifacts, &blob(1, 40), PutOptions::default())?;
        let b = store.put_bytes(ContentClass::Artifacts, &blob(2, 40), PutOptions::default())?;
        // Touch `a`, so `b` is now least recently used.
        store.get(&a.digest)?.expect("a present");
        let c = store.put_bytes(ContentClass::Artifacts, &blob(3, 40), PutOptions::default())?;
        assert!(store.contains(&a.digest)?);
        assert!(!store.contains(&b.digest)?, "b was LRU and is evicted");
        assert!(!b.path.exists());
        assert!(store.contains(&c.digest)?);
        Ok(())
    }

    #[test]
    fn lru_never_evicts_a_pinned_entry() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = open(dir.path(), config(100, 100));
        let owner = PinOwner::snapshot("snap-1");
        let pinned = store.put_bytes(
            ContentClass::Snapshots,
            &blob(1, 60),
            PutOptions::default().pin(owner.clone()),
        )?;
        // Oldest and pinned: every later put must evict around it.
        let b = store.put_bytes(ContentClass::Snapshots, &blob(2, 30), PutOptions::default())?;
        let c = store.put_bytes(ContentClass::Snapshots, &blob(3, 30), PutOptions::default())?;
        assert!(store.contains(&pinned.digest)?);
        assert!(!store.contains(&b.digest)?);
        assert!(store.contains(&c.digest)?);

        // A put that needs the pinned bytes gone evicts everything unpinned
        // (`c`) and still fails rather than evicting them.
        let err = store
            .put_bytes(ContentClass::Snapshots, &blob(4, 50), PutOptions::default())
            .unwrap_err();
        assert!(format!("{err:#}").contains("budget exhausted"), "{err:#}");
        assert!(store.contains(&pinned.digest)?);
        assert!(!store.contains(&c.digest)?);

        // Neither TTL nor explicit removal touches it.
        let d = store.put_bytes(ContentClass::Snapshots, &blob(5, 10), PutOptions::default())?;
        let report = store.gc_at(now_secs() + 10 * 3600)?;
        assert!(!report.expired.contains(&pinned.digest));
        assert!(report.expired.contains(&d.digest));
        assert!(store.remove(&pinned.digest).is_err());

        // Released, it is an ordinary LRU candidate again.
        assert_eq!(store.release_owner(&owner)?, 1);
        store.put_bytes(ContentClass::Snapshots, &blob(4, 50), PutOptions::default())?;
        assert!(!store.contains(&pinned.digest)?);
        Ok(())
    }

    #[test]
    fn per_class_budgets_are_honored() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = open(dir.path(), config(100, 1000));
        let artifact =
            store.put_bytes(ContentClass::Artifacts, &blob(1, 90), PutOptions::default())?;
        // A big snapshot fills its own class without touching artifacts.
        store.put_bytes(
            ContentClass::Snapshots,
            &blob(2, 900),
            PutOptions::default(),
        )?;
        assert!(store.contains(&artifact.digest)?);
        // Snapshots evict snapshots.
        store.put_bytes(
            ContentClass::Snapshots,
            &blob(3, 900),
            PutOptions::default(),
        )?;
        assert!(store.contains(&artifact.digest)?);
        let usage = store.usage()?;
        assert_eq!(usage[0].used_bytes, 90);
        assert_eq!(usage[1].used_bytes, 900);
        // Bigger than the whole class budget is refused outright.
        let err = store
            .put_bytes(
                ContentClass::Artifacts,
                &blob(4, 101),
                PutOptions::default(),
            )
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("content.budgets.artifacts"),
            "{err:#}"
        );
        Ok(())
    }

    #[test]
    fn gc_expires_unused_entries_after_ttl() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = open(dir.path(), config(1000, 1000));
        let entry = store.put_bytes(ContentClass::Artifacts, b"hello", PutOptions::default())?;
        assert!(store.gc_at(now_secs() + 60)?.expired.is_empty());
        let report = store.gc_at(now_secs() + 3601)?;
        assert_eq!(report.expired, vec![entry.digest.clone()]);
        assert_eq!(report.bytes_freed, 5);
        assert!(!store.contains(&entry.digest)?);
        Ok(())
    }

    #[test]
    fn corrupt_entry_is_detected_by_digest() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = open(dir.path(), config(1000, 1000));
        let owner = PinOwner::snapshot("s");
        let entry = store.put_bytes(
            ContentClass::Snapshots,
            b"original",
            PutOptions::default().pin(owner.clone()),
        )?;
        store.verify(&entry.digest)?;

        // Same length, different bytes: only a rehash notices.
        set_writable(&entry.path);
        fs::write(&entry.path, b"tampered")?;
        assert!(store.get(&entry.digest)?.is_some(), "get is size-only");
        let dest = dir.path().join("out");
        let err = store.copy_out(&entry.digest, &dest).unwrap_err();
        assert!(format!("{err:#}").contains("corrupt"), "{err:#}");
        assert!(!dest.exists(), "corrupt bytes never reach the destination");
        assert!(!store.contains(&entry.digest)?, "corrupt entry dropped");
        assert_eq!(
            store.pinned_by(&owner)?,
            vec![entry.digest.clone()],
            "the pin survives so the caller can see the part is lost"
        );

        // Re-putting the good bytes repairs it, still pinned.
        let repaired =
            store.put_bytes(ContentClass::Snapshots, b"original", PutOptions::default())?;
        assert!(repaired.pinned);
        store.verify(&entry.digest)?;

        // A truncated blob is caught even by the cheap path.
        set_writable(&repaired.path);
        fs::write(&repaired.path, b"short")?;
        assert!(store.get(&entry.digest)?.is_none());
        Ok(())
    }

    #[test]
    fn copy_out_produces_a_writable_independent_copy() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = open(dir.path(), config(1000, 1000));
        let entry = store.put_bytes(ContentClass::Artifacts, b"payload", PutOptions::default())?;
        let dest = dir.path().join("copy");
        store.copy_out(&entry.digest, &dest)?;
        fs::write(&dest, b"changed by a task")?;
        store.verify(&entry.digest)?;
        Ok(())
    }

    #[test]
    fn import_file_moves_and_writer_streams() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = open(dir.path(), config(1000, 1000));
        let staged = store.staging_path("memory")?;
        fs::write(&staged, b"vm memory")?;
        let entry = store.import_file(
            ContentClass::Snapshots,
            &staged,
            PutOptions::default().label("memory"),
        )?;
        assert!(!staged.exists());
        assert_eq!(entry.label.as_deref(), Some("memory"));
        assert_eq!(entry.class, ContentClass::Snapshots);

        let mut writer = store.writer(ContentClass::Snapshots, PutOptions::default())?;
        writer.write_all(b"vm ")?;
        writer.write_all(b"memory")?;
        let streamed = writer.finish()?;
        assert_eq!(streamed.digest, entry.digest);

        // A dropped writer leaves nothing behind.
        let mut abandoned = store.writer(ContentClass::Snapshots, PutOptions::default())?;
        abandoned.write_all(b"partial")?;
        drop(abandoned);
        assert_eq!(fs::read_dir(store.root().join(TMP_DIR))?.count(), 0);
        Ok(())
    }

    #[test]
    fn pins_and_entries_survive_reopen_and_reconcile_cleans_up() -> R<()> {
        let dir = tempfile::tempdir()?;
        let owner = PinOwner::snapshot("keep");
        let (kept, lost) = {
            let store = open(dir.path(), config(1000, 1000));
            let kept = store.put_bytes(
                ContentClass::Snapshots,
                b"kept",
                PutOptions::default().pin(owner.clone()),
            )?;
            let lost = store.put_bytes(ContentClass::Artifacts, b"lost", PutOptions::default())?;
            (kept, lost)
        };
        fs::remove_file(&lost.path)?;
        let stray = dir
            .path()
            .join("content")
            .join(BLOB_DIR)
            .join("f".repeat(64));
        fs::write(&stray, b"stray")?;

        let store = open(dir.path(), config(1000, 1000));
        let entry = store.get(&kept.digest)?.expect("kept survives");
        assert!(entry.pinned);
        assert_eq!(store.pins(&kept.digest)?, vec![owner]);
        assert!(!store.contains(&lost.digest)?, "row without blob dropped");
        assert!(!stray.exists(), "blob without row deleted");
        Ok(())
    }

    #[test]
    fn pin_if_present_pins_only_what_is_there() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = open(dir.path(), config(1000, 1000));
        let owner = PinOwner::allocation(9);
        let entry = store.put_bytes(ContentClass::Snapshots, b"part", PutOptions::default())?;
        assert!(store.pin_if_present(&entry.digest, &owner)?);
        assert_eq!(store.pins(&entry.digest)?, vec![owner.clone()]);
        let absent = ContentDigest::of_bytes(b"evicted");
        assert!(!store.pin_if_present(&absent, &owner)?);
        assert!(store.pins(&absent)?.is_empty(), "no dangling pin");
        Ok(())
    }

    #[test]
    fn budget_specs_parse_and_size_against_the_disk() -> R<()> {
        const GIB: u64 = 1024 * 1024 * 1024;
        assert_eq!(BudgetSpec::parse("50GiB")?, BudgetSpec::Bytes(50 * GIB));
        assert_eq!(BudgetSpec::parse("25%")?.resolve(400 * GIB), 100 * GIB);
        assert!(BudgetSpec::parse("0%").is_err());
        assert!(BudgetSpec::parse("100.5%").is_err());
        // Defaults clamp, an operator's share does not.
        assert_eq!(DEF_CONTENT_SNAPSHOTS_BUDGET.resolve(GIB), 2 * GIB);
        assert_eq!(
            DEF_CONTENT_SNAPSHOTS_BUDGET.resolve(10_000 * GIB),
            200 * GIB
        );
        assert_eq!(BudgetSpec::parse("20%")?.resolve(10_000 * GIB), 2_000 * GIB);
        assert_eq!(
            DEF_CONTENT_ARTIFACTS_BUDGET.to_string(),
            "10% of the disk, clamped to 1 GiB..20 GiB"
        );
        // A real filesystem has a size.
        #[cfg(unix)]
        assert!(filesystem_bytes(&std::env::temp_dir()).is_some_and(|total| total > 0));
        Ok(())
    }

    #[test]
    fn min_free_disk_evicts_then_refuses() -> R<()> {
        let dir = tempfile::tempdir()?;
        let mut cfg = config(1000, 1000);
        cfg.min_free_disk = 100;
        let free = Arc::new(Mutex::new(1000u64));
        let probe_free = free.clone();
        let store = ContentStore::open(dir.path().join("content"), cfg)?
            .with_free_space_probe(Box::new(move |_| Some(*probe_free.lock().unwrap())));
        let old = store.put_bytes(ContentClass::Artifacts, b"old", PutOptions::default())?;
        let pinned = store.put_bytes(
            ContentClass::Snapshots,
            b"pinned",
            PutOptions::default().pin(PinOwner::snapshot("p")),
        )?;
        *free.lock().unwrap() = 50;
        // Evicting can't raise the fake probe, so the put evicts every
        // unpinned entry and still refuses.
        let err = store
            .put_bytes(ContentClass::Artifacts, b"new", PutOptions::default())
            .unwrap_err();
        assert_eq!(
            format!("{err:#}"),
            "node disk is below content.minFreeDisk (50 bytes free, floor 100 bytes) and no \
             unpinned content is left to evict"
        );
        assert!(!store.contains(&old.digest)?);
        assert!(store.contains(&pinned.digest)?);
        Ok(())
    }
}
