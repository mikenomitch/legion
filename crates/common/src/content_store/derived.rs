//! The `derived` class of the node content store: everything Legion builds
//! on a node from content the node already verified (docs/artifacts.md,
//! "Derived content"; plan: internal_docs/plans/derived-content-plan.md).
//!
//! A derived entry is expensive to build, cheap to keep, the same for every
//! unit of its inputs on the node, and safe to throw away: a sandbox's base
//! root disk today, and compiled Wasm, pre-initialized images, and prepared
//! VM base snapshots when their runtimes exist.
//!
//! It differs from the other two classes in one way: an entry is named by
//! its **recipe key** (a hash of what it was built from and what built it),
//! not by the digest of its bytes. Everything else is the store's: one byte
//! budget for the class, LRU eviction, pins, the TTL, `minFreeDisk`, and GC.
//!
//! # Layout
//!
//! ```text
//! <content store>/derived/
//!   <kind>/<key>/    one committed entry: a directory the builder filled
//!   tmp/             builds in progress, cleared when the store opens
//! ```
//!
//! # The trust rule
//!
//! Derived content is built here, from inputs this node verified, and is
//! never fetched, published, or moved. There is no way to put an entry
//! except [`ContentStore::ensure_derived`] (or the staged form, which a
//! builder with its own fill strategy uses), and derived entries are left
//! out of the set a node advertises, so no other node is ever steered to
//! fetch one.
//!
//! # Nothing per unit
//!
//! A [`Recipe`] cannot carry a secret: its inputs are content digests,
//! numbers, and SHA-256 fingerprints the recipe computes itself, under names
//! that are compile-time strings. Per-unit material (env, identity,
//! hostnames) goes into each unit's own copy after it is cloned from the
//! entry, never into the entry.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar};
use std::time::Instant;

use rusqlite::{params, Connection, OptionalExtension};

use super::*;

/// Where derived entries live under the store root.
pub(super) const DERIVED_DIR: &str = "derived";
const DERIVED_TMP: &str = "tmp";

/// What a derived entry is. The kind is part of the key and of the path,
/// and labels the class's metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DerivedKind {
    /// A sandbox's base root disk, built from an image's layers.
    Rootfs,
    /// A compiled WebAssembly module.
    Cwasm,
    /// A pre-initialized language image.
    PreinitImage,
    /// A pre-initialized task entry (an image plus the author's script).
    PreinitEntry,
    /// A base memory snapshot of a booted VM, for prepared starts.
    VmBase,
}

impl DerivedKind {
    pub const ALL: [DerivedKind; 5] = [
        DerivedKind::Rootfs,
        DerivedKind::Cwasm,
        DerivedKind::PreinitImage,
        DerivedKind::PreinitEntry,
        DerivedKind::VmBase,
    ];

    /// The name in paths, the index, metrics labels, and API replies.
    pub fn as_str(self) -> &'static str {
        match self {
            DerivedKind::Rootfs => "rootfs",
            DerivedKind::Cwasm => "cwasm",
            DerivedKind::PreinitImage => "preinit-image",
            DerivedKind::PreinitEntry => "preinit-entry",
            DerivedKind::VmBase => "vm-base",
        }
    }
}

impl fmt::Display for DerivedKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DerivedKind {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> R<Self> {
        DerivedKind::ALL
            .into_iter()
            .find(|kind| kind.as_str() == raw)
            .with_context(|| format!("unknown derived content kind {raw:?}"))
    }
}

/// What a derived entry is built from, and by what: its identity.
///
/// The key is a hash of the kind, every input, and the builder version, so
/// two recipes share an entry only when all three match. An input is a
/// content digest, a number, or a fingerprint (the SHA-256 of bytes the
/// caller hands over, computed here), so the recipe that reaches the index
/// and the API holds no raw caller text. Names are `&'static str`: a name
/// is code, never data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recipe {
    kind: DerivedKind,
    inputs: BTreeMap<String, String>,
    builder_version: String,
    confined: bool,
}

impl Recipe {
    /// A recipe for `kind`, built by `builder_version`: Legion's own version
    /// and format for an in-tree builder, the engine's version for a
    /// compiler.
    pub fn new(kind: DerivedKind, builder_version: impl Into<String>) -> Self {
        Recipe {
            kind,
            inputs: BTreeMap::new(),
            builder_version: builder_version.into(),
            confined: false,
        }
    }

    /// An input that is content the node verified.
    pub fn digest(self, name: &'static str, digest: &ContentDigest) -> Self {
        self.put(name.to_string(), digest.as_str().to_string())
    }

    /// An input that is not a content digest (a file's identity, a CPU
    /// feature set): the recipe keeps only its SHA-256.
    pub fn fingerprint(self, name: &'static str, bytes: &[u8]) -> Self {
        self.put(name.to_string(), fingerprint(bytes))
    }

    /// The `index`th of an ordered list of inputs (an image's layers, bottom
    /// first). A value shaped like a content digest is kept as one, anything
    /// else as its fingerprint.
    pub fn listed(self, name: &'static str, index: usize, value: &str) -> Self {
        let value = match ContentDigest::parse(value) {
            Ok(digest) => digest.as_str().to_string(),
            Err(_) => fingerprint(value.as_bytes()),
        };
        self.put(format!("{name}.{index:04}"), value)
    }

    /// A numeric input (a size, a count).
    pub fn number(self, name: &'static str, value: u64) -> Self {
        self.put(name.to_string(), value.to_string())
    }

    /// Marks the build as one that executes or parses untrusted input
    /// (compiling a component, running an author's top-level code), which
    /// must run in a confined builder process. [`ContentStore::ensure_derived`]
    /// refuses such a recipe: it builds in this process.
    pub fn confined(mut self) -> Self {
        self.confined = true;
        self
    }

    fn put(mut self, name: String, value: String) -> Self {
        let replaced = self.inputs.insert(name, value);
        debug_assert!(replaced.is_none(), "a recipe input was named twice");
        self
    }

    pub fn kind(&self) -> DerivedKind {
        self.kind
    }

    pub fn builder_version(&self) -> &str {
        &self.builder_version
    }

    pub fn is_confined(&self) -> bool {
        self.confined
    }

    /// The inputs as recorded: name to digest, number, or fingerprint.
    pub fn inputs(&self) -> &BTreeMap<String, String> {
        &self.inputs
    }

    /// The entry's key: 64 lowercase hex characters.
    pub fn key(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"legion-derived\0");
        hasher.update(self.kind.as_str().as_bytes());
        hasher.update(b"\0builder\0");
        hasher.update(self.builder_version.as_bytes());
        for (name, value) in &self.inputs {
            hasher.update(b"\0input\0");
            hasher.update(name.as_bytes());
            hasher.update(b"\0");
            hasher.update(value.as_bytes());
        }
        hasher.hex()
    }
}

fn fingerprint(bytes: &[u8]) -> String {
    format!("sha256:{}", Sha256::hex_of(bytes))
}

/// One committed derived entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedEntry {
    pub kind: DerivedKind,
    pub key: String,
    /// The entry's directory. Read-only by convention: clone out of it.
    pub path: PathBuf,
    /// Bytes of disk the entry actually uses (allocated blocks, so a sparse
    /// image counts what it costs).
    pub size: u64,
    /// The recipe inputs it was built from.
    pub inputs: BTreeMap<String, String>,
    pub builder_version: String,
    /// Whether it was built by a confined builder.
    pub confined: bool,
    /// Unix seconds.
    pub created_at: i64,
    /// Unix seconds; refreshed on every use.
    pub last_used_at: i64,
    pub pinned: bool,
}

/// Why a built entry was not kept. The build's output is discarded and the
/// caller carries on without the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedSkip {
    /// The class budget is `0`.
    Disabled,
    /// The entry is larger than the whole class budget.
    Oversized,
    /// The disk is below `minFreeDisk`: nothing rebuildable is kept then.
    BelowMinFree,
    /// Pinned entries leave no room for it.
    NoRoom,
    /// Another build committed the same key first.
    AlreadyPresent,
    /// The index or the filesystem refused it.
    Failed,
}

impl DerivedSkip {
    /// The reason in an operator's words.
    pub fn reason(self) -> &'static str {
        match self {
            DerivedSkip::Disabled => "derived content is disabled on this node",
            DerivedSkip::Oversized => "it is larger than the derived content budget",
            DerivedSkip::BelowMinFree => "the disk is below minFreeDisk",
            DerivedSkip::NoRoom => "pinned derived content leaves no room for it",
            DerivedSkip::AlreadyPresent => "it was already cached",
            DerivedSkip::Failed => "the content store could not record it",
        }
    }

    fn outcome(self) -> &'static str {
        match self {
            DerivedSkip::Disabled => "disabled",
            DerivedSkip::Oversized => "oversized",
            DerivedSkip::BelowMinFree => "below_min_free",
            DerivedSkip::NoRoom => "no_room",
            DerivedSkip::AlreadyPresent => "duplicate",
            DerivedSkip::Failed => "error",
        }
    }
}

/// What [`ContentStore::ensure_derived`] found or did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DerivedEnsured {
    /// The entry was already here, or a concurrent build just committed it.
    Present(DerivedEntry),
    /// This call built and committed it.
    Built(DerivedEntry),
    /// The build ran and its output was not kept.
    Skipped(DerivedSkip),
}

impl DerivedEnsured {
    /// The entry, when the node holds one.
    pub fn entry(&self) -> Option<&DerivedEntry> {
        match self {
            DerivedEnsured::Present(entry) | DerivedEnsured::Built(entry) => Some(entry),
            DerivedEnsured::Skipped(_) => None,
        }
    }
}

/// One build in flight. Waiters block until it is done, whatever happened.
#[derive(Default)]
struct Flight {
    done: std::sync::Mutex<bool>,
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

/// Builds in flight, by `<kind>/<key>`.
#[derive(Default)]
pub(super) struct Flights(std::sync::Mutex<BTreeMap<String, Arc<Flight>>>);

/// The right to build one key. Dropping it (on success, failure, or a skip)
/// wakes everyone waiting for that key. It owns what it needs, so a builder
/// can finish on another thread.
pub struct DerivedFlight {
    flights: Arc<Flights>,
    id: String,
    flight: Arc<Flight>,
}

impl fmt::Debug for DerivedFlight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DerivedFlight")
            .field("id", &self.id)
            .finish()
    }
}

impl Drop for DerivedFlight {
    fn drop(&mut self) {
        self.flights
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.id);
        *self.flight.done.lock().unwrap_or_else(|p| p.into_inner()) = true;
        self.flight.finished.notify_all();
    }
}

/// The pin table's key for a derived entry. Pins share one table with
/// blobs, so releasing an owner releases both kinds.
fn pin_id(kind: DerivedKind, key: &str) -> String {
    format!("derived:{kind}:{key}")
}

fn flight_id(kind: DerivedKind, key: &str) -> String {
    format!("{kind}/{key}")
}

/// A key is the recipe's hex digest. Checked wherever one arrives as text,
/// so a key can never name a path outside its kind's directory.
fn valid_key(key: &str) -> bool {
    key.len() == 64 && key.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Bytes of disk a file or tree actually uses.
pub fn allocated_bytes(path: &Path) -> u64 {
    let Ok(meta) = fs::symlink_metadata(path) else {
        return 0;
    };
    if meta.is_dir() {
        return fs::read_dir(path)
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| allocated_bytes(&entry.path()))
            .sum();
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        meta.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    meta.len()
}

pub(super) const DERIVED_SCHEMA: &str = "
    -- Derived content (crates/common/src/content_store/derived.rs): entries
    -- named by what they were built from, not by their bytes.
    CREATE TABLE IF NOT EXISTS derived(
      kind TEXT NOT NULL,
      key TEXT NOT NULL,
      inputs TEXT NOT NULL,
      builder TEXT NOT NULL,
      confined INTEGER NOT NULL,
      size INTEGER NOT NULL,
      created_at INTEGER NOT NULL,
      last_used_at INTEGER NOT NULL,
      use_seq INTEGER NOT NULL,
      PRIMARY KEY(kind, key));
    CREATE INDEX IF NOT EXISTS derived_lru ON derived(use_seq);";

type Row = (String, String, String, String, bool, i64, i64, i64, bool);

const SELECT_ROW: &str = "SELECT kind, key, inputs, builder, confined, size, created_at,
    last_used_at,
    EXISTS (SELECT 1 FROM pin WHERE pin.digest = 'derived:' || kind || ':' || key)
    FROM derived";

fn read_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Row> {
    Ok((
        r.get(0)?,
        r.get(1)?,
        r.get(2)?,
        r.get(3)?,
        r.get(4)?,
        r.get(5)?,
        r.get(6)?,
        r.get(7)?,
        r.get(8)?,
    ))
}

pub(super) fn derived_used(db: &Connection) -> R<u64> {
    let used: i64 = db.query_row("SELECT COALESCE(SUM(size), 0) FROM derived", [], |r| {
        r.get(0)
    })?;
    Ok(used as u64)
}

pub(super) fn derived_max_seq(db: &Connection) -> R<i64> {
    Ok(
        db.query_row("SELECT COALESCE(MAX(use_seq), 0) FROM derived", [], |r| {
            r.get(0)
        })?,
    )
}

/// Unpinned derived entries, least recently used first.
fn lru_unpinned_derived(db: &Connection) -> R<Vec<(DerivedKind, String, u64, i64)>> {
    let mut stmt = db.prepare(
        "SELECT kind, key, size, last_used_at FROM derived
         WHERE NOT EXISTS
           (SELECT 1 FROM pin WHERE pin.digest = 'derived:' || kind || ':' || key)
         ORDER BY use_seq ASC",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows
        .into_iter()
        .filter_map(|(kind, key, size, used)| Some((kind.parse().ok()?, key, size as u64, used)))
        .collect())
}

impl ContentStore {
    /// The class budget in bytes. `0` turns derived content off: nothing is
    /// kept, and every consumer builds what it needs each time.
    pub fn derived_budget(&self) -> u64 {
        self.config.budgets.derived
    }

    fn derived_root(&self) -> PathBuf {
        self.root.join(DERIVED_DIR)
    }

    /// Where the entry for `kind` and `key` lives, whether or not it exists.
    pub fn derived_path(&self, kind: DerivedKind, key: &str) -> PathBuf {
        self.derived_root().join(kind.as_str()).join(key)
    }

    /// A fresh directory under `derived/tmp/`, on the store's filesystem,
    /// for a build to fill. [`ContentStore::derived_commit`] moves it into
    /// place; a build that fails removes it, and whatever a crash leaves is
    /// cleared when the store next opens.
    pub fn derived_stage(&self) -> R<PathBuf> {
        let dir = self
            .derived_root()
            .join(DERIVED_TMP)
            .join(crate::util::random_string(12));
        fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        Ok(dir)
    }

    /// Looks up an entry, marking it used (LRU and TTL) when `touch`. An
    /// entry whose directory is gone is dropped and reported absent.
    pub fn derived_lookup(
        &self,
        kind: DerivedKind,
        key: &str,
        touch: bool,
    ) -> R<Option<DerivedEntry>> {
        if !valid_key(key) {
            return Ok(None);
        }
        let mut inner = self.inner.lock().expect("content store");
        let Some(entry) = self.load_derived(&inner.db, kind, key)? else {
            return Ok(None);
        };
        if !entry.path.is_dir() {
            warn!(%kind, key, "Derived content entry missing on disk; dropping");
            self.remove_derived_row_and_dir(&inner.db, kind, key)?;
            return Ok(None);
        }
        if touch {
            inner.seq += 1;
            inner.db.execute(
                "UPDATE derived SET last_used_at = ?1, use_seq = ?2 WHERE kind = ?3 AND key = ?4",
                params![now_secs(), inner.seq, kind.as_str(), key],
            )?;
        }
        Ok(Some(entry))
    }

    /// The entry a recipe names, marked used. Counts a hit.
    pub fn derived_get(&self, recipe: &Recipe) -> R<Option<DerivedEntry>> {
        let entry = self.derived_lookup(recipe.kind(), &recipe.key(), true)?;
        if entry.is_some() {
            crate::telemetry::worker_metrics()
                .derived_hits_total
                .inc(&[recipe.kind().as_str()]);
        }
        Ok(entry)
    }

    /// Takes the right to build `kind`/`key`, or, when a build is already in
    /// flight, waits for it to finish and returns `None`: look the entry up
    /// again then.
    pub fn derived_claim(&self, kind: DerivedKind, key: &str) -> Option<DerivedFlight> {
        let id = flight_id(kind, key);
        let mut flights = self
            .derived_flights
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(flight) = flights.get(&id).cloned() {
            drop(flights);
            flight.wait();
            return None;
        }
        let flight = Arc::new(Flight::default());
        flights.insert(id.clone(), flight.clone());
        Some(DerivedFlight {
            flights: self.derived_flights.clone(),
            id,
            flight,
        })
    }

    /// How many builds are in flight right now.
    pub fn derived_builds_in_flight(&self) -> usize {
        self.derived_flights
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len()
    }

    /// Moves a staged build (a directory from [`ContentStore::derived_stage`])
    /// into place under the recipe's key, evicting least recently used
    /// unpinned entries of the class to make room. `staged` is gone either
    /// way: renamed into place, or removed with the reason it was not kept.
    ///
    /// The rename is the commit, so a crash leaves either a whole entry or
    /// only a temp directory that the next open clears.
    pub fn derived_commit(
        &self,
        recipe: &Recipe,
        staged: &Path,
    ) -> Result<DerivedEntry, DerivedSkip> {
        let kind = recipe.kind();
        let result = self.derived_commit_inner(recipe, staged);
        let metrics = crate::telemetry::worker_metrics();
        match &result {
            Ok(_) => metrics.derived_builds_total.inc(&[kind.as_str(), "built"]),
            Err(skip) => {
                let _ = fs::remove_dir_all(staged);
                metrics
                    .derived_builds_total
                    .inc(&[kind.as_str(), skip.outcome()]);
            }
        }
        result
    }

    fn derived_commit_inner(
        &self,
        recipe: &Recipe,
        staged: &Path,
    ) -> Result<DerivedEntry, DerivedSkip> {
        let (kind, key) = (recipe.kind(), recipe.key());
        let budget = self.derived_budget();
        if budget == 0 {
            return Err(DerivedSkip::Disabled);
        }
        let size = allocated_bytes(staged);
        if size > budget {
            return Err(DerivedSkip::Oversized);
        }
        // Rebuildable content is never kept at the expense of the floor.
        let floor = self.config.min_free_disk;
        if floor > 0 && (self.free_space)(&self.root).is_some_and(|free| free < floor) {
            return Err(DerivedSkip::BelowMinFree);
        }
        let failed = |err: anyhow::Error| {
            warn!(%kind, key, error = %format!("{err:#}"), "Could not commit derived content");
            DerivedSkip::Failed
        };
        let mut inner = self.inner.lock().expect("content store");
        if self
            .load_derived(&inner.db, kind, &key)
            .map_err(failed)?
            .is_some_and(|entry| entry.path.is_dir())
        {
            return Err(DerivedSkip::AlreadyPresent);
        }
        // Make room: least recently used first, across every kind.
        let mut used = derived_used(&inner.db).map_err(failed)?;
        if used + size > budget {
            for (old_kind, old_key, old_size, _) in
                lru_unpinned_derived(&inner.db).map_err(failed)?
            {
                self.remove_derived_row_and_dir(&inner.db, old_kind, &old_key)
                    .map_err(failed)?;
                info!(kind = %old_kind, key = %old_key, size = old_size, "Evicted derived content (budget)");
                used = used.saturating_sub(old_size);
                if used + size <= budget {
                    break;
                }
            }
            if used + size > budget {
                return Err(DerivedSkip::NoRoom);
            }
        }
        let dest = self.derived_path(kind, &key);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| failed(anyhow!(err).context("create the kind directory")))?;
        }
        // A directory with no row is a leftover the index never knew.
        let _ = fs::remove_dir_all(&dest);
        fs::rename(staged, &dest)
            .map_err(|err| failed(anyhow!(err).context("move the build into place")))?;
        let now = now_secs();
        inner.seq += 1;
        let inputs = serde_json::to_string(recipe.inputs()).unwrap_or_else(|_| "{}".into());
        if let Err(err) = inner.db.execute(
            "INSERT OR REPLACE INTO derived(kind, key, inputs, builder, confined, size,
               created_at, last_used_at, use_seq)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?8)",
            params![
                kind.as_str(),
                key,
                inputs,
                recipe.builder_version(),
                recipe.is_confined(),
                size as i64,
                now,
                inner.seq
            ],
        ) {
            let _ = fs::remove_dir_all(&dest);
            return Err(failed(anyhow!(err).context("record the entry")));
        }
        self.derived_changed(&inner.db);
        debug!(%kind, key, size, "Stored derived content");
        self.load_derived(&inner.db, kind, &key)
            .map_err(failed)?
            .ok_or(DerivedSkip::Failed)
    }

    /// Makes sure this node holds what `recipe` describes: returns the
    /// committed entry, waits for a build in flight, or builds it.
    ///
    /// `build` fills the directory it is given. It runs in this process, so
    /// a recipe marked [`Recipe::confined`] is refused: its build must run
    /// in a confined builder process, which hands its output to
    /// [`ContentStore::derived_commit`] itself. Concurrent calls for one key
    /// build once. Blocking: call it from a blocking thread.
    pub fn ensure_derived(
        &self,
        recipe: &Recipe,
        build: impl FnOnce(&Path) -> R<()>,
    ) -> R<DerivedEnsured> {
        if recipe.is_confined() {
            bail!(
                "derived content of kind {} is built from untrusted input and needs a confined \
                 builder; it cannot be built in the worker process",
                recipe.kind()
            );
        }
        if self.derived_budget() == 0 {
            return Ok(DerivedEnsured::Skipped(DerivedSkip::Disabled));
        }
        let (kind, key) = (recipe.kind(), recipe.key());
        if let Some(entry) = self.derived_get(recipe)? {
            return Ok(DerivedEnsured::Present(entry));
        }
        let Some(flight) = self.derived_claim(kind, &key) else {
            // A build was in flight and has finished: look again, once.
            return Ok(match self.derived_get(recipe)? {
                Some(entry) => DerivedEnsured::Present(entry),
                None => DerivedEnsured::Skipped(DerivedSkip::Failed),
            });
        };
        // The entry may have landed between the lookup and the claim.
        if let Some(entry) = self.derived_get(recipe)? {
            return Ok(DerivedEnsured::Present(entry));
        }
        let staged = self.derived_stage()?;
        let started = Instant::now();
        if let Err(err) = build(&staged) {
            let _ = fs::remove_dir_all(&staged);
            crate::telemetry::worker_metrics()
                .derived_builds_total
                .inc(&[kind.as_str(), "error"]);
            return Err(err.context(format!("build derived content {kind}/{key}")));
        }
        crate::telemetry::worker_metrics()
            .derived_build_seconds
            .observe(&[kind.as_str()], started.elapsed().as_secs_f64());
        let result = match self.derived_commit(recipe, &staged) {
            Ok(entry) => DerivedEnsured::Built(entry),
            Err(skip) => DerivedEnsured::Skipped(skip),
        };
        drop(flight);
        Ok(result)
    }

    /// Records how long a build took, for a builder that stages and commits
    /// on its own instead of through [`ContentStore::ensure_derived`].
    pub fn derived_observe_build(&self, kind: DerivedKind, seconds: f64) {
        crate::telemetry::worker_metrics()
            .derived_build_seconds
            .observe(&[kind.as_str()], seconds);
    }

    /// Every derived entry, most recently used first.
    pub fn derived_list(&self) -> R<Vec<DerivedEntry>> {
        let inner = self.inner.lock().expect("content store");
        let mut stmt = inner
            .db
            .prepare(&format!("{SELECT_ROW} ORDER BY use_seq DESC"))?;
        let rows = stmt
            .query_map([], read_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows
            .into_iter()
            .filter_map(|row| self.entry_of(row))
            .collect())
    }

    /// Pins a present entry for `owner` (a prepared-VM pool holding VMs
    /// restored from its base snapshot). A pinned entry is never evicted or
    /// expired. Idempotent; fails when the entry is not here.
    pub fn derived_pin(&self, kind: DerivedKind, key: &str, owner: &PinOwner) -> R<()> {
        let inner = self.inner.lock().expect("content store");
        if self.load_derived(&inner.db, kind, key)?.is_none() {
            bail!("cannot pin derived content {kind}/{key}: not in the store");
        }
        inner.db.execute(
            "INSERT OR IGNORE INTO pin(digest, owner, created_at) VALUES (?1, ?2, ?3)",
            params![pin_id(kind, key), owner.as_str(), now_secs()],
        )?;
        self.derived_changed(&inner.db);
        Ok(())
    }

    /// Removes `owner`'s pin. Returns whether it held one.
    pub fn derived_unpin(&self, kind: DerivedKind, key: &str, owner: &PinOwner) -> R<bool> {
        let inner = self.inner.lock().expect("content store");
        let removed = inner.db.execute(
            "DELETE FROM pin WHERE digest = ?1 AND owner = ?2",
            params![pin_id(kind, key), owner.as_str()],
        )? > 0;
        self.derived_changed(&inner.db);
        Ok(removed)
    }

    /// Removes an entry now, pinned or not. Returns whether it existed.
    pub fn derived_remove(&self, kind: DerivedKind, key: &str) -> R<bool> {
        if !valid_key(key) {
            return Ok(false);
        }
        let inner = self.inner.lock().expect("content store");
        let existed = self.load_derived(&inner.db, kind, key)?.is_some();
        self.remove_derived_row_and_dir(&inner.db, kind, key)?;
        inner
            .db
            .execute("DELETE FROM pin WHERE digest = ?1", [pin_id(kind, key)])?;
        Ok(existed)
    }

    /// A counter that moves whenever the set of derived entries, their
    /// sizes, or their pins change (uses do not move it). What a reporter
    /// compares to know whether it has anything new to say.
    pub fn derived_generation(&self) -> u64 {
        self.derived_generation
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    // ---- internals, shared with the store's own eviction and GC ----------

    /// Notes a change to the derived set and refreshes the class gauge.
    fn derived_changed(&self, db: &Connection) {
        self.derived_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Ok(used) = derived_used(db) {
            crate::telemetry::worker_metrics()
                .derived_bytes
                .set(used.min(i64::MAX as u64) as i64);
        }
    }

    fn entry_of(&self, row: Row) -> Option<DerivedEntry> {
        let (kind, key, inputs, builder_version, confined, size, created_at, last_used_at, pinned) =
            row;
        let kind: DerivedKind = kind.parse().ok()?;
        Some(DerivedEntry {
            path: self.derived_path(kind, &key),
            kind,
            key,
            size: size as u64,
            inputs: serde_json::from_str(&inputs).unwrap_or_default(),
            builder_version,
            confined,
            created_at,
            last_used_at,
            pinned,
        })
    }

    fn load_derived(
        &self,
        db: &Connection,
        kind: DerivedKind,
        key: &str,
    ) -> R<Option<DerivedEntry>> {
        let row = db
            .query_row(
                &format!("{SELECT_ROW} WHERE kind = ?1 AND key = ?2"),
                params![kind.as_str(), key],
                read_row,
            )
            .optional()?;
        Ok(row.and_then(|row| self.entry_of(row)))
    }

    fn remove_derived_row_and_dir(&self, db: &Connection, kind: DerivedKind, key: &str) -> R<()> {
        let dir = self.derived_path(kind, key);
        if dir.exists() {
            fs::remove_dir_all(&dir)
                .with_context(|| format!("remove derived content {}", dir.display()))?;
        }
        db.execute(
            "DELETE FROM derived WHERE kind = ?1 AND key = ?2",
            params![kind.as_str(), key],
        )?;
        self.derived_changed(db);
        Ok(())
    }

    /// Evicts unpinned derived entries, least recently used first, while
    /// `keep_going` says so. Returns the bytes freed. The disk floor calls
    /// this before it touches an artifact or a checkpoint: what can be
    /// rebuilt goes first.
    pub(super) fn evict_derived_while(
        &self,
        db: &Connection,
        why: &str,
        mut keep_going: impl FnMut() -> bool,
    ) -> R<u64> {
        let mut freed = 0;
        for (kind, key, size, _) in lru_unpinned_derived(db)? {
            if !keep_going() {
                break;
            }
            self.remove_derived_row_and_dir(db, kind, &key)?;
            info!(%kind, key, size, "Evicted derived content ({why})");
            freed += size;
        }
        Ok(freed)
    }

    /// The derived half of a GC pass: TTL expiry, then the budget.
    pub(super) fn gc_derived(&self, db: &Connection, now: i64, report: &mut GcReport) -> R<()> {
        if let Some(ttl) = self.config.ttl {
            let cutoff = now - ttl.as_secs() as i64;
            for (kind, key, size, last_used) in lru_unpinned_derived(db)? {
                if last_used <= cutoff {
                    self.remove_derived_row_and_dir(db, kind, &key)?;
                    report.bytes_freed += size;
                    report.derived_removed.push(format!("{kind}/{key}"));
                }
            }
        }
        let budget = self.derived_budget();
        let mut used = derived_used(db)?;
        if used > budget {
            for (kind, key, size, _) in lru_unpinned_derived(db)? {
                self.remove_derived_row_and_dir(db, kind, &key)?;
                report.bytes_freed += size;
                report.derived_removed.push(format!("{kind}/{key}"));
                used = used.saturating_sub(size);
                if used <= budget {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Opens the derived tree: clears builds a previous process left half
    /// done, drops rows whose directory is gone, removes directories the
    /// index does not know, and trims to a budget that may have shrunk.
    pub(super) fn reconcile_derived(&self) -> R<()> {
        let root = self.derived_root();
        let tmp = root.join(DERIVED_TMP);
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        let inner = self.inner.lock().expect("content store");
        let mut stmt = inner.db.prepare("SELECT kind, key FROM derived")?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(stmt);
        let mut known = std::collections::HashSet::new();
        for (kind, key) in rows {
            let dir = root.join(&kind).join(&key);
            if kind.parse::<DerivedKind>().is_ok() && valid_key(&key) && dir.is_dir() {
                known.insert((kind, key));
            } else {
                debug!(
                    kind,
                    key, "Dropping a derived content row with no entry on disk"
                );
                inner.db.execute(
                    "DELETE FROM derived WHERE kind = ?1 AND key = ?2",
                    params![kind, key],
                )?;
            }
        }
        for kind_dir in fs::read_dir(&root)?.flatten() {
            let kind = kind_dir.file_name().to_string_lossy().into_owned();
            if kind == DERIVED_TMP {
                continue;
            }
            if kind.parse::<DerivedKind>().is_err() || !kind_dir.path().is_dir() {
                let _ = fs::remove_dir_all(kind_dir.path());
                let _ = fs::remove_file(kind_dir.path());
                continue;
            }
            for entry in fs::read_dir(kind_dir.path())?.flatten() {
                let key = entry.file_name().to_string_lossy().into_owned();
                if !known.contains(&(kind.clone(), key.clone())) {
                    debug!(kind, key, "Removing unindexed derived content");
                    let _ = fs::remove_dir_all(entry.path());
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
        let mut report = GcReport::default();
        // Only the budget here: an entry's age is judged by the GC loop.
        let budget = self.derived_budget();
        let mut used = derived_used(&inner.db)?;
        if used > budget {
            for (kind, key, size, _) in lru_unpinned_derived(&inner.db)? {
                self.remove_derived_row_and_dir(&inner.db, kind, &key)?;
                report.bytes_freed += size;
                used = used.saturating_sub(size);
                if used <= budget {
                    break;
                }
            }
        }
        self.derived_changed(&inner.db);
        Ok(())
    }

    /// The class's usage row, for [`ContentStore::usage`].
    pub(super) fn derived_usage(&self, db: &Connection) -> R<ClassUsage> {
        let (entries, used): (i64, i64) = db.query_row(
            "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM derived",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let pinned: i64 = db.query_row(
            "SELECT COALESCE(SUM(size), 0) FROM derived WHERE EXISTS
               (SELECT 1 FROM pin WHERE pin.digest = 'derived:' || kind || ':' || key)",
            [],
            |r| r.get(0),
        )?;
        Ok(ClassUsage {
            class: ContentClass::Derived,
            budget: self.derived_budget(),
            used_bytes: used as u64,
            pinned_bytes: pinned as u64,
            entries: entries as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;

    const MIB: u64 = 1 << 20;

    fn store(dir: &Path, derived: u64) -> ContentStore {
        ContentStore::open(
            dir.join("content"),
            ContentStoreConfig {
                budgets: ContentBudgets {
                    artifacts: 64 * MIB,
                    snapshots: 64 * MIB,
                    derived,
                },
                ttl: Some(Duration::from_secs(3600)),
                min_free_disk: 0,
            },
        )
        .expect("open store")
    }

    fn layer(n: u8) -> ContentDigest {
        ContentDigest::of_bytes(&[n])
    }

    fn rootfs(n: u8) -> Recipe {
        Recipe::new(DerivedKind::Rootfs, "test-1")
            .digest("layer", &layer(n))
            .number("headroomMib", 256)
    }

    /// Fills the build directory with `bytes` of real data.
    fn fill(bytes: usize) -> impl FnOnce(&Path) -> R<()> {
        move |dir| {
            fs::write(dir.join("out"), vec![0xAB; bytes])?;
            Ok(())
        }
    }

    #[test]
    fn the_key_is_stable_and_every_part_changes_it_test() {
        let base = rootfs(1);
        assert_eq!(base.key(), rootfs(1).key(), "the same recipe, the same key");
        assert_eq!(base.key().len(), 64);
        assert_ne!(base.key(), rootfs(2).key(), "another input value");
        assert_ne!(
            base.key(),
            rootfs(1).number("extra", 1).key(),
            "another input"
        );
        assert_ne!(
            base.key(),
            Recipe::new(DerivedKind::Rootfs, "test-2")
                .digest("layer", &layer(1))
                .number("headroomMib", 256)
                .key(),
            "another builder version"
        );
        assert_ne!(
            base.key(),
            Recipe::new(DerivedKind::VmBase, "test-1")
                .digest("layer", &layer(1))
                .number("headroomMib", 256)
                .key(),
            "another kind"
        );
        // Order of construction does not matter; order in a list does.
        let ab = Recipe::new(DerivedKind::Cwasm, "e")
            .number("a", 1)
            .number("b", 2);
        let ba = Recipe::new(DerivedKind::Cwasm, "e")
            .number("b", 2)
            .number("a", 1);
        assert_eq!(ab.key(), ba.key());
        let one = Recipe::new(DerivedKind::Rootfs, "e")
            .listed("layer", 0, "x")
            .listed("layer", 1, "y");
        let other = Recipe::new(DerivedKind::Rootfs, "e")
            .listed("layer", 0, "y")
            .listed("layer", 1, "x");
        assert_ne!(one.key(), other.key());
    }

    #[test]
    fn a_recipe_keeps_no_caller_text_test() {
        // Whatever a caller hands over, the recipe (and so the index and
        // the API) holds a digest, a number, or a fingerprint of it.
        let secret = "hunter2-api-token";
        let recipe = Recipe::new(DerivedKind::PreinitEntry, "engine-1")
            .fingerprint("script", secret.as_bytes())
            .listed("layer", 0, secret)
            .digest("image", &layer(9))
            .number("size", 4);
        let recorded = serde_json::to_string(recipe.inputs()).unwrap();
        assert!(!recorded.contains(secret), "{recorded}");
        for (name, value) in recipe.inputs() {
            assert!(
                value.parse::<u64>().is_ok() || ContentDigest::parse(value).is_ok(),
                "{name} = {value}"
            );
        }
        // A list entry that is a digest stays readable.
        let listed = Recipe::new(DerivedKind::Rootfs, "e").listed("layer", 0, layer(3).as_str());
        assert_eq!(listed.inputs()["layer.0000"], layer(3).as_str());
    }

    #[test]
    fn ensure_builds_once_then_hits_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = store(dir.path(), 16 * MIB);
        let recipe = rootfs(1);
        let built = store.ensure_derived(&recipe, fill(4096))?;
        let DerivedEnsured::Built(entry) = built else {
            panic!("{built:?}");
        };
        assert_eq!(
            entry.path,
            store.derived_path(DerivedKind::Rootfs, &recipe.key())
        );
        assert!(entry.path.join("out").is_file());
        assert_eq!(entry.inputs, *recipe.inputs());
        assert_eq!(entry.builder_version, "test-1");
        assert!(entry.size >= 4096);
        let again = store.ensure_derived(&recipe, |_| panic!("built twice"))?;
        assert!(matches!(again, DerivedEnsured::Present(_)), "{again:?}");
        assert!(
            fs::read_dir(dir.path().join("content/derived/tmp"))?
                .next()
                .is_none(),
            "nothing is left staged"
        );
        // It is listed, counted against its own class, and not a blob.
        assert_eq!(store.derived_list()?.len(), 1);
        let usage = store.usage()?;
        let derived = usage
            .iter()
            .find(|class| class.class == ContentClass::Derived)
            .expect("derived class");
        assert_eq!((derived.entries, derived.budget), (1, 16 * MIB));
        assert_eq!(derived.used_bytes, entry.size);
        assert!(store.list()?.is_empty(), "never a content-addressed blob");
        assert!(
            store.sizes_at_least(0)?.is_empty(),
            "never advertised to the manager for fetching"
        );
        Ok(())
    }

    #[test]
    fn concurrent_ensures_of_one_key_build_once_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = Arc::new(store(dir.path(), 16 * MIB));
        let builds = Arc::new(AtomicUsize::new(0));
        let outcomes: Vec<DerivedEnsured> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let store = store.clone();
                    let builds = builds.clone();
                    scope.spawn(move || {
                        store
                            .ensure_derived(&rootfs(1), |out| {
                                builds.fetch_add(1, Ordering::SeqCst);
                                std::thread::sleep(Duration::from_millis(150));
                                fs::write(out.join("out"), b"built")?;
                                Ok(())
                            })
                            .unwrap()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(builds.load(Ordering::SeqCst), 1, "{outcomes:?}");
        assert_eq!(
            outcomes
                .iter()
                .filter(|o| matches!(o, DerivedEnsured::Present(_)))
                .count(),
            7,
            "{outcomes:?}"
        );
        assert_eq!(store.derived_builds_in_flight(), 0);
        Ok(())
    }

    #[test]
    fn a_failed_or_crashed_build_leaves_no_entry_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let recipe = rootfs(1);
        {
            let store = store(dir.path(), 16 * MIB);
            let err = store
                .ensure_derived(&recipe, |out| {
                    fs::write(out.join("half"), b"half")?;
                    bail!("mkfs failed")
                })
                .unwrap_err();
            assert!(format!("{err:#}").contains("mkfs failed"), "{err:#}");
            assert!(store.derived_get(&recipe)?.is_none());
            assert_eq!(store.derived_builds_in_flight(), 0, "the claim is released");
            assert!(fs::read_dir(dir.path().join("content/derived/tmp"))?
                .next()
                .is_none());
            // A crash mid-build: the process dies with a staged directory.
            let staged = store.derived_stage()?;
            fs::write(staged.join("half"), b"half")?;
        }
        // And a directory under a real key that no row ever named.
        let stray = dir.path().join("content/derived/rootfs").join(recipe.key());
        fs::create_dir_all(&stray)?;
        fs::create_dir_all(dir.path().join("content/derived/not-a-kind"))?;
        let store = store(dir.path(), 16 * MIB);
        assert!(fs::read_dir(dir.path().join("content/derived/tmp"))?
            .next()
            .is_none());
        assert!(!stray.exists(), "an unindexed entry is not an entry");
        assert!(!dir.path().join("content/derived/not-a-kind").exists());
        assert!(store.derived_get(&recipe)?.is_none());
        // The next ensure builds it.
        assert!(matches!(
            store.ensure_derived(&recipe, fill(16))?,
            DerivedEnsured::Built(_)
        ));
        Ok(())
    }

    #[test]
    fn the_budget_is_one_lru_across_kinds_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        // Room for two 1 MiB entries, not three.
        let store = store(dir.path(), 5 * MIB / 2);
        let a = rootfs(1);
        let b = Recipe::new(DerivedKind::Cwasm, "engine-1").digest("module", &layer(2));
        let c = Recipe::new(DerivedKind::VmBase, "vmm-1").digest("image", &layer(3));
        store.ensure_derived(&a, fill(MIB as usize))?;
        store.ensure_derived(&b, fill(MIB as usize))?;
        // Use `a`, so `b` (another kind) is the least recently used.
        assert!(store.derived_get(&a)?.is_some());
        assert!(matches!(
            store.ensure_derived(&c, fill(MIB as usize))?,
            DerivedEnsured::Built(_)
        ));
        assert!(store.derived_lookup(a.kind(), &a.key(), false)?.is_some());
        assert!(
            store.derived_lookup(b.kind(), &b.key(), false)?.is_none(),
            "the least recently used entry goes, whatever its kind"
        );
        assert!(!store.derived_path(b.kind(), &b.key()).exists());
        assert!(store.derived_lookup(c.kind(), &c.key(), false)?.is_some());
        // Larger than the whole budget: built, never kept.
        let big = rootfs(9);
        assert_eq!(
            store.ensure_derived(&big, fill(3 * MIB as usize))?,
            DerivedEnsured::Skipped(DerivedSkip::Oversized)
        );
        assert!(store.derived_lookup(a.kind(), &a.key(), false)?.is_some());
        Ok(())
    }

    #[test]
    fn pinned_entries_survive_pressure_ttl_and_the_floor_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = store(dir.path(), 5 * MIB / 2);
        let pool = PinOwner::new("vm-pool:machine")?;
        let (a, b, c) = (rootfs(1), rootfs(2), rootfs(3));
        store.ensure_derived(&a, fill(MIB as usize))?;
        store.derived_pin(a.kind(), &a.key(), &pool)?;
        store.ensure_derived(&b, fill(MIB as usize))?;
        // `a` is the oldest, and pinned: `b` goes instead.
        store.ensure_derived(&c, fill(MIB as usize))?;
        let held = store.derived_lookup(a.kind(), &a.key(), false)?.expect("a");
        assert!(held.pinned);
        assert!(store.derived_lookup(b.kind(), &b.key(), false)?.is_none());
        // The TTL passes: only the unpinned entry expires.
        let report = store.gc_at(now_secs() + 7200)?;
        assert_eq!(report.derived_removed, vec![format!("rootfs/{}", c.key())]);
        assert!(store.derived_lookup(a.kind(), &a.key(), false)?.is_some());
        let usage = store.derived_usage(&store.inner.lock().unwrap().db)?;
        assert_eq!(usage.pinned_bytes, usage.used_bytes);
        // Pins leave no room: a build is not kept, and nothing is evicted.
        store.ensure_derived(&b, fill(MIB as usize))?;
        store.derived_pin(b.kind(), &b.key(), &pool)?;
        assert_eq!(
            store.ensure_derived(&c, fill(MIB as usize))?,
            DerivedEnsured::Skipped(DerivedSkip::NoRoom)
        );
        // Releasing the owner releases its derived pins with its blob pins.
        assert_eq!(store.release_owner(&pool)?, 2);
        assert!(matches!(
            store.ensure_derived(&c, fill(MIB as usize))?,
            DerivedEnsured::Built(_)
        ));
        assert!(store
            .derived_pin(a.kind(), &rootfs(8).key(), &pool)
            .is_err());
        Ok(())
    }

    #[test]
    fn the_disk_floor_takes_derived_content_first_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let free = Arc::new(StdMutex::new(1000u64));
        let probe_free = free.clone();
        let mut config = ContentStoreConfig {
            budgets: ContentBudgets {
                artifacts: 64 * MIB,
                snapshots: 64 * MIB,
                derived: 64 * MIB,
            },
            ttl: None,
            min_free_disk: 100,
        };
        let store = ContentStore::open(dir.path().join("content"), config.clone())?
            .with_free_space_probe(Box::new(move |_| Some(*probe_free.lock().unwrap())));
        let old = store.put_bytes(ContentClass::Artifacts, b"old", PutOptions::default())?;
        let (a, b) = (rootfs(1), rootfs(2));
        store.ensure_derived(&a, fill(4096))?;
        store.ensure_derived(&b, fill(4096))?;
        store.derived_pin(b.kind(), &b.key(), &PinOwner::new("pool:x")?)?;

        // Below the floor, a build is not kept.
        *free.lock().unwrap() = 50;
        assert_eq!(
            store.ensure_derived(&rootfs(3), fill(16))?,
            DerivedEnsured::Skipped(DerivedSkip::BelowMinFree)
        );
        // A put that needs the floor evicts derived content first. Freeing
        // the one unpinned derived entry is enough here, so the artifact
        // that was next in line stays.
        let lifted = free.clone();
        let derived_dir = store.derived_path(a.kind(), &a.key());
        drop(store);
        config.min_free_disk = 100;
        let store = ContentStore::open(dir.path().join("content"), config)?.with_free_space_probe(
            Box::new(move |_| {
                Some(if derived_dir.exists() {
                    *lifted.lock().unwrap()
                } else {
                    1000
                })
            }),
        );
        store.put_bytes(ContentClass::Artifacts, b"new", PutOptions::default())?;
        assert!(
            store.derived_lookup(a.kind(), &a.key(), false)?.is_none(),
            "the rebuildable entry went first"
        );
        assert!(store.contains(&old.digest)?, "the artifact was not touched");
        assert!(
            store.derived_lookup(b.kind(), &b.key(), false)?.is_some(),
            "a pinned entry is never evicted"
        );
        Ok(())
    }

    #[test]
    fn a_zero_budget_turns_the_class_off_and_empties_it_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let recipe = rootfs(1);
        {
            let store = store(dir.path(), 16 * MIB);
            store.ensure_derived(&recipe, fill(4096))?;
        }
        let store = store(dir.path(), 0);
        assert!(store.derived_get(&recipe)?.is_none(), "emptied at open");
        assert_eq!(
            store.ensure_derived(&recipe, |_| panic!("built while disabled"))?,
            DerivedEnsured::Skipped(DerivedSkip::Disabled)
        );
        Ok(())
    }

    #[test]
    fn a_confined_recipe_is_never_built_in_process_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = store(dir.path(), 16 * MIB);
        let recipe = Recipe::new(DerivedKind::Cwasm, "engine-1")
            .digest("module", &layer(1))
            .confined();
        let err = store
            .ensure_derived(&recipe, |_| panic!("ran untrusted input in the worker"))
            .unwrap_err();
        assert!(format!("{err:#}").contains("confined builder"), "{err:#}");
        // A confined builder's output is committed with the staged form,
        // and the entry remembers how it was built.
        let staged = store.derived_stage()?;
        fs::write(staged.join("module.cwasm"), b"compiled")?;
        let entry = store.derived_commit(&recipe, &staged).expect("committed");
        assert!(entry.confined);
        Ok(())
    }

    #[test]
    fn a_blob_put_cannot_use_the_derived_class_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = store(dir.path(), 16 * MIB);
        let err = store
            .put_bytes(
                ContentClass::Derived,
                b"compiled elsewhere",
                PutOptions::default(),
            )
            .unwrap_err();
        assert!(format!("{err:#}").contains("never put"), "{err:#}");
        Ok(())
    }
}
