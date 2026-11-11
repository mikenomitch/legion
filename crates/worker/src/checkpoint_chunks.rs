//! Published checkpoint parts as content-addressed chunks
//! (internal_docs/plans/chunked-checkpoint-publish-plan.md;
//! docs/suspend-and-resume.md, "How published parts are stored").
//!
//! Two checkpoints of one sandbox are mostly the same bytes: between them
//! the guest dirtied some of its memory and a few blocks of its root disk,
//! and much of a young guest's memory and disk is still zero. Published as
//! one sealed object per part, every checkpoint re-uploads all of it, and
//! every sandbox of one image re-uploads the same root disk. So every part
//! is cut into fixed-size chunks instead:
//!
//! ```text
//! <contentStore.url>/sealed/<index name>   the part's index: the one object its
//!                                          manifest `url` names
//! <contentStore.url>/sealed/<chunk name>   one sealed object per distinct,
//!                                          non-zero chunk
//! ```
//!
//! - **Zero chunks are never stored.** The index marks them, and a fetch
//!   leaves a hole in a sparse file.
//! - **Chunks are keyed convergent per job and dedup window**
//!   ([`ChunkScope`]): the same chunk in the same scope is the same object,
//!   so a publish uploads only chunks the store does not hold yet. A chunk
//!   is reused only inside the window it was uploaded in, which keeps a
//!   bucket lifecycle rule of "older than the longest-lived checkpoint plus
//!   one window" safe.
//! - **The index lists every chunk's digest and key**, sealed under a key
//!   only the part key derives ([`PartKey::index_key`]), so a restoring node
//!   needs exactly the part key it already receives.
//!
//! Fixed-size chunks, not content-defined ones: memory and disk images are
//! block-addressed, so nothing ever shifts and a fixed grid lines up with
//! pages and filesystem blocks. Plain hashing, not VMM dirty-page tracking:
//! it works for every runtime and needs no state carried between
//! checkpoints.
//!
//! # Index format (the plaintext inside the sealed index object)
//!
//! ```text
//! "LGNPIDX1" | chunk size (u32 BE) | part length (u64 BE) | chunk count (u64 BE)
//! per chunk:  0x00 ‖ 64 zero bytes                 an all-zero chunk
//!             0x01 ‖ sha256 (32) ‖ chunk key (32)  a stored chunk
//! ```
//!
//! The last chunk may be short. The chunk count is always
//! `ceil(length / chunk size)`.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use object_store::{path::Path as ObjectPath, ObjectStore};
use tokio::task::JoinSet;
use zeroize::Zeroizing;

use crate::checkpoint_seal::{open_bytes, seal_bytes, ChunkScope, PartKey, SealError, KEY_BYTES};
use crate::content_publish::PublishSourceMismatch;
use crate::content_store::ContentDigest;
use crate::prelude::*;

/// Default `contentStore.chunkSize`.
pub const DEF_CHUNK_SIZE: u32 = 1024 * 1024;
/// The smallest and largest `contentStore.chunkSize`. Below 64 KiB a part is
/// tens of thousands of objects per GiB; above 16 MiB one dirty page
/// re-uploads too much.
pub const MIN_CHUNK_SIZE: u32 = 64 * 1024;
pub const MAX_CHUNK_SIZE: u32 = 16 * 1024 * 1024;
/// Chunk sizes are whole pages and disk blocks, so the grid lines up with
/// what the guest dirties.
pub const CHUNK_ALIGN: u32 = 4096;
/// Default `contentStore.dedupWindow`.
pub const DEF_DEDUP_WINDOW: Duration = Duration::from_secs(24 * 3600);
pub const MIN_DEDUP_WINDOW: Duration = Duration::from_secs(60);

/// Chunk requests in flight per part. S3 serves one connection at roughly
/// 50 to 100 MB/s and each request pays a round trip, so this is what lets
/// one part's transfer use the node's bandwidth.
const TRANSFERS: usize = 16;
/// Chunk bytes in flight per part: large chunks get fewer concurrent
/// requests, so a 16 MiB chunk size holds 64 MiB, not 256.
const IN_FLIGHT_BYTES: usize = 64 * 1024 * 1024;
/// Threads sealing and opening chunks for one part.
const CRYPTO_THREADS: usize = 4;
const INDEX_MAGIC: &[u8; 8] = b"LGNPIDX1";
const INDEX_HEADER_BYTES: usize = 8 + 4 + 8 + 8;
const ENTRY_BYTES: usize = 1 + 32 + KEY_BYTES;
/// Most chunk names a node remembers as already in its store, about 50 MiB
/// of memory at the cap. Past it the node forgets them all and asks the
/// store again (one `HEAD` per chunk). A node with a hundred sandboxes of a
/// 1 GiB guest remembers about 55,000.
const KNOWN_CHUNKS_CAP: usize = 1 << 20;

/// How a node cuts and dedupes the parts it publishes (`contentStore`
/// `chunkSize` and `dedupWindow`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkSettings {
    pub chunk_size: u32,
    pub dedup_window: Duration,
}

impl Default for ChunkSettings {
    fn default() -> Self {
        Self {
            chunk_size: DEF_CHUNK_SIZE,
            dedup_window: DEF_DEDUP_WINDOW,
        }
    }
}

impl ChunkSettings {
    /// The dedup window `now` falls in: `(window length in seconds, window
    /// number)`.
    pub fn window_at(&self, now: SystemTime) -> (u64, u64) {
        let secs = self.dedup_window.as_secs().max(1);
        let since = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        (secs, since / secs)
    }

    pub fn window_now(&self) -> (u64, u64) {
        self.window_at(SystemTime::now())
    }
}

/// One chunk of a part, as its index records it.
#[derive(Clone, PartialEq, Eq)]
enum Entry {
    Zero,
    Data {
        digest: [u8; 32],
        key: Zeroizing<[u8; KEY_BYTES]>,
    },
}

/// A part's index: how to put the part back together from its chunks.
#[derive(Clone, PartialEq, Eq)]
pub struct PartIndex {
    chunk_size: u32,
    len: u64,
    entries: Vec<Entry>,
}

impl std::fmt::Debug for PartIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartIndex")
            .field("chunk_size", &self.chunk_size)
            .field("len", &self.len)
            .field("chunks", &self.entries.len())
            .field("zero_chunks", &self.zero_chunks())
            .finish_non_exhaustive()
    }
}

fn chunk_count(len: u64, chunk_size: u32) -> u64 {
    len.div_ceil(u64::from(chunk_size))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn digest_of(digest: &[u8; 32]) -> ContentDigest {
    ContentDigest::parse(&format!("sha256:{}", hex(digest))).expect("64 hex characters")
}

/// Whether every byte is zero. Sixteen bytes at a time: this runs over
/// every published byte.
fn is_zero(bytes: &[u8]) -> bool {
    let (words, tail) = bytes.as_chunks::<16>();
    words.iter().all(|word| u128::from_ne_bytes(*word) == 0) && tail.iter().all(|byte| *byte == 0)
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .expect("32 bytes")
}

impl PartIndex {
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    pub fn chunks(&self) -> usize {
        self.entries.len()
    }

    pub fn zero_chunks(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| matches!(entry, Entry::Zero))
            .count()
    }

    /// Plaintext bytes in chunk `index`.
    fn chunk_len(&self, index: usize) -> usize {
        let start = index as u64 * u64::from(self.chunk_size);
        self.len
            .saturating_sub(start)
            .min(u64::from(self.chunk_size)) as usize
    }

    fn encode(&self) -> Zeroizing<Vec<u8>> {
        let mut out = Zeroizing::new(Vec::with_capacity(
            INDEX_HEADER_BYTES + self.entries.len() * ENTRY_BYTES,
        ));
        out.extend_from_slice(INDEX_MAGIC);
        out.extend_from_slice(&self.chunk_size.to_be_bytes());
        out.extend_from_slice(&self.len.to_be_bytes());
        out.extend_from_slice(&(self.entries.len() as u64).to_be_bytes());
        for entry in &self.entries {
            match entry {
                Entry::Zero => out.extend_from_slice(&[0u8; ENTRY_BYTES]),
                Entry::Data { digest, key } => {
                    out.push(1);
                    out.extend_from_slice(digest);
                    out.extend_from_slice(key.as_ref());
                }
            }
        }
        out
    }

    /// Parse an opened index. The index is authenticated by its seal, so a
    /// malformed one is a bug or a format this build does not read, never
    /// tampering, and says so.
    fn decode(bytes: &[u8]) -> R<Self> {
        if bytes.len() < INDEX_HEADER_BYTES || &bytes[..8] != INDEX_MAGIC {
            bail!("the published part index has no LGNPIDX1 header (a format this build does not read)");
        }
        let chunk_size = u32::from_be_bytes(bytes[8..12].try_into().expect("4 bytes"));
        let len = u64::from_be_bytes(bytes[12..20].try_into().expect("8 bytes"));
        let count = u64::from_be_bytes(bytes[20..28].try_into().expect("8 bytes"));
        if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) {
            bail!("the published part index has chunk size {chunk_size}, outside {MIN_CHUNK_SIZE}..={MAX_CHUNK_SIZE}");
        }
        if count != chunk_count(len, chunk_size)
            || (bytes.len() - INDEX_HEADER_BYTES) as u64 != count * ENTRY_BYTES as u64
        {
            bail!(
                "the published part index lists {count} chunks in {} bytes for a {len}-byte part",
                bytes.len()
            );
        }
        // The length check above leaves no remainder.
        let entries = bytes[INDEX_HEADER_BYTES..]
            .as_chunks::<ENTRY_BYTES>()
            .0
            .iter()
            .map(|raw| match raw[0] {
                0 => Ok(Entry::Zero),
                1 => {
                    let mut key = Zeroizing::new([0u8; KEY_BYTES]);
                    key.copy_from_slice(&raw[33..]);
                    Ok(Entry::Data {
                        digest: raw[1..33].try_into().expect("32 bytes"),
                        key,
                    })
                }
                other => bail!("the published part index has an entry of kind {other}"),
            })
            .collect::<R<Vec<_>>>()?;
        Ok(Self {
            chunk_size,
            len,
            entries,
        })
    }

    /// The key that opens stored chunk `index`, with the digest its
    /// plaintext must hash to. `None` for a zero chunk.
    fn chunk_key(&self, index: usize) -> R<Option<PartKey>> {
        match &self.entries[index] {
            Entry::Zero => Ok(None),
            Entry::Data { digest, key } => {
                PartKey::from_secret("chunk", digest_of(digest), key.as_ref()).map(Some)
            }
        }
    }
}

/// Chunk names this node has seen in its store during the current dedup
/// window, so a repeat checkpoint does not ask the store about chunks it
/// just uploaded. Names are scoped to their window, so a new window starts
/// empty. Shared by every publish on the node.
#[derive(Default)]
pub struct KnownChunks {
    inner: Mutex<KnownInner>,
}

#[derive(Default)]
struct KnownInner {
    window: Option<(u64, u64)>,
    names: HashSet<[u8; KEY_BYTES]>,
}

impl std::fmt::Debug for KnownChunks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().expect("known chunks");
        f.debug_struct("KnownChunks")
            .field("window", &inner.window)
            .field("names", &inner.names.len())
            .finish()
    }
}

impl KnownChunks {
    fn enter_window(&self, window: (u64, u64)) {
        let mut inner = self.inner.lock().expect("known chunks");
        if inner.window != Some(window) {
            inner.window = Some(window);
            inner.names.clear();
        }
    }

    fn contains(&self, name: &[u8; KEY_BYTES]) -> bool {
        self.inner
            .lock()
            .expect("known chunks")
            .names
            .contains(name)
    }

    fn insert(&self, names: impl IntoIterator<Item = [u8; KEY_BYTES]>) {
        let mut inner = self.inner.lock().expect("known chunks");
        for name in names {
            if inner.names.len() >= KNOWN_CHUNKS_CAP {
                inner.names.clear();
            }
            inner.names.insert(name);
        }
    }

    /// How many names are remembered (tests and debug logs).
    pub fn len(&self) -> usize {
        self.inner.lock().expect("known chunks").names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// What one part's publish did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PublishedPart {
    /// The store already held this part's index for this window: nothing
    /// was read or sent.
    pub already_published: bool,
    /// Plaintext bytes of the chunks uploaded.
    pub uploaded_bytes: u64,
    pub uploaded_chunks: u64,
    /// Non-zero chunks the store already held (or this part repeats).
    pub reused_chunks: u64,
    pub zero_chunks: u64,
}

fn runtime() -> R<tokio::runtime::Runtime> {
    // Publishing and fetching are blocking by contract (they run on blocking
    // threads, like the artifact fetchers), and object_store is async: a
    // private runtime whose workers seal, open, and move chunks in parallel
    // while the calling thread reads or writes the part.
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(CRYPTO_THREADS)
        .thread_name("legion-chunks")
        .enable_all()
        .build()
        .context("build the chunk transfer runtime")
}

/// How many chunks of `chunk_size` move at once, at most `cap` (always
/// [`TRANSFERS`] outside tests and benchmarks).
fn transfers(chunk_size: u32, cap: usize) -> usize {
    (IN_FLIGHT_BYTES / chunk_size.max(1) as usize)
        .clamp(2, TRANSFERS)
        .min(cap.max(1))
}

fn object(name: &str) -> ObjectPath {
    ObjectPath::from(name)
}

/// Publish the part at `source` (which must hash to `key.digest()`) into
/// `objects` (the store's `sealed/` prefix) as chunks keyed in `scope`, then
/// its index at `index_name` ([`PartKey::index_object_name`]). The index goes
/// last, so a reader that finds it finds every chunk it names. Fails with
/// [`PublishSourceMismatch`], writing no index, when the bytes do not hash
/// to the digest. Blocking.
pub fn publish_part(
    objects: Arc<dyn ObjectStore>,
    source: &Path,
    key: &PartKey,
    scope: &ChunkScope,
    index_name: &str,
    settings: ChunkSettings,
    known: &KnownChunks,
) -> R<PublishedPart> {
    publish_part_with(
        objects, source, key, scope, index_name, settings, known, TRANSFERS,
    )
}

/// [`publish_part`] with at most `cap` chunk uploads in flight.
#[allow(clippy::too_many_arguments)]
fn publish_part_with(
    objects: Arc<dyn ObjectStore>,
    source: &Path,
    key: &PartKey,
    scope: &ChunkScope,
    index_name: &str,
    settings: ChunkSettings,
    known: &KnownChunks,
    cap: usize,
) -> R<PublishedPart> {
    known.enter_window(settings.window_now());
    let runtime = runtime()?;
    runtime.block_on(async {
        match objects.head(&object(index_name)).await {
            Ok(_) => {
                return Ok(PublishedPart {
                    already_published: true,
                    ..Default::default()
                })
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(anyhow!(err).context(format!("head index {index_name}"))),
        }
        let file = fs::File::open(source).with_context(|| format!("open {}", source.display()))?;
        let len = file.metadata()?.len();
        let chunk_size = settings.chunk_size;
        let count = chunk_count(len, chunk_size);
        let mut whole = ring::digest::Context::new(&ring::digest::SHA256);
        let mut entries = Vec::with_capacity(count as usize);
        let mut outcome = PublishedPart::default();
        // Chunks this publish queued, so a part that repeats a chunk (an
        // all-0xff page run, a duplicated file) uploads it once.
        let mut queued = HashSet::new();
        let mut confirmed = Vec::new();
        // Dropping a JoinSet aborts what is still in it, so any early return
        // below (a failed upload, an unreadable source, a digest mismatch)
        // cancels the other uploads and never writes the index.
        let mut inflight = JoinSet::new();
        let max_in_flight = transfers(chunk_size, cap);
        let mut buf = vec![0u8; chunk_size as usize];
        for index in 0..count {
            let offset = index * u64::from(chunk_size);
            let n = (len - offset).min(u64::from(chunk_size)) as usize;
            file.read_exact_at(&mut buf[..n], offset).map_err(|err| {
                anyhow!(err).context(format!(
                    "read {} at {offset}; the part changed while being published",
                    source.display()
                ))
            })?;
            let chunk = &buf[..n];
            whole.update(chunk);
            if is_zero(chunk) {
                entries.push(Entry::Zero);
                outcome.zero_chunks += 1;
                continue;
            }
            let digest = sha256(chunk);
            let chunk_key = scope.chunk_key(&digest_of(&digest))?;
            let name = chunk_key.object_id();
            entries.push(Entry::Data {
                digest,
                key: Zeroizing::new(*chunk_key.secret()),
            });
            if known.contains(&name) || !queued.insert(name) {
                outcome.reused_chunks += 1;
                continue;
            }
            let objects = objects.clone();
            let plain = chunk.to_vec();
            inflight.spawn(async move {
                let path = object(&hex(&name));
                match objects.head(&path).await {
                    Ok(_) => return Ok((name, 0u64)),
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(err) => return Err(anyhow!(err).context(format!("head chunk {path}"))),
                }
                let sealed = seal_bytes(&chunk_key, &plain)?;
                objects
                    .put(&path, sealed.into())
                    .await
                    .with_context(|| format!("upload chunk {path}"))?;
                Ok::<_, anyhow::Error>((name, plain.len() as u64))
            });
            while inflight.len() >= max_in_flight {
                let done = inflight.join_next().await.expect("in flight");
                record_upload(done, &mut outcome, &mut confirmed)?;
            }
        }
        // Every chunk was hashed as it was read: publish the index only when
        // those are exactly the bytes the digest names.
        let whole: [u8; 32] = whole.finish().as_ref().try_into().expect("32 bytes");
        let actual = digest_of(&whole);
        if &actual != key.digest() {
            return Err(anyhow::Error::new(PublishSourceMismatch {
                expected: key.digest().clone(),
                actual,
            }));
        }
        while let Some(done) = inflight.join_next().await {
            record_upload(done, &mut outcome, &mut confirmed)?;
        }
        known.insert(confirmed);
        let index = PartIndex {
            chunk_size,
            len,
            entries,
        };
        let sealed = seal_bytes(&key.index_key(index_name)?, &index.encode())?;
        objects
            .put(&object(index_name), sealed.into())
            .await
            .with_context(|| format!("upload index {index_name}"))?;
        Ok(outcome)
    })
}

fn record_upload(
    done: Result<R<([u8; KEY_BYTES], u64)>, tokio::task::JoinError>,
    outcome: &mut PublishedPart,
    confirmed: &mut Vec<[u8; KEY_BYTES]>,
) -> R<()> {
    let (name, uploaded) = done.context("chunk upload task")??;
    if uploaded > 0 {
        outcome.uploaded_chunks += 1;
        outcome.uploaded_bytes += uploaded;
    } else {
        outcome.reused_chunks += 1;
    }
    confirmed.push(name);
    Ok(())
}

/// Bounds on one part's fetch.
#[derive(Debug, Clone, Copy)]
pub struct FetchLimits {
    /// The largest part this node fetches (`artifacts.fetch.maxArtifactSize`).
    pub max_bytes: Option<u64>,
    pub deadline: Option<Instant>,
    /// Tries per chunk. object_store already retries transient HTTP errors
    /// inside each try.
    pub attempts: u32,
}

/// What one part's fetch did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FetchedPart {
    /// Plaintext bytes downloaded (zero chunks are not downloaded).
    pub fetched_bytes: u64,
    pub chunks: u64,
    pub zero_chunks: u64,
}

/// A chunk the index names is not in the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingChunk {
    pub index: u64,
    pub name: String,
}

impl std::fmt::Display for MissingChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "chunk {} ({}) of the published part is missing from the content store: something \
             deleted it, for example a bucket lifecycle rule shorter than the checkpoint's age \
             plus contentStore.dedupWindow",
            self.index, self.name
        )
    }
}

impl std::error::Error for MissingChunk {}

/// Whether an error will fail the same way on every try: tampered or
/// wrongly keyed bytes, a deleted chunk.
pub fn is_permanent(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause.is::<MissingChunk>()
            || cause
                .downcast_ref::<SealError>()
                .is_some_and(|seal| !matches!(seal, SealError::Truncated { .. }))
    })
}

async fn get_bytes(objects: &Arc<dyn ObjectStore>, name: &str, index: u64) -> R<bytes::Bytes> {
    let fetched = match objects.get(&object(name)).await {
        Ok(fetched) => fetched,
        Err(object_store::Error::NotFound { .. }) => {
            return Err(anyhow::Error::new(MissingChunk {
                index,
                name: name.to_string(),
            }))
        }
        Err(err) => return Err(anyhow!(err).context(format!("get {name}"))),
    };
    fetched
        .bytes()
        .await
        .with_context(|| format!("read {name}"))
}

/// Fetch the part whose index is `index_name` in `objects` (the store's
/// `sealed/` prefix) into a new sparse file at `dest`: open the index with
/// `key` (the part key), then fetch, open, and verify every distinct stored
/// chunk once, up to [`TRANSFERS`] at a time, retrying only the chunks that
/// failed. A chunk the part repeats is written at every offset it fills. On
/// any failure the other fetches are cancelled and `dest` is removed. The
/// caller checks the whole file against `key.digest()` (the content store
/// import does). Blocking.
pub fn fetch_part(
    objects: Arc<dyn ObjectStore>,
    index_name: &str,
    key: &PartKey,
    dest: &Path,
    limits: FetchLimits,
) -> R<FetchedPart> {
    fetch_part_with(objects, index_name, key, dest, limits, TRANSFERS)
}

/// Removes a half-written part on drop unless disarmed.
struct RemoveOnDrop(Option<PathBuf>);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_file(path);
        }
    }
}

/// [`fetch_part`] with at most `cap` chunk fetches in flight.
fn fetch_part_with(
    objects: Arc<dyn ObjectStore>,
    index_name: &str,
    key: &PartKey,
    dest: &Path,
    limits: FetchLimits,
    cap: usize,
) -> R<FetchedPart> {
    let runtime = runtime()?;
    runtime.block_on(async {
        let attempts = limits.attempts.max(1);
        let mut sealed_index = None;
        let mut last_err = None;
        let mut tried = 0;
        for attempt in 1..=attempts {
            tried = attempt;
            match get_bytes(&objects, index_name, 0).await {
                Ok(bytes) => {
                    sealed_index = Some(bytes);
                    break;
                }
                Err(err) if is_permanent(&err) || attempt == attempts => {
                    last_err = Some(err);
                    break;
                }
                Err(err) => {
                    warn!(index = index_name, attempt, error = %format!("{err:#}"), "Part index fetch failed; retrying");
                    last_err = Some(err);
                    tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
                }
            }
        }
        let Some(sealed_index) = sealed_index else {
            let err = last_err.unwrap_or_else(|| anyhow!("no attempt ran"));
            // The index itself is gone: say so plainly, not as chunk 0.
            if err.chain().any(|cause| cause.is::<MissingChunk>()) {
                bail!(
                    "the published part's index {index_name} is not in the content store (it was \
                     deleted, or the snapshot was published to a different store)"
                );
            }
            return Err(err.context(format!(
                "fetching part index {index_name} after {tried} attempt(s)"
            )));
        };
        let plain = Zeroizing::new(
            open_bytes(&key.index_key(index_name)?, &sealed_index)
                .with_context(|| format!("opening part index {index_name}"))?,
        );
        let index = Arc::new(PartIndex::decode(&plain)?);
        if let Some(max) = limits.max_bytes {
            if index.len > max {
                bail!(
                    "the published part is {} bytes, over this node's artifact size limit of {max}",
                    index.len
                );
            }
        }
        let file = Arc::new(
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(dest)
                .with_context(|| format!("create {}", dest.display()))?,
        );
        let mut cleanup = RemoveOnDrop(Some(dest.to_path_buf()));
        // Zero chunks stay holes: the file is sparse wherever the guest
        // never wrote.
        file.set_len(index.len)?;
        // One fetch per distinct chunk, in file order. A part that repeats a
        // chunk (an all-0xff page run, a duplicated file) stores it once, so
        // it is downloaded once and written at each offset that holds it.
        let mut offsets: HashMap<[u8; 32], Vec<usize>> = HashMap::new();
        for (i, entry) in index.entries.iter().enumerate() {
            if let Entry::Data { digest, .. } = entry {
                offsets.entry(*digest).or_default().push(i);
            }
        }
        let mut pending: Vec<Vec<usize>> = offsets.into_values().collect();
        pending.sort_unstable_by_key(|at| at[0]);
        let mut outcome = FetchedPart {
            chunks: index.entries.len() as u64,
            zero_chunks: index.zero_chunks() as u64,
            ..Default::default()
        };
        let max_in_flight = transfers(index.chunk_size, cap);
        for attempt in 1..=attempts {
            if let Some(deadline) = limits.deadline {
                if Instant::now() >= deadline {
                    bail!(
                        "fetch deadline exceeded with {} of {} chunks left",
                        pending.len(),
                        index.entries.len()
                    );
                }
            }
            let mut failed = Vec::new();
            let mut errors: Vec<anyhow::Error> = Vec::new();
            // Dropping a JoinSet aborts what is still in it, so a permanent
            // failure below cancels the other fetches before `dest` goes.
            let mut inflight = JoinSet::new();
            let mut queue = pending.into_iter();
            loop {
                while inflight.len() < max_in_flight {
                    let Some(at) = queue.next() else { break };
                    let (objects, index, file) = (objects.clone(), index.clone(), file.clone());
                    inflight.spawn(async move {
                        let result = async {
                            let i = at[0];
                            let chunk_key = index.chunk_key(i)?.expect("a stored chunk");
                            let name = chunk_key.object_name();
                            let sealed = get_bytes(&objects, &name, i as u64).await?;
                            let plain = Zeroizing::new(
                                open_bytes(&chunk_key, &sealed)
                                    .with_context(|| format!("opening chunk {i} ({name})"))?,
                            );
                            if plain.len() != index.chunk_len(i)
                                || &digest_of(&sha256(&plain)) != chunk_key.digest()
                            {
                                bail!("chunk {i} ({name}) does not match the digest its index records");
                            }
                            for &j in &at {
                                file.write_all_at(&plain, j as u64 * u64::from(index.chunk_size))?;
                            }
                            Ok::<u64, anyhow::Error>(plain.len() as u64)
                        }
                        .await;
                        (at, result)
                    });
                }
                let Some(done) = inflight.join_next().await else { break };
                let (at, result) = done.context("chunk fetch task")?;
                match result {
                    Ok(bytes) => outcome.fetched_bytes += bytes,
                    Err(err) if is_permanent(&err) => return Err(err),
                    Err(err) => {
                        failed.push(at);
                        errors.push(err);
                    }
                }
            }
            if failed.is_empty() {
                cleanup.0 = None;
                return Ok(outcome);
            }
            let first = errors.into_iter().next().expect("an error per failure");
            if attempt == attempts {
                return Err(first.context(format!(
                    "{} chunk(s) still failed after {attempts} attempt(s)",
                    failed.len()
                )));
            }
            warn!(
                index = index_name,
                attempt,
                failed = failed.len(),
                error = %format!("{first:#}"),
                "Chunk fetches failed; retrying those"
            );
            tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
            pending = failed;
        }
        unreachable!("the last attempt returns")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint_seal::ChunkKey;
    use object_store::local::LocalFileSystem;

    const CK: [u8; KEY_BYTES] = [7u8; KEY_BYTES];
    const KIB: usize = 1024;

    fn settings(chunk_size: u32) -> ChunkSettings {
        ChunkSettings {
            chunk_size,
            dedup_window: DEF_DEDUP_WINDOW,
        }
    }

    struct Store {
        dir: tempfile::TempDir,
        objects: Arc<dyn ObjectStore>,
    }

    impl Store {
        fn new() -> R<Self> {
            let dir = tempfile::tempdir()?;
            fs::create_dir_all(dir.path().join("sealed"))?;
            let objects: Arc<dyn ObjectStore> =
                Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("sealed"))?);
            Ok(Self { dir, objects })
        }

        fn objects(&self) -> usize {
            fs::read_dir(self.dir.path().join("sealed"))
                .unwrap()
                .count()
        }

        fn stored_bytes(&self) -> u64 {
            fs::read_dir(self.dir.path().join("sealed"))
                .unwrap()
                .map(|entry| entry.unwrap().metadata().unwrap().len())
                .sum()
        }
    }

    /// A deterministic pseudo-random buffer (never all zero per 4 KiB).
    fn noise(len: usize, seed: u64) -> Vec<u8> {
        let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect()
    }

    struct Published {
        outcome: PublishedPart,
        index_name: String,
        key: PartKey,
        /// How long `publish_part` alone took (the benches report it).
        secs: f64,
    }

    fn publish(
        store: &Store,
        bytes: &[u8],
        chunk_size: u32,
        job: &str,
        known: &KnownChunks,
    ) -> R<Published> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("part");
        fs::write(&path, bytes)?;
        let digest = ContentDigest::of_bytes(bytes);
        let key = PartKey::derive(&CK, "ck_test", &digest)?;
        let settings = settings(chunk_size);
        let (secs, window) = settings.window_now();
        let scope = ChunkKey::derive(&CK, "ck_test", job)?.scope(secs, window)?;
        let index_name = key.index_object_name(&scope, chunk_size);
        let started = Instant::now();
        let outcome = publish_part(
            store.objects.clone(),
            &path,
            &key,
            &scope,
            &index_name,
            settings,
            known,
        )?;
        Ok(Published {
            outcome,
            index_name,
            key,
            secs: started.elapsed().as_secs_f64(),
        })
    }

    fn fetch(store: &Store, published: &Published) -> R<(Vec<u8>, FetchedPart)> {
        let dir = tempfile::tempdir()?;
        let dest = dir.path().join("fetched");
        let outcome = fetch_part(
            store.objects.clone(),
            &published.index_name,
            &published.key,
            &dest,
            FetchLimits {
                max_bytes: None,
                deadline: None,
                attempts: 2,
            },
        )?;
        Ok((fs::read(&dest)?, outcome))
    }

    #[test]
    fn a_part_round_trips_across_chunk_boundaries() -> R<()> {
        let chunk = 64 * KIB;
        for len in [0, 1, chunk - 1, chunk, chunk + 1, 5 * chunk + 17] {
            let store = Store::new()?;
            let bytes = noise(len, len as u64);
            let published = publish(&store, &bytes, chunk as u32, "job", &KnownChunks::default())?;
            assert_eq!(
                published.outcome.uploaded_bytes, len as u64,
                "len {len}: every byte is new"
            );
            let (back, fetched) = fetch(&store, &published)?;
            assert_eq!(back, bytes, "len {len}");
            assert_eq!(fetched.fetched_bytes, len as u64);
            assert_eq!(fetched.chunks, len.div_ceil(chunk) as u64);
        }
        Ok(())
    }

    #[test]
    fn zero_chunks_are_never_stored_and_come_back_as_holes() -> R<()> {
        let store = Store::new()?;
        let chunk = 64 * KIB;
        // 16 chunks: data, 14 zero chunks, data.
        let mut bytes = vec![0u8; 16 * chunk];
        bytes[..chunk].copy_from_slice(&noise(chunk, 1));
        bytes[15 * chunk..].copy_from_slice(&noise(chunk, 2));
        let published = publish(&store, &bytes, chunk as u32, "job", &KnownChunks::default())?;
        assert_eq!(published.outcome.zero_chunks, 14);
        assert_eq!(published.outcome.uploaded_chunks, 2);
        assert_eq!(published.outcome.uploaded_bytes, 2 * chunk as u64);
        // Two chunks and the index.
        assert_eq!(store.objects(), 3);
        let (back, fetched) = fetch(&store, &published)?;
        assert_eq!(back, bytes);
        assert_eq!(fetched.fetched_bytes, 2 * chunk as u64);
        assert_eq!(fetched.zero_chunks, 14);
        Ok(())
    }

    /// The point of the format: a second checkpoint of the same guest with
    /// a small dirty set uploads only the chunks that changed.
    #[test]
    fn a_repeat_checkpoint_uploads_only_changed_chunks() -> R<()> {
        let store = Store::new()?;
        let chunk = 64 * KIB;
        let known = KnownChunks::default();
        let first = noise(64 * chunk, 3);
        let one = publish(&store, &first, chunk as u32, "job", &known)?;
        assert_eq!(one.outcome.uploaded_chunks, 64);
        let mut second = first.clone();
        for page in [3usize, 40, 41] {
            second[page * chunk + 100] ^= 0xff;
        }
        let objects_before = store.objects();
        let two = publish(&store, &second, chunk as u32, "job", &known)?;
        assert_eq!(two.outcome.uploaded_chunks, 3);
        assert_eq!(two.outcome.uploaded_bytes, 3 * chunk as u64);
        assert_eq!(two.outcome.reused_chunks, 61);
        assert_eq!(
            store.objects(),
            objects_before + 3 + 1,
            "3 chunks and an index"
        );
        // Both checkpoints still restore exactly.
        assert_eq!(fetch(&store, &one)?.0, first);
        assert_eq!(fetch(&store, &two)?.0, second);

        // A node that never saw the first publish (no known names) still
        // uploads nothing it does not have to: it asks the store.
        let mut third = second.clone();
        third[10 * chunk] ^= 1;
        let fresh = publish(&store, &third, chunk as u32, "job", &KnownChunks::default())?;
        assert_eq!(fresh.outcome.uploaded_chunks, 1);
        assert_eq!(fresh.outcome.reused_chunks, 63);

        // Publishing the same part again in the same window does nothing.
        let again = publish(&store, &third, chunk as u32, "job", &known)?;
        assert!(again.outcome.already_published);
        Ok(())
    }

    #[test]
    fn sandboxes_of_one_job_share_chunks_and_other_jobs_do_not() -> R<()> {
        let store = Store::new()?;
        let chunk = 64 * KIB;
        let image = noise(32 * chunk, 4);
        let mut a = image.clone();
        a[chunk] ^= 1;
        let mut b = image.clone();
        b[2 * chunk] ^= 1;
        publish(&store, &a, chunk as u32, "job", &KnownChunks::default())?;
        let shared = publish(&store, &b, chunk as u32, "job", &KnownChunks::default())?;
        assert_eq!(
            shared.outcome.uploaded_chunks, 2,
            "the chunk b changed, and the image chunk a changed that b kept"
        );
        let other = publish(
            &store,
            &b,
            chunk as u32,
            "other-job",
            &KnownChunks::default(),
        )?;
        assert_eq!(
            other.outcome.uploaded_chunks, 32,
            "another job's chunks are keyed differently"
        );
        Ok(())
    }

    #[test]
    fn a_new_dedup_window_uploads_again() -> R<()> {
        let store = Store::new()?;
        let chunk = 64 * KIB;
        let bytes = noise(8 * chunk, 5);
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("part");
        fs::write(&path, &bytes)?;
        let key = PartKey::derive(&CK, "ck_test", &ContentDigest::of_bytes(&bytes))?;
        let chunks = ChunkKey::derive(&CK, "ck_test", "job")?;
        let known = KnownChunks::default();
        let mut names = Vec::new();
        for window in [100, 101] {
            let scope = chunks.scope(3600, window)?;
            let name = key.index_object_name(&scope, chunk as u32);
            let outcome = publish_part(
                store.objects.clone(),
                &path,
                &key,
                &scope,
                &name,
                settings(chunk as u32),
                &known,
            )?;
            assert_eq!(outcome.uploaded_chunks, 8, "window {window}");
            names.push(name);
        }
        assert_ne!(names[0], names[1], "each window has its own index");
        Ok(())
    }

    #[test]
    fn the_store_holds_no_plaintext_and_no_plaintext_digests() -> R<()> {
        let store = Store::new()?;
        let chunk = 64 * KIB;
        // A 32-byte line repeated: every chunk holds the same bytes.
        let bytes = b"AWS_SECRET_ACCESS_KEY=hunter2 !\n".repeat(8 * chunk / 32);
        publish(&store, &bytes, chunk as u32, "job", &KnownChunks::default())?;
        for entry in fs::read_dir(store.dir.path().join("sealed"))? {
            let entry = entry?;
            let raw = fs::read(entry.path())?;
            assert!(!raw.windows(21).any(|w| w == b"AWS_SECRET_ACCESS_KEY"));
            let name = entry.file_name().to_string_lossy().to_string();
            for piece in bytes.chunks(chunk) {
                assert_ne!(name, ContentDigest::of_bytes(piece).hex());
            }
        }
        // The eight identical chunks share one object, beside the index.
        assert_eq!(store.objects(), 2);
        assert!(store.stored_bytes() < 2 * chunk as u64);
        Ok(())
    }

    #[test]
    fn a_tampered_or_missing_chunk_fails_by_name() -> R<()> {
        let store = Store::new()?;
        let chunk = 64 * KIB;
        let bytes = noise(4 * chunk, 6);
        let published = publish(&store, &bytes, chunk as u32, "job", &KnownChunks::default())?;
        let sealed = store.dir.path().join("sealed");
        let chunk_files: Vec<_> = fs::read_dir(&sealed)?
            .map(|entry| entry.unwrap().path())
            .filter(|path| !path.ends_with(&published.index_name))
            .collect();
        assert_eq!(chunk_files.len(), 4);

        let mut raw = fs::read(&chunk_files[0])?;
        raw[30] ^= 1;
        fs::write(&chunk_files[0], &raw)?;
        let err = fetch(&store, &published).unwrap_err();
        assert!(is_permanent(&err), "{err:#}");
        assert!(
            format!("{err:#}").contains("failed authentication"),
            "{err:#}"
        );

        fs::remove_file(&chunk_files[0])?;
        let err = fetch(&store, &published).unwrap_err();
        assert!(
            format!("{err:#}").contains("missing from the content store"),
            "{err:#}"
        );
        assert!(format!("{err:#}").contains("dedupWindow"), "{err:#}");

        // A chunk swapped for another chunk of the same part fails too: its
        // key does not open it.
        fs::copy(&chunk_files[1], &chunk_files[0])?;
        let err = fetch(&store, &published).unwrap_err();
        assert!(
            format!("{err:#}").contains("failed authentication"),
            "{err:#}"
        );
        Ok(())
    }

    #[test]
    fn the_index_opens_only_with_its_part_key() -> R<()> {
        let store = Store::new()?;
        let chunk = 64 * KIB;
        let bytes = noise(2 * chunk, 7);
        let published = publish(&store, &bytes, chunk as u32, "job", &KnownChunks::default())?;
        let wrong = PartKey::derive(&[9u8; KEY_BYTES], "ck_other", published.key.digest())?;
        let err = fetch(
            &store,
            &Published {
                key: wrong,
                ..published
            },
        )
        .unwrap_err();
        assert!(
            format!("{err:#}").contains("failed authentication"),
            "{err:#}"
        );
        Ok(())
    }

    #[test]
    fn a_source_that_does_not_match_its_digest_publishes_no_index() -> R<()> {
        let store = Store::new()?;
        let chunk = 64 * KIB;
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("part");
        fs::write(&path, noise(2 * chunk, 8))?;
        let key = PartKey::derive(&CK, "ck_test", &ContentDigest::of_bytes(b"other"))?;
        let scope = ChunkKey::derive(&CK, "ck_test", "job")?.scope(3600, 1)?;
        let name = key.index_object_name(&scope, chunk as u32);
        let err = publish_part(
            store.objects.clone(),
            &path,
            &key,
            &scope,
            &name,
            settings(chunk as u32),
            &KnownChunks::default(),
        )
        .unwrap_err();
        assert!(
            err.downcast_ref::<PublishSourceMismatch>().is_some(),
            "{err:#}"
        );
        assert!(!store.dir.path().join("sealed").join(&name).exists());
        Ok(())
    }

    #[test]
    fn the_index_encodes_and_rejects_what_it_cannot_read() -> R<()> {
        let index = PartIndex {
            chunk_size: 64 * 1024,
            len: 64 * 1024 + 5,
            entries: vec![
                Entry::Zero,
                Entry::Data {
                    digest: [3u8; 32],
                    key: Zeroizing::new([4u8; KEY_BYTES]),
                },
            ],
        };
        let encoded = index.encode();
        assert_eq!(encoded.len(), INDEX_HEADER_BYTES + 2 * ENTRY_BYTES);
        assert_eq!(PartIndex::decode(&encoded)?, index);
        assert_eq!(index.chunk_len(1), 5);
        let mut short = encoded.to_vec();
        short.pop();
        assert!(PartIndex::decode(&short).is_err());
        let mut foreign = encoded.to_vec();
        foreign[..8].copy_from_slice(b"LGNPIDX9");
        assert!(format!("{:#}", PartIndex::decode(&foreign).unwrap_err()).contains("LGNPIDX1"));
        // Debug never shows a chunk key.
        assert!(!format!("{index:?}").contains("4, 4"));
        Ok(())
    }

    #[test]
    fn large_chunks_move_fewer_at_a_time() {
        assert_eq!(transfers(MIN_CHUNK_SIZE, TRANSFERS), TRANSFERS);
        assert_eq!(transfers(DEF_CHUNK_SIZE, TRANSFERS), TRANSFERS);
        assert_eq!(transfers(MAX_CHUNK_SIZE, TRANSFERS), 4);
        assert_eq!(transfers(DEF_CHUNK_SIZE, 1), 1, "a cap of one is serial");
    }

    #[test]
    fn windows_follow_the_clock() {
        let settings = ChunkSettings {
            chunk_size: DEF_CHUNK_SIZE,
            dedup_window: Duration::from_secs(3600),
        };
        let at = |secs| settings.window_at(UNIX_EPOCH + Duration::from_secs(secs));
        assert_eq!(at(0), (3600, 0));
        assert_eq!(at(3599), (3600, 0));
        assert_eq!(at(3600), (3600, 1));
    }

    // ---- parallel transfers: a store that is slow, counts, and fails -----

    use object_store::memory::InMemory;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOpts,
        PutOptions, PutPayload, PutResult,
    };
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    /// An in-memory store where every call takes `latency` plus `per_mib`
    /// per MiB moved, like one S3 connection. It records the most calls ever
    /// in flight at once, counts chunk puts and gets, and fails the puts and
    /// gets whose (1-based) numbers are listed.
    #[derive(Debug, Default)]
    struct Slow {
        inner: InMemory,
        latency: Duration,
        per_mib: Duration,
        now: AtomicUsize,
        max: AtomicUsize,
        puts: AtomicUsize,
        gets: AtomicUsize,
        fail_puts: Vec<usize>,
        fail_gets: Vec<usize>,
    }

    impl std::fmt::Display for Slow {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Slow")
        }
    }

    /// Counts a call as in flight until it is dropped, so an aborted call
    /// stops counting too.
    struct InFlight<'a>(&'a AtomicUsize);

    impl Drop for InFlight<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, SeqCst);
        }
    }

    impl Slow {
        fn new(latency_ms: u64) -> Self {
            Self {
                latency: Duration::from_millis(latency_ms),
                ..Default::default()
            }
        }

        fn enter(&self) -> InFlight<'_> {
            let now = self.now.fetch_add(1, SeqCst) + 1;
            self.max.fetch_max(now, SeqCst);
            InFlight(&self.now)
        }

        async fn wait(&self, bytes: usize) {
            let transfer = self.per_mib.mul_f64(bytes as f64 / (1 << 20) as f64);
            tokio::time::sleep(self.latency + transfer).await;
        }

        fn injected(numbers: &[usize], n: usize) -> object_store::Result<()> {
            if numbers.contains(&n) {
                return Err(object_store::Error::Generic {
                    store: "Slow",
                    source: format!("injected failure of call {n}").into(),
                });
            }
            Ok(())
        }
    }

    #[tonic::async_trait]
    impl ObjectStore for Slow {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            let _in_flight = self.enter();
            let n = self.puts.fetch_add(1, SeqCst) + 1;
            self.wait(payload.content_length()).await;
            Self::injected(&self.fail_puts, n)?;
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let _in_flight = self.enter();
            if options.head {
                self.wait(0).await;
                return self.inner.get_opts(location, options).await;
            }
            let n = self.gets.fetch_add(1, SeqCst) + 1;
            let size = match self.inner.head(location).await {
                Ok(meta) => meta.size,
                Err(_) => 0,
            };
            self.wait(size).await;
            Self::injected(&self.fail_gets, n)?;
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    /// Publish `bytes` into `objects` with at most `cap` uploads in flight.
    fn publish_to(objects: Arc<Slow>, bytes: &[u8], chunk_size: u32, cap: usize) -> R<Published> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("part");
        fs::write(&path, bytes)?;
        let key = PartKey::derive(&CK, "ck_test", &ContentDigest::of_bytes(bytes))?;
        let scope = ChunkKey::derive(&CK, "ck_test", "job")?.scope(3600, 1)?;
        let index_name = key.index_object_name(&scope, chunk_size);
        let started = Instant::now();
        let outcome = publish_part_with(
            objects,
            &path,
            &key,
            &scope,
            &index_name,
            settings(chunk_size),
            &KnownChunks::default(),
            cap,
        )?;
        Ok(Published {
            outcome,
            index_name,
            key,
            secs: started.elapsed().as_secs_f64(),
        })
    }

    /// Fetch `published` from `objects` with at most `cap` fetches in flight
    /// into `dest`.
    fn fetch_from(
        objects: Arc<Slow>,
        published: &Published,
        dest: &Path,
        attempts: u32,
        cap: usize,
    ) -> R<FetchedPart> {
        fetch_part_with(
            objects,
            &published.index_name,
            &published.key,
            dest,
            FetchLimits {
                max_bytes: None,
                deadline: None,
                attempts,
            },
            cap,
        )
    }

    async fn object_names(objects: &Slow) -> Vec<ObjectPath> {
        use futures::StreamExt;
        objects
            .inner
            .list(None)
            .map(|meta| meta.unwrap().location)
            .collect()
            .await
    }

    #[test]
    fn transfers_run_in_parallel_up_to_the_cap() -> R<()> {
        let chunk = 64 * KIB;
        let bytes = noise(48 * chunk, 20);
        for cap in [1, 4, TRANSFERS] {
            let objects = Arc::new(Slow::new(5));
            let published = publish_to(objects.clone(), &bytes, chunk as u32, cap)?;
            assert_eq!(published.outcome.uploaded_chunks, 48);
            assert_eq!(
                objects.puts.load(SeqCst),
                48 + 1,
                "every chunk, then the index"
            );
            let max = objects.max.swap(0, SeqCst);
            assert!(
                max <= cap && (cap == 1 || max > 1),
                "publish with cap {cap} had {max} calls in flight"
            );

            let dir = tempfile::tempdir()?;
            let dest = dir.path().join("part");
            fetch_from(objects.clone(), &published, &dest, 1, cap)?;
            assert_eq!(
                fs::read(&dest)?,
                bytes,
                "assembled in order whatever order chunks land in"
            );
            let max = objects.max.load(SeqCst);
            assert!(
                max <= cap && (cap == 1 || max > 1),
                "fetch with cap {cap} had {max} calls in flight"
            );
        }
        Ok(())
    }

    #[test]
    fn a_failed_upload_cancels_the_rest_and_publishes_no_index() -> R<()> {
        let chunk = 64 * KIB;
        let objects = Arc::new(Slow {
            fail_puts: vec![3],
            ..Slow::new(20)
        });
        let bytes = noise(64 * chunk, 21);
        let Err(err) = publish_to(objects.clone(), &bytes, chunk as u32, 8) else {
            panic!("a failed chunk upload fails the publish");
        };
        assert!(format!("{err:#}").contains("injected failure"), "{err:#}");
        let started = objects.puts.load(SeqCst);
        assert!(
            started < 64,
            "the uploads still queued were cancelled, not run ({started} started)"
        );
        // Nothing keeps running after the publish returns.
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(objects.puts.load(SeqCst), started);
        assert_eq!(objects.now.load(SeqCst), 0, "no call is still in flight");
        let runtime = tokio::runtime::Runtime::new()?;
        let names = runtime.block_on(object_names(&objects));
        assert!(names.len() < 64, "{} objects", names.len());
        let key = PartKey::derive(&CK, "ck_test", &ContentDigest::of_bytes(&bytes))?;
        let scope = ChunkKey::derive(&CK, "ck_test", "job")?.scope(3600, 1)?;
        let index = key.index_object_name(&scope, chunk as u32);
        assert!(
            !names.iter().any(|name| name.as_ref() == index),
            "a reader never finds an index whose chunks are not all there"
        );
        Ok(())
    }

    #[test]
    fn a_failed_fetch_retries_only_its_chunks() -> R<()> {
        let chunk = 64 * KIB;
        let bytes = noise(32 * chunk, 22);
        let objects = Arc::new(Slow::new(2));
        let published = publish_to(objects.clone(), &bytes, chunk as u32, TRANSFERS)?;
        // Gets 1 (the index) and on: fail two chunk gets once each.
        let flaky = Arc::new(Slow {
            inner: objects.inner.fork(),
            fail_gets: vec![4, 9],
            ..Slow::new(2)
        });
        let dir = tempfile::tempdir()?;
        let dest = dir.path().join("part");
        let fetched = fetch_from(flaky.clone(), &published, &dest, 2, TRANSFERS)?;
        assert_eq!(fs::read(&dest)?, bytes);
        assert_eq!(fetched.fetched_bytes, bytes.len() as u64);
        assert_eq!(
            flaky.gets.load(SeqCst),
            1 + 32 + 2,
            "the index, every chunk, and the two that failed once more"
        );

        // Out of attempts: the error says so and the half-written file goes.
        let flaky = Arc::new(Slow {
            inner: objects.inner.fork(),
            fail_gets: vec![5],
            ..Slow::new(2)
        });
        let dest = dir.path().join("part-2");
        let err = fetch_from(flaky, &published, &dest, 1, TRANSFERS).unwrap_err();
        assert!(
            format!("{err:#}").contains("1 chunk(s) still failed"),
            "{err:#}"
        );
        assert!(
            !dest.exists(),
            "a failed fetch leaves no partial part behind"
        );
        Ok(())
    }

    #[test]
    fn a_missing_chunk_cancels_the_fetch_and_removes_the_file() -> R<()> {
        let chunk = 64 * KIB;
        let objects = Arc::new(Slow::new(20));
        let published = publish_to(objects.clone(), &noise(64 * chunk, 23), chunk as u32, 8)?;
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let victim = object_names(&objects)
                .await
                .into_iter()
                .find(|name| name.as_ref() != published.index_name)
                .expect("a chunk");
            objects.inner.delete(&victim).await
        })?;
        objects.gets.store(0, SeqCst);
        let dir = tempfile::tempdir()?;
        let dest = dir.path().join("part");
        let err = fetch_from(objects.clone(), &published, &dest, 3, 8).unwrap_err();
        assert!(
            format!("{err:#}").contains("missing from the content store"),
            "{err:#}"
        );
        assert!(
            !dest.exists(),
            "a failed fetch leaves no partial part behind"
        );
        let started = objects.gets.load(SeqCst);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            objects.gets.load(SeqCst),
            started,
            "the other fetches were cancelled"
        );
        assert_eq!(objects.now.load(SeqCst), 0, "no call is still in flight");
        Ok(())
    }

    #[test]
    fn a_chunk_the_part_repeats_is_fetched_once() -> R<()> {
        let chunk = 64 * KIB;
        let page = noise(chunk, 24);
        let mut bytes = page.repeat(8);
        bytes.extend_from_slice(&noise(chunk, 25));
        let objects = Arc::new(Slow::new(0));
        let published = publish_to(objects.clone(), &bytes, chunk as u32, TRANSFERS)?;
        assert_eq!(published.outcome.uploaded_chunks, 2);
        assert_eq!(published.outcome.reused_chunks, 7);
        let dir = tempfile::tempdir()?;
        let dest = dir.path().join("part");
        let fetched = fetch_from(objects.clone(), &published, &dest, 1, TRANSFERS)?;
        assert_eq!(fs::read(&dest)?, bytes);
        assert_eq!(fetched.fetched_bytes, 2 * chunk as u64);
        assert_eq!(objects.gets.load(SeqCst), 1 + 2, "the index and two chunks");
        Ok(())
    }

    /// Serial against parallel chunk transfers, through a store where every
    /// call pays a round trip and each connection moves a fixed rate, like
    /// S3. Run with `cargo test --release --lib checkpoint_chunks::tests::
    /// chunk_transfer_parallelism_bench -- --ignored --nocapture`.
    /// `LEGION_CHUNK_BENCH_MIB` sets the part size (default 256),
    /// `LEGION_CHUNK_BENCH_RTT_MS` the per-call latency (default 20), and
    /// `LEGION_CHUNK_BENCH_CONN_MBPS` one connection's rate in MB/s
    /// (default 80).
    #[test]
    #[ignore]
    fn chunk_transfer_parallelism_bench() -> R<()> {
        let env = |name: &str, default: u64| -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        let mib = env("LEGION_CHUNK_BENCH_MIB", 256) as usize;
        let rtt = env("LEGION_CHUNK_BENCH_RTT_MS", 20);
        let mbps = env("LEGION_CHUNK_BENCH_CONN_MBPS", 80).max(1);
        let bytes = noise(mib << 20, 30);
        for cap in [1, TRANSFERS] {
            let objects = Arc::new(Slow {
                per_mib: Duration::from_secs_f64((1 << 20) as f64 / (mbps as f64 * 1e6)),
                ..Slow::new(rtt)
            });
            let published = publish_to(objects.clone(), &bytes, DEF_CHUNK_SIZE, cap)?;
            let publish_secs = published.secs;
            let dir = tempfile::tempdir()?;
            let dest = dir.path().join("part");
            let started = Instant::now();
            fetch_from(objects.clone(), &published, &dest, 1, cap)?;
            let fetch_secs = started.elapsed().as_secs_f64();
            println!(
                "chunk_transfer_parallelism_bench: {mib} MiB part, 1 MiB chunks, {rtt} ms per call, \
                 {mbps} MB/s per connection, {cap} in flight: publish {publish_secs:.2}s \
                 ({:.0} MB/s), fetch {fetch_secs:.2}s ({:.0} MB/s)",
                bytes.len() as f64 / publish_secs / 1e6,
                bytes.len() as f64 / fetch_secs / 1e6,
            );
        }
        Ok(())
    }

    /// Publish and fetch throughput and bytes for a sequence of checkpoints
    /// of one synthetic guest with a small dirty set, through a `file://`
    /// store. Run with `cargo test --release --lib checkpoint_chunks::tests::
    /// chunked_publish_bench -- --ignored --nocapture`.
    /// `LEGION_CHUNK_BENCH_MIB` sets the part size (default 1024),
    /// `LEGION_CHUNK_BENCH_DIRTY_PCT` the share of 4 KiB pages dirtied
    /// between checkpoints in clustered runs (default 2), and
    /// `LEGION_CHUNK_BENCH_ZERO_PCT` the share of the part that is zero
    /// (default 50).
    #[test]
    #[ignore]
    fn chunked_publish_bench() -> R<()> {
        let env = |name: &str, default: u64| -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        let mib = env("LEGION_CHUNK_BENCH_MIB", 1024) as usize;
        let dirty_pct = env("LEGION_CHUNK_BENCH_DIRTY_PCT", 2);
        let zero_pct = env("LEGION_CHUNK_BENCH_ZERO_PCT", 50);
        let chunk = env("LEGION_CHUNK_BENCH_CHUNK", DEF_CHUNK_SIZE as u64) as u32;
        let store = Store::new()?;
        let len = mib << 20;
        let mut bytes = noise(len, 11);
        let zero_from = len - len * zero_pct as usize / 100;
        bytes[zero_from..].fill(0);
        let known = KnownChunks::default();
        let pages = zero_from / 4096;
        let mut seed = 12u64;
        for round in 0..4 {
            if round > 0 {
                // Dirty runs of 16 pages (64 KiB), the way a guest's
                // allocator and page cache touch memory.
                let runs = pages * dirty_pct as usize / 100 / 16;
                for r in 0..runs {
                    seed = seed
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let page = (seed >> 33) as usize % pages.saturating_sub(16).max(1);
                    let at = page * 4096;
                    bytes[at..at + 16 * 4096].copy_from_slice(&noise(16 * 4096, seed ^ r as u64));
                }
            }
            // Time only the publish and the fetch, not writing the source
            // file or reading the result back.
            let published = publish(&store, &bytes, chunk, "bench", &known)?;
            let publish_secs = published.secs;
            let dir = tempfile::tempdir()?;
            let dest = dir.path().join("fetched");
            let started = Instant::now();
            let fetched = fetch_part(
                store.objects.clone(),
                &published.index_name,
                &published.key,
                &dest,
                FetchLimits {
                    max_bytes: None,
                    deadline: None,
                    attempts: 1,
                },
            )?;
            let fetch_secs = started.elapsed().as_secs_f64();
            let back = fs::read(&dest)?;
            assert!(back == bytes);
            println!(
                "chunked_publish_bench round {round}: {mib} MiB part, chunk {} KiB: uploaded {:.1} MiB \
                 ({} chunks, {} reused, {} zero) in {publish_secs:.2}s; fetched {:.1} MiB in {fetch_secs:.2}s; \
                 store holds {:.1} MiB",
                chunk / 1024,
                published.outcome.uploaded_bytes as f64 / (1 << 20) as f64,
                published.outcome.uploaded_chunks,
                published.outcome.reused_chunks,
                published.outcome.zero_chunks,
                fetched.fetched_bytes as f64 / (1 << 20) as f64,
                store.stored_bytes() as f64 / (1 << 20) as f64,
            );
        }
        Ok(())
    }
}
