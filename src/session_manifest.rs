//! Session manifests in the content store, and the conditional writes that
//! fence moved sessions (session durability plan §6.6 and §6.7;
//! docs/session-mode.md, "Idle sessions" and "No split brain").
//!
//! An unpromoted session whose task sets `suspend.durability = "store"`
//! keeps one small JSON document in its node's `contentStore`:
//!
//! ```text
//! <contentStore.url>/sessions/<job>/<id-prefix>/<id>.json
//! ```
//!
//! The path is derived from the job and the session id alone, so any node
//! (or a picker) finds it with no lookup. The manifest names the session's
//! **generation** and the **node** holding that generation, and, once the
//! session has been checkpointed, the published checkpoint (part URLs and
//! the key ids that open them).
//!
//! The bucket is the fencing authority. Every write is conditional:
//!
//! - an idempotent `store` start (a derived id, which two nodes could mint
//!   at once) **creates** the manifest with `If-None-Match: *` before its
//!   gate opens, and the loser answers `ALREADY_EXISTS` naming the winner;
//! - every resume, on the session's own node or another, **claims**
//!   generation g+1 with `If-Match` on the ETag it read, before its gate
//!   opens, and the loser answers `ABORTED` naming the winner;
//! - every later write by the running copy (a suspend, a periodic
//!   checkpoint, a checkpoint at worker stop) is an `If-Match` write that
//!   first checks the manifest still names this node at this generation.
//!   A copy that finds a higher generation (or another node at its own)
//!   was superseded, and ends itself.
//!
//! A move also leaves a **fence marker** for the old node at
//! `<contentStore.url>/sessions/fence/<old node>/<id>`, holding the
//! generation it superseded. Every node lists its own fence prefix every
//! `sessions.fenceListEvery` and stops any listed session at or below that
//! generation (mechanism 2).
//!
//! `file://` stores implement the same conditional writes with an advisory
//! lock on a sidecar file and a content-hash ETag, which is exact across
//! processes on one host and on a shared filesystem with working `flock`;
//! `s3://` stores send real `If-None-Match` / `If-Match` preconditions.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use object_store::{path::Path as ObjectPath, ObjectStore, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};

use crate::prelude::*;
use crate::storage::objects::StoreUrl;
use crate::worker::session_log::SessionCheckpoint;

/// The manifest format this build writes and reads.
pub const MANIFEST_VERSION: u32 = 1;

/// Every session manifest and fence marker lives under this prefix.
pub const SESSIONS_PREFIX: &str = "sessions";

/// Fence markers live under `sessions/fence/<node>/`.
pub const FENCE_SEGMENT: &str = "fence";

/// What a session manifest says the session is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum ManifestState {
    /// A copy runs on `node` (or is being resumed there).
    #[default]
    Running,
    /// Suspended: nothing runs; `checkpoint` is what a resume restores.
    Suspended,
}

impl ManifestState {
    pub fn as_str(self) -> &'static str {
        match self {
            ManifestState::Running => "running",
            ManifestState::Suspended => "suspended",
        }
    }
}

/// One session's manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SessionManifest {
    pub version: u32,
    pub session_id: String,
    pub job_id: String,
    pub job_config_id: String,
    pub task_name: String,
    /// 1 at start, one more for every resume that claimed it.
    pub generation: u64,
    /// The node holding `generation`.
    pub node: String,
    /// That node's worker-plane address, so a resuming node can probe it
    /// (`Worker.GetSession`) before moving a running session.
    #[serde(default)]
    pub node_address: String,
    pub state: ManifestState,
    /// When the session was first issued, unix ms. Kept across moves.
    pub lease_issued_at_unix_ms: i64,
    /// The session's reserved resources, as JSON.
    #[serde(default)]
    pub resources: String,
    /// The newest published checkpoint: what a resume restores. Its
    /// `manifest_json` is the snapshot manifest with every memory part's
    /// `url` and `keyId`. `None` until the session is first checkpointed (a
    /// derived-id start's manifest is created before anything runs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<SessionCheckpoint>,
    pub updated_at_unix_ms: i64,
}

impl SessionManifest {
    /// Whether `node` at `generation` still owns the session by this
    /// manifest: nobody claimed a newer generation, and nobody else claimed
    /// this one.
    pub fn owned_by(&self, node: &str, generation: u64) -> bool {
        self.generation < generation || (self.generation == generation && self.node == node)
    }
}

/// The two characters a session's manifests are spread over: the first two
/// of the id's random part (after the node tag's `.`, or after the task
/// name's last `-` for a derived id), lowercased. Random base32, so the
/// prefixes are even.
pub fn id_prefix(session_id: &str) -> String {
    let random = match session_id.rsplit_once('.') {
        Some((_, rest)) => rest,
        None => session_id
            .rsplit_once('-')
            .map(|(_, rest)| rest)
            .unwrap_or(session_id),
    };
    let prefix: String = random.chars().take(2).collect();
    if prefix.is_empty() {
        "_".to_string()
    } else {
        prefix.to_ascii_lowercase()
    }
}

/// The manifest's object name under the content store's root.
pub fn manifest_object(job_id: &str, session_id: &str) -> String {
    format!(
        "{SESSIONS_PREFIX}/{job_id}/{}/{session_id}.json",
        id_prefix(session_id)
    )
}

/// The fence prefix a node lists.
pub fn fence_prefix(node_id: &str) -> String {
    format!("{SESSIONS_PREFIX}/{FENCE_SEGMENT}/{node_id}/")
}

/// The fence marker a move writes for the node it moved a session from.
pub fn fence_object(node_id: &str, session_id: &str) -> String {
    format!("{}{session_id}", fence_prefix(node_id))
}

/// What a fence marker holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FenceMarker {
    pub session_id: String,
    /// The generation the move superseded: the old node stops its copy at
    /// or below it.
    pub generation: u64,
    /// The node the session moved to.
    pub moved_to: String,
    pub at_unix_ms: i64,
}

/// A conditional write that did not happen.
#[derive(Debug)]
pub enum CasError {
    /// The precondition failed: the object exists (create) or changed since
    /// it was read (update).
    Conflict,
    /// The store could not be reached, or answered something else.
    Unavailable(anyhow::Error),
}

impl From<anyhow::Error> for CasError {
    fn from(err: anyhow::Error) -> Self {
        CasError::Unavailable(err)
    }
}

/// An object's bytes and the version token a conditional update names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned {
    pub bytes: Vec<u8>,
    pub etag: String,
}

/// A store that holds manifests and fence markers. Every method blocks:
/// call it from a blocking thread.
pub trait ManifestBackend: Send + Sync {
    /// The object, or `None` when it does not exist.
    fn get(&self, object: &str) -> R<Option<Versioned>>;
    /// Write only if nothing is there (`If-None-Match: *`). Returns the new
    /// ETag.
    fn create(&self, object: &str, bytes: &[u8]) -> Result<String, CasError>;
    /// Write only if the object is still at `etag` (`If-Match`). Returns the
    /// new ETag. A missing object is a conflict.
    fn update(&self, object: &str, bytes: &[u8], etag: &str) -> Result<String, CasError>;
    /// Write unconditionally (fence markers).
    fn put(&self, object: &str, bytes: &[u8]) -> R<()>;
    /// The object names under `prefix`.
    fn list(&self, prefix: &str) -> R<Vec<String>>;
    /// Delete; a missing object is not an error.
    fn delete(&self, object: &str) -> R<()>;
    /// The object's URL, for status output.
    fn url_for(&self, object: &str) -> String;
}

// ---------------------------------------------------------------------------
// file://
// ---------------------------------------------------------------------------

/// Manifests in a directory (a shared mount, or a test's temp dir). Each
/// conditional write takes an exclusive `flock` on `<object>.lock`, reads
/// the current bytes, compares their content hash with the expected ETag,
/// and replaces the object by rename, so a reader never sees a partial
/// document and two writers never both win.
#[derive(Debug, Clone)]
pub struct FileManifestBackend {
    root: PathBuf,
}

impl FileManifestBackend {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path(&self, object: &str) -> PathBuf {
        self.root.join(object)
    }

    fn etag_of(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(bytes);
        digest[..16].iter().map(|b| format!("{b:02x}")).collect()
    }

    fn read(path: &Path) -> R<Option<Vec<u8>>> {
        match std::fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(anyhow!(err).context(format!("read {}", path.display()))),
        }
    }

    fn write(path: &Path, bytes: &[u8]) -> R<()> {
        let dir = path.parent().context("object path has no parent")?;
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("object");
        let tmp = dir.join(format!(
            ".{name}.{}.{}.tmp",
            std::process::id(),
            crate::util::now_unix_ms()
        ));
        std::fs::write(&tmp, bytes).with_context(|| format!("write {}", tmp.display()))?;
        if let Err(err) = std::fs::rename(&tmp, path) {
            let _ = std::fs::remove_file(&tmp);
            return Err(anyhow!(err).context(format!("replace {}", path.display())));
        }
        Ok(())
    }

    /// Run `f` holding the object's sidecar lock.
    fn locked<T>(&self, object: &str, f: impl FnOnce(&Path) -> T) -> R<T> {
        use std::os::unix::io::AsRawFd;
        let path = self.path(object);
        let dir = path.parent().context("object path has no parent")?;
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let lock_path = dir.join(format!(
            ".{}.lock",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("object")
        ));
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("open {}", lock_path.display()))?;
        // SAFETY: flock on a file descriptor we own for the call's duration.
        let rc = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) };
        if rc != 0 {
            bail!(
                "lock {}: {}",
                lock_path.display(),
                std::io::Error::last_os_error()
            );
        }
        let out = f(&path);
        // SAFETY: as above; closing the file would release it too.
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_UN) };
        Ok(out)
    }
}

impl ManifestBackend for FileManifestBackend {
    fn get(&self, object: &str) -> R<Option<Versioned>> {
        Ok(Self::read(&self.path(object))?.map(|bytes| Versioned {
            etag: Self::etag_of(&bytes),
            bytes,
        }))
    }

    fn create(&self, object: &str, bytes: &[u8]) -> Result<String, CasError> {
        self.locked(object, |path| {
            if Self::read(path)?.is_some() {
                return Err(CasError::Conflict);
            }
            Self::write(path, bytes)?;
            Ok(Self::etag_of(bytes))
        })?
    }

    fn update(&self, object: &str, bytes: &[u8], etag: &str) -> Result<String, CasError> {
        self.locked(object, |path| {
            match Self::read(path)? {
                Some(current) if Self::etag_of(&current) == etag => {}
                _ => return Err(CasError::Conflict),
            }
            Self::write(path, bytes)?;
            Ok(Self::etag_of(bytes))
        })?
    }

    fn put(&self, object: &str, bytes: &[u8]) -> R<()> {
        Self::write(&self.path(object), bytes)
    }

    fn list(&self, prefix: &str) -> R<Vec<String>> {
        let dir = self.path(prefix.trim_end_matches('/'));
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(anyhow!(err).context(format!("list {}", dir.display()))),
        };
        let mut names = Vec::new();
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') || !entry.file_type()?.is_file() {
                continue;
            }
            names.push(format!("{}/{name}", prefix.trim_end_matches('/')));
        }
        names.sort();
        Ok(names)
    }

    fn delete(&self, object: &str) -> R<()> {
        match std::fs::remove_file(self.path(object)) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(anyhow!(err).context(format!("delete {object}"))),
        }
    }

    fn url_for(&self, object: &str) -> String {
        crate::storage::objects::local_url(&self.path(object))
    }
}

// ---------------------------------------------------------------------------
// object stores (s3://, and the in-memory store the tests use)
// ---------------------------------------------------------------------------

/// Manifests in an `object_store` store. For S3 the client must be opened
/// with conditional puts ([`crate::storage::objects::OpenOptions`]), which
/// make `PutMode::Create` send `If-None-Match: *` and `PutMode::Update` send
/// `If-Match: <etag>`.
pub struct ObjectStoreManifestBackend {
    /// Builds the client per call, so rotated credentials are picked up.
    client: Box<dyn Fn() -> R<Arc<dyn ObjectStore>> + Send + Sync>,
    /// The store's root (`s3://bucket/<prefix>`): object keys and URLs.
    root: StoreUrl,
}

impl std::fmt::Debug for ObjectStoreManifestBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreManifestBackend")
            .field("root", &self.root.url().as_str())
            .finish_non_exhaustive()
    }
}

impl ObjectStoreManifestBackend {
    pub fn new(
        root: StoreUrl,
        client: impl Fn() -> R<Arc<dyn ObjectStore>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            client: Box::new(client),
            root,
        }
    }

    fn key(&self, object: &str) -> R<ObjectPath> {
        self.root.key(object)
    }

    fn run<T>(&self, fut: impl std::future::Future<Output = T>) -> R<T> {
        // Blocking by contract, like the content publisher: object_store is
        // async, so each call runs on a private current-thread runtime.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build session manifest runtime")?;
        Ok(runtime.block_on(fut))
    }

    fn conditional(&self, object: &str, bytes: &[u8], mode: PutMode) -> Result<String, CasError> {
        let client = (self.client)()?;
        let key = self.key(object)?;
        let payload = bytes.to_vec();
        let outcome = self.run(async move {
            client
                .put_opts(
                    &key,
                    payload.into(),
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await
        })?;
        match outcome {
            Ok(result) => Ok(result.e_tag.unwrap_or_default()),
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::NotFound { .. }) => Err(CasError::Conflict),
            Err(err) => Err(CasError::Unavailable(
                anyhow!(err).context(format!("conditional write of {}", self.url_for(object))),
            )),
        }
    }
}

impl ManifestBackend for ObjectStoreManifestBackend {
    fn get(&self, object: &str) -> R<Option<Versioned>> {
        let client = (self.client)()?;
        let key = self.key(object)?;
        let url = self.url_for(object);
        self.run(async move {
            match client.get(&key).await {
                Ok(result) => {
                    let etag = result.meta.e_tag.clone().unwrap_or_default();
                    let bytes = result
                        .bytes()
                        .await
                        .with_context(|| format!("read {url}"))?;
                    Ok(Some(Versioned {
                        bytes: bytes.to_vec(),
                        etag,
                    }))
                }
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(err) => Err(anyhow!(err).context(format!("read {url}"))),
            }
        })?
    }

    fn create(&self, object: &str, bytes: &[u8]) -> Result<String, CasError> {
        self.conditional(object, bytes, PutMode::Create)
    }

    fn update(&self, object: &str, bytes: &[u8], etag: &str) -> Result<String, CasError> {
        self.conditional(
            object,
            bytes,
            PutMode::Update(UpdateVersion {
                e_tag: Some(etag.to_string()),
                version: None,
            }),
        )
    }

    fn put(&self, object: &str, bytes: &[u8]) -> R<()> {
        let client = (self.client)()?;
        let key = self.key(object)?;
        let payload = bytes.to_vec();
        let url = self.url_for(object);
        self.run(async move {
            client
                .put(&key, payload.into())
                .await
                .map(|_| ())
                .with_context(|| format!("write {url}"))
        })?
    }

    fn list(&self, prefix: &str) -> R<Vec<String>> {
        use futures::TryStreamExt;
        let client = (self.client)()?;
        let key = self.key(prefix.trim_end_matches('/'))?;
        let root = self.root.clone();
        self.run(async move {
            let metas: Vec<object_store::ObjectMeta> = client
                .list(Some(&key))
                .try_collect()
                .await
                .context("list session fence markers")?;
            let mut names: Vec<String> = metas
                .into_iter()
                .map(|meta| root.name_of(&meta.location))
                .collect();
            names.sort();
            Ok(names)
        })?
    }

    fn delete(&self, object: &str) -> R<()> {
        let client = (self.client)()?;
        let key = self.key(object)?;
        let url = self.url_for(object);
        self.run(async move {
            match client.delete(&key).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(err) => Err(anyhow!(err).context(format!("delete {url}"))),
            }
        })?
    }

    fn url_for(&self, object: &str) -> String {
        self.root.object_url(object)
    }
}

// ---------------------------------------------------------------------------
// The node's view: generations, claims, fences
// ---------------------------------------------------------------------------

/// Why a manifest operation did not do what was asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestError {
    /// A newer generation (or another node at this one) holds the session:
    /// the caller's copy is stale and must end. Carries the winner.
    Superseded { generation: u64, node: String },
    /// A claim lost a race: another resume claimed the generation first.
    Lost { generation: u64, node: String },
    /// A create found a manifest already there (an idempotent start that
    /// another node won).
    AlreadyExists { generation: u64, node: String },
    /// No manifest at the session's path.
    NotFound,
    /// The content store could not be reached (or answered garbage).
    Unavailable(String),
}

impl ManifestError {
    /// The winner a refusal names, as `node=<id> generation=<g>`, the
    /// machine-readable tail of every refusal message.
    pub fn winner(&self) -> Option<(String, u64)> {
        match self {
            ManifestError::Superseded { generation, node }
            | ManifestError::Lost { generation, node }
            | ManifestError::AlreadyExists { generation, node } => {
                Some((node.clone(), *generation))
            }
            _ => None,
        }
    }
}

impl std::fmt::Display for ManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestError::Superseded { generation, node } => write!(
                f,
                "superseded: the session's manifest names generation {generation} on node \
                 {node}, so this copy is stale [winner node={node} generation={generation}]"
            ),
            ManifestError::Lost { generation, node } => write!(
                f,
                "superseded: another resume claimed the session first; generation \
                 {generation} runs on node {node} [winner node={node} generation={generation}]"
            ),
            ManifestError::AlreadyExists { generation, node } => write!(
                f,
                "already_exists: the session already runs on node {node} (generation \
                 {generation}) [winner node={node} generation={generation}]"
            ),
            ManifestError::NotFound => {
                write!(f, "the session has no manifest in the content store")
            }
            ManifestError::Unavailable(why) => write!(
                f,
                "the content store holding session manifests could not be reached: {why}"
            ),
        }
    }
}

impl std::error::Error for ManifestError {}

/// Read the winner out of a refusal message (`[winner node=<id>
/// generation=<g>]`), for a caller that only has the gRPC status.
pub fn winner_from_message(message: &str) -> Option<(String, u64)> {
    let start = message.find("[winner node=")? + "[winner node=".len();
    let rest = &message[start..];
    let end = rest.find(']')?;
    let (node, generation) = rest[..end].split_once(" generation=")?;
    Some((node.to_string(), generation.parse().ok()?))
}

/// The node's handle on session manifests: the backend plus who this node
/// is, and when it last reached the store (for self-fencing).
pub struct SessionManifests {
    backend: Arc<dyn ManifestBackend>,
    node_id: String,
    node_address: String,
    last_success: Mutex<Option<Instant>>,
}

impl std::fmt::Debug for SessionManifests {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionManifests")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

/// How many times a write re-reads after a conflict it might have caused
/// itself (two writes of one copy racing) before giving up.
const WRITE_ATTEMPTS: usize = 4;

impl SessionManifests {
    pub fn new(
        backend: Arc<dyn ManifestBackend>,
        node_id: impl Into<String>,
        node_address: impl Into<String>,
    ) -> Self {
        Self {
            backend,
            node_id: node_id.into(),
            node_address: node_address.into(),
            last_success: Mutex::new(None),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn node_address(&self) -> &str {
        &self.node_address
    }

    /// When this node last completed any call to the store.
    pub fn last_success(&self) -> Option<Instant> {
        *self.last_success.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn reached(&self) {
        *self.last_success.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
    }

    /// The URL of a session's manifest.
    pub fn manifest_url(&self, job_id: &str, session_id: &str) -> String {
        self.backend.url_for(&manifest_object(job_id, session_id))
    }

    fn unavailable(&self, err: anyhow::Error) -> ManifestError {
        ManifestError::Unavailable(format!("{err:#}"))
    }

    /// The session's manifest and its ETag, or `None`.
    pub fn read(
        &self,
        job_id: &str,
        session_id: &str,
    ) -> Result<Option<(SessionManifest, String)>, ManifestError> {
        let object = manifest_object(job_id, session_id);
        let found = self
            .backend
            .get(&object)
            .map_err(|err| self.unavailable(err))?;
        self.reached();
        let Some(found) = found else {
            return Ok(None);
        };
        let manifest: SessionManifest = serde_json::from_slice(&found.bytes).map_err(|err| {
            ManifestError::Unavailable(format!(
                "the session manifest at {} is unreadable: {err}",
                self.backend.url_for(&object)
            ))
        })?;
        Ok(Some((manifest, found.etag)))
    }

    fn encode(manifest: &SessionManifest) -> Vec<u8> {
        serde_json::to_vec_pretty(manifest).unwrap_or_default()
    }

    /// A fresh manifest for a session this node holds at `generation`.
    #[allow(clippy::too_many_arguments)]
    pub fn manifest_for(
        &self,
        session_id: &str,
        job_id: &str,
        job_config_id: &str,
        task_name: &str,
        generation: u64,
        state: ManifestState,
        lease_issued_at_unix_ms: i64,
        resources: &str,
        checkpoint: Option<SessionCheckpoint>,
    ) -> SessionManifest {
        SessionManifest {
            version: MANIFEST_VERSION,
            session_id: session_id.to_string(),
            job_id: job_id.to_string(),
            job_config_id: job_config_id.to_string(),
            task_name: task_name.to_string(),
            generation,
            node: self.node_id.clone(),
            node_address: self.node_address.clone(),
            state,
            lease_issued_at_unix_ms,
            resources: resources.to_string(),
            checkpoint,
            updated_at_unix_ms: crate::util::now_unix_ms(),
        }
    }

    /// Create a new session's manifest (`If-None-Match: *`): an idempotent
    /// `store` start, before its gate opens. A manifest already there
    /// answers [`ManifestError::AlreadyExists`] with its holder — unless it
    /// names this node and this node does not run the session (`ours_stale`
    /// says so: a leftover of an ended session whose delete never landed),
    /// which is claimed over at the next generation instead.
    pub fn create(
        &self,
        manifest: &SessionManifest,
        ours_stale: bool,
    ) -> Result<(SessionManifest, String), ManifestError> {
        let object = manifest_object(&manifest.job_id, &manifest.session_id);
        match self.backend.create(&object, &Self::encode(manifest)) {
            Ok(etag) => {
                self.reached();
                Ok((manifest.clone(), etag))
            }
            Err(CasError::Unavailable(err)) => Err(self.unavailable(err)),
            Err(CasError::Conflict) => {
                self.reached();
                let Some((existing, etag)) = self.read(&manifest.job_id, &manifest.session_id)?
                else {
                    // Deleted between the two calls: one more try.
                    return match self.backend.create(&object, &Self::encode(manifest)) {
                        Ok(etag) => Ok((manifest.clone(), etag)),
                        Err(CasError::Unavailable(err)) => Err(self.unavailable(err)),
                        Err(CasError::Conflict) => Err(ManifestError::Unavailable(
                            "the session manifest is being created and deleted concurrently".into(),
                        )),
                    };
                };
                if existing.node == self.node_id && ours_stale {
                    let mut next = manifest.clone();
                    next.generation = existing.generation + 1;
                    return match self.backend.update(&object, &Self::encode(&next), &etag) {
                        Ok(etag) => Ok((next, etag)),
                        Err(CasError::Unavailable(err)) => Err(self.unavailable(err)),
                        Err(CasError::Conflict) => {
                            let winner = self
                                .read(&manifest.job_id, &manifest.session_id)?
                                .map(|(m, _)| (m.generation, m.node))
                                .unwrap_or((existing.generation, existing.node));
                            Err(ManifestError::AlreadyExists {
                                generation: winner.0,
                                node: winner.1,
                            })
                        }
                    };
                }
                Err(ManifestError::AlreadyExists {
                    generation: existing.generation,
                    node: existing.node,
                })
            }
        }
    }

    /// Write the manifest of a session this node holds at
    /// `manifest.generation` (a suspend, a periodic or worker-stop
    /// checkpoint, a failed resume handing the session back): only while
    /// the bucket still says this node owns that generation. Creates the
    /// manifest when there is none yet (a minted id's first checkpoint).
    /// A copy that finds itself superseded gets
    /// [`ManifestError::Superseded`] and must end (mechanism 1).
    pub fn write_own(&self, manifest: &SessionManifest) -> Result<String, ManifestError> {
        let object = manifest_object(&manifest.job_id, &manifest.session_id);
        let bytes = Self::encode(manifest);
        for _ in 0..WRITE_ATTEMPTS {
            let current = self.read(&manifest.job_id, &manifest.session_id)?;
            let written = match current {
                None => self.backend.create(&object, &bytes),
                Some((current, etag)) => {
                    if !current.owned_by(&self.node_id, manifest.generation) {
                        return Err(ManifestError::Superseded {
                            generation: current.generation,
                            node: current.node,
                        });
                    }
                    self.backend.update(&object, &bytes, &etag)
                }
            };
            match written {
                Ok(etag) => {
                    self.reached();
                    return Ok(etag);
                }
                Err(CasError::Unavailable(err)) => return Err(self.unavailable(err)),
                Err(CasError::Conflict) => continue,
            }
        }
        Err(ManifestError::Unavailable(
            "the session manifest kept changing under this write".into(),
        ))
    }

    /// Claim the next generation of a session (every `store` resume, before
    /// its gate opens): read the manifest, and write it back naming this
    /// node at generation + 1 with `If-Match` on the ETag read. Returns the
    /// manifest as read (the old holder and its checkpoint) and the claimed
    /// one. A racing claim that won answers [`ManifestError::Lost`] naming
    /// it; no manifest answers [`ManifestError::NotFound`].
    pub fn claim_next(
        &self,
        job_id: &str,
        session_id: &str,
    ) -> Result<(SessionManifest, SessionManifest), ManifestError> {
        let Some((current, etag)) = self.read(job_id, session_id)? else {
            return Err(ManifestError::NotFound);
        };
        let claimed = self.claim_from(&current, &etag)?;
        Ok((current, claimed))
    }

    /// Claim generation `current.generation + 1` on the ETag `current` was
    /// read at: a move that read the manifest, decided, and admitted the
    /// session claims against exactly what it read, so anything written
    /// since (another resume, the old copy's own checkpoint) makes it lose
    /// with [`ManifestError::Lost`].
    pub fn claim_from(
        &self,
        current: &SessionManifest,
        etag: &str,
    ) -> Result<SessionManifest, ManifestError> {
        let mut claimed = current.clone();
        claimed.version = MANIFEST_VERSION;
        claimed.generation = current.generation + 1;
        claimed.node = self.node_id.clone();
        claimed.node_address = self.node_address.clone();
        claimed.state = ManifestState::Running;
        claimed.updated_at_unix_ms = crate::util::now_unix_ms();
        self.swap(current, etag, claimed)
    }

    /// Claim the next generation of a session this node holds (a resume on
    /// the node that suspended it): the manifest must still name this node
    /// at `held.generation` (or be absent: the first publish never landed),
    /// otherwise the held copy is stale and gets
    /// [`ManifestError::Superseded`]. `held` is the manifest this node would
    /// write; the claim keeps the bucket's checkpoint when `held` names none.
    pub fn claim_held(&self, held: &SessionManifest) -> Result<SessionManifest, ManifestError> {
        let mut next = held.clone();
        next.generation = held.generation + 1;
        next.state = ManifestState::Running;
        next.node = self.node_id.clone();
        next.node_address = self.node_address.clone();
        next.updated_at_unix_ms = crate::util::now_unix_ms();
        match self.read(&held.job_id, &held.session_id)? {
            None => {
                let object = manifest_object(&held.job_id, &held.session_id);
                match self.backend.create(&object, &Self::encode(&next)) {
                    Ok(_) => {
                        self.reached();
                        Ok(next)
                    }
                    Err(CasError::Unavailable(err)) => Err(self.unavailable(err)),
                    Err(CasError::Conflict) => {
                        let winner = self
                            .read(&held.job_id, &held.session_id)?
                            .map(|(m, _)| (m.generation, m.node))
                            .unwrap_or((held.generation, held.node.clone()));
                        Err(ManifestError::Lost {
                            generation: winner.0,
                            node: winner.1,
                        })
                    }
                }
            }
            Some((current, etag)) => {
                if !current.owned_by(&self.node_id, held.generation) {
                    return Err(ManifestError::Superseded {
                        generation: current.generation,
                        node: current.node,
                    });
                }
                if next.checkpoint.is_none() {
                    next.checkpoint = current.checkpoint.clone();
                }
                next.generation = current.generation.max(held.generation) + 1;
                self.swap(&current, &etag, next)
            }
        }
    }

    /// Replace `current` (read at `etag`) with `next`, or name the winner.
    fn swap(
        &self,
        current: &SessionManifest,
        etag: &str,
        next: SessionManifest,
    ) -> Result<SessionManifest, ManifestError> {
        let object = manifest_object(&current.job_id, &current.session_id);
        match self.backend.update(&object, &Self::encode(&next), etag) {
            Ok(_) => {
                self.reached();
                Ok(next)
            }
            Err(CasError::Unavailable(err)) => Err(self.unavailable(err)),
            Err(CasError::Conflict) => {
                self.reached();
                let winner = self
                    .read(&current.job_id, &current.session_id)?
                    .map(|(manifest, _)| (manifest.generation, manifest.node))
                    .unwrap_or((current.generation, current.node.clone()));
                Err(ManifestError::Lost {
                    generation: winner.0,
                    node: winner.1,
                })
            }
        }
    }

    /// Delete a session's manifest when it ended for good (stopped,
    /// destroyed, or its workload exited) on this node at `generation`:
    /// only while the manifest still names this node at that generation,
    /// so an old copy ending never deletes the newer copy's record.
    pub fn delete_own(
        &self,
        job_id: &str,
        session_id: &str,
        generation: u64,
    ) -> Result<bool, ManifestError> {
        let Some((current, _)) = self.read(job_id, session_id)? else {
            return Ok(false);
        };
        if current.node != self.node_id || current.generation != generation {
            return Ok(false);
        }
        self.backend
            .delete(&manifest_object(job_id, session_id))
            .map_err(|err| self.unavailable(err))?;
        Ok(true)
    }

    /// Leave a fence marker for the node a session moved from
    /// (mechanism 2): it stops its copy at or below `generation` on its next
    /// fence listing.
    pub fn fence(
        &self,
        old_node: &str,
        session_id: &str,
        generation: u64,
    ) -> Result<(), ManifestError> {
        let marker = FenceMarker {
            session_id: session_id.to_string(),
            generation,
            moved_to: self.node_id.clone(),
            at_unix_ms: crate::util::now_unix_ms(),
        };
        self.backend
            .put(
                &fence_object(old_node, session_id),
                &serde_json::to_vec(&marker).unwrap_or_default(),
            )
            .map_err(|err| self.unavailable(err))?;
        self.reached();
        Ok(())
    }

    /// The fence markers left for this node: one `LIST` plus one read per
    /// marker (markers are rare: one per move away from this node).
    pub fn fences(&self) -> Result<Vec<FenceMarker>, ManifestError> {
        let names = self
            .backend
            .list(&fence_prefix(&self.node_id))
            .map_err(|err| self.unavailable(err))?;
        self.reached();
        let mut markers = Vec::new();
        for name in names {
            // A job named `fence` would share the prefix; its manifests end
            // in `.json`, markers never do.
            if name.ends_with(".json") {
                continue;
            }
            let Some(found) = self
                .backend
                .get(&name)
                .map_err(|err| self.unavailable(err))?
            else {
                continue;
            };
            match serde_json::from_slice::<FenceMarker>(&found.bytes) {
                Ok(marker) => markers.push(marker),
                Err(err) => {
                    warn!(marker = %name, error = %err, "Ignoring an unreadable session fence marker");
                }
            }
        }
        Ok(markers)
    }

    /// Remove a fence marker this node acted on.
    pub fn clear_fence(&self, session_id: &str) -> Result<(), ManifestError> {
        self.backend
            .delete(&fence_object(&self.node_id, session_id))
            .map_err(|err| self.unavailable(err))
    }
}

/// Build the manifest backend for a worker's `contentStore`. A `file://`
/// store keeps the `flock` backend: `object_store`'s local filesystem cannot
/// replace an object only if it is unchanged (`PutMode::Update`), which every
/// claim needs.
pub fn backend_for(
    target: &crate::content_publish::ContentStoreTarget,
    credentials: Arc<dyn crate::artifacts::CredentialSource>,
) -> R<Arc<dyn ManifestBackend>> {
    let root = target.validate()?;
    if let Some(dir) = root.local_root() {
        return Ok(Arc::new(FileManifestBackend::new(dir)));
    }
    let target = target.clone();
    Ok(Arc::new(ObjectStoreManifestBackend::new(root, move || {
        target.open(credentials.as_ref(), true)
    })))
}

/// Whether a node that last reached a manager `since_manager` ago and its
/// content store `since_store` ago (either `None`: never) must fence a
/// session whose task sets `fence_after` (mechanism 4): only when it has
/// reached neither for that long.
pub fn self_fence_due(
    since_manager: Option<std::time::Duration>,
    since_store: Option<std::time::Duration>,
    fence_after: std::time::Duration,
) -> bool {
    let silent =
        |since: Option<std::time::Duration>| since.is_none_or(|since| since >= fence_after);
    silent(since_manager) && silent(since_store)
}

#[cfg(test)]
mod session_manifest_test {
    use super::*;

    fn manifests(root: &Path, node: &str) -> SessionManifests {
        SessionManifests::new(
            Arc::new(FileManifestBackend::new(root)),
            node,
            format!("https://{node}:4848"),
        )
    }

    fn fresh(on: &SessionManifests, id: &str, generation: u64) -> SessionManifest {
        on.manifest_for(
            id,
            "agents",
            "agents-1",
            "workspace",
            generation,
            ManifestState::Running,
            1_000,
            "{}",
            None,
        )
    }

    #[test]
    fn manifests_live_at_a_path_derived_from_the_job_and_id_test() {
        assert_eq!(
            manifest_object("agents", "workspace-q4n7c2rd.7k2qd9x4m1b8c3rj5t0v6wye2h"),
            "sessions/agents/7k/workspace-q4n7c2rd.7k2qd9x4m1b8c3rj5t0v6wye2h.json"
        );
        assert_eq!(
            manifest_object("agents", "my-task-Q4N7C2RD9X"),
            "sessions/agents/q4/my-task-Q4N7C2RD9X.json"
        );
        assert_eq!(fence_object("w-17", "ws-1"), "sessions/fence/w-17/ws-1");
    }

    #[test]
    fn file_conditional_writes_create_once_and_update_on_the_etag_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let backend = FileManifestBackend::new(dir.path());
        let first = backend.create("sessions/a/x.json", b"one").map_err(cas)?;
        assert!(matches!(
            backend.create("sessions/a/x.json", b"two"),
            Err(CasError::Conflict)
        ));
        let second = backend
            .update("sessions/a/x.json", b"two", &first)
            .map_err(cas)?;
        assert_ne!(first, second);
        assert!(
            matches!(
                backend.update("sessions/a/x.json", b"three", &first),
                Err(CasError::Conflict)
            ),
            "a stale ETag loses"
        );
        assert!(matches!(
            backend.update("sessions/a/missing.json", b"x", &first),
            Err(CasError::Conflict)
        ));
        let read = backend.get("sessions/a/x.json")?.expect("written");
        assert_eq!(
            (read.bytes.as_slice(), read.etag.as_str()),
            (&b"two"[..], second.as_str())
        );
        Ok(())
    }

    fn cas(err: CasError) -> anyhow::Error {
        match err {
            CasError::Conflict => anyhow!("conflict"),
            CasError::Unavailable(err) => err,
        }
    }

    /// Plan §6.7 row 10: two resumes race for the same generation; the
    /// conditional write lets exactly one win, and the loser learns who.
    #[test]
    fn racing_claims_have_one_winner_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let a = manifests(dir.path(), "node-a");
        a.write_own(&fresh(&a, "ws-1", 1)).map_err(|e| anyhow!(e))?;

        // Eight resumes read the same manifest, then all claim on what they
        // read at once (a claim that reads after another landed claims on
        // top of it, which is a later resume, not a race).
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let contenders: Vec<_> = (0..8)
            .map(|n| {
                let root = dir.path().to_path_buf();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let node = manifests(&root, &format!("node-{n}"));
                    let (current, etag) = node
                        .read("agents", "ws-1")
                        .and_then(|found| found.ok_or(ManifestError::NotFound))?;
                    barrier.wait();
                    node.claim_from(&current, &etag)
                        .map(|claimed| (current, claimed))
                })
            })
            .collect();
        let outcomes: Vec<_> = contenders
            .into_iter()
            .map(|handle| handle.join().expect("thread"))
            .collect();
        let winners: Vec<_> = outcomes.iter().filter(|o| o.is_ok()).collect();
        assert_eq!(winners.len(), 1, "{outcomes:?}");
        let (_, claimed) = winners[0].as_ref().expect("won");
        assert_eq!(claimed.generation, 2);
        for outcome in &outcomes {
            if let Err(err) = outcome {
                assert_eq!(
                    err,
                    &ManifestError::Lost {
                        generation: 2,
                        node: claimed.node.clone()
                    }
                );
                assert_eq!(
                    winner_from_message(&err.to_string()),
                    Some((claimed.node.clone(), 2))
                );
            }
        }
        Ok(())
    }

    /// Plan §6.7 row 3: two idempotent `store` starts of one derived id;
    /// the manifest create lets one win and names it to the other.
    #[test]
    fn an_idempotent_start_race_has_one_winner_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let a = manifests(dir.path(), "node-a");
        let b = manifests(dir.path(), "node-b");
        a.create(&fresh(&a, "ws-derived", 1), false)
            .map_err(|e| anyhow!(e))?;
        let lost = b
            .create(&fresh(&b, "ws-derived", 1), false)
            .expect_err("b loses");
        assert_eq!(
            lost,
            ManifestError::AlreadyExists {
                generation: 1,
                node: "node-a".into()
            }
        );
        // A leftover of a session node-a no longer runs is claimed over.
        let (taken, _) = a
            .create(&fresh(&a, "ws-derived", 1), true)
            .map_err(|e| anyhow!(e))?;
        assert_eq!(taken.generation, 2);
        Ok(())
    }

    /// Mechanism 1: once a move claimed g+1, the old copy's next write
    /// (a checkpoint, a suspend) fails and names the winner.
    #[test]
    fn a_superseded_copy_fails_its_next_write_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let a = manifests(dir.path(), "node-a");
        let b = manifests(dir.path(), "node-b");
        let mine = fresh(&a, "ws-1", 1);
        a.write_own(&mine).map_err(|e| anyhow!(e))?;
        a.write_own(&mine).map_err(|e| anyhow!(e))?;
        let (before, claimed) = b.claim_next("agents", "ws-1").map_err(|e| anyhow!(e))?;
        assert_eq!((before.node.as_str(), before.generation), ("node-a", 1));
        assert_eq!((claimed.node.as_str(), claimed.generation), ("node-b", 2));
        assert_eq!(
            a.write_own(&mine),
            Err(ManifestError::Superseded {
                generation: 2,
                node: "node-b".into()
            })
        );
        // The winner keeps writing; the old copy cannot delete its record.
        b.write_own(&claimed).map_err(|e| anyhow!(e))?;
        assert!(!a.delete_own("agents", "ws-1", 1).map_err(|e| anyhow!(e))?);
        assert!(b.read("agents", "ws-1").map_err(|e| anyhow!(e))?.is_some());
        assert!(b.delete_own("agents", "ws-1", 2).map_err(|e| anyhow!(e))?);
        assert_eq!(
            a.claim_next("agents", "ws-1").expect_err("gone"),
            ManifestError::NotFound
        );
        Ok(())
    }

    #[test]
    fn fence_markers_are_listed_per_node_and_cleared_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let a = manifests(dir.path(), "node-a");
        let b = manifests(dir.path(), "node-b");
        b.fence("node-a", "ws-1", 3).map_err(|e| anyhow!(e))?;
        b.fence("node-c", "ws-2", 1).map_err(|e| anyhow!(e))?;
        let markers = a.fences().map_err(|e| anyhow!(e))?;
        assert_eq!(markers.len(), 1);
        assert_eq!(
            (
                markers[0].session_id.as_str(),
                markers[0].generation,
                markers[0].moved_to.as_str()
            ),
            ("ws-1", 3, "node-b")
        );
        a.clear_fence("ws-1").map_err(|e| anyhow!(e))?;
        assert!(a.fences().map_err(|e| anyhow!(e))?.is_empty());
        assert!(a.last_success().is_some());
        Ok(())
    }

    /// The same conditional semantics through the object-store backend the
    /// S3 client uses, against `object_store`'s in-memory store (which
    /// honours `PutMode::Create` and `PutMode::Update` with ETags as S3
    /// does with `S3ConditionalPut::ETagMatch`).
    #[test]
    fn object_store_backend_honours_the_same_preconditions_test() -> R<()> {
        let memory: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend: Arc<dyn ManifestBackend> = Arc::new(ObjectStoreManifestBackend::new(
            StoreUrl::parse("s3://bucket/prefix")?,
            move || Ok(memory.clone()),
        ));
        let a = SessionManifests::new(backend.clone(), "node-a", "");
        let b = SessionManifests::new(backend.clone(), "node-b", "");
        a.create(&fresh(&a, "ws-1", 1), false)
            .map_err(|e| anyhow!(e))?;
        assert!(matches!(
            b.create(&fresh(&b, "ws-1", 1), false),
            Err(ManifestError::AlreadyExists { .. })
        ));
        let (_, claimed) = b.claim_next("agents", "ws-1").map_err(|e| anyhow!(e))?;
        assert_eq!(claimed.generation, 2);
        assert!(matches!(
            a.claim_next("agents", "ws-1"),
            Ok((_, ref c)) if c.generation == 3
        ));
        assert!(matches!(
            b.write_own(&claimed),
            Err(ManifestError::Superseded { generation: 3, .. })
        ));
        a.fence("node-b", "ws-1", 2).map_err(|e| anyhow!(e))?;
        let markers = b.fences().map_err(|e| anyhow!(e))?;
        assert_eq!(markers.len(), 1);
        assert_eq!(
            backend.url_for(&manifest_object("agents", "ws-1")),
            "s3://bucket/prefix/sessions/agents/1/ws-1.json"
        );
        Ok(())
    }

    #[test]
    fn a_node_fences_only_when_it_reached_neither_test() {
        use std::time::Duration;
        let after = Duration::from_secs(120);
        let s = Duration::from_secs;
        assert!(
            !self_fence_due(Some(s(5)), Some(s(500)), after),
            "managers reachable"
        );
        assert!(
            !self_fence_due(Some(s(500)), Some(s(5)), after),
            "bucket reachable"
        );
        assert!(self_fence_due(Some(s(121)), Some(s(130)), after));
        assert!(
            self_fence_due(Some(s(120)), None, after),
            "never reached the bucket"
        );
        assert!(!self_fence_due(Some(s(119)), None, after));
    }
}
