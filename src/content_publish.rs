//! Publishing node content to a shared store (suspend plan §6.8, amendment 1;
//! internal_docs/plans/checkpoint-encryption-plan.md;
//! internal_docs/plans/chunked-checkpoint-publish-plan.md).
//!
//! The artifact fetchers are read-only and stay that way. Publishing is the
//! one write path: a worker with a `contentStore` block in its config cuts a
//! digest from its node content store ([`crate::content_store`]) into
//! chunks, seals each one, and pushes the chunks it has not pushed before,
//! then a sealed index that lists them ([`crate::checkpoint_chunks`]):
//!
//! ```text
//! <contentStore.url>/sealed/<index or chunk name>
//! ```
//!
//! Every published object is sealed ([`crate::checkpoint_seal`]); there is
//! no plaintext mode. Chunk keys, and so chunk names, are keyed functions of
//! the chunk's plaintext digest, scoped to the job and the dedup window, so
//! publishing is idempotent and incremental: a chunk the store already
//! holds is not uploaded again, and a part already published in the window
//! uploads nothing. Every published digest's index URL is recorded in the
//! node content store's index, so a caller can name where each part lives.
//!
//! Reads never come through the publisher. Any worker restores published
//! content through [`crate::artifacts::ArtifactStager::fetch_sealed_into_store`],
//! which reads the index and the chunks from the store the URL names, opens
//! them, and verifies the reassembled part against its digest. Those reads
//! are signed with the reading node's own `contentStore` credential
//! ([`ContentStoreTarget::read_credential`]) when the URL is under its
//! `contentStore.url` ([`ContentStoreTarget::contains_url`]).
//!
//! # Example (what a snapshot publish does with a part)
//!
//! ```no_run
//! # use legion::content_publish::*;
//! # use legion::checkpoint_seal::{ChunkKey, PartKey};
//! # fn demo(publisher: &ContentPublishing, key: &PartKey, chunks: &ChunkKey) -> legion::prelude::R<()> {
//! // `key` and `chunks` came from the manager with the publish request.
//! let published = publisher.publish(key, chunks)?;
//! // Record `published.url` and `key.key_id()` in the snapshot manifest.
//! # let _ = published; Ok(()) }
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::prefix::PrefixStore;
use object_store::{path::Path as ObjectPath, ObjectStore};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::artifacts::{Credential, CredentialSource};
use crate::checkpoint_chunks::{
    ChunkSettings, KnownChunks, CHUNK_ALIGN, DEF_CHUNK_SIZE, DEF_DEDUP_WINDOW, MAX_CHUNK_SIZE,
    MIN_CHUNK_SIZE, MIN_DEDUP_WINDOW,
};
use crate::checkpoint_seal::{ChunkKey, PartKey, OBJECT_PREFIX};
use crate::content_store::{ContentDigest, ContentStore};
use crate::prelude::*;

/// What one publish did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedContent {
    pub digest: ContentDigest,
    /// Where any worker can fetch it, e.g. `s3://bucket/prefix/sealed/<name>`.
    pub url: String,
    /// The checkpoint key the object is sealed under (`ck_…`).
    pub key_id: String,
    /// The part's plaintext size.
    pub bytes: u64,
    /// Plaintext bytes of the chunks this publish uploaded: 0 when the
    /// store already held the part, and only the changed chunks when it
    /// held an earlier checkpoint of the same guest.
    pub uploaded_bytes: u64,
}

/// The bytes being published do not hash to the digest they were published
/// as: the local blob is corrupt, and nothing was published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishSourceMismatch {
    pub expected: ContentDigest,
    pub actual: ContentDigest,
}

impl std::fmt::Display for PublishSourceMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "refusing to publish {}: the source bytes hash to {}",
            self.expected, self.actual
        )
    }
}

impl std::error::Error for PublishSourceMismatch {}

/// A destination that accepts sealed objects.
pub trait ContentPublisher: Send + Sync {
    /// The URL a sealed object named `object_name` is (or would be)
    /// published at. Pure: no I/O.
    fn url_for(&self, object_name: &str) -> String;

    /// The store's sealed objects (`<url>/sealed/`), as an object store
    /// rooted there. Chunks and part indexes are written and read through
    /// it ([`crate::checkpoint_chunks`]).
    fn sealed_objects(&self) -> R<Arc<dyn ObjectStore>>;

    /// Write a small document (an archive manifest) at `object_name`,
    /// relative to the store's root rather than under the sealed-object
    /// prefix, replacing any earlier one, and return its URL. Readers never
    /// see a partial document. Blocking: call from a blocking thread.
    fn put_document(&self, object_name: &str, bytes: &[u8]) -> R<String>;
}

/// Worker config `contentStore.auth`. Absent means the node's ambient
/// credentials (for S3: the standard AWS environment variables, then the
/// instance profile), which is how a region's workers usually reach their own
/// region's bucket.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContentStoreAuth {
    /// KV path holding an AWS credential object, the same shape artifact
    /// `auth.kv` reads (docs/artifacts.md, "Credential shapes").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv: Option<String>,
    /// Accepted so the plan's surface parses, then refused with a hint: an
    /// S3 upload needs an AWS credential, and exchanging a node identity
    /// token for one (STS) is not wired yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_identity: Option<serde_json::Value>,
}

/// Worker config `contentStore` block (docs/artifacts.md, "Publishing
/// content").
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContentStoreTarget {
    /// `s3://bucket/optional/prefix` or `file:///shared/dir`.
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<ContentStoreAuth>,
    /// The size published parts are cut into (`"1MiB"` when unset): each
    /// distinct non-zero chunk is one object, so a repeat checkpoint
    /// uploads only the chunks that changed (docs/suspend-and-resume.md,
    /// "How published parts are stored"). 64 KiB to 16 MiB, a multiple of
    /// 4 KiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_size: Option<String>,
    /// How long chunks are shared between publishes (`"24h"` when unset).
    /// A chunk is reused only by publishes in the window it was uploaded
    /// in, so a bucket lifecycle rule that expires `sealed/` objects older
    /// than the longest-lived checkpoint plus this window never removes a
    /// chunk a checkpoint still needs. At least 1m.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedup_window: Option<String>,
}

impl ContentStoreTarget {
    /// `chunkSize` and `dedupWindow`, parsed and checked.
    pub fn chunk_settings(&self) -> R<ChunkSettings> {
        let chunk_size = match self.chunk_size.as_deref() {
            None => DEF_CHUNK_SIZE,
            Some(raw) => {
                let bytes = crate::structs::config::parse_byte_size(raw)
                    .with_context(|| format!("contentStore.chunkSize {raw:?}"))?;
                if !(u64::from(MIN_CHUNK_SIZE)..=u64::from(MAX_CHUNK_SIZE)).contains(&bytes)
                    || bytes % u64::from(CHUNK_ALIGN) != 0
                {
                    bail!(
                        "contentStore.chunkSize {raw:?} must be 64KiB to 16MiB and a multiple of \
                         4KiB (the default is 1MiB)"
                    );
                }
                bytes as u32
            }
        };
        let dedup_window = match self.dedup_window.as_deref() {
            None => DEF_DEDUP_WINDOW,
            Some(raw) => {
                let window = humantime::parse_duration(raw.trim()).with_context(|| {
                    format!("contentStore.dedupWindow {raw:?} is not a duration")
                })?;
                if window < MIN_DEDUP_WINDOW {
                    bail!("contentStore.dedupWindow must be at least 1m, got {raw:?}");
                }
                window
            }
        };
        Ok(ChunkSettings {
            chunk_size,
            dedup_window,
        })
    }

    /// Admission-time validation, run when the manager loads the worker
    /// config so a typo fails at startup instead of at the first publish.
    pub fn validate(&self) -> R<Url> {
        let url = Url::parse(self.url.trim())
            .with_context(|| format!("invalid contentStore.url {:?}", self.url))?;
        match url.scheme() {
            "s3" => {
                if url.host_str().unwrap_or_default().is_empty() {
                    bail!("contentStore.url {:?} has no bucket", self.url);
                }
            }
            "file" => {
                if url.to_file_path().is_err() {
                    bail!(
                        "contentStore.url {:?} is not an absolute file path",
                        self.url
                    );
                }
            }
            other => {
                bail!("contentStore.url scheme {other:?} is not supported; use s3:// or file://")
            }
        }
        if url.query().is_some_and(|q| !q.is_empty()) && url.scheme() == "file" {
            bail!("contentStore.url {:?}: file URLs take no query", self.url);
        }
        self.chunk_settings()?;
        if let Some(auth) = &self.auth {
            if auth.node_identity.is_some() {
                bail!(
                    "contentStore.auth.nodeIdentity is not supported yet: S3 uploads need an AWS \
                     credential, and node identity tokens are not exchanged for one. Use \
                     contentStore.auth.kv, or omit auth to use the node's ambient credentials \
                     (AWS environment variables or the instance profile)"
                );
            }
            match auth.kv.as_deref() {
                Some(path) if path.trim().is_empty() => {
                    bail!("contentStore.auth.kv cannot be empty")
                }
                Some(_) if url.scheme() == "file" => {
                    bail!("contentStore.auth applies to s3:// stores only")
                }
                _ => {}
            }
        }
        Ok(url)
    }

    /// Build the publisher. `credentials` resolves `auth.kv` at publish time.
    pub fn publisher(
        &self,
        credentials: Arc<dyn CredentialSource>,
    ) -> R<Arc<dyn ContentPublisher>> {
        let url = self.validate()?;
        Ok(match url.scheme() {
            "file" => Arc::new(FileContentPublisher::new(
                url.to_file_path().expect("validated"),
            )),
            _ => Arc::new(S3ContentPublisher {
                url,
                kv: self.auth.as_ref().and_then(|a| a.kv.clone()),
                credentials,
            }),
        })
    }

    /// Whether `url` names an object in this store: the same scheme, bucket
    /// (or directory root), and `endpoint`, under the store's prefix. A
    /// restoring node fetches such a URL with [`Self::read_credential`]
    /// rather than anonymously. A sibling prefix (`content-old/` beside
    /// `content/`) or another endpoint serving the same bucket name is not
    /// this store, so the store's credential is never sent there.
    pub fn contains_url(&self, url: &str) -> bool {
        let (Ok(store), Ok(url)) = (self.validate(), Url::parse(url.trim())) else {
            return false;
        };
        let endpoint = |url: &Url| {
            url.query_pairs()
                .find(|(name, _)| name == "endpoint")
                .map(|(_, value)| value.into_owned())
        };
        let prefix = store.path().trim_end_matches('/');
        url.scheme() == store.scheme()
            && url.host_str() == store.host_str()
            && endpoint(&url) == endpoint(&store)
            && url
                .path()
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('/'))
    }

    /// The credential an object in this store is read with: the AWS
    /// credential at `auth.kv` (resolved now, so a rotated credential is
    /// picked up as the publisher picks it up), the node's ambient chain
    /// when there is no `auth`, and none for a `file://` store.
    pub fn read_credential(
        &self,
        credentials: &dyn CredentialSource,
        redacted_source: &str,
    ) -> R<Option<Credential>> {
        if self.validate()?.scheme() == "file" {
            return Ok(None);
        }
        match self.auth.as_ref().and_then(|auth| auth.kv.as_deref()) {
            Some(path) => credentials
                .kv_credential(path, redacted_source)
                .with_context(|| format!("contentStore credentials from kv {path}"))
                .map(Some),
            None => Ok(Some(Credential::AwsAmbient)),
        }
    }
}

/// Publishes into a directory (a shared mount, or a test's temp dir).
#[derive(Debug, Clone)]
pub struct FileContentPublisher {
    root: PathBuf,
}

impl FileContentPublisher {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_for(&self, object_name: &str) -> PathBuf {
        self.root.join(OBJECT_PREFIX).join(object_name)
    }
}

/// Write `bytes` to `dest` atomically: beside it, then renamed over it.
fn write_file_atomically(dest: &Path, bytes: &[u8]) -> R<()> {
    let dir = dest.parent().context("document path has no parent")?;
    fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let name = dest
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("document");
    let tmp = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    fs::write(&tmp, bytes).with_context(|| format!("write {}", tmp.display()))?;
    if let Err(err) = fs::rename(&tmp, dest) {
        let _ = fs::remove_file(&tmp);
        return Err(anyhow!(err).context(format!("publish to {}", dest.display())));
    }
    Ok(())
}

impl ContentPublisher for FileContentPublisher {
    fn url_for(&self, object_name: &str) -> String {
        let path = self.path_for(object_name);
        Url::from_file_path(&path)
            .map(|url| url.to_string())
            .unwrap_or_else(|_| format!("file://{}", path.display()))
    }

    fn sealed_objects(&self) -> R<Arc<dyn ObjectStore>> {
        let dir = self.root.join(OBJECT_PREFIX);
        fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        // object_store writes beside the destination and renames, so a
        // reader never sees a partial object under its final name.
        Ok(Arc::new(
            LocalFileSystem::new_with_prefix(&dir)
                .with_context(|| format!("open {} as a content store", dir.display()))?,
        ))
    }

    fn put_document(&self, object_name: &str, bytes: &[u8]) -> R<String> {
        let dest = self.root.join(object_name);
        write_file_atomically(&dest, bytes)?;
        Ok(Url::from_file_path(&dest)
            .map(|url| url.to_string())
            .unwrap_or_else(|_| format!("file://{}", dest.display())))
    }
}

/// Publishes to an S3-compatible bucket. The URL takes the same `region` and
/// `endpoint` query parameters the `s3://` fetcher reads, and they are kept on
/// the published URL so readers reach the same endpoint.
pub struct S3ContentPublisher {
    url: Url,
    kv: Option<String>,
    credentials: Arc<dyn CredentialSource>,
}

impl std::fmt::Debug for S3ContentPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ContentPublisher")
            .field("url", &self.url.as_str())
            .field("kv", &self.kv)
            .finish_non_exhaustive()
    }
}

impl S3ContentPublisher {
    fn key_for(&self, object_name: &str) -> String {
        let prefix = self.url.path().trim_matches('/');
        if prefix.is_empty() {
            format!("{OBJECT_PREFIX}/{object_name}")
        } else {
            format!("{prefix}/{OBJECT_PREFIX}/{object_name}")
        }
    }

    fn client(&self) -> R<object_store::aws::AmazonS3> {
        let credential = match self.kv.as_deref() {
            Some(path) => Some(
                self.credentials
                    .kv_credential(path, self.url.as_str())
                    .with_context(|| format!("contentStore credentials from kv {path}"))?,
            ),
            None => None,
        };
        s3_client(&self.url, credential)
    }
}

/// An S3 client for the bucket an `s3://bucket/prefix?region=…&endpoint=…`
/// URL names. The URL's `region` and `endpoint` win over the credential's;
/// with no credential the client uses the ambient chain (the AWS_*
/// variables, web identity, then the instance profile). Shared by the
/// worker's publisher and the manager's archive reads.
pub fn s3_client(url: &Url, credential: Option<Credential>) -> R<object_store::aws::AmazonS3> {
    s3_builder(url, credential)?
        .build()
        .context("build s3 client for contentStore")
}

/// [`s3_client`] with conditional writes on: `PutMode::Create` sends
/// `If-None-Match: *` and `PutMode::Update` sends `If-Match`, as the
/// session manifests' generations need ([`crate::session_manifest`]).
pub fn s3_client_conditional(
    url: &Url,
    credential: Option<Credential>,
) -> R<object_store::aws::AmazonS3> {
    s3_builder(url, credential)?
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .build()
        .context("build s3 client for contentStore")
}

fn s3_builder(url: &Url, credential: Option<Credential>) -> R<AmazonS3Builder> {
    let bucket = url.host_str().context("contentStore.url has no bucket")?;
    let mut region: Option<String> = None;
    let mut endpoint: Option<String> = None;
    if let Some(Credential::Aws {
        region: r,
        endpoint: e,
        ..
    }) = &credential
    {
        region = r.clone();
        endpoint = e.clone();
    }
    for (name, value) in url.query_pairs() {
        match name.as_ref() {
            "region" => region = Some(value.into_owned()),
            "endpoint" => endpoint = Some(value.into_owned()),
            _ => {}
        }
    }
    // `from_env` is the ambient chain: AWS_* variables, then the instance
    // profile, when no explicit keys are set below.
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    if let Some(region) = region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = endpoint {
        let allow_http = endpoint.starts_with("http://");
        builder = builder.with_endpoint(endpoint).with_allow_http(allow_http);
    }
    match credential {
        Some(Credential::Aws {
            access_key_id,
            secret_access_key,
            session_token,
            ..
        }) => {
            builder = builder
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key);
            if let Some(token) = session_token {
                builder = builder.with_token(token);
            }
        }
        Some(other) => bail!("contentStore needs an aws credential, got {other:?}"),
        None => {}
    }
    Ok(builder)
}

/// The object key of `object_name` under an `s3://bucket/prefix` URL's
/// prefix.
pub fn s3_key(url: &Url, object_name: &str) -> String {
    let prefix = url.path().trim_matches('/');
    if prefix.is_empty() {
        object_name.to_string()
    } else {
        format!("{prefix}/{object_name}")
    }
}

/// The URL of `object_name` under a `contentStore.url` root, keeping an
/// `s3://` root's query (`region`, `endpoint`) so readers reach the same
/// endpoint.
pub fn document_url(root: &Url, object_name: &str) -> String {
    match root.scheme() {
        "file" => {
            let path = root
                .to_file_path()
                .map(|path| path.join(object_name))
                .unwrap_or_else(|_| PathBuf::from(object_name));
            Url::from_file_path(&path)
                .map(|url| url.to_string())
                .unwrap_or_else(|_| format!("file://{}", path.display()))
        }
        _ => {
            let mut url = root.clone();
            url.set_path(&format!("/{}", s3_key(root, object_name)));
            url.to_string()
        }
    }
}

/// The sealed-object directory a published object's URL is in, as an object
/// store rooted there, and the object's name in it: what a reading node
/// fetches a part's index and chunks through
/// ([`crate::checkpoint_chunks::fetch_part`]). `credential` signs `s3://`
/// reads: the node's own `contentStore` credential
/// ([`ContentStoreTarget::read_credential`], `AwsAmbient` for the ambient
/// chain), or `None` to read anonymously.
pub fn sealed_objects_for_url(
    url: &str,
    credential: Option<Credential>,
) -> R<(Arc<dyn ObjectStore>, String)> {
    let parsed = Url::parse(url.trim()).with_context(|| format!("invalid part URL {url:?}"))?;
    match parsed.scheme() {
        "file" => {
            let path = parsed
                .to_file_path()
                .map_err(|_| anyhow!("part URL {url:?} is not an absolute file path"))?;
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .with_context(|| format!("part URL {url:?} names no object"))?
                .to_string();
            let dir = path.parent().context("part URL has no directory")?;
            let objects = LocalFileSystem::new_with_prefix(dir)
                .with_context(|| format!("open {} as a content store", dir.display()))?;
            Ok((Arc::new(objects), name))
        }
        "s3" => {
            let key = parsed.path().trim_start_matches('/');
            let (dir, name) = key
                .rsplit_once('/')
                .with_context(|| format!("part URL {url:?} names no object under a prefix"))?;
            let client = match credential {
                None => s3_builder(&parsed, None)?.with_skip_signature(true),
                Some(Credential::AwsAmbient) => s3_builder(&parsed, None)?,
                Some(credential) => s3_builder(&parsed, Some(credential))?,
            }
            .build()
            .context("build s3 client for contentStore")?;
            Ok((Arc::new(PrefixStore::new(client, dir)), name.to_string()))
        }
        other => bail!(
            "part URL scheme {other:?} is not supported; parts live in s3:// or file:// stores"
        ),
    }
}

impl ContentPublisher for S3ContentPublisher {
    fn url_for(&self, object_name: &str) -> String {
        let mut url = self.url.clone();
        url.set_path(&format!("/{}", self.key_for(object_name)));
        url.to_string()
    }

    fn sealed_objects(&self) -> R<Arc<dyn ObjectStore>> {
        let prefix = self.key_for("");
        Ok(Arc::new(PrefixStore::new(
            self.client()?,
            prefix.trim_end_matches('/'),
        )))
    }

    fn put_document(&self, object_name: &str, bytes: &[u8]) -> R<String> {
        let client = self.client()?;
        let object = ObjectPath::from(s3_key(&self.url, object_name));
        let url = document_url(&self.url, object_name);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build content publish runtime")?;
        // A single PUT replaces the object atomically: readers see the old
        // document or the new one, never a mix.
        runtime
            .block_on(client.put(&object, bytes.to_vec().into()))
            .with_context(|| format!("write {url}"))?;
        Ok(url)
    }
}

/// The node-side publishing service: the node content store plus the
/// configured publisher. This is the seam Phase 7b uses to publish snapshot
/// parts (`durability = "store"`).
pub struct ContentPublishing {
    store: Arc<ContentStore>,
    publisher: Arc<dyn ContentPublisher>,
    settings: ChunkSettings,
    /// Chunk names this node has uploaded or found in the store during the
    /// current dedup window, shared by every publish.
    known: KnownChunks,
}

impl std::fmt::Debug for ContentPublishing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContentPublishing")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl ContentPublishing {
    /// Publishing with the default chunk size and dedup window.
    pub fn new(store: Arc<ContentStore>, publisher: Arc<dyn ContentPublisher>) -> Self {
        Self::with_settings(store, publisher, ChunkSettings::default())
    }

    pub fn with_settings(
        store: Arc<ContentStore>,
        publisher: Arc<dyn ContentPublisher>,
        settings: ChunkSettings,
    ) -> Self {
        Self {
            store,
            publisher,
            settings,
            known: KnownChunks::default(),
        }
    }

    /// Write a document beside the published content (see
    /// [`ContentPublisher::put_document`]).
    pub fn put_document(&self, object_name: &str, bytes: &[u8]) -> R<String> {
        self.publisher.put_document(object_name, bytes)
    }

    pub fn store(&self) -> &Arc<ContentStore> {
        &self.store
    }

    pub fn settings(&self) -> ChunkSettings {
        self.settings
    }

    /// Publish the part `key` names, which the node content store must
    /// hold, as sealed chunks keyed from `chunks` (the job's chunk key) in
    /// the current dedup window, and record its URL (its index's). Chunks
    /// the store already holds in this window are not uploaded again, zero
    /// chunks are never uploaded, and a part already recorded as published
    /// in this window, or whose index is already in the store, uploads
    /// nothing. The bytes are hashed as they are read, so a corrupt local
    /// blob is never published: it is dropped from the store instead.
    #[instrument(skip(self, key, chunks), fields(digest = %key.digest(), key_id = %key.key_id()))]
    pub fn publish(&self, key: &PartKey, chunks: &ChunkKey) -> R<PublishedContent> {
        let digest = key.digest();
        let entry = self
            .store
            .get(digest)?
            .with_context(|| format!("content {digest} is not in the node store"))?;
        let (window_secs, window) = self.settings.window_now();
        let scope = chunks.scope(window_secs, window)?;
        let index_name = key.index_object_name(&scope, self.settings.chunk_size);
        let url = self.publisher.url_for(&index_name);
        let published = |uploaded_bytes| PublishedContent {
            digest: digest.clone(),
            url: url.clone(),
            key_id: key.key_id().to_string(),
            bytes: entry.size,
            uploaded_bytes,
        };
        if self.store.published_url(digest)?.as_deref() == Some(url.as_str()) {
            return Ok(published(0));
        }
        let started = std::time::Instant::now();
        let outcome = match crate::checkpoint_chunks::publish_part(
            self.publisher.sealed_objects()?,
            &entry.path,
            key,
            &scope,
            &index_name,
            self.settings,
            &self.known,
        ) {
            Ok(outcome) => outcome,
            Err(err) if err.downcast_ref::<PublishSourceMismatch>().is_some() => {
                // Rehash to drop the corrupt blob from the store, so the
                // next capture or fetch replaces it.
                return Err(match self.store.verify(digest) {
                    Err(verify_err) => verify_err,
                    Ok(_) => err,
                });
            }
            Err(err) => return Err(err),
        };
        self.store.record_published(digest, &url)?;
        info!(
            url = %url,
            bytes = entry.size,
            uploaded_bytes = outcome.uploaded_bytes,
            uploaded_chunks = outcome.uploaded_chunks,
            reused_chunks = outcome.reused_chunks,
            zero_chunks = outcome.zero_chunks,
            already_published = outcome.already_published,
            scope = scope.label(),
            duration_ms = started.elapsed().as_millis() as u64,
            "Published sealed content"
        );
        Ok(published(outcome.uploaded_bytes))
    }

    /// Where a digest was published from this node, if it was.
    pub fn published_url(&self, digest: &ContentDigest) -> R<Option<String>> {
        self.store.published_url(digest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::AnonymousCredentials;
    use crate::content_store::{ContentClass, ContentStoreConfig, PutOptions};

    fn target(raw: serde_json::Value) -> R<ContentStoreTarget> {
        Ok(serde_json::from_value(raw)?)
    }

    fn part_key(digest: &ContentDigest) -> PartKey {
        PartKey::derive(&[5u8; 32], "ck_test", digest).unwrap()
    }

    #[test]
    fn contains_url_matches_only_this_store() -> R<()> {
        let store = target(serde_json::json!({ "url": "s3://bucket/content/" }))?;
        assert!(store.contains_url("s3://bucket/content/sealed/abc"));
        assert!(!store.contains_url("s3://bucket/content-old/sealed/abc"));
        assert!(!store.contains_url("s3://bucket/sealed/abc"));
        assert!(!store.contains_url("s3://other/content/sealed/abc"));
        assert!(!store.contains_url("file:///content/sealed/abc"));

        // The whole bucket, and the endpoint is part of the store's identity.
        let minio = target(serde_json::json!({
            "url": "s3://bucket?endpoint=http://minio:9000"
        }))?;
        assert!(minio.contains_url("s3://bucket/sealed/abc?endpoint=http://minio:9000"));
        assert!(!minio.contains_url("s3://bucket/sealed/abc"));

        let shared = target(serde_json::json!({ "url": "file:///mnt/store" }))?;
        assert!(shared.contains_url("file:///mnt/store/sealed/abc"));
        assert!(!shared.contains_url("file:///mnt/storage/sealed/abc"));
        Ok(())
    }

    #[test]
    fn read_credential_follows_the_store_auth() -> R<()> {
        let ambient = target(serde_json::json!({ "url": "s3://bucket/content" }))?;
        assert_eq!(
            ambient.read_credential(&AnonymousCredentials, "s3://bucket/content/sealed/x")?,
            Some(Credential::AwsAmbient)
        );
        let shared = target(serde_json::json!({ "url": "file:///mnt/store" }))?;
        assert_eq!(shared.read_credential(&AnonymousCredentials, "x")?, None);
        // An unreadable KV credential names the contentStore setting.
        let kv = target(serde_json::json!({
            "url": "s3://bucket/content",
            "auth": { "kv": "secrets/content-store" }
        }))?;
        let err = kv.read_credential(&AnonymousCredentials, "x").unwrap_err();
        assert!(
            format!("{err:#}").contains("contentStore credentials from kv secrets/content-store"),
            "{err:#}"
        );
        Ok(())
    }

    fn chunk_key() -> ChunkKey {
        ChunkKey::derive(&[5u8; 32], "ck_test", "job").unwrap()
    }

    fn open_store(dir: &Path) -> R<Arc<ContentStore>> {
        Ok(Arc::new(ContentStore::open(
            dir.join("content"),
            ContentStoreConfig {
                min_free_disk: 0,
                ..Default::default()
            },
        )?))
    }

    #[test]
    fn file_publisher_writes_sealed_chunks_and_an_index_and_is_idempotent() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = open_store(dir.path())?;
        let bytes = b"snapshot part bytes with a SECRET_TOKEN=abc123 ".repeat(50_000);
        let entry = store.put_bytes(ContentClass::Snapshots, &bytes, PutOptions::default())?;
        let key = part_key(&entry.digest);

        let shared = dir.path().join("shared");
        let target = target(serde_json::json!({
            "url": Url::from_directory_path(&shared).unwrap().to_string(),
        }))?;
        let publishing = ContentPublishing::with_settings(
            store.clone(),
            target.publisher(Arc::new(AnonymousCredentials))?,
            target.chunk_settings()?,
        );

        let first = publishing.publish(&key, &chunk_key())?;
        assert_eq!(first.uploaded_bytes, bytes.len() as u64);
        assert_eq!(first.bytes, bytes.len() as u64);
        assert_eq!(first.key_id, "ck_test");
        // The URL names the index, beside the chunks, and nothing in the
        // store holds plaintext or is named by a plaintext digest.
        let index = Url::parse(&first.url)?.to_file_path().unwrap();
        assert_eq!(index.parent(), Some(shared.join("sealed").as_path()));
        let objects: Vec<_> = fs::read_dir(shared.join("sealed"))?.collect::<Result<_, _>>()?;
        assert_eq!(
            objects.len(),
            bytes
                .len()
                .div_ceil(crate::checkpoint_chunks::DEF_CHUNK_SIZE as usize)
                + 1,
            "one object per chunk, and the index"
        );
        for object in &objects {
            let sealed = fs::read(object.path())?;
            assert_eq!(&sealed[..8], crate::checkpoint_seal::SEAL_MAGIC);
            assert!(
                !sealed.windows(19).any(|w| w == b"SECRET_TOKEN=abc123"),
                "the published object holds no plaintext"
            );
        }
        assert!(
            !shared.join("sha256").exists(),
            "nothing is published under the plaintext digest"
        );
        assert_eq!(
            publishing.published_url(&entry.digest)?,
            Some(first.url.clone())
        );

        // Second publish: recorded, so nothing moves.
        let second = publishing.publish(&key, &chunk_key())?;
        assert_eq!(second.uploaded_bytes, 0);
        assert_eq!(second.url, first.url);

        // A fresh node (no record) publishing the same bytes finds the
        // index already in the store: same key, same window, same objects.
        let other_dir = tempfile::tempdir()?;
        let other_store = open_store(other_dir.path())?;
        other_store.put_bytes(ContentClass::Snapshots, &bytes, PutOptions::default())?;
        let other =
            ContentPublishing::new(other_store, Arc::new(FileContentPublisher::new(&shared)));
        let third = other.publish(&key, &chunk_key())?;
        assert_eq!(third.uploaded_bytes, 0);
        assert_eq!(third.url, first.url);
        assert_eq!(fs::read_dir(shared.join("sealed"))?.count(), objects.len());

        // Any node reads it back to the exact bytes through the URL.
        let (objects, name) = sealed_objects_for_url(&first.url, None)?;
        let fetched = dir.path().join("fetched");
        crate::checkpoint_chunks::fetch_part(
            objects,
            &name,
            &key,
            &fetched,
            crate::checkpoint_chunks::FetchLimits {
                max_bytes: None,
                deadline: None,
                attempts: 1,
            },
        )?;
        assert_eq!(fs::read(&fetched)?, bytes);
        Ok(())
    }

    #[test]
    fn publishing_refuses_bytes_that_do_not_match_the_digest() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = open_store(dir.path())?;
        let entry = store.put_bytes(
            ContentClass::Snapshots,
            b"actual bytes",
            PutOptions::default(),
        )?;
        // Corrupt the blob behind the store's back.
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&entry.path, fs::Permissions::from_mode(0o644))?;
        fs::write(&entry.path, b"rotted bytes")?;
        let key = part_key(&entry.digest);
        let shared = dir.path().join("shared");
        let publishing =
            ContentPublishing::new(store.clone(), Arc::new(FileContentPublisher::new(&shared)));
        let err = publishing.publish(&key, &chunk_key()).unwrap_err();
        assert!(format!("{err:#}").contains("corrupt"), "{err:#}");
        // No index was written, so nothing names the bad bytes, and the
        // corrupt blob left the store.
        let names: Vec<_> = fs::read_dir(shared.join("sealed"))?
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert!(names.len() <= 1, "at most the one chunk: {names:?}");
        assert!(store.get(&entry.digest)?.is_none());
        Ok(())
    }

    /// Publish and fetch through a real S3-compatible store: a first
    /// publish, a repeat with a few chunks changed, a fetch on a "node" with
    /// nothing, and an anonymous read the private bucket refuses. Run
    /// against a MinIO with `LEGION_TEST_S3_URL` (for example
    /// `s3://legion-test?endpoint=http://127.0.0.1:9000&region=us-east-1`)
    /// and the bucket's keys in `AWS_ACCESS_KEY_ID` and
    /// `AWS_SECRET_ACCESS_KEY`: `cargo test --lib
    /// s3_publish_and_fetch_round_trip -- --ignored --nocapture`.
    /// `LEGION_TEST_S3_MIB` sets the part size (default 64).
    #[test]
    #[ignore]
    fn s3_publish_and_fetch_round_trip() -> R<()> {
        let url = std::env::var("LEGION_TEST_S3_URL").context("set LEGION_TEST_S3_URL")?;
        let mib: usize = std::env::var("LEGION_TEST_S3_MIB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64);
        let target = target(serde_json::json!({ "url": url }))?;
        let chunk = crate::checkpoint_chunks::DEF_CHUNK_SIZE as usize;
        // A quarter zero, the rest distinct per chunk.
        let mut bytes = vec![0u8; mib * chunk];
        for (i, piece) in bytes.chunks_mut(chunk).enumerate().skip(mib / 4) {
            for (j, byte) in piece.iter_mut().enumerate() {
                *byte = (i.wrapping_mul(2654435761) ^ j.wrapping_mul(40503) ^ (j >> 9)) as u8 | 1;
            }
        }
        let nonzero = (mib - mib / 4) * chunk;
        let publish = |bytes: &[u8]| -> R<(PublishedContent, PartKey, f64)> {
            let dir = tempfile::tempdir()?;
            let store = open_store(dir.path())?;
            let entry = store.put_bytes(ContentClass::Snapshots, bytes, PutOptions::default())?;
            let key = part_key(&entry.digest);
            let publishing =
                ContentPublishing::new(store, target.publisher(Arc::new(AnonymousCredentials))?);
            let started = std::time::Instant::now();
            let published = publishing.publish(&key, &chunk_key())?;
            Ok((published, key, started.elapsed().as_secs_f64()))
        };
        let (first, key, first_secs) = publish(&bytes)?;
        // A fresh store prefix holds none of it; a rerun of the test finds
        // the index already there and uploads nothing.
        assert!(
            first.uploaded_bytes == nonzero as u64 || first.uploaded_bytes == 0,
            "{}",
            first.uploaded_bytes
        );
        let mut changed = bytes.clone();
        for i in [mib / 2, mib / 2 + 1, mib - 1] {
            changed[i * chunk + 7] ^= 0x55;
        }
        let (second, second_key, second_secs) = publish(&changed)?;
        assert!(
            second.uploaded_bytes <= 3 * chunk as u64,
            "{}",
            second.uploaded_bytes
        );

        let fetch_dir = tempfile::tempdir()?;
        let fetched_path = fetch_dir.path().join("part");
        let started = std::time::Instant::now();
        let (objects, name) = sealed_objects_for_url(&second.url, Some(Credential::AwsAmbient))?;
        let fetched = crate::checkpoint_chunks::fetch_part(
            objects,
            &name,
            &second_key,
            &fetched_path,
            crate::checkpoint_chunks::FetchLimits {
                max_bytes: None,
                deadline: None,
                attempts: 3,
            },
        )?;
        let fetch_secs = started.elapsed().as_secs_f64();
        assert!(fs::read(&fetched_path)? == changed);
        assert_eq!(fetched.fetched_bytes, nonzero as u64);

        let (anonymous, name) = sealed_objects_for_url(&first.url, None)?;
        let err = crate::checkpoint_chunks::fetch_part(
            anonymous,
            &name,
            &key,
            &fetch_dir.path().join("anonymous"),
            crate::checkpoint_chunks::FetchLimits {
                max_bytes: None,
                deadline: None,
                attempts: 1,
            },
        )
        .unwrap_err();
        println!("anonymous read refused: {err:#}");
        println!(
            "s3_publish_and_fetch_round_trip: {mib} MiB part ({} MiB non-zero): first publish \
             uploaded {:.1} MiB in {first_secs:.2}s, repeat with 3 chunks changed uploaded \
             {:.1} MiB in {second_secs:.2}s, fetch of {:.1} MiB in {fetch_secs:.2}s",
            nonzero >> 20,
            first.uploaded_bytes as f64 / (1 << 20) as f64,
            second.uploaded_bytes as f64 / (1 << 20) as f64,
            fetched.fetched_bytes as f64 / (1 << 20) as f64,
        );
        Ok(())
    }

    #[test]
    fn chunk_settings_default_and_validate() -> R<()> {
        let plain = target(serde_json::json!({ "url": "s3://b" }))?;
        assert_eq!(plain.chunk_settings()?, ChunkSettings::default());
        let tuned = target(serde_json::json!({
            "url": "s3://b", "chunkSize": "4MiB", "dedupWindow": "6h",
        }))?;
        let settings = tuned.chunk_settings()?;
        assert_eq!(settings.chunk_size, 4 * 1024 * 1024);
        assert_eq!(settings.dedup_window.as_secs(), 6 * 3600);
        for (raw, needle) in [
            (
                serde_json::json!({ "url": "s3://b", "chunkSize": "32KiB" }),
                "64KiB to 16MiB",
            ),
            (
                serde_json::json!({ "url": "s3://b", "chunkSize": "1000000" }),
                "multiple of",
            ),
            (
                serde_json::json!({ "url": "s3://b", "chunkSize": "big" }),
                "chunkSize",
            ),
            (
                serde_json::json!({ "url": "s3://b", "dedupWindow": "10s" }),
                "at least 1m",
            ),
            (
                serde_json::json!({ "url": "s3://b", "dedupWindow": "soon" }),
                "not a duration",
            ),
        ] {
            let err = target(raw)?.validate().unwrap_err();
            assert!(format!("{err:#}").contains(needle), "{err:#}");
        }
        Ok(())
    }

    #[test]
    fn s3_urls_keep_the_prefix_and_endpoint() -> R<()> {
        let target = target(serde_json::json!({
            "url": "s3://legion-content/usw2?endpoint=http://minio:9000&region=auto",
            "auth": { "kv": "legion/content" },
        }))?;
        let publisher = target.publisher(Arc::new(AnonymousCredentials))?;
        let key = part_key(&ContentDigest::of_bytes(b"x"));
        assert_eq!(
            publisher.url_for(&key.object_name()),
            format!(
                "s3://legion-content/usw2/sealed/{}?endpoint=http://minio:9000&region=auto",
                key.object_name()
            )
        );
        Ok(())
    }

    /// Publish throughput on a large synthetic part through the `file://`
    /// publisher (the checkpoint-encryption plan's §9 measurement). Run with
    /// `cargo test --release --lib publish_throughput -- --ignored
    /// --nocapture`; `LEGION_PUBLISH_BENCH_MIB` sets the size (default 2048).
    #[test]
    #[ignore]
    fn publish_throughput() -> R<()> {
        let mib: usize = std::env::var("LEGION_PUBLISH_BENCH_MIB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2048);
        let dir = tempfile::tempdir()?;
        let store = Arc::new(ContentStore::open(
            dir.path().join("content"),
            ContentStoreConfig {
                min_free_disk: 0,
                ..Default::default()
            },
        )?);
        let src = dir.path().join("synthetic");
        {
            use std::io::Write;
            let mut out = std::io::BufWriter::new(fs::File::create(&src)?);
            let mut block = vec![0u8; 1024 * 1024];
            for i in 0..mib {
                for (j, byte) in block.iter_mut().enumerate() {
                    *byte = (i.wrapping_mul(131) ^ j.wrapping_mul(7)) as u8;
                }
                out.write_all(&block)?;
            }
        }
        let entry = store.import_file(ContentClass::Snapshots, &src, PutOptions::default())?;
        let key = part_key(&entry.digest);
        let mut runs = Vec::new();
        for run in 0..3 {
            let shared = dir.path().join(format!("shared-{run}"));
            // The whole publish, as a worker runs it: a fresh destination
            // each run, so every run seals and writes the full part.
            let publishing =
                ContentPublishing::new(store.clone(), Arc::new(FileContentPublisher::new(&shared)));
            let started = std::time::Instant::now();
            let published = publishing.publish(&key, &chunk_key())?;
            let secs = started.elapsed().as_secs_f64();
            assert_eq!(published.uploaded_bytes, published.bytes);
            runs.push(secs);
            fs::remove_dir_all(&shared)?;
        }
        runs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = runs[1];
        println!(
            "publish_throughput: {mib} MiB sealed in {median:.3}s median of {runs:?} = {:.0} MiB/s",
            mib as f64 / median
        );
        Ok(())
    }

    #[test]
    fn target_validation_names_the_problem() {
        for (raw, needle) in [
            (serde_json::json!({ "url": "https://x/y" }), "not supported"),
            (serde_json::json!({ "url": "s3:///key" }), "no bucket"),
            (
                serde_json::json!({ "url": "s3://b", "auth": { "nodeIdentity": true } }),
                "nodeIdentity is not supported yet",
            ),
            (
                serde_json::json!({ "url": "file:///tmp/x", "auth": { "kv": "a" } }),
                "s3:// stores only",
            ),
        ] {
            let err = target(raw).unwrap().validate().unwrap_err();
            assert!(format!("{err:#}").contains(needle), "{err:#}");
        }
        assert!(target(serde_json::json!({ "url": "s3://b", "bogus": 1 })).is_err());
    }
}
