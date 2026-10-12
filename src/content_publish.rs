//! Publishing node content to a shared store (suspend plan §6.8, amendment 1;
//! internal_docs/plans/checkpoint-encryption-plan.md).
//!
//! The artifact fetchers are read-only and stay that way. Publishing is the
//! one write path: a worker with a `contentStore` block in its config seals a
//! digest from its node content store ([`crate::content_store`]) and pushes
//! it to that store, at a name derived from the part's key:
//!
//! ```text
//! <contentStore.url>/sealed/<object name>
//! ```
//!
//! Every published part is sealed ([`crate::checkpoint_seal`]); there is no
//! plaintext mode. The key, and so the name, is a keyed function of the
//! plaintext digest, so publishing is still idempotent: publishing bytes the
//! store already holds uploads nothing. Every published digest's URL is
//! recorded in the node content store's index, so a caller (Phase 7b's
//! snapshot manifests) can name where each part lives.
//!
//! Reads never come through here. Any worker restores published content with
//! the ordinary `s3://` or `file://` artifact fetcher
//! ([`crate::artifacts::ArtifactStager::fetch_sealed_into_store`]), which
//! opens the sealed object and verifies the plaintext against the digest.
//!
//! # Example (what Phase 7b does with a snapshot part)
//!
//! ```no_run
//! # use legion::content_publish::*;
//! # use legion::checkpoint_seal::PartKey;
//! # fn demo(publisher: &ContentPublishing, key: &PartKey) -> legion::prelude::R<()> {
//! // `key` came from the manager with the publish request.
//! let published = publisher.publish(key)?;
//! // Record `published.url` and `key.key_id()` in the snapshot manifest.
//! # let _ = published; Ok(()) }
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::{path::Path as ObjectPath, ObjectStore, WriteMultipart};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::artifacts::{Credential, CredentialSource};
use crate::checkpoint_seal::{sealed_len, PartKey, SealingReader, OBJECT_PREFIX};
use crate::content_store::{ContentDigest, ContentStore};
use crate::prelude::*;

/// Multipart part size for S3 uploads. Snapshot memory files run to
/// gigabytes, so uploads stream in parts rather than buffering the file.
const S3_PART_BYTES: usize = 16 * 1024 * 1024;

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
    /// False when the store already held the object and nothing was sent.
    pub uploaded: bool,
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

/// A destination that accepts sealed parts. Implementations must be
/// idempotent: publishing an object the destination already holds succeeds
/// without rewriting it.
pub trait ContentPublisher: Send + Sync {
    /// The URL a sealed object named `object_name` is (or would be)
    /// published at. Pure: no I/O.
    fn url_for(&self, object_name: &str) -> String;

    /// Seal the bytes at `path`, which must hash to `key.digest()`, and
    /// upload them. Fails with [`PublishSourceMismatch`] (and publishes
    /// nothing) when they do not. Blocking: call from a blocking thread.
    fn publish_sealed(&self, key: &PartKey, path: &Path) -> R<PublishedContent>;

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
}

impl ContentStoreTarget {
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

    fn publish_sealed(&self, key: &PartKey, path: &Path) -> R<PublishedContent> {
        let size = fs::metadata(path)
            .with_context(|| format!("stat {}", path.display()))?
            .len();
        let name = key.object_name();
        let dest = self.path_for(&name);
        let published = |uploaded| PublishedContent {
            digest: key.digest().clone(),
            url: self.url_for(&name),
            key_id: key.key_id().to_string(),
            bytes: size,
            uploaded,
        };
        // Same name and size: already published. An object tampered with in
        // place still fails the reader's authentication.
        if fs::metadata(&dest).is_ok_and(|meta| meta.len() == sealed_len(size)) {
            return Ok(published(false));
        }
        let dir = dest.parent().expect("object path has a parent");
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        // Write beside the destination and rename, so a reader never sees a
        // partial object under the final name.
        let tmp = dir.join(format!(".{name}.{}.tmp", std::process::id()));
        let sealed = (|| {
            let source =
                fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
            let mut reader = SealingReader::new(key, source, size)?;
            let mut out =
                fs::File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
            reader
                .write_all_to(&mut out)
                .with_context(|| format!("seal {} to {}", path.display(), tmp.display()))?;
            // The sealer hashed the plaintext it read: these are exactly the
            // bytes the digest names, or nothing is published.
            let actual = reader.finish()?;
            if &actual != key.digest() {
                return Err(anyhow::Error::new(PublishSourceMismatch {
                    expected: key.digest().clone(),
                    actual,
                }));
            }
            Ok(())
        })();
        if let Err(err) = sealed {
            let _ = fs::remove_file(&tmp);
            return Err(err);
        }
        fs::rename(&tmp, &dest).with_context(|| format!("publish to {}", dest.display()))?;
        Ok(published(true))
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
    builder.build().context("build s3 client for contentStore")
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

impl ContentPublisher for S3ContentPublisher {
    fn url_for(&self, object_name: &str) -> String {
        let mut url = self.url.clone();
        url.set_path(&format!("/{}", self.key_for(object_name)));
        url.to_string()
    }

    fn publish_sealed(&self, key: &PartKey, path: &Path) -> R<PublishedContent> {
        let size = fs::metadata(path)
            .with_context(|| format!("stat {}", path.display()))?
            .len();
        let client = self.client()?;
        let name = key.object_name();
        let object = ObjectPath::from(self.key_for(&name));
        let url = self.url_for(&name);
        // Publishing is blocking by contract (like fetchers); object_store is
        // async, so run it on a private current-thread runtime.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build content publish runtime")?;
        let uploaded = runtime.block_on(async {
            match client.head(&object).await {
                Ok(meta) if meta.size as u64 == sealed_len(size) => return Ok(false),
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(err) => return Err(anyhow!(err).context(format!("head {url}"))),
            }
            let source =
                fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
            let mut reader = SealingReader::new(key, source, size)?;
            let upload = client
                .put_multipart(&object)
                .await
                .with_context(|| format!("start upload to {url}"))?;
            let mut writer = WriteMultipart::new_with_chunk_size(upload, S3_PART_BYTES);
            let streamed: R<()> = async {
                while let Some(sealed) = reader
                    .next_sealed()
                    .with_context(|| format!("seal {}", path.display()))?
                {
                    // Bound in-flight parts so a multi-GiB file never buffers.
                    writer
                        .wait_for_capacity(4)
                        .await
                        .with_context(|| format!("upload to {url}"))?;
                    writer.write(sealed);
                }
                Ok(())
            }
            .await;
            // The sealer hashed the plaintext it read: complete the upload
            // only when those are exactly the bytes the digest names.
            let verified = streamed.and_then(|()| {
                let actual = reader.finish()?;
                if &actual != key.digest() {
                    return Err(anyhow::Error::new(PublishSourceMismatch {
                        expected: key.digest().clone(),
                        actual,
                    }));
                }
                Ok(())
            });
            if let Err(err) = verified {
                let _ = writer.abort().await;
                return Err(err);
            }
            writer
                .finish()
                .await
                .with_context(|| format!("complete upload to {url}"))?;
            Ok::<bool, anyhow::Error>(true)
        })?;
        Ok(PublishedContent {
            digest: key.digest().clone(),
            url,
            key_id: key.key_id().to_string(),
            bytes: size,
            uploaded,
        })
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
}

impl std::fmt::Debug for ContentPublishing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContentPublishing").finish_non_exhaustive()
    }
}

impl ContentPublishing {
    pub fn new(store: Arc<ContentStore>, publisher: Arc<dyn ContentPublisher>) -> Self {
        Self { store, publisher }
    }

    /// Write a document beside the published content (see
    /// [`ContentPublisher::put_document`]).
    pub fn put_document(&self, object_name: &str, bytes: &[u8]) -> R<String> {
        self.publisher.put_document(object_name, bytes)
    }

    pub fn store(&self) -> &Arc<ContentStore> {
        &self.store
    }

    /// Seal and publish the part `key` names, which the node content store
    /// must hold, and record its URL. Idempotent: a part already recorded as
    /// published at this URL, or already present at the destination,
    /// uploads nothing. The sealer hashes the bytes as it reads them, so a
    /// corrupt local blob is never published: it is dropped from the store
    /// instead.
    #[instrument(skip(self, key), fields(digest = %key.digest(), key_id = %key.key_id()))]
    pub fn publish(&self, key: &PartKey) -> R<PublishedContent> {
        let digest = key.digest();
        let entry = self
            .store
            .get(digest)?
            .with_context(|| format!("content {digest} is not in the node store"))?;
        let url = self.publisher.url_for(&key.object_name());
        if self.store.published_url(digest)?.as_deref() == Some(url.as_str()) {
            return Ok(PublishedContent {
                digest: digest.clone(),
                url,
                key_id: key.key_id().to_string(),
                bytes: entry.size,
                uploaded: false,
            });
        }
        let published = match self.publisher.publish_sealed(key, &entry.path) {
            Ok(published) => published,
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
        self.store.record_published(digest, &published.url)?;
        info!(
            url = %published.url,
            bytes = published.bytes,
            uploaded = published.uploaded,
            "Published sealed content"
        );
        Ok(published)
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
    fn file_publisher_seals_is_idempotent_and_opens_by_key() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = Arc::new(ContentStore::open(
            dir.path().join("content"),
            ContentStoreConfig {
                min_free_disk: 0,
                ..Default::default()
            },
        )?);
        let bytes = b"snapshot part bytes with a SECRET_TOKEN=abc123 ".repeat(1000);
        let entry = store.put_bytes(ContentClass::Snapshots, &bytes, PutOptions::default())?;
        let key = part_key(&entry.digest);

        let shared = dir.path().join("shared");
        let target = target(serde_json::json!({
            "url": Url::from_directory_path(&shared).unwrap().to_string(),
        }))?;
        let publishing = ContentPublishing::new(
            store.clone(),
            target.publisher(Arc::new(AnonymousCredentials))?,
        );

        let first = publishing.publish(&key)?;
        assert!(first.uploaded);
        assert_eq!(first.bytes, bytes.len() as u64);
        assert_eq!(first.key_id, "ck_test");
        let on_disk = shared.join("sealed").join(key.object_name());
        let sealed = fs::read(&on_disk)?;
        assert_eq!(sealed.len() as u64, sealed_len(bytes.len() as u64));
        assert!(
            !sealed.windows(19).any(|w| w == b"SECRET_TOKEN=abc123"),
            "the published object holds no plaintext"
        );
        assert!(
            !shared.join("sha256").exists(),
            "nothing is published under the plaintext digest"
        );
        assert_eq!(
            first.url,
            Url::from_file_path(&on_disk).unwrap().to_string()
        );
        assert_eq!(
            publishing.published_url(&entry.digest)?,
            Some(first.url.clone())
        );

        // Second publish: recorded, so nothing moves.
        let second = publishing.publish(&key)?;
        assert!(!second.uploaded);
        assert_eq!(second.url, first.url);

        // A fresh node (no record) publishing the same bytes finds them
        // already at the destination: the same key seals the same object.
        let other = FileContentPublisher::new(&shared);
        let third = other.publish_sealed(&key, &entry.path)?;
        assert!(!third.uploaded);
        assert_eq!(third.url, first.url);

        // The object opens with the key back to the exact bytes.
        let copy = dir.path().join("fetched");
        fs::copy(&on_disk, &copy)?;
        crate::checkpoint_seal::open_file_in_place(&key, &copy)?;
        assert_eq!(fs::read(&copy)?, bytes);
        Ok(())
    }

    #[test]
    fn file_publisher_refuses_bytes_that_do_not_match_the_digest() -> R<()> {
        let dir = tempfile::tempdir()?;
        let src = dir.path().join("part");
        fs::write(&src, b"actual bytes")?;
        let claimed = ContentDigest::of_bytes(b"other bytes");
        let key = part_key(&claimed);
        let publisher = FileContentPublisher::new(dir.path().join("shared"));
        let err = publisher.publish_sealed(&key, &src).unwrap_err();
        assert!(
            err.downcast_ref::<PublishSourceMismatch>().is_some(),
            "{err:#}"
        );
        assert!(format!("{err:#}").contains("hash to"), "{err:#}");
        assert!(!dir
            .path()
            .join("shared/sealed")
            .join(key.object_name())
            .exists());
        // No temp file is left behind either.
        let leftovers: Vec<_> = fs::read_dir(dir.path().join("shared/sealed"))?.collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
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
            let published = publishing.publish(&key)?;
            let secs = started.elapsed().as_secs_f64();
            assert!(published.uploaded);
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
