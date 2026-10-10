//! Publishing node content to a shared store (suspend plan §6.8, amendment 1).
//!
//! The artifact fetchers are read-only and stay that way. Publishing is the
//! one write path: a worker with a `contentStore` block in its config pushes a
//! digest from its node content store ([`crate::content_store`]) to that
//! store, at a key derived from the digest alone:
//!
//! ```text
//! <contentStore.url>/sha256/<hex>
//! ```
//!
//! Because the key is the digest, publishing is idempotent: publishing bytes
//! the store already holds uploads nothing. Every published digest's URL is
//! recorded in the node content store's index, so a caller (Phase 7b's
//! snapshot manifests) can name where each part lives.
//!
//! Reads never come through here. Any worker restores published content with
//! the ordinary `s3://` or `file://` artifact fetcher, with the digest as the
//! artifact's `checksum`, so the bytes are verified exactly like any other
//! digest-pinned artifact ([`published_artifact`]).
//!
//! # Example (what Phase 7b does with a snapshot part)
//!
//! ```no_run
//! # use legion::content_publish::*;
//! # use legion::content_store::*;
//! # fn demo(store: &ContentStore, publisher: &ContentPublishing) -> legion::prelude::R<()> {
//! let digest = ContentDigest::parse("sha256:0000000000000000000000000000000000000000000000000000000000000000")?;
//! let published = publisher.publish(&digest)?;
//! // Record `published.url` in the snapshot manifest. Another node restores it:
//! let spec = published_artifact(&published.url, &digest);
//! # let _ = spec; Ok(()) }
//! ```

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::{path::Path as ObjectPath, ObjectStore, WriteMultipart};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::artifacts::{Credential, CredentialSource};
use crate::content_store::{ContentDigest, ContentStore};
use crate::prelude::*;
use crate::structs::ArtifactSpec;

/// Where published content goes under the store URL, so one bucket can hold
/// other things beside it and the layout leaves room for other algorithms.
const DIGEST_PREFIX: &str = "sha256";

/// Multipart part size for S3 uploads. Snapshot memory files run to
/// gigabytes, so uploads stream in parts rather than buffering the file.
const S3_PART_BYTES: usize = 16 * 1024 * 1024;

/// What one publish did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedContent {
    pub digest: ContentDigest,
    /// Where any worker can fetch it, e.g. `s3://bucket/prefix/sha256/<hex>`.
    pub url: String,
    pub bytes: u64,
    /// False when the store already held the digest and nothing was sent.
    pub uploaded: bool,
}

/// A destination that accepts content by digest. Implementations must be
/// idempotent: publishing a digest the destination already holds succeeds
/// without rewriting it.
pub trait ContentPublisher: Send + Sync {
    /// The URL a digest is (or would be) published at. Pure: no I/O.
    fn url_for(&self, digest: &ContentDigest) -> String;

    /// Publish the bytes at `path`, which hash to `digest`, and return where
    /// they now live. Blocking: call from a blocking thread.
    fn publish_file(&self, digest: &ContentDigest, path: &Path) -> R<PublishedContent>;
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

/// The artifact spec that fetches published content back, verified by
/// digest. Phase 7b hands this to the ordinary artifact stager on restore.
pub fn published_artifact(url: &str, digest: &ContentDigest) -> ArtifactSpec {
    ArtifactSpec {
        checksum: Some(digest.to_string()),
        ..ArtifactSpec::from_source(url)
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

    fn path_for(&self, digest: &ContentDigest) -> PathBuf {
        self.root.join(DIGEST_PREFIX).join(digest.hex())
    }
}

impl ContentPublisher for FileContentPublisher {
    fn url_for(&self, digest: &ContentDigest) -> String {
        Url::from_file_path(self.path_for(digest))
            .map(|url| url.to_string())
            .unwrap_or_else(|_| format!("file://{}", self.path_for(digest).display()))
    }

    fn publish_file(&self, digest: &ContentDigest, path: &Path) -> R<PublishedContent> {
        let size = fs::metadata(path)
            .with_context(|| format!("stat {}", path.display()))?
            .len();
        let dest = self.path_for(digest);
        // Same digest and same size: already published. A blob that was
        // tampered with in place still fails the reader's digest check.
        if fs::metadata(&dest).is_ok_and(|meta| meta.len() == size) {
            return Ok(PublishedContent {
                digest: digest.clone(),
                url: self.url_for(digest),
                bytes: size,
                uploaded: false,
            });
        }
        let dir = dest.parent().expect("digest path has a parent");
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        // Write beside the destination and rename, so a reader never sees a
        // partial blob under the final name.
        let tmp = dir.join(format!(".{}.{}.tmp", digest.hex(), std::process::id()));
        fs::copy(path, &tmp).with_context(|| format!("copy to {}", tmp.display()))?;
        // Hash what landed, not what we meant to send.
        let actual = ContentDigest::of_file(&tmp)?;
        if &actual != digest {
            let _ = fs::remove_file(&tmp);
            bail!("refusing to publish {digest}: the source bytes hash to {actual}");
        }
        fs::rename(&tmp, &dest).with_context(|| format!("publish to {}", dest.display()))?;
        Ok(PublishedContent {
            digest: digest.clone(),
            url: self.url_for(digest),
            bytes: size,
            uploaded: true,
        })
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
    fn key_for(&self, digest: &ContentDigest) -> String {
        let prefix = self.url.path().trim_matches('/');
        if prefix.is_empty() {
            format!("{DIGEST_PREFIX}/{}", digest.hex())
        } else {
            format!("{prefix}/{DIGEST_PREFIX}/{}", digest.hex())
        }
    }

    fn client(&self) -> R<object_store::aws::AmazonS3> {
        let bucket = self
            .url
            .host_str()
            .context("contentStore.url has no bucket")?;
        let mut region: Option<String> = None;
        let mut endpoint: Option<String> = None;
        let credential = match self.kv.as_deref() {
            Some(path) => Some(
                self.credentials
                    .kv_credential(path, self.url.as_str())
                    .with_context(|| format!("contentStore credentials from kv {path}"))?,
            ),
            None => None,
        };
        if let Some(Credential::Aws {
            region: r,
            endpoint: e,
            ..
        }) = &credential
        {
            region = r.clone();
            endpoint = e.clone();
        }
        for (name, value) in self.url.query_pairs() {
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
}

impl ContentPublisher for S3ContentPublisher {
    fn url_for(&self, digest: &ContentDigest) -> String {
        let mut url = self.url.clone();
        url.set_path(&format!("/{}", self.key_for(digest)));
        url.to_string()
    }

    fn publish_file(&self, digest: &ContentDigest, path: &Path) -> R<PublishedContent> {
        let size = fs::metadata(path)
            .with_context(|| format!("stat {}", path.display()))?
            .len();
        let client = self.client()?;
        let key = ObjectPath::from(self.key_for(digest));
        let url = self.url_for(digest);
        // Publishing is blocking by contract (like fetchers); object_store is
        // async, so run it on a private current-thread runtime.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build content publish runtime")?;
        let uploaded = runtime.block_on(async {
            match client.head(&key).await {
                Ok(meta) if meta.size as u64 == size => return Ok(false),
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(err) => return Err(anyhow!(err).context(format!("head {url}"))),
            }
            let upload = client
                .put_multipart(&key)
                .await
                .with_context(|| format!("start upload to {url}"))?;
            let mut writer = WriteMultipart::new_with_chunk_size(upload, S3_PART_BYTES);
            let mut file =
                fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
            let mut buf = vec![0u8; 1024 * 1024];
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                // Bound in-flight parts so a multi-GiB file never buffers.
                writer
                    .wait_for_capacity(4)
                    .await
                    .with_context(|| format!("upload to {url}"))?;
                writer.write(&buf[..n]);
            }
            writer
                .finish()
                .await
                .with_context(|| format!("complete upload to {url}"))?;
            Ok::<bool, anyhow::Error>(true)
        })?;
        Ok(PublishedContent {
            digest: digest.clone(),
            url,
            bytes: size,
            uploaded,
        })
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

    pub fn store(&self) -> &Arc<ContentStore> {
        &self.store
    }

    /// Publish a digest the node content store holds, and record its URL.
    /// Idempotent: a digest already recorded as published, or already present
    /// at the destination, uploads nothing. The local blob is rehashed first,
    /// so corrupt bytes are never published.
    #[instrument(skip(self), fields(digest = %digest))]
    pub fn publish(&self, digest: &ContentDigest) -> R<PublishedContent> {
        let entry = self.store.verify(digest)?;
        if let Some(url) = self.store.published_url(digest)? {
            if url == self.publisher.url_for(digest) {
                return Ok(PublishedContent {
                    digest: digest.clone(),
                    url,
                    bytes: entry.size,
                    uploaded: false,
                });
            }
        }
        let published = self.publisher.publish_file(digest, &entry.path)?;
        self.store.record_published(digest, &published.url)?;
        info!(
            url = %published.url,
            bytes = published.bytes,
            uploaded = published.uploaded,
            "Published content"
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

    #[test]
    fn file_publisher_is_idempotent_and_readable_by_digest() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = Arc::new(ContentStore::open(
            dir.path().join("content"),
            ContentStoreConfig {
                min_free_disk: 0,
                ..Default::default()
            },
        )?);
        let bytes = b"snapshot part bytes".repeat(1000);
        let entry = store.put_bytes(ContentClass::Snapshots, &bytes, PutOptions::default())?;

        let shared = dir.path().join("shared");
        let target = target(serde_json::json!({
            "url": Url::from_directory_path(&shared).unwrap().to_string(),
        }))?;
        let publishing = ContentPublishing::new(
            store.clone(),
            target.publisher(Arc::new(AnonymousCredentials))?,
        );

        let first = publishing.publish(&entry.digest)?;
        assert!(first.uploaded);
        assert_eq!(first.bytes, bytes.len() as u64);
        let on_disk = shared.join("sha256").join(entry.digest.hex());
        assert_eq!(fs::read(&on_disk)?, bytes);
        assert_eq!(
            first.url,
            Url::from_file_path(&on_disk).unwrap().to_string()
        );
        assert_eq!(
            publishing.published_url(&entry.digest)?,
            Some(first.url.clone())
        );

        // Second publish: recorded, so nothing moves.
        let second = publishing.publish(&entry.digest)?;
        assert!(!second.uploaded);
        assert_eq!(second.url, first.url);

        // A fresh node (no record) publishing the same bytes finds them
        // already at the destination.
        let other = FileContentPublisher::new(&shared);
        let third = other.publish_file(&entry.digest, &entry.path)?;
        assert!(!third.uploaded);
        assert_eq!(third.url, first.url);

        // Readers go through the ordinary fetcher spec, pinned to the digest.
        let spec = published_artifact(&first.url, &entry.digest);
        assert_eq!(spec.checksum.as_deref(), Some(entry.digest.as_str()));
        assert_eq!(spec.source, first.url);
        Ok(())
    }

    #[test]
    fn file_publisher_refuses_bytes_that_do_not_match_the_digest() -> R<()> {
        let dir = tempfile::tempdir()?;
        let src = dir.path().join("part");
        fs::write(&src, b"actual bytes")?;
        let claimed = ContentDigest::of_bytes(b"other bytes");
        let publisher = FileContentPublisher::new(dir.path().join("shared"));
        let err = publisher.publish_file(&claimed, &src).unwrap_err();
        assert!(format!("{err:#}").contains("hash to"), "{err:#}");
        assert!(!dir
            .path()
            .join("shared/sha256")
            .join(claimed.hex())
            .exists());
        Ok(())
    }

    #[test]
    fn s3_urls_keep_the_prefix_and_endpoint() -> R<()> {
        let target = target(serde_json::json!({
            "url": "s3://legion-content/usw2?endpoint=http://minio:9000&region=auto",
            "auth": { "kv": "legion/content" },
        }))?;
        let publisher = target.publisher(Arc::new(AnonymousCredentials))?;
        let digest = ContentDigest::of_bytes(b"x");
        assert_eq!(
            publisher.url_for(&digest),
            format!(
                "s3://legion-content/usw2/sha256/{}?endpoint=http://minio:9000&region=auto",
                digest.hex()
            )
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
