//! `oci://` fetcher built on the `oci-client` crate (plan judgement call 4):
//! registry auth dance (anonymous, basic, bearer), manifest resolution
//! (index → platform manifest), and two materializations:
//!
//! - an **OCI layout tree** for image consumers (the container plugin), and
//! - **selected-layer extraction** for ORAS consumers (`mediaType`) — model
//!   weights, WASM modules, and friends through the same pipeline.
//!
//! `resolve()` turns tags into manifest digests for `resolve: "pin"`.

use std::path::PathBuf;

use oci_client::client::{ClientConfig, ClientProtocol};
use oci_client::manifest::{ImageIndexEntry, OciDescriptor, OciImageManifest, OciManifest};
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};

use crate::artifacts::fetcher::{
    ArtifactFetcher, Credential, FetchRequest, FetchedArtifact, FetchedPayload, ResolvedIdentity,
};
use crate::artifacts::stage::ArtifactSink;
use crate::content_store::{ContentClass, ContentDigest, ContentStore, PutOptions};
use crate::prelude::*;

/// Annotation carrying an ORAS layer's preferred file name.
const TITLE_ANNOTATION: &str = "org.opencontainers.image.title";

#[derive(Debug, Default)]
pub struct OciFetcher;

impl OciFetcher {
    pub fn new() -> Self {
        Self
    }
}

/// Parses `oci://registry/repo[:tag][@digest][?protocol=http]` into an
/// oci-client [`Reference`] plus the client protocol (plain-http registries
/// are a dev/test affordance behind an explicit query param).
fn parse_reference(req: &FetchRequest<'_>) -> R<(Reference, ClientProtocol)> {
    let host = req
        .url
        .host_str()
        .with_context(|| format!("{} has no registry host", req.redacted_source))?;
    let mut reference_str = String::from(host);
    if let Some(port) = req.url.port() {
        reference_str.push(':');
        reference_str.push_str(&port.to_string());
    }
    reference_str.push_str(req.url.path());
    let reference: Reference = reference_str
        .parse()
        .with_context(|| format!("invalid OCI reference in {}", req.redacted_source))?;

    let protocol = if req
        .url
        .query_pairs()
        .any(|(key, value)| key == "protocol" && value == "http")
    {
        ClientProtocol::Http
    } else {
        ClientProtocol::Https
    };
    Ok((reference, protocol))
}

fn registry_auth(req: &FetchRequest<'_>) -> R<RegistryAuth> {
    match req.credential {
        None => Ok(RegistryAuth::Anonymous),
        Some(Credential::Basic { username, password }) => {
            Ok(RegistryAuth::Basic(username.clone(), password.clone()))
        }
        Some(Credential::Bearer { token }) => Ok(RegistryAuth::Bearer(token.clone())),
        Some(other) => bail!(
            "oci artifacts support basic/bearer credentials, got {other:?} for {}",
            req.redacted_source
        ),
    }
}

/// The host architecture under the registry's (Go) naming.
fn image_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "x86" => "amd",
        "aarch64" => "arm64",
        "powerpc64" => "ppc64le",
        other => other,
    }
}

/// Picks `linux/<host arch>` out of a multi-platform image index.
///
/// Container images hold Linux guests whatever the host runs — Apple
/// `container` boots a Linux VM on macOS — so the platform to pull is not the
/// host's. oci-client's default resolver uses the host's, which on macOS asks
/// for `darwin/arm64` and matches nothing ("no entry found in image index
/// manifest matching client's default platform"). On Linux this resolves the
/// same entry the default would.
fn linux_platform_resolver(manifests: &[ImageIndexEntry]) -> Option<String> {
    manifests
        .iter()
        .find(|entry| {
            entry.platform.as_ref().is_some_and(|platform| {
                platform.os == "linux" && platform.architecture == image_arch()
            })
        })
        .map(|entry| entry.digest.clone())
}

/// `for_image` selects the image-layout path's Linux platform resolution.
/// ORAS artifacts (`mediaType`) are not container rootfs and keep the host
/// platform default, since a platform-indexed ORAS artifact is meant for the
/// machine fetching it.
///
/// Clients are shared, one per protocol, platform selection, and credential,
/// and driven by one runtime ([`blocking_runtime`]). A client built per call
/// loaded the host's CA bundle, opened new TLS connections, and asked the
/// registry for a new bearer token every time, so resolving a tag cost a
/// token request and a `HEAD`, each behind its own handshake, on every
/// sandbox start. A shared one keeps its connections and its token cache.
/// The credential is part of the key because the client caches tokens by
/// registry and repository, not by who asked: a token minted for one job's
/// credential must never serve another job's pull.
fn oci_client(protocol: ClientProtocol, for_image: bool, auth: &RegistryAuth) -> Client {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    /// Bounds the cache for a node whose jobs use many credentials; clients
    /// past it are built per call, as before.
    const MAX_CLIENTS: usize = 64;
    static CLIENTS: OnceLock<Mutex<HashMap<(bool, bool, [u8; 32]), Client>>> = OnceLock::new();

    let key = (
        protocol == ClientProtocol::Http,
        for_image,
        credential_fingerprint(auth),
    );
    let mut clients = CLIENTS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if let Some(client) = clients.get(&key) {
        return client.clone();
    }
    let mut config = ClientConfig {
        protocol,
        connect_timeout: Some(std::time::Duration::from_secs(30)),
        ..ClientConfig::default()
    };
    if for_image {
        config.platform_resolver = Some(Box::new(linux_platform_resolver));
    }
    let client = Client::new(config);
    if clients.len() < MAX_CLIENTS {
        clients.insert(key, client.clone());
    }
    client
}

/// A digest of a registry credential, for the keys of [`oci_client`]'s
/// clients and of shared tag resolutions. The credential itself is never
/// kept beyond the client that uses it.
fn credential_fingerprint(auth: &RegistryAuth) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    match auth {
        RegistryAuth::Anonymous => hasher.update(b"anonymous"),
        RegistryAuth::Basic(username, password) => {
            hasher.update(b"basic\0");
            hasher.update(username.as_bytes());
            hasher.update(b"\0");
            hasher.update(password.as_bytes());
        }
        RegistryAuth::Bearer(token) => {
            hasher.update(b"bearer\0");
            hasher.update(token.as_bytes());
        }
    }
    hasher.finalize().into()
}

/// The runtime every OCI fetch on the node runs on. Shared, so the shared
/// clients' pooled connections (which live on the runtime that opened them)
/// outlive one fetch. Multi-threaded, so staging threads can `block_on` it
/// at once.
fn blocking_runtime() -> R<&'static tokio::runtime::Runtime> {
    static RUNTIME: std::sync::OnceLock<Result<tokio::runtime::Runtime, String>> =
        std::sync::OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("oci-fetch")
                .enable_all()
                .build()
                .map_err(|err| err.to_string())
        })
        .as_ref()
        .map_err(|err| anyhow!("build oci fetch runtime: {err}"))
}

/// Checks descriptor sizes against the artifact size budget before any blob
/// is pulled, so over-limit images fail fast.
fn check_size_budget(req: &FetchRequest<'_>, descriptors: &[&OciDescriptor]) -> R<u64> {
    let total: u64 = descriptors.iter().map(|d| d.size.max(0) as u64).sum();
    if total > req.limits.max_bytes {
        bail!(
            "{} is {} bytes of blobs, over the {}-byte artifact size limit",
            req.redacted_source,
            total,
            req.limits.max_bytes
        );
    }
    Ok(total)
}

async fn pull_blob_to_file(
    client: &Client,
    reference: &Reference,
    descriptor: &OciDescriptor,
    path: &std::path::Path,
) -> R<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::File::create(path)
        .await
        .with_context(|| format!("create blob file {}", path.display()))?;
    client
        .pull_blob(reference, descriptor, &mut file)
        .await
        .with_context(|| format!("pull blob {}", descriptor.digest))?;
    // A tokio file hands its last write to the blocking pool and returns;
    // dropping it unflushed lets the caller read the path before the bytes
    // land. The caller hard-links the blob into the node content store at
    // once, which then hashed an empty or partial file, refused it, and the
    // next start downloaded the layer again.
    tokio::io::AsyncWriteExt::flush(&mut file)
        .await
        .with_context(|| format!("write blob file {}", path.display()))?;
    Ok(())
}

fn blob_path(layout_root: &std::path::Path, digest: &str) -> R<PathBuf> {
    let (algorithm, hex) = digest
        .split_once(':')
        .with_context(|| format!("invalid blob digest {digest}"))?;
    if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("invalid blob digest {digest}");
    }
    Ok(layout_root.join("blobs").join(algorithm).join(hex))
}

impl ArtifactFetcher for OciFetcher {
    fn schemes(&self) -> &'static [&'static str] {
        &["oci"]
    }

    /// Resolve a tag to its manifest digest (no blob downloads). Digest-pinned
    /// references need no resolution.
    fn resolve(&self, req: &FetchRequest<'_>) -> R<Option<ResolvedIdentity>> {
        let (reference, protocol) = parse_reference(req)?;
        if reference.digest().is_some() {
            return Ok(None);
        }
        let auth = registry_auth(req)?;
        // Pinning HEADs the reference itself, so no platform is selected: a
        // multi-platform tag pins to its index digest, which is what the
        // per-node fetch then resolves a platform manifest out of.
        let client = oci_client(protocol.clone(), false, &auth);
        let runtime = blocking_runtime()?;
        let digest = runtime
            .block_on(client.fetch_manifest_digest(&reference, &auth))
            .with_context(|| format!("resolve manifest digest for {}", req.redacted_source))?;

        let pinned_reference = reference.clone_with_digest(digest.clone());
        let mut pinned_source = format!("oci://{pinned_reference}");
        if protocol == ClientProtocol::Http {
            pinned_source.push_str("?protocol=http");
        }
        Ok(Some(ResolvedIdentity {
            pinned_source,
            digest,
        }))
    }

    fn fetch(&self, req: &FetchRequest<'_>, sink: &mut ArtifactSink) -> R<FetchedArtifact> {
        let (reference, protocol) = parse_reference(req)?;
        let auth = registry_auth(req)?;
        let client = oci_client(protocol, req.media_type.is_none(), &auth);
        let runtime = blocking_runtime()?;

        match req.media_type {
            Some(media_type) => {
                // Resolve (possibly via an image index) to one platform manifest.
                let (manifest, manifest_digest) = runtime
                    .block_on(client.pull_image_manifest(&reference, &auth))
                    .with_context(|| format!("pull manifest for {}", req.redacted_source))?;
                self.fetch_oras_layers(
                    req,
                    sink,
                    runtime,
                    &client,
                    &reference,
                    manifest,
                    manifest_digest,
                    media_type,
                )
            }
            None => self.fetch_image_layout(req, sink, runtime, &client, &reference, &auth),
        }
    }
}

/// The manifest media types an image pull accepts: single-platform manifests
/// and multi-platform indexes, in both the OCI and Docker v2 dialects.
const MANIFEST_MEDIA_TYPES: &[&str] = &[
    oci_client::manifest::OCI_IMAGE_MEDIA_TYPE,
    oci_client::manifest::IMAGE_MANIFEST_MEDIA_TYPE,
    oci_client::manifest::OCI_IMAGE_INDEX_MEDIA_TYPE,
    oci_client::manifest::IMAGE_MANIFEST_LIST_MEDIA_TYPE,
];

/// Image references being pulled on this node right now. A second start of
/// the same image waits for the first pull instead of downloading the same
/// layers alongside it, then finds them in the node content store.
static IMAGE_PULLS: std::sync::Mutex<std::collections::BTreeSet<String>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());
static IMAGE_PULL_DONE: std::sync::Condvar = std::sync::Condvar::new();

/// A claim on one image reference's pull; released on drop.
struct ImagePullClaim(String);

impl ImagePullClaim {
    fn claim(reference: &str) -> Self {
        let mut pulls = IMAGE_PULLS.lock().unwrap_or_else(|p| p.into_inner());
        while pulls.contains(reference) {
            pulls = IMAGE_PULL_DONE
                .wait(pulls)
                .unwrap_or_else(|p| p.into_inner());
        }
        pulls.insert(reference.to_string());
        ImagePullClaim(reference.to_string())
    }
}

impl Drop for ImagePullClaim {
    fn drop(&mut self) {
        let mut pulls = IMAGE_PULLS.lock().unwrap_or_else(|p| p.into_inner());
        pulls.remove(&self.0);
        IMAGE_PULL_DONE.notify_all();
    }
}

/// Tag resolutions this node finished, by reference and credential: the
/// manifest digest and when the request to the registry was sent.
static TAG_RESOLUTIONS: std::sync::Mutex<
    std::collections::BTreeMap<String, (String, std::time::Instant)>,
> = std::sync::Mutex::new(std::collections::BTreeMap::new());

/// How long a finished resolution is remembered. Only pulls that were
/// already waiting when it was sent can use it, so this bounds memory, not
/// staleness.
const TAG_RESOLUTION_MEMORY: std::time::Duration = std::time::Duration::from_secs(600);

/// Which tag resolutions a pull may share: the same reference, asked with
/// the same credential. A pull never reuses an answer the registry gave to
/// someone else's credential.
fn tag_resolution_key(reference: &Reference, auth: &RegistryAuth) -> String {
    let fingerprint: String = credential_fingerprint(auth)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("{}\0{fingerprint}", reference.whole())
}

/// A tag's current digest for a pull that asked at `requested_at`.
///
/// A resolution *sent* after this pull asked is at least as fresh as the
/// answer this pull would get by asking itself, so it is reused, finished or
/// still in flight. One request is in flight per key: a burst of N starts
/// pays about two round trips (the first pull's, and one for the pulls that
/// arrived while it was out), never N. A pull with no such answer asks the
/// registry.
fn resolve_tag_shared(
    key: &str,
    requested_at: std::time::Instant,
    resolve: impl FnOnce() -> R<String>,
) -> R<String> {
    let mut resolve = Some(resolve);
    loop {
        if let Some((digest, sent_at)) = TAG_RESOLUTIONS
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(key)
        {
            if *sent_at >= requested_at {
                return Ok(digest.clone());
            }
        }
        // One resolution in flight per key. A pull that finds one waits for
        // it, and uses its answer when it was sent after the pull asked;
        // otherwise it goes round again and sends (or waits for) a newer one.
        let (flight, owner) = {
            let mut flights = TAG_FLIGHTS.lock().unwrap_or_else(|p| p.into_inner());
            match flights.get(key) {
                Some(flight) => (flight.clone(), false),
                None => {
                    let flight = std::sync::Arc::new(TagFlight {
                        sent_at: std::time::Instant::now(),
                        answer: std::sync::Mutex::new(None),
                        done: std::sync::Condvar::new(),
                    });
                    flights.insert(key.to_string(), flight.clone());
                    (flight, true)
                }
            }
        };
        if owner {
            let resolve = resolve.take().expect("a pull owns at most one flight");
            let answer = resolve();
            if let Ok(digest) = answer.as_ref() {
                let mut resolutions = TAG_RESOLUTIONS.lock().unwrap_or_else(|p| p.into_inner());
                resolutions.retain(|_, (_, at)| {
                    flight.sent_at.duration_since(*at) < TAG_RESOLUTION_MEMORY
                });
                // Keep the newest answer if another flight with this key
                // finished first.
                let newer = resolutions
                    .get(key)
                    .is_some_and(|(_, at)| *at > flight.sent_at);
                if !newer {
                    resolutions.insert(key.to_string(), (digest.clone(), flight.sent_at));
                }
            }
            *flight.answer.lock().unwrap_or_else(|p| p.into_inner()) = Some(
                answer
                    .as_ref()
                    .map(String::clone)
                    .map_err(|err| format!("{err:#}")),
            );
            TAG_FLIGHTS
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(key);
            flight.done.notify_all();
            return answer;
        }
        let mut answer = flight.answer.lock().unwrap_or_else(|p| p.into_inner());
        while answer.is_none() {
            answer = flight.done.wait(answer).unwrap_or_else(|p| p.into_inner());
        }
        if flight.sent_at >= requested_at {
            return answer
                .clone()
                .expect("checked above")
                .map_err(|err| anyhow!(err));
        }
        // Sent before this pull asked: too old to answer it. Ask again.
    }
}

/// A tag resolution in flight ([`resolve_tag_shared`]).
struct TagFlight {
    sent_at: std::time::Instant,
    answer: std::sync::Mutex<Option<Result<String, String>>>,
    done: std::sync::Condvar,
}

/// Tag resolutions in flight, by the same key as [`TAG_RESOLUTIONS`].
static TAG_FLIGHTS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<TagFlight>>>,
> = std::sync::LazyLock::new(Default::default);

/// Image blobs (manifests, configs, layers) in the node content store
/// (`artifacts` class), by digest. Every blob of an image is immutable and
/// named by its digest, so a blob the store holds is the blob the registry
/// would send: serving it skips the download and, for a manifest, the
/// registry request that pull-rate limits count.
///
/// Everything here is best effort. A blob the store cannot keep (budget full
/// of pinned content, disk low) costs the next start a download, never this
/// start its image.
struct ImageBlobCache<'a> {
    store: &'a ContentStore,
}

impl ImageBlobCache<'_> {
    /// A cached blob's bytes, for manifests and indexes (small JSON, so they
    /// are rehashed on every read: one that no longer matches its digest is
    /// dropped from the store and fetched again).
    fn read(&self, digest: &str) -> Option<Vec<u8>> {
        let digest = ContentDigest::parse(digest).ok()?;
        if !self.store.contains(&digest).ok()? {
            return None;
        }
        let entry = self.store.verify(&digest).ok()?;
        std::fs::read(&entry.path).ok()
    }

    /// Places a cached blob at `dest`: a hard link into the store's blob
    /// (same filesystem, no bytes copied), else a copy. `false` on a miss.
    fn place(&self, digest: &str, dest: &std::path::Path) -> bool {
        let Ok(digest) = ContentDigest::parse(digest) else {
            return false;
        };
        let Ok(Some(entry)) = self.store.get(&digest) else {
            return false;
        };
        if let Some(parent) = dest.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::remove_file(dest);
        std::fs::hard_link(&entry.path, dest).is_ok() || std::fs::copy(&entry.path, dest).is_ok()
    }

    /// The platform manifest of the image `top_digest` names, when it and
    /// every blob it lists are already in the store: an image this node can
    /// lay out with no registry request. `None` when anything is missing.
    fn whole_image(&self, top_digest: &str) -> Option<(Vec<u8>, String)> {
        let top = self.read(top_digest)?;
        let (raw, digest) = match serde_json::from_slice::<OciManifest>(&top).ok()? {
            OciManifest::Image(_) => (top, top_digest.to_string()),
            OciManifest::ImageIndex(index) => {
                let digest = linux_platform_resolver(&index.manifests)?;
                (self.read(&digest)?, digest)
            }
        };
        let manifest: OciImageManifest = serde_json::from_slice(&raw).ok()?;
        let whole = std::iter::once(&manifest.config)
            .chain(manifest.layers.iter())
            .all(|descriptor| {
                ContentDigest::parse(&descriptor.digest)
                    .ok()
                    .and_then(|digest| self.store.contains(&digest).ok())
                    .unwrap_or(false)
            });
        whole.then_some((raw, digest))
    }

    /// Keeps bytes fetched from the registry, verified against `digest`.
    fn keep_bytes(&self, digest: &str, bytes: &[u8]) {
        let Ok(digest) = ContentDigest::parse(digest) else {
            return;
        };
        if let Err(err) = self.store.put_bytes(
            ContentClass::Artifacts,
            bytes,
            PutOptions::default().expect(digest.clone()),
        ) {
            debug!(%digest, error = %format!("{err:#}"), "Could not cache image manifest");
        }
    }

    /// Keeps a blob just pulled into `path`, without copying it when the
    /// layout and the store share a filesystem: a hard link moves into the
    /// store, which re-verifies the digest and makes the shared file
    /// read-only. `path` stays in place either way.
    fn keep_file(&self, digest: &str, path: &std::path::Path) {
        let Ok(digest) = ContentDigest::parse(digest) else {
            return;
        };
        let opts = PutOptions::default()
            .expect(digest.clone())
            .label("image blob");
        let result = match self.store.staging_path("image-blob") {
            Ok(staged) if std::fs::hard_link(path, &staged).is_ok() => {
                self.store
                    .import_file(ContentClass::Artifacts, &staged, opts)
            }
            _ => self.store.put_file(ContentClass::Artifacts, path, opts),
        };
        if let Err(err) = result {
            warn!(
                %digest,
                error = %format!("{err:#}"),
                "Could not cache image layer in node content store"
            );
        }
    }

    /// The manifest digest an image *tag* names now, for a node that may
    /// already hold it.
    ///
    /// One `HEAD` request: it carries the digest, downloads nothing, and
    /// Docker Hub does not count it toward the anonymous pull limit. A tag
    /// that moved is therefore picked up on the next start. When the
    /// registry cannot answer (rate limited, down, offline), the digest the
    /// tag last resolved to on this node is used, so a cached image still
    /// starts; with no record, the registry's error stands.
    fn resolve_tag(
        &self,
        runtime: &tokio::runtime::Runtime,
        client: &Client,
        reference: &Reference,
        auth: &RegistryAuth,
        redacted: &str,
    ) -> R<String> {
        let key = reference.whole();
        match runtime.block_on(client.fetch_manifest_digest(reference, auth)) {
            Ok(digest) => {
                if let Ok(parsed) = ContentDigest::parse(&digest) {
                    if let Err(err) = self.store.record_image_ref(&key, &parsed) {
                        debug!(error = %format!("{err:#}"), "Could not record image tag digest");
                    }
                }
                Ok(digest)
            }
            Err(err) => match self.store.image_ref(&key) {
                Ok(Some((digest, resolved_at))) => {
                    warn!(
                        image = %redacted,
                        %digest,
                        resolved_at,
                        error = %err,
                        "Registry did not answer for this tag; using the digest it last resolved                          to on this node"
                    );
                    Ok(digest.as_str().to_string())
                }
                _ => Err(anyhow::Error::from(err))
                    .with_context(|| format!("resolve manifest digest for {redacted}")),
            },
        }
    }
}

impl OciFetcher {
    /// ORAS materialization: pull only layers matching `media_type` into
    /// files named by their title annotation (digest hex as fallback).
    #[allow(clippy::too_many_arguments)]
    fn fetch_oras_layers(
        &self,
        req: &FetchRequest<'_>,
        sink: &mut ArtifactSink,
        runtime: &tokio::runtime::Runtime,
        client: &Client,
        reference: &Reference,
        manifest: OciImageManifest,
        manifest_digest: String,
        media_type: &str,
    ) -> R<FetchedArtifact> {
        let selected: Vec<&OciDescriptor> = manifest
            .layers
            .iter()
            .filter(|layer| layer.media_type == media_type)
            .collect();
        if selected.is_empty() {
            let available: Vec<&str> = manifest
                .layers
                .iter()
                .map(|layer| layer.media_type.as_str())
                .collect();
            bail!(
                "{} has no layers with mediaType {media_type} (available: {available:?})",
                req.redacted_source
            );
        }
        let total = check_size_budget(req, &selected)?;
        sink.set_bytes_total(Some(total));

        let layers_dir = sink.dir().join("layers");
        let mut files = Vec::with_capacity(selected.len());
        for descriptor in selected {
            req.limits.check_deadline(req.redacted_source)?;
            let file_name = descriptor
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get(TITLE_ANNOTATION))
                .map(|title| sanitize_title(title))
                .unwrap_or_else(|| {
                    descriptor
                        .digest
                        .split_once(':')
                        .map(|(_, hex)| hex.to_string())
                        .unwrap_or_else(|| "layer".to_string())
                });
            let path = layers_dir.join(file_name);
            runtime.block_on(pull_blob_to_file(client, reference, descriptor, &path))?;
            sink.record_external_bytes(descriptor.size.max(0) as u64)?;
            files.push(path);
        }
        Ok(FetchedArtifact {
            bytes: sink.bytes_written(),
            payload: FetchedPayload::Layers {
                files,
                media_type: media_type.to_string(),
                digest: manifest_digest,
            },
        })
    }

    /// Image materialization: write a complete on-disk OCI layout
    /// (`oci-layout`, `index.json`, `blobs/`) that image-consuming runtimes
    /// hand to their runtime.
    ///
    /// With a node content store on the sink, every blob is looked up there
    /// first and kept there after a download (see [`ImageBlobCache`]):
    ///
    /// - a digest reference already held is served with no registry request;
    /// - a tag costs one `HEAD` to learn its current digest, then nothing
    ///   more when the node holds that digest's blobs;
    /// - a tag that moved downloads the new manifest and only the layers the
    ///   node does not hold yet.
    fn fetch_image_layout(
        &self,
        req: &FetchRequest<'_>,
        sink: &mut ArtifactSink,
        runtime: &tokio::runtime::Runtime,
        client: &Client,
        reference: &Reference,
        auth: &RegistryAuth,
    ) -> R<FetchedArtifact> {
        let store = sink.content_store().cloned();
        let cache = store.as_deref().map(|store| ImageBlobCache { store });
        let requested_at = std::time::Instant::now();

        // The digest the reference names: given, or (with a cache to serve
        // it from) a tag's current digest, shared with any pull of the same
        // tag whose request went out after this one arrived.
        let top_digest = match (reference.digest(), cache.as_ref()) {
            (Some(digest), _) => Some(digest.to_string()),
            (None, Some(cache)) => Some(resolve_tag_shared(
                &tag_resolution_key(reference, auth),
                requested_at,
                || cache.resolve_tag(runtime, client, reference, auth, req.redacted_source),
            )?),
            (None, None) => None,
        };
        // An image already whole on the node (every start but its first)
        // is laid out with no claim: a burst of starts of one image used to
        // assemble their layouts one at a time behind the claim, which on a
        // 96-vCPU node cost the thousandth start of a burst over 5 s. Only a
        // pull with something to download takes the claim, so concurrent
        // starts still never download the same blob twice.
        let whole = match (cache.as_ref(), top_digest.as_deref()) {
            (Some(cache), Some(digest)) => cache.whole_image(digest),
            _ => None,
        };
        let _claim = match (&whole, cache.as_ref()) {
            (None, Some(_)) => Some(ImagePullClaim::claim(&reference.whole())),
            _ => None,
        };
        let (raw_manifest, manifest_digest, manifest_cached) = match whole {
            Some((raw, digest)) => (raw, digest, true),
            None => self.platform_manifest(
                req,
                runtime,
                client,
                reference,
                auth,
                top_digest.as_deref(),
                cache.as_ref(),
            )?,
        };
        let manifest: OciImageManifest = serde_json::from_slice(&raw_manifest)
            .with_context(|| format!("parse image manifest for {}", req.redacted_source))?;

        let mut descriptors: Vec<&OciDescriptor> = vec![&manifest.config];
        descriptors.extend(manifest.layers.iter());
        let total = check_size_budget(req, &descriptors)?;
        sink.set_bytes_total(Some(total));

        let layout_root = sink.dir().join("layout");
        std::fs::create_dir_all(&layout_root)?;
        let manifest_path = blob_path(&layout_root, &manifest_digest)?;
        if let Some(parent) = manifest_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&manifest_path, &raw_manifest)?;

        let (mut cached, mut pulled) = (0usize, 0usize);
        for descriptor in descriptors {
            req.limits.check_deadline(req.redacted_source)?;
            let path = blob_path(&layout_root, &descriptor.digest)?;
            if cache
                .as_ref()
                .is_some_and(|cache| cache.place(&descriptor.digest, &path))
            {
                cached += 1;
            } else {
                runtime.block_on(pull_blob_to_file(client, reference, descriptor, &path))?;
                if let Some(cache) = cache.as_ref() {
                    cache.keep_file(&descriptor.digest, &path);
                }
                pulled += 1;
            }
            sink.record_external_bytes(descriptor.size.max(0) as u64)?;
        }
        if cache.is_some() {
            info!(
                image = %req.redacted_source,
                digest = %manifest_digest,
                blobs_cached = cached,
                blobs_pulled = pulled,
                manifest_cached,
                "Image layout ready (node content store)"
            );
        }

        let manifest_media_type = manifest
            .media_type
            .clone()
            .unwrap_or_else(|| oci_client::manifest::OCI_IMAGE_MEDIA_TYPE.to_string());
        let index = json!({
            "schemaVersion": 2,
            "manifests": [{
                "mediaType": manifest_media_type,
                "digest": manifest_digest,
                "size": raw_manifest.len(),
                "annotations": {
                    "org.opencontainers.image.ref.name": reference.to_string(),
                },
            }],
        });
        std::fs::write(layout_root.join("index.json"), serde_json::to_vec(&index)?)?;
        std::fs::write(
            layout_root.join("oci-layout"),
            br#"{"imageLayoutVersion":"1.0.0"}"#,
        )?;

        Ok(FetchedArtifact {
            bytes: sink.bytes_written(),
            payload: FetchedPayload::Image {
                oci_layout: layout_root,
                reference: reference.to_string(),
                digest: manifest_digest,
            },
        })
    }

    /// The `linux/<arch>` image manifest for `reference`, byte-exact (the
    /// layout stores it under its digest, which only the original bytes
    /// reproduce), with its digest and whether the store served it.
    ///
    /// `top_digest` is what the reference names (a manifest or an index).
    /// The store is tried first; the registry is asked for whatever it does
    /// not hold, and what the registry sends is kept for next time.
    #[allow(clippy::too_many_arguments)]
    fn platform_manifest(
        &self,
        req: &FetchRequest<'_>,
        runtime: &tokio::runtime::Runtime,
        client: &Client,
        reference: &Reference,
        auth: &RegistryAuth,
        top_digest: Option<&str>,
        cache: Option<&ImageBlobCache<'_>>,
    ) -> R<(Vec<u8>, String, bool)> {
        let pull = |target: &Reference| -> R<(Vec<u8>, String)> {
            let (raw, digest) = runtime
                .block_on(client.pull_manifest_raw(target, auth, MANIFEST_MEDIA_TYPES))
                .with_context(|| format!("pull manifest for {}", req.redacted_source))?;
            if let Some(cache) = cache {
                cache.keep_bytes(&digest, &raw);
            }
            Ok((raw, digest))
        };

        // The top-level manifest: cached, else fetched (by digest when known,
        // so the bytes match what the HEAD reported).
        let cached_top = match (cache, top_digest) {
            (Some(cache), Some(digest)) => cache.read(digest).map(|raw| (raw, digest.to_string())),
            _ => None,
        };
        let top_from_cache = cached_top.is_some();
        let (raw, digest) = match cached_top {
            Some(hit) => hit,
            None => pull(&match top_digest {
                Some(digest) => reference.clone_with_digest(digest.to_string()),
                None => reference.clone(),
            })?,
        };

        let parsed: OciManifest = serde_json::from_slice(&raw)
            .with_context(|| format!("parse manifest for {}", req.redacted_source))?;
        let index = match parsed {
            OciManifest::Image(_) => return Ok((raw, digest, top_from_cache)),
            OciManifest::ImageIndex(index) => index,
        };
        let platform_digest = linux_platform_resolver(&index.manifests).with_context(|| {
            format!(
                "{} has no linux/{} image in its index",
                req.redacted_source,
                image_arch()
            )
        })?;
        if let Some(raw) = cache.and_then(|cache| cache.read(&platform_digest)) {
            return Ok((raw, platform_digest, top_from_cache));
        }
        let (raw, digest) = pull(&reference.clone_with_digest(platform_digest))?;
        Ok((raw, digest, false))
    }
}

/// ORAS titles can carry paths; keep only a safe basename.
fn sanitize_title(title: &str) -> String {
    std::path::Path::new(title)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .filter(|name| !name.is_empty() && name != ".." && name != ".")
        .unwrap_or_else(|| "layer".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A burst of pulls of one tag sends one resolution. A pull that asked
    /// after that request went out never takes its answer: it gets a newer
    /// one.
    #[test]
    fn concurrent_pulls_of_a_tag_share_one_resolution_test() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        let key = format!("test-single-flight-{}", std::process::id());
        let sent = Arc::new(AtomicUsize::new(0));
        let asked = std::time::Instant::now();
        let threads: Vec<_> = (0..16)
            .map(|_| {
                let (key, sent) = (key.clone(), sent.clone());
                std::thread::spawn(move || {
                    resolve_tag_shared(&key, asked, || {
                        sent.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_millis(50));
                        Ok("sha256:aaaa".to_string())
                    })
                })
            })
            .collect();
        for thread in threads {
            assert_eq!(thread.join().unwrap().unwrap(), "sha256:aaaa");
        }
        assert_eq!(sent.load(Ordering::SeqCst), 1, "one request for the burst");

        // A pull that asks after that answer was sent resolves again.
        let later = std::time::Instant::now();
        let digest = resolve_tag_shared(&key, later, || {
            sent.fetch_add(1, Ordering::SeqCst);
            Ok("sha256:bbbb".to_string())
        })
        .unwrap();
        assert_eq!(digest, "sha256:bbbb");
        assert_eq!(sent.load(Ordering::SeqCst), 2);
    }

    /// Shared registry clients are keyed by credential: one job's token
    /// cache never serves another's pull.
    #[test]
    fn registry_clients_are_never_shared_across_credentials_test() {
        let basic = |user: &str, password: &str| {
            credential_fingerprint(&RegistryAuth::Basic(user.into(), password.into()))
        };
        assert_eq!(basic("ci", "s3cret"), basic("ci", "s3cret"));
        assert_ne!(basic("ci", "s3cret"), basic("ci", "other"));
        assert_ne!(basic("ci", "s3cret"), basic("cis", "3cret"));
        assert_ne!(
            credential_fingerprint(&RegistryAuth::Anonymous),
            credential_fingerprint(&RegistryAuth::Bearer("anonymous".into()))
        );
    }
    use crate::artifacts::fetcher::FetchLimits;
    use crate::structs::parse_artifact_source;

    fn request<'a>(
        parsed: &'a crate::structs::ParsedSource,
        limits: &'a FetchLimits,
        source: &'a str,
    ) -> FetchRequest<'a> {
        FetchRequest {
            scheme: &parsed.scheme,
            url: &parsed.url,
            redacted_source: source,
            checksum: None,
            media_type: None,
            credential: None,
            limits,
        }
    }

    #[test]
    fn parses_references_with_tags_digests_and_ports() -> R<()> {
        let limits = FetchLimits::default();

        let source = "oci://ghcr.io/acme/api:v42";
        let parsed = parse_artifact_source(source)?;
        let (reference, protocol) = parse_reference(&request(&parsed, &limits, source))?;
        assert_eq!(reference.registry(), "ghcr.io");
        assert_eq!(reference.repository(), "acme/api");
        assert_eq!(reference.tag(), Some("v42"));
        assert_eq!(protocol, ClientProtocol::Https);

        let digest_source = format!(
            "oci://registry.acme.io/models/embedder@sha256:{}",
            "a".repeat(64)
        );
        let parsed = parse_artifact_source(&digest_source)?;
        let (reference, _) = parse_reference(&request(&parsed, &limits, &digest_source))?;
        assert!(reference.digest().is_some());

        let local = "oci://localhost:5000/app:v1?protocol=http";
        let parsed = parse_artifact_source(local)?;
        let (reference, protocol) = parse_reference(&request(&parsed, &limits, local))?;
        assert_eq!(reference.registry(), "localhost:5000");
        assert_eq!(protocol, ClientProtocol::Http);
        Ok(())
    }

    /// A pull that queued behind another reuses the tag digest that pull
    /// asked for after it arrived, and a pull that arrives later asks again.
    #[test]
    fn queued_pulls_share_a_tag_resolution_sent_after_they_arrived() -> R<()> {
        let reference: Reference = "registry.test/shared/img:v1".parse()?;
        let key = tag_resolution_key(&reference, &RegistryAuth::Anonymous);
        let asked = std::sync::atomic::AtomicUsize::new(0);
        let resolve = |digest: &'static str| {
            let asked = &asked;
            move || {
                asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(digest.to_string())
            }
        };

        // Two pulls arrive; the first asks the registry once its turn
        // comes, which is after the second arrived.
        let first_arrived = std::time::Instant::now();
        let second_arrived = std::time::Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert_eq!(
            resolve_tag_shared(&key, first_arrived, resolve("sha256:a"))?,
            "sha256:a"
        );
        assert_eq!(
            resolve_tag_shared(&key, second_arrived, resolve("sha256:b"))?,
            "sha256:a"
        );
        assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 1);

        // A pull that arrives after that answer was sent asks again, and
        // sees the tag move.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let later = std::time::Instant::now();
        assert_eq!(
            resolve_tag_shared(&key, later, resolve("sha256:b"))?,
            "sha256:b"
        );
        assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 2);

        // Another credential never shares an answer.
        let other = tag_resolution_key(
            &reference,
            &RegistryAuth::Basic("user".into(), "secret".into()),
        );
        assert_ne!(key, other);
        assert_eq!(
            resolve_tag_shared(&other, later, resolve("sha256:c"))?,
            "sha256:c"
        );
        assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 3);
        Ok(())
    }

    #[test]
    fn digest_pinned_references_skip_resolution() -> R<()> {
        let limits = FetchLimits::default();
        let source = format!("oci://ghcr.io/acme/api@sha256:{}", "a".repeat(64));
        let parsed = parse_artifact_source(&source)?;
        // No network: digest-pinned returns Ok(None) before any client call.
        let resolved = OciFetcher::new().resolve(&request(&parsed, &limits, &source))?;
        assert!(resolved.is_none());
        Ok(())
    }

    #[test]
    fn title_sanitization() {
        assert_eq!(sanitize_title("weights.bin"), "weights.bin");
        assert_eq!(sanitize_title("dir/weights.bin"), "weights.bin");
        assert_eq!(sanitize_title("../../escape"), "escape");
        assert_eq!(sanitize_title(".."), "layer");
        assert_eq!(sanitize_title(""), "layer");
    }

    /// Request path -> (content type, body).
    type Routes = Arc<std::sync::Mutex<HashMap<String, (String, Vec<u8>)>>>;

    /// A registry small enough to read: `GET /v2/`, manifests by tag or
    /// digest (`HEAD` and `GET`), and blobs, over plain HTTP with one request
    /// per connection. Every request is logged as `"<METHOD> <path>"`, and
    /// `down` makes it answer 503 to everything, like a rate-limited or
    /// failing registry.
    struct FakeRegistry {
        port: u16,
        /// path -> (content type, body); manifests are keyed by both tag
        /// and digest.
        routes: Routes,
        log: Arc<std::sync::Mutex<Vec<String>>>,
        down: Arc<std::sync::atomic::AtomicBool>,
    }

    impl FakeRegistry {
        fn start() -> Self {
            use std::io::{BufRead, Write};
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            let port = listener.local_addr().expect("addr").port();
            let routes: Routes = Arc::default();
            let log: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
            let down = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (thread_routes, thread_log, thread_down) =
                (routes.clone(), log.clone(), down.clone());
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { continue };
                    let mut reader = std::io::BufReader::new(stream.try_clone().expect("clone"));
                    let mut request_line = String::new();
                    if reader.read_line(&mut request_line).is_err() {
                        continue;
                    }
                    loop {
                        let mut header = String::new();
                        if reader.read_line(&mut header).is_err() || header.trim().is_empty() {
                            break;
                        }
                    }
                    let mut parts = request_line.split_whitespace();
                    let method = parts.next().unwrap_or_default().to_string();
                    let path = parts.next().unwrap_or_default().to_string();
                    thread_log.lock().unwrap().push(format!("{method} {path}"));
                    let found = if thread_down.load(std::sync::atomic::Ordering::SeqCst) {
                        None
                    } else if path == "/v2/" {
                        Some(("application/json".to_string(), b"{}".to_vec()))
                    } else {
                        thread_routes.lock().unwrap().get(&path).cloned()
                    };
                    let response = match found {
                        Some((content_type, body)) => {
                            let digest = ContentDigest::of_bytes(&body);
                            let mut head = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\n\
                                 Docker-Content-Digest: {}\r\nContent-Length: {}\r\n\
                                 Connection: close\r\n\r\n",
                                digest.as_str(),
                                body.len()
                            )
                            .into_bytes();
                            if method != "HEAD" {
                                head.extend_from_slice(&body);
                            }
                            head
                        }
                        None => {
                            let status = if thread_down.load(std::sync::atomic::Ordering::SeqCst) {
                                "503 Service Unavailable"
                            } else {
                                "404 Not Found"
                            };
                            format!(
                                "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            )
                            .into_bytes()
                        }
                    };
                    let _ = stream.write_all(&response);
                }
            });
            FakeRegistry {
                port,
                routes,
                log,
                down,
            }
        }

        fn serve(&self, path: String, content_type: &str, body: Vec<u8>) {
            self.routes
                .lock()
                .unwrap()
                .insert(path, (content_type.to_string(), body));
        }

        /// Publishes `lib/img:<tag>` as a one-platform index over a manifest
        /// with a config and the given layers, returning the index digest.
        fn publish(&self, tag: &str, config: &[u8], layers: &[&[u8]]) -> String {
            let descriptor = |media_type: &str, bytes: &[u8]| {
                json!({
                    "mediaType": media_type,
                    "digest": ContentDigest::of_bytes(bytes).as_str(),
                    "size": bytes.len(),
                })
            };
            let manifest = serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": oci_client::manifest::OCI_IMAGE_MEDIA_TYPE,
                "config": descriptor(oci_client::manifest::IMAGE_CONFIG_MEDIA_TYPE, config),
                "layers": layers
                    .iter()
                    .map(|layer| descriptor(oci_client::manifest::IMAGE_LAYER_GZIP_MEDIA_TYPE, layer))
                    .collect::<Vec<_>>(),
            }))
            .unwrap();
            let mut platform_entry =
                descriptor(oci_client::manifest::OCI_IMAGE_MEDIA_TYPE, &manifest);
            platform_entry["platform"] = json!({ "os": "linux", "architecture": image_arch() });
            let index = serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": oci_client::manifest::OCI_IMAGE_INDEX_MEDIA_TYPE,
                "manifests": [platform_entry],
            }))
            .unwrap();
            let index_digest = ContentDigest::of_bytes(&index).as_str().to_string();
            let manifest_digest = ContentDigest::of_bytes(&manifest).as_str().to_string();
            for blob in std::iter::once(config).chain(layers.iter().copied()) {
                self.serve(
                    format!(
                        "/v2/lib/img/blobs/{}",
                        ContentDigest::of_bytes(blob).as_str()
                    ),
                    "application/octet-stream",
                    blob.to_vec(),
                );
            }
            self.serve(
                format!("/v2/lib/img/manifests/{manifest_digest}"),
                oci_client::manifest::OCI_IMAGE_MEDIA_TYPE,
                manifest,
            );
            for name in [tag.to_string(), index_digest.clone()] {
                self.serve(
                    format!("/v2/lib/img/manifests/{name}"),
                    oci_client::manifest::OCI_IMAGE_INDEX_MEDIA_TYPE,
                    index.clone(),
                );
            }
            index_digest
        }

        /// Requests logged since the last call.
        fn take_log(&self) -> Vec<String> {
            std::mem::take(&mut *self.log.lock().unwrap())
        }
    }

    fn open_store(dir: &std::path::Path) -> Arc<ContentStore> {
        Arc::new(
            ContentStore::open(
                dir.join("content"),
                crate::content_store::ContentStoreConfig {
                    min_free_disk: 0,
                    ..Default::default()
                },
            )
            .expect("open content store"),
        )
    }

    /// Pulls `source` into a fresh directory under `dir`, through `store`.
    fn pull(
        dir: &std::path::Path,
        name: &str,
        source: &str,
        store: Option<Arc<ContentStore>>,
    ) -> R<std::path::PathBuf> {
        let parsed = parse_artifact_source(source)?;
        let limits = FetchLimits::default();
        let mut sink = ArtifactSink::new(dir.join(name), limits.max_bytes)?;
        sink.set_content_store(store);
        let fetched = OciFetcher::new().fetch(&request(&parsed, &limits, source), &mut sink)?;
        match fetched.payload {
            FetchedPayload::Image { oci_layout, .. } => Ok(oci_layout),
            other => panic!("expected an image, got {other:?}"),
        }
    }

    fn blob_pulls(log: &[String]) -> Vec<&String> {
        log.iter()
            .filter(|line| line.starts_with("GET") && line.contains("/blobs/"))
            .collect()
    }

    fn manifest_gets(log: &[String]) -> Vec<&String> {
        log.iter()
            .filter(|line| line.starts_with("GET") && line.contains("/manifests/"))
            .collect()
    }

    /// P2-9: every start pulled the image again, and anonymous registries
    /// rate-limit that. With a node content store, a repeated start of the
    /// same tag costs one HEAD, a registry that stops answering does not stop
    /// a cached image from starting, and a moved tag downloads only the
    /// layers the node does not hold.
    #[test]
    fn repeated_pulls_are_served_from_the_node_content_store() -> R<()> {
        let registry = FakeRegistry::start();
        let dir = tempfile::tempdir()?;
        let store = open_store(dir.path());
        let source = format!("oci://127.0.0.1:{}/lib/img:v1?protocol=http", registry.port);
        let (base, app_v1, app_v2) = (
            b"base-layer".as_slice(),
            b"app-v1".as_slice(),
            b"app-v2".as_slice(),
        );
        registry.publish("v1", br#"{"config":{"Env":["V=1"]}}"#, &[base, app_v1]);

        // First pull: everything comes from the registry and is kept.
        let layout = pull(dir.path(), "first", &source, Some(store.clone()))?;
        assert!(layout.join("index.json").is_file());
        let log = registry.take_log();
        assert_eq!(blob_pulls(&log).len(), 3, "config + 2 layers: {log:?}");
        assert_eq!(
            manifest_gets(&log).len(),
            2,
            "index + platform manifest: {log:?}"
        );
        assert_eq!(store.list()?.len(), 5, "index, manifest, config, 2 layers");
        let layer = layout
            .join("blobs/sha256")
            .join(ContentDigest::of_bytes(app_v1).hex());
        assert_eq!(std::fs::read(&layer)?, app_v1);

        // Second pull of the same tag: one HEAD, no manifest or blob GETs.
        let again = pull(dir.path(), "second", &source, Some(store.clone()))?;
        let log = registry.take_log();
        assert!(
            log.iter()
                .any(|line| line.starts_with("HEAD") && line.ends_with("/manifests/v1")),
            "{log:?}"
        );
        assert!(manifest_gets(&log).is_empty(), "{log:?}");
        assert!(blob_pulls(&log).is_empty(), "{log:?}");
        assert_eq!(
            std::fs::read(again.join("index.json"))?,
            std::fs::read(layout.join("index.json"))?
        );

        // The registry stops answering: the tag's last digest on this node
        // still starts.
        registry
            .down
            .store(true, std::sync::atomic::Ordering::SeqCst);
        pull(dir.path(), "offline", &source, Some(store.clone()))?;
        assert!(blob_pulls(&registry.take_log()).is_empty());
        registry
            .down
            .store(false, std::sync::atomic::Ordering::SeqCst);

        // The tag moves: the new manifests and the changed layer are pulled,
        // the shared base layer is not.
        registry.publish("v1", br#"{"config":{"Env":["V=2"]}}"#, &[base, app_v2]);
        let moved = pull(dir.path(), "moved", &source, Some(store.clone()))?;
        let log = registry.take_log();
        let pulled = blob_pulls(&log);
        assert_eq!(pulled.len(), 2, "new config + changed layer only: {log:?}");
        assert!(
            !pulled
                .iter()
                .any(|line| line.contains(ContentDigest::of_bytes(base).hex())),
            "{log:?}"
        );
        assert!(moved
            .join("blobs/sha256")
            .join(ContentDigest::of_bytes(app_v2).hex())
            .is_file());
        Ok(())
    }

    /// A digest reference the node holds needs no registry at all, and a
    /// node without a store pulls everything, as before.
    #[test]
    fn digest_references_and_storeless_pulls() -> R<()> {
        let registry = FakeRegistry::start();
        let dir = tempfile::tempdir()?;
        let store = open_store(dir.path());
        let index = registry.publish("v1", br#"{"config":{}}"#, &[b"only-layer"]);
        let pinned = format!(
            "oci://127.0.0.1:{}/lib/img@{index}?protocol=http",
            registry.port
        );

        pull(dir.path(), "warm", &pinned, Some(store.clone()))?;
        registry.take_log();
        registry
            .down
            .store(true, std::sync::atomic::Ordering::SeqCst);
        pull(dir.path(), "hit", &pinned, Some(store.clone()))?;
        let log = registry.take_log();
        assert!(
            log.iter().all(|line| line == "GET /v2/"),
            "a held digest asks the registry for nothing: {log:?}"
        );
        registry
            .down
            .store(false, std::sync::atomic::Ordering::SeqCst);

        // No store: the full pull, every time.
        let tag = format!("oci://127.0.0.1:{}/lib/img:v1?protocol=http", registry.port);
        pull(dir.path(), "plain-1", &tag, None)?;
        pull(dir.path(), "plain-2", &tag, None)?;
        assert_eq!(blob_pulls(&registry.take_log()).len(), 4);
        Ok(())
    }

    #[test]
    fn blob_paths_are_digest_addressed() -> R<()> {
        let root = std::path::Path::new("/layout");
        let path = blob_path(root, &format!("sha256:{}", "b".repeat(64)))?;
        assert_eq!(path, root.join("blobs").join("sha256").join("b".repeat(64)));
        assert!(blob_path(root, "garbage").is_err());
        assert!(blob_path(root, "sha256:../escape").is_err());
        Ok(())
    }

    /// End-to-end pull of a public image into a valid on-disk OCI layout.
    /// Ignored by default (needs network + registry access). Override the
    /// reference with `LEGION_OCI_TEST_IMAGE` (e.g. to a Docker Hub mirror when
    /// docker.io anonymous pulls are rate-limited).
    ///
    /// Run with: `cargo test --lib fetch_public_image -- --ignored --nocapture`
    #[test]
    #[ignore = "network: pulls a public image from a registry"]
    fn fetch_public_image_produces_oci_layout() -> R<()> {
        let source = std::env::var("LEGION_OCI_TEST_IMAGE")
            .unwrap_or_else(|_| "oci://docker.io/library/hello-world:latest".to_string());
        let parsed = parse_artifact_source(&source)?;
        let limits = FetchLimits::default();
        let req = request(&parsed, &limits, &source);

        let staging = tempfile::tempdir()?;
        let mut sink = ArtifactSink::new(staging.path().to_path_buf(), limits.max_bytes)?;
        let fetched = OciFetcher::new().fetch(&req, &mut sink)?;

        match &fetched.payload {
            FetchedPayload::Image {
                oci_layout, digest, ..
            } => {
                assert!(oci_layout.join("oci-layout").is_file(), "oci-layout marker");
                assert!(oci_layout.join("index.json").is_file(), "index.json");
                assert!(oci_layout.join("blobs").is_dir(), "blobs/ dir");
                assert!(digest.starts_with("sha256:"), "digest {digest}");
                let (_, hex) = digest.split_once(':').expect("digest has algo");
                assert!(
                    oci_layout.join("blobs").join("sha256").join(hex).is_file(),
                    "manifest blob present at its digest"
                );
            }
            other => panic!("expected an Image payload, got {other:?}"),
        }
        assert!(fetched.bytes > 0, "fetched non-zero bytes");
        Ok(())
    }
}
