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
use oci_client::manifest::{OciDescriptor, OciImageManifest};
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};

use crate::artifacts::fetcher::{
    ArtifactFetcher, Credential, FetchRequest, FetchedArtifact, FetchedPayload, ResolvedIdentity,
};
use crate::artifacts::stage::ArtifactSink;
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

fn oci_client(protocol: ClientProtocol) -> Client {
    Client::new(ClientConfig {
        protocol,
        connect_timeout: Some(std::time::Duration::from_secs(30)),
        ..ClientConfig::default()
    })
}

fn blocking_runtime() -> R<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build oci fetch runtime")
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
        let client = oci_client(protocol.clone());
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
        let client = oci_client(protocol);
        let runtime = blocking_runtime()?;

        // Resolve (possibly via an image index) to one platform manifest.
        let (manifest, manifest_digest) = runtime
            .block_on(client.pull_image_manifest(&reference, &auth))
            .with_context(|| format!("pull manifest for {}", req.redacted_source))?;

        match req.media_type {
            Some(media_type) => self.fetch_oras_layers(
                req,
                sink,
                &runtime,
                &client,
                &reference,
                manifest,
                manifest_digest,
                media_type,
            ),
            None => self.fetch_image_layout(
                req,
                sink,
                &runtime,
                &client,
                &reference,
                &auth,
                manifest,
                manifest_digest,
            ),
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
    /// (`oci-layout`, `index.json`, `blobs/`) that image-consuming kinds
    /// hand to their runtime.
    #[allow(clippy::too_many_arguments)]
    fn fetch_image_layout(
        &self,
        req: &FetchRequest<'_>,
        sink: &mut ArtifactSink,
        runtime: &tokio::runtime::Runtime,
        client: &Client,
        reference: &Reference,
        auth: &RegistryAuth,
        manifest: OciImageManifest,
        manifest_digest: String,
    ) -> R<FetchedArtifact> {
        let mut descriptors: Vec<&OciDescriptor> = vec![&manifest.config];
        descriptors.extend(manifest.layers.iter());
        let total = check_size_budget(req, &descriptors)?;
        sink.set_bytes_total(Some(total));

        let layout_root = sink.dir().join("layout");
        std::fs::create_dir_all(&layout_root)?;

        // Byte-exact manifest content for the digest-addressed blob: re-pull
        // raw at the pinned digest (serializing the parsed struct would not
        // reproduce the original bytes).
        let pinned = reference.clone_with_digest(manifest_digest.clone());
        let (raw_manifest, _) = runtime
            .block_on(client.pull_manifest_raw(
                &pinned,
                auth,
                &[
                    oci_client::manifest::IMAGE_MANIFEST_MEDIA_TYPE,
                    oci_client::manifest::OCI_IMAGE_MEDIA_TYPE,
                ],
            ))
            .with_context(|| format!("pull raw manifest for {}", req.redacted_source))?;
        let manifest_path = blob_path(&layout_root, &manifest_digest)?;
        if let Some(parent) = manifest_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&manifest_path, &raw_manifest)?;

        for descriptor in descriptors {
            req.limits.check_deadline(req.redacted_source)?;
            let path = blob_path(&layout_root, &descriptor.digest)?;
            runtime.block_on(pull_blob_to_file(client, reference, descriptor, &path))?;
            sink.record_external_bytes(descriptor.size.max(0) as u64)?;
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
