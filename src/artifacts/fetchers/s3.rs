//! `s3://` fetcher: `object_store`-backed, speaking any S3-compatible
//! endpoint (AWS, MinIO, R2, GCS interop). Region/endpoint come from the
//! credential object or query params
//! (`s3://bucket/key?endpoint=https://minio.internal:9000&region=auto`),
//! query params winning. The store is opened through the object-storage
//! layer ([`crate::objects`]), like every other object store.

use std::io::Write;

use futures::StreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};

use crate::artifacts::fetcher::{ArtifactFetcher, FetchRequest, FetchedArtifact, FetchedPayload};
use crate::artifacts::stage::ArtifactSink;
use crate::objects::{
    self, ObjectCredential, OpenOptions, RegionDefault, StoreUrl, FALLBACK_REGION,
};
use crate::prelude::*;

#[derive(Debug, Default)]
pub struct S3Fetcher;

impl S3Fetcher {
    pub fn new() -> Self {
        Self
    }
}

struct S3Target {
    store: Arc<dyn ObjectStore>,
    key: ObjectPath,
    file_name: String,
}

impl std::fmt::Debug for S3Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Target")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

fn build_target(req: &FetchRequest<'_>) -> R<S3Target> {
    if req.url.host_str().unwrap_or_default().is_empty() {
        bail!("{} has no bucket host", req.redacted_source);
    }
    // A region is a DNS label of the default AWS endpoint
    // (`s3.<region>.amazonaws.com`), so anything else in it could change
    // the host the client dials.
    let query_region = req
        .url
        .query_pairs()
        .find(|(name, _)| name == "region")
        .map(|(_, value)| value.into_owned());
    let credential_region = match req.credential {
        Some(crate::artifacts::fetcher::Credential::Aws { region, .. }) => region.clone(),
        _ => None,
    };
    for region in [query_region, credential_region].into_iter().flatten() {
        if !valid_region(&region) {
            bail!(
                "{} names region {region:?}; a region is lowercase letters, digits, and \
                 hyphens",
                req.redacted_source
            );
        }
    }
    // A custom endpoint is a host the job chose, judged like any other fetch
    // destination: the `endpoint` query parameter, else the one the KV
    // credential names (the order the client itself applies). The default
    // AWS endpoints are left alone.
    if let Some(guard) = req.limits.egress.as_deref() {
        let endpoint = req
            .url
            .query_pairs()
            .find(|(name, _)| name == "endpoint")
            .map(|(_, value)| value.into_owned())
            .filter(|endpoint| !endpoint.trim().is_empty())
            .or_else(|| match req.credential {
                Some(crate::artifacts::fetcher::Credential::Aws { endpoint, .. }) => endpoint
                    .clone()
                    .filter(|endpoint| !endpoint.trim().is_empty()),
                _ => None,
            });
        if let Some(endpoint) = endpoint {
            let endpoint = url::Url::parse(&endpoint)
                .with_context(|| format!("{} has an invalid endpoint", req.redacted_source))?;
            let host = endpoint
                .host()
                .with_context(|| format!("{} endpoint names no host", req.redacted_source))?;
            let port = endpoint.port_or_known_default().unwrap_or(443);
            crate::artifacts::fetchers::http::judged_addrs(
                guard,
                &host,
                port,
                req.redacted_source,
            )?;
        }
    }
    let url = StoreUrl::from_url(req.url.clone())
        .with_context(|| format!("invalid s3 source {}", req.redacted_source))?;
    let key = url.key("")?;
    if key.as_ref().is_empty() {
        bail!("{} has no object key", req.redacted_source);
    }

    let credential = match req.credential {
        Some(credential) => ObjectCredential::from_artifact(
            credential,
            &format!("s3 artifact {}", req.redacted_source),
        )?,
        // Anonymous: public buckets are read without signing.
        None => ObjectCredential::Anonymous,
    };
    let options = OpenOptions {
        // The ambient chain keeps the node's environment region, as the
        // contentStore publisher does, so a node reads its store in the
        // region it publishes to. A jobspec artifact (a KV credential, or
        // none) resolves the same on every node.
        region_default: match credential {
            ObjectCredential::Ambient => RegionDefault::Environment,
            _ => RegionDefault::Fixed(FALLBACK_REGION),
        },
        ..OpenOptions::default()
    };

    let file_name = req
        .url
        .path()
        .trim_start_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("object")
        .to_string();
    Ok(S3Target {
        store: objects::open(&url, &credential, &options)
            .with_context(|| format!("build s3 client for {}", req.redacted_source))?,
        key,
        file_name,
    })
}

/// Whether `region` can only ever be a region: a non-empty run of
/// lowercase letters, digits, and hyphens.
fn valid_region(region: &str) -> bool {
    !region.is_empty()
        && region
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

impl ArtifactFetcher for S3Fetcher {
    fn schemes(&self) -> &'static [&'static str] {
        &["s3"]
    }

    fn fetch(&self, req: &FetchRequest<'_>, sink: &mut ArtifactSink) -> R<FetchedArtifact> {
        let target = build_target(req)?;

        // Fetchers are blocking by contract; object_store is async, so run
        // it on a private current-thread runtime inside this staging thread.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build s3 fetch runtime")?;

        let mut writer = sink.file_writer(&target.file_name)?;
        let total = runtime.block_on(async {
            let result = target
                .store
                .get(&target.key)
                .await
                .with_context(|| format!("get {}", req.redacted_source))?;
            let size = result.meta.size as u64;
            let mut stream = result.into_stream();
            let mut written = 0u64;
            while let Some(chunk) = stream.next().await {
                req.limits.check_deadline(req.redacted_source)?;
                let chunk = chunk.with_context(|| format!("read {}", req.redacted_source))?;
                writer
                    .write_all(&chunk)
                    .with_context(|| format!("write fetched bytes for {}", req.redacted_source))?;
                written += chunk.len() as u64;
            }
            Ok::<(u64, u64), anyhow::Error>((size, written))
        })?;
        let _ = total;
        let (path, digest, bytes) = writer.finish()?;
        Ok(FetchedArtifact {
            bytes,
            payload: FetchedPayload::File { path, digest },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::fetcher::{Credential, FetchLimits};
    use crate::structs::parse_artifact_source;

    #[test]
    fn query_params_override_credential_endpoint() -> R<()> {
        let source =
            "s3://acme-builds/api/v42.tar.gz?endpoint=http://minio.internal:9000&region=auto";
        let parsed = parse_artifact_source(source)?;
        let limits = FetchLimits::default();
        let credential = Credential::Aws {
            access_key_id: "AK".into(),
            secret_access_key: "SK".into(),
            session_token: None,
            region: Some("us-east-1".into()),
            endpoint: Some("https://other.example.com".into()),
        };
        let request = FetchRequest {
            scheme: &parsed.scheme,
            url: &parsed.url,
            redacted_source: source,
            checksum: None,
            media_type: None,
            credential: Some(&credential),
            limits: &limits,
        };
        // Building the target validates bucket/key/credential plumbing
        // without any network traffic.
        let target = build_target(&request)?;
        assert_eq!(target.file_name, "v42.tar.gz");
        assert_eq!(target.key.as_ref(), "api/v42.tar.gz");
        Ok(())
    }

    /// Regression (TODO "an `s3://` `endpoint` is judged before the fetch
    /// but ..."): the endpoint a KV credential names is judged like a query
    /// `endpoint`, and a region that is not a region is refused before it
    /// can become part of a host name.
    #[test]
    fn the_host_guard_judges_the_credential_endpoint_and_regions_test() -> R<()> {
        use crate::structs::host_guard::{HostGuard, HostGuardConfig};
        let limits = FetchLimits {
            egress: Some(std::sync::Arc::new(HostGuard::new(
                &HostGuardConfig::default(),
            )?)),
            ..FetchLimits::default()
        };
        let build = |source: &str, credential: Option<&Credential>| -> R<S3Target> {
            let parsed = parse_artifact_source(source)?;
            build_target(&FetchRequest {
                scheme: &parsed.scheme,
                url: &parsed.url,
                redacted_source: source,
                checksum: None,
                media_type: None,
                credential,
                limits: &limits,
            })
        };
        let aws = |region: &str, endpoint: Option<&str>| Credential::Aws {
            access_key_id: "AK".into(),
            secret_access_key: "SK".into(),
            session_token: None,
            region: Some(region.into()),
            endpoint: endpoint.map(str::to_string),
        };
        let metadata = aws("us-east-1", Some("http://169.254.169.254"));
        let err = build("s3://bucket/key", Some(&metadata))
            .expect_err("the credential's endpoint is judged");
        assert!(format!("{err:#}").contains("host guard refuses"), "{err:#}");

        let err =
            build("s3://bucket/key?region=x@10.0.0.5/", None).expect_err("a region is a DNS label");
        assert!(format!("{err:#}").contains("names region"), "{err:#}");
        let err = build("s3://bucket/key", Some(&aws("US East", None)))
            .expect_err("so is the credential's");
        assert!(format!("{err:#}").contains("names region"), "{err:#}");

        build(
            "s3://bucket/key?region=eu-west-1",
            Some(&aws("us-east-1", None)),
        )?;
        Ok(())
    }

    #[test]
    fn missing_key_rejected() {
        let source = "s3://bucket-only";
        let parsed = parse_artifact_source(source).expect("parse");
        let limits = FetchLimits::default();
        let request = FetchRequest {
            scheme: &parsed.scheme,
            url: &parsed.url,
            redacted_source: source,
            checksum: None,
            media_type: None,
            credential: None,
            limits: &limits,
        };
        assert!(build_target(&request).is_err());
    }

    #[test]
    fn non_aws_credential_rejected() {
        let source = "s3://bucket/key";
        let parsed = parse_artifact_source(source).expect("parse");
        let limits = FetchLimits::default();
        let credential = Credential::Bearer {
            token: "tok".into(),
        };
        let request = FetchRequest {
            scheme: &parsed.scheme,
            url: &parsed.url,
            redacted_source: source,
            checksum: None,
            media_type: None,
            credential: Some(&credential),
            limits: &limits,
        };
        let err = build_target(&request).unwrap_err();
        assert!(err.to_string().contains("aws credential"), "{err}");
        // Redacted debug only — no token material in the error.
        assert!(!format!("{err:#}").contains("tok"), "{err:#}");
    }

    #[test]
    fn ambient_credential_is_accepted() -> R<()> {
        let source = "s3://store/content/sealed/abc";
        let parsed = parse_artifact_source(source)?;
        let limits = FetchLimits::default();
        let request = FetchRequest {
            scheme: &parsed.scheme,
            url: &parsed.url,
            redacted_source: source,
            checksum: None,
            media_type: None,
            credential: Some(&Credential::AwsAmbient),
            limits: &limits,
        };
        let target = build_target(&request)?;
        assert_eq!(target.key.as_ref(), "content/sealed/abc");
        Ok(())
    }
}
