//! `s3://` fetcher: `object_store`-backed, speaking any S3-compatible
//! endpoint (AWS, MinIO, R2, GCS interop). Region/endpoint come from the
//! credential object or query params
//! (`s3://bucket/key?endpoint=https://minio.internal:9000&region=auto`),
//! query params winning.

use std::io::Write;

use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::{path::Path as ObjectPath, ObjectStore};

use crate::artifacts::fetcher::{
    ArtifactFetcher, Credential, FetchRequest, FetchedArtifact, FetchedPayload,
};
use crate::artifacts::stage::ArtifactSink;
use crate::prelude::*;

#[derive(Debug, Default)]
pub struct S3Fetcher;

impl S3Fetcher {
    pub fn new() -> Self {
        Self
    }
}

struct S3Target {
    store: object_store::aws::AmazonS3,
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
    let bucket = req
        .url
        .host_str()
        .with_context(|| format!("{} has no bucket host", req.redacted_source))?;
    let key = req.url.path().trim_start_matches('/');
    if key.is_empty() {
        bail!("{} has no object key", req.redacted_source);
    }

    let mut region: Option<String> = None;
    let mut endpoint: Option<String> = None;
    if let Some(Credential::Aws {
        region: cred_region,
        endpoint: cred_endpoint,
        ..
    }) = req.credential
    {
        region = cred_region.clone();
        endpoint = cred_endpoint.clone();
    }
    // Query params win over credential-provided values.
    for (name, value) in req.url.query_pairs() {
        match name.as_ref() {
            "region" => region = Some(value.into_owned()),
            "endpoint" => endpoint = Some(value.into_owned()),
            _ => {}
        }
    }

    let ambient = matches!(req.credential, Some(Credential::AwsAmbient));
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    match region {
        Some(region) => builder = builder.with_region(region),
        // The ambient chain keeps the region `from_env` read from
        // `AWS_REGION`, as the contentStore publisher does, so a node reads
        // its store in the region it publishes to.
        None if ambient => {}
        None => builder = builder.with_region("us-east-1"),
    }
    if let Some(endpoint) = endpoint {
        let allow_http = endpoint.starts_with("http://");
        builder = builder.with_endpoint(endpoint).with_allow_http(allow_http);
    }
    match req.credential {
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
        // `from_env` above already carries the ambient chain: sign with it.
        Some(Credential::AwsAmbient) => {}
        Some(other) => {
            bail!(
                "s3 artifacts require an aws credential, got {other:?} for {}",
                req.redacted_source
            );
        }
        // Anonymous: public buckets are read without signing.
        None => {
            builder = builder.with_skip_signature(true);
        }
    }

    let file_name = key.rsplit('/').next().unwrap_or("object").to_string();
    Ok(S3Target {
        store: builder.build().context("build s3 client")?,
        key: ObjectPath::from(key),
        file_name,
    })
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
    use crate::artifacts::fetcher::FetchLimits;
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
