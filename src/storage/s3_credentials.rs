//! S3 credential providers for the manager's streaming persistence path.
//!
//! This is the manager's half of workload identity. Tasks and worker nodes
//! *receive* Legion-issued OIDC tokens (`src/manager/workload_identity.rs`);
//! the manager instead **presents** a token issued by the platform it runs on
//! — an EKS IRSA projected token, a GKE workload-identity token, a SPIRE
//! JWT-SVID, or any other file containing a JWT — and exchanges it for
//! short-lived S3 credentials via STS `AssumeRoleWithWebIdentity`.
//!
//! The manager deliberately does **not** sign this token with Legion's own
//! issuer. The signing key lives in the SQLite store that streaming
//! persistence is backing up, so a Legion-issued credential would (a) let
//! anyone who reads the store destroy its backups, (b) be unobtainable during
//! a restore, when no store exists yet, and (c) require the manager's own
//! JWKS endpoint to be reachable for AWS to validate it. The full argument is
//! in `internal_docs/plans/manager-workload-identity-plan.md` §6.
//!
//! Secret handling: the token bytes and the returned secret key never appear
//! in `Debug` output, in error strings, or in any log line. That is asserted
//! by tests at the bottom of this file rather than left to review.

use std::sync::Arc;
use std::time::{Duration as StdDuration, SystemTime};

use object_store::aws::{AwsCredential, AwsCredentialProvider};
use object_store::{CredentialProvider, Error as ObjectStoreError, Result as ObjectStoreResult};

use crate::prelude::*;
use crate::structs::{
    StaticS3CredentialsConfigFull, StreamingCredentialsConfigFull, StreamingCredentialsProvider,
    WebIdentityCredentialsConfigFull,
};

/// How long before a session credential actually expires we start treating it
/// as stale. Large enough that a multipart upload signed just before the
/// refresh still completes against a valid credential.
const REFRESH_SKEW: StdDuration = StdDuration::from_secs(300);

/// Timeout for one STS exchange. Short: the caller (a replica sync tick or a
/// lease renewal) retries, and a hung credential fetch would stall the loop it
/// blocks.
const STS_TIMEOUT: StdDuration = StdDuration::from_secs(10);

/// Builds the credential provider for the configured source, or `None` when
/// the credentials should be left to `object_store`'s own resolution — which
/// is what `environment` means, and what every pre-existing config gets.
///
/// Returning `None` rather than an env-reading provider is deliberate: it
/// keeps the historical code path byte-identical for configs that predate this
/// block.
pub fn build_credential_provider(
    cfg: &StreamingCredentialsConfigFull,
) -> R<Option<AwsCredentialProvider>> {
    match cfg.provider {
        StreamingCredentialsProvider::Environment => Ok(None),
        StreamingCredentialsProvider::Static => {
            let statics = cfg
                .r#static
                .as_ref()
                .context("streaming credentials provider 'static' requires a static block")?;
            Ok(Some(Arc::new(StaticCredentials::new(statics))))
        }
        StreamingCredentialsProvider::WebIdentity => {
            let web = cfg.web_identity.as_ref().context(
                "streaming credentials provider 'webIdentity' requires a webIdentity block",
            )?;
            Ok(Some(Arc::new(WebIdentityCredentials::new(web)?)))
        }
    }
}

// ── static ───────────────────────────────────────────────────────────────────

/// Literal keys from the config file. Exists so that `static` and
/// `webIdentity` reach the replica and the lease through the same seam, rather
/// than one going through `AmazonS3Builder::with_access_key_id` and the other
/// through a provider.
struct StaticCredentials {
    credential: Arc<AwsCredential>,
}

impl StaticCredentials {
    fn new(cfg: &StaticS3CredentialsConfigFull) -> Self {
        Self {
            credential: Arc::new(AwsCredential {
                key_id: cfg.access_key_id.clone(),
                secret_key: cfg.secret_access_key.clone(),
                token: cfg.session_token.clone(),
            }),
        }
    }
}

impl std::fmt::Debug for StaticCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Not even the key id: it is an identifier an operator can correlate,
        // but it is also half of the secret pair and there is no reason for it
        // to reach a log.
        f.write_str("StaticCredentials(redacted)")
    }
}

#[async_trait::async_trait]
impl CredentialProvider for StaticCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> ObjectStoreResult<Arc<AwsCredential>> {
        Ok(Arc::clone(&self.credential))
    }
}

// ── web identity ─────────────────────────────────────────────────────────────

/// A session credential plus the instant it stops being usable.
#[derive(Clone)]
struct CachedCredential {
    credential: Arc<AwsCredential>,
    expires_at: SystemTime,
}

/// `object_store::AwsCredential` derives `Debug` and prints its secret key, so
/// anything wrapping one has to shadow that with a redacted impl of its own.
impl std::fmt::Debug for CachedCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedCredential")
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

impl CachedCredential {
    /// Usable only while we are comfortably clear of expiry — see
    /// [`REFRESH_SKEW`].
    fn is_fresh(&self, now: SystemTime) -> bool {
        match self.expires_at.duration_since(now) {
            Ok(remaining) => remaining > REFRESH_SKEW,
            // `expires_at` is already in the past.
            Err(_) => false,
        }
    }
}

/// Exchanges a platform-issued OIDC token for STS session credentials, caching
/// them until they near expiry.
pub struct WebIdentityCredentials {
    token_file: std::path::PathBuf,
    role_arn: String,
    session_name: String,
    duration_seconds: u32,
    endpoint: String,
    http: reqwest::Client,
    /// Single-flight: a burst of concurrent object-store calls performs one
    /// exchange, not one per call.
    cached: tokio::sync::Mutex<Option<CachedCredential>>,
}

impl std::fmt::Debug for WebIdentityCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Role ARN and token *path* are operator-facing identifiers and safe.
        // The token contents and the session secret are not, and never reach
        // this struct's rendering.
        f.debug_struct("WebIdentityCredentials")
            .field("role_arn", &self.role_arn)
            .field("token_file", &self.token_file)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl WebIdentityCredentials {
    pub fn new(cfg: &WebIdentityCredentialsConfigFull) -> R<Self> {
        let http = reqwest::Client::builder()
            .timeout(STS_TIMEOUT)
            .build()
            .context("build STS http client")?;
        Ok(Self {
            token_file: std::path::PathBuf::from(&cfg.token_file),
            role_arn: cfg.role_arn.clone(),
            session_name: cfg.session_name.clone(),
            duration_seconds: cfg.duration_seconds,
            endpoint: sts_endpoint(cfg),
            http,
            cached: tokio::sync::Mutex::new(None),
        })
    }

    /// Reads the token fresh from disk. Never cached: projected
    /// service-account tokens are rotated underneath a long-running process,
    /// and caching them is the classic IRSA failure.
    fn read_token(&self) -> R<String> {
        let raw = std::fs::read_to_string(&self.token_file).with_context(|| {
            format!("read web-identity token file {}", self.token_file.display())
        })?;
        let token = raw.trim().to_string();
        if token.is_empty() {
            bail!(
                "web-identity token file {} is empty",
                self.token_file.display()
            );
        }
        Ok(token)
    }

    /// One `AssumeRoleWithWebIdentity` call. The request is deliberately
    /// unsigned — the JWT is the authentication, which is exactly why this
    /// path needs no bootstrap credential and works during a restore.
    async fn exchange(&self) -> R<CachedCredential> {
        let token = self.read_token()?;
        let duration = self.duration_seconds.to_string();
        let form = [
            ("Action", "AssumeRoleWithWebIdentity"),
            ("Version", "2011-06-15"),
            ("RoleArn", self.role_arn.as_str()),
            ("RoleSessionName", self.session_name.as_str()),
            ("DurationSeconds", duration.as_str()),
            ("WebIdentityToken", token.as_str()),
        ];

        let response = self
            .http
            .post(&self.endpoint)
            .form(&form)
            .send()
            .await
            .with_context(|| format!("POST AssumeRoleWithWebIdentity to {}", self.endpoint))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .context("read AssumeRoleWithWebIdentity response body")?;

        if !status.is_success() {
            // STS's own error code/message is the useful diagnostic
            // (`InvalidIdentityToken`, `AccessDenied`, `ExpiredTokenException`
            // …). Neither carries the token we sent.
            let (code, message) = parse_sts_error(&body);
            bail!(
                "AssumeRoleWithWebIdentity failed ({status}): {code}: {message} \
                 [role {}, token file {}]",
                self.role_arn,
                self.token_file.display()
            );
        }

        parse_assume_role_response(&body)
    }

    /// Cached credential if fresh, otherwise exchange. The lock is held across
    /// the exchange on purpose: it is the single-flight.
    async fn credential(&self) -> R<Arc<AwsCredential>> {
        let mut guard = self.cached.lock().await;
        let now = SystemTime::now();
        if let Some(cached) = guard.as_ref() {
            if cached.is_fresh(now) {
                return Ok(Arc::clone(&cached.credential));
            }
        }

        let fresh = self.exchange().await?;
        let credential = Arc::clone(&fresh.credential);
        let ttl = fresh
            .expires_at
            .duration_since(now)
            .unwrap_or(StdDuration::ZERO);
        *guard = Some(fresh);
        info!(
            role_arn = %self.role_arn,
            ttl_secs = ttl.as_secs(),
            "Refreshed manager state-replication credentials via web identity"
        );
        Ok(credential)
    }
}

#[async_trait::async_trait]
impl CredentialProvider for WebIdentityCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> ObjectStoreResult<Arc<AwsCredential>> {
        self.credential()
            .await
            .map_err(|err| ObjectStoreError::Generic {
                store: "S3",
                // `anyhow`'s chain, which by construction holds STS codes and
                // file paths but no secret material.
                source: format!("{err:#}").into(),
            })
    }
}

/// Resolve the STS endpoint: explicit override, then the regional endpoint for
/// `stsRegion`, then the global one. Regional is preferred where a region is
/// known because the global endpoint is a single point of failure that AWS
/// itself recommends against for new workloads.
fn sts_endpoint(cfg: &WebIdentityCredentialsConfigFull) -> String {
    if let Some(endpoint) = cfg
        .sts_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|e| !e.is_empty())
    {
        return endpoint.to_string();
    }
    match cfg
        .sts_region
        .as_deref()
        .map(str::trim)
        .filter(|r| !r.is_empty())
    {
        Some(region) => format!("https://sts.{region}.amazonaws.com"),
        None => "https://sts.amazonaws.com".to_string(),
    }
}

// ── STS XML ──────────────────────────────────────────────────────────────────

/// Pulls the text of the first occurrence of each named element out of an STS
/// XML document. Deliberately a flat scan rather than a serde model: the
/// response shape is four leaf strings, and a scan cannot be broken by a
/// namespace prefix or an element AWS adds later.
fn extract_elements(xml: &str, names: &[&str]) -> HashMap<String, String> {
    use quick_xml::events::Event;

    let mut found: HashMap<String, String> = HashMap::new();
    let mut reader = quick_xml::Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut current: Option<String> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = local_name(e.name().as_ref());
                current = names
                    .iter()
                    .find(|candidate| **candidate == name)
                    .map(|c| (*c).to_string());
            }
            Ok(Event::Text(e)) => {
                if let Some(name) = current.take() {
                    if let Ok(text) = e.unescape() {
                        found.entry(name).or_insert_with(|| text.into_owned());
                    }
                }
            }
            Ok(Event::End(_)) => current = None,
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
    }
    found
}

fn local_name(raw: &[u8]) -> String {
    let name = String::from_utf8_lossy(raw);
    match name.rsplit_once(':') {
        Some((_, local)) => local.to_string(),
        None => name.into_owned(),
    }
}

/// `(code, message)` from an STS `<ErrorResponse>`, with placeholders when the
/// body is not the shape we expect (an HTML error page from a proxy, say).
fn parse_sts_error(body: &str) -> (String, String) {
    let found = extract_elements(body, &["Code", "Message"]);
    (
        found
            .get("Code")
            .cloned()
            .unwrap_or_else(|| "Unknown".to_string()),
        found
            .get("Message")
            .cloned()
            .unwrap_or_else(|| "no message in STS response".to_string()),
    )
}

fn parse_assume_role_response(body: &str) -> R<CachedCredential> {
    let found = extract_elements(
        body,
        &[
            "AccessKeyId",
            "SecretAccessKey",
            "SessionToken",
            "Expiration",
        ],
    );

    let key_id = found
        .get("AccessKeyId")
        .cloned()
        .context("AssumeRoleWithWebIdentity response has no AccessKeyId")?;
    let secret_key = found
        .get("SecretAccessKey")
        .cloned()
        .context("AssumeRoleWithWebIdentity response has no SecretAccessKey")?;
    let session_token = found.get("SessionToken").cloned();

    // A response without an Expiration is treated as expiring at the requested
    // duration — conservative, because the effect is an earlier refresh.
    let expires_at = match found.get("Expiration") {
        Some(raw) => parse_rfc3339(raw).with_context(|| {
            format!("AssumeRoleWithWebIdentity returned an unparseable Expiration {raw:?}")
        })?,
        None => SystemTime::now() + StdDuration::from_secs(u64::from(DEF_WEB_IDENTITY_DURATION)),
    };

    Ok(CachedCredential {
        credential: Arc::new(AwsCredential {
            key_id,
            secret_key,
            token: session_token,
        }),
        expires_at,
    })
}

/// Fallback session lifetime when STS omits `Expiration`; matches the config
/// default rather than importing it, so this module stays standalone.
const DEF_WEB_IDENTITY_DURATION: u32 = 3600;

fn parse_rfc3339(raw: &str) -> R<SystemTime> {
    let parsed = chrono::DateTime::parse_from_rfc3339(raw.trim())
        .with_context(|| format!("parse timestamp {raw:?}"))?;
    Ok(SystemTime::from(parsed))
}

#[cfg(test)]
mod s3_credentials_test {
    use super::*;

    const SENTINEL_TOKEN: &str = "eyJhbGciOiJFUzI1NiJ9.SUPERSECRETTOKENBYTES.sig";
    const SENTINEL_SECRET: &str = "SUPERSECRETSESSIONKEY";

    fn ok_response() -> String {
        format!(
            r#"<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>ASIAEXAMPLE</AccessKeyId>
      <SecretAccessKey>{SENTINEL_SECRET}</SecretAccessKey>
      <SessionToken>FwoGZXIvYXdzEXAMPLE</SessionToken>
      <Expiration>2030-01-01T00:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>"#
        )
    }

    fn web_identity_cfg(token_file: &str) -> WebIdentityCredentialsConfigFull {
        WebIdentityCredentialsConfigFull {
            token_file: token_file.to_string(),
            role_arn: "arn:aws:iam::123456789012:role/legion-manager-state".into(),
            session_name: "legion-manager".into(),
            duration_seconds: 3600,
            sts_region: Some("us-east-1".into()),
            sts_endpoint: None,
        }
    }

    #[test]
    fn parses_a_successful_exchange_test() {
        let parsed = parse_assume_role_response(&ok_response()).expect("parse");
        assert_eq!(parsed.credential.key_id, "ASIAEXAMPLE");
        assert_eq!(parsed.credential.secret_key, SENTINEL_SECRET);
        assert_eq!(
            parsed.credential.token.as_deref(),
            Some("FwoGZXIvYXdzEXAMPLE")
        );
        assert!(parsed.is_fresh(SystemTime::now()));
    }

    #[test]
    fn missing_credentials_are_an_error_not_a_default_test() {
        let err = parse_assume_role_response("<AssumeRoleWithWebIdentityResponse/>")
            .expect_err("must reject");
        assert!(err.to_string().contains("AccessKeyId"), "{err}");
    }

    #[test]
    fn surfaces_the_sts_error_code_and_message_test() {
        let body = r#"<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <Error>
    <Type>Sender</Type>
    <Code>InvalidIdentityToken</Code>
    <Message>Couldn't retrieve verification key from your identity provider</Message>
  </Error>
</ErrorResponse>"#;
        let (code, message) = parse_sts_error(body);
        assert_eq!(code, "InvalidIdentityToken");
        assert!(message.contains("verification key"), "{message}");
    }

    #[test]
    fn unrecognized_error_bodies_still_yield_a_diagnostic_test() {
        let (code, message) = parse_sts_error("<html><body>502 Bad Gateway</body></html>");
        assert_eq!(code, "Unknown");
        assert!(message.contains("no message"), "{message}");
    }

    #[test]
    fn credentials_inside_the_skew_window_are_stale_test() {
        let now = SystemTime::now();
        let make = |ttl: StdDuration| CachedCredential {
            credential: Arc::new(AwsCredential {
                key_id: "k".into(),
                secret_key: "s".into(),
                token: None,
            }),
            expires_at: now + ttl,
        };
        // Comfortably ahead of expiry: reuse.
        assert!(make(REFRESH_SKEW + StdDuration::from_secs(60)).is_fresh(now));
        // Inside the skew window: refresh even though it has not expired.
        assert!(!make(REFRESH_SKEW - StdDuration::from_secs(1)).is_fresh(now));
        // Already expired.
        assert!(!CachedCredential {
            credential: Arc::new(AwsCredential {
                key_id: "k".into(),
                secret_key: "s".into(),
                token: None,
            }),
            expires_at: now - StdDuration::from_secs(1),
        }
        .is_fresh(now));
    }

    #[test]
    fn sts_endpoint_resolution_test() {
        let mut cfg = web_identity_cfg("/tmp/token");
        assert_eq!(sts_endpoint(&cfg), "https://sts.us-east-1.amazonaws.com");

        cfg.sts_endpoint = Some("https://sts.internal:8443".into());
        assert_eq!(sts_endpoint(&cfg), "https://sts.internal:8443");

        cfg.sts_endpoint = None;
        cfg.sts_region = None;
        assert_eq!(sts_endpoint(&cfg), "https://sts.amazonaws.com");
    }

    #[test]
    fn token_file_is_reread_on_every_exchange_test() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("token");
        std::fs::write(&path, "first-token\n").expect("write");

        let provider =
            WebIdentityCredentials::new(&web_identity_cfg(&path.to_string_lossy())).expect("build");
        assert_eq!(provider.read_token().expect("read"), "first-token");

        // A projected service-account token rotates underneath the process;
        // the next exchange must pick up the new bytes.
        std::fs::write(&path, "second-token\n").expect("rewrite");
        assert_eq!(provider.read_token().expect("read"), "second-token");
    }

    #[test]
    fn empty_and_missing_token_files_are_rejected_test() {
        let dir = tempfile::tempdir().expect("tempdir");
        let empty = dir.path().join("empty");
        std::fs::write(&empty, "   \n").expect("write");
        let provider = WebIdentityCredentials::new(&web_identity_cfg(&empty.to_string_lossy()))
            .expect("build");
        let err = provider.read_token().expect_err("empty token rejected");
        assert!(err.to_string().contains("is empty"), "{err}");

        let missing = dir.path().join("nope");
        let provider = WebIdentityCredentials::new(&web_identity_cfg(&missing.to_string_lossy()))
            .expect("build");
        assert!(provider.read_token().is_err());
    }

    #[test]
    fn secret_material_never_reaches_debug_output_test() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("token");
        std::fs::write(&path, SENTINEL_TOKEN).expect("write");

        let provider =
            WebIdentityCredentials::new(&web_identity_cfg(&path.to_string_lossy())).expect("build");
        let rendered = format!("{provider:?}");
        assert!(!rendered.contains(SENTINEL_TOKEN), "{rendered}");
        assert!(!rendered.contains("SUPERSECRET"), "{rendered}");
        // The operator-facing identifiers are still there — a redacted Debug
        // that says nothing is its own diagnostic problem.
        assert!(rendered.contains("legion-manager-state"), "{rendered}");

        let statics = StaticCredentials::new(&StaticS3CredentialsConfigFull {
            access_key_id: "AKIAEXAMPLE".into(),
            secret_access_key: SENTINEL_SECRET.into(),
            session_token: None,
        });
        let rendered = format!("{statics:?}");
        assert!(!rendered.contains(SENTINEL_SECRET), "{rendered}");
        assert!(!rendered.contains("AKIAEXAMPLE"), "{rendered}");

        // `AwsCredential` itself derives Debug and prints its secret, so the
        // cache wrapper has to shadow it.
        let cached = parse_assume_role_response(&ok_response()).expect("parse");
        let rendered = format!("{cached:?}");
        assert!(!rendered.contains(SENTINEL_SECRET), "{rendered}");
    }

    #[tokio::test]
    async fn static_provider_returns_the_configured_keys_test() {
        let provider = StaticCredentials::new(&StaticS3CredentialsConfigFull {
            access_key_id: "AKIAEXAMPLE".into(),
            secret_access_key: "secret".into(),
            session_token: Some("session".into()),
        });
        let credential = provider.get_credential().await.expect("credential");
        assert_eq!(credential.key_id, "AKIAEXAMPLE");
        assert_eq!(credential.token.as_deref(), Some("session"));
    }

    #[test]
    fn environment_provider_defers_to_object_store_test() {
        let cfg = StreamingCredentialsConfigFull::default();
        assert!(build_credential_provider(&cfg).expect("build").is_none());
    }

    #[test]
    fn build_requires_the_matching_block_test() {
        let cfg = StreamingCredentialsConfigFull {
            provider: StreamingCredentialsProvider::WebIdentity,
            r#static: None,
            web_identity: None,
        };
        assert!(build_credential_provider(&cfg).is_err());

        let cfg = StreamingCredentialsConfigFull {
            provider: StreamingCredentialsProvider::Static,
            r#static: None,
            web_identity: None,
        };
        assert!(build_credential_provider(&cfg).is_err());
    }
}
