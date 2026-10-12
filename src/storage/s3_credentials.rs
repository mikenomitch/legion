//! Object-storage credentials for streaming persistence.
//!
//! Every S3 store streaming persistence opens — the WAL replica, the restore a
//! new primary runs before it serves, and the lease object — is built by
//! [`build_s3_store`], so all three authenticate as the same principal through
//! one resolution order:
//!
//! 1. `persistence.streaming.credentials` in the manager config (literal keys).
//! 2. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`).
//! 3. Web identity: `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`
//!    (+ `AWS_ROLE_SESSION_NAME`) — EKS IRSA injects these; SPIRE, Vault or any
//!    process that writes a JWT to a file can be wired the same way.
//! 4. An ECS task role: `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`.
//! 5. EC2 instance metadata.
//!
//! Steps 3 and 5 are `object_store`'s own providers: its `AmazonS3Builder::build`
//! falls through to them whenever no static keys are set, and they re-read the
//! token file on every exchange and refresh before expiry. Legion's part is to
//! make the order explicit, identical for every consumer, and visible — the
//! source is resolved once at boot, logged, and misconfigurations that would
//! otherwise surface as a silent fall-through to instance metadata (a
//! different, usually broader principal) fail the boot instead.
//!
//! This is the manager's half of workload identity, and it deliberately
//! *presents* a platform-issued identity rather than one Legion signs for
//! itself: the signing key lives in the store being backed up. The argument
//! is in `internal_docs/plans/manager-workload-identity-plan.md` §6.

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut};
use object_store::ObjectStore;

use crate::prelude::*;
use crate::structs::StreamingCredentialsConfig;

const ENV_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
const ENV_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
const ENV_SESSION_TOKEN: &str = "AWS_SESSION_TOKEN";
const ENV_WEB_IDENTITY_TOKEN_FILE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";
const ENV_ROLE_ARN: &str = "AWS_ROLE_ARN";
const ENV_CONTAINER_RELATIVE_URI: &str = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
const ENV_CONTAINER_FULL_URI: &str = "AWS_CONTAINER_CREDENTIALS_FULL_URI";

/// Where streaming persistence's object-storage credentials come from. The
/// variants are in resolution order.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum CredentialSource {
    /// `persistence.streaming.credentials`.
    Config(StreamingCredentialsConfig),
    /// `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`.
    Environment(StreamingCredentialsConfig),
    /// `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`, exchanged by
    /// `object_store` via `AssumeRoleWithWebIdentity`.
    WebIdentity {
        role_arn: String,
        token_file: String,
    },
    /// `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` (ECS task role).
    ContainerRole { relative_uri: String },
    /// EC2 instance metadata — what `object_store` does when nothing else is
    /// configured.
    InstanceMetadata,
}

impl CredentialSource {
    /// Stable label for logs.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            CredentialSource::Config(_) => "config",
            CredentialSource::Environment(_) => "environment",
            CredentialSource::WebIdentity { .. } => "web_identity",
            CredentialSource::ContainerRole { .. } => "container_role",
            CredentialSource::InstanceMetadata => "instance_metadata",
        }
    }
}

/// Keys are withheld; the role ARN and token path are operator-facing
/// identifiers and stay visible.
impl std::fmt::Debug for CredentialSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredentialSource::WebIdentity {
                role_arn,
                token_file,
            } => f
                .debug_struct("WebIdentity")
                .field("role_arn", role_arn)
                .field("token_file", token_file)
                .finish(),
            other => f.write_str(other.as_str()),
        }
    }
}

/// Resolve the credential source from the config block and an environment
/// lookup. `env` returns the variable's value when it is set (even if empty).
///
/// Fails on the configurations whose `object_store` behavior would be a
/// surprise: half a key pair, a web-identity variable without its partner,
/// and EKS Pod Identity (which `object_store` does not support, so it would
/// fall through to the *node's* instance role).
pub(crate) fn resolve_credential_source(
    config: Option<&StreamingCredentialsConfig>,
    env: &dyn Fn(&str) -> Option<String>,
) -> R<CredentialSource> {
    if let Some(keys) = config {
        validate_key_pair(keys, "persistence.streaming.credentials")?;
        return Ok(CredentialSource::Config(keys.clone()));
    }

    let nonempty = |name: &str| env(name).filter(|value| !value.trim().is_empty());

    let env_keys = StreamingCredentialsConfig {
        access_key_id: nonempty(ENV_ACCESS_KEY_ID),
        secret_access_key: nonempty(ENV_SECRET_ACCESS_KEY),
        session_token: nonempty(ENV_SESSION_TOKEN),
    };
    if env_keys.access_key_id.is_some() || env_keys.secret_access_key.is_some() {
        validate_key_pair(&env_keys, "the AWS_* environment")?;
        return Ok(CredentialSource::Environment(env_keys));
    }

    match (
        nonempty(ENV_WEB_IDENTITY_TOKEN_FILE),
        nonempty(ENV_ROLE_ARN),
    ) {
        (Some(token_file), Some(role_arn)) => {
            return Ok(CredentialSource::WebIdentity {
                role_arn,
                token_file,
            })
        }
        (Some(_), None) => bail!(
            "{ENV_WEB_IDENTITY_TOKEN_FILE} is set but {ENV_ROLE_ARN} is not; web identity \
             needs both, and without them streaming persistence would fall back to \
             instance metadata"
        ),
        (None, Some(_)) => bail!(
            "{ENV_ROLE_ARN} is set but {ENV_WEB_IDENTITY_TOKEN_FILE} is not; web identity \
             needs both, and without them streaming persistence would fall back to \
             instance metadata"
        ),
        (None, None) => {}
    }

    if let Some(relative_uri) = nonempty(ENV_CONTAINER_RELATIVE_URI) {
        return Ok(CredentialSource::ContainerRole { relative_uri });
    }

    if nonempty(ENV_CONTAINER_FULL_URI).is_some() {
        bail!(
            "{ENV_CONTAINER_FULL_URI} is set (EKS Pod Identity), which streaming persistence \
             does not support: it would fall back to the node's instance role. Use IRSA \
             ({ENV_WEB_IDENTITY_TOKEN_FILE} + {ENV_ROLE_ARN}) or \
             persistence.streaming.credentials"
        );
    }

    Ok(CredentialSource::InstanceMetadata)
}

fn validate_key_pair(keys: &StreamingCredentialsConfig, site: &str) -> R<()> {
    let has = |v: &Option<String>| v.as_deref().is_some_and(|v| !v.trim().is_empty());
    match (has(&keys.access_key_id), has(&keys.secret_access_key)) {
        (true, true) => Ok(()),
        (false, _) => {
            bail!("{site} has no access key id; set accessKeyId and secretAccessKey together")
        }
        (true, false) => {
            bail!("{site} has no secret access key; set accessKeyId and secretAccessKey together")
        }
    }
}

/// Resolve against the manager's real process environment — the same one
/// `object_store` reads for the web-identity and instance-metadata providers.
pub(crate) fn resolve_from_process_env(
    config: Option<&StreamingCredentialsConfig>,
) -> R<CredentialSource> {
    resolve_credential_source(config, &|name| std::env::var(name).ok())
}

/// Where an S3 store points. Both the replica (from its `s3://` URL) and the
/// lease (from `lease.objectStorage`) reduce to this.
#[derive(Debug, Clone)]
pub(crate) struct S3Location {
    pub bucket: String,
    pub region: String,
    /// Custom endpoint (R2, MinIO, …); `None` for AWS.
    pub endpoint: Option<String>,
    /// Path-style addressing (MinIO, Backblaze, …).
    pub force_path_style: bool,
    /// Skip TLS verification (rustyriver's `skipVerify`); also allows http.
    pub skip_verify: bool,
    /// Send `If-None-Match` / `If-Match` on conditional writes. The lease's
    /// compare-and-swap needs it: without it `object_store` answers every
    /// create-or-update put with `NotImplemented`.
    pub conditional_put: bool,
}

/// Build an S3 store for streaming persistence, authenticated by `source`.
pub(crate) fn build_s3_store(
    location: &S3Location,
    source: &CredentialSource,
) -> R<Arc<dyn ObjectStore>> {
    if location.bucket.trim().is_empty() {
        bail!("object-storage bucket is required");
    }
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&location.bucket)
        .with_region(&location.region)
        .with_virtual_hosted_style_request(!location.force_path_style);
    if location.conditional_put {
        builder = builder.with_conditional_put(S3ConditionalPut::ETagMatch);
    }

    if let Some(endpoint) = location.endpoint.as_deref().filter(|e| !e.is_empty()) {
        // object_store refuses plaintext unless told otherwise; a local MinIO
        // on http:// is the common case.
        let allow_http = endpoint.starts_with("http://")
            || location.skip_verify
            || rustyriver::replica_url::is_local_endpoint(endpoint);
        builder = builder.with_endpoint(endpoint).with_allow_http(allow_http);
    }

    builder = match source {
        CredentialSource::Config(keys) | CredentialSource::Environment(keys) => {
            let mut b = builder
                .with_access_key_id(keys.access_key_id.clone().unwrap_or_default())
                .with_secret_access_key(keys.secret_access_key.clone().unwrap_or_default());
            if let Some(token) = &keys.session_token {
                b = b.with_token(token.clone());
            }
            b
        }
        CredentialSource::ContainerRole { relative_uri } => builder.with_config(
            AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
            relative_uri,
        ),
        // object_store's `build` reads the web-identity variables and falls
        // back to instance metadata on its own.
        CredentialSource::WebIdentity { .. } | CredentialSource::InstanceMetadata => builder,
    };

    let store = builder
        .build()
        .with_context(|| format!("failed to build S3 store for bucket {}", location.bucket))?;
    Ok(Arc::new(store))
}

#[cfg(test)]
mod s3_credentials_test {
    use super::*;

    const SENTINEL_SECRET: &str = "SUPERSECRETACCESSKEY";

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn keys(id: Option<&str>, secret: Option<&str>) -> StreamingCredentialsConfig {
        StreamingCredentialsConfig {
            access_key_id: id.map(str::to_string),
            secret_access_key: secret.map(str::to_string),
            session_token: None,
        }
    }

    fn location() -> S3Location {
        S3Location {
            bucket: "state".into(),
            region: "us-west-2".into(),
            endpoint: None,
            force_path_style: false,
            skip_verify: false,
            conditional_put: false,
        }
    }

    #[test]
    fn config_keys_outrank_every_ambient_source_test() {
        let env = env_of(&[
            (ENV_ACCESS_KEY_ID, "AKIAENV"),
            (ENV_SECRET_ACCESS_KEY, "envsecret"),
            (ENV_WEB_IDENTITY_TOKEN_FILE, "/var/run/token"),
            (ENV_ROLE_ARN, "arn:aws:iam::1:role/r"),
        ]);
        let cfg = keys(Some("AKIACFG"), Some("cfgsecret"));
        let source = resolve_credential_source(Some(&cfg), &env).unwrap();
        assert_eq!(source, CredentialSource::Config(cfg));
    }

    #[test]
    fn ambient_sources_resolve_in_aws_chain_order_test() {
        let all = [
            (ENV_ACCESS_KEY_ID, "AKIAENV"),
            (ENV_SECRET_ACCESS_KEY, "envsecret"),
            (ENV_SESSION_TOKEN, "session"),
            (ENV_WEB_IDENTITY_TOKEN_FILE, "/var/run/token"),
            (ENV_ROLE_ARN, "arn:aws:iam::1:role/r"),
            (ENV_CONTAINER_RELATIVE_URI, "/v2/credentials/abc"),
        ];
        // Each step drops the winning source and checks the next one answers.
        let source = resolve_credential_source(None, &env_of(&all)).unwrap();
        assert_eq!(
            source,
            CredentialSource::Environment(StreamingCredentialsConfig {
                access_key_id: Some("AKIAENV".into()),
                secret_access_key: Some("envsecret".into()),
                session_token: Some("session".into()),
            })
        );

        let source = resolve_credential_source(None, &env_of(&all[3..])).unwrap();
        assert_eq!(
            source,
            CredentialSource::WebIdentity {
                role_arn: "arn:aws:iam::1:role/r".into(),
                token_file: "/var/run/token".into(),
            }
        );

        let source = resolve_credential_source(None, &env_of(&all[5..])).unwrap();
        assert_eq!(
            source,
            CredentialSource::ContainerRole {
                relative_uri: "/v2/credentials/abc".into()
            }
        );

        let source = resolve_credential_source(None, &env_of(&[])).unwrap();
        assert_eq!(source, CredentialSource::InstanceMetadata);
    }

    #[test]
    fn empty_variables_count_as_unset_test() {
        let env = env_of(&[(ENV_ACCESS_KEY_ID, ""), (ENV_SECRET_ACCESS_KEY, "  ")]);
        assert_eq!(
            resolve_credential_source(None, &env).unwrap(),
            CredentialSource::InstanceMetadata
        );
    }

    #[test]
    fn half_a_key_pair_fails_rather_than_falling_through_test() {
        let err =
            resolve_credential_source(Some(&keys(Some("AKIA"), None)), &env_of(&[])).unwrap_err();
        assert!(err.to_string().contains("no secret access key"), "{err}");
        assert!(
            err.to_string()
                .contains("persistence.streaming.credentials"),
            "{err}"
        );

        let err =
            resolve_credential_source(None, &env_of(&[(ENV_SECRET_ACCESS_KEY, "s")])).unwrap_err();
        assert!(err.to_string().contains("no access key id"), "{err}");
        assert!(err.to_string().contains("AWS_*"), "{err}");
    }

    #[test]
    fn half_a_web_identity_fails_rather_than_falling_through_test() {
        let err = resolve_credential_source(
            None,
            &env_of(&[(ENV_WEB_IDENTITY_TOKEN_FILE, "/var/run/token")]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("AWS_ROLE_ARN is not"), "{err}");

        let err = resolve_credential_source(None, &env_of(&[(ENV_ROLE_ARN, "arn")])).unwrap_err();
        assert!(
            err.to_string()
                .contains("AWS_WEB_IDENTITY_TOKEN_FILE is not"),
            "{err}"
        );
    }

    #[test]
    fn eks_pod_identity_is_refused_not_downgraded_to_the_node_role_test() {
        let env = env_of(&[(
            ENV_CONTAINER_FULL_URI,
            "http://169.254.170.23/v1/credentials",
        )]);
        let err = resolve_credential_source(None, &env).unwrap_err();
        assert!(err.to_string().contains("EKS Pod Identity"), "{err}");
    }

    #[test]
    fn secrets_never_reach_debug_output_test() {
        let cfg = StreamingCredentialsConfig {
            access_key_id: Some("AKIAEXAMPLE".into()),
            secret_access_key: Some(SENTINEL_SECRET.into()),
            session_token: Some("SESSIONSECRET".into()),
        };
        for rendered in [
            format!("{cfg:?}"),
            format!("{:?}", CredentialSource::Config(cfg.clone())),
            format!("{:?}", CredentialSource::Environment(cfg.clone())),
        ] {
            assert!(!rendered.contains(SENTINEL_SECRET), "{rendered}");
            assert!(!rendered.contains("SESSIONSECRET"), "{rendered}");
            assert!(!rendered.contains("AKIAEXAMPLE"), "{rendered}");
        }

        // The operator-facing identifiers of a web identity stay visible.
        let rendered = format!(
            "{:?}",
            CredentialSource::WebIdentity {
                role_arn: "arn:aws:iam::1:role/legion-state".into(),
                token_file: "/var/run/token".into(),
            }
        );
        assert!(rendered.contains("legion-state"), "{rendered}");
        assert!(rendered.contains("/var/run/token"), "{rendered}");
    }

    // The provider object_store picks is observable through the built store's
    // Debug. That output includes secrets for static keys, which is why it is
    // only ever inspected here and never logged.
    #[test]
    fn built_store_uses_the_resolved_provider_test() {
        let store = build_s3_store(
            &location(),
            &CredentialSource::Config(keys(Some("AKIA"), Some("s"))),
        )
        .unwrap();
        assert!(format!("{store:?}").contains("StaticCredentialProvider"));

        let store = build_s3_store(
            &location(),
            &CredentialSource::ContainerRole {
                relative_uri: "/v2/credentials/abc".into(),
            },
        )
        .unwrap();
        let rendered = format!("{store:?}");
        assert!(rendered.contains("TaskCredentialProvider"), "{rendered}");
        assert!(rendered.contains("/v2/credentials/abc"), "{rendered}");

        // Nothing set: object_store's own fallback, instance metadata. (Only
        // meaningful when the test process has no AWS web-identity variables,
        // which CI and dev shells do not.)
        if std::env::var(ENV_WEB_IDENTITY_TOKEN_FILE).is_err() {
            let store = build_s3_store(&location(), &CredentialSource::InstanceMetadata).unwrap();
            assert!(format!("{store:?}").contains("InstanceCredentialProvider"));
        }
    }

    #[test]
    fn conditional_put_is_only_on_when_asked_for_test() {
        let mut loc = location();
        let rendered = format!(
            "{:?}",
            build_s3_store(&loc, &CredentialSource::InstanceMetadata).unwrap()
        );
        assert!(rendered.contains("conditional_put: None"), "{rendered}");

        loc.conditional_put = true;
        let rendered = format!(
            "{:?}",
            build_s3_store(&loc, &CredentialSource::InstanceMetadata).unwrap()
        );
        assert!(rendered.contains("ETagMatch"), "{rendered}");
    }

    #[test]
    fn plaintext_endpoints_are_allowed_and_bucket_is_required_test() {
        let mut loc = location();
        loc.endpoint = Some("http://minio.internal:9000".into());
        loc.force_path_style = true;
        let rendered = format!(
            "{:?}",
            build_s3_store(&loc, &CredentialSource::InstanceMetadata).unwrap()
        );
        assert!(rendered.contains("minio.internal:9000"), "{rendered}");

        loc.bucket = String::new();
        let err = build_s3_store(&loc, &CredentialSource::InstanceMetadata).unwrap_err();
        assert!(err.to_string().contains("bucket is required"), "{err}");
    }
}
