//! The AWS half of the object-storage layer: who an S3 store signs as, and
//! the one way an `AmazonS3` client is built.
//!
//! Every S3 store Legion opens resolves its principal through
//! [`resolve_principal`], in one order:
//!
//! 1. Explicit keys the caller hands in: `persistence.streaming.credentials`,
//!    or the KV credential a `contentStore.auth.kv` or artifact `auth.kv`
//!    names.
//! 2. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`).
//! 3. Web identity: `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`
//!    (+ `AWS_ROLE_SESSION_NAME`) — EKS IRSA injects these; SPIRE, Vault or any
//!    process that writes a JWT to a file can be wired the same way.
//! 4. An ECS task role: `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`.
//! 5. EC2 instance metadata.
//!
//! Steps 2 to 5 are the *ambient* chain ([`ObjectCredential::Ambient`]); an
//! anonymous store (a public artifact bucket) resolves nothing and sends
//! unsigned requests. Steps 3 and 5 are `object_store`'s own providers: its
//! `AmazonS3Builder::build` falls through to them whenever no static keys are
//! set, and they re-read the token file on every exchange and refresh before
//! expiry. Legion's part is to make the order explicit, identical for every
//! consumer, and visible — streaming persistence resolves and logs it at boot —
//! and to fail the configurations that would otherwise surface as a silent
//! fall-through to instance metadata (a different, usually broader principal).
//!
//! This is also the manager's half of workload identity, and it deliberately
//! *presents* a platform-issued identity rather than one Legion signs for
//! itself: the signing key lives in the store being backed up. The argument
//! is in `internal_docs/plans/manager-workload-identity-plan.md` §6.

use object_store::aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut};

use super::{ObjectCredential, OpenOptions, RegionDefault};
use crate::prelude::*;

const ENV_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
const ENV_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
const ENV_SESSION_TOKEN: &str = "AWS_SESSION_TOKEN";
const ENV_WEB_IDENTITY_TOKEN_FILE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";
const ENV_ROLE_ARN: &str = "AWS_ROLE_ARN";
const ENV_CONTAINER_RELATIVE_URI: &str = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
const ENV_CONTAINER_FULL_URI: &str = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
const ENV_REGION: &str = "AWS_REGION";
const ENV_DEFAULT_REGION: &str = "AWS_DEFAULT_REGION";

/// The region `object_store` (like every AWS SDK) falls back to.
pub const FALLBACK_REGION: &str = "us-east-1";

/// A literal AWS key pair. `Debug` withholds all three fields: the key id is
/// half of the pair and no log needs it.
#[derive(Clone, PartialEq, Eq)]
pub struct AwsKeys {
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Only for temporary keys (an `ASIA…` key id from STS).
    pub session_token: Option<String>,
}

impl std::fmt::Debug for AwsKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsKeys")
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Who an S3 store signs as, once [`resolve_principal`] has resolved an
/// [`ObjectCredential`]. The ambient variants are in resolution order.
#[derive(Clone, PartialEq, Eq)]
pub enum Principal {
    /// Unsigned requests (a public bucket).
    Anonymous,
    /// Keys the caller handed in: `persistence.streaming.credentials`, or a
    /// KV credential.
    Explicit(AwsKeys),
    /// `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`.
    Environment(AwsKeys),
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

impl Principal {
    /// Stable label for logs. Explicit keys log as `config`, the label
    /// streaming persistence has always printed for its `credentials` block.
    pub fn as_str(&self) -> &'static str {
        match self {
            Principal::Anonymous => "anonymous",
            Principal::Explicit(_) => "config",
            Principal::Environment(_) => "environment",
            Principal::WebIdentity { .. } => "web_identity",
            Principal::ContainerRole { .. } => "container_role",
            Principal::InstanceMetadata => "instance_metadata",
        }
    }
}

/// Keys are withheld; the role ARN and token path are operator-facing
/// identifiers and stay visible.
impl std::fmt::Debug for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Principal::WebIdentity {
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

/// Resolve who a store signs as. `env` returns the variable's value when it
/// is set (even if empty); only [`ObjectCredential::Ambient`] reads it.
///
/// Fails on the configurations whose `object_store` behavior would be a
/// surprise: half a key pair, a web-identity variable without its partner,
/// and EKS Pod Identity (which `object_store` does not support, so it would
/// fall through to the *node's* instance role).
pub fn resolve_principal(
    credential: &ObjectCredential,
    env: &dyn Fn(&str) -> Option<String>,
) -> R<Principal> {
    match credential {
        ObjectCredential::Anonymous => Ok(Principal::Anonymous),
        ObjectCredential::Aws(aws) => {
            validate_key_pair(&aws.keys, &aws.origin)?;
            Ok(Principal::Explicit(aws.keys.clone()))
        }
        ObjectCredential::Ambient => resolve_ambient(env),
    }
}

/// [`resolve_principal`] against the real process environment — the same one
/// `object_store` reads for the web-identity and instance-metadata providers.
pub fn resolve_from_process_env(credential: &ObjectCredential) -> R<Principal> {
    resolve_principal(credential, &process_env)
}

/// The process environment, as the `env` lookups here take it.
pub(crate) fn process_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// The region the environment names (`AWS_REGION`, then
/// `AWS_DEFAULT_REGION`), as [`RegionDefault::Environment`] reads it.
pub fn environment_region(env: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    let nonempty = |name: &str| env(name).filter(|value| !value.trim().is_empty());
    nonempty(ENV_REGION).or_else(|| nonempty(ENV_DEFAULT_REGION))
}

fn resolve_ambient(env: &dyn Fn(&str) -> Option<String>) -> R<Principal> {
    let nonempty = |name: &str| env(name).filter(|value| !value.trim().is_empty());

    let id = nonempty(ENV_ACCESS_KEY_ID);
    let secret = nonempty(ENV_SECRET_ACCESS_KEY);
    if id.is_some() || secret.is_some() {
        let keys = AwsKeys {
            access_key_id: id.unwrap_or_default(),
            secret_access_key: secret.unwrap_or_default(),
            session_token: nonempty(ENV_SESSION_TOKEN),
        };
        validate_key_pair(&keys, "the AWS_* environment")?;
        return Ok(Principal::Environment(keys));
    }

    match (
        nonempty(ENV_WEB_IDENTITY_TOKEN_FILE),
        nonempty(ENV_ROLE_ARN),
    ) {
        (Some(token_file), Some(role_arn)) => {
            return Ok(Principal::WebIdentity {
                role_arn,
                token_file,
            })
        }
        (Some(_), None) => bail!(
            "{ENV_WEB_IDENTITY_TOKEN_FILE} is set but {ENV_ROLE_ARN} is not; web identity \
             needs both, and without them object storage would fall back to instance metadata"
        ),
        (None, Some(_)) => bail!(
            "{ENV_ROLE_ARN} is set but {ENV_WEB_IDENTITY_TOKEN_FILE} is not; web identity \
             needs both, and without them object storage would fall back to instance metadata"
        ),
        (None, None) => {}
    }

    if let Some(relative_uri) = nonempty(ENV_CONTAINER_RELATIVE_URI) {
        return Ok(Principal::ContainerRole { relative_uri });
    }

    if nonempty(ENV_CONTAINER_FULL_URI).is_some() {
        bail!(
            "{ENV_CONTAINER_FULL_URI} is set (EKS Pod Identity), which Legion's object-storage \
             client does not support: it would fall back to the node's instance role. Use IRSA \
             ({ENV_WEB_IDENTITY_TOKEN_FILE} + {ENV_ROLE_ARN}) or explicit keys \
             (persistence.streaming.credentials, contentStore.auth.kv)"
        );
    }

    Ok(Principal::InstanceMetadata)
}

fn validate_key_pair(keys: &AwsKeys, site: &str) -> R<()> {
    let has = |v: &str| !v.trim().is_empty();
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

/// Where an S3 store points. An `s3://bucket/prefix?region=…&endpoint=…` URL
/// reduces to this ([`super::StoreUrl`]), and so do the streaming replica
/// (through rustyriver's own URL dialect) and the lease (from
/// `lease.objectStorage`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct S3Location {
    pub bucket: String,
    /// `None`: the credential's region, then [`OpenOptions::region_default`].
    pub region: Option<String>,
    /// Custom endpoint (R2, MinIO, …); `None`: the credential's, then AWS.
    pub endpoint: Option<String>,
    /// `Some(true)` for path-style addressing (MinIO, Backblaze, …),
    /// `Some(false)` for virtual-hosted; `None` leaves `object_store`'s
    /// default (or `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`).
    pub force_path_style: Option<bool>,
    /// rustyriver's `skipVerify`: allows a plaintext endpoint.
    pub skip_verify: bool,
}

/// Everything an `AmazonS3` client is built from, decided before anything is
/// built so the decisions are testable. Never logged: it holds the keys.
pub(crate) struct S3Plan {
    pub bucket: String,
    /// `None`: whatever the imported environment settings say, else
    /// `object_store`'s `us-east-1`.
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub allow_http: bool,
    pub force_path_style: Option<bool>,
    pub conditional_put: bool,
    pub principal: Principal,
    /// The non-credential `AWS_*` settings imported from the environment.
    pub env_settings: Vec<(AmazonS3ConfigKey, String)>,
}

/// Decide how to build the client for `location`, signing as `principal`.
/// The region is the location's, then the credential's, then
/// `options.region_default`; the endpoint is the location's, then the
/// credential's. `env` answers the region lookups and `env_vars` is the
/// environment [`OpenOptions::aws_env_settings`] imports from.
pub(crate) fn plan(
    location: &S3Location,
    credential: &ObjectCredential,
    principal: Principal,
    options: &OpenOptions,
    env: &dyn Fn(&str) -> Option<String>,
    env_vars: impl IntoIterator<Item = (String, String)>,
) -> R<S3Plan> {
    if location.bucket.trim().is_empty() {
        bail!("object-storage bucket is required");
    }
    let (credential_region, credential_endpoint) = match credential {
        ObjectCredential::Aws(aws) => (aws.region.clone(), aws.endpoint.clone()),
        _ => (None, None),
    };
    let nonempty = |value: Option<String>| value.filter(|v| !v.trim().is_empty());
    let region = nonempty(location.region.clone())
        .or(nonempty(credential_region))
        .or_else(|| match options.region_default {
            RegionDefault::Environment => environment_region(env),
            RegionDefault::Fixed(region) => Some(region.to_string()),
        });
    let endpoint = nonempty(location.endpoint.clone()).or(nonempty(credential_endpoint));
    // object_store refuses plaintext unless told otherwise; a local MinIO on
    // http:// is the common case.
    let allow_http = endpoint.as_deref().is_some_and(|endpoint| {
        endpoint.starts_with("http://")
            || location.skip_verify
            || rustyriver::replica_url::is_local_endpoint(endpoint)
    });
    let env_settings = if options.aws_env_settings {
        env_store_settings(env_vars)
    } else {
        Vec::new()
    };
    Ok(S3Plan {
        bucket: location.bucket.clone(),
        region,
        endpoint,
        allow_http,
        force_path_style: location.force_path_style,
        conditional_put: options.conditional_put,
        principal,
        env_settings,
    })
}

/// The `AWS_*` variables `AmazonS3Builder::from_env` would apply that are
/// store settings rather than the principal or the region: the endpoint,
/// `AWS_ALLOW_HTTP`, server-side encryption, checksums, proxy and timeout
/// options, the IMDS endpoint and v1 fallback, and the like. Keys, the
/// session token, the ECS URI and the region are left out:
/// [`resolve_principal`] and [`OpenOptions::region_default`] own those.
fn env_store_settings(
    vars: impl IntoIterator<Item = (String, String)>,
) -> Vec<(AmazonS3ConfigKey, String)> {
    let mut settings: Vec<(AmazonS3ConfigKey, String)> = vars
        .into_iter()
        .filter(|(name, _)| name.starts_with("AWS_"))
        .filter_map(|(name, value)| {
            let key: AmazonS3ConfigKey = name.to_ascii_lowercase().parse().ok()?;
            let owned_elsewhere = matches!(
                key,
                AmazonS3ConfigKey::AccessKeyId
                    | AmazonS3ConfigKey::SecretAccessKey
                    | AmazonS3ConfigKey::Token
                    | AmazonS3ConfigKey::ContainerCredentialsRelativeUri
                    | AmazonS3ConfigKey::Region
                    | AmazonS3ConfigKey::DefaultRegion
                    | AmazonS3ConfigKey::Bucket
            );
            (!owned_elsewhere).then_some((key, value))
        })
        .collect();
    // Deterministic, so two names for one key (AWS_ENDPOINT and
    // AWS_ENDPOINT_URL) resolve the same way every time.
    settings.sort_by(|a, b| {
        format!("{:?}", a.0)
            .cmp(&format!("{:?}", b.0))
            .then_with(|| a.1.cmp(&b.1))
    });
    settings
}

/// The process environment, as [`plan`]'s `env_vars` takes it.
pub(crate) fn process_env_vars() -> Vec<(String, String)> {
    std::env::vars_os()
        .filter_map(|(name, value)| Some((name.into_string().ok()?, value.into_string().ok()?)))
        .collect()
}

/// Build the client a [`plan`] describes.
pub(crate) fn build(plan: S3Plan) -> R<AmazonS3> {
    let mut builder = AmazonS3Builder::new();
    for (key, value) in plan.env_settings {
        builder = builder.with_config(key, value);
    }
    builder = builder.with_bucket_name(&plan.bucket);
    if let Some(region) = &plan.region {
        builder = builder.with_region(region);
    }
    if let Some(path_style) = plan.force_path_style {
        builder = builder.with_virtual_hosted_style_request(!path_style);
    }
    if plan.conditional_put {
        builder = builder.with_conditional_put(S3ConditionalPut::ETagMatch);
    }
    if let Some(endpoint) = &plan.endpoint {
        builder = builder
            .with_endpoint(endpoint)
            .with_allow_http(plan.allow_http);
    }
    builder = match &plan.principal {
        Principal::Anonymous => builder.with_skip_signature(true),
        Principal::Explicit(keys) | Principal::Environment(keys) => {
            let mut b = builder
                .with_access_key_id(&keys.access_key_id)
                .with_secret_access_key(&keys.secret_access_key);
            if let Some(token) = &keys.session_token {
                b = b.with_token(token);
            }
            b
        }
        Principal::ContainerRole { relative_uri } => builder.with_config(
            AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
            relative_uri,
        ),
        // object_store's `build` reads the web-identity variables and falls
        // back to instance metadata on its own.
        Principal::WebIdentity { .. } | Principal::InstanceMetadata => builder,
    };
    builder
        .build()
        .with_context(|| format!("failed to build S3 store for bucket {}", plan.bucket))
}

#[cfg(test)]
mod aws_test {
    use super::super::AwsCredential;
    use super::*;

    const SENTINEL_SECRET: &str = "SUPERSECRETACCESSKEY";

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn keys(id: &str, secret: &str) -> AwsKeys {
        AwsKeys {
            access_key_id: id.to_string(),
            secret_access_key: secret.to_string(),
            session_token: None,
        }
    }

    fn explicit(id: &str, secret: &str) -> ObjectCredential {
        ObjectCredential::Aws(AwsCredential {
            keys: keys(id, secret),
            region: None,
            endpoint: None,
            origin: "persistence.streaming.credentials".into(),
        })
    }

    fn location() -> S3Location {
        S3Location {
            bucket: "state".into(),
            region: Some("us-west-2".into()),
            endpoint: None,
            force_path_style: Some(false),
            skip_verify: false,
        }
    }

    /// Streaming persistence's options: everything comes from its config.
    fn config_only() -> OpenOptions {
        OpenOptions {
            aws_env_settings: false,
            ..OpenOptions::default()
        }
    }

    fn build_with(location: &S3Location, principal: Principal, options: &OpenOptions) -> AmazonS3 {
        let decided = plan(
            location,
            &ObjectCredential::Ambient,
            principal,
            options,
            &env_of(&[]),
            Vec::new(),
        )
        .unwrap();
        build(decided).unwrap()
    }

    #[test]
    fn explicit_keys_outrank_every_ambient_source_test() {
        let env = env_of(&[
            (ENV_ACCESS_KEY_ID, "AKIAENV"),
            (ENV_SECRET_ACCESS_KEY, "envsecret"),
            (ENV_WEB_IDENTITY_TOKEN_FILE, "/var/run/token"),
            (ENV_ROLE_ARN, "arn:aws:iam::1:role/r"),
        ]);
        let principal = resolve_principal(&explicit("AKIACFG", "cfgsecret"), &env).unwrap();
        assert_eq!(principal, Principal::Explicit(keys("AKIACFG", "cfgsecret")));
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
        let ambient = ObjectCredential::Ambient;
        // Each step drops the winning source and checks the next one answers.
        let principal = resolve_principal(&ambient, &env_of(&all)).unwrap();
        assert_eq!(
            principal,
            Principal::Environment(AwsKeys {
                access_key_id: "AKIAENV".into(),
                secret_access_key: "envsecret".into(),
                session_token: Some("session".into()),
            })
        );

        let principal = resolve_principal(&ambient, &env_of(&all[3..])).unwrap();
        assert_eq!(
            principal,
            Principal::WebIdentity {
                role_arn: "arn:aws:iam::1:role/r".into(),
                token_file: "/var/run/token".into(),
            }
        );

        let principal = resolve_principal(&ambient, &env_of(&all[5..])).unwrap();
        assert_eq!(
            principal,
            Principal::ContainerRole {
                relative_uri: "/v2/credentials/abc".into()
            }
        );

        let principal = resolve_principal(&ambient, &env_of(&[])).unwrap();
        assert_eq!(principal, Principal::InstanceMetadata);
    }

    #[test]
    fn anonymous_reads_nothing_from_the_environment_test() {
        // A broken ambient chain does not stop a public-bucket fetch.
        let env = env_of(&[(ENV_ACCESS_KEY_ID, "AKIA")]);
        assert_eq!(
            resolve_principal(&ObjectCredential::Anonymous, &env).unwrap(),
            Principal::Anonymous
        );
        assert!(resolve_principal(&ObjectCredential::Ambient, &env).is_err());
    }

    #[test]
    fn empty_variables_count_as_unset_test() {
        let env = env_of(&[(ENV_ACCESS_KEY_ID, ""), (ENV_SECRET_ACCESS_KEY, "  ")]);
        assert_eq!(
            resolve_principal(&ObjectCredential::Ambient, &env).unwrap(),
            Principal::InstanceMetadata
        );
    }

    #[test]
    fn half_a_key_pair_fails_rather_than_falling_through_test() {
        let err = resolve_principal(&explicit("AKIA", ""), &env_of(&[])).unwrap_err();
        assert!(err.to_string().contains("no secret access key"), "{err}");
        assert!(
            err.to_string()
                .contains("persistence.streaming.credentials"),
            "{err}"
        );

        let err = resolve_principal(
            &ObjectCredential::Ambient,
            &env_of(&[(ENV_SECRET_ACCESS_KEY, "s")]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("no access key id"), "{err}");
        assert!(err.to_string().contains("AWS_*"), "{err}");
    }

    #[test]
    fn half_a_web_identity_fails_rather_than_falling_through_test() {
        let err = resolve_principal(
            &ObjectCredential::Ambient,
            &env_of(&[(ENV_WEB_IDENTITY_TOKEN_FILE, "/var/run/token")]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("AWS_ROLE_ARN is not"), "{err}");

        let err = resolve_principal(
            &ObjectCredential::Ambient,
            &env_of(&[(ENV_ROLE_ARN, "arn")]),
        )
        .unwrap_err();
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
        let err = resolve_principal(&ObjectCredential::Ambient, &env).unwrap_err();
        assert!(err.to_string().contains("EKS Pod Identity"), "{err}");
    }

    #[test]
    fn secrets_never_reach_debug_output_test() {
        let keys = AwsKeys {
            access_key_id: "AKIAEXAMPLE".into(),
            secret_access_key: SENTINEL_SECRET.into(),
            session_token: Some("SESSIONSECRET".into()),
        };
        let credential = ObjectCredential::Aws(AwsCredential {
            keys: keys.clone(),
            region: None,
            endpoint: None,
            origin: "kv legion/content".into(),
        });
        for rendered in [
            format!("{keys:?}"),
            format!("{credential:?}"),
            format!("{:?}", Principal::Explicit(keys.clone())),
            format!("{:?}", Principal::Environment(keys.clone())),
        ] {
            assert!(!rendered.contains(SENTINEL_SECRET), "{rendered}");
            assert!(!rendered.contains("SESSIONSECRET"), "{rendered}");
            assert!(!rendered.contains("AKIAEXAMPLE"), "{rendered}");
        }

        // The operator-facing identifiers of a web identity stay visible.
        let rendered = format!(
            "{:?}",
            Principal::WebIdentity {
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
        let store = build_with(
            &location(),
            Principal::Explicit(keys("AKIA", "s")),
            &config_only(),
        );
        assert!(format!("{store:?}").contains("StaticCredentialProvider"));

        let store = build_with(
            &location(),
            Principal::ContainerRole {
                relative_uri: "/v2/credentials/abc".into(),
            },
            &config_only(),
        );
        let rendered = format!("{store:?}");
        assert!(rendered.contains("TaskCredentialProvider"), "{rendered}");
        assert!(rendered.contains("/v2/credentials/abc"), "{rendered}");

        // Nothing set: object_store's own fallback, instance metadata. (Only
        // meaningful when the test process has no AWS web-identity variables,
        // which CI and dev shells do not.)
        if std::env::var(ENV_WEB_IDENTITY_TOKEN_FILE).is_err() {
            let store = build_with(&location(), Principal::InstanceMetadata, &config_only());
            assert!(format!("{store:?}").contains("InstanceCredentialProvider"));
        }

        // Anonymous skips signing altogether.
        let store = build_with(&location(), Principal::Anonymous, &config_only());
        let rendered = format!("{store:?}");
        assert!(rendered.contains("skip_signature: true"), "{rendered}");
    }

    #[test]
    fn conditional_put_is_only_on_when_asked_for_test() {
        let loc = location();
        let rendered = format!(
            "{:?}",
            build_with(&loc, Principal::InstanceMetadata, &config_only())
        );
        assert!(rendered.contains("conditional_put: None"), "{rendered}");

        let options = OpenOptions {
            conditional_put: true,
            ..config_only()
        };
        let rendered = format!(
            "{:?}",
            build_with(&loc, Principal::InstanceMetadata, &options)
        );
        assert!(rendered.contains("ETagMatch"), "{rendered}");
    }

    #[test]
    fn plaintext_endpoints_are_allowed_and_bucket_is_required_test() {
        let mut loc = location();
        loc.endpoint = Some("http://minio.internal:9000".into());
        loc.force_path_style = Some(true);
        let rendered = format!(
            "{:?}",
            build_with(&loc, Principal::InstanceMetadata, &config_only())
        );
        assert!(rendered.contains("minio.internal:9000"), "{rendered}");

        loc.bucket = String::new();
        let err = plan(
            &loc,
            &ObjectCredential::Ambient,
            Principal::InstanceMetadata,
            &config_only(),
            &env_of(&[]),
            Vec::new(),
        )
        .err()
        .expect("a bucket is required");
        assert!(err.to_string().contains("bucket is required"), "{err}");
    }

    /// The region and endpoint a store signs for: the location's (the URL's
    /// `?region=` / `?endpoint=`) win over the credential's, which win over
    /// the consumer's default.
    #[test]
    fn region_and_endpoint_precedence_test() {
        let env = env_of(&[
            (ENV_REGION, "eu-west-1"),
            (ENV_DEFAULT_REGION, "ap-south-1"),
        ]);
        let kv = ObjectCredential::Aws(AwsCredential {
            keys: keys("AKIA", "s"),
            region: Some("us-west-2".into()),
            endpoint: Some("https://kv.example.com".into()),
            origin: "kv legion/content".into(),
        });
        let bare = S3Location {
            bucket: "b".into(),
            ..S3Location::default()
        };
        let decide =
            |location: &S3Location, credential: &ObjectCredential, options: OpenOptions| {
                let principal = resolve_principal(credential, &env).unwrap();
                let decided =
                    plan(location, credential, principal, &options, &env, Vec::new()).unwrap();
                (decided.region, decided.endpoint)
            };

        // The URL beats the credential.
        let url = S3Location {
            region: Some("auto".into()),
            endpoint: Some("http://minio:9000".into()),
            ..bare.clone()
        };
        assert_eq!(
            decide(&url, &kv, OpenOptions::default()),
            (Some("auto".into()), Some("http://minio:9000".into()))
        );
        // The credential beats the default.
        assert_eq!(
            decide(&bare, &kv, OpenOptions::default()),
            (
                Some("us-west-2".into()),
                Some("https://kv.example.com".into())
            )
        );
        // The environment default: AWS_REGION, then AWS_DEFAULT_REGION.
        assert_eq!(
            decide(&bare, &ObjectCredential::Ambient, OpenOptions::default()).0,
            Some("eu-west-1".into())
        );
        assert_eq!(
            environment_region(&env_of(&[(ENV_DEFAULT_REGION, "ap-south-1")])).as_deref(),
            Some("ap-south-1")
        );
        // A fixed default ignores the environment.
        let fixed = OpenOptions {
            region_default: RegionDefault::Fixed(FALLBACK_REGION),
            ..OpenOptions::default()
        };
        assert_eq!(
            decide(&bare, &ObjectCredential::Anonymous, fixed).0,
            Some("us-east-1".into())
        );
    }

    #[test]
    fn env_settings_import_store_settings_but_not_the_principal_test() {
        let vars = [
            ("AWS_ACCESS_KEY_ID", "AKIA"),
            ("AWS_SECRET_ACCESS_KEY", SENTINEL_SECRET),
            ("AWS_SESSION_TOKEN", "tok"),
            ("AWS_REGION", "eu-west-1"),
            ("AWS_DEFAULT_REGION", "eu-west-2"),
            ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/x"),
            ("AWS_ENDPOINT", "http://minio:9000"),
            ("AWS_ALLOW_HTTP", "true"),
            ("AWS_SERVER_SIDE_ENCRYPTION", "aws:kms"),
            ("AWS_NOT_A_SETTING", "x"),
            ("HOME", "/root"),
        ]
        .map(|(k, v)| (k.to_string(), v.to_string()));
        let settings = env_store_settings(vars.clone());
        let names: Vec<String> = settings.iter().map(|(key, _)| format!("{key:?}")).collect();
        assert_eq!(settings.len(), 3, "{names:?}");
        assert!(settings.iter().any(
            |(key, value)| *key == AmazonS3ConfigKey::Endpoint && value == "http://minio:9000"
        ));
        assert!(!settings.iter().any(|(_, value)| value == SENTINEL_SECRET));

        // Streaming persistence opts out: its config describes the store.
        let decided = plan(
            &location(),
            &ObjectCredential::Ambient,
            Principal::InstanceMetadata,
            &config_only(),
            &env_of(&[]),
            vars,
        )
        .unwrap();
        assert!(decided.env_settings.is_empty());
    }
}
