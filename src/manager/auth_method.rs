//! Auth methods: the pluggable identity sources behind login and bearer
//! authentication.
//!
//! Every way of proving who you are implements one trait, [`AuthMethod`]:
//! verify a credential, return a principal plus ACL policy names. Built-in
//! implementations:
//!
//! - [`UserpassMethod`] — username/password against the manager's user store
//!   (argon2id hashes). Login-only; always registered as `userpass`.
//! - [`JwtAuthMethod`] — externally issued JWTs/OIDC tokens, validated against
//!   JWKS (by URL, inline document, or OIDC issuer discovery) or an HMAC
//!   secret, with bound issuer/audiences and binding rules mapping claims to
//!   policies. Works both at login (exchange for an ACL token) and as a
//!   per-request bearer credential. One instance per `acl.authMethods` entry.
//! - [`WebhookMethod`] — delegates credential verification to an
//!   operator-supplied HTTP endpoint, which returns the principal and policy
//!   names. This is the recommended way to integrate custom identity systems
//!   (LDAP, SAML bridges, bespoke SSO) in any language, behind a
//!   process/network trust boundary. Login-only.
//!
//! Library users can register additional implementations with
//! [`crate::manager::acl::Authenticator::register_method`].
//!
//! ## Future: dynamic auth method plugins
//!
//! This trait is deliberately shaped like Legion's other plugin entry traits
//! (`TaskPlugin`, `SchedulingPolicy`, `RolloutStrategyPlugin`): object-safe,
//! `Send + Sync`, built-ins dispatched by name. A `plugins/auth/<name>/`
//! `cdylib` kind exporting `get() -> Box<dyn AuthMethod>` could be added like
//! the other plugin categories, but is held back deliberately: auth plugins
//! run inside the manager and decide cluster admission, so a buggy or
//! malicious one is a full compromise — a much worse failure domain than a
//! task or scheduling plugin. Use the webhook method instead; if a dylib kind
//! is ever added it must reuse this exact trait, and the security-sensitive
//! invariants stay in core regardless: methods only ever yield policy *names*
//! (never management access), and token minting/hashing never moves into a
//! method.

use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::{AclAuthMethodConfig, AclBindingRule};

use futures::future::BoxFuture;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use std::sync::RwLock;
use std::time::Duration;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tonic::Status;

/// Reserved name of the built-in username/password method.
pub const USERPASS_METHOD: &str = "userpass";

/// Why an auth method rejected a credential.
///
/// Deliberately transport-agnostic (no tonic types) so implementations stay
/// portable to out-of-process plugins; the authenticator maps these onto gRPC
/// status codes in one place.
#[derive(Debug, Clone)]
pub enum AuthError {
    /// The request is malformed for this method (missing username, etc.).
    InvalidRequest(String),
    /// The credential failed verification.
    InvalidCredentials(String),
    /// The credential verified but maps to no access.
    Denied(String),
    /// The method itself failed (storage, key material unavailable, ...).
    Internal(String),
}

impl AuthError {
    pub fn to_status(&self) -> Status {
        match self {
            AuthError::InvalidRequest(message) => Status::invalid_argument(message.clone()),
            AuthError::InvalidCredentials(message) => Status::unauthenticated(message.clone()),
            AuthError::Denied(message) => Status::permission_denied(message.clone()),
            AuthError::Internal(message) => {
                error!("auth method failure: {message}");
                Status::internal("auth method failure")
            }
        }
    }
}

/// Credentials carried by a Login request, as seen by an auth method.
#[derive(Debug, Clone, Copy, Default)]
pub struct LoginCredentials<'a> {
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
    pub jwt: Option<&'a str>,
}

/// A successfully verified identity: who the caller is and which ACL policies
/// apply. This is the only thing a method can grant — methods can never
/// confer management access or mint tokens themselves.
#[derive(Debug, Clone)]
pub struct VerifiedIdentity {
    /// Principal name without the method prefix (for example `alice@corp`).
    pub principal: String,
    /// ACL policy names this identity carries.
    pub policies: Vec<String>,
    /// RFC3339 expiry of the presented credential itself, when it has one
    /// (for example a JWT `exp`). Independent of minted-token TTLs.
    pub expires_at: Option<String>,
}

/// Public, secret-free description of an auth method, served by
/// `GetAuthMethods` so login UIs can present the available options.
#[derive(Debug, Clone, Default)]
pub struct MethodPublicInfo {
    pub name: String,
    /// `userpass`, `jwt`, `oidc`, `webhook`, or `custom`.
    pub kind: String,
    /// OIDC issuer URL (oidc kind), for client-side discovery.
    pub oidc_issuer: Option<String>,
    /// OAuth client ID for browser/CLI redirect login (oidc kind).
    pub oidc_client_id: Option<String>,
    /// OAuth scopes for redirect login (oidc kind).
    pub oidc_scopes: Vec<String>,
}

/// A source of identity: verifies credentials and maps them to ACL policies.
///
/// See the module docs for the built-in implementations and the planned
/// plugin story.
pub trait AuthMethod: Send + Sync {
    /// Unique method name; `legion login --method <name>` selects it.
    fn name(&self) -> &str;

    /// Exchange login credentials for an identity. The authenticator mints
    /// the resulting ACL token; methods only verify and map. Async because
    /// some methods (webhook, future device flows) verify over the network.
    fn login<'a>(
        &'a self,
        store: &'a Store,
        credentials: LoginCredentials<'a>,
    ) -> BoxFuture<'a, Result<VerifiedIdentity, AuthError>>;

    /// Validate a credential presented directly as a request bearer token.
    ///
    /// Return `None` when this method cannot interpret such credentials at
    /// all (for example userpass); `Some(Err(..))` when it tried and failed.
    /// This runs on the hot path of every request, so it must stay local —
    /// methods that would need a network call per request should be
    /// login-only and return `None`.
    fn validate_bearer(&self, _credential: &str) -> Option<Result<VerifiedIdentity, AuthError>> {
        None
    }

    /// Maximum (and default) TTL for ACL tokens minted via Login through this
    /// method.
    fn login_token_ttl(&self) -> Duration;

    /// Whether a token minted through this method must expire no later than
    /// the credential presented for it (`VerifiedIdentity::expires_at`).
    /// True for workload identity: a workload's identity token is retired by
    /// a fork or snapshot (its identity epoch moves), and an ACL token minted
    /// from it must not outlive it in every copy of the workload's memory.
    fn clamps_to_credential_expiry(&self) -> bool {
        false
    }

    /// Public metadata for login UIs. Never include secrets or key material.
    fn public_info(&self) -> MethodPublicInfo {
        MethodPublicInfo {
            name: self.name().to_string(),
            kind: "custom".to_string(),
            ..Default::default()
        }
    }

    /// Refresh remote validation material (for example JWKS documents).
    ///
    /// Called at manager startup and then every `acl.jwksRefreshInterval`.
    /// Failures must be handled internally (log and keep stale material) so
    /// a flaky identity provider cannot lock out authentication.
    fn refresh(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// Builds the configured method for one `acl.authMethods` entry.
pub fn method_from_config(
    config: &AclAuthMethodConfig,
    default_login_ttl: Duration,
) -> R<Box<dyn AuthMethod>> {
    let method_type = config.r#type.clone().unwrap_or_else(|| "jwt".to_string());
    match method_type.as_str() {
        "jwt" | "oidc" => Ok(Box::new(JwtAuthMethod::from_config(
            config,
            default_login_ttl,
        )?)),
        "webhook" => Ok(Box::new(WebhookMethod::from_config(
            config,
            default_login_ttl,
        )?)),
        other => bail!(
            "auth method `{}`: unknown type `{other}` (expected `jwt`, `oidc`, or `webhook`)",
            config.name
        ),
    }
}

/// Built-in username/password method backed by the `acl_user` table.
pub struct UserpassMethod {
    login_ttl: Duration,
}

impl UserpassMethod {
    pub fn new(login_ttl: Duration) -> Self {
        UserpassMethod { login_ttl }
    }
}

impl AuthMethod for UserpassMethod {
    fn name(&self) -> &str {
        USERPASS_METHOD
    }

    fn login<'a>(
        &'a self,
        store: &'a Store,
        credentials: LoginCredentials<'a>,
    ) -> BoxFuture<'a, Result<VerifiedIdentity, AuthError>> {
        Box::pin(async move {
            let username = credentials
                .username
                .filter(|u| !u.trim().is_empty())
                .ok_or_else(|| {
                    AuthError::InvalidRequest("userpass login requires `username`".into())
                })?;
            let password = credentials.password.ok_or_else(|| {
                AuthError::InvalidRequest("userpass login requires `password`".into())
            })?;
            let user = store
                .get_acl_user(username)
                .map_err(|e| AuthError::Internal(format!("user lookup failed: {e:#}")))?
                // Same error for unknown user and bad password (no user enumeration).
                .ok_or_else(|| {
                    AuthError::InvalidCredentials("invalid username or password".into())
                })?;
            if !crate::manager::acl::verify_password(password, &user.password_hash) {
                return Err(AuthError::InvalidCredentials(
                    "invalid username or password".into(),
                ));
            }
            Ok(VerifiedIdentity {
                principal: username.to_string(),
                policies: user.policies,
                expires_at: None,
            })
        })
    }

    fn login_token_ttl(&self) -> Duration {
        self.login_ttl
    }

    fn public_info(&self) -> MethodPublicInfo {
        MethodPublicInfo {
            name: USERPASS_METHOD.to_string(),
            kind: "userpass".to_string(),
            ..Default::default()
        }
    }
}

/// Built-in JWT/OIDC method: validates externally issued tokens and maps
/// verified claims onto policies via binding rules.
pub struct JwtAuthMethod {
    name: String,
    is_oidc: bool,
    jwks_url: Option<String>,
    hmac_secret: Option<String>,
    issuer: Option<String>,
    bound_issuer: Option<String>,
    bound_audiences: Vec<String>,
    user_claim: String,
    binding_rules: Vec<AclBindingRule>,
    token_ttl: Duration,
    oidc_client_id: Option<String>,
    oidc_scopes: Vec<String>,
    /// Cached JWKS document ({"keys": [...]}); primed from inline config and
    /// refreshed from the network by `refresh`.
    keys: RwLock<Option<Value>>,
}

/// An identity provider URL must be `https`, or `http` to a loopback host
/// (a local test provider).
fn require_https_unless_loopback(raw: &str) -> R<()> {
    let url = url::Url::parse(raw).with_context(|| format!("{raw:?} is not a URL"))?;
    let loopback = match url.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    };
    match url.scheme() {
        "https" => Ok(()),
        "http" if loopback => Ok(()),
        scheme => bail!("{raw:?} uses {scheme}; identity provider URLs must be https"),
    }
}

impl JwtAuthMethod {
    pub fn from_config(config: &AclAuthMethodConfig, default_login_ttl: Duration) -> R<Self> {
        let name = config.name.trim().to_string();
        if name.is_empty() {
            bail!("auth method name must not be empty");
        }
        let method_type = config.r#type.clone().unwrap_or_else(|| "jwt".to_string());
        let is_oidc = match method_type.as_str() {
            "jwt" => false,
            "oidc" => true,
            other => bail!("auth method `{name}`: JwtAuthMethod cannot handle type `{other}`"),
        };
        let static_jwks = config
            .jwks_json
            .as_ref()
            .map(|raw| serde_json::from_str::<Value>(raw))
            .transpose()
            .with_context(|| format!("auth method `{name}`: jwksJson is not valid JSON"))?;
        if is_oidc && config.issuer.is_none() {
            bail!("auth method `{name}`: oidc methods require `issuer` for discovery");
        }
        if !is_oidc
            && config.jwks_url.is_none()
            && static_jwks.is_none()
            && config.hmac_secret.is_none()
        {
            bail!("auth method `{name}`: jwt methods require one of `jwksUrl`, `jwksJson`, or `hmacSecret`");
        }
        // An issuer mints tokens for many services. Without an audience to
        // match, a token it minted for any of them (another app's ID token,
        // a CI job's OIDC token) would log in here.
        let mut bound_audiences = config.bound_audiences.clone().unwrap_or_default();
        if bound_audiences.is_empty() {
            if let Some(client_id) = config
                .oidc_client_id
                .as_ref()
                .filter(|id| !id.trim().is_empty())
            {
                bound_audiences.push(client_id.clone());
            }
        }
        if bound_audiences.is_empty() && config.hmac_secret.is_none() {
            bail!(
                "auth method `{name}`: set `boundAudiences` (an oidc method may set \
                 `oidcClientId` instead); without one, a token its issuer minted for any other \
                 service would be accepted"
            );
        }
        // Keys and discovery fetched over plain HTTP could be replaced in
        // transit, and with them every identity the method vouches for.
        for (field, url) in [("jwksUrl", &config.jwks_url), ("issuer", &config.issuer)] {
            if let Some(url) = url.as_deref() {
                require_https_unless_loopback(url)
                    .with_context(|| format!("auth method `{name}`: {field}"))?;
            }
        }
        let token_ttl = match &config.token_ttl {
            Some(raw) => {
                parse_ttl(raw).with_context(|| format!("auth method `{name}`: invalid tokenTtl"))?
            }
            None => default_login_ttl,
        };
        Ok(JwtAuthMethod {
            name,
            is_oidc,
            jwks_url: config.jwks_url.clone(),
            hmac_secret: config.hmac_secret.clone(),
            issuer: config.issuer.clone(),
            bound_issuer: config.bound_issuer.clone(),
            bound_audiences,
            user_claim: config
                .user_claim
                .clone()
                .unwrap_or_else(|| "sub".to_string()),
            binding_rules: config.binding_rules.clone().unwrap_or_default(),
            token_ttl,
            oidc_client_id: config.oidc_client_id.clone(),
            oidc_scopes: config
                .oidc_scopes
                .clone()
                .unwrap_or_else(|| vec!["openid".to_string()]),
            keys: RwLock::new(static_jwks),
        })
    }

    /// Validates signature + bounds and returns the claims.
    fn validate(&self, token: &str) -> R<Value> {
        let header = decode_header(token).context("invalid JWT header")?;

        let (key, algorithm) = if let Some(secret) = &self.hmac_secret {
            (
                DecodingKey::from_secret(secret.as_bytes()),
                Algorithm::HS256,
            )
        } else {
            let jwks = self
                .keys
                .read()
                .ok()
                .and_then(|keys| keys.clone())
                .ok_or_else(|| anyhow!("no JWKS available for method `{}` yet", self.name))?;
            decoding_key_from_jwks(&jwks, header.kid.as_deref(), header.alg)?
        };

        let mut validation = Validation::new(algorithm);
        if self.bound_audiences.is_empty() {
            validation.validate_aud = false;
        } else {
            validation.set_audience(&self.bound_audiences);
        }
        let issuer = self.bound_issuer.as_ref().or(self.issuer.as_ref());
        if let Some(issuer) = issuer {
            validation.set_issuer(&[issuer]);
        }

        let data = decode::<Value>(token, &key, &validation).context("JWT validation failed")?;
        Ok(data.claims)
    }

    fn identity_from_claims(&self, claims: &Value) -> VerifiedIdentity {
        VerifiedIdentity {
            principal: claims
                .get(&self.user_claim)
                .and_then(Value::as_str)
                .unwrap_or("jwt-user")
                .to_string(),
            policies: bound_policies(&self.binding_rules, claims),
            expires_at: claims
                .get("exp")
                .and_then(Value::as_i64)
                .and_then(|exp| OffsetDateTime::from_unix_timestamp(exp).ok())
                .and_then(|dt| dt.format(&Rfc3339).ok()),
        }
    }

    async fn fetch_jwks(&self) -> R<Value> {
        let jwks_url = match &self.jwks_url {
            Some(url) => url.clone(),
            None => {
                // OIDC discovery: issuer/.well-known/openid-configuration.
                let issuer = self.issuer.as_ref().context("oidc method missing issuer")?;
                let discovery_url = format!(
                    "{}/.well-known/openid-configuration",
                    issuer.trim_end_matches('/')
                );
                let discovery: Value = reqwest::Client::new()
                    .get(&discovery_url)
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await
                    .with_context(|| format!("fetching {discovery_url}"))?;
                discovery["jwks_uri"]
                    .as_str()
                    .context("discovery document missing jwks_uri")?
                    .to_string()
            }
        };
        let jwks: Value = reqwest::Client::new()
            .get(&jwks_url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .with_context(|| format!("fetching {jwks_url}"))?;
        Ok(jwks)
    }

    /// True when this method validates against remote key material.
    fn needs_remote_keys(&self) -> bool {
        self.hmac_secret.is_none() && (self.jwks_url.is_some() || self.is_oidc)
    }
}

impl AuthMethod for JwtAuthMethod {
    fn name(&self) -> &str {
        &self.name
    }

    fn login<'a>(
        &'a self,
        _store: &'a Store,
        credentials: LoginCredentials<'a>,
    ) -> BoxFuture<'a, Result<VerifiedIdentity, AuthError>> {
        Box::pin(async move {
            let jwt = credentials.jwt.ok_or_else(|| {
                AuthError::InvalidRequest(format!("auth method `{}` requires `jwt`", self.name))
            })?;
            let claims = self.validate(jwt).map_err(|e| {
                AuthError::InvalidCredentials(format!("JWT validation failed: {e:#}"))
            })?;
            let identity = self.identity_from_claims(&claims);
            if identity.policies.is_empty() {
                return Err(AuthError::Denied(format!(
                    "login via `{}` succeeded but no binding rules matched any policies",
                    self.name
                )));
            }
            Ok(identity)
        })
    }

    fn validate_bearer(&self, credential: &str) -> Option<Result<VerifiedIdentity, AuthError>> {
        Some(match self.validate(credential) {
            Ok(claims) => Ok(self.identity_from_claims(&claims)),
            Err(e) => Err(AuthError::InvalidCredentials(format!("{e:#}"))),
        })
    }

    fn login_token_ttl(&self) -> Duration {
        self.token_ttl
    }

    fn public_info(&self) -> MethodPublicInfo {
        MethodPublicInfo {
            name: self.name.clone(),
            kind: if self.is_oidc { "oidc" } else { "jwt" }.to_string(),
            oidc_issuer: if self.is_oidc {
                self.issuer.clone()
            } else {
                None
            },
            oidc_client_id: self.oidc_client_id.clone(),
            oidc_scopes: self.oidc_scopes.clone(),
        }
    }

    fn refresh(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if !self.needs_remote_keys() {
                return;
            }
            match self.fetch_jwks().await {
                Ok(jwks) => {
                    if let Ok(mut keys) = self.keys.write() {
                        *keys = Some(jwks);
                    }
                }
                Err(e) => {
                    // Keep any previously cached document so transient IdP
                    // outages do not lock out token validation.
                    warn!(method = %self.name, "Failed to refresh JWKS: {e:#}");
                }
            }
        })
    }
}

/// Built-in control-plane identity method (workload-identity plan Phase 7).
///
/// Verifies a bearer token this cluster issued for its reserved `legion`
/// identity — `aud` equal to the cluster's own issuer — against the manager's
/// **in-process** signing keys (no network / self-referential discovery), and
/// maps it to a principal `workload:<sub>` plus any policies from the
/// configured binding rules. Auto-registered under the name `legion` whenever
/// `workloadIdentity.enabled`, so a workload can authenticate back to Legion
/// with the same token it federates outward with.
///
/// This delivers the *verified identity primitive*: with no binding rules a
/// control-plane token verifies to a principal but grants no policies. Mapping
/// claims to permissions is done via `workloadIdentity.controlPlaneBindings`,
/// the seam the broader ACL effort builds on.
pub struct ControlPlaneIdentityMethod {
    state: std::sync::Arc<crate::manager::workload_identity::WorkloadIdentityState>,
    binding_rules: Vec<AclBindingRule>,
    token_ttl: Duration,
    /// Read for the instance's current identity epoch, so a token copied
    /// into a fork or snapshot is refused
    /// ([`crate::manager::workload_identity::check_identity_epoch`]).
    store: Store,
}

impl ControlPlaneIdentityMethod {
    pub fn new(
        state: std::sync::Arc<crate::manager::workload_identity::WorkloadIdentityState>,
        binding_rules: Vec<AclBindingRule>,
        token_ttl: Duration,
        store: Store,
    ) -> Self {
        Self {
            state,
            binding_rules,
            token_ttl,
            store,
        }
    }

    fn identity_from_token(&self, token: &str) -> Result<VerifiedIdentity, AuthError> {
        let claims = self.state.verify_control_plane(token).map_err(|e| {
            AuthError::InvalidCredentials(format!("control-plane token verification failed: {e:#}"))
        })?;
        // One indexed instance read, and only for a token that names an
        // instance: the rest of this path is signature checks.
        crate::manager::workload_identity::check_identity_epoch(&self.store, &claims).map_err(
            |e| AuthError::InvalidCredentials(format!("control-plane token refused: {e:#}")),
        )?;
        let claims = Value::Object(claims);
        let sub = claims
            .get("sub")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        Ok(VerifiedIdentity {
            // The authenticator prefixes this with the method name, yielding a
            // `legion:<sub>` principal — distinct from node (`node:`) and user
            // principals.
            principal: sub.to_string(),
            policies: bound_policies(&self.binding_rules, &claims),
            expires_at: claims
                .get("exp")
                .and_then(Value::as_i64)
                .and_then(|exp| OffsetDateTime::from_unix_timestamp(exp).ok())
                .and_then(|dt| dt.format(&Rfc3339).ok()),
        })
    }
}

impl AuthMethod for ControlPlaneIdentityMethod {
    fn name(&self) -> &str {
        crate::structs::workload_identity::CONTROL_PLANE_IDENTITY_NAME
    }

    fn login<'a>(
        &'a self,
        _store: &'a Store,
        credentials: LoginCredentials<'a>,
    ) -> BoxFuture<'a, Result<VerifiedIdentity, AuthError>> {
        Box::pin(async move {
            let jwt = credentials.jwt.ok_or_else(|| {
                AuthError::InvalidRequest(
                    "control-plane login requires the `legion` identity token as `jwt`".into(),
                )
            })?;
            let identity = self.identity_from_token(jwt)?;
            if identity.policies.is_empty() {
                return Err(AuthError::Denied(
                    "control-plane token verified but no controlPlaneBindings granted any policies"
                        .into(),
                ));
            }
            Ok(identity)
        })
    }

    fn validate_bearer(&self, credential: &str) -> Option<Result<VerifiedIdentity, AuthError>> {
        Some(self.identity_from_token(credential))
    }

    fn login_token_ttl(&self) -> Duration {
        self.token_ttl
    }

    fn clamps_to_credential_expiry(&self) -> bool {
        true
    }

    fn public_info(&self) -> MethodPublicInfo {
        MethodPublicInfo {
            name: self.name().to_string(),
            kind: "workload-identity".to_string(),
            ..Default::default()
        }
    }
}

/// Built-in webhook method: delegates verification to an external endpoint.
///
/// Legion POSTs `{"method", "username"?, "password"?, "jwt"?}` to
/// `webhookUrl` and expects HTTP 200 with
/// `{"principal": "...", "policies": ["..."]}`. 401 rejects the credential,
/// 403 means "verified but no access"; anything else is an internal error.
/// Login-only by design: a per-request network call would put the webhook on
/// the hot path of every RPC.
pub struct WebhookMethod {
    name: String,
    url: String,
    bearer: Option<String>,
    token_ttl: Duration,
}

impl WebhookMethod {
    pub fn from_config(config: &AclAuthMethodConfig, default_login_ttl: Duration) -> R<Self> {
        let name = config.name.trim().to_string();
        if name.is_empty() {
            bail!("auth method name must not be empty");
        }
        let url = config.webhook_url.clone().with_context(|| {
            format!("auth method `{name}`: webhook methods require `webhookUrl`")
        })?;
        let token_ttl = match &config.token_ttl {
            Some(raw) => {
                parse_ttl(raw).with_context(|| format!("auth method `{name}`: invalid tokenTtl"))?
            }
            None => default_login_ttl,
        };
        Ok(WebhookMethod {
            name,
            url,
            bearer: config.webhook_bearer.clone(),
            token_ttl,
        })
    }
}

impl AuthMethod for WebhookMethod {
    fn name(&self) -> &str {
        &self.name
    }

    fn login<'a>(
        &'a self,
        _store: &'a Store,
        credentials: LoginCredentials<'a>,
    ) -> BoxFuture<'a, Result<VerifiedIdentity, AuthError>> {
        Box::pin(async move {
            let body = json!({
                "method": self.name,
                "username": credentials.username,
                "password": credentials.password,
                "jwt": credentials.jwt,
            });
            let mut request = reqwest::Client::new()
                .post(&self.url)
                .timeout(Duration::from_secs(10))
                .json(&body);
            if let Some(bearer) = &self.bearer {
                request = request.bearer_auth(bearer);
            }
            let response = request.send().await.map_err(|e| {
                AuthError::Internal(format!("auth webhook `{}` unreachable: {e}", self.name))
            })?;

            match response.status().as_u16() {
                200 => {}
                401 => {
                    return Err(AuthError::InvalidCredentials(format!(
                        "auth webhook `{}` rejected the credentials",
                        self.name
                    )))
                }
                403 => {
                    return Err(AuthError::Denied(format!(
                        "auth webhook `{}` verified the credentials but denied access",
                        self.name
                    )))
                }
                status => {
                    return Err(AuthError::Internal(format!(
                        "auth webhook `{}` returned unexpected status {status}",
                        self.name
                    )))
                }
            }

            let payload: Value = response.json().await.map_err(|e| {
                AuthError::Internal(format!(
                    "auth webhook `{}` returned invalid JSON: {e}",
                    self.name
                ))
            })?;
            let principal = payload["principal"]
                .as_str()
                .filter(|p| !p.trim().is_empty())
                .ok_or_else(|| {
                    AuthError::Internal(format!(
                        "auth webhook `{}` response is missing `principal`",
                        self.name
                    ))
                })?
                .to_string();
            let policies: Vec<String> = payload["policies"]
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            if policies.is_empty() {
                return Err(AuthError::Denied(format!(
                    "auth webhook `{}` granted no policies",
                    self.name
                )));
            }
            Ok(VerifiedIdentity {
                principal,
                policies,
                expires_at: None,
            })
        })
    }

    fn login_token_ttl(&self) -> Duration {
        self.token_ttl
    }

    fn public_info(&self) -> MethodPublicInfo {
        MethodPublicInfo {
            name: self.name.clone(),
            kind: "webhook".to_string(),
            ..Default::default()
        }
    }
}

pub(crate) fn parse_ttl(raw: &str) -> R<Duration> {
    let parsed: humantime::Duration = raw
        .parse()
        .map_err(|e| anyhow!("invalid duration `{raw}`: {e}"))?;
    Ok(parsed.into())
}

/// Evaluates binding rules against verified JWT claims.
fn bound_policies(rules: &[AclBindingRule], claims: &Value) -> Vec<String> {
    let mut policies: Vec<String> = Vec::new();
    for rule in rules {
        let matched = match (&rule.claim, &rule.value) {
            (None, _) => true,
            (Some(claim), None) => claims.get(claim).is_some(),
            (Some(claim), Some(value)) => claim_contains(claims.get(claim), value),
        };
        if matched {
            for policy in &rule.policies {
                if !policies.contains(policy) {
                    policies.push(policy.clone());
                }
            }
        }
    }
    policies
}

fn claim_contains(claim: Option<&Value>, expected: &str) -> bool {
    match claim {
        None => false,
        Some(Value::String(s)) => s == expected,
        Some(Value::Array(items)) => items.iter().any(|item| item.as_str() == Some(expected)),
        Some(Value::Number(n)) => n.to_string() == expected,
        Some(Value::Bool(b)) => b.to_string() == expected,
        _ => false,
    }
}

/// Builds a `DecodingKey` from a JWKS document, selecting by `kid` when the
/// token names one and falling back to the first key of a compatible type.
fn decoding_key_from_jwks(
    jwks: &Value,
    kid: Option<&str>,
    alg: Algorithm,
) -> R<(DecodingKey, Algorithm)> {
    let keys = jwks["keys"]
        .as_array()
        .context("JWKS missing `keys` array")?;
    let candidates: Vec<&Value> = match kid {
        Some(kid) => keys
            .iter()
            .filter(|key| key["kid"].as_str() == Some(kid))
            .collect(),
        None => keys.iter().collect(),
    };
    if candidates.is_empty() {
        bail!("no JWKS key matches kid {kid:?} (try refreshing JWKS)");
    }
    // Restrict to asymmetric algorithms; HS* via JWKS is not supported.
    let algorithm = match alg {
        Algorithm::RS256
        | Algorithm::RS384
        | Algorithm::RS512
        | Algorithm::PS256
        | Algorithm::PS384
        | Algorithm::PS512
        | Algorithm::ES256
        | Algorithm::ES384 => alg,
        other => bail!("unsupported JWT algorithm {other:?} for JWKS validation"),
    };
    for key in candidates {
        match key["kty"].as_str() {
            Some("RSA") => {
                if let (Some(n), Some(e)) = (key["n"].as_str(), key["e"].as_str()) {
                    return Ok((DecodingKey::from_rsa_components(n, e)?, algorithm));
                }
            }
            Some("EC") => {
                if let (Some(x), Some(y)) = (key["x"].as_str(), key["y"].as_str()) {
                    return Ok((DecodingKey::from_ec_components(x, y)?, algorithm));
                }
            }
            _ => continue,
        }
    }
    bail!("no usable RSA/EC key found in JWKS for kid {kid:?}")
}

#[cfg(test)]
mod auth_method_test {
    use super::*;

    #[test]
    fn binding_rules_map_claims_to_policies() {
        let rules = vec![
            AclBindingRule {
                claim: Some("groups".into()),
                value: Some("platform".into()),
                policies: vec!["ops".into()],
            },
            AclBindingRule {
                claim: Some("email_verified".into()),
                value: None,
                policies: vec!["verified".into()],
            },
            AclBindingRule {
                claim: None,
                value: None,
                policies: vec!["base".into(), "ops".into()],
            },
        ];

        let claims = json!({
            "groups": ["eng", "platform"],
            "email_verified": true,
        });
        assert_eq!(
            bound_policies(&rules, &claims),
            vec!["ops", "verified", "base"]
        );

        let no_match = json!({ "groups": ["sales"] });
        assert_eq!(bound_policies(&rules, &no_match), vec!["base", "ops"]);
    }

    #[test]
    fn claim_contains_handles_scalar_and_array_claims() {
        assert!(claim_contains(Some(&json!("a")), "a"));
        assert!(!claim_contains(Some(&json!("a")), "b"));
        assert!(claim_contains(Some(&json!(["a", "b"])), "b"));
        assert!(claim_contains(Some(&json!(42)), "42"));
        assert!(claim_contains(Some(&json!(true)), "true"));
        assert!(!claim_contains(None, "a"));
    }

    #[test]
    fn jwt_method_config_validation() {
        let ttl = Duration::from_secs(3600);

        // jwt type with no key source is rejected.
        let keyless = AclAuthMethodConfig {
            name: "nokeys".into(),
            r#type: Some("jwt".into()),
            ..Default::default()
        };
        assert!(JwtAuthMethod::from_config(&keyless, ttl).is_err());

        // oidc type requires an issuer.
        let no_issuer = AclAuthMethodConfig {
            name: "sso".into(),
            r#type: Some("oidc".into()),
            ..Default::default()
        };
        assert!(JwtAuthMethod::from_config(&no_issuer, ttl).is_err());

        // hmac-secret jwt method is valid, login-only fields default sanely.
        let ok = AclAuthMethodConfig {
            name: "ci".into(),
            hmac_secret: Some("s".into()),
            ..Default::default()
        };
        let method = JwtAuthMethod::from_config(&ok, ttl).expect("valid method");
        assert_eq!(method.name(), "ci");
        assert_eq!(method.login_token_ttl(), ttl);
        assert!(!method.needs_remote_keys());
    }

    #[test]
    fn method_from_config_dispatches_by_type() {
        let ttl = Duration::from_secs(60);

        let jwt = AclAuthMethodConfig {
            name: "ci".into(),
            hmac_secret: Some("s".into()),
            ..Default::default()
        };
        assert_eq!(method_from_config(&jwt, ttl).expect("jwt").name(), "ci");

        let webhook = AclAuthMethodConfig {
            name: "hook".into(),
            r#type: Some("webhook".into()),
            webhook_url: Some("http://127.0.0.1:1/auth".into()),
            ..Default::default()
        };
        let method = method_from_config(&webhook, ttl).expect("webhook");
        assert_eq!(method.public_info().kind, "webhook");
        // Webhooks are login-only: never on the per-request bearer path.
        assert!(method.validate_bearer("anything").is_none());

        // Webhook without a url is rejected.
        let no_url = AclAuthMethodConfig {
            name: "hook".into(),
            r#type: Some("webhook".into()),
            ..Default::default()
        };
        assert!(method_from_config(&no_url, ttl).is_err());

        // Unknown types are rejected.
        let saml = AclAuthMethodConfig {
            name: "x".into(),
            r#type: Some("saml".into()),
            ..Default::default()
        };
        assert!(method_from_config(&saml, ttl).is_err());
    }

    /// A key-verified method must name its audience (an oidc method's
    /// client id counts), and its provider URLs must be https off loopback.
    #[test]
    fn jwt_methods_require_an_audience_and_https_test() {
        let ttl = Duration::from_secs(60);
        let jwks = AclAuthMethodConfig {
            name: "ci".into(),
            r#type: Some("jwt".into()),
            jwks_url: Some("https://token.actions.example.com/.well-known/jwks".into()),
            ..Default::default()
        };
        let err = JwtAuthMethod::from_config(&jwks, ttl)
            .err()
            .expect("no audience");
        assert!(format!("{err:#}").contains("boundAudiences"), "{err:#}");
        let bound = AclAuthMethodConfig {
            bound_audiences: Some(vec!["legion".into()]),
            ..jwks.clone()
        };
        JwtAuthMethod::from_config(&bound, ttl).expect("bound audience");
        let plain = AclAuthMethodConfig {
            jwks_url: Some("http://idp.example.com/jwks".into()),
            ..bound.clone()
        };
        let err = JwtAuthMethod::from_config(&plain, ttl)
            .err()
            .expect("plain http");
        assert!(format!("{err:#}").contains("https"), "{err:#}");
        let local = AclAuthMethodConfig {
            jwks_url: Some("http://127.0.0.1:8200/jwks".into()),
            ..bound
        };
        JwtAuthMethod::from_config(&local, ttl).expect("loopback http is a local provider");
        let oidc = AclAuthMethodConfig {
            name: "corp".into(),
            r#type: Some("oidc".into()),
            issuer: Some("https://idp.test".into()),
            oidc_client_id: Some("legion-ui".into()),
            ..Default::default()
        };
        let method = JwtAuthMethod::from_config(&oidc, ttl).expect("client id is the audience");
        assert_eq!(method.bound_audiences, vec!["legion-ui"]);
    }

    #[test]
    fn public_info_exposes_oidc_metadata_without_secrets() {
        let ttl = Duration::from_secs(60);
        let oidc = AclAuthMethodConfig {
            name: "corp".into(),
            r#type: Some("oidc".into()),
            issuer: Some("https://idp.test".into()),
            oidc_client_id: Some("legion-ui".into()),
            oidc_scopes: Some(vec!["openid".into(), "email".into()]),
            hmac_secret: Some("never-exposed".into()),
            ..Default::default()
        };
        let info = JwtAuthMethod::from_config(&oidc, ttl)
            .expect("valid oidc method")
            .public_info();
        assert_eq!(info.kind, "oidc");
        assert_eq!(info.oidc_issuer.as_deref(), Some("https://idp.test"));
        assert_eq!(info.oidc_client_id.as_deref(), Some("legion-ui"));
        assert_eq!(info.oidc_scopes, vec!["openid", "email"]);
    }

    /// Minimal one-shot HTTP server for webhook tests: answers every
    /// connection with the given status and body.
    async fn stub_http_server(status: u16, body: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub server");
        let addr = listener.local_addr().expect("stub addr");
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    // Read until end of headers, then the Content-Length body.
                    let mut request = Vec::new();
                    let mut buf = [0u8; 1024];
                    let (mut header_end, mut content_length) = (None, 0usize);
                    loop {
                        let Ok(read) = socket.read(&mut buf).await else {
                            return;
                        };
                        if read == 0 {
                            break;
                        }
                        request.extend_from_slice(&buf[..read]);
                        if header_end.is_none() {
                            if let Some(idx) =
                                request.windows(4).position(|window| window == b"\r\n\r\n")
                            {
                                header_end = Some(idx + 4);
                                let headers = String::from_utf8_lossy(&request[..idx]);
                                content_length = headers
                                    .lines()
                                    .find_map(|line| {
                                        line.to_ascii_lowercase()
                                            .strip_prefix("content-length:")
                                            .map(|value| value.trim().parse().unwrap_or(0))
                                    })
                                    .unwrap_or(0);
                            }
                        }
                        if let Some(end) = header_end {
                            if request.len() >= end + content_length {
                                break;
                            }
                        }
                    }
                    let reason = match status {
                        200 => "OK",
                        401 => "Unauthorized",
                        403 => "Forbidden",
                        _ => "Error",
                    };
                    let response = format!(
                        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        format!("http://{addr}/auth")
    }

    fn webhook_method(url: String) -> WebhookMethod {
        WebhookMethod::from_config(
            &AclAuthMethodConfig {
                name: "hook".into(),
                r#type: Some("webhook".into()),
                webhook_url: Some(url),
                ..Default::default()
            },
            Duration::from_secs(60),
        )
        .expect("valid webhook method")
    }

    fn webhook_store() -> Store {
        let dir = tempfile::Builder::new()
            .prefix("legion-webhook-")
            .tempdir()
            .expect("tempdir")
            .keep();
        Store::start_storage(&crate::structs::ManagerConfigFull {
            store_path: dir.to_str().expect("path").to_string(),
            ..Default::default()
        })
        .expect("store")
    }

    #[tokio::test]
    async fn webhook_login_maps_response_to_identity() -> R<()> {
        let store = webhook_store();
        let url = stub_http_server(200, r#"{"principal": "alice", "policies": ["readers"]}"#).await;
        let method = webhook_method(url);

        let identity = method
            .login(
                &store,
                LoginCredentials {
                    username: Some("alice"),
                    password: Some("hunter2"),
                    jwt: None,
                },
            )
            .await
            .expect("webhook login succeeds");
        assert_eq!(identity.principal, "alice");
        assert_eq!(identity.policies, vec!["readers"]);
        Ok(())
    }

    #[tokio::test]
    async fn webhook_login_maps_failures_to_auth_errors() -> R<()> {
        let store = webhook_store();

        let rejected = webhook_method(stub_http_server(401, "{}").await);
        assert!(matches!(
            rejected.login(&store, LoginCredentials::default()).await,
            Err(AuthError::InvalidCredentials(_))
        ));

        let denied = webhook_method(stub_http_server(403, "{}").await);
        assert!(matches!(
            denied.login(&store, LoginCredentials::default()).await,
            Err(AuthError::Denied(_))
        ));

        let broken = webhook_method(stub_http_server(500, "{}").await);
        assert!(matches!(
            broken.login(&store, LoginCredentials::default()).await,
            Err(AuthError::Internal(_))
        ));

        // 200 with no policies grants nothing.
        let empty = webhook_method(
            stub_http_server(200, r#"{"principal": "alice", "policies": []}"#).await,
        );
        assert!(matches!(
            empty.login(&store, LoginCredentials::default()).await,
            Err(AuthError::Denied(_))
        ));

        // 200 without a principal is a webhook contract error.
        let nameless = webhook_method(stub_http_server(200, r#"{"policies": ["x"]}"#).await);
        assert!(matches!(
            nameless.login(&store, LoginCredentials::default()).await,
            Err(AuthError::Internal(_))
        ));

        // Unreachable webhook is an internal error, not a credential error.
        let unreachable = webhook_method("http://127.0.0.1:1/auth".into());
        assert!(matches!(
            unreachable.login(&store, LoginCredentials::default()).await,
            Err(AuthError::Internal(_))
        ));
        Ok(())
    }

    #[test]
    fn decoding_key_selection_respects_kid_and_algorithm() {
        let jwks = json!({
            "keys": [
                { "kid": "sym", "kty": "oct", "k": "secret" },
                { "kid": "rsa-1", "kty": "RSA", "n": "su-PAnWNTI_dW3RMzZsBROWcaNGE8wgIUFSxJWBQDXR\
        2eVQuRZ3id43kgEHkcLBpkE5SJxgcXi1MNc7eZK9hif8MIuLfXHadV_qhg5MnzW-fDGAJoLpLrqJsTSeJl4cYHcAhUVLfNAaC\
        Z2k1MWLcWdQVNxiqgkLLkJU3qjOOSn0", "e": "AQAB" },
            ]
        });

        // kid selects the RSA key; HS256 via JWKS is refused.
        assert!(decoding_key_from_jwks(&jwks, Some("rsa-1"), Algorithm::RS256).is_ok());
        assert!(decoding_key_from_jwks(&jwks, Some("missing"), Algorithm::RS256).is_err());
        assert!(decoding_key_from_jwks(&jwks, Some("rsa-1"), Algorithm::HS256).is_err());
        // Without kid, falls back to the first usable asymmetric key.
        assert!(decoding_key_from_jwks(&jwks, None, Algorithm::RS256).is_ok());
    }

    #[tokio::test]
    async fn control_plane_method_verifies_legion_tokens_and_maps_policies() {
        use crate::manager::workload_identity::WorkloadIdentityState;
        use crate::structs::workload_identity::{
            WorkloadIdentity, WorkloadIdentityClaims, CONTROL_PLANE_IDENTITY_NAME,
        };
        use crate::structs::{AclBindingRule, WorkloadIdentityConfigFull};
        use std::sync::Arc;
        use std::time::SystemTime;

        let store = webhook_store();
        let config = WorkloadIdentityConfigFull {
            enabled: true,
            issuer: Some("https://manager.test:4747".into()),
            ..Default::default()
        };
        let state = Arc::new(
            WorkloadIdentityState::initialize(
                &store,
                &config,
                "https://manager.test:4747",
                SystemTime::now(),
            )
            .expect("wi state"),
        );

        // Issue a control-plane (`legion`) token: no operator audience → aud is
        // the cluster issuer.
        let claims = WorkloadIdentityClaims {
            region: "auto".into(),
            job: "api".into(),
            group: "web".into(),
            task: "server".into(),
            node_id: "node-1".into(),
            alloc_id: "7".into(),
            instance: None,
            slot: None,
            epoch: None,
        };
        let legion = WorkloadIdentity {
            name: CONTROL_PLANE_IDENTITY_NAME.into(),
            audience: Vec::new(),
            ..Default::default()
        };
        let token = state
            .issue(&claims, &legion, SystemTime::now())
            .expect("issue")
            .token;

        // Method maps the verified token → principal + policies via binding rules.
        let method = ControlPlaneIdentityMethod::new(
            state.clone(),
            vec![AclBindingRule {
                claim: Some("legion.io/job".into()),
                value: Some("api".into()),
                policies: vec!["kv-reader".into()],
            }],
            Duration::from_secs(60),
            store.clone(),
        );
        let verified = method
            .validate_bearer(&token)
            .expect("method interprets JWT")
            .expect("token verifies");
        assert_eq!(verified.principal, "auto:default:api:web:server");
        assert_eq!(verified.policies, vec!["kv-reader"]);

        // A token minted for a foreign audience (a normal federation token) is
        // NOT accepted as a control-plane credential.
        let aws = WorkloadIdentity {
            name: "aws".into(),
            audience: vec!["sts.amazonaws.com".into()],
            ..Default::default()
        };
        let aws_token = state
            .issue(&claims, &aws, SystemTime::now())
            .expect("issue aws")
            .token;
        assert!(method
            .validate_bearer(&aws_token)
            .expect("interprets")
            .is_err());
    }

    #[test]
    fn auth_errors_map_to_grpc_codes() {
        assert_eq!(
            AuthError::InvalidRequest("x".into()).to_status().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            AuthError::InvalidCredentials("x".into()).to_status().code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            AuthError::Denied("x".into()).to_status().code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            AuthError::Internal("x".into()).to_status().code(),
            tonic::Code::Internal
        );
    }
}
