//! Authentication and ACL enforcement runtime.
//!
//! Every credential type converges on one [`AclIdentity`]:
//!
//! - **Legion ACL tokens** (opaque `lgn_...` secrets) are hashed and looked up
//!   in the store; their attached policies become the identity's rules.
//! - **External JWTs / OIDC tokens** are validated against a configured auth
//!   method (JWKS, issuer/audience bounds) and mapped to policies via binding
//!   rules — either per request (bearer) or once at `Login` (which mints a
//!   short-lived ACL token).
//! - **Username/password** logins verify an argon2id hash and mint a
//!   short-lived ACL token carrying the user's policies.
//! - **No credential** resolves to the `anonymous` policy when one exists.
//!
//! Enforcement is default-deny once ACLs are enabled. In audit-only mode
//! denials are logged under the `acl_audit` tracing target but allowed, so
//! operators can stage policies before turning enforcement on.

// tonic::Status is ~176 bytes; returning it directly matches the rest of the
// gRPC surface and keeps handler call sites to a single `?`.
#![allow(clippy::result_large_err)]

use crate::prelude::*;
use crate::storage::acl_store::AclTokenRecord;
use crate::storage::store::Store;
use crate::structs::acl::{AclAction, AclIdentity, AclResource, AclScope, AclSource};
use crate::structs::{AclAuthMethodConfig, AclBindingRule, AclConfigFull, AclTokenInfo};

use argon2::password_hash::{
    rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString,
};
use argon2::Argon2;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::sync::RwLock;
use std::time::Duration;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tonic::metadata::MetadataMap;
use tonic::Status;

/// Prefix for Legion ACL token secrets, so logs/configs are recognizable and
/// JWTs can be told apart from opaque tokens.
pub const TOKEN_PREFIX: &str = "lgn_";

/// Policy name applied to requests that present no credential.
pub const ANONYMOUS_POLICY: &str = "anonymous";

/// Reserved auth-method name for built-in username/password login.
pub const USERPASS_METHOD: &str = "userpass";

const DEFAULT_LOGIN_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// One configured JWT/OIDC auth method with defaults resolved.
#[derive(Debug, Clone)]
pub struct AuthMethod {
    pub name: String,
    pub is_oidc: bool,
    pub jwks_url: Option<String>,
    pub static_jwks: Option<Value>,
    pub hmac_secret: Option<String>,
    pub issuer: Option<String>,
    pub bound_issuer: Option<String>,
    pub bound_audiences: Vec<String>,
    pub user_claim: String,
    pub binding_rules: Vec<AclBindingRule>,
    pub token_ttl: Duration,
}

impl AuthMethod {
    fn from_config(config: &AclAuthMethodConfig, login_ttl: Duration) -> R<Self> {
        let name = config.name.trim().to_string();
        if name.is_empty() {
            bail!("auth method name must not be empty");
        }
        if name == USERPASS_METHOD {
            bail!("auth method name `{USERPASS_METHOD}` is reserved for the built-in method");
        }
        let method_type = config.r#type.clone().unwrap_or_else(|| "jwt".to_string());
        let is_oidc = match method_type.as_str() {
            "jwt" => false,
            "oidc" => true,
            other => {
                bail!("auth method `{name}`: unknown type `{other}` (expected `jwt` or `oidc`)")
            }
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
        let token_ttl = match &config.token_ttl {
            Some(raw) => {
                parse_ttl(raw).with_context(|| format!("auth method `{name}`: invalid tokenTtl"))?
            }
            None => login_ttl,
        };
        Ok(AuthMethod {
            name,
            is_oidc,
            jwks_url: config.jwks_url.clone(),
            static_jwks,
            hmac_secret: config.hmac_secret.clone(),
            issuer: config.issuer.clone(),
            bound_issuer: config.bound_issuer.clone(),
            bound_audiences: config.bound_audiences.clone().unwrap_or_default(),
            user_claim: config
                .user_claim
                .clone()
                .unwrap_or_else(|| "sub".to_string()),
            binding_rules: config.binding_rules.clone().unwrap_or_default(),
            token_ttl,
        })
    }
}

fn parse_ttl(raw: &str) -> R<Duration> {
    let parsed: humantime::Duration = raw
        .parse()
        .map_err(|e| anyhow!("invalid duration `{raw}`: {e}"))?;
    Ok(parsed.into())
}

/// Hashes a token secret for at-rest storage and lookup.
pub fn hash_secret(secret: &str) -> String {
    format!("{:x}", Sha256::digest(secret.as_bytes()))
}

fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buf);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

/// Hashes a password with argon2id (OWASP-recommended defaults).
pub fn hash_password(password: &str) -> R<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| anyhow!("failed to hash password: {e}"))
}

/// Verifies a password against a stored argon2id PHC string.
pub fn verify_password(password: &str, stored_hash: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(stored_hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

/// The manager's ACL state: config plus the JWKS cache for auth methods.
#[derive(Debug)]
pub struct AclRuntime {
    pub enabled: bool,
    pub audit_only: bool,
    pub login_ttl: Duration,
    pub jwks_refresh_interval: Duration,
    pub methods: Vec<AuthMethod>,
    /// method name -> fetched JWKS document ({"keys": [...]}).
    jwks_cache: RwLock<HashMap<String, Value>>,
}

impl AclRuntime {
    /// Runtime with ACLs off; every caller gets full access.
    pub fn disabled() -> Self {
        AclRuntime {
            enabled: false,
            audit_only: false,
            login_ttl: DEFAULT_LOGIN_TTL,
            jwks_refresh_interval: Duration::from_secs(300),
            methods: Vec::new(),
            jwks_cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn from_config(config: &AclConfigFull) -> R<Self> {
        let login_ttl = parse_ttl(&config.login_ttl).context("invalid acl.loginTtl")?;
        let jwks_refresh_interval =
            parse_ttl(&config.jwks_refresh_interval).context("invalid acl.jwksRefreshInterval")?;
        let mut methods = Vec::new();
        for method_config in &config.auth_methods {
            let method = AuthMethod::from_config(method_config, login_ttl)?;
            if methods.iter().any(|m: &AuthMethod| m.name == method.name) {
                bail!("duplicate auth method name `{}`", method.name);
            }
            methods.push(method);
        }
        let runtime = AclRuntime {
            enabled: config.enabled,
            audit_only: config.audit_only,
            login_ttl,
            jwks_refresh_interval,
            methods,
            jwks_cache: RwLock::new(HashMap::new()),
        };
        runtime.load_static_jwks();
        Ok(runtime)
    }

    /// Primes the cache with inline JWKS documents (no network needed).
    fn load_static_jwks(&self) {
        let mut cache = match self.jwks_cache.write() {
            Ok(cache) => cache,
            Err(_) => return,
        };
        for method in &self.methods {
            if let Some(jwks) = &method.static_jwks {
                cache.insert(method.name.clone(), jwks.clone());
            }
        }
    }

    /// Fetches remote JWKS documents for all methods that need them.
    ///
    /// Called once at startup and then periodically (`jwksRefreshInterval`).
    /// Failures are logged and leave any previously cached document in place,
    /// so transient IdP outages do not lock out token validation.
    pub async fn refresh_jwks(&self) {
        for method in &self.methods {
            if method.static_jwks.is_some() || method.hmac_secret.is_some() {
                continue;
            }
            match self.fetch_jwks(method).await {
                Ok(jwks) => {
                    if let Ok(mut cache) = self.jwks_cache.write() {
                        cache.insert(method.name.clone(), jwks);
                    }
                }
                Err(e) => {
                    warn!(method = %method.name, "Failed to refresh JWKS: {e:#}");
                }
            }
        }
    }

    async fn fetch_jwks(&self, method: &AuthMethod) -> R<Value> {
        let jwks_url = match &method.jwks_url {
            Some(url) => url.clone(),
            None => {
                // OIDC discovery: issuer/.well-known/openid-configuration.
                let issuer = method
                    .issuer
                    .as_ref()
                    .context("oidc method missing issuer")?;
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

    /// Resolves request metadata to an identity. This is the single
    /// authentication entrypoint used by every RPC handler.
    pub fn authenticate(
        &self,
        metadata: &MetadataMap,
        store: &Store,
    ) -> Result<AclIdentity, Status> {
        if !self.enabled {
            return Ok(AclIdentity::acl_disabled());
        }

        let Some(credential) = extract_bearer(metadata) else {
            return self.anonymous_identity(store);
        };

        // Opaque Legion tokens take priority; anything JWT-shaped goes through
        // the configured auth methods.
        if !credential.starts_with(TOKEN_PREFIX) && credential.matches('.').count() == 2 {
            return self.jwt_identity(&credential, store);
        }
        self.token_identity(&credential, store)
    }

    fn anonymous_identity(&self, store: &Store) -> Result<AclIdentity, Status> {
        let policies = vec![ANONYMOUS_POLICY.to_string()];
        let rules = store
            .resolve_acl_rules(&policies)
            .map_err(internal_acl_error)?;
        Ok(AclIdentity {
            principal: "anonymous".into(),
            accessor_id: None,
            is_management: false,
            policies,
            rules,
            source: AclSource::Anonymous,
            expires_at: None,
            audit_only: self.audit_only,
        })
    }

    fn token_identity(&self, secret: &str, store: &Store) -> Result<AclIdentity, Status> {
        let record = store
            .get_acl_token_by_hash(&hash_secret(secret))
            .map_err(internal_acl_error)?
            .ok_or_else(|| Status::unauthenticated("unknown or revoked ACL token"))?;

        if record.is_expired(OffsetDateTime::now_utc()) {
            return Err(Status::unauthenticated(format!(
                "ACL token `{}` has expired; log in again or create a new token",
                record.name
            )));
        }

        let rules = if record.is_management() {
            Vec::new()
        } else {
            store
                .resolve_acl_rules(&record.policies)
                .map_err(internal_acl_error)?
        };

        Ok(AclIdentity {
            principal: record.name.clone(),
            accessor_id: Some(record.accessor_id.clone()),
            is_management: record.is_management(),
            policies: record.policies.clone(),
            rules,
            source: AclSource::Token,
            expires_at: record.expires_at.clone(),
            audit_only: self.audit_only,
        })
    }

    fn jwt_identity(&self, token: &str, store: &Store) -> Result<AclIdentity, Status> {
        if self.methods.is_empty() {
            return Err(Status::unauthenticated(
                "a JWT was presented but no auth methods are configured",
            ));
        }
        let mut last_error = String::new();
        for method in &self.methods {
            match self.validate_jwt(method, token) {
                Ok(claims) => {
                    let principal = claims
                        .get(&method.user_claim)
                        .and_then(Value::as_str)
                        .unwrap_or("jwt-user")
                        .to_string();
                    let policies = bound_policies(&method.binding_rules, &claims);
                    let rules = store
                        .resolve_acl_rules(&policies)
                        .map_err(internal_acl_error)?;
                    return Ok(AclIdentity {
                        principal: format!("{}:{}", method.name, principal),
                        accessor_id: None,
                        is_management: false,
                        policies,
                        rules,
                        source: AclSource::Jwt,
                        expires_at: claims
                            .get("exp")
                            .and_then(Value::as_i64)
                            .and_then(|exp| OffsetDateTime::from_unix_timestamp(exp).ok())
                            .and_then(|dt| dt.format(&Rfc3339).ok()),
                        audit_only: self.audit_only,
                    });
                }
                Err(e) => {
                    debug!(method = %method.name, "JWT validation failed: {e:#}");
                    last_error = format!("{e:#}");
                }
            }
        }
        Err(Status::unauthenticated(format!(
            "JWT was not accepted by any configured auth method (last error: {last_error})"
        )))
    }

    /// Validates signature + bounds for one method and returns the claims.
    fn validate_jwt(&self, method: &AuthMethod, token: &str) -> R<Value> {
        let header = decode_header(token).context("invalid JWT header")?;

        let (key, algorithm) = if let Some(secret) = &method.hmac_secret {
            (
                DecodingKey::from_secret(secret.as_bytes()),
                Algorithm::HS256,
            )
        } else {
            let jwks = self
                .jwks_cache
                .read()
                .ok()
                .and_then(|cache| cache.get(&method.name).cloned())
                .ok_or_else(|| anyhow!("no JWKS available for method `{}` yet", method.name))?;
            decoding_key_from_jwks(&jwks, header.kid.as_deref(), header.alg)?
        };

        let mut validation = Validation::new(algorithm);
        if method.bound_audiences.is_empty() {
            validation.validate_aud = false;
        } else {
            validation.set_audience(&method.bound_audiences);
        }
        let issuer = method.bound_issuer.as_ref().or(method.issuer.as_ref());
        if let Some(issuer) = issuer {
            validation.set_issuer(&[issuer]);
        }

        let data = decode::<Value>(token, &key, &validation).context("JWT validation failed")?;
        Ok(data.claims)
    }

    /// Mints a new opaque ACL token and persists its hash.
    pub fn mint_token(
        &self,
        store: &Store,
        name: &str,
        token_type: &str,
        policies: &[String],
        ttl: Option<Duration>,
    ) -> Result<(AclTokenInfo, String), Status> {
        let record = build_token_record(name, token_type, policies, ttl)
            .map_err(|e| Status::internal(format!("failed to build token: {e:#}")))?;
        let (record, secret) = record;
        store
            .insert_acl_token(&record)
            .map_err(internal_acl_error)?;
        Ok((record.to_info(), secret))
    }

    /// Handles Login for both `userpass` and JWT/OIDC methods.
    pub fn login(
        &self,
        store: &Store,
        method: &str,
        username: Option<&str>,
        password: Option<&str>,
        jwt: Option<&str>,
        requested_ttl: Option<&str>,
    ) -> Result<(AclTokenInfo, String), Status> {
        if method == USERPASS_METHOD {
            let username = username
                .filter(|u| !u.trim().is_empty())
                .ok_or_else(|| Status::invalid_argument("userpass login requires `username`"))?;
            let password = password
                .ok_or_else(|| Status::invalid_argument("userpass login requires `password`"))?;
            let user = store
                .get_acl_user(username)
                .map_err(internal_acl_error)?
                // Same error for unknown user and bad password (no user enumeration).
                .ok_or_else(|| Status::unauthenticated("invalid username or password"))?;
            if !verify_password(password, &user.password_hash) {
                return Err(Status::unauthenticated("invalid username or password"));
            }
            let ttl = self.effective_login_ttl(requested_ttl, self.login_ttl)?;
            return self.mint_token(
                store,
                &format!("login:{username}"),
                "client",
                &user.policies,
                Some(ttl),
            );
        }

        let method_config = self
            .methods
            .iter()
            .find(|m| m.name == method)
            .ok_or_else(|| Status::invalid_argument(format!("unknown auth method `{method}`")))?;
        let jwt = jwt.ok_or_else(|| {
            Status::invalid_argument(format!("auth method `{method}` requires `jwt`"))
        })?;
        let claims = self
            .validate_jwt(method_config, jwt)
            .map_err(|e| Status::unauthenticated(format!("JWT validation failed: {e:#}")))?;
        let principal = claims
            .get(&method_config.user_claim)
            .and_then(Value::as_str)
            .unwrap_or("jwt-user")
            .to_string();
        let policies = bound_policies(&method_config.binding_rules, &claims);
        if policies.is_empty() {
            return Err(Status::permission_denied(format!(
                "login via `{method}` succeeded but no binding rules matched any policies"
            )));
        }
        let ttl = self.effective_login_ttl(requested_ttl, method_config.token_ttl)?;
        self.mint_token(
            store,
            &format!("{method}:{principal}"),
            "client",
            &policies,
            Some(ttl),
        )
    }

    /// Requested TTLs are capped at the method's configured maximum.
    fn effective_login_ttl(
        &self,
        requested: Option<&str>,
        cap: Duration,
    ) -> Result<Duration, Status> {
        match requested {
            None => Ok(cap),
            Some(raw) => {
                let requested = parse_ttl(raw)
                    .map_err(|e| Status::invalid_argument(format!("invalid ttl: {e:#}")))?;
                Ok(requested.min(cap))
            }
        }
    }

    /// One-shot creation of the initial management token.
    pub fn bootstrap(&self, store: &Store) -> Result<(AclTokenInfo, String), Status> {
        let (record, secret) = build_token_record("bootstrap", "management", &[], None)
            .map_err(|e| Status::internal(format!("failed to build token: {e:#}")))?;
        let inserted = store
            .bootstrap_acl_token(&record)
            .map_err(internal_acl_error)?;
        if !inserted {
            return Err(Status::failed_precondition(
                "ACL system is already bootstrapped (a management token exists); \
                 delete all management tokens to re-bootstrap",
            ));
        }
        Ok((record.to_info(), secret))
    }
}

/// Builds a token record plus its secret. The secret is returned exactly once
/// and only its SHA-256 hash is stored.
fn build_token_record(
    name: &str,
    token_type: &str,
    policies: &[String],
    ttl: Option<Duration>,
) -> R<(AclTokenRecord, String)> {
    let now = OffsetDateTime::now_utc();
    let secret = format!("{TOKEN_PREFIX}{}", random_hex(32));
    let expires_at = match ttl {
        Some(ttl) => Some((now + time::Duration::try_from(ttl)?).format(&Rfc3339)?),
        None => None,
    };
    let record = AclTokenRecord {
        accessor_id: random_hex(16),
        secret_hash: hash_secret(&secret),
        name: name.to_string(),
        token_type: token_type.to_string(),
        policies: policies.to_vec(),
        expires_at,
        created_at: now.format(&Rfc3339)?,
    };
    Ok((record, secret))
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

/// Pulls a bearer credential from `authorization` or `x-legion-token`.
fn extract_bearer(metadata: &MetadataMap) -> Option<String> {
    if let Some(value) = metadata.get("authorization").and_then(|v| v.to_str().ok()) {
        let value = value.trim();
        if let Some(rest) = value
            .strip_prefix("Bearer ")
            .or_else(|| value.strip_prefix("bearer "))
        {
            let token = rest.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        } else if !value.is_empty() {
            // Tolerate a bare token in the authorization header.
            return Some(value.to_string());
        }
    }
    metadata
        .get("x-legion-token")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
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

fn internal_acl_error(err: anyhow::Error) -> Status {
    error!("ACL storage error: {err:?}");
    Status::internal("ACL storage error")
}

/// Enforces an authorization decision, emitting a structured audit event on
/// every denial (enforced or audit-only).
///
/// Use via `ManagerAgentService::check_acl` for the common single-line case,
/// or directly after request parsing when the scope comes from the body.
pub fn enforce(
    identity: &AclIdentity,
    resource: AclResource,
    action: AclAction,
    scope: &AclScope,
) -> Result<(), Status> {
    if identity.would_allow(resource, action, scope) {
        return Ok(());
    }

    warn!(
        target: "acl_audit",
        principal = %identity.principal,
        accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
        source = identity.source.as_str(),
        resource = resource.as_str(),
        action = action.as_str(),
        namespace = scope.namespace.as_deref().unwrap_or("-"),
        name = scope.name.as_deref().unwrap_or("-"),
        pool = scope.pool.as_deref().unwrap_or("-"),
        path = scope.path.as_deref().unwrap_or("-"),
        enforced = !identity.audit_only,
        "ACL denial"
    );

    if identity.audit_only {
        return Ok(());
    }

    let hint = if identity.source == AclSource::Anonymous {
        "; no token was provided (set LEGION_TOKEN or run `legion login`)"
    } else {
        ""
    };
    Err(Status::permission_denied(format!(
        "permission denied: {}:{} requires a policy granting it{hint}",
        resource.as_str(),
        action.as_str(),
    )))
}

#[cfg(test)]
mod acl_runtime_test {
    use super::*;
    use crate::structs::{AclConfig, ManagerConfigFull};
    use jsonwebtoken::{encode, EncodingKey, Header};
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-acl-runtime-")
            .tempdir()
            .context("Error creating temp store directory")?
            .keep();
        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("Error parsing store path")?
                .to_string(),
            ..Default::default()
        };
        Store::start_storage(&store_config)
    }

    fn enabled_runtime() -> AclRuntime {
        AclRuntime::from_config(
            &AclConfig {
                enabled: Some(true),
                ..Default::default()
            }
            .with_defaults(),
        )
        .expect("runtime should build")
    }

    fn metadata_with_token(token: &str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            format!("Bearer {token}").parse().expect("valid header"),
        );
        metadata
    }

    #[test]
    fn disabled_runtime_grants_management_identity() -> R<()> {
        let store = setup_store()?;
        let runtime = AclRuntime::disabled();
        let identity = runtime.authenticate(&MetadataMap::new(), &store)?;
        assert!(identity.is_management);
        assert_eq!(identity.source, AclSource::Disabled);
        Ok(())
    }

    #[test]
    fn anonymous_uses_anonymous_policy_when_present() -> R<()> {
        let store = setup_store()?;
        let runtime = enabled_runtime();

        let identity = runtime.authenticate(&MetadataMap::new(), &store)?;
        assert_eq!(identity.principal, "anonymous");
        assert!(!identity.allows(AclResource::Job, AclAction::Read, &AclScope::any()));

        store.upsert_acl_policy(
            ANONYMOUS_POLICY,
            "migration policy",
            r#"[{"resources": ["job"], "actions": ["read"]}]"#,
        )?;
        let identity = runtime.authenticate(&MetadataMap::new(), &store)?;
        assert!(identity.allows(AclResource::Job, AclAction::Read, &AclScope::any()));
        assert!(!identity.allows(AclResource::Job, AclAction::Write, &AclScope::any()));
        Ok(())
    }

    #[test]
    fn token_roundtrip_authenticates_and_enforces_policies() -> R<()> {
        let store = setup_store()?;
        let runtime = enabled_runtime();

        store.upsert_acl_policy(
            "kv-team",
            "",
            r#"[{"resources": ["kv"], "actions": ["read", "write"], "paths": ["team/*"]}]"#,
        )?;
        let (info, secret) = runtime
            .mint_token(&store, "ci", "client", &["kv-team".into()], None)
            .map_err(|s| anyhow!(s.to_string()))?;
        assert!(secret.starts_with(TOKEN_PREFIX));
        assert_eq!(info.token_type, "client");

        let identity = runtime.authenticate(&metadata_with_token(&secret), &store)?;
        assert_eq!(identity.principal, "ci");
        assert_eq!(
            identity.accessor_id.as_deref(),
            Some(info.accessor_id.as_str())
        );
        assert!(identity.allows(AclResource::Kv, AclAction::Write, &AclScope::path("team/a")));
        assert!(!identity.allows(
            AclResource::Kv,
            AclAction::Write,
            &AclScope::path("other/a")
        ));
        assert!(!identity.allows(AclResource::Job, AclAction::Read, &AclScope::any()));

        // Unknown token fails authentication entirely.
        let result = runtime.authenticate(&metadata_with_token("lgn_bogus"), &store);
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
        Ok(())
    }

    #[test]
    fn expired_tokens_are_rejected() -> R<()> {
        let store = setup_store()?;
        let runtime = enabled_runtime();
        let (_, secret) = runtime
            .mint_token(
                &store,
                "fleeting",
                "client",
                &["p".into()],
                Some(Duration::ZERO),
            )
            .map_err(|s| anyhow!(s.to_string()))?;
        std::thread::sleep(Duration::from_millis(5));
        let result = runtime.authenticate(&metadata_with_token(&secret), &store);
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
        Ok(())
    }

    #[test]
    fn management_tokens_bypass_policies() -> R<()> {
        let store = setup_store()?;
        let runtime = enabled_runtime();
        let (_, secret) = runtime
            .bootstrap(&store)
            .map_err(|s| anyhow!(s.to_string()))?;
        let identity = runtime.authenticate(&metadata_with_token(&secret), &store)?;
        assert!(identity.is_management);
        assert!(identity.allows(AclResource::Acl, AclAction::Write, &AclScope::none()));

        // Second bootstrap is refused.
        let again = runtime.bootstrap(&store);
        assert_eq!(again.unwrap_err().code(), tonic::Code::FailedPrecondition);
        Ok(())
    }

    #[test]
    fn userpass_login_mints_scoped_token() -> R<()> {
        let store = setup_store()?;
        let runtime = enabled_runtime();
        store.upsert_acl_policy(
            "readers",
            "",
            r#"[{"resources": ["job"], "actions": ["read"]}]"#,
        )?;
        store.upsert_acl_user(
            "alice",
            Some(&hash_password("hunter2")?),
            &["readers".into()],
        )?;

        // Wrong password and unknown user produce the same error.
        let bad = runtime.login(
            &store,
            USERPASS_METHOD,
            Some("alice"),
            Some("nope"),
            None,
            None,
        );
        assert_eq!(bad.unwrap_err().code(), tonic::Code::Unauthenticated);
        let unknown = runtime.login(&store, USERPASS_METHOD, Some("bob"), Some("x"), None, None);
        assert_eq!(unknown.unwrap_err().code(), tonic::Code::Unauthenticated);

        let (info, secret) = runtime
            .login(
                &store,
                USERPASS_METHOD,
                Some("alice"),
                Some("hunter2"),
                None,
                None,
            )
            .map_err(|s| anyhow!(s.to_string()))?;
        assert_eq!(info.name, "login:alice");
        assert!(info.expires_at.is_some(), "login tokens must expire");

        let identity = runtime.authenticate(&metadata_with_token(&secret), &store)?;
        assert!(identity.allows(AclResource::Job, AclAction::Read, &AclScope::any()));
        assert!(!identity.allows(AclResource::Job, AclAction::Write, &AclScope::any()));
        Ok(())
    }

    fn hs256_runtime_and_jwt(claims: Value) -> R<(AclRuntime, String)> {
        let config = AclConfig {
            enabled: Some(true),
            auth_methods: Some(vec![crate::structs::AclAuthMethodConfig {
                name: "ci".into(),
                r#type: Some("jwt".into()),
                hmac_secret: Some("test-secret".into()),
                bound_audiences: Some(vec!["legion".into()]),
                bound_issuer: Some("https://idp.test".into()),
                user_claim: Some("sub".into()),
                binding_rules: Some(vec![
                    crate::structs::AclBindingRule {
                        claim: Some("groups".into()),
                        value: Some("platform".into()),
                        policies: vec!["ops".into()],
                    },
                    crate::structs::AclBindingRule {
                        claim: None,
                        value: None,
                        policies: vec!["base".into()],
                    },
                ]),
                ..Default::default()
            }]),
            ..Default::default()
        };
        let runtime = AclRuntime::from_config(&config.with_defaults())?;
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-secret"),
        )?;
        Ok((runtime, token))
    }

    fn future_exp() -> i64 {
        (OffsetDateTime::now_utc() + time::Duration::hours(1)).unix_timestamp()
    }

    #[test]
    fn jwt_bearer_maps_claims_to_policies() -> R<()> {
        let store = setup_store()?;
        store.upsert_acl_policy("ops", "", r#"[{"resources": ["node"], "actions": ["*"]}]"#)?;
        store.upsert_acl_policy(
            "base",
            "",
            r#"[{"resources": ["job"], "actions": ["read"]}]"#,
        )?;

        let (runtime, token) = hs256_runtime_and_jwt(json!({
            "sub": "alice@corp",
            "iss": "https://idp.test",
            "aud": "legion",
            "groups": ["platform", "eng"],
            "exp": future_exp(),
        }))?;

        let identity = runtime.authenticate(&metadata_with_token(&token), &store)?;
        assert_eq!(identity.principal, "ci:alice@corp");
        assert_eq!(identity.source, AclSource::Jwt);
        assert!(identity.policies.contains(&"ops".to_string()));
        assert!(identity.policies.contains(&"base".to_string()));
        assert!(identity.allows(
            AclResource::Node,
            AclAction::Write,
            &AclScope::pool("default")
        ));
        Ok(())
    }

    #[test]
    fn jwt_with_wrong_audience_or_issuer_is_rejected() -> R<()> {
        let store = setup_store()?;
        let (runtime, _) = hs256_runtime_and_jwt(json!({}))?;

        let bad_aud = encode(
            &Header::new(Algorithm::HS256),
            &json!({"sub": "x", "iss": "https://idp.test", "aud": "other", "exp": future_exp()}),
            &EncodingKey::from_secret(b"test-secret"),
        )?;
        assert_eq!(
            runtime
                .authenticate(&metadata_with_token(&bad_aud), &store)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );

        let bad_iss = encode(
            &Header::new(Algorithm::HS256),
            &json!({"sub": "x", "iss": "https://evil.test", "aud": "legion", "exp": future_exp()}),
            &EncodingKey::from_secret(b"test-secret"),
        )?;
        assert_eq!(
            runtime
                .authenticate(&metadata_with_token(&bad_iss), &store)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );

        let bad_sig = encode(
            &Header::new(Algorithm::HS256),
            &json!({"sub": "x", "iss": "https://idp.test", "aud": "legion", "exp": future_exp()}),
            &EncodingKey::from_secret(b"wrong-secret"),
        )?;
        assert_eq!(
            runtime
                .authenticate(&metadata_with_token(&bad_sig), &store)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        Ok(())
    }

    #[test]
    fn jwt_login_exchanges_for_acl_token() -> R<()> {
        let store = setup_store()?;
        store.upsert_acl_policy("ops", "", r#"[{"resources": ["node"], "actions": ["*"]}]"#)?;
        store.upsert_acl_policy(
            "base",
            "",
            r#"[{"resources": ["job"], "actions": ["read"]}]"#,
        )?;

        let (runtime, token) = hs256_runtime_and_jwt(json!({
            "sub": "ci-bot",
            "iss": "https://idp.test",
            "aud": "legion",
            "exp": future_exp(),
        }))?;

        let (info, secret) = runtime
            .login(&store, "ci", None, None, Some(&token), None)
            .map_err(|s| anyhow!(s.to_string()))?;
        assert_eq!(info.name, "ci:ci-bot");
        assert!(info.expires_at.is_some());

        let identity = runtime.authenticate(&metadata_with_token(&secret), &store)?;
        assert_eq!(identity.source, AclSource::Token);
        assert!(identity.allows(AclResource::Job, AclAction::Read, &AclScope::any()));
        Ok(())
    }

    #[test]
    fn audit_only_mode_logs_but_allows() -> R<()> {
        let store = setup_store()?;
        let runtime = AclRuntime::from_config(
            &AclConfig {
                enabled: Some(true),
                audit_only: Some(true),
                ..Default::default()
            }
            .with_defaults(),
        )?;
        let identity = runtime.authenticate(&MetadataMap::new(), &store)?;
        assert!(enforce(
            &identity,
            AclResource::Job,
            AclAction::Write,
            &AclScope::any()
        )
        .is_ok());
        assert!(!identity.would_allow(AclResource::Job, AclAction::Write, &AclScope::any()));
        Ok(())
    }

    #[test]
    fn enforce_denies_with_helpful_message() -> R<()> {
        let store = setup_store()?;
        let runtime = enabled_runtime();
        let identity = runtime.authenticate(&MetadataMap::new(), &store)?;
        let err = enforce(
            &identity,
            AclResource::Job,
            AclAction::Write,
            &AclScope::any(),
        )
        .expect_err("anonymous must be denied");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("job:write"));
        assert!(err.message().contains("legion login"));
        Ok(())
    }

    #[test]
    fn password_hashing_roundtrip() -> R<()> {
        let hash = hash_password("s3cret")?;
        assert!(hash.starts_with("$argon2"));
        assert!(verify_password("s3cret", &hash));
        assert!(!verify_password("wrong", &hash));
        assert!(!verify_password("s3cret", "not-a-hash"));
        Ok(())
    }

    #[test]
    fn config_validation_rejects_bad_methods() {
        let reserved = AclConfig {
            enabled: Some(true),
            auth_methods: Some(vec![crate::structs::AclAuthMethodConfig {
                name: USERPASS_METHOD.into(),
                ..Default::default()
            }]),
            ..Default::default()
        };
        assert!(AclRuntime::from_config(&reserved.with_defaults()).is_err());

        let keyless = AclConfig {
            enabled: Some(true),
            auth_methods: Some(vec![crate::structs::AclAuthMethodConfig {
                name: "nokeys".into(),
                r#type: Some("jwt".into()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        assert!(AclRuntime::from_config(&keyless.with_defaults()).is_err());

        let duplicate = AclConfig {
            enabled: Some(true),
            auth_methods: Some(vec![
                crate::structs::AclAuthMethodConfig {
                    name: "a".into(),
                    hmac_secret: Some("x".into()),
                    ..Default::default()
                },
                crate::structs::AclAuthMethodConfig {
                    name: "a".into(),
                    hmac_secret: Some("y".into()),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };
        assert!(AclRuntime::from_config(&duplicate.with_defaults()).is_err());
    }
}
