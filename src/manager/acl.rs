//! The authenticator: resolves request credentials to identities and mints
//! ACL tokens.
//!
//! Every credential type converges on one [`AclIdentity`]:
//!
//! - **Legion ACL tokens** (opaque `lgn_...` secrets) are hashed and looked up
//!   in the store; their attached policies become the identity's rules.
//! - **Auth methods** ([`crate::manager::auth_method::AuthMethod`]) handle
//!   everything else: username/password, JWTs, and OIDC tokens — either per
//!   request (bearer) or once at `Login`, which mints a short-lived ACL token
//!   carrying the method's mapped policies.
//! - **No credential** resolves to the `anonymous` policy when one exists.
//!
//! [`Authenticator`] is the long-lived object the manager holds: the ACL mode
//! flags from config, the registered auth methods, and the token-minting
//! logic. Policy evaluation lives in `crate::structs::acl`; per-endpoint
//! enforcement is [`enforce`].
//!
//! Enforcement is default-deny once ACLs are enabled. In audit-only mode
//! denials are logged under the `acl_audit` tracing target but allowed, so
//! operators can stage policies before turning enforcement on.

// tonic::Status is ~176 bytes; returning it directly matches the rest of the
// gRPC surface and keeps handler call sites to a single `?`.
#![allow(clippy::result_large_err)]

use crate::manager::auth_method::{
    parse_ttl, AuthMethod, JwtAuthMethod, LoginCredentials, UserpassMethod, VerifiedIdentity,
};
use crate::prelude::*;
use crate::storage::acl_store::AclTokenRecord;
use crate::storage::store::Store;
use crate::structs::acl::{AclAction, AclIdentity, AclResource, AclScope, AclSource};
use crate::structs::{AclConfigFull, AclTokenInfo};

pub use crate::manager::auth_method::USERPASS_METHOD;

use argon2::password_hash::{
    rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString,
};
use argon2::Argon2;
use rand::RngCore;
use sha2::{Digest, Sha256};
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

const DEFAULT_LOGIN_TTL: Duration = Duration::from_secs(24 * 60 * 60);

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

/// The manager's authenticator: ACL mode flags, the registered auth methods,
/// and token minting. One instance lives for the life of the manager and is
/// shared by every RPC handler.
pub struct Authenticator {
    pub enabled: bool,
    pub audit_only: bool,
    pub login_ttl: Duration,
    pub jwks_refresh_interval: Duration,
    methods: Vec<Box<dyn AuthMethod>>,
}

impl std::fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authenticator")
            .field("enabled", &self.enabled)
            .field("audit_only", &self.audit_only)
            .field("methods", &self.method_names())
            .finish()
    }
}

impl Authenticator {
    /// Authenticator with ACLs off; every caller gets full access.
    pub fn disabled() -> Self {
        Authenticator {
            enabled: false,
            audit_only: false,
            login_ttl: DEFAULT_LOGIN_TTL,
            jwks_refresh_interval: Duration::from_secs(300),
            methods: vec![Box::new(UserpassMethod::new(DEFAULT_LOGIN_TTL))],
        }
    }

    pub fn from_config(config: &AclConfigFull) -> R<Self> {
        let login_ttl = parse_ttl(&config.login_ttl).context("invalid acl.loginTtl")?;
        let jwks_refresh_interval =
            parse_ttl(&config.jwks_refresh_interval).context("invalid acl.jwksRefreshInterval")?;
        let mut authenticator = Authenticator {
            enabled: config.enabled,
            audit_only: config.audit_only,
            login_ttl,
            jwks_refresh_interval,
            methods: Vec::new(),
        };
        // The built-in username/password method is always available.
        authenticator.register_method(Box::new(UserpassMethod::new(login_ttl)))?;
        for method_config in &config.auth_methods {
            let method = JwtAuthMethod::from_config(method_config, login_ttl)?;
            authenticator.register_method(Box::new(method))?;
        }
        Ok(authenticator)
    }

    /// Registers an auth method. Names must be unique (`userpass` is taken by
    /// the built-in method).
    ///
    /// This is the extension seam for custom identity sources when embedding
    /// Legion as a library; see `crate::manager::auth_method` for the planned
    /// plugin story.
    pub fn register_method(&mut self, method: Box<dyn AuthMethod>) -> R<()> {
        if method.name().trim().is_empty() {
            bail!("auth method name must not be empty");
        }
        if self.methods.iter().any(|m| m.name() == method.name()) {
            bail!("auth method `{}` is already registered", method.name());
        }
        self.methods.push(method);
        Ok(())
    }

    /// Names of all registered auth methods (always includes `userpass`).
    pub fn method_names(&self) -> Vec<&str> {
        self.methods.iter().map(|m| m.name()).collect()
    }

    /// Refreshes remote validation material (JWKS, ...) for every method.
    ///
    /// Called once at startup and then every `jwksRefreshInterval`. Methods
    /// swallow their own failures so a flaky identity provider cannot lock
    /// out authentication.
    pub async fn refresh_auth_methods(&self) {
        for method in &self.methods {
            method.refresh().await;
        }
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
        // the registered auth methods.
        if !credential.starts_with(TOKEN_PREFIX) && credential.matches('.').count() == 2 {
            return self.bearer_identity(&credential, store);
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

    /// Routes a JWT-shaped bearer credential through the auth methods; the
    /// first method that validates it wins.
    fn bearer_identity(&self, credential: &str, store: &Store) -> Result<AclIdentity, Status> {
        let mut attempted = false;
        let mut last_error = String::new();
        for method in &self.methods {
            let Some(result) = method.validate_bearer(credential) else {
                continue;
            };
            attempted = true;
            match result {
                Ok(verified) => return self.method_identity(method.name(), verified, store),
                Err(e) => {
                    let message = e.to_status().message().to_string();
                    debug!(method = %method.name(), "bearer validation failed: {message}");
                    last_error = message;
                }
            }
        }
        if !attempted {
            return Err(Status::unauthenticated(
                "a JWT was presented but no JWT/OIDC auth methods are configured",
            ));
        }
        Err(Status::unauthenticated(format!(
            "JWT was not accepted by any configured auth method (last error: {last_error})"
        )))
    }

    fn method_identity(
        &self,
        method_name: &str,
        verified: VerifiedIdentity,
        store: &Store,
    ) -> Result<AclIdentity, Status> {
        let rules = store
            .resolve_acl_rules(&verified.policies)
            .map_err(internal_acl_error)?;
        Ok(AclIdentity {
            principal: format!("{method_name}:{}", verified.principal),
            accessor_id: None,
            is_management: false,
            policies: verified.policies,
            rules,
            source: AclSource::Jwt,
            expires_at: verified.expires_at,
            audit_only: self.audit_only,
        })
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

    /// Handles Login: dispatches to the named auth method and mints an ACL
    /// token carrying the verified identity's policies.
    ///
    /// Token names are `<method>:<principal>` (for example `userpass:alice`)
    /// so audit logs show both the method and the external identity.
    pub fn login(
        &self,
        store: &Store,
        method_name: &str,
        username: Option<&str>,
        password: Option<&str>,
        jwt: Option<&str>,
        requested_ttl: Option<&str>,
    ) -> Result<(AclTokenInfo, String), Status> {
        let method = self
            .methods
            .iter()
            .find(|m| m.name() == method_name)
            .ok_or_else(|| {
                Status::invalid_argument(format!("unknown auth method `{method_name}`"))
            })?;
        let verified = method
            .login(
                store,
                LoginCredentials {
                    username,
                    password,
                    jwt,
                },
            )
            .map_err(|e| e.to_status())?;
        let ttl = self.effective_login_ttl(requested_ttl, method.login_token_ttl())?;
        self.mint_token(
            store,
            &format!("{method_name}:{}", verified.principal),
            "client",
            &verified.policies,
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
mod authenticator_test {
    use super::*;
    use crate::manager::auth_method::AuthError;
    use crate::structs::{AclConfig, ManagerConfigFull};
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-acl-auth-")
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

    fn enabled_authenticator() -> Authenticator {
        Authenticator::from_config(
            &AclConfig {
                enabled: Some(true),
                ..Default::default()
            }
            .with_defaults(),
        )
        .expect("authenticator should build")
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
    fn disabled_authenticator_grants_management_identity() -> R<()> {
        let store = setup_store()?;
        let auth = Authenticator::disabled();
        let identity = auth.authenticate(&MetadataMap::new(), &store)?;
        assert!(identity.is_management);
        assert_eq!(identity.source, AclSource::Disabled);
        Ok(())
    }

    #[test]
    fn anonymous_uses_anonymous_policy_when_present() -> R<()> {
        let store = setup_store()?;
        let auth = enabled_authenticator();

        let identity = auth.authenticate(&MetadataMap::new(), &store)?;
        assert_eq!(identity.principal, "anonymous");
        assert!(!identity.allows(AclResource::Job, AclAction::Read, &AclScope::any()));

        store.upsert_acl_policy(
            ANONYMOUS_POLICY,
            "migration policy",
            r#"[{"resources": ["job"], "actions": ["read"]}]"#,
        )?;
        let identity = auth.authenticate(&MetadataMap::new(), &store)?;
        assert!(identity.allows(AclResource::Job, AclAction::Read, &AclScope::any()));
        assert!(!identity.allows(AclResource::Job, AclAction::Write, &AclScope::any()));
        Ok(())
    }

    #[test]
    fn token_roundtrip_authenticates_and_enforces_policies() -> R<()> {
        let store = setup_store()?;
        let auth = enabled_authenticator();

        store.upsert_acl_policy(
            "kv-team",
            "",
            r#"[{"resources": ["kv"], "actions": ["read", "write"], "paths": ["team/*"]}]"#,
        )?;
        let (info, secret) = auth
            .mint_token(&store, "ci", "client", &["kv-team".into()], None)
            .map_err(|s| anyhow!(s.to_string()))?;
        assert!(secret.starts_with(TOKEN_PREFIX));
        assert_eq!(info.token_type, "client");

        let identity = auth.authenticate(&metadata_with_token(&secret), &store)?;
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
        let result = auth.authenticate(&metadata_with_token("lgn_bogus"), &store);
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
        Ok(())
    }

    #[test]
    fn expired_tokens_are_rejected() -> R<()> {
        let store = setup_store()?;
        let auth = enabled_authenticator();
        let (_, secret) = auth
            .mint_token(
                &store,
                "fleeting",
                "client",
                &["p".into()],
                Some(Duration::ZERO),
            )
            .map_err(|s| anyhow!(s.to_string()))?;
        std::thread::sleep(Duration::from_millis(5));
        let result = auth.authenticate(&metadata_with_token(&secret), &store);
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
        Ok(())
    }

    #[test]
    fn management_tokens_bypass_policies() -> R<()> {
        let store = setup_store()?;
        let auth = enabled_authenticator();
        let (_, secret) = auth.bootstrap(&store).map_err(|s| anyhow!(s.to_string()))?;
        let identity = auth.authenticate(&metadata_with_token(&secret), &store)?;
        assert!(identity.is_management);
        assert!(identity.allows(AclResource::Acl, AclAction::Write, &AclScope::none()));

        // Second bootstrap is refused.
        let again = auth.bootstrap(&store);
        assert_eq!(again.unwrap_err().code(), tonic::Code::FailedPrecondition);
        Ok(())
    }

    #[test]
    fn userpass_login_mints_scoped_token() -> R<()> {
        let store = setup_store()?;
        let auth = enabled_authenticator();
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
        let bad = auth.login(
            &store,
            USERPASS_METHOD,
            Some("alice"),
            Some("nope"),
            None,
            None,
        );
        assert_eq!(bad.unwrap_err().code(), tonic::Code::Unauthenticated);
        let unknown = auth.login(&store, USERPASS_METHOD, Some("bob"), Some("x"), None, None);
        assert_eq!(unknown.unwrap_err().code(), tonic::Code::Unauthenticated);

        let (info, secret) = auth
            .login(
                &store,
                USERPASS_METHOD,
                Some("alice"),
                Some("hunter2"),
                None,
                None,
            )
            .map_err(|s| anyhow!(s.to_string()))?;
        assert_eq!(info.name, "userpass:alice");
        assert!(info.expires_at.is_some(), "login tokens must expire");

        let identity = auth.authenticate(&metadata_with_token(&secret), &store)?;
        assert!(identity.allows(AclResource::Job, AclAction::Read, &AclScope::any()));
        assert!(!identity.allows(AclResource::Job, AclAction::Write, &AclScope::any()));
        Ok(())
    }

    fn hs256_authenticator_and_jwt(claims: Value) -> R<(Authenticator, String)> {
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
        let auth = Authenticator::from_config(&config.with_defaults())?;
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-secret"),
        )?;
        Ok((auth, token))
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

        let (auth, token) = hs256_authenticator_and_jwt(json!({
            "sub": "alice@corp",
            "iss": "https://idp.test",
            "aud": "legion",
            "groups": ["platform", "eng"],
            "exp": future_exp(),
        }))?;

        let identity = auth.authenticate(&metadata_with_token(&token), &store)?;
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
        let (auth, _) = hs256_authenticator_and_jwt(json!({}))?;

        let bad_aud = encode(
            &Header::new(Algorithm::HS256),
            &json!({"sub": "x", "iss": "https://idp.test", "aud": "other", "exp": future_exp()}),
            &EncodingKey::from_secret(b"test-secret"),
        )?;
        assert_eq!(
            auth.authenticate(&metadata_with_token(&bad_aud), &store)
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
            auth.authenticate(&metadata_with_token(&bad_iss), &store)
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
            auth.authenticate(&metadata_with_token(&bad_sig), &store)
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

        let (auth, token) = hs256_authenticator_and_jwt(json!({
            "sub": "ci-bot",
            "iss": "https://idp.test",
            "aud": "legion",
            "exp": future_exp(),
        }))?;

        let (info, secret) = auth
            .login(&store, "ci", None, None, Some(&token), None)
            .map_err(|s| anyhow!(s.to_string()))?;
        assert_eq!(info.name, "ci:ci-bot");
        assert!(info.expires_at.is_some());

        let identity = auth.authenticate(&metadata_with_token(&secret), &store)?;
        assert_eq!(identity.source, AclSource::Token);
        assert!(identity.allows(AclResource::Job, AclAction::Read, &AclScope::any()));
        Ok(())
    }

    /// A minimal custom auth method, proving the registration seam works for
    /// library users (and, later, plugins) without touching the core.
    struct StaticTestMethod;

    impl AuthMethod for StaticTestMethod {
        fn name(&self) -> &str {
            "static-test"
        }

        fn login(
            &self,
            _store: &Store,
            credentials: LoginCredentials<'_>,
        ) -> Result<VerifiedIdentity, AuthError> {
            if credentials.password == Some("letmein") {
                Ok(VerifiedIdentity {
                    principal: "robot".into(),
                    policies: vec!["readers".into()],
                    expires_at: None,
                })
            } else {
                Err(AuthError::InvalidCredentials("bad static secret".into()))
            }
        }

        fn login_token_ttl(&self) -> Duration {
            Duration::from_secs(60)
        }
    }

    #[test]
    fn custom_auth_methods_can_be_registered() -> R<()> {
        let store = setup_store()?;
        store.upsert_acl_policy(
            "readers",
            "",
            r#"[{"resources": ["job"], "actions": ["read"]}]"#,
        )?;

        let mut auth = enabled_authenticator();
        auth.register_method(Box::new(StaticTestMethod))?;
        assert!(auth.method_names().contains(&"static-test"));

        // Duplicate names are rejected (userpass is pre-registered).
        assert!(auth.register_method(Box::new(StaticTestMethod)).is_err());
        assert!(auth
            .register_method(Box::new(UserpassMethod::new(Duration::from_secs(1))))
            .is_err());

        let bad = auth.login(&store, "static-test", None, Some("wrong"), None, None);
        assert_eq!(bad.unwrap_err().code(), tonic::Code::Unauthenticated);

        let (info, secret) = auth
            .login(&store, "static-test", None, Some("letmein"), None, None)
            .map_err(|s| anyhow!(s.to_string()))?;
        assert_eq!(info.name, "static-test:robot");

        let identity = auth.authenticate(&metadata_with_token(&secret), &store)?;
        assert_eq!(identity.principal, "static-test:robot");
        assert!(identity.allows(AclResource::Job, AclAction::Read, &AclScope::any()));
        assert!(!identity.allows(AclResource::Job, AclAction::Write, &AclScope::any()));
        Ok(())
    }

    #[test]
    fn audit_only_mode_logs_but_allows() -> R<()> {
        let store = setup_store()?;
        let auth = Authenticator::from_config(
            &AclConfig {
                enabled: Some(true),
                audit_only: Some(true),
                ..Default::default()
            }
            .with_defaults(),
        )?;
        let identity = auth.authenticate(&MetadataMap::new(), &store)?;
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
        let auth = enabled_authenticator();
        let identity = auth.authenticate(&MetadataMap::new(), &store)?;
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
                hmac_secret: Some("x".into()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        assert!(Authenticator::from_config(&reserved.with_defaults()).is_err());

        let keyless = AclConfig {
            enabled: Some(true),
            auth_methods: Some(vec![crate::structs::AclAuthMethodConfig {
                name: "nokeys".into(),
                r#type: Some("jwt".into()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        assert!(Authenticator::from_config(&keyless.with_defaults()).is_err());

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
        assert!(Authenticator::from_config(&duplicate.with_defaults()).is_err());
    }
}
