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
    method_from_config, parse_ttl, AuthMethod, LoginCredentials, MethodPublicInfo, UserpassMethod,
    VerifiedIdentity,
};
use crate::prelude::*;
use crate::storage::acl_store::{AclTokenRecord, JoinTokenRecord};
use crate::storage::store::Store;
use crate::structs::acl::{AclAction, AclIdentity, AclResource, AclScope, AclSource};
use crate::structs::token::{JoinToken, JOIN_SECRET_PREFIX};
use crate::structs::{AclConfigFull, AclTokenInfo, JoinTokenInfo};

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

/// A fresh random ACL token secret (`lgn_` plus 32 random bytes in hex).
pub fn generate_token_secret() -> String {
    format!("{TOKEN_PREFIX}{}", random_hex(32))
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

/// Token type for per-node worker credentials minted at registration.
pub const WORKER_TOKEN_TYPE: &str = "worker";

/// The manager's authenticator: ACL mode flags, the registered auth methods,
/// and token minting. One instance lives for the life of the manager and is
/// shared by every RPC handler.
pub struct Authenticator {
    pub enabled: bool,
    pub audit_only: bool,
    pub login_ttl: Duration,
    /// Upper bound for client-token TTLs (CreateAclToken and Login).
    /// `None` means unbounded; management tokens are always exempt.
    pub token_max_ttl: Option<Duration>,
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
            token_max_ttl: None,
            jwks_refresh_interval: Duration::from_secs(300),
            methods: vec![Box::new(UserpassMethod::new(DEFAULT_LOGIN_TTL))],
        }
    }

    pub fn from_config(config: &AclConfigFull) -> R<Self> {
        let login_ttl = parse_ttl(&config.login_ttl).context("invalid acl.loginTtl")?;
        let token_max_ttl = config
            .token_max_ttl
            .as_deref()
            .map(parse_ttl)
            .transpose()
            .context("invalid acl.tokenMaxTtl")?;
        let jwks_refresh_interval =
            parse_ttl(&config.jwks_refresh_interval).context("invalid acl.jwksRefreshInterval")?;
        let mut authenticator = Authenticator {
            enabled: config.enabled,
            audit_only: config.audit_only,
            login_ttl,
            token_max_ttl,
            jwks_refresh_interval,
            methods: Vec::new(),
        };
        // The built-in username/password method is always available.
        authenticator.register_method(Box::new(UserpassMethod::new(login_ttl)))?;
        for method_config in &config.auth_methods {
            let method = method_from_config(method_config, login_ttl)?;
            authenticator.register_method(method)?;
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

    /// Public, secret-free metadata for every registered method (served by
    /// `GetAuthMethods` for login UIs).
    pub fn method_infos(&self) -> Vec<MethodPublicInfo> {
        self.methods.iter().map(|m| m.public_info()).collect()
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
            is_worker: false,
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
        self.record_identity(record, store)
    }

    /// The identity of the token with this accessor id, as a request
    /// carrying its secret would authenticate: `Ok(None)` once the token is
    /// deleted, `Unauthenticated` once it has expired. How a direct exec
    /// session, which holds only the minting token's accessor, is re-checked
    /// (docs/exec-and-actions.md, "Direct exec").
    pub fn identity_for_accessor(
        &self,
        accessor_id: &str,
        store: &Store,
    ) -> Result<Option<AclIdentity>, Status> {
        if !self.enabled {
            return Ok(Some(AclIdentity::acl_disabled()));
        }
        match store
            .get_acl_token_by_accessor(accessor_id)
            .map_err(internal_acl_error)?
        {
            Some(record) => self.record_identity(record, store).map(Some),
            None => Ok(None),
        }
    }

    fn record_identity(
        &self,
        record: AclTokenRecord,
        store: &Store,
    ) -> Result<AclIdentity, Status> {
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
            is_worker: record.token_type == WORKER_TOKEN_TYPE,
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
            is_worker: false,
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
        let record = build_token_record(name, token_type, policies, ttl, None)
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
    pub async fn login(
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
            .await
            .map_err(|e| e.to_status())?;
        let mut ttl = self.effective_login_ttl(requested_ttl, method.login_token_ttl())?;
        if method.clamps_to_credential_expiry() {
            ttl = ttl.min(remaining_lifetime(verified.expires_at.as_deref())?);
        }
        self.mint_token(
            store,
            &format!("{method_name}:{}", verified.principal),
            "client",
            &verified.policies,
            Some(ttl),
        )
    }

    /// Requested TTLs are capped at the method's configured maximum and the
    /// global `acl.tokenMaxTtl` bound.
    fn effective_login_ttl(
        &self,
        requested: Option<&str>,
        cap: Duration,
    ) -> Result<Duration, Status> {
        let ttl = match requested {
            None => cap,
            Some(raw) => {
                let requested = parse_ttl(raw)
                    .map_err(|e| Status::invalid_argument(format!("invalid ttl: {e:#}")))?;
                requested.min(cap)
            }
        };
        Ok(match self.token_max_ttl {
            Some(max) => ttl.min(max),
            None => ttl,
        })
    }

    /// Mints the initial management token when no management token exists.
    ///
    /// Called by the manager at startup (and on raft control-lane leadership
    /// acquisition), never by an RPC: there is no open bootstrap endpoint, so
    /// no local workload can race the operator to it
    /// (docs/auth-and-acls.md, "Quick start"). `provided_secret` comes from
    /// `acl.bootstrapSecretFile` for infrastructure-as-code pre-seeding and
    /// must be at least 24 characters. Returns `None` (minting nothing) when a
    /// management token already exists; the existence check and the insert
    /// share one replicated transaction, so two managers cannot both mint.
    pub fn mint_initial_management_token(
        &self,
        store: &Store,
        provided_secret: Option<&str>,
    ) -> Result<Option<(AclTokenInfo, String)>, Status> {
        if let Some(secret) = provided_secret {
            if secret.len() < 24 {
                return Err(Status::invalid_argument(
                    "acl.bootstrapSecretFile must hold a secret of at least 24 characters",
                ));
            }
        }
        let (record, secret) = build_token_record(
            "bootstrap",
            "management",
            &[],
            None,
            provided_secret.map(str::to_string),
        )
        .map_err(|e| Status::internal(format!("failed to build token: {e:#}")))?;
        let inserted = store
            .bootstrap_acl_token(&record)
            .map_err(internal_acl_error)?;
        if !inserted {
            return Ok(None);
        }
        Ok(Some((record.to_info(), secret)))
    }

    /// Mints (or rotates) the per-node worker credential returned by
    /// `WorkerRegister`. Any previous token for the node is revoked first, so
    /// re-registering is also how a node credential gets rotated.
    pub fn mint_node_token(&self, store: &Store, node_id: &str) -> Result<String, Status> {
        store
            .delete_node_tokens(node_id)
            .map_err(internal_acl_error)?;
        let (_, secret) = self.mint_token(
            store,
            &format!("node:{node_id}"),
            WORKER_TOKEN_TYPE,
            &[],
            None,
        )?;
        Ok(secret)
    }

    /// Creates a worker join token: stores the secret hash and returns the
    /// composite token (`lgnjoin_...`) that introduces a worker in one
    /// command.
    pub fn mint_join_token(
        &self,
        store: &Store,
        advertise_addr: &str,
        worker_type: &str,
        ttl: Option<Duration>,
        description: &str,
    ) -> Result<(JoinTokenInfo, String), Status> {
        self.mint_join_token_with(
            store,
            advertise_addr,
            worker_type,
            ttl,
            description,
            None,
            None,
            Default::default(),
        )
    }

    /// [`Self::mint_join_token`] with a use limit and attributes stamped on
    /// every node that registers with it (`CreateJoinToken.max_uses` and
    /// `attributes`). The attributes must already be checked
    /// (`check_join_attributes`).
    #[allow(clippy::too_many_arguments)]
    pub fn mint_join_token_for_nodes(
        &self,
        store: &Store,
        advertise_addr: &str,
        worker_type: &str,
        ttl: Option<Duration>,
        description: &str,
        max_uses: Option<u32>,
        attributes: std::collections::BTreeMap<String, String>,
    ) -> Result<(JoinTokenInfo, String), Status> {
        self.mint_join_token_with(
            store,
            advertise_addr,
            worker_type,
            ttl,
            description,
            None,
            max_uses,
            attributes,
        )
    }

    /// [`Self::mint_join_token`] for a manager's own in-process worker: on a
    /// raft follower the token's row is forwarded to the control lane's
    /// leader (`ForwardLaneWrite`) instead of failing, so every manager can
    /// start its system worker, not only the leader. Client RPCs keep the
    /// non-forwarding mint, and follow the leader hint as before.
    ///
    /// `pinned_attrs` (a JSON object) are the node attributes a registration
    /// through this token gets whatever the worker reports: the manager's
    /// own settings for its worker's `namespaces` and `legion.role`.
    pub fn mint_local_worker_join_token(
        &self,
        store: &Store,
        advertise_addr: &str,
        worker_type: &str,
        ttl: Option<Duration>,
        description: &str,
        pinned_attrs: &Value,
    ) -> Result<(JoinTokenInfo, String), Status> {
        self.mint_join_token_with(
            store,
            advertise_addr,
            worker_type,
            ttl,
            description,
            Some(pinned_attrs.to_string()),
            None,
            Default::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn mint_join_token_with(
        &self,
        store: &Store,
        advertise_addr: &str,
        worker_type: &str,
        ttl: Option<Duration>,
        description: &str,
        // Set only for a manager's own in-process worker, whose token is
        // also the one forwarded to the control leader.
        pinned_attrs: Option<String>,
        max_uses: Option<u32>,
        attributes: std::collections::BTreeMap<String, String>,
    ) -> Result<(JoinTokenInfo, String), Status> {
        let forward_to_leader = pinned_attrs.is_some();
        let now = OffsetDateTime::now_utc();
        let secret = format!("{JOIN_SECRET_PREFIX}{}", random_hex(24));
        let expires_at = match ttl {
            Some(ttl) => Some(
                time::Duration::try_from(ttl)
                    .ok()
                    .map(|delta| now + delta)
                    .and_then(|expiry| expiry.format(&Rfc3339).ok())
                    .ok_or_else(|| Status::invalid_argument("join token ttl out of range"))?,
            ),
            None => None,
        };
        let record = JoinTokenRecord {
            id: random_hex(8),
            secret_hash: hash_secret(&secret),
            worker_type: worker_type.to_string(),
            description: description.to_string(),
            expires_at,
            created_at: now
                .format(&Rfc3339)
                .map_err(|e| Status::internal(format!("clock error: {e}")))?,
            pinned_attrs,
            max_uses,
            uses: 0,
            attributes,
        };
        // The token pins the CA the managers sign with, so a joining worker
        // can tell its manager from whoever answers at `addr` (plan §6.5).
        // Created here on a cluster's first token.
        let ca_hash = store
            .cluster_ca()
            .ensure(store)
            .map(|view| view.pin)
            .map_err(|e| {
                error!(error = %e, "Failed to read or create the cluster CA for a join token");
                // A misconfigured CA needs an operator; anything else may
                // clear on retry (the CA not replicated yet, a signer down).
                let code = match e {
                    crate::manager::cluster_ca::CaError::FailedPrecondition(_) => {
                        tonic::Code::FailedPrecondition
                    }
                    _ => tonic::Code::Unavailable,
                };
                Status::new(
                    code,
                    format!("the cluster CA is unavailable, so a join token can't pin it: {e}"),
                )
            })?;
        if forward_to_leader {
            store.insert_join_token_forwarding(&record)
        } else {
            store.insert_join_token(&record)
        }
        .map_err(internal_acl_error)?;
        let composite = JoinToken::new(advertise_addr, secret, worker_type, ca_hash)
            .encode()
            .map_err(|e| Status::internal(format!("failed to encode join token: {e:#}")))?;
        Ok((record.to_info(), composite))
    }

    /// Validates a join secret presented at worker registration. Only called
    /// when ACLs are enforced.
    pub fn verify_join_secret(
        &self,
        store: &Store,
        secret: Option<&str>,
    ) -> Result<JoinTokenRecord, Status> {
        let secret = secret.filter(|s| !s.trim().is_empty()).ok_or_else(|| {
            Status::unauthenticated(
                "worker registration requires a join token when ACLs are enabled; \
                 create one with `legion join-token create`",
            )
        })?;
        let record = store
            .get_join_token_by_hash(&hash_secret(secret))
            .map_err(internal_acl_error)?
            .ok_or_else(|| Status::unauthenticated("unknown or revoked join token"))?;
        if record.is_expired(OffsetDateTime::now_utc()) {
            return Err(Status::unauthenticated(
                "join token has expired; create a new one with `legion join-token create`",
            ));
        }
        Ok(record)
    }
}

/// Builds a token record plus its secret. The secret is returned exactly once
/// and only its SHA-256 hash is stored.
fn build_token_record(
    name: &str,
    token_type: &str,
    policies: &[String],
    ttl: Option<Duration>,
    provided_secret: Option<String>,
) -> R<(AclTokenRecord, String)> {
    let now = OffsetDateTime::now_utc();
    let secret = provided_secret.unwrap_or_else(|| format!("{TOKEN_PREFIX}{}", random_hex(32)));
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

/// Worker-plane gate for `Heartbeat` and `HealthCheck`: requires a node token
/// (minted at registration) or a management token. Emits an `acl_audit`
/// denial event like `enforce`.
pub fn enforce_worker_plane(identity: &AclIdentity) -> Result<(), Status> {
    if identity.is_management || identity.is_worker {
        return Ok(());
    }

    warn!(
        target: "acl_audit",
        principal = %identity.principal,
        accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
        source = identity.source.as_str(),
        resource = "worker-plane",
        action = "call",
        enforced = !identity.audit_only,
        "ACL denial"
    );

    if identity.audit_only {
        return Ok(());
    }

    Err(Status::unauthenticated(
        "worker-plane endpoints require a node token (returned by WorkerRegister) \
         or a management token",
    ))
}

/// The node a worker credential speaks for: `Some(id)` for a per-node worker
/// token (principal `node:<id>`), `None` for every other identity.
pub fn worker_node_id(identity: &AclIdentity) -> Option<&str> {
    if !identity.is_worker {
        return None;
    }
    identity.principal.strip_prefix("node:")
}

/// Binds a worker-plane request's node id to the caller. A worker token may
/// only act as its own node: without this, any node could heartbeat, report
/// health, or ratify claims as a peer. Management identities (and disabled
/// ACLs) act for any node. Audit-only mode logs the mismatch and allows it,
/// matching `enforce_worker_plane`.
pub fn enforce_node_binding(identity: &AclIdentity, node_id: &str) -> Result<(), Status> {
    if identity.is_management || worker_node_id(identity) == Some(node_id) {
        return Ok(());
    }

    warn!(
        target: "acl_audit",
        principal = %identity.principal,
        accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
        source = identity.source.as_str(),
        resource = "worker-plane",
        action = "call",
        node_id = %node_id,
        enforced = !identity.audit_only,
        "ACL denial: node id does not match the caller's node token"
    );

    if identity.audit_only {
        return Ok(());
    }

    Err(Status::permission_denied(format!(
        "{:?} may not act as node {node_id:?}; a node token only speaks for its own node",
        identity.principal
    )))
}

/// How often a long-lived stream (`StreamEvents`, exec sessions) re-checks
/// its caller's credential. A revoked or expired token, or a policy that no
/// longer grants the stream, ends the stream within one interval.
pub const STREAM_REAUTH_INTERVAL: Duration = Duration::from_secs(30);

/// The credential a long-lived stream was opened with, kept so the stream can
/// re-authenticate while it runs. Authorization is checked once when a stream
/// opens; without this, a stream outlives its token's revocation or expiry.
#[derive(Clone)]
pub struct StreamCredential {
    auth: std::sync::Arc<Authenticator>,
    /// Only the credential headers, not the whole request metadata.
    metadata: MetadataMap,
    /// How often to re-check (`STREAM_REAUTH_INTERVAL` outside tests).
    pub interval: Duration,
}

impl StreamCredential {
    pub fn new(auth: std::sync::Arc<Authenticator>, metadata: &MetadataMap) -> Self {
        let mut kept = MetadataMap::new();
        for header in ["authorization", "x-legion-token"] {
            if let Some(value) = metadata.get(header) {
                kept.insert(header, value.clone());
            }
        }
        StreamCredential {
            auth,
            metadata: kept,
            interval: STREAM_REAUTH_INTERVAL,
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Resolves the credential again, as if the caller had sent a new
    /// request. Fails with `Unauthenticated` once the token is revoked or
    /// expired.
    pub fn reauthenticate(&self, store: &Store) -> Result<AclIdentity, Status> {
        self.auth.authenticate(&self.metadata, store)
    }
}

/// Pulls a bearer credential from `authorization` or `x-legion-token`.
pub(crate) fn extract_bearer(metadata: &MetadataMap) -> Option<String> {
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
        template = scope.template.as_deref().unwrap_or("-"),
        enforced = !identity.audit_only,
        "ACL denial"
    );

    if identity.audit_only {
        return Ok(());
    }

    // A caller with no credential is unauthenticated, not unauthorized: it
    // has not said who it is. The hint names where the token lives on the
    // manager's own machine, since that is where a fresh cluster's operator
    // is most often standing.
    if identity.source == AclSource::Anonymous {
        return Err(Status::unauthenticated(format!(
            "{}:{} requires a token, and no token was provided\n\
             Hint: set LEGION_TOKEN or run `legion login`. On the manager's machine, the \
             initial management token is in <dataDir>/bootstrap-token and \
             ~/.legion/tokens/<host>_<port>",
            resource.as_str(),
            action.as_str(),
        )));
    }
    // Where the token CAN act, from its own rules: the denial names no
    // target (a scope derived from a lookup would leak it), only the
    // caller's reach.
    let reach = if resource.is_namespaced() && scope.namespace.is_some() {
        identity.namespaces_granting(resource, action)
    } else {
        Vec::new()
    };
    if !reach.is_empty() {
        return Err(Status::permission_denied(format!(
            "permission denied: {}:{} requires a policy granting it in this namespace
             Hint: this token can {} {} in {}{}",
            resource.as_str(),
            action.as_str(),
            action.as_str(),
            resource.as_str(),
            reach.join(", "),
            reserved_hint(scope),
        )));
    }
    Err(Status::permission_denied(format!(
        "permission denied: {}:{} requires a policy granting it{}",
        resource.as_str(),
        action.as_str(),
        reserved_hint(scope),
    )))
}

/// The extra hint for a denial inside a reserved name: a `legion-*`
/// namespace or a key under `legion/`, which a token's `*` does not reach
/// (docs/namespaces.md, "Reserved namespaces"). It names only the rule, not
/// the target, which the caller already sent.
fn reserved_hint(scope: &AclScope) -> &'static str {
    if scope
        .namespace
        .as_deref()
        .is_some_and(crate::structs::namespace::is_reserved_namespace)
    {
        "\nHint: `*` does not reach reserved `legion-*` namespaces; a rule must name them, \
         as `namespaces = [\"legion-system\"]` or `[\"*\", \"legion-*\"]`"
    } else if scope
        .path
        .as_deref()
        .is_some_and(|path| path.starts_with(crate::structs::acl::RESERVED_KV_PREFIX))
    {
        "\nHint: `*` does not reach keys under the reserved `legion/` prefix; a rule must \
         name it, as `paths = [\"legion/*\"]`"
    } else {
        ""
    }
}

/// How long a presented credential has left, from its RFC 3339 expiry. A
/// credential that names none, or has already expired, cannot back a token.
fn remaining_lifetime(expires_at: Option<&str>) -> Result<Duration, Status> {
    let expires_at = expires_at
        .and_then(|raw| OffsetDateTime::parse(raw, &Rfc3339).ok())
        .ok_or_else(|| {
            Status::unauthenticated("the identity token names no expiry, so it cannot back a login")
        })?;
    Duration::try_from(expires_at - OffsetDateTime::now_utc())
        .ok()
        .filter(|left| !left.is_zero())
        .ok_or_else(|| Status::unauthenticated("the identity token has expired"))
}

#[cfg(test)]
mod authenticator_test {
    use super::*;
    use crate::manager::auth_method::AuthError;
    use crate::structs::{AclConfig, ManagerConfigFull};
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use tempfile::Builder;

    /// A workload-identity login lasts no longer than the identity token it
    /// was minted from, and an expired or expiry-less token backs nothing.
    #[test]
    fn workload_logins_end_with_their_identity_token_test() {
        let in_ten_minutes = (OffsetDateTime::now_utc() + time::Duration::minutes(10))
            .format(&Rfc3339)
            .unwrap();
        let left = remaining_lifetime(Some(&in_ten_minutes)).expect("live token");
        assert!(left <= Duration::from_secs(600) && left > Duration::from_secs(590));
        let an_hour_ago = (OffsetDateTime::now_utc() - time::Duration::hours(1))
            .format(&Rfc3339)
            .unwrap();
        assert!(remaining_lifetime(Some(&an_hour_ago)).is_err());
        assert!(remaining_lifetime(None).is_err());
    }

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-acl-auth-")
            .tempdir_in(crate::test_scratch::root())
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
            r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#,
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
        let (_, secret) = auth
            .mint_initial_management_token(&store, None)
            .map_err(|s| anyhow!(s.to_string()))?
            .expect("first mint creates the token");
        let identity = auth.authenticate(&metadata_with_token(&secret), &store)?;
        assert!(identity.is_management);
        assert!(identity.allows(AclResource::Acl, AclAction::Write, &AclScope::none()));

        // A second mint creates nothing while a management token exists.
        let again = auth
            .mint_initial_management_token(&store, None)
            .map_err(|s| anyhow!(s.to_string()))?;
        assert!(again.is_none());
        Ok(())
    }

    #[test]
    fn initial_token_accepts_a_preseeded_secret() -> R<()> {
        let store = setup_store()?;
        let auth = enabled_authenticator();

        // Too-short secrets are rejected before any state changes.
        let short = auth.mint_initial_management_token(&store, Some("short"));
        assert_eq!(short.unwrap_err().code(), tonic::Code::InvalidArgument);

        let provided = "lgn_preseeded_for_iac_0123456789";
        let (_, secret) = auth
            .mint_initial_management_token(&store, Some(provided))
            .map_err(|s| anyhow!(s.to_string()))?
            .expect("first mint creates the token");
        assert_eq!(secret, provided);

        let identity = auth.authenticate(&metadata_with_token(provided), &store)?;
        assert!(identity.is_management);
        Ok(())
    }

    #[tokio::test]
    async fn userpass_login_mints_scoped_token() -> R<()> {
        let store = setup_store()?;
        let auth = enabled_authenticator();
        store.upsert_acl_policy(
            "readers",
            "",
            r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#,
        )?;
        store.upsert_acl_user(
            "alice",
            Some(&hash_password("hunter2")?),
            &["readers".into()],
        )?;

        // Wrong password and unknown user produce the same error.
        let bad = auth
            .login(
                &store,
                USERPASS_METHOD,
                Some("alice"),
                Some("nope"),
                None,
                None,
            )
            .await;
        assert_eq!(bad.unwrap_err().code(), tonic::Code::Unauthenticated);
        let unknown = auth
            .login(&store, USERPASS_METHOD, Some("bob"), Some("x"), None, None)
            .await;
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
            .await
            .map_err(|s| anyhow!(s.to_string()))?;
        assert_eq!(info.name, "userpass:alice");
        assert!(info.expires_at.is_some(), "login tokens must expire");

        let identity = auth.authenticate(&metadata_with_token(&secret), &store)?;
        assert!(identity.allows(AclResource::Job, AclAction::Read, &AclScope::any()));
        assert!(!identity.allows(AclResource::Job, AclAction::Write, &AclScope::any()));
        Ok(())
    }

    #[tokio::test]
    async fn login_ttl_is_clamped_by_global_max() -> R<()> {
        let store = setup_store()?;
        store.upsert_acl_policy(
            "readers",
            "",
            r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#,
        )?;
        store.upsert_acl_user("alice", Some(&hash_password("pw")?), &["readers".into()])?;

        let auth = Authenticator::from_config(
            &AclConfig {
                enabled: Some(true),
                token_max_ttl: Some("1h".into()),
                ..Default::default()
            }
            .with_defaults(),
        )?;

        // Requesting 100h is clamped to the 1h global bound.
        let (info, _) = auth
            .login(
                &store,
                USERPASS_METHOD,
                Some("alice"),
                Some("pw"),
                None,
                Some("100h"),
            )
            .await
            .map_err(|s| anyhow!(s.to_string()))?;
        let expires =
            OffsetDateTime::parse(&info.expires_at.expect("login tokens expire"), &Rfc3339)?;
        let in_two_hours = OffsetDateTime::now_utc() + time::Duration::hours(2);
        assert!(expires < in_two_hours, "expiry must respect tokenMaxTtl");
        Ok(())
    }

    #[test]
    fn node_tokens_authenticate_as_worker_identities_and_rotate() -> R<()> {
        let store = setup_store()?;
        let auth = enabled_authenticator();

        let first = auth
            .mint_node_token(&store, "worker-a")
            .map_err(|s| anyhow!(s.to_string()))?;
        let identity = auth.authenticate(&metadata_with_token(&first), &store)?;
        assert!(identity.is_worker);
        assert!(!identity.is_management);
        assert_eq!(identity.principal, "node:worker-a");
        // Worker tokens pass the worker plane but grant no API access.
        assert!(enforce_worker_plane(&identity).is_ok());
        assert!(!identity.allows(AclResource::Job, AclAction::Read, &AclScope::any()));

        // Re-minting rotates: the old secret stops working.
        let second = auth
            .mint_node_token(&store, "worker-a")
            .map_err(|s| anyhow!(s.to_string()))?;
        assert!(auth
            .authenticate(&metadata_with_token(&first), &store)
            .is_err());
        assert!(auth
            .authenticate(&metadata_with_token(&second), &store)
            .is_ok());

        // Anonymous and plain client identities fail the worker plane.
        let anonymous = auth.authenticate(&MetadataMap::new(), &store)?;
        assert_eq!(
            enforce_worker_plane(&anonymous).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        Ok(())
    }

    #[test]
    fn node_binding_limits_worker_tokens_to_their_own_node() -> R<()> {
        let store = setup_store()?;
        let auth = enabled_authenticator();
        let secret = auth
            .mint_node_token(&store, "worker-a")
            .map_err(|s| anyhow!(s.to_string()))?;
        let worker = auth.authenticate(&metadata_with_token(&secret), &store)?;
        assert_eq!(worker_node_id(&worker), Some("worker-a"));
        assert!(enforce_node_binding(&worker, "worker-a").is_ok());
        assert_eq!(
            enforce_node_binding(&worker, "worker-b")
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        // Management (and ACLs-off) identities act for any node.
        assert!(enforce_node_binding(&AclIdentity::acl_disabled(), "worker-b").is_ok());

        // Audit-only mode logs the mismatch but lets it through.
        let audit_worker = AclIdentity {
            audit_only: true,
            ..worker
        };
        assert!(enforce_node_binding(&audit_worker, "worker-b").is_ok());
        Ok(())
    }

    #[test]
    fn stream_credentials_fail_once_the_token_is_revoked() -> R<()> {
        let store = setup_store()?;
        let auth = std::sync::Arc::new(enabled_authenticator());
        let (info, secret) = auth
            .mint_token(&store, "streamer", "client", &[], None)
            .map_err(|s| anyhow!(s.to_string()))?;
        let credential = StreamCredential::new(auth.clone(), &metadata_with_token(&secret));
        assert_eq!(credential.interval, STREAM_REAUTH_INTERVAL);
        assert!(credential.reauthenticate(&store).is_ok());

        store.delete_acl_token(&info.accessor_id)?;
        assert_eq!(
            credential.reauthenticate(&store).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        Ok(())
    }

    #[test]
    fn join_tokens_mint_and_verify() -> R<()> {
        let store = setup_store()?;
        let auth = enabled_authenticator();

        let (info, composite) = auth
            .mint_join_token(
                &store,
                "http://10.0.0.5:4747",
                "gpu",
                Some(Duration::from_secs(3600)),
                "rack 12",
            )
            .map_err(|s| anyhow!(s.to_string()))?;
        assert_eq!(info.worker_type, "gpu");
        assert!(info.expires_at.is_some());

        // The composite is decodable and carries the advertised address.
        let decoded = JoinToken::decode(&composite)?;
        assert_eq!(decoded.addr, "http://10.0.0.5:4747");
        assert_eq!(decoded.worker_type, "gpu");

        // The embedded secret verifies; garbage and missing secrets do not.
        let record = auth
            .verify_join_secret(&store, Some(&decoded.secret))
            .map_err(|s| anyhow!(s.to_string()))?;
        assert_eq!(record.id, info.id);
        assert_eq!(
            auth.verify_join_secret(&store, Some("ljt_bogus"))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            auth.verify_join_secret(&store, None).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );

        // Expired join tokens are rejected.
        let (_, expired_composite) = auth
            .mint_join_token(&store, "http://x:1", "default", Some(Duration::ZERO), "")
            .map_err(|s| anyhow!(s.to_string()))?;
        std::thread::sleep(Duration::from_millis(5));
        let expired = JoinToken::decode(&expired_composite)?;
        assert_eq!(
            auth.verify_join_secret(&store, Some(&expired.secret))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );

        // Revocation by id.
        store.delete_join_token(&info.id)?;
        assert!(auth
            .verify_join_secret(&store, Some(&decoded.secret))
            .is_err());
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
            r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#,
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

    #[tokio::test]
    async fn jwt_login_exchanges_for_acl_token() -> R<()> {
        let store = setup_store()?;
        store.upsert_acl_policy("ops", "", r#"[{"resources": ["node"], "actions": ["*"]}]"#)?;
        store.upsert_acl_policy(
            "base",
            "",
            r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#,
        )?;

        let (auth, token) = hs256_authenticator_and_jwt(json!({
            "sub": "ci-bot",
            "iss": "https://idp.test",
            "aud": "legion",
            "exp": future_exp(),
        }))?;

        let (info, secret) = auth
            .login(&store, "ci", None, None, Some(&token), None)
            .await
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

        fn login<'a>(
            &'a self,
            _store: &'a Store,
            credentials: LoginCredentials<'a>,
        ) -> futures::future::BoxFuture<'a, Result<VerifiedIdentity, AuthError>> {
            Box::pin(async move {
                if credentials.password == Some("letmein") {
                    Ok(VerifiedIdentity {
                        principal: "robot".into(),
                        policies: vec!["readers".into()],
                        expires_at: None,
                    })
                } else {
                    Err(AuthError::InvalidCredentials("bad static secret".into()))
                }
            })
        }

        fn login_token_ttl(&self) -> Duration {
            Duration::from_secs(60)
        }
    }

    #[tokio::test]
    async fn custom_auth_methods_can_be_registered() -> R<()> {
        let store = setup_store()?;
        store.upsert_acl_policy(
            "readers",
            "",
            r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#,
        )?;

        let mut auth = enabled_authenticator();
        auth.register_method(Box::new(StaticTestMethod))?;
        assert!(auth.method_names().contains(&"static-test"));
        // Unspecialized methods surface as `custom` in login UIs.
        assert!(auth
            .method_infos()
            .iter()
            .any(|info| info.name == "static-test" && info.kind == "custom"));

        // Duplicate names are rejected (userpass is pre-registered).
        assert!(auth.register_method(Box::new(StaticTestMethod)).is_err());
        assert!(auth
            .register_method(Box::new(UserpassMethod::new(Duration::from_secs(1))))
            .is_err());

        let bad = auth
            .login(&store, "static-test", None, Some("wrong"), None, None)
            .await;
        assert_eq!(bad.unwrap_err().code(), tonic::Code::Unauthenticated);

        let (info, secret) = auth
            .login(&store, "static-test", None, Some("letmein"), None, None)
            .await
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
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("job:write"));
        assert!(err.message().contains("legion login"));
        assert!(err.message().contains("bootstrap-token"));
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
