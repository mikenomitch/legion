//! Manager-as-OIDC-IdP: signing keys, token issuance, JWKS, and the OIDC
//! discovery document.
//!
//! See `internal_docs/plans/workload-identity-oidc-plan.md`. The spec/claims primitives
//! live in `crate::structs::workload_identity`; this module owns the crypto and
//! issuer side:
//!
//! - [`SigningKey`] — an ES256 keypair plus the public JWK material and a
//!   rotation lifecycle (`active` → `retiring` → `expired`).
//! - [`Issuer`] — holds the active + retiring keys and turns
//!   [`WorkloadIdentityClaims`] + a [`WorkloadIdentity`] spec into a signed JWT,
//!   serves the JWKS, and emits the discovery document.
//!
//! The private key is never serialized into a token or RPC reply: only the
//! public JWK leaves this module. Keys *are* persisted (private PEM included)
//! in the manager's SQLite `signing_key` table — the same trust domain as the
//! CA material — which is what keeps externally-held tokens verifiable across
//! manager restarts. [`Issuer::load_or_generate`] restores the key set on
//! boot; [`Issuer::rotate_persisted`] / [`Issuer::expire_keys_persisted`]
//! write lifecycle transitions through to the store and are driven by the
//! rotation loop in `crate::manager::daemon`.
//!
//! [`WorkloadIdentityState`] bundles the issuer with its config for the three
//! consumers that share it: the `IssueWorkloadIdentityToken` RPC
//! (`crate::manager::server`), the public discovery/JWKS HTTP routes
//! (`crate::manager::oidc_http`), and the rotation loop.

use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::workload_identity::{
    is_protected_claim_key, NodeIdentityClaims, NodeIdentityConfig, SigningKeyRecord,
    WorkloadIdentity, WorkloadIdentityClaims, CONTROL_PLANE_IDENTITY_NAME,
};
use crate::structs::{Allocation, TaskConfig, WorkloadIdentityConfigFull};

/// Standard OIDC discovery route.
pub const DISCOVERY_PATH: &str = "/.well-known/openid-configuration";
/// Default JWKS route (operator-overridable via config).
pub const DEFAULT_JWKS_PATH: &str = "/.well-known/jwks.json";

/// Lifecycle state of a signing key. A retiring key stays in the JWKS until the
/// last token it could have signed has expired — this overlap is what prevents
/// valid-but-unverifiable tokens during rotation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyState {
    Active,
    Retiring,
    Expired,
}

impl KeyState {
    pub fn as_str(self) -> &'static str {
        match self {
            KeyState::Active => "active",
            KeyState::Retiring => "retiring",
            KeyState::Expired => "expired",
        }
    }

    pub fn parse(raw: &str) -> R<Self> {
        match raw {
            "active" => Ok(KeyState::Active),
            "retiring" => Ok(KeyState::Retiring),
            "expired" => Ok(KeyState::Expired),
            other => bail!("unknown signing key state {other:?}"),
        }
    }
}

/// An ES256 signing key plus its public JWK material.
pub struct SigningKey {
    pub kid: String,
    pub alg: Algorithm,
    /// PKCS#8 PEM of the private key. Never leaves this struct.
    private_pem: String,
    /// base64url(P-256 affine X coordinate).
    x: String,
    /// base64url(P-256 affine Y coordinate).
    y: String,
    pub created_at: u64,
    /// Last instant a token signed by this key can still be valid. `None` while
    /// the key is active (it keeps signing); set when the key starts retiring.
    pub not_after: Option<u64>,
    pub state: KeyState,
}

impl SigningKey {
    /// Generate a fresh ES256 (P-256) signing key, active as of `created_at`.
    pub fn generate(created_at: u64) -> R<Self> {
        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)
            .context("failed to generate ES256 signing key")?;
        let private_pem = key_pair.serialize_pem();

        // For P-256, ring (via rcgen) exposes the public key as the uncompressed
        // SEC1 point: 0x04 || X(32) || Y(32).
        let raw = key_pair.public_key_raw();
        if raw.len() != 65 || raw[0] != 0x04 {
            bail!(
                "unexpected P-256 public key encoding (len {}, tag {:#x})",
                raw.len(),
                raw.first().copied().unwrap_or(0)
            );
        }
        let x = base64url(&raw[1..33]);
        let y = base64url(&raw[33..65]);
        // Stable, collision-resistant key id derived from the public point.
        let kid = base64url(&Sha256::digest(raw))[..16].to_string();

        Ok(Self {
            kid,
            alg: Algorithm::ES256,
            private_pem,
            x,
            y,
            created_at,
            not_after: None,
            state: KeyState::Active,
        })
    }

    /// The persisted form of this key (private PEM included; the store is the
    /// manager's trust domain — see the `signing_key` migration comment).
    pub fn to_record(&self) -> SigningKeyRecord {
        SigningKeyRecord {
            kid: self.kid.clone(),
            alg: "ES256".into(),
            private_key_pem: self.private_pem.clone(),
            public_jwk_x: self.x.clone(),
            public_jwk_y: self.y.clone(),
            created_at: self.created_at,
            not_after: self.not_after,
            state: self.state.as_str().into(),
        }
    }

    /// Restore a key from its persisted form.
    pub fn from_record(record: &SigningKeyRecord) -> R<Self> {
        if record.alg != "ES256" {
            bail!(
                "signing key {} has unsupported algorithm {:?} (only ES256 is implemented)",
                record.kid,
                record.alg
            );
        }
        Ok(Self {
            kid: record.kid.clone(),
            alg: Algorithm::ES256,
            private_pem: record.private_key_pem.clone(),
            x: record.public_jwk_x.clone(),
            y: record.public_jwk_y.clone(),
            created_at: record.created_at,
            not_after: record.not_after,
            state: KeyState::parse(&record.state)?,
        })
    }

    fn encoding_key(&self) -> R<EncodingKey> {
        EncodingKey::from_ec_pem(self.private_pem.as_bytes())
            .context("failed to load signing key as ES256 encoding key")
    }

    /// The public JWK for this key (no private material).
    pub fn public_jwk(&self) -> Value {
        json!({
            "kty": "EC",
            "crv": "P-256",
            "alg": "ES256",
            "use": "sig",
            "kid": self.kid,
            "x": self.x,
            "y": self.y,
        })
    }

    /// Decoding key for verifying tokens signed by this key (used by Legion's
    /// own control-plane verification path and by tests; external providers use
    /// the published JWKS).
    pub fn decoding_key(&self) -> R<DecodingKey> {
        DecodingKey::from_ec_components(&self.x, &self.y)
            .context("failed to build decoding key from JWK components")
    }

    fn sign(&self, claims: &Map<String, Value>) -> R<String> {
        let header = Header {
            kid: Some(self.kid.clone()),
            alg: self.alg,
            ..Default::default()
        };
        encode(&header, claims, &self.encoding_key()?).context("failed to sign identity token")
    }
}

/// A signed token plus its expiry, returned to the worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedToken {
    pub token: String,
    pub expires_at: u64,
    pub kid: String,
}

/// The manager's OIDC issuer: owns the signing keys and mints tokens.
pub struct Issuer {
    issuer_url: String,
    jwks_path: String,
    default_ttl: Duration,
    /// The largest lifetime any single token may have, equal to the rotation
    /// overlap window `max(rotationAge, defaultTtl)`. A token's `exp` is clamped
    /// to at most `now + max_token_ttl` (P1): a long-TTL identity signed just
    /// before rotation would otherwise outlive its signing key's `not_after`
    /// (the key's overlap window is exactly this long) and become
    /// valid-but-unverifiable once the key expires. Clamping `exp` at issuance
    /// guarantees every token a key signs expires inside that key's overlap.
    max_token_ttl: Duration,
    /// Cluster-default `sub` template; `None` means use the built-in default.
    sub_template: Option<String>,
    /// Active key first, then any retiring keys still inside their overlap window.
    keys: Vec<SigningKey>,
}

impl Issuer {
    /// Create an issuer with a freshly generated active key (in-memory only;
    /// production paths use [`Issuer::load_or_generate`] so keys survive
    /// manager restarts).
    pub fn new(
        issuer_url: impl Into<String>,
        jwks_path: impl Into<String>,
        default_ttl: Duration,
        sub_template: Option<String>,
        now: SystemTime,
    ) -> R<Self> {
        Self::new_with_max_ttl(
            issuer_url,
            jwks_path,
            default_ttl,
            default_ttl,
            sub_template,
            now,
        )
    }

    /// Like [`Issuer::new`], but with an explicit `max_token_ttl` (the rotation
    /// overlap window). Production paths derive `max_token_ttl` from the
    /// manager config (`max(rotationAge, defaultTtl)`); the simple [`Issuer::new`]
    /// uses `default_ttl` for both, which is correct for tests that never set a
    /// per-identity TTL above the default.
    pub fn new_with_max_ttl(
        issuer_url: impl Into<String>,
        jwks_path: impl Into<String>,
        default_ttl: Duration,
        max_token_ttl: Duration,
        sub_template: Option<String>,
        now: SystemTime,
    ) -> R<Self> {
        let key = SigningKey::generate(unix_secs(now)?)?;
        Ok(Self {
            issuer_url: issuer_url.into(),
            jwks_path: jwks_path.into(),
            default_ttl,
            max_token_ttl,
            sub_template,
            keys: vec![key],
        })
    }

    /// Create an issuer backed by the store: restore all non-expired signing
    /// keys, or generate **and persist** the first active key on first boot.
    /// This is what keeps tokens held by third parties verifiable across
    /// manager restarts — the JWKS keeps publishing the same public keys and
    /// the same private key keeps signing.
    pub fn load_or_generate(
        store: &Store,
        issuer_url: impl Into<String>,
        jwks_path: impl Into<String>,
        default_ttl: Duration,
        sub_template: Option<String>,
        now: SystemTime,
    ) -> R<Self> {
        Self::load_or_generate_with_max_ttl(
            store,
            issuer_url,
            jwks_path,
            default_ttl,
            default_ttl,
            sub_template,
            now,
        )
    }

    /// Like [`Issuer::load_or_generate`], but with an explicit `max_token_ttl`
    /// (the rotation overlap window) used to clamp every token's `exp` (P1).
    #[allow(clippy::too_many_arguments)]
    pub fn load_or_generate_with_max_ttl(
        store: &Store,
        issuer_url: impl Into<String>,
        jwks_path: impl Into<String>,
        default_ttl: Duration,
        max_token_ttl: Duration,
        sub_template: Option<String>,
        now: SystemTime,
    ) -> R<Self> {
        let mut keys = Vec::new();
        for record in store.list_signing_keys()? {
            if record.state == KeyState::Expired.as_str() {
                continue;
            }
            keys.push(SigningKey::from_record(&record)?);
        }

        if !keys.iter().any(|k| k.state == KeyState::Active) {
            let key = SigningKey::generate(unix_secs(now)?)?;
            store.insert_signing_key(&key.to_record())?;
            info!(kid = %key.kid, "Generated and persisted initial workload-identity signing key");
            keys.insert(0, key);
        } else {
            debug!(
                keys = keys.len(),
                "Restored workload-identity signing keys from store"
            );
        }

        Ok(Self {
            issuer_url: issuer_url.into(),
            jwks_path: jwks_path.into(),
            default_ttl,
            max_token_ttl,
            sub_template,
            keys,
        })
    }

    pub fn issuer_url(&self) -> &str {
        &self.issuer_url
    }

    fn active_key(&self) -> R<&SigningKey> {
        self.keys
            .iter()
            .find(|k| k.state == KeyState::Active)
            .context("issuer has no active signing key")
    }

    /// Find a key by id among the non-expired keys (for verification).
    pub fn key(&self, kid: &str) -> Option<&SigningKey> {
        self.keys
            .iter()
            .find(|k| k.kid == kid && k.state != KeyState::Expired)
    }

    /// The JWKS document: every non-expired public key.
    pub fn jwks(&self) -> Value {
        let keys: Vec<Value> = self
            .keys
            .iter()
            .filter(|k| k.state != KeyState::Expired)
            .map(SigningKey::public_jwk)
            .collect();
        json!({ "keys": keys })
    }

    /// The OIDC discovery document served at `/.well-known/openid-configuration`.
    pub fn discovery_document(&self) -> Value {
        json!({
            "issuer": self.issuer_url,
            "jwks_uri": format!("{}{}", self.issuer_url, self.jwks_path),
            "response_types_supported": ["id_token"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["ES256"],
            "claims_supported": [
                "iss", "sub", "aud", "exp", "iat", "nbf", "jti",
                "legion.io/region", "legion.io/job", "legion.io/group",
                "legion.io/task", "legion.io/node", "legion.io/alloc",
            ],
        })
    }

    /// The audience the token is actually signed with. For the reserved
    /// control-plane identity the manager owns the audience (the cluster
    /// issuer); otherwise the spec-declared, validation-enforced audiences win.
    fn effective_audience(&self, identity: &WorkloadIdentity) -> Vec<String> {
        if identity.name == CONTROL_PLANE_IDENTITY_NAME && identity.audience.is_empty() {
            vec![self.issuer_url.clone()]
        } else {
            identity.audience.clone()
        }
    }

    /// Issue a signed token for `claims` under `identity`. The caller (manager)
    /// derives `claims` from the scheduling record; this function never reads
    /// identity facts from worker-supplied input.
    pub fn issue(
        &self,
        claims: &WorkloadIdentityClaims,
        identity: &WorkloadIdentity,
        now: SystemTime,
    ) -> R<IssuedToken> {
        let key = self.active_key()?;
        let iat = unix_secs(now)?;
        let ttl = identity.parsed_ttl()?.unwrap_or(self.default_ttl);

        // P1: clamp `exp` so no token outlives the overlap window its signing
        // key gets when it retires. A per-identity `ttl` is unbounded at
        // admission (rotationAge isn't reachable at spec-validation time), so a
        // long-TTL token signed just before rotation could otherwise expire
        // after its key's `not_after` and become valid-but-unverifiable. The
        // bound is `now + max_token_ttl` where `max_token_ttl == overlap ==
        // max(rotationAge, defaultTtl)`; we additionally clamp to the active
        // key's `not_after` if one is ever set (a key should not sign once
        // retiring, but be defensive). See docs/workload-identity.md.
        let ttl = ttl.min(self.max_token_ttl);
        let mut exp = iat + ttl.as_secs();
        if let Some(not_after) = key.not_after {
            exp = exp.min(not_after);
        }

        let template = identity
            .sub_template
            .as_deref()
            .or(self.sub_template.as_deref());
        let subject = claims.subject_for(template)?;

        let audience = self.effective_audience(identity);
        if audience.is_empty() {
            bail!(
                "identity {:?} has no audience; refusing to mint a replayable token",
                identity.name
            );
        }

        let mut body = Map::new();
        for (claim, value) in claims.legion_claims() {
            body.insert(claim, json!(value));
        }
        // Static, non-reserved extra claims. Admission already rejects reserved
        // and `legion.io/*` keys, but the signing sink re-checks (belt and
        // suspenders): the signer must never let an extra claim overwrite a
        // derived/trust-critical claim (iss/sub/aud/exp/iat/nbf/jti or
        // legion.io/*), even if admission validation were somehow bypassed.
        for (claim, value) in &identity.extra_claims {
            if is_protected_claim_key(claim) {
                warn!(
                    claim = %claim,
                    identity = %identity.name,
                    "Refusing to let extra_claims override a protected claim"
                );
                continue;
            }
            body.insert(claim.clone(), json!(value));
        }

        self.sign_body(key, &subject, &audience, body, iat, exp, now)
    }

    /// Issue a signed token for a *node* identity. Mirrors [`Issuer::issue`]
    /// but derives the subject/claims from [`NodeIdentityClaims`] and the
    /// node-identity config (audience/ttl/sub_template). The manager derives
    /// the claims from the node's registration record — never worker input.
    pub fn issue_node(
        &self,
        claims: &NodeIdentityClaims,
        identity: &NodeIdentityConfig,
        now: SystemTime,
    ) -> R<IssuedToken> {
        let key = self.active_key()?;
        let iat = unix_secs(now)?;
        let ttl = identity.parsed_ttl()?.unwrap_or(self.default_ttl);
        let ttl = ttl.min(self.max_token_ttl);
        let mut exp = iat + ttl.as_secs();
        if let Some(not_after) = key.not_after {
            exp = exp.min(not_after);
        }

        let subject = claims.subject_for(identity.sub_template.as_deref())?;
        if identity.audience.is_empty() {
            bail!(
                "node identity {:?} has no audience; refusing to mint a replayable token",
                identity.name
            );
        }

        let mut body = Map::new();
        for (claim, value) in claims.legion_claims() {
            body.insert(claim, json!(value));
        }
        self.sign_body(key, &subject, &identity.audience, body, iat, exp, now)
    }

    /// Stamp the registered JWT claims onto `body` (which already carries the
    /// derived `legion.io/*` claims) and sign it. Shared by task and node
    /// issuance so the two can never drift on the trust-critical fields.
    #[allow(clippy::too_many_arguments)]
    fn sign_body(
        &self,
        key: &SigningKey,
        subject: &str,
        audience: &[String],
        mut body: Map<String, Value>,
        iat: u64,
        exp: u64,
        now: SystemTime,
    ) -> R<IssuedToken> {
        body.insert("iss".into(), json!(self.issuer_url));
        body.insert("sub".into(), json!(subject));
        body.insert("aud".into(), json!(audience));
        body.insert("iat".into(), json!(iat));
        body.insert("nbf".into(), json!(iat));
        body.insert("exp".into(), json!(exp));
        body.insert("jti".into(), json!(jti(subject, iat, now)));

        let token = key.sign(&body)?;
        Ok(IssuedToken {
            token,
            expires_at: exp,
            kid: key.kid.clone(),
        })
    }

    /// Mint a **boot ticket**: the narrow, short-lived capability an edge node
    /// verifies offline before starting work on its own (docs/edge-boot.md).
    ///
    /// Deliberately a separate entry point from [`Issuer::issue`] rather than
    /// an identity with unusual claims. A boot ticket authorizes a *start*,
    /// not a workload — it carries the reserved `legion:node` audience, its
    /// own `legion.io/scope: boot` marker, and a caller-supplied `jti` the
    /// manager records for single-use enforcement. Routing it through the
    /// workload-identity path would make one code path responsible for two
    /// different trust decisions, and would let a task audience and a node
    /// audience be confused for one another.
    ///
    /// The `exp` is clamped to the signing key's overlap window for the same
    /// reason every other token is: a ticket must never outlive the key that
    /// can verify it.
    pub fn issue_boot_ticket(
        &self,
        jti: &str,
        subject: &str,
        claims: Map<String, Value>,
        iat: u64,
        exp: u64,
    ) -> R<IssuedToken> {
        self.issue_ticket(
            crate::structs::edge_boot::BOOT_TICKET_AUDIENCE,
            jti,
            subject,
            claims,
            iat,
            exp,
        )
    }

    /// A node-redeemed ticket for `audience` (`legion:node` boot tickets,
    /// `legion:exec` exec tickets), signed with the active key and verified
    /// by workers against the JWKS. Same clamping as every token: it never
    /// outlives the key that can verify it.
    pub fn issue_ticket(
        &self,
        audience: &str,
        jti: &str,
        subject: &str,
        claims: Map<String, Value>,
        iat: u64,
        exp: u64,
    ) -> R<IssuedToken> {
        let key = self.active_key()?;
        let mut exp = exp.min(iat + self.max_token_ttl.as_secs());
        if let Some(not_after) = key.not_after {
            exp = exp.min(not_after);
        }
        if exp <= iat {
            bail!("ticket TTL collapsed to zero against the signing key's overlap window");
        }

        let mut body = claims;
        body.insert("iss".into(), json!(self.issuer_url));
        body.insert("sub".into(), json!(subject));
        body.insert("aud".into(), json!([audience]));
        body.insert("iat".into(), json!(iat));
        body.insert("nbf".into(), json!(iat));
        body.insert("exp".into(), json!(exp));
        body.insert("jti".into(), json!(jti));

        let token = key.sign(&body)?;
        Ok(IssuedToken {
            token,
            expires_at: exp,
            kid: key.kid.clone(),
        })
    }

    /// Verify a token against this issuer's keys (control-plane verification
    /// path; external relying parties use the published JWKS instead). Returns
    /// the decoded claims on success.
    pub fn verify(&self, token: &str, expected_audience: &str) -> R<Map<String, Value>> {
        let header = jsonwebtoken::decode_header(token).context("invalid token header")?;
        let kid = header.kid.context("token is missing a kid header")?;
        let key = self
            .key(&kid)
            .with_context(|| format!("no (non-expired) signing key for kid {kid}"))?;

        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_issuer(&[&self.issuer_url]);
        validation.set_audience(&[expected_audience]);

        let data = decode::<Map<String, Value>>(token, &key.decoding_key()?, &validation)
            .context("token failed verification")?;
        Ok(data.claims)
    }

    /// Unix-seconds creation time of the active key (drives the rotation loop).
    pub fn active_key_created_at(&self) -> R<u64> {
        Ok(self.active_key()?.created_at)
    }

    /// Rotate the signing key: the current active key becomes `retiring` with a
    /// `not_after` covering the last token it could have signed, and a new
    /// active key is generated. `max_token_ttl` should be the largest TTL any
    /// identity can request so no signed token outlives the overlap window.
    /// In-memory only; production paths use [`Issuer::rotate_persisted`].
    pub fn rotate(&mut self, now: SystemTime, max_token_ttl: Duration) -> R<()> {
        self.rotate_keys(now, max_token_ttl)?;
        Ok(())
    }

    /// Like [`Issuer::rotate`], but writes the lifecycle transitions through to
    /// the store: retiring keys are updated in place and the new active key is
    /// inserted, so a restart mid-overlap restores the exact same key set.
    pub fn rotate_persisted(
        &mut self,
        store: &Store,
        now: SystemTime,
        max_token_ttl: Duration,
    ) -> R<()> {
        let (retired, new_record) = self.rotate_keys(now, max_token_ttl)?;
        for (kid, not_after) in &retired {
            store.update_signing_key_state(kid, KeyState::Retiring.as_str(), Some(*not_after))?;
        }
        store.insert_signing_key(&new_record)?;
        info!(
            new_kid = %new_record.kid,
            retired = retired.len(),
            "Rotated workload-identity signing key"
        );
        Ok(())
    }

    /// Shared rotation body: returns the (kid, not_after) pairs that started
    /// retiring plus the persisted form of the new active key.
    fn rotate_keys(
        &mut self,
        now: SystemTime,
        max_token_ttl: Duration,
    ) -> R<(Vec<(String, u64)>, SigningKeyRecord)> {
        let now_secs = unix_secs(now)?;
        let mut retired = Vec::new();
        for key in &mut self.keys {
            if key.state == KeyState::Active {
                key.state = KeyState::Retiring;
                let not_after = now_secs + max_token_ttl.as_secs();
                key.not_after = Some(not_after);
                retired.push((key.kid.clone(), not_after));
            }
        }
        let new_key = SigningKey::generate(now_secs)?;
        let record = new_key.to_record();
        self.keys.insert(0, new_key);
        Ok((retired, record))
    }

    /// Drop retiring keys whose overlap window has closed. Returns the kids of
    /// the keys that were expired. In-memory only; production paths use
    /// [`Issuer::expire_keys_persisted`].
    pub fn expire_keys(&mut self, now: SystemTime) -> R<Vec<String>> {
        let now_secs = unix_secs(now)?;
        let mut expired = Vec::new();
        self.keys.retain(|k| match (k.state, k.not_after) {
            (KeyState::Retiring, Some(not_after)) if now_secs > not_after => {
                expired.push(k.kid.clone());
                false
            }
            _ => true,
        });
        Ok(expired)
    }

    /// Like [`Issuer::expire_keys`], but marks the expired rows in the store
    /// (rows are retained for audit; they are simply never loaded or
    /// published again). Returns how many keys were expired.
    pub fn expire_keys_persisted(&mut self, store: &Store, now: SystemTime) -> R<usize> {
        let expired = self.expire_keys(now)?;
        for kid in &expired {
            store.update_signing_key_state(kid, KeyState::Expired.as_str(), None)?;
            info!(%kid, "Expired workload-identity signing key past its overlap window");
        }
        Ok(expired.len())
    }

    /// Replace the in-memory key set from the (replicated) store. This is how a
    /// **standby** manager keeps its JWKS and signer in step with the primary:
    /// keys are read once at boot, so without this a follower would serve a
    /// boot-frozen JWKS and miss keys the primary rotated in. Never generates a
    /// key — minting the first/rotated key is the primary's job — so a follower
    /// simply adopts whatever the primary persisted. Returns true if the set
    /// changed.
    pub fn reload_keys(&mut self, store: &Store) -> R<bool> {
        let mut keys = Vec::new();
        for record in store.list_signing_keys()? {
            if record.state == KeyState::Expired.as_str() {
                continue;
            }
            keys.push(SigningKey::from_record(&record)?);
        }
        if keys.is_empty() {
            // Nothing (non-expired) persisted yet; keep the current set rather
            // than blanking a live JWKS.
            return Ok(false);
        }
        let before: std::collections::BTreeSet<&str> =
            self.keys.iter().map(|k| k.kid.as_str()).collect();
        let after: std::collections::BTreeSet<&str> = keys.iter().map(|k| k.kid.as_str()).collect();
        let changed = before != after;
        self.keys = keys;
        Ok(changed)
    }
}

/// Shared workload-identity state for the manager process: the effective
/// config plus the store-backed [`Issuer`] behind a lock. Shared (via `Arc`)
/// by the `IssueWorkloadIdentityToken` RPC handler, the public discovery/JWKS
/// HTTP routes, and the background rotation loop. Only constructed when
/// `workloadIdentity.enabled` is true — when disabled the manager carries no
/// issuer at all and the HTTP routes are not mounted.
pub struct WorkloadIdentityState {
    config: WorkloadIdentityConfigFull,
    issuer: RwLock<Issuer>,
}

impl std::fmt::Debug for WorkloadIdentityState {
    // Manual impl: never expose issuer/key internals through Debug.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkloadIdentityState")
            .field("enabled", &self.config.enabled)
            .field("jwks_path", &self.config.jwks_path)
            .finish_non_exhaustive()
    }
}

impl WorkloadIdentityState {
    /// Build the state from the effective manager config, restoring (or
    /// first-boot generating + persisting) signing keys from the store.
    ///
    /// `default_issuer_url` is used when `workloadIdentity.issuer` is unset —
    /// the manager's own listener address. For real third-party federation the
    /// operator must configure the externally-reachable HTTPS issuer URL,
    /// since it is baked into `iss` and the discovery document.
    pub fn initialize(
        store: &Store,
        config: &WorkloadIdentityConfigFull,
        default_issuer_url: &str,
        now: SystemTime,
    ) -> R<Self> {
        // Trim a trailing slash so `jwks_uri = issuer + jwks_path` (which
        // requires a leading slash on jwks_path, checked below) never produces
        // a double slash or a malformed URL.
        let issuer_url = config
            .issuer
            .clone()
            .unwrap_or_else(|| default_issuer_url.to_string())
            .trim_end_matches('/')
            .to_string();

        // Only ES256 is implemented; fail fast rather than silently signing
        // ES256 when an operator asked for something else (e.g. RS256).
        if config.signing_alg != "ES256" {
            bail!(
                "workloadIdentity.signingAlg {:?} is not supported (only ES256 is implemented)",
                config.signing_alg
            );
        }

        // The JWKS route matches the request path by exact string equality and
        // request paths always start with '/', so a jwks_path without a leading
        // slash yields an unreachable route and a malformed jwks_uri. Reject it
        // at startup instead of serving an unreachable JWKS.
        if !config.jwks_path.starts_with('/') {
            bail!(
                "workloadIdentity.jwksPath {:?} must start with '/'",
                config.jwks_path
            );
        }

        // Fail fast on a malformed TTL/rotation duration rather than silently
        // falling back to a default (these two govern token lifetime and the
        // rotation overlap window — a typo must not pass quietly).
        config
            .default_ttl
            .parse::<humantime::Duration>()
            .with_context(|| {
                format!(
                    "invalid workloadIdentity.defaultTtl {:?} (use e.g. \"15m\")",
                    config.default_ttl
                )
            })?;
        config
            .rotation_age
            .parse::<humantime::Duration>()
            .with_context(|| {
                format!(
                    "invalid workloadIdentity.rotationAge {:?} (use e.g. \"24h\")",
                    config.rotation_age
                )
            })?;

        // P2: validate the cluster-default `sub` template the same way a
        // per-identity override is validated. Without this a cluster default
        // like `"{job}"` (below the uniqueness floor, or using a churning
        // `{alloc}`/`{node}` placeholder) would silently collapse distinct
        // workloads into one shared identity, since `render_subject` only
        // checks field availability, not the floor. Fail manager startup with
        // a clear error instead. `stateful = false` here: the cluster default
        // must render for every task, including stateless ones, so the
        // stateful-only `{instance}`/`{slot}` placeholders are disallowed in a
        // cluster default.
        crate::structs::workload_identity::validate_sub_template(&config.sub_template, false)
            .with_context(|| {
                format!(
                    "cluster-default workloadIdentity.subTemplate {:?} is invalid",
                    config.sub_template
                )
            })?;

        // Token `exp` is clamped to the rotation overlap window (P1).
        let overlap = config
            .parsed_rotation_age()
            .max(config.parsed_default_ttl());
        let issuer = Issuer::load_or_generate_with_max_ttl(
            store,
            issuer_url,
            config.jwks_path.clone(),
            config.parsed_default_ttl(),
            overlap,
            Some(config.sub_template.clone()),
            now,
        )?;
        Ok(Self {
            config: config.clone(),
            issuer: RwLock::new(issuer),
        })
    }

    pub fn config(&self) -> &WorkloadIdentityConfigFull {
        &self.config
    }

    /// The route the JWKS is served at (path component only).
    pub fn jwks_path(&self) -> &str {
        &self.config.jwks_path
    }

    fn read_issuer(&self) -> R<std::sync::RwLockReadGuard<'_, Issuer>> {
        self.issuer
            .read()
            .map_err(|_| anyhow!("workload identity issuer lock poisoned"))
    }

    fn write_issuer(&self) -> R<std::sync::RwLockWriteGuard<'_, Issuer>> {
        self.issuer
            .write()
            .map_err(|_| anyhow!("workload identity issuer lock poisoned"))
    }

    pub fn issuer_url(&self) -> R<String> {
        Ok(self.read_issuer()?.issuer_url().to_string())
    }

    pub fn discovery_document(&self) -> R<Value> {
        Ok(self.read_issuer()?.discovery_document())
    }

    pub fn jwks(&self) -> R<Value> {
        Ok(self.read_issuer()?.jwks())
    }

    /// How long a token minted now for `identity` lives: its own `ttl` or
    /// the cluster default, clamped to the rotation overlap the way
    /// [`Issuer::issue`] clamps `exp`. Used to decide when a token the node
    /// holds for upstream injection is due for a refresh (half of this).
    pub fn token_lifetime(&self, identity: &WorkloadIdentity) -> std::time::Duration {
        let default_ttl = self.config.parsed_default_ttl();
        let overlap = self.config.parsed_rotation_age().max(default_ttl);
        identity
            .parsed_ttl()
            .ok()
            .flatten()
            .unwrap_or(default_ttl)
            .min(overlap)
    }

    /// Sign a token for `claims` under `identity` with the active key.
    pub fn issue(
        &self,
        claims: &WorkloadIdentityClaims,
        identity: &WorkloadIdentity,
        now: SystemTime,
    ) -> R<IssuedToken> {
        self.read_issuer()?.issue(claims, identity, now)
    }

    /// Sign a node-identity token with the active key.
    pub fn issue_node(
        &self,
        claims: &NodeIdentityClaims,
        identity: &NodeIdentityConfig,
        now: SystemTime,
    ) -> R<IssuedToken> {
        self.read_issuer()?.issue_node(claims, identity, now)
    }

    /// Look up a configured node identity by name.
    pub fn node_identity(&self, name: &str) -> Option<NodeIdentityConfig> {
        self.config
            .node_identities
            .iter()
            .find(|identity| identity.name == name)
            .cloned()
    }

    /// Verify a control-plane token: one this cluster issued with `aud` equal
    /// to its own issuer URL (the reserved `legion` identity). Returns the
    /// decoded claims. This is the primitive Phase 7 delivers — mapping the
    /// verified identity to permissions is the separate ACL effort.
    pub fn verify_control_plane(&self, token: &str) -> R<Map<String, Value>> {
        let issuer = self.read_issuer()?;
        let audience = issuer.issuer_url().to_string();
        issuer.verify(token, &audience)
    }

    /// Verify a token against the issuer's current key set (control-plane
    /// verification path; external relying parties use the published JWKS).
    pub fn verify(&self, token: &str, expected_audience: &str) -> R<Map<String, Value>> {
        self.read_issuer()?.verify(token, expected_audience)
    }

    /// Mint a boot ticket (docs/edge-boot.md). See
    /// [`Issuer::issue_boot_ticket`] for why this is a separate entry point
    /// from the workload-identity mint.
    pub fn issue_boot_ticket(
        &self,
        jti: &str,
        subject: &str,
        claims: Map<String, Value>,
        iat: u64,
        exp: u64,
    ) -> R<IssuedToken> {
        self.read_issuer()?
            .issue_boot_ticket(jti, subject, claims, iat, exp)
    }

    /// Mint an exec ticket (docs/exec-and-actions.md, "Direct exec").
    pub fn issue_exec_ticket(
        &self,
        jti: &str,
        subject: &str,
        claims: Map<String, Value>,
        iat: u64,
        exp: u64,
    ) -> R<IssuedToken> {
        self.read_issuer()?.issue_ticket(
            crate::structs::exec_ticket::EXEC_TICKET_AUDIENCE,
            jti,
            subject,
            claims,
            iat,
            exp,
        )
    }

    /// One pass of the rotation state machine, persisting every transition:
    /// rotate when the active key is older than `rotationAge`, then expire
    /// retiring keys past their overlap window. Returns true when a rotation
    /// happened. Driven by the background loop in `crate::manager::daemon`.
    pub fn rotation_tick(&self, store: &Store, now: SystemTime) -> R<bool> {
        let rotation_age = self.config.parsed_rotation_age();
        // Overlap window: a retiring key stays published for one further
        // rotation period (and at least the default TTL), so any token it
        // signed verifies until expiry as long as identity TTLs stay at or
        // below rotationAge (documented in docs/workload-identity.md).
        let overlap = rotation_age.max(self.config.parsed_default_ttl());

        let mut issuer = self.write_issuer()?;
        let now_secs = now
            .duration_since(UNIX_EPOCH)
            .context("system clock before Unix epoch")?
            .as_secs();
        let age = now_secs.saturating_sub(issuer.active_key_created_at()?);
        let rotated = if age > rotation_age.as_secs() {
            issuer.rotate_persisted(store, now, overlap)?;
            true
        } else {
            false
        };
        issuer.expire_keys_persisted(store, now)?;
        Ok(rotated)
    }

    /// Refresh the in-memory key set from the replicated store. Called on each
    /// rotation-check tick by **non-primary** managers so a follower's JWKS and
    /// signer pick up keys the primary rotated in (staleness bounded by the
    /// check interval). Returns true if the key set changed.
    pub fn reload_from_store(&self, store: &Store) -> R<bool> {
        self.write_issuer()?.reload_keys(store)
    }
}

/// Derive workload-identity claims for an allocation from the scheduling
/// record. This is the manager-side source of truth the issuance RPC
/// stamps into tokens — claims are never trusted from a worker.
///
/// For stateful allocations the claims carry the stable `instance` key (and
/// `slot` in slot mode) read from the allocation's instance record, which is
/// what makes the token's identity stable across reschedules: every allocation
/// for slot N of a group resolves to the same `legion.io/instance` and the
/// same instance-keyed `sub` template rendering. Stateless allocations yield
/// `instance: None` / `slot: None`, exactly today's behavior.
pub fn claims_for_allocation(
    store: &Store,
    allocation: &Allocation,
    task: &TaskConfig,
    region: &str,
) -> R<WorkloadIdentityClaims> {
    let instance = match allocation.instance_id.as_deref() {
        Some(instance_id) => store.get_instance(instance_id)?,
        None => None,
    };

    Ok(WorkloadIdentityClaims {
        region: region.to_string(),
        job: allocation.job_id.clone(),
        group: task.group.clone(),
        task: task.name.clone(),
        node_id: allocation.node_id.clone(),
        alloc_id: allocation.id.to_string(),
        instance: instance.as_ref().map(|i| i.instance_id.clone()),
        slot: instance.as_ref().and_then(|i| i.slot_index),
        epoch: instance.as_ref().map(|i| i.identity_epoch),
    })
}

/// Refuse a verified token whose instance has moved past the epoch it was
/// issued at (docs/workload-identity.md, "Forks and snapshots").
///
/// A fork or snapshot capture copies the instance's tokens into the
/// checkpoint, and every fork (or `from_snapshot` instance) starts with them
/// in memory. The capture raises the instance's `identity_epoch`, so here,
/// in Legion's own verifiers, those copies stop working while the instance
/// itself gets a fresh token. Signature, issuer, audience, and expiry are
/// the caller's to check first. A token naming no instance (a stateless
/// task) has nothing to compare. A token naming an instance that no longer
/// exists is refused, since nothing should still hold it. A token with no
/// epoch claim was issued before epochs existed and counts as epoch 0.
pub fn check_identity_epoch(store: &Store, claims: &Map<String, Value>) -> R<()> {
    use crate::structs::workload_identity::{EPOCH_CLAIM, INSTANCE_CLAIM};
    let Some(instance_id) = claims.get(INSTANCE_CLAIM).and_then(Value::as_str) else {
        return Ok(());
    };
    let token_epoch = claims
        .get(EPOCH_CLAIM)
        .and_then(Value::as_str)
        .and_then(|epoch| epoch.parse::<u64>().ok())
        .unwrap_or(0);
    let Some(instance) = store.get_instance(instance_id)? else {
        bail!("token was issued to instance {instance_id}, which no longer exists");
    };
    if token_epoch < instance.identity_epoch {
        bail!(
            "token was issued to instance {instance_id} at identity epoch {token_epoch}, before \
             a fork or snapshot of it was captured (now epoch {}), so it may be a copy a fork \
             holds. The instance itself holds a newer token",
            instance.identity_epoch
        );
    }
    Ok(())
}

/// Seconds since the Unix epoch.
fn unix_secs(now: SystemTime) -> R<u64> {
    Ok(now
        .duration_since(UNIX_EPOCH)
        .context("system clock before Unix epoch")?
        .as_secs())
}

/// A unique-enough token id for audit/revocation tracing. Derived from the
/// subject, issue time, and sub-second clock so concurrently-issued tokens for
/// the same subject still differ.
fn jti(subject: &str, iat: u64, now: SystemTime) -> String {
    let nanos = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let seed = format!("{subject}:{iat}:{nanos}");
    base64url(&Sha256::digest(seed.as_bytes()))[..24].to_string()
}

/// base64url encoding without padding (RFC 7515 / JWK).
fn base64url(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((n >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(n & 0x3f) as usize] as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::workload_identity::WorkloadIdentityFile;

    fn now() -> SystemTime {
        SystemTime::now()
    }

    fn issuer() -> Issuer {
        Issuer::new(
            "https://manager.example.com:4747",
            DEFAULT_JWKS_PATH,
            Duration::from_secs(900),
            None,
            now(),
        )
        .unwrap()
    }

    fn claims() -> WorkloadIdentityClaims {
        WorkloadIdentityClaims {
            region: "auto".into(),
            job: "api".into(),
            group: "web".into(),
            task: "server".into(),
            node_id: "node-1".into(),
            alloc_id: "alloc-42".into(),
            instance: None,
            slot: None,
            epoch: None,
        }
    }

    fn aws_identity() -> WorkloadIdentity {
        WorkloadIdentity {
            name: "aws".into(),
            audience: vec!["sts.amazonaws.com".into()],
            file: Some(WorkloadIdentityFile {
                path: "/var/run/legion/identity/aws/token".into(),
                mode: Some("0400".into()),
            }),
            ..Default::default()
        }
    }

    /// Decode + verify a token the way an external relying party would: using
    /// only the published JWKS, selecting the key by the `kid` header.
    fn verify_via_jwks(
        jwks: &Value,
        token: &str,
        audience: &str,
        issuer_url: &str,
    ) -> Map<String, Value> {
        let header = jsonwebtoken::decode_header(token).unwrap();
        let kid = header.kid.unwrap();
        let jwk = jwks["keys"]
            .as_array()
            .unwrap()
            .iter()
            .find(|k| k["kid"] == kid)
            .expect("kid present in jwks");
        let decoding =
            DecodingKey::from_ec_components(jwk["x"].as_str().unwrap(), jwk["y"].as_str().unwrap())
                .unwrap();
        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_issuer(&[issuer_url]);
        validation.set_audience(&[audience]);
        decode::<Map<String, Value>>(token, &decoding, &validation)
            .unwrap()
            .claims
    }

    #[test]
    fn token_verifies_against_published_jwks() {
        let iss = issuer();
        let issued = iss.issue(&claims(), &aws_identity(), now()).unwrap();
        let jwks = iss.jwks();

        let decoded = verify_via_jwks(&jwks, &issued.token, "sts.amazonaws.com", iss.issuer_url());
        assert_eq!(decoded["sub"], json!("auto:api:web:server"));
        assert_eq!(decoded["iss"], json!(iss.issuer_url()));
        assert_eq!(decoded["legion.io/alloc"], json!("alloc-42"));
        assert_eq!(decoded["aud"], json!(["sts.amazonaws.com"]));
    }

    #[test]
    fn header_kid_matches_a_jwks_entry() {
        let iss = issuer();
        let issued = iss.issue(&claims(), &aws_identity(), now()).unwrap();
        let header = jsonwebtoken::decode_header(&issued.token).unwrap();
        assert_eq!(header.kid.as_deref(), Some(issued.kid.as_str()));
        let jwks = iss.jwks();
        let kids: Vec<&str> = jwks["keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|k| k["kid"].as_str().unwrap())
            .collect();
        assert!(kids.contains(&issued.kid.as_str()));
    }

    #[test]
    fn verification_fails_for_wrong_audience() {
        let iss = issuer();
        let issued = iss.issue(&claims(), &aws_identity(), now()).unwrap();
        let jwks = iss.jwks();
        let header = jsonwebtoken::decode_header(&issued.token).unwrap();
        let jwk = &jwks["keys"][0];
        assert_eq!(header.kid.as_deref(), jwk["kid"].as_str());
        let decoding =
            DecodingKey::from_ec_components(jwk["x"].as_str().unwrap(), jwk["y"].as_str().unwrap())
                .unwrap();
        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_issuer(&[iss.issuer_url()]);
        validation.set_audience(&["vault.internal"]); // not the token's aud
        assert!(decode::<Map<String, Value>>(&issued.token, &decoding, &validation).is_err());
    }

    #[test]
    fn expired_token_fails_verification() {
        let iss = issuer();
        let short = WorkloadIdentity {
            ttl: Some("1s".into()),
            ..aws_identity()
        };
        // Issue far enough in the past that the 1s token is expired well beyond
        // jsonwebtoken's default 60s validation leeway.
        let issued_at = now() - Duration::from_secs(180);
        let issued = iss.issue(&claims(), &short, issued_at).unwrap();
        let err = iss.verify(&issued.token, "sts.amazonaws.com").unwrap_err();
        assert!(err.to_string().contains("verification"));
    }

    #[test]
    fn per_identity_sub_template_overrides_default() {
        let iss = issuer();
        let id = WorkloadIdentity {
            sub_template: Some("legion/{job}/{task}".into()),
            ..aws_identity()
        };
        let issued = iss.issue(&claims(), &id, now()).unwrap();
        let decoded = iss.verify(&issued.token, "sts.amazonaws.com").unwrap();
        assert_eq!(decoded["sub"], json!("legion/api/server"));
    }

    #[test]
    fn cluster_sub_template_applies_when_no_override() {
        let iss = Issuer::new(
            "https://m.example.com",
            DEFAULT_JWKS_PATH,
            Duration::from_secs(900),
            Some("{region}/{job}/{group}/{task}".into()),
            now(),
        )
        .unwrap();
        let issued = iss.issue(&claims(), &aws_identity(), now()).unwrap();
        let decoded = iss.verify(&issued.token, "sts.amazonaws.com").unwrap();
        assert_eq!(decoded["sub"], json!("auto/api/web/server"));
    }

    #[test]
    fn extra_claims_are_merged() {
        let iss = issuer();
        let mut id = aws_identity();
        id.extra_claims
            .insert("app.example.com/tier".into(), "backend".into());
        let issued = iss.issue(&claims(), &id, now()).unwrap();
        let decoded = iss.verify(&issued.token, "sts.amazonaws.com").unwrap();
        assert_eq!(decoded["app.example.com/tier"], json!("backend"));
    }

    #[test]
    fn extra_claims_cannot_override_derived_or_legion_claims() {
        // Belt-and-suspenders (P2): even with admission bypassed, the signing
        // sink must never let extra_claims overwrite a derived/trust-critical
        // claim. We inject `sub` and `legion.io/job` directly into the spec.
        let iss = issuer();
        let mut id = aws_identity();
        id.extra_claims.insert("sub".into(), "attacker".into());
        id.extra_claims
            .insert("legion.io/job".into(), "spoofed".into());
        id.extra_claims.insert("iss".into(), "evil-issuer".into());
        let issued = iss.issue(&claims(), &id, now()).unwrap();
        let decoded = iss.verify(&issued.token, "sts.amazonaws.com").unwrap();
        // Derived values win; the forged extra claims are dropped.
        assert_eq!(decoded["sub"], json!("auto:api:web:server"));
        assert_eq!(decoded["legion.io/job"], json!("api"));
        assert_eq!(decoded["iss"], json!(iss.issuer_url()));
    }

    #[test]
    fn token_exp_is_clamped_to_the_overlap_window() {
        // rotationAge/overlap is 900s here (Issuer::new uses default_ttl for
        // both). A per-identity ttl far above that must be clamped so the token
        // cannot outlive a retiring key's not_after.
        let iss = issuer();
        let long = WorkloadIdentity {
            ttl: Some("48h".into()),
            ..aws_identity()
        };
        let when = now();
        let issued = iss.issue(&claims(), &long, when).unwrap();
        let iat = when.duration_since(UNIX_EPOCH).unwrap().as_secs();
        // exp clamped to iat + overlap (900s), not iat + 48h.
        assert!(
            issued.expires_at <= iat + 900,
            "exp {} not clamped (iat {iat})",
            issued.expires_at
        );
        assert!(issued.expires_at >= iat + 899);
    }

    #[test]
    fn long_ttl_token_signed_before_rotation_still_verifies_after_rotation() {
        // The whole point of P1: a token signed right before rotation must stay
        // verifiable until it expires, because its (clamped) exp falls inside
        // the retiring key's overlap window.
        let mut iss = issuer();
        let overlap = Duration::from_secs(900);
        let long = WorkloadIdentity {
            ttl: Some("24h".into()), // would outlive the key without clamping
            ..aws_identity()
        };
        let issued = iss.issue(&claims(), &long, now()).unwrap();

        // Rotate immediately after signing: the just-used key starts retiring
        // with not_after = now + overlap.
        iss.rotate(now(), overlap).unwrap();

        // The pre-rotation token still verifies during the overlap window — its
        // exp was clamped to <= the key's not_after.
        iss.verify(&issued.token, "sts.amazonaws.com").unwrap();
    }

    #[test]
    fn control_plane_identity_audience_defaults_to_issuer() {
        let iss = issuer();
        let id = WorkloadIdentity {
            name: CONTROL_PLANE_IDENTITY_NAME.into(),
            audience: vec![], // manager owns the audience for the built-in identity
            file: Some(WorkloadIdentityFile {
                path: "/var/run/legion/identity/legion/token".into(),
                mode: None,
            }),
            ..Default::default()
        };
        let issued = iss.issue(&claims(), &id, now()).unwrap();
        // aud == issuer, so verification against the cluster issuer succeeds.
        let decoded = iss.verify(&issued.token, iss.issuer_url()).unwrap();
        assert_eq!(decoded["aud"], json!([iss.issuer_url()]));
    }

    #[test]
    fn discovery_document_has_required_fields() {
        let iss = issuer();
        let doc = iss.discovery_document();
        assert_eq!(doc["issuer"], json!(iss.issuer_url()));
        assert_eq!(
            doc["jwks_uri"],
            json!(format!("{}{}", iss.issuer_url(), DEFAULT_JWKS_PATH))
        );
        assert_eq!(
            doc["id_token_signing_alg_values_supported"],
            json!(["ES256"])
        );
    }

    // --- rotation state machine ---

    #[test]
    fn rotation_keeps_retiring_key_verifiable_during_overlap() {
        let mut iss = issuer();
        let max_ttl = Duration::from_secs(900);

        // Token signed by the original (soon-to-retire) key.
        let issued = iss.issue(&claims(), &aws_identity(), now()).unwrap();
        let original_kid = issued.kid.clone();

        iss.rotate(now(), max_ttl).unwrap();

        // New active key is different; both are published in the JWKS.
        let active = iss.active_key().unwrap();
        assert_ne!(active.kid, original_kid);
        let kids: Vec<String> = iss.jwks()["keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|k| k["kid"].as_str().unwrap().to_string())
            .collect();
        assert!(kids.contains(&original_kid));
        assert!(kids.contains(&active.kid));

        // The pre-rotation token still verifies during the overlap window.
        iss.verify(&issued.token, "sts.amazonaws.com").unwrap();
    }

    #[test]
    fn retiring_key_dropped_after_overlap_window_closes() {
        let mut iss = issuer();
        let max_ttl = Duration::from_secs(900);

        let issued = iss.issue(&claims(), &aws_identity(), now()).unwrap();
        let original_kid = issued.kid.clone();

        // Rotate as-of 20 minutes ago so the retiring key's not_after is already
        // in the past, then expire.
        let rotated_at = now() - Duration::from_secs(20 * 60);
        iss.rotate(rotated_at, max_ttl).unwrap();
        let expired = iss.expire_keys(now()).unwrap();
        assert_eq!(expired.len(), 1);

        // Key is gone from the JWKS and verification can no longer find it.
        let kids: Vec<String> = iss.jwks()["keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|k| k["kid"].as_str().unwrap().to_string())
            .collect();
        assert!(!kids.contains(&original_kid));
        assert!(iss.verify(&issued.token, "sts.amazonaws.com").is_err());
    }

    #[test]
    fn newly_signed_tokens_use_the_new_active_key_after_rotation() {
        let mut iss = issuer();
        let first = iss.issue(&claims(), &aws_identity(), now()).unwrap();
        iss.rotate(now(), Duration::from_secs(900)).unwrap();
        let second = iss.issue(&claims(), &aws_identity(), now()).unwrap();
        assert_ne!(first.kid, second.kid);
        assert_eq!(second.kid, iss.active_key().unwrap().kid);
    }

    // --- claims derivation from the scheduling record ---

    fn claims_test_store() -> R<Store> {
        let store_dir = tempfile::Builder::new()
            .prefix("legion-wi-claims-")
            .tempdir()
            .context("error creating temp store directory")?
            .keep();
        let config = crate::structs::ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("error parsing store path")?
                .to_string(),
            ..Default::default()
        };
        let store = Store::start_storage(&config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    fn submit_and_place(store: &Store, task: crate::structs::TaskConfig) -> R<Allocation> {
        use crate::structs::{JobConfig, NodeMsg, Submission};
        store.insert_node(NodeMsg {
            id: "node-1".into(),
            pool: "default".into(),
            status: "active".into(),
            availability: "available".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: "{}".into(),
            dynamic_attrs: "{}".into(),
            health_checks: "{}".into(),
            last_heartbeat: "2026-01-01 00:00:00 +00:00:00".into(),
            heartbeat_source: String::new(),
        })?;
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "claims-job".into(),
            tasks: vec![task],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: "job.ncl".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config("claims-job")?.id();
        let (task_id, _) = store
            .get_task_configs_for_job_config(&job_config_id)?
            .into_iter()
            .next()
            .context("missing task config")?;
        store.place_alloc("node-1", task_id)
    }

    #[test]
    fn claims_for_stateless_allocation_have_no_instance() -> R<()> {
        let store = claims_test_store()?;
        let task = crate::structs::TaskConfig::new("server", "noop");
        let allocation = submit_and_place(&store, task.clone())?;

        let derived = claims_for_allocation(&store, &allocation, &task, "auto")?;
        assert_eq!(derived.job, "claims-job");
        assert_eq!(derived.task, "server");
        assert_eq!(derived.group, "default");
        assert_eq!(derived.alloc_id, allocation.id.to_string());
        assert_eq!(derived.node_id, "node-1");
        assert_eq!(derived.instance, None);
        assert_eq!(derived.slot, None);
        assert!(!derived.is_stateful());
        Ok(())
    }

    #[test]
    fn claims_for_stateful_allocation_carry_instance_and_slot() -> R<()> {
        use crate::structs::{InstanceMode, StatefulConfig, TaskConfig};

        let store = claims_test_store()?;
        let task = TaskConfig {
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Slot,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
                envelope: None,
                promote: None,
            }),
            ..TaskConfig::new("db", "noop")
        };
        let allocation = submit_and_place(&store, task.clone())?;
        assert!(allocation.instance_id.is_some());

        let derived = claims_for_allocation(&store, &allocation, &task, "auto")?;
        assert_eq!(derived.instance.as_deref(), Some("claims-job-default-0"));
        assert_eq!(derived.slot, Some(0));
        assert!(derived.is_stateful());

        // The instance-keyed sub template now renders, and the legion claims
        // carry the stable instance key.
        let sub = derived.render_subject("{region}:{job}:{group}:{task}:{instance}")?;
        assert_eq!(sub, "auto:claims-job:default:db:claims-job-default-0");
        let legion = derived.legion_claims();
        assert_eq!(
            legion.get("legion.io/instance").map(String::as_str),
            Some("claims-job-default-0")
        );
        assert_eq!(legion.get("legion.io/slot").map(String::as_str), Some("0"));
        Ok(())
    }

    // --- signing-key persistence (restart semantics) ---

    fn store_issuer(store: &Store) -> Issuer {
        Issuer::load_or_generate(
            store,
            "https://manager.example.com:4747",
            DEFAULT_JWKS_PATH,
            Duration::from_secs(900),
            None,
            now(),
        )
        .unwrap()
    }

    #[test]
    fn first_boot_generates_and_persists_the_active_key() -> R<()> {
        let store = claims_test_store()?;
        let iss = store_issuer(&store);

        let records = store.list_signing_keys()?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].state, "active");
        assert_eq!(records[0].alg, "ES256");
        assert_eq!(records[0].kid, iss.active_key().unwrap().kid);
        Ok(())
    }

    #[test]
    fn issuer_reload_verifies_token_issued_before_restart() -> R<()> {
        let store = claims_test_store()?;

        // "First boot": issue a token, then drop the issuer entirely.
        let first = store_issuer(&store);
        let issued = first.issue(&claims(), &aws_identity(), now())?;
        let first_kid = issued.kid.clone();
        drop(first);

        // "Restart": a brand-new issuer over the same store must restore the
        // same key — same kid in the JWKS, and the old token still verifies.
        let second = store_issuer(&store);
        assert_eq!(second.active_key().unwrap().kid, first_kid);
        let decoded = second.verify(&issued.token, "sts.amazonaws.com")?;
        assert_eq!(decoded["sub"], json!("auto:api:web:server"));

        // No duplicate key was generated.
        assert_eq!(store.list_signing_keys()?.len(), 1);
        Ok(())
    }

    #[test]
    fn rotation_persists_and_survives_restart() -> R<()> {
        let store = claims_test_store()?;
        let mut first = store_issuer(&store);
        let issued = first.issue(&claims(), &aws_identity(), now())?;
        let original_kid = issued.kid.clone();

        first.rotate_persisted(&store, now(), Duration::from_secs(900))?;
        let new_kid = first.active_key().unwrap().kid.clone();
        drop(first);

        let records = store.list_signing_keys()?;
        assert_eq!(records.len(), 2);
        let by_kid = |kid: &str| records.iter().find(|r| r.kid == kid).unwrap();
        assert_eq!(by_kid(&original_kid).state, "retiring");
        assert!(by_kid(&original_kid).not_after.is_some());
        assert_eq!(by_kid(&new_kid).state, "active");

        // After a restart both keys are served and the pre-rotation token
        // still verifies during the overlap window.
        let second = store_issuer(&store);
        assert_eq!(second.active_key().unwrap().kid, new_kid);
        second.verify(&issued.token, "sts.amazonaws.com")?;
        let kids: Vec<&str> = records.iter().map(|r| r.kid.as_str()).collect();
        for key in second.jwks()["keys"].as_array().unwrap() {
            assert!(kids.contains(&key["kid"].as_str().unwrap()));
        }
        Ok(())
    }

    #[test]
    fn expired_keys_are_marked_in_store_and_not_reloaded() -> R<()> {
        let store = claims_test_store()?;
        let mut iss = store_issuer(&store);
        let original_kid = iss.active_key().unwrap().kid.clone();

        // Rotate in the past so the retiring key's window is already closed.
        let rotated_at = now() - Duration::from_secs(20 * 60);
        iss.rotate_persisted(&store, rotated_at, Duration::from_secs(900))?;
        let expired = iss.expire_keys_persisted(&store, now())?;
        assert_eq!(expired, 1);

        let records = store.list_signing_keys()?;
        let original = records.iter().find(|r| r.kid == original_kid).unwrap();
        assert_eq!(original.state, "expired");
        // The audit row keeps its recorded overlap window.
        assert!(original.not_after.is_some());

        // A restarted issuer loads only the non-expired key.
        let reloaded = store_issuer(&store);
        assert!(reloaded.key(&original_kid).is_none());
        assert_eq!(reloaded.jwks()["keys"].as_array().unwrap().len(), 1);
        Ok(())
    }

    #[test]
    fn invalid_cluster_default_sub_template_fails_initialize() -> R<()> {
        let store = claims_test_store()?;
        // Below the uniqueness floor: collapses distinct workloads into one.
        let config = WorkloadIdentityConfigFull {
            enabled: true,
            issuer: Some("https://manager.example.com:4747".into()),
            sub_template: "{job}".into(),
            ..Default::default()
        };
        let err =
            WorkloadIdentityState::initialize(&store, &config, "http://127.0.0.1:4747", now())
                .unwrap_err();
        assert!(err.to_string().contains("subTemplate"), "{err}");

        // A churning placeholder in the cluster default is also rejected.
        let config = WorkloadIdentityConfigFull {
            enabled: true,
            issuer: Some("https://manager.example.com:4747".into()),
            sub_template: "{job}:{group}:{task}:{alloc}".into(),
            ..Default::default()
        };
        let err =
            WorkloadIdentityState::initialize(&store, &config, "http://127.0.0.1:4747", now())
                .unwrap_err();
        assert!(err.to_string().contains("subTemplate"), "{err}");
        Ok(())
    }

    #[test]
    fn valid_cluster_default_sub_template_passes_initialize() -> R<()> {
        let store = claims_test_store()?;
        let config = WorkloadIdentityConfigFull {
            enabled: true,
            issuer: Some("https://manager.example.com:4747".into()),
            sub_template: "{region}/{job}/{group}/{task}".into(),
            ..Default::default()
        };
        WorkloadIdentityState::initialize(&store, &config, "http://127.0.0.1:4747", now())?;
        Ok(())
    }

    #[test]
    fn rotation_tick_rotates_only_past_rotation_age() -> R<()> {
        let store = claims_test_store()?;
        let config = WorkloadIdentityConfigFull {
            enabled: true,
            issuer: Some("https://manager.example.com:4747".into()),
            rotation_age: "24h".into(),
            ..Default::default()
        };
        let state =
            WorkloadIdentityState::initialize(&store, &config, "http://127.0.0.1:4747", now())?;

        // Fresh key: not due.
        assert!(!state.rotation_tick(&store, now())?);
        assert_eq!(store.list_signing_keys()?.len(), 1);

        // Pretend 25h elapsed: due.
        let later = now() + Duration::from_secs(25 * 3600);
        assert!(state.rotation_tick(&store, later)?);
        let records = store.list_signing_keys()?;
        assert_eq!(records.len(), 2);
        assert_eq!(records.iter().filter(|r| r.state == "active").count(), 1);
        assert_eq!(records.iter().filter(|r| r.state == "retiring").count(), 1);

        // Far past the overlap window the retiring key is expired.
        let much_later = now() + Duration::from_secs(80 * 3600);
        state.rotation_tick(&store, much_later)?;
        let records = store.list_signing_keys()?;
        assert_eq!(records.iter().filter(|r| r.state == "expired").count(), 1);
        Ok(())
    }

    #[test]
    fn old_key_tokens_verify_until_not_after_and_new_tokens_use_new_kid() -> R<()> {
        let store = claims_test_store()?;
        let config = WorkloadIdentityConfigFull {
            enabled: true,
            issuer: Some("https://manager.example.com:4747".into()),
            ..Default::default()
        };
        let state =
            WorkloadIdentityState::initialize(&store, &config, "http://127.0.0.1:4747", now())?;

        let old = state.issue(&claims(), &aws_identity(), now())?;
        let later = now() + Duration::from_secs(25 * 3600);
        assert!(state.rotation_tick(&store, later)?);

        // Old-key token still verifies during the overlap (the token itself
        // has expired by `later`, but key resolution must still succeed —
        // verify against fresh wall-clock where the token is alive).
        state.verify(&old.token, "sts.amazonaws.com")?;

        let new = state.issue(&claims(), &aws_identity(), now())?;
        assert_ne!(new.kid, old.kid);
        Ok(())
    }

    #[test]
    fn initialize_rejects_unsupported_signing_alg() -> R<()> {
        let store = claims_test_store()?;
        let config = WorkloadIdentityConfigFull {
            enabled: true,
            signing_alg: "RS256".into(),
            ..Default::default()
        };
        let err = WorkloadIdentityState::initialize(&store, &config, "https://m:4747", now())
            .unwrap_err();
        assert!(err.to_string().contains("signingAlg"), "{err}");
        Ok(())
    }

    #[test]
    fn initialize_rejects_jwks_path_without_leading_slash() -> R<()> {
        let store = claims_test_store()?;
        let config = WorkloadIdentityConfigFull {
            enabled: true,
            jwks_path: "jwks.json".into(),
            ..Default::default()
        };
        let err = WorkloadIdentityState::initialize(&store, &config, "https://m:4747", now())
            .unwrap_err();
        assert!(err.to_string().contains("jwksPath"), "{err}");
        Ok(())
    }

    #[test]
    fn initialize_rejects_malformed_durations() -> R<()> {
        for (ttl, rot) in [("nonsense", "24h"), ("15m", "24hours!")] {
            let store = claims_test_store()?;
            let config = WorkloadIdentityConfigFull {
                enabled: true,
                default_ttl: ttl.into(),
                rotation_age: rot.into(),
                ..Default::default()
            };
            let err = WorkloadIdentityState::initialize(&store, &config, "https://m:4747", now())
                .unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("defaultTtl") || msg.contains("rotationAge"),
                "{msg}"
            );
        }
        Ok(())
    }

    #[test]
    fn initialize_trims_trailing_slash_from_issuer() -> R<()> {
        let store = claims_test_store()?;
        let config = WorkloadIdentityConfigFull {
            enabled: true,
            issuer: Some("https://manager.example.com:4747/".into()),
            ..Default::default()
        };
        let state = WorkloadIdentityState::initialize(&store, &config, "https://m:4747", now())?;
        // No double slash in the advertised jwks_uri.
        let discovery = state.discovery_document()?;
        assert_eq!(
            discovery["jwks_uri"],
            json!("https://manager.example.com:4747/.well-known/jwks.json")
        );
        Ok(())
    }

    #[test]
    fn standby_reload_picks_up_primary_rotated_key() -> R<()> {
        // Two issuers over one store model a primary + a standby manager
        // sharing the replicated signing_key table (the plan's HA case).
        let store = claims_test_store()?;
        let mut primary = store_issuer(&store);
        let standby = store_issuer(&store);

        // Same starting key on both.
        let start_kid = primary.active_key().unwrap().kid.clone();
        assert_eq!(standby.active_key().unwrap().kid, start_kid);

        // Primary rotates; the new key lands in the store.
        primary.rotate_persisted(&store, now(), Duration::from_secs(900))?;
        let new_kid = primary.active_key().unwrap().kid.clone();
        assert_ne!(new_kid, start_kid);

        // Before reload the standby's JWKS lacks the new kid...
        let jwks_before = standby.jwks();
        let has = |jwks: &Value, kid: &str| {
            jwks["keys"]
                .as_array()
                .unwrap()
                .iter()
                .any(|k| k["kid"] == json!(kid))
        };
        assert!(!has(&jwks_before, &new_kid));

        // ...after reloading from the store it serves the new key and can
        // verify a token the primary signed with it.
        let mut standby = standby;
        assert!(standby.reload_keys(&store)?);
        assert!(has(&standby.jwks(), &new_kid));
        assert_eq!(standby.active_key().unwrap().kid, new_kid);
        let token = primary.issue(&claims(), &aws_identity(), now())?;
        assert_eq!(token.kid, new_kid);
        standby.verify(&token.token, "sts.amazonaws.com")?;
        Ok(())
    }

    #[test]
    fn control_plane_identity_is_issued_with_issuer_audience() -> R<()> {
        let iss = issuer();
        // The reserved `legion` identity carries no operator audience; the
        // manager stamps aud = the cluster issuer.
        let legion = WorkloadIdentity {
            name: crate::structs::workload_identity::CONTROL_PLANE_IDENTITY_NAME.into(),
            audience: Vec::new(),
            file: Some(WorkloadIdentityFile {
                path: "/run/legion/token".into(),
                mode: None,
            }),
            ..Default::default()
        };
        let issued = iss.issue(&claims(), &legion, now())?;
        // Verifies with expected audience = issuer, and not with a foreign one.
        iss.verify(&issued.token, iss.issuer_url())?;
        assert!(iss.verify(&issued.token, "sts.amazonaws.com").is_err());
        Ok(())
    }

    #[test]
    fn node_token_verifies_and_carries_node_scope() -> R<()> {
        use crate::structs::workload_identity::{NodeIdentityClaims, NodeIdentityConfig};
        let iss = issuer();
        let claims = NodeIdentityClaims {
            region: "auto".into(),
            pool: "default".into(),
            node_id: "node-7".into(),
        };
        let identity = NodeIdentityConfig {
            name: "pull".into(),
            audience: vec!["sts.amazonaws.com".into()],
            ttl: Some("15m".into()),
            sub_template: None,
        };
        let issued = iss.issue_node(&claims, &identity, now())?;
        let jwks = iss.jwks();
        let decoded = verify_via_jwks(&jwks, &issued.token, "sts.amazonaws.com", iss.issuer_url());
        assert_eq!(decoded["sub"], json!("auto:node:default"));
        assert_eq!(decoded["legion.io/scope"], json!("node"));
        assert_eq!(decoded["legion.io/node"], json!("node-7"));
        Ok(())
    }

    #[test]
    fn base64url_matches_known_vectors() {
        // RFC 4648 test vectors, url-safe, unpadded.
        assert_eq!(base64url(b""), "");
        assert_eq!(base64url(b"f"), "Zg");
        assert_eq!(base64url(b"fo"), "Zm8");
        assert_eq!(base64url(b"foo"), "Zm9v");
        assert_eq!(base64url(b"foob"), "Zm9vYg");
        assert_eq!(base64url(b"fooba"), "Zm9vYmE");
        assert_eq!(base64url(b"foobar"), "Zm9vYmFy");
    }
}
