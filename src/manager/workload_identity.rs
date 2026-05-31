//! Manager-as-OIDC-IdP: signing keys, token issuance, JWKS, and the OIDC
//! discovery document.
//!
//! See `docs/plans/workload-identity-oidc-plan.md`. The spec/claims primitives
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
//! public JWK leaves this module.
//!
//! What this module deliberately does *not* yet do (deferred phases, see the
//! plan): mount the discovery/JWKS routes on the manager's TLS listener
//! (Phase 1 wiring), the `IssueWorkloadIdentityToken` gRPC (Phase 2), worker
//! delivery/refresh (Phase 4), SQLite key persistence, and the background
//! rotation loop (Phase 5). The primitives here are built so those phases are
//! wiring, not redesign — [`Issuer::rotate`] / [`Issuer::expire_keys`] already
//! implement the rotation state machine a loop would drive.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::prelude::*;
use crate::structs::workload_identity::{
    WorkloadIdentity, WorkloadIdentityClaims, CONTROL_PLANE_IDENTITY_NAME,
};

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
    /// Cluster-default `sub` template; `None` means use the built-in default.
    sub_template: Option<String>,
    /// Active key first, then any retiring keys still inside their overlap window.
    keys: Vec<SigningKey>,
}

impl Issuer {
    /// Create an issuer with a freshly generated active key.
    pub fn new(
        issuer_url: impl Into<String>,
        jwks_path: impl Into<String>,
        default_ttl: Duration,
        sub_template: Option<String>,
        now: SystemTime,
    ) -> R<Self> {
        let key = SigningKey::generate(unix_secs(now)?)?;
        Ok(Self {
            issuer_url: issuer_url.into(),
            jwks_path: jwks_path.into(),
            default_ttl,
            sub_template,
            keys: vec![key],
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
        let exp = iat + ttl.as_secs();

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
        body.insert("iss".into(), json!(self.issuer_url));
        body.insert("sub".into(), json!(subject));
        body.insert("aud".into(), json!(audience));
        body.insert("iat".into(), json!(iat));
        body.insert("nbf".into(), json!(iat));
        body.insert("exp".into(), json!(exp));
        body.insert("jti".into(), json!(jti(&subject, iat, now)));

        for (claim, value) in claims.legion_claims() {
            body.insert(claim, json!(value));
        }
        // Static, non-reserved extra claims (reserved keys rejected at admission).
        for (claim, value) in &identity.extra_claims {
            body.insert(claim.clone(), json!(value));
        }

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

    /// Rotate the signing key: the current active key becomes `retiring` with a
    /// `not_after` covering the last token it could have signed, and a new
    /// active key is generated. `max_token_ttl` should be the largest TTL any
    /// identity can request so no signed token outlives the overlap window.
    pub fn rotate(&mut self, now: SystemTime, max_token_ttl: Duration) -> R<()> {
        let now_secs = unix_secs(now)?;
        for key in &mut self.keys {
            if key.state == KeyState::Active {
                key.state = KeyState::Retiring;
                key.not_after = Some(now_secs + max_token_ttl.as_secs());
            }
        }
        let new_key = SigningKey::generate(now_secs)?;
        self.keys.insert(0, new_key);
        Ok(())
    }

    /// Drop retiring keys whose overlap window has closed. Returns how many keys
    /// were expired. A driving loop calls this periodically.
    pub fn expire_keys(&mut self, now: SystemTime) -> R<usize> {
        let now_secs = unix_secs(now)?;
        let before = self.keys.len();
        self.keys.retain(|k| match (k.state, k.not_after) {
            (KeyState::Retiring, Some(not_after)) => now_secs <= not_after,
            _ => true,
        });
        Ok(before - self.keys.len())
    }
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
        assert_eq!(expired, 1);

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
