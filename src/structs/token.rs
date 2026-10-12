//! Worker join tokens.
//!
//! A join token introduces a worker to the cluster in one command. It is a
//! self-describing composite: base64url-encoded JSON carrying the manager
//! address, the worker type, a random secret, and the pin of the cluster CA
//! (`caHash = "sha256:<hex>"`). The secret is what the manager
//! authenticates — it stores its SHA-256 hash and validates it at
//! `WorkerPreRegister`/`WorkerRegister` when ACLs are enforced. The pin is
//! what the *worker* authenticates the manager with: it refuses any manager
//! whose CA does not match, so whoever answers at a stolen or spoofed address
//! can't pose as the cluster (worker-plane-auth plan §6.5). The address and
//! worker type are conveniences so `legion join <TOKEN>` needs no other
//! configuration.
//!
//! This replaces the original HS512 JWT join tokens, which were signed with a
//! compiled-in constant key: anyone with the binary could forge them, so they
//! authenticated nothing and amounted to an obfuscated config blob. A stored
//! random secret is simpler and actually verifiable server-side.

use crate::prelude::*;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

/// Prefix for composite join tokens (recognizable in configs and logs).
pub const JOIN_TOKEN_PREFIX: &str = "lgnjoin_";

/// Prefix for the secret embedded in a join token.
pub const JOIN_SECRET_PREFIX: &str = "ljt_";

/// Decoded contents of a composite join token.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JoinToken {
    /// Format version. Version 2 added the required CA pin; version 1 tokens
    /// are refused.
    pub v: u8,
    /// Manager address the worker should connect to (for example
    /// `http://10.0.0.5:4747`).
    pub addr: String,
    /// Random secret, validated against the manager's stored hash.
    pub secret: String,
    /// Worker type (template) this token introduces.
    pub worker_type: String,
    /// The pin of the cluster CA the managers sign with when the token was
    /// minted: `sha256:` plus the hex SHA-256 of the CA certificate's DER.
    pub ca_hash: String,
}

/// The join token format this binary writes and reads.
pub const JOIN_TOKEN_VERSION: u8 = 2;

impl JoinToken {
    pub fn new(
        addr: impl Into<String>,
        secret: impl Into<String>,
        worker_type: impl Into<String>,
        ca_hash: impl Into<String>,
    ) -> Self {
        JoinToken {
            v: JOIN_TOKEN_VERSION,
            addr: addr.into(),
            secret: secret.into(),
            worker_type: worker_type.into(),
            ca_hash: ca_hash.into(),
        }
    }

    /// Encodes to the composite string handed to workers.
    pub fn encode(&self) -> R<String> {
        let json = serde_json::to_vec(self)?;
        Ok(format!(
            "{JOIN_TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(json)
        ))
    }

    /// Decodes a composite token string.
    pub fn decode(token: &str) -> R<JoinToken> {
        let encoded = token.trim().strip_prefix(JOIN_TOKEN_PREFIX).ok_or_else(|| {
            anyhow!("not a join token (expected a `{JOIN_TOKEN_PREFIX}...` string from `legion join-token create`)")
        })?;
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .context("join token is not valid base64url")?;
        let raw: Value =
            serde_json::from_slice(&bytes).context("join token payload is not valid")?;
        let version = raw.get("v").and_then(Value::as_u64).unwrap_or(0);
        if version < JOIN_TOKEN_VERSION as u64 {
            bail!(
                "this join token (format v{version}) does not pin the cluster CA, so the worker \
                 can't tell its manager from an impostor\n\
                 Hint: mint a new token with `legion join-token create`"
            );
        }
        if version > JOIN_TOKEN_VERSION as u64 {
            bail!(
                "join token format v{version} is newer than this binary supports \
                 (v{JOIN_TOKEN_VERSION}); upgrade legion"
            );
        }
        let decoded: JoinToken =
            serde_json::from_value(raw).context("join token payload is not valid")?;
        if decoded.addr.is_empty() || decoded.secret.is_empty() {
            bail!("join token is missing its manager address or secret");
        }
        if !decoded
            .ca_hash
            .starts_with(crate::cluster_tls::CA_PIN_PREFIX)
            || decoded.ca_hash.len() != crate::cluster_tls::CA_PIN_PREFIX.len() + 64
        {
            bail!("join token is missing its cluster CA pin (caHash)");
        }
        Ok(decoded)
    }
}

#[cfg(test)]
mod join_token_test {
    use super::*;

    const PIN: &str = "sha256:9c1e4f0000000000000000000000000000000000000000000000000000000000";

    #[test]
    fn encode_decode_roundtrip() -> R<()> {
        let token = JoinToken::new("https://10.0.0.5:4747", "ljt_abc123", "gpu", PIN);
        let encoded = token.encode()?;
        assert!(encoded.starts_with(JOIN_TOKEN_PREFIX));
        let decoded = JoinToken::decode(&encoded)?;
        assert_eq!(decoded, token);
        // Whitespace from copy/paste is tolerated.
        assert_eq!(JoinToken::decode(&format!("  {encoded}\n"))?, token);
        Ok(())
    }

    #[test]
    fn decode_rejects_malformed_tokens() {
        assert!(JoinToken::decode("not-a-token").is_err());
        assert!(JoinToken::decode("lgnjoin_!!!not-base64!!!").is_err());
        let not_json = format!("{JOIN_TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(b"hello"));
        assert!(JoinToken::decode(&not_json).is_err());
        // Old-style JWTs are rejected with a pointer to the new command.
        let err = JoinToken::decode("eyJhbGciOiJIUzUxMiJ9.x.y").unwrap_err();
        assert!(err.to_string().contains("join-token create"));
    }

    fn encoded(value: Value) -> R<String> {
        Ok(format!(
            "{JOIN_TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&value)?)
        ))
    }

    #[test]
    fn decode_rejects_future_versions_and_empty_fields() -> R<()> {
        let future =
            encoded(json!({"v": 9, "addr": "a", "secret": "s", "workerType": "t", "caHash": PIN}))?;
        assert!(JoinToken::decode(&future)
            .unwrap_err()
            .to_string()
            .contains("upgrade"));

        let empty =
            encoded(json!({"v": 2, "addr": "", "secret": "", "workerType": "t", "caHash": PIN}))?;
        assert!(JoinToken::decode(&empty).is_err());
        Ok(())
    }

    #[test]
    fn a_v1_token_without_a_ca_pin_is_refused_with_a_hint_test() -> R<()> {
        let v1 = encoded(
            json!({"v": 1, "addr": "http://10.0.0.5:4747", "secret": "ljt_x", "workerType": "t"}),
        )?;
        let err = JoinToken::decode(&v1).unwrap_err().to_string();
        assert!(err.contains("does not pin the cluster CA"), "{err}");
        assert!(err.contains("legion join-token create"), "{err}");

        let unpinned =
            encoded(json!({"v": 2, "addr": "a", "secret": "s", "workerType": "t", "caHash": ""}))?;
        assert!(JoinToken::decode(&unpinned)
            .unwrap_err()
            .to_string()
            .contains("caHash"));
        Ok(())
    }
}
