//! Worker join tokens.
//!
//! A join token introduces a worker to the cluster in one command. It is a
//! self-describing composite: base64url-encoded JSON carrying the manager
//! address, the worker type, and a random secret. The secret is the only
//! authenticated part — the manager stores its SHA-256 hash and validates it
//! at `WorkerPreRegister`/`WorkerRegister` when ACLs are enforced. The
//! address and worker type are conveniences so `legion join <TOKEN>` needs no
//! other configuration.
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
    /// Format version, for forward compatibility.
    pub v: u8,
    /// Manager address the worker should connect to (for example
    /// `http://10.0.0.5:4747`).
    pub addr: String,
    /// Random secret, validated against the manager's stored hash.
    pub secret: String,
    /// Worker type (template) this token introduces.
    pub worker_type: String,
}

impl JoinToken {
    pub fn new(
        addr: impl Into<String>,
        secret: impl Into<String>,
        worker_type: impl Into<String>,
    ) -> Self {
        JoinToken {
            v: 1,
            addr: addr.into(),
            secret: secret.into(),
            worker_type: worker_type.into(),
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
        let decoded: JoinToken =
            serde_json::from_slice(&bytes).context("join token payload is not valid")?;
        if decoded.v != 1 {
            bail!(
                "join token format v{} is newer than this binary supports (v1); upgrade legion",
                decoded.v
            );
        }
        if decoded.addr.is_empty() || decoded.secret.is_empty() {
            bail!("join token is missing its manager address or secret");
        }
        Ok(decoded)
    }
}

#[cfg(test)]
mod join_token_test {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() -> R<()> {
        let token = JoinToken::new("http://10.0.0.5:4747", "ljt_abc123", "gpu");
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

    #[test]
    fn decode_rejects_future_versions_and_empty_fields() -> R<()> {
        let future = format!(
            "{JOIN_TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(
                &json!({"v": 9, "addr": "a", "secret": "s", "workerType": "t"})
            )?)
        );
        assert!(JoinToken::decode(&future)
            .unwrap_err()
            .to_string()
            .contains("upgrade"));

        let empty = format!(
            "{JOIN_TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(
                &json!({"v": 1, "addr": "", "secret": "", "workerType": "t"})
            )?)
        );
        assert!(JoinToken::decode(&empty).is_err());
        Ok(())
    }
}
