//! Exec tickets: the credential for direct exec (sandbox-exec plan §6.3,
//! §6.4; docs/exec-and-actions.md, "Direct exec").
//!
//! A manager mints one after running the same authorization `ExecAllocation`
//! runs; the client presents it to the worker on the worker plane, and the
//! worker verifies it offline against the cluster JWKS (fetched over TLS
//! pinned to the cluster CA). It is a JWT signed with the cluster's
//! workload-identity key, like a boot ticket, with its own audience so the
//! two can never stand in for each other.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The `aud` of an exec ticket.
pub const EXEC_TICKET_AUDIENCE: &str = "legion:exec";

/// Seconds a ticket may be redeemed for, when the mint names none.
pub const DEFAULT_TTL_SECS: u64 = 60;
/// The longest redeem window a mint may ask for.
pub const MAX_TTL_SECS: u64 = 600;
/// How long a session opened with a ticket may run, when the mint names
/// none.
pub const DEFAULT_MAX_SESSION_SECS: u64 = 3600;

/// The Legion claims of an exec ticket. `iss`, `aud`, `exp`, and the
/// signature are checked by the JWT layer; everything here is checked by
/// the worker against its own state.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExecTicketClaims {
    /// `accessor:<id>` of the token that minted it (audit, revocation), or
    /// `key:<id>` for a delegated minting key.
    pub sub: String,
    pub jti: String,
    pub iat: i64,
    pub exp: i64,
    /// The node the ticket is redeemable on.
    #[serde(rename = "legion.io/node")]
    pub node: String,
    #[serde(rename = "legion.io/job")]
    pub job: String,
    #[serde(rename = "legion.io/task")]
    pub task: String,
    /// The allocation the session runs in.
    #[serde(rename = "legion.io/alloc")]
    pub allocation_id: i64,
    /// The instance the caller named, when it named one.
    #[serde(
        rename = "legion.io/instance",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub instance: Option<String>,
    /// The rendered argv the session must run. Absent on an `exec` rule
    /// ticket minted without a command: any argv.
    #[serde(
        rename = "legion.io/argv",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub argv: Option<Vec<String>>,
    /// Set when minted for an action.
    #[serde(
        rename = "legion.io/action",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub action: Option<String>,
    /// Whether the session may (and must) be a PTY session.
    #[serde(rename = "legion.io/tty", default)]
    pub tty: bool,
    /// Seconds the session may run before the worker ends it.
    #[serde(rename = "legion.io/max_session")]
    pub max_session: u64,
    /// Redemptions allowed. Always 1 today.
    #[serde(rename = "legion.io/uses", default = "one")]
    pub uses: u32,
}

fn one() -> u32 {
    1
}

/// The prefix of a delegated exec key's id, and so of the `kid` a ticket
/// signed with one carries.
pub const EXEC_KEY_PREFIX: &str = "xk_";

/// A new delegated key id (`xk_` and 16 random characters).
pub fn new_exec_key_id() -> String {
    format!(
        "{EXEC_KEY_PREFIX}{}",
        crate::util::random_string(16).to_lowercase()
    )
}

/// The DER prefix of every P-256 SubjectPublicKeyInfo: the
/// `id-ecPublicKey` / `prime256v1` algorithm identifier and the bit-string
/// header of a 65-byte uncompressed point.
const P256_SPKI_PREFIX: [u8; 26] = [
    0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08, 0x2a,
    0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
];

/// Checks that `pem` is one P-256 public key (SPKI, `-----BEGIN PUBLIC
/// KEY-----`), the only kind an ES256 ticket can be verified with.
pub fn validate_p256_public_key(pem: &str) -> anyhow::Result<()> {
    use base64::Engine;
    let body: String = pem
        .lines()
        .map(str::trim)
        .skip_while(|line| *line != "-----BEGIN PUBLIC KEY-----")
        .skip(1)
        .take_while(|line| *line != "-----END PUBLIC KEY-----")
        .collect();
    if body.is_empty() {
        anyhow::bail!(
            "the public key must be PEM with a `-----BEGIN PUBLIC KEY-----` block \
             (`openssl ec -in key.pem -pubout`)"
        );
    }
    let der = base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|err| anyhow::anyhow!("the public key PEM is not valid base64: {err}"))?;
    if der.len() != P256_SPKI_PREFIX.len() + 65
        || der[..P256_SPKI_PREFIX.len()] != P256_SPKI_PREFIX
        || der[P256_SPKI_PREFIX.len()] != 0x04
    {
        anyhow::bail!("the public key is not a P-256 (prime256v1) key; exec tickets are ES256");
    }
    jsonwebtoken::DecodingKey::from_ec_pem(pem.as_bytes())
        .map_err(|err| anyhow::anyhow!("unusable public key: {err}"))?;
    Ok(())
}

/// A new ticket id (`ext_` and 24 random characters).
pub fn new_exec_ticket_id() -> String {
    format!("ext_{}", crate::util::random_string(24).to_lowercase())
}

/// The `sub` of a ticket minted by `identity`: its token's accessor, or its
/// principal when the credential has no accessor (a JWT auth method, or a
/// cluster with ACLs off).
pub fn subject_for(accessor_id: Option<&str>, principal: &str) -> String {
    match accessor_id {
        Some(accessor) => format!("accessor:{accessor}"),
        None => format!("principal:{principal}"),
    }
}

impl ExecTicketClaims {
    /// The Legion claims as a JSON map, for signing.
    pub fn to_claims(&self) -> Map<String, Value> {
        match serde_json::to_value(self) {
            Ok(Value::Object(map)) => map,
            _ => Map::new(),
        }
    }

    /// Parses the claims out of a verified token's payload.
    pub fn from_claims(claims: &Map<String, Value>) -> anyhow::Result<Self> {
        serde_json::from_value(Value::Object(claims.clone()))
            .map_err(|err| anyhow::anyhow!("exec ticket claims are malformed: {err}"))
    }

    /// The minting token's accessor id, when a token (not a delegated key)
    /// minted it.
    pub fn accessor(&self) -> Option<&str> {
        self.sub.strip_prefix("accessor:")
    }

    /// Whether this ticket authorizes a session with `argv` and `tty`.
    pub fn allows(&self, argv: &[String], tty: bool) -> Result<(), String> {
        if tty != self.tty {
            return Err(format!(
                "the ticket was minted for tty = {}, not tty = {tty}",
                self.tty
            ));
        }
        match &self.argv {
            Some(expected) if expected.as_slice() != argv => Err(format!(
                "the ticket names the command {expected:?}, not {argv:?}"
            )),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod exec_ticket_test {
    use super::*;

    fn claims() -> ExecTicketClaims {
        ExecTicketClaims {
            sub: "accessor:7c1f".into(),
            jti: "ext_1".into(),
            iat: 100,
            exp: 160,
            node: "w-17".into(),
            job: "agents".into(),
            task: "workspace".into(),
            allocation_id: 88213,
            instance: Some("user-42".into()),
            argv: Some(vec!["/bin/bash".into()]),
            action: None,
            tty: true,
            max_session: 3600,
            uses: 1,
        }
    }

    #[test]
    fn claims_round_trip_with_their_legion_names_test() {
        let map = claims().to_claims();
        assert_eq!(map["legion.io/node"], "w-17");
        assert_eq!(map["legion.io/alloc"], 88213);
        assert_eq!(ExecTicketClaims::from_claims(&map).unwrap(), claims());
        assert_eq!(claims().accessor(), Some("7c1f"));
    }

    #[test]
    fn only_p256_public_keys_are_accepted_test() {
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        validate_p256_public_key(&key.public_key_pem()).unwrap();
        let p384 = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P384_SHA384).unwrap();
        assert!(validate_p256_public_key(&p384.public_key_pem()).is_err());
        // A private key is not a public key.
        assert!(validate_p256_public_key(&key.serialize_pem()).is_err());
        assert!(validate_p256_public_key("not a key").is_err());
    }

    #[test]
    fn a_ticket_allows_only_its_command_and_tty_test() {
        let ticket = claims();
        assert!(ticket.allows(&["/bin/bash".into()], true).is_ok());
        assert!(ticket.allows(&["/bin/sh".into()], true).is_err());
        assert!(ticket.allows(&["/bin/bash".into()], false).is_err());
        let any = ExecTicketClaims {
            argv: None,
            ..claims()
        };
        assert!(any.allows(&["anything".into()], true).is_ok());
    }
}
