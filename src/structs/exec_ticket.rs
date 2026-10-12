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
