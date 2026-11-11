//! The egress judge: the checks every way out of an allocation runs, in one
//! order (docs/network-handlers.md, "Gated mode" and "The host guard").
//!
//! 1. A gated allocation fails closed: while its boot gate is shut, or its
//!    policy has not arrived, everything is refused ([`gate_block`]).
//! 2. A named destination is resolved once and every address is judged by
//!    the host guard. A guard refusal is final: no rule can allow it.
//! 3. The egress chain, once per resolved address with the name still
//!    attached, so a name rule matches the name and a CIDR rule the address.
//!
//! The gated proxy, capture, and an external upstream (L4 and HTTP mode)
//! call [`judge`] and dial only the addresses it returns, never the name
//! again (worker-plane-auth plan §6.11). A catalog upstream, whose endpoints
//! are addresses already, calls [`gate_block`] and then [`connect_verdict`]
//! per endpoint.

use crate::prelude::*;
use crate::structs::netpolicy::{ConnectFacts, EffectivePolicy, RequestFacts, Verdict};
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::guard::{self, GuardRefusal};
use crate::worker::netd::netns;
use crate::worker::netd::policy::{egress_connect_verdict, gated_egress_connect_verdict};

use std::net::SocketAddr;

/// Why a flow is refused: the verdict's source and reason, which a `403`, a
/// `net.denied` event, and `legion net policy test` carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denial {
    pub source: String,
    pub reason: Option<String>,
}

impl Denial {
    pub fn new(source: impl Into<String>, reason: Option<String>) -> Self {
        Self {
            source: source.into(),
            reason,
        }
    }
}

/// An HTTP request head as policy sees it: method, path, lower-cased headers.
pub type RequestHead<'a> = (&'a str, &'a str, &'a [(String, String)]);

/// A gated allocation's fail-closed check, ahead of any rule: its boot gate
/// is shut, or its policy (`policy`, already looked up) has not arrived. A
/// cooperative allocation is never blocked here.
pub fn gate_block(
    context: &ForwarderContext,
    allocation_id: i64,
    gated: bool,
    policy: Option<&EffectivePolicy>,
) -> Option<Denial> {
    if !gated {
        return None;
    }
    if netns::gate_is_shut(allocation_id) {
        return Some(Denial::new(
            "gate",
            Some("network gate is shut until the placement is ratified".into()),
        ));
    }
    if policy.is_none() && !context.policy_known(allocation_id) {
        return Some(Denial::new(
            "policy-pending",
            Some("network policy has not arrived yet".into()),
        ));
    }
    None
}

/// The connect-time verdict. With `default_deny` (a gated allocation's
/// proxy and capture), a flow no egress chain covers is denied
/// (`gated-default`); otherwise it is allowed. A declared upstream is an
/// explicit allowance, so it never takes the default deny.
pub fn connect_verdict(
    default_deny: bool,
    policy: Option<&EffectivePolicy>,
    facts: &ConnectFacts<'_>,
) -> Verdict {
    if default_deny {
        gated_egress_connect_verdict(policy, facts)
    } else {
        egress_connect_verdict(policy, facts)
    }
}

/// Judges one named destination and returns the addresses that may be
/// dialed, in the order of the module docs. `gated` turns on the
/// fail-closed checks; `declared` says the destination is a declared
/// upstream, which never takes the `gated-default` deny. `request` is the
/// head when the flow is an HTTP request under an HTTP chain; without it the
/// chain is evaluated at connect time.
///
/// A name that does not resolve still reads as a refusal when the policy
/// refuses it by name (more useful than a 502); allowed by name, it returns
/// no addresses, and the caller's dial fails, though a `proxy` handler may
/// still take the flow and resolve the name on its own side.
pub async fn judge(
    context: &ForwarderContext,
    allocation_id: i64,
    gated: bool,
    declared: bool,
    host: &str,
    port: u16,
    request: Option<RequestHead<'_>>,
) -> Result<Vec<SocketAddr>, Denial> {
    let default_deny = gated && !declared;
    if gated && netns::gate_is_shut(allocation_id) {
        return Err(
            gate_block(context, allocation_id, gated, None).expect("a shut gate always blocks")
        );
    }
    let resolved =
        guard::resolve_and_guard(context.resolver(), context.host_guard(), host, port).await;
    let policy = context.policy_for(allocation_id);
    let addrs = match resolved {
        Ok(addrs) => addrs,
        Err(GuardRefusal::Denied(reason)) => {
            return Err(Denial::new(
                crate::structs::host_guard::HOST_GUARD_SOURCE,
                Some(reason),
            ))
        }
        Err(GuardRefusal::Unresolved(reason)) => {
            debug!(allocation_id, %reason, "Egress destination did not resolve");
            if let Some(denial) = gate_block(context, allocation_id, gated, policy.as_deref()) {
                return Err(denial);
            }
            let facts = ConnectFacts {
                host: Some(host),
                ip: None,
                port,
                identity: None,
            };
            return match connect_verdict(default_deny, policy.as_deref(), &facts) {
                Verdict::Deny { source, reason } => Err(Denial::new(source, reason)),
                _ => Ok(Vec::new()),
            };
        }
    };
    if let Some(denial) = gate_block(context, allocation_id, gated, policy.as_deref()) {
        return Err(denial);
    }
    let mut first_denial: Option<Denial> = None;
    let mut allowed = Vec::with_capacity(addrs.len());
    for addr in addrs {
        let ip = Some(addr.ip());
        let verdict = match (policy.as_ref().and_then(|p| p.egress.as_ref()), request) {
            (Some(chain), Some((method, path, headers))) if chain.is_http() => {
                crate::structs::netpolicy::evaluate_request(
                    chain,
                    &RequestFacts {
                        host: Some(host),
                        ip,
                        port,
                        identity: None,
                        method,
                        path,
                        headers,
                    },
                )
            }
            _ => connect_verdict(
                default_deny,
                policy.as_deref(),
                &ConnectFacts {
                    host: Some(host),
                    ip,
                    port,
                    identity: None,
                },
            ),
        };
        match verdict {
            Verdict::Allow | Verdict::Defer => allowed.push(addr),
            Verdict::Deny { source, reason } => {
                first_denial.get_or_insert(Denial::new(source, reason));
            }
        }
    }
    if allowed.is_empty() {
        return Err(first_denial.unwrap_or_else(|| Denial::new("policy", None)));
    }
    Ok(allowed)
}
