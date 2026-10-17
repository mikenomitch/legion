//! Resolve, judge, then dial the address that was judged
//! (worker-plane-auth plan §6.11, calls entry 14).
//!
//! Every connection the datapath dials on an allocation's behalf to a
//! destination the allocation named (the egress proxy's three paths, external
//! upstreams, and `proxy` handler targets) goes through
//! [`resolve_and_guard`]: the name is resolved once, every address is judged
//! against the host guard (`crate::structs::host_guard`), and only addresses
//! that passed are returned for the dial. Nothing resolves the name again
//! between the verdict and the connect, which closes the hostname,
//! `localhost`, decimal-literal (`2130706433`), and DNS-rebinding bypasses.
//!
//! Service upstreams are not judged here: their endpoints come from the
//! service catalog, which Legion itself fills, not from the allocation.

use crate::structs::host_guard::{GuardDenial, HostGuard};
use std::net::{IpAddr, SocketAddr};

/// Name resolution for the datapath. Behind a trait so tests can answer
/// names (and change their answers) without DNS.
#[tonic::async_trait]
pub trait Resolver: Send + Sync {
    async fn resolve(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>>;
}

/// The system resolver (`getaddrinfo` through tokio). An IP literal is used
/// as is; anything else, including forms `str::parse` rejects such as
/// `2130706433` or `0177.0.0.1`, goes to `getaddrinfo`, which is exactly
/// how the dial would have read it.
pub struct SystemResolver;

#[tonic::async_trait]
impl Resolver for SystemResolver {
    async fn resolve(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        if let Ok(ip) = bare.parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        Ok(tokio::net::lookup_host((bare, port)).await?.collect())
    }
}

/// Why nothing may be dialed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardRefusal {
    /// The host guard refused every address the name resolved to. The
    /// reason names the first one.
    Denied(String),
    /// The name did not resolve. Not a policy verdict: the caller answers it
    /// like a failed dial.
    Unresolved(String),
}

/// Resolves `host`, judges every address against `guard`, and returns the
/// addresses that passed, in resolution order. With no guard (a context
/// built without one, as unit tests and the pre-policy path do) every
/// address passes.
pub async fn resolve_and_guard(
    resolver: &dyn Resolver,
    guard: Option<&HostGuard>,
    host: &str,
    port: u16,
) -> Result<Vec<SocketAddr>, GuardRefusal> {
    let resolved = resolver
        .resolve(host, port)
        .await
        .map_err(|err| GuardRefusal::Unresolved(format!("{host}: {err}")))?;
    if resolved.is_empty() {
        return Err(GuardRefusal::Unresolved(format!(
            "{host} resolved to no address"
        )));
    }
    let Some(guard) = guard else {
        return Ok(resolved);
    };
    let mut first_denial: Option<GuardDenial> = None;
    let mut passed = Vec::with_capacity(resolved.len());
    for addr in resolved {
        match guard.judge(addr.ip(), addr.port()) {
            Ok(()) => passed.push(addr),
            Err(denial) => {
                first_denial.get_or_insert(denial);
            }
        }
    }
    if passed.is_empty() {
        let denial = first_denial.expect("every address was denied");
        return Err(GuardRefusal::Denied(denial.reason(Some(host))));
    }
    Ok(passed)
}

/// Splits `host:port` (or `[v6]:port`) text. `None` without a port.
pub fn split_target(target: &str) -> Option<(String, u16)> {
    let target = target.trim();
    if let Some(rest) = target.strip_prefix('[') {
        let (host, tail) = rest.split_once(']')?;
        let port = tail.strip_prefix(':')?.parse().ok()?;
        return Some((host.to_string(), port));
    }
    let (host, port) = target.rsplit_once(':')?;
    if host.contains(':') {
        return None;
    }
    Some((host.to_string(), port.parse().ok()?))
}

/// Dials a `host:port` the allocation's policy named (a `proxy` handler's
/// target) through the host guard: resolved once, judged, and only an
/// address that passed is dialed. The error is the text a handler-health
/// report shows.
pub async fn dial_guarded(
    context: &crate::worker::forwarder::ForwarderContext,
    target: &str,
    timeout: std::time::Duration,
) -> Result<tokio::net::TcpStream, String> {
    let (host, port) = split_target(target).ok_or_else(|| format!("{target} is not host:port"))?;
    let addrs = resolve_and_guard(context.resolver(), context.host_guard(), &host, port)
        .await
        .map_err(|refusal| match refusal {
            GuardRefusal::Denied(reason) => format!(
                "{}: {reason}",
                crate::structs::host_guard::HOST_GUARD_SOURCE
            ),
            GuardRefusal::Unresolved(reason) => reason,
        })?;
    crate::worker::netd::listen::dial_tcp(&addrs[..], timeout)
        .await
        .map_err(|err| err.to_string())
}

/// `host:port` text for an address list, for denial records.
pub fn display_addrs(addrs: &[SocketAddr]) -> String {
    addrs
        .iter()
        .map(SocketAddr::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
pub mod test_support {
    //! A resolver tests program by name, and that counts its calls.
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    pub struct FakeResolver {
        /// Answers per name, consumed front to back; the last one repeats.
        answers: Mutex<HashMap<String, Vec<Vec<IpAddr>>>>,
        pub calls: Mutex<Vec<String>>,
    }

    impl FakeResolver {
        pub fn new() -> Arc<Self> {
            Arc::new(FakeResolver::default())
        }

        pub fn answer(&self, name: &str, answers: Vec<Vec<IpAddr>>) {
            self.answers
                .lock()
                .unwrap()
                .insert(name.to_string(), answers);
        }

        pub fn call_count(&self, name: &str) -> usize {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|call| *call == name)
                .count()
        }
    }

    #[tonic::async_trait]
    impl Resolver for FakeResolver {
        async fn resolve(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
            self.calls.lock().unwrap().push(host.to_string());
            let programmed = {
                let mut answers = self.answers.lock().unwrap();
                answers.get_mut(host).map(|queue| {
                    if queue.len() > 1 {
                        queue.remove(0)
                    } else {
                        queue.first().cloned().unwrap_or_default()
                    }
                })
            };
            match programmed {
                Some(next) => Ok(next
                    .into_iter()
                    .map(|ip| SocketAddr::new(ip, port))
                    .collect()),
                None => SystemResolver.resolve(host, port).await,
            }
        }
    }
}

#[cfg(test)]
mod guard_test {
    use super::test_support::FakeResolver;
    use super::*;
    use crate::structs::host_guard::HostGuardConfig;

    fn guard() -> HostGuard {
        HostGuard::new(&HostGuardConfig::default()).expect("guard")
    }

    #[tokio::test]
    async fn names_are_judged_by_the_address_they_resolve_to_test() {
        let resolver = FakeResolver::new();
        resolver.answer(
            "metadata.test",
            vec![vec!["169.254.169.254".parse().unwrap()]],
        );
        let guard = guard();
        for name in [
            "localhost",
            "metadata.test",
            "2130706433",
            "0177.0.0.1",
            "127.1",
        ] {
            let refused = resolve_and_guard(resolver.as_ref(), Some(&guard), name, 80).await;
            assert!(
                matches!(refused, Err(GuardRefusal::Denied(_))),
                "{name} must be refused, got {refused:?}"
            );
        }
        let Err(GuardRefusal::Denied(reason)) =
            resolve_and_guard(resolver.as_ref(), Some(&guard), "metadata.test", 80).await
        else {
            panic!("metadata.test refused");
        };
        assert_eq!(
            reason,
            "metadata.test resolves to 169.254.169.254 (link-local); allocations may not reach it"
        );
    }

    #[tokio::test]
    async fn the_name_is_resolved_once_and_only_judged_addresses_return_test() {
        let resolver = FakeResolver::new();
        resolver.answer(
            "mixed.test",
            vec![vec![
                "127.0.0.1".parse().unwrap(),
                "93.184.216.34".parse().unwrap(),
            ]],
        );
        let passed = resolve_and_guard(resolver.as_ref(), Some(&guard()), "mixed.test", 443)
            .await
            .expect("one address passes");
        assert_eq!(passed, vec!["93.184.216.34:443".parse().unwrap()]);
        assert_eq!(resolver.call_count("mixed.test"), 1);
    }

    #[tokio::test]
    async fn an_unresolvable_name_is_not_a_policy_denial_test() {
        let resolver = FakeResolver::new();
        resolver.answer("empty.test", vec![vec![]]);
        assert!(matches!(
            resolve_and_guard(resolver.as_ref(), Some(&guard()), "empty.test", 80).await,
            Err(GuardRefusal::Unresolved(_))
        ));
    }
}
