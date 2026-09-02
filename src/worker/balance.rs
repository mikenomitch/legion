//! Load-balancing strategies for the node-local upstream forwarders.
//!
//! Every connection a task makes to `LEGION_UPSTREAM_<SERVICE>` lands on a
//! loopback listener owned by [`crate::worker::forwarder`], which asks a
//! [`Balancer`] which of the service's healthy endpoints to dial. Strategies
//! are stateless with respect to the endpoint list — it is handed to `pick` on
//! every call because the manager refreshes it every heartbeat — but may keep
//! their own cursor/RNG state, so each listener owns its own instance.
//!
//! # Adding a strategy
//!
//! Two steps, because `BALANCERS` is the *only* list of strategy names in the
//! tree — config validation reads it too, so there is no second place to forget.
//!
//! 1. Write the impl here:
//!
//!    ```ignore
//!    #[derive(Debug)]
//!    struct FirstHealthy;
//!
//!    impl Balancer for FirstHealthy {
//!        fn name(&self) -> &'static str { "first_healthy" }
//!        fn pick(&self, endpoints: &[Endpoint], _inflight: &HashMap<String, usize>) -> Option<usize> {
//!            (!endpoints.is_empty()).then_some(0)
//!        }
//!    }
//!    ```
//!
//! 2. Add one row to `BALANCERS`:
//!    `("first_healthy", || Arc::new(FirstHealthy))`.
//!
//! That is it: `legion job run` accepts `balance: "first_healthy"` immediately,
//! and the error message for unknown names lists it.

use crate::prelude::*;
use crate::structs::normalize_balance;

use rand::Rng;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Strategy used when an upstream names none. Re-exported from the config
/// types so the datapath and the validator can never drift apart.
pub use crate::structs::DEFAULT_BALANCE;

/// One dialable backend of an upstream service, as reported by the manager in
/// `HeartbeatReply.upstream_endpoints`. `addr` is the `host:port` the
/// forwarder dials; `allocation_id`/`node_id` identify the backing allocation
/// for diagnostics (and for future locality-aware strategies).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub addr: String,
    pub allocation_id: i64,
    pub node_id: String,
    /// The backing service's tags (`service.tags`), for weighted splits by
    /// tag in HTTP mode. Empty when the feed carried none.
    pub tags: Vec<String>,
}

/// Picks which endpoint a new connection goes to.
///
/// Implementations must be cheap and non-blocking: `pick` is called on the
/// accept path, once per dial attempt. `Debug` is required so a listener's
/// strategy (and any state it keeps) can be logged.
pub trait Balancer: std::fmt::Debug + Send + Sync {
    /// Canonical strategy name — this impl's key in `BALANCERS`.
    fn name(&self) -> &'static str;

    /// Pick an index into `endpoints`, or `None` when nothing is dialable.
    /// `inflight` maps endpoint address -> current in-flight connection count
    /// through this listener, for load-aware strategies; strategies that do not
    /// care simply ignore it.
    fn pick(&self, endpoints: &[Endpoint], inflight: &HashMap<String, usize>) -> Option<usize>;
}

/// One registry row: a canonical strategy name (already normalized) and the
/// constructor that builds a fresh instance of it.
type BalancerRow = (&'static str, fn() -> Arc<dyn Balancer>);

/// The strategy registry.
///
/// This is the single source of truth. [`balancer`] dispatches through it and
/// [`balancer_names`] — which config validation
/// (`crate::structs::is_known_balance`) and its error message read — is derived
/// from it, so a name and an implementation cannot get out of sync: there is
/// only one list.
const BALANCERS: &[BalancerRow] = &[
    ("round_robin", default_balancer),
    ("random", || Arc::new(Random)),
    ("least_conn", || Arc::new(LeastConn::default())),
];

/// The strategy names config validation accepts, in canonical form. Read
/// straight off `BALANCERS`, so it always describes what this build can
/// actually run.
pub fn balancer_names() -> Vec<&'static str> {
    BALANCERS.iter().map(|(name, _)| *name).collect()
}

/// Build the balancer for `name` (case/dash insensitive). Each call returns a
/// fresh instance so per-listener cursors stay independent.
///
/// Submitted configs are validated up front, so an unknown name here means a
/// stored allocation predates the strategy being removed — the caller should
/// fall back to [`DEFAULT_BALANCE`] rather than drop the upstream.
pub fn balancer(name: &str) -> R<Arc<dyn Balancer>> {
    let normalized = normalize_balance(name);
    match BALANCERS
        .iter()
        .find(|(known, _)| *known == normalized.as_str())
    {
        Some((_, build)) => Ok(build()),
        None => bail!(
            "unknown balance strategy '{name}'; valid strategies are {}",
            balancer_names().join(", ")
        ),
    }
}

/// The [`DEFAULT_BALANCE`] strategy, infallibly. Used when an allocation names
/// a strategy this build does not have: falling back beats dropping the
/// upstream, and the caller cannot handle an error usefully anyway.
pub fn default_balancer() -> Arc<dyn Balancer> {
    Arc::new(RoundRobin::default())
}

/// Even spread: hand out endpoints in order, wrapping around. The cursor is
/// shared by every connection on one listener, so two concurrent accepts still
/// land on different endpoints.
#[derive(Debug, Default)]
struct RoundRobin {
    cursor: AtomicUsize,
}

impl Balancer for RoundRobin {
    fn name(&self) -> &'static str {
        "round_robin"
    }

    fn pick(&self, endpoints: &[Endpoint], _inflight: &HashMap<String, usize>) -> Option<usize> {
        if endpoints.is_empty() {
            return None;
        }
        // Relaxed is enough: the cursor only has to be unique-ish, never
        // ordered against other memory.
        Some(self.cursor.fetch_add(1, Ordering::Relaxed) % endpoints.len())
    }
}

/// Uniform random choice. Stateless, and the best default when many clients
/// share one backend set (no synchronized cursors).
#[derive(Debug)]
struct Random;

impl Balancer for Random {
    fn name(&self) -> &'static str {
        "random"
    }

    fn pick(&self, endpoints: &[Endpoint], _inflight: &HashMap<String, usize>) -> Option<usize> {
        if endpoints.is_empty() {
            return None;
        }
        Some(rand::thread_rng().gen_range(0..endpoints.len()))
    }
}

/// Fewest in-flight connections wins; ties are broken round-robin so a cold
/// backend set still spreads evenly. Counts are per-listener, which is the
/// honest scope — a worker only knows about the connections it forwards.
#[derive(Debug, Default)]
struct LeastConn {
    cursor: AtomicUsize,
}

impl Balancer for LeastConn {
    fn name(&self) -> &'static str {
        "least_conn"
    }

    fn pick(&self, endpoints: &[Endpoint], inflight: &HashMap<String, usize>) -> Option<usize> {
        if endpoints.is_empty() {
            return None;
        }
        let load = |endpoint: &Endpoint| inflight.get(&endpoint.addr).copied().unwrap_or(0);
        let least = endpoints.iter().map(load).min().unwrap_or(0);
        let tied: Vec<usize> = endpoints
            .iter()
            .enumerate()
            .filter(|(_, endpoint)| load(endpoint) == least)
            .map(|(index, _)| index)
            .collect();
        // `least` came from this list, so at least one endpoint matches it.
        let offset = self.cursor.fetch_add(1, Ordering::Relaxed) % tied.len().max(1);
        tied.get(offset).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(addr: &str) -> Endpoint {
        Endpoint {
            addr: addr.to_string(),
            allocation_id: 1,
            node_id: "node-a".into(),
            tags: Vec::new(),
        }
    }

    fn endpoints() -> Vec<Endpoint> {
        vec![endpoint("10.0.0.1:80"), endpoint("10.0.0.2:80")]
    }

    #[test]
    fn registry_rows_are_canonical_and_self_consistent() -> R<()> {
        // The registry is the only strategy list, so what it has to guarantee
        // is that each row is internally coherent: the key is already
        // normalized (so `balancer()` can look it up after normalizing user
        // input), unique, and matches the impl's own `name()`.
        let mut seen = std::collections::HashSet::new();
        for name in balancer_names() {
            assert_eq!(
                normalize_balance(name),
                name,
                "registry keys must be written in canonical form"
            );
            assert!(seen.insert(name), "duplicate registry key {name}");
            assert_eq!(
                balancer(name)?.name(),
                name,
                "registry returned a balancer whose name does not match its key"
            );
            // …and validation accepts exactly these names.
            assert!(crate::structs::is_known_balance(name));
        }
        assert!(balancer_names().contains(&DEFAULT_BALANCE));
        assert_eq!(default_balancer().name(), DEFAULT_BALANCE);
        Ok(())
    }

    #[test]
    fn a_name_outside_the_registry_is_rejected_everywhere() {
        // The reverse direction: a strategy that is not a registry row must be
        // unbuildable *and* unsubmittable, so a half-added strategy fails loudly
        // rather than being accepted at submit and dropped at runtime.
        assert!(balancer("first_healthy").is_err());
        assert!(!crate::structs::is_known_balance("first_healthy"));
    }

    #[test]
    fn unknown_strategy_errors_and_lists_valid_names() {
        let err = balancer("magic").unwrap_err().to_string();
        assert!(err.contains("unknown balance strategy 'magic'"), "{err}");
        for name in balancer_names() {
            assert!(err.contains(name), "{err} is missing {name}");
        }
    }

    #[test]
    fn strategy_names_are_normalized_before_lookup() -> R<()> {
        assert_eq!(balancer(" Least-Conn ")?.name(), "least_conn");
        assert_eq!(balancer("ROUND_ROBIN")?.name(), "round_robin");
        Ok(())
    }

    #[test]
    fn round_robin_cycles_through_endpoints() -> R<()> {
        let balancer = balancer("round_robin")?;
        let endpoints = endpoints();
        let inflight = HashMap::new();
        let picks: Vec<usize> = (0..5)
            .filter_map(|_| balancer.pick(&endpoints, &inflight))
            .collect();
        assert_eq!(picks, vec![0, 1, 0, 1, 0]);
        Ok(())
    }

    #[test]
    fn every_strategy_returns_none_for_an_empty_endpoint_list() -> R<()> {
        let inflight = HashMap::new();
        for name in balancer_names() {
            assert_eq!(
                balancer(name)?.pick(&[], &inflight),
                None,
                "{name} must not pick from an empty list"
            );
        }
        Ok(())
    }

    #[test]
    fn random_stays_in_range_and_reaches_both_endpoints() -> R<()> {
        let balancer = balancer("random")?;
        let endpoints = endpoints();
        let inflight = HashMap::new();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            let index = balancer.pick(&endpoints, &inflight).expect("pick");
            assert!(index < endpoints.len());
            seen.insert(index);
        }
        // 2^-200 chance of a false failure.
        assert_eq!(seen.len(), 2, "random must reach every endpoint");
        Ok(())
    }

    #[test]
    fn least_conn_prefers_the_idle_endpoint() -> R<()> {
        let balancer = balancer("least_conn")?;
        let endpoints = endpoints();
        let inflight: HashMap<String, usize> = [("10.0.0.1:80".to_string(), 3)].into();
        for _ in 0..5 {
            assert_eq!(balancer.pick(&endpoints, &inflight), Some(1));
        }
        Ok(())
    }

    #[test]
    fn least_conn_breaks_ties_round_robin() -> R<()> {
        let balancer = balancer("least_conn")?;
        let endpoints = endpoints();
        // Equal load (including "no entry at all") must not pin one endpoint.
        let inflight: HashMap<String, usize> = [
            ("10.0.0.1:80".to_string(), 2),
            ("10.0.0.2:80".to_string(), 2),
        ]
        .into();
        let picks: Vec<usize> = (0..4)
            .filter_map(|_| balancer.pick(&endpoints, &inflight))
            .collect();
        assert_eq!(picks, vec![0, 1, 0, 1]);
        Ok(())
    }
}
