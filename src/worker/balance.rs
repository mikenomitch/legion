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
use std::time::{Duration, Instant};

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
    /// The endpoint terminates mesh TLS: dial it with the node certificate
    /// and name the consuming workload in a PROXY v2 TLV.
    pub mesh_tls: bool,
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

    /// Learn from one dial or request through `addr`. The datapath reports
    /// every outcome it sees; strategies that do not learn ignore it.
    fn observe(&self, _addr: &str, _outcome: Outcome) {}
}

/// What the datapath saw on one endpoint, reported through
/// [`Balancer::observe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The endpoint answered. On L4 this is the time to connect, and then
    /// the time from the first request byte to the first reply byte. In
    /// HTTP mode it is the time to the response head.
    Latency(Duration),
    /// The dial failed or timed out, the connection reset before a reply,
    /// or (HTTP mode) the endpoint answered 5xx.
    Failure,
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
    ("least_latency", default_balancer),
    ("round_robin", || Arc::new(RoundRobin::seeded())),
    ("least_conn", || Arc::new(LeastConn::seeded())),
    ("local_first", || Arc::new(LocalFirst::default())),
];

/// This node's id, for `local_first`. Set once by the worker at startup.
static LOCAL_NODE: std::sync::OnceLock<String> = std::sync::OnceLock::new();

pub fn set_local_node(node_id: &str) {
    let _ = LOCAL_NODE.set(node_id.to_string());
}

pub fn local_node() -> Option<&'static str> {
    LOCAL_NODE.get().map(String::as_str)
}

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
    Arc::new(LeastLatency::default())
}

/// How fast an idle latency estimate fades. Upward moves are immediate (a
/// slow reply raises the estimate at once); downward ones follow this time
/// constant, so one fast reply does not erase a run of slow ones.
const LATENCY_DECAY: Duration = Duration::from_secs(10);

/// What a failure costs in the estimate. Large enough that a failing
/// endpoint loses every pairing with a healthy one, and it decays like any
/// other sample, so the endpoint is retried once it has cooled down.
const FAILURE_PENALTY: Duration = Duration::from_secs(1);

/// The estimate given to endpoints before anything is known about any of
/// them. Only the ratio between endpoints matters, so the value is nominal.
const NOMINAL_LATENCY: Duration = Duration::from_millis(1);

/// Latency-aware: power of two choices over a peak-EWMA latency estimate,
/// weighted by in-flight load (Finagle's and Linkerd's balancer).
///
/// Each pick samples two endpoints at random and takes the one with the
/// lower `latency × (in-flight + 1)`. Sampling two instead of scanning all
/// keeps a slow endpoint from being starved forever and keeps concurrent
/// picks from herding onto one "best" endpoint. An endpoint with no samples
/// yet starts at the mean of the others, so a new backend gets a fair
/// share instead of a flood.
///
/// Estimates decay toward zero while an endpoint is not picked, so one that
/// was slow or failing is tried again after a few multiples of
/// [`LATENCY_DECAY`].
#[derive(Debug, Default)]
struct LeastLatency {
    estimates: Mutex<HashMap<String, PeakEwma>>,
}

/// One endpoint's latency estimate in nanoseconds, as of `at`.
#[derive(Debug, Clone, Copy)]
struct PeakEwma {
    nanos: f64,
    at: Instant,
}

impl PeakEwma {
    /// The estimate at `now`, decayed for the time since the last sample.
    fn at(&self, now: Instant) -> f64 {
        self.nanos * decay_weight(now.saturating_duration_since(self.at))
    }
}

fn decay_weight(elapsed: Duration) -> f64 {
    (-elapsed.as_secs_f64() / LATENCY_DECAY.as_secs_f64()).exp()
}

impl Balancer for LeastLatency {
    fn name(&self) -> &'static str {
        "least_latency"
    }

    fn pick(&self, endpoints: &[Endpoint], inflight: &HashMap<String, usize>) -> Option<usize> {
        match endpoints.len() {
            0 => return None,
            1 => return Some(0),
            _ => {}
        }
        let mut rng = rand::thread_rng();
        let first = rng.gen_range(0..endpoints.len());
        // A second, distinct endpoint: an offset in 1..len from the first.
        let second = (first + rng.gen_range(1..endpoints.len())) % endpoints.len();

        let now = Instant::now();
        let mut estimates = self.estimates.lock().unwrap_or_else(|p| p.into_inner());
        // Endpoints come and go with dynamic ports; keep the map bounded by
        // what this listener can still dial.
        if estimates.len() > endpoints.len() * 2 + 16 {
            estimates.retain(|addr, _| endpoints.iter().any(|e| e.addr == *addr));
        }
        let known: Vec<f64> = endpoints
            .iter()
            .filter_map(|e| estimates.get(&e.addr).map(|s| s.at(now)))
            .collect();
        let fallback = if known.is_empty() {
            NOMINAL_LATENCY.as_nanos() as f64
        } else {
            known.iter().sum::<f64>() / known.len() as f64
        };
        let cost = |index: usize| {
            let endpoint = &endpoints[index];
            let latency = estimates
                .get(&endpoint.addr)
                .map_or(fallback, |s| s.at(now))
                // An estimate decayed to nothing still has to count load.
                .max(1.0);
            let load = inflight.get(&endpoint.addr).copied().unwrap_or(0);
            latency * (load + 1) as f64
        };
        // `first` and `second` are in random order, so ties break randomly.
        Some(if cost(second) < cost(first) {
            second
        } else {
            first
        })
    }

    fn observe(&self, addr: &str, outcome: Outcome) {
        let now = Instant::now();
        let sample = match outcome {
            Outcome::Latency(latency) => latency.as_nanos() as f64,
            Outcome::Failure => FAILURE_PENALTY.as_nanos() as f64,
        };
        let mut estimates = self.estimates.lock().unwrap_or_else(|p| p.into_inner());
        let estimate = estimates.entry(addr.to_string()).or_insert(PeakEwma {
            nanos: sample,
            at: now,
        });
        let current = estimate.at(now);
        estimate.nanos = if sample > current {
            // Peak: a slow reply counts in full, immediately.
            sample
        } else {
            let weight = decay_weight(now.saturating_duration_since(estimate.at));
            current * weight + sample * (1.0 - weight)
        };
        estimate.at = now;
    }
}

/// Even spread: hand out endpoints in order, wrapping around. The cursor is
/// shared by every connection on one listener, so two concurrent accepts still
/// land on different endpoints.
#[derive(Debug, Default)]
struct RoundRobin {
    cursor: AtomicUsize,
}

/// A cursor starting at a random offset, so many listeners built at once
/// (every allocation of a job, say) do not all send their first connection
/// to the same endpoint.
fn seeded_cursor() -> AtomicUsize {
    AtomicUsize::new(rand::thread_rng().gen_range(0..1024))
}

impl RoundRobin {
    fn seeded() -> Self {
        RoundRobin {
            cursor: seeded_cursor(),
        }
    }
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

/// Locality first: round-robin over the endpoints on this node, widening to
/// every endpoint when no healthy one is local. Fail-open on purpose — a
/// caller never waits for a local backend that is not there.
#[derive(Debug, Default)]
struct LocalFirst {
    cursor: AtomicUsize,
}

impl Balancer for LocalFirst {
    fn name(&self) -> &'static str {
        "local_first"
    }

    fn pick(&self, endpoints: &[Endpoint], _inflight: &HashMap<String, usize>) -> Option<usize> {
        if endpoints.is_empty() {
            return None;
        }
        let local: Vec<usize> = match local_node() {
            Some(node) => endpoints
                .iter()
                .enumerate()
                .filter(|(_, e)| e.node_id == node)
                .map(|(i, _)| i)
                .collect(),
            None => Vec::new(),
        };
        let turn = self.cursor.fetch_add(1, Ordering::Relaxed);
        if local.is_empty() {
            Some(turn % endpoints.len())
        } else {
            Some(local[turn % local.len()])
        }
    }
}

/// Fewest in-flight connections wins; ties are broken round-robin so a cold
/// backend set still spreads evenly. Counts are per-listener, which is the
/// honest scope — a worker only knows about the connections it forwards.
#[derive(Debug, Default)]
struct LeastConn {
    cursor: AtomicUsize,
}

impl LeastConn {
    fn seeded() -> Self {
        LeastConn {
            cursor: seeded_cursor(),
        }
    }
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

    /// Fresh `round_robin` listeners over one endpoint list spread their
    /// first picks instead of all starting at index 0.
    #[test]
    fn fresh_round_robin_listeners_spread_their_first_picks_test() -> R<()> {
        let endpoints = endpoints();
        let inflight = HashMap::new();
        let mut first = std::collections::HashSet::new();
        for _ in 0..64 {
            first.insert(balancer("round_robin")?.pick(&endpoints, &inflight));
        }
        // 2^-63 chance of a false failure.
        assert_eq!(first.len(), 2, "first picks reach every endpoint");
        Ok(())
    }

    #[test]
    fn local_first_prefers_this_node_and_widens_when_nothing_is_local() {
        set_local_node("node-a");
        let endpoints = vec![
            Endpoint {
                addr: "10.0.0.1:1".into(),
                allocation_id: 1,
                node_id: "node-b".into(),
                tags: Vec::new(),
                mesh_tls: false,
            },
            Endpoint {
                addr: "10.0.0.2:1".into(),
                allocation_id: 2,
                node_id: "node-a".into(),
                tags: Vec::new(),
                mesh_tls: false,
            },
            Endpoint {
                addr: "10.0.0.3:1".into(),
                allocation_id: 3,
                node_id: "node-a".into(),
                tags: Vec::new(),
                mesh_tls: false,
            },
        ];
        let strategy = balancer("local_first").unwrap();
        let picks: Vec<usize> = (0..4)
            .map(|_| strategy.pick(&endpoints, &HashMap::new()).unwrap())
            .collect();
        assert!(picks.iter().all(|i| *i == 1 || *i == 2), "{picks:?}");
        assert!(
            picks.contains(&1) && picks.contains(&2),
            "round-robins the local pair: {picks:?}"
        );
        let remote_only = vec![endpoints[0].clone()];
        assert_eq!(
            strategy.pick(&remote_only, &HashMap::new()),
            Some(0),
            "widens when nothing is local"
        );
        assert!(balancer_names().contains(&"local_first"));
    }

    fn endpoint(addr: &str) -> Endpoint {
        Endpoint {
            addr: addr.to_string(),
            allocation_id: 1,
            node_id: "node-a".into(),
            tags: Vec::new(),
            mesh_tls: false,
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
        // In order, wrapping; the start is random per listener.
        assert!(
            picks == vec![0, 1, 0, 1, 0] || picks == vec![1, 0, 1, 0, 1],
            "{picks:?}"
        );
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
    fn least_latency_sheds_traffic_from_a_slow_endpoint() -> R<()> {
        let balancer = balancer("least_latency")?;
        let endpoints = vec![
            endpoint("10.0.0.1:80"),
            endpoint("10.0.0.2:80"),
            endpoint("10.0.0.3:80"),
        ];
        balancer.observe("10.0.0.1:80", Outcome::Latency(Duration::from_millis(1)));
        balancer.observe("10.0.0.2:80", Outcome::Latency(Duration::from_millis(1)));
        balancer.observe("10.0.0.3:80", Outcome::Latency(Duration::from_millis(50)));
        let inflight = HashMap::new();
        let slow = (0..300)
            .filter(|_| balancer.pick(&endpoints, &inflight) == Some(2))
            .count();
        // Every pairing that includes the slow endpoint goes to the other
        // one, so it is never picked while the others stay fast.
        assert_eq!(slow, 0, "the slow endpoint kept getting picks");
        Ok(())
    }

    #[test]
    fn least_latency_counts_load() -> R<()> {
        let balancer = balancer("least_latency")?;
        let endpoints = endpoints();
        for endpoint in &endpoints {
            balancer.observe(&endpoint.addr, Outcome::Latency(Duration::from_millis(1)));
        }
        // Equal latency, one endpoint busy: the idle one wins every pairing.
        let busy: HashMap<String, usize> = [("10.0.0.1:80".to_string(), 4)].into();
        for _ in 0..20 {
            assert_eq!(balancer.pick(&endpoints, &busy), Some(1));
        }
        Ok(())
    }

    #[test]
    fn least_latency_penalizes_failures_and_estimates_rise_at_once() {
        let strategy = LeastLatency::default();
        strategy.observe("a", Outcome::Latency(Duration::from_millis(1)));
        strategy.observe("a", Outcome::Latency(Duration::from_millis(40)));
        let now = Instant::now();
        let after_peak = strategy.estimates.lock().unwrap()["a"].at(now);
        assert!(
            after_peak >= 39e6,
            "a slow reply must count in full: {after_peak}"
        );
        strategy.observe("b", Outcome::Failure);
        let failed = strategy.estimates.lock().unwrap()["b"].at(now);
        assert!(failed >= 0.99e9, "a failure costs the penalty: {failed}");
    }

    #[test]
    fn decay_brings_an_old_estimate_back_toward_zero() {
        let then = Instant::now();
        let estimate = PeakEwma {
            nanos: 1e9,
            at: then,
        };
        let later = then + LATENCY_DECAY * 5;
        assert!(estimate.at(later) < 1e9 * 0.01, "{}", estimate.at(later));
    }

    #[test]
    fn least_latency_handles_one_endpoint_and_a_cold_start() -> R<()> {
        let balancer = balancer("least_latency")?;
        assert_eq!(
            balancer.pick(&[endpoint("10.0.0.1:80")], &HashMap::new()),
            Some(0)
        );
        // Nothing observed yet: every endpoint ties, and ties break
        // randomly, so both are reached.
        let endpoints = endpoints();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            seen.insert(balancer.pick(&endpoints, &HashMap::new()).expect("pick"));
        }
        assert_eq!(seen.len(), 2);
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
        // Ties alternate; where the cursor starts is random per listener.
        assert!(
            picks == vec![0, 1, 0, 1] || picks == vec![1, 0, 1, 0],
            "{picks:?}"
        );
        Ok(())
    }
}
