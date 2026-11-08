//! Topology-aware gang placement (docs/gang-scheduling.md, "Topology";
//! architecture review AI7).
//!
//! A gang with `topology = { key, mode }` places inside one **domain**: the
//! eligible nodes that share one value of the node attribute `key`. The
//! planner tries whole domains, one at a time, and keeps the first in which
//! the gang reaches its quorum; this module decides which domains exist,
//! roughly how many members each can hold, and the order to try them in.
//! The placement itself is the ordinary per-replica placement, restricted to
//! the domain's nodes, so every other rule (constraints, ports, `distinct`,
//! preemption) applies unchanged inside it.

use std::collections::HashMap;

use serde_json::Value;

use crate::constraints::{node_attr, value_token};
use crate::resources::{ResourcePool, ResourceRequest, ResourceValue};
use crate::storage::store::SharedNodePools;
use crate::structs::{Node, TopologyMode};

/// Domains one gang may try per plan. Each try is a full placement of the
/// gang inside the domain, so this bounds planning cost on a cluster with
/// hundreds of racks; the ordering puts the domains most likely to fit
/// first.
pub(super) const MAX_TOPOLOGY_TRIALS: usize = 8;

/// One domain: the eligible nodes sharing one value of the topology key.
#[derive(Debug, Clone)]
pub(super) struct TopologyDomain<'b> {
    /// The value's canonical token ([`value_token`]), for identity.
    pub(super) token: String,
    /// The value as a person reads it (`r12`, not `"r12"`).
    pub(super) label: String,
    pub(super) nodes: Vec<&'b Node>,
    /// Upper estimate of how many replicas the domain can hold now: free
    /// capacity over the request, per node (each capped at what the plan
    /// needs), summed. Ignores anti-affinity, ports, and `distinct`, which
    /// the real placement inside the domain still checks.
    pub(super) estimate: u32,
}

/// The value of `key` on `node` as a person reads it.
pub(super) fn domain_label(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => value_token(other),
    }
}

/// The domains among `eligible`, ordered by token. Nodes without `key` are
/// in no domain.
pub(super) fn domains<'b>(
    eligible: &[&'b Node],
    key: &str,
    pools: &SharedNodePools,
    request: &ResourceRequest,
    cap: u32,
) -> Vec<TopologyDomain<'b>> {
    let mut by_token: HashMap<String, TopologyDomain<'b>> = HashMap::new();
    for node in eligible {
        let Some(value) = node_attr(node, key).filter(|value| !value.is_null()) else {
            continue;
        };
        let token = value_token(value);
        let fit = pools
            .get(node.id.as_str())
            .map_or(0, |pool| replicas_fit(pool, request, cap));
        let domain = by_token
            .entry(token.clone())
            .or_insert_with(|| TopologyDomain {
                token,
                label: domain_label(value),
                nodes: Vec::new(),
                estimate: 0,
            });
        domain.nodes.push(node);
        // Summed uncapped: best fit needs to tell a domain that holds
        // exactly the gang from one that holds three of it.
        domain.estimate = domain.estimate.saturating_add(fit);
    }
    let mut domains: Vec<TopologyDomain<'b>> = by_token.into_values().collect();
    domains.sort_by(|left, right| left.token.cmp(&right.token));
    domains
}

/// How many replicas of `request` fit in `pool`, at most `cap`: the
/// smallest `free / requested` over the amounts it asks for. A request for
/// specific items (device ids) fits once or not at all.
pub(super) fn replicas_fit(pool: &ResourcePool, request: &ResourceRequest, cap: u32) -> u32 {
    let mut fit = cap as f64;
    for (name, demand) in &request.values {
        match demand {
            ResourceValue::Amount(amount) if *amount > 0.0 => match pool.values.get(name) {
                Some(ResourceValue::Amount(free)) if *free > 0.0 => {
                    fit = fit.min((free / amount).floor());
                }
                _ => return 0,
            },
            ResourceValue::Amount(_) => {}
            ResourceValue::Items(items) => match pool.values.get(name) {
                Some(ResourceValue::Items(free)) if items.is_subset(free) => fit = fit.min(1.0),
                _ => return 0,
            },
        }
    }
    if fit.is_finite() {
        fit.max(0.0) as u32
    } else {
        cap
    }
}

/// The order to try domains in, at most [`MAX_TOPOLOGY_TRIALS`] of them.
///
/// - `pinned` (the domain most of a running gang's members occupy) comes
///   first; under `required` it is the only domain a top-up may use.
/// - Then domains whose estimate holds every replica the plan places
///   (`needed`), smallest first: best fit, which keeps large domains whole
///   for large gangs.
/// - Then domains that hold the quorum (`need_now`), largest first.
/// - Then the rest, largest first: they can only fit by preemption.
pub(super) fn trial_order(
    domains: &[TopologyDomain<'_>],
    needed: u32,
    need_now: u32,
    pinned: Option<&str>,
    mode: TopologyMode,
) -> Vec<usize> {
    let pinned_index = pinned.and_then(|token| {
        domains
            .iter()
            .position(|domain| domain.token.as_str() == token)
    });
    if pinned.is_some() && mode == TopologyMode::Required {
        return pinned_index.into_iter().collect();
    }
    let mut whole: Vec<usize> = Vec::new();
    let mut quorum: Vec<usize> = Vec::new();
    let mut rest: Vec<usize> = Vec::new();
    for (index, domain) in domains.iter().enumerate() {
        if Some(index) == pinned_index {
            continue;
        }
        if domain.estimate >= needed.max(1) {
            whole.push(index);
        } else if domain.estimate >= need_now.max(1) {
            quorum.push(index);
        } else {
            rest.push(index);
        }
    }
    // Ties fall back to token order, which `domains` already sorted by.
    whole.sort_by_key(|index| domains[*index].estimate);
    quorum.sort_by_key(|index| std::cmp::Reverse(domains[*index].estimate));
    rest.sort_by_key(|index| std::cmp::Reverse(domains[*index].estimate));
    pinned_index
        .into_iter()
        .chain(whole)
        .chain(quorum)
        .chain(rest)
        .take(MAX_TOPOLOGY_TRIALS)
        .collect()
}

/// A `preferred` gang's fallback when no single domain fits the quorum: the
/// nodes of the fewest domains whose estimates together reach `need_now`,
/// largest first, and how many domains that is. `None` when even every
/// domain together does not reach it (the last fallback, any eligible node,
/// is all that is left) or when one domain already would.
pub(super) fn fewest_domains<'b>(
    domains: &[TopologyDomain<'b>],
    need_now: u32,
) -> Option<(Vec<&'b Node>, usize)> {
    let mut order: Vec<&TopologyDomain<'b>> = domains.iter().collect();
    order.sort_by_key(|domain| std::cmp::Reverse(domain.estimate));
    let mut total = 0u32;
    let mut nodes = Vec::new();
    for (taken, domain) in order.into_iter().enumerate() {
        total = total.saturating_add(domain.estimate);
        nodes.extend(domain.nodes.iter().copied());
        if total >= need_now.max(1) {
            return (taken > 0).then_some((nodes, taken + 1));
        }
    }
    None
}

/// What topology did for one gang in one plan, for its [`super::GangReport`].
#[derive(Debug, Clone, Default)]
pub(super) struct TopologyOutcome {
    /// The domain the gang placed in, or for one that did not place, the
    /// domain whose try got furthest. Empty when a `preferred` gang spans
    /// domains.
    pub(super) domain: String,
    /// Domains with at least one eligible node.
    pub(super) domains: u32,
}

#[cfg(test)]
mod topology_test {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    fn node(id: &str, rack: Option<&str>) -> Node {
        let mut attrs = json!({});
        if let Some(rack) = rack {
            attrs["rack"] = json!(rack);
        }
        Node {
            id: id.into(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs: attrs,
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    fn pool(memory: f64) -> Arc<ResourcePool> {
        Arc::new(ResourcePool {
            values: HashMap::from([("memory".to_string(), ResourceValue::Amount(memory))]),
            capacity: Default::default(),
        })
    }

    fn request(memory: f64) -> ResourceRequest {
        ResourceRequest {
            values: HashMap::from([("memory".to_string(), ResourceValue::Amount(memory))]),
        }
    }

    #[test]
    fn domains_group_by_attribute_and_estimate_capacity() {
        let nodes = [
            node("a1", Some("a")),
            node("a2", Some("a")),
            node("b1", Some("b")),
            node("x", None),
        ];
        let refs: Vec<&Node> = nodes.iter().collect();
        let pools: SharedNodePools = [
            ("a1".to_string(), pool(3000.0)),
            ("a2".to_string(), pool(1000.0)),
            ("b1".to_string(), pool(8000.0)),
            ("x".to_string(), pool(8000.0)),
        ]
        .into_iter()
        .collect();
        let found = domains(&refs, "rack", &pools, &request(1000.0), 6);
        assert_eq!(found.len(), 2, "the node without the key is in no domain");
        assert_eq!(found[0].label, "a");
        assert_eq!(found[0].nodes.len(), 2);
        assert_eq!(found[0].estimate, 4);
        assert_eq!(found[1].label, "b");
        assert_eq!(
            found[1].estimate, 6,
            "a node's share is capped at what the plan needs"
        );
    }

    fn domain(token: &str, estimate: u32) -> TopologyDomain<'static> {
        TopologyDomain {
            token: token.into(),
            label: token.into(),
            nodes: Vec::new(),
            estimate,
        }
    }

    #[test]
    fn whole_fits_go_first_smallest_first_then_quorum_fits_largest_first() {
        let found = [
            domain("a", 9),
            domain("b", 4),
            domain("c", 8),
            domain("d", 6),
            domain("e", 1),
        ];
        let order = trial_order(&found, 8, 6, None, TopologyMode::Required);
        let labels: Vec<&str> = order.iter().map(|i| found[*i].label.as_str()).collect();
        assert_eq!(labels, vec!["c", "a", "d", "b", "e"]);
    }

    #[test]
    fn a_required_top_up_stays_in_its_domain() {
        let found = [domain("a", 9), domain("b", 9)];
        assert_eq!(
            trial_order(&found, 2, 2, Some("b"), TopologyMode::Required),
            vec![1]
        );
        assert!(
            trial_order(&found, 2, 2, Some("gone"), TopologyMode::Required).is_empty(),
            "a domain with no eligible node left cannot be replaced elsewhere"
        );
        assert_eq!(
            trial_order(&found, 2, 2, Some("b"), TopologyMode::Preferred),
            vec![1, 0]
        );
    }

    #[test]
    fn trials_are_bounded() {
        let found: Vec<TopologyDomain> = (0..20).map(|i| domain(&format!("d{i:02}"), 9)).collect();
        assert_eq!(
            trial_order(&found, 8, 8, None, TopologyMode::Required).len(),
            MAX_TOPOLOGY_TRIALS
        );
    }

    #[test]
    fn the_fewest_domains_that_hold_the_quorum() {
        let found = [domain("a", 3), domain("b", 5), domain("c", 2)];
        let (_, count) = fewest_domains(&found, 7).expect("b and a hold 8");
        assert_eq!(count, 2);
        assert!(
            fewest_domains(&found, 5).is_none(),
            "one domain already would"
        );
        assert!(fewest_domains(&found, 11).is_none(), "not even all of them");
    }

    #[test]
    fn replica_estimates() {
        let free = pool(4096.0);
        assert_eq!(replicas_fit(&free, &request(1024.0), 10), 4);
        assert_eq!(replicas_fit(&free, &request(1024.0), 2), 2);
        assert_eq!(replicas_fit(&free, &request(8192.0), 10), 0);
        let cpu = ResourceRequest {
            values: HashMap::from([("cpu".to_string(), ResourceValue::Amount(100.0))]),
        };
        assert_eq!(
            replicas_fit(&free, &cpu, 10),
            0,
            "a dim the node lacks fits nothing"
        );
    }
}
