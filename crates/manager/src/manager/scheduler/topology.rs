//! Topology-aware gang placement (docs/gang-scheduling.md, "Topology";
//! architecture review AI7).
//!
//! A gang with `topology = { keys, mode }` places inside one **domain**: the
//! eligible nodes that share one value of a node attribute. `keys` are
//! levels, innermost first (`["rack", "zone"]`): the planner tries the
//! domains of the first level, one at a time, and keeps the first in which
//! the gang reaches its quorum; only when none does does it try the next
//! level. This module decides which domains exist, roughly how many members
//! each can hold (with and without preemption), and the order to try them
//! in.
//! The placement itself is the ordinary per-replica placement, restricted to
//! the domain's nodes, so every other rule (constraints, ports, `distinct`,
//! preemption) applies unchanged inside it.

use std::collections::HashMap;

use serde_json::Value;

use crate::constraints::{node_attr, value_token};
use crate::resources::{ResourcePool, ResourceRequest, ResourceValue};
use crate::storage::store::SharedNodePools;
use crate::structs::{Node, TopologyMode};

/// Domains one gang may try per plan, across all its levels. Each try is a
/// full placement of the gang inside the domain, so this bounds planning
/// cost on a cluster with hundreds of racks. Only domains whose estimate
/// (counting what preemption could free) reaches the quorum are tried at
/// all, so the bound only bites when that many domains could plausibly fit
/// and every one of them turned out not to.
pub(super) const MAX_TOPOLOGY_TRIALS: usize = 16;

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
    /// The same estimate over free capacity plus what preemption could
    /// reclaim (allocations in a band below the gang's), an upper bound on
    /// what the domain could hold if the placement inside it preempts. Equal
    /// to `estimate` when the gang cannot preempt.
    pub(super) reachable: u32,
}

/// The value of `key` on `node` as a person reads it.
pub(super) fn domain_label(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => value_token(other),
    }
}

/// The domains among `eligible`, ordered by token. Nodes without `key` are
/// in no domain. `reclaim` is, per node, its free capacity plus what
/// preemption could free there; `None` when the gang cannot preempt.
pub(super) fn domains<'b>(
    eligible: &[&'b Node],
    key: &str,
    pools: &SharedNodePools,
    reclaim: Option<&HashMap<String, ResourcePool>>,
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
        let reachable = match reclaim.and_then(|reclaim| reclaim.get(node.id.as_str())) {
            Some(pool) => replicas_fit(pool, request, cap).max(fit),
            None => fit,
        };
        let domain = by_token
            .entry(token.clone())
            .or_insert_with(|| TopologyDomain {
                token,
                label: domain_label(value),
                nodes: Vec::new(),
                estimate: 0,
                reachable: 0,
            });
        domain.nodes.push(node);
        // Summed uncapped: best fit needs to tell a domain that holds
        // exactly the gang from one that holds three of it.
        domain.estimate = domain.estimate.saturating_add(fit);
        domain.reachable = domain.reachable.saturating_add(reachable);
    }
    let mut domains: Vec<TopologyDomain<'b>> = by_token.into_values().collect();
    domains.sort_by(|left, right| left.token.cmp(&right.token));
    domains
}

/// How many replicas of `request` fit in `pool`, at most `cap`: the
/// smallest `free / requested` over the amounts it asks for. A request for
/// specific items (device ids) fits once or not at all.
pub(crate) fn replicas_fit(pool: &ResourcePool, request: &ResourceRequest, cap: u32) -> u32 {
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

/// The order to try one level's domains in. The caller stops after
/// [`MAX_TOPOLOGY_TRIALS`] tries across every level.
///
/// - `pinned` (the domain most of a running gang's members occupy) comes
///   first; under `required` it is the only domain a top-up may use.
/// - Then domains whose estimate holds every replica the plan places
///   (`needed`), smallest first: best fit, which keeps large domains whole
///   for large gangs.
/// - Then domains that hold the quorum (`need_now`), largest first.
/// - Then domains that hold the quorum only with what preemption could
///   reclaim, largest reclaimable first.
/// - The rest cannot hold the quorum even by preempting everything below
///   the gang's band, and are never tried: this is what keeps the trial
///   bound from skipping a domain where preemption would have made room.
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
    let mut preempting: Vec<usize> = Vec::new();
    for (index, domain) in domains.iter().enumerate() {
        if Some(index) == pinned_index {
            continue;
        }
        if domain.estimate >= needed.max(1) {
            whole.push(index);
        } else if domain.estimate >= need_now.max(1) {
            quorum.push(index);
        } else if domain.reachable >= need_now.max(1) {
            preempting.push(index);
        }
    }
    // Ties fall back to token order, which `domains` already sorted by.
    whole.sort_by_key(|index| domains[*index].estimate);
    quorum.sort_by_key(|index| std::cmp::Reverse(domains[*index].estimate));
    preempting.sort_by_key(|index| std::cmp::Reverse(domains[*index].reachable));
    pinned_index
        .into_iter()
        .chain(whole)
        .chain(quorum)
        .chain(preempting)
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
    /// The level (`topology.keys` entry) `domain` belongs to; empty with no
    /// domain.
    pub(super) key: String,
    /// Domains with at least one eligible node, at that level (the first
    /// level when no domain was chosen).
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
            ..Default::default()
        })
    }

    fn request(memory: f64) -> ResourceRequest {
        ResourceRequest {
            values: HashMap::from([("memory".to_string(), ResourceValue::Amount(memory))]),
            devices: Vec::new(),
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
        let found = domains(&refs, "rack", &pools, None, &request(1000.0), 6);
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
            reachable: estimate,
        }
    }

    #[test]
    fn whole_fits_go_first_smallest_first_then_quorum_fits_largest_first() {
        let mut found = vec![
            domain("a", 9),
            domain("b", 4),
            domain("c", 8),
            domain("d", 6),
            domain("e", 1),
            domain("f", 2),
        ];
        // `f` holds the quorum only by preempting; `b` and `e` never do.
        found[5].reachable = 7;
        let order = trial_order(&found, 8, 6, None, TopologyMode::Required);
        let labels: Vec<&str> = order.iter().map(|i| found[*i].label.as_str()).collect();
        assert_eq!(labels, vec!["c", "a", "d", "f"]);
    }

    #[test]
    fn reclaimable_capacity_counts_toward_reachable_only() {
        let nodes = [node("a1", Some("a"))];
        let refs: Vec<&Node> = nodes.iter().collect();
        let pools: SharedNodePools = [("a1".to_string(), pool(1000.0))].into_iter().collect();
        let reclaim: HashMap<String, ResourcePool> = [("a1".to_string(), (*pool(4000.0)).clone())]
            .into_iter()
            .collect();
        let found = domains(&refs, "rack", &pools, Some(&reclaim), &request(1000.0), 8);
        assert_eq!(found[0].estimate, 1);
        assert_eq!(found[0].reachable, 4);
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
            devices: Vec::new(),
        };
        assert_eq!(
            replicas_fit(&free, &cpu, 10),
            0,
            "a dim the node lacks fits nothing"
        );
    }
}
