//! Node-side policy cache and denial ledger.
//!
//! The manager ships `[(allocation, hash)]` on every heartbeat reply. This
//! cache keeps the bodies by hash (one per distinct merged policy on the
//! node, not one per allocation), remembers which allocation runs which hash,
//! and hands listeners a cheap [`PolicyLookup`] to evaluate against. Bodies
//! that a fetch could not retrieve keep whatever the allocation had before —
//! the same fail-static contract the endpoint feed follows — and an
//! allocation the reply no longer lists loses its policy on that reply.

use crate::prelude::*;
use crate::structs::netpolicy::{evaluate_connect, ConnectFacts, EffectivePolicy, Verdict};
use crate::structs::{AllocNetPolicy, NetDenial};

use std::collections::{BTreeMap, HashSet};
use std::sync::RwLock;

/// Cap on distinct denial rows carried per heartbeat; the rest fold into one
/// `truncated` row so a flood cannot bloat the heartbeat.
pub const MAX_DENIALS_PER_HEARTBEAT: usize = 256;

/// Static facts about an allocation the datapath attributes traffic to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AllocIdentity {
    pub job_id: String,
    pub task_name: String,
    /// The task's group — part of the workload identity subject.
    pub group: String,
    pub labels: BTreeMap<String, String>,
    /// The allocation's assigned ports by label (`AllocPort::host_port`),
    /// so a `proxy` handler can name a sidecar's `port` label.
    pub ports: BTreeMap<String, u16>,
}

#[derive(Default)]
struct CacheState {
    /// allocation id → policy hash the manager last named for it.
    by_alloc: HashMap<i64, String>,
    /// hash → merged body.
    bodies: HashMap<String, Arc<EffectivePolicy>>,
    /// Attribution facts per allocation, from the desired-allocation payload.
    identities: HashMap<i64, AllocIdentity>,
}

/// The cache. One per worker process; shared with every listener.
#[derive(Default)]
pub struct NetPolicyCache {
    state: RwLock<CacheState>,
}

/// What a listener asks for: the effective policy of one allocation, or
/// `None` when it has none (and therefore runs unfiltered — the pre-policy
/// behavior).
pub trait PolicyLookup: Send + Sync {
    fn policy_for(&self, allocation_id: i64) -> Option<Arc<EffectivePolicy>>;
    fn identity_for(&self, allocation_id: i64) -> Option<AllocIdentity>;
}

impl NetPolicyCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the allocation → hash map from a successful heartbeat reply and
    /// return the hashes whose bodies are not cached yet, for the caller to
    /// fetch. Allocations not in `assignments` lose their policy.
    pub fn apply_assignments(&self, assignments: &[AllocNetPolicy]) -> Vec<String> {
        let mut state = self.state.write().unwrap_or_else(|p| p.into_inner());
        state.by_alloc.clear();
        let mut missing: HashSet<String> = HashSet::new();
        for assignment in assignments {
            if assignment.policy_hash.is_empty() {
                continue;
            }
            state
                .by_alloc
                .insert(assignment.allocation_id, assignment.policy_hash.clone());
            if !state.bodies.contains_key(&assignment.policy_hash) {
                missing.insert(assignment.policy_hash.clone());
            }
        }
        // Drop bodies no allocation references any more, so the cache is
        // bounded by the number of distinct policies in use on the node.
        let live: HashSet<String> = state.by_alloc.values().cloned().collect();
        state.bodies.retain(|hash, _| live.contains(hash));
        missing.into_iter().collect()
    }

    /// Store a fetched body. A body whose own hash disagrees with the key is
    /// rejected — the manager and worker must agree on the content address.
    pub fn insert_body(&self, hash: &str, body: EffectivePolicy) -> R<()> {
        if !body.hash.is_empty() && body.hash != hash {
            bail!(
                "policy body hash {} does not match requested hash {hash}",
                body.hash
            );
        }
        let mut state = self.state.write().unwrap_or_else(|p| p.into_inner());
        state.bodies.insert(hash.to_string(), Arc::new(body));
        Ok(())
    }

    /// Record attribution facts for the allocations the node runs.
    pub fn set_identities(&self, identities: HashMap<i64, AllocIdentity>) {
        let mut state = self.state.write().unwrap_or_else(|p| p.into_inner());
        state.identities = identities;
    }

    /// Every cached body by hash, for persistence across worker restarts.
    pub fn bodies(&self) -> Vec<(String, EffectivePolicy)> {
        self.state
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .bodies
            .iter()
            .map(|(hash, body)| (hash.clone(), (**body).clone()))
            .collect()
    }

    /// Every allocation's attribution facts (the desired set as of the last
    /// heartbeat).
    pub fn identities(&self) -> HashMap<i64, AllocIdentity> {
        self.state
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .identities
            .clone()
    }

    /// Whether the allocation's policy body is present (a gated allocation
    /// must not start until it is).
    pub fn is_ready(&self, allocation_id: i64) -> bool {
        let state = self.state.read().unwrap_or_else(|p| p.into_inner());
        match state.by_alloc.get(&allocation_id) {
            Some(hash) => state.bodies.contains_key(hash),
            None => true,
        }
    }

    /// Whether the manager has named a policy hash for this allocation at
    /// all (as opposed to "no policy applies"). Together with `is_ready`
    /// this distinguishes *pending* — a gated allocation must fail closed —
    /// from *known empty*.
    pub fn has_assignment(&self, allocation_id: i64) -> bool {
        let state = self.state.read().unwrap_or_else(|p| p.into_inner());
        state.by_alloc.contains_key(&allocation_id)
    }

    /// The hash the manager named for this allocation, if any.
    pub fn hash_for(&self, allocation_id: i64) -> Option<String> {
        let state = self.state.read().unwrap_or_else(|p| p.into_inner());
        state.by_alloc.get(&allocation_id).cloned()
    }

    /// Hashes currently assigned but not yet fetched.
    pub fn pending_hashes(&self) -> Vec<String> {
        let state = self.state.read().unwrap_or_else(|p| p.into_inner());
        let mut pending: Vec<String> = state
            .by_alloc
            .values()
            .filter(|hash| !state.bodies.contains_key(*hash))
            .cloned()
            .collect();
        pending.sort();
        pending.dedup();
        pending
    }

    /// Diagnostics: (allocations with a policy, distinct bodies cached).
    pub fn stats(&self) -> (usize, usize) {
        let state = self.state.read().unwrap_or_else(|p| p.into_inner());
        (state.by_alloc.len(), state.bodies.len())
    }
}

impl PolicyLookup for NetPolicyCache {
    fn policy_for(&self, allocation_id: i64) -> Option<Arc<EffectivePolicy>> {
        let state = self.state.read().unwrap_or_else(|p| p.into_inner());
        let hash = state.by_alloc.get(&allocation_id)?;
        state.bodies.get(hash).cloned()
    }

    fn identity_for(&self, allocation_id: i64) -> Option<AllocIdentity> {
        let state = self.state.read().unwrap_or_else(|p| p.into_inner());
        state.identities.get(&allocation_id).cloned()
    }
}

/// Evaluate an egress connection for an allocation. `None` policy means
/// allow (no chain declared anywhere).
pub fn egress_connect_verdict(
    policy: Option<&EffectivePolicy>,
    facts: &ConnectFacts<'_>,
) -> Verdict {
    match policy.and_then(|p| p.egress.as_ref()) {
        Some(chain) => evaluate_connect(chain, facts),
        None => Verdict::Allow,
    }
}

/// Evaluate an egress connection for a **gated** allocation: like
/// [`egress_connect_verdict`], except that an allocation no egress chain
/// covers is denied with [`SOURCE_GATED_DEFAULT`] instead of allowed. Gated
/// means nothing leaves unless a rule says so.
///
/// [`SOURCE_GATED_DEFAULT`]: crate::structs::netpolicy::SOURCE_GATED_DEFAULT
pub fn gated_egress_connect_verdict(
    policy: Option<&EffectivePolicy>,
    facts: &ConnectFacts<'_>,
) -> Verdict {
    match policy.and_then(|p| p.egress.as_ref()) {
        Some(chain) => evaluate_connect(chain, facts),
        None => {
            let destination = match (facts.host, facts.ip) {
                (Some(host), _) if facts.port > 0 => format!("{host}:{}", facts.port),
                (Some(host), _) => host.to_string(),
                (None, Some(ip)) if facts.port > 0 => format!("{ip}:{}", facts.port),
                (None, Some(ip)) => ip.to_string(),
                (None, None) => "this destination".to_string(),
            };
            Verdict::Deny {
                source: crate::structs::netpolicy::SOURCE_GATED_DEFAULT.to_string(),
                reason: Some(crate::structs::netpolicy::gated_default_reason(
                    &destination,
                )),
            }
        }
    }
}

/// Evaluate an ingress connection for an allocation.
pub fn ingress_connect_verdict(
    policy: Option<&EffectivePolicy>,
    facts: &ConnectFacts<'_>,
) -> Verdict {
    match policy.and_then(|p| p.ingress.as_ref()) {
        Some(chain) => evaluate_connect(chain, facts),
        None => Verdict::Allow,
    }
}

/// Key a denial folds under.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DenialKey {
    allocation_id: i64,
    direction: String,
    destination: String,
    source: String,
    protocol: String,
    method: String,
    path: String,
}

/// Denials recorded since the last heartbeat, folded by key.
#[derive(Default)]
pub struct Denials {
    pending: Mutex<HashMap<DenialKey, NetDenial>>,
}

impl Denials {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one denial. `identity` is the verified peer identity when any.
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        allocation_id: i64,
        who: Option<&AllocIdentity>,
        direction: &str,
        protocol: &str,
        destination: &str,
        source: &str,
        reason: Option<&str>,
        identity: Option<&str>,
        method: &str,
        path: &str,
    ) {
        self.record_count(
            allocation_id,
            who,
            direction,
            protocol,
            destination,
            source,
            reason,
            identity,
            method,
            path,
            1,
        );
    }

    /// [`Self::record`] for `count` identical denials at once: the kernel
    /// host guard reports packet counters, not one call per drop.
    #[allow(clippy::too_many_arguments)]
    pub fn record_count(
        &self,
        allocation_id: i64,
        who: Option<&AllocIdentity>,
        direction: &str,
        protocol: &str,
        destination: &str,
        source: &str,
        reason: Option<&str>,
        identity: Option<&str>,
        method: &str,
        path: &str,
        count: u32,
    ) {
        if count == 0 {
            return;
        }
        let key = DenialKey {
            allocation_id,
            direction: direction.to_string(),
            destination: destination.to_string(),
            source: source.to_string(),
            protocol: protocol.to_string(),
            method: method.to_string(),
            path: path.to_string(),
        };
        let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        if pending.len() >= MAX_DENIALS_PER_HEARTBEAT && !pending.contains_key(&key) {
            // Fold overflow into one row so the heartbeat stays bounded.
            let truncated = DenialKey {
                allocation_id: 0,
                direction: direction.to_string(),
                destination: "truncated".to_string(),
                source: "truncated".to_string(),
                protocol: String::new(),
                method: String::new(),
                path: String::new(),
            };
            let entry = pending.entry(truncated).or_insert_with(|| NetDenial {
                allocation_id: 0,
                direction: direction.to_string(),
                destination: "truncated".to_string(),
                source: "truncated".to_string(),
                ..Default::default()
            });
            entry.count = entry.count.saturating_add(count);
            return;
        }
        let entry = pending.entry(key).or_insert_with(|| NetDenial {
            allocation_id,
            job_id: who.map(|w| w.job_id.clone()).unwrap_or_default(),
            task_name: who.map(|w| w.task_name.clone()).unwrap_or_default(),
            direction: direction.to_string(),
            destination: destination.to_string(),
            source: source.to_string(),
            reason: reason.unwrap_or_default().to_string(),
            protocol: protocol.to_string(),
            identity: identity.unwrap_or_default().to_string(),
            count: 0,
            labels: who
                .map(|w| w.labels.clone().into_iter().collect())
                .unwrap_or_default(),
            method: method.to_string(),
            path: path.to_string(),
        });
        entry.count = entry.count.saturating_add(count);
    }

    /// Take everything recorded so far, for the next heartbeat.
    pub fn drain(&self) -> Vec<NetDenial> {
        let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        let mut out: Vec<NetDenial> = pending.drain().map(|(_, d)| d).collect();
        out.sort_by(|a, b| {
            a.allocation_id
                .cmp(&b.allocation_id)
                .then_with(|| a.destination.cmp(&b.destination))
        });
        out
    }

    /// Put denials back after a failed heartbeat so they are not lost.
    pub fn restore(&self, denials: Vec<NetDenial>) {
        let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        for denial in denials {
            let key = DenialKey {
                allocation_id: denial.allocation_id,
                direction: denial.direction.clone(),
                destination: denial.destination.clone(),
                source: denial.source.clone(),
                protocol: denial.protocol.clone(),
                method: denial.method.clone(),
                path: denial.path.clone(),
            };
            let count = denial.count;
            let entry = pending.entry(key).or_insert(denial);
            entry.count = entry.count.max(count);
        }
    }
}

/// Build the per-allocation attribution map from a desired-allocation
/// payload's `task_network` (labels) and identity fields.
pub fn identities_from_desired(
    desired: &[crate::structs::TaskAllocation],
) -> HashMap<i64, AllocIdentity> {
    let mut out = HashMap::new();
    for allocation in desired {
        let labels = if allocation.task_network.trim().is_empty() {
            BTreeMap::new()
        } else {
            serde_json::from_str::<crate::structs::NetworkConfig>(&allocation.task_network)
                .map(|network| network.labels)
                .unwrap_or_default()
        };
        let group = if allocation.task_group.is_empty() {
            crate::structs::default_task_group()
        } else {
            allocation.task_group.clone()
        };
        let ports = if allocation.network.trim().is_empty() {
            BTreeMap::new()
        } else {
            serde_json::from_str::<crate::structs::AllocNetwork>(&allocation.network)
                .map(|network| {
                    network
                        .ports
                        .iter()
                        .map(|port| (port.name.clone(), port.host_port))
                        .collect()
                })
                .unwrap_or_default()
        };
        out.insert(
            allocation.allocation_id,
            AllocIdentity {
                job_id: allocation.job_id.clone(),
                task_name: allocation.task_name.clone(),
                group,
                labels,
                ports,
            },
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::netpolicy::{merge_policies, InlinePolicy, NetPolicyObject};
    use serde_json::json;

    fn body(allow: &str) -> EffectivePolicy {
        let mut p: NetPolicyObject = serde_json::from_value(json!({
            "name": "p", "enforcement": "require", "egress": { "allow": [allow] }
        }))
        .unwrap();
        p.normalize_and_validate().unwrap();
        merge_policies(&[p], InlinePolicy::default())
    }

    #[test]
    fn assignments_report_missing_bodies_and_fail_static() {
        let cache = NetPolicyCache::new();
        let b = body("a.example");
        let missing = cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 1,
            policy_hash: b.hash.clone(),
        }]);
        assert_eq!(missing, vec![b.hash.clone()]);
        assert!(!cache.is_ready(1));
        assert!(cache.policy_for(1).is_none());

        cache.insert_body(&b.hash, b.clone()).unwrap();
        assert!(cache.is_ready(1));
        assert!(cache.pending_hashes().is_empty());
        let got = cache.policy_for(1).unwrap();
        let verdict = egress_connect_verdict(
            Some(&got),
            &ConnectFacts {
                host: Some("a.example"),
                port: 443,
                ..Default::default()
            },
        );
        assert_eq!(verdict, Verdict::Allow);
        let verdict = egress_connect_verdict(
            Some(&got),
            &ConnectFacts {
                host: Some("b.example"),
                port: 443,
                ..Default::default()
            },
        );
        assert!(matches!(verdict, Verdict::Deny { .. }));

        // Same hash again: nothing to fetch; a body no one references is dropped.
        let again = cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 1,
            policy_hash: b.hash.clone(),
        }]);
        assert!(again.is_empty());
        cache.apply_assignments(&[]);
        assert_eq!(cache.stats(), (0, 0));
        assert!(
            cache.policy_for(1).is_none(),
            "an allocation dropped from the reply loses its policy"
        );

        // A mismatched body is refused.
        let other = body("z.example");
        assert!(cache.insert_body("deadbeef", other).is_err());
    }

    #[test]
    fn denials_fold_by_key_and_bound_the_heartbeat() {
        let denials = Denials::new();
        let who = AllocIdentity {
            group: "default".into(),
            ports: BTreeMap::new(),
            job_id: "agent".into(),
            task_name: "sandbox".into(),
            labels: [("user".to_string(), "alice".to_string())]
                .into_iter()
                .collect(),
        };
        for _ in 0..3 {
            denials.record(
                7,
                Some(&who),
                "egress",
                "tcp",
                "evil.example:443",
                "policy/p#0",
                Some("nope"),
                None,
                "",
                "",
            );
        }
        denials.record(
            7,
            Some(&who),
            "egress",
            "tcp",
            "other.example:443",
            "fallthrough",
            None,
            None,
            "",
            "",
        );
        let drained = denials.drain();
        assert_eq!(drained.len(), 2);
        let evil = drained
            .iter()
            .find(|d| d.destination == "evil.example:443")
            .unwrap();
        assert_eq!(evil.count, 3);
        assert_eq!(evil.labels.get("user").map(String::as_str), Some("alice"));
        assert_eq!(evil.job_id, "agent");
        assert!(denials.drain().is_empty());

        for i in 0..(MAX_DENIALS_PER_HEARTBEAT + 5) {
            denials.record(
                1,
                None,
                "egress",
                "tcp",
                &format!("h{i}:1"),
                "fallthrough",
                None,
                None,
                "",
                "",
            );
        }
        let drained = denials.drain();
        assert_eq!(drained.len(), MAX_DENIALS_PER_HEARTBEAT + 1);
        let truncated = drained
            .iter()
            .find(|d| d.destination == "truncated")
            .unwrap();
        assert_eq!(truncated.count, 5);

        // A failed heartbeat puts them back.
        denials.record(
            2,
            None,
            "ingress",
            "tcp",
            "peer:1",
            "fallthrough",
            None,
            None,
            "",
            "",
        );
        let batch = denials.drain();
        denials.restore(batch);
        assert_eq!(denials.drain().len(), 1);
    }

    #[test]
    fn identities_come_from_task_network_labels() {
        let alloc = crate::structs::TaskAllocation {
            allocation_id: 3,
            job_id: "j".into(),
            task_name: "t".into(),
            task_network: json!({ "mode": "auto", "labels": { "tier": "free" } }).to_string(),
            ..Default::default()
        };
        let ids = identities_from_desired(&[alloc]);
        assert_eq!(ids[&3].labels.get("tier").map(String::as_str), Some("free"));
    }
}
