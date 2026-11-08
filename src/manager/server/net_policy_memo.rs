//! Memoized network-policy assignments (mesh-control-path plan §6.2, review
//! NET2; docs/network-handlers.md, "Policy objects").
//!
//! An allocation's effective policy is a pure function of the stored
//! policies (named by their generation), the selector facts (node pool when
//! a policy selects on pools, runtime, job, labels from the task's
//! `network`), the task's own `network` and `service` declarations, and,
//! for an allocation whose placement says `gated`, its placed network. The
//! memo keys on exactly those, so a heartbeat costs one hash and one
//! comparison per allocation: the stored policies are listed once per
//! generation, and each distinct input is parsed and merged once.
//!
//! **Keys are verified, not trusted.** A 64-bit hash picks the bucket and
//! the full inputs are compared before a result is reused: a job author
//! writes `network` and `service`, and a crafted hash collision must not
//! hand one task another's policy.

use super::*;
use crate::storage::endpoint_index::StableHash;
use crate::structs::netpolicy::{EffectivePolicy, NetPolicyObject, NetSelectorFacts};

/// Results remembered. Cleared wholesale when full (and on every policy
/// write, which moves the generation).
const MEMO_MAX: usize = 16_384;

/// The inputs one result was computed from.
#[derive(Debug)]
struct Inputs {
    node_pool: Option<String>,
    runtime: String,
    job: String,
    task_network: String,
    task_service: String,
    /// The placed network, only when it may say `gated`.
    placed: String,
}

impl Inputs {
    fn matches(&self, other: &InputsRef<'_>) -> bool {
        self.node_pool.as_deref() == other.node_pool
            && self.runtime == other.runtime
            && self.job == other.job
            && self.task_network == other.task_network
            && self.task_service == other.task_service
            && self.placed == other.placed
    }
}

#[derive(Debug, Clone, Copy)]
struct InputsRef<'a> {
    node_pool: Option<&'a str>,
    runtime: &'a str,
    job: &'a str,
    task_network: &'a str,
    task_service: &'a str,
    placed: &'a str,
}

impl<'a> InputsRef<'a> {
    fn of(allocation: &'a crate::structs::TaskAllocation, node_pool: Option<&'a str>) -> Self {
        let placed = if allocation
            .network
            .contains(crate::structs::netpolicy::MODE_GATED)
        {
            allocation.network.as_str()
        } else {
            ""
        };
        InputsRef {
            node_pool,
            runtime: &allocation.task_runtime,
            job: &allocation.job_id,
            task_network: &allocation.task_network,
            task_service: &allocation.task_service,
            placed,
        }
    }

    fn hash(&self) -> u64 {
        StableHash::default()
            .str(self.node_pool.unwrap_or("\0none"))
            .str(self.runtime)
            .str(self.job)
            .str(self.task_network)
            .str(self.task_service)
            .str(self.placed)
            .finish()
    }

    fn owned(&self) -> Inputs {
        Inputs {
            node_pool: self.node_pool.map(str::to_string),
            runtime: self.runtime.to_string(),
            job: self.job.to_string(),
            task_network: self.task_network.to_string(),
            task_service: self.task_service.to_string(),
            placed: self.placed.to_string(),
        }
    }
}

/// `None`: the allocation has no policy to assign.
type Assignment = Option<Arc<EffectivePolicy>>;

#[derive(Debug, Default)]
struct MemoInner {
    /// The generation `stored` and `results` belong to.
    generation: Option<u64>,
    stored: Arc<Vec<NetPolicyObject>>,
    /// A stored policy selects on node pools.
    needs_pool: bool,
    results: HashMap<u64, Vec<(Inputs, Assignment)>>,
    entries: usize,
    /// Merges performed (tests).
    merges: usize,
}

/// See the module docs.
#[derive(Debug, Default)]
pub(crate) struct NetPolicyMemo {
    inner: Mutex<MemoInner>,
}

impl NetPolicyMemo {
    fn lock(&self) -> std::sync::MutexGuard<'_, MemoInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The stored policies for `generation`, listed once per generation.
    fn stored(
        &self,
        generation: u64,
        list: impl FnOnce() -> R<Vec<NetPolicyObject>>,
    ) -> R<(Arc<Vec<NetPolicyObject>>, bool)> {
        {
            let inner = self.lock();
            if inner.generation == Some(generation) {
                return Ok((Arc::clone(&inner.stored), inner.needs_pool));
            }
        }
        let stored = Arc::new(list()?);
        let needs_pool = stored.iter().any(|p| !p.selector.node_pools.is_empty());
        let mut inner = self.lock();
        *inner = MemoInner {
            generation: Some(generation),
            stored: Arc::clone(&stored),
            needs_pool,
            merges: inner.merges,
            ..Default::default()
        };
        Ok((stored, needs_pool))
    }

    fn get(&self, generation: u64, hash: u64, inputs: &InputsRef<'_>) -> Option<Assignment> {
        let inner = self.lock();
        if inner.generation != Some(generation) {
            return None;
        }
        inner
            .results
            .get(&hash)?
            .iter()
            .find(|(held, _)| held.matches(inputs))
            .map(|(_, assignment)| assignment.clone())
    }

    fn put(&self, generation: u64, hash: u64, inputs: &InputsRef<'_>, assignment: Assignment) {
        let mut inner = self.lock();
        if inner.generation != Some(generation) {
            return;
        }
        inner.merges += 1;
        if inner.entries >= MEMO_MAX {
            inner.results.clear();
            inner.entries = 0;
        }
        inner
            .results
            .entry(hash)
            .or_default()
            .push((inputs.owned(), assignment));
        inner.entries += 1;
    }

    /// Merges performed so far (tests).
    #[cfg(test)]
    pub(crate) fn merges(&self) -> usize {
        self.lock().merges
    }
}

/// One allocation's assignment, computed: `None` when it has no policy and
/// is not gated, or its task network does not parse.
fn assignment(
    allocation: &crate::structs::TaskAllocation,
    stored: &[NetPolicyObject],
    node_pool: Option<&str>,
) -> Assignment {
    let placed_gated = allocation
        .network
        .contains(crate::structs::netpolicy::MODE_GATED)
        && serde_json::from_str::<crate::structs::AllocNetwork>(&allocation.network)
            .is_ok_and(|placed| placed.is_gated());
    let network: Option<crate::structs::NetworkConfig> =
        if allocation.task_network.trim().is_empty() {
            None
        } else {
            match serde_json::from_str(&allocation.task_network) {
                Ok(network) => Some(network),
                Err(err) => {
                    warn!(
                        allocation_id = allocation.allocation_id,
                        error = ?err,
                        "Skipping net policy; invalid task network payload"
                    );
                    return None;
                }
            }
        };
    let facts = NetSelectorFacts {
        node_pool: node_pool.map(str::to_string),
        runtime: allocation.task_runtime.clone(),
        job: allocation.job_id.clone(),
        labels: network
            .as_ref()
            .map(|n| n.labels.clone())
            .unwrap_or_default(),
    };
    let service: Option<crate::structs::ServiceConfig> =
        (!allocation.task_service.trim().is_empty())
            .then(|| serde_json::from_str(&allocation.task_service).ok())
            .flatten();
    let (effective, _) = merge_net_policies(stored, &facts, network.as_ref(), service.as_ref());
    // A gated allocation is told about an *empty* policy too, so the node
    // can tell "known: nothing covers it" (`gated-default`) from "pending"
    // (`policy-pending`). Gated by its declaration, by a `require` policy,
    // or by placement resolving `auto` (a default sandbox, gated plan
    // Phase 2), which the allocation's own network records.
    let gated = placed_gated
        || network
            .as_ref()
            .is_some_and(|n| n.mode == crate::structs::netpolicy::MODE_GATED)
        || effective.mode.as_deref() == Some(crate::structs::netpolicy::MODE_GATED);
    if effective.is_empty() && !gated {
        return None;
    }
    Some(Arc::new(effective))
}

impl ManagerDaemonService {
    /// [`Self::build_net_policies`] through the memo.
    pub(super) fn memoized_net_policies(
        &self,
        node_id: &str,
        desired: &[crate::structs::TaskAllocation],
    ) -> Vec<AllocNetPolicy> {
        // Gated by placement, which can resolve `auto` with no `network`
        // block on the task at all (a default sandbox, gated plan Phase 2):
        // the allocation's own network records it.
        let placed_gated = |allocation: &crate::structs::TaskAllocation| {
            allocation
                .network
                .contains(crate::structs::netpolicy::MODE_GATED)
                && serde_json::from_str::<crate::structs::AllocNetwork>(&allocation.network)
                    .is_ok_and(|placed| placed.is_gated())
        };
        let generation = self.net_policy_generation_cached();
        let any_inline = desired
            .iter()
            .any(|allocation| !allocation.task_network.trim().is_empty());
        if generation == 0 && !any_inline && !desired.iter().any(placed_gated) {
            return Vec::new();
        }
        let listed = self.net_policy_memo.stored(generation, || {
            Ok(self
                .store
                .list_net_policies()?
                .into_iter()
                .map(|record| record.object)
                .collect())
        });
        let (stored, needs_pool) = match listed {
            Ok(listed) => listed,
            Err(err) => {
                warn!(node_id = %node_id, error = ?err, "Skipping net policies; listing failed");
                return Vec::new();
            }
        };
        // The node's pool once, and only when a policy could select on it.
        let node_pool = if needs_pool {
            self.store.get_node(node_id).ok().map(|node| node.pool)
        } else {
            None
        };
        let mut out = Vec::new();
        for allocation in desired {
            let inputs = InputsRef::of(allocation, node_pool.as_deref());
            let hash = inputs.hash();
            let result = match self.net_policy_memo.get(generation, hash, &inputs) {
                Some(result) => result,
                None => {
                    let result = assignment(allocation, &stored, node_pool.as_deref());
                    self.net_policy_memo
                        .put(generation, hash, &inputs, result.clone());
                    result
                }
            };
            let Some(effective) = result else {
                continue;
            };
            // `FetchDatapathMaterial` serves bodies from the cache, which is
            // bounded and may have dropped this one since it was merged.
            let cached = self
                .net_policy_cache
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .contains_key(&effective.hash);
            if !cached {
                self.remember_net_policy(&effective);
            }
            out.push(AllocNetPolicy {
                allocation_id: allocation.allocation_id,
                policy_hash: effective.hash.clone(),
            });
        }
        out
    }
}
