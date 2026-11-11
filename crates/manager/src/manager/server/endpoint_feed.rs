//! A node's endpoint feed (mesh-control-path plan §6.1, review NET1;
//! docs/service-discovery.md, "How endpoints reach a node").
//!
//! Two halves, both cheap on a steady beat:
//!
//! - **Bindings**: which endpoint set each listener on the node uses, as
//!   (allocation, upstream key, handler?) → (service, consumer). Built from
//!   memoized parses of each allocation's `task_connect` and of each policy
//!   body's `proxy` handlers, so a beat hashes a payload instead of parsing
//!   it. Sent only when their digest differs from the worker's.
//! - **Sets**: one per distinct (service, consumer) on the node, read from
//!   the store's [`crate::storage::endpoint_index::EndpointIndex`]. Sent
//!   only when their digest differs from the worker's.
//!
//! The same sets are pushed on the node's work stream when the index sees a
//! consumed service change ([`endpoint_push`], [`run_endpoint_refresher`]),
//! so a backend going down does not wait for the node's next heartbeat.
//!
//! **Never fatal.** A payload that does not parse, or a set the store cannot
//! read, is left out: the worker keeps what it held (fail-static), and the
//! node's reconciliation never stops over a connect-layer problem.

use super::*;
use crate::storage::endpoint_index::{SetKey, StableHash};
use crate::structs::{
    EndpointBinding, EndpointFeed, EndpointFeedState, EndpointSet, StreamWorkReply,
};
use std::collections::BTreeSet;

/// Distinct parsed payloads remembered. Cleared wholesale when full; each
/// entry is one JSON parse to rebuild.
const NEEDS_MEMO_MAX: usize = 8_192;

/// How long the refresher waits after a wake before re-reading, so a burst
/// of commits (a rollout, a node loss) becomes one re-read per service.
const REFRESH_DEBOUNCE: std::time::Duration = std::time::Duration::from_millis(25);

/// How often the refresher sweeps idle services and departed nodes.
const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// What one allocation's `task_connect` asks of the feed: its service
/// upstreams as (key, canonical service), and the `proxy` handler services
/// its upstreams name.
#[derive(Debug, Default)]
struct ConnectNeeds {
    upstreams: Vec<(String, String)>,
    handler_services: Vec<String>,
}

/// A parse and the (consuming job, payload) it was parsed from. The memo is
/// bucketed by a 64-bit hash and every hit compares the full inputs: a job
/// author writes the payload, and a crafted collision must not hand one
/// allocation another's upstreams.
type ParsedConnect = (String, String, Arc<ConnectNeeds>);

static CONNECT_NEEDS: std::sync::LazyLock<Mutex<HashMap<u64, Vec<ParsedConnect>>>> =
    std::sync::LazyLock::new(Default::default);

static POLICY_HANDLERS: std::sync::LazyLock<Mutex<HashMap<String, Arc<Vec<String>>>>> =
    std::sync::LazyLock::new(Default::default);

fn memo_get<K: std::hash::Hash + Eq, V: Clone>(memo: &Mutex<HashMap<K, V>>, key: &K) -> Option<V> {
    memo.lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(key)
        .cloned()
}

fn memo_put<K: std::hash::Hash + Eq, V>(memo: &Mutex<HashMap<K, V>>, key: K, value: V) {
    let mut memo = memo.lock().unwrap_or_else(|p| p.into_inner());
    if memo.len() >= NEEDS_MEMO_MAX {
        memo.clear();
    }
    memo.insert(key, value);
}

/// The parse of `allocation`'s `task_connect`, memoized by content (and the
/// consuming job, which decides an unqualified service's namespace). A
/// payload that does not parse yields no needs, logged once per payload.
fn connect_needs(allocation: &crate::structs::TaskAllocation) -> Arc<ConnectNeeds> {
    let key = StableHash::default()
        .str(&allocation.job_id)
        .str(&allocation.task_connect)
        .finish();
    let hit = CONNECT_NEEDS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&key)
        .and_then(|bucket| {
            bucket
                .iter()
                .find(|(job, payload, _)| {
                    *job == allocation.job_id && *payload == allocation.task_connect
                })
                .map(|(_, _, needs)| Arc::clone(needs))
        });
    if let Some(needs) = hit {
        return needs;
    }
    let needs =
        match serde_json::from_str::<crate::structs::ConnectConfig>(&allocation.task_connect) {
            Ok(connect) => {
                let mut needs = ConnectNeeds::default();
                for upstream in &connect.upstreams {
                    // The service lives in the consumer's own namespace unless
                    // the upstream names another (docs/namespaces.md). External
                    // upstreams dial a fixed destination and need no feed.
                    if let Some(service) = upstream.service_ref(&allocation.job_id) {
                        needs.upstreams.push((upstream.key(), service));
                    }
                    for hop in &upstream.handlers {
                        if let Some(service) = hop.kind.endpoint_need() {
                            needs.handler_services.push(service.to_string());
                        }
                    }
                }
                needs
            }
            Err(err) => {
                // A payload the manager wrote itself failing to parse is a bug
                // retrying cannot fix: skip its upstreams, keep the node going.
                warn!(
                    allocation_id = allocation.allocation_id,
                    error = ?err,
                    "Skipping upstream endpoints; invalid connect payload"
                );
                ConnectNeeds::default()
            }
        };
    let needs = Arc::new(needs);
    {
        let mut memo = CONNECT_NEEDS.lock().unwrap_or_else(|p| p.into_inner());
        if memo.len() >= NEEDS_MEMO_MAX {
            memo.clear();
        }
        memo.entry(key).or_default().push((
            allocation.job_id.clone(),
            allocation.task_connect.clone(),
            Arc::clone(&needs),
        ));
    }
    needs
}

/// Every listener binding on a node. `policy_handlers` names the `proxy`
/// handler services of an allocation's effective policy.
pub(crate) fn endpoint_bindings(
    allocations: &[crate::structs::TaskAllocation],
    policy_handlers: impl Fn(&crate::structs::TaskAllocation) -> Arc<Vec<String>>,
) -> Vec<EndpointBinding> {
    let mut bindings = Vec::new();
    for allocation in allocations {
        let mut handler_services: Vec<String> = policy_handlers(allocation).to_vec();
        if !allocation.task_connect.trim().is_empty() {
            let needs = connect_needs(allocation);
            for (key, service) in &needs.upstreams {
                bindings.push(EndpointBinding {
                    allocation_id: allocation.allocation_id,
                    upstream: key.clone(),
                    for_handler: false,
                    service: service.clone(),
                    consumer: allocation.job_id.clone(),
                });
            }
            handler_services.extend(needs.handler_services.iter().cloned());
        }
        handler_services.sort();
        handler_services.dedup();
        for service in handler_services {
            // A handler's `service` resolves like an upstream: in the
            // allocation's own namespace unless it is qualified.
            let target = crate::structs::namespace::qualify_in(
                crate::structs::namespace::job_namespace(&allocation.job_id),
                &service,
            );
            bindings.push(EndpointBinding {
                allocation_id: allocation.allocation_id,
                upstream: service,
                for_handler: true,
                service: target,
                consumer: allocation.job_id.clone(),
            });
        }
    }
    bindings.sort_by(|a, b| {
        (a.allocation_id, a.for_handler, &a.upstream).cmp(&(
            b.allocation_id,
            b.for_handler,
            &b.upstream,
        ))
    });
    bindings
}

/// `EndpointFeed.bindings_digest`: empty for no bindings, which is also what
/// a worker that holds none reports.
pub(crate) fn bindings_digest(bindings: &[EndpointBinding]) -> String {
    if bindings.is_empty() {
        return String::new();
    }
    let mut hash = StableHash::default();
    hash.u64(bindings.len() as u64);
    for binding in bindings {
        hash.i64(binding.allocation_id)
            .str(&binding.upstream)
            .bool(binding.for_handler)
            .str(&binding.service)
            .str(&binding.consumer);
    }
    format!("{:016x}", hash.finish())
}

/// One set on the wire.
fn wire_set(key: &SetKey, view: &crate::storage::endpoint_index::ConsumerView) -> EndpointSet {
    EndpointSet {
        service: key.service.clone(),
        consumer: key.consumer.clone(),
        version: view.version,
        digest: view.digest,
        endpoints: view.endpoints.as_ref().clone(),
    }
}

/// The node's feed as a delta against `held`: the bindings when their digest
/// moved, and every set whose digest differs from the worker's. Registers
/// the node's sets with the index so changes are pushed. Reads the catalog
/// only for sets the index holds stale; reads nothing at all when the node
/// has no bindings.
pub(crate) fn build_endpoint_feed(
    store: &Store,
    node_id: &str,
    bindings: Vec<EndpointBinding>,
    held: Option<&EndpointFeedState>,
) -> EndpointFeed {
    let index = store.endpoint_index();
    let digest = bindings_digest(&bindings);
    let held_digest = held.map(|h| h.bindings_digest.as_str()).unwrap_or("");
    let keys: BTreeSet<SetKey> = bindings
        .iter()
        .map(|binding| SetKey::new(binding.service.clone(), binding.consumer.clone()))
        .collect();
    index.register(node_id, &keys);
    let held_sets: HashMap<(&str, &str), u64> = held
        .map(|held| {
            held.sets
                .iter()
                .map(|set| ((set.service.as_str(), set.consumer.as_str()), set.digest))
                .collect()
        })
        .unwrap_or_default();
    let mut sets = Vec::new();
    for key in &keys {
        match store.endpoint_view(key) {
            Ok(view) => {
                if held_sets.get(&(key.service.as_str(), key.consumer.as_str()))
                    != Some(&view.digest)
                {
                    debug!(
                        node_id,
                        service = %key.service,
                        consumer = %key.consumer,
                        version = view.version,
                        endpoints = view.endpoints.len(),
                        "Sending a changed endpoint set"
                    );
                    sets.push(wire_set(key, &view));
                }
            }
            Err(err) => {
                // Omitting the set leaves the worker on its last known good
                // endpoints; failing here would stall the node's whole
                // reconciliation over one service.
                warn!(
                    node_id,
                    service = %key.service,
                    error = ?err,
                    "Skipping an endpoint set; service lookup failed"
                );
            }
        }
    }
    let unchanged = digest == held_digest;
    EndpointFeed {
        epoch: index.epoch(),
        bindings_unchanged: unchanged,
        bindings: if unchanged { Vec::new() } else { bindings },
        bindings_digest: digest,
        sets,
    }
}

/// The push-only `StreamWork` message for the sets queued for `node_id`, or
/// `None` when none is. Blocking: a set the refresher has not re-read yet is
/// read here.
pub(crate) fn endpoint_push(store: &Store, node_id: &str) -> Option<StreamWorkReply> {
    let index = store.endpoint_index();
    let keys = index.take_pending(node_id);
    if keys.is_empty() {
        return None;
    }
    let sets: Vec<EndpointSet> = keys
        .iter()
        .filter_map(|key| match store.endpoint_view(key) {
            Ok(view) => Some(wire_set(key, &view)),
            Err(err) => {
                debug!(node_id, service = %key.service, error = ?err, "Not pushing an endpoint set; lookup failed");
                None
            }
        })
        .collect();
    if sets.is_empty() {
        return None;
    }
    debug!(node_id, sets = sets.len(), "Pushing changed endpoint sets");
    Some(StreamWorkReply {
        epoch: store.work_log().epoch(),
        push_only: true,
        endpoint_feed: Some(EndpointFeed {
            epoch: index.epoch(),
            bindings_unchanged: true,
            sets,
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// Re-read the consumed services the projection marked dirty, so the work
/// streams of the nodes that consume them push the new sets; sweep idle
/// services and departed nodes once a minute; and re-read every consumed
/// set at least every `MAX_SET_AGE`. Runs on every manager, leader or not:
/// a worker may stream from any of them.
pub async fn run_endpoint_refresher(store: Store) {
    let index = store.endpoint_index();
    let wake = index.refresh_notify();
    let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = wake.notified() => {
                tokio::time::sleep(REFRESH_DEBOUNCE).await;
            }
            _ = tokio::time::sleep(crate::storage::endpoint_index::MAX_SET_AGE) => {}
            _ = sweep.tick() => {
                index.sweep(std::time::Instant::now());
                continue;
            }
        }
        let store = store.clone();
        match tokio::task::spawn_blocking(move || store.refresh_endpoint_sets()).await {
            Ok(0) => {}
            Ok(read) => debug!(services = read, "Re-read consumed endpoint sets"),
            Err(err) => warn!(error = ?err, "Endpoint refresher task failed"),
        }
    }
}

impl ManagerDaemonService {
    /// The `proxy` handler services of `allocation`'s effective policy
    /// (`policies` are this beat's assignments), memoized per policy body.
    fn policy_handler_services(
        &self,
        allocation: &crate::structs::TaskAllocation,
        policies: &HashMap<i64, &str>,
    ) -> Arc<Vec<String>> {
        let Some(hash) = policies.get(&allocation.allocation_id) else {
            return Arc::default();
        };
        if let Some(services) = memo_get(&POLICY_HANDLERS, &hash.to_string()) {
            return services;
        }
        let services = {
            let cache = self
                .net_policy_cache
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let Some(policy) = cache.get(*hash) else {
                // Not cached on this manager yet (the beat that assigned it
                // caches it): no handler binding until it is.
                return Arc::default();
            };
            let mut services = Vec::new();
            for chain in [policy.egress.as_ref(), policy.ingress.as_ref()]
                .into_iter()
                .flatten()
            {
                for hop in &chain.handlers {
                    if let Some(service) = hop.handler.kind.endpoint_need() {
                        services.push(service.to_string());
                    }
                }
            }
            services
        };
        let services = Arc::new(services);
        memo_put(&POLICY_HANDLERS, hash.to_string(), Arc::clone(&services));
        services
    }

    /// The heartbeat's endpoint feed for `node_id`: bindings from the
    /// allocations the node runs and their policy assignments, sets from the
    /// index, both as a delta against what the worker reported holding.
    pub(super) fn node_endpoint_feed(
        &self,
        node_id: &str,
        allocations: &[crate::structs::TaskAllocation],
        policies: &[AllocNetPolicy],
        held: Option<&EndpointFeedState>,
    ) -> EndpointFeed {
        let by_allocation: HashMap<i64, &str> = policies
            .iter()
            .map(|p| (p.allocation_id, p.policy_hash.as_str()))
            .collect();
        let bindings = endpoint_bindings(allocations, |allocation| {
            self.policy_handler_services(allocation, &by_allocation)
        });
        build_endpoint_feed(&self.store, node_id, bindings, held)
    }
}
