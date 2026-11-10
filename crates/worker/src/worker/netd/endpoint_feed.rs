//! The worker's copy of its endpoint feed (mesh-control-path plan §6.1;
//! docs/service-discovery.md, "How endpoints reach a node").
//!
//! The manager sends a delta: the node's **bindings** (which set each
//! listener uses) when they changed, and the **sets** whose content the
//! node does not hold. This state applies the delta and hands each listener
//! a shared, dialable list that only changes when its set does, so a
//! steady beat costs the forwarders nothing.
//!
//! - A set is applied only when it is newer than the one held: another
//!   manager epoch, or a higher version in the same epoch. A heartbeat
//!   reply built before a push can arrive after it; the version keeps it
//!   from rolling the push back.
//! - A set is held only while a binding names it.
//! - Nothing here fails: a feed that names a set the manager left out (its
//!   lookup failed) keeps the copy the node held, which is the fail-static
//!   contract the datapath has always had.

use crate::structs::{
    EndpointBinding, EndpointFeed, EndpointSet, EndpointSetVersion, ServiceEndpoint,
};
use crate::worker::balance::Endpoint;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// A held set as the datapath persists it.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersistedSet {
    pub epoch: u64,
    pub set: EndpointSet,
}

/// The feed as the datapath persists it: listeners come back from disk on a
/// worker restart with the endpoints they had.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersistedFeed {
    #[serde(default)]
    pub bindings: Vec<EndpointBinding>,
    #[serde(default)]
    pub sets: Vec<PersistedSet>,
}

impl PersistedFeed {
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty() && self.sets.is_empty()
    }
}

/// One listener's endpoints, as the forwarder takes them.
#[derive(Debug, Clone)]
pub struct ListenerEndpoints {
    pub allocation_id: i64,
    /// The upstream key, or a `proxy` handler's service.
    pub upstream: String,
    pub for_handler: bool,
    pub endpoints: Arc<Vec<Endpoint>>,
}

type Key = (String, String);

#[derive(Debug)]
struct Held {
    epoch: u64,
    set: EndpointSet,
    dialable: Arc<Vec<Endpoint>>,
}

impl Held {
    fn new(epoch: u64, set: EndpointSet) -> Self {
        let dialable = Arc::new(crate::worker::netd::specs::dialable(&set.endpoints));
        Held {
            epoch,
            set,
            dialable,
        }
    }
}

/// The node's endpoint feed. See the module docs.
#[derive(Debug, Default)]
pub struct EndpointFeedState {
    bindings_digest: String,
    bindings: Vec<EndpointBinding>,
    sets: HashMap<Key, Held>,
}

fn key_of(service: &str, consumer: &str) -> Key {
    (service.to_string(), consumer.to_string())
}

impl EndpointFeedState {
    /// Apply one feed (a heartbeat reply's, or a push). Returns how many
    /// sets were taken.
    pub fn apply(&mut self, feed: &EndpointFeed) -> usize {
        if !feed.bindings_unchanged {
            self.bindings = feed.bindings.clone();
            self.bindings_digest = feed.bindings_digest.clone();
        }
        let bound: HashSet<Key> = self
            .bindings
            .iter()
            .map(|binding| key_of(&binding.service, &binding.consumer))
            .collect();
        let mut taken = 0;
        for set in &feed.sets {
            let key = key_of(&set.service, &set.consumer);
            if !bound.contains(&key) {
                continue;
            }
            let newer = match self.sets.get(&key) {
                None => true,
                Some(held) => held.epoch != feed.epoch || set.version > held.set.version,
            };
            if newer {
                self.sets.insert(key, Held::new(feed.epoch, set.clone()));
                taken += 1;
            }
        }
        self.sets.retain(|key, _| bound.contains(key));
        taken
    }

    /// What the next heartbeat reports holding.
    pub fn report(&self) -> crate::structs::EndpointFeedState {
        let mut sets: Vec<EndpointSetVersion> = self
            .sets
            .values()
            .map(|held| EndpointSetVersion {
                service: held.set.service.clone(),
                consumer: held.set.consumer.clone(),
                epoch: held.epoch,
                version: held.set.version,
                digest: held.set.digest,
            })
            .collect();
        sets.sort_by(|a, b| (&a.service, &a.consumer).cmp(&(&b.service, &b.consumer)));
        crate::structs::EndpointFeedState {
            bindings_digest: self.bindings_digest.clone(),
            sets,
        }
    }

    /// Every binding whose set is held, with its shared, dialable list. A
    /// binding whose set the node never received is left out: its listener
    /// keeps what it had.
    pub fn listeners(&self) -> Vec<ListenerEndpoints> {
        self.bindings
            .iter()
            .filter_map(|binding| {
                let held = self
                    .sets
                    .get(&key_of(&binding.service, &binding.consumer))?;
                Some(ListenerEndpoints {
                    allocation_id: binding.allocation_id,
                    upstream: binding.upstream.clone(),
                    for_handler: binding.for_handler,
                    endpoints: Arc::clone(&held.dialable),
                })
            })
            .collect()
    }

    /// Whether any held set has a mesh endpoint: the node then needs its
    /// mesh certificate to dial it.
    pub fn mesh_wanted(&self) -> bool {
        self.sets
            .values()
            .any(|held| held.dialable.iter().any(|endpoint| endpoint.mesh_tls))
    }

    /// The upstream endpoints of one allocation, by upstream key, for its
    /// config-file templates. `proxy` handler targets are not templated.
    pub fn template_endpoints(&self, allocation_id: i64) -> BTreeMap<String, Vec<Endpoint>> {
        self.listeners()
            .into_iter()
            .filter(|listener| listener.allocation_id == allocation_id && !listener.for_handler)
            .map(|listener| (listener.upstream, listener.endpoints.as_ref().clone()))
            .collect()
    }

    /// The raw endpoints one listener uses (diagnostics and tests).
    pub fn endpoints_of(&self, allocation_id: i64, upstream: &str) -> Option<&[ServiceEndpoint]> {
        let binding = self
            .bindings
            .iter()
            .find(|b| b.allocation_id == allocation_id && b.upstream == upstream)?;
        self.sets
            .get(&key_of(&binding.service, &binding.consumer))
            .map(|held| held.set.endpoints.as_slice())
    }

    pub fn snapshot(&self) -> PersistedFeed {
        let mut sets: Vec<PersistedSet> = self
            .sets
            .values()
            .map(|held| PersistedSet {
                epoch: held.epoch,
                set: held.set.clone(),
            })
            .collect();
        sets.sort_by(|a, b| {
            (&a.set.service, &a.set.consumer).cmp(&(&b.set.service, &b.set.consumer))
        });
        PersistedFeed {
            bindings: self.bindings.clone(),
            sets,
        }
    }

    /// The feed a restarted worker comes back with: the bindings of the
    /// allocations that survived, and the sets they name. The bindings
    /// digest is not restored, so the first heartbeat is sent the node's
    /// bindings again; the sets are reported, so only changed ones are.
    pub fn restore(persisted: PersistedFeed, live: &HashSet<i64>) -> Self {
        let bindings: Vec<EndpointBinding> = persisted
            .bindings
            .into_iter()
            .filter(|binding| live.contains(&binding.allocation_id))
            .collect();
        let bound: HashSet<Key> = bindings
            .iter()
            .map(|binding| key_of(&binding.service, &binding.consumer))
            .collect();
        let sets = persisted
            .sets
            .into_iter()
            .map(|held| {
                (
                    key_of(&held.set.service, &held.set.consumer),
                    Held::new(held.epoch, held.set),
                )
            })
            .filter(|(key, _)| bound.contains(key))
            .collect();
        EndpointFeedState {
            bindings_digest: String::new(),
            bindings,
            sets,
        }
    }
}

#[cfg(test)]
mod endpoint_feed_state_test {
    use super::*;

    fn binding(allocation_id: i64, upstream: &str, service: &str) -> EndpointBinding {
        EndpointBinding {
            allocation_id,
            upstream: upstream.into(),
            for_handler: false,
            service: service.into(),
            consumer: "web".into(),
        }
    }

    fn set(service: &str, version: u64, ports: &[u32]) -> EndpointSet {
        EndpointSet {
            service: service.into(),
            consumer: "web".into(),
            version,
            digest: version * 1000 + ports.len() as u64,
            endpoints: ports
                .iter()
                .map(|port| ServiceEndpoint {
                    address: "10.0.0.5".into(),
                    port: *port,
                    allocation_id: i64::from(*port),
                    ..Default::default()
                })
                .collect(),
        }
    }

    fn full(epoch: u64, bindings: Vec<EndpointBinding>, sets: Vec<EndpointSet>) -> EndpointFeed {
        EndpointFeed {
            epoch,
            bindings_unchanged: false,
            bindings_digest: format!("d{}", bindings.len()),
            bindings,
            sets,
        }
    }

    fn push(epoch: u64, sets: Vec<EndpointSet>) -> EndpointFeed {
        EndpointFeed {
            epoch,
            bindings_unchanged: true,
            sets,
            ..Default::default()
        }
    }

    #[test]
    fn allocations_binding_one_set_share_one_list() {
        let mut state = EndpointFeedState::default();
        state.apply(&full(
            1,
            vec![binding(1, "db", "db"), binding(2, "db", "db")],
            vec![set("db", 3, &[5432])],
        ));
        let listeners = state.listeners();
        assert_eq!(listeners.len(), 2);
        assert!(Arc::ptr_eq(
            &listeners[0].endpoints,
            &listeners[1].endpoints
        ));
        assert_eq!(listeners[0].endpoints[0].addr, "10.0.0.5:5432");
        let report = state.report();
        assert_eq!(report.bindings_digest, "d2");
        assert_eq!(report.sets.len(), 1);
        assert_eq!((report.sets[0].epoch, report.sets[0].version), (1, 3));
    }

    #[test]
    fn a_stale_set_is_ignored_and_a_newer_one_replaces_only_its_list() {
        let mut state = EndpointFeedState::default();
        state.apply(&full(
            1,
            vec![binding(1, "db", "db"), binding(1, "cache", "cache")],
            vec![set("db", 5, &[1, 2]), set("cache", 5, &[9])],
        ));
        let cache_of = |state: &EndpointFeedState| {
            state
                .listeners()
                .into_iter()
                .find(|l| l.upstream == "cache")
                .unwrap()
                .endpoints
        };
        let cache_before = cache_of(&state);
        // A heartbeat reply built before the last push arrives after it.
        assert_eq!(state.apply(&push(1, vec![set("db", 4, &[1])])), 0);
        assert_eq!(state.endpoints_of(1, "db").unwrap().len(), 2);
        // A newer version replaces db, and only db.
        assert_eq!(state.apply(&push(1, vec![set("db", 6, &[1])])), 1);
        assert_eq!(state.endpoints_of(1, "db").unwrap().len(), 1);
        assert!(Arc::ptr_eq(&cache_before, &cache_of(&state)), "untouched");
        // Another manager's epoch is taken whatever its version.
        assert_eq!(state.apply(&push(2, vec![set("db", 1, &[1, 2, 3])])), 1);
        assert_eq!(state.endpoints_of(1, "db").unwrap().len(), 3);
    }

    #[test]
    fn unbound_sets_are_dropped_and_never_taken_from_a_push() {
        let mut state = EndpointFeedState::default();
        state.apply(&full(
            1,
            vec![binding(1, "db", "db")],
            vec![set("db", 1, &[1])],
        ));
        assert_eq!(state.apply(&push(1, vec![set("other", 9, &[1])])), 0);
        // The binding goes away: so does its set.
        state.apply(&full(1, Vec::new(), Vec::new()));
        assert!(state.listeners().is_empty());
        assert!(state.report().sets.is_empty());
        assert_eq!(state.report().bindings_digest, "d0");
    }

    #[test]
    fn a_missing_set_keeps_the_listener_on_what_it_had() {
        let mut state = EndpointFeedState::default();
        state.apply(&full(
            1,
            vec![binding(1, "db", "db")],
            vec![set("db", 1, &[1])],
        ));
        // The manager could not read db this beat: no set, bindings the same.
        state.apply(&EndpointFeed {
            epoch: 1,
            bindings_unchanged: true,
            ..Default::default()
        });
        assert_eq!(state.endpoints_of(1, "db").unwrap().len(), 1);
    }

    #[test]
    fn a_restore_keeps_surviving_allocations_and_resends_bindings() {
        let mut state = EndpointFeedState::default();
        state.apply(&full(
            1,
            vec![binding(1, "db", "db"), binding(2, "cache", "cache")],
            vec![set("db", 1, &[1]), set("cache", 1, &[2])],
        ));
        let restored = EndpointFeedState::restore(state.snapshot(), &[1].into_iter().collect());
        assert_eq!(restored.listeners().len(), 1);
        let report = restored.report();
        assert_eq!(report.bindings_digest, "", "bindings are asked for again");
        assert_eq!(report.sets.len(), 1);
        assert_eq!(report.sets[0].service, "db");
    }

    /// Mesh-control-path plan §6.5: an IPv6 endpoint is dialable, bracketed.
    #[test]
    fn an_ipv6_endpoint_is_bracketed_and_dialable() {
        let mut state = EndpointFeedState::default();
        let mut v6 = set("db", 1, &[5432]);
        v6.endpoints[0].address = "fd00::7".into();
        state.apply(&full(1, vec![binding(1, "db", "db")], vec![v6]));
        let listener = state.listeners().remove(0);
        assert_eq!(listener.endpoints[0].addr, "[fd00::7]:5432");
        let addr: std::net::SocketAddr = listener.endpoints[0].addr.parse().expect("dialable");
        assert!(addr.is_ipv6());
    }
}
