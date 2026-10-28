//! Sharding the autoscaler across replicas (docs/autoscaling.md,
//! "Replicas"; autoscaler-v2 plan §6.9, calls entry 13).
//!
//! Policies hash to [`SHARDS`] shards ([`crate::autoscaler::shard`]). Each
//! replica holds a **membership lease** and **shard leases**, KV locks
//! under `legion/apps/autoscaler/` with a [`LEASE_TTL`] TTL renewed every
//! [`RENEW_EVERY`], and evaluates only the policies of the shards it holds:
//!
//! ```text
//! legion/apps/autoscaler/members/<instance>   one per live replica
//! legion/apps/autoscaler/shards/all           a lone replica: every shard
//! legion/apps/autoscaler/shards/<n>           with several: one per shard
//! ```
//!
//! - **A lone replica holds one lease, `shards/all`**, not 64: the default
//!   (one replica) costs two lease writes per renewal, not 65.
//! - **With several, each holds up to `ceil(64 / live)`** individual
//!   shard leases: it releases what it holds above that, and takes free
//!   shards up to it. A replica that holds `all` when a second one joins
//!   first takes its own share, then releases `all`; the newcomer takes the
//!   rest on its next round. A replica that dies holds its leases until
//!   they expire (one TTL), and the others take its shards then.
//! - **Leases are not fences** (KV locks carry no fencing token, calls
//!   entry 12): two replicas may briefly both think they own a shard. Every
//!   write the engine makes is compare-and-set, so the stale one loses.
//!
//! [`plan`] is the pure part (which leases to take, keep, and give up),
//! unit-tested on its own; [`Shards`] runs it against the KV store.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::autoscaler::shard::SHARDS;
use crate::cmd::helpers::AuthedManagerClient;
use crate::prelude::*;
use crate::structs::{AcquireKvLockRequest, ListKvRequest, ReleaseKvLockRequest};

/// How long a lease lives without renewal.
pub const LEASE_TTL: Duration = Duration::from_secs(15);
/// How often leases are renewed (a third of the TTL).
pub const RENEW_EVERY: Duration = Duration::from_secs(5);
/// Every lease's key starts here (the app's grant covers `legion/apps/autoscaler/*`).
pub const LEASE_PREFIX: &str = "legion/apps/autoscaler/";
const MEMBERS: &str = "legion/apps/autoscaler/members/";
const SHARD_PREFIX: &str = "legion/apps/autoscaler/shards/";
const ALL: &str = "legion/apps/autoscaler/shards/all";

/// A shard's lease key.
pub fn shard_key(shard: u32) -> String {
    format!("{SHARD_PREFIX}{shard:02}")
}

/// A member's lease key.
pub fn member_key(id: &str) -> String {
    format!("{MEMBERS}{id}")
}

/// The name a replica holds its leases under: its instance, with anything
/// but letters, digits, `.`, `_` and `-` made a `-` (`alloc 41` holds
/// `members/alloc-41`).
pub fn lease_id(instance: &str) -> String {
    let id: String = instance
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    if id.is_empty() {
        "autoscaler".to_string()
    } else {
        id
    }
}

/// The leases, as one listing shows them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseView {
    /// Live members, by instance.
    pub members: BTreeSet<String>,
    /// Individual shard leases: shard -> holder.
    pub holders: BTreeMap<u32, String>,
    /// The holder of `shards/all`, when held.
    pub all: Option<String>,
}

impl LeaseView {
    /// Read a listing of `(key, owner)` pairs under [`LEASE_PREFIX`].
    pub fn from_locks<'a>(locks: impl Iterator<Item = (&'a str, &'a str)>) -> Self {
        let mut view = LeaseView::default();
        for (key, owner) in locks {
            if let Some(instance) = key.strip_prefix(MEMBERS) {
                view.members.insert(instance.to_string());
            } else if key == ALL {
                view.all = Some(owner.to_string());
            } else if let Some(shard) = key
                .strip_prefix(SHARD_PREFIX)
                .and_then(|n| n.parse::<u32>().ok())
                .filter(|shard| *shard < SHARDS)
            {
                view.holders.insert(shard, owner.to_string());
            }
        }
        view
    }
}

/// What one round does.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// Take (or renew) `shards/all`.
    pub hold_all: bool,
    /// Give up `shards/all` (after taking `acquire`).
    pub release_all: bool,
    /// Individual shards to take.
    pub acquire: Vec<u32>,
    /// Individual shards held, to renew.
    pub renew: Vec<u32>,
    /// Individual shards to give up.
    pub release: Vec<u32>,
    /// The share each member aims for (64 when alone).
    pub target: u32,
}

/// The order this replica prefers free shards in: a hash of the pair, so
/// replicas taking shards at the same moment mostly reach for different
/// ones (rendezvous hashing), and a replica that comes back prefers the
/// shards it had.
fn preference(me: &str, shard: u32) -> u64 {
    crate::autoscaler::shard::fnv1a(format!("{me}\u{0}{shard}").as_bytes())
}

/// Plan one round for `me`, which holds the individual shards `held` (and
/// `shards/all` when `holding_all`), against `view` (which lists `me` as a
/// member once its membership lease is current).
pub fn plan(me: &str, view: &LeaseView, held: &BTreeSet<u32>, holding_all: bool) -> Plan {
    let mut members = view.members.clone();
    members.insert(me.to_string());
    let others_hold_all = view.all.as_deref().is_some_and(|holder| holder != me);
    if members.len() == 1 {
        // Alone: one lease for everything, once no one else holds it.
        let hold_all = !others_hold_all;
        return Plan {
            hold_all,
            release_all: false,
            acquire: Vec::new(),
            renew: if hold_all {
                Vec::new()
            } else {
                held.iter().copied().collect()
            },
            // `all` covers what this replica held one by one.
            release: if hold_all {
                held.iter().copied().collect()
            } else {
                Vec::new()
            },
            target: SHARDS,
        };
    }
    let target = SHARDS.div_ceil(members.len() as u32);
    let mut plan = Plan {
        target,
        ..Default::default()
    };
    if others_hold_all {
        // Another replica still holds everything; it gives `all` up once
        // it sees this one. Keep what is held, take nothing.
        plan.renew = held.iter().copied().collect();
        return plan;
    }
    let mut mine: Vec<u32> = held.iter().copied().collect();
    if mine.len() as u32 > target {
        // Give up the surplus, the least preferred first.
        mine.sort_by_key(|shard| std::cmp::Reverse(preference(me, *shard)));
        let surplus = mine.len() - target as usize;
        plan.release = mine.drain(..surplus).collect();
        plan.release.sort_unstable();
    }
    let missing = (target as usize).saturating_sub(mine.len());
    if missing > 0 {
        let mut free: Vec<u32> = (0..SHARDS)
            .filter(|shard| !view.holders.contains_key(shard) && !held.contains(shard))
            .collect();
        free.sort_by_key(|shard| std::cmp::Reverse(preference(me, *shard)));
        plan.acquire = free.into_iter().take(missing).collect();
        plan.acquire.sort_unstable();
    }
    mine.sort_unstable();
    plan.renew = mine;
    plan.release_all = holding_all;
    plan
}

/// Which shards this replica evaluates now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ownership {
    /// The shards, `None` meaning all of them (a lone replica, or sharding
    /// off).
    pub shards: Option<BTreeSet<u32>>,
    /// Live members at the last round.
    pub members: usize,
    /// Bumped whenever `shards` changes (the policy watch restarts).
    pub epoch: u64,
    /// Whether a lease controller has published (false: no leases, this
    /// engine evaluates everything, as in tests).
    pub sharded: bool,
}

impl Default for Ownership {
    fn default() -> Self {
        Self {
            shards: None,
            members: 1,
            epoch: 0,
            sharded: false,
        }
    }
}

impl Ownership {
    /// Whether this replica evaluates `shard`.
    pub fn owns(&self, shard: u32) -> bool {
        self.shards.as_ref().is_none_or(|shards| shards.contains(&shard))
    }

    /// The shard list a watch or a report names (empty: all).
    pub fn list(&self) -> Vec<u32> {
        self.shards
            .as_ref()
            .map(|shards| shards.iter().copied().collect())
            .unwrap_or_default()
    }
}

/// Lease timing (tests shorten it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseTiming {
    /// The TTL.
    pub ttl: Duration,
    /// The renewal period.
    pub renew: Duration,
}

impl Default for LeaseTiming {
    fn default() -> Self {
        Self {
            ttl: LEASE_TTL,
            renew: RENEW_EVERY,
        }
    }
}

/// The lease controller of one replica.
pub struct Shards {
    client: AuthedManagerClient,
    /// The replica's [`lease_id`]: its member key and every lease's owner.
    id: String,
    timing: LeaseTiming,
    /// Lock ids of the leases held: key -> lock id.
    held: BTreeMap<String, String>,
    ownership: Arc<RwLock<Ownership>>,
}

impl Shards {
    /// A controller for `instance`, publishing into `ownership`.
    pub fn new(
        client: AuthedManagerClient,
        instance: &str,
        timing: LeaseTiming,
        ownership: Arc<RwLock<Ownership>>,
    ) -> Self {
        Self {
            client,
            id: lease_id(instance),
            timing,
            held: BTreeMap::new(),
            ownership,
        }
    }

    fn ttl_seconds(&self) -> i64 {
        self.timing.ttl.as_secs().max(1) as i64
    }

    /// Take or renew `key` (a lock this replica already holds is renewed
    /// by acquiring it again with its own lock id). Returns whether it is
    /// held.
    async fn take(&mut self, key: &str) -> bool {
        let lock_id = self
            .held
            .get(key)
            .cloned()
            .unwrap_or_else(|| format!("{:032x}", rand::random::<u128>()));
        match self
            .client
            .acquire_kv_lock(AcquireKvLockRequest {
                key: key.to_string(),
                lock_id: Some(lock_id.clone()),
                owner: Some(self.id.clone()),
                ttl_seconds: self.ttl_seconds(),
                force: false,
            })
            .await
        {
            Ok(_) => {
                self.held.insert(key.to_string(), lock_id);
                true
            }
            Err(status) => {
                if self.held.remove(key).is_some() {
                    warn!(lease = key, "Lost an autoscaler lease: {}", status.message());
                } else {
                    debug!(lease = key, "Lease not taken: {}", status.message());
                }
                false
            }
        }
    }

    async fn give_up(&mut self, key: &str) {
        if let Some(lock_id) = self.held.remove(key) {
            if let Err(status) = self
                .client
                .release_kv_lock(ReleaseKvLockRequest {
                    key: key.to_string(),
                    lock_id,
                })
                .await
            {
                debug!(lease = key, "Releasing a lease: {}", status.message());
            }
        }
    }

    fn held_shards(&self) -> BTreeSet<u32> {
        self.held
            .keys()
            .filter_map(|key| key.strip_prefix(SHARD_PREFIX))
            .filter_map(|n| n.parse().ok())
            .collect()
    }

    /// One round: renew membership, read the leases, plan, apply, and
    /// publish what this replica now owns. Returns the ownership.
    pub async fn step(&mut self) -> Ownership {
        let me = member_key(&self.id);
        if !self.take(&me).await {
            // Not even membership: the manager is unreachable or refused.
            // Keep evaluating what was owned (everything, for a replica that
            // never held a lease); the compare-and-set writes keep an
            // overlap safe.
            return self.current();
        }
        let listing = match self
            .client
            .list_kv(ListKvRequest {
                prefix: LEASE_PREFIX.to_string(),
                include_values: false,
                include_locks: true,
            })
            .await
        {
            Ok(reply) => reply.into_inner().locks,
            Err(status) => {
                warn!("Listing the autoscaler's leases: {}", status.message());
                return self.current();
            }
        };
        let view = LeaseView::from_locks(
            listing
                .iter()
                .map(|lock| (lock.key.as_str(), lock.owner.as_deref().unwrap_or_default())),
        );
        let held = self.held_shards();
        let holding_all = self.held.contains_key(ALL);
        let plan = plan(&self.id, &view, &held, holding_all);
        let mut all = false;
        if plan.hold_all {
            all = self.take(ALL).await;
        }
        for shard in &plan.renew {
            self.take(&shard_key(*shard)).await;
        }
        for shard in &plan.acquire {
            self.take(&shard_key(*shard)).await;
        }
        if all || !plan.hold_all {
            for shard in &plan.release {
                self.give_up(&shard_key(*shard)).await;
            }
        }
        if plan.release_all {
            self.give_up(ALL).await;
        }
        let mut members = view.members.clone();
        members.insert(self.id.clone());
        let members = members.len();
        if all {
            // Every shard but those a departed replica still holds.
            let others: BTreeSet<u32> = view
                .holders
                .iter()
                .filter(|(_, holder)| **holder != self.id)
                .map(|(shard, _)| *shard)
                .collect();
            return self.publish(
                (!others.is_empty())
                    .then(|| (0..SHARDS).filter(|s| !others.contains(s)).collect()),
                members,
            );
        }
        let shards = self.held_shards();
        self.publish(Some(shards), members)
    }

    fn current(&self) -> Ownership {
        self.ownership
            .read()
            .map(|ownership| ownership.clone())
            .unwrap_or_default()
    }

    fn publish(&self, shards: Option<BTreeSet<u32>>, members: usize) -> Ownership {
        let mut ownership = self
            .ownership
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if ownership.shards != shards {
            ownership.epoch += 1;
            ownership.shards = shards;
        }
        ownership.members = members;
        ownership.sharded = true;
        ownership.clone()
    }

    /// Release every lease (a clean stop), so the others take the shards
    /// at once instead of after one TTL.
    pub async fn release_all(&mut self) {
        let keys: Vec<String> = self.held.keys().cloned().collect();
        for key in keys {
            self.give_up(&key).await;
        }
    }

    /// Run a round every renewal period until `stop` fires (or its sender
    /// is dropped), then release every lease.
    pub fn spawn(
        mut self,
        mut stop: tokio::sync::oneshot::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.timing.renew);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick fires at once; the caller just stepped.
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = &mut stop => {
                        self.release_all().await;
                        return;
                    }
                    _ = ticker.tick() => {}
                }
                let before = self.current();
                let after = self.step().await;
                if before.shards != after.shards {
                    info!(
                        instance = %self.id,
                        members = after.members,
                        shards = after.shards.as_ref().map_or(SHARDS as usize, |s| s.len()),
                        "Autoscaler shards rebalanced"
                    );
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(members: &[&str], holders: &[(u32, &str)], all: Option<&str>) -> LeaseView {
        LeaseView {
            members: members.iter().map(|m| m.to_string()).collect(),
            holders: holders.iter().map(|(s, h)| (*s, h.to_string())).collect(),
            all: all.map(str::to_string),
        }
    }

    /// Simulate replicas taking turns until nothing changes; returns who
    /// holds what.
    fn converge(names: &[&str], mut state: BTreeMap<String, (BTreeSet<u32>, bool)>) -> BTreeMap<String, (BTreeSet<u32>, bool)> {
        for _ in 0..20 {
            let before = state.clone();
            for name in names {
                let mut holders = BTreeMap::new();
                let mut all = None;
                for (who, (shards, holds_all)) in &state {
                    for shard in shards {
                        holders.insert(*shard, who.clone());
                    }
                    if *holds_all {
                        all = Some(who.clone());
                    }
                }
                let lease_view = LeaseView {
                    members: names.iter().map(|n| n.to_string()).collect(),
                    holders,
                    all,
                };
                let (held, holding_all) = state.get(*name).cloned().unwrap_or_default();
                let plan = plan(name, &lease_view, &held, holding_all);
                let mut shards = held.clone();
                let mut holds = holding_all;
                if plan.hold_all && lease_view.all.as_deref().is_none_or(|h| h == *name) {
                    holds = true;
                }
                for shard in &plan.acquire {
                    if !lease_view.holders.contains_key(shard) {
                        shards.insert(*shard);
                    }
                }
                for shard in &plan.release {
                    shards.remove(shard);
                }
                if plan.release_all {
                    holds = false;
                }
                state.insert(name.to_string(), (shards, holds));
            }
            if state == before {
                break;
            }
        }
        state
    }

    #[test]
    fn a_lone_replica_holds_one_lease_for_everything_test() {
        let plan = plan("a", &view(&["a"], &[], None), &BTreeSet::new(), false);
        assert!(plan.hold_all);
        assert!(plan.acquire.is_empty() && plan.renew.is_empty());
        assert_eq!(plan.target, SHARDS);
        // A lone replica that held shards one by one gives them up for `all`.
        let held: BTreeSet<u32> = [1, 2].into_iter().collect();
        let plan = super::plan("a", &view(&["a"], &[(1, "a"), (2, "a")], None), &held, false);
        assert_eq!(plan.release, vec![1, 2]);
        // Someone else's `all` (a departed replica's, not yet expired):
        // wait for it.
        let plan = super::plan("a", &view(&["a"], &[], Some("b")), &BTreeSet::new(), false);
        assert!(!plan.hold_all);
    }

    #[test]
    fn replicas_split_the_shards_and_rebalance_test() {
        // One replica first: it holds `all`.
        let state = converge(&["a"], BTreeMap::new());
        assert_eq!(state["a"], (BTreeSet::new(), true));
        // Two more join: everyone ends with a fair share, `all` released.
        let state = converge(&["a", "b", "c"], state);
        let mut seen = BTreeSet::new();
        for (who, (shards, all)) in &state {
            assert!(!all, "{who} still holds all");
            // ceil(64 / 3) = 22 at most each: 22, 22 and 20.
            assert!((20..=22).contains(&shards.len()), "{who}: {}", shards.len());
            for shard in shards {
                assert!(seen.insert(*shard), "shard {shard} held twice");
            }
        }
        assert_eq!(seen.len(), SHARDS as usize);
        // `c` dies: its leases expire, and a and b split everything.
        let mut survivors = state.clone();
        survivors.remove("c");
        let state = converge(&["a", "b"], survivors);
        let total: usize = state.values().map(|(shards, _)| shards.len()).sum();
        assert_eq!(total, SHARDS as usize);
        assert!(state.values().all(|(shards, _)| shards.len() == 32));
    }

    #[test]
    fn a_replica_waits_while_another_holds_everything_test() {
        let plan = plan("b", &view(&["a", "b"], &[], Some("a")), &BTreeSet::new(), false);
        assert!(plan.acquire.is_empty() && !plan.hold_all);
        assert_eq!(plan.target, 32);
        // The holder of `all` takes its share, then lets `all` go.
        let plan = super::plan("a", &view(&["a", "b"], &[], Some("a")), &BTreeSet::new(), true);
        assert_eq!(plan.acquire.len(), 32);
        assert!(plan.release_all);
    }

    #[test]
    fn lease_ids_are_key_safe_test() {
        assert_eq!(lease_id("alloc 41"), "alloc-41");
        assert_eq!(lease_id("web-1.local"), "web-1.local");
        assert_eq!(lease_id("  "), "autoscaler");
        assert_eq!(member_key("alloc-41"), "legion/apps/autoscaler/members/alloc-41");
    }

    /// The leases live under the reserved `legion/` prefix: a tenant's
    /// `paths = ["*"]` does not reach them, and the app's own grant does.
    #[test]
    fn leases_are_out_of_a_tenants_reach_test() {
        use crate::structs::acl::kv_path_glob_match;
        for key in [member_key("alloc-1"), shard_key(7), ALL.to_string()] {
            assert!(key.starts_with(LEASE_PREFIX));
            assert!(!kv_path_glob_match("*", &key), "{key}");
            assert!(kv_path_glob_match("legion/apps/autoscaler/*", &key), "{key}");
        }
    }

    #[test]
    fn listings_parse_and_ownership_answers_test() {
        let view = LeaseView::from_locks(
            [
                ("legion/apps/autoscaler/members/alloc 1", "alloc 1"),
                ("legion/apps/autoscaler/shards/07", "alloc 1"),
                ("legion/apps/autoscaler/shards/all", "alloc 2"),
                ("legion/apps/autoscaler/shards/99", "alloc 2"),
                ("legion/apps/autoscaler/other", "x"),
            ]
            .into_iter(),
        );
        assert_eq!(view.members.len(), 1);
        assert_eq!(view.holders.get(&7).map(String::as_str), Some("alloc 1"));
        assert_eq!(view.holders.len(), 1);
        assert_eq!(view.all.as_deref(), Some("alloc 2"));
        assert_eq!(shard_key(7), "legion/apps/autoscaler/shards/07");
        let ownership = Ownership {
            shards: Some([3].into_iter().collect()),
            members: 2,
            epoch: 1,
            sharded: true,
        };
        assert!(ownership.owns(3) && !ownership.owns(4));
        assert!(Ownership::default().owns(63));
    }
}
