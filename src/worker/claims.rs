//! Worker-side bookkeeping for provisional allocations: the claim table and
//! the local resource ledger (`internal_docs/plans/edge-boot-plan.md` §6.1).
//!
//! A **provisional allocation** is work this node has started that the store
//! has not yet recorded. It has no allocation id — the manager mints those
//! inside the apply transaction — so the worker keys it by a *local key*: a
//! negative `i64` that can never collide with a manager-minted id. Every map
//! in [`crate::worker::task_manager::WorkerTaskManager`] therefore works on
//! provisional work unchanged, and ratification records the manager's id
//! alongside the local key rather than moving anything.
//!
//! Both structures are in memory only, and that is a design decision, not an
//! omission: a worker restart kills provisional work by design (plan §6.6), so
//! there is nothing to persist for an unratified claim. Ratified claims are
//! persisted through the normal allocation path, carrying their external id.
//!
//! The two safety properties this module owns:
//!
//! - **A node cannot double-admit itself into oversubscription.** Every claim
//!   holds its request in [`LocalLedger`] from admission until it is confirmed
//!   or killed, and admission fails when the hold would not fit.
//! - **One live claim per instance id per node.** Two requests for the same
//!   sandbox arriving at the same node resolve locally, before either boots;
//!   only races that cross nodes need the manager.

use crate::prelude::*;
use crate::structs::edge_boot::{ClaimStage, Gate, OnRefused};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// One provisional allocation this node is holding.
#[derive(Debug, Clone)]
pub struct ClaimRecord {
    pub claim_id: String,
    /// Worker-local allocation key. Always negative, so it can never collide
    /// with a manager-minted allocation id.
    pub local_key: i64,
    /// Gate in force, after intersecting the task's mode with any ticket
    /// ceiling and the caller's request.
    pub gate: Gate,
    pub stage: ClaimStage,
    /// When the node kills this claim if it is still unratified.
    pub lease_deadline: Instant,
    /// The same deadline as unix seconds, for the heartbeat report.
    pub lease_deadline_unix: i64,
    pub job_id: String,
    pub job_config_id: String,
    pub task_name: String,
    /// Instance being raced for; empty for stateless tasks.
    pub instance_id: String,
    /// Normalized resource request held in the ledger.
    pub resources: Value,
    /// `manager` (pre-commit dispatch) or `edge` (a boot ticket).
    pub origin: String,
    pub on_refused: OnRefused,
    /// Boot ticket `jti` for edge starts; empty otherwise. Never the ticket.
    pub ticket_jti: String,
    /// Manager-minted allocation id, once ratified.
    pub allocation_id: Option<i64>,
    /// Set for a session (docs/session-mode.md): work admitted into its
    /// task's envelope rather than the node's free capacity. A session has no
    /// lease deadline and is never ratified by the store; it is ratified only
    /// when promoted, and then still runs inside the envelope.
    pub session: Option<SessionClaim>,
}

/// What a session claim carries beyond an ordinary claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionClaim {
    /// When the manager's router issued the session, unix milliseconds. Of
    /// two copies of one session id, the later is stopped.
    pub lease_issued_at_unix_ms: i64,
    /// The session's generation (session durability plan §6.7): 1 at
    /// start, one more for every resume that claims its manifest. Of two
    /// copies of one session id, the higher generation survives.
    pub generation: u64,
}

impl ClaimRecord {
    pub fn is_ratified(&self) -> bool {
        self.allocation_id.is_some()
    }

    pub fn is_session(&self) -> bool {
        self.session.is_some()
    }
}

/// The node's provisional claims, keyed by claim id.
///
/// Every lookup a start makes is O(1): a node holding thousands of sessions
/// admits the next one without scanning them (session durability plan
/// §6.1). The indexes below are kept in step by `insert`, `remove`, and
/// `ratify`, the only mutations of a claim's identity.
#[derive(Debug, Default)]
pub struct ClaimTable {
    claims: HashMap<String, ClaimRecord>,
    /// Instance id -> claim id, for the one-live-claim-per-instance rule.
    by_instance: HashMap<String, String>,
    /// Local key -> claim id.
    by_key: HashMap<i64, String>,
    /// Manager allocation id -> claim id, for ratified claims.
    by_allocation: HashMap<i64, String>,
    /// What each session task's claims hold, by `(job_id, task_name)`: the
    /// running totals envelope admission checks instead of summing every
    /// session on each start.
    session_usage: HashMap<(String, String), HashMap<String, f64>>,
    /// Local keys, handed out downward from -1. Seeded below any restored
    /// key at startup so a restart cannot reissue a key a ratified claim is
    /// still using.
    next_local_key: i64,
}

impl ClaimTable {
    pub fn new() -> Self {
        ClaimTable {
            next_local_key: -1,
            ..ClaimTable::default()
        }
    }

    fn add_usage(&mut self, record: &ClaimRecord, sign: f64) {
        if !record.is_session() {
            return;
        }
        let key = (record.job_id.clone(), record.task_name.clone());
        let totals = self.session_usage.entry(key.clone()).or_default();
        for (name, amount) in crate::structs::session::resource_amounts(&record.resources) {
            *totals.entry(name).or_insert(0.0) += sign * amount;
        }
        if totals.values().all(|amount| amount.abs() < 1e-9) {
            self.session_usage.remove(&key);
        }
    }

    /// What the session claims of one task hold on this node, promoted ones
    /// included (a promoted session runs inside the envelope until it
    /// stops).
    pub fn session_usage(&self, job_id: &str, task_name: &str) -> HashMap<String, f64> {
        self.session_usage
            .get(&(job_id.to_string(), task_name.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Ensure future keys stay below every key already in use. Called with
    /// the local keys restored from the worker state file.
    pub fn reserve_below(&mut self, used: impl IntoIterator<Item = i64>) {
        for key in used {
            if key < self.next_local_key {
                self.next_local_key = key - 1;
            }
        }
    }

    fn mint_local_key(&mut self) -> i64 {
        let key = self.next_local_key;
        self.next_local_key -= 1;
        key
    }

    /// Register a new claim. Fails when this node already holds a live claim
    /// for the same instance id — the local half of the at-most-one rule,
    /// which resolves same-node races without involving the manager at all.
    pub fn insert(&mut self, mut record: ClaimRecord) -> R<ClaimRecord> {
        if self.claims.contains_key(&record.claim_id) {
            bail!("claim {} already exists on this node", record.claim_id);
        }
        if !record.instance_id.is_empty() && self.instance_is_claimed(&record.instance_id) {
            bail!(
                "instance {} is already claimed on this node",
                record.instance_id
            );
        }
        record.local_key = self.mint_local_key();
        let stored = record.clone();
        self.index(&record);
        self.claims.insert(record.claim_id.clone(), record);
        Ok(stored)
    }

    fn index(&mut self, record: &ClaimRecord) {
        if !record.instance_id.is_empty() {
            self.by_instance
                .insert(record.instance_id.clone(), record.claim_id.clone());
        }
        self.by_key
            .insert(record.local_key, record.claim_id.clone());
        if let Some(allocation_id) = record.allocation_id {
            self.by_allocation
                .insert(allocation_id, record.claim_id.clone());
        }
        self.add_usage(record, 1.0);
    }

    /// Re-register a claim restored after a worker restart under the local
    /// key it already runs with (its directories, files, and process are
    /// keyed by it). Future keys are seeded below it.
    pub fn insert_restored(&mut self, record: ClaimRecord) -> R<ClaimRecord> {
        if self.claims.contains_key(&record.claim_id) {
            bail!("claim {} already exists on this node", record.claim_id);
        }
        if !record.instance_id.is_empty() && self.instance_is_claimed(&record.instance_id) {
            bail!(
                "instance {} is already claimed on this node",
                record.instance_id
            );
        }
        if self.by_key.contains_key(&record.local_key) {
            bail!("local key {} is already in use", record.local_key);
        }
        self.reserve_below([record.local_key]);
        let stored = record.clone();
        self.index(&record);
        self.claims.insert(record.claim_id.clone(), record);
        Ok(stored)
    }

    pub fn get(&self, claim_id: &str) -> Option<&ClaimRecord> {
        self.claims.get(claim_id)
    }

    /// Record how far a claim's launch has progressed.
    pub fn set_stage(&mut self, claim_id: &str, stage: ClaimStage) {
        if let Some(claim) = self.claims.get_mut(claim_id) {
            claim.stage = stage;
        }
    }

    pub fn remove(&mut self, claim_id: &str) -> Option<ClaimRecord> {
        let record = self.claims.remove(claim_id)?;
        if self.by_instance.get(&record.instance_id) == Some(&record.claim_id) {
            self.by_instance.remove(&record.instance_id);
        }
        self.by_key.remove(&record.local_key);
        if let Some(allocation_id) = record.allocation_id {
            self.by_allocation.remove(&allocation_id);
        }
        self.add_usage(&record, -1.0);
        Some(record)
    }

    pub fn is_empty(&self) -> bool {
        self.claims.is_empty()
    }

    pub fn len(&self) -> usize {
        self.claims.len()
    }

    /// Whether a live claim already holds this instance id on this node.
    pub fn instance_is_claimed(&self, instance_id: &str) -> bool {
        self.by_instance.contains_key(instance_id)
    }

    /// The live claim holding an instance id on this node.
    pub fn by_instance(&self, instance_id: &str) -> Option<&ClaimRecord> {
        self.by_instance
            .get(instance_id)
            .and_then(|claim_id| self.claims.get(claim_id))
    }

    /// Whether an unratified claim on this node may be running the work a
    /// committed payload describes: same job config and task. Its
    /// ratification binds the payload to the claim; starting the payload on
    /// its own would run it twice. (A payload names no instance, so two
    /// instances of one task are not told apart; the caller only defers.)
    ///
    /// Sessions never match: a committed payload is never a session's
    /// ratification (a promotion names its session explicitly, see
    /// `TaskAllocation.promoted_from_session`).
    pub fn unratified_matches(&self, job_config_id: &str, task_name: &str) -> bool {
        self.claims.values().any(|claim| {
            !claim.is_ratified()
                && !claim.is_session()
                && claim.job_config_id == job_config_id
                && claim.task_name == task_name
        })
    }

    pub fn by_local_key(&self, local_key: i64) -> Option<&ClaimRecord> {
        self.by_key
            .get(&local_key)
            .and_then(|claim_id| self.claims.get(claim_id))
    }

    /// The claim that became this allocation id, if any.
    pub fn by_allocation_id(&self, allocation_id: i64) -> Option<&ClaimRecord> {
        self.by_allocation
            .get(&allocation_id)
            .and_then(|claim_id| self.claims.get(claim_id))
    }

    /// Local keys of claims that are NOT yet ratified. These are exempt from
    /// the heartbeat reconcile's stop sweep for the length of their lease:
    /// the manager cannot list work it does not know about, and an absence
    /// must not be read as "stop this". Sessions are among them for as long
    /// as they run: no desired set ever lists one.
    pub fn unratified_local_keys(&self) -> Vec<i64> {
        self.claims
            .values()
            .filter(|claim| !claim.is_ratified())
            .map(|claim| claim.local_key)
            .collect()
    }

    /// Record the manager's verdict. Returns the updated record.
    pub fn ratify(&mut self, claim_id: &str, allocation_id: i64) -> Option<ClaimRecord> {
        let claim = self.claims.get_mut(claim_id)?;
        if let Some(previous) = claim.allocation_id.replace(allocation_id) {
            self.by_allocation.remove(&previous);
        }
        self.by_allocation
            .insert(allocation_id, claim_id.to_string());
        Some(claim.clone())
    }

    /// Claims whose lease has expired and which are still unratified. These
    /// are killed unconditionally — the backstop for every failure mode the
    /// reply path cannot cover (a partition, a dead manager, a lost reply).
    /// Sessions have no lease deadline and never expire.
    pub fn expired(&self, now: Instant) -> Vec<ClaimRecord> {
        let mut expired = self
            .claims
            .values()
            .filter(|claim| {
                !claim.is_ratified() && !claim.is_session() && now >= claim.lease_deadline
            })
            .cloned()
            .collect::<Vec<_>>();
        expired.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
        expired
    }

    /// Every unratified claim, sorted by claim id, for the heartbeat report.
    /// Sessions are left out: they go in the session report instead.
    pub fn unratified(&self) -> Vec<ClaimRecord> {
        let mut claims = self
            .claims
            .values()
            .filter(|claim| !claim.is_ratified() && !claim.is_session())
            .cloned()
            .collect::<Vec<_>>();
        claims.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
        claims
    }

    /// Every session claim, promoted or not, sorted by claim id.
    pub fn sessions(&self) -> Vec<ClaimRecord> {
        let mut claims = self
            .claims
            .values()
            .filter(|claim| claim.is_session())
            .cloned()
            .collect::<Vec<_>>();
        claims.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
        claims
    }

    /// Every claim, sorted by claim id.
    pub fn all(&self) -> Vec<ClaimRecord> {
        let mut claims = self.claims.values().cloned().collect::<Vec<_>>();
        claims.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
        claims
    }

    /// Local key -> manager allocation id, for the ratified claims. This is
    /// the translation the heartbeat and log paths apply; nothing is rekeyed
    /// and no directory ever moves.
    pub fn external_ids(&self) -> HashMap<i64, i64> {
        self.claims
            .values()
            .filter_map(|claim| claim.allocation_id.map(|id| (claim.local_key, id)))
            .collect()
    }
}

/// The node's own view of its free capacity, used to admit provisional starts.
///
/// **Advisory by design.** The manager's apply-time re-validation is the
/// authority on whether a placement fits; this exists so a node cannot admit
/// twenty sandboxes it has room for two of, and so unratified usage is
/// visible. Keeping it advisory avoids two sources of truth for capacity
/// (plan §11, "Does the local ledger need to be authoritative").
///
/// Unknown resources have infinite capacity, matching
/// [`crate::resources::ResourcePool`]: a node that did not declare `cores`
/// does not reject every task that asks for one.
#[derive(Debug, Clone, Default)]
pub struct LocalLedger {
    capacity: HashMap<String, f64>,
    /// Committed usage, keyed by worker-local allocation key.
    committed: HashMap<i64, HashMap<String, f64>>,
    /// Provisional holds, keyed by claim id.
    holds: HashMap<String, HashMap<String, f64>>,
    /// Cap on concurrent provisional claims (`edgeBoot.maxProvisionalPerNode`
    /// on the manager, mirrored onto the dispatch). 0 = unlimited.
    max_provisional: usize,
}

impl LocalLedger {
    /// Build from the node's declared totals and reservations. `resources`
    /// and `reserved` are the worker-config JSON objects; `max_allocations`
    /// is the node's allocation cap.
    pub fn new(resources: &Value, reserved: &Value, max_allocations: Option<u32>) -> Self {
        let mut capacity = HashMap::new();
        if let Some(object) = resources.as_object() {
            for (name, value) in object {
                if let Some(amount) = value.as_f64() {
                    capacity.insert(name.clone(), amount);
                }
            }
        }
        if let Some(object) = reserved.as_object() {
            for (name, value) in object {
                if let Some(amount) = value.as_f64() {
                    let entry = capacity.entry(name.clone()).or_insert(f64::INFINITY);
                    if entry.is_finite() {
                        *entry -= amount;
                    }
                }
            }
        }
        if let Some(max) = max_allocations {
            capacity.insert("allocations".to_string(), f64::from(max));
        }
        LocalLedger {
            capacity,
            committed: HashMap::new(),
            holds: HashMap::new(),
            max_provisional: 0,
        }
    }

    pub fn set_max_provisional(&mut self, max: usize) {
        self.max_provisional = max;
    }

    fn amounts(value: &Value) -> HashMap<String, f64> {
        let mut amounts = HashMap::new();
        if let Some(object) = value.as_object() {
            for (name, value) in object {
                if let Some(amount) = value.as_f64() {
                    amounts.insert(name.clone(), amount);
                }
            }
        }
        // Every allocation, provisional or not, consumes one allocation slot.
        amounts.insert("allocations".to_string(), 1.0);
        amounts
    }

    fn capacity_of(&self, name: &str) -> f64 {
        self.capacity.get(name).copied().unwrap_or(f64::INFINITY)
    }

    fn used(&self, name: &str) -> f64 {
        let committed: f64 = self
            .committed
            .values()
            .filter_map(|usage| usage.get(name))
            .sum();
        let held: f64 = self
            .holds
            .values()
            .filter_map(|usage| usage.get(name))
            .sum();
        committed + held
    }

    /// Free capacity for one resource. Infinite when the node never declared
    /// it.
    pub fn free(&self, name: &str) -> f64 {
        self.capacity_of(name) - self.used(name)
    }

    /// Record a committed allocation's usage.
    pub fn commit(&mut self, local_key: i64, resources: &Value) {
        self.committed.insert(local_key, Self::amounts(resources));
    }

    /// Release a committed allocation's usage.
    pub fn release(&mut self, local_key: i64) {
        self.committed.remove(&local_key);
    }

    /// Try to hold `resources` for `claim_id`. Fails — without holding
    /// anything — when the node genuinely has no room, or when the
    /// provisional cap is already reached.
    pub fn admit(&mut self, claim_id: &str, resources: &Value) -> R<()> {
        if self.max_provisional > 0 && self.holds.len() >= self.max_provisional {
            bail!(
                "node is already holding {} provisional claims (edgeBoot.maxProvisionalPerNode)",
                self.holds.len()
            );
        }
        let request = Self::amounts(resources);
        for (name, amount) in &request {
            if *amount > self.free(name) {
                bail!(
                    "node has {} {name} free but the claim needs {amount}",
                    self.free(name)
                );
            }
        }
        self.holds.insert(claim_id.to_string(), request);
        Ok(())
    }

    /// Release a provisional hold (the claim was killed, or became a
    /// committed allocation and moved to [`Self::commit`]).
    pub fn release_hold(&mut self, claim_id: &str) {
        self.holds.remove(claim_id);
    }

    pub fn held_claims(&self) -> usize {
        self.holds.len()
    }
}

/// A claim's default lease deadline from a duration, as both an [`Instant`]
/// (what the worker compares against) and unix seconds (what the wire
/// carries).
pub fn lease_deadline(now: Instant, timeout: Duration) -> (Instant, i64) {
    let deadline = now + timeout;
    let unix = time::OffsetDateTime::now_utc().unix_timestamp() + timeout.as_secs() as i64;
    (deadline, unix)
}

#[cfg(test)]
mod claims_test {
    use super::*;
    use serde_json::json;

    fn record(claim_id: &str, instance_id: &str) -> ClaimRecord {
        ClaimRecord {
            claim_id: claim_id.into(),
            local_key: 0,
            gate: Gate::Boot,
            stage: ClaimStage::Claimed,
            lease_deadline: Instant::now() + Duration::from_secs(5),
            lease_deadline_unix: 0,
            job_id: "agent-sandboxes".into(),
            job_config_id: "jc-1".into(),
            task_name: "workspace".into(),
            instance_id: instance_id.into(),
            resources: json!({ "memory": 512 }),
            origin: "edge".into(),
            on_refused: OnRefused::Kill,
            ticket_jti: String::new(),
            allocation_id: None,
            session: None,
        }
    }

    #[test]
    fn local_keys_are_negative_and_unique() -> R<()> {
        let mut table = ClaimTable::new();
        let a = table.insert(record("clm_a", "user-1"))?;
        let b = table.insert(record("clm_b", "user-2"))?;
        assert!(a.local_key < 0 && b.local_key < 0);
        assert_ne!(a.local_key, b.local_key);
        Ok(())
    }

    #[test]
    fn restored_keys_are_never_reissued() -> R<()> {
        // After a restart the counter must start below every key a ratified
        // claim is still using, or a new claim would collide with running
        // work.
        let mut table = ClaimTable::new();
        table.reserve_below([-1, -7, -3]);
        let fresh = table.insert(record("clm_a", "user-1"))?;
        assert!(fresh.local_key < -7, "got {}", fresh.local_key);
        Ok(())
    }

    #[test]
    fn a_restored_claim_keeps_its_key_and_its_room_test() -> R<()> {
        let mut table = ClaimTable::new();
        let mut restored = record("clm_r", "workspace-x");
        restored.local_key = -40;
        restored.session = Some(SessionClaim {
            lease_issued_at_unix_ms: 1,
            generation: 1,
        });
        restored.resources = serde_json::json!({ "cpu": 250 });
        let stored = table.insert_restored(restored)?;
        assert_eq!(stored.local_key, -40, "the key the process runs under");
        assert_eq!(
            table.by_local_key(-40).map(|c| c.claim_id.as_str()),
            Some("clm_r")
        );
        assert_eq!(
            table
                .session_usage("agent-sandboxes", "workspace")
                .get("cpu"),
            Some(&250.0)
        );
        let fresh = table.insert(record("clm_b", "user-2"))?;
        assert!(fresh.local_key < -40, "got {}", fresh.local_key);
        table.remove("clm_r");
        assert!(table
            .session_usage("agent-sandboxes", "workspace")
            .is_empty());
        assert!(!table.instance_is_claimed("workspace-x"));
        Ok(())
    }

    #[test]
    fn one_live_claim_per_instance_per_node() -> R<()> {
        let mut table = ClaimTable::new();
        table.insert(record("clm_a", "user-42"))?;
        // The same-node half of the at-most-one rule: resolved locally,
        // before anything boots.
        assert!(table.insert(record("clm_b", "user-42")).is_err());
        // A different instance is fine.
        table.insert(record("clm_c", "user-43"))?;
        Ok(())
    }

    #[test]
    fn stateless_claims_do_not_collide() -> R<()> {
        let mut table = ClaimTable::new();
        table.insert(record("clm_a", ""))?;
        table.insert(record("clm_b", ""))?;
        assert_eq!(table.len(), 2);
        Ok(())
    }

    #[test]
    fn duplicate_claim_ids_are_rejected() -> R<()> {
        let mut table = ClaimTable::new();
        table.insert(record("clm_a", "user-1"))?;
        assert!(table.insert(record("clm_a", "user-2")).is_err());
        Ok(())
    }

    #[test]
    fn only_unratified_claims_expire() -> R<()> {
        let mut table = ClaimTable::new();
        let mut stale = record("clm_a", "user-1");
        stale.lease_deadline = Instant::now() - Duration::from_secs(1);
        table.insert(stale)?;
        let mut ratified = record("clm_b", "user-2");
        ratified.lease_deadline = Instant::now() - Duration::from_secs(1);
        table.insert(ratified)?;
        table.ratify("clm_b", 8123).expect("ratified");

        let expired = table.expired(Instant::now());
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].claim_id, "clm_a");
        // A ratified claim is an ordinary allocation; the lease is over.
        assert_eq!(table.unratified().len(), 1);
        assert_eq!(
            table
                .external_ids()
                .get(&table.get("clm_b").unwrap().local_key),
            Some(&8123)
        );
        Ok(())
    }

    #[test]
    fn sessions_never_expire_and_stay_out_of_the_claims_report_test() -> R<()> {
        let mut table = ClaimTable::new();
        let mut session = record("session:workspace-a", "workspace-a");
        session.lease_deadline = Instant::now() - Duration::from_secs(1);
        session.session = Some(SessionClaim {
            lease_issued_at_unix_ms: 1,
            generation: 1,
        });
        let session = table.insert(session)?;
        table.insert(record("clm_b", "user-2"))?;

        assert!(table.expired(Instant::now()).is_empty());
        assert_eq!(table.unratified().len(), 1);
        assert_eq!(table.sessions().len(), 1);
        // Still protected from the reconcile sweep: no desired set lists it.
        assert!(table.unratified_local_keys().contains(&session.local_key));
        // A committed payload of the same task is not this session's
        // ratification.
        table.remove("clm_b");
        assert!(!table.unratified_matches("jc-1", "workspace"));
        Ok(())
    }

    #[test]
    fn a_claim_that_does_not_fit_holds_nothing() -> R<()> {
        let mut ledger = LocalLedger::new(&json!({ "memory": 1024 }), &json!({}), None);
        ledger.admit("clm_a", &json!({ "memory": 768 }))?;
        // Second claim does not fit; the failure must not leave a partial
        // hold behind or the node slowly leaks capacity to rejected starts.
        assert!(ledger.admit("clm_b", &json!({ "memory": 768 })).is_err());
        assert_eq!(ledger.held_claims(), 1);
        assert_eq!(ledger.free("memory"), 256.0);

        ledger.release_hold("clm_a");
        assert_eq!(ledger.free("memory"), 1024.0);
        Ok(())
    }

    #[test]
    fn committed_allocations_and_holds_share_the_pool() -> R<()> {
        let mut ledger = LocalLedger::new(&json!({ "memory": 1024 }), &json!({}), None);
        ledger.commit(11, &json!({ "memory": 512 }));
        assert_eq!(ledger.free("memory"), 512.0);
        ledger.admit("clm_a", &json!({ "memory": 512 }))?;
        assert_eq!(ledger.free("memory"), 0.0);
        assert!(ledger.admit("clm_b", &json!({ "memory": 1 })).is_err());
        ledger.release(11);
        ledger.admit("clm_b", &json!({ "memory": 1 }))?;
        Ok(())
    }

    #[test]
    fn reserved_resources_come_off_the_top() {
        let ledger = LocalLedger::new(
            &json!({ "memory": 1024 }),
            &json!({ "memory": 256 }),
            Some(4),
        );
        assert_eq!(ledger.free("memory"), 768.0);
        assert_eq!(ledger.free("allocations"), 4.0);
    }

    #[test]
    fn undeclared_resources_are_unlimited() {
        // Same posture as the scheduler's pool: a node that never
        // fingerprinted `gpu` must not reject every task that asks for one.
        let ledger = LocalLedger::new(&json!({}), &json!({}), None);
        assert!(ledger.free("gpu").is_infinite());
    }

    #[test]
    fn every_claim_consumes_an_allocation_slot() -> R<()> {
        let mut ledger = LocalLedger::new(&json!({}), &json!({}), Some(2));
        ledger.admit("clm_a", &json!({}))?;
        ledger.admit("clm_b", &json!({}))?;
        assert!(ledger.admit("clm_c", &json!({})).is_err());
        Ok(())
    }

    #[test]
    fn the_provisional_cap_is_enforced_before_resources() -> R<()> {
        let mut ledger = LocalLedger::new(&json!({}), &json!({}), None);
        ledger.set_max_provisional(1);
        ledger.admit("clm_a", &json!({}))?;
        let err = ledger.admit("clm_b", &json!({})).expect_err("capped");
        assert!(err.to_string().contains("maxProvisionalPerNode"), "{err}");
        Ok(())
    }
}
