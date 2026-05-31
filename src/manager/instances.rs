//! Stable instance identity — the persistent layer between a task spec and its
//! allocations (stateful-workloads plan).
//!
//! See `docs/plans/stateful-workloads-and-instance-identity-plan.md` and
//! `docs/stateful-workloads.md`. An [`Instance`] is a durable handle that
//! outlives any single allocation: the task says "I want N slots (or these K
//! keys)", the instance table records each instance's stable identity and
//! current allocation, and the scheduler reconciles instances→allocations the
//! way it already reconciles count→allocations.
//!
//! This module owns the *identity model and the pure materialization/reconcile
//! logic*; persistence lives in `crate::storage`. The instance id it produces
//! is exactly the stable handle the workload-identity `sub` extension consumes
//! (`legion.io/instance` / `legion.io/slot`).
//!
//! Deferred to later phases (see the plan): the scheduler reconcile loop that
//! turns desired instances into allocations and copies `instance_id` onto the
//! allocation, the keyed-mode gRPC API, DNS/service discovery, and volume
//! affinity scheduling. The functions here are the testable core those phases
//! drive.

use serde::{Deserialize, Serialize};

use crate::prelude::*;
use crate::structs::workload_identity::WorkloadIdentityClaims;
use crate::structs::{StatefulConfig, StatefulMode};

/// A stable, durable instance identity that survives reschedules.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Instance {
    /// Stable id: `<job>:<group>:<slot-or-key>`.
    pub id: String,
    pub job_id: String,
    pub group: String,
    pub mode: StatefulMode,
    /// Slot mode only: 0, 1, 2, …
    pub ordinal: Option<u32>,
    /// Keyed mode only: the caller-supplied stable key.
    pub key: Option<String>,
    /// Should this instance be running?
    pub desired: bool,
    /// Keyed instances that were destroyed stay tombstoned so their key (and any
    /// federated credentials scoped to it) can never be silently reissued.
    pub tombstoned: bool,
    /// The allocation currently realizing this instance, if any.
    pub current_alloc: Option<String>,
    /// Last node it ran on (volume-affinity hint).
    pub node_id: Option<String>,
    /// Deterministic DNS-style name, when `stable_network_id` is set.
    pub network_id: Option<String>,
    pub created_at: String,
}

/// Build the canonical instance id `<job>:<group>:<slot-or-key>`.
pub fn instance_id(job: &str, group: &str, slot_or_key: &str) -> String {
    format!("{job}:{group}:{slot_or_key}")
}

/// Derive the deterministic per-instance network name. Slot instances get
/// `<group>-<ordinal>.<job>.<region>`; keyed instances get `<key>.<job>.<region>`.
pub fn slot_network_id(region: &str, job: &str, group: &str, ordinal: u32) -> String {
    format!("{group}-{ordinal}.{job}.{region}")
}

pub fn keyed_network_id(region: &str, job: &str, key: &str) -> String {
    format!("{key}.{job}.{region}")
}

impl Instance {
    fn new_slot(
        job_id: &str,
        group: &str,
        ordinal: u32,
        stateful: &StatefulConfig,
        region: &str,
        now: &str,
    ) -> Self {
        let network_id = stateful
            .stable_network_id
            .then(|| slot_network_id(region, job_id, group, ordinal));
        Instance {
            id: instance_id(job_id, group, &ordinal.to_string()),
            job_id: job_id.to_string(),
            group: group.to_string(),
            mode: StatefulMode::Slot,
            ordinal: Some(ordinal),
            key: None,
            desired: true,
            tombstoned: false,
            current_alloc: None,
            node_id: None,
            network_id,
            created_at: now.to_string(),
        }
    }

    fn new_keyed(
        job_id: &str,
        group: &str,
        key: &str,
        stateful: &StatefulConfig,
        region: &str,
        now: &str,
    ) -> Self {
        let network_id = stateful
            .stable_network_id
            .then(|| keyed_network_id(region, job_id, key));
        Instance {
            id: instance_id(job_id, group, key),
            job_id: job_id.to_string(),
            group: group.to_string(),
            mode: StatefulMode::Keyed,
            ordinal: None,
            key: Some(key.to_string()),
            desired: true,
            tombstoned: false,
            current_alloc: None,
            node_id: None,
            network_id,
            created_at: now.to_string(),
        }
    }

    /// The stable suffix consumed by workload identity: the ordinal (slot) or
    /// the key (keyed).
    pub fn suffix(&self) -> String {
        match self.mode {
            StatefulMode::Slot => self.ordinal.map(|o| o.to_string()).unwrap_or_default(),
            StatefulMode::Keyed => self.key.clone().unwrap_or_default(),
        }
    }

    /// Build workload-identity claims for this instance, filling the
    /// per-instance `legion.io/instance` / `legion.io/slot` fields and the
    /// stable `{instance}`/`{slot}` template inputs. Identity persists across
    /// reschedules even though `alloc_id` churns.
    pub fn workload_identity_claims(
        &self,
        region: &str,
        task: &str,
        node_id: &str,
        alloc_id: &str,
    ) -> WorkloadIdentityClaims {
        WorkloadIdentityClaims {
            region: region.to_string(),
            job: self.job_id.clone(),
            group: self.group.clone(),
            task: task.to_string(),
            node_id: node_id.to_string(),
            alloc_id: alloc_id.to_string(),
            instance: Some(self.suffix()),
            slot: self.ordinal,
        }
    }
}

/// The `LEGION_INSTANCE_*` environment variables exposed to a task for this
/// instance + its current allocation. `LEGION_INSTANCE_SLOT` is present only in
/// slot mode. This is the rendering Phase 2 injects via the task-launch path;
/// it's a pure function here so it can be unit-tested and reused.
pub fn instance_env(instance: &Instance, alloc_id: &str) -> Vec<(String, String)> {
    let mode = match instance.mode {
        StatefulMode::Slot => "slot",
        StatefulMode::Keyed => "keyed",
    };
    let mut env = vec![
        ("LEGION_INSTANCE_ID".to_string(), instance.id.clone()),
        ("LEGION_INSTANCE_MODE".to_string(), mode.to_string()),
        ("LEGION_ALLOC_ID".to_string(), alloc_id.to_string()),
    ];
    if let Some(ordinal) = instance.ordinal {
        env.push(("LEGION_INSTANCE_SLOT".to_string(), ordinal.to_string()));
    }
    if let Some(network_id) = &instance.network_id {
        env.push(("LEGION_INSTANCE_NETWORK_ID".to_string(), network_id.clone()));
    }
    env
}

/// Reconcile slot-mode instances against a desired `count`.
///
/// - Ordinals `0..count` are (kept) desired. Because the id is a pure function
///   of the ordinal, scaling down then up *automatically* recycles ordinals
///   densely — ordinal N is always the same instance id.
/// - Existing instances with ordinal `>= count` are marked undesired (scale
///   down) but retained so a later scale-up revives the same identity.
///
/// Returns the full instance set (existing rows updated in place, new ones
/// appended), so the caller can persist the diff.
pub fn reconcile_slots(
    existing: &[Instance],
    job_id: &str,
    group: &str,
    count: u32,
    stateful: &StatefulConfig,
    region: &str,
    now: &str,
) -> Vec<Instance> {
    use std::collections::BTreeMap;

    // Index existing slot instances for this job+group by ordinal.
    let mut by_ordinal: BTreeMap<u32, Instance> = BTreeMap::new();
    for inst in existing {
        if inst.job_id == job_id && inst.group == group && inst.mode == StatefulMode::Slot {
            if let Some(ordinal) = inst.ordinal {
                by_ordinal.insert(ordinal, inst.clone());
            }
        }
    }

    // Desired ordinals 0..count: revive/create and mark desired.
    for ordinal in 0..count {
        by_ordinal
            .entry(ordinal)
            .and_modify(|inst| inst.desired = true)
            .or_insert_with(|| Instance::new_slot(job_id, group, ordinal, stateful, region, now));
    }

    // Ordinals at or above count: scale down (retain for dense reuse).
    for (ordinal, inst) in by_ordinal.iter_mut() {
        if *ordinal >= count {
            inst.desired = false;
        }
    }

    by_ordinal.into_values().collect()
}

/// Create a keyed instance. Rejects an existing live key (no duplicates) and a
/// tombstoned key (never recycle a destroyed key).
pub fn create_keyed(
    existing: &[Instance],
    job_id: &str,
    group: &str,
    key: &str,
    stateful: &StatefulConfig,
    region: &str,
    now: &str,
) -> R<Instance> {
    if key.trim().is_empty() {
        bail!("keyed instance key cannot be empty");
    }
    let id = instance_id(job_id, group, key);
    if let Some(existing) = existing.iter().find(|i| i.id == id) {
        if existing.tombstoned {
            bail!(
                "instance key {key:?} was destroyed and is tombstoned; keyed instance keys are \
                 never recycled"
            );
        }
        bail!("instance key {key:?} already exists");
    }
    Ok(Instance::new_keyed(
        job_id, group, key, stateful, region, now,
    ))
}

/// Mark a keyed instance destroyed: undesired and tombstoned (kept forever so
/// the key is never silently recycled). Returns the updated instance.
pub fn destroy_keyed(mut instance: Instance) -> Instance {
    instance.desired = false;
    instance.tombstoned = true;
    instance.current_alloc = None;
    instance
}

/// Materialize desired instances for a task from its spec. Slot mode produces
/// `0..count`; keyed mode is API-driven so reconcile leaves the existing
/// (non-tombstoned) instances untouched.
pub fn materialize(
    existing: &[Instance],
    job_id: &str,
    group: &str,
    count: u32,
    stateful: &StatefulConfig,
    region: &str,
    now: &str,
) -> Vec<Instance> {
    match stateful.mode {
        StatefulMode::Slot => {
            reconcile_slots(existing, job_id, group, count, stateful, region, now)
        }
        // Keyed instances are created/destroyed via API, not from count.
        StatefulMode::Keyed => existing
            .iter()
            .filter(|i| i.job_id == job_id && i.group == group && i.mode == StatefulMode::Keyed)
            .cloned()
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: &str = "2026-01-01T00:00:00Z";

    fn slot_config() -> StatefulConfig {
        StatefulConfig {
            mode: StatefulMode::Slot,
            stable_network_id: true,
            volume_affinity: true,
        }
    }

    fn keyed_config() -> StatefulConfig {
        StatefulConfig {
            mode: StatefulMode::Keyed,
            stable_network_id: true,
            volume_affinity: false,
        }
    }

    #[test]
    fn stable_id_format() {
        assert_eq!(instance_id("db", "data", "0"), "db:data:0");
        assert_eq!(
            instance_id("sb", "main", "tenant-acme"),
            "sb:main:tenant-acme"
        );
    }

    #[test]
    fn network_id_derivation() {
        assert_eq!(slot_network_id("auto", "db", "data", 1), "data-1.db.auto");
        assert_eq!(keyed_network_id("auto", "sb", "acme"), "acme.sb.auto");
    }

    #[test]
    fn slot_materialization_creates_dense_ordinals() {
        let instances = reconcile_slots(&[], "db", "data", 3, &slot_config(), "auto", NOW);
        let ids: Vec<&str> = instances.iter().map(|i| i.id.as_str()).collect();
        assert_eq!(ids, ["db:data:0", "db:data:1", "db:data:2"]);
        assert!(instances.iter().all(|i| i.desired));
        assert_eq!(instances[1].ordinal, Some(1));
        assert_eq!(instances[1].network_id.as_deref(), Some("data-1.db.auto"));
    }

    #[test]
    fn scale_down_marks_high_ordinals_undesired() {
        let three = reconcile_slots(&[], "db", "data", 3, &slot_config(), "auto", NOW);
        let scaled = reconcile_slots(&three, "db", "data", 1, &slot_config(), "auto", NOW);
        // All three rows retained; only ordinal 0 desired.
        assert_eq!(scaled.len(), 3);
        assert!(scaled[0].desired);
        assert!(!scaled[1].desired);
        assert!(!scaled[2].desired);
    }

    #[test]
    fn scale_back_up_reuses_same_instance_ids() {
        let three = reconcile_slots(&[], "db", "data", 3, &slot_config(), "auto", NOW);
        let down = reconcile_slots(&three, "db", "data", 1, &slot_config(), "auto", NOW);
        let up = reconcile_slots(&down, "db", "data", 3, &slot_config(), "auto", NOW);
        let ids: Vec<&str> = up.iter().map(|i| i.id.as_str()).collect();
        assert_eq!(ids, ["db:data:0", "db:data:1", "db:data:2"]);
        assert!(up.iter().all(|i| i.desired), "all revived as same ids");
        // created_at preserved → these are the *same* instances, not new ones.
        assert!(up.iter().all(|i| i.created_at == NOW));
    }

    #[test]
    fn slot_reconcile_ignores_other_groups_and_jobs() {
        let other = vec![Instance::new_slot(
            "other",
            "g",
            0,
            &slot_config(),
            "auto",
            NOW,
        )];
        let result = reconcile_slots(&other, "db", "data", 1, &slot_config(), "auto", NOW);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "db:data:0");
    }

    #[test]
    fn keyed_create_then_duplicate_rejected() {
        let acme = create_keyed(&[], "sb", "main", "acme", &keyed_config(), "auto", NOW).unwrap();
        assert_eq!(acme.id, "sb:main:acme");
        assert_eq!(acme.network_id.as_deref(), Some("acme.sb.auto"));

        let err =
            create_keyed(&[acme], "sb", "main", "acme", &keyed_config(), "auto", NOW).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn destroyed_key_is_tombstoned_and_never_recycled() {
        let acme = create_keyed(&[], "sb", "main", "acme", &keyed_config(), "auto", NOW).unwrap();
        let dead = destroy_keyed(acme);
        assert!(dead.tombstoned);
        assert!(!dead.desired);

        let err =
            create_keyed(&[dead], "sb", "main", "acme", &keyed_config(), "auto", NOW).unwrap_err();
        assert!(err.to_string().contains("tombstoned"), "{err}");
    }

    #[test]
    fn keyed_create_rejects_empty_key() {
        let err = create_keyed(&[], "sb", "main", "  ", &keyed_config(), "auto", NOW).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn materialize_keyed_is_api_driven_not_count_driven() {
        let acme = create_keyed(&[], "sb", "main", "acme", &keyed_config(), "auto", NOW).unwrap();
        // count is ignored in keyed mode; existing instances pass through.
        let result = materialize(&[acme], "sb", "main", 99, &keyed_config(), "auto", NOW);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "sb:main:acme");
    }

    #[test]
    fn instance_feeds_workload_identity_claims() {
        let three = reconcile_slots(&[], "db", "data", 3, &slot_config(), "auto", NOW);
        let claims = three[1].workload_identity_claims("auto", "server", "node-7", "alloc-99");
        assert!(claims.is_stateful());
        assert_eq!(claims.instance.as_deref(), Some("1"));
        assert_eq!(claims.slot, Some(1));

        // The stateful sub extension carries the stable instance, not alloc_id.
        let sub = claims
            .render_subject("{region}:{job}:{group}:{task}:{instance}")
            .unwrap();
        assert_eq!(sub, "auto:db:data:server:1");

        let legion = claims.legion_claims();
        assert_eq!(legion["legion.io/instance"], "1");
        assert_eq!(legion["legion.io/slot"], "1");
    }

    #[test]
    fn slot_instance_env_includes_slot_and_network() {
        let three = reconcile_slots(&[], "db", "data", 3, &slot_config(), "auto", NOW);
        let env: std::collections::HashMap<String, String> =
            instance_env(&three[2], "alloc-5").into_iter().collect();
        assert_eq!(env["LEGION_INSTANCE_ID"], "db:data:2");
        assert_eq!(env["LEGION_INSTANCE_MODE"], "slot");
        assert_eq!(env["LEGION_INSTANCE_SLOT"], "2");
        assert_eq!(env["LEGION_ALLOC_ID"], "alloc-5");
        assert_eq!(env["LEGION_INSTANCE_NETWORK_ID"], "data-2.db.auto");
    }

    #[test]
    fn keyed_instance_env_omits_slot() {
        let acme = create_keyed(&[], "sb", "main", "acme", &keyed_config(), "auto", NOW).unwrap();
        let env: std::collections::HashMap<String, String> =
            instance_env(&acme, "alloc-1").into_iter().collect();
        assert_eq!(env["LEGION_INSTANCE_ID"], "sb:main:acme");
        assert_eq!(env["LEGION_INSTANCE_MODE"], "keyed");
        assert!(!env.contains_key("LEGION_INSTANCE_SLOT"));
    }

    #[test]
    fn keyed_instance_feeds_workload_identity_claims() {
        let acme = create_keyed(&[], "sb", "main", "acme", &keyed_config(), "auto", NOW).unwrap();
        let claims = acme.workload_identity_claims("auto", "runtime", "node-1", "alloc-1");
        assert_eq!(claims.instance.as_deref(), Some("acme"));
        assert_eq!(claims.slot, None); // keyed instances have no ordinal
        let sub = claims
            .render_subject("{region}:{job}:{group}:{task}:{instance}")
            .unwrap();
        assert_eq!(sub, "auto:sb:main:runtime:acme");
    }
}
